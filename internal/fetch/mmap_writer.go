package fetch

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// fileWriter is the abstraction over the output file, allowing us to
// swap pwrite (slow path) for mmap (fast path) per-download.
type fileWriter interface {
	WriteAt(buf []byte, off int64) (int, error)
	Sync() error
	Close() error
	Truncate(size int64) error
}

// rawFileWriter wraps *os.File for the fileWriter interface. Used as
// the fallback when mmap is unavailable (e.g., size of 0).
type rawFileWriter struct{ F *os.File }

// WriteAt delegates to F.WriteAt.
func (r *rawFileWriter) WriteAt(buf []byte, off int64) (int, error) {
	return r.F.WriteAt(buf, off)
}

// ReadAt enables post-task manifest verification without mmap.
func (r *rawFileWriter) ReadAt(buf []byte, off int64) (int, error) {
	return r.F.ReadAt(buf, off)
}

// Sync delegates to F.Sync (fsync/fdatasync on fd).
func (r *rawFileWriter) Sync() error { return r.F.Sync() }

// Close delegates to F.Close.
func (r *rawFileWriter) Close() error { return r.F.Close() }

// Truncate truncates the file to size.
func (r *rawFileWriter) Truncate(size int64) error {
	return r.F.Truncate(size)
}

// Bytes returns nil for raw writers — the generic Read→buffer→write path
// is used. Workers type-assert fileWriter to mmapWriterBytes to skip
// the memcpy step when the mmap slice is available.
func (r *rawFileWriter) Bytes() []byte { return nil }

// mmapWriterBytes is the interface to access the mmap'd slice. Workers
// type-assert to this to skip the memcpy step.
type mmapWriterBytes interface {
	fileWriter
	Bytes() []byte
}

// mmapWriter writes to a memory-mapped file. This is the fast path:
// bytes flow into the page cache via memcpy with no per-buffer syscall.
type mmapWriter struct {
	fd     *os.File
	data   []byte
	size   int64
	synced bool // set by Sync() so Close() skips a redundant fsync
}

// newMmapWriter creates and mmaps the output file in RW shared mode.
func newMmapWriter(path string, size int64) (*mmapWriter, error) {
	if size <= 0 || size > 1<<62 {
		return nil, fmt.Errorf("mmap: invalid size %d", size)
	}
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if err := fd.Truncate(size); err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("truncate: %w", err)
	}
	data, err := mmapSys(fd.Fd(), int(size))
	if err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("mmap: %w", err)
	}
	// Hint the kernel we'll access the bytes sequentially. This avoids
	// the reader-side readahead state machine being initialized for a
	// memory region we'll only write to.
	_ = hintSequential(data)
	return &mmapWriter{fd: fd, data: data, size: size}, nil
}

// WriteAt copies buf into the mmap'd view at off.
func (m *mmapWriter) WriteAt(buf []byte, off int64) (int, error) {
	if off+int64(len(buf)) > int64(len(m.data)) {
		return 0, fmt.Errorf("mmap write OOB: off=%d len=%d cap=%d",
			off, len(buf), len(m.data))
	}
	copy(m.data[off:], buf)
	return len(buf), nil
}

// Bytes returns the underlying mmap-d byte slice. Workers can directly
// read responses into this slice at the correct offset, avoiding one
// memcpy per chunk. Returns nil for non-mmap writers.
func (m *mmapWriter) Bytes() []byte { return m.data }

// Sync flushes dirty mmap pages via the underlying fd. Note: mmap MAP_SHARED
// pages are also auto-flushed to the page cache on memcpy; Sync() ensures
// page cache contents reach the disk, surviving process crashes.
func (m *mmapWriter) Sync() error {
	m.synced = true
	return m.fd.Sync()
}

// Close unmaps + closes the fd. Safe to call multiple times. On the
// finalize success path Sync() already ran, so the redundant second
// fsync is skipped; when Close is reached without a prior Sync (error
// paths) it fsyncs once to keep the write durable.
func (m *mmapWriter) Close() error {
	if m.data == nil {
		return nil
	}
	var syncErr error
	if !m.synced {
		syncErr = m.fd.Sync()
	}
	unmapErr := munmapSys(m.data)
	m.data = nil
	closeErr := m.fd.Close()
	m.fd = nil
	// Surface fsync failures first: a lost sync means dirty pages may
	// not be durable on disk — callers must treat the write as
	// non-durable even if later integrity verification reads back
	// page-cache bytes (which can read fine despite stale on-disk state).
	if syncErr != nil {
		return syncErr
	}
	if unmapErr != nil {
		return unmapErr
	}
	return closeErr
}

// Truncate truncates the mmap'd file to size.
func (m *mmapWriter) Truncate(size int64) error {
	if size < 0 || size > m.size {
		return fmt.Errorf("mmap truncate: invalid size %d (max %d)", size, m.size)
	}
	if err := m.fd.Truncate(size); err != nil {
		return err
	}
	if size < m.size {
		// Remap to smaller size. Nil out m.data immediately after
		// munmap so a concurrent reader can't observe a dangling
		// pointer if the subsequent mmapSys fails (use-after-free).
		unmapErr := munmapSys(m.data)
		m.data = nil
		if unmapErr != nil {
			return unmapErr
		}
		m.size = size
		data, err := mmapSys(m.fd.Fd(), int(size))
		if err != nil {
			return err
		}
		m.data = data
	}
	return nil
}

// allocateFileWriter returns the fastest fileWriter for the given
// output. For size > 0 it prefers mmap (no pwrite syscalls). When
// mmap fails it falls back to plain pwrite via *os.File.
//
// When resume is true and the file already exists with the right size,
// we keep it open RDWR for mmap (no truncation). Otherwise we create
// or truncate to size and mmap the freshly-sized region.
func allocateFileWriter(path string, size int64, resume bool, noMmap bool) (fileWriter, error) {
	// Refuse to write the output through a symlink: with attacker- or
	// operator-supplied URLs, a pre-placed symlink at the output path
	// could redirect writes into an arbitrary file. Same guard clearResume
	// applies to sidecars. Intermediate directories are not checked.
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("refusing to write output through symlink %q", path)
	}
	if noMmap {
		return allocateRawFile(path, size, resume)
	}
	if size <= 0 {
		return allocateRawFile(path, size, resume)
	}
	if resume {
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Fresh download (resume enabled but no partial yet):
				// still prefer mmap — previously this fell through to
				// raw pwrite and paid one syscall per buffer.
				return allocateMmapOrRaw(path, size, false)
			}
			// Some other stat failure (perm denied, vanished mid-call,
			// etc). Refuse rather than silently truncating: the caller
			// can re-run with --no-resume to disambiguate.
			return nil, fmt.Errorf("stat %s for resume: %w (use --no-resume to retry)", path, err)
		}
		if info.Size() != size {
			// File exists but wrong size — treat as fresh (truncate).
			return allocateMmapOrRaw(path, size, false)
		}
		// File exists at expected size: keep existing bytes.
		fd, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		m := &mmapWriter{
			fd:   fd,
			data: nil, // lazy-mmap below
			size: size,
		}
		mw, err := m.withMmap()
		if err != nil {
			// Do not leak the RDWR fd; fall back to pwrite preserving bytes.
			_ = fd.Close()
			return allocateRawFile(path, size, true)
		}
		return mw, nil
	}
	return allocateMmapOrRaw(path, size, false)
}

// allocateMmapOrRaw prefers mmap; falls back to raw pwrite on failure.
func allocateMmapOrRaw(path string, size int64, resume bool) (fileWriter, error) {
	mw, err := newMmapWriter(path, size)
	if err != nil {
		return allocateRawFile(path, size, resume)
	}
	return mw, nil
}

// withMmap finishes mapping the file into m.data. Used when the file
// pre-existed at the right size.
func (m *mmapWriter) withMmap() (*mmapWriter, error) {
	if m.data != nil {
		return m, nil
	}
	data, err := mmapSys(m.fd.Fd(), int(m.size))
	if err != nil {
		return nil, fmt.Errorf("mmap resume: %w", err)
	}
	m.data = data
	return m, nil
}

// allocateRawFile is the fallback writer for size=0 (regular file).
// Honors `resume` by skipping the O_TRUNC flag if true.
// Opens RDWR so post-task manifest VerifyRange can re-read spans.
func allocateRawFile(path string, size int64, resume bool) (fileWriter, error) {
	flags := os.O_RDWR | os.O_CREATE
	if !resume {
		flags |= os.O_TRUNC
	}
	fd, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	if size > 0 {
		info, ferr := fd.Stat()
		if ferr != nil {
			_ = fd.Close()
			return nil, ferr
		}
		if info.Size() != size {
			if terr := fd.Truncate(size); terr != nil {
				_ = fd.Close()
				return nil, fmt.Errorf("truncate: %w", terr)
			}
		}
	}
	return &rawFileWriter{F: fd}, nil
}
