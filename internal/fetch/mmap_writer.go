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
}

// rawFileWriter wraps *os.File for the fileWriter interface. Used as
// the fallback when mmap is unavailable (e.g., size of 0).
type rawFileWriter struct{ F *os.File }

// WriteAt delegates to F.WriteAt.
func (r *rawFileWriter) WriteAt(buf []byte, off int64) (int, error) {
	return r.F.WriteAt(buf, off)
}

// Sync delegates to F.Sync (fsync/fdatasync on fd).
func (r *rawFileWriter) Sync() error { return r.F.Sync() }

// Close delegates to F.Close.
func (r *rawFileWriter) Close() error { return r.F.Close() }

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
	fd   *os.File
	data []byte
	size int64
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
		fd.Close()
		return nil, fmt.Errorf("truncate: %w", err)
	}
	data, err := mmapSys(fd.Fd(), int(size))
	if err != nil {
		fd.Close()
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
func (m *mmapWriter) Sync() error { return m.fd.Sync() }

// Close unmaps + closes the fd. Safe to call multiple times.
func (m *mmapWriter) Close() error {
	if m.data == nil {
		return nil
	}
	_ = m.fd.Sync()
	unmapErr := munmapSys(m.data)
	m.data = nil
	closeErr := m.fd.Close()
	m.fd = nil
	if unmapErr != nil {
		return unmapErr
	}
	return closeErr
}

// allocateFileWriter returns the fastest fileWriter for the given
// output. For size > 0 it prefers mmap (no pwrite syscalls). When
// mmap fails it falls back to plain pwrite via *os.File.
//
// When resume is true and the file already exists with the right size,
// we keep it open RDWR for mmap (no truncation). Otherwise we create
// or truncate to size and mmap the freshly-sized region.
func allocateFileWriter(path string, size int64, resume bool) (fileWriter, error) {
	if size <= 0 {
		return allocateRawFile(path, size, resume)
	}
	if resume {
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Fresh download, no partial file yet — write-only
				// with truncation is correct.
				return allocateRawFile(path, size, false)
			}
			// Some other stat failure (perm denied, vanished mid-call,
			// etc). Refuse rather than silently truncating: the caller
			// can re-run with --no-resume to disambiguate.
			return nil, fmt.Errorf("stat %s for resume: %w (use --no-resume to retry)", path, err)
		}
		if info.Size() != size {
			// File exists but wrong size — treat as fresh: truncate
			// to expected size is correct here, but make it explicit.
			return allocateRawFile(path, size, false)
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
		return m.withMmap()
	}
	mw, err := newMmapWriter(path, size)
	if err != nil {
		return allocateRawFile(path, size, false)
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
func allocateRawFile(path string, size int64, resume bool) (fileWriter, error) {
	flags := os.O_WRONLY | os.O_CREATE
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
			fd.Close()
			return nil, ferr
		}
		if info.Size() != size {
			if terr := fd.Truncate(size); terr != nil {
				fd.Close()
				return nil, fmt.Errorf("truncate: %w", terr)
			}
		}
	}
	return &rawFileWriter{F: fd}, nil
}
