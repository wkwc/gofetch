package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// readBody streams the 206 response body for one task into f, using the
// zero-copy mmap path when the writer exposes its underlying slice.
func (d *Downloader) readBody(ctx context.Context, task Task, f fileWriter, ws *workerState, body io.ReadCloser) error {
	// Idle-timeout wrapper so a stalled body after headers is retryable
	// instead of hanging forever (steal only fires after bytesDone ≥ 1).
	idle := newIdleBody(ctx, body, defaultBodyIdle)
	defer drainAndClose(idle)
	_, err := d.pumpBody(idle, f, ws, task.Start, task.End, true)
	return err
}

// mmapBytes returns the writer's mmap'd backing slice, or nil when the
// writer is not mmap-backed (raw pwrite fallback).
func mmapBytes(f fileWriter) []byte {
	if wb, ok := f.(mmapWriterBytes); ok {
		return wb.Bytes()
	}
	return nil
}

// pumpBody streams body into f across the inclusive byte range
// [start, end], returning bytes written. When strict is true, EOF before
// the range is fully written is wrapped as io.ErrUnexpectedEOF so the
// worker treats it as retryable; when false (single-stream) EOF ends the
// stream and the caller checks the byte count itself. end < 0 means the
// size is unknown: write everything until EOF (caller truncates).
//
// Zero-copy fast path: when the writer exposes an mmap slice, reads go
// straight into it and no pooled buffer is acquired.
func (d *Downloader) pumpBody(body io.Reader, f fileWriter, ws *workerState, start, end int64, strict bool) (int64, error) {
	direct := mmapBytes(f)
	if end >= 0 && direct != nil && end+1 > int64(len(direct)) {
		return 0, fmt.Errorf("mmap slice short: end=%d len=%d", end, len(direct))
	}
	manifest := d.manifest
	bufCap := int64(d.bufSize)
	cursor := start
	var buf []byte
	if direct == nil {
		buf = acquireBuf(d.bufSize)
		defer releaseBuf(buf)
	}
	for {
		if end >= 0 && cursor > end {
			return cursor - start, nil
		}
		// Pick the read destination: zero-copy into the mmap slice (pre-
		// clamped to file/range bounds) or a pooled buffer (clamped after
		// the read to the range).
		var dest []byte
		if direct != nil {
			remaining := int64(len(direct)) - cursor
			if remaining <= 0 {
				return cursor - start, nil
			}
			want := bufCap
			if want > remaining {
				want = remaining
			}
			if end >= 0 {
				if rem := end - cursor + 1; want > rem {
					want = rem
				}
			}
			dest = direct[cursor : cursor+want]
		} else {
			dest = buf
		}
		n, rerr := body.Read(dest)
		if n > 0 {
			d.rateLimit.wait(n)
			if direct == nil && end >= 0 {
				if remaining := end - cursor + 1; int64(n) > remaining {
					n = int(remaining)
				}
			}
			if direct == nil {
				if _, err := f.WriteAt(dest[:n], cursor); err != nil {
					return cursor - start, err
				}
			}
			cursor += int64(n)
			if ws != nil {
				ws.bytesDone.Store(cursor - start)
			}
			if manifest != nil {
				if err := manifest.VerifyChunk(cursor-int64(n), cursor-1, dest[:n]); err != nil {
					return cursor - start, err
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				if strict && end >= 0 && cursor <= end {
					// Wrap io.ErrUnexpectedEOF so isTransient() (which
					// tests errors.Is(err, io.ErrUnexpectedEOF)) treats a
					// server that closed mid-range as retryable and the
					// worker requeues the range instead of fatally
					// aborting the download. A plain fmt.Errorf here loses
					// the transient classification.
					return cursor - start, fmt.Errorf("server closed connection mid-range: got %d of expected %d bytes: %w",
						cursor-start, end-start+1, io.ErrUnexpectedEOF)
				}
				return cursor - start, nil
			}
			return cursor - start, rerr
		}
	}
}

// verifyTaskRange checks any fully-contained manifest chunks for the
// completed task. Mmap path is zero-copy; raw path reads via WriteAt-
// compatible SectionReader-style ReadAt when available.
func (d *Downloader) verifyTaskRange(task Task, f fileWriter) error {
	if d.manifest == nil {
		return nil
	}
	size := task.Len()
	if size <= 0 {
		return nil
	}
	if data := mmapBytes(f); data != nil {
		if task.End+1 > int64(len(data)) {
			return fmt.Errorf("manifest: task %d-%d past mmap len %d", task.Start, task.End, len(data))
		}
		return d.manifest.VerifyRange(task.Start, task.End, data[task.Start:task.End+1])
	}
	// Non-mmap: re-read the span for VerifyRange when the writer is a
	// raw file (os.File.ReadAt).
	type readerAt interface {
		ReadAt([]byte, int64) (int, error)
	}
	ra, ok := f.(readerAt)
	if !ok {
		return nil // defer to VerifyFull
	}
	buf := make([]byte, size)
	if _, err := ra.ReadAt(buf, task.Start); err != nil {
		return fmt.Errorf("manifest: re-read task %d-%d: %w", task.Start, task.End, err)
	}
	return d.manifest.VerifyRange(task.Start, task.End, buf)
}

// bufSizeLarge is the pooled read-buffer capacity. Requests larger than
// this allocate a fresh slice (not returned to the pool).
const bufSizeLarge = 256 * 1024

// bufPool recycles per-worker read buffers at bufSizeLarge.
var bufPool = sync.Pool{
	New: func() any { b := make([]byte, bufSizeLarge); return &b },
}

// acquireBuf returns a buffer of length n. Buffers up to bufSizeLarge are
// served from the pool; larger requests allocate a fresh (non-pooled) slice
// the caller releases by simply letting it be GC'd.
func acquireBuf(n int) []byte {
	if n <= 0 {
		return nil
	}
	if n <= bufSizeLarge {
		bp := bufPool.Get().(*[]byte)
		if cap(*bp) < n {
			// Undersized pool entry (should not happen with our New) —
			// put it back and allocate fresh so we never leak the slot.
			bufPool.Put(bp)
			return make([]byte, n)
		}
		return (*bp)[:n]
	}
	return make([]byte, n)
}

// releaseBuf returns a buffer to the pool only when capacity is exactly
// bufSizeLarge so the next acquireBuf never sees a short slice.
func releaseBuf(b []byte) {
	if cap(b) != bufSizeLarge {
		return
	}
	bp := b[:bufSizeLarge:bufSizeLarge]
	bufPool.Put(&bp)
}
