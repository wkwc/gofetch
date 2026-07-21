package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// singleDownload is used when the server does not support Range:
// one worker streams the entire body at offset 0 without Range headers.
// The file writer f is managed by the caller (already open, will be closed by caller).
func (d *Downloader) singleDownload(ctx context.Context, total int64, completed []Task, f fileWriter) error {
	ws := newWorkerState()
	prog := newProgress(total, []*workerState{ws})
	seedResumeBytes(prog, completed)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		drainAndClose(resp.Body)
		return fmt.Errorf("GET %s: status %d", d.url, resp.StatusCode)
	}

	if enc := resp.Header.Get("Content-Encoding"); !isIdentity(enc) {
		drainAndClose(resp.Body)
		return fmt.Errorf("GET %s: unexpected Content-Encoding %q", d.url, enc)
	}

	// Idle-timeout the body so a stalled single-stream GET cannot hang
	// forever after headers (range workers already use idleBody).
	idle := newIdleBody(ctx, resp.Body, defaultBodyIdle)

	// Zero-copy fast path when the file is mmap'd.
	if wb, ok := f.(mmapWriterBytes); ok {
		if data := wb.Bytes(); data != nil {
			return d.singleMmap(idle, ws, data, total)
		}
	}

	defer drainAndClose(idle)

	buf := acquireBuf(d.bufSize)
	defer releaseBuf(buf)
	manifest := d.manifest
	// Non-range GET always delivers the full object from byte 0. Never
	// offset the write cursor by resumed bytes — that splices body[0]
	// onto file[done] and corrupts the output. If we already have
	// progress, ignore it and rewrite from the start.
	if len(completed) > 0 {
		d.vlog("single-stream: ignoring %d completed ranges (server has no ranges)", len(completed))
	}
	var cursor int64
	for {
		n, rerr := idle.Read(buf)
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], cursor); werr != nil {
				return werr
			}
			cursor += int64(n)
			ws.bytesDone.Store(cursor)
			if manifest != nil {
				if err := manifest.VerifyChunk(cursor-int64(n), cursor-1, buf[:n]); err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return rerr
		}
		if total > 0 && cursor >= total {
			break
		}
	}
	if total > 0 && cursor < total {
		return fmt.Errorf("server closed connection early: got %d of expected %d bytes", cursor, total)
	}
	// Unknown-size downloads: drop any stale tail from a larger prior file.
	if total <= 0 && cursor >= 0 {
		if err := f.Truncate(cursor); err != nil {
			return fmt.Errorf("truncate to %d: %w", cursor, err)
		}
	}
	// Download owns finalize (Sync/Close/hash); do not double-close here.
	_ = prog
	return nil
}

// singleMmap streams the body straight into the mmap'd output slice.
// `body` is typically an idleBody wrapping the HTTP response body.
func (d *Downloader) singleMmap(body io.ReadCloser, ws *workerState, data []byte, total int64) error {
	defer drainAndClose(body)
	ws.reset(Task{Start: 0, End: total - 1})

	manifest := d.manifest
	bufCap := int64(d.bufSize)
	cursor := int64(0)
	for {
		remaining := int64(len(data)) - cursor
		if remaining <= 0 {
			return nil
		}
		want := bufCap
		if want > remaining {
			want = remaining
		}
		buf := data[cursor : cursor+want]
		n, rerr := body.Read(buf)
		if n > 0 {
			cursor += int64(n)
			ws.bytesDone.Store(cursor)
			if manifest != nil {
				if err := manifest.VerifyChunk(cursor-int64(n), cursor-1, buf[:n]); err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				if total > 0 && cursor < total {
					return fmt.Errorf("server closed connection early: got %d of expected %d bytes", cursor, total)
				}
				return nil
			}
			return rerr
		}
	}
}
