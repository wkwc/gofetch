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

	// Zero-copy fast path when the file is mmap'd.
	if wb, ok := f.(mmapWriterBytes); ok {
		if data := wb.Bytes(); data != nil {
			return d.singleMmap(resp.Body, ws, data, total)
		}
	}

	defer drainAndClose(resp.Body)

	buf := acquireBuf(d.bufSize)
	defer releaseBuf(buf)
	manifest := d.manifest
	// Seed cursor from any resumed bytes (typically zero here since a
	// non-ranged server can't resume, but prog already accounts for them).
	doneBytes, _ := prog.snapshot()
	var cursor int64 = doneBytes
	for {
		n, rerr := resp.Body.Read(buf)
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
	return d.finalize(f, prog)
}

// singleMmap streams the body straight into the mmap'd output slice.
// Bytes from any previously-completed resume (preDone) are skipped by
// seeking the body past them rather than trusting `bytesDone` (which
// is reset() below) or `prog.snapshot` (which reflects seeded bytes).
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
				return nil
			}
			return rerr
		}
	}
}
