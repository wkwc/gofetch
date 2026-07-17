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
func (d *Downloader) singleDownload(ctx context.Context, total int64, completed []Task) error {
	f, err := allocateFileWriter(d.outFile, total, d.resumeEnabled)
	if err != nil {
		return err
	}
	defer f.Close()

	states := []*workerState{newWorkerState()}
	prog := newProgress(total, states)

	// Pre-fill completed bytes so snapshot returns the right total.
	var doneBytes int64
	for _, t := range completed {
		doneBytes += t.Len()
	}
	states[0].bytesDone.Store(doneBytes)

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
	defer drainAndClose(resp.Body)

	buf := acquireBuf(d.bufSize)
	defer releaseBuf(buf)
	ws := states[0]
	var cursor int64
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], cursor); werr != nil {
				return werr
			}
			cursor += int64(n)
			ws.bytesDone.Store(doneBytes + cursor)
			if d.manifest != nil {
				if err := d.manifest.VerifyChunk(cursor-int64(n), cursor-1, buf[:n]); err != nil {
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
	}
	return d.finalize(f, prog)
}
