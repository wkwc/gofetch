package fetch

import (
	"context"
	"fmt"
	"net/http"
)

// singleDownload is used when the server does not support Range:
// one worker streams the entire body at offset 0 without Range headers.
// The file writer f is managed by the caller (already open, will be closed by caller).
//
// Per-event progress (the live progress bar) is intentionally not wired here;
// the caller threads progress through finalize() at the end. There is no
// parallel work to monitor, so no worker states are created on this path.
func (d *Downloader) singleDownload(ctx context.Context, url string, total int64, completed []Task, f fileWriter) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	// Match range path: never accept compressed bodies that would
	// desync Content-Length / integrity checks (transport also disables
	// transparent gzip, but proxies can still inject encoding).
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		drainAndClose(resp.Body)
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}

	if enc := resp.Header.Get("Content-Encoding"); !isIdentity(enc) {
		drainAndClose(resp.Body)
		return fmt.Errorf("GET %s: unexpected Content-Encoding %q", url, enc)
	}

	// Idle-timeout the body so a stalled single-stream GET cannot hang
	// forever after headers (range workers already use idleBody).
	idle := newIdleBody(ctx, resp.Body, defaultBodyIdle)
	defer drainAndClose(idle)

	// Non-range GET always delivers the full object from byte 0. Never
	// offset the write cursor by resumed bytes — that splices body[0]
	// onto file[done] and corrupts the output. If we already have
	// progress, ignore it and rewrite from the start.
	if len(completed) > 0 {
		d.vlog("single-stream: ignoring %d completed ranges (server has no ranges)", len(completed))
	}

	// end is total-1 when the size is known; pumpBody gets end=-1 for
	// unknown-size downloads so it streams to EOF without clamping.
	end := int64(-1)
	if total > 0 {
		end = total - 1
	}
	written, err := d.pumpBody(idle, f, nil, 0, end, false)
	if err != nil {
		return err
	}
	if total > 0 && written < total {
		return fmt.Errorf("server closed connection early: got %d of expected %d bytes", written, total)
	}
	// Unknown-size downloads: drop any stale tail from a larger prior file.
	// Invariant: any Truncate-to-smaller MUST be paired with a reset of the
	// resume accumulator (seedCompleted(nil) + clearResume), otherwise
	// `completed` keeps pointing at byte ranges past the new EOF and the
	// next run would silently skip the (now-missing) tail. Single-stream
	// downloads currently don't persist resume mid-stream, so dropping the
	// accumulator here is both safe and the documented contract for any
	// future caller that does (resumable single-stream is the obvious next
	// feature for the InProgress/InProgressDone sidecar fields).
	if total <= 0 {
		if err := f.Truncate(written); err != nil {
			return fmt.Errorf("truncate to %d: %w", written, err)
		}
		// The size was unknown until EOF; now that we know it, record it
		// so finalize reports the true byte count (not 0 B).
		d.totalSize = written
		if d.resumePath != "" {
			d.seedCompleted(nil)
			_ = clearResume(d.resumePath)
		}
	}
	// Download owns finalize (Sync/Close/hash); do not double-close here.
	return nil
}
