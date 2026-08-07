// Package fetch implements a streaming, range-aware HTTP downloader
// with adaptive work-stealing, multi-mirror support, resume capability,
// and integrity verification.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Downloader drives the parallel download of a single URL into a single file.
type Downloader struct {
	url           string
	mirrors       []string
	outFile       string
	workersN      int
	bufSize       int
	resumeEnabled bool
	quiet         bool
	verbose       bool

	client       *http.Client
	autoConfig   AutoConfig
	totalSize    int64
	hashAlgo     string
	expectedHash string
	resumePath   string
	manifest     *Manifest
	startTime    time.Time

	lastResumeSave atomic.Int64
	retryMu        sync.Mutex
	retryCount     map[[2]int64]int

	// completed accumulates finished ranges for resume sidecars.
	// Seeded from loadResume; updated on every successful task so
	// worker.reset() cannot drop progress before the next save.
	completedMu sync.Mutex
	completed   []Task

	// workerStates holds live state for each worker so we can
	// persist in-progress progress to the resume sidecar.
	workerStates []*workerState
}

// Options configures NewDownloader. Zero values enable auto-optimization.
// Only set fields you care about — everything else is auto-tuned.
type Options struct {
	NoResume     bool
	HashAlgo     string
	ExpectedHash string
	Verbose      bool
	Quiet        bool
	Mirrors      []string
}

// NewDownloader constructs a Downloader with auto-configured defaults.
func NewDownloader(rawURL, outPath string, opt Options) *Downloader {
	ac := AutoConfigure(0)
	d := &Downloader{
		url:           rawURL,
		mirrors:       opt.Mirrors,
		outFile:       outPath,
		workersN:      ac.Workers,
		bufSize:       ac.BufSize,
		resumeEnabled: !opt.NoResume,
		quiet:         opt.Quiet,
		verbose:       opt.Verbose,
		autoConfig:    ac,
		hashAlgo:      opt.HashAlgo,
		expectedHash:  opt.ExpectedHash,
	}
	// Only set resumePath when resume is enabled so saves/tickers
	// and sidecar cleanup stay fully disabled under --no-resume.
	if d.resumeEnabled {
		d.resumePath = resumePath(outPath)
	}
	// Client.Timeout must be 0: it covers the entire body transfer and
	// would kill multi-MB downloads after a few seconds. Per-phase
	// limits live on the Transport (dial / TLS / response headers);
	// overall deadline is the caller's context.
	d.client = &http.Client{
		Timeout:       0,
		Transport:     newAutoTransport(ac),
		CheckRedirect: CheckRedirectSafe,
	}
	return d
}

// smallFileThreshold is the file size below which parallel range downloads
// don't help: startup overhead dominates. Use single-stream instead.
const smallFileThreshold = 64 * 1024

// Download attempts each URL in order, falling over to mirrors on error.
// On success it calls finalize with the active progress tracker and file
// writer; on failure it returns the most-recent error.
func (d *Downloader) Download(ctx context.Context) error {
	origURL := d.url
	urls := append([]string{origURL}, d.mirrors...)
	var lastErr error

	for i, activeURL := range urls {
		if i > 0 {
			d.vlog("mirror %d/%d failed (%v), trying mirror %d/%d: %s",
				i, len(urls), lastErr, i+1, len(urls), activeURL)
			// Reset retry counters when failing over to a fresh mirror:
			// the per-range budget should not be shared across mirrors
			// (a range that exhausted retries against mirror 1 deserves
			// a full retry budget against the next probed mirror).
			d.retryMu.Lock()
			d.retryCount = nil
			d.retryMu.Unlock()
		}
		d.startTime = time.Now()

		info, err := d.probeURL(ctx, activeURL)
		if err != nil {
			lastErr = fmt.Errorf("mirror %d (%s) probe: %w", i+1, activeURL, err)
			continue
		}

		// If we carried completed ranges from a failed mirror, wipe them
		// when the new probe proves a different size (cannot reuse bytes).
		if d.resumeEnabled && info.total > 0 && d.totalSize > 0 && info.total != d.totalSize {
			d.vlog("size changed %d → %d; discarding progress", d.totalSize, info.total)
			_ = os.Truncate(d.outFile, 0)
			d.seedCompleted(nil)
			_ = clearResume(d.resumePath)
		}

		d.totalSize = info.total
		d.autoConfig.Retune(info.total)
		d.workersN = d.autoConfig.Workers
		d.bufSize = d.autoConfig.BufSize
		d.vlog("ranges=%v total=%s", info.supportsRanges, humanBytes(info.total))

		var completed []Task
		if d.resumeEnabled {
			st, err := loadResume(d.resumePath, activeURL, info.total)
			if err != nil {
				d.vlog("corrupt resume file, restarting from scratch: %v", err)
				_ = clearResume(d.resumePath)
				// Preserve in-memory progress from same-size mirror failover.
				completed = d.snapshotCompleted()
			} else if st != nil {
				completed = st.Completed
				// Promote partial in-progress bytes into completed so
				// uncompleted() skips already-written spans. The leftover
				// [Start+Done, End] stays out of completed and is re-seeded.
				// (Older code inverted this and marked the undownloaded
				// remainder as done — permanent holes on resume.)
				if st.InProgress != nil && st.InProgressDone > 0 {
					writtenEnd := st.InProgress.Start + st.InProgressDone - 1
					if writtenEnd >= st.InProgress.Start && writtenEnd <= st.InProgress.End {
						completed = append(completed, Task{
							Start: st.InProgress.Start,
							End:   writtenEnd,
						})
						d.vlog("resuming in-progress range %d-%d (done %d bytes)",
							st.InProgress.Start, st.InProgress.End, st.InProgressDone)
					}
				}
				completed = dedupTasks(completed)
				d.seedCompleted(completed)

				// Inherit the hash algo+value that was active when
				// the sidecar was written, so a sha512 download
				// surviving a process restart verifies with the
				// right algorithm (we never persist just the digest).
				if st.HashAlgo != "" && d.hashAlgo == "" {
					d.hashAlgo = st.HashAlgo
				}
				if st.ExpectedHash != "" && d.expectedHash == "" {
					d.expectedHash = st.ExpectedHash
				}
				d.vlog("resumed from %d completed chunks (algo=%s)", len(completed), d.hashAlgo)
			} else {
				// URL mismatch (typical on mirror failover): the on-disk
				// sidecar is for a different URL, so we look at in-memory
				// completed ranges carried from a prior attempt.
				//
				// Two mirrors that report the *same* size may carry
				// *different* bytes (a stale snapshot, a misconfigured
				// mirror, a same-length man-in-the-middle). Without a
				// per-chunk manifest to vouch for the bytes a prior
				// mirror wrote, reusing completed ranges would silently
				// splice bytes from two different files into the output
				// and only show up as a final whole-file hash mismatch.
				// Per-chunk verification during the next download
				// (worker.go VerifyChunk) is only consulted when a
				// manifest is loaded — so gate reuse on its presence.
				if d.manifest != nil {
					completed = d.snapshotCompleted()
					if len(completed) > 0 {
						d.vlog("reusing %d in-memory completed ranges for mirror (manifest vouches)", len(completed))
					}
				} else {
					n := len(d.snapshotCompleted())
					completed = nil
					d.seedCompleted(nil)
					if n > 0 {
						d.vlog("mirror switch without manifest; discarded %d in-memory ranges to avoid cross-mirror splicing", n)
					}
				}
			}
		}

		f, err := allocateFileWriter(d.outFile, info.total, d.resumeEnabled)
		if err != nil {
			lastErr = fmt.Errorf("mirror %d (%s) file setup: %w", i+1, activeURL, err)
			continue
		}

		// Each mirror iteration opens a fresh file writer. Close it
		// explicitly before the next attempt instead of using defer,
		// which would leak file descriptors for earlier mirrors until
		// the entire function returns.
		err = d.downloadFromMirror(ctx, activeURL, info, completed, f)
		if err == nil {
			d.url = origURL
			// Single ownership of Sync/Close/hash: leaves never finalize.
			return d.finalize(f, nil)
		}
		_ = f.Close()
		lastErr = fmt.Errorf("mirror %d (%s) failed: %w", i+1, activeURL, err)
		// User-initiated cancel (Ctrl-C / timeout): the cancel path in
		// range.go already flushed a durable sidecar via maybeSaveResume(true).
		// Do NOT clear it, do NOT fail over to the next mirror, and do NOT
		// delete the partial output — the user asked to stop, not to retry.
		// Returning here leaves the resume sidecar intact so the next
		// invocation resumes from the flushed progress.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return lastErr
		}
		// Keep on-disk bytes + completed ranges until the *next* iteration
		// successfully probes and proves a size mismatch. Pre-probing the
		// next URL here doubles RTT and can spuriously wipe on transient
		// probe failure. Wipe only when we know we will not keep progress.
		if !d.resumeEnabled {
			_ = os.Remove(d.outFile)
			continue
		}
		// Clear URL-keyed resume sidecar; in-memory completed survives
		// for same-size failover. Size mismatch is handled after the
		// next successful probe (below, at loop top via seed logic).
		// NOTE: a genuine same-size mirror MUST NOT reuse completed
		// ranges across a *different host* — see resetCompletedForHost
		// below, which discards the accumulator unless a per-chunk
		// manifest can vouch for the bytes.
		_ = clearResume(d.resumePath)
		d.vlog("mirror failed; keeping %d completed ranges pending next probe",
			len(d.snapshotCompleted()))
	}
	return lastErr
}

// downloadFromMirror attempts to download from a single URL using either
// range or single-stream mode.
func (d *Downloader) downloadFromMirror(ctx context.Context, activeURL string, info probeInfo, completed []Task, f fileWriter) error {
	// Temporarily set d.url so runTask/workerLoop use the active mirror.
	savedURL := d.url
	d.url = activeURL
	defer func() { d.url = savedURL }()

	if !info.supportsRanges || (info.total > 0 && info.total < smallFileThreshold) {
		return d.singleDownload(ctx, info.total, completed, f)
	}
	return d.rangeDownload(ctx, info.total, completed, f)
}
