// Package fetch implements a streaming, range-aware HTTP downloader
// with adaptive work-stealing, multi-mirror support, resume capability,
// and integrity verification.
package fetch

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"sync"
	"time"
)

// Downloader drives the parallel download of a single URL into a single file.
type Downloader struct {
	url           string
	mirrors       []string
	outFile       string
	workerCount   int
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

	headers   []string
	userAgent string
	rateLimit *rateLimiter

	// workersSet/bufSet track explicit overrides so applyProbe's Retune
	// leaves user-provided values alone.
	workersSet bool
	bufSet     bool

	lastResumeSaveMu sync.Mutex
	lastResumeSave   time.Time
	retryMu          sync.Mutex
	retryCount       map[Task]int

	// completed accumulates finished ranges for resume sidecars.
	// Seeded from loadResume; updated on every successful task so
	// worker.reset() cannot drop progress before the next save.
	completedMu sync.Mutex
	completed   []Task
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
	// Headers are extra request headers ("Name: value") sent on every
	// request (probes, ranges, single-stream).
	Headers []string
	// RateLimit caps aggregate download throughput in bytes/second
	// (0 = unlimited). Applies per Downloader (per file).
	RateLimit int64
	// Proxy overrides the environment's HTTP(S)_PROXY / ALL_PROXY when set.
	Proxy string
	// UserAgent overrides the default gofetch User-Agent header.
	UserAgent string
	// Workers and BufSize are optional escape hatches. When > 0 they
	// override the auto-tuned values; 0 keeps auto-tuning.
	Workers int
	BufSize int
	// RetryMax overrides the per-chunk retry budget (transient errors and
	// retryable HTTP statuses, default 10). 0 keeps the default.
	RetryMax int
	// CACert is a path to a PEM file of extra root CAs to trust (private
	// or self-signed dataset mirrors). Empty uses the system pool.
	CACert string
}

// NewDownloader constructs a Downloader with auto-configured defaults.
func NewDownloader(rawURL, outPath string, opt Options) *Downloader {
	ac := AutoConfigure(0)
	// Optional escape hatches override the auto-tuned values.
	if opt.Workers > 0 {
		ac.Workers = opt.Workers
	}
	if opt.BufSize > 0 {
		ac.BufSize = opt.BufSize
	}
	if opt.RetryMax > 0 {
		ac.RetryMax = opt.RetryMax
	}
	ua := opt.UserAgent
	if ua == "" {
		ua = defaultUserAgent
	}
	d := &Downloader{
		url:           rawURL,
		mirrors:       opt.Mirrors,
		outFile:       outPath,
		workerCount:   ac.Workers,
		bufSize:       ac.BufSize,
		workersSet:    opt.Workers > 0,
		bufSet:        opt.BufSize > 0,
		resumeEnabled: !opt.NoResume,
		quiet:         opt.Quiet,
		verbose:       opt.Verbose,
		autoConfig:    ac,
		hashAlgo:      opt.HashAlgo,
		expectedHash:  opt.ExpectedHash,
		headers:       opt.Headers,
		userAgent:     ua,
		rateLimit:     newRateLimiter(opt.RateLimit),
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
		Transport:     newAutoTransport(ac, opt.Proxy, loadRootCAs(opt.CACert)),
		CheckRedirect: CheckRedirectSafe,
	}
	return d
}

// loadRootCAs returns a root pool that trusts the PEM file at path in
// addition to the system pool, or nil when path is empty or unreadable.
func loadRootCAs(path string) *x509.CertPool {
	if path == "" {
		return nil
	}
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pemData) {
		return nil
	}
	return pool
}

// smallFileThreshold is the file size below which parallel range downloads
// don't help: startup overhead dominates. Use single-stream instead.
const smallFileThreshold = 64 * 1024

// ProbeInfo describes what a download from a URL would do, without
// fetching the body. Reported by ProbeURL.
type ProbeInfo struct {
	URL            string
	Total          int64
	SupportsRanges bool
	Workers        int
	BufSize        int
}

// ProbeURL probes url (HEAD, falling back to a 1-byte range GET) and
// reports the announced size, range support, and the auto-tuned
// concurrency a download would use — without writing anything to disk.
func ProbeURL(ctx context.Context, rawURL string) (ProbeInfo, error) {
	d := NewDownloader(rawURL, "", Options{})
	info, err := d.probeURL(ctx, rawURL)
	if err != nil {
		return ProbeInfo{}, err
	}
	d.applyProbe(info)
	return ProbeInfo{
		URL:            rawURL,
		Total:          info.total,
		SupportsRanges: info.supportsRanges,
		Workers:        d.workerCount,
		BufSize:        d.bufSize,
	}, nil
}

// Download attempts each URL in order, falling over to mirrors on error.
// On success it calls finalize with the active progress tracker and file
// writer; on failure it returns the most-recent error.
func (d *Downloader) Download(ctx context.Context) error {
	urls := append([]string{d.url}, d.mirrors...)
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

		// applyProbe commits size + auto-tune and wipes progress if the
		// probe proves a different size than a prior mirror (cannot reuse bytes).
		d.applyProbe(info)

		// completed holds ranges to skip: from a matching on-disk sidecar,
		// or in-memory progress carried over from a failed mirror.
		completed := d.resolveResume(activeURL, info.total)

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
		// next successful probe (below, at loop top via applyProbe).
		// NOTE: a genuine same-size mirror MUST NOT reuse completed
		// ranges across a *different host* — resolveResume gates reuse
		// on a per-chunk manifest that can vouch for the bytes.
		_ = clearResume(d.resumePath)
		d.vlog("mirror failed; keeping %d completed ranges pending next probe",
			len(d.snapshotCompleted()))
	}
	return lastErr
}

// applyProbe records probe results: re-tunes auto-config and wipes
// accumulated progress if a mirror serves a different size than the
// one a prior mirror advertised (the bytes cannot be reused).
func (d *Downloader) applyProbe(info probeInfo) {
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
	if !d.workersSet {
		d.workerCount = d.autoConfig.Workers
	}
	if !d.bufSet {
		d.bufSize = d.autoConfig.BufSize
	}
	d.vlog("ranges=%v total=%s", info.supportsRanges, HumanBytes(info.total))
}

// resolveResume derives the seed ranges (completed) for a fresh mirror
// attempt from the on-disk sidecar (keyed to activeURL) plus any
// in-memory progress carried over from a prior failed mirror. It seeds
// the accumulator as a side effect. Returns nil when resume is disabled.
func (d *Downloader) resolveResume(activeURL string, total int64) []Task {
	if !d.resumeEnabled {
		return nil
	}
	st, err := loadResume(d.resumePath, activeURL, total)
	if err != nil {
		d.vlog("corrupt resume file, restarting from scratch: %v", err)
		_ = clearResume(d.resumePath)
		// Preserve in-memory progress from same-size mirror failover.
		return d.snapshotCompleted()
	}
	if st == nil {
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
			completed := d.snapshotCompleted()
			if len(completed) > 0 {
				d.vlog("reusing %d in-memory completed ranges for mirror (manifest vouches)", len(completed))
			}
			return completed
		}
		n := len(d.snapshotCompleted())
		d.seedCompleted(nil)
		if n > 0 {
			d.vlog("mirror switch without manifest; discarded %d in-memory ranges to avoid cross-mirror splicing", n)
		}
		return nil
	}

	completed := st.Completed
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
	return completed
}

// downloadFromMirror attempts to download from a single URL using either
// range or single-stream mode.
func (d *Downloader) downloadFromMirror(ctx context.Context, activeURL string, info probeInfo, completed []Task, f fileWriter) error {
	// Load per-chunk integrity manifest for both range and single-stream
	// paths (previously only rangeDownload loaded it).
	d.loadManifestIfPresent()

	if !info.supportsRanges || (info.total > 0 && info.total < smallFileThreshold) {
		return d.singleDownload(ctx, activeURL, info.total, completed, f)
	}
	err := d.rangeDownload(ctx, activeURL, info.total, completed, f)
	if errors.Is(err, errRangeNotSupported) {
		// Probe advertised ranges but the first Range GET returned 200.
		// Abort range mode and stream the whole object once. Do not treat
		// any partial range progress as complete — singleDownload rewrites
		// from byte 0 and ignores completed ranges.
		d.vlog("Range request returned 200 OK; falling back to single-stream")
		return d.singleDownload(ctx, activeURL, info.total, completed, f)
	}
	return err
}

// loadManifestIfPresent tries to load <outFile>.gofetch.manifest.
// ErrNotExist is the common "no manifest" case; any other error (corrupt
// JSON, unknown version) is logged so integrity is not silently dropped.
func (d *Downloader) loadManifestIfPresent() {
	if d.manifest != nil {
		return
	}
	manifestPath := d.outFile + ".gofetch.manifest"
	if m, err := LoadManifest(manifestPath); err == nil {
		d.manifest = m
		d.vlog("loaded manifest with %d chunks", len(m.Chunks))
	} else if !errors.Is(err, fs.ErrNotExist) {
		d.vlog("manifest load failed (skipping integrity check): %v", err)
	}
}
