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
		Timeout:   0,
		Transport: newAutoTransport(ac),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("too many redirects")
			}
			return nil
		},
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
		}
		d.startTime = time.Now()

		info, err := d.probeURL(ctx, activeURL)
		if err != nil {
			lastErr = fmt.Errorf("mirror %d (%s) probe: %w", i+1, activeURL, err)
			continue
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
				clearResume(d.resumePath)
				d.seedCompleted(nil)
			} else if st != nil {
				completed = st.Completed
				sortByStart(completed)
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
				d.seedCompleted(nil)
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
		downloaded := d.downloadFromMirror(ctx, activeURL, info, completed, f)
		if downloaded.ok() {
			d.url = origURL
			// Single ownership of Sync/Close/hash: leaves never finalize.
			return d.finalize(f, nil)
		}
		_ = f.Close()

		lastErr = fmt.Errorf("mirror %d (%s) failed: %w", i+1, activeURL, downloaded.err)
		// Mirror failed: the file may contain partial/corrupt data from
		// this failed attempt. Truncate to 0 so the next mirror attempt
		// starts fresh. Resume sidecars are URL-keyed so progress from
		// this mirror cannot be reused on a different URL anyway.
		if d.resumeEnabled {
			_ = os.Truncate(d.outFile, 0)
			d.seedCompleted(nil)
			clearResume(d.resumePath)
		} else {
			os.Remove(d.outFile)
		}
	}
	return lastErr
}

// downloadFromMirror attempts to download from a single URL using either
// range or single-stream mode. Returns (true, nil) on success.
func (d *Downloader) downloadFromMirror(ctx context.Context, activeURL string, info probeInfo, completed []Task, f fileWriter) mirrorResult {
	// Temporarily set d.url so runTask/workerLoop use the active mirror.
	savedURL := d.url
	d.url = activeURL
	defer func() { d.url = savedURL }()

	var err error
	if !info.supportsRanges || (info.total > 0 && info.total < smallFileThreshold) {
		err = d.singleDownload(ctx, info.total, completed, f)
	} else {
		err = d.rangeDownload(ctx, info.total, completed, f)
	}
	return mirrorResult{err: err}
}

// mirrorResult carries the outcome of a single mirror attempt.
type mirrorResult struct {
	err error
}

func (r mirrorResult) ok() bool { return r.err == nil }
