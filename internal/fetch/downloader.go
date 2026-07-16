// Package fetch implements a streaming, range-aware HTTP downloader
// with adaptive work-stealing, multi-mirror support, resume capability,
// and integrity verification.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Downloader drives the parallel download of a single URL into a single file.
type Downloader struct {
	url           string
	outFile       string
	workersN      int
	bufSize       int
	resumeEnabled bool
	quiet         bool
	verbose       bool

	client       *http.Client
	autoConfig   AutoConfig
	totalSize    int64
	hashAlgo     string // "sha256" or "sha512"
	expectedHash string
	resumePath   string
	manifest     *Manifest
	startTime    time.Time

	lastResumeSave atomic.Int64
	retryMu        sync.Mutex
	retryCount     map[int64]int
}

// Options configures NewDownloader. Zero values enable auto-optimization.
// Only set fields you care about — everything else is auto-tuned.
type Options struct {
	NoResume     bool
	HashAlgo     string // "sha256" or "sha512"; empty = sha256
	ExpectedHash string // hex hash to verify; empty = skip
	Verbose      bool
	Quiet        bool
}

// NewDownloader constructs a Downloader with auto-configured defaults.
func NewDownloader(rawURL, outPath string, opt Options) *Downloader {
	ac := AutoConfigure(0)

	algo := opt.HashAlgo
	if algo == "" {
		algo = "sha256"
	}

	d := &Downloader{
		url:           rawURL,
		outFile:       outPath,
		workersN:      ac.Workers,
		bufSize:       ac.BufSize,
		resumeEnabled: !opt.NoResume,
		quiet:         opt.Quiet,
		verbose:       opt.Verbose,
		autoConfig:    ac,
		hashAlgo:      algo,
		expectedHash:  opt.ExpectedHash,
		resumePath:    resumePath(outPath),
	}
	d.client = &http.Client{
		Timeout:   ac.Timeout,
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

// Download fetches d.url into d.outFile.
// Returns nil on success, or the first worker error.
func (d *Downloader) Download(ctx context.Context) error {
	d.startTime = time.Now()

	info, err := d.probeURL(ctx, d.url)
	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}
	d.totalSize = info.total
	d.autoConfig.Retune(info.total)
	d.workersN = d.autoConfig.Workers
	d.bufSize = d.autoConfig.BufSize
	d.vlog("ranges=%v total=%s", info.supportsRanges, humanBytes(info.total))

	var completed []Task
	if d.resumeEnabled {
		if st, _ := loadResume(d.resumePath, d.url, info.total); st != nil {
			completed = st.Completed
			sortByStart(completed)
			d.vlog("resumed from %d completed chunks", len(completed))
		}
	}

	// Single stream is faster for tiny files. The work-stealing monitor
	// and worker coordination cost more than parallel speedup.
	if !info.supportsRanges || (info.total > 0 && info.total < smallFileThreshold) {
		return d.singleDownload(ctx, info.total, completed)
	}
	return d.rangeDownload(ctx, info.total, completed)
}
