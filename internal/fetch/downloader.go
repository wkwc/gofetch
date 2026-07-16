// Package fetch implements a streaming, range-aware HTTP downloader
// with adaptive work-stealing, multi-mirror support, resume capability,
// and SHA-256 integrity verification.
package fetch

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Downloader drives the parallel download of a single URL (or mirror
// set) into a single file.
type Downloader struct {
	url           string
	mirrors       []string
	outFile       string
	workersN      int
	bufSize       int
	timeout       time.Duration
	userAgent     string
	resumeEnabled bool

	client       *http.Client
	totalSize    int64
	expectedHash string // SHA-256 hex; empty = skip verification
	resumePath   string
	startTime    time.Time

	retryMu        sync.Mutex
	retryCount     map[Task]int
	lastResumeSave atomic.Int64
}

// Options configures NewDownloader.
type Options struct {
	WorkerCount    int
	BufSize        int
	Timeout        time.Duration
	Mirrors        []string
	ExpectedSHA256 string // hex-encoded SHA-256; empty skips verification
	Resume         bool
	UserAgent      string
}

// NewDownloader constructs a Downloader with sane defaults.
func NewDownloader(rawURL, outPath string, opt Options) *Downloader {
	if opt.WorkerCount < 1 {
		opt.WorkerCount = 4
	}
	if opt.BufSize < 1 {
		opt.BufSize = 64 * 1024
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 30 * time.Second
	}
	ua := opt.UserAgent
	if ua == "" {
		ua = "gofetch/0.1"
	}
	d := &Downloader{
		url:           rawURL,
		mirrors:       opt.Mirrors,
		outFile:       outPath,
		workersN:      opt.WorkerCount,
		bufSize:       opt.BufSize,
		timeout:       opt.Timeout,
		userAgent:     ua,
		resumeEnabled: opt.Resume,
		resumePath:    resumePath(outPath),
		expectedHash:  opt.ExpectedSHA256,
	}
	d.client = &http.Client{
		Timeout: opt.Timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: opt.WorkerCount + 1,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
	return d
}

// Download fetches d.url (with mirror failover) into d.outFile.
// Returns nil on success, or the first worker error.
func (d *Downloader) Download(ctx context.Context) error {
	d.startTime = time.Now()

	mirror, info, err := d.selectMirror(ctx)
	if err != nil {
		return fmt.Errorf("mirror: %w", err)
	}
	d.url = mirror.URL
	d.totalSize = info.total

	var completed []Task
	if d.resumeEnabled {
		if st, _ := loadResume(d.resumePath, d.url, info.total); st != nil {
			completed = sortByStart(st.Completed)
		}
	}
	if !info.supportsRanges {
		return d.singleDownload(ctx, info.total, completed)
	}
	return d.rangeDownload(ctx, info.total, completed)
}
