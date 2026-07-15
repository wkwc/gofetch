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
	URL           string
	Mirrors       []string
	OutFile       string
	WorkersN      int
	BufSize       int
	Timeout       time.Duration
	UserAgent     string
	VerifyConfig  VerifyConfig
	ResumeEnabled bool

	client       *http.Client
	totalSize    int64
	expectedHash string // resolved from VerifyConfig.Expected at Download time
	resumePath   string
	startTime    time.Time

	retryMu        sync.Mutex
	retryCount     map[Task]int
	lastResumeSave atomic.Int64 // unix nano
}

// Options configures NewDownloader.
type Options struct {
	WorkerCount    int
	BufSize        int
	Timeout        time.Duration
	Mirrors        []string
	ExpectedSHA256 string // convenience: equivalent to VerifyConfig.Expected
	Resume         bool
	VerifyConfig   VerifyConfig
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
	d := &Downloader{
		URL:           rawURL,
		Mirrors:       append([]string{rawURL}, opt.Mirrors...),
		OutFile:       outPath,
		WorkersN:      opt.WorkerCount,
		BufSize:       opt.BufSize,
		Timeout:       opt.Timeout,
		UserAgent:     "gofetch/0.1",
		VerifyConfig:  opt.VerifyConfig,
		ResumeEnabled: opt.Resume,
		resumePath:    resumePath(outPath),
		expectedHash:  opt.ExpectedSHA256,
	}
	d.client = &http.Client{
		Timeout: opt.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        opt.WorkerCount * 2,
			MaxIdleConnsPerHost: opt.WorkerCount,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		},
	}
	return d
}

// Download fetches d.URL (with mirror failover) into d.OutFile.
// Returns nil on success, or the first worker error.
func (d *Downloader) Download(ctx context.Context) error {
	d.startTime = time.Now()

	mirror, info, err := d.selectMirror(ctx)
	if err != nil {
		return fmt.Errorf("mirror: %w", err)
	}
	d.URL = mirror.URL
	d.totalSize = info.total
	if d.VerifyConfig.Expected != "" {
		d.expectedHash = d.VerifyConfig.Expected
	}

	var completed []Task
	if d.ResumeEnabled {
		if st, _ := loadResume(d.resumePath, d.URL, info.total); st != nil {
			completed = sortByStart(st.Completed)
		}
	}
	if !info.supportsRanges {
		return d.singleDownload(ctx, info.total, completed)
	}
	return d.rangeDownload(ctx, info.total, completed)
}
