package fetch

import (
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"runtime"
	"time"
)

// AutoConfig holds all auto-tuned download parameters.
// Users never set these directly; AutoConfigure computes them.
type AutoConfig struct {
	Workers   int
	BufSize   int
	RetryMax  int           // per-chunk retry cap; 0 = unlimited
	RetryBase time.Duration // starting backoff
	RetryCap  time.Duration // maximum backoff
	Timeout   time.Duration // per-request connect+read timeout
	Compress  bool          // request gzip compression
}

// AutoConfigure computes optimal parameters for the given file size hint.
// fileSizeHint may be 0 (unknown).
func AutoConfigure(fileSizeHint int64) AutoConfig {
	cores := runtime.NumCPU()
	workers := cores * 2
	if workers < 4 {
		workers = 4
	}
	if workers > 16 {
		workers = 16
	}

	bufSize := 64 * 1024
	if fileSizeHint > 100<<20 {
		bufSize = 256 * 1024
	}

	return AutoConfig{
		Workers:   workers,
		BufSize:   bufSize,
		RetryMax:  10, // 10 retries per chunk before giving up
		RetryBase: 100 * time.Millisecond,
		RetryCap:  30 * time.Second,
		Timeout:   10 * time.Second,
		Compress:  false, // binary downloads — gzip offers nothing useful and corrupts Range responses
	}
}

// Backoff computes the sleep duration for retry n (0-indexed).
// Exponential growth, capped at RetryCap, with up to 25% upward jitter.
func (ac *AutoConfig) Backoff(n int) time.Duration {
	exp := math.Pow(2, float64(n))
	d := time.Duration(float64(ac.RetryBase) * exp)
	if d > ac.RetryCap {
		d = ac.RetryCap
	}
	jitter := rand.N(int64(d/4 + 1))
	return d + time.Duration(jitter)
}

// newAutoTransport builds an http.Transport with TCP keepalive and
// proxy detection from environment.
func newAutoTransport(ac AutoConfig) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   ac.Timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: ac.Timeout,
	}
}

// Retune updates Workers and BufSize based on actual file size.
// Call after probe when total size is known.
func (ac *AutoConfig) Retune(totalSize int64) {
	if totalSize > 100<<20 && ac.BufSize < 256*1024 {
		ac.BufSize = 256 * 1024
	}
	if totalSize < 1<<20 && ac.Workers > 4 {
		ac.Workers = 4
	}
	// Note: transport not recreated (keepalive preserved)
}
