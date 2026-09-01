package fetch

import (
	"math"
	"math/rand/v2"
	"net/http"
	"runtime"
	"time"
)

// AutoConfig holds all auto-tuned download parameters.
// Users never set these directly; AutoConfigure computes them.
type AutoConfig struct {
	Workers   int
	BufSize   int
	RetryMax  int
	RetryBase time.Duration
	RetryCap  time.Duration
	Timeout   time.Duration
}

// AutoConfigure computes optimal parameters for the given file size hint.
// fileSizeHint may be 0 (unknown).
func AutoConfigure(fileSizeHint int64) AutoConfig {
	return AutoConfig{
		Workers:   scaleWorkers(fileSizeHint, runtime.NumCPU()),
		BufSize:   scaleBufSize(fileSizeHint),
		RetryMax:  10,
		RetryBase: 100 * time.Millisecond,
		RetryCap:  30 * time.Second,
		Timeout:   10 * time.Second,
	}
}

// scaleWorkers scales worker count based on file size hint and CPU count.
// Small files don't benefit from many workers (overhead > speedup).
// Large files benefit from high concurrency, capped at 32 to keep the
// transport's MaxIdleConnsPerHost consistent.
func scaleWorkers(totalSize int64, cores int) int {
	if totalSize <= 0 {
		w := cores + 2
		return min(max(w, 4), 12)
	}
	// Aim for ~8 chunks per worker so workers stay busy without
	// idle spin on giant files.
	chunkCount := totalSize / (1 << 20)
	if chunkCount <= 0 {
		chunkCount = 1
	}
	w := int(chunkCount) / 8
	// Floor at a modest minimum so tiny files still use a few workers.
	// On single-CPU hosts (CI sandboxes, cgroup-pinned containers)
	// cores/2 == 0, so max(1, cores/2) prevents the floor from
	// collapsing to zero workers (and a misleading "workers: 0" report).
	minW := max(1, cores/2)
	if w < minW {
		return max(4, minW)
	}
	if w > 32 {
		return 32
	}
	return w
}

// scaleBufSize picks a buffer size per range request, sized to amortize
// syscall overhead. Bigger buffers mean fewer syscalls and HTTP round-trips
// per MiB but use more memory per worker.
func scaleBufSize(totalSize int64) int {
	switch {
	case totalSize <= 0:
		return 64 * 1024
	case totalSize < 1<<20:
		return 32 * 1024
	case totalSize < 100<<20:
		return 64 * 1024
	default:
		return 256 * 1024
	}
}

// Backoff computes the sleep duration for retry n (0-indexed).
// Exponential growth, capped at RetryCap, with up to 25% upward jitter.
func (ac *AutoConfig) Backoff(n int) time.Duration {
	exp := math.Pow(2, float64(n))
	d := time.Duration(float64(ac.RetryBase) * exp)
	if d > ac.RetryCap || d < 0 {
		d = ac.RetryCap
	}
	return d + time.Duration(rand.N(int64(d/4)+1))
}

// Retune re-derives Workers and BufSize from the true total once known.
func (ac *AutoConfig) Retune(totalSize int64) {
	if totalSize <= 0 {
		return
	}
	ac.BufSize = scaleBufSize(totalSize)
	ac.Workers = scaleWorkers(totalSize, runtime.NumCPU())
}

// TCP_NOTSENT_LOWAT (0x19) is not exported by the linux syscall
// package as of Go 1.26. The previous 0x17 was TCP_FASTOPEN on
// every arch, so we were setting FASTOPEN twice and never touching
// NOTSENT_LOWAT. Defined here so future kernels can adopt these
// without losing our perf-tuning; on unsupported kernels the
// syscall is silently a no-op via the SetsockoptInt return.
const (
	tcpFastOpen      = 23
	tcpNotSentLowat  = 25
	tcpNotSentLowatV = 131072
)

// newAutoTransport builds an http.Transport with TCP keepalive,
// proxy detection from environment, and optimized socket options
// (TCP_NODELAY, TCP_FASTOPEN, TCP_NOTSENT_LOWAT, faster keepalive).
//
// DialContextAuto pins each target dial to a non-private resolved IP
// (DNS-rebinding resistant). Proxy hosts from HTTP(S)_PROXY/ALL_PROXY
// are trusted operator config and may be private (e.g. 127.0.0.1).
func newAutoTransport(ac AutoConfig) *http.Transport {
	maxIdlePerHost := max(ac.Workers, 4)
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           DialContextAuto,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   maxIdlePerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: ac.Timeout,
		ReadBufferSize:        ac.BufSize,
		WriteBufferSize:       ac.BufSize,
	}
	return tr
}
