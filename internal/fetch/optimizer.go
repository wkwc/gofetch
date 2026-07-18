package fetch

import (
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"runtime"
	"syscall"
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
	Compress  bool
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
		// Range downloads never request compression: a server that
		// gzips a Range response emits encoded-byte counts at the
		// transport layer which misalign with the requested offsets.
		Compress: false,
	}
}

// scaleWorkers scales worker count based on file size hint and CPU count.
// Small files don't benefit from many workers (overhead > speedup).
// Large files benefit from high concurrency, capped at 32 to keep the
// transport's MaxIdleConnsPerHost consistent.
func scaleWorkers(totalSize int64, cores int) int {
	if totalSize <= 0 {
		w := cores + 2
		return clampInt(w, 4, 12)
	}
	// Aim for ~8 chunks per worker so workers stay busy without
	// idle spin on giant files.
	chunkCount := totalSize / (1 << 20)
	if chunkCount <= 0 {
		chunkCount = 1
	}
	w := int(chunkCount) / 8
	if w < cores/2 {
		return max(4, cores/2)
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

// tcpFastOpen and tcpNotSentLowat are not exported by the linux syscall
// package as of Go 1.26. They are defined here so future kernels can
// adopt them without losing our perf-tuning; on unsupported kernels
// the syscall is silently a no-op via the SetsockoptInt return.
const (
	tcpFastOpen      = 23
	tcpNotSentLowat  = 0x17
	tcpNotSentLowatV = 131072
)

// newAutoTransport builds an http.Transport with TCP keepalive,
// proxy detection from environment, and optimized socket options
// (TCP_NODELAY, TCP_FASTOPEN, TCP_NOTSENT_LOWAT, faster keepalive).
func newAutoTransport(ac AutoConfig) *http.Transport {
	maxIdlePerHost := max(ac.Workers, 4)
	dialer := &net.Dialer{
		Timeout:   ac.Timeout,
		KeepAlive: 10 * time.Second,
	}
	dialer.Control = func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
			// TCP_FASTOPEN (Linux 4.11+) — ignored if unavailable.
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpFastOpen, 1)
			// TCP_NOTSENT_LOWAT (Linux 4.15+) — wires kernel-side pacing.
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowat, tcpNotSentLowatV)
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 10)
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3)
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)
		})
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   maxIdlePerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: ac.Timeout,
		ReadBufferSize:        ac.BufSize,
		WriteBufferSize:       ac.BufSize,
	}
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
