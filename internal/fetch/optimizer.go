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
	workers := scaleWorkers(fileSizeHint, cores)
	bufSize := scaleBufSize(fileSizeHint)

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

// scaleWorkers scales worker count based on file size hint and CPU count.
// Small files don't benefit from many workers (overhead > speedup).
// Large files benefit from high concurrency, capped at 32 to keep the
// transport's MaxIdleConnsPerHost consistent.
func scaleWorkers(totalSize int64, cores int) int {
	if totalSize <= 0 {
		// Unknown: assume "reasonable size", half the old heuristic.
		w := cores + 2
		if w < 4 {
			w = 4
		}
		if w > 12 {
			w = 12
		}
		return w
	}
	// 1 worker per chunk is wasteful. Aim for ~10-100 chunks per worker.
	// 1 MiB chunks: totalSize/1MiB = chunk count.
	chunkCount := int(totalSize / (1 << 20))
	if chunkCount <= 0 {
		chunkCount = 1
	}
	w := chunkCount / 8
	if w < 4 {
		return 4
	}
	if w > 32 {
		return 32
	}
	// Round up to a sensible CPU-aligned value
	switch {
	case w <= cores:
		return w
	case w <= cores*2:
		return cores * 2
	default:
		return min(w, 32)
	}
}

// scaleBufSize picks a buffer size per range request, sized to amortize
// syscall overhead. Bigger buffers mean fewer syscalls and HTTP round-trips
// per MiB but use more memory per worker.
func scaleBufSize(totalSize int64) int {
	switch {
	case totalSize <= 0:
		return 64 * 1024
	case totalSize < 1<<20: // < 1 MiB: keep small
		return 32 * 1024
	case totalSize < 100<<20: // 1..100 MiB
		return 64 * 1024
	default: // >= 100 MiB: bigger buffers
		return 256 * 1024
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

// newAutoTransport builds an http.Transport with TCP keepalive,
// proxy detection from environment, and optimized socket options.
func newAutoTransport(ac AutoConfig) *http.Transport {
	// Match MaxIdleConnsPerHost to the upper bound of Workers so
	// connection reuse can sustain our peak concurrency.
	maxIdlePerHost := ac.Workers
	if maxIdlePerHost < 4 {
		maxIdlePerHost = 4
	}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   ac.Timeout,
			KeepAlive: 10 * time.Second,
			Control: func(network, address string, c syscall.RawConn) error {
				return c.Control(func(fd uintptr) {
					// TCP_NODELAY: disable Nagle's algorithm for lower latency
					syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
					// TCP_FASTOPEN: enable fast open (Linux 4.11+, fallback silently on older kernels)
					syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, 23, 1) // TCP_FASTOPEN = 23
					// TCP_NOTSENT_LOWAT: reduce TCP bufferbloat (Linux 4.15+)
					syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, 0x17, 131072) // TCP_NOTSENT_LOWAT = 0x17
					// TCP_KEEPIDLE/INTVL/COUNT: faster dead peer detection
					syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 10)
					syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3)
					syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3)
				})
			},
		}).DialContext,
		ForceAttemptHTTP2:     true, // Enable HTTP/2 with graceful fallback to HTTP/1.1
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   maxIdlePerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: ac.Timeout,
		ReadBufferSize:        ac.BufSize,
		WriteBufferSize:       ac.BufSize,
	}
}

// Retune updates Workers and BufSize based on actual file size.
// Call after probe when total size is known.
func (ac *AutoConfig) Retune(totalSize int64) {
	if totalSize <= 0 {
		return
	}
	newBuf := scaleBufSize(totalSize)
	if ac.BufSize != newBuf {
		ac.BufSize = newBuf
	}
	newWorkers := scaleWorkers(totalSize, runtime.NumCPU())
	if ac.Workers != newWorkers {
		ac.Workers = newWorkers
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}