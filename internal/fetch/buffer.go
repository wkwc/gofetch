package fetch

import "sync"

// bufPool recycles per-worker read buffers.
// Entries larger than 1 MiB are too big to pool cheaply.
var bufPool = sync.Pool{
	New: func() any { b := make([]byte, 64*1024); return &b },
}

// acquireBuf returns a buffer of length n, drawing from the pool.
func acquireBuf(n int) []byte {
	bp := bufPool.Get().(*[]byte)
	if cap(*bp) < n {
		*bp = make([]byte, n)
		return *bp
	}
	return (*bp)[:n]
}

// releaseBuf returns b to the pool, dropping oversized buffers.
func releaseBuf(b []byte) {
	if cap(b) > 1<<20 {
		return
	}
	bp := b[:cap(b):cap(b)]
	bufPool.Put(&bp)
}
