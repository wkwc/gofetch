package fetch

import "sync"

// bufPool recycles per-worker read buffers, keyed by capacity class.
// Avoids pooling oversized buffers (>1 MiB) that would waste memory.
var bufPool = sync.Pool{
	New: func() any { b := make([]byte, 64*1024); return &b },
}

// acquireBuf returns a buffer of at least length n, drawing from the pool.
func acquireBuf(n int) []byte {
	bp := bufPool.Get().(*[]byte)
	if cap(*bp) >= n {
		return (*bp)[:n]
	}
	// Pool buffer too small; allocate fresh. Don't return the old one
	// since it's the wrong size class.
	return make([]byte, n)
}

// releaseBuf returns b to the pool, dropping oversized buffers.
func releaseBuf(b []byte) {
	if cap(b) > 1<<20 || cap(b) == 0 {
		return
	}
	bp := b[:cap(b):cap(b)]
	bufPool.Put(&bp)
}
