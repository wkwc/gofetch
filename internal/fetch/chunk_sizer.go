package fetch

import (
	"sync"
	"time"
)

// ChunkSizer dynamically adjusts chunk size based on observed throughput and RTT
type ChunkSizer struct {
	mu sync.Mutex

	minChunkSize int64
	maxChunkSize int64
	currentSize  int64

	// Performance metrics
	rttEstimate time.Duration
}

// NewChunkSizer creates an adaptive chunk sizer
func NewChunkSizer(minSize, maxSize int64) *ChunkSizer {
	if minSize <= 0 {
		minSize = 256 * 1024 // 256KB
	}
	if maxSize <= 0 {
		maxSize = 16 * 1024 * 1024 // 16MB
	}
	return &ChunkSizer{
		minChunkSize: 256 * 1024,       // 256KB
		maxChunkSize: 16 * 1024 * 1024, // 16MB
		currentSize:  1024 * 1024,      // 1MB default
	}
}

// RecordTransfer records a completed transfer for adaptive sizing
func (cs *ChunkSizer) RecordTransfer(size int64, duration time.Duration, rtt time.Duration) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	throughput := float64(size) / duration.Seconds()
	if throughput <= 0 {
		return
	}

	// Exponential moving average for throughput
	if throughput <= 0 {
		return
	}

	// Target 50ms per chunk
	optimalSize := int64(float64(size) / duration.Seconds() * 0.05)

	// Clamp to bounds
	if optimalSize < 256*1024 {
		optimalSize = 256 * 1024
	}
	if optimalSize > 16*1024*1024 {
		optimalSize = 16 * 1024 * 1024
	}

	// Smooth transition (max 2x change per sample)
	maxChange := cs.currentSize * 2
	if optimalSize > cs.currentSize+maxChange {
		optimalSize = cs.currentSize + maxChange
	}
	if optimalSize < cs.currentSize/2 {
		optimalSize = cs.currentSize / 2
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.currentSize = optimalSize
}

// GetChunkSize returns the current recommended chunk size
func (cs *ChunkSizer) GetChunkSize() int64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.currentSize
}
