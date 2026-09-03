package fetch

import (
	"context"
	"testing"
	"time"
)

// TestRateLimiterShortBurst verifies a small burst passes immediately
// (tokens are available) rather than stalling on the first read.
func TestRateLimiterShortBurst(t *testing.T) {
	rl := newRateLimiter(1 << 20) // 1 MiB/s
	start := time.Now()
	rl.wait(context.Background(), 1024)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("first 1 KiB read took %v, want ~instant", d)
	}
}

// TestRateLimiterThroughput verifies the long-run average stays at or
// below the configured cap.
func TestRateLimiterThroughput(t *testing.T) {
	const rate int64 = 1 << 20 // 1 MiB/s
	rl := newRateLimiter(rate)

	start := time.Now()
	// 2 MiB total in 4 KiB reads (~2s at the cap).
	const total = 2 << 20
	for sent := 0; sent < total; sent += 4 << 10 {
		rl.wait(context.Background(), 4<<10)
	}
	elapsed := time.Since(start)
	got := float64(total) / elapsed.Seconds()
	if got > float64(rate)*1.15 {
		t.Errorf("throughput %.0f B/s exceeds cap %.0f B/s", got, float64(rate))
	}
	if elapsed < time.Second {
		t.Errorf("2 MiB at 1 MiB/s finished in %v, too fast", elapsed)
	}
}

// TestRateLimiterNilSafe verifies the nil receiver is a no-op.
func TestRateLimiterNilSafe(t *testing.T) {
	var rl *rateLimiter
	rl.wait(context.Background(), 1024) // must not panic
}

func TestRateLimiterCancelInterrupts(t *testing.T) {
	// A deficit sleep must return promptly when the context is cancelled,
	// so Ctrl-C under a low rate limit is not held up by the full sleep.
	rl := newRateLimiter(1 << 20) // 1 MiB/s
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	cancel()              // cancelled before the wait
	rl.wait(ctx, 256<<10) // would sleep 250ms at full deficit
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("cancelled wait took %v, want prompt return", d)
	}
}
