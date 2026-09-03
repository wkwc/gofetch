package fetch

import (
	"context"
	"sync"
	"time"
)

// rateLimiter is a token bucket shared across workers to cap aggregate
// download throughput at rate bytes/second. Burst is capped at one
// second's worth of tokens so the very first reads still flow quickly.
type rateLimiter struct {
	rate   int64
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// newRateLimiter returns a limiter for the given bytes/second cap, or
// nil when rate <= 0 (unthrottled).
func newRateLimiter(rate int64) *rateLimiter {
	if rate <= 0 {
		return nil
	}
	return &rateLimiter{rate: rate}
}

// wait consumes n tokens for n bytes just read, sleeping as needed so
// the long-run average stays at or below the cap. Nil-safe.
//
// The deficit sleep holds the mutex so concurrent workers serialize
// their sleeps — otherwise N workers would each throttle to the full
// cap and the aggregate rate would be N× too fast. It is interruptible
// via ctx so a cancelled download (Ctrl-C) is not held up by a full
// deficit sleep at a low rate. `last` is refreshed after the sleep so
// its duration is never double-counted.
func (rl *rateLimiter) wait(ctx context.Context, n int) {
	if rl == nil || n <= 0 {
		return
	}
	rl.mu.Lock()
	now := time.Now()
	if rl.last.IsZero() {
		rl.last = now
	}
	rl.tokens += float64(now.Sub(rl.last)) * float64(rl.rate) / float64(time.Second)
	if burst := float64(rl.rate); rl.tokens > burst {
		rl.tokens = burst
	}
	if rl.tokens >= float64(n) {
		rl.tokens -= float64(n)
		rl.last = now
		rl.mu.Unlock()
		return
	}
	deficit := float64(n) - rl.tokens
	sleep := time.Duration(deficit / float64(rl.rate) * float64(time.Second))
	rl.tokens = 0
	if sleep > 0 {
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}
	}
	// The sleep refilled exactly `deficit` tokens which were consumed
	// above; reset the base so the next call measures only post-sleep
	// time (or, on cancellation, so the download can stop promptly).
	rl.last = time.Now()
	rl.mu.Unlock()
}
