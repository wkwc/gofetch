package fetch

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	ac := AutoConfigure(0)
	backoffs := make([]time.Duration, 6)
	for i := 0; i < 6; i++ {
		backoffs[i] = ac.Backoff(i)
	}
	if backoffs[0] < 100*time.Millisecond || backoffs[0] > 150*time.Millisecond {
		t.Errorf("backoff[0] = %v, want ~100-125ms", backoffs[0])
	}
	if backoffs[1] < 200*time.Millisecond || backoffs[1] > 300*time.Millisecond {
		t.Errorf("backoff[1] = %v, want ~200-250ms", backoffs[1])
	}
	if backoffs[2] < 400*time.Millisecond || backoffs[2] > 500*time.Millisecond {
		t.Errorf("backoff[2] = %v, want ~400-500ms", backoffs[2])
	}
	for i := 3; i < 6; i++ {
		if backoffs[i] > 30*time.Second+1*time.Second {
			t.Errorf("backoff[%d] = %v, want <= 31s", i, backoffs[i])
		}
	}

	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		seen[ac.Backoff(2)] = true
	}
	if len(seen) == 1 {
		t.Error("backoff produced identical values 100 times — no jitter")
	}
}

func TestAutoConfigTimeout(t *testing.T) {
	ac := AutoConfigure(0)
	if ac.Timeout != 10*time.Second {
		t.Errorf("initial timeout = %v, want 10s", ac.Timeout)
	}
}

func TestAutoConfigRetune(t *testing.T) {
	ac := AutoConfigure(0)
	ac.Retune(200<<20, false, false) // 200 MB
	if ac.BufSize != 256*1024 {
		t.Errorf("Retune(200MB) bufSize = %d, want 256KB", ac.BufSize)
	}
	ac.Retune(100, false, false) // tiny file
	if ac.BufSize != 32*1024 {
		t.Errorf("Retune(100B) bufSize = %d, want 32KB", ac.BufSize)
	}

	// Overridden values are preserved by Retune.
	ac2 := AutoConfigure(0)
	ac2.Workers = 4
	ac2.BufSize = 64 << 10
	ac2.Retune(200<<20, true, true)
	if ac2.Workers != 4 || ac2.BufSize != 64<<10 {
		t.Errorf("Retune clobbered overrides: workers=%d buf=%d, want 4/64KB", ac2.Workers, ac2.BufSize)
	}
	// Partially overridden: only the free field is retuned.
	ac3 := AutoConfigure(0)
	ac3.BufSize = 32 << 10
	ac3.Retune(200<<20, false, true)
	if ac3.BufSize != 32<<10 {
		t.Errorf("buf override clobbered: %d, want 32KB", ac3.BufSize)
	}
	if ac3.Workers == 0 {
		t.Error("workers should still be retuned when not overridden")
	}
}

func TestScaleWorkers(t *testing.T) {
	cores := 8
	if w := scaleWorkers(0, cores); w < 4 || w > 12 {
		t.Errorf("scaleWorkers(0) = %d, want 4-12", w)
	}
	if w := scaleWorkers(100, cores); w != 4 {
		t.Errorf("scaleWorkers(100B) = %d, want 4", w)
	}
	if w := scaleWorkers(64<<20, cores); w < 4 || w > cores*2 {
		t.Errorf("scaleWorkers(64MiB) = %d, want 4-%d", w, cores*2)
	}
}

func TestScaleBufSize(t *testing.T) {
	cases := []struct {
		size int64
		want int
	}{
		{0, 64 * 1024},
		{1 << 10, 32 * 1024},
		{1 << 20, 64 * 1024},
		{99 << 20, 64 * 1024},
		{200 << 20, 256 * 1024},
	}
	for _, c := range cases {
		got := scaleBufSize(c.size)
		if got != c.want {
			t.Errorf("scaleBufSize(%d) = %d, want %d", c.size, got, c.want)
		}
	}
}

func TestBackoffCapAndJitter(t *testing.T) {
	ac := AutoConfigure(0)
	// n=0 → RetryBase with jitter ≤ 25% upward.
	for i := 0; i < 50; i++ {
		d := ac.Backoff(0)
		if d < ac.RetryBase || d > ac.RetryBase+ac.RetryBase/4 {
			t.Fatalf("Backoff(0) = %v, outside [base, base*1.25]", d)
		}
	}
	// Large n caps at RetryCap; jitter is applied on top (up to +25%).
	for i := 0; i < 50; i++ {
		d := ac.Backoff(30)
		if d < ac.RetryCap {
			t.Fatalf("Backoff(30) = %v, below cap %v", d, ac.RetryCap)
		}
		if d > ac.RetryCap+ac.RetryCap/4 {
			t.Fatalf("Backoff(30) = %v, exceeds cap+jitter", d)
		}
	}
}
