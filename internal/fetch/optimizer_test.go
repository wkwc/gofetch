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
	if ac.Workers != 16 && ac.Workers != 8 { // depends on CPU
		t.Logf("initial workers = %d", ac.Workers)
	}
	ac.Retune(200 << 20) // 200 MB
	if ac.BufSize != 256*1024 {
		t.Errorf("Retune(200MB) bufSize = %d, want 256KB", ac.BufSize)
	}
	ac.Retune(100) // tiny file
	if ac.Workers > 4 {
		t.Errorf("Retune(100B) workers = %d, want <= 4", ac.Workers)
	}
}