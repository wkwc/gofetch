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
	t.Logf("initial workers = %d", ac.Workers)
	ac.Retune(200 << 20) // 200 MB
	if ac.BufSize != 256*1024 {
		t.Errorf("Retune(200MB) bufSize = %d, want 256KB", ac.BufSize)
	}
	ac.Retune(100) // tiny file
	if ac.BufSize != 16*1024 {
		t.Errorf("Retune(100B) bufSize = %d, want 16KB", ac.BufSize)
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
		{1 << 10, 16 * 1024},
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