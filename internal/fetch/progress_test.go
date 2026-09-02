package fetch

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPrintProgressRedirected(t *testing.T) {
	// Force non-TTY: point os.Stderr at a pipe and reset the cached TTY
	// probe so printProgress re-evaluates against the swapped stderr.
	old := os.Stderr
	var buf bytes.Buffer
	r, w, _ := os.Pipe()
	os.Stderr = w
	stderrTTYOnce = sync.Once{}
	defer func() {
		os.Stderr = old
		stderrTTYOnce = sync.Once{}
	}()

	d := &Downloader{quiet: false}
	p := newProgress(100, nil)
	p.add(60)

	d.printProgress(p, false) // live update should be suppressed
	d.printProgress(p, true)  // final should print plain line

	_ = w.Close()
	os.Stderr = old
	_, _ = buf.ReadFrom(r)
	out := buf.String()
	if strings.ContainsAny(out, "\r\x1b") {
		t.Fatalf("redirected output leaked ANSI/CR: %q", out)
	}
	if out == "" {
		t.Fatal("expected final plain line")
	}
	t.Logf("redirected output: %q", out)
}

func TestSpeedAndETA(t *testing.T) {
	p := newProgress(1<<20, nil)
	// First call primes prev state and returns no EWMA.
	if _, eta := p.speedAndETA(1000); eta != 0 {
		t.Errorf("first call ETA = %v, want 0", eta)
	}
	// Advance time past the 0.5s gate; second snapshot computes a speed.
	// speedAndETA reads time.Now(), so we can only assert monotonic sanity:
	// after enough progress the ETA must shrink toward zero.
	p.prevTime = time.Now().Add(-time.Second).UnixNano()
	speed1, eta1 := p.speedAndETA(512 << 10)
	if speed1 <= 0 {
		t.Errorf("speed = %v, want > 0", speed1)
	}
	if eta1 <= 0 {
		t.Errorf("ETA = %v, want > 0 (half a file at speed1)", eta1)
	}
	// Completing the rest should drop ETA to ~0 on the next update.
	p.prevTime = time.Now().Add(-time.Second).UnixNano()
	_, eta2 := p.speedAndETA(p.total)
	if eta2 != 0 {
		t.Errorf("ETA at completion = %v, want 0", eta2)
	}
}

func TestHumanBytesExported(t *testing.T) {
	cases := map[int64]string{
		0:       "0 B",
		1023:    "1023 B",
		1 << 20: "1.0 MB",
		1 << 30: "1.00 GB",
	}
	for n, want := range cases {
		if got := HumanBytes(n); got != want {
			t.Errorf("HumanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}
