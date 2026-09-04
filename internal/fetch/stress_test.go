package fetch

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestInterruptStressDownload runs repeated downloads with random
// cancellation timing (some complete, some partial), asserting: a
// byte-complete file implies nil error, partial files carry a resume
// sidecar, and the goroutine count stays bounded (no leaks). Exercises
// the interrupt/completion race, retry/requeue, and idle-body teardown
// under sustained load; runs under -race in CI.
func TestInterruptStressDownload(t *testing.T) {
	payload := makePayload(512 * 1024)
	srv := newRangeServer(t, payload, nil)
	rng := rand.New(rand.NewPCG(99, 7))

	// Warm up pools and transports.
	for i := 0; i < 5; i++ {
		out := filepath.Join(t.TempDir(), "o.bin")
		d := NewDownloader(srv.URL, out, Options{Quiet: true, NoResume: true})
		_ = d.Download(testCtx(t, 30*time.Second))
		d.Close()
	}
	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 150; i++ {
		out := filepath.Join(t.TempDir(), "o.bin")
		d := NewDownloader(srv.URL, out, Options{Quiet: true})
		ctx, cancel := context.WithCancel(context.Background())
		// Random cancel: immediate, a few ms (mid-download), or never.
		switch rng.IntN(10) {
		case 0, 1:
			cancel()
		case 2, 3, 4, 5:
			time.AfterFunc(time.Duration(rng.IntN(3000))*time.Microsecond, cancel)
		}
		err := d.Download(ctx)
		cancel()
		d.Close()
		got, _ := os.ReadFile(out)
		complete := bytes.Equal(got, payload)
		if complete && err != nil {
			t.Fatalf("iter %d: byte-complete file reported error %v", i, err)
		}
		if !complete {
			if _, serr := os.Stat(out + ".gofetch.resume"); serr != nil && err == nil {
				t.Fatalf("iter %d: partial file with no sidecar and no error", i)
			}
		}
	}
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+8 {
		t.Fatalf("goroutine leak: %d -> %d after 150 interrupt downloads", before, after)
	}
}
