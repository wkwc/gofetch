package fetch

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
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

// TestInterruptStressPwrite stresses the non-mmap (raw pwrite) writer
// path with random interrupts — the path only otherwise runs on Windows
// or with --no-mmap, so shake out WriteAt/ReadAt concurrency here.
func TestInterruptStressPwrite(t *testing.T) {
	payload := makePayload(512 * 1024)
	srv := newRangeServer(t, payload, nil)
	rng := rand.New(rand.NewPCG(11, 13))
	for i := 0; i < 100; i++ {
		out := filepath.Join(t.TempDir(), "o.bin")
		d := NewDownloader(srv.URL, out, Options{Quiet: true, NoMmap: true})
		ctx, cancel := context.WithCancel(context.Background())
		if rng.IntN(3) != 0 {
			time.AfterFunc(time.Duration(rng.IntN(3000))*time.Microsecond, cancel)
		}
		err := d.Download(ctx)
		cancel()
		d.Close()
		got, _ := os.ReadFile(out)
		if bytes.Equal(got, payload) && err != nil {
			t.Fatalf("iter %d: complete pwrite file reported error %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			if _, serr := os.Stat(out + ".gofetch.resume"); serr != nil && err == nil {
				t.Fatalf("iter %d: partial pwrite, no sidecar, no error", i)
			}
		}
	}
}

// TestMirrorFailoverWithManifestResume is the most combined scenario:
// interrupt a manifest-verified download, then resume through a mirror
// failover (bad primary, good mirror), verifying the manifest gates
// cross-mirror reuse correctly.
func TestMirrorFailoverWithManifestResume(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	good := newRangeServer(t, payload, nil)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)

	out := filepath.Join(t.TempDir(), "o.bin")
	m := manifestFromPayload(t, payload, 1<<20, -1)
	if err := WriteManifest(out+".gofetch.manifest", m); err != nil {
		t.Fatal(err)
	}
	d1 := NewDownloader(good.URL, out, Options{Quiet: true, Mirrors: []string{bad.URL}})
	ctx1, cancel1 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_ = d1.Download(ctx1)
	cancel1()
	d1.Close()

	d2 := NewDownloader(bad.URL, out, Options{Quiet: true, Mirrors: []string{good.URL}})
	if err := d2.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("resume via mirror: %v", err)
	}
	d2.Close()
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch after mirror failover + resume: got %d, want %d", len(got), len(payload))
	}
}
