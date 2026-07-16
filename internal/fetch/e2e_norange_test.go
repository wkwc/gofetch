package fetch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEndToEndNoRange verifies the single-stream fallback when the
// server doesn't advertise Range support: one worker downloads the
// whole body at offset 0.
func TestEndToEndNoRange(t *testing.T) {
	payload := makePayload(256 * 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 4,
		BufSize:     32 * 1024,
		Timeout:     10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := d.Download(ctx); err != nil {
		t.Fatalf("Download: %v", err)
	}

	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("size = %d, want %d", len(got), len(payload))
	}
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatal("content mismatch")
	}
}

// TestEndToEndRangeLiar verifies that when a server advertises Accept-Ranges
// in its HEAD response but then returns 200 OK (full body) instead of 206
// for actual range requests, the download fails with a clear error rather
// than silently corrupting the output.
func TestEndToEndRangeLiar(t *testing.T) {
	payload := makePayload(256 * 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		// Lie: advertise ranges but return 200 with full body
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 2,
		BufSize:     32 * 1024,
		Timeout:     10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := d.Download(ctx)
	if err == nil {
		t.Fatal("expected error for server returning 200 on range request, got nil")
	}
}
