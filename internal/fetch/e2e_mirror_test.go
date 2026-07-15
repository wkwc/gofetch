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

// TestMirrorETagMismatch: when two healthy mirrors return different
// ETag headers, the loader must refuse the download rather than
// pick silently inconsistent content.
func TestMirrorETagMismatch(t *testing.T) {
	payload1 := makePayload(256 * 1024)
	payload2 := makePayload(512 * 1024)

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag1"`)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload1)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload1)
	}))
	t.Cleanup(srv1.Close)

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag2"`)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload2)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload2)
	}))
	t.Cleanup(srv2.Close)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv1.URL, outFile, Options{
		WorkerCount: 2,
		BufSize:     32 * 1024,
		Timeout:     10 * time.Second,
		Mirrors:     []string{srv2.URL},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := d.Download(ctx); err == nil {
		t.Fatal("expected ETag mismatch error, got nil")
	}
}

// TestMirrorSelectionConsistency: two mirrors serve the same file
// contents; the fetch must succeed and produce the expected SHA-256.
func TestMirrorSelectionConsistency(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	srv1 := newRangeServer(t, payload)
	srv2 := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv1.URL, outFile, Options{
		WorkerCount: 4,
		BufSize:     64 * 1024,
		Timeout:     10 * time.Second,
		Mirrors:     []string{srv2.URL},
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
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatal("content mismatch with mirror failover")
	}
}
