package fetch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEndToEndDownload verifies the basic happy path: 4 workers, 2 MB file,
// range-supported server. The output must match the server's payload bit-for-bit.
func TestEndToEndDownload(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 4,
		BufSize:     64 * 1024,
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
