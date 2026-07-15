package fetch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEndToEndConcurrentWorkersCleanFile stress-tests 8 workers on a
// 4 MB file with 16 KiB buffers. Even under contention, the final
// bytes must match the source — no torn writes, no off-by-one.
func TestEndToEndConcurrentWorkersCleanFile(t *testing.T) {
	payload := makePayload(4 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 8,
		BufSize:     16 * 1024,
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
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatal("content mismatch under concurrency stress")
	}
}
