package fetch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEndToEndResume verifies that an aborted download can be completed
// successfully via a second invocation: the byte-level fault-tolerance
// (sparse + WriteAt reclamation) means contents are byte-perfect.
func TestEndToEndResume(t *testing.T) {
	payload := makePayload(4 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	// First run: cancel after 1.5s, before the 5s resume save tick fires.
	d1 := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 4,
		BufSize:     64 * 1024,
		Timeout:     10 * time.Second,
		Resume:      true,
	})

	ctx1, cancel1 := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	if err := d1.Download(ctx1); err != nil && err != context.DeadlineExceeded {
		t.Fatalf("first Download: %v", err)
	}
	cancel1()

	// Second run: complete the download. Partial bytes are already on
	// disk from the abort, and re-downloading them is idempotent via WriteAt.
	d2 := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 4,
		BufSize:     64 * 1024,
		Timeout:     10 * time.Second,
		Resume:      true,
	})

	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()

	if err := d2.Download(ctx2); err != nil {
		t.Fatalf("second Download: %v", err)
	}

	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("size = %d, want %d", len(got), len(payload))
	}
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatal("content mismatch after resume")
	}

	// Resume state cleaned up on success.
	if _, err := os.Stat(resumePath(outFile)); !os.IsNotExist(err) {
		t.Fatal("resume state file should be deleted on success")
	}
}
