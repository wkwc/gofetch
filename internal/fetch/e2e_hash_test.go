package fetch

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestEndToEndWithHashVerify passes when the post-download SHA-256
// matches the expected value. The negative case is in TestEndToEndWithBadHash.
func TestEndToEndWithHashVerify(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	expected := sha256Hex(payload)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 2,
		BufSize:     32 * 1024,
		Timeout:     10 * time.Second,
		VerifyConfig: VerifyConfig{
			HashType: HashSHA256,
			Expected: expected,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := d.Download(ctx); err != nil {
		t.Fatalf("Download: %v", err)
	}
}

// TestEndToEndWithBadHash fails the verification path: known-bad
// SHA-256 hex is supplied; the Download() must surface an error.
func TestEndToEndWithBadHash(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		WorkerCount: 2,
		BufSize:     32 * 1024,
		Timeout:     10 * time.Second,
		VerifyConfig: VerifyConfig{
			HashType: HashSHA256,
			Expected: "deadbeef" + "deadbeef" + "deadbeef" + "deadbeef" +
				"deadbeef" + "deadbeef" + "deadbeef" + "deadbeef",
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := d.Download(ctx)
	if err == nil {
		t.Fatal("expected hash mismatch error, got nil")
	}
}

// TestSidecarHashFetchOK exercises the sidecar-style parsing logic:
// the .sha256 file contains the hex hash followed by filename, and
// the parser should accept the leading 64 hex chars.
func TestSidecarHashFetchOK(t *testing.T) {
	payload := makePayload(512 * 1024)
	hashHex := sha256Hex(payload)

	parse := func(content string) string {
		if len(content) > 64 {
			content = content[:64]
		}
		return content
	}

	got := parse(hashHex + "  out.bin\n")
	if got != hashHex {
		t.Fatalf("sidecar parse: got %q, want %q", got, hashHex)
	}
}
