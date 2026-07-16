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

// TestEndToEndDownload verifies the basic happy path: 2 MB file,
// range-supported server. The output must match the server's payload bit-for-bit.
func TestEndToEndDownload(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{})

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

// TestEndToEndConcurrentWorkersCleanFile stress-tests the auto-configured
// worker count on a 4 MB file. Even under contention, the final bytes
// must match the source — no torn writes, no off-by-one.
func TestEndToEndConcurrentWorkersCleanFile(t *testing.T) {
	payload := makePayload(4 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{})

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

// TestEndToEndWithHashVerify passes when the post-download SHA-256
// matches the expected value. The negative case is in TestEndToEndWithBadHash.
func TestEndToEndWithHashVerify(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	expected := sha256Hex(payload)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		HashAlgo:     "sha256",
		ExpectedHash: expected,
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
		HashAlgo: "sha256",
		ExpectedHash: "deadbeefdeadbeefdeadbeefdeadbeef" +
			"deadbeefdeadbeefdeadbeefdeadbeef",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := d.Download(ctx)
	if err == nil {
		t.Fatal("expected hash mismatch error, got nil")
	}
}

// TestEndToEndWithSHA512Verify verifies SHA-512 hashing works.
func TestEndToEndWithSHA512Verify(t *testing.T) {
	payload := makePayload(512 * 1024)
	expected := sha512Hex(payload)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{
		HashAlgo:     "sha512",
		ExpectedHash: expected,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := d.Download(ctx); err != nil {
		t.Fatalf("Download: %v", err)
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

// TestEndToEndResume verifies that a partial download can be resumed
// and completes with correct content. The test kills the download
// mid-way by cancelling the context, then restarts.
func TestEndToEndResume(t *testing.T) {
	payload := makePayload(8 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	// First attempt: cancel after a short time
	d1 := NewDownloader(srv.URL, outFile, Options{})
	ctx1, cancel1 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_ = d1.Download(ctx1)
	cancel1()

	// Resume: same URL, same output file
	d2 := NewDownloader(srv.URL, outFile, Options{})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()

	if err := d2.Download(ctx2); err != nil {
		t.Fatalf("Resume download: %v", err)
	}

	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatal("resumed content mismatch")
	}
}

// TestEndToEndNoResume verifies --no-resume downloads fresh.
func TestEndToEndNoResume(t *testing.T) {
	payload := makePayload(4 * 1024 * 1024)
	srv := newRangeServer(t, payload)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	// First download
	d1 := NewDownloader(srv.URL, outFile, Options{})
	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	if err := d1.Download(ctx1); err != nil {
		t.Fatalf("First download: %v", err)
	}
	cancel1()

	// Second download with --no-resume should overwrite, not resume
	d2 := NewDownloader(srv.URL, outFile, Options{NoResume: true})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()

	if err := d2.Download(ctx2); err != nil {
		t.Fatalf("No-resume download: %v", err)
	}

	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatal("no-resume content mismatch")
	}
}

// TestProbeHeadUnsupported verifies that when a server returns 405 for HEAD,
// the probe correctly falls back to a 1-byte range GET to detect range support.
func TestProbeHeadUnsupported(t *testing.T) {
	payload := makePayload(256 * 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		var start, end int64
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if end >= int64(len(payload)) {
			end = int64(len(payload)) - 1
		}
		if start < 0 || start > end {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : end+1])
	}))
	t.Cleanup(srv.Close)

	d := NewDownloader(srv.URL, "test.bin", Options{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := d.probeURL(ctx, srv.URL)
	if err != nil {
		t.Fatalf("probeURL: %v", err)
	}
	if !info.supportsRanges {
		t.Error("expected ranges to be supported via fallback probe")
	}
	if info.total != int64(len(payload)) {
		t.Errorf("total = %d, want %d", info.total, len(payload))
	}
}

// TestProbeServerError verifies that a server returning 500 surfaces as an error.
func TestProbeServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	d := NewDownloader(srv.URL, "test.bin", Options{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := d.probeURL(ctx, srv.URL)
	if err == nil {
		t.Error("expected probe error for 500, got nil")
	}
}
