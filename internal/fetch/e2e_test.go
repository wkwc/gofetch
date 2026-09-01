package fetch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestEndToEndDownload verifies the basic happy path: 2 MB file,
// range-supported server. The output must match the server's payload bit-for-bit.
func TestEndToEndDownload(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)

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
	srv := newRangeServer(t, payload, nil)

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
	srv := newRangeServer(t, payload, nil)

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
	srv := newRangeServer(t, payload, nil)

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
	srv := newRangeServer(t, payload, nil)

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

// TestSidecarHashParsing exercises the hash flag parsing logic that
// the CLI uses for sidecar-style hashes (algo:hex format).
func TestSidecarHashParsing(t *testing.T) {
	payload := makePayload(512 * 1024)
	hashHex := sha256Hex(payload)

	// Test that ParseHashFlag correctly handles sha256:hex format
	algo, got, err := ParseHashFlag("sha256:" + hashHex)
	if err != nil {
		t.Fatalf("ParseHashFlag: %v", err)
	}
	if algo != "sha256" {
		t.Fatalf("algo = %q, want sha256", algo)
	}
	if got != hashHex {
		t.Fatalf("hash parse: got %q, want %q", got, hashHex)
	}
}

// TestEndToEndResume verifies that a partial download can be resumed
// and completes with correct content. The test kills the download
// mid-way by cancelling the context, then restarts.
func TestEndToEndResume(t *testing.T) {
	payload := makePayload(8 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)

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
	srv := newRangeServer(t, payload, nil)

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
	srv := newRangeServer(t, payload, &rangeServerConfig{
		Head: func(w http.ResponseWriter, r *http.Request, payload []byte) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		},
	})

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

// TestProbeHeadNoContentLength falls through to the range-GET probe when
// HEAD 200 omits Content-Length (historical bug: ok=true short-circuited).
func TestProbeHeadNoContentLength(t *testing.T) {
	payload := makePayload(64 * 1024)
	srv := newRangeServer(t, payload, &rangeServerConfig{
		Head: func(w http.ResponseWriter, r *http.Request, payload []byte) {
			// Intentionally omit Content-Length so probe falls through.
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
		},
	})

	d := NewDownloader(srv.URL, "test.bin", Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := d.probeURL(ctx, srv.URL)
	if err != nil {
		t.Fatalf("probeURL: %v", err)
	}
	if !info.supportsRanges {
		t.Error("expected ranges via range-GET fallthrough after HEAD without CL")
	}
	if info.total != int64(len(payload)) {
		t.Errorf("total = %d, want %d", info.total, len(payload))
	}
}

// TestQuietModeStillVerifiesHash ensures -q never skips integrity checks.
func TestQuietModeStillVerifiesHash(t *testing.T) {
	payload := makePayload(256 * 1024)
	srv := newRangeServer(t, payload, nil)
	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")

	// Bad hash must fail even in quiet mode.
	d := NewDownloader(srv.URL, outFile, Options{
		Quiet:        true,
		HashAlgo:     "sha256",
		ExpectedHash: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := d.Download(ctx); err == nil {
		t.Fatal("expected hash mismatch error in quiet mode")
	}

	// Good hash must succeed in quiet mode (and Sync/Close the file).
	d2 := NewDownloader(srv.URL, outFile, Options{
		Quiet:        true,
		NoResume:     true,
		HashAlgo:     "sha256",
		ExpectedHash: sha256Hex(payload),
	})
	if err := d2.Download(ctx); err != nil {
		t.Fatalf("quiet+good hash: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatal("content mismatch after quiet download")
	}
}

// TestShortRangeBodyErrors ensures a truncated 206 is not treated as success.
func TestShortRangeBodyErrors(t *testing.T) {
	payload := makePayload(64 * 1024)
	// Promise the full range but write only half, then close.
	srv := newRangeServer(t, payload, &rangeServerConfig{
		Write: func(w http.ResponseWriter, r *http.Request, payload []byte, start, end int64) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)/2))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[:len(payload)/2])
		},
	})

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{NoResume: true, Quiet: true})
	// Force single worker path size still uses ranges for 64KiB (threshold is 64KiB exclusive).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := d.Download(ctx)
	if err == nil {
		t.Fatal("expected short-body error, got success")
	}
}

// TestMidRangeEOFRetriesRecovery pins the G6 fix: a server that closes
// the connection mid-range on the first attempt (the canonical transient
// network failure — TCP close after a partial transfer) must have its
// range requeued and retried. When the second attempt serves the full
// range, the download recovers and the output matches bit-for-bit.
// Before the fix, the mid-range EOF was wrapped so that isTransient()
// returned false and the whole download fatally aborted.
func TestMidRangeEOFRetriesRecovery(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)

	// rangeKey tracks how many body-served requests have been issued for
	// a given range so we can truncate only the first attempt.
	var attemptsMu sync.Mutex
	attempts := map[string]int{}
	firstHalf := func(start, end int64) int64 { return end - (end-start+1)/2 + 1 }

	srv := newRangeServer(t, payload, &rangeServerConfig{
		Write: func(w http.ResponseWriter, r *http.Request, payload []byte, start, end int64) {
			key := fmt.Sprintf("%d-%d", start, end)
			attemptsMu.Lock()
			attempts[key]++
			n := attempts[key]
			attemptsMu.Unlock()

			// Every range is truncated on its FIRST attempt: the headers
			// promise the full range (so the Content-Range check passes) but
			// we write only the first half and then close the connection —
			// exactly a CDN closing mid-stream. The second attempt serves
			// the full range. Truncating every attempt (TestShortRangeBodyErrors)
			// only exercises the retry-budget exhaustion path.
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			if n == 1 {
				_, _ = w.Write(payload[start : firstHalf(start, end)+1])
				return
			}
			_, _ = w.Write(payload[start : end+1])
		},
	})

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")
	opt := Options{NoResume: true, Quiet: true, HashAlgo: "sha256", ExpectedHash: sha256Hex(payload)}
	d := NewDownloader(srv.URL, outFile, opt)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.Download(ctx); err != nil {
		t.Fatalf("expected recovery after mid-range EOF retry, got error: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("output mismatch after truncated-then-full retry: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestRange200FallsBackToSingle pins the F2 fix: a server that advertises
// Accept-Ranges on HEAD but returns 200 OK (full body) for Range GETs must
// not fatally fail. range mode aborts and single-stream completes the file.
// Partial range tasks must not be marked complete before the fallback.
func TestRange200FallsBackToSingle(t *testing.T) {
	// Above smallFileThreshold so downloadFromMirror chooses range mode.
	payload := makePayload(128 * 1024)
	var rangeHits, fullHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		// Always ignore Range and return 200 with the full body.
		if r.Header.Get("Range") != "" {
			rangeHits.Add(1)
		} else {
			fullHits.Add(1)
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{NoResume: true, Quiet: true})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := d.Download(ctx); err != nil {
		t.Fatalf("expected single-stream fallback success, got: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch after 200-fallback: got %d bytes, want %d", len(got), len(payload))
	}
	if rangeHits.Load() < 1 {
		t.Fatal("expected at least one Range GET that returned 200")
	}
	if fullHits.Load() < 1 {
		t.Fatal("expected a non-Range single-stream GET after fallback")
	}
}
