package fetch

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestEndToEndDownload verifies the basic happy path: 2 MB file,
// range-supported server. The output must match the server's payload bit-for-bit.
func TestEndToEndDownload(t *testing.T) {
	downloadAndVerify(t, makePayload(2*1024*1024), Options{})
}

// TestEndToEndConcurrentWorkersCleanFile stress-tests the auto-configured
// worker count on a 4 MB file. Even under contention, the final bytes
// must match the source — no torn writes, no off-by-one.
func TestEndToEndConcurrentWorkersCleanFile(t *testing.T) {
	downloadAndVerify(t, makePayload(4*1024*1024), Options{})
}

// TestEndToEndWithHashVerify passes when the post-download SHA-256
// matches the expected value. The negative case is in TestEndToEndWithBadHash.
func TestEndToEndWithHashVerify(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	downloadAndVerify(t, payload, Options{HashAlgo: "sha256", ExpectedHash: sha256Hex(payload)})
}

// TestEndToEndWithBadHash fails the verification path: known-bad
// SHA-256 hex is supplied; the Download() must surface an error.
func TestEndToEndWithBadHash(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)
	outFile := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{
		HashAlgo:     "sha256",
		ExpectedHash: "deadbeefdeadbeefdeadbeefdeadbeef" + "deadbeefdeadbeefdeadbeefdeadbeef",
	})
	if err := d.Download(testCtx(t, 30*time.Second)); err == nil {
		t.Fatal("expected hash mismatch error, got nil")
	}
}

// TestEndToEndWithSHA512Verify verifies SHA-512 hashing works.
func TestEndToEndWithSHA512Verify(t *testing.T) {
	payload := makePayload(512 * 1024)
	downloadAndVerify(t, payload, Options{HashAlgo: "sha512", ExpectedHash: sha512Hex(payload)})
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
// mid-way by cancelling the context, then restarts on the same file.
func TestEndToEndResume(t *testing.T) {
	payload := makePayload(8 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)
	outFile := filepath.Join(t.TempDir(), "out.bin")

	// First attempt: cancel after a short time.
	d1 := NewDownloader(srv.URL, outFile, Options{})
	ctx1, cancel1 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_ = d1.Download(ctx1)
	cancel1()

	// Resume: same URL, same output file.
	d2 := NewDownloader(srv.URL, outFile, Options{})
	if err := d2.Download(testCtx(t, 30*time.Second)); err != nil {
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

// TestEndToEndNoResume verifies --no-resume downloads fresh, overwriting
// a file that a previous run completed (and its resume sidecar).
func TestEndToEndNoResume(t *testing.T) {
	payload := makePayload(4 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)
	outFile := filepath.Join(t.TempDir(), "out.bin")

	// First download (completes, clearing any sidecar).
	d1 := NewDownloader(srv.URL, outFile, Options{})
	if err := d1.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("First download: %v", err)
	}

	// Second download with --no-resume should overwrite, not resume.
	d2 := NewDownloader(srv.URL, outFile, Options{NoResume: true})
	if err := d2.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("No-resume download: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
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
		Head: func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		},
	})

	d := NewDownloader(srv.URL, "test.bin", Options{})
	info, err := d.probeURL(testCtx(t, 10*time.Second), srv.URL)
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	d := NewDownloader(srv.URL, "test.bin", Options{})
	_, err := d.probeURL(testCtx(t, 10*time.Second), srv.URL)
	if err == nil {
		t.Error("expected probe error for 500, got nil")
	}
}

// TestProbeHeadNoContentLength falls through to the range-GET probe when
// HEAD 200 omits Content-Length (historical bug: ok=true short-circuited).
func TestProbeHeadNoContentLength(t *testing.T) {
	payload := makePayload(64 * 1024)
	srv := newRangeServer(t, payload, &rangeServerConfig{
		Head: func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			// Intentionally omit Content-Length so probe falls through.
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
		},
	})

	d := NewDownloader(srv.URL, "test.bin", Options{})
	info, err := d.probeURL(testCtx(t, 10*time.Second), srv.URL)
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
	outFile := filepath.Join(t.TempDir(), "out.bin")

	// Bad hash must fail even in quiet mode.
	d := NewDownloader(srv.URL, outFile, Options{
		Quiet:        true,
		HashAlgo:     "sha256",
		ExpectedHash: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	if err := d.Download(testCtx(t, 30*time.Second)); err == nil {
		t.Fatal("expected hash mismatch error in quiet mode")
	}

	// Good hash must succeed in quiet mode (and Sync/Close the file).
	d2 := NewDownloader(srv.URL, outFile, Options{
		Quiet:        true,
		NoResume:     true,
		HashAlgo:     "sha256",
		ExpectedHash: sha256Hex(payload),
	})
	if err := d2.Download(testCtx(t, 30*time.Second)); err != nil {
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
		Write: func(w http.ResponseWriter, _ *http.Request, payload []byte, _, _ int64) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)/2))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[:len(payload)/2])
		},
	})

	outFile := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{NoResume: true, Quiet: true})
	err := d.Download(testCtx(t, 10*time.Second))
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
		Write: func(w http.ResponseWriter, _ *http.Request, payload []byte, start, end int64) {
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

	outFile := filepath.Join(t.TempDir(), "out.bin")
	opt := Options{NoResume: true, Quiet: true, HashAlgo: "sha256", ExpectedHash: sha256Hex(payload)}
	d := NewDownloader(srv.URL, outFile, opt)
	if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
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

	outFile := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{NoResume: true, Quiet: true})
	if err := d.Download(testCtx(t, 15*time.Second)); err != nil {
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

// TestUnknownSizeSingleStream verifies the no-Content-Length path: the
// server streams a body with unknown size (chunked), single-stream is used,
// and the download completes with the real byte count recorded on the
// Downloader so finalize reports the actual size.
func TestUnknownSizeSingleStream(t *testing.T) {
	payload := makePayload(300 * 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		// No Content-Length → Go uses chunked encoding; probe falls back
		// to a range GET which also omits size → single-stream.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	outFile := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{Quiet: true, NoResume: true})
	if err := d.Download(testCtx(t, 15*time.Second)); err != nil {
		t.Fatalf("Download: %v", err)
	}
	if d.totalSize != int64(len(payload)) {
		t.Errorf("d.totalSize = %d, want %d", d.totalSize, len(payload))
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestRateLimitedNoThrash pins the steal-vs-rate-limit interaction: with a
// rate cap below the monitor's steal threshold (~700 KiB/s), the
// work-stealing monitor must not cancel and re-request partially-written
// ranges (that would thrash forever). Each 1 MiB seed range is requested
// exactly once.
func TestRateLimitedNoThrash(t *testing.T) {
	const rate int64 = 512 << 10 // 512 KiB/s < ~700 KiB/s steal threshold
	payload := makePayload(2 << 20)

	var mu sync.Mutex
	reqs := map[string]int{}
	srv := newRangeServer(t, payload, &rangeServerConfig{
		Write: func(w http.ResponseWriter, _ *http.Request, payload []byte, start, end int64) {
			mu.Lock()
			reqs[fmt.Sprintf("%d-%d", start, end)]++
			mu.Unlock()
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			const block = 16 * 1024
			for cur := start; cur <= end; cur += block {
				end2 := cur + block - 1
				if end2 > end {
					end2 = end
				}
				_, _ = w.Write(payload[cur : end2+1])
			}
		},
	})

	outFile := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{RateLimit: rate, Quiet: true, NoResume: true})
	if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("Download: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reqs) != 2 {
		t.Fatalf("got %d distinct range requests, want the 2 seeds exactly: %v", len(reqs), reqs)
	}
	for k, n := range reqs {
		if n != 1 {
			t.Errorf("range %s requested %d times, want 1 (steal thrash)", k, n)
		}
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestProbeURL verifies --info's probe mode reports size/range support and
// the auto-tuned concurrency without downloading anything.
func TestProbeURL(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)

	p, err := ProbeURL(testCtx(t, 10*time.Second), srv.URL)
	if err != nil {
		t.Fatalf("ProbeURL: %v", err)
	}
	if p.Total != int64(len(payload)) {
		t.Errorf("Total = %d, want %d", p.Total, len(payload))
	}
	if !p.SupportsRanges {
		t.Error("expected SupportsRanges")
	}
	if p.Workers < 1 || p.BufSize < 1 {
		t.Errorf("auto config missing: workers=%d buf=%d", p.Workers, p.BufSize)
	}
}

// TestWorkersBufOverride verifies the -x/--buf-size escape hatches: the
// overrides survive applyProbe's auto-retune and the download still works.
func TestWorkersBufOverride(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)
	outFile := filepath.Join(t.TempDir(), "out.bin")

	d := NewDownloader(srv.URL, outFile, Options{Workers: 2, BufSize: 32 * 1024})
	if d.autoConfig.Workers != 2 {
		t.Errorf("workers = %d, want 2", d.autoConfig.Workers)
	}
	if d.autoConfig.BufSize != 32*1024 {
		t.Errorf("bufSize = %d, want 32768", d.autoConfig.BufSize)
	}
	if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("Download: %v", err)
	}
	// applyProbe/Retune must not clobber the overrides.
	if d.autoConfig.Workers != 2 || d.autoConfig.BufSize != 32*1024 {
		t.Errorf("overrides clobbered by Retune: workers=%d buf=%d", d.autoConfig.Workers, d.autoConfig.BufSize)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("content mismatch")
	}
}

// manifestFromPayload builds a sha256 manifest over data in chunkSize
// chunks. corruptChunk >= 0 replaces that chunk's hash with a wrong value.
func manifestFromPayload(t *testing.T, data []byte, chunkSize int64, corruptChunk int) *Manifest {
	t.Helper()
	m := &Manifest{Version: ManifestVersion, Algo: "sha256"}
	idx := 0
	for start := int64(0); start < int64(len(data)); start += chunkSize {
		end := start + chunkSize - 1
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		h := sha256Hex(data[start : end+1])
		if idx == corruptChunk {
			h = strings.Repeat("0", len(h))
		}
		m.Chunks = append(m.Chunks, ChunkHash{Start: start, End: end, Hash: h})
		idx++
	}
	m.buildIndex()
	return m
}

// TestEndToEndManifestVerification exercises the production integrity
// path: a manifest beside the output is loaded, every chunk is verified
// as its task completes (verifyTaskRange), and VerifyFull passes at the
// end.
func TestEndToEndManifestVerification(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024) // two 1 MiB chunks
	srv := newRangeServer(t, payload, nil)
	outFile := filepath.Join(t.TempDir(), "out.bin")
	if err := WriteManifest(outFile+".gofetch.manifest", manifestFromPayload(t, payload, 1<<20, -1)); err != nil {
		t.Fatal(err)
	}

	d := NewDownloader(srv.URL, outFile, Options{NoResume: true, Quiet: true})
	if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("Download with valid manifest: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("content mismatch")
	}
}

// TestEndToEndManifestCorruptChunkFails verifies that a manifest whose
// expected hash for one chunk is wrong aborts the download — the chunk
// fails verification as its task completes, before any final hash step.
func TestEndToEndManifestCorruptChunkFails(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	srv := newRangeServer(t, payload, nil)
	outFile := filepath.Join(t.TempDir(), "out.bin")
	if err := WriteManifest(outFile+".gofetch.manifest", manifestFromPayload(t, payload, 1<<20, 1)); err != nil {
		t.Fatal(err)
	}

	d := NewDownloader(srv.URL, outFile, Options{NoResume: true, Quiet: true})
	if err := d.Download(testCtx(t, 30*time.Second)); err == nil {
		t.Fatal("expected download to fail on a corrupt manifest chunk, got nil")
	}
}

// TestEndToEndWithMD5Verify verifies MD5-verified downloads work end to
// end (the algorithm real dataset servers like Zenodo/4TU publish).
func TestEndToEndWithMD5Verify(t *testing.T) {
	payload := makePayload(512 * 1024)
	m := md5.Sum(payload)
	expected := hex.EncodeToString(m[:])
	downloadAndVerify(t, payload, Options{HashAlgo: "md5", ExpectedHash: expected})
}

// newChaosServer serves a range-able payload while randomly injecting
// failures (mid-range truncation, abrupt connection reset, retryable
// 503, wrong Content-Range, wrong Content-Length). Seeded so runs are
// reproducible.
func newChaosServer(t *testing.T, payload []byte, rng *rand.Rand) *httptest.Server {
	t.Helper()
	var mu sync.Mutex // guards rng; handlers run in parallel workers
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		rh := r.Header.Get("Range")
		if rh == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		var start, end int64
		_, _ = fmt.Sscanf(rh, "bytes=%d-%d", &start, &end)
		if end >= int64(len(payload)) {
			end = int64(len(payload)) - 1
		}
		mu.Lock()
		roll := rng.IntN(100)
		mu.Unlock()
		switch {
		case roll < 70: // serve correctly
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : end+1])
		case roll < 80: // promise full range, write half, then close
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			half := start + (end-start+1)/2
			_, _ = w.Write(payload[start : half+1])
		case roll < 85: // abrupt connection reset after headers
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
		case roll < 90: // retryable 503 with a short Retry-After
			w.Header().Set("Retry-After", "1")
			http.Error(w, "busy", http.StatusServiceUnavailable)
		case roll < 95: // wrong Content-Range → hard task error
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start+1, end+1, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : end+1])
		default: // wrong Content-Length vs body → short read
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", (end-start+1)/2))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : end+1])
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestChaosServerNoSilentCorruption runs downloads against a server that
// randomly corrupts/truncates/resets responses, with whole-file hash
// verification on. The invariant: a nil error implies byte-perfect
// output — the downloader must either converge to correct bytes or fail,
// never succeed with corrupt data.
func TestChaosServerNoSilentCorruption(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	expected := sha256Hex(payload)
	for seed := uint64(1); seed <= 4; seed++ {
		rng := rand.New(rand.NewPCG(seed, 0xfeed))
		srv := newChaosServer(t, payload, rng)
		out := filepath.Join(t.TempDir(), "out.bin")
		d := NewDownloader(srv.URL, out, Options{
			Quiet:        true,
			NoResume:     true,
			HashAlgo:     "sha256",
			ExpectedHash: expected,
		})
		err := d.Download(testCtx(t, 45*time.Second))
		got, _ := os.ReadFile(out)
		if err == nil && !bytes.Equal(got, payload) {
			t.Fatalf("seed %d: SUCCESS with corrupt output (%d bytes, want %d)", seed, len(got), len(payload))
		}
		t.Logf("seed %d: err=%v", seed, err)
	}
}

// TestChaosServerHardFailClean verifies the "fail cleanly" half of the
// no-silent-corruption invariant: a server that always sends a wrong
// Content-Range must produce a hard error, never a trusted success.
func TestChaosServerHardFailClean(t *testing.T) {
	payload := makePayload(1 * 1024 * 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Header.Get("Range") == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		// Always lie about the served slice.
		w.Header().Set("Content-Range", "bytes 999999-1999999/2000000")
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte{0})
	}))
	t.Cleanup(srv.Close)

	out := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, out, Options{Quiet: true, NoResume: true})
	if err := d.Download(testCtx(t, 30*time.Second)); err == nil {
		t.Fatal("expected a hard error from wrong Content-Range, got success")
	}
	// The output must not be left looking complete.
	if got, _ := os.ReadFile(out); bytes.Equal(got, payload) {
		t.Fatal("output looks complete despite hard failure — must not be trusted")
	}
}

// TestHTTPSDownloadWithCACert exercises the never-before-tested TLS +
// HTTP/2 path: gofetch's transport forces HTTP/2 for ALPN, but every
// other e2e test runs over plain HTTP/1.1. A self-signed test server
// becomes trusted via the --ca-cert PEM, and the handler asserts the
// requests actually arrived over HTTP/2 (ProtoMajor == 2).
func TestHTTPSDownloadWithCACert(t *testing.T) {
	payload := makePayload(2 * 1024 * 1024)
	var sawH2 atomic.Bool
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 {
			sawH2.Store(true)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// Trust the self-signed server cert via the --ca-cert mechanism.
	certPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(certPath, srv.Certificate().Raw, 0o644); err != nil {
		t.Fatal(err)
	}
	// Certificate().Raw is DER, not PEM — encode it.
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(certPath, pemBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	outFile := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, outFile, Options{CACert: certPath, Quiet: true, NoResume: true})
	if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("Download over TLS: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch over TLS: got %d bytes, want %d", len(got), len(payload))
	}
	if !sawH2.Load() {
		t.Error("expected requests to arrive over HTTP/2 (ALPN), got HTTP/1.x")
	}
}

// TestInterruptOnCompleteFileIsSuccess pins the misleading-interrupt bug:
// a Ctrl-C that races a download's completion must not turn a fully-written
// file into an "interrupted; partial progress saved" error. The race window
// (last worker finishes vs the signal) is tiny, so this runs many trials and
// asserts the invariant: a complete file implies a nil error.
func TestInterruptOnCompleteFileIsSuccess(t *testing.T) {
	payload := makePayload(256 * 1024)
	srv := newRangeServer(t, payload, nil)
	for i := 0; i < 300; i++ {
		out := filepath.Join(t.TempDir(), "out.bin")
		d := NewDownloader(srv.URL, out, Options{Quiet: true, NoResume: true})
		ctx, cancel := context.WithCancel(context.Background())
		// Fire the cancel shortly after start, racing the completion.
		timer := time.AfterFunc(300*time.Microsecond, cancel)
		err := d.Download(ctx)
		timer.Stop()
		got, _ := os.ReadFile(out)
		// A sparse file reaches full length by writing only its last byte,
		// so completeness is judged by CONTENT, not size.
		complete := bytes.Equal(got, payload)
		if complete && err != nil {
			t.Fatalf("trial %d: byte-complete file reported error %v", i, err)
		}
		if !complete && err == nil {
			t.Fatalf("trial %d: byte-incomplete file reported success", i)
		}
	}
}

// TestNoGoroutineLeak downloads repeatedly and asserts the goroutine count
// stays flat — catching leaks in the per-body idleBody goroutine, the
// drain-result timers, or the transport's keep-alive machinery that single
// runs would miss.
func TestNoGoroutineLeak(t *testing.T) {
	payload := makePayload(512 * 1024)
	srv := newRangeServer(t, payload, nil)
	dl := func() {
		out := filepath.Join(t.TempDir(), "o.bin")
		d := NewDownloader(srv.URL, out, Options{Quiet: true, NoResume: true})
		if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
			t.Fatalf("Download: %v", err)
		}
		d.Close() // release keep-alive connections, as the CLI does
	}
	// Warm up pools/transports.
	for i := 0; i < 5; i++ {
		dl()
	}
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		dl()
	}
	runtime.GC()
	time.Sleep(100 * time.Millisecond) // let keep-alive/timers settle
	after := runtime.NumGoroutine()
	if after > before+8 {
		t.Fatalf("goroutine leak: %d -> %d after 50 downloads", before, after)
	}
}
