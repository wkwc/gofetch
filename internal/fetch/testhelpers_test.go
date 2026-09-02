package fetch

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func init() {
	// httptest binds 127.0.0.1; production SSRF dial still blocks loopback.
	AllowLoopbackDial = true
}

// makePayload generates n random bytes for test fixtures.
func makePayload(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// rangeServerConfig customizes newRangeServer. A nil Head uses the default
// (200 OK with Content-Length). Write lets tests override the per-range body
// (e.g. truncate mid-range); the default streams the full requested span.
type rangeServerConfig struct {
	Head  func(w http.ResponseWriter, r *http.Request, payload []byte)
	Write func(w http.ResponseWriter, r *http.Request, payload []byte, start, end int64)
}

// newRangeServer serves fixed payload bytes, honoring Range requests.
// The optional config customizes HEAD behavior and the per-range writer.
func newRangeServer(t *testing.T, payload []byte, cfg *rangeServerConfig) *httptest.Server {
	t.Helper()
	head := func(w http.ResponseWriter, _ *http.Request, payload []byte) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.WriteHeader(http.StatusOK)
	}
	if cfg != nil && cfg.Head != nil {
		head = cfg.Head
	}
	write := func(w http.ResponseWriter, r *http.Request, payload []byte, start, end int64) {
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
			if r.Context().Err() != nil {
				return
			}
		}
	}
	if cfg != nil && cfg.Write != nil {
		write = cfg.Write
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			head(w, r, payload)
			return
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		start, end, ok := parseRangeHeader(rangeHeader, len(payload))
		if !ok {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		write(w, r, payload, start, end)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// sha256Hex returns the hex SHA-256 of data.
func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// sha512Hex returns the hex SHA-512 of data.
func sha512Hex(data []byte) string {
	h := sha512.Sum512(data)
	return hex.EncodeToString(h[:])
}

// parseRangeHeader parses "bytes=START-END" against a payload of the given
// size, clamping END to size-1. Returns ok=false for malformed or
// unsatisfiable ranges (START<0 or START>END after clamping).
func parseRangeHeader(h string, size int) (start, end int64, ok bool) {
	var s, e int64
	if _, err := fmt.Sscanf(h, "bytes=%d-%d", &s, &e); err != nil {
		return 0, 0, false
	}
	if e >= int64(size) {
		e = int64(size) - 1
	}
	if s < 0 || s > e {
		return 0, 0, false
	}
	return s, e, true
}

// testCtx returns a context canceled by t.Cleanup after the timeout.
func testCtx(t testing.TB, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// downloadAndVerify downloads payload from a fresh range server into a
// temp file with the given options and asserts byte equality. Returns the
// output path for tests that need to inspect it further.
func downloadAndVerify(t *testing.T, payload []byte, opt Options) string {
	t.Helper()
	srv := newRangeServer(t, payload, nil)
	out := filepath.Join(t.TempDir(), "out.bin")
	d := NewDownloader(srv.URL, out, opt)
	if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	return out
}
