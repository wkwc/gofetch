package fetch

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// BenchmarkParallelDownload measures parallel-download throughput on loopback.
func BenchmarkParallelDownload(b *testing.B) {
	const size = 16 * 1024 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 7)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "")
		h := r.Header.Get("Range")
		if h == "" {
			w.Header().Set("Content-Length", strconv.Itoa(size))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		start, end := parseRangeFast(h)
		w.Header().Set("Content-Range", contentRange(int64(start), int64(end), size))
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : end+1])
	}))
	defer srv.Close()

	dir := b.TempDir()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		out := filepath.Join(dir, "out.bin")
		d := NewDownloader(srv.URL, out, Options{Quiet: true, NoResume: true})
		if err := d.Download(testCtx(b, 30*time.Second)); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(size))
}

// parseRangeFast extracts start/end from "bytes=START-END" via the stdlib
// (strconv.Atoi uses optimized asm; the previous hand-rolled loop saved
// nothing and duplicated what the stdlib already vectorizes).
func parseRangeFast(h string) (start, end int) {
	const p = "bytes="
	if !strings.HasPrefix(h, p) {
		return 0, 0
	}
	s := h[len(p):]
	dash := strings.IndexByte(s, '-')
	if dash < 1 || dash >= len(s)-1 {
		return 0, 0
	}
	start, err1 := strconv.Atoi(s[:dash])
	end, err2 := strconv.Atoi(s[dash+1:])
	if err1 != nil || err2 != nil {
		return 0, 0
	}
	return start, end
}
