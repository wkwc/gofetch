package fetch

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
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
		start, end, ok := parseRangeHeader(h, size)
		if !ok {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", contentRange(start, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
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
