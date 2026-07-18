package fetch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
			w.Header().Set("Content-Length", i64toa(int64(size)))
			w.WriteHeader(http.StatusOK)
			w.Write(payload)
			return
		}
		start, end := parseRangeFast(h)
		w.Header().Set("Content-Range", rangeHdr(start, end, size))
		w.Header().Set("Content-Length", i64toa(int64(end-start+1)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(payload[start : end+1])
	}))
	defer srv.Close()

	dir := b.TempDir()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		out := filepath.Join(dir, "out.bin")
		d := NewDownloader(srv.URL, out, Options{Quiet: true, NoResume: true})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := d.Download(ctx); err != nil {
			b.Fatal(err)
		}
		cancel()
	}
	b.SetBytes(int64(size))
}

// parseRangeFast extracts start/end from "bytes=START-END" — manual parsing
// is faster than fmt.Sscanf in the hot server loop.
func parseRangeFast(h string) (int, int) {
	const p = "bytes="
	if len(h) < len(p) {
		return 0, 0
	}
	s := h[len(p):]
	dash := strings.IndexByte(s, '-')
	if dash < 1 || dash >= len(s)-1 {
		return 0, 0
	}
	return i64atoi(s[:dash]), i64atoi(s[dash+1:])
}

func rangeHdr(start, end, total int) string {
	const maxLen = 64
	var b [maxLen]byte
	i := copy(b[:], "bytes ")
	i += i64toaAppend(b[i:], int64(start))
	b[i] = '-'
	i++
	i += i64toaAppend(b[i:], int64(end))
	b[i] = '/'
	i++
	i += i64toaAppend(b[i:], int64(total))
	return string(b[:i])
}

func i64toaAppend(b []byte, n int64) int {
	if n == 0 {
		b[0] = '0'
		return 1
	}
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	copy(b, b[i:])
	return len(b) - i
}

func i64atoi(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		n = n*10 + int(s[i]-'0')
	}
	return n
}

func i64toa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
