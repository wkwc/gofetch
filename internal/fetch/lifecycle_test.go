package fetch

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeakAcrossDownloads guards 100go #62: every goroutine a
// download starts (workers, work-stealing monitor, idle-body helpers,
// transport loops) must stop when the download ends and Close runs.
// Async runtime timers make exact counts flaky, so the test settles
// (GC + sleep) and allows a slack of 2 across 3 downloads.
func TestNoGoroutineLeakAcrossDownloads(t *testing.T) {
	payload := make([]byte, 8<<20)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		if h := r.Header.Get("Range"); h != "" {
			start, end := parseRangeFast(h)
			if end >= len(payload) {
				end = len(payload) - 1
			}
			w.Header().Set("Content-Range", contentRange(int64(start), int64(end), len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : end+1])
			return
		}
		w.Header().Set("Content-Length", "8388608")
		_, _ = w.Write(payload)
	}))
	defer srv.Close()
	// Warm up (transport init, pool fill), then settle.
	for i := 0; i < 2; i++ {
		d := NewDownloader(srv.URL, filepath.Join(t.TempDir(), "w.bin"), Options{Quiet: true, NoResume: true})
		if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
			t.Fatal(err)
		}
		d.Close()
	}
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	base := runtime.NumGoroutine()
	for i := 0; i < 3; i++ {
		d := NewDownloader(srv.URL, filepath.Join(t.TempDir(), "o.bin"), Options{Quiet: true, NoResume: true})
		if err := d.Download(testCtx(t, 30*time.Second)); err != nil {
			t.Fatal(err)
		}
		d.Close()
	}
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	after := runtime.NumGoroutine()
	t.Logf("goroutines before=%d after=%d delta=%d", base, after, after-base)
	if after-base > 2 {
		t.Fatalf("goroutine leak: before=%d after=%d", base, after)
	}
}
