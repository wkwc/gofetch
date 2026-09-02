package fetch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  struct {
			start, end, total int64
			ok                bool
		}
	}{
		{"basic", "bytes 0-99/100", struct {
			start, end, total int64
			ok                bool
		}{0, 99, 100, true}},
		{"large", "bytes 1000-1999/5000", struct {
			start, end, total int64
			ok                bool
		}{1000, 1999, 5000, true}},
		{"missing prefix", "0-99/100", struct {
			start, end, total int64
			ok                bool
		}{0, 0, 0, false}},
		{"malformed", "bytes 0/100", struct {
			start, end, total int64
			ok                bool
		}{0, 0, 0, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, e, total, ok := parseContentRange(tt.input)
			if ok != tt.want.ok || s != tt.want.start || e != tt.want.end || total != tt.want.total {
				t.Errorf("parseContentRange(%q) = (%d,%d,%d,%v), want (%d,%d,%d,%v)",
					tt.input, s, e, total, ok, tt.want.start, tt.want.end, tt.want.total, tt.want.ok)
			}
		})
	}
}

func TestProbeRangeGetMalformedContentRange(t *testing.T) {
	// 206 with a malformed Content-Range but a sound Content-Length must
	// fall back to CL and disable ranges rather than hard-fail the probe.
	payload := makePayload(32 * 1024)
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		hits++
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.Header().Set("Content-Range", "not-a-range")
		w.WriteHeader(http.StatusPartialContent)
	}))
	t.Cleanup(srv.Close)

	d := NewDownloader(srv.URL, "test.bin", Options{})
	info, err := d.probeURL(testCtx(t, 10*time.Second), srv.URL)
	if err != nil {
		t.Fatalf("probeURL: %v", err)
	}
	if info.supportsRanges {
		t.Error("expected ranges disabled on malformed Content-Range")
	}
	if info.total != int64(len(payload)) {
		t.Errorf("total = %d, want %d", info.total, len(payload))
	}
}
