package fetch

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// TestProbeDecisionMatrix verifies the probe's (HEAD, then range-GET
// fallback) returns the correct {supportsRanges, total} for every
// realistic server response combination.
func TestProbeDecisionMatrix(t *testing.T) {
	const total = 1024
	payload := make([]byte, total)

	cases := []struct {
		name       string
		headStatus int
		headCL     bool
		headAR     bool
		range206   bool // if false, range-GET returns 200
		wantRanges bool
		wantTotal  int64
	}{
		{"HEAD 200 + CL + AR", 200, true, true, true, true, total},
		{"HEAD 200 + CL, no AR", 200, true, false, true, false, total},
		{"HEAD 200, no CL", 200, false, true, true, true, total},
		{"HEAD 405, range 206", 405, true, true, true, true, total},
		{"HEAD 405, range 200", 405, true, true, false, false, total},
		{"HEAD 500 (permanent)", 500, true, true, true, false, 0}, // probe error
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					if tt.headCL {
						w.Header().Set("Content-Length", strconv.Itoa(total))
					}
					if tt.headAR {
						w.Header().Set("Accept-Ranges", "bytes")
					}
					w.WriteHeader(tt.headStatus)
					return
				}
				if tt.range206 {
					w.Header().Set("Content-Range", contentRange(0, int64(total-1), total))
					w.Header().Set("Content-Length", "1")
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(payload[:1])
					return
				}
				w.Header().Set("Content-Length", strconv.Itoa(total))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(payload)
			}))
			defer srv.Close()

			d := NewDownloader(srv.URL, "test.bin", Options{})
			info, err := d.probeURL(testCtx(t, 10*time.Second), srv.URL)
			if tt.wantTotal == 0 {
				if err == nil {
					t.Fatalf("expected probe error, got %+v", info)
				}
				return
			}
			if err != nil {
				t.Fatalf("probeURL: %v", err)
			}
			if info.supportsRanges != tt.wantRanges {
				t.Errorf("supportsRanges = %v, want %v", info.supportsRanges, tt.wantRanges)
			}
			if info.total != tt.wantTotal {
				t.Errorf("total = %d, want %d", info.total, tt.wantTotal)
			}
		})
	}
}

// TestProbeWorkersMatchActualPath pins that ProbeURL reports the worker
// count a download would actually use: 1 for tiny files (single-stream
// fallback), the auto-tuned count otherwise.
func TestProbeWorkersMatchActualPath(t *testing.T) {
	t.Run("tiny file probes one worker", func(t *testing.T) {
		srv := newRangeServer(t, makePayload(1024), nil)
		p, err := ProbeURL(testCtx(t, 10*time.Second), srv.URL)
		if err != nil {
			t.Fatalf("ProbeURL: %v", err)
		}
		if p.Workers != 1 {
			t.Errorf("Workers = %d, want 1 (single-stream fallback)", p.Workers)
		}
	})

	t.Run("large file probes range workers", func(t *testing.T) {
		srv := newRangeServer(t, makePayload(4<<20), nil)
		p, err := ProbeURL(testCtx(t, 10*time.Second), srv.URL)
		if err != nil {
			t.Fatalf("ProbeURL: %v", err)
		}
		if p.Workers <= 1 {
			t.Errorf("Workers = %d, want auto-tuned range count > 1", p.Workers)
		}
		if !p.SupportsRanges {
			t.Error("want SupportsRanges for a range-capable server")
		}
	})
}
