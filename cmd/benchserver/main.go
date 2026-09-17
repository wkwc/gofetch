// benchserver is a high-performance test HTTP server for benchmarking gofetch.
// It serves a fixed payload and supports Range requests.
package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxSizeMB = 4096

func main() {
	sizeMB, _ := strconv.Atoi(os.Getenv("BENCH_SIZE_MB"))
	if sizeMB <= 0 {
		sizeMB = 64
	}
	if sizeMB > maxSizeMB {
		sizeMB = maxSizeMB
	}
	payload := make([]byte, sizeMB*1024*1024)
	for i := range payload {
		payload[i] = byte(i % 251) // deterministic pseudo-random
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}

		// Parse "bytes=START-END" with strconv (no fmt reflection on the
		// per-request measurement path — this server's overhead is the
		// benchmark's noise floor).
		start, end, ok := parseBenchRange(rangeHeader)
		if !ok {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if end >= int64(len(payload)) {
			end = int64(len(payload)) - 1
		}
		if start < 0 || start > end {
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		w.Header().Set("Content-Range", benchRangeHeader(start, end, int64(len(payload))))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)

		// Stream in 4MB chunks for highest throughput under concurrency
		const chunk = 4 << 20 // 4 MiB
		for cur := start; cur <= end; cur += chunk {
			stop := cur + chunk - 1
			if stop > end {
				stop = end
			}
			_, _ = w.Write(payload[cur : stop+1])
			if r.Context().Err() != nil {
				return
			}
		}
	})

	addr := "127.0.0.1:9120"
	if a := os.Getenv("BENCH_ADDR"); a != "" {
		addr = a
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           nil,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	fmt.Fprintf(os.Stderr, "benchserver listening on %s (%d MB payload)\n", addr, sizeMB)
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// parseBenchRange parses "bytes=START-END" without fmt.Sscanf reflection.
func parseBenchRange(h string) (start, end int64, ok bool) {
	const p = "bytes="
	if !strings.HasPrefix(h, p) {
		return 0, 0, false
	}
	rest := h[len(p):]
	dash := strings.IndexByte(rest, '-')
	if dash < 1 || dash >= len(rest)-1 {
		return 0, 0, false
	}
	s, err1 := strconv.ParseInt(rest[:dash], 10, 64)
	e, err2 := strconv.ParseInt(rest[dash+1:], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return s, e, true
}

// benchRangeHeader builds a "bytes START-END/TOTAL" header with AppendInt.
func benchRangeHeader(start, end, total int64) string {
	var b [64]byte
	buf := b[:0]
	buf = append(buf, "bytes "...)
	buf = strconv.AppendInt(buf, start, 10)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, end, 10)
	buf = append(buf, '/')
	buf = strconv.AppendInt(buf, total, 10)
	return string(buf)
}
