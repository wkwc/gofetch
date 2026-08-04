// benchserver is a high-performance test HTTP server for benchmarking gofetch.
// It serves a fixed payload and supports Range requests.
package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
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

		// Parse "bytes=START-END"
		var start, end int64
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
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

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
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

	addr := ":9120"
	if a := os.Getenv("BENCH_ADDR"); a != "" {
		addr = a
	}

	fmt.Fprintf(os.Stderr, "benchserver listening on %s (%d MB payload)\n", addr, sizeMB)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
