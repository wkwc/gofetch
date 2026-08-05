#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

BENCHSERVER=./benchserver
GOFETCH=./gofetch
URL="http://127.0.0.1:9120/"
TMPDIR=$(mktemp -d)
trap 'kill %1 2>/dev/null; rm -rf "$TMPDIR"' EXIT

# Build
echo "=== Building ==="
go build -o benchserver ./cmd/benchserver/ 2>&1
go build -o gofetch ./cmd/gofetch/ 2>&1

run_bench() {
    local size_mb=$1
    local label=$2

    echo ""
    echo "=== Benchmark: ${label} (${size_mb} MB) ==="
    
    # Start server
    BENCH_SIZE_MB=$size_mb $BENCHSERVER &
    SERVER_PID=$!
    sleep 0.5
    
    # Verify server is up
    if ! curl -s -o /dev/null -w "%{http_code}" "$URL" | grep -q 200; then
        echo "Server failed to start"
        kill $SERVER_PID 2>/dev/null
        return 1
    fi
    
    echo "--- Default auto-configured ---"
    $GOFETCH -o "$TMPDIR/out_auto.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_auto.bin"
    
    echo "--- Quiet mode ---"
    $GOFETCH -q -o "$TMPDIR/out_quiet.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_quiet.bin"
    
    kill $SERVER_PID 2>/dev/null
    wait $SERVER_PID 2>/dev/null || true
    sleep 0.3
}

echo ""
echo "=========================================="
echo "  gofetch benchmark suite"
echo "=========================================="
echo ""

# Warm up
run_bench 1 "warmup"

# Size scaling test
for size in 1 16 64 256; do
    run_bench $size "${size}MB"
done

# Hyperfine comparison: different sizes with auto-configured workers
echo ""
echo "=== Hyperfine: Size scaling (auto workers) ==="
for size in 16 64 256; do
    BENCH_SIZE_MB=$size $BENCHSERVER &
    SERVER_PID=$!
    sleep 0.5
    hyperfine --warmup 2 --min-runs 3 \
        -n "gofetch-${size}MB" "$GOFETCH -o $TMPDIR/b${size}.bin $URL" \
        --cleanup "rm -f $TMPDIR/b${size}.bin" \
        2>&1 || true
    kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null || true
done

echo ""
echo "=== Summary ==="
echo "All benchmarks completed."
