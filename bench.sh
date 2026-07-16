#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

BENCHSERVER=./benchserver
GOFETCH=./gofetch
ADDR=":9120"
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
    
    echo "--- Single worker (baseline) ---"
    $GOFETCH -w 1 -o "$TMPDIR/out_1w.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_1w.bin"
    
    echo "--- Auto workers (default) ---"
    $GOFETCH -o "$TMPDIR/out_auto.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_auto.bin"
    
    echo "--- 4 workers ---"
    $GOFETCH -w 4 -o "$TMPDIR/out_4w.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_4w.bin"
    
    echo "--- 8 workers ---"
    $GOFETCH -w 8 -o "$TMPDIR/out_8w.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_8w.bin"
    
    echo "--- 16 workers ---"
    $GOFETCH -w 16 -o "$TMPDIR/out_16w.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_16w.bin"
    
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

# Hyperfine comparison: 1w vs auto vs 8w
echo ""
echo "=== Hyperfine: Worker count comparison (64 MB) ==="
BENCH_SIZE_MB=64 $BENCHSERVER &
sleep 0.5

hyperfine --warmup 2 --min-runs 3 \
    -n "gofetch-1w"  "$GOFETCH -w 1 -o $TMPDIR/b1.bin $URL" \
    -n "gofetch-auto" "$GOFETCH -o $TMPDIR/b2.bin $URL" \
    -n "gofetch-4w"  "$GOFETCH -w 4 -o $TMPDIR/b4.bin $URL" \
    -n "gofetch-8w"  "$GOFETCH -w 8 -o $TMPDIR/b8.bin $URL" \
    -n "gofetch-16w" "$GOFETCH -w 16 -o $TMPDIR/b16.bin $URL" \
    --cleanup "rm -f $TMPDIR/b1.bin $TMPDIR/b2.bin $TMPDIR/b4.bin $TMPDIR/b8.bin $TMPDIR/b16.bin" \
    2>&1

kill %1 2>/dev/null; wait %1 2>/dev/null || true

# Size scaling test
for size in 1 16 64 256; do
    run_bench $size "${size}MB"
done

echo ""
echo "=== Summary ==="
echo "All benchmarks completed."
