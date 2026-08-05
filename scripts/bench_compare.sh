#!/usr/bin/env bash
set -uo pipefail  # not -e, we'll track errors ourselves

cd "$(dirname "$0")"

GOFETCH=$(pwd)/gofetch
BENCHSERVER=$(pwd)/benchserver
ARIA2C=$(which aria2c 2>/dev/null || echo "")

if [ -z "$ARIA2C" ]; then
    echo "ERROR: aria2c not found"
    exit 1
fi

echo "go:    $GOFETCH"
echo "aria:  $ARIA2C"
echo "bench: $BENCHSERVER"

# Build
echo ""
echo "=== Building ==="
go build -o "$GOFETCH" ./cmd/gofetch/ 2>&1
go build -o "$BENCHSERVER" ./cmd/benchserver/ 2>&1

RUNS="${RUNS:-3}"
SIZE_MB="${1:-64}"
TMPDIR=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$TMPDIR"' EXIT

wait_for_server() {
    for i in {1..30}; do
        if curl -s -o /dev/null -w "%{http_code}" "$1" 2>/dev/null | grep -q 200; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# Start benchserver
BENCH_SIZE_MB=$SIZE_MB "$BENCHSERVER" &
SERVER_PID=$!
sleep 0.5

if ! wait_for_server "http://127.0.0.1:9120/"; then
    echo "Server failed to start"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo ""
echo "==================================================="
echo "  Direct comparison: gofetch vs aria2c (${SIZE_MB}MB file)"
echo "  Runs per scenario: $RUNS"
echo "==================================================="

echo ""
echo "--- gofetch (auto workers, no flags) ---"
for i in $(seq 1 $RUNS); do
    rm -f "$TMPDIR/gf.bin"
    T1=$(date +%s%N)
    "$GOFETCH" -q -o "$TMPDIR/gf.bin" http://127.0.0.1:9120/ 2>/dev/null && \
        echo "  run $i: $(( ($(date +%s%N) - T1) / 1000000 )) ms" || \
        echo "  run $i: FAIL"
done
echo ""
echo "--- gofetch (-v, verbose for comparison) ---"
for i in $(seq 1 $RUNS); do
    rm -f "$TMPDIR/gfv.bin"
    T1=$(date +%s%N)
    "$GOFETCH" -q -o "$TMPDIR/gfv.bin" http://127.0.0.1:9120/ 2>/dev/null && \
        echo "  run $i: $(( ($(date +%s%N) - T1) / 1000000 )) ms" || \
        echo "  run $i: FAIL"
done

echo ""
echo "--- aria2c (defaults, 5 connections) ---"
for i in $(seq 1 $RUNS); do
    rm -f "$TMPDIR/aria.bin"
    T1=$(date +%s%N)
    aria2c -q -d "$TMPDIR" -o "aria.bin" http://127.0.0.1:9120/ 2>/dev/null && \
        echo "  run $i: $(( ($(date +%s%N) - T1) / 1000000 )) ms" || \
        echo "  run $i: FAIL"
done

echo ""
echo "--- aria2c (-x 16, 16 parallel) ---"
for i in $(seq 1 $RUNS); do
    rm -f "$TMPDIR/aria16.bin"
    T1=$(date +%s%N)
    aria2c -q -x 16 -s 16 -d "$TMPDIR" -o "aria16.bin" http://127.0.0.1:9120/ 2>/dev/null && \
        echo "  run $i: $(( ($(date +%s%N) - T1) / 1000000 )) ms" || \
        echo "  run $i: FAIL"
done

echo ""
echo "--- aria2c (-x 16, 1MB split) ---"
for i in $(seq 1 $RUNS); do
    rm -f "$TMPDIR/aria16s.bin"
    T1=$(date +%s%N)
    aria2c -q -x 16 -s 16 -k 1M -d "$TMPDIR" -o "aria16s.bin" http://127.0.0.1:9120/ 2>/dev/null && \
        echo "  run $i: $(( ($(date +%s%N) - T1) / 1000000 )) ms" || \
        echo "  run $i: FAIL"
done

# Stop server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=== Benchmarks complete ==="