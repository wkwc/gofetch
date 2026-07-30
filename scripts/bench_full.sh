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

wait_for_server() {
    local url=$1
    for i in {1..30}; do
        if curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null | grep -q 200; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

run_test() {
    local size_mb=$1
    local label=$2
    
    echo ""
    echo "=== ${label} (${size_mb} MB) ==="
    
    BENCH_SIZE_MB=$size_mb $BENCHSERVER &
    SERVER_PID=$!
    
    if ! wait_for_server "$URL"; then
        echo "Server failed to start"
        kill $SERVER_PID 2>/dev/null
        return 1
    fi
    
    # Auto (default)
    echo "--- auto workers (default) ---"
    $GOFETCH -o "$TMPDIR/out_auto.bin" "$URL" 2>&1 | tail -10
    rm -f "$TMPDIR/out_auto.bin"
    
    # Quiet mode
    echo "--- quiet (-q) ---"
    $GOFETCH -q -o "$TMPDIR/out_quiet.bin" "$URL" 2>&1
    rm -f "$TMPDIR/out_quiet.bin"
    
    kill $SERVER_PID 2>/dev/null
    wait $SERVER_PID 2>/dev/null || true
    sleep 0.2
}

echo ""
echo "=========================================="
echo "  gofetch benchmark suite"
echo "=========================================="
echo ""

# Warm up
run_test 1 "warmup"

# Main benchmarks
run_test 16 "16 MB"
run_test 64 "64 MB"
run_test 256 "256 MB"

# Hyperfine comparison - just auto vs different implementations
# Since we removed -w, we can't easily test different worker counts
# But we can test different tools if available

echo ""
echo "=== Hash verification test ==="
BENCH_SIZE_MB=16 $BENCHSERVER &
SERVER_PID=$!

if wait_for_server "$URL"; then
    # Get expected hash
    curl -s "$URL" | sha256sum | cut -d' ' -f1 > /tmp/expected_hash.txt
    EXPECTED=$(cat /tmp/expected_hash.txt)
    echo "Expected SHA256: $EXPECTED"

    echo "--- With correct hash ---"
    $GOFETCH -o "$TMPDIR/verify_ok.bin" -h "sha256:$EXPECTED" "$URL" 2>&1 | tail -5
    rm -f "$TMPDIR/verify_ok.bin"

    echo "--- With wrong hash (should fail) ---"
    $GOFETCH -o "$TMPDIR/verify_fail.bin" -h "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef" "$URL" 2>&1 | tail -3
    rm -f "$TMPDIR/verify_fail.bin"
fi

kill %1 2>/dev/null; wait %1 2>/dev/null || true

# Resume test
echo ""
echo "=== Resume test (64 MB) ==="
BENCH_SIZE_MB=64 $BENCHSERVER &
SERVER_PID=$!

if wait_for_server "$URL"; then
    echo "--- First run: cancel after 1.5s ---"
    timeout 1.5s $GOFETCH -o "$TMPDIR/resume.bin" "$URL" 2>&1 | tail -5 || true

    echo "--- Second run: resume and complete ---"
    $GOFETCH -o "$TMPDIR/resume.bin" "$URL" 2>&1 | tail -5
    rm -f "$TMPDIR/resume.bin" "$TMPDIR/resume.bin.gofetch.resume"
fi

kill %1 2>/dev/null; wait %1 2>/dev/null || true

# Quiet mode test
echo ""
echo "=== Quiet mode (-q) ==="
BENCH_SIZE_MB=4 $BENCHSERVER &
SERVER_PID=$!

if wait_for_server "$URL"; then
    echo "Output with -q (prints only filename):"
    $GOFETCH -q -o "$TMPDIR/quiet.bin" "$URL" 2>&1
    rm -f "$TMPDIR/quiet.bin"
fi

kill %1 2>/dev/null; wait %1 2>/dev/null || true

# Verbose mode test
echo ""
echo "=== Verbose mode (-v) ==="
BENCH_SIZE_MB=4 $BENCHSERVER &
SERVER_PID=$!

if wait_for_server "$URL"; then
    $GOFETCH -v -o "$TMPDIR/verbose.bin" "$URL" 2>&1 | head -20
    rm -f "$TMPDIR/verbose.bin"
fi

kill %1 2>/dev/null; wait %1 2>/dev/null || true

echo ""
echo "=== All benchmarks completed ==="