#!/usr/bin/env bash
# Shared helpers for scripts/bench.sh. Source, don't execute directly.

BENCH_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${BENCH_LIB_DIR}/.." && pwd)"

GOFETCH="${GOFETCH:-${REPO_ROOT}/gofetch}"
BENCHSERVER="${BENCHSERVER:-${REPO_ROOT}/benchserver}"
URL="${URL:-http://127.0.0.1:9120/}"
TMPDIR=""
SERVER_PID=""

bench_setup() {
  cd "${REPO_ROOT}" || exit
  TMPDIR="$(mktemp -d)"
  trap bench_cleanup EXIT
}

bench_cleanup() {
  [ -n "${SERVER_PID}" ] && kill "${SERVER_PID}" 2>/dev/null || true
  [ -n "${TMPDIR}" ] && rm -rf "${TMPDIR}"
}

bench_build() {
  # Build once per invocation: bench.sh all runs quick/full/compare in turn.
  if [ "${BENCH_BUILT:-0}" = "1" ]; then
    return
  fi
  echo "=== Building ==="
  go build -o "${GOFETCH}" ./cmd/gofetch/
  go build -o "${BENCHSERVER}" ./cmd/benchserver/
  BENCH_BUILT=1
}

bench_wait_for_server() {
  for i in {1..30}; do
    if curl -s -o /dev/null -w "%{http_code}" "${URL}" 2>/dev/null | grep -q 200; then
      return 0
    fi
    sleep 0.1
  done
  echo "Server failed to start" >&2
  return 1
}

bench_start_server() {
  local size_mb="${1:-64}"
  BENCH_SIZE_MB="${size_mb}" "${BENCHSERVER}" &
  SERVER_PID=$!
  sleep 0.5
  bench_wait_for_server
}

bench_stop_server() {
  [ -n "${SERVER_PID}" ] && kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
  sleep 0.2
}

bench_maybe_hyperfine() {
  if command -v hyperfine >/dev/null 2>&1; then
    echo "=== Hyperfine: Size scaling (auto workers) ==="
    local size
    for size in 16 64 256; do
      bench_start_server "${size}"
      hyperfine --warmup 2 --min-runs 3 \
        -n "gofetch-${size}MB" "${GOFETCH} --allow-loopback -o ${TMPDIR}/b${size}.bin ${URL}" \
        --cleanup "rm -f ${TMPDIR}/b${size}.bin" \
        2>&1 || true
      bench_stop_server
    done
  else
    echo "hyperfine not installed; skipping size-scaling comparison"
  fi
}