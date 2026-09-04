#!/usr/bin/env bash
# Consolidated benchmark suite for gofetch.
#
# Usage:
#   ./scripts/bench.sh quick   [SIZE_MB]   # auto/quiet timing per size + optional hyperfine
#   ./scripts/bench.sh full    [SIZE_MB]   # sizes + hash verify + resume + mode tests
#   ./scripts/bench.sh compare [SIZE_MB]   # gofetch vs aria2c (requires aria2c)
#   ./scripts/bench.sh all     [SIZE_MB]   # quick + full + compare
#
# Environment:
#   RUNS      iterations for timed scenarios (default 3)
#   URL       benchserver base URL (default http://127.0.0.1:9120/)
set -uo pipefail

cd "$(dirname "$0")" || exit
source ./bench_lib.sh

RUNS="${RUNS:-3}"

# run_timed runs tool+flags with `-o OUT URL` RUNS times, printing
# wall-clock ms per run. stderr is silenced so the timing loop stays clean.
run_timed() {
  local label="$1" out="$2" ; shift 2
  echo "--- ${label}"
  for i in $(seq 1 "${RUNS}"); do
    rm -f "${out}"
    local t1 t2
    t1=$(date +%s%N)
    if "$@" -o "${out}" "${URL}" 2>/dev/null; then
      t2=$(date +%s%N)
      echo "  run ${i}: $(( (t2 - t1) / 1000000 )) ms"
    else
      echo "  run ${i}: FAIL"
    fi
  done
}

cmd_quick() {
  local size_mb="${1:-64}"
  echo "== quick: auto/quiet timing across sizes =="
  bench_build
  for size in 1 16 64 256; do
    bench_start_server "${size}"
    run_timed "${size}MB auto" "${TMPDIR}/a${size}.bin" "${GOFETCH}" --allow-loopback
    run_timed "${size}MB quiet" "${TMPDIR}/q${size}.bin" "${GOFETCH}" --allow-loopback -q
    bench_stop_server
  done
  bench_maybe_hyperfine
}

cmd_full() {
  local size_mb="${1:-64}"
  echo "== full: sizes + hash verify + resume + mode tests (default ${size_mb}MB) =="
  bench_build

  # Warm up
  bench_start_server 1
  "${GOFETCH}" --allow-loopback -o "${TMPDIR}/warmup.bin" "${URL}" 2>&1 | tail -3 || true
  rm -f "${TMPDIR}/warmup.bin"
  bench_stop_server

  for size in 16 64 256; do
    echo ""
    echo "=== ${size} MB ==="
    bench_start_server "${size}"
    "${GOFETCH}" --allow-loopback -o "${TMPDIR}/auto_${size}.bin" "${URL}" 2>&1 | tail -3
    rm -f "${TMPDIR}/auto_${size}.bin"
    bench_stop_server
  done

  echo ""
  echo "=== Hash verification (16 MB) ==="
  bench_start_server 16
  local expected
  expected=$(curl -s "${URL}" | sha256sum | cut -d' ' -f1)
  echo "Expected SHA256: ${expected}"
  "${GOFETCH}" --allow-loopback -o "${TMPDIR}/verify_ok.bin" -h "sha256:${expected}" "${URL}" 2>&1 | tail -3 || true
  rm -f "${TMPDIR}/verify_ok.bin"
  echo "--- Wrong hash (should fail)"
  "${GOFETCH}" --allow-loopback -o "${TMPDIR}/verify_fail.bin" \
    -h "sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef" \
    "${URL}" 2>&1 | tail -3 || true
  rm -f "${TMPDIR}/verify_fail.bin"
  bench_stop_server

  echo ""
  echo "=== Resume test (64 MB) ==="
  bench_start_server 64
  echo "--- First run: cancel after 1.5s"
  timeout 1.5s "${GOFETCH}" --allow-loopback -o "${TMPDIR}/resume.bin" "${URL}" 2>&1 | tail -3 || true
  echo "--- Second run: resume and complete"
  "${GOFETCH}" --allow-loopback -o "${TMPDIR}/resume.bin" "${URL}" 2>&1 | tail -3 || true
  rm -f "${TMPDIR}/resume.bin" "${TMPDIR}/resume.bin.gofetch.resume"
  bench_stop_server

  echo ""
  echo "=== Mode tests (4 MB) ==="
  bench_start_server 4
  echo "--- Quiet (-q):"
  "${GOFETCH}" --allow-loopback -q -o "${TMPDIR}/quiet.bin" "${URL}" 2>&1 || true
  echo "--- Verbose (-v):"
  "${GOFETCH}" --allow-loopback -v -o "${TMPDIR}/verbose.bin" "${URL}" 2>&1 | head -20 || true
  echo "--- Manifest (-manifest-out):"
  "${GOFETCH}" --allow-loopback -o "${TMPDIR}/mf.bin" -manifest-out "${TMPDIR}/mf.gofetch.manifest" "${URL}" 2>&1 | tail -3 || true
  ls -l "${TMPDIR}/mf.gofetch.manifest" 2>/dev/null || true
  bench_stop_server

  echo ""
  echo "=== All benchmarks completed ==="
}

cmd_compare() {
  local size_mb="${1:-64}"
  local aria2c
  aria2c=$(command -v aria2c 2>/dev/null || true)
  if [ -z "${aria2c}" ]; then
    echo "ERROR: aria2c not found" >&2
    exit 1
  fi
  echo "== compare: gofetch vs aria2c (${size_mb}MB) =="
  bench_build
  echo "go:    ${GOFETCH}"
  echo "aria:  ${aria2c}"

  bench_start_server "${size_mb}"
  echo ""
  echo "=== gofetch (auto workers) ==="
  run_timed "gofetch auto" "${TMPDIR}/gf.bin" "${GOFETCH}" --allow-loopback -q
  echo "--- gofetch verbose (-v) ---"
  run_timed "gofetch -v" "${TMPDIR}/gfv.bin" "${GOFETCH}" --allow-loopback -q -v
  echo "--- aria2c (defaults, 5 connections) ---"
  run_timed "aria2c default" "${TMPDIR}/aria.bin" aria2c -q -d "${TMPDIR}" -o "aria.bin"
  echo "--- aria2c (-x 16) ---"
  run_timed "aria2c -x16" "${TMPDIR}/aria16.bin" aria2c -q -x 16 -s 16 -d "${TMPDIR}" -o "aria16.bin"
  echo "--- aria2c (-x 16, 1MB split) ---"
  run_timed "aria2c -x16 -k1M" "${TMPDIR}/aria16s.bin" aria2c -q -x 16 -s 16 -k 1M -d "${TMPDIR}" -o "aria16s.bin"
  bench_stop_server

  echo ""
  echo "=== Benchmark complete ==="
}

usage() {
  echo "usage: $0 {quick|full|compare|all} [SIZE_MB]" >&2
  exit 2
}

case "${1:-}" in
  quick)   bench_setup; cmd_quick "${2:-64}" ;;
  full)    bench_setup; cmd_full "${2:-64}" ;;
  compare) bench_setup; cmd_compare "${2:-64}" ;;
  all)     bench_setup; cmd_quick "${2:-64}"; cmd_full "${2:-64}"; cmd_compare "${2:-64}" ;;
  *)       usage ;;
esac

echo "== done =="