#!/usr/bin/env bash
# Real-internet throughput comparison: gofetch vs aria2c vs wget2 vs curl
# on the same large file for a fixed window. Every tool gets identical
# wall time via timeout -s INT; throughput = bytes/window.
#
#   ./scripts/bench_real.sh                  # default 12s window, Arch ISO
#   WINDOW=15 ./scripts/bench_real.sh https://example.com/big.iso
set -uo pipefail

cd "$(dirname "$0")/.."

URL="${URL:-https://mirror.rackspace.com/archlinux/iso/latest/archlinux-x86_64.iso}"
GOFETCH="${GOFETCH:-$(pwd)/gofetch}"
WINDOW="${WINDOW:-12}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "== building =="
go build -o "$GOFETCH" ./cmd/gofetch

# run_bw NAME -- CMD... : run CMD for WINDOW seconds, report MB/s.
run_bw() {
  local name=$1; shift; [ "$1" = "--" ] && shift
  local out="$TMP/$name.bin"
  local t0 t1 ns bytes
  t0=$(date +%s%N)
  timeout -s INT "$WINDOW" "$@" >/dev/null 2>&1
  t1=$(date +%s%N)
  ns=$((t1 - t0))
  bytes=$(wc -c "$out" 2>/dev/null | awk '{print $1}')
  printf '%-14s %6.1f MB/s  (%s bytes in %.1fs)\n' \
    "$name" "$(awk -v b="${bytes:-0}" -v n="$ns" 'BEGIN{print b/n*1e9/1048576}')" \
    "${bytes:-0}" "$(awk -v n="$ns" 'BEGIN{print n/1e9}')"
}

aria2c="$(command -v aria2c || true)"
wget2="$(command -v wget2 || true)"

echo ""
echo "window=${WINDOW}s  url=${URL}"
echo ""

echo "--- gofetch worker scaling (does more connections help?) ---"
for w in 4 8 16 32; do
  run_bw "gofetch-x$w" -- "$GOFETCH" -q -x "$w" -o "$TMP/gofetch-x$w.bin" "$URL"
done

if [ -n "$aria2c" ]; then
  echo "--- aria2c ---"
  run_bw "aria2c-def"  -- aria2c -q --file-allocation=none -d "$TMP" -o aria2c-def.bin "$URL"
  run_bw "aria2c-x16"  -- aria2c -q -x 16 -s 16 --file-allocation=none -d "$TMP" -o aria2c-x16.bin "$URL"
fi

if [ -n "$wget2" ]; then
  echo "--- wget2 (h2 chunked, 30 streams/conn) ---"
  run_bw "wget2" -- wget2 --chunk-size=1M -O "$TMP/wget2.bin" "$URL"
fi

echo "--- single stream ---"
run_bw "curl" -- curl -s -o "$TMP/curl.bin" "$URL"

echo ""
echo "== done (identical ${WINDOW}s window; higher MB/s wins) =="