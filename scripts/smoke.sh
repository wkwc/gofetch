#!/usr/bin/env bash
# Black-box operational smoke test for the gofetch CLI.
#
# Exercises every flag against the local benchserver (or a real URL) and
# asserts exit codes, stdout/stderr separation, sidecars, and signal
# handling — the behaviours unit tests can't reach through run().
#
# Usage:
#   ./scripts/smoke.sh                          # local benchserver (default)
#   ./scripts/smoke.sh https://example.com/f.bin # any real URL
set -uo pipefail

cd "$(dirname "$0")/.."
source ./scripts/bench_lib.sh

URL="${1:-http://127.0.0.1:9120/}"
GOFETCH="${GOFETCH:-$(pwd)/gofetch}"
TMP="$(mktemp -d)"
trap 'bench_cleanup; rm -rf "$TMP"' EXIT

PASS=0
FAIL=0

ok()  { PASS=$((PASS + 1)); printf 'ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL + 1)); printf 'FAIL %s\n' "$1"; }

# run CODE -- CMD... : runs CMD, asserts exit code == CODE.
run() {
  local want=$1; shift
  [ "${1:-}" = "--" ] && shift
  "$@" >/dev/null 2>&1
  local got=$?
  if [ "$got" -eq "$want" ]; then
    ok "exit $got: $*"
  else
    bad "exit $got (want $want): $*"
  fi
}

# runl = run against the loopback benchserver (needs --allow-loopback).
runl() { local want=$1; shift; [ "${1:-}" = "--" ] && shift; run "$want" -- "$GOFETCH" --allow-loopback "$@"; }
# runx = run with no URL at all (version/help/usage).
runx() { local want=$1; shift; [ "${1:-}" = "--" ] && shift; run "$want" -- "$GOFETCH" "$@"; }

echo "== building =="
go build -o "$GOFETCH" ./cmd/gofetch
go build -o "$BENCHSERVER" ./cmd/benchserver/

LOCAL=0
if [ "${URL#http://127.0.0.1:}" != "$URL" ] || [ "${URL#http://localhost:}" != "$URL" ]; then
  LOCAL=1
  bench_start_server 2
  SHA256="$(curl -s "$URL" | sha256sum | cut -d' ' -f1)"
  MD5="$(curl -s "$URL" | md5sum | cut -d' ' -f1)"
fi

F="$TMP/a.bin"

echo "== basic =="
runl 0 -- -q -o "$F" "$URL"
[ -s "$F" ] && ok "file written" || bad "file missing/empty"
runx 2
runx 2 -- -bogus

echo "== stdout/stderr separation =="
OUT="$("$GOFETCH" --allow-loopback -q -o "$TMP/q.bin" "$URL" 2>/dev/null)"
[ -n "$OUT" ] && ok "quiet prints filename on stdout" || bad "quiet stdout empty"
ERR="$("$GOFETCH" --allow-loopback -q -o "$TMP/e.bin" "$URL" 2>&1 >/dev/null)"
[ -z "$ERR" ] && ok "quiet writes nothing to stderr" || bad "quiet stderr: $ERR"

echo "== output paths =="
mkdir -p "$TMP/dir"
runl 0 -- -q -o "$TMP/dir" "$URL/file.bin"
[ -s "$TMP/dir/file.bin" ] && ok "-o existing dir downloads into it" || bad "-o dir"
runl 0 -- -q -o "$TMP/multi" "$URL/one.bin" "$URL/two.bin"
{ [ -s "$TMP/multi/one.bin" ] && [ -s "$TMP/multi/two.bin" ]; } && ok "multi-URL into dir" || bad "multi-URL"

echo "== hash verification =="
if [ "$LOCAL" = 1 ]; then
  runl 0 -- -q -h "sha256:$SHA256" -o "$TMP/h.bin" "$URL"
  runl 1 -- -q -h "sha256:0000000000000000000000000000000000000000000000000000000000000000" -o "$TMP/h.bin" "$URL"
  runl 0 -- -q -h "md5:$MD5" -o "$TMP/md.bin" "$URL"
  runl 1 -- -q -h "md5:00000000000000000000000000000000" -o "$TMP/md.bin" "$URL"
  runl 0 -- -q -h "$SHA256" -o "$TMP/bare.bin" "$URL"
  printf '%s  file.bin\n' "$SHA256" > "$TMP/file.bin.sha256"
  runl 0 -- -q -o "$TMP/file.bin" "$URL"
else
  echo "   (skipping hash/auto tests for non-local URL)"
fi

echo "== flags =="
runl 0 -- -q -v -o "$TMP/v.bin" "$URL"
runl 0 -- -q --no-resume -o "$TMP/nr.bin" "$URL"
runl 0 -- -q -x 4 --buf-size 64k -o "$TMP/x.bin" "$URL"
runl 0 -- -q --max-retries 3 -o "$TMP/mr.bin" "$URL"
runl 0 -- -q -H 'X-Smoke: 1' -A 'smoke/1.0' -o "$TMP/hdr.bin" "$URL"
runl 1 -- -q -x 999 -o "$TMP/x.bin" "$URL"
runl 1 -- -q --max-retries 101 -o "$TMP/x.bin" "$URL"
runx 0 -- --version
runx 0 -- -help

echo "== probe =="
runl 0 -- --info "$URL"
"$GOFETCH" --allow-loopback --info "$URL" 2>/dev/null | grep -q 'checksum: none' && ok "--info reports no checksum (none)" || bad "--info checksum line"
runl 0 -- --info --json "$URL"
"$GOFETCH" --allow-loopback --info --json "$URL" 2>/dev/null | grep -q '"supports_ranges"' && ok "--info --json valid JSON" || bad "--info --json"
runl 1 -- --json "$URL"

echo "== manifest =="
if [ "$LOCAL" = 1 ]; then
  runl 0 -- -q -manifest-out "$TMP/m.gofetch.manifest" -o "$TMP/m.bin" "$URL"
  [ -s "$TMP/m.gofetch.manifest" ] && ok "manifest written" || bad "manifest missing"
fi

echo "== no-clobber =="
if [ "$LOCAL" = 1 ]; then
  cp "$F" "$TMP/c.bin"
  runl 0 -- -q --no-clobber -o "$TMP/c.bin" "$URL"
fi

echo "== symlink output rejected =="
if [ "$LOCAL" = 1 ]; then
  touch "$TMP/target.bin"
  ln -s "$TMP/target.bin" "$TMP/link.bin"
  runl 1 -- -q -o "$TMP/link.bin" "$URL"
  [ ! -s "$TMP/target.bin" ] && ok "symlink target untouched" || bad "symlink target modified"
fi

echo "== rate limit (sanity: completes) =="
runl 0 -- -q --limit-rate 10M -o "$TMP/rl.bin" "$URL"

echo "== mirror fallback (bad port then good) =="
if [ "$LOCAL" = 1 ]; then
  runl 0 -- -q -m "http://127.0.0.1:1/mirror.bin" -o "$TMP/mirror.bin" "$URL"
fi

echo "== interrupt (SIGINT -> 130, resume sidecar kept) =="
if [ "$LOCAL" = 1 ]; then
  bench_stop_server
  bench_start_server 32
  "$GOFETCH" --allow-loopback -q --limit-rate 8M -o "$TMP/int.bin" "$URL" >/dev/null 2>&1 &
  PID=$!
  sleep 0.3
  kill -INT "$PID"
  wait "$PID" 2>/dev/null
  CODE=$?
  [ "$CODE" -eq 130 ] && ok "SIGINT exit 130 (got $CODE)" || bad "SIGINT exit $CODE, want 130"
  [ -e "$TMP/int.bin.gofetch.resume" ] && ok "resume sidecar kept" || bad "resume sidecar missing"
  # Resume completes against the still-running 128MB server.
  runl 0 -- -q --limit-rate 8M -o "$TMP/int.bin" "$URL"
  bench_stop_server
fi

echo ""
echo "== smoke: $PASS passed, $FAIL failed =="
[ "$FAIL" -eq 0 ]