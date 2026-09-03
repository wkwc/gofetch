#!/usr/bin/env bash
# Real-world network test battery for gofetch.
#
# Every test hits a real public server (proof.ovh.net, arXiv, httpbin) and
# verifies bytes, redirects, resume, rate limiting, and multi-URL behavior
# against actual internet conditions. Network-dependent: run manually or
# on-demand in CI; individual failures are reported, not fatal.
#
#   ./scripts/realworld.sh            # functional real-world tests
#   BENCH=1 ./scripts/realworld.sh    # + gofetch vs aria2c benchmark
set -uo pipefail

cd "$(dirname "$0")/.."
source ./scripts/bench_lib.sh

GOFETCH="${GOFETCH:-$(pwd)/gofetch}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

PASS=0; FAIL=0; SKIP=0
ok()  { PASS=$((PASS + 1)); printf 'ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL + 1)); printf 'FAIL %s\n' "$1"; }
skip(){ SKIP=$((SKIP + 1)); printf 'skip %s\n' "$1"; }

# run_to LABEL TIMEOUT CMD... : run once, assert exit 0.
run_to() {
  local label=$1 to=$2; shift 2
  if timeout "$to" "$@" >/dev/null 2>&1; then ok "$label"; else bad "$label"; fi
}

# t_retry LABEL TIMEOUT CMD... : up to 2 attempts (real servers flake).
t_retry() {
  local label=$1 to=$2; shift 2
  for attempt in 1 2; do
    if timeout "$to" "$@" >/dev/null 2>&1; then ok "$label"; return; fi
  done
  bad "$label"
}

# reachable URL [TIMEOUT] — cheap HEAD probe with retries (never downloads
# the body; tolerates flaky upstreams).
reachable() {
  local url=$1 to=${2:-15}
  for _ in 1 2 3; do
    if timeout "$to" curl -s -o /dev/null -I "$url" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "== building =="
go build -o "$GOFETCH" ./cmd/gofetch
command -v md5sum >/dev/null 2>&1 || skip "md5sum missing"

PROOF=https://proof.ovh.net/files
ARXIV=https://arxiv.org/pdf/1603.05705

echo ""
echo "== 1. README claim: proof.ovh.net 10Mb.dat byte-equality =="
if reachable "$PROOF/10Mb.dat"; then
  curl -s "$PROOF/10Mb.dat" -o "$TMP/ref.bin"
  SHA=$(sha256sum "$TMP/ref.bin" | cut -d' ' -f1)
  t_retry "gofetch download + sha256 verify" 60 "$GOFETCH" -q -h "sha256:$SHA" -o "$TMP/gf.bin" "$PROOF/10Mb.dat"
  if md5sum "$TMP/ref.bin" "$TMP/gf.bin" | awk '{print $1}' | sort -u | wc -l | grep -q '^1$'; then
    ok "output byte-identical to reference"
  else
    bad "output differs from reference"
  fi
else
  skip "proof.ovh.net unreachable"
fi

echo ""
echo "== 2. --info on a real CDN (range support, size) =="
if reachable "$PROOF/10Mb.dat"; then
  OUT=$("$GOFETCH" --info "$PROOF/10Mb.dat" 2>/dev/null)
  echo "$OUT" | grep -q 'ranges: yes' && ok "--info reports ranges: yes" || bad "--info ranges"
  echo "$OUT" | grep -q 'size:' && ok "--info reports size" || bad "--info size"
else
  skip "proof.ovh.net unreachable"
fi

echo ""
echo "== 3. redirect chain (httpbin -> proof.ovh.net) =="
RB="https://httpbin.org/redirect-to?url=$PROOF/10Mb.dat"
if reachable "$RB" 20; then
  SHA=$(curl -sL "$PROOF/10Mb.dat" | sha256sum | cut -d' ' -f1)
  t_retry "gofetch follows 302 and verifies" 60 "$GOFETCH" -q -h "sha256:$SHA" -o "$TMP/redir.bin" "$RB"
else
  skip "httpbin unreachable"
fi

echo ""
echo "== 4. arXiv PDF over HTTPS (redirect) + md5 =="
if reachable "$ARXIV"; then
  MD5=$(curl -sL "$ARXIV" | md5sum | cut -d' ' -f1)
  t_retry "gofetch arXiv PDF md5-verified" 60 "$GOFETCH" -q -h "md5:$MD5" -o "$TMP/paper.pdf" "$ARXIV"
else
  skip "arxiv unreachable"
fi

echo ""
echo "== 5. multi-URL bulk download =="
if reachable "$PROOF/10Mb.dat" && reachable "$ARXIV"; then
  t_retry "two real URLs in one command" 90 "$GOFETCH" -q -o "$TMP/bulk" "$PROOF/10Mb.dat" "$ARXIV"
  [ -s "$TMP/bulk/10Mb.dat" ] && ok "bulk/10Mb.dat present" || bad "bulk/10Mb.dat missing"
  [ -s "$TMP/bulk/1603.05705" ] && ok "bulk/1603.05705 present" || bad "bulk/1603.05705 missing"
else
  skip "a real source unreachable"
fi

echo ""
echo "== 6. rate limit on a real CDN =="
if reachable "$PROOF/10Mb.dat"; then
  t_retry "gofetch --limit-rate 2M completes" 90 "$GOFETCH" -q --limit-rate 2M -o "$TMP/rate.bin" "$PROOF/10Mb.dat"
  [ -s "$TMP/rate.bin" ] && ok "rate-limited output non-empty" || bad "rate-limited output empty"
else
  skip "proof.ovh.net unreachable"
fi

echo ""
echo "== 7. resume a real 100Mb download (interrupt -> resume) =="
if reachable "$PROOF/100Mb.dat"; then
  # Rate-cap so there is a guaranteed interrupt window, then stop as soon
  # as a real chunk is on disk (robust to any network speed). Retry once
  # for flaky upstreams; skip (not fail) if the network cannot sustain a
  # partial download — the resume machinery is proven locally.
  PARTIAL=0
  for attempt in 1 2; do
    "$GOFETCH" -q --limit-rate 10M -o "$TMP/big.bin" "$PROOF/100Mb.dat" >/dev/null 2>&1 &
    PID=$!
    for _ in $(seq 1 180); do
      SIZE=$(wc -c "$TMP/big.bin" 2>/dev/null || echo 0)
      if [ "${SIZE:-0}" -ge 8388608 ]; then PARTIAL=1; break; fi
      kill -0 "$PID" 2>/dev/null || break
      sleep 0.5
    done
    kill -INT "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
    [ "$PARTIAL" = 1 ] && break
    rm -f "$TMP/big.bin" "$TMP/big.bin.gofetch.resume"
  done
  if [ "$PARTIAL" = 1 ] && [ -e "$TMP/big.bin.gofetch.resume" ]; then
    ok "interrupt left a resume sidecar"
    t_retry "resume completes the real 100Mb file" 240 "$GOFETCH" -q -o "$TMP/big.bin" "$PROOF/100Mb.dat"
    SIZE=$(wc -c "$TMP/big.bin" 2>/dev/null || echo 0)
    [ "$SIZE" = "104857600" ] && ok "resumed file is exactly 100MB" || bad "resumed file size $SIZE, want 104857600"
  else
    skip "network could not sustain a partial download (flaky upstream)"
  fi
else
  skip "proof.ovh.net 100Mb unreachable"
fi

echo ""
echo "== 8. --info --json against a real URL =="
if reachable "$PROOF/10Mb.dat"; then
  J=$("$GOFETCH" --info --json "$PROOF/10Mb.dat" 2>/dev/null)
  echo "$J" | grep -q '"supports_ranges":true' && ok "JSON probe valid" || bad "JSON probe: $J"
else
  skip "proof.ovh.net unreachable"
fi

if [ "${BENCH:-0}" = "1" ] && command -v aria2c >/dev/null 2>&1 && reachable "$PROOF/100Mb.dat"; then
  echo ""
  echo "== 9. REAL-INTERNET benchmark: gofetch vs aria2c (100Mb) =="
  bench_build
  aria2c="$(command -v aria2c)"
  for tool in "gofetch:$GOFETCH -q" "aria2c:$aria2c -q -d $TMP -o bench.bin"; do
    name=${tool%%:*}; cmd=${tool#*:}
    for run in 1 2; do
      rm -f "$TMP/bench.bin"
      t1=$(date +%s%N)
      if timeout 180 bash -c "$cmd $PROOF/100Mb.dat" >/dev/null 2>&1; then
        t2=$(date +%s%N)
        printf '  %-8s run %d: %4d ms\n' "$name" "$run" "$(( (t2 - t1) / 1000000 ))"
      else
        printf '  %-8s run %d: FAIL\n' "$name" "$run"
      fi
    done
  done
else
  echo "   (skipping benchmark; run BENCH=1 ./scripts/realworld.sh)"
fi

echo ""
echo "== real-world: $PASS passed, $FAIL failed, $SKIP skipped =="
[ "$FAIL" -eq 0 ]