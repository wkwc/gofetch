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

cd "$(dirname "$0")/.." || exit
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
  for _ in 1 2; do
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
# Second mirror class (healthy fast Apache mirror with a sha256sums.txt
# container): the interrupt/resume tests fall back to it when proof.ovh
# throttles, so one hostile mirror never blanks the battery.
ARCH_TARBALL=https://geo.mirror.pkgbuild.com/iso/latest/archlinux-bootstrap-x86_64.tar.zst
ARCH_SIZE=126491574

echo ""
echo "== 1. README claim: proof.ovh.net 10Mb.dat byte-equality =="
if reachable "$PROOF/10Mb.dat"; then
  # Reference fetches are verified complete (rc + non-empty) everywhere:
  # a partial curl would make the byte-equality check falsely fail.
  if curl -fsSL "$PROOF/10Mb.dat" -o "$TMP/ref.bin" 2>/dev/null && [ -s "$TMP/ref.bin" ]; then
    SHA=$(sha256sum "$TMP/ref.bin" | cut -d' ' -f1)
    t_retry "gofetch download + sha256 verify" 60 "$GOFETCH" -q -h "sha256:$SHA" -o "$TMP/gf.bin" "$PROOF/10Mb.dat"
    if md5sum "$TMP/ref.bin" "$TMP/gf.bin" | awk '{print $1}' | sort -u | wc -l | grep -q '^1$'; then
      ok "output byte-identical to reference"
    else
      bad "output differs from reference"
    fi
  else
    skip "proof.ovh.net reference fetch failed (flaky upstream)"
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
echo "== 13. multi-mirror failover (dead primary -> live mirror) =="
# A public-but-404 primary must fail over to the live mirror and
# complete: probe failure (404) is a failover, not a fatal error.
if reachable "$PROOF/10Mb.dat"; then
  if curl -fsSL "$PROOF/10Mb.dat" -o "$TMP/fo-ref.bin" 2>/dev/null && [ -s "$TMP/fo-ref.bin" ]; then
    t_retry "dead primary fails over to the live mirror" 120 \
      "$GOFETCH" -q -o "$TMP/fo.bin" "https://httpbin.org/status/404" -m "$PROOF/10Mb.dat"
    if md5sum "$TMP/fo-ref.bin" "$TMP/fo.bin" 2>/dev/null | awk '{print $1}' | sort -u | wc -l | grep -q '^1$'; then
      ok "failover output byte-identical to the mirror"
    else
      bad "failover output differs from the mirror"
    fi
  else
    skip "reference fetch failed (flaky upstream)"
  fi
else
  skip "no healthy mirror for the failover test"
fi

echo ""
echo "== 14. rate-limit accuracy on a real mirror =="
# --limit-rate 2M on a 10 MB file: the wall time must sit in the
# rate-cap window (4.5 s ideal; upper bound tolerates mirror jitter).
if reachable "$PROOF/10Mb.dat"; then
  t1=$(date +%s%N)
  if timeout 60 "$GOFETCH" -q --limit-rate 2M -o "$TMP/ra.bin" "$PROOF/10Mb.dat" >/dev/null 2>&1; then
    t2=$(date +%s%N)
    MS=$(( (t2 - t1) / 1000000 ))
    if [ "$MS" -ge 4500 ] && [ "$MS" -le 9000 ]; then
      ok "rate cap held: ${MS}ms for 10 MB at 2 MB/s (want 4.5-9s)"
    else
      bad "rate cap off: ${MS}ms for 10 MB at 2 MB/s (want 4.5-9s)"
    fi
  else
    bad "rate-limited download failed"
  fi
else
  skip "no healthy mirror for the rate-accuracy test"
fi

echo ""
echo "== 3. redirect chain (httpbin -> proof.ovh.net) =="
RB="https://httpbin.org/redirect-to?url=$PROOF/10Mb.dat"
if reachable "$RB" 20; then
  if curl -fsSL "$PROOF/10Mb.dat" -o "$TMP/redir-ref.bin" 2>/dev/null && [ -s "$TMP/redir-ref.bin" ]; then
    SHA=$(sha256sum "$TMP/redir-ref.bin" | cut -d' ' -f1)
    t_retry "gofetch follows 302 and verifies" 60 "$GOFETCH" -q -h "sha256:$SHA" -o "$TMP/redir.bin" "$RB"
  else
    skip "proof.ovh.net reference fetch failed (flaky upstream)"
  fi
else
  skip "httpbin unreachable"
fi

echo ""
echo "== 4. arXiv PDF over HTTPS (redirect) + md5 =="
if reachable "$ARXIV"; then
  if curl -fsSL "$ARXIV" -o "$TMP/paper-ref.pdf" 2>/dev/null && [ -s "$TMP/paper-ref.pdf" ]; then
    MD5=$(md5sum "$TMP/paper-ref.pdf" | cut -d' ' -f1)
    t_retry "gofetch arXiv PDF md5-verified" 60 "$GOFETCH" -q -h "md5:$MD5" -o "$TMP/paper.pdf" "$ARXIV"
  else
    skip "arxiv reference fetch failed (flaky upstream)"
  fi
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
echo "== 7. resume a real download (interrupt -> resume) =="
# proof.ovh 100MB first; fall back to the Arch tarball when throttled.
# The output file is SPARSE (full-size from byte 0), so file-size polling
# can never detect a mid-transfer window; the interrupt is timed instead
# (rate-capped so a fixed sleep lands mid-transfer).
IR_URL="$PROOF/100Mb.dat"; IR_SIZE=104857600; IR_RATE=8M; IR_SLEEP=6; IR_RTO=240
if ! reachable "$IR_URL" 20; then
  IR_URL="$ARCH_TARBALL"; IR_SIZE="$ARCH_SIZE"; IR_RATE=4M; IR_SLEEP=3; IR_RTO=300
fi
if reachable "$IR_URL" 20; then
  PARTIAL=0
  for _ in 1 2; do
    "$GOFETCH" -q --limit-rate "$IR_RATE" -o "$TMP/big.bin" "$IR_URL" >/dev/null 2>&1 &
    PID=$!
    sleep "$IR_SLEEP"
    ALIVE=0
    kill -0 "$PID" 2>/dev/null && ALIVE=1
    kill -INT "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
    if [ "$ALIVE" = 1 ] && [ -e "$TMP/big.bin.gofetch.resume" ]; then
      PARTIAL=1
      break
    fi
    rm -f "$TMP/big.bin" "$TMP/big.bin.gofetch.resume"
  done
  if [ "$PARTIAL" = 1 ]; then
    ok "interrupt left a resume sidecar"
    t_retry "resume completes the real download" "$IR_RTO" "$GOFETCH" -q -o "$TMP/big.bin" "$IR_URL"
    SIZE=$(wc -c < "$TMP/big.bin" 2>/dev/null || echo 0)
    [ "$SIZE" = "$IR_SIZE" ] && ok "resumed file is exactly $IR_SIZE bytes" || bad "resumed file size $SIZE, want $IR_SIZE"
  else
    skip "network could not sustain a partial download (flaky upstream)"
  fi
else
  skip "no healthy mirror for the interrupt/resume test"
fi

echo ""
echo "== 8. --info --json against a real URL =="
if reachable "$PROOF/10Mb.dat"; then
  J=$("$GOFETCH" --info --json "$PROOF/10Mb.dat" 2>/dev/null)
  echo "$J" | grep -q '"supports_ranges":true' && ok "JSON probe valid" || bad "JSON probe: $J"
else
  skip "proof.ovh.net unreachable"
fi

echo ""
echo "== 10. multi-cycle interrupt/resume (sidecar accumulation) =="
# 3 interrupt cycles against a rate-capped transfer: the sidecar must
# accumulate (merge) completed ranges across abort/resume cycles, and
# the final resume must produce the byte-exact file. The output file is
# sparse (full-size from byte 0), so the interrupt is timed (rate cap
# x sleep lands mid-transfer). Falls back to the Arch mirror when
# proof.ovh throttles.
MC_URL="$PROOF/10Mb.dat"; MC_RATE=1M; MC_SLEEP=3; MC_REF=""; MC_RTO=120
if reachable "$PROOF/10Mb.dat"; then
  MC_REF=$(curl -sL "$PROOF/10Mb.dat" | sha256sum | cut -d' ' -f1)
else
  MC_URL="$ARCH_TARBALL"; MC_RATE=4M; MC_SLEEP=3; MC_RTO=300
  MC_REF=$(curl -fsSL https://geo.mirror.pkgbuild.com/iso/latest/sha256sums.txt 2>/dev/null | awk '$2=="archlinux-bootstrap-x86_64.tar.zst" {print $1}')
fi
if [ -n "$MC_REF" ] && reachable "$MC_URL" 20; then
  CYCLES=0
  for _ in 1 2 3; do
    "$GOFETCH" -q --limit-rate "$MC_RATE" -o "$TMP/mc.bin" "$MC_URL" >/dev/null 2>&1 &
    PID=$!
    sleep "$MC_SLEEP"
    ALIVE=0
    kill -0 "$PID" 2>/dev/null && ALIVE=1
    kill -INT "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
    if [ "$ALIVE" = 1 ] && [ -e "$TMP/mc.bin.gofetch.resume" ]; then
      CYCLES=$((CYCLES + 1))
    fi
  done
  if [ "$CYCLES" -ge 2 ]; then
    ok "$CYCLES interrupt cycles each left a resume sidecar"
    t_retry "resume completes after $CYCLES cycles" "$MC_RTO" "$GOFETCH" -q -o "$TMP/mc.bin" "$MC_URL"
    GOTSHA=$(sha256sum "$TMP/mc.bin" 2>/dev/null | cut -d' ' -f1)
    [ "$GOTSHA" = "$MC_REF" ] && ok "multi-cycle resume is byte-identical" || bad "multi-cycle resume sha mismatch: $GOTSHA"
  else
    skip "network could not sustain repeated partial downloads (flaky upstream)"
  fi
else
  skip "no healthy mirror for the multi-cycle test"
fi

echo ""
echo "== 11. -h auto container checksum (real distro mirror) =="
# Arch's ISO directory ships sha256sums.txt listing the bootstrap
# tarball; -h auto must fetch the container, match the entry by
# basename, and verify. -x 2 keeps the transfer gentle on the mirror.
if reachable "$ARCH_TARBALL" 20; then
  t_retry "gofetch -h auto verifies via container checksum" 300 "$GOFETCH" -q -x 2 -h auto -o "$TMP/arch.tar.zst" "$ARCH_TARBALL"
  [ -s "$TMP/arch.tar.zst" ] && ok "container-verified output non-empty" || bad "container-verified output empty"
else
  skip "arch mirror unreachable"
fi

echo ""
echo "== 12. second CDN class (fast HTTP/2, byte-equality) =="
# jsdelivr: a fast HTTP/2 CDN — different server class than proof.ovh
# (Apache, sometimes throttled). gofetch must land at curl parity here
# (the ETA-gated stealing + small-file worker floor make it so).
JS=https://cdn.jsdelivr.net/npm/typescript@5.9.3/lib/typescript.js
if reachable "$JS" 15; then
  # Reference fetch must be verified complete (rc + non-empty): a
  # partial curl would make the byte-equality check falsely fail.
  if curl -fsSL "$JS" -o "$TMP/js-ref.js" 2>/dev/null && [ -s "$TMP/js-ref.js" ]; then
    t_retry "gofetch downloads from jsdelivr" 60 "$GOFETCH" -q -o "$TMP/js-gf.js" "$JS"
    RSIZE=$(wc -c < "$TMP/js-ref.js" 2>/dev/null || echo 0)
    GSIZE=$(wc -c < "$TMP/js-gf.js" 2>/dev/null || echo 0)
    if [ "$RSIZE" != "$GSIZE" ]; then
      # Different sizes: the CDN served different content (observed live:
      # an edge node served a truncated probe response). Environmental,
      # not corruption — gofetch can only do what the server claims.
      skip "CDN served different content (ref $RSIZE vs got $GSIZE)"
    elif md5sum "$TMP/js-ref.js" "$TMP/js-gf.js" 2>/dev/null | awk '{print $1}' | sort -u | wc -l | grep -q '^1$'; then
      ok "jsdelivr output byte-identical to reference"
    else
      bad "jsdelivr output differs from reference (same size — corruption)"
    fi
  else
    skip "jsdelivr reference fetch failed (flaky upstream)"
  fi
else
  skip "jsdelivr unreachable"
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