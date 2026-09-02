#!/usr/bin/env bash
# Fuzz gofetch's parsers and range algebra.
#
# Usage:
#   ./scripts/fuzz.sh                 # quick fuzz of every target (default 30s each)
#   ./scripts/fuzz.sh FuzzParseUint   # fuzz one target until Ctrl-C / FUZZTIME elapses
#   FUZZTIME=2m ./scripts/fuzz.sh FuzzManifestJSON
#
# New interesting inputs are saved under internal/fetch/testdata/fuzz/
# (commit them; they become regression seeds for normal `go test`).
set -euo pipefail

cd "$(dirname "$0")/.."

TARGET="${1:-}"
FUZZTIME="${FUZZTIME:-30s}"

list_targets() {
  grep -oE '^func (Fuzz[A-Za-z0-9]+)' internal/fetch/fuzz_test.go | awk '{print $2}'
}

if [ -n "$TARGET" ]; then
  echo "==> fuzzing ${TARGET} (${FUZZTIME}; Ctrl-C to stop early)"
  exec go test ./internal/fetch/ -run '^$' -fuzz "^${TARGET}$" -fuzztime "${FUZZTIME}"
fi

echo "==> fuzzing every target (${FUZZTIME} each; override with FUZZTIME=2m)"
for t in $(list_targets); do
  echo "--- ${t}"
  go test ./internal/fetch/ -run '^$' -fuzz "^${t}$" -fuzztime "${FUZZTIME}" || true
done
echo "==> done"