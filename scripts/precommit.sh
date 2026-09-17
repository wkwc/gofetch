#!/usr/bin/env bash
# Fast local gate: formatting + vet + focused unit tests (+race) + build,
# plus golangci-lint when installed. Full suite (race-shuffle-count=2,
# fuzz smoke, smoke.sh, govulncheck, deadcode) stays in CI.
# Runs in <60s; installed as .git/hooks/pre-commit to gate every commit.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
step() { printf 'precommit: %s\n' "$*"; }

# find_tool resolves a helper from PATH or the Go bin dir (GOPATH/bin is
# often not on PATH in ephemeral shells, e.g. ~/go/bin/golangci-lint).
find_tool() {
  if command -v "$1" >/dev/null; then command -v "$1"; return 0; fi
  local gobin
  gobin="$(go env GOPATH 2>/dev/null)/bin/$1"
  if [ -x "$gobin" ]; then echo "$gobin"; return 0; fi
  return 1
}
GOFUMPT="$(find_tool gofumpt || true)"
GOLANGCI="$(find_tool golangci-lint || true)"

# gofumpt is stricter than gofmt and is the repo standard (.golangci.yml);
# fall back to gofmt -s when gofumpt is nowhere to be found.
if [ -n "$GOFUMPT" ]; then
  step "gofumpt check ($GOFUMPT)"
  unformatted=$("$GOFUMPT" -l ./cmd ./internal)
  tool=gofumpt
else
  step "gofmt -s check (gofumpt not installed)"
  unformatted=$(gofmt -s -l ./cmd ./internal)
  tool="gofmt -s"
fi
if [ -n "$unformatted" ]; then
  echo "not $tool clean:"; echo "$unformatted"
  fail=1
else
  echo "  ok"
fi

step "go vet ./..."
if ! go vet ./...; then fail=1; else echo "  ok"; fi

step "focused unit tests (parsers + range algebra + manifest)"
if ! go test -count=1 -run 'TestParse|TestSplit|TestDedup|TestUncompleted|TestSeed|TestManifest|TestSidecar|TestHash|TestVerify|TestChecksum|TestBackoff|TestScale' ./internal/fetch/ ./cmd/...; then
  fail=1
else
  echo "  ok"
fi

# The pool/queue/range hot paths are concurrency-sensitive; a focused
# -race slice is cheap (~2s) and catches data races pre-commit.
step "focused race tests (pool + queue + range algebra)"
if ! go test -race -count=1 -run 'TestSplit|TestDedup|TestUncompleted|TestManifest|TestAcquire|TestBuf|TestPool|TestQueue|TestVerify|TestParse' ./internal/fetch/; then
  fail=1
else
  echo "  ok"
fi

step "build ./..."
if ! go build ./...; then fail=1; else echo "  ok"; fi

step "shell scripts (bash -n + shellcheck -S warning if present)"
sh_fail=0
for f in scripts/*.sh; do
  bash -n "$f" || sh_fail=1
done
if command -v shellcheck >/dev/null; then
  shellcheck -S warning scripts/*.sh || sh_fail=1
else
  echo "  shellcheck not installed — bash -n only (CI enforces shellcheck)"
fi
if [ "$sh_fail" -eq 0 ]; then echo "  ok"; else fail=1; fi

# Strongest gate, soft locally: CI pins golangci-lint v2.13.2 and enforces
# it. Run it here when present. A copy built against an older Go toolchain
# cannot even load this repo's config — that is an environment problem,
# not a code problem, so skip (loudly) instead of failing.
if [ -n "$GOLANGCI" ]; then
  step "golangci-lint config verify + run ($GOLANGCI)"
  # `run` alone does not enforce schema strictly (CI's action runs
  # `config verify` first and fails on it), so verify explicitly for
  # local/CI parity.
  if verify_out=$("$GOLANGCI" config verify 2>&1); then
    if lint_out=$("$GOLANGCI" run --timeout=5m ./... 2>&1); then
      echo "  ok"
    elif echo "$lint_out" | grep -q "lower than the targeted Go version"; then
      echo "  skip ($GOLANGCI too old for the go1.27 toolchain — CI enforces v2.13.2)"
    else
      echo "$lint_out"
      fail=1
    fi
  elif echo "$verify_out" | grep -q "lower than the targeted Go version"; then
    echo "  skip ($GOLANGCI too old for the go1.27 toolchain — CI enforces v2.13.2)"
  else
    echo "$verify_out"
    fail=1
  fi
else
  step "golangci-lint (not installed — CI enforces)"
  echo "  skip"
fi

if [ "$fail" -ne 0 ]; then
  echo "precommit: FAILED — fix the above before committing"
  exit 1
fi
echo "precommit: all gates passed"
