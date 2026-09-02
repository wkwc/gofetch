#!/usr/bin/env bash
# Install gofetch from a GitHub Release (checksum-verified).
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/<owner>/gofetch/main/scripts/install.sh | bash
#   VERSION=v1.0.0 REPO=owner/gofetch ./scripts/install.sh
set -euo pipefail

REPO="${REPO:-wkwc/gofetch}"
VERSION="${VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/bin}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported arch: ${ARCH}" >&2; exit 1 ;;
esac
case "${OS}" in
  linux|darwin) ;;
  *) echo "unsupported os: ${OS}" >&2; exit 1 ;;
esac

if [ "${VERSION}" = "latest" ]; then
  # Prefer the GitHub CLI when available; fall back to a JSON scrape.
  if command -v gh >/dev/null 2>&1; then
    VERSION="$(gh api "repos/${REPO}/releases/latest" --jq .tag_name 2>/dev/null || true)"
  fi
  if [ -z "${VERSION}" ]; then
    VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
  fi
fi
[ -n "${VERSION}" ] || { echo "could not resolve version" >&2; exit 1; }

BASE="gofetch-${VERSION}-${OS}-${ARCH}"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${BASE}"
SUMS_URL="https://github.com/${REPO}/releases/download/${VERSION}/SHA256SUMS"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

echo "==> downloading ${BASE}"
curl -fsSL "${URL}" -o "${TMP}/${BASE}"
curl -fsSL "${SUMS_URL}" -o "${TMP}/SHA256SUMS"

echo "==> verifying SHA-256"
(
  cd "${TMP}"
  # Only check our asset line
  grep " ${BASE}\$" SHA256SUMS | sha256sum -c -
)

mkdir -p "${INSTALL_DIR}"
install -m 0755 "${TMP}/${BASE}" "${INSTALL_DIR}/gofetch"
echo "==> installed ${INSTALL_DIR}/gofetch (${VERSION})"
echo "    ensure ${INSTALL_DIR} is on PATH"
"${INSTALL_DIR}/gofetch" -version || true
