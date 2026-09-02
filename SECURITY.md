# Security policy — gofetch

## Threat model

gofetch is a **client CLI** that fetches attacker- or operator-supplied URLs onto the local machine.

| Risk | Mitigation |
|------|------------|
| SSRF to link-local / private IPs | Dial-time IP checks; redirect policy; fail-closed DNS errors |
| Path traversal via `-o` | Caller-controlled path (document; do not run as root on untrusted args) |
| Partial writes / resume corruption | Resume sidecars + optional hash verification (`-h`) |
| Supply chain of gofetch itself | Signed GitHub releases + attestations |

## Non-goals

- Not a multi-tenant download service.
- Loopback dials are blocked by default: production never enables them, and the
  CLI only lifts the block via the explicit `--allow-loopback` flag, which exists
  solely for the repo's own benchmark scripts and local tests. Do not pass
  `--allow-loopback` with URLs you do not trust — it disables the SSRF guard for
  loopback/private destinations.

## Reporting

Open a private security advisory on the repository.

## Release verification

```bash
sha256sum -c SHA256SUMS
gh attestation verify gofetch-<ver>-linux-amd64 --repo <owner>/gofetch
```
