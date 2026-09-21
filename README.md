# gofetch

## Install

```bash
# From GitHub Releases (checksum-verified)
curl -fsSL https://raw.githubusercontent.com/wkwc/gofetch/main/scripts/install.sh | bash

# Or pin a version
VERSION=v1.0.0 REPO=wkwc/gofetch bash scripts/install.sh
```

Verify releases with `SHA256SUMS` and `gh attestation verify` (see SECURITY.md).

An opinionated concurrent HTTP downloader. Single binary, zero external dependencies — stdlib only.

```
$ gofetch https://proof.ovh.net/files/10Mb.dat
  ######################## 100.0%  10.0 MB / 10.0 MB  1.2 GB/s  ETA 0s

  download complete
  bytes:   10.0 MB
  time:    8ms
  speed:   1.25 GB/s
  workers: 16
```

## Features

| Feature | Flag | Description |
|---|---|---|
| **Output** | `-o PATH` | Output file path (default: basename of URL); an existing directory downloads into it |
| **Multiple URLs** | `gofetch URL1 URL2` | Download several URLs at once (with `-o`, into a directory) |
| **Quiet** | `-q` | Suppress progress bar; print only filename on success |
| **Verbose** | `-v` | Verbose logging to stderr (mirror selection, task starts, retries, chunk verification) |
| **Hash** | `-h SPEC` | Verify integrity. Zero-config by default (auto-detects local sidecar); override with `md5:hex`, `sha1:hex`, `sha256:hex`, `sha512:hex`, `auto`, or a sidecar path. **Note:** `-h` is the hash flag — use `-help` for help |
| **No Resume** | `--no-resume` | Disable resume from `.gofetch.resume` (default: enabled) |
| **Mirrors** | `-m URL1,URL2` | Comma-separated mirror URLs tried in order on failure |
| **Headers** | `-H "Name: value"` | Send a custom header (repeatable; auth/cookies) |
| **User-Agent** | `-A VALUE` | Override the default `gofetch/1.0` User-Agent (alias: `--user-agent`) |
| **Rate limit** | `--limit-rate` | Cap aggregate download speed per file (`500k`, `2M`, `1G`) |
| **Probe** | `--info` | Print size / range support / planned workers / whether `-h auto` would find a checksum, without downloading |
| **JSON probe** | `--info --json` | Emit one JSON object per URL (`url`, `size`, `supports_ranges`, `workers`, `buf_size`, `checksum`) |
| **Workers** | `-x N` / `--workers` | Override the auto-tuned worker count (0 = auto) |
| **Buffer** | `--buf-size` | Override the auto-tuned per-worker read buffer (`64k`, `1M`) |
| **Retries** | `--max-retries` | Override the per-chunk retry budget (0 = auto, default 10) |
| **No Clobber** | `--no-clobber` | Skip downloads whose output file is already complete (a partial download with a resume sidecar is resumed, not skipped) |
| **CA cert** | `--ca-cert PATH` | Trust extra root CAs from a PEM file (private / self-signed dataset mirrors) |
| **Proxy** | `--proxy URL` | HTTP(S)/SOCKS5 proxy; overrides the environment |
| **Manifest** | `-manifest-out PATH` | After download, write a per-chunk integrity manifest to PATH |
| **Local bench** | `--allow-loopback` | Permit loopback/private dials (for the bundled benchserver / tests; unsafe for untrusted URLs) |

## Usage

```bash
# Basic download (auto-verifies a local .sha256/.sha512 sidecar if present)
gofetch https://example.com/file.bin

# Custom output path
gofetch -o out.bin https://example.com/file.bin

# Auto-detect sidecar (local first, else fetch URL.sha256 / URL.sha512)
gofetch -h auto https://example.com/file.bin

# Local sidecar file
gofetch -h /path/to/file.sha256 https://example.com/file.bin

# Mirror fallback (tried in order on failure; bare hostnames get https://)
gofetch -m mirror1.com,mirror2.com https://primary.com/file.bin

# Multiple URLs at once (each to its basename; -o must be a directory)
gofetch -o ~/Downloads https://a.example/x.bin https://b.example/y.bin

# Authenticated / header-bearing request
gofetch -H 'Authorization: Bearer token' -H 'Cookie: session=abc' -o out.bin https://example.com/private.bin

# Cap aggregate bandwidth (per file)
gofetch --limit-rate 2M -o out.bin https://example.com/large.bin

# Probe without downloading (add --json for one machine-readable object per URL)
gofetch --info https://example.com/file.bin

# Escape hatches for the auto-tuned engine
gofetch -x 16 --buf-size 256k -o out.bin https://example.com/large.bin

# Generate a per-chunk integrity manifest after download (chunk-level verification for future runs)
gofetch -manifest-out out.gofetch.manifest -o out.bin https://example.com/file.bin

# Local benchmark against the bundled benchserver (only for trusted local URLs!)
gofetch --allow-loopback -o out.bin http://127.0.0.1:9120/
```

All flags in the table above work as shown; `-q`/`-v` control output verbosity,
`--no-resume` forces a fresh download, and `-A`/`--proxy`/`--no-clobber`/`--ca-cert`
behave per their one-line descriptions. If the server doesn't support `Range`,
gofetch gracefully falls back to a single GET stream.

The progress bar is terminal-aware: on a TTY it renders the live `\r` bar;
when stderr is piped or redirected it stays headless-clean, printing a single
plain final line (no ANSI codes). On Ctrl-C/SIGTERM the partial download is
preserved to the resume sidecar and gofetch exits with status 130, printing
`interrupted; partial progress saved to <out>.gofetch.resume, re-run to resume` —
the resume sidecar is the authoritative completeness marker (its absence means the
file is complete; an interrupted file is full-size but sparse).

## Why

Most "range downloaders" are dumb: split into N chunks, fetch each, merge at the end.
That wastes disk I/O on temp files, breaks when a server is slow, and offers nothing
but "parallel curl."

`gofetch` is built around a few opinionated choices:

1. **Sparse file + `WriteAt`.** The target file is `Truncate`d to its full size up front,
   and every worker writes its bytes directly to the final offsets. No temp files, no merge.
2. **Adaptive work stealing.** A monitor goroutine ticks every 500 ms. If a worker is
   "slow" (on a chunk ≥ 512 KiB and has fetched < 1 MiB after a 1.5 s grace period),
   the monitor *cancels* that worker's HTTP request, splits its remaining range,
   and pushes the unfinished half back to the shared work queue for another worker to grab.
   Stealing is also ETA-gated: when the download is moving well and will finish
   within ~5 s, stealing only churns connections (measured on a shaped mirror:
   1.6× faster with the gate on a 10 MB download).
3. **Lock-free progress.** There is no shared `done` counter — the progress
   display sums worker-local `bytesDone` atomics on demand.
4. **Zero user knobs for the engine.** Workers (capped at 32), buffer size,
   transport tuning, retries, parallelism, HTTP version, and chunk size are
   all derived from the server's `Content-Length`, `runtime.NumCPU()` and
   round-trip-time class. The flags are only for things that genuinely
   require user input (`-o`, `-q`, `-v`, `-h`, `--no-resume`, plus
   opt-in `-H`/`-A`/`--limit-rate`/`--proxy`).
5. **Small file fallback.** Files smaller than 64 KiB skip the worker/monitor
   stack entirely and use a single GET stream (parallel overhead dominates).
6. **Chunk-level integrity.** When a `<output>.gofetch.manifest` is present,
   each chunk is verified against its expected hash immediately after being
   written to disk (see Integrity Verification).

## Error Handling

- **Transient network errors** (connection reset, unexpected EOF, timeout) are retried with exponential backoff. Retries measure lack of progress, not slowness: a chunk that keeps writing bytes between retries is on a slow-but-alive link and never exhausts the budget; only repeated no-progress requeues (default 10) fail the chunk.
- **HTTP 429/503/502/504/408** are retried respecting `Retry-After` header.
- **Permanent errors** (invalid URL, unsupported status codes) fail immediately.
- **HTTP 416** (Range Not Satisfiable) is a hard error for the range (not treated as complete); the worker fails that task rather than marking unwritten bytes done.

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All requested downloads succeeded (or were skipped by `--no-clobber`) |
| `1` | Any download/probe failed (multi-URL mode continues past per-file failures) |
| `2` | Usage / flag error |
| `130` | Interrupted (Ctrl-C / SIGTERM / SIGHUP); partial saved to `<out>.gofetch.resume`, re-run to resume |

The output path is printed to stdout only on success; all progress, summary, and
errors go to stderr, so `gofetch -q URL 2>/dev/null` prints just the filename.

## Integrity Verification

The `-h` flag supports multiple formats:

| Format | Example |
|---|---|
| *(default)* | No `-h` needed: a local `<output>.md5`/`.sha1`/`.sha256`/`.sha512` sidecar is auto-detected next to the output file |
| `md5:hex` | `gofetch -h md5:...` (Zenodo / 4TU / Planck datasets publish MD5) |
| `sha1:hex` | `gofetch -h sha1:...` |
| `sha256:hex` | `gofetch -h sha256:abc123...` |
| `sha512:hex` | `gofetch -h sha512:abc123...` |
| `auto` | local sidecar first, else fetches `URL.md5`/`URL.sha1`/`URL.sha256`/`URL.sha512` sidecars, then falls back to container checksum files (`sha256sums.txt` / `SHA256SUMS`) in the same directory (Linux ISO mirrors). Scheme matches the primary URL; SSRF host guard applies |
| bare hex (32 → md5, 40 → sha1, 64 → sha256, 128 → sha512) | `gofetch -h abc123...` |
| local sidecar file | `gofetch -h /path/file.sha256 https://...` |

MD5 and SHA-1 are supported for **integrity verification** of third-party
dataset files (the algorithms those publishers ship); they are not
collision-resistant, so prefer sha256/sha512 when tamper resistance matters.
Sidecar files use common formats (`<hash> [filename]` or just `<hash>`);
algorithm is inferred from hash length or file extension, and explicit `-h`
values override auto-detection.

A remotely auto-discovered checksum (`-h auto` fetching `URL.sha256` and
friends) protects against accidental corruption, but it does **not** establish
provenance if the download host and checksum host are both compromised — the
attacker simply publishes a matching checksum for tampered bytes. For
authenticity-sensitive downloads, require a hash pinned from an independent
source, a signature, or a build attestation instead of trusting auto-discovery.

For extra assurance, a manifest file (`<output>.gofetch.manifest`) can be created
alongside the download containing per-chunk SHA-256 hashes (1 MiB chunks). If
present, gofetch verifies each chunk during download and the whole file on
completion. Create one by downloading (or re-downloading) with `-manifest-out`:

```bash
gofetch -manifest-out file.gofetch.manifest -o file https://example.com/file
```

On a manifest verification failure gofetch locates the corrupt chunk(s),
surgically trims only those byte ranges from the resume sidecar, and the next
run re-fetches just the bad spans instead of the whole file.

## Resume

On first run, a sidecar file `<output>.gofetch.resume` records completed byte
ranges (deduplicated and merged, so it stays compact across abort/resume
cycles). If the process is killed or crashes, re-run the same command: it
skips completed ranges and continues where it left off.

A sidecar is only trusted together with its partial output file: if the
file is missing or its size no longer matches the download, the claims are
stale (the bytes are gone) and gofetch re-fetches everything rather than
produce a hole-ridden file.

## Project Layout

```
gofetch/
  cmd/gofetch/      # CLI: flags, run loop, sidecar/hash resolution, validation
  cmd/benchserver/  # Synthetic Range-capable HTTP server for benchmarks
  internal/fetch/   # Engine: workers, work-stealing monitor, resume, integrity,
                    #   auto-tuning, SSRF hardening, mmap/pwrite writers
  scripts/          # bench, fuzz, smoke, real-world, install suites
```

## Benchmark suite

One consolidated script, four modes (shared server lifecycle lives in `scripts/bench_lib.sh`):

```bash
./scripts/bench.sh quick                # auto/quiet timing across sizes + hyperfine if present
./scripts/bench.sh full                 # sizes, hash verify, resume, mode + manifest tests
./scripts/bench.sh compare [SIZE_MB]    # gofetch vs aria2c (requires aria2c)
./scripts/bench.sh all                  # everything
# RUNS=5 SIZE_MB=64 ./scripts/bench.sh compare
```

The bundled `cmd/benchserver` serves deterministic payloads with Range support on
`127.0.0.1:9120`; the suite passes `--allow-loopback` so gofetch can talk to it
(never use that flag with URLs you do not trust).

## Fuzzing

```bash
./scripts/fuzz.sh                  # quick fuzz of every target (30s each)
./scripts/fuzz.sh FuzzParseUint    # one target until Ctrl-C / FUZZTIME elapses
FUZZTIME=2m ./scripts/fuzz.sh FuzzManifestJSON
```

Fuzz targets cover the parsers (`Content-Range`, `Retry-After`, hash/sidecar
flags, manifest JSON) and the range algebra. Seed corpora run as unit tests;
new interesting inputs are committed as regression seeds (CI runs a short
fuzz smoke on every push).

## Smoke test

```bash
./scripts/smoke.sh                         # full CLI surface vs local benchserver
./scripts/smoke.sh https://example.com/f.bin  # or any real URL
```

Black-box: every flag, exit codes (0/1/2/130), stdout/stderr separation,
sidecar auto-detection, mirror fallback, rate limiting, manifest output,
`--no-clobber`, and Ctrl-C resumability. Runs in CI on every push.

## Real-world tests

```bash
./scripts/realworld.sh        # functional tests against real public servers
BENCH=1 ./scripts/realworld.sh  # + gofetch vs aria2c benchmark on real internet
```

Network-dependent, never gates CI (flaky upstreams report a skip, never a
false failure). Covers byte-equality on `proof.ovh.net`, `--info` detection,
multi-hop redirects, HTTPS+MD5 datasets, multi-URL downloads, rate limiting,
and interrupt/resume. Throughput comparisons live in `./scripts/bench_real.sh`.

## Benchmark

### Loopback (synthetic)

On a Linux 16-core box, loopback, 64 MB:

| Tool | Median (3 runs) |
| ---- | --------------- |
| `gofetch -q` | ~105 ms |
| aria2c (`-x 16`) | ~480 ms |
| aria2c (default) | ~550 ms |

### Real internet (1.5 GB Arch Linux ISO, ~42 ms RTT)

Same file, identical window per tool, measured on a live mirror
(`./scripts/bench_real.sh`):

| Tool | Throughput |
| ---- | ---------- |
| `gofetch` (auto) | up to ~305 MB/s |
| aria2c (`-x 16 -s 16`) | ~286 MB/s |
| aria2c (default) | ~8 MB/s |
| `curl` (single stream) | ~7 MB/s |
| `wget2` (HTTP/2 chunked) | ~4 MB/s |

The honest summary: **gofetch beats aria2c's tuned `-x 16`** on a live
high-latency connection and crushes single-stream tools by ~30-70× (aria2c's
*default* is a single connection — `-x 16` must be passed explicitly).
gofetch needs **zero tuning flags** to hit peak speed, uses sparse files
(survives disk quotas where `fallocate` preallocation fails), and
auto-verifies with `-h auto`.

Speed comes from parallel ranges + auto-tuned workers/buffers/retries, not
the write path: the `mmap(2)` and native `pwrite` writers both saturate real
links (~13 MB/s throttled 1.5 GB ISO: identical wall time, ~7 MiB peak RSS)
and the page cache on loopback. `mmap` is the default; `--no-mmap` covers
filesystems where mapping misbehaves (NFS, FUSE, overcommit limits).

## CI/CD

A workflow at `.github/workflows/ci.yml` lints, tests, and builds on every push to `main`:

- `gofmt` + `gofumpt` checks, `go vet`, `staticcheck`, `golangci-lint`, `govulncheck`, `deadcode`, `shellcheck`
- `go test -race -shuffle=on -count=2` (shuffled order catches order-dependent tests)
- CLI smoke test, plus a short `go test -fuzz` smoke on every fuzz target
- Build stripped release binary
- GitHub Actions dependencies are tracked by Dependabot (`.github/dependabot.yml`)

## Tested

- Go 1.27 (CI pins 1.27.1), Linux/amd64
- Release targets (`linux/{amd64,arm64}`, `darwin/{amd64,arm64}`, `windows/amd64`) cross-compile clean; Linux/amd64 is the fully tested platform.
- Verified byte-equality against `proof.ovh.net/files/10Mb.dat` (10 MiB) and `100Mb.dat` (100 MiB) — MD5/SHA256 match.
- `go vet`, `gofmt`, `gofumpt`, `go build` clean; race detector and `staticcheck` clean.
- 210+ tests pass under `-race -shuffle=on` (plus 41 black-box smoke checks):
  differential range-algebra tests against brute-force oracles, and chaos tests
  that truncate/reset/mislabel server responses — byte-perfect output or clean
  failure, never silent corruption.

## License

MPL-2.0
