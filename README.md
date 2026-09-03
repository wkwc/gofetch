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

# Quiet mode (script-friendly: prints filename only)
gofetch -q https://example.com/file.bin

# Verbose mode (debug to stderr)
gofetch -v https://example.com/file.bin

# Disable resume (always download fresh)
gofetch --no-resume https://example.com/file.bin

# Mirror fallback (tried in order on failure; bare hostnames get https://)
gofetch -m mirror1.com,mirror2.com https://primary.com/file.bin

# Multiple URLs at once (each to its basename; -o must be a directory)
gofetch -o ~/Downloads https://a.example/x.bin https://b.example/y.bin

# Existing directory as -o downloads into it by basename
gofetch -o ~/Downloads https://example.com/file.bin

# Authenticated / header-bearing request
gofetch -H 'Authorization: Bearer token' -H 'Cookie: session=abc' -o out.bin https://example.com/private.bin

# Custom User-Agent (some mirrors reject the default)
gofetch -A 'Mozilla/5.0 (compatible)' -o out.bin https://example.com/file.bin

# Cap aggregate bandwidth (per file)
gofetch --limit-rate 2M -o out.bin https://example.com/large.bin

# Explicit proxy (overrides HTTP_PROXY/HTTPS_PROXY/ALL_PROXY)
gofetch --proxy http://proxy.example.com:8080 -o out.bin https://example.com/file.bin

# Probe without downloading (script-friendly: prints size/ranges/workers)
gofetch --info https://example.com/file.bin

# Machine-readable probe (one JSON object per URL)
gofetch --info --json https://example.com/file.bin

# Escape hatches for the auto-tuned engine
gofetch -x 16 --buf-size 256k -o out.bin https://example.com/large.bin

# Skip if the output already exists
gofetch --no-clobber -o out.bin https://example.com/file.bin

# Trust a private/self-signed dataset mirror's CA
gofetch --ca-cert /etc/ssl/certs/my-mirror-ca.pem -o out.bin https://mirror.example.edu/file.bin

# Generate a per-chunk integrity manifest after download (chunk-level verification for future runs)
gofetch -manifest-out out.gofetch.manifest -o out.bin https://example.com/file.bin

# Local benchmark against the bundled benchserver (only for trusted local URLs!)
gofetch --allow-loopback -o out.bin http://127.0.0.1:9120/
```

If the server doesn't support `Range`, it gracefully falls back to a single GET stream.

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
   "slow" (on a chunk > 512 KiB and has fetched < 1 MiB after a 1.5 s grace period),
   the monitor *cancels* that worker's HTTP request, splits its remaining range,
   and pushes the unfinished half back to the shared work queue for another worker to grab.
3. **Lock-free progress.** There is no shared `done` counter — the progress
   display sums worker-local `bytesDone` atomics on demand.
4. **Zero user knobs for the engine.** Workers, buffer size, transport tuning, retries, parallelism,
   HTTP version, and chunk size are all derived from the server's `Content-Length`,
   `runtime.NumCPU()` and round-trip-time class. The flags are only for things
   that genuinely require user input (`-o`, `-q`, `-v`, `-h`, `--no-resume`, plus
   opt-in `-H`/`-A`/`--limit-rate`/`--proxy`).
5. **Small file fallback.** Files smaller than 64 KiB skip the worker/monitor
   stack entirely and use a single GET stream (parallel overhead dominates).

## Design

- **Pre-allocation:** `os.File.Truncate(total)` on an empty file gives a sparse file on Linux/ext4 — instant size, no real block I/O until bytes are written.
- **Offset-safe writes:** `(*os.File).WriteAt` is goroutine-safe per POSIX on a single file handle. Multiple workers writing disjoint ranges is correct.
- **Cancellation for stealing:** Each worker's HTTP request runs on a child context (`context.WithCancel`). The monitor calls the cancel func to abort a slow request mid-flight; the worker sees `context.Canceled`, loops back, and picks up the next task from the queue (which now includes the stolen remainder).
- **Per-worker progress:** Each worker tracks its own bytesDone in an `atomic.Int64` (used by the monitor for steal decisions). The shared `progress.done` is gone — `progress.snapshot()` sums worker counters on demand (~4×/sec for the progress bar, once at finalize). This eliminated per-buffer CAS contention.
- **Adaptive auto-config:** `AutoConfigure` then `Retune()` after the probe selects `Workers` and `BufSize` based on `runtime.NumCPU()` and the announced `Content-Length`. Workers are capped at 32; tiny files (< 64 KiB) fall back to single-stream by default.
- **Chunk-level integrity:** When a manifest is present, each chunk is verified against its expected hash immediately after being written to disk.

## Error Handling

- **Transient network errors** (connection reset, unexpected EOF, timeout) are retried with exponential backoff (up to 10 retries per chunk).
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

`-h auto` (and the zero-config default) also understands **container
checksum files** — `sha256sums.txt` (Arch) / `SHA256SUMS` (Ubuntu, Debian) —
both next to the local output and on the remote mirror, matching the entry
for the file being downloaded. So `gofetch -h auto <arch-iso-url>` verifies a
Linux ISO with zero configuration:

Integrity verification is zero-config when a checksum sidecar already sits
beside the output (the common case for mirrors that ship `.sha256`/`.sha512`
files) — `gofetch` finds and uses it automatically. Explicit `-h` values
override auto-detection.

Sidecar files are parsed in common formats: `<hash> [filename]` or just `<hash>`.
Algorithm is inferred from hash length (32 = md5, 40 = sha1, 64 = sha256,
128 = sha512) or file extension.

For extra assurance, a manifest file (`<output>.gofetch.manifest`) can be created
alongside the download containing per-chunk SHA-256 hashes. If present, gofetch
verifies each chunk during download and the whole file on completion. Create one
by downloading (or re-downloading) with `-manifest-out`:

```bash
gofetch -manifest-out file.gofetch.manifest -o file https://example.com/file
```

Chunks are 1 MiB by default. On a manifest verification failure gofetch locates
the corrupt chunk(s), surgically trims only those byte ranges from the resume
sidecar, and the next run re-fetches just the bad spans instead of the whole file.

## Resume

On first run, a sidecar file `<output>.gofetch.resume` is created/updated
with the set of completed byte ranges. If the process is killed or crashes,
re-run the same command: it reads the state, skips the completed ranges,
and continues from where it left off.

Completed ranges are deduplicated and merged before saving, so the state file
stays compact even across many abort/resume cycles.

## Project Layout

```
gofetch/
  go.mod
  cmd/gofetch/
    main.go                  # CLI entrypoint, flag parsing, run loop
    sidecar.go               # -h resolution: local/remote sidecar auto-detect
    validate.go              # URL + mirror validation (SSRF guards)
    manifest.go              # -manifest-out writer
    main_test.go
  cmd/benchserver/           # Synthetic HTTP server for benchmarking
  internal/fetch/
    downloader.go            # Core Downloader type and constructor
    worker.go                # Worker goroutine, HTTP range requests, retry loop
    body.go                  # Body pumping, per-range reads, pooled buffers
    monitor.go               # Work-stealing monitor
    range.go                 # Parallel range-download orchestration
    single.go                # Single-stream fallback (no range support)
    mirror.go                # Server probing, Content-Range parsing, request builder
    seeds.go                 # Range splitting and gap computation
    task.go                  # Task struct and lock-free FIFO queue
    progress.go              # Progress tracking, byte formatting, verbose log
    finalize.go              # Sync/close, integrity verify, download summary
    resume.go                # Resume state: persistence, accumulator, sidecar cleanup
    hash.go                  # SHA-256/512/MD5/SHA-1 computation and verification
    sidecar.go               # Sidecar hash parsing (shared with CLI)
    manifest.go              # Per-chunk integrity manifest (O(1) lookup, generator)
    optimizer.go             # Auto-config + transport factory
    ratelimit.go             # Aggregate bandwidth limiter (--limit-rate)
    ssrf.go                  # SSRF hardening, safe dial/redirect policy
    idle_body.go             # Idle-timeout body wrapper
    mmap_*.go                # mmap-backed writer + platform stubs (pwrite is the default with --no-mmap)
    sockopts_*.go            # Linux TCP tuning + platform stubs
    fuzz_test.go             # Fuzz targets for parsers and range algebra
    *_test.go                # Unit, property, differential, chaos, and e2e tests
  scripts/
    bench.sh                 # Consolidated benchmark suite (quick|full|compare|all)
    bench_lib.sh             # Shared bench helpers (build, server lifecycle)
    bench_real.sh            # Real-internet throughput vs aria2c/wget2/curl
    fuzz.sh                  # Fuzz all targets
    smoke.sh                 # Black-box CLI smoke test (CI)
    realworld.sh             # Real-network functional battery
    install.sh               # Release installer (checksum-verified)
```

## Benchmark

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
flags, manifest JSON) and the range algebra (`splitRange`, `uncompleted`,
`dedupTasks`). Their seed corpora run as normal unit tests; interesting inputs
discovered during fuzzing are saved under `internal/fetch/testdata/fuzz/` and
committed as regression seeds (CI also runs a short fuzz smoke).

## Smoke test

```bash
./scripts/smoke.sh                         # full CLI surface vs local benchserver
./scripts/smoke.sh https://example.com/f.bin  # or any real URL
```

A black-box check that exercises every flag and asserts exit codes (0/1/2/130),
stdout/stderr separation, sidecar auto-detection, mirror fallback, rate
limiting, manifest output, `--no-clobber`, and that Ctrl-C leaves a resumable
partial file. Runs in CI on every push.

## Real-world tests

```bash
./scripts/realworld.sh        # functional tests against real public servers
BENCH=1 ./scripts/realworld.sh  # + gofetch vs aria2c benchmark on real internet
```

Real-internet throughput comparisons live in `./scripts/bench_real.sh`.

Network-dependent (run manually or on-demand), so it never gates CI. Proves
against real servers: byte-equality on `proof.ovh.net` (the README's canonical
download), `--info` range/size detection, multi-hop 302 redirects via
`httpbin.org`, arXiv PDFs over HTTPS with MD5, bulk multi-URL downloads, real
rate limiting, and (network permitting) interrupt/resume of a 100 MB file.
Flaky upstreams degrade to a reported skip, never a false failure.

## Benchmark

### Loopback (synthetic)

On a Linux 16-core box, loopback, 64 MB:

| Tool | Median (3 runs) |
| ---- | --------------- |
| `gofetch -q` | ~105 ms |
| aria2c (`-x 16`) | ~480 ms |
| aria2c (default) | ~550 ms |

### Real internet (1.5 GB Arch Linux ISO, ~42 ms RTT)

Same file, identical 12 s window per tool, measured on a live mirror
(`./scripts/bench_real.sh`):

| Tool | Throughput |
| ---- | ---------- |
| `gofetch` (auto) | ~109 MB/s |
| aria2c (`-x 16 -s 16`) | ~119 MB/s |
| aria2c (default) | ~9 MB/s |
| `curl` (single stream) | ~8 MB/s |
| `wget2` (HTTP/2 chunked) | ~4 MB/s |

The honest summary: **gofetch matches aria2c's tuned `-x 16` — both saturate
the server's aggregate link — and crushes single-stream tools by ~13×** on a
high-latency real connection. aria2c's *default* is a single connection
(`-x 16` must be passed explicitly), which is why the stock comparison favors
gofetch so strongly. gofetch also wins on robustness: it needs **zero tuning
flags** to hit peak speed, uses sparse files (works where aria2c's
`fallocate` preallocation fails under disk quotas), and auto-verifies with
`-h auto`.

gofetch is fast because it fetches many ranges in parallel and auto-tunes
the worker count, buffer, and retry budget from the server's
`Content-Length` — not because of its write path. Measured on real
workloads, the two write backends are equivalent: the `mmap(2)` writer and
the native `pwrite` writer both saturate real links (~13 MB/s throttled
1.5 GB ISO: identical wall time and ~7 MiB peak RSS) and both saturate the
page cache on loopback. `mmap` remains the default; pass `--no-mmap` for
filesystems where memory-mapping misbehaves (NFS, FUSE, overcommit limits).

## CI/CD

A workflow at `.github/workflows/ci.yml` lints, tests, and builds on every push to `main`:

- `gofmt` check, `go vet`, `staticcheck`, `golangci-lint`, `govulncheck`, `deadcode`
- `go test -race -shuffle=on` (shuffled order catches order-dependent tests)
- 15s `go test -fuzz` smoke on a parser target (corpus committed under `internal/fetch/testdata/fuzz/`)
- Build stripped release binary
- GitHub Actions dependencies are tracked by Dependabot (`.github/dependabot.yml`)

## Tested

- Go 1.26, Linux/amd64
- Verified byte-equality against `proof.ovh.net/files/10Mb.dat` (10 MiB) and `100Mb.dat` (100 MiB) — MD5/SHA256 match.
- `go vet`, `gofmt`, `go build` clean.
- Race detector clean (`go test -race`).
- `staticcheck` clean.
- 160+ tests pass under `-race -shuffle=on`, including:
  - **differential tests** checking the range algebra (`splitRange`, `uncompleted`, `dedupTasks`) against brute-force oracles on thousands of randomized inputs;
  - **chaos tests** that randomly truncate/reset/mislabel server responses and assert the downloader either converges to byte-perfect output or fails cleanly — never silent corruption.

## License

MPL-2.0
