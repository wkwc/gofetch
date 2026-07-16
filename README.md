# gofetch

A small, single-binary concurrent downloader in Go with adaptive range work-stealing,
pre-allocated sparse files, concurrent `WriteAt` writes, resume capability,
and integrity verification.

```
$ gofetch https://proof.ovh.net/files/10Mb.dat
  ######################## 100.0%  10.0 MB / 10.0 MB  1.2 GB/s  ETA 0s

  download complete
  bytes:   10.0 MB
  time:    8ms
  speed:   1.25 GB/s
  workers: 16
```

## Why

Most "range downloaders" are dumb: split into N chunks, fetch each, merge at the end.
That wastes disk I/O on temp files, breaks when a server is slow, and offers nothing
but "parallel curl."

`gofetch` does three things differently:

1. **Sparse file + `WriteAt`.** The target file is `Truncate`d to its full size up front,
   and every worker writes its bytes directly to the final offsets. No temp files, no merge.
2. **Adaptive work stealing.** A monitor goroutine ticks every 500 ms. If a worker is
   "slow" (on a chunk > 512 KiB and has fetched < 1 MiB after a 1.5 s grace period),
   the monitor *cancels* that worker's HTTP request, splits its remaining range,
   and pushes the unfinished half back to the shared work queue for another worker to grab.
3. **No shared locks in the hot path.** Worker state (`bytesDone`, `curTask`, `cancel`)
   uses `atomic.Pointer` / `atomic.Int64`; the only mutexes are the work queue itself
   and the total-bytes progress counter (low contention: one increment per ~64 KiB).

## Features

| Feature | Flag | Description |
|---------|------|-------------|
| **Output** | `-o PATH` | Output file path (default: basename of URL) |
| **Quiet** | `-q` | Suppress progress bar; print only filename on success |
| **Verbose** | `-v` | Verbose logging to stderr (mirror selection, task starts, retries, chunk verification) |
| **Hash** | `-h SPEC` | Verify integrity: `sha256:hex`, `sha512:hex`, `auto` (fetch sidecar), or path to `.sha256`/`.sha512` file |
| **No Resume** | `--no-resume` | Disable resume from `.gofetch.resume` (default: enabled) |

## Usage

```bash
# Basic download
gofetch https://example.com/file.bin

# Custom output path
gofetch -o out.bin https://example.com/file.bin

# Hash verification (explicit)
gofetch -h sha256:abc123... https://example.com/file.bin

# SHA-512 verification
gofetch -h sha512:abc123... https://example.com/file.bin

# Auto-detect sidecar hash file (fetches URL.sha256, URL.sha512, etc.)
gofetch -h auto https://example.com/file.bin

# Local sidecar file
gofetch -h /path/to/file.sha256 https://example.com/file.bin

# Quiet mode (script-friendly: prints filename only)
gofetch -q https://example.com/file.bin

# Verbose mode (debug to stderr)
gofetch -v https://example.com/file.bin

# Disable resume (always download fresh)
gofetch --no-resume https://example.com/file.bin
```

If the server doesn't support `Range`, it gracefully falls back to a single GET stream.

## Resume

On first run, a sidecar file `<output>.gofetch.resume` is created/updated
with the set of completed byte ranges. If the process is killed or crashes,
re-run the same command: it reads the state, skips the completed ranges,
and continues from where it left off.

Completed ranges are deduplicated and merged before saving, so the state file
stays compact even across many abort/resume cycles.

## Integrity Verification

The `-h` flag supports multiple formats:

| Format | Example |
|--------|---------|
| `sha256:hex` | `gofetch -h sha256:abc123...` |
| `sha512:hex` | `gofetch -h sha512:abc123...` |
| `auto` (fetch sidecar) | `gofetch -h auto https://...` fetches `.sha256` then `.sha512` sidecars |
| bare hex (assumes sha256) | `gofetch -h abc123...` |
| local sidecar file | `gofetch -h /path/file.sha256 https://...` |

Sidecar files are parsed in common formats: `<hash> [filename]` or just `<hash>`.
Algorithm is inferred from hash length (64 = sha256, 128 = sha512) or file extension.

For extra assurance, a manifest file (`<output>.gofetch.manifest`) can be created
alongside the download containing per-chunk SHA-256 hashes. If present, gofetch
verifies each chunk during download and the whole file on completion.

## Error Handling

- **Transient network errors** (connection reset, unexpected EOF, timeout) are retried
  with exponential backoff (up to 10 retries per chunk).
- **HTTP 429/503/502/504/408** are retried respecting `Retry-After` header.
- **Permanent errors** (invalid URL, unsupported status codes) fail immediately.
- **HTTP 416** (Range Not Satisfiable) is treated as "already complete" and skipped.

## Project Layout

```
gofetch/
  go.mod
  cmd/gofetch/main.go              # CLI entrypoint
  internal/fetch/
    downloader.go                  # Core Downloader type and constructor
    worker.go                      # Worker goroutine, HTTP range requests, error handling
    monitor.go                     # Work-stealing monitor
    range.go                       # Parallel range-download orchestration
    single.go                      # Single-stream fallback (no range support)
    mirror.go                      # Server probing, Content-Range parsing
    seeds.go                       # Range splitting and gap computation
    task.go                        # Task struct and lock-free FIFO queue
    buffer.go                      # sync.Pool buffer recycling
    progress.go                    # Thread-safe progress tracking and display
    finalize.go                    # Hash verification, resume save, sparse allocation
    resume.go                      # Resume state persistence (JSON sidecar)
    hash.go                        # SHA-256/512 computation and verification
    manifest.go                    # Per-chunk integrity manifest
    format.go                      # Human-readable byte formatting
    verbose.go                     # Verbose logging
    transport.go                   # HTTP transport factory
    optimizer.go                   # Auto-configuration logic
    extra_tests.go                 # Additional unit tests
    testhelpers_test.go            # Test fixtures and helpers
    *_test.go                      # Unit and e2e tests
```

Single binary, zero external dependencies — stdlib only.

## Design Notes

- **Pre-allocation:** `os.File.Truncate(total)` on an empty file gives a sparse
  file on Linux/ext4 — instant size, no real block I/O until bytes are written.
- **Offset-safe writes:** `(*os.File).WriteAt` is goroutine-safe per POSIX on a
  single file handle. Multiple workers writing disjoint ranges is correct.
- **Cancellation for stealing:** Each worker's HTTP request runs on a child
  context (`context.WithCancel`). The monitor calls the cancel func to abort
  a slow request mid-flight; the worker sees `context.Canceled`, loops back,
  and picks up the next task from the queue (which now includes the stolen
  remainder).
- **Progress atomicity:** `bytesDone` is an `atomic.Int64` updated per buffer
  flush; the monitor reads it without locks. The total progress uses a CAS loop
  because it's a read+modify pair, but contention is ~1 lock per 64 KiB.
- **Chunk-level integrity:** When a manifest is present, each chunk is verified
  against its expected hash immediately after being written to disk.

## Tested

- Go 1.26, Linux/amd64
- Verified byte-equality against `proof.ovh.net/files/10Mb.dat` (10 MiB)
  and `100Mb.dat` (100 MiB) — MD5/SHA256 match.
- `go vet`, `gofmt`, `go build` clean.
- Race detector clean (`go test -race`).
- `staticcheck` clean.
- 30 tests pass under `-race -count=5`.

## Limitations

- No proxy support yet (reads `HTTP_PROXY`/`HTTPS_PROXY` from env but not fully tested).
- Resume state only stores completed *ranges*; partially written ranges are
  retried from scratch on resume (the file already has those bytes, so it's
  idempotent).

## License

Apache-2.0