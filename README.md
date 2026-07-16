# gofetch

A small, single-binary streaming downloader in Go with adaptive range work-stealing,
pre-allocated sparse files, concurrent `WriteAt` writes, multi-mirror failover,
resume capability, and integrity verification.

```
$ gofetch -w 4 https://proof.ovh.net/files/10Mb.dat
  #####...................  20.0%  2.0 MB / 10.0 MB
  ####################....  81.8%  8.2 MB / 10.0 MB
  ########################  99.8%  10.0 MB / 10.0 MB
```

## Why

Most "range downloaders" are dumb: split into N chunks, fetch each, merge at the end.
That wastes disk I/O on temp files, breaks when one mirror is slow, and offers nothing
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
| **Workers** | `-w N` | Concurrent range workers (default 4) |
| **Buffer** | `-buf N` | Per-worker read buffer (default 64 KiB) |
| **Timeout** | `-timeout D` | Per-request HTTP timeout (default 30s) |
| **Output** | `-o PATH` | Output file path |
| **Quiet** | `-q` | Suppress progress bar |
| **Mirrors** | `-mirrors "u1,u2"` | Comma-separated fallback URLs; fastest healthy one wins |
| **Hash** | `-hash <hex>` | Expected SHA256 (hex); fails on mismatch |
| **Resume** | `-resume` (default) | Save `.gofetch.resume` state; resume on restart |
| **User-Agent** | `-useragent` | Custom User-Agent header (default `gofetch/0.1`) |

## Usage

```bash
# Basic download
gofetch https://example.com/large.bin

# 8 workers, custom output, with SHA256 verification
gofetch -w 8 -o model.safetensors -hash 4670af0752b0ee0a571c17eb6923b722e9c557cd26e6b9ec25c2155098f3dc62 \
  https://huggingface.co/.../model.safetensors

# Mirror list + resume
gofetch -mirrors "https://mirror1/file,https://mirror2/file" \
  -resume -o dataset.tar.zst \
  https://primary/file
```

If the server doesn't support `Range`, it gracefully falls back to a single GET stream.

## Resume

On first run with `-resume` (default), a sidecar file `<output>.gofetch.resume`
is created/updated every 5 seconds with the set of completed byte ranges.
If the process is killed or crashes, re-run the same command: it reads the state,
skips the completed ranges, and continues from where it left off.

Completed ranges are deduplicated and merged before saving, so the state file
stays compact even across many abort/resume cycles.

## Mirror selection

All mirrors (including the primary URL) are probed in parallel (HEAD → range GET fallback).
The first healthy mirror with the lowest 1-byte latency is chosen.

If a range request returns 200 OK instead of 206 Partial Content (indicating the server
does not actually support range requests despite advertising it), the download fails
with a clear error.

## Integrity

- `-hash <hex>`: verifies SHA256 after download.

## Error handling

- **Transient network errors** (connection reset, unexpected EOF, timeout) are retried
  with exponential backoff (up to 5 retries per range chunk).
- **Permanent errors** (invalid URL, unsupported status codes) kill the download immediately.

## Project layout

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
    mirror.go                      # Mirror probing, selection, Content-Range parsing
    seeds.go                       # Range splitting and gap computation
    task.go                        # Task struct and lock-free FIFO queue
    buffer.go                      # sync.Pool buffer recycling
    progress.go                    # Thread-safe progress tracking and display
    finalize.go                    # Hash verification, resume save, sparse allocation
    resume.go                      # Resume state persistence (JSON sidecar)
    hash.go                        # SHA-256 computation and verification
    format.go                      # Human-readable byte formatting
```

Single binary, zero external dependencies — stdlib only.

## Design notes

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
  flush; the monitor reads it without locks. The total progress uses a mutex
  because it's a read+modify pair, but contention is ~1 lock per 64 KiB.

## Tested

- Go 1.26, Linux/amd64
- Verified byte-equality against `proof.ovh.net/files/10Mb.dat` (10 MiB)
  and `100Mb.dat` (100 MiB) — MD5/SHA256 match.
- `go vet`, `gofmt`, `go build` clean.
- Race detector clean (`go test -race`).

## Limitations

- No proxy support yet.
- Resume state only stores completed *ranges*; partially written ranges are
  retried from scratch on resume (the file already has those bytes, so it's
  idempotent).

## License

Apache-2.0
