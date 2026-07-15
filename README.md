# gofetch

A small, single-binary streaming downloader in Go with adaptive range work-stealing,
pre-allocated sparse files, concurrent `WriteAt` writes, multi-mirror failover,
resume capability, and integrity verification.

```
$ gofetch -w 4 https://proof.ovh.net/files/10Mb.dat
  #####...................  20.0%  2.0 MB/10485760
  ####################....  81.8%  8.2 MB/10485760
  ########################  99.8%  10.0 MB/10485760
  done: 10Mb.dat
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
| **Sidecar** | `-hash auto` | Fetch hash from `<url>.sha256` |
| **Resume** | `-resume` (default) | Save `.gofetch.resume` state; resume on restart |
| **QUIC** | `-quic` (default) | Prefer HTTP/3 (QUIC) when available |

## Usage

```bash
# Basic download
gofetch https://example.com/large.bin

# 8 workers, custom output, with SHA256 verification
gofetch -w 8 -o model.safetensors -hash 4670af0752b0ee0a571c17eb6923b722e9c557cd26e6b9ec25c2155098f3dc62 \
  https://huggingface.co/.../model.safetensors

# Mirror list + auto sidecar hash + resume
gofetch -mirrors "https://mirror1/file,https://mirror2/file" \
  -hash auto -resume -o dataset.tar.zst \
  https://primary/file
```

If the server doesn't support `Range`, it gracefully falls back to a single GET stream.

## Resume

On first run with `-resume` (default), a sidecar file `<output>.gofetch.resume`
is created/updated every 5 seconds with the set of completed byte ranges.
If the process is killed or crashes, re-run the same command: it reads the state,
skips the completed ranges, and continues from where it left off.

## Mirror selection

All mirrors are probed in parallel (HEAD → range GET fallback). The first
healthy mirror with the lowest 1-byte latency is chosen. If that mirror fails
mid-download, the next healthy mirror is tried automatically.

## Integrity

- `-hash <hex>`: verifies SHA256 after download.
- `-hash auto`: appends `.sha256` to the primary URL, fetches it, and verifies.
  The sidecar file may contain just the hex hash or `hash  filename` (like `sha256sum` output).

## QUIC / HTTP3

`-quic` (enabled by default) prefers HTTP/3 via `quic-go` when the server
advertises `alt-svc: h3`. Falls back transparently to HTTP/2 over TLS.
No separate TCP fallback flag — if QUIC fails, the mirror failover logic
handles it.

## Project layout

```
gofetch/
  go.mod
  cmd/gofetch/main.go                 # CLI entrypoint
  internal/fetch/downloader.go        # Core downloader (workers, monitor, mirrors, resume, hash)
```

Single binary, stdlib + `github.com/quic-go/quic-go` (for HTTP/3), zero other deps.

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

## Limitations

- No retries per se — a failed range goes back to the queue and another
  worker retries it (but there's no max-retry cap).
- No proxy support yet.
- Resume state only stores completed *ranges*; partially written ranges are
  retried from scratch on resume (the file already has those bytes, so it's
  idempotent).
- Sidecar hash fetch assumes the `.sha256` file is at the same origin.
- QUIC support depends on `quic-go` (not stdlib yet).

## License

Apache-2.0