// Command gofetch is an opinionated concurrent HTTP downloader.
//
// Usage:
//
//	gofetch [options] <url> [url2 ...]
//
// It just works. Workers, buffers, timeouts, retries, compression, proxy,
// and resume are all auto-configured. The flags are for things that
// genuinely require user input: output path, hash verification, custom
// headers, and bandwidth caps.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/wkwc/gofetch/internal/fetch"
)

// version is injected at link time: -ldflags="-X main.version=v1.2.3".
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:]))
}

// headerList accumulates repeatable -H/--header flags.
type headerList []string

func (h *headerList) String() string { return strings.Join(*h, ", ") }

func (h *headerList) Set(v string) error {
	if v == "" {
		return errors.New("empty header")
	}
	*h = append(*h, v)
	return nil
}

func run(args []string) int {
	fs := flag.NewFlagSet("gofetch", flag.ContinueOnError)
	var cfg cliConfig
	fs.StringVar(&cfg.outPath, "o", "", "output file path (default: basename of URL); an existing directory downloads into it")
	fs.BoolVar(&cfg.quiet, "q", false, "suppress progress output")
	fs.BoolVar(&cfg.verbose, "v", false, "verbose logging")
	fs.StringVar(&cfg.hashFlag, "h", "", "verify integrity: auto-detects local sidecar, or md5:hex / sha1:hex / sha256:hex / sha512:hex / path / auto")
	fs.BoolVar(&cfg.noResume, "no-resume", false, "disable resume (default: on)")
	fs.StringVar(&cfg.mirrorsFlag, "m", "", "comma-separated mirror URLs tried on failure (bare hostnames get https://)")
	fs.StringVar(&cfg.manifestOut, "manifest-out", "", "after download, write a per-chunk integrity manifest of the output to this path")
	fs.StringVar(&cfg.limitRate, "limit-rate", "", "cap aggregate download speed (per file): e.g. 500k, 2M, 1G")
	fs.StringVar(&cfg.userAgent, "A", "", "custom User-Agent header")
	fs.StringVar(&cfg.proxy, "proxy", "", "HTTP(S)/SOCKS5 proxy URL (overrides environment)")
	fs.BoolVar(&cfg.allowLocal, "allow-loopback", false, "permit loopback/private dials (local benchmarks/tests only; unsafe for untrusted URLs)")
	fs.BoolVar(&cfg.info, "info", false, "probe URLs and print size/range support without downloading")
	fs.IntVar(&cfg.workers, "x", 0, "override auto-tuned worker count (0 = auto)")
	fs.StringVar(&cfg.bufSize, "buf-size", "", "override auto-tuned read buffer per worker (e.g. 64k, 1M)")
	fs.IntVar(&cfg.maxRetries, "max-retries", 0, "override the per-chunk retry budget (0 = auto, default 10)")
	fs.BoolVar(&cfg.noClobber, "no-clobber", false, "skip downloads whose output file already exists")
	fs.BoolVar(&cfg.noMmap, "no-mmap", false, "use raw pwrite instead of mmap (filesystems where mmap misbehaves)")
	fs.StringVar(&cfg.caCert, "ca-cert", "", "PEM file of extra root CAs to trust (private/self-signed mirrors)")
	fs.BoolVar(&cfg.jsonOut, "json", false, "with --info, emit JSON (one object per URL)")
	fs.BoolVar(&cfg.showVersion, "version", false, "print version and exit")
	fs.Var(&cfg.headers, "H", "send a custom header 'Name: value' (repeatable)")
	fs.Var(&cfg.headers, "header", "send a custom header 'Name: value' (repeatable)")
	fs.StringVar(&cfg.userAgent, "user-agent", "", "custom User-Agent header")
	fs.IntVar(&cfg.workers, "workers", 0, "override auto-tuned worker count (0 = auto)")
	fs.Usage = func() { usage(fs) }

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		fs.Usage()
		return 2
	}

	if cfg.showVersion {
		fmt.Printf("gofetch %s (go %s)\n", version, runtime.Version())
		return 0
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return 2
	}
	if cfg.allowLocal {
		// Explicit opt-in for the repo's own benchmark scripts and local
		// testing against a benchserver on 127.0.0.1. Never pass this for
		// URLs you do not trust. SECURITY.md documents the tradeoff.
		fetch.AllowLoopbackDial.Store(true)
	}

	// Signal context is created early so Ctrl-C/SIGTERM/SIGHUP also
	// interrupt URL-validation DNS lookups, not just the download.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	var err error
	if cfg, err = resolveConfig(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}

	rawURLs := fs.Args()
	// Validate every URL up front (all-or-nothing), like the old CLI.
	for _, u := range rawURLs {
		if err := validateURL(ctx, u); err != nil {
			fmt.Fprintln(os.Stderr, "gofetch:", err)
			return 1
		}
	}

	// --info probes each URL and reports without downloading.
	if cfg.info {
		return runInfo(ctx, rawURLs, cfg.jsonOut)
	}

	outs, err := resolveOutputs(cfg.outPath, rawURLs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}

	// Multi-URL batches share one transport so keep-alive connections
	// carry across URLs to the same host (no per-URL TLS handshake).
	var sharedTr *http.Transport
	if len(rawURLs) > 1 {
		sharedTr = fetch.NewTransport(fetch.Options{
			Proxy: cfg.proxy, CACert: cfg.caCert, Workers: cfg.workers, BufSize: int(cfg.bufBytes),
		})
		defer sharedTr.CloseIdleConnections()
	}

	exit := 0
	multi := len(rawURLs) > 1
	for i, rawURL := range rawURLs {
		if code := runOne(ctx, cfg, rawURL, outs[i], sharedTr, multi); code != 0 {
			if code == interruptExit {
				return code
			}
			exit = 1
		}
	}
	return exit
}

// resolveOutputs maps each URL to its output path. With a single URL,
// -o (or the URL basename) is the file; an existing directory -o means
// "download into it". With multiple URLs, -o is a directory (created if
// missing) and each file is its URL basename.
func resolveOutputs(outPath string, rawURLs []string) ([]string, error) {
	outs := make([]string, len(rawURLs))
	if len(rawURLs) == 1 {
		outs[0] = resolveOut(outPath, rawURLs[0])
		return outs, nil
	}

	dir := outPath
	if dir == "" {
		dir = "."
	}
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			return nil, fmt.Errorf("-o %q is not a directory (required for multiple URLs)", dir)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir %q: %w", dir, err)
	}

	seen := make(map[string]int, len(rawURLs))
	for i, u := range rawURLs {
		base := urlBaseName(u)
		outs[i] = filepath.Join(dir, base)
		seen[outs[i]]++
	}
	for p, n := range seen {
		if n > 1 {
			return nil, fmt.Errorf("multiple URLs map to %q; disambiguate with a different URL path or -o", p)
		}
	}
	return outs, nil
}

// resolveOut derives a single URL's output path. An empty -o uses the
// URL basename; an existing directory -o means "download into it".
func resolveOut(outPath, rawURL string) string {
	base := urlBaseName(rawURL)
	if outPath == "" {
		return base
	}
	if info, err := os.Stat(outPath); err == nil && info.IsDir() {
		return filepath.Join(outPath, base)
	}
	return outPath
}

// urlBaseName returns the URL path basename, defaulting for empty/root.
func urlBaseName(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "downloaded.bin"
	}
	base := filepath.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		return "downloaded.bin"
	}
	return base
}

func usage(fs *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, "usage: gofetch [options] <url> [url2 ...]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "opinionated concurrent downloader — everything auto-tuned internally")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "options:")
	fs.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "note: -h is the integrity-hash flag; use -help for this help")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "examples:")
	for _, ex := range usageExamples {
		fmt.Fprintln(os.Stderr, ex)
	}
}

// usageExamples is the -help example list as data, so adding an example
// can't desync the help text (each entry is one full output line).
var usageExamples = []string{
	"  gofetch https://example.com/file.bin",
	"  gofetch -o out.bin https://example.com/file.bin",
	"  gofetch -o ~/Downloads https://example.com/file.bin     # existing dir",
	"  gofetch -o ~/Downloads url1 url2 url3                   # multiple files",
	"  gofetch --info https://example.com/file.bin             # probe, no download",
	"  gofetch -H 'Authorization: Bearer token' -o out.bin https://example.com/file.bin",
	"  gofetch --limit-rate 2M -o out.bin https://example.com/file.bin",
	"  gofetch -x 16 --buf-size 256k -o out.bin https://example.com/file.bin",
	"  gofetch --no-clobber -o out.bin https://example.com/file.bin  # skip if exists",
	"  gofetch --ca-cert ca.pem -o out.bin https://mirror.example.com/f.bin  # private CA",
	"  gofetch -h auto https://example.com/file.bin",
	"  gofetch -m mirror1,mirror2 https://primary.com/file.bin",
	"  gofetch --allow-loopback -o out.bin http://127.0.0.1:9120/  # local benchserver",
}
