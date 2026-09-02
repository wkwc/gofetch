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
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/wkwc/gofetch/internal/fetch"
)

// version is injected at link time: -ldflags="-X main.version=v1.2.3"
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
	var (
		outPath     = fs.String("o", "", "output file path (default: basename of URL); an existing directory downloads into it")
		quiet       = fs.Bool("q", false, "suppress progress output")
		verbose     = fs.Bool("v", false, "verbose logging")
		hashFlag    = fs.String("h", "", "verify integrity: auto-detects local sidecar, or sha256:hex / sha512:hex / path / auto")
		noResume    = fs.Bool("no-resume", false, "disable resume (default: on)")
		mirrorsFlag = fs.String("m", "", "comma-separated mirror URLs tried on failure (bare hostnames get https://)")
		manifestOut = fs.String("manifest-out", "", "after download, write a per-chunk integrity manifest of the output to this path")
		limitRate   = fs.String("limit-rate", "", "cap aggregate download speed (per file): e.g. 500k, 2M, 1G")
		userAgent   = fs.String("A", "", "custom User-Agent header")
		proxy       = fs.String("proxy", "", "HTTP(S)/SOCKS5 proxy URL (overrides environment)")
		allowLocal  = fs.Bool("allow-loopback", false, "permit loopback/private dials (local benchmarks/tests only; unsafe for untrusted URLs)")
		showVersion = fs.Bool("version", false, "print version and exit")
		headers     headerList
	)
	fs.Var(&headers, "H", "send a custom header 'Name: value' (repeatable)")
	fs.Var(&headers, "header", "send a custom header 'Name: value' (repeatable)")
	fs.StringVar(userAgent, "user-agent", "", "custom User-Agent header")
	fs.Usage = func() { usage(fs) }

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		fs.Usage()
		return 2
	}

	if *showVersion {
		fmt.Printf("gofetch %s (go %s)\n", version, runtime.Version())
		return 0
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return 2
	}
	if *allowLocal {
		// Explicit opt-in for the repo's own benchmark scripts and local
		// testing against a benchserver on 127.0.0.1. Never pass this for
		// URLs you do not trust. SECURITY.md documents the tradeoff.
		fetch.AllowLoopbackDial = true
	}

	// Signal context is created early so Ctrl-C/SIGTERM/SIGHUP also
	// interrupt URL-validation DNS lookups, not just the download.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	mirrors, err := normalizeMirrors(ctx, *mirrorsFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	rate, err := parseRateLimit(*limitRate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	if err := validateHeaders(headers); err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	if *proxy != "" {
		if p, err := url.Parse(*proxy); err != nil || (p.Scheme != "http" && p.Scheme != "https" && p.Scheme != "socks5") {
			fmt.Fprintln(os.Stderr, "gofetch: invalid --proxy URL (use http://, https:// or socks5://)")
			return 1
		}
	}

	rawURLs := fs.Args()
	// Validate every URL up front (all-or-nothing), like the old CLI.
	for _, u := range rawURLs {
		if err := validateURL(ctx, u); err != nil {
			fmt.Fprintln(os.Stderr, "gofetch:", err)
			return 1
		}
	}

	outs, err := resolveOutputs(*outPath, rawURLs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}

	exit := 0
	for i, rawURL := range rawURLs {
		out := outs[i]
		algo, hashHex, err := resolveHash(ctx, *hashFlag, rawURL, out)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gofetch: %s: %v\n", rawURL, err)
			exit = 1
			continue
		}
		d := fetch.NewDownloader(rawURL, out, fetch.Options{
			HashAlgo:     algo,
			ExpectedHash: hashHex,
			NoResume:     *noResume,
			Verbose:      *verbose,
			Quiet:        *quiet,
			Mirrors:      mirrors,
			Headers:      headers,
			RateLimit:    rate,
			Proxy:        *proxy,
			UserAgent:    *userAgent,
		})

		if err := d.Download(ctx); err != nil {
			// User-initiated cancel (Ctrl-C / SIGTERM / SIGHUP) is not a
			// failure of the downloader — the partial progress was already
			// flushed to the resume sidecar, so say so plainly instead of
			// wrapping it as a mirror error.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				fmt.Fprintln(os.Stderr, "gofetch: interrupted; partial progress saved, re-run to resume")
				return 130
			}
			fmt.Fprintf(os.Stderr, "gofetch: %s: %v\n", rawURL, err)
			exit = 1
			continue
		}
		if *manifestOut != "" {
			mout := *manifestOut
			if len(rawURLs) > 1 {
				if err := os.MkdirAll(mout, 0o755); err != nil {
					fmt.Fprintf(os.Stderr, "gofetch: %s: manifest dir: %v\n", rawURL, err)
					exit = 1
					continue
				}
				mout = filepath.Join(mout, filepath.Base(out)+".gofetch.manifest")
			}
			if err := writeManifest(mout, out); err != nil {
				fmt.Fprintf(os.Stderr, "gofetch: %s: %v\n", rawURL, err)
				exit = 1
				continue
			}
		}
		// Always print the output path on success (quiet: filename only;
		// verbose/normal: summary already went to stderr in finalize).
		fmt.Println(out)
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
		out, err := resolveOut(outPath, rawURLs[0])
		if err != nil {
			return nil, err
		}
		outs[0] = out
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
func resolveOut(outPath, rawURL string) (string, error) {
	base := urlBaseName(rawURL)
	if outPath == "" {
		return base, nil
	}
	if info, err := os.Stat(outPath); err == nil && info.IsDir() {
		return filepath.Join(outPath, base), nil
	}
	return outPath, nil
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
	fmt.Fprintln(os.Stderr, "  gofetch https://example.com/file.bin")
	fmt.Fprintln(os.Stderr, "  gofetch -o out.bin https://example.com/file.bin")
	fmt.Fprintln(os.Stderr, "  gofetch -o ~/Downloads https://example.com/file.bin     # existing dir")
	fmt.Fprintln(os.Stderr, "  gofetch -o ~/Downloads url1 url2 url3                   # multiple files")
	fmt.Fprintln(os.Stderr, "  gofetch -H 'Authorization: Bearer token' -o out.bin https://example.com/file.bin")
	fmt.Fprintln(os.Stderr, "  gofetch --limit-rate 2M -o out.bin https://example.com/file.bin")
	fmt.Fprintln(os.Stderr, "  gofetch -h auto https://example.com/file.bin")
	fmt.Fprintln(os.Stderr, "  gofetch -m mirror1,mirror2 https://primary.com/file.bin")
	fmt.Fprintln(os.Stderr, "  gofetch --allow-loopback -o out.bin http://127.0.0.1:9120/  # local benchserver")
}
