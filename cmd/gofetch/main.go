// Command gofetch is an opinionated concurrent HTTP downloader.
//
// Usage:
//
//	gofetch [options] <url>
//
// It just works. Workers, buffers, timeouts, retries, compression, proxy,
// and resume are all auto-configured. The only flags are for things that
// genuinely require user input: output path, hash verification, and
// verbosity.
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
	"syscall"

	"github.com/wkwc/gofetch/internal/fetch"
)

// version is injected at link time: -ldflags="-X main.version=v1.2.3"
var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	var (
		outPath     = flag.String("o", "", "output file path (default: basename of URL)")
		quiet       = flag.Bool("q", false, "suppress progress output")
		verbose     = flag.Bool("v", false, "verbose logging")
		hashFlag    = flag.String("h", "", "verify integrity: auto-detects local sidecar, or sha256:hex / sha512:hex / path / auto")
		noResume    = flag.Bool("no-resume", false, "disable resume (default: on)")
		mirrorsFlag = flag.String("m", "", "comma-separated mirror URLs tried on failure (bare hostnames get https://)")
		manifestOut = flag.String("manifest-out", "", "after download, write a per-chunk integrity manifest of the output to this path")
		allowLocal  = flag.Bool("allow-loopback", false, "permit loopback/private dials (local benchmarks/tests only; unsafe for untrusted URLs)")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: gofetch [options] <url>")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "opinionated concurrent downloader — everything auto-tuned internally")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "options:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "examples:")
		fmt.Fprintln(os.Stderr, "  gofetch https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -o out.bin https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -h auto https://example.com/file.bin        # local sidecar, else fetch .sha256/.sha512")
		fmt.Fprintln(os.Stderr, "  gofetch -h sha256:abc123... https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -q https://example.com/file.bin             # quiet (prints filename only)")
		fmt.Fprintln(os.Stderr, "  gofetch -v https://example.com/file.bin              # verbose (debug to stderr)")
		fmt.Fprintln(os.Stderr, "  gofetch -m mirror1,mirror2,mirror3 https://primary.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -manifest-out out.gofetch.manifest https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch --allow-loopback -o out.bin http://127.0.0.1:9120/   # local benchserver")
	}
	flag.Parse()

	if *allowLocal {
		// Explicit opt-in for the repo's own benchmark scripts and local
		// testing against a benchserver on 127.0.0.1. Never pass this for
		// URLs you do not trust. SECURITY.md documents the tradeoff.
		fetch.AllowLoopbackDial = true
	}

	if *showVersion {
		fmt.Println("gofetch", version)
		return 0
	}

	if flag.NArg() != 1 {
		flag.Usage()
		return 2
	}
	rawURL := flag.Arg(0)

	if err := validateURL(rawURL); err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	u, _ := url.Parse(rawURL)

	out := *outPath
	if out == "" {
		base := filepath.Base(u.Path)
		if base == "" || base == "/" {
			base = "downloaded.bin"
		}
		out = base
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mirrors, err := normalizeMirrors(*mirrorsFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}

	algo, hashHex, err := resolveHash(ctx, *hashFlag, rawURL, out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}

	d := fetch.NewDownloader(rawURL, out, fetch.Options{
		HashAlgo:     algo,
		ExpectedHash: hashHex,
		NoResume:     *noResume,
		Verbose:      *verbose,
		Quiet:        *quiet,
		Mirrors:      mirrors,
	})

	if err := d.Download(ctx); err != nil {
		// User-initiated cancel (Ctrl-C / SIGTERM) is not a failure of
		// the downloader — the partial progress was already flushed to
		// the resume sidecar, so say so plainly instead of wrapping it
		// as a mirror error.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintln(os.Stderr, "gofetch: interrupted; partial progress saved, re-run to resume")
			return 130
		}
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	if *manifestOut != "" {
		if err := writeManifest(*manifestOut, out); err != nil {
			fmt.Fprintln(os.Stderr, "gofetch:", err)
			return 1
		}
	}
	// Always print the output path on success (quiet: filename only;
	// verbose/normal: summary already went to stderr in finalize).
	fmt.Println(out)
	return 0
}
