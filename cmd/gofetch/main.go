// Command gofetch downloads a single URL (or mirror list) to disk using
// concurrent HTTP range requests, adaptive work-stealing, multi-mirror
// failover, resume capability, and integrity verification.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/local/gofetch/internal/fetch"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		workers  = flag.Int("w", 4, "number of concurrent workers")
		bufSize  = flag.Int("buf", 64*1024, "per-worker buffer size in bytes")
		timeout  = flag.Duration("timeout", 30*time.Second, "per-request HTTP timeout")
		outPath  = flag.String("o", "", "output file path (default: basename of URL)")
		quiet    = flag.Bool("q", false, "suppress progress output")
		mirrors  = flag.String("mirrors", "", "comma-separated list of mirror URLs")
		hashFlag = flag.String("hash", "", "expected SHA256 hash (hex) of the file")
		resume   = flag.Bool("resume", true, "enable resume from .gofetch.resume state file")
		ua       = flag.String("useragent", "gofetch/0.1", "User-Agent header")
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: gofetch [options] <url>")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nexamples:")
		fmt.Fprintln(os.Stderr, "  gofetch -w 8 https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -mirrors 'https://a/file,https://b/file' -resume -o out.bin")
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		return 2
	}
	rawURL := flag.Arg(0)

	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		fmt.Fprintln(os.Stderr, "gofetch: invalid url:", rawURL)
		return 1
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		fmt.Fprintln(os.Stderr, "gofetch: unsupported scheme:", u.Scheme, "(use http or https)")
		return 1
	}

	if *hashFlag != "" {
		if err := fetch.ValidateHexSHA256(*hashFlag); err != nil {
			fmt.Fprintln(os.Stderr, "gofetch: invalid -hash:", err)
			return 1
		}
	}

	var mirrorList []string
	if *mirrors != "" {
		for _, m := range strings.Split(*mirrors, ",") {
			m = strings.TrimSpace(m)
			if m != "" && m != rawURL {
				mirrorList = append(mirrorList, m)
			}
		}
	}

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

	d := fetch.NewDownloader(rawURL, out, fetch.Options{
		WorkerCount:    *workers,
		BufSize:        *bufSize,
		Timeout:        *timeout,
		Mirrors:        mirrorList,
		Resume:         *resume,
		ExpectedSHA256: *hashFlag,
		UserAgent:      *ua,
	})

	if err := d.Download(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "gofetch:", err)
		return 1
	}
	if !*quiet {
		fmt.Println(out)
	}
	return 0
}
