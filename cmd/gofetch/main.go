// Command gofetch downloads a single URL (or mirror list) to disk using
// concurrent HTTP range requests, adaptive work-stealing, and QUIC transport.
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
	exit := run()
	os.Exit(exit)
}

func run() int {
	var (
		workers    = flag.Int("w", 4, "number of concurrent workers")
		bufSize    = flag.Int("buf", 64*1024, "per-worker buffer size in bytes")
		timeout    = flag.Duration("timeout", 30*time.Second, "per-request HTTP timeout")
		outPath    = flag.String("o", "", "output file path (default: basename of URL)")
		quiet      = flag.Bool("q", false, "suppress progress output")
		mirrors    = flag.String("mirrors", "", "comma-separated list of mirror URLs")
		hashFlag   = flag.String("hash", "", "expected SHA256 hash (hex) or 'auto' to fetch .sha256 sidecar")
		resume     = flag.Bool("resume", true, "enable resume from .gofetch.json state file")
		preferQUIC = flag.Bool("quic", true, "prefer QUIC/HTTP3 transport when available")
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: gofetch [options] <url>")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nexamples:")
		fmt.Fprintln(os.Stderr, "  gofetch -w 8 https://example.com/file.bin")
		fmt.Fprintln(os.Stderr, "  gofetch -mirrors 'https://a/file,https://b/file' -hash auto -resume -o out.bin")
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

	mirrorList := []string{rawURL}
	if *mirrors != "" {
		for _, m := range strings.Split(*mirrors, ",") {
			m = strings.TrimSpace(m)
			if m != "" {
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

	hashType := fetch.HashSHA256
	var expectedHash string
	sidecar := false
	if *hashFlag == "auto" {
		sidecar = true
	} else if *hashFlag != "" {
		expectedHash = *hashFlag
	}

	d := fetch.NewDownloader(rawURL, out, fetch.Options{
		WorkerCount: *workers,
		BufSize:     *bufSize,
		Timeout:     *timeout,
		Mirrors:     mirrorList,
		PreferQUIC:  *preferQUIC,
		Resume:      *resume,
		VerifyConfig: fetch.VerifyConfig{
			HashType:   hashType,
			Expected:   expectedHash,
			Sidecar:    sidecar,
			SidecarURL: rawURL + ".sha256",
		},
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
