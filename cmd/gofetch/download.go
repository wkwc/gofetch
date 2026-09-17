package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	fssys "io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/wkwc/gofetch/internal/fetch"
)

// interruptExit is the process status for Ctrl-C/SIGTERM/SIGHUP: partial
// progress was flushed to the resume sidecar, so the shell sees 130
// (128+SIGINT) like any other interrupted program.
const interruptExit = 130

// cliConfig carries CLI settings from flag parsing to the download loop.
// Raw string flags (mirrorsFlag, limitRate, bufSize) are validated once by
// resolveConfig, which fills the derived fields (mirrors, rate, bufBytes).
// Binding flags directly into this struct (instead of ~20 flag pointers)
// is what lets runOne/runInfo stay small and independently readable.
type cliConfig struct {
	outPath     string
	hashFlag    string
	manifestOut string
	userAgent   string
	proxy       string
	caCert      string
	mirrorsFlag string
	limitRate   string
	bufSize     string
	headers     headerList
	mirrors     []string
	rate        int64
	bufBytes    int64
	workers     int
	maxRetries  int
	quiet       bool
	verbose     bool
	noResume    bool
	noClobber   bool
	noMmap      bool
	jsonOut     bool
	info        bool
	allowLocal  bool
	showVersion bool
}

// resolveConfig validates raw flag values and fills the derived fields.
// Every failure prints as "gofetch: <err>" with exit 1 at the caller, so
// returned errors carry no prefix. Order matches the flag help listing,
// and messages are byte-identical to the historical CLI.
func resolveConfig(ctx context.Context, cfg cliConfig) (cliConfig, error) {
	mirrors, err := normalizeMirrors(ctx, cfg.mirrorsFlag)
	if err != nil {
		return cfg, err
	}
	cfg.mirrors = mirrors
	rate, err := parseRateLimit(cfg.limitRate)
	if err != nil {
		return cfg, err
	}
	cfg.rate = rate
	if err := validateHeaders(cfg.headers); err != nil {
		return cfg, err
	}
	if cfg.proxy != "" {
		if p, err := url.Parse(cfg.proxy); err != nil || (p.Scheme != "http" && p.Scheme != "https" && p.Scheme != "socks5") {
			return cfg, errors.New("invalid --proxy URL (use http://, https:// or socks5://)")
		}
	}
	if cfg.workers < 0 || cfg.workers > 256 {
		return cfg, errors.New("-x workers must be 0 (auto) or between 1 and 256")
	}
	if cfg.maxRetries < 0 || cfg.maxRetries > 100 {
		return cfg, errors.New("--max-retries must be 0 (auto) or between 1 and 100")
	}
	if cfg.jsonOut && !cfg.info {
		return cfg, errors.New("--json requires --info")
	}
	if cfg.caCert != "" {
		if err := fetch.ValidateCACert(cfg.caCert); err != nil {
			return cfg, fmt.Errorf("--ca-cert: %v", err)
		}
	}
	if cfg.bufSize != "" {
		// Same number+suffix parser as --limit-rate; value is bytes here.
		bufBytes, err := parseRateLimit(cfg.bufSize)
		if err != nil {
			return cfg, err
		}
		if bufBytes != 0 && (bufBytes < 4096 || bufBytes > 32<<20) {
			return cfg, errors.New("--buf-size must be between 4k and 32M")
		}
		cfg.bufBytes = bufBytes
	}
	return cfg, nil
}

// runOne downloads a single URL to out, returning the process exit code
// for it (0 ok, 1 failed/skipped-with-error, interruptExit on cancel).
// Callers stop the batch on interruptExit, else accumulate failures.
func runOne(ctx context.Context, cfg cliConfig, rawURL, out string, sharedTr *http.Transport, multi bool) int {
	if cfg.noClobber {
		// Skip only COMPLETE files. A partial download is identified by
		// a resume sidecar — skipping it would strand the partial bytes
		// forever; instead proceed and resume.
		if _, err := os.Stat(out); err == nil {
			if _, serr := os.Stat(out + ".gofetch.resume"); errors.Is(serr, fssys.ErrNotExist) {
				fmt.Fprintf(os.Stderr, "gofetch: %s: already exists, skipping\n", out)
				return 0
			}
		}
	}
	algo, hashHex, err := resolveHash(ctx, cfg.hashFlag, rawURL, out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gofetch: %s: %v\n", rawURL, err)
		return 1
	}
	d := fetch.NewDownloader(rawURL, out, fetch.Options{
		HashAlgo:     algo,
		ExpectedHash: hashHex,
		NoResume:     cfg.noResume,
		Verbose:      cfg.verbose,
		Quiet:        cfg.quiet,
		Mirrors:      cfg.mirrors,
		Headers:      cfg.headers,
		RateLimit:    cfg.rate,
		Proxy:        cfg.proxy,
		UserAgent:    cfg.userAgent,
		Workers:      cfg.workers,
		BufSize:      int(cfg.bufBytes),
		RetryMax:     cfg.maxRetries,
		CACert:       cfg.caCert,
		NoMmap:       cfg.noMmap,
		Transport:    sharedTr,
	})

	err = d.Download(ctx)
	d.Close() // release keep-alive connections regardless of outcome
	if err != nil {
		// User-initiated cancel (Ctrl-C / SIGTERM / SIGHUP) is not a
		// failure of the downloader — the partial progress was already
		// flushed to the resume sidecar, so say so plainly instead of
		// wrapping it as a mirror error.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintf(os.Stderr,
				"gofetch: interrupted; partial progress saved to %s, re-run to resume\n",
				out+".gofetch.resume")
			return interruptExit
		}
		fmt.Fprintf(os.Stderr, "gofetch: %s: %v\n", rawURL, err)
		return 1
	}
	if cfg.manifestOut != "" {
		mout := cfg.manifestOut
		if multi {
			if err := os.MkdirAll(mout, 0o755); err != nil {
				fmt.Fprintf(os.Stderr, "gofetch: %s: manifest dir: %v\n", rawURL, err)
				return 1
			}
			mout = filepath.Join(mout, filepath.Base(out)+".gofetch.manifest")
		}
		if err := writeManifest(mout, out); err != nil {
			fmt.Fprintf(os.Stderr, "gofetch: %s: %v\n", rawURL, err)
			return 1
		}
	}
	// Always print the output path on success (quiet: filename only;
	// verbose/normal: summary already went to stderr in finalize).
	fmt.Println(out)
	return 0
}

// runInfo probes each URL and reports without downloading.
func runInfo(ctx context.Context, rawURLs []string, jsonOut bool) int {
	exit := 0
	for _, u := range rawURLs {
		p, err := fetch.ProbeURL(ctx, u)
		// Would -h auto verify this URL? Local sidecar first, then the
		// remote checksum detection.
		cksum, _, _ := autoDetectLocalSidecar(urlBaseName(u))
		if cksum == "" {
			cksum, _, _ = autoDetectRemoteSidecar(ctx, u)
		}
		if err != nil {
			if jsonOut {
				emitProbeJSON(u, fetch.ProbeInfo{}, err, "")
			} else {
				fmt.Fprintf(os.Stderr, "gofetch: %s: %v\n", u, err)
			}
			exit = 1
			continue
		}
		if jsonOut {
			emitProbeJSON(u, p, nil, cksum)
		} else {
			ranges := "no"
			if p.SupportsRanges {
				ranges = "yes"
			}
			checksum := "none"
			if cksum != "" {
				checksum = cksum + " (auto)"
			}
			fmt.Printf("url:    %s\n", u)
			fmt.Printf("size:   %s\n", fetch.HumanBytes(p.Total))
			fmt.Printf("ranges: %s\n", ranges)
			fmt.Printf("workers: %d\n", p.Workers)
			fmt.Printf("buf:    %s\n", fetch.HumanBytes(int64(p.BufSize)))
			fmt.Printf("checksum: %s\n", checksum)
			fmt.Println()
		}
	}
	return exit
}

// probeJSON is the machine-readable shape of one --info --json result.
// On failure only url and error are populated; success carries the probe.
type probeJSON struct {
	URL            string `json:"url"`
	Size           int64  `json:"size,omitempty"`
	SupportsRanges bool   `json:"supports_ranges,omitempty"`
	Workers        int    `json:"workers,omitempty"`
	BufSize        int    `json:"buf_size,omitempty"`
	Checksum       string `json:"checksum,omitempty"` // algo -h auto would find ("" = none)
	Error          string `json:"error,omitempty"`
}

// emitProbeJSON prints one probe result (or its error) as a JSON line.
// Uses encoding/json (not fmt %q) so arbitrary URL bytes stay valid JSON.
func emitProbeJSON(rawURL string, p fetch.ProbeInfo, err error, checksum string) {
	out := probeJSON{URL: rawURL}
	if err != nil {
		out.Error = err.Error()
	} else {
		out.Size = p.Total
		out.SupportsRanges = p.SupportsRanges
		out.Workers = p.Workers
		out.BufSize = p.BufSize
		out.Checksum = checksum
	}
	_ = json.NewEncoder(os.Stdout).Encode(out)
}
