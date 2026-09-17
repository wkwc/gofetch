package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	fssys "io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/wkwc/gofetch/internal/fetch"
)

// interruptExit is the process status for Ctrl-C/SIGTERM/SIGHUP: partial
// progress was flushed to the resume sidecar, so the shell sees 130
// (128+SIGINT) like any other interrupted program.
const interruptExit = 130

// cliConfig carries validated CLI settings from flag parsing to the
// download loop. Grouping them (instead of threading ~15 flag pointers
// through every helper) is what lets runOne/runInfo stay small and
// independently readable.
type cliConfig struct {
	outPath     string
	hashFlag    string
	manifestOut string
	userAgent   string
	proxy       string
	caCert      string
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
