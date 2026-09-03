package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// drainLimit caps how much of a rejected response body we will read
// before closing. Bounds hang/bandwidth abuse on huge 4xx/5xx bodies
// while still allowing connection reuse for modest leftovers.
const drainLimit = 32 << 10 // 32 KiB

// drainAndClose discards a bounded amount of remaining bytes and closes
// the body so the underlying connection can be reused by http.Transport.
func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, drainLimit))
	_ = body.Close()
}

// probeInfo is what we learn about a server before downloading.
type probeInfo struct {
	supportsRanges bool
	total          int64
}

// probeURL tries HEAD first; if HEAD returns 405/400 we fall back to a
// 1-byte range GET. Probe requests are retried on transient HTTP statuses
// (429/5xx) with a short backoff — a rate-limited or briefly-busy server
// must not fail the whole download on a single probe, matching the range
// path's retry behavior.
func (d *Downloader) probeURL(ctx context.Context, rawURL string) (probeInfo, error) {
	var (
		info probeInfo
		err  error
	)
	for attempt := 0; attempt < 3; attempt++ {
		info, err = d.probeOnce(ctx, rawURL)
		if err == nil || !isRetryableProbe(err) {
			return info, err
		}
		d.vlog("probe transient error (%v), retrying", err)
		// Probes are cheap; a 1s backoff keeps a rate-limited server from
		// failing the whole download without stalling on a slow one.
		if !sleepCtx(ctx, time.Second) {
			return info, ctx.Err()
		}
	}
	return info, err
}

// probeOnce performs one HEAD probe (falling back to a range GET when the
// server rejects HEAD).
func (d *Downloader) probeOnce(ctx context.Context, rawURL string) (probeInfo, error) {
	if info, ok, err := d.probeHeadURL(ctx, rawURL); ok || err != nil {
		return info, err
	}
	return d.probeRangeGetURL(ctx, rawURL)
}

// probeRequest issues a single probe request (HEAD or range GET), drains
// the body so the connection can be reused, and returns the response.
func (d *Downloader) probeRequest(ctx context.Context, method, rawURL, rangeHeader string) (*http.Response, error) {
	req, err := d.newRequest(ctx, method, rawURL, rangeHeader)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	drainAndClose(resp.Body)
	return resp, nil
}

func (d *Downloader) probeHeadURL(ctx context.Context, rawURL string) (probeInfo, bool, error) {
	resp, err := d.probeRequest(ctx, http.MethodHead, rawURL, "")
	if err != nil {
		return probeInfo{}, false, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		ar := resp.Header.Get("Accept-Ranges")
		total := resp.ContentLength
		if total < 0 {
			// No Content-Length: fall through to range-GET so we can
			// learn size from Content-Range (comment previously claimed
			// this happened, but ok=true short-circuited probeURL).
			return probeInfo{}, false, nil
		}
		return probeInfo{
			supportsRanges: ar != "" && ar != "none",
			total:          total,
		}, true, nil
	case http.StatusMethodNotAllowed, http.StatusBadRequest, http.StatusNotImplemented:
		return probeInfo{}, false, nil
	default:
		return probeInfo{}, false, httpError("HEAD", rawURL, resp.StatusCode)
	}
}

func (d *Downloader) probeRangeGetURL(ctx context.Context, rawURL string) (probeInfo, error) {
	resp, err := d.probeRequest(ctx, http.MethodGet, rawURL, "bytes=0-0")
	if err != nil {
		return probeInfo{}, err
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		_, _, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok {
			// 206 with a malformed Content-Range — some servers (esp.
			// proxies) return a malformed range header even when
			// Content-Length is sound. Fall back to Content-Length
			// and disable range support for this server rather than
			// hard-failing a download we could otherwise complete.
			if cl := resp.ContentLength; cl > 0 {
				return probeInfo{supportsRanges: false, total: cl}, nil
			}
			return probeInfo{}, errors.New("malformed Content-Range and no Content-Length")
		}
		return probeInfo{supportsRanges: true, total: total}, nil
	case http.StatusOK:
		// ContentLength may be -1 when the server omits it (chunked /
		// unknown size). Keep supportsRanges=false and total as-is so
		// single-stream path handles unknown size explicitly.
		return probeInfo{supportsRanges: false, total: resp.ContentLength}, nil
	default:
		return probeInfo{}, httpError("range GET", rawURL, resp.StatusCode)
	}
}

// httpStatusError carries the HTTP status of a failed probe so callers can
// distinguish transient (429/5xx) failures, which are retried, from
// permanent ones.
type httpStatusError struct {
	method string
	url    string
	code   int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s %s: status %d", e.method, e.url, e.code)
}

func httpError(method, url string, code int) error {
	return &httpStatusError{method: method, url: url, code: code}
}

// isRetryableProbe reports whether a probe error is a transient HTTP
// status (429/5xx) worth retrying.
func isRetryableProbe(err error) bool {
	var he *httpStatusError
	return errors.As(err, &he) && isRetryableHTTP(he.code)
}

// parseContentRange parses "bytes START-END/TOTAL". Returns ok=false for
// malformed input, including inverted ranges (START > END) which no
// well-behaved server sends.
func parseContentRange(v string) (start, end, total int64, ok bool) {
	if !strings.HasPrefix(v, "bytes ") {
		return 0, 0, 0, false
	}
	rest := v[len("bytes "):]
	dash := strings.IndexByte(rest, '-')
	slash := strings.IndexByte(rest, '/')
	if dash < 0 || slash < 0 || slash < dash {
		return 0, 0, 0, false
	}
	start, err1 := parseUint(rest[:dash])
	end, err2 := parseUint(rest[dash+1 : slash])
	total, err3 := parseUint(rest[slash+1:])
	if err1 != nil || err2 != nil || err3 != nil || end < start {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

// parseUint parses a non-empty base-10 unsigned integer.
// parseUint parses a non-empty base-10 unsigned integer. Wraps
// strconv.ParseUint (which already rejects '+', '-', and non-digits),
// adding a MaxInt64 bound so Content-Range totals can never wrap.
func parseUint(s string) (int64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if n > math.MaxInt64 {
		return 0, errors.New("overflow")
	}
	return int64(n), nil
}

// defaultUserAgent is the User-Agent unless overridden via Options.
const defaultUserAgent = "gofetch/1.0"

// newRequest builds an HTTP request with gofetch's fixed headers and any
// user-supplied custom headers. User headers are applied first; the
// correctness-critical ones (User-Agent, Accept-Encoding: identity, and
// the task Range) are set last so a `-H` value can never override them —
// a proxy-injected or user-set gzip would desync Range offsets and break
// integrity verification.
func (d *Downloader) newRequest(ctx context.Context, method, url, rangeHeader string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	for _, h := range d.headers {
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
	req.Header.Set("User-Agent", d.userAgent)
	req.Header.Set("Accept-Encoding", "identity")
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	return req, nil
}
