package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
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
// 1-byte range GET.
func (d *Downloader) probeURL(ctx context.Context, rawURL string) (probeInfo, error) {
	if info, ok, err := d.probeHeadURL(ctx, rawURL); ok || err != nil {
		return info, err
	}
	return d.probeRangeGetURL(ctx, rawURL)
}

// probeRequest issues a single probe request (HEAD or range GET), drains
// the body so the connection can be reused, and returns the response.
func (d *Downloader) probeRequest(ctx context.Context, method, rawURL, rangeHeader string) (*http.Response, error) {
	req, err := newRequest(ctx, method, rawURL, rangeHeader)
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

func httpError(method, url string, code int) error {
	return fmt.Errorf("%s %s: status %d", method, url, code)
}

// parseContentRange parses "bytes START-END/TOTAL".
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
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

// parseUint parses a non-empty base-10 unsigned integer.
func parseUint(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("empty")
	}
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, errors.New("non-digit")
		}
		d := int64(c - '0')
		if n > (math.MaxInt64-d)/10 {
			return 0, errors.New("overflow")
		}
		n = n*10 + d
	}
	return n, nil
}

// userAgent is a fixed, sensible default.
const userAgent = "gofetch/1.0"

// newRequest builds an HTTP request with gofetch's fixed headers: the
// user agent and an explicit Accept-Encoding: identity so a proxy can
// never inject compression that would desync Range offsets or integrity
// checks (the transport already disables transparent gzip). rangeHeader
// may be empty for non-range requests.
func newRequest(ctx context.Context, method, url, rangeHeader string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept-Encoding", "identity")
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	return req, nil
}
