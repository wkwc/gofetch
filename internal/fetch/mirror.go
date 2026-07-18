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

// drainAndClose discards any remaining bytes and closes the body, so the
// underlying connection can be reused by http.Transport.
func drainAndClose(body io.ReadCloser) { _, _ = io.Copy(io.Discard, body); body.Close() }

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

func (d *Downloader) probeHeadURL(ctx context.Context, rawURL string) (probeInfo, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return probeInfo{}, false, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := d.client.Do(req)
	if err != nil {
		return probeInfo{}, false, err
	}
	drainAndClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusOK:
		ar := resp.Header.Get("Accept-Ranges")
		total := resp.ContentLength
		if total < 0 {
			// Server didn't send Content-Length. Treat as unknown size;
			// the range-GET fallback will determine the actual size.
			total = 0
		}
		return probeInfo{
			supportsRanges: ar != "" && ar != "none",
			total:          total,
		}, true, nil
	case resp.StatusCode == http.StatusMethodNotAllowed,
		resp.StatusCode == http.StatusBadRequest,
		resp.StatusCode == http.StatusNotImplemented:
		return probeInfo{}, false, nil
	default:
		return probeInfo{}, false, httpError("HEAD", rawURL, resp.StatusCode)
	}
}

func (d *Downloader) probeRangeGetURL(ctx context.Context, rawURL string) (probeInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return probeInfo{}, err
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", userAgent)
	resp, err := d.client.Do(req)
	if err != nil {
		return probeInfo{}, err
	}
	drainAndClose(resp.Body)
	switch resp.StatusCode {
	case http.StatusPartialContent:
		_, _, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok {
			return probeInfo{}, errors.New("malformed Content-Range")
		}
		return probeInfo{supportsRanges: true, total: total}, nil
	case http.StatusOK:
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
