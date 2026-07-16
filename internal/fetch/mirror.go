package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

// drainAndClose discards any remaining bytes and closes the body, so the
// underlying connection can be reused by http.Transport.
func drainAndClose(body io.ReadCloser) { _, _ = io.Copy(io.Discard, body); body.Close() }

// Mirror represents a download source with its measured latency.
type Mirror struct {
	URL       string
	Latency   time.Duration
	Healthy   bool
	ETag      string
	TotalSize int64
}

// probeInfo is what we learn about a server before downloading.
type probeInfo struct {
	supportsRanges bool
	total          int64
	etag           string
}

// selectMirror probes all d.mirrors in parallel, picks the healthiest,
// and validates ETag agreement across all healthy mirrors.
func (d *Downloader) selectMirror(ctx context.Context) (*Mirror, probeInfo, error) {
	type result struct {
		mirror *Mirror
		info   probeInfo
		err    error
	}
	all := append([]string{d.url}, d.mirrors...)
	ch := make(chan result, len(all))
	for _, u := range all {
		go func(rawURL string) {
			m := &Mirror{URL: rawURL}
			start := time.Now()
			info, err := d.probeURL(ctx, rawURL)
			m.Latency = time.Since(start)
			if err == nil && info.total >= 0 {
				m.Healthy = true
				m.ETag = info.etag
				m.TotalSize = info.total
			}
			ch <- result{m, info, err}
		}(u)
	}
	results := make([]result, len(all))
	for i := range results {
		results[i] = <-ch
	}

	var best *Mirror
	bestInfo := probeInfo{}
	for _, r := range results {
		if r.err != nil || !r.mirror.Healthy {
			continue
		}
		if best == nil || r.mirror.Latency < best.Latency {
			best = r.mirror
			bestInfo = r.info
		}
	}
	if best == nil {
		return nil, probeInfo{}, errors.New("all mirrors failed")
	}
	var etag string
	for _, r := range results {
		if r.err != nil || !r.mirror.Healthy || r.mirror.ETag == "" {
			continue
		}
		if etag == "" {
			etag = r.mirror.ETag
		} else if etag != r.mirror.ETag {
			return nil, probeInfo{}, fmt.Errorf("mirrors serve different files: ETag %q vs %q", etag, r.mirror.ETag)
		}
	}
	return best, bestInfo, nil
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
	req.Header.Set("User-Agent", d.userAgent)
	resp, err := d.client.Do(req)
	if err != nil {
		return probeInfo{}, false, err
	}
	drainAndClose(resp.Body)
	switch {
	case resp.StatusCode == http.StatusOK:
		ar := resp.Header.Get("Accept-Ranges")
		return probeInfo{supportsRanges: ar != "" && ar != "none", total: resp.ContentLength, etag: resp.Header.Get("ETag")}, true, nil
	case resp.StatusCode == http.StatusMethodNotAllowed,
		resp.StatusCode == http.StatusBadRequest,
		resp.StatusCode == http.StatusNotImplemented:
		// Server doesn't support HEAD; fall back to a 1-byte range GET.
		return probeInfo{}, false, nil
	default:
		return probeInfo{}, false, fmt.Errorf("HEAD %s: status %d", rawURL, resp.StatusCode)
	}
}

func (d *Downloader) probeRangeGetURL(ctx context.Context, rawURL string) (probeInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return probeInfo{}, err
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", d.userAgent)
	resp, err := d.client.Do(req)
	if err != nil {
		return probeInfo{}, err
	}
	drainAndClose(resp.Body)
	switch resp.StatusCode {
	case http.StatusPartialContent:
		_, _, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok {
			return probeInfo{}, fmt.Errorf("malformed Content-Range")
		}
		return probeInfo{supportsRanges: true, total: total, etag: resp.Header.Get("ETag")}, nil
	case http.StatusOK:
		return probeInfo{supportsRanges: false, total: resp.ContentLength, etag: resp.Header.Get("ETag")}, nil
	default:
		return probeInfo{}, fmt.Errorf("range GET %s: status %d", rawURL, resp.StatusCode)
	}
}

// parseContentRange parses "bytes START-END/TOTAL".
// Returns (start, end, total, ok).
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
// Returns an error on empty input, non-digit byte, or overflow.
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
