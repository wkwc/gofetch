package main

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/wkwc/gofetch/internal/fetch"
)

// validateURL ensures the URL is fetchable in a public-downloader
// context: scheme is http or https, AND the resolved IP is not a
// private/loopback/link-local/unspecified address. DNS failures REJECT
// the URL — an attacker who can fail DNS resolution (or trick
// getaddrinfo into short-timeout behaviour) must not be able to bypass
// the internal-IP check by exploiting a transient lookup error.
// ctx bounds the DNS lookup so a slow resolver cannot hang startup.
func validateURL(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid URL: %s", rawURL)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported scheme: %s (use http or https)", u.Scheme)
	}
	// SSRF: reject URLs that resolve to loopback / private / link-local /
	// multicast / unspecified IPs. Combined with DialContextAuto (which
	// pins each dial to a non-private resolved IP), DNS rebinding cannot
	// pivot the connection into an internal host mid-stream.
	if fetch.HostIsPrivateContext(ctx, u.Hostname()) {
		return fmt.Errorf("URL host %q resolves to a private/internal address (SSRF guard)",
			u.Hostname())
	}
	return nil
}

// normalizeMirrors parses the -m flag into validated mirror URLs. Bare
// hostnames get https:// prepended so `-m mirror1.com,mirror2.com` works
// as documented. Each mirror is validated with the same SSRF guards as the
// primary URL.
func normalizeMirrors(ctx context.Context, flag string) ([]string, error) {
	if flag == "" {
		return nil, nil
	}
	parts := strings.Split(flag, ",")
	mirrors := make([]string, 0, len(parts))
	for i, m := range parts {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		if !strings.Contains(m, "://") {
			m = "https://" + m
		}
		if err := validateURL(ctx, m); err != nil {
			return nil, fmt.Errorf("mirror %d: %w", i+1, err)
		}
		mirrors = append(mirrors, m)
	}
	return mirrors, nil
}

// parseRateLimit parses a human bandwidth cap like "500k", "2M", "1G"
// (KiB/MiB/GiB, case-insensitive) into bytes/second. Bare numbers are
// bytes/second. "" or "0" disables throttling.
func parseRateLimit(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	mult := int64(1)
	switch last := s[len(s)-1]; last {
	case 'k', 'K':
		mult = 1 << 10
		s = s[:len(s)-1]
	case 'm', 'M':
		mult = 1 << 20
		s = s[:len(s)-1]
	case 'g', 'G':
		mult = 1 << 30
		s = s[:len(s)-1]
	}
	if s == "" {
		return 0, fmt.Errorf("invalid rate %q (use e.g. 500k, 2M, 1G)", s)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid rate %q (use e.g. 500k, 2M, 1G)", s)
	}
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("rate too large: %q", s)
	}
	return n * mult, nil
}

// validateHeaders rejects malformed "Name: value" headers up front so
// the download loop never starts with a header that will be dropped.
func validateHeaders(headers []string) error {
	for _, h := range headers {
		name, _, ok := strings.Cut(h, ":")
		if !ok || strings.TrimSpace(name) == "" {
			return fmt.Errorf("invalid header %q (expected 'Name: value')", h)
		}
	}
	return nil
}
