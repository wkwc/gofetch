package main

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/wkwc/gofetch/internal/fetch"
)

// validateURL ensures the URL is fetchable in a public-downloader
// context: scheme is http or https, AND the resolved IP is not a
// private/loopback/link-local/unspecified address. DNS failures REJECT
// the URL — an attacker who can fail DNS resolution (or trick
// getaddrinfo into short-timeout behaviour) must not be able to bypass
// the internal-IP check by exploiting a transient lookup error.
func validateURL(rawURL string) error {
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
	if fetch.HostIsPrivate(u.Hostname()) {
		return fmt.Errorf("URL host %q resolves to a private/internal address (SSRF guard)",
			u.Hostname())
	}
	return nil
}

// normalizeMirrors parses the -m flag into validated mirror URLs. Bare
// hostnames get https:// prepended so `-m mirror1.com,mirror2.com` works
// as documented. Each mirror is validated with the same SSRF guards as the
// primary URL.
func normalizeMirrors(flag string) ([]string, error) {
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
		if err := validateURL(m); err != nil {
			return nil, fmt.Errorf("mirror %d: %w", i+1, err)
		}
		mirrors = append(mirrors, m)
	}
	return mirrors, nil
}
