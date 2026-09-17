package main

import (
	"context"
	"strings"
	"testing"
)

// TestResolveConfig pins every flag-validation guard: each invalid input
// must fail with the historical message (the CLI prints "gofetch: <err>").
func TestResolveConfig(t *testing.T) {
	ctx := context.Background()
	valid := cliConfig{}
	cases := []struct {
		name    string
		mutate  func(*cliConfig)
		wantErr string
	}{
		{"proxy scheme", func(c *cliConfig) { c.proxy = "ftp://x" }, "invalid --proxy URL"},
		{"workers negative", func(c *cliConfig) { c.workers = -1 }, "-x workers must be 0 (auto) or between 1 and 256"},
		{"workers too many", func(c *cliConfig) { c.workers = 257 }, "-x workers must be 0 (auto) or between 1 and 256"},
		{"retries negative", func(c *cliConfig) { c.maxRetries = -1 }, "--max-retries must be 0 (auto) or between 1 and 100"},
		{"retries too many", func(c *cliConfig) { c.maxRetries = 101 }, "--max-retries must be 0 (auto) or between 1 and 100"},
		{"json without info", func(c *cliConfig) { c.jsonOut = true }, "--json requires --info"},
		{"buf-size garbage", func(c *cliConfig) { c.bufSize = "huge" }, "invalid rate"},
		{"buf-size too small", func(c *cliConfig) { c.bufSize = "1k" }, "--buf-size must be between 4k and 32M"},
		{"buf-size too big", func(c *cliConfig) { c.bufSize = "64M" }, "--buf-size must be between 4k and 32M"},
		{"bad header", func(c *cliConfig) { c.headers = headerList{"no-colon"} }, "invalid header"},
		{"bad mirror", func(c *cliConfig) { c.mirrorsFlag = "ftp://x" }, "mirror 1:"},
		{"bad rate", func(c *cliConfig) { c.limitRate = "fast" }, "invalid rate"},
		{"missing ca-cert", func(c *cliConfig) { c.caCert = "/nonexistent-ca.pem" }, "--ca-cert:"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			if _, err := resolveConfig(ctx, cfg); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("resolveConfig = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
	t.Run("valid minimal", func(t *testing.T) {
		cfg, err := resolveConfig(ctx, valid)
		if err != nil {
			t.Fatalf("resolveConfig = %v, want nil", err)
		}
		if cfg.rate != 0 || cfg.bufBytes != 0 || len(cfg.mirrors) != 0 {
			t.Errorf("derived fields = %+v, want zeros", cfg)
		}
	})
	t.Run("valid overrides", func(t *testing.T) {
		cfg, err := resolveConfig(ctx, cliConfig{workers: 8, maxRetries: 3, limitRate: "2M", bufSize: "64k"})
		if err != nil {
			t.Fatalf("resolveConfig = %v, want nil", err)
		}
		if cfg.rate != 2<<20 || cfg.bufBytes != 64<<10 || cfg.workers != 8 || cfg.maxRetries != 3 {
			t.Errorf("derived = rate %d buf %d workers %d retries %d, want 2M/64k/8/3",
				cfg.rate, cfg.bufBytes, cfg.workers, cfg.maxRetries)
		}
	})
}
