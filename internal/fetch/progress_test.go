package fetch

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestPrintProgressRedirected(t *testing.T) {
	// Force non-TTY: point os.Stderr at a pipe and reset the cached TTY
	// probe so printProgress re-evaluates against the swapped stderr.
	old := os.Stderr
	var buf bytes.Buffer
	r, w, _ := os.Pipe()
	os.Stderr = w
	stderrTTYOnce = sync.Once{}
	defer func() {
		os.Stderr = old
		stderrTTYOnce = sync.Once{}
	}()

	d := &Downloader{quiet: false}
	p := newProgress(100, nil)
	p.add(60)

	d.printProgress(p, false) // live update should be suppressed
	d.printProgress(p, true)  // final should print plain line

	_ = w.Close()
	os.Stderr = old
	_, _ = buf.ReadFrom(r)
	out := buf.String()
	if strings.ContainsAny(out, "\r\x1b") {
		t.Fatalf("redirected output leaked ANSI/CR: %q", out)
	}
	if out == "" {
		t.Fatal("expected final plain line")
	}
	t.Logf("redirected output: %q", out)
}
