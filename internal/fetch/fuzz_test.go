package fetch

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Fuzz targets run their seed corpus during normal `go test` (as unit
// tests) and can be fuzzed with `go test -fuzz=FuzzName`. They harden
// the parsers and range algebra against adversarial inputs — gofetch
// consumes attacker-controlled URLs, Content-Length, Content-Range and
// manifest files.

// FuzzParseContentRange checks parseContentRange never panics and that
// a successful parse returns a self-consistent range.
func FuzzParseContentRange(f *testing.F) {
	for _, seed := range []string{
		"bytes 0-9/10",
		"bytes 0-0/1",
		"bytes 100-199/1000",
		"bytes 0-9/0",
		"",
		"bytes -1-9/10",
		"bytes 0-9",
		"garbage",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, v string) {
		start, end, total, ok := parseContentRange(v)
		if ok {
			if start < 0 || end < start || total < 0 {
				t.Fatalf("parseContentRange(%q) = %d-%d/%d, inconsistent", v, start, end, total)
			}
		}
	})
}

// FuzzParseUint checks parseUint never panics and never returns a
// negative value (it is unsigned by construction).
func FuzzParseUint(f *testing.F) {
	for _, seed := range []string{"0", "1", "18446744073709551615", "", "-1", "abc", "999999999999999999999999"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, v string) {
		n, err := parseUint(v)
		if err == nil && n < 0 {
			t.Fatalf("parseUint(%q) = %d, negative", v, n)
		}
	})
}

// FuzzParseRetryAfter checks parseRetryAfter never panics and always
// yields a non-negative duration within the documented cap.
func FuzzParseRetryAfter(f *testing.F) {
	for _, seed := range []string{"", "5", "300", "301", "0", "-1", "abc", "1000000000000"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, v string) {
		h := http.Header{}
		h.Set("Retry-After", v)
		d := parseRetryAfter(h)
		if d < 0 || d > 300*time.Second {
			t.Fatalf("parseRetryAfter(%q) = %v, out of range", v, d)
		}
	})
}

// FuzzParseHashFlag checks ParseHashFlag never panics and that success
// implies a non-empty hex and a known algorithm.
func FuzzParseHashFlag(f *testing.F) {
	for _, seed := range []string{
		"",
		"sha256:" + strings.Repeat("a", 64),
		"sha512:" + strings.Repeat("b", 128),
		"sha256:zz",
		"deadbeef",
		"md5:abc",
		"sha256",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, v string) {
		algo, hex, err := ParseHashFlag(v)
		if err == nil && v != "" {
			if hex == "" {
				t.Fatalf("ParseHashFlag(%q) returned empty hex on success", v)
			}
			if algo != "sha256" && algo != "sha512" {
				t.Fatalf("ParseHashFlag(%q) = algo %q", v, algo)
			}
		}
	})
}

// FuzzParseSidecarContent checks ParseSidecarContent never panics and
// that success implies a non-empty hex and a known algorithm.
func FuzzParseSidecarContent(f *testing.F) {
	f.Add("abc  file.sha256", "file.sha256")
	f.Add("", "file")
	f.Add("zzz", "file.sha256")
	f.Add(strings.Repeat("a", 128)+"  out.bin\n", "file.sha512")
	f.Fuzz(func(t *testing.T, content, src string) {
		algo, hex, err := ParseSidecarContent(content, src)
		if err == nil {
			if hex == "" {
				t.Fatalf("ParseSidecarContent(%q,%q) empty hex", content, src)
			}
			if algo != "sha256" && algo != "sha512" {
				t.Fatalf("ParseSidecarContent(%q,%q) algo=%q", content, src, algo)
			}
		}
	})
}

// FuzzSplitRange checks splitRange never panics, never exceeds
// maxSeedTasks, and emits sorted, contiguous, non-overlapping tasks for
// well-formed (non-negative) offsets.
func FuzzSplitRange(f *testing.F) {
	f.Add(int64(0), int64(0), int64(1024))
	f.Add(int64(100), int64(1024*1024), int64(1<<20))
	f.Add(int64(0), int64(1<<40), int64(1<<20))
	f.Add(int64(5), int64(1000), int64(0))
	f.Add(int64(-10), int64(1000), int64(16))
	f.Fuzz(func(t *testing.T, offset, length, chunkSize int64) {
		tasks := splitRange(offset, length, chunkSize)
		if len(tasks) > maxSeedTasks {
			t.Fatalf("splitRange produced %d tasks > maxSeedTasks", len(tasks))
		}
		if offset < 0 || length < 0 {
			return // caller-error domain: no structural guarantees
		}
		for i, task := range tasks {
			if task.Start < 0 || task.End < task.Start {
				t.Fatalf("task[%d] invalid: %+v", i, task)
			}
			if i > 0 && task.Start <= tasks[i-1].End {
				t.Fatalf("tasks overlap: %+v then %+v", tasks[i-1], task)
			}
		}
	})
}

// FuzzUncompleted checks uncompleted never panics and that gaps are
// within bounds and non-overlapping.
func FuzzUncompleted(f *testing.F) {
	f.Add(int64(0), int64(100), int64(10), int64(20))
	f.Add(int64(0), int64(9), int64(0), int64(4))
	f.Add(int64(0), int64(9), int64(0), int64(200))
	f.Add(int64(50), int64(100), int64(0), int64(60))
	f.Fuzz(func(t *testing.T, fs, fe, cs, ce int64) {
		if fs < 0 || fe < fs {
			return
		}
		full := Task{Start: fs, End: fe}
		gaps := uncompleted(full, []Task{{Start: cs, End: ce}})
		for i, g := range gaps {
			if g.Start < fs || g.End > fe || g.End < g.Start {
				t.Fatalf("gap[%d] %+v outside full %+v", i, g, full)
			}
			if i > 0 && g.Start <= gaps[i-1].End {
				t.Fatalf("gaps overlap: %+v then %+v", gaps[i-1], g)
			}
		}
	})
}

// FuzzDedupTasks checks dedupTasks never panics and that the merged
// output of well-formed inputs is sorted and non-overlapping.
func FuzzDedupTasks(f *testing.F) {
	f.Add(int64(0), int64(10), int64(5), int64(15))
	f.Add(int64(0), int64(0), int64(1), int64(1))
	f.Add(int64(10), int64(20), int64(0), int64(5))
	f.Add(int64(0), int64(10), int64(10), int64(5))
	f.Fuzz(func(t *testing.T, a1, a2, b1, b2 int64) {
		if a1 > a2 || b1 > b2 {
			return // caller-error domain: inverted ranges are tolerated, not merged
		}
		tasks := dedupTasks([]Task{{Start: a1, End: a2}, {Start: b1, End: b2}})
		for i, task := range tasks {
			if task.End < task.Start {
				t.Fatalf("task[%d] invalid: %+v", i, task)
			}
			if i > 0 && tasks[i].Start <= tasks[i-1].End {
				t.Fatalf("tasks overlap: %+v then %+v", tasks[i-1], task)
			}
		}
	})
}

// FuzzManifestJSON checks LoadManifest never panics and that accepted
// manifests are internally consistent (valid version, no inverted
// chunks) with a buildable index.
func FuzzManifestJSON(f *testing.F) {
	f.Add([]byte(`{"version":1,"algo":"sha256","chunks":[{"start":0,"end":9,"hash":"abc"}]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"version":0}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"version":1,"chunks":[{"start":5,"end":2}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "m.json")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		m, err := LoadManifest(path)
		if err != nil {
			return
		}
		if m.Version < 1 || m.Version > ManifestVersion {
			t.Fatalf("LoadManifest accepted version %d", m.Version)
		}
		m.buildIndex()
		for _, c := range m.Chunks {
			if c.End < c.Start {
				t.Fatalf("LoadManifest kept inverted chunk %+v", c)
			}
		}
	})
}
