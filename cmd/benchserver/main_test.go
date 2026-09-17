package main

import "testing"

// TestParseBenchRange pins the benchserver's Range parsing: valid
// "bytes=START-END" inputs round-trip, malformed ones are rejected.
func TestParseBenchRange(t *testing.T) {
	tests := []struct {
		in    string
		start int64
		end   int64
		ok    bool
	}{
		{"bytes=0-0", 0, 0, true},
		{"bytes=2-5", 2, 5, true},
		{"bytes=1048576-2097151", 1048576, 2097151, true},
		{"", 0, 0, false},
		{"bytes=", 0, 0, false},
		{"bytes=-5", 0, 0, false},
		{"bytes=5-", 0, 0, false},
		{"bytes=5", 0, 0, false},
		{"bytes=a-b", 0, 0, false},
		{"items=0-5", 0, 0, false},
	}
	for _, tt := range tests {
		s, e, ok := parseBenchRange(tt.in)
		if ok != tt.ok || s != tt.start || e != tt.end {
			t.Errorf("parseBenchRange(%q) = %d,%d,%v, want %d,%d,%v",
				tt.in, s, e, ok, tt.start, tt.end, tt.ok)
		}
	}
}

// TestBenchRangeHeader pins the exact Content-Range wire format.
func TestBenchRangeHeader(t *testing.T) {
	if got := benchRangeHeader(2, 5, 10); got != "bytes 2-5/10" {
		t.Errorf("benchRangeHeader = %q, want %q", got, "bytes 2-5/10")
	}
	if got := benchRangeHeader(0, 0, 268435456); got != "bytes 0-0/268435456" {
		t.Errorf("benchRangeHeader = %q, want %q", got, "bytes 0-0/268435456")
	}
}
