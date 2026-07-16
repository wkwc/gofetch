package fetch

import (
	"testing"
)

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  struct {
			start, end, total int64
			ok                bool
		}
	}{
		{"basic", "bytes 0-99/100", struct {
			start, end, total int64
			ok                bool
		}{0, 99, 100, true}},
		{"large", "bytes 1000-1999/5000", struct {
			start, end, total int64
			ok                bool
		}{1000, 1999, 5000, true}},
		{"missing prefix", "0-99/100", struct {
			start, end, total int64
			ok                bool
		}{0, 0, 0, false}},
		{"malformed", "bytes 0/100", struct {
			start, end, total int64
			ok                bool
		}{0, 0, 0, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, e, total, ok := parseContentRange(tt.input)
			if ok != tt.want.ok || s != tt.want.start || e != tt.want.end || total != tt.want.total {
				t.Errorf("parseContentRange(%q) = (%d,%d,%d,%v), want (%d,%d,%d,%v)",
					tt.input, s, e, total, ok, tt.want.start, tt.want.end, tt.want.total, tt.want.ok)
			}
		})
	}
}