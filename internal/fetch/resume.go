package fetch

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"sort"
	"time"
)

// ResumeState is the JSON document written next to a partial download.
// HashAlgo/ExpectedHash carry algorithm metadata so resuming a sha512
// download doesn't get demoted to sha256 by a reader that only sees
// d.expectedHash on the receiver side.
type ResumeState struct {
	URL            string    `json:"url"`
	OutFile        string    `json:"out_file"`
	TotalSize      int64     `json:"total_size"`
	HashAlgo       string    `json:"hash_algo,omitempty"`
	ExpectedHash   string    `json:"expected_hash,omitempty"`
	Completed      []Task    `json:"completed"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// resumePath returns the canonical sidecar path for an output file.
func resumePath(outFile string) string { return outFile + ".gofetch.resume" }

// saveResume writes the current progress to disk atomically (tmp+rename).
// Returns nil if d.resumePath is empty (resume disabled).
func (d *Downloader) saveResume(completed []Task) error {
	if d.resumePath == "" {
		return nil
	}
	state := ResumeState{
		URL: d.url, OutFile: d.outFile, TotalSize: d.totalSize,
		HashAlgo: d.hashAlgo, ExpectedHash: d.expectedHash,
		Completed: dedupTasks(completed),
		CreatedAt: d.startTime, UpdatedAt: time.Now(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := d.resumePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, d.resumePath)
}

// loadResume reads and validates a state file. Returns nil state with
// no error if the URL or size don't match (a different download, not
// an error) or if the file does not exist. Algo/expected_hash are
// inherited from JSON tags backwards; on a legacy file lacking the
// new fields, the caller should leave d.hashAlgo untouched.
func loadResume(path, url string, totalSize int64) (*ResumeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var st ResumeState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.URL != url || st.TotalSize != totalSize {
		return nil, nil
	}
	return &st, nil
}

// clearResume removes the resume state file. No-op if path is empty.
func clearResume(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

// sortByStart sorts tasks in-place by Start ascending.
func sortByStart(tasks []Task) {
	if len(tasks) > 1 {
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].Start < tasks[j].Start })
	}
}

// dedupTasks merges overlapping or adjacent completed ranges and sorts
// them by Start. This prevents the resume file from growing with
// redundant entries over repeated abort/resume cycles.
func dedupTasks(tasks []Task) []Task {
	if len(tasks) <= 1 {
		return tasks
	}
	sorted := make([]Task, len(tasks))
	copy(sorted, tasks)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })
	n := 1
	for _, t := range sorted[1:] {
		last := &sorted[n-1]
		if t.Start <= last.End+1 {
			if t.End > last.End {
				last.End = t.End
			}
		} else {
			sorted[n] = t
			n++
		}
	}
	return sorted[:n]
}
