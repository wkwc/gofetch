package fetch

import (
	"encoding/json"
	"os"
	"sort"
	"time"
)

// ResumeState is the JSON document written next to a partial download.
type ResumeState struct {
	URL            string    `json:"url"`
	OutFile        string    `json:"out_file"`
	TotalSize      int64     `json:"total_size"`
	ExpectedSHA256 string    `json:"expected_sha256,omitempty"`
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
		URL: d.URL, OutFile: d.OutFile, TotalSize: d.totalSize,
		ExpectedSHA256: d.expectedHash, Completed: completed,
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
// an error) or if the file does not exist.
func loadResume(path, url string, totalSize int64) (*ResumeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
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
func sortByStart(tasks []Task) []Task {
	if len(tasks) > 1 {
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].Start < tasks[j].Start })
	}
	return tasks
}
