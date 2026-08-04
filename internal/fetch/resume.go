package fetch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ResumeState is the JSON document written next to a partial download.
// HashAlgo/ExpectedHash carry algorithm metadata so resuming a sha512
// download doesn't get demoted to sha256 by a reader that only sees
// d.expectedHash on the receiver side.
// InProgress holds the currently running task and its partial progress
// (bytes done) so an interrupted download resumes from the exact offset.
type ResumeState struct {
	URL            string    `json:"url"`
	OutFile        string    `json:"out_file"`
	TotalSize      int64     `json:"total_size"`
	HashAlgo       string    `json:"hash_algo,omitempty"`
	ExpectedHash   string    `json:"expected_hash,omitempty"`
	Completed      []Task    `json:"completed"`
	InProgress     *Task     `json:"in_progress,omitempty"`
	InProgressDone int64     `json:"in_progress_done,omitempty"`
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

	// Capture in-progress task from any worker.
	//
	// workerState.reset() (worker.go) Stores curTask BEFORE bytesDone(=0)
	// and bumps taskGen AFTER both — this seqcst ordering lets readers
	// detect a mid-reset interleave (curTask=B, bytesDone=A.size) by
	// re-checking taskGen, exactly as the steal plan does at
	// monitor.stealPlan. Without this re-check, saveResume could persist
	// (curTask=B, done=A.size), then loadResume silently marks
	// bytes [B.Start, B.Start+A.size-1] as completed (downloader.go:150-160)
	// even though they were never downloaded — silent corruption recoverable
	// only by an explicit -h hash verify at end of run.
	var inProgress *Task
	var inProgressDone int64
	for _, ws := range d.workerStates {
		if ws == nil {
			continue
		}
		curTask := ws.curTask.Load()
		if curTask == nil {
			continue
		}
		gen := ws.taskGen.Load()
		done := ws.bytesDone.Load()
		// Re-check taskGen: if the worker reset() between the two reads
		// above, curTask+done are from different generations and the pair
		// is unsafe to persist.
		if ws.taskGen.Load() != gen {
			continue
		}
		if done > 0 {
			inProgress = curTask
			inProgressDone = done
			break
		}
	}

	state := ResumeState{
		URL: d.url, OutFile: d.outFile, TotalSize: d.totalSize,
		HashAlgo: d.hashAlgo, ExpectedHash: d.expectedHash,
		Completed:      dedupTasks(completed),
		InProgress:     inProgress,
		InProgressDone: inProgressDone,
		CreatedAt:      d.startTime,
		UpdatedAt:      time.Now(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := d.resumePath + ".tmp"
	// Clear a stale .tmp from a prior crash so O_EXCL cannot permanently
	// disable resume saves for the rest of the download.
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// fsync parent dir for crash durability: a renamed file's metadata
	// is only durable if the directory containing the rename's new link
	// is fsynced. See AIO/APFS/ext4 docs and the rename(2) NOTES section.
	// filepath.Dir never returns "" (returns "." for relative paths), so
	// the historical `if parent != ""` guard was dead code — removed.
	// We open the parent, fsync it, and Propagate any fsync error so the
	// caller doesn't treat this save as durable-succeeded: a silent skip
	// here would defeat the entire purpose of the resume file. The rename
	// is still attempted after a failed dir-fsync, because a possibly-
	// durable .resume is strictly better than a guaranteed orphan .tmp
	// and no rename at all. Parent open failures are quiet because some
	// environments (sandboxed CI, read-only root) cannot fsync the parent
	// but can still rename within it, and that is not actionable per save.
	parent := filepath.Dir(d.resumePath)
	var dirFsyncErr error
	if df, err := os.Open(parent); err == nil {
		dirFsyncErr = df.Sync()
		_ = df.Close()
		if dirFsyncErr != nil {
			d.vlog("resume: parent dir fsync failed (%v); rename will proceed but is not crash-durable", dirFsyncErr)
		}
	}
	if err := os.Rename(tmp, d.resumePath); err != nil {
		return err
	}
	return dirFsyncErr
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
// Rejects symlinks to prevent attacker from deleting arbitrary files
// via a pre-placed symlink at the resume path.
func clearResume(path string) error {
	if path == "" {
		return nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to remove symlink at %s", path)
	}
	return os.Remove(path)
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
