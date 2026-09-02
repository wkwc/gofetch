package fetch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
// states are the live worker states of the running range download (nil for
// single-stream and for post-run saves) used to capture the in-progress task.
func (d *Downloader) saveResume(url string, completed []Task, states []*workerState) error {
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
	for _, ws := range states {
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
		URL: url, OutFile: d.outFile, TotalSize: d.totalSize,
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
	return atomicWriteFile(d.resumePath, data)
}

// atomicWriteFile writes data to path atomically (tmp + fsync + rename)
// and fsyncs the parent directory for crash durability.
//
// The tmp is cleared first so O_EXCL cannot permanently disable writes
// after a prior crash orphaned a .tmp. Parent fsync: a renamed file's
// metadata is only durable if the directory containing the rename's new
// link is fsynced (see rename(2) NOTES). filepath.Dir never returns ""
// (returns "." for relative paths). A failed parent fsync is returned so
// callers don't treat the save as durable-succeeded, but the rename is
// still attempted: a possibly-durable file is strictly better than a
// guaranteed orphan .tmp. Parent open failures are quiet because some
// environments (sandboxed CI, read-only root) cannot fsync the parent
// but can still rename within it.
func atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
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
	var dirFsyncErr error
	if df, err := os.Open(filepath.Dir(path)); err == nil {
		dirFsyncErr = df.Sync()
		_ = df.Close()
	}
	if err := os.Rename(tmp, path); err != nil {
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
	sorted := sortedByStart(tasks)
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

// maybeSaveResume writes the resume state if at least 1 second has
// passed since the last save. Cheap throttle to avoid I/O storms.
// No-op when resume is disabled.
//
// force=true bypasses the throttle: the caller (cancel path, run-end)
// must always emit a final sidecar otherwise the last recordCompleted
// of the run is silently dropped if it landed within the throttle
// window — next run would re-fetch the bytes the loop already wrote.
func (d *Downloader) maybeSaveResume(force bool, url string, states []*workerState) {
	if d.resumePath == "" {
		return
	}
	if !force {
		// Monotonic time.Time (not raw UnixNano): a wall-clock jump cannot
		// turn a just-saved state into "long ago" or vice versa.
		d.lastResumeSaveMu.Lock()
		should := d.lastResumeSave.IsZero() || time.Since(d.lastResumeSave) >= time.Second
		d.lastResumeSaveMu.Unlock()
		if !should {
			return
		}
	}
	if d.saveResume(url, d.snapshotCompleted(), states) == nil {
		d.lastResumeSaveMu.Lock()
		d.lastResumeSave = time.Now()
		d.lastResumeSaveMu.Unlock()
	}
}

// recordCompleted appends a fully finished range to the durable
// accumulator used by resume sidecars. Seeded from loadResume at
// start so prior progress is never dropped when workers reset.
//
// Deliberately does not dedup on every call: hot path is one append
// per completed chunk. saveResume and seedCompleted merge ranges.
func (d *Downloader) recordCompleted(t Task) {
	if d.resumePath == "" {
		return
	}
	d.completedMu.Lock()
	d.completed = append(d.completed, t)
	d.completedMu.Unlock()
}

// seedCompleted replaces the accumulator (used after loading a resume file).
func (d *Downloader) seedCompleted(tasks []Task) {
	d.completedMu.Lock()
	d.completed = dedupTasks(append([]Task(nil), tasks...))
	d.completedMu.Unlock()
}

// dropCompletedOverlapping surgically trims the resume accumulator so
// that the byte ranges corresponding to corrupt manifest chunks are no
// longer claimed as completed — the next run re-fetches just those
// spans instead of the entire file. Because `completed` is stored
// merged (dedupTasks unions adjacent/overlapping ranges into one big
// Task), a naive "drop any task that overlaps a bad chunk" would
// discard the whole merged range and be no better than clearing. We
// instead SUBTRACT the union of bad byte ranges from each completed
// task, keeping the surviving fragments.
//
// bad chunks are [c.Start, c.End] inclusive; converted to Task
// internally. Returns the trimmed accumulator via d.completed.
func (d *Downloader) dropCompletedOverlapping(bad []ChunkHash) {
	if len(bad) == 0 {
		return
	}
	// Build sorted + merged bad byte ranges.
	badRanges := make([]Task, 0, len(bad))
	for _, c := range bad {
		badRanges = append(badRanges, Task{Start: c.Start, End: c.End})
	}
	badRanges = dedupTasks(badRanges) // sorts + merges, O(n log n)

	d.completedMu.Lock()
	kept := d.completed[:0]
	for _, t := range d.completed {
		// Subtract every bad range that intersects [t.Start, t.End];
		// badRanges is sorted by Start so we can stop early.
		cursor := t.Start
		droppedAny := false
		for _, b := range badRanges {
			if b.End < cursor {
				continue // bad range entirely before our cursor
			}
			if b.Start > t.End {
				break // no further bad range touches this task
			}
			// b intersects [cursor, t.End] (and b.Start >= cursor, b.End <= t.End region)
			if b.Start > cursor {
				kept = append(kept, Task{Start: cursor, End: b.Start - 1})
			}
			cursor = b.End + 1
			droppedAny = true
			if cursor > t.End {
				break
			}
		}
		if cursor <= t.End {
			kept = append(kept, Task{Start: cursor, End: t.End})
		} else if !droppedAny && cursor == t.Start {
			// Defensive: cursor never advanced AND nothing dropped → t untouched.
			kept = append(kept, t)
		}
	}
	d.completed = dedupTasks(kept)
	d.completedMu.Unlock()
}

// snapshotCompleted returns a copy of accumulated completed ranges.
// Compacts the accumulator (merge overlapping/adjacent) so repeated
// resume saves do not retain one entry per historical chunk forever.
func (d *Downloader) snapshotCompleted() []Task {
	d.completedMu.Lock()
	defer d.completedMu.Unlock()
	d.completed = dedupTasks(d.completed)
	out := make([]Task, len(d.completed))
	copy(out, d.completed)
	return out
}
