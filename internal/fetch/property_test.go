package fetch

import (
	"math/rand/v2"
	"testing"
)

// These differential tests check the range algebra against a brute-force
// oracle on thousands of randomized inputs. The existing fuzz targets
// prove the functions never panic and stay structurally sane; these prove
// they compute the *right answer*.

// bruteUncompleted computes the gaps of full not covered by any valid
// completed range, by marking covered bytes and scanning. Matches the
// malformed-entry filter of uncompleted.
func bruteUncompleted(full Task, completed []Task) []Task {
	covered := make(map[int64]bool)
	for _, c := range completed {
		if c.Start < 0 || c.End < c.Start || c.End < full.Start || c.Start > full.End {
			continue
		}
		for i := c.Start; i <= c.End && i <= full.End; i++ {
			covered[i] = true
		}
	}
	var gaps []Task
	start := int64(-1)
	for i := full.Start; i <= full.End; i++ {
		if !covered[i] && start == -1 {
			start = i
		} else if covered[i] && start != -1 {
			gaps = append(gaps, Task{Start: start, End: i - 1})
			start = -1
		}
	}
	if start != -1 {
		gaps = append(gaps, Task{Start: start, End: full.End})
	}
	return gaps
}

func TestUncompletedDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for iter := 0; iter < 5000; iter++ {
		full := Task{Start: int64(rng.IntN(12)), End: int64(12 + rng.IntN(20))}
		n := rng.IntN(4)
		comp := make([]Task, 0, n)
		for i := 0; i < n; i++ {
			comp = append(comp, Task{Start: int64(rng.IntN(40)), End: int64(rng.IntN(40))})
		}
		got := uncompleted(full, comp)
		want := bruteUncompleted(full, comp)
		if !tasksEqual(got, want) {
			t.Fatalf("uncompleted(%+v, %v) = %v, want %v", full, comp, got, want)
		}
	}
}

// bruteDedup merges tasks by marking covered bytes then emitting
// contiguous runs. Adjacent runs (gap of 1) are merged, matching
// dedupTasks (t.Start <= last.End+1). Bounds the universe at the
// maximum End in the input (plus 1 for the closing run).
func bruteDedup(tasks []Task) []Task {
	maxV := int64(-1)
	for _, task := range tasks {
		if task.End > maxV {
			maxV = task.End
		}
	}
	covered := make(map[int64]bool)
	for _, task := range tasks {
		if task.End < task.Start {
			continue
		}
		for i := task.Start; i <= task.End; i++ {
			covered[i] = true
		}
	}
	var out []Task
	start := int64(-1)
	for i := int64(0); i <= maxV; i++ {
		if covered[i] && start == -1 {
			start = i
		} else if !covered[i] && start != -1 {
			out = append(out, Task{Start: start, End: i - 1})
			start = -1
		}
	}
	if start != -1 {
		out = append(out, Task{Start: start, End: maxV})
	}
	return out
}

func TestDedupTasksDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for iter := 0; iter < 5000; iter++ {
		n := rng.IntN(4)
		var tasks []Task
		for i := 0; i < n; i++ {
			a, b := rng.IntN(130), rng.IntN(130)
			if a > b {
				a, b = b, a // well-formed only
			}
			tasks = append(tasks, Task{Start: int64(a), End: int64(b)})
		}
		got := dedupTasks(tasks)
		want := bruteDedup(tasks)
		if !tasksEqual(got, want) {
			t.Fatalf("dedupTasks(%v) = %v, want %v", tasks, got, want)
		}
	}
}

// TestSplitRangeCoverageDifferential verifies splitRange's output exactly
// tiles [offset, offset+length): no gaps, no overlap, total == length.
func TestSplitRangeCoverageDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	for iter := 0; iter < 5000; iter++ {
		offset := int64(rng.IntN(1 << 20))
		length := int64(rng.IntN(1 << 22))
		chunk := int64(1 + rng.IntN(1<<20))
		tasks := splitRange(offset, length, chunk)
		if length <= 0 || chunk <= 0 {
			if len(tasks) != 0 {
				t.Fatalf("splitRange(%d,%d,%d) = %v, want empty", offset, length, chunk, tasks)
			}
			continue
		}
		var total int64
		prev := offset - 1
		for _, task := range tasks {
			if task.Start != prev+1 {
				t.Fatalf("gap/overlap: %v then %v (offset=%d len=%d chunk=%d)",
					Task{Start: prev, End: task.Start - 1}, task, offset, length, chunk)
			}
			if task.End < task.Start || task.Start < offset {
				t.Fatalf("task %+v out of range [%d,%d)", task, offset, offset+length)
			}
			total += task.Len()
			prev = task.End
		}
		if len(tasks) > 0 && total != length {
			t.Fatalf("splitRange coverage = %d, want %d (offset=%d len=%d chunk=%d)",
				total, length, offset, length, chunk)
		}
	}
}

func tasksEqual(a, b []Task) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
