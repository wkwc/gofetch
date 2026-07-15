package fetch

import "sort"

// seedTasks splits [offset..offset+length-1] into tasks of at most
// maxChunk bytes. With workersN=1 the only intent is to chunk by
// maxChunk, giving fine resume granularity per chunk.
// Smaller chunks = finer resume granularity after a crash/restart.
func seedTasks(offset, length int64, workersN int, maxChunk int64) []Task {
	if length <= 0 {
		return nil
	}
	if workersN < 1 {
		workersN = 1
	}
	chunk := length / int64(workersN)
	if chunk < 1 {
		chunk = 1
	}
	if maxChunk > 0 && chunk > maxChunk {
		chunk = maxChunk
	}
	if chunk < 1 {
		chunk = 1
	}
	end := offset + length
	var out []Task
	for cursor := offset; cursor < end; {
		stop := cursor + chunk
		if stop > end {
			stop = end
		}
		out = append(out, Task{Start: cursor, End: stop - 1})
		cursor = stop
	}
	return out
}

// uncompleted returns the portions of `full` not covered by `completed`.
// `completed` may be in any order; it is sorted internally.
func uncompleted(full Task, completed []Task) []Task {
	if len(completed) == 0 {
		return []Task{full}
	}
	sorted := make([]Task, len(completed))
	copy(sorted, completed)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })
	var out []Task
	cursor := full.Start
	for _, c := range sorted {
		if c.End < full.Start || c.Start > full.End {
			continue
		}
		if c.Start > cursor {
			out = append(out, Task{Start: cursor, End: c.Start - 1})
		}
		if c.End+1 > cursor {
			cursor = c.End + 1
		}
	}
	if cursor <= full.End {
		out = append(out, Task{Start: cursor, End: full.End})
	}
	return out
}
