package fetch

import "sort"

// splitRange splits [offset, offset+length) into tasks of at most
// chunkSize bytes each.
func splitRange(offset, length, chunkSize int64) []Task {
	if length <= 0 || chunkSize <= 0 {
		return nil
	}
	end := offset + length
	n := (length + chunkSize - 1) / chunkSize
	out := make([]Task, 0, n)
	for cursor := offset; cursor < end; {
		stop := cursor + chunkSize
		if stop > end {
			stop = end
		}
		out = append(out, Task{Start: cursor, End: stop - 1})
		cursor = stop
	}
	return out
}

// uncompleted returns the portions of `full` not covered by `completed`.
// `completed` may be in any order; it is sorted internally. Malformed
// entries (Start<0, Start>End, or outside `full`) are skipped.
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
		if c.Start < 0 || c.End < c.Start || c.End < full.Start || c.Start > full.End {
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
