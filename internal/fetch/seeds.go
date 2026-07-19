package fetch

import "sort"

// splitRange splits [offset, offset+length) into tasks of at most
// chunkSize bytes each.
func splitRange(offset, length, chunkSize int64) []Task {
	if length <= 0 || chunkSize <= 0 {
		return nil
	}
	// Check for overflow before adding
	end, overflow := addSat(offset, length)
	if overflow {
		// Treat as "very large" file; cap at int64 max
		end = int64(^uint64(0) >> 1)
	}
	n := (length + chunkSize - 1) / chunkSize
	out := make([]Task, 0, n)
	for cursor := offset; cursor < end; {
		stop := cursor + chunkSize
		if stop > end || stop < cursor { // overflow check
			stop = end
		}
		out = append(out, Task{Start: cursor, End: stop - 1})
		cursor = stop
	}
	return out
}

// addSat returns x+y and whether it overflowed int64.
func addSat(x, y int64) (int64, bool) {
	if y > 0 && x > int64(^uint64(0)>>1)-y {
		return int64(^uint64(0) >> 1), true
	}
	if y < 0 && x < int64(^uint64(0)>>1)+y {
		return int64(^uint64(0) >> 1), true
	}
	return x + y, false
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
