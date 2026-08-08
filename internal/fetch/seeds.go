package fetch

import "sort"

// maxSeedTasks caps how many Task values a single splitRange call (and
// therefore the range-mode seed queue) may allocate. At the default 1 MiB
// chunk this covers 256 GiB; larger totals grow the chunk size so the
// count stays ≤ max. Without this, a hostile Content-Length of ~1 PiB
// would materialize ~1e9 Task structs and OOM the process — the previous
// "saneChunkCount" only capped make() capacity while the append loop
// still grew without bound.
const maxSeedTasks = 262144

// minSeedChunk is the preferred per-task size (1 MiB). Larger files may
// use bigger chunks via seedChunkSize / splitRange auto-growth.
const minSeedChunk int64 = 1 << 20

// seedChunkSize returns the chunk size to use when seeding range tasks
// for a file of the given total size. Always ≥ minSeedChunk, and large
// enough that ceil(total/chunk) ≤ maxSeedTasks.
func seedChunkSize(total int64) int64 {
	if total <= 0 {
		return minSeedChunk
	}
	// chunk = max(minSeedChunk, ceil(total/maxSeedTasks))
	// total and maxSeedTasks are positive and maxSeedTasks fits int64,
	// so total/maxSeedTasks cannot overflow int64.
	cs := total / maxSeedTasks
	if total%maxSeedTasks != 0 {
		cs++
	}
	if cs < minSeedChunk {
		return minSeedChunk
	}
	return cs
}

// splitRange splits [offset, offset+length) into tasks of at most
// chunkSize bytes each. If length/chunkSize would exceed maxSeedTasks,
// chunkSize is increased so the returned slice stays bounded (full
// coverage is preserved; individual tasks simply get larger).
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
	// n = ceil(length / chunkSize), saturating to avoid int64 overflow
	// panic in `make` for server-controlled values near MaxInt64.
	n := length / chunkSize
	if rem := length % chunkSize; rem != 0 {
		n++
	}
	if n > maxSeedTasks {
		// Grow chunkSize so ceil(length/chunkSize) ≤ maxSeedTasks while
		// still covering [offset, end). Defense in depth: callers should
		// already pass seedChunkSize(total), but uncompleted() gaps or
		// future call sites must not re-introduce unbounded growth.
		chunkSize = length / maxSeedTasks
		if length%maxSeedTasks != 0 {
			chunkSize++
		}
		if chunkSize <= 0 {
			// length was huge relative to maxSeedTasks but the division
			// somehow underflowed; fail closed with a single task.
			return []Task{{Start: offset, End: end - 1}}
		}
		n = length / chunkSize
		if rem := length % chunkSize; rem != 0 {
			n++
		}
		if n > maxSeedTasks {
			n = maxSeedTasks
		}
	} else if n < 0 {
		n = 0
	}
	out := make([]Task, 0, n)
	for cursor := offset; cursor < end; {
		// Hard stop: never emit more than maxSeedTasks even if arithmetic
		// is adversarial. The last task absorbs the remainder.
		if int64(len(out)) >= maxSeedTasks-1 && cursor < end {
			out = append(out, Task{Start: cursor, End: end - 1})
			break
		}
		stop := cursor + chunkSize
		if stop > end || stop < cursor { // overflow check
			stop = end
		}
		out = append(out, Task{Start: cursor, End: stop - 1})
		cursor = stop
	}
	return out
}

// addSat returns x+y and whether it overflowed int64. Caller guarantees y>=0
// (splitRange returns early when length<=0), so only positive overflow is
// possible; the saturating value is math.MaxInt64.
func addSat(x, y int64) (int64, bool) {
	if y > 0 && x > int64(^uint64(0)>>1)-y {
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
