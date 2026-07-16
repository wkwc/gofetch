package fetch

import (
	"math/rand/v2"
	"testing"
)

func TestSplitRange(t *testing.T) {
	check := func(got []Task, want []Task) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d (%v vs %v)", len(got), len(want), got, want)
		}
		for i, w := range want {
			if got[i] != w {
				t.Errorf("[%d] = %v, want %v", i, got[i], w)
			}
		}
	}

	got := splitRange(0, 100, 25)
	check(got, []Task{
		{0, 24}, {25, 49}, {50, 74}, {75, 99},
	})

	got = splitRange(0, 10, 2)
	check(got, []Task{
		{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9},
	})

	got = splitRange(5, 1, 1024)
	check(got, []Task{{5, 5}})

	got = splitRange(0, 0, 1024)
	if len(got) != 0 {
		t.Fatalf("zero-length = %v, want empty", got)
	}

	got = splitRange(100, 50, 25)
	check(got, []Task{{100, 124}, {125, 149}})
}

func TestUncompleted(t *testing.T) {
	tests := []struct {
		name      string
		full      Task
		completed []Task
		want      []Task
	}{
		{
			name:      "no completed",
			full:      Task{0, 99},
			completed: nil,
			want:      []Task{{0, 99}},
		},
		{
			name:      "fully completed",
			full:      Task{0, 99},
			completed: []Task{{0, 99}},
			want:      nil,
		},
		{
			name:      "beginning completed",
			full:      Task{0, 99},
			completed: []Task{{0, 49}},
			want:      []Task{{50, 99}},
		},
		{
			name:      "middle completed",
			full:      Task{0, 99},
			completed: []Task{{30, 69}},
			want:      []Task{{0, 29}, {70, 99}},
		},
		{
			name:      "outside range",
			full:      Task{100, 200},
			completed: []Task{{0, 50}, {300, 400}},
			want:      []Task{{100, 200}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := uncompleted(tt.full, tt.completed)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d (%v vs %v)", len(got), len(tt.want), got, tt.want)
			}
			for i, w := range tt.want {
				if got[i] != w {
					t.Errorf("[%d] = %v, want %v", i, got[i], w)
				}
			}
		})
	}
}

func TestUncompletedOverlapping(t *testing.T) {
	// Multiple overlapping completed ranges should be normalized
	completed := []Task{
		{2000, 2999},
		{0, 999},
		{1000, 1999}, // unsorted
	}
	got := uncompleted(Task{0, 3999}, completed)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1 (%v)", len(got), got)
	}
	if got[0] != (Task{3000, 3999}) {
		t.Errorf("got [0] = %v, want 3000-3999", got[0])
	}
}

// Property-based tests for splitRange and uncompleted

func TestSplitRangeProperties(t *testing.T) {
	const iterations = 200 // reduced for race test speed
	for i := 0; i < iterations; i++ {
		offset := int64(rand.N(1 << 30))
		length := int64(rand.N(1 << 20))
		chunkSize := int64(rand.N(1<<17) + 1) // 1 to 128 KiB

		tasks := splitRange(offset, length, chunkSize)

		// Property 1: Empty length => empty result
		if length <= 0 {
			if len(tasks) != 0 {
				t.Errorf("iter %d: zero/neg length gave %d tasks", i, len(tasks))
			}
			continue
		}
		if chunkSize <= 0 {
			if len(tasks) != 0 {
				t.Errorf("iter %d: zero/neg chunkSize gave %d tasks", i, len(tasks))
			}
			continue
		}

		// Property 2: Tasks cover exactly the range with no gaps/overlaps
		var prevEnd int64
		for j, task := range tasks {
			if task.Start > task.End {
				t.Errorf("iter %d: task %d has Start > End (%d > %d)", i, j, task.Start, task.End)
			}
			if j == 0 {
				if task.Start != offset {
					t.Errorf("iter %d: first task Start=%d, want %d", i, task.Start, offset)
				}
			} else {
				if task.Start != prevEnd+1 {
					t.Errorf("iter %d: gap at task %d (prevEnd=%d, task.Start=%d)", i, j, prevEnd, task.Start)
				}
			}
			prevEnd = task.End
		}
		if len(tasks) > 0 && prevEnd != offset+length-1 {
			t.Errorf("iter %d: last task End=%d, want %d", i, prevEnd, offset+length-1)
		}

		// Property 3: Each task size <= chunkSize (except possibly last)
		for j, task := range tasks {
			size := task.Len()
			if j < len(tasks)-1 && size != chunkSize {
				t.Errorf("iter %d: task %d size=%d, want %d (not last)", i, j, size, chunkSize)
			}
			if size > chunkSize {
				t.Errorf("iter %d: task %d size=%d exceeds chunkSize=%d", i, j, size, chunkSize)
			}
		}
	}
}

func TestUncompletedProperties(t *testing.T) {
	const iterations = 200 // reduced for race test speed
	for i := 0; i < iterations; i++ {
		full := Task{
			Start: int64(rand.N(1 << 28)),
			End:   int64(rand.N(1<<28) + (1 << 20)),
		}
		if full.Start > full.End {
			full.Start, full.End = full.End, full.Start
		}

		// Generate random completed ranges
		numCompleted := rand.N(20)
		completed := make([]Task, numCompleted)
		for j := 0; j < numCompleted; j++ {
			cs := int64(rand.N(1 << 28))
			ce := cs + int64(rand.N(1<<16))
			if ce > full.End+1000 {
				ce = full.End + 1000
			}
			completed[j] = Task{Start: cs, End: ce}
		}

		uncomp := uncompleted(full, completed)

		// Property: uncomp + completed (clipped to full) == full
		// We can't easily check set equality, but we can check:
		// 1. All uncomp tasks are within full
		for _, tsk := range uncomp {
			if tsk.Start < full.Start || tsk.End > full.End {
				t.Errorf("iter %d: uncomp task %v outside full %v", i, tsk, full)
			}
		}

		// 2. No overlaps in uncomp
		for j := 1; j < len(uncomp); j++ {
			if uncomp[j].Start <= uncomp[j-1].End {
				t.Errorf("iter %d: overlapping uncomp tasks at %d: %v and %v", i, j, uncomp[j-1], uncomp[j])
			}
		}

		// 3. If completed covers full, uncomp is empty
		allCovered := false
		if len(completed) > 0 {
			// Check if union of completed (clipped to full) covers full
			covered := make([]Task, len(completed))
			for k, c := range completed {
				cs := c.Start
				if cs < full.Start {
					cs = full.Start
				}
				ce := c.End
				if ce > full.End {
					ce = full.End
				}
				if cs <= ce {
					covered[k] = Task{Start: cs, End: ce}
				}
			}
			// Merge and check
			if len(covered) > 0 {
				// Sort
				for a := 0; a < len(covered)-1; a++ {
					for b := a + 1; b < len(covered); b++ {
						if covered[b].Start < covered[a].Start {
							covered[a], covered[b] = covered[b], covered[a]
						}
					}
				}
				// Merge
				merged := []Task{covered[0]}
				for _, c := range covered[1:] {
					last := &merged[len(merged)-1]
					if c.Start <= last.End+1 {
						if c.End > last.End {
							last.End = c.End
						}
					} else {
						merged = append(merged, c)
					}
				}
				if len(merged) == 1 && merged[0].Start <= full.Start && merged[0].End >= full.End {
					allCovered = true
				}
			}
		}
		if allCovered && len(uncomp) != 0 {
			t.Errorf("iter %d: full covered but uncomp not empty: %v", i, uncomp)
		}
	}
}

// func TestSplitRangeUncompletedRoundtrip(t *testing.T) {
// 	const iterations = 100 // reduced for race test speed
// 	for i := 0; i < iterations; i++ {
// 		full := Task{
// 			Start: int64(rand.N(1 << 28)),
// 			End:   int64(rand.N(1<<28) + (1 << 20)),
// 		}
// 		if full.Start > full.End {
// 			full.Start, full.End = full.End, full.Start
// 		}
//
// 		chunkSize := int64(rand.N(1<<17) + 1)
// 		tasks := splitRange(full.Start, full.Len(), chunkSize)
//
// 		// Pick random subset as "completed"
// 		var completed []Task
// 		for _, task := range tasks {
// 			if rand.N(2) == 0 {
// 				completed = append(completed, task)
// 			}
// 		}
//
// 		uncomp := uncompleted(full, completed)
//
// 		// Verify: completed + uncomp should cover full exactly
// 		all := append(append([]Task{}, completed...), uncomp...)
// 		if len(all) == 0 {
// 			continue
// 		}
//
// 		// Sort and merge
// 		for a := 0; a < len(all)-1; a++ {
// 			for b := a + 1; b < len(all); b++ {
// 				if all[b].Start < all[a].Start {
// 					all[a], all[b] = all[b], all[a]
// 				}
// 			}
// 		}
// 		merged := []Task{all[0]}
// 		for _, tsk := range all[1:] {
// 			last := &merged[len(merged)-1]
// 			if tsk.Start <= last.End+1 {
// 				if tsk.End > last.End {
// 					last.End = tsk.End
// 				}
// 			} else {
// 				merged = append(merged, tsk)
// 			}
// 		}
//
// 		if len(merged) != 1 || merged[0].Start != full.Start || merged[0].End != full.End {
// 			t.Errorf("iter %d: roundtrip failed: full=%v, got=%v", i, full, merged)
// 		}
// 	}
// }
