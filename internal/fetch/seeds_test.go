package fetch

import "testing"

func TestSeedTasks(t *testing.T) {
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

	// 100 bytes, workersN=4, no maxChunk cap → 4 chunks
	got := seedTasks(0, 100, 4, 0)
	check(got, []Task{
		{0, 24}, {25, 49}, {50, 74}, {75, 99},
	})

	// 10 bytes split into 2-byte chunks (maxChunk)
	got = seedTasks(0, 10, 4, 2)
	check(got, []Task{
		{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9},
	})

	// single byte
	got = seedTasks(5, 1, 4, 1024)
	check(got, []Task{{5, 5}})

	// zero length
	got = seedTasks(0, 0, 4, 1024)
	if len(got) != 0 {
		t.Fatalf("zero-length = %v, want empty", got)
	}

	// offset > 0
	got = seedTasks(100, 50, 2, 1024)
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
