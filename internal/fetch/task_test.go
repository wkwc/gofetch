package fetch

import "testing"

func TestTaskLen(t *testing.T) {
	tests := []struct {
		task Task
		want int64
	}{
		{Task{Start: 0, End: 0}, 1},
		{Task{Start: 0, End: 99}, 100},
		{Task{Start: 100, End: 199}, 100},
		{Task{Start: 0, End: 1048575}, 1048576},
	}
	for _, tt := range tests {
		if got := tt.task.Len(); got != tt.want {
			t.Errorf("Task{Start:%d,End:%d}.Len() = %d, want %d", tt.task.Start, tt.task.End, got, tt.want)
		}
	}
}

func TestQueue(t *testing.T) {
	q := &Queue{}
	if _, ok := q.Pop(); ok {
		t.Fatal("Pop on empty queue should return false")
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}

	q.PushMany([]Task{{0, 10}, {11, 20}, {21, 30}})
	if q.Len() != 3 {
		t.Fatalf("Len = %d, want 3", q.Len())
	}
	// FIFO
	for _, want := range []int64{0, 11, 21} {
		got, ok := q.Pop()
		if !ok {
			t.Fatalf("Pop returned false")
		}
		if got.Start != want {
			t.Errorf("popped Start = %d, want %d", got.Start, want)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("Pop on now-empty queue should return false")
	}
}
