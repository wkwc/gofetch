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

	q.PushMany([]Task{{0, 10}, {11, 20}, {21, 30}})
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

// TestQueueFIFOOrder verifies the ring buffer preserves FIFO order when
// it fills, wraps, and grows (NewQueue(1) forces growth from the start).
func TestQueueFIFOOrder(t *testing.T) {
	for _, n := range []int{0, 1, 7, 8, 9, 100, 1000} {
		q := NewQueue(n, 0)
		for i := 0; i < n; i++ {
			q.Push(Task{Start: int64(i)})
		}
		if q.Len() != n {
			t.Fatalf("n=%d: Len=%d", n, q.Len())
		}
		for i := 0; i < n; i++ {
			got, ok := q.Pop()
			if !ok || got.Start != int64(i) {
				t.Fatalf("n=%d: pop %d = %+v ok=%v", n, i, got, ok)
			}
		}
		if _, ok := q.Pop(); ok {
			t.Fatalf("n=%d: pop on empty succeeded", n)
		}
	}
}

// TestQueueFIFOInterleaved mixes pushes and pops so the ring wraps and
// grows mid-stream; every pop must return the earliest not-yet-popped
// value, and the drain must be exact.
func TestQueueFIFOInterleaved(t *testing.T) {
	q := NewQueue(1, 0) // tiny start to force growth
	next, pushes := 0, 0
	for i := 0; i < 1000; i++ {
		if i%2 == 0 || i < 3 {
			q.Push(Task{Start: int64(pushes)})
			pushes++
		}
		if i%3 == 0 {
			if got, ok := q.Pop(); ok && got.Start != int64(next) {
				t.Fatalf("iter %d: pop %d, want %d", i, got.Start, next)
			} else if ok {
				next++
			}
		}
	}
	for {
		got, ok := q.Pop()
		if !ok {
			break
		}
		if got.Start != int64(next) {
			t.Fatalf("drain: pop %d, want %d", got.Start, next)
		}
		next++
	}
	if next != pushes {
		t.Fatalf("popped %d, pushed %d", next, pushes)
	}
}
