package fetch

import "sync"

// Task is an inclusive byte range [Start, End].
type Task struct {
	Start int64
	End   int64
}

// Len returns the number of bytes in this range.
func (t Task) Len() int64 { return t.End - t.Start + 1 }

// Queue is a FIFO of Tasks backed by a ring buffer with capacity limit.
type Queue struct {
	mu     sync.Mutex
	buf    []Task
	head   int
	tail   int
	count  int
	maxCap int // maximum capacity (0 = unlimited)
}

// NewQueue returns a Queue with pre-allocated capacity for at least n tasks.
// If maxCap > 0, queue capacity is capped at maxCap; 0 means no limit.
// Prefer maxCap == 0 for download work queues so steal/retry never
// silently drop tasks or livelock when growth is capped.
func NewQueue(n, maxCap int) *Queue {
	size := 8
	for size < n {
		size *= 2
	}
	if maxCap > 0 && size > maxCap {
		size = maxCap
	}
	return &Queue{buf: make([]Task, size), maxCap: maxCap}
}

// Push enqueues a task. Grows unbounded when maxCap is 0; when capped
// and full, grows past maxCap rather than overwriting (maxCap is a
// soft target, never a silent-drop limit).
func (q *Queue) Push(t Task) {
	q.mu.Lock()
	if q.count >= len(q.buf) {
		q.growForced()
	}
	q.buf[q.tail] = t
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
	q.mu.Unlock()
}

// PushMany enqueues all tasks in order.
func (q *Queue) PushMany(tasks []Task) {
	if len(tasks) == 0 {
		return
	}
	q.mu.Lock()
	needed := q.count + len(tasks)
	for len(q.buf) < needed {
		q.growForced()
	}
	for _, t := range tasks {
		q.buf[q.tail] = t
		q.tail = (q.tail + 1) % len(q.buf)
		q.count++
	}
	q.mu.Unlock()
}

// Pop removes and returns the front task, or (zero, false) if empty.
func (q *Queue) Pop() (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return Task{}, false
	}
	t := q.buf[q.head]
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	return t, true
}

// growForced doubles capacity (or grows to count+1), ignoring maxCap.
// Used when we must accept a task rather than drop or livelock.
func (q *Queue) growForced() {
	n := len(q.buf) * 2
	if n < 8 {
		n = 8
	}
	if n <= q.count {
		n = q.count + 1
	}
	newBuf := make([]Task, n)
	if q.count > 0 {
		if q.head < q.tail {
			copy(newBuf, q.buf[q.head:q.tail])
		} else {
			c := copy(newBuf, q.buf[q.head:])
			copy(newBuf[c:], q.buf[:q.tail])
		}
	}
	q.buf = newBuf
	q.head = 0
	q.tail = q.count
}
