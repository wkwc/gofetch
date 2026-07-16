package fetch

import "sync"

// Task is an inclusive byte range [Start, End].
type Task struct {
	Start int64
	End   int64
}

// Len returns the number of bytes in this range.
func (t Task) Len() int64 { return t.End - t.Start + 1 }

// Queue is a FIFO of Tasks backed by a ring buffer.
type Queue struct {
	mu    sync.Mutex
	buf   []Task
	head  int
	tail  int
	count int
}

// Push enqueues a task.
func (q *Queue) Push(t Task) {
	q.mu.Lock()
	q.grow()
	q.buf[q.tail] = t
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
	q.mu.Unlock()
}

// PushMany enqueues all tasks in order.
func (q *Queue) PushMany(tasks []Task) {
	q.mu.Lock()
	for _, t := range tasks {
		q.grow()
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

// Len returns the number of pending tasks.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// grow ensures at least one free slot, doubling capacity if needed.
func (q *Queue) grow() {
	if q.count < len(q.buf) {
		return
	}
	n := len(q.buf) * 2
	if n < 8 {
		n = 8
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
