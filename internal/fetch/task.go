package fetch

import "sync"

// Task is an inclusive byte range [Start, End].
type Task struct {
	Start int64
	End   int64
}

// Len returns the number of bytes in this range.
func (t Task) Len() int64 { return t.End - t.Start + 1 }

// Queue is a FIFO of Tasks. Zero value is ready.
type Queue struct {
	mu    sync.Mutex
	tasks []Task
}

// Push enqueues a task.
func (q *Queue) Push(t Task) {
	q.mu.Lock()
	q.tasks = append(q.tasks, t)
	q.mu.Unlock()
}

// PushMany enqueues all tasks in order.
func (q *Queue) PushMany(tasks []Task) {
	q.mu.Lock()
	q.tasks = append(q.tasks, tasks...)
	q.mu.Unlock()
}

// Pop removes and returns the front task, or (zero, false) if empty.
func (q *Queue) Pop() (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return Task{}, false
	}
	t := q.tasks[0]
	copy(q.tasks, q.tasks[1:])
	q.tasks = q.tasks[:len(q.tasks)-1]
	return t, true
}

// Len returns the number of pending tasks.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}
