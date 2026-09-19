package fetch

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
)

// TestSeedQueueCoversUncompleted is the coverage invariant behind
// seedQueue: drained tasks, merged, must equal exactly the uncompleted
// gaps of [0, total) — every missing byte seeded once, nothing else.
// Randomized completed sets include overlapping, adjacent, negative,
// and out-of-range entries.
func TestSeedQueueCoversUncompleted(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 7))
	for trial := 0; trial < 200; trial++ {
		total := int64(1+rng.IntN(40))*(1<<20) + int64(rng.IntN(1000))
		var completed []Task
		for i := 0; i < rng.IntN(8); i++ {
			s := int64(rng.IntN(int(total)+100)) - 50
			e := s + int64(rng.IntN(3<<20))
			completed = append(completed, Task{Start: s, End: e})
		}
		q := seedQueue(total, completed)
		var got []Task
		for {
			tk, ok := q.Pop()
			if !ok {
				break
			}
			got = append(got, tk)
		}
		want := uncompleted(Task{Start: 0, End: total - 1}, completed)
		merged := dedupTasks(got)
		if len(merged) != len(want) {
			t.Fatalf("trial %d: merged %d tasks, want %d gaps (total %d)", trial, len(merged), len(want), total)
		}
		for i := range want {
			if merged[i] != want[i] {
				t.Fatalf("trial %d: task %d = %+v, want %+v", trial, i, merged[i], want[i])
			}
		}
	}
}

// TestSeedQueueHostileTotal pins the OOM guard: a ~1 PiB Content-Length
// must still seed a bounded queue with full byte coverage (bigger
// chunks, not more tasks).
func TestSeedQueueHostileTotal(t *testing.T) {
	const total = int64(1) << 50
	q := seedQueue(total, nil)
	if n := q.Len(); n > maxSeedTasks {
		t.Fatalf("seeded %d tasks, want ≤ %d", n, maxSeedTasks)
	}
	var sum int64
	for {
		tk, ok := q.Pop()
		if !ok {
			break
		}
		sum += tk.Len()
	}
	if sum != total {
		t.Fatalf("seeded %d bytes, want %d", sum, total)
	}
}

// TestFirstWorkerError pins the steal/cancel classification shared by
// the completion check and the supervisor error report.
func TestFirstWorkerError(t *testing.T) {
	idle := newWorkerState()
	cancelled := newWorkerState()
	cancelled.setErr(context.Canceled)
	failed := newWorkerState()
	boom := errors.New("boom")
	failed.setErr(boom)

	if _, ok := firstWorkerError(nil, true); ok {
		t.Fatal("empty states must report no error")
	}
	if _, ok := firstWorkerError([]*workerState{idle, cancelled}, true); ok {
		t.Fatal("cancellations must be skipped with skipCancel=true")
	}
	if err, ok := firstWorkerError([]*workerState{idle, cancelled}, false); !ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("skipCancel=false must report cancellations, got %v/%v", err, ok)
	}
	if err, ok := firstWorkerError([]*workerState{cancelled, failed}, true); !ok || !errors.Is(err, boom) {
		t.Fatalf("must report the real failure, got %v/%v", err, ok)
	}
}

// TestAllTasksDone pins the completion predicate: drained queue plus no
// real worker error means done, even under a fired context; anything
// else means not done.
func TestAllTasksDone(t *testing.T) {
	ctx := context.Background()
	empty := NewQueue(0, 0)
	failed := newWorkerState()
	failed.setErr(errors.New("boom"))
	if !allTasksDone(ctx, []*workerState{newWorkerState()}, empty) {
		t.Fatal("empty queue + no errors must be done")
	}
	if allTasksDone(ctx, []*workerState{failed}, empty) {
		t.Fatal("worker failure must not be done even with empty queue")
	}
	full := NewQueue(1, 0)
	full.Push(Task{Start: 0, End: 9})
	// NOTE: live context + non-empty queue reports done — unreachable in
	// production (workers only exit on an empty pop or a recorded error,
	// both checked elsewhere), so the predicate doesn't spend branches
	// on it. Only the fired-context + queued-work case is meaningful:
	// a signal interrupted real work.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	// Fired context + drained queue + no errors: finished before the
	// signal, still done (late Ctrl-C on a complete file is success).
	if !allTasksDone(cancelled, []*workerState{newWorkerState()}, empty) {
		t.Fatal("fired context with drained queue must be done")
	}
	if allTasksDone(cancelled, []*workerState{newWorkerState()}, full) {
		t.Fatal("fired context with queued work must not be done")
	}
}
