package fetch

import (
	"errors"
	"testing"
)

func TestWorkerStateError(t *testing.T) {
	ws := newWorkerState()

	if err, ok := ws.err(); ok {
		t.Fatalf("unexpected initial error: %v", err)
	}

	ws.setErr(errors.New("boom"))
	err, ok := ws.err()
	if !ok || err.Error() != "boom" {
		t.Fatalf("err = %v ok = %v, want boom/true", err, ok)
	}

	// Second setErr should not overwrite
	ws.setErr(errors.New("second"))
	err, _ = ws.err()
	if err.Error() != "boom" {
		t.Fatalf("err should not be overwritten: got %v", err)
	}

	// nil is a no-op
	ws2 := newWorkerState()
	ws2.setErr(nil)
	if _, ok := ws2.err(); ok {
		t.Fatal("nil error should not set anything")
	}
}

func TestReset(t *testing.T) {
	ws := newWorkerState()
	task := Task{100, 200}
	ws.reset(task)

	if got := ws.curTask.Load(); got == nil || *got != task {
		t.Fatalf("curTask = %v, want %v", got, task)
	}
	if got := ws.bytesDone.Load(); got != 0 {
		t.Errorf("bytesDone = %d, want 0", got)
	}
	if ws.stealFlag.Load() {
		t.Error("stealFlag should reset to false")
	}
	if ws.startedAt.Load() == 0 {
		t.Error("startedAt should be set")
	}
}
