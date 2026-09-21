package fetch

import (
	"context"
	"testing"
	"time"
)

// etaGateFixture stages one worker that is moving but slow (512 KiB
// done, +256 KiB/tick — under stealSlowBytes so stealPlan itself would
// call it a candidate) and runs monitor for ticks ticks. Grace is
// staged to block tick 1 only, so from tick 2 the ETA gate is what
// decides. Returns the queue and worker state for assertions.
func etaGateFixture(t *testing.T, total int64, ticks int) (*Queue, *workerState) {
	t.Helper()
	states := []*workerState{newWorkerState()}
	queue := NewQueue(0, 0)
	ws := states[0]
	ws.reset(Task{Start: 0, End: 4<<20 - 1})
	// 800 ms in the past: tick 1 (at +500 ms) is within the 1.5 s grace
	// (no steal), tick 2 (at +1 s) is past it (candidate).
	ws.startedAt.Store(time.Now().Add(-800 * time.Millisecond).UnixNano())
	ws.bytesDone.Store(512 << 10)
	cancelFn := context.CancelFunc(func() {})
	ws.cancelFn.Store(&cancelFn)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Duration(ticks) * monitorInterval)
			cancel()
		}()
		monitor(ctx, states, queue, total)
	}()
	// Advance progress 128 KiB/tick, phase-offset (sleep half a tick
	// first) so each add lands BETWEEN monitor reads: every tick then
	// sees exactly one fresh 128 KiB (rate ~256 KB/s, deterministic).
	// bytesDone stays under stealSlowBytes (1 MiB) through the steal
	// window (tick 2), so stealPlan itself still calls the worker a
	// candidate — the ETA gate is what decides. The worker is moving,
	// so once the gate suppresses stealing the queue drains on its own
	// instead of hanging.
	go func() {
		time.Sleep(monitorInterval / 2)
		for i := 0; i < ticks; i++ {
			ws.bytesDone.Add(128 << 10)
			time.Sleep(monitorInterval)
		}
	}()
	<-done
	time.Sleep(100 * time.Millisecond) // let the advancer finish
	return queue, ws
}

// TestMonitorEtaGateSuppressed pins the aggregate-ETA gate: a moving
// download with ETA under the gate must not steal (no churn), even
// though the worker is a steal candidate by stealPlan's own conditions.
func TestMonitorEtaGateSuppressed(t *testing.T) {
	// total 1.5 MiB, 512 KiB done at 128-512 KB/s → ETA 1-3 s < 5 s gate.
	queue, ws := etaGateFixture(t, 1536<<10, 5)
	if n := queue.Len(); n != 0 {
		t.Errorf("stolen tasks queued = %d, want 0 (ETA gate must suppress)", n)
	}
	if ws.cancelFn.Load() == nil {
		t.Error("cancelFn was consumed — a steal fired despite the ETA gate")
	}
}

// TestMonitorEtaGateAllowsSteal is the control: same slow-moving worker
// but a long ETA (total 100 MiB) — stealing must fire.
func TestMonitorEtaGateAllowsSteal(t *testing.T) {
	queue, ws := etaGateFixture(t, 100<<20, 5)
	if n := queue.Len(); n != 1 {
		t.Errorf("stolen tasks queued = %d, want 1 (long ETA must steal)", n)
	}
	if ws.cancelFn.Load() != nil {
		t.Error("cancelFn not consumed — steal did not fire")
	}
}
