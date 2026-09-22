package rollout

import (
	"testing"
	"time"
)

// startEngine 构造两批设备（可指定每批大小与配额）并 Start。
func startEngine(t *testing.T, store Store, specs []BatchSpec, cfg Config) *Engine {
	t.Helper()
	eng := newTestEngine(t, store, mustState(t, cfg, specs))
	if err := eng.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	return eng
}

func ackCmd(t *testing.T, eng *Engine, cmd Command) AcceptResult {
	t.Helper()
	res, err := eng.Report(t.Context(), ackForCommand(cmd, 1, OutcomeSuccess, "fw-2.0"))
	if err != nil || !res.Accepted {
		t.Fatalf("success report device=%s: accepted=%v err=%v reason=%s",
			cmd.DeviceID, res.Accepted, err, res.Reason)
	}
	return res
}

func TestStagedRolloutHappyPath(t *testing.T) {
	ctx := t.Context()
	specs := []BatchSpec{
		{Index: 0, DeviceIDs: []string{"a0", "a1", "a2", "a3", "a4"}, InFlightQuota: 2},
		{Index: 1, DeviceIDs: []string{"b0", "b1", "b2", "b3", "b4"}, InFlightQuota: 5},
	}
	eng := startEngine(t, NewMemoryStore(), specs, testConfig())

	// 第一批配额 2：每次最多下发 2 台，槽位占满后继续 dispatch 必须为空。
	t1, _ := eng.Dispatch(ctx)
	if len(t1) != 2 {
		t.Fatalf("batch0 first dispatch want 2, got %d (%v)", len(t1), t1)
	}
	t.Logf("batch0 dispatch#1 tasks=%v", t1)
	if again, _ := eng.Dispatch(ctx); len(again) != 0 {
		t.Fatalf("quota full, second dispatch must be empty, got %v", again)
	}

	// 回报一台 -> 按空闲槽位补新设备，直到第一批 5 台全部成功；
	// 在途设备不得重复下发，未到 5 台全部终态不得开门。
	outstanding := map[string]Command{}
	for id, c := range t1 {
		outstanding[id] = c
	}
	finished := 0
	for finished < 5 {
		var cmd Command
		for id, c := range outstanding {
			cmd = c
			delete(outstanding, id)
			break
		}
		ackCmd(t, eng, cmd)
		finished++
		if finished < 5 && eng.Snapshot().Current != 0 {
			t.Fatalf("gate advanced early after %d acks", finished)
		}
		if finished == 5 {
			break
		}
		fill, _ := eng.Dispatch(ctx)
		for id, c := range fill {
			if _, inflight := outstanding[id]; inflight {
				t.Fatalf("in-flight device %s re-dispatched", id)
			}
			outstanding[id] = c
		}
		t.Logf("ack %s (finished=%d) -> refill %v", cmd.DeviceID, finished, fill)
		if len(fill) == 0 && len(outstanding) == 0 {
			t.Fatalf("no progress with %d devices unfinished", 5-finished)
		}
	}

	st := eng.Snapshot()
	if st.Current != 1 {
		t.Fatalf("want advance to batch1, current=%d", st.Current)
	}
	if st.Batches[0].Decision != GateOpen || !st.Batches[0].GateClosed {
		t.Fatalf("batch0 gate not open: %+v", st.Batches[0])
	}
	t.Logf("batch0 gate OPEN -> current=1")

	// 第二批一次性全部成功，发布单完成。
	tb, _ := eng.Dispatch(ctx)
	if len(tb) != 5 {
		t.Fatalf("batch1 dispatch want 5, got %d", len(tb))
	}
	for _, cmd := range tb {
		ackCmd(t, eng, cmd)
	}
	st = eng.Snapshot()
	if st.Phase != PhaseCompleted {
		t.Fatalf("want COMPLETED, got %s", st.Phase)
	}
	if st.Batches[1].Succeeded != 5 {
		t.Fatalf("batch1 succeeded=%d", st.Batches[1].Succeeded)
	}
	t.Logf("release COMPLETED, batch counters: %+v", st.Batches)

	if extra, _ := eng.Dispatch(ctx); len(extra) != 0 {
		t.Fatalf("dispatch after completion: %v", extra)
	}
}

func TestDispatchNeverExceedsQuota(t *testing.T) {
	ctx := t.Context()
	specs := []BatchSpec{{Index: 0, DeviceIDs: []string{"x0", "x1", "x2", "x3"}, InFlightQuota: 3}}
	eng := startEngine(t, NewMemoryStore(), specs, testConfig())
	tasks, _ := eng.Dispatch(ctx)
	if len(tasks) != 3 {
		t.Fatalf("quota=3 want 3, got %d", len(tasks))
	}
	if again, _ := eng.Dispatch(ctx); len(again) != 0 {
		t.Fatalf("must not over-dispatch quota: %v", again)
	}
	var first Command
	for _, c := range tasks {
		first = c
		break
	}
	ackCmd(t, eng, first)
	more, _ := eng.Dispatch(ctx)
	if len(more) != 1 {
		t.Fatalf("after one slot freed want 1 dispatch, got %v", more)
	}
	t.Logf("quota respected: 3 -> 0 -> 1 after ack")
}

func TestPauseStopsDispatch(t *testing.T) {
	ctx := t.Context()
	specs := []BatchSpec{{Index: 0, DeviceIDs: []string{"p0", "p1"}, InFlightQuota: 1}}
	eng := startEngine(t, NewMemoryStore(), specs, testConfig())
	first, _ := eng.Dispatch(ctx)
	if err := eng.Pause(ctx); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if tasks, _ := eng.Dispatch(ctx); len(tasks) != 0 {
		t.Fatalf("dispatch while paused: %v", tasks)
	}
	for _, cmd := range first {
		ackCmd(t, eng, cmd) // 暂停期间上报仍受理
	}
	if err := eng.Resume(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	rest, _ := eng.Dispatch(ctx)
	if len(rest) != 1 {
		t.Fatalf("after resume want remaining dispatch, got %v", rest)
	}
	t.Logf("pause/resume honored: paused=0 tasks, resumed=%v", rest)
}

func TestTickRequeuesTimedOutTasks(t *testing.T) {
	ctx := t.Context()
	specs := []BatchSpec{{Index: 0, DeviceIDs: []string{"t0", "t1"}, InFlightQuota: 2}}
	eng := startEngine(t, NewMemoryStore(), specs, testConfig())
	tasks, _ := eng.Dispatch(ctx)
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(tasks))
	}
	// 未超时：不重排队。
	now := timeNow()
	if n, err := eng.Tick(ctx, now.Add(5*time.Second), 10*time.Second); err != nil || n != 0 {
		t.Fatalf("early tick want 0, got n=%d err=%v", n, err)
	}
	// 超时后两台都退回 AWAITING，且不占用在途配额、不计失败。
	n, err := eng.Tick(ctx, now.Add(11*time.Second), 10*time.Second)
	if err != nil || n != 2 {
		t.Fatalf("timeout tick want 2, got n=%d err=%v", n, err)
	}
	snap := eng.Snapshot()
	if snap.Batches[0].InFlight != 0 || snap.Batches[0].Awaiting != 2 || snap.Batches[0].Failed != 0 {
		t.Fatalf("unexpected counters after timeout: %+v", snap.Batches[0])
	}
	// 重投使用新代次，Tries 累加。
	again, _ := eng.Dispatch(ctx)
	if len(again) != 2 {
		t.Fatalf("want 2 redispatched, got %d", len(again))
	}
	for _, c := range again {
		if c.Attempt != 2 {
			t.Fatalf("redispatch attempt want 2, got %d", c.Attempt)
		}
	}
	t.Logf("timeout requeue + redispatch OK: %v", again)
}
