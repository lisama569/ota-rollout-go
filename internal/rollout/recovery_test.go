package rollout

import (
	"errors"
	"testing"
)

// TestRestartResumeMidBatch 验证：
//  1. 进程重启后从持久化状态恢复，停在未完成批次；
//  2. 已成功设备不重复下发、不重复计数；
//  3. 崩溃前在途任务重启后重投，跨纪元的迟到旧上报判 STALE。
func TestRestartResumeMidBatch(t *testing.T) {
	ctx := t.Context()
	store := tFileStore(t)
	specs := []BatchSpec{
		{Index: 0, DeviceIDs: []string{"a0", "a1", "a2", "a3", "a4"}, InFlightQuota: 2},
		{Index: 1, DeviceIDs: []string{"b0", "b1", "b2", "b3", "b4"}, InFlightQuota: 5},
	}

	// 第一轮“进程”：下发 2 台、1 台成功、1 台仍在途。
	eng1 := startEngine(t, store, specs, testConfig())
	tasks, _ := eng1.Dispatch(ctx)
	if len(tasks) != 2 {
		t.Fatalf("initial dispatch want 2, got %d", len(tasks))
	}
	ids := make([]string, 0, 2)
	for id := range tasks {
		ids = append(ids, id)
	}
	doneID, inflightID := ids[0], ids[1]
	doneCmd := tasks[doneID]
	inflightCmd := tasks[inflightID]
	ackCmd(t, eng1, doneCmd)
	before := eng1.Snapshot()
	t.Logf("pre-restart: done=%s inflight=%s(attempt=%d epoch=%d) current=%d phase=%s",
		doneID, inflightID, inflightCmd.Attempt, inflightCmd.Epoch, before.Current, before.Phase)

	// 模拟进程重启：新建引擎从同一文件存储恢复。
	eng2, err := NewEngine(ctx, store, nil, testLogger(t))
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	st := eng2.Snapshot()
	if st.Phase != PhaseRolling || st.Current != 0 || st.Epoch != 1 {
		t.Fatalf("resume at wrong position: phase=%s current=%d epoch=%d",
			st.Phase, st.Current, st.Epoch)
	}
	if d := st.Devices[doneID]; d.State != DevSucceeded || !d.Counted {
		t.Fatalf("completed device not restored: %+v", d)
	}
	// 崩溃前在途的任务被退回排队：等待新 Epoch 下重新下发。
	if d := st.Devices[inflightID]; d.State != DevAwaiting {
		t.Fatalf("in-flight device should be requeued: %+v", d)
	}
	if st.Batches[0].Succeeded != 1 || st.Batches[0].Awaiting < 1 {
		t.Fatalf("counters not restored: %+v", st.Batches[0])
	}

	// 跨纪元的迟到旧上报（带旧 epoch=0）必须判 STALE，不能与重投命令混淆。
	stale := sendRaw(t, eng2, ackForCommand(inflightCmd, 1, OutcomeSuccess, "fw-2.0"))
	if stale.Accepted || stale.Reason != ReasonStale {
		t.Fatalf("pre-restart stale report must be rejected, got %+v", stale)
	}
	if eng2.Snapshot().Batches[0].Succeeded != 1 {
		t.Fatalf("stale report changed counters")
	}

	// 继续调度直到两批全部成功；已成功设备绝不再出现在下发集合。
	finishAll := func(eng *Engine) {
		for it := 0; it < 50; it++ {
			s := eng.Snapshot()
			if s.Phase.Terminal() {
				return
			}
			tasks, _ := eng.Dispatch(ctx)
			if len(tasks) == 0 {
				t.Fatalf("stalled at iter %d: %s", it, s.Phase)
			}
			for _, cmd := range tasks {
				if s2 := eng.Snapshot().Devices[cmd.DeviceID]; s2.State == DevSucceeded {
					t.Fatalf("already-succeeded device %s re-dispatched", cmd.DeviceID)
				}
				ackCmd(t, eng, cmd)
			}
		}
		t.Fatal("did not finish in 50 iters")
	}
	finishAll(eng2)
	st = eng2.Snapshot()
	if st.Phase != PhaseCompleted {
		t.Fatalf("want COMPLETED after resume, got %s", st.Phase)
	}
	if st.Batches[0].Succeeded != 5 || st.Batches[1].Succeeded != 5 {
		t.Fatalf("counters wrong: %+v", st.Batches)
	}

	// 再次重启：终态恢复后不再下发，重复上报依旧 STALE。
	eng3, err := NewEngine(ctx, store, nil, testLogger(t))
	if err != nil {
		t.Fatalf("recover terminal: %v", err)
	}
	if eng3.Snapshot().Phase != PhaseCompleted {
		t.Fatalf("terminal phase not restored: %s", eng3.Snapshot().Phase)
	}
	if more, _ := eng3.Dispatch(ctx); len(more) != 0 {
		t.Fatalf("dispatch after recovered completion: %v", more)
	}
	dup := ackForCommand(doneCmd, 1, OutcomeSuccess, "fw-2.0")
	res := sendRaw(t, eng3, dup)
	if res.Accepted || res.Reason != ReasonStale {
		t.Fatalf("duplicate after restart not stale: %+v", res)
	}
}

// TestRestartResumeDuringRollback 验证回滚进行中崩溃重启：
// 在途回滚任务退回排队，回滚继续排空，结论为 FAILED；已回滚设备不重复计数。
func TestRestartResumeDuringRollback(t *testing.T) {
	ctx := t.Context()
	store := tFileStore(t)
	ids := []string{"e0", "e1", "e2", "e3", "e4"}
	specs := []BatchSpec{{Index: 0, DeviceIDs: ids, InFlightQuota: 5}}
	eng := startEngine(t, store, specs, testConfig())
	tasks, _ := eng.Dispatch(ctx)
	// 3 成功 + 2 失败 => 失败率 40% > 20%，触发回滚。
	for _, id := range ids[:3] {
		sendCmd(t, eng, tasks[id], 1, OutcomeSuccess, "fw-2.0")
	}
	for _, id := range ids[3:] {
		sendCmd(t, eng, tasks[id], 1, OutcomeFailed, "fw-1.0")
	}
	if eng.Snapshot().Phase != PhaseRollingBack {
		t.Fatalf("want ROLLING_BACK, got %s", eng.Snapshot().Phase)
	}
	// 下发回滚任务但在回报前“崩溃”。
	rb, _ := eng.DispatchRollback(ctx)
	if len(rb) != 3 {
		t.Fatalf("want 3 rollback tasks, got %d", len(rb))
	}

	// 重启恢复：全部 3 个回滚任务退回排队（不丢失、不重复计数）。
	eng2, err := NewEngine(ctx, store, nil, testLogger(t))
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	st := eng2.Snapshot()
	if st.Phase != PhaseRollingBack || st.Epoch != 1 {
		t.Fatalf("want resume ROLLING_BACK at epoch=1, got %s epoch=%d", st.Phase, st.Epoch)
	}
	if st.Batches[0].RollbackPending != 3 || st.Batches[0].RollbackInFlight != 0 {
		t.Fatalf("rollback tasks not requeued: %+v", st.Batches[0])
	}
	if st.Batches[0].RolledBack != 0 {
		t.Fatalf("nothing confirmed rolled back yet, got %d", st.Batches[0].RolledBack)
	}

	// 重新下发全部回滚并完成。
	rest, _ := eng2.DispatchRollback(ctx)
	if len(rest) != 3 {
		t.Fatalf("want 3 re-dispatched rollback tasks, got %d", len(rest))
	}
	for _, cmd := range rest {
		sendCmd(t, eng2, cmd, 1, OutcomeRollbackDone, "fw-1.0")
	}
	if eng2.Snapshot().Phase != PhaseFailed {
		t.Fatalf("want FAILED after resumed rollback, got %s", eng2.Snapshot().Phase)
	}
	if got := eng2.Snapshot().Batches[0].RolledBack; got != 3 {
		t.Fatalf("rolled back=%d want 3", got)
	}
}

// TestBatchCursorNeverRegresses 验证批次位点单调：已推进到后批次后，
// 试图用旧状态覆盖持久化必须被明确拒绝，而不是从中间继续。
func TestBatchCursorNeverRegresses(t *testing.T) {
	ctx := t.Context()
	store := tFileStore(t)
	specs := []BatchSpec{
		{Index: 0, DeviceIDs: []string{"a0", "a1", "a2", "a3", "a4"}, InFlightQuota: 5},
		{Index: 1, DeviceIDs: []string{"b0", "b1", "b2", "b3", "b4"}, InFlightQuota: 5},
	}
	eng := startEngine(t, store, specs, testConfig())
	tasks, _ := eng.Dispatch(ctx)
	for _, cmd := range tasks {
		ackCmd(t, eng, cmd)
	}
	if eng.Snapshot().Current != 1 {
		t.Fatalf("want current=1, got %d", eng.Snapshot().Current)
	}

	old := mustState(t, testConfig(), specs)
	old.Current = 0
	old.Phase = PhaseRolling
	err := store.Save(ctx, old)
	var br *ErrBatchRegress
	if !errors.As(err, &br) {
		t.Fatalf("want ErrBatchRegress, got %v", err)
	}
	t.Logf("regression 1 -> 0 explicitly rejected: %v", br)

	eng2, err := NewEngine(ctx, store, nil, testLogger(t))
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if eng2.Snapshot().Current != 1 || eng2.Snapshot().Phase != PhaseRolling {
		t.Fatalf("committed progress altered: current=%d phase=%s",
			eng2.Snapshot().Current, eng2.Snapshot().Phase)
	}
}
