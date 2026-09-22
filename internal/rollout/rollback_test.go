package rollout

import (
	"testing"
)

// sendCmd 以命令携带的 attempt/epoch 构造上报并驱动引擎，打印关键输入与判定。
func sendCmd(t *testing.T, eng *Engine, cmd Command, proto int, outcome ReportOutcome, firmware string) AcceptResult {
	t.Helper()
	r := ackForCommand(cmd, proto, outcome, firmware)
	return sendReport(t, eng, r)
}

// sendRaw 直接提交一条手工构造的上报（用于乱序/畸形/跨纪元场景）。
func sendRaw(t *testing.T, eng *Engine, r DeviceReport) AcceptResult {
	t.Helper()
	return sendReport(t, eng, r)
}

func sendReport(t *testing.T, eng *Engine, r DeviceReport) AcceptResult {
	t.Helper()
	res, err := eng.Report(t.Context(), r)
	if err != nil {
		t.Fatalf("report %s: %v", r.DeviceID, err)
	}
	t.Logf("in: device=%s batch=%d attempt=%d epoch=%d proto=%d outcome=%-13s fw=%s => accepted=%v state=%s decision=%s %s",
		r.DeviceID, r.BatchIndex, r.Attempt, r.Epoch, r.ProtoVersion, r.Outcome, r.Firmware,
		res.Accepted, res.State, res.Decision, res.Detail)
	return res
}

func TestFailureRateTriggersRollbackOfUpgradedDevices(t *testing.T) {
	ctx := t.Context()
	// 10 台一批：3 台成功换装后，连续永久失败使失败率超过 20%，必须回滚已成功设备。
	ids := []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"}
	specs := []BatchSpec{{Index: 0, DeviceIDs: ids, InFlightQuota: 10}}
	eng := startEngine(t, NewMemoryStore(), specs, testConfig())
	tasks, _ := eng.Dispatch(ctx)
	if len(tasks) != 10 {
		t.Fatalf("want 10 tasks, got %d", len(tasks))
	}

	for _, id := range ids[:3] {
		sendCmd(t, eng, tasks[id], 1, OutcomeSuccess, "fw-2.0")
	}
	if eng.Snapshot().Phase != PhaseRolling {
		t.Fatalf("phase=%s, want still ROLLING", eng.Snapshot().Phase)
	}
	// 2 台永久失败 => 20% 恰好在阈值边界（严格大于才触发），仍 PENDING。
	for _, id := range ids[3:5] {
		sendCmd(t, eng, tasks[id], 1, OutcomeFailed, "fw-1.0")
	}
	if eng.Snapshot().Phase != PhaseRolling {
		t.Fatalf("phase=%s at boundary, want ROLLING", eng.Snapshot().Phase)
	}
	// 第 3 台永久失败 => 30% > 20%，触发回滚：3 台成功设备进入回滚排队。
	sendCmd(t, eng, tasks[ids[5]], 1, OutcomeFailed, "fw-1.0")
	st := eng.Snapshot()
	if st.Phase != PhaseRollingBack {
		t.Fatalf("want ROLLING_BACK, got %s", st.Phase)
	}
	if st.Batches[0].RollbackPending != 3 {
		t.Fatalf("want 3 rollback pending, got %d", st.Batches[0].RollbackPending)
	}
	// d6..d9 仍在升级在途（迟到上报未决），此时还没有 SKIPPED。
	if st.Batches[0].InFlight != 4 || st.Batches[0].Skipped != 0 {
		t.Fatalf("want 4 still in-flight, got in_flight=%d skipped=%d",
			st.Batches[0].InFlight, st.Batches[0].Skipped)
	}
	t.Logf("rollback triggered: pending=%d in_flight_pending_reports=%d",
		st.Batches[0].RollbackPending, st.Batches[0].InFlight)

	// 迟到的失败上报在 ROLLING_BACK 阶段继续受理：失败设备未换装 -> SKIPPED。
	for _, id := range ids[6:] {
		sendCmd(t, eng, tasks[id], 1, OutcomeFailed, "fw-1.0")
	}
	if got := eng.Snapshot().Batches[0].Skipped; got != 4 {
		t.Fatalf("want 4 late-failed devices skipped, got %d", got)
	}

	// 回滚任务下发（受同一配额约束），设备回报 ROLLBACK_DONE。
	rb, _ := eng.DispatchRollback(ctx)
	if len(rb) != 3 {
		t.Fatalf("want 3 rollback tasks, got %d", len(rb))
	}
	for _, cmd := range rb {
		res := sendCmd(t, eng, cmd, 1, OutcomeRollbackDone, "fw-1.0")
		if !res.Accepted {
			t.Fatalf("rollback done rejected: %s", res.Reason)
		}
	}
	st = eng.Snapshot()
	if st.Phase != PhaseFailed {
		t.Fatalf("want FAILED, got %s", st.Phase)
	}
	if st.Batches[0].RolledBack != 3 {
		t.Fatalf("want 3 rolled back, got %d", st.Batches[0].RolledBack)
	}
	t.Logf("release FAILED after rollback, rolled_back=%d failed=%d",
		st.Batches[0].RolledBack, st.Batches[0].Failed)
}

func TestSelfRevertTriggersRollback(t *testing.T) {
	ctx := t.Context()
	ids := []string{"s0", "s1", "s2", "s3", "s4"}
	cfg := testConfig()
	cfg.MaxFailureRate = 1
	cfg.MaxRollbackRate = 0.2
	specs := []BatchSpec{{Index: 0, DeviceIDs: ids, InFlightQuota: 5}}
	eng := startEngine(t, NewMemoryStore(), specs, cfg)
	tasks, _ := eng.Dispatch(ctx)

	for _, id := range ids[:3] {
		sendCmd(t, eng, tasks[id], 1, OutcomeSuccess, "fw-2.0")
	}
	sendCmd(t, eng, tasks[ids[3]], 1, OutcomeRollbackSelf, "fw-2.0")
	if eng.Snapshot().Phase != PhaseRolling {
		t.Fatalf("1/5 self-revert should be within threshold, phase=%s", eng.Snapshot().Phase)
	}
	sendCmd(t, eng, tasks[ids[4]], 1, OutcomeRollbackSelf, "fw-2.0")
	st := eng.Snapshot()
	if st.Phase != PhaseRollingBack {
		t.Fatalf("want ROLLING_BACK, got %s", st.Phase)
	}
	// 自回退设备已在旧版本：计入 RolledBack/SelfReverted，但不再排队回滚。
	if st.Batches[0].RollbackPending != 3 || st.Batches[0].SelfReverted != 2 {
		t.Fatalf("unexpected counters: %+v", st.Batches[0])
	}
	rb, _ := eng.DispatchRollback(ctx)
	if len(rb) != 3 {
		t.Fatalf("want 3 rollback tasks for succeeded devices, got %d", len(rb))
	}
	for _, cmd := range rb {
		sendCmd(t, eng, cmd, 1, OutcomeRollbackDone, "fw-1.0")
	}
	if eng.Snapshot().Phase != PhaseFailed {
		t.Fatalf("want terminal FAILED, got %s", eng.Snapshot().Phase)
	}
}

func TestRetriableFailuresRetryThenExhaust(t *testing.T) {
	ctx := t.Context()
	ids := []string{"r0", "r1", "r2", "r3", "r4"}
	specs := []BatchSpec{{Index: 0, DeviceIDs: ids, InFlightQuota: 5}}
	eng := startEngine(t, NewMemoryStore(), specs, testConfig())

	first, _ := eng.Dispatch(ctx)
	c0 := first["r0"]
	if c0.Attempt != 1 {
		t.Fatalf("first attempt want 1, got %d", c0.Attempt)
	}
	sendCmd(t, eng, c0, 1, OutcomeRetriable, "fw-1.0")
	if got := eng.Snapshot().Devices["r0"].State; got != DevAwaiting {
		t.Fatalf("r0 after first retry want AWAITING, got %s", got)
	}
	again, _ := eng.Dispatch(ctx)
	c1 := again["r0"]
	if c1.Attempt != 2 {
		t.Fatalf("redispatch attempt want 2, got %d", c1.Attempt)
	}
	sendCmd(t, eng, c1, 1, OutcomeRetriable, "fw-1.0")
	again, _ = eng.Dispatch(ctx)
	c2 := again["r0"]
	if c2.Attempt != 3 {
		t.Fatalf("redispatch attempt want 3, got %d", c2.Attempt)
	}
	res := sendCmd(t, eng, c2, 1, OutcomeRetriable, "fw-1.0")
	if res.Reason != ReasonRetriesExhausted {
		t.Fatalf("want RETRIES_EXHAUSTED, got %s", res.Reason)
	}
	if eng.Snapshot().Batches[0].Failed != 1 {
		t.Fatalf("exhausted retry not counted as failure: %+v", eng.Snapshot().Batches[0])
	}
	// 旧代次的重复/迟到上报不得重复计数。
	stale := sendRaw(t, eng, ackForCommand(c0, 1, OutcomeFailed, "fw-1.0"))
	if stale.Accepted || stale.Reason != ReasonStale {
		t.Fatalf("stale report must be ignored, got %+v", stale)
	}
	if eng.Snapshot().Batches[0].Failed != 1 {
		t.Fatalf("stale report double-counted failure: %d", eng.Snapshot().Batches[0].Failed)
	}
	t.Logf("retriable->exhausted path OK, stale duplicate ignored")

	// 其余 4 台成功 => 成功 4/5=80% 达到门槛，批次开门。
	for _, id := range ids[1:] {
		sendCmd(t, eng, first[id], 1, OutcomeSuccess, "fw-2.0")
	}
	st := eng.Snapshot()
	if st.Phase != PhaseCompleted || st.Batches[0].Decision != GateOpen {
		t.Fatalf("want COMPLETED with open gate, phase=%s decision=%s",
			st.Phase, st.Batches[0].Decision)
	}
}

func TestIncompatibleDeviceIsTerminalFailure(t *testing.T) {
	ctx := t.Context()
	ids := []string{"i0", "i1", "i2", "i3", "i4"}
	specs := []BatchSpec{{Index: 0, DeviceIDs: ids, InFlightQuota: 5}}
	eng := startEngine(t, NewMemoryStore(), specs, testConfig())
	tasks, _ := eng.Dispatch(ctx)

	// 协议版本过高：上报在 classify 阶段被判为设备不兼容（不静默丢弃）。
	res := sendCmd(t, eng, tasks["i0"], 9, OutcomeSuccess, "fw-2.0")
	if res.Accepted || res.Reason != ReasonProtoTooNew {
		t.Fatalf("want rejected INCOMPATIBLE_PROTO_TOO_NEW, got accepted=%v reason=%s",
			res.Accepted, res.Reason)
	}
	// 该上报未被接受，设备仍在途；超时重投或设备改用受支持协议显式上报 INCOMPATIBLE。
	res2 := sendCmd(t, eng, tasks["i0"], 2, OutcomeIncompatible, "fw-1.0")
	if !res2.Accepted || res2.State != DevIncompatible {
		t.Fatalf("want accepted INCOMPATIBLE, got %+v", res2)
	}
	if b := eng.Snapshot().Batches[0]; b.Failed != 1 {
		t.Fatalf("incompatible device must count into failure, got Failed=%d", b.Failed)
	}
	for _, id := range ids[1:] {
		sendCmd(t, eng, tasks[id], 2, OutcomeSuccess, "fw-2.0")
	}
	if eng.Snapshot().Phase != PhaseCompleted {
		t.Fatalf("want COMPLETED, got %s", eng.Snapshot().Phase)
	}
}
