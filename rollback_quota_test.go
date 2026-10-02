package ota

import "testing"

// queuePlan：批次0 有 3 台设备、在途配额 1。2 台升级成功后第 3 台报不可兼容，
// evaluated=3 时失败率 33.3% > 25% 即熔断，需要回滚的恰好有 2 台，
// 可清晰验证“配额 1 下逐台回滚、其余固定排队、前一台释放后补下一台”。
func queuePlan() Plan {
	p := testPlan()
	p.MaxFailRate = 25
	p.MaxRollbackRate = 25
	p.MinEvaluated = 2
	p.MaxRollbackAttempts = 2
	p.Batches[0] = BatchSpec{Index: 0,
		Devices:     []DeviceID{"d1", "d2", "d3"},
		MaxInFlight: 1}
	return p
}

// TestRollbackQuotaAndQueueOrder：配额 1 下两台已升级设备的回滚严格排队、
// 顺序固定、在途总数（升级+回滚）永远不超过配额，且逐台补位。
func TestRollbackQuotaAndQueueOrder(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, queuePlan())

	// 配额 1：逐台升级。d1、d2 成功，d3 不可兼容熔断。
	succeedDevice(t, o, "d1", 0, 1)
	o.tickForTest()
	succeedDevice(t, o, "d2", 0, 1)
	o.tickForTest()
	inc := upgradeFlow("d3", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("d3 incompatible: kind=%v", d.Kind)
	}

	st := o.State()
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, want rolling-back", st.Phase)
	}
	// 熔断瞬间：d1 立即放行（占唯一名额），d2 按名单位置排队。
	if st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1=%v, want rollback-inflight", st.Devices["d1"].Phase)
	}
	if st.Devices["d2"].Phase != PhaseRollbackQueued {
		t.Fatalf("d2=%v, want queued", st.Devices["d2"].Phase)
	}
	if len(st.RollbackQueue) != 1 || st.RollbackQueue[0] != "d2" {
		t.Fatalf("queue=%v, want [d2]", st.RollbackQueue)
	}
	rs := o.RollbackStatus()
	if len(rs.Inflight) != 1 || rs.Inflight[0] != "d1" || len(rs.Queued) != 1 {
		t.Fatalf("status=%+v, want inflight=[d1] queued=[d2]", rs)
	}

	// d1 未完成前 tick 不得放行 d2，也没有给 d2 下发回滚任务。
	o.tickForTest()
	if n := tr.countTask(taskID("rl-test-001", 0, "d2", TaskRollback, 1)); n != 0 {
		t.Fatalf("d2 dispatched before d1 released: %d", n)
	}
	// 在途视图始终只有 1 台，不超配额。
	if got := o.RollbackStatus(); len(got.Inflight) != 1 || len(got.Queued) != 1 {
		t.Fatalf("inflight/queued changed before release: %+v", got)
	}

	// d1 回到基线 => 释放名额 => 泵自动补 d2。
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback ok: %v", d.Kind)
	}
	st = o.State()
	if st.Devices["d1"].Phase != PhaseDone || !st.Devices["d1"].Restored || st.Devices["d1"].Baseline != "1.0.0" {
		t.Fatalf("d1=%v restored=%v baseline=%q, want done/restored/1.0.0",
			st.Devices["d1"].Phase, st.Devices["d1"].Restored, st.Devices["d1"].Baseline)
	}
	if st.Devices["d2"].Phase != PhaseRollbackInflight {
		t.Fatalf("d2=%v, want inflight after d1 released", st.Devices["d2"].Phase)
	}
	// 已确认回滚完成的 d1 绝不能再被下发，也不能重复计数。
	if n := tr.countTask(taskID("rl-test-001", 0, "d1", TaskRollback, 1)); n != 1 {
		t.Fatalf("d1 rollback dispatched %d times, want exactly 1", n)
	}

	// d2 完成 => 全部回到基线 => 安全中止。
	if d, _ := o.HandleReport(rollbackFlow("d2", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d2 rollback: %v", d.Kind)
	}
	st = o.State()
	if st.Phase != RolloutAborted || st.Batches[0].Phase != BatchFailed {
		t.Fatalf("phase=%v batch=%v, want aborted/failed", st.Phase, st.Batches[0].Phase)
	}
	rs = o.RollbackStatus()
	if len(rs.Queued) != 0 || len(rs.Inflight) != 0 || len(rs.Waiting) != 0 || len(rs.Manual) != 0 {
		t.Fatalf("unexpected residual rollback state: %+v", rs)
	}
	if len(rs.Restored) != 2 {
		t.Fatalf("restored=%v, want 2 devices", rs.Restored)
	}

	// 审计历史中每台设备的回滚成功判定只能出现一次。
	counts := map[DeviceID]int{}
	for _, d := range st.History {
		if d.Kind == DecisionRollbackSucceeded {
			counts[d.DeviceID]++
		}
	}
	for id, n := range counts {
		if n != 1 {
			t.Fatalf("device %s rollback-succeeded verdicts=%d, want 1", id, n)
		}
	}
}

// TestSharedQuotaUpgradeAndRollback：回滚与升级共享当前批次配额。
// 熔断时仍有一台设备停在升级在途（占着唯一名额），队首的回滚设备必须等它
// 终态释放名额后才能放行——同一时刻两条链路的在途总数不超过配额。
func TestSharedQuotaUpgradeAndRollback(t *testing.T) {
	p := queuePlan() // 3 台、配额 1
	o, tr, _ := newTestOrchestrator(t, p)

	succeedDevice(t, o, "d1", 0, 1)
	o.tickForTest() // 释放名额后自动放行 d2 升级
	if st := o.State(); st.Devices["d2"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d2=%v, want upgrade-inflight", st.Devices["d2"].Phase)
	}

	// 手工进入回滚波次：d1 已升级需回滚（入队），d2 仍升级在途（占名额）。
	o.mu.Lock()
	o.st.Phase = RolloutRollingBack
	o.st.Batches[0].Phase = BatchRollingBack
	rec1 := o.st.Devices["d1"]
	rec1.Phase = PhaseUpgraded
	o.enqueueRollbackLocked(rec1)
	o.pumpRollbacksLocked()
	o.mu.Unlock()

	// 配额 1 已被 d2 的在途升级占用：d1 即使在队首也不能被放行为回滚在途。
	st := o.State()
	if st.Devices["d2"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d2=%v, want upgrade-inflight holding the slot", st.Devices["d2"].Phase)
	}
	if st.Devices["d1"].Phase != PhaseRollbackQueued {
		t.Fatalf("d1=%v, want queued behind the in-flight upgrade", st.Devices["d1"].Phase)
	}
	if n := tr.countTask(taskID("rl-test-001", 0, "d1", TaskRollback, 1)); n != 0 {
		t.Fatalf("d1 rollback dispatched while upgrade holds the only slot: %d", n)
	}

	// d2 升级失败（回滚期不重试升级，直接安全终态）=> 释放名额 => d1 才被放行。
	f2 := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, protoV1)
	f2.ReasonCode = "download-timeout"
	if d, _ := o.HandleReport(f2); d.Kind != DecisionFailedRetryable {
		t.Fatalf("d2 failure during rollback: %v", d.Kind)
	}
	st = o.State()
	if st.Devices["d2"].Phase != PhaseDone {
		t.Fatalf("d2=%v, want done(stayed on old firmware)", st.Devices["d2"].Phase)
	}
	if st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1=%v, want inflight after upgrade slot released", st.Devices["d1"].Phase)
	}
}
