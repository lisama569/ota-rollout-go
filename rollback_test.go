package ota

import (
	"testing"
	"time"
)

// failPlan：批次0 有 4 台设备，阈值失败率/回滚率上限 25%，最小样本 2。
func failPlan() Plan {
	p := testPlan()
	p.MaxFailRate = 25
	p.MaxRollbackRate = 25
	p.MinEvaluated = 2
	p.Batches[0] = BatchSpec{Index: 0,
		Devices:     []DeviceID{"d1", "d2", "d3", "d4"},
		MaxInFlight: 2}
	return p
}

// TestFailRateTriggersRollback：样本达到 2 后 50% 失败率即熔断，
// 已升级设备被回滚，未开始的批次被永久关闭。
func TestFailRateTriggersRollback(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, failPlan())

	// d1 成功。
	succeedDevice(t, o, "d1", 0, 1)
	// d2 上报不可兼容（协议主版本更高）。
	bad := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2, Minor: 1})
	bad.ReasonCode = "protocol-too-new"
	d, err := o.HandleReport(bad)
	if err != nil || d.Kind != DecisionFailedIncompatible || d.Class != ClassIncompatible {
		t.Fatalf("incompatible: kind=%v class=%v err=%v", d.Kind, d.Class, err)
	}
	if st := o.State(); st.Devices["d2"].Phase != PhaseIncompatible {
		t.Fatalf("d2 phase=%v", st.Devices["d2"].Phase)
	}

	st := o.State()
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%d, want rolling-back after 1/2=50%% fail rate", st.Phase)
	}
	// d3 在 d1 成功后已占用释放的名额在途升级：回滚期不重试升级，等其终态；
	// d4 从未放量，必须直接落安全终态、不再下发升级。
	if st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1 phase=%v, want rollback-inflight", st.Devices["d1"].Phase)
	}
	if st.Devices["d3"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d3 phase=%v, want upgrade-inflight (awaiting terminal)", st.Devices["d3"].Phase)
	}
	if st.Devices["d4"].Phase != PhaseDone {
		t.Fatalf("d4=%v, want done(never-upgraded)", st.Devices["d4"].Phase)
	}
	// d3 随后报告临时失败：回滚期不得重试升级，直接安全终态。
	f3 := upgradeFlow("d3", 0, 1, ReportUpgradeFailure, protoV1)
	f3.ReasonCode = "download-timeout"
	if d, _ := o.HandleReport(f3); d.Kind != DecisionFailedRetryable {
		t.Fatalf("d3 failure during rollback: kind=%v", d.Kind)
	}
	if st := o.State(); st.Devices["d3"].Phase != PhaseDone {
		t.Fatalf("d3 phase=%v, want done without retry", st.Devices["d3"].Phase)
	}
	// d1 回滚成功 => 全部离开新固件 => 中止。
	rb := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	if d, err := o.HandleReport(rb); err != nil || d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("rollback ok: %v %v", d.Kind, err)
	}
	st = o.State()
	if st.Phase != RolloutAborted || st.Batches[0].Phase != BatchFailed {
		t.Fatalf("phase=%d batch=%d, want aborted/failed", st.Phase, st.Batches[0].Phase)
	}
	// 批次 1 的设备永远不应收到升级任务。
	for _, tk := range tr.tasks() {
		if tk.Device == "d5" || tk.Device == "d6" || tk.Device == "d7" {
			t.Fatalf("later-batch device %s got task after rollback: %+v", tk.Device, tk)
		}
	}
}

// TestRollbackRateTriggersRollback：设备自报已回滚（回滚率）超阈值同样熔断。
func TestRollbackRateTriggersRollback(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, failPlan())
	succeedDevice(t, o, "d1", 0, 1)
	// d2 在升级在途时直接上报 rollback-success（设备在升级任务上下文中自发回到旧版）。
	r := DeviceReport{
		DeviceID: "d2",
		TaskID:   taskID("rl-test-001", 0, "d2", TaskUpgrade, 1),
		Kind:     ReportRollbackSuccess,
		Protocol: protoV1,
	}
	d, err := o.HandleReport(r)
	if err != nil || d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("self rollback: %v %v", d.Kind, err)
	}
	st := o.State()
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%d, want rolling-back (rollback rate 50%%)", st.Phase)
	}
	if st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1 must be rolled back, phase=%v", st.Devices["d1"].Phase)
	}
}

// TestThreeFailureClasses：不可兼容 / 无效上报 / 可重试失败必须得到可区分结论。
func TestThreeFailureClasses(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, testPlan())

	// 1) 无效：缺 TaskID 的畸形帧，必须显式判定 invalid，设备状态保持在途。
	malformed := DeviceReport{DeviceID: "d1", Kind: ReportUpgradeSuccess, Protocol: protoV1, ToVer: "2.0.0"}
	d, err := o.HandleReport(malformed)
	if err != nil || d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("malformed: kind=%v class=%v err=%v", d.Kind, d.Class, err)
	}
	if st := o.State(); st.Devices["d1"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d1 phase changed by invalid report: %v", st.Devices["d1"].Phase)
	}

	// 2) 无效：目标版本张冠李戴，不计成功也不消耗重试。
	wrongVer := upgradeFlow("d1", 0, 1, ReportUpgradeFailure, protoV1)
	wrongVer.ToVer = "9.9.9"
	wrongVer.ReasonCode = "flash-failed"
	if d, _ := o.HandleReport(wrongVer); d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("wrong version: kind=%v class=%v", d.Kind, d.Class)
	}
	if st := o.State(); st.Devices["d1"].Attempt != 1 {
		t.Fatalf("attempt consumed by invalid report: %d", st.Devices["d1"].Attempt)
	}

	// 3) 可重试：临时失败，下发 attempt 2。
	retry := upgradeFlow("d1", 0, 1, ReportUpgradeFailure, protoV1)
	retry.ReasonCode = "download-timeout"
	if d, _ := o.HandleReport(retry); d.Kind != DecisionRetried || d.Class != ClassRetryable {
		t.Fatalf("retry: kind=%v class=%v", d.Kind, d.Class)
	}
	if st := o.State(); st.Devices["d1"].CurrentTask !=
		taskID("rl-test-001", 0, "d1", TaskUpgrade, 2) {
		t.Fatalf("current task not retry#2: %s", st.Devices["d1"].CurrentTask)
	}

	// 4) 不可兼容：协议主版本更高 => 终态 incompatible。
	inc := upgradeFlow("d1", 0, 2, ReportUpgradeFailure, Semver{Major: 9})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible || d.Class != ClassIncompatible {
		t.Fatalf("incompatible: kind=%v class=%v", d.Kind, d.Class)
	}
	if st := o.State(); st.Devices["d1"].Phase != PhaseIncompatible {
		t.Fatalf("d1 phase=%v", st.Devices["d1"].Phase)
	}
}

// TestRetryExhaustion：可重试失败超过 MaxAttempts 后落 failed 终态。
func TestRetryExhaustion(t *testing.T) {
	p := testPlan()
	p.MaxAttempts = 2
	o, tr, _ := newTestOrchestrator(t, p)

	// attempt 1 失败 => 重试 attempt 2。
	f1 := upgradeFlow("d1", 0, 1, ReportUpgradeFailure, protoV1)
	f1.ReasonCode = "download-timeout"
	if d, _ := o.HandleReport(f1); d.Kind != DecisionRetried {
		t.Fatalf("first failure should retry, got %v", d.Kind)
	}
	// attempt 2 失败 => 耗尽，failed 终态。
	f2 := upgradeFlow("d1", 0, 2, ReportUpgradeFailure, protoV1)
	f2.ReasonCode = "download-timeout"
	d, _ := o.HandleReport(f2)
	if d.Kind != DecisionFailedRetryable {
		t.Fatalf("second failure should exhaust, got %v", d.Kind)
	}
	if st := o.State(); st.Devices["d1"].Phase != PhaseFailed {
		t.Fatalf("phase=%v want failed", st.Devices["d1"].Phase)
	}
	// 之后再来的同任务失败帧必须是 stale/duplicate，不重复计数。
	d2, _ := o.HandleReport(f2)
	if d2.Kind != DecisionStale && d2.Kind != DecisionDuplicate {
		t.Fatalf("late frame kind=%v, want stale/duplicate", d2.Kind)
	}
	// 仅下发过 attempt1、attempt2 两个升级任务。
	if n := tr.countTask(taskID("rl-test-001", 0, "d1", TaskUpgrade, 1)); n != 1 {
		t.Fatalf("attempt1 dispatched %d", n)
	}
	if n := tr.countTask(taskID("rl-test-001", 0, "d1", TaskUpgrade, 2)); n != 1 {
		t.Fatalf("attempt2 dispatched %d", n)
	}
}

// TestRollbackFailureExhaustsAborts：回滚任务反复失败、耗尽独立预算后，
// 设备进入明确的人工介入终态（ClassRollbackBudgetExhausted），发布必须中止，
// 而不是无限重试或假装回滚成功。重试按固定节奏发生，预算计数持久化。
func TestRollbackFailureExhaustsAborts(t *testing.T) {
	p := failPlan()
	p.MaxAttempts = 2
	// 固定节奏设为 0 会走默认值，这里显式给一个小间隔并在测试中用确定性时钟驱动。
	p.RollbackRetryInterval = 50 * time.Millisecond
	clock := newFakeClock(time.Unix(1000, 0))
	defer clock.restore()
	o, _, _ := newTestOrchestrator(t, p)
	succeedDevice(t, o, "d1", 0, 1)
	inc := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("incompatible: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v", st.Phase)
	}
	// d1 回滚失败：预算 2 内，进入等待重试，排定 attempt 2。
	f1 := rollbackFlow("d1", 0, 1, ReportRollbackFailure)
	f1.ReasonCode = "rollback-flash-failed"
	if d, _ := o.HandleReport(f1); d.Kind != DecisionRetried {
		t.Fatalf("rollback fail #1: %v, want retried(waiting)", d.Kind)
	}
	st := o.State()
	if st.Devices["d1"].Phase != PhaseRollbackWaiting || st.Devices["d1"].RollbackAttempt != 2 {
		t.Fatalf("d1 after fail#1: phase=%v attempt=%d, want waiting/2",
			st.Devices["d1"].Phase, st.Devices["d1"].RollbackAttempt)
	}
	if st.Devices["d1"].NextAttemptAt.IsZero() {
		t.Fatal("next attempt time not persisted")
	}
	// 未到节奏点：tick 不得提前重投（比较 tick 前后的任务数差值）。
	base := len(o.TransportTasksForTest())
	o.tickForTest()
	if n := len(o.TransportTasksForTest()) - base; n != 0 {
		t.Fatalf("rollback redriven before cadence: %d extra tasks", n)
	}
	// 到点后 tick 以相同的确定性 TaskID 重投 attempt 2。
	clock.advance(51 * time.Millisecond)
	before := len(o.TransportTasksForTest())
	o.tickForTest()
	tasks := o.TransportTasksForTest()
	if len(tasks) != before+1 || tasks[len(tasks)-1].ID != taskID("rl-test-001", 0, "d1", TaskRollback, 2) {
		t.Fatalf("redrive tasks=%+v, want one new rollback attempt2", tasks)
	}
	// 重复 tick（未产生新失败）不得多打一轮。
	o.tickForTest()
	if n := len(o.TransportTasksForTest()); n != before+1 {
		t.Fatalf("extra redrive dispatched: tasks=%d", n)
	}
	// attempt 2 再失败 => 预算耗尽，人工介入终态。
	f2 := rollbackFlow("d1", 0, 2, ReportRollbackFailure)
	f2.ReasonCode = "rollback-flash-failed"
	d, _ := o.HandleReport(f2)
	if d.Kind != DecisionRollbackFailed || d.Class != ClassRollbackBudgetExhausted {
		t.Fatalf("rollback fail #2: kind=%v class=%v, want rollback-failed/rollback-budget-exhausted",
			d.Kind, d.Class)
	}
	st = o.State()
	if st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted", st.Phase)
	}
	if st.Devices["d1"].Phase != PhaseRollbackFailed {
		t.Fatalf("d1 phase=%v, want rollback-failed(manual)", st.Devices["d1"].Phase)
	}
	// 人工终态后 tick 不得再下发任何任务。
	o.tickForTest()
	if n := len(o.TransportTasksForTest()); n != before+1 {
		t.Fatalf("tasks after manual abort=%d, want %d", n, before+1)
	}
}
