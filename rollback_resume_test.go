package ota

import "testing"

// TestResumeRollbackQueueAndInflight：回滚波次中（1 台在途、1 台排队）重启，
// 队列与在途状态必须原样恢复：在途任务以相同 TaskID 重放，排队设备继续排队，
// 已确认完成的设备不再下发；随后流程能接着走到安全中止。
func TestResumeRollbackQueueAndInflight(t *testing.T) {
	p := queuePlan()
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	succeedDevice(t, o, "d1", 0, 1)
	o.tickForTest()
	succeedDevice(t, o, "d2", 0, 1)
	o.tickForTest()
	inc := upgradeFlow("d3", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("incompatible: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v", st.Phase)
	}

	// 重启：d1 回滚在途（需重放相同 TaskID），d2 仍在队列（不得下发）。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	st := o2.State()
	if st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1=%v, want rollback-inflight replayed", st.Devices["d1"].Phase)
	}
	if st.Devices["d2"].Phase != PhaseRollbackQueued || len(st.RollbackQueue) != 1 {
		t.Fatalf("d2=%v queue=%v, want d2 still queued", st.Devices["d2"].Phase, st.RollbackQueue)
	}
	tasks := tr2.tasks()
	if len(tasks) != 1 || tasks[0].Device != "d1" || tasks[0].Kind != TaskRollback {
		t.Fatalf("resumed tasks=%+v, want only d1 rollback replay", tasks)
	}
	if tasks[0].ID != taskID("rl-test-001", 0, "d1", TaskRollback, 1) {
		t.Fatalf("replayed task id=%s changed across restart", tasks[0].ID)
	}
	if tasks[0].Target != FirmwareVersion("1.0.0") {
		t.Fatalf("rollback target=%q, want baseline 1.0.0", tasks[0].Target)
	}

	// 续跑：d1 回到基线后 d2 才被补入，d2 完成 => 安全中止。
	if d, err := o2.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); err != nil || d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback after resume: %v %v", d.Kind, err)
	}
	if got := o2.State(); got.Devices["d2"].Phase != PhaseRollbackInflight {
		t.Fatalf("d2=%v, want inflight after d1 released", got.Devices["d2"].Phase)
	}
	if d, err := o2.HandleReport(rollbackFlow("d2", 0, 1, ReportRollbackSuccess)); err != nil || d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d2 rollback after resume: %v %v", d.Kind, err)
	}
	if got := o2.State(); got.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted", got.Phase)
	}
	// 已确认回滚完成的 d1 在第二段进程里绝不再下发。
	if n := tr2.countTask(taskID("rl-test-001", 0, "d1", TaskRollback, 1)); n != 1 {
		t.Fatalf("d1 rollback replayed/dispatched %d times in 2nd process, want 1", n)
	}
}

// TestResumeRollbackRetrying：回滚失败进入定时等待后重启：
//   - 已用回滚尝试次数（预算）跨重启保持，重启不会多打一轮；
//   - 未到持久化的 NextAttemptAt 前绝不重投，到点后才以正确尝试号重放；
//   - 再失败到预算耗尽即人工终态，重启也无法把已用预算抹掉。
func TestResumeRollbackRetrying(t *testing.T) {
	p := queuePlan()
	p.RollbackRetryInterval = 50_000_000 // 50ms，用确定性时钟驱动
	store := NewMemoryStore()
	clock := newFakeClock(mustTime("2026-10-03T00:00:00Z"))
	defer clock.restore()

	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	succeedDevice(t, o, "d1", 0, 1)
	o.tickForTest()
	succeedDevice(t, o, "d2", 0, 1)
	o.tickForTest()
	inc := upgradeFlow("d3", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("incompatible: %v", d.Kind)
	}
	// d1 回滚 attempt1 失败 => 等待，attempt2 排在 +50ms。
	f1 := rollbackFlow("d1", 0, 1, ReportRollbackFailure)
	f1.ReasonCode = "rollback-flash-failed"
	if d, _ := o.HandleReport(f1); d.Kind != DecisionRetried {
		t.Fatalf("fail#1: %v", d.Kind)
	}
	scheduled := o.State().Devices["d1"].NextAttemptAt
	clock.advance(10_000_000) // 只走 10ms，未到点。

	// 重启。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	st := o2.State()
	if st.Devices["d1"].Phase != PhaseRollbackWaiting || st.Devices["d1"].RollbackAttempt != 2 {
		t.Fatalf("d1=%v attempt=%d, want waiting/2 preserved", st.Devices["d1"].Phase, st.Devices["d1"].RollbackAttempt)
	}
	if !st.Devices["d1"].NextAttemptAt.Equal(scheduled) {
		t.Fatalf("nextAttemptAt changed across restart: got %v want %v", st.Devices["d1"].NextAttemptAt, scheduled)
	}
	// 恢复本身不得重投等待中的任务；恢复后补偿 tick（仍未到点）也不得提前重投。
	o2.tickForTest()
	if n := len(tr2.tasks()); n != 0 {
		t.Fatalf("waiting rollback redriven %d times before scheduled time", n)
	}
	// d2 仍排队，且不会因为 d1 在等待而被错误放行（等待设备保留名额）。
	if st := o2.State(); st.Devices["d2"].Phase != PhaseRollbackQueued {
		t.Fatalf("d2=%v, want still queued while d1 holds slot", st.Devices["d2"].Phase)
	}

	// 到点：tick 重投 attempt2（相同确定性 TaskID），不多打一轮。
	clock.advance(41_000_000) // 累计 51ms
	o2.tickForTest()
	tasks := tr2.tasks()
	if len(tasks) != 1 || tasks[0].ID != taskID("rl-test-001", 0, "d1", TaskRollback, 2) {
		t.Fatalf("redrive after restart=%+v, want one attempt2", tasks)
	}
	o2.tickForTest()
	if n := len(tr2.tasks()); n != 1 {
		t.Fatalf("duplicate redrive: %d tasks", n)
	}

	// attempt2 再失败：预算（2）已在重启前用掉 1，重启不抹除 => 立即耗尽，人工终态。
	f2 := rollbackFlow("d1", 0, 2, ReportRollbackFailure)
	f2.ReasonCode = "rollback-flash-failed"
	d, _ := o2.HandleReport(f2)
	if d.Kind != DecisionRollbackFailed || d.Class != ClassRollbackBudgetExhausted {
		t.Fatalf("fail#2: kind=%v class=%v, want rollback-failed/budget-exhausted", d.Kind, d.Class)
	}
	st = o2.State()
	if st.Phase != RolloutAborted || st.Devices["d1"].Phase != PhaseRollbackFailed {
		t.Fatalf("phase=%v d1=%v, want aborted/rollback-failed", st.Phase, st.Devices["d1"].Phase)
	}
}

// TestRollbackWaitsForInflightUpgrade：回滚波次中若仍有设备停在升级在途
// （其终态决定它是否需要回滚），即使当前一个回滚在途/排队设备都没有，
// 也不得提前宣告安全中止；等该设备终态后才能收敛。
func TestRollbackWaitsForInflightUpgrade(t *testing.T) {
	p := queuePlan() // 3 台、配额 1
	o, _, _ := newTestOrchestrator(t, p)

	succeedDevice(t, o, "d1", 0, 1)
	o.tickForTest() // d2 被放行升级
	if st := o.State(); st.Devices["d2"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d2=%v, want upgrade-inflight", st.Devices["d2"].Phase)
	}

	// 手工进入回滚波次：没有设备已升级（回滚侧为空），但 d2 升级在途未决。
	o.mu.Lock()
	o.st.Phase = RolloutRollingBack
	o.st.Batches[0].Phase = BatchRollingBack
	_ = o.evaluateRollbackLocked()
	phase := o.st.Phase
	o.mu.Unlock()
	if phase != RolloutRollingBack {
		t.Fatalf("phase=%v, must keep rolling-back while an upgrade is in-flight", phase)
	}

	// d2 升级成功 => 进入回滚队列（仍是回滚中，未中止）；随后回到基线才安全收敛。
	if d, _ := o.HandleReport(upgradeFlow("d2", 0, 1, ReportAcknowledge, protoV1)); d.Kind != DecisionAcked {
		t.Fatalf("d2 ack during rollback: %v", d.Kind)
	}
	if d, _ := o.HandleReport(upgradeFlow("d2", 0, 1, ReportUpgradeSuccess, protoV1)); d.Kind != DecisionSucceeded {
		t.Fatalf("d2 success during rollback: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, d2 now needs rollback", st.Phase)
	}
	if st := o.State(); st.Devices["d2"].Phase != PhaseRollbackInflight {
		// 配额1：d1 还在排队（名次靠前），d2 成功后 d1 先被放行，d2 排队。
		if st.Devices["d2"].Phase != PhaseRollbackQueued && st.Devices["d1"].Phase != PhaseRollbackInflight {
			t.Fatalf("unexpected phases: d1=%v d2=%v", st.Devices["d1"].Phase, st.Devices["d2"].Phase)
		}
	}
	// 把所有需要回滚的设备（d1、d2）确认回到基线 => 安全中止。
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback: %v", d.Kind)
	}
	if d, _ := o.HandleReport(rollbackFlow("d2", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d2 rollback: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted after all restored", st.Phase)
	}
}
