package ota

import (
	"sync"
	"testing"
)

// 触发一次回滚并让 d1 进入回滚在途（queuePlan：配额1，d2 排队）。
func rollbackInflightD1(t *testing.T, p Plan) (*Orchestrator, *stubTransport) {
	t.Helper()
	o, tr, _ := newTestOrchestrator(t, p)
	succeedDevice(t, o, "d1", 0, 1)
	o.tickForTest()
	succeedDevice(t, o, "d2", 0, 1)
	o.tickForTest()
	inc := upgradeFlow("d3", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("incompatible: %v", d.Kind)
	}
	if st := o.State(); st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1=%v, want rollback-inflight", st.Devices["d1"].Phase)
	}
	if st := o.State(); st.Devices["d1"].Baseline != "1.0.0" {
		t.Fatalf("baseline=%q, want 1.0.0", st.Devices["d1"].Baseline)
	}
	return o, tr
}

// TestRollbackVersionMismatchIsInvalid：回滚成功帧报回的落地版本与基线不一致，
// 不算成功、原因可区分（invalid + 明确原因），设备保持回滚在途、不消耗重试预算，
// 等设备重报；随后报回正确基线才确认成功并补位队列。
func TestRollbackVersionMismatchIsInvalid(t *testing.T) {
	p := queuePlan()
	o, tr := rollbackInflightD1(t, p)

	wrong := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	wrong.ToVer = "1.5.0" // 设备落到一个既不是目标也不是基线的版本
	d, err := o.HandleReport(wrong)
	if err != nil || d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("mismatch: kind=%v class=%v err=%v", d.Kind, d.Class, err)
	}
	t.Logf("mismatch decision reason=%q", d.Reason)
	st := o.State()
	if st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1=%v, must stay rollback-inflight awaiting re-report", st.Devices["d1"].Phase)
	}
	if st.Devices["d1"].RollbackAttempt != 1 {
		t.Fatalf("rollback attempt=%d, invalid report must not consume budget", st.Devices["d1"].RollbackAttempt)
	}
	// 配额仍被 d1 占用，d2 不得补入。
	if st.Devices["d2"].Phase != PhaseRollbackQueued {
		t.Fatalf("d2=%v, want queued", st.Devices["d2"].Phase)
	}

	// 没说版本（缺 ToVer）同样无效、保持在途。
	noVer := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	noVer.ToVer = ""
	if d, _ := o.HandleReport(noVer); d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("missing version: kind=%v class=%v", d.Kind, d.Class)
	}
	if st := o.State(); st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1=%v after missing-version, want still inflight", st.Devices["d1"].Phase)
	}

	// 重报正确基线版本 => 成功，释放名额补 d2，且没有产生额外回滚下发给 d1。
	good := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	if d, _ := o.HandleReport(good); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("correct baseline: %v", d.Kind)
	}
	if n := tr.countTask(taskID("rl-test-001", 0, "d1", TaskRollback, 1)); n != 1 {
		t.Fatalf("d1 rollback dispatched %d times, invalid reports must not trigger redispatch", n)
	}
	if st := o.State(); st.Devices["d2"].Phase != PhaseRollbackInflight {
		t.Fatalf("d2=%v, want inflight after d1 confirmed", st.Devices["d2"].Phase)
	}
}

// TestRollbackSuccessWithoutBaselineRecorded：若设备升级阶段始终没上报 FromVer，
// 基线为空，此时回滚成功无法核验，必须判无效并保持在途等待重报，绝不假装成功。
func TestRollbackSuccessWithoutBaselineRecorded(t *testing.T) {
	p := queuePlan()
	o, _, _ := newTestOrchestrator(t, p)
	// 手工构造不带 FromVer 的升级上报，使基线为空。
	up := func(id DeviceID, kind ReportKind) DeviceReport {
		r := upgradeFlow(id, 0, 1, kind, protoV1)
		r.FromVer = ""
		return r
	}
	if d, _ := o.HandleReport(up("d1", ReportAcknowledge)); d.Kind != DecisionAcked {
		t.Fatalf("d1 ack: %v", d.Kind)
	}
	if d, _ := o.HandleReport(up("d1", ReportUpgradeSuccess)); d.Kind != DecisionSucceeded {
		t.Fatalf("d1 up: %v", d.Kind)
	}
	o.tickForTest()
	if d, _ := o.HandleReport(up("d2", ReportAcknowledge)); d.Kind != DecisionAcked {
		t.Fatalf("d2 ack: %v", d.Kind)
	}
	if d, _ := o.HandleReport(up("d2", ReportUpgradeSuccess)); d.Kind != DecisionSucceeded {
		t.Fatalf("d2 up: %v", d.Kind)
	}
	o.tickForTest()
	inc := up("d3", ReportUpgradeFailure)
	inc.Protocol = Semver{Major: 2}
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("d3 inc: %v", d.Kind)
	}
	if st := o.State(); st.Devices["d1"].Baseline != "" {
		t.Fatalf("baseline should be empty, got %q", st.Devices["d1"].Baseline)
	}
	// 即便回滚帧带了 ToVer，也因无基线可核验而无效。
	rb := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	if d, _ := o.HandleReport(rb); d.Kind != DecisionFailedInvalid {
		t.Fatalf("rollback without baseline: %v, want failed-invalid", d.Kind)
	}
	if st := o.State(); st.Devices["d1"].Phase != PhaseRollbackInflight {
		t.Fatalf("d1=%v, want inflight awaiting baseline evidence", st.Devices["d1"].Phase)
	}
}

// TestConcurrentRollbackReportsUnique：多 goroutine 并发向同一台在途回滚设备
// 投递“正确基线成功 / 错误版本成功 / 重复成功”帧，回滚成功结论只能生效一次，
// 设备只回基线一次，队列只补位一次；-race 下无数据竞争。
func TestConcurrentRollbackReportsUnique(t *testing.T) {
	p := queuePlan()
	o, tr := rollbackInflightD1(t, p)

	const workers = 32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var r DeviceReport
			switch {
			case w%4 == 0:
				r = rollbackFlow("d1", 0, 1, ReportRollbackSuccess) // 正确
			case w%4 == 1:
				r = rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
				r.ToVer = "1.5.0" // 对不上
			default:
				r = rollbackFlow("d1", 0, 1, ReportRollbackSuccess) // 重复正确
			}
			d, err := o.HandleReport(r)
			if err != nil {
				t.Errorf("worker %d: %v", w, err)
				return
			}
			t.Logf("worker=%d toVer=%q decision=%s class=%s reason=%q",
				w, r.ToVer, decisionName(d.Kind), className(d.Class), d.Reason)
		}(w)
	}
	wg.Wait()

	st := o.State()
	if st.Devices["d1"].Phase != PhaseDone || !st.Devices["d1"].Restored {
		t.Fatalf("d1=%v restored=%v, want done/restored exactly once",
			st.Devices["d1"].Phase, st.Devices["d1"].Restored)
	}
	// 回滚成功判定在历史中只能出现一次。
	n := 0
	for _, hd := range st.History {
		if hd.DeviceID == "d1" && hd.Kind == DecisionRollbackSucceeded {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("rollback-succeeded verdicts=%d, want 1", n)
	}
	// d1 的回滚任务全程只下发一次；d2 在唯一一次成功后被补位一次。
	if c := tr.countTask(taskID("rl-test-001", 0, "d1", TaskRollback, 1)); c != 1 {
		t.Fatalf("d1 rollback dispatched %d times, want 1", c)
	}
	if st.Devices["d2"].Phase != PhaseRollbackInflight {
		t.Fatalf("d2=%v, want inflight (queue advanced exactly once)", st.Devices["d2"].Phase)
	}
}

// TestRollbackFailureDuplicatesCountedOnce：同一帧回滚失败在重试等待期被
// 并发/重复送达，只能消耗一次预算：不会从 attempt1 直接跳到 attempt3，也不会
// 提前进入人工终态。
func TestRollbackFailureDuplicatesCountedOnce(t *testing.T) {
	p := queuePlan()
	p.MaxRollbackAttempts = 3
	o, _ := rollbackInflightD1(t, p)

	f := rollbackFlow("d1", 0, 1, ReportRollbackFailure)
	f.ReasonCode = "rollback-flash-failed"

	const workers = 16
	var wg sync.WaitGroup
	results := make([]DecisionKind, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			d, err := o.HandleReport(f)
			if err != nil {
				t.Errorf("worker %d: %v", w, err)
			}
			results[w] = d.Kind
		}(w)
	}
	wg.Wait()

	retried, dup := 0, 0
	for _, k := range results {
		switch k {
		case DecisionRetried:
			retried++
		case DecisionDuplicate, DecisionStale:
			dup++
		default:
			t.Fatalf("unexpected decision kind %v", k)
		}
	}
	if retried != 1 || dup != workers-1 {
		t.Fatalf("retried=%d dup/stale=%d, want exactly 1 retry and %d duplicates",
			retried, dup, workers-1)
	}
	st := o.State()
	if st.Devices["d1"].Phase != PhaseRollbackWaiting || st.Devices["d1"].RollbackAttempt != 2 {
		t.Fatalf("d1=%v attempt=%d, want waiting/2 (budget consumed once)",
			st.Devices["d1"].Phase, st.Devices["d1"].RollbackAttempt)
	}
	if st.Devices["d1"].NextAttemptAt.IsZero() {
		t.Fatal("scheduled next attempt time must be persisted")
	}
}
