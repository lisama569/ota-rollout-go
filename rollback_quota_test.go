package ota

import (
	"strings"
	"sync"
	"testing"
)

// wavePlan：批次0（d1,d2，配额2）全绿通过；批次1（d3,d4，配额1）第二台不可兼容，
// 在批次1触发回滚 => 回滚配额 = 批次1配额 = 1。已升级设备 d1,d2,d3 排队回滚。
func wavePlan() Plan {
	p := testPlan()
	p.MinEvaluated = 2
	p.Batches = []BatchSpec{
		{Index: 0, Devices: []DeviceID{"d1", "d2"}, MaxInFlight: 2},
		{Index: 1, Devices: []DeviceID{"d3", "d4"}, MaxInFlight: 1},
	}
	return p
}

// driveToRollbackWave 把发布驱动到回滚波次：d1,d2,d3 已升级，d4 不可兼容触发熔断。
func driveToRollbackWave(t *testing.T, o *Orchestrator) {
	t.Helper()
	succeedDevice(t, o, "d1", 0, 1)
	succeedDevice(t, o, "d2", 0, 1)
	succeedDevice(t, o, "d3", 1, 1)
	inc := upgradeFlow("d4", 1, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("d4 incompatible: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, want rolling-back", st.Phase)
	}
}

func rollbackTasks(tr *stubTransport) []Task {
	var out []Task
	for _, tk := range tr.tasks() {
		if tk.Kind == TaskRollback {
			out = append(out, tk)
		}
	}
	return out
}

// TestRollbackQuotaAndQueueing：回滚与放量一样受在途配额约束（升级+回滚合计不超配额），
// 超出的设备按“批次序+计划内设备序”的固定顺序排队，前一台落回基线后补下一台。
func TestRollbackQuotaAndQueueing(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, wavePlan())
	driveToRollbackWave(t, o)

	// 配额 1：只有 d1 被放行，d2/d3 排队；放行顺序是计划顺序而非上报顺序。
	rbs := rollbackTasks(tr)
	if len(rbs) != 1 || rbs[0].Device != "d1" || rbs[0].Attempt != 1 {
		t.Fatalf("rollback dispatched=%+v, want only d1 attempt 1", rbs)
	}
	if rbs[0].Target != oldVer {
		t.Fatalf("rollback target=%q, want recorded baseline %q", rbs[0].Target, oldVer)
	}
	s := o.Stats()
	t.Logf("after trigger: %+v", s)
	if s.Quota != 1 || s.RollbackInflight != 1 || s.RollbackQueued != 2 || s.UpgradeInflight != 0 {
		t.Fatalf("stats=%+v, want quota=1 rollbackInflight=1 queued=2", s)
	}
	st := o.State()
	if st.Devices["d2"].Phase != PhaseUpgraded || st.Devices["d3"].Phase != PhaseUpgraded {
		t.Fatalf("d2=%v d3=%v, want both queued (upgraded)",
			st.Devices["d2"].Phase, st.Devices["d3"].Phase)
	}

	// d1 落回基线 => 释放名额，d2 补进；d2 完成 => d3 补进。全程在途不超配额。
	for _, id := range []DeviceID{"d1", "d2"} {
		ok := rollbackFlow(id, 0, 1, ReportRollbackSuccess)
		if d, _ := o.HandleReport(ok); d.Kind != DecisionRollbackSucceeded {
			t.Fatalf("%s rollback: %v", id, d.Kind)
		}
		s = o.Stats()
		t.Logf("after %s restored: %+v", id, s)
		if s.RollbackInflight+s.UpgradeInflight > s.Quota {
			t.Fatalf("in-flight %d exceeds quota %d after %s",
				s.RollbackInflight+s.UpgradeInflight, s.Quota, id)
		}
	}
	rbs = rollbackTasks(tr)
	if len(rbs) != 3 || rbs[1].Device != "d2" || rbs[2].Device != "d3" {
		t.Fatalf("rollback order=%v, want d1,d2,d3 in plan order",
			[]DeviceID{rbs[0].Device, rbs[1].Device, rbs[2].Device})
	}

	// 最后一台落回基线 => 回滚波次收敛，发布安全中止。
	if d, _ := o.HandleReport(rollbackFlow("d3", 1, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d3 rollback: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted after wave finished", st.Phase)
	}
	// 已确认回滚的设备不得再被下发。
	if n := tr.countTask(taskID("rl-test-001", 0, "d1", TaskRollback, 1)); n != 1 {
		t.Fatalf("d1 rollback dispatched %d times, want 1", n)
	}
}

// TestRollbackResumeMidWave：回滚波次中途重启，排队与在途状态接着走：
// 在途任务以相同 TaskID 重放，排队设备不占新名额，已完成设备不再下发也不重复计数。
func TestRollbackResumeMidWave(t *testing.T) {
	p := wavePlan()
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	driveToRollbackWave(t, o)

	// 第一次重启：d1 在途重放（同 TaskID），d2/d3 仍在排队，不得被补发。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume1: %v", err)
	}
	tasks := tr2.tasks()
	if len(tasks) != 1 || tasks[0].ID != taskID("rl-test-001", 0, "d1", TaskRollback, 1) {
		t.Fatalf("resume1 dispatched=%+v, want single replay of d1 rollback a1", tasks)
	}
	if s := o2.Stats(); s.RollbackInflight != 1 || s.RollbackQueued != 2 {
		t.Fatalf("after resume1 stats=%+v, want inflight=1 queued=2", s)
	}

	// d1 落回基线 => d2 补进；再次重启：只有 d2 的在途被重放，d1 不再出现。
	if d, _ := o2.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback: %v", d.Kind)
	}
	tr3 := &stubTransport{}
	o3 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr3})
	if err := o3.RecoverOrResume(); err != nil {
		t.Fatalf("resume2: %v", err)
	}
	tasks = tr3.tasks()
	if len(tasks) != 1 || tasks[0].ID != taskID("rl-test-001", 0, "d2", TaskRollback, 1) {
		t.Fatalf("resume2 dispatched=%+v, want single replay of d2 rollback a1", tasks)
	}

	// 走完剩余波次；d1 的 rollback-succeeded 判定跨重启只能有一次。
	if d, _ := o3.HandleReport(rollbackFlow("d2", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d2 rollback: %v", d.Kind)
	}
	if d, _ := o3.HandleReport(rollbackFlow("d3", 1, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d3 rollback: %v", d.Kind)
	}
	st := o3.State()
	if st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted", st.Phase)
	}
	n := 0
	for _, h := range st.History {
		if h.DeviceID == "d1" && h.Kind == DecisionRollbackSucceeded {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("d1 rollback-succeeded verdicts=%d across restarts, want 1", n)
	}
}

// TestRollbackRetryBudgetSurvivesRestart：回滚重试预算跨重启保持：
// 已用次数不被抹掉、重启不多打一轮，预算耗尽后进入需人工介入的终态且原因可区分。
func TestRollbackRetryBudgetSurvivesRestart(t *testing.T) {
	p := failPlan()
	p.MaxRollbackAttempts = 2
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	succeedDevice(t, o, "d1", 0, 1)
	inc := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("incompatible: %v", d.Kind)
	}

	// 预算 2：第 1 次失败 => 重试 attempt 2。
	f1 := rollbackFlow("d1", 0, 1, ReportRollbackFailure)
	f1.ReasonCode = "rollback-flash-failed"
	if d, _ := o.HandleReport(f1); d.Kind != DecisionRetried {
		t.Fatalf("rollback fail #1: %v, want retried", d.Kind)
	}

	// 重启：只重放在途的 attempt 2（同 TaskID），不多打一轮、不重置预算。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	tasks := tr2.tasks()
	if len(tasks) != 1 || tasks[0].ID != taskID("rl-test-001", 0, "d1", TaskRollback, 2) {
		t.Fatalf("resume dispatched=%+v, want single replay of d1 rollback a2", tasks)
	}
	if got := o2.State().Devices["d1"].RollbackAttempt; got != 2 {
		t.Fatalf("rollback attempt=%d after restart, want 2 (budget preserved)", got)
	}

	// 第 2 次失败 => 预算耗尽：需人工介入终态，原因与其他失败可区分。
	f2 := rollbackFlow("d1", 0, 2, ReportRollbackFailure)
	f2.ReasonCode = "rollback-flash-failed"
	d, _ := o2.HandleReport(f2)
	t.Logf("exhausted decision: kind=%s class=%s reason=%q", decisionName(d.Kind), className(d.Class), d.Reason)
	if d.Kind != DecisionRollbackFailed || d.Class != ClassManual {
		t.Fatalf("kind=%v class=%v, want rollback-failed/manual-intervention", d.Kind, d.Class)
	}
	if !strings.Contains(d.Reason, "manual intervention") {
		t.Fatalf("reason %q must call out manual intervention", d.Reason)
	}
	st := o2.State()
	if st.Devices["d1"].Phase != PhaseRollbackFailed || st.Phase != RolloutAborted {
		t.Fatalf("device=%v rollout=%v, want rollback-failed/aborted",
			st.Devices["d1"].Phase, st.Phase)
	}

	// 再重启：终态设备不再下发任何任务，已用预算保持。
	tr3 := &stubTransport{}
	o3 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr3})
	if err := o3.RecoverOrResume(); err != nil {
		t.Fatalf("resume after abort: %v", err)
	}
	if n := len(tr3.tasks()); n != 0 {
		t.Fatalf("dispatched %d tasks after budget exhausted, want 0", n)
	}
	if got := o3.State().Devices["d1"].RollbackAttempt; got != 2 {
		t.Fatalf("rollback attempt=%d after second restart, want 2", got)
	}
}

// TestRollbackBaselineMismatch：回滚结果必须落在记录的升级前基线上：
// 版本对不上或缺失都是可区分的无效原因，不消耗预算、保持在途等待重报。
func TestRollbackBaselineMismatch(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, failPlan())
	succeedDevice(t, o, "d1", 0, 1)
	inc := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("incompatible: %v", d.Kind)
	}
	if got := o.State().Devices["d1"].BaselineVer; got != oldVer {
		t.Fatalf("baseline=%q, want %q recorded from upgrade reports", got, oldVer)
	}

	// 1) 落回版本与基线对不上：无效，不计成功，保持在途。
	wrong := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	wrong.ToVer = "9.9.9"
	d, _ := o.HandleReport(wrong)
	t.Logf("mismatch decision: kind=%s class=%s reason=%q", decisionName(d.Kind), className(d.Class), d.Reason)
	if d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("mismatch: kind=%v class=%v, want failed-invalid/invalid", d.Kind, d.Class)
	}
	// 2) 干脆没说版本：同样无效。
	missing := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	missing.ToVer = ""
	if d, _ := o.HandleReport(missing); d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("missing version: kind=%v class=%v", d.Kind, d.Class)
	}
	rec := o.State().Devices["d1"]
	if rec.Phase != PhaseRollbackInflight || rec.RollbackAttempt != 1 {
		t.Fatalf("phase=%v attempt=%d, want still in-flight attempt 1 (budget untouched)",
			rec.Phase, rec.RollbackAttempt)
	}

	// 3) 重报正确基线版本：唯一一次成功判定。
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("correct baseline: %v", d.Kind)
	}
	if got := o.State().Devices["d1"].Phase; got != PhaseDone {
		t.Fatalf("phase=%v, want done", got)
	}
}

// TestRollbackBaselineUnknownCompat：基线未知（设备升级上报未携带 FromVer）时，
// 采纳首个非空回报版本为基线；空版本仍然无效。这是版本基线的兼容范围。
func TestRollbackBaselineUnknownCompat(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, failPlan())
	ack := upgradeFlow("d1", 0, 1, ReportAcknowledge, protoV1)
	ack.FromVer = ""
	ok := upgradeFlow("d1", 0, 1, ReportUpgradeSuccess, protoV1)
	ok.FromVer = ""
	if _, err := o.HandleReport(ack); err != nil {
		t.Fatal(err)
	}
	if _, err := o.HandleReport(ok); err != nil {
		t.Fatal(err)
	}
	inc := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if _, err := o.HandleReport(inc); err != nil {
		t.Fatal(err)
	}
	if got := o.State().Devices["d1"].BaselineVer; got != "" {
		t.Fatalf("baseline=%q, want unknown", got)
	}
	// 空版本：无效，保持在途。
	empty := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	empty.ToVer = ""
	if d, _ := o.HandleReport(empty); d.Kind != DecisionFailedInvalid {
		t.Fatalf("empty version with unknown baseline: %v", d.Kind)
	}
	// 首个非空版本：采纳为基线并计成功。
	good := rollbackFlow("d1", 0, 1, ReportRollbackSuccess)
	good.ToVer = "1.2.3"
	if d, _ := o.HandleReport(good); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("adopt baseline: %v", d.Kind)
	}
	if got := o.State().Devices["d1"].BaselineVer; got != "1.2.3" {
		t.Fatalf("baseline=%q, want adopted 1.2.3", got)
	}
}

// TestConcurrentRollbackReports：并发重复的回滚成功帧只产生一次成功判定，无数据竞争。
func TestConcurrentRollbackReports(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, failPlan())
	succeedDevice(t, o, "d1", 0, 1)
	inc := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if _, err := o.HandleReport(inc); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess))
			if err != nil {
				t.Errorf("report: %v", err)
				return
			}
			t.Logf("concurrent rollback report -> %s", decisionName(d.Kind))
		}()
	}
	wg.Wait()

	st := o.State()
	if st.Devices["d1"].Phase != PhaseDone {
		t.Fatalf("d1 phase=%v, want done", st.Devices["d1"].Phase)
	}
	n := 0
	for _, h := range st.History {
		if h.DeviceID == "d1" && h.Kind == DecisionRollbackSucceeded {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("rollback-succeeded verdicts=%d, want exactly 1", n)
	}
}
