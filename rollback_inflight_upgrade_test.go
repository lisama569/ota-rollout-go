package ota

import (
	"sync"
	"testing"
)

// inflightWavePlan：批次0（d1，配额1）先全绿；批次1（d2,d3，配额2）中
// d2 不可兼容触发熔断时，d3 的升级任务还在途（终态未回报）。
// 回滚配额 = 批次1配额 = 2。
func inflightWavePlan() Plan {
	p := testPlan()
	p.MinEvaluated = 1
	p.Batches = []BatchSpec{
		{Index: 0, Devices: []DeviceID{"d1"}, MaxInFlight: 1},
		{Index: 1, Devices: []DeviceID{"d2", "d3"}, MaxInFlight: 2},
	}
	return p
}

// driveToInflightWave 把发布驱动到“熔断时 d3 仍在升级途中”的现场：
// d1 已升级（批次0通过），d2 不可兼容触发熔断，d3 升级在途。
// 触发后 d1 按配额进入回滚在途，d3 的升级任务仍未回报。
func driveToInflightWave(t *testing.T, o *Orchestrator) {
	t.Helper()
	succeedDevice(t, o, "d1", 0, 1)
	inc := upgradeFlow("d2", 1, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("d2 incompatible: %v", d.Kind)
	}
	st := o.State()
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, want rolling-back", st.Phase)
	}
	if got := st.Devices["d3"].Phase; got != PhaseUpgradeInflight {
		t.Fatalf("d3 phase=%v, want upgrade in-flight at circuit-break moment", got)
	}
	t.Logf("circuit breaker fired while d3 upgrade in-flight; d1 queued for rollback")
}

// TestRollbackWaveWaitsForInflightUpgrade：熔断时仍有设备在升级途中，
// 回滚波次不得因“此刻没有回滚在途/排队”而提前收敛：
//  1. d1 回滚完成后，d3 仍可能在新固件上 => 发布必须保持回滚中；
//  2. d3 的无效上报不改变等待关系；
//  3. d3 升级成功后按同一配额排进回滚队列，重复成功帧只算一次；
//  4. d3 落回基线后波次才收敛，发布安全中止。
func TestRollbackWaveWaitsForInflightUpgrade(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, inflightWavePlan())
	driveToInflightWave(t, o)

	// 触发瞬间：d3 升级在途 + d1 回滚在途，合计 2 = 配额，不超。
	s := o.Stats()
	t.Logf("at trigger: %+v", s)
	if s.UpgradeInflight != 1 || s.RollbackInflight != 1 || s.UpgradeInflight+s.RollbackInflight > s.Quota {
		t.Fatalf("stats=%+v, want upgrade=1 rollback=1 and combined <= quota", s)
	}

	// d1 落回基线 => 回滚在途归零、排队归零，但 d3 升级未回报：
	// 这正是旧实现误判“已安全中止”的时刻，现在必须保持回滚中。
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback: %v", d.Kind)
	}
	s = o.Stats()
	t.Logf("after d1 restored, d3 still upgrading: %+v", s)
	if st := o.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, want rolling-back while d3 upgrade outcome unknown", st.Phase)
	}

	// d3 的无效失败帧（目标版本张冠李戴）：不计终态、不消耗重试，仍在途 => 仍不收敛。
	bad := upgradeFlow("d3", 1, 1, ReportUpgradeFailure, protoV1)
	bad.ToVer = "9.9.9"
	d, _ := o.HandleReport(bad)
	t.Logf("d3 invalid report -> kind=%s class=%s reason=%q", decisionName(d.Kind), className(d.Class), d.Reason)
	if d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("d3 invalid: kind=%v class=%v", d.Kind, d.Class)
	}
	if st := o.State(); st.Phase != RolloutRollingBack || st.Devices["d3"].Phase != PhaseUpgradeInflight {
		t.Fatalf("phase=%v d3=%v, want rolling-back with d3 still in-flight",
			st.Phase, st.Devices["d3"].Phase)
	}

	// d3 升级成功：已到新固件 => 与其他设备同规则排进回滚队列，按配额放行。
	ok := upgradeFlow("d3", 1, 1, ReportUpgradeSuccess, protoV1)
	if d, _ := o.HandleReport(ok); d.Kind != DecisionSucceeded {
		t.Fatalf("d3 late success: %v", d.Kind)
	}
	rbs := rollbackTasks(tr)
	if len(rbs) != 2 || rbs[1].Device != "d3" || rbs[1].Attempt != 1 || rbs[1].Target != oldVer {
		t.Fatalf("rollback tasks=%+v, want d3 attempt 1 targeting baseline %q", rbs, oldVer)
	}
	if st := o.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, want rolling-back while d3 rollback in-flight", st.Phase)
	}

	// 重复的成功帧：属于已完结的旧升级任务，只能得到 stale（不变更状态），
	// 不得再触发一次回滚下发。
	dup := upgradeFlow("d3", 1, 1, ReportUpgradeSuccess, protoV1)
	if d, _ := o.HandleReport(dup); d.Kind != DecisionStale {
		t.Fatalf("d3 duplicate success: %v, want stale", d.Kind)
	}
	if n := tr.countTask(taskID("rl-test-001", 1, "d3", TaskRollback, 1)); n != 1 {
		t.Fatalf("d3 rollback dispatched %d times, want 1", n)
	}

	// d3 落回基线 => 再没有任何设备可能在新固件上，波次收敛、安全中止。
	if d, _ := o.HandleReport(rollbackFlow("d3", 1, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d3 rollback: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted only after every device left new firmware", st.Phase)
	}
	t.Logf("wave converged only after d3 restored to baseline; rollout safely aborted")
}

// TestRollbackWaveRestartWaitsForInflightUpgrade：回滚波次中途（含重启）都要
// 等到“熔断时还在升级的设备”有最终结论才收口：
//  1. d1 已回滚完成、d3 升级在途时重启 => 恢复后仍是回滚中，d3 升级任务按同 TaskID 重放；
//  2. d3 升级成功后进入回滚，再次重启 => 回滚任务按同 TaskID 重放，不提前收敛；
//  3. d3 落回基线后才中止；已完成的设备重启后不再被下发。
func TestRollbackWaveRestartWaitsForInflightUpgrade(t *testing.T) {
	p := inflightWavePlan()
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	driveToInflightWave(t, o)
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback: %v", d.Kind)
	}
	t.Logf("restarting with d1 restored and d3 upgrade still in-flight")

	// 第一次重启：唯一的未决设备是 d3（升级在途）。恢复后不得判收敛，
	// d3 的升级任务以相同 TaskID 重放，d1 不再被下发。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume1: %v", err)
	}
	if st := o2.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v after resume, want rolling-back (d3 outcome unknown)", st.Phase)
	}
	tasks := tr2.tasks()
	if len(tasks) != 1 || tasks[0].ID != taskID("rl-test-001", 1, "d3", TaskUpgrade, 1) {
		t.Fatalf("resume1 dispatched=%+v, want single replay of d3 upgrade a1", tasks)
	}

	// d3 升级成功 => 排进回滚队列并按配额放行。
	if d, _ := o2.HandleReport(upgradeFlow("d3", 1, 1, ReportUpgradeSuccess, protoV1)); d.Kind != DecisionSucceeded {
		t.Fatalf("d3 late success: %v", d.Kind)
	}
	if n := tr2.countTask(taskID("rl-test-001", 1, "d3", TaskRollback, 1)); n != 1 {
		t.Fatalf("d3 rollback dispatched %d times, want 1", n)
	}
	t.Logf("d3 succeeded after restart and was admitted to rollback queue; restarting again mid-rollback")

	// 第二次重启：d3 回滚在途 => 同 TaskID 重放，仍不收敛。
	tr3 := &stubTransport{}
	o3 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr3})
	if err := o3.RecoverOrResume(); err != nil {
		t.Fatalf("resume2: %v", err)
	}
	tasks = tr3.tasks()
	if len(tasks) != 1 || tasks[0].ID != taskID("rl-test-001", 1, "d3", TaskRollback, 1) {
		t.Fatalf("resume2 dispatched=%+v, want single replay of d3 rollback a1", tasks)
	}
	if st := o3.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v after resume2, want rolling-back", st.Phase)
	}

	// d3 落回基线 => 波次收敛、安全中止。
	if d, _ := o3.HandleReport(rollbackFlow("d3", 1, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d3 rollback: %v", d.Kind)
	}
	if st := o3.State(); st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted after d3 restored", st.Phase)
	}

	// 第三次重启：所有设备均已离开新固件 => 不再下发任何任务。
	tr4 := &stubTransport{}
	o4 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr4})
	if err := o4.RecoverOrResume(); err != nil {
		t.Fatalf("resume3: %v", err)
	}
	if n := len(tr4.tasks()); n != 0 {
		t.Fatalf("dispatched %d tasks after wave converged, want 0", n)
	}
	// d3 的 rollback-succeeded 判定跨多次重启只能有一次。
	n := 0
	for _, h := range o4.State().History {
		if h.DeviceID == "d3" && h.Kind == DecisionRollbackSucceeded {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("d3 rollback-succeeded verdicts=%d across restarts, want 1", n)
	}
	t.Logf("wave converged after d3 restored; converged devices never re-dispatched across restarts")
}

// TestConcurrentInflightUpgradeReports：熔断后并发投递“升级在途设备”的成功帧
// （夹杂重复），成功判定与回滚下发都只能有一次，无数据竞争。
func TestConcurrentInflightUpgradeReports(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, inflightWavePlan())
	driveToInflightWave(t, o)

	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			d, err := o.HandleReport(upgradeFlow("d3", 1, 1, ReportUpgradeSuccess, protoV1))
			if err != nil {
				t.Errorf("worker %d: %v", w, err)
				return
			}
			t.Logf("worker=%d d3 success report -> %s", w, decisionName(d.Kind))
		}(w)
	}
	wg.Wait()

	st := o.State()
	n := 0
	for _, h := range st.History {
		if h.DeviceID == "d3" && h.Kind == DecisionSucceeded {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("d3 succeeded verdicts=%d, want exactly 1", n)
	}
	if got := st.Devices["d3"].Phase; got != PhaseRollbackInflight && got != PhaseUpgraded {
		t.Fatalf("d3 phase=%v, want queued or rollback in-flight", got)
	}
	if c := tr.countTask(taskID("rl-test-001", 1, "d3", TaskRollback, 1)); c != 1 {
		t.Fatalf("d3 rollback dispatched %d times, want 1", c)
	}

	// 收尾：d1、d3 依次落回基线 => 收敛中止。
	for _, id := range []DeviceID{"d1", "d3"} {
		batch := BatchIndex(0)
		if id == "d3" {
			batch = 1
		}
		if d, _ := o.HandleReport(rollbackFlow(id, batch, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
			t.Fatalf("%s rollback: %v", id, d.Kind)
		}
	}
	if st := o.State(); st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted", st.Phase)
	}
}
