package ota

import (
	"sync"
	"testing"
)

// inflightPlan：单批次 d1..d4，配额 2；阈值 0%、最小样本 1，
// 任何一次终态失败都会立即熔断。熔断时恰好有一台设备（d3）升级在途，
// 且在途升级会占掉一个回滚配额名额（升级+回滚合计不超过 2）。
func inflightPlan() Plan {
	p := testPlan()
	p.MaxFailRate = 0
	p.MaxRollbackRate = 0
	p.MinEvaluated = 1
	p.Batches = []BatchSpec{
		{Index: 0, Devices: []DeviceID{"d1", "d2", "d3", "d4"}, MaxInFlight: 2},
	}
	return p
}

// triggerWithInflightUpgrade 构造目标路径：d1 已升级；d2 不可兼容熔断；
// d3 升级任务在途（熔断时尚未回报最终结果）；d4 因配额从未放量。
func triggerWithInflightUpgrade(t *testing.T, o *Orchestrator) {
	t.Helper()
	// 初始按配额 2 下发 d1,d2。d1 升级成功释放名额 => d3 被补下发（在途）。
	succeedDevice(t, o, "d1", 0, 1)
	if st := o.State(); st.Devices["d3"].Phase != PhaseUpgradeInflight {
		t.Fatalf("setup: d3 phase=%v, want upgrade-inflight", st.Devices["d3"].Phase)
	}
	// d2 报不可兼容 => 熔断；此刻 d3 的升级任务仍在途。
	inc := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("d2 incompatible: %v", d.Kind)
	}
	st := o.State()
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, want rolling-back", st.Phase)
	}
	if st.Devices["d3"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d3 phase=%v, want upgrade-inflight at circuit-break moment",
			st.Devices["d3"].Phase)
	}
	if st.Devices["d4"].Phase != PhaseDone {
		t.Fatalf("d4 phase=%v, want done (never upgraded)", st.Devices["d4"].Phase)
	}
}

// TestRollbackWaitsForInflightUpgradeThenSuccess：
// 熔断时仍有设备在升级，波次不得因“恰好没有回滚在途/排队”而提前收敛；
// 该设备随后升级成功，必须按与其他设备相同的规则、同一条在途配额进入回滚队列；
// 回滚完成后波次才收敛。
func TestRollbackWaitsForInflightUpgradeThenSuccess(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, inflightPlan())
	triggerWithInflightUpgrade(t, o)

	// 熔断后：d3 升级在途占 1 个名额，d1 的回滚占另 1 个名额，合计恰好配额 2。
	s := o.Stats()
	t.Logf("right after trigger: %+v", s)
	if s.UpgradeInflight != 1 || s.RollbackInflight != 1 || s.Quota != 2 {
		t.Fatalf("stats=%+v, want upgrade=1 rollback=1 quota=2 (shared quota)", s)
	}
	if s.UpgradeInflight+s.RollbackInflight > s.Quota {
		t.Fatalf("in-flight upgrade+rollback %d exceeds quota %d",
			s.UpgradeInflight+s.RollbackInflight, s.Quota)
	}

	// 先让已放行回滚的 d1 落回基线。即便此刻没有任何回滚在途、也没有设备排队，
	// 只要 d3 还在升级在途，发布就必须停在回滚中，不能安全中止。
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback: %v", d.Kind)
	}
	st := o.State()
	t.Logf("after d1 restored with d3 still upgrading: phase=%s d3=%s",
		phaseName(st.Phase), devicePhaseName(st.Devices["d3"].Phase))
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, must stay rolling-back while d3 upgrade is unresolved", st.Phase)
	}
	s = o.Stats()
	t.Logf("stats while waiting on d3: %+v", s)
	if s.UpgradeInflight != 1 {
		t.Fatalf("UpgradeInflight=%d, want 1 (d3 unresolved)", s.UpgradeInflight)
	}
	if s.OutstandingOnNewFirmware != 1 {
		t.Fatalf("OutstandingOnNewFirmware=%d, want 1 (d3 upgrade result pending)",
			s.OutstandingOnNewFirmware)
	}

	// d3 随后上报升级成功：它已跑到新固件，必须被纳入回滚，不能停在新固件上无人处理。
	succeedDevice(t, o, "d3", 0, 1)
	st = o.State()
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, still rolling-back after d3 success", st.Phase)
	}
	if st.Devices["d3"].Phase != PhaseRollbackInflight {
		t.Fatalf("d3 phase=%v, want rollback-inflight (admitted under the same quota)",
			st.Devices["d3"].Phase)
	}
	s = o.Stats()
	t.Logf("stats after d3 success admitted to rollback: %+v", s)
	if s.UpgradeInflight+s.RollbackInflight > s.Quota {
		t.Fatalf("in-flight upgrade+rollback %d exceeds quota %d",
			s.UpgradeInflight+s.RollbackInflight, s.Quota)
	}
	if n := tr.countTask(taskID("rl-test-001", 0, "d3", TaskRollback, 1)); n != 1 {
		t.Fatalf("d3 rollback dispatched %d times, want 1", n)
	}

	// d3 落回基线 => 所有到过新固件的设备都已退回，波次才收敛、安全中止。
	if d, _ := o.HandleReport(rollbackFlow("d3", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d3 rollback: %v", d.Kind)
	}
	st = o.State()
	if st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted only after d3 restored", st.Phase)
	}
}

// TestRollbackInflightUpgradeInvalidThenFailure：熔断时升级在途的设备，
// 先收到无效失败帧：不得被当成“已离开新固件”，任务保持在途、波次继续等待；
// 随后收到有效（可重试）升级失败：回滚期不再重试升级，设备留旧版作为安全终态，
// 此时所有设备均已离开新固件，波次才收敛。
func TestRollbackInflightUpgradeInvalidThenFailure(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, inflightPlan())
	triggerWithInflightUpgrade(t, o)

	// d1 落回基线，此刻只剩 d3 一台升级在途（无回滚在途、无排队）。
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback: %v", d.Kind)
	}

	// d3 报一帧无效失败（目标版本张冠李戴）：保持升级在途，波次不得收敛。
	bad := upgradeFlow("d3", 0, 1, ReportUpgradeFailure, protoV1)
	bad.ReasonCode = "flash-failed"
	bad.ToVer = "9.9.9"
	d, _ := o.HandleReport(bad)
	t.Logf("invalid failure decision: kind=%s class=%s reason=%q",
		decisionName(d.Kind), className(d.Class), d.Reason)
	if d.Kind != DecisionFailedInvalid || d.Class != ClassInvalid {
		t.Fatalf("invalid failure: kind=%v class=%v", d.Kind, d.Class)
	}
	st := o.State()
	if st.Devices["d3"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d3 phase=%v, must remain upgrade-inflight after invalid report",
			st.Devices["d3"].Phase)
	}
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, invalid report must not converge the wave", st.Phase)
	}

	// d3 再报有效可重试失败：回滚期不重试升级，落安全终态（留旧版），波次收敛。
	fail := upgradeFlow("d3", 0, 1, ReportUpgradeFailure, protoV1)
	fail.ReasonCode = "download-timeout"
	if d, _ := o.HandleReport(fail); d.Kind != DecisionFailedRetryable {
		t.Fatalf("d3 retryable failure during rollback: %v", d.Kind)
	}
	st = o.State()
	if st.Devices["d3"].Phase != PhaseDone {
		t.Fatalf("d3 phase=%v, want done (stays on old firmware, no upgrade retry)",
			st.Devices["d3"].Phase)
	}
	if st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted after all devices left new firmware", st.Phase)
	}
}

// TestRollbackInflightUpgradeSurvivesRestart：熔断时仍在升级在途的设备，
// 在回滚波次中途重启后：等待关系原样保持——波次不得因重启而提前收敛；
// 在途升级任务以相同 TaskID 重放；设备随后升级成功仍被纳入回滚；
// 已确认回滚完成的设备不再下发、也不重复计数。
func TestRollbackInflightUpgradeSurvivesRestart(t *testing.T) {
	p := inflightPlan()
	store := NewMemoryStore()
	o1 := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o1.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	triggerWithInflightUpgrade(t, o1)

	// 第一次重启（波次中途）：d3 的在途升级任务与 d1 的在途回滚任务都必须以
	// 相同 TaskID 重放；波次仍是 rolling-back，绝不能在恢复时被判成已收敛。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume1: %v", err)
	}
	st := o2.State()
	t.Logf("after resume1: phase=%s d3=%s d1=%s",
		phaseName(st.Phase), devicePhaseName(st.Devices["d3"].Phase),
		devicePhaseName(st.Devices["d1"].Phase))
	if st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v after restart, must keep waiting on d3", st.Phase)
	}
	replayed := tr2.tasks()
	wantReplay := map[TaskID]bool{
		taskID("rl-test-001", 0, "d3", TaskUpgrade, 1):  false,
		taskID("rl-test-001", 0, "d1", TaskRollback, 1): false,
	}
	if len(replayed) != len(wantReplay) {
		t.Fatalf("resume replayed %d tasks %+v, want %d (d3 upgrade + d1 rollback)",
			len(replayed), replayed, len(wantReplay))
	}
	for _, tk := range replayed {
		if _, ok := wantReplay[tk.ID]; !ok {
			t.Fatalf("unexpected replay task %+v", tk)
		}
		wantReplay[tk.ID] = true
	}
	for id, seen := range wantReplay {
		if !seen {
			t.Fatalf("task %s not replayed after restart", id)
		}
	}

	// d1 在重启后落回基线；d3 仍在升级在途 => 波次仍不收敛。
	if d, _ := o2.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback after restart: %v", d.Kind)
	}
	if st := o2.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v, d3 still unresolved", st.Phase)
	}

	// 第二次重启：d1 已确认回滚完成，绝不能再被下发；只剩 d3 的升级任务重放。
	tr3 := &stubTransport{}
	o3 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr3})
	if err := o3.RecoverOrResume(); err != nil {
		t.Fatalf("resume2: %v", err)
	}
	replayed = tr3.tasks()
	if len(replayed) != 1 || replayed[0].ID != taskID("rl-test-001", 0, "d3", TaskUpgrade, 1) {
		t.Fatalf("resume2 replayed=%+v, want only d3 upgrade a1 (d1 done, no redispatch)", replayed)
	}
	if st := o3.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v after second restart, still waiting on d3", st.Phase)
	}

	// d3 升级成功 => 纳入回滚；完成回滚 => 波次收敛。
	succeedDevice(t, o3, "d3", 0, 1)
	if st := o3.State(); st.Devices["d3"].Phase != PhaseRollbackInflight {
		t.Fatalf("d3 phase=%v, want rollback-inflight after post-restart upgrade success",
			st.Devices["d3"].Phase)
	}
	if d, _ := o3.HandleReport(rollbackFlow("d3", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d3 rollback: %v", d.Kind)
	}
	st = o3.State()
	if st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted", st.Phase)
	}
	// 跨重启，d1 的回滚成功判定只能有一次。
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

// TestRollbackInflightUpgradeConcurrentReports：熔断时仍在升级在途的设备，
// 其升级成功帧被多个 goroutine 以乱序、重复方式并发喂入：
// 升级成功结论必须唯一（设备只被纳入一次回滚、只下发一次回滚任务），
// 波次最终仍正确收敛。-race 下不得出现数据竞争。
func TestRollbackInflightUpgradeConcurrentReports(t *testing.T) {
	p := inflightPlan()
	store := NewMemoryStore()
	tr := &stubTransport{}
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	triggerWithInflightUpgrade(t, o)
	if d, _ := o.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("d1 rollback: %v", d.Kind)
	}
	// 此刻仅 d3 一台升级在途，波次正挂在它的最终结果上。

	ack := upgradeFlow("d3", 0, 1, ReportAcknowledge, protoV1)
	ok := upgradeFlow("d3", 0, 1, ReportUpgradeSuccess, protoV1)
	rbOK := rollbackFlow("d3", 0, 1, ReportRollbackSuccess)

	// 多协程并发投喂：ACK、成功帧及其重复帧乱序到达。
	feed := []DeviceReport{ack, ok, ok, ok, ack}
	done := make(chan Decision, len(feed)*4)
	var wg sync.WaitGroup
	for round := 0; round < 4; round++ {
		for _, r := range feed {
			wg.Add(1)
			go func(rep DeviceReport) {
				defer wg.Done()
				d, err := o.HandleReport(rep)
				if err != nil {
					t.Errorf("report: %v", err)
					return
				}
				done <- d
			}(r)
		}
	}
	wg.Wait()
	close(done)
	for d := range done {
		t.Logf("concurrent d3 report -> %s task=%s", decisionName(d.Kind), d.TaskID)
	}

	st := o.State()
	if st.Devices["d3"].Phase != PhaseRollbackInflight {
		t.Fatalf("d3 phase=%v, want rollback-inflight: exactly one upgrade success admitted it",
			st.Devices["d3"].Phase)
	}
	nSucceeded := 0
	for _, h := range st.History {
		if h.DeviceID == "d3" && h.Kind == DecisionSucceeded {
			nSucceeded++
		}
	}
	if nSucceeded != 1 {
		t.Fatalf("d3 upgrade-succeeded verdicts=%d under concurrency, want exactly 1", nSucceeded)
	}
	if n := tr.countTask(taskID("rl-test-001", 0, "d3", TaskRollback, 1)); n != 1 {
		t.Fatalf("d3 rollback dispatched %d times, want exactly 1", n)
	}

	// 回滚成功帧再并发重复：只能有一次 rollback-succeeded，波次恰好收敛一次。
	var wg2 sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			if _, err := o.HandleReport(rbOK); err != nil {
				t.Errorf("rollback report: %v", err)
			}
		}()
	}
	wg2.Wait()
	st = o.State()
	if st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted", st.Phase)
	}
	nRB := 0
	nAbort := 0
	for _, h := range st.History {
		if h.DeviceID == "d3" && h.Kind == DecisionRollbackSucceeded {
			nRB++
		}
		if h.Kind == DecisionAborted {
			nAbort++
		}
	}
	if nRB != 1 {
		t.Fatalf("d3 rollback-succeeded verdicts=%d, want 1", nRB)
	}
	if nAbort != 1 {
		t.Fatalf("abort decisions=%d, want exactly 1 convergence", nAbort)
	}
}
