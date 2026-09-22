package ota

import (
	"errors"
	"testing"
)

// TestResumeDoesNotRedispatchFinished：批次0一台已成功、一台在途时“重启”，
// 已确认设备不得重复下发，在途设备以相同 TaskID 重放。
func TestResumeDoesNotRedispatchFinished(t *testing.T) {
	p := testPlan()
	store := NewMemoryStore()
	tr1 := &stubTransport{}
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr1})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	// d1 成功，d2 只 ACK（仍在途），然后“进程退出”。
	succeedDevice(t, o, "d1", 0, 1)
	if d, err := o.HandleReport(upgradeFlow("d2", 0, 1, ReportAcknowledge, protoV1)); err != nil {
		t.Fatal(err)
	} else if d.Kind != DecisionAcked {
		t.Fatalf("d2 ack: %v", d.Kind)
	}

	// 新进程：新编排器 + 新传输，但复用持久化存储。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	st := o2.State()
	if st.CurrentBatch != 0 || st.Devices["d1"].Phase != PhaseUpgraded {
		t.Fatalf("resumed wrong state: batch=%d d1=%v", st.CurrentBatch, st.Devices["d1"].Phase)
	}
	// 恢复后只应重放 d2 的在途任务；d1 绝不能再下发。
	resumed := tr2.tasks()
	if len(resumed) != 1 {
		t.Fatalf("resumed dispatches=%d, want 1 (d2 in-flight replay)", len(resumed))
	}
	if resumed[0].Device != "d2" {
		t.Fatalf("resumed device=%s, want d2", resumed[0].Device)
	}
	wantID := taskID("rl-test-001", 0, "d2", TaskUpgrade, 1)
	if resumed[0].ID != wantID {
		t.Fatalf("resumed task=%s, want same deterministic id %s", resumed[0].ID, wantID)
	}

	// d2 在“重启后”完成：批次0 过门，进入批次1；d1 仍然只在历史中成功一次。
	if d, err := o2.HandleReport(upgradeFlow("d2", 0, 1, ReportUpgradeSuccess, protoV1)); err != nil || d.Kind != DecisionSucceeded {
		t.Fatalf("d2 after resume: %v %v", d.Kind, err)
	}
	st = o2.State()
	if st.CurrentBatch != 1 {
		t.Fatalf("batch=%d, want 1", st.CurrentBatch)
	}
	successCount := 0
	for _, d := range st.History {
		if d.DeviceID == "d1" && d.Kind == DecisionSucceeded {
			successCount++
		}
	}
	if successCount != 1 {
		t.Fatalf("d1 success verdicts=%d across restart, want 1", successCount)
	}
}

// TestResumeRollbackWave：回滚进行中重启，升级成功但尚未回滚完成的设备
// 会被补发相同 TaskID 的回滚任务，收敛后安全中止。
func TestResumeRollbackWave(t *testing.T) {
	p := failPlan()
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	succeedDevice(t, o, "d1", 0, 1)
	// d2 不可兼容 => 50% 失败率熔断，d1 进入回滚在途。
	inc := upgradeFlow("d2", 0, 1, ReportUpgradeFailure, Semver{Major: 2})
	inc.ReasonCode = "protocol-too-new"
	if d, _ := o.HandleReport(inc); d.Kind != DecisionFailedIncompatible {
		t.Fatalf("incompatible: %v", d.Kind)
	}
	if st := o.State(); st.Phase != RolloutRollingBack {
		t.Fatalf("phase=%v", st.Phase)
	}

	// 重启：d1 回滚任务必须以相同 TaskID 重放。
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	tasks := tr2.tasks()
	if len(tasks) != 1 || tasks[0].Device != "d1" || tasks[0].Kind != TaskRollback {
		t.Fatalf("resumed tasks=%+v, want single d1 rollback", tasks)
	}
	if tasks[0].ID != taskID("rl-test-001", 0, "d1", TaskRollback, 1) {
		t.Fatalf("rollback task id changed across restart: %s", tasks[0].ID)
	}
	if d, err := o2.HandleReport(rollbackFlow("d1", 0, 1, ReportRollbackSuccess)); err != nil || d.Kind != DecisionRollbackSucceeded {
		t.Fatalf("rollback after resume: %v %v", d.Kind, err)
	}
	if st := o2.State(); st.Phase != RolloutAborted {
		t.Fatalf("phase=%v, want aborted after rollback wave", st.Phase)
	}
}

// TestResumeRejectsPlanMismatch：持久化计划指纹不一致必须拒绝恢复。
func TestResumeRejectsPlanMismatch(t *testing.T) {
	p := testPlan()
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	changed := testPlan()
	changed.Target = "9.9.9"
	o2 := NewOrchestrator(Config{Plan: changed, Store: store, Transport: &stubTransport{}})
	if err := o2.RecoverOrResume(); err != ErrPlanChanged {
		t.Fatalf("err=%v, want ErrPlanChanged", err)
	}
}

// TestResumeRejectsBatchRollback：批次位点回退必须明确拒绝，而不是从中间继续。
func TestResumeRejectsBatchRollback(t *testing.T) {
	// 1) 直接校验位点规则：请求起点晚于已持久化位点即拒绝。
	st := &RolloutState{
		RolloutID:    "x",
		CurrentBatch: 2,
		Batches:      map[BatchIndex]*BatchState{2: {Index: 2}},
		Devices:      map[DeviceID]*DeviceRecord{},
	}
	if err := CheckNoRewind(st, 0); err != nil {
		t.Fatalf("requested start 0 at persisted 2 must be allowed (continue): %v", err)
	}
	if err := CheckNoRewind(st, 2); err != nil {
		t.Fatalf("requested start 2 at persisted 2 must be allowed: %v", err)
	}
	if err := CheckNoRewind(st, 3); !errors.Is(err, ErrBatchIndexRolledBack) {
		t.Fatalf("err=%v, want ErrBatchIndexRolledBack", err)
	}

	// 2) 端到端：持久化位点前进到批次 2 后可以继续；任何要求从更后批次启动的请求被拒。
	p := testPlan()
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	s := o.State()
	s.CurrentBatch = 2
	s.Batches[0].Phase = BatchPassed
	s.Batches[1].Phase = BatchPassed
	s.Batches[2].Phase = BatchActive
	_ = store.Save(&s) // 合法：位点前进到 2
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume at batch 2: %v", err)
	}
	// 已恢复到批次 2，之后任何“从批次 3 重新开始”的位点请求都必须被拒绝。
	if err := CheckNoRewind(o2.StatePtr(), 3); !errors.Is(err, ErrBatchIndexRolledBack) {
		t.Fatalf("err=%v, want ErrBatchIndexRolledBack", err)
	}
}

// TestResumeAfterCompleted：已完成的发布重启后仍是完成态，不再下发任何任务。
func TestResumeAfterCompleted(t *testing.T) {
	p := testPlan()
	store := NewMemoryStore()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	for b, spec := range p.Batches {
		for _, id := range spec.Devices {
			succeedDevice(t, o, id, BatchIndex(b), 1)
		}
	}
	if st := o.State(); st.Phase != RolloutCompleted {
		t.Fatalf("phase=%v", st.Phase)
	}
	tr2 := &stubTransport{}
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("resume completed: %v", err)
	}
	if len(tr2.tasks()) != 0 {
		t.Fatalf("completed rollout dispatched %d tasks after restart", len(tr2.tasks()))
	}
}
