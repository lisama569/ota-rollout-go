package ota

import (
	"testing"
)

// succeedDevice 发送 ACK + 成功，模拟正常设备完成一次升级。
func succeedDevice(t *testing.T, o *Orchestrator, id DeviceID, batch BatchIndex, attempt int) {
	t.Helper()
	ack := upgradeFlow(id, batch, attempt, ReportAcknowledge, protoV1)
	ok := upgradeFlow(id, batch, attempt, ReportUpgradeSuccess, protoV1)
	if d, err := o.HandleReport(ack); err != nil || d.Kind != DecisionAcked {
		t.Fatalf("%s ack: d=%v err=%v", id, d.Kind, err)
	}
	if d, err := o.HandleReport(ok); err != nil || d.Kind != DecisionSucceeded {
		t.Fatalf("%s success: d=%v err=%v", id, d.Kind, err)
	}
}

// TestHappyPathBatchesAdvance：只有当前批次全部健康后才进入下一批，最终完成。
func TestHappyPathBatchesAdvance(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, testPlan())

	// 初始即应按批次 0 配额（2）把两台设备全部下发。
	tasks := tr.tasks()
	if len(tasks) != 2 {
		t.Fatalf("initial dispatch = %d tasks, want 2", len(tasks))
	}
	for _, tk := range tasks {
		if tk.Batch != 0 || tk.Kind != TaskUpgrade {
			t.Fatalf("unexpected initial task: %+v", tk)
		}
	}

	succeedDevice(t, o, "d1", 0, 1)
	if st := o.State(); st.CurrentBatch != 0 {
		t.Fatalf("batch advanced with only 1/2 success: %d", st.CurrentBatch)
	}
	succeedDevice(t, o, "d2", 0, 1)

	st := o.State()
	if st.CurrentBatch != 1 || st.Phase != RolloutRunning {
		t.Fatalf("after batch0: batch=%d phase=%d, want batch=1 running", st.CurrentBatch, st.Phase)
	}
	// 进入批次 1 后，新批次设备被下发，已完成设备不得重复下发。
	if len(tr.tasks()) != 2+2 { // 批次1 配额 2
		t.Fatalf("after advance tasks=%d, want 4", len(tr.tasks()))
	}

	succeedDevice(t, o, "d3", 1, 1)
	succeedDevice(t, o, "d4", 1, 1)
	// 批次1 还有 d5 未终态：不能推进。
	if st := o.State(); st.CurrentBatch != 1 {
		t.Fatalf("advanced early: %d", st.CurrentBatch)
	}
	// d5 在途（d3/d4 完成释放配额），应已被补下发。
	if st := o.State(); st.Devices["d5"].Phase != PhaseUpgradeInflight {
		t.Fatalf("d5 phase=%v, want inflight", st.Devices["d5"].Phase)
	}
	succeedDevice(t, o, "d5", 1, 1)

	if st := o.State(); st.CurrentBatch != 2 {
		t.Fatalf("batch after 1 = %d, want 2", st.CurrentBatch)
	}
	succeedDevice(t, o, "d6", 2, 1)
	succeedDevice(t, o, "d7", 2, 1)

	st = o.State()
	if st.Phase != RolloutCompleted {
		t.Fatalf("phase=%d, want completed", st.Phase)
	}
	// 已完成设备绝不在任务列表中重复出现。
	counts := map[TaskID]int{}
	for _, tk := range tr.tasks() {
		counts[tk.ID]++
	}
	for id, n := range counts {
		if n > 1 {
			t.Fatalf("task %s dispatched %d times", id, n)
		}
	}
	// 审计历史必须包含批次推进与完成事件。
	var sawAdvance, sawComplete bool
	for _, d := range st.History {
		if d.Kind == DecisionBatchAdvanced {
			sawAdvance = true
		}
		if d.Kind == DecisionRolloutCompleted {
			sawComplete = true
		}
	}
	if !sawAdvance || !sawComplete {
		t.Fatalf("history missing events: advance=%v complete=%v", sawAdvance, sawComplete)
	}
}

// TestQuotaEnforced：在途数量不得超过当前批次配额，单设备至多一个在途任务。
func TestQuotaEnforced(t *testing.T) {
	p := testPlan()
	p.Batches[0] = BatchSpec{Index: 0, Devices: []DeviceID{"d1", "d2"}, MaxInFlight: 1}
	o, tr, _ := newTestOrchestrator(t, p)
	if n := len(tr.tasks()); n != 1 {
		t.Fatalf("with quota 1, dispatched %d, want 1", n)
	}
	// d1 只 ACK 未终态：仍占在途名额，d2 不得被下发。
	if d, err := o.HandleReport(upgradeFlow("d1", 0, 1, ReportAcknowledge, protoV1)); err != nil || d.Kind != DecisionAcked {
		t.Fatalf("ack: %v %v", d.Kind, err)
	}
	if n := len(tr.tasks()); n != 1 {
		t.Fatalf("after ack dispatched %d, want still 1 (ack holds slot)", n)
	}
	// d1 成功后释放名额，d2 才被下发。
	if d, err := o.HandleReport(upgradeFlow("d1", 0, 1, ReportUpgradeSuccess, protoV1)); err != nil || d.Kind != DecisionSucceeded {
		t.Fatalf("success: %v %v", d.Kind, err)
	}
	if n := len(tr.tasks()); n != 2 {
		t.Fatalf("after d1 success dispatched %d, want 2", n)
	}
	if tr.tasks()[1].Device != "d2" {
		t.Fatalf("second task device=%s, want d2", tr.tasks()[1].Device)
	}
}
