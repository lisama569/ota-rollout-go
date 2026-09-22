package ota

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentDuplicateReports：多 goroutine 对同一设备并发投递成功帧与 ACK 帧
// （其中夹杂大量重复帧）。设备只能成功一次，终态唯一，任务不重复计数，无数据竞争。
//
// 说明：按“首帧终态优先”语义，同一任务互相矛盾的首帧（成功 vs 失败）谁先到达谁生效，
// 那是合法的设备行为二义性，由独立的顺序测试覆盖；本测试只保证重复帧不产生重复结论。
func TestConcurrentDuplicateReports(t *testing.T) {
	o, tr, _ := newTestOrchestrator(t, testPlan())

	const workers = 32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			kind := ReportAcknowledge
			if w%2 == 0 {
				kind = ReportUpgradeSuccess
			}
			r := upgradeFlow("d1", 0, 1, kind, protoV1)
			d, err := o.HandleReport(r)
			if err != nil {
				t.Errorf("worker %d: %v", w, err)
				return
			}
			t.Logf("worker=%d report=%s decision=%s reason=%q", w, reportKindName(r.Kind), decisionName(d.Kind), d.Reason)
		}(w)
	}
	wg.Wait()

	st := o.State()
	if st.Devices["d1"].Phase != PhaseUpgraded {
		t.Fatalf("d1 phase=%v, want upgraded", st.Devices["d1"].Phase)
	}
	// 成功判定在整个历史中只能出现一次。
	successCount := 0
	for _, d := range st.History {
		if d.DeviceID == "d1" && d.Kind == DecisionSucceeded {
			successCount++
		}
	}
	if successCount != 1 {
		t.Fatalf("succeeded verdict count=%d, want exactly 1", successCount)
	}
	// 首次升级任务全程只下发一次（没有任何重试/重复下发）。
	if n := tr.countTask(taskID("rl-test-001", 0, "d1", TaskUpgrade, 1)); n != 1 {
		t.Fatalf("d1 upgrade#1 dispatched %d times, want 1", n)
	}
}

// TestOutOfOrderSuccessBeforeAck：成功帧先于 ACK 到达，之后迟到的 ACK
// 只能得到 duplicate，终态仍然是成功。
func TestOutOfOrderSuccessBeforeAck(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, testPlan())
	ok := upgradeFlow("d1", 0, 1, ReportUpgradeSuccess, protoV1)
	ack := upgradeFlow("d1", 0, 1, ReportAcknowledge, protoV1)
	if d, err := o.HandleReport(ok); err != nil || d.Kind != DecisionSucceeded {
		t.Fatalf("early success: %v %v", d.Kind, err)
	}
	if d, _ := o.HandleReport(ack); d.Kind != DecisionDuplicate {
		t.Fatalf("late ack: kind=%v, want duplicate", d.Kind)
	}
	if st := o.State(); st.Devices["d1"].Phase != PhaseUpgraded {
		t.Fatalf("phase=%v", st.Devices["d1"].Phase)
	}
}

// TestStaleUnknownTaskID：不属于任何在途/历史任务的帧判无效；旧尝试的帧判 stale。
func TestStaleUnknownTaskID(t *testing.T) {
	o, _, _ := newTestOrchestrator(t, testPlan())
	random := upgradeFlow("d1", 0, 1, ReportUpgradeSuccess, protoV1)
	random.TaskID = "totally-bogus-id"
	if d, _ := o.HandleReport(random); d.Kind != DecisionFailedInvalid {
		t.Fatalf("bogus task: %v, want failed-invalid", d.Kind)
	}
	// 设备保持在途。
	if st := o.State(); st.Devices["d1"].Phase != PhaseUpgradeInflight {
		t.Fatalf("phase=%v", st.Devices["d1"].Phase)
	}

	// d1 从 attempt1 失败重试到 attempt2，attempt1 的迟到帧必须是 stale。
	f1 := upgradeFlow("d1", 0, 1, ReportUpgradeFailure, protoV1)
	f1.ReasonCode = "download-timeout"
	if d, _ := o.HandleReport(f1); d.Kind != DecisionRetried {
		t.Fatalf("retry: %v", d.Kind)
	}
	late := upgradeFlow("d1", 0, 1, ReportUpgradeSuccess, protoV1)
	if d, _ := o.HandleReport(late); d.Kind != DecisionStale {
		t.Fatalf("late old-attempt success: %v, want stale", d.Kind)
	}
}

// TestDeterministicVerdictUnderShuffledReports：同一批上报以多种打乱顺序喂入，
// 终态结论必须完全一致（唯一可复现）。
func TestDeterministicVerdictUnderShuffledReports(t *testing.T) {
	frames := func() []DeviceReport {
		var fs []DeviceReport
		for _, id := range []DeviceID{"d1", "d2"} {
			fs = append(fs,
				upgradeFlow(id, 0, 1, ReportAcknowledge, protoV1),
				upgradeFlow(id, 0, 1, ReportUpgradeSuccess, protoV1),
				upgradeFlow(id, 0, 1, ReportUpgradeSuccess, protoV1), // 重复
			)
		}
		return fs
	}

	// 三种确定性排列：正序、逆序、交叉。
	orders := map[string][]int{
		"forward": {0, 1, 2, 3, 4, 5},
		"reverse": {5, 4, 3, 2, 1, 0},
		"cross":   {0, 3, 1, 4, 2, 5},
	}
	summaries := map[string]string{}
	for name, order := range orders {
		o, _, _ := newTestOrchestrator(t, testPlan())
		fs := frames()
		for _, i := range order {
			d, err := o.HandleReport(fs[i])
			if err != nil {
				t.Fatalf("%s[%d]: %v", name, i, err)
			}
			t.Logf("order=%s feed=%d device=%s report=%s -> %s",
				name, i, fs[i].DeviceID, reportKindName(fs[i].Kind), decisionName(d.Kind))
		}
		st := o.State()
		summaries[name] = fmt.Sprintf("phase=%d batch=%d d1=%v d2=%v",
			st.Phase, st.CurrentBatch,
			st.Devices["d1"].Phase, st.Devices["d2"].Phase)
	}
	if summaries["forward"] != summaries["reverse"] ||
		summaries["forward"] != summaries["cross"] {
		t.Fatalf("non-deterministic outcomes: %+v", summaries)
	}
	t.Logf("all orders converged: %s", summaries["forward"])
}
