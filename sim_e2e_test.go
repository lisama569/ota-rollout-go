package ota

import (
	"context"
	"testing"
	"time"
)

// buildSim 构造两批次发布：
//
//	批次0（4台，配额2）：成功 / 先失败后成功（重复帧）/ 不可兼容 / 畸形首报后成功；
//	批次1（3台，配额3）：成功 / 成功 / 成功。
//
// 批次0 在 2 个终态样本时若出现 1 台不可兼容（50% 失败率，阈值25%）即熔断回滚，
// 因此该场景预期以“安全中止”结束，用于演练回滚链路。
func buildSimPlan() Plan {
	return Plan{
		RolloutID:       "rl-sim-001",
		Target:          "2.0.0",
		Protocol:        Semver{Major: 1},
		MaxAttempts:     3,
		MaxFailRate:     25,
		MaxRollbackRate: 25,
		MinEvaluated:    2,
		Batches: []BatchSpec{
			{Index: 0, Devices: []DeviceID{"s1", "s2", "s3", "s4"}, MaxInFlight: 2},
			{Index: 1, Devices: []DeviceID{"s5", "s6", "s7"}, MaxInFlight: 3},
		},
	}
}

func simDevices() []*SimDevice {
	return []*SimDevice{
		{ID: "s1", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess, ExtraDuplicates: 1, OutOfOrder: true},
		{ID: "s2", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehRetryThenSuccess, FailBefore: 1, ExtraDuplicates: 1},
		{ID: "s3", Protocol: Semver{Major: 2}, CurrentVer: "1.0.0", Behavior: BehIncompatible},
		{ID: "s4", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess, MalformedFirst: true},
		{ID: "s5", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess},
		{ID: "s6", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess},
		{ID: "s7", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess},
	}
}

func newSimWith(p Plan, devs []*SimDevice) *SimTransport {
	tr := NewSimTransport().WithJitter(3 * time.Millisecond)
	for _, d := range devs {
		tr.AddDevice(d)
	}
	return tr
}

// TestSimE2ERollbackWave：端到端演练失败熔断 -> 已升级设备回滚 -> 安全中止。
func TestSimE2ERollbackWave(t *testing.T) {
	p := buildSimPlan()
	store := NewMemoryStore()
	tr := newSimWith(p, simDevices())
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr})

	ctx, cancel := context.WithCancel(context.Background())
	trDone := make(chan struct{})
	go func() { tr.Run(ctx); close(trDone) }()

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("e2e timed out")
	}
	cancel()
	<-trDone

	st := o.State()
	t.Logf("final phase=%s batch=%d", phaseName(st.Phase), st.CurrentBatch)
	if st.Phase != RolloutAborted {
		t.Fatalf("phase=%s, want aborted after incompatible device broke the gate", phaseName(st.Phase))
	}
	if st.Devices["s3"].Phase != PhaseIncompatible {
		t.Fatalf("s3 phase=%s, want incompatible", devicePhaseName(st.Devices["s3"].Phase))
	}
	// 没有任何设备停留在“新固件但未回滚”的状态。
	for id, d := range st.Devices {
		if d.Phase == PhaseUpgraded || d.Phase == PhaseRollbackInflight {
			t.Fatalf("device %s left at phase %s after rollback wave", id, devicePhaseName(d.Phase))
		}
	}
	// 批次1设备不得被放量。
	for _, id := range []DeviceID{"s5", "s6", "s7"} {
		if st.Batches[1].Dispatched[id] {
			t.Fatalf("device %s dispatched after gate failure", id)
		}
	}
}

// happyDevices 返回全成功路径的模拟设备（含重复、乱序、一次重试等扰动）。
func happyDevices() []*SimDevice {
	return []*SimDevice{
		{ID: "h1", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess, ExtraDuplicates: 1},
		{ID: "h2", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess, ExtraDuplicates: 1},
		{ID: "h3", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess, OutOfOrder: true},
		{ID: "h4", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess},
		{ID: "h5", Protocol: Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: BehSuccess},
	}
}

// TestSimE2EHappyPathWithRestart：全成功场景，但在中途重启进程，
// 验证恢复后续跑完成、无重复下发。
func TestSimE2EHappyPathWithRestart(t *testing.T) {
	p := Plan{
		RolloutID:       "rl-sim-002",
		Target:          "2.0.0",
		Protocol:        Semver{Major: 1},
		MaxAttempts:     2,
		MaxFailRate:     50,
		MaxRollbackRate: 50,
		MinEvaluated:    4,
		Batches: []BatchSpec{
			{Index: 0, Devices: []DeviceID{"h1", "h2", "h3"}, MaxInFlight: 2},
			{Index: 1, Devices: []DeviceID{"h4", "h5"}, MaxInFlight: 2},
		},
	}
	store := NewMemoryStore()
	devs1 := happyDevices()
	tr1 := newSimWith(p, devs1)
	o1 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr1})
	ctx1, cancel1 := context.WithCancel(context.Background())
	go tr1.Run(ctx1)
	if err := o1.RecoverOrResume(); err != nil {
		t.Fatalf("recover1: %v", err)
	}
	errCh1 := make(chan error, 1)
	go func() { errCh1 <- o1.runLoop(ctx1) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if o1.State().Devices["h1"].Phase == PhaseUpgraded {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if o1.State().Devices["h1"].Phase != PhaseUpgraded {
		t.Fatal("h1 never upgraded before restart")
	}
	cancel1()
	// 必须等旧传输的所有设备协程真正退出，才能安全丢弃旧设备对象，
	// 否则旧协程可能仍在写设备的尝试计数而被 -race 报告。
	// （第二段使用全新的设备对象，这里仅保证旧进程完全停机。）
	select {
	case <-errCh1:
	case <-time.After(time.Second):
	}
	tr1.waitStopped()

	// 第二段：新进程（新模拟设备 + 新通道），从持久化状态续跑到完成。
	tr2 := newSimWith(p, happyDevices())
	o2 := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr2})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go tr2.Run(ctx2)
	done := make(chan error, 1)
	go func() { done <- o2.Run(ctx2) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run2: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("e2e resume timed out")
	}
	st := o2.State()
	if st.Phase != RolloutCompleted {
		t.Fatalf("phase=%s, want completed", phaseName(st.Phase))
	}
	for _, b := range p.Batches {
		for _, id := range b.Devices {
			if st.Devices[id].Phase != PhaseUpgraded {
				t.Fatalf("device %s phase=%s, want upgraded", id, devicePhaseName(st.Devices[id].Phase))
			}
		}
	}
	// 每台设备在审计历史中只能有一次 succeeded 判定（跨重启不重复计数）。
	counts := map[DeviceID]int{}
	for _, d := range st.History {
		if d.Kind == DecisionSucceeded {
			counts[d.DeviceID]++
		}
	}
	for _, b := range p.Batches {
		for _, id := range b.Devices {
			if counts[id] != 1 {
				t.Fatalf("device %s succeeded verdicts=%d, want 1", id, counts[id])
			}
		}
	}
}
