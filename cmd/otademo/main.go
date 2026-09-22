// Command otademo 在本地用模拟设备与内存传输通道完整演练一次灰度发布：
// 两批次放量、失败熔断、已升级设备回滚、状态持久化到本地 JSON 文件。
//
// 用法：
//
//	go run ./cmd/otademo -state ./ota-demo-state.json
//
// 再次运行会从持久化状态恢复（本演示默认让发布走到安全中止的终态，
// 想看完整成功路径可把 s3 的 Behavior 改为 BehSuccess）。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	ota "github.com/lisama569/ota-rollout-go"
)

func main() {
	statePath := flag.String("state", "./ota-demo-state.json", "persistent state file")
	flag.Parse()

	plan := ota.Plan{
		RolloutID:       "rl-demo",
		Target:          "2.0.0",
		Protocol:        ota.Semver{Major: 1, Minor: 0, Patch: 0},
		MaxAttempts:     3,
		MaxFailRate:     25,
		MaxRollbackRate: 25,
		MinEvaluated:    2,
		Batches: []ota.BatchSpec{
			{Index: 0, Devices: []ota.DeviceID{"s1", "s2", "s3", "s4"}, MaxInFlight: 2},
			{Index: 1, Devices: []ota.DeviceID{"s5", "s6", "s7"}, MaxInFlight: 3},
		},
	}

	devs := []*ota.SimDevice{
		{ID: "s1", Protocol: ota.Semver{Major: 1}, Behavior: ota.BehSuccess, ExtraDuplicates: 1},
		{ID: "s2", Protocol: ota.Semver{Major: 1}, Behavior: ota.BehRetryThenSuccess, FailBefore: 1},
		{ID: "s3", Protocol: ota.Semver{Major: 2}, Behavior: ota.BehIncompatible},
		{ID: "s4", Protocol: ota.Semver{Major: 1}, Behavior: ota.BehSuccess, MalformedFirst: true},
		{ID: "s5", Protocol: ota.Semver{Major: 1}, Behavior: ota.BehSuccess},
		{ID: "s6", Protocol: ota.Semver{Major: 1}, Behavior: ota.BehSuccess},
		{ID: "s7", Protocol: ota.Semver{Major: 1}, Behavior: ota.BehSuccess},
	}

	store := ota.NewFileStore(*statePath)
	tr := ota.NewSimTransport().WithJitter(2 * time.Millisecond)
	for _, d := range devs {
		tr.AddDevice(d)
	}
	o := ota.NewOrchestrator(ota.Config{Plan: plan, Store: store, Transport: tr})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	trDone := make(chan struct{})
	go func() { tr.Run(ctx); close(trDone) }()

	if err := o.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
		os.Exit(1)
	}
	st := o.State()
	fmt.Printf("\n=== demo finished: phase=%s currentBatch=%d stateFile=%s ===\n",
		phaseName(st.Phase), st.CurrentBatch, *statePath)
	<-trDone
}

func phaseName(p ota.RolloutPhase) string {
	switch p {
	case ota.RolloutRunning:
		return "running"
	case ota.RolloutRollingBack:
		return "rolling-back"
	case ota.RolloutCompleted:
		return "completed"
	case ota.RolloutAborted:
		return "aborted(after-safe-rollback)"
	}
	return "unknown"
}
