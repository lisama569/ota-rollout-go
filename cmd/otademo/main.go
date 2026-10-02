// Command otademo 在本地用模拟设备与内存传输通道完整演练一次灰度发布的
// 受控回滚：分批放量 -> 健康门槛熔断 -> 回滚按当前批次配额限流、固定顺序排队
// -> 一台设备回滚先失败、在独立重试预算内按固定节奏重试成功 -> 全部回到
// 升级前记录的版本基线 -> 安全中止。状态持久化到本地 JSON 文件。
//
// 用法：
//
//	go run ./cmd/otademo -state ./ota-demo-state.json
//
// 进程在流程走完后会自行收尾退出（编排器到达终态后主动 cancel 传输上下文），
// 不依赖外部超时；-state 指向的文件可删除后重跑，再次运行会从终态恢复、不再下发。
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
		// 回滚独立重试预算 3，固定节奏 150ms（到点才重投，重启也不多打一轮）。
		MaxRollbackAttempts:   3,
		RollbackRetryInterval: 150 * time.Millisecond,
		Batches: []ota.BatchSpec{
			// 批次0 在途配额只有 1：熔断后回滚也是一台一台放，其余固定排队，
			// 升级与回滚两条链路的总在途数永远不超过 1。
			{Index: 0, Devices: []ota.DeviceID{"s1", "s2", "s3"}, MaxInFlight: 1},
			{Index: 1, Devices: []ota.DeviceID{"s4", "s5", "s6"}, MaxInFlight: 2},
		},
	}

	devs := []*ota.SimDevice{
		// s1 回滚前两次失败、第三次成功：演练固定节奏重试与独立预算。
		{ID: "s1", Protocol: ota.Semver{Major: 1}, CurrentVer: "1.0.0",
			Behavior: ota.BehRollbackRetryThenSuccess, RollbackFailBefore: 2},
		{ID: "s2", Protocol: ota.Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: ota.BehSuccess},
		{ID: "s3", Protocol: ota.Semver{Major: 2}, CurrentVer: "1.0.0", Behavior: ota.BehIncompatible},
		{ID: "s4", Protocol: ota.Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: ota.BehSuccess},
		{ID: "s5", Protocol: ota.Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: ota.BehSuccess},
		{ID: "s6", Protocol: ota.Semver{Major: 1}, CurrentVer: "1.0.0", Behavior: ota.BehSuccess},
	}

	store := ota.NewFileStore(*statePath)
	tr := ota.NewSimTransport().WithJitter(2 * time.Millisecond)
	for _, d := range devs {
		tr.AddDevice(d)
	}
	o := ota.NewOrchestrator(ota.Config{Plan: plan, Store: store, Transport: tr})

	// 进程自持上下文：编排器到达终态后主动 cancel，传输协程随之退出，
	// Run / 传输 Run 都返回，main 正常收尾——不靠外部超时兜底。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trDone := make(chan struct{})
	go func() { tr.Run(ctx); close(trDone) }()

	if err := o.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
		os.Exit(1)
	}
	// 编排器已到终态：主动通知模拟传输退出并等待其完全收尾。
	cancel()
	<-trDone

	st := o.State()
	rs := o.RollbackStatus()
	fmt.Printf("\n=== demo finished: phase=%s currentBatch=%d stateFile=%s ===\n",
		phaseName(st.Phase), st.CurrentBatch, *statePath)
	fmt.Printf("rollback view: restored=%v waiting=%v inflight=%v queued=%v manual=%v\n",
		rs.Restored, rs.Waiting, rs.Inflight, rs.Queued, rs.Manual)
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
		return "aborted(after-controlled-rollback)"
	}
	return "unknown"
}
