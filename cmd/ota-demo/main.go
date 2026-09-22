// Command ota-demo 在本地用模拟设备与传输通道跑一遍完整灰度发布，
// 演示分批放量、乱序/重复上报、失败回滚与重启续跑。
//
// 用法：
//
//	go run ./cmd/ota-demo -scenario happy      # 三批全部成功，含重试/乱序/重复
//	go run ./cmd/ota-demo -scenario rollback   # 失败率超阈值触发回滚
//	go run ./cmd/ota-demo -state ./testdata/demo-state.json -scenario happy
//
// 指定已存在的 -state 文件即模拟“重启续跑”。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/ota-rollout-go/internal/rollout"
	"github.com/lisama569/ota-rollout-go/internal/sim"
)

func main() {
	scenario := flag.String("scenario", "happy", "happy | rollback")
	statePath := flag.String("state", filepath.Join(os.TempDir(), "ota-demo-state.json"), "状态文件路径")
	flag.Parse()

	ctx := context.Background()
	logf := func(format string, args ...any) {
		log.Printf("    "+format, args...)
	}

	cfg := rollout.Config{
		MaxProtoVersion:  2,
		MaxAttempts:      3,
		SuccessThreshold: 0.8,
		MaxFailureRate:   0.2,
		MaxRollbackRate:  0.2,
		TargetFirmware:   "fw-2.0",
		BaselineFirmware: "fw-1.0",
	}

	var specs []rollout.BatchSpec
	var fleet *sim.Fleet
	switch *scenario {
	case "happy":
		specs, fleet = buildHappy()
	case "rollback":
		specs, fleet = buildRollback()
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenario)
		os.Exit(2)
	}

	store := rollout.NewFileStore(*statePath)
	var init *rollout.State
	if existing, err := store.Load(ctx); err != nil {
		log.Fatalf("load state: %v", err)
	} else if existing == nil {
		init, err = rollout.NewRelease("demo-"+*scenario, cfg, specs)
		if err != nil {
			log.Fatalf("new release: %v", err)
		}
		log.Printf("== 新建发布单 demo-%s，状态文件：%s ==", *scenario, *statePath)
	} else {
		log.Printf("== 检测到已有状态，重启续跑：phase=%s current_batch=%d ==",
			existing.Phase, existing.Current)
	}

	eng, err := rollout.NewEngine(ctx, store, init, logf)
	if err != nil {
		log.Fatalf("new engine: %v", err)
	}
	if eng.Snapshot().Phase == rollout.PhasePending {
		if err := eng.Start(ctx); err != nil {
			log.Fatalf("start: %v", err)
		}
	}

	ch := sim.NewChannel(eng, fleet, 10*time.Millisecond, logf)
	if err := sim.RunDriver(ctx, eng, fleet, ch, 1000); err != nil {
		log.Fatalf("driver: %v", err)
	}

	st := eng.Snapshot()
	log.Printf("== 发布结束 phase=%s ==", st.Phase)
	for i, b := range st.Batches {
		log.Printf("批次 %d: total=%d success=%d failed=%d self_reverted=%d rolled_back=%d skipped=%d decision=%s",
			i, b.Total, b.Succeeded, b.Failed, b.SelfReverted, b.RolledBack, b.Skipped, b.Decision)
	}
	if st.Phase == rollout.PhaseFailed {
		os.Exit(1)
	}
}

func buildHappy() ([]rollout.BatchSpec, *sim.Fleet) {
	rows := []hdef{
		// 批次 0：4 台，1 台重试后成功，1 台乱序+重复。
		{"h-a0", nil, false, false, 2},
		{"h-a1", nil, true, true, 2},
		{"h-a2", []rollout.ReportOutcome{rollout.OutcomeRetriable, rollout.OutcomeSuccess}, false, false, 2},
		{"h-a3", nil, false, false, 2},
		// 批次 1：5 台，1 台两次重试，1 台乱序。
		{"h-b0", nil, false, false, 2},
		{"h-b1", nil, true, false, 2},
		{"h-b2", []rollout.ReportOutcome{rollout.OutcomeRetriable, rollout.OutcomeRetriable, rollout.OutcomeSuccess}, false, true, 2},
		{"h-b3", nil, false, false, 2},
		{"h-b4", nil, false, false, 2},
		// 批次 2：3 台，全部成功。
		{"h-c0", nil, false, false, 2},
		{"h-c1", nil, true, true, 2},
		{"h-c2", nil, false, false, 2},
	}
	specs := []rollout.BatchSpec{
		{Index: 0, DeviceIDs: hids(rows[0:4]), InFlightQuota: 2},
		{Index: 1, DeviceIDs: hids(rows[4:9]), InFlightQuota: 3},
		{Index: 2, DeviceIDs: hids(rows[9:12]), InFlightQuota: 3},
	}
	var bs []sim.Behavior
	for _, r := range rows {
		bs = append(bs, sim.Behavior{
			DeviceID: r.id, ProtoVersion: r.proto, Outcomes: r.outcomes,
			ReorderLate: r.late, Duplicate: r.dup,
		})
	}
	return specs, sim.NewFleet("fw-2.0", "fw-1.0", bs...)
}

func buildRollback() ([]rollout.BatchSpec, *sim.Fleet) {
	rows := []rdef{
		{"r-0", rollout.OutcomeSuccess, false, false},
		{"r-1", rollout.OutcomeSuccess, true, true}, // 已换装：回滚目标
		{"r-2", rollout.OutcomeSuccess, false, false},
		{"r-3", rollout.OutcomeFailed, false, false},
		{"r-4", rollout.OutcomeFailed, true, false}, // 30% 失败率 > 20%
		{"r-5", rollout.OutcomeSuccess, false, false},
		{"r-6", rollout.OutcomeRollbackSelf, false, false},
		{"r-7", rollout.OutcomeSuccess, false, false},
		{"r-8", rollout.OutcomeRetriable, false, false},
		{"r-9", rollout.OutcomeSuccess, false, false},
	}
	specs := []rollout.BatchSpec{{Index: 0, DeviceIDs: rids(rows), InFlightQuota: 4}}
	var bs []sim.Behavior
	for _, r := range rows {
		bs = append(bs, sim.Behavior{
			DeviceID: r.id, ProtoVersion: 2,
			Outcomes:    []rollout.ReportOutcome{r.outcome},
			ReorderLate: r.late, Duplicate: r.dup,
		})
	}
	return specs, sim.NewFleet("fw-2.0", "fw-1.0", bs...)
}

type hdef struct {
	id        string
	outcomes  []rollout.ReportOutcome
	late, dup bool
	proto     int
}

func hids(rows []hdef) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.id
	}
	return out
}

type rdef struct {
	id      string
	outcome rollout.ReportOutcome
	late    bool
	dup     bool
}

func rids(rows []rdef) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.id
	}
	return out
}
