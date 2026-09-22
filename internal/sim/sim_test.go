package sim

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/lisama569/ota-rollout-go/internal/rollout"
)

func simCfg() rollout.Config {
	return rollout.Config{
		MaxProtoVersion:  2,
		MaxAttempts:      3,
		SuccessThreshold: 0.8,
		MaxFailureRate:   0.25,
		MaxRollbackRate:  0.25,
		TargetFirmware:   "fw-2.0",
		BaselineFirmware: "fw-1.0",
	}
}

func logger(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		log.Printf("[%s] %s", t.Name(), logSprintf(format, args...))
	}
}

func TestConcurrentOutOfOrderAndDuplicates_Completes(t *testing.T) {
	t.Parallel()
	const devices = 12
	var bs []Behavior
	for i := 0; i < devices; i++ {
		id := "dev-" + itoa(i)
		bs = append(bs, Behavior{
			DeviceID: id, ProtoVersion: 2,
			// 部分设备先可重试失败再成功，覆盖重试路径。
			Outcomes: outcomesFor(i),
			// 一半设备制造延迟乱序，1/3 设备制造重复上报。
			ReorderLate: i%2 == 0,
			Duplicate:   i%3 == 0,
		})
	}
	specs := []rollout.BatchSpec{{
		Index:         0,
		DeviceIDs:     ids(devices),
		InFlightQuota: 4,
	}}

	// 同样的输入场景连续跑 3 次：无论 goroutine 调度如何，结论必须一致。
	var prevPhase rollout.Phase
	var prevSucc, prevRetry int
	for run := 0; run < 3; run++ {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		store := rollout.NewMemoryStore()
		st, err := rollout.NewRelease("rel-concurrency", simCfg(), specs)
		if err != nil {
			t.Fatalf("new release: %v", err)
		}
		eng, err := rollout.NewEngine(ctx, store, st, logger(t))
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		if err := eng.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		fleet := NewFleet("fw-2.0", "fw-1.0", bs...)
		ch := NewChannel(eng, fleet, 5*time.Millisecond, logger(t))
		if err := RunDriver(ctx, eng, fleet, ch, 500); err != nil {
			cancel()
			t.Fatalf("run %d driver: %v", run, err)
		}
		cancel()
		snap := eng.Snapshot()
		retryCount := countRetries(snap)
		t.Logf("run=%d final phase=%s succeeded=%d failed=%d retried_devices=%d",
			run, snap.Phase, snap.Batches[0].Succeeded, snap.Batches[0].Failed, retryCount)
		if snap.Phase != rollout.PhaseCompleted {
			t.Fatalf("run %d want COMPLETED, got %s counters=%+v",
				run, snap.Phase, snap.Batches[0])
		}
		if snap.Batches[0].Succeeded != devices {
			t.Fatalf("run %d want %d successes, got %d",
				run, devices, snap.Batches[0].Succeeded)
		}
		if run > 0 && (snap.Phase != prevPhase || snap.Batches[0].Succeeded != prevSucc ||
			retryCount != prevRetry) {
			t.Fatalf("non-reproducible result run=%d", run)
		}
		prevPhase, prevSucc, prevRetry = snap.Phase, snap.Batches[0].Succeeded, retryCount
	}
}

func TestConcurrentFailuresRollbackConsistent(t *testing.T) {
	t.Parallel()
	const devices = 8
	var bs []Behavior
	for i := 0; i < devices; i++ {
		b := Behavior{DeviceID: "dev-" + itoa(i), ProtoVersion: 2}
		switch {
		case i < 4:
			b.Outcomes = []rollout.ReportOutcome{rollout.OutcomeSuccess}
		case i < 6:
			b.Outcomes = []rollout.ReportOutcome{rollout.OutcomeFailed}
		default:
			// 两台先成功：触发回滚后必须被回滚。
			b.Outcomes = []rollout.ReportOutcome{rollout.OutcomeSuccess}
			b.ReorderLate = true
			b.Duplicate = true
		}
		bs = append(bs, b)
	}
	specs := []rollout.BatchSpec{{Index: 0, DeviceIDs: ids(devices), InFlightQuota: 3}}

	var prevPhase rollout.Phase
	var prevRolledBack, prevFailed int
	for run := 0; run < 3; run++ {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		store := rollout.NewMemoryStore()
		st, _ := rollout.NewRelease("rel-rollback", simCfg(), specs)
		eng, err := rollout.NewEngine(ctx, store, st, logger(t))
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		_ = eng.Start(ctx)
		fleet := NewFleet("fw-2.0", "fw-1.0", bs...)
		ch := NewChannel(eng, fleet, 5*time.Millisecond, logger(t))
		if err := RunDriver(ctx, eng, fleet, ch, 500); err != nil {
			cancel()
			t.Fatalf("driver run %d: %v", run, err)
		}
		cancel()
		snap := eng.Snapshot()
		b0 := snap.Batches[0]
		t.Logf("run=%d final phase=%s failed=%d rolled_back=%d skipped=%d",
			run, snap.Phase, b0.Failed, b0.RolledBack, b0.Skipped)
		if snap.Phase != rollout.PhaseFailed {
			t.Fatalf("run %d want FAILED, got %s", run, snap.Phase)
		}
		// 6 台成功换装的设备全部必须回到基线（rolled_back 含自回滚）。
		if b0.RolledBack != 6 {
			t.Fatalf("run %d want 6 rolled back, got %d", run, b0.RolledBack)
		}
		if b0.Failed != 2 {
			t.Fatalf("run %d want 2 failures, got %d", run, b0.Failed)
		}
		// 模拟机群最终固件必须全部是基线版本。
		for i := 0; i < devices; i++ {
			id := "dev-" + itoa(i)
			if fw := fleet.CurrentFirmware(id); fw != "fw-1.0" {
				t.Fatalf("device %s firmware=%s after rollback", id, fw)
			}
		}
		if run > 0 && (snap.Phase != prevPhase || b0.RolledBack != prevRolledBack ||
			b0.Failed != prevFailed) {
			t.Fatalf("non-reproducible rollback result run=%d", run)
		}
		prevPhase, prevRolledBack, prevFailed = snap.Phase, b0.RolledBack, b0.Failed
	}
}

func TestIncompatibleAndMalformedReports(t *testing.T) {
	ctx := t.Context()
	specs := []rollout.BatchSpec{{
		Index:         0,
		DeviceIDs:     []string{"ok0", "ok1", "ok2", "ok3", "ok4", "bad-proto", "malformed", "missing", "retry-ok", "retry-ok2"},
		InFlightQuota: 10,
	}}
	cfg := simCfg()
	cfg.MaxFailureRate = 0.5 // 本场景关注分类正确性，放宽失败阈值避免触发回滚
	st, _ := rollout.NewRelease("rel-compat", cfg, specs)
	eng, err := rollout.NewEngine(ctx, rollout.NewMemoryStore(), st, logger(t))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	_ = eng.Start(ctx)
	tasks, _ := eng.Dispatch(ctx)
	report := func(id string, proto int, outcome rollout.ReportOutcome, fw string) rollout.AcceptResult {
		cmd := tasks[id]
		r := rollout.DeviceReport{
			DeviceID: id, BatchIndex: cmd.Batch, Attempt: cmd.Attempt, Epoch: cmd.Epoch,
			ProtoVersion: proto, Outcome: outcome, Firmware: fw, At: time.Now(),
		}
		res, err := eng.Report(ctx, r)
		if err != nil {
			t.Fatalf("report: %v", err)
		}
		t.Logf("device=%s proto=%d outcome=%s fw=%q => accepted=%v reason=%s state=%s",
			id, proto, outcome, fw, res.Accepted, res.Reason, res.State)
		return res
	}

	// 协议版本过高：设备不兼容，不可升级，上报不得被静默丢弃也不得计成功。
	r1 := report("bad-proto", 99, rollout.OutcomeSuccess, "fw-2.0")
	if r1.Accepted || r1.Reason != rollout.ReasonProtoTooNew {
		t.Fatalf("proto too new: %+v", r1)
	}
	// 字段畸形：固件与结果不一致。
	r2 := report("malformed", 2, rollout.OutcomeSuccess, "fw-evil")
	if r2.Accepted || r2.Reason != rollout.ReasonMalformed {
		t.Fatalf("malformed: %+v", r2)
	}
	// 字段缺失：空 outcome。
	cmd := tasks["missing"]
	r3raw := rollout.DeviceReport{DeviceID: "missing", BatchIndex: cmd.Batch,
		Attempt: cmd.Attempt, Epoch: cmd.Epoch, ProtoVersion: 2, At: time.Now()}
	r3, _ := eng.Report(ctx, r3raw)
	if r3.Accepted || r3.Reason != rollout.ReasonMissingField {
		t.Fatalf("missing field: %+v", r3)
	}
	// 无效上报后设备仍在途：可重试类失败 -> 重试 -> 成功。
	r4 := report("retry-ok", 2, rollout.OutcomeRetriable, "fw-1.0")
	if !r4.Accepted || r4.State != rollout.DevAwaiting {
		t.Fatalf("retriable should requeue: %+v", r4)
	}
	again, _ := eng.Dispatch(ctx)
	if c, ok := again["retry-ok"]; !ok {
		t.Fatalf("retriable device not re-dispatched")
	} else {
		r := rollout.DeviceReport{DeviceID: "retry-ok", BatchIndex: c.Batch, Attempt: c.Attempt,
			Epoch: c.Epoch, ProtoVersion: 2, Outcome: rollout.OutcomeSuccess,
			Firmware: "fw-2.0", At: time.Now()}
		if res, _ := eng.Report(ctx, r); !res.Accepted {
			t.Fatalf("retry then success: %+v", res)
		}
	}
	// 另一台重试设备三次都瞬时失败 => 耗尽转永久失败。
	for i := 0; i < 3; i++ {
		c := tasks["retry-ok2"]
		if i > 0 {
			more, _ := eng.Dispatch(ctx)
			c = more["retry-ok2"]
		}
		r := rollout.DeviceReport{DeviceID: "retry-ok2", BatchIndex: c.Batch, Attempt: c.Attempt,
			Epoch: c.Epoch, ProtoVersion: 2, Outcome: rollout.OutcomeRetriable,
			Firmware: "fw-1.0", At: time.Now()}
		res, _ := eng.Report(ctx, r)
		t.Logf("retry-ok2 try#%d => accepted=%v reason=%s state=%s", i+1, res.Accepted, res.Reason, res.State)
		if i == 2 && res.Reason != rollout.ReasonRetriesExhausted {
			t.Fatalf("want exhausted, got %+v", res)
		}
	}
}

func outcomesFor(i int) []rollout.ReportOutcome {
	switch i % 4 {
	case 0:
		return []rollout.ReportOutcome{rollout.OutcomeRetriable, rollout.OutcomeSuccess}
	case 1:
		return []rollout.ReportOutcome{rollout.OutcomeRetriable, rollout.OutcomeRetriable, rollout.OutcomeSuccess}
	default:
		return []rollout.ReportOutcome{rollout.OutcomeSuccess}
	}
}

func countRetries(s *rollout.State) int {
	n := 0
	for _, d := range s.Devices {
		if d.Tries > 1 {
			n++
		}
	}
	return n
}

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "dev-" + itoa(i)
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func logSprintf(format string, args ...any) string {
	return sprintf(format, args...)
}
