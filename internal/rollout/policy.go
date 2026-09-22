package rollout

import (
	"errors"
	"fmt"
	"math"
)

// NewRelease 构建一个全新发布单的初始状态，并完成配置与批次的静态校验。
//
// 批次成员必须满足：批次下标连续从 0 开始、每批至少一台设备、设备不允许跨批重复。
func NewRelease(releaseID string, cfg Config, batches []BatchSpec) (*State, error) {
	if releaseID == "" {
		return nil, errors.New("release id is empty")
	}
	if cfg.MaxProtoVersion < 0 {
		return nil, errors.New("MaxProtoVersion must be >= 0")
	}
	if cfg.MaxAttempts < 1 {
		return nil, errors.New("MaxAttempts must be >= 1")
	}
	if cfg.SuccessThreshold <= 0 || cfg.SuccessThreshold > 1 {
		return nil, errors.New("SuccessThreshold must be within (0,1]")
	}
	if cfg.MaxFailureRate < 0 || cfg.MaxFailureRate > 1 {
		return nil, errors.New("MaxFailureRate must be within [0,1]")
	}
	if cfg.MaxRollbackRate < 0 || cfg.MaxRollbackRate > 1 {
		return nil, errors.New("MaxRollbackRate must be within [0,1]")
	}
	if cfg.TargetFirmware == "" || cfg.BaselineFirmware == "" {
		return nil, errors.New("target/baseline firmware must not be empty")
	}
	if cfg.TargetFirmware == cfg.BaselineFirmware {
		return nil, errors.New("target firmware must differ from baseline")
	}
	if len(batches) == 0 {
		return nil, errors.New("at least one batch is required")
	}

	devs := map[string]*DeviceProgress{}
	specs := make([]BatchSpec, len(batches))
	bp := make([]BatchProgress, len(batches))
	for i, b := range batches {
		if b.Index != i {
			return nil, fmt.Errorf("batch at position %d declares index %d", i, b.Index)
		}
		if len(b.DeviceIDs) == 0 {
			return nil, fmt.Errorf("batch %d has no devices", i)
		}
		if b.InFlightQuota < 1 {
			return nil, fmt.Errorf("batch %d quota must be >= 1", i)
		}
		if b.InFlightQuota > len(b.DeviceIDs) {
			// 配额超过本批设备数没有意义，收敛为本批设备数。
			b.InFlightQuota = len(b.DeviceIDs)
		}
		specs[i] = b
		// 设备初始为 IDLE（从未下发），不计入 Awaiting（Awaiting 专指可重试/
		// 崩溃重投的排队）；未完成数由 evaluateGate 用终态计数反推。
		bp[i] = BatchProgress{Index: i, Total: len(b.DeviceIDs)}
		for _, id := range b.DeviceIDs {
			if id == "" {
				return nil, fmt.Errorf("batch %d contains empty device id", i)
			}
			if _, dup := devs[id]; dup {
				return nil, fmt.Errorf("device %s appears in more than one batch", id)
			}
			devs[id] = &DeviceProgress{ID: id, Batch: i, State: DevIdle}
		}
	}
	return &State{
		ReleaseID: releaseID,
		Phase:     PhasePending,
		Specs:     specs,
		Batches:   bp,
		Devices:   devs,
		Current:   0,
		Config:    cfg,
	}, nil
}

// classifyReport 对一条原始上报做“可接受性”判定。它是纯函数：
// 只依据当前状态与上报内容，不修改任何状态，从而保证乱序/重复输入下结论可复现。
//
// 三类需要区分的结论：
//   - 设备不兼容（ReasonProtoTooNew / ReasonIncompatibleResult）：不可升级；
//   - 上报无效（ReasonMissingField / ReasonMalformed）：不静默丢弃也不计成功，原样拒绝；
//   - 可重试失败（ReasonRetriable）：由调用方在尝试上限内重新下发。
//
// 另外迟到/串号/重复上报返回 ReasonStale / ReasonBatchMismatch：忽略而非计数。
func classifyReport(s *State, r DeviceReport) (RejectReason, string) {
	if r.DeviceID == "" {
		return ReasonMissingField, "device_id is empty"
	}
	if r.Outcome == "" {
		return ReasonMissingField, "outcome is empty"
	}
	switch r.Outcome {
	case OutcomeSuccess, OutcomeRetriable, OutcomeFailed,
		OutcomeIncompatible, OutcomeRollbackSelf, OutcomeRollbackDone:
	default:
		return ReasonMalformed, fmt.Sprintf("unknown outcome %q", r.Outcome)
	}
	if r.At.IsZero() {
		return ReasonMissingField, "timestamp is zero"
	}

	d, ok := s.Devices[r.DeviceID]
	if !ok {
		return ReasonMalformed, "device is not part of the release"
	}
	if r.BatchIndex != d.Batch {
		return ReasonBatchMismatch, fmt.Sprintf(
			"report batch %d != device batch %d (late/foreign report)", r.BatchIndex, d.Batch)
	}
	// 协议版本高于编排器：设备不兼容，不可升级。该判定先于在途核对，
	// 因为它是设备固有属性，即使上报迟到也不改变“不可升级”的分类语义。
	if r.ProtoVersion > s.Config.MaxProtoVersion {
		return ReasonProtoTooNew, fmt.Sprintf(
			"device proto %d > supported max %d", r.ProtoVersion, s.Config.MaxProtoVersion)
	}

	// 回滚完成上报只在设备回滚在途时接受（排队态尚未下发，收到必为迟到/串号）。
	if r.Outcome == OutcomeRollbackDone {
		if d.State != DevRollingBack {
			return ReasonStale, fmt.Sprintf("ROLLBACK_DONE while device state=%s", d.State)
		}
		if r.Epoch != d.Epoch {
			return ReasonStale, fmt.Sprintf(
				"rollback report epoch=%d != current epoch=%d (pre-restart stale report)",
				r.Epoch, d.Epoch)
		}
		if r.Attempt != d.Attempt {
			return ReasonStale, fmt.Sprintf("rollback attempt %d != current %d", r.Attempt, d.Attempt)
		}
		if r.Firmware != s.Config.BaselineFirmware {
			return ReasonMalformed, fmt.Sprintf(
				"rollback done reports firmware %q, want baseline %q", r.Firmware, s.Config.BaselineFirmware)
		}
		return ReasonNone, "rollback completion accepted"
	}

	// 升级类上报必须匹配当前在途任务代次与恢复纪元。
	if d.State != DevInFlight {
		return ReasonStale, fmt.Sprintf("report while device state=%s, no in-flight task", d.State)
	}
	if r.Epoch != d.Epoch {
		return ReasonStale, fmt.Sprintf(
			"report epoch=%d != current epoch=%d (pre-restart stale report)", r.Epoch, d.Epoch)
	}
	if r.Attempt != d.Attempt {
		return ReasonStale, fmt.Sprintf("report attempt %d != in-flight attempt %d (duplicate/late)", r.Attempt, d.Attempt)
	}
	switch r.Outcome {
	case OutcomeSuccess, OutcomeRollbackSelf:
		if r.Firmware != s.Config.TargetFirmware {
			return ReasonMalformed, fmt.Sprintf(
				"upgrade ack with firmware %q, want target %q", r.Firmware, s.Config.TargetFirmware)
		}
	case OutcomeRetriable, OutcomeFailed, OutcomeIncompatible:
		// 失败/不兼容类上报设备仍在基线版本，固件字段必须与基线一致，否则视为畸形。
		if r.Firmware != "" && r.Firmware != s.Config.BaselineFirmware {
			return ReasonMalformed, fmt.Sprintf(
				"failure report carries unexpected firmware %q", r.Firmware)
		}
	}
	return ReasonNone, "report accepted"
}

// rate 返回 x/total 的比例；total 为 0 时定义为 0。
func rate(x, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(x) / float64(total)
}

// evaluateGate 是批次健康门槛的纯函数判定。
//
// 规则（以本批次设备总数 Total 为分母，保证判定唯一、与上报到达顺序无关）：
//   - 失败率 > MaxFailureRate：立即 BLOCKED_FAILURE_RATE，暂停后续放量并触发回滚；
//   - 自回滚率 > MaxRollbackRate：立即 BLOCKED_ROLLBACK_RATE，同样触发回滚；
//     （阈值采用严格大于，达到但不超过阈值仍允许继续观察。）
//   - 批次所有设备都到终态（无在途、无待下发）后：
//     成功率 >= SuccessThreshold 则 OPEN 进入下一批，否则按越界原因 BLOCKED。
//   - 其余情况 PENDING，继续收集证据。
func evaluateGate(cfg Config, b BatchProgress) (GateDecision, string) {
	failRate := rate(b.Failed, b.Total)
	rollbackRate := rate(b.SelfReverted, b.Total)

	if failRate > cfg.MaxFailureRate+1e-12 {
		return GateBlocked, fmt.Sprintf(
			"failure rate %.3f > threshold %.3f (%d/%d)",
			failRate, cfg.MaxFailureRate, b.Failed, b.Total)
	}
	if rollbackRate > cfg.MaxRollbackRate+1e-12 {
		return GateRollbackRate, fmt.Sprintf(
			"rollback rate %.3f > threshold %.3f (%d/%d)",
			rollbackRate, cfg.MaxRollbackRate, b.SelfReverted, b.Total)
	}

	// 已达终态（含被跳过）的设备之外都算未完成：从未下发的 IDLE、重试排队的
	// AWAITING、升级在途的 IN_FLIGHT 都必须等待，不能因为配额未轮转而提前判死。
	decided := b.Succeeded + b.Failed + b.SelfReverted + b.Skipped
	outstanding := b.Total - decided
	if outstanding > 0 {
		return GatePending, fmt.Sprintf(
			"waiting: in_flight=%d awaiting=%d decided=%d/%d",
			b.InFlight, b.Awaiting, decided, b.Total)
	}

	successRate := rate(b.Succeeded, b.Total)
	if successRate+1e-12 >= cfg.SuccessThreshold {
		return GateOpen, fmt.Sprintf(
			"batch settled: success rate %.3f >= threshold %.3f (%d/%d)",
			successRate, cfg.SuccessThreshold, b.Succeeded, b.Total)
	}
	// 全部终态且成功率不足，但单项比率又未越阈值（例如不兼容设备计入失败导致），
	// 仍然不能开门：按失败率阻塞处理，结论必须唯一。
	if failRate > math.SmallestNonzeroFloat64 {
		return GateBlocked, fmt.Sprintf(
			"batch settled but success rate %.3f < threshold %.3f",
			successRate, cfg.SuccessThreshold)
	}
	return GateRollbackRate, fmt.Sprintf(
		"batch settled but success rate %.3f < threshold %.3f (rollbacks)",
		successRate, cfg.SuccessThreshold)
}
