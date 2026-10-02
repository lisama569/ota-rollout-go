package ota

import (
	"fmt"
	"time"
)

// validateReport 对原始上报做结构性校验，返回 Invalid 类原因。
// 校验只针对“报文本身”，不依赖设备当前状态。
func validateReport(r DeviceReport) (FailureClass, string) {
	if r.DeviceID == "" {
		return ClassInvalid, "report missing device id"
	}
	if r.TaskID == "" {
		return ClassInvalid, "report missing task id"
	}
	switch r.Kind {
	case ReportAcknowledge, ReportUpgradeSuccess, ReportUpgradeFailure,
		ReportRollbackSuccess, ReportRollbackFailure:
	default:
		return ClassInvalid, fmt.Sprintf("report has unknown kind %d", r.Kind)
	}
	if r.Protocol.Major < 0 || r.Protocol.Minor < 0 || r.Protocol.Patch < 0 {
		return ClassInvalid, "report has malformed (negative) protocol version"
	}
	return ClassNone, ""
}

// classifyFailure 对一帧“失败上报”做业务分类：
//   - 设备协议主版本更高 或 reason 明确为协议过新：ClassIncompatible（不可升级，终态）；
//   - 字段语义畸形（目标版本与计划不一致等）：ClassInvalid；
//   - 其余视为可重试的临时失败：ClassRetryable。
func classifyFailure(o *Orchestrator, r DeviceReport, expectedKind TaskKind) (FailureClass, string) {
	plan := o.cfg.Plan
	if r.Protocol.Major > plan.Protocol.Major {
		return ClassIncompatible, fmt.Sprintf(
			"device protocol v%d.%d.%d newer than plan v%d.%d.%d: device cannot be upgraded",
			r.Protocol.Major, r.Protocol.Minor, r.Protocol.Patch,
			plan.Protocol.Major, plan.Protocol.Minor, plan.Protocol.Patch)
	}
	if r.ReasonCode == "protocol-too-new" {
		return ClassIncompatible, "device reports protocol incompatibility"
	}
	// 任务种类与上报种类不符（例如设备正在等回滚却上报升级失败），
	// 属于内容畸形：不计成功、不消耗重试次数，但也不是设备不可升级。
	if expectedKind == TaskRollback && r.Kind != ReportRollbackFailure {
		return ClassInvalid, fmt.Sprintf("rollback task received non-rollback failure kind %d", r.Kind)
	}
	if expectedKind == TaskUpgrade && r.Kind != ReportUpgradeFailure {
		return ClassInvalid, fmt.Sprintf("upgrade task received non-upgrade failure kind %d", r.Kind)
	}
	// ToVer 非空时必须与计划目标一致；不一致说明上报张冠李戴，按无效处理。
	// 该约束只针对升级任务：回滚任务的 ToVer/目标是设备自己的版本基线，
	// 与计划目标不同，回滚的版本正确性在回滚成功时按基线单独核验。
	if expectedKind == TaskUpgrade && r.ToVer != "" && r.ToVer != plan.Target {
		return ClassInvalid, fmt.Sprintf("report target %q mismatches plan target %q", r.ToVer, plan.Target)
	}
	return ClassRetryable, fmt.Sprintf("retryable failure: code=%s reason=%s", r.ReasonCode, r.Reason)
}

// HandleReport 处理一帧设备上报，返回该上报对应的唯一判定。
//
// 并发安全：整段在互斥锁内执行。乱序/重复/迟到上报遵循“首帧终态优先”：
// 同一 TaskID 的第一帧终态判定是唯一生效的判定，之后到来的重复帧得到
// DecisionDuplicate（同结论）或 DecisionStale（与之矛盾的迟到帧），均不改变状态。
//
// 注意：无效上报（缺字段/畸形）不会被静默丢弃，而是产生带 ClassInvalid 原因的判定。
func (o *Orchestrator) HandleReport(r DeviceReport) (Decision, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.handleReportLocked(r)
}

func (o *Orchestrator) decideLocked(r DeviceReport, k DecisionKind, class FailureClass, reason string) Decision {
	d := Decision{
		At:       timeNow(),
		DeviceID: r.DeviceID,
		TaskID:   r.TaskID,
		Kind:     k,
		Class:    class,
		Reason:   reason,
	}
	if rec, ok := o.st.Devices[r.DeviceID]; ok {
		d.Batch = rec.Batch
	}
	o.cfg.Logger.Printf("decision device=%s task=%s kind=%s report=%s class=%s reason=%q",
		r.DeviceID, r.TaskID, decisionName(k), reportKindName(r.Kind), className(class), reason)
	o.st.recordDecision(d)
	return d
}

// knownTaskLocked 判断 taskID 是否曾经是该设备的升级/回滚任务（用于区分
// “旧任务的迟到帧”与“完全无法识别的无效帧”）。
func (o *Orchestrator) knownTaskLocked(rec *DeviceRecord, id TaskID) bool {
	for a := 0; a <= o.cfg.Plan.MaxAttempts; a++ {
		if taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskUpgrade, a) == id {
			return true
		}
		if taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskRollback, a) == id {
			return true
		}
	}
	return false
}

func (o *Orchestrator) handleReportLocked(r DeviceReport) (Decision, error) {
	if o.st == nil {
		return Decision{}, ErrInvalidState
	}

	// 1) 报文结构校验：缺字段/未知类型/畸形版本。
	if class, why := validateReport(r); class != ClassNone {
		// 连设备都识别不出来的帧，无法挂到设备记录上，仍然判定并记录，绝不静默丢弃。
		d := Decision{At: timeNow(), DeviceID: r.DeviceID, TaskID: r.TaskID,
			Kind: DecisionFailedInvalid, Class: ClassInvalid, Reason: why}
		o.cfg.Logger.Printf("decision device=%q task=%q kind=%s report=%s class=%s reason=%q",
			r.DeviceID, r.TaskID, decisionName(d.Kind), reportKindName(r.Kind), className(class), why)
		o.st.recordDecision(d)
		return d, nil
	}

	rec, ok := o.st.Devices[r.DeviceID]
	if !ok {
		return o.decideLocked(r, DecisionIgnored, ClassInvalid,
			"report from device that is not part of the plan"), ErrDeviceNotInPlan
	}
	dBatch := rec.Batch

	// 2) 乱序保护：同一任务携带更大 Seq 的帧先到后，旧帧只作陈旧帧忽略。
	if r.Seq != 0 && rec.LastSeq != 0 && r.Seq < rec.LastSeq {
		d := o.decideLocked(r, DecisionStale, ClassNone,
			fmt.Sprintf("stale seq %d < last %d", r.Seq, rec.LastSeq))
		d.Batch = dBatch
		return d, nil
	}

	// 3) 按设备当前阶段分发判定。
	switch rec.Phase {
	case PhaseUpgradeInflight:
		return o.handleUpgradeInflightLocked(rec, r)
	case PhaseRollbackInflight:
		return o.handleRollbackInflightLocked(rec, r)
	case PhaseRollbackQueued:
		// 设备在回滚队列中、尚未收到任何回滚任务：只可能收到旧升级任务的迟到帧。
		if o.knownTaskLocked(rec, r.TaskID) {
			d := o.duplicateOrStaleLocked(rec, r)
			return d, nil
		}
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			"report while device is queued for rollback and has no rollback task yet")
		return d, nil
	case PhaseRollbackWaiting:
		// 定时重试窗口内：当前（已失败）任务的任何迟到帧一律视为重复/陈旧，
		// 失败结论已经生效，绝不能再触发一次重试或多扣一次预算；
		// 未来尝试号或未知任务的帧也不改状态——重试节奏由编排器掌握，设备不能抢跑。
		if r.TaskID == rec.CurrentTask {
			d := o.duplicateOrStaleLocked(rec, r)
			return d, nil
		}
		if o.knownTaskLocked(rec, r.TaskID) {
			d := o.decideLocked(r, DecisionStale, ClassNone,
				"report during rollback retry wait; attempt not dispatched yet")
			d.Batch = rec.Batch
			return d, nil
		}
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			"report with unknown task id during rollback retry wait")
		return d, nil
	case PhasePending:
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			"report while device has no in-flight task (pending)")
		_ = o.persistLocked()
		return d, nil
	default:
		// 设备已终态（done/incompatible/failed/rollback-failed）或已升级（PhaseUpgraded）：
		// 只接受已知旧任务的重复/迟到帧。
		if o.knownTaskLocked(rec, r.TaskID) {
			return o.duplicateOrStaleLocked(rec, r), nil
		}
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			"report with unknown task id for device in terminal/upgraded phase")
		return d, nil
	}
}

// duplicateOrStaleLocked 处理已离开在途状态后到达的帧：
// 与已生效结论一致 => duplicate；与之矛盾 => stale。都不改状态。
func (o *Orchestrator) duplicateOrStaleLocked(rec *DeviceRecord, r DeviceReport) Decision {
	var consistent bool
	switch {
	case rec.Phase == PhaseDone && rec.Restored:
		// 经编排器回滚确认回到基线：回滚成功帧（含落地版本）为一致。
		consistent = r.Kind == ReportRollbackSuccess
	case rec.Phase == PhaseDone && rec.Success:
		// 正常完成：回滚成功帧也可能是设备自发回滚后的重复，这里只认升级成功帧为一致。
		consistent = r.Kind == ReportUpgradeSuccess
	case rec.Phase == PhaseDone:
		// 其余安全终态（从未升级/回滚期升级失败/自发回退）：失败或回滚成功帧都可一致。
		consistent = r.Kind == ReportUpgradeFailure || r.Kind == ReportRollbackSuccess
	case rec.Phase == PhaseIncompatible:
		consistent = r.Kind == ReportUpgradeFailure
	case rec.Phase == PhaseFailed:
		consistent = r.Kind == ReportUpgradeFailure
	case rec.Phase == PhaseRollbackFailed:
		consistent = r.Kind == ReportRollbackFailure
	case rec.Phase == PhaseUpgraded:
		consistent = r.Kind == ReportUpgradeSuccess || r.Kind == ReportAcknowledge
	case rec.Phase == PhaseRollbackWaiting:
		consistent = r.Kind == ReportRollbackFailure
	}
	k := DecisionStale
	why := "late contradicting report for an already resolved task"
	if consistent || r.Kind == ReportAcknowledge {
		k = DecisionDuplicate
		why = "duplicate report for an already resolved task; verdict unchanged"
	}
	d := o.decideLocked(r, k, ClassNone, why)
	d.Batch = rec.Batch
	return d
}

// handleUpgradeInflightLocked 处理“升级任务在途”阶段的上报。
func (o *Orchestrator) handleUpgradeInflightLocked(rec *DeviceRecord, r DeviceReport) (Decision, error) {
	if r.TaskID != rec.CurrentTask {
		if o.knownTaskLocked(rec, r.TaskID) {
			d := o.decideLocked(r, DecisionStale, ClassNone,
				"report belongs to an older attempt, current task is "+string(rec.CurrentTask))
			return d, nil
		}
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			"report task id does not match current in-flight task "+string(rec.CurrentTask))
		return d, nil
	}
	// 任何对当前升级任务的有效帧都可以贡献基线：设备尚未跑上新固件时，
	// FromVer 就是它升级前的真实版本；只记录首个非空值，之后不可改写。
	o.captureBaselineLocked(rec, r.FromVer)

	switch r.Kind {
	case ReportAcknowledge:
		rec.LastSeq = maxSeq(rec.LastSeq, r.Seq)
		o.captureBaselineLocked(rec, r.FromVer)
		rec.UpdatedAt = timeNow()
		d := o.decideLocked(r, DecisionAcked, ClassNone, "device acknowledged upgrade task")
		_ = o.persistLocked()
		return d, nil

	case ReportUpgradeSuccess:
		// 即便成功帧早于 ACK（乱序），终态也直接生效，ACK 之后到来只会是重复。
		o.captureBaselineLocked(rec, r.FromVer)
		rec.Phase = PhaseUpgraded
		rec.Success = true
		rec.CurrentTask = ""
		rec.Class = ClassNone
		rec.LastReason = "upgrade success"
		rec.LastSeq = maxSeq(rec.LastSeq, r.Seq)
		rec.UpdatedAt = timeNow()
		d := o.decideLocked(r, DecisionSucceeded, ClassNone,
			"device confirmed running target firmware")
		// 回滚波次期间升级才成功：设备已到新固件，立即入回滚队列，由配额泵放行，
		// 不能停在 upgraded 无人处理。
		if o.st.Phase == RolloutRollingBack {
			o.pumpRollbacksLocked()
		}
		if err := o.afterDeviceEventLocked(r); err != nil {
			return d, err
		}
		return d, nil

	case ReportUpgradeFailure:
		class, why := classifyFailure(o, r, TaskUpgrade)
		switch class {
		case ClassIncompatible:
			rec.Phase = PhaseIncompatible
			rec.Success = false
			rec.CurrentTask = ""
			rec.Class = ClassIncompatible
			rec.LastReason = why
			rec.UpdatedAt = timeNow()
			d := o.decideLocked(r, DecisionFailedIncompatible, ClassIncompatible, why)
			if err := o.afterDeviceEventLocked(r); err != nil {
				return d, err
			}
			return d, nil

		case ClassInvalid:
			// 无效失败帧：不计成功、不消耗重试次数，任务保持在途，等待有效重报。
			rec.Class = ClassInvalid
			rec.LastReason = why
			rec.UpdatedAt = timeNow()
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid, why)
			_ = o.persistLocked()
			return d, nil

		default: // ClassRetryable
			used := rec.Attempt // 刚失败的尝试号
			rec.LastReason = why
			rec.LastSeq = maxSeq(rec.LastSeq, r.Seq)
			rec.UpdatedAt = timeNow()
			// 回滚波次期间在途升级失败：设备未跑到新固件，不再重试升级，直接安全终态。
			if o.st.Phase == RolloutRollingBack {
				rec.Phase = PhaseDone
				rec.CurrentTask = ""
				rec.Success = false
				rec.LastReason = "upgrade failed during rollback wave; no retry, device stays on old firmware"
				d := o.decideLocked(r, DecisionFailedRetryable, ClassRetryable, rec.LastReason)
				if err := o.afterDeviceEventLocked(r); err != nil {
					return d, err
				}
				return d, nil
			}
			if used >= o.cfg.Plan.MaxAttempts {
				rec.Phase = PhaseFailed
				rec.CurrentTask = ""
				rec.Class = ClassRetryable
				d := o.decideLocked(r, DecisionFailedRetryable, ClassRetryable,
					fmt.Sprintf("%s; attempts exhausted at %d", why, used))
				if err := o.afterDeviceEventLocked(r); err != nil {
					return d, err
				}
				return d, nil
			}
			// 仍有重试额度：重新下发同一批次的新升级任务（尝试号 +1）。
			next := used + 1
			rec.Attempt = next
			task := Task{
				ID:      taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskUpgrade, next),
				Kind:    TaskUpgrade,
				Device:  rec.DeviceID,
				Batch:   rec.Batch,
				Attempt: next,
				Target:  o.cfg.Plan.Target,
			}
			rec.CurrentTask = task.ID
			d := o.decideLocked(r, DecisionRetried, ClassRetryable,
				fmt.Sprintf("%s; dispatch retry attempt %d/%d", why, next, o.cfg.Plan.MaxAttempts))
			if err := o.persistLocked(); err != nil {
				return d, err
			}
			o.dispatchLocked(task)
			return d, nil
		}

	case ReportRollbackSuccess:
		// 设备在未被要求回滚时自行回到旧版本：视为已回滚，纳入回滚率统计。
		rec.Phase = PhaseDone
		rec.Success = false
		rec.CurrentTask = ""
		rec.LastReason = "device self-rollback reported during upgrade window"
		rec.UpdatedAt = timeNow()
		d := o.decideLocked(r, DecisionRollbackSucceeded, ClassNone, rec.LastReason)
		if err := o.afterDeviceEventLocked(r); err != nil {
			return d, err
		}
		return d, nil

	default:
		// 在途升级任务收到回滚失败等畸形组合帧：无效，保持在途等待重报。
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			fmt.Sprintf("unexpected report kind %s for in-flight upgrade task", reportKindName(r.Kind)))
		_ = o.persistLocked()
		return d, nil
	}
}

// handleRollbackInflightLocked 处理“回滚任务在途”阶段的上报。
func (o *Orchestrator) handleRollbackInflightLocked(rec *DeviceRecord, r DeviceReport) (Decision, error) {
	if r.TaskID != rec.CurrentTask {
		if o.knownTaskLocked(rec, r.TaskID) {
			d := o.decideLocked(r, DecisionStale, ClassNone,
				"report belongs to an older task, current rollback task is "+string(rec.CurrentTask))
			return d, nil
		}
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			"report task id does not match current in-flight rollback task")
		return d, nil
	}

	switch r.Kind {
	case ReportAcknowledge:
		d := o.decideLocked(r, DecisionAcked, ClassNone, "device acknowledged rollback task")
		_ = o.persistLocked()
		return d, nil

	case ReportRollbackSuccess:
		// 版本基线校验：设备必须明确报回它落地的版本（ToVer），且必须等于
		// 升级前记录的基线。缺版本 / 版本对不上都不算成功：判无效、不消耗重试
		// 预算，设备保持回滚在途，等待设备重报（或重发后的新帧）。
		baseline := rec.Baseline
		if baseline == "" {
			why := "rollback success without a recorded version baseline: cannot verify the restored firmware"
			rec.Class = ClassInvalid
			rec.LastReason = why
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid, why)
			_ = o.persistLocked()
			return d, nil
		}
		if r.ToVer == "" {
			why := fmt.Sprintf("rollback success without landing version: expected baseline %s, device reported no version", baseline)
			rec.Class = ClassInvalid
			rec.LastReason = why
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid, why)
			_ = o.persistLocked()
			return d, nil
		}
		if r.ToVer != baseline {
			why := fmt.Sprintf("rollback landing version %q does not match recorded baseline %q", r.ToVer, baseline)
			rec.Class = ClassInvalid
			rec.LastReason = why
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid, why)
			_ = o.persistLocked()
			return d, nil
		}
		rec.Phase = PhaseDone
		rec.CurrentTask = ""
		rec.Success = false
		rec.Restored = true
		rec.Class = ClassNone
		rec.NextAttemptAt = time.Time{}
		rec.LastReason = fmt.Sprintf("rollback success: device restored to baseline %s", baseline)
		rec.LastSeq = maxSeq(rec.LastSeq, r.Seq)
		rec.UpdatedAt = timeNow()
		d := o.decideLocked(r, DecisionRollbackSucceeded, ClassNone, rec.LastReason)
		if err := o.afterDeviceEventLocked(r); err != nil {
			return d, err
		}
		return d, nil

	case ReportRollbackFailure:
		class, why := classifyFailure(o, r, TaskRollback)
		if class == ClassIncompatible {
			// 回滚场景下“协议过新”没有意义：回滚失败一律按可重试失败处理。
			class = ClassRetryable
			why = "retryable rollback failure: " + r.Reason
		}
		if class == ClassInvalid {
			// 无效失败帧：不计成功、不消耗回滚预算，任务保持在途，等待有效重报。
			rec.Class = ClassInvalid
			rec.LastReason = why
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid, why)
			_ = o.persistLocked()
			return d, nil
		}
		budget := o.rollbackBudgetLocked()
		used := rec.RollbackAttempt
		rec.LastReason = why
		rec.LastSeq = maxSeq(rec.LastSeq, r.Seq)
		if used >= budget {
			// 回滚重试预算耗尽：停止一切重试，设备进入明确的人工介入终态，
			// 原因类别与其它失败可区分；发布立即冻结，不再推进队列后续设备。
			rec.Phase = PhaseRollbackFailed
			rec.CurrentTask = ""
			rec.Class = ClassRollbackBudgetExhausted
			rec.NextAttemptAt = time.Time{}
			rec.UpdatedAt = timeNow()
			d := o.decideLocked(r, DecisionRollbackFailed, ClassRollbackBudgetExhausted,
				fmt.Sprintf("%s; rollback retry budget exhausted after %d/%d attempts, manual intervention required",
					why, used, budget))
			o.markAbortedLocked(fmt.Sprintf(
				"rollback retry budget exhausted for device %s after %d/%d attempts",
				rec.DeviceID, used, budget), true)
			return d, o.persistLocked()
		}
		// 预算内：不立刻重发，按固定、可复现的节奏排定下一次尝试。
		// 尝试号与下次尝试时刻都持久化：重启不会抹掉已用预算，也不会提前多打一轮。
		next := used + 1
		nextAt := timeNow().Add(o.rollbackIntervalLocked())
		rec.RollbackAttempt = next
		rec.CurrentTask = taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskRollback, used)
		rec.Phase = PhaseRollbackWaiting
		rec.NextAttemptAt = nextAt
		rec.UpdatedAt = timeNow()
		d := o.decideLocked(r, DecisionRetried, ClassRetryable,
			fmt.Sprintf("%s; rollback retry %d/%d scheduled at %s (fixed cadence)",
				why, next, budget, nextAt.Format("15:04:05.000")))
		if err := o.persistLocked(); err != nil {
			return d, err
		}
		// 设备释放了在途槽位但保留回滚名额：立刻尝试把队列下一台补入，
		// 本设备的定时重投由 tick 到点执行。
		o.pumpRollbacksLocked()
		return d, o.persistLocked()

	default:
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			fmt.Sprintf("unexpected report kind %s for in-flight rollback task", reportKindName(r.Kind)))
		_ = o.persistLocked()
		return d, nil
	}
}

func maxSeq(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// captureBaselineLocked 从设备对升级任务上报的 FromVer 中一次性记录回滚基线：
// 只记录首个非空值，之后任何上报都不能改写（保证回滚目标可复现、不被设备随口改掉）。
func (o *Orchestrator) captureBaselineLocked(rec *DeviceRecord, from FirmwareVersion) {
	if rec.Baseline == "" && from != "" {
		rec.Baseline = from
		o.cfg.Logger.Printf("baseline captured device=%s baseline=%s", rec.DeviceID, from)
	}
}
