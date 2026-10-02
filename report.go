package ota

import "fmt"

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
	// ToVer 非空时必须与任务目标一致；不一致说明上报张冠李戴，按无效处理。
	// 升级任务目标是计划版本；回滚任务目标是设备记录的基线版本。
	want := plan.Target
	if expectedKind == TaskRollback {
		want = targetForRollback(o, r.DeviceID)
	}
	if r.ToVer != "" && want != "" && r.ToVer != want {
		return ClassInvalid, fmt.Sprintf("report target %q mismatches expected %q", r.ToVer, want)
	}
	return ClassRetryable, fmt.Sprintf("retryable failure: code=%s reason=%s", r.ReasonCode, r.Reason)
}

// targetForRollback 返回设备回滚任务的目标版本（记录的基线），未知时为空。
func targetForRollback(o *Orchestrator, id DeviceID) FirmwareVersion {
	if rec, ok := o.st.Devices[id]; ok {
		return rec.BaselineVer
	}
	return ""
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
	maxAttempt := o.cfg.Plan.MaxAttempts
	if rb := o.cfg.Plan.rollbackBudget(); rb > maxAttempt {
		maxAttempt = rb
	}
	for a := 0; a <= maxAttempt; a++ {
		if taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskUpgrade, a) == id {
			return true
		}
		if taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskRollback, a) == id {
			return true
		}
	}
	return false
}

// recordBaselineLocked 从设备上报的 FromVer 记录回滚基线：首帧优先，之后不再改变，
// 保证乱序/重复上报下基线唯一可复现。
func recordBaselineLocked(rec *DeviceRecord, r DeviceReport) {
	if rec.BaselineVer == "" && r.FromVer != "" {
		rec.BaselineVer = r.FromVer
	}
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
	case PhasePending:
		d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
			"report while device has no in-flight task (pending)")
		_ = o.persistLocked()
		return d, nil
	default:
		// 设备已终态（done/incompatible/failed）或已升级（PhaseUpgraded）：
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
	case rec.Phase == PhaseDone && rec.Success:
		// 正常完成：回滚成功帧也可能是设备自发回滚后的重复，这里只认升级成功帧为一致。
		consistent = r.Kind == ReportUpgradeSuccess
	case rec.Phase == PhaseIncompatible:
		consistent = r.Kind == ReportUpgradeFailure
	case rec.Phase == PhaseFailed:
		consistent = r.Kind == ReportUpgradeFailure
	case rec.Phase == PhaseUpgraded:
		consistent = r.Kind == ReportUpgradeSuccess || r.Kind == ReportAcknowledge
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

	switch r.Kind {
	case ReportAcknowledge:
		recordBaselineLocked(rec, r)
		rec.LastSeq = maxSeq(rec.LastSeq, r.Seq)
		rec.UpdatedAt = timeNow()
		d := o.decideLocked(r, DecisionAcked, ClassNone, "device acknowledged upgrade task")
		_ = o.persistLocked()
		return d, nil

	case ReportUpgradeSuccess:
		// 即便成功帧早于 ACK（乱序），终态也直接生效，ACK 之后到来只会是重复。
		recordBaselineLocked(rec, r)
		rec.Phase = PhaseUpgraded
		rec.Success = true
		rec.CurrentTask = ""
		rec.Class = ClassNone
		rec.LastReason = "upgrade success"
		rec.LastSeq = maxSeq(rec.LastSeq, r.Seq)
		rec.UpdatedAt = timeNow()
		d := o.decideLocked(r, DecisionSucceeded, ClassNone,
			"device confirmed running target firmware")
		// 回滚波次期间升级才成功：设备已到新固件，进入回滚队列，
		// 由 pump 按配额放行，不能停在 upgraded 无人处理。
		if o.st.Phase == RolloutRollingBack {
			o.pumpRollbackQueueLocked()
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
		// 基线校验：回滚结果必须落在记录的升级前版本上。
		if r.ToVer == "" {
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
				"rollback success report missing landed version; cannot confirm baseline, awaiting re-report")
			_ = o.persistLocked()
			return d, nil
		}
		if rec.BaselineVer == "" {
			// 兼容范围：基线未知（如从旧版本状态恢复）时，采纳首个非空回报版本为基线。
			rec.BaselineVer = r.ToVer
		}
		if r.ToVer != rec.BaselineVer {
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid,
				fmt.Sprintf("rollback landed on %q but recorded baseline is %q; not counted, awaiting re-report",
					r.ToVer, rec.BaselineVer))
			_ = o.persistLocked()
			return d, nil
		}
		rec.Phase = PhaseDone
		rec.CurrentTask = ""
		rec.Success = false
		rec.LastReason = fmt.Sprintf("rollback success: device restored to baseline %s", rec.BaselineVer)
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
			d := o.decideLocked(r, DecisionFailedInvalid, ClassInvalid, why)
			_ = o.persistLocked()
			return d, nil
		}
		used := rec.RollbackAttempt
		budget := o.cfg.Plan.rollbackBudget()
		rec.LastReason = why
		if used >= budget {
			// 回滚重试预算耗尽：设备可能仍在新固件上，落“需人工介入”终态，
			// 原因与其他失败明确可区分；发布中止。重启不得抹掉已用次数、也不再补发。
			rec.Phase = PhaseRollbackFailed
			rec.CurrentTask = ""
			rec.Class = ClassManual
			rec.UpdatedAt = timeNow()
			d := o.decideLocked(r, DecisionRollbackFailed, ClassManual,
				fmt.Sprintf("%s; rollback budget exhausted at %d/%d, manual intervention required",
					why, used, budget))
			o.markAbortedLocked("rollback budget exhausted for device " + string(rec.DeviceID) +
				": manual intervention required")
			return d, o.persistLocked()
		}
		// 预算内：按确定性节奏（尝试号 +1，TaskID 确定）重新下发回滚任务。
		next := used + 1
		d := o.decideLocked(r, DecisionRetried, ClassRetryable,
			fmt.Sprintf("%s; dispatch rollback retry %d/%d", why, next, budget))
		o.dispatchRollbackLocked(rec, next)
		return d, nil

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
