package ota

import (
	"context"
	"fmt"
	"time"
)

// dispatchLocked 实际向传输通道投递一个任务。
// 传输层失败只记录日志，不改变状态：Run 的周期性补偿会再次尝试，
// 从而保证“至少投递一次”，而 TaskID 的确定性使重复投递天然幂等。
func (o *Orchestrator) dispatchLocked(task Task) {
	if o.cfg.Transport == nil {
		return
	}
	bs := o.st.Batches[task.Batch]
	if bs.sent == nil {
		bs.sent = make(map[TaskID]bool)
	}
	if err := o.cfg.Transport.Send(task); err != nil {
		o.cfg.Logger.Printf("dispatch deferred device=%s task=%s: %v (will retry on next tick)",
			task.Device, task.ID, err)
		return
	}
	bs.sent[task.ID] = true
	o.cfg.Logger.Printf("dispatch device=%s task=%s kind=%s attempt=%d batch=%d",
		task.Device, task.ID, taskKindName(task.Kind), task.Attempt, task.Batch)
}

// inFlightLocked 统计某批次当前在途的升级/回滚任务数。
func (o *Orchestrator) inFlightLocked(b BatchIndex) int {
	n := 0
	for _, d := range o.st.Devices {
		if d.Batch == b && (d.Phase == PhaseUpgradeInflight || d.Phase == PhaseRollbackInflight) {
			n++
		}
	}
	return n
}

// inFlightAllLocked 统计全部批次在途的升级/回滚任务数（回滚波次可能跨批次，
// 整体配额约束必须覆盖两条链路，不能把设备打满）。
func (o *Orchestrator) inFlightAllLocked() (upgrade, rollback int) {
	for _, d := range o.st.Devices {
		switch d.Phase {
		case PhaseUpgradeInflight:
			upgrade++
		case PhaseRollbackInflight:
			rollback++
		}
	}
	return upgrade, rollback
}

// rollbackQuotaLocked 返回回滚波次的在途配额：沿用触发回滚时当前批次的配额，
// 升级与回滚两条链路的在途总数不得超过它。
func (o *Orchestrator) rollbackQuotaLocked() int {
	cur := int(o.st.CurrentBatch)
	if cur < 0 || cur >= len(o.cfg.Plan.Batches) {
		return 1
	}
	if q := o.cfg.Plan.Batches[cur].MaxInFlight; q > 0 {
		return q
	}
	return 1
}

// rollbackBudget 返回单台设备回滚任务的重试预算（含首次下发）。
func (p *Plan) rollbackBudget() int {
	if p.MaxRollbackAttempts > 0 {
		return p.MaxRollbackAttempts
	}
	return p.MaxAttempts
}

// InflightStats 是升级/回滚两条链路在途与排队情况的一眼快照。
type InflightStats struct {
	Quota            int // 当前生效的在途配额（发布期为当前批次配额，回滚期为回滚配额）
	UpgradeInflight  int // 升级任务在途数
	RollbackInflight int // 回滚任务在途数
	RollbackQueued   int // 已升级未回滚、正在排队等名额的设备数
}

// Stats 返回当前在途/排队快照，供运维观测与测试断言。
func (o *Orchestrator) Stats() InflightStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.statsLocked()
}

func (o *Orchestrator) statsLocked() InflightStats {
	var s InflightStats
	if o.st == nil {
		return s
	}
	s.UpgradeInflight, s.RollbackInflight = o.inFlightAllLocked()
	for _, rec := range o.st.Devices {
		if rec.Phase == PhaseUpgraded {
			s.RollbackQueued++
		}
	}
	if o.st.Phase == RolloutRollingBack {
		s.Quota = o.rollbackQuotaLocked()
	} else if int(o.st.CurrentBatch) < len(o.cfg.Plan.Batches) {
		s.Quota = o.cfg.Plan.Batches[int(o.st.CurrentBatch)].MaxInFlight
	}
	return s
}

// dispatchPendingLocked 在当前批次配额内给待升级设备下发首次升级任务。
func (o *Orchestrator) dispatchPendingLocked() {
	if o.st.Phase != RolloutRunning {
		return
	}
	bIdx := o.st.CurrentBatch
	bs := o.st.Batches[bIdx]
	if bs.Phase != BatchActive {
		return
	}
	spec := o.cfg.Plan.Batches[int(bIdx)]
	inFlight := o.inFlightLocked(bIdx)
	for _, id := range spec.Devices {
		if inFlight >= spec.MaxInFlight {
			return
		}
		rec := o.st.Devices[id]
		if rec.Phase != PhasePending || bs.Dispatched[id] {
			continue
		}
		rec.Attempt = 1
		task := Task{
			ID:      taskID(o.st.RolloutID, bIdx, id, TaskUpgrade, rec.Attempt),
			Kind:    TaskUpgrade,
			Device:  id,
			Batch:   bIdx,
			Attempt: rec.Attempt,
			Target:  o.cfg.Plan.Target,
		}
		rec.Phase = PhaseUpgradeInflight
		rec.CurrentTask = task.ID
		rec.UpdatedAt = timeNow()
		bs.Dispatched[id] = true
		inFlight++
		o.cfg.Logger.Printf("admit device=%s into batch=%d inFlight=%d/%d",
			id, bIdx, inFlight, spec.MaxInFlight)
		_ = o.persistLocked()
		o.dispatchLocked(task)
	}
}

func taskKindName(k TaskKind) string {
	if k == TaskRollback {
		return "rollback"
	}
	return "upgrade"
}

// batchCounters 汇总某批次当前的判定计数，是推进/回滚判定的唯一依据。
type batchCounters struct {
	total      int // 批次设备总数
	pending    int // 尚未下发
	inflight   int // 升级在途（含 ACK 未终态）
	upgraded   int // 已确认升级成功
	done       int // 已回到旧版本（终态，安全）
	incompat   int // 不可兼容终态
	failed     int // 重试耗尽终态
	evaluated  int // 已有终态判定的设备数
	failures   int // 失败设备数（不可兼容 + 重试耗尽）
	rolledback int // 已回滚设备数（done）
}

func (o *Orchestrator) countBatchLocked(b BatchIndex) batchCounters {
	c := batchCounters{total: len(o.cfg.Plan.Batches[int(b)].Devices)}
	for _, id := range o.cfg.Plan.Batches[int(b)].Devices {
		rec := o.st.Devices[id]
		switch rec.Phase {
		case PhasePending:
			c.pending++
		case PhaseUpgradeInflight:
			c.inflight++
		case PhaseUpgraded:
			c.upgraded++
			c.evaluated++
		case PhaseDone:
			c.done++
			c.evaluated++
			c.rolledback++
		case PhaseIncompatible:
			c.incompat++
			c.evaluated++
			c.failures++
		case PhaseFailed:
			c.failed++
			c.evaluated++
		case PhaseRollbackInflight:
			c.inflight++
		}
	}
	return c
}

// rate 以百分制返回比值，分母为 0 时返回 0。
func rate(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) * 100 / float64(d)
}

// evaluateRunningLocked 在每次设备事件后评估发布阶段：
//  1. 若失败率/回滚率超过阈值（且达到最小样本数），立即暂停后续放量并触发全量回滚；
//  2. 若当前批次全部终态且门槛达标，推进到下一批；
//  3. 最后一批通过 => 发布完成。
//
// 判定基于计数器的确定值，不依赖上报顺序，因此乱序/重复下结论唯一可复现。
func (o *Orchestrator) evaluateRunningLocked(trigger DeviceID) error {
	b := o.st.CurrentBatch
	c := o.countBatchLocked(b)
	failRate := rate(c.failures, c.evaluated)
	rbRate := rate(c.rolledback, c.evaluated)
	p := o.cfg.Plan

	o.cfg.Logger.Printf("gate batch=%d total=%d evaluated=%d upgraded=%d rolledback=%d failed=%d inflight=%d failRate=%.1f%% rbRate=%.1f%% (limits %.1f%%/%.1f%%, minN=%d) trigger=%s",
		b, c.total, c.evaluated, c.upgraded, c.rolledback, c.failures, c.inflight,
		failRate, rbRate, p.MaxFailRate, p.MaxRollbackRate, p.MinEvaluated, trigger)

	// 达到最小样本数后，任一比率超阈值立即熔断。
	if c.evaluated >= p.MinEvaluated {
		if failRate > p.MaxFailRate || rbRate > p.MaxRollbackRate {
			reason := fmt.Sprintf(
				"health gate violated at evaluated=%d: failRate=%.1f%% (limit %.1f%%), rollbackRate=%.1f%% (limit %.1f%%)",
				c.evaluated, failRate, p.MaxFailRate, rbRate, p.MaxRollbackRate)
			return o.triggerRollbackLocked(reason)
		}
	}

	// 批次内仍有未终态设备：继续放量/等待，不推进。
	if c.evaluated+c.inflight+c.pending != c.total || c.inflight > 0 || c.pending > 0 {
		return nil
	}

	// 全部终态。门槛最终复核（即便 evaluated < MinEvaluated，只要没有通过设备也不能放行）。
	if c.upgraded == 0 || failRate > p.MaxFailRate || rbRate > p.MaxRollbackRate {
		return o.triggerRollbackLocked(fmt.Sprintf(
			"batch %d exhausted without meeting gate: upgraded=%d failRate=%.1f%% rbRate=%.1f%%",
			b, c.upgraded, failRate, rbRate))
	}

	// 健康门槛通过。
	o.st.Batches[b].Phase = BatchPassed
	o.st.recordDecision(Decision{
		At:     timeNow(),
		Batch:  b,
		Kind:   DecisionBatchAdvanced,
		Reason: fmt.Sprintf("batch %d passed gate: upgraded=%d failRate=%.1f%% rbRate=%.1f%%", b, c.upgraded, failRate, rbRate),
	})
	o.cfg.Logger.Printf("batch %d PASSED gate, upgraded=%d/%d", b, c.upgraded, c.total)

	next := int(b) + 1
	if next >= len(p.Batches) {
		o.st.Phase = RolloutCompleted
		o.st.CurrentBatch = BatchIndex(len(p.Batches)) - 1
		o.st.recordDecision(Decision{At: timeNow(), Kind: DecisionRolloutCompleted,
			Reason: "all batches passed the health gate"})
		o.cfg.Logger.Printf("ROLLOUT COMPLETED id=%s", o.st.RolloutID)
		return o.persistLocked()
	}
	o.st.CurrentBatch = BatchIndex(next)
	o.st.Batches[o.st.CurrentBatch].Phase = BatchActive
	o.cfg.Logger.Printf("advance to batch %d", o.st.CurrentBatch)
	return o.persistLocked()
}

// triggerRollbackLocked 暂停后续放量并进入回滚波次。
// 尚未开始、失败耗尽、不可兼容的设备不在新固件上，直接落为安全终态，不再下发；
// 已升级（upgraded）的设备不立即全量下发，而是进入回滚队列，由
// pumpRollbackQueueLocked 按配额与固定顺序逐台放行。
func (o *Orchestrator) triggerRollbackLocked(reason string) error {
	if o.st.Phase != RolloutRunning {
		return nil
	}
	o.st.Phase = RolloutRollingBack
	cur := o.st.CurrentBatch
	o.st.Batches[cur].Phase = BatchRollingBack
	o.st.recordDecision(Decision{
		At:     timeNow(),
		Batch:  cur,
		Kind:   DecisionRollbackTriggered,
		Reason: reason,
	})
	o.cfg.Logger.Printf("ROLLBACK TRIGGERED batch=%d reason=%s", cur, reason)

	for _, p := range o.cfg.Plan.Batches {
		for _, id := range p.Devices {
			rec := o.st.Devices[id]
			switch rec.Phase {
			case PhasePending:
				// 还没开始升级的设备无需回滚，标记安全终态。
				rec.Phase = PhaseDone
				rec.Success = false
				rec.LastReason = "rollback wave: never upgraded"
				rec.UpdatedAt = timeNow()
			case PhaseUpgradeInflight:
				// 在途升级设备等待其终态上报后由事件处理进入回滚（若成功）或落终态（若失败）。
			}
		}
	}
	if err := o.persistLocked(); err != nil {
		return err
	}
	// 按配额放行第一批回滚任务，其余 upgraded 设备排队等待。
	o.pumpRollbackQueueLocked()
	return nil
}

// markAbortedLocked 在回滚无法完成（回滚失败耗尽）时将发布置为中止态。
func (o *Orchestrator) markAbortedLocked(reason string) {
	o.st.Phase = RolloutAborted
	if bs := o.st.Batches[o.st.CurrentBatch]; bs != nil && bs.Phase == BatchRollingBack {
		bs.Phase = BatchFailed
	}
	o.st.recordDecision(Decision{At: timeNow(), Batch: o.st.CurrentBatch,
		Kind: DecisionAborted, Reason: reason})
	o.cfg.Logger.Printf("ROLLOUT ABORTED reason=%s", reason)
}

// afterDeviceEventLocked 是设备终态事件后的统一入口：落盘 -> 评估门槛 -> 补偿下发。
// 顺序很重要：必须先评估门槛，一旦熔断就先转入回滚态，补偿下发不会再放进新设备。
func (o *Orchestrator) afterDeviceEventLocked(r DeviceReport) error {
	if err := o.persistLocked(); err != nil {
		return err
	}
	switch o.st.Phase {
	case RolloutRunning:
		if err := o.evaluateRunningLocked(r.DeviceID); err != nil {
			return err
		}
		// 评估可能已经触发回滚，只有仍处于发布态时才补放新设备。
		if o.st.Phase == RolloutRunning {
			o.dispatchPendingLocked()
		}
		return nil
	case RolloutRollingBack:
		return o.evaluateRollbackLocked()
	}
	return nil
}

// onNewFirmwareLocked 返回仍“可能处在新固件上”的设备清单，用于日志与收敛判定：
//   - PhaseUpgradeInflight：升级任务已发出、终态未回报，可能已跑到新固件；
//   - PhaseUpgraded：已确认在新固件，排队等回滚名额；
//   - PhaseRollbackInflight：回滚任务在途，尚未确认落回基线。
//
// 升级失败、上报无效、回滚中途重启的设备都仍落在上述某一阶段，
// 不会被误当成“已经离开新固件”。
func (o *Orchestrator) onNewFirmwareLocked() []DeviceID {
	var out []DeviceID
	seen := make(map[DeviceID]bool)
	for _, spec := range o.cfg.Plan.Batches {
		for _, id := range spec.Devices {
			if seen[id] {
				continue
			}
			seen[id] = true
			switch o.st.Devices[id].Phase {
			case PhaseUpgradeInflight, PhaseUpgraded, PhaseRollbackInflight:
				out = append(out, id)
			}
		}
	}
	return out
}

// evaluateRollbackLocked 判断回滚波次是否结束：
//   - 先按配额补放排队中的回滚任务；
//   - 仍有设备可能在新固件上（升级在途 / 已升级排队 / 回滚在途）=> 未收敛，
//     保持回滚中，由后续设备终态上报或 tick 继续驱动；
//   - 所有跑到过新固件的设备都已落回基线或进入明确终态 => 回滚完成，安全中止。
func (o *Orchestrator) evaluateRollbackLocked() error {
	o.pumpRollbackQueueLocked()
	s := o.statsLocked()
	pending := o.onNewFirmwareLocked()
	o.cfg.Logger.Printf("rollback wave status: upgradeInflight=%d rollbackInflight=%d queued=%d quota=%d pendingOnNewFirmware=%v",
		s.UpgradeInflight, s.RollbackInflight, s.RollbackQueued, s.Quota, pending)
	if len(pending) > 0 {
		o.cfg.Logger.Printf("rollback wave not converged: %d device(s) still on (or heading to) new firmware: %v",
			len(pending), pending)
		return nil
	}
	// 所有设备都已离开新固件：回滚波次完成，批次标记失败，发布中止。
	cur := o.st.CurrentBatch
	o.st.Batches[cur].Phase = BatchFailed
	o.st.Phase = RolloutAborted
	o.st.recordDecision(Decision{At: timeNow(), Batch: cur, Kind: DecisionAborted,
		Reason: "rollback wave finished: all upgraded devices were restored"})
	o.cfg.Logger.Printf("ROLLBACK WAVE FINISHED batch=%d, rollout aborted safely", cur)
	return o.persistLocked()
}

// pumpRollbackQueueLocked 按“批次序 + 计划内设备序”的固定顺序，把排队中
// （PhaseUpgraded）的设备补进回滚在途，直到升级+回滚在途总数达到回滚配额。
// 顺序只依赖计划本身，与上报先后无关，因此排队次序可复现。
func (o *Orchestrator) pumpRollbackQueueLocked() {
	if o.st.Phase != RolloutRollingBack {
		return
	}
	quota := o.rollbackQuotaLocked()
	for _, spec := range o.cfg.Plan.Batches {
		for _, id := range spec.Devices {
			up, rb := o.inFlightAllLocked()
			if up+rb >= quota {
				return
			}
			rec := o.st.Devices[id]
			if rec.Phase != PhaseUpgraded {
				continue
			}
			o.dispatchRollbackLocked(rec, rec.RollbackAttempt+1)
		}
	}
}

// dispatchRollbackLocked 为单台设备下发一次回滚任务（首次或重试），
// 目标版本为记录的升级前基线。先落盘再投递，保证重启后可按同一位点续跑。
func (o *Orchestrator) dispatchRollbackLocked(rec *DeviceRecord, attempt int) {
	rec.RollbackAttempt = attempt
	task := Task{
		ID:      taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskRollback, attempt),
		Kind:    TaskRollback,
		Device:  rec.DeviceID,
		Batch:   rec.Batch,
		Attempt: attempt,
		Target:  rec.BaselineVer,
	}
	rec.Phase = PhaseRollbackInflight
	rec.CurrentTask = task.ID
	rec.UpdatedAt = timeNow()
	up, rb := o.inFlightAllLocked()
	o.cfg.Logger.Printf("admit rollback device=%s attempt=%d baseline=%q inFlight=%d/%d (upgrade=%d rollback=%d)",
		rec.DeviceID, attempt, rec.BaselineVer, up+rb, o.rollbackQuotaLocked(), up, rb)
	_ = o.persistLocked()
	o.dispatchLocked(task)
}

// redispatchInflightLocked 在重启恢复后重放在途任务：
// DeviceRecord 显示在途、但本进程 transport 尚未投递过该 TaskID 时重投。
// TaskID 确定 => 同一任务；设备按 TaskID 去重 => 不会重复计数。
func (o *Orchestrator) redispatchInflightLocked() {
	if o.cfg.Transport == nil {
		return
	}
	for _, rec := range o.st.Devices {
		bs := o.st.Batches[rec.Batch]
		if bs.sent == nil {
			bs.sent = make(map[TaskID]bool)
		}
		var kind TaskKind
		var attempt int
		switch rec.Phase {
		case PhaseUpgradeInflight:
			kind, attempt = TaskUpgrade, rec.Attempt
		case PhaseRollbackInflight:
			kind, attempt = TaskRollback, rec.RollbackAttempt
		default:
			continue
		}
		expectID := taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, kind, attempt)
		if rec.CurrentTask != expectID || bs.sent[expectID] {
			continue
		}
		o.dispatchLocked(Task{
			ID: expectID, Kind: kind, Device: rec.DeviceID,
			Batch: rec.Batch, Attempt: attempt, Target: o.cfg.Plan.Target,
		})
	}
}

// tickLocked 执行一轮补偿：补在途、补待下发、并在回滚期检查收敛。
func (o *Orchestrator) tickLocked() {
	o.redispatchInflightLocked()
	if o.st.Phase == RolloutRunning {
		o.dispatchPendingLocked()
	} else if o.st.Phase == RolloutRollingBack {
		_ = o.evaluateRollbackLocked()
	}
}

// defaultTickInterval 是无上报事件时的补偿与门槛检查间隔。
const defaultTickInterval = 20 * time.Millisecond

// Run 执行恢复并驱动调度循环，直到发布到达终态或 ctx 取消。
func (o *Orchestrator) Run(ctx context.Context) error {
	if err := o.RecoverOrResume(); err != nil {
		return err
	}
	return o.runLoop(ctx)
}

// runLoop 是恢复完成后的消费 + 补偿循环。
func (o *Orchestrator) runLoop(ctx context.Context) error {
	o.mu.Lock()
	o.redispatchInflightLocked()
	o.dispatchPendingLocked()
	o.mu.Unlock()

	reports := make(<-chan DeviceReport)
	if o.cfg.Transport != nil {
		reports = o.cfg.Transport.Reports()
	}
	ticker := time.NewTicker(defaultTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-reports:
			if !ok {
				// 通道关闭：继续靠 tick 收敛（测试中通常直接见终态后退出）。
				reports = nil
				continue
			}
			if _, err := o.HandleReport(r); err != nil {
				o.cfg.Logger.Printf("handle report error device=%s: %v", r.DeviceID, err)
			}
		case <-ticker.C:
			o.mu.Lock()
			o.tickLocked()
			phase := o.st.Phase
			o.mu.Unlock()
			if phase == RolloutCompleted || phase == RolloutAborted {
				return nil
			}
		}

		o.mu.Lock()
		phase := o.st.Phase
		o.mu.Unlock()
		if phase == RolloutCompleted || phase == RolloutAborted {
			return nil
		}
	}
}
