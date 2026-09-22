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

// triggerRollbackLocked 暂停后续放量并对所有“已经升级到新版本”的设备下发回滚任务。
// 尚未开始、失败耗尽、不可兼容的设备不在新固件上，直接落为安全终态，不再下发。
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
			case PhaseUpgraded:
				rec.RollbackAttempt = 1
				task := Task{
					ID:      taskID(o.st.RolloutID, rec.Batch, id, TaskRollback, rec.RollbackAttempt),
					Kind:    TaskRollback,
					Device:  id,
					Batch:   rec.Batch,
					Attempt: rec.RollbackAttempt,
				}
				rec.Phase = PhaseRollbackInflight
				rec.CurrentTask = task.ID
				rec.UpdatedAt = timeNow()
				o.dispatchLocked(task)
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
	return o.persistLocked()
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

// evaluateRollbackLocked 判断回滚波次是否结束：
//   - 没有任何设备还在新固件（upgraded）或回滚在途 => 回滚完成；
//   - 此时若仍有设备停在 upgraded，说明回滚投递/确认缺失，保持等待，由 tick 补偿。
func (o *Orchestrator) evaluateRollbackLocked() error {
	upgraded, inflight := 0, 0
	for _, rec := range o.st.Devices {
		switch rec.Phase {
		case PhaseUpgraded:
			upgraded++
		case PhaseRollbackInflight:
			inflight++
		}
		// 注意：PhaseUpgradeInflight 不计入未完成。回滚波次期间，在途升级设备
		// 失败会直接落安全终态、成功会先变 upgraded 再在此处补发回滚，
		// 因此这里只需关心“仍在新固件上”与“回滚任务在途”两类设备。
	}
	if upgraded > 0 {
		// upgraded 状态出现在回滚期（升级成功事件与回滚触发的竞态兜底）：补发回滚任务。
		o.redispatchRollbacksLocked()
	}
	if inflight > 0 || upgraded > 0 {
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

// redispatchRollbacksLocked 为仍是 upgraded 的设备补发回滚任务。
func (o *Orchestrator) redispatchRollbacksLocked() {
	for _, rec := range o.st.Devices {
		if rec.Phase != PhaseUpgraded {
			continue
		}
		if rec.RollbackAttempt == 0 {
			rec.RollbackAttempt = 1
		}
		task := Task{
			ID:      taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskRollback, rec.RollbackAttempt),
			Kind:    TaskRollback,
			Device:  rec.DeviceID,
			Batch:   rec.Batch,
			Attempt: rec.RollbackAttempt,
		}
		rec.Phase = PhaseRollbackInflight
		rec.CurrentTask = task.ID
		o.dispatchLocked(task)
	}
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
		o.redispatchRollbacksLocked()
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
