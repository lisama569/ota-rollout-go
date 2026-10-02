package ota

import (
	"context"
	"fmt"
	"sort"
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

// inFlightLocked 统计某批次当前在途的升级任务数（仅升级链路）。
func (o *Orchestrator) inFlightLocked(b BatchIndex) int {
	n := 0
	for _, d := range o.st.Devices {
		if d.Batch == b && d.Phase == PhaseUpgradeInflight {
			n++
		}
	}
	return n
}

// defaultRollbackRetryInterval 是回滚失败后固定重试节奏的默认值。
const defaultRollbackRetryInterval = 100 * time.Millisecond

// rollbackBudgetLocked 返回每台设备的回滚尝试总次数上限（独立于升级预算）。
func (o *Orchestrator) rollbackBudgetLocked() int {
	if o.cfg.Plan.MaxRollbackAttempts > 0 {
		return o.cfg.Plan.MaxRollbackAttempts
	}
	return o.cfg.Plan.MaxAttempts
}

// rollbackIntervalLocked 返回回滚失败后下一次重投的固定间隔。
func (o *Orchestrator) rollbackIntervalLocked() time.Duration {
	if o.cfg.Plan.RollbackRetryInterval > 0 {
		return o.cfg.Plan.RollbackRetryInterval
	}
	return defaultRollbackRetryInterval
}

// deviceRankLocked 返回设备在整个发布计划中的全局名次（批次序号 + 批次内名单位置），
// 是回滚排队顺序的唯一依据：与事件到达顺序无关，保证固定、可复现。
func (o *Orchestrator) deviceRankLocked(id DeviceID) int {
	rank := 0
	for _, b := range o.cfg.Plan.Batches {
		for off, d := range b.Devices {
			if d == id {
				return rank + off
			}
		}
		rank += len(b.Devices)
	}
	return int(^uint(0) >> 1) // 计划外设备排到最后（正常不会出现）
}

// sortRollbackQueueLocked 按全局名次稳定排序回滚队列。
func (o *Orchestrator) sortRollbackQueueLocked() {
	q := o.st.RollbackQueue
	sort.SliceStable(q, func(i, j int) bool {
		return o.deviceRankLocked(q[i]) < o.deviceRankLocked(q[j])
	})
	o.st.RollbackQueue = q
}

// enqueueRollbackLocked 把一台已在新固件上的设备放入回滚等待队列（幂等）。
// 只有 PhaseUpgraded 的设备能入队；已在队列/在途/终态的设备不会被重复加入，
// 因此已确认回滚完成的设备不会被重复下发或重复计数。
func (o *Orchestrator) enqueueRollbackLocked(rec *DeviceRecord) bool {
	if rec.Phase != PhaseUpgraded {
		return false
	}
	for _, id := range o.st.RollbackQueue {
		if id == rec.DeviceID {
			return false
		}
	}
	rec.Phase = PhaseRollbackQueued
	rec.CurrentTask = ""
	rec.UpdatedAt = timeNow()
	o.st.RollbackQueue = append(o.st.RollbackQueue, rec.DeviceID)
	o.sortRollbackQueueLocked()
	o.cfg.Logger.Printf("rollback enqueue device=%s batch=%d queuePos=%d baseline=%s",
		rec.DeviceID, rec.Batch, o.queuePositionLocked(rec.DeviceID), rec.Baseline)
	return true
}

// queuePositionLocked 返回设备在回滚队列中的从 1 开始的位置，不在队列返回 0。
func (o *Orchestrator) queuePositionLocked(id DeviceID) int {
	for i, q := range o.st.RollbackQueue {
		if q == id {
			return i + 1
		}
	}
	return 0
}

// rollbackOccupiedLocked 统计回滚波次当前占用在途配额的设备数（全发布范围）：
//   - 当前批次仍在途的升级任务（回滚触发时已在途、等终态的升级设备）；
//   - 回滚在途设备；
//   - 回滚失败后等待定时重试的设备（等待期间不释放名额，避免名额被队列后续设备占用）。
//
// 升级与回滚两条链路共享当前批次配额，整体在途数永远不超过配额，不会把设备打满。
func (o *Orchestrator) rollbackOccupiedLocked() int {
	n := 0
	for _, rec := range o.st.Devices {
		switch rec.Phase {
		case PhaseRollbackInflight, PhaseRollbackWaiting:
			n++
		case PhaseUpgradeInflight:
			if rec.Batch == o.st.CurrentBatch {
				n++
			}
		}
	}
	return n
}

// admitRollbackTaskLocked 在配额内为一台排队设备生成并下发回滚任务。
func (o *Orchestrator) admitRollbackTaskLocked(rec *DeviceRecord) {
	if rec.RollbackAttempt == 0 {
		rec.RollbackAttempt = 1
	}
	task := Task{
		ID:      taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, TaskRollback, rec.RollbackAttempt),
		Kind:    TaskRollback,
		Device:  rec.DeviceID,
		Batch:   rec.Batch,
		Attempt: rec.RollbackAttempt,
		Target:  rec.Baseline,
	}
	rec.Phase = PhaseRollbackInflight
	rec.CurrentTask = task.ID
	rec.NextAttemptAt = time.Time{}
	rec.UpdatedAt = timeNow()
	o.dispatchLocked(task)
}

// redriveWaitingLocked 把已到固定重试时刻的等待设备原地重投（不新占名额、不增加尝试号）。
func (o *Orchestrator) redriveWaitingLocked(now time.Time) int {
	n := 0
	for _, rec := range o.st.Devices {
		if rec.Phase != PhaseRollbackWaiting {
			continue
		}
		if !rec.NextAttemptAt.IsZero() && now.Before(rec.NextAttemptAt) {
			continue
		}
		o.cfg.Logger.Printf("rollback redrive device=%s attempt=%d (scheduled at %s)",
			rec.DeviceID, rec.RollbackAttempt, rec.NextAttemptAt.Format("15:04:05.000"))
		o.admitRollbackTaskLocked(rec)
		n++
	}
	return n
}

// pumpRollbacksLocked 是回滚波次的配额泵，每个 tick / 设备事件后执行：
//  1. 兜底：回滚期任何仍停在 Upgraded 的设备先入队（升级成功与回滚触发的竞态）；
//  2. 定时重试：等待中的设备到点原地重投；
//  3. 配额放行：严格按队列顺序，在当前批次配额剩余名额内逐台放行排队设备；
//     前一台设备回到基线释放名额后，下一台才会被补入。
func (o *Orchestrator) pumpRollbacksLocked() {
	if o.st.Phase != RolloutRollingBack {
		return
	}
	now := timeNow()

	// 1) 竞态兜底：回滚期升级才成功的设备入队。
	for _, b := range o.cfg.Plan.Batches {
		for _, id := range b.Devices {
			if rec := o.st.Devices[id]; rec.Phase == PhaseUpgraded {
				o.enqueueRollbackLocked(rec)
			}
		}
	}

	// 2) 到点的定时重试先重投（占用的是它自己原来的名额）。
	o.redriveWaitingLocked(now)

	// 3) 队列按固定顺序在配额内放行。
	quota := 0
	if int(o.st.CurrentBatch) < len(o.cfg.Plan.Batches) {
		quota = o.cfg.Plan.Batches[int(o.st.CurrentBatch)].MaxInFlight
	}
	remaining := o.st.RollbackQueue[:0]
	occupied := o.rollbackOccupiedLocked()
	admitted := 0
	for _, id := range o.st.RollbackQueue {
		rec := o.st.Devices[id]
		// 设备可能已在其他路径离开队列（正常不会），出队时做一次权威核对。
		if rec.Phase != PhaseRollbackQueued {
			continue
		}
		if occupied >= quota {
			remaining = append(remaining, id)
			continue
		}
		o.admitRollbackTaskLocked(rec)
		occupied++
		admitted++
	}
	o.st.RollbackQueue = remaining
	if admitted > 0 {
		o.cfg.Logger.Printf("rollback pump admitted=%d occupied=%d/%d queued=%d",
			admitted, occupied, quota, len(remaining))
	}
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
		case PhaseRollbackInflight, PhaseRollbackQueued, PhaseRollbackWaiting:
			// 回滚链路上的设备不计入升级评估的终态样本；在途升级才占 inflight。
		case PhaseRollbackFailed:
			c.failed++
			c.evaluated++
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

// triggerRollbackLocked 暂停后续放量，把所有“已经升级到新版本”的设备按
// （批次, 名单位置）的固定顺序放入回滚等待队列，再由配额泵逐台放行。
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
				o.enqueueRollbackLocked(rec)
			case PhasePending:
				// 还没开始升级的设备无需回滚，标记安全终态。
				rec.Phase = PhaseDone
				rec.Success = false
				rec.LastReason = "rollback wave: never upgraded"
				rec.UpdatedAt = timeNow()
			case PhaseUpgradeInflight:
				// 在途升级设备等待其终态上报后由事件处理进入回滚队列（若成功）
				// 或落终态（若失败）。
			}
		}
	}
	if err := o.persistLocked(); err != nil {
		return err
	}
	// 立即按当前批次配额放行队列头部设备（回滚与在途升级共享配额）。
	o.pumpRollbacksLocked()
	return o.persistLocked()
}

// markAbortedLocked 在回滚无法完成（回滚预算耗尽）时将发布置为人工介入中止态。
// manual=true 表示有设备可能仍停在新固件上，必须人工处理；false 表示
// 所有需要回滚的设备都已回到基线，是安全中止。
func (o *Orchestrator) markAbortedLocked(reason string, manual bool) {
	o.st.Phase = RolloutAborted
	if bs := o.st.Batches[o.st.CurrentBatch]; bs != nil && bs.Phase == BatchRollingBack {
		bs.Phase = BatchFailed
	}
	kind := DecisionAborted
	d := reason
	if manual {
		d = "MANUAL INTERVENTION REQUIRED: " + reason
	}
	o.st.recordDecision(Decision{At: timeNow(), Batch: o.st.CurrentBatch,
		Kind: kind, Reason: d})
	if manual {
		o.cfg.Logger.Printf("ROLLOUT ABORTED (manual intervention required) reason=%s", reason)
	} else {
		o.cfg.Logger.Printf("ROLLOUT ABORTED reason=%s", reason)
	}
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

// evaluateRollbackLocked 驱动配额泵并判断回滚波次是否结束：
//   - 仍有设备停在 Upgraded / 在队列 / 回滚在途 / 等待重试 => 继续受控推进；
//   - 任一设备进入 PhaseRollbackFailed（预算耗尽）=> 立即人工中止，不再推进其余设备；
//   - 所有需要回滚的设备都回到基线上（或本就不在新固件上）=> 安全中止。
func (o *Orchestrator) evaluateRollbackLocked() error {
	o.pumpRollbacksLocked()

	upgraded, upInflight, queued, inflight, waiting, manual := 0, 0, 0, 0, 0, 0
	for _, rec := range o.st.Devices {
		switch rec.Phase {
		case PhaseUpgraded:
			upgraded++
		case PhaseUpgradeInflight:
			// 回滚触发时已在途的升级任务：它可能成功（随后必须回滚）也可能失败
			//（留旧版 Done）。在终态前它是未决设备，回滚波次绝不能提前宣告安全收敛。
			upInflight++
		case PhaseRollbackQueued:
			queued++
		case PhaseRollbackInflight:
			inflight++
		case PhaseRollbackWaiting:
			waiting++
		case PhaseRollbackFailed:
			manual++
		}
	}
	st := o.rollbackStatusLocked()
	o.cfg.Logger.Printf("rollback gate upgraded=%d upgradeInflight=%d queued=%d inflight=%d waiting=%d restored=%d manual=%d statusQ=%v",
		upgraded, upInflight, queued, inflight, waiting, len(st.Restored), manual, st.Queued)

	// 预算耗尽的设备存在 => 已是人工介入终态（markAbortedLocked 已落盘），保持。
	if manual > 0 || o.st.Phase == RolloutAborted {
		return nil
	}
	if upgraded+upInflight+queued+inflight+waiting > 0 {
		return nil
	}
	// 所有设备都已离开新固件：回滚波次安全收敛，批次标记失败，发布中止。
	cur := o.st.CurrentBatch
	o.st.Batches[cur].Phase = BatchFailed
	o.st.Phase = RolloutAborted
	o.st.recordDecision(Decision{At: timeNow(), Batch: cur, Kind: DecisionAborted,
		Reason: "rollback wave finished: all upgraded devices were restored to their baseline"})
	o.cfg.Logger.Printf("ROLLBACK WAVE FINISHED batch=%d, rollout aborted safely", cur)
	return o.persistLocked()
}

// redispatchInflightLocked 在重启恢复后重放在途任务：
// DeviceRecord 显示在途、但本进程 transport 尚未投递过该 TaskID 时重投。
// TaskID 确定 => 同一任务；设备按 TaskID 去重 => 不会重复计数。
// 注意：PhaseRollbackWaiting 的设备必须等到持久化的 NextAttemptAt 才重投，
// 因此重启不会提前补打一轮；其定时重投由 tick 中的 pump 负责。
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
			// PhaseRollbackWaiting 不在此重放：它必须等到持久化的 NextAttemptAt
			// 由 pumpRollbacksLocked 定时重投，否则重启/tick 会提前多打一轮。
			continue
		}
		expectID := taskID(o.st.RolloutID, rec.Batch, rec.DeviceID, kind, attempt)
		if rec.CurrentTask != expectID || bs.sent[expectID] {
			continue
		}
		target := o.cfg.Plan.Target
		if kind == TaskRollback {
			target = rec.Baseline
		}
		o.dispatchLocked(Task{
			ID: expectID, Kind: kind, Device: rec.DeviceID,
			Batch: rec.Batch, Attempt: attempt, Target: target,
		})
	}
}

// normalizeRollbackLocked 重启恢复时对账回滚队列：
// 已在队列中的 PhaseRollbackQueued 设备保留；仍停在 Upgraded 的设备补入队列；
// 已离开新固件（终态/在途/等待）的设备若残留在队列中则剔除。顺序仍按全局名次，
// 保证重启后排队与在途状态接着走、顺序可复现。
func (o *Orchestrator) normalizeRollbackLocked() {
	seen := make(map[DeviceID]bool, len(o.st.RollbackQueue))
	kept := o.st.RollbackQueue[:0]
	for _, id := range o.st.RollbackQueue {
		rec, ok := o.st.Devices[id]
		if !ok {
			continue
		}
		if rec.Phase != PhaseRollbackQueued {
			continue
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		kept = append(kept, id)
	}
	o.st.RollbackQueue = kept
	if o.st.Phase == RolloutRollingBack {
		for _, b := range o.cfg.Plan.Batches {
			for _, id := range b.Devices {
				if rec := o.st.Devices[id]; rec.Phase == PhaseUpgraded {
					o.enqueueRollbackLocked(rec)
				}
			}
		}
		o.sortRollbackQueueLocked()
	}
}

// tickLocked 执行一轮补偿：补在途、补待下发、并在回滚期按配额泵推进与检查收敛。
func (o *Orchestrator) tickLocked() {
	o.redispatchInflightLocked()
	if o.st.Phase == RolloutRunning {
		o.dispatchPendingLocked()
	} else if o.st.Phase == RolloutRollingBack {
		o.normalizeRollbackLocked()
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
