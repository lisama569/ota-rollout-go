package rollout

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Engine 是灰度发布编排器：负责挑选设备下发、接收上报、健康门槛判定、
// 回滚编排，并在每次状态转移后原子持久化。所有公开方法均串行化内部状态，
// 因此“推进与回滚的判定”在任意并发/乱序上报下都是唯一且可复现的。
type Engine struct {
	mu    sync.Mutex
	store Store
	state *State
	now   func() time.Time
	logf  func(string, ...any)
}

// NewEngine 创建编排器并从 store 恢复：
//   - store 中存在状态：忽略 init，重启续跑到未完成批次；
//     已确认终态的设备保持终态，不会被重复下发或重复计数；
//   - store 为空：用 init 初始化并原子落盘。
//
// 已持久化但尚未得到上报确认的在途任务会被保留为 IN_FLIGHT（等待设备的
// 重复/迟到上报或 Tick 超时重投），不会因为重启而把任务“补发”成两个。
func NewEngine(ctx context.Context, store Store, init *State, logf func(string, ...any)) (*Engine, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rollout state: %w", err)
	}
	if loaded == nil {
		if init == nil {
			return nil, fmt.Errorf("no persisted state and no initial state provided")
		}
		loaded = cloneState(init)
		loaded.UpdatedAt = time.Now()
		if err := store.Save(ctx, loaded); err != nil {
			return nil, fmt.Errorf("persist initial state: %w", err)
		}
		logf("engine init release=%s batches=%d devices=%d phase=%s",
			loaded.ReleaseID, len(loaded.Batches), len(loaded.Devices), loaded.Phase)
	} else {
		// 重启续跑：恢复纪元 +1，使崩溃前在途命令的迟到旧上报可被识别为 STALE。
		loaded.Epoch++
		logf("engine resume release=%s phase=%s current_batch=%d epoch=%d",
			loaded.ReleaseID, loaded.Phase, loaded.Current, loaded.Epoch)
	}
	if len(loaded.Specs) != len(loaded.Batches) {
		return nil, fmt.Errorf("persisted state corrupt: %d specs vs %d batches",
			len(loaded.Specs), len(loaded.Batches))
	}
	requeued := recoverInFlight(loaded)
	eng := &Engine{store: store, state: loaded, now: time.Now, logf: logf}
	if requeued > 0 {
		// 崩溃前在途的命令可能已随进程丢失，统一退回排队并持久化；
		// 重投沿用同一 attempt 代次但属于新 Epoch，因此设备侧对重复命令幂等、
		// 跨纪元的迟到旧上报会被判 STALE 而不是重复计数。
		loaded.UpdatedAt = time.Now()
		if err := store.Save(ctx, loaded); err != nil {
			return nil, fmt.Errorf("persist recovery requeue: %w", err)
		}
		logf("recovery requeued %d in-flight tasks at epoch=%d", requeued, loaded.Epoch)
	}
	return eng, nil
}

// recoverInFlight 把持久化状态中的在途任务退回排队：
//   - ROLLING 阶段的 IN_FLIGHT -> AWAITING（升级重投）；
//   - ROLLING_BACK 阶段的 ROLLING_BACK -> ROLLBACK_QUEUED（回滚重投）。
//
// 已确认终态的设备不受影响，因此不会重复下发或重复计数。
func recoverInFlight(s *State) int {
	n := 0
	for i := range s.Batches {
		b := &s.Batches[i]
		for _, id := range s.Specs[i].DeviceIDs {
			d := s.Devices[id]
			switch d.State {
			case DevInFlight:
				if s.Phase == PhaseRolling || s.Phase == PhasePaused {
					d.State = DevAwaiting
					b.InFlight--
					b.Awaiting++
					n++
				}
			case DevRollingBack:
				if s.Phase == PhaseRollingBack {
					d.State = DevRollbackQueued
					b.RollbackInFlight--
					b.RollbackPending++
					n++
				}
			}
		}
	}
	return n
}

// commit 在持锁状态下打时间戳并原子持久化；位点回退等冲突由 Store 拒绝。
func (e *Engine) commit(ctx context.Context) error {
	e.state.UpdatedAt = e.now()
	if err := e.store.Save(ctx, e.state); err != nil {
		return err
	}
	return nil
}

// Start 将 PENDING 发布单推进到 ROLLING。
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch e.state.Phase {
	case PhasePending:
		e.state.Phase = PhaseRolling
		e.logf("phase -> ROLLING release=%s", e.state.ReleaseID)
		return e.commit(ctx)
	case PhaseRolling, PhasePaused, PhaseRollingBack, PhaseCompleted, PhaseFailed:
		return &ErrInvalidState{Detail: fmt.Sprintf("Start called in phase %s", e.state.Phase)}
	default:
		return &ErrInvalidState{Detail: fmt.Sprintf("unknown phase %s", e.state.Phase)}
	}
}

// Pause 人工暂停放量：暂停期间不再挑选新设备，但上报仍被正常受理。
func (e *Engine) Pause(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state.Phase != PhaseRolling {
		return &ErrInvalidState{Detail: fmt.Sprintf("Pause called in phase %s", e.state.Phase)}
	}
	e.state.Phase = PhasePaused
	e.logf("phase -> PAUSED batch=%d", e.state.Current)
	return e.commit(ctx)
}

// Resume 解除人工暂停。
func (e *Engine) Resume(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state.Phase != PhasePaused {
		return &ErrInvalidState{Detail: fmt.Sprintf("Resume called in phase %s", e.state.Phase)}
	}
	e.state.Phase = PhaseRolling
	e.logf("phase -> ROLLING batch=%d", e.state.Current)
	return e.commit(ctx)
}

// Dispatch 为当前批次挑选待下发设备，返回 deviceID -> 命令 的升级任务集合。
//
// 不变量：
//   - 仅在 ROLLING 阶段下发升级任务；PAUSED/ROLLING_BACK 等阶段返回空集合；
//   - 同一台设备任一时刻最多一个在途任务；
//   - 本批在途总数不超过该批 InFlightQuota；
//   - 挑选顺序按设备 ID 排序，保证每次运行挑选结果确定、可复现。
func (e *Engine) Dispatch(ctx context.Context) (map[string]Command, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	out := map[string]Command{}
	if e.state.Phase != PhaseRolling {
		return out, nil
	}
	ci := e.state.Current
	batch := &e.state.Batches[ci]
	spec := e.state.Specs[ci]
	capacity := spec.InFlightQuota - batch.InFlight
	if capacity <= 0 {
		return out, nil
	}
	// 槽位空闲时，候选 = IDLE（从未下发）+ AWAITING（重试排队/崩溃重投）。

	candidates := make([]string, 0, len(spec.DeviceIDs))
	for _, id := range spec.DeviceIDs {
		// IDLE=本批从未下发；AWAITING=可重试失败后重新排队。
		if st := e.state.Devices[id].State; st == DevIdle || st == DevAwaiting {
			candidates = append(candidates, id)
		}
	}
	sort.Strings(candidates)
	if len(candidates) > capacity {
		candidates = candidates[:capacity]
	}
	now := e.now()
	for _, id := range candidates {
		d := e.state.Devices[id]
		wasAwaiting := d.State == DevAwaiting
		d.State = DevInFlight
		// 每次下发都使用新的命令代次（设备侧对旧 attempt 的迟到/重复上报会被
		// 判为 STALE）；Tries 记录累计尝试次数用于重试上限判定。
		d.Attempt++
		d.Tries++
		d.Epoch = e.state.Epoch
		d.UpdatedAt = now
		if wasAwaiting {
			batch.Awaiting--
		}
		batch.InFlight++
		out[id] = Command{DeviceID: id, Batch: ci, Attempt: d.Attempt, Epoch: d.Epoch}
		e.logf("dispatch device=%s batch=%d attempt=%d tries=%d epoch=%d inflight=%d/%d",
			id, ci, d.Attempt, d.Tries, d.Epoch, batch.InFlight, spec.InFlightQuota)
	}
	batch.Dispatched = true
	if err := e.commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// Report 处理一条设备上报。重复、乱序、迟到上报均通过 Attempt 代次与设备
// 当前状态被识别为 STALE 而忽略，绝不重复计数；无效上报原样拒绝并说明原因。
func (e *Engine) Report(ctx context.Context, r DeviceReport) (AcceptResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	reason, why := classifyReport(e.state, r)
	if reason != ReasonNone {
		e.logf("report rejected device=%s seq=%d outcome=%s reason=%s detail=%s",
			r.DeviceID, r.Seq, r.Outcome, reason, why)
		return AcceptResult{Accepted: false, Reason: reason, Detail: why,
			State: devStateOrUnknown(e.state, r.DeviceID)}, nil
	}

	var res AcceptResult
	switch r.Outcome {
	case OutcomeRollbackDone:
		res = e.applyRollbackDone(ctx, r)
	default:
		res = e.applyUpgradeReport(ctx, r)
	}
	res.Accepted = true
	e.logf("report accepted device=%s seq=%d outcome=%s dev_state=%s batch_decision=%s detail=%s",
		r.DeviceID, r.Seq, r.Outcome, res.State, res.Decision, res.Detail)
	return res, nil
}

// applyUpgradeReport 处理升级类结果（调用前 classifyReport 已通过）。
func (e *Engine) applyUpgradeReport(ctx context.Context, r DeviceReport) AcceptResult {
	d := e.state.Devices[r.DeviceID]
	b := &e.state.Batches[d.Batch]
	now := e.now()

	switch r.Outcome {
	case OutcomeSuccess:
		b.InFlight--
		if e.state.Phase == PhaseRollingBack {
			// 门槛已判死、回滚已启动后才迟到的成功：设备确实换装了，
			// 必须排队回滚，绝不允许它以 SUCCESS 终态游离于回滚之外。
			d.State = DevRollbackQueued
			b.RollbackPending++
			d.Attempt++
			d.Epoch = e.state.Epoch
			e.logf("late success device=%s during ROLLING_BACK -> queued rollback attempt=%d",
				d.ID, d.Attempt)
		} else {
			d.State = DevSucceeded
			b.Succeeded++
			d.Counted = true
		}
		d.UpdatedAt = now

	case OutcomeRollbackSelf:
		// 设备换装后自行回退：已经是旧版本，无需再下发回滚，但要计入回滚率。
		d.State = DevRolledBack
		b.InFlight--
		b.SelfReverted++
		b.RolledBack++
		d.Counted = true
		d.UpdatedAt = now

	case OutcomeFailed:
		b.InFlight--
		if e.state.Phase == PhaseRollingBack {
			// 回滚已触发后才迟到的永久失败：设备未换装，终止升级但不重复影响失败率。
			d.State = DevSkipped
			b.Skipped++
			d.UpdatedAt = now
			e.logf("late permanent failure device=%s during ROLLING_BACK -> SKIPPED", d.ID)
		} else {
			d.State = DevFailed
			b.Failed++
			d.Counted = true
			d.UpdatedAt = now
		}

	case OutcomeIncompatible:
		b.InFlight--
		if e.state.Phase == PhaseRollingBack {
			d.State = DevSkipped
			b.Skipped++
			d.UpdatedAt = now
			e.logf("late incompatible report device=%s during ROLLING_BACK -> SKIPPED", d.ID)
		} else {
			d.State = DevIncompatible
			b.Failed++ // 不可升级视为本批永久失败，参与失败率门槛
			d.Counted = true
			d.UpdatedAt = now
		}

	case OutcomeRetriable:
		b.InFlight--
		if e.state.Phase == PhaseRollingBack {
			// 回滚已触发：不再给该设备重新升级，也不会换装，直接跳过。
			d.State = DevSkipped
			b.Skipped++
			d.UpdatedAt = now
			e.logf("retriable report device=%s during ROLLING_BACK -> SKIPPED", d.ID)
		} else if d.Tries >= e.state.Config.MaxAttempts {
			// 重试次数用尽：转为永久失败终态。
			d.State = DevFailed
			b.Failed++
			d.Counted = true
			d.UpdatedAt = now
			e.logf("retries exhausted device=%s tries=%d -> FAILED", d.ID, d.Tries)
		} else {
			// 可重试失败：退回待下发，稍后由 Dispatch 用新 attempt 重新下发。
			d.State = DevAwaiting
			b.Awaiting++
			d.UpdatedAt = now
		}
	}

	res := AcceptResult{State: d.State, Reason: terminalReason(d.State, r.Outcome)}
	if e.state.Phase == PhaseRollingBack {
		res.Decision, res.Detail = e.advanceRollback(ctx)
	} else {
		res.Decision, res.Detail = e.afterBatchEvent(ctx, d.Batch)
	}
	return res
}

// applyRollbackDone 处理回滚任务完成确认（调用前 classifyReport 已通过）。
func (e *Engine) applyRollbackDone(ctx context.Context, r DeviceReport) AcceptResult {
	d := e.state.Devices[r.DeviceID]
	b := &e.state.Batches[d.Batch]
	d.State = DevRolledBack
	b.RollbackInFlight--
	b.RolledBack++
	d.Counted = true
	d.UpdatedAt = e.now()

	res := AcceptResult{State: DevRolledBack, Reason: ReasonNone}
	res.Decision, res.Detail = e.advanceRollback(ctx)
	return res
}

// afterBatchEvent 在升级类上报落地后评估门槛并执行推进/回滚，返回门槛结论。
func (e *Engine) afterBatchEvent(ctx context.Context, batchIdx int) (GateDecision, string) {
	b := &e.state.Batches[batchIdx]
	if b.GateClosed {
		return b.Decision, "gate already closed: " + string(b.Decision)
	}
	decision, why := evaluateGate(e.state.Config, *b)
	if decision == GatePending {
		// 即使门槛未决，本次上报造成的设备状态转移也必须原子落盘，
		// 否则崩溃重启会丢失“已接受但尚未开门”的确认。
		_ = e.commit(ctx)
		return GatePending, why
	}
	switch decision {
	case GateOpen:
		b.GateClosed = true
		b.Decision = GateOpen
		if batchIdx+1 >= len(e.state.Batches) {
			e.state.Phase = PhaseCompleted
			e.state.Current = batchIdx
			e.logf("gate OPEN final batch=%d -> COMPLETED (%s)", batchIdx, why)
		} else {
			// 批次位点只允许单调前进；旧批次设备全部终态，不存在“从中间继续”。
			e.state.Current = batchIdx + 1
			e.logf("gate OPEN batch=%d -> advance batch=%d (%s)", batchIdx, e.state.Current, why)
		}
		_ = e.commit(ctx)
		return GateOpen, why
	default:
		// GateBlocked / GateRollbackRate：暂停后续放量并触发已升级设备回滚。
		b.GateClosed = true
		b.Decision = decision
		e.startRollback(ctx, batchIdx, why)
		return decision, why
	}
}

// startRollback 将发布单切入 ROLLING_BACK：
//   - 已换装成功（DevSucceeded）的设备进入回滚排队；
//   - 仍在升级在途的设备保留在途：其迟到的成功/失败上报会在 Report 中分流
//     （成功则补排队回滚，可重试/失败则跳过），因此没有任务会被静默丢失；
//   - 尚未换装的待下发设备（含重试中）直接 SKIPPED，不再升级也无需回滚。
func (e *Engine) startRollback(ctx context.Context, batchIdx int, why string) {
	e.state.Phase = PhaseRollingBack
	now := e.now()
	for _, spec := range e.state.Specs {
		b := &e.state.Batches[spec.Index]
		for _, id := range spec.DeviceIDs {
			d := e.state.Devices[id]
			switch d.State {
			case DevSucceeded:
				d.State = DevRollbackQueued
				d.Attempt++ // 回滚使用新一代次，旧升级上报迟到即判 STALE
				d.Epoch = e.state.Epoch
				b.RollbackPending++
				d.UpdatedAt = now
			case DevAwaiting:
				d.State = DevSkipped
				b.Awaiting--
				b.Skipped++
				d.UpdatedAt = now
			case DevIdle:
				// 尚未获得过下发机会的设备：批次已判死，不再升级，也无需回滚。
				d.State = DevSkipped
				b.Skipped++
				d.UpdatedAt = now
			}
		}
	}
	e.logf("gate BLOCKED batch=%d decision=%s cause=%s -> ROLLING_BACK (rollback_pending across batches)",
		batchIdx, e.state.Batches[batchIdx].Decision, why)
	_ = e.commit(ctx)
}

// advanceRollback 在回滚相关事件落地后检查回滚是否全部结束。
// 回滚在途与排队都清零时发布单终态 FAILED。
func (e *Engine) advanceRollback(ctx context.Context) (GateDecision, string) {
	pendingInflight := 0
	for _, b := range e.state.Batches {
		pendingInflight += b.RollbackInFlight + b.RollbackPending + b.InFlight
	}
	if pendingInflight == 0 {
		e.state.Phase = PhaseFailed
		e.logf("rollback drained -> FAILED release=%s rolled_back=%d",
			e.state.ReleaseID, totalRolledBack(e.state))
		_ = e.commit(ctx)
		return GateRollbackRate, "rollback complete, release FAILED"
	}
	// 回滚尚未排空，但本次设备状态转移必须落盘。
	_ = e.commit(ctx)
	return GatePending, fmt.Sprintf(
		"rollback in progress, pending/inflight=%d", pendingInflight)
}

// DispatchRollback 在 ROLLING_BACK 阶段挑选排队中的回滚任务下发。
//
// 每批回滚在途不超过该批 InFlightQuota（整体在途配额约束对回滚同样生效），
// 挑选按设备 ID 排序以保证可复现；已处于 ROLLING_BACK 的设备不会被重复下发。
func (e *Engine) DispatchRollback(ctx context.Context) (map[string]Command, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string]Command{}
	if e.state.Phase != PhaseRollingBack {
		return out, nil
	}
	now := e.now()
	for _, spec := range e.state.Specs {
		b := &e.state.Batches[spec.Index]
		capacity := spec.InFlightQuota - b.RollbackInFlight
		if capacity <= 0 || b.RollbackPending == 0 {
			continue
		}
		ids := make([]string, 0, len(spec.DeviceIDs))
		for _, id := range spec.DeviceIDs {
			if e.state.Devices[id].State == DevRollbackQueued {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		if len(ids) > capacity {
			ids = ids[:capacity]
		}
		for _, id := range ids {
			d := e.state.Devices[id]
			d.State = DevRollingBack
			d.Epoch = e.state.Epoch
			b.RollbackPending--
			b.RollbackInFlight++
			d.UpdatedAt = now
			out[id] = Command{DeviceID: id, Batch: spec.Index, Attempt: d.Attempt,
				Epoch: d.Epoch, Rollback: true}
			e.logf("dispatch rollback device=%s batch=%d attempt=%d epoch=%d rb_inflight=%d/%d",
				id, spec.Index, d.Attempt, d.Epoch, b.RollbackInFlight, spec.InFlightQuota)
		}
	}
	if len(out) > 0 {
		if err := e.commit(ctx); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Snapshot 返回当前持久化状态的深拷贝。
func (e *Engine) Snapshot() *State {
	e.mu.Lock()
	defer e.mu.Unlock()
	return cloneState(e.state)
}

// Tick 处理超时任务，attempt 不变：
//   - ROLLING 阶段：升级在途超时 -> 退回待下发重新调度（设备侧按 attempt 去重）；
//   - ROLLING_BACK 阶段：回滚在途超时 -> 退回回滚排队重新调度。
func (e *Engine) Tick(ctx context.Context, now time.Time, timeout time.Duration) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	requeued := 0
	for _, spec := range e.state.Specs {
		b := &e.state.Batches[spec.Index]
		for _, id := range spec.DeviceIDs {
			d := e.state.Devices[id]
			switch {
			case e.state.Phase == PhaseRolling && d.State == DevInFlight &&
				now.Sub(d.UpdatedAt) >= timeout:
				d.State = DevAwaiting
				b.InFlight--
				b.Awaiting++
				requeued++
				e.logf("upgrade timeout requeue device=%s batch=%d attempt=%d", id, spec.Index, d.Attempt)
			case e.state.Phase == PhaseRollingBack && d.State == DevRollingBack &&
				now.Sub(d.UpdatedAt) >= timeout:
				d.State = DevRollbackQueued
				b.RollbackInFlight--
				b.RollbackPending++
				requeued++
				e.logf("rollback timeout requeue device=%s batch=%d attempt=%d", id, spec.Index, d.Attempt)
			}
		}
	}
	if requeued > 0 {
		if err := e.commit(ctx); err != nil {
			return 0, err
		}
	}
	return requeued, nil
}

func devStateOrUnknown(s *State, id string) DeviceState {
	if d, ok := s.Devices[id]; ok {
		return d.State
	}
	return DeviceState("UNKNOWN")
}

func terminalReason(st DeviceState, o ReportOutcome) RejectReason {
	switch st {
	case DevFailed:
		if o == OutcomeRetriable {
			return ReasonRetriesExhausted
		}
		return ReasonNone
	case DevIncompatible:
		return ReasonIncompatibleResult
	}
	return ReasonNone
}

func totalRolledBack(s *State) int {
	n := 0
	for _, b := range s.Batches {
		n += b.RolledBack
	}
	return n
}
