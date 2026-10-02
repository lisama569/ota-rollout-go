package ota

import (
	"fmt"
	"sync"
	"time"
)

// Config 是编排器的配置。
type Config struct {
	Plan      Plan
	Store     Store
	Transport Transport
	Logger    Logger
}

// Orchestrator 是灰度发布的编排器。所有公开方法都在同一把互斥锁下执行，
// 保证并发上报下判定唯一、可复现，也让 -race 下不出现数据竞争。
type Orchestrator struct {
	mu        sync.Mutex
	cfg       Config
	st        *RolloutState
	startedAt time.Time
}

func NewOrchestrator(cfg Config) *Orchestrator {
	if cfg.Logger == nil {
		cfg.Logger = newStdLogger()
	}
	return &Orchestrator{cfg: cfg, startedAt: time.Now()}
}

// persist 在持锁状态下保存状态。Save 失败时返回错误，调用方不得假定状态已落盘。
func (o *Orchestrator) persistLocked() error {
	return o.cfg.Store.Save(o.st)
}

// RecoverOrResume 在启动时加载持久化状态并校验：
//   - 状态不存在：按当前计划初始化（新发布，从批次 0 开始）；
//   - 状态存在：计划指纹必须一致；
//   - 批次位点只允许在已持久化的位置或之后“继续”，任何回退请求一律拒绝。
//
// 因为编排器只会从同一个 Store 恢复自身计划，位点校验主要防御：
// 被持久化文件/外部存储篡改、或有人把另一份（位点更靠后的）状态覆盖进来。
func (o *Orchestrator) RecoverOrResume() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	st, err := o.cfg.Store.Load()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if st == nil {
		o.st = newState(&o.cfg.Plan)
		o.cfg.Logger.Printf("init new rollout id=%s batches=%d target=%s",
			o.st.RolloutID, len(o.cfg.Plan.Batches), o.cfg.Plan.Target)
		if err := o.persistLocked(); err != nil {
			return err
		}
		// 新发布：立即按批次 0 配额开始放量。
		o.dispatchPendingLocked()
		return nil
	}

	if st.RolloutID != o.cfg.Plan.RolloutID {
		o.cfg.Logger.Printf("resume rejected: rollout id mismatch persisted=%s requested=%s",
			st.RolloutID, o.cfg.Plan.RolloutID)
		return ErrPlanChanged
	}
	if st.PlanHash != PlanHash(&o.cfg.Plan) {
		o.cfg.Logger.Printf("resume rejected: plan fingerprint mismatch persisted=%s requested=%s",
			st.PlanHash, PlanHash(&o.cfg.Plan))
		return ErrPlanChanged
	}
	// 批次位点只允许停留在已持久化的位置继续：任何把位点向后回退的请求
	// （计划首批次索引大于持久化位点、或外部篡改了 CurrentBatch）都必须明确拒绝，
	// 不允许从批次中间重新放量。
	requestedStart := BatchIndex(0)
	if len(o.cfg.Plan.Batches) > 0 {
		requestedStart = o.cfg.Plan.Batches[0].Index
	}
	if err := CheckNoRewind(st, requestedStart); err != nil {
		o.cfg.Logger.Printf("resume rejected: %v persistedBatch=%d requestedStart=%d",
			err, st.CurrentBatch, requestedStart)
		return err
	}
	if int(st.CurrentBatch) >= len(o.cfg.Plan.Batches) && st.Phase == RolloutRunning {
		return fmt.Errorf("%w: batch %d out of plan range", ErrInvalidState, st.CurrentBatch)
	}
	o.st = st
	o.cfg.Logger.Printf("resumed rollout id=%s phase=%s currentBatch=%d devices=%d history=%d",
		st.RolloutID, phaseName(st.Phase), st.CurrentBatch, len(st.Devices), len(st.History))
	// 恢复后立即补偿：重放在途任务（相同确定性 TaskID，设备按 TaskID 去重），
	// 若仍在发布期则按当前批次配额继续放量；若在回滚期则按回滚配额补放队列并检查收敛。
	o.redispatchInflightLocked()
	o.dispatchPendingLocked()
	if o.st.Phase == RolloutRollingBack {
		if err := o.evaluateRollbackLocked(); err != nil {
			return err
		}
	}
	return nil
}

// CheckNoRewind 显式校验“批次位点不得回退”：
//   - 请求的起始批次不得晚于已持久化的当前批次（否则意味着跳过/倒带）；
//   - 当前批次不得小于 0。
//
// 该规则单独暴露，便于运维工具在不启动编排器的情况下校验持久化文件。
func CheckNoRewind(st *RolloutState, requestedStart BatchIndex) error {
	if st.CurrentBatch < 0 || requestedStart < 0 {
		return ErrInvalidState
	}
	if requestedStart > st.CurrentBatch {
		return fmt.Errorf("%w: persisted current batch is %d but requested start is %d",
			ErrBatchIndexRolledBack, st.CurrentBatch, requestedStart)
	}
	return nil
}

// State 返回当前状态的深拷贝快照。
func (o *Orchestrator) State() RolloutState {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.st == nil {
		return RolloutState{}
	}
	return o.st.Clone()
}

// StatePtr 返回内部状态指针（已加锁），供同包的恢复/位点校验使用。
// 调用方不得在解锁后继续持有修改。
func (o *Orchestrator) StatePtr() *RolloutState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.st
}

// taskID 生成确定性的任务 ID：相同（设备, 批次, 种类, 尝试号）永远生成同一 ID，
// 因此重启后重放在途任务不会产生新任务，也不会被设备或状态机当成重复下发。
func taskID(rollout string, batch BatchIndex, d DeviceID, kind TaskKind, attempt int) TaskID {
	k := "u"
	if kind == TaskRollback {
		k = "r"
	}
	return TaskID(fmt.Sprintf("%s-b%d-%s-%s-a%d", rollout, batch, k, d, attempt))
}

// phaseName / batchPhaseName 用于日志可读化。
func phaseName(p RolloutPhase) string {
	switch p {
	case RolloutRunning:
		return "running"
	case RolloutRollingBack:
		return "rolling-back"
	case RolloutCompleted:
		return "completed"
	case RolloutAborted:
		return "aborted"
	}
	return "unknown"
}

func batchPhaseName(p BatchPhase) string {
	switch p {
	case BatchPending:
		return "pending"
	case BatchActive:
		return "active"
	case BatchPassed:
		return "passed"
	case BatchRollingBack:
		return "rolling-back"
	case BatchFailed:
		return "failed"
	}
	return "unknown"
}

// HandleReport / Run / 调度与门槛评估在 report.go、engine.go 中实现。
