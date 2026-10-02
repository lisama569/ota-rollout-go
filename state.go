package ota

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// DevicePhase 是单台设备在发布过程中的生命周期阶段。
type DevicePhase int

const (
	PhasePending DevicePhase = iota
	PhaseUpgradeInflight
	PhaseUpgraded
	// PhaseRollbackQueued 设备需要回滚，但当前批次在途配额已满，按确定性顺序排队等待。
	PhaseRollbackQueued
	PhaseRollbackInflight
	// PhaseRollbackWaiting 上一次回滚任务已确认失败，在固定重试节奏内等待下一次重投。
	// 仍占用该设备的回滚在途名额（不把名额让给队列后续设备）。
	PhaseRollbackWaiting
	PhaseDone
	PhaseIncompatible
	// PhaseFailed 表示可重试失败已耗尽，设备无法完成升级的终态。
	PhaseFailed
	// PhaseRollbackFailed 回滚重试预算耗尽的人工介入终态：设备可能仍停在新固件上。
	PhaseRollbackFailed
)

// DeviceRecord 是单台设备的持久化状态。
type DeviceRecord struct {
	DeviceID        DeviceID    `json:"device_id"`
	Batch           BatchIndex  `json:"batch"`
	Phase           DevicePhase `json:"phase"`
	CurrentTask     TaskID      `json:"current_task"`
	Attempt         int         `json:"attempt"`
	RollbackAttempt int         `json:"rollback_attempt"`
	// Baseline 是设备升级前运行的版本（回滚基线）：从设备对升级任务的首帧
	// 非空 FromVer 中记录，一旦记录不再改变。回滚成功帧的落地版本必须等于它。
	Baseline FirmwareVersion `json:"baseline"`
	// NextAttemptAt 是回滚失败后下一次允许重投的最早时刻；零值表示立即可投。
	NextAttemptAt time.Time `json:"next_attempt_at"`
	// Restored 标记设备经编排器回滚确认已回到基线版本（区别于“从未升级”的 Done）。
	Restored   bool         `json:"restored"`
	Success    bool         `json:"success"`
	Class      FailureClass `json:"class"`
	LastReason string       `json:"last_reason"`
	LastSeq    uint64       `json:"last_seq"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

// BatchPhase 是批次的生命周期阶段。
type BatchPhase int

const (
	BatchPending BatchPhase = iota
	BatchActive
	BatchPassed
	BatchRollingBack
	BatchFailed
)

// BatchState 是单个批次的持久化状态。
type BatchState struct {
	Index      BatchIndex        `json:"index"`
	Phase      BatchPhase        `json:"phase"`
	Dispatched map[DeviceID]bool `json:"dispatched"`
	// sent 只存在于内存（json:"-"）：记录已在本进程实际投递过的 TaskID。
	// 重启后在途任务会以相同的确定性 TaskID 重投，设备/状态机按 TaskID 去重，
	// 因此重投不会造成重复计数。
	sent map[TaskID]bool `json:"-"`
}

// RolloutPhase 是整个发布的生命周期阶段。
type RolloutPhase int

const (
	RolloutRunning RolloutPhase = iota
	RolloutRollingBack
	RolloutCompleted
	RolloutAborted
)

// maxHistoryEntries 限制审计历史长度，防止长跑进程内存无限增长。
const maxHistoryEntries = 4096

// RolloutState 是需要持久化的完整编排状态。
type RolloutState struct {
	RolloutID    string                     `json:"rollout_id"`
	PlanHash     string                     `json:"plan_hash"`
	CurrentBatch BatchIndex                 `json:"current_batch"`
	Phase        RolloutPhase               `json:"phase"`
	Devices      map[DeviceID]*DeviceRecord `json:"devices"`
	Batches      map[BatchIndex]*BatchState `json:"batches"`
	// RollbackQueue 是回滚波次的等待队列：严格按（批次序号, 批次内名单位置）
	// 排序，与事件到达顺序无关，因此排队顺序固定、可复现，重启后原样恢复。
	RollbackQueue []DeviceID `json:"rollback_queue"`
	History       []Decision `json:"history"`
}

// RollbackStatus 是回滚链路一眼可看明白的在途/排队视图，由状态实时派生（不持久化）。
type RollbackStatus struct {
	// Queued 排队等待配额的设备（确定性顺序）。
	Queued []DeviceID
	// Inflight 回滚任务已下发、等待设备终态的设备。
	Inflight []DeviceID
	// Waiting 已失败、在固定重试节奏内等待下一次重投的设备。
	Waiting []DeviceID
	// Restored 已确认回到基线上的设备。
	Restored []DeviceID
	// Manual 回滚预算耗尽、需要人工介入的设备。
	Manual []DeviceID
}

// RollbackStatusLocked 派生平分级的回滚视图，供运维/日志/测试使用。
func (o *Orchestrator) RollbackStatus() RollbackStatus {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.rollbackStatusLocked()
}

func (o *Orchestrator) rollbackStatusLocked() RollbackStatus {
	s := RollbackStatus{}
	// 队列直接按持久化顺序给出；其余视图按全局确定性顺序（批次、名单）给出。
	s.Queued = append(s.Queued, o.st.RollbackQueue...)
	for _, b := range o.cfg.Plan.Batches {
		for _, id := range b.Devices {
			rec := o.st.Devices[id]
			switch rec.Phase {
			case PhaseRollbackInflight:
				s.Inflight = append(s.Inflight, id)
			case PhaseRollbackWaiting:
				s.Waiting = append(s.Waiting, id)
			case PhaseDone:
				if rec.Restored {
					s.Restored = append(s.Restored, id)
				}
			case PhaseRollbackFailed:
				s.Manual = append(s.Manual, id)
			}
		}
	}
	return s
}

// PlanHash 计算计划的指纹。重启恢复时计划必须与持久化时逐字节一致，
// 防止换了一份计划却从旧位点继续。
func PlanHash(p *Plan) string {
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// newState 按计划构造一份全新的编排状态（批次 0 为 Active）。
func newState(p *Plan) *RolloutState {
	st := &RolloutState{
		RolloutID:    p.RolloutID,
		PlanHash:     PlanHash(p),
		CurrentBatch: 0,
		Phase:        RolloutRunning,
		Devices:      make(map[DeviceID]*DeviceRecord),
		Batches:      make(map[BatchIndex]*BatchState),
	}
	for _, b := range p.Batches {
		st.Batches[b.Index] = &BatchState{
			Index:      b.Index,
			Phase:      BatchPending,
			Dispatched: make(map[DeviceID]bool),
			sent:       make(map[TaskID]bool),
		}
		for _, d := range b.Devices {
			st.Devices[d] = &DeviceRecord{DeviceID: d, Batch: b.Index, Phase: PhasePending}
		}
	}
	if len(p.Batches) > 0 {
		st.Batches[0].Phase = BatchActive
	}
	return st
}

// recordDecision 追加一条审计判定，超出上限时丢弃最旧的条目。
func (st *RolloutState) recordDecision(d Decision) {
	st.History = append(st.History, d)
	if len(st.History) > maxHistoryEntries {
		st.History = st.History[len(st.History)-maxHistoryEntries:]
	}
}

// Clone 返回状态的深拷贝，用于对外快照，避免外部读到可变内部状态。
func (st *RolloutState) Clone() RolloutState {
	raw, _ := json.Marshal(st)
	var cp RolloutState
	_ = json.Unmarshal(raw, &cp)
	return cp
}
