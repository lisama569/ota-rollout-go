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
	PhaseRollbackInflight
	PhaseDone
	PhaseIncompatible
	// PhaseFailed 表示可重试失败已耗尽，设备无法完成升级的终态。
	PhaseFailed
)

// DeviceRecord 是单台设备的持久化状态。
type DeviceRecord struct {
	DeviceID        DeviceID     `json:"device_id"`
	Batch           BatchIndex   `json:"batch"`
	Phase           DevicePhase  `json:"phase"`
	CurrentTask     TaskID       `json:"current_task"`
	Attempt         int          `json:"attempt"`
	RollbackAttempt int          `json:"rollback_attempt"`
	Success         bool         `json:"success"`
	Class           FailureClass `json:"class"`
	LastReason      string       `json:"last_reason"`
	LastSeq         uint64       `json:"last_seq"`
	UpdatedAt       time.Time    `json:"updated_at"`
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
	History      []Decision                 `json:"history"`
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
