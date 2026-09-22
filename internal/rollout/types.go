// Package rollout implements a local firmware staged rollout orchestrator.
//
// 状态机核心对象、结果分类原因与错误定义集中在本文件；判定逻辑在 engine.go，
// 持久化在 store.go，测试辅助在 sim 包。
package rollout

import "time"

// Phase 是发布单生命周期中的阶段。
type Phase string

const (
	// PhasePending 发布单已创建但尚未开始。
	PhasePending Phase = "PENDING"
	// PhaseRolling 正在按批次放量。
	PhaseRolling Phase = "ROLLING"
	// PhaseRollingBack 已触发失败回滚，正在回收已升级设备。
	PhaseRollingBack Phase = "ROLLING_BACK"
	// PhaseCompleted 全部批次放量成功。
	PhaseCompleted Phase = "COMPLETED"
	// PhaseFailed 回滚完成后发布单终态失败。
	PhaseFailed Phase = "FAILED"
	// PhasePaused 人工暂停（用于演示与测试暂停后续放量）。
	PhasePaused Phase = "PAUSED"
)

// Terminal 报告该阶段是否为终态。
func (p Phase) Terminal() bool { return p == PhaseCompleted || p == PhaseFailed }

// DeviceState 是单台设备在发布过程中的状态。
type DeviceState string

const (
	// DevIdle 尚未被选中升级。
	DevIdle DeviceState = "IDLE"
	// DevIncompatible 协议版本高于编排器，判定为不可升级的终态。
	DevIncompatible DeviceState = "INCOMPATIBLE"
	// DevAwaiting 在批次成员集合内，尚未下发（等待在途配额）。
	DevAwaiting DeviceState = "AWAITING"
	// DevInFlight 已有一个升级任务在途（同一设备最多一个）。
	DevInFlight DeviceState = "IN_FLIGHT"
	// DevSucceeded 升级成功（终态，不重复下发、不重复计数）。
	DevSucceeded DeviceState = "SUCCESS"
	// DevFailed 升级永久失败（终态，计入失败率，但设备未换装无需回滚）。
	DevFailed DeviceState = "FAILED"
	// DevSkipped 触发回滚时仍未换装的设备（重试中/未下发）：不再升级，终态，不计数。
	DevSkipped DeviceState = "SKIPPED"
	// DevRollbackQueued 已换装成功且回滚已触发，回滚任务排队等待下发。
	DevRollbackQueued DeviceState = "ROLLBACK_QUEUED"
	// DevRollingBack 回滚任务在途（同一设备最多一个在途任务）。
	DevRollingBack DeviceState = "ROLLING_BACK"
	// DevRolledBack 设备已回滚到旧版本（终态，计入回滚率）。
	DevRolledBack DeviceState = "ROLLED_BACK"
)

// RejectReason 为“上报无效 / 设备不兼容 / 可重试失败”三类判定提供可区分原因。
type RejectReason string

const (
	// ReasonNone 上报被接受。
	ReasonNone RejectReason = ""
	// ReasonProtoTooNew 设备协议版本高于编排器支持的最高版本：设备不兼容、不可升级。
	ReasonProtoTooNew RejectReason = "INCOMPATIBLE_PROTO_TOO_NEW"
	// ReasonMissingField 上报缺少必填字段：上报无效。
	ReasonMissingField RejectReason = "INVALID_MISSING_FIELD"
	// ReasonMalformed 上报字段内容畸形（如空结果、未知枚举）：上报无效。
	ReasonMalformed RejectReason = "INVALID_MALFORMED"
	// ReasonStale 迟到上报：任务代次过期或设备当前无在途任务，忽略而非计数。
	ReasonStale RejectReason = "STALE_REPORT"
	// ReasonBatchMismatch 上报所属批次与设备当前批次不一致：迟到/串号上报，忽略。
	ReasonBatchMismatch RejectReason = "BATCH_MISMATCH"
	// ReasonRetriable 升级瞬时失败：可重试，未超过尝试上限前不做终态计数。
	ReasonRetriable RejectReason = "RETRIABLE_FAILURE"
	// ReasonRetriesExhausted 重试次数用尽：转为永久失败终态。
	ReasonRetriesExhausted RejectReason = "RETRIES_EXHAUSTED"
	// ReasonIncompatibleResult 设备成功握手但明确上报不兼容：不可升级终态。
	ReasonIncompatibleResult RejectReason = "INCOMPATIBLE_DEVICE"
)

// Command 是编排器下发给一台设备的任务（升级或回滚）。
type Command struct {
	DeviceID string
	Batch    int
	Attempt  int
	Epoch    int64
	Rollback bool
}

// DeviceReport 是设备经由传输通道发回的原始上报。
type DeviceReport struct {
	// DeviceID 设备唯一标识。
	DeviceID string
	// BatchIndex 上报声称所属的批次（可能伪造/迟到，需要与编排状态核对）。
	BatchIndex int
	// Attempt 本次任务的下发代次（每次下发递增），用于识别迟到/重复上报。
	Attempt int
	// Epoch 为命令下发时的进程恢复纪元，由设备原样带回；与设备当前纪元不符
	// 的上报是跨重启的迟到旧上报，判 STALE。
	Epoch int64
	// ProtoVersion 设备当前协议主版本。
	ProtoVersion int
	// Outcome 设备上报的结果。
	Outcome ReportOutcome
	// Firmware 设备当前正在运行的固件版本，用于核对换装/回滚是否真实生效。
	Firmware string
	// Seq 传输通道给上报分配的全局序号，仅用于日志与可复现排序，不参与判定。
	Seq int64
	// At 上报发生时间。
	At time.Time
}

// ReportOutcome 为上报结果枚举。
type ReportOutcome string

const (
	// OutcomeSuccess 设备确认已运行目标固件。
	OutcomeSuccess ReportOutcome = "SUCCESS"
	// OutcomeRetriable 下载/校验等瞬时失败，可重新下发。
	OutcomeRetriable ReportOutcome = "RETRIABLE"
	// OutcomeFailed 设备明确永久失败（硬件不支持等），不再重试。
	OutcomeFailed ReportOutcome = "FAILED"
	// OutcomeIncompatible 设备握手成功但声明无法接受该固件。
	OutcomeIncompatible ReportOutcome = "INCOMPATIBLE"
	// OutcomeRollbackSelf 设备换装后自行回退到旧固件（计入回滚率）。
	OutcomeRollbackSelf ReportOutcome = "ROLLBACK_SELF"
	// OutcomeRollbackDone 设备确认编排器下发的回滚任务完成。
	OutcomeRollbackDone ReportOutcome = "ROLLBACK_DONE"
)

// GateDecision 为批次健康门槛判定结论。
type GateDecision string

const (
	// GateOpen 当前批次已达成健康门槛，允许进入下一批。
	GateOpen GateDecision = "OPEN"
	// GatePending 批次仍有在途任务，证据不足，继续等待。
	GatePending GateDecision = "PENDING"
	// GateBlocked 失败率超阈值：暂停放量并触发回滚。
	GateBlocked GateDecision = "BLOCKED_FAILURE_RATE"
	// GateRollbackRate 自回滚率超阈值：暂停放量并触发回滚。
	GateRollbackRate GateDecision = "BLOCKED_ROLLBACK_RATE"
)

// BatchSpec 描述一个批次的成员与配额。
type BatchSpec struct {
	// Index 批次序号，从 0 开始。
	Index int
	// DeviceIDs 本批次纳入升级范围的设备。
	DeviceIDs []string
	// InFlightQuota 本批次同时允许的在途任务上限（整体在途不超过当前批次配额）。
	InFlightQuota int
}

// Config 为发布策略配置。
type Config struct {
	// MaxProtoVersion 编排器支持的最高设备协议版本；更高版本设备不兼容。
	MaxProtoVersion int
	// MaxAttempts 单台设备可重试下发的最大次数（>=1）。
	MaxAttempts int
	// SuccessThreshold 进入下一批要求的最低成功比例（0,1]。
	SuccessThreshold float64
	// MaxFailureRate 允许的最高永久失败比例，超过即触发回滚（[0,1]）。
	MaxFailureRate float64
	// MaxRollbackRate 允许的最高自回滚/已回滚比例，超过即触发回滚（[0,1]）。
	MaxRollbackRate float64
	// TargetFirmware 本次发布的目标固件版本。
	TargetFirmware string
	// BaselineFirmware 回滚目标（旧）固件版本。
	BaselineFirmware string
}

// DeviceProgress 是单台设备的持久化进展。
type DeviceProgress struct {
	ID    string
	Batch int
	State DeviceState
	// Attempt 是当前在途命令的代次：每次（重新）下发都递增，用于设备侧去重
	// 与引擎识别迟到/重复上报。
	Attempt int
	// Epoch 是当前在途命令所属的进程恢复纪元。
	Epoch int64
	// Tries 是升级已尝试的总次数，达到 MaxAttempts 后可重试失败转为永久失败。
	Tries int
	Proto int
	// Counted 为 true 表示该设备的终态已计入批次汇总，重复上报不得重复计数。
	Counted   bool
	UpdatedAt time.Time
}

// BatchProgress 是单个批次的持久化位点。
type BatchProgress struct {
	Index        int
	Total        int
	Succeeded    int
	Failed       int
	SelfReverted int
	RolledBack   int
	// InFlight / Awaiting 统计升级任务：在途 / 等待下发。
	InFlight int
	Awaiting int
	// Skipped 为触发回滚时仍未换装、被终止升级的设备数（不参与成败计数）。
	Skipped int
	// RollbackPending / RollbackInFlight 统计回滚任务：待回滚的成功设备 / 回滚在途。
	RollbackPending  int
	RollbackInFlight int
	// Dispatched 本批次是否已开始挑选设备下发。
	Dispatched bool
	// GateClosed 健康门槛是否已做出终局判定。
	GateClosed bool
	Decision   GateDecision
}

// State 是需要持久化的全部编排状态；每次状态转移都整体原子落盘。
type State struct {
	ReleaseID string
	Phase     Phase
	// Epoch 是进程恢复纪元：每次从持久化状态重启都 +1。下发命令携带当前纪元，
	// 设备上报原样带回；跨纪元的在途旧上报判 STALE，避免与重投命令混淆。
	Epoch int64
	// Specs 为批次静态规格（成员与在途配额），恢复后仍需据此继续调度。
	Specs     []BatchSpec
	Batches   []BatchProgress
	Devices   map[string]*DeviceProgress
	Current   int // 当前推进到的批次下标（单调不减）
	Config    Config
	UpdatedAt time.Time
}

// AcceptResult 是一次上报被处理后的结构化结论，供日志解释“判定依据”。
type AcceptResult struct {
	Accepted bool
	Reason   RejectReason
	State    DeviceState
	Decision GateDecision
	Detail   string
}

// ErrBatchRegress 在批次位点被要求回退（小于已提交位点）时返回。
type ErrBatchRegress struct{ From, To int }

func (e *ErrBatchRegress) Error() string { return "batch cursor regression rejected" }

// ErrConflict 表示并发调用与已提交状态冲突（例如重复恢复、状态版本过期）。
type ErrConflict struct{ Detail string }

func (e *ErrConflict) Error() string { return "state conflict: " + e.Detail }

// ErrInvalidState 表示在当前发布阶段不允许该操作。
type ErrInvalidState struct{ Detail string }

func (e *ErrInvalidState) Error() string { return "invalid state: " + e.Detail }
