package ota

import "time"

// DeviceID 是设备的唯一标识。
type DeviceID string

// BatchIndex 是批次的序号，从 0 开始。
type BatchIndex int

// FirmwareVersion 是固件版本字符串。
type FirmwareVersion string

// TaskID 是单次下发任务的唯一标识。
type TaskID string

// Semver 是设备上报协议使用的语义化版本号。
type Semver struct {
	Major int
	Minor int
	Patch int
}

// ReportKind 是设备上报的类型。
type ReportKind int

const (
	ReportUnknown ReportKind = iota
	ReportAcknowledge
	ReportUpgradeSuccess
	ReportUpgradeFailure
	ReportRollbackSuccess
	ReportRollbackFailure
)

// FailureClass 是失败原因的分类，三类原因必须可区分。
type FailureClass int

const (
	ClassNone FailureClass = iota
	// ClassIncompatible 设备不兼容，不可升级（终态）。
	ClassIncompatible
	// ClassInvalid 上报内容无效（缺字段/畸形/版本过新），不计成功，可要求重报。
	ClassInvalid
	// ClassRetryable 可重试失败（临时故障）。
	ClassRetryable
	// ClassRollbackBudgetExhausted 回滚重试预算耗尽：设备可能仍停留在新固件上，
	// 回滚停止，发布进入必须人工介入的终态。与其他失败原因明确可区分。
	ClassRollbackBudgetExhausted
)

// DeviceReport 是设备上报给编排器的一帧报文。
type DeviceReport struct {
	DeviceID   DeviceID
	TaskID     TaskID
	Kind       ReportKind
	Protocol   Semver
	FromVer    FirmwareVersion
	ToVer      FirmwareVersion
	ReasonCode string
	Reason     string
	Seq        uint64
	At         time.Time
}

// DecisionKind 是编排器对一次上报或一次调度的判定结果。
type DecisionKind int

const (
	DecisionNone DecisionKind = iota
	DecisionIgnored
	DecisionDuplicate
	DecisionStale
	DecisionAcked
	DecisionSucceeded
	DecisionFailedIncompatible
	DecisionFailedInvalid
	DecisionFailedRetryable
	DecisionRetried
	DecisionRollbackSucceeded
	DecisionRollbackFailed
	DecisionBatchAdvanced
	DecisionRollbackTriggered
	DecisionRolloutCompleted
	DecisionAborted
)

// Decision 记录一次判定的关键输入与依据，用于可解释的审计日志。
type Decision struct {
	At       time.Time
	DeviceID DeviceID
	TaskID   TaskID
	Batch    BatchIndex
	Kind     DecisionKind
	Class    FailureClass
	Reason   string
}

// TaskKind 区分升级任务与回滚任务。
type TaskKind int

const (
	TaskUpgrade TaskKind = iota
	TaskRollback
)

// Task 是编排器下发给设备的一次任务。
type Task struct {
	ID      TaskID
	Kind    TaskKind
	Device  DeviceID
	Batch   BatchIndex
	Attempt int
	Target  FirmwareVersion
}

// BatchSpec 描述一个批次：设备名单与在途配额。
type BatchSpec struct {
	Index       BatchIndex
	Devices     []DeviceID
	MaxInFlight int
}

// Plan 是一次灰度发布的完整计划。
type Plan struct {
	RolloutID   string
	Target      FirmwareVersion
	Protocol    Semver
	Batches     []BatchSpec
	MaxAttempts int
	// MaxFailRate / MaxRollbackRate 为允许的上限（含边界），超过即暂停并回滚。
	MaxFailRate     float64
	MaxRollbackRate float64
	// MinEvaluated 是比率判定生效所需的最少终态样本数，避免个位数样本误杀。
	MinEvaluated int
	// MaxRollbackAttempts 是回滚任务的独立重试预算（每台设备的回滚尝试总次数上限）。
	// 为 0 时回退使用 MaxAttempts。预算持久化在设备记录上，重启不会把已用次数抹掉。
	MaxRollbackAttempts int
	// RollbackRetryInterval 是回滚失败后下一次重投的固定节奏（含边界、可复现）。
	// 为 0 时使用 defaultRollbackRetryInterval。下次尝试时刻会持久化，
	// 重启后未到点绝不提前重投，到点由补偿 tick 补发，不会多打一轮。
	RollbackRetryInterval time.Duration
}
