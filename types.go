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
	// ClassManual 重试预算耗尽，进入需要人工介入的终态（与其他失败可区分）。
	ClassManual
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
	// MaxRollbackAttempts 是单台设备回滚任务的重试预算（含首次下发）。
	// <= 0 时回落为 MaxAttempts。预算耗尽后设备进入需人工介入的终态。
	MaxRollbackAttempts int
	// MaxFailRate / MaxRollbackRate 为允许的上限（含边界），超过即暂停并回滚。
	MaxFailRate     float64
	MaxRollbackRate float64
	// MinEvaluated 是比率判定生效所需的最少终态样本数，避免个位数样本误杀。
	MinEvaluated int
}
