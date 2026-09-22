package ota

import "errors"

// 编排器对外暴露的可判定错误。
var (
	ErrBatchIndexRolledBack = errors.New("batch position rollback is rejected: persisted state is ahead of requested plan")
	ErrPlanChanged          = errors.New("plan fingerprint mismatch: persisted rollout does not match requested plan")
	ErrRolloutFinished      = errors.New("rollout already finished")
	ErrBatchNotReady        = errors.New("current batch has not reached the health gate")
	ErrNoCurrentTask        = errors.New("device has no current in-flight task")
	ErrTaskMismatch         = errors.New("report task id does not match current in-flight task")
	ErrInFlightQuota        = errors.New("batch in-flight quota exhausted")
	ErrInvalidState         = errors.New("persisted state is invalid")
	ErrDeviceNotInPlan      = errors.New("device is not part of the rollout plan")
)
