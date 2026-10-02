package ota

import "time"

// timeNow 是可在测试中替换的时钟，默认使用墙钟。
var timeNow = func() time.Time { return time.Now() }

func reportKindName(k ReportKind) string {
	switch k {
	case ReportAcknowledge:
		return "ack"
	case ReportUpgradeSuccess:
		return "upgrade-success"
	case ReportUpgradeFailure:
		return "upgrade-failure"
	case ReportRollbackSuccess:
		return "rollback-success"
	case ReportRollbackFailure:
		return "rollback-failure"
	default:
		return "unknown"
	}
}

func decisionName(k DecisionKind) string {
	switch k {
	case DecisionIgnored:
		return "ignored"
	case DecisionDuplicate:
		return "duplicate"
	case DecisionStale:
		return "stale"
	case DecisionAcked:
		return "acked"
	case DecisionSucceeded:
		return "succeeded"
	case DecisionFailedIncompatible:
		return "failed-incompatible"
	case DecisionFailedInvalid:
		return "failed-invalid"
	case DecisionFailedRetryable:
		return "failed-retryable"
	case DecisionRetried:
		return "retried"
	case DecisionRollbackSucceeded:
		return "rollback-succeeded"
	case DecisionRollbackFailed:
		return "rollback-failed"
	case DecisionBatchAdvanced:
		return "batch-advanced"
	case DecisionRollbackTriggered:
		return "rollback-triggered"
	case DecisionRolloutCompleted:
		return "rollout-completed"
	case DecisionAborted:
		return "aborted"
	default:
		return "none"
	}
}

func className(c FailureClass) string {
	switch c {
	case ClassIncompatible:
		return "incompatible"
	case ClassInvalid:
		return "invalid"
	case ClassRetryable:
		return "retryable"
	case ClassManual:
		return "manual-intervention"
	default:
		return "none"
	}
}

func devicePhaseName(p DevicePhase) string {
	switch p {
	case PhasePending:
		return "pending"
	case PhaseUpgradeInflight:
		return "upgrade-inflight"
	case PhaseUpgraded:
		return "upgraded"
	case PhaseRollbackInflight:
		return "rollback-inflight"
	case PhaseDone:
		return "done"
	case PhaseIncompatible:
		return "incompatible"
	case PhaseFailed:
		return "failed"
	case PhaseRollbackFailed:
		return "rollback-failed"
	}
	return "unknown"
}
