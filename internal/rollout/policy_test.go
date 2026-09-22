package rollout

import (
	"testing"
)

func TestNewReleaseValidation(t *testing.T) {
	cfg := testConfig()
	cases := []struct {
		name   string
		rel    string
		cfg    Config
		specs  []BatchSpec
		errMsg string
	}{
		{"empty release id", "", cfg, batches(2), "release id"},
		{"bad attempts", "r", func() Config { c := cfg; c.MaxAttempts = 0; return c }(), batches(2), "MaxAttempts"},
		{"bad success threshold", "r", func() Config { c := cfg; c.SuccessThreshold = 1.5; return c }(), batches(2), "SuccessThreshold"},
		{"bad failure rate", "r", func() Config { c := cfg; c.MaxFailureRate = -0.1; return c }(), batches(2), "MaxFailureRate"},
		{"empty firmware", "r", func() Config { c := cfg; c.TargetFirmware = ""; return c }(), batches(2), "firmware"},
		{"same firmware", "r", func() Config { c := cfg; c.TargetFirmware = c.BaselineFirmware; return c }(), batches(2), "differ"},
		{"no batches", "r", cfg, nil, "at least one batch"},
		{"index gap", "r", cfg, []BatchSpec{
			{Index: 0, DeviceIDs: []string{"x"}, InFlightQuota: 1},
			{Index: 2, DeviceIDs: []string{"y"}, InFlightQuota: 1},
		}, "declares index 2"},
		{"empty batch", "r", cfg, []BatchSpec{
			{Index: 0, DeviceIDs: nil, InFlightQuota: 1},
		}, "no devices"},
		{"duplicate device", "r", cfg, []BatchSpec{
			{Index: 0, DeviceIDs: []string{"x"}, InFlightQuota: 1},
			{Index: 1, DeviceIDs: []string{"x"}, InFlightQuota: 1},
		}, "more than one batch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRelease(tc.rel, tc.cfg, tc.specs)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errMsg)
			}
			assertContains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestEvaluateGate(t *testing.T) {
	cfg := testConfig()
	t.Run("pending while outstanding", func(t *testing.T) {
		b := BatchProgress{Total: 10, Succeeded: 5, InFlight: 3, Awaiting: 2}
		d, why := evaluateGate(cfg, b)
		t.Logf("input total=%d succ=%d inflight=%d awaiting=%d => %s (%s)",
			b.Total, b.Succeeded, b.InFlight, b.Awaiting, d, why)
		if d != GatePending {
			t.Fatalf("want PENDING, got %s", d)
		}
	})
	t.Run("failure rate exceeded blocks early", func(t *testing.T) {
		// 10 台里已永久失败 3 台（30% > 20%），即使还有在途也必须立即阻塞。
		b := BatchProgress{Total: 10, Failed: 3, InFlight: 5, Awaiting: 2}
		d, why := evaluateGate(cfg, b)
		t.Logf("decision=%s why=%s", d, why)
		if d != GateBlocked {
			t.Fatalf("want BLOCKED_FAILURE_RATE, got %s", d)
		}
	})
	t.Run("rollback rate exceeded blocks", func(t *testing.T) {
		b := BatchProgress{Total: 10, Succeeded: 5, SelfReverted: 3, InFlight: 2}
		d, why := evaluateGate(cfg, b)
		t.Logf("decision=%s why=%s", d, why)
		if d != GateRollbackRate {
			t.Fatalf("want BLOCKED_ROLLBACK_RATE, got %s", d)
		}
	})
	t.Run("threshold boundary is inclusive", func(t *testing.T) {
		// 10 台失败 2 台 = 20%，阈值 20%：严格大于才阻塞，不应阻塞。
		b := BatchProgress{Total: 10, Succeeded: 7, Failed: 2, InFlight: 1}
		d, _ := evaluateGate(cfg, b)
		if d != GatePending {
			t.Fatalf("want PENDING at exact threshold, got %s", d)
		}
	})
	t.Run("settled success opens gate", func(t *testing.T) {
		b := BatchProgress{Total: 10, Succeeded: 9, Failed: 1}
		d, why := evaluateGate(cfg, b)
		t.Logf("decision=%s why=%s", d, why)
		if d != GateOpen {
			t.Fatalf("want OPEN, got %s", d)
		}
	})
	t.Run("settled below success threshold blocks", func(t *testing.T) {
		// 成功 70% < 80%，失败 0、自回滚 3：由回滚分支给出终局阻塞。
		b := BatchProgress{Total: 10, Succeeded: 7, SelfReverted: 3}
		d, _ := evaluateGate(cfg, b)
		if d == GateOpen || d == GatePending {
			t.Fatalf("want blocking decision, got %s", d)
		}
	})
}

func TestClassifyReport(t *testing.T) {
	s := mustState(t, testConfig(), batches(3))
	s.Phase = PhaseRolling
	ctx := t.Context()
	eng := newTestEngine(t, NewMemoryStore(), s)
	tasks, err := eng.Dispatch(ctx)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("want 3 tasks, got %d", len(tasks))
	}

	cases := []struct {
		name   string
		r      DeviceReport
		reason RejectReason
	}{
		{"missing device id",
			reportAt("", 0, 1, 1, OutcomeSuccess, "fw-2.0"), ReasonMissingField},
		{"missing outcome",
			reportAt(devName(0, 0), 0, 1, 1, "", "fw-2.0"), ReasonMissingField},
		{"malformed outcome",
			reportAt(devName(0, 0), 0, 1, 1, "WEIRD", "fw-2.0"), ReasonMalformed},
		{"unknown device",
			reportAt("ghost", 0, 1, 1, OutcomeSuccess, "fw-2.0"), ReasonMalformed},
		{"proto too new",
			reportAt(devName(0, 0), 0, 1, 99, OutcomeSuccess, "fw-2.0"), ReasonProtoTooNew},
		{"batch mismatch",
			reportAt(devName(0, 0), 1, 1, 1, OutcomeSuccess, "fw-2.0"), ReasonBatchMismatch},
		{"stale attempt duplicate/late",
			reportAt(devName(0, 0), 0, 2, 1, OutcomeSuccess, "fw-2.0"), ReasonStale},
		{"success wrong firmware",
			reportAt(devName(0, 0), 0, 1, 1, OutcomeSuccess, "fw-evil"), ReasonMalformed},
		{"valid success",
			reportAt(devName(0, 0), 0, 1, 1, OutcomeSuccess, "fw-2.0"), ReasonNone},
		{"duplicate after accepted is stale",
			reportAt(devName(0, 0), 0, 1, 1, OutcomeSuccess, "fw-2.0"), ReasonStale},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := eng.Report(ctx, tc.r)
			if err != nil {
				t.Fatalf("report: %v", err)
			}
			t.Logf("input device=%s attempt=%d outcome=%s fw=%s => accepted=%v reason=%s state=%s detail=%s",
				tc.r.DeviceID, tc.r.Attempt, tc.r.Outcome, tc.r.Firmware,
				res.Accepted, res.Reason, res.State, res.Detail)
			if res.Reason != tc.reason {
				t.Fatalf("want reason=%s got=%s (%s)", tc.reason, res.Reason, res.Detail)
			}
			if tc.reason == ReasonNone && !res.Accepted {
				t.Fatalf("expected accepted")
			}
		})
	}
}

func assertContains(t *testing.T, got, sub string) {
	t.Helper()
	if !contains(got, sub) {
		t.Fatalf("error %q does not contain %q", got, sub)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
