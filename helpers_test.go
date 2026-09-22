package ota

import (
	"sync"
	"testing"
	"time"
)

// stubTransport 记录编排器下发的全部任务，并发安全；上报通道为空通道。
type stubTransport struct {
	mu    sync.Mutex
	sent  []Task
	block bool
}

func (s *stubTransport) Send(t Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, t)
	return nil
}

func (s *stubTransport) Reports() <-chan DeviceReport { return nil }

func (s *stubTransport) tasks() []Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]Task, len(s.sent))
	copy(cp, s.sent)
	return cp
}

func (s *stubTransport) countTask(id TaskID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.sent {
		if t.ID == id {
			n++
		}
	}
	return n
}

// testPlan 构造 3 个批次的灰度计划：2 / 3 / 2 台设备。
func testPlan() Plan {
	return Plan{
		RolloutID:       "rl-test-001",
		Target:          "2.0.0",
		Protocol:        Semver{Major: 1, Minor: 0, Patch: 0},
		MaxAttempts:     3,
		MaxFailRate:     25,
		MaxRollbackRate: 25,
		MinEvaluated:    4,
		Batches: []BatchSpec{
			{Index: 0, Devices: []DeviceID{"d1", "d2"}, MaxInFlight: 2},
			{Index: 1, Devices: []DeviceID{"d3", "d4", "d5"}, MaxInFlight: 2},
			{Index: 2, Devices: []DeviceID{"d6", "d7"}, MaxInFlight: 2},
		},
	}
}

func newTestOrchestrator(t *testing.T, p Plan) (*Orchestrator, *stubTransport, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	tr := &stubTransport{}
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: tr})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	return o, tr, store
}

// upgradeFlow 构造指定设备升级尝试的上报帧。
func upgradeFlow(id DeviceID, batch BatchIndex, attempt int, kind ReportKind, proto Semver) DeviceReport {
	return DeviceReport{
		DeviceID: id,
		TaskID:   taskID("rl-test-001", batch, id, TaskUpgrade, attempt),
		Kind:     kind,
		Protocol: proto,
		ToVer:    "2.0.0",
		At:       time.Now(),
	}
}

func rollbackFlow(id DeviceID, batch BatchIndex, attempt int, kind ReportKind) DeviceReport {
	return DeviceReport{
		DeviceID: id,
		TaskID:   taskID("rl-test-001", batch, id, TaskRollback, attempt),
		Kind:     kind,
		Protocol: Semver{Major: 1},
		At:       time.Now(),
	}
}

var protoV1 = Semver{Major: 1}
