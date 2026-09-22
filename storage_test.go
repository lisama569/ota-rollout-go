package ota

import (
	"path/filepath"
	"testing"
)

// TestFileStoreRoundTrip：状态写入临时文件后能无损读回，未完成批次、
// 设备阶段、审计历史都保持一致；文件不存在时 Load 返回 nil。
func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := NewFileStore(path)

	if st, err := store.Load(); err != nil || st != nil {
		t.Fatalf("empty store: st=%v err=%v, want (nil,nil)", st, err)
	}

	p := testPlan()
	o := NewOrchestrator(Config{Plan: p, Store: store, Transport: &stubTransport{}})
	if err := o.RecoverOrResume(); err != nil {
		t.Fatal(err)
	}
	succeedDevice(t, o, "d1", 0, 1)
	succeedDevice(t, o, "d2", 0, 1)

	// 用一个全新的 FileStore 从磁盘读回。
	reloaded := NewFileStore(path)
	o2 := NewOrchestrator(Config{Plan: p, Store: reloaded, Transport: &stubTransport{}})
	if err := o2.RecoverOrResume(); err != nil {
		t.Fatalf("recover from disk: %v", err)
	}
	st := o2.State()
	if st.CurrentBatch != 1 {
		t.Fatalf("batch=%d, want 1", st.CurrentBatch)
	}
	if st.Devices["d1"].Phase != PhaseUpgraded || st.Devices["d2"].Phase != PhaseUpgraded {
		t.Fatalf("d1=%v d2=%v, both upgraded", st.Devices["d1"].Phase, st.Devices["d2"].Phase)
	}
	if len(st.History) == 0 {
		t.Fatal("history empty after reload")
	}
}

// TestFileStoreRejectsCorrupt：半截/损坏 JSON 必须报错而不是带病恢复。
func TestFileStoreRejectsCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := writeTestFile(path, []byte("{not-json")); err != nil {
		t.Fatal(err)
	}
	store := NewFileStore(path)
	if _, err := store.Load(); err == nil {
		t.Fatal("corrupt state loaded without error")
	}
}
