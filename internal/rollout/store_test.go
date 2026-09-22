package rollout

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreRoundTripAndIsolation(t *testing.T) {
	ctx := context.Background()
	st := mustState(t, testConfig(), batches(2, 2))
	store := NewMemoryStore()
	if err := store.Save(ctx, st); err != nil {
		t.Fatalf("save: %v", err)
	}
	// 修改原对象不影响已提交快照。
	st.Current = 1
	st.Devices[devName(0, 0)].State = DevSucceeded

	got, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Current != 0 {
		t.Fatalf("snapshot leaked: current=%d", got.Current)
	}
	if got.Devices[devName(0, 0)].State != DevIdle {
		t.Fatalf("snapshot leaked: state=%s", got.Devices[devName(0, 0)].State)
	}
	t.Logf("round-trip isolation OK: current=%d dev0=%s",
		got.Current, got.Devices[devName(0, 0)].State)
}

func TestStoreRejectsBatchCursorRegression(t *testing.T) {
	ctx := context.Background()
	for name, store := range map[string]Store{
		"memory": NewMemoryStore(),
		"file":   tFileStore(t),
	} {
		t.Run(name, func(t *testing.T) {
			st := mustState(t, testConfig(), batches(2, 2))
			if err := store.Save(ctx, st); err != nil {
				t.Fatalf("save initial: %v", err)
			}
			advanced := mustState(t, testConfig(), batches(2, 2))
			advanced.Current = 1
			if err := store.Save(ctx, advanced); err != nil {
				t.Fatalf("save advanced: %v", err)
			}
			regressed := mustState(t, testConfig(), batches(2, 2))
			regressed.Current = 0
			err := store.Save(ctx, regressed)
			t.Logf("attempt cursor 1 -> 0 rejected: %v", err)
			var br *ErrBatchRegress
			if !errors.As(err, &br) {
				t.Fatalf("want *ErrBatchRegress, got %v", err)
			}
			if br.From != 1 || br.To != 0 {
				t.Fatalf("unexpected regression fields: %+v", br)
			}
			// 被拒绝后，已提交状态仍然是前进位点，不会从中间继续。
			got, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got.Current != 1 {
				t.Fatalf("committed cursor altered: %d", got.Current)
			}
		})
	}
}

func TestFileStoreAtomicLoad(t *testing.T) {
	ctx := context.Background()
	store := tFileStore(t)
	if s, err := store.Load(ctx); err != nil || s != nil {
		t.Fatalf("fresh store want (nil,nil), got %v, %v", s, err)
	}
	st := mustState(t, testConfig(), batches(3))
	st.Phase = PhaseRolling
	if err := store.Save(ctx, st); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Phase != PhaseRolling || len(got.Batches) != 1 || got.Batches[0].Total != 3 {
		t.Fatalf("unexpected loaded state: phase=%s batches=%+v", got.Phase, got.Batches)
	}
	if len(got.Specs[0].DeviceIDs) != 3 {
		t.Fatalf("specs not persisted: %+v", got.Specs)
	}
	t.Logf("file store reload OK phase=%s batch0 devices=%v",
		got.Phase, got.Specs[0].DeviceIDs)
}
