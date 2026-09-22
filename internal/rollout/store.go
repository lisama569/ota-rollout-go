package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Store 是编排状态的持久化抽象。
//
// Save 必须整体原子写入并拒绝批次位点回退：当 next.Current 小于已提交的
// Current 时返回 *ErrBatchRegress。Load 在进程重启后恢复状态。
type Store interface {
	Save(ctx context.Context, next *State) error
	Load(ctx context.Context) (*State, error)
}

// FileStore 把状态以 JSON 原子写入本地文件（写临时文件后 rename）。
type FileStore struct {
	path string
}

// NewFileStore 创建文件存储。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

type persistedEnvelope struct {
	Version int    `json:"version"`
	State   *State `json:"state"`
}

// Save 原子保存状态并拒绝批次位点回退。
//
// 实现要点：
//   - 先读入已提交快照，只做单调位点校验：Current 只允许前进，不允许从中间回退继续；
//   - 写入同目录临时文件 -> fsync -> rename，保证落盘原子性，崩溃不会出现半截 JSON；
//   - 保存的是传入状态的深拷贝，避免调用方后续修改污染已提交快照。
func (s *FileStore) Save(ctx context.Context, next *State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if next == nil {
		return errors.New("cannot save nil state")
	}
	if prev, err := s.Load(ctx); err != nil {
		return fmt.Errorf("load previous state before save: %w", err)
	} else if prev != nil && next.Current < prev.Current {
		return &ErrBatchRegress{From: prev.Current, To: next.Current}
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".rollout-state-*")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	enc := persistedEnvelope{Version: 1, State: cloneState(next)}
	data, err := json.MarshalIndent(enc, "", "  ")
	if err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("marshal state: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close state file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("rename state file: %w", err)
	}
	return nil
}

// Load 读取状态；状态文件不存在时返回 (nil, nil) 表示全新发布。
func (s *FileStore) Load(ctx context.Context) (*State, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}
	var env persistedEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse state file: %w", err)
	}
	if env.Version != 1 {
		return nil, fmt.Errorf("unsupported state version %d", env.Version)
	}
	return env.State, nil
}

// MemoryStore 是仅供测试的内存存储，同样执行位点回退校验与深拷贝隔离。
type MemoryStore struct {
	snap *State
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Save 保存状态快照的深拷贝，并拒绝批次位点回退。
func (s *MemoryStore) Save(ctx context.Context, next *State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if next == nil {
		return errors.New("cannot save nil state")
	}
	if s.snap != nil && next.Current < s.snap.Current {
		return &ErrBatchRegress{From: s.snap.Current, To: next.Current}
	}
	s.snap = cloneState(next)
	return nil
}

// Load 读取状态快照的深拷贝，调用方修改不会影响存储内副本。
func (s *MemoryStore) Load(ctx context.Context) (*State, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.snap == nil {
		return nil, nil
	}
	return cloneState(s.snap), nil
}

func cloneState(s *State) *State {
	if s == nil {
		return nil
	}
	cp := *s
	cp.Batches = make([]BatchProgress, len(s.Batches))
	copy(cp.Batches, s.Batches)
	if s.Devices != nil {
		cp.Devices = make(map[string]*DeviceProgress, len(s.Devices))
		for k, v := range s.Devices {
			d := *v
			cp.Devices[k] = &d
		}
	}
	return &cp
}
