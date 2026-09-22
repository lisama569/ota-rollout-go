package ota

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 是编排状态的持久化抽象。
type Store interface {
	// Load 在状态不存在时返回 (nil, nil)，表示首次启动。
	Load() (*RolloutState, error)
	Save(state *RolloutState) error
}

// FileStore 使用本地 JSON 文件 + 临时文件原子重命名持久化状态，
// 进程崩溃时文件只会是旧版本或新版本，不会出现半截 JSON。
type FileStore struct {
	path string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Load() (*RolloutState, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var st RolloutState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	if err := st.validate(); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *FileStore) Save(state *RolloutState) error {
	if err := state.validate(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".ota-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

// MemoryStore 以序列化字节保存在内存中：每次 Save 都完整编码、每次 Load 都完整解码，
// 行为与 FileStore 一致（拿不到旧指针），用于在测试中模拟进程重启。
type MemoryStore struct {
	data []byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Load() (*RolloutState, error) {
	if len(s.data) == 0 {
		return nil, nil
	}
	var st RolloutState
	if err := json.Unmarshal(s.data, &st); err != nil {
		return nil, err
	}
	if err := st.validate(); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *MemoryStore) Save(state *RolloutState) error {
	if err := state.validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	s.data = raw
	return nil
}

// validate 检查持久化状态的基本不变量，损坏文件必须报错而不是带病运行。
func (st *RolloutState) validate() error {
	if st.RolloutID == "" {
		return ErrInvalidState
	}
	if st.Batches == nil {
		return ErrInvalidState
	}
	if st.Devices == nil {
		return ErrInvalidState
	}
	if _, ok := st.Batches[st.CurrentBatch]; !ok && st.Phase == RolloutRunning {
		return ErrInvalidState
	}
	for id, d := range st.Devices {
		if d == nil {
			return ErrInvalidState
		}
		if d.DeviceID != id {
			return ErrInvalidState
		}
		if _, ok := st.Batches[d.Batch]; !ok {
			return ErrInvalidState
		}
	}
	return nil
}
