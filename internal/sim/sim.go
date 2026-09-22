// Package sim 提供本地模拟设备与传输通道：设备行为可配置，
// 通道可制造乱序、重复与迟到上报，用于驱动 rollout.Engine。
package sim

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/ota-rollout-go/internal/rollout"
)

// Behavior 描述一台模拟设备的行为剧本。
type Behavior struct {
	DeviceID     string
	ProtoVersion int
	// Outcomes 按尝试次序依次返回的结果；最后一个结果会重复用于后续尝试。
	// 对 rollback 命令使用 RollbackOutcomes（缺省一律回滚成功）。
	Outcomes []rollout.ReportOutcome
	// ReorderLate 为 true 时该设备的上报在通道中被延迟、乱序投递。
	ReorderLate bool
	// Duplicate 为 true 时通道额外投递一份重复上报（相同 attempt、相同内容）。
	Duplicate bool
	// RollbackOutcomes 回滚命令的结果剧本，缺省为 ROLLBACK_DONE。
	RollbackOutcomes []rollout.ReportOutcome
}

func (b *Behavior) outcomeFor(attempt int) rollout.ReportOutcome {
	if len(b.Outcomes) == 0 {
		return rollout.OutcomeSuccess
	}
	if attempt-1 < len(b.Outcomes) {
		return b.Outcomes[attempt-1]
	}
	return b.Outcomes[len(b.Outcomes)-1]
}

func (b *Behavior) rollbackFor(attempt int) rollout.ReportOutcome {
	if len(b.RollbackOutcomes) == 0 {
		return rollout.OutcomeRollbackDone
	}
	if attempt-1 < len(b.RollbackOutcomes) {
		return b.RollbackOutcomes[attempt-1]
	}
	return b.RollbackOutcomes[len(b.RollbackOutcomes)-1]
}

// Fleet 是模拟设备集合，记录每台设备收到的命令以模拟“同一 attempt 重复命令去重”。
type Fleet struct {
	mu        sync.Mutex
	behaviors map[string]*Behavior
	target    string
	baseline  string
	// seen[device] = 已响应过的 (kind, attempt)，重复命令不再产生第二条上报。
	seen map[string]map[cmdKey]bool
	// currentFirmware 模拟设备当前运行的固件版本。
	currentFirmware map[string]string
	seq             atomic.Int64
}

type cmdKey struct {
	kind    string // "up" | "rb"
	attempt int
}

// NewFleet 根据设备行为剧本创建模拟机群。
func NewFleet(target, baseline string, bs ...Behavior) *Fleet {
	f := &Fleet{
		behaviors:       map[string]*Behavior{},
		seen:            map[string]map[cmdKey]bool{},
		currentFirmware: map[string]string{},
		target:          target,
		baseline:        baseline,
	}
	for _, b := range bs {
		bc := b
		f.behaviors[b.DeviceID] = &bc
		f.seen[b.DeviceID] = map[cmdKey]bool{}
		f.currentFirmware[b.DeviceID] = baseline
	}
	return f
}

// HandleUpgrade 模拟设备收到升级下发命令，返回它将发出的上报；
// 同一 (device, attempt) 的重复命令返回 ok=false（设备本地去重，不重复上报）。
func (f *Fleet) HandleUpgrade(cmd rollout.Command) (rollout.DeviceReport, bool) {
	deviceID, batch, attempt := cmd.DeviceID, cmd.Batch, cmd.Attempt
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.behaviors[deviceID]
	if !ok {
		return rollout.DeviceReport{}, false
	}
	k := cmdKey{"up", attempt}
	if f.seen[deviceID][k] {
		return rollout.DeviceReport{}, false
	}
	f.seen[deviceID][k] = true

	outcome := b.outcomeFor(attempt)
	firmware := f.baseline
	switch outcome {
	case rollout.OutcomeSuccess:
		firmware = f.target
		f.currentFirmware[deviceID] = f.target
	case rollout.OutcomeRollbackSelf:
		// 设备换装后自行回退到基线；上报需携带目标固件以证明“确实换装过”，
		// 引擎据此计自回滚。设备当前实际运行的是基线版本。
		firmware = f.target
		f.currentFirmware[deviceID] = f.baseline
	}
	return rollout.DeviceReport{
		DeviceID:     deviceID,
		BatchIndex:   batch,
		Attempt:      attempt,
		Epoch:        cmd.Epoch,
		ProtoVersion: b.ProtoVersion,
		Outcome:      outcome,
		Firmware:     firmware,
		Seq:          f.seq.Add(1),
		At:           time.Now(),
	}, true
}

// HandleRollback 模拟设备收到回滚命令。
func (f *Fleet) HandleRollback(cmd rollout.Command) (rollout.DeviceReport, bool) {
	deviceID, batch, attempt := cmd.DeviceID, cmd.Batch, cmd.Attempt
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.behaviors[deviceID]
	if !ok {
		return rollout.DeviceReport{}, false
	}
	k := cmdKey{"rb", attempt}
	if f.seen[deviceID][k] {
		return rollout.DeviceReport{}, false
	}
	f.seen[deviceID][k] = true
	outcome := b.rollbackFor(attempt)
	if outcome == rollout.OutcomeRollbackDone {
		f.currentFirmware[deviceID] = f.baseline
	}
	return rollout.DeviceReport{
		DeviceID:     deviceID,
		BatchIndex:   batch,
		Attempt:      attempt,
		Epoch:        cmd.Epoch,
		ProtoVersion: b.ProtoVersion,
		Outcome:      outcome,
		Firmware:     f.baseline,
		Seq:          f.seq.Add(1),
		At:           time.Now(),
	}, true
}

// CurrentFirmware 返回设备当前模拟运行的固件（测试断言用）。
func (f *Fleet) CurrentFirmware(deviceID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentFirmware[deviceID]
}

// Channel 是模拟传输通道：按设备行为决定上报是否延迟、乱序与重复。
type Channel struct {
	eng   *rollout.Engine
	fleet *Fleet
	logf  func(string, ...any)
	mu    sync.Mutex
	delay time.Duration // ReorderLate 设备上报的额外延迟
}

// NewChannel 创建绑定到引擎与机群的模拟通道。
func NewChannel(eng *rollout.Engine, fleet *Fleet, delay time.Duration, logf func(string, ...any)) *Channel {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Channel{eng: eng, fleet: fleet, delay: delay, logf: logf}
}

// SendUpgrade 执行升级命令并把设备上报（可能重复/迟到）投递给引擎。
func (c *Channel) SendUpgrade(ctx context.Context, cmd rollout.Command) {
	r, ok := c.fleet.HandleUpgrade(cmd)
	if !ok {
		return
	}
	c.deliver(ctx, cmd.DeviceID, r)
}

// SendRollback 执行回滚命令并投递回滚上报。
func (c *Channel) SendRollback(ctx context.Context, cmd rollout.Command) {
	r, ok := c.fleet.HandleRollback(cmd)
	if !ok {
		return
	}
	c.deliver(ctx, cmd.DeviceID, r)
}

func (c *Channel) deliver(ctx context.Context, deviceID string, r rollout.DeviceReport) {
	b := c.fleet.behaviors[deviceID]
	isLate := b != nil && b.ReorderLate
	dup := b != nil && b.Duplicate

	send := func(rep rollout.DeviceReport, tag string) {
		if isLate {
			select {
			case <-time.After(c.delay):
			case <-ctx.Done():
				return
			}
		}
		c.logf("channel deliver %s device=%s seq=%d attempt=%d outcome=%s",
			tag, rep.DeviceID, rep.Seq, rep.Attempt, rep.Outcome)
		if _, err := c.eng.Report(ctx, rep); err != nil {
			c.logf("channel report error device=%s: %v", rep.DeviceID, err)
		}
	}
	if dup {
		// 重复副本立即投递，主副本按延迟策略投递，制造“重复先于正本/乱序”。
		go send(r, "dup")
	}
	send(r, "orig")
}

// RunDriver 是本地可跑的驱动循环：
//
// 反复向引擎索要升级/回滚任务 -> 交给机群执行 -> 经由通道回传上报，
// 直到发布单进入终态。乱序上报通过带缓冲的 goroutine 并发投递。
func RunDriver(ctx context.Context, eng *rollout.Engine, fleet *Fleet, ch *Channel, maxIters int) error {
	for i := 0; i < maxIters; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		st := eng.Snapshot()
		if st.Phase.Terminal() {
			return nil
		}

		var wg sync.WaitGroup
		switch st.Phase {
		case rollout.PhaseRolling:
			tasks, err := eng.Dispatch(ctx)
			if err != nil {
				return fmt.Errorf("dispatch: %w", err)
			}
			for _, cmd := range tasks {
				wg.Add(1)
				go func(cmd rollout.Command) {
					defer wg.Done()
					ch.SendUpgrade(ctx, cmd)
				}(cmd)
			}
		case rollout.PhaseRollingBack:
			tasks, err := eng.DispatchRollback(ctx)
			if err != nil {
				return fmt.Errorf("dispatch rollback: %w", err)
			}
			for _, cmd := range tasks {
				wg.Add(1)
				go func(cmd rollout.Command) {
					defer wg.Done()
					ch.SendRollback(ctx, cmd)
				}(cmd)
			}
		}
		wg.Wait()

		// 终态或无进展时退出（PAUSED 由调用方处理，这里直接结束驱动）。
		st2 := eng.Snapshot()
		if st2.Phase.Terminal() {
			return nil
		}
		if st2.Phase == rollout.PhasePaused {
			return fmt.Errorf("driver stopped: release is PAUSED")
		}
	}
	return fmt.Errorf("driver exceeded max iterations %d", maxIters)
}
