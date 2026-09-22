package ota

import (
	"context"
	"sync"
	"time"
)

// Transport 是本地模拟传输通道：编排器下发任务，设备上报经通道投递。
type Transport interface {
	Send(task Task) error
	Reports() <-chan DeviceReport
}

// 预定义的模拟设备行为名。
const (
	// BehSuccess：正常收到升级后回报 ACK + 成功。
	BehSuccess = "success"
	// BehRetryThenSuccess：前 FailBefore 个升级任务可重试失败，之后成功。
	BehRetryThenSuccess = "retry-then-success"
	// BehRetryExhaust：升级任务一律可重试失败（耗尽配额）。
	BehRetryExhaust = "retry-exhaust"
	// BehIncompatible：设备协议主版本更高，上报不可兼容。
	BehIncompatible = "incompatible"
	// BehRollbackFail：回滚任务失败（用于演练中止路径）。
	BehRollbackFail = "rollback-fail"
)

// ReportAction 是模拟设备对一次任务的反应动作：延迟 Delay 后上报 Report。
type ReportAction struct {
	Report DeviceReport
	Delay  time.Duration
}

// SimDevice 是一台本地模拟设备的行为脚本描述。
type SimDevice struct {
	ID         DeviceID
	Protocol   Semver
	CurrentVer FirmwareVersion
	// Behavior 取 Beh* 常量。
	Behavior string
	// FailBefore：在 retry-then-success 下，前多少次升级任务先报可重试失败。
	FailBefore int
	// ExtraDuplicates：终态上报额外重复发送的次数（模拟重复/迟到重复）。
	ExtraDuplicates int
	// MalformedFirst：对首个升级任务先发一帧缺 TaskID 的畸形上报，再发正确上报。
	MalformedFirst bool
	// OutOfOrder：成功上报先于 ACK 发出（模拟乱序）。
	OutOfOrder bool

	upgradeAttempts  int
	rollbackAttempts int
}

// React 按行为脚本生成对某个任务的全部反应动作。
// 每个 SimDevice 只由其专属的单个 goroutine 调用，尝试计数无需额外加锁。
func (d *SimDevice) React(task Task, target FirmwareVersion) []ReportAction {
	now := time.Now()
	base := DeviceReport{
		DeviceID: d.ID,
		TaskID:   task.ID,
		Protocol: d.Protocol,
		FromVer:  d.CurrentVer,
		ToVer:    target,
		At:       now,
	}
	switch task.Kind {
	case TaskRollback:
		d.rollbackAttempts++
		if d.Behavior == BehRollbackFail {
			r := base
			r.Kind = ReportRollbackFailure
			r.ReasonCode = "rollback-flash-failed"
			r.Reason = "simulated rollback failure"
			return []ReportAction{{Report: r}}
		}
		ack := base
		ack.Kind = ReportAcknowledge
		ok := base
		ok.Kind = ReportRollbackSuccess
		return []ReportAction{{Report: ack, Delay: time.Millisecond}, {Report: ok, Delay: 2 * time.Millisecond}}
	}

	// 升级任务
	d.upgradeAttempts++
	var actions []ReportAction
	if d.MalformedFirst && d.upgradeAttempts == 1 {
		bad := base
		bad.TaskID = "" // 缺字段：编排器必须判定为无效上报而非丢弃
		bad.Kind = ReportUpgradeSuccess
		actions = append(actions, ReportAction{Report: bad})
	}
	if d.Behavior == BehIncompatible {
		r := base
		r.Kind = ReportUpgradeFailure
		r.ReasonCode = "protocol-too-new"
		r.Reason = "device speaks a newer protocol major version"
		actions = append(actions, ReportAction{Report: r})
		return actions
	}
	ack := base
	ack.Kind = ReportAcknowledge
	if d.Behavior == BehRetryExhaust ||
		(d.Behavior == BehRetryThenSuccess && d.upgradeAttempts <= d.FailBefore) {
		fail := base
		fail.Kind = ReportUpgradeFailure
		fail.ReasonCode = "download-timeout"
		fail.Reason = "simulated transient download failure"
		actions = append(actions, ReportAction{Report: ack, Delay: time.Millisecond})
		actions = append(actions, ReportAction{Report: fail, Delay: 2 * time.Millisecond})
		// 失败终态也可能被设备重复上报。
		for i := 0; i < d.ExtraDuplicates; i++ {
			dup := fail
			dup.At = time.Now()
			actions = append(actions, ReportAction{Report: dup, Delay: time.Duration(3+i) * time.Millisecond})
		}
		return actions
	}
	ok := base
	ok.Kind = ReportUpgradeSuccess
	first, second := ReportAction{Report: ack, Delay: time.Millisecond}, ReportAction{Report: ok, Delay: 2 * time.Millisecond}
	if d.OutOfOrder {
		first, second = second, first
	}
	actions = append(actions, first, second)
	for i := 0; i < d.ExtraDuplicates; i++ {
		dup := ok
		dup.At = time.Now()
		actions = append(actions, ReportAction{Report: dup, Delay: time.Duration(3+i) * time.Millisecond})
	}
	return actions
}

// simRuntime 是每台设备一个串行 goroutine 的运行时。
type simRuntime struct {
	dev   *SimDevice
	inbox chan Task
	seen  map[TaskID]bool
}

// SimTransport 是基于内存通道的本地模拟传输。
type SimTransport struct {
	out     chan DeviceReport
	mu      sync.Mutex
	devices map[DeviceID]*simRuntime
	wg      sync.WaitGroup
	jitter  time.Duration
	started bool
	stopped chan struct{}
}

// NewSimTransport 创建模拟通道，out 为大缓冲，保证设备发送不依赖消费速度。
func NewSimTransport() *SimTransport {
	return &SimTransport{
		out:     make(chan DeviceReport, 65536),
		devices: make(map[DeviceID]*simRuntime),
		stopped: make(chan struct{}),
	}
}

// waitStopped 阻塞直到所有设备协程退出（Run 返回后）。
func (t *SimTransport) waitStopped() { <-t.stopped }

// WithJitter 设置每帧上报的确定性抖动上限（按任务哈希取值，无随机源）。
func (t *SimTransport) WithJitter(d time.Duration) *SimTransport {
	t.jitter = d
	return t
}

func (t *SimTransport) AddDevice(d *SimDevice) *SimTransport {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.devices[d.ID] = &simRuntime{dev: d, inbox: make(chan Task, 1), seen: make(map[TaskID]bool)}
	return t
}

// Send 投递任务给目标设备。同一设备在途任务未结束时拒绝再投，作为编排器约束的兜底。
func (t *SimTransport) Send(task Task) error {
	t.mu.Lock()
	rt, ok := t.devices[task.Device]
	t.mu.Unlock()
	if !ok {
		return ErrDeviceNotInPlan
	}
	select {
	case rt.inbox <- task:
		return nil
	default:
		return ErrInFlightQuota
	}
}

func (t *SimTransport) Reports() <-chan DeviceReport { return t.out }

// Run 为每台设备启动串行处理协程，ctx 取消后退出并关闭上报通道。
func (t *SimTransport) Run(ctx context.Context) {
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return
	}
	t.started = true
	rts := make([]*simRuntime, 0, len(t.devices))
	for _, rt := range t.devices {
		rts = append(rts, rt)
	}
	t.mu.Unlock()

	for _, rt := range rts {
		t.wg.Add(1)
		go func(rt *simRuntime) {
			defer t.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-rt.inbox:
					actions := rt.dev.React(task, task.Target)
					for _, a := range actions {
						delay := a.Delay + t.hashJitter(task.ID, a.Report.Kind)
						select {
						case <-ctx.Done():
							return
						case <-time.After(delay):
						}
						select {
						case <-ctx.Done():
							return
						case t.out <- a.Report:
						}
					}
				}
			}
		}(rt)
	}
	t.wg.Wait()
	close(t.out)
	close(t.stopped)
}

// hashJitter 依据任务 ID 与上报类型产生 [0, jitter) 的确定延迟，
// 让并发上报顺序有变化但完全可复现。
func (t *SimTransport) hashJitter(id TaskID, kind ReportKind) time.Duration {
	if t.jitter <= 0 {
		return 0
	}
	var h uint32 = 2166136261
	for _, c := range string(id) {
		h ^= uint32(c)
		h *= 16777619
	}
	h ^= uint32(kind) * 2654435761
	return time.Duration(h % uint32(t.jitter))
}
