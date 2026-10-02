package ota

import "time"

// fakeClock 替换包级 timeNow，让“固定重试节奏”相关测试确定性可复现，不依赖真实 sleep。
type fakeClock struct {
	t time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{t: start}
	timeNow = c.now
	return c
}

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func (c *fakeClock) restore() { timeNow = func() time.Time { return time.Now() } }

// mustTime 解析 RFC3339 时间，供确定性时钟测试使用。
func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
