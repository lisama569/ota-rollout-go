package ota

import (
	"log"
	"os"
)

// Logger 是编排器使用的最小日志接口，关键输入与判定依据都会打印：
// 每次下发、每帧上报的设备/任务/类型、分类原因、门槛计数与推进/回滚决定。
type Logger interface {
	Printf(format string, args ...any)
}

type stdLogger struct {
	l *log.Logger
}

func newStdLogger() Logger {
	return &stdLogger{l: log.New(os.Stdout, "[ota] ", log.LstdFlags|log.Lmicroseconds)}
}

func (l *stdLogger) Printf(format string, args ...any) {
	l.l.Printf(format, args...)
}
