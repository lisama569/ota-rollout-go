package rollout

import (
	"log"
	"os"
	"testing"
	"time"
)

// testLogger 打印关键输入与判定依据，带测试名前缀，方便定位。
func testLogger(t *testing.T) func(string, ...any) {
	t.Helper()
	return func(format string, args ...any) {
		t.Helper()
		log.Printf("[%s] %s", t.Name(), sprintf(format, args...))
	}
}

func sprintf(format string, args ...any) string {
	return logf(format, args...)
}

// testConfig 给出常用严格/宽松两套门槛配置。
func testConfig() Config {
	return Config{
		MaxProtoVersion:  2,
		MaxAttempts:      3,
		SuccessThreshold: 0.8,
		MaxFailureRate:   0.2,
		MaxRollbackRate:  0.2,
		TargetFirmware:   "fw-2.0",
		BaselineFirmware: "fw-1.0",
	}
}

func batches(sizes ...int) []BatchSpec {
	var specs []BatchSpec
	for i, n := range sizes {
		ids := make([]string, n)
		for j := range ids {
			ids[j] = devName(i, j)
		}
		specs = append(specs, BatchSpec{Index: i, DeviceIDs: ids, InFlightQuota: n})
	}
	return specs
}

func devName(batch, idx int) string {
	return "d-" + string(rune('a'+batch)) + "-" + itoa(idx)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func mustState(t *testing.T, cfg Config, specs []BatchSpec) *State {
	t.Helper()
	s, err := NewRelease("rel-test", cfg, specs)
	if err != nil {
		t.Fatalf("NewRelease: %v", err)
	}
	return s
}

func newTestEngine(t *testing.T, store Store, st *State) *Engine {
	t.Helper()
	ctx := t.Context()
	eng, err := NewEngine(ctx, store, st, testLogger(t))
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return eng
}

func reportAt(id string, batch, attempt, proto int, outcome ReportOutcome, firmware string) DeviceReport {
	return DeviceReport{
		DeviceID: id, BatchIndex: batch, Attempt: attempt, Epoch: 0, ProtoVersion: proto,
		Outcome: outcome, Firmware: firmware, At: time.Now(),
	}
}

// ackForCommand 根据下发命令构造上报，自动带上正确的 attempt 与 epoch。
func ackForCommand(cmd Command, proto int, outcome ReportOutcome, firmware string) DeviceReport {
	return DeviceReport{
		DeviceID: cmd.DeviceID, BatchIndex: cmd.Batch, Attempt: cmd.Attempt, Epoch: cmd.Epoch,
		ProtoVersion: proto, Outcome: outcome, Firmware: firmware, At: time.Now(),
	}
}

func tFileStore(t *testing.T) *FileStore {
	t.Helper()
	dir := t.TempDir()
	return NewFileStore(dir + string(os.PathSeparator) + "state.json")
}

func timeNow() time.Time { return time.Now() }
