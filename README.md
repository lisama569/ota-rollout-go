# ota-rollout-go

本地可跑的固件灰度发布编排器：用**本地模拟设备**与**模拟传输通道**驱动升级流程，
验证分批放量、失败回滚、重启续跑在乱序/重复/迟到上报下结论是否一致且可解释。

无任何外部依赖（标准库 + 本地 JSON 文件存储），`go test ./...` 即可复现。

## 目录结构

```
internal/rollout/   编排核心
  types.go          状态机枚举、配置、持久化状态、判定原因（可区分原因码）
  policy.go         纯函数：配置校验、上报可接受性分类、批次健康门槛判定
  engine.go         编排器：串行化调度、上报状态转移、回滚编排、重启恢复
  store.go          FileStore（原子写 JSON）/ MemoryStore，位点回退拒绝
internal/sim/       模拟设备（可配置行为剧本）与模拟通道（乱序/重复/迟到）
cmd/ota-demo/       本地端到端演示：正常放量 / 失败回滚
```

## 发布状态机

```
                 Start
 PENDING ─────────────────► ROLLING ◄────────────┐
                              │  ▲ Pause/Resume   │
                              │  └────► PAUSED    │
        失败率/回滚率超阈值     │                  │ 重启恢复（Epoch+1，在途重排队）
                              ▼                  │
                        ROLLING_BACK ─────────────┘
                              │
                 回滚全部排空   ▼
                           FAILED（终态）

   全部批次开门            COMPLETED（终态）
```

### 设备状态

| 状态 | 含义 |
| --- | --- |
| `IDLE` | 本批成员，尚未获得下发 |
| `AWAITING` | 可重试失败后排队，或崩溃重启后在途任务重排队 |
| `IN_FLIGHT` | 恰好有一个升级命令在途（同一设备最多一个） |
| `SUCCESS` | 已确认运行目标固件，终态，**不重复下发、不重复计数** |
| `FAILED` / `INCOMPATIBLE` | 永久失败 / 设备不兼容，终态，计入失败率，设备未换装无需回滚 |
| `SKIPPED` | 触发回滚时尚未换装（含迟到失败），终止升级，不计入成败 |
| `ROLLBACK_QUEUED` / `ROLLING_BACK` | 已换装成功、回滚排队 / 回滚命令在途 |
| `ROLLED_BACK` | 已确认回到基线固件，终态 |

## 放量规则（健康门槛）

分母统一为**批次设备总数**，因此判定与上报到达顺序无关、可复现：

1. 每批有独立的在途配额 `InFlightQuota`：整体在途数不得超过当前批次配额；
2. 失败率 `Failed/Total` **严格大于** `MaxFailureRate` → 立即 `BLOCKED_FAILURE_RATE`，
   不再等待未决上报，暂停后续放量并进入回滚；
3. 自回滚率 `SelfReverted/Total` **严格大于** `MaxRollbackRate`
   → `BLOCKED_ROLLBACK_RATE`，同样立即回滚（恰等于阈值不触发，边界唯一）；
4. 否则等待全部设备到达终态：`Succeeded/Total >= SuccessThreshold` 开门进下一批，
   达不到则按越界类型给出阻塞结论；批次位点只允许单调前进。

早期阻塞特性：即使大部分设备还在途，只要已有终态计数越过阈值就立即触发，
做到“失败率超阈值必须暂停后续放量”。

## 回滚规则

- 进入 `ROLLING_BACK` 时：
  - `SUCCESS` 设备 → `ROLLBACK_QUEUED`，使用**新命令代次**（attempt+1）下发回滚；
  - `IDLE`/`AWAITING`（确定未换装）→ `SKIPPED`；
  - 仍 `IN_FLIGHT` 的设备**保留在途**：其迟到上报继续受理——迟到成功则补入回滚队列，
    迟到失败/可重试/不兼容则 `SKIPPED`，没有任务被静默丢失；
- `ROLLBACK_SELF`（设备换装后自行回退）计入自回滚率与已回滚数，设备已在旧版本，不再重复回滚；
- 回滚下发同样受每批在途配额约束；回滚全部排空（无排队/在途）后发布单终态 `FAILED`。

## 上报分类：三类原因严格区分

`classifyReport` 是纯函数，输入当前状态 + 原始上报，输出原因码：

| 原因码 | 类别 | 处理 |
| --- | --- | --- |
| `INCOMPATIBLE_PROTO_TOO_NEW` / `INCOMPATIBLE_DEVICE` | 设备不兼容，不可升级 | 终态 `INCOMPATIBLE`，计入失败率 |
| `INVALID_MISSING_FIELD` / `INVALID_MALFORMED` | 上报无效（字段缺失、结果枚举未知、固件与结果矛盾等） | **不静默丢弃、不计成功**，原样拒绝并说明原因，设备仍在途 |
| `RETRIABLE_FAILURE` / `RETRIES_EXHAUSTED` | 瞬时失败可重试 / 重试耗尽 | 尝试次数 ≤ `MaxAttempts` 重投，耗尽转 `FAILED` |
| `STALE_REPORT` | 旧代次/跨重启纪元/无在途任务的迟到或重复上报 | 忽略，绝不重复计数 |
| `BATCH_MISMATCH` | 上报批次与设备所属批次不一致（串号/迟到） | 忽略 |

关键机制：

- **Attempt（命令代次）**：每次下发递增，设备对重复命令去重；旧 attempt 上报判 STALE；
- **Epoch（恢复纪元）**：进程每次从持久化状态重启都 +1，上报必须带回命令下发时的纪元，
  跨纪元迟到旧上报判 STALE，从而与崩溃重投命令明确区分。

## 持久化与重启续跑语义

- 所有状态放在单个 `State` 中，**每次状态转移整体原子落盘**：
  FileStore 写同目录临时文件 → fsync → rename，崩溃不会留下半截 JSON；
- 重启后：
  - 恢复到未完成批次与发布阶段；
  - 已确认终态的设备保持终态，不重复下发/计数；
  - 崩溃前在途命令可能已随进程丢失，统一退回排队（升级→`AWAITING`，回滚→`ROLLBACK_QUEUED`），
    Epoch +1 后重投；跨纪元迟到旧上报判 STALE；
- **批次位点回退被明确拒绝**：`Store.Save` 发现 `Current` 小于已提交位点时返回
  `*ErrBatchRegress`，旧快照覆盖不会让发布“从中间回退继续”。

## 可复现性

- 编排器所有公开方法用互斥锁串行化，状态转移是确定函数；
- 设备挑选按设备 ID 排序，门槛判定只依赖计数器与总数；
- 因此并发、乱序、重复上报只影响“上报被处理的先后”，不影响最终计数器与结论
  （集成测试用相同输入连跑 3 轮断言结果一致）。

## 本地验证

```bash
# 全部单测（含并发/乱序/重复/重启/兼容性场景），竞态检测
go test -race -count=1 ./...

# 多轮稳定性（并发用例结果必须始终一致）
go test -race -count=5 ./internal/sim/

# 只看端到端判定日志（关键输入与判定依据都打印在测试日志里）
go test -v ./internal/rollout/ -run 'TestFailureRate|TestRestart'
```

端到端演示：

```bash
# 正常三批放量（含可重试、乱序、重复上报）
go run ./cmd/ota-demo -scenario happy -state /tmp/ota-demo.json

# 失败率超阈值触发回滚（进程退出码 1）
go run ./cmd/ota-demo -scenario rollback -state /tmp/ota-rb.json

# 对同一 -state 文件再次运行 = 模拟重启续跑：
# 终态会直接恢复并不再下发；非终态则从断点继续
```

## 测试场景索引

| 关注点 | 测试 |
| --- | --- |
| 配置与门槛纯逻辑 | `TestNewReleaseValidation`、`TestEvaluateGate*` |
| 上报分类（缺失/畸形/不兼容/乱序/重复） | `TestClassifyReport` |
| 在途配额、唯一在途、暂停 | `TestDispatchNeverExceedsQuota`、`TestPauseStopsDispatch` |
| 分批推进 | `TestStagedRolloutHappyPath` |
| 失败率/自回滚率触发回滚、迟到上报分流 | `TestFailureRateTriggersRollbackOfUpgradedDevices`、`TestSelfRevertTriggersRollback` |
| 重试与耗尽 | `TestRetriableFailuresRetryThenExhaust` |
| 不兼容判定 | `TestIncompatibleDeviceIsTerminalFailure` |
| 重启续跑、跨纪元 STALE | `TestRestartResumeMidBatch`、`TestRestartResumeDuringRollback` |
| 位点回退拒绝 | `TestBatchCursorNeverRegresses`、`TestStoreRejectsBatchCursorRegression` |
| 并发/乱序/重复 + 可复现 + race | `sim.TestConcurrentOutOfOrderAndDuplicates_Completes`、`sim.TestConcurrentFailuresRollbackConsistent` |
