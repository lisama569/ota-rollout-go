# 固件灰度发布编排器（本地可跑）

一个用 **本地模拟设备 + 内存传输通道** 驱动的固件 OTA 灰度发布编排器。
不依赖任何外部服务，所有异常路径（乱序、重复、迟到、畸形上报、协议不兼容、
重启续跑）都在本仓库的单元/集成测试中稳定、可复现地验证。

- 语言：Go（`go 1.25`，在 Go 1.26 上开发验证）
- 无第三方依赖，标准库即可运行

## 快速开始

```bash
# 跑全部测试（带竞态检测与关键判定日志）
go test -race -v ./...

# 多次重复，确认结论稳定可复现
go test -race -count=10 ./...

# 跑本地演示：配额1 下受控回滚——排队、固定节奏重试（2 次失败后成功）、
# 版本基线核验，流程走完进程自行退出（不靠外部超时兜底）。
go run ./cmd/otademo -state ./ota-demo-state.json
# 再跑一次：从同一个状态文件恢复，已中止态不再下发任何任务，立即退出
go run ./cmd/otademo -state ./ota-demo-state.json
# 想重新演练：删掉状态文件即可
rm -f ./ota-demo-state.json
```

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | 计划、上报、任务、判定等核心值类型 |
| `state.go` | 发布/批次/设备状态机、计划指纹、深拷贝、审计历史 |
| `report.go` | 上报结构校验、失败三分类、`HandleReport` 全部判定分支 |
| `engine.go` | 配额调度、健康门槛、批次推进、回滚队列/配额泵/定时重试、重启补偿、`Run` 循环 |
| `orchestrator.go` | 编排器装配、持久化、`RecoverOrResume` 与位点回退校验 |
| `storage.go` | `FileStore`（临时文件 + 原子 rename）与 `MemoryStore`（测试/重启模拟） |
| `sim.go` | 本地模拟设备行为脚本与内存传输通道（确定性延迟、乱序、重复、畸形帧、回滚版本/重试） |
| `rollback_quota_test.go` / `rollback_resume_test.go` / `rollback_baseline_test.go` | 回滚限流排队、回滚期重启续跑与预算持久化、版本基线与并发唯一性 |
| `cmd/otademo` | 本地可运行的端到端演示（受控回滚，流程走完自行退出） |

## 发布状态机

### 整批发布（RolloutPhase）

```
Running ──门槛超阈值/批次耗尽未达标──▶ RollingBack ──所有新固件设备回到基线──▶ Aborted（安全中止）
   │                                      │
   │                                      └─某设备回滚重试预算耗尽（设备可能仍在新固件）
   │                                         ──▶ Aborted（人工介入，立即冻结，不再推进队列）
   └────────全部批次过门──────────────────────────────────────────────────▶ Completed
```

### 单个设备（DevicePhase）

```
Pending
  │ 配额允许，下发升级任务 a1
  ▼
UpgradeInflight
  ├─ ACK / 任一有效帧：记录 FromVer 作为回滚基线（一次性，之后不可改写）
  ├─ UpgradeSuccess ──▶ Upgraded ──回滚触发，入队──▶ RollbackQueued
  │                                                  │ 配额有空位，按队列顺序放行
  │                                                  ▼
  │                                              RollbackInflight
  │                                                  ├─ RollbackSuccess(ToVer==基线) ─▶ Done(restored)
  │                                                  ├─ RollbackSuccess(缺版本/版本≠基线) ─▶ invalid，
  │                                                  │     保持在途等重报，不消耗重试预算
  │                                                  ├─ RollbackFailure（预算内）─▶ RollbackWaiting
  │                                                  │     按固定节奏到点原地重投（名额保留）
  │                                                  └─ RollbackFailure（预算耗尽）
  │                                                        ─▶ RollbackFailed（人工介入终态，
  │                                                            class=rollback-budget-exhausted）
  ├─ UpgradeFailure(incompatible) ─▶ Incompatible（终态，永不重试，计失败）
  ├─ UpgradeFailure(retryable) ─────▶ 未耗尽：下发 a2...，仍在 UpgradeInflight
  │                                  已耗尽：Failed（终态，计失败）
  ├─ UpgradeFailure/帧(invalid) ───▶ 任务保持在途，不计成功、不消耗重试，等待有效重报
  └─ RollbackSuccess（设备自发回退）─▶ Done（计入回滚率）
```

回滚波次期间仍在途的升级设备：升级成功会立即进入回滚队列；临时失败**不再重试升级**，
直接落 `Done`（设备从未跑到新固件，天然安全）。

## 放量规则（健康门槛）

每个批次有设备名单与在途配额 `MaxInFlight`：

1. 只有当前批次处于 `Active` 才允许放量；任一设备任一时刻至多一个在途任务；
   批次整体在途数严格不超过配额。设备终态释放名额后，后续设备才被补下发。
2. 每次设备终态事件后，基于**确定性计数器**评估（与上报到达顺序无关）：
   - `failRate = (incompatible + retryable 耗尽) / 已终态设备数`
   - `rollbackRate = 已回到旧版本设备数 / 已终态设备数`
3. 已终态设备数达到 `MinEvaluated` 后，任一比率**严格超过** `MaxFailRate` /
   `MaxRollbackRate`（边界值不熔断）即**立即暂停后续放量并触发回滚**；
   未达到最小样本数时不会被个位数样本误杀。
4. 批次全部设备终态后复核：`upgraded==0` 或任一比率超阈值 => 回滚；
   否则批次 `Passed`，推进下一批。最后一批通过 => `Completed`。

## 回滚规则（受控、可恢复、可解释）

触发回滚时按设备当前状态分别处理：

| 设备状态 | 处理 |
| --- | --- |
| `Upgraded`（已在新固件） | 进入回滚队列，在当前批次配额内逐台放行；失败按预算与固定节奏重试 |
| `UpgradeInflight`（在途升级） | 等终态：成功→进回滚队列；失败→不重试升级，留旧版 `Done` |
| `Pending`（尚未放量） | 直接 `Done`（从未升级，无需回滚），且永不被补下发升级 |
| `Incompatible` / `Failed` | 终态保持，计失败 |

回滚是**全发布范围**的（之前已过门批次里仍在新固件上的设备也会被回滚）。

### 配额与排队（升级 / 回滚共享在途预算）

- 回滚**不单独开闸**：同一时刻，“当前批次仍在途的升级任务” + “回滚在途任务”
  + “回滚失败后等待定时重试的设备”总数严格不超过**当前批次的 `MaxInFlight`**。
  两条链路的在途情况可通过 `RolloutStatus()` 一眼看明白（`Queued / Inflight /
  Waiting / Restored / Manual`），整体不会把设备打满。
- 等待重试的设备**保留它自己的名额**：重试窗口内队列后续设备不会顶上来，
  队列顺序与“谁占名额”都稳定可解释。
- 排队顺序**固定、可复现**：严格按（批次序号, 批次内名单位置）排序，
  与上报到达顺序无关。队首设备回到基线释放名额后，下一台才被补入。
- 回滚队列（`RolloutQueue`）随状态持久化：重启后队列、在途、等待重试状态
  原样恢复；已确认回滚完成的设备不在队列、不会被重复下发或重复计数。

### 重试预算与人工介入

- 回滚使用**独立**于升级的重试预算 `MaxRollbackAttempts`（为 0 时回退用
  `MaxAttempts`）。每台设备的已用尝试次数持久化在设备记录上。
- 预算内失败后**不立即重发**：按固定节奏 `RollbackRetryInterval`
  （默认 100ms）排定 `NextAttemptAt` 并持久化，设备进入 `RollbackWaiting`，
  由补偿 tick 到点以**相同确定性 TaskID** 原地重投。重启不会抹掉已用次数，
  未到点绝不提前重投，因此不会多打一轮；重复/迟到的失败帧只得到
  `duplicate/stale`，不会重复扣预算。
- 预算耗尽：设备进入明确终态 `RollbackFailed`，失败类别是可区分的
  `rollback-budget-exhausted`；编排器**立即冻结**（停止推进队列后续设备、
  不再下发），发布 `Aborted`，审计决策写明“MANUAL INTERVENTION REQUIRED”。
- 所有需要回滚的设备都确认回到基线后，回滚波次安全收敛、发布以 `Aborted`
  结束——这是“安全的中止”，区别于 `Completed`；只有“预算耗尽、设备可能仍
  停在新固件”才是需要人工介入的非安全中止。

### 版本基线（回滚结果必须落回升级前版本）

- 每台设备升级前运行的版本作为**回滚基线**：从它对升级任务的首帧非空
  `FromVer` 记录（ACK、成功、失败帧都可贡献），一旦记录不再改变。下发的
  回滚任务 `Target` 即该基线。
- 回滚成功帧必须携带落地版本 `ToVer` 且**等于记录的基线**才算成功：
  - 设备报回一个对不上的版本，或干脆没说版本 → 判定 `failed-invalid`
    （原因字符串区分“版本不符 / 缺版本 / 无基线可核验”），**不计成功、不
    消耗重试预算**，设备保持回滚在途，等待设备重报；
  - 只有 `ToVer == Baseline` 才落 `Done(restored)`，并据此释放名额、补位队列。
- 兼容范围：基线是**逐设备**记录的字符串版本，允许同批次设备升级前版本
  不同；每台设备只与**自己的**基线比对。设备在未被编排器要求回滚时的
  自发回退（升级窗口内的 rollback-success）仍按原语义直接计入回滚率，
  不做基线校验（那时没有对应的编排器回滚任务）。

## 上报判定：唯一、可复现、可解释

编排器对每帧上报都产出一条带原因的 `Decision`（同时写日志与审计历史）：

- **首帧终态优先**：同一 `TaskID` 的第一帧终态判定是唯一生效结论。之后
  - 与结论一致的重复帧 => `duplicate`（不重复计数、不重复下发）；
  - 与结论矛盾的迟到帧 => `stale`（忽略，但留痕）；
  - 属于旧尝试号的帧 => `stale`；无法识别的 TaskID => `failed-invalid`。
- **三类失败严格可区分**（`FailureClass`）：
  - `incompatible`：设备协议主版本高于计划（或设备明确报 `protocol-too-new`），
    设备不可升级，落终态，永不重试；
  - `invalid`：缺 DeviceID/TaskID、未知上报类型、版本号畸形、目标版本与计划不符、
    任务种类与上报种类不符等。**绝不静默丢弃**，显式判无效；不计成功，也不消耗重试次数；
  - `retryable`：其余临时故障（如 `download-timeout`），在 `MaxAttempts` 内下发新任务重试。
- **乱序**：成功帧早于 ACK 到达时直接生效，迟到 ACK 只是重复；可选的 `Seq`
  字段提供更严格的乱序保护（更小序号判 `stale`）。
- 全部判定在单把互斥锁下完成，计数器是确定值，因此同一批上报无论以何种
  乱序/并发顺序喂入，终态结论一致（见 `TestDeterministicVerdictUnderShuffledReports`）。

任务 ID 是确定性的：`<rollout>-b<batch>-<u|r>-<device>-a<attempt>`，
同一（发布、批次、设备、种类、尝试号）永远是同一 ID。

## 持久化与重启续跑语义

- 每次状态迁移后整盘保存：`FileStore` 写临时文件、`fsync` 后原子 `rename`，
  崩溃只会看到旧版或新版 JSON，不会出现半截文件；损坏文件加载即报错。
- 保存的 JSON 含完整设备状态、批次位点与审计历史；`MemoryStore` 以
  “编码字节 → 重新解码”方式提供完全一致的重启模拟。
- `RecoverOrResume` 的校验：
  1. 状态不存在：初始化新发布，从批次 0 开始；
  2. `RolloutID` 不一致或 **计划指纹（计划内容 SHA-256）不一致**：拒绝（`ErrPlanChanged`），
     绝不允许换计划后沿旧位点继续；
  3. **批次位点回退一律拒绝**（`ErrBatchIndexRolledBack`，见 `CheckNoRewind`），
     不允许“从批次中间重新放量”。
- 恢复后的补偿（不会重复计数）：
  - 已确认终态的设备**绝不再下发**；
  - 状态显示在途的任务以**相同确定性 TaskID 重投**（至少投递一次），
    设备与状态机都按 TaskID 去重，因此重复投递是幂等的；
  - 仍在发布态时按当前批次配额继续放量；
  - 回滚态时先**对账回滚队列**（剔除已离队设备、补入仍停在新固件的设备、
    按全局名次重排），再由配额泵续跑；
  - `RollbackWaiting` 的设备必须等到持久化的 `NextAttemptAt` 才重投，
    重启既不会抹掉已用回滚预算，也不会提前多打一轮。

## 本地验证方法

```bash
# 全量 + 竞态
go test -race ./...

# 关注的场景测试：
go test -race -v -run TestConcurrentDuplicateReports       # 32 并发重复上报，终态唯一
go test -race -v -run TestDeterministicVerdictUnderShuffledReports  # 乱序结论一致
go test -race -v -run TestSimE2E                           # 模拟通道端到端 + 中途重启
go test -race -v -run TestResume                           # 重启续跑/指纹/位点回退
go test -race -v -run TestThreeFailureClasses              # 三类原因可区分
go test -race -v -run TestFileStore                        # 真实文件持久化

# 回滚加固相关场景：
go test -race -v -run TestRollbackQuotaAndQueueOrder       # 回滚限流、固定顺序排队、逐台补位
go test -race -v -run TestSharedQuotaUpgradeAndRollback    # 升级/回滚共享在途配额
go test -race -v -run TestResumeRollback                   # 回滚期重启：队列/在途/定时重试续跑
go test -race -v -run TestRollbackFailureExhaustsAborts    # 固定节奏重试 + 预算耗尽人工终态
go test -race -v -run TestRollbackVersionMismatchIsInvalid # 回滚落地版本与基线对不上判无效
go test -race -v -run TestRollbackSuccessWithoutBaselineRecorded  # 没说版本/无基线不算成功
go test -race -v -run TestConcurrentRollbackReportsUnique  # 并发成功/错版/重复，结论唯一
go test -race -v -run TestRollbackFailureDuplicatesCountedOnce    # 重复失败帧只扣一次预算
```

测试日志会打印关键输入与判定依据，例如：

```
baseline captured device=s1 baseline=1.0.0
rollback enqueue device=s1 batch=0 queuePos=1 baseline=1.0.0
rollback pump admitted=1 occupied=1/1 queued=1
decision ... kind=retried class=retryable reason="...; rollback retry 2/3 scheduled at 05:10:53.394 (fixed cadence)"
rollback redrive device=s1 attempt=2 (scheduled at 05:10:53.394)
decision ... kind=failed-invalid class=invalid reason="rollback landing version \"1.5.0\" does not match recorded baseline \"1.0.0\""
rollback gate upgraded=0 queued=0 inflight=0 waiting=0 restored=2 manual=0 statusQ=[]
```

## 设计取舍

- **单锁串行化判定**：灰度发布是低频控制面操作，正确性优先于吞吐；单把锁让
  “乱序/重复/并发下结论唯一”和 `-race` 清洁都容易论证。
- **确定性优先**：任务 ID 确定、门槛只看计数器、模拟延迟按哈希取值（无随机源），
  因此测试无需 sleep 碰运气即可稳定复现。
- **无效帧不丢弃**：宁可要求设备重报并显式留痕，也不静默按成功/失败计入。
