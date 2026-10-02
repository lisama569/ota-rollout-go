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

# 跑本地演示：两批次放量 -> 一台设备协议不兼容 -> 熔断 -> 已升级设备回滚 -> 安全中止
go run ./cmd/otademo -state ./ota-demo-state.json
# 再跑一次：从同一个状态文件恢复，不重复下发任何任务
go run ./cmd/otademo -state ./ota-demo-state.json
```

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | 计划、上报、任务、判定等核心值类型 |
| `state.go` | 发布/批次/设备状态机、计划指纹、深拷贝、审计历史 |
| `report.go` | 上报结构校验、失败三分类、`HandleReport` 全部判定分支 |
| `engine.go` | 配额调度、健康门槛、批次推进、回滚波次、重启补偿、`Run` 循环 |
| `orchestrator.go` | 编排器装配、持久化、`RecoverOrResume` 与位点回退校验 |
| `storage.go` | `FileStore`（临时文件 + 原子 rename）与 `MemoryStore`（测试/重启模拟） |
| `sim.go` | 本地模拟设备行为脚本与内存传输通道（确定性延迟、乱序、重复、畸形帧） |
| `cmd/otademo` | 本地可运行的端到端演示 |

## 发布状态机

### 整批发布（RolloutPhase）

```
Running ──门槛超阈值/批次耗尽未达标──▶ RollingBack ──所有新固件设备回退完毕──▶ Aborted
   │                                      │
   │                                      └─回滚失败耗尽（设备卡在新固件）──▶ Aborted（需人工介入）
   └────────全部批次过门──────────────────────────────────────────────────▶ Completed
```

### 单个设备（DevicePhase）

```
Pending
  │ 配额允许，下发升级任务 a1
  ▼
UpgradeInflight
  ├─ ACK（可乱序，不改变终态）
  ├─ UpgradeSuccess ───────────────▶ Upgraded ──收到回滚任务──▶ RollbackInflight
  │                                                                  ├─ RollbackSuccess ─▶ Done
  │                                                                  └─ RollbackFailure×N（耗尽）▶ 触发 Aborted
  ├─ UpgradeFailure(incompatible) ─▶ Incompatible（终态，永不重试，计失败）
  ├─ UpgradeFailure(retryable) ─────▶ 未耗尽：下发 a2...，仍在 UpgradeInflight
  │                                  已耗尽：Failed（终态，计失败）
  ├─ UpgradeFailure/帧(invalid) ───▶ 任务保持在途，不计成功、不消耗重试，等待有效重报
  └─ RollbackSuccess（设备自发回退）─▶ Done（计入回滚率）
```

`RollbackInflight` 下另有：`RollbackFailure` 在回滚预算内重试，耗尽落
`RollbackFailed`（需人工介入终态）；`RollbackSuccess` 版本与基线不符/缺失时
判 `invalid`，保持在途等待重报。

回滚波次期间仍在途的升级设备：升级成功会进入回滚队列按配额放行；临时失败
**不再重试升级**，直接落 `Done`（设备从未跑到新固件，天然安全）。

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

## 回滚规则

触发回滚时按设备当前状态分别处理：

| 设备状态 | 处理 |
| --- | --- |
| `Upgraded`（已在新固件） | 进入回滚队列，按配额逐台放行（见下） |
| `UpgradeInflight`（在途升级） | 等终态：成功→进回滚队列；失败→不再重试，留旧版 `Done` |
| `Pending`（尚未放量） | 直接 `Done`（从未升级，无需回滚），且永不被补下发升级 |
| `Incompatible` / `Failed` | 终态保持，计失败 |

回滚是**全发布范围**的（之前已过门批次里仍在新固件上的设备也会被回滚）。
所有需要回滚的设备都回到基线版本后，回滚波次收敛，发布以 `Aborted` 结束——
这是“安全的中止”，区别于 `Completed`；只有“设备卡在新固件且回滚预算耗尽”
（`RollbackFailed`）才是需要人工介入的非安全中止。

### 回滚配额与排队

- 回滚波次的在途配额 = 触发回滚时当前批次的 `MaxInFlight`；
  **升级与回滚两条链路的在途总数**任何时候都不超过该配额（整体不会把设备打满）。
- 需要回滚的设备按**批次序 + 计划内设备序**的固定顺序排队（与上报先后无关，
  可复现）；前一台设备落回基线释放名额后，队列中的下一台才被补发。
- `Stats()` 返回 `Quota / UpgradeInflight / RollbackInflight / RollbackQueued`
  快照，两条链路的在途与排队情况一眼可看；日志中每次放行与每次波次评估
  也会打印这组计数。
- 排队（`Upgraded`）与在途（`RollbackInflight`）状态都持久化：重启后在途任务
  以相同 TaskID 重放、队列按同一顺序继续放行；已确认回滚完成的设备不再下发、
  不重复计数。

### 回滚重试预算与人工介入

- 单台设备的回滚重试预算为 `MaxRollbackAttempts`（含首次下发；`<=0` 时回落为
  `MaxAttempts`）。预算内失败按确定性节奏重试（尝试号 +1，TaskID 确定）。
- 预算耗尽：设备落 `RollbackFailed` 终态、发布 `Aborted`，判定分类为
  `manual-intervention`（`ClassManual`），与 `incompatible` / `invalid` /
  `retryable` 明确可区分，等待人工介入。
- 已用次数持久化在 `RollbackAttempt`：重启不会抹掉预算，也不会因此多打一轮；
  已耗尽设备在重启后不再收到任何任务。

### 版本基线

- 每台设备升级前自报的版本（首个携带 `FromVer` 的升级上报，首帧优先）记录为
  回滚基线 `BaselineVer`；回滚任务的目标版本即该基线。
- `RollbackSuccess` 只有落回版本**等于基线**才计成功；版本对不上或缺失都判
  `invalid`（可区分原因），不消耗重试预算，设备保持在途等待重报。
- 兼容范围：基线未知（如从旧版本状态恢复、设备上报未携带 `FromVer`）时，
  采纳首个**非空**回报版本为基线；空版本仍然无效。

## 上报判定：唯一、可复现、可解释

编排器对每帧上报都产出一条带原因的 `Decision`（同时写日志与审计历史）：

- **首帧终态优先**：同一 `TaskID` 的第一帧终态判定是唯一生效结论。之后
  - 与结论一致的重复帧 => `duplicate`（不重复计数、不重复下发）；
  - 与结论矛盾的迟到帧 => `stale`（忽略，但留痕）；
  - 属于旧尝试号的帧 => `stale`；无法识别的 TaskID => `failed-invalid`。
- **三类失败严格可区分**（`FailureClass`），另有第四类人工介入终态：
  - `incompatible`：设备协议主版本高于计划（或设备明确报 `protocol-too-new`），
    设备不可升级，落终态，永不重试；
  - `invalid`：缺 DeviceID/TaskID、未知上报类型、版本号畸形、目标版本与任务目标不符、
    回滚落回版本与基线不符/缺失、任务种类与上报种类不符等。**绝不静默丢弃**，
    显式判无效；不计成功，也不消耗重试次数；
  - `retryable`：其余临时故障（如 `download-timeout`），在 `MaxAttempts` 内下发新任务重试；
  - `manual-intervention`：回滚重试预算耗尽，设备可能仍在新固件上，
    落 `RollbackFailed` 终态并中止发布，等待人工介入。
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
  - 仍在发布态时按当前批次配额继续放量；回滚态时按回滚配额补放队列并检查收敛，
    已用掉的回滚重试次数原样保留。

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

# 本轮新增的回滚加固能力：
go test -race -v -run TestRollbackQuotaAndQueueing         # 回滚配额/排队/放行顺序
go test -race -v -run TestRollbackResumeMidWave            # 回滚波次中途重启续跑
go test -race -v -run TestRollbackRetryBudgetSurvivesRestart  # 重试预算跨重启保持
go test -race -v -run TestRollbackBaseline                 # 版本基线不符/缺失/兼容
go test -race -v -run TestConcurrentRollbackReports        # 并发回滚上报结论唯一

# 本地演示（流程走完进程自行退出，不依赖外部超时）：
go run ./cmd/otademo -state /tmp/ota-demo.json   # 熔断 -> 配额内排队回滚 -> 安全中止
go run ./cmd/otademo -state /tmp/ota-demo.json   # 再跑：从状态文件恢复，不重复下发
```

测试日志会打印关键输入与判定依据，例如：

```
decision device=s3 task=...a1 kind=failed-incompatible report=upgrade-failure
  class=incompatible reason="device protocol v2.0.0 newer than plan v1.0.0: ..."
gate batch=0 total=4 evaluated=2 upgraded=1 rolledback=0 failed=1 inflight=1
  failRate=50.0% rbRate=0.0% (limits 25.0%/25.0%, minN=2) trigger=s3
ROLLBACK TRIGGERED batch=0 reason="health gate violated at evaluated=2: ..."
```

## 设计取舍

- **单锁串行化判定**：灰度发布是低频控制面操作，正确性优先于吞吐；单把锁让
  “乱序/重复/并发下结论唯一”和 `-race` 清洁都容易论证。
- **确定性优先**：任务 ID 确定、门槛只看计数器、模拟延迟按哈希取值（无随机源），
  因此测试无需 sleep 碰运气即可稳定复现。
- **无效帧不丢弃**：宁可要求设备重报并显式留痕，也不静默按成功/失败计入。
