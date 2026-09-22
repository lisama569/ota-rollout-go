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

回滚波次期间仍在途的升级设备：升级成功会立即补发回滚任务；临时失败**不再重试升级**，
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

## 回滚规则

触发回滚时按设备当前状态分别处理：

| 设备状态 | 处理 |
| --- | --- |
| `Upgraded`（已在新固件） | 下发回滚任务；失败可重试，耗尽则整单 `Aborted` 等待人工介入 |
| `UpgradeInflight`（在途升级） | 等终态：成功→立即回滚；失败→不再重试，留旧版 `Done` |
| `Pending`（尚未放量） | 直接 `Done`（从未升级，无需回滚），且永不被补下发升级 |
| `Incompatible` / `Failed` | 终态保持，计失败 |

回滚是**全发布范围**的（之前已过门批次里仍在新固件上的设备也会被回滚）。
所有需要回滚的设备都回到旧版本后，回滚波次收敛，发布以 `Aborted` 结束——
这是“安全的中止”，区别于 `Completed`；只有“设备卡在新固件且回滚耗尽”才是
需要人工介入的非安全中止。

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
  - 仍在发布态时按当前批次配额继续放量；回滚态时补发缺失的回滚任务。

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
