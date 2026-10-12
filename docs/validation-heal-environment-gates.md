# Heal 环境门控与原生阻断

第一批 13 个用例保留全部 recipe、分支和 source assertions，并新增 `environment_cases.py` 的控制流程与断言，接入 validation 调度。它们默认关闭，只产生 `BLOCKED` 结果与未满足原因；默认路径不会连接或修改 fleet。真实执行沿用 auto-testing 的 S3/Admin collector、ACK ledger、候选包绑定、物理故障 oracle 和恢复模式，专用资源操作由部署环境的适配器提供。当前没有把适配器协议或未运行代码计为 canonical PASS。

设置 `AUTO_TESTING_ENABLE_ENV_GATED_HEAL=1` 并通过 `VALIDATION_HEAL_ENVIRONMENT_CAPABILITIES` 指向 JSON 能力清单后，门控会检查全部能力。缺项仍返回 `BLOCKED`。能力齐全后还要用 `VALIDATION_HEAL_ENVIRONMENT_RUNTIME` 指向 runtime JSON；只有候选/checkout/client 摘要、全部拓扑、当前 Heal SSH 节点和适配器摘要校验通过后才会运行分支。未绑定适配器仍为 `BLOCKED`；执行结果需要 source assertion 审核，当前实现不会自动生成 canonical PASS。

能力清单示例：

```json
{"capabilities": ["dedicated_faultfs_or_device", "slow_io_short_read_eio", "namespace_isolation", "fault_layer_restore"]}
```

| Code | 用例 | 当前未满足原因 | 启用条件 |
| --- | --- | --- | --- |
| F08 | `heal.mixed-version-upgrade-rollback` | 没有经过审核的不可变双版本组合 | 两个兼容 release、混合 peer 状态、滚动升级/回滚和格式兼容矩阵 |
| F09 | `heal.dedicated-host-forced-restart` | 共享 Heal 节点不具备 H6 专用主机及平台断电能力 | 专用 H6、平台 power-cycle、带外恢复、boot_id/平台事件证据 |
| G07 | `heal.race-ilm-expiry-and-transition` | 缺隔离 ILM/tier 与真实操作交叠窗口 | 专用租户、Expire/Transition/Restore 窗口、远端版本映射、规则清理 |
| G08 | `heal.race-replication-resync` | 缺独占复制两端与 resync 控制 | 隔离复制对、resync、跨端版本映射、复制/Heal 交叠证据 |
| H01 | `heal.with-expansion-rebalance` | 缺完整 H5 扩容/rebalance 拓扑 | H5 扩容、rebalance task、逐对象 pool/set 映射、scanner 退休证据 |
| H02 | `heal.with-rebalance-stop-resume` | 缺独立 rebalance 停止/恢复/重启控制 | H5 rebalance、独立 task、stop/resume 和 service restart |
| H03 | `heal.with-decommission` | 缺可回滚的退役拓扑 | H5 decommission、survivor pool、写入限制、移动 ACK 与物理权威证据 |
| H04 | `heal.with-decommission-cancel-clear` | 缺退役新代次与容量锁重试窗口 | cancel/clear/resume、#2423 锁窗口、代次绑定 ACK ledger |
| H05 | `heal.after-retired-pool-removal` | 缺 retired pool 移除后的 survivor-only 拓扑 | retired pool 配置移除、源持续离线、survivor 权威、旧 pool 写保护 |
| H07 | `heal.ec84-multi-set-restart` | H4 的至少 24 个独立 endpoint 不可用 | H3 12 独立盘、H4 24 endpoint、逐 set 身份与重启/崩溃证据 |
| I01 | `heal.slow-io-and-eio` | 没有专用 faultfs/故障设备 | 专用故障层、slow/short-read/EIO、namespace 隔离、原状恢复 |
| I06 | `heal.kms-and-tier-outage` | 没有专用 KMS/tier 租户与依赖隔离 | 独占租户、依赖网络隔离、key 脱敏、tier 版本映射 |
| J05 | `heal.long-soak` | 缺可持续 24h/72h 的独占 lane 与资源预算 | 真实墙钟窗口、限速和监控、循环故障、到期回收；短跑不能替代 |

`matrix.py list` 同时输出 `automaticExecutor`、`dispatchable` 和 `executionPolicy`：67 个 Heal 自动执行器、13 个环境门控、4 个证据/执行器阻断。调度覆盖共 80 个 Heal，连同 74 个非 Heal 为 154/158；这不代表 154 个 live PASS。

第二批 4 个用例未进入可执行调度。经核实，C11/F07/J02 已有 candidate-bound 原生回归；这三项主要缺完整真机编排与证据闭环。I05 仍缺精确服务端屏障，增加节点不能解决：

| Code | 用例 | Block 原因 | 解锁所需证据 |
| --- | --- | --- | --- |
| C11 | `heal.bucket-incarnation-recreation` | H1/H2 旧元数据盘重返、原始 checkpoint/resume 与 committed MRF 重放编排已实现；精确候选的 9 项存储原生回归和 3 项 manager incarnation 前置证据已独立核验。完整现场分支尚无独立审计结果 | 旧/新 incarnation、正常 S3 删除与新 ACK、物理范围和原生 artifact 精确绑定；单分支不提升整例 |
| F07 | `heal.checkpoint-and-mrf-corruption` | 已实现 H1/H2 checkpoint 截断、未来 schema、committed MRF CRC 故障及独立安全重扫；10 项原生回归作为前置证据。真实较早代次回放与私有限额卷的 journal ENOSPC 已实现；仍缺完整 native cut 审核，完整 live 尚未审计 | 真实 durable 记录故障与 publication 证据绑定；现有 interruption hooks 不等于每个 fsync 或断电 |
| I05 | `heal.lock-timeout-and-caller-abort` | 请求耗时/TCP 断开不能证明服务端锁 owner 与提交顺序 | lock owner、timeout、caller abort 和 stale read-repair 的服务端屏障 |
| J02 | `heal.outcome-counters-and-error-truth` | 已有 2 项 outcome 原生回归；缺混合 healthy/repaired/failed/deferred/unknown 真机夹具、dry-run 独立任务与完整 manifest 对账 | 每个 disposition、计数和 attemptFailures 对账；不是已证实产品缺少 API |

I05 和已接入 A09、G01–G06 的精确屏障分支仍受候选包对应、经过审核的 `e2e-test-hooks` 限制。第一批 G07/G08 的 native 分支与 H04 容量锁窗口也保留 BLOCKED；不能用外部请求重试或最终对象正确来声称精确 lock/version/marker/MPU/copy 提交屏障已命中。

资源适配器协议使用 stdin/stdout JSON，不调用 shell。runtime 中的 `adapter` 包含绝对可执行路径、SHA-256 和操作 allowlist；`environment_cases.required_operations(case_id)` 给出所需操作。每个请求携带 run ID、原候选 commit、sequence、拓扑/分支 scope、bucket prefix、租约和参数。响应必须回显上述身份、按 `heal_probe.canonical(request)` 的 UTF-8 SHA-256 绑定原请求，并把真实观察写在 `observation`，禁止返回 verdict。资源适配器需独立实现平台 power control、故障层、pool lifecycle 或 KMS/tier 隔离；本 PR 未向当前共享环境部署适配器。

runtime 的 `binding` 使用现有 `heal_probe.validate_binding` 契约，另含 `schemaVersion: 1`、`provider: auto-testing`、`sshNodes`、`rcBinary`、`topologies`、`capabilities`、`adapter` 和对应资源字段。F08 需要 `releasePair`，F09 需要 `hostResourceId`，I01 需要 `faultDeviceId/ioLatencyMs`，G07/I06 需要 `kmsKeyId/tierEndpoint/tenantId/dependencyNamespaceId`，G08 需要 `replicaEndpoint`，J05 需要 `soakLimits`。H6 专用主机应由适配器按 `hostResourceId` 独占绑定；SSH fleet 仍记录当前 auto-testing Heal 节点。secret 通过环境传入，适配器诊断会脱敏，`redactionEnvironmentVariables` 可指定额外 secret 环境变量名。

J05 的实际控制循环保留固定 86400/259200 秒窗口、精确版本写入/删除 ACK、三类循环故障、恢复、资源阈值和到期全量校验。单元测试用可注入的时钟验证窗口控制，不修改生产时长。G07/G08 精确 native 分支不调用资源适配器。清理失败保留原失败与 DIRTY 原因，停止后续分支。
