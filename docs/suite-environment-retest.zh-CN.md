# 测试套件与环境问题修复后的复测

本次改动对应 [backlog #3173](https://github.com/rustfs/backlog/issues/3173) 的测试侧归因，基于已合并的 auto-testing #144。它不代表修复了服务端 503、命名空间锁超时或性能异常。

| 原失败 | 修复行为 | 复测验收 |
|---|---|---|
| TBL-101 导入失败 | 从候选对应的 RustFS checkout 准备整个 `scripts/table-catalog`，记录源码 SHA 与文件哈希；先检查本地模块，再创建 venv | 端到端实际读写成功；缺模块必须报告工具准备错误 |
| M-1x2EC1 启动非参与节点 | 显式传递参与节点数量，只对这些节点提交非阻塞启动并等待 active | 完整保留各拓扑 / EC / 故障探针，不能用未执行代替通过 |
| Rebalance 活动判据超时 | 单独增大 Rebalance 活动夹具并先校验容量，保留 `Started` 与非零进度断言；提前完成明确报告活动窗口不足 | stop/resume、live I/O、operation guards 必须在活动窗口执行；完成态仍要求非零迁移进度及数据校验 |
| 扩池负载写满单盘 | 2 秒容量采样，单盘利用率与剩余字节保护；预留在途对象和观测到的增长量，缩短停止等待 | 达到目标才通过；容量保护提前停止是环境容量失败，仍执行 MPU 清理 |
| 性能轮次含服务错误却计入摘要 | 错误日志使该轮失败，原始指标保留作诊断；逐轮记录 Report 是否出现、退出码、错误行数 | `rounds.tsv` 和 Markdown 包含全部选定轮次；超时与未执行都不算通过 |
| AppleDouble 输入 / 语法报告缺口 | 排除 `._*`，对每个真实脚本独立执行 `bash -n`，保存逐文件结果 | 任一真实脚本失败，整体返回非零；空选择也不通过 |

## Table 源码准备

```bash
python3 scripts/prepare_table_smoke.py prepare \
  --source-root /path/to/rustfs-checkout \
  --destination /path/to/new-run/table-catalog \
  --expected-sha FULL_CANDIDATE_SOURCE_SHA
```

`--destination` 必须是新目录，避免混入旧版本模块。链式 workflow 使用 manifest 的 source SHA；独立 workflow 可通过 `source_ref` 指定与产物匹配的源码。未指定时使用 release tag 或当次 checkout 的 main，并在 `source-manifest.json` 保留实际 SHA。

Table workflow 将 `table-catalog/source-manifest.json` 与报告、用例表和日志一起上传，保留实际 smoke 源码 SHA 与文件哈希，供 runner 临时目录清理后追溯。

## 容量配置

Rebalance 源用例明确要求活动阶段已有非零迁移进度。上一轮每对象 32 MiB 的任务在首次周期保存进度前完成，因此不能通过。专用活动夹具默认改为每对象 128 MiB（默认 128 对象，其中 3 个边界对象更小），通过 `RUSTFS_ACTIVE_REBALANCE_FIXTURE_BYTES` / `--active-rebalance-fixture-bytes` 配置，并记录在 run manifest。写入前按 EC 占用上界与每盘至少 1 GiB 的剩余余量检查容量。其他 Decommission 活动夹具保留原大小；更快的机器仍可能需要增加夹具，窗口不足时用例失败而不是放松断言。

容量采样与 Rebalance / Decommission 的 `--poll-interval` 分开。默认每 2 秒采样，一次直接 API 请求上限 3 秒，活动写入期间不做长重试或代理探测。代理仍在写入前后探测，错误继续保留。

预留量为每盘最少剩余字节（默认 512 MiB）加 `对象大小 × 并发数`，再加观测到的集群增长速率乘以采样、请求与停止窗口。集群增长作为单盘增长的保守上界，不假设数据均匀。容量库存为空、无效或发生变化会停止继续写入。

可配置 `RUSTFS_CAPACITY_POLL_INTERVAL`、`RUSTFS_CAPACITY_SAMPLE_TIMEOUT`、`RUSTFS_CAPACITY_MIN_FREE_BYTES`、`RUSTFS_CAPACITY_MAX_DISK_PERCENT`、`RUSTFS_WARP_STOP_GRACE`、`RUSTFS_WARP_OBJ_SIZE`。小容量环境应先按磁盘余量降低并发或目标利用率。旁路 `.capacity.jsonl` 保留每次判断的输入、余量与停止原因；这是一项负载保护，无法保证在不可预测的突发写入下绝不耗尽容量。

MPU 列表错误会保留 HTTP status、`x-rustfs-error-cause`、request ID 与 retry-after（服务端返回时）；不采集完整 AWS debug 输出或响应 Cookie。

## 结果边界

性能轮次总超时仍覆盖准备、负载和收尾。`timeout_before_report` / `timeout_after_report` 仅说明是否出现 Warp Report，不能推断 Warp 内部卡在具体阶段。不要把扩池发生的磁盘满用来解释此前的性能超时。

`--parse-only` 输出独立的 `rounds.reparsed.tsv`、`summary.reparsed.tsv`、`summary.reparsed.md` 与 `master.reparsed.log`，保留原执行台账、摘要及日志。重解析无法恢复耗时和原始运行配置；应以原 `rounds.tsv` 中的 `NOT_RUN / aborted` 及耗时为执行证据。性能 workflow 同时上传原执行台账与重解析产物。步骤 6 缺少原摘要时展示重解析摘要，重解析失败仍返回非零。

复测需保留产品源码 SHA、CI 产物身份、二进制 SHA-256、测试提交 SHA、实际配置和完整逐条结果。VM 写操作及最终恢复使用同一个共享锁；恢复后核对四节点、24 个数据挂载与对象校验。
