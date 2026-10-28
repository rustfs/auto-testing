# 用例迁移实现审查

审查日期：2026-10-28。冻结源 catalog 提交为
`524eb5b32c3236497fdd8a9271df0494fa3d9974`，SHA-256 为
`463fdd02dee43e42fef4f3aacf7506709ce87af535e1a53d91c521bc2e361cad`。
本次交付范围是 auto-testing 中的测试流程、原断言和证据采集。产品执行结果独立保留。

## 实现和执行结果

| 测试实现状态 | canonical 用例数 | 含义 |
| --- | ---: | --- |
| 已实现 | 145 | 各源分支已有执行路径、原断言及证据采集；其中 115 项已有完整 live 审计，30 项运行失败或尚未完整验收。 |
| 环境条件限制，默认关闭 | 13 | 操作流程、能力检查、原断言和未满足原因已保留；执行需要明确的资源及经过审核的资源适配器。 |
| 实现部分完成 | 0 | 已补齐 F07、I05 的测试接线和源流程总控。 |
| 合计 | 158 | 保留全部源 canonical ID。 |

独立 live 状态仍为 115 项 accepted、22 项 retained-failure、4 项 pending、
4 项 blocked、13 项 disabled。共 43 项尚无完整 live 验收，其中 13 项按约定默认关闭。
这些结果跨多个不可变候选和历史批次，不构成某个候选通过全部 158 项的产品认证。
22 项失败需要逐例归因，不能统一宣称为产品缺陷，也不能直接据此认定测试没有迁移。

`validation-migration-review.json` 列出全部 158 个 ID、实现状态、代码/测试引用、
源分支、缺口及独立 live 状态。`reviewed_files` 固定引用文件摘要，变化后须重新审查。
文件存在和本地测试通过不代替源语义审查或 live 审计。本文件不覆盖原始结果。

```bash
python3 -B scripts/validation_migration_review.py --require-complete
```

此门禁仅因测试实现缺口失败，产品 FAIL 不影响实现计数。13 项环境流程另行输出所需
适配器清单。产品门禁 `validation_report.py` 仍要求源断言、清理、来源和完整 live PASS。

## F07：持久化临界点

冻结源 `docs/heal-cases.md#F07` / `scenarios/heal-recipes.json` 要求精确本 run 的
checkpoint/MRF 故障、持久化责任保留、损坏记录保留，以及独立 native fsync/commit
窗口补证。入口 `persistence.py` 组合以下已存在的能力：

| 能力 | 证明范围 |
| --- | --- |
| `persistence.py` / `persistence_mrf.py` | 五种 live 字节损坏、写失败及重扫分支，保留原责任、失败和恢复证据。 |
| manager checkpoint-page-order / checkpoint-stale-manager | 正常发布和 reload 次序；旧 manager 不能覆盖新 cursor。正常 reload 不冒称提交中断。 |
| manager mrf-migration-boundaries | 实际 AfterPayload、BeforeManifest、AfterManifest、AfterReadback 四个中断窗口及保留/重试断言。 |
| manager root-uncertain-publication | 实际终态写入后注入故障，保留 pending owner，重启隔离旧任务。 |
| `persistence_native.py` | 六个 ECStore CAS/MRF 目录 fsync 故障回归，覆盖更新、首次创建、删除和恢复锚点。 |

checkpoint 通过 `EcstoreDiskAPI::compare_and_update_file` 落盘。manager 的发布次序、
中断/重启责任与 ECStore CAS 落盘失败分别补证；没有为每个 fsync 发明额外测试要求。
审查源码为允许的不可变候选 `072b57b37559fbe626aeda8abf61617fdcbdd926` 和
`1aa9be87d18cf941e5020f19e8622c26ba4b10ac`；完整目录 fsync 组合只允许后者。

`collect_native_persistence` 在执行前核对 native manifest 与 fleet 候选，执行六项回归
后独立验证并再次核对候选；`native_branch_result` 再核对 manager/native 同候选。
成功和失败分别写入 `independent-native-persistence.json` 与
`independent-native-persistence-failure.json`，原始 native 日志保留在 `native-persistence/`。
缺少 artifact 时只允许原诊断子集，不能宣称完整 native coverage。

使用当前 auto-testing 环境时，由现有 workflow 传入以下目录：

```text
VALIDATION_HEAL_MANAGER_DIRECTORY/
  tool-run.json
  manager-tools/
  manager-evidence/
VALIDATION_HEAL_NATIVE_DIRECTORY/
  tool-run.json
  native-tools/
  native-evidence/
```

伴随 workflow 在精确目录 fsync 候选上下载已有 native artifact，并将目录传给入口。
同一候选的原生结果仍是独立 prerequisite；不代表 H1/H2 fleet 命中了内部窗口，不声称
物理断电或每个 file fsync hook 已执行。F07 的完整独立 live 审计待完成，canonical 门禁保留。

## I05：源流程总控和代次证明

`lock_source.py` 复用 `lock_timeout_runner.py`、`lock_timeout.py`、`lock_successor.py`
及 `lock_observer.py`。默认 `source-plan` 顺序执行 timeout/H1、timeout/H2、
queued-successor/H1、queued-successor/H2，每次使用独立 run ID、bucket 和证据目录。
任何分支、证据链或清理失败立即停止后续部署，保留原请求、ACK、日志及停机现场。

部署前只读验证同一成功 hosted artifact 中的三个精确源码 native 子集：

| 子集 | 证明范围 |
| --- | --- |
| I05 owner/unlink/stale | 历史 read-repair owner、取消 physical unlink owner 和原有 stale final rename 回归。旧 stale 测试只检查 ETag。 |
| G02 explicit-version append | 真实缺片重建在 final rename 屏障暂停；新版本 ACK 和物理 tail drain 后释放；检查类型化结果、全部存活 body、版本、dataDir 和逐盘 shard。 |
| G03 explicit-version delete | historical/current 删除与最终提交竞态，保留上述完整内容和物理代次断言。 |

G02/G03 的较强证明来自已有原生测试，不给旧 ETag 测试补写能力。两组覆盖 4/2 与
16/4，分别保留 6 与 12 条有序屏障里程碑。append 可能安全修复原选定版本并保留新版本，
不把所有类型化结果统一描述为 stale 拒绝。候选固定为
`757b751482bb970bc321e92059690a7698554358`；三个 proof 的 package/source tree/
Cargo.lock/二进制/工作流 attempt/helper/contract 身份必须一致。

原生前提可单独检查，命令不执行下载的二进制：

```bash
python3 -B scripts/validation_heal/lock_source.py \
  --directory "$VALIDATION_HEAL_NATIVE_DIRECTORY" \
  --candidate candidate.json --testing-commit "$AUTO_TESTING_SHA"
```

fleet 控制器保留 DV 历史版本和 DL 单次并发请求/ACK ledger，继续核对所有 peer 的
完整对象/版本、停机物理分片、锁事件和有界 FD 收尾。DL 是工作负载语义，不是新存储格式。
新证据链关联请求写出、超时/排队、TCP 断开、原 guard drop、successor 获取和观察结束；
对原始 bytes 逐一固定 SHA-256，核对请求/lock owner/binding 和同一协调者时钟顺序。
不会比较不同主机 monotonic 时钟，也不把本地 guard drop 当成分布式 owner 释放。

`source-plan-summary.json` 记录四个源分支的 fleet/native 证明范围；
`sourceProceduresComplete` 只有 H1/H2 四个实例完整且清理通过才为 true。单独选择 timeout
或 queued-successor 仍是诊断子集。全部控制器完成也不自动增加 canonical PASS；完整
source-plan 的独立 live 审计仍待执行。

## 失败归因和后续验收

C11 的旧 metadata、checkpoint、MRF replay 和 native incarnation 均有执行路径；
J02 的混合 outcome、dry-run、deferred 和 unknown-negative 均有控制器。其历史产品
结果和证据限制保留，尤其不能修改 J02 的 Failed/Unknown oracle 来获取全绿。

G01/G02 的失败 PUT 不重试，未知 mutation 不算成功。B03/B04 仍缺精确 mount/切换
适配器，在部署前阻断。今后按产品缺陷、环境前提、证据缺失和测试实现问题逐项归因；
有明确测试实现问题才重开实现缺口。实现交付完成不代替候选包的后续回归与产品验收。
