# Heal validation procedures

These source procedures require live execution and branch evidence on the current auto-testing Heal fleet.
Use [the operations reference](validation-heal-operations.md) before injecting a fault. A procedure card or complete archive is not a PASS.

<a id="A01"></a>

## A01: heal.admin-healthy-noop

Topologies: H1,H2. Branches: normal, deep.

1. 用 DB 夹具生成完整 manifest；先对每个对象保存 S3 基线和四盘分片摘要，关闭 scanner/MRF/auto-heal。
2. 依次执行 START object（scanMode=1、2），WAIT 原 token 直至终态；再 STATUS 同 token 两次。
3. 对每个对象的 outcome 区分 Normal 的 metadata_healthy 与 Deep 的 verified_healthy；两者均要求 healed=0、unchanged>0，Normal 不得冒充 payload 完整性证明。比较目标/控制对象物理摘要和 S3 清单，重复请求不得新建版本。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [STATUS](validation-heal-operations.md#STATUS) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- 对健康对象分别执行 normal/deep，再重复查询
- 不修改用户数据或版本；Normal 只接受 `metadata_healthy`，Deep 才要求 `verified_healthy`，healthy 与 repaired 分开计数，不能把检查盘数当修复盘数。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A02"></a>

## A02: heal.admin-scope-isolation

Topologies: H1,H2. Branches: root, bucket, prefix, object, pool-set.

1. 用 DS 在两个桶的 a/、ab/、b/ 写入不同内容；另在未选 set 放控制对象；逐对象预先记录实际 pool/set。
2. 分别隔离选中对象、相邻前缀及另一桶/未选 set 的一个 part。每个 selector 使用新夹具和故障 ID。
3. 对象请求路径到完整 key；prefix 路径到 a/ 且 recursive=true；bucket 路径只含桶；root 为 /；pool/set 写入 body。START 后 WAIT。
4. 先 PHYSICAL：选择范围内全部重建、范围外控制故障仍缺失；然后 GET 验证，避免验证请求提前修好控制故障。root 必须包含整个专用服务清单。

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 分别以 root、bucket、prefix、object、pool/set 选择器启动
- 目标范围实际遍历；相邻前缀、其他桶及未选 set 的故障保持原状；按对象清单而非目录数量验收。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A03"></a>

## A03: heal.admin-token-cross-node-status

Topologies: H2. Branches: coordinator-to-peer, coordinator-to-stable-endpoint.

1. 在 H2 同一跨节点 set 写入 DB；保存协调者、目标、稳定入口分别对应的端点，不把同一节点的两个别名当跨节点。
2. 隔离目标节点 part；从 n0 START，记录原始 clientToken；通过 n1 和 stable 对同一路径、同 token 执行 STATUS。
3. 按同一任务身份对齐三端状态，最多等候冻结的状态收敛预算；终态及核心计数必须一致，404/unknown 不能视为结束。
4. WAIT 后先物理证明目标重建，再从所有 S3 入口 VERIFY；保存每个请求实际连接的节点日志。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [STATUS](validation-heal-operations.md#STATUS) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 从节点甲启动，经乙及稳定入口按原 token 查询
- 任务身份、scope、终态及核心计数有界收敛；状态缺失/未知不能解释为完成。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A04"></a>

## A04: heal.admin-dry-run

Topologies: H1,H2. Branches: missing-remove0-recreate0, missing-remove1-recreate0, bitflip-remove0-recreate1, bitflip-remove1-recreate1, versioned-missing, versioned-bitflip.

1. 分别用 DB 与 DV 建夹具，冻结全部 versionId/marker；关闭后台来源，不在注入后 GET。
2. 在原始 part 上做 missing/bitflip，每组独立新目录；保存故障后的 PHYSICAL 快照作为 dry-run 前基线。
3. START body 同时传 dryRun=true、scanMode=2 和该实例的 remove/recreate；WAIT 仅接受 dry_run_observed，不能要求 dry-run 持有 incarnation。
4. 前后所有分片/元数据/marker 字节与版本清单相同；没有 repaired ACK。完成非变更证明后恢复故障，再 VERIFY。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 对缺片与损坏对象执行 dryRun，参数化 remove/recreate
- 有明确 dry_run_observed 诊断；用户对象分片、xl.meta、版本与删除标记不变；后台修复干扰须排除。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A05"></a>

## A05: heal.admin-overlap-and-force-start

Topologies: H1,H2. Branches: same-scope, parent-child, disjoint, force-start.

1. 用 DX 超过 128 项的外部分片夹具注入多对象缺片；预先从固定候选记录 overlap policy（复用 token 或冲突错误），禁止接受任意结果。
2. START 递归范围，WINDOW 证明 0<processed<expected 且任务 running；在窗口内重发同范围、父子前缀或不相交范围。
3. 保存每次 token/HTTP 错误；forceStart 实例在 query 加 forceStart=true。观察旧任务停止与新任务独立接管责任，不能同时提交陈旧结果。
4. 等待所有被受理任务终态；逐对象补片/版本校验，检查活动任务与在途写退出，未命中活动窗口计 NOT_OBSERVED。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY) → [LOGS](validation-heal-operations.md#LOGS)

Source assertions:

- 活动中重发同范围、父子前缀和不相交请求，再 forceStart
- 按已固定 overlap policy 验证合并返回原 token 或明确冲突；forceStart 不能泄漏旧任务责任或并发破坏对象。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A06"></a>

## A06: heal.admin-stop-and-stale-token

Topologies: H1,H2. Branches: stop-active, repeat-stop, old-status, old-stop-new-task.

1. DX 批量缺片后 START；WINDOW 观察非零且未完成进度，再 STOP 原 token。
2. 状态显示 aborted/cancelled 后仍观察物理写入静默窗口；停止受理成功不能替代在途退出。
3. 新 START 得到不同 token，再对旧 token STATUS/STOP；新任务不应被旧请求终止，重复 stop 按冻结的 HTTP/错误码验收。
4. 新任务完成后完整 VERIFY；记录取消项不能算 repaired，所有暂态错误保留原始 body。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [STOP](validation-heal-operations.md#STOP) → [STATUS](validation-heal-operations.md#STATUS) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- 非零修复进度时 stop，稳定后新建任务，再发旧 token 的查询/停止
- 停止需等在途写结束并稳定；新任务身份独立，旧请求不得终止新任务；重复 stop 按公开契约处理。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A07"></a>

## A07: heal.admin-invalid-request

Topologies: H1,H2. Branches: duplicate-query, invalid-bool, invalid-scan, conflicting-flags, invalid-selector, oversize-body.

1. 建立健康 DB 和 PHYSICAL 基线；保存后台/任务状态及对象版本集合，用于检查负请求未意外启动任务。
2. 逐个发送服务端负例：重复 clientToken；forceStart=wat；scanMode=99；forceStart=true&forceStop=true；pool=-1/set 越界；超过候选限制的 body。每个例固定预期 HTTP 及结构化错误码。
3. 使用 ADMIN_RAW 的重复 --query 和原始 --body-file，不能只调用 rc 的参数校验；不使用超出专用 Heal 路由的请求。
4. 全部响应无有效新任务受理，服务仍可用；PHYSICAL、版本、后台责任无改变；若返回 2xx 或错误无明确原因保留 FAIL。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [ADMIN_RAW](validation-heal-operations.md#ADMIN_RAW) → [STATUS](validation-heal-operations.md#STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 发送非法 selector、重复 query、非法布尔/scanMode、冲突 flags、超长 body
- 服务端返回已核实的结构化错误；无误启动、范围扩大或持久化副作用；CLI 参数错误不能替代服务端负例。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A08"></a>

## A08: heal.admin-authorization

Topologies: H1,H2. Branches: anonymous, bad-signature, expired-signature, denied-user, least-privilege-user.

1. 通过 IAM 手册在专用服务创建两个临时用户，分别没有和仅有 HealAdminAction；用受控凭据环境运行，先证明限制用户不能读未授权对象。
2. 管理员创建待查询/停止任务；分别对 start/status/stop 发匿名、坏签名、有效签名但时间超期、无授权用户请求；过期请求由隔离客户端签名时间设置产生，不改服务器时间。
3. 检查冻结的 AccessDenied/签名/时间错误；响应不暴露对象结果。最小授权用户完成受理/查询/停止，证明接口本身可工作。
4. 移除临时用户和策略并验证凭据失效；所有原始证据先脱敏再归档，不把管理员 token 暴露给无关主体。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [IAM](validation-heal-operations.md#IAM) → [START](validation-heal-operations.md#START) → [ADMIN_RAW](validation-heal-operations.md#ADMIN_RAW) → [STATUS](validation-heal-operations.md#STATUS) → [STOP](validation-heal-operations.md#STOP) → [VERIFY](validation-heal-operations.md#VERIFY) → [RECOVER](validation-heal-operations.md#RECOVER)

Source assertions:

- 匿名、坏签名、过期签名、无 HealAdminAction 用户与最小授权用户分别请求
- 未授权 start/status/stop 均不能越权或泄漏对象信息；授权用户可完成规定操作，凭据不得写入证据。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A09"></a>

## A09: heal.admin-nolock-safety

Topologies: H1,H2. Branches: overwrite-nolock, delete-nolock, native-lock-fence.

1. DB/DV 准备同一目标并缺片；两客户端使用同 key，冻结每个 PUT/DELETE 的唯一 nonce 和 ACK ledger。
2. 一端 START body nolock=true，另一端按 WORKLOAD 提交覆盖或精确 versionId 删除；记录确实交叠的运行区间。
3. 按 ACK 因果顺序重建期望清单并 VERIFY；最后成功覆盖不回退、已删除版本不复活。
4. 另外运行 NATIVE 的锁/提交屏障回归并保存精确测试名、源码 SHA、命中日志；黑盒竞态与 native-lock-fence 单独结果，不以偶然重叠证明强制锁时点。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 通过签名 API 传 nolock=true，并与同对象提交制造确定性争用
- 兼容字段不得跳过存储层对象锁；最后成功写入和已确认删除保持正确，原生屏障与真机竞争证据分开。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="A10"></a>

## A10: heal.admin-option-contract

Topologies: H1,H2. Branches: recursive-false, recursive-true, recreate, remove-false, remove-true, update-parity, read-repair, selector-valid, selector-invalid.

1. 将候选 HealOpts 支持表和危险参数副作用写入本批 contract.json；不根据 CLI help 推导 API 未暴露字段。
2. 为每个 variant 使用新 DB/DV：固定范围、足够权威和控制对象；remove=true 只对经 C10 证明无活引用的本次孤儿执行。
3. 逐一 START 并对比只改变一个字段的基线；recursive 检查子前缀，pool/set 检查实际目标身份，非法组合要求结构化拒绝且无副作用。
4. WAIT、PHYSICAL、VERIFY 三者验收；updateParity/readRepair 若候选不支持必须显示明确拒绝，不可悄悄按默认修复算覆盖。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 参数化 recursive、recreate、remove、updateParity、readRepair 与合法/非法 pool/set 组合
- 仅测试已核实的候选语义；normal/deep 与危险 remove 的副作用明确；API 独有选项不得伪称 rc 已覆盖。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B01"></a>

## B01: heal.disk-loss-and-repair

Topologies: H1,H2. Branches: original-drive-return, offline-ack-repair.

1. 由 layout.json 选一成员盘，记录同版本 N/R/W；使用 DRIVE_GATE 仅让本服务看不到该逻辑盘，不卸载共享 XFS。
2. 故障窗口经测试进程 mount namespace 的读失败与 Admin offline 状态双重见证；在 quorum 允许范围执行 GET 和唯一 key PUT，逐次记 ACK。
3. DRIVE_GATE 恢复原盘身份后，等待所选手工/自动来源处理离线期间责任；不得复制备份补盘。
4. 验证所有故障前对象及成功 ACK 对象、目标盘实际新增分片；节点 ready/GET 成功不是补片证据。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 让一个已确认 set 成员盘对 heal 服务不可用，持续读取，恢复原盘
- 记录真实故障窗口和源/目标盘身份；在可恢复条件下最终补齐目标分片及完整 manifest，不只验证降级 GET。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B02"></a>

## B02: heal.disk-readonly-and-permission

Topologies: H1,H2. Branches: read-only, permission-denied.

1. 在专用测试服务的 mount namespace 设置目标根只读；permission 实例用专用非 root 服务 UID 和独立目录 ACL，不能对 root 仅 chmod 后假称生效。
2. 在服务相同身份/namespace 执行有界写探针，要求 EROFS/EACCES；先保存并隔离目标 part，再触发 deep heal。
3. 目标不可写期间必须保留失败/待修责任，不能 reported repaired；PHYSICAL 确認没有写到父目录/系统盘。
4. 解除只读或恢复原 ACL，重新修复并 VERIFY；保留故障期间错误和恢复后物理补片证据。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 仅对独占测试卷制造只读/拒绝访问，恢复后重试
- 不可写目标不得 reported repaired；错误及待修责任可见；恢复后真实写入目标且不落到系统盘或其他目录。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B03"></a>

## B03: heal.disk-replacement-formatted

Topologies: H1,H2. Branches: new-disk-data, new-disk-versioned.

1. 完整基线后停止本例服务，将目标逻辑盘 data 整体隔离到本 run 的 quarantine；使用 REPLACE 建空的新 data 根。
2. 按候选初始化契约启动，读取 format.json 证明新 this UUID 属于原 pool/set/slot；其他盘格式不得变成新集群。
3. 显式 START root/deep 或本实例指定重建来源，旧盘备份不可读给测试进程；WAIT 全部责任。
4. 逐对象/版本验证目标盘分片与同角色基线摘要；确认不是从 quarantine 拷贝回去；恢复操作只在取证后做并标为 rescue。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [BASELINE](validation-heal-operations.md#BASELINE) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT](validation-heal-operations.md#FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 换入空的新逻辑盘，确认新 disk UUID/正确格式后触发重建
- 新盘与旧盘身份可区分且属于正确 pool/set/slot；逐版本重建完成，不能从隔离备份复制回来冒充 heal。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B04"></a>

## B04: heal.disk-replacement-unformatted

Topologies: H1,H2. Branches: blank-unformatted, foreign-format.

1. REPLACE 建没有 format 的空逻辑盘；记录空目录证据，启动并观察候选支持的安全初始化或明确拒绝。
2. 第二实例使用同 run 的第二个隔离小集群生成外来 format，将其按 FORMAT_BAD 手册装入替换根，保留原始副本和所有 UUID。
3. 外来身份不能被静默覆盖或挂到错误 set；若拒绝，等待有界 deadline 并确认其他正常盘数据不变。
4. 停止本服务、撤回替换根、恢复原身份后 VERIFY；空盘正常接纳实例还须逐对象重建证明。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT_BAD](validation-heal-operations.md#FORMAT_BAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 引入空未格式化测试盘，并与带异集群格式的盘分别比较
- 按候选支持的初始化契约安全接纳或明确拒绝；不得覆盖外来身份或将未就绪盘标为可写健康。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B05"></a>

## B05: heal.multiple-shard-loss

Topologies: H1,H2. Branches: loss-1-data, loss-1-parity, loss-2-through-m-data-parity.

1. 从候选/实际元数据确定 M=N-R，枚举 1..M 的合法缺片数及 data/parity 组合，写入本批展开参数表；不得只运行 loss=1。
2. 每实例用相同版本的新对象，将指定成员上的 part 分别 FAULT missing；多份元数据也要证明指向同 version/data_dir。
3. 在满足修复提交条件时 START deep/WAIT；先检查全部目标片与角色基线相同，再 VERIFY。
4. 每次恢复隔离备份后换新对象再做下一组合；低于 R 的组合另由 B07 计负例，不合并成绩。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 按实际同版本 EC 布局依次缺失 1..M 个 shard，覆盖 data/parity 角色组合
- 每个参数实例单独满足读取与写入 quorum；满足修复条件时全部目标重建，不把跨版本或跨 set 的 shard 合并。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B06"></a>

## B06: heal.read-quorum-boundary

Topologies: H1,H2. Branches: exact-r-read, exact-r-heal-write-limited, restored-w.

1. 记录读取 R、修复写 W、锁 quorum 的来源；选择同一版本保留恰好 R 个权威片，其他片隔离。
2. 第一次请求 GET 验证读取边界并记录 read-repair 干扰；用第二个相同形态新对象执行只手工 deep heal。
3. 如果只有 R 但写/锁 quorum 不足，要求拒绝持久化成功；若拓扑中该组合不可实现，在运行前声明精确原因并选满足条件的拓扑，不伪造离线数。
4. 恢复到 W 后重新启动修复并保存独立 token，比较精确目标片与全部内容；成功读不能充修复成功。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [GET](validation-heal-operations.md#GET) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 保留恰好 R 个同版本权威 shard，测试读取及修复提交，再恢复写 quorum
- 读取边界和修复写入边界分别判定；不能因还能 GET 就要求在写 quorum 不足时提交成功。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B07"></a>

## B07: heal.below-read-quorum

Topologies: H1,H2. Branches: r-minus-1-unversioned, r-minus-1-historical.

1. 选定精确 versionId 保留 R-1 份同版本权威；隔离其余 part，确保没有其他 pool 或旧版本兜底被当作同版本来源。
2. GET 必须返回冻结的不可恢复错误，不能返回 200 错内容或空对象；deep heal 的 execution/disposition 必须如实失败/未解决。
3. 在恢复前快照所有好片和 xl.meta，证实负例没有扩大损坏或执行 remove；保留失败任务原 token。
4. 仅在负例取证完成后 RECOVER 原始片，再显式 heal 和 VERIFY；恢复后的绿不能覆盖故障期间的错误。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [GET](validation-heal-operations.md#GET) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 使权威来源小于 R，执行 GET 与 deep heal，再恢复被隔离源
- 故障期间不得返回伪造的成功内容、空对象或误删记录；明确 unresolved/失败；恢复后原对象和版本完整。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B08"></a>

## B08: heal.below-write-quorum

Topologies: H1,H2. Branches: read-present-write-absent, lock-quorum-absent.

1. 用 layout.json 分别计算对象写和锁 quorum；DRIVE_GATE/NETWORK 构造仍可读但不能安全提交的测试服务可见性。
2. 执行唯一 key PUT 与已有缺片对象 heal；保存每个 ACK/HTTP/body，超时写必须查询唯一 key 解析结果，禁止直接重试换 key。
3. 期间不能 reported repaired 或返回无法兑现的成功写 ACK；从各 peer 收集 quorum 与锁失败日志。
4. 解除故障后按全部已确认/已解析写入重建 manifest，实际补齐目标并 VERIFY；普通低 quorum 错误不是基础设施错误。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [NETWORK](validation-heal-operations.md#NETWORK) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 构造读 quorum 尚存而写/锁 quorum 不足，执行 heal 与新 PUT
- 不能虚报持久化修复或成功写入；分别记录对象、元数据、分布式锁 quorum；恢复后核对所有已确认写入。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B09"></a>

## B09: heal.node-outage-rejoin

Topologies: H2. Branches: one-node-stop, rejoin-with-new-versions.

1. 确认 H2 目标 set 跨节点，统计停一节点实际损失的盘数，不能使用每台独立 pool 的 H5 替代。
2. PROCESS stop 精确测试 unit，见证 MainPID 消失及端点不可达；保持其他节点服务。
3. 按计算允许的 quorum 执行降级 PUT/DELETE/GET，保存版本化 ACK；重入同一节点同一数据根。
4. 观察 MRF/修复责任直到补齐离线期间成功对象；从所有节点 VERIFY 和目标 PHYSICAL，不能只看进程 ready。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [PROCESS](validation-heal-operations.md#PROCESS) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 停止同一跨节点 set 的一个节点，执行允许的降级 I/O，再重入
- 按实际丢失盘数判定可用性；缺失期间成功提交的对象补齐；节点重新 ready 不等于所有对象已重建。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="B10"></a>

## B10: heal.disk-and-node-flapping

Topologies: H2. Branches: drive-flap-3, node-flap-3, interrupt-rebuild-again.

1. 预注册三个故障周期、每次最长窗口和总 deadline；准备带 nonce 的 ACK ledger，设置只属于本服务的 drive gate 或 unit 操作。
2. 每轮离线/在线都先见证实际状态；第三轮在 WINDOW 证明部分重建时再次离线，不以固定 sleep 替代。
3. 记录每轮责任/队列、任务身份、错误率及 CPU/FD；超过预注册重试/资源预算即失败，不继续循环等绿。
4. 彻底解除故障后验证全部已确认对象、目标分片和持久队列收敛；活动窗口没命中记 NOT_OBSERVED。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [PROCESS](validation-heal-operations.md#PROCESS) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [METRICS](validation-heal-operations.md#METRICS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 重复短暂离线/在线并在重建途中再次失联
- 重试与队列有界、身份稳定，无重复任务风暴；最终故障解除后收敛，所有已确认对象保持正确。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C01"></a>

## C01: heal.missing-part-file

Topologies: H1,H2. Branches: data-single-part, parity-single-part, multipart-first, multipart-middle, multipart-last.

1. DB/DM 写入后用 FORMAT/PHYSICAL 将每个 version/part 映射到真实盘角色；每个 variant 选不同目标，不按盘序号猜 data/parity。
2. 停住注入目标写入后 FAULT missing 只移除已冻结摘要的 part，xl.meta 和 format 保持；恢复运行后禁止目标 GET。
3. START deep 并 WAIT；首先验证该角色目标 part 恢复到故障前完整 SHA-256/长度，再 GET/Range 和版本清单。
4. 任何仅 xl.meta 恢复、重建错误 part 或 control 变更都失败；清理时才允许使用原始备份。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 仅隔离指定对象某个 part/shard，保留 format 与其他元数据，执行 deep heal
- 对应数据或校验分片重新生成，part/版本身份准确；不得只看到 xl.meta 重现就通过。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C02"></a>

## C02: heal.data-shard-bitrot

Topologies: H1,H2. Branches: data-first-byte, data-middle-byte, data-last-byte, multipart-data.

1. 记录候选解析出的 data shard；用不重复 SHAKE 内容避免数据片/校验片摘要相同，验证健康基线。
2. FAULT bitflip 在冻结的首/中/尾字节偏移各翻转一位；保存偏移、注入前后 hash 及目标物理角色。
3. 保持足够正确来源，分别执行本批预注册 deep/read-repair 触发；手工实例不得预读目标。
4. 目标恢复完整原始片摘要，解码后全文和 Range 正确；只返回正确 GET 但坏片仍在必须失败。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 在指定 data shard 的已知偏移翻转字节，包含首/中/尾位置
- 故障前后摘要证明注入生效；deep 或 read-repair 发现损坏并修复；独立 bitrot/EC 及 S3 全内容验证通过。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C03"></a>

## C03: heal.parity-shard-bitrot

Topologies: H1,H2. Branches: parity-first-byte, parity-middle-byte, parity-last-byte.

1. 选择经 FORMAT 证明的 parity 片，保留足够 data；普通读取是否需要该片另作控制实例，不能触发后再声称纯 deep。
2. FAULT bitflip 覆盖首/中/尾位置；注入后仅 PHYSICAL，不 GET/HEAD。
3. START deep/WAIT，要求 repaired 责任对应正确对象/版本/pool/set；与修复前同角色 SHA-256 比较。
4. 全文 GET 及版本/metadata 对照通过且错误 parity 已消失才通过；后台提前补片则该触发实例未观测。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 损坏 parity shard，使普通 GET 仍可能成功，再执行 deep heal
- 必须识别并重建损坏 parity；只有 GET 正确、无目标分片修复不能通过。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C04"></a>

## C04: heal.truncated-and-zeroed-shard

Topologies: H1,H2. Branches: truncate-half, truncate-near-tail, zero-all, checksum-header.

1. 保存外部 part 完整备份及正确 metadata quorum；每种损坏使用新的独立对象。
2. FAULT truncate 默认保留一半；near-tail 按 FAULT_FORMAT 手册只截去最后一字节并记录原始长度；zero 清零全部；header 翻转前 32 字节。
3. 见证实际长度/摘要差异后 START deep，捕获短读/校验诊断与责任结果。
4. 目标原始长度和完整片摘要恢复，GET 无截断/补零，控制对象不变；拒绝把一份可解析 metadata 当内容健康。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 分别截断 shard、置零和破坏校验头，保留正确元数据仲裁来源
- 短读、全零和校验损坏被准确识别；修复不能接受截断内容或错误长度。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C05"></a>

## C05: heal.missing-or-corrupt-xlmeta

Topologies: H1,H2. Branches: missing-xlmeta, truncated-xlmeta, corrupt-xlmeta, inline-payload, versioned-xlmeta.

1. DB 小对象与 DV 历史版本先全量读取，FORMAT 解析每盘 xl.meta 的版本/inline/data_dir 关联并保存解析器源码 SHA。
2. 在少数成员上对精确对象 xl.meta 做 missing/truncate/header；inline 实例仅由匹配候选的解析器定位 payload 后翻位，不能随便偏移后称 inline。
3. 保留同版本正确来源，START deep/WAIT；恢复后重新解析 xl.meta，核对所有版本、marker 与 data 引用。
4. 每个 versionId 全 GET 和版本集合一致；可以有合法重编码，但不能新建空 metadata、遗漏历史版本或移动删除语义。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 在少数成员上移走或损坏 xl.meta/inline 内容，保留足够权威来源
- 恢复同一对象的全部应存版本、数据引用与删除标记；不以新建空元数据替代真正修复。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C06"></a>

## C06: heal.stale-version-rejoin

Topologies: H1,H2. Branches: unversioned-overwrite, delete-historical-version, delete-marker.

1. 保存 DV/旧代次，DRIVE_GATE 让一个成员离线且其旧数据原样保留。
2. 在线 quorum 执行新内容覆盖、精确旧 versionId 删除或创建 marker；串行 ACK 确定因果顺序，并在在线端确认提交。
3. 恢复携带旧数据的同一盘，核对原 disk UUID 后观察手工/自动修复，并保存旧/新代次权威选择日志。
4. 最终最新值与全部历史/marker 严格匹配 ACK 清单；已删除版本不复活；目标旧盘同一 key 的 metadata 与内容纠正。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [START](validation-heal-operations.md#START) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 盘离线后执行覆盖/版本删除，再让携带旧元数据的盘重入
- 以已确认的新代次/版本为准；旧当前值与已删版本不复活，旧盘被纠正而非成为新的错误权威。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C07"></a>

## C07: heal.conflicting-subquorum-authority

Topologies: H1,H2. Branches: disjoint-version-quorums, disjoint-incarnations, native-authority-fence.

1. 同一个本例 key 串行写两个不同版本/代次，保存各自成员副本；按 FORMAT_BAD 在静止服务下组合为两个各小于 R 的集合，原片全部保留。
2. 解析每个成员版本身份，生成按 version/incarnation 分组的计数表；即使总数达到 R，也证明每一组都没有 quorum。
3. GET/deep heal 必须安全拒绝，不以较新 mtime 或跨版本片并集提交；检查原本仍可恢复的证据没有被 remove。
4. 运行匹配源代码的 NATIVE 权威仲裁回归；在真机解除混合现场后恢复原夹具并 VERIFY，两类证据分开计数。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FORMAT_BAD](validation-heal-operations.md#FORMAT_BAD) → [GET](validation-heal-operations.md#GET) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 用受控原生夹具构造互不构成 quorum 的版本/代次集合，再做对应真机反例
- 禁止把不同版本的 shard 数量并集当成权威；无法证明时安全拒绝且保留可恢复证据，不猜测较新时间戳。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C08"></a>

## C08: heal.metadata-payload-mismatch

Topologies: H1,H2. Branches: length-mismatch, payload-mismatch, insufficient-authority.

1. 先保存版本化外部 part 的全套 metadata 与 hash；从本例另一种长度/内容的版本取得替换 payload。
2. 停服务按 FAULT_FORMAT 只更换少数 part 的内容或截断，保留原 xl.meta 声称的长度/摘要，不改健康成员。
3. deep heal 对比 metadata 与实际内容；充分权威实例应重建，不充分权威实例应拒绝且保留好的副本。
4. 取证后恢复，验证精确版本无跨代内容；保留实际解析结果，不把任意无效 header 注入当作长度字段覆盖。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 构造元数据声称某长度/摘要但实际 part 不匹配的少数副本
- 元数据与内容联合判断；不把可解析的 xl.meta 当健康；权威不足时拒绝覆盖正确副本。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C09"></a>

## C09: heal.foreign-format-and-unknown-schema

Topologies: H1,H2. Branches: foreign-disk, foreign-set, future-format, future-object-schema.

1. 用第二个隔离测试集群生成真实外来 disk/set format；当前实例只有一个替换逻辑根可被修改。
2. FORMAT_BAD 在服务停止时分别装入外来 format 或按候选解析器支持的版本头写入未来 schema；记录原始 bytes/digest，不碰共享盘 format。
3. 启动、GET/deep heal，核对明确隔离/unsupported，不得把未知格式当 missing 初始化或写成新集群。
4. 停止服务收集原记录仍存在的物理证据；撤回坏根、恢复原身份后完整 VERIFY；无法确认格式字段不能冒充已注入。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT_BAD](validation-heal-operations.md#FORMAT_BAD) → [PROCESS](validation-heal-operations.md#PROCESS) → [START](validation-heal-operations.md#START) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 提供外来 disk/set 身份、未来 schema 或不支持的记录格式
- 明确隔离/拒绝且不破坏原记录，不把未知当 missing 后直接重建；解除故障后原数据可用。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C10"></a>

## C10: heal.dangling-and-orphan-safety

Topologies: H1,H2. Branches: remove-false, remove-true-before-grace, remove-true-after-grace, shared-version-reference, pending-mpu.

1. 创建 DV/DM 并合法覆盖/删除形成候选孤儿；以匹配格式解析器列出所有 version→data_dir 引用和 pending UploadId。
2. 只在没有活引用的本次测试目录形成 dangling；保存 orphan 成因与真实年龄，宽限期实例等待配置窗口，不改全局时间。
3. 分别 START remove=false/true；宽限期前、共享历史引用、pending MPU 必须保留，过期且有充分删除证明的孤儿才允许清理。
4. 比较物理引用集合，所有应存版本及未完成 upload list_parts 完整；remove 误删任何共享引用即失败，救援只在取证后。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 构造 dangling part、旧 data_dir 与共享历史引用，分别 remove=false/true、宽限期前后检查
- 默认不得破坏仍可恢复数据；显式 remove 只清除具有充分删除证明且过期的本次孤儿，不删共享版本或 MPU。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="C11"></a>

## C11: heal.bucket-incarnation-recreation

Topologies: H1,H2. Branches: old-metadata-rejoin, old-checkpoint-replay, old-mrf-replay, native-incarnation-fence.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`EVIDENCE_INCARNATION_LIVE_ORCHESTRATION`): 已实现 H1/H2 旧元数据重返、真实 checkpoint/resume 回放、ACK 绑定的 committed MRF 回放和精确 native 证据门禁；尚缺全部现场分支的独立审计，诊断执行保持 FAIL/BLOCKED，原生前置通过不能替代 canonical 验收。

1. 建旧 DV 桶并让一盘离线；按 S3 正常列举/删除全部版本和 marker，确认 delete_bucket 成功，禁止直接内部清空绕过删除。
2. 同名 create_bucket 后写新内容，保存新 incarnation；恢复旧盘并让旧任务/持久 MRF 重放。
3. 对比任务身份和代次守卫，旧责任不得删除/覆盖新对象，不得把旧 marker 或数据复活到新桶。
4. 按 NATIVE 验证精确 incarnation 提交屏障；真机清单及新桶配置始终匹配成功 ACK，旧/新物理范围独立留证。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [S3](validation-heal-operations.md#S3) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [START](validation-heal-operations.md#START) → [FORMAT](validation-heal-operations.md#FORMAT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 盘离线时完成旧桶合法清空/删除，再同名建桶写入新对象，恢复旧盘与旧任务
- 旧 bucket incarnation 的 checkpoint/MRF/元数据不得污染新桶；旧任务不得删除或恢复新 incarnation 的对象。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D01"></a>

## D01: heal.object-size-boundaries

Topologies: H1,H2. Branches: zero, one, 4kib-minus-exact-plus, inline-minus-exact-plus, stripe-minus-exact-plus, large-external.

1. SEED_DB 显式包含 0、1、4095、4096、4097；从固定候选配置/FORMAT 获得 inline 与 EC block 阈值，新增阈值±1 和一个多 block 外部对象。
2. 每个大小先按物理记录确认实际形态；零字节/inline 没有外部 part 时改用合法 metadata/inline 故障，不能虚构 part。
3. data/parity 上依次 missing 与 bitflip，START deep/WAIT；每个形态独立 variant 计数。
4. 核对全体长度/摘要/ETag/HEAD/LIST；跨边界及末字节 Range 为正确 206，空对象范围按冻结契约拒绝。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RANGE](validation-heal-operations.md#RANGE) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 覆盖 0/1 字节、页/inline/EC block 边界±1以及大对象
- 每个实际存储形态均确认后注入；长度、SHA-256、ETag、HEAD/LIST 与边界 Range 正确，无假设阈值。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D02"></a>

## D02: heal.completed-multipart-object

Topologies: H1,H2. Branches: first-part, middle-part, last-part, cross-part-range.

1. SEED_DM 建至少三 part 且非末 part≥5 MiB，非等长内容和独立 nonce；保存 UploadId、每个 part ETag、Complete ACK/最终版本。
2. 每实例损坏第一、中间、末尾 part 对应的一个物理片；按 FORMAT 映射实际 part，不依赖文件名猜逻辑位置。
3. START deep/WAIT 后先验证对应目标片，再完整 GET；最终 ETag 应与基线一致，但不用 multipart ETag 计算整对象 MD5。
4. RANGE 覆盖两个 part 边界及尾部，比较逐字节/摘要、metadata 和版本；其他未完成 MPU 仍可 list_parts。

Operations: [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RANGE](validation-heal-operations.md#RANGE) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 修复已完成的不等长多 part 对象，分别损坏首/中/末 part
- 完整内容、part 顺序、原版本/ETag/元数据和跨 part Range 不变；multipart ETag 不能当整对象 MD5。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D03"></a>

## D03: heal.historical-versions

Topologies: H1,H2. Branches: old-version-part, current-version-part, recursive-all-versions, multiple-version-targets.

1. SEED_DV 每 key 三个不同 payload 版本，逐一保存 versionId/ETag；可额外写 marker，清单不按当前 GET 代替历史读取。
2. 为不同历史/当前版本选不同盘和 part，metadata 故障实例保留同版本 quorum。
3. 对 object 和 recursive bucket 分别 START/WAIT；物理解析必须显示所有应存 version/data_dir 而不是只最新对象。
4. 对每个原 versionId 全 GET，LIST versions 无新增/遗漏，isLatest 身份与 marker 顺序保持。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 不同版本分布不同损坏位置，按对象和递归范围修复
- 所有原 versionId 可寻址且内容正确，最新版本身份不漂移，无多余版本或遗漏历史版本。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D04"></a>

## D04: heal.delete-marker-semantics

Topologies: H1,H2. Branches: current-marker, historical-marker, missing-marker-replica, stale-disk-marker.

1. 创建三个数据版本与当前/历史 marker，记录 delete_object 返回的 VersionId/DeleteMarker。
2. 缺少 marker 的实例使用 xl.meta 少数副本缺失；陈旧实例通过成员离线后合法创建/删除 marker 形成，不能直接改任意 metadata。
3. heal 后 current GET/HEAD 对当前 marker 应为 404 且正确 marker 身份头；指定 marker versionId 的 GET 按 S3 契约为 405 并保留版本头。
4. LIST versions 精确集合与最新标记一致；显式删除当前 marker 后露出的应是预期历史对象，不能产生额外数据版本。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [S3](validation-heal-operations.md#S3) → [FAULT](validation-heal-operations.md#FAULT) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 缺失/陈旧 marker 副本后 heal，并按当前值与精确 marker versionId 查询
- 当前 GET/HEAD 与指定 marker 的状态码及身份头符合契约；marker 不复活数据，LIST versions 无重复。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D05"></a>

## D05: heal.versioning-suspended-null

Topologies: H1,H2. Branches: enabled-to-suspended, null-overwrite, null-delete-marker.

1. 启用版本写三代，再 put_bucket_versioning Status=Suspended，连续写两次 null 版本并创建/删除 null marker。
2. 基线同时保存非 null 历史和当前 null 身份；对目标外部 part 或 xl.meta 制造少数缺失。
3. START deep/WAIT；记录 null 不能被转换成随机 versionId，也不能与启用期历史版本混片。
4. VERIFY 全部原 versionId、null 内容和 marker；再次开启版本后写新对象，验证旧历史仍可寻址。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [S3](validation-heal-operations.md#S3) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 在版本开启、暂停、null 版本覆盖之间建夹具再修复
- null 和非 null 版本不混淆，不新增假版本；暂停前历史对象仍准确可读。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D06"></a>

## D06: heal.object-metadata-and-tags

Topologies: H1,H2. Branches: user-metadata, content-headers, versioned-tags.

1. 用 DS 设置中文值可编码的 metadata、Content-Type、Content-Encoding、Content-Disposition、Cache-Control 与两组 tags；内容本身使用已声明编码。
2. 先 GET/HEAD/get_object_tagging 精确记录；对外部 part 和少数 xl.meta 分别故障。
3. START deep/WAIT，比较所有对外属性和逐版本 tags，修复不新增版本、不丢用户 key 大小写规范化结果。
4. 允许内部 metadata 合法重写，但必须通过 FORMAT 引用和物理内容证明；不把 Content-Encoding 当服务端压缩已启用。

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 包含用户 metadata、Content-Type/Encoding/Disposition、tags 的对象修复
- 对外 metadata、tags 和相关响应头逐项相同；不要求无关内部元数据字节完全一致。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D07"></a>

## D07: heal.range-head-and-conditions

Topologies: H1,H2. Branches: head, range-valid, range-outside, if-match, if-none-match, if-modified-since, if-unmodified-since.

1. DB/DM 基线保存 ETag/LastModified 和跨 part 内容；先执行所有条件请求，冻结预期 200/206/304/412/416。
2. 对精确 part 制造缺失/坏校验并 repair；同一组 HEAD、Range、IfMatch/IfNoneMatch、时间条件再次请求。
3. 304/412/416 验证响应无错误成功内容；合法范围比较准确长度、ContentRange 和原片段摘要。
4. 完整 VERIFY 版本和 metadata；条件请求必须用 API 参数传递，不由本地短路模拟状态码。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [RANGE](validation-heal-operations.md#RANGE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RANGE](validation-heal-operations.md#RANGE) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 修复前后验证 HEAD、合法/越界 Range、If-Match/If-None-Match 及时间条件
- 200/206/304/412/416 等按逐请求契约判断，绝不返回旧内容、错误范围或错误 ETag。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D08"></a>

## D08: heal.encrypted-objects

Topologies: H1,H2. Branches: sse-s3, sse-kms, sse-c, encrypted-versioned.

1. DEPENDENCY 先验证候选支持的每种 SSE 与专用 KMS/key 就绪；不支持的产品能力按明确错误契约另记，不能删除该参数实例。
2. 用 S3 ServerSideEncryption/SSEKMSKeyId 或 SSECustomerAlgorithm/SSECustomerKey 创建对象；密钥从环境注入到内存，不放命令行/清单。
3. 在密文物理层选择 part 做 missing/bitflip；deep heal 后比较同角色原密文片或匹配候选的重编码验证。
4. 用正确授权逐版本解密 GET 校验原文，用缺少/错误密钥请求验证拒绝；不得写出明文替代或把密钥错误判成不存在。

Operations: [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [SEED_DE](validation-heal-operations.md#SEED_DE) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 按候选支持的 SSE-S3/SSE-KMS/SSE-C 建加密对象并修复
- 授权解密后原内容及版本一致，物理验证使用正确编码/密文层；密钥不可用时不误删、不降级明文，证据脱敏。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D09"></a>

## D09: heal.object-lock-retention

Topologies: H1,H2. Branches: governance-retention, compliance-retention, legal-hold, historical-protected-version.

1. 创建启用 ObjectLock 的专用桶，SEED_DO 使用短且真实未到期的 retention 或 ON legal hold；记录 UTC 截止和独占资源到期安排。
2. 在无 bypass 权限角色下对受保护 versionId DELETE 必须 AccessDenied，确认保护本来有效。
3. 对受保护版本缺片/xl.meta 缺失后 repair，核对内容及 get_object_retention/legal_hold；保留期内再次删除仍拒绝。
4. legal hold 仅正常 API 解除；Compliance 等到真实到期后清理，禁止改时钟或内部删文件；未到期现场标 RETAINED，不能交还空环境。

Operations: [SEED_DO](validation-heal-operations.md#SEED_DO) → [S3](validation-heal-operations.md#S3) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [RECOVER](validation-heal-operations.md#RECOVER)

Source assertions:

- 对开启 retention/legal hold 的受保护版本修复，期间测试非法删除
- heal 不绕过保留/hold，也不丢失其元数据；拒绝删除与合法修复均有明确证据。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D10"></a>

## D10: heal.special-keys-and-pagination

Topologies: H1,H2. Branches: unicode-space-percent, encoded-slash, adjacent-prefixes, multi-page-list.

1. DS 创建中文、空格、%2F 字面量、真实 / 和多层前缀键；至少 257 个对象并设置 List MaxKeys=17，确保跨页。
2. FORMAT 建逻辑 key→物理路径映射，确认 %2F 与 / 不混淆；文件故障只用受限相对路径工具，不拼 shell。
3. 对选定 key 分别 missing part/xl.meta，使用 URL percent-encode 后的 Admin scope，deep heal。
4. 分页 LIST/versions 与 manifest 无遗漏/重复；控制前缀不变；对每个特殊 key 精确 GET 和 metadata 校验。

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 覆盖中文、空格、百分号、编码斜线、多层前缀和多页对象列表
- 逻辑 key 与物理路径映射准确，不跨桶/前缀；分页无遗漏重复，不能把用户 key 当可执行 shell 或裸文件路径。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="D11"></a>

## D11: heal.bucket-metadata-integrity

Topologies: H1,H2. Branches: versioning-config, bucket-policy-tags, lock-config, unknown-record-negative.

1. DS/DV 创建隔离桶配置：版本、只授权本桶的策略、bucket tags、可到期锁配置；通过 S3 GET 配置存基线。
2. FORMAT_BAD 白名单仅包含该桶对应的系统 bucket metadata 文件；在匹配格式/可恢复契约已核实后少数副本缺失/截断。
3. 不能使用通用 heal_fault 修改 .rustfs.sys；按专用系统记录手册在停止服务、精确备份和审批清单下操作，未知格式实例只要求安全拒绝。
4. repair 后逐 API 读取配置比对，业务对象版本完整；确认 IAM/凭据配置及其他桶系统记录完全未变。

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [S3](validation-heal-operations.md#S3) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- 在隔离桶白名单内测试可支持的桶元数据恢复，并核对外部 bucket 配置
- 版本设置、策略/标签/保留配置按契约保持；不得遍历凭据配置或把未知内部记录当可重建数据。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="E01"></a>

## E01: heal.manual-normal-attribution

Topologies: H1,H2. Branches: normal-missing-data, normal-missing-parity.

1. 部署 manual 配置：关闭 scanner、auto-heal、MRF；保留 format，记录服务实际环境与 PID，不只保存拟用 env 文件。
2. FAULT missing 后不能 GET/HEAD 目标；先确认缺失仍在，再 START scanMode=1。
3. WAIT 精确 token/outcome，先 PHYSICAL 证明目标新生成，再 VERIFY；源归属需结合配置、时间线和任务对象。
4. 正常扫描未发现静默 bitrot 不能因此判 E01 失败；那是 E02/C02/C03 的 deep 实例。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 禁止预读目标，保留 format，隔离背景干扰后显式 normal heal
- 证明本次 Admin token 实际覆盖目标并修复可由 normal 检出的缺失；不要求 normal 发现所有静默 bitrot。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="E02"></a>

## E02: heal.manual-deep-attribution

Topologies: H1,H2. Branches: deep-parity-bitrot, deep-data-bitrot.

1. manual 配置排除其他后台来源；FORMAT 确认分片角色，注入 bitflip 并保存坏片仍存在证据。
2. 不先读目标，START scanMode=2；保留请求 body、clientToken、incarnation/pool/set 与最终对象 disposition。
3. 先验证坏片完整恢复，再读取原文；如果任务开始前已有恢复，记录 ATTRIBUTION_NOT_OBSERVED。
4. 检查健康 control 没被意外修改，source 不能仅由最后一次发出的命令推测。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 在普通读取仍可成功的 parity 损坏上显式 deep heal
- 按 token/对象/版本及物理变更证明 deep 检查和修复，不把后台提前修复算给手工任务。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="E03"></a>

## E03: heal.read-repair-attribution

Topologies: H1,H2. Branches: get-missing, range-bitrot, historical-version-read.

1. 关闭 scanner/auto-heal，按候选 read-repair 实现配置其所需 MRF 队列，记录为何开启；不发送 Admin heal。
2. FAULT 后物理证明损坏尚存；只对一个精确 key/version 发一次带请求 ID 的 GET 或 Range。
3. GET 内容先验正确还不够：只观察 PHYSICAL/后台状态/日志，直到该片重建；异步路径需关联 read-repair 入队和责任完成。
4. 完成补片后才全量 VERIFY；如果只能看到读取成功但找不到 source 证据，该路径仍 NOT_OBSERVED，不能计 PASS。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [CONFIG](validation-heal-operations.md#CONFIG) → [FAULT](validation-heal-operations.md#FAULT) → [GET](validation-heal-operations.md#GET) → [RANGE](validation-heal-operations.md#RANGE) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [LOGS](validation-heal-operations.md#LOGS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 不发 Admin heal，对已确认的特定损坏对象发一次可追踪 GET/Range
- 读取正确且后续真实补片；若走异步队列需证实 source=read-repair，不能只有 GET 成功。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="E04"></a>

## E04: heal.scanner-cold-object-repair

Topologies: H1,H2. Branches: cold-missing, cold-bitrot, scanner-cycle-boundary.

1. 启用 scanner 和所需自动修复，预注册 cycle/start delay/bitrot cycle 与预算；先用健康冷对象证明扫描器实际巡检。
2. 注入时用 PROCESS 短停以保持静止窗口；重启后不发目标 GET/HEAD、不发 Admin heal。
3. 只用磁盘快照、scanner 遍历日志和后台责任观察，证明当前周期检查目标，记录发现→入队→补片。
4. PHYSICAL 完成后再全量 VERIFY；单纯周期计数增长、全局 idle 或其他自动 replacement 先修不能记 scanner 通过。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [PROCESS](validation-heal-operations.md#PROCESS) → [FAULT](validation-heal-operations.md#FAULT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [LOGS](validation-heal-operations.md#LOGS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 不 GET/HEAD 损坏目标、不发手工 heal，仅等待已启用的 scanner 路径
- 冷对象被发现并修复，来源和遍历窗口明确；scanner 周期结束但目标没检查不能通过。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="E05"></a>

## E05: heal.mrf-offline-write-replay

Topologies: H2. Branches: offline-put, offline-version-delete, offline-marker.

1. 在 H2 开启 durable MRF，先验证目标节点离线时剩余同 set 满足提交条件；DRIVE_GATE 或 PROCESS 只影响目标成员。
2. 在线节点执行唯一 key PUT、精确版本 DELETE、marker 操作；逐个保存 ACK 和责任 identity。
3. 在目标仍离线时采集 journal/状态，证明责任已持久化；恢复同一盘身份，不发手工 heal。
4. 观察 MRF 重放和退休理由；只有实际 repaired/verified_healthy/authoritatively_absent 后退休；VERIFY 全部 ACK 和目标片，不能以队列空独自证明。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [CONFIG](validation-heal-operations.md#CONFIG) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 成员离线时完成有 quorum 的 PUT/DELETE，确认待修责任后重入
- durable MRF 责任持久且身份精确；只在 repaired/verified healthy/authoritatively absent 等已证明结果后释放，成功提交不丢失。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="E06"></a>

## E06: heal.replacement-auto-rebuild

Topologies: H1,H2. Branches: replacement-unversioned, replacement-versioned-marker.

1. 启用 replacement auto-heal；REPLACE 注入空逻辑盘，旧盘备份对服务不可达；保存新 UUID/slot。
2. 重启/接纳后不发 Admin heal、不读目标；仅查询 BACKGROUND/REPLACEMENT、日志及 PHYSICAL。
3. 从 replacement generation、ready 与责任完成状态关联所有 manifest 对象/版本的补片时间线。
4. 全部目标分片实际重建后 VERIFY；服务 ready、计数重置或队列空都不能提前结束。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [CONFIG](validation-heal-operations.md#CONFIG) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT](validation-heal-operations.md#FORMAT) → [REPLACEMENT](validation-heal-operations.md#REPLACEMENT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 替换盘接纳后不发 Admin/目标 GET，观察自动 replacement heal
- 替换代次、ready/completion 与盘上全部目标版本一致；不能用服务 ready、队列空或计数重置代替完成。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F01"></a>

## F01: heal.root-graceful-coordinator-restart

Topologies: H1,H2. Branches: root-partial-graceful, same-record-disk-resume.

1. DX root heal 启动前保存原协调者、拓扑、记录盘和预算；所有 root checkpoint 通过 CHECKPOINT 只读白名单采集。
2. WINDOW 证明 0<已修目标数<总数 且任务 running，再 PROCESS graceful-stop 原协调者；记录退出信号和持久记录。
3. 用原 unit/env/数据根/候选重启原协调者，不换记录盘；观察原任务 ID/剩余责任恢复，不能将新扫描当原任务续跑。
4. 最终全部对象/片正确，root 活动记录退休，重复重启无重复任务；未观察部分进度保留 NOT_OBSERVED。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [STATUS](validation-heal-operations.md#STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- root heal 非零进度时优雅重启原协调者，保留原拓扑与记录盘
- 在明确支持的边界内恢复原 task ID 和剩余责任/预算，完成后无遗留活动记录；不扩张为协调盘丢失后的故障转移。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F02"></a>

## F02: heal.target-graceful-restart

Topologies: H2. Branches: target-partial-graceful, versioned-target-partial.

1. REPLACE 目标逻辑盘并启用自动重建，使用 DX/DV 使目标总量可观测；协调者不是重启目标。
2. PHYSICAL 与 REPLACEMENT 双证实目标只完成部分，再 PROCESS 对目标 unit 优雅停止；保存 MainPID、退出码、前后 boot_id。
3. 保留同一新 disk UUID/代次，正常启动原目标服务；不能换新目录重测。
4. 观察余下目标继续构建且任务责任一致，全部版本/marker/片完整；备份不得参与重建。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [REPLACE](validation-heal-operations.md#REPLACE) → [REPLACEMENT](validation-heal-operations.md#REPLACEMENT) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 替换目标仅完成部分重建时优雅重启目标节点
- 故障确实发生于部分完成窗口；重启后续建同一目标盘/代次，最终 manifest 与全部目标分片正确。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F03"></a>

## F03: heal.target-process-crash

Topologies: H2. Branches: target-partial-sigkill, target-partial-sigkill-versioned.

1. 与 F02 同样准备替换盘和可观察部分进度，先冻结将被杀掉的 unit MainPID/进程启动时间。
2. WINDOW 内 PROCESS kill，精确 systemctl kill --signal=SIGKILL --kill-whom=main；记录 signal=9 与非优雅退出，明确不是物理断电。
3. 正常启动同 unit/根/新 disk UUID；记录恢复 responsibility/代次，不允许丢弃尚未完成对象。
4. 全量 VERIFY/PHYSICAL，检查临时文件和持久责任无泄漏；最终通过不抹掉这次真实中断的证据。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [REPLACE](validation-heal-operations.md#REPLACE) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 部分重建时 SIGKILL 测试进程，再正常启动
- unclean shutdown 与修复进度有真实证据；已提交对象不丢失，未完成目标不会被当完整；明确不是断电测试。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F04"></a>

## F04: heal.coordinator-process-crash

Topologies: H2. Branches: root-coordinator-kill, mrf-coordinator-kill, replacement-coordinator-kill.

1. 三个机制使用独立场景和冻结的持久化支持表；在协调者非零且未完成进度时采集 CHECKPOINT。
2. PROCESS SIGKILL 协调者，目标节点保持；恢复原根/原拓扑。
3. 分别核对机制承诺：持久任务必须续责；易失 token 丢失不能算已完成，应显示未完成并允许明确的新扫描补偿。
4. 原任务与补偿任务分别保存；最终 VERIFY 所有 ACK、版本和片，不能把安全重扫成功写成原 token 自动恢复。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DB](validation-heal-operations.md#SEED_DB) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 协调者非优雅退出并恢复，分别观察 root、MRF、replacement
- 按各机制已核实的持久化边界判断；不得把丢失的易失 token 当成功，需显式记录剩余责任并可安全重扫恢复。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F05"></a>

## F05: heal.full-cluster-restart

Topologies: H2. Branches: all-graceful-restart, all-restart-with-mpu, repeat-full-restart.

1. 准备 DB/DV/DM、pending UploadId 与未完成持久修复责任；冻结全部节点/盘/slot。
2. WINDOW 命中后按固定顺序只停止本例全部 units；保存每节点退出码、checkpoint/MRF 与完整时序。
3. 按原拓扑同时或预注册顺序重启，等待全部 peer，不能用自动重新格式化或新数据根恢复。
4. 完成后全版本/marker/MPU 和物理片校验，再重复一次重启验证幂等，任何丢失的已承诺责任计失败。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 有未完成修复责任时重启全部测试节点，固定原拓扑/磁盘身份
- 有持久化保证的责任不遗失；全部端点恢复一致，旧版本/marker/MPU 与目标分片完整，重复重启不扩散错误。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F06"></a>

## F06: heal.cancelled-task-restart

Topologies: H1,H2. Branches: cancel-restart, old-token-after-restart, force-new-after-cancel.

1. DX 缺片任务在 WINDOW 中 STOP；STATUS 与磁盘静默窗口证明取消和在途退出完成。
2. 保存旧 token/checkpoint，PROCESS 优雅重启；确认已取消任务不会自行复活或报 repaired。
3. 新 START/forceStart 获取独立 token，向旧 token 发 stop/status；不得影响新任务。
4. 新任务完成后 VERIFY/PHYSICAL；取消记录/新任务责任不串代。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [STOP](validation-heal-operations.md#STOP) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [STATUS](validation-heal-operations.md#STATUS) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- 完成取消及在途退出后重启，随后 forceStart/新任务正常修复
- 已取消的任务不自动复活；旧 checkpoint/token 不能取消或覆盖新任务，旧责任不会误标为修复完成。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F07"></a>

## F07: heal.checkpoint-and-mrf-corruption

Topologies: H1,H2. Branches: checkpoint-truncate, mrf-checksum, future-schema, stale-generation, journal-write-failure, native-persist-fence.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`EVIDENCE_DURABLE_JOURNAL_ORCHESTRATION`): 已实现 H1/H2 checkpoint 截断、未来 schema、真实较早代次回放、committed MRF CRC 故障、私有限额卷的真实 journal ENOSPC 和独立安全重扫；10 项原生回归仅为前置证据。仍缺全部所需 fsync/commit 临界点审核和完整现场分支的独立 live 审计，诊断执行不能提升 canonical 验收。

1. 先制造并证明真实 durable root/MRF 责任，CHECKPOINT 定位精确本 run task/record，其他系统记录禁止触碰。
2. 停止测试服务，按 FAULT_FORMAT 对这些已审核记录分别截断、坏校验、未来 schema、旧代次；写失败实例用独占限额故障层，不塞满真实共享盘。
3. 重启后核对解析/错误状态：未知或损坏记录保留、可诊断，不能静默删除并 ACK；责任仍能安全重扫处理。
4. 精确 fsync/commit 崩溃窗口执行 NATIVE 回归，保存屏障命中证据；真机记录损坏与原生临界点不要混算。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [START](validation-heal-operations.md#START) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 受控构造 checkpoint/MRF 截断、坏校验、未来 schema、陈旧代次及写入失败
- 已确认持久化责任不得静默删除或伪 ACK；未知记录保留且可诊断；精确崩溃窗口由原生屏障证明，不伪称真机命中。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F08"></a>

## F08: heal.mixed-version-upgrade-rollback

Topologies: H2. Branches: rolling-old-new, mixed-peer-status, rollback-new-old, mixed-restart.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_IMMUTABLE_RELEASE_PAIR`): 需要两个已固定且相互兼容的 RustFS release、混合版本 peer 状态和经过审核的回滚兼容矩阵；当前 auto-testing Heal 节点没有这组不可变双版本资源。

1. 固定两个 release optimized 包的 commit/tree/binary/格式兼容说明；UPGRADE 先验证支持回滚的记录版本，不能使用任意旧包。
2. 有持久修复责任时逐节点升级，保留数据根和 unit；混合阶段通过各 endpoint STATUS/BACKGROUND 并持续唯一版本写入。
3. 缺少能力的 peer 必须显示 partial/unsupported，不能 aggregate 成 complete；新记录不被老节点误退休。
4. 按兼容方案回滚并重启，验证所有 ACK 和责任；不兼容格式只测明确拒绝/保护，禁止冒险降级覆盖。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [START](validation-heal-operations.md#START) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [UPGRADE](validation-heal-operations.md#UPGRADE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [STATUS](validation-heal-operations.md#STATUS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 固定前后两个明确兼容候选，覆盖滚动升级、混合 peer、回滚与重启
- 能力不足必须显示 partial/unsupported 而非完整成功；按格式兼容契约保留责任，旧版本不能误退休新版本记录。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="F09"></a>

## F09: heal.dedicated-host-forced-restart

Topologies: H6. Branches: dedicated-vm-forced-stop, dedicated-host-power-cycle.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_H6_HOST`): 需要 H6 专用主机、平台强制停启权限、带外恢复和 boot_id/平台事件证据；共享 Heal 节点不能证明真实电源循环。

1. 先取得 H6 专项资源 ID、独占证明和带外恢复批准；普通 fixed fleet 禁止执行本例。
2. 持续限量写入并在 replacement 部分重建窗口，记录已成功 ACK/结果未知写集合与最近持久化证据。
3. HOST_POWER 使用批准的平台命令强制停启，保留平台活动 ID、时间线和 boot_id；VM stop 与真实电源断电分别命名实例。
4. 恢复后按原盘/根重启并解析未知写；全版本/片和持久责任验收，不把 SIGKILL 或软件 reboot 当作此项证据。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [REPLACE](validation-heal-operations.md#REPLACE) → [WINDOW](validation-heal-operations.md#WINDOW) → [HOST_POWER](validation-heal-operations.md#HOST_POWER) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 经专项授权在部分修复/持续写入时强制重启独占测试 VM/主机
- 保留平台事件与 ACK ledger；验证落盘和恢复，不把 SIGKILL 当断电；VM 强制停止也不等同于物理控制器断电。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G01"></a>

## G01: heal.race-unversioned-overwrite

Topologies: H1,H2. Branches: overwrite-vs-deep, overwrite-vs-read-repair, native-commit-cleanup.

1. 同 key 串行 nonce 的覆盖客户端与 heal 客户端独立；WORKLOAD 每次记录 request-start/end、ACK、payload hash，不凭跨机墙钟判最后版本。
2. 旧对象缺片/bitflip 后两端同时运行，WINDOW 证明实际交叠；每个 worker 的同 key PUT 必须串行，避免期望值本身无序。
3. 停止写入后解析全部未知 ACK，按最后可证明提交核对最新 GET 与 data_dir；不能回滚旧内容。
4. 运行 NATIVE commit/cleanup 屏障实例；真实竞争没命中精确窗口只算黑盒实例，记录 no-lock/锁重验日志。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 同 key 连续覆盖与 heal 同时推进，包含提交尾部清理窗口
- 按可证明的提交顺序验证最终最后一次成功写入；不能回滚到旧 data_dir，未确认超时写入单独解析。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G02"></a>

## G02: heal.race-versioned-put

Topologies: H1,H2. Branches: new-version-vs-historical-heal, read-all-versions-during-heal, native-version-fence.

1. DV 历史版本缺片时，前台串行写入独立 payload 的新版本，保存每个成功 versionId。
2. START deep 与 WORKLOAD 交叠；读取端始终指定 versionId 验证新旧内容，禁止用 current GET 代替。
3. 静默后按完整 ACK ledger 重建期望版本集合，最新值根据串行提交顺序判断；全量 PHYSICAL 检查不同 data_dir 无混片。
4. 精确版本提交屏障运行 NATIVE；任何未解析超时写/缺失 ACK 阻止最终通过。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 修复历史版本时创建新版本并重复读取各 versionId
- 新旧版本不混片、不串内容；已返回成功的每个版本都保留，最新值按确定的先后关系验收。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G03"></a>

## G03: heal.race-version-delete

Topologies: H1,H2. Branches: delete-old-version-vs-deep, delete-current-version-vs-deep, native-delete-fence.

1. 选 DV 的精确 versionId 制造可恢复缺片；前台删除请求明确带该 ID，不是只创建 marker。
2. WINDOW 内并发 delete_object 与 deep heal，保存 DELETE 成功回执和 heal scope/identity。
3. 终态按精确 version GET 应不存在，LIST versions 不得复活；其他所有版本内容保持。
4. 运行 NATIVE 删除/修复提交交错点；清理旧片仅作为最终物理证据，不能先用备份复活已删版本。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 对损坏的精确历史 versionId 发 DELETE，与 deep heal 竞争
- 已确认删除的版本不能被复活，其他历史版本不受影响；原生临界点测试与真机竞争分别留证。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G04"></a>

## G04: heal.race-markers-and-batch-delete

Topologies: H1,H2. Branches: create-marker, delete-marker, batch-version-delete, unversioned-delete, native-marker-fence.

1. 分别建立版本化和非版本化桶；marker创建/删除、批量带versionId删除和无版本DELETE分开运行，不混淆接口语义。
2. 在 xl.meta 少数缺失下启动 heal，同时提交冻结的删除列表；delete_objects 必须逐项检查 Deleted/Errors，HTTP200不代表全成功。
3. 按逐项 ACK 计算最终数据版本、marker 和最新值集合；精确 GET验证不存在项和存留项。
4. native-marker-fence 独立取证；恢复故障不得把已确认删除的旧 metadata 覆盖回来。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 与创建/删除 marker、批量版本删除和无版本删除分别交错
- 按逐项 DELETE 回执核对版本/marker 集合，无复活、误删或遗漏批量错误。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G05"></a>

## G05: heal.race-multipart-complete-abort

Topologies: H1,H2. Branches: upload-continue, complete, abort, native-mpu-fence.

1. DM 同时准备已提交对象与两个可识别的 pending UploadId，各自 part/ETag 独立。
2. 已提交对象缺片并 heal；并发给 upload-A 续传并 Complete，upload-B Abort，WINDOW 保存实际交叠。
3. Complete 返回版本只出现一次，完整内容为按PartNumber排序的ACK part；Abort 的 UploadId 不再可list_parts，不能误伤另一个upload或已提交对象。
4. 核对所有 MPU 列表和物理目录；原生提交/取消屏障另用 NATIVE，不允许仅发出并发请求即声称命中。

Operations: [SEED_DM](validation-heal-operations.md#SEED_DM) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- heal 期间对不同已知 UploadId 分别续传、Complete、Abort
- 成功 Complete 内容只出现一次；Abort 不破坏已提交对象或别的 upload；未完成 part 不被伪造为完整对象。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G06"></a>

## G06: heal.race-copy-and-metadata-update

Topologies: H1,H2. Branches: copy-source, copy-destination, replace-metadata, update-tags, native-copy-fence.

1. DS 源/目标使用不同内容和 metadata，copy 请求 CopySource 使用结构化 Bucket/Key/VersionId，不拼路径字符串。
2. 源或目标缺片并 heal，同窗口 COPY、MetadataDirective=REPLACE 的自拷贝、put_object_tagging 分别运行。
3. 保存对应提交ETag/versionId及更新顺序，验证最终内容与该提交 metadata/tags 组合一致；旧 read-repair 必须重验代次。
4. 检查源目标和control均无交叉覆盖；NATIVE 记录锁升级/陈旧提交拒绝的精确窗口。

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- COPY 源/目标和标签/元数据更新与修复交错
- 最终对象内容与对应提交的 metadata 一致，读修复发现新代次应重验/放弃陈旧提交而非覆盖。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G07"></a>

## G07: heal.race-ilm-expiry-and-transition

Topologies: H2. Branches: current-expiry, noncurrent-expiry, transition, restore, native-ilm-fence.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_ILM_TIER_WINDOW`): 需要隔离 ILM/tier 租户、真实 Expire/Transition/Restore 窗口和远端版本映射；共享 ILM 环境不能安全制造交叠窗口。

1. 用现有 ILM 专用远端 tier 能力建立本例桶和规则；记录真实到期窗口/远端version映射，不在共享桶上改规则。
2. 在实际出现 Expire/Transition/Restore 进度时启动缺片 heal，WINDOW 必须从日志/状态证明交叠；提前完成只能记 NOT_OBSERVED。
3. 过期 ACK/ILM 删除证明决定哪些版本应不存在，禁止 heal 复活；迁移对象通过真实远端 GET/restore 验证原版本内容。
4. 本地物理与远端引用双重校验；源端 GET 成功不足，清理只删除本例规则/远端数据。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DE](validation-heal-operations.md#SEED_DE) → [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [ILM](validation-heal-operations.md#ILM) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 分别与当前/历史版本过期、远端 transition/restore 同窗口执行
- 已授权过期不被恢复；迁移状态与远端版本引用正确；只有实际观察到交叠才计并发覆盖，tier 未配置需明确阻断对应实例。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="G08"></a>

## G08: heal.race-replication-resync

Topologies: H2. Branches: replicate-put, replicate-delete, resync, native-replication-fence.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_ISOLATED_REPLICATION_PAIR`): 需要隔离的复制两端、resync 控制、跨端版本映射和 Heal/resync 实际交叠窗口；当前固定 fleet 没有独占复制对。

1. 准备两个隔离复制端点/本例桶、明确源目标版本映射和同步规则；先跑一份健康复制控制。
2. 源端缺片 heal 同时提交 PUT/DELETE 并启动指定 resync；保存两侧日志与跨端版本映射，证明操作时间交叠。
3. 分别从两端全量列举版本、marker、内容并比对复制契约；源端正确但远端缺失/无限重试必须失败。
4. 恢复临时规则/凭据，确认无无限循环/重复传播；精确交错仅由匹配 NATIVE 证据补充。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [REPLICATION](validation-heal-operations.md#REPLICATION) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [LOGS](validation-heal-operations.md#LOGS) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 在隔离复制两端与 heal 同时运行新增/删除/重同步
- 对端版本、删除语义及源数据最终一致，无无限回环或重复传播；不得依赖仅源端 GET 宣称复制正确。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="H01"></a>

## H01: heal.with-expansion-rebalance

Topologies: H5. Branches: expand-while-data-heal, expand-while-parity-heal, versioned-rebalance.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_EXPANSION_REBALANCE`): 需要 H5 pool 扩容、rebalance 和 Heal 同时活动的完整拓扑，并逐对象保存 pool/set 迁移证据；当前 auto-testing 未绑定扩容前后布局与专用操作窗口。

1. 使用已批准 H5，先在源 pool 写 DB/DV 并以 FORMAT/PHYSICAL 证明位置；新 pool 为空且未纳入旧选择器。
2. 目标 data/parity 缺失或bitflip后按 POOL 手册扩容重启，确认新拓扑；启动 rebalance 与 heal。
3. WINDOW 保存两任务活动证据，逐对象跟踪源/目标 pool/set/版本身份，不能只看全局数量。
4. 两任务均正常完成后物理证明目标权威副本，VERIFY全版本，保留scanner backlog的持久提交/退休证据。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [POOL](validation-heal-operations.md#POOL) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 先证明源 pool 持有夹具，再扩容并与修复同时 rebalance
- 修复目标不得指向错误 pool/set；新旧数据完整、移动终态正确，保留 scanner backlog 提交权威检查。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="H02"></a>

## H02: heal.with-rebalance-stop-resume

Topologies: H5. Branches: rebalance-stop, rebalance-resume, rebalance-service-restart.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_REBALANCE_LIFECYCLE`): 需要可观测的 H5 rebalance、停止/恢复/服务重启控制及独立 task 身份；当前 auto-testing 未绑定这两类任务的独立身份和操作窗口。

1. H5 中使用足够对象使移动可观测，缺片 heal 与 rebalance 同时活动；记录两类独立 task ID。
2. 只停止 rebalance，观察 heal 不被误取消；然后恢复 rebalance，重启实例仅重启本例 unit。
3. 持续唯一 key 写入并保存 ACK；停止/恢复前后物理和任务状态要连续，不用新数据根替代恢复。
4. 最终两类任务各自满足完成契约、对象/版本及移动目标正确，重复停止不影响新代次。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [POOL](validation-heal-operations.md#POOL) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- heal 活动期间 stop/restart/resume rebalance
- 两类任务状态和身份独立；停止/恢复移动不误终止修复或丢失对象，恢复后两者均满足各自契约。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="H03"></a>

## H03: heal.with-decommission

Topologies: H5. Branches: heal-decommission-source, heal-survivor-target, versioned-multipart.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_DECOMMISSION`): 需要 H5 pool decommission、survivor 目标、写入限制和移动 ACK/物理权威证据；当前 auto-testing 未绑定本例退役前后拓扑和恢复方案。

1. 先证实夹具位于待退役源，另写 survivor control；分别在源和目标制造在quorum内可恢复故障。
2. 按既有 Pool 行为启动退役与 heal；退役期间按当前与 MinIO 一致的写入限制检查拒绝，不能把拒绝写当数据丢失。
3. 源数据只能在目标提交权威后删除；记录移动 ACK 与 target physical，而不是临时副本数量。
4. 退役完成后停止/移除源，只访问 survivor 全版本/MPU/内容；若还依赖源兜底必失败。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [POOL](validation-heal-operations.md#POOL) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 分别在待退役源和存活目标制造可恢复缺片，再执行退役
- 不得提前删源或以临时副本冒充迁移成功；按 Pool 既定写入限制验收；只剩 survivor 后仍完整可读且有物理证明。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="H04"></a>

## H04: heal.with-decommission-cancel-clear

Topologies: H5. Branches: cancel-during-heal, clear-after-inflight-exit, resume-new-generation, capacity-lock-retry-window.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_DECOMMISSION_CANCEL`): 需要退役取消/清理/新代次恢复以及 #2423 所需容量锁重试窗口；普通取消结果不能替代这个临界点。

1. 在移动和 heal 都有部分进度时取消退役；保存取消持久化记录、通知和在途退出顺序。
2. 确认已退出后 Clear，再恢复/新建退役；旧 token/代次不得误取消新任务，持续 ACK ledger 不能丢。
3. 容量锁超时实例需要 LOCK_WINDOW 或匹配 NATIVE 屏障，实际见证 #2423 所涉重试路径；普通取消成功不能代替。
4. 两流程收敛后 VERIFY/PHYSICAL；没有命中锁窗口单列 NOT_OBSERVED，不声称覆盖 #2423 的临界点。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [POOL](validation-heal-operations.md#POOL) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [LOCK_WINDOW](validation-heal-operations.md#LOCK_WINDOW) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- heal/移动争用中取消退役、Clear、恢复，并覆盖容量锁超时恢复
- 保持持久化后通知及在途退出顺序；不误取消新代次；对 #2423 的重试窗口单独注明真机是否实际触发，不只看最终绿色。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="H05"></a>

## H05: heal.after-retired-pool-removal

Topologies: H5. Branches: survivor-missing-part, survivor-missing-metadata, retired-source-offline.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_RETIRED_POOL_REMOVAL`): 需要完整迁移后移除 retired pool、源保持离线和 survivor-only 访问；当前 auto-testing 未提供本例源持续离线和 survivor-only 的绑定证据。

1. 依既有 Pool 全量迁移并完成退休配置移除，停止源服务，证明入口仅绑定 survivor；保留源不可达证据。
2. 只在 survivor 的选定对象/版本制造缺片或metadata缺失；START root或正确pool/set deep。
3. 记录 source authority 选择，不能向已退休 pool 写入，也不能无权威覆盖/错误回退（#2422）。
4. 检查survivor实际补片、全部版本/marker和源根未变化；在源持续停机情况下全量 VERIFY。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [POOL](validation-heal-operations.md#POOL) → [PROCESS](validation-heal-operations.md#PROCESS) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [LOGS](validation-heal-operations.md#LOGS)

Source assertions:

- 源 pool 完全退役移除并停机后，只在 survivor 上触发修复
- 不再向已移除源写入或依赖其兜底；来源权威和版本正确；#2422 路径不能出现无权威覆盖或错误回退。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="H06"></a>

## H06: heal.multi-pool-target-isolation

Topologies: H5. Branches: selected-pool, selected-set, unselected-control-fault.

1. 在两个以上 pool/set分别放不同对象，用实际layout确认归属；健康control与控制故障分开登记。
2. 只选一个pool/set START；其他pool保留缺片控制，不先GET它们。
3. PHYSICAL先确认选中目标补齐，未选故障原样保留；任务、容量、配额与drive身份都指向正确目标。
4. 取证后才恢复控制故障和VERIFY；对象在其他pool可读不能证明所选盘完成修复。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 仅破坏选定 pool/set，并在另一 pool 保留不同对象与控制故障
- 选择器、任务、容量/配额和目标 drive 一致；未选 pool 不受错误修复，跨 pool 存在不等于该目标已修复。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="H07"></a>

## H07: heal.ec84-multi-set-restart

Topologies: H3,H4. Branches: ec84-one-set-restart, ec84-one-set-crash, ec84-two-sets-restart, ec84-two-sets-crash.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_EC84_MULTI_SET_TOPOLOGY`): 需要 H3 的 12 个独立盘和 H4 的至少 24 个独立 endpoint，并按 set 分别记录重启/崩溃窗口；当前 H4 资源不可用。

1. 分别绑定 H3 12盘和 H4 至少24盘，FORMAT必须证实每set为8data+4parity；同一物理设备多个目录不能算独立故障盘。
2. 每个set都准备DB/DV/DX并证明对象归属，REPLACE选定目标，在目标仅部分重建时优雅重启或SIGKILL。
3. 保存实际中断窗口、盘代次、每set已修/未修对象，不能跨set合并片数。
4. 每个set单独完成物理与全版本校验；四盘pilot不进入该项分子，native EC8+4回归作为独立补证。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [REPLACE](validation-heal-operations.md#REPLACE) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 固定 EC8+4 单 set 与至少两 set，部分重建时重启/崩溃目标
- 每个 set 的故障覆盖和重建分别证明，不跨 set 拼 shard；必须满足真实盘数及映射，不能用 4 盘用例替代 EC8+4。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="I01"></a>

## I01: heal.slow-io-and-eio

Topologies: H1,H2. Branches: bounded-latency, short-read, eio.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_FAULT_IO`): 需要专用 faultfs 或故障设备、可恢复的 slow/short-read/EIO 注入和 namespace 隔离；共享盘模拟会污染其他运行。

1. IO_FAULT 只在预分配专用故障设备或受限faultfs，冻结延迟/错误率、IO范围、最长窗口和原状恢复命令。
2. 在服务相同namespace/UID先证明迟延/短读/EIO真实生效，再启动目标缺片heal；不得让dd填满共享盘。
3. 错误窗口记录重试、CPU、队列和物理片，不能提前reported healthy；暂态与永久错误分开记录。
4. 撤销故障后重新观察责任完成并VERIFY；故障层证据缺失算INFRA_ERROR，不把模拟EIO说成真实介质损坏。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DX](validation-heal-operations.md#SEED_DX) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [METRICS](validation-heal-operations.md#METRICS) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 在独占故障层注入有界延迟、短读、EIO，解除后观察恢复
- 暂态与永久错误分开，重试有界且无忙循环；解除故障后实际修复，期间不得提前标健康。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="I02"></a>

## I02: heal.enospc-inodes-and-quota

Topologies: H1,H2. Branches: bounded-volume-enospc, bounded-inode-exhaustion, bucket-quota.

1. IO_FAULT使用有容量上限的独立loop/fault卷，预注册字节/inode限制及本例桶配额，不改变共享XFS全局配额。
2. 先证明服务实际写返回ENOSPC或quota错误；准备缺片目标和DM写入，再触发heal/PUT，逐次保留ACK和错误。
3. 不可写期间不得损坏健康来源、虚报补片或释放仍需容量；元数据/part双重快照。
4. 释放本例占用或提高已批准限额后续修，VERIFY全内容和目标片；共享盘可用空间保持安全余量。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [QUOTA](validation-heal-operations.md#QUOTA) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 限制专用测试卷空间/inode，并参数化本次桶配额
- 仅填专用受限卷、不耗尽共享真实盘；失败不破坏好副本或释放尚需容量，释放空间后正确续修。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="I03"></a>

## I03: heal.network-partition-and-lock-quorum

Topologies: H2. Branches: minority-partition, read-boundary, write-boundary, lock-boundary, rejoin.

1. H2 先冻结实际节点/slot/对象R/W和锁quorum，NETWORK规则只匹配本例服务端口并带run ID、超时恢复。
2. 构造少数派及边界分区，双端探针证实隔离；SSH、跳板机和其他服务端口保持连通。
3. 双方执行带nonce的PUT/GET/heal，少数派不能承诺不安全提交；BACKGROUND必须暴露missing peer/partial coverage。
4. 解除规则并检查规则计数归零，按ACK因果对账全版本/内容及目标片，无分叉、旧代覆盖或丢失责任。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [NETWORK](validation-heal-operations.md#NETWORK) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [METRICS](validation-heal-operations.md#METRICS) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 仅隔离测试服务节点间流量，覆盖少数派、读/写/锁 quorum 边界及恢复
- 少数派不得承诺无安全性的修复或写入；cluster coverage 不完整不能显示全局健康；恢复无分叉、跨代覆盖。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="I04"></a>

## I04: heal.queue-pressure-and-admission

Topologies: H1,H2. Branches: full-queue, dropped-admission, merged-duplicates, cross-bucket-fairness.

1. 专用服务冻结队列容量、并发、最大请求数和时限；DX与多桶夹具通过WORKLOAD施加有上限的Admin/read-repair请求。
2. 分别记录Full/Dropped/Merged的真实返回及任务ID，重复token/合并不按独立修复数计分。
3. 持久责任不能因入队失败被退休；记录journal、队列深度、等待时延和资源曲线，超过预算立即停注入。
4. 停止负载后在deadline内收敛，所有可修目标真实补齐；不能只以CLI exit0或全局idle通过。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [CONFIG](validation-heal-operations.md#CONFIG) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [METRICS](validation-heal-operations.md#METRICS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 多来源请求冲击有限队列，混合重复任务、不同优先级和多个桶
- Full/Dropped/Merged 语义与实际任务一致；丢弃不能丢 durable 责任；在预先声明负载窗口内无无限堆积或健康误报。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="I05"></a>

## I05: heal.lock-timeout-and-caller-abort

Topologies: H1,H2. Branches: object-lock-timeout, caller-disconnect, stale-read-repair, native-lock-owner-fence.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`NATIVE_LOCK_OWNER_ABORT_BARRIER`): 需要真实 lock owner、超时、调用方断开和 stale read-repair 的服务端所有权/提交顺序；黑盒请求耗时只能证明 TCP 断开，不能证明锁提交屏障，候选包没有 reviewed e2e-test-hooks。

1. LOCK_WINDOW用本例慢IO/受控原生锁屏障持有目标对象锁，先证明真实争用超过预注册预算。
2. 同时heal/覆盖或删除；调用方断开实例仅断本次TCP连接，不杀服务器或关闭跳板机；记录请求开始/断开/服务在途结束。
3. 锁释放后观察取消/超时不提前释放所有权、不重复提交，陈旧read-repair必须重验版本。
4. 核对所有ACK和最终代次，检查锁/FD无泄漏；黑盒不能证明锁所有权时以NATIVE窗口独立补证，不能猜命中。

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [LOCK_WINDOW](validation-heal-operations.md#LOCK_WINDOW) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [METRICS](validation-heal-operations.md#METRICS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- 真实对象锁争用超过预算及调用方断开，再释放锁恢复
- 取消/超时不得提前释放在途所有权或重复提交；陈旧 read-repair 必须重验代次；恢复后无泄漏和错误覆盖。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="I06"></a>

## I06: heal.kms-and-tier-outage

Topologies: H2. Branches: kms-unavailable, kms-denied, tier-unavailable, tier-version-unavailable.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_KMS_TIER_TENANT`): 需要专用 KMS/tier 租户、依赖网络隔离和安全的 key/tier 版本证据；不能在共享依赖上全局断网。

1. 先在专用KMS/tier租户验证DE/DV夹具可正常解密/restore，保存非敏感key/tier版本标识。
2. DEPENDENCY仅断本测试客户端/服务到该依赖的专用namespace路由，或撤销本例租户权限；不全局停共享KMS。
3. 发GET/restore/deep heal并观察责任，依赖错误不能判对象不存在、可remove、已修好；密钥不进入错误日志。
4. 恢复依赖和权限后原版本可解密/恢复，物理/责任完整；远端对象不存在与远端暂不可达分别判定。

Operations: [SEED_DE](validation-heal-operations.md#SEED_DE) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [NETWORK](validation-heal-operations.md#NETWORK) → [S3](validation-heal-operations.md#S3) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 仅让已分配测试 KMS/远端 tier 暂不可用，持续查询与修复
- 依赖故障不得解释为对象不存在/可删/已修复，不暴露密钥；恢复后原版本可解密或恢复，责任未丢失。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="J01"></a>

## J01: heal.status-partial-disabled-unknown

Topologies: H1,H2. Branches: enabled-idle, disabled, uninitialized, missing-peer, recovered-complete.

1. 在独立服务配置分别观察 enabled无任务、HEAL禁用、初始化未完成，保留实际启动配置及时间线。
2. H2 PROCESS停一个peer并跨节点查询BACKGROUND/REPLACEMENT；完整coverage必须含expected/responded/unknown明细。
3. disabled/uninitialized/partial/degraded不能被rc或聚合器解释成全局完成；收集rc原输出与raw Admin逐字段对照。
4. 恢复所有peer后才允许完整idle，依然用PHYSICAL/manifest核对无遗留责任；未观测初始化窗口单独记NOT_OBSERVED。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [PROCESS](validation-heal-operations.md#PROCESS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [REPLACEMENT](validation-heal-operations.md#REPLACEMENT) → [RC_STATUS](validation-heal-operations.md#RC_STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 分别检查 enabled/disabled/uninitialized、全体可达及缺失 peer 的状态
- idle 只在覆盖完整且真实无活动时成立；partial/unknown/degraded/disabled 不被客户端转换为全部完成。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="J02"></a>

## J02: heal.outcome-counters-and-error-truth

Topologies: H1,H2. Branches: mixed-healthy-repaired-failed, dry-run, deferred, readonly-terminal-failure, unknown-negative.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`EVIDENCE_MIXED_OUTCOME_ORCHESTRATION`): 已有 mixed healthy/repaired/failed、独立 dry-run、依赖 deferral、真实 SDK legacy Unknown、原始全副本损坏元数据 Unknown、只读永久失败/新 token 补偿及版本级 grace Deferred/新 token 清理执行流程，均按逐对象 manifest/attemptFailures 对账；仍需完整串行 live 证据及独立验收，故保持禁止默认 dispatch 和 canonical admission；本地检查不能作为完整 PASS。

1. 同批独立key建立健康、可恢复缺片、低于R的不可恢复对象；deferred由冻结的锁/依赖故障产生，dry-run另建任务。
2. 保存逐对象物理基线和期望disposition，对结果的execution/coverage/counters/object identities做逐项对账。
3. unknown/skipped/completed_with_errors不算修复成功，legacy objectsHealed与canonical healed分开解释；attemptFailures不得被隐藏。
4. 使用独立完整manifest弥补有界outcome窗口，保持不可修对象的正确失败记录；恢复后另任务修复不覆盖原结果。
5. grace 分支先用 SDK PUT 建立两个真实版本并验证完整分片；保存一个成员的原对象树后，SDK DELETE 精确历史版本，逐盘验证仅当前版本存在且其逻辑元数据、数据片及控制对象未变。只向原成员回放原对象树；实际候选进程固定 grace=120 秒，不修改元数据年龄或时钟。原任务须在窗口内完成，精确报告旧版 deferred、当前版 verified_healthy 及 retryNotBefore；自然等待过期后原 token 仍不变，新 token 才能清理旧版。清理后先核对全部物理字节等于 DELETE 后权威现场，再跨节点 GET/404 和复查旧 token，最后才做 fixture finalization。错过窗口保留 NOT_OBSERVED；异常保留现场并标 DIRTY；救援仅恢复 DELETE 后权威树，不复活已删除版本。
6. Manual diagnostic entry: explicitly select heal.outcome-counters-and-error-truth through Functional Chain with the frozen candidate and candidate-bound manager/inspector tools. The default catalog policy stays evidence-blocked; even all passing diagnostic branches retain complete=false, canonicalCasePassClaimed=false and a blocking gate verdict until independent acceptance.
7. For a pre-replay public-manifest refusal, recover only after the complete SDK request/ACK ledger proves all writes acknowledged and no DELETE or replay occurred. Bind all four original physical members and the reserved owner, stop the exact candidate, then preserve all roots and state using durable same-filesystem non-overwriting rename. Partial archive retries require the same durable inventory; missing roots or drift retain DIRTY. Isolation is not product repair or restoration and never rewrites the failed case.
8. Manual J02 candidate admission is limited to the original 072b57b37559fbe626aeda8abf61617fdcbdd926 and corrected 1c7fc818dd858247c45b39542abd8291b1e6e31b sources. Each execution requires the matching immutable package and independently audited inspector/manager bundle. A corrected-candidate run is new evidence; it cannot replace an original failure. The full exact-version, physical, timing, retry and responsibility oracles remain unchanged, and all passing diagnostics still require independent canonical acceptance.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [LOCK_WINDOW](validation-heal-operations.md#LOCK_WINDOW) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 同批次混合健康、可修、不可修、deferred、dry-run 对象
- execution/coverage/disposition/计数和逐对象物理结果相符；unknown、skipped 或 completed_with_errors 不能算修复成功。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="J03"></a>

## J03: heal.incremental-cursor-and-truncation

Topologies: H1,H2. Branches: within-window-cursor, past-retention-gap, cross-node-cursor, terminal-truncation.

1. DX至少257个目标，超过128诊断窗口；边启动recursive heal边以sinceSeq查询，每次请求/nextSeq/minSeq/items全部保存。
2. 单端连续读取应无重复倒退；故意使用过旧cursor应明示truncated/gap；跨节点查询按任务/epoch而非全局数字比较。
3. 任务终态也读取旧cursor，验证有界列表不被当完整清单；所有错误与缺口原样进入报告。
4. 最终用全量独立S3/physical manifest与canonical counters验收覆盖；若任务太快仍可做终态截断，但不能声称观察了运行中游标。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [CURSOR](validation-heal-operations.md#CURSOR) → [STATUS](validation-heal-operations.md#STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 生成超过结果保留上限的任务，分页轮询 sinceSeq，并跨节点/终态读取
- nextSeq/minSeq 单调且 gap/truncation 明示；有界 items 不是完整责任清单，最终须用独立 manifest/计数补证，不静默丢错误。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="J04"></a>

## J04: heal.repeat-idempotency

Topologies: H1,H2. Branches: normal-repeat, deep-repeat, repeat-after-restart, versioned-repeat.

1. DB/DV缺片先修复；versioned 的整份 xl.meta 缺失允许所有 identity repaired，或至少一个 repaired 加其余正健康态；marker 只能为 repaired/metadata_healthy，counters.healed 与 repaired 数量一致、counters.unchanged 与其余数量一致，unknown/skipped/failed=0。完整保存 post-heal S3/physical 作为第二轮基线，不使用最初故障前 xl.meta 要求内部字节完全相同。
2. 再次 normal/deep：normal 只接受 metadata_healthy，deep 才要求 verified_healthy；两者均 healed=0、unknown/skipped/failed=0，不得新增 repaired，所有原版本/marker 身份与数量一致。
3. PROCESS原unit重启后再重复；检查新任务有独立token但不产生新对象版本、无限责任或重复修复计数。
4. 允许经格式证据证明的内部维护记录变化，用户内容/引用不变，目标片摘要稳定。

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [PROCESS](validation-heal-operations.md#PROCESS)

Source assertions:

- 第一次修复后重复 normal/deep、重启再检查
- versioned 首次允许所有 identity repaired 或至少一个 repaired 加其余正健康态，marker 仅 repaired/metadata_healthy，计数必须与 receipt 集合一致；normal repeat 接受 `metadata_healthy`，deep repeat 要求 `verified_healthy`，repeat marker 为 `metadata_healthy` 且 healed=0；unknown/skipped/failed 不得成为 PASS。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="J05"></a>

## J05: heal.long-soak

Topologies: H2,H5. Branches: 24h, 72h.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_LONG_SOAK_LANE`): 必须在独占 H2/H5 lane 真实运行 24 小时和 72 小时，包含限速、监控、到期回收和完整资源预算；短跑不能替代 soak 证据。

1. 只在专门分配的H2/H5独占lane，冻结完整24h或72h墙钟窗口、循环故障日程、并发/限速/存储上限和到期回收安排。
2. WORKLOAD持续记录逐请求ACK，周期性故障仅使用本run DRIVE_GATE/PROCESS/依赖规则；每轮见证与恢复，不无限加数据。
3. 持续METRICS、日志、队列/MRF/checkpoint取样；资源增长、panic、未解释责任达到预注册阈值立即保留失败证据。
4. 到期停止写入/注入，解除全部故障，解析未知ACK后全量VERIFY和PHYSICAL；短跑不能充24h/72h结果。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [PROCESS](validation-heal-operations.md#PROCESS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [METRICS](validation-heal-operations.md#METRICS) → [LOGS](validation-heal-operations.md#LOGS) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 独占环境运行预注册 24h/72h 窗口，混合修复、掉线、写入和版本删除
- 每次 ACK 可追踪；最终全量 manifest 正确，队列/内存/FD/临时空间无无界增长，无后台 panic 或未解释持久责任。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。

<a id="J06"></a>

## J06: heal.performance-abba

Topologies: H2. Branches: abba-repeat-1, abba-repeat-2, abba-repeat-3.

1. 同硬件/候选/rc预注册预热、每轮时长、并发、读写比例、数据量/缓存策略、P95/P99与错误率阈值；禁止结果出来再定门槛。
2. 按A无heal、B有heal、B有heal、A无heal顺序，每轮从相同逻辑内容和队列基线开始；完成至少三组独立ABBA。
3. WORKLOAD记录每请求延迟/成功/错误/超时，METRICS记录CPU/IO/FD及实际修复字节；错误不从总请求分母删除。
4. 报告每轮及配对差异、修复吞吐和GET/PUT各自P95/P99；功能VERIFY必须通过，性能阈值也必须独立满足。

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [PERFORMANCE](validation-heal-operations.md#PERFORMANCE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [METRICS](validation-heal-operations.md#METRICS) → [LOGS](validation-heal-operations.md#LOGS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- 同硬件同候选按 ABBA 运行无 heal 基线和有 heal 负载，重复并固定限速
- 分别报告修复吞吐、GET/PUT P95/P99、错误率和 CPU/IO；阈值事先冻结，不能事后挑轮次或把功能 PASS 充性能验收。

先保存失败现场及物理/状态证据，停止本例写入与在途任务，再按 RECOVER 撤销本例故障。并发版本已变化时禁止盲目恢复旧 xl.meta；恢复不能充作产品修复。未恢复环境标 DIRTY，保留本例备份和原失败结果。

逐项满足设计公共契约、该分支步骤及附加断言后人工评审。缺资源为 BLOCKED，窗口未命中为 NOT_OBSERVED，产品错误为 FAIL，工具/恢复错误为 INFRA_ERROR；未运行不记 PASS。
