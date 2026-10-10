# Heal 真机验证设计与验收契约

## 状态与范围

本规范保留源仓库的 84 个 Heal canonical 用例、301 个操作分支及 532 个适用拓扑分支模板。资源要求、公共通过判据和逐项附加断言必须一起满足。

入口为 `scripts/validation_heal/matrix.py`、[逐例操作卡](validation-heal-cases.md) 和 [执行手册](validation-heal-operations.md)。运行环境是当前 auto-testing 的 Heal 节点；候选及 rc 使用当前 validation lane 的不可变包和实际二进制摘要。源仓库历史 pilot 结果不能计入本次通过数量。

22 个用例已接入自动分发：A01、A03、A04、A07、A08、B05、B07、C01–C05、D01–D03、D05–D07、D10、E01、E02、J04。分片执行器使用实际盘角色、停止服务后的文件故障、原始同角色分片摘要与逐块 HighwayHash 校验。A03 的两个分支只执行 H2，比较协调者、独立 peer 和固定节点入口的任务身份、终态及完整 outcome。A04 保留四个 DB 参数组合及两个 DV 分支，每个拓扑分别创建完整 DB/DV 环境；dry-run 不要求 incarnation，但必须有完整 dry_run_observed 集合且无对象修改。A08 保留五种主体和各自 start/status/stop 操作，使用真正重新计算的过期签名及仅授权 `admin:Heal` 的临时用户，清理后验证凭据失效。其余 62 项保留源 `operator-assisted` 方式。每项的实际执行状态见 [迁移映射](validation-migration-map.md)。方案、计划或完整文件归档均不能代替 live PASS。

H07 的单 set 两个分支只属于 H3，双 set 两个分支只属于 H4；四个不匹配组合记为 `nonApplicableCombinations`。缺少机器属于资源阻塞。

目标不是只证明“坏盘后仍能 GET”，而是分别证明：

1. 故障真实发生在指定对象、版本、set 和目标盘。
2. 客户可见控制接口及期望触发源处理了这份修复责任。
3. 可恢复时确实修好了目标分片；不可恢复时明确失败且不扩大损坏。
4. 对象、历史版本、删除语义、并发成功写入和持久化责任正确。
5. 原始证据完整，临时故障被解除，环境可安全交还。

## 版本与已核实的产品边界

接口参考来自源设计记录的 RustFS 与 rc 提交，保留下面的源码链接用于追溯。每批执行仍以当前 candidate manifest 的精确源码、包、已安装二进制摘要和 auto-testing/rc 摘要为准，不由历史版本的验证结果推断本次候选已通过。

| 已核实的接口/行为 | 对设计的影响 |
| --- | --- |
| 签名 Admin POST `/rustfs/admin/v3/heal/`、`/{bucket}`、`/{bucket}/{prefix}`，token 查询与 forceStop/forceStart | 启动回执仅表示受理；必须查询任务并结合内容与物理证据验收。 |
| POST `/rustfs/admin/v3/background-heal/status`，GET `/rustfs/admin/v4/heal/replacement-recovery` | 后台状态、完整 peer coverage、replacement readiness 与具体任务终态分开判断。 |
| outcome 含 execution、coverage、counters、逐对象 disposition；objects/items 是有界诊断窗口 | finished、队列为 0、HTTP 200 都不是“所有对象已修好”的充分条件。 |
| query 支持 sinceSeq；结果含 nextSeq/minSeq/truncated | 需要游标、截断和缺口负例；被截断的 items 不能用作完整清单。 |
| Admin 保留 nolock wire 字段，但实际强制走正常对象锁路径 | A09 验证其不能绕过锁；不沿用历史架构文档中允许 Admin 绕锁的旧描述。 |
| root heal 优雅重启恢复限于原协调者、原拓扑且记录盘仍可读 | 不预设跨协调者故障转移、记录盘丢失恢复或任意降级兼容；MRF/replacement/root 各按各自责任契约验收。 |
| 参考 rc 提供 heal start/status/stop 及 bucket/prefix/token、normal/deep、dry-run、remove/recreate | pool/set、forceStart、sinceSeq 等未在该 CLI 版本暴露的功能通过签名 Admin 测，不虚构 rc 参数；新批次先检查实际 help/schema。 |

源码：[Admin heal](https://github.com/rustfs/rustfs/blob/940e221988694da33fe005054bb86086bae88cd6/rustfs/src/admin/handlers/heal.rs)、[HealOpts](https://github.com/rustfs/rustfs/blob/940e221988694da33fe005054bb86086bae88cd6/crates/heal-contracts/src/heal_channel.rs)、[outcome/责任回执](https://github.com/rustfs/rustfs/blob/940e221988694da33fe005054bb86086bae88cd6/crates/heal/src/heal/outcome.rs)、[rc 命令](https://github.com/rustfs/cli/blob/204eb5ad85b46a007afd4da538cbeb1cadd867b6/crates/cli/src/commands/admin/heal.rs)。

## 拓扑与故障域

下表是**需求模型，不是已经分配的可用机器清单**。所有真机动作必须经跳板机和固定 fleet lease，不能私自创建/删除 Azure VM、磁盘或网络。已有 example profile 不是运行许可；实现时必须绑定节点、端口、服务、mount UUID、盘数、pool/set/slot 与独占范围，读回实际 EC 元数据确认布局。

| 编号 | 最低形状/用途 | 当前边界 |
| --- | --- | --- |
| H1 | 单节点四独立盘、一个 EC set；基础盘修复 | 可参考 single-4disk；使用独立 heal 服务和每盘 heal-validation 根。需要先绑定空闲资源，不复用现有 Pool 数据目录。 |
| H2 | 四节点各四盘、同一分布式 pool，明确固定 set 划分；跨节点故障/锁/状态 | 可参考 cluster-1pool；必须证明目标 set 跨节点，不能只看全局节点总数。 |
| H3 | 精确 EC8+4 单 set，参考主仓库的 3 节点×4 盘测试形状 | 是测试参数，不是生产部署推荐；旧两节点 Pool 环境不满足此拓扑。 |
| H4 | 精确 EC8+4 至少两个 set，参考 3 节点×8 盘、同 pool | 至少 24 个独立盘端点及正确 set 映射；不得用 4 盘或一个 set 替代。固定资源未分配时为阻塞，不是通过。 |
| H5 | 两个以上 pool；分开登记两节点各自四盘的生命周期形状和跨节点 pool 形状 | 现 lifecycle-a 每台一个独立 pool，无节点级冗余；可验证盘级修复及移动交互，不能证明节点故障容错。对齐主仓库多 pool EC8+4 基准时，另登记其三单机×12 盘/三个 pool 布局，不混用计数。 |
| H6 | 独占 VM/主机与专用故障盘，可做强制重启 | 需要额外运维授权、带外恢复和事件证据；普通故障 lane 不允许自动进入。 |

生产拓扑取舍可参考 [RustFS Linux 部署说明](https://docs.rustfs.com/en/installation/linux)：单机多盘的盘冗余不等于整机故障冗余。本设计的结论只对实际验证的拓扑成立。

每个对象/版本预先记录 N、data/parity、分片索引与 set/盘映射，再从候选实现与元数据确定读取 quorum R、修复写 quorum W 和锁 quorum。测试 R-1/R/R+1 与 W-1/W/W+1 的**合法组合**，不以全局在线盘数或固定“坏一半仍能写”代替计算。跨版本、跨 set 的片不能相加；R 与 W 不一定相同。N=1 无 EC 的能力/不可恢复负例属于额外参数实例，不得宣传可自动找回完全丢失的数据。

## 故障后端与安全门禁

所有注入必须有 machine-readable fault-plan 和 fault-receipt；不存在“命令成功即注入成功”。默认每次只隔离本用例一个已解析目标，再按预注册组合扩展。使用以下分类，报告必须记录实际 faultBackend，不能互相冒充。

| 编号 | 故障类别 | 注入有效性与限制 |
| --- | --- | --- |
| F0 | 无故障、纯接口请求 | 记录稳定基线；仅用于无副作用/负请求验证。 |
| F1 | 指定 part/shard 缺失 | 先校验精确路径和摘要，以同盘隔离备份/原子移位保存，再证明目标缺失；不是物理掉盘。 |
| F2 | 指定字节翻转、截断、校验头损坏 | 在隔离测试对象副本上操作，保存偏移、长度、前后摘要及原始备份；必须有足够未损坏权威。 |
| F3 | 指定 xl.meta/inline/身份/桶元数据缺失或损坏 | 需要与候选格式匹配的只读解析器和允许列表；不得操作 IAM/凭据配置、共享 format 或无关对象。 |
| F4 | 原逻辑盘/服务卷短暂不可达、只读 | 采用仅影响测试服务的受限挂载命名空间或故障层，并实际验证访问失败。不得卸载含其他服务数据的共享 XFS mount；chmod 不能未经验证当作有效故障。 |
| F5 | 慢 I/O、EIO、短读、ENOSPC/inode 故障 | 只在预分配独占测试设备或有容量上限的故障层；不得塞满共享数据盘。明确模拟故障与真实设备故障的区别。 |
| F6 | 测试进程优雅停止、SIGKILL、重启 | 限定已验证服务和 MainPID，证明在部分进度窗口发生；记录 boot/process 身份。SIGKILL 不是断电。 |
| F7 | 节点间测试端口网络故障 | 仅对本服务地址/端口、受控 namespace 或独占环境，保护 SSH/跳板机/管理平面；规则带唯一 ID、超时和幂等恢复。 |
| F8 | 空替换逻辑盘、新 disk UUID | 验证原/新盘和 pool/set/slot；不把移动回备份当重建。真实设备 detach/attach 另需独占资源授权，不修改共享盘。 |
| F9 | checkpoint/commit/锁窗口的确定性故障 | 优先原生 runtime-fence/E2E 屏障；Release 黑盒无法证明精确窗口时，单独列原生证据与真机覆盖缺口，禁止给优化包塞入隐藏测试 hook 冒充同一候选。 |
| F10 | 独占 VM/主机强制重启 | 单独审批的破坏性 lane；有带外恢复和平台时间线。VM forced stop 不证明物理控制器缓存断电语义。 |
| F11 | 队列/请求压力、测试 KMS/tier/配额依赖故障 | 只作用于已分配测试租户/进程，限量限时，禁止全局断开共享 KMS 或远端服务。 |

fault-plan 至少包含 run/case/variant ID、拓扑指纹、对象/版本/incarnation、pool/set/slot、盘 UUID、精确文件允许列表、设备/根目录身份、注入前条件、变更预算、备份摘要、恢复动作和自动解除故障截止时间。路径按目录描述符解析，拒绝链接逃逸、设备漂移、未知文件和自由 shell 插值。

故障解除与数据清理是两件事：网络/只读/进程等临时环境损伤要安全解除；失败数据、分片、checkpoint 和证据应保留，环境标记 DIRTY/QUARANTINED，不能继续下一例。捕获证据后可按已批准流程恢复业务可达性，但不覆盖失败记录或把恢复重跑算首次通过。备份只可用于失败后的救援/重置，不参与修复成功判定。

## 数据夹具与参数维度

| 编号 | 内容 |
| --- | --- |
| DB | 确定性基础 manifest：默认 128 个对象，包含 0/1、4095/4096/4097 字节以及实际 inline、EC stripe/block、multipart 边界±1；按磁盘证据确认形态，不硬编码内部阈值。 |
| DM | 已完成的多 part 对象、非等长 part、跨 part Range；另外创建可追踪 UploadId 的 pending/Complete/Abort 夹具，遵守 S3 最小非末 part 大小限制。 |
| DV | 每 key 至少三个不同内容版本、当前/历史 delete marker、versioning suspended/null、明确删除回执；记录 versionId、Last-Modified、ETag 和版本列表。 |
| DS | 多桶、相邻前缀、多页 key、中文/空格/百分号等编码，metadata/tags/content headers 与隔离桶配置。 |
| DE | 候选明确支持且依赖就绪的 SSE-S3/KMS/C、远端 tier 版本；密钥只由受控凭据渠道提供，禁止进入日志/manifest。 |
| DO | object-lock、retention 和 legal hold 的受保护版本；使用可到期测试策略，防止成功用例因无法清理永久占用环境。 |
| DL | 并发 PUT/GET/HEAD/LIST/COPY/DELETE、版本及 MPU 操作的逐请求 ACK ledger；每 key 用递增 nonce 与确定性内容，保存请求/响应和不确定结果。 |
| DX | 保证可观察部分进度、大于状态窗口（>128 项）的夹具；默认上限在运行计划中冻结，长稳/性能单独配额，不靠无限加数据等到偶然命中。 |

按 case/variant 参数化：normal/deep、data/parity shard、inline/外置 part、单片/多片、原盘/新盘、对象/前缀/root/set、版本/加密、协调者/目标节点、单节点/多节点/多 set/pool。使用预先冻结的 pairwise 组合控制规模；损坏+quorum、旧代次+并发删除、目标重建+进程中断等高风险组合必须显式三维覆盖，不由 pairwise 替代。

候选声明支持的非默认 parity/storage class、服务端压缩 on/off 也应加入 DB/DM/DV 参数；Content-Encoding 响应头不是服务端压缩已被执行的证据。每个实例都从实际元数据确认编码形态。Object Lock 的拒绝删除必须发生于真实保留期内；清理不能绕过 Compliance、修改主机时间或以内部删文件规避限制。Legal hold 仅由本次授权测试角色正常解除；保留期超出 lane 窗口的实例必须预先分配独占 stateful 环境及到期处理方案，未清理环境不能交还 READY。

重试不能隐藏 ACK：PUT/Complete 超时属于结果未知，必须按唯一 key/version/UploadId 查询解析；不能把未收到成功的一次写当丢失，也不能把重试新版本当同一次成功。并发时只根据可证明的 happens-before/提交回执判断“最后一次”，不凭跨节点墙钟排序。

## 公共通过判据（每例均适用）

每行用例的完整规范 = 本节 + 对应拓扑/夹具/故障契约 + 行内操作与附加断言。参数实例独立留证、计分。

1. **预检和基线。** 全部预期 peer/磁盘、candidate/rc/harness 身份一致；预先 GET/HEAD/LIST 完整基线并保存必要的盘上记录。记录扫描/自动 heal 配置与任务归属。
2. **注入和见证。** 以只读物理证据确认故障确实发生，未影响其他对象/服务；注入后不要先 GET 目标而意外触发 read-repair。受理过快或已被其他来源修好，不能冒充本例触发成功。
3. **控制面真相。** 收集每次 rc exitCode、签名 Admin HTTP/body、token、source、scope、outcome 和 peer coverage。区分 repaired、verified_healthy、metadata_healthy、authoritatively_absent、deferred、failed、unknown、cancelled、dry_run_observed；deferred、failed、unknown、cancelled、dry_run_observed 不能作为“修复完成”。`metadata_healthy` 只证明 metadata/identity 健康，不证明 payload integrity；Normal scan 不得冒充 `verified_healthy`，deep payload 证明继续要求 `verified_healthy`。实际 wire 枚举以固定候选为准。
4. **对象正确。** 所有必须存在的对象/版本全量 GET 验证长度/SHA-256/ETag/metadata/tags；HEAD、分页 LIST、历史版本与 marker 语义、已确认删除和并发 ACK 全部核对。允许修复产生合法内部元数据变化，不要求整个 xl.meta 字节不变。
5. **实际补片。** 在静默校验窗口解析目标物理数据与元数据，核对版本/代次/data_dir、part 长度、bitrot 校验与 EC 一致性。对 data/parity 各有独立重建或重新编码对照，或用包含目标片的固定合法解码集合佐证；不能只统计文件、看 mtime 或依赖 GET 自动纠删码兜底。密文/压缩存储层不能直接拿明文摘要比较。格式不支持/HighwayHash 未验证时必须标明缺口，不能声称已完成深度物理验收。
6. **持久化与幂等。** 需重启的用例验证重启后责任和数据；重复 heal 不引入新版本、复活删除或无限增加任务。状态计数按进程/任务/epoch 区分，不能把重置后的全局计数简单判为回退。
7. **完成条件。** 正向修复要有相关责任完成、范围 coverage 完整、对象与物理证据一致。无关的全局 idle、列表项被截断、缺失 peer、unknown/残留待修工作都不能代替证明。预期不可恢复/权限/取消/dry-run 用例按预注册负向契约通过，明确它不是修复成功。
8. **恢复和证据。** 解除临时故障，保存 before/fault/after/restart 四阶段证据，校验全局结果/JUnit/计划/对象数量/证据引用及清理。任何取证缺失、目标误命中、恢复失败或结果结构损坏均阻断该 lane。

针对精确时间窗口，必须观察活动/部分进度或受控屏障，再注入故障；普通重建允许快速完成但必须有实际补片证据。禁止用固定 sleep 或“重跑直到绿”代替见证。建议起始测试预算：接口就绪 90 秒、普通修复 15 分钟、重启恢复 30 分钟、同 epoch 静默窗口 5 秒；这些是待校准的**测试预算，不是产品 SLO**，所有最终预算须在运行前冻结。预期暂态错误只按明确 code 有界重试，每次都留证。

判定类型：产品安全/内容/终态错误为 fail；环境注入/取证工具失败为 infra_error；缺少尚未分配拓扑或辅助能力在部署前阻断。不得把产品 SlowDown 或低 quorum 下的预期错误泛化成 infra_error。必选能力缺失、未知选项、缺失参数实例不能用 unsupported/skipped 充 PASS；不适用项只能在运行前的 applicability manifest 中说明，环境尚未准备好不是“不适用”。

## 84 个基础用例

编号用于设计评审；`heal.*` 为稳定 catalog ID。topology、fixture、fault 中逗号表示必须展开记录的适用实例/后端集合，不表示同时向全部对象注入所有故障。对应的组合、先后和最小 quorum 必须在编译后的 variant plan 中明确，未证明的一项不得计为通过。

| 分类 | 设计条目数 |
| --- | ---: |
| A 控制面、任务与权限契约 | 10 |
| B 磁盘故障、替换与 quorum 边界 | 10 |
| C 分片损坏、元数据与权威选择 | 11 |
| D 对象类型与 S3 语义 | 11 |
| E 触发路径与修复责任 | 6 |
| F 中断、持久化、恢复与升级 | 9 |
| G 与前台和后台操作并发 | 8 |
| H 多 Pool、移动与多 set 隔离 | 7 |
| I 资源、依赖故障与调度 | 6 |
| J 可观测性、幂等、长稳与性能 | 6 |
| 合计 | 84 |

<a id="cases-a"></a>

### A. 控制面、任务与权限契约

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| A01 · `heal.admin-healthy-noop` | P0 | H1,H2 / DB / F0 | 对健康对象分别执行 normal/deep，再重复查询 | 不修改用户数据或版本；Normal 只接受 `metadata_healthy`，Deep 才要求 `verified_healthy`，healthy 与 repaired 分开计数，不能把检查盘数当修复盘数。 |
| A02 · `heal.admin-scope-isolation` | P0 | H1,H2 / DB,DS / F1 | 分别以 root、bucket、prefix、object、pool/set 选择器启动 | 目标范围实际遍历；相邻前缀、其他桶及未选 set 的故障保持原状；按对象清单而非目录数量验收。 |
| A03 · `heal.admin-token-cross-node-status` | P0 | H2 / DB / F1 | 从节点甲启动，经乙及稳定入口按原 token 查询 | 任务身份、scope、终态及核心计数有界收敛；状态缺失/未知不能解释为完成。 |
| A04 · `heal.admin-dry-run` | P0 | H1,H2 / DB,DV / F1,F2 | 对缺片与损坏对象执行 dryRun，参数化 remove/recreate | 有明确 dry_run_observed 诊断；用户对象分片、xl.meta、版本与删除标记不变；后台修复干扰须排除。 |
| A05 · `heal.admin-overlap-and-force-start` | P1 | H1,H2 / DB,DS / F1 | 活动中重发同范围、父子前缀和不相交请求，再 forceStart | 按已固定 overlap policy 验证合并返回原 token 或明确冲突；forceStart 不能泄漏旧任务责任或并发破坏对象。 |
| A06 · `heal.admin-stop-and-stale-token` | P0 | H1,H2 / DB / F1 | 非零修复进度时 stop，稳定后新建任务，再发旧 token 的查询/停止 | 停止需等在途写结束并稳定；新任务身份独立，旧请求不得终止新任务；重复 stop 按公开契约处理。 |
| A07 · `heal.admin-invalid-request` | P0 | H1,H2 / DB / F0 | 发送非法 selector、重复 query、非法布尔/scanMode、冲突 flags、超长 body | 服务端返回已核实的结构化错误；无误启动、范围扩大或持久化副作用；CLI 参数错误不能替代服务端负例。 |
| A08 · `heal.admin-authorization` | P0 | H1,H2 / DB / F0 | 匿名、坏签名、过期签名、无 HealAdminAction 用户与最小授权用户分别请求 | 未授权 start/status/stop 均不能越权或泄漏对象信息；授权用户可完成规定操作，凭据不得写入证据。 |
| A09 · `heal.admin-nolock-safety` | P0 | H1,H2 / DL,DV / F1,F9 | 通过签名 API 传 nolock=true，并与同对象提交制造确定性争用 | 兼容字段不得跳过存储层对象锁；最后成功写入和已确认删除保持正确，原生屏障与真机竞争证据分开。 |
| A10 · `heal.admin-option-contract` | P1 | H1,H2 / DB,DV / F1,F2 | 参数化 recursive、recreate、remove、updateParity、readRepair 与合法/非法 pool/set 组合 | 仅测试已核实的候选语义；normal/deep 与危险 remove 的副作用明确；API 独有选项不得伪称 rc 已覆盖。 |

<a id="cases-b"></a>

### B. 磁盘故障、替换与 quorum 边界

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| B01 · `heal.disk-loss-and-repair` | P0 | H1,H2 / DB / F4 | 让一个已确认 set 成员盘对 heal 服务不可用，持续读取，恢复原盘 | 记录真实故障窗口和源/目标盘身份；在可恢复条件下最终补齐目标分片及完整 manifest，不只验证降级 GET。 |
| B02 · `heal.disk-readonly-and-permission` | P1 | H1,H2 / DB / F4,F5 | 仅对独占测试卷制造只读/拒绝访问，恢复后重试 | 不可写目标不得 reported repaired；错误及待修责任可见；恢复后真实写入目标且不落到系统盘或其他目录。 |
| B03 · `heal.disk-replacement-formatted` | P0 | H1,H2 / DB,DV / F8 | 换入空的新逻辑盘，确认新 disk UUID/正确格式后触发重建 | 新盘与旧盘身份可区分且属于正确 pool/set/slot；逐版本重建完成，不能从隔离备份复制回来冒充 heal。 |
| B04 · `heal.disk-replacement-unformatted` | P1 | H1,H2 / DB / F8 | 引入空未格式化测试盘，并与带异集群格式的盘分别比较 | 按候选支持的初始化契约安全接纳或明确拒绝；不得覆盖外来身份或将未就绪盘标为可写健康。 |
| B05 · `heal.multiple-shard-loss` | P0 | H1,H2 / DB,DV / F1 | 按实际同版本 EC 布局依次缺失 1..M 个 shard，覆盖 data/parity 角色组合 | 每个参数实例单独满足读取与写入 quorum；满足修复条件时全部目标重建，不把跨版本或跨 set 的 shard 合并。 |
| B06 · `heal.read-quorum-boundary` | P0 | H1,H2 / DB / F1 | 保留恰好 R 个同版本权威 shard，测试读取及修复提交，再恢复写 quorum | 读取边界和修复写入边界分别判定；不能因还能 GET 就要求在写 quorum 不足时提交成功。 |
| B07 · `heal.below-read-quorum` | P0 | H1,H2 / DB,DV / F1 | 使权威来源小于 R，执行 GET 与 deep heal，再恢复被隔离源 | 故障期间不得返回伪造的成功内容、空对象或误删记录；明确 unresolved/失败；恢复后原对象和版本完整。 |
| B08 · `heal.below-write-quorum` | P0 | H1,H2 / DB,DL / F4 | 构造读 quorum 尚存而写/锁 quorum 不足，执行 heal 与新 PUT | 不能虚报持久化修复或成功写入；分别记录对象、元数据、分布式锁 quorum；恢复后核对所有已确认写入。 |
| B09 · `heal.node-outage-rejoin` | P0 | H2 / DB,DV,DL / F6 | 停止同一跨节点 set 的一个节点，执行允许的降级 I/O，再重入 | 按实际丢失盘数判定可用性；缺失期间成功提交的对象补齐；节点重新 ready 不等于所有对象已重建。 |
| B10 · `heal.disk-and-node-flapping` | P1 | H2 / DB,DL / F4,F6 | 重复短暂离线/在线并在重建途中再次失联 | 重试与队列有界、身份稳定，无重复任务风暴；最终故障解除后收敛，所有已确认对象保持正确。 |

<a id="cases-c"></a>

### C. 分片损坏、元数据与权威选择

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| C01 · `heal.missing-part-file` | P0 | H1,H2 / DB,DM / F1 | 仅隔离指定对象某个 part/shard，保留 format 与其他元数据，执行 deep heal | 对应数据或校验分片重新生成，part/版本身份准确；不得只看到 xl.meta 重现就通过。 |
| C02 · `heal.data-shard-bitrot` | P0 | H1,H2 / DB,DM / F2 | 在指定 data shard 的已知偏移翻转字节，包含首/中/尾位置 | 故障前后摘要证明注入生效；deep 或 read-repair 发现损坏并修复；独立 bitrot/EC 及 S3 全内容验证通过。 |
| C03 · `heal.parity-shard-bitrot` | P0 | H1,H2 / DB,DM / F2 | 损坏 parity shard，使普通 GET 仍可能成功，再执行 deep heal | 必须识别并重建损坏 parity；只有 GET 正确、无目标分片修复不能通过。 |
| C04 · `heal.truncated-and-zeroed-shard` | P0 | H1,H2 / DB,DM / F2 | 分别截断 shard、置零和破坏校验头，保留正确元数据仲裁来源 | 短读、全零和校验损坏被准确识别；修复不能接受截断内容或错误长度。 |
| C05 · `heal.missing-or-corrupt-xlmeta` | P0 | H1,H2 / DB,DV / F3 | 在少数成员上移走或损坏 xl.meta/inline 内容，保留足够权威来源 | 恢复同一对象的全部应存版本、数据引用与删除标记；不以新建空元数据替代真正修复。 |
| C06 · `heal.stale-version-rejoin` | P0 | H1,H2 / DV,DL / F4 | 盘离线后执行覆盖/版本删除，再让携带旧元数据的盘重入 | 以已确认的新代次/版本为准；旧当前值与已删版本不复活，旧盘被纠正而非成为新的错误权威。 |
| C07 · `heal.conflicting-subquorum-authority` | P0 | H1,H2 / DV / F3,F9 | 用受控原生夹具构造互不构成 quorum 的版本/代次集合，再做对应真机反例 | 禁止把不同版本的 shard 数量并集当成权威；无法证明时安全拒绝且保留可恢复证据，不猜测较新时间戳。 |
| C08 · `heal.metadata-payload-mismatch` | P1 | H1,H2 / DB,DV / F2,F3 | 构造元数据声称某长度/摘要但实际 part 不匹配的少数副本 | 元数据与内容联合判断；不把可解析的 xl.meta 当健康；权威不足时拒绝覆盖正确副本。 |
| C09 · `heal.foreign-format-and-unknown-schema` | P0 | H1,H2 / DB / F3,F8 | 提供外来 disk/set 身份、未来 schema 或不支持的记录格式 | 明确隔离/拒绝且不破坏原记录，不把未知当 missing 后直接重建；解除故障后原数据可用。 |
| C10 · `heal.dangling-and-orphan-safety` | P1 | H1,H2 / DV,DM / F1,F3 | 构造 dangling part、旧 data_dir 与共享历史引用，分别 remove=false/true、宽限期前后检查 | 默认不得破坏仍可恢复数据；显式 remove 只清除具有充分删除证明且过期的本次孤儿，不删共享版本或 MPU。 |
| C11 · `heal.bucket-incarnation-recreation` | P0 | H1,H2 / DV / F4,F9 | 盘离线时完成旧桶合法清空/删除，再同名建桶写入新对象，恢复旧盘与旧任务 | 旧 bucket incarnation 的 checkpoint/MRF/元数据不得污染新桶；旧任务不得删除或恢复新 incarnation 的对象。 |

<a id="cases-d"></a>

### D. 对象类型与 S3 语义

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| D01 · `heal.object-size-boundaries` | P0 | H1,H2 / DB / F1,F2 | 覆盖 0/1 字节、页/inline/EC block 边界±1以及大对象 | 每个实际存储形态均确认后注入；长度、SHA-256、ETag、HEAD/LIST 与边界 Range 正确，无假设阈值。 |
| D02 · `heal.completed-multipart-object` | P0 | H1,H2 / DM / F1,F2 | 修复已完成的不等长多 part 对象，分别损坏首/中/末 part | 完整内容、part 顺序、原版本/ETag/元数据和跨 part Range 不变；multipart ETag 不能当整对象 MD5。 |
| D03 · `heal.historical-versions` | P0 | H1,H2 / DV / F1,F3 | 不同版本分布不同损坏位置，按对象和递归范围修复 | 所有原 versionId 可寻址且内容正确，最新版本身份不漂移，无多余版本或遗漏历史版本。 |
| D04 · `heal.delete-marker-semantics` | P0 | H1,H2 / DV / F3,F4 | 缺失/陈旧 marker 副本后 heal，并按当前值与精确 marker versionId 查询 | 当前 GET/HEAD 与指定 marker 的状态码及身份头符合契约；marker 不复活数据，LIST versions 无重复。 |
| D05 · `heal.versioning-suspended-null` | P1 | H1,H2 / DV / F1,F3 | 在版本开启、暂停、null 版本覆盖之间建夹具再修复 | null 和非 null 版本不混淆，不新增假版本；暂停前历史对象仍准确可读。 |
| D06 · `heal.object-metadata-and-tags` | P1 | H1,H2 / DS / F1,F3 | 包含用户 metadata、Content-Type/Encoding/Disposition、tags 的对象修复 | 对外 metadata、tags 和相关响应头逐项相同；不要求无关内部元数据字节完全一致。 |
| D07 · `heal.range-head-and-conditions` | P1 | H1,H2 / DB,DM / F1,F2 | 修复前后验证 HEAD、合法/越界 Range、If-Match/If-None-Match 及时间条件 | 200/206/304/412/416 等按逐请求契约判断，绝不返回旧内容、错误范围或错误 ETag。 |
| D08 · `heal.encrypted-objects` | P0 | H1,H2 / DE / F1,F2 | 按候选支持的 SSE-S3/SSE-KMS/SSE-C 建加密对象并修复 | 授权解密后原内容及版本一致，物理验证使用正确编码/密文层；密钥不可用时不误删、不降级明文，证据脱敏。 |
| D09 · `heal.object-lock-retention` | P0 | H1,H2 / DO,DV / F1,F3 | 对开启 retention/legal hold 的受保护版本修复，期间测试非法删除 | heal 不绕过保留/hold，也不丢失其元数据；拒绝删除与合法修复均有明确证据。 |
| D10 · `heal.special-keys-and-pagination` | P1 | H1,H2 / DS / F1,F3 | 覆盖中文、空格、百分号、编码斜线、多层前缀和多页对象列表 | 逻辑 key 与物理路径映射准确，不跨桶/前缀；分页无遗漏重复，不能把用户 key 当可执行 shell 或裸文件路径。 |
| D11 · `heal.bucket-metadata-integrity` | P0 | H1,H2 / DS,DV / F3 | 在隔离桶白名单内测试可支持的桶元数据恢复，并核对外部 bucket 配置 | 版本设置、策略/标签/保留配置按契约保持；不得遍历凭据配置或把未知内部记录当可重建数据。 |

<a id="cases-e"></a>

### E. 触发路径与修复责任

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| E01 · `heal.manual-normal-attribution` | P1 | H1,H2 / DB / F1 | 禁止预读目标，保留 format，隔离背景干扰后显式 normal heal | 证明本次 Admin token 实际覆盖目标并修复可由 normal 检出的缺失；不要求 normal 发现所有静默 bitrot。 |
| E02 · `heal.manual-deep-attribution` | P0 | H1,H2 / DB / F2 | 在普通读取仍可成功的 parity 损坏上显式 deep heal | 按 token/对象/版本及物理变更证明 deep 检查和修复，不把后台提前修复算给手工任务。 |
| E03 · `heal.read-repair-attribution` | P0 | H1,H2 / DB,DV / F1,F2 | 不发 Admin heal，对已确认的特定损坏对象发一次可追踪 GET/Range | 读取正确且后续真实补片；若走异步队列需证实 source=read-repair，不能只有 GET 成功。 |
| E04 · `heal.scanner-cold-object-repair` | P0 | H1,H2 / DB / F1,F2 | 不 GET/HEAD 损坏目标、不发手工 heal，仅等待已启用的 scanner 路径 | 冷对象被发现并修复，来源和遍历窗口明确；scanner 周期结束但目标没检查不能通过。 |
| E05 · `heal.mrf-offline-write-replay` | P0 | H2 / DL,DV / F4 | 成员离线时完成有 quorum 的 PUT/DELETE，确认待修责任后重入 | durable MRF 责任持久且身份精确；只在 repaired/verified healthy/authoritatively absent 等已证明结果后释放，成功提交不丢失。 |
| E06 · `heal.replacement-auto-rebuild` | P0 | H1,H2 / DB,DV / F8 | 替换盘接纳后不发 Admin/目标 GET，观察自动 replacement heal | 替换代次、ready/completion 与盘上全部目标版本一致；不能用服务 ready、队列空或计数重置代替完成。 |

<a id="cases-f"></a>

### F. 中断、持久化、恢复与升级

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| F01 · `heal.root-graceful-coordinator-restart` | P0 | H1,H2 / DX / F6 | root heal 非零进度时优雅重启原协调者，保留原拓扑与记录盘 | 在明确支持的边界内恢复原 task ID 和剩余责任/预算，完成后无遗留活动记录；不扩张为协调盘丢失后的故障转移。 |
| F02 · `heal.target-graceful-restart` | P0 | H2 / DB,DV,DX / F6,F8 | 替换目标仅完成部分重建时优雅重启目标节点 | 故障确实发生于部分完成窗口；重启后续建同一目标盘/代次，最终 manifest 与全部目标分片正确。 |
| F03 · `heal.target-process-crash` | P0 | H2 / DB,DV,DX / F6,F8 | 部分重建时 SIGKILL 测试进程，再正常启动 | unclean shutdown 与修复进度有真实证据；已提交对象不丢失，未完成目标不会被当完整；明确不是断电测试。 |
| F04 · `heal.coordinator-process-crash` | P0 | H2 / DB,DX / F6 | 协调者非优雅退出并恢复，分别观察 root、MRF、replacement | 按各机制已核实的持久化边界判断；不得把丢失的易失 token 当成功，需显式记录剩余责任并可安全重扫恢复。 |
| F05 · `heal.full-cluster-restart` | P0 | H2 / DB,DV,DM / F6 | 有未完成修复责任时重启全部测试节点，固定原拓扑/磁盘身份 | 有持久化保证的责任不遗失；全部端点恢复一致，旧版本/marker/MPU 与目标分片完整，重复重启不扩散错误。 |
| F06 · `heal.cancelled-task-restart` | P0 | H1,H2 / DB,DX / F6 | 完成取消及在途退出后重启，随后 forceStart/新任务正常修复 | 已取消的任务不自动复活；旧 checkpoint/token 不能取消或覆盖新任务，旧责任不会误标为修复完成。 |
| F07 · `heal.checkpoint-and-mrf-corruption` | P0 | H1,H2 / DB,DV / F9 | 受控构造 checkpoint/MRF 截断、坏校验、未来 schema、陈旧代次及写入失败 | 已确认持久化责任不得静默删除或伪 ACK；未知记录保留且可诊断；精确崩溃窗口由原生屏障证明，不伪称真机命中。 |
| F08 · `heal.mixed-version-upgrade-rollback` | P1 | H2 / DB,DV / F6,F9 | 固定前后两个明确兼容候选，覆盖滚动升级、混合 peer、回滚与重启 | 能力不足必须显示 partial/unsupported 而非完整成功；按格式兼容契约保留责任，旧版本不能误退休新版本记录。 |
| F09 · `heal.dedicated-host-forced-restart` | P1 | H6 / DB,DV,DX / F10 | 经专项授权在部分修复/持续写入时强制重启独占测试 VM/主机 | 保留平台事件与 ACK ledger；验证落盘和恢复，不把 SIGKILL 当断电；VM 强制停止也不等同于物理控制器断电。 |

<a id="cases-g"></a>

### G. 与前台和后台操作并发

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| G01 · `heal.race-unversioned-overwrite` | P0 | H1,H2 / DL / F1,F2,F9 | 同 key 连续覆盖与 heal 同时推进，包含提交尾部清理窗口 | 按可证明的提交顺序验证最终最后一次成功写入；不能回滚到旧 data_dir，未确认超时写入单独解析。 |
| G02 · `heal.race-versioned-put` | P0 | H1,H2 / DV,DL / F1,F9 | 修复历史版本时创建新版本并重复读取各 versionId | 新旧版本不混片、不串内容；已返回成功的每个版本都保留，最新值按确定的先后关系验收。 |
| G03 · `heal.race-version-delete` | P0 | H1,H2 / DV / F1,F2,F9 | 对损坏的精确历史 versionId 发 DELETE，与 deep heal 竞争 | 已确认删除的版本不能被复活，其他历史版本不受影响；原生临界点测试与真机竞争分别留证。 |
| G04 · `heal.race-markers-and-batch-delete` | P0 | H1,H2 / DV / F3,F9 | 与创建/删除 marker、批量版本删除和无版本删除分别交错 | 按逐项 DELETE 回执核对版本/marker 集合，无复活、误删或遗漏批量错误。 |
| G05 · `heal.race-multipart-complete-abort` | P0 | H1,H2 / DM,DL / F1,F9 | heal 期间对不同已知 UploadId 分别续传、Complete、Abort | 成功 Complete 内容只出现一次；Abort 不破坏已提交对象或别的 upload；未完成 part 不被伪造为完整对象。 |
| G06 · `heal.race-copy-and-metadata-update` | P1 | H1,H2 / DS,DL / F1,F9 | COPY 源/目标和标签/元数据更新与修复交错 | 最终对象内容与对应提交的 metadata 一致，读修复发现新代次应重验/放弃陈旧提交而非覆盖。 |
| G07 · `heal.race-ilm-expiry-and-transition` | P0 | H2 / DV,DE / F1,F9,F11 | 分别与当前/历史版本过期、远端 transition/restore 同窗口执行 | 已授权过期不被恢复；迁移状态与远端版本引用正确；只有实际观察到交叠才计并发覆盖，tier 未配置需明确阻断对应实例。 |
| G08 · `heal.race-replication-resync` | P1 | H2 / DV,DL / F1,F9,F11 | 在隔离复制两端与 heal 同时运行新增/删除/重同步 | 对端版本、删除语义及源数据最终一致，无无限回环或重复传播；不得依赖仅源端 GET 宣称复制正确。 |

<a id="cases-h"></a>

### H. 多 Pool、移动与多 set 隔离

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| H01 · `heal.with-expansion-rebalance` | P0 | H5 / DB,DV / F1,F2 | 先证明源 pool 持有夹具，再扩容并与修复同时 rebalance | 修复目标不得指向错误 pool/set；新旧数据完整、移动终态正确，保留 scanner backlog 提交权威检查。 |
| H02 · `heal.with-rebalance-stop-resume` | P1 | H5 / DB,DL / F1,F6 | heal 活动期间 stop/restart/resume rebalance | 两类任务状态和身份独立；停止/恢复移动不误终止修复或丢失对象，恢复后两者均满足各自契约。 |
| H03 · `heal.with-decommission` | P0 | H5 / DB,DV,DM / F1,F2 | 分别在待退役源和存活目标制造可恢复缺片，再执行退役 | 不得提前删源或以临时副本冒充迁移成功；按 Pool 既定写入限制验收；只剩 survivor 后仍完整可读且有物理证明。 |
| H04 · `heal.with-decommission-cancel-clear` | P0 | H5 / DB,DL / F1,F9 | heal/移动争用中取消退役、Clear、恢复，并覆盖容量锁超时恢复 | 保持持久化后通知及在途退出顺序；不误取消新代次；对 #2423 的重试窗口单独注明真机是否实际触发，不只看最终绿色。 |
| H05 · `heal.after-retired-pool-removal` | P0 | H5 / DB,DV / F1,F3 | 源 pool 完全退役移除并停机后，只在 survivor 上触发修复 | 不再向已移除源写入或依赖其兜底；来源权威和版本正确；#2422 路径不能出现无权威覆盖或错误回退。 |
| H06 · `heal.multi-pool-target-isolation` | P0 | H5 / DB,DV / F1,F4 | 仅破坏选定 pool/set，并在另一 pool 保留不同对象与控制故障 | 选择器、任务、容量/配额和目标 drive 一致；未选 pool 不受错误修复，跨 pool 存在不等于该目标已修复。 |
| H07 · `heal.ec84-multi-set-restart` | P0 | H3,H4 / DB,DV,DX / F6,F8 | 固定 EC8+4 单 set 与至少两 set，部分重建时重启/崩溃目标 | 每个 set 的故障覆盖和重建分别证明，不跨 set 拼 shard；必须满足真实盘数及映射，不能用 4 盘用例替代 EC8+4。 |

<a id="cases-i"></a>

### I. 资源、依赖故障与调度

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| I01 · `heal.slow-io-and-eio` | P1 | H1,H2 / DB,DX / F5 | 在独占故障层注入有界延迟、短读、EIO，解除后观察恢复 | 暂态与永久错误分开，重试有界且无忙循环；解除故障后实际修复，期间不得提前标健康。 |
| I02 · `heal.enospc-inodes-and-quota` | P0 | H1,H2 / DB,DM / F5,F11 | 限制专用测试卷空间/inode，并参数化本次桶配额 | 仅填专用受限卷、不耗尽共享真实盘；失败不破坏好副本或释放尚需容量，释放空间后正确续修。 |
| I03 · `heal.network-partition-and-lock-quorum` | P0 | H2 / DB,DV,DL / F7 | 仅隔离测试服务节点间流量，覆盖少数派、读/写/锁 quorum 边界及恢复 | 少数派不得承诺无安全性的修复或写入；cluster coverage 不完整不能显示全局健康；恢复无分叉、跨代覆盖。 |
| I04 · `heal.queue-pressure-and-admission` | P1 | H1,H2 / DX / F11 | 多来源请求冲击有限队列，混合重复任务、不同优先级和多个桶 | Full/Dropped/Merged 语义与实际任务一致；丢弃不能丢 durable 责任；在预先声明负载窗口内无无限堆积或健康误报。 |
| I05 · `heal.lock-timeout-and-caller-abort` | P0 | H1,H2 / DL,DV / F5,F9 | 真实对象锁争用超过预算及调用方断开，再释放锁恢复 | 取消/超时不得提前释放在途所有权或重复提交；陈旧 read-repair 必须重验代次；恢复后无泄漏和错误覆盖。 |
| I06 · `heal.kms-and-tier-outage` | P1 | H2 / DE,DV / F11 | 仅让已分配测试 KMS/远端 tier 暂不可用，持续查询与修复 | 依赖故障不得解释为对象不存在/可删/已修复，不暴露密钥；恢复后原版本可解密或恢复，责任未丢失。 |

<a id="cases-j"></a>

### J. 可观测性、幂等、长稳与性能

| 编号 / 稳定 ID | P | 拓扑 / 夹具 / 故障 | 操作与触发 | 附加通过判据 |
| --- | --- | --- | --- | --- |
| J01 · `heal.status-partial-disabled-unknown` | P0 | H1,H2 / DB / F0,F6 | 分别检查 enabled/disabled/uninitialized、全体可达及缺失 peer 的状态 | idle 只在覆盖完整且真实无活动时成立；partial/unknown/degraded/disabled 不被客户端转换为全部完成。 |
| J02 · `heal.outcome-counters-and-error-truth` | P0 | H1,H2 / DB,DV / F1,F2 | 同批次混合健康、可修、不可修、deferred、dry-run 对象 | execution/coverage/disposition/计数和逐对象物理结果相符；unknown、skipped 或 completed_with_errors 不能算修复成功。 |
| J03 · `heal.incremental-cursor-and-truncation` | P0 | H1,H2 / DS,DX / F1 | 生成超过结果保留上限的任务，分页轮询 sinceSeq，并跨节点/终态读取 | nextSeq/minSeq 单调且 gap/truncation 明示；有界 items 不是完整责任清单，最终须用独立 manifest/计数补证，不静默丢错误。 |
| J04 · `heal.repeat-idempotency` | P1 | H1,H2 / DB,DV / F1 | 第一次修复后重复 normal/deep、重启再检查 | versioned 首次允许所有 identity repaired 或至少一个 repaired 加其余正健康态，marker 仅 repaired/metadata_healthy，计数必须与 receipt 集合一致；normal repeat 接受 `metadata_healthy`，deep repeat 要求 `verified_healthy`，repeat marker 为 `metadata_healthy` 且 healed=0；unknown/skipped/failed 不得成为 PASS。 |
| J05 · `heal.long-soak` | P2 | H2,H5 / DX,DL,DV / F4,F6,F11 | 独占环境运行预注册 24h/72h 窗口，混合修复、掉线、写入和版本删除 | 每次 ACK 可追踪；最终全量 manifest 正确，队列/内存/FD/临时空间无无界增长，无后台 panic 或未解释持久责任。 |
| J06 · `heal.performance-abba` | P2 | H2 / DX,DL / F1,F11 | 同硬件同候选按 ABBA 运行无 heal 基线和有 heal 负载，重复并固定限速 | 分别报告修复吞吐、GET/PUT P95/P99、错误率和 CPU/IO；阈值事先冻结，不能事后挑轮次或把功能 PASS 充性能验收。 |

## 与主仓库既有验证的复用关系

以相同源 SHA 的实现为准，先读脚本的 scope、环境要求和故障 oracle，再适配到固定 fleet；**不直接在共享机器运行原 E2E 的清盘/建进程脚本**。已有覆盖只是复用线索，不能直接标记本目录 planned 用例为 implemented。

| 已有资产 | 可复用内容 / 本矩阵对应 | 不能混淆的边界 |
| --- | --- | --- |
| `crates/e2e_test/src/heal_erasure_disk_rebuild_test.rs` | runtime wiped disk、Admin deep、remote replacement，B/C/E/F | 进程 E2E 与固定真机、同节点目录盘与独立 XFS 物理盘需分别标记；检查其 skip 分支，必选故障能力缺失不能通过。 |
| registry 的 background-target-restart / crash | F02/F03、目标部分进度与 replacement shard 证据 | SIGKILL 不是断电；原夹具无版本覆盖不能自动扩张为 DV 已覆盖。 |
| registry 的 EC8+4 single-set / multi-set / multi-pool 项 | H07/H06 与相应 F02/F03 参数 | 精确盘数、set/pool 分布与同一故障窗口要匹配，4 盘 PASS 不能覆盖 G14。 |
| W13 durable MRF 与 W16 recovery-intent/quota 证据 | E05、F07、I02、J02 | 责任 ACK、恢复意图、配额与终态内容都需要原始实测输入，不只运行 JSON assembler。 |
| G05/G06 status/outcome、G02 checkpoint、G09 upgrade 证据生产脚本 | A03、J01-J03、F07/F08 | schema 自检、descriptor 组装和真实故障执行是三种不同证据，不可互相代替。 |
| ECStore/heal 原生 runtime-fence、版本删除/覆盖竞争回归 | A09、C07、G01-G06、I05 | 原生可控屏障覆盖精确窗口；没有在 fleet 触发该窗口时只记录功能覆盖，不把两类日志混为真机因果证明。 |

来源：[必选 E2E registry](https://github.com/rustfs/rustfs/blob/940e221988694da33fe005054bb86086bae88cd6/.config/scanner-heal-required-tests.json)、[磁盘重建 E2E](https://github.com/rustfs/rustfs/blob/940e221988694da33fe005054bb86086bae88cd6/crates/e2e_test/src/heal_erasure_disk_rebuild_test.rs)、[Linux evidence plan](https://github.com/rustfs/rustfs/blob/940e221988694da33fe005054bb86086bae88cd6/scripts/run_scanner_heal_linux_evidence_plan.py)、[脚本分类](https://github.com/rustfs/rustfs/blob/940e221988694da33fe005054bb86086bae88cd6/scripts/README.md)。其中 plan/preflight 不产生 measured evidence，assembler 不替代采集。

## 当前框架的执行方式

Functional Chain 在共享节点并发锁内调用 auto-testing 的 canonical dispatcher。普通 live 执行消费候选 deb 和工具产物，无需 RustFS checkout；只读 metadata inspector 和原生屏障测试须在构建阶段绑定精确候选源码，单独留证。

尚未自动化的分支由 `matrix.py plan` 固定全部适用拓扑、源断言、操作顺序、参数绑定与恢复步骤。`matrix.py audit` 校验九类证据的摘要和分支完整性，结果仍须逐项审阅，绝不自行生成 canonical PASS。


正向 H1/H2 夹具显式启用 `RUSTFS_SHARD_INTEGRITY_WRITE=true` 和 `RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED=true`。该确认只针对本例私有 endpoint 集合：所有参与节点先安装并校验同一候选二进制，再启动专用服务；配置从真实进程环境读回。候选默认值为 false，默认 legacy 对象的 unknown 回执已单独保留为失败证据，不能改写为 repaired，也不从保护模式推断默认部署已满足这些正向证明契约。
