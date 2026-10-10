# Heal 真机执行手册：84 项操作卡的公共命令

此手册和 [84 项逐例操作卡](validation-heal-cases.md) 一起构成可执行的**人工操作方案 + 脚本辅助**。不是 84 个无人值守自动测试，也不是通过报告。操作卡列出 301 个分支模板；真实盘角色、阈值、损坏数量和拓扑仍需在每批 `parameterBindings` 展开，不能把模板数量当实测次数。

公共验收契约见 [Heal 规范](validation-heal-contract.md)。源仓库历史 pilot 结果不计入当前 canonical 分母。自动入口 `scripts/run_validation_case.sh CASE_ID CANDIDATE_MANIFEST TESTING_COMMIT ARTIFACTS_DIR` 支持 A01、A03、A04、A07、C01–C04、D02、D07、E01、E02，使用当前 Heal 节点、候选 deb 和独立 H1/H2 服务；其余操作卡仍需按本手册操作留证。

## 1. 使用入口与交付物

```bash
python scripts/validation_heal/matrix.py list
python scripts/validation_heal/matrix.py show C02
python scripts/validation_heal/matrix.py show F02,F03,H07
python scripts/validation_heal/matrix.py plan --selection all \
  --run-id manual-heal-20260910-a --out reports/validation-heal-84-plan.json
python scripts/validation_heal/matrix.py plan --selection A01,A04,C01,C02,C04,J04 \
  --topologies H1 --run-id manual-heal-20260910-b --out reports/heal-h1-plan.json
```

`plan` 只冻结选择/分支和对应操作，不部署、不评分。`show` 同时给出步骤、命令入口、验收和恢复要求。不同拓扑是不同实例；未选拓扑会保留 `scopeExclusions`，不能据局部结果声称全矩阵通过。

自动用例通过 RustFS Functional Chain 的 `validation_case` 和精确 `auto_testing_sha` 分发。先用同一不可变候选的 `build_run_id`/`build_run_attempt`、`validation_tools_only=true` 构建只读 metadata inspector；live 分发提供该成功构建的 `validation_tools_run_id` 与 `validation_tools_run_attempt`。工具的 candidate/helper SHA、Linux 二进制 SHA 和独立 Cargo.lock 摘要均须通过检查，再复制到本例节点。普通 live 执行消费构建产物，不需要 RustFS checkout。

| 工具 | 已实现的职责 | 不代表什么 |
| --- | --- | --- |
| `matrix.py` | 84 项/301 分支索引、逐例操作卡、冻结计划、证据完整性审计 | 不自动分配拓扑，不根据文件存在就判 PASS |
| `heal_probe.py` | 带签名/匿名/坏签名的原始 Admin；受限 S3 操作；全版本/标签/内容摘要清单及比较 | HTTP 检查通过不代表 heal 成功；inventory 的 GET 会触发读修复 |
| `heal_workload.py` | DB/DV/DM/DS/DX 真实夹具；有界并发、同key前台修改串行、请求前及ACK后持久落盘 | 不替用户推导竞态后的最终版本；超时写必须单独解析 |
| `heal_fault.py` | Linux 描述符相对寻址、mount ID 保护、单文件 missing/bitflip/truncate/zero/header、快照和恢复 | 文件故障不是物理掉盘；不修改系统 metadata；不盲目恢复新代次 |
| `prepare_heal_meta_inspector.py` | 为精确候选源码准备只读 Rust metadata 解析器工程 | 需在批准的构建机编译；解析 metadata 不是验证 bitrot/EC |
| `shards.py` | C01–C04、D02、D07、E01、E02 全 24 分支以及 A03 的两个 H2 跨节点状态分支、A04 的六个 DB/DV dry-run 分支；静默故障、真实角色、同角色原片与 HighwayHash、先物理后 GET/Range、MPU control | 有自动入口不等于候选通过；任务 unknown、目标只恢复 metadata 或救援备份均不能记 PASS |
| `check_bitrot_tool.py` | 在固定 MinIO/历史 RustFS 产物上检查 24 个健康片和 12 个坏片负例 | 工具验证不等于 live 用例通过；不调用被测 RustFS 服务 |

## 2. 先绑定环境，禁止直接猜节点

live 命令使用当前 auto-testing 的 `smoke-testing` runner、`RUSTFS_NODES` 和 `RUSTFS_SSH_USER`，复用 `rustfs_heal_test.sh` 的 Heal 节点。运行前取得与 Functional Chain 一致的共享节点并发锁。H1–H6 是测试拓扑要求，不是另一组 SSH 主机；不得按源仓库历史主机名或 IP 选择机器。

用 `matrix.py plan` 冻结当前 SSH 节点与所有适用操作分支，再根据本次只读 preflight 填写私有 `lab.json`。计划中的 `verified:false` 不可直接改成资源验证通过。H4 需要至少 24 个已分配独立盘端点；H6 需要独占主机及平台事件/恢复绑定。缺少资源的分支保留为 BLOCKED。

填写内容须包含：

- 本批 run ID、截止时间、独占服务/端口、jump route、实际主机名、每个磁盘的 mount UUID/major:minor、服务 unit/原 PID/数据根；
- 与当前 validation lane 相同的不可变 candidate manifest、源码 commit、包与已安装二进制 SHA-256；当前 rc 路径与二进制 SHA-256；auto-testing commit；保存真实构建清单，不只填摘要字符串；
- H1–H6 实際形状、每个对象 N/data/parity/R/W/lock quorum、pool/set/slot/盘 UUID；H4 至少 24 个实际盘端点；
- 最长修复/重启/故障时限、对象/字节/请求/并发预算、所有危险动作的精确目标及恢复人；
- 包含平台操作、独占故障设备、旧版本、KMS/tier 或原生屏障的实例，先提供能力及授权证据。

`binding-check` 只是语法/租约检查，不是资源或包身份的远程证明：

```bash
python scripts/validation_heal/heal_probe.py --binding lab.json --out reports/binding-check.json binding-check
```

先在跳板只读核验 `hostname`、`findmnt -J`、`lsblk -J`、`ss -lnt`、二进制 `sha256sum`/`--version`、实际运行配置与 PID。保存输出为 preflight。租约没有发放、拓扑不符、端口占用、磁盘 UUID 漂移时在这里 BLOCKED，不先注入后跳过。

### 独立实验服务的部署

H1 使用当前 Heal 节点经核验的四个独立 mount，**另建** `/data/rustfsN/heal-validation/<runId>/data` 和同级 `quarantine`，root-owned 0700；每个 `<runId>` 根写入包含 `runId` 的 root-owned `owner.json`。先检查每个根不存在；已存在的根不能覆盖。复用 `rustfs_heal_test.sh` 的 candidate deb 下载、SHA 校验和安装流程，把已安装且摘要已核验的 `/usr/bin/rustfs` 放在本批 state 下；执行用例不需要 RustFS 源码 checkout。凭据文件为 0600。以下专用 unit 仍运行在当前 Heal 节点，使用已分配的 9000 测试端口：

```bash
# 在目标节点；所有变量来自已审核的 lab binding，不接受任意在线输入。
sudo systemd-run --unit="$HEAL_UNIT" --property=Type=exec \
  --property=PrivateMounts=yes --property=KillMode=control-group \
  --property=TimeoutStopSec=30 --property=RuntimeMaxSec=3600 \
  --property=MemoryMax=2G --property=CPUQuota=200% \
  --property=WorkingDirectory="$HEAL_STATE" \
  --property=EnvironmentFile="$HEAL_STATE/service.env" \
  "$HEAL_STATE/rustfs" "${HEAL_DATA_ROOTS[@]}"
```

需重启的场景保留 unit 定义、env、二进制及原始 endpoint 参数，不使用 `--collect`；RuntimeMaxSec 按该批预算冻结，J05 必须单独分配长时服务。`HEAL_DATA_ROOTS` 是已审核的 Bash 数组，不从未审核字符串 eval。H2/H3/H4/H5 用各节点一致的 endpoint 序列/显式 pool 分组配置；启动后从每盘 `format.json` 读回，不能从启动参数猜拓扑成功。不得直接运行会清理旧 Pool 根的部署/reset 命令。

### 命令约定

下列函数在**跳板机**执行。`HEAL_BINDING`、`HEAL_REPORT` 是该实例私有绝对路径；`B` 是绑定 prefix 下的一个桶，`K` 为逻辑 key，`V` 为先前 ACK 的精确版本。每条命令的 label 唯一，已有证据拒绝覆盖。凭据只通过受控环境注入 `HEAL_ACCESS_KEY`/`HEAL_SECRET_KEY`/可选 session token，禁用 `set -x`。

```bash
set -euo pipefail
umask 077
hp() {
  local label=$1
  shift
  python scripts/validation_heal/heal_probe.py --binding "$HEAL_BINDING" \
    --out "$HEAL_REPORT/$label.json" "$@"
}
hw() {
  local label=$1
  shift
  python scripts/validation_heal/heal_workload.py --binding "$HEAL_BINDING" \
    --out "$HEAL_REPORT/$label" "$@"
}
B=$(jq -er .bucketPrefix "$HEAL_BINDING")
RUN=$(jq -er .runId "$HEAL_BINDING")
```

特权故障命令在**经跳板连接的目标节点**执行。`ROOT` 是本例目标盘 `<runId>` 根；`NODE` 是已绑定 SSH alias；把该 SHA 的 `heal_fault.py` 放在 root-owned 的本批 state 下并核对摘要。不得从逻辑 key 拼 SSH shell。以下 `hf` 仅在该目标节点的交互 shell 定义：

```bash
hf() { sudo python3 "$HEAL_STATE/heal_fault.py" --root "$ROOT" --run-id "$RUN" "$@"; }
hf init --bucket-prefix "$B" --lease-expires-at "$LEASE_EXPIRES_AT"
```

初始化只增加故障所有权标记，不创建/格式化数据盘。每盘只 init 一次。系统重启导致 mount ID 变化时暂停注入；先重新核对 UUID/major:minor/所有权并人工重新绑定，不绕过检查。

## 3. 数据与客户端命令

<a id="SEED_DB"></a>
<a id="SEED_DV"></a>
<a id="SEED_DM"></a>
<a id="SEED_DS"></a>
<a id="SEED_DX"></a>

### 确定性夹具

每个命令使用新桶，禁止在现有夹具桶上重复 seed 后称同一轮。

```bash
hw seed-db seed --fixture DB --bucket "$B-db"
hw seed-dv seed --fixture DV --bucket "$B-dv"
hw seed-dm seed --fixture DM --bucket "$B-dm"
hw seed-ds seed --fixture DS --bucket "$B-ds"
hw seed-dx seed --fixture DX --bucket "$B-dx"
# 替换为候选确认的真实 inline/block 边界；可另外生成一桶：
hw seed-boundary seed --fixture DB --bucket "$B-boundary" \
  --count 128 --sizes "$BOUNDARY_SIZES"
```

DB 默认 128 项，包含 0/1/4095/4096/4097、1MiB±1、多MiB，非重复 SHAKE 数据；1MiB 不是预设产品阈值。DV 默认 4 key，每 key 三个版本，交替创建当前 marker。DM 有已完成三 part（前两 part 分别 5MiB+17、5MiB+93，末 part 1MiB+47）及两个独立 pending upload。DS 默认 257 特殊/多前缀 key；DX 默认 257 外部对象。自动 seed 不包括 suspended/null、特殊压缩、SSE/retention 等额外分支，按下列 S3 命令补充并保存 ACK。

从 ACK 文件取得真正的 UploadId/VersionId/part ETag；不要在多个上传之间复用猜测的 ID。总字节预算包含所有版本/part，不足时先扩展经批准的预算，不自动跳过对象。

后续命令的 `B` 必须改为该实例实际桶，例如 `B="$(jq -er .bucketPrefix "$HEAL_BINDING")-db"`，不要对没有创建的裸 prefix 做 inventory。根目录 owner 仍绑定原始 run prefix，以允许本run的各后缀桶。

seed 完成后自动以完整 GET/版本清单对账确定性原文与创建 ACK，保存 `expected-fixture.json`、`baseline.json`；没有通过基线校验就不准进入注入。这个 GET 发生在故障前，不可拿 seed 命令监测 scanner/read-repair 故障窗口。

<a id="S3"></a>
<a id="GET"></a>

### 单个 S3 操作与精确版本

```bash
hp put s3 --operation put_object --size 5243003 --seed "$RUN/new/1" \
  --params "$(jq -nc --arg b "$B" --arg k "$K" '{Bucket:$b,Key:$k,ContentType:"application/octet-stream",Metadata:{nonce:"1"}}')"
hp get s3 --operation get_object \
  --params "$(jq -nc --arg b "$B" --arg k "$K" --arg v "$V" '{Bucket:$b,Key:$k,VersionId:$v}')"
hp suspend s3 --operation put_bucket_versioning \
  --params "$(jq -nc --arg b "$B" '{Bucket:$b,VersioningConfiguration:{Status:"Suspended"}}')"
hp delete-version s3 --operation delete_object --expect-http 204 \
  --params "$(jq -nc --arg b "$B" --arg k "$K" --arg v "$V" '{Bucket:$b,Key:$k,VersionId:$v}')"
```

请求复杂时 `--params-file` 使用 JSON。接口参数为 boto3 snake_case 操作名、原生 CamelCase 参数；只允许本 run 桶、受限操作，copy 源也必须在本 run。GET 只保存完整内容的 hash/长度，不把对象内容写入日志。PUT/Complete 超时记 resultUnknown，下一步用相同唯一 key/version/UploadId 查询解析，不静默重试。

`--expect-http` 必须使用该请求预注册的精确值。例如 create/put/list 常见 200，delete/abort 常见 204，Range 206；负例按候选契约分别 304/403/404/405/412/416。CLI 返回 0 只说明该次预期 HTTP 匹配，永不宣称整个 case PASS。SSE-C 参数值可使用字面量 `$HEAL_SSE_C_KEY`，脚本只在内存替换，输出脱敏。

<a id="BASELINE"></a>
<a id="VERIFY"></a>

### 全版本基线和最终核对

```bash
hp baseline inventory --bucket "$B"
# 先完成触发归属和 PHYSICAL，再调用下面 GET 型检查：
hp post-heal inventory --bucket "$B"
hp s3-comparison compare --before "$HEAL_REPORT/baseline.json" \
  --after "$HEAL_REPORT/post-heal.json"
```

inventory 分页遍历全部版本/marker，按每个 versionId GET 全内容、tags 和公开 metadata，再用分页 current LIST 交叉核对；对象上限 10,000、单对象 64MiB，批次字节预算来自绑定文件。更大对象场景需单独预注册流式工具和预算，不能截断后称全量。

并发、生命周期、合法删除场景的预期清单会变化：人工先按逐请求 ACK 因果关系生成 `expected-post.json`，记录每项增删对应的 ACK 文件；再比较实际清单，不能直接拿静态基线判失败或拿实际输出复制成期望。未知写入未解析则不可通过。加密/归档对象先用授权 GET/restore 参数逐版本取证；默认 inventory 不会自动解锁、传 SSE-C 或 restore。恢复型 GET 必须保存密文/明文两层对应关系。

<a id="RANGE"></a>

### Range、HEAD、条件请求

```bash
hp head s3 --operation head_object --params-file request-head.json
hp range s3 --operation get_object --expect-http 206 --params-file request-range.json
hp if-none-match s3 --operation get_object --expect-http 304 --params-file request-if-none-match.json
hp if-match-reject s3 --operation get_object --expect-http 412 --params-file request-if-match-bad.json
hp range-outside s3 --operation get_object --expect-http 416 --params-file request-range-outside.json
```

JSON 都包含本例 Bucket/Key/可选 VersionId；另外分别添加 `Range:"bytes=5242879-5242882"`、`IfNoneMatch:<原ETag>`、`IfMatch:<错误但语法合法ETag>`、超出对象长度的 Range。时间条件用 `IfModifiedSince`/`IfUnmodifiedSince` 的 ISO 日期；从 HEAD 的真实 LastModified 推导早/晚两个控制值。验证 ContentRange、响应长度和从确定性原文计算的片段 hash，不能只验状态码。

<a id="SEED_DE"></a>
<a id="SEED_DO"></a>
<a id="DEPENDENCY"></a>

### SSE、Object Lock 和外部依赖

```bash
hp sse-s3-put s3 --operation put_object --size 5243003 --seed "$RUN/sse" --params-file sse-s3-put.json
hp sse-kms-put s3 --operation put_object --size 5243003 --seed "$RUN/kms" --params-file sse-kms-put.json
hp sse-c-put s3 --operation put_object --size 5243003 --seed "$RUN/ssec" --params-file sse-c-put.json
hp locked-bucket s3 --operation create_bucket --params-file locked-bucket.json
hp retained-put s3 --operation put_object --size 5243003 --seed "$RUN/lock" --params-file retained-put.json
hp retention s3 --operation get_object_retention --params-file protected-version.json
hp hold s3 --operation get_object_legal_hold --params-file protected-version.json
hp reject-delete s3 --operation delete_object --expect-http 403 --params-file protected-version.json
```

SSE JSON 分别加 `ServerSideEncryption:"AES256"`、`ServerSideEncryption:"aws:kms",SSEKMSKeyId:<本例专用key>`、`SSECustomerAlgorithm:"AES256",SSECustomerKey:"$HEAL_SSE_C_KEY"`。SSE-C GET 也提供同一密钥，先用原请求证明可解密。所有外部 key/endpoint 是经过审批的测试租户，不复用生产依赖。

锁桶 JSON 为 `{Bucket:...,ObjectLockEnabledForBucket:true}`。受保护 PUT 使用 `ObjectLockMode:"GOVERNANCE"` 或 `"COMPLIANCE"`、实际未来 UTC `ObjectLockRetainUntilDate`，或 `ObjectLockLegalHoldStatus:"ON"`。必须验证无 bypass 角色在保留期间真的不能删除；脚本拒绝 BypassGovernanceRetention=true。Legal hold 解除使用 `put_object_legal_hold` 的 `LegalHold:{Status:"OFF"}`，仅作用于本例版本。Compliance 等真实到期，不改主机时间、不内部删文件。

KMS/tier outage 使用 NETWORK 在本例专用 net namespace 内仅阻断对应依赖的 IP/port，或仅撤销本例租户凭据；恢复原规则/权限后重试原 version。tier 缺失一个远端版本可通过远端本例桶 API 删除并保留ACK/原备份，但这是“远端对象丢失”负例，不等同网络中断。

## 4. Admin 触发、状态与窗口

<a id="CONFIG"></a>

实际服务配置要取证，但**不要转储包含凭据的全环境**。manual 模式记录以下非敏感字段：

```text
RUSTFS_HEAL_ENABLED=true
RUSTFS_SCANNER_ENABLED=false
RUSTFS_HEAL_AUTO_HEAL_ENABLE=false
RUSTFS_HEAL_MRF_ENABLE=false
RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE=false
```

scanner/自动/MRF 场景按操作卡逐项开启相应字段，并冻结 scanner cycle/start delay/bitrot cycle。配置变更只改专用 service.env，重启本例 unit 后证明实际生效。read-repair 所需队列依赖按候选源码核实，不能为了“排除后台”关掉被测路径。

<a id="START"></a>
<a id="STATUS"></a>
<a id="STOP"></a>
<a id="WAIT"></a>
<a id="ADMIN_RAW"></a>

### 原始 Admin 命令

`SCOPE_PATH` 为编码后的 `/rustfs/admin/v3/heal/<bucket>/<key-or-prefix>`；key 的每个字节按 URL 编码，root 为 `/rustfs/admin/v3/heal/`。JSON 中 pool/set 是实际零基索引，不能在 H5 固定填 0。例：

```bash
hp start admin --path "$SCOPE_PATH" \
  --body '{"recursive":false,"dryRun":false,"remove":false,"recreate":false,"scanMode":2,"updateParity":false,"nolock":false,"readRepair":false,"pool":0,"set":0}'
TOKEN=$(jq -er '.observation.response.clientToken | select(type=="string" and length>0)' "$HEAL_REPORT/start.json")
hp status-001 admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN"
hp stop admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN" --query forceStop=true
hp force-start admin --path "$SCOPE_PATH" --query forceStart=true --body-file heal-options.json
# 保留重复 query，真正送到服务端验证：
hp invalid-duplicate admin --path "$SCOPE_PATH" --query clientToken=a \
  --query clientToken=b --expect-http 400
```

WAIT 用有界循环保存每一响应，不仅保存最后绿色状态；普通 repair 900 秒、重启后 1800 秒是起始测试预算，须预注册，不是产品 SLO：

```bash
for i in $(seq 1 900); do
  label=$(printf 'wait-%04d' "$i")
  hp "$label" admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN"
  summary=$(jq -er '.observation.response.summary' "$HEAL_REPORT/$label.json")
  if [ "$summary" != running ]; then break; fi
  sleep 1
done
```

循环退出后人工/候选 adapter 检查：execution completed、coverage complete；failed/unknown/attemptFailures=0；不溢出；逐对象 disposition 与设计匹配；非 dry-run 的 incarnation/版本/pool/set 正确。dry-run 只允许 observed，healed=0，随后无物理变更。负例以预注册错误/未完成契约判，不套正向 completed。

`summary=finished` 或 HTTP200 单独不够；`truncated=false` 可以省略，null/true 不算 false；status.settings 是查询默认值，不代表原任务实际 scanMode。outcome 的 128 项窗口不是完整对象清单。当前 rc 会简化部分字段，以 raw Admin + 独立 manifest 为准。

<a id="BACKGROUND"></a>
<a id="REPLACEMENT"></a>
<a id="RC_STATUS"></a>

```bash
hp background admin --path /rustfs/admin/v3/background-heal/status
hp replacement admin --method GET --path /rustfs/admin/v4/heal/replacement-recovery
"$RC_BIN" --json --no-color admin heal status "$RC_ALIAS"
```

rc 必须是本批冻结二进制，私有 RC_CONFIG_DIR/alias 只指向本例服务，stdout/stderr/exitCode 都留存。查询其他节点：直接运行 heal_probe 的全局 `--endpoint n1`/`--endpoint stable`；保存实际连接 peer 身份。健康状态需要 expected/responded/unknown 与所有 peer 覆盖，不允许缺 peer 时 idle 假绿。

<a id="WINDOW"></a>
<a id="CURSOR"></a>

### 部分进度与增量游标

```bash
hp cursor-001 admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN" --query sinceSeq=0
NEXT=$(jq -er '.observation.response.nextSeq - 1' "$HEAL_REPORT/cursor-001.json")
hp cursor-002 admin --path "$SCOPE_PATH" --query "clientToken=$TOKEN" --query "sinceSeq=$NEXT"
```

WINDOW 的必要证据是 running 且 `0<processed<预先冻结对象总数`，再结合目标片快照证明确实还有未建文件；保存观测响应、触发命令和目标PID/代次。单看processed不足时需要显式 target completed/expected 集合。状态/采样时延必须小于故障窗口，超过就判 NOT_OBSERVED。不能固定 sleep 后无条件杀进程，不能无限重跑直到碰巧命中。

CURSOR 逐响应保存 nextSeq/minSeq/truncated/items；过旧 cursor 的 gap 是预期观测项，不可静默丢弃。跨节点和重启后按 task/epoch分组，有界窗口之外用完整manifest补证。DX 257 是起始数据量；若仍过快，下一批预注册增加负载或受限故障层，而不是本批临时改分母。

固定候选的 `sinceSeq` 是排他游标，`nextSeq` 是下一待分配序号，因此完整响应之后传 `nextSeq - 1`。上例只适用于本次 items 完整返回；若 payload 本身裁剪了 items，只推进到 `max(sinceSeq + 1, minSeq) + 实际收到项数 - 1`，继续读取未返回后缀。不要把响应裁剪或保留缺口隐藏在 nextSeq 跳跃中。J03 自动执行器预注册 1025 个 1MiB+73 字节的外部对象，实际越过该候选 1024 项的 item retention；128 项 outcome 诊断窗口另行验证。每节点物理清单显式限制 12288 文件，每批独立 streaming 校验最多 128 个 part。未观察到两个有效 running 增量窗口时记录 NOT_OBSERVED，完整用例不能 PASS。H1 的跨节点分支明确不适用，H2 查询所有四个实际节点。

<a id="IAM"></a>

### 权限和过期签名

在专用服务先保存当前 rc 的 `admin user --help`、`admin policy --help`、`admin heal --help`。使用候选 IAM 支持的 Admin Heal action 创建最小策略（从候选 `HealAdminAction` 的实际字符串导出，不猜拼写）；另建明确不含该 action 的角色。示例用户/策略动作按本批 rc help 的参数顺序执行并保存原始回执，凭据用私有输入通道，不开 shell trace。

已核对参考候选 action 为 `admin:Heal`，最小策略主体为 `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:Heal"],"Resource":["arn:aws:s3:::*"]}]}`；RustFS policy parser 要求带类型的 ARN，裸 `"*"` 会被拒绝。将这个权限仅赋予本例临时身份，不改共享管理员策略。新候选仍需核对 action 和 rc attach 语法。

```bash
hp anonymous-start admin --path "$SCOPE_PATH" --auth anonymous --expect-http 403 --body-file heal-options.json
hp bad-signature admin --path "$SCOPE_PATH" --auth bad-signature --expect-http 403 --body-file heal-options.json
# 无权限/最小权限实例：更换本次调用环境中的 HEAL_ACCESS_KEY/HEAL_SECRET_KEY，执行相同请求。
```

过期签名：保存一次已签请求，在签名有效期外重放到**相同本例地址/路径/请求体**，仅等待客户端签名超期，不改变服务器时钟。先用新签名同请求做控制。时限按候选签名校验窗口预注册；签名本身含敏感身份，原请求存私有0600文件，只归档脱敏结果和请求摘要。对 start/status/stop 都测试；token 不能替代权限。结束时通过正常 IAM API 删除本例用户/策略并验证旧凭据失效。

## 5. 物理证据与故障后端

<a id="FORMAT"></a>
<a id="PHYSICAL"></a>

### 真实布局、角色和物理快照

在目标节点逐盘读取本例 `data/.rustfs.sys/format.json`（只读）及 mount UUID/major:minor，保存 pool/set/this 映射。不能把同一设备的多个目录作为独立盘。逐对象 xl.meta 使用与候选相同源码的只读解析器：

```bash
python scripts/validation_heal/prepare_heal_meta_inspector.py --source "$CANDIDATE_SOURCE" \
  --commit "$CANDIDATE_COMMIT" --out "$INSPECTOR_BUILD"
# 仅批准的构建机；生成独立 helper 的锁文件后冻结，和服务构建清单分开。
cargo build --release --manifest-path "$INSPECTOR_BUILD/Cargo.toml"
sha256sum "$INSPECTOR_BUILD/Cargo.lock" "$INSPECTOR_BUILD/target/release/heal-meta-inspect"
"$INSPECTOR_BUILD/target/release/heal-meta-inspect" "$XL_META_EVIDENCE_COPY" "$B" "$K"
```

解析器不输出用户metadata或密钥，只输出版本、data_dir、parts、EC参数/分片角色。首先对同批健康样本和损坏样本分别证明成功/拒绝；不支持的版本不得换别的版本解析器后宣称同候选验证。源 checkout 必须为固定 SHA 且无已跟踪修改。

```bash
# 目标节点。所有输出重定向到本例证据文件，不能覆盖已有阶段。
hf inspect --path "$TARGET_RELATIVE_PATH"
hf snapshot --bucket "$B"
```

快照对整桶每个文件保存长度/hash，目录描述符逐层拒绝 symlink/hardlink/不同mount ID（含同设备bind mount）；最多10,000文件/8GiB，每文件64MiB。四阶段 before/fault/after/restart 每盘各一份。外部part与**同一个角色、同版本的健康原片**字节完全相同是独立重建对照；数据应不重复且四片摘要不同。不得读取 quarantine 来辅助服务器重建。

xl.meta 可合法重写；比较解析后的版本/引用，而非要求所有内部字节不变。允许的 xl.meta.bkp 必须属于同目标data_dir且摘要等于原xl.meta，不能泛化豁免任意额外文件。普通解析模式不验证 payload；`--verify-part XL_META BUCKET KEY PART_FILE VERSION_ID PART_NUMBER` 按候选 metadata 的实际版本、part 和 legacy/current framing 逐块验证 HighwayHash，同时拒绝短读、尾随字节和不支持的格式。分片执行器还逐项要求所有同版本片与故障前原始同角色长度/hash 完全一致，并确认基线全文、真实 set slot 及不重复分片摘要。它不支持加密/压缩、inline 或改变编码形态的物理验收；这些用例仍保留各自工具要求。

<a id="FAULT"></a>

### 单文件故障（已有脚本）

先停止本例写入并让测试进程静止；格式映射已确认后选择精确物理路径，不能只找第一个part。注入/恢复时 `--service-quiescent` 是操作者确认，工具不会替你停止共享服务。

```bash
hf apply --path "$TARGET_RELATIVE_PATH" --fault-id C02-data-middle \
  --kind bitflip --offset "$BYTE_OFFSET" --expected-sha256 "$BASELINE_SHA256" --service-quiescent
# 其他实例各用新故障ID：--kind missing / truncate / zero / header
hf snapshot --bucket "$B"
```

`truncate` 保留一半；`header` 翻转前32字节；bitflip 可指定任意有效offset；zero 若不会改变字节则拒绝。先持久化intent/原件，再变更，再见证；重复apply拒绝覆盖。备份在同run隔离目录，绝不在RustFS数据根中。

文件故障为 F1/F2/对象级F3，不是整盘故障。helper 明确禁止 `.rustfs.sys`、format、IAM、任意shell或设备操作。用例需要系统记录/特殊格式时走下面人工白名单后端。

<a id="FAULT_FORMAT"></a>
<a id="FORMAT_BAD"></a>
<a id="CHECKPOINT"></a>

### 格式/系统记录故障（人工白名单后端）

先 `PROCESS stop`，从候选源码/真实task定位**本例**精确记录，生成 `record-plan.json`：绝对路径、run/task/bucket/incarnation、盘UUID、原sha/size、变更offset或替换源sha、允许新增/删除字段、备份和undo。第二人/批准的操作人逐项审核；禁止递归 `.rustfs.sys` 修改。

可操作的配方：

1. **near-tail/长度不符**：复制目标part到本例quarantine并校验sha；在该目标副本上 `truncate -s "$NEW_SIZE" "$TARGET"`，其中 NEW_SIZE=原size−1或指定长度。核对新size/hash和原metadata未变。停止服务期间执行，目标路径来自已审核清单。
2. **外来format/set**：同run的第二个隔离小集群正常启动产生format，停止两服务。仅把其format安装到本例空替换根，不改健康盘；记录原/外来id、this、sets。启动后期待安全拒绝，原外来format不能被改写。
3. **未来format版本**：在本例替换根的format JSON副本，用 `jq` 只更改已核实的版本字段为不支持值；原件与变更diff归档。未来对象/checkpoint schema 按候选编解码结构更改对应版本头，不能随机翻位后称“未来schema”。如果存在校验，必须重算后证明结构可解析且schema未知；否则只计坏校验实例。
4. **冲突代次/quorum**：串行合法写V1/V2或两个incarnation，分别保存全套本例副本；停服务后按预注册盘分配表安装各自不足R的集合。重启前用FORMAT逐盘核对，证明没有任一同版本quorum。
5. **checkpoint/MRF坏记录**：只读保存 `.rustfs.sys/root-heal-<task-id>.json` 与候选实际journal路径；确认属于本run。截断、替换为从本run早期阶段保留的陈旧代次、或单独更改校验字段。不得凭目录名删除整个queue。写失败通过IO_FAULT构造，不改权限后未经见证就称失败。
6. **bucket metadata**：只列入对应本例桶的记录；建立同run健康备份，分别缺失/截断后重启/heal。原策略、tags、版本/lock配置通过S3核对，其他系统文件摘要不变。
7. **inline偏移**：用匹配候选的codec在证据副本中定位目标version的inline map/value；保存解析offset/长度及反向映射，之后只修改该value。不会解析时不能任意翻xl.meta头冒充inline损坏；改用已明确定义的“坏metadata”实例，原inline实例仍未覆盖。
8. **orphan/dangling**：通过正常完成/覆盖/删除形成历史目录，FORMAT证明所有活version/MPU引用后再测试remove。宽限期按真实时间/本例服务配置等待，不改主机时间。共享引用控制必须保留。

上述人工操作每一步均需精确原件备份、变更前后hash、操作返回值与复查；不能直接 `rm` 记录。用 `mv -n` 同盘移到隔离路径可保留原件，但必须核实源消失/目标摘要相同，因为 `mv -n` 跳过未必报错。恢复时先保留失败后新记录，若有新ACK/代次，禁止把旧metadata拷回去。

<a id="DRIVE_GATE"></a>

### 逻辑盘不可达、只读、权限

仅在已设置 PrivateMounts 的**本例服务 mount namespace**使用；先证明目标PID仍属于本例unit、`/proc/$PID/ns/mnt` 与host不同，且目标挂载非shared。不能进入host namespace卸载 `/data/rustfsN`。原理参考 [Linux nsenter](https://www.man7.org/linux/man-pages/man1/nsenter.1.html)。

只读配方（在目标节点，DATA为本例完整数据根）：

```bash
sudo nsenter -t "$HEAL_PID" -m -- mount --bind "$DATA" "$DATA"
sudo nsenter -t "$HEAL_PID" -m -- mount -o remount,bind,ro "$DATA"
sudo nsenter -t "$HEAL_PID" -m -- findmnt -T "$DATA" -o TARGET,SOURCE,OPTIONS
# 以服务实际UID在该namespace做本例临时文件写探针，必须观察EROFS。
# 解除仅此额外的bind层：
sudo nsenter -t "$HEAL_PID" -m -- umount "$DATA"
```

不可达用经批准的该服务 mount namespace 故障层覆盖此数据根（返回EIO的专用device-mapper设备，见IO_FAULT）；不能把空目录覆盖当作掉盘，因为可能触发新盘初始化。权限实例可使用本例非root UID +精确目录ACL拒绝，在相同UID实测EACCES；root服务单纯chmod不算有效注入。恢复后重新验证mount/磁盘身份和读写可达；在 namespace 内 helper 的跨mount拒绝是正确保护，不应关闭它。

<a id="REPLACE"></a>

### 新逻辑盘替换

停止本例unit及写入，校验 ROOT/data 与owner/run/mount；将**精确 data 根**同盘移至本例 quarantine 的唯一 old-disk 目录，新建同位置空data根。其他盘不动。重启后读取format，确认新disk UUID与slot；格式初始化失败要如实记录，不手动补出一个UUID。

```bash
# 仅已审核的本例 ROOT；先验证备份目标不存在，不递归删除任何目录。
sudo mv -n -- "$ROOT/data" "$ROOT/quarantine/$FAULT_ID-old-disk"
sudo install -d -m 0700 -- "$ROOT/data"
```

检查旧根确实移走、摘要清单完整、服务无法访问quarantine；再正常启动。rebuild期间不得从old-disk复制part。失败救援时先停止unit并把新失败data根移至另一个保留目录，再恢复旧根；该恢复明确为rescue，不是heal结果。真实设备detach/attach仅属于另行批准的H6，不由此命令模拟。

<a id="PROCESS"></a>
<a id="UPGRADE"></a>

### 进程停止、崩溃、重启、升级

```bash
sudo systemctl show "$HEAL_UNIT" -p MainPID -p ExecMainStartTimestampMonotonic -p FragmentPath -p ActiveState
sudo systemctl stop "$HEAL_UNIT"
sudo systemctl start "$HEAL_UNIT"
# 独立 crash 实例，必须先 WINDOW、记录被杀的实际MainPID：
sudo systemctl kill --kill-whom=main --signal=SIGKILL "$HEAL_UNIT"
sudo systemctl show "$HEAL_UNIT" -p ExecMainCode -p ExecMainStatus -p Result
sudo systemctl reset-failed "$HEAL_UNIT"
sudo systemctl start "$HEAL_UNIT"
```

每次操作前核对 MainPID/exe 摘要/starttime，拒绝0或已替换PID，不 `pkill rustfs`。保存 `/proc/sys/kernel/random/boot_id`，SIGKILL前后boot_id相同，不能说断电。

UPGRADE 对本例节点逐一stop，用已经验证的第二个optimized release二进制替换该unit的exec路径（保留旧二进制），原env/endpoint/data不变，start后核对每个peer实际版本。旧新两份构建清单、包sha、库格式兼容表都须预注册；未知格式禁止直接回滚，改测安全拒绝。精确升级期间责任按CHECKPOINT验证，不依赖版本字符串判断兼容。

<a id="NETWORK"></a>

### 测试端口分区 / 依赖故障

优先预分配专用net namespace，证明它与host不同；需要业务节点连接的路由已配置，SSH仍走host。只对本例API/RPC端口或本例KMS/tier endpoint加规则。先保存规则/路由，设置独立超时恢复任务。例：

```bash
sudo nsenter -t "$HEAL_PID" -n -- iptables -w -I OUTPUT 1 \
  -d "$TEST_PEER_IP" -p tcp --dport "$TEST_SERVICE_PORT" \
  -m comment --comment "$FAULT_ID" -j DROP
sudo nsenter -t "$HEAL_PID" -n -- iptables -w -nvL OUTPUT --line-numbers
# 相同namespace、完整相同规则删除，不flush任何链：
sudo nsenter -t "$HEAL_PID" -n -- iptables -w -D OUTPUT \
  -d "$TEST_PEER_IP" -p tcp --dport "$TEST_SERVICE_PORT" \
  -m comment --comment "$FAULT_ID" -j DROP
```

双向分区分别在绑定双方namespace应用，不能扩大到22、管理平面或host OUTPUT。记录丢包计数与双端探针。无独立net namespace或专项host规则批准时该实例BLOCKED，不能全局断网凑结果。

<a id="IO_FAULT"></a>
<a id="QUOTA"></a>
<a id="LOCK_WINDOW"></a>

### 慢IO/EIO/容量与锁窗口

此组**只在预分配独占故障盘/有硬上限的专用loop设备**操作，绝不对 `/dev/nvme*` 共享生产设备运行。操作前记录backing file、loop/device-mapper名字、major:minor、mount UUID、上限和唯一FAULT_ID。设备创建/format需要本批单独的资源批准，不是case默认授权。

已分配专用 `HEAL_DM_NAME` / `HEAL_BACKING_DEV` 的device-mapper配方：

```bash
sudo dmsetup table "$HEAL_DM_NAME"
sudo dmsetup suspend "$HEAL_DM_NAME"
sudo dmsetup reload "$HEAL_DM_NAME" --table "0 $HEAL_SECTORS delay $HEAL_BACKING_DEV 0 200"
sudo dmsetup resume "$HEAL_DM_NAME"
# EIO实例用完全返回error的表；只用于专用故障层：
sudo dmsetup suspend "$HEAL_DM_NAME"
sudo dmsetup reload "$HEAL_DM_NAME" --table "0 $HEAL_SECTORS error"
sudo dmsetup resume "$HEAL_DM_NAME"
```

恢复reload预先保存的精确原table，不猜linear偏移；在同服务namespace做定长读写见证（记录耗时/errno），并验证源设备可用。块设备EIO不等同应用层短读，短读实例使用FAULT truncate和真实读错误证据；若需“无内容变化的短读”必须有已分配的faultfs后端，另记能力条件。

ENOSPC仅写本例**硬上限卷**中的占位文件，先 `findmnt`/`df -B1` 和 inode计数确认不会耗尽外层XFS；按上限分块填充至返回ENOSPC并记录实际服务写失败。inode实例用创建空文件耗尽该卷的inode预算，不能在共享根创建无限文件。释放只删除已列举的本例占位文件或回收专用卷，不使用宽目录递归删除。

桶配额使用冻结rc的 `quota --help`，对本例桶 `quota set`/`quota info`；前后查询证明限额真实生效，结束恢复原配置。禁止把客户端本地限速当服务器quota。

```bash
"$RC_BIN" --json --no-color quota info "$RC_ALIAS/$B"
"$RC_BIN" --json --no-color quota set "$RC_ALIAS/$B" 64M
# 只在原配置没有quota且本例结束时清除；否则恢复原限额：
"$RC_BIN" --json --no-color quota clear "$RC_ALIAS/$B"
```

LOCK_WINDOW：慢IO层作用于一个实际持有对象写锁的请求，另发同key heal/PUT。用日志/原生runtime fence证明锁获取、超时、调用方退出、在途提交退出顺序；没有可观察锁窗口则黑盒实例为NOT_OBSERVED。客户端断开可由有界 `timeout` 结束**本次请求进程**，不杀服务；之后继续采集服务端在途状态。精确所有权证明依赖NATIVE，不能从一次超时推导锁已释放。

<a id="HOST_POWER"></a>

### 强制停机

只对H6已批准的精确Azure资源ID执行，先核验该VM独占、没有其他服务、磁盘持久化策略及带外恢复；记录当前boot_id和平台状态：

```bash
az vm get-instance-view --ids "$APPROVED_EXCLUSIVE_VM_ID"
az vm stop --ids "$APPROVED_EXCLUSIVE_VM_ID" --skip-shutdown
az vm start --ids "$APPROVED_EXCLUSIVE_VM_ID"
az monitor activity-log list --resource-id "$APPROVED_EXCLUSIVE_VM_ID" \
  --start-time "$RUN_STARTED_AT" --end-time "$RUN_FINISHED_AT"
```

这是真实VM强制停止，不证明物理控制器掉电。物理主机power-cycle必须使用该独占机器已批准的带外控制台，记录BMC事件和电源周期；不能给未知机型编造通用命令。无授权不执行，也不把SIGKILL替代成通过。

## 6. 并发、Pool、ILM、复制及原生证据

<a id="WORKLOAD"></a>
<a id="METRICS"></a>
<a id="LOGS"></a>
<a id="PERFORMANCE"></a>

### 有界负载与观测

`trace.json` 是按序的S3操作数组，例如：

```json
[
  {"operation":"put_object","params":{"Bucket":"rv-heal-20260910-a01","Key":"race-${worker}"},"size":5243003,"expectHttp":200},
  {"operation":"head_object","params":{"Bucket":"rv-heal-20260910-a01","Key":"race-${worker}"},"expectHttp":200}
]
```

```bash
hw writer run --trace trace.json --rounds 100 --workers 2 --interval 0.1 --duration 300
```

同worker串行；不同worker用不同key可明确先后，测试真正同key竞争时人工按ACK/不确定结果建立期望。`${round}`、`${worker}` 只做JSON字符串替换，不eval。上传内容由run/worker/round/step确定，大小/摘要写入ACK。每次ACK立即独占写文件；意外错误停止后续入场并保留在途结果。不因服务返回错误而无限重试。

长稳分支增加已批准 `--duration` 至86400/259200、足够rounds及限速，并核对实际elapsed，提前耗尽rounds/字节预算不算跑满窗口；每批最多1,000,000请求/16worker/64GiB写入预算，可按预注册多个分段运行并保留连续时序。不可通过事后拼接零散短跑冒充连续24h。

Admin队列压力使用同样的限量并发方式调用hp（例如shell后台最多4个固定任务），先冻结请求集合，独立label保存每个返回；不能把S3负载当Admin admission覆盖。

```bash
# 在目标节点按固定频率采样，时间窗口与本例一致；不要采集全环境凭据。
sudo journalctl -u "$HEAL_UNIT" --since "$RUN_STARTED_AT" --until "$RUN_FINISHED_AT" --no-pager
sudo systemctl show "$HEAL_UNIT" -p MemoryCurrent -p TasksCurrent -p CPUUsageNSec
pidstat -dru -p "$HEAL_PID" 1 60
iostat -dx 1 60
```

同时记录FD数量、目标IO、队列/持久记录及临时空间；工具未安装先预检阻断所需性能证据，不跑完才省略。性能ABBA每组四轮、至少三组：warmup、并发、内容、缓存/初始队列、负载窗口和阈值固定。分别统计GET/PUT P95/P99、错误率（所有失败/超时都在总分母）、修复吞吐、CPU/IO。没有预注册门限只能出测量报告，不能记性能通过。

<a id="POOL"></a>

### Pool交互

使用**本例H5服务**的rc alias，已有13项Pool回归结果仅为前置控制，不直接调用其会reset旧fleet的部署脚本。扩容/移除拓扑复用已审阅的endpoint分组方案，但根目录替换为本例heal命名空间。所有poolId从实际status和format映射获得。

```bash
"$RC_BIN" --json --no-color admin rebalance start "$RC_ALIAS"
"$RC_BIN" --json --no-color admin rebalance status "$RC_ALIAS"
"$RC_BIN" --json --no-color admin rebalance stop "$RC_ALIAS"
"$RC_BIN" --json --no-color admin decommission start "$RC_ALIAS" "$POOL_ID" --by-id
"$RC_BIN" --json --no-color admin decommission status "$RC_ALIAS"
"$RC_BIN" --json --no-color admin decommission cancel "$RC_ALIAS" "$POOL_ID" --by-id
"$RC_BIN" --json --no-color admin decommission clear "$RC_ALIAS" "$POOL_ID" --by-id
```

不是把以上所有操作依次跑完：按H01–H06操作卡在相应窗口执行。每次保存rc stdout/stderr/exitCode、相关Admin状态及前后manifest。退役写入限制沿用既定产品契约；仅survivor验证时源必须真实停机且配置移除。#2422/#2423的临界点是否实际触发单独登记。

<a id="ILM"></a>
<a id="REPLICATION"></a>

### ILM、远端tier、复制

使用本仓库已实现lane作为**控制实验**核对候选功能，并在本例独立服务重新建立专用桶/远端租户。配置JSON只含本例资源，API操作如下：

```bash
hp ilm-put s3 --operation put_bucket_lifecycle_configuration --params-file lifecycle.json
hp ilm-get s3 --operation get_bucket_lifecycle_configuration --params-file bucket.json
hp restore s3 --operation restore_object --params-file restore-version.json --expect-http 202
hp replication-put s3 --operation put_bucket_replication --params-file replication.json
hp replication-get s3 --operation get_bucket_replication --params-file bucket.json
```

`lifecycle.json` 使用已验证候选的真实期限；规则ID含run，Filter仅本例prefix，分别配置Expiration/NoncurrentVersionExpiration/Transition。不要用未合并的分钟级测试hook冒充release。远端tier的初始化依照当前rc `ilm tier add --help` 和已有 `scripts/validation_ilm/ilm_behavior.py` 的实际调用，填写本例endpoint/bucket/region与受控凭据；原始凭据不归档。

复制配置含本例role/destination ARN、Filter和DeleteMarkerReplication；先证明对端版本映射/删除语义。resync使用本批rc `replicate resync --help` 所支持的实际命令，保存启动ID和两端状态。观察真实重叠窗口；没有tier/第二端，相关分支BLOCKED而非删除该项。结束通过API删除本例lifecycle/replication配置，远端本例数据按版本清单清理，不改共享服务规则。

已核对参考rc的resync操作（仍须新批help核验）：

```bash
"$RC_BIN" --json --no-color replicate resync start "$RC_ALIAS/$B" \
  --target-arn "$TEST_TARGET_ARN" --reset-id "$RUN" --yes
"$RC_BIN" --json --no-color replicate resync status "$RC_ALIAS/$B" --target-arn "$TEST_TARGET_ARN"
```

<a id="NATIVE"></a>

### 精确屏障与原生回归补证

F9类窗口不能靠普通release黑盒保证命中。使用同源码SHA的**独立native回归二进制**，绝不换掉被测optimized release服务。先列出真实测试并固定完整测试名、crate、源码位置和预期屏障事件：

```bash
cargo test -p rustfs-heal -- --list
cargo test -p rustfs-ecstore -- --list
cargo test -p e2e_test -- --list
# NATIVE_TEST必须逐字来自上面列表，日志必须显示恰好1项且屏障命中：
cargo test -p "$NATIVE_CRATE" "$NATIVE_TEST" -- --exact --nocapture
```

已有replacement回归函数包含 `test_cluster_root_heal_recovers_remote_shards_after_background_target_restart`、`..._crash`、EC8+4和multi-set版本，完整限定名从该commit --list取得。它们默认创建本地E2E临时目录，**不是远端真实独立盘部署器**；不通过环境变量把这些会清理临时目录的测试指到现有fleet。原生锁/代次/checkpoint测试按所选操作卡在同commit源码中找到精确回归和屏障标记后运行，不存在对应屏障的分支要保留“需源侧增加确定性测试”的能力缺口，不能给计划生成器喂手写JSON声称measured。

归档 candidate SHA、native测试源码SHA、完整命令、测试数量、exitCode、运行时间及屏障事件。native与fleet分别计分；缺失精确native证据不影响已经完成的黑盒分支真实性，但不能宣布该case所有分支齐全。

## 7. 恢复、报告和安全交还

<a id="RECOVER"></a>

先保存出错时的状态、日志和PHYSICAL，停止本例负载/任务/进程；再按fault receipt撤销规则/只读层/替换根。对已失效lease允许受限恢复，不允许继续注入。单文件恢复：

```bash
hf recover --fault-id C02-data-middle --service-quiescent
```

只恢复同run的备份，原件摘要不符则拒绝；目标如果已经是原始正确字节不重写；如果变成既不是原始也不是注入后字节（可能有新ACK或合法metadata重写），拒绝盲目覆盖，交人工保留现场。恢复工具输出 `physicalRepairPassAllowed=false`；任何 `restoredFromBackup=true` 都不能作为产品补片证据。

强制故障设置第二条恢复通道/超时撤销；本地 finally 不是网络或进程被杀时可靠的唯一恢复办法。退出前枚举本例网络规则、bind层、dm表、测试unit、临时IAM和未完成MPU，逐条撤销并复核。保留失败数据/backup，retention未到期则保留专用资源；记录剩余路径/到期安排。未恢复为 DIRTY/QUARANTINED，不继续下一用例。

每个**实际参数实例**都填写人工结论和证据索引，不只填一个基础case总绿：

```text
preflight/  基础设施/包/rc/harness/lease/拓扑/预注册参数
baseline/   S3版本清单、物理基线、ACK、桶配置
fault/      精确plan、intent、原件摘要、故障有效性见证
trigger/    原始Admin/GET/后台来源及活动窗口
control/    token/epoch/coverage/counter/负向错误/游标原始响应
physical/   after/restart分片、metadata映射、独立重建对照
s3/         所有应存/应删版本与ACK解析、完整内容和配置
recovery/   故障解除、停止服务、retained路径与安全交还
logs/       全部相关节点的有界日志/资源/平台或native事件
```

人工结论为 MANUAL_PASS/FAIL/INFRA_ERROR/BLOCKED/NOT_OBSERVED，列评审人、每条断言及引用。`evidence.json`包含runId、instanceId、九类evidence的数组，每个引用为相对于该实例目录的`path`和`sha256`。全部分支完成才能评审基础case；缺资源/窗口未命中/证据不全都不是PASS。

```bash
python scripts/validation_heal/matrix.py audit --plan reports/validation-heal-84-plan.json \
  --evidence-dir reports/heal-84 --out reports/heal-84-evidence-audit.json
```

audit检查完整性/路径/摘要，输出`EVIDENCE_COMPLETE_REVIEW_REQUIRED`仍不是产品通过报告。原始失败批次永久保留，修复后新批次另记。本文提供全84项的可操作方案，但实际机器、专用依赖和native屏障的可用性必须单独核验，不能把“方案就绪”改写成“环境就绪”或“已实测”。


自动正向夹具固定使用独立分片承诺配置；`integrity-fleet-attestation` 记录本例 endpoint/节点/候选摘要，启动前逐节点 reserve 校验候选，启动后实际进程环境读回两项开关。此配置只作用于 run-owned 服务。默认 legacy 配置的失败批次独立保留；保护模式不代表旧版本混部或默认环境能力。D07 的条件请求以服务器 HEAD 的 LastModified 和基线 ETag 冻结预期，不改主机时间；304/412/416 不能包含成功 payload 或未知传输结果。
