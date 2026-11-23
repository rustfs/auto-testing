# 严格的 SSE 契约用例

[English](sse-contract-cases.md)

`scripts/sse_contract_cases.py` 里有十九条用例，陈述客户端在服务端加密上必须观察到什么。集群套件把它们登记在原有加密用例旁边，结果走同一套 `[PASS]`、`[FAIL]` 行和用例表。

## “严格”指什么

- **响应头恰好相符。** 把响应归约为以 `x-amz-server-side-encryption` 开头的那些头，这个集合必须与期望完全相等：不能少，也不能多。SSE-S3 是 `{AES256}`，SSE-KMS 是 `aws:kms` 加上请求解析到的密钥 ID，SSE-C 是客户算法加密钥 MD5，明文对象一个都没有。
- **拒绝必须确切。** 拒绝必须是期望的 HTTP 状态码和 S3 错误码，不使用 `400|InvalidArgument` 这类模式匹配。
- **不留残余。** 被拒绝的写之后，对同一个键做 HEAD，必须返回 404。
- **字节。** 读回的数据按 SHA-256 和 `Content-Length` 比较。
- **不允许空过。** 一条没有评估任何期望的用例判为失败。

表驱动用例会在失败行里写出是哪一个输入行；某一行出错不会让后面的行不执行。

## `rustfs-s3-compat-test.sh` 里的用例

这个套件的服务端设置了 `RUSTFS_SSE_S3_MASTER_KEY`，没有 KMS。`ENC-204` 依赖这一点：KMS 在运行时 SSE-KMS 会被接受，这条用例就会失败。

| 用例 | 名称 | 成立条件 |
| --- | --- | --- |
| `ENC-201` | SseS3ExactHeaders | 0、1、71680 字节的 SSE-S3 对象，PUT、GET、HEAD 的响应恰好只带 `x-amz-server-side-encryption: AES256`，读回字节一致，列举结果给出明文大小。 |
| `ENC-202` | SseCExactHeaders | 0、1、71680 字节的 SSE-C 对象，PUT、GET、HEAD 的响应恰好只带客户算法头与密钥 MD5 头；不带客户密钥的 GET 以 400 InvalidRequest 拒绝。 |
| `ENC-203` | BucketDefaultExact | 桶默认 AES256 会加密未指明方案的 PUT，SSE-C 的 PUT 仍是 SSE-C；删除默认配置后新对象不带任何 SSE 头，之前的对象仍按 SSE-S3 读回。 |
| `ENC-204` | SseKmsNotDowngraded | 没有 KMS 时，SSE-KMS 的 PUT 以 400 InvalidRequest 拒绝且不落盘，主密钥下的 SSE-S3 仍能往返。 |
| `ENC-205` | SseCTripleNegatives | 七种不完整或自相矛盾的 SSE-C 三元组，各自以 400 InvalidRequest 拒绝，随后对该键的 HEAD 返回 404。 |
| `ENC-206` | SseCCopySourceKey | 从 SSE-C 源复制时，不带或带错源密钥都以明确的 400 拒绝且不落盘；带对源密钥时，目标对象只能用它自己的密钥打开。 |
| `ENC-207` | SseCMultipartExact | SSE-C 多段上传的 CreateMultipartUpload 与 CompleteMultipartUpload 响应恰好只带客户头，带密钥读回字节一致，不带密钥被拒绝。 |
| `ENC-208` | SseCPartKeyMismatch | UploadPart 不带密钥、带另一把密钥、或对明文上传带密钥，均以 400 InvalidRequest 拒绝；之后这次 SSE-C 上传仍能完成。 |
| `ENC-209` | SseReadHeaderMismatch | 六种读请求头与对象落盘方式不符的 GET，各以确切的错误码拒绝，且不返回对象字节。 |
| `ENC-210` | ListingEtagNotPlaintextMd5 | ListObjectsV2、ListObjects、ListObjectVersions 都不会把明文的 MD5 作为 SSE-S3 或 SSE-C 对象的 ETag，以一个明文对象作对照。 |

## `rustfs-kms-test.sh` 里的用例

这个套件的服务端运行 KMS，默认密钥为 `kms-default`，后端是 Local 或 Vault KV2。辅助脚本通过 `--kms-default-key` 得到默认密钥 ID，并在用例开始前经 admin 接口创建 `sse-contract-key-1` 与 `sse-contract-key-2`；已存在的密钥直接复用。这些用例排在 `KMS-318` 之后、按密钥授权与故障类用例之前。

| 用例 | 名称 | 成立条件 |
| --- | --- | --- |
| `KMS-601` | SseS3KeyNotNamed | KMS 下的 SSE-S3 恰好只带 AES256 并能往返；八种操作的响应都不带 KMS 密钥 ID 头，也不出现默认密钥的名字。 |
| `KMS-602` | SseKmsKeyResolutionExact | 不带密钥 ID 的 SSE-KMS 响应给出默认密钥 ID；显式指定非默认密钥时，0、1、71680 字节对象的 PUT、GET、HEAD 都给出该密钥 ID。 |
| `KMS-603` | SseKmsRangeExact | SSE-KMS 对象的四种范围读返回 206、确切的 Content-Range、对应的明文片段，以及恰好的 SSE-KMS 响应头。 |
| `KMS-604` | BucketDefaultKmsExact | 桶默认 aws:kms 且不带密钥 ID 时固定为默认密钥，SSE-C 仍是 SSE-C；显式 AES256 的 PUT 不继承桶的 KMS 密钥。 |
| `KMS-605` | CopySchemeMatrix | 明文、SSE-S3、SSE-KMS、SSE-C 之间的六种 CopyObject 组合，目标对象的 GET 与 HEAD 恰好是目标方案的响应头，数据字节一致。 |
| `KMS-606` | MultipartSseKmsExact | 三段的 SSE-KMS 上传在创建、完成、GET、HEAD 上恰好带该密钥的 SSE-KMS 响应头，长度完整，ETag 为三段形式。 |
| `KMS-607` | SsePutHeaderNegatives | 十种畸形的 SSE 请求头组合以 400 InvalidArgument 拒绝，不存在的 KMS 密钥以 400 KMS.NotFoundException 拒绝；每个键的 HEAD 返回 404。 |
| `KMS-608` | SseReadHeaderMismatch | 七种读请求头与对象落盘方式不符的 GET（含一个 SSE-KMS 对象），各以确切的错误码拒绝，且不返回对象字节。 |
| `KMS-609` | ContextAndEtagNotEchoed | 带加密上下文写入的对象，其 PUT、GET、HEAD 响应不回显上下文；任何列举都不把明文 MD5 作为加密对象的 ETag。 |

## 单独运行一条

```bash
./rustfs-s3-compat-test.sh --topology single-single -y --cases ENC-204 --package-url <deb 地址>
./rustfs-kms-test.sh --topology single-single --backends local -y --cases KMS-605 --package-url <deb 地址>

RUSTFS_ACCESS_KEY=... RUSTFS_SECRET_KEY=... \
  python3 scripts/sse_contract_cases.py --endpoint http://node:9000 --case KMS-605 --kms-default-key kms-default
```

辅助脚本需要 Python 3.8 或更新版本，只用标准库。它直接连接 endpoint，不理会代理环境变量。每条用例在名为 `sse-<用例编号>-<随机串>` 的桶里工作，结束后尽力清理。退出码：0 表示用例成立，1 表示不成立（endpoint 连不上也算），64 表示命令行有误。从不使用 2，因为 `run_test` 把它读作 UNSUPPORTED。

目录里的编号是 `s3.compatibility.enc-2xx` 和 `source-procedure.kms.kms-6xx`，所以 `--validation-case` 选单条用例的方式与原有用例相同。

## 已经验证了什么

- 每条用例都在本机单进程服务端上成立，服务端用对应套件写入的环境变量启动：`ENC-2xx` 是仅主密钥；`KMS-6xx` 是 Local 与 Vault KV2 两种 KMS，并另外加上 `RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true` 与 `RUSTFS_ENCRYPTION_FRAME_V2=true` 跑过。
- 用两个套件自己的 `run_all_tests` 对这些服务端跑过这些用例，输出了预期的 `[PASS]` 行；对没有主密钥的服务端，`ENC-201` 与 `ENC-204` 输出 `[FAIL]` 并写出它们看到的拒绝。
- `test_sse_contract_cases.py` 让每条用例对一个行为符合要求的内存版 S3 跑一遍，再每次改错一个行为跑一遍；对应的用例必须失败并指出它看到了什么。

这些用例还没有在机群上跑过。多节点拓扑和打包后的二进制，要等定时套件带上这些用例之后才算覆盖。

## 有意没有搬过来的部分

这些用例取自一个驱动裸服务端二进制的套件。其中有些内容需要已部署集群给不了的条件，因此没有搬：用别的密钥来源启动服务端（完全没有、非法主密钥、Vault Transit、Static 后端）、两个二进制的比较、检查对象在盘上的落盘形态、在整次运行里查找密钥材料。使用不同身份的按密钥授权目前由 `KMS-401`、`KMS-402` 宽松覆盖，这里没有收紧。
