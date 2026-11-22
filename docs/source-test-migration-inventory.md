# Source test migration inventory
This inventory is generated from the 2025-04-30 source audit. It separates behavior procedures, local compatibility procedures, native implementation tests, benchmarks, and CI automation. The behavior migration catalog is maintained in `validation-cases.json`.
## Snapshot totals
- 18 workflow entries
- 243 Go test files
- 1,081 `Test*` entries
- 872 benchmark entries
- 0 fuzz entries
## Migration status vocabulary
- `mapped-procedure` means the behavior has a target executor or local procedure entry.
- `partial-procedure` means part of the behavior is represented and an external dependency or matrix remains.
- `inventory-only-native-unit` means the source test is an implementation-level unit test that requires a native product test adapter for a one-to-one port.
- `ci-only` means the source item is build, lint, security, or repository automation rather than an S3/Heal behavior case.
## Workflows
| Source entry | Category | Target status | Target coverage |
| --- | --- | --- | --- |
| `.github/workflows/go-lint.yml` | 单元测试 + lint | `mapped-procedure` | local contract suite and source-style checks |
| `.github/workflows/go.yml` | 功能测试 | `mapped-procedure` | S3 compatibility and configured-endpoint suites |
| `.github/workflows/go-healing.yml` | Healing 功能测试 | `mapped-procedure` | Heal and availability suites |
| `.github/workflows/go-resiliency.yml` | Resiliency 功能测试 | `mapped-procedure` | Heal corruption and recovery suites |
| `.github/workflows/external-s3-suite.yml` | external-s3-suite（外部 S3 套件） | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/replication.yaml` | 高级功能测试 | `mapped-procedure` | pool, lifecycle, replication, IAM and object suites |
| `.github/workflows/iam-integrations.yaml` | IAM 集成测试 | `partial-procedure` | identity and policy suites, external provider matrix pending |
| `.github/workflows/go-cross.yml` | 构建验证 | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/go-fips.yml` | 构建验证 / FIPS | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/root-disable.yml` | 功能测试 | `mapped-procedure` | identity and root-policy regression suites |
| `.github/workflows/upgrade-ci-cd.yaml` | 升级测试 | `mapped-procedure` | upgrade suite |
| `.github/workflows/helm-lint.yml` | Lint | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/shfmt.yml` | Lint | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/typos.yml` | Lint | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/vulncheck.yml` | 安全扫描 | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/depsreview.yaml` | 安全扫描 | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/issues.yaml` | 非测试自动化 | `ci-only` | CI/build/lint/security automation |
| `.github/workflows/lock.yml` | 非测试自动化 | `ci-only` | CI/build/lint/security automation |

## Scripts and CI helpers
| Source entry | Target status | Target coverage |
| --- | --- | --- |
| `Makefile` | `mapped-procedure` | catalog dispatch and suite runners |
| `buildscripts/race.sh` | `ci-only` | workflow or build support |
| `buildscripts/verify-build.sh` | `ci-only` | workflow or build support |
| `buildscripts/verify-healing.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `buildscripts/verify-healing-empty-erasure-set.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `buildscripts/heal-inconsistent-versions.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `buildscripts/verify-healing-with-root-disks.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `buildscripts/rewrite-old-new.sh` | `ci-only` | workflow or build support |
| `buildscripts/resolve-right-versions.sh` | `ci-only` | workflow or build support |
| `docs/resiliency/resiliency-tests.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `docs/resiliency/resiliency-verify-healing-script.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `docs/distributed/decom.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `docs/bucket/lifecycle/setup_ilm_transition.sh` | `mapped-procedure` | Heal, ILM, pool, or tier executor |
| `.github/workflows/run-external-s3-suite.sh` | `ci-only` | workflow or build support |
| `docs/bucket/replication/setup_replication.sh` | `mapped-local-procedure` | replication executor and compatibility catalog |
| `docs/site-replication/run-replication-with-checksum-header.sh` | `mapped-local-procedure` | replication executor and compatibility catalog |
| `internal/config/scanner/scanner.go` | `ci-only` | workflow or build support |

## Test files and functions
| Section | Source file | Test entries | Benchmarks | Target status | Target coverage |
| --- | --- | ---: | ---: | --- | --- |
| Data Scanner / 用量缓存 | `cmd/data-scanner_test.go` | 2 | 0 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Data Scanner / 用量缓存 | `cmd/data-usage-cache_gen_test.go` | 17 | 43 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Data Scanner / 用量缓存 | `cmd/data-usage-cache_test.go` | 2 | 0 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Data Scanner / 用量缓存 | `cmd/data-usage_test.go` | 3 | 0 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Healing | `cmd/background-newdisks-heal-ops_gen_test.go` | 2 | 5 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Healing | `cmd/erasure-heal_test.go` | 1 | 0 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Healing | `cmd/erasure-healing-common_test.go` | 5 | 0 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Healing | `cmd/erasure-healing_test.go` | 11 | 0 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| Bitrot | `cmd/bitrot_test.go` | 1 | 0 | `mapped-procedure` | scanner, ILM, Heal, and availability executors |
| S3 Select（非 data scanner） | `internal/s3select/jstream/scanner_test.go` | 2 | 3 | `mapped-local-procedure` | compatibility and S3 Select procedure entries |
| 基准测试文件 | `internal/grid/benchmark_test.go` | 0 | 2 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 基准测试文件 | `internal/ringbuffer/ring_buffer_benchmark_test.go` | 0 | 6 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 基准测试文件 | `internal/s3select/select_benchmark_test.go` | 0 | 12 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/batch-expire_gen_test.go` | 6 | 15 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/batch-job-common-types_gen_test.go` | 10 | 25 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/batch-replicate_gen_test.go` | 12 | 30 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/batch-rotate_gen_test.go` | 10 | 25 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/bootstrap-peer-server_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/bucket-metadata_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/bucket-replication-metrics_gen_test.go` | 16 | 40 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/bucket-replication-utils_gen_test.go` | 20 | 50 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/bucket-stats_gen_test.go` | 18 | 45 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/erasure-server-pool-decom_gen_test.go` | 10 | 25 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/erasure-server-pool-rebalance_gen_test.go` | 10 | 25 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/last-minute_gen_test.go` | 6 | 15 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/local-locker_gen_test.go` | 6 | 15 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/metacache-set_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/metacache-walk_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/metacache_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/metrics-v2_gen_test.go` | 4 | 12 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/mrf_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/site-replication-metrics_gen_test.go` | 12 | 30 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/site-replication-utils_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/storage-datatypes_gen_test.go` | 66 | 165 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/tier-last-day-stats_gen_test.go` | 4 | 10 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/tier_gen_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/xl-storage-format-v1_gen_test.go` | 12 | 30 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `cmd/xl-storage-format-v2_gen_test.go` | 10 | 25 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `internal/bucket/bandwidth/monitor_gen_test.go` | 4 | 10 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `internal/dsync/lock-args_gen_test.go` | 4 | 10 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| 生成的编解码测试 | `internal/grid/msg_gen_test.go` | 12 | 30 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/admin-handlers-users-race_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/admin-handlers-users_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/admin-handlers_test.go` | 5 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/api-errors_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/api-headers_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/api-resources_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/api-response_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/api-utils_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/auth-handler_test.go` | 7 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/batch-handlers_gen_test.go` | 6 | 15 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/batch-handlers_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/bucket-handlers_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/bucket-lifecycle-handlers_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/bucket-policy-handlers_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/crossdomain-xml-handler_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/generic-handlers_test.go` | 4 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/handler-utils_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/jwt_test.go` | 1 | 3 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/kms-handlers_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/lock-rest-client_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/lock-rest-server-common_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-api-deleteobject_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-api-getobjectinfo_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-api-listobjects_test.go` | 6 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-api-multipart_test.go` | 9 | 8 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-api-options_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-api-putobject_test.go` | 4 | 30 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-api-utils_test.go` | 14 | 4 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-handlers-common_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object-handlers_test.go` | 19 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/object_api_suite_test.go` | 16 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/storage-rest-common_gen_test.go` | 4 | 10 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/storage-rest_test.go` | 8 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| API / Handler 测试 | `cmd/sts-handlers_test.go` | 12 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/batch-expire_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/batch-job-common-types_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/batch-replicate_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/bucket-encryption_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/bucket-lifecycle_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/bucket-replication-utils_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/bucket-replication_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/common-main_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/config-current_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/config-encrypted_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/copy-part-range_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/dummy-data-generator_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/dynamic-timeouts_test.go` | 9 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/encryption-v1_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/endpoint-ellipses_test.go` | 5 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/endpoint_contrib_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/endpoint_test.go` | 5 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure-decode_test.go` | 2 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure-encode_test.go` | 1 | 5 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure-metadata-utils_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure-metadata_test.go` | 7 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure-object_test.go` | 16 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure-server-pool-decom_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure-sets_test.go` | 4 | 2 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/erasure_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/format-erasure_test.go` | 6 | 6 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/http-tracer_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/httprange_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/iam-etcd-store_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/iam-object-store_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/local-locker_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/metacache-bucket_test.go` | 0 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/metacache-entries_test.go` | 11 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/metacache-stream_test.go` | 11 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/metacache_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/metrics-v2_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/namespace-lock_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/net_test.go` | 9 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/os-readdir_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/os-reliable_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/policy_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/post-policy_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/postpolicyform_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/server-main_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/server-startup-msg_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/server_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/sftp-server_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/signature-v2_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/signature-v4-parser_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/signature-v4-utils_test.go` | 7 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/signature-v4_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/site-replication_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/storage-datatypes_test.go` | 0 | 9 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/streaming-signature-v4_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/test-utils_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/tier_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/update-notifier_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/update_test.go` | 9 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/url_test.go` | 0 | 2 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/utils_test.go` | 13 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/version_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage-errors_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage-format-utils_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage-format-v2_test.go` | 10 | 4 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage-format_test.go` | 5 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage-free-version_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage_test.go` | 23 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage_unix_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| cmd 单元测试 | `cmd/xl-storage_windows_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/amztime/iso8601_time_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/amztime/parse_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/arn/arn_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/auth/credentials_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bpool/bpool_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/bandwidth/monitor_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/encryption/bucket-sse-config_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/delmarker-expiration_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/evaluator_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/expiration_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/filter_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/lifecycle_test.go` | 14 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/noncurrentversion_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/rule_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/lifecycle/transition_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/object/lock/lock_test.go` | 13 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/replication/replication_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/replication/rule_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/bucket/versioning/versioning_test.go` | 5 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/cachevalue/cache_test.go` | 2 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/bool-flag_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/certs_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/compress/compress_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/config_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/crypto_test.go` | 1 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/dns/etcd_dns_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/etcd/etcd_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/identity/openid/jwks_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/identity/openid/jwt_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/lambda/event/arn_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/lambda/event/targetid_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/lambda/event/targetidset_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/config/storageclass/storage-class_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/crypto/header_test.go` | 10 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/crypto/key_test.go` | 5 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/crypto/metadata_test.go` | 11 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/crypto/sse_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/deadlineconn/deadlineconn_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/disk/disk_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/disk/stat_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/dsync/drwmutex_test.go` | 10 | 4 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/dsync/dsync_test.go` | 10 | 7 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/etag/etag_test.go` | 9 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/arn_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/config_test.go` | 13 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/name_test.go` | 7 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/rules_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/rulesmap_test.go` | 5 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/target/mysql_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/target/nats_contrib_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/target/nats_tls_contrib_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/target/nsq_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/target/postgresql_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/targetid_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/targetidset_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/event/targetlist_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/grid/connection_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/grid/grid_test.go` | 5 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/grid/types_test.go` | 4 | 3 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/handlers/proxy_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/hash/reader_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/http/check_port_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/http/listener_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/http/server_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/ioutil/ioutil_test.go` | 7 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/jwt/parser_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/kms/config_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/kms/dek_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/kms/secret-key_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/lock/lock_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/lock/lock_windows_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/lsync/lrwmutex_test.go` | 11 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/mountinfo/mountinfo_linux_test.go` | 4 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/pubsub/pubsub_test.go` | 7 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/rest/client_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/ringbuffer/ring_buffer_test.go` | 10 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/csv/reader_contrib_test.go` | 3 | 4 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/json/preader_test.go` | 1 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/json/reader_test.go` | 1 | 1 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/jstream/decoder_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/select_test.go` | 10 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/simdj/reader_amd64_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/sql/jsonpath_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/sql/parser_test.go` | 13 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/sql/stringfuncs_contrib_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/sql/stringfuncs_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/sql/timestampfuncs_test.go` | 1 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/s3select/sql/value_test.go` | 6 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/store/batch_test.go` | 3 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/store/queuestore_test.go` | 8 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |
| internal 单元测试 | `internal/store/store_test.go` | 2 | 0 | `inventory-only-native-unit` | requires a native implementation-test adapter for direct parity |

## Function-level index
The source audit lists the following function entries. Names are preserved as audit identifiers, with vendor-specific names normalized in this document.
### `cmd/data-scanner_test.go`
- Tests: `TestApplyNewerNoncurrentVersionsLimit`, `TestEvalActionFromLifecycle`
### `cmd/data-usage-cache_gen_test.go`
- Tests: `TestMarshalUnmarshalallTierStats`, `TestEncodeDecodeallTierStats`, `TestMarshalUnmarshalcurrentScannerCycle`, `TestMarshalUnmarshaldataUsageCache`, `TestEncodeDecodedataUsageCache`, `TestMarshalUnmarshaldataUsageCacheInfo`, `TestEncodeDecodedataUsageCacheInfo`, `TestMarshalUnmarshaldataUsageEntry`, `TestEncodeDecodedataUsageEntry`, `TestMarshalUnmarshalsizeHistogram`, `TestEncodeDecodesizeHistogram`, `TestMarshalUnmarshalsizeHistogramV1`, `TestEncodeDecodesizeHistogramV1`, `TestMarshalUnmarshaltierStats`, `TestEncodeDecodetierStats`, `TestMarshalUnmarshalversionsHistogram`, `TestEncodeDecodeversionsHistogram`
- Benchmarks: `BenchmarkMarshalMsgallTierStats`, `BenchmarkAppendMsgallTierStats`, `BenchmarkUnmarshalallTierStats`, `BenchmarkEncodeallTierStats`, `BenchmarkDecodeallTierStats`, `BenchmarkMarshalMsgcurrentScannerCycle`, `BenchmarkAppendMsgcurrentScannerCycle`, `BenchmarkUnmarshalcurrentScannerCycle`, `BenchmarkMarshalMsgdataUsageCache`, `BenchmarkAppendMsgdataUsageCache`, `BenchmarkUnmarshaldataUsageCache`, `BenchmarkEncodedataUsageCache`, `BenchmarkDecodedataUsageCache`, `BenchmarkMarshalMsgdataUsageCacheInfo`, `BenchmarkAppendMsgdataUsageCacheInfo`, `BenchmarkUnmarshaldataUsageCacheInfo`, `BenchmarkEncodedataUsageCacheInfo`, `BenchmarkDecodedataUsageCacheInfo`, `BenchmarkMarshalMsgdataUsageEntry`, `BenchmarkAppendMsgdataUsageEntry`, `BenchmarkUnmarshaldataUsageEntry`, `BenchmarkEncodedataUsageEntry`, `BenchmarkDecodedataUsageEntry`, `BenchmarkMarshalMsgsizeHistogram`, `BenchmarkAppendMsgsizeHistogram`, `BenchmarkUnmarshalsizeHistogram`, `BenchmarkEncodesizeHistogram`, `BenchmarkDecodesizeHistogram`, `BenchmarkMarshalMsgsizeHistogramV1`, `BenchmarkAppendMsgsizeHistogramV1`, `BenchmarkUnmarshalsizeHistogramV1`, `BenchmarkEncodesizeHistogramV1`, `BenchmarkDecodesizeHistogramV1`, `BenchmarkMarshalMsgtierStats`, `BenchmarkAppendMsgtierStats`, `BenchmarkUnmarshaltierStats`, `BenchmarkEncodetierStats`, `BenchmarkDecodetierStats`, `BenchmarkMarshalMsgversionsHistogram`, `BenchmarkAppendMsgversionsHistogram`, `BenchmarkUnmarshalversionsHistogram`, `BenchmarkEncodeversionsHistogram`, `BenchmarkDecodeversionsHistogram`
### `cmd/data-usage-cache_test.go`
- Tests: `TestSizeHistogramToMap`, `TestMigrateSizeHistogramFromV1`
### `cmd/data-usage_test.go`
- Tests: `TestDataUsageUpdate`, `TestDataUsageUpdatePrefix`, `TestDataUsageCacheSerialize`
### `cmd/background-newdisks-heal-ops_gen_test.go`
- Tests: `TestMarshalUnmarshalhealingTracker`, `TestEncodeDecodehealingTracker`
- Benchmarks: `BenchmarkMarshalMsghealingTracker`, `BenchmarkAppendMsghealingTracker`, `BenchmarkUnmarshalhealingTracker`, `BenchmarkEncodehealingTracker`, `BenchmarkDecodehealingTracker`
### `cmd/erasure-heal_test.go`
- Tests: `TestErasureHeal`
### `cmd/erasure-healing-common_test.go`
- Tests: `TestCommonTime`, `TestListOnlineDisks`, `TestListOnlineDisksSmallObjects`, `TestDisksWithAllParts`, `TestCommonParities`
### `cmd/erasure-healing_test.go`
- Tests: `TestIsObjectDangling`, `TestHealing`, `TestHealingVersioned`, `TestHealingDanglingObject`, `TestHealCorrectQuorum`, `TestHealObjectCorruptedPools`, `TestHealObjectCorruptedXLMeta`, `TestHealObjectCorruptedParts`, `TestHealObjectErasure`, `TestHealEmptyDirectoryErasure`, `TestHealLastDataShard`
### `cmd/bitrot_test.go`
- Tests: `TestAllBitrotAlgorithms`
### `internal/s3select/jstream/scanner_test.go`
- Tests: `TestScanner`, `TestScannerFailure`
- Benchmarks: `BenchmarkBufioScanner`, `BenchmarkBufioReader`, `BenchmarkScanner`
- Benchmarks: `BenchmarkRequests`, `BenchmarkStream`
- Benchmarks: `BenchmarkRingBuffer_Sync`, `BenchmarkRingBuffer_AsyncRead`, `BenchmarkRingBuffer_AsyncReadBlocking`, `BenchmarkRingBuffer_AsyncWrite`, `BenchmarkRingBuffer_AsyncWriteBlocking`, `BenchmarkIoPipeReader`
- Benchmarks: `BenchmarkSelectAll_100K`, `BenchmarkSelectAll_1M`, `BenchmarkSelectAll_2M`, `BenchmarkSelectAll_10M`, `BenchmarkSingleCol_100K`, `BenchmarkSingleCol_1M`, `BenchmarkSingleCol_2M`, `BenchmarkSingleCol_10M`, `BenchmarkAggregateCount_100K`, `BenchmarkAggregateCount_1M`, `BenchmarkAggregateCount_2M`, `BenchmarkAggregateCount_10M`
### `cmd/batch-expire_gen_test.go`
- Tests: `TestMarshalUnmarshalBatchJobExpire`, `TestEncodeDecodeBatchJobExpire`, `TestMarshalUnmarshalBatchJobExpireFilter`, `TestEncodeDecodeBatchJobExpireFilter`, `TestMarshalUnmarshalBatchJobExpirePurge`, `TestEncodeDecodeBatchJobExpirePurge`
- Benchmarks: `BenchmarkMarshalMsgBatchJobExpire`, `BenchmarkAppendMsgBatchJobExpire`, `BenchmarkUnmarshalBatchJobExpire`, `BenchmarkEncodeBatchJobExpire`, `BenchmarkDecodeBatchJobExpire`, `BenchmarkMarshalMsgBatchJobExpireFilter`, `BenchmarkAppendMsgBatchJobExpireFilter`, `BenchmarkUnmarshalBatchJobExpireFilter`, `BenchmarkEncodeBatchJobExpireFilter`, `BenchmarkDecodeBatchJobExpireFilter`, `BenchmarkMarshalMsgBatchJobExpirePurge`, `BenchmarkAppendMsgBatchJobExpirePurge`, `BenchmarkUnmarshalBatchJobExpirePurge`, `BenchmarkEncodeBatchJobExpirePurge`, `BenchmarkDecodeBatchJobExpirePurge`
### `cmd/batch-job-common-types_gen_test.go`
- Tests: `TestMarshalUnmarshalBatchJobKV`, `TestEncodeDecodeBatchJobKV`, `TestMarshalUnmarshalBatchJobNotification`, `TestEncodeDecodeBatchJobNotification`, `TestMarshalUnmarshalBatchJobRetry`, `TestEncodeDecodeBatchJobRetry`, `TestMarshalUnmarshalBatchJobSizeFilter`, `TestEncodeDecodeBatchJobSizeFilter`, `TestMarshalUnmarshalBatchJobSnowball`, `TestEncodeDecodeBatchJobSnowball`
- Benchmarks: `BenchmarkMarshalMsgBatchJobKV`, `BenchmarkAppendMsgBatchJobKV`, `BenchmarkUnmarshalBatchJobKV`, `BenchmarkEncodeBatchJobKV`, `BenchmarkDecodeBatchJobKV`, `BenchmarkMarshalMsgBatchJobNotification`, `BenchmarkAppendMsgBatchJobNotification`, `BenchmarkUnmarshalBatchJobNotification`, `BenchmarkEncodeBatchJobNotification`, `BenchmarkDecodeBatchJobNotification`, `BenchmarkMarshalMsgBatchJobRetry`, `BenchmarkAppendMsgBatchJobRetry`, `BenchmarkUnmarshalBatchJobRetry`, `BenchmarkEncodeBatchJobRetry`, `BenchmarkDecodeBatchJobRetry`, `BenchmarkMarshalMsgBatchJobSizeFilter`, `BenchmarkAppendMsgBatchJobSizeFilter`, `BenchmarkUnmarshalBatchJobSizeFilter`, `BenchmarkEncodeBatchJobSizeFilter`, `BenchmarkDecodeBatchJobSizeFilter`, `BenchmarkMarshalMsgBatchJobSnowball`, `BenchmarkAppendMsgBatchJobSnowball`, `BenchmarkUnmarshalBatchJobSnowball`, `BenchmarkEncodeBatchJobSnowball`, `BenchmarkDecodeBatchJobSnowball`
### `cmd/batch-replicate_gen_test.go`
- Tests: `TestMarshalUnmarshalBatchJobReplicateCredentials`, `TestEncodeDecodeBatchJobReplicateCredentials`, `TestMarshalUnmarshalBatchJobReplicateFlags`, `TestEncodeDecodeBatchJobReplicateFlags`, `TestMarshalUnmarshalBatchJobReplicateSource`, `TestEncodeDecodeBatchJobReplicateSource`, `TestMarshalUnmarshalBatchJobReplicateTarget`, `TestEncodeDecodeBatchJobReplicateTarget`, `TestMarshalUnmarshalBatchJobReplicateV1`, `TestEncodeDecodeBatchJobReplicateV1`, `TestMarshalUnmarshalBatchReplicateFilter`, `TestEncodeDecodeBatchReplicateFilter`
- Benchmarks: `BenchmarkMarshalMsgBatchJobReplicateCredentials`, `BenchmarkAppendMsgBatchJobReplicateCredentials`, `BenchmarkUnmarshalBatchJobReplicateCredentials`, `BenchmarkEncodeBatchJobReplicateCredentials`, `BenchmarkDecodeBatchJobReplicateCredentials`, `BenchmarkMarshalMsgBatchJobReplicateFlags`, `BenchmarkAppendMsgBatchJobReplicateFlags`, `BenchmarkUnmarshalBatchJobReplicateFlags`, `BenchmarkEncodeBatchJobReplicateFlags`, `BenchmarkDecodeBatchJobReplicateFlags`, `BenchmarkMarshalMsgBatchJobReplicateSource`, `BenchmarkAppendMsgBatchJobReplicateSource`, `BenchmarkUnmarshalBatchJobReplicateSource`, `BenchmarkEncodeBatchJobReplicateSource`, `BenchmarkDecodeBatchJobReplicateSource`, `BenchmarkMarshalMsgBatchJobReplicateTarget`, `BenchmarkAppendMsgBatchJobReplicateTarget`, `BenchmarkUnmarshalBatchJobReplicateTarget`, `BenchmarkEncodeBatchJobReplicateTarget`, `BenchmarkDecodeBatchJobReplicateTarget`, `BenchmarkMarshalMsgBatchJobReplicateV1`, `BenchmarkAppendMsgBatchJobReplicateV1`, `BenchmarkUnmarshalBatchJobReplicateV1`, `BenchmarkEncodeBatchJobReplicateV1`, `BenchmarkDecodeBatchJobReplicateV1`, `BenchmarkMarshalMsgBatchReplicateFilter`, `BenchmarkAppendMsgBatchReplicateFilter`, `BenchmarkUnmarshalBatchReplicateFilter`, `BenchmarkEncodeBatchReplicateFilter`, `BenchmarkDecodeBatchReplicateFilter`
### `cmd/batch-rotate_gen_test.go`
- Tests: `TestMarshalUnmarshalBatchJobKeyRotateEncryption`, `TestEncodeDecodeBatchJobKeyRotateEncryption`, `TestMarshalUnmarshalBatchJobKeyRotateFlags`, `TestEncodeDecodeBatchJobKeyRotateFlags`, `TestMarshalUnmarshalBatchJobKeyRotateV1`, `TestEncodeDecodeBatchJobKeyRotateV1`, `TestMarshalUnmarshalBatchKeyRotateFilter`, `TestEncodeDecodeBatchKeyRotateFilter`, `TestMarshalUnmarshalBatchKeyRotateNotification`, `TestEncodeDecodeBatchKeyRotateNotification`
- Benchmarks: `BenchmarkMarshalMsgBatchJobKeyRotateEncryption`, `BenchmarkAppendMsgBatchJobKeyRotateEncryption`, `BenchmarkUnmarshalBatchJobKeyRotateEncryption`, `BenchmarkEncodeBatchJobKeyRotateEncryption`, `BenchmarkDecodeBatchJobKeyRotateEncryption`, `BenchmarkMarshalMsgBatchJobKeyRotateFlags`, `BenchmarkAppendMsgBatchJobKeyRotateFlags`, `BenchmarkUnmarshalBatchJobKeyRotateFlags`, `BenchmarkEncodeBatchJobKeyRotateFlags`, `BenchmarkDecodeBatchJobKeyRotateFlags`, `BenchmarkMarshalMsgBatchJobKeyRotateV1`, `BenchmarkAppendMsgBatchJobKeyRotateV1`, `BenchmarkUnmarshalBatchJobKeyRotateV1`, `BenchmarkEncodeBatchJobKeyRotateV1`, `BenchmarkDecodeBatchJobKeyRotateV1`, `BenchmarkMarshalMsgBatchKeyRotateFilter`, `BenchmarkAppendMsgBatchKeyRotateFilter`, `BenchmarkUnmarshalBatchKeyRotateFilter`, `BenchmarkEncodeBatchKeyRotateFilter`, `BenchmarkDecodeBatchKeyRotateFilter`, `BenchmarkMarshalMsgBatchKeyRotateNotification`, `BenchmarkAppendMsgBatchKeyRotateNotification`, `BenchmarkUnmarshalBatchKeyRotateNotification`, `BenchmarkEncodeBatchKeyRotateNotification`, `BenchmarkDecodeBatchKeyRotateNotification`
### `cmd/bootstrap-peer-server_gen_test.go`
- Tests: `TestMarshalUnmarshalServerSystemConfig`, `TestEncodeDecodeServerSystemConfig`
- Benchmarks: `BenchmarkMarshalMsgServerSystemConfig`, `BenchmarkAppendMsgServerSystemConfig`, `BenchmarkUnmarshalServerSystemConfig`, `BenchmarkEncodeServerSystemConfig`, `BenchmarkDecodeServerSystemConfig`
### `cmd/bucket-metadata_gen_test.go`
- Tests: `TestMarshalUnmarshalBucketMetadata`, `TestEncodeDecodeBucketMetadata`
- Benchmarks: `BenchmarkMarshalMsgBucketMetadata`, `BenchmarkAppendMsgBucketMetadata`, `BenchmarkUnmarshalBucketMetadata`, `BenchmarkEncodeBucketMetadata`, `BenchmarkDecodeBucketMetadata`
### `cmd/bucket-replication-metrics_gen_test.go`
- Tests: `TestMarshalUnmarshalActiveWorkerStat`, `TestEncodeDecodeActiveWorkerStat`, `TestMarshalUnmarshalInQueueMetric`, `TestEncodeDecodeInQueueMetric`, `TestMarshalUnmarshalInQueueStats`, `TestEncodeDecodeInQueueStats`, `TestMarshalUnmarshalProxyMetric`, `TestEncodeDecodeProxyMetric`, `TestMarshalUnmarshalQStat`, `TestEncodeDecodeQStat`, `TestMarshalUnmarshalReplicationMRFStats`, `TestEncodeDecodeReplicationMRFStats`, `TestMarshalUnmarshalSMA`, `TestEncodeDecodeSMA`, `TestMarshalUnmarshalXferStats`, `TestEncodeDecodeXferStats`
- Benchmarks: `BenchmarkMarshalMsgActiveWorkerStat`, `BenchmarkAppendMsgActiveWorkerStat`, `BenchmarkUnmarshalActiveWorkerStat`, `BenchmarkEncodeActiveWorkerStat`, `BenchmarkDecodeActiveWorkerStat`, `BenchmarkMarshalMsgInQueueMetric`, `BenchmarkAppendMsgInQueueMetric`, `BenchmarkUnmarshalInQueueMetric`, `BenchmarkEncodeInQueueMetric`, `BenchmarkDecodeInQueueMetric`, `BenchmarkMarshalMsgInQueueStats`, `BenchmarkAppendMsgInQueueStats`, `BenchmarkUnmarshalInQueueStats`, `BenchmarkEncodeInQueueStats`, `BenchmarkDecodeInQueueStats`, `BenchmarkMarshalMsgProxyMetric`, `BenchmarkAppendMsgProxyMetric`, `BenchmarkUnmarshalProxyMetric`, `BenchmarkEncodeProxyMetric`, `BenchmarkDecodeProxyMetric`, `BenchmarkMarshalMsgQStat`, `BenchmarkAppendMsgQStat`, `BenchmarkUnmarshalQStat`, `BenchmarkEncodeQStat`, `BenchmarkDecodeQStat`, `BenchmarkMarshalMsgReplicationMRFStats`, `BenchmarkAppendMsgReplicationMRFStats`, `BenchmarkUnmarshalReplicationMRFStats`, `BenchmarkEncodeReplicationMRFStats`, `BenchmarkDecodeReplicationMRFStats`, `BenchmarkMarshalMsgSMA`, `BenchmarkAppendMsgSMA`, `BenchmarkUnmarshalSMA`, `BenchmarkEncodeSMA`, `BenchmarkDecodeSMA`, `BenchmarkMarshalMsgXferStats`, `BenchmarkAppendMsgXferStats`, `BenchmarkUnmarshalXferStats`, `BenchmarkEncodeXferStats`, `BenchmarkDecodeXferStats`
### `cmd/bucket-replication-utils_gen_test.go`
- Tests: `TestMarshalUnmarshalBucketReplicationResyncStatus`, `TestEncodeDecodeBucketReplicationResyncStatus`, `TestMarshalUnmarshalMRFReplicateEntries`, `TestEncodeDecodeMRFReplicateEntries`, `TestMarshalUnmarshalMRFReplicateEntry`, `TestEncodeDecodeMRFReplicateEntry`, `TestMarshalUnmarshalReplicateDecision`, `TestEncodeDecodeReplicateDecision`, `TestMarshalUnmarshalReplicationState`, `TestEncodeDecodeReplicationState`, `TestMarshalUnmarshalResyncDecision`, `TestEncodeDecodeResyncDecision`, `TestMarshalUnmarshalResyncTarget`, `TestEncodeDecodeResyncTarget`, `TestMarshalUnmarshalResyncTargetDecision`, `TestEncodeDecodeResyncTargetDecision`, `TestMarshalUnmarshalResyncTargetsInfo`, `TestEncodeDecodeResyncTargetsInfo`, `TestMarshalUnmarshalTargetReplicationResyncStatus`, `TestEncodeDecodeTargetReplicationResyncStatus`
- Benchmarks: `BenchmarkMarshalMsgBucketReplicationResyncStatus`, `BenchmarkAppendMsgBucketReplicationResyncStatus`, `BenchmarkUnmarshalBucketReplicationResyncStatus`, `BenchmarkEncodeBucketReplicationResyncStatus`, `BenchmarkDecodeBucketReplicationResyncStatus`, `BenchmarkMarshalMsgMRFReplicateEntries`, `BenchmarkAppendMsgMRFReplicateEntries`, `BenchmarkUnmarshalMRFReplicateEntries`, `BenchmarkEncodeMRFReplicateEntries`, `BenchmarkDecodeMRFReplicateEntries`, `BenchmarkMarshalMsgMRFReplicateEntry`, `BenchmarkAppendMsgMRFReplicateEntry`, `BenchmarkUnmarshalMRFReplicateEntry`, `BenchmarkEncodeMRFReplicateEntry`, `BenchmarkDecodeMRFReplicateEntry`, `BenchmarkMarshalMsgReplicateDecision`, `BenchmarkAppendMsgReplicateDecision`, `BenchmarkUnmarshalReplicateDecision`, `BenchmarkEncodeReplicateDecision`, `BenchmarkDecodeReplicateDecision`, `BenchmarkMarshalMsgReplicationState`, `BenchmarkAppendMsgReplicationState`, `BenchmarkUnmarshalReplicationState`, `BenchmarkEncodeReplicationState`, `BenchmarkDecodeReplicationState`, `BenchmarkMarshalMsgResyncDecision`, `BenchmarkAppendMsgResyncDecision`, `BenchmarkUnmarshalResyncDecision`, `BenchmarkEncodeResyncDecision`, `BenchmarkDecodeResyncDecision`, `BenchmarkMarshalMsgResyncTarget`, `BenchmarkAppendMsgResyncTarget`, `BenchmarkUnmarshalResyncTarget`, `BenchmarkEncodeResyncTarget`, `BenchmarkDecodeResyncTarget`, `BenchmarkMarshalMsgResyncTargetDecision`, `BenchmarkAppendMsgResyncTargetDecision`, `BenchmarkUnmarshalResyncTargetDecision`, `BenchmarkEncodeResyncTargetDecision`, `BenchmarkDecodeResyncTargetDecision`, `BenchmarkMarshalMsgResyncTargetsInfo`, `BenchmarkAppendMsgResyncTargetsInfo`, `BenchmarkUnmarshalResyncTargetsInfo`, `BenchmarkEncodeResyncTargetsInfo`, `BenchmarkDecodeResyncTargetsInfo`, `BenchmarkMarshalMsgTargetReplicationResyncStatus`, `BenchmarkAppendMsgTargetReplicationResyncStatus`, `BenchmarkUnmarshalTargetReplicationResyncStatus`, `BenchmarkEncodeTargetReplicationResyncStatus`, `BenchmarkDecodeTargetReplicationResyncStatus`
### `cmd/bucket-stats_gen_test.go`
- Tests: `TestMarshalUnmarshalBucketReplicationStat`, `TestEncodeDecodeBucketReplicationStat`, `TestMarshalUnmarshalBucketReplicationStats`, `TestEncodeDecodeBucketReplicationStats`, `TestMarshalUnmarshalBucketStats`, `TestEncodeDecodeBucketStats`, `TestMarshalUnmarshalBucketStatsMap`, `TestEncodeDecodeBucketStatsMap`, `TestMarshalUnmarshalReplQNodeStats`, `TestEncodeDecodeReplQNodeStats`, `TestMarshalUnmarshalReplicationLastHour`, `TestEncodeDecodeReplicationLastHour`, `TestMarshalUnmarshalReplicationLastMinute`, `TestEncodeDecodeReplicationLastMinute`, `TestMarshalUnmarshalReplicationLatency`, `TestEncodeDecodeReplicationLatency`, `TestMarshalUnmarshalReplicationQueueStats`, `TestEncodeDecodeReplicationQueueStats`
- Benchmarks: `BenchmarkMarshalMsgBucketReplicationStat`, `BenchmarkAppendMsgBucketReplicationStat`, `BenchmarkUnmarshalBucketReplicationStat`, `BenchmarkEncodeBucketReplicationStat`, `BenchmarkDecodeBucketReplicationStat`, `BenchmarkMarshalMsgBucketReplicationStats`, `BenchmarkAppendMsgBucketReplicationStats`, `BenchmarkUnmarshalBucketReplicationStats`, `BenchmarkEncodeBucketReplicationStats`, `BenchmarkDecodeBucketReplicationStats`, `BenchmarkMarshalMsgBucketStats`, `BenchmarkAppendMsgBucketStats`, `BenchmarkUnmarshalBucketStats`, `BenchmarkEncodeBucketStats`, `BenchmarkDecodeBucketStats`, `BenchmarkMarshalMsgBucketStatsMap`, `BenchmarkAppendMsgBucketStatsMap`, `BenchmarkUnmarshalBucketStatsMap`, `BenchmarkEncodeBucketStatsMap`, `BenchmarkDecodeBucketStatsMap`, `BenchmarkMarshalMsgReplQNodeStats`, `BenchmarkAppendMsgReplQNodeStats`, `BenchmarkUnmarshalReplQNodeStats`, `BenchmarkEncodeReplQNodeStats`, `BenchmarkDecodeReplQNodeStats`, `BenchmarkMarshalMsgReplicationLastHour`, `BenchmarkAppendMsgReplicationLastHour`, `BenchmarkUnmarshalReplicationLastHour`, `BenchmarkEncodeReplicationLastHour`, `BenchmarkDecodeReplicationLastHour`, `BenchmarkMarshalMsgReplicationLastMinute`, `BenchmarkAppendMsgReplicationLastMinute`, `BenchmarkUnmarshalReplicationLastMinute`, `BenchmarkEncodeReplicationLastMinute`, `BenchmarkDecodeReplicationLastMinute`, `BenchmarkMarshalMsgReplicationLatency`, `BenchmarkAppendMsgReplicationLatency`, `BenchmarkUnmarshalReplicationLatency`, `BenchmarkEncodeReplicationLatency`, `BenchmarkDecodeReplicationLatency`, `BenchmarkMarshalMsgReplicationQueueStats`, `BenchmarkAppendMsgReplicationQueueStats`, `BenchmarkUnmarshalReplicationQueueStats`, `BenchmarkEncodeReplicationQueueStats`, `BenchmarkDecodeReplicationQueueStats`
### `cmd/erasure-server-pool-decom_gen_test.go`
- Tests: `TestMarshalUnmarshalPoolDecommissionInfo`, `TestEncodeDecodePoolDecommissionInfo`, `TestMarshalUnmarshalPoolStatus`, `TestEncodeDecodePoolStatus`, `TestMarshalUnmarshaldecomError`, `TestEncodeDecodedecomError`, `TestMarshalUnmarshalpoolMeta`, `TestEncodeDecodepoolMeta`, `TestMarshalUnmarshalpoolSpaceInfo`, `TestEncodeDecodepoolSpaceInfo`
- Benchmarks: `BenchmarkMarshalMsgPoolDecommissionInfo`, `BenchmarkAppendMsgPoolDecommissionInfo`, `BenchmarkUnmarshalPoolDecommissionInfo`, `BenchmarkEncodePoolDecommissionInfo`, `BenchmarkDecodePoolDecommissionInfo`, `BenchmarkMarshalMsgPoolStatus`, `BenchmarkAppendMsgPoolStatus`, `BenchmarkUnmarshalPoolStatus`, `BenchmarkEncodePoolStatus`, `BenchmarkDecodePoolStatus`, `BenchmarkMarshalMsgdecomError`, `BenchmarkAppendMsgdecomError`, `BenchmarkUnmarshaldecomError`, `BenchmarkEncodedecomError`, `BenchmarkDecodedecomError`, `BenchmarkMarshalMsgpoolMeta`, `BenchmarkAppendMsgpoolMeta`, `BenchmarkUnmarshalpoolMeta`, `BenchmarkEncodepoolMeta`, `BenchmarkDecodepoolMeta`, `BenchmarkMarshalMsgpoolSpaceInfo`, `BenchmarkAppendMsgpoolSpaceInfo`, `BenchmarkUnmarshalpoolSpaceInfo`, `BenchmarkEncodepoolSpaceInfo`, `BenchmarkDecodepoolSpaceInfo`
### `cmd/erasure-server-pool-rebalance_gen_test.go`
- Tests: `TestMarshalUnmarshalrebalanceInfo`, `TestEncodeDecoderebalanceInfo`, `TestMarshalUnmarshalrebalanceMeta`, `TestEncodeDecoderebalanceMeta`, `TestMarshalUnmarshalrebalanceMetrics`, `TestEncodeDecoderebalanceMetrics`, `TestMarshalUnmarshalrebalanceStats`, `TestEncodeDecoderebalanceStats`, `TestMarshalUnmarshalrstats`, `TestEncodeDecoderstats`
- Benchmarks: `BenchmarkMarshalMsgrebalanceInfo`, `BenchmarkAppendMsgrebalanceInfo`, `BenchmarkUnmarshalrebalanceInfo`, `BenchmarkEncoderebalanceInfo`, `BenchmarkDecoderebalanceInfo`, `BenchmarkMarshalMsgrebalanceMeta`, `BenchmarkAppendMsgrebalanceMeta`, `BenchmarkUnmarshalrebalanceMeta`, `BenchmarkEncoderebalanceMeta`, `BenchmarkDecoderebalanceMeta`, `BenchmarkMarshalMsgrebalanceMetrics`, `BenchmarkAppendMsgrebalanceMetrics`, `BenchmarkUnmarshalrebalanceMetrics`, `BenchmarkEncoderebalanceMetrics`, `BenchmarkDecoderebalanceMetrics`, `BenchmarkMarshalMsgrebalanceStats`, `BenchmarkAppendMsgrebalanceStats`, `BenchmarkUnmarshalrebalanceStats`, `BenchmarkEncoderebalanceStats`, `BenchmarkDecoderebalanceStats`, `BenchmarkMarshalMsgrstats`, `BenchmarkAppendMsgrstats`, `BenchmarkUnmarshalrstats`, `BenchmarkEncoderstats`, `BenchmarkDecoderstats`
### `cmd/last-minute_gen_test.go`
- Tests: `TestMarshalUnmarshalAccElem`, `TestEncodeDecodeAccElem`, `TestMarshalUnmarshalLastMinuteHistogram`, `TestEncodeDecodeLastMinuteHistogram`, `TestMarshalUnmarshallastMinuteLatency`, `TestEncodeDecodelastMinuteLatency`
- Benchmarks: `BenchmarkMarshalMsgAccElem`, `BenchmarkAppendMsgAccElem`, `BenchmarkUnmarshalAccElem`, `BenchmarkEncodeAccElem`, `BenchmarkDecodeAccElem`, `BenchmarkMarshalMsgLastMinuteHistogram`, `BenchmarkAppendMsgLastMinuteHistogram`, `BenchmarkUnmarshalLastMinuteHistogram`, `BenchmarkEncodeLastMinuteHistogram`, `BenchmarkDecodeLastMinuteHistogram`, `BenchmarkMarshalMsglastMinuteLatency`, `BenchmarkAppendMsglastMinuteLatency`, `BenchmarkUnmarshallastMinuteLatency`, `BenchmarkEncodelastMinuteLatency`, `BenchmarkDecodelastMinuteLatency`
### `cmd/local-locker_gen_test.go`
- Tests: `TestMarshalUnmarshallocalLockMap`, `TestEncodeDecodelocalLockMap`, `TestMarshalUnmarshallockRequesterInfo`, `TestEncodeDecodelockRequesterInfo`, `TestMarshalUnmarshallockStats`, `TestEncodeDecodelockStats`
- Benchmarks: `BenchmarkMarshalMsglocalLockMap`, `BenchmarkAppendMsglocalLockMap`, `BenchmarkUnmarshallocalLockMap`, `BenchmarkEncodelocalLockMap`, `BenchmarkDecodelocalLockMap`, `BenchmarkMarshalMsglockRequesterInfo`, `BenchmarkAppendMsglockRequesterInfo`, `BenchmarkUnmarshallockRequesterInfo`, `BenchmarkEncodelockRequesterInfo`, `BenchmarkDecodelockRequesterInfo`, `BenchmarkMarshalMsglockStats`, `BenchmarkAppendMsglockStats`, `BenchmarkUnmarshallockStats`, `BenchmarkEncodelockStats`, `BenchmarkDecodelockStats`
### `cmd/metacache-set_gen_test.go`
- Tests: `TestMarshalUnmarshallistPathOptions`, `TestEncodeDecodelistPathOptions`
- Benchmarks: `BenchmarkMarshalMsglistPathOptions`, `BenchmarkAppendMsglistPathOptions`, `BenchmarkUnmarshallistPathOptions`, `BenchmarkEncodelistPathOptions`, `BenchmarkDecodelistPathOptions`
### `cmd/metacache-walk_gen_test.go`
- Tests: `TestMarshalUnmarshalWalkDirOptions`, `TestEncodeDecodeWalkDirOptions`
- Benchmarks: `BenchmarkMarshalMsgWalkDirOptions`, `BenchmarkAppendMsgWalkDirOptions`, `BenchmarkUnmarshalWalkDirOptions`, `BenchmarkEncodeWalkDirOptions`, `BenchmarkDecodeWalkDirOptions`
### `cmd/metacache_gen_test.go`
- Tests: `TestMarshalUnmarshalmetacache`, `TestEncodeDecodemetacache`
- Benchmarks: `BenchmarkMarshalMsgmetacache`, `BenchmarkAppendMsgmetacache`, `BenchmarkUnmarshalmetacache`, `BenchmarkEncodemetacache`, `BenchmarkDecodemetacache`
### `cmd/metrics-v2_gen_test.go`
- Tests: `TestMarshalUnmarshalMetricDescription`, `TestMarshalUnmarshalMetricV2`, `TestMarshalUnmarshalMetricsGroupOpts`, `TestMarshalUnmarshalMetricsGroupV2`
- Benchmarks: `BenchmarkMarshalMsgMetricDescription`, `BenchmarkAppendMsgMetricDescription`, `BenchmarkUnmarshalMetricDescription`, `BenchmarkMarshalMsgMetricV2`, `BenchmarkAppendMsgMetricV2`, `BenchmarkUnmarshalMetricV2`, `BenchmarkMarshalMsgMetricsGroupOpts`, `BenchmarkAppendMsgMetricsGroupOpts`, `BenchmarkUnmarshalMetricsGroupOpts`, `BenchmarkMarshalMsgMetricsGroupV2`, `BenchmarkAppendMsgMetricsGroupV2`, `BenchmarkUnmarshalMetricsGroupV2`
### `cmd/mrf_gen_test.go`
- Tests: `TestMarshalUnmarshalPartialOperation`, `TestEncodeDecodePartialOperation`
- Benchmarks: `BenchmarkMarshalMsgPartialOperation`, `BenchmarkAppendMsgPartialOperation`, `BenchmarkUnmarshalPartialOperation`, `BenchmarkEncodePartialOperation`, `BenchmarkDecodePartialOperation`
### `cmd/site-replication-metrics_gen_test.go`
- Tests: `TestMarshalUnmarshalRStat`, `TestEncodeDecodeRStat`, `TestMarshalUnmarshalRTimedMetrics`, `TestEncodeDecodeRTimedMetrics`, `TestMarshalUnmarshalSRMetric`, `TestEncodeDecodeSRMetric`, `TestMarshalUnmarshalSRMetricsSummary`, `TestEncodeDecodeSRMetricsSummary`, `TestMarshalUnmarshalSRStats`, `TestEncodeDecodeSRStats`, `TestMarshalUnmarshalSRStatus`, `TestEncodeDecodeSRStatus`
- Benchmarks: `BenchmarkMarshalMsgRStat`, `BenchmarkAppendMsgRStat`, `BenchmarkUnmarshalRStat`, `BenchmarkEncodeRStat`, `BenchmarkDecodeRStat`, `BenchmarkMarshalMsgRTimedMetrics`, `BenchmarkAppendMsgRTimedMetrics`, `BenchmarkUnmarshalRTimedMetrics`, `BenchmarkEncodeRTimedMetrics`, `BenchmarkDecodeRTimedMetrics`, `BenchmarkMarshalMsgSRMetric`, `BenchmarkAppendMsgSRMetric`, `BenchmarkUnmarshalSRMetric`, `BenchmarkEncodeSRMetric`, `BenchmarkDecodeSRMetric`, `BenchmarkMarshalMsgSRMetricsSummary`, `BenchmarkAppendMsgSRMetricsSummary`, `BenchmarkUnmarshalSRMetricsSummary`, `BenchmarkEncodeSRMetricsSummary`, `BenchmarkDecodeSRMetricsSummary`, `BenchmarkMarshalMsgSRStats`, `BenchmarkAppendMsgSRStats`, `BenchmarkUnmarshalSRStats`, `BenchmarkEncodeSRStats`, `BenchmarkDecodeSRStats`, `BenchmarkMarshalMsgSRStatus`, `BenchmarkAppendMsgSRStatus`, `BenchmarkUnmarshalSRStatus`, `BenchmarkEncodeSRStatus`, `BenchmarkDecodeSRStatus`
### `cmd/site-replication-utils_gen_test.go`
- Tests: `TestMarshalUnmarshalSiteResyncStatus`, `TestEncodeDecodeSiteResyncStatus`
- Benchmarks: `BenchmarkMarshalMsgSiteResyncStatus`, `BenchmarkAppendMsgSiteResyncStatus`, `BenchmarkUnmarshalSiteResyncStatus`, `BenchmarkEncodeSiteResyncStatus`, `BenchmarkDecodeSiteResyncStatus`
### `cmd/storage-datatypes_gen_test.go`
- Tests: `TestMarshalUnmarshalBaseOptions`, `TestEncodeDecodeBaseOptions`, `TestMarshalUnmarshalCheckPartsHandlerParams`, `TestEncodeDecodeCheckPartsHandlerParams`, `TestMarshalUnmarshalCheckPartsResp`, `TestEncodeDecodeCheckPartsResp`, `TestMarshalUnmarshalDeleteBulkReq`, `TestEncodeDecodeDeleteBulkReq`, `TestMarshalUnmarshalDeleteFileHandlerParams`, `TestEncodeDecodeDeleteFileHandlerParams`, `TestMarshalUnmarshalDeleteOptions`, `TestEncodeDecodeDeleteOptions`, `TestMarshalUnmarshalDeleteVersionHandlerParams`, `TestEncodeDecodeDeleteVersionHandlerParams`, `TestMarshalUnmarshalDeleteVersionsErrsResp`, `TestEncodeDecodeDeleteVersionsErrsResp`, `TestMarshalUnmarshalDiskInfo`, `TestEncodeDecodeDiskInfo`, `TestMarshalUnmarshalDiskInfoOptions`, `TestEncodeDecodeDiskInfoOptions`, `TestMarshalUnmarshalDiskMetrics`, `TestEncodeDecodeDiskMetrics`, `TestMarshalUnmarshalFileInfo`, `TestEncodeDecodeFileInfo`, `TestMarshalUnmarshalFileInfoVersions`, `TestEncodeDecodeFileInfoVersions`, `TestMarshalUnmarshalFilesInfo`, `TestEncodeDecodeFilesInfo`, `TestMarshalUnmarshalListDirResult`, `TestEncodeDecodeListDirResult`, `TestMarshalUnmarshalLocalDiskIDs`, `TestEncodeDecodeLocalDiskIDs`, `TestMarshalUnmarshalMetadataHandlerParams`, `TestEncodeDecodeMetadataHandlerParams`, `TestMarshalUnmarshalRawFileInfo`, `TestEncodeDecodeRawFileInfo`, `TestMarshalUnmarshalReadAllHandlerParams`, `TestEncodeDecodeReadAllHandlerParams`, `TestMarshalUnmarshalReadMultipleReq`, `TestEncodeDecodeReadMultipleReq`, `TestMarshalUnmarshalReadMultipleResp`, `TestEncodeDecodeReadMultipleResp`, `TestMarshalUnmarshalReadPartsReq`, `TestEncodeDecodeReadPartsReq`, `TestMarshalUnmarshalReadPartsResp`, `TestEncodeDecodeReadPartsResp`, `TestMarshalUnmarshalRenameDataHandlerParams`, `TestEncodeDecodeRenameDataHandlerParams`, `TestMarshalUnmarshalRenameDataInlineHandlerParams`, `TestEncodeDecodeRenameDataInlineHandlerParams`, `TestMarshalUnmarshalRenameDataResp`, `TestEncodeDecodeRenameDataResp`, `TestMarshalUnmarshalRenameFileHandlerParams`, `TestEncodeDecodeRenameFileHandlerParams`, `TestMarshalUnmarshalRenameOptions`, `TestEncodeDecodeRenameOptions`, `TestMarshalUnmarshalRenamePartHandlerParams`, `TestEncodeDecodeRenamePartHandlerParams`, `TestMarshalUnmarshalUpdateMetadataOpts`, `TestEncodeDecodeUpdateMetadataOpts`, `TestMarshalUnmarshalVolInfo`, `TestEncodeDecodeVolInfo`, `TestMarshalUnmarshalVolsInfo`, `TestEncodeDecodeVolsInfo`, `TestMarshalUnmarshalWriteAllHandlerParams`, `TestEncodeDecodeWriteAllHandlerParams`
- Benchmarks: `BenchmarkMarshalMsgBaseOptions`, `BenchmarkAppendMsgBaseOptions`, `BenchmarkUnmarshalBaseOptions`, `BenchmarkEncodeBaseOptions`, `BenchmarkDecodeBaseOptions`, `BenchmarkMarshalMsgCheckPartsHandlerParams`, `BenchmarkAppendMsgCheckPartsHandlerParams`, `BenchmarkUnmarshalCheckPartsHandlerParams`, `BenchmarkEncodeCheckPartsHandlerParams`, `BenchmarkDecodeCheckPartsHandlerParams`, `BenchmarkMarshalMsgCheckPartsResp`, `BenchmarkAppendMsgCheckPartsResp`, `BenchmarkUnmarshalCheckPartsResp`, `BenchmarkEncodeCheckPartsResp`, `BenchmarkDecodeCheckPartsResp`, `BenchmarkMarshalMsgDeleteBulkReq`, `BenchmarkAppendMsgDeleteBulkReq`, `BenchmarkUnmarshalDeleteBulkReq`, `BenchmarkEncodeDeleteBulkReq`, `BenchmarkDecodeDeleteBulkReq`, `BenchmarkMarshalMsgDeleteFileHandlerParams`, `BenchmarkAppendMsgDeleteFileHandlerParams`, `BenchmarkUnmarshalDeleteFileHandlerParams`, `BenchmarkEncodeDeleteFileHandlerParams`, `BenchmarkDecodeDeleteFileHandlerParams`, `BenchmarkMarshalMsgDeleteOptions`, `BenchmarkAppendMsgDeleteOptions`, `BenchmarkUnmarshalDeleteOptions`, `BenchmarkEncodeDeleteOptions`, `BenchmarkDecodeDeleteOptions`, `BenchmarkMarshalMsgDeleteVersionHandlerParams`, `BenchmarkAppendMsgDeleteVersionHandlerParams`, `BenchmarkUnmarshalDeleteVersionHandlerParams`, `BenchmarkEncodeDeleteVersionHandlerParams`, `BenchmarkDecodeDeleteVersionHandlerParams`, `BenchmarkMarshalMsgDeleteVersionsErrsResp`, `BenchmarkAppendMsgDeleteVersionsErrsResp`, `BenchmarkUnmarshalDeleteVersionsErrsResp`, `BenchmarkEncodeDeleteVersionsErrsResp`, `BenchmarkDecodeDeleteVersionsErrsResp`, `BenchmarkMarshalMsgDiskInfo`, `BenchmarkAppendMsgDiskInfo`, `BenchmarkUnmarshalDiskInfo`, `BenchmarkEncodeDiskInfo`, `BenchmarkDecodeDiskInfo`, `BenchmarkMarshalMsgDiskInfoOptions`, `BenchmarkAppendMsgDiskInfoOptions`, `BenchmarkUnmarshalDiskInfoOptions`, `BenchmarkEncodeDiskInfoOptions`, `BenchmarkDecodeDiskInfoOptions`, `BenchmarkMarshalMsgDiskMetrics`, `BenchmarkAppendMsgDiskMetrics`, `BenchmarkUnmarshalDiskMetrics`, `BenchmarkEncodeDiskMetrics`, `BenchmarkDecodeDiskMetrics`, `BenchmarkMarshalMsgFileInfo`, `BenchmarkAppendMsgFileInfo`, `BenchmarkUnmarshalFileInfo`, `BenchmarkEncodeFileInfo`, `BenchmarkDecodeFileInfo`, `BenchmarkMarshalMsgFileInfoVersions`, `BenchmarkAppendMsgFileInfoVersions`, `BenchmarkUnmarshalFileInfoVersions`, `BenchmarkEncodeFileInfoVersions`, `BenchmarkDecodeFileInfoVersions`, `BenchmarkMarshalMsgFilesInfo`, `BenchmarkAppendMsgFilesInfo`, `BenchmarkUnmarshalFilesInfo`, `BenchmarkEncodeFilesInfo`, `BenchmarkDecodeFilesInfo`, `BenchmarkMarshalMsgListDirResult`, `BenchmarkAppendMsgListDirResult`, `BenchmarkUnmarshalListDirResult`, `BenchmarkEncodeListDirResult`, `BenchmarkDecodeListDirResult`, `BenchmarkMarshalMsgLocalDiskIDs`, `BenchmarkAppendMsgLocalDiskIDs`, `BenchmarkUnmarshalLocalDiskIDs`, `BenchmarkEncodeLocalDiskIDs`, `BenchmarkDecodeLocalDiskIDs`, `BenchmarkMarshalMsgMetadataHandlerParams`, `BenchmarkAppendMsgMetadataHandlerParams`, `BenchmarkUnmarshalMetadataHandlerParams`, `BenchmarkEncodeMetadataHandlerParams`, `BenchmarkDecodeMetadataHandlerParams`, `BenchmarkMarshalMsgRawFileInfo`, `BenchmarkAppendMsgRawFileInfo`, `BenchmarkUnmarshalRawFileInfo`, `BenchmarkEncodeRawFileInfo`, `BenchmarkDecodeRawFileInfo`, `BenchmarkMarshalMsgReadAllHandlerParams`, `BenchmarkAppendMsgReadAllHandlerParams`, `BenchmarkUnmarshalReadAllHandlerParams`, `BenchmarkEncodeReadAllHandlerParams`, `BenchmarkDecodeReadAllHandlerParams`, `BenchmarkMarshalMsgReadMultipleReq`, `BenchmarkAppendMsgReadMultipleReq`, `BenchmarkUnmarshalReadMultipleReq`, `BenchmarkEncodeReadMultipleReq`, `BenchmarkDecodeReadMultipleReq`, `BenchmarkMarshalMsgReadMultipleResp`, `BenchmarkAppendMsgReadMultipleResp`, `BenchmarkUnmarshalReadMultipleResp`, `BenchmarkEncodeReadMultipleResp`, `BenchmarkDecodeReadMultipleResp`, `BenchmarkMarshalMsgReadPartsReq`, `BenchmarkAppendMsgReadPartsReq`, `BenchmarkUnmarshalReadPartsReq`, `BenchmarkEncodeReadPartsReq`, `BenchmarkDecodeReadPartsReq`, `BenchmarkMarshalMsgReadPartsResp`, `BenchmarkAppendMsgReadPartsResp`, `BenchmarkUnmarshalReadPartsResp`, `BenchmarkEncodeReadPartsResp`, `BenchmarkDecodeReadPartsResp`, `BenchmarkMarshalMsgRenameDataHandlerParams`, `BenchmarkAppendMsgRenameDataHandlerParams`, `BenchmarkUnmarshalRenameDataHandlerParams`, `BenchmarkEncodeRenameDataHandlerParams`, `BenchmarkDecodeRenameDataHandlerParams`, `BenchmarkMarshalMsgRenameDataInlineHandlerParams`, `BenchmarkAppendMsgRenameDataInlineHandlerParams`, `BenchmarkUnmarshalRenameDataInlineHandlerParams`, `BenchmarkEncodeRenameDataInlineHandlerParams`, `BenchmarkDecodeRenameDataInlineHandlerParams`, `BenchmarkMarshalMsgRenameDataResp`, `BenchmarkAppendMsgRenameDataResp`, `BenchmarkUnmarshalRenameDataResp`, `BenchmarkEncodeRenameDataResp`, `BenchmarkDecodeRenameDataResp`, `BenchmarkMarshalMsgRenameFileHandlerParams`, `BenchmarkAppendMsgRenameFileHandlerParams`, `BenchmarkUnmarshalRenameFileHandlerParams`, `BenchmarkEncodeRenameFileHandlerParams`, `BenchmarkDecodeRenameFileHandlerParams`, `BenchmarkMarshalMsgRenameOptions`, `BenchmarkAppendMsgRenameOptions`, `BenchmarkUnmarshalRenameOptions`, `BenchmarkEncodeRenameOptions`, `BenchmarkDecodeRenameOptions`, `BenchmarkMarshalMsgRenamePartHandlerParams`, `BenchmarkAppendMsgRenamePartHandlerParams`, `BenchmarkUnmarshalRenamePartHandlerParams`, `BenchmarkEncodeRenamePartHandlerParams`, `BenchmarkDecodeRenamePartHandlerParams`, `BenchmarkMarshalMsgUpdateMetadataOpts`, `BenchmarkAppendMsgUpdateMetadataOpts`, `BenchmarkUnmarshalUpdateMetadataOpts`, `BenchmarkEncodeUpdateMetadataOpts`, `BenchmarkDecodeUpdateMetadataOpts`, `BenchmarkMarshalMsgVolInfo`, `BenchmarkAppendMsgVolInfo`, `BenchmarkUnmarshalVolInfo`, `BenchmarkEncodeVolInfo`, `BenchmarkDecodeVolInfo`, `BenchmarkMarshalMsgVolsInfo`, `BenchmarkAppendMsgVolsInfo`, `BenchmarkUnmarshalVolsInfo`, `BenchmarkEncodeVolsInfo`, `BenchmarkDecodeVolsInfo`, `BenchmarkMarshalMsgWriteAllHandlerParams`, `BenchmarkAppendMsgWriteAllHandlerParams`, `BenchmarkUnmarshalWriteAllHandlerParams`, `BenchmarkEncodeWriteAllHandlerParams`, `BenchmarkDecodeWriteAllHandlerParams`
### `cmd/tier-last-day-stats_gen_test.go`
- Tests: `TestMarshalUnmarshalDailyAllTierStats`, `TestEncodeDecodeDailyAllTierStats`, `TestMarshalUnmarshallastDayTierStats`, `TestEncodeDecodelastDayTierStats`
- Benchmarks: `BenchmarkMarshalMsgDailyAllTierStats`, `BenchmarkAppendMsgDailyAllTierStats`, `BenchmarkUnmarshalDailyAllTierStats`, `BenchmarkEncodeDailyAllTierStats`, `BenchmarkDecodeDailyAllTierStats`, `BenchmarkMarshalMsglastDayTierStats`, `BenchmarkAppendMsglastDayTierStats`, `BenchmarkUnmarshallastDayTierStats`, `BenchmarkEncodelastDayTierStats`, `BenchmarkDecodelastDayTierStats`
### `cmd/tier_gen_test.go`
- Tests: `TestMarshalUnmarshalTierConfigMgr`, `TestEncodeDecodeTierConfigMgr`
- Benchmarks: `BenchmarkMarshalMsgTierConfigMgr`, `BenchmarkAppendMsgTierConfigMgr`, `BenchmarkUnmarshalTierConfigMgr`, `BenchmarkEncodeTierConfigMgr`, `BenchmarkDecodeTierConfigMgr`
### `cmd/xl-storage-format-v1_gen_test.go`
- Tests: `TestMarshalUnmarshalChecksumInfo`, `TestEncodeDecodeChecksumInfo`, `TestMarshalUnmarshalErasureInfo`, `TestEncodeDecodeErasureInfo`, `TestMarshalUnmarshalObjectPartInfo`, `TestEncodeDecodeObjectPartInfo`, `TestMarshalUnmarshalStatInfo`, `TestEncodeDecodeStatInfo`, `TestMarshalUnmarshalchecksumInfoJSON`, `TestEncodeDecodechecksumInfoJSON`, `TestMarshalUnmarshalxlMetaV1Object`, `TestEncodeDecodexlMetaV1Object`
- Benchmarks: `BenchmarkMarshalMsgChecksumInfo`, `BenchmarkAppendMsgChecksumInfo`, `BenchmarkUnmarshalChecksumInfo`, `BenchmarkEncodeChecksumInfo`, `BenchmarkDecodeChecksumInfo`, `BenchmarkMarshalMsgErasureInfo`, `BenchmarkAppendMsgErasureInfo`, `BenchmarkUnmarshalErasureInfo`, `BenchmarkEncodeErasureInfo`, `BenchmarkDecodeErasureInfo`, `BenchmarkMarshalMsgObjectPartInfo`, `BenchmarkAppendMsgObjectPartInfo`, `BenchmarkUnmarshalObjectPartInfo`, `BenchmarkEncodeObjectPartInfo`, `BenchmarkDecodeObjectPartInfo`, `BenchmarkMarshalMsgStatInfo`, `BenchmarkAppendMsgStatInfo`, `BenchmarkUnmarshalStatInfo`, `BenchmarkEncodeStatInfo`, `BenchmarkDecodeStatInfo`, `BenchmarkMarshalMsgchecksumInfoJSON`, `BenchmarkAppendMsgchecksumInfoJSON`, `BenchmarkUnmarshalchecksumInfoJSON`, `BenchmarkEncodechecksumInfoJSON`, `BenchmarkDecodechecksumInfoJSON`, `BenchmarkMarshalMsgxlMetaV1Object`, `BenchmarkAppendMsgxlMetaV1Object`, `BenchmarkUnmarshalxlMetaV1Object`, `BenchmarkEncodexlMetaV1Object`, `BenchmarkDecodexlMetaV1Object`
### `cmd/xl-storage-format-v2_gen_test.go`
- Tests: `TestMarshalUnmarshalxlMetaDataDirDecoder`, `TestEncodeDecodexlMetaDataDirDecoder`, `TestMarshalUnmarshalxlMetaV2DeleteMarker`, `TestEncodeDecodexlMetaV2DeleteMarker`, `TestMarshalUnmarshalxlMetaV2Object`, `TestEncodeDecodexlMetaV2Object`, `TestMarshalUnmarshalxlMetaV2Version`, `TestEncodeDecodexlMetaV2Version`, `TestMarshalUnmarshalxlMetaV2VersionHeader`, `TestEncodeDecodexlMetaV2VersionHeader`
- Benchmarks: `BenchmarkMarshalMsgxlMetaDataDirDecoder`, `BenchmarkAppendMsgxlMetaDataDirDecoder`, `BenchmarkUnmarshalxlMetaDataDirDecoder`, `BenchmarkEncodexlMetaDataDirDecoder`, `BenchmarkDecodexlMetaDataDirDecoder`, `BenchmarkMarshalMsgxlMetaV2DeleteMarker`, `BenchmarkAppendMsgxlMetaV2DeleteMarker`, `BenchmarkUnmarshalxlMetaV2DeleteMarker`, `BenchmarkEncodexlMetaV2DeleteMarker`, `BenchmarkDecodexlMetaV2DeleteMarker`, `BenchmarkMarshalMsgxlMetaV2Object`, `BenchmarkAppendMsgxlMetaV2Object`, `BenchmarkUnmarshalxlMetaV2Object`, `BenchmarkEncodexlMetaV2Object`, `BenchmarkDecodexlMetaV2Object`, `BenchmarkMarshalMsgxlMetaV2Version`, `BenchmarkAppendMsgxlMetaV2Version`, `BenchmarkUnmarshalxlMetaV2Version`, `BenchmarkEncodexlMetaV2Version`, `BenchmarkDecodexlMetaV2Version`, `BenchmarkMarshalMsgxlMetaV2VersionHeader`, `BenchmarkAppendMsgxlMetaV2VersionHeader`, `BenchmarkUnmarshalxlMetaV2VersionHeader`, `BenchmarkEncodexlMetaV2VersionHeader`, `BenchmarkDecodexlMetaV2VersionHeader`
### `internal/bucket/bandwidth/monitor_gen_test.go`
- Tests: `TestMarshalUnmarshalBucketBandwidthReport`, `TestEncodeDecodeBucketBandwidthReport`, `TestMarshalUnmarshalDetails`, `TestEncodeDecodeDetails`
- Benchmarks: `BenchmarkMarshalMsgBucketBandwidthReport`, `BenchmarkAppendMsgBucketBandwidthReport`, `BenchmarkUnmarshalBucketBandwidthReport`, `BenchmarkEncodeBucketBandwidthReport`, `BenchmarkDecodeBucketBandwidthReport`, `BenchmarkMarshalMsgDetails`, `BenchmarkAppendMsgDetails`, `BenchmarkUnmarshalDetails`, `BenchmarkEncodeDetails`, `BenchmarkDecodeDetails`
### `internal/dsync/lock-args_gen_test.go`
- Tests: `TestMarshalUnmarshalLockArgs`, `TestEncodeDecodeLockArgs`, `TestMarshalUnmarshalLockResp`, `TestEncodeDecodeLockResp`
- Benchmarks: `BenchmarkMarshalMsgLockArgs`, `BenchmarkAppendMsgLockArgs`, `BenchmarkUnmarshalLockArgs`, `BenchmarkEncodeLockArgs`, `BenchmarkDecodeLockArgs`, `BenchmarkMarshalMsgLockResp`, `BenchmarkAppendMsgLockResp`, `BenchmarkUnmarshalLockResp`, `BenchmarkEncodeLockResp`, `BenchmarkDecodeLockResp`
### `internal/grid/msg_gen_test.go`
- Tests: `TestMarshalUnmarshalconnectReq`, `TestEncodeDecodeconnectReq`, `TestMarshalUnmarshalconnectResp`, `TestEncodeDecodeconnectResp`, `TestMarshalUnmarshalmessage`, `TestEncodeDecodemessage`, `TestMarshalUnmarshalmuxConnectError`, `TestEncodeDecodemuxConnectError`, `TestMarshalUnmarshalpingMsg`, `TestEncodeDecodepingMsg`, `TestMarshalUnmarshalpongMsg`, `TestEncodeDecodepongMsg`
- Benchmarks: `BenchmarkMarshalMsgconnectReq`, `BenchmarkAppendMsgconnectReq`, `BenchmarkUnmarshalconnectReq`, `BenchmarkEncodeconnectReq`, `BenchmarkDecodeconnectReq`, `BenchmarkMarshalMsgconnectResp`, `BenchmarkAppendMsgconnectResp`, `BenchmarkUnmarshalconnectResp`, `BenchmarkEncodeconnectResp`, `BenchmarkDecodeconnectResp`, `BenchmarkMarshalMsgmessage`, `BenchmarkAppendMsgmessage`, `BenchmarkUnmarshalmessage`, `BenchmarkEncodemessage`, `BenchmarkDecodemessage`, `BenchmarkMarshalMsgmuxConnectError`, `BenchmarkAppendMsgmuxConnectError`, `BenchmarkUnmarshalmuxConnectError`, `BenchmarkEncodemuxConnectError`, `BenchmarkDecodemuxConnectError`, `BenchmarkMarshalMsgpingMsg`, `BenchmarkAppendMsgpingMsg`, `BenchmarkUnmarshalpingMsg`, `BenchmarkEncodepingMsg`, `BenchmarkDecodepingMsg`, `BenchmarkMarshalMsgpongMsg`, `BenchmarkAppendMsgpongMsg`, `BenchmarkUnmarshalpongMsg`, `BenchmarkEncodepongMsg`, `BenchmarkDecodepongMsg`
### `cmd/admin-handlers-users-race_test.go`
- Tests: `TestIAexternal`
### `cmd/admin-handlers-users_test.go`
- Tests: `TestIAexternal`, `TestIAM_AMPInternalIDPServerSuite`
### `cmd/admin-handlers_test.go`
- Tests: `TestServiceRestartHandler`, `TestAdminServerInfo`, `TestToAdminAPIErrCode`, `TestExtractHealInitParams`, `TestTopLockEntries`
### `cmd/api-errors_test.go`
- Tests: `TestAPIErrCode`, `TestAPIErrCodeDefinition`
### `cmd/api-headers_test.go`
- Tests: `TestNewRequestID`
### `cmd/api-resources_test.go`
- Tests: `TestListObjectsV2Resources`, `TestListObjectsV1Resources`, `TestGetObjectsResources`
### `cmd/api-response_test.go`
- Tests: `TestObjectLocation`, `TestGetURLScheme`
### `cmd/api-utils_test.go`
- Tests: `TestS3EncodeName`
### `cmd/auth-handler_test.go`
- Tests: `TestGetRequestAuthType`, `TestS3SupportedAuthType`, `TestIsRequestPresignedSignatureV2`, `TestIsRequestPresignedSignatureV4`, `TestIsReqAuthenticated`, `TestCheckAdminRequestAuthType`, `TestValidateAdminSignature`
### `cmd/batch-handlers_gen_test.go`
- Tests: `TestMarshalUnmarshalBatchJobPrefix`, `TestEncodeDecodeBatchJobPrefix`, `TestMarshalUnmarshalBatchJobRequest`, `TestEncodeDecodeBatchJobRequest`, `TestMarshalUnmarshalbatchJobInfo`, `TestEncodeDecodebatchJobInfo`
- Benchmarks: `BenchmarkMarshalMsgBatchJobPrefix`, `BenchmarkAppendMsgBatchJobPrefix`, `BenchmarkUnmarshalBatchJobPrefix`, `BenchmarkEncodeBatchJobPrefix`, `BenchmarkDecodeBatchJobPrefix`, `BenchmarkMarshalMsgBatchJobRequest`, `BenchmarkAppendMsgBatchJobRequest`, `BenchmarkUnmarshalBatchJobRequest`, `BenchmarkEncodeBatchJobRequest`, `BenchmarkDecodeBatchJobRequest`, `BenchmarkMarshalMsgbatchJobInfo`, `BenchmarkAppendMsgbatchJobInfo`, `BenchmarkUnmarshalbatchJobInfo`, `BenchmarkEncodebatchJobInfo`, `BenchmarkDecodebatchJobInfo`
### `cmd/batch-handlers_test.go`
- Tests: `TestBatchJobPrefix_UnmarshalYAML`
### `cmd/bucket-handlers_test.go`
- Tests: `TestRemoveBucketHandler`, `TestGetBucketLocationHandler`, `TestHeadBucketHandler`, `TestListMultipartUploadsHandler`, `TestListBucketsHandler`, `TestAPIDeleteMultipleObjectsHandler`
### `cmd/bucket-lifecycle-handlers_test.go`
- Tests: `TestBucketLifecycleWrongCredentials`, `TestBucketLifecycle`
### `cmd/bucket-policy-handlers_test.go`
- Tests: `TestCreateBucket`, `TestPutBucketPolicyHandler`, `TestGetBucketPolicyHandler`, `TestDeleteBucketPolicyHandler`
### `cmd/crossdomain-xml-handler_test.go`
- Tests: `TestCrossXMLHandler`
### `cmd/generic-handlers_test.go`
- Tests: `TestGuessIsRPC`, `TestIsHTTPHeaderSizeTooLarge`, `TestContainsReservedMetadata`, `TestSSETLSHandler`
- Benchmarks: `Benchmark_hasBadPathComponent`
### `cmd/handler-utils_test.go`
- Tests: `TestIsValidLocationConstraint`, `TestExtractMetadataHeaders`, `TestGetResource`
### `cmd/jwt_test.go`
- Tests: `TestWebRequestAuthenticate`
- Benchmarks: `BenchmarkParseJWTStandardClaims`, `BenchmarkParseJWTMapClaims`, `BenchmarkAuthenticateNode`
### `cmd/kms-handlers_test.go`
- Tests: `TestKMSHandlersCreateKey`, `TestKMSHandlersKeyStatus`, `TestKMSHandlersAPIs`, `TestKMSHandlersListKeys`, `TestKMSHandlerAdminAPI`, `TestKMSHandlerNotConfiguredOrInvalidCreds`
### `cmd/lock-rest-client_test.go`
- Tests: `TestLockRESTlient`
### `cmd/lock-rest-server-common_test.go`
- Tests: `TestLockRpcServerRemoveEntry`
### `cmd/object-api-deleteobject_test.go`
- Tests: `TestDeleteObject`
### `cmd/object-api-getobjectinfo_test.go`
- Tests: `TestGetObjectInfo`
### `cmd/object-api-listobjects_test.go`
- Tests: `TestListObjectsVersionedFolders`, `TestListObjectsOnVersionedBuckets`, `TestListObjects`, `TestDeleteObjectVersionMarker`, `TestListObjectVersions`, `TestListObjectsContinuation`
- Benchmarks: `BenchmarkListObjects`
### `cmd/object-api-multipart_test.go`
- Tests: `TestObjectNewMultipartUpload`, `TestObjectAbortMultipartUpload`, `TestObjectAPIIsUploadIDExists`, `TestObjectAPIPutObjectPart`, `TestListMultipartUploads`, `TestListObjectPartsStale`, `TestListObjectPartsDiskNotFound`, `TestListObjectParts`, `TestObjectCompleteMultipartUpload`
- Benchmarks: `BenchmarkPutObjectPart5MbFS`, `BenchmarkPutObjectPart5MbErasure`, `BenchmarkPutObjectPart10MbFS`, `BenchmarkPutObjectPart10MbErasure`, `BenchmarkPutObjectPart25MbFS`, `BenchmarkPutObjectPart25MbErasure`, `BenchmarkPutObjectPart50MbFS`, `BenchmarkPutObjectPart50MbErasure`
### `cmd/object-api-options_test.go`
- Tests: `TestGetAndValidateAttributesOpts`
### `cmd/object-api-putobject_test.go`
- Tests: `TestObjectAPIPutObjectSingle`, `TestObjectAPIPutObjectDiskNotFound`, `TestObjectAPIPutObjectStaleFiles`, `TestObjectAPIMultipartPutObjectStaleFiles`
- Benchmarks: `BenchmarkPutObjectVerySmallFS`, `BenchmarkPutObjectVerySmallErasure`, `BenchmarkPutObject10KbFS`, `BenchmarkPutObject10KbErasure`, `BenchmarkPutObject100KbFS`, `BenchmarkPutObject100KbErasure`, `BenchmarkPutObject1MbFS`, `BenchmarkPutObject1MbErasure`, `BenchmarkPutObject5MbFS`, `BenchmarkPutObject5MbErasure`, `BenchmarkPutObject10MbFS`, `BenchmarkPutObject10MbErasure`, `BenchmarkPutObject25MbFS`, `BenchmarkPutObject25MbErasure`, `BenchmarkPutObject50MbFS`, `BenchmarkPutObject50MbErasure`, `BenchmarkParallelPutObjectVerySmallFS`, `BenchmarkParallelPutObjectVerySmallErasure`, `BenchmarkParallelPutObject10KbFS`, `BenchmarkParallelPutObject10KbErasure`, `BenchmarkParallelPutObject100KbFS`, `BenchmarkParallelPutObject100KbErasure`, `BenchmarkParallelPutObject1MbFS`, `BenchmarkParallelPutObject1MbErasure`, `BenchmarkParallelPutObject5MbFS`, `BenchmarkParallelPutObject5MbErasure`, `BenchmarkParallelPutObject10MbFS`, `BenchmarkParallelPutObject10MbErasure`, `BenchmarkParallelPutObject25MbFS`, `BenchmarkParallelPutObject25MbErasure`
### `cmd/object-api-utils_test.go`
- Tests: `TestPathTraversalExploit`, `TestIsValidBucketName`, `TestIsValidObjectName`, `TestGetCompleteMultipartMD5`, `TestIssource`, `TestRemoveStandardStorageClass`, `TestCleanMetadata`, `TestCleanMetadataKeys`, `TestIsCompressed`, `TestExcludeForCompression`, `TestGetActualSize`, `TestGetCompressedOffsets`, `TestS2CompressReader`, `Test_pathNeedsClean`
- Benchmarks: `BenchmarkConcatImplementation`, `BenchmarkPathJoinOld`, `BenchmarkPathJoin`, `BenchmarkGetPartFileWithTrie`
### `cmd/object-handlers-common_test.go`
- Tests: `TestCanonicalizeETag`, `TestCheckPreconditions`
### `cmd/object-handlers_test.go`
- Tests: `TestAPIHeadObjectHandler`, `TestAPIHeadObjectHandlerWithEncryption`, `TestAPIGetObjectHandler`, `TestAPIGetObjectWithMPHandler`, `TestAPIGetObjectWithPartNumberHandler`, `TestAPIPutObjectStreamSigV4Handler`, `TestAPIPutObjectHandler`, `TestAPICopyObjectPartHandlerSanity`, `TestAPICopyObjectPartHandler`, `TestAPICopyObjectHandler`, `TestAPINewMultipartHandler`, `TestAPINewMultipartHandlerParallel`, `TestAPICompleteMultipartHandler`, `TestAPIAbortMultipartHandler`, `TestAPIDeleteObjectHandler`, `TestAPIPutObjectPartHandlerStreaming`, `TestAPIPutObjectPartHandler`, `TestAPIListObjectPartsHandlerPreSign`, `TestAPIListObjectPartsHandler`
### `cmd/object_api_suite_test.go`
- Tests: `TestMakeBucket`, `TestMultipartObjectCreation`, `TestMultipartObjectAbort`, `TestMultipleObjectCreation`, `TestPaging`, `TestObjectOverwriteWorks`, `TestNonExistentBucketOperations`, `TestBucketRecreateFails`, `TestPutObject`, `TestPutObjectInSubdir`, `TestListBuckets`, `TestListBucketsOrder`, `TestListObjectsTestsForNonExistentBucket`, `TestNonExistentObjectInBucket`, `TestGetDirectoryReturnsObjectNotFound`, `TestContentType`
### `cmd/storage-rest-common_gen_test.go`
- Tests: `TestMarshalUnmarshalnsScannerOptions`, `TestEncodeDecodensScannerOptions`, `TestMarshalUnmarshalnsScannerResp`, `TestEncodeDecodensScannerResp`
- Benchmarks: `BenchmarkMarshalMsgnsScannerOptions`, `BenchmarkAppendMsgnsScannerOptions`, `BenchmarkUnmarshalnsScannerOptions`, `BenchmarkEncodensScannerOptions`, `BenchmarkDecodensScannerOptions`, `BenchmarkMarshalMsgnsScannerResp`, `BenchmarkAppendMsgnsScannerResp`, `BenchmarkUnmarshalnsScannerResp`, `BenchmarkEncodensScannerResp`, `BenchmarkDecodensScannerResp`
### `cmd/storage-rest_test.go`
- Tests: `TestStorageRESTClientDiskInfo`, `TestStorageRESTClientStatInfoFile`, `TestStorageRESTClientListDir`, `TestStorageRESTClientReadAll`, `TestStorageRESTClientReadFile`, `TestStorageRESTClientAppendFile`, `TestStorageRESTClientDeleteFile`, `TestStorageRESTClientRenameFile`
### `cmd/sts-handlers_test.go`
- Tests: `TestIAexternal`, `TestIAMWithLDAPServerSuite`, `TestIAMWithLDAPNonNormalizedBaseDNConfigServerSuite`, `TestIAMExportImportWithLDAP`, `TestIAMImportAssetWithLDAP`, `TestIAMWithOpenIDServerSuite`, `TestIAMWithOpenIDWithRolePolicyServerSuite`, `TestIAMWithOpenIDWithRolePolicyWithPolicyVariablesServerSuite`, `TestIAMWithOpenIDMultipleConfigsValidation1`, `TestIAMWithOpenIDMultipleConfigsValidation2`, `TestIAMWithOpenIDWithMultipleRolesServerSuite`, `TestIAM_AMPWithOpenIDWithMultipleRolesServerSuite`
### `cmd/batch-expire_test.go`
- Tests: `TestParseBatchJobExpire`
### `cmd/batch-job-common-types_test.go`
- Tests: `TestBatchJobSizeInRange`, `TestBatchJobSizeValidate`
### `cmd/batch-replicate_test.go`
- Tests: `TestParseBatchJobReplicate`
### `cmd/bucket-encryption_test.go`
- Tests: `TestValidateBucketSSEConfig`
### `cmd/bucket-lifecycle_test.go`
- Tests: `TestParseRestoreObjStatus`, `TestRestoreObjStatusRoundTrip`, `TestRestoreObjOnDisk`, `TestIsRestoredObjectOnDisk`, `TestObjectIsRemote`, `TestValidateTransitionTier`
### `cmd/bucket-replication-utils_test.go`
- Tests: `TestReplicatedInfos`, `TestParseReplicateDecision`, `TestCompositeReplicationStatus`
### `cmd/bucket-replication_test.go`
- Tests: `TestReplicationResync`, `TestReplicationResyncwrapper`
### `cmd/common-main_test.go`
- Tests: `Test_readFromSecret`, `Test_source`
### `cmd/config-current_test.go`
- Tests: `TestServerConfig`
### `cmd/config-encrypted_test.go`
- Tests: `TestDecryptData`
### `cmd/copy-part-range_test.go`
- Tests: `TestParseCopyPartRangeSpec`
### `cmd/dummy-data-generator_test.go`
- Tests: `TestDummyDataGenerator`, `TestCmpReaders`
### `cmd/dynamic-timeouts_test.go`
- Tests: `TestDynamicTimeoutSingleIncrease`, `TestDynamicTimeoutDualIncrease`, `TestDynamicTimeoutSingleDecrease`, `TestDynamicTimeoutDualDecrease`, `TestDynamicTimeoutManyDecreases`, `TestDynamicTimeoutConcurrent`, `TestDynamicTimeoutHitMinimum`, `TestDynamicTimeoutAdjustExponential`, `TestDynamicTimeoutAdjustNormalized`
### `cmd/encryption-v1_test.go`
- Tests: `TestEncryptRequest`, `TestDecryptObjectInfo`, `TestDecryptETag`, `TestGetDecryptedRange_Issue50`, `TestGetDecryptedRange`, `TestGetDefaultOpts`
### `cmd/endpoint-ellipses_test.go`
- Tests: `TestCreateServerEndpoints`, `TestGetDivisibleSize`, `TestGetSetIndexesEnvOverride`, `TestGetSetIndexes`, `TestParseEndpointSet`
### `cmd/endpoint_contrib_test.go`
- Tests: `TestUpdateDomainIPs`
### `cmd/endpoint_test.go`
- Tests: `TestNewEndpoint`, `TestNewEndpoints`, `TestCreateEndpoints`, `TestGetLocalPeer`, `TestGetRemotePeers`
### `cmd/erasure-decode_test.go`
- Tests: `TestErasureDecode`, `TestErasureDecodeRandomOffsetLength`
- Benchmarks: `BenchmarkErasureDecodeQuick`, `BenchmarkErasureDecode_4_64KB`, `BenchmarkErasureDecode_8_20MB`, `BenchmarkErasureDecode_12_30MB`, `BenchmarkErasureDecode_16_40MB`
### `cmd/erasure-encode_test.go`
- Tests: `TestErasureEncode`
- Benchmarks: `BenchmarkErasureEncodeQuick`, `BenchmarkErasureEncode_4_64KB`, `BenchmarkErasureEncode_8_20MB`, `BenchmarkErasureEncode_12_30MB`, `BenchmarkErasureEncode_16_40MB`
### `cmd/erasure-metadata-utils_test.go`
- Tests: `TestDiskCount`, `TestReduceErrs`, `TestHashOrder`, `TestShuffleDisks`, `TestEvalDisks`, `Test_hashOrder`
### `cmd/erasure-metadata_test.go`
- Tests: `TestAddObjectPart`, `TestObjectPartIndex`, `TestObjectToPartOffset`, `TestFindFileInfoInQuorum`, `TestTransitionInfoEquals`, `TestSkipTierFreeVersion`, `TestListObjectParities`
### `cmd/erasure-object_test.go`
- Tests: `TestRepeatPutObjectPart`, `TestErasureDeleteObjectBasic`, `TestDeleteObjectsVersionedTwoPools`, `TestDeleteObjectsVersioned`, `TestErasureDeleteObjectsErasureSet`, `TestErasureDeleteObjectDiskNotFound`, `TestErasureDeleteObjectDiskNotFoundErasure4`, `TestErasureDeleteObjectDiskNotFoundErr`, `TestGetObjectNoQuorum`, `TestHeadObjectNoQuorum`, `TestPutObjectNoQuorum`, `TestPutObjectNoQuorumSmall`, `TestPutObjectSmallInlineData`, `TestObjectQuorumFromMeta`, `TestGetObjectInlineNotInline`, `TestGetObjectWithOutdatedDisks`
### `cmd/erasure-server-pool-decom_test.go`
- Tests: `TestPoolMetaValidate`
### `cmd/erasure-sets_test.go`
- Tests: `TestSipHashMod`, `TestCrcHashMod`, `TestNewErasureSets`, `TestHashedLayer`
- Benchmarks: `BenchmarkCrcHash`, `BenchmarkSipHash`
### `cmd/erasure_test.go`
- Tests: `TestErasureEncodeDecode`
### `cmd/format-erasure_test.go`
- Tests: `TestFixFormatV3`, `TestFormatErasureEmpty`, `TestFormatErasureMigrate`, `TestCheckFormatErasureValue`, `TestGetFormatErasureInQuorumCheck`, `TestNewFormatSets`
- Benchmarks: `BenchmarkGetFormatErasureInQuorumOld`, `BenchmarkGetFormatErasureInQuorum`, `BenchmarkInitStorageDisks256`, `BenchmarkInitStorageDisks1024`, `BenchmarkInitStorageDisks2048`, `BenchmarkInitStorageDisksMax`
### `cmd/http-tracer_test.go`
- Tests: `TestRedactLDAPPwd`
### `cmd/httprange_test.go`
- Tests: `TestHTTPRequestRangeSpec`, `TestHTTPRequestRangeToHeader`
### `cmd/iam-etcd-store_test.go`
- Tests: `TestExtractPrefixAndSuffix`
### `cmd/iam-object-store_test.go`
- Tests: `TestSplitPath`
### `cmd/local-locker_test.go`
- Tests: `TestLocalLockerExpire`, `TestLocalLockerUnlock`, `Test_localLocker_expireOldLocksExpire`, `Test_localLocker_RUnlock`
- Benchmarks: `Benchmark_bucketMetacache_findCache`
### `cmd/metacache-entries_test.go`
- Tests: `Test_metaCacheEntries_sort`, `Test_metaCacheEntries_forwardTo`, `Test_metaCacheEntries_merge`, `Test_metaCacheEntries_filterObjects`, `Test_metaCacheEntries_filterPrefixes`, `Test_metaCacheEntries_filterRecursive`, `Test_metaCacheEntries_filterRecursiveRoot`, `Test_metaCacheEntries_filterRecursiveRootSep`, `Test_metaCacheEntries_filterPrefix`, `Test_metaCacheEntry_isInDir`, `Test_metaCacheEntries_resolve`
### `cmd/metacache-stream_test.go`
- Tests: `Test_metacacheReader_readNames`, `Test_metacacheReader_readN`, `Test_metacacheReader_readNDirs`, `Test_metacacheReader_readNPrefix`, `Test_metacacheReader_readFn`, `Test_metacacheReader_readAll`, `Test_metacacheReader_forwardTo`, `Test_metacacheReader_next`, `Test_metacacheReader_peek`, `Test_newMetacacheStream`, `Test_metacacheReader_skip`
### `cmd/metacache_test.go`
- Tests: `Test_baseDirFromPrefix`, `Test_metacache_finished`, `Test_metacache_worthKeeping`
### `cmd/metrics-v2_test.go`
- Tests: `TestGetHistogramMetrics_BucketCount`, `TestGetHistogramMetrics_Values`
### `cmd/namespace-lock_test.go`
- Tests: `TestGetSource`, `TestNSLockRace`
### `cmd/net_test.go`
- Tests: `TestMustSplitHostPort`, `TestSortIPs`, `TestMustGetLocalIP4`, `TestGetHostIP`, `TestGetAPIEndpoints`, `TestCheckLocalServerAddr`, `TestExtractHostPort`, `TestSameLocalAddrs`, `TestIsHostIP`
### `cmd/os-readdir_test.go`
- Tests: `TestReadDirFail`, `TestReadDir`, `TestReadDirN`
### `cmd/os-reliable_test.go`
- Tests: `TestOSMkdirAll`, `TestOSRenameAll`
### `cmd/policy_test.go`
- Tests: `TestPolicySysIsAllowed`, `TestPolicyToBucketAccessPolicy`, `TestBucketAccessPolicyToPolicy`
### `cmd/post-policy_test.go`
- Tests: `TestPostPolicyReservedBucketExploit`, `TestPostPolicyBucketHandler`, `TestPostPolicyBucketHandlerRedirect`
### `cmd/postpolicyform_test.go`
- Tests: `TestParsePostPolicyForm`, `TestPostPolicyForm`
### `cmd/server-main_test.go`
- Tests: `TestServerConfigFile`, `TestNewObjectLayer`
### `cmd/server-startup-msg_test.go`
- Tests: `TestStripStandardPorts`, `TestPrintServerCommonMessage`, `TestPrintCLIAccessMsg`, `TestPrintStartupMessage`
### `cmd/server_test.go`
- Tests: `TestServerSuite`
### `cmd/sftp-server_test.go`
- Tests: `TestSFTPAuthentication`
### `cmd/signature-v2_test.go`
- Tests: `TestResourceListSorting`, `TestDoesPresignedV2SignatureMatch`, `TestValidateV2AuthHeader`, `TestDoesPolicySignatureV2Match`
### `cmd/signature-v4-parser_test.go`
- Tests: `TestParseCredentialHeader`, `TestParseSignature`, `TestParseSignedHeaders`, `TestParseSignV4`, `TestDoesV4PresignParamsExist`, `TestParsePreSignV4`
### `cmd/signature-v4-utils_test.go`
- Tests: `TestCheckValid`, `TestSkipContentSha256Cksum`, `TestIsValidRegion`, `TestExtractSignedHeaders`, `TestSignV4TrimAll`, `TestGetContentSha256Cksum`, `TestCheckMetaHeaders`
### `cmd/signature-v4_test.go`
- Tests: `TestDoesPolicySignatureMatch`, `TestDoesPresignedSignatureMatch`
### `cmd/site-replication_test.go`
- Tests: `TestGetMissingSiteNames`
- Benchmarks: `BenchmarkDecodeVolInfoMsgp`, `BenchmarkDecodeDiskInfoMsgp`, `BenchmarkDecodeDiskInfoGOB`, `BenchmarkEncodeDiskInfoMsgp`, `BenchmarkEncodeDiskInfoGOB`, `BenchmarkDecodeFileInfoMsgp`, `BenchmarkDecodeFileInfoGOB`, `BenchmarkEncodeFileInfoMsgp`, `BenchmarkEncodeFileInfoGOB`
### `cmd/streaming-signature-v4_test.go`
- Tests: `TestReadChunkLine`, `TestParseS3ChunkExtension`, `TestReadCRLF`, `TestParseHexUint`
### `cmd/test-utils_test.go`
- Tests: `TestMain`, `TestToErrIsNil`
### `cmd/tier_test.go`
- Tests: `TestTierMetrics`
### `cmd/update-notifier_test.go`
- Tests: `TestPrepareUpdateMessage`
### `cmd/update_test.go`
- Tests: `Testsource`, `TestReleaseTagToNFromTimeConversion`, `TestDownloadURL`, `TestUserAgent`, `TestIsDCOS`, `TestIsKubernetes`, `TestGetHelmVersion`, `TestDownloadReleaseData`, `TestParseReleaseData`
- Benchmarks: `BenchmarkURLQueryForm`, `BenchmarkURLQuery`
### `cmd/utils_test.go`
- Tests: `TestMaxObjectSize`, `TestMinAllowedPartSize`, `TestMaxPartID`, `TestPath2BucketObjectName`, `TestStartProfiler`, `TestCheckURL`, `TestDumpRequest`, `TestToS3ETag`, `TestCeilFrac`, `TestIsErrIgnored`, `TestQueries`, `TestLCP`, `TestGetsource`
### `cmd/version_test.go`
- Tests: `TestVersion`
### `cmd/xl-storage-errors_test.go`
- Tests: `TestSysErrors`
### `cmd/xl-storage-format-utils_test.go`
- Tests: `Test_hashDeterministicString`, `TestGetFileInfoVersions`
### `cmd/xl-storage-format-v2_test.go`
- Tests: `TestReadXLMetaNoData`, `TestXLV2FormatData`, `TestUsesDataDir`, `TestDeleteVersionWithSharedDataDir`, `Test_xlMetaV2Shallow_Load`, `Test_xlMetaV2Shallow_LoadTimeStamp`, `Test_mergeXLV2Versions`, `Test_mergeXLV2Versions2`, `Test_mergeEntryChannels`, `TestXsource`
- Benchmarks: `Benchmark_mergeXLV2Versions`, `Benchmark_xlMetaV2Shallow_Load`, `BenchmarkToFileInfoNoParts`, `BenchmarkToFileInfoWithParts`
### `cmd/xl-storage-format_test.go`
- Tests: `TestIsXLMetaFormatValid`, `TestIsXLMetaErasureInfoValid`, `TestGetXLMetaV1Jsoniter1`, `TestGetXLMetaV1Jsoniter10`, `TestGetPartSizeFromIdx`
- Benchmarks: `BenchmarkXlMetaV2Shallow`
### `cmd/xl-storage-free-version_test.go`
- Tests: `TestFreeVersion`, `TestSkipFreeVersion`
### `cmd/xl-storage_test.go`
- Tests: `TestCheckPathLength`, `TestIsValidVolname`, `TestXLStorageGetDiskInfo`, `TestXLStorageIsDirEmpty`, `TestXLStorageReadVersionLegacy`, `TestXLStorageReadVersion`, `TestXLStorageReadAll`, `TestNewXLStorage`, `TestXLStorageMakeVol`, `TestXLStorageDeleteVol`, `TestXLStorageStatVol`, `TestXLStorageListVols`, `TestXLStorageListDir`, `TestXLStorageDeleteFile`, `TestXLStorageReadFile`, `TestXLStorageReadFileWithVerify`, `TestXLStorageFormatFileChange`, `TestXLStorageAppendFile`, `TestXLStorageRenameFile`, `TestXLStorageDeleteVersion`, `TestXLStorageStatInfoFile`, `TestXLStorageVerifyFile`, `TestXLStorageReadMetadata`
### `cmd/xl-storage_unix_test.go`
- Tests: `TestIsValidUmaskVol`, `TestIsValidUmaskFile`
### `cmd/xl-storage_windows_test.go`
- Tests: `TestUNCPaths`, `TestUNCPathENOTDIR`
### `internal/amztime/iso8601_time_test.go`
- Tests: `TestISO8601Format`
### `internal/amztime/parse_test.go`
- Tests: `TestParse`
### `internal/arn/arn_test.go`
- Tests: `TestARN_String`, `TestNewIAMRoleARN`, `TestParse`
### `internal/auth/credentials_test.go`
- Tests: `TestExpToInt64`, `TestIsAccessKeyValid`, `TestIsSecretKeyValid`, `TestGetNewCredentials`, `TestCreateCredentials`, `TestCredentialsEqual`
### `internal/bpool/bpool_test.go`
- Tests: `TestBytePool`
### `internal/bucket/bandwidth/monitor_test.go`
- Tests: `TestMonitor_GetReport`
### `internal/bucket/encryption/bucket-sse-config_test.go`
- Tests: `TestParseBucketSSEConfig`
### `internal/bucket/lifecycle/delmarker-expiration_test.go`
- Tests: `TestDelMarkerExpParseAndValidate`
### `internal/bucket/lifecycle/evaluator_test.go`
- Tests: `TestNewerNoncurrentVersions`, `TestEmptyEvaluator`
### `internal/bucket/lifecycle/expiration_test.go`
- Tests: `TestInvalidExpiration`
### `internal/bucket/lifecycle/filter_test.go`
- Tests: `TestUnsupportedFilters`, `TestObjectSizeFilters`, `TestTestTags`
### `internal/bucket/lifecycle/lifecycle_test.go`
- Tests: `TestParseAndValidateLifecycleConfig`, `TestMarshalLifecycleConfig`, `TestExpectedExpiryTime`, `TestEval`, `TestHasActiveRules`, `TestSetPredictionHeaders`, `TestTransitionTier`, `TestTransitionTierWithPrefixAndTags`, `TestNoncurrentVersionsLimit`, `TestMaxNoncurrentBackwardCompat`, `TestParseLifecycleConfigWithID`, `TestFilterAndSetPredictionHeaders`, `TestFilterRules`, `TestDeleteAllVersions`
### `internal/bucket/lifecycle/noncurrentversion_test.go`
- Tests: `Test_NoncurrentVersionsExpiration_Validation`
### `internal/bucket/lifecycle/rule_test.go`
- Tests: `TestInvalidRules`
### `internal/bucket/lifecycle/transition_test.go`
- Tests: `TestTransitionUnmarshalXML`
### `internal/bucket/object/lock/lock_test.go`
- Tests: `TestParseMode`, `TestParseLegalHoldStatus`, `TestUnmarshalDefaultRetention`, `TestParseObjectLockConfig`, `TestParseObjectRetention`, `TestIsObjectLockRequested`, `TestIsObjectLockGovernanceBypassSet`, `TestParseObjectLockRetentionHeaders`, `TestGetObjectRetentionMeta`, `TestGetObjectLegalHoldMeta`, `TestParseObjectLegalHold`, `TestFilterObjectLockMetadata`, `TestToString`
### `internal/bucket/replication/replication_test.go`
- Tests: `TestParseAndValidateReplicationConfig`, `TestReplicate`, `TestHasActiveRules`, `TestFilterActionableRules`
### `internal/bucket/replication/rule_test.go`
- Tests: `TestMetadataReplicate`
### `internal/bucket/versioning/versioning_test.go`
- Tests: `TestParseConfig`, `TestMarshalXML`, `TestVersioningZero`, `TestExcludeFolders`, `TestExcludedPrefixesMatch`
### `internal/cachevalue/cache_test.go`
- Tests: `TestCacheCtx`, `TestCache`
- Benchmarks: `BenchmarkCache`
### `internal/config/bool-flag_test.go`
- Tests: `TestBoolFlagString`, `TestBoolFlagMarshalJSON`, `TestBoolFlagUnmarshalJSON`, `TestParseBoolFlag`
### `internal/config/certs_test.go`
- Tests: `TestParsePublicCertFile`, `TestLoadX509KeyPair`
### `internal/config/compress/compress_test.go`
- Tests: `TestParseCompressIncludes`
### `internal/config/config_test.go`
- Tests: `TestKVFields`, `TestValidRegion`
### `internal/config/crypto_test.go`
- Tests: `TestEncryptDecrypt`
- Benchmarks: `BenchmarkEncrypt`
### `internal/config/dns/etcd_dns_test.go`
- Tests: `TestDNSJoin`, `TestPath`, `TestUnPath`
### `internal/config/etcd/etcd_test.go`
- Tests: `TestParseEndpoints`
### `internal/config/identity/openid/jwks_test.go`
- Tests: `TestAzurePublicKey`, `TestPublicKey`
### `internal/config/identity/openid/jwt_test.go`
- Tests: `TestUpdateClaimsExpiry`, `TestJWTHMACType`, `TestJWT`, `TestDefaultExpiryDuration`, `TestExpCorrect`, `TestKeycloakProviderInitialization`
### `internal/config/lambda/event/arn_test.go`
- Tests: `TestARNString`, `TestParseARN`
### `internal/config/lambda/event/targetid_test.go`
- Tests: `TestTargetDString`, `TestTargetDToARN`, `TestTargetDMarshalJSON`, `TestTargetDUnmarshalJSON`
### `internal/config/lambda/event/targetidset_test.go`
- Tests: `TestTargetIDSetClone`, `TestTargetIDSetUnion`, `TestTargetIDSetDifference`, `TestNewTargetIDSet`
### `internal/config/storageclass/storage-class_test.go`
- Tests: `TestParseStorageClass`, `TestValidateParity`, `TestParityCount`, `TestIsValidStorageClassKind`
### `internal/crypto/header_test.go`
- Tests: `TestIsRequested`, `TestKMSIsRequested`, `TestKMSParseHTTP`, `TestS3IsRequested`, `TestS3Parse`, `TestSSECIsRequested`, `TestSSECopyIsRequested`, `TestSSECParse`, `TestSSECopyParse`, `TestRemoveSensitiveHeaders`
### `internal/crypto/key_test.go`
- Tests: `TestGenerateKey`, `TestGenerateIV`, `TestSealUnsealKey`, `TestDerivePartKey`, `TestSealETag`
### `internal/crypto/metadata_test.go`
- Tests: `TestIsMultipart`, `TestIsEncrypted`, `TestS3IsEncrypted`, `TestSSECIsEncrypted`, `TestS3ParseMetadata`, `TestCreateMultipartMetadata`, `TestSSECParseMetadata`, `TestS3CreateMetadata`, `TestSSECCreateMetadata`, `TestIsETagSealed`, `TestRemoveInternalEntries`
### `internal/crypto/sse_test.go`
- Tests: `TestS3String`, `TestSSECString`, `TestSSECUnsealObjectKey`, `TestSSECopyUnsealObjectKey`
### `internal/deadlineconn/deadlineconn_test.go`
- Tests: `TestBuffConnReadTimeout`, `TestBuffConnReadCheckTimeout`
### `internal/disk/disk_test.go`
- Tests: `TestFree`
### `internal/disk/stat_test.go`
- Tests: `TestReadDriveStats`
### `internal/dsync/drwmutex_test.go`
- Tests: `TestSimpleWriteLockAcquired`, `TestSimpleWriteLockTimedOut`, `TestDualWriteLockAcquired`, `TestDualWriteLockTimedOut`, `TestParallelReaders`, `TestRWMutex`, `TestUnlockPanic`, `TestUnlockPanic2`, `TestRUnlockPanic`, `TestRUnlockPanic2`
- Benchmarks: `BenchmarkRWMutexWrite100`, `BenchmarkRWMutexWrite10`, `BenchmarkRWMutexWorkWrite100`, `BenchmarkRWMutexWorkWrite10`
### `internal/dsync/dsync_test.go`
- Tests: `TestMain`, `TestSimpleLock`, `TestSimpleLockUnlockMultipleTimes`, `TestTwoSimultaneousLocksForSameResource`, `TestThreeSimultaneousLocksForSameResource`, `TestTwoSimultaneousLocksForDifferentResources`, `TestSuccessfulLockRefresh`, `TestFailedRefreshLock`, `TestUnlockShouldNotTimeout`, `TestMutex`
- Benchmarks: `BenchmarkMutexUncontended`, `BenchmarkMutex`, `BenchmarkMutexSlack`, `BenchmarkMutexWork`, `BenchmarkMutexWorkSlack`, `BenchmarkMutexNoSpin`, `BenchmarkMutexSpin`
### `internal/etag/etag_test.go`
- Tests: `TestParse`, `TestString`, `TestEqual`, `TestReader`, `TestMultipart`, `TestIsEncrypted`, `TestFormat`, `TestFromContentMD5`, `TestDecrypt`
### `internal/event/arn_test.go`
- Tests: `TestARNString`, `TestARNMarshalXML`, `TestARNUnmarshalXML`, `TestParseARN`
### `internal/event/config_test.go`
- Tests: `TestValidateFilterRuleValue`, `TestFilterRuleUnmarshalXML`, `TestFilterRuleListUnmarshalXML`, `TestFilterRuleListPattern`, `TestQueueUnmarshalXML`, `TestQueueValidate`, `TestQueueSetRegion`, `TestQueueToRulesMap`, `TestConfigUnmarshalXML`, `TestConfigValidate`, `TestConfigSetRegion`, `TestConfigToRulesMap`, `TestParseConfig`
### `internal/event/name_test.go`
- Tests: `TestNameExpand`, `TestNameString`, `TestNameMarshalXML`, `TestNameUnmarshalXML`, `TestNameMarshalJSON`, `TestNameUnmarshalJSON`, `TestParseName`
### `internal/event/rules_test.go`
- Tests: `TestNewPattern`, `TestRulesAdd`, `TestRulesMatch`, `TestRulesClone`, `TestRulesUnion`, `TestRulesDifference`
### `internal/event/rulesmap_test.go`
- Tests: `TestRulesMapClone`, `TestRulesMapAdd`, `TestRulesMapRemove`, `TestRulesMapMatch`, `TestNewRulesMap`
### `internal/event/target/mysql_test.go`
- Tests: `TestMySQLRegistration`
### `internal/event/target/nats_contrib_test.go`
- Tests: `TestNatsConnPlain`, `TestNatsConnUserPass`, `TestNatsConnToken`, `TestNatsConnNKeySeed`
### `internal/event/target/nats_tls_contrib_test.go`
- Tests: `TestNatsConnTLSCustomCA`, `TestNatsConnTLSCustomCAHandshakeFirst`, `TestNatsConnTLSClientAuthorization`
### `internal/event/target/nsq_test.go`
- Tests: `TestNSQArgs_Validate`
### `internal/event/target/postgresql_test.go`
- Tests: `TestPostgreSQLRegistration`, `TestPsqlTableNameValidation`
### `internal/event/targetid_test.go`
- Tests: `TestTargetDString`, `TestTargetDToARN`, `TestTargetDMarshalJSON`, `TestTargetDUnmarshalJSON`
### `internal/event/targetidset_test.go`
- Tests: `TestTargetIDSetClone`, `TestTargetIDSetUnion`, `TestTargetIDSetDifference`, `TestNewTargetIDSet`
### `internal/event/targetlist_test.go`
- Tests: `TestTargetListAdd`, `TestTargetListExists`, `TestTargetListList`, `TestNewTargetList`
### `internal/grid/connection_test.go`
- Tests: `TestDisconnect`, `TestShouldConnect`
### `internal/grid/grid_test.go`
- Tests: `TestSingleRoundtrip`, `TestSingleRoundtripNotReady`, `TestSingleRoundtripGenerics`, `TestSingleRoundtripGenericsRecycle`, `TestStreamSuite`
### `internal/grid/types_test.go`
- Tests: `TestMarshalUnmarshalMSS`, `TestMarshalUnmarshalMSSNil`, `TestMarshalUnmarshalBytes`, `TestMarshalUnmarshalBytesNil`
- Benchmarks: `BenchmarkMarshalMsgMSS`, `BenchmarkAppendMsgMSS`, `BenchmarkUnmarshalMSS`
### `internal/handlers/proxy_test.go`
- Tests: `TestGetScheme`, `TestGetSourceIP`, `TestXFFDisabled`
### `internal/hash/reader_test.go`
- Tests: `TestHashReaderHelperMethods`, `TestHashReaderVerification`, `TestHashReaderInvalidArguments`
### `internal/http/check_port_test.go`
- Tests: `TestCheckPortAvailability`
### `internal/http/listener_test.go`
- Tests: `TestNewHTTPListener`, `TestHTTPListenerStartClose`, `TestHTTPListenerAddr`, `TestHTTPListenerAddrs`
### `internal/http/server_test.go`
- Tests: `TestNewServer`
### `internal/ioutil/ioutil_test.go`
- Tests: `TestDeadlineWorker`, `TestDeadlineWriter`, `TestCloseOnWriter`, `TestAppendFile`, `TestSkipReader`, `TestSameFile`, `TestCopyAligned`
### `internal/jwt/parser_test.go`
- Tests: `TestParserParse`
### `internal/kms/config_test.go`
- Tests: `TestIsPresent`
### `internal/kms/dek_test.go`
- Tests: `TestEncodeDecodeDEK`
### `internal/kms/secret-key_test.go`
- Tests: `TestSingleKeyRoundtrip`, `TestDecryptKey`
### `internal/lock/lock_test.go`
- Tests: `TestLockFail`, `TestLockDirFail`, `TestRWLockedFile`, `TestLockAndUnlock`
### `internal/lock/lock_windows_test.go`
- Tests: `TestFixLongPath`
### `internal/lsync/lrwmutex_test.go`
- Tests: `TestSimpleWriteLockAcquired`, `TestSimpleWriteLockTimedOut`, `TestDualWriteLockAcquired`, `TestDualWriteLockTimedOut`, `TestParallelReaders`, `TestRWMutex`, `TestDRLocker`, `TestUnlockPanic`, `TestUnlockPanic2`, `TestRUnlockPanic`, `TestRUnlockPanic2`
### `internal/mountinfo/mountinfo_linux_test.go`
- Tests: `TestCrossDeviceMountPaths`, `TestCrossDeviceMount`, `TestReadProcmountInfos`, `TestReadProcMountFrom`
### `internal/pubsub/pubsub_test.go`
- Tests: `TestSubscribe`, `TestNumSubscribersMask`, `TestSubscribeExceedingLimit`, `TestUnsubscribe`, `TestPubSub`, `TestMultiPubSub`, `TestMultiPubSubMask`
### `internal/rest/client_test.go`
- Tests: `TestNetworkError_Unwrap`
### `internal/ringbuffer/ring_buffer_test.go`
- Tests: `TestRingBuffer_interface`, `TestRingBuffer_Write`, `TestRingBuffer_WriteBlocking`, `TestRingBuffer_Read`, `TestRingBuffer_Blocking`, `TestRingBuffer_BlockingBig`, `TestRingBuffer_ByteInterface`, `TestRingBufferCloseError`, `TestRingBufferCloseErrorUnblocks`, `TestWriteAfterWriterClose`
### `internal/s3select/csv/reader_contrib_test.go`
- Tests: `TestRead`, `TestReadExtended`, `TestReadFailures`
- Benchmarks: `BenchmarkReaderBasic`, `BenchmarkReaderHuge`, `BenchmarkReaderReplace`, `BenchmarkReaderReplaceTwo`
### `internal/s3select/json/preader_test.go`
- Tests: `TestNewPReader`
- Benchmarks: `BenchmarkPReader`
### `internal/s3select/json/reader_test.go`
- Tests: `TestNewReader`
- Benchmarks: `BenchmarkReader`
### `internal/s3select/jstream/decoder_test.go`
- Tests: `TestDecoderSimple`, `TestDecoderNested`, `TestDecoderFlat`, `TestDecoderMultiDoc`, `TestDecoderReaderFailure`, `TestDecoderMaxDepth`
### `internal/s3select/select_test.go`
- Tests: `TestJSONQueries`, `TestCSVQueries`, `TestCSVQueries2`, `TestCSVQueries3`, `TestCSVInput`, `TestJSONInput`, `TestCSVRanges`, `TestParquetInput`, `TestParquetInputSchema`, `TestParquetInputSchemaCSV`
### `internal/s3select/simdj/reader_amd64_test.go`
- Tests: `TestNDJSON`
### `internal/s3select/sql/jsonpath_test.go`
- Tests: `TestJsonpathEval`
### `internal/s3select/sql/parser_test.go`
- Tests: `TestJSONPathElement`, `TestJSONPath`, `TestIdentifierParsing`, `TestLiteralStringParsing`, `TestFunctionParsing`, `TestSqlLexer`, `TestSelectWhere`, `TestLikeClause`, `TestBetweenClause`, `TestFromClauseJSONPath`, `TestSelectParsing`, `TestSqlLexerArithOps`, `TestParseSelectStatement`
### `internal/s3select/sql/stringfuncs_contrib_test.go`
- Tests: `TestEvalSQLSubstring`
### `internal/s3select/sql/stringfuncs_test.go`
- Tests: `TestEvalSQLLike`
### `internal/s3select/sql/timestampfuncs_test.go`
- Tests: `TestParseAndDisplaySQLTimestamp`
### `internal/s3select/sql/value_test.go`
- Tests: `TestValue_SameTypeAs`, `TestValue_Equals`, `TestValue_CSVString`, `TestValue_bytesToInt`, `TestValue_bytesToFloat`, `TestValue_bytesToBool`
### `internal/store/batch_test.go`
- Tests: `TestBatchCommit`, `TestBatchCommitOnExit`, `TestBatchWithConcurrency`
### `internal/store/queuestore_test.go`
- Tests: `TestQueueStorePut`, `TestQueueStoreGet`, `TestQueueStoreDel`, `TestQueueStoreLimit`, `TestQueueStoreListN`, `TestMultiplePutGetRaw`, `TestMultiplePutGets`, `TestMixedPutGets`
### `internal/store/store_test.go`
- Tests: `TestKeyString`, `TestParseKey`

## Current gaps
The current 158 source-catalog scenarios and the local compatibility procedures cover behavior-level migration. Native implementation tests, generated serialization tests, benchmarks, build-only checks, external provider matrices, and repository automation remain separately classified above. They require their own native or CI adapter before they can be represented as black-box validation cases.

The local catalog also indexes 215 existing procedure functions from the S3,
security, tier, storage, and key-management suites, alongside 20 scanner
regression procedures. These entries retain the
original suite function and can be selected individually through the validation
dispatcher. A registered procedure still requires a live candidate run before
it can become an accepted result.

The local catalog additionally indexes 7 table-catalog procedures and 18
upgrade compatibility procedures. Table procedures select one `TBL-*` entry,
and upgrade procedures select one `UPG-*` verification from the existing
upgrade cycle. Both suites support dispatcher dry-runs and preserve their
original assertions and diagnostics.

Eight source test files contain no `Test*` or `Benchmark*` identifier in the
audit table. They remain inventory entries with no executable function to
crosswalk: `cmd/benchmark-utils_test.go`,
`internal/bucket/replication/datatypes_gen_test.go`,
`cmd/leak-detect_test.go`, `cmd/naughty-disk_test.go`,
`internal/dsync/dsync-client_test.go`, `internal/dsync/dsync-server_test.go`,
`internal/grid/grid_types_msgp_test.go`, and
`internal/grid/grid_types_test.go`.
