#!/usr/bin/env python3
"""Execution policy for Heal cases whose live proof needs special handling.

The recipe remains the source of the operator procedure.  This module only
classifies whether the dispatcher may enter that procedure and records the
reason when it cannot make a canonical PASS claim.
"""

from __future__ import annotations

import os
from typing import Mapping


ENABLE_ENV = "AUTO_TESTING_ENABLE_ENV_GATED_HEAL"
CAPABILITIES_ENV = "VALIDATION_HEAL_ENVIRONMENT_CAPABILITIES"


def _environment(
    code: str,
    reason_code: str,
    reason: str,
    capabilities: tuple[str, ...],
    *,
    profile: str = "cluster-1pool",
) -> dict:
    return {
        "kind": "environment-gated",
        "disabledByDefault": True,
        "dispatchable": True,
        "code": code,
        "reasonCode": reason_code,
        "reason": reason,
        "requiredCapabilities": list(capabilities),
        "enableVariable": ENABLE_ENV,
        "capabilitiesVariable": CAPABILITIES_ENV,
        "profile": profile,
    }


def _native(code: str, reason_code: str, reason: str, capabilities: tuple[str, ...]) -> dict:
    return {
        "kind": "evidence-blocked",
        "disabledByDefault": False,
        "dispatchable": False,
        "code": code,
        "reasonCode": reason_code,
        "reason": reason,
        "requiredCapabilities": list(capabilities),
        "enableVariable": None,
        "capabilitiesVariable": None,
    }


# These cases are procedure-complete, but the current auto-testing fleet does
# not provide the resources needed to run them safely.  They are dispatchable
# only through the explicit environment gate and are disabled by default.
ENVIRONMENT_GATED_CASES = {
    "heal.mixed-version-upgrade-rollback": _environment(
        "F08", "ENV_IMMUTABLE_RELEASE_PAIR",
        "需要两个已固定且相互兼容的 RustFS release、混合版本 peer 状态和经过审核的回滚兼容矩阵；当前 auto-testing Heal 节点没有这组不可变双版本资源。",
        ("immutable_release_pair", "mixed_version_peer_status", "rolling_upgrade_rollback", "rollback_compatibility_matrix"),
    ),
    "heal.dedicated-host-forced-restart": _environment(
        "F09", "ENV_DEDICATED_H6_HOST",
        "需要 H6 专用主机、平台强制停启权限、带外恢复和 boot_id/平台事件证据；共享 Heal 节点不能证明真实电源循环。",
        ("dedicated_h6_host", "platform_power_cycle", "out_of_band_recovery", "boot_id_evidence"),
    ),
    "heal.race-ilm-expiry-and-transition": _environment(
        "G07", "ENV_DEDICATED_ILM_TIER_WINDOW",
        "需要隔离 ILM/tier 租户、真实 Expire/Transition/Restore 窗口和远端版本映射；共享 ILM 环境不能安全制造交叠窗口。",
        ("dedicated_ilm_tier", "real_expire_transition_restore_window", "remote_version_mapping", "isolated_rule_cleanup"),
    ),
    "heal.race-replication-resync": _environment(
        "G08", "ENV_ISOLATED_REPLICATION_PAIR",
        "需要隔离的复制两端、resync 控制、跨端版本映射和 Heal/resync 实际交叠窗口；当前固定 fleet 没有独占复制对。",
        ("isolated_replication_pair", "resync_control", "cross_endpoint_version_mapping", "replication_overlap_window"),
    ),
    "heal.with-expansion-rebalance": _environment(
        "H01", "ENV_H5_EXPANSION_REBALANCE",
        "需要 H5 pool 扩容、rebalance 和 Heal 同时活动的完整拓扑，并逐对象保存 pool/set 迁移证据；当前 auto-testing 未绑定扩容前后布局与专用操作窗口。",
        ("h5_pool_expansion", "rebalance_task", "per_object_pool_set_mapping", "retired_scanner_evidence"),
    ),
    "heal.with-rebalance-stop-resume": _environment(
        "H02", "ENV_H5_REBALANCE_LIFECYCLE",
        "需要可观测的 H5 rebalance、停止/恢复/服务重启控制及独立 task 身份；当前 auto-testing 未绑定这两类任务的独立身份和操作窗口。",
        ("h5_rebalance", "rebalance_stop_resume", "independent_task_identity", "service_restart_control"),
    ),
    "heal.with-decommission": _environment(
        "H03", "ENV_H5_DECOMMISSION",
        "需要 H5 pool decommission、survivor 目标、写入限制和移动 ACK/物理权威证据；当前 auto-testing 未绑定本例退役前后拓扑和恢复方案。",
        ("h5_decommission", "survivor_pool", "decommission_write_guard", "movement_ack_physical_proof"),
    ),
    "heal.with-decommission-cancel-clear": _environment(
        "H04", "ENV_H5_DECOMMISSION_CANCEL",
        "需要退役取消/清理/新代次恢复以及 #2423 所需容量锁重试窗口；普通取消结果不能替代这个临界点。",
        ("h5_decommission", "cancel_clear_resume", "capacity_lock_retry_window", "generation_bound_ack_ledger"),
    ),
    "heal.after-retired-pool-removal": _environment(
        "H05", "ENV_H5_RETIRED_POOL_REMOVAL",
        "需要完整迁移后移除 retired pool、源保持离线和 survivor-only 访问；当前 auto-testing 未提供本例源持续离线和 survivor-only 的绑定证据。",
        ("h5_retired_pool_removal", "source_offline_proof", "survivor_authority", "retired_pool_write_guard"),
    ),
    "heal.ec84-multi-set-restart": _environment(
        "H07", "ENV_EC84_MULTI_SET_TOPOLOGY",
        "需要 H3 的 12 个独立盘和 H4 的至少 24 个独立 endpoint，并按 set 分别记录重启/崩溃窗口；当前 H4 资源不可用。",
        ("h3_ec84_12_independent_disks", "h4_ec84_24_independent_endpoints", "per_set_identity", "per_set_restart_crash_evidence"),
    ),
    "heal.slow-io-and-eio": _environment(
        "I01", "ENV_DEDICATED_FAULT_IO",
        "需要专用 faultfs 或故障设备、可恢复的 slow/short-read/EIO 注入和 namespace 隔离；共享盘模拟会污染其他运行。",
        ("dedicated_faultfs_or_device", "slow_io_short_read_eio", "namespace_isolation", "fault_layer_restore"),
    ),
    "heal.kms-and-tier-outage": _environment(
        "I06", "ENV_DEDICATED_KMS_TIER_TENANT",
        "需要专用 KMS/tier 租户、依赖网络隔离和安全的 key/tier 版本证据；不能在共享依赖上全局断网。",
        ("dedicated_kms_tier_tenant", "dependency_network_isolation", "key_redaction", "tier_version_mapping"),
    ),
    "heal.long-soak": _environment(
        "J05", "ENV_DEDICATED_LONG_SOAK_LANE",
        "必须在独占 H2/H5 lane 真实运行 24 小时和 72 小时，包含限速、监控、到期回收和完整资源预算；短跑不能替代 soak 证据。",
        ("dedicated_soak_lane", "wall_clock_24h_72h", "resource_budget_monitoring", "scheduled_fault_recovery"),
    ),
}


# These cases still lack complete orchestration/evidence, and I05 also lacks
# an exact service-side ownership barrier. More nodes alone do not close them.
EVIDENCE_BLOCKED_CASES = {
    "heal.bucket-incarnation-recreation": _native(
        "C11", "EVIDENCE_INCARNATION_LIVE_ORCHESTRATION",
        "已实现 H1/H2 旧元数据重返、真实 checkpoint/resume 回放、ACK 绑定的 committed MRF 回放和精确 native 证据门禁；尚缺全部现场分支的独立审计，诊断执行保持 FAIL/BLOCKED，原生前置通过不能替代 canonical 验收。",
        ("bucket_incarnation_isolation", "checkpoint_mrf_replay_guard", "incarnation_native_evidence_binding", "complete_live_orchestrator"),
    ),
    "heal.checkpoint-and-mrf-corruption": _native(
        "F07", "EVIDENCE_DURABLE_JOURNAL_ORCHESTRATION",
        "已有 journal 故障工具和 10 项候选版本持久化原生回归，但尚缺 H1/H2 全六分支编排、写失败诊断、责任保留和安全重扫；现有 publication interruption 证据也不等于每个 fsync 临界点或断电证据。",
        ("durable_checkpoint_mrf_fault", "write_failure_retention", "publication_boundary_evidence_binding", "complete_live_orchestrator"),
    ),
    "heal.lock-timeout-and-caller-abort": _native(
        "I05", "NATIVE_LOCK_OWNER_ABORT_BARRIER",
        "需要真实 lock owner、超时、调用方断开和 stale read-repair 的服务端所有权/提交顺序；黑盒请求耗时只能证明 TCP 断开，不能证明锁提交屏障，候选包没有 reviewed e2e-test-hooks。",
        ("server_lock_owner", "lock_timeout_fence", "caller_abort_sequence", "stale_read_repair_fence", "reviewed_e2e_test_hooks"),
    ),
    "heal.outcome-counters-and-error-truth": _native(
        "J02", "EVIDENCE_MIXED_OUTCOME_ORCHESTRATION",
        "已有 2 项候选版本 outcome 原生回归和部分计数校验，但尚缺真实混合 healthy/repaired/failed/deferred/unknown 夹具、dry-run 独立任务、逐对象完整 manifest 与计数/attemptFailures 对账；这是执行器与证据未闭环，并非已证实产品缺少 API。",
        ("mixed_outcome_fixture", "per_object_canonical_receipts", "complete_live_orchestrator", "attempt_failure_visibility"),
    ),
}

FIRST_BATCH = frozenset(ENVIRONMENT_GATED_CASES)
SECOND_BATCH = frozenset(EVIDENCE_BLOCKED_CASES)
DISPATCHABLE_CASES = FIRST_BATCH | frozenset(
    {
        "heal.queue-pressure-and-admission", "heal.admin-option-contract", "heal.admin-nolock-safety",
        "heal.race-unversioned-overwrite", "heal.race-versioned-put", "heal.race-version-delete",
        "heal.race-markers-and-batch-delete", "heal.race-multipart-complete-abort",
        "heal.race-copy-and-metadata-update", "heal.dangling-and-orphan-safety",
        "heal.conflicting-subquorum-authority", "heal.multi-pool-target-isolation",
        "heal.network-partition-and-lock-quorum", "heal.enospc-inodes-and-quota", "heal.disk-and-node-flapping",
        "heal.coordinator-process-crash", "heal.encrypted-objects", "heal.foreign-format-and-unknown-schema",
        "heal.below-write-quorum", "heal.bucket-metadata-integrity", "heal.admin-scope-isolation",
        "heal.admin-healthy-noop", "heal.admin-invalid-request", "heal.admin-token-cross-node-status",
        "heal.admin-dry-run", "heal.missing-part-file", "heal.data-shard-bitrot", "heal.parity-shard-bitrot",
        "heal.truncated-and-zeroed-shard", "heal.completed-multipart-object", "heal.range-head-and-conditions",
        "heal.manual-normal-attribution", "heal.manual-deep-attribution", "heal.performance-abba",
        "heal.historical-versions", "heal.repeat-idempotency", "heal.versioning-suspended-null",
        "heal.below-read-quorum", "heal.missing-or-corrupt-xlmeta", "heal.object-metadata-and-tags",
        "heal.multiple-shard-loss", "heal.special-keys-and-pagination", "heal.object-size-boundaries",
        "heal.admin-authorization", "heal.node-outage-rejoin", "heal.object-lock-retention",
        "heal.metadata-payload-mismatch", "heal.read-repair-attribution", "heal.disk-readonly-and-permission",
        "heal.scanner-cold-object-repair", "heal.delete-marker-semantics", "heal.status-partial-disabled-unknown",
        "heal.incremental-cursor-and-truncation", "heal.admin-stop-and-stale-token", "heal.cancelled-task-restart",
        "heal.admin-overlap-and-force-start", "heal.root-graceful-coordinator-restart", "heal.read-quorum-boundary",
        "heal.stale-version-rejoin", "heal.disk-loss-and-repair", "heal.disk-replacement-formatted",
        "heal.full-cluster-restart", "heal.replacement-auto-rebuild", "heal.target-graceful-restart",
        "heal.target-process-crash", "heal.mrf-offline-write-replay", "heal.disk-replacement-unformatted",
    }
)


def policy_for(case_id: str) -> dict | None:
    return ENVIRONMENT_GATED_CASES.get(case_id) or EVIDENCE_BLOCKED_CASES.get(case_id)


def enabled(environ: Mapping[str, str] | None = None) -> bool:
    values = environ if environ is not None else os.environ
    return values.get(ENABLE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}
