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
        "Requires two pinned, mutually compatible RustFS releases, mixed-version peer state, and a reviewed rollback-compatibility matrix; the current auto-testing Heal node does not have this immutable dual-version resource set.",
        ("immutable_release_pair", "mixed_version_peer_status", "rolling_upgrade_rollback", "rollback_compatibility_matrix"),
    ),
    "heal.dedicated-host-forced-restart": _environment(
        "F09", "ENV_DEDICATED_H6_HOST",
        "Requires a dedicated H6 host, platform force stop/start permission, out-of-band recovery, and boot_id/platform-event evidence; a shared Heal node cannot prove a real power cycle.",
        ("dedicated_h6_host", "platform_power_cycle", "out_of_band_recovery", "boot_id_evidence"),
    ),
    "heal.race-ilm-expiry-and-transition": _environment(
        "G07", "ENV_DEDICATED_ILM_TIER_WINDOW",
        "Requires an isolated ILM/tier tenant, a real Expire/Transition/Restore window, and remote version mapping; a shared ILM environment cannot safely create an overlap window.",
        ("dedicated_ilm_tier", "real_expire_transition_restore_window", "remote_version_mapping", "isolated_rule_cleanup"),
    ),
    "heal.race-replication-resync": _environment(
        "G08", "ENV_ISOLATED_REPLICATION_PAIR",
        "Requires isolated replication endpoints, resync control, cross-endpoint version mapping, and an actual Heal/resync overlap window; the current fixed fleet has no exclusive replication pair.",
        ("isolated_replication_pair", "resync_control", "cross_endpoint_version_mapping", "replication_overlap_window"),
    ),
    "heal.with-expansion-rebalance": _environment(
        "H01", "ENV_H5_EXPANSION_REBALANCE",
        "Requires a complete topology in which H5 pool expansion, rebalance, and Heal are active at the same time, with pool/set migration evidence saved per object; current auto-testing has not bound the layout before and after expansion or a dedicated operation window.",
        ("h5_pool_expansion", "rebalance_task", "per_object_pool_set_mapping", "retired_scanner_evidence"),
    ),
    "heal.with-rebalance-stop-resume": _environment(
        "H02", "ENV_H5_REBALANCE_LIFECYCLE",
        "Requires observable H5 rebalance, stop/resume/service-restart control, and independent task identities; current auto-testing has not bound the independent identities and operation windows of these two task classes.",
        ("h5_rebalance", "rebalance_stop_resume", "independent_task_identity", "service_restart_control"),
    ),
    "heal.with-decommission": _environment(
        "H03", "ENV_H5_DECOMMISSION",
        "Requires H5 pool decommission, a survivor target, write restrictions, and move-ACK and physical-authority evidence; current auto-testing has not bound this case's topology before and after decommission, or its recovery plan.",
        ("h5_decommission", "survivor_pool", "decommission_write_guard", "movement_ack_physical_proof"),
    ),
    "heal.with-decommission-cancel-clear": _environment(
        "H04", "ENV_H5_DECOMMISSION_CANCEL",
        "Requires decommission cancellation, cleanup, and new-generation recovery, plus the capacity-lock retry window required by #2423; an ordinary cancellation result cannot take the place of this boundary.",
        ("h5_decommission", "cancel_clear_resume", "capacity_lock_retry_window", "generation_bound_ack_ledger"),
    ),
    "heal.after-retired-pool-removal": _environment(
        "H05", "ENV_H5_RETIRED_POOL_REMOVAL",
        "Requires the retired pool to be removed after a complete migration, the source to stay offline, and survivor-only access; current auto-testing has not provided bound evidence for this case that the source stays offline and that access is survivor-only.",
        ("h5_retired_pool_removal", "source_offline_proof", "survivor_authority", "retired_pool_write_guard"),
    ),
    "heal.ec84-multi-set-restart": _environment(
        "H07", "ENV_EC84_MULTI_SET_TOPOLOGY",
        "Requires 12 independent disks for H3 and at least 24 independent endpoints for H4, with the restart and crash windows recorded per set; H4 resources are currently unavailable.",
        ("h3_ec84_12_independent_disks", "h4_ec84_24_independent_endpoints", "per_set_identity", "per_set_restart_crash_evidence"),
    ),
    "heal.slow-io-and-eio": _environment(
        "I01", "ENV_DEDICATED_FAULT_IO",
        "Requires a dedicated faultfs or fault device, recoverable slow/short-read/EIO injection, and namespace isolation; simulating on a shared disk would contaminate other runs.",
        ("dedicated_faultfs_or_device", "slow_io_short_read_eio", "namespace_isolation", "fault_layer_restore"),
    ),
    "heal.kms-and-tier-outage": _environment(
        "I06", "ENV_DEDICATED_KMS_TIER_TENANT",
        "Requires a dedicated KMS/tier tenant, network isolation of the dependency, and safe key/tier version evidence; do not cut the network globally on a shared dependency.",
        ("dedicated_kms_tier_tenant", "dependency_network_isolation", "key_redaction", "tier_version_mapping"),
    ),
    "heal.long-soak": _environment(
        "J05", "ENV_DEDICATED_LONG_SOAK_LANE",
        "Must actually run for 24 hours and for 72 hours on an exclusive H2/H5 lane, including rate limiting, monitoring, expiry reclamation, and a complete resource budget. A short run cannot substitute for soak evidence.",
        ("dedicated_soak_lane", "wall_clock_24h_72h", "resource_budget_monitoring", "scheduled_fault_recovery"),
    ),
}


# These cases still lack complete orchestration/evidence, and I05 also lacks
# an exact service-side ownership barrier. More nodes alone do not close them.
EVIDENCE_BLOCKED_CASES = {
    "heal.bucket-incarnation-recreation": _native(
        "C11", "EVIDENCE_INCARNATION_LIVE_ORCHESTRATION",
        "H1/H2 old-metadata rejoin, real checkpoint/resume replay, ACK-bound committed MRF replay, and the precise native evidence gate are implemented; an independent audit of every live branch is still missing, diagnostic execution stays FAIL/BLOCKED, and a passing native prerequisite cannot replace canonical acceptance.",
        ("bucket_incarnation_isolation", "checkpoint_mrf_replay_guard", "incarnation_native_evidence_binding", "complete_live_orchestrator"),
    ),
    "heal.checkpoint-and-mrf-corruption": _native(
        "F07", "EVIDENCE_DURABLE_JOURNAL_ORCHESTRATION",
        "Implemented H1/H2 checkpoint truncation, future schema, replay of a real earlier generation, a committed MRF CRC fault, real journal ENOSPC on a private quota volume, and an independent safe rescan; the 10 native regressions are only prerequisite evidence. Still missing are review of every required fsync/commit boundary and an independent live audit of the complete live branches. Diagnostic execution cannot raise canonical acceptance.",
        ("durable_checkpoint_mrf_fault", "write_failure_retention", "publication_boundary_evidence_binding", "complete_live_orchestrator"),
    ),
    "heal.lock-timeout-and-caller-abort": _native(
        "I05", "NATIVE_LOCK_OWNER_ABORT_BARRIER",
        "A native subset from the same candidate source already exists for lock owner, cancelled physical unlink, and stale read-repair. The H1/H2 timeout and queued-successor diagnostics have both passed an independent live audit (39767336741/1, 39817606025/1), including the original ACKs, historical generations, real lock events, each peer inventory, new-shard integrity after shutdown, and bounded FD cleanup. Still missing are precise ownership at the actual disconnect boundary, a complete native timeout/stale barrier, the stale-generation controller, and the complete H1/H2 DL/DV object/ACK/physical/lock/FD closed loop. Precise source cfg(test) native evidence is retained on its own; the diagnostic subset and the native subset cannot stand in for a complete canonical PASS.",
        ("server_lock_owner", "lock_timeout_fence", "caller_abort_sequence", "stale_read_repair_fence", "exact_source_native_evidence", "complete_live_orchestrator", "final_object_lock_fd_closure"),
    ),
    "heal.outcome-counters-and-error-truth": _native(
        "J02", "EVIDENCE_MIXED_OUTCOME_ORCHESTRATION",
        "Execution flows already exist for mixed healthy/repaired/failed, an independent dry-run, dependency deferral, a real SDK legacy Unknown, an original all-replica corrupt-metadata Unknown, read-only permanent failure with new-token compensation, and version-level grace Deferred with new-token cleanup, all reconciled per object against the manifest and attemptFailures. Complete serial live evidence and independent acceptance are still required, so default dispatch and canonical admission stay prohibited; a local check cannot serve as a complete PASS.",
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
