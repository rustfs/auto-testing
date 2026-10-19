#!/usr/bin/env python3
"""F07 checkpoint corruption prerequisites; incomplete branches stay blocked."""

import argparse
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys
import time

try:
    from . import checkpoint as checkpoints, journal_fault as journal
    from .heal_probe import Probe, canonical, compare_inventories, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .manager_fixture import verify as verify_manager
    from .matrix import select
    from .shards import files, map_target, run_task, validate_fault, validate_repaired_physical
    from .stop import idle_fleet, status
    from .root_restart import recovery_status, same_owner, TIMEOUT_SECONDS
    from .scanner_repair import JournalWindow
    from .fleet_node import PROCESS_FIELDS
    from .persistence_mrf import run as run_mrf_checksum
    from .versions import public_version_id, verify_proof, version_identity
except ImportError:
    import checkpoint as checkpoints, journal_fault as journal
    from heal_probe import Probe, canonical, compare_inventories, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from manager_fixture import verify as verify_manager
    from matrix import select
    from shards import files, map_target, run_task, validate_fault, validate_repaired_physical
    from stop import idle_fleet, status
    from root_restart import recovery_status, same_owner, TIMEOUT_SECONDS
    from scanner_repair import JournalWindow
    from fleet_node import PROCESS_FIELDS
    from persistence_mrf import run as run_mrf_checksum
    from versions import public_version_id, verify_proof, version_identity


CASE_ID = "heal.checkpoint-and-mrf-corruption"
BRANCHES = ("checkpoint-truncate", "mrf-checksum", "future-schema", "stale-generation",
            "journal-write-failure", "native-persist-fence")
IMPLEMENTED = {"checkpoint-truncate": "truncate", "future-schema": "future-schema",
               "stale-generation": "stale-generation"}
BLOCKERS = {}
SIZE = 5 * 1024**2 + 123
TERMINAL_OPTIONS_REFERENCE = {"candidateCommits": (
    "072b57b37559fbe626aeda8abf61617fdcbdd926", "1aa9be87d18cf941e5020f19e8622c26ba4b10ac"),
    "sourceFiles": {
        "crates/heal/src/heal/manager/scheduler.rs": "79ce1f9c278c664231b6a1eca7c521f8e98742fe6d17d8f852368da4e77cc33b",
        "crates/heal/src/heal/manager.rs": "ba9289942fbbe8ce2632686ae86b9d1be0ae9923420e3b091d67148b4ee02094",
        "crates/config/src/constants/heal.rs": "82b8614dfa7cc111e694138035e4a84ee23035c719f0286fc0565d7181ca014f"},
    "runtimeSetting": {"RUSTFS_HEAL_TASK_TIMEOUT_SECS": str(TIMEOUT_SECONDS)},
    "normalization": "scheduler get_or_insert fills only an explicit null timeout; all other fields stay exact"}


def checkpoint_fixture(branch, fixture):
    require(branch in {*IMPLEMENTED, "journal-write-failure"} and fixture in {"DB", "DV"},
            "unreviewed checkpoint fixture")
    if branch == "journal-write-failure":
        # Keep all data and inodes inside the four fixed 128 MiB filesystems.
        return (169 if fixture == "DB" else 36), 1024**2 + 123
    if branch == "stale-generation":
        # More real healthy responsibilities keep both durable generations
        # observable without changing task deadlines or synthesizing records.
        return (511 if fixture == "DB" else 144), 2 * 1024**2 + 123
    return (257 if fixture == "DB" else 80), SIZE


def same_terminal_options(admitted, terminal):
    if not isinstance(admitted, dict) or not isinstance(terminal, dict):
        return False
    if journal.same_json(admitted, terminal):
        return True
    # The source-bound scheduler fills None from the process-verified fleet
    # setting. It preserves explicit budgets, including a checkpointed one.
    if "timeout" not in admitted or admitted["timeout"] is not None:
        return False
    return journal.same_json(terminal, {**admitted, "timeout": {"secs": TIMEOUT_SECONDS, "nanos": 0}})


def exact_records(observed, token, family, *, allow_frozen=False):
    names = journal.names(family, token)
    require(isinstance(observed, dict) and len(observed) in {1, 4}, "journal omitted bound physical nodes")
    present = {}
    for node, census in observed.items():
        rows = census.get("records")
        require(census.get("readOnly") is True and (census.get("quiescent") is True
                or allow_frozen and census.get("stable") is True and census.get("serviceFrozen") is True
                    and census.get("processBound") is True and census.get("independentReads") == 2)
                and census.get("family") == family and census.get("taskId") == token
                and census.get("directoryEnumerationUsed") is False
                and isinstance(rows, list) and len(rows) == 4 * len(names),
                "retention proof needs a stopped exact-path census")
        seen = set()
        for row in rows:
            pair = row.get("disk"), row.get("relativePath")
            require(type(pair[0]) is int and 0 <= pair[0] < 4 and pair[1] in names and pair not in seen
                    and type(row.get("present")) is bool, "journal census duplicated or escaped an exact disk/path")
            seen.add(pair)
            if row["present"]:
                checkpoints.bytes_of(row)
                require(pair[1] not in present, "journal record has multiple physical owners")
                present[pair[1]] = {"node": node, **row}
            else:
                require(not any(name in row for name in ("size", "sha256", "rawBase64")),
                        "absent journal record contains invented bytes")
    return present


def failed_task(current):
    outcome = current.get("outcome") or {}
    require(current.get("summary") == "stopped" and current.get("settings") == checkpoints.OPTIONS
            and outcome.get("execution") == {"state": "aborted", "reason": "untraversable"}
            and outcome.get("coverage") == "partial"
            and isinstance(current.get("detail"), str) and "checkpoint" in current["detail"].lower(),
            "corrupt original checkpoint was silently acknowledged, cancelled or lost its diagnostic")
    counters = outcome.get("counters") or {}
    require(all(type(counters.get(name)) is int and counters[name] == 0 for name in
                ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures"))
            and counters.get("overflowed") is False and outcome.get("objects") == []
            and outcome.get("objectsTruncated") is False,
            "checkpoint load failure invented object processing or repair")


def retained_corruption(observed, roots, frozen, receipt, current, *, earlier=None):
    token = frozen["taskId"]
    failed_task(current)
    require(receipt.get("applied") is True and receipt.get("originalBackupRetained") is True
            and receipt.get("productRepairClaimed") is False
            and receipt.get("family") == "checkpoint" and receipt.get("taskId") == token
            and receipt.get("disk") == frozen["disk"]
            and receipt.get("relativePath") == frozen["checkpoint"]["relativePath"]
            and journal.same_json(receipt.get("before"), journal.entry(checkpoints.bytes_of(frozen["checkpoint"]))),
            "journal injection receipt changed the frozen original task or bytes")
    if receipt.get("kind") == "stale-generation": stale_receipt(earlier, frozen, receipt)
    require(receipt.get("kind") in IMPLEMENTED.values()
            and receipt.get("injected") == journal.entry(journal.transform(
                checkpoints.bytes_of(frozen["checkpoint"]), receipt, {"bucket": frozen["bucket"]}, None,
                earlier=checkpoints.bytes_of(earlier["checkpoint"]) if earlier is not None else None)),
            "journal corruption differs from its reviewed transformation")
    records = exact_records(observed, token, "checkpoint")
    names = journal.names("checkpoint", token)
    require(set(records) == set(names[:3]), "corrupt checkpoint/resume/blocked fence was deleted or unexpectedly replaced")
    for row in records.values():
        require((row["node"], row["disk"]) == (frozen["node"], frozen["disk"]),
                "corrupt checkpoint or diagnostic moved to another physical owner")
    require(journal.entry(checkpoints.bytes_of(records[names[0]])) == receipt.get("injected")
            and checkpoints.bytes_of(records[names[1]]) == checkpoints.bytes_of(frozen["resume"])
            and checkpoints.bytes_of(records[names[2]]) == b"blocked",
            "product overwrote unknown bytes, changed resume ownership or omitted its blocked diagnostic")
    root = exact_records(roots, token, "root")
    root_names = journal.names("root", token)
    # An explicit durable failed terminal may retire the runnable admission.
    # The corruption and blocked checkpoint remain independently retained.
    require(set(root) in (set(root_names[1:]), set(root_names)),
            "original responsibility has no exact durable failure terminal and report")
    owner = frozen["rootAdmission"]
    require(all((row["node"], row["disk"]) == (owner["node"], owner["disk"]) for row in root.values()),
            "durable failure report changed the original root owner")
    terminal = journal.decode_json(checkpoints.bytes_of(root[root_names[1]]))
    report = journal.decode_json(checkpoints.bytes_of(root[root_names[2]]))
    require(type(terminal.get("schema")) is int and terminal["schema"] == 1 and terminal.get("task_id") == token
            and journal.same_json(terminal.get("heal_type"), owner["value"]["heal_type"])
            and same_terminal_options(owner["value"]["options"], terminal.get("options"))
            and isinstance(terminal.get("status"), dict) and set(terminal["status"]) == {"Failed"}
            and isinstance(terminal["status"]["Failed"], dict)
            and isinstance(terminal["status"]["Failed"].get("error"), str)
            and "checkpoint" in terminal["status"]["Failed"]["error"].lower()
            and type(report.get("schema")) is int and report["schema"] == 1
            and journal.same_json(report.get("terminal"), terminal)
            and canonical(report.get("outcome")) == canonical(current["outcome"]),
            "original responsibility lost its durable checkpoint failure diagnostic")
    if root_names[0] in root:
        require(checkpoints.bytes_of(root[root_names[0]]) == checkpoints.bytes_of(owner),
                "retained runnable admission changed original responsibility")
    return {"originalTaskId": token, "corruptBytesRetained": True, "resumeBytesRetained": True,
            "blockedDiagnosticRetained": True, "durableOriginalFailureRetained": True,
            "originalTaskRepairClaimed": False, "backupRescueUsed": False}


def rescan_outcome(current, bucket, expected, repairs, incarnation):
    wanted = {version_identity(entry): entry for entry in expected}
    require(wanted and len(wanted) == len(expected) and repairs <= set(wanted), "rescan manifest duplicated or escaped ACKs")
    outcome = current.get("outcome") or {}
    # HealTaskStatusPayload omits a false legacy truncation flag. Canonical
    # receipt truncation remains explicit and independently reconciled below.
    require(current.get("summary") == "finished" and outcome.get("execution") == {"state": "completed"}
            and current.get("settings") == checkpoints.OPTIONS and type(current.get("truncated", False)) is bool
            and outcome.get("coverage") == "complete"
            and outcome.get("objectsTruncated") is (len(wanted) > 128), "safe rescan lacks complete truthful coverage")
    counts = {"processed": len(wanted), "healed": len(repairs), "unchanged": len(wanted) - len(repairs),
              "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(outcome.get("counters", {}).get(name)) is type(value)
                and outcome["counters"][name] == value for name, value in counts.items()),
            "safe rescan counters differ from independent complete ACK and physical manifests")
    objects = outcome.get("objects")
    require(isinstance(objects, list) and len(objects) == min(128, len(wanted)), "rescan omitted bounded diagnostics")
    seen = set()
    for receipt in objects:
        identity = receipt.get("identity") or {}
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(pair in wanted and pair not in seen and identity.get("kind") == "object"
                and identity.get("bucket") == bucket and identity.get("bucketIncarnationId") == incarnation
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0,
                "rescan diagnostic changed an ACK identity, incarnation or set")
        state = "repaired" if pair in repairs else "metadata_healthy" if wanted[pair]["deleteMarker"] else "verified_healthy"
        require(receipt.get("disposition", {}).get("state") == state, "rescan diagnostic invented repair or hid unknown work")
        seen.add(pair)
    return {"canonicalCounters": counts, "completeIndependentManifestsRequired": True,
            "boundedDiagnosticsAreCompleteManifest": len(wanted) <= 128, "originalFailedTaskReclassified": False}


def stale_receipt(earlier, frozen, receipt):
    require(isinstance(earlier, dict), "stale rollback has no original earlier generation")
    checkpoints.after_generation(frozen, earlier)
    require(earlier["value"]["admin"]["outcome"]["counters"]["healed"] == 0,
            "NOT_OBSERVED: earlier capture is after the fixture's first actual repair")
    require(receipt.get("kind") == "stale-generation" and receipt.get("family") == "checkpoint"
            and receipt.get("taskId") == frozen["taskId"] and type(receipt.get("disk")) is int
            and receipt["disk"] == frozen["disk"]
            and receipt.get("relativePath") == frozen["checkpoint"]["relativePath"]
            and receipt.get("captureId") == earlier["captureId"]
            and journal.same_json(receipt.get("before"), journal.entry(checkpoints.bytes_of(frozen["checkpoint"])))
            and journal.same_json(receipt.get("injected"), journal.entry(checkpoints.bytes_of(earlier["checkpoint"])))
            and journal.same_json(receipt.get("capturedGeneration"), receipt["injected"])
            and receipt.get("applied") is True and receipt.get("originalBackupRetained") is True
            and receipt.get("productRepairClaimed") is False,
            "stale injection changed the original task, physical owner or earlier product bytes")
    return {"originalTaskId": frozen["taskId"], "exactEarlierProductBytesInstalled": True,
            "oldCheckpointManagerCasClaimed": False, "staleRecordRejectionClaimed": False}


def completed_original(roots, frozen, current):
    token, owner = frozen["taskId"], frozen["rootAdmission"]
    records = exact_records(roots, token, "root")
    names = journal.names("root", token)
    require(set(records) == set(names[1:]) and all((row["node"], row["disk"]) ==
            (owner["node"], owner["disk"]) for row in records.values()),
            "original replay lost its terminal/report owner or retained runnable responsibility")
    terminal = journal.decode_json(checkpoints.bytes_of(records[names[1]]))
    report = journal.decode_json(checkpoints.bytes_of(records[names[2]]))
    require(type(terminal.get("schema")) is int and terminal["schema"] == 1
            and terminal.get("task_id") == token and terminal.get("status") == "Completed"
            and journal.same_json(terminal.get("heal_type"), owner["value"]["heal_type"])
            and same_terminal_options(owner["value"]["options"], terminal.get("options"))
            and type(report.get("schema")) is int and report["schema"] == 1
            and journal.same_json(report.get("terminal"), terminal)
            and journal.same_json(report.get("outcome"), current.get("outcome")),
            "original replay silently retired work or changed its admitted scope/options/outcome")
    return {"sameOriginalTaskAndPhysicalOwner": True, "durableCompletionMatchesFullManifest": True,
            "originalResponsibilityRecreated": False, "backupRescueUsed": False}


def successful_stale_replay(fleet, probe, binding, expected, baseline, original, targets,
                            frozen, earlier, receipt, fault_scene, current, active):
    token, bucket = frozen["taskId"], frozen["bucket"]
    stale_receipt(earlier, frozen, receipt)
    indexed = files(fault_scene)
    missing = [target for target in targets if (target["node"], target["disk"], target["relativePath"]) not in indexed]
    require(missing, "NOT_OBSERVED: rollback had no remaining actual object repair responsibility")
    proof = rescan_outcome(current, bucket, expected, {version_identity(target) for target in missing}, frozen["bucketIncarnationId"])
    proof.pop("originalFailedTaskReclassified")
    active.remove((checkpoints.PATH, token))
    idle_fleet(fleet, binding)
    fleet.stop()
    roots = checkpoints.records(fleet, token, "completed-original-stale-replay", quiescent=True, family="root")
    durable = completed_original(roots, frozen, current)
    healed = fleet.snapshot("original-replay-physical-before-any-rescan-or-GET", quiescent=True)
    fleet.record("complete-original-replay-physical-proof", validate_repaired_physical(original, healed, targets[0], repair_targets=targets))
    for target in targets: verify_proof(fleet, target, "original-replay-full-cohort-integrity")
    fleet.start()
    admissions = []
    rescanned = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
        option_overrides={"recursive": True}, admissions=admissions)
    require(len(admissions) == 1 and admissions[0]["clientToken"] != token,
            "independent stale-record rescan reused the original responsibility")
    rescan = rescan_outcome(rescanned, bucket, expected, set(), frozen["bucketIncarnationId"])
    repeated = recovery_status(probe, fleet, token)
    require(all(canonical(repeated.get(name)) == canonical(current.get(name)) for name in ("summary", "settings", "outcome", "truncated")),
            "independent rescan rewrote the original replay result")
    for endpoint in binding["endpoints"]:
        peer = Probe(binding, endpoint)
        same = status(peer, fleet, checkpoints.PATH, token, "same-original-stale-replay-peer")
        require(all(canonical(same.get(name)) == canonical(current.get(name)) for name in ("summary", "settings", "outcome", "truncated")),
                "original stale replay token did not converge on every peer")
        inventory = fleet.record("complete-peer-S3", {"endpoint": endpoint, **peer.inventory(bucket,
            unversioned_current_tags=expected[0]["versionId"] == "null")})
        verify_seed(expected, inventory); compare_inventories(baseline, inventory)
    fleet.stop()
    require(physical_content(healed) == physical_content(fleet.snapshot("physical-after-rescan-and-business-reads", quiescent=True)),
            "rescan or business reads repaired work missing from the original replay proof")
    retained = checkpoints.records(fleet, token, "retained-original-stale-terminal-after-rescan", quiescent=True, family="root")
    completed_original(retained, frozen, repeated)
    require(canonical(exact_records(retained, token, "root")) == canonical(exact_records(roots, token, "root")),
            "rescan or business reads changed original durable completion bytes")
    fleet.record("complete-original-generation-reconciliation", {"earlier": earlier, "later": frozen,
        "injection": receipt, "originalCompletion": proof, "durableCompletion": durable, "separateRescan": rescan,
        "oldRecordResurrectionUsed": False, "canonicalCasePassClaimed": False})
    # A completed product task may remove its checkpoint. Exact original-byte
    # evidence and full repair proof precede discarding rescue registrations.
    fleet.journal_faults.clear()
    fleet.faults.clear()
    return {"verdict": "PASS", "originalGenerationReconciliation": proof, "durableOriginalCompletion": durable,
            "separateRescan": rescan, "staleRecordRejectionClaimed": False,
            "completePhysicalAndS3ManifestsVerified": True, "canonicalCasePassClaimed": False}


def retained_write_failure(observed, roots, frozen, current, events):
    token = frozen["taskId"]
    outcome = current.get("outcome") or {}
    detail = current.get("detail", "")
    require(current.get("summary") == "stopped" and current.get("settings") == checkpoints.OPTIONS
            and outcome.get("execution") == {"state": "aborted", "reason": "untraversable"}
            and outcome.get("coverage") == "partial" and isinstance(detail, str)
            and "failed to save checkpoint" in detail.lower()
            and any(word in detail.lower() for word in ("no space", "enospc", "os error 28")),
            "journal write refusal was not an actual checkpoint ENOSPC diagnostic")
    expected = frozen["value"]["admin"]["outcome"]
    require(all(canonical(outcome.get(name)) == canonical(expected.get(name))
            for name in ("counters", "objects", "objectsTruncated")),
            "failed checkpoint write lost or invented acknowledged durable progress")
    owner = checkpoints.owned_checkpoint(observed, token, frozen["bucket"])
    require(owner["checkpoint"] == frozen["checkpoint"] and owner["resume"] == frozen["resume"],
            "journal failure removed or rewrote the last original durable checkpoint/resume")
    root = exact_records(roots, token, "root", allow_frozen=True)
    names, original = journal.names("root", token), frozen["rootAdmission"]
    require(set(root) in ({names[0]}, set(names[1:]), set(names))
            and all((row["node"], row["disk"]) == (original["node"], original["disk"]) for row in root.values()),
            "journal failure silently retired or changed its original durable owner")
    if names[0] in root:
        raw = checkpoints.bytes_of(root[names[0]])
        if raw != checkpoints.bytes_of(original):
            # checkpoint_failed_execution may consume the original timeout;
            # every other admitted field and the physical owner stay fixed.
            same_owner(original, {**root[names[0]], "value": journal.decode_json(raw)})
    if names[1] in root:
        terminal = journal.decode_json(checkpoints.bytes_of(root[names[1]]))
        report = journal.decode_json(checkpoints.bytes_of(root[names[2]]))
        require(type(terminal.get("schema")) is int and terminal["schema"] == 1 and terminal.get("task_id") == token
                and journal.same_json(terminal.get("heal_type"), original["value"]["heal_type"])
                and same_terminal_options(original["value"]["options"], terminal.get("options"))
                and terminal.get("status") == {"Failed": {"error": detail}}
                and type(report.get("schema")) is int and report["schema"] == 1
                and journal.same_json(report.get("terminal"), terminal)
                and journal.same_json(report.get("outcome"), outcome),
                "journal failure lost its exact durable original failed terminal/report")
    diagnostics = [event for values in events.values() for event in values
        if event.get("target") == "rustfs::heal::task" and event.get("message") == "Heal task failed"
        and event.get("task_id") == token and isinstance(event.get("error"), str)
        and "failed to save checkpoint" in event["error"].lower()
        and any(word in event["error"].lower() for word in ("no space", "enospc", "os error 28"))]
    require(diagnostics, "NOT_OBSERVED: candidate has no original-task checkpoint write error")
    return {"originalTaskId": token, "lastDurableProgressRetained": True, "originalResponsibilityRetained": True,
            "originalAdmissionRetained": names[0] in root,
            "actualCheckpointWriteFailure": True, "diagnostics": diagnostics,
            "failedWriteAcknowledged": False, "originalFailureReclassified": False, "backupRescueUsed": False}


def recover_write_before_rescan(fleet, probe, binding, expected, baseline, original, targets,
                                frozen, failed, failure_proof, failure_scene, remaining, events, active):
    token, bucket = frozen["taskId"], frozen["bucket"]
    # Export the first failed attempt before product recovery may legitimately
    # replace its runtime status with a successful replay of the same token.
    fleet.record("original-write-failure-before-recovery", {"firstFailedAttempt": failed, "proof": failure_proof})
    fleet.stop()
    require(physical_content(fleet.snapshot("write-failure-before-original-recovery", quiescent=True))
            == physical_content(failure_scene), "space release changed the original failed repair scene")
    retained_write_failure(checkpoints.records(fleet, token, "write-failure-checkpoint-before-recovery", quiescent=True),
        checkpoints.records(fleet, token, "write-failure-root-before-recovery", quiescent=True, family="root"),
        frozen, failed, events)
    fleet.start()
    deadline = time.monotonic() + 180
    while True:
        require(time.monotonic() < deadline, "released original write responsibility did not reach a bounded recovery outcome")
        current = recovery_status(probe, fleet, token)
        require(time.monotonic() < deadline, "released original write responsibility did not reach a bounded recovery outcome")
        if current.get("summary") != "running": break
        time.sleep(.2)
    fleet.stop()
    roots = checkpoints.records(fleet, token, "write-original-root-after-recovery", quiescent=True, family="root")
    if current.get("summary") != "finished":
        require(all(canonical(current.get(name)) == canonical(failed.get(name))
                    for name in ("summary", "settings", "outcome", "detail")),
                "original write recovery neither retained its failure nor completed verified work")
        retained = retained_write_failure(
            checkpoints.records(fleet, token, "write-original-checkpoint-after-recovery", quiescent=True),
            roots, frozen, current, events)
        require(retained["originalAdmissionRetained"] is False,
                "original write responsibility still owns its durable scope after recovery; separate rescan is not admitted")
        require(physical_content(fleet.snapshot("write-original-failed-recovery-physical", quiescent=True))
                == physical_content(failure_scene), "failed original recovery changed its acknowledged repair scene")
        fleet.start()
        return None
    completion = rescan_outcome(current, bucket, expected,
        {version_identity(target) for target in remaining}, frozen["bucketIncarnationId"])
    completion.pop("originalFailedTaskReclassified")
    durable = completed_original(roots, frozen, current)
    healed = fleet.snapshot("write-original-replay-physical-before-rescan-or-GET", quiescent=True)
    fleet.record("write-original-replay-complete-physical-proof",
        validate_repaired_physical(original, healed, targets[0], repair_targets=targets))
    for target in targets: verify_proof(fleet, target, "write-original-replay-full-shard-integrity")
    fleet.start()
    admissions = []
    rescanned = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
        option_overrides={"recursive": True}, admissions=admissions)
    require(len(admissions) == 1 and admissions[0]["clientToken"] != token,
            "write recovery rescan reused or relabelled the original responsibility")
    rescan = rescan_outcome(rescanned, bucket, expected, set(), frozen["bucketIncarnationId"])
    repeated = recovery_status(probe, fleet, token)
    require(all(canonical(repeated.get(name)) == canonical(current.get(name))
                for name in ("summary", "settings", "outcome", "detail", "truncated")),
            "separate rescan rewrote the original write recovery result")
    for endpoint in binding["endpoints"]:
        peer = Probe(binding, endpoint)
        same = status(peer, fleet, checkpoints.PATH, token, "same-original-write-replay-peer")
        require(all(canonical(same.get(name)) == canonical(current.get(name))
                    for name in ("summary", "settings", "outcome", "detail", "truncated")),
                "original write replay did not converge on every peer")
        inventory = fleet.record("complete-peer-S3", {"endpoint": endpoint, **peer.inventory(bucket,
            unversioned_current_tags=expected[0]["versionId"] == "null")})
        verify_seed(expected, inventory); compare_inventories(baseline, inventory)
    fleet.stop()
    require(physical_content(healed) == physical_content(fleet.snapshot("write-replay-after-rescan-and-reads", quiescent=True)),
            "rescan or business reads repaired work missing from the original write replay proof")
    retained = checkpoints.records(fleet, token, "retained-write-original-completion", quiescent=True, family="root")
    completed_original(retained, frozen, repeated)
    require(canonical(exact_records(retained, token, "root")) == canonical(exact_records(roots, token, "root")),
            "rescan or business reads changed the original write replay durable completion")
    replay = {"realSameTaskReplayBeforeSeparateRescan": True, "failedFirstAttemptRetained": True,
              "separateRescanIsOriginalTaskCompletion": False, "durableOriginalCompletion": durable}
    fleet.record("complete-original-journal-write-reconciliation", {"firstFailedAttempt": failed,
        "retainedFailureProof": failure_proof, "originalReplayCompletion": current,
        "originalReplayPhysicalAndCounters": completion, "separateRescan": rescan, "restartProof": replay})
    fleet.faults.clear()
    return {"verdict": "PASS", "originalWriteFailure": failure_proof, "separateRescan": rescan,
            "originalAfterRestart": replay, "completePhysicalAndS3ManifestsVerified": True, "canonicalCasePassClaimed": False}


def journal_write_failure(fleet, probe, binding, expected, baseline, original, targets, frozen, active):
    token, bucket = frozen["taskId"], frozen["bucket"]
    require(frozen["value"]["admin"]["outcome"]["counters"]["healed"] == 0,
            "NOT_OBSERVED: journal failure missed the checkpoint before any actual repair")
    window = JournalWindow(fleet, "F07-original-checkpoint-write-failure", event_targets={"rustfs::heal::task", "rustfs::heal::manager"})
    for node in fleet.nodes:
        filled = fleet.record("stopped-private-journal-exhaustion", {"node": node,
            **fleet.remote(node, "bounded-volumes-fill", quiescent=True)})
        require(filled.get("quiescent") is True and filled.get("hostSharedMountRemounted") is False
                and filled.get("candidateProcessWitnessRequired") is True
                and len(filled.get("changes", [])) == 4
                and all(row.get("actualErrno") == 28 and row.get("freeBlocks") == 0 for row in filled["changes"]),
                "journal failure did not exhaust only its four fixed owned volumes")
    fleet.start()
    for node in fleet.nodes:
        identity = fleet.remote(node, "process")
        witness = fleet.record("candidate-private-journal-ENOSPC", {"node": node,
            **fleet.remote(node, "bounded-volumes-witness", expected=identity)})
        require(witness.get("sameOriginalProcess") is True and witness.get("hostSharedMountRemounted") is False
                and len(witness.get("disks", [])) == 4 and all(row.get("writeErrno") == 28
                    and row.get("sameServiceUidAndMountNamespace") is True for row in witness["disks"]),
                "journal write fault lacks a real candidate UID/mount/process ENOSPC witness")
    deadline = time.monotonic() + 180
    while True:
        current = recovery_status(probe, fleet, token)
        if current.get("summary") != "running": break
        require(time.monotonic() < deadline, "original checkpoint write failure exceeded its bounded observation window")
        time.sleep(.2)
    # Pause every process before exporting the failed durable scene or freeing
    # space. Partial pause/fill ACKs remain recoverable by the Fleet registry.
    for node in fleet.nodes:
        identity = fleet.record("write-failure-before-freeze", {"node": node, **fleet.remote(node, "process")})
        fleet.frozen_processes[node] = {name: identity[name] for name in identity if name != "node"}
        fleet.record("write-failure-freeze", {"node": node, **fleet.remote(node, "process-signal",
            signal="SIGSTOP", expected=fleet.frozen_processes[node])})
        after = fleet.remote(node, "process")
        require(after.get("state") == "T" and all(after.get(name) == identity.get(name) for name in PROCESS_FIELDS),
                "journal failure freeze missed its original candidate")
    proof = retained_write_failure(checkpoints.records(fleet, token, "original-write-failed-checkpoint"),
        checkpoints.records(fleet, token, "original-write-failed-admission", family="root"), frozen, current, window.capture())
    scene = fleet.snapshot("original-journal-write-failure-physical")
    indexed = files(scene)
    remaining = [target for target in targets if (target["node"], target["disk"], target["relativePath"]) not in indexed]
    require(remaining, "NOT_OBSERVED: failed journal task left no actual repair for its separate rescan")
    partial = copy.deepcopy(original)
    missing = {(target["node"], target["disk"], target["relativePath"]) for target in remaining}
    for node, value in partial.items():
        value["files"] = [row for row in value["files"] if (node, row["disk"], row["path"]) not in missing]
    validate_repaired_physical(partial, scene, targets[0], repair_targets=targets)
    for node in fleet.nodes:
        released = fleet.record("release-only-owned-journal-filler", {"node": node,
            **fleet.remote(node, "bounded-volumes-release", expected=fleet.frozen_processes[node])})
        require(released.get("sameOriginalProcess") is True and released.get("hostSharedMountRemounted") is False
                and len(released.get("disks", [])) == 4 and all(row.get("writeErrno") == 0
                    and row.get("freeBlocks", 0) > 0 for row in released["disks"]),
                "journal release did not restore its candidate's exact owned resource")
    for node, identity in tuple(fleet.frozen_processes.items()):
        fleet.record("write-failure-resume-after-release", {"node": node,
            **fleet.remote(node, "process-signal", signal="SIGCONT", expected=identity)})
        del fleet.frozen_processes[node]
    active.remove((checkpoints.PATH, token))
    idle_fleet(fleet, binding)
    if proof["originalAdmissionRetained"]:
        recovered = recover_write_before_rescan(fleet, probe, binding, expected, baseline, original, targets,
            frozen, current, proof, scene, remaining, window.events, active)
        if recovered is not None: return recovered
    admissions = []
    rescanned = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
        option_overrides={"recursive": True}, admissions=admissions)
    require(len(admissions) == 1 and admissions[0]["clientToken"] != token,
            "journal release rescan reused the failed original responsibility")
    rescan = rescan_outcome(rescanned, bucket, expected, {version_identity(target) for target in remaining}, frozen["bucketIncarnationId"])
    repeated = recovery_status(probe, fleet, token)
    require(all(canonical(repeated.get(name)) == canonical(current.get(name)) for name in ("summary", "settings", "outcome", "detail")),
            "safe rescan rewrote the original journal write failure")
    fleet.stop()
    repaired = fleet.snapshot("journal-rescan-complete-physical-before-any-GET", quiescent=True)
    validate_repaired_physical(original, repaired, targets[0], repair_targets=targets)
    for target in targets: verify_proof(fleet, target, "journal-rescan-full-shard-integrity")
    retained_write_failure(checkpoints.records(fleet, token, "retained-write-failed-checkpoint", quiescent=True),
        checkpoints.records(fleet, token, "retained-write-failed-admission", quiescent=True, family="root"), frozen, repeated, window.events)
    fleet.start()
    deadline = time.monotonic() + 180
    while True:
        final = recovery_status(probe, fleet, token)
        if final.get("summary") != "running": break
        require(time.monotonic() < deadline, "released original responsibility has no bounded restart outcome")
        time.sleep(.2)
    if final.get("summary") == "finished":
        replay = rescan_outcome(final, bucket, expected, set(), frozen["bucketIncarnationId"])
        replay.pop("originalFailedTaskReclassified")
        replay.update(realSameTaskReplayAfterSeparateRescan=True, failedFirstAttemptRetained=True,
                      separateRescanIsOriginalTaskCompletion=False)
    else:
        require(all(canonical(final.get(name)) == canonical(current.get(name)) for name in ("summary", "settings", "outcome", "detail")),
                "restart neither retained the original failure nor completed a verified same-task replay")
        replay = {"originalFailureStillRetained": True, "sameTaskCompletionClaimed": False}
    for endpoint in binding["endpoints"]:
        peer = Probe(binding, endpoint)
        same = status(peer, fleet, checkpoints.PATH, token, "same-original-write-failure-peer")
        require(all(canonical(same.get(name)) == canonical(final.get(name)) for name in ("summary", "settings", "outcome", "detail")),
                "peer lost the failed original journal responsibility")
        inventory = fleet.record("complete-peer-S3", {"endpoint": endpoint, **peer.inventory(bucket,
            unversioned_current_tags=expected[0]["versionId"] == "null")})
        verify_seed(expected, inventory); compare_inventories(baseline, inventory)
    fleet.stop()
    require(physical_content(repaired) == physical_content(fleet.snapshot("journal-after-business-reads", quiescent=True)),
            "business reads repaired objects missing from the journal rescan proof")
    final_roots = checkpoints.records(fleet, token, "retained-write-admission-after-reads", quiescent=True, family="root")
    if final.get("summary") == "finished": replay["durableOriginalCompletion"] = completed_original(final_roots, frozen, final)
    else:
        retained_write_failure(checkpoints.records(fleet, token, "retained-write-failure-after-reads", quiescent=True),
            final_roots, frozen, final, window.events)
    fleet.record("complete-original-journal-write-reconciliation", {"firstFailedAttempt": current,
        "retainedFailureProof": proof, "separateRescan": rescan, "originalAfterRestart": final, "restartProof": replay})
    fleet.faults.clear()
    return {"verdict": "PASS", "originalWriteFailure": proof, "separateRescan": rescan, "originalAfterRestart": replay,
            "completePhysicalAndS3ManifestsVerified": True, "canonicalCasePassClaimed": False}


def run_checkpoint_fault(args, topology, nodes, fixture, branch, candidate):
    require(branch in IMPLEMENTED or branch == "journal-write-failure", "unimplemented journal branch cannot mutate the fleet")
    require(candidate.get("source_sha") in TERMINAL_OPTIONS_REFERENCE["candidateCommits"],
            "unsupported candidate terminal-options contract")
    directory = args.evidence / fixture / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    bounded = branch == "journal-write-failure"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, resource_profile="cursor",
                  runtime_profile="journal-bounded" if bounded else "root-restart")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    fleet.snapshot_buckets = [bucket]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    count, object_size = checkpoint_fixture(branch, fixture)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 512, "maxInventoryBytes": 2 * 1024**3}
    fleet.record("binding", binding)
    fleet.record("candidate-bound-inspector", verify_inspector(args.tool_directory, candidate))
    fleet.record("candidate-terminal-options-contract", {
        **TERMINAL_OPTIONS_REFERENCE, "candidateCommit": candidate["source_sha"]})
    probe, active, pending = Probe(binding), [], None
    result = {"topology": topology, "fixture": fixture, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    def allocate():
        for node in nodes:
            fleet.record("fixed-private-journal-volume-allocation", {"node": node,
                **fleet.remote(node, "bounded-volumes-create", branch="bounded-volume-enospc")})
    try:
        if bounded: fleet.deploy(args.binary_sha, bucket, access, secret, before_start=allocate)
        else: fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        pending = {"operation": "seed-original-checkpoint-fixture"}
        expected = seed(Recorder(probe, directory / "fixture"), bucket, fixture, count, [object_size])
        baseline = fleet.record("ACK-bound-complete-S3", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        fleet.record("complete-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        pending = None
        idle_fleet(fleet, binding)
        fleet.stop()
        original = fleet.snapshot("healthy-physical-baseline", quiescent=True)
        targets = [map_target(original, bucket, f"objects/{index:04d}.bin", "data", 1,
            next(entry["versionId"] for entry in expected if entry["key"] == f"objects/{index:04d}.bin"
                 and not entry["deleteMarker"]), latest_required=False) for index in range(count - 4, count)]
        prior = original
        for index, target in enumerate(targets):
            verify_proof(fleet, target, "baseline-full-cohort-bitrot")
            params = {"disk": target["disk"], "faultId": f"F07-shard-{index}"}
            fleet.faults.append((target["node"], params))
            receipt = fleet.record("missing-original-shard", fleet.remote(target["node"], "apply-fault", **params,
                relativePath=target["relativePath"], expectedSha256=target["sha256"], kind="missing"))
            current = fleet.snapshot("actual-missing-shard", quiescent=True)
            validate_fault(prior, current, target, receipt)
            prior = current
        fleet.start()
        require(physical_content(prior) == physical_content(fleet.snapshot("before-original-admission")),
                "ATTRIBUTION_NOT_OBSERVED: background activity changed the original shard faults")
        pending = {"operation": "admit-original-checkpoint-task"}
        token = checkpoints.admit(probe, fleet, active)
        pending = None
        earlier = checkpoints.capture_earlier(fleet, token, bucket, "F07-earlier-generation", advance=True) if branch == "stale-generation" else None
        if earlier is not None:
            require(earlier["value"]["admin"]["outcome"]["counters"]["healed"] == 0,
                    "NOT_OBSERVED: earlier generation missed the pre-repair capture window")
        frozen, identity = checkpoints.freeze(fleet, token, bucket, earlier=earlier)
        frozen = checkpoints.stop_for_fault(fleet, token, bucket, frozen, identity)
        if bounded:
            result.update(journal_write_failure(fleet, probe, binding, expected, baseline, original, targets, frozen, active))
            return result
        params = {"family": "checkpoint", "taskId": token, "disk": frozen["disk"],
            "relativePath": frozen["checkpoint"]["relativePath"], "faultId": "F07-" + branch,
            "kind": IMPLEMENTED[branch], "expectedBefore": journal.entry(checkpoints.bytes_of(frozen["checkpoint"]))}
        if earlier is not None: params["captureId"] = earlier["captureId"]
        fleet.journal_faults.append((frozen["node"], params))
        receipt = fleet.record("stopped-original-checkpoint-injection", fleet.remote(frozen["node"], "journal-apply", **params))
        if earlier is not None:
            fleet.record("original-earlier-generation-injection-proof", stale_receipt(earlier, frozen, receipt))
            injected = checkpoints.owned_checkpoint(checkpoints.records(fleet, token, "independent-injected-original-generation", quiescent=True), token, bucket)
            require(checkpoints.bytes_of(injected["checkpoint"]) == checkpoints.bytes_of(earlier["checkpoint"])
                    and injected["resume"] == frozen["resume"], "stale rollback changed its original companion or actual bytes")
            fault_scene = fleet.snapshot("physical-at-original-generation-replay-boundary", quiescent=True)
        fleet.start()
        for node in fleet.nodes:
            identity = fleet.record("post-injection-candidate-process", {"node": node, **fleet.remote(node, "process")})
            require(identity.get("settings", {}).get("RUSTFS_HEAL_TASK_TIMEOUT_SECS") == str(TIMEOUT_SECONDS),
                    "terminal normalization has no original candidate runtime timeout witness")
        deadline = time.monotonic() + 180
        while True:
            failed = recovery_status(probe, fleet, token)
            if failed.get("summary") != "running": break
            require(time.monotonic() < deadline, "original corrupt checkpoint task did not produce a bounded diagnostic")
            time.sleep(.2)
        if earlier is not None and failed.get("summary") == "finished":
            result.update(successful_stale_replay(fleet, probe, binding, expected, baseline, original, targets,
                frozen, earlier, receipt, fault_scene, failed, active))
            return result
        failed_task(failed)
        active.remove((checkpoints.PATH, token))
        idle_fleet(fleet, binding)
        fleet.stop()
        corrupt_scene = checkpoints.records(fleet, token, "original-corrupt-checkpoint", quiescent=True)
        failure_roots = checkpoints.records(fleet, token, "original-durable-failure", quiescent=True, family="root")
        result["originalFailure"] = retained_corruption(corrupt_scene, failure_roots, frozen, receipt, failed, earlier=earlier)
        before_rescan = fleet.snapshot("failure-scene-before-any-rescan", quiescent=True)
        indexed = files(before_rescan)
        remaining = [target for target in targets if (target["node"], target["disk"], target["relativePath"]) not in indexed]
        require(remaining, "NOT_OBSERVED: no original object responsibility remains for the separate safe rescan")
        partial = copy.deepcopy(original)
        missing = {(target["node"], target["disk"], target["relativePath"]) for target in remaining}
        for node, value in partial.items():
            value["files"] = [row for row in value["files"] if (node, row["disk"], row["path"]) not in missing]
        fleet.record("partial-original-repair-and-control-proof", validate_repaired_physical(
            partial, before_rescan, targets[0], repair_targets=targets))
        fleet.start()
        admissions = []
        rescanned = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
            option_overrides={"recursive": True}, admissions=admissions)
        require(len(admissions) == 1 and admissions[0]["clientToken"] != token, "safe rescan reused or relabelled the failed original token")
        result["separateRescan"] = rescan_outcome(rescanned, bucket, expected,
            {version_identity(target) for target in remaining}, frozen["bucketIncarnationId"])
        # Complete independent manifests cover the bounded public diagnostics.
        # The small-result peer helper intentionally rejects such truncation.
        for endpoint in tuple(binding["endpoints"])[1:]:
            deadline = time.monotonic() + 30
            while True:
                peer = status(Probe(binding, endpoint), fleet, admissions[0]["path"], admissions[0]["clientToken"], "same-rescan-peer")
                if all(canonical(peer.get(name)) == canonical(rescanned.get(name))
                       for name in ("summary", "settings", "outcome", "truncated")): break
                require(time.monotonic() < deadline, "same original rescan token did not converge on an actual peer")
                time.sleep(.2)
        fleet.stop()
        repaired = fleet.snapshot("complete-physical-before-any-GET", quiescent=True)
        fleet.record("complete-physical-proof", validate_repaired_physical(original, repaired, targets[0], repair_targets=targets))
        for target in targets: verify_proof(fleet, target, "rescan-full-cohort-bitrot")
        # New-task repair must not erase or advance the retained corrupt record.
        fleet.start()
        repeated = status(probe, fleet, checkpoints.PATH, token, "original-failure-after-separate-rescan")
        require(canonical(repeated.get("outcome")) == canonical(failed.get("outcome")), "safe rescan rewrote the original failure")
        for endpoint in binding["endpoints"]:
            inventory = fleet.record("complete-peer-S3", {"endpoint": endpoint, **Probe(binding, endpoint).inventory(
                bucket, unversioned_current_tags=fixture == "DB")})
            fleet.record("complete-peer-ACK-and-content-proof", {**verify_seed(expected, inventory), **compare_inventories(baseline, inventory)})
        fleet.stop()
        require(physical_content(repaired) == physical_content(fleet.snapshot("physical-after-GET", quiescent=True)),
                "business reads repaired data that the independent rescan had not proved")
        retained_checkpoint = checkpoints.records(fleet, token, "retained-corrupt-checkpoint-after-rescan", quiescent=True)
        retained_roots = checkpoints.records(fleet, token, "retained-original-failure-after-rescan", quiescent=True, family="root")
        retained_corruption(retained_checkpoint, retained_roots, frozen, receipt, repeated, earlier=earlier)
        require(canonical(exact_records(retained_checkpoint, token, "checkpoint")) == canonical(exact_records(corrupt_scene, token, "checkpoint"))
                and canonical(exact_records(retained_roots, token, "root")) == canonical(exact_records(failure_roots, token, "root")),
                "separate rescan or business reads changed the original corrupt record or durable failure bytes")
        fleet.faults.clear()  # Complete physical/S3 proof precedes discarding shard rescue registrations.
        result.update(verdict="PASS", originalFailureReclassified=False, completePhysicalAndS3ManifestsVerified=True)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("original-failure-scene", quiescent=True)
        except Exception as error: result["failureEvidenceError"] = str(error)
    finally:
        clean = fleet.cleanup(preserve_namespaces=pending is not None)
        result["cleanup"] = "PASS" if clean and pending is None else "FAIL"
        if pending is not None: result["unresolvedMutation"] = pending
        fleet.record("checkpoint-branch-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"F07 interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"): parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    parser.add_argument("--case-id", choices=(CASE_ID,), required=True)
    args = parser.parse_args()
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "F07 recipe branches changed")
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] in {checkpoints.REFERENCE["candidateCommit"], "1aa9be87d18cf941e5020f19e8622c26ba4b10ac"},
            "F07 checkpoint failure contract requires an exact reviewed candidate with identical checkpoint sources")
    native = verify_manager(os.environ.get("VALIDATION_HEAL_MANAGER_DIRECTORY", ""), candidate, CASE_ID)
    save(args.evidence / "independent-native-persistence-prerequisites.json", native)
    require(re.fullmatch(r"[0-9a-f]{40}", args.testing_commit) and re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)
            and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "F07 requires immutable workflow and binary identities")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) == len(set(nodes)) == 4 and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "F07 requires the four distinct current auto-testing Heal nodes")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes)):
            for fixture in ("DB", "DV"):
                for branch in BRANCHES:
                    if branch in IMPLEMENTED or branch == "journal-write-failure":
                        result = run_checkpoint_fault(args, topology, selected, fixture, branch, candidate)
                    elif branch == "mrf-checksum":
                        result = run_mrf_checksum(args, topology, selected, fixture, candidate)
                    elif branch == "native-persist-fence":
                        result = {"requestedTopology": topology, "requestedFixture": fixture, "topology": None,
                            "fixture": None, "branch": branch, "verdict": "PREREQUISITE_PASS", "cleanup": "PASS",
                            "nativeProof": native, "fleetBranchExecuted": False, "canonicalCasePassClaimed": False,
                            "evidenceScope": "exact-candidate checkpoint/MRF publication native prerequisite",
                            "individualFsyncHooksClaimed": False, "physicalPowerLossClaimed": False,
                            "requiredNativeCoverageComplete": False,
                            "remainingNativeCoverage": "review every required checkpoint/MRF fsync and commit cut against the source assertions"}
                    else:
                        result = {"topology": topology, "fixture": fixture, "branch": branch, "verdict": "BLOCKED", "cleanup": "PASS",
                            "reason": BLOCKERS[branch], "fleetBranchExecuted": False, "canonicalCasePassClaimed": False}
                    results.append(result)
                    if result["cleanup"] != "PASS": return 1
    finally:
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": False,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL",
            "diagnosticBranchesPassed": len(results) == 24 and all(row["cleanup"] == "PASS" and
                row["verdict"] == ("BLOCKED" if row["branch"] in BLOCKERS else
                    "PREREQUISITE_PASS" if row["branch"] == "native-persist-fence" else "PASS") for row in results),
            "canonicalCasePassClaimed": False, "reason": "full native cut coverage and independent live audit remain blocked"})
    return 1


if __name__ == "__main__": sys.exit(main())
