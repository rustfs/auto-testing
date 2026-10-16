#!/usr/bin/env python3
"""J02: reconcile original mixed-task receipts with ACKs and physical faults."""

import argparse
import errno
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys

try:
    from .heal_probe import Probe, canonical, compare_inventories, require, save
    from .heal_workload import Recorder, payload, seed, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .heal_fault import bounded_fault_id
    from .inspector import verify as verify_inspector
    from .manager_fixture import verify as verify_manager
    from .matrix import select
    from .native_regressions import CANDIDATE
    from .outcome_grace import run as original_grace_probe
    from .shards import files, map_target, run_task, validate_fault, validate_repaired_physical, validate_repaired_status
    from .stop import idle_fleet
    from .versions import (metadata_target, public_version_id, validate_unrecoverable_get,
        validate_version_receipts, verify_proof, verify_quorum_contract, version_identity)
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, require, save
    from heal_workload import Recorder, payload, seed, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from heal_fault import bounded_fault_id
    from inspector import verify as verify_inspector
    from manager_fixture import verify as verify_manager
    from matrix import select
    from native_regressions import CANDIDATE
    from outcome_grace import run as original_grace_probe
    from shards import files, map_target, run_task, validate_fault, validate_repaired_physical, validate_repaired_status
    from stop import idle_fleet
    from versions import (metadata_target, public_version_id, validate_unrecoverable_get,
        validate_version_receipts, verify_proof, verify_quorum_contract, version_identity)


CASE_ID = "heal.outcome-counters-and-error-truth"
REVIEWED_CANDIDATES = frozenset({CANDIDATE, "1c7fc818dd858247c45b39542abd8291b1e6e31b"})
BRANCHES = ("mixed-healthy-repaired-failed", "dry-run", "deferred", "readonly-terminal-failure", "unknown-negative")
SIZE = 5 * 1024**2 + 123
# The reviewed immutable candidate makes one attempt and three bucket-object retries.
FAILED_ATTEMPTS = 4
SOURCE_ASSERTION_GAPS = ()


def reconcile(status, bucket, expected, options, incarnation, *, attempt_failures, legacy_unknown=()):
    """The manifest describes responsibility; legacy objectsHealed is not evidence."""
    wanted = {version_identity(entry): entry for entry in expected}
    require(wanted and len(wanted) == len(expected), "J02 manifest is empty or duplicates ACK identities")
    require(type(attempt_failures) is int and attempt_failures >= 0, "J02 invalid attempt-failure oracle")
    legacy_unknown = set(legacy_unknown)
    require(legacy_unknown <= set(wanted) and legacy_unknown == {pair for pair, entry in wanted.items() if entry["state"] == "unknown"},
            "J02 unknown responsibility lacks its independently frozen legacy manifest")
    require(not legacy_unknown or any(entry["state"] in {"verified_healthy", "metadata_healthy"} for entry in expected),
            "J02 legacy negative requires a separate protected control")
    failed = sum(entry["state"] == "failed" for entry in expected)
    require(status.get("summary") == ("stopped" if failed else "finished")
            and status.get("settings") == options and status.get("truncated", False) is False,
            "J02 task has a wrong terminal, options or incomplete public result")
    outcome = status.get("outcome") or {}
    require(outcome.get("execution", {}).get("state") == ("completed_with_errors" if failed else "completed")
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False,
            "J02 task hid failed execution or lacks complete canonical coverage")
    receipts = outcome.get("objects", [])
    require(isinstance(receipts, list) and len(receipts) == len(wanted), "J02 omitted or duplicated object receipts")
    observed = {}
    counters = dict(processed=len(wanted), healed=0, unchanged=0, skipped=0,
                    failed=0, unknown=0, attemptFailures=attempt_failures, overflowed=False)
    for receipt in receipts:
        identity = receipt.get("identity", {})
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(pair in wanted and pair not in observed and identity.get("kind") == "object" and identity.get("bucket") == bucket
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0,
                "J02 receipt has a foreign/duplicate object, version or erasure set")
        expected_entry = wanted[pair]
        state = expected_entry["state"]
        require(state in {"repaired", "verified_healthy", "metadata_healthy", "failed", "dry_run_observed", "deferred"}
                or state == "unknown" and pair in legacy_unknown,
                "J02 unknown, skipped or absent work cannot become a successful fixture result")
        disposition = receipt.get("disposition", {})
        require(disposition.get("state") == state, "J02 disposition differs from the frozen physical fault plan")
        if state == "deferred":
            # Existence-check deferral cannot establish a version or incarnation.
            # This is one key-level refusal, never a version repair receipt.
            require(expected_entry.get("keyLevelRefusal") is True and pair[1] == "null"
                    and identity.get("versionId") is None and identity.get("bucketIncarnationId") is None
                    and disposition.get("details") == {"reason": "transient_existence_check", "retryNotBefore": None},
                    "J02 deferred dependency claimed a version/incarnation or changed its typed reason")
        else:
            require(isinstance(incarnation, str) and incarnation
                    and identity.get("bucketIncarnationId") == incarnation,
                    "J02 receipt escaped the original healthy-task bucket incarnation")
        if state == "unknown":
            counters["unknown"] += 1
            require(disposition == {"state": "unknown"} and receipt.get("detail") is None,
                    "J02 legacy unknown changed its frozen proofless disposition")
        if state == "failed":
            require(disposition.get("details") == "retry_exhausted", "J02 below-R failure lost bounded retry exhaustion")
        category = {"repaired": "healed", "verified_healthy": "unchanged", "metadata_healthy": "unchanged",
                    "failed": "failed", "dry_run_observed": "skipped", "deferred": "skipped", "unknown": "skipped"}[state]
        counters[category] += 1
        observed[pair] = state
    actual_counters = outcome.get("counters", {})
    require(all(type(actual_counters.get(name)) is type(value) and actual_counters[name] == value
                for name, value in counters.items()), "J02 canonical counters hide retries or differ from complete dispositions")
    return {"originalIncarnation": incarnation, "completeManifestReconciled": True,
            "canonicalCounters": counters, "receipts": len(observed), "legacyObjectsHealedUsedAsRepairEvidence": False,
            "deferredIsRepair": False, "failedIsRepair": False, "unknownIsRepair": False}


def append_legacy_fixture(probe, fleet, directory, binding, expected, fixture):
    """Keep an ACK-bound protected control while writing new real legacy objects."""
    for node in fleet.nodes:
        proof = fleet.record("one-way-legacy-write-phase", {"node": node, **fleet.remote(node, "outcome-legacy-phase")})
        require(proof.get("runId") == binding["runId"] and proof.get("binarySha256") == binding["candidateBinarySha256"]
                and proof.get("phase") == "legacy" and proof.get("oneWay") is True
                and proof.get("candidateUnchanged") is True and proof.get("otherRuntimeSettingsUnchanged") is True
                and proof.get("credentialsDisclosed") is False, "J02 phase acknowledgement differs from its original candidate")
    fleet.start()
    running = fleet.snapshot("actual-legacy-writing-processes")
    require(all(scene.get("backgroundConfig", {}).get("RUSTFS_SHARD_INTEGRITY_WRITE") == "false"
                and scene["backgroundConfig"].get("RUSTFS_HEAL_MRF_ENABLE") == "false"
                and scene["backgroundConfig"].get("RUSTFS_SCANNER_ENABLED") == "false" for scene in running.values()),
            "J02 legacy bytes require actual write-disabled, isolated candidate processes")
    recorder = Recorder(probe, directory / "legacy-fixture")
    appended = []
    for index in range(2):
        key = f"legacy/{index:04d}.bin"
        versions = 3 if fixture == "DV" else 1
        for version in range(versions):
            body = payload(f"{binding['bucketPrefix']}/{key}/{version}", SIZE)
            response = recorder.call("put_object", {"Bucket": binding["bucketPrefix"], "Key": key,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": fixture, "nonce": f"{index}-{version}"},
                "Tagging": "suite=heal&fixture=" + fixture}, body)
            require(fixture != "DV" or response.get("VersionId") not in {None, "null"}, "J02 legacy DV PUT lacks an ACK version")
            appended.append({"key": key, "versionId": response.get("VersionId", "null"), "isLatest": version == versions - 1,
                "deleteMarker": False, "payload": {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body)}})
        if fixture == "DV" and index == 0:
            response = recorder.call("delete_object", {"Bucket": binding["bucketPrefix"], "Key": key})
            require(response.get("DeleteMarker") is True and response.get("VersionId"), "J02 legacy marker ACK is incomplete")
            appended[-1]["isLatest"] = False
            appended.append({"key": key, "versionId": response["VersionId"], "isLatest": True, "deleteMarker": True})
    return expected + appended


def legacy_manifest(original, bucket, expected):
    """Require a complete physical cohort for every ACK version before admission."""
    manifest, unknown, evidence = [], [], []
    for entry in expected:
        legacy = entry["key"].startswith("legacy/")
        state = "metadata_healthy" if entry["deleteMarker"] else "unknown" if legacy else "verified_healthy"
        manifest.append({**entry, "state": state})
        if entry["deleteMarker"]:
            continue
        target = map_target(original, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False)
        parts = target["logical"]["parts"]
        member_parts = [version["parts"] for member in target["cohort"] for version in member["metadata"]["versions"]
                        if public_version_id(version.get("versionId")) == public_version_id(entry["versionId"])]
        require(len(member_parts) == len(target["cohort"]) and all(parts and all(type(part.get("integrityDescriptorPresent")) is bool
                and part["integrityDescriptorPresent"] is (not legacy) for part in parts) for parts in member_parts),
                "J02 metadata lacks exact protected/legacy part descriptor evidence")
        require(target["logical"]["size"] == entry["payload"]["size"], "J02 metadata size differs from the ACK")
        evidence.append({"key": entry["key"], "versionId": entry["versionId"], "legacy": legacy,
                         "completeCohort": target["cohort"], "parts": parts})
        if legacy:
            unknown.append(version_identity(entry))
    require(unknown and any(not item["legacy"] for item in evidence), "J02 legacy scene lacks an independent protected control")
    return {"manifest": manifest, "unknownIdentities": unknown, "physicalLegacyEvidence": evidence,
            "syntheticMetadataWritten": False, "legacyPayloadIntegrityCertified": False}


def fault_plan(original, bucket, expected, branch):
    require(branch in BRANCHES[:4], "J02 unknown diagnostic cannot mutate fleet files")
    repair_entry = next(entry for entry in expected if entry["key"] == "objects/0001.bin" and not entry["deleteMarker"]
                        and (branch != "readonly-terminal-failure" or entry["isLatest"]))
    failed_entry = next(entry for entry in expected if entry["key"] == "objects/0002.bin" and not entry["deleteMarker"])
    repair = map_target(original, bucket, repair_entry["key"], "data", 1, repair_entry["versionId"], latest_required=False)
    failed = map_target(original, bucket, failed_entry["key"], "data", 1, failed_entry["versionId"], latest_required=False)
    if branch == "readonly-terminal-failure":
        require(repair["logical"]["isLatest"] is True, "J02 readonly target must be the original current ACK version")
        return {"repair": repair, "faults": [repair], "originalReadonlySourceProcedure": True}
    ordered = sorted(failed["cohort"], key=lambda member: member["index"])
    read_quorum = failed["logical"]["erasure"]["data_blocks"]
    faults = [{**failed, **member} for member in ordered[read_quorum - 1:]]
    if branch == "deferred":
        faults = [metadata_target(original, target) for target in faults]
        manifest = [{"key": failed["key"], "versionId": "null", "state": "deferred", "keyLevelRefusal": True}]
    else:
        faults.insert(0, repair)
        manifest = [{**entry, "state": "dry_run_observed" if branch == "dry-run" else
                     "repaired" if version_identity(entry) == version_identity(repair) else
                     "failed" if version_identity(entry) == version_identity(failed) else
                     "metadata_healthy" if entry["deleteMarker"] else "verified_healthy"} for entry in expected]
    require(len({(item["node"], item["disk"], item["relativePath"]) for item in faults}) == len(faults)
            and len(ordered) - (len(faults) - (branch != "deferred")) == read_quorum - 1,
            "J02 fault plan duplicates files or does not leave precisely R-1 authoritative members")
    return {"repair": repair, "failed": failed, "faults": faults, "manifest": manifest,
            "readQuorum": read_quorum, "retainedMembers": ordered[:read_quorum - 1]}


def rescue(fleet):
    for node, params in tuple(fleet.faults):
        fleet.record("restore-fixture-without-relabeling-original-task", {
            "node": node, **fleet.remote(node, "recover-fault", **params)})
        fleet.faults.remove((node, params))


def retained_failure_scene(original, faulted, after, plan, branch):
    if branch in {"dry-run", "deferred"}:
        require(physical_content(faulted) == physical_content(after), "J02 dry-run/deferred task mutated a physical file")
        return {"physicalUnchanged": True, "productRepairClaimed": False}
    # Restore only the intentionally unrecoverable files in the oracle view.
    # The fleet remains faulted until after the original failed task is archived.
    combined = json.loads(json.dumps(after))
    originals, observed = files(original), files(after)
    for target in plan["faults"][1:]:
        identity = target["node"], target["disk"], target["relativePath"]
        require(identity not in observed, "J02 below-R task invented an unrecoverable shard")
        combined[target["node"]]["files"].append(originals[identity])
    proof = validate_repaired_physical(original, combined, plan["repair"])
    return {**proof, "failedMembersStillAbsent": len(plan["faults"]) - 1,
            "oracleViewOnly": True, "fixtureRescueIsProductRepair": False}


def readonly_failure(status, bucket, key, options):
    """Freeze the source operation card's unbound permanent key-level failure."""
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "stopped" and status.get("settings") == options
            and status.get("truncated", False) is False and outcome.get("execution", {}).get("state") == "completed_with_errors"
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False,
            "J02 readonly task lost its complete permanent terminal failure")
    objects = outcome.get("objects", [])
    require(len(objects) == 1, "J02 readonly task lost its single key-level responsibility")
    identity = objects[0].get("identity", {})
    require(set(identity) == {"kind", "bucket", "object", "versionId", "bucketIncarnationId", "poolIndex", "setIndex"}
            and identity.get("kind") == "object" and identity.get("bucket") == bucket and identity.get("object") == key
            and identity.get("versionId") is None and identity.get("bucketIncarnationId") is None
            and all(type(identity.get(name)) is int and identity[name] == 0 for name in ("poolIndex", "setIndex"))
            and objects[0].get("disposition") == {"state": "failed", "details": "permanent"},
            "J02 readonly failure invented a version/incarnation or changed its original failure class")
    expected = {"processed": 1, "healed": 0, "unchanged": 0, "skipped": 0, "failed": 1,
                "unknown": 0, "attemptFailures": 1, "overflowed": False}
    counters = outcome.get("counters", {})
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in expected.items()),
            "J02 readonly failure hid attempts or reported a repair")
    return {"canonicalCounters": expected, "originalKeyLevelFailurePreserved": True, "repairClaimed": False}


def unchanged_token(probe, fleet, admission, status, label):
    response = fleet.record(label, probe.admin(admission["path"], [("clientToken", admission["clientToken"])], b""))
    require(response.get("httpStatus") == 200 and response.get("response", {}).get("summary") == status["summary"]
            and response["response"].get("settings") == status["settings"]
            and response["response"].get("truncated", False) is False
            and canonical(response["response"].get("outcome")) == canonical(status["outcome"]),
            "J02 later work replaced the original readonly failure responsibility")


def readonly_compensation(probe, fleet, binding, original, faulted, plan, expected, baseline, incarnation, active):
    target = plan["repair"]
    gate = fleet.record("readonly-actual-process-probe", {"node": target["node"],
        **fleet.remote(target["node"], "drive-gate-probe")})
    require(type(gate.get("disk")) is int and gate["disk"] == target["disk"] and type(gate.get("errno")) is int and gate["errno"] == errno.EROFS
            and gate.get("name") == "EROFS" and gate.get("sameServiceIdentity") is True
            and gate.get("privateMountNamespace") is True and gate.get("hostMountRemounted") is False,
            "J02 readonly refusal was not observed in the original candidate namespace")
    options = {**BASE_OPTIONS, "recursive": False}
    scope = {"bucket": binding["bucketPrefix"], "key": target["key"]}
    controls = [entry for entry in expected if entry["key"] != target["key"] and entry["isLatest"] and not entry["deleteMarker"]]
    require(controls, "J02 readonly task lacks an independently healthy current control")
    control = controls[0]
    control_status = run_task(probe, fleet, {"bucket": scope["bucket"], "key": control["key"]}, 2, active,
        option_overrides=options, status_endpoints=tuple(binding["endpoints"])[1:])
    fleet.record("readonly-healthy-control-under-gate", control_status)
    control_proof = validate_version_receipts(control_status, scope["bucket"], [control], 2)
    require(control_status.get("settings") == options and control_proof["bucketIncarnationId"] == incarnation
            and control_status["outcome"]["objects"][0]["identity"].get("kind") == "object",
            "J02 readonly control lost the original incarnation or actual Deep verification")
    require(physical_content(fleet.snapshot("readonly-after-gated-control-before-admission")) == physical_content(faulted),
            "J02 healthy control changed a fault or its own original bytes")
    admissions = []
    status = run_task(probe, fleet, scope, 2, active, option_overrides=options,
                      status_endpoints=tuple(binding["endpoints"])[1:], admissions=admissions)
    fleet.record("readonly-original-terminal-before-release", status)
    negative = readonly_failure(status, scope["bucket"], scope["key"], options)
    require(len(admissions) == 1, "J02 readonly task lacks one independent admission")
    idle_fleet(fleet, binding)
    fleet.stop()
    require(physical_content(fleet.snapshot("readonly-original-failure-physical", quiescent=True)) == physical_content(faulted),
            "J02 readonly task changed its still-unrepaired scene")
    cleared = fleet.record("readonly-stopped-release", {"node": target["node"],
        **fleet.remote(target["node"], "drive-gate", mode="clear")})
    require(cleared.get("state") == "CLEAR", "J02 readonly gate was not released")
    fleet.drive_gates.remove(target["node"])
    fleet.start()
    require(physical_content(fleet.snapshot("readonly-released-before-new-token")) == physical_content(faulted),
            "J02 release repaired the object before compensating admission")
    unchanged_token(probe, fleet, admissions[0], status, "readonly-original-token-after-release")
    next_admissions = []
    repaired = run_task(probe, fleet, scope, 2, active, option_overrides=options,
                       status_endpoints=tuple(binding["endpoints"])[1:], admissions=next_admissions)
    require(len(next_admissions) == 1 and next_admissions[0]["clientToken"] != admissions[0]["clientToken"],
            "J02 compensation reused the original failed token")
    fleet.record("readonly-new-token-original-result", repaired)
    validate_repaired_status(repaired, target)
    require(repaired.get("settings") == options and repaired["outcome"]["objects"][0]["identity"].get("kind") == "object"
            and repaired["outcome"]["objects"][0]["identity"].get("bucketIncarnationId") == incarnation,
            "J02 compensation changed options or the original protected-control incarnation")
    idle_fleet(fleet, binding)
    fleet.stop()
    after = fleet.snapshot("readonly-new-token-physical-before-get", quiescent=True)
    physical = validate_repaired_physical(original, after, target)
    verify_proof(fleet, target, "readonly-new-token-full-shard-bitrot-before-get")
    rescue(fleet)
    require(physical_content(fleet.snapshot("readonly-after-fixture-rescue", quiescent=True)) == physical_content(after),
            "J02 fixture rescue changed the product's compensating repair")
    fleet.start()
    for endpoint in binding["endpoints"]:
        inventory = fleet.record("readonly-compensated-all-peer-S3", Probe(binding, endpoint).inventory(
            binding["bucketPrefix"], unversioned_current_tags=all(entry["versionId"] == "null" for entry in expected)))
        verify_seed(expected, inventory)
        compare_inventories(baseline, inventory)
    unchanged_token(probe, fleet, admissions[0], status, "readonly-original-token-after-compensation")
    return {"negative": negative, "physical": physical, "originalToken": admissions[0]["clientToken"],
            "compensatingToken": next_admissions[0]["clientToken"], "sameTokenRecoveryClaimed": False,
            "originalFailureUnchanged": True, "productRepairByNewToken": True}



def opaque_metadata_outcome(status, bucket, key, options, incarnation):
    """An unobserved Unknown must remain unobserved, never successful repair."""
    require(isinstance(incarnation, str) and bool(incarnation)
            and status.get("summary") in {"finished", "stopped"} and status.get("settings") == options
            and status.get("truncated", False) is False, "J02 opaque metadata task did not terminate with its exact options")
    outcome = status.get("outcome") or {}
    counters = outcome.get("counters") or {}
    names = ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures")
    require(all(type(counters.get(name)) is int and counters[name] >= 0 for name in names)
            and counters.get("overflowed") is False and counters["processed"] == 1
            and counters["processed"] == sum(counters[name] for name in ("healed", "unchanged", "skipped", "failed"))
            and counters["unknown"] <= counters["skipped"]
            and counters["healed"] == counters["unchanged"] == 0,
            "J02 opaque metadata counters falsely resolve or omit original responsibility")
    objects = outcome.get("objects")
    require(outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False
            and isinstance(objects, list) and len(objects) == 1,
            "J02 opaque metadata responsibility is incomplete")
    identity = objects[0].get("identity") or {}
    state = (objects[0].get("disposition") or {}).get("state")
    require(identity.get("kind") == "object" and identity.get("bucket") == bucket and identity.get("object") == key
            and identity.get("versionId") is None and identity.get("bucketIncarnationId") in {None, incarnation}
            and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
            and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
            and state in {"unknown", "deferred", "failed"},
            "J02 opaque metadata invented a healthy, absent or resolved identity")
    require(counters["unknown"] == (1 if state == "unknown" else 0)
            and counters["failed"] == (1 if state == "failed" else 0)
            and counters["skipped"] == (0 if state == "failed" else 1)
            and outcome.get("execution", {}).get("state") == ("completed_with_errors" if state == "failed" else "completed")
            and status["summary"] == ("stopped" if state == "failed" else "finished"),
            "J02 opaque metadata counters or execution disagree with the original disposition")
    require(state != "unknown" or isinstance(status.get("detail"), str) and bool(status["detail"].strip()),
            "J02 opaque Unknown lacks the original legacy warning")
    return {"verdict": "PASS" if state == "unknown" else "NOT_OBSERVED", "unknownObserved": state == "unknown",
            "originalCorruptMetadataSourceProcedure": True, "originalDisposition": state,
            "canonicalCounters": counters, "productRepairClaimed": False, "canonicalPassClaimed": False}


def original_opaque_probe(probe, fleet, binding, directory, expected, fixture, incarnation, active):
    """Corrupt every original metadata copy through the existing file-fault path."""
    bucket, key = binding["bucketPrefix"], "opaque/source.bin"
    fleet.start()
    body = payload(bucket + "/" + key, SIZE)
    response = Recorder(probe, directory / "original-opaque-fixture").call("put_object", {
        "Bucket": bucket, "Key": key, "ContentType": "application/octet-stream"}, body)
    version = public_version_id(response.get("VersionId"))
    require(fixture != "DV" or version != "null", "J02 opaque DV fixture lacks an exact ACK version")
    entry = {"key": key, "versionId": version, "isLatest": True, "deleteMarker": False,
             "payload": {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body)}}
    expected = [*expected, entry]
    baseline = fleet.record("original-opaque-ACK-full-S3", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
    fleet.record("original-opaque-complete-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
    options = {**BASE_OPTIONS, "recursive": False}
    anchor = run_task(probe, fleet, {"bucket": bucket, "key": key}, 2, active, option_overrides=options,
                      status_endpoints=tuple(binding["endpoints"])[1:])
    require(validate_version_receipts(anchor, bucket, [entry], 2)["bucketIncarnationId"] == incarnation,
            "J02 opaque anchor changed the original incarnation")
    idle_fleet(fleet, binding); fleet.stop()
    original = fleet.snapshot("original-opaque-complete-physical", quiescent=True)
    target = map_target(original, bucket, key, "data", 1, version, latest_required=False)
    verify_proof(fleet, target, "original-opaque-full-cohort-bitrot")
    targets = [metadata_target(original, {**target, **member}) for member in target["cohort"]]
    require(len(targets) == len(fleet.nodes) * 4
            and len({(row["node"], row["disk"]) for row in targets}) == len(targets),
            "J02 opaque corruption must cover every original physical metadata copy")
    fleet.record("original-opaque-metadata-fault-plan", targets)
    metadata_faults, prior = {}, original
    for index, row in enumerate(targets):
        params = {"disk": row["disk"], "faultId": bounded_fault_id("J02", "original-opaque", index)}
        fleet.faults.append((row["node"], params))
        receipt = fleet.record("original-opaque-byte-zero-bitflip", {"node": row["node"], **fleet.remote(
            row["node"], "apply-fault", **params, relativePath=row["relativePath"], expectedSha256=row["sha256"],
            kind="bitflip", offset=0)})
        require(receipt.get("kind") == "bitflip" and type(receipt.get("offset")) is int and receipt["offset"] == 0
                and isinstance(receipt.get("fault"), dict), "J02 opaque injection changed its original byte-zero fault")
        metadata_faults.setdefault(row["node"], {})[f"{row['disk']}:{row['relativePath']}"] = receipt["fault"]
        damaged = fleet.snapshot("original-opaque-physical-injection", quiescent=True, metadata_faults=metadata_faults)
        validate_fault(prior, damaged, row, receipt); prior = damaged
    damaged_files = files(damaged)
    require(all(damaged_files[(row["node"], row["disk"], row["relativePath"])].get("metadataError", {})
                .get("exactWitnessedFault") is True
                and damaged_files[(row["node"], row["disk"], row["relativePath"])]["metadataError"].get("validMetadataClaimed") is False
                and "metadata" not in damaged_files[(row["node"], row["disk"], row["relativePath"])] for row in targets),
            "J02 original opaque fixture retained a readable metadata copy")
    fleet.start()
    # A separate protected prefix remains observable during the opaque fault.
    control = run_task(probe, fleet, {"bucket": bucket, "key": "objects/"}, 2, active,
        option_overrides={**options, "recursive": True}, status_endpoints=tuple(binding["endpoints"])[1:])
    require(validate_version_receipts(control, bucket, [item for item in expected if item["key"].startswith("objects/")], 2)
            ["bucketIncarnationId"] == incarnation, "J02 opaque fault lost its independent healthy control")
    require(physical_content(fleet.snapshot("original-opaque-before-admission", metadata_faults=metadata_faults)) == physical_content(damaged),
            "J02 opaque physical fault changed before original admission")
    admissions = []
    status = run_task(probe, fleet, {"bucket": bucket, "key": key}, 2, active, option_overrides=options,
                      status_endpoints=tuple(binding["endpoints"])[1:], admissions=admissions)
    fleet.record("original-opaque-terminal-before-rescue", status)
    result = opaque_metadata_outcome(status, bucket, key, options, incarnation)
    require(len(admissions) == 1, "J02 opaque original admission is ambiguous")
    idle_fleet(fleet, binding); fleet.stop()
    require(physical_content(fleet.snapshot("original-opaque-after-task-before-rescue", quiescent=True,
                                         metadata_faults=metadata_faults)) == physical_content(damaged),
            "J02 opaque original task changed metadata, payload or control bytes")
    rescue(fleet)
    require(physical_content(fleet.snapshot("original-opaque-restored-original-bytes", quiescent=True)) == physical_content(original),
            "J02 opaque rescue did not restore the exact original metadata generation")
    fleet.start()
    for endpoint in binding["endpoints"]:
        observed = fleet.record("original-opaque-restored-full-S3", Probe(binding, endpoint).inventory(
            bucket, unversioned_current_tags=fixture == "DB"))
        verify_seed(expected, observed); compare_inventories(baseline, observed)
    unchanged_token(probe, fleet, admissions[0], status, "original-opaque-token-after-fixture-rescue")
    idle_fleet(fleet, binding); fleet.stop()
    require(physical_content(fleet.snapshot("original-opaque-after-restored-business-reads", quiescent=True)) == physical_content(original),
            "J02 opaque business reads changed the restored generation")
    result.update(originalMetadataCopiesCorrupted=len(targets), physicalFaultRetainedBeforeRescue=True,
                  originalBytesRestored=True, originalTokenUnchanged=True, fixtureRescueIsProductRepair=False)
    return expected, baseline, result


def run_branch(args, topology, nodes, fixture, branch, candidate):
    directory = args.evidence / fixture / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory,
                  runtime_profile="outcome-legacy" if branch == "unknown-negative" else "outcome-grace" if branch == "deferred" else "manual")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    fleet.snapshot_buckets = [bucket]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 128, "maxInventoryBytes": 512 * 1024**2}
    fleet.record("binding", binding)
    contract = verify_quorum_contract(fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate)), candidate)
    probe, active, pending = Probe(binding), [], None
    result = {"topology": topology, "fixture": fixture, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        pending = {"operation": "seed", "bucket": bucket}
        expected = seed(Recorder(probe, directory / "fixture"), bucket, fixture, 1 if branch == "unknown-negative" else 4 if branch == "readonly-terminal-failure" else 3, [SIZE])
        baseline = fleet.record("ACK-bound-full-S3", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        fleet.record("complete-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        pending = None
        options = {**BASE_OPTIONS, "recursive": True}
        anchor = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
                          option_overrides=options, status_endpoints=tuple(binding["endpoints"])[1:])
        incarnation = validate_version_receipts(anchor, bucket, expected, 2)["bucketIncarnationId"]
        idle_fleet(fleet, binding)
        fleet.stop()
        if branch == "unknown-negative":
            pending = {"operation": "original-corrupt-metadata-source-probe", "bucket": bucket}
            expected, baseline, result["originalCorruptMetadataProbe"] = original_opaque_probe(
                probe, fleet, binding, directory, expected, fixture, incarnation, active)
            pending = None
            fleet.snapshot("protected-control-before-legacy-writing", quiescent=True)
            pending = {"operation": "append-real-legacy-fixture", "bucket": bucket}
            expected = append_legacy_fixture(probe, fleet, directory, binding, expected, fixture)
            baseline = fleet.record("legacy-ACK-bound-full-S3", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
            fleet.record("complete-protected-and-legacy-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
            pending = None
            idle_fleet(fleet, binding)
            fleet.stop()
        if branch == "deferred":
            pending = {"operation": "original-version-specific-grace-source-procedure", "bucket": bucket}
            expected, baseline, result["originalGraceProcedure"] = original_grace_probe(
                probe, fleet, binding, directory, expected, incarnation, active, unchanged_token)
            pending = None
        original = fleet.snapshot("healthy-physical-baseline", quiescent=True)
        plan = legacy_manifest(original, bucket, expected) if branch == "unknown-negative" else fault_plan(original, bucket, expected, branch)
        fleet.record("independent-manifest-and-frozen-fault-plan", plan)
        for target in [plan[key] for key in ("repair", "failed") if key in plan]:
            verify_proof(fleet, target, "original-full-cohort-bitrot")
        prior = original
        faulted = original
        for index, target in enumerate(plan.get("faults", [])):
            params = {"disk": target["disk"], "faultId": bounded_fault_id("J02", branch, index)}
            fleet.faults.append((target["node"], params))
            receipt = fleet.record("stopped-original-file-fault", {"node": target["node"], **fleet.remote(
                target["node"], "apply-fault", **params, relativePath=target["relativePath"],
                expectedSha256=target["sha256"], kind="missing")})
            faulted = fleet.snapshot("readonly-single-missing-part" if branch == "readonly-terminal-failure" else "actual-R-minus-one-fault", quiescent=True)
            validate_fault(prior, faulted, target, receipt)
            prior = faulted
        if branch == "readonly-terminal-failure":
            target = plan["repair"]
            fleet.drive_gates.add(target["node"])
            fleet.record("readonly-stopped-gate-arm", {"node": target["node"], **fleet.remote(
                target["node"], "drive-gate", disk=target["disk"], mode="read-only")})
        fleet.start()
        require(physical_content(faulted) == physical_content(fleet.snapshot("before-original-task")),
                "J02 attribution window lost: background activity changed a fault before admission")
        if branch == "readonly-terminal-failure":
            result["readonly"] = readonly_compensation(probe, fleet, binding, original, faulted, plan,
                expected, baseline, incarnation, active)
            result.update(verdict="PASS", fixtureRescueIsProductRepair=False, sameTokenRecoveryClaimed=False)
            return result
        if branch == "mixed-healthy-repaired-failed":
            for endpoint in binding["endpoints"]:
                observed = fleet.record("pre-task-failed-version-GET", Probe(binding, endpoint).s3("get_object", {
                    "Bucket": bucket, "Key": plan["failed"]["key"], "VersionId": plan["failed"]["versionId"]}))
                validate_unrecoverable_get(observed, contract)
        options = {**options, "dryRun": branch == "dry-run", "recursive": branch != "deferred"}
        scope = {"bucket": bucket, "key": plan["failed"]["key"] if branch == "deferred" else ""}
        admissions = []
        status = run_task(probe, fleet, scope, 2, active, option_overrides=options,
                          status_endpoints=tuple(binding["endpoints"])[1:], admissions=admissions)
        require(len(admissions) == 1, "J02 original fault task lacks an exact independent admission")
        fleet.record("original-terminal-before-any-rescue", status)
        result["responsibility"] = reconcile(status, bucket, plan["manifest"], options, incarnation,
            attempt_failures=FAILED_ATTEMPTS if branch == "mixed-healthy-repaired-failed" else 1 if branch == "deferred" else 0,
            legacy_unknown=tuple(tuple(pair) for pair in plan.get("unknownIdentities", [])))
        idle_fleet(fleet, binding)
        fleet.stop()
        after = fleet.snapshot("original-result-physical-scene", quiescent=True)
        if branch == "unknown-negative":
            require(physical_content(original) == physical_content(after), "J02 legacy negative mutated its original physical scene")
            result["physical"] = {"physicalUnchanged": True, "productRepairClaimed": False,
                                  "legacyPayloadIntegrityCertified": False}
        else:
            result["physical"] = retained_failure_scene(original, faulted, after, plan, branch)
        if branch == "mixed-healthy-repaired-failed":
            verify_proof(fleet, plan["repair"], "actual-repaired-shard-before-rescue-or-GET")
        rescue(fleet)
        restored = fleet.snapshot("fixture-restored-after-original-task-archival", quiescent=True)
        if branch == "mixed-healthy-repaired-failed":
            validate_repaired_physical(original, restored, plan["repair"])
        else:
            require(physical_content(original) == physical_content(restored), "J02 rescue changed the original ACK generation")
        for target in [plan[key] for key in ("repair", "failed") if key in plan]:
            verify_proof(fleet, target, "all-original-shards-after-fixture-rescue")
        fleet.start()
        for endpoint in binding["endpoints"]:
            observed = fleet.record("all-peer-restored-full-S3", Probe(binding, endpoint).inventory(
                bucket, unversioned_current_tags=fixture == "DB"))
            verify_seed(expected, observed)
            compare_inventories(baseline, observed)
        repeated = fleet.record("original-token-after-rescue", probe.admin(admissions[0]["path"],
            [("clientToken", admissions[0]["clientToken"])], b""))
        require(repeated.get("httpStatus") == 200 and repeated.get("response", {}).get("summary") == status["summary"]
                and repeated["response"].get("settings") == options
                and repeated["response"].get("truncated", False) is False
                and canonical(repeated["response"].get("outcome")) == canonical(status["outcome"]),
                "J02 rescue overwrote the original failed/deferred/dry-run responsibility")
        unobserved = any(result.get(name, {}).get("verdict") == "NOT_OBSERVED"
                         for name in ("originalCorruptMetadataProbe", "originalGraceProcedure"))
        result.update(verdict="NOT_OBSERVED" if unobserved else "PASS", recoveryIsProductRepair=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try: fleet.snapshot("failure-scene-before-fault-rescue")
            except Exception as capture: fleet.record("failure-scene-error", {"error": str(capture)})
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-unresolved-error", {"error": str(exc)})
        if pending is not None:
            try: fleet.stop()
            except Exception as exc: fleet.record("retain-stop-error", {"error": str(exc)})
            fleet.record("retain-unknown-seed-receipt", {"state": "DIRTY", "namespacesRetained": True, "request": pending})
            clean = False
        else:
            clean = fleet.cleanup()
        result["cleanup"] = "PASS" if clean else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"J02 interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--case-id", choices=(CASE_ID,), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    parser.add_argument("--rc-bin", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--tool-directory", type=Path, required=True)
    args = parser.parse_args()
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "J02 executor omitted a source branch")
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] in REVIEWED_CANDIDATES, "J02 retry/deferral contract is not reviewed for this candidate")
    native = verify_manager(os.environ.get("VALIDATION_HEAL_MANAGER_DIRECTORY", ""), candidate, CASE_ID)
    save(args.evidence / "independent-native-outcome-guard.json", native)
    require(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)
            and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "J02 immutable workflow/binary identities required")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) == len(set(nodes)) == 4 and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "J02 requires the four current distinct auto-testing Heal nodes")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes)):
            for fixture in ("DB", "DV"):
                for branch in BRANCHES:
                    result = run_branch(args, topology, selected, fixture, branch, candidate)
                    results.append(result)
                    if result["cleanup"] != "PASS": return 1
    finally:
        wanted = {(topology, fixture, branch) for topology in ("H1", "H2") for fixture in ("DB", "DV") for branch in BRANCHES}
        complete = {(row["topology"], row["fixture"], row["branch"]) for row in results} == wanted and len(results) == len(wanted)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL",
            "diagnosticBranchesPassed": complete and all(row["verdict"] == row["cleanup"] == "PASS" for row in results),
            "complete": False, "canonicalCasePassClaimed": False, "sourceAssertionGaps": list(SOURCE_ASSERTION_GAPS),
            "canonicalAdmissionEnabled": False, "liveSourceProcedureAcceptancePending": True})
    return 1


if __name__ == "__main__": sys.exit(main())
