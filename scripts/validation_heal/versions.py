#!/usr/bin/env python3
"""D03/J04: frozen version cohorts, recursive repair and repeat responsibility."""

from __future__ import annotations

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

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .shards import (SIZES, files, map_target, run_task, validate_fault,
                         validate_repaired_physical, verify_cohort, verify_ranges)
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import (SIZES, files, map_target, run_task, validate_fault,
                        validate_repaired_physical, verify_cohort, verify_ranges)


BRANCHES = {
    "heal.historical-versions": ("old-version-part", "current-version-part",
                                 "recursive-all-versions", "multiple-version-targets"),
    "heal.repeat-idempotency": ("normal-repeat", "deep-repeat", "repeat-after-restart", "versioned-repeat"),
    "heal.versioning-suspended-null": ("enabled-to-suspended", "null-overwrite", "null-delete-marker"),
}
VERSION_SIZE = 5 * 1024**2 + 123


def version_identity(entry):
    return entry["key"], entry.get("versionId") or "null"


def validate_version_receipts(status, bucket, expected, mode, *, required_repairs=(), metadata_repairs=()):
    """Prove the complete responsibility set; counters alone never certify repair."""
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "finished" and status.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed"
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False,
            "version task lacks complete terminal coverage")
    wanted = {version_identity(entry): entry for entry in expected}
    require(wanted and len(wanted) == len(expected), "fixture has duplicate version responsibility")
    objects = outcome.get("objects", [])
    require(len(objects) == len(wanted), "version task omitted or duplicated a version/marker receipt")
    actual, incarnation = {}, None
    required_repairs, metadata_repairs = set(required_repairs), set(metadata_repairs)
    require(required_repairs <= set(wanted) and metadata_repairs <= {key for key, _ in wanted},
            "repair plan escapes the selected responsibility set")
    for receipt in objects:
        identity = receipt.get("identity", {})
        pair = identity.get("object"), identity.get("versionId") or "null"
        require(pair in wanted and pair not in actual and identity.get("bucket") == bucket
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and isinstance(identity.get("bucketIncarnationId"), str) and identity["bucketIncarnationId"],
                "version responsibility has a wrong, duplicate or incomplete identity")
        incarnation = incarnation or identity["bucketIncarnationId"]
        require(identity["bucketIncarnationId"] == incarnation, "version receipts mix bucket incarnations")
        state = receipt.get("disposition", {}).get("state")
        healthy = "metadata_healthy" if wanted[pair]["deleteMarker"] or mode == 1 else "verified_healthy"
        if pair in required_repairs:
            require(state == "repaired", "faulted version lacks a repaired responsibility")
        elif pair[0] in metadata_repairs:
            require(state in {"repaired", healthy}, "metadata recovery returned unknown or a false healthy disposition")
        else:
            require(state == healthy, "repeat or control version claimed repair, unknown, or insufficient integrity")
        actual[pair] = state
    require(set(actual) == set(wanted), "version responsibility set differs from acknowledged fixture")
    for key in metadata_repairs:
        require(any(pair[0] == key and state == "repaired" for pair, state in actual.items()),
                "missing metadata was never attributed as repaired")
    healed = sum(state == "repaired" for state in actual.values())
    counters = outcome.get("counters", {})
    wanted_counters = {"processed": len(wanted), "healed": healed, "unchanged": len(wanted) - healed,
                       "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(counters.get(name)) is type(value) and counters[name] == value
                for name, value in wanted_counters.items()), "version counters differ from complete receipt dispositions")
    return {"completeVersionAndMarkerResponsibility": True, "receipts": len(actual), "repaired": healed,
            "mode": mode, "bucketIncarnationId": incarnation}


def choose_target(snapshot, bucket, entry, role, *, excluded_drives=()):
    target = map_target(snapshot, bucket, entry["key"], role, 1, entry["versionId"], latest_required=False)
    selected = next((shard for shard in target["cohort"] if shard["role"] == role
                     and (shard["node"], shard["disk"]) not in excluded_drives), None)
    require(selected is not None, "distinct physical version fault position is unavailable")
    return {**target, **selected}


def metadata_target(snapshot, part_target):
    relative = f"{part_target['bucket']}/{part_target['key']}/xl.meta"
    entry = files(snapshot)[part_target["node"], part_target["disk"], relative]
    return {**part_target, "relativePath": relative, "sha256": entry["sha256"], "size": entry["size"]}


def suspend_fixture(recorder, bucket, historical):
    expected = copy.deepcopy(historical)
    recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Suspended"}})
    require(recorder.call("get_bucket_versioning", {"Bucket": bucket}).get("Status") == "Suspended",
            "versioning suspension was not persisted")
    for key in sorted({entry["key"] for entry in historical}):
        body = payload(f"{bucket}/{key}/3", VERSION_SIZE)
        params = {"Bucket": bucket, "Key": key, "ContentType": "application/octet-stream",
                  "Metadata": {"heal-fixture": "suspended-null"}, "Tagging": "suite=heal&fixture=DV"}
        for content in (payload(f"{bucket}/{key}/superseded-null", VERSION_SIZE), body):
            response = recorder.call("put_object", params, content)
            require(response.get("VersionId", "null") == "null", "suspended PUT created a non-null version")
        # Create/delete one null marker on every key, then rebuild the live null
        # value. This witnesses actual suspended overwrite and deletion ACKs;
        # it does not invent history by editing xl.meta.
        marker = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
        require(marker.get("DeleteMarker") is True and marker.get("VersionId", "null") == "null",
                "suspended delete did not acknowledge a null marker")
        deleted = recorder.call("delete_object", {"Bucket": bucket, "Key": key, "VersionId": "null"})
        require(deleted.get("DeleteMarker") is True and deleted.get("VersionId", "null") == "null",
                "precise null-marker removal was not acknowledged")
        response = recorder.call("put_object", params, body)
        require(response.get("VersionId", "null") == "null", "suspended overwrite changed null identity")
        for entry in expected:
            if entry["key"] == key:
                entry["isLatest"] = False
        null_entry = {"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                      "payload": {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body)}}
        if key == "objects/0002.bin":
            marker = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
            require(marker.get("DeleteMarker") is True and marker.get("VersionId", "null") == "null",
                    "final suspended delete lacks the exact null marker")
            null_entry = {"key": key, "versionId": "null", "deleteMarker": True, "isLatest": True}
        expected.append(null_entry)
    return expected


def reenable_and_verify(recorder, fleet, bucket, baseline):
    recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    require(recorder.call("get_bucket_versioning", {"Bucket": bucket}).get("Status") == "Enabled",
            "versioning was not re-enabled after null repair")
    key = "objects/re-enabled.bin"
    body = payload(f"{bucket}/{key}/0", VERSION_SIZE)
    put = recorder.call("put_object", {"Bucket": bucket, "Key": key}, body)
    require(put.get("VersionId") not in {None, "null"}, "re-enabled versioning did not create a real new version")
    current = fleet.record("re-enabled-s3", recorder.probe.inventory(bucket))
    new_entries = [entry for entry in current["entries"] if entry["key"] == key]
    require(len(new_entries) == 1 and new_entries[0]["versionId"] == put["VersionId"]
            and new_entries[0]["payload"] == {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            and new_entries[0]["isLatest"] is True and not new_entries[0]["deleteMarker"],
            "re-enabled version identity or independent payload differs")
    # The inventory's current listing also contains the new key. Compare
    # original entries directly, and require one exact additional entry.
    require(len(current["entries"]) == len(baseline["entries"]) + 1
            and [entry for entry in current["entries"] if entry["key"] != key] == baseline["entries"],
            "re-enabling versioning changed prior null, historical or marker identities/content")
    return {"reenabled": True, "newVersionId": put["VersionId"], "allOriginalVersionsStillAddressable": True}


def branch_plan(case_id, branch, snapshot, bucket, expected, fixture):
    """Plans are derived from ACK order and decoded physical roles, never disk numbers."""
    branch_index = BRANCHES[case_id].index(branch)
    key = f"objects/{branch_index:04d}.bin" if fixture == "DV" else f"objects/{8 + 9 * branch_index:04d}.bin"
    if branch == "versioned-repeat":
        key = "objects/0000.bin"
    selected = [entry for entry in expected if entry["key"] == key]
    data = [entry for entry in selected if not entry["deleteMarker"]]
    if case_id == "heal.versioning-suspended-null":
        historical = [entry for entry in data if entry["versionId"] != "null"]
        require(len(historical) == 3, "suspended fixture lost enabled-period historical versions")
        if branch == "null-delete-marker":
            old = choose_target(snapshot, bucket, historical[0], "data")
            proofs = [choose_target(snapshot, bucket, entry, "data") for entry in historical]
            proofs = [{**item, **next(shard for shard in item["cohort"]
                       if (shard["node"], shard["disk"]) == (old["node"], old["disk"]))} for item in proofs]
            require(any(entry["versionId"] == "null" and entry["deleteMarker"] and entry["isLatest"]
                        for entry in selected), "null-marker branch lacks its current marker")
            faults, required, metadata_keys = [metadata_target(snapshot, old)], set(), {key}
        else:
            null = next((entry for entry in data if entry["versionId"] == "null" and entry["isLatest"]), None)
            require(null is not None, "suspended fixture lost its live null identity")
            target = choose_target(snapshot, bucket, null, "data" if branch_index == 0 else "parity")
            faults, proofs, required, metadata_keys = [target], [target], {version_identity(null)}, set()
        return {"scope": {"bucket": bucket, "key": key}, "recursive": True, "expected": selected,
                "faults": faults, "proofs": proofs, "requiredRepairs": required, "metadataRepairs": metadata_keys}
    require(len(data) == (3 if fixture == "DV" else 1), "branch is missing its distinct acknowledged payload versions")
    old = choose_target(snapshot, bucket, data[0], "data")
    if case_id == "heal.repeat-idempotency":
        if fixture == "DV":
            proofs = [choose_target(snapshot, bucket, entry, "data") for entry in data]
            # A whole xl.meta contains all three versions and the marker. Keep
            # every version's target-drive maintenance references in the oracle.
            proofs = [{**item, **next(shard for shard in item["cohort"]
                       if (shard["node"], shard["disk"]) == (old["node"], old["disk"]))} for item in proofs]
            faults = [metadata_target(snapshot, old)]
            required, metadata_keys = set(), {key}
        else:
            proofs, faults, required, metadata_keys = [old], [old], {version_identity(data[0])}, set()
        return {"scope": {"bucket": bucket, "key": key}, "recursive": fixture == "DV",
                "expected": selected, "faults": faults, "proofs": proofs,
                "requiredRepairs": required, "metadataRepairs": metadata_keys}
    current = choose_target(snapshot, bucket, data[-1], "parity", excluded_drives={(old["node"], old["disk"])})
    faults, proofs, metadata_keys = [old], [old], set()
    if branch == "current-version-part":
        require(data[-1]["isLatest"] is True, "current-version branch selected a historical version")
        faults, proofs = [current], [current]
    elif branch == "recursive-all-versions":
        # Missing historical part plus another drive's complete metadata tests
        # both F1 and F3 with an intact same-version authority quorum.
        faults, proofs = [old, metadata_target(snapshot, current)], [old, current]
        metadata_keys = {key}
    elif branch == "multiple-version-targets":
        faults, proofs = [old, current], [old, current]
    recursive_bucket = branch == "recursive-all-versions"
    return {"scope": {"bucket": bucket, "key": "" if recursive_bucket else key}, "recursive": True,
            "expected": expected if recursive_bucket else selected, "faults": faults, "proofs": proofs,
            "requiredRepairs": {version_identity(item) for item in faults if not item["relativePath"].endswith("/xl.meta")},
            "metadataRepairs": metadata_keys}


def verify_version_s3(probe, fleet, bucket, expected, baseline, label, *, fixture="DV"):
    current = fleet.record(label, probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
    comparison = compare_inventories(baseline, current)
    ranges = []
    for entry in expected:
        if entry["deleteMarker"]:
            continue
        head = fleet.record(label + "-head", probe.s3("head_object", {
            "Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]}))
        observed = next(item for item in baseline["entries"] if version_identity(item) == version_identity(entry))
        require(head["acknowledged"] and all(head["response"].get(name) == value
                for name, value in observed["properties"].items()), "version HEAD differs from frozen public properties")
        if entry["payload"]["size"] < 1024**2:
            continue
        nonce = next(index for index, item in enumerate(
            item for item in expected if item["key"] == entry["key"] and not item["deleteMarker"])
            if item["versionId"] == entry["versionId"])
        ranges.append({"key": entry["key"], "versionId": entry["versionId"], "proof": verify_ranges(
            probe, fleet, bucket, entry["key"], version_id=entry["versionId"],
            expected_body=payload(f"{bucket}/{entry['key']}/{nonce}", entry["payload"]["size"]))})
    return {"inventory": comparison, "historicalRanges": ranges}


def run_topology(args, topology, nodes, fixture):
    directory = args.evidence / fixture / topology
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory)
    candidate = json.loads(args.candidate.read_text())
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 256, "maxInventoryBytes": 512 * 1024**2}
    fleet.record("binding", binding)
    recipe = select(args.case_id)[0]
    require(list(BRANCHES[args.case_id]) == recipe["variants"], "executor omitted a source version branch")
    branches = [branch for branch in BRANCHES[args.case_id]
                if args.case_id != "heal.repeat-idempotency" or (branch == "versioned-repeat") == (fixture == "DV")]
    result = {"topology": topology, "fixture": fixture, "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    active, admissions = [], []
    probe = Probe(binding)
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, fixture,
                        4 if fixture == "DV" else 128, [VERSION_SIZE] if fixture == "DV" else SIZES)
        if args.case_id == "heal.versioning-suspended-null":
            expected = suspend_fixture(recorder, bucket, expected)
        baseline = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        for branch in branches:
            registrations = []
            try:
                fleet.stop()
                before = fleet.snapshot("physical-before-" + branch, quiescent=True)
                plan = branch_plan(args.case_id, branch, before, bucket, expected, fixture)
                fleet.record("fault-plan-" + branch, {**plan, "requiredRepairs": sorted(plan["requiredRepairs"]),
                                                     "metadataRepairs": sorted(plan["metadataRepairs"])})
                for target in plan["proofs"]:
                    verify_cohort(fleet, target, "baseline-bitrot-" + branch)
                prior = before
                for index, target in enumerate(plan["faults"]):
                    params = {"disk": target["disk"], "faultId": recipe["code"] + "-" + branch + "-" + str(index)}
                    registration = target["node"], params
                    fleet.faults.append(registration)
                    registrations.append(registration)
                    receipt = fleet.record("fault-" + branch, fleet.remote(target["node"], "apply-fault",
                        **params, relativePath=target["relativePath"], expectedSha256=target["sha256"], kind="missing"))
                    witnessed = fleet.snapshot("physical-fault-" + branch, quiescent=True)
                    validate_fault(prior, witnessed, target, receipt)
                    prior = witnessed
                fleet.start()
                witness = fleet.snapshot("physical-before-trigger-" + branch)
                require(physical_content(prior) == physical_content(witness),
                        "ATTRIBUTION_NOT_OBSERVED: version fault changed before the manual token")
                status = run_task(probe, fleet, plan["scope"], 2, active,
                                  option_overrides={"recursive": plan["recursive"]}, admissions=admissions)
                fleet.stop()
                repaired = fleet.snapshot("physical-after-" + branch, quiescent=True)
                physical = validate_repaired_physical(before, repaired, plan["proofs"][0], repair_targets=plan["proofs"])
                bitrot = [verify_cohort(fleet, target, "repaired-bitrot-" + branch) for target in plan["proofs"]]
                fleet.start()
                s3 = verify_version_s3(probe, fleet, bucket, expected, baseline, "s3-after-" + branch, fixture=fixture)
                # These faults have now been independently repaired and fully
                # proved. Finalization discards their backups; it never writes
                # old metadata over a legitimate product re-encoding.
                for registration in registrations:
                    fleet.faults.remove(registration)
                registrations.clear()
                fleet.record("fault-finalization-proof", {"variant": branch, "allOriginalPartBytesVerified": True,
                    "decodedVersionReferencesAndS3Equal": True, "backupRestorationUsed": False,
                    "canonicalVerdict": "still requires the exact responsibility oracle"})
                responsibility = validate_version_receipts(status, bucket, plan["expected"], 2,
                    required_repairs=plan["requiredRepairs"], metadata_repairs=plan["metadataRepairs"])
                repeats = []
                if args.case_id == "heal.repeat-idempotency":
                    fleet.stop()
                    post_heal = fleet.snapshot("post-heal-repeat-baseline-" + branch, quiescent=True)
                    fleet.start()
                    modes = (1,) if branch == "normal-repeat" else (2,)
                    if branch in {"repeat-after-restart", "versioned-repeat"}:
                        modes = (1, 2)
                    first_token = admissions[-1]["clientToken"]
                    for mode in modes:
                        # Restart the same run-owned units, keeping disks and
                        # candidate identity, before the process repeat branch.
                        if branch == "repeat-after-restart":
                            old_processes = fleet.snapshot("before-process-restart")
                            fleet.stop()
                            fleet.start()
                            new_processes = fleet.snapshot("restarted-process")
                            require(all(old_processes[node]["unit"] == new_processes[node]["unit"]
                                    and old_processes[node]["pid"] != new_processes[node]["pid"]
                                    for node in nodes), "process repeat did not restart the same original units")
                            fleet.record("process-restart-proof", {"before": old_processes, "after": new_processes})
                        repeat = run_task(probe, fleet, plan["scope"], mode, active,
                            option_overrides={"recursive": plan["recursive"]}, admissions=admissions)
                        require(admissions[-1]["clientToken"] != first_token
                                and len({entry["clientToken"] for entry in admissions}) == len(admissions),
                                "repeat task reused a token instead of admitting independent responsibility")
                        proof = validate_version_receipts(repeat, bucket, plan["expected"], mode)
                        fleet.stop()
                        after_repeat = fleet.snapshot("physical-repeat-" + branch, quiescent=True)
                        validate_repaired_physical(post_heal, after_repeat, plan["proofs"][0], repair_targets=plan["proofs"])
                        for target in plan["proofs"]:
                            verify_cohort(fleet, target, "repeat-bitrot-" + branch)
                        fleet.start()
                        proof["s3"] = verify_version_s3(probe, fleet, bucket, expected, baseline, "s3-repeat-" + branch, fixture=fixture)
                        repeats.append(proof)
                result["branches"][branch] = {"verdict": "PASS", "physical": physical, "bitrot": bitrot,
                    "s3": s3, "responsibility": responsibility, "repeats": repeats}
            except (Exception, KeyboardInterrupt) as exc:
                result["branches"][branch] = {"verdict": "FAIL", "failure": {"type": type(exc).__name__, "message": str(exc)}}
                try:
                    fleet.stop()
                    fleet.snapshot("physical-failure-" + branch, quiescent=True)
                    for registration in reversed(registrations):
                        node, params = registration
                        fleet.record("rescue-" + branch, fleet.remote(node, "recover-fault", **params))
                        fleet.faults.remove(registration)
                except Exception as recovery_error:
                    result["branches"][branch]["recoveryError"] = str(recovery_error)
                    raise
                if isinstance(exc, KeyboardInterrupt) or active:
                    raise
        result["verdict"] = "PASS" if len(result["branches"]) == len(branches) and all(
            item["verdict"] == "PASS" for item in result["branches"].values()) else "FAIL"
        if args.case_id == "heal.versioning-suspended-null" and result["verdict"] == "PASS":
            result["reenable"] = reenable_and_verify(recorder, fleet, bucket, baseline)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
    finally:
        for path, token in active:
            try:
                fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc:
                fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("topology-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--case-id", choices=tuple(BRANCHES), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    parser.add_argument("--rc-bin", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--tool-directory", type=Path, required=True)
    args = parser.parse_args()
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes)
            and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal nodes required")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)), "invalid candidate binary digest")
    require(bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "workflow fleet identity required")
    fixtures = ("DB", "DV") if args.case_id == "heal.repeat-idempotency" else ("DV",)
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for fixture in fixtures:
                results.append(run_topology(args, topology, selected, fixture))
                if results[-1]["cleanup"] != "PASS":
                    break
            if results[-1]["cleanup"] != "PASS":
                break
    finally:
        complete = len(results) == 2 * len(fixtures) and all(
            item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": args.case_id, "results": results,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "version case requires every H1/H2 source branch; inspect topology-result evidence")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal version validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
