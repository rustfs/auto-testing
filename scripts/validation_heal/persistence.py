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
    from .root_restart import recovery_status
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
    from root_restart import recovery_status
    from versions import public_version_id, verify_proof, version_identity


CASE_ID = "heal.checkpoint-and-mrf-corruption"
BRANCHES = ("checkpoint-truncate", "mrf-checksum", "future-schema", "stale-generation",
            "journal-write-failure", "native-persist-fence")
IMPLEMENTED = {"checkpoint-truncate": "truncate", "future-schema": "future-schema"}
BLOCKERS = {
    "mrf-checksum": "H1/H2 ACK-bound committed MRF corruption, retention and replay orchestration is incomplete",
    "stale-generation": "an actual earlier checkpoint capture exists; complete rollback and durable responsibility reconciliation is still required",
    "journal-write-failure": "the private bounded volume exists; actual checkpoint write refusal, retained ownership and release/rescan orchestration is still required",
    "native-persist-fence": "publication interruption prerequisites do not prove every required checkpoint/MRF fsync and commit interruption boundary",
}
SIZE = 5 * 1024**2 + 123


def exact_records(observed, token, family):
    names = journal.names(family, token)
    require(isinstance(observed, dict) and len(observed) in {1, 4}, "journal omitted bound physical nodes")
    present = {}
    for node, census in observed.items():
        rows = census.get("records")
        require(census.get("readOnly") is True and census.get("quiescent") is True
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


def retained_corruption(observed, roots, frozen, receipt, current):
    token = frozen["taskId"]
    failed_task(current)
    require(receipt.get("applied") is True and receipt.get("originalBackupRetained") is True
            and receipt.get("productRepairClaimed") is False
            and receipt.get("family") == "checkpoint" and receipt.get("taskId") == token
            and receipt.get("disk") == frozen["disk"]
            and receipt.get("relativePath") == frozen["checkpoint"]["relativePath"]
            and journal.same_json(receipt.get("before"), journal.entry(checkpoints.bytes_of(frozen["checkpoint"]))),
            "journal injection receipt changed the frozen original task or bytes")
    require(receipt.get("kind") in IMPLEMENTED.values()
            and receipt.get("injected") == journal.entry(journal.transform(
                checkpoints.bytes_of(frozen["checkpoint"]), receipt, {"bucket": frozen["bucket"]}, None)),
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
            and journal.same_json(terminal.get("options"), owner["value"]["options"])
            and isinstance(terminal.get("status"), dict) and set(terminal["status"]) == {"Failed"}
            and isinstance(terminal["status"]["Failed"], dict)
            and isinstance(terminal["status"]["Failed"].get("error"), str)
            and "checkpoint" in terminal["status"]["Failed"]["error"].lower()
            and type(report.get("schema")) is int and report["schema"] == 1
            and report.get("terminal") == terminal
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
    require(current.get("summary") == "finished" and outcome.get("execution") == {"state": "completed"}
            and current.get("settings") == checkpoints.OPTIONS and type(current.get("truncated")) is bool
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


def run_checkpoint_fault(args, topology, nodes, fixture, branch, candidate):
    require(branch in IMPLEMENTED, "unimplemented journal branch cannot mutate the fleet")
    directory = args.evidence / fixture / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, resource_profile="cursor", runtime_profile="root-restart")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    fleet.snapshot_buckets = [bucket]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    count = 257 if fixture == "DB" else 80
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 512, "maxInventoryBytes": 2 * 1024**3}
    fleet.record("binding", binding)
    fleet.record("candidate-bound-inspector", verify_inspector(args.tool_directory, candidate))
    probe, active = Probe(binding), []
    result = {"topology": topology, "fixture": fixture, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        expected = seed(Recorder(probe, directory / "fixture"), bucket, fixture, count, [SIZE])
        baseline = fleet.record("ACK-bound-complete-S3", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        fleet.record("complete-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
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
        token = checkpoints.admit(probe, fleet, active)
        frozen, identity = checkpoints.freeze(fleet, token, bucket)
        frozen = checkpoints.stop_for_fault(fleet, token, bucket, frozen, identity)
        params = {"family": "checkpoint", "taskId": token, "disk": frozen["disk"],
            "relativePath": frozen["checkpoint"]["relativePath"], "faultId": "F07-" + branch,
            "kind": IMPLEMENTED[branch], "expectedBefore": journal.entry(checkpoints.bytes_of(frozen["checkpoint"]))}
        fleet.journal_faults.append((frozen["node"], params))
        receipt = fleet.record("stopped-original-checkpoint-injection", fleet.remote(frozen["node"], "journal-apply", **params))
        fleet.start()
        deadline = time.monotonic() + 180
        while True:
            failed = recovery_status(probe, fleet, token)
            if failed.get("summary") != "running": break
            require(time.monotonic() < deadline, "original corrupt checkpoint task did not produce a bounded diagnostic")
            time.sleep(.2)
        failed_task(failed)
        active.remove((checkpoints.PATH, token))
        idle_fleet(fleet, binding)
        fleet.stop()
        corrupt_scene = checkpoints.records(fleet, token, "original-corrupt-checkpoint", quiescent=True)
        failure_roots = checkpoints.records(fleet, token, "original-durable-failure", quiescent=True, family="root")
        result["originalFailure"] = retained_corruption(corrupt_scene, failure_roots, frozen, receipt, failed)
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
        retained_corruption(retained_checkpoint, retained_roots, frozen, receipt, repeated)
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
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
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
    require(candidate["source_sha"] == checkpoints.REFERENCE["candidateCommit"], "F07 checkpoint failure contract requires the exact reviewed candidate")
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
                    result = run_checkpoint_fault(args, topology, selected, fixture, branch, candidate) if branch in IMPLEMENTED else {
                        "topology": topology, "fixture": fixture, "branch": branch, "verdict": "BLOCKED", "cleanup": "PASS",
                        "reason": BLOCKERS[branch], "fleetBranchExecuted": False, "canonicalCasePassClaimed": False}
                    results.append(result)
                    if result["cleanup"] != "PASS": return 1
    finally:
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": False,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL",
            "canonicalCasePassClaimed": False, "reason": "four complete branch orchestrations remain blocked"})
    return 1


if __name__ == "__main__": sys.exit(main())
