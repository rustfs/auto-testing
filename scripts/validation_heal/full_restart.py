#!/usr/bin/env python3
"""F05: all owned peers restart with original durable root responsibility."""

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
import time
import uuid

try:
    from .heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .shards import pending_inventory, validate_repaired_physical
    from .cursor import OBJECTS, SIZE, TASK_SECONDS, map_cohorts, verify_all_parts, validate_missing_batch
    from .root_restart import (PATH, OPTIONS, records, owned_records, root_intent, same_owner,
                               partial_physical, recovery_status)
    from .stop import wait_partial, idle_fleet
    from .fleet_node import PROCESS_FIELDS
    from .versions import public_version_id
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import pending_inventory, validate_repaired_physical
    from cursor import OBJECTS, SIZE, TASK_SECONDS, map_cohorts, verify_all_parts, validate_missing_batch
    from root_restart import PATH, OPTIONS, records, owned_records, root_intent, same_owner, partial_physical, recovery_status
    from stop import wait_partial, idle_fleet
    from fleet_node import PROCESS_FIELDS
    from versions import public_version_id


CASE_ID = "heal.full-cluster-restart"
BRANCHES = ("all-graceful-restart", "all-restart-with-mpu", "repeat-full-restart")


def manifest(fixtures):
    wanted = {(bucket, entry["key"], entry["versionId"]): entry
              for bucket, fixture in fixtures.items() for entry in fixture["expected"]}
    require(len(wanted) == sum(len(fixture["expected"]) for fixture in fixtures.values())
            and len(wanted) > 1024, "full restart lacks its frozen complete ACK manifest")
    return wanted


def responsibility(current, wanted, repaired):
    outcome = current.get("outcome") or {}
    require(current.get("summary") == "finished" and outcome.get("execution", {}).get("state") == "completed"
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is True,
            "root recovery has no complete responsibility or bounded diagnostic receipts")
    require(repaired and set(repaired) <= set(wanted), "root recovery repair set escaped original ACKs")
    counts = {"processed": len(wanted), "healed": len(repaired), "unchanged": len(wanted) - len(repaired),
              "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    counters = outcome.get("counters", {})
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in counts.items()),
            "root recovery counters omit ACKs, lost responsibility or claim false repair")
    objects = outcome.get("objects")
    require(isinstance(objects, list) and 0 < len(objects) <= 128, "root diagnostics exceed their bounded contract")
    seen, incarnations = set(), {}
    for receipt in objects:
        identity = receipt.get("identity", {})
        pair = identity.get("bucket"), identity.get("object"), public_version_id(identity.get("versionId"))
        incarnation = identity.get("bucketIncarnationId")
        require(pair in wanted and pair not in seen and isinstance(incarnation, str) and incarnation
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0,
                "root diagnostics contain an unknown, duplicate or incomplete ACK identity")
        require(incarnations.setdefault(pair[0], incarnation) == incarnation, "root diagnostics mixed bucket incarnations")
        state = "repaired" if pair in repaired else "metadata_healthy" if wanted[pair]["deleteMarker"] else "verified_healthy"
        require(receipt.get("disposition", {}).get("state") == state, "root diagnostic falsely attributed a repair or integrity check")
        seen.add(pair)
    return {"canonicalCounters": counters, "diagnosticObjects": len(objects),
            "diagnosticsAreFullManifest": False, "originalACKs": len(wanted)}


def terminal_records(observed, token, owner, current, wanted):
    present = owned_records(observed, token)
    require(set(present) == {"terminal", "report"}
            and all((item["node"], item["disk"]) == (owner["node"], owner["disk"]) for item in present.values()),
            "all-peer restart lost the original record owner or retained runnable responsibility")
    terminal, report = present["terminal"]["value"], present["report"]["value"]
    require(type(terminal.get("schema")) is int and terminal["schema"] == 1
            and terminal.get("task_id") == token and terminal.get("status") == "Completed"
            and terminal.get("heal_type") == {"type": "cluster"}
            and canonical(terminal.get("options")) == canonical(owner["value"]["options"]),
            "root completion changed task, scope, checkpointed options or original budget")
    require(type(report.get("schema")) is int and report["schema"] == 1 and report.get("terminal") == terminal
            and canonical(report.get("outcome")) == canonical(current.get("outcome")),
            "root durable report differs from the original recovered task")
    counters = (report.get("outcome") or {}).get("counters") or {}
    require(type(report.get("next_seq")) is int and type(counters.get("processed")) is int
            and report["next_seq"] >= counters["processed"] + 1
            and type(report.get("min_seq")) is int and 1 <= report["min_seq"] <= report["next_seq"]
            and isinstance(report.get("seqed_items"), list) and 0 < len(report["seqed_items"]) <= 1024,
            "root durable report has incomplete retained sequence bounds")
    seen, bucket_events, previous = set(), set(), report["min_seq"] - 1
    wanted_buckets = {pair[0] for pair in wanted}
    for sequence, item in report["seqed_items"]:
        require(type(sequence) is int and sequence == previous + 1 and sequence < report["next_seq"],
                "root durable report skipped a sequence or changed its retained suffix")
        if item.get("type") == "bucket":
            require(item.get("bucket") in wanted_buckets and item.get("object") == ""
                    and item.get("versionId") == "" and item.get("objectSize") == 0
                    and item["bucket"] not in bucket_events,
                    "root durable report contains an unknown or duplicate bucket event")
            bucket_events.add(item["bucket"])
        else:
            pair = item.get("bucket"), item.get("object"), public_version_id(item.get("versionId"))
            require(item.get("type") == "object" and pair in wanted and pair not in seen
                    and item.get("objectSize") == wanted[pair].get("payload", {}).get("size", 0),
                    "root durable report changed an ACK version/marker/size")
            seen.add(pair)
        previous = sequence
    require(previous == report["next_seq"] - 1, "root durable report omitted its retained suffix")
    return {"sameRecordDisk": True, "runnableIntentRetiredOnEveryDisk": True,
            "terminalAndReportMatchOriginalToken": True, "retainedItems": len(seen)}


def all_processes(fleet, label):
    return fleet.record(label, {node: fleet.remote(node, "process") for node in fleet.nodes})


def continuity(before, after):
    require(set(before) == set(after) and len(before) == 4, "full restart omitted an original peer")
    for node, old in before.items():
        new = after[node]
        require(all(new.get(name) == old.get(name) for name in PROCESS_FIELDS if name not in {"pid", "startTicks"})
                and type(new.get("pid")) is int and new["pid"] > 1 and type(new.get("startTicks")) is int
                and (new["pid"], new["startTicks"]) != (old["pid"], old["startTicks"]),
                "all-peer restart changed candidate/boot/unit/environment/namespace or did not restart a peer")
    return {"allFourOriginalProcessesRestarted": True, "sameBootCandidateUnitsEnvironmentAndNamespace": True}


def graceful_all(fleet, processes, first):
    require(first in processes and set(processes) == set(fleet.nodes), "graceful stop omitted original peer identities")
    order = [first, *(node for node in fleet.nodes if node != first)]
    fleet.record("registered-all-peer-stop-order", {"nodes": order, "onlyOwnedUnits": True})
    exits = {}
    for node in order:
        identity = processes[node]
        if node not in fleet.frozen_processes:
            fleet.frozen_processes[node] = identity
            fleet.record("full-restart-freeze", fleet.remote(node, "process-signal", signal="SIGSTOP", expected=identity))
        exits[node] = fleet.record("full-restart-graceful-exit", {"node": node,
            **fleet.remote(node, "process-graceful", expected=identity)})
        require(exits[node].get("actualExitCode") == 0 and exits[node].get("signal") == "SIGTERM"
                and exits[node].get("pidfdBound") is True, "a peer lacks a witnessed normal graceful exit")
        del fleet.frozen_processes[node]
        fleet.record("release-exited-unit", {"node": node, **fleet.remote(node, "stop")})
    return exits


def verify_public(probe, fleet, fixtures, pending, label):
    for endpoint in probe.binding["endpoints"]:
        peer = Probe(probe.binding, endpoint)
        for bucket, fixture in fixtures.items():
            actual = fleet.record(label, {"endpoint": endpoint, "bucket": bucket,
                **peer.inventory(bucket, unversioned_current_tags=fixture["fixture"] != "DV", page_size=127)})
            verify_seed(fixture["expected"], actual)
            compare_inventories(fixture["baseline"], actual)
        actual_pending = fleet.record(label + "-pending-mpu", {"endpoint": endpoint,
            "uploads": pending_inventory(peer, pending["bucket"])})
        require(actual_pending["uploads"] == pending["uploads"], "full restart lost an exact pending UploadId/part/ETag")


def complete_pending_uploads(recorder, fleet, binding, pending):
    bucket = pending["bucket"]
    for upload in pending["uploads"]:
        recorder.call("complete_multipart_upload", {"Bucket": bucket, "Key": upload["key"], "UploadId": upload["uploadId"],
            "MultipartUpload": {"Parts": [{"PartNumber": item["PartNumber"], "ETag": item["ETag"]} for item in upload["parts"]]}})
        expected_body = payload(f"{bucket}/{upload['key']}", 5 * 1024**2 + 3)
        for endpoint in binding["endpoints"]:
            observed = fleet.record("pending-mpu-content-after-repeat", Probe(binding, endpoint).s3("get_object", {"Bucket": bucket, "Key": upload["key"]}))
            require(observed.get("acknowledged") is True and observed.get("resultUnknown") is False
                    and observed.get("payload") == {"size": len(expected_body), "sha256": hashlib.sha256(expected_body).hexdigest()},
                    "restarted pending MPU parts lost their actual acknowledged bytes")


def run_topology(args, nodes):
    directory = args.evidence / "H2"
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h2"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, resource_profile="full-restart", runtime_profile="root-restart")
    candidate = json.loads(args.candidate.read_text())
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    fleet.snapshot_buckets = [bucket, bucket + "-dx", bucket + "-versions"]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, 5)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 2048, "maxInventoryBytes": 2 * 1024**3}
    fleet.record("binding", binding)
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "full restart omitted a source branch")
    result = {"topology": "H2", "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    active, probe = [], Probe(binding)
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), "H2"))
        fleet.record("registered-full-restart-budget", {"dxObjects": OBJECTS, "dxBytes": SIZE,
            "taskSeconds": 300, "recoveryObservationSeconds": TASK_SECONDS, "serviceMinutes": 45,
            "sourceFixtures": ["DB", "DV", "DM"], "allOriginalUnitsRestartedTwice": True})
        recorder = Recorder(probe, directory / "fixture")
        db = seed(recorder, bucket, "DB", 128, [0, 4096, SIZE, 5 * 1024**2 + 123])
        db += seed(recorder, bucket, "DM", 1, [SIZE], existing_bucket=True)
        dx_bucket, dv_bucket = bucket + "-dx", bucket + "-versions"
        dx = seed(recorder, dx_bucket, "DX", OBJECTS, [SIZE])
        dv = seed(recorder, dv_bucket, "DV", 4, [5 * 1024**2 + 123])
        fixtures = {name: {"fixture": kind, "expected": expected} for name, kind, expected in
                    ((bucket, "DB", db), (dx_bucket, "DX", dx), (dv_bucket, "DV", dv))}
        for name, fixture in fixtures.items():
            fixture["baseline"] = fleet.record("s3-baseline", probe.inventory(name, unversioned_current_tags=fixture["fixture"] != "DV", page_size=127))
            fleet.record("fixture-manifest", {"bucket": name, "expected": fixture["expected"], **verify_seed(fixture["expected"], fixture["baseline"])})
        wanted = manifest(fixtures)
        pending = {"bucket": bucket, "uploads": fleet.record("mpu-baseline", pending_inventory(probe, bucket))}
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        dx_scene = copy.deepcopy(original)
        for value in dx_scene.values(): value["files"] = [item for item in value["files"] if item["path"].startswith(dx_bucket + "/")]
        cohorts = map_cohorts(dx_scene, dx_bucket, dx)
        verify_all_parts(fleet, cohorts, "full-restart-baseline-streaming")
        targets, receipts = [cohorts[index] for index in range(0, OBJECTS, 32)], []
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "relativePath": target["relativePath"], "kind": "missing",
                      "faultId": bounded_fault_id("F05", "full-restart", index), "expectedSha256": target["sha256"]}
            fleet.faults.append((target["node"], params))
            receipts.append(fleet.record("fault", {"node": target["node"], **fleet.remote(target["node"], "apply-fault", **params)}))
        faulted = fleet.snapshot("physical-fault", quiescent=True)
        fleet.record("fault-proof", validate_missing_batch(original, faulted, targets, receipts))
        fleet.start()
        require(physical_content(faulted) == physical_content(fleet.snapshot("physical-before-trigger")),
                "ATTRIBUTION_NOT_OBSERVED: restart fault changed before root admission")
        admission = fleet.record("root-admission", probe.admin(PATH, [], json.dumps(OPTIONS).encode()))
        token = admission.get("response", {}).get("clientToken")
        require(admission.get("httpStatus") == 200 and isinstance(token, str) and str(uuid.UUID(token)) == token, "root admission failed")
        active.append((PATH, token))
        first = wait_partial(probe, fleet, PATH, token, targets, repair_required=True)
        owners = owned_records(records(fleet, token, "root-intent-owner"), token)
        require(set(owners) == {"intent"}, "NOT_OBSERVED: root completed before full-cluster interruption")
        coordinator = owners["intent"]["node"]
        processes = all_processes(fleet, "all-peers-before-restart")
        fleet.frozen_processes[coordinator] = processes[coordinator]
        fleet.record("root-freeze", fleet.remote(coordinator, "process-signal", signal="SIGSTOP", expected=processes[coordinator]))
        _, before_partial = partial_physical(original, fleet.snapshot("root-frozen-physical"), targets)
        before_record = root_intent(records(fleet, token, "root-intent-before-all-stop"), token, coordinator)
        exits = graceful_all(fleet, processes, coordinator)
        after_observed = fleet.record("root-intent-after-all-stop", {node: fleet.remote(node, "task-records", taskId=token, quiescent=True) for node in nodes})
        after_record = root_intent(after_observed, token, coordinator)
        budget = same_owner(before_record, after_record)
        stopped = fleet.snapshot("all-peers-stopped-physical", quiescent=True)
        remaining, after_partial = partial_physical(original, stopped, targets)
        fleet.start()
        restarted = all_processes(fleet, "all-peers-after-restart")
        changed = continuity(processes, restarted)
        deadline = time.monotonic() + TASK_SECONDS
        while True:
            current = recovery_status(probe, fleet, token)
            require(current.get("summary") in {"running", "finished"}, "original root task failed, stopped or disappeared after full restart")
            if current["summary"] == "finished": break
            require(time.monotonic() < deadline, "original root task exceeded its full-restart recovery budget")
            time.sleep(0.05)
        repaired = {(item["bucket"], item["key"], item["versionId"]) for item in remaining}
        proof = responsibility(current, wanted, repaired)
        active.remove((PATH, token))
        idle_fleet(fleet, binding)
        journal = records(fleet, token, "root-terminal-before-repeat")
        durable = terminal_records(journal, token, after_record, current, wanted)
        fleet.stop()
        rebuilt = fleet.snapshot("physical-repaired-before-get", quiescent=True)
        validate_repaired_physical(original, rebuilt, targets[0], repair_targets=targets)
        verify_all_parts(fleet, cohorts, "full-restart-repaired-streaming")
        fleet.start()
        verify_public(probe, fleet, fixtures, pending, "all-peer-s3-after-first-restart")
        before_repeat = all_processes(fleet, "all-peers-before-repeat")
        graceful_all(fleet, before_repeat, coordinator)
        require(physical_content(rebuilt) == physical_content(fleet.snapshot("physical-before-repeat", quiescent=True)), "first recovery GET repaired a previously unproven file")
        fleet.start()
        repeated = all_processes(fleet, "all-peers-after-repeat")
        repeat_processes = continuity(before_repeat, repeated)
        idle_fleet(fleet, binding)
        repeat = recovery_status(probe, fleet, token)
        require(repeat.get("summary") == "finished" and canonical(repeat.get("outcome")) == canonical(current["outcome"]), "repeat full restart revived or changed original responsibility")
        repeated_journal = records(fleet, token, "root-terminal-after-repeat")
        terminal_records(repeated_journal, token, after_record, repeat, wanted)
        require(canonical(journal) == canonical(repeated_journal), "repeat full restart changed completed journal bytes")
        verify_public(probe, fleet, fixtures, pending, "all-peer-s3-after-repeat")
        fleet.stop()
        require(physical_content(rebuilt) == physical_content(fleet.snapshot("physical-after-repeat-get", quiescent=True)), "repeat full restart or verification changed a user file")
        fleet.start()
        complete_pending_uploads(recorder, fleet, binding, pending)
        result["branches"] = {
            BRANCHES[0]: {"verdict": "PASS", "originalTaskId": token, "runningWindow": first["outcome"]["counters"],
                "beforeInterruptionPhysical": before_partial, "afterAllActualExitsPhysical": after_partial,
                "actualPeerExits": exits, "processContinuity": changed, "budget": budget, "responsibility": proof,
                "durableTerminal": durable, "newRootAdmissionIssued": False},
            BRANCHES[1]: {"verdict": "PASS", "exactPendingUploadsAndPartsPreserved": True,
                "pendingPartsCompletedAndFullContentCheckedOnAllPeers": True, "completedMPUAndHistoricalVersionsAndMarkersVerified": True},
            BRANCHES[2]: {"verdict": "PASS", "processContinuity": repeat_processes,
                "completedJournalBytesUnchanged": True, "originalTerminalUnchanged": True, "allUserFilesUnchanged": True}}
        result["verdict"] = "PASS"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            # Fleet.cleanup resumes a frozen original process before stopping it.
            if not fleet.frozen_processes:
                fleet.stop(); fleet.snapshot("physical-failure", quiescent=True)
        except Exception as evidence_error: result["failureEvidenceError"] = str(evidence_error)
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("topology-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"Heal full-restart interrupted by signal {signum}")
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
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()[:4]
    require(len(nodes) == len(set(nodes)) == 4 and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal peers required")
    require(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))
            and re.fullmatch(r"[0-9a-f]{64}", args.binary_sha), "immutable candidate and workflow identity required")
    results = []
    try: results.append(run_topology(args, nodes))
    finally:
        complete = len(results) == 1 and results[0]["verdict"] == "PASS" and results[0]["cleanup"] == "PASS"
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": results[0]["cleanup"] if results else "FAIL", "complete": complete})
    require(complete, "full-cluster restart requires every H2 source branch and clean finalization")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal full-restart validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
