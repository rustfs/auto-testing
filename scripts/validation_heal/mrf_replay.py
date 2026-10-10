#!/usr/bin/env python3
"""E05: ACK-bound committed MRF responsibility, automatic replay and retirement."""

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
from urllib import request
from urllib.error import URLError
from urllib.parse import quote

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .mrf_records import checkpoint, verify_reference
    from .node_outage import quorum_plan, put_ack, delete_ack, replica_census, validate_deleted_read
    from .scanner_repair import JournalWindow, event_time
    from .shards import files, map_target, verify_cohort
    from .versions import verify_quorum_contract
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from mrf_records import checkpoint, verify_reference
    from node_outage import quorum_plan, put_ack, delete_ack, replica_census, validate_deleted_read
    from scanner_repair import JournalWindow, event_time
    from shards import files, map_target, verify_cohort
    from versions import verify_quorum_contract


CASE_ID = "heal.mrf-offline-write-replay"
BRANCHES = ("offline-put", "offline-version-delete", "offline-marker")
SIZE = 1024**2 + 73
PERSIST_SECONDS = 30
REPLAY_SECONDS = 240


def identity(item):
    return item["bucket"], item["key"], item["versionId"], item["poolIndex"], item["setIndex"], item["kind"]


def acknowledge(recorder, bucket, branch, expected):
    manifest, obligations, deleted = copy.deepcopy(expected), [], []
    for index in range(3):
        if branch == "offline-put":
            key = f"offline/new-{index:02d}.bin"
            body = payload(f"{bucket}/{key}/offline", SIZE + index)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "Metadata": {"heal-fixture": "durable-MRF", "branch": branch}, "Tagging": "suite=heal&fixture=DV"}, body)
            manifest = put_ack(manifest, key, body, response, "DV")
            version = response["VersionId"]
            absent = False
        elif branch == "offline-version-delete":
            entry = next(entry for entry in expected if entry["key"] == f"objects/{index + 1:04d}.bin"
                         and not entry["isLatest"] and not entry["deleteMarker"])
            key, version = entry["key"], entry["versionId"]
            response = recorder.call("delete_object", {"Bucket": bucket, "Key": key, "VersionId": version})
            manifest = delete_ack(manifest, key, response, "DV", version=version)
            deleted.append(entry)
            absent = True
        else:
            require(branch == "offline-marker", "unknown durable MRF branch")
            key = f"objects/{index + 1:04d}.bin"
            response = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
            manifest = delete_ack(manifest, key, response, "DV")
            version, absent = response["VersionId"], False
        require(response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {200, 204}, "MRF workload lacks an explicit commit ACK")
        obligations.append({"bucket": bucket, "key": key, "versionId": version, "poolIndex": 0,
                            "setIndex": 0, "kind": 3, "authoritativelyAbsentRequired": absent})
    require(len({identity(item) for item in obligations}) == 3, "MRF workload reused an ACK identity")
    return manifest, obligations, deleted


def pending_bindings(checkpoints, obligations, earliest_ms):
    bindings = {}
    for obligation in obligations:
        wanted, owners = identity(obligation), []
        for node, value in checkpoints.items():
            matched = [intent for intent in value["intents"] if identity(intent) == wanted]
            require(len(matched) <= 1, "MRF committed snapshot duplicated the same ACK responsibility")
            if not matched: continue
            intent = matched[0]
            lifecycle = intent.get("lifecycle", {})
            require(type(intent["enqueuedAtMs"]) is int and earliest_ms <= intent["enqueuedAtMs"] <= int(time.time() * 1000) + 5000
                    and lifecycle.get("source_bucket_incarnation_id")
                    and lifecycle.get("state") == {"state": "active"}
                    and lifecycle.get("last_operator_acceptance") is None,
                    "MRF responsibility is stale, held, unverified or accepted by an operator")
            owners.append({"node": node, "checkpointOwner": value["owner"], "sequence": value["sequence"],
                "responsibilityId": lifecycle["responsibility_id"],
                "bucketIncarnationId": lifecycle["source_bucket_incarnation_id"], "intentDigest": intent["intentDigest"]})
        require(bool(owners), "NOT_OBSERVED: acknowledged offline identity has no committed durable MRF responsibility")
        require(len({owner["bucketIncarnationId"] for owner in owners}) == 1, "MRF owners disagree on the original bucket generation")
        bindings[wanted] = owners
    return bindings


def retirement(checkpoints, obligations, bindings):
    for obligation in obligations:
        wanted = identity(obligation)
        for owner in bindings[wanted]:
            value = checkpoints[owner["node"]]
            if value["present"]:
                require(value["owner"] == owner["checkpointOwner"] and value["sequence"] >= owner["sequence"],
                        "MRF retirement changed owner or rolled back the committed sequence")
                require(not any(identity(intent) == wanted or intent.get("lifecycle", {}).get("responsibility_id")
                            == owner["responsibilityId"] for intent in value["intents"]),
                        "NOT_OBSERVED: exact MRF ACK responsibility has not retired")
            else:
                require(value.get("allAllowlistedFilesAbsent") is True, "MRF retirement has an unknown record state")
        require(not any(identity(intent) == wanted for value in checkpoints.values() for intent in value["intents"]),
                "MRF ACK responsibility migrated to another owner instead of retiring")
    return {"allExactACKResponsibilitiesRetired": True, "queueDepthUsedAsProof": False,
            "operatorAcceptanceUsed": False, "responsibilityCount": len(obligations)}


def source_tasks(events, obligation, owners, after):
    tasks = []
    for node in {owner["node"] for owner in owners}:
        rows = events.get(node, [])
        for start in rows:
            stamp, token = event_time(start), start.get("task_id")
            if not (start.get("target") == "rustfs::heal::task" and start.get("message") == "Heal object started"
                    and start.get("stage") == "start" and start.get("bucket") == obligation["bucket"]
                    and start.get("object") == obligation["key"] and start.get("version_id") == obligation["versionId"]
                    and isinstance(token, str) and token and stamp is not None and stamp >= after):
                continue
            admission = next((event for event in rows if event.get("target") == "rustfs::heal::manager"
                and event.get("message") == "Heal queue request accepted" and event.get("request_id") == token
                and event.get("source") == "mrf" and event.get("result") == "accepted"
                and event_time(event) is not None and after <= event_time(event) <= stamp), None)
            completed = next((event for event in rows if event.get("target") == "rustfs::heal::manager"
                and event.get("state") == "task_completed" and event.get("task_id") == token
                and event_time(event) is not None and event_time(event) >= stamp), None)
            if admission is not None and completed is not None:
                tasks.append({"node": node, "taskId": token, "admission": admission, "started": start, "completed": completed})
    return tasks


def verified_receipt(status, obligation, owners):
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "finished" and status.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed" and outcome.get("coverage") == "complete"
            and outcome.get("objectsTruncated") is False, "MRF task cannot discharge incomplete or failed responsibility")
    objects = outcome.get("objects")
    require(isinstance(objects, list) and len(objects) == 1, "MRF task did not preserve the exact ACK version scope")
    receipt = objects[0]
    item = receipt.get("identity", {})
    incarnation = {owner["bucketIncarnationId"] for owner in owners}
    require(item.get("kind") == "object" and item.get("bucket") == obligation["bucket"]
            and item.get("object") == obligation["key"] and item.get("versionId") == obligation["versionId"]
            and type(item.get("poolIndex")) is int and item["poolIndex"] == obligation["poolIndex"]
            and type(item.get("setIndex")) is int and item["setIndex"] == obligation["setIndex"]
            and item.get("bucketIncarnationId") in incarnation, "MRF proof belongs to another ACK, set or bucket generation")
    state = receipt.get("disposition", {}).get("state")
    allowed = {"authoritatively_absent"} if obligation["authoritativelyAbsentRequired"] else {"repaired", "verified_healthy"}
    require(state in allowed, "MRF retirement has no storage-verified disposition for this exact responsibility")
    counters = outcome.get("counters", {})
    expected = {"processed": 1, "healed": int(state == "repaired"), "unchanged": int(state != "repaired"),
                "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0}
    require(all(type(counters.get(name)) is int and counters[name] == count for name, count in expected.items())
            and counters.get("overflowed") is False, "MRF proof counters hide unknown, failed or overflowed responsibility")
    return {"identity": item, "disposition": state, "completeSuccessfulTask": True}


def read_checkpoints(fleet, nodes, label, *, quiescent=False):
    observed = {}
    for node in nodes:
        raw = fleet.record(label + "-raw", {"node": node, **fleet.remote(node, "mrf-records", quiescent=quiescent)})
        observed[node] = checkpoint(raw)
    return fleet.record(label + "-decoded", observed)


def deleted_parts_absent(snapshot, targets):
    indexed = files(snapshot)
    for target in targets:
        for shard in target["cohort"]:
            require((shard["node"], shard["disk"], shard["relativePath"]) not in indexed,
                    "MRF retired an explicit deletion with its original data shard still present")


def wait_replay(fleet, window, binding, nodes, bucket, expected, obligations, bindings, rejoin_time,
                before, authorities, deleted_targets):
    deadline, proofs = time.monotonic() + REPLAY_SECONDS, {}
    while True:
        events = window.capture()
        for obligation in obligations:
            key = identity(obligation)
            if key in proofs: continue
            for task in source_tasks(events, obligation, bindings[key], rejoin_time):
                endpoint = "node" + str(nodes.index(task["node"]) + 1)
                path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + quote(obligation["key"], safe="/")
                observed = fleet.record("MRF-exact-task-status", {"source": task,
                    **Probe(binding, endpoint).admin(path, [("clientToken", task["taskId"])], b"")})
                require(observed.get("httpStatus") == 200, "MRF source task status is unavailable")
                try:
                    proof = verified_receipt(observed.get("response", {}), obligation, bindings[key])
                except ValueError as exc:
                    fleet.record("MRF-undischarged-task-result", {"task": task, "error": str(exc)})
                    continue
                proofs[key] = {"source": task, "proof": proof}
                break
        scene = fleet.snapshot("MRF-replay-physical-progress")
        try:
            require({node: value["formats"] for node, value in scene.items()} == {node: value["formats"] for node, value in before.items()},
                    "MRF rejoin changed an original disk UUID or slot")
            replica_census(scene, bucket, expected, authorities)
            deleted_parts_absent(scene, deleted_targets)
            require(set(proofs) == set(bindings), "NOT_OBSERVED: MRF replay lacks an exact verified source task for every ACK")
            retired = retirement(read_checkpoints(fleet, nodes, "MRF-retirement"), obligations, bindings)
            return proofs, retired
        except ValueError as exc:
            fleet.record("MRF-replay-incomplete", {"error": str(exc), "verifiedACKs": len(proofs)})
            require(time.monotonic() < deadline, "MRF automatic replay deadline exceeded: " + str(exc))
            time.sleep(2)


def run_branch(args, branch, nodes):
    directory = args.evidence / branch
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h2"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="durable-mrf")
    candidate = json.loads(args.candidate.read_text())
    tool = fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    contract = verify_quorum_contract(tool, candidate)
    fleet.record("candidate-MRF-record-and-proof-contract", verify_reference(candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, 5)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 128, "maxInventoryBytes": 128 * 1024**2}
    fleet.record("binding", binding)
    probe = Probe(binding)
    result = {"topology": "H2", "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, "DV", 4, [SIZE])
        baseline = fleet.record("s3-baseline", probe.inventory(bucket))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        before = fleet.snapshot("physical-baseline")
        fleet.record("topology-proof", validate_topology(before, "H2"))
        initial_records = read_checkpoints(fleet, nodes, "MRF-baseline")
        require(all(not value["intents"] for value in initial_records.values()), "fresh MRF fixture has preexisting responsibilities")
        target = map_target(before, bucket, "objects/0001.bin", "data", 1,
            next(entry["versionId"] for entry in expected if entry["key"] == "objects/0001.bin" and entry["isLatest"]))
        offline_node, online_nodes = nodes[-1], nodes[:-1]
        fleet.record("offline-quorum-plan", quorum_plan(target, offline_node, contract))
        stopped = fleet.record("offline-original-node", fleet.remote(offline_node, "stop"))
        require(stopped.get("quiescent") is True and stopped.get("service", {}).get("MainPID") == "0", "MRF target did not actually stop")
        try:
            with request.build_opener(request.ProxyHandler({})).open(binding["endpoints"]["node4"] + "/health/ready", timeout=3):
                raise ValueError("MRF offline endpoint still accepts requests")
        except URLError as exc:
            require(isinstance(exc.reason, ConnectionRefusedError), "MRF target outage has no exact connection refusal")
            fleet.record("offline-refusal", {"node": offline_node, "connectionRefused": True})
        frozen = fleet.record("original-offline-scene", fleet.remote(offline_node, "snapshot", quiescent=True))
        earliest_ms = int(time.time() * 1000) - 5000
        expected, obligations, deleted = acknowledge(recorder, bucket, branch, expected)
        fleet.record("offline-ACK-responsibility-manifest", {"expected": expected, "obligations": obligations, "deleted": deleted})
        acknowledged = fleet.record("offline-S3-ACK-proof", probe.inventory(bucket))
        fleet.record("offline-S3-full-manifest", verify_seed(expected, acknowledged))
        deadline = time.monotonic() + PERSIST_SECONDS
        while True:
            try:
                checkpoints = read_checkpoints(fleet, online_nodes, "offline-committed-MRF")
                bindings = pending_bindings(checkpoints, obligations, earliest_ms)
                break
            except ValueError as exc:
                if not str(exc).startswith("NOT_OBSERVED:"): raise
                require(time.monotonic() < deadline, "MRF persistence deadline exceeded: " + str(exc))
                time.sleep(1)
        fleet.record("durable-ACK-binding", [{"identity": list(key), "owners": value} for key, value in bindings.items()])
        still_offline = fleet.record("offline-after-ACKs", fleet.remote(offline_node, "snapshot", quiescent=True))
        require(physical_content({offline_node: frozen}) == physical_content({offline_node: still_offline}), "MRF ACKs reached the stopped original node")
        authorities = fleet.record("offline-physical-authority", {node: fleet.remote(node, "snapshot", quiescent=node == offline_node) for node in nodes})
        deleted_targets = [map_target(before, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False) for entry in deleted]
        window = JournalWindow(fleet, "MRF-rejoin")
        rejoin_time = datetime.now(timezone.utc)
        fleet.record("rejoin-original-node", fleet.remote(offline_node, "start"))
        fleet.wait_ready()
        fleet.record("registered-automatic-replay-budget", {"persistenceSeconds": PERSIST_SECONDS,
            "replaySeconds": REPLAY_SECONDS, "adminMutations": 0, "targetReadsBeforePhysicalProof": 0,
            "source": "MRF", "originalNodeAndDiskIdentitiesRequired": True})
        proofs, retired = wait_replay(fleet, window, binding, nodes, bucket, expected, obligations, bindings,
                                     rejoin_time, before, authorities, deleted_targets)
        fleet.record("MRF-verified-responsibility-retirement", {"retirement": retired,
            "proofs": [{"identity": list(key), **value} for key, value in proofs.items()]})
        retirement(read_checkpoints(fleet, nodes, "MRF-repeat-retirement"), obligations, bindings)
        fleet.stop()
        repaired = fleet.snapshot("MRF-quiescent-repaired", quiescent=True)
        targets = replica_census(repaired, bucket, expected, authorities)
        deleted_parts_absent(repaired, deleted_targets)
        streaming = [verify_cohort(fleet, target, "MRF-streaming-proof") for target in targets]
        retirement(read_checkpoints(fleet, nodes, "MRF-quiescent-retirement", quiescent=True), obligations, bindings)
        fleet.start()
        for endpoint in binding["endpoints"]:
            peer = Probe(binding, endpoint)
            inventory = fleet.record("MRF-all-peer-S3", {"endpoint": endpoint, **peer.inventory(bucket)})
            fleet.record("MRF-all-peer-manifest", {**verify_seed(expected, inventory), **compare_inventories(acknowledged, inventory)})
            for entry in deleted:
                validate_deleted_read(fleet.record("MRF-deleted-version-refusal", peer.s3("get_object",
                    {"Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]})), entry["versionId"])
        fleet.stop()
        require(physical_content(repaired) == physical_content(fleet.snapshot("MRF-after-verification", quiescent=True)),
                "S3 verification changed the physical MRF result")
        result.update(verdict="PASS", durableACKs=len(obligations), sourceTasks=len(proofs), retirement=retired,
            streamingVersions=len(streaming), allPeerS3ManifestEqual=True, adminMutationCount=0, backupRestorationUsed=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try:
                fleet.stop()
                fleet.snapshot("MRF-failure-physical", quiescent=True)
                read_checkpoints(fleet, nodes, "MRF-failure-records", quiescent=True)
            except Exception as evidence_error:
                result["failureEvidenceError"] = str(evidence_error)
    finally:
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
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
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "four current Heal nodes required")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)), "invalid candidate binary digest")
    require(bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "workflow fleet identity required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "MRF executor omitted a source branch")
    results = []
    try:
        for branch in BRANCHES:
            results.append(run_branch(args, branch, nodes[:4]))
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == len(BRANCHES) and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "E05 requires all three H2 durable MRF branches and complete cleanup")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal durable MRF validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
