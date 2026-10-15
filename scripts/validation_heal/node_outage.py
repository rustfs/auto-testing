#!/usr/bin/env python3
"""B09: acknowledged degraded I/O and physical MRF repair on the current H2 fleet."""

import argparse
import copy
from datetime import UTC, datetime, timedelta
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

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .shards import SIZES, files, map_target, run_task
    from .versions import public_version_id, validate_version_receipts, verify_quorum_contract
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import SIZES, files, map_target, run_task
    from versions import public_version_id, validate_version_receipts, verify_quorum_contract


BRANCHES = {"one-node-stop": "DB", "rejoin-with-new-versions": "DV"}
SIZE = 1024**2 + 47


def quorum_plan(target, offline_node, contract):
    require(contract.get("recognized") is True and contract.get("writeQuorumRule") == "data-plus-one-if-equal-parity",
            "node outage requires recognized candidate read/write quorum policies")
    erasure = target["logical"]["erasure"]
    data, parity = erasure["data_blocks"], erasure["parity_blocks"]
    lost = [shard for shard in target["cohort"] if shard["node"] == offline_node]
    remaining = data + parity - len(lost)
    write = data + int(data == parity)
    require(len(lost) == 4 and len(target["cohort"]) == 16 and data + parity == 16
            and len({shard["node"] for shard in target["cohort"]}) == 4
            and remaining >= max(data, write), "selected node outage cannot admit the degraded read/write workload")
    return {"offlineNode": offline_node, "offlineDiskUUIDs": [shard["diskUUID"] for shard in lost],
            "lostSameSetDrives": len(lost), "remainingSameSetDrives": remaining,
            "readQuorum": data, "writeQuorum": write, "policy": contract["writeQuorumRule"],
            "distributedLockAdmission": "each successful write ACK also proves its actual lock admission"}


def put_ack(expected, key, body, response, fixture):
    version = response.get("VersionId", "null")
    require(fixture != "DV" or version not in {None, "null", ""}, "degraded versioned PUT omitted its version identity")
    require(fixture != "DB" or version in {None, "null"}, "never-versioned PUT unexpectedly created a version")
    version = public_version_id(version)
    result = copy.deepcopy(expected)
    if fixture == "DB": result = [entry for entry in result if entry["key"] != key]
    else:
        require(not any(entry["key"] == key and entry["versionId"] == version for entry in result),
                "degraded PUT reused an acknowledged version")
        for entry in result:
            if entry["key"] == key: entry["isLatest"] = False
    result.append({"key": key, "versionId": version, "deleteMarker": False, "isLatest": True,
                   "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
    return result


def delete_ack(expected, key, response, fixture, *, version=None):
    result = copy.deepcopy(expected)
    if version is not None:
        require(response.get("VersionId") == version and response.get("DeleteMarker") in {None, False},
                "explicit data-version DELETE did not acknowledge the exact identity")
        require(any(entry["key"] == key and entry["versionId"] == version and not entry["isLatest"] for entry in result),
                "historical delete would change the expected current identity")
        return [entry for entry in result if (entry["key"], entry["versionId"]) != (key, version)]
    if fixture == "DB":
        require(response.get("DeleteMarker") in {None, False} and response.get("VersionId") in {None, "null"},
                "never-versioned DELETE returned a marker or foreign version")
        return [entry for entry in result if entry["key"] != key]
    marker = response.get("VersionId")
    require(response.get("DeleteMarker") is True and marker not in {None, "", "null"}
            and not any(entry["key"] == key and entry["versionId"] == marker for entry in result),
            "degraded marker DELETE omitted or reused its acknowledged identity")
    for entry in result:
        if entry["key"] == key: entry["isLatest"] = False
    result.append({"key": key, "versionId": marker, "deleteMarker": True, "isLatest": True})
    return result


def workload(recorder, bucket, expected, fixture):
    manifest = copy.deepcopy(expected)
    for index in range(6):
        key = f"offline/new-{index:02d}.bin"
        body = payload(f"{bucket}/{key}/offline", SIZE + index)
        response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
            "Metadata": {"heal-fixture": "offline-ack"}, "Tagging": "suite=heal&fixture=offline"}, body)
        manifest = put_ack(manifest, key, body, response, fixture)
    for index in (1, 3):
        key = f"objects/{index:04d}.bin"
        body = payload(f"{bucket}/{key}/offline-overwrite", SIZE + index)
        response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
            "Metadata": {"heal-fixture": "offline-overwrite"}, "Tagging": "suite=heal&fixture=offline"}, body)
        manifest = put_ack(manifest, key, body, response, fixture)
    key = "objects/0002.bin" if fixture == "DV" else "objects/0006.bin"
    response = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
    manifest = delete_ack(manifest, key, response, fixture)
    if fixture == "DV":
        old = next(entry for entry in expected if entry["key"] == "objects/0001.bin" and not entry["isLatest"])
        response = recorder.call("delete_object", {"Bucket": bucket, "Key": old["key"], "VersionId": old["versionId"]})
        manifest = delete_ack(manifest, old["key"], response, fixture, version=old["versionId"])
    for entry in [entry for entry in manifest if not entry["deleteMarker"]][:6]:
        observed = recorder.probe.s3("get_object", {"Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]})
        # Archive failures as well as complete bodies' hashes before checking.
        save(recorder.output / ("offline-get-" + hashlib.sha256((entry["key"] + entry["versionId"]).encode()).hexdigest()[:20] + ".json"), observed)
        require(observed.get("acknowledged") and observed.get("payload") == entry["payload"],
                "degraded historical GET differs from acknowledged bytes")
        recorder.call("head_object", {"Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]})
    return manifest


def replica_census(snapshot, bucket, expected, offline_snapshot, *, topology="H2"):
    validate_topology(snapshot, topology)
    original = files(offline_snapshot)
    wanted = {}
    for entry in expected:
        wanted.setdefault(entry["key"], set()).add((entry["versionId"], entry["deleteMarker"], entry["isLatest"]))
    for node, value in snapshot.items():
        for disk in range(4):
            observed = {}
            for item in value["files"]:
                if item["disk"] != disk: continue
                metadata = item.get("metadata")
                if metadata:
                    require(metadata.get("bucket") == bucket, "metadata escaped the run bucket")
                    versions = {(public_version_id(version.get("versionId")), version["deleted"], version["isLatest"])
                                for version in metadata["versions"]}
                    if versions: observed[metadata["key"]] = versions
            require(observed == wanted, "rejoined replica lacks or resurrected an acknowledged version/marker")
    targets = []
    for entry in expected:
        if entry["deleteMarker"]: continue
        target = map_target(snapshot, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False)
        for shard in target["cohort"]:
            previous = original.get((shard["node"], shard["disk"], shard["relativePath"]))
            require(previous is None or (shard["sha256"] == previous["sha256"] and shard["size"] == previous["size"]),
                    "rejoin changed an authoritative surviving shard")
        targets.append(target)
    return targets


def validate_deleted_read(observed, version):
    response = observed.get("response") or {}
    require(observed.get("acknowledged") is False and observed.get("resultUnknown") is False and "payload" not in observed
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
            and response.get("Error", {}).get("Code") == ("NoSuchKey" if version == "null" else "NoSuchVersion"),
            "rejoined endpoint returned a deleted identity or an unrecognized refusal")


def run_branch(args, branch, nodes):
    fixture = BRANCHES[branch]
    directory = args.evidence / branch
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h2"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="offline-replay")
    candidate = json.loads(args.candidate.read_text())
    tool = fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    contract = verify_quorum_contract(tool, candidate)
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, 5)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 512, "maxInventoryBytes": 512 * 1024**2}
    fleet.record("binding", binding)
    probe, active = Probe(binding), []
    result = {"topology": "H2", "branch": branch, "fixture": fixture, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        require(contract.get("writeQuorumRule") == "data-plus-one-if-equal-parity",
                "node outage needs a fresh recognized candidate write-quorum contract")
        fleet.deploy(args.binary_sha, bucket, access, secret)
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, fixture, 128 if fixture == "DB" else 4, [SIZE])
        initial = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, initial)})
        before = fleet.snapshot("physical-baseline")
        offline_node = nodes[-1]
        target = map_target(before, bucket, "objects/0001.bin", "data", 1,
                            next(entry["versionId"] for entry in expected if entry["key"] == "objects/0001.bin" and entry["isLatest"]))
        fleet.record("quorum-fault-plan", quorum_plan(target, offline_node, contract))
        stop = fleet.record("node-stopped", fleet.remote(offline_node, "stop"))
        require(stop.get("quiescent") is True and stop.get("service", {}).get("MainPID") == "0", "node stop lacks process exit")
        opener = request.build_opener(request.ProxyHandler({}))
        try:
            with opener.open(binding["endpoints"]["node4"] + "/health/ready", timeout=3):
                raise ValueError("stopped node endpoint still accepts requests")
        except URLError as exc:
            require(isinstance(exc.reason, ConnectionRefusedError), "node outage was not a witnessed connection refusal")
            fleet.record("node-endpoint-refused", {"endpoint": "node4", "connectionRefused": True})
        frozen_offline = fleet.record("offline-node-baseline", fleet.remote(offline_node, "snapshot", quiescent=True))
        expected = workload(recorder, bucket, expected, fixture)
        acknowledged = fleet.record("s3-degraded-ack-manifest", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        fleet.record("degraded-full-proof", {"expected": expected, **verify_seed(expected, acknowledged)})
        previous = {(entry["key"], entry["versionId"]): entry for entry in initial["entries"]}
        retained = {(entry["key"], entry["versionId"]) for entry in acknowledged["entries"]}
        for entry in acknowledged["entries"]:
            old = previous.get((entry["key"], entry["versionId"]))
            if old and not entry["deleteMarker"] and old.get("payload") == entry.get("payload"):
                require(old["properties"] == entry["properties"] and old["tags"] == entry["tags"],
                        "degraded I/O changed an untouched version's public metadata or tags")
        still_offline = fleet.record("offline-node-after-acks", fleet.remote(offline_node, "snapshot", quiescent=True))
        require(physical_content({offline_node: frozen_offline}) == physical_content({offline_node: still_offline}),
                "offline node received writes before it rejoined")
        authorities = fleet.record("physical-offline-authorities", {node: fleet.remote(node, "snapshot", quiescent=node == offline_node) for node in nodes})
        fleet.record("node-rejoined", fleet.remote(offline_node, "start"))
        fleet.wait_ready()
        fleet.record("trigger", {"source": "MRF", "manualTasksBeforePhysicalRepair": False,
                                  "targetS3ReadsBeforePhysicalRepair": False, "budgetSeconds": 180})
        deadline, last_error = time.monotonic() + 180, None
        while True:
            scene = fleet.snapshot("physical-mrf-progress")
            try:
                targets = replica_census(scene, bucket, expected, authorities)
                break
            except ValueError as exc:
                last_error = str(exc)
            require(time.monotonic() < deadline, "MRF physical rejoin deadline exceeded: " + last_error)
            time.sleep(2)
        fleet.stop()
        repaired = fleet.snapshot("physical-repaired-quiescent", quiescent=True)
        targets = replica_census(repaired, bucket, expected, authorities)
        proofs = []
        for target in targets:
            for shard in target["cohort"]:
                observed = fleet.record("rejoined-streaming-bitrot", fleet.remote(shard["node"], "verify-part",
                    disk=shard["disk"], relativePath=shard["relativePath"], versionId=shard["versionId"], partNumber=1))
                require(observed.get("physicalBitrotVerified") is True and observed["sha256"] == shard["sha256"]
                        and observed["physicalSize"] == shard["size"], "rejoined shard failed its exact committed checksum")
                proofs.append({"node": shard["node"], "disk": shard["disk"], "key": target["key"], "versionId": shard["versionId"]})
        fleet.start()
        status = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active, option_overrides={"recursive": True})
        responsibility = validate_version_receipts(status, bucket, expected, 2)
        fleet.stop()
        final = fleet.snapshot("physical-after-verifier", quiescent=True)
        require(physical_content(repaired) == physical_content(final), "verification task unexpectedly repaired or changed the MRF result")
        fleet.start()
        for endpoint in binding["endpoints"]:
            entry_probe = Probe(binding, endpoint)
            current = fleet.record("s3-rejoined-" + endpoint, entry_probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
            compare_inventories(acknowledged, current)
            for key, version in set(previous) - retained:
                params = {"Bucket": bucket, "Key": key}
                if version != "null": params["VersionId"] = version
                observed = fleet.record("deleted-identity-read-" + endpoint, entry_probe.s3("get_object", params))
                validate_deleted_read(observed, version)
        result.update(verdict="PASS", allNodeS3ManifestEqual=True, responsibility=responsibility,
                      physical={"allExpectedReplicaIdentities": True, "survivingShardsUnchanged": True,
                                "newTargetShardsVerified": True, "streamingShardProofs": len(proofs),
                                "backupRestorationUsed": False, "source": "MRF"})
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try:
                fleet.stop()
                fleet.snapshot("physical-failure-scene", quiescent=True)
            except Exception as capture:
                fleet.record("failure-scene-error", {"message": str(capture)})
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-error", {"message": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("topology-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"node-outage workflow signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"): parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=("heal.node-outage-rejoin",), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) == 4 and len(set(nodes)) == 4 and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four existing Heal nodes required")
    require(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha) and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "fixed workflow/candidate identities required")
    require(list(BRANCHES) == select(args.case_id)[0]["variants"], "node-outage executor omitted a source branch")
    results = []
    try:
        for branch in BRANCHES:
            results.append(run_branch(args, branch, nodes))
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == len(BRANCHES) and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": args.case_id, "results": results,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "B09 requires both complete H2 branches; inspect topology-result evidence")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal node outage validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
