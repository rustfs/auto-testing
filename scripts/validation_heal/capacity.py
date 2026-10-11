#!/usr/bin/env python3
"""I02: actual bounded ENOSPC/inode exhaustion and owned hard-quota recovery."""

import argparse
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
    from .shards import (map_target, run_task, validate_fault, validate_repaired_physical,
                         verify_cohort, pending_inventory, verify_ranges)
    from .versions import validate_version_receipts, verify_quorum_contract
    from .drive_gate import validate_unwritable
    from .stop import idle_fleet
    from .write_quorum import resolve_put
    from .bounded_volume import IMAGE_BYTES, MAX_INODES, MIN_SHARED_FREE
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import (map_target, run_task, validate_fault, validate_repaired_physical,
                        verify_cohort, pending_inventory, verify_ranges)
    from versions import validate_version_receipts, verify_quorum_contract
    from drive_gate import validate_unwritable
    from stop import idle_fleet
    from write_quorum import resolve_put
    from bounded_volume import IMAGE_BYTES, MAX_INODES, MIN_SHARED_FREE


CASE_ID = "heal.enospc-inodes-and-quota"
BRANCHES = ("bounded-volume-enospc", "bounded-inode-exhaustion", "bucket-quota")
SIZE = 5 * 1024**2 + 97
CONTRACT = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "sourceFiles": {"rustfs/src/admin/handlers/quota.rs":
        "6a2b062d3d1c4cd81d934c570d21cb44dcd2c1883f6a85d2265e01b466924f5c",
        "rustfs/src/error.rs": "656d38972c06339015c174405571a94f19d8dd64a680e1a7ca631f80205f99a7"},
    "quota": {"blockedNewLogicalPUT": {"httpStatus": 400, "code": "InvalidRequest",
        "message": "Bucket quota exceeded."}, "limitBytes": 1024**2, "releasedLimitBytes": 256 * 1024**2,
        "existingObjectShardRepair": "does not reserve a new S3 object or increase logical bucket usage"},
    "volume": {"imageBytesPerOriginalDisk": IMAGE_BYTES, "maximumInodesPerImage": MAX_INODES,
        "sharedDriveFreeMarginBytes": MIN_SHARED_FREE, "loopVolumesIncreasePhysicalTopologyCount": False},
    "load": {"DBObjects": 8, "completedMPUObjects": 1, "pendingMPUs": 2,
        "uniqueNegativePUTs": 1, "uniqueReleasedCapacityPUTs": 1, "uncertainRequestRetries": 0, "fillDeadlineSecondsPerDisk": 60}}


def refusal(observed, *, quota):
    require(observed.get("acknowledged") is False and observed.get("clientCallInvoked") is True,
            "capacity-negative PUT succeeded or was never invoked")
    if observed.get("resultUnknown") is True:
        return {"state": "UNKNOWN_PENDING_RESOLUTION", "retryAllowed": False}
    response = observed.get("response") or {}
    error = response.get("Error") or {}
    if quota:
        require(response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 400
                and error.get("Code") == "InvalidRequest" and error.get("Message", "").startswith("Bucket quota exceeded."),
                "quota-negative PUT did not return the reviewed actual hard-quota refusal")
    else:
        require(response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {500, 503}
                and error.get("Code") in {"ServiceUnavailable", "XMinioWriteQuorum", "InternalError"},
                "capacity-negative PUT lacks a reviewed structured storage refusal")
    require(observed.get("resultUnknown") is False, "capacity PUT is neither refused nor explicitly unresolved")
    return {"state": "REFUSED", "code": error["Code"], "retryAllowed": False}


def quota_config(probe, fleet, bucket, limit):
    path = "/rustfs/admin/v3/quota/" + bucket
    body = json.dumps({"quota": limit, "quota_type": "HARD"}).encode()
    configured = fleet.record("owned-bucket-quota-PUT", probe.admin(path, [], body, method="PUT"))
    require(configured.get("httpStatus") == 200 and configured.get("response", {}).get("bucket") == bucket
            and configured["response"].get("quota") == limit and configured["response"].get("quota_type") == "HARD",
            "hard quota was not acknowledged on the exact owned bucket")
    for endpoint in probe.binding["endpoints"]:
        observed = fleet.record("all-peer-owned-quota-GET", Probe(probe.binding, endpoint).admin(path, [], b"", method="GET"))
        require(observed.get("httpStatus") == 200 and observed.get("response", {}).get("bucket") == bucket
                and observed["response"].get("quota") == limit and observed["response"].get("quota_type") == "HARD",
                "peer did not observe the exact acknowledged quota limit")
    return {"bucket": bucket, "limitBytes": limit, "allPeersVerified": True, "sharedFilesystemQuotaChanged": False}


def verify_s3(fleet, binding, bucket, expected, baseline, pending):
    result = {}
    for endpoint in binding["endpoints"]:
        probe = Probe(binding, endpoint)
        inventory = fleet.record("all-peer-final-complete-S3", {"endpoint": endpoint,
            **probe.inventory(bucket, unversioned_current_tags=True)})
        proof = {**verify_seed(expected, inventory), **compare_inventories(baseline, inventory)}
        require(fleet.record("all-peer-pending-MPU", pending_inventory(probe, bucket)) == pending,
                "capacity failure changed an acknowledged pending MPU part or UploadId")
        proof["ranges"] = [verify_ranges(probe, fleet, bucket, entry["key"], expected_body=payload(
            f"{bucket}/{entry['key']}/0", entry["payload"]["size"])) for entry in expected]
        result[endpoint] = proof
    return result


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    bounded = branch != "bucket-quota"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory,
                  runtime_profile="bounded-capacity" if bounded else "manual")
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] == CONTRACT["candidateCommit"], "unreviewed I02 error/quota contract")
    manifest = fleet.record("candidate-bound-inspector", verify_inspector(args.tool_directory, candidate))
    verify_quorum_contract(manifest, candidate)
    fleet.record("registered-capacity-contract", CONTRACT)
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
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
    probe, active, allocated = Probe(binding), [], []
    result = {"topology": topology, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    attempted, negative_body, negative_key = None, None, "capacity-negative/unique.bin"
    released_attempted, released_body, released_key = None, None, "capacity-released/different-nonce.bin"
    quota_set = False
    fill_initiated = False

    def allocate():
        for node in nodes:
            allocated.append(node)
            fleet.record("fixed-owned-volume-allocation", {"node": node, **fleet.remote(
                node, "bounded-volumes-create", branch=branch)})

    def release():
        nonlocal quota_set
        if bounded:
            for node in allocated:
                identity = fleet.remote(node, "process")
                if identity["pid"]:
                    fleet.record("release-only-owned-filler", {"node": node, **fleet.remote(
                        node, "bounded-volumes-release", expected=identity)})
                else:
                    fleet.record("release-only-owned-filler", {"node": node, **fleet.remote(
                        node, "bounded-volumes-release", quiescent=True)})
        elif quota_set:
            quota_config(probe, fleet, bucket, CONTRACT["quota"]["releasedLimitBytes"])
            quota_set = False

    try:
        fleet.deploy(args.binary_sha, bucket, access, secret, before_start=allocate if bounded else None)
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, "DB", 8, [SIZE])
        expected += seed(recorder, bucket, "DM", 1, [SIZE], existing_bucket=True)
        pending = fleet.record("independent-pending-MPU-ACKs", pending_inventory(probe, bucket))
        baseline = fleet.record("complete-S3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("all-fixture-ACKs", {"expected": expected, **verify_seed(expected, baseline)})
        fleet.stop()
        original = fleet.snapshot("original-complete-physical", quiescent=True)
        fleet.record("actual-independent-topology", validate_topology(original, topology))
        targets = [map_target(original, bucket, expected[index]["key"], "data", 1) for index in (0, 8)]
        for target in targets:
            fleet.record("original-target-streaming-proof", verify_cohort(fleet, target, "original-target"))
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "faultId": f"I02-target-{index}"}
            fleet.faults.append((target["node"], params))
            receipt = fleet.record("exact-missing-part", fleet.remote(target["node"], "apply-fault", **params,
                relativePath=target["relativePath"], kind="missing", expectedSha256=target["sha256"]))
            scene = fleet.snapshot("exact-missing-part-witness", quiescent=True)
            prior = original if index == 0 else faulted
            fleet.record("missing-part-proof", validate_fault(prior, scene, target, receipt))
            faulted = scene
        fleet.start()
        require(physical_content(faulted) == physical_content(fleet.snapshot("manual-source-isolation-before-fault")),
                "capacity target changed before the registered capacity fault")
        if bounded:
            fill_initiated = True
            for node in nodes:
                identity = fleet.remote(node, "process")
                fleet.record("actual-resource-exhaustion", {"node": node, **fleet.remote(
                    node, "bounded-volumes-fill", expected=identity)})
        else:
            quota_set = True
            result["quota"] = quota_config(probe, fleet, bucket, CONTRACT["quota"]["limitBytes"])
        negative_body = payload(f"{bucket}/{negative_key}/0", SIZE)
        # This unique call has durable request and ACK files even when the
        # expected refusal causes Recorder to reject it. No retry is issued.
        negative = Recorder(probe, directory / "negative-PUT")
        attempted = {"acknowledged": False, "resultUnknown": True, "clientCallInvoked": True,
                     "reason": "durable request must be resolved if its ACK cannot be read"}
        try:
            negative.call("put_object", {"Bucket": bucket, "Key": negative_key}, negative_body)
        except ValueError:
            pass
        ack = json.loads((negative.output / "000001-ack.json").read_text())
        attempted = ack["observation"]
        result["negativePUT"] = refusal(attempted, quota=not bounded)
        if not bounded:
            require(result["negativePUT"]["state"] == "REFUSED", "quota dependency failed without its exact quota refusal")
        result["negativeTasks"] = []
        for entry in (expected[0], expected[8]):
            current = run_task(probe, fleet, {"bucket": bucket, "key": entry["key"]}, 2, active)
            proof = validate_unwritable(current, bucket, entry) if bounded else validate_version_receipts(
                current, bucket, [entry], 2, required_repairs={(entry["key"], "null")})
            result["negativeTasks"].append(proof)
        if bounded:
            for node in nodes:
                identity = fleet.remote(node, "process")
                fleet.record("resource-still-exhausted-after-Heal", {"node": node, **fleet.remote(
                    node, "bounded-volumes-witness", expected=identity)})
        fleet.stop()
        negative_scene = fleet.snapshot("negative-complete-physical-before-release", quiescent=True)
        if bounded:
            require(physical_content(faulted) == physical_content(negative_scene),
                    "unwritable Heal or PUT changed a healthy source, metadata or the missing target")
        else:
            result["quotaPhysical"] = validate_repaired_physical(original, negative_scene, targets[0], repair_targets=targets)
            for target in targets:
                fleet.record("quota-existing-object-streaming-proof", verify_cohort(fleet, target, "quota-repaired"))
        for node in nodes:
            fleet.record("negative-owned-service-diagnostics", {"node": node, **fleet.remote(node, "logs")})
        if bounded:
            release()
            fleet.start()
        else:
            fleet.start()
            release()
        require(physical_content(negative_scene) == physical_content(fleet.snapshot("capacity-return-before-recovery-Heal")),
                "capacity return altered files before the independent recovery task")
        result["recoveryTasks"] = []
        for entry in (expected[0], expected[8]):
            current = run_task(probe, fleet, {"bucket": bucket, "key": entry["key"]}, 2, active)
            result["recoveryTasks"].append(validate_version_receipts(current, bucket, [entry], 2,
                required_repairs={(entry["key"], "null")} if bounded else ()))
        idle_fleet(fleet, binding)
        fleet.stop()
        repaired = fleet.snapshot("complete-product-repaired-before-S3", quiescent=True)
        result["physical"] = validate_repaired_physical(original, repaired, targets[0], repair_targets=targets)
        for target in targets:
            fleet.record("product-reconstructed-streaming-proof", verify_cohort(fleet, target, "all-repaired-targets"))
        fleet.faults.clear()
        fleet.start()
        reads = [fleet.record("single-negative-PUT-final-resolution", Probe(binding, endpoint).s3("get_object", {
            "Bucket": bucket, "Key": negative_key})) for endpoint in binding["endpoints"]]
        result["putResolution"] = resolve_put(attempted, reads, {"size": len(negative_body),
            "sha256": hashlib.sha256(negative_body).hexdigest()})
        require(result["putResolution"]["state"] == "ABSENT", "capacity-refused unique PUT committed unexpectedly")
        result["s3"] = verify_s3(fleet, binding, bucket, expected, baseline, pending)
        fleet.stop()
        require(physical_content(repaired) == physical_content(fleet.snapshot("physical-after-full-S3", quiescent=True)),
                "S3 verification changed the frozen product-repaired scene")
        fleet.start()
        released_body = payload(f"{bucket}/{released_key}/0", SIZE)
        released_payload = {"size": len(released_body), "sha256": hashlib.sha256(released_body).hexdigest()}
        positive = Recorder(probe, directory / "capacity-released-PUT")
        released_attempted = {"acknowledged": False, "resultUnknown": True, "clientCallInvoked": True}
        try:
            positive.call("put_object", {"Bucket": bucket, "Key": released_key}, released_body)
        finally:
            ack_path = positive.output / "000001-ack.json"
            if ack_path.exists():
                released_attempted = json.loads(ack_path.read_text())["observation"]
        released_reads = []
        for endpoint in binding["endpoints"]:
            observed = fleet.record("released-capacity-success-ACK-GET", Probe(binding, endpoint).s3(
                "get_object", {"Bucket": bucket, "Key": released_key}))
            require(observed.get("acknowledged") is True and observed.get("resultUnknown") is False
                    and observed.get("payload") == released_payload, "released capacity PUT ACK cannot be redeemed")
            released_reads.append(observed)
        result["releasedPUTResolution"] = resolve_put(released_attempted, released_reads, released_payload)
        result["releasedCapacityPUT"] = {"differentUniqueKey": released_key, "allPeersRedeemed": True,
                                          "originalNegativePUTRetried": False}
        result["verdict"] = "PASS"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("physical-original-failure-before-any-rescue", quiescent=True)
        except Exception as evidence_error:
            result["failureEvidenceError"] = str(evidence_error)
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-unresolved-error", {"error": str(exc)})
        recovery_failed = False
        try:
            if fill_initiated or quota_set:
                release()
            if attempted and "putResolution" not in result:
                if fleet.remote(nodes[0], "process")["pid"] == 0:
                    fleet.start()
                reads = [fleet.record("failed-case-PUT-resolution", Probe(binding, endpoint).s3("get_object", {
                    "Bucket": bucket, "Key": negative_key})) for endpoint in binding["endpoints"]]
                result["putResolution"] = resolve_put(attempted, reads, {"size": len(negative_body),
                    "sha256": hashlib.sha256(negative_body).hexdigest()})
            if released_attempted and "releasedPUTResolution" not in result:
                if fleet.remote(nodes[0], "process")["pid"] == 0:
                    fleet.start()
                reads = [fleet.record("failed-case-released-PUT-resolution", Probe(binding, endpoint).s3("get_object", {
                    "Bucket": bucket, "Key": released_key})) for endpoint in binding["endpoints"]]
                result["releasedPUTResolution"] = resolve_put(released_attempted, reads, {"size": len(released_body),
                    "sha256": hashlib.sha256(released_body).hexdigest()})
        except Exception as exc:
            recovery_failed = True
            result["recoveryError"] = str(exc)
        if recovery_failed:
            result["cleanup"] = "FAIL"
            fleet.record("retained-unresolved-capacity-environment", {"state": "DIRTY", "namespacesRetained": True,
                "productRepairClaimed": False, "failureVerdictPreserved": True})
        else:
            result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"I02 workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=[CASE_ID], required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha) and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")),
            "immutable workflow candidate identity required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "I02 source branch omitted")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", n) for n in nodes),
            "four current Heal nodes required")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, selected, branch))
                if results[-1]["cleanup"] != "PASS": break
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == 6 and all(row["verdict"] == "PASS" and row["cleanup"] == "PASS" for row in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": complete,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL"})
    require(complete, "I02 requires all real H1/H2 capacity faults, complete ACK/physical proofs and clean finalization")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"I02 capacity validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
