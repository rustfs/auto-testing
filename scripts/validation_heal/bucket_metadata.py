#!/usr/bin/env python3
"""D11: bucket-config recovery and unknown-envelope refusal on H1/H2."""

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
import time
from urllib.parse import quote

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .bucket_records import bucket_path
    from .versions import validate_version_receipts
    from .stop import idle_fleet
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from bucket_records import bucket_path
    from versions import validate_version_receipts
    from stop import idle_fleet


CASE_ID = "heal.bucket-metadata-integrity"
BRANCHES = ("versioning-config", "bucket-policy-tags", "lock-config", "unknown-record-negative")
REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926", "sourceFiles": {
    "crates/ecstore/src/bucket/metadata.rs": "3b4f9ed4dba259f259653ba4bf1568c54a10b71ff0f6111135394e225a38d990",
    "crates/filemeta/src/filemeta/codec.rs": "ef82b3390b0a377e423f99fb384c6a8a96beaf5722b0c907852cc0c904c4f04d"},
    "physicalRecord": ".rustfs.sys/buckets/<owned-bucket>/.metadata.bin/xl.meta",
    "representation": "system object XL2 envelope, not a raw JSON or raw inner bucket-msgpack file",
    "unknownNegative": "outer XL2 major 1 -> 2; all other bytes retained; decoder rejects before checksum",
    "repairSourcesAllowed": ["candidate startup/config read", "explicit bucket Heal"],
    "credentialBoundary": "fresh owned service; IAM subtree must remain absent; never enumerated"}
GETTERS = ("get_bucket_versioning", "get_bucket_policy", "get_bucket_tagging", "get_object_lock_configuration")


def seed_fixture(recorder, buckets):
    expected = {}
    for bucket in buckets:
        recorder.call("create_bucket", {"Bucket": bucket, "ObjectLockEnabledForBucket": True})
        recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
        expected[bucket] = []
        for key in ("objects/target.bin", "objects/control.bin"):
            for nonce in range(2):
                body = payload(f"D11/{bucket}/{key}/{nonce}", 1024**2 + 97)
                response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                    "Metadata": {"suite": "D11", "nonce": str(nonce)}, "Tagging": "suite=heal&fixture=DS"}, body)
                version = response.get("VersionId")
                require(isinstance(version, str) and version not in {"", "null"}, "bucket fixture lacks a version ACK")
                expected[bucket].append({"key": key, "versionId": version, "isLatest": nonce == 1, "deleteMarker": False,
                    "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
        document = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*",
            "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bucket}/objects/*"}]}
        recorder.call("put_bucket_policy", {"Bucket": bucket, "Policy": json.dumps(document)})
        recorder.call("put_bucket_tagging", {"Bucket": bucket, "Tagging": {"TagSet": [
            {"Key": "suite", "Value": "D11"}, {"Key": "bucket", "Value": bucket}]}})
        # Configure defaults AFTER writes. The historical fixture has no
        # retained objects, so namespace finalization cannot bypass retention.
        recorder.call("put_object_lock_configuration", {"Bucket": bucket, "ObjectLockConfiguration": {
            "ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}}}})
    return expected


def configuration(observed, operation):
    require(observed.get("acknowledged") is True and observed.get("resultUnknown") is False,
            "bucket configuration read failed or has an unknown outcome")
    response = {key: value for key, value in observed["response"].items() if key != "ResponseMetadata"}
    if operation == "get_bucket_policy":
        response["Policy"] = json.loads(response["Policy"])
    if operation == "get_bucket_tagging":
        response["TagSet"] = sorted(response["TagSet"], key=lambda tag: (tag["Key"], tag["Value"]))
    require(bool(response), "bucket configuration silently became an empty default")
    return response


def configs(probe, fleet, bucket, label):
    return {op: configuration(fleet.record(label, {"bucket": bucket, **probe.s3(op, {"Bucket": bucket})}), op)
            for op in GETTERS}


def system_snapshot(fleet, buckets, label, faults=None):
    return fleet.record(label, {node: fleet.remote(node, "bucket-record-snapshot", buckets=buckets,
        metadataFaults=(faults or {}).get(node, {})) for node in fleet.nodes})


def system_files(value):
    result = {(node, row["disk"], row["path"]): {name: row[name] for name in ("size", "sha256")}
              for node, sample in value.items() for row in sample["files"]}
    require(len(result) == sum(len(sample["files"]) for sample in value.values()), "duplicate system file identity")
    require(all(sample["credentialsRead"] is False and all(row["present"] is False and row["traversed"] is False
        for row in sample["iamNamespace"]) for sample in value.values()), "credential boundary changed")
    return result


def record_plan(snapshot, bucket, topology, physical):
    path = bucket_path(bucket, bucket) + "/xl.meta"
    targets = [(node, row) for node, sample in snapshot.items() for row in sample["files"] if row["path"] == path]
    require(len(targets) == (4 if topology == "H1" else 16), "bucket config lacks every original member")
    geometry, indices, latest = None, set(), None
    for node, row in targets:
        metadata = row.get("metadata") or {}
        require(metadata.get("bucket") == ".rustfs.sys" and metadata.get("key") == f"buckets/{bucket}/.metadata.bin",
                "system object decoder identity differs")
        versions = metadata.get("versions", [])
        require(len(versions) == 1 and versions[0].get("deleted") is False and versions[0].get("isLatest") is True,
                "bucket record must have one authoritative live version")
        version = versions[0]
        erasure = version["erasure"]
        current = {key: erasure[key] for key in ("data_blocks", "parity_blocks", "distribution", "block_size", "algorithm")}
        require(geometry is None or current == geometry, "bucket config lacks one erasure authority")
        geometry = current
        layouts = [record["value"]["xl"] for record in physical[node]["formats"] if record["disk"] == row["disk"]]
        require(len(layouts) == 1 and len(layouts[0]["sets"]) == 1,
                "system object member lacks its actual disk/set format")
        layout = layouts[0]
        require(erasure["index"] == erasure["distribution"][layout["sets"][0].index(layout["this"])],
                "system shard index differs from its physical disk UUID/set slot")
        require(erasure["index"] not in indices and version.get("size", 0) > 4, "duplicate or empty config shard")
        indices.add(erasure["index"])
        identity = version.get("versionId"), version.get("dataDir"), version["size"], version["parts"]
        require(latest is None or identity == latest, "bucket-config version/dataDir differs across members")
        latest = identity
    require(geometry["data_blocks"] + geometry["parity_blocks"] == len(targets)
            and indices == set(range(1, len(targets) + 1)) and geometry["parity_blocks"] >= 1,
            "system record geometry lacks one recoverable-member loss")
    return targets


def refusal(observed):
    response = observed.get("response") or {}
    require(observed.get("acknowledged") is False and observed.get("resultUnknown") is False
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {500, 503}
            and response.get("Error", {}).get("Code") in {"InternalError", "XMinioReadQuorum", "ServiceUnavailable"},
            "unknown bucket-config envelope was served as a default or lacks a structured server refusal")


def bucket_heal(fleet, probe, bucket, active, *, negative=False):
    path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="")
    admitted = fleet.record("bucket-heal-admission", probe.admin(path, [], json.dumps({**BASE_OPTIONS, "recursive": True}).encode()))
    require(admitted.get("httpStatus") == 200 and isinstance(admitted.get("response"), dict), "bucket Heal was not admitted")
    token = admitted["response"].get("clientToken")
    require(isinstance(token, str) and token, "bucket Heal omitted task identity")
    active.append((path, token))
    deadline = time.monotonic() + 180
    while True:
        raw = fleet.record("bucket-heal-status", probe.admin(path, [("clientToken", token)], b""))
        require(raw.get("httpStatus") == 200 and isinstance(raw.get("response"), dict), "bucket Heal lost its task")
        current = raw["response"]
        if current.get("summary") != "running":
            break
        require(time.monotonic() < deadline, "bucket Heal exceeded its fixed deadline")
        time.sleep(.2)
    active.remove((path, token))
    if negative:
        outcome = current.get("outcome", {})
        require(outcome.get("execution", {}).get("state") in {"failed", "completed_with_errors"}
                and outcome.get("counters", {}).get("healed") == 0,
                "unknown record became successful bucket Heal responsibility")
    return current


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory)
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    buckets = [bucket, bucket + "-control"]
    fleet.snapshot_buckets = buckets
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] == REFERENCE["candidateCommit"], "bucket-config schema contract is unreviewed for this candidate")
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), "maxInventoryObjects": 32,
        "maxInventoryBytes": 64 * 1024**2}
    fleet.record("source-bound-system-record-contract", REFERENCE)
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    fleet.record("binding", binding)
    probe, active = Probe(binding), []
    result = {"topology": topology, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        expected = seed_fixture(Recorder(probe, directory / "fixture"), buckets)
        baseline_config = {name: configs(probe, fleet, name, "config-baseline") for name in buckets}
        baseline_s3 = {name: fleet.record("S3-baseline", probe.inventory(name)) for name in buckets}
        for name in buckets:
            fleet.record("ACK-manifest", {"bucket": name, "expected": expected[name], **verify_seed(expected[name], baseline_s3[name])})
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        validate_topology(original, topology)
        records = system_snapshot(fleet, buckets, "system-record-baseline")
        targets = record_plan(records, bucket, topology, original)
        record_plan(records, buckets[1], topology, original)
        negative = branch == "unknown-record-negative"
        selected = targets if negative else targets[:1]
        kind = "future-xl-schema" if negative else "truncate" if branch == "bucket-policy-tags" else "missing"
        fault_map, registered = {}, []
        wanted_fault = system_files(records).copy()
        for index, (node, target) in enumerate(selected):
            params = {"disk": target["disk"], "faultId": f"D11-{branch}-{index}"}
            fleet.bucket_record_faults.append((node, params))
            registered.append((node, params))
            receipt = fleet.record("exact-system-record-fault", fleet.remote(node, "bucket-record-apply", **params,
                bucket=bucket, relativePath=target["path"], expectedSha256=target["sha256"], kind=kind))
            pair = node, target["disk"], target["path"]
            if receipt["fault"] is None:
                wanted_fault.pop(pair)
            else:
                wanted_fault[pair] = receipt["fault"]
                fault_map.setdefault(node, {})[f"{target['disk']}:{target['path']}"] = receipt["fault"]
        fault_scene = system_snapshot(fleet, buckets, "system-record-fault-witness", fault_map)
        require(system_files(fault_scene) == wanted_fault, "system fault changed an unselected record")
        require(physical_content(original) == physical_content(fleet.snapshot("business-fault-control", quiescent=True)),
                "system fault changed business objects or disk formats")
        fleet.start()
        if negative:
            for endpoint in binding["endpoints"]:
                observer = Probe(binding, endpoint)
                for operation in GETTERS:
                    refusal(fleet.record("unknown-record-config-refusal", observer.s3(operation, {"Bucket": bucket})))
                require(configs(observer, fleet, buckets[1], "healthy-config-control") == baseline_config[buckets[1]],
                        "unknown primary record changed the control bucket")
            # Public refusal and untouched raw unknown records are the oracle;
            # startup failure alone cannot satisfy this branch.
            task = bucket_heal(fleet, probe, bucket, active, negative=True)
        else:
            task = bucket_heal(fleet, probe, bucket, active)
            validate_version_receipts(task, bucket, expected[bucket], 2)
            for endpoint in binding["endpoints"]:
                for name in buckets:
                    require(configs(Probe(binding, endpoint), fleet, name, "post-repair-config") == baseline_config[name],
                            "versioning/policy/tags/lock config changed after record repair")
        idle_fleet(fleet, binding)
        fleet.stop()
        after = system_snapshot(fleet, buckets, "system-record-before-rescue-and-business-GET", fault_map if negative else None)
        require(system_files(after) == (wanted_fault if negative else system_files(records)),
                "unknown record was rewritten or recovery failed to restore complete original config bytes")
        require(physical_content(original) == physical_content(fleet.snapshot("physical-before-GET", quiescent=True)),
                "bucket metadata recovery changed business versions or disk formats")
        # Positive repair is already witnessed. Negative rescue is separately
        # recorded and is never counted as candidate repair.
        for node, params in registered:
            if negative:
                rescue = fleet.record("unknown-record-backup-rescue", fleet.remote(node, "bucket-record-recover", **params))
                require(rescue["physicalRepairPassAllowed"] is False, "backup rescue claimed repair")
            fleet.bucket_record_faults.remove((node, params))
        fleet.start()
        for endpoint in binding["endpoints"]:
            observer = Probe(binding, endpoint)
            for name in buckets:
                require(configs(observer, fleet, name, "final-config") == baseline_config[name], "final config differs")
                actual = fleet.record("all-peer-S3-after", {"endpoint": endpoint, **observer.inventory(name)})
                fleet.record("full-S3-proof", {**verify_seed(expected[name], actual), **compare_inventories(baseline_s3[name], actual)})
        fleet.stop()
        require(system_files(system_snapshot(fleet, buckets, "final-system-records")) == system_files(records),
                "final system records differ from same-run baseline")
        require(physical_content(original) == physical_content(fleet.snapshot("final-business-physical", quiescent=True)),
                "business reads changed the proven result")
        result.update(verdict="PASS", configAPIs=list(GETTERS), completeSystemBytes=True, taskId=task.get("clientToken"),
            unknownSchemaRefused=negative, backupRescueExcludedFromRepair=negative, credentialsRead=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("physical-failure", quiescent=True)
        except Exception as error:
            result["failureEvidenceError"] = str(error)
    finally:
        for path, token in active:
            try:
                fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc:
                fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"bucket metadata validation interrupted by signal {signum}")
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
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "four original Heal nodes required")
    require(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))
            and re.fullmatch(r"[0-9a-f]{64}", args.binary_sha), "immutable workflow/candidate identities required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "bucket metadata executor omitted a source branch")
    results = []
    try:
        for topology, selected_nodes in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, selected_nodes, branch))
                if results[-1]["cleanup"] != "PASS":
                    break
            if results[-1]["cleanup"] != "PASS":
                break
    finally:
        complete = len(results) == 8 and all(row["verdict"] == "PASS" and row["cleanup"] == "PASS" for row in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL", "complete": complete})
    require(complete, "D11 requires all eight H1/H2 record recovery/refusal instances and clean finalization")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal bucket metadata validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
