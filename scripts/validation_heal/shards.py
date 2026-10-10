#!/usr/bin/env python3
"""Run external-shard faults with frozen identities and physical repair oracles."""

from __future__ import annotations

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
import uuid

try:
    from .heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select


# Every branch uses a distinct object. Positions refer to physical bytes,
# including the checksum header; multipart numbers come from decoded metadata.
BRANCHES = {
    "heal.missing-part-file": [
        ("data-single-part", "objects/0008.bin", "data", 1, "missing", None, 2),
        ("parity-single-part", "objects/0017.bin", "parity", 1, "missing", None, 2),
        ("multipart-first", "multipart/0000.bin", "data", 1, "missing", None, 2),
        ("multipart-middle", "multipart/0001.bin", "data", 2, "missing", None, 2),
        ("multipart-last", "multipart/0002.bin", "data", 3, "missing", None, 2)],
    "heal.data-shard-bitrot": [
        ("data-first-byte", "objects/0008.bin", "data", 1, "bitflip", "first", 2),
        ("data-middle-byte", "objects/0017.bin", "data", 1, "bitflip", "middle", 2),
        ("data-last-byte", "objects/0026.bin", "data", 1, "bitflip", "last", 2),
        ("multipart-data", "multipart/0000.bin", "data", 2, "bitflip", "middle", 2)],
    "heal.parity-shard-bitrot": [
        ("parity-first-byte", "objects/0008.bin", "parity", 1, "bitflip", "first", 2),
        ("parity-middle-byte", "objects/0017.bin", "parity", 1, "bitflip", "middle", 2),
        ("parity-last-byte", "objects/0026.bin", "parity", 1, "bitflip", "last", 2)],
    "heal.truncated-and-zeroed-shard": [
        ("truncate-half", "objects/0008.bin", "data", 1, "truncate", None, 2),
        ("truncate-near-tail", "objects/0017.bin", "data", 1, "truncate", "last", 2),
        ("zero-all", "objects/0026.bin", "data", 1, "zero", None, 2),
        ("checksum-header", "objects/0035.bin", "data", 1, "header", None, 2)],
    "heal.completed-multipart-object": [
        ("first-part", "multipart/0000.bin", "data", 1, "missing", None, 2),
        ("middle-part", "multipart/0001.bin", "data", 2, "missing", None, 2),
        ("last-part", "multipart/0002.bin", "data", 3, "missing", None, 2),
        ("cross-part-range", "multipart/0003.bin", "data", 2, "bitflip", "middle", 2)],
    "heal.manual-normal-attribution": [
        ("normal-missing-data", "objects/0008.bin", "data", 1, "missing", None, 1),
        ("normal-missing-parity", "objects/0017.bin", "parity", 1, "missing", None, 1)],
    "heal.manual-deep-attribution": [
        ("deep-parity-bitrot", "objects/0008.bin", "parity", 1, "bitflip", "middle", 2),
        ("deep-data-bitrot", "objects/0017.bin", "data", 1, "bitflip", "middle", 2)],
}
SIZES = [0, 1, 4095, 4096, 4097, 1024**2 - 1, 1024**2, 1024**2 + 1, 5 * 1024**2 + 123]
MULTIPART_SIZES = (5 * 1024**2 + 17, 5 * 1024**2 + 93, 1024**2 + 47)


def files(snapshot):
    entries = {(node, entry["disk"], entry["path"]): entry
               for node, observation in snapshot.items() for entry in observation["files"]}
    require(len(entries) == sum(len(value["files"]) for value in snapshot.values()),
            "duplicate physical file identity")
    return entries


def map_target(snapshot, bucket, key, role, part_number, version_id="null"):
    validate_topology(snapshot, "H1" if len(snapshot) == 1 else "H2")
    indexed = files(snapshot)
    cohort, logical = [], None
    for node, observation in snapshot.items():
        for format_entry in observation["formats"]:
            disk = format_entry["disk"]
            identity = (node, disk, f"{bucket}/{key}/xl.meta")
            metadata = indexed.get(identity, {}).get("metadata", {})
            require(metadata.get("bucket") == bucket and metadata.get("key") == key,
                    "missing authoritative metadata identity on a baseline drive")
            versions = metadata.get("versions", [])
            require(len(versions) == 1 and (versions[0].get("versionId") or "null") == version_id,
                    "baseline metadata version differs from acknowledged fixture")
            version = versions[0]
            require(version.get("isLatest") is True and version.get("deleted") is False
                    and version.get("inline") is None and version.get("dataDir"), "expected live external object")
            data_dir = version["dataDir"]
            require(str(uuid.UUID(data_dir)) == data_dir, "noncanonical data directory")
            erasure = version["erasure"]
            data, parity = erasure["data_blocks"], erasure["parity_blocks"]
            distribution = erasure["distribution"]
            require(type(data) is int and type(parity) is int and data > 0 and parity > 0
                    and data + parity == len(distribution)
                    and sorted(distribution) == list(range(1, data + parity + 1)), "invalid erasure geometry")
            layout = format_entry["value"]["xl"]
            slot = layout["sets"][0].index(layout["this"])
            require(erasure["index"] == distribution[slot], "shard role does not match the disk's actual set slot")
            actual_role = "data" if erasure["index"] <= data else "parity"
            require(version.get("role") == actual_role, "decoded shard role differs from erasure index")
            logical_entry = {name: version.get(name) for name in
                             ("versionId", "isLatest", "deleted", "dataDir", "size", "parts", "usesLegacyChecksum")}
            logical_entry["erasure"] = {name: erasure[name] for name in
                                         ("data_blocks", "parity_blocks", "block_size", "distribution", "algorithm")}
            require(logical is None or logical == logical_entry, "baseline versions or part geometry lack one full-set authority")
            logical = logical_entry
            parts = [part for part in version["parts"] if part["number"] == part_number]
            require(len(parts) == 1 and parts[0]["size"] == parts[0]["actualSize"] > 0,
                    "unsupported compressed/encrypted or ambiguous fixture part")
            relative = f"{bucket}/{key}/{data_dir}/part.{part_number}"
            entry = indexed.get((node, disk, relative))
            require(entry and entry["size"] > 32, "external part is missing from the healthy baseline")
            cohort.append({"node": node, "disk": disk, "relativePath": relative, "role": actual_role,
                "index": erasure["index"], "versionId": version_id, "partNumber": part_number,
                "dataDir": data_dir, "diskUUID": layout["this"], "setSlot": slot,
                "sha256": entry["sha256"], "size": entry["size"], "metadata": metadata,
                "baselineMetadataSha256": indexed[identity]["sha256"]})
    require(len(cohort) == logical["erasure"]["data_blocks"] + logical["erasure"]["parity_blocks"]
            and len({entry["index"] for entry in cohort}) == len(cohort)
            and len({entry["sha256"] for entry in cohort}) == len(cohort),
            "independent full-set shard identity or nonrepeating content is missing")
    target = next((entry for entry in cohort if entry["role"] == role), None)
    require(target is not None, "requested shard role is not present")
    return {**target, "bucket": bucket, "key": key, "poolIndex": 0, "setIndex": 0,
            "cohort": cohort, "logical": logical}


def verify_cohort(fleet, target, label):
    proofs = []
    for entry in target["cohort"]:
        proof = fleet.record(label, {"node": entry["node"], **fleet.remote(entry["node"], "verify-part",
            disk=entry["disk"], relativePath=entry["relativePath"],
            versionId=entry["versionId"], partNumber=entry["partNumber"])})
        require(proof["sha256"] == entry["sha256"] and proof["physicalSize"] == entry["size"],
                "physical part differs from the frozen same-role reconstruction oracle")
        proofs.append(proof)
    return {"allShardBitrotVerified": True, "sameRoleOriginalShardBytesRestored": True,
            "proof": "complete pre-fault shard oracle, distinct shards, verified streaming checksums", "shards": proofs}


def validate_fault(before, after, target, receipt):
    left, right = files(before), files(after)
    identity = (target["node"], target["disk"], target["relativePath"])
    require(receipt.get("relativePath") == target["relativePath"]
            and receipt.get("before") == {"sha256": target["sha256"], "size": target["size"]},
            "fault receipt differs from the frozen target")
    for node in before:
        require(before[node]["formats"] == after[node]["formats"], "file fault changed disk format")
    expected = receipt.get("fault")
    if expected is None:
        require(identity not in right, "missing target was not physically witnessed")
    else:
        require(identity in right and {name: right[identity][name] for name in ("sha256", "size")} == expected
                and expected != receipt["before"], "changed shard was not physically witnessed")
    require({key: value for key, value in left.items() if key != identity}
            == {key: value for key, value in right.items() if key != identity},
            "file fault changed metadata, controls or another shard")


def validate_repaired_physical(before, after, target):
    left, right = files(before), files(after)
    meta_path = f"{target['bucket']}/{target['key']}/xl.meta"
    for node in before:
        require(before[node]["formats"] == after[node]["formats"], "repair changed disk format")
    for identity, entry in left.items():
        actual = right.get(identity)
        require(actual is not None, "repair lost a baseline file or control")
        if identity[2] != meta_path:
            require(actual == entry, "repair did not restore exact part bytes or changed a control")
        else:
            def semantics(metadata):
                return {"bucket": metadata["bucket"], "key": metadata["key"], "versions": [
                    {**version, "erasure": {name: value for name, value in version["erasure"].items() if name != "checksums"}}
                    for version in metadata["versions"]]}
            require(actual.get("metadata") and semantics(actual["metadata"]) == semantics(entry["metadata"]),
                    "repair changed decoded version/part identity")
    for identity in set(right) - set(left):
        original = left.get((identity[0], identity[1], meta_path))
        allowed = f"{target['bucket']}/{target['key']}/{target['dataDir']}/xl.meta.bkp"
        require(identity[2] == allowed and original
                and right[identity]["sha256"] == original["sha256"] and right[identity]["size"] == original["size"],
                "repair created an unexpected physical file")
    return {"targetLengthAndFullSha256Restored": True, "controlFilesUnchanged": True,
            "decodedVersionsUnchanged": True, "formatUnchanged": True, "readBeforeRepair": False}


def validate_repaired_status(status, target):
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "finished" and status.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed"
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False,
            "repair did not prove complete terminal coverage")
    expected = {"processed": 1, "healed": 1, "unchanged": 0, "skipped": 0,
                "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    counters = outcome.get("counters", {})
    require(all(type(counters.get(name)) is type(value) and counters[name] == value
                for name, value in expected.items()), "repair counters do not prove exactly one repaired object")
    objects = outcome.get("objects", [])
    require(len(objects) == 1, "repair lacks one complete object receipt")
    identity = objects[0].get("identity", {})
    require(identity.get("bucket") == target["bucket"] and identity.get("object") == target["key"]
            and (identity.get("versionId") or "null") == target["versionId"]
            and type(identity.get("poolIndex")) is int and identity["poolIndex"] == target["poolIndex"]
            and type(identity.get("setIndex")) is int and identity["setIndex"] == target["setIndex"]
            and bool(identity.get("bucketIncarnationId")), "repair responsibility identity is incomplete or different")
    require(objects[0].get("disposition", {}).get("state") == "repaired",
            "task finish or unknown receipt does not prove repair")


def run_task(probe, fleet, target, mode, active):
    path = "/rustfs/admin/v3/heal/" + quote(target["bucket"], safe="") + "/" + quote(target["key"], safe="/")
    started = fleet.record("trigger", probe.admin(path, [], json.dumps({**BASE_OPTIONS, "scanMode": mode}).encode()))
    require(started.get("httpStatus") == 200 and isinstance(started.get("response"), dict), "repair admission failed")
    token = started["response"].get("clientToken")
    require(isinstance(token, str) and token, "repair admission omitted token")
    active.append((path, token))
    deadline = time.monotonic() + 180
    while True:
        observation = fleet.record("control", probe.admin(path, [("clientToken", token)], b""))
        require(observation.get("httpStatus") == 200 and isinstance(observation.get("response"), dict), "repair status failed")
        status = observation["response"]
        if status.get("summary") != "running":
            break
        require(time.monotonic() < deadline, "repair task deadline exceeded")
        time.sleep(0.2)
    active.remove((path, token))
    for _ in range(2):
        repeat = fleet.record("control-repeat", probe.admin(path, [("clientToken", token)], b""))
        require(repeat.get("httpStatus") == 200 and repeat.get("response", {}).get("summary") == status.get("summary")
                and canonical(repeat["response"].get("outcome")) == canonical(status.get("outcome")),
                "terminal repair responsibility changed")
    return status


def pending_inventory(probe, bucket):
    response = probe.s3("list_multipart_uploads", {"Bucket": bucket})
    require(response["acknowledged"] and not response["response"].get("IsTruncated"), "MPU control listing failed")
    entries = []
    for upload in response["response"].get("Uploads", []):
        require(upload["Key"] in {"pending-a", "pending-b"}, "unexpected pending MPU")
        parts = probe.s3("list_parts", {"Bucket": bucket, "Key": upload["Key"], "UploadId": upload["UploadId"]})
        require(parts["acknowledged"] and not parts["response"].get("IsTruncated"), "pending MPU part listing failed")
        entries.append({"key": upload["Key"], "uploadId": upload["UploadId"], "parts": [
            {name: part[name] for name in ("PartNumber", "ETag", "Size")} for part in parts["response"].get("Parts", [])]})
    require(len(entries) == 2 and {entry["key"] for entry in entries} == {"pending-a", "pending-b"}
            and all(len(entry["parts"]) == 1 for entry in entries), "acknowledged pending MPU controls are missing")
    return sorted(entries, key=lambda entry: entry["key"])


def verify_ranges(probe, fleet, bucket, key):
    if key.startswith("multipart/"):
        body = b"".join(payload(f"{bucket}/{key}/{number}", size) for number, size in enumerate(MULTIPART_SIZES, 1))
        boundaries = (MULTIPART_SIZES[0], sum(MULTIPART_SIZES[:2]))
    else:
        body = payload(f"{bucket}/{key}/0", SIZES[int(key.rsplit("/", 1)[1].split(".")[0]) % len(SIZES)])
        boundaries = (1024**2, len(body) // 2)
    proofs = []
    for start, end in [(point - 17, point + 29) for point in boundaries] + [(len(body) - 47, len(body) - 1)]:
        require(0 <= start <= end < len(body), "invalid fixture range")
        observed = fleet.record("s3-range", probe.s3("get_object", {"Bucket": bucket, "Key": key,
            "VersionId": "null", "Range": f"bytes={start}-{end}"}))
        wanted = body[start:end + 1]
        require(observed["acknowledged"] and observed["response"]["ResponseMetadata"]["HTTPStatusCode"] == 206
                and observed["response"].get("ContentRange") == f"bytes {start}-{end}/{len(body)}"
                and observed["payload"] == {"size": len(wanted), "sha256": hashlib.sha256(wanted).hexdigest()},
                "range content or part ordering differs from independent fixture")
        proofs.append({"start": start, "end": end, "sha256": hashlib.sha256(wanted).hexdigest()})
    return proofs


def run_topology(args, topology, nodes):
    directory = args.evidence / topology
    directory.mkdir()
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
    require([branch[0] for branch in BRANCHES[args.case_id]] == recipe["variants"], "executor omitted a source branch")
    result = {"topology": topology, "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    active = []
    probe = Probe(binding)
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, "DB", 128, SIZES)
        needs_multipart = "DM" in recipe["contract"]["probes"]["fixture"].split(",")
        if needs_multipart:
            expected += seed(recorder, bucket, "DM", 4, SIZES, existing_bucket=True)
        baseline = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        pending = fleet.record("mpu-baseline", pending_inventory(probe, bucket)) if needs_multipart else None
        for entry in baseline["entries"]:
            head = recorder.call("head_object", {"Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]})
            require(all(head.get(name) == value for name, value in entry["properties"].items()), "HEAD differs from GET baseline")
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        targets = [(branch, map_target(original, bucket, branch[1], branch[2], branch[3]))
                   for branch in BRANCHES[args.case_id]]
        fleet.record("fault-plan", {"caseId": args.case_id, "topology": topology,
            "branches": [{"variant": branch[0], "faultBackend": "logical-file-fault", "target": target,
                          "kind": branch[4], "position": branch[5], "scanMode": branch[6]} for branch, target in targets]})
        for branch, target in targets:
            name, key, role, number, kind, position, mode = branch
            fault_id = recipe["code"] + "-" + name
            recovery = {"disk": target["disk"], "faultId": fault_id}
            registered = False
            try:
                fleet.stop()
                before = fleet.snapshot("physical-before-" + name, quiescent=True)
                verify_cohort(fleet, target, "baseline-bitrot-" + name)
                offset = {"first": 0, "middle": target["size"] // 2, "last": target["size"] - 1}.get(position)
                fleet.faults.append((target["node"], recovery))
                registered = True
                receipt = fleet.record("fault-" + name, fleet.remote(target["node"], "apply-fault",
                    **recovery, relativePath=target["relativePath"], expectedSha256=target["sha256"], kind=kind, offset=offset))
                after_fault = fleet.snapshot("physical-fault-" + name, quiescent=True)
                validate_fault(before, after_fault, target, receipt)
                fleet.start()
                witness = fleet.snapshot("physical-before-trigger-" + name)
                require(physical_content(after_fault) == physical_content(witness),
                        "ATTRIBUTION_NOT_OBSERVED: fault changed before the manual task")
                status = run_task(probe, fleet, target, mode, active)
                fleet.stop()
                repaired = fleet.snapshot("physical-after-" + name, quiescent=True)
                physical = validate_repaired_physical(before, repaired, target)
                bitrot = verify_cohort(fleet, target, "repaired-bitrot-" + name)
                validate_repaired_status(status, target)
                fleet.start()
                current = fleet.record("s3-after-" + name, probe.inventory(bucket, unversioned_current_tags=True))
                comparison = compare_inventories(baseline, current)
                ranges = verify_ranges(probe, fleet, bucket, key)
                if pending is not None:
                    require(fleet.record("mpu-after-" + name, pending_inventory(probe, bucket)) == pending,
                            "repair changed pending MPU identities or parts")
                result["branches"][name] = {"verdict": "PASS", "physical": physical, "bitrot": bitrot,
                    "s3": comparison, "ranges": ranges, "trigger": "manual-normal" if mode == 1 else "manual-deep"}
            except (Exception, KeyboardInterrupt) as exc:
                result["branches"][name] = {"verdict": "FAIL", "failure": {"type": type(exc).__name__, "message": str(exc)}}
                try:
                    fleet.stop()
                    fleet.snapshot("physical-failure-" + name, quiescent=True)
                    if registered:
                        rescue = fleet.record("rescue-" + name, fleet.remote(target["node"], "recover-fault", **recovery))
                        fleet.faults.remove((target["node"], recovery))
                        result["branches"][name]["rescue"] = rescue
                except Exception as recovery_error:
                    result["branches"][name]["recoveryError"] = str(recovery_error)
                    raise
                if isinstance(exc, KeyboardInterrupt) or active:
                    raise
        result["verdict"] = "PASS" if all(value["verdict"] == "PASS" for value in result["branches"].values()) else "FAIL"
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
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            results.append(run_topology(args, topology, selected))
            if results[-1]["cleanup"] != "PASS":
                break
    finally:
        complete = len(results) == 2 and all(value["verdict"] == "PASS" and value["cleanup"] == "PASS" for value in results)
        save(args.evidence / "healthy-summary.json", {"caseId": args.case_id, "results": results,
             "cleanup": "PASS" if results and all(value["cleanup"] == "PASS" for value in results) else "FAIL", "complete": complete})
    require(complete, "Heal case requires every source H1/H2 branch; inspect topology-result evidence")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal shard validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
