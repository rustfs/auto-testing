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
    "heal.range-head-and-conditions": [
        ("head", "objects/0008.bin", "data", 1, "missing", None, 2),
        ("range-valid", "multipart/0000.bin", "data", 2, "bitflip", "middle", 2),
        ("range-outside", "objects/0017.bin", "data", 1, "missing", None, 2),
        ("if-match", "objects/0026.bin", "data", 1, "bitflip", "middle", 2),
        ("if-none-match", "objects/0035.bin", "data", 1, "missing", None, 2),
        ("if-modified-since", "objects/0044.bin", "data", 1, "bitflip", "middle", 2),
        ("if-unmodified-since", "objects/0053.bin", "data", 1, "missing", None, 2)],
    "heal.admin-dry-run": [
        ("missing-remove0-recreate0", "objects/0008.bin", "data", 1, "missing", None, 2),
        ("missing-remove1-recreate0", "objects/0017.bin", "data", 1, "missing", None, 2),
        ("bitflip-remove0-recreate1", "objects/0026.bin", "data", 1, "bitflip", "middle", 2),
        ("bitflip-remove1-recreate1", "objects/0035.bin", "data", 1, "bitflip", "middle", 2),
        ("versioned-missing", "objects/0000.bin", "data", 1, "missing", None, 2),
        ("versioned-bitflip", "objects/0001.bin", "data", 1, "bitflip", "middle", 2)],
    "heal.admin-token-cross-node-status": [
        ("coordinator-to-peer", "objects/0008.bin", "data", 1, "missing", None, 2),
        ("coordinator-to-stable-endpoint", "objects/0017.bin", "data", 1, "missing", None, 2)],
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


def map_target(snapshot, bucket, key, role, part_number, version_id="null", *, preferred_nodes=None, latest_required=True,
               multi_set=False, set_index=0):
    require(type(multi_set) is bool and type(set_index) is int and 0 <= set_index < (2 if multi_set else 1),
            "invalid explicit object set mapping")
    validate_topology(snapshot, "H1" if len(snapshot) == 1 else "H2", expected_sets=2 if multi_set else 1)
    indexed = files(snapshot)
    cohort, logical = [], None
    for node, observation in snapshot.items():
        for format_entry in observation["formats"]:
            disk = format_entry["disk"]
            identity = (node, disk, f"{bucket}/{key}/xl.meta")
            layout = format_entry["value"]["xl"]
            if layout["this"] not in layout["sets"][set_index]:
                require(not any(owner == node and drive == disk and path.startswith(f"{bucket}/{key}/")
                                for owner, drive, path in indexed), "object files span the selected and unselected erasure sets")
                continue
            metadata = indexed.get(identity, {}).get("metadata", {})
            require(metadata.get("bucket") == bucket and metadata.get("key") == key,
                    "missing authoritative metadata identity on a baseline drive")
            versions = [version for version in metadata.get("versions", []) if (version.get("versionId") or "null") == version_id]
            require(len(versions) == 1,
                    "baseline metadata version differs from acknowledged fixture")
            version = versions[0]
            require((not latest_required or version.get("isLatest") is True) and version.get("deleted") is False
                    and version.get("inline") is None and version.get("dataDir"), "expected live external object")
            data_dir = version["dataDir"]
            require(str(uuid.UUID(data_dir)) == data_dir, "noncanonical data directory")
            erasure = version["erasure"]
            data, parity = erasure["data_blocks"], erasure["parity_blocks"]
            distribution = erasure["distribution"]
            require(type(data) is int and type(parity) is int and data > 0 and parity > 0
                    and data + parity == len(distribution)
                    and data + parity == len(layout["sets"][set_index])
                    and sorted(distribution) == list(range(1, data + parity + 1)), "invalid erasure geometry")
            slot = layout["sets"][set_index].index(layout["this"])
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
    require(logical is not None and len(cohort) == logical["erasure"]["data_blocks"] + logical["erasure"]["parity_blocks"]
            and len({entry["index"] for entry in cohort}) == len(cohort)
            and len({entry["sha256"] for entry in cohort}) == len(cohort),
            "independent full-set shard identity or nonrepeating content is missing")
    target = next((entry for entry in cohort if entry["role"] == role
                   and (preferred_nodes is None or entry["node"] in preferred_nodes)), None)
    require(target is not None, "requested shard role is not present")
    return {**target, "bucket": bucket, "key": key, "poolIndex": 0, "setIndex": set_index,
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


def validate_repaired_physical(before, after, target, *, repair_targets=None):
    left, right = files(before), files(after)
    repair_targets = repair_targets or [target]
    meta_paths = {f"{item['bucket']}/{item['key']}/xl.meta" for item in repair_targets}
    backups = {(item["node"], item["disk"], f"{item['bucket']}/{item['key']}/{item['dataDir']}/xl.meta.bkp"):
               left.get((item["node"], item["disk"], f"{item['bucket']}/{item['key']}/xl.meta"))
               for item in repair_targets}
    completed_sidecars = {(item["node"], item["disk"],
                          f"{item['bucket']}/{item['key']}/{item['dataDir']}/part.{part['number']}.meta")
                         for item in repair_targets for part in item["logical"]["parts"]}
    def exact_backup(identity, actual):
        original = backups.get(identity)
        return bool(original and actual and actual["sha256"] == original["sha256"] and actual["size"] == original["size"])
    for node in before:
        require(before[node]["formats"] == after[node]["formats"], "repair changed disk format")
    for identity, entry in left.items():
        actual = right.get(identity)
        # Complete MPU commits the authoritative part manifest into xl.meta.
        # A healed target directory can discard only its stale per-part sidecars;
        # other versions, drives, pending uploads and payload files stay exact.
        if actual is None and identity in completed_sidecars:
            continue
        require(actual is not None, "repair lost a baseline file or control")
        if identity in backups and actual != entry:
            # Repeated repair replaces only this target's rollback metadata
            # with the exact authoritative bytes observed before this repair.
            require(exact_backup(identity, actual), "target rollback backup is not the exact pre-repair metadata")
        elif identity[2] not in meta_paths:
            require(actual == entry, "repair did not restore exact part bytes or changed a control")
        else:
            def semantics(metadata):
                return {"bucket": metadata["bucket"], "key": metadata["key"], "versions": [
                    {**version, "inline": {name: version["inline"].get(name) for name in ("length", "sha256")}
                        if version.get("inline") else None,
                     "erasure": {name: value for name, value in version["erasure"].items() if name != "checksums"}}
                    for version in metadata["versions"]]}
            require(actual.get("metadata") and semantics(actual["metadata"]) == semantics(entry["metadata"]),
                    "repair changed decoded version/part identity")
    for identity in set(right) - set(left):
        require(exact_backup(identity, right[identity]),
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


def validate_dry_run_status(status, target, expected):
    outcome = status.get("outcome") or {}
    identities = {(entry["key"], entry["versionId"]) for entry in expected if entry["key"] == target["key"]}
    require(status.get("summary") == "finished" and status.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed" and outcome.get("coverage") == "complete"
            and outcome.get("objectsTruncated") is False and identities, "dry-run did not prove complete terminal coverage")
    objects = outcome.get("objects", [])
    require(len(objects) == len(identities), "dry-run omitted versions or markers")
    actual = set()
    for entry in objects:
        identity = entry.get("identity", {})
        actual.add((identity.get("object"), identity.get("versionId") or "null"))
        require(identity.get("bucket") == target["bucket"]
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and entry.get("disposition", {}).get("state") == "dry_run_observed",
                "dry-run admitted a different responsibility or claimed repair")
    require(actual == identities, "dry-run responsibility versions differ from acknowledged fixture")
    wanted = {"processed": len(identities), "healed": 0, "unchanged": 0, "skipped": len(identities),
              "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    counters = outcome.get("counters", {})
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in wanted.items()),
            "dry-run counters do not match the complete dry_run_observed responsibility set")


def peer_status_equal(coordinator, peer, *, task, peer_task):
    # The candidate renders startTime at response serialization; it is not a
    # persistent task epoch. Bind the actual signed queries to admission instead.
    return (coordinator.get("summary") == "finished" and peer.get("summary") == "finished"
            and isinstance(task, tuple) and len(task) == 2 and all(task) and task == peer_task
            and canonical(coordinator.get("outcome")) == canonical(peer.get("outcome"))
            and coordinator.get("outcome", {}).get("coverage") == "complete"
            and peer.get("truncated", False) is False)


def run_task(probe, fleet, target, mode, active, *, status_endpoints=(), option_overrides=None, admissions=None):
    path = "/rustfs/admin/v3/heal/" + quote(target["bucket"], safe="")
    if target["key"]:
        path += "/" + quote(target["key"], safe="/")
    started = fleet.record("trigger", probe.admin(path, [], json.dumps(
        {**BASE_OPTIONS, "scanMode": mode, **(option_overrides or {})}).encode()))
    require(started.get("httpStatus") == 200 and isinstance(started.get("response"), dict), "repair admission failed")
    token = started["response"].get("clientToken")
    require(isinstance(token, str) and token, "repair admission omitted token")
    if admissions is not None:
        admissions.append({"path": path, "clientToken": token})
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
    convergence_deadline = time.monotonic() + 30
    for endpoint in status_endpoints:
        remote_probe = Probe(probe.binding, endpoint)
        require(remote_probe.endpoint != probe.endpoint, "peer status must query another actual endpoint")
        while True:
            observed = fleet.record("cross-node-control", remote_probe.admin(path, [("clientToken", token)], b""))
            require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
                    "cross-node status is missing; 404/unknown cannot certify task completion")
            if peer_status_equal(status, observed["response"], task=(path, token), peer_task=(path, token)):
                break
            require(time.monotonic() < convergence_deadline, "peer responsibility did not converge within 30 seconds")
            time.sleep(0.2)
    if status_endpoints:
        fleet.record("cross-node-convergence", {"path": path, "clientToken": token,
            "coordinator": probe.endpoint_name, "queriedEndpoints": list(status_endpoints), "budgetSeconds": 30,
            "sameOriginalPathAndToken": True, "completeOutcomeEqual": True,
            "responseStartTimeIsTaskIdentity": False})
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


def verify_ranges(probe, fleet, bucket, key, *, version_id="null", expected_body=None):
    if key.startswith("multipart/"):
        body = b"".join(payload(f"{bucket}/{key}/{number}", size) for number, size in enumerate(MULTIPART_SIZES, 1))
        boundaries = (MULTIPART_SIZES[0], sum(MULTIPART_SIZES[:2]))
    else:
        body = expected_body if expected_body is not None else payload(
            f"{bucket}/{key}/0", SIZES[int(key.rsplit("/", 1)[1].split(".")[0]) % len(SIZES)])
        boundaries = (1024**2, len(body) // 2)
    proofs = []
    require(len(body) > 0, "positive ranges require a nonempty fixture")
    intervals = [(max(0, point - 17), min(len(body) - 1, point + 29))
                 for point in boundaries if 0 < point < len(body)]
    intervals.append((max(0, len(body) - 47), len(body) - 1))
    for start, end in intervals:
        require(0 <= start <= end < len(body), "invalid fixture range")
        observed = fleet.record("s3-range", probe.s3("get_object", {"Bucket": bucket, "Key": key,
            "VersionId": version_id, "Range": f"bytes={start}-{end}"}))
        wanted = body[start:end + 1]
        require(observed["acknowledged"] and observed["response"]["ResponseMetadata"]["HTTPStatusCode"] == 206
                and observed["response"].get("ContentRange") == f"bytes {start}-{end}/{len(body)}"
                and observed["payload"] == {"size": len(wanted), "sha256": hashlib.sha256(wanted).hexdigest()},
                "range content or part ordering differs from independent fixture")
        proofs.append({"start": start, "end": end, "sha256": hashlib.sha256(wanted).hexdigest()})
    return proofs


def validate_conditional_response(observed, status, baseline, *, code=None, full_body=False):
    response = observed.get("response") or {}
    require(response.get("ResponseMetadata", {}).get("HTTPStatusCode") == status,
            f"conditional request expected HTTP {status}")
    if status >= 300:
        require(observed.get("acknowledged") is False and observed.get("resultUnknown") is False
                and "payload" not in observed, "conditional denial returned success data or unknown transport outcome")
        if code is not None:
            require(response.get("Error", {}).get("Code") == code, "conditional response has the wrong structured error")
    else:
        require(observed.get("acknowledged") is True and response.get("ETag") == baseline["properties"]["ETag"],
                "conditional request returned the wrong object ETag")
        if full_body:
            require(observed.get("payload") == baseline["payload"], "conditional GET returned wrong full content")


def verify_conditions(probe, fleet, branch, baseline, head, label):
    name, key = branch[:2]
    entry = next(entry for entry in baseline["entries"] if entry["key"] == key)
    params = {"Bucket": baseline["bucket"], "Key": key, "VersionId": entry["versionId"]}
    etag, modified = head["ETag"], head["LastModified"]
    require(isinstance(modified, datetime) and modified.tzinfo is not None, "baseline LastModified must be server time")
    requests = {
        "head": [("head_object", {}, 200, None)],
        "range-outside": [("get_object", {"Range": f"bytes={entry['payload']['size']}-"}, 416, "InvalidRange")],
        "if-match": [("get_object", {"IfMatch": etag}, 200, None),
                     ("get_object", {"IfMatch": '"00000000000000000000000000000000"'}, 412, "PreconditionFailed")],
        "if-none-match": [("get_object", {"IfNoneMatch": etag}, 304, None),
                          ("get_object", {"IfNoneMatch": '"00000000000000000000000000000000"'}, 200, None)],
        "if-modified-since": [("get_object", {"IfModifiedSince": modified + timedelta(seconds=2)}, 304, None),
                              ("get_object", {"IfModifiedSince": modified - timedelta(seconds=2)}, 200, None)],
        "if-unmodified-since": [("get_object", {"IfUnmodifiedSince": modified - timedelta(seconds=2)}, 412, "PreconditionFailed"),
                                ("get_object", {"IfUnmodifiedSince": modified + timedelta(seconds=2)}, 200, None)]}
    if name == "range-valid":
        return verify_ranges(probe, fleet, baseline["bucket"], key)
    results = []
    for operation, extra, status, code in requests[name]:
        observed = fleet.record(label + "-" + name, probe.s3(operation, {**params, **extra}))
        validate_conditional_response(observed, status, entry, code=code, full_body=operation == "get_object" and status == 200)
        if operation == "head_object":
            require(observed["response"].get("LastModified") == modified
                    and all(observed["response"].get(field) == value for field, value in entry["properties"].items()),
                    "HEAD content properties or server LastModified changed")
        results.append({"operation": operation, "status": status, "condition": list(extra)})
    return results


def run_topology(args, topology, nodes, *, fixture="DB"):
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
    result = {"topology": topology, "fixture": fixture, "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    active = []
    probe = Probe(binding)
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, fixture, 128 if fixture == "DB" else 4,
                        SIZES if fixture == "DB" else [5 * 1024**2 + 123])
        needs_multipart = "DM" in recipe["contract"]["probes"]["fixture"].split(",")
        if needs_multipart:
            expected += seed(recorder, bucket, "DM", 4, SIZES, existing_bucket=True)
        baseline = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        pending = fleet.record("mpu-baseline", pending_inventory(probe, bucket)) if needs_multipart else None
        heads = {}
        for entry in baseline["entries"]:
            if entry["deleteMarker"]:
                continue
            head = recorder.call("head_object", {"Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]})
            heads[entry["key"]] = head
            require(all(head.get(name) == value for name, value in entry["properties"].items()), "HEAD differs from GET baseline")
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        cross_node = args.case_id == "heal.admin-token-cross-node-status"
        if cross_node:
            fleet.record("cross-node-binding", {"coordinator": "node1", "peer": "node2", "stableEndpoint": "node3",
                "endpoints": binding["endpoints"], "stableEndpointKind": "fixed current auto-testing node address",
                "faultTargetExcludesCoordinator": True})
        dry_run = args.case_id == "heal.admin-dry-run"
        conditions = args.case_id == "heal.range-head-and-conditions"
        selected_branches = [branch for branch in BRANCHES[args.case_id]
                            if not dry_run or branch[0].startswith("versioned-") == (fixture == "DV")]
        if conditions:
            fleet.start()
            for branch in selected_branches:
                verify_conditions(probe, fleet, branch, baseline, heads[branch[1]], "baseline-condition")
            fleet.stop()
        targets = []
        for branch in selected_branches:
            version = next(entry for entry in expected if entry["key"] == branch[1] and not entry["deleteMarker"])
            targets.append((branch, map_target(original, bucket, branch[1], branch[2], branch[3], version["versionId"],
                preferred_nodes=nodes[1:] if cross_node else None, latest_required=fixture != "DV")))
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
                status = run_task(probe, fleet, target, mode, active,
                    status_endpoints=("node2", "node3") if cross_node else (),
                    option_overrides={"dryRun": True, "recursive": fixture == "DV",
                                      "remove": "remove1" in name, "recreate": "recreate1" in name}
                    if dry_run else None)
                listing = fleet.record("dry-run-version-list", probe.s3("list_object_versions", {"Bucket": bucket})) if dry_run else None
                fleet.stop()
                repaired = fleet.snapshot("physical-after-" + name, quiescent=True)
                if dry_run:
                    require(physical_content(after_fault) == physical_content(repaired),
                            "dry-run changed object shards, metadata or delete markers")
                    validate_dry_run_status(status, target, expected)
                    require(listing["acknowledged"] and not listing["response"].get("IsTruncated"), "dry-run version listing incomplete")
                    observed_versions = {(entry["Key"], entry["VersionId"], entry["IsLatest"], deleted)
                        for field, deleted in (("Versions", False), ("DeleteMarkers", True))
                        for entry in listing["response"].get(field, [])}
                    require(observed_versions == {(entry["key"], entry["versionId"], entry["isLatest"], entry["deleteMarker"])
                        for entry in expected}, "dry-run changed version or delete-marker manifest")
                    fleet.record("dry-run-rescue", fleet.remote(target["node"], "recover-fault", **recovery))
                    fleet.faults.remove((target["node"], recovery))
                    registered = False
                    recovered = fleet.snapshot("physical-recovered-" + name, quiescent=True)
                    require(physical_content(before) == physical_content(recovered), "dry-run fault recovery differs from baseline")
                    physical = {"dryRunPhysicalUnchanged": True, "repairedAck": False, "backupUsedOnlyAfterNoMutationProof": True}
                else:
                    physical = validate_repaired_physical(before, repaired, target)
                bitrot = verify_cohort(fleet, target, ("recovered-bitrot-" if dry_run else "repaired-bitrot-") + name)
                fleet.start()
                current = fleet.record("s3-after-" + name, probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
                comparison = compare_inventories(baseline, current)
                if cross_node:
                    for endpoint in binding["endpoints"]:
                        peer_s3 = fleet.record("s3-entry-" + endpoint, Probe(binding, endpoint).inventory(
                            bucket, unversioned_current_tags=True))
                        compare_inventories(baseline, peer_s3)
                ranges = verify_ranges(probe, fleet, bucket, key, version_id=target["versionId"],
                    expected_body=payload(f"{bucket}/{key}/0", 5 * 1024**2 + 123) if fixture == "DV" else None)
                condition_proof = verify_conditions(probe, fleet, branch, baseline, heads[key], "repaired-condition") if conditions else None
                if pending is not None:
                    require(fleet.record("mpu-after-" + name, pending_inventory(probe, bucket)) == pending,
                            "repair changed pending MPU identities or parts")
                if not dry_run:
                    validate_repaired_status(status, target)
                result["branches"][name] = {"verdict": "PASS", "physical": physical, "bitrot": bitrot,
                    "s3": comparison, "ranges": ranges, "trigger": "manual-normal" if mode == 1 else "manual-deep"}
                if condition_proof is not None:
                    result["branches"][name]["conditions"] = condition_proof
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
    topologies = set(select(args.case_id)[0]["contract"]["probes"]["topology"].split(","))
    fixtures = ("DB", "DV") if args.case_id == "heal.admin-dry-run" else ("DB",)
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            if topology not in topologies:
                continue
            for fixture in fixtures:
                scoped_args = argparse.Namespace(**vars(args))
                if len(fixtures) > 1:
                    scoped_args.evidence = args.evidence / fixture
                    scoped_args.evidence.mkdir(exist_ok=True)
                results.append(run_topology(scoped_args, topology, selected, fixture=fixture))
                if results[-1]["cleanup"] != "PASS":
                    break
            if results[-1]["cleanup"] != "PASS":
                break
    finally:
        complete = len(results) == len(topologies) * len(fixtures) and all(value["verdict"] == "PASS" and value["cleanup"] == "PASS" for value in results)
        save(args.evidence / "healthy-summary.json", {"caseId": args.case_id, "results": results,
             "cleanup": "PASS" if results and all(value["cleanup"] == "PASS" for value in results) else "FAIL", "complete": complete})
    require(complete, "Heal case requires every source H1/H2 branch; inspect topology-result evidence")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal shard validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
