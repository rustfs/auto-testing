#!/usr/bin/env python3
"""G01/G03 version races; a blocked native fence never certifies the case."""

from __future__ import annotations

import argparse
import copy
from concurrent.futures import ThreadPoolExecutor
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
    from .heal_workload import Recorder, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .shards import files, map_target, validate_repaired_physical
    from .cursor import validate_missing_batch, verify_part_manifest
    from .versions import public_version_id
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import files, map_target, validate_repaired_physical
    from cursor import validate_missing_batch, verify_part_manifest
    from versions import public_version_id


CASE_IDS = ("heal.race-unversioned-overwrite", "heal.race-versioned-put", "heal.race-version-delete", "heal.race-markers-and-batch-delete", "heal.race-multipart-complete-abort", "heal.race-copy-and-metadata-update")
CASE_BRANCHES = {
    "heal.race-unversioned-overwrite": ("overwrite-vs-deep", "overwrite-vs-read-repair", "native-commit-cleanup"),
    "heal.race-versioned-put": ("new-version-vs-historical-heal", "read-all-versions-during-heal", "native-version-fence"),
    "heal.race-version-delete": ("delete-old-version-vs-deep", "delete-current-version-vs-deep", "native-delete-fence"),
    "heal.race-markers-and-batch-delete": ("create-marker", "delete-marker", "batch-version-delete", "unversioned-delete", "native-marker-fence"),
    "heal.race-multipart-complete-abort": ("upload-continue", "complete", "abort", "native-mpu-fence"),
    "heal.race-copy-and-metadata-update": ("copy-source", "copy-destination", "replace-metadata", "update-tags", "native-copy-fence"),
}
G01_BRANCHES = CASE_BRANCHES["heal.race-unversioned-overwrite"][:2]
G02_BRANCHES = CASE_BRANCHES["heal.race-versioned-put"][:2]
G03_BRANCHES = CASE_BRANCHES["heal.race-version-delete"][:2]
G04_BRANCHES = CASE_BRANCHES["heal.race-markers-and-batch-delete"][:4]
G05_BRANCHES = CASE_BRANCHES["heal.race-multipart-complete-abort"][:3]
G06_BRANCHES = CASE_BRANCHES["heal.race-copy-and-metadata-update"][:4]
OBJECTS = 257
SIZE = 257 * 1024 + 123
TARGET_KEY = "objects/0256.bin"
REFERENCE_KEY = "objects/0000.bin"
G06_TARGET_KEY = "actual/slash/0256.bin"
G06_REFERENCE_KEY = "space key/0000.bin"
G05_TARGET_KEY = "multipart/target.bin"
G05_PART_SIZES = (5 * 1024 * 1024 + 17, 1024 * 1024 + 93, 1024 * 1024 + 47)
REFERENCE = {
    "candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "sourceFiles": {
        "rustfs/src/admin/handlers/heal.rs": "7e4007c29a8329ced8e48b88111400e79cffd845bd6ddc6243b79f3cd48e8199",
        "crates/heal/src/heal/channel.rs": "7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0"},
    "requested": {**BASE_OPTIONS, "recursive": True, "nolock": False, "pool": None, "set": None},
    "effective": {**BASE_OPTIONS, "recursive": True, "nolock": False, "pool": None, "set": None},
    "nativeFence": "pinned release has no reviewed e2e-test-hooks artifact or exact Admin commit/cleanup barrier",
}


class WindowNotObserved(ValueError):
    """No observable client overlap; this is not a product success."""


def target_key(branch):
    if branch in G05_BRANCHES: return G05_TARGET_KEY
    return G06_TARGET_KEY if branch in G06_BRANCHES else TARGET_KEY


def reference_key(branch):
    return G06_REFERENCE_KEY if branch in G06_BRANCHES else REFERENCE_KEY


class RaceRecorder(Recorder):
    """Keep unresolved mutations and exact SDK receipts after any assertion."""
    def __init__(self, *args, versioned, **kwargs):
        super().__init__(*args, **kwargs)
        self.versioned, self.pending, self.seen = versioned, None, set()
        self.marker_branch = None
        self.batch_expected_ids = set()

    def call(self, operation, params, body=None, **kwargs):
        self.pending = {"operation": operation, "params": copy.deepcopy(params)}
        result = super().call(operation, params, body, **kwargs)
        version = result.get("VersionId")
        if operation == "put_object":
            require(isinstance(result.get("ETag"), str) and result["ETag"]
                    and result.get("DeleteMarker") in {None, False}, "G01/G03 PUT ACK lacks ETag or changed kind")
            if self.versioned:
                require(isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int
                        and (params["Key"], version) not in self.seen, "G01/G03 PUT ACK has an invalid/reused VersionId")
                self.seen.add((params["Key"], version))
            else:
                require(version in {None, "null"}, "G01/G03 never-versioned PUT created a real version")
        elif operation == "delete_object":
            if self.marker_branch in G04_BRANCHES:
                known = {value for key, value in self.seen if key == params["Key"]}
                if params.get("VersionId") is None:
                    if self.versioned:
                        require(result.get("DeleteMarker") is True and isinstance(version, str)
                                and version not in known, "G04 marker creation ACK lacks a new marker identity")
                        self.seen.add((params["Key"], version))
                    else:
                        require(version in {None, "null"} and result.get("DeleteMarker") in {None, False},
                                "G04 unversioned DELETE ACK created an unexpected marker")
                else:
                    require(self.versioned and params["VersionId"] in known and version == params["VersionId"]
                            and result.get("DeleteMarker") is True,
                            "G04 marker removal ACK changed the selected marker identity")
            else:
                require(self.versioned and params.get("VersionId") in {
                    value for key, value in self.seen if key == params["Key"]}
                    and version == params["VersionId"] and result.get("DeleteMarker") in {None, False},
                    "G01/G03 DELETE ACK changed the exact known data-version identity")
        elif operation == "delete_objects" and self.marker_branch == "batch-version-delete":
            deleted = result.get("Deleted") or []
            errors = result.get("Errors") or []
            deleted_ids = {row.get("VersionId") for row in deleted}
            require(not errors and deleted_ids == self.batch_expected_ids
                    and all(isinstance(value, str) for value in deleted_ids),
                    "G04 batch DELETE ACK has errors or incomplete per-item receipts")
        self.pending = None
        return result


def settings(observed):
    require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
            "G01/G03 status has an unknown or failed HTTP response")
    value = observed["response"]
    require(value.get("summary") in {"running", "finished"}, "G01/G03 original task disappeared, stopped or failed")
    # Canonical JSON distinguishes bool/int and preserves raw null selectors.
    require(canonical(value.get("settings")) == canonical(REFERENCE["effective"]),
            "G01/G03 Admin nolock was not safely normalized or changed unrelated options")
    return value["settings"]


def sample_overlap(ack, observed, path, token):
    foreground = ack.get("observation") or {}
    settings(observed)
    value = observed["response"]
    a, z = foreground.get("callStartedMonotonicNs"), foreground.get("callFinishedMonotonicNs")
    b, c = observed.get("callStartedMonotonicNs"), observed.get("callFinishedMonotonicNs")
    processed = (value.get("outcome") or {}).get("counters", {}).get("processed")
    return (foreground.get("acknowledged") is True and foreground.get("clientCallInvoked") is True
            and foreground.get("clockDomain") and foreground["clockDomain"] == observed.get("clockDomain")
            and observed.get("path") == path and [tuple(row) for row in observed.get("query", [])] == [("clientToken", token)]
            and all(type(tick) is int for tick in (a, z, b, c)) and 0 < a < b < c < z
            and value["summary"] == "running" and type(processed) is int and processed > 0)


def seed_fixture(recorder, bucket, branch, *, count=OBJECTS):
    require(branch in (*G01_BRANCHES, *G02_BRANCHES, *G03_BRANCHES, *G04_BRANCHES, *G05_BRANCHES, *G06_BRANCHES) and type(count) is int and 3 <= count <= OBJECTS, "G01/G02/G03/G04/G05/G06 unregistered fixture")
    recorder.call("create_bucket", {"Bucket": bucket})
    if branch in G04_BRANCHES:
        versioned = branch != "unversioned-delete"
        if versioned:
            recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
        entries = []
        for index in range(count - 1):
            key = f"objects/{index:04d}.bin"
            body = payload(f"{bucket}/{key}/0", SIZE)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "G04", "nonce": str(index)},
                "Tagging": "suite=heal&fixture=G04"}, body)
            entries.append({"key": key, "versionId": public_version_id(response.get("VersionId")),
                "isLatest": True, "deleteMarker": False,
                "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
        target_versions = 1 if not versioned else 3
        for generation in range(target_versions):
            body = payload(f"{bucket}/{TARGET_KEY}/{generation}", SIZE)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": TARGET_KEY,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "G04", "generation": str(generation)},
                "Tagging": "suite=heal&fixture=G04"}, body)
            version = public_version_id(response.get("VersionId"))
            for row in entries:
                if row["key"] == TARGET_KEY: row["isLatest"] = False
            entries.append({"key": TARGET_KEY, "versionId": version, "isLatest": generation == target_versions - 1,
                "deleteMarker": False, "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
        if branch == "delete-marker":
            marker = recorder.call("delete_object", {"Bucket": bucket, "Key": TARGET_KEY})
            marker_id = marker.get("VersionId")
            for row in entries:
                if row["key"] == TARGET_KEY: row["isLatest"] = False
            entries.append({"key": TARGET_KEY, "versionId": marker_id, "isLatest": True, "deleteMarker": True})
        return entries
    if branch in G05_BRANCHES:
        entries = []
        for index in range(count - 1):
            key = f"objects/{index:04d}.bin"
            body = payload(f"{bucket}/{key}/0", SIZE)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "DM", "nonce": str(index)},
                "Tagging": "suite=heal&fixture=DM"}, body)
            entries.append({"key": key, "versionId": public_version_id(response.get("VersionId")),
                "isLatest": True, "deleteMarker": False,
                "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
        upload = recorder.call("create_multipart_upload", {"Bucket": bucket, "Key": G05_TARGET_KEY,
            "Metadata": {"heal-fixture": "DM", "multipart": "target"}, "ContentType": "application/octet-stream"})
        parts, digest = [], hashlib.sha256()
        for number, size in enumerate(G05_PART_SIZES, 1):
            body = payload(f"{bucket}/{G05_TARGET_KEY}/{number}", size); digest.update(body)
            response = recorder.call("upload_part", {"Bucket": bucket, "Key": G05_TARGET_KEY,
                "UploadId": upload["UploadId"], "PartNumber": number}, body)
            parts.append({"PartNumber": number, "ETag": response["ETag"]})
        completed = recorder.call("complete_multipart_upload", {"Bucket": bucket, "Key": G05_TARGET_KEY,
            "UploadId": upload["UploadId"], "MultipartUpload": {"Parts": parts}})
        entries.append({"key": G05_TARGET_KEY, "versionId": public_version_id(completed.get("VersionId")),
            "isLatest": True, "deleteMarker": False,
            "payload": {"size": sum(G05_PART_SIZES), "sha256": digest.hexdigest()}})
        return entries
    if branch in (*G02_BRANCHES, *G03_BRANCHES):
        recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    entries = []
    if branch in G06_BRANCHES:
        keys = [G06_REFERENCE_KEY] + [f"ds/{index:04d}.bin" for index in range(1, count - 1)] + [G06_TARGET_KEY]
    else:
        keys = [f"objects/{index:04d}.bin" for index in range(count - 1)] + [TARGET_KEY]
    current_target = target_key(branch)
    for key in keys:
        versions = 3 if key == current_target and branch in (*G02_BRANCHES, *G03_BRANCHES) else 1
        for generation in range(versions):
            nonce = f"{bucket}/{key}/{generation}"
            body = payload(nonce, SIZE)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "G01/G03", "nonce": nonce},
                "Tagging": "suite=heal&fixture=G01/G03"}, body)
            entries.append({"key": key, "versionId": public_version_id(response.get("VersionId")),
                "isLatest": generation == versions - 1, "deleteMarker": False,
                "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
    return entries


def operation_plan(bucket, branch, expected):
    require(branch in (*G01_BRANCHES, *G02_BRANCHES, *G03_BRANCHES, *G04_BRANCHES, *G05_BRANCHES, *G06_BRANCHES), "G01/G02/G03/G04/G05/G06 unregistered foreground branch")
    if branch in G01_BRANCHES:
        # Three distinct serial commits; the final independent reference was
        # written before injection, allowing a coding-index byte comparison.
        return [{"operation": "put_object", "params": {"Bucket": bucket, "Key": TARGET_KEY,
                    "ContentType": "application/octet-stream", "Metadata": {"nonce": str(index)},
                    "Tagging": "race=overwrite"},
                 "nonce": f"{bucket}/overwrite/{index}" if index < 2 else f"{bucket}/{REFERENCE_KEY}/0"}
                for index in range(3)]
    current_target = target_key(branch); current_reference = reference_key(branch)
    target = [row for row in expected if row["key"] == current_target]
    if branch in G02_BRANCHES:
        require(len(target) == 3 and len({row["versionId"] for row in target}) == 3, "G02 DV lacks three ACK-ordered versions")
        return [{"operation": "put_object", "params": {"Bucket": bucket, "Key": TARGET_KEY,
                    "ContentType": "application/octet-stream", "Metadata": {"race": "versioned-put", "nonce": str(index)},
                    "Tagging": "race=versioned-put"},
                 "nonce": f"{bucket}/versioned-put/{index}", "readVersionId": target[0]["versionId"],
                 "readPayload": target[0].get("payload")}
                for index in range(3)]
    if branch in G04_BRANCHES:
        target = [row for row in expected if row["key"] == TARGET_KEY]
        if branch == "create-marker":
            require(len(target) == 3 and target[-1]["isLatest"], "G04 create-marker fixture lacks current data")
            return [{"operation": "delete_object", "params": {"Bucket": bucket, "Key": TARGET_KEY},
                     "nonce": "create-marker"}]
        if branch == "delete-marker":
            marker = next((row for row in target if row["deleteMarker"] and row["isLatest"]), None)
            require(marker is not None, "G04 delete-marker fixture lacks an acknowledged current marker")
            return [{"operation": "delete_object", "params": {"Bucket": bucket, "Key": TARGET_KEY,
                                                                  "VersionId": marker["versionId"]},
                     "nonce": "delete-marker"}]
        if branch == "batch-version-delete":
            data = [row for row in target if not row["deleteMarker"]]
            require(len(data) == 3, "G04 batch fixture lacks three data versions")
            return [{"operation": "delete_objects", "params": {"Bucket": bucket,
                "Delete": {"Objects": [{"Key": TARGET_KEY, "VersionId": data[0]["versionId"]},
                                         {"Key": TARGET_KEY, "VersionId": data[1]["versionId"]}],
                            "Quiet": False}}, "nonce": "batch-version-delete"}]
        return [{"operation": "delete_object", "params": {"Bucket": bucket, "Key": TARGET_KEY},
                 "nonce": "unversioned-delete"}]
    if branch in G05_BRANCHES:
        # Each branch uses a fresh UploadId for the same already committed
        # object.  The foreground resolver binds every later request to the
        # exact create ACK and never guesses an UploadId from LIST output.
        if branch == "upload-continue":
            return [
                {"operation": "create_multipart_upload", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY},
                 "uploadName": "continue"},
                {"operation": "upload_part", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY,
                 "UploadIdRef": "continue", "PartNumber": 1}, "nonce": f"{bucket}/{G05_TARGET_KEY}/continue/1",
                 "size": G05_PART_SIZES[0]},
            ]
        if branch == "complete":
            return [
                {"operation": "create_multipart_upload", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY},
                 "uploadName": "complete"},
                *[{"operation": "upload_part", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY,
                    "UploadIdRef": "complete", "PartNumber": number},
                   "nonce": f"{bucket}/{G05_TARGET_KEY}/complete/{number}", "size": size}
                  for number, size in enumerate(G05_PART_SIZES, 1)],
                {"operation": "complete_multipart_upload", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY,
                 "UploadIdRef": "complete", "MultipartUpload": {"Parts": []}}, "uploadName": "completed"},
            ]
        return [
            {"operation": "create_multipart_upload", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY},
             "uploadName": "abort"},
            {"operation": "upload_part", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY,
             "UploadIdRef": "abort", "PartNumber": 1}, "nonce": f"{bucket}/{G05_TARGET_KEY}/abort/1",
             "size": G05_PART_SIZES[0]},
            {"operation": "abort_multipart_upload", "params": {"Bucket": bucket, "Key": G05_TARGET_KEY,
             "UploadIdRef": "abort"}, "uploadName": "aborted"},
        ]
    if branch in G06_BRANCHES:
        if branch == "copy-source":
            return [{"operation": "copy_object", "params": {"Bucket": bucket, "Key": current_target,
                "CopySource": {"Bucket": bucket, "Key": current_reference, "VersionId": "null"}}, "nonce": "copy-source"}]
        if branch == "copy-destination":
            return [{"operation": "copy_object", "params": {"Bucket": bucket, "Key": current_reference,
                "CopySource": {"Bucket": bucket, "Key": current_target, "VersionId": "null"}}, "nonce": "copy-destination"}]
        if branch == "replace-metadata":
            return [{"operation": "copy_object", "params": {"Bucket": bucket, "Key": current_target,
                "CopySource": {"Bucket": bucket, "Key": current_target, "VersionId": "null"},
                "MetadataDirective": "REPLACE", "Metadata": {"race": "replace-metadata"},
                "ContentType": "application/x-heal-race"}, "nonce": "replace-metadata"}]
        return [{"operation": "put_object_tagging", "params": {"Bucket": bucket, "Key": current_target,
            "Tagging": {"TagSet": [{"Key": "race", "Value": f"updated-{index:02d}"}]}},
            "nonce": f"tag-{index:02d}"} for index in range(16)]
    require(len(target) == 3 and len({row["versionId"] for row in target}) == 3
            and target[-1]["isLatest"], "G01/G03 DV lacks three ACK-ordered versions")
    return [{"operation": "delete_object", "params": {"Bucket": bucket, "Key": current_target,
                                                      "VersionId": (target[0] if branch == G03_BRANCHES[0] else target[-1])["versionId"]}}]


def final_inventory(baseline, branch, plan, responses):
    expected_length = (3 if branch in (*G01_BRANCHES, *G02_BRANCHES)
                       else len(plan) if branch in G05_BRANCHES
                       else 16 if branch == "update-tags" else 1)
    require(len(plan) == len(responses) == expected_length,
            "G01/G03/G05 foreground plan/ACK sequence is incomplete")
    result = copy.deepcopy(baseline)
    current_target, current_reference = target_key(branch), reference_key(branch)
    if branch in (*G01_BRANCHES, *G02_BRANCHES):
        entry = next(row for row in result["entries"] if row["key"] == current_target)
        step, ack = plan[-1], responses[-1]
        body = payload(step["nonce"], SIZE)
        require(isinstance(ack.get("ETag"), str) and ack["ETag"]
                and (branch in G02_BRANCHES or ack.get("VersionId") in {None, "null"}), "G01/G02/G03 last PUT ACK lost its identity")
        if branch in G01_BRANCHES:
            entry.update(payload={"size": len(body), "sha256": hashlib.sha256(body).hexdigest()},
                         tags=[{"Key": "race", "Value": "overwrite"}])
            entry["properties"].update(ETag=ack["ETag"], ContentLength=len(body), ContentType="application/octet-stream",
                                       Metadata=step["params"]["Metadata"])
        else:
            for row in result["entries"]:
                if row["key"] == current_target: row["isLatest"] = False
            result["entries"].append({"key": current_target, "versionId": public_version_id(ack.get("VersionId")),
                "isLatest": True, "deleteMarker": False,
                "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()},
                "properties": {"ETag": ack["ETag"], "ContentLength": len(body),
                                "ContentType": "application/octet-stream", "Metadata": step["params"]["Metadata"]},
                "tags": [{"Key": "race", "Value": "versioned-put"}]})
    elif branch in G04_BRANCHES:
        require(len(responses) == 1, "G04 foreground plan/ACK sequence is incomplete")
        target_rows = [row for row in result["entries"] if row["key"] == TARGET_KEY]
        if branch == "create-marker":
            marker = responses[0].get("VersionId")
            require(responses[0].get("DeleteMarker") is True and isinstance(marker, str),
                    "G04 marker creation ACK lacks its exact marker identity")
            for row in target_rows: row["isLatest"] = False
            result["entries"].append({"key": TARGET_KEY, "versionId": marker,
                                      "isLatest": True, "deleteMarker": True})
        elif branch == "delete-marker":
            marker = next(row for row in target_rows if row["deleteMarker"] and row["isLatest"])
            require(responses[0].get("DeleteMarker") is True and responses[0].get("VersionId") == marker["versionId"],
                    "G04 marker delete ACK changed the selected marker identity")
            result["entries"] = [row for row in result["entries"] if row is not marker]
            data = [row for row in result["entries"] if row["key"] == TARGET_KEY and not row["deleteMarker"]]
            require(data, "G04 marker delete removed every retained data version")
            for row in data: row["isLatest"] = row is data[-1]
        elif branch == "batch-version-delete":
            deleted = responses[0].get("Deleted") or []
            deleted_ids = {row.get("VersionId") for row in deleted}
            require(len(deleted_ids) == 2 and not responses[0].get("Errors"),
                    "G04 batch delete ACK is not a complete per-item success")
            result["entries"] = [row for row in result["entries"]
                                 if not (row["key"] == TARGET_KEY and row["versionId"] in deleted_ids)]
        else:
            require(responses[0].get("VersionId") in {None, "null"} and responses[0].get("DeleteMarker") in {None, False},
                    "G04 unversioned delete ACK changed object identity")
            result["entries"] = [row for row in result["entries"] if row["key"] != TARGET_KEY]
    elif branch in G04_BRANCHES:
        if branch == "unversioned-delete":
            require(len(original_target) == 1 and not current_target_values,
                    "G04 unversioned DELETE retained or replaced the committed target")
        else:
            require(len(original_target) >= 1 and len(current_target_values) >= 1,
                    "G04 marker operation lost the retained data generation")
            require(current_target_values[0]["dataDir"] in {target["dataDir"] for target in original_target},
                    "G04 marker operation changed the committed data generation")
    elif branch in G05_BRANCHES:
        runtime = plan[0].get("runtime") or {}
        upload_id = runtime.get("uploadIds", {}).get(branch.split("-", 1)[-1])
        require(isinstance(upload_id, str) and upload_id, "G05 foreground lost its exact UploadId")
        if branch == "complete":
            entry = next(row for row in result["entries"] if row["key"] == current_target)
            digest = hashlib.sha256()
            total = 0
            for number, size in enumerate(G05_PART_SIZES, 1):
                body = payload(f"{plan[0]['params']['Bucket']}/{G05_TARGET_KEY}/complete/{number}", size)
                digest.update(body); total += len(body)
            ack = responses[-1]
            require(isinstance(ack.get("ETag"), str) and ack["ETag"],
                    "G05 Complete ACK lacks its committed ETag")
            entry.update(payload={"size": total, "sha256": digest.hexdigest()},
                         versionId=public_version_id(ack.get("VersionId")), isLatest=True,
                         deleteMarker=False)
            entry.setdefault("properties", {}).update(ETag=ack["ETag"], ContentLength=total,
                                                        ContentType="application/octet-stream")
        else:
            require(branch in {"upload-continue", "abort"}, "G05 unknown MPU branch")
    elif branch in G06_BRANCHES:
        entry = next(row for row in result["entries"] if row["key"] == current_target)
        copy_result = responses[-1].get("CopyObjectResult") or {}
        copy_etag = copy_result.get("ETag") or responses[-1].get("ETag")
        if branch == "copy-source":
            reference = next(row for row in result["entries"] if row["key"] == current_reference)
            entry["payload"] = copy.deepcopy(reference["payload"])
            require(isinstance(copy_etag, str) and copy_etag, "G06 copy-source ACK lacks its committed ETag")
            entry["properties"].update(ETag=copy_etag)
        elif branch == "copy-destination":
            destination = next(row for row in result["entries"] if row["key"] == current_reference)
            destination["payload"] = copy.deepcopy(entry["payload"])
            require(isinstance(copy_etag, str) and copy_etag, "G06 copy-destination ACK lacks its committed ETag")
            destination["properties"].update(ETag=copy_etag)
        elif branch == "replace-metadata":
            require(isinstance(copy_etag, str) and copy_etag, "G06 metadata replacement ACK lacks its committed ETag")
            entry["properties"].update(Metadata=plan[0]["params"]["Metadata"], ContentType=plan[0]["params"]["ContentType"])
            entry["properties"]["ETag"] = copy_etag
        else:
            entry["tags"] = copy.deepcopy(plan[-1]["params"]["Tagging"]["TagSet"])
    else:
        version = plan[0]["params"]["VersionId"]
        require(responses[0].get("VersionId") == version, "G01/G03 DELETE response changed the selected VersionId")
        result["entries"] = [row for row in result["entries"]
                             if (row["key"], row["versionId"]) != (current_target, version)]
        require(len(result["entries"]) + 1 == len(baseline["entries"]), "G01/G03 deleted a foreign/missing version")
    return result


def object_scene(snapshot, bucket, key):
    return {node: {**value, "files": [row for row in value["files"]
                                    if row["path"].startswith(f"{bucket}/{key}/")]}
            for node, value in snapshot.items()}


def map_fixture(snapshot, bucket, expected):
    live_expected = [row for row in expected if not row.get("deleteMarker")]
    mapped = {(row["key"], row["versionId"]): map_target(
        object_scene(snapshot, bucket, row["key"]), bucket, row["key"], "data", 1,
        row["versionId"], latest_required=False) for row in live_expected}
    require(len(mapped) == len(live_expected), "G01/G04 physical map omitted/duplicated an ACK")
    return mapped


def physical_transition(before, after, bucket, mapped, final, branch):
    """Prove current ACK authority before allowing any GET or backup rescue."""
    require(all(before[node]["formats"] == after[node]["formats"] for node in before),
            "G01/G03 race changed an original disk format")
    known = {row["key"] for row in final["entries"]}
    require(all(any(row["path"].startswith(f"{bucket}/{key}/") for key in known)
                for value in after.values() for row in value["files"]), "G01/G03 created an unacknowledged physical key")
    current_target, current_reference = target_key(branch), reference_key(branch)
    for (key, version), target in mapped.items():
        if key == current_target: continue
        validate_repaired_physical(object_scene(before, bucket, key), object_scene(after, bucket, key), target)
    survivors = map_fixture(after, bucket, final["entries"])
    original_target = [value for (key, _), value in mapped.items() if key == current_target]
    current_target_values = [value for (key, _), value in survivors.items() if key == current_target]
    if branch in G01_BRANCHES:
        require(len(original_target) == len(current_target_values) == 1
                and original_target[0]["dataDir"] != current_target_values[0]["dataDir"],
                "G01/G03 overwrite retained the old physical generation")
        reference = next(target for (key, _), target in mapped.items() if key == current_reference)
        require({part["index"]: (part["sha256"], part["size"]) for part in reference["cohort"]}
                == {part["index"]: (part["sha256"], part["size"]) for part in current_target_values[0]["cohort"]},
                "G01/G03 last ACK shard bytes/coding indices differ from the independent payload reference")
    elif branch in G02_BRANCHES:
        require(len(original_target) == 3 and len(current_target_values) == 6,
                "G02 version race lost an acknowledged historical or new version")
        old_dirs = {target["dataDir"] for target in original_target}
        new_dirs = {target["dataDir"] for target in current_target_values} - old_dirs
        require(len(new_dirs) == 3 and old_dirs.isdisjoint(new_dirs),
                "G02 versions share or resurrect a physical data directory")
        require(all(any(part["size"] == SIZE for part in target["cohort"])
                    for target in current_target_values if target["dataDir"] in new_dirs),
                "G02 new version physical parts are incomplete")
    elif branch in G05_BRANCHES:
        require(len(original_target) == len(current_target_values) == 1,
                "G05 multipart transition changed the target identity count")
        if branch == "complete":
            require(original_target[0]["dataDir"] != current_target_values[0]["dataDir"],
                    "G05 Complete retained the old physical generation")
            require(len(current_target_values[0]["cohort"]) == len(original_target[0]["cohort"])
                    and all(part["size"] > 0 for part in current_target_values[0]["cohort"]),
                    "G05 Complete physical parts are incomplete")
        else:
            require(original_target[0]["dataDir"] == current_target_values[0]["dataDir"],
                    "G05 pending/aborted MPU changed the committed object generation")
            require([(part["index"], part["sha256"], part["size"]) for part in original_target[0]["cohort"]]
                    == [(part["index"], part["sha256"], part["size"]) for part in current_target_values[0]["cohort"]],
                    "G05 pending/aborted MPU changed committed object bytes")
    elif branch in G06_BRANCHES:
        changed_key = current_reference if branch == "copy-destination" else current_target
        target_after = next(value for (key, _), value in survivors.items() if key == changed_key)
        target_before = next(value for (key, _), value in mapped.items() if key == changed_key)
        if branch in {"copy-source", "copy-destination"}:
            source_key = current_reference if branch == "copy-source" else current_target
            source = next(value for (key, _), value in mapped.items() if key == source_key)
            require(target_after["dataDir"] != target_before["dataDir"]
                    and {part["index"]: (part["sha256"], part["size"]) for part in target_after["cohort"]}
                    == {part["index"]: (part["sha256"], part["size"]) for part in source["cohort"]},
                    "G06 copy did not publish a distinct target generation with source shard bytes")
        else:
            require(target_after["dataDir"] == target_before["dataDir"]
                    and [(part["index"], part["sha256"], part["size"]) for part in target_after["cohort"]]
                    == [(part["index"], part["sha256"], part["size"]) for part in target_before["cohort"]],
                    "G06 metadata/tag update changed the data generation")
    else:
        remaining = {(row["key"], row["versionId"]) for row in final["entries"]}
        removed = [value for pair, value in mapped.items() if pair not in remaining]
        require(len(removed) == 1, "G01/G03 delete transition lacks one exact deleted version")
        for target in current_target_values:
            original = mapped[current_target, target["versionId"]]
            require(target["logical"] == original["logical"]
                    and [(row["index"], row["sha256"], row["size"]) for row in target["cohort"]]
                    == [(row["index"], row["sha256"], row["size"]) for row in original["cohort"]],
                    "G01/G03 delete changed another retained ACK generation")
    current_dirs = {target["dataDir"] for target in current_target_values}
    for value in after.values():
        for row in value["files"]:
            prefix = f"{bucket}/{current_target}/"
            if not row["path"].startswith(prefix): continue
            if branch == "unversioned-delete":
                raise ValueError("G04 unversioned DELETE left a physical target leaf")
            suffix = row["path"][len(prefix):]
            require(suffix == "xl.meta" or suffix.split("/")[0] in current_dirs,
                    "G01/G03 resurrected an old/deleted data directory or left an unbound target leaf")
    return survivors, {"allControlShardsRestored": True, "lastACKGenerationVerified": True,
                        "physicalProofBeforeGETOrRescue": True, "oldVersionResurrected": False}


def terminal_proof(status, bucket, before, final):
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "finished" and outcome.get("execution", {}).get("state") == "completed"
            and outcome.get("coverage") == "complete", "G01/G03 task lacks complete successful terminal coverage")
    old = {(row["key"], row["versionId"]) for row in before["entries"]}
    new = {(row["key"], row["versionId"]) for row in final["entries"]}
    counters = outcome.get("counters") or {}
    require(all(type(counters.get(key)) is int and counters[key] >= 0
                for key in ("processed", "healed", "unchanged", "failed", "unknown", "skipped", "attemptFailures"))
            and all(counters[key] == 0 for key in ("failed", "unknown", "skipped", "attemptFailures"))
            and counters.get("overflowed") is False and counters["processed"] == counters["healed"] + counters["unchanged"]
            and len(old & new) <= counters["processed"] <= len(old | new),
            "G01/G03 terminal responsibility exceeds ACK bounds or hides unknown/failed work")
    receipts = outcome.get("objects")
    require(isinstance(receipts, list) and 0 < len(receipts) <= 128
            and outcome.get("objectsTruncated") is (counters["processed"] > 128),
            "G01/G03 confused truncated diagnostics with its independent complete ACK manifest")
    seen, incarnation = set(), None
    for receipt in receipts:
        identity = receipt.get("identity") or {}
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(identity.get("bucket") == bucket and pair in old | new and pair not in seen
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and isinstance(identity.get("bucketIncarnationId"), str) and identity["bucketIncarnationId"]
                and receipt.get("disposition", {}).get("state") in {"repaired", "verified_healthy"},
                "G01/G03 diagnostics changed version/incarnation/scope or claimed unknown/false health")
        incarnation = incarnation or identity["bucketIncarnationId"]
        require(incarnation == identity["bucketIncarnationId"], "G01/G03 diagnostics mix incarnations")
        seen.add(pair)
    return {"minimumACKResponsibilities": len(old & new), "maximumACKResponsibilities": len(old | new),
            "counters": counters, "exactRaceEnumerationClaimed": False}


def observe(probe, fleet, path, token):
    value = fleet.record("original-token-status", probe.admin(path, [("clientToken", token)], b""))
    settings(value)
    return value


def wait_running(probe, fleet, path, token, total):
    deadline = time.monotonic() + 30
    while True:
        observed = observe(probe, fleet, path, token)
        value = observed["response"]
        processed = (value.get("outcome") or {}).get("counters", {}).get("processed")
        if value["summary"] == "finished":
            raise WindowNotObserved("G01/G03 task finished before its partial-progress race window")
        if type(processed) is int and 0 < processed < total: return observed
        if time.monotonic() >= deadline:
            raise WindowNotObserved("G01/G03 nonzero partial progress was not observed within 30 seconds")
        time.sleep(.05)


def wait_terminal(probe, fleet, path, token):
    deadline = time.monotonic() + 240
    while True:
        current = observe(probe, fleet, path, token)
        if current["response"]["summary"] == "finished": return current["response"]
        require(time.monotonic() < deadline, "G01/G03 original task exceeded its terminal budget")
        time.sleep(.2)


def foreground(recorder, plan):
    responses = []
    upload_ids = {}
    part_etags = {}
    for step in plan:
        params = copy.deepcopy(step["params"])
        upload_ref = params.pop("UploadIdRef", None)
        if upload_ref is not None:
            require(upload_ref in upload_ids, "G05 foreground referenced an unacknowledged UploadId")
            params["UploadId"] = upload_ids[upload_ref]
        if step["operation"] == "complete_multipart_upload":
            parts = [{"PartNumber": number, "ETag": part_etags[(upload_ref, number)]}
                     for number, _ in enumerate(G05_PART_SIZES, 1)]
            params["MultipartUpload"] = {"Parts": parts}
        body = None
        if step["operation"] == "put_object":
            body = payload(step["nonce"], SIZE)
        elif step["operation"] == "upload_part":
            body = payload(step["nonce"], step["size"])
        response = recorder.call(step["operation"], params, body)
        responses.append(response)
        if step.get("uploadName") in {"continue", "complete", "abort"}:
            require(isinstance(response.get("UploadId"), str) and response["UploadId"],
                    "G05 CreateMultipartUpload ACK lacks UploadId")
            upload_ids[step["uploadName"]] = response["UploadId"]
        if step["operation"] == "upload_part":
            require(isinstance(response.get("ETag"), str) and response["ETag"],
                    "G05 UploadPart ACK lacks ETag")
            part_etags[(upload_ref, params["PartNumber"])] = response["ETag"]
    plan[0]["runtime"] = {"uploadIds": upload_ids, "partEtags": part_etags}
    return responses, {"uploadIds": upload_ids, "partEtags": part_etags}


def race(recorder, plan, probe, fleet, path, token):
    samples = []
    # Only the worker mutates Recorder state. All Fleet.record writes run on
    # the controller, so evidence sequence allocation never races.
    readers = []
    read_branch = bool(plan and plan[0].get("branch") in {"overwrite-vs-read-repair", "read-all-versions-during-heal"})
    with ThreadPoolExecutor(max_workers=2 if read_branch else 1) as pool:
        worker = pool.submit(foreground, recorder, plan)
        if read_branch:
            def read_repair():
                deadline = time.monotonic() + 90
                while not worker.done() and time.monotonic() < deadline:
                    params = {"Bucket": plan[0]["params"]["Bucket"], "Key": plan[0]["params"]["Key"]}
                    if plan[0].get("readVersionId"): params["VersionId"] = plan[0]["readVersionId"]
                    observed = probe.s3("get_object", params)
                    if plan[0].get("branch") == "read-all-versions-during-heal":
                        response = observed.get("response") or {}
                        require(observed.get("acknowledged") is True and not observed.get("resultUnknown")
                                and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
                                and response.get("VersionId") == params["VersionId"]
                                and observed.get("payload") == plan[0]["readPayload"],
                                "G02 exact historical read was not acknowledged with original bytes")
                    readers.append(observed)
                    time.sleep(.01)
            read_future = pool.submit(read_repair)
        else:
            read_future = None
        deadline = time.monotonic() + 120
        try:
            while not worker.done() and time.monotonic() < deadline:
                samples.append(observe(probe, fleet, path, token))
                time.sleep(.01)
        except BaseException:
            recorder.mutations.poison("status observation failed; drain in-flight mutations only")
            raise
        if not worker.done(): recorder.mutations.poison("foreground deadline; drain only")
        responses, runtime = worker.result(timeout=60)
        if plan and runtime:
            plan[0]["runtime"] = runtime
        if read_future is not None:
            read_future.result(timeout=60)
    acks = [json.loads(path.read_text()) for path in sorted(recorder.output.glob("*-ack.json"))]
    require(len(acks) == len(plan) and all(row.get("operationCheck") == "PASS" for row in acks),
            "G01/G03 lacks every durable foreground ACK")
    overlaps = [any(sample_overlap(ack, sample, path, token) for sample in samples) for ack in acks]
    fleet.record("read-repair-overlap-proof", {"branch": plan[0].get("branch") if plan else None,
        "readCalls": len(readers), "allReadsObserved": bool(readers) if read_branch else True,
        "nativeFenceProved": False})
    fleet.record("public-call-overlap-proof", {"everyMutationOverlapped": all(overlaps), "mutations": overlaps,
        "nativeFenceProved": False, "serverCommitOverlapProved": False})
    return responses, all(overlaps)


def verify_multipart_state(probe, fleet, bucket, branch, runtime, expected_parts):
    """Bind MPU control evidence to the foreground UploadId/part ACKs."""
    require(branch in G05_BRANCHES and isinstance(runtime, dict), "G05 runtime receipt is missing")
    name = branch.split("-", 1)[-1]
    upload_id = (runtime.get("uploadIds") or {}).get(name)
    require(isinstance(upload_id, str) and upload_id, "G05 runtime UploadId is missing")
    listed = fleet.record("G05-list-multipart-uploads", probe.s3("list_multipart_uploads", {"Bucket": bucket}))
    uploads = (listed.get("response") or {}).get("Uploads", [])
    matching = [row for row in uploads if row.get("UploadId") == upload_id]
    if branch == "upload-continue":
        require(len(matching) == 1, "G05 continued UploadId disappeared from ListMultipartUploads")
        parts = fleet.record("G05-list-parts", probe.s3("list_parts", {"Bucket": bucket, "Key": G05_TARGET_KEY,
                                                                        "UploadId": upload_id}))
        rows = (parts.get("response") or {}).get("Parts", [])
        require(len(rows) == len(expected_parts) and all(
            row.get("PartNumber") == number and row.get("ETag") == runtime["partEtags"][(name, number)]
            and row.get("Size") == size for row, (number, size) in zip(rows, expected_parts)),
            "G05 continued UploadId lost an acknowledged part")
    else:
        require(not matching, f"G05 {branch} left a completed/aborted UploadId visible")
        if branch == "abort":
            parts = fleet.record("G05-list-aborted-parts", probe.s3("list_parts", {"Bucket": bucket,
                                                                                    "Key": G05_TARGET_KEY,
                                                                                    "UploadId": upload_id}))
            response = parts.get("response") or {}
            require(parts.get("acknowledged") is False and parts.get("resultUnknown") is False
                    and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404,
                    "G05 aborted UploadId still exposes acknowledged parts")
    return {"uploadId": upload_id, "listedUploads": len(uploads), "branch": branch,
            "acknowledgedParts": len(expected_parts) if branch == "upload-continue" else 0}


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True, exist_ok=False)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="manual")
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] == REFERENCE["candidateCommit"], "G01/G03 candidate contract is not reviewed")
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 512, "maxInventoryBytes": 512 * 1024**2}
    fleet.record("binding", binding)
    current_target_key, current_reference_key = target_key(branch), reference_key(branch)
    fleet.record("source-contract", {**REFERENCE, "branch": branch, "objectCount": OBJECTS, "objectSize": SIZE,
        "requiredPUTs": 3 if branch in (*G01_BRANCHES, *G02_BRANCHES) else 0, "serverCommitOverlapProved": False})
    probe, active, recorder, writer = Probe(binding), [], None, None
    result = {"topology": topology, "branch": branch,
              "fixture": "DL" if branch in (*G01_BRANCHES, "unversioned-delete") else "DV" if branch in G04_BRANCHES
              else "DM" if branch in G05_BRANCHES
              else "DS" if branch in G06_BRANCHES else "DV",
              "verdict": "RUNNING", "cleanup": "PENDING", "canonicalCasePassClaimed": False}
    changed_target_registered, target_authority, mpu_runtime = None, False, None
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = RaceRecorder(probe, directory / "seed-ACKs",
                                versioned=branch in (*G02_BRANCHES, *G03_BRANCHES, *G04_BRANCHES[:3]))
        recorder.marker_branch = branch if branch in G04_BRANCHES else None
        expected = seed_fixture(recorder, bucket, branch)
        baseline = fleet.record("complete-baseline-S3", probe.inventory(
            bucket, unversioned_current_tags=branch in (*G01_BRANCHES, "unversioned-delete", *G05_BRANCHES, *G06_BRANCHES)))
        verify_seed(expected, baseline)
        plan = operation_plan(bucket, branch, expected)
        for step in plan:
            step["branch"] = branch
        fleet.record("frozen-foreground-plan", plan)
        fleet.stop()
        before = fleet.snapshot("complete-baseline-physical", quiescent=True)
        mapped = map_fixture(before, bucket, expected)
        verify_part_manifest(fleet, list(mapped.values()), "baseline-streaming", count=len(mapped))
        target_version = (next(entry for entry in expected if entry["key"] == current_target_key and entry["isLatest"])
                          if branch in (G02_BRANCHES[1], G03_BRANCHES[1])
                          else next(entry for entry in expected if entry["key"] == current_target_key))
        targets = [mapped[row["key"], row["versionId"]] for row in expected
                   if row["key"] != current_target_key or row["versionId"] == target_version["versionId"]]
        require(len(targets) == OBJECTS, "G01/G03 fault plan omitted a key or changed its source dimension")
        receipts = []
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "faultId": bounded_fault_id("G01/G03", branch, index)}
            fleet.faults.append((target["node"], params))
            if target["key"] == current_target_key: changed_target_registered = target["node"], params
            receipts.append(fleet.record("exact-missing-part", {"node": target["node"], **fleet.remote(
                target["node"], "apply-fault", **params, relativePath=target["relativePath"],
                expectedSha256=target["sha256"], kind="missing")}))
        faulted = fleet.snapshot("complete-fault-physical", quiescent=True)
        fleet.record("fault-batch-proof", validate_missing_batch(before, faulted, targets, receipts))
        fleet.start()
        require(physical_content(faulted) == physical_content(fleet.snapshot("attribution-before-START")),
                "NOT_OBSERVED: G01/G03 missing shards changed before manual admission")
        path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="")
        fleet.record("prepared-Admin-request", {"path": path, "query": [], "options": REFERENCE["requested"]})
        start = fleet.record("original-Admin-start", probe.admin(path, [], json.dumps(REFERENCE["requested"]).encode()))
        token = start.get("response", {}).get("clientToken") if isinstance(start.get("response"), dict) else None
        require(start.get("httpStatus") == 200 and isinstance(token, str) and token, "G01/G03 admission lacks an original token")
        active.append((path, token))
        wait_running(probe, fleet, path, token, len(expected))
        witness = fleet.snapshot("target-missing-before-foreground")
        target = next(target for target in targets if target["key"] == current_target_key)
        if (target["node"], target["disk"], target["relativePath"]) in files(witness):
            raise WindowNotObserved("G01/G03 old target already repaired before foreground mutations")
        writer = RaceRecorder(probe, directory / "foreground-ACKs",
                              versioned=branch in (*G02_BRANCHES, *G03_BRANCHES, *G04_BRANCHES[:3]))
        writer.marker_branch = branch if branch in G04_BRANCHES else None
        writer.seen.update((row["key"], row["versionId"]) for row in expected)
        if branch == "batch-version-delete":
            writer.batch_expected_ids = {item["VersionId"] for item in plan[0]["params"]["Delete"]["Objects"]}
        responses, overlapped = race(writer, plan, probe, fleet, path, token)
        mpu_runtime = plan[0].get("runtime") if branch in G05_BRANCHES else None
        if branch in G05_BRANCHES:
            verify_multipart_state(probe, fleet, bucket, branch, mpu_runtime,
                                    [(1, G05_PART_SIZES[0])] if branch != "complete" else list(enumerate(G05_PART_SIZES, 1)))
        final = fleet.record("ACK-derived-final-manifest", final_inventory(baseline, branch, plan, responses))
        terminal = wait_terminal(probe, fleet, path, token)
        active.remove((path, token))
        for _ in range(2):
            repeated = observe(probe, fleet, path, token)["response"]
            require(canonical(repeated.get("outcome")) == canonical(terminal.get("outcome"))
                    and repeated["summary"] == terminal["summary"], "G01/G03 terminal responsibility changed on repetition")
        # Collect physical evidence even if terminal counters fail. Old target
        # bytes are never rescued over a successful overwrite or exact delete.
        fleet.stop()
        after = fleet.snapshot("product-physical-before-GET-or-rescue", quiescent=True)
        survivors, physical = physical_transition(before, after, bucket, mapped, final, branch)
        fleet.record("last-ACK-physical-proof", physical)
        verify_part_manifest(fleet, list(survivors.values()), "final-streaming", count=len(survivors))
        target_authority = True
        # Every missing-part registration is now superseded by product output;
        # leaving it in Fleet.faults would make cleanup attempt a blind backup
        # rescue over the new ACK generation.
        fleet.faults.clear()
        fleet.record("obsolete-target-fault-retired", {"fault": changed_target_registered,
            "reason": "complete last-ACK physical authority proves the original generation obsolete",
            "oldBytesRestored": False, "backupRetainedUntilCleanup": True})
        fleet.start()
        for endpoint in binding["endpoints"]:
            current = fleet.record("all-peer-final-S3", {"endpoint": endpoint, **Probe(binding, endpoint).inventory(
                bucket, unversioned_current_tags=branch in (*G01_BRANCHES, "unversioned-delete", *G05_BRANCHES, *G06_BRANCHES))})
            compare_inventories(final, current)
            if branch in G03_BRANCHES:
                negative = fleet.record("all-peer-deleted-version-GET", Probe(binding, endpoint).s3("get_object", plan[0]["params"]))
                response = negative.get("response") or {}
                require(negative.get("acknowledged") is False and negative.get("resultUnknown") is False
                        and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
                        and response.get("Error", {}).get("Code") == "NoSuchVersion",
                        "G01/G03 deleted exact VersionId revived or failed for another reason")
        fleet.stop()
        require(physical_content(after) == physical_content(fleet.snapshot("physical-after-S3", quiescent=True)),
                "G01/G03 final GET changed the already-proven physical result")
        result["terminal"] = terminal_proof(terminal, bucket, baseline, final)
        if not overlapped: raise WindowNotObserved("G01/G03 did not observe every required SDK call overlap")
        result.update(verdict="PASS", physical=physical, publicCallOverlapObserved=True,
                      nativeFenceProved=False, serverCommitOverlapProved=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="NOT_OBSERVED" if isinstance(exc, WindowNotObserved) else "FAIL",
                      failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("retained-failure-scene", quiescent=True)
        except Exception as capture: fleet.record("failure-scene-error", {"error": str(capture)})
    finally:
        for path, token in active:
            try: fleet.record("cancel-original-token", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("cancel-error", {"error": str(exc)})
        if branch == "upload-continue" and mpu_runtime:
            try:
                fleet.start()
                upload_id = mpu_runtime["uploadIds"]["continue"]
                fleet.record("G05-cleanup-abort", probe.s3("abort_multipart_upload", {
                    "Bucket": bucket, "Key": G05_TARGET_KEY, "UploadId": upload_id}))
                fleet.record("G05-cleanup-abort-proof", verify_multipart_state(
                    probe, fleet, bucket, "abort", {"uploadIds": {"abort": upload_id}, "partEtags": {}}, []))
            except Exception as exc:
                fleet.record("G05-cleanup-abort-error", {"error": str(exc), "uploadId": mpu_runtime.get("uploadIds", {}).get("continue")})
                result["cleanup"] = "FAIL"
        unresolved = any(item is not None and item.pending is not None for item in (recorder, writer))
        mutated = writer is not None and writer.sequence > 0
        if unresolved or mutated and not target_authority:
            try: fleet.stop()
            except Exception as exc: fleet.record("retain-stop-error", {"error": str(exc)})
            fleet.record("last-ACK-rescue-required", {"state": "DIRTY", "namespacesRetained": True,
                "unresolvedMutation": unresolved, "lastACKPhysicalAuthorityVerified": target_authority,
                "originalGenerationBlindRestorationProhibited": True})
            result["cleanup"] = "FAIL"
        else:
            cleanup_ok = fleet.cleanup()
            result["cleanup"] = "FAIL" if result.get("cleanup") == "FAIL" or not cleanup_ok else "PASS"
        fleet.record("branch-result", result)
    return result


def native_blocked(candidate, topology, case_id):
    require(topology in {"H1", "H2"}, "G01/G03 native prerequisite lost its source topology")
    return {"caseId": case_id, "topology": topology, "branch": CASE_BRANCHES[case_id][-1], "verdict": "BLOCKED", "cleanup": "PASS",
            "candidateCommit": candidate["source_sha"], "canonicalCasePassClaimed": False,
            "reason": REFERENCE["nativeFence"], "nativeFenceProved": False, "fleetExecutionClaimed": False}


def complete_results(results, case_id):
    branches = CASE_BRANCHES[case_id]
    wanted = {(topology, branch) for topology in ("H1", "H2") for branch in branches}
    actual = {(row["topology"], row["branch"]) for row in results}
    require(len(actual) == len(results) and actual <= wanted, "G01/G03 duplicate or foreign topology/branch")
    return actual == wanted and all(row["verdict"] == row["cleanup"] == "PASS" for row in results) and all(
        row.get("nativeFenceProved") is True for row in results if row["branch"] == branches[-1])


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"G01/G03 interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=list(CASE_BRANCHES), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    case_id = args.case_id; branches = CASE_BRANCHES[case_id]
    require(select(case_id)[0]["variants"] == list(branches), "G01/G03 executor omitted a source branch")
    require(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha) and re.fullmatch(r"[0-9a-f]{40}", args.testing_commit)
            and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "G01/G03 immutable identities required")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes))
            and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "G01/G03 requires four distinct current Heal nodes")
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] == REFERENCE["candidateCommit"], "G01/G03 candidate contract not reviewed")
    results = [native_blocked(candidate, topology, case_id) for topology in ("H1", "H2")]
    try:
        stop = False
        for topology in ("H1", "H2"):
            for branch in branches[:-1]:
                row = run_branch(args, topology, nodes[:1] if topology == "H1" else nodes[:4], branch)
                row["caseId"] = case_id
                results.append(row)
                if row["cleanup"] != "PASS": stop = True; break
            if stop: break
    finally:
        complete = complete_results(results, case_id)
        save(args.evidence / "healthy-summary.json", {"caseId": case_id, "results": results, "complete": complete,
            "cleanup": "PASS" if all(row["cleanup"] == "PASS" for row in results) else "FAIL",
            "canonicalCasePassClaimed": False, "nativePrerequisitesBlocked": True})
    require(complete, "G01/G03 native prerequisites remain BLOCKED; every H1/H2 branch stays in the denominator")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"G01/G03 validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
