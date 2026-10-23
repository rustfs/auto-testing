#!/usr/bin/env python3
"""D03/J04: frozen version cohorts, recursive repair and repeat responsibility."""

from __future__ import annotations

import argparse
import copy
import gzip
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
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .boundaries import (BRANCHES as SIZE_BRANCHES, verify_size_contract, boundary_sizes,
                             seed_boundaries, verify_boundary_ranges)
    from .object_lock import BRANCHES as LOCK_BRANCHES, KEYS as LOCK_KEYS, seed_locked, prove_locked, cleanup_locked_fleet
    from .read_repair import BRANCHES as READ_BRANCHES, tracked_read_task
    from .drive_gate import validate_unwritable
    from .scanner_repair import BRANCHES as SCANNER_BRANCHES, SOURCE_TARGETS, JournalWindow, observe_task as scanner_task
    from .markers import (BRANCHES as MARKER_BRANCHES, INSTANCES as MARKER_INSTANCES, seed_markers,
                          marker_plan, prepare_stale, verify_markers, reveal_current)
    from .stale_rejoin import BRANCHES as STALE_BRANCHES, run_branch as stale_branch
    from .disk_loss import BRANCHES as DISK_LOSS_BRANCHES, run_branch as disk_loss_branch, info_disks
    from .replacement import BRANCHES as REPLACEMENT_BRANCHES, AUTOMATIC_BRANCHES as AUTO_REPLACEMENT_BRANCHES, GRACEFUL_BRANCHES, CRASH_BRANCHES, run_branch as replacement_branch
    from .shards import (SIZES, files, map_target, run_task, validate_fault,
        validate_coordination_rejection, validate_coordination_rejection_logs,
                         validate_repaired_physical, verify_cohort, verify_ranges)
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from boundaries import (BRANCHES as SIZE_BRANCHES, verify_size_contract, boundary_sizes,
                            seed_boundaries, verify_boundary_ranges)
    from object_lock import BRANCHES as LOCK_BRANCHES, KEYS as LOCK_KEYS, seed_locked, prove_locked, cleanup_locked_fleet
    from read_repair import BRANCHES as READ_BRANCHES, tracked_read_task
    from drive_gate import validate_unwritable
    from scanner_repair import BRANCHES as SCANNER_BRANCHES, SOURCE_TARGETS, JournalWindow, observe_task as scanner_task
    from markers import (BRANCHES as MARKER_BRANCHES, INSTANCES as MARKER_INSTANCES, seed_markers,
                         marker_plan, prepare_stale, verify_markers, reveal_current)
    from stale_rejoin import BRANCHES as STALE_BRANCHES, run_branch as stale_branch
    from disk_loss import BRANCHES as DISK_LOSS_BRANCHES, run_branch as disk_loss_branch, info_disks
    from replacement import BRANCHES as REPLACEMENT_BRANCHES, AUTOMATIC_BRANCHES as AUTO_REPLACEMENT_BRANCHES, GRACEFUL_BRANCHES, CRASH_BRANCHES, run_branch as replacement_branch
    from shards import (SIZES, files, map_target, run_task, validate_fault,
        validate_coordination_rejection, validate_coordination_rejection_logs,
                        validate_repaired_physical, verify_cohort, verify_ranges)


BRANCHES = {
    "heal.historical-versions": ("old-version-part", "current-version-part",
                                 "recursive-all-versions", "multiple-version-targets"),
    "heal.repeat-idempotency": ("normal-repeat", "deep-repeat", "repeat-after-restart", "versioned-repeat"),
    "heal.versioning-suspended-null": ("enabled-to-suspended", "null-overwrite", "null-delete-marker"),
    "heal.below-read-quorum": ("r-minus-1-unversioned", "r-minus-1-historical"),
    "heal.missing-or-corrupt-xlmeta": ("missing-xlmeta", "truncated-xlmeta", "corrupt-xlmeta", "inline-payload", "versioned-xlmeta"),
    "heal.object-metadata-and-tags": ("user-metadata", "content-headers", "versioned-tags"),
    "heal.multiple-shard-loss": ("loss-1-data", "loss-1-parity", "loss-2-through-m-data-parity"),
    "heal.special-keys-and-pagination": ("unicode-space-percent", "encoded-slash", "adjacent-prefixes", "multi-page-list"),
    "heal.object-size-boundaries": SIZE_BRANCHES,
    "heal.object-lock-retention": LOCK_BRANCHES,
    "heal.metadata-payload-mismatch": ("length-mismatch", "payload-mismatch", "insufficient-authority"),
    "heal.read-repair-attribution": READ_BRANCHES,
    "heal.disk-readonly-and-permission": ("read-only", "permission-denied"),
    "heal.scanner-cold-object-repair": SCANNER_BRANCHES,
    "heal.delete-marker-semantics": MARKER_BRANCHES,
    "heal.stale-version-rejoin": STALE_BRANCHES,
    "heal.disk-loss-and-repair": DISK_LOSS_BRANCHES,
    "heal.disk-replacement-formatted": REPLACEMENT_BRANCHES,
    "heal.replacement-auto-rebuild": AUTO_REPLACEMENT_BRANCHES,
    "heal.target-graceful-restart": GRACEFUL_BRANCHES,
    "heal.target-process-crash": CRASH_BRANCHES,
}
VERSION_SIZE = 5 * 1024**2 + 123
SEMANTIC_KEYS = ("中文/文件/metadata.bin", "space key/headers.bin", "literal%2Fslash/versioned.bin")
SPECIAL_KEYS = ("中文/空 格%25.bin", "space key/object.bin", "literal%2Fslash/object.bin",
                "literal/slash/object.bin", "a/object.bin", "ab/object.bin", "b/object.bin", "nested/a/b/object.bin")


def seed_mismatch(recorder, bucket):
    recorder.call("create_bucket", {"Bucket": bucket})
    recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    expected = []
    for branch in BRANCHES["heal.metadata-payload-mismatch"]:
        key = "mismatch/" + branch + ".bin"
        for nonce, size in enumerate((VERSION_SIZE, VERSION_SIZE, 2 * 1024**2 + 147)):
            body = payload(f"{bucket}/{key}/{nonce}", size)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "Metadata": {"heal-fixture": "metadata-payload-mismatch", "generation": str(nonce)},
                "Tagging": f"suite=heal&generation=v{nonce}"}, body)
            version = response.get("VersionId")
            require(version not in {None, "", "null"}, "mismatch fixture omitted a real version ACK")
            expected.append({"key": key, "versionId": version, "isLatest": nonce == 2, "deleteMarker": False,
                             "payload": {"size": size, "sha256": hashlib.sha256(body).hexdigest()}})
    return expected


def mismatch_plan(branch, snapshot, bucket, expected):
    key = "mismatch/" + branch + ".bin"
    selected = [entry for entry in expected if entry["key"] == key]
    require(len(selected) == 3 and len({entry["versionId"] for entry in selected}) == 3
            and all(not entry["deleteMarker"] for entry in selected), "mismatch fixture lacks three distinct data versions")
    target = choose_target(snapshot, bucket, selected[0], "data")
    donor = choose_target(snapshot, bucket, selected[2 if branch == "length-mismatch" else 1], "data")
    wanted_size = selected[0]["payload"]["size"]
    require((selected[2]["payload"]["size"] != wanted_size) and selected[1]["payload"]["size"] == wanted_size,
            "mismatch donor sizes do not distinguish length and payload faults")
    erasure = target["logical"]["erasure"]
    require(erasure == donor["logical"]["erasure"], "mismatch donor has a different EC layout")
    read = erasure["data_blocks"]
    ordered = sorted(target["cohort"], key=lambda shard: shard["index"])
    negative = branch == "insufficient-authority"
    replaced = ordered[read - 1:] if negative else [target]
    faults = []
    for shard in replaced:
        source = next(item for item in donor["cohort"] if (item["node"], item["disk"]) == (shard["node"], shard["disk"]))
        require(source["index"] == shard["index"] and source["role"] == shard["role"]
                and source["dataDir"] != shard["dataDir"] and source["sha256"] != shard["sha256"]
                and (source["size"] != shard["size"] if branch == "length-mismatch" else source["size"] == shard["size"]),
                "mismatch donor is not the frozen same-role distinct-generation part")
        faults.append({**target, **shard, "donorRelativePath": source["relativePath"], "donorSha256": source["sha256"],
                       "donorSize": source["size"]})
    return {"scope": {"bucket": bucket, "key": key}, "recursive": True, "expected": selected,
            "faults": faults, "proofs": [target], "donorProof": donor, "kind": "replace-part",
            "requiredRepairs": set() if negative else {version_identity(target)}, "metadataRepairs": set(),
            "negative": negative, "readQuorum": read, "retainedShards": ordered[:read - 1] if negative else []}


def seed_special_keys(recorder, bucket):
    recorder.call("create_bucket", {"Bucket": bucket})
    expected = []
    keys = [*SPECIAL_KEYS, *(f"pages/{index:04d}.bin" for index in range(257))]
    for key in keys:
        body = payload(f"{bucket}/{key}/0", VERSION_SIZE if key in SPECIAL_KEYS else 128)
        response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
            "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "DS",
                "logical-key-sha256": hashlib.sha256(key.encode()).hexdigest()}}, body)
        require(response.get("VersionId") in {None, "null"}, "special-key fixture unexpectedly became versioned")
        expected.append({"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
            "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
    require(len(keys) == len(set(keys)) > 257, "special keys collided with the pagination fixture")
    return expected


def verify_pagination(inventory):
    pagination = inventory.get("pagination") or {}
    entries = inventory["entries"]
    require(pagination.get("requestedPageSize") == 17 and len(entries) > 257
            and {entry["key"] for entry in entries}.issuperset(SPECIAL_KEYS),
            "special-key inventory omitted its explicit pagination or distinct logical keys")
    for name in ("versionPageSizes", "currentPageSizes"):
        pages = pagination.get(name, [])
        require(len(pages) > 1 and all(type(count) is int and 0 <= count <= 17 for count in pages)
                and sum(pages) == len(entries), "multi-page LIST omitted or duplicated acknowledged objects")
    return {"maxKeys": 17, "versionPages": len(pagination["versionPageSizes"]),
            "currentPages": len(pagination["currentPageSizes"]), "acknowledgedObjects": len(entries)}


def semantic_body(bucket, key, nonce):
    return gzip.compress(payload(f"{bucket}/{key}/{nonce}", VERSION_SIZE), mtime=0)


def seed_semantics(recorder, bucket):
    recorder.call("create_bucket", {"Bucket": bucket})
    recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    expected, public = [], []
    for key in SEMANTIC_KEYS:
        for nonce in range(3):
            body = semantic_body(bucket, key, nonce)
            metadata = {"UserKey": quote(f"中文值-{nonce}", safe=""), "generation": str(nonce)}
            headers = {"ContentType": "application/octet-stream", "ContentEncoding": "gzip",
                       "ContentDisposition": "attachment; filename*=UTF-8''" + quote("中文文件.bin", safe=""),
                       "CacheControl": "no-store"}
            color = "blue" if nonce % 2 == 0 else "red"
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key, "Metadata": metadata,
                **headers, "Tagging": f"color={color}&generation=v{nonce}"}, body)
            require(response.get("VersionId") not in {None, "null"}, "semantic fixture omitted its precise historical version")
            expected.append({"key": key, "versionId": response["VersionId"], "deleteMarker": False, "isLatest": nonce == 2,
                "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
            public.append({"key": key, "versionId": response["VersionId"],
                "properties": {**headers, "Metadata": {name.lower(): value for name, value in metadata.items()}},
                "tags": [{"Key": "color", "Value": color}, {"Key": "generation", "Value": f"v{nonce}"}],
                "payloadEncoding": "actual deterministic gzip bytes; no server-side compression claim"})
    return expected, public


def verify_semantics(public, baseline):
    for spec in public:
        entry = next((entry for entry in baseline["entries"] if version_identity(entry) == version_identity(spec)), None)
        require(entry is not None and entry["tags"] == spec["tags"]
                and all(entry["properties"].get(name) == value for name, value in spec["properties"].items()),
                "semantic baseline lost normalized user metadata, actual encoding headers or exact versioned tags")
    return {"versionedTagSetsAndPublicHeadersVerified": True, "expected": public}


def version_identity(entry):
    return entry["key"], public_version_id(entry.get("versionId"))


def public_version_id(value):
    # The candidate's typed Admin identity serializes Uuid::nil(), whereas
    # the S3 identity and ACK ledger expose that same null version as "null".
    # Normalize only those representations; every non-null UUID stays exact.
    return "null" if value in {None, "", "null", "00000000-0000-0000-0000-000000000000"} else value


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
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
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


def verify_quorum_contract(manifest, candidate):
    contract = manifest.get("quorumContract") or {}
    extractor = Path(__file__).with_name("prepare_heal_meta_inspector.py")
    require(contract.get("recognized") is True and contract.get("candidateCommit") == candidate["source_sha"]
            and contract.get("readQuorumRule") == "decoded-data-blocks"
            and contract.get("extractorSourceSha256") == hashlib.sha256(extractor.read_bytes()).hexdigest()
            and set(contract.get("sourceFiles", {})) == {"rustfs/src/error.rs", "crates/ecstore/src/set_disk/metadata.rs"}
            and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in contract["sourceFiles"].values())
            and contract.get("unrecoverableGet", {}).get("httpStatus") == 503
            and isinstance(contract.get("unrecoverableGet", {}).get("code"), str),
            "negative quorum case requires a fresh recognized exact-candidate contract artifact")
    return contract


def validate_unrecoverable_get(observation, contract):
    expected = contract["unrecoverableGet"]
    response = observation.get("response") or {}
    require(observation.get("acknowledged") is False and observation.get("resultUnknown") is False
            and "payload" not in observation
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == expected["httpStatus"]
            and response.get("Error", {}).get("Code") == expected["code"],
            "below-quorum GET returned data, an unknown transport outcome or a different frozen S3 error")


def validate_failed_version(status, bucket, expected, target):
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "stopped" and status.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed_with_errors"
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False,
            "negative Heal task hid failed execution or incomplete responsibility")
    objects = outcome.get("objects", [])
    wanted = {version_identity(entry): entry for entry in expected}
    require(version_identity(target) in wanted and len(objects) == len(wanted), "negative Heal omitted a version or marker")
    actual = set()
    for receipt in objects:
        identity = receipt.get("identity", {})
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(pair in wanted and pair not in actual and identity.get("bucket") == bucket
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0,
                "negative Heal attributed the failure to another version or set")
        disposition = receipt.get("disposition", {})
        if pair == version_identity(target):
            require(disposition.get("state") == "failed"
                    and disposition.get("details") in {"recoverable", "retry_exhausted", "permanent"},
                    "unrecoverable version was reported repaired, healthy, absent, skipped or unknown")
        else:
            require(disposition.get("state") == ("metadata_healthy" if wanted[pair]["deleteMarker"] else "verified_healthy"),
                    "negative Heal lost another historical version's positive integrity")
        actual.add(pair)
    require(actual == set(wanted), "negative Heal changed the responsibility set")
    wanted_counters = {"processed": len(wanted), "healed": 0, "unchanged": len(wanted) - 1,
                       "failed": 1, "skipped": 0, "unknown": 0, "overflowed": False}
    counters = outcome.get("counters", {})
    require(all(type(counters.get(name)) is type(value) and counters[name] == value
                for name, value in wanted_counters.items())
            and type(counters.get("attemptFailures")) is int and counters["attemptFailures"] >= 1,
            "negative Heal failure counters hide attempts or claim repair")
    return {"failedVersion": version_identity(target), "completeResponsibility": True,
            "attemptFailures": counters["attemptFailures"], "repairPassClaimed": False}


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


def validate_all_members_online(observed, snapshot):
    topology = validate_topology(snapshot, "H1" if len(snapshot) == 1 else "H2")
    indexed = info_disks(observed, topology["independentDrives"])
    require(len({disk["endpoint"] for disk in indexed.values()}) == len(indexed),
            "multi-loss Admin members reuse a physical endpoint")
    members = []
    for slot, expected_uuid in enumerate(topology["diskUUIDs"]):
        disk = indexed[0, 0, slot]
        require(disk.get("uuid") in {None, "", expected_uuid},
                "multi-loss Admin slot differs from the frozen format UUID")
        members.append({"slot": [0, 0, slot], "uuid": disk.get("uuid"), "endpoint": disk["endpoint"],
                        "state": disk.get("state"), "runtimeState": disk.get("runtimeState")})
    online = all(member["uuid"] == topology["diskUUIDs"][member["slot"][2]]
                 and member["state"] == "ok" and member["runtimeState"] in {None, "online"}
                 for member in members)
    return {"allFormattedMembersOnline": online, "members": members}


def wait_all_members_online(probe, fleet, snapshot, label):
    # Read readiness only proves quorum. Poll complete member views before the
    # single admitted repair; no task or fixture operation is retried here.
    deadline = time.monotonic() + 90
    while True:
        views = {}
        for endpoint in probe.binding["endpoints"]:
            require(time.monotonic() < deadline,
                    "NOT_OBSERVED: multi-loss full-member startup exceeded 90 seconds")
            reader = Probe(probe.binding, endpoint)
            observed = fleet.record("multi-loss-members-" + label + "-" + endpoint,
                reader.admin("/rustfs/admin/v3/info", [], b"", method="GET"))
            views[endpoint] = validate_all_members_online(observed, snapshot)
        proof = fleet.record("multi-loss-members-proof-" + label,
            {"views": views, "allEndpointViewsOnline": all(
                view["allFormattedMembersOnline"] for view in views.values()),
             "observationOnly": True, "healTaskRetried": False})
        if proof["allEndpointViewsOnline"]:
            require(time.monotonic() < deadline,
                    "NOT_OBSERVED: multi-loss full-member startup exceeded 90 seconds")
            return proof
        remaining = deadline - time.monotonic()
        require(remaining > 0, "NOT_OBSERVED: multi-loss full-member startup exceeded 90 seconds")
        time.sleep(min(2, remaining))


def multi_loss_instances(snapshot, bucket, expected, fixture):
    sample = next(entry for entry in expected if not entry["deleteMarker"])
    target = choose_target(snapshot, bucket, sample, "data")
    geometry = target["logical"]["erasure"]
    data, parity = geometry["data_blocks"], geometry["parity_blocks"]
    require(data > 0 and parity > 0 and data + parity == len(target["cohort"]),
            "multi-loss matrix needs a complete same-version EC authority")
    instances = []
    for loss in range(1, parity + 1):
        for data_loss in range(max(0, loss - parity), min(loss, data) + 1):
            source_branch = ("loss-1-data" if data_loss else "loss-1-parity") if loss == 1 else "loss-2-through-m-data-parity"
            instances.append({"name": f"{source_branch}-n{loss}-d{data_loss}-p{loss - data_loss}",
                "sourceBranch": source_branch, "loss": loss, "dataLoss": data_loss, "parityLoss": loss - data_loss,
                "key": f"objects/{len(instances):04d}.bin", "fixture": fixture,
                "readQuorum": data, "remainingSameVersionShards": data + parity - loss,
                "writeCommitCondition": "all physical members online; only selected part files isolated"})
    require({entry["sourceBranch"] for entry in instances} == set(BRANCHES["heal.multiple-shard-loss"]),
            "actual EC geometry cannot cover every required multi-loss branch")
    require(len({entry["key"] for entry in instances}) == len(instances)
            and all(entry["remainingSameVersionShards"] >= data for entry in instances),
            "multi-loss matrix reused a fixture or crossed below its read quorum")
    return instances


def multi_loss_plan(instance, snapshot, bucket, expected, fixture):
    selected = [entry for entry in expected if entry["key"] == instance["key"]]
    data = [entry for entry in selected if not entry["deleteMarker"]]
    require(len(data) == (3 if fixture == "DV" else 1), "multi-loss instance lacks its fresh ACK cohort")
    target = choose_target(snapshot, bucket, data[0], "data")
    cohort = sorted(target["cohort"], key=lambda shard: shard["index"])
    missing = [shard for shard in cohort if shard["role"] == "data"][:instance["dataLoss"]]
    missing += [shard for shard in cohort if shard["role"] == "parity"][:instance["parityLoss"]]
    require(len(missing) == instance["loss"] and len(cohort) - len(missing) >= instance["readQuorum"],
            "actual multi-loss target roles or quorum differ from the frozen parameter matrix")
    faults = [{**target, **shard} for shard in missing]
    return {"scope": {"bucket": bucket, "key": instance["key"]}, "recursive": fixture == "DV",
            "expected": selected, "faults": faults, "proofs": faults,
            "requiredRepairs": {version_identity(data[0])}, "metadataRepairs": set(), "parameters": instance}


def map_inline_target(snapshot, bucket, entry, *, role="data", allow_empty=False):
    validate_topology(snapshot, "H1" if len(snapshot) == 1 else "H2")
    indexed, cohort, authority = files(snapshot), [], None
    path = f"{bucket}/{entry['key']}/xl.meta"
    for node, observation in snapshot.items():
        for physical in observation["formats"]:
            file = indexed.get((node, physical["disk"], path), {})
            metadata = file.get("metadata", {})
            versions = [version for version in metadata.get("versions", [])
                        if (version.get("versionId") or "null") == entry["versionId"]]
            require(metadata.get("bucket") == bucket and metadata.get("key") == entry["key"] and len(versions) == 1,
                    "inline baseline lacks an exact full-set metadata authority")
            version = versions[0]
            inline, erasure = version.get("inline") or {}, version["erasure"]
            layout = physical["value"]["xl"]
            slot = layout["sets"][0].index(layout["this"])
            empty = allow_empty and entry["payload"]["size"] == 0
            require(version.get("deleted") is False and version.get("size") == entry["payload"]["size"]
                    and erasure["index"] == erasure["distribution"][slot], "inline identity or physical role is unsupported")
            if empty:
                require(not inline and version["parts"] and all(part["size"] == part["actualSize"] == 0 for part in version["parts"])
                        and not any(identity[0] == node and identity[1] == physical["disk"]
                                    and identity[2].startswith(f"{bucket}/{entry['key']}/")
                                    and identity[2].rsplit("/", 1)[-1].startswith("part.") for identity in indexed),
                        "zero object unexpectedly has inline bytes or external payload parts")
            else:
                require(entry["payload"]["size"] > 0 and inline.get("rawOccurrenceCount") == 1
                        and type(inline.get("rawOffset")) is int and type(inline.get("length")) is int and inline["length"] > 32
                        and 0 <= inline["rawOffset"] < inline["rawOffset"] + inline["length"] <= file["size"]
                        and re.fullmatch(r"[0-9a-f]{64}", inline.get("sha256", "")),
                        "inline location or digest is unsupported")
            actual_role = "data" if erasure["index"] <= erasure["data_blocks"] else "parity"
            require(version.get("role") == actual_role, "inline erasure index and role differ")
            logical = {name: version.get(name) for name in ("versionId", "isLatest", "deleted", "dataDir", "size", "parts", "usesLegacyChecksum")}
            logical["erasure"] = {name: erasure[name] for name in ("data_blocks", "parity_blocks", "block_size", "distribution", "algorithm")}
            require(authority is None or logical == authority, "inline version or part geometry lacks full-set authority")
            authority = logical
            cohort.append({"node": node, "disk": physical["disk"], "relativePath": path,
                "versionId": entry["versionId"], "partNumber": 1, "dataDir": version.get("dataDir"),
                "diskUUID": layout["this"], "setSlot": slot, "role": actual_role, "index": erasure["index"],
                "sha256": file["sha256"], "size": file["size"], "inlineValue": inline})
    require(len(cohort) == authority["erasure"]["data_blocks"] + authority["erasure"]["parity_blocks"]
            and len({item["index"] for item in cohort}) == len(cohort), "inline set is incomplete or duplicated")
    target = next(item for item in cohort if item["role"] == role)
    return {**target, "bucket": bucket, "key": entry["key"], "poolIndex": 0, "setIndex": 0,
            "inlineProof": not empty, "zeroProof": empty, "cohort": cohort, "logical": authority}


def verify_proof(fleet, target, label):
    if target.get("zeroProof"):
        return fleet.record(label, {"zeroLengthMetadataAuthorityVerified": True, "payloadBytes": 0,
            "bitrotBlocksApplicable": False, "physicalPartInvented": False,
            "proof": "decoded complete same-version metadata; physical no-part baseline; independent post-repair oracle"})
    if not target.get("inlineProof"):
        return verify_cohort(fleet, target, label)
    proofs = []
    for entry in target["cohort"]:
        proof = fleet.record(label, {"node": entry["node"], **fleet.remote(entry["node"], "verify-inline",
            disk=entry["disk"], relativePath=entry["relativePath"], versionId=entry["versionId"])})
        require(proof.get("physicalBitrotVerified") is True and proof.get("inlineLength") == entry["inlineValue"]["length"]
                and proof.get("inlineSha256") == entry["inlineValue"]["sha256"],
                "recovered inline shard differs from frozen same-role bytes")
        proofs.append(proof)
    return {"allInlineShardsBitrotVerified": True, "originalInlineSha256Restored": True, "shards": proofs}


def boundary_plan(instance, snapshot, bucket, expected):
    selected = [entry for entry in expected if entry["key"] == instance["key"]]
    require(len(selected) == 1 and selected[0]["payload"]["size"] == instance["size"], "boundary lost its ACK size")
    shape, kind = instance["expectedShape"], instance["kind"]
    target = choose_target(snapshot, bucket, selected[0], instance["role"]) if shape == "external" else map_inline_target(
        snapshot, bucket, selected[0], role=instance["role"], allow_empty=shape == "zero-metadata")
    offset = None
    if shape == "inline" and kind == "bitflip":
        offset = target["inlineValue"]["rawOffset"] + 32
    if shape == "zero-metadata" and kind == "bitflip":
        kind = "header"
    metadata = shape != "external" and not (shape == "inline" and instance["kind"] == "bitflip")
    return {"scope": {"bucket": bucket, "key": instance["key"]}, "recursive": False,
            "expected": selected, "faults": [target], "proofs": [target], "kind": kind, "offset": offset,
            "requiredRepairs": set() if metadata else {version_identity(selected[0])},
            "metadataRepairs": {instance["key"]} if metadata else set(), "parameters": instance}


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
    if case_id == "heal.scanner-cold-object-repair":
        key = f"objects/{8 + 9 * SCANNER_BRANCHES.index(branch):04d}.bin"
        selected = [entry for entry in expected if entry["key"] == key]
        require(len(selected) == 1 and selected[0]["versionId"] == "null", "scanner fixture lacks a unique cold target")
        target = choose_target(snapshot, bucket, selected[0], "parity" if branch == "cold-bitrot" else "data")
        return {"scope": {"bucket": bucket, "key": key}, "recursive": False, "expected": selected,
                "faults": [target], "proofs": [target], "requiredRepairs": {version_identity(target)},
                "metadataRepairs": set(), "kind": "bitflip" if branch == "cold-bitrot" else "missing",
                "offset": 32 if branch == "cold-bitrot" else None}
    if case_id == "heal.disk-readonly-and-permission":
        key = f"objects/{8 + 9 * BRANCHES[case_id].index(branch):04d}.bin"
        selected = [entry for entry in expected if entry["key"] == key]
        require(len(selected) == 1 and selected[0]["versionId"] == "null", "unwritable fixture lacks a unique unversioned target")
        target = choose_target(snapshot, bucket, selected[0], "data")
        return {"scope": {"bucket": bucket, "key": key}, "recursive": False, "expected": selected,
                "faults": [target], "proofs": [target], "requiredRepairs": {version_identity(target)},
                "metadataRepairs": set(), "driveGate": branch}
    if case_id == "heal.read-repair-attribution":
        key = "objects/0001.bin" if fixture == "DV" else f"objects/{8 + 9 * READ_BRANCHES.index(branch):04d}.bin"
        data = [entry for entry in expected if entry["key"] == key and not entry["deleteMarker"]]
        require(len(data) == (3 if fixture == "DV" else 1), "read target lacks its acknowledged version history")
        require(branch != "historical-version-read" or not data[0]["isLatest"], "historical read selected a current version")
        target = choose_target(snapshot, bucket, data[0], "data")
        return {"scope": {"bucket": bucket, "key": key}, "recursive": False, "expected": [data[0]],
                "faults": [target], "proofs": [target], "requiredRepairs": {version_identity(target)},
                "metadataRepairs": set(), "kind": "bitflip" if branch == "range-bitrot" else "missing",
                "offset": 32 if branch == "range-bitrot" else None}
    if case_id == "heal.metadata-payload-mismatch":
        return mismatch_plan(branch, snapshot, bucket, expected)
    if case_id == "heal.object-lock-retention":
        index = LOCK_BRANCHES.index(branch)
        key = LOCK_KEYS[index]
        selected = [entry for entry in expected if entry["key"] == key]
        require(len(selected) == 3 and all(not entry["deleteMarker"] for entry in selected), "protected fixture lost its three distinct versions")
        target = choose_target(snapshot, bucket, selected[0 if index == 3 else -1], "parity" if index == 1 else "data")
        metadata = index >= 2
        proofs = [target]
        if metadata:
            proofs = [choose_target(snapshot, bucket, entry, "data") for entry in selected]
            proofs = [{**item, **next(shard for shard in item["cohort"]
                       if (shard["node"], shard["disk"]) == (target["node"], target["disk"]))} for item in proofs]
        return {"scope": {"bucket": bucket, "key": key}, "recursive": True, "expected": selected,
                "faults": [metadata_target(snapshot, target)] if metadata else [target], "proofs": proofs,
                "requiredRepairs": set() if metadata else {version_identity(target)},
                "metadataRepairs": {key} if metadata else set()}
    branch_index = BRANCHES[case_id].index(branch)
    key = f"objects/{branch_index:04d}.bin" if fixture == "DV" else f"objects/{8 + 9 * branch_index:04d}.bin"
    if branch == "versioned-repeat":
        key = "objects/0000.bin"
    if case_id == "heal.missing-or-corrupt-xlmeta":
        key = "objects/0000.bin" if fixture == "DV" else f"objects/{branch_index + 1:04d}.bin"
    if case_id == "heal.object-metadata-and-tags":
        key = SEMANTIC_KEYS[branch_index]
    if case_id == "heal.special-keys-and-pagination":
        key = SPECIAL_KEYS[(0, 2, 4, 7)[branch_index]]
    selected = [entry for entry in expected if entry["key"] == key]
    data = [entry for entry in selected if not entry["deleteMarker"]]
    if case_id == "heal.special-keys-and-pagination":
        require(len(data) == 1 and data[0]["versionId"] == "null", "special-key fault lost its exact logical identity")
        target = choose_target(snapshot, bucket, data[0], "parity" if branch == "adjacent-prefixes" else "data")
        metadata = branch in {"encoded-slash", "multi-page-list"}
        return {"scope": {"bucket": bucket, "key": key}, "recursive": False, "expected": selected,
                "faults": [metadata_target(snapshot, target) if metadata else target], "proofs": [target],
                "requiredRepairs": set() if metadata else {version_identity(data[0])},
                "metadataRepairs": {key} if metadata else set()}
    if case_id == "heal.object-metadata-and-tags":
        require(len(data) == 3, "metadata/tag fixture lacks three independently acknowledged versions")
        target = choose_target(snapshot, bucket, data[0] if branch == "user-metadata" else data[-1],
                               "parity" if branch == "versioned-tags" else "data")
        proofs, faults, required, metadata_keys = [target], [target], {version_identity(target)}, set()
        if branch == "content-headers":
            faults, required, metadata_keys = [metadata_target(snapshot, target)], set(), {key}
            proofs = [choose_target(snapshot, bucket, entry, "data") for entry in data]
            proofs = [{**item, **next(shard for shard in item["cohort"]
                       if (shard["node"], shard["disk"]) == (target["node"], target["disk"]))} for item in proofs]
        return {"scope": {"bucket": bucket, "key": key}, "recursive": True, "expected": selected,
                "faults": faults, "proofs": proofs, "requiredRepairs": required, "metadataRepairs": metadata_keys,
                "kind": {"user-metadata": "missing", "content-headers": "header", "versioned-tags": "bitflip"}[branch]}
    if case_id == "heal.missing-or-corrupt-xlmeta":
        if fixture == "DB":
            target = map_inline_target(snapshot, bucket, data[0])
            faults, proofs = [target], [target]
        else:
            target = choose_target(snapshot, bucket, data[0], "data")
            faults = [metadata_target(snapshot, target)]
            proofs = [choose_target(snapshot, bucket, entry, "data") for entry in data]
            proofs = [{**item, **next(shard for shard in item["cohort"]
                       if (shard["node"], shard["disk"]) == (target["node"], target["disk"]))} for item in proofs]
        kind = {"truncated-xlmeta": "truncate", "corrupt-xlmeta": "header", "inline-payload": "bitflip"}.get(branch, "missing")
        offset = target["inlineValue"]["rawOffset"] + 32 if branch == "inline-payload" else None
        return {"scope": {"bucket": bucket, "key": key}, "recursive": fixture == "DV", "expected": selected,
                "faults": faults, "proofs": proofs, "requiredRepairs": {version_identity(data[0])} if branch == "inline-payload" else set(),
                "metadataRepairs": {key}, "kind": kind, "offset": offset}
    if case_id == "heal.below-read-quorum":
        target = choose_target(snapshot, bucket, data[0], "data")
        read_quorum = target["logical"]["erasure"]["data_blocks"]
        ordered = sorted(target["cohort"], key=lambda entry: entry["index"])
        retained, missing = ordered[:read_quorum - 1], ordered[read_quorum - 1:]
        require(len(retained) == read_quorum - 1 and missing, "below-quorum fault did not retain exactly R-1 authorities")
        return {"scope": {"bucket": bucket, "key": key}, "recursive": True, "expected": selected,
                "faults": [{**target, **shard} for shard in missing], "proofs": [target],
                "requiredRepairs": set(), "metadataRepairs": set(), "negative": True,
                "readQuorum": read_quorum, "retainedShards": retained}
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
    current = fleet.record(label, probe.inventory(bucket, unversioned_current_tags=fixture in {"DB", "DK"},
                                                   page_size=17 if fixture == "DK" else None))
    comparison = compare_inventories(baseline, current)
    if fixture == "DK":
        comparison["pagination"] = verify_pagination(current)
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
        nonce = entry.get("payloadNonce", nonce)
        body = semantic_body(bucket, entry["key"], nonce) if fixture == "DS" else payload(
            f"{bucket}/{entry['key']}/{nonce}", entry["payload"]["size"])
        ranges.append({"key": entry["key"], "versionId": entry["versionId"], "proof": verify_ranges(
            probe, fleet, bucket, entry["key"], version_id=entry["versionId"],
            expected_body=body)})
    return {"inventory": comparison, "historicalRanges": ranges}


def run_topology(args, topology, nodes, fixture):
    directory = args.evidence / fixture / topology
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    target_restart = args.case_id in {"heal.target-graceful-restart", "heal.target-process-crash"}
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, resource_profile="cursor" if target_restart else "standard",
                  runtime_profile="scanner" if args.case_id == "heal.scanner-cold-object-repair" else
                  "read-repair" if args.case_id == "heal.read-repair-attribution" else
                  "replacement" if args.case_id == "heal.replacement-auto-rebuild" or target_restart else
                  "stale-rejoin" if args.case_id == "heal.stale-version-rejoin" else "manual")
    if target_restart:
        fleet.node_runtime_profiles = {node: "replacement-target" for node in nodes[1:]}
    candidate = json.loads(args.candidate.read_text())
    tool_manifest = fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    contract = verify_quorum_contract(tool_manifest, candidate) if args.case_id in {
        "heal.below-read-quorum", "heal.metadata-payload-mismatch", "heal.stale-version-rejoin",
        "heal.disk-loss-and-repair"} else None
    size_rule = verify_size_contract(tool_manifest, candidate) if args.case_id == "heal.object-size-boundaries" else None
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45 if size_rule or target_restart else 30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 2048 if target_restart else 512, "maxInventoryBytes": 2 * 1024**3 if target_restart else 512 * 1024**2}
    fleet.record("binding", binding)
    recipe = select(args.case_id)[0]
    require(list(BRANCHES[args.case_id]) == recipe["variants"], "executor omitted a source version branch")
    branches = [branch for branch in BRANCHES[args.case_id]
                if (args.case_id != "heal.repeat-idempotency" or (branch == "versioned-repeat") == (fixture == "DV"))
                and (args.case_id != "heal.below-read-quorum" or (branch == "r-minus-1-historical") == (fixture == "DV"))
                and (args.case_id != "heal.read-repair-attribution" or (branch == "historical-version-read") == (fixture == "DV"))
                and (args.case_id != "heal.missing-or-corrupt-xlmeta" or (branch == "versioned-xlmeta") == (fixture == "DV"))]
    if args.case_id == "heal.stale-version-rejoin":
        branches = [branch for branch in STALE_BRANCHES if (branch == "unversioned-overwrite") == (fixture == "DB")]
    if args.case_id == "heal.disk-replacement-formatted":
        branches = [branch for branch in REPLACEMENT_BRANCHES if (branch == "new-disk-data") == (fixture == "DB")]
    if args.case_id == "heal.replacement-auto-rebuild":
        branches = [branch for branch in AUTO_REPLACEMENT_BRANCHES if (branch == "replacement-unversioned") == (fixture == "DB")]
    if target_restart:
        branches = [branch for index, branch in enumerate(BRANCHES[args.case_id]) if (index == 0) == (fixture == "DB")]
    result = {"topology": topology, "fixture": fixture, "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    active, admissions = [], []
    probe = Probe(binding)
    lock_context = {} if args.case_id == "heal.object-lock-retention" else None
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        processes = fleet.snapshot("deployed-topology")
        fleet.record("topology-proof", validate_topology(processes, topology))
        if args.case_id == "heal.multiple-shard-loss":
            wait_all_members_online(probe, fleet, processes, "before-seed")
        scanner = args.case_id == "heal.scanner-cold-object-repair"
        if scanner:
            fleet.record("scanner-registered-budget", {"perBranchSeconds": 240, "sourceBranches": list(SCANNER_BRANCHES),
                "actualProcessConfiguration": {node: value["backgroundConfig"] for node, value in processes.items()},
                "otherAutomaticSourcesDisabled": True, "targetReadsAfterFaultBeforePhysicalProof": 0})
        recorder = Recorder(probe, directory / "fixture")
        public, instances, sizes = None, None, None
        if size_rule:
            expected = seed(recorder, bucket, "DB", 1, [VERSION_SIZE])
            calibration = fleet.snapshot("size-calibration")
            target = choose_target(calibration, bucket, expected[0], "data")
            sizes = boundary_sizes(size_rule, target, processes)
            fleet.record("candidate-size-contract", {"sourceContract": size_rule, "parameters": sizes})
            extra, instances = seed_boundaries(recorder, bucket, sizes)
            expected += extra
        elif lock_context is not None:
            expected = seed_locked(recorder, fleet, bucket, lock_context)
        elif args.case_id == "heal.metadata-payload-mismatch":
            expected = seed_mismatch(recorder, bucket)
        elif args.case_id == "heal.delete-marker-semantics":
            expected = seed_markers(recorder, bucket, VERSION_SIZE)
            instances = list(MARKER_INSTANCES)
        elif target_restart:
            expected = seed(recorder, bucket, fixture, 1025 if fixture == "DB" else 64, [1024**2 + 73])
            fleet.record("replacement-interruption-budget", {"dataObjects": 1025 if fixture == "DB" else 192, "objectBytes": 1024**2 + 73, "requiresActualPartialPhysicalProgress": True, "runtimeMinutes": 45})
        elif fixture == "DK":
            expected = seed_special_keys(recorder, bucket)
        elif fixture == "DS":
            expected, public = seed_semantics(recorder, bucket)
        else:
            expected = seed(recorder, bucket, fixture,
                            16 if args.case_id == "heal.multiple-shard-loss" else 4 if fixture == "DV" or args.case_id == "heal.stale-version-rejoin" else 128,
                            [VERSION_SIZE] if args.case_id in {"heal.multiple-shard-loss", "heal.stale-version-rejoin"} or fixture == "DV" else SIZES)
        if args.case_id == "heal.versioning-suspended-null":
            expected = suspend_fixture(recorder, bucket, expected)
        if args.case_id == "heal.stale-version-rejoin":
            for key in {entry["key"] for entry in expected}:
                for nonce, entry in enumerate(entry for entry in expected if entry["key"] == key and not entry["deleteMarker"]):
                    entry["payloadNonce"] = nonce
        baseline = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=fixture in {"DB", "DK"},
                                                              page_size=17 if fixture == "DK" else None))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        if lock_context is not None:
            fleet.record("lock-baseline-proof", prove_locked(probe, fleet, bucket, lock_context, label="lock-baseline"))
        if fixture == "DK":
            fleet.record("special-key-pagination-baseline", verify_pagination(baseline))
        if public is not None:
            fleet.record("semantic-baseline", verify_semantics(public, baseline))
        if scanner:
            healthy_before = fleet.snapshot("scanner-healthy-physical-before")
            healthy_window = JournalWindow(fleet, "scanner-healthy", event_targets=SOURCE_TARGETS)
            for branch in branches:
                plan = branch_plan(args.case_id, branch, healthy_before, bucket, expected, fixture)
                status = scanner_task(probe, fleet, healthy_window, bucket, plan["expected"][0], branch, healthy=True)
                fleet.record("scanner-healthy-responsibility", validate_version_receipts(status, bucket, plan["expected"], 2))
            require(physical_content(healthy_before) == physical_content(fleet.snapshot("scanner-healthy-physical-after")),
                    "healthy scanner preflight changed acknowledged object files")
        if args.case_id == "heal.multiple-shard-loss":
            fleet.stop()
            instances = multi_loss_instances(fleet.snapshot("multi-loss-geometry", quiescent=True), bucket, expected, fixture)
            fleet.record("multi-loss-parameter-matrix", {"instances": instances, "allLossCountsAndRoleCompositions": True})
            branches = [entry["name"] for entry in instances]
            fleet.start()
        if size_rule:
            branches = [entry["name"] for entry in instances]
            fleet.record("boundary-parameter-matrix", {"instances": instances, "sourceBranches": list(SIZE_BRANCHES)})
        if args.case_id == "heal.delete-marker-semantics":
            branches = [entry["name"] for entry in instances]
            fleet.record("marker-parameter-matrix", {"instances": instances, "sourceBranches": list(MARKER_BRANCHES)})
        restart_after_failure = False
        for branch in branches:
            registrations = []
            metadata_faults = {}
            gate_node, gated_proof = None, None
            stale_marker_started = False
            try:
                if lock_context is not None:
                    fleet.record("lock-before-fault", prove_locked(probe, fleet, bucket, lock_context,
                        key=LOCK_KEYS[LOCK_BRANCHES.index(branch)], label="lock-before-" + branch))
                if args.case_id == "heal.multiple-shard-loss":
                    if restart_after_failure:
                        fleet.start()
                        restart_after_failure = False
                    wait_all_members_online(probe, fleet, processes, "before-fault-" + branch)
                fleet.stop()
                if args.case_id in {"heal.disk-replacement-formatted", "heal.replacement-auto-rebuild", "heal.target-graceful-restart", "heal.target-process-crash"}:
                    stale_marker_started = True
                    result["branches"][branch] = replacement_branch(recorder, fleet, bucket, branch, expected, baseline, active, admissions)
                    stale_marker_started = False
                    continue
                if args.case_id == "heal.disk-loss-and-repair":
                    stale_marker_started = True  # ACKs may create a new authority; never restore an older fixture.
                    expected, baseline, proof = disk_loss_branch(recorder, fleet, bucket, branch, expected, active, admissions, contract)
                    result["branches"][branch] = proof
                    stale_marker_started = False
                    continue
                if args.case_id == "heal.stale-version-rejoin":
                    stale_marker_started = True
                    expected, baseline, proof = stale_branch(recorder, fleet, bucket, branch, expected, active, admissions, contract)
                    result["branches"][branch] = proof
                    stale_marker_started = False
                    continue
                before = fleet.snapshot("physical-before-" + branch, quiescent=True)
                scanner_window = JournalWindow(fleet, "scanner-fault-" + branch, event_targets=SOURCE_TARGETS) if scanner else None
                if size_rule:
                    plan = boundary_plan(next(entry for entry in instances if entry["name"] == branch), before, bucket, expected)
                elif args.case_id == "heal.delete-marker-semantics":
                    plan = marker_plan(next(entry for entry in instances if entry["name"] == branch), before, bucket, expected)
                elif instances:
                    plan = multi_loss_plan(next(entry for entry in instances if entry["name"] == branch), before, bucket, expected, fixture)
                else:
                    plan = branch_plan(args.case_id, branch, before, bucket, expected, fixture)
                fleet.record("fault-plan-" + branch, {**plan, "requiredRepairs": sorted(plan["requiredRepairs"]),
                                                     "metadataRepairs": sorted(plan["metadataRepairs"])})
                for target in plan["proofs"]:
                    verify_proof(fleet, target, "baseline-bitrot-" + branch)
                if plan.get("donorProof"):
                    verify_proof(fleet, plan["donorProof"], "baseline-donor-bitrot-" + branch)
                prior = before
                if plan.get("staleMarker"):
                    stale_marker_started = True
                    before, prior, expected, baseline = prepare_stale(recorder, fleet, bucket, plan, before, expected)
                for index, target in enumerate(plan["faults"]):
                    params = {"disk": target["disk"], "faultId": bounded_fault_id(recipe["code"], branch, index)}
                    registration = target["node"], params
                    fleet.faults.append(registration)
                    registrations.append(registration)
                    receipt = fleet.record("fault-" + branch, fleet.remote(target["node"], "apply-fault",
                        **params, relativePath=target["relativePath"], expectedSha256=target["sha256"],
                        kind=plan.get("kind", "missing"), offset=plan.get("offset"),
                        **({"donorRelativePath": target["donorRelativePath"], "donorSha256": target["donorSha256"]}
                           if plan.get("donorProof") else {})))
                    if plan.get("donorProof"):
                        require(receipt.get("donor") == {"relativePath": target["donorRelativePath"],
                            "sha256": target["donorSha256"], "size": target["donorSize"]}
                            and receipt.get("fault") == {"sha256": target["donorSha256"], "size": target["donorSize"]},
                            "replacement receipt did not witness the exact frozen donor bytes")
                    if target["relativePath"].endswith("/xl.meta") and receipt.get("fault"):
                        metadata_faults.setdefault(target["node"], {})[target["relativePath"]] = receipt["fault"]
                    witnessed = fleet.snapshot("physical-fault-" + branch, quiescent=True, metadata_faults=metadata_faults)
                    validate_fault(prior, witnessed, target, receipt)
                    prior = witnessed
                if plan.get("driveGate"):
                    gate_node = plan["proofs"][0]["node"]
                    fleet.drive_gates.add(gate_node)
                    fleet.record("drive-gate-armed", fleet.remote(gate_node, "drive-gate",
                        disk=plan["proofs"][0]["disk"], mode=plan["driveGate"]))
                fleet.start()
                if args.case_id == "heal.multiple-shard-loss":
                    wait_all_members_online(probe, fleet, before, "before-task-" + branch)
                if gate_node is not None:
                    fleet.record("service-write-denial", fleet.remote(gate_node, "drive-gate-probe"))
                witness = fleet.snapshot("physical-before-trigger-" + branch, metadata_faults=metadata_faults)
                if not scanner:
                    require(physical_content(prior) == physical_content(witness),
                            "ATTRIBUTION_NOT_OBSERVED: version fault changed before the manual token")
                if plan.get("negative"):
                    target = plan["proofs"][0]
                    fleet.record("quorum-contract", {"contract": contract, "readQuorum": plan["readQuorum"],
                        "availableSameVersionShards": len(plan["retainedShards"]), "poolIndex": 0, "setIndex": 0})
                    denied = fleet.record("negative-get", probe.s3("get_object", {
                        "Bucket": bucket, "Key": target["key"], "VersionId": target["versionId"]}))
                    validate_unrecoverable_get(denied, contract)
                    require(physical_content(prior) == physical_content(fleet.snapshot("physical-after-negative-get")),
                            "negative read widened damage or silently healed below-quorum fragments")
                admissions_before_attempt = len(admissions)
                if args.case_id == "heal.read-repair-attribution":
                    status = tracked_read_task(probe, fleet, bucket, branch, plan["expected"][0])
                elif scanner:
                    status = scanner_task(probe, fleet, scanner_window, bucket, plan["expected"][0], branch)
                else:
                    status = run_task(probe, fleet, plan["scope"], 2, active,
                                      option_overrides={"recursive": plan["recursive"]}, admissions=admissions,
                                      allow_coordination_rejection=(args.case_id == "heal.disk-readonly-and-permission"
                                                                    and branch == "permission-denied"))
                fleet.stop()
                repaired = fleet.snapshot("physical-after-" + branch, quiescent=True, metadata_faults=metadata_faults)
                if gate_node is not None:
                    require(physical_content(prior) == physical_content(repaired),
                            "unwritable window changed object files or wrote outside the target")
                    if status.get("admissionState") == "coordination-rejected":
                        rejection = validate_coordination_rejection(status, path=status.get("path"))
                        logs = {node: fleet.record("coordination-rejection-logs", {"node": node,
                            **fleet.remote(node, "logs")}) for node in fleet.nodes}
                        rejection["logProof"] = validate_coordination_rejection_logs(logs)
                        rejection["physicalUnchanged"] = True
                        gated_proof = rejection
                    else:
                        gated_proof = validate_unwritable(status, bucket, plan["expected"][0])
                    fleet.record("unwritable-responsibility", gated_proof)
                    fleet.record("drive-gate-released", fleet.remote(gate_node, "drive-gate", mode="clear"))
                    fleet.drive_gates.remove(gate_node)
                    gate_node = None
                    fleet.start()
                    restored_status = run_task(probe, fleet, plan["scope"], 2, active,
                        option_overrides={"recursive": False}, admissions=admissions)
                    if status.get("admissionState") == "coordination-rejected":
                        require(len(admissions) == admissions_before_attempt + 1
                                and admissions[-1]["clientToken"] not in {item["clientToken"]
                                    for item in admissions[:admissions_before_attempt]},
                                "restored writable repair lacks a fresh unique task identity")
                    else:
                        require(len(admissions) >= 2 and admissions[-1]["clientToken"] != admissions[-2]["clientToken"],
                                "restored writable repair reused the failed task identity")
                    status = restored_status
                    fleet.stop()
                    repaired = fleet.snapshot("physical-writable-repair", quiescent=True)
                negative_proof = None
                if plan.get("negative"):
                    require(physical_content(prior) == physical_content(repaired),
                            "below-quorum Heal deleted metadata, changed good shards or falsely persisted repair")
                    negative_proof = validate_failed_version(status, bucket, plan["expected"], plan["proofs"][0])
                    fleet.record("negative-before-rescue-proof", negative_proof)
                    for registration in reversed(registrations):
                        node, params = registration
                        fleet.record("negative-rescue", fleet.remote(node, "recover-fault", **params))
                        fleet.faults.remove(registration)
                    registrations.clear()
                    recovered = fleet.snapshot("physical-backup-recovered-" + branch, quiescent=True)
                    require(physical_content(before) == physical_content(recovered), "negative fixture rescue differs from original")
                    fleet.start()
                    status = run_task(probe, fleet, plan["scope"], 2, active,
                        option_overrides={"recursive": True}, admissions=admissions)
                    fleet.stop()
                    repaired = fleet.snapshot("physical-after-negative-recovery-task", quiescent=True)
                physical = validate_repaired_physical(before, repaired, plan["proofs"][0], repair_targets=plan["proofs"])
                bitrot = [verify_proof(fleet, target, "repaired-bitrot-" + branch) for target in plan["proofs"]]
                fleet.start()
                s3 = verify_version_s3(probe, fleet, bucket, expected, baseline, "s3-after-" + branch, fixture=fixture)
                if lock_context is not None:
                    s3["protection"] = prove_locked(probe, fleet, bucket, lock_context,
                        key=LOCK_KEYS[LOCK_BRANCHES.index(branch)], label="lock-after-" + branch)
                if args.case_id == "heal.delete-marker-semantics":
                    s3["markerSemantics"] = verify_markers(probe, fleet, bucket, plan["scope"]["key"], expected)
                if size_rule:
                    s3["actualBoundaryRanges"] = verify_boundary_ranges(probe, fleet, bucket, expected, sizes["actualECBlockSize"])
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
                if args.case_id == "heal.delete-marker-semantics":
                    expected, baseline, s3["markerRemoval"] = reveal_current(
                        recorder, fleet, bucket, plan["scope"]["key"], expected, baseline)
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
                            verify_proof(fleet, target, "repeat-bitrot-" + branch)
                        fleet.start()
                        proof["s3"] = verify_version_s3(probe, fleet, bucket, expected, baseline, "s3-repeat-" + branch, fixture=fixture)
                        repeats.append(proof)
                result["branches"][branch] = {"verdict": "PASS", "physical": physical, "bitrot": bitrot,
                    "s3": s3, "responsibility": responsibility, "repeats": repeats}
                if instances:
                    result["branches"][branch]["parameters"] = plan["parameters"]
                if negative_proof is not None:
                    result["branches"][branch]["negative"] = negative_proof
                    result["branches"][branch]["physical"]["backupUsedAfterFailureProof"] = True
                    result["branches"][branch]["physical"]["productRepairClaimed"] = False
                if gated_proof is not None:
                    result["branches"][branch]["unwritableWindow"] = gated_proof
            except (Exception, KeyboardInterrupt) as exc:
                result["branches"][branch] = {"verdict": "FAIL", "failure": {"type": type(exc).__name__, "message": str(exc)}}
                try:
                    fleet.stop()
                    if args.case_id in {"heal.disk-replacement-formatted", "heal.replacement-auto-rebuild", "heal.target-graceful-restart", "heal.target-process-crash"}:
                        for node in fleet.nodes:
                            fleet.record("replacement-failure-format-and-original", {"node": node,
                                **fleet.remote(node, "replacement-state")})
                    if gate_node is not None:
                        fleet.record("gate-error-rescue", fleet.remote(gate_node, "drive-gate", mode="clear"))
                        fleet.drive_gates.remove(gate_node)
                    fleet.snapshot("physical-failure-" + branch, quiescent=True, metadata_faults=metadata_faults)
                    for node in tuple(fleet.drive_gates):
                        fleet.record("marker-gate-error-rescue", fleet.remote(node, "drive-gate", mode="clear"))
                        fleet.drive_gates.remove(node)
                    for registration in reversed(registrations):
                        node, params = registration
                        fleet.record("rescue-" + branch, fleet.remote(node, "recover-fault", **params))
                        fleet.faults.remove(registration)
                except Exception as recovery_error:
                    result["branches"][branch]["recoveryError"] = str(recovery_error)
                    raise
                if isinstance(exc, KeyboardInterrupt) or active or stale_marker_started:
                    raise
                restart_after_failure = args.case_id == "heal.multiple-shard-loss"
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
        cleaned = fleet.cleanup() if lock_context is None else cleanup_locked_fleet(probe, fleet, bucket, lock_context)
        result["cleanup"] = "PASS" if cleaned else "FAIL"
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
    fixtures = ("DB", "DV") if args.case_id in {"heal.repeat-idempotency", "heal.below-read-quorum", "heal.missing-or-corrupt-xlmeta", "heal.multiple-shard-loss", "heal.read-repair-attribution", "heal.stale-version-rejoin", "heal.disk-replacement-formatted", "heal.replacement-auto-rebuild", "heal.target-graceful-restart", "heal.target-process-crash"} else ("DV",)
    if args.case_id == "heal.object-metadata-and-tags":
        fixtures = ("DS",)
    if args.case_id == "heal.special-keys-and-pagination":
        fixtures = ("DK",)
    if args.case_id == "heal.object-size-boundaries":
        fixtures = ("DB",)
    if args.case_id == "heal.object-lock-retention":
        fixtures = ("DO",)
    if args.case_id in {"heal.disk-readonly-and-permission", "heal.scanner-cold-object-repair", "heal.disk-loss-and-repair"}:
        fixtures = ("DB",)
    allowed_topologies = set(select(args.case_id)[0]["contract"]["probes"]["topology"].split(","))
    require(allowed_topologies <= {"H1", "H2"}, "version executor lacks a required topology")
    topologies = [(name, selected) for name, selected in (("H1", nodes[:1]), ("H2", nodes[:4])) if name in allowed_topologies]
    results = []
    try:
        for topology, selected in topologies:
            for fixture in fixtures:
                results.append(run_topology(args, topology, selected, fixture))
                if results[-1]["cleanup"] != "PASS":
                    break
            if results[-1]["cleanup"] != "PASS":
                break
    finally:
        complete = len(results) == len(topologies) * len(fixtures) and all(
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
