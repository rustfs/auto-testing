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
import threading
import time
from urllib.parse import quote
import uuid

try:
    from .heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed as seed_workload, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .shards import files, map_target, validate_repaired_physical
    from .cursor import validate_missing_batch, verify_part_manifest
    from .versions import public_version_id
    from .race_native_evidence import verify as verify_native
    from .race_io import set_fleet as set_race_io_cap
    from .mpu_race_backups import verify_versions as verify_version_backups
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed as seed_workload, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import files, map_target, validate_repaired_physical
    from cursor import validate_missing_batch, verify_part_manifest
    from versions import public_version_id
    from race_native_evidence import verify as verify_native
    from race_io import set_fleet as set_race_io_cap
    from mpu_race_backups import verify_versions as verify_version_backups


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
G06_TARGET_KEY = "中文/文件/0252.bin"
G06_REFERENCE_KEY = "literal%2Fslash/0254.bin"
REFERENCE = {
    "candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "sourceFiles": {
        "rustfs/src/admin/handlers/heal.rs": "7e4007c29a8329ced8e48b88111400e79cffd845bd6ddc6243b79f3cd48e8199",
        "crates/heal/src/heal/channel.rs": "7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0"},
    "requested": {**BASE_OPTIONS, "recursive": True, "nolock": False, "pool": None, "set": None},
    "effective": {**BASE_OPTIONS, "recursive": True, "nolock": False, "pool": None, "set": None},
    "nativeFence": "pinned release has no reviewed e2e-test-hooks artifact or exact Admin commit/cleanup barrier",
}
NATIVE_CANDIDATE = '6afdec9fe1b0b88007f2e5b445c19bf73d51d3ca'
NATIVE_REFERENCE = {
    'candidateCommit': NATIVE_CANDIDATE,
    'sourceFiles': {
        'rustfs/src/admin/handlers/heal.rs': '6187d9331555ea8421e0d4fe1449d310dd31d0c48f4e57ff43365701d3c6621b',
        'crates/heal/src/heal/channel.rs': 'b806939f7101c88ec0746b1b377f25ee5e88cc4871527bd348c7ee5f3f7f195d'},
    'requested': {**BASE_OPTIONS, 'recursive': True, 'nolock': False, 'pool': None, 'set': None},
    'effective': {**BASE_OPTIONS, 'recursive': True, 'nolock': False, 'pool': None, 'set': None},
    'nativeFence': 'exact-source real Heal during committed overwrite cleanup plus stale read-repair final rename',
}
VERSION_CANDIDATE = 'bd0671ba781d8ebf509f4f03533228dfb44496f1'
VERSION_REFERENCE = {**NATIVE_REFERENCE, 'candidateCommit': VERSION_CANDIDATE,
    'nativeFence': 'exact-source Heal cleanup and explicit-version commit and deletion fences'}
MARKER_CANDIDATE = '8380466f3a27c3c3ef4115084178a0ab99a78f32'
MARKER_REFERENCE = {**VERSION_REFERENCE, 'candidateCommit': MARKER_CANDIDATE,
    'nativeFence': 'exact-source Heal cleanup, version and marker/batch-delete commit fences'}
COPY_CANDIDATE = '757b751482bb970bc321e92059690a7698554358'
COPY_REFERENCE = {**MARKER_REFERENCE, 'candidateCommit': COPY_CANDIDATE,
    'nativeFence': 'exact-source streamed COPY, metadata and tag final-rename fences'}
NATIVE_RUNTIME = {
    NATIVE_CANDIDATE: {'reference': NATIVE_REFERENCE, 'cases': ('heal.race-unversioned-overwrite',)},
    VERSION_CANDIDATE: {'reference': VERSION_REFERENCE,
        'cases': ('heal.race-unversioned-overwrite', 'heal.race-versioned-put', 'heal.race-version-delete')},
    MARKER_CANDIDATE: {'reference': MARKER_REFERENCE,
        'cases': ('heal.race-unversioned-overwrite', 'heal.race-versioned-put', 'heal.race-version-delete',
                  'heal.race-markers-and-batch-delete')},
    COPY_CANDIDATE: {'reference': COPY_REFERENCE,
        'cases': ('heal.race-copy-and-metadata-update', 'heal.race-multipart-complete-abort')},
}
# Registry subsets do not enable a fleet case. Its reviewed runtime source and
# complete case-specific native group must both be present before bootstrap.
FULL_NATIVE_GROUPS = {
    'heal.race-unversioned-overwrite': ['committed-overwrite-cleanup', 'stale-read-repair-final-rename'],
    'heal.race-versioned-put': ['versioned-read-repair-lock-owner', 'explicit-version-append-final-rename'],
    'heal.race-version-delete': ['explicit-version-delete-final-rename'],
    'heal.race-markers-and-batch-delete': ['marker-and-batch-delete-final-rename'],
    'heal.race-copy-and-metadata-update': ['copy-and-metadata-final-rename'],
    'heal.race-multipart-complete-abort': ['multipart-continue-complete-abort-final-rename'],
}


def source_contract(candidate, case_id):
    require(case_id in CASE_IDS, 'unregistered race case')
    if candidate.get('source_sha') == REFERENCE['candidateCommit']:
        require(case_id not in ('heal.race-multipart-complete-abort','heal.race-markers-and-batch-delete'),
                'G04/G05 require their exact-source multipart or marker/batch native group')
        return REFERENCE
    entry = NATIVE_RUNTIME.get(candidate.get('source_sha'))
    require(entry is not None and case_id in entry['cases'] and case_id in FULL_NATIVE_GROUPS
            and entry['reference']['candidateCommit'] == candidate['source_sha'],
            'race candidate/case contract is not reviewed')
    return entry['reference']


def native_preflight(candidate, testing_commit, case_id, directory):
    reference = source_contract(candidate, case_id)
    if reference is REFERENCE:
        return None
    require(bool(directory), 'race requires exact-source native proof before any fleet mutation')
    proof = verify_native(directory, candidate, testing_commit, case_id)
    require(proof['requiredRegressionLabels'] == FULL_NATIVE_GROUPS[case_id]
            and proof['requiredNativeSubsetPassed'] is True and proof['wholeCasePassClaimed'] is False,
            'case-specific native commit group is incomplete')
    return proof


class WindowNotObserved(ValueError):
    """No observable client overlap; this is not a product success."""


def target_key(branch):
    return G06_TARGET_KEY if branch in G06_BRANCHES else TARGET_KEY


def reference_key(branch):
    return G06_REFERENCE_KEY if branch in G06_BRANCHES else REFERENCE_KEY


class RaceRecorder(Recorder):
    """Keep unresolved mutations and exact SDK receipts after any assertion."""
    def __init__(self, *args, versioned, **kwargs):
        super().__init__(*args, **kwargs)
        self.versioned, self.pending, self.seen = versioned, None, set()

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
            require(self.versioned and params.get("VersionId") in {
                value for key, value in self.seen if key == params["Key"]}
                and version == params["VersionId"] and result.get("DeleteMarker") in {None, False},
                "G01/G03 DELETE ACK changed the exact known data-version identity")
        self.pending = None
        return result


def settings(observed, reference=REFERENCE):
    require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
            "G01/G03 status has an unknown or failed HTTP response")
    value = observed["response"]
    require(value.get("summary") in {"running", "finished"}, "G01/G03 original task disappeared, stopped or failed")
    # Canonical JSON distinguishes bool/int and preserves raw null selectors.
    require(canonical(value.get("settings")) == canonical(reference["effective"]),
            "G01/G03 Admin nolock was not safely normalized or changed unrelated options")
    return value["settings"]


def sample_overlap(ack, observed, path, token, reference=REFERENCE):
    foreground = ack.get("observation") or {}
    settings(observed, reference)
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
    require(branch in (*G01_BRANCHES, *G02_BRANCHES, *G03_BRANCHES, *G06_BRANCHES) and type(count) is int and 3 <= count <= OBJECTS, "G01/G02/G03/G04/G05/G06 unregistered fixture")
    if branch in G06_BRANCHES:
        require(count == OBJECTS, "G06 requires the frozen complete 257-key DS population")
        return seed_workload(recorder, bucket, "DS", count, [SIZE])
    recorder.call("create_bucket", {"Bucket": bucket})
    if branch in (*G02_BRANCHES, *G03_BRANCHES):
        recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    entries = []
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
    require(branch in (*G01_BRANCHES, *G02_BRANCHES, *G03_BRANCHES, *G06_BRANCHES), "G01/G02/G03/G04/G05/G06 unregistered foreground branch")
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
                 "nonce": f"{bucket}/objects/{index:04d}.bin/0", "referenceKey": f"objects/{index:04d}.bin",
                 }
                for index in range(3)]
    if branch in G06_BRANCHES:
        if branch == "copy-source":
            return [{"operation": "copy_object", "params": {"Bucket": bucket, "Key": current_reference,
                "CopySource": {"Bucket": bucket, "Key": current_target, "VersionId": "null"}}, "nonce": "copy-source"}]
        if branch == "copy-destination":
            return [{"operation": "copy_object", "params": {"Bucket": bucket, "Key": current_target,
                "CopySource": {"Bucket": bucket, "Key": current_reference, "VersionId": "null"}}, "nonce": "copy-destination"}]
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
                                                      "VersionId": (target[0] if branch == G03_BRANCHES[0] else target[-1])["versionId"]},
             "seedVersionOrder": [row['versionId'] for row in target]}]


def version_put_entries(baseline, plan, responses):
    """Build only received serial PUT ACKs; a missing response stays unknown."""
    require(len(plan) == 3 and 1 <= len(responses) <= 3, 'G02 ACK prefix is incomplete or excessive')
    added = []
    original = {row['versionId'] for row in baseline['entries'] if row['key'] == TARGET_KEY}
    require(len(original) == 3, 'G02 lacks its three original acknowledged versions')
    seen = set(original)
    for index, (step, ack) in enumerate(zip(plan[:len(responses)], responses, strict=True)):
        version = ack.get('VersionId')
        require(isinstance(version, str) and re.fullmatch(r'[0-9a-f-]{36}', version)
                and str(uuid.UUID(version)) == version and uuid.UUID(version).int != 0 and version not in seen
                and isinstance(ack.get('ETag'), str) and ack['ETag'], 'G02 PUT lacks a distinct real version ACK')
        seen.add(version)
        key = f'objects/{index:04d}.bin'
        reference = [row for row in baseline['entries'] if row['key'] == key and row.get('isLatest') is True
                     and row.get('deleteMarker') is False]
        body = payload(step['nonce'], SIZE)
        contents = {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
        require(step.get('referenceKey') == key and step['nonce'] == f'{step["params"]["Bucket"]}/{key}/0'
                and len(reference) == 1 and contents == reference[0]['payload'],
                'G02 new PUT bytes differ from the original independent control')
        added.append({'key': TARGET_KEY, 'versionId': version, 'isLatest': index == len(responses) - 1,
            'deleteMarker': False, 'payload': contents,
            'properties': {'ETag': ack['ETag'], 'ContentLength': len(body),
                           'ContentType': step['params']['ContentType'], 'Metadata': step['params']['Metadata']},
            'tags': [{'Key': 'race', 'Value': 'versioned-put'}]})
    return added


def final_inventory(baseline, branch, plan, responses):
    expected_length = (3 if branch in (*G01_BRANCHES, *G02_BRANCHES)
                       else 16 if branch == "update-tags" else 1)
    require(len(plan) == len(responses) == expected_length,
            "G01/G03/G05 foreground plan/ACK sequence is incomplete")
    result = copy.deepcopy(baseline)
    current_target, current_reference = target_key(branch), reference_key(branch)
    if branch in G02_BRANCHES:
        added = version_put_entries(baseline, plan, responses)
        for row in result['entries']:
            if row['key'] == current_target: row['isLatest'] = False
        result['entries'].extend(added)
        result['acknowledgedVersionAppendOrder'] = [row['versionId'] for row in added]
        result['entries'].sort(key=lambda row: (row['key'], row['versionId']))
    elif branch in G01_BRANCHES:
        entry = next(row for row in result["entries"] if row["key"] == current_target)
        step, ack = plan[-1], responses[-1]
        body = payload(step["nonce"], SIZE)
        require(isinstance(ack.get("ETag"), str) and ack["ETag"]
                and ack.get("VersionId") in {None, "null"}, "G01/G03 last PUT ACK lost its identity")
        entry.update(payload={"size": len(body), "sha256": hashlib.sha256(body).hexdigest()},
                     tags=[{"Key": "race", "Value": "overwrite"}])
        entry["properties"].update(ETag=ack["ETag"], ContentLength=len(body), ContentType="application/octet-stream",
                                   Metadata=step["params"]["Metadata"])
    elif branch in G06_BRANCHES:
        entry = next(row for row in result["entries"] if row["key"] == current_target)
        copy_result = responses[-1].get("CopyObjectResult") or {}
        copy_etag = copy_result.get("ETag") or responses[-1].get("ETag")
        if branch in {"copy-source", "copy-destination"}:
            source_key, destination_key = ((current_target, current_reference) if branch == "copy-source"
                                           else (current_reference, current_target))
            source = next(row for row in result["entries"] if row["key"] == source_key)
            destination = next(row for row in result["entries"] if row["key"] == destination_key)
            require(isinstance(copy_etag, str) and copy_etag, "G06 COPY ACK lacks its committed ETag")
            for field in ("payload", "properties", "tags"):
                destination[field] = copy.deepcopy(source[field])
            destination["properties"]["ETag"] = copy_etag
        elif branch == "replace-metadata":
            require(isinstance(copy_etag, str) and copy_etag, "G06 metadata replacement ACK lacks its committed ETag")
            entry["properties"].update(Metadata=plan[0]["params"]["Metadata"], ContentType=plan[0]["params"]["ContentType"])
            entry["properties"]["ETag"] = copy_etag
        else:
            entry["tags"] = copy.deepcopy(plan[-1]["params"]["Tagging"]["TagSet"])
    else:
        version = plan[0]["params"]["VersionId"]
        require(responses[0].get("VersionId") == version, "G01/G03 DELETE response changed the selected VersionId")
        order = plan[0].get('seedVersionOrder')
        target_rows = [row for row in result['entries'] if row['key'] == current_target]
        require(branch in G03_BRANCHES and isinstance(order, list) and len(order) == len(set(order)) == 3
                and set(order) == {row['versionId'] for row in target_rows}
                and all(row['deleteMarker'] is False and row['isLatest'] is (row['versionId'] == order[-1])
                        for row in target_rows)
                and version == order[0 if branch == G03_BRANCHES[0] else -1],
                'G03 DELETE lost its original serial seed ACK order or selected version')
        result["entries"] = [row for row in result["entries"]
                             if (row["key"], row["versionId"]) != (current_target, version)]
        require(len(result["entries"]) + 1 == len(baseline["entries"]), "G01/G03 deleted a foreign/missing version")
        latest = next(item for item in reversed(order) if item != version)
        for row in result['entries']:
            if row['key'] == current_target:
                row['isLatest'] = row['versionId'] == latest
    return result


def object_scene(snapshot, bucket, key):
    return {node: {**value, "files": [row for row in value["files"]
                                    if row["path"].startswith(f"{bucket}/{key}/")]}
            for node, value in snapshot.items()}


def map_fixture(snapshot, bucket, expected):
    live_expected = [row for row in expected if not row.get("deleteMarker")]
    mapped = {}
    for row in live_expected:
        target = map_target(object_scene(snapshot, bucket, row["key"]), bucket, row["key"], "data", 1,
                            row["versionId"], latest_required=False)
        # Frozen H1 removes the fourth physical drive's shard, regardless of
        # shuffled coding role. H2 uses the same local slot on its first host.
        selected = next((part for part in target["cohort"]
                         if part["node"] == sorted(snapshot)[0] and part["disk"] == 3), None)
        require(selected is not None, "race fixture lacks its original fourth-drive member")
        mapped[row["key"], row["versionId"]] = {**target, **selected}
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
    changed_key = (current_reference if branch == "copy-source" else current_target)
    for (key, version), target in mapped.items():
        if key == (changed_key if branch in G06_BRANCHES else current_target): continue
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
        require(len(old_dirs) == len(new_dirs) == 3 and old_dirs.isdisjoint(new_dirs),
                "G02 versions share or resurrect a physical data directory")
        old_versions = {target['versionId'] for target in original_target}
        entries = [row for row in final['entries'] if row['key'] == current_target]
        added = {row['versionId']: row for row in entries if row['versionId'] not in old_versions}
        order = final.get('acknowledgedVersionAppendOrder', [])
        require(len(order) == len(set(order)) == 3 and set(order) == set(added),
                'G02 physical transition lost the explicit serial ACK append order')
        appended = [added[version] for version in order]
        require(len(appended) == 3 and len({row['versionId'] for row in entries}) == len(entries) == 6,
                'G02 physical transition lacks all three serial new version ACKs')
        def coding(target):
            return {part['index']: (part['sha256'], part['size']) for part in target['cohort']}
        for target in current_target_values:
            if target['versionId'] in old_versions:
                original = mapped[current_target, target['versionId']]
                require({key: value for key, value in target['logical'].items() if key != 'isLatest'}
                        == {key: value for key, value in original['logical'].items() if key != 'isLatest'}
                        and coding(target) == coding(original), 'G02 append changed a retained historical version')
            else:
                index = next(i for i, row in enumerate(appended) if row['versionId'] == target['versionId'])
                control_key = f'objects/{index:04d}.bin'
                controls = [row for row in final['entries'] if row['key'] == control_key and row.get('isLatest') is True]
                references = [value for (key, _), value in mapped.items() if key == control_key]
                require(len(controls) == len(references) == 1 and appended[index]['payload'] == controls[0]['payload'],
                        'G02 new ACK bytes lack their independent pre-fault control')
                reference = references[0]
                require(target['logical']['size'] == SIZE and len(target['logical']['parts']) == 1
                        and target['logical']['parts'][0]['size'] == target['logical']['parts'][0]['actualSize'] == SIZE
                        and coding(target) == coding(reference)
                        and all(target['logical']['erasure'][key] == reference['logical']['erasure'][key]
                                for key in ('data_blocks', 'parity_blocks', 'block_size', 'algorithm')),
                        'G02 new version logical size or coding-index bytes differ from its immutable reference')
        expected_versions = sorted((row['versionId'], row['isLatest'], row['deleteMarker']) for row in entries)
        for value in after.values():
            for row in value['files']:
                if row['path'] != f'{bucket}/{current_target}/xl.meta': continue
                versions = row['metadata']['versions']
                require(all(type(v.get('isLatest')) is bool and type(v.get('deleted')) is bool for v in versions)
                        and sorted((public_version_id(v.get('versionId')), v['isLatest'], v['deleted']) for v in versions)
                        == expected_versions, 'G02 physical version population differs from all original and new ACKs')
    elif branch in G06_BRANCHES:
        target_after = next(value for (key, _), value in survivors.items() if key == changed_key)
        target_before = next(value for (key, _), value in mapped.items() if key == changed_key)
        if branch in {"copy-source", "copy-destination"}:
            source_key = current_target if branch == "copy-source" else current_reference
            source = next(value for (key, _), value in mapped.items() if key == source_key)
            require(target_after["dataDir"] != target_before["dataDir"]
                    and {part["index"]: (part["sha256"], part["size"]) for part in target_after["cohort"]}
                    == {part["index"]: (part["sha256"], part["size"]) for part in source["cohort"]},
                    "G06 copy did not publish a distinct target generation with source shard bytes")
        else:
            # The frozen metadata/tag oracle permits a new current directory;
            # every coding index must still contain the original payload bytes.
            require({part["index"]: (part["sha256"], part["size"]) for part in target_after["cohort"]}
                    == {part["index"]: (part["sha256"], part["size"]) for part in target_before["cohort"]},
                    "G06 metadata/tag update changed the original coding bytes")
    else:
        remaining = {(row["key"], row["versionId"]) for row in final["entries"]}
        removed = [value for pair, value in mapped.items() if pair not in remaining]
        require(len(removed) == 1, "G01/G03 delete transition lacks one exact deleted version")
        for target in current_target_values:
            original = mapped[current_target, target["versionId"]]
            require({name: value for name, value in target['logical'].items() if name != 'isLatest'}
                    == {name: value for name, value in original['logical'].items() if name != 'isLatest'}
                    and [(row["index"], row["sha256"], row["size"]) for row in target["cohort"]]
                    == [(row["index"], row["sha256"], row["size"]) for row in original["cohort"]],
                    "G01/G03 delete changed another retained ACK generation")
        wanted = sorted((row['versionId'], row['isLatest'], row['deleteMarker'])
                        for row in final['entries'] if row['key'] == current_target)
        indexed = files(after)
        for node, value in after.items():
            for drive in value['formats']:
                versions = indexed[node, drive['disk'], f'{bucket}/{current_target}/xl.meta']['metadata']['versions']
                require(all(type(row.get('isLatest')) is bool and type(row.get('deleted')) is bool for row in versions)
                        and sorted((row['versionId'], row['isLatest'], row['deleted']) for row in versions) == wanted,
                        'G03 physical version/latest population differs from retained serial ACKs')
    inspected_keys = {current_target, changed_key} if branch in G06_BRANCHES else {current_target}
    for value in after.values():
        for row in value["files"]:
            key = next((key for key in inspected_keys if row["path"].startswith(f"{bucket}/{key}/")), None)
            if key is None: continue
            prefix = f"{bucket}/{key}/"
            current_dirs = {target["dataDir"] for (name, _), target in survivors.items() if name == key}
            suffix = row["path"][len(prefix):]
            require(suffix == "xl.meta" or suffix.split("/")[0] in current_dirs,
                    "G01/G03 resurrected an old/deleted data directory or left an unbound target leaf")
            if branch in G02_BRANCHES and suffix != 'xl.meta':
                require(re.fullmatch(r'[0-9a-f-]{36}/(?:part\.1|part\.1\.integrity\.[0-9a-f-]{36}|xl\.meta\.bkp)', suffix),
                        'G02 retained an unacknowledged physical target leaf')
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


def observe(probe, fleet, path, token, reference=REFERENCE):
    value = fleet.record("original-token-status", probe.admin(path, [("clientToken", token)], b""))
    settings(value, reference)
    return value


def wait_running(probe, fleet, path, token, total, reference=REFERENCE):
    deadline = time.monotonic() + 30
    while True:
        observed = observe(probe, fleet, path, token, reference)
        value = observed["response"]
        processed = (value.get("outcome") or {}).get("counters", {}).get("processed")
        if value["summary"] == "finished":
            raise WindowNotObserved("G01/G03 task finished before its partial-progress race window")
        if type(processed) is int and 0 < processed < total: return observed
        if time.monotonic() >= deadline:
            raise WindowNotObserved("G01/G03 nonzero partial progress was not observed within 30 seconds")
        time.sleep(.05)


def wait_terminal(probe, fleet, path, token, reference=REFERENCE):
    deadline = time.monotonic() + 240
    while True:
        current = observe(probe, fleet, path, token, reference)
        if current["response"]["summary"] == "finished": return current["response"]
        require(time.monotonic() < deadline, "G01/G03 original task exceeded its terminal budget")
        time.sleep(.2)


def wait_version_window(probe, fleet, path, token, reference, before, targets, bucket):
    """Frozen G02 window: repaired controls and at least 32 physical targets pending."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        observed = observe(probe, fleet, path, token, reference)
        snapshot = fleet.snapshot('G02-live-partial-physical-window')
        complete, pending = [], []
        for target in targets:
            try:
                validate_repaired_physical(object_scene(before, bucket, target['key']),
                    object_scene(snapshot, bucket, target['key']), target)
            except ValueError:
                pending.append(target['key'])
            else:
                complete.append(target['key'])
        fleet.record('G02-partial-window', {'status': observed, 'completeKeys': complete, 'pendingKeys': pending})
        value = observed['response']; processed = (value.get('outcome') or {}).get('counters', {}).get('processed')
        if value['summary'] == 'running' and type(processed) is int and processed > 0 and complete and len(pending) >= 32:
            if TARGET_KEY not in pending:
                raise WindowNotObserved('G02 selected historical target repaired before the source partial window')
            return snapshot
        if not pending:
            raise WindowNotObserved('G02 all physical targets repaired before the source partial window')
        require(value['summary'] == 'running', 'G02 original root stopped before physical partial repair')
        time.sleep(.25)
    raise WindowNotObserved('G02 nonzero physical partial window was not observed within its source 120-second budget')


def foreground(recorder, plan, *, committed=None):
    responses = []
    for step in plan:
        require(step['operation'] not in {'create_multipart_upload', 'upload_part', 'complete_multipart_upload',
                                         'abort_multipart_upload'}, 'G05 requires its same-key ACK model and schedule')
        body = payload(step['nonce'], SIZE) if step['operation'] == 'put_object' else None
        response = recorder.call(step['operation'], copy.deepcopy(step['params']), body)
        responses.append(response)
        if committed is not None:
            committed(responses)
    return responses, {}


def read_versions(recorder, ledger, lock, done):
    """Read each ACK-known version, including a final round after the writer drains."""
    proofs = []
    deadline = time.monotonic() + 60
    for cycle in range(16):
        final_round = done.is_set()
        with lock:
            entries = copy.deepcopy(ledger['entries'])
        for entry in entries:
            if time.monotonic() >= deadline:
                return proofs
            params = {'Bucket': ledger['bucket'], 'Key': entry['key'], 'VersionId': entry['versionId']}
            recorder.call('get_object', params, identity={'readCycle': cycle})
            ack = json.loads((recorder.output / f'{recorder.sequence:06d}-ack.json').read_text())
            observed = ack['observation']; response = observed.get('response') or {}
            require(observed.get('acknowledged') is True and observed.get('resultUnknown') is False
                    and not observed.get('clientCleanupError')
                    and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200
                    and observed.get('params') == params and response.get('VersionId') == entry['versionId']
                    and observed.get('payload') == entry['payload']
                    and {key: response[key] for key in entry['properties'] if key in response} == entry['properties'],
                    'G02 historical GET did not preserve its exact acknowledged version bytes/properties')
            proofs.append(ack)
        if final_round:
            break
    return proofs


def race(recorder, plan, probe, fleet, path, token, reference=REFERENCE, *, baseline=None):
    samples = []
    # Only the worker mutates Recorder state. All Fleet.record writes run on
    # the controller, so evidence sequence allocation never races.
    readers = []
    read_branch = bool(plan and plan[0].get("branch") in {"overwrite-vs-read-repair", "read-all-versions-during-heal"})
    version_reads = bool(plan and plan[0].get('branch') == G02_BRANCHES[1])
    version_race = bool(plan and plan[0].get('branch') in G02_BRANCHES)
    done = threading.Event(); lock = threading.Lock()
    ledger = {'bucket': plan[0]['params']['Bucket'], 'entries': []}
    if version_reads:
        require(baseline is not None, 'G02 historical reads need their complete original S3 baseline')
        ledger['entries'] = [copy.deepcopy(row) for row in baseline['entries'] if row['key'] == TARGET_KEY]
        require(len(ledger['entries']) == 3, 'G02 historical reader lacks three original ACK-known versions')
    def committed(responses):
        added = version_put_entries(baseline, plan, responses)
        with lock:
            originals = [copy.deepcopy(row) for row in baseline['entries'] if row['key'] == TARGET_KEY]
            for entry in originals: entry['isLatest'] = False
            ledger['entries'] = originals + added
    def writing():
        try:
            return foreground(recorder, plan, committed=committed if version_reads else None)
        finally:
            done.set()
    with ThreadPoolExecutor(max_workers=2 if read_branch else 1) as pool:
        worker = pool.submit(writing)
        if version_reads:
            read_recorder = Recorder(probe, recorder.output.parent / 'historical-read-ACKs')
            read_future = pool.submit(read_versions, read_recorder, ledger, lock, done)
        elif read_branch:
            def read_repair():
                deadline = time.monotonic() + 90
                while not worker.done() and time.monotonic() < deadline:
                    params = {"Bucket": plan[0]["params"]["Bucket"], "Key": plan[0]["params"]["Key"]}
                    if plan[0].get("readVersionId"): params["VersionId"] = plan[0]["readVersionId"]
                    observed = probe.s3("get_object", params)
                    readers.append(observed)
                    time.sleep(.01)
            read_future = pool.submit(read_repair)
        else:
            read_future = None
        deadline = time.monotonic() + (90 if version_race else 120)
        try:
            while (not worker.done() or version_reads and read_future is not None and not read_future.done()) and time.monotonic() < deadline:
                samples.append(observe(probe, fleet, path, token, reference))
                time.sleep(.025 if version_race else .01)
        except BaseException:
            recorder.mutations.poison("status observation failed; drain in-flight mutations only")
            raise
        if not worker.done(): recorder.mutations.poison("foreground deadline; drain only")
        responses, runtime = worker.result(timeout=60)
        if plan and runtime:
            plan[0]["runtime"] = runtime
        if read_future is not None:
            value = read_future.result(timeout=60)
            if version_reads:
                readers = value
    acks = [json.loads(path.read_text()) for path in sorted(recorder.output.glob("*-ack.json"))]
    require(len(acks) == len(plan) and all(row.get("operationCheck") == "PASS" for row in acks),
            "G01/G03 lacks every durable foreground ACK")
    overlaps = [any(sample_overlap(ack, sample, path, token, reference) for sample in samples) for ack in acks]
    read_complete = True
    reader_overlap = True
    if version_reads:
        required = {row['versionId'] for row in ledger['entries']}
        actual = {row['observation']['params']['VersionId'] for row in readers}
        read_complete = len(required) == 6 and actual == required
        reader_overlap = any(sample_overlap(ack, sample, path, token, reference) for ack in readers for sample in samples)
        fleet.record('G02-historical-read-coverage', {'requiredVersionIds': sorted(required),
            'observedVersionIds': sorted(actual), 'complete': read_complete, 'readerCallOverlapObserved': reader_overlap,
            'allReadsBoundToDurableACKs': True, 'nativeFenceProved': False, 'canonicalCasePassClaimed': False})
    fleet.record("read-repair-overlap-proof", {"branch": plan[0].get("branch") if plan else None,
        "readCalls": len(readers), "allReadsObserved": bool(readers) if read_branch else True,
        "nativeFenceProved": False})
    fleet.record("public-call-overlap-proof", {"everyMutationOverlapped": all(overlaps), "mutations": overlaps,
        "nativeFenceProved": False, "serverCommitOverlapProved": False})
    return responses, all(overlaps) and read_complete and reader_overlap


def run_branch(args, topology, nodes, branch):
    if args.case_id == 'heal.race-markers-and-batch-delete':
        try:
            from .version_race_runner import run_branch as run_marker_branch
        except ImportError:
            from version_race_runner import run_branch as run_marker_branch
        source_contract(json.loads(args.candidate.read_text()), args.case_id)
        return run_marker_branch(args, topology, nodes, branch)

    if args.case_id == 'heal.race-multipart-complete-abort':
        # Lazy import keeps the generic controller and G05's model acyclic.
        try:
            from .mpu_race_runner import run_branch as run_mpu_branch
        except ImportError:
            from mpu_race_runner import run_branch as run_mpu_branch
        source_contract(json.loads(args.candidate.read_text()), args.case_id)
        return run_mpu_branch(args, topology, nodes, branch)
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True, exist_ok=False)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="manual")
    candidate = json.loads(args.candidate.read_text())
    reference = source_contract(candidate, args.case_id)
    inspector = fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
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
    fleet.record("source-contract", {**reference, "branch": branch, "objectCount": OBJECTS, "objectSize": SIZE,
        "requiredPUTs": 3 if branch in (*G01_BRANCHES, *G02_BRANCHES) else 0, "serverCommitOverlapProved": False})
    probe, active, recorder, writer = Probe(binding), [], None, None
    result = {"topology": topology, "branch": branch,
              "fixture": "DL" if branch in G01_BRANCHES else "DS" if branch in G06_BRANCHES else "DV",
              "verdict": "RUNNING", "cleanup": "PENDING", "canonicalCasePassClaimed": False}
    changed_target_registered, target_authority = None, False
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = RaceRecorder(probe, directory / "seed-ACKs",
                                versioned=branch in (*G02_BRANCHES, *G03_BRANCHES))
        expected = seed_fixture(recorder, bucket, branch)
        baseline = fleet.record("complete-baseline-S3", probe.inventory(
            bucket, unversioned_current_tags=branch in (*G01_BRANCHES, *G06_BRANCHES)))
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
                          if branch == G03_BRANCHES[1]
                          else next(entry for entry in expected if entry["key"] == current_target_key))
        targets = [mapped[row["key"], row["versionId"]] for row in expected
                   if row["key"] != current_target_key or row["versionId"] == target_version["versionId"]]
        require(len(targets) == OBJECTS, "G01/G03 fault plan omitted a key or changed its source dimension")
        receipts = []
        case_code = select(args.case_id)[0]["code"]
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "faultId": bounded_fault_id(case_code, branch, index)}
            fleet.faults.append((target["node"], params))
            if target["key"] == current_target_key: changed_target_registered = target["node"], params
            receipts.append(fleet.record("exact-missing-part", {"node": target["node"], **fleet.remote(
                target["node"], "apply-fault", **params, relativePath=target["relativePath"],
                expectedSha256=target["sha256"], kind="missing")}))
        faulted = fleet.snapshot("complete-fault-physical", quiescent=True)
        fleet.record("fault-batch-proof", validate_missing_batch(before, faulted, targets, receipts))
        fleet.start()
        set_race_io_cap(fleet, args.case_id, branch, True, nodes=sorted({target['node'] for target in targets}))
        require(physical_content(faulted) == physical_content(fleet.snapshot("attribution-before-START")),
                "NOT_OBSERVED: G01/G03 missing shards changed before manual admission")
        path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="")
        fleet.record("prepared-Admin-request", {"path": path, "query": [], "options": reference["requested"]})
        start = fleet.record("original-Admin-start", probe.admin(path, [], json.dumps(reference["requested"]).encode()))
        token = start.get("response", {}).get("clientToken") if isinstance(start.get("response"), dict) else None
        require(start.get("httpStatus") == 200 and isinstance(token, str) and token, "G01/G03 admission lacks an original token")
        active.append((path, token))
        if branch in G02_BRANCHES:
            witness = wait_version_window(probe, fleet, path, token, reference, before, targets, bucket)
        else:
            wait_running(probe, fleet, path, token, len(expected), reference)
            witness = fleet.snapshot("target-missing-before-foreground")
        target = next(target for target in targets if target["key"] == current_target_key)
        if (target["node"], target["disk"], target["relativePath"]) in files(witness):
            raise WindowNotObserved("G01/G03 old target already repaired before foreground mutations")
        writer = RaceRecorder(probe, directory / "foreground-ACKs",
                              versioned=branch in (*G02_BRANCHES, *G03_BRANCHES))
        writer.seen.update((row["key"], row["versionId"]) for row in expected)
        if branch in G02_BRANCHES:
            responses, overlapped = race(writer, plan, probe, fleet, path, token, reference, baseline=baseline)
        else:
            responses, overlapped = race(writer, plan, probe, fleet, path, token, reference)
        set_race_io_cap(fleet, args.case_id, branch, False, nodes=sorted({target['node'] for target in targets}))
        final = fleet.record("ACK-derived-final-manifest", final_inventory(baseline, branch, plan, responses))
        terminal = wait_terminal(probe, fleet, path, token, reference)
        active.remove((path, token))
        for _ in range(2):
            repeated = observe(probe, fleet, path, token, reference)["response"]
            require(canonical(repeated.get("outcome")) == canonical(terminal.get("outcome"))
                    and repeated["summary"] == terminal["summary"], "G01/G03 terminal responsibility changed on repetition")
        # Collect physical evidence even if terminal counters fail. Old target
        # bytes are never rescued over a successful overwrite or exact delete.
        fleet.stop()
        after = fleet.snapshot("product-physical-before-GET-or-rescue", quiescent=True)
        survivors, physical = physical_transition(before, after, bucket, mapped, final, branch)
        if branch in G02_BRANCHES:
            backups = {node: fleet.remote(node, 'version-race-backups', bucket=bucket,
                expectedBinarySha256=args.binary_sha, expectedInspectorSha256=inspector['binarySha256']) for node in nodes}
            fleet.record('G02-stopped-backup-decoding', backups)
            original_order = [row['versionId'] for row in expected if row['key'] == current_target_key]
            fleet.record('G02-serial-ACK-backup-prefix-proof', verify_version_backups(
                before, after, backups, nodes, bucket, current_target_key, original_order,
                final['acknowledgedVersionAppendOrder'], args.binary_sha, inspector['binarySha256']))
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
                bucket, unversioned_current_tags=branch in (*G01_BRANCHES, *G06_BRANCHES))})
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
    if sys.argv[1:2] == ['preflight']:
        parser = argparse.ArgumentParser(description='Read-only native admission before fleet bootstrap')
        parser.add_argument('--candidate', type=Path, required=True)
        parser.add_argument('--case-id', choices=list(CASE_BRANCHES), required=True)
        parser.add_argument('--testing-commit', required=True)
        args = parser.parse_args(sys.argv[2:])
        candidate = json.loads(args.candidate.read_text())
        proof = native_preflight(candidate, args.testing_commit, args.case_id,
                                 os.environ.get('VALIDATION_HEAL_NATIVE_DIRECTORY'))
        print(json.dumps({'nativeProof': proof, 'fleetMutationRequested': False, 'canonicalCasePassClaimed': False}))
        return
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
    native = native_preflight(candidate, args.testing_commit, case_id,
                              os.environ.get('VALIDATION_HEAL_NATIVE_DIRECTORY'))
    if native is None:
        results = [native_blocked(candidate, topology, case_id) for topology in ("H1", "H2")]
    else:
        save(args.evidence / 'independent-native-race-subset.json', native)
        results = [{'caseId': case_id, 'topology': topology, 'branch': branches[-1], 'verdict': 'PASS',
            'cleanup': 'PASS', 'candidateCommit': candidate['source_sha'], 'nativeFenceProved': True,
            'nativeProof': native, 'nativePhysicalShapes': ['4/2', '16/4'],
            'distributedFleetExecutionClaimed': False, 'fleetExecutionClaimed': False,
            'canonicalCasePassClaimed': False} for topology in ('H1', 'H2')]
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
            "canonicalCasePassClaimed": False, "nativePrerequisitesBlocked": native is None})
    require(complete, "race coverage or native prerequisites remain incomplete; every H1/H2 branch stays in the denominator")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"G01/G03 validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
