"""C11: same-name bucket recreation with original metadata and journal replay."""

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
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .fleet_node import PROCESS_FIELDS
    from .bucket_metadata import system_snapshot, system_files
    from .inspector import verify as verify_inspector
    from .manager_fixture import verify as verify_manager
    from .incarnation_native import verify as verify_storage
    from .matrix import select
    from .shards import files, map_target, run_task, verify_cohort
    from . import checkpoint as checkpoints, journal_fault as journal, mrf_replay as mrf
    from .mrf_records import canonical_uuid, verify_reference as verify_mrf_reference
    from .persistence import exact_records
    from .scanner_repair import JournalWindow
    from .stop import idle_fleet, status
    from .root_restart import recovery_status
    from .versions import public_version_id, validate_version_receipts, verify_quorum_contract, version_identity
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from fleet_node import PROCESS_FIELDS
    from bucket_metadata import system_snapshot, system_files
    from inspector import verify as verify_inspector
    from manager_fixture import verify as verify_manager
    from incarnation_native import verify as verify_storage
    from matrix import select
    from shards import files, map_target, run_task, verify_cohort
    import checkpoint as checkpoints, journal_fault as journal, mrf_replay as mrf
    from mrf_records import canonical_uuid, verify_reference as verify_mrf_reference
    from persistence import exact_records
    from scanner_repair import JournalWindow
    from stop import idle_fleet, status
    from root_restart import recovery_status
    from versions import public_version_id, validate_version_receipts, verify_quorum_contract, version_identity


CASE_ID = "heal.bucket-incarnation-recreation"
BRANCHES = ("old-metadata-rejoin", "old-checkpoint-replay", "old-mrf-replay", "native-incarnation-fence")
SIZE = 5 * 1024**2 + 123


def old_manifest(expected):
    wanted = {version_identity(entry): entry for entry in expected}
    require(wanted and len(wanted) == len(expected)
            and all(isinstance(pair[0], str) and pair[0] and isinstance(pair[1], str)
                    and str(uuid.UUID(pair[1])) == pair[1] and uuid.UUID(pair[1]).int != 0
                    and type(entry.get("deleteMarker")) is bool for pair, entry in wanted.items()),
            "C11 old manifest lacks distinct acknowledged data-version/marker identities")
    require(any(entry["deleteMarker"] for entry in expected) and any(not entry["deleteMarker"] for entry in expected),
            "C11 old DV fixture must contain both actual data versions and markers")
    return wanted


def delete_old_bucket(recorder, bucket, expected):
    """Only normal S3 deletion can establish the bucket deletion boundary."""
    wanted = old_manifest(expected)
    verify_seed(expected, recorder.probe.inventory(bucket))
    removed = []
    for (key, version), entry in wanted.items():
        ack = recorder.call("delete_object", {"Bucket": bucket, "Key": key, "VersionId": version})
        require(ack.get("ResponseMetadata", {}).get("HTTPStatusCode") == 204
                and ack.get("VersionId") == version
                and ack.get("DeleteMarker", False) is entry["deleteMarker"],
                "C11 explicit DELETE did not acknowledge the exact old data/marker kind and identity")
        removed.append({"key": key, "versionId": version, "deleteMarker": entry["deleteMarker"]})
    empty = recorder.probe.inventory(bucket)
    uploads = recorder.call("list_multipart_uploads", {"Bucket": bucket, "MaxUploads": 1})
    require(empty.get("entries") == [] and uploads.get("IsTruncated") is False and uploads.get("Uploads", []) == [],
            "C11 old bucket still contains a version, marker or pending upload")
    ack = recorder.call("delete_bucket", {"Bucket": bucket})
    require(ack.get("ResponseMetadata", {}).get("HTTPStatusCode") == 204,
            "C11 normal S3 delete_bucket did not succeed")
    absent = recorder.probe.s3("head_bucket", {"Bucket": bucket})
    require(absent.get("acknowledged") is False and absent.get("resultUnknown") is False
            and absent.get("response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 404,
            "C11 successful delete_bucket has no observed absence boundary")
    return {"deleted": removed, "deleteBucketAcknowledged": True, "absenceObserved": True,
            "internalNamespaceRemovalUsed": False}


def seed_successor(recorder, bucket, old):
    old_ids = set(old_manifest(old))
    recorder.call("create_bucket", {"Bucket": bucket})
    recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    recorder.call("put_bucket_tagging", {"Bucket": bucket, "Tagging": {
        "TagSet": [{"Key": "validation-generation", "Value": "successor"}]}})
    expected = []
    for index, key in enumerate(sorted({entry["key"] for entry in old})):
        for generation in range(3):
            body = payload(f"{bucket}/{key}/successor/{generation}", SIZE + generation + 71)
            ack = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "Metadata": {"heal-fixture": "C11", "incarnation": "successor", "generation": str(generation)},
                "Tagging": "suite=heal&incarnation=successor"}, body)
            version = ack.get("VersionId")
            require(isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int != 0
                    and (key, version) not in old_ids, "C11 successor reused an old or missing version ACK")
            expected.append({"key": key, "versionId": version, "deleteMarker": False,
                "isLatest": generation == 2, "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
        if index % 2 == 0:
            ack = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
            require(ack.get("DeleteMarker") is True and isinstance(ack.get("VersionId"), str)
                    and str(uuid.UUID(ack["VersionId"])) == ack["VersionId"] and uuid.UUID(ack["VersionId"]).int != 0
                    and (key, ack["VersionId"]) not in old_ids, "C11 successor marker reused an old identity")
            expected[-1]["isLatest"] = False
            expected.append({"key": key, "versionId": ack["VersionId"], "deleteMarker": True, "isLatest": True})
    require(not (old_ids & set(old_manifest(expected)))
            and {entry["payload"]["sha256"] for entry in old if not entry["deleteMarker"]}.isdisjoint(
                entry["payload"]["sha256"] for entry in expected if not entry["deleteMarker"]),
            "C11 successor bytes/identities do not distinguish the deleted generation")
    return expected


def stale_member_unchanged(before, after, member):
    require({node: row["formats"] for node, row in before.items()} ==
            {node: row["formats"] for node, row in after.items()}, "C11 recreation changed disk identity or topology")
    left, right = files(before), files(after)
    original = {identity: row for identity, row in left.items() if identity[:2] == member}
    require(original and original == {identity: row for identity, row in right.items() if identity[:2] == member},
            "C11 gated old metadata member changed before its actual rejoin")
    return {"node": member[0], "disk": member[1], "originalOldMemberBytesUnchanged": True}


def successor_physical(before, after, bucket, expected, old, member):
    wanted = {}
    for entry in expected:
        wanted.setdefault(entry["key"], set()).add((entry["versionId"], entry["deleteMarker"], entry["isLatest"]))
    old_ids, prior = set(old_manifest(old)), files(before)
    current = files(after)
    require({identity: row for identity, row in prior.items() if not identity[2].startswith(bucket + "/")} ==
            {identity: row for identity, row in current.items() if not identity[2].startswith(bucket + "/")},
            "C11 recreation or rejoin changed a control bucket's physical content")
    require({node: row["formats"] for node, row in before.items()} ==
            {node: row["formats"] for node, row in after.items()}, "C11 rejoin changed disk formats")
    for node, census in after.items():
        for disk in range(4):
            actual = {}
            for item in census["files"]:
                meta = item.get("metadata")
                if item["disk"] != disk or not meta: continue
                if meta.get("bucket") != bucket: continue
                require(meta.get("bucket") == bucket and meta.get("key") in wanted,
                        "C11 final metadata escaped successor ACK scope")
                rows = meta.get("versions")
                require(isinstance(rows, list), "C11 final metadata omitted decoded versions")
                identities = {(public_version_id(row.get("versionId")), row["deleted"], row["isLatest"]) for row in rows}
                require(len(identities) == len(rows) and meta["key"] not in actual
                        and not any((meta["key"], version) in old_ids for version, _, _ in identities),
                        "C11 final metadata resurrected an old identity or duplicated successor responsibility")
                actual[meta["key"]] = identities
            require(actual == wanted, "C11 rejoined member lacks complete successor versions/markers")
    cohorts = []
    for entry in expected:
        if entry["deleteMarker"]: continue
        target = map_target(after, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False)
        for shard in target["cohort"]:
            if (shard["node"], shard["disk"]) == member: continue
            original = prior.get((shard["node"], shard["disk"], shard["relativePath"]))
            require(original and original["sha256"] == shard["sha256"] and original["size"] == shard["size"],
                    "C11 old member overwrote a surviving successor shard")
        cohorts.append(target)
    return cohorts


def config(probe, bucket):
    versioning = probe.s3("get_bucket_versioning", {"Bucket": bucket})
    tags = probe.s3("get_bucket_tagging", {"Bucket": bucket})
    require(versioning.get("acknowledged") is True and versioning.get("resultUnknown") is False
            and versioning.get("response", {}).get("Status") == "Enabled"
            and tags.get("acknowledged") is True and tags.get("resultUnknown") is False
            and tags.get("response", {}).get("TagSet") == [{"Key": "validation-generation", "Value": "successor"}],
            "C11 old bucket configuration polluted the acknowledged successor configuration")
    return {"versioning": versioning, "tagging": tags}


def run_metadata_rejoin(args, topology, nodes, candidate):
    directory = args.evidence / topology / "old-metadata-rejoin"
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory)
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    control = bucket + "-control"
    fleet.snapshot_buckets = [bucket, control]
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
    tool = fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    verify_quorum_contract(tool, candidate)
    probe, active, pending = Probe(binding), [], None
    result = {"topology": topology, "fixture": "DV", "branch": "old-metadata-rejoin", "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("actual-topology", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = Recorder(probe, directory / "fixture")
        pending = {"operation": "seed-old-bucket"}
        controls = seed(recorder, control, "DB", 1, [SIZE])
        old = seed(recorder, bucket, "DV", 3, [SIZE])
        old_manifest(old)
        fleet.record("old-ACK-manifest", {"expected": old, **verify_seed(old, probe.inventory(bucket))})
        pending = None
        initial = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active, option_overrides={"recursive": True})
        old_incarnation = validate_version_receipts(initial, bucket, old, 2)["bucketIncarnationId"]
        idle_fleet(fleet, binding)
        fleet.stop()
        original = fleet.snapshot("old-generation-before-gate", quiescent=True)
        system_snapshot(fleet, [bucket, control], "old-generation-physical-bucket-records")
        entry = next(entry for entry in old if not entry["deleteMarker"])
        target = map_target(original, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False)
        shard = next(row for row in reversed(target["cohort"]) if row["node"] == nodes[-1])
        member = shard["node"], shard["disk"]
        fleet.drive_gates.add(member[0])
        fleet.record("old-member-gate", fleet.remote(member[0], "drive-gate", disk=member[1], mode="permission-denied"))
        fleet.start()
        denied = fleet.record("old-member-service-denial", fleet.remote(member[0], "drive-gate-probe"))
        require(denied.get("readProbe", {}).get("errno") == 13 and denied.get("errno") == 13
                and denied.get("dacBypassRemoved") is True, "C11 isolated member has no actual read/write denial")
        pending = {"operation": "normal-S3-delete-and-recreate"}
        result["deletedOriginal"] = fleet.record("normal-S3-bucket-deletion", delete_old_bucket(recorder, bucket, old))
        successor = seed_successor(recorder, bucket, old)
        baseline = fleet.record("successor-ACK-bound-S3", probe.inventory(bucket))
        verify_seed(successor, baseline)
        fleet.record("successor-config-ACK", config(probe, bucket))
        pending = None
        authority = fleet.snapshot("old-and-successor-physical-scopes-before-rejoin")
        result["staleMember"] = stale_member_unchanged(original, authority, member)
        processes = {node: fleet.remote(node, "process") for node in nodes}
        release = fleet.record("same-process-old-member-rejoin", fleet.remote(member[0], "drive-gate-release-live"))
        require(release.get("sameOriginalProcess") is True and release.get("serviceRestarted") is False,
                "C11 old member rejoin replaced its original process")
        fleet.drive_gates.remove(member[0])
        rejoined = {node: fleet.remote(node, "process") for node in nodes}
        require(all(all(processes[node].get(name) == rejoined[node].get(name) for name in PROCESS_FIELDS) for node in nodes),
                "C11 rejoin changed the original candidate/process/unit")
        fleet.snapshot("actual-rejoined-old-member-before-successor-task")
        status = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
                          option_overrides={"recursive": True}, status_endpoints=tuple(binding["endpoints"])[1:])
        responsibility = validate_version_receipts(status, bucket, successor, 2,
            required_repairs={version_identity(entry) for entry in successor if not entry["deleteMarker"]})
        require(responsibility["bucketIncarnationId"] != old_incarnation, "C11 new bucket rebound the deleted incarnation")
        result["incarnations"] = {"old": old_incarnation, "successor": responsibility["bucketIncarnationId"]}
        result["successorResponsibility"] = responsibility
        idle_fleet(fleet, binding)
        fleet.stop()
        after = fleet.snapshot("successor-physical-before-business-reads", quiescent=True)
        successor_system = system_snapshot(fleet, [bucket, control], "successor-physical-bucket-records-before-reads")
        cohorts = successor_physical(authority, after, bucket, successor, old, member)
        result["streaming"] = [verify_cohort(fleet, target, "successor-full-shard-integrity") for target in cohorts]
        fleet.start()
        for endpoint in binding["endpoints"]:
            peer = Probe(binding, endpoint)
            inventory = fleet.record("all-peer-successor-S3", {"endpoint": endpoint, **peer.inventory(bucket)})
            verify_seed(successor, inventory)
            compare_inventories(baseline, inventory)
            control_inventory = fleet.record("all-peer-control-bucket-S3", {"endpoint": endpoint,
                **peer.inventory(control, unversioned_current_tags=True)})
            verify_seed(controls, control_inventory)
            fleet.record("all-peer-successor-config", {"endpoint": endpoint, **config(peer, bucket)})
            for (key, version), entry in old_manifest(old).items():
                missing = fleet.record("all-peer-deleted-incarnation-version", peer.s3("head_object", {
                    "Bucket": bucket, "Key": key, "VersionId": version}))
                require(missing.get("acknowledged") is False and missing.get("resultUnknown") is False
                        and missing.get("response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 404,
                        "C11 old version/marker reappeared in the successor namespace")
        fleet.stop()
        require(physical_content(after) == physical_content(fleet.snapshot("after-successor-business-reads", quiescent=True)),
                "C11 business reads repaired data not proved before reads")
        require(system_files(successor_system) == system_files(system_snapshot(fleet, [bucket, control],
            "successor-physical-bucket-records-after-reads")), "C11 reads changed acknowledged successor bucket records")
        result.update(verdict="PASS", successorManifestVerified=True, oldBackupRestorationUsed=False,
                      checkpointReplayClaimed=False, mrfReplayClaimed=False, nativeCommitFenceClaimed=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("original-recreation-failure-scene", quiescent=True)
        except Exception as error: result["failureEvidenceError"] = str(error)
    finally:
        if pending is not None:
            result["unresolvedMutation"] = pending
            fleet.record("unresolved-S3-mutation", pending)
            clean = fleet.cleanup(preserve_namespaces=True)
            result["cleanup"] = "FAIL"
            result["environmentCleanupSucceeded"] = clean
        else:
            result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("incarnation-branch-result", result)
    return result



def record_digests(rows):
    return [{name: row[name] for name in ("disk", "relativePath", "size", "sha256")}
            for row in rows if row["present"]]


def checkpoint_rejection(current, frozen):
    outcome = current.get("outcome") or {}
    original = frozen["value"]["admin"]["outcome"]
    require(current.get("summary") == "stopped" and journal.same_json(current.get("settings"), checkpoints.OPTIONS)
            and isinstance(current.get("detail"), str) and current["detail"].startswith("stale_bucket_incarnation:")
            and outcome.get("execution") == {"state": "aborted", "reason": "untraversable"}
            and outcome.get("coverage") == "partial" and current.get("truncated") is False,
            "C11 old checkpoint has no explicit original stale-incarnation failure")
    require(all(journal.same_json(outcome.get(name), original.get(name))
                for name in ("counters", "objects", "objectsTruncated")),
            "C11 old checkpoint processed successor work or rewrote original acknowledged progress")
    return {"originalTaskId": frozen["taskId"], "bucketIncarnationId": frozen["bucketIncarnationId"],
            "staleIncarnationRejected": True, "originalProgressRetained": True, "successorWorkClaimed": False}


def checkpoint_terminal(observed, frozen, current):
    token = frozen["taskId"]
    names = journal.names("root", token)
    records = exact_records(observed, token, "root")
    require(set(records) == set(names[1:]), "C11 old checkpoint lacks an exact durable terminal/report or is still runnable")
    owner = frozen["rootAdmission"]
    require(all((row["node"], row["disk"]) == (owner["node"], owner["disk"]) for row in records.values()),
            "C11 old checkpoint terminal moved original durable ownership")
    terminal = journal.decode_json(checkpoints.bytes_of(records[names[1]]))
    report = journal.decode_json(checkpoints.bytes_of(records[names[2]]))
    require(type(terminal.get("schema")) is int and terminal["schema"] == 1 and terminal.get("task_id") == token
            and journal.same_json(terminal.get("heal_type"), owner["value"]["heal_type"])
            and journal.same_json(terminal.get("options"), owner["value"]["options"])
            and terminal.get("status") == {"Failed": {"error": current["detail"]}}
            and type(report.get("schema")) is int and report["schema"] == 1
            and journal.same_json(report.get("terminal"), terminal)
            and journal.same_json(report.get("outcome"), current["outcome"]),
            "C11 old checkpoint durable rejection differs from original token/scope/outcome")
    return {"originalTaskId": token, "sameOriginalOwner": True, "durableFailureRetained": True}


def retained_mrf(checkpoints_by_node, obligations, bindings, successor_incarnation):
    require(successor_incarnation and all(owner["bucketIncarnationId"] != successor_incarnation
            for owners in bindings.values() for owner in owners), "C11 successor reused the old MRF incarnation")
    retained = []
    for obligation in obligations:
        wanted = mrf.identity(obligation)
        for owner in bindings[wanted]:
            value = checkpoints_by_node[owner["node"]]
            require(value.get("present") is True and value["owner"] != owner["checkpointOwner"]
                    and value["sequence"] > owner["sequence"], "C11 old MRF responsibility was lost or not durably reclassified")
            canonical_uuid(value["owner"])
            matches = [item for item in value["intents"] if mrf.identity(item) == wanted
                       and item.get("intentDigest") == owner["intentDigest"]]
            require(len(matches) == 1, "C11 exact old MRF ACK responsibility disappeared or duplicated")
            lifecycle = matches[0].get("lifecycle", {})
            state = lifecycle.get("state", {})
            require(lifecycle.get("responsibility_id") == owner["responsibilityId"]
                    and lifecycle.get("source_bucket_incarnation_id") == owner["bucketIncarnationId"]
                    and lifecycle.get("last_operator_acceptance") is None
                    and state.get("state") == "bucket_incarnation_changed"
                    and set(state) == {"state", "source_bucket_incarnation_id", "observed_bucket_incarnation_id", "detected_at_ms"}
                    and state["source_bucket_incarnation_id"] == owner["bucketIncarnationId"]
                    and state["observed_bucket_incarnation_id"] == successor_incarnation
                    and type(state["detected_at_ms"]) is int and 0 < state["detected_at_ms"] <= int(time.time() * 1000) + 5000,
                    "C11 MRF responsibility was retired, accepted, rebound or lacks durable incarnation-change diagnostics")
            retained.append({"node": owner["node"], "identity": obligation,
                "originalCheckpointOwner": owner["checkpointOwner"], "currentCheckpointOwner": value["owner"],
                "responsibilityId": owner["responsibilityId"], "state": state})
    return {"retainedResponsibilities": retained, "operatorAcceptanceUsed": False,
            "oldResponsibilityRetired": False, "successorRepairClaimed": False}


def replay_isolation(before, after):
    require(physical_content(before) == physical_content(after),
            "C11 old persisted work changed successor/control metadata, shards, integrity or disk formats")
    return {"allSuccessorAndControlPhysicalBytesUnchanged": True, "formatUnchanged": True}


def successor_receipts(current, bucket, successor):
    successor_incarnations = {row.get("identity", {}).get("bucketIncarnationId")
                             for row in current.get("outcome", {}).get("objects", [])}
    require(len(successor_incarnations) == 1 and None not in successor_incarnations,
            "C11 successor diagnostics mix or omit bucket generations")
    successor_incarnation = successor_incarnations.pop()
    # Complete independent manifests cover diagnostics capped at 128 objects.
    wanted = old_manifest(successor)
    journal.task_id(successor_incarnation)
    outcome = current.get("outcome") or {}
    require(current.get("summary") == "finished" and type(current.get("truncated")) is bool
            and current["truncated"] is (len(wanted) > 128) and outcome.get("execution") == {"state": "completed"}
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is (len(wanted) > 128)
            and len(outcome.get("objects", [])) == min(128, len(wanted)),
            "C11 successor scan lacks truthful bounded diagnostic coverage")
    counts = {"processed": len(wanted), "healed": 0, "unchanged": len(wanted), "skipped": 0,
              "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(outcome.get("counters", {}).get(key)) is type(value)
                and outcome["counters"][key] == value for key, value in counts.items()),
            "C11 successor scan counters differ from its complete ACK manifest")
    seen = set()
    for row in outcome["objects"]:
        item = row.get("identity", {})
        pair = item.get("object"), public_version_id(item.get("versionId"))
        require(pair in wanted and pair not in seen and item.get("bucket") == bucket
                and item.get("bucketIncarnationId") == successor_incarnation
                and type(item.get("poolIndex")) is int and item["poolIndex"] == 0
                and type(item.get("setIndex")) is int and item["setIndex"] == 0
                and row.get("disposition", {}).get("state") == ("metadata_healthy" if wanted[pair]["deleteMarker"] else "verified_healthy"),
                "C11 successor diagnostic changed an ACK, kind, generation or successful disposition")
        seen.add(pair)
    return {"bucketIncarnationId": successor_incarnation,
        "completeIndependentManifestsRequired": True, "boundedDiagnosticsAreCompleteManifest": len(wanted) <= 128}


def run_replay(args, topology, nodes, branch, candidate):
    require(branch in {"old-checkpoint-replay", "old-mrf-replay"}, "unregistered C11 replay branch")
    family = "checkpoint" if branch == "old-checkpoint-replay" else "mrf"
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="incarnation-" + family,
                  resource_profile="cursor" if family == "checkpoint" else "standard")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    control = bucket + "-control"
    fleet.snapshot_buckets = [bucket, control]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 512, "maxInventoryBytes": 2 * 1024**3}
    fleet.record("binding", binding)
    tool = fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    verify_quorum_contract(tool, candidate)
    if family == "mrf": fleet.record("MRF-reviewed-source-contract", verify_mrf_reference(candidate))
    probe, active, pending = Probe(binding), [], None
    result = {"topology": topology, "fixture": "DV", "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING",
              "canonicalCasePassClaimed": False}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("actual-topology", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = Recorder(probe, directory / "fixture")
        pending = {"operation": "seed-old-DV"}
        old = seed(recorder, bucket, "DV", 80 if family == "checkpoint" else 3, [1024**2 + 123])
        old_manifest(old)
        fleet.record("old-ACK-manifest", {"expected": old, **verify_seed(old, probe.inventory(bucket))})
        pending = None
        frozen, bindings, obligations, gate_member = None, None, None, None
        if family == "checkpoint":
            token = checkpoints.admit(probe, fleet, active)
            frozen, process_identity = checkpoints.freeze(fleet, token, bucket)
            frozen = checkpoints.stop_for_fault(fleet, token, bucket, frozen, process_identity)
            old_incarnation = frozen["bucketIncarnationId"]
            fleet.record("original-old-incarnation-checkpoint", frozen)
        else:
            initial = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
                               option_overrides={"recursive": True})
            old_incarnation = validate_version_receipts(initial, bucket, old, 2)["bucketIncarnationId"]
            idle_fleet(fleet, binding)
            fleet.stop()
            original = fleet.snapshot("old-before-partial-write-gate", quiescent=True)
            entry = next(entry for entry in old if not entry["deleteMarker"])
            target = map_target(original, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False)
            shard = next(row for row in reversed(target["cohort"]) if row["node"] == nodes[-1])
            gate_member = shard["node"], shard["disk"]
            fleet.drive_gates.add(gate_member[0])
            fleet.record("old-MRF-member-gate", fleet.remote(gate_member[0], "drive-gate", disk=gate_member[1], mode="permission-denied"))
            fleet.start()
            denial = fleet.record("old-MRF-member-denial", fleet.remote(gate_member[0], "drive-gate-probe"))
            require(denial.get("readProbe", {}).get("errno") == 13 and denial.get("errno") == 13
                    and denial.get("dacBypassRemoved") is True, "C11 MRF partial write has no actual read/write denial")
            earliest = int(time.time() * 1000) - 5000
            pending = {"operation": "old-MRF-partial-write-ACKs"}
            old, obligations, _ = mrf.acknowledge(recorder, bucket, "offline-put", old)
            verify_seed(old, probe.inventory(bucket))
            pending = None
            deadline = time.monotonic() + mrf.PERSIST_SECONDS
            while True:
                try:
                    bindings = mrf.pending_bindings(mrf.read_checkpoints(fleet, nodes, "C11-old-committed-MRF"), obligations, earliest)
                    break
                except ValueError as exc:
                    if not str(exc).startswith("NOT_OBSERVED:"): raise
                    require(time.monotonic() < deadline, "C11 old ACK-bound MRF persistence window not observed")
                    time.sleep(.5)
            require(all(owner["bucketIncarnationId"] == old_incarnation for owners in bindings.values() for owner in owners),
                    "C11 MRF ACKs belong to another old generation")
            fleet.record("original-old-MRF-ACK-bindings", {"obligations": obligations,
                "bindings": [{"identity": list(key), "owners": value} for key, value in bindings.items()]})
            fleet.stop()
            bindings = mrf.pending_bindings(mrf.read_checkpoints(fleet, nodes, "C11-old-MRF-stopped", quiescent=True), obligations, earliest)
            token = None
        params = {"family": family, "taskId": token, "bucketIncarnationId": old_incarnation}
        for node in nodes:
            if family == "checkpoint":
                observed = [*fleet.remote(node, "journal-census", family="checkpoint", taskId=token)["records"],
                            *fleet.remote(node, "journal-census", family="root", taskId=token)["records"]]
                # The adapter uses disk-major order over both exact families.
                observed.sort(key=lambda row: (row["disk"], [*journal.names("checkpoint", token), *journal.names("root", token)].index(row["relativePath"])))
            else:
                observed = fleet.remote(node, "incarnation-census", **params)["records"]
            fleet.record("C11-independent-original-record-census", {"node": node, **params, "records": observed})
            fleet.incarnation_replays.append((node, params.copy()))
            receipt = fleet.record("archive-and-detach-original-replay", {"node": node, **fleet.remote(
                node, "incarnation-capture", **params, expectedRecords=record_digests(observed))})
            require(receipt.get("detached") is True and receipt.get("originalProductBytes") is True
                    and receipt.get("replacementBytesAccepted") is False, "C11 original durable record detachment is incomplete")
            detached = fleet.record("C11-independent-detached-record-census", {"node": node, **fleet.remote(
                node, "incarnation-census", **params)})
            require(all(not row["present"] for row in detached["records"]), "C11 independent observer still sees original replay sources")
            fleet.record("C11-transition-runtime", {"node": node, **fleet.remote(node, "incarnation-phase", phase="transition")})
        if gate_member is not None:
            fleet.record("C11-old-MRF-member-release", fleet.remote(gate_member[0], "drive-gate", mode="clear"))
            fleet.drive_gates.remove(gate_member[0])
        fleet.start()
        pending = {"operation": "normal-S3-delete-and-successor-recreate"}
        result["deletedOriginal"] = fleet.record("normal-S3-bucket-deletion", delete_old_bucket(recorder, bucket, old))
        successor = seed_successor(recorder, bucket, old)
        controls = seed(recorder, control, "DB", 1, [SIZE])
        baseline = fleet.record("successor-ACK-bound-S3", probe.inventory(bucket))
        verify_seed(successor, baseline)
        fleet.record("successor-config-ACK", config(probe, bucket))
        pending = None
        successor_status = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
            option_overrides={"recursive": True})
        successor_responsibility = successor_receipts(successor_status, bucket, successor)
        require(successor_responsibility["bucketIncarnationId"] != old_incarnation,
                "C11 successor generation was not distinct before any original replay")
        idle_fleet(fleet, binding)
        fleet.stop()
        authority = fleet.snapshot("complete-successor-physical-before-original-replay", quiescent=True)
        successor_physical(authority, authority, bucket, successor, old, (None, None))
        for node in nodes:
            receipt = fleet.record("install-exact-original-replay", {"node": node, **fleet.remote(node, "incarnation-install", **params)})
            require(receipt.get("installed") is True and receipt.get("originalArchiveRetained") is True,
                    "C11 original record transaction was not completely installed")
            installed = fleet.record("C11-independent-installed-record-census", {"node": node, **fleet.remote(
                node, "incarnation-census", **params)})
            require(journal.same_json(installed["records"], receipt["records"]),
                    "C11 independent observer differs from the exact installed original bytes")
            fleet.record("C11-replay-runtime", {"node": node, **fleet.remote(node, "incarnation-phase", phase="replay")})
        authority = fleet.snapshot("successor-physical-before-original-replay", quiescent=True)
        system_before = system_snapshot(fleet, [bucket, control], "successor-system-before-original-replay")
        window = JournalWindow(fleet, "C11-original-replay", event_targets={"rustfs::heal::manager", "rustfs::heal::task", "rustfs::heal::mrf"})
        fleet.start()
        if family == "checkpoint":
            deadline = time.monotonic() + 180
            while True:
                current = recovery_status(probe, fleet, token)
                if current.get("summary") != "running": break
                require(time.monotonic() < deadline, "C11 original checkpoint has no bounded terminal")
                time.sleep(.2)
            result["originalCheckpointRejection"] = checkpoint_rejection(current, frozen)
            for endpoint in binding["endpoints"]:
                peer = status(Probe(binding, endpoint), fleet, checkpoints.PATH, token, "C11-original-rejection-peer")
                require(journal.same_json(peer.get("outcome"), current.get("outcome"))
                        and peer.get("summary") == current.get("summary") and peer.get("detail") == current.get("detail"),
                        "C11 peer lost the original rejected checkpoint outcome")
            active.remove((checkpoints.PATH, token))
        else:
            deadline = time.monotonic() + 180
            while True:
                try:
                    result["retainedOldMrfResponsibility"] = retained_mrf(
                        mrf.read_checkpoints(fleet, nodes, "C11-old-MRF-replay-retained"), obligations, bindings,
                        successor_responsibility["bucketIncarnationId"])
                    break
                except ValueError as exc:
                    fleet.record("C11-MRF-rejection-not-yet-observed", {"error": str(exc)})
                    require(time.monotonic() < deadline, "C11 MRF durable stale-generation diagnostic not observed: " + str(exc))
                    time.sleep(1)
            result["originalMrfReplayEvents"] = window.capture()
        fleet.stop()
        replay_isolation(authority, fleet.snapshot("successor-physical-after-original-replay-before-reads", quiescent=True))
        require(system_files(system_before) == system_files(system_snapshot(fleet, [bucket, control], "successor-system-after-original-replay")),
                "C11 original replay changed successor bucket configuration/incarnation bytes")
        if family == "checkpoint":
            result["durableOriginalRejection"] = checkpoint_terminal(checkpoints.records(
                fleet, token, "C11-durable-original-terminal", quiescent=True, family="root"), frozen, current)
        fleet.start()
        responsibility = successor_responsibility
        if family == "mrf":
            deadline = time.monotonic() + 180
            while True:
                try:
                    result["retainedOldMrfResponsibility"] = retained_mrf(mrf.read_checkpoints(fleet, nodes, "C11-retained-old-MRF"),
                        obligations, bindings, responsibility["bucketIncarnationId"])
                    break
                except ValueError as exc:
                    fleet.record("C11-MRF-rejection-not-yet-observed", {"error": str(exc)})
                    require(time.monotonic() < deadline, "C11 MRF durable stale-generation diagnostic not observed: " + str(exc))
                    time.sleep(1)
            result["originalMrfReplayEvents"] = window.capture()
        idle_fleet(fleet, binding)
        fleet.stop()
        final = fleet.snapshot("successor-physical-before-business-reads", quiescent=True)
        # Persisted old work has no authority to change any surviving successor bytes.
        replay_isolation(authority, final)
        cohorts = successor_physical(final, final, bucket, successor, old, (None, None))
        cohorts.extend(map_target(final, control, entry["key"], "data", 1, entry["versionId"], latest_required=False)
                       for entry in controls if not entry["deleteMarker"])
        result["fullShardIntegrity"] = [verify_cohort(fleet, target, "successor-full-shard-integrity") for target in cohorts]
        fleet.start()
        for endpoint in binding["endpoints"]:
            peer = Probe(binding, endpoint)
            inventory = fleet.record("all-peer-successor-S3", {"endpoint": endpoint, **peer.inventory(bucket)})
            verify_seed(successor, inventory)
            compare_inventories(baseline, inventory)
            verify_seed(controls, fleet.record("all-peer-control-S3", {"endpoint": endpoint, **peer.inventory(control, unversioned_current_tags=True)}))
            fleet.record("all-peer-successor-config", {"endpoint": endpoint, **config(peer, bucket)})
            for key, version in old_manifest(old):
                missing = fleet.record("all-peer-old-incarnation-version", peer.s3("head_object", {"Bucket": bucket, "Key": key, "VersionId": version}))
                require(missing.get("acknowledged") is False and missing.get("resultUnknown") is False
                        and missing.get("response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 404,
                        "C11 original version/marker reappeared in the successor namespace")
        fleet.stop()
        replay_isolation(final, fleet.snapshot("successor-after-business-reads", quiescent=True))
        require(system_files(system_before) == system_files(system_snapshot(fleet, [bucket, control], "successor-system-after-business-reads")),
                "C11 business reads changed acknowledged successor bucket records")
        if family == "checkpoint":
            checkpoint_terminal(checkpoints.records(fleet, token, "C11-retained-terminal-after-business-reads", quiescent=True, family="root"), frozen, current)
        else:
            retained_mrf(mrf.read_checkpoints(fleet, nodes, "C11-retained-MRF-after-business-reads", quiescent=True),
                         obligations, bindings, responsibility["bucketIncarnationId"])
        result.update(verdict="PASS", checkpointReplayClaimed=family == "checkpoint", mrfReplayClaimed=family == "mrf",
            successorResponsibility=responsibility, oldBackupRestorationUsed=False, nativeCommitFenceClaimed=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("original-replay-failure-scene", quiescent=True)
        except Exception as error: result["failureEvidenceError"] = str(error)
    finally:
        clean = fleet.cleanup(preserve_namespaces=pending is not None)
        result["cleanup"] = "PASS" if clean and pending is None else "FAIL"
        if pending is not None:
            result.update(unresolvedMutation=pending, environmentCleanupSucceeded=clean)
        fleet.record("incarnation-replay-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"C11 interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"): parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=(CASE_ID,), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "C11 source branch inventory changed")
    candidate = json.loads(args.candidate.read_text())
    native = verify_manager(os.environ.get("VALIDATION_HEAL_MANAGER_DIRECTORY", ""), candidate, CASE_ID)
    save(args.evidence / "independent-native-incarnation-prerequisites.json", native)
    storage = verify_storage(os.environ.get("VALIDATION_HEAL_NATIVE_DIRECTORY", ""), candidate, args.testing_commit)
    save(args.evidence / "independent-native-storage-commit.json", storage)
    require(re.fullmatch(r"[0-9a-f]{40}", args.testing_commit) and re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)
            and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "C11 immutable workflow identities required")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) == len(set(nodes)) == 4 and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "C11 requires the four distinct existing auto-testing Heal nodes")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes)):
            for branch in BRANCHES:
                if branch == "old-metadata-rejoin":
                    result = run_metadata_rejoin(args, topology, selected, candidate)
                elif branch == "native-incarnation-fence":
                    result = {"requestedTopology": topology, "topology": None, "fixture": "DV", "branch": branch,
                        "verdict": "PREREQUISITE_PASS", "cleanup": "PASS", "fleetBranchExecuted": False,
                        "evidenceScope": "independent exact-candidate native storage regression",
                        "nativeProof": storage, "canonicalCasePassClaimed": False}
                else:
                    result = run_replay(args, topology, selected, branch, candidate)
                results.append(result)
                if result["cleanup"] != "PASS": return 1
    finally:
        diagnostic_complete = len(results) == 8 and all(row["verdict"] == (
            "PREREQUISITE_PASS" if row["branch"] == "native-incarnation-fence" else "PASS") for row in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": False,
            "diagnosticBranchesPassed": diagnostic_complete,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL",
            "canonicalCasePassClaimed": False, "reason": "C11 full case review and canonical dispatch remain gated"})
    return 1


if __name__ == "__main__": sys.exit(main())
