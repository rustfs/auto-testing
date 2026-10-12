"""C11: same-name bucket recreation with one actual stale metadata member."""

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
import uuid

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .fleet_node import PROCESS_FIELDS
    from .bucket_metadata import system_snapshot, system_files
    from .inspector import verify as verify_inspector
    from .manager_fixture import verify as verify_manager
    from .matrix import select
    from .shards import files, map_target, run_task, verify_cohort
    from .stop import idle_fleet
    from .versions import public_version_id, validate_version_receipts, verify_quorum_contract, version_identity
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from fleet_node import PROCESS_FIELDS
    from bucket_metadata import system_snapshot, system_files
    from inspector import verify as verify_inspector
    from manager_fixture import verify as verify_manager
    from matrix import select
    from shards import files, map_target, run_task, verify_cohort
    from stop import idle_fleet
    from versions import public_version_id, validate_version_receipts, verify_quorum_contract, version_identity


CASE_ID = "heal.bucket-incarnation-recreation"
BRANCHES = ("old-metadata-rejoin", "old-checkpoint-replay", "old-mrf-replay", "native-incarnation-fence")
BLOCKERS = {
    "old-checkpoint-replay": "exact old-incarnation runnable checkpoint replay and original terminal evidence are incomplete",
    "old-mrf-replay": "ACK-bound old-incarnation committed MRF replay and retained responsibility evidence are incomplete",
    "native-incarnation-fence": "manager incarnation regression prerequisites do not prove the required exact storage commit barrier",
}
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
            clean = fleet.cleanup()
            result["cleanup"] = "FAIL"
            result["environmentCleanupSucceeded"] = clean
        else:
            result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("incarnation-branch-result", result)
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
    require(re.fullmatch(r"[0-9a-f]{40}", args.testing_commit) and re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)
            and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "C11 immutable workflow identities required")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) == len(set(nodes)) == 4 and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "C11 requires the four distinct existing auto-testing Heal nodes")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes)):
            for branch in BRANCHES:
                result = run_metadata_rejoin(args, topology, selected, candidate) if branch == "old-metadata-rejoin" else {
                    "topology": topology, "fixture": "DV", "branch": branch, "verdict": "BLOCKED", "cleanup": "PASS",
                    "reason": BLOCKERS[branch], "fleetBranchExecuted": False, "canonicalCasePassClaimed": False}
                results.append(result)
                if result["cleanup"] != "PASS": return 1
    finally:
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": False,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL",
            "canonicalCasePassClaimed": False, "reason": "old checkpoint/MRF replay and exact commit fence remain blocked"})
    return 1


if __name__ == "__main__": sys.exit(main())
