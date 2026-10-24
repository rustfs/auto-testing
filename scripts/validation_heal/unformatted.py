#!/usr/bin/env python3
"""B04: candidate-created blank-member initialization or exact safe refusal.

The foreign format is exported unchanged from a second released-process H1
deployment. Restoring the original archive is recovery for a negative branch,
and never earns a product-rebuild claim.
"""

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

try:
    from .heal_probe import Probe, compare_inventories, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .disk_loss import info_disks, member_state
    from .replacement import format_identity, physical_rebuild, automatic_completion
    from .shards import files, map_target
except ImportError:
    from heal_probe import Probe, compare_inventories, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from disk_loss import info_disks, member_state
    from replacement import format_identity, physical_rebuild, automatic_completion
    from shards import files, map_target


CASE_ID = "heal.disk-replacement-unformatted"
BRANCHES = ("blank-unformatted", "foreign-format")
OBSERVATION_SECONDS = 90
SIZE = 1024**2 + 73
REJECTED_STATES = {"disk not found", "disk access denied", "file access denied", "volume access denied",
                   "offline", "unformatted disk", "corrupted format", "inconsistent disk", "corrupted backend"}


def binding(args, candidate, nodes, run_id, bucket):
    return {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 512, "maxInventoryBytes": 512 * 1024**2}


def foreign_donor(args, node):
    directory = args.evidence / "foreign-donor-H1"
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h1"
    fleet = Fleet([node], directory, run_id, args.tool_directory)
    candidate = json.loads(args.candidate.read_text())
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    exported, error = None, None
    try:
        fleet.deploy(args.binary_sha, bucket, secrets.token_hex(16), secrets.token_hex(24))
        fleet.record("actual-foreign-donor-topology", validate_topology(fleet.snapshot("foreign-donor-formats"), "H1"))
        fleet.stop()
        exported = fleet.record("released-process-foreign-format", fleet.remote(node, "format-export", disk=0))
        require(exported.get("readOnly") is True and exported.get("binarySha256") == args.binary_sha,
                "foreign format was not exported from the exact stopped released candidate")
    except (Exception, KeyboardInterrupt) as exc:
        error = exc
    finally:
        clean = fleet.cleanup()
        fleet.record("foreign-donor-finalization", {"cleanup": "PASS" if clean else "FAIL", "productRepairClaimed": False})
    require(clean, "foreign donor cleanup failed; primary cluster must not start")
    if error is not None: raise error
    return exported


def refusal(observed, target, original_endpoint, foreign):
    total = len(target["cohort"])
    indexed = info_disks(observed, total)
    selected = indexed[(0, 0, target["setSlot"])]
    require(selected["endpoint"] == original_endpoint and selected.get("state") in REJECTED_STATES,
            "unready/foreign member lacks a structured exact-slot refusal or was marked healthy")
    permitted_ids = {"", target["diskUUID"]} | ({foreign["value"]["xl"]["this"]} if foreign else set())
    require(selected.get("uuid") in permitted_ids and selected.get("runtimeState") not in {"online", "ready"},
            "unready/foreign member became writable or acquired an invented disk identity")
    expected = {shard["setSlot"]: shard["diskUUID"] for shard in target["cohort"]}
    for slot, disk in indexed.items():
        if slot[2] == target["setSlot"]: continue
        require(disk.get("state") == "ok" and disk.get("uuid") == expected[slot[2]]
                and disk.get("runtimeState") in {None, "online"}, "replacement refusal changed another original member")
    return {"exactSlot": target["setSlot"], "endpoint": selected["endpoint"], "state": selected["state"],
            "reportedUUID": selected.get("uuid"), "unreadyMemberHealthyClaimed": False}


def negative_scene(before, scene, member, branch, foreign):
    left, right = files(before), files(scene)
    require({key: value for key, value in right.items() if key[:2] != member}
            == {key: value for key, value in left.items() if key[:2] != member}, "refusal changed a surviving user file")
    require(not any(key[:2] == member for key in right), "unready/foreign member received user data")
    for node, value in before.items():
        for old in value["formats"]:
            current = next(item for item in scene[node]["formats"] if item["disk"] == old["disk"])
            if (node, old["disk"]) != member:
                require(current == old, "refusal changed an unrelated original disk format")
            elif branch == "foreign-format":
                require(current.get("sha256") == foreign["sha256"] and current.get("value") == foreign["value"],
                        "candidate overwrote the exact foreign cluster/member format")
            else:
                require(current.get("present") is False and current.get("unformattedReplacement") is True,
                        "blank refusal created an unexplained format")
    return {"allSurvivingUserFilesAndFormatsUnchanged": True, "targetUserFilesAbsent": True,
            "foreignIdentityUnchanged": branch == "foreign-format", "productRepairClaimed": False}


def run_branch(args, topology, nodes, branch, foreign):
    directory = args.evidence / f"{topology}-{branch}"
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="replacement",
                  require_replacement_admission=True)
    candidate = json.loads(args.candidate.read_text())
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    lab = binding(args, candidate, nodes, run_id, bucket)
    fleet.record("binding", lab)
    probe = Probe(lab)
    result = {"topology": topology, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        require(candidate["source_sha"] == "072b57b37559fbe626aeda8abf61617fdcbdd926", "unsupported candidate initialization/refusal contract")
        fleet.deploy(args.binary_sha, bucket, access, secret)
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, "DB", 128, [SIZE])
        baseline = fleet.record("S3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("fixture-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        fleet.stop()
        before = fleet.snapshot("original-full-physical", quiescent=True)
        fleet.record("actual-original-topology", validate_topology(before, topology))
        target = map_target(before, bucket, expected[0]["key"], "data", 1)
        node = nodes[-1]
        selected = next(shard for shard in target["cohort"] if shard["node"] == node)
        target = {**target, **selected}
        member, disk = (node, target["disk"]), target["disk"]
        physical_format = next(item for item in before[node]["formats"] if item["disk"] == disk)
        fleet.start()
        original = member_state(fleet.record("original-member-Admin", probe.admin("/rustfs/admin/v3/info", [], b"", method="GET")),
                                target, len(target["cohort"]))
        fleet.stop()
        replaced = fleet.record("whole-original-root-quarantined", fleet.remote(node, "replace-drive", disk=disk,
            expectedFormatSha256=physical_format["sha256"]))
        require(replaced.get("newDataRootEmpty") is True and replaced.get("oldRootMovedIntact") is True,
                "B04 replacement lacks its actual empty root and exact original archive")
        if branch == "foreign-format":
            require(foreign["binarySha256"] == args.binary_sha, "foreign donor used another released candidate")
            fleet.record("foreign-source-provenance", foreign)
            fleet.record("exact-foreign-format-injection", fleet.remote(node, "foreign-format", rawBase64=foreign["rawBase64"], sha256=foreign["sha256"]))
        earliest = int(time.time())
        fleet.record("registered-initialization-refusal-window", {"seconds": OBSERVATION_SECONDS,
            "branch": branch, "directoryTestRootFallback": True, "actualDeviceReplacementClaimed": False,
            "manualHealMutations": 0, "refusalRequiresExactAdminSlotAndUnchangedScene": True})
        fleet.start()
        fleet.record("actual-old-archive-refusal", fleet.remote(node, "replacement-probe"))
        deadline, samples, initialized = time.monotonic() + OBSERVATION_SECONDS, [], False
        while True:
            state = fleet.record("initialization-physical-format-window", fleet.remote(node, "replacement-state"))
            require(state.get("originalArchiveUnchanged") is True, "candidate accessed or changed the original replacement archive")
            current = next(item for item in state["formats"] if item["disk"] == disk)
            if branch == "blank-unformatted" and current["present"]:
                initialized = True
                break
            if branch == "foreign-format":
                require(current.get("sha256") == foreign["sha256"] and current.get("value") == foreign["value"],
                        "candidate overwrote or reidentified a foreign formatted member")
            samples.append(refusal(fleet.record("exact-unready-member-Admin", probe.admin("/rustfs/admin/v3/info", [], b"", method="GET")),
                target, original["endpoint"], foreign if branch == "foreign-format" else None))
            if time.monotonic() >= deadline: break
            time.sleep(5)
        if initialized:
            current = fleet.snapshot("candidate-generated-initialized-format")
            fleet.record("actual-new-disk-identity", format_identity(before, current, member))
            endpoint = replaced["dataPath"] if topology == "H1" else f"http://rustfs-node{nodes.index(node) + 1}:9000{replaced['dataPath']}"
            responsibility = automatic_completion(probe, fleet, endpoint, earliest)
            fleet.stop()
            rebuilt = fleet.snapshot("automatic-blank-rebuild", quiescent=True)
            physical = fleet.record("complete-original-role-rebuild", physical_rebuild(before, rebuilt, member))
            parts = []
            for entry in expected:
                cohort = map_target(rebuilt, bucket, entry["key"], "data", 1)
                shard = next(shard for shard in cohort["cohort"] if (shard["node"], shard["disk"]) == member)
                parts.append({"disk": disk, "relativePath": shard["relativePath"], "versionId": "null", "partNumber": 1,
                              "sha256": shard["sha256"], "physicalSize": shard["size"]})
            streamed = fleet.record("all-new-member-streaming-checks", fleet.remote(node, "verify-parts", parts=[
                {name: part[name] for name in ("disk", "relativePath", "versionId", "partNumber")} for part in parts]))
            require(streamed.get("complete") is True and len(streamed.get("parts", [])) == len(parts), "blank rebuild omitted a streaming part")
            for wanted, proof in zip(parts, streamed["parts"], strict=True):
                require(proof.get("physicalBitrotVerified") is True and all(proof.get(name) == value for name, value in wanted.items()),
                        "blank rebuild streaming proof differs from the original same-role ACK shard")
            result.update(initialization="candidate-generated-and-completely-rebuilt", responsibility=responsibility,
                          physical=physical, backupRestorationUsed=False)
        else:
            require(len(samples) >= 3, "unformatted/foreign refusal did not span its registered observation window")
            fleet.stop()
            scene = fleet.record("refused-complete-physical", {peer: fleet.remote(peer, "snapshot", quiescent=True,
                allowUnformattedReplacement=peer == node) for peer in nodes})
            physical = negative_scene(before, scene, member, branch, foreign)
            fleet.record("safe-refusal-negative-proof", {"samples": samples, "physical": physical})
            recovery = fleet.record("negative-original-member-recovery", fleet.remote(node, "restore-replacement"))
            require(recovery.get("originalRestoredExactly") is True and recovery.get("productRepairClaimed") is False,
                    "negative recovery was not exact or falsely claimed product reconstruction")
            rebuilt = fleet.snapshot("negative-original-restored", quiescent=True)
            require(physical_content(before) == physical_content(rebuilt), "refusal recovery changed the original physical scene")
            result.update(initialization="structured-safe-refusal", physical=physical, refusalSamples=len(samples),
                          backupRestorationUsed=True, productRepairClaimed=False)
        fleet.start()
        for endpoint in lab["endpoints"]:
            inventory = fleet.record("all-peer-final-S3", {"endpoint": endpoint,
                **Probe(lab, endpoint).inventory(bucket, unversioned_current_tags=True)})
            fleet.record("all-peer-exact-ACK-manifest", {**verify_seed(expected, inventory), **compare_inventories(baseline, inventory)})
        fleet.stop()
        require(physical_content(rebuilt) == physical_content(fleet.snapshot("after-S3-physical", quiescent=True)), "S3 verification changed the proven physical result")
        result.update(verdict="PASS", all128ACKsPreserved=True, allPeerS3ManifestEqual=True, syntheticFormatOrUUIDUsed=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try:
                fleet.stop()
                for peer in nodes:
                    fleet.record("B04-failure-format-and-original", {"node": peer, **fleet.remote(peer, "replacement-state")})
            except Exception as evidence_error:
                result["failureEvidenceError"] = str(evidence_error)
    finally:
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
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
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal nodes required")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)) and bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "verified workflow candidate required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "B04 executor omitted a source branch")
    results, donor_error = [], None
    try:
        donor = foreign_donor(args, nodes[0])
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, selected, branch, donor))
                if results[-1]["cleanup"] != "PASS": break
            if results[-1]["cleanup"] != "PASS": break
    except (Exception, KeyboardInterrupt) as exc:
        donor_error = {"type": type(exc).__name__, "message": str(exc)}
    finally:
        complete = len(results) == 4 and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "donorFailure": donor_error,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "B04 requires both source branches on H1/H2, released foreign donor and complete cleanup")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal unformatted replacement validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
