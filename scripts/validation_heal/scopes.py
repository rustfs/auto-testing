#!/usr/bin/env python3
"""A02: exact root/bucket/prefix/object and genuine erasure-set isolation."""

from __future__ import annotations

import argparse
import copy
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
    from .heal_fault import bounded_fault_id
    from .shards import files, map_target, validate_fault, validate_repaired_physical
    from .versions import public_version_id
    from .stop import idle_fleet
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import files, map_target, validate_fault, validate_repaired_physical
    from versions import public_version_id
    from stop import idle_fleet


CASE_ID = "heal.admin-scope-isolation"
BRANCHES = ("root", "bucket", "prefix", "object", "pool-set")
SIZE = 1024**2 + 73
KEYS = ("a/target.bin", "a/target.bin-suffix", "a/child/neighbor.bin", "a/中文 空%2F.bin",
        "a/literal/slash.bin", "a/literal%2Fslash.bin", "a/06.bin", "a/07.bin",
        "ab/control.bin", "ab/09.bin", "ab/10.bin", "ab/11.bin",
        "b/control.bin", "b/13.bin", "b/14.bin", "b/15.bin")
REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "sourceFiles": {"crates/heal/src/heal/channel.rs": "7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0",
                    "crates/heal/src/heal/task.rs": "81e8e2ec8807d1094095e444d0aaf8913f94114836bc1bc9f7953cbeff98d855",
                    "crates/ecstore/src/core/sets.rs": "06543b1d083f489346171ba489b1eb1e0b7899c844f0ebf15e16ad467cf5e2f1",
                    "crates/ecstore/src/layout/disks_layout.rs": "b143aa9fa97b2cd32f8a2cd548779e0656cab0f197988483ee8a92b5ab3f17f8"},
    "nullableIdentityScope": "unrestricted admitted selectors; exact ACK and physical set binding still required",
    "h2": "four original nodes, two 8-drive sets, two original mounts per node per set, disk-major endpoints",
    "h1": "original single-node four-drive set; no invented unselected erasure set"}


def seed_fixture(recorder, buckets, branch):
    expected = {}
    for bucket in buckets:
        recorder.call("create_bucket", {"Bucket": bucket})
        expected[bucket] = []
        for key in KEYS:
            body = payload(f"A02/{branch}/{bucket}/{key}", SIZE)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "Metadata": {"suite": "A02", "branch": branch}, "Tagging": "suite=heal&fixture=DS"}, body)
            require(response.get("VersionId") in {None, "null"}, "scope fixture must remain never-versioned")
            expected[bucket].append({"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
    return expected


def object_mapping(snapshot, expected, topology):
    validate_topology(snapshot, topology, expected_sets=2 if topology == "H2" else 1)
    indexed = files(snapshot)
    mapped = {}
    for bucket, entries in expected.items():
        for index, entry in enumerate(entries):
            identity = bucket, entry["key"]
            scopes = set()
            for node, observation in snapshot.items():
                for record in observation["formats"]:
                    if (node, record["disk"], f"{bucket}/{entry['key']}/xl.meta") not in indexed: continue
                    layout = record["value"]["xl"]
                    members = [number for number, group in enumerate(layout["sets"]) if layout["this"] in group]
                    require(len(members) == 1, "object member belongs to ambiguous erasure sets")
                    scopes.add(members[0])
            require(len(scopes) == 1, "object metadata lacks one exact physical set authority")
            set_index = next(iter(scopes))
            mapped[identity] = map_target(snapshot, bucket, entry["key"], "data" if index % 2 == 0 else "parity", 1,
                multi_set=topology == "H2", set_index=set_index)
    require(len(mapped) == sum(len(entries) for entries in expected.values()), "physical scope manifest omitted an ACK")
    if topology == "H2": require({target["setIndex"] for target in mapped.values()} == {0, 1}, "fixture did not populate both actual sets")
    return mapped


def selection(branch, primary, mapped):
    require(branch in BRANCHES and mapped, "invalid scope case or manifest")
    selected = {pair for pair, target in mapped.items() if branch == "root"
        or branch == "bucket" and pair[0] == primary
        or branch == "prefix" and pair[0] == primary and pair[1].startswith("a/")
        or branch == "object" and pair == (primary, "a/target.bin")
        or branch == "pool-set" and target["poolIndex"] == 0 and target["setIndex"] == 0}
    require(selected, "selector has no acknowledged object scope")
    return selected


def request_spec(branch, primary):
    options = {**BASE_OPTIONS, "recursive": branch != "object", "pool": None, "set": None}
    path = "/rustfs/admin/v3/heal/"
    if branch not in {"root", "pool-set"}:
        path += quote(primary, safe="")
        if branch == "prefix": path += "/" + quote("a/", safe="/")
        elif branch == "object": path += "/" + quote("a/target.bin", safe="/")
    if branch == "pool-set": options.update(pool=0, set=0)
    require(branch in BRANCHES, "unknown selector branch")
    return path, options


def fault_targets(primary, secondary, mapped, topology):
    pairs = [(primary, key) for key in ("a/target.bin", "a/child/neighbor.bin", "ab/control.bin", "b/control.bin")]
    pairs.append((secondary, "a/target.bin"))
    if topology == "H2":
        for number in (0, 1):
            if not any(mapped[pair]["setIndex"] == number for pair in pairs):
                pair = next(pair for pair, target in mapped.items() if target["setIndex"] == number)
                pairs.append(pair)
    require(len(pairs) == len(set(pairs)), "scope fault list reused an object")
    return [mapped[pair] for pair in pairs]


def responsibility(current, mapped, selected, repaired, options):
    outcome = current.get("outcome") or {}
    require(current.get("summary") == "finished" and current.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed" and outcome.get("coverage") == "complete"
            and outcome.get("objectsTruncated") is False, "scope task lacks complete traversal and diagnostics")
    require(repaired <= selected <= set(mapped) and repaired, "scope repair plan lacks selected faults or escaped ACKs")
    objects = outcome.get("objects")
    require(isinstance(objects, list) and len(objects) == len(selected), "scope receipts omitted or added an object")
    seen, incarnations, resolved = set(), {}, []
    for receipt in objects:
        item = receipt.get("identity", {})
        pair = item.get("bucket"), item.get("object")
        require(pair in selected and pair not in seen and item.get("kind") == "object"
                and public_version_id(item.get("versionId")) == "null", "scope receipt has a wrong/duplicate ACK identity")
        target = mapped[pair]
        scope = item.get("poolIndex"), item.get("setIndex")
        requested = options["pool"], options["set"]
        if requested != (None, None):
            require(all(type(value) is int for value in scope) and scope == requested == (target["poolIndex"], target["setIndex"]),
                    "selected pool/set receipt lost or changed explicit scope")
        elif scope != (None, None):
            require(all(type(value) is int for value in scope) and scope == (target["poolIndex"], target["setIndex"]),
                    "unrestricted receipt contradicts independently observed physical set")
        incarnation = item.get("bucketIncarnationId")
        require(isinstance(incarnation, str) and incarnation and incarnations.setdefault(pair[0], incarnation) == incarnation,
                "scope receipts lack or mix bucket incarnations")
        state = "repaired" if pair in repaired else "verified_healthy"
        require(receipt.get("disposition", {}).get("state") == state, "scope receipt claimed unknown, false repair or incomplete integrity")
        seen.add(pair)
        resolved.append({"bucket": pair[0], "key": pair[1], "rawSelectorScope": list(scope),
                         "physicalPoolIndex": target["poolIndex"], "physicalSetIndex": target["setIndex"],
                         "originalSetDiskUUIDs": [member["diskUUID"] for member in target["cohort"]]})
    counts = {"processed": len(selected), "healed": len(repaired), "unchanged": len(selected) - len(repaired),
              "failed": 0, "skipped": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(outcome.get("counters", {}).get(name)) is type(value) and outcome["counters"][name] == value
                for name, value in counts.items()), "scope counters differ from exact physical/ACK selection")
    return {"exactSelectedResponsibility": True, "rawIdentitiesNotRewritten": True, "physicalScopeBindings": resolved}


def physical_selection(original, repaired, targets, selected):
    repairs = [target for target in targets if (target["bucket"], target["key"]) in selected]
    controls = [target for target in targets if (target["bucket"], target["key"]) not in selected]
    expected = copy.deepcopy(original)
    missing = {(target["node"], target["disk"], target["relativePath"]) for target in controls}
    actual = files(repaired)
    for identity in missing: require(identity not in actual, "unselected control was repaired before the physical proof")
    for node, value in expected.items():
        value["files"] = [entry for entry in value["files"] if (node, entry["disk"], entry["path"]) not in missing]
    require(repairs, "physical isolation has no selected repair target")
    proof = validate_repaired_physical(expected, repaired, repairs[0], repair_targets=repairs)
    return {**proof, "selectedReconstructedParts": len(repairs), "unselectedStillMissingParts": len(controls),
            "beforeAnyTargetGETOrBackupRescue": True}


def verify_parts(fleet, mapped, label):
    members = [member for target in mapped.values() for member in target["cohort"]]
    identities = {(member["node"], member["disk"], member["relativePath"]) for member in members}
    require(len(identities) == len(members) and members, "part manifest duplicated or omitted a physical identity")
    for node in fleet.nodes:
        local = [member for member in members if member["node"] == node]
        for offset in range(0, len(local), 128):
            batch = local[offset:offset + 128]
            raw = fleet.record(label, {"node": node, **fleet.remote(node, "verify-parts", parts=[{
                name: member[name] for name in ("disk", "relativePath", "versionId", "partNumber")} for member in batch])})
            require(raw.get("complete") is True and len(raw.get("parts", [])) == len(batch), "scope streaming verifier omitted a shard")
            for member, proof in zip(batch, raw["parts"], strict=True):
                require(proof.get("physicalBitrotVerified") is True and proof.get("physicalSize") == member["size"]
                        and all(proof.get(name) == member[name] for name in ("disk", "relativePath", "versionId", "partNumber", "sha256")),
                        "scope part failed exact identity/hash/streaming verification")
    return {"exactStreamingParts": len(members), "exactObjects": len(mapped)}


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="scope-isolation" if topology == "H2" else "manual")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    buckets = [bucket, bucket + "-other"]
    fleet.snapshot_buckets = buckets
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] == REFERENCE["candidateCommit"], "scope/nullable selector contract is not reviewed for this candidate")
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), "maxInventoryObjects": 128,
        "maxInventoryBytes": 256 * 1024**2}
    fleet.record("source-bound-scope-contract", REFERENCE)
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    fleet.record("binding", binding)
    probe, active = Probe(binding), []
    result = {"topology": topology, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        expected = seed_fixture(Recorder(probe, directory / "fixture"), buckets, branch)
        baselines = {name: fleet.record("s3-baseline", probe.inventory(name, unversioned_current_tags=True)) for name in buckets}
        for name in buckets: fleet.record("ACK-manifest", {"bucket": name, "expected": expected[name], **verify_seed(expected[name], baselines[name])})
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        mapped = object_mapping(original, expected, topology)
        fleet.record("complete-physical-object-mapping", [{"bucket": pair[0], "key": pair[1], "pool": target["poolIndex"],
            "set": target["setIndex"], "members": [{key: member[key] for key in ("node", "disk", "diskUUID", "setSlot", "index")}
            for member in target["cohort"]]} for pair, target in mapped.items()])
        verify_parts(fleet, mapped, "streaming-baseline")
        selected = selection(branch, bucket, mapped)
        targets = fault_targets(*buckets, mapped, topology)
        selected_faults = {(target["bucket"], target["key"]) for target in targets} & selected
        path, options = request_spec(branch, bucket)
        fleet.record("exact-selector-plan", {"branch": branch, "path": path, "options": options,
            "selectedACKs": [list(pair) for pair in sorted(selected)], "selectedFaults": [list(pair) for pair in sorted(selected_faults)],
            "unselectedSetControl": "actual set 1" if topology == "H2" else "NOT_APPLICABLE: source H1 has one original four-drive set"})
        prior, registrations = original, []
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "faultId": bounded_fault_id("A02", branch, index)}
            fleet.faults.append((target["node"], params))
            registrations.append((target["node"], params, target))
            raw = fleet.record("fault", fleet.remote(target["node"], "apply-fault", **params,
                relativePath=target["relativePath"], expectedSha256=target["sha256"], kind="missing"))
            scene = fleet.snapshot("physical-after-fault", quiescent=True)
            validate_fault(prior, scene, target, raw)
            prior = scene
        fleet.start()
        require(physical_content(prior) == physical_content(fleet.snapshot("physical-before-trigger")),
                "ATTRIBUTION_NOT_OBSERVED: selector fault changed before admission")
        start = fleet.record("selector-admission", probe.admin(path, [], json.dumps(options).encode()))
        require(start.get("httpStatus") == 200 and isinstance(start.get("response"), dict), "selector admission failed")
        token = start["response"].get("clientToken")
        require(isinstance(token, str) and token, "selector admission omitted original token")
        active.append((path, token))
        deadline = time.monotonic() + 180
        while True:
            raw = fleet.record("selector-status", probe.admin(path, [("clientToken", token)], b""))
            require(raw.get("httpStatus") == 200 and isinstance(raw.get("response"), dict), "original selector task status failed")
            current = raw["response"]
            if current.get("summary") != "running": break
            require(time.monotonic() < deadline, "scope task exceeded registered deadline")
            time.sleep(.2)
        active.remove((path, token))
        outcome_proof = responsibility(current, mapped, selected, selected_faults, options)
        idle_fleet(fleet, binding)
        fleet.stop()
        repaired = fleet.snapshot("physical-after-selector-before-GET", quiescent=True)
        physical_proof = physical_selection(original, repaired, targets, selected)
        streaming_proof = verify_parts(fleet, {pair: mapped[pair] for pair in selected}, "selected-streaming-proof")
        # The unselected control backups are recovered only after preserving
        # the complete scope isolation proof, and never count as task repair.
        for node, params, target in registrations:
            if (target["bucket"], target["key"]) not in selected:
                raw = fleet.record("unselected-control-backup-rescue", fleet.remote(node, "recover-fault", **params))
                require(raw.get("physicalRepairPassAllowed") is False, "control restoration claimed product repair")
            fleet.faults.remove((node, params))
        recovered = fleet.snapshot("physical-after-control-rescue", quiescent=True)
        validate_repaired_physical(original, recovered, targets[0], repair_targets=targets)
        verify_parts(fleet, mapped, "complete-streaming-after")
        fleet.start()
        for endpoint in binding["endpoints"]:
            for name in buckets:
                actual = fleet.record("all-peer-S3-after", {"endpoint": endpoint, **Probe(binding, endpoint).inventory(name, unversioned_current_tags=True)})
                fleet.record("full-ACK-S3-proof", {"endpoint": endpoint, **verify_seed(expected[name], actual), **compare_inventories(baselines[name], actual)})
        fleet.stop()
        require(physical_content(recovered) == physical_content(fleet.snapshot("physical-after-GET", quiescent=True)), "S3 verification changed the proven selector result")
        result.update(verdict="PASS", taskId=token, responsibility=outcome_proof, physical=physical_proof, streaming=streaming_proof,
                      controlBackupRescueExcludedFromRepair=True)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("physical-failure", quiescent=True)
        except Exception as error: result["failureEvidenceError"] = str(error)
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"Heal scope interrupted by signal {signum}")
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
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal nodes required")
    require(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")) and re.fullmatch(r"[0-9a-f]{64}", args.binary_sha), "immutable workflow/candidate identities required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "scope executor omitted a source selector")
    results = []
    try:
        for topology, selected_nodes in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, selected_nodes, branch))
                if results[-1]["cleanup"] != "PASS": break
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == 10 and all(row["verdict"] == "PASS" and row["cleanup"] == "PASS" for row in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL", "complete": complete})
    require(complete, "A02 requires every real selector on H1/H2, exact physical scope isolation and cleanup")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal scope validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
