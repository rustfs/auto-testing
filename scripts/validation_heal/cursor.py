#!/usr/bin/env python3
"""J03: real retained-window gaps with independent, complete DX verification."""

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

try:
    from .heal_probe import Probe, canonical, compare_inventories, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .shards import files, map_target, validate_repaired_physical
    from .versions import public_version_id
    from .fleet_node import resource_limits
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import files, map_target, validate_repaired_physical
    from versions import public_version_id
    from fleet_node import resource_limits


CASE_ID = "heal.incremental-cursor-and-truncation"
BRANCHES = ("within-window-cursor", "past-retention-gap", "cross-node-cursor", "terminal-truncation")
RETAINED_ITEMS = 1024
OBJECTS = RETAINED_ITEMS + 1
SIZE = 1024**2 + 73
TASK_SECONDS = 600
LIMIT_REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "path": "crates/heal/src/heal/task.rs",
    "sourceSha256": "81e8e2ec8807d1094095e444d0aaf8913f94114836bc1bc9f7953cbeff98d855",
    "retainedItems": RETAINED_ITEMS, "diagnosticObjects": 128,
    "contract": "sinceSeq is exclusive; nextSeq is the next sequence to allocate",
    "differentCandidates": "the measured terminal window must still actually exceed retention; no inferred gap"}


def wanted_pairs(expected, *, count=OBJECTS):
    wanted = {(entry["key"], public_version_id(entry["versionId"])): entry for entry in expected}
    require(type(count) is int and 129 <= count <= OBJECTS
            and len(wanted) == len(expected) == count and all(not entry["deleteMarker"]
            and pair[1] == "null" and entry["payload"]["size"] == SIZE for pair, entry in wanted.items()),
            "DX fixture must contain every pre-registered distinct external-size ACK")
    return wanted


class Window:
    """One endpoint's cursors, within one admitted task and deployment epoch."""

    def __init__(self, bucket, expected, token, endpoint, epoch):
        self.bucket, self.wanted = bucket, wanted_pairs(expected)
        require(isinstance(token, str) and token and isinstance(epoch, str) and epoch,
                "cursor lacks an admitted task/deployment epoch")
        self.token, self.endpoint, self.epoch = token, endpoint, epoch
        self.cursor, self.next_seq, self.min_seq = 0, 1, 1
        self.seen, self.running_windows, self.gaps = set(), 0, []

    def check(self, status, cursor):
        require(type(cursor) is int and cursor >= 0, "invalid exclusive cursor")
        next_seq, min_seq = status.get("nextSeq"), status.get("minSeq")
        require(type(next_seq) is int and type(min_seq) is int and 1 <= min_seq <= next_seq <= OBJECTS + 1,
                "cursor response omitted or corrupted its actual sequence bounds")
        require(status.get("summary") in {"running", "finished"}, "cursor task failed, stopped or disappeared")
        truncated = status.get("truncated", False)
        require(type(truncated) is bool, "cursor truncation flag is not boolean")
        gap = cursor + 1 < min_seq
        require(not gap or truncated, "past-retention gap was silently omitted")
        items = status.get("items", [])
        require(isinstance(items, list) and len(items) <= RETAINED_ITEMS, "invalid retained item window")
        first = max(cursor + 1, min_seq)
        available = max(0, next_seq - first)
        require(len(items) <= available and (len(items) == available or truncated),
                "cursor items silently skip sequences or exceed their bounds")
        pairs = []
        for item in items:
            pair = item.get("object"), public_version_id(item.get("versionId"))
            require(item.get("bucket") == self.bucket and pair in self.wanted
                    and item.get("objectSize") == self.wanted[pair]["payload"]["size"],
                    "retained item has the wrong bucket/key/version or acknowledged size")
            pairs.append(pair)
        require(len(pairs) == len(set(pairs)), "retained window duplicated an object/version")
        return {"nextSeq": next_seq, "minSeq": min_seq, "retentionGap": gap,
                "payloadTruncated": len(items) != available, "firstReturnedSeq": first,
                "pairs": pairs, "returnedItems": len(items), "truncated": truncated}

    def consume(self, status, cursor, *, token, endpoint, epoch):
        require((token, endpoint, epoch) == (self.token, self.endpoint, self.epoch),
                "cursor crossed a task, endpoint or deployment epoch")
        require(cursor == self.cursor, "cursor jumped over an unconsumed sequence")
        observed = self.check(status, cursor)
        require(observed["nextSeq"] >= self.next_seq and observed["minSeq"] >= self.min_seq,
                "same-task cursor bounds moved backwards")
        require(not self.seen.intersection(observed["pairs"]), "incremental cursor duplicated an earlier item")
        self.seen.update(observed["pairs"])
        if observed["retentionGap"]:
            self.gaps.append({"sinceSeq": cursor, "minSeq": observed["minSeq"], "truncated": True})
        # The API may trim its serialized payload separately from item retention.
        # Advance over received items only, preserving any unreturned suffix.
        if observed["returnedItems"]:
            self.cursor = observed["firstReturnedSeq"] + observed["returnedItems"] - 1
        elif not observed["payloadTruncated"]:
            self.cursor = max(self.cursor, observed["nextSeq"] - 1)
        counters = (status.get("outcome") or {}).get("counters", {})
        processed = counters.get("processed")
        if (status["summary"] == "running" and type(processed) is int and 0 < processed < OBJECTS
                and observed["returnedItems"] and not observed["retentionGap"] and not observed["payloadTruncated"]):
            self.running_windows += 1
        self.next_seq, self.min_seq = observed["nextSeq"], observed["minSeq"]
        return {name: value for name, value in observed.items() if name != "pairs"} | {
            "exclusiveNextCursor": self.cursor, "taskId": token, "endpoint": endpoint, "deploymentEpoch": epoch}


def complete_outcome(status, bucket, expected, repaired):
    wanted = wanted_pairs(expected)
    require(repaired and set(repaired) <= set(wanted), "cursor repair plan is empty or escapes ACKs")
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "finished" and outcome.get("execution", {}).get("state") == "completed"
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is True,
            "terminal cursor task omitted its diagnostic truncation or complete coverage")
    counters = outcome.get("counters", {})
    counts = {"processed": OBJECTS, "healed": len(repaired), "unchanged": OBJECTS - len(repaired),
              "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in counts.items()),
            "terminal canonical counters do not cover every frozen object and real missing shard")
    receipts = outcome.get("objects")
    require(isinstance(receipts, list) and 0 < len(receipts) <= 128 < OBJECTS,
            "diagnostic receipts were incorrectly treated as a complete responsibility manifest")
    seen, incarnation = set(), None
    for receipt in receipts:
        identity = receipt.get("identity", {})
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(identity.get("bucket") == bucket and pair in wanted and pair not in seen
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and isinstance(identity.get("bucketIncarnationId"), str) and identity["bucketIncarnationId"],
                "bounded diagnostic receipt has a wrong, duplicate or incomplete identity")
        incarnation = incarnation or identity["bucketIncarnationId"]
        require(incarnation == identity["bucketIncarnationId"], "diagnostics mixed bucket incarnations")
        require(receipt.get("disposition", {}).get("state") == ("repaired" if pair in repaired else "verified_healthy"),
                "bounded diagnostics claimed false repair, unknown responsibility or incomplete integrity")
        seen.add(pair)
    return {"canonicalCounters": counters, "bucketIncarnationId": incarnation,
            "diagnosticObjects": len(receipts), "diagnosticObjectsAreCompleteManifest": False}


def terminal_proof(status, bucket, expected, repaired, window):
    proof = complete_outcome(status, bucket, expected, repaired)
    retained = window.check(status, 0)
    require(retained["retentionGap"] and retained["truncated"] and retained["nextSeq"] == OBJECTS + 1,
            "terminal task never actually exceeded its retained item window")
    return {**proof, "retainedWindow": {name: value for name, value in retained.items() if name != "pairs"}}


def map_cohorts(snapshot, bucket, expected, *, count=OBJECTS):
    """Index once, then use the existing complete-set oracle on each object."""
    wanted_pairs(expected, count=count)
    by_key = {entry["key"]: {node: {"formats": value["formats"], "files": []}
                             for node, value in snapshot.items()} for entry in expected}
    for node, observation in snapshot.items():
        for entry in observation["files"]:
            require(entry["path"].startswith(bucket + "/"), "physical manifest contains an unowned bucket")
            suffix = entry["path"][len(bucket) + 1:]
            key = suffix.removesuffix("/xl.meta") if suffix.endswith("/xl.meta") else suffix.rsplit("/", 2)[0]
            require(key in by_key, "physical baseline contains an unacknowledged object")
            by_key[key][node]["files"].append(entry)
    return [map_target(by_key[entry["key"]], bucket, entry["key"], "data" if index % 2 == 0 else "parity", 1)
            for index, entry in enumerate(expected)]


def verify_all_parts(fleet, cohorts, label, *, count=OBJECTS):
    require(type(count) is int and 129 <= count <= OBJECTS and len(cohorts) == count,
            "complete physical verification differs from its frozen object count")
    width = 4 if len(fleet.nodes) == 1 else 16
    identities = [(entry["node"], entry["disk"], entry["relativePath"], entry["versionId"], entry["partNumber"])
                  for target in cohorts for entry in target["cohort"]]
    require(all(len(target["cohort"]) == width for target in cohorts)
            and len(identities) == len(set(identities)) == count * width
            and all(entry[0] in fleet.nodes for entry in identities), "complete part manifest duplicated or omitted a frozen shard")
    total = 0
    for node in fleet.nodes:
        entries = [shard for target in cohorts for shard in target["cohort"] if shard["node"] == node]
        for offset in range(0, len(entries), 128):
            batch = entries[offset:offset + 128]
            observed = fleet.record(label, {"node": node, **fleet.remote(node, "verify-parts", parts=[{
                name: entry[name] for name in ("disk", "relativePath", "versionId", "partNumber")} for entry in batch])})
            require(observed.get("complete") is True and len(observed.get("parts", [])) == len(batch),
                    "full external-part verifier omitted a batch item")
            for entry, proof in zip(batch, observed["parts"], strict=True):
                require(proof.get("physicalBitrotVerified") is True
                        and all(proof.get(name) == entry[name] for name in ("disk", "relativePath", "versionId", "partNumber", "sha256"))
                        and proof.get("physicalSize") == entry["size"], "external part differs from its frozen streaming/hash oracle")
            total += len(batch)
    require(total == count * width, "full external-part verifier missed a disk/object")
    return {"allExternalObjects": count, "allPartsStreamingVerified": total,
            "sameRoleOriginalShardBytesRestored": True}


def validate_missing_batch(before, after, targets, receipts):
    left, right = files(before), files(after)
    require(len(targets) == len(receipts) > 1, "cursor fault group omitted its exact receipts")
    removed = set()
    for target, receipt in zip(targets, receipts, strict=True):
        identity = target["node"], target["disk"], target["relativePath"]
        require(identity in left and identity not in right and identity not in removed
                and receipt.get("relativePath") == target["relativePath"] and receipt.get("fault") is None
                and receipt.get("before") == {"sha256": target["sha256"], "size": target["size"]},
                "cursor missing shard was not physically witnessed on its frozen drive")
        removed.add(identity)
    require({key: value for key, value in left.items() if key not in removed} == right
            and all(before[node]["formats"] == after[node]["formats"] for node in before),
            "cursor fault group changed metadata, a control shard or disk format")
    return {"exactMissingParts": len(removed), "otherFilesAndFormatsUnchanged": True}


def observe_task(probe, fleet, binding, bucket, expected, targets, active, *, original=None):
    path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/objects/"
    start = fleet.record("trigger", probe.admin(path, [], json.dumps({**BASE_OPTIONS, "recursive": True}).encode()))
    require(start.get("httpStatus") == 200 and isinstance(start.get("response"), dict), "cursor task admission failed")
    token = start["response"].get("clientToken")
    require(isinstance(token, str) and token, "cursor task omitted its real token")
    active.append((path, token))
    epoch = fleet.run_id + "/" + token
    window = Window(bucket, expected, token, "node1", epoch)
    fleet.record("cursor-identity", {"taskId": token, "path": path, "deploymentEpoch": epoch,
        "processRestartedDuringTask": False, "responseStartTimeIsEpoch": False,
        "reason": "candidate renders startTime on each response; use admission token and uninterrupted deployment"})
    deadline = time.monotonic() + TASK_SECONDS
    while True:
        cursor = window.cursor
        response = fleet.record("cursor-control", probe.admin(path, [("clientToken", token), ("sinceSeq", str(cursor))], b""))
        require(response.get("httpStatus") == 200 and isinstance(response.get("response"), dict), "incremental status failed")
        status = response["response"]
        fleet.record("cursor-window-proof", window.consume(status, cursor, token=token, endpoint="node1", epoch=epoch))
        if status["summary"] == "finished" and window.cursor == window.next_seq - 1:
            break
        require(time.monotonic() < deadline, "incremental task/retained suffix exceeded its pre-registered deadline")
        time.sleep(0.05)
    active.remove((path, token))
    repaired = {(entry["key"], entry["versionId"]) for entry in targets}
    proofs, old_status = {}, None
    for endpoint in binding["endpoints"]:
        client = Probe(binding, endpoint)
        peer_window = Window(bucket, expected, token, endpoint, epoch)
        observed = fleet.record("terminal-old-cursor", client.admin(path, [("clientToken", token), ("sinceSeq", "0")], b""))
        require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
                "terminal old cursor failed on an actual bound endpoint")
        current = observed["response"]
        proofs[endpoint] = terminal_proof(current, bucket, expected, repaired, peer_window)
        require(canonical(current.get("outcome")) == canonical(status.get("outcome")),
                "terminal responsibility changed or crossed a task on another endpoint")
        if old_status is not None:
            require(all(canonical(current.get(name)) == canonical(old_status.get(name)) for name in
                        ("summary", "nextSeq", "minSeq", "items", "truncated", "outcome")),
                    "same-token terminal cursor window differs across nodes")
        old_status = current
        caught_up = fleet.record("terminal-caught-up", client.admin(path,
            [("clientToken", token), ("sinceSeq", str(current["nextSeq"] - 1))], b""))
        require(caught_up.get("httpStatus") == 200 and isinstance(caught_up.get("response"), dict), "caught-up cursor failed")
        empty = peer_window.check(caught_up["response"], current["nextSeq"] - 1)
        require(empty["returnedItems"] == 0 and empty["nextSeq"] == current["nextSeq"]
                and empty["minSeq"] == current["minSeq"]
                and canonical(caught_up["response"].get("outcome")) == canonical(current.get("outcome")),
                "caught-up terminal cursor repeated data or changed responsibility")
    return {"within-window-cursor": {"verdict": "PASS" if window.running_windows >= 2 else "NOT_OBSERVED",
                "nonemptyRunningIncrementalWindows": window.running_windows, "duplicateItems": 0,
                "reason": "" if window.running_windows >= 2 else "two useful running windows were not observed; no timing success claim"},
            "past-retention-gap": {"verdict": "PASS", "oldCursor": 0, "endpointProofs": proofs, "incrementalGaps": window.gaps},
            "cross-node-cursor": {"verdict": "PASS" if len(binding["endpoints"]) > 1 else "NOT_APPLICABLE",
                "queriedEndpoints": list(binding["endpoints"]), "sameTaskId": token, "sameDeploymentEpoch": epoch,
                "reason": "H1 is one physical node; actual cross-node reads are required and performed on H2"},
            "terminal-truncation": {"verdict": "PASS", "endpointProofs": proofs,
                "completeManifestEvidenceRequired": True}}


def run_topology(args, topology, nodes, *, observer=observe_task, branches=BRANCHES,
                 count=OBJECTS, fixture_builder=None, missing_indices=None,
                 source_branches=None, runtime_profile="manual", policy_reference=LIMIT_REFERENCE):
    directory = args.evidence / topology
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, resource_profile="cursor", runtime_profile=runtime_profile)
    require(type(count) is int and 129 <= count <= OBJECTS, "invalid pre-registered DX size")
    missing_indices = list(range(0, count, 64)) if missing_indices is None else list(missing_indices)
    require(len(missing_indices) > 1 and len(set(missing_indices)) == len(missing_indices)
            and all(type(index) is int and 0 <= index < count for index in missing_indices), "invalid DX fault indices")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    candidate = json.loads(args.candidate.read_text())
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(),
        "bucketPrefix": bucket, "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": count, "maxInventoryBytes": count * SIZE}
    fleet.record("binding", binding)
    fleet.record("pre-registered-budget", {"fixture": "DX", "objects": count, "objectBytes": SIZE,
        "logicalBytes": count * SIZE, "taskSeconds": TASK_SECONDS, "sourceLimitReference": policy_reference,
        "maxSnapshotFilesPerNode": resource_limits("cursor")["maxSnapshotFiles"], "snapshotSecondsPerNode": 600, "verificationBatchItems": 128,
        "missingIndices": missing_indices, "taskScope": "frozen prefix scopes; one result per ACK",
        "faultLeaseMinutes": 30, "serviceRuntimeSeconds": 2700, "topologyLeaseMinutes": 45})
    recipe = select(args.case_id)[0]
    require(recipe["variants"] == list(source_branches or branches) and set(branches) <= set(recipe["variants"]),
            "DX executor omitted or invented a source branch")
    result = {"topology": topology, "fixture": "DX", "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    active, probe = [], Probe(binding)
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = Recorder(probe, directory / "fixture")
        expected = fixture_builder(recorder, bucket) if fixture_builder else seed(recorder, bucket, "DX", count, [SIZE])
        wanted_pairs(expected, count=count)
        baseline = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=True, page_size=127))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        cohorts = map_cohorts(original, bucket, expected, count=count)
        fleet.record("all-external-baseline", verify_all_parts(fleet, cohorts, "baseline-part-batch", count=count))
        targets = [cohorts[index] for index in missing_indices]
        receipts = []
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "relativePath": target["relativePath"], "kind": "missing",
                "faultId": bounded_fault_id(recipe["code"], "dx-window", index), "expectedSha256": target["sha256"]}
            fleet.faults.append((target["node"], params))
            receipts.append(fleet.record("fault", {"node": target["node"], **fleet.remote(target["node"], "apply-fault", **params)}))
        faulted = fleet.snapshot("physical-fault", quiescent=True)
        fleet.record("fault-proof", validate_missing_batch(original, faulted, targets, receipts))
        fleet.start()
        require(physical_content(faulted) == physical_content(fleet.snapshot("physical-before-trigger")),
                "ATTRIBUTION_NOT_OBSERVED: DX fault changed before the admitted manual task")
        result["branches"] = observer(probe, fleet, binding, bucket, expected, targets, active, original=original)
        require(set(result["branches"]) == set(branches), "DX observer omitted or invented a source branch")
        fleet.stop()
        repaired = fleet.snapshot("physical-repaired-before-get", quiescent=True)
        fleet.record("physical-repair-proof", validate_repaired_physical(original, repaired, targets[0], repair_targets=targets))
        fleet.record("all-external-repair-proof", verify_all_parts(fleet, cohorts, "repaired-part-batch", count=count))
        fleet.start()
        for endpoint in binding["endpoints"]:
            inventory = fleet.record("s3-complete-after", {"endpoint": endpoint, **Probe(binding, endpoint).inventory(
                bucket, unversioned_current_tags=True, page_size=127)})
            fleet.record("independent-full-manifest-proof", {"endpoint": endpoint,
                **verify_seed(expected, inventory), **compare_inventories(baseline, inventory)})
        fleet.stop()
        require(physical_content(repaired) == physical_content(fleet.snapshot("physical-after-get", quiescent=True)),
                "final GET changed physical files; prior repair attribution was incomplete")
        result["completePhysicalAndS3ManifestsVerified"] = True
        if "terminal-truncation" in result["branches"]:
            result["branches"]["terminal-truncation"]["completePhysicalAndS3ManifestsVerified"] = True
        result["verdict"] = "PASS" if all(value["verdict"] == "PASS" or name == "cross-node-cursor"
            and topology == "H1" and value["verdict"] == "NOT_APPLICABLE" for name, value in result["branches"].items()) else "FAIL"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop()
            fleet.snapshot("physical-failure", quiescent=True)
        except Exception as evidence_error:
            result["failureEvidenceError"] = str(evidence_error)
    finally:
        for path, token in active:
            try:
                fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc:
                fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("topology-result", result)
    return result


def main(*, case_id=CASE_ID, observer=observe_task, branches=BRANCHES, runner=run_topology):
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--case-id", choices=(case_id,), required=True)
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
            results.append(runner(args, topology, selected, observer=observer, branches=branches))
            if results[-1]["cleanup"] != "PASS":
                break
    finally:
        complete = len(results) == 2 and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": args.case_id, "results": results,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, f"{args.case_id} requires every applicable H1/H2 branch; an unobserved running window is not PASS")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal cursor validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
