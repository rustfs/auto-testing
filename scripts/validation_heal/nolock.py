#!/usr/bin/env python3
"""A09 normalized public races and separately bound native storage fences."""

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
    from .shards import files, validate_repaired_physical
    from .cursor import validate_missing_batch, verify_part_manifest
    from .versions import public_version_id
    from .race import COPY_CANDIDATE, COPY_REFERENCE, map_fixture as map_race_fixture
    from .race_native_evidence import verify as verify_native
    from .race_io import set_fleet as set_race_io_cap
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import files, validate_repaired_physical
    from cursor import validate_missing_batch, verify_part_manifest
    from versions import public_version_id
    from race import COPY_CANDIDATE, COPY_REFERENCE, map_fixture as map_race_fixture
    from race_native_evidence import verify as verify_native
    from race_io import set_fleet as set_race_io_cap


CASE_ID = "heal.admin-nolock-safety"
BRANCHES = ("overwrite-nolock", "delete-nolock", "native-lock-fence")
BLACK_BOX = BRANCHES[:2]
OBJECTS = 257
SIZE = 257 * 1024 + 123
TARGET_KEY = "objects/0256.bin"
REFERENCE_KEY = "objects/0000.bin"
REFERENCE = {
    "candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "sourceFiles": {
        "rustfs/src/admin/handlers/heal.rs": "7e4007c29a8329ced8e48b88111400e79cffd845bd6ddc6243b79f3cd48e8199",
        "crates/heal/src/heal/channel.rs": "7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0"},
    "requested": {**BASE_OPTIONS, "recursive": True, "nolock": True, "pool": None, "set": None},
    "effective": {**BASE_OPTIONS, "recursive": True, "nolock": False, "pool": None, "set": None},
    "nativeFence": "pinned release has no reviewed e2e-test-hooks artifact or exact Admin lock/commit barrier",
}
NATIVE_REFERENCE = {**REFERENCE, 'candidateCommit': COPY_CANDIDATE,
    'sourceFiles': dict(COPY_REFERENCE['sourceFiles']),
    'nativeFence': 'exact-source default Deep exclusive owner fences queued PUT and exact historical DELETE'}
NATIVE_GROUP = ['default-heal-exclusive-owner']


def source_contract(candidate):
    for reference in (REFERENCE, NATIVE_REFERENCE):
        if candidate.get('source_sha') == reference['candidateCommit']:
            return reference
    raise ValueError('A09 candidate contract is not reviewed')


def native_preflight(candidate, testing_commit, directory):
    reference = source_contract(candidate)
    if reference is REFERENCE:
        return None
    require(bool(directory), 'A09 requires exact-source native proof before fleet mutation')
    proof = verify_native(directory, candidate, testing_commit, CASE_ID)
    require(proof['requiredRegressionLabels'] == NATIVE_GROUP and proof['requiredNativeSubsetPassed'] is True
            and proof['wholeCasePassClaimed'] is False and proof['canonicalCasePassClaimed'] is False,
            'A09 exact default-owner native group is incomplete')
    return proof


class WindowNotObserved(ValueError):
    """No observable client overlap; this is not a product success."""


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
                    and result.get("DeleteMarker") in {None, False}, "A09 PUT ACK lacks ETag or changed kind")
            if self.versioned:
                require(isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int
                        and (params["Key"], version) not in self.seen, "A09 PUT ACK has an invalid/reused VersionId")
                self.seen.add((params["Key"], version))
            else:
                require(version in {None, "null"}, "A09 never-versioned PUT created a real version")
        elif operation == "delete_object":
            require(self.versioned and params.get("VersionId") in {
                value for key, value in self.seen if key == params["Key"]}
                and version == params["VersionId"] and result.get("DeleteMarker") in {None, False},
                "A09 DELETE ACK changed the exact known data-version identity")
        self.pending = None
        return result


def settings(observed, reference=REFERENCE):
    require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
            "A09 status has an unknown or failed HTTP response")
    value = observed["response"]
    require(value.get("summary") in {"running", "finished"}, "A09 original task disappeared, stopped or failed")
    # Canonical JSON distinguishes bool/int and preserves raw null selectors.
    require(canonical(value.get("settings")) == canonical(reference["effective"]),
            "A09 Admin nolock was not safely normalized or changed unrelated options")
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
    require(branch in BLACK_BOX and type(count) is int and 3 <= count <= OBJECTS, "A09 unregistered fixture")
    recorder.call("create_bucket", {"Bucket": bucket})
    if branch == "delete-nolock":
        recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    entries = []
    keys = [f"objects/{index:04d}.bin" for index in range(count - 1)] + [TARGET_KEY]
    for key in keys:
        versions = 3 if key == TARGET_KEY and branch == "delete-nolock" else 1
        for generation in range(versions):
            nonce = f"{bucket}/{key}/{generation}"
            body = payload(nonce, SIZE)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "A09", "nonce": nonce},
                "Tagging": "suite=heal&fixture=A09"}, body)
            entries.append({"key": key, "versionId": public_version_id(response.get("VersionId")),
                "isLatest": generation == versions - 1, "deleteMarker": False,
                "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
    return entries


def operation_plan(bucket, branch, expected):
    require(branch in BLACK_BOX, "A09 unregistered foreground branch")
    if branch == "overwrite-nolock":
        # Three distinct serial commits; the final independent reference was
        # written before injection, allowing a coding-index byte comparison.
        return [{"operation": "put_object", "params": {"Bucket": bucket, "Key": TARGET_KEY,
                    "ContentType": "application/octet-stream", "Metadata": {"nonce": str(index)},
                    "Tagging": "race=overwrite"},
                 "nonce": f"{bucket}/overwrite/{index}" if index < 2 else f"{bucket}/{REFERENCE_KEY}/0"}
                for index in range(3)]
    target = [row for row in expected if row["key"] == TARGET_KEY]
    require(len(target) == 3 and len({row["versionId"] for row in target}) == 3
            and target[-1]["isLatest"], "A09 DV lacks three ACK-ordered versions")
    return [{"operation": "delete_object", "params": {"Bucket": bucket, "Key": TARGET_KEY,
                                                      "VersionId": target[0]["versionId"]}}]


def final_inventory(baseline, branch, plan, responses):
    require(len(plan) == len(responses) == (3 if branch == "overwrite-nolock" else 1),
            "A09 foreground plan/ACK sequence is incomplete")
    result = copy.deepcopy(baseline)
    if branch == "overwrite-nolock":
        entry = next(row for row in result["entries"] if row["key"] == TARGET_KEY)
        step, ack = plan[-1], responses[-1]
        body = payload(step["nonce"], SIZE)
        require(isinstance(ack.get("ETag"), str) and ack["ETag"]
                and ack.get("VersionId") in {None, "null"}, "A09 last PUT ACK lost its identity")
        entry.update(payload={"size": len(body), "sha256": hashlib.sha256(body).hexdigest()},
                     tags=[{"Key": "race", "Value": "overwrite"}])
        entry["properties"].update(ETag=ack["ETag"], ContentLength=len(body), ContentType="application/octet-stream",
                                   Metadata=step["params"]["Metadata"])
    else:
        version = plan[0]["params"]["VersionId"]
        require(responses[0].get("VersionId") == version, "A09 DELETE response changed the selected VersionId")
        result["entries"] = [row for row in result["entries"]
                             if (row["key"], row["versionId"]) != (TARGET_KEY, version)]
        require(len(result["entries"]) + 1 == len(baseline["entries"]), "A09 deleted a foreign/missing version")
    return result


def object_scene(snapshot, bucket, key):
    return {node: {**value, "files": [row for row in value["files"]
                                    if row["path"].startswith(f"{bucket}/{key}/")]}
            for node, value in snapshot.items()}


def map_fixture(snapshot, bucket, expected):
    mapped = map_race_fixture(snapshot, bucket, expected)
    require(len(mapped) == len(expected), "A09 physical map omitted/duplicated an ACK")
    return mapped


def physical_transition(before, after, bucket, mapped, final, branch):
    """Prove current ACK authority before allowing any GET or backup rescue."""
    require(all(before[node]["formats"] == after[node]["formats"] for node in before),
            "A09 race changed an original disk format")
    known = {row["key"] for row in final["entries"]}
    require(all(any(row["path"].startswith(f"{bucket}/{key}/") for key in known)
                for value in after.values() for row in value["files"]), "A09 created an unacknowledged physical key")
    for (key, version), target in mapped.items():
        if key == TARGET_KEY: continue
        validate_repaired_physical(object_scene(before, bucket, key), object_scene(after, bucket, key), target)
    survivors = map_fixture(after, bucket, final["entries"])
    original_target = [value for (key, _), value in mapped.items() if key == TARGET_KEY]
    current_target = [value for (key, _), value in survivors.items() if key == TARGET_KEY]
    if branch == "overwrite-nolock":
        require(len(original_target) == len(current_target) == 1
                and original_target[0]["dataDir"] != current_target[0]["dataDir"],
                "A09 overwrite retained the old physical generation")
        reference = next(target for (key, _), target in mapped.items() if key == REFERENCE_KEY)
        require({part["index"]: (part["sha256"], part["size"]) for part in reference["cohort"]}
                == {part["index"]: (part["sha256"], part["size"]) for part in current_target[0]["cohort"]},
                "A09 last ACK shard bytes/coding indices differ from the independent payload reference")
    else:
        remaining = {(row["key"], row["versionId"]) for row in final["entries"]}
        removed = [value for pair, value in mapped.items() if pair not in remaining]
        require(len(removed) == 1, "A09 delete transition lacks one exact deleted version")
        for target in current_target:
            original = mapped[TARGET_KEY, target["versionId"]]
            require(target["logical"] == original["logical"]
                    and [(row["index"], row["sha256"], row["size"]) for row in target["cohort"]]
                    == [(row["index"], row["sha256"], row["size"]) for row in original["cohort"]],
                    "A09 delete changed another retained ACK generation")
    current_dirs = {target["dataDir"] for target in current_target}
    for value in after.values():
        for row in value["files"]:
            prefix = f"{bucket}/{TARGET_KEY}/"
            if not row["path"].startswith(prefix): continue
            suffix = row["path"][len(prefix):]
            require(suffix == "xl.meta" or suffix.split("/")[0] in current_dirs,
                    "A09 resurrected an old/deleted data directory or left an unbound target leaf")
    return survivors, {"allControlShardsRestored": True, "lastACKGenerationVerified": True,
                        "physicalProofBeforeGETOrRescue": True, "oldVersionResurrected": False}


def terminal_proof(status, bucket, before, final):
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "finished" and outcome.get("execution", {}).get("state") == "completed"
            and outcome.get("coverage") == "complete", "A09 task lacks complete successful terminal coverage")
    old = {(row["key"], row["versionId"]) for row in before["entries"]}
    new = {(row["key"], row["versionId"]) for row in final["entries"]}
    counters = outcome.get("counters") or {}
    require(all(type(counters.get(key)) is int and counters[key] >= 0
                for key in ("processed", "healed", "unchanged", "failed", "unknown", "skipped", "attemptFailures"))
            and all(counters[key] == 0 for key in ("failed", "unknown", "skipped", "attemptFailures"))
            and counters.get("overflowed") is False and counters["processed"] == counters["healed"] + counters["unchanged"]
            and len(old & new) <= counters["processed"] <= len(old | new),
            "A09 terminal responsibility exceeds ACK bounds or hides unknown/failed work")
    receipts = outcome.get("objects")
    require(isinstance(receipts, list) and 0 < len(receipts) <= 128
            and outcome.get("objectsTruncated") is (counters["processed"] > 128),
            "A09 confused truncated diagnostics with its independent complete ACK manifest")
    seen, incarnation = set(), None
    for receipt in receipts:
        identity = receipt.get("identity") or {}
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(identity.get("bucket") == bucket and pair in old | new and pair not in seen
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and isinstance(identity.get("bucketIncarnationId"), str) and identity["bucketIncarnationId"]
                and receipt.get("disposition", {}).get("state") in {"repaired", "verified_healthy"},
                "A09 diagnostics changed version/incarnation/scope or claimed unknown/false health")
        incarnation = incarnation or identity["bucketIncarnationId"]
        require(incarnation == identity["bucketIncarnationId"], "A09 diagnostics mix incarnations")
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
            raise WindowNotObserved("A09 task finished before its partial-progress race window")
        if type(processed) is int and 0 < processed < total: return observed
        if time.monotonic() >= deadline:
            raise WindowNotObserved("A09 nonzero partial progress was not observed within 30 seconds")
        time.sleep(.05)


def wait_terminal(probe, fleet, path, token, reference=REFERENCE):
    deadline = time.monotonic() + 240
    while True:
        current = observe(probe, fleet, path, token, reference)
        if current["response"]["summary"] == "finished": return current["response"]
        require(time.monotonic() < deadline, "A09 original task exceeded its terminal budget")
        time.sleep(.2)


def foreground(recorder, plan):
    responses = []
    for step in plan:
        body = payload(step["nonce"], SIZE) if step["operation"] == "put_object" else None
        responses.append(recorder.call(step["operation"], step["params"], body))
    return responses


def race(recorder, plan, probe, fleet, path, token, reference=REFERENCE):
    samples = []
    # Only the worker mutates Recorder state. All Fleet.record writes run on
    # the controller, so evidence sequence allocation never races.
    with ThreadPoolExecutor(max_workers=1) as pool:
        worker = pool.submit(foreground, recorder, plan)
        deadline = time.monotonic() + 120
        try:
            while not worker.done() and time.monotonic() < deadline:
                samples.append(observe(probe, fleet, path, token, reference))
                time.sleep(.01)
        except BaseException:
            recorder.mutations.poison("status observation failed; drain in-flight mutations only")
            raise
        if not worker.done(): recorder.mutations.poison("foreground deadline; drain only")
        responses = worker.result(timeout=60)
    acks = [json.loads(path.read_text()) for path in sorted(recorder.output.glob("*-ack.json"))]
    require(len(acks) == len(plan) and all(row.get("operationCheck") == "PASS" for row in acks),
            "A09 lacks every durable foreground ACK")
    overlaps = [any(sample_overlap(ack, sample, path, token, reference) for sample in samples) for ack in acks]
    fleet.record("public-call-overlap-proof", {"everyMutationOverlapped": all(overlaps), "mutations": overlaps,
        "nativeFenceProved": False, "serverCommitOverlapProved": False})
    return responses, all(overlaps)


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True, exist_ok=False)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="manual")
    candidate = json.loads(args.candidate.read_text())
    reference = source_contract(candidate)
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
    fleet.record("source-contract", {**reference, "branch": branch, "objectCount": OBJECTS, "objectSize": SIZE,
        "requiredPUTs": 3 if branch == BLACK_BOX[0] else 0, "serverCommitOverlapProved": False})
    probe, active, recorder, writer = Probe(binding), [], None, None
    result = {"topology": topology, "branch": branch, "fixture": "DL" if branch == BLACK_BOX[0] else "DV",
              "verdict": "RUNNING", "cleanup": "PENDING", "canonicalCasePassClaimed": False}
    changed_target_registered, target_authority = None, False
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = RaceRecorder(probe, directory / "seed-ACKs", versioned=branch == BLACK_BOX[1])
        expected = seed_fixture(recorder, bucket, branch)
        baseline = fleet.record("complete-baseline-S3", probe.inventory(bucket, unversioned_current_tags=branch == BLACK_BOX[0]))
        verify_seed(expected, baseline)
        plan = operation_plan(bucket, branch, expected)
        fleet.record("frozen-foreground-plan", plan)
        fleet.stop()
        before = fleet.snapshot("complete-baseline-physical", quiescent=True)
        mapped = map_fixture(before, bucket, expected)
        verify_part_manifest(fleet, list(mapped.values()), "baseline-streaming", count=len(mapped))
        earliest_target = next(entry for entry in expected if entry["key"] == TARGET_KEY)
        targets = [mapped[row["key"], row["versionId"]] for row in expected
                   if row["key"] != TARGET_KEY or row["versionId"] == earliest_target["versionId"]]
        require(len(targets) == OBJECTS, "A09 fault plan omitted a key or changed its source dimension")
        receipts = []
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "faultId": bounded_fault_id("A09", branch, index)}
            fleet.faults.append((target["node"], params))
            if target["key"] == TARGET_KEY: changed_target_registered = target["node"], params
            receipts.append(fleet.record("exact-missing-part", {"node": target["node"], **fleet.remote(
                target["node"], "apply-fault", **params, relativePath=target["relativePath"],
                expectedSha256=target["sha256"], kind="missing")}))
        faulted = fleet.snapshot("complete-fault-physical", quiescent=True)
        fleet.record("fault-batch-proof", validate_missing_batch(before, faulted, targets, receipts))
        fleet.start()
        fault_hosts = sorted({target['node'] for target in targets})
        set_race_io_cap(fleet, CASE_ID, branch, True, nodes=fault_hosts)
        require(physical_content(faulted) == physical_content(fleet.snapshot("attribution-before-START")),
                "NOT_OBSERVED: A09 missing shards changed before manual admission")
        path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="")
        fleet.record("prepared-Admin-request", {"path": path, "query": [], "options": reference["requested"]})
        start = fleet.record("original-Admin-start", probe.admin(path, [], json.dumps(reference["requested"]).encode()))
        token = start.get("response", {}).get("clientToken") if isinstance(start.get("response"), dict) else None
        require(start.get("httpStatus") == 200 and isinstance(token, str) and token, "A09 admission lacks an original token")
        active.append((path, token))
        wait_running(probe, fleet, path, token, len(expected), reference)
        witness = fleet.snapshot("target-missing-before-foreground")
        target = next(target for target in targets if target["key"] == TARGET_KEY)
        if (target["node"], target["disk"], target["relativePath"]) in files(witness):
            raise WindowNotObserved("A09 old target already repaired before foreground mutations")
        writer = RaceRecorder(probe, directory / "foreground-ACKs", versioned=branch == BLACK_BOX[1])
        writer.seen.update((row["key"], row["versionId"]) for row in expected)
        responses, overlapped = race(writer, plan, probe, fleet, path, token, reference)
        set_race_io_cap(fleet, CASE_ID, branch, False, nodes=fault_hosts)
        final = fleet.record("ACK-derived-final-manifest", final_inventory(baseline, branch, plan, responses))
        terminal = wait_terminal(probe, fleet, path, token, reference)
        active.remove((path, token))
        for _ in range(2):
            repeated = observe(probe, fleet, path, token, reference)["response"]
            require(canonical(repeated.get("outcome")) == canonical(terminal.get("outcome"))
                    and repeated["summary"] == terminal["summary"], "A09 terminal responsibility changed on repetition")
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
                bucket, unversioned_current_tags=branch == BLACK_BOX[0])})
            compare_inventories(final, current)
            if branch == BLACK_BOX[1]:
                negative = fleet.record("all-peer-deleted-version-GET", Probe(binding, endpoint).s3("get_object", plan[0]["params"]))
                response = negative.get("response") or {}
                require(negative.get("acknowledged") is False and negative.get("resultUnknown") is False
                        and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
                        and response.get("Error", {}).get("Code") == "NoSuchVersion",
                        "A09 deleted exact VersionId revived or failed for another reason")
        fleet.stop()
        require(physical_content(after) == physical_content(fleet.snapshot("physical-after-S3", quiescent=True)),
                "A09 final GET changed the already-proven physical result")
        result["terminal"] = terminal_proof(terminal, bucket, baseline, final)
        if not overlapped: raise WindowNotObserved("A09 did not observe every required SDK call overlap")
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
            result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def native_blocked(candidate, topology):
    require(topology in {"H1", "H2"}, "A09 native prerequisite lost its source topology")
    return {"topology": topology, "branch": BRANCHES[-1], "verdict": "BLOCKED", "cleanup": "PASS",
            "candidateCommit": candidate["source_sha"], "canonicalCasePassClaimed": False,
            "reason": REFERENCE["nativeFence"], "nativeFenceProved": False, "fleetExecutionClaimed": False}


def complete_results(results):
    wanted = {(topology, branch) for topology in ("H1", "H2") for branch in BRANCHES}
    actual = {(row["topology"], row["branch"]) for row in results}
    require(len(actual) == len(results) and actual <= wanted, "A09 duplicate or foreign topology/branch")
    # No blocked, omitted or best-effort native branch can complete A09.
    return actual == wanted and all(row["verdict"] == row["cleanup"] == "PASS" for row in results) and all(
        row.get("nativeFenceProved") is True for row in results if row["branch"] == BRANCHES[-1])


def main():
    if sys.argv[1:2] == ['preflight']:
        parser = argparse.ArgumentParser(description='Read-only A09 native admission before bootstrap')
        parser.add_argument('--candidate', type=Path, required=True)
        parser.add_argument('--case-id', choices=[CASE_ID], required=True)
        parser.add_argument('--testing-commit', required=True)
        args = parser.parse_args(sys.argv[2:])
        proof = native_preflight(json.loads(args.candidate.read_text()), args.testing_commit,
                                 os.environ.get('VALIDATION_HEAL_NATIVE_DIRECTORY'))
        print(json.dumps({'nativeProof':proof,'fleetMutationRequested':False,'canonicalCasePassClaimed':False}))
        return
    def interrupted(signum, frame): raise KeyboardInterrupt(f"A09 interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=[CASE_ID], required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "A09 executor omitted a source branch")
    require(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha) and re.fullmatch(r"[0-9a-f]{40}", args.testing_commit)
            and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "A09 immutable identities required")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes))
            and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "A09 requires four distinct current Heal nodes")
    candidate = json.loads(args.candidate.read_text())
    native = native_preflight(candidate, args.testing_commit, os.environ.get('VALIDATION_HEAL_NATIVE_DIRECTORY'))
    if native is None:
        results = [native_blocked(candidate, topology) for topology in ("H1", "H2")]
    else:
        save(args.evidence/'independent-native-nolock-subset.json', native)
        results = [{'caseId':CASE_ID,'topology':topology,'branch':BRANCHES[-1],'verdict':'PASS','cleanup':'PASS',
                    'candidateCommit':candidate['source_sha'],'nativeFenceProved':True,'nativeProof':native,
                    'nativePhysicalShapes':['4/2','16/4'],'fleetExecutionClaimed':False,
                    'distributedFleetExecutionClaimed':False,'canonicalCasePassClaimed':False}
                   for topology in ('H1','H2')]
    try:
        stop = False
        for topology in ("H1", "H2"):
            for branch in BLACK_BOX:
                row = run_branch(args, topology, nodes[:1] if topology == "H1" else nodes[:4], branch)
                results.append(row)
                if row["cleanup"] != "PASS":
                    stop = True
                    break
            if stop:
                break
    finally:
        complete = complete_results(results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": complete,
            "cleanup": "PASS" if all(row["cleanup"] == "PASS" for row in results) else "FAIL",
            "canonicalCasePassClaimed": False, "nativePrerequisitesBlocked": native is None})
    require(complete, "A09 full race/native coverage incomplete; every H1/H2 branch stays in the denominator")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"A09 validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
