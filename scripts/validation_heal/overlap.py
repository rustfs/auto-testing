#!/usr/bin/env python3
"""A05: frozen merge policy and force replacement in real active windows."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from urllib.parse import quote
import xml.etree.ElementTree as ET

try:
    from .heal_probe import canonical, payload, require
    from .healthy import BASE_OPTIONS, physical_content
    from .cursor import SIZE, TASK_SECONDS, run_topology as dx_topology, main as dx_main, wanted_pairs
    from .stop import start, status, idle_fleet, validate_retired_stop
    from .versions import public_version_id
except ImportError:
    from heal_probe import canonical, payload, require
    from healthy import BASE_OPTIONS, physical_content
    from cursor import SIZE, TASK_SECONDS, run_topology as dx_topology, main as dx_main, wanted_pairs
    from stop import start, status, idle_fleet, validate_retired_stop
    from versions import public_version_id


CASE_ID = "heal.admin-overlap-and-force-start"
BRANCHES = ("same-scope", "parent-child", "disjoint", "force-start")
SCOPE_OBJECTS = 257
OBJECTS = 2 * SCOPE_OBJECTS
PREFIXES = ("objects/a/child/", "objects/b/")
PRIMARY = "objects/a/"
MISSING = tuple(range(0, SCOPE_OBJECTS, 64))
POLICY = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "runtime": {"RUSTFS_HEAL_OVERLAP_POLICY": "merge", "RUSTFS_HEAL_PAGE_PARALLEL_ENABLE": "false"},
    "same-scope": "HTTP 200 and the original admitted token",
    "parent-child": {"httpStatus": 409, "code": "OperationAborted",
                     "message": "heal request not admitted: admission=dropped, reason=overlapping_paths"},
    "disjoint": "HTTP 200 and an independent token; original remains running",
    "force-start": "HTTP 200 and independent token; old outcome aborted/cancelled with partial coverage",
    "references": ["crates/config/src/constants/heal.rs", "crates/heal/src/heal/manager.rs:admin_overlap_rejection",
                   "rustfs/src/admin/handlers/heal.rs:reject_heal_admission"],
    "itemRetention": 1024, "diagnosticObjects": 128,
    "retentionGapClaimed": False, "nativeCommitRaceClaimed": False}


def seed_scopes(recorder, bucket):
    recorder.call("create_bucket", {"Bucket": bucket})
    expected = []
    for prefix in PREFIXES:
        for index in range(SCOPE_OBJECTS):
            key = f"{prefix}{index:04d}.bin"
            body = payload(f"{bucket}/{key}/0", SIZE)
            ack = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "DX", "scope": prefix},
                "Tagging": "suite=heal&fixture=DX"}, body)
            require(ack.get("VersionId", "null") == "null", "DX fixture unexpectedly enabled versioning")
            expected.append({"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                "payload": {"size": SIZE, "sha256": hashlib.sha256(body).hexdigest()}})
    wanted_pairs(expected, count=OBJECTS)
    return expected


def scope_expected(expected, prefix):
    selected = [entry for entry in expected if entry["key"].startswith(prefix)]
    require(len(selected) == SCOPE_OBJECTS, "scope escaped its frozen 257-object fixture")
    return selected


def counters(current, *, cancelled=False):
    value = (current.get("outcome") or {}).get("counters", {})
    for name in ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures"):
        require(type(value.get(name)) is int and value[name] >= 0, "scope omitted typed canonical counters")
    cancellation_count = sum(receipt.get("disposition", {}).get("state") == "cancelled"
                             for receipt in (current.get("outcome") or {}).get("objects", []))
    require(all(value[name] == 0 for name in ("failed", "unknown", "attemptFailures"))
            and value["skipped"] == (cancellation_count if cancelled else 0)
            and value.get("overflowed") is False
            and value["processed"] == value["healed"] + value["unchanged"] + value["skipped"],
            "scope counters hide failed, unknown or unresolved responsibility")
    return value


def active_window(current):
    value = counters(current)
    return (current.get("summary") == "running" and current.get("outcome", {}).get("execution", {}).get("state") == "running"
            and 0 < value["processed"] < SCOPE_OBJECTS)


def wait_window(probe, fleet, path, token):
    deadline = time.monotonic() + TASK_SECONDS
    while True:
        current = status(probe, fleet, path, token, "overlap-running-window")
        if active_window(current):
            return current
        require(current.get("summary") == "running", "NOT_OBSERVED: scope finished before a nonzero active window")
        require(time.monotonic() < deadline, "NOT_OBSERVED: active scope exceeded the frozen window budget")
        time.sleep(0.05)


def monotonic(before, after, *, cancelled=False):
    left, right = counters(before), counters(after, cancelled=cancelled)
    require(all(right[name] >= left[name] for name in ("processed", "healed", "unchanged")),
            "same-token scope counters moved backwards")


def overlap_rejection(observed):
    require(observed.get("httpStatus") == 409 and not observed.get("transportError"),
            "parent-child overlap violated the frozen HTTP contract")
    body = observed.get("response")
    fields = {element.tag.rsplit("}", 1)[-1]: element.text for element in ET.fromstring(body).iter()} if isinstance(body, str) else body
    require(isinstance(fields, dict) and (fields.get("Code") or fields.get("code")) == "OperationAborted"
            and (fields.get("Message") or fields.get("message")) == POLICY["parent-child"]["message"],
            "parent-child overlap lost its exact structured reason")
    return POLICY["parent-child"]


def scope_proof(current, bucket, expected, damaged, *, cancelled=False, healed=None, replacement=False):
    wanted = wanted_pairs(expected, count=SCOPE_OBJECTS)
    require(damaged and set(damaged) <= set(wanted), "scope repair plan escaped its exact ACK identities")
    value = counters(current, cancelled=cancelled)
    outcome = current.get("outcome") or {}
    if cancelled:
        require(current.get("summary") == "stopped" and outcome.get("execution") == {"state": "aborted", "reason": "cancelled"}
                and outcome.get("coverage") == "partial" and 0 < value["processed"] < SCOPE_OBJECTS
                and value["healed"] <= len(damaged), "forceStart falsely completed or lost the old partial responsibility")
    else:
        require(current.get("summary") == "finished" and outcome.get("execution") == {"state": "completed"}
                and outcome.get("coverage") == "complete" and value["processed"] == SCOPE_OBJECTS
                and type(healed) is int and value["healed"] == healed,
                "scope failed complete coverage or its exact planned repair count")
    receipts, incarnation, seen, dispositions = outcome.get("objects"), None, set(), {}
    require(isinstance(receipts, list) and 0 < len(receipts) <= 128
            and type(outcome.get("objectsTruncated")) is bool
            and (cancelled or outcome["objectsTruncated"] is True), "scope omitted bounded diagnostic truth")
    for receipt in receipts:
        identity = receipt.get("identity", {})
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(pair in wanted and pair not in seen and identity.get("bucket") == bucket
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and isinstance(identity.get("bucketIncarnationId"), str) and identity["bucketIncarnationId"],
                "scope receipt crossed a bucket/key/version/set or omitted incarnation")
        incarnation = incarnation or identity["bucketIncarnationId"]
        require(incarnation == identity["bucketIncarnationId"], "scope mixed bucket incarnations")
        state = receipt.get("disposition", {}).get("state")
        require(state in ({"repaired", "verified_healthy", "cancelled"} if cancelled else {"repaired", "verified_healthy"})
                and (state != "repaired" or pair in damaged)
                and (state != "verified_healthy" or pair not in damaged or replacement),
                "scope diagnostic concealed an unresolved or false repair")
        dispositions[pair[0]] = state
        seen.add(pair)
    return {"canonicalCounters": value, "bucketIncarnationId": incarnation, "diagnosticDispositions": dispositions,
            "diagnosticObjectsAreCompleteManifest": False, "coverage": outcome["coverage"]}


def collect_scope_items(state, bucket, wanted, ledger, *, since=0):
    """Retain original same-token items before completed-report byte eviction."""
    next_seq, min_seq = state.get("nextSeq"), state.get("minSeq")
    require(type(next_seq) is int and type(min_seq) is int and 1 <= min_seq <= next_seq <= SCOPE_OBJECTS + 1,
            "scope item sequence bounds are missing or invalid")
    require(next_seq > max(ledger, default=0), "same-token scope item sequence moved backwards")
    require(min_seq == 1 or state.get("truncated") is True, "scope silently lost an early item sequence")
    items = state.get("items", [])
    first = max(since + 1, min_seq)
    available = max(0, next_seq - first)
    require(isinstance(items, list) and len(items) <= available
            and (len(items) == available or state.get("truncated") is True), "scope silently omitted item sequences")
    for index, item in enumerate(items, first):
        pair = item.get("object"), public_version_id(item.get("versionId"))
        require(item.get("bucket") == bucket and pair in wanted
                and item.get("objectSize") == wanted[pair]["payload"]["size"], "scope item is outside its exact ACK manifest")
        require(index not in ledger or canonical(ledger[index]) == canonical(item), "same-token scope item changed its original sequence")
        require(not any(seq != index and (entry.get("object"), public_version_id(entry.get("versionId"))) == pair
                        for seq, entry in ledger.items()), "scope item duplicated a real object/version")
        ledger[index] = item


def completed_scope(probe, fleet, path, token, bucket, expected, damaged, *, healed, replacement=False,
                    observations=(), sample_other=None):
    deadline = time.monotonic() + TASK_SECONDS
    wanted = wanted_pairs(expected, count=SCOPE_OBJECTS)
    ledger = {}
    for observation in observations:
        collect_scope_items(observation, bucket, wanted, ledger)
    while True:
        current = status(probe, fleet, path, token, "overlap-terminal-wait")
        collect_scope_items(current, bucket, wanted, ledger)
        if sample_other is not None:
            sample_other()
        if current.get("summary") != "running":
            break
        counters(current)
        require(time.monotonic() < deadline, "scope exceeded its frozen completion budget")
        time.sleep(0.05)
    proof = scope_proof(current, bucket, expected, damaged, healed=healed, replacement=replacement)
    # The completed-report byte budget can evict a prefix even below the
    # 1024-item count cap. A complete proof needs the original running items.
    cursor = 0
    while cursor + 1 in ledger: cursor += 1
    first_page = True
    while first_page or cursor < SCOPE_OBJECTS:
        first_page = False
        observed = fleet.record("overlap-complete-items", probe.admin(path,
            [("clientToken", token), ("sinceSeq", str(cursor))], b""))
        require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict), "scope cursor failed")
        page = observed["response"]
        require(page.get("summary") == "finished" and page.get("nextSeq") == SCOPE_OBJECTS + 1
                and canonical(page.get("outcome")) == canonical(current["outcome"]), "terminal scope changed, lost items or crossed tokens")
        previous = cursor
        collect_scope_items(page, bucket, wanted, ledger, since=cursor)
        while cursor + 1 in ledger: cursor += 1
        require(cursor == SCOPE_OBJECTS or cursor > previous,
                "scope lost an uncollected early item; a response byte-limit gap cannot certify completeness")
        require(time.monotonic() < deadline, "scope item suffix exceeded completion budget")
    seen = {(item.get("object"), public_version_id(item.get("versionId"))) for item in ledger.values()}
    require(set(ledger) == set(range(1, SCOPE_OBJECTS + 1)), "scope sequence ledger is incomplete")
    require(seen == set(wanted), "terminal scope missed a real object/version")
    evidence = {"path": path, "clientToken": token, "bucket": bucket,
                "items": [{"seq": seq, "item": ledger[seq]} for seq in sorted(ledger)]}
    fleet.record("overlap-complete-item-ledger", evidence)
    return current, {**proof, "completeItemIdentities": len(seen), "retentionGapClaimed": False,
                     "completeItemLedgerSha256": hashlib.sha256(canonical(evidence).encode()).hexdigest()}


def force_accounting(old, new, damaged):
    require(old["bucketIncarnationId"] == new["bucketIncarnationId"]
            and old["canonicalCounters"]["healed"] + new["canonicalCounters"]["healed"] == len(damaged),
            "forceStart leaked or duplicated repair responsibility between its task identities")
    for key in set(old["diagnosticDispositions"]) & set(new["diagnosticDispositions"]):
        require(not (old["diagnosticDispositions"][key] == new["diagnosticDispositions"][key] == "repaired"),
                "old and new tasks both committed a repair receipt for the same missing shard")
    return {"independentTokens": True, "sameBucketIncarnation": True, "exactTotalRepairs": len(damaged)}


def observe_branch(branch, probe, fleet, binding, bucket, expected, targets, active, *, original):
    fleet.record("frozen-overlap-policy", POLICY)
    path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + PRIMARY
    primary = scope_expected(expected, PRIMARY)
    damaged = {(entry["key"], entry["versionId"]) for entry in targets if entry["key"].startswith(PRIMARY)}
    old = start(probe, fleet, path, active)
    before = wait_window(probe, fleet, path, old)
    proof = {"oldTaskId": old, "runningBeforeRequest": counters(before), "policy": "merge"}
    if branch in {"same-scope", "parent-child"}:
        request_path = path if branch == "same-scope" else path + "child/"
        observed = fleet.record("overlap-request", probe.admin(request_path, [], json.dumps({**BASE_OPTIONS, "recursive": True}).encode()))
        if branch == "same-scope":
            require(observed.get("httpStatus") == 200 and observed.get("response", {}).get("clientToken") == old,
                    "same-scope start did not reuse the active token under merge policy")
        else:
            proof["typedRejection"] = overlap_rejection(observed)
        after = status(probe, fleet, path, old, "overlap-window-after-request")
        require(active_window(after), "NOT_OBSERVED: overlap request was not bracketed by the same running task")
        monotonic(before, after)
        proof["runningAfterRequest"] = counters(after)
        terminal, proof["completeResponsibility"] = completed_scope(probe, fleet, path, old, bucket, primary, damaged,
            healed=len(damaged), observations=(before, after))
        active.remove((path, old))
    elif branch == "disjoint":
        other_path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + PREFIXES[1]
        new = start(probe, fleet, other_path, active)
        require(new != old, "disjoint scope reused another scope's token")
        after = status(probe, fleet, path, old, "original-after-disjoint-start")
        require(active_window(after), "NOT_OBSERVED: original task finished before the disjoint admission was bracketed")
        monotonic(before, after)
        other_observations = [wait_window(probe, fleet, other_path, new)]
        other = scope_expected(expected, PREFIXES[1])
        other_damaged = {(entry["key"], entry["versionId"]) for entry in targets if entry["key"].startswith(PREFIXES[1])}
        def sample_disjoint():
            other_observations.append(status(probe, fleet, other_path, new, "overlap-disjoint-parallel-status"))
        terminal, proof["completeResponsibility"] = completed_scope(probe, fleet, path, old, bucket, primary, damaged,
            healed=len(damaged), observations=(before, after), sample_other=sample_disjoint)
        other_terminal, proof["disjointResponsibility"] = completed_scope(probe, fleet, other_path, new, bucket, other, other_damaged,
            healed=len(other_damaged), observations=other_observations)
        require(proof["completeResponsibility"]["bucketIncarnationId"] == proof["disjointResponsibility"]["bucketIncarnationId"],
                "disjoint tasks crossed bucket incarnation")
        active.remove((path, old)); active.remove((other_path, new))
        proof.update(newTaskId=new, runningAfterRequest=counters(after))
    else:
        require(branch == "force-start", "unregistered overlap branch")
        new = start(probe, fleet, path, active, force=True)
        require(new != old, "forceStart reused the original token")
        first_old = status(probe, fleet, path, old, "force-old-initial-cancellation")
        require(first_old.get("summary") != "finished", "NOT_OBSERVED: original task completed before forceStart could cancel it")
        initial = scope_proof(first_old, bucket, primary, damaged, cancelled=True)
        monotonic(before, first_old, cancelled=True)
        new_before = wait_window(probe, fleet, path, new)
        retired = fleet.record("force-old-stop", probe.admin(path, [("clientToken", old), ("forceStop", "true")], b""))
        proof["oldStopIsolation"] = validate_retired_stop(retired, old)
        new_after = status(probe, fleet, path, new, "force-new-after-old-stop")
        require(new_after.get("summary") != "stopped", "old token stopped the replacement task")
        require(active_window(new_after), "NOT_OBSERVED: old stop was not bracketed by the same running replacement")
        monotonic(new_before, new_after)
        # The old worker can refine its cancelled report while retiring. Wait
        # for both actual workers to drain before freezing final responsibility.
        deadline = time.monotonic() + TASK_SECONDS
        new_observations = [new_before, new_after]
        while True:
            draining = status(probe, fleet, path, new, "force-new-drain")
            new_observations.append(draining)
            if draining.get("summary") != "running":
                break
            counters(draining)
            require(time.monotonic() < deadline, "replacement task exceeded completion budget")
            time.sleep(0.05)
        idle_fleet(fleet, binding)
        old_final = status(probe, fleet, path, old, "force-old-settled")
        settled = scope_proof(old_final, bucket, primary, damaged, cancelled=True)
        require(settled["bucketIncarnationId"] == initial["bucketIncarnationId"]
                and all(settled["canonicalCounters"][name] >= initial["canonicalCounters"][name]
                        for name in ("processed", "healed", "unchanged")), "cancelled refinement changed identity or moved backwards")
        terminal, new_proof = completed_scope(probe, fleet, path, new, bucket, primary, damaged,
            healed=len(damaged) - settled["canonicalCounters"]["healed"], replacement=True, observations=new_observations)
        proof.update(newTaskId=new, initialCancellation=initial, settledCancellation=settled,
            newRunningBeforeOldStop=counters(new_before), newRunningAfterOldStop=counters(new_after),
            completeResponsibility=new_proof, responsibilityAccounting=force_accounting(settled, new_proof, damaged))
        active.remove((path, old)); active.remove((path, new))
    proof["allPeerDrain"] = idle_fleet(fleet, binding)
    quiet = []
    for index in range(3):
        quiet.append(fleet.snapshot("overlap-live-quiet"))
        stable = status(probe, fleet, path, old, "overlap-old-quiet-status")
        require(canonical(stable.get("outcome")) == canonical((old_final if branch == "force-start" else terminal)["outcome"]),
                "retired old task rewrote responsibility after actual drain")
        if branch == "force-start":
            require(canonical(status(probe, fleet, path, new, "overlap-new-quiet-status").get("outcome")) == canonical(terminal["outcome"]),
                    "old worker overwrote the replacement outcome after its completion")
        elif branch == "disjoint":
            require(canonical(status(probe, fleet, other_path, new, "overlap-disjoint-quiet-status").get("outcome")) == canonical(other_terminal["outcome"]),
                    "retired task rewrote a disjoint scope's responsibility")
        if index < 2:
            time.sleep(5)
    require(all(physical_content(value) == physical_content(quiet[0])
                and all((value[node]["pid"], value[node]["unit"]) == (quiet[0][node]["pid"], quiet[0][node]["unit"])
                        for node in fleet.nodes) for value in quiet[1:]), "retired task kept writing or service changed during live quiet proof")
    return {branch: {"verdict": "PASS", **proof, "liveQuietSamples": 3, "betweenSamplesSeconds": 5,
                     "completePhysicalAndS3ProofRequired": True, "nativeCommitRaceClaimed": False}}


def run_topology(args, topology, nodes, *, observer=None, branches=BRANCHES):
    require(tuple(branches) == BRANCHES, "overlap executor omitted source branches")
    results = []
    for branch in branches:
        branch_args = argparse.Namespace(**vars(args))
        branch_args.evidence = args.evidence / branch
        branch_args.evidence.mkdir(exist_ok=True)
        indices = list(MISSING) + ([SCOPE_OBJECTS + index for index in MISSING] if branch == "disjoint" else [])
        result = dx_topology(branch_args, topology, nodes,
            observer=lambda *values, **options: observe_branch(branch, *values, **options), branches=(branch,),
            source_branches=BRANCHES, count=OBJECTS, fixture_builder=seed_scopes, missing_indices=indices,
            runtime_profile="overlap", policy_reference=POLICY)
        results.append({"branch": branch, **result})
        if result["cleanup"] != "PASS":
            break
    return {"topology": topology, "instances": results, "branches": {entry["branch"]: entry for entry in results},
        "verdict": "PASS" if len(results) == len(branches) and all(entry["verdict"] == "PASS" for entry in results) else "FAIL",
        "cleanup": "PASS" if results and all(entry["cleanup"] == "PASS" for entry in results) else "FAIL"}


if __name__ == "__main__":
    try:
        dx_main(case_id=CASE_ID, branches=BRANCHES, runner=run_topology)
    except Exception as exc:
        print(f"Heal overlap validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
