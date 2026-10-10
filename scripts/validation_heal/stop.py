#!/usr/bin/env python3
"""A06: cancellation, actual quiet writes and stale-token isolation on DX."""

from __future__ import annotations

import copy
import json
import sys
import time
from urllib.parse import quote
import xml.etree.ElementTree as ET

try:
    from .heal_probe import Probe, canonical, require
    from .healthy import BASE_OPTIONS, physical_content
    from .cursor import OBJECTS, TASK_SECONDS, Window, wanted_pairs, terminal_proof, main as dx_main
    from .shards import files, validate_repaired_physical
    from .versions import public_version_id
    from .status import validate_background
except ImportError:
    from heal_probe import Probe, canonical, require
    from healthy import BASE_OPTIONS, physical_content
    from cursor import OBJECTS, TASK_SECONDS, Window, wanted_pairs, terminal_proof, main as dx_main
    from shards import files, validate_repaired_physical
    from versions import public_version_id
    from status import validate_background


CASE_ID = "heal.admin-stop-and-stale-token"
BRANCHES = ("stop-active", "repeat-stop", "old-status", "old-stop-new-task")
QUIET_SECONDS = 5
POLICY = {"referenceCandidate": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "cancelActive": {"httpStatus": 200, "summary": "stopped"},
    "repeatOrRetiredTokenStop": {"httpStatus": 500, "code": "InternalError", "messagePrefix": "Heal task not found: "},
    "oldStatus": "retained aborted/cancelled outcome, partial coverage; same path/token",
    "sources": ["crates/heal/src/heal/manager.rs:cancel_task", "crates/heal/src/heal/channel.rs:process_cancel_request",
                "rustfs/src/admin/handlers/heal.rs:force_stop handler"],
    "unsupportedCandidates": "fail the frozen HTTP/body contract; do not accept arbitrary alternatives"}


def status(probe, fleet, path, token, label):
    observed = fleet.record(label, probe.admin(path, [("clientToken", token)], b""))
    require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict), "token status failed")
    return observed["response"]


def start(probe, fleet, path, active):
    observed = fleet.record("trigger", probe.admin(path, [], json.dumps({**BASE_OPTIONS, "recursive": True}).encode()))
    require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict), "DX task admission failed")
    token = observed["response"].get("clientToken")
    require(isinstance(token, str) and token, "DX task omitted its admitted token")
    active.append((path, token))
    return token


def partial_window(status, targets, *, repair_required):
    counters = (status.get("outcome") or {}).get("counters", {})
    processed, healed = counters.get("processed"), counters.get("healed")
    for name in ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures"):
        require(type(counters.get(name)) is int and counters[name] >= 0, "active window omitted typed canonical counters")
    require(all(counters[name] == 0 for name in ("failed", "unknown", "attemptFailures"))
            and counters.get("overflowed") is False, "active window contains a real failure or unknown responsibility")
    return (status.get("summary") == "running" and status.get("outcome", {}).get("execution", {}).get("state") == "running"
            and 0 < processed < OBJECTS and (not repair_required or 0 < healed < len(targets)))


def wait_partial(probe, fleet, path, token, targets, *, repair_required):
    deadline = time.monotonic() + TASK_SECONDS
    while True:
        current = status(probe, fleet, path, token, "running-window")
        if partial_window(current, targets, repair_required=repair_required):
            return current
        require(current.get("summary") == "running", "NOT_OBSERVED: task finished before the required active window")
        require(time.monotonic() < deadline, "NOT_OBSERVED: active window exceeded the frozen budget")
        time.sleep(0.05)


def validate_retired_stop(observed, token):
    require(observed.get("httpStatus") == 500 and not observed.get("transportError"),
            "retired token stop did not follow the frozen HTTP contract")
    body = observed.get("response")
    if isinstance(body, str):
        fields = {element.tag.rsplit("}", 1)[-1]: element.text for element in ET.fromstring(body).iter()}
    else:
        fields = body if isinstance(body, dict) else {}
    require((fields.get("Code") or fields.get("code")) == "InternalError"
            and (fields.get("Message") or fields.get("message")) == "Heal task not found: " + token,
            "retired-token response has the wrong exact structured error/task identity")
    return {"httpStatus": 500, "code": "InternalError", "exactRetiredTaskId": token}


def validate_cancelled(current, bucket, expected, targets):
    wanted = wanted_pairs(expected)
    outcome = current.get("outcome") or {}
    require(current.get("summary") == "stopped" and outcome.get("execution") == {"state": "aborted", "reason": "cancelled"}
            and outcome.get("coverage") == "partial", "cancelled responsibility was incorrectly reported as complete/repaired")
    counters = outcome.get("counters", {})
    for name in ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures"):
        require(type(counters.get(name)) is int and counters[name] >= 0, "cancelled task omitted typed counters")
    require(0 < counters["processed"] < OBJECTS and 0 < counters["healed"] < len(targets)
            and counters["processed"] == sum(counters[name] for name in ("healed", "unchanged", "skipped", "failed"))
            and all(counters[name] == 0 for name in ("failed", "unknown", "attemptFailures"))
            and counters.get("overflowed") is False, "cancelled counters hide partial coverage, errors or unresolved responsibility")
    objects, seen, incarnation, dispositions = outcome.get("objects"), set(), None, []
    require(isinstance(objects, list) and 0 < len(objects) <= 128, "cancelled task omitted bounded diagnostic receipts")
    repaired = {(entry["key"], entry["versionId"]) for entry in targets}
    for receipt in objects:
        identity = receipt.get("identity", {})
        pair = identity.get("object"), public_version_id(identity.get("versionId"))
        require(pair in wanted and pair not in seen and identity.get("bucket") == bucket
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and isinstance(identity.get("bucketIncarnationId"), str) and identity["bucketIncarnationId"],
                "cancelled diagnostic receipt lost its exact responsibility identity")
        incarnation = incarnation or identity["bucketIncarnationId"]
        require(incarnation == identity["bucketIncarnationId"], "cancelled diagnostics mixed bucket incarnations")
        state = receipt.get("disposition", {}).get("state")
        require(state in {"repaired", "verified_healthy", "cancelled"}
                and (state != "repaired" or pair in repaired)
                and (state != "verified_healthy" or pair not in repaired),
                "cancelled/unknown/unrepaired object falsely counted as repaired or healthy")
        seen.add(pair)
        dispositions.append({"key": pair[0], "versionId": pair[1], "state": state})
    return {"canonicalCounters": counters, "coverage": "partial", "bucketIncarnationId": incarnation,
            "unstartedObjectsAreNotRepaired": True, "diagnosticObjectsAreCompleteManifest": False,
            "diagnosticDispositions": dispositions}


def cancelled_physical(original, quiet, targets, cancelled):
    actual = files(quiet)
    restored, remaining = [], []
    for target in targets:
        entry = actual.get((target["node"], target["disk"], target["relativePath"]))
        if entry is None:
            remaining.append(target)
        else:
            require(entry["sha256"] == target["sha256"] and entry["size"] == target["size"],
                    "cancelled task committed incorrect shard bytes")
            restored.append(target)
    require(restored and remaining and len(restored) == cancelled["canonicalCounters"]["healed"],
            "cancelled physical repair count disagrees with its retained outcome")
    restored_pairs = {(entry["key"], entry["versionId"]) for entry in restored}
    require(isinstance(cancelled.get("diagnosticDispositions"), list) and cancelled["diagnosticDispositions"],
            "cancelled physical proof omitted its witnessed diagnostic identities")
    for receipt in cancelled["diagnosticDispositions"]:
        pair = receipt["key"], receipt["versionId"]
        require((receipt["state"] == "repaired") == (pair in restored_pairs),
                "cancelled diagnostic disposition disagrees with the actual restored object")
    expected = copy.deepcopy(original)
    missing = {(entry["node"], entry["disk"], entry["relativePath"]) for entry in remaining}
    for node, observation in expected.items():
        observation["files"] = [entry for entry in observation["files"] if (node, entry["disk"], entry["path"]) not in missing]
    proof = validate_repaired_physical(expected, quiet, restored[0], repair_targets=restored)
    return remaining, {**proof, "actuallyRestoredParts": len(restored), "stillMissingParts": len(remaining),
                       "backupRescueUsed": False, "cancelledCountersMatchPhysical": True}


def idle_fleet(fleet, binding):
    deadline = time.monotonic() + 30
    while True:
        proofs = {}
        for endpoint in binding["endpoints"]:
            observed = fleet.record("task-exit-background", {"endpoint": endpoint, **Probe(binding, endpoint).admin(
                "/rustfs/admin/v3/background-heal/status", [], b"")})
            require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
                    "task exit has no complete background status")
            value = observed["response"]
            require(value.get("state") != "idle" or value.get("clusterStatusComplete") is True
                    and value.get("coverage", {}).get("unknown") == 0
                    and value.get("coverage", {}).get("responded") == len(binding["endpoints"]),
                    "task exit falsely reports a partial fleet as idle")
            try:
                validate_background(value, "idle", len(binding["endpoints"]))
                proofs[endpoint] = {"state": "idle", "clusterStatusComplete": True,
                    "coverage": value["coverage"], "healOperations": value["healOperations"]}
            except ValueError:
                pass
        if len(proofs) == len(binding["endpoints"]):
            return proofs
        require(time.monotonic() < deadline, "task did not drain queued/active/retrying responsibility on every peer")
        time.sleep(0.1)


def observe_task(probe, fleet, binding, bucket, expected, targets, active, *, original):
    path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/objects/"
    fleet.record("frozen-cancel-policy", POLICY)
    old = start(probe, fleet, path, active)
    first = wait_partial(probe, fleet, path, old, targets, repair_required=True)
    stop_ack = fleet.record("stop-active", probe.admin(path, [("clientToken", old), ("forceStop", "true")], b""))
    require(stop_ack.get("httpStatus") == 200 and stop_ack.get("response", {}).get("summary") == "stopped",
            "active cancellation did not follow the frozen acknowledgement contract")
    cancelled = status(probe, fleet, path, old, "cancelled-outcome")
    cancellation = validate_cancelled(cancelled, bucket, expected, targets)
    # Keep the actual service alive. An ACK or stopping its process would mask
    # writes that escaped task cancellation.
    quiet = []
    exit_proofs = []
    for index in range(3):
        exit_proofs.append(idle_fleet(fleet, binding))
        started = time.monotonic_ns()
        scene = fleet.snapshot("cancelled-live-quiet")
        quiet.append(scene)
        fleet.record("quiet-sample-time", {"sample": index, "startedMonotonicNs": started,
            "finishedMonotonicNs": time.monotonic_ns(), "serviceKeptRunning": True, "minimumBetweenSamplesSeconds": QUIET_SECONDS})
        unchanged = status(probe, fleet, path, old, "quiet-old-status")
        validate_cancelled(unchanged, bucket, expected, targets)
        require(canonical(unchanged["outcome"]) == canonical(cancelled["outcome"]), "retired responsibility changed during write quiet window")
        if index < 2:
            time.sleep(QUIET_SECONDS)
    require(all(physical_content(scene) == physical_content(quiet[0]) for scene in quiet[1:]),
            "cancelled task continued physical writes after its terminal acknowledgement")
    require(all(all((scene[node]["pid"], scene[node]["unit"]) == (quiet[0][node]["pid"], quiet[0][node]["unit"])
                    for node in fleet.nodes) for scene in quiet[1:]),
            "quiet window replaced the original live service process")
    remaining, physical = cancelled_physical(original, quiet[-1], targets, cancellation)
    active.remove((path, old))
    repeated = fleet.record("repeat-stop", probe.admin(path, [("clientToken", old), ("forceStop", "true")], b""))
    repeated_proof = validate_retired_stop(repeated, old)
    new = start(probe, fleet, path, active)
    require(new != old, "new task reused a cancelled token")
    before = wait_partial(probe, fleet, path, new, remaining, repair_required=False)
    old_status = status(probe, fleet, path, old, "old-status-during-new")
    validate_cancelled(old_status, bucket, expected, targets)
    require(canonical(old_status["outcome"]) == canonical(cancelled["outcome"]), "new task overwrote old partial responsibility")
    stale = fleet.record("old-stop-during-new", probe.admin(path, [("clientToken", old), ("forceStop", "true")], b""))
    stale_proof = validate_retired_stop(stale, old)
    after = status(probe, fleet, path, new, "new-after-stale-stop")
    require(after.get("summary") != "stopped", "old token actually stopped the new task")
    require(partial_window(after, remaining, repair_required=False),
            "NOT_OBSERVED: stale-stop request was not bracketed by the same active new task")
    after_stale_counters = dict(after["outcome"]["counters"])
    deadline = time.monotonic() + TASK_SECONDS
    while after.get("summary") == "running":
        require(time.monotonic() < deadline, "new task exceeded the frozen completion budget")
        time.sleep(0.05)
        after = status(probe, fleet, path, new, "new-task-control")
    active.remove((path, new))
    response = fleet.record("new-terminal-old-cursor", probe.admin(path, [("clientToken", new), ("sinceSeq", "0")], b""))
    require(response.get("httpStatus") == 200 and isinstance(response.get("response"), dict), "new task terminal cursor failed")
    terminal = terminal_proof(response["response"], bucket, expected, {(entry["key"], entry["versionId"]) for entry in remaining},
        Window(bucket, expected, new, "node1", fleet.run_id + "/" + new))
    require(canonical(response["response"].get("outcome")) == canonical(after.get("outcome")),
            "new task terminal responsibility changed")
    old_final = status(probe, fleet, path, old, "old-final-status")
    require(canonical(old_final.get("outcome")) == canonical(cancelled["outcome"]), "new completion rewrote the original cancellation")
    final_exit = idle_fleet(fleet, binding)
    return {"stop-active": {"verdict": "PASS", "activeWindow": first["outcome"]["counters"],
                "cancelled": cancellation, "physicalQuietProof": physical, "liveQuietSamples": 3, "betweenSamplesSeconds": QUIET_SECONDS,
                "sameLiveProcesses": True, "allPeerIdleProofs": exit_proofs},
            "repeat-stop": {"verdict": "PASS", **repeated_proof},
            "old-status": {"verdict": "PASS", "oldTaskId": old, "originalPartialOutcomePreserved": True},
            "old-stop-new-task": {"verdict": "PASS", "oldTaskId": old, "newTaskId": new,
                "newRunningBefore": before["outcome"]["counters"], "newRunningAfter": after_stale_counters,
                "staleStop": stale_proof, "newCompleteResponsibility": terminal, "allPeerIdleProof": final_exit}}


if __name__ == "__main__":
    try:
        dx_main(case_id=CASE_ID, observer=observe_task, branches=BRANCHES)
    except Exception as exc:
        print(f"Heal stop validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
