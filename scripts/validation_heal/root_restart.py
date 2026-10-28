#!/usr/bin/env python3
"""F01: original root responsibility, owner and timeout across graceful restart."""

from __future__ import annotations

import argparse
import copy
import json
import sys
import time
import uuid

try:
    from .heal_probe import Probe, canonical, require
    from .healthy import BASE_OPTIONS, physical_content
    from .cursor import OBJECTS, TASK_SECONDS, complete_outcome, wanted_pairs, run_topology as dx_topology, main as dx_main
    from .shards import files, validate_repaired_physical
    from .stop import status, wait_partial, idle_fleet, validate_unknown_token
    from .cancel_restart import REFERENCE as JOURNAL_REFERENCE
    from .fleet_node import PROCESS_FIELDS
    from .versions import public_version_id
except ImportError:
    from heal_probe import Probe, canonical, require
    from healthy import BASE_OPTIONS, physical_content
    from cursor import OBJECTS, TASK_SECONDS, complete_outcome, wanted_pairs, run_topology as dx_topology, main as dx_main
    from shards import files, validate_repaired_physical
    from stop import status, wait_partial, idle_fleet, validate_unknown_token
    from cancel_restart import REFERENCE as JOURNAL_REFERENCE
    from fleet_node import PROCESS_FIELDS
    from versions import public_version_id


CASE_ID = "heal.root-graceful-coordinator-restart"
BRANCHES = ("root-partial-graceful", "same-record-disk-resume")
PATH = "/rustfs/admin/v3/heal/"
TIMEOUT_SECONDS = 300
OPTIONS = {**BASE_OPTIONS, "recursive": True, "pool": None, "set": None}
REFERENCE = {**JOURNAL_REFERENCE, "intentSchema": 3, "rootHealType": {"type": "cluster"},
             "taskTimeoutSeconds": TIMEOUT_SECONDS, "recovery": "original admission replay; no cursor-resume or failover claim",
             "processBoundary": "same unit/env/volumes/candidate and boot; coordinator only; pidfd SIGTERM, actual normal exit zero"}


def records(fleet, token, label, *, stopped_node=None):
    return fleet.record(label, {node: fleet.remote(node, "task-records", taskId=token,
        quiescent=node == stopped_node) for node in fleet.nodes})


def owned_records(observed, token):
    require(isinstance(token, str) and str(uuid.UUID(token)) == token, "root token is not a canonical task UUID")
    present = {}
    require(observed and len(observed) in {1, 4}, "root journal omitted bound physical nodes")
    for node, value in observed.items():
        require(value.get("readOnly") is True and value.get("taskId") == token
                and isinstance(value.get("records"), list) and len(value["records"]) == 12, "root journal omitted a disk/name witness")
        seen = set()
        for entry in value["records"]:
            pair = entry.get("disk"), entry.get("kind")
            require(type(pair[0]) is int and 0 <= pair[0] < 4 and pair[1] in {"intent", "terminal", "report"}
                    and pair not in seen and type(entry.get("present")) is bool, "root journal has duplicate or invalid physical identities")
            seen.add(pair)
            if entry["present"]:
                require(pair[1] not in present, "root task has multiple durable owners")
                present[pair[1]] = {"node": node, **entry}
    return present


def root_intent(observed, token, coordinator):
    present = owned_records(observed, token)
    require(set(present) == {"intent"}, "NOT_OBSERVED: original runnable root intent is unavailable or already terminal")
    entry = present["intent"]
    value = entry.get("value", {})
    require(entry["node"] == coordinator and type(value.get("schema")) is int and value["schema"] == 3
            and value.get("task_id") == token and value.get("heal_type") == {"type": "cluster"}
            and not value.get("bucket_incarnation_id") and value.get("priority") == "High"
            and type(value.get("retry_attempts")) is int and value["retry_attempts"] == 0,
            "root intent changed coordinator/scope/identity/priority or contains a retry")
    options = value.get("options", {})
    required = {"scan_mode": 2, "remove_corrupted": False, "recreate_missing": False,
                "update_parity": False, "recursive": True, "dry_run": False, "no_lock": False,
                "pool_index": None, "set_index": None}
    require(all(type(options.get(name)) is type(expect) and options[name] == expect for name, expect in required.items()),
            "root intent lost its exact admitted options/selectors")
    require(isinstance(value.get("created_at"), dict), "root intent omitted original admission time")
    return entry


def duration_ns(value):
    require(isinstance(value, dict) and set(value) == {"secs", "nanos"}
            and type(value["secs"]) is int and 0 <= value["secs"] < 2**64
            and type(value["nanos"]) is int and 0 <= value["nanos"] < 1_000_000_000, "invalid persisted timeout duration")
    return value["secs"] * 1_000_000_000 + value["nanos"]


def same_owner(before, after):
    require(all(before[name] == after[name] for name in ("node", "disk", "relativePath")), "root record moved to a different physical owner")
    left, right = copy.deepcopy(before["value"]), copy.deepcopy(after["value"])
    old_timeout, new_timeout = left["options"].pop("timeout", None), right["options"].pop("timeout", None)
    require(canonical(left) == canonical(right), "restart changed the original root responsibility or non-timeout options")
    remaining = duration_ns(new_timeout)
    maximum = duration_ns(old_timeout) if old_timeout is not None else TIMEOUT_SECONDS * 1_000_000_000
    require(0 < remaining <= maximum <= TIMEOUT_SECONDS * 1_000_000_000, "graceful restart removed, refunded or exhausted the original task budget")
    return {"samePhysicalOwner": True, "nonTimeoutIntentUnchanged": True, "beforeTimeoutNs": maximum,
            "checkpointRemainingTimeoutNs": remaining, "budgetNotRefunded": True}


def partial_physical(original, current, targets):
    actual, restored, remaining = files(current), [], []
    for target in targets:
        entry = actual.get((target["node"], target["disk"], target["relativePath"]))
        if entry is None:
            remaining.append(target)
        else:
            require(entry["sha256"] == target["sha256"] and entry["size"] == target["size"], "partial root repair committed incorrect shard bytes")
            restored.append(target)
    require(restored and remaining, "NOT_OBSERVED: actual root repair was not partial at the interruption boundary")
    expected = copy.deepcopy(original)
    missing = {(entry["node"], entry["disk"], entry["relativePath"]) for entry in remaining}
    for node, value in expected.items():
        value["files"] = [entry for entry in value["files"] if (node, entry["disk"], entry["path"]) not in missing]
    proof = validate_repaired_physical(expected, current, restored[0], repair_targets=restored)
    return remaining, {**proof, "actualRestoredParts": len(restored), "actualPendingParts": len(remaining), "backupRescueUsed": False}


def restart_identity(before, after, coordinator):
    require(set(before) == set(after) and coordinator in before, "restart omitted an original peer")
    for node, old in before.items():
        new = after[node]
        require(all(new.get(name) == old.get(name) for name in PROCESS_FIELDS if name not in {"pid", "startTicks"}),
                "restart changed boot, unit, namespace, candidate, command or environment")
        require(type(new.get("pid")) is int and new["pid"] > 1 and type(new.get("startTicks")) is int, "restart has no actual new process")
        if node == coordinator:
            require((new["pid"], new["startTicks"]) != (old["pid"], old["startTicks"]), "coordinator was not restarted")
        else:
            require((new["pid"], new["startTicks"]) == (old["pid"], old["startTicks"]), "non-coordinator peer restarted or changed process")
    return {"sameUnitEnvironmentVolumesAndCandidate": True, "sameBootAndNamespace": True, "onlyCoordinatorProcessChanged": True}


def recovery_status(probe, fleet, token):
    deadline = time.monotonic() + 15
    while True:
        observed = fleet.record("original-token-recovery-query", probe.admin(PATH, [("clientToken", token)], b""))
        if observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict):
            return observed["response"]
        # The exact absent-token error is an observation during admission
        # replay, never completion evidence. All other failures remain errors.
        validate_unknown_token(observed, token)
        require(time.monotonic() < deadline, "original root token stayed unavailable beyond the 15-second recovery budget")
        time.sleep(0.1)


def root_terminal(observed, token, owner, current, bucket, expected):
    wanted = wanted_pairs(expected)
    present = owned_records(observed, token)
    require(set(present) == {"terminal", "report"}, "completed root task lost its terminal/report or retained runnable responsibility")
    require(all((entry["node"], entry["disk"]) == (owner["node"], owner["disk"]) for entry in present.values()),
            "root terminal/report changed original record disk")
    terminal, report = present["terminal"]["value"], present["report"]["value"]
    require(type(terminal.get("schema")) is int and terminal["schema"] == 1 and terminal.get("task_id") == token
            and terminal.get("heal_type") == {"type": "cluster"} and terminal.get("status") == "Completed",
            "root durable terminal changed task/scope/schema or is not completed")
    require(canonical(terminal.get("options")) == canonical(owner["value"]["options"]),
            "completed root task changed its checkpointed options or refunded the remaining budget")
    require(type(report.get("schema")) is int and report["schema"] == 1 and report.get("terminal") == terminal
            and canonical(report.get("outcome")) == canonical(current.get("outcome")), "root durable report lost the completed responsibility")
    require(type(report.get("next_seq")) is int and type(report.get("min_seq")) is int
            and 1 <= report["min_seq"] <= report["next_seq"] and isinstance(report.get("seqed_items"), list)
            and 0 < len(report["seqed_items"]) <= 1024, "root report omitted valid bounded result items")
    previous, seen = 0, set()
    for sequence, item in report["seqed_items"]:
        pair = item.get("object"), public_version_id(item.get("versionId"))
        require(type(sequence) is int and max(previous + 1, report["min_seq"]) <= sequence < report["next_seq"]
                and item.get("bucket") == bucket and pair in wanted and pair not in seen
                and item.get("objectSize") == wanted[pair]["payload"]["size"], "root report items escaped complete ACK identities")
        previous = sequence; seen.add(pair)
    return {"runnableIntentRetiredOnEveryDrive": True, "sameOriginalRecordDisk": True,
            "completedReportMatchesControlPlane": True, "retainedDiagnosticsAreFullManifest": False}


def crashed_owner(before, after):
    require(canonical(before) == canonical(after),
            "SIGKILL changed or moved the frozen original root intent without a graceful checkpoint")
    return {"samePhysicalOwnerAndIntentBytes": True, "gracefulTimeoutCheckpointClaimed": False,
            "recoveryBoundary": "replay the persisted admission; no cursor or elapsed-budget checkpoint claim"}


def observe_task(probe, fleet, binding, bucket, expected, targets, active, *, original, interruption="graceful"):
    require(interruption in {"graceful", "sigkill"}, "unsupported root interruption")
    require(interruption != "sigkill" or len(fleet.nodes) == 4, "F04 root coordinator crash requires actual H2")
    fleet.record("root-restart-contract", {**REFERENCE, "interruption": interruption,
        "processBoundary": REFERENCE["processBoundary"] if interruption == "graceful" else
            "coordinator-only pidfd SIGKILL; persisted admission replay, without shutdown checkpoint"})
    started = fleet.record("root-admission", probe.admin(PATH, [], json.dumps(OPTIONS).encode()))
    require(started.get("httpStatus") == 200 and isinstance(started.get("response"), dict), "root admission failed")
    token = started["response"].get("clientToken")
    require(isinstance(token, str) and str(uuid.UUID(token)) == token, "root admission omitted canonical task ID")
    active.append((PATH, token))
    first = wait_partial(probe, fleet, PATH, token, targets, repair_required=True)
    admitted = records(fleet, token, "root-intent-admitted-owner")
    owners = owned_records(admitted, token)
    require(set(owners) == {"intent"}, "NOT_OBSERVED: original root owner finished before the interruption window")
    coordinator = owners["intent"]["node"]
    require(coordinator in fleet.nodes, "root coordinator is outside the actual bound fleet")
    require(interruption != "sigkill" or all(target["node"] != coordinator for target in targets),
            "NOT_OBSERVED: F04 root crash must keep every missing-shard target process alive")
    root_intent(admitted, token, coordinator)
    fleet.record("actual-root-coordinator", {"node": coordinator, "source": "unique physical admitted intent owner", "taskId": token})
    processes = fleet.record("processes-before-interruption", {node: fleet.remote(node, "process") for node in fleet.nodes})
    identity = processes[coordinator]
    fleet.frozen_processes[coordinator] = identity
    fleet.record("root-freeze", fleet.remote(coordinator, "process-signal", signal="SIGSTOP", expected=identity))
    frozen = fleet.record("root-frozen-process", fleet.remote(coordinator, "process"))
    require(frozen.get("state") == "T" and all(frozen.get(name) == identity.get(name) for name in PROCESS_FIELDS), "root freeze missed the original process")
    scene = fleet.snapshot("root-frozen-physical")
    pending, before_physical = partial_physical(original, scene, targets)
    before_record = root_intent(records(fleet, token, "root-intent-before-stop"), token, coordinator)
    exit_proof = fleet.record("root-actual-exit", fleet.remote(coordinator,
        "process-graceful" if interruption == "graceful" else "process-crash", expected=identity))
    if interruption == "sigkill":
        require(exit_proof.get("actualSignal") == 9 and exit_proof.get("graceful") is False
                and exit_proof.get("pidfdBound") is True and exit_proof.get("platformEventIssued") is False,
                "root coordinator has no exact process-bound SIGKILL exit witness")
    del fleet.frozen_processes[coordinator]
    fleet.record("release-exited-unit", fleet.remote(coordinator, "stop"))
    after_record = root_intent(records(fleet, token, "root-intent-after-exit", stopped_node=coordinator), token, coordinator)
    budget = same_owner(before_record, after_record) if interruption == "graceful" else crashed_owner(before_record, after_record)
    exit_scenes = []
    for index in range(3):
        exit_scenes.append(fleet.record("root-physical-after-exit", {node: fleet.remote(node, "snapshot", quiescent=node == coordinator) for node in fleet.nodes}))
        if index < 2:
            time.sleep(5)
    require(all(physical_content(value) == physical_content(exit_scenes[0]) for value in exit_scenes[1:]),
            "coordinator's retiring peer writes did not settle before restart")
    after_scene = exit_scenes[-1]
    remaining, after_physical = partial_physical(original, after_scene, targets)
    require({entry["key"] for entry in remaining} <= {entry["key"] for entry in pending}, "interruption lost an already repaired shard")
    fleet.record("root-original-unit-restart", fleet.remote(coordinator, "start"))
    fleet.wait_ready()
    restarted = fleet.record("processes-after-restart", {node: fleet.remote(node, "process") for node in fleet.nodes})
    continuity = restart_identity(processes, restarted, coordinator)
    deadline = time.monotonic() + TASK_SECONDS
    while True:
        current = recovery_status(probe, fleet, token)
        require(current.get("summary") in {"running", "finished"}, "original root task failed or became cancelled during recovery")
        if current["summary"] == "finished":
            break
        require(time.monotonic() < deadline, "original root task did not recover within its pre-registered budget")
        time.sleep(0.05)
    terminal = complete_outcome(current, bucket, expected, {(entry["key"], entry["versionId"]) for entry in remaining})
    active.remove((PATH, token))
    idle = idle_fleet(fleet, binding)
    journal_before = records(fleet, token, "root-completed-records")
    journal = root_terminal(journal_before, token, after_record, current, bucket, expected)
    complete_scene = fleet.snapshot("root-completed-live-scene")
    # Repeat coordinator restart after completion. It must replay the durable
    # terminal unchanged, never create a new task or write another user shard.
    fleet.record("completed-coordinator-stop", fleet.remote(coordinator, "stop"))
    fleet.record("completed-coordinator-start", fleet.remote(coordinator, "start"))
    fleet.wait_ready()
    repeated = fleet.record("processes-after-repeat-restart", {node: fleet.remote(node, "process") for node in fleet.nodes})
    restart_identity(restarted, repeated, coordinator)
    repeat_idle = idle_fleet(fleet, binding)
    repeat = recovery_status(probe, fleet, token)
    require(repeat.get("summary") == "finished" and canonical(repeat.get("outcome")) == canonical(current["outcome"]), "repeat restart lost or changed original terminal responsibility")
    journal_after = records(fleet, token, "root-completed-records-after-repeat")
    root_terminal(journal_after, token, after_record, repeat, bucket, expected)
    require(canonical(journal_before) == canonical(journal_after), "repeat restart rewrote the completed root journal")
    require(physical_content(complete_scene) == physical_content(fleet.snapshot("root-repeat-live-scene")), "completed task revived and rewrote user files after repeat restart")
    if interruption == "sigkill":
        return {"root-coordinator-kill": {"verdict": "PASS", "originalTaskId": token,
            "runningWindow": first["outcome"]["counters"], "beforeInterruptionPhysical": before_physical,
            "afterActualExitPhysical": after_physical, "actualExit": exit_proof, "processContinuity": continuity,
            "persistedAdmission": budget, "sameTaskRecovered": True, "newStartRequestIssued": False,
            "completeResponsibility": terminal, "terminalJournal": journal, "repeatRestartIdle": repeat_idle,
            "completedJournalBytesPreserved": True, "allPeerIdle": idle}}
    return {"root-partial-graceful": {"verdict": "PASS", "originalTaskId": token, "runningWindow": first["outcome"]["counters"],
                "beforeInterruptionPhysical": before_physical, "afterActualExitPhysical": after_physical,
                "sameTaskRecovered": True, "newStartRequestIssued": False, "processContinuity": continuity,
                "completeResponsibility": terminal, "allPeerIdle": idle},
            "same-record-disk-resume": {"verdict": "PASS", "originalTaskId": token, "originalRecordOwner": before_record,
                "budget": budget, "terminalJournal": journal, "repeatRestartIdle": repeat_idle,
                "completedJournalBytesPreserved": True, "cursorResumeClaimed": False, "recordDiskFailoverClaimed": False}}


def run_topology(args, topology, nodes, *, observer=observe_task, branches=BRANCHES):
    return dx_topology(args, topology, nodes, observer=observer, branches=branches, runtime_profile="root-restart", policy_reference=REFERENCE)


if __name__ == "__main__":
    try:
        dx_main(case_id=CASE_ID, observer=observe_task, branches=BRANCHES, runner=run_topology)
    except Exception as exc:
        print(f"Heal root-restart validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
