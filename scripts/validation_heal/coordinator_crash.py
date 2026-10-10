#!/usr/bin/env python3
"""F04: distinct H2 root, MRF and replacement coordinator SIGKILL boundaries."""

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
    from .heal_probe import Probe, canonical, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .cursor import run_topology as dx_topology
    from . import root_restart, mrf_replay, replacement, replacement_journal
    from .mrf_records import checkpoint, REFERENCE as MRF_REFERENCE
    from .shards import files
    from .scanner_repair import event_time
    from .stop import validate_unknown_token
    from .unformatted import binding as lab_binding
    from .fleet_node import PROCESS_FIELDS
except ImportError:
    from heal_probe import Probe, canonical, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from cursor import run_topology as dx_topology
    import root_restart, mrf_replay, replacement, replacement_journal
    from mrf_records import checkpoint, REFERENCE as MRF_REFERENCE
    from shards import files
    from scanner_repair import event_time
    from stop import validate_unknown_token
    from unformatted import binding as lab_binding
    from fleet_node import PROCESS_FIELDS


CASE_ID = "heal.coordinator-process-crash"
BRANCHES = ("root-coordinator-kill", "mrf-coordinator-kill", "replacement-coordinator-kill")
MRF_COUNT = 64
REPLACEMENT_COUNT = 1025
WINDOW_SECONDS = 90
REFERENCE = {"candidateCommit": root_restart.REFERENCE["candidateCommit"],
    "root": root_restart.REFERENCE, "mrf": MRF_REFERENCE, "replacement": replacement_journal.REFERENCE,
    "concurrencyConfigSource": {"crates/config/src/constants/heal.rs":
        "82b8614dfa7cc111e694138035e4a84ee23035c719f0286fc0565d7181ca014f"},
    "supportTable": {
        "root": "durable original admission and token; SIGKILL cannot checkpoint elapsed timeout or cursor",
        "mrf": "durable exact ACK responsibility; task token is volatile, automatic rescan uses a new token",
        "replacement": "durable original generation and target identities; target process remains alive"},
    "fault": "one exact run-owned paused coordinator process, Linux pidfd SIGKILL; same boot/roots/candidate"}


def processes(fleet, label):
    return fleet.record(label, {node: fleet.remote(node, "process") for node in fleet.nodes})


def continuity(before, after, owner, *, private_mounts=False):
    require(len(before) == 4 and set(before) == set(after) and owner in before,
            "coordinator crash omitted an actual H2 peer")
    for node, old in before.items():
        new = after[node]
        ignored = {"pid", "startTicks"} | ({"mountNamespace"} if private_mounts else set()) if node == owner else set()
        require(all(new.get(name) == old.get(name) for name in PROCESS_FIELDS if name not in ignored),
                "coordinator restart changed a target/peer process, boot, candidate, unit or environment")
        require(type(new.get("pid")) is int and new["pid"] > 1 and type(new.get("startTicks")) is int,
                "coordinator recovery omitted an actual running process")
        if node == owner:
            require((new["pid"], new["startTicks"]) != (old["pid"], old["startTicks"])
                    and isinstance(new.get("mountNamespace"), str) and new["mountNamespace"],
                    "coordinator did not actually restart in a witnessed namespace")
    return {"onlyCoordinatorRestarted": True, "everyTargetAndPeerKeptOriginalProcess": True,
            "sameBootUnitCandidateSettingsAndRoots": True, "privateMountNamespaceMayRestart": private_mounts}


def freeze(fleet, owner, before):
    identity = before[owner]
    fleet.frozen_processes[owner] = identity
    fleet.record("coordinator-SIGSTOP", fleet.remote(owner, "process-signal", signal="SIGSTOP", expected=identity))
    current = fleet.record("coordinator-frozen-process", fleet.remote(owner, "process"))
    require(current.get("state") == "T" and all(current.get(name) == identity.get(name) for name in PROCESS_FIELDS),
            "coordinator freeze missed its original process birth")
    return identity


def kill(fleet, owner, identity, peers):
    proof = fleet.record("coordinator-actual-SIGKILL", fleet.remote(owner, "process-crash", expected=identity))
    require(proof.get("actualSignal") == 9 and proof.get("signal") == "SIGKILL" and proof.get("graceful") is False
            and proof.get("pidfdBound") is True and proof.get("platformEventIssued") is False,
            "coordinator interruption lacks its exact non-graceful process-bound exit")
    del fleet.frozen_processes[owner]
    for node in fleet.nodes:
        if node == owner: continue
        actual = fleet.record("peer-kept-running-during-crash", fleet.remote(node, "process"))
        require(all(actual.get(name) == peers[node].get(name) for name in PROCESS_FIELDS),
                "target or non-coordinator peer changed process during SIGKILL")
    fleet.record("exited-coordinator-unit-released", fleet.remote(owner, "stop"))
    return proof


def replacement_checkpoint(observed, generation):
    token, endpoint = generation["taskId"], generation["targetSlots"][0]
    require(observed.get("taskId") == token and observed.get("readOnly") is True
            and observed.get("stable") is True and len(observed.get("records", [])) == 12,
            "replacement checkpoint lost its exact generation or whole physical-root census")
    seen, intents, seals = set(), {}, set()
    for row in observed["records"]:
        pair = row.get("disk"), row.get("kind")
        require(type(pair[0]) is int and 0 <= pair[0] < 4 and pair[1] in replacement_journal.FILES
                and pair not in seen and type(row.get("present")) is bool,
                "replacement checkpoint duplicated or invented a disk/name identity")
        seen.add(pair)
        suffix = replacement_journal.FILES[pair[1]][0]
        require(row.get("relativePath") == f".rustfs.sys/buckets/ahm-replacement/{token}_{suffix}",
                "replacement checkpoint escaped the exact reviewed generation name")
        if not row["present"]: continue
        require(type(row.get("size")) is int and row["size"] > 0
                and bool(re.fullmatch(r"[0-9a-f]{64}", row.get("sha256", ""))), "replacement checkpoint lacks whole-file identity")
        if pair[1] == "seal":
            require(row.get("sealed") is True and row["size"] == 6
                    and row["sha256"] == hashlib.sha256(b"sealed").hexdigest(), "replacement intent is not committed")
            seals.add(pair[0])
        elif pair[1] == "proof":
            raise ValueError("NOT_OBSERVED: replacement already has completion proof before its required crash")
        else:
            value = row.get("value", {})
            require(type(value.get("schema_version")) is int and value["schema_version"] == 7
                    and value.get("task_id") == token and value.get("replacement_generation") == token
                    and value.get("set_disk_id") == generation["setDiskId"]
                    and value.get("replacement_targets") == [endpoint] and value.get("completed") is False,
                    "replacement intent lost its original generation/set/target or is already complete")
            identities = value.get("replacement_target_identities")
            require(isinstance(identities, list) and len(identities) == 1 and identities[0].get("endpoint") == endpoint
                    and isinstance(identities[0].get("canonical_path"), str) and identities[0]["canonical_path"].startswith("/")
                    and isinstance(identities[0].get("physical_device_ids"), list) and identities[0]["physical_device_ids"]
                    and all(isinstance(item, str) and item for item in identities[0]["physical_device_ids"])
                    and isinstance(identities[0].get("filesystem_identity"), str) and identities[0]["filesystem_identity"],
                    "replacement checkpoint omitted the actual target path/device/filesystem identity")
            intents[pair[0]] = row
    require(intents and set(intents) <= seals, "NOT_OBSERVED: coordinator has no sealed durable replacement intent")
    return {"generation": token, "sealedRecordDisks": sorted(intents), "originalTargetBound": True,
            "physicalPartialWindowRequiredSeparately": True}


def interrupt_replacement(fleet, owner, proofs, original_format, generation):
    target = proofs[0]["node"]
    require(owner != target and len(fleet.nodes) == 4, "F04 requires a coordinator distinct from the live replacement target")
    before = processes(fleet, "replacement-coordinator-and-target-processes")
    identity = freeze(fleet, owner, before)
    scene = fleet.snapshot("replacement-coordinator-frozen-scene")
    progress = replacement.target_progress(scene, proofs)
    require(progress["partial"], "NOT_OBSERVED: replacement was not physically partial at coordinator freeze")
    require(all(scene[node]["formats"] == original_format[node]["formats"] for node in fleet.nodes),
            "replacement changed its target UUID/set before the coordinator crash")
    original = fleet.record("replacement-coordinator-durable-checkpoint", fleet.remote(owner,
        "replacement-records", taskId=generation["taskId"]))
    checkpoint_proof = replacement_checkpoint(original, generation)
    exit_proof = kill(fleet, owner, identity, before)
    after = fleet.record("replacement-coordinator-records-after-SIGKILL", fleet.remote(owner,
        "replacement-records", taskId=generation["taskId"], quiescent=True))
    replacement_checkpoint(after, generation)
    require(canonical(original) == canonical(after), "SIGKILL lost or rewrote the coordinator's committed replacement intent")
    stopped_scene = fleet.record("replacement-target-still-partial-after-coordinator-exit", {
        node: fleet.remote(node, "snapshot", quiescent=node == owner) for node in fleet.nodes})
    after_progress = replacement.target_progress(stopped_scene, proofs)
    require(after_progress["partial"] and set(progress["completeDataVersions"]) <= set(after_progress["completeDataVersions"]),
            "coordinator exit lost restored parts or missed the unfinished replacement window")
    require(all(stopped_scene[node]["formats"] == original_format[node]["formats"] for node in fleet.nodes),
            "coordinator crash changed a replacement disk identity")
    fleet.record("same-replacement-coordinator-restarted", fleet.remote(owner, "start"))
    fleet.wait_ready()
    restarted = processes(fleet, "replacement-processes-after-coordinator-restart")
    continuity_proof = continuity(before, restarted, owner, private_mounts=True)
    archive = fleet.record("original-target-archive-still-refused", fleet.remote(target, "replacement-probe"))
    require(archive.get("serviceReadDenied") is True and archive.get("sameOriginalServiceProcess") is True,
            "live target gained backup access during the coordinator restart")
    return {"branch": "replacement-coordinator-kill", "coordinator": owner, "targetKeptRunning": target,
        "originalGeneration": generation, "before": progress, "afterActualExit": after_progress,
        "checkpoint": checkpoint_proof, "actualExit": exit_proof, "processContinuity": continuity_proof,
        "sameCommittedGenerationRequiredForCompletion": True, "adminCompensationTasks": 0}


def mrf_progress(snapshot, authorities, bucket, obligations, target):
    """Observe partial physical ACK replay; final cohort streaming is a separate gate."""
    validate_topology(snapshot, "H2")
    current, original, complete = files(snapshot), files(authorities), []
    for item in obligations:
        wanted, metadata_path = item["versionId"], f"{bucket}/{item['key']}/xl.meta"
        candidates = []
        for (node, disk, path), entry in original.items():
            if node == target or path != metadata_path: continue
            versions = [value for value in entry.get("metadata", {}).get("versions", [])
                        if (value.get("versionId") or "null") == wanted]
            require(len(versions) == 1, "MRF physical authority omitted its exact ACK version")
            value = versions[0]
            logical = {name: value.get(name) for name in
                ("versionId", "isLatest", "deleted", "dataDir", "size", "parts", "usesLegacyChecksum")}
            logical["erasure"] = {name: value.get("erasure", {}).get(name) for name in
                ("data_blocks", "parity_blocks", "block_size", "distribution", "algorithm")}
            candidates.append(logical)
        require(len(candidates) == 12 and all(value == candidates[0] for value in candidates)
                and candidates[0]["deleted"] is False and candidates[0]["isLatest"] is True,
                "MRF progress has no single original twelve-member ACK authority")
        authority = candidates[0]
        restored = 0
        for layout in snapshot[target]["formats"]:
            disk = layout["disk"]
            metadata = current.get((target, disk, metadata_path), {}).get("metadata", {})
            matches = [value for value in metadata.get("versions", []) if (value.get("versionId") or "null") == wanted]
            if not matches: continue
            require(len(matches) == 1 and metadata.get("bucket") == bucket and metadata.get("key") == item["key"],
                    "MRF replay confused physical key/version identities")
            version, erasure = matches[0], matches[0].get("erasure", {})
            logical = {name: version.get(name) for name in authority if name != "erasure"}
            logical["erasure"] = {name: erasure.get(name) for name in authority["erasure"]}
            geometry = layout["value"]["xl"]
            slot = geometry["sets"][0].index(geometry["this"])
            require(logical == authority and type(erasure.get("index")) is int
                    and erasure["index"] == erasure["distribution"][slot]
                    and version.get("role") == ("data" if erasure["index"] <= erasure["data_blocks"] else "parity"),
                    "MRF replay changed logical authority or its actual target set role")
            parts = version.get("parts", [])
            require(version.get("inline") is None and len(parts) == 1 and parts[0].get("number") == 1
                    and parts[0].get("size") == parts[0].get("actualSize") > 1024**2,
                    "MRF progress requires its reviewed external plaintext part")
            path = f"{bucket}/{item['key']}/{version['dataDir']}/part.1"
            entry = current.get((target, disk, path))
            if entry:
                require(type(entry.get("size")) is int and entry["size"] > 32
                        and bool(re.fullmatch(r"[0-9a-f]{64}", entry.get("sha256", ""))),
                        "MRF progress lacks complete part-file identity")
                restored += 1
        if restored == 4: complete.append(list(mrf_replay.identity(item)))
    return {"completeACKs": complete, "pendingACKs": len(obligations) - len(complete),
            "partial": 0 < len(complete) < len(obligations), "finalStreamingProofClaimed": False}


def active_mrf_task(events, obligations, bindings, after, target):
    candidates = []
    for item in obligations:
        for owner in bindings[mrf_replay.identity(item)]:
            node = owner["node"]
            if node == target: continue
            rows = events.get(node, [])
            for start in rows:
                token, stamp = start.get("task_id"), event_time(start)
                if not (start.get("target") == "rustfs::heal::task" and start.get("stage") == "start"
                        and start.get("message") == "Heal object started" and start.get("bucket") == item["bucket"]
                        and start.get("object") == item["key"] and "version_id" in start
                        and mrf_replay.source_version(start["version_id"], item["versionId"])
                        and isinstance(token, str) and token and stamp is not None and stamp >= after): continue
                accepted = any(row.get("target") == "rustfs::heal::manager" and row.get("source") == "mrf"
                    and row.get("message") == "Heal queue request accepted" and row.get("result") == "accepted"
                    and row.get("request_id") == token and event_time(row) is not None
                    and after <= event_time(row) <= stamp for row in rows)
                terminal = any(row.get("target") == "rustfs::heal::manager" and row.get("task_id") == token
                    and row.get("state") == "task_completed" and event_time(row) is not None
                    and event_time(row) >= stamp for row in rows)
                if accepted and not terminal: candidates.append((node, token, item))
    require(len(candidates) == len({(node, token) for node, token, _ in candidates}),
            "MRF source duplicated an active task identity")
    return candidates


def interrupt_mrf(probe, fleet, window, binding, bucket, expected, obligations, bindings, rejoin_time,
                  before, authorities, target):
    fleet.record("MRF-crash-support-table", REFERENCE)
    fleet.record("MRF-pre-registered-crash-window", {"objects": MRF_COUNT, "windowSeconds": WINDOW_SECONDS,
        "maxConcurrentHeals": 1, "maxConcurrentPerSet": 1, "replayBatch": 1,
        "actualProcessConfiguration": {node: value["backgroundConfig"] for node, value in before.items()},
        "partialPhysicalProgressRequired": True, "fixedSleepUsedAsWindow": False})
    deadline = time.monotonic() + WINDOW_SECONDS
    chosen = None
    while chosen is None:
        events = window.capture()
        scene = fleet.snapshot("MRF-coordinator-running-window")
        progress = mrf_progress(scene, authorities, bucket, obligations, target)
        for owner, token, obligation in active_mrf_task(events, obligations, bindings, rejoin_time, target):
            if not progress["partial"] or list(mrf_replay.identity(obligation)) in progress["completeACKs"]: continue
            endpoint = "node" + str(fleet.nodes.index(owner) + 1)
            path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + quote(obligation["key"], safe="/")
            observed = fleet.record("MRF-original-volatile-task-running", Probe(binding, endpoint).admin(path, [("clientToken", token)], b""))
            require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
                    "MRF active source task lost its exact status before freeze")
            if observed["response"].get("summary") == "running":
                chosen = owner, token, obligation, endpoint, path
                break
        require(chosen is not None or time.monotonic() < deadline, "NOT_OBSERVED: no active MRF coordinator with partial physical ACK replay")
        if chosen is None: time.sleep(.1)
    owner, token, obligation, endpoint, path = chosen
    peers = processes(fleet, "MRF-processes-before-coordinator-freeze")
    process_identity = freeze(fleet, owner, peers)
    frozen = fleet.snapshot("MRF-frozen-coordinator-partial-scene")
    progress = mrf_progress(frozen, authorities, bucket, obligations, target)
    require(progress["partial"] and list(mrf_replay.identity(obligation)) not in progress["completeACKs"],
            "NOT_OBSERVED: original MRF task finished before its physical crash boundary")
    require(any(node == owner and active == token for node, active, _ in
                active_mrf_task(window.capture(), obligations, bindings, rejoin_time, target)),
            "NOT_OBSERVED: MRF task became terminal before the coordinator freeze")
    raw = fleet.record("MRF-frozen-committed-records", fleet.remote(owner, "mrf-records"))
    committed = checkpoint(raw)
    # The original binding already checked the actual enqueue time. Require
    # that exact intent digest below, rather than re-dating it at rejoin.
    bound = mrf_replay.pending_bindings({owner: committed}, [obligation], 0)
    original_owner = next(item for item in bindings[mrf_replay.identity(obligation)] if item["node"] == owner)
    recovered_owner = bound[mrf_replay.identity(obligation)][0]
    require(all(recovered_owner[name] == original_owner[name] for name in
        ("node", "checkpointOwner", "responsibilityId", "bucketIncarnationId"))
        and recovered_owner["sequence"] >= original_owner["sequence"], "MRF pending crash checkpoint changed its original ACK responsibility")
    exit_proof = kill(fleet, owner, process_identity, peers)
    after = fleet.record("MRF-committed-records-after-SIGKILL", fleet.remote(owner, "mrf-records", quiescent=True))
    require(canonical(raw) == canonical(after), "SIGKILL changed the frozen committed MRF record bytes")
    fleet.record("same-MRF-coordinator-restarted", fleet.remote(owner, "start"))
    fleet.wait_ready()
    restarted = processes(fleet, "MRF-processes-after-coordinator-restart")
    continuity_proof = continuity(peers, restarted, owner)
    unknown = fleet.record("lost-volatile-MRF-token-refused", Probe(binding, endpoint).admin(path, [("clientToken", token)], b""))
    validate_unknown_token(unknown, token)
    return {"branch": "mrf-coordinator-kill", "coordinator": owner, "targetKeptRunning": target,
        "volatileOriginalTaskId": token, "ackIdentity": list(mrf_replay.identity(obligation)),
        "originalResponsibilityId": original_owner["responsibilityId"], "partialPhysicalWindow": progress,
        "originalIntentDigest": original_owner["intentDigest"], "frozenIntentDigest": recovered_owner["intentDigest"],
        "actualExit": exit_proof, "processContinuity": continuity_proof,
        "oldTokenCompletedClaim": False, "newAutomaticTaskRequired": True, "manualCompensationTasks": 0}


def root_observer(*args, **kwargs):
    return root_restart.observe_task(*args, **kwargs, interruption="sigkill")


def run_replacement(args, nodes):
    directory = args.evidence / "replacement-coordinator-kill"
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h2"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="replacement", resource_profile="full-restart")
    fleet.node_runtime_profiles = {node: "replacement-target" for node in nodes[1:]}
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] == REFERENCE["candidateCommit"], "unreviewed coordinator durability boundary")
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    fleet.record("registered-coordinator-support-table", REFERENCE)
    fleet.record("registered-replacement-crash-budget", {"objects": REPLACEMENT_COUNT,
        "objectBytes": mrf_replay.SIZE, "rebuildSeconds": replacement.REBUILD_SECONDS,
        "requiresNonzeroAndUnfinishedPhysicalProgress": True})
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = lab_binding(args, candidate, nodes, run_id, bucket)
    binding.update(maxInventoryObjects=REPLACEMENT_COUNT, maxInventoryBytes=2 * 1024**3,
        leaseExpiresAt=(datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat())
    fleet.record("binding", binding)
    probe, active = Probe(binding), []
    result = {"topology": "H2", "branch": "replacement-coordinator-kill", "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("actual-topology", validate_topology(fleet.snapshot("replacement-H2-topology"), "H2"))
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, "DB", REPLACEMENT_COUNT, [mrf_replay.SIZE])
        baseline = fleet.record("replacement-full-S3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("replacement-independent-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        fleet.stop()
        proof = replacement.run_branch(recorder, fleet, bucket, "replacement-coordinator-kill", expected, baseline,
            active, [], coordinator_interruptor=interrupt_replacement)
        require(proof["responsibility"]["coordinatorInterruption"]["targetKeptRunning"] == nodes[-1],
                "replacement coordinator branch did not keep its actual target running")
        result.update(verdict="PASS", proof=proof)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try: fleet.stop(); fleet.snapshot("replacement-coordinator-failure", quiescent=True)
            except Exception as capture: result["failureEvidenceError"] = str(capture)
    finally:
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"): parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=(CASE_ID,), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "F04 requires the actual four current Heal nodes")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha))
            and bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "verified workflow candidate required")
    require(json.loads(args.candidate.read_text())["source_sha"] == REFERENCE["candidateCommit"], "unreviewed coordinator persistence support")
    recipe = select(CASE_ID)[0]
    require(recipe["variants"] == list(BRANCHES) and recipe["contract"]["probes"]["topology"] == "H2",
            "F04 omitted its source branches or topology")
    results = []
    try:
        root_args = copy.copy(args)
        root_args.evidence = args.evidence / "root-coordinator-kill"
        root_args.evidence.mkdir()
        root = dx_topology(root_args, "H2", nodes[:4], observer=root_observer, branches=(BRANCHES[0],),
            source_branches=BRANCHES, runtime_profile="root-restart", policy_reference=REFERENCE, target_nodes=nodes[1:4])
        results.append({**root, "branch": BRANCHES[0]})
        if root["cleanup"] == "PASS":
            mrf_args = copy.copy(args)
            mrf_args.evidence = args.evidence / "mrf-coordinator-kill"
            mrf_args.evidence.mkdir()
            mrf = mrf_replay.run_branch(mrf_args, "offline-put", nodes[:4], after_rejoin=interrupt_mrf,
                put_count=MRF_COUNT, fixture="DB")
            results.append({**mrf, "branch": BRANCHES[1], "mechanismScenario": mrf["branch"]})
        if results[-1]["cleanup"] == "PASS": results.append(run_replacement(args, nodes[:4]))
    finally:
        complete = len(results) == 3 and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "F04 requires all three H2 coordinator mechanisms, real partial SIGKILL windows and complete cleanup")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal coordinator crash validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
