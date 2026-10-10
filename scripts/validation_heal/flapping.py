#!/usr/bin/env python3
"""B10: three bounded member-loss cycles and an observed rebuild interruption."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
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
from urllib import request
from urllib.error import URLError
from urllib.parse import quote

try:
    from .heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .fleet_node import PROCESS_FIELDS
    from .disk_loss import quorum_plan as drive_quorum, member_state
    from .node_outage import quorum_plan as node_quorum, put_ack, replica_census
    from .mrf_records import verify_reference
    from .mrf_replay import identity, pending_bindings, read_checkpoints, retirement, wait_replay, source_version
    from .scanner_repair import JournalWindow, event_time
    from .shards import files, map_target, verify_cohort
    from .stale_rejoin import logical_metadata
    from .versions import verify_quorum_contract
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from fleet_node import PROCESS_FIELDS
    from disk_loss import quorum_plan as drive_quorum, member_state
    from node_outage import quorum_plan as node_quorum, put_ack, replica_census
    from mrf_records import verify_reference
    from mrf_replay import identity, pending_bindings, read_checkpoints, retirement, wait_replay, source_version
    from scanner_repair import JournalWindow, event_time
    from shards import files, map_target, verify_cohort
    from stale_rejoin import logical_metadata
    from versions import verify_quorum_contract


CASE_ID = "heal.disk-and-node-flapping"
BRANCHES = ("drive-flap-3", "node-flap-3", "interrupt-rebuild-again")
SIZE = 1024**2 + 73
PROTOCOL = {"cycles": 3, "uniqueACKsPerCycle": [16, 16, 64], "foregroundConcurrency": 4,
    "faultWindowSeconds": 120, "persistenceSeconds": 30, "replaySeconds": 240,
    "partialWindowSeconds": 90, "branchDeadlineSeconds": 1200,
    "extraThirdCycleInterruption": 1, "maxActiveTasks": 4, "maxQueuedTasks": 512,
    "maxAcceptedTasksPerACKPerOwner": 64, "maxSystemTasksPerOwner": 256,
    "maxFDs": 4096, "maxRssBytes": 4 * 1024**3,
    "maxSampledCpuCores": 8, "maxForegroundErrorRate": 0,
    "adminCompensationTasks": 0, "backupRestorationUsed": False}


def same_process(before, after):
    require(all(before.get(name) == after.get(name) for name in PROCESS_FIELDS),
            "flapping changed an unrelated process or a live-gated service birth")


def node_return(before, after):
    require(before.get("pid", 0) > 1 and after.get("pid", 0) > 1
            and (before["pid"], before["startTicks"]) != (after["pid"], after["startTicks"])
            and all(before.get(name) == after.get(name) for name in
                ("bootId", "unit", "executable", "binarySha256", "commandSha256", "environmentFileSha256", "settings")),
            "node flap did not restart the exact original unit, candidate, boot and runtime")


def untouched(before, after, members):
    left, right = files(before), files(after)
    require({node: value["formats"] for node, value in before.items()} ==
            {node: value["formats"] for node, value in after.items()}, "flap changed original disk UUIDs or set slots")
    require(all(right.get(key) == value for key, value in left.items()),
            "flap changed an original acknowledged shard or control")
    require(all(key[:2] not in members for key in set(right) - set(left)),
            "offline target received a new ACK shard while inaccessible")
    return {"originalShardAndControlBytesUnchanged": True, "offlineMembersReceivedNoACKFiles": True}


def replay_progress(scene, authorities, bucket, obligations, members):
    """Observe exact original member roles; this is not final streaming verification."""
    validate_topology(scene, "H2")
    require(members and len(members) in {1, 4}, "flap progress requires the exact original lost members")
    current, original, complete = files(scene), files(authorities), []
    layouts = {(node, row["disk"]): row["value"]["xl"] for node, value in scene.items() for row in value["formats"]}
    require(members <= set(layouts), "flap target members escaped the original topology")
    for item in obligations:
        require(item["bucket"] == bucket and item["versionId"] == "null", "flap progress crossed an ACK identity")
        path = f"{bucket}/{item['key']}/xl.meta"
        sources = [entry["metadata"] for (node, disk, name), entry in original.items()
                   if name == path and (node, disk) not in members and entry.get("metadata")]
        require(len(sources) == 16 - len(members) and
                all(canonical(logical_metadata(value)) == canonical(logical_metadata(sources[0])) for value in sources),
                "flap has no complete surviving ACK authority")
        authority = logical_metadata(sources[0])
        versions = authority.get("versions", [])
        require(len(versions) == 1 and versions[0].get("versionId") == "null"
                and versions[0].get("isLatest") is True and versions[0].get("deleted") is False,
                "flap authority is a different version or marker")
        restored = 0
        for member in members:
            metadata = current.get((*member, path), {}).get("metadata")
            if metadata is None: continue
            require(canonical(logical_metadata(metadata)) == canonical(authority), "flap replay changed ACK metadata authority")
            version = metadata["versions"][0]
            erasure, layout = version["erasure"], layouts[member]
            slot = layout["sets"][0].index(layout["this"])
            require(type(erasure.get("index")) is int and erasure["index"] == erasure["distribution"][slot]
                    and version.get("role") == ("data" if erasure["index"] <= erasure["data_blocks"] else "parity"),
                    "flap replay put an ACK on the wrong original set role")
            parts = version.get("parts", [])
            require(version.get("inline") is None and version.get("dataDir") and len(parts) == 1
                    and parts[0].get("number") == 1 and parts[0].get("size") == parts[0].get("actualSize") > 1024**2,
                    "flap progress requires one actual external plaintext ACK part")
            part = current.get((*member, f"{bucket}/{item['key']}/{version['dataDir']}/part.1"))
            if part is not None:
                require(type(part.get("size")) is int and part["size"] > 32
                        and re.fullmatch(r"[0-9a-f]{64}", part.get("sha256", "")), "flap replay lacks an actual complete part identity")
                restored += 1
        if restored == len(members): complete.append(list(identity(item)))
    return {"completeACKs": complete, "pendingACKs": len(obligations) - len(complete),
            "partial": 0 < len(complete) < len(obligations), "finalStreamingProofClaimed": False}


def retained_pending(checkpoints, obligations, bindings, pending):
    require(pending and pending <= {identity(item) for item in obligations}, "interruption has no exact incomplete ACKs")
    for wanted in pending:
        for owner in bindings[wanted]:
            value = checkpoints[owner["node"]]
            rows = [row for row in value["intents"] if identity(row) == wanted]
            require(value["present"] and value["owner"] == owner["checkpointOwner"]
                    and value["sequence"] >= owner["sequence"] and len(rows) == 1
                    and rows[0]["lifecycle"]["responsibility_id"] == owner["responsibilityId"]
                    and rows[0]["lifecycle"]["source_bucket_incarnation_id"] == owner["bucketIncarnationId"]
                    and rows[0]["lifecycle"]["state"] == {"state": "active"}
                    and rows[0]["lifecycle"].get("last_operator_acceptance") is None,
                    "incomplete interrupted ACK was retired, rebound, rolled back or operator-accepted")
    return {"pendingOriginalResponsibilities": len(pending), "ownerIncarnationAndResponsibilityStable": True}


def bounded_admissions(events, obligations):
    lookup = {(item["bucket"], item["key"], item["versionId"]): identity(item) for item in obligations}
    tokens, counts, system_counts = {}, {}, {}
    for node, rows in events.items():
        accepted = {row.get("request_id") for row in rows if row.get("target") == "rustfs::heal::manager"
            and row.get("message") == "Heal queue request accepted" and row.get("source") == "mrf" and row.get("result") == "accepted"}
        for row in rows:
            if row.get("target") != "rustfs::heal::task" or row.get("message") != "Heal object started": continue
            wanted = (row.get("bucket"), row.get("object"), "null")
            require((wanted in lookup or wanted[0] == ".rustfs.sys" and isinstance(wanted[1], str) and wanted[1])
                    and "version_id" in row and source_version(row["version_id"], "null"),
                    "flapping source task escaped owned ACKs or the internal system bucket")
            bound = lookup[wanted] if wanted in lookup else (*wanted, "internal-system")
            token = row.get("task_id")
            require(isinstance(token, str) and token in accepted and event_time(row) is not None,
                    "flapping task has no actual accepted MRF admission")
            previous = tokens.get(token)
            require(previous is None or previous == (node, bound), "flap task token changed owner or ACK identity")
            if previous is None:
                tokens[token] = (node, bound)
                key = node, bound
                counts[key] = counts.get(key, 0) + 1
                require(counts[key] <= PROTOCOL["maxAcceptedTasksPerACKPerOwner"], "flapping exceeded its registered retry/task budget")
                if wanted not in lookup:
                    system_counts[node] = system_counts.get(node, 0) + 1
                    require(system_counts[node] <= PROTOCOL["maxSystemTasksPerOwner"], "flapping exceeded its registered internal-system task budget")
    return {"distinctAcceptedTaskIds": len(tokens), "maxTasksForOneACKOwner": max(counts.values(), default=0),
            "internalSystemTasks": sum(system_counts.values()), "internalSystemTasksCertifyOwnedACKs": False,
            "duplicateTokensCountedAsIndependentRepairs": False}


class Budget:
    def __init__(self, fleet, binding):
        self.fleet, self.binding = fleet, binding
        self.deadline = time.monotonic() + PROTOCOL["branchDeadlineSeconds"]
        self.previous = {}
        self.samples = 0

    def check(self):
        require(time.monotonic() < self.deadline, "flapping exceeded its preregistered total deadline")

    def sample(self, nodes):
        self.check()
        for node in nodes:
            value = self.fleet.record("flap-process-resource-sample", {"node": node, **self.fleet.remote(node, "process-metrics")})
            require(value.get("readOnly") is True and type(value.get("fileDescriptors")) is int
                    and 0 < value["fileDescriptors"] <= PROTOCOL["maxFDs"]
                    and type(value.get("rssBytes")) is int and 0 < value["rssBytes"] <= PROTOCOL["maxRssBytes"],
                    "flapping exceeded its frozen FD/RSS resource budget")
            previous = self.previous.get(node)
            if previous is not None:
                if all(previous["identity"].get(name) == value["identity"].get(name) for name in PROCESS_FIELDS):
                    elapsed = (value["monotonicNs"] - previous["monotonicNs"]) / 1e9
                    require(elapsed > 0 and type(value["clockTicksPerSecond"]) is int and value["clockTicksPerSecond"] > 0
                            and value["clockTicksPerSecond"] == previous["clockTicksPerSecond"]
                            and value["cpuTicks"] >= previous["cpuTicks"] and value["clockDomain"] == previous["clockDomain"],
                            "flapping resource clock or counters regressed")
                    cores = (value["cpuTicks"] - previous["cpuTicks"]) / value["clockTicksPerSecond"] / elapsed
                    self.fleet.record("flap-CPU-interval", {"node": node, "cpuCores": cores, "elapsedSeconds": elapsed})
                    require(cores <= PROTOCOL["maxSampledCpuCores"], "flapping exceeded its frozen CPU budget")
                else:
                    require(node == self.fleet.nodes[-1], "flapping changed an unrelated peer process")
                    node_return(previous["identity"], value["identity"])
            self.previous[node] = value
            endpoint = "node" + str(self.fleet.nodes.index(node) + 1)
            observed = self.fleet.record("flap-queue-resource-sample", Probe(self.binding, endpoint).admin(
                "/rustfs/admin/v3/background-heal/status", [], b""))
            require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict), "flap queue status unavailable")
            body = observed["response"]
            require(type(body.get("healActiveTasks")) is int and 0 <= body["healActiveTasks"] <= PROTOCOL["maxActiveTasks"]
                    and type(body.get("healQueueLength")) is int and 0 <= body["healQueueLength"] <= PROTOCOL["maxQueuedTasks"],
                    "flapping exceeded its registered active/queued task budget")
        self.samples += 1


def foreground(recorder, bucket, cycle, expected, deadline):
    count = PROTOCOL["uniqueACKsPerCycle"][cycle - 1]
    def put(index):
        require(time.monotonic() < deadline, "foreground admission exceeded the registered offline window")
        key = f"flap/cycle-{cycle}/nonce-{index:03d}.bin"
        require(not any(entry["key"] == key for entry in expected), "flap reused a previous ACK key")
        body = payload(f"{bucket}/{key}/0", SIZE + index)
        ack = recorder.call("put_object", {"Bucket": bucket, "Key": key,
            "Metadata": {"heal-fixture": "DB-DL", "cycle": str(cycle), "nonce": str(index)},
            "Tagging": "suite=heal&fixture=flap"}, body, admission_deadline=deadline)
        require(ack.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200, "flap PUT lacks its explicit commit ACK")
        recorder.call("head_object", {"Bucket": bucket, "Key": key}, admission_deadline=deadline)
        return key, body, ack
    def control_read():
        require(time.monotonic() < deadline, "control admission exceeded the registered offline window")
        entry = expected[0]
        observed = recorder.probe.s3("get_object", {"Bucket": bucket, "Key": entry["key"]})
        save(recorder.output / f"cycle-{cycle}-degraded-control-get.json", observed)
        require(observed.get("acknowledged") is True and observed.get("payload") == entry["payload"], "degraded flap control GET differs from ACK")
        recorder.call("list_objects_v2", {"Bucket": bucket, "Prefix": f"flap/cycle-{cycle}/"}, admission_deadline=deadline)
    with ThreadPoolExecutor(max_workers=PROTOCOL["foregroundConcurrency"]) as pool:
        reads = pool.submit(control_read)
        writes = [pool.submit(put, index) for index in range(count)]
        rows = [future.result() for future in writes]
        reads.result()
    manifest, obligations = copy.deepcopy(expected), []
    for key, body, ack in rows:
        manifest = put_ack(manifest, key, body, ack, "DB")
        obligations.append({"bucket": bucket, "key": key, "versionId": "null", "poolIndex": 0, "setIndex": 0,
                            "kind": 3, "authoritativelyAbsentRequired": False})
    return manifest, obligations


def refuse_endpoint(binding):
    try:
        with request.build_opener(request.ProxyHandler({})).open(binding["endpoints"]["node4"] + "/health/ready", timeout=3):
            raise ValueError("flapped node still accepts endpoint requests")
    except URLError as exc:
        require(isinstance(exc.reason, ConnectionRefusedError), "node flap has no exact connection refusal")
    return {"connectionRefused": True}


class MemberFault:
    def __init__(self, fleet, binding, target, drive, budget):
        self.fleet, self.binding, self.target, self.drive, self.budget = fleet, binding, target, drive, budget
        self.node = fleet.nodes[-1]
        self.baseline = None
        self.processes = None

    def inject(self):
        self.processes = {node: self.fleet.remote(node, "process") for node in self.fleet.nodes}
        self.fleet.record("flap-processes-before-loss", self.processes)
        if self.drive:
            self.fleet.drive_gates.add(self.node)
            self.fleet.record("flap-live-gate-arm", self.fleet.remote(self.node, "drive-gate-arm-live",
                disk=self.target["disk"], expected=self.processes[self.node]))
            self.fleet.record("flap-live-gate-active", self.fleet.remote(self.node, "drive-gate-activate"))
        else:
            stopped = self.fleet.record("flap-owned-node-stop", self.fleet.remote(self.node, "stop"))
            require(stopped.get("quiescent") is True and stopped.get("service", {}).get("MainPID") == "0", "flap target unit did not stop")
        return self.witness()

    def witness(self):
        self.budget.check()
        if not self.drive:
            current = self.fleet.record("flap-stopped-process", self.fleet.remote(self.node, "process"))
            require(current["pid"] == 0 and current["bootId"] == self.processes[self.node]["bootId"], "flap target stopped witness changed")
            return self.fleet.record("flap-endpoint-refusal", refuse_endpoint(self.binding))
        current = self.fleet.remote(self.node, "process")
        same_process(self.processes[self.node], current)
        denial = self.fleet.record("flap-service-drive-refusal", self.fleet.remote(self.node, "drive-gate-probe"))
        require(denial.get("errno") == 13 and denial.get("readProbe", {}).get("errno") == 13
                and denial.get("dacBypassRemoved") is True and denial.get("disk") == self.target["disk"],
                "drive flap lacks exact service read/write denial")
        deadline = min(time.monotonic() + 90, self.budget.deadline)
        while True:
            observed = self.fleet.record("flap-admin-member-offline", Probe(self.binding).admin(
                "/rustfs/admin/v3/info", [], b"", method="GET"))
            try:
                proof = member_state(observed, self.target, 16, self.baseline)
                return self.fleet.record("flap-drive-double-witness", {"service": denial, "admin": proof})
            except ValueError as exc:
                if not str(exc).startswith("NOT_OBSERVED:"): raise
                require(time.monotonic() < deadline, str(exc))
                time.sleep(1)

    def release(self):
        if self.drive:
            observed = self.fleet.record("flap-live-drive-rejoin", self.fleet.remote(self.node, "drive-gate-release-live"))
            require(observed.get("sameOriginalProcess") is True and observed.get("serviceRestarted") is False,
                    "drive flap rejoin restarted its service")
            self.fleet.drive_gates.remove(self.node)
        else:
            self.fleet.record("flap-original-node-rejoin", self.fleet.remote(self.node, "start"))
        self.fleet.wait_ready()
        returned = {node: self.fleet.remote(node, "process") for node in self.fleet.nodes}
        for node in self.fleet.nodes:
            if node == self.node and not self.drive: node_return(self.processes[node], returned[node])
            else: same_process(self.processes[node], returned[node])
        self.fleet.record("flap-process-continuity", returned)
        if self.drive:
            deadline = min(time.monotonic() + 90, self.budget.deadline)
            while True:
                observed = self.fleet.record("flap-admin-member-online", Probe(self.binding).admin(
                    "/rustfs/admin/v3/info", [], b"", method="GET"))
                try:
                    online = member_state(observed, self.target, 16)
                    require(online == self.baseline, "flap original member returned with a different identity")
                    break
                except ValueError:
                    require(time.monotonic() < deadline, "NOT_OBSERVED: original flapped drive did not return online")
                    time.sleep(1)


def interrupt_rebuild(fleet, window, binding, bucket, obligations, bindings, authorities, members, fault, budget, after, all_obligations):
    deadline = min(time.monotonic() + PROTOCOL["partialWindowSeconds"], budget.deadline)
    while True:
        budget.sample(fleet.nodes)
        events = window.capture()
        bounded_admissions(events, all_obligations)
        scene = fleet.snapshot("flap-third-cycle-rebuild-window")
        progress = fleet.record("flap-third-cycle-physical-progress", replay_progress(scene, authorities, bucket, obligations, members))
        pending = {identity(item) for item in obligations} - {tuple(item) for item in progress["completeACKs"]}
        active = None
        if progress["partial"]:
            for item in obligations:
                if identity(item) not in pending: continue
                for owner in bindings[identity(item)]:
                    endpoint = "node" + str(fleet.nodes.index(owner["node"]) + 1)
                    starts = [row for row in events[owner["node"]] if row.get("target") == "rustfs::heal::task"
                        and row.get("message") == "Heal object started" and row.get("bucket") == bucket
                        and row.get("object") == item["key"] and "version_id" in row and source_version(row["version_id"], "null")
                        and event_time(row) is not None and event_time(row) >= after]
                    for row in reversed(starts):
                        path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + quote(item["key"], safe="/")
                        observed = fleet.record("flap-interrupted-task-running", Probe(binding, endpoint).admin(path,
                            [("clientToken", row["task_id"])], b""))
                        require(observed.get("httpStatus") == 200, "third-cycle original task lost its exact status")
                        if observed.get("response", {}).get("summary") == "running":
                            active = {"node": owner["node"], "taskId": row["task_id"], "ACK": list(identity(item))}
                            break
                    if active: break
                if active: break
        if active:
            interrupted_at = time.monotonic()
            fault.inject()
            frozen = fleet.record("flap-third-cycle-offline-progress", replay_progress(
                {node: fleet.remote(node, "snapshot", quiescent=node == fault.node) for node in fleet.nodes},
                authorities, bucket, obligations, members))
            require(frozen["partial"] and active["ACK"] not in frozen["completeACKs"],
                    "NOT_OBSERVED: actual rebuild completed before the third-cycle repeated loss")
            pending = {identity(item) for item in obligations} - {tuple(item) for item in frozen["completeACKs"]}
            retained = retained_pending(read_checkpoints(fleet, fleet.nodes[:-1], "flap-interrupted-durable-records"),
                obligations, bindings, pending)
            fault.witness()
            fault.release()
            require(time.monotonic() - interrupted_at <= PROTOCOL["faultWindowSeconds"],
                    "third-cycle repeated loss exceeded its registered window budget")
            return {"runningOriginalTask": active, "physicalWindow": frozen, "durable": retained,
                "extraInterruptionCount": 1, "fixedSleepUsedAsWindow": False}
        require(time.monotonic() < deadline, "NOT_OBSERVED: no exact running MRF task with partial third-cycle physical reconstruction")
        time.sleep(.1)


def run_branch(args, branch, nodes):
    directory = args.evidence / branch
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h2"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="flapping")
    candidate = json.loads(args.candidate.read_text())
    tool = fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    contract = verify_quorum_contract(tool, candidate)
    fleet.record("candidate-durable-MRF-reference", verify_reference(candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, 5)}, "defaultEndpoint": "node1",
        "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 128, "maxInventoryBytes": 256 * 1024**2}
    fleet.record("binding", binding)
    fleet.record("preregistered-three-cycle-protocol", {**PROTOCOL, "branch": branch,
        "lossKind": "one-original-drive" if branch == "drive-flap-3" else "one-original-node",
        "thirdCycleExtraInterruptionRequired": branch == "interrupt-rebuild-again"})
    probe, result = Probe(binding), {"topology": "H2", "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING", "cycles": []}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        recorder = Recorder(probe, directory / "foreground-ACK-ledger")
        expected = seed(recorder, bucket, "DB", 4, [SIZE])
        fleet.record("original-S3-manifest", verify_seed(expected, probe.inventory(bucket, unversioned_current_tags=True)))
        before = fleet.snapshot("original-physical-baseline")
        fleet.record("topology-proof", validate_topology(before, "H2"))
        require(all(not value["intents"] for value in read_checkpoints(fleet, nodes, "fresh-MRF-baseline").values()),
                "fresh flapping service has preexisting durable responsibilities")
        target = map_target(before, bucket, "objects/0001.bin", "data", 1)
        target = {**target, **next(shard for shard in target["cohort"] if shard["node"] == nodes[-1])}
        drive = branch == "drive-flap-3"
        members = {(nodes[-1], target["disk"])} if drive else {(nodes[-1], disk) for disk in range(4)}
        fleet.record("actual-loss-quorum-plan", drive_quorum(target, contract) if drive else node_quorum(target, nodes[-1], contract))
        budget = Budget(fleet, binding)
        fault = MemberFault(fleet, binding, target, drive, budget)
        if drive:
            fault.baseline = member_state(fleet.record("flap-original-admin-member", probe.admin(
                "/rustfs/admin/v3/info", [], b"", method="GET")), target, 16)
        all_obligations, all_bindings = [], {}
        window = JournalWindow(fleet, "flapping-source-and-retry-window",
            event_targets={"rustfs::heal::manager", "rustfs::heal::task"})
        for cycle in range(1, PROTOCOL["cycles"] + 1):
            budget.sample(nodes)
            original = fleet.snapshot("cycle-original-physical")
            started = time.monotonic()
            offline = fault.inject()
            online = nodes if drive else nodes[:-1]
            budget.sample(online)
            earliest_ms = int(time.time() * 1000) - 5000
            expected, obligations = foreground(recorder, bucket, cycle, expected,
                min(started + PROTOCOL["faultWindowSeconds"], budget.deadline))
            all_obligations.extend(obligations)
            deadline = min(time.monotonic() + PROTOCOL["persistenceSeconds"], budget.deadline)
            while True:
                try:
                    checkpoints = read_checkpoints(fleet, online, "flap-offline-committed-ACKs")
                    bindings = pending_bindings(checkpoints, obligations, earliest_ms)
                    break
                except ValueError as exc:
                    if not str(exc).startswith("NOT_OBSERVED:"): raise
                    require(time.monotonic() < deadline, "flap durable persistence exceeded its budget: " + str(exc))
                    time.sleep(.2)
            all_bindings.update(bindings)
            fleet.record("cycle-durable-ACK-bindings", [{"identity": list(key), "owners": value} for key, value in bindings.items()])
            fault.witness()
            authorities = {node: fleet.remote(node, "snapshot", quiescent=not drive and node == nodes[-1]) for node in nodes}
            fleet.record("cycle-offline-authorities", authorities)
            fleet.record("cycle-original-control-proof", untouched(original, authorities, members))
            require(time.monotonic() - started <= PROTOCOL["faultWindowSeconds"], "flap offline window exceeded its registered budget")
            returned = datetime.now(timezone.utc)
            fault.release()
            interruption = None
            if branch == "interrupt-rebuild-again" and cycle == 3:
                interruption = interrupt_rebuild(fleet, window, binding, bucket, obligations, bindings, authorities,
                    members, fault, budget, returned, all_obligations)
            def observe():
                budget.sample(nodes)
                bounded_admissions(window.capture(), all_obligations)
            proofs, retired = wait_replay(fleet, window, binding, nodes, bucket, expected, obligations, bindings,
                returned, original, authorities, [], observe=observe, deadline_limit=budget.deadline)
            fleet.record("cycle-full-ACK-task-proofs", [{"identity": list(key), **value} for key, value in proofs.items()])
            reconstructed = fleet.snapshot("cycle-complete-physical-before-target-GET")
            cohorts = replica_census(reconstructed, bucket, expected, authorities)
            for cohort in cohorts: verify_cohort(fleet, cohort, "cycle-full-streaming-proof")
            inventory = fleet.record("cycle-complete-S3-manifest", probe.inventory(bucket, unversioned_current_tags=True))
            fleet.record("cycle-ACK-content-proof", verify_seed(expected, inventory))
            result["cycles"].append({"cycle": cycle, "offlineWitness": offline, "uniqueACKs": len(obligations),
                "sourceTasks": len(proofs), "retirement": retired, "interruption": interruption,
                "sameOriginalDiskUUIDs": True, "physicalAndStreamingBeforeTargetGET": True})
        budget.sample(nodes)
        fleet.stop()
        reconstructed = fleet.snapshot("final-quiescent-physical", quiescent=True)
        cohorts = replica_census(reconstructed, bucket, expected, before)
        for cohort in cohorts: verify_cohort(fleet, cohort, "final-full-streaming-proof")
        retired = retirement(read_checkpoints(fleet, nodes, "final-original-responsibility-retirement", quiescent=True), all_obligations, all_bindings)
        admissions = bounded_admissions(window.capture(), all_obligations)
        fleet.start()
        for endpoint in binding["endpoints"]:
            actual = fleet.record("final-full-peer-manifest", {"endpoint": endpoint,
                **Probe(binding, endpoint).inventory(bucket, unversioned_current_tags=True)})
            fleet.record("final-peer-ACK-proof", {**verify_seed(expected, actual), **compare_inventories(inventory, actual)})
        fleet.stop()
        require(physical_content(reconstructed) == physical_content(fleet.snapshot("final-after-S3-verification", quiescent=True)),
                "target verification altered the frozen product reconstruction")
        budget.check()
        result.update(verdict="PASS", uniqueACKs=len(all_obligations), resourceSamples=budget.samples,
            boundedAdmissions=admissions, durableRetirement=retired, fullPeerManifestEqual=True,
            originalDiskUUIDsPreserved=True, adminCompensationTasks=0, backupRestorationUsed=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try:
                fleet.stop()
                fleet.snapshot("flapping-failure-physical", quiescent=True)
                read_checkpoints(fleet, nodes, "flapping-failure-durable-records", quiescent=True)
            except Exception as evidence_error:
                result["failureEvidenceError"] = str(evidence_error)
    finally:
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("testing-commit", "binary-sha"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--case-id", choices=(CASE_ID,), required=True)
    args = parser.parse_args()
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "four current Heal nodes required")
    require(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha), "invalid candidate binary digest")
    require(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")), "workflow fleet identity required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "flapping executor omitted a source branch")
    results = []
    try:
        for branch in BRANCHES:
            results.append(run_branch(args, branch, nodes[:4]))
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == len(BRANCHES) and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": complete,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL"})
    require(complete, "B10 requires all three bounded H2 branches, actual third-cycle interruption and full cleanup")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal flapping validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
