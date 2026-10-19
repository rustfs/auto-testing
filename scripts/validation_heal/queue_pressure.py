#!/usr/bin/env python3
"""I04: bounded multi-bucket pressure and durable responsibility through Full."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import secrets
import sys
import time
from urllib.parse import quote
import xml.etree.ElementTree as ET

try:
    from .heal_probe import Probe, compare_inventories, require
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .fleet_node import PROCESS_FIELDS, runtime_config
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .cursor import SIZE, map_cohorts, validate_missing_batch, verify_part_manifest, main as generic_main
    from .shards import map_target, validate_repaired_physical, verify_cohort
    from .heal_fault import bounded_fault_id
    from .stop import status, idle_fleet
    from .scanner_repair import JournalWindow, event_time
    from .mrf_records import verify_reference, REFERENCE as MRF_REFERENCE
    from .mrf_replay import (acknowledge, identity, pending_bindings, read_checkpoints, retirement,
                             source_tasks, verified_receipt)
except ImportError:
    from heal_probe import Probe, compare_inventories, require
    from heal_workload import Recorder, seed, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from fleet_node import PROCESS_FIELDS, runtime_config
    from inspector import verify as verify_inspector
    from matrix import select
    from cursor import SIZE, map_cohorts, validate_missing_batch, verify_part_manifest, main as generic_main
    from shards import map_target, validate_repaired_physical, verify_cohort
    from heal_fault import bounded_fault_id
    from stop import status, idle_fleet
    from scanner_repair import JournalWindow, event_time
    from mrf_records import verify_reference, REFERENCE as MRF_REFERENCE
    from mrf_replay import (acknowledge, identity, pending_bindings, read_checkpoints, retirement,
                            source_tasks, verified_receipt)


CASE_ID = "heal.queue-pressure-and-admission"
BRANCHES = ("full-queue", "dropped-admission", "merged-duplicates", "cross-bucket-fairness")
COUNTS = (769, 129, 129, 129)
CAPACITY = 2
MAX_ADMIN_REQUESTS = 24
PRESSURE_SECONDS = 45
DRAIN_SECONDS = 600
RESOURCE_BOUNDS = {"rssBytes": 3 * 1024**3, "fileDescriptors": 8192, "threads": 2048}
REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "sources": {"crates/heal/src/heal/manager.rs":
        "ba9289942fbbe8ce2632686ae86b9d1be0ae9923420e3b091d67148b4ee02094",
        "crates/heal/src/heal/mrf_queue.rs": MRF_REFERENCE["sourceFiles"]["crates/heal/src/heal/mrf_queue.rs"]},
    "adminPriority": "High", "partialWritePriority": "Normal", "queueCapacity": CAPACITY,
    "maxConcurrentHeals": 1, "maxConcurrentPerSet": 1,
    "droppedReason": "overlapping_paths", "mergedRule": "same scope returns the existing token",
    "fullRule": "HTTP 503 SlowDown; admission=full, reason=none",
    "fairness": "every admitted owned bucket must finish within the frozen drain budget",
    "persistentProof": "all exact degraded-write ACKs remain committed during MRF Full; each retires only with its verified source task",
    "nativeCriticalWindowClaimed": False}


def validate_durable_gate(value, run_id, bucket):
    control = value.get("systemMetadataControl") or {}
    require(value.get("scope") == "durable-bucket" and type(value.get("disk")) is int and value["disk"] == 0
            and value.get("dataPath") == f"/data/rustfs1/heal-validation/{run_id}/data/{bucket}"
            and value.get("systemMetadataAccessible") is True and control.get("sameServiceIdentity") is True
            and all(type(control.get(label, {}).get("errno")) is int and control[label]["errno"] == 0
                    and type(control[label].get("uid")) is int and control[label]["uid"] == 0
                    and type(control[label].get("gid")) is int and control[label]["gid"] == 0 for label in ("read", "write"))
            and value.get("sameServiceIdentity") is True and value.get("privateMountNamespace") is True
            and value.get("dacBypassRemoved") is True and value.get("name") == "EACCES"
            and value.get("errno") == 13 and value.get("readProbe", {}).get("errno") == 13,
            "queue pressure needs exact durable-bucket refusal and readable/writable system metadata")
    return value


def rejection(observed, *, admission, reason, code, http):
    require(observed.get("httpStatus") == http and not observed.get("transportError"),
            "queue rejection has a wrong status or an unknown transport result")
    value = observed.get("response")
    if isinstance(value, str):
        value = {element.tag.rsplit("}", 1)[-1]: element.text for element in ET.fromstring(value).iter()}
    require(isinstance(value, dict) and not value.get("clientToken")
            and (value.get("Code") or value.get("code")) == code
            and (value.get("Message") or value.get("message")) ==
                f"heal request not admitted: admission={admission}, reason={reason}",
            "queue rejection erased its actual admission/reason or invented a task")
    return {"httpStatus": http, "code": code, "admission": admission, "reason": reason,
            "independentRepairCount": 0}


def queue_snapshot(value, peers):
    require(value.get("clusterStatusComplete") is True
            and value.get("coverage", {}).get("expected") == peers
            and value["coverage"].get("responded") == peers and value["coverage"].get("unknown") == 0,
            "queue pressure has incomplete actual peer coverage")
    operations = value.get("healOperations") or {}
    for name in ("queueLength", "activeTasks", "retryingTasks"):
        require(type(operations.get(name)) is int and operations[name] >= 0, "queue pressure omitted typed backlog")
    require(operations["queueLength"] <= peers * CAPACITY and operations["activeTasks"] <= peers,
            "queue or active work exceeds its frozen capacity")
    require(value.get("healQueueLength") == operations["queueLength"]
            and value.get("healActiveTasks") == operations["activeTasks"], "queue counters disagree")
    for name in ("queuedByPriority", "activeByPriority", "queuedBySource", "activeBySource"):
        counts = operations.get(name)
        require(isinstance(counts, dict) and counts and all(type(item) is int and item >= 0 for item in counts.values()),
                "queue sample omitted actual source/priority counters")
        require(sum(counts.values()) == operations["queueLength" if name.startswith("queued") else "activeTasks"],
                "queue source/priority counters hide work")
    telemetry = operations.get("admission") or {}
    require(all(type(telemetry.get(name)) is int and telemetry[name] >= 0
                for name in ("accepted", "full", "dropped", "merged")), "queue sample omitted actual admission telemetry")
    return operations


def resources(samples, baseline):
    require(set(samples) == set(baseline), "resource sample dropped a current owned node")
    for node, current in samples.items():
        require(current.get("readOnly") is True and current.get("clockDomain") == baseline[node].get("clockDomain")
                and type(current.get("monotonicNs")) is int and current["monotonicNs"] > baseline[node]["monotonicNs"]
                and all(current.get("identity", {}).get(name) == baseline[node]["identity"].get(name) for name in PROCESS_FIELDS),
                "resource curve crossed its original process birth or monotonic clock")
        require(all(type(current.get(name)) is int and 0 <= current[name] <= limit
                    for name, limit in RESOURCE_BOUNDS.items()), "queue pressure exceeded a pre-registered resource ceiling")
    return {"withinFrozenLimits": True, "nodes": list(samples), "limits": RESOURCE_BOUNDS}


def active_window(current, count):
    outcome = current.get("outcome") or {}
    processed = outcome.get("counters", {}).get("processed")
    return (current.get("summary") == "running" and outcome.get("execution", {}).get("state") == "running"
            and type(processed) is int and 0 < processed < count)


def terminal(current, bucket, expected, missing):
    wanted = {(entry["key"], entry["versionId"]): entry for entry in expected}
    require(len(wanted) == len(expected) and missing <= set(wanted), "queue scope escaped its frozen ACK set")
    outcome = current.get("outcome") or {}
    require(current.get("summary") == "finished" and outcome.get("execution") == {"state": "completed"}
            and outcome.get("coverage") == "complete", "queue task did not complete its actual responsibility")
    counts = {"processed": len(expected), "healed": len(missing), "unchanged": len(expected) - len(missing),
              "failed": 0, "skipped": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    counters = outcome.get("counters") or {}
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in counts.items()),
            "queue task counters hide unresolved, false or failed repairs")
    rows = outcome.get("objects")
    require(isinstance(rows, list) and 0 < len(rows) <= 128 and outcome.get("objectsTruncated") is (len(expected) > 128),
            "queue task confused bounded diagnostics with the independent complete manifest")
    seen, incarnation = set(), None
    for row in rows:
        item = row.get("identity") or {}
        pair = item.get("object"), item.get("versionId") or "null"
        require(item.get("bucket") == bucket and pair in wanted and pair not in seen
                and type(item.get("poolIndex")) is int and item["poolIndex"] == 0
                and type(item.get("setIndex")) is int and item["setIndex"] == 0
                and isinstance(item.get("bucketIncarnationId"), str) and item["bucketIncarnationId"],
                "queue receipt crossed a bucket/version/incarnation/set or duplicated a result")
        incarnation = incarnation or item["bucketIncarnationId"]
        require(item["bucketIncarnationId"] == incarnation
                and row.get("disposition", {}).get("state") == ("repaired" if pair in missing else "verified_healthy"),
                "queue receipt claimed an incorrect disposition or bucket generation")
        seen.add(pair)
    return {"canonicalCounters": counters, "bucketIncarnationId": incarnation,
            "diagnosticObjects": len(rows), "completePhysicalAndS3ManifestRequiredSeparately": True}


def retained(checkpoints, obligations, bindings):
    """A newer sequence may publish the same responsibility; a new owner may not."""
    observed = {}
    for obligation in obligations:
        key = identity(obligation)
        require(key in bindings, "retention proof lacks its original exact ACK binding")
        for owner in bindings[key]:
            current = checkpoints.get(owner["node"], {})
            require(current.get("present") is True and current.get("owner") == owner["checkpointOwner"]
                    and type(current.get("sequence")) is int and current["sequence"] >= owner["sequence"],
                    "queue failure lost or replaced the original committed MRF owner")
            matches = [item for item in current.get("intents", []) if identity(item) == key]
            require(len(matches) == 1 and matches[0].get("intentDigest") == owner["intentDigest"]
                    and matches[0].get("lifecycle", {}).get("responsibility_id") == owner["responsibilityId"]
                    and matches[0]["lifecycle"].get("source_bucket_incarnation_id") == owner["bucketIncarnationId"]
                    and matches[0]["lifecycle"].get("state") == {"state": "active"}
                    and matches[0]["lifecycle"].get("last_operator_acceptance") is None,
                    "queue failure retired, held, operator-accepted or altered the exact durable responsibility")
        observed[str(key)] = {"owners": len(bindings[key]), "originalResponsibilityRetained": True}
    return observed


def mrf_full_events(events, nodes, after):
    return [{"node": node, "event": row} for node in nodes for row in events.get(node, [])
            if row.get("target") == "rustfs::heal::manager" and row.get("event") == "heal_queue_admission"
            and row.get("source") == "mrf" and row.get("priority") == "Normal"
            and row.get("result") == "rejected_full" and row.get("queue_capacity") == CAPACITY
            and row.get("queue_len") == CAPACITY and isinstance(row.get("request_id"), str) and row["request_id"]
            and event_time(row) is not None and event_time(row) >= after]


class Pressure:
    def __init__(self, probe, fleet, binding, active):
        self.probe, self.fleet, self.binding, self.active = probe, fleet, binding, active
        self.requests, self.samples = 0, []
        self.baseline = {node: fleet.record("resource-baseline", {"node": node, **fleet.remote(node, "process-metrics")})
                         for node in fleet.nodes}

    def call(self, path):
        self.requests += 1
        require(self.requests <= MAX_ADMIN_REQUESTS, "pressure exceeded its pre-registered request ceiling")
        begin = time.monotonic_ns()
        observed = self.fleet.record("bounded-pressure-request", self.probe.admin(path, [],
            json.dumps({**BASE_OPTIONS, "recursive": True}).encode()))
        self.fleet.record("bounded-admission-latency", {"path": path, "requestNumber": self.requests,
            "beginMonotonicNs": begin, "endMonotonicNs": time.monotonic_ns(),
            "httpStatus": observed.get("httpStatus"), "errorsIncludedInRequestCount": True})
        return observed

    def admit(self, path):
        observed = self.call(path)
        token = (observed.get("response") or {}).get("clientToken") if isinstance(observed.get("response"), dict) else None
        require(observed.get("httpStatus") == 200 and isinstance(token, str) and token, "pressure request lacks an accepted token")
        self.active.append((path, token))
        return token

    def sample(self):
        observed = self.fleet.record("actual-queue-sample", self.probe.admin("/rustfs/admin/v3/background-heal/status", [], b""))
        require(observed.get("httpStatus") == 200, "pressure status failed")
        operations = queue_snapshot(observed.get("response") or {}, len(self.fleet.nodes))
        samples = {node: self.fleet.record("resource-curve", {"node": node, **self.fleet.remote(node, "process-metrics")})
                   for node in self.fleet.nodes}
        self.fleet.record("bounded-resource-proof", resources(samples, self.baseline))
        self.samples.append({"monotonicNs": time.monotonic_ns(), "operations": operations})
        return operations


def run_topology(args, topology, nodes, **unused):
    directory = args.evidence / topology
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="queue-pressure", resource_profile="cursor")
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] == REFERENCE["candidateCommit"], "unsupported candidate queue contract")
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    fleet.record("MRF-contract", verify_reference(candidate))
    prefix = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    fleet.snapshot_buckets = [prefix, *[f"{prefix}-queue-{index}" for index in range(len(COUNTS))], prefix + "-durable"]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": prefix,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": sum(COUNTS) + 16, "maxInventoryBytes": (sum(COUNTS) + 16) * SIZE}
    fleet.record("binding", binding)
    fleet.record("frozen-pressure-budget", {"source": REFERENCE, "runtime": runtime_config("queue-pressure"),
        "bucketObjectCounts": list(COUNTS), "objectBytes": SIZE, "pressureSeconds": PRESSURE_SECONDS,
        "drainSeconds": DRAIN_SECONDS, "maxAdminRequests": MAX_ADMIN_REQUESTS, "resourceBounds": RESOURCE_BOUNDS,
        "durableDegradedWrites": 3, "maxConcurrentClients": 1, "fourSourceBranches": list(BRANCHES)})
    require(set(select(CASE_ID)[0]["variants"]) == set(BRANCHES), "source queue branches changed")
    probe, active, scopes = Probe(binding), [], []
    result = {"topology": topology, "verdict": "RUNNING", "cleanup": "PENDING", "branches": {}}
    try:
        fleet.deploy(args.binary_sha, prefix, access, secret)
        for index, count in enumerate(COUNTS):
            bucket = f"{prefix}-queue-{index}"
            recorder = Recorder(probe, directory / f"fixture-{index}")
            expected = seed(recorder, bucket, "DX", count, [SIZE])
            baseline = fleet.record("full-baseline", probe.inventory(bucket, unversioned_current_tags=True))
            fleet.record("ACK-full-manifest", {"bucket": bucket, "expected": expected, **verify_seed(expected, baseline)})
            scopes.append({"bucket": bucket, "expected": expected, "baseline": baseline,
                "path": "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/objects/"})
        durable_bucket = prefix + "-durable"
        durable_recorder = Recorder(probe, directory / "durable-fixture")
        durable_expected = seed(durable_recorder, durable_bucket, "DB", 4, [SIZE])
        durable_baseline = fleet.record("durable-baseline", probe.inventory(durable_bucket, unversioned_current_tags=True))
        verify_seed(durable_expected, durable_baseline)
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        fleet.record("topology-proof", validate_topology(original, topology))
        targets = []
        for index, scope in enumerate(scopes):
            scope["cohorts"] = map_cohorts(original_subset(original, scope["bucket"]), scope["bucket"], scope["expected"], count=COUNTS[index])
            verify_part_manifest(fleet, scope["cohorts"], "baseline-part-manifest", count=COUNTS[index])
            target = scope["cohorts"][-1]
            scope["missing"] = {(target["key"], target["versionId"])}
            targets.append(target)
            params = {"disk": target["disk"], "relativePath": target["relativePath"], "kind": "missing",
                "faultId": bounded_fault_id("I04", "queue", index), "expectedSha256": target["sha256"]}
            fleet.faults.append((target["node"], params))
            scope["fault"] = fleet.record("exact-missing-part", {"node": target["node"], **fleet.remote(target["node"], "apply-fault", **params)})
        faulted = fleet.snapshot("physical-fault", quiescent=True)
        fleet.record("fault-proof", validate_missing_batch(original, faulted, targets, [scope["fault"] for scope in scopes]))
        fleet.start()
        require(physical_content(faulted) == physical_content(fleet.snapshot("physical-before-pressure")),
                "ATTRIBUTION_NOT_OBSERVED: an owned fault changed before pressure admission")
        initial_checkpoints = read_checkpoints(fleet, nodes, "MRF-empty-baseline")
        require(all(not item["intents"] for item in initial_checkpoints.values()), "pressure fixture has unrelated durable responsibility")
        window = JournalWindow(fleet, "pressure-admission", event_targets={"rustfs::heal::manager", "rustfs::heal::task"})
        pressure = Pressure(probe, fleet, binding, active)
        first = scopes[0]
        first["token"] = pressure.admit(first["path"])
        deadline = time.monotonic() + PRESSURE_SECONDS
        while True:
            running = status(probe, fleet, first["path"], first["token"], "pressure-running-window")
            if active_window(running, COUNTS[0]): break
            require(running.get("summary") == "running" and time.monotonic() < deadline,
                    "NOT_OBSERVED: no partial running task for queue pressure")
            time.sleep(0.05)
        merged = pressure.call(first["path"])
        require(merged.get("httpStatus") == 200 and merged.get("response", {}).get("clientToken") == first["token"],
                "duplicate admission created another task instead of merging its original token")
        dropped = rejection(pressure.call(first["path"] + "0001.bin"), admission="dropped",
                            reason="overlapping_paths", code="OperationAborted", http=409)
        require(active_window(status(probe, fleet, first["path"], first["token"], "after-merge-and-drop"), COUNTS[0]),
                "NOT_OBSERVED: Merged/Dropped were not bracketed by the same active task")
        for scope in scopes[1:3]:
            scope["admittedMonotonicNs"] = time.monotonic_ns()
            scope["token"] = pressure.admit(scope["path"])
        require(len({scope["token"] for scope in scopes[:3]}) == 3, "independent buckets reused a task identity")
        rejected = rejection(pressure.call(scopes[3]["path"]), admission="full", reason="none", code="SlowDown", http=503)
        require(pressure.sample()["queueLength"] == CAPACITY, "NOT_OBSERVED: Full was not witnessed with the actual two-slot queue")
        # Deny only the dedicated durable bucket on one owned drive. System
        # metadata must remain readable/writable for the real MRF journal.
        gate_node, gate_disk = nodes[0], 0
        owner = fleet.record("original-gated-process", fleet.remote(gate_node, "process"))
        fleet.drive_gates.add(gate_node)
        fleet.record("permission-gate-armed", fleet.remote(gate_node, "drive-gate-arm-live", disk=gate_disk, expected=owner))
        fleet.record("permission-gate-activated", fleet.remote(gate_node, "drive-gate-activate"))
        gate_proof = fleet.record("actual-service-refusal", fleet.remote(gate_node, "drive-gate-probe"))
        validate_durable_gate(gate_proof, run_id, durable_bucket)
        earliest_ms, ack_start = int(time.time() * 1000) - 5000, datetime.now(timezone.utc)
        durable_expected, obligations, _ = acknowledge(durable_recorder, durable_bucket, "offline-put", durable_expected, fixture="DB")
        fleet.record("durable-ACK-ledger", {"expected": durable_expected, "obligations": obligations})
        deadline = time.monotonic() + PRESSURE_SECONDS
        while True:
            records = read_checkpoints(fleet, nodes, "committed-ACKs-during-Full")
            try:
                bindings = pending_bindings(records, obligations, earliest_ms)
            except ValueError as exc:
                require(str(exc).startswith("NOT_OBSERVED:") and time.monotonic() < deadline,
                        "durable pressure persistence failed: " + str(exc))
                time.sleep(0.5)
                continue
            all_intents = {identity(item) for value in records.values() for item in value["intents"]}
            require(all_intents == {identity(item) for item in obligations}, "MRF Full isolation contains foreign or unexplained durable ACKs")
            full = mrf_full_events(window.capture(), nodes, ack_start)
            if full: break
            require(time.monotonic() < deadline, "NOT_OBSERVED: the degraded-write MRF source never encountered real Full")
            time.sleep(0.2)
        # Capture again AFTER the rejection event, before rejoining. Admission
        # alone cannot discharge any of the three exact durable responsibilities.
        fleet.record("Full-durable-retention-proof", retained(read_checkpoints(fleet, nodes, "after-MRF-Full"), obligations, bindings))
        pressure.sample()
        rejoin_time = datetime.now(timezone.utc)
        released = fleet.record("same-process-drive-rejoin", fleet.remote(gate_node, "drive-gate-release-live"))
        require(released.get("sameOriginalProcess") is True and released.get("serviceRestarted") is False,
                "pressure release restarted the scheduler or replaced its original process")
        fleet.drive_gates.remove(gate_node)
        deadline, proofs, completed = time.monotonic() + DRAIN_SECONDS, {}, {}
        while True:
            operations = pressure.sample()
            if "token" not in scopes[3] and operations["queueLength"] < CAPACITY:
                retry = pressure.call(scopes[3]["path"])
                if retry.get("httpStatus") == 200:
                    token = retry.get("response", {}).get("clientToken")
                    require(isinstance(token, str) and token and token not in {scope["token"] for scope in scopes[:3]},
                            "previously Full bucket lost its independent admitted token")
                    scopes[3].update(token=token, admittedMonotonicNs=time.monotonic_ns())
                    active.append((scopes[3]["path"], token))
                else:
                    rejection(retry, admission="full", reason="none", code="SlowDown", http=503)
            for scope in scopes:
                if "token" not in scope or scope["bucket"] in completed: continue
                current = status(probe, fleet, scope["path"], scope["token"], "per-bucket-drain")
                if current.get("summary") != "running":
                    completed[scope["bucket"]] = terminal(current, scope["bucket"], scope["expected"], scope["missing"])
                    completed[scope["bucket"]]["completionMonotonicNs"] = time.monotonic_ns()
                    active.remove((scope["path"], scope["token"]))
            events = window.capture()
            for obligation in obligations:
                key = identity(obligation)
                if key in proofs: continue
                for task in source_tasks(events, obligation, bindings[key], rejoin_time):
                    endpoint = "node" + str(nodes.index(task["node"]) + 1)
                    path = "/rustfs/admin/v3/heal/" + quote(durable_bucket, safe="") + "/" + quote(obligation["key"], safe="/")
                    current = status(Probe(binding, endpoint), fleet, path, task["taskId"], "MRF-exact-ACK-completion")
                    proofs[key] = {"task": task, "receipt": verified_receipt(current, obligation, bindings[key])}
                    break
            if len(completed) == len(scopes) and len(proofs) == len(obligations):
                try:
                    retired = retirement(read_checkpoints(fleet, nodes, "exact-retirement-after-proof"), obligations, bindings)
                    break
                except ValueError as exc:
                    require(str(exc).startswith("NOT_OBSERVED:") and time.monotonic() < deadline,
                            "durable queue retirement failed: " + str(exc))
            require(time.monotonic() < deadline, "queue drain exceeded its pre-registered cross-bucket completion budget")
            time.sleep(2)
        idle_fleet(fleet, binding)
        fleet.stop()
        repaired = fleet.snapshot("physical-product-result-before-GET", quiescent=True)
        validate_repaired_physical(original_subset(original, None, excluded=durable_bucket),
            original_subset(repaired, None, excluded=durable_bucket), targets[0], repair_targets=targets)
        for index, scope in enumerate(scopes):
            fleet.record("complete-external-product-proof", verify_part_manifest(fleet, scope["cohorts"], "repaired-part-manifest", count=COUNTS[index]))
        for entry in durable_expected:
            target = map_target(original_subset(repaired, durable_bucket), durable_bucket, entry["key"], "data", 1, entry["versionId"])
            fleet.record("all-degraded-ACK-parts", verify_cohort(fleet, target, "durable-all-member-streaming"))
        retirement(read_checkpoints(fleet, nodes, "quiescent-retirement", quiescent=True), obligations, bindings)
        fleet.start()
        for endpoint in binding["endpoints"]:
            peer = Probe(binding, endpoint)
            for scope in scopes:
                inventory = fleet.record("all-peer-S3", {"endpoint": endpoint, "bucket": scope["bucket"],
                    **peer.inventory(scope["bucket"], unversioned_current_tags=True)})
                fleet.record("all-peer-full-manifest", {**verify_seed(scope["expected"], inventory),
                    **compare_inventories(scope["baseline"], inventory)})
            inventory = fleet.record("all-peer-durable-S3", {"endpoint": endpoint,
                **peer.inventory(durable_bucket, unversioned_current_tags=True)})
            fleet.record("all-peer-durable-manifest", verify_seed(durable_expected, inventory))
        fleet.stop()
        require(physical_content(repaired) == physical_content(fleet.snapshot("physical-after-S3", quiescent=True)),
                "verification GET supplied repairs absent from the prior physical result")
        result.update(verdict="PASS", branches={
            "full-queue": {"verdict": "PASS", "adminRejection": rejected, "MRFRejections": full,
                           "durableRetirement": retired, "verifiedSourceACKs": len(proofs)},
            "dropped-admission": {"verdict": "PASS", "typedRejection": dropped, "independentRepairCount": 0},
            "merged-duplicates": {"verdict": "PASS", "originalTaskId": first["token"], "independentRepairCount": 0},
            "cross-bucket-fairness": {"verdict": "PASS", "completedBuckets": completed, "drainBudgetSeconds": DRAIN_SECONDS,
                "requestCount": pressure.requests, "boundedQueueAndResourceSamples": pressure.samples}},
            completePhysicalAndS3ManifestsVerified=True, backupRestorationUsedAsRepair=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            if fleet.reserved:
                fleet.stop()
                fleet.snapshot("physical-failure", quiescent=True)
                read_checkpoints(fleet, nodes, "retained-failure-responsibility", quiescent=True)
        except Exception as evidence_error:
            result["failureEvidenceError"] = str(evidence_error)
    finally:
        for path, token in active:
            try:
                fleet.record("stop-unresolved-owned-task", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc:
                fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("topology-result", result)
    return result


def original_subset(snapshot, bucket, *, excluded=None):
    return {node: {**value, "files": [entry for entry in value["files"]
            if (bucket is None or entry["path"].startswith(bucket + "/"))
            and (excluded is None or not entry["path"].startswith(excluded + "/"))]}
            for node, value in snapshot.items()}


if __name__ == "__main__":
    try:
        generic_main(case_id=CASE_ID, branches=BRANCHES, runner=run_topology)
    except Exception as exc:
        print(f"Heal queue-pressure validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
