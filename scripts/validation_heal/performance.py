#!/usr/bin/env python3
"""J06: three fixed ABBA groups with concurrent ACK traffic and real repair."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
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
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .cursor import OBJECTS, SIZE, map_cohorts, verify_all_parts, validate_missing_batch, complete_outcome
    from .shards import validate_repaired_physical
    from .stop import idle_fleet, status
    from .fleet_node import PROCESS_FIELDS
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from cursor import OBJECTS, SIZE, map_cohorts, verify_all_parts, validate_missing_batch, complete_outcome
    from shards import validate_repaired_physical
    from stop import idle_fleet, status
    from fleet_node import PROCESS_FIELDS


CASE_ID = "heal.performance-abba"
BRANCHES = ("abba-repeat-1", "abba-repeat-2", "abba-repeat-3")
# These are acceptance gates chosen before deployment, not results derived
# from the measured candidate. Changing one requires a new immutable run.
PROTOCOL = {"groups": 3, "order": ["A", "B", "B", "A"], "secondsPerPhase": 30,
    "requestsPerSecond": 4, "concurrency": 2, "readWriteRatio": "1:1",
    "loadObjects": 16, "loadObjectBytes": 65536, "warmupRequests": 16,
    "repairObjects": OBJECTS, "repairObjectBytes": SIZE, "missingIndices": list(range(0, OBJECTS, 64)),
    "phaseAdmissionGraceSeconds": 1, "minimumSamplesPerOperation": 50,
    "maxErrorRate": 0, "getP95Ms": 5000, "getP99Ms": 10000,
    "putP95Ms": 5000, "putP99Ms": 10000, "maxPairedLatencyRatio": 3,
    "minimumRunningOverlapSeconds": 1, "minimumBracketedCalls": 4,
    "minimumRepairMiBPerSecond": .001, "maxProcessCpuCores": 2.5,
    "maxProcessRssBytes": 2800 * 1024**2, "maxProcessFDs": 4096,
    "metricsIntervalSeconds": 2, "repairDeadlineSeconds": 300,
    "cachePolicy": "same original four roots; restart every phase; fixed warmup; no host cache eviction",
    "latencyScope": "collector scheduled admission through complete SDK response/body; includes queue delay and errors",
    "retryPolicy": "no retries; every scheduled GET and PUT remains in the denominator",
    "hardwareScope": "same candidate, rc, hosts, mounts and original disk UUIDs for all twelve phases"}


def schedule(protocol=PROTOCOL):
    require(protocol == PROTOCOL, "J06 requires the immutable preregistered protocol")
    return [{"group": group, "phase": index, "mode": mode, "branch": BRANCHES[group - 1]}
            for group in range(1, 4) for index, mode in enumerate(protocol["order"], 1)]


def percentile(values, fraction):
    require(values and all(type(value) in {int, float} and math.isfinite(value) and value >= 0 for value in values)
            and fraction in {.95, .99}, "invalid performance sample or percentile")
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def load_body(bucket, index):
    return payload(f"{bucket}/load/{index:02d}/0", PROTOCOL["loadObjectBytes"])


def load_manifest(bucket):
    return [{"key": f"load/{index:02d}.bin", "versionId": "null", "deleteMarker": False,
             "isLatest": True, "payload": {"size": PROTOCOL["loadObjectBytes"],
             "sha256": hashlib.sha256(load_body(bucket, index)).hexdigest()}}
            for index in range(PROTOCOL["loadObjects"])]


def prepare_load(recorder, bucket):
    recorder.call("create_bucket", {"Bucket": bucket})
    for index, entry in enumerate(load_manifest(bucket)):
        response = recorder.call("put_object", {"Bucket": bucket, "Key": entry["key"]}, load_body(bucket, index))
        require(response.get("VersionId") in {None, "null"}, "performance load bucket unexpectedly versioned")


def repair_view(snapshot, bucket):
    return {node: {**value, "files": [entry for entry in value["files"] if entry["path"].startswith(bucket + "/")]}
            for node, value in snapshot.items()}


def same_hardware(original, actual):
    require(set(original) == set(actual), "ABBA hardware omitted or added a node")
    for node in original:
        require(original[node]["formats"] == actual[node]["formats"]
                and original[node]["hostname"] == actual[node]["hostname"]
                and original[node]["binarySha256"] == actual[node]["binarySha256"],
                "ABBA candidate, host or disk identities changed")
    return {"sameHostsCandidateAndDiskUUIDs": True}


def request(probe, bucket, sequence, epoch, output):
    operation = "get_object" if sequence % 2 == 0 else "put_object"
    index = (sequence // 2) % PROTOCOL["loadObjects"]
    body = load_body(bucket, index)
    scheduled = epoch + sequence / PROTOCOL["requestsPerSecond"]
    remaining = scheduled - time.monotonic()
    if remaining > 0: time.sleep(remaining)
    params = {"Bucket": bucket, "Key": f"load/{index:02d}.bin"}
    prepared = {"sequence": sequence, "operation": operation, "params": params,
                "scheduledMonotonicNs": int(scheduled * 1e9),
                "expectedPayload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}}
    path = output / f"{sequence:05d}-request.json"
    save(path, prepared)
    if time.monotonic() > epoch + PROTOCOL["secondsPerPhase"] + PROTOCOL["phaseAdmissionGraceSeconds"]:
        observed = {"acknowledged": False, "resultUnknown": False, "clientCallInvoked": False,
                    "error": {"type": "AdmissionDeadline", "message": "scheduled sample missed the phase admission deadline"}}
    else:
        observed = probe.s3(operation, params, body if operation == "put_object" else None)
    finished = time.monotonic_ns()
    code = (observed.get("response") or {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
    success = observed.get("acknowledged") is True and observed.get("resultUnknown") is False and code == 200 \
        and not observed.get("clientCleanupError")
    if operation == "get_object": success = success and observed.get("payload") == prepared["expectedPayload"]
    else: success = success and (observed.get("response") or {}).get("VersionId") in {None, "null"}
    result = {**prepared, "requestSha256": hashlib.sha256(path.read_bytes()).hexdigest(), "observation": observed,
              "finishedMonotonicNs": finished, "latencyMs": (finished - prepared["scheduledMonotonicNs"]) / 1e6,
              "verdict": "PASS" if success else "FAIL"}
    save(output / f"{sequence:05d}-ack.json", result)
    return result


def summarize(rows):
    count = PROTOCOL["secondsPerPhase"] * PROTOCOL["requestsPerSecond"]
    require(len(rows) == count and {row["sequence"] for row in rows} == set(range(count)),
            "scheduled requests were omitted, duplicated or filtered")
    statistics = {"scheduledRequests": count, "failedRequests": sum(row["verdict"] != "PASS" for row in rows)}
    statistics["errorRate"] = statistics["failedRequests"] / count
    for operation, label in (("get_object", "get"), ("put_object", "put")):
        samples = [row["latencyMs"] for row in rows if row["operation"] == operation]
        require(len(samples) == count // 2 >= PROTOCOL["minimumSamplesPerOperation"], "GET/PUT sample count is incomplete")
        statistics[label] = {"requests": len(samples), "p95Ms": percentile(samples, .95), "p99Ms": percentile(samples, .99)}
    return statistics


def resource_delta(before, after):
    require(set(before) == set(after) and len(before) == 4, "performance metrics require all four actual peers")
    result = {}
    for node, start in before.items():
        end = after[node]
        require(start.get("readOnly") is True and end.get("readOnly") is True
                and all(start["identity"].get(name) == end["identity"].get(name) for name in PROCESS_FIELDS)
                and start["clockDomain"] == end["clockDomain"] == start["identity"]["bootId"]
                and type(start["clockTicksPerSecond"]) is int and start["clockTicksPerSecond"] > 0
                and start["clockTicksPerSecond"] == end["clockTicksPerSecond"], "resource counters crossed a process or clock identity")
        elapsed = (end["monotonicNs"] - start["monotonicNs"]) / 1e9
        require(elapsed > 0 and end["cpuTicks"] >= start["cpuTicks"], "resource clock/CPU counters went backwards")
        io = {}
        for name in ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes"):
            require(type(start["io"].get(name)) is int and type(end["io"].get(name)) is int
                    and end["io"][name] >= start["io"][name], "process IO counters went backwards or were omitted")
            io[name] = end["io"][name] - start["io"][name]
        result[node] = {"elapsedSeconds": elapsed,
            "cpuCores": (end["cpuTicks"] - start["cpuTicks"]) / start["clockTicksPerSecond"] / elapsed,
            "maxSampledRssBytes": max(start["rssBytes"], end["rssBytes"]),
            "maxSampledFDs": max(start["fileDescriptors"], end["fileDescriptors"]), "ioDelta": io}
    return result


def overlap(rows, windows):
    intervals = []
    for left, right in zip(windows, windows[1:]):
        lc, rc = left["response"].get("outcome", {}).get("counters", {}), right["response"].get("outcome", {}).get("counters", {})
        for counter in (lc, rc):
            require(all(type(counter.get(name)) is int and counter[name] >= 0 for name in
                        ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures")),
                    "active performance task lacks typed counters")
            require(all(counter[name] == 0 for name in ("failed", "unknown", "attemptFailures"))
                    and counter.get("overflowed") is False, "performance task has failed or unknown responsibility")
        require(rc["processed"] >= lc["processed"] and rc["healed"] >= lc["healed"], "same performance task progress went backwards")
        if all(row["response"].get("summary") == "running" and row["response"].get("outcome", {}).get("execution", {}).get("state") == "running"
               for row in (left, right)) and 0 < lc["processed"] < rc["processed"] < OBJECTS:
            start, end = left["callFinishedMonotonicNs"], right["callStartedMonotonicNs"]
            if end > start: intervals.append((start, end))
    calls = [row for row in rows if row["observation"].get("clientCallInvoked") is True and any(
        start <= row["observation"]["callStartedMonotonicNs"] <= row["observation"]["callFinishedMonotonicNs"] <= end
        for start, end in intervals)]
    duration = sum((end - start) / 1e9 for start, end in intervals)
    require(duration >= PROTOCOL["minimumRunningOverlapSeconds"] and len(calls) >= PROTOCOL["minimumBracketedCalls"]
            and {row["operation"] for row in calls} == {"get_object", "put_object"},
            "NOT_OBSERVED: workload GET/PUT did not overlap independently witnessed nonzero partial repair")
    return {"bracketedRunningSeconds": duration, "bracketedCalls": len(calls), "bothOperationsDuringRealRepair": True}


def acceptance(phases):
    require([{name: phase[name] for name in ("group", "phase", "mode", "branch")} for phase in phases] == schedule(),
            "ABBA phases were omitted, reordered, repeated or selected after measurement")
    ratios = []
    for phase in phases:
        require(phase["statistics"]["errorRate"] <= PROTOCOL["maxErrorRate"], "performance request error-rate gate failed")
        for operation in ("get", "put"):
            for quantile in ("95", "99"):
                value = phase["statistics"][operation]["p" + quantile + "Ms"]
                require(math.isfinite(value) and 0 <= value <= PROTOCOL[operation + "P" + quantile + "Ms"],
                        "absolute performance latency gate failed")
        for node in phase["resources"].values():
            require(node["cpuCores"] <= PROTOCOL["maxProcessCpuCores"]
                    and node["maxSampledRssBytes"] <= PROTOCOL["maxProcessRssBytes"]
                    and node["maxSampledFDs"] <= PROTOCOL["maxProcessFDs"], "performance resource gate failed")
        if phase["mode"] == "B":
            require(phase["overlap"]["bothOperationsDuringRealRepair"] is True and phase["repair"]["physicalVerified"] is True
                    and phase["repair"]["MiBPerSecond"] >= PROTOCOL["minimumRepairMiBPerSecond"],
                    "performance phase did not prove actual physical repair throughput")
        else:
            require(phase["noHeal"] is True and phase["repair"] is None, "baseline phase contains a heal task")
    for group in range(1, 4):
        selected = [phase for phase in phases if phase["group"] == group]
        for operation in ("get", "put"):
            for quantile in ("95", "99"):
                name = "p" + quantile + "Ms"
                a = sum(phase["statistics"][operation][name] for phase in selected if phase["mode"] == "A") / 2
                b = sum(phase["statistics"][operation][name] for phase in selected if phase["mode"] == "B") / 2
                require(a > 0 and b / a <= PROTOCOL["maxPairedLatencyRatio"], "paired ABBA latency regression gate failed")
                ratios.append({"group": group, "operation": operation, "percentile": name, "aMeanMs": a,
                               "bMeanMs": b, "differenceMs": b - a, "ratio": b / a})
    return {"allTwelvePhasesAccepted": True, "pairedComparisons": ratios,
            "thresholdsFrozenBeforeDeployment": True, "functionalChecksAreSeparate": True}


def sample_metrics(fleet, label):
    return fleet.record(label, {node: fleet.remote(node, "process-metrics") for node in fleet.nodes})


def run_phase(spec, fleet, probe, binding, bucket, expected, load_bucket, hardware, cohorts, active, directory):
    label = f"g{spec['group']}-p{spec['phase']}-{spec['mode']}"
    output = directory / label
    output.mkdir()
    fleet.record("phase-protocol-" + label, {**spec, "protocol": PROTOCOL})
    idle_fleet(fleet, binding)
    fleet.stop()
    original = repair_view(fleet.snapshot("physical-phase-baseline-" + label, quiescent=True), bucket)
    same_hardware(hardware, original)
    targets = [cohorts[index] for index in PROTOCOL["missingIndices"]]
    registrations, receipts = [], []
    if spec["mode"] == "B":
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "relativePath": target["relativePath"], "kind": "missing",
                "faultId": bounded_fault_id("J06", label, index), "expectedSha256": target["sha256"]}
            fleet.faults.append((target["node"], params))
            registrations.append((target["node"], params))
            receipts.append(fleet.record("fault-" + label, {"node": target["node"], **fleet.remote(target["node"], "apply-fault", **params)}))
        faulted = repair_view(fleet.snapshot("physical-phase-fault-" + label, quiescent=True), bucket)
        fleet.record("fault-proof-" + label, validate_missing_batch(original, faulted, targets, receipts))
    else: faulted = original
    fleet.start()
    before_trigger = repair_view(fleet.snapshot("physical-phase-before-trigger-" + label), bucket)
    require(physical_content(faulted) == physical_content(before_trigger), "ATTRIBUTION_NOT_OBSERVED: ABBA fault changed before trigger")
    for index in range(PROTOCOL["warmupRequests"]):
        observed = fleet.record("warmup-" + label, probe.s3("get_object", {"Bucket": load_bucket, "Key": f"load/{index:02d}.bin"}))
        require(observed.get("acknowledged") is True and observed.get("payload") == load_manifest(load_bucket)[index]["payload"], "fixed warmup failed")
    initial_metrics = sample_metrics(fleet, "metrics-before-" + label)
    path, token, windows, terminal = "/rustfs/admin/v3/heal/" + quote(bucket, safe=""), None, [], None
    repair_started = None
    if spec["mode"] == "B":
        repair_started = time.monotonic()
        start = fleet.record("trigger-" + label, probe.admin(path, [], json.dumps({**BASE_OPTIONS, "recursive": True}).encode()))
        require(start.get("httpStatus") == 200 and isinstance(start.get("response"), dict), "ABBA repair admission failed")
        token = start["response"].get("clientToken")
        require(isinstance(token, str) and token, "ABBA repair omitted task identity")
        active.append((path, token))
    else: idle_fleet(fleet, binding)
    epoch = time.monotonic()
    samples, metric_rows = [], [initial_metrics]
    next_metric = epoch + PROTOCOL["metricsIntervalSeconds"]
    repair_finished = None
    with ThreadPoolExecutor(max_workers=PROTOCOL["concurrency"]) as workers:
        futures = [workers.submit(request, probe, load_bucket, sequence, epoch, output)
                   for sequence in range(PROTOCOL["secondsPerPhase"] * PROTOCOL["requestsPerSecond"])]
        while not all(future.done() for future in futures) or token and terminal is None:
            if token and terminal is None:
                observed = fleet.record("performance-running-" + label, probe.admin(path, [("clientToken", token)], b""))
                require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict), "ABBA task status failed")
                windows.append(observed)
                if observed["response"].get("summary") != "running":
                    terminal, repair_finished = observed["response"], time.monotonic()
                    active.remove((path, token))
                require(time.monotonic() - repair_started <= PROTOCOL["repairDeadlineSeconds"], "ABBA task exceeded frozen deadline")
            if time.monotonic() >= next_metric:
                metric_rows.append(sample_metrics(fleet, "metrics-during-" + label))
                if spec["mode"] == "A": idle_fleet(fleet, binding)
                next_metric = time.monotonic() + PROTOCOL["metricsIntervalSeconds"]
            time.sleep(.2)
        samples = [future.result() for future in futures]
    remaining = epoch + PROTOCOL["secondsPerPhase"] - time.monotonic()
    if remaining > 0: time.sleep(remaining)
    metric_rows.append(sample_metrics(fleet, "metrics-after-" + label))
    resources = resource_delta(metric_rows[0], metric_rows[-1])
    for left, right in zip(metric_rows, metric_rows[1:]):
        delta = resource_delta(left, right)
        for node in resources:
            for name in ("cpuCores", "maxSampledRssBytes", "maxSampledFDs"):
                resources[node][name] = max(resources[node][name], delta[node][name])
    statistics = summarize(samples)
    proof = {**spec, "statistics": statistics, "resources": resources, "taskId": token, "repair": None, "noHeal": token is None}
    if token:
        repaired_pairs = {(target["key"], "null") for target in targets}
        proof["responsibility"] = complete_outcome(terminal, bucket, expected, repaired_pairs)
        proof["overlap"] = overlap(samples, windows)
        require(repair_finished > repair_started, "repair throughput has no measured positive duration")
    idle_fleet(fleet, binding)
    fleet.stop()
    repaired = repair_view(fleet.snapshot("physical-phase-after-" + label, quiescent=True), bucket)
    same_hardware(hardware, repaired)
    if token:
        fleet.record("physical-proof-" + label, validate_repaired_physical(original, repaired, targets[0], repair_targets=targets))
        fleet.record("streaming-proof-" + label, verify_all_parts(fleet, cohorts, "part-batch-" + label))
        byte_count = sum(target["size"] for target in targets)
        proof["repair"] = {"physicalVerified": True, "originalShardBytes": byte_count,
            "seconds": repair_finished - repair_started, "MiBPerSecond": byte_count / 1024**2 / (repair_finished - repair_started),
            "scope": "actual reconstructed target shard bytes; not logical S3 bytes"}
        for registration in registrations: fleet.faults.remove(registration)
    else:
        require(physical_content(original) == physical_content(repaired), "no-heal baseline changed the repair fixture")
    fleet.start()
    fleet.record("load-functional-after-" + label, verify_seed(load_manifest(load_bucket), probe.inventory(load_bucket, unversioned_current_tags=True)))
    if token:
        # A complete outcome was already tied to the original token before
        # physical verification. Later GETs cannot create its repair evidence.
        require(status(probe, fleet, path, token, "retained-terminal-" + label).get("outcome") == terminal.get("outcome"),
                "physical/workload checks changed the retained original outcome")
    fleet.record("phase-result-" + label, proof)
    return proof


def run(args, nodes):
    directory = args.evidence / "H2"
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h2"
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    load_bucket = bucket + "-load"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, resource_profile="cursor")
    fleet.snapshot_buckets = [bucket, load_bucket]
    candidate = json.loads(args.candidate.read_text())
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, 5)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 1100, "maxInventoryBytes": 2 * 1024**3}
    fleet.record("pre-registered-performance-protocol", copy.deepcopy(PROTOCOL))
    fleet.record("binding", binding)
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "performance executor omitted a source repetition")
    probe, active = Probe(binding), []
    result = {"topology": "H2", "verdict": "RUNNING", "branches": {}, "phases": [], "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), "H2"))
        expected = seed(Recorder(probe, directory / "repair-fixture"), bucket, "DX", OBJECTS, [SIZE])
        prepare_load(Recorder(probe, directory / "load-fixture"), load_bucket)
        baseline = fleet.record("s3-repair-baseline", probe.inventory(bucket, unversioned_current_tags=True, page_size=127))
        fleet.record("repair-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        load_baseline = fleet.record("s3-load-baseline", probe.inventory(load_bucket, unversioned_current_tags=True))
        fleet.record("load-ACK-manifest", verify_seed(load_manifest(load_bucket), load_baseline))
        fleet.stop()
        hardware = repair_view(fleet.snapshot("physical-baseline", quiescent=True), bucket)
        cohorts = map_cohorts(hardware, bucket, expected)
        fleet.record("complete-streaming-baseline", verify_all_parts(fleet, cohorts, "baseline-part-batch"))
        fleet.start()
        for spec in schedule():
            result["phases"].append(run_phase(spec, fleet, probe, binding, bucket, expected, load_bucket, hardware, cohorts, active, directory))
        result["performanceAcceptance"] = acceptance(result["phases"])
        for endpoint in binding["endpoints"]:
            peer = Probe(binding, endpoint)
            actual = fleet.record("complete-repair-S3-after", {"endpoint": endpoint, **peer.inventory(bucket, unversioned_current_tags=True, page_size=127)})
            fleet.record("complete-repair-S3-proof", {"endpoint": endpoint, **verify_seed(expected, actual), **compare_inventories(baseline, actual)})
            load_actual = fleet.record("complete-load-S3-after", {"endpoint": endpoint, **peer.inventory(load_bucket, unversioned_current_tags=True)})
            fleet.record("complete-load-S3-proof", {"endpoint": endpoint, **verify_seed(load_manifest(load_bucket), load_actual),
                **compare_inventories(load_baseline, load_actual)})
        for branch in BRANCHES:
            result["branches"][branch] = {"verdict": "PASS", "phases": [row for row in result["phases"] if row["branch"] == branch],
                "independentPerformanceAndFunctionalGates": True}
        result["verdict"] = "PASS"
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
        fleet.record("topology-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"Heal performance interrupted by signal {signum}")
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
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node)
            for node in nodes), "four current Heal nodes required")
    require(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")) and re.fullmatch(r"[0-9a-f]{64}", args.binary_sha),
            "immutable workflow and candidate identities required")
    result = run(args, nodes[:4])
    complete = result["verdict"] == "PASS" and result["cleanup"] == "PASS"
    save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": [result], "cleanup": result["cleanup"], "complete": complete})
    require(complete, "J06 requires all three full ABBA groups, independent performance/functional gates and cleanup")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal performance validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
