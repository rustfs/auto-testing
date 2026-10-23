#!/usr/bin/env python3
"""B08: independently witnessed write and distributed-lock quorum loss."""

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
from urllib import request
from urllib.error import URLError

try:
    from .heal_probe import Probe, payload, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .shards import (SIZES, files, map_target, run_task, validate_fault,
        validate_coordination_rejection, validate_coordination_rejection_logs,
        validate_repaired_physical, verify_cohort)
    from .versions import validate_version_receipts, verify_quorum_contract, verify_version_s3
    from .drive_gate import validate_unwritable
    from .quorum_boundary import LOCK_REFERENCE, lock_reference
    from .stop import idle_fleet
except ImportError:
    from heal_probe import Probe, payload, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import (SIZES, files, map_target, run_task, validate_fault,
        validate_coordination_rejection, validate_coordination_rejection_logs,
        validate_repaired_physical, verify_cohort)
    from versions import validate_version_receipts, verify_quorum_contract, verify_version_s3
    from drive_gate import validate_unwritable
    from quorum_boundary import LOCK_REFERENCE, lock_reference
    from stop import idle_fleet


CASE_ID = "heal.below-write-quorum"
BRANCHES = ("read-present-write-absent", "lock-quorum-absent")
REFUSALS = {"XMinioWriteQuorum", "XMinioLockTimeout", "InternalError", "ServiceUnavailable", "SlowDown", "RequestTimeout"}
LOCAL_LOCK_REASON = ("H1 uses the candidate's in-process local manager, not a distributed client quorum. "
                     "Losing its only process also removes the read endpoint; distributed peer-loss is applicable on H2 only.")
STORAGE_REFERENCE = {"candidateCommit": LOCK_REFERENCE["candidateCommit"],
    "sourceFiles": {"crates/ecstore/src/config/storageclass.rs":
        "e7c29544e2798c246e78d978f60f103bd6446eccd560c5c9b490316bca64bf9c"},
    "H2LockFixture": "explicit EC:8 only for the fresh run-owned sixteen-drive lock-loss branch",
    "WriteFixture": "all N readable authorities; independent read-only gates remove W even when R equals W"}

STORAGE_REFERENCES = {
    STORAGE_REFERENCE['candidateCommit']: STORAGE_REFERENCE,
    '1a3f038c90704f9ad9693eddcf7d861c9530f13c': {
        **STORAGE_REFERENCE, 'candidateCommit': '1a3f038c90704f9ad9693eddcf7d861c9530f13c'},
}


def source_reference(candidate_commit):
    locks = lock_reference(candidate_commit)
    require(candidate_commit in STORAGE_REFERENCES, "B08 storage boundary is not reviewed for this candidate")
    return {"lock": locks, "storage": copy.deepcopy(STORAGE_REFERENCES[candidate_commit])}


def plan_for(snapshot, target, branch, contract):
    require(branch in BRANCHES and contract.get("recognized") is True
            and contract.get("writeQuorumRule") == "data-plus-one-if-equal-parity", "unrecognized B08 contract")
    cohort = target["cohort"]
    geometry = target["logical"]["erasure"]
    read, parity = geometry["data_blocks"], geometry["parity_blocks"]
    write, total = read + int(read == parity), read + parity
    hosts = list(snapshot)
    require(len(hosts) in {1, 4} and total == len(cohort) == 4 * len(hosts)
            and len({(row["node"], row["disk"]) for row in cohort}) == total
            and all({row["disk"] for row in cohort if row["node"] == node} == set(range(4)) for node in hosts),
            "B08 requires actual full H1/H2 member identities")
    require(total - 1 >= read, "layout cannot retain R payload sources after the original missing part")
    lock = {"mode": "local" if len(hosts) == 1 else "distributed", "clients": len(hosts),
            "sharedQuorum": (len(hosts) + 1) // 2, "exclusiveQuorum": len(hosts) // 2 + 1}
    if branch == "lock-quorum-absent" and len(hosts) == 1:
        return {"applicable": False, "reason": LOCAL_LOCK_REASON, "readQuorum": read,
                "writeQuorum": write, "lock": lock}
    ordered = sorted(cohort, key=lambda row: (hosts.index(row["node"]), row["disk"]))
    if branch == "read-present-write-absent":
        # Include the precise missing target in the read-only set. All N
        # metadata authorities and lock clients remain readable/online.
        selected = [next(row for row in ordered if (row["node"], row["disk"]) == (target["node"], target["disk"]))]
        selected += [row for row in ordered if (row["node"], row["disk"]) != (target["node"], target["disk"])][:total - write]
        gates = {node: sorted(row["disk"] for row in selected if row["node"] == node) for node in hosts}
        gates = {node: disks for node, disks in gates.items() if disks}
        offline = []
        sources, metadata, writable, clients = total - 1, total, write - 1, len(hosts)
    else:
        offline = hosts[lock["exclusiveQuorum"] - 1:]
        require(len(offline) == 2, "H2 must lose exactly two distinct lock clients")
        # The missing target belongs to an offline member, preserving all R
        # payload sources on the survivors. Do not subtract it twice.
        require(target["node"] in offline, "lock-negative missing target must be on an unavailable member")
        gates = {}
        sources = metadata = writable = total - 4 * len(offline)
        clients = len(hosts) - len(offline)
    require(sources >= read and writable < write and metadata >= read
            and (clients >= lock["exclusiveQuorum"] if gates else lock["sharedQuorum"] <= clients < lock["exclusiveQuorum"]),
            "B08 fault does not preserve R or independently remove W/lock admission")
    return {"applicable": True, "readQuorum": read, "writeQuorum": write, "totalDrives": total,
            "readablePayloadSources": sources, "readableMetadataAuthorities": metadata,
            "writableDrives": writable, "lock": {**lock, "availableClients": clients},
            "gates": gates, "offlineNodes": offline,
            "backend": "service-private-read-only-mounts" if gates else "exact-owned-peer-process-stop",
            "hostNetworkRulesChanged": False, "hostMountRemounted": False}


def expected_reads(snapshot, node, relative):
    indexed = files(snapshot)
    result = []
    for disk in range(4):
        entry = indexed.get((node, disk, relative))
        require(entry is not None, "quorum witness lacks original metadata on a selected disk")
        result.append({name: entry[name] for name in ("size", "sha256")})
    return result


def partition_snapshot(fleet, plan, label):
    """Keep every physical member while attesting each actual process state."""
    offline = set(plan['offlineNodes'])
    require(offline <= set(fleet.nodes), 'quorum snapshot has a foreign stopped node')
    return fleet.record(label, {node: fleet.remote(node, 'snapshot', quiescent=node in offline)
                               for node in fleet.nodes})


def validate_gate_witness(observed, plan, online):
    require(set(observed) == set(online), "quorum probe omitted an available candidate")
    rows = [(node, row) for node, sample in observed.items() for row in sample["disks"]]
    require(len(rows) == 4 * len(online) and len({(node, row["disk"]) for node, row in rows}) == len(rows)
            and all(row["readableMetadata"] is True and row["sameServiceUidAndMountNamespace"] is True for _, row in rows),
            "quorum gate witness omitted or duplicated real metadata authorities")
    readonly = {(node, disk) for node, disks in plan["gates"].items() for disk in disks}
    require(all(type(row["writable"]) is bool and row["writable"] is ((node, row["disk"]) not in readonly)
                and row["writeErrno"] == (30 if (node, row["disk"]) in readonly else 0) for node, row in rows)
            and sum(row["writable"] for _, row in rows) == plan["writableDrives"],
            "measured service write refusals differ from the frozen W boundary")
    return {"measuredReadableMetadataAuthorities": len(rows), "measuredWritableDrives": plan["writableDrives"],
            "diskCountsAreNotLockAdmission": True}


def negative_put(observed):
    require(observed.get("acknowledged") is False and observed.get("clientCallInvoked") is True,
            "quorum-negative PUT returned a success ACK or was never invoked")
    if observed.get("resultUnknown") is True:
        return {"state": "UNKNOWN_PENDING_RESOLUTION", "retryAllowed": False}
    response = observed.get("response") or {}
    require(observed.get("resultUnknown") is False and response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {500, 503}
            and response.get("Error", {}).get("Code") in REFUSALS,
            "quorum-negative PUT lacks a structured candidate refusal")
    return {"state": "REFUSED", "retryAllowed": False, "code": response["Error"]["Code"]}


def resolve_put(observed, reads, expected_payload):
    require(bool(reads), "unique PUT outcome was not queried after recovery")
    states = []
    for read in reads:
        if read.get("acknowledged") is True and read.get("resultUnknown") is False:
            require(read.get("payload") == expected_payload, "uncertain PUT resolved to incorrect committed bytes")
            states.append("COMMITTED")
        else:
            response = read.get("response") or {}
            require(read.get("acknowledged") is False and read.get("resultUnknown") is False
                    and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
                    and response.get("Error", {}).get("Code") == "NoSuchKey", "unique PUT outcome remains unresolved")
            states.append("ABSENT")
    require(len(set(states)) == 1, "peers disagree on the unique PUT commit outcome")
    if observed.get("acknowledged") is True:
        require(states[0] == "COMMITTED", "a success PUT ACK cannot be redeemed on the recovered peers")
    else:
        require(observed.get("resultUnknown") is True or states[0] == "ABSENT", "refused PUT nevertheless committed")
    return {"state": states[0], "allPeersAgree": True, "retryIssued": False, "successAckClaimed": False}


def endpoint_refused(binding, node_index):
    endpoint = binding["endpoints"][f"node{node_index + 1}"]
    try:
        with request.build_opener(request.ProxyHandler({})).open(endpoint + "/health/ready", timeout=2):
            raise ValueError("stopped lock peer still accepts requests")
    except URLError as exc:
        require(isinstance(exc.reason, ConnectionRefusedError), "lock peer failure is not witnessed endpoint refusal")
    return {"endpoint": endpoint, "connectionRefused": True, "lockAndDiskEndpointUnavailable": True}


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    lock_fixture = topology == "H2" and branch == "lock-quorum-absent"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="lock-quorum" if lock_fixture else "manual")
    candidate = json.loads(args.candidate.read_text())
    reference = source_reference(candidate["source_sha"])
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), "maxInventoryObjects": 256,
        "maxInventoryBytes": 512 * 1024**2}
    fleet.record("lock-policy-source-contract", reference["lock"])
    fleet.record("storage-boundary-source-contract", reference["storage"])
    contract = verify_quorum_contract(fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate)), candidate)
    fleet.record("binding", binding)
    probe, active, admissions = Probe(binding), [], []
    result = {"topology": topology, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    put_observed, put_key, put_payload, original = None, None, None, None
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        if lock_fixture:
            processes = fleet.snapshot("lock-fixture-actual-storage-configuration")
            require(all(value["storageClassEnvironment"]["RUSTFS_STORAGE_CLASS_STANDARD"] == "EC:8"
                        for value in processes.values()), "lock fixture did not load its actual sixteen-drive eight-parity configuration")
        expected = seed(Recorder(probe, directory / "fixture"), bucket, "DB", 128, SIZES)
        baseline = fleet.record("S3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        fleet.stop()
        original = fleet.snapshot("physical-baseline", quiescent=True)
        validate_topology(original, topology)
        selected = next(entry for entry in expected if entry["payload"]["size"] > 1024**2)
        target = map_target(original, bucket, selected["key"], "data", 1)
        if branch == "lock-quorum-absent" and topology == "H2":
            shard = next(row for row in target["cohort"] if row["node"] == nodes[-1])
            target = {**target, **shard}
        plan = fleet.record("frozen-quorum-plan", plan_for(original, target, branch, contract))
        if not plan["applicable"]:
            result.update(verdict="NOT_APPLICABLE", reason=plan["reason"])
            return result
        params = {"disk": target["disk"], "faultId": "B08-missing-target"}
        registration = target["node"], params
        fleet.faults.append(registration)
        receipt = fleet.record("target-fault", fleet.remote(target["node"], "apply-fault", **params,
            relativePath=target["relativePath"], kind="missing", expectedSha256=target["sha256"]))
        faulted = fleet.snapshot("physical-fault", quiescent=True)
        validate_fault(original, faulted, target, receipt)
        for node, disks in plan["gates"].items():
            fleet.quorum_gates.add(node)
            fleet.record("write-gate-intent", {"node": node, **fleet.remote(node, "quorum-gate", disks=disks)})
        online = [node for node in nodes if node not in plan["offlineNodes"]]
        if plan["offlineNodes"]:
            # Start the intact cluster first, then isolate its exact two owned
            # peer processes. No host firewall, mount or VM is changed.
            fleet.start()
            for node in plan["offlineNodes"]:
                stopped = fleet.record("lock-peer-stopped", {"node": node, **fleet.remote(node, "stop")})
                require(stopped.get("quiescent") is True and stopped.get("service", {}).get("MainPID") == "0",
                        "lock peer process did not actually exit")
                fleet.record("lock-peer-endpoint-refused", endpoint_refused(binding, nodes.index(node)))
        else:
            fleet.start()
        relative = f"{bucket}/{selected['key']}/xl.meta"
        def witness(label):
            raw = fleet.record(label, {node: fleet.remote(node, "quorum-gate-probe", relativePath=relative,
                expectedReads=expected_reads(original, node, relative)) for node in online})
            return validate_gate_witness(raw, plan, online)
        fleet.record("quorums-before-operations", witness("service-quorum-before"))
        require(physical_content(faulted) == physical_content(partition_snapshot(fleet, plan, "physical-before-operations")),
                "quorum loss changed the fault before its negative operations")
        read = fleet.record("read-without-write-admission", probe.s3("get_object", {"Bucket": bucket, "Key": selected["key"]}))
        require(read.get("acknowledged") is True and read.get("resultUnknown") is False
                and read.get("payload") == selected["payload"], "read quorum did not serve exact acknowledged bytes")
        put_key = "quorum-negative/unique-put.bin"
        put_body = payload(f"{bucket}/{put_key}/0", 5 * 1024**2 + 97)
        put_payload = {"size": len(put_body), "sha256": hashlib.sha256(put_body).hexdigest()}
        put_observed = fleet.record("single-negative-PUT-no-retry", probe.s3("put_object", {"Bucket": bucket, "Key": put_key,
            "Metadata": {"heal-fixture": "DL"}, "Tagging": "suite=heal&fixture=DL"}, put_body))
        result["put"] = negative_put(put_observed)
        status = run_task(probe, fleet, {"bucket": bucket, "key": selected["key"]}, 2, active,
                          admissions=admissions, allow_coordination_rejection=(topology == "H2"))
        coordination_rejected = status.get("admissionState") == "coordination-rejected"
        result["negativeResponsibility"] = validate_coordination_rejection(status) if coordination_rejected \
            else validate_unwritable(status, bucket, selected)
        result["negativeResponsibility"]["physicalMutationClaimed"] = False
        fleet.record("quorums-after-operations", witness("service-quorum-after"))
        for node in plan["offlineNodes"]:
            fleet.record("lock-peer-still-refused", endpoint_refused(binding, nodes.index(node)))
        # Evidence must precede any source return, gate removal, backup rescue
        # or target GET on the recovered cluster.
        fleet.stop()
        scene = fleet.snapshot("physical-negative-before-recovery", quiescent=True)
        require(physical_content(faulted) == physical_content(scene), "negative write/Heal mutated target or intact controls")
        logs = {node: fleet.record("negative-quorum-peer-logs", {"node": node, **fleet.remote(node, "logs")})
                for node in nodes}
        if coordination_rejected:
            result["coordinationRejectionLogs"] = validate_coordination_rejection_logs(logs)
        for node in tuple(fleet.quorum_gates):
            fleet.record("release-write-quorum-gate", fleet.remote(node, "quorum-gate", mode="clear"))
            fleet.quorum_gates.remove(node)
        fleet.start()
        require(physical_content(faulted) == physical_content(fleet.snapshot("physical-source-return-before-repair")),
                "source return repaired the target before the independent manual recovery task")
        recovered = run_task(probe, fleet, {"bucket": bucket, "key": selected["key"]}, 2, active,
                             status_endpoints=tuple(binding["endpoints"])[1:], admissions=admissions)
        if coordination_rejected:
            require(len(admissions) == 1 and admissions[0]["clientToken"],
                    "recovered quorum repair lacks its first admitted task identity")
        else:
            require(len(admissions) == 2 and admissions[-1]["clientToken"] != admissions[-2]["clientToken"],
                    "recovered quorum repair reused the negative-window task identity")
        result["recoveryResponsibility"] = validate_version_receipts(recovered, bucket, [selected], 2,
            required_repairs={(selected["key"], "null")})
        idle_fleet(fleet, binding)
        fleet.stop()
        after = fleet.snapshot("physical-reconstructed-before-GET", quiescent=True)
        result["physical"] = validate_repaired_physical(original, after, target)
        result["streaming"] = verify_cohort(fleet, target, "all-original-cohort-bitrot")
        fleet.faults.remove(registration)
        fleet.start()
        reads = [fleet.record("unique-PUT-resolution", Probe(binding, endpoint).s3("get_object", {
            "Bucket": bucket, "Key": put_key})) for endpoint in binding["endpoints"]]
        result["putResolution"] = resolve_put(put_observed, reads, put_payload)
        # The complete physical no-mutation oracle also proves this particular
        # unknown call did not commit. No uncertain call is silently discarded.
        require(result["putResolution"]["state"] == "ABSENT", "unique PUT committed despite the proven W/lock loss")
        result["s3"] = {endpoint: verify_version_s3(Probe(binding, endpoint), fleet, bucket, expected, baseline,
            "all-peer-S3-" + endpoint, fixture="DB") for endpoint in binding["endpoints"]}
        result["verdict"] = "PASS"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-unresolved-error", {"error": str(exc)})
        # An unknown unique PUT is resolved even if a later assertion failed.
        # Recovery cannot convert that original failed branch to PASS.
        if put_observed and put_key and "putResolution" not in result:
            try:
                fleet.stop()
                for node in tuple(fleet.quorum_gates):
                    fleet.record("failed-branch-release-gate", fleet.remote(node, "quorum-gate", mode="clear"))
                    fleet.quorum_gates.remove(node)
                fleet.start()
                reads = [fleet.record("failed-branch-unique-PUT-resolution", Probe(binding, endpoint).s3("get_object", {
                    "Bucket": bucket, "Key": put_key})) for endpoint in binding["endpoints"]]
                result["putResolution"] = resolve_put(put_observed, reads, put_payload)
            except Exception as exc:
                result["putResolution"] = {"state": "UNRESOLVED", "error": str(exc)}
        if result.get("putResolution", {}).get("state") == "UNRESOLVED":
            fleet.stop()
            fleet.record("retained-unresolved-PUT", {"state": "DIRTY", "namespacesRetained": True,
                "reason": "the original unique request has no complete all-peer resolution"})
            clean = False
        else:
            clean = fleet.cleanup()
        result["cleanup"] = "PASS" if clean else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"B08 workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=[CASE_ID], required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha) and re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", "")),
            "immutable workflow candidate identity required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "B08 source branch omitted")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", n) for n in nodes),
            "four current Heal nodes required")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, selected, branch))
                if results[-1]["cleanup"] != "PASS": break
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == 4 and all(row["cleanup"] == "PASS" and (row["verdict"] == "PASS"
            or row["topology"] == "H1" and row["branch"] == "lock-quorum-absent"
            and row["verdict"] == "NOT_APPLICABLE" and row.get("reason") == LOCAL_LOCK_REASON) for row in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "complete": complete,
            "cleanup": "PASS" if results and all(row["cleanup"] == "PASS" for row in results) else "FAIL"})
    require(complete, "B08 requires full independent write/lock negative evidence, product recovery and all-peer ACK verification")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"B08 write/lock quorum validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
