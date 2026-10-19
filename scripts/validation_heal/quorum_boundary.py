#!/usr/bin/env python3
"""B06: independent read, manual repair and restored-source quorum witnesses."""

from __future__ import annotations

import argparse
import copy
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys

try:
    from .heal_probe import Probe, payload, require, save
    from .heal_workload import Recorder, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .shards import files, map_target, run_task, validate_fault, validate_repaired_physical, verify_cohort
    from .versions import (validate_version_receipts, validate_unrecoverable_get,
                           verify_quorum_contract, verify_version_s3)
except ImportError:
    from heal_probe import Probe, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import files, map_target, run_task, validate_fault, validate_repaired_physical, verify_cohort
    from versions import (validate_version_receipts, validate_unrecoverable_get,
                          verify_quorum_contract, verify_version_s3)


CASE_ID = "heal.read-quorum-boundary"
BRANCHES = ("exact-r-read", "exact-r-heal-write-limited", "restored-w")
SIZE = 5 * 1024**2 + 123
# These reviewed lock policies are deliberately candidate-specific. They do
# not require a service checkout on the jump host or a modified service binary.
LOCK_REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "clientDomain": "distinct nonempty pool endpoint host:port; H1 uses its local manager",
    "exclusiveQuorumRule": "floor(clientCount/2)+1; local manager=1",
    "sharedQuorumRule": "ceil(clientCount/2); local manager=1",
    "sourceFiles": {
        "crates/ecstore/src/core/sets.rs": "06543b1d083f489346171ba489b1eb1e0b7899c844f0ebf15e16ad467cf5e2f1",
        "crates/ecstore/src/runtime/sources.rs": "7d88f56e6d06fd31850bbbdb2fd9faf20fcffbaedf380888f5f9285c309794f2",
        "crates/ecstore/src/set_disk/ops/locking.rs": "44d50605c5538adf8fc6f25034e426011d0087b37e895f8246c79cbd711b2a42",
        "crates/ecstore/src/set_disk/ops/heal.rs": "767c468b780cf5f4e863b078f46c63440549e75d88b6b43bb59ed83c3b4a47b2",
        "crates/lock/src/distributed_lock.rs": "657796708fdc5f826f896147b74a9366dae875fd6b69e4b253994966ed5f6e37"}}


LOCK_REFERENCES = {
    LOCK_REFERENCE["candidateCommit"]: LOCK_REFERENCE,
    "1c7fc818dd858247c45b39542abd8291b1e6e31b": {
        **LOCK_REFERENCE,
        "candidateCommit": "1c7fc818dd858247c45b39542abd8291b1e6e31b",
        "rpcControlChannelPolicy": "class-aware control channel; audience and replay-scope authentication retained; live admission remains required",
        "sourceFiles": {
            **LOCK_REFERENCE["sourceFiles"],
            "crates/ecstore/src/runtime/sources.rs": "fad5d30e9f76a71c41a4dd5cfa816d3ea49b0fa082ab111118e2ee5a6f8fcc90",
            "crates/ecstore/src/set_disk/ops/heal.rs": "fc576119f455848ce279772ffca6d2943cb9f909968cae367860a9887d3e020e",
            "crates/ecstore/src/cluster/rpc/remote_locker.rs": "506a16274efc0ac7dd2ffa4e2faf2b2a057deacfb8fb9480f4785768628b9cbf",
            "crates/ecstore/src/cluster/rpc/client.rs": "b37e4107e898a3467520c8516608c8eaaa0c1d56a2eac82d11982eb483396291"},
    },
}


LOCK_REFERENCES['85c706cac09d53d0649487b8f1865f18beb53225'] = {
    **LOCK_REFERENCES['1c7fc818dd858247c45b39542abd8291b1e6e31b'],
    'candidateCommit': '85c706cac09d53d0649487b8f1865f18beb53225'}


def lock_reference(candidate_commit):
    require(isinstance(candidate_commit, str) and candidate_commit in LOCK_REFERENCES,
            "lock policy is not reviewed for this candidate")
    return copy.deepcopy(LOCK_REFERENCES[candidate_commit])


def seed_fixtures(recorder, bucket):
    recorder.call("create_bucket", {"Bucket": bucket})
    expected = []
    for branch in BRANCHES:
        key = "boundary/" + branch + ".bin"
        body = payload(f"{bucket}/{key}/0", SIZE)
        response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
            "Metadata": {"heal-fixture": "DB", "branch": branch}, "Tagging": "suite=heal&fixture=DB"}, body)
        require(response.get("VersionId") in {None, "null"}, "B06 requires never-versioned independent objects")
        expected.append({"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                         "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
    return expected


def boundary_plan(branch, snapshot, bucket, expected, contract):
    require(branch in BRANCHES and contract.get("recognized") is True
            and contract.get("writeQuorumRule") == "data-plus-one-if-equal-parity",
            "B06 requires a recognized candidate read/write quorum contract")
    selected = [entry for entry in expected if entry["key"] == "boundary/" + branch + ".bin"]
    require(len(selected) == 1 and selected[0].get("versionId") == "null"
            and selected[0].get("deleteMarker") is False, "B06 branch reused or lost its independent ACK")
    target = map_target(snapshot, bucket, selected[0]["key"], "data", 1)
    geometry = target["logical"]["erasure"]
    read, parity = geometry["data_blocks"], geometry["parity_blocks"]
    write, total = read + int(read == parity), read + parity
    retained_count = read - 1 if branch == "restored-w" else read
    ordered = sorted(target["cohort"], key=lambda item: item["index"])
    retained, missing = ordered[:retained_count], ordered[retained_count:]
    faults = [{**target, **shard} for shard in missing]
    restore_count = write - retained_count if branch == "restored-w" else 0
    require(read > 1 and len(ordered) == total and len({item["index"] for item in ordered}) == total
            and len(retained) == retained_count and 0 <= restore_count < len(faults),
            "B06 layout cannot leave real product repair targets after restoration to W")
    hosts = len(snapshot)
    require(hosts in {1, 4}, "B06 lock client domain requires actual H1/H2 topology")
    return {"scope": {"bucket": bucket, "key": selected[0]["key"]}, "expected": selected,
        "target": target, "faults": faults, "retained": retained, "restoreCount": restore_count,
        "readQuorum": read, "writeQuorum": write, "sameVersionSourcesBefore": retained_count,
        "writableTargets": total, "metadataAuthorities": total,
        "lock": {"mode": "local" if hosts == 1 else "distributed", "clientCount": hosts,
                 "sharedQuorum": (hosts + 1) // 2, "exclusiveQuorum": hosts // 2 + 1,
                 "allEndpointsOnline": True, "successfulRepairAlsoProvesActualAdmission": True},
        "insufficientWriteOrLockApplicable": False,
        "nonApplicableReason": "F1 isolates payload files only; all N drives, metadata and host lock endpoints remain online. "
            "R counts readable payload authorities, not writable drives. B08 independently injects actual write/lock loss."}


def source_witness(snapshot, plan, missing):
    indexed = files(snapshot)
    identities = {(item["node"], item["disk"], item["relativePath"]) for item in missing}
    cohort = plan["target"]["cohort"]
    require(len(identities) == len(missing) and identities <= {
        (item["node"], item["disk"], item["relativePath"]) for item in cohort}, "missing authority escaped its version/set")
    for item in cohort:
        identity = item["node"], item["disk"], item["relativePath"]
        actual = indexed.get(identity)
        if identity in identities:
            require(actual is None, "ATTRIBUTION_NOT_OBSERVED: isolated source reappeared before manual repair")
        else:
            require(actual is not None and actual["sha256"] == item["sha256"] and actual["size"] == item["size"],
                    "retained same-version source differs from its full shard oracle")
        metadata = indexed.get((item["node"], item["disk"], f"{plan['scope']['bucket']}/{plan['scope']['key']}/xl.meta"))
        require(metadata and metadata["sha256"] == item["baselineMetadataSha256"],
                "F1 changed metadata authority or used a different generation")
    return {"sameVersionPayloadSources": len(cohort) - len(identities), "missingTargets": len(identities),
            "allMetadataAuthoritiesOnlineAndUnchanged": True, "allRetainedShardBytesUnchanged": True}


def read_observation(observation, expected):
    require(observation.get("acknowledged") is True and observation.get("resultUnknown") is False
            and observation.get("response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
            and observation.get("payload") == expected["payload"], "GET at exactly R did not return the acknowledged bytes")


def read_interference(before, after, plan):
    """Permit only exact product reconstruction, and retain each observed target."""
    left, right = files(before), files(after)
    targets = {(item["node"], item["disk"], item["relativePath"]): item for item in plan["faults"]}
    reconstructed = []
    for identity, target in targets.items():
        require(identity not in left, "exact-R GET began with an already reconstructed source")
        if identity in right:
            require(right[identity]["sha256"] == target["sha256"] and right[identity]["size"] == target["size"],
                    "GET produced a target with incorrect shard bytes")
            reconstructed.append({"node": identity[0], "disk": identity[1], "relativePath": identity[2]})
    return {"readRepairObserved": bool(reconstructed), "reconstructedTargets": reconstructed,
            "manualRepairClaimed": False, "manualStartIssuedForReadBranch": False,
            "note": "Any unexpected reconstruction is retained as interference; this GET never certifies the separate manual branch."}


def recover_registered(fleet, registrations, label):
    for registration in reversed(tuple(registrations)):
        node, params = registration
        receipt = fleet.record(label, fleet.remote(node, "recover-fault", **params))
        require(receipt.get("physicalRepairPassAllowed") is False, "backup restoration must not certify product repair")
        fleet.faults.remove(registration)
        registrations.remove(registration)


def run_topology(args, topology, nodes):
    directory = args.evidence / "DB" / topology
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory)
    candidate = json.loads(args.candidate.read_text())
    locks = lock_reference(candidate["source_sha"])
    contract = verify_quorum_contract(fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate)), candidate)
    require(contract.get("writeQuorumRule") == "data-plus-one-if-equal-parity", "B06 write policy is not recognized")
    require(list(BRANCHES) == select(CASE_ID)[0]["variants"], "B06 executor omitted a source branch")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 512, "maxInventoryBytes": 512 * 1024**2}
    fleet.record("binding", binding)
    fleet.record("quorum-source-contract", {"payloadAndWrite": contract, "locks": locks})
    probe, active, admissions = Probe(binding), [], []
    result = {"topology": topology, "fixture": "DB", "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("topology-proof", validate_topology(fleet.snapshot("deployed-topology"), topology))
        expected = seed_fixtures(Recorder(probe, directory / "fixture"), bucket)
        baseline = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        for branch in BRANCHES:
            registrations = []
            try:
                fleet.stop()
                before = fleet.snapshot("physical-before-" + branch, quiescent=True)
                plan = boundary_plan(branch, before, bucket, expected, contract)
                fleet.record("quorum-plan-" + branch, plan)
                verify_cohort(fleet, plan["target"], "baseline-bitrot-" + branch)
                prior = before
                for index, target in enumerate(plan["faults"]):
                    params = {"disk": target["disk"], "faultId": bounded_fault_id("B06", branch, index)}
                    registration = target["node"], params
                    fleet.faults.append(registration)
                    registrations.append(registration)
                    receipt = fleet.record("fault-" + branch, fleet.remote(target["node"], "apply-fault", **params,
                        relativePath=target["relativePath"], expectedSha256=target["sha256"], kind="missing"))
                    faulted = fleet.snapshot("physical-fault-" + branch, quiescent=True)
                    validate_fault(prior, faulted, target, receipt)
                    prior = faulted
                source_witness(prior, plan, plan["faults"])
                fleet.start()
                witness = fleet.snapshot("physical-before-trigger-" + branch)
                require(physical_content(prior) == physical_content(witness), "ATTRIBUTION_NOT_OBSERVED: B06 fault changed before trigger")
                fleet.record("source-window-" + branch, source_witness(witness, plan, plan["faults"]))
                proof = {"quorums": {name: plan[name] for name in ("readQuorum", "writeQuorum", "writableTargets", "lock",
                    "insufficientWriteOrLockApplicable", "nonApplicableReason")}}
                if branch == "exact-r-read":
                    observed = fleet.record("exact-r-get", probe.s3("get_object", {"Bucket": bucket, "Key": plan["scope"]["key"]}))
                    read_observation(observed, plan["expected"][0])
                    after_get = fleet.snapshot("physical-after-exact-r-get")
                    proof["read"] = read_interference(witness, after_get, plan)
                    fleet.stop()
                    recover_registered(fleet, registrations, "get-fixture-backup-rescue")
                    repaired = fleet.snapshot("physical-after-get-rescue", quiescent=True)
                    validate_repaired_physical(before, repaired, plan["target"], repair_targets=plan["faults"])
                    proof["backupRescueNotManualRepair"] = True
                else:
                    remaining = plan["faults"]
                    if branch == "restored-w":
                        denied = fleet.record("below-r-get", probe.s3("get_object", {"Bucket": bucket, "Key": plan["scope"]["key"]}))
                        validate_unrecoverable_get(denied, contract)
                        require(physical_content(prior) == physical_content(fleet.snapshot("physical-after-below-r-get")),
                                "below-R GET changed good shards or repaired without authority")
                        previous_token = admissions[-1]["clientToken"] if admissions else None
                        fleet.stop()
                        stopped = fleet.snapshot("physical-before-restore", quiescent=True)
                        require(physical_content(prior) == physical_content(stopped), "below-R read window changed original authority")
                        proof["belowReadWindow"] = {"frozenUnrecoverableGetMatched": True,
                            "sourceWitness": source_witness(stopped, plan, plan["faults"]), "backupRestoreHasNotRun": True}
                        count = plan["restoreCount"]
                        restored_registrations = registrations[:count]
                        recover_registered(fleet, restored_registrations, "restore-original-sources-to-w")
                        del registrations[:count]
                        remaining = plan["faults"][count:]
                        prior = fleet.snapshot("physical-at-w", quiescent=True)
                        proof["atW"] = source_witness(prior, plan, remaining)
                        require(proof["atW"]["sameVersionPayloadSources"] == plan["writeQuorum"], "restore did not reach exactly W")
                        proof["restoredOriginalSources"] = count
                        fleet.start()
                        witness = fleet.snapshot("physical-before-restored-w-trigger")
                        require(physical_content(prior) == physical_content(witness), "ATTRIBUTION_NOT_OBSERVED: restored-W fault changed before its token")
                        source_witness(witness, plan, remaining)
                    status = run_task(probe, fleet, plan["scope"], 2, active, admissions=admissions)
                    if branch == "restored-w":
                        require(previous_token is None or admissions[-1]["clientToken"] != previous_token,
                                "restored-W Heal reused another branch's manual token")
                        proof["previousIndependentToken"], proof["restoredToken"] = previous_token, admissions[-1]["clientToken"]
                    fleet.stop()
                    repaired = fleet.snapshot("physical-after-manual-" + branch, quiescent=True)
                    proof["responsibility"] = validate_version_receipts(status, bucket, plan["expected"], 2,
                        required_repairs={(plan["scope"]["key"], "null")})
                    proof["physical"] = validate_repaired_physical(before, repaired, plan["target"], repair_targets=remaining)
                    proof["productReconstructedTargets"] = len(remaining)
                    for registration in registrations:
                        fleet.faults.remove(registration)
                    registrations.clear()
                proof["bitrot"] = verify_cohort(fleet, plan["target"], "repaired-bitrot-" + branch)
                fleet.start()
                proof["s3"] = verify_version_s3(probe, fleet, bucket, expected, baseline, "s3-after-" + branch, fixture="DB")
                result["branches"][branch] = {"verdict": "PASS", **proof}
            except (Exception, KeyboardInterrupt) as exc:
                result["branches"][branch] = {"verdict": "FAIL", "failure": {"type": type(exc).__name__, "message": str(exc)}}
                fleet.stop()
                fleet.snapshot("physical-failure-" + branch, quiescent=True)
                recover_registered(fleet, registrations, "error-backup-rescue-" + branch)
                if isinstance(exc, KeyboardInterrupt) or active:
                    raise
        result["verdict"] = "PASS" if len(result["branches"]) == len(BRANCHES) and all(
            item["verdict"] == "PASS" for item in result["branches"].values()) else "FAIL"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
    finally:
        for path, token in active:
            try:
                fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc:
                fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("topology-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
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
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes) and all(
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal nodes required")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)), "invalid candidate binary digest")
    require(bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "workflow fleet identity required")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            results.append(run_topology(args, topology, selected))
            if results[-1]["cleanup"] != "PASS":
                break
    finally:
        complete = len(results) == 2 and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "B06 requires all independent source branches on H1/H2 and complete cleanup")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal read quorum validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
