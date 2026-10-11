#!/usr/bin/env python3
"""Audit and finalize only explicitly supported, exactly archived failures."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re

try:
    from .healthy import Fleet, physical_content
    from .heal_probe import require, save
except ImportError:
    from healthy import Fleet, physical_content
    from heal_probe import require, save


def quota_recovery_plan(artifact, failed_run, candidate):
    """A private H1 quota failure before workload/Heal admits no unknown writes."""
    case = "heal.enospc-inodes-and-quota"
    results = list(artifact.glob(f"*/run/{case}/cases/{case}.json"))
    require(len(results) == 1, "quota recovery requires one exact I02 result")
    result = json.loads(results[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL" and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"], "quota failed-result candidate or cleanup differs")
    evidence = results[0].parents[1] / f"evidence/{case}"
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    retained = [row for row in summary["results"] if row.get("cleanup") != "PASS"]
    require(summary.get("cleanup") == "FAIL" and len(retained) == 1, "quota recovery cannot omit another retained instance")
    row = retained[0]
    require(row.get("topology") == "H1" and row.get("branch") == "bucket-quota" and row.get("verdict") == "FAIL"
            and row.get("failure", {}).get("message") == "peer did not observe the exact acknowledged quota limit"
            and set(row) <= {"topology", "branch", "verdict", "cleanup", "failure", "recoveryError"},
            "quota recovery only supports failure before any negative PUT or Heal task")
    folder = evidence / "H1/bucket-quota"
    def one(suffix):
        matches = list(folder.glob("*-" + suffix + ".json"))
        require(len(matches) == 1, "quota recovery lacks one exact " + suffix)
        return json.loads(matches[0].read_text())
    binding, before, failure = one("binding"), one("original-complete-physical"), one("physical-original-failure-before-any-rescue")
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h1", binding["runId"]) is not None
            and binding["candidateBinarySha256"] == provenance["binary_sha256"]
            and binding["harnessCommit"] == provenance["testing_commit"] and len(before) == 1
            and set(before) == set(failure), "quota archived run/candidate/host differs")
    node = next(iter(before))
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) is not None, "quota archived node is invalid")
    live = one("manual-source-isolation-before-fault")
    require(all(value.get("quiescent") is True for scene in (before, failure) for value in scene.values())
            and set(live) == {node} and (live[node].get("backgroundConfig") or {}).get("RUSTFS_SCANNER_ENABLED") == "false"
            and live[node].get("binarySha256") == provenance["binary_sha256"],
            "quota archive has no stopped fixture and actual isolated candidate runtime")
    quota = one("owned-bucket-quota-PUT")
    read = one("all-peer-owned-quota-GET")
    require(quota.get("path") == read.get("path") == "/rustfs/admin/v3/quota/" + binding["bucketPrefix"]
            and quota.get("method") == "PUT" and quota.get("httpStatus") == 200
            and quota.get("response", {}).get("quota") == 1024**2 and quota["response"].get("quota_type") == "HARD"
            and read.get("method") == "GET" and read.get("httpStatus") == 503
            and "authoritative bucket usage is not available yet" in str(read.get("response")),
            "quota recovery lacks its exact acknowledged limit and usage-unavailable failure")
    require(not (folder / "negative-PUT").exists() and not (folder / "capacity-released-PUT").exists(),
            "quota recovery cannot finalize an invoked or uncertain workload")
    receipts = [json.loads(path.read_text()) for path in folder.glob("*-exact-missing-part.json")]
    require(len(receipts) == 2 and len({item.get("faultId") for item in receipts}) == 2,
            "quota rescue requires both original fault receipts")
    left = {(item["disk"], item["path"]): item for item in before[node]["files"]}
    right = {(item["disk"], item["path"]): item for item in failure[node]["files"]}
    require(before[node]["formats"] == failure[node]["formats"] and len(left) == len(before[node]["files"])
            and len(right) == len(failure[node]["files"]), "quota archived disk identity or file census differs")
    faults, removed = [], set()
    for receipt in receipts:
        matches = [key for key, entry in left.items() if entry["path"] == receipt.get("relativePath")
                   and key not in right and receipt.get("before") == {"sha256": entry["sha256"], "size": entry["size"]}]
        require(receipt.get("runId") == binding["runId"] and receipt.get("kind") == "missing"
                and re.fullmatch(r"I02-[a-z0-9-]{1,48}", receipt.get("faultId", "")) is not None and len(matches) == 1
                and matches[0] not in removed, "quota fault receipt has a wrong/duplicate run, shard or backup identity")
        removed.add(matches[0])
        faults.append({"disk": matches[0][0], "faultId": receipt["faultId"]})
    require({key: value for key, value in left.items() if key not in removed} == right,
            "quota archived failure changed other content or versions; preserve namespace")
    return [{"kind": "owned-quota", "runId": binding["runId"], "nodes": [node], "node": node,
             "bucket": binding["bucketPrefix"], "binarySha256": provenance["binary_sha256"],
             "before": before, "failure": failure, "faults": faults,
             "expectedSceneSha256": hashlib.sha256(json.dumps(physical_content(before)[node], sort_keys=True,
                                                               separators=(",", ":")).encode()).hexdigest()}]


def recovery_plan(artifact, failed_run, candidate):
    require(type(failed_run) is int and failed_run > 0, "exact failed workflow run required")
    if list(artifact.glob("*/run/heal.enospc-inodes-and-quota/cases/heal.enospc-inodes-and-quota.json")):
        require(not list(artifact.glob("*/run/heal.object-size-boundaries/cases/*.json")), "mixed failed case recovery artifact")
        return quota_recovery_plan(artifact, failed_run, candidate)
    performance = list(artifact.glob("*/run/heal.performance-abba/cases/heal.performance-abba.json"))
    if performance:
        return performance_recovery_plan(artifact, failed_run, candidate)
    results = list(artifact.glob("*/run/heal.object-size-boundaries/cases/heal.object-size-boundaries.json"))
    require(len(results) == 1, "failed artifact must have one exact D01 canonical result")
    result = json.loads(results[0].read_text())
    require(result["verdict"] == "FAIL" and result["cleanup_status"] == "FAIL"
            and result["provenance"]["source_commit"] == candidate["source_sha"]
            and result["provenance"]["candidate_sha256"] == candidate["package_sha256"], "failed result candidate or cleanup identity differs")
    evidence = results[0].parents[1] / "evidence/heal.object-size-boundaries"
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    plans = []
    for topology in summary["results"]:
        if topology["cleanup"] == "PASS":
            continue
        require(topology["topology"] in {"H1", "H2"} and topology["fixture"] == "DB"
                and "invalid fault ID" in topology.get("failure", {}).get("message", ""), "cleanup failure needs a different recovery procedure")
        failed = [(name, branch) for name, branch in topology["branches"].items()
                  if "invalid fault ID" in branch.get("failure", {}).get("message", "")]
        require(len(failed) == 1, "cannot identify one rejected fault branch")
        branch, _ = failed[0]
        folder = evidence / "DB" / topology["topology"]
        files = list(folder.glob("*-physical-before-" + branch + ".json"))
        faults = list(folder.glob("*-fault-plan-" + branch + ".json"))
        bindings = list(folder.glob("*-binding.json"))
        require(len(files) == len(faults) == len(bindings) == 1, "recovery lacks exact archived pre-fault/binding/plan evidence")
        before, fault, binding = (json.loads(path.read_text()) for path in (files[0], faults[0], bindings[0]))
        require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-{topology['topology'].lower()}", binding["runId"]) is not None,
                "archived run namespace differs from failed Actions run")
        require(len(fault["faults"]) == 1, "rejected-ID recovery cannot omit earlier faults in the same branch")
        target = fault["faults"][0]
        rejected_id = "D01-" + branch + "-0"
        require(re.fullmatch(r"D01-[a-z0-9-]{49,128}", rejected_id) is not None, "failed ID was not rejected for excessive length")
        require(target["node"] in before and target["sha256"] and binding["candidateBinarySha256"] == result["provenance"]["binary_sha256"],
                "fault/candidate physical identity differs")
        nodes = list(before)
        require(all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "invalid archived node name")
        plans.append({"runId": binding["runId"], "nodes": nodes, "node": target["node"], "disk": target["disk"],
            "rejectedFaultId": rejected_id, "binarySha256": binding["candidateBinarySha256"], "before": before,
            "expectedSceneSha256": hashlib.sha256(json.dumps(physical_content(before)[target["node"]],
                sort_keys=True, separators=(",", ":")).encode()).hexdigest()})
    require(plans, "failed artifact has no supported retained environment")
    return plans


def performance_recovery_plan(artifact, failed_run, candidate):
    """Finalize a performance run that expired before its next fault was applied.

    The performance executor registers a fault before invoking the node-side
    mutation.  If the lease expires at that boundary, cleanup still sees the
    registration and tries to recover an intent that was never created.  This
    path is deliberately narrower than a general fault rescue: it accepts only
    the archived phase boundary with no fault or fault-proof record and an
    unchanged physical scene, then asks the node to perform its ordinary
    run-owned cleanup.
    """
    case = "heal.performance-abba"
    results = list(artifact.glob(f"*/run/{case}/cases/{case}.json"))
    require(len(results) == 1, "performance recovery requires one exact J06 result")
    result = json.loads(results[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL"
            and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"],
            "performance failed-result candidate or cleanup differs")
    evidence = results[0].parents[1] / f"evidence/{case}"
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    require(summary.get("cleanup") == "FAIL" and len(summary.get("results", [])) == 1,
            "performance recovery cannot omit or combine a retained instance")
    row = summary["results"][0]
    failure = row.get("failure", {})
    require(row.get("topology") == "H2" and row.get("verdict") == "FAIL"
            and row.get("cleanup") == "FAIL"
            and failure.get("type") == "ValueError"
            and "apply-fault failed" in failure.get("message", "")
            and "lease expired" in failure.get("message", ""),
            "performance recovery only supports a fault lease boundary failure")
    folder = evidence / "H2"
    binding_files = list(folder.glob("*-binding.json"))
    failure_files = list(folder.glob("*-physical-failure.json"))
    boundary_files = list(folder.glob("*-physical-phase-baseline-g3-p3-B.json"))
    protocol_files = list(folder.glob("*-phase-protocol-g3-p3-B.json"))
    require(len(binding_files) == len(failure_files) == len(boundary_files) == len(protocol_files) == 1,
            "performance recovery lacks one exact retained boundary")
    binding = json.loads(binding_files[0].read_text())
    before = json.loads(boundary_files[0].read_text())
    failure_scene = json.loads(failure_files[0].read_text())
    protocol = json.loads(protocol_files[0].read_text())
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h2", binding.get("runId", ""))
            and binding.get("candidateBinarySha256") == provenance.get("binary_sha256")
            and binding.get("harnessCommit") == provenance.get("testing_commit")
            and protocol.get("group") == 3 and protocol.get("phase") == 3 and protocol.get("mode") == "B"
            and physical_content(before) == physical_content(failure_scene),
            "performance retained scene or run identity changed")
    # A failed remote apply has no durable fault evidence.  A fault record or
    # proof at this boundary would require the normal registered-fault rescue.
    require(not list(folder.glob("*-fault-g3-p3-B.json"))
            and not list(folder.glob("*-fault-proof-g3-p3-B.json")),
            "performance boundary contains an applied fault; refuse shortcut cleanup")
    nodes = sorted(failure_scene)
    require(nodes and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "performance archived node identity is invalid")
    bucket = binding.get("bucketPrefix")
    require(isinstance(bucket, str) and re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", bucket),
            "performance binding has no exact primary bucket")
    buckets = {bucket}
    for value in failure_scene.values():
        for item in value.get("files", []):
            name = item.get("path", "").split("/", 1)[0]
            if name == bucket or name.startswith(bucket + "-"):
                buckets.add(name)
    require(buckets == {bucket, bucket + "-load"},
            "performance retained scene omitted or added a run-owned bucket")
    return [{"kind": "performance-no-fault-applied", "runId": binding["runId"], "nodes": nodes,
             "node": nodes[0],
             "buckets": sorted(buckets),
             "before": failure_scene, "expectedSceneSha256": hashlib.sha256(json.dumps(
                 physical_content(failure_scene)[nodes[0]], sort_keys=True, separators=(",", ":")
             ).encode()).hexdigest(), "binarySha256": provenance["binary_sha256"]}]


def recovery_scene_matches(current, plan):
    """A retry may contain only the exact original shards already rescued."""
    observed = physical_content(current)
    expected = physical_content(plan.get("failure", plan["before"]))
    if plan.get("kind") != "owned-quota":
        return observed == expected
    original = physical_content(plan["before"])
    if set(observed) != set(expected):
        return False
    for node, scene in observed.items():
        if scene["formats"] != expected[node]["formats"]:
            return False
        files = {(entry["disk"], entry["path"]): entry for entry in scene["files"]}
        failed = {(entry["disk"], entry["path"]): entry for entry in expected[node]["files"]}
        before = {(entry["disk"], entry["path"]): entry for entry in original[node]["files"]}
        if len(files) != len(scene["files"]) or not set(failed) <= set(files) <= set(before):
            return False
        if any(files[key] != entry for key, entry in failed.items()):
            return False
        if any(files[key] != before[key] for key in set(files) - set(failed)):
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--failed-run-id", type=int, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    plans = recovery_plan(args.artifact, args.failed_run_id, json.loads(args.candidate.read_text()))
    os.environ["RUSTFS_SSH_USER"] = os.environ.get("RUSTFS_SSH_USER") or "azureuser"
    assigned = (os.environ.get("RUSTFS_NODES") or "vm000 vm001 vm002 vm003").split()
    require(all(set(plan["nodes"]) <= set(assigned) for plan in plans), "archived nodes do not belong to the current auto-testing fleet")
    args.evidence.mkdir(parents=True, exist_ok=False)
    complete = []
    for plan in plans:
        fleet = Fleet(plan["nodes"], args.evidence, plan["runId"])
        if plan.get("buckets"):
            fleet.snapshot_buckets = plan["buckets"]
        fleet.record("archived-recovery-plan", {key: value for key, value in plan.items() if key != "before"})
        states = [fleet.record("namespace-state", {"node": node,
            **fleet.remote(node, "namespace-state")}) for node in plan["nodes"]]
        if all(not item["state"]["exists"] and not any(root["exists"] for root in item["roots"])
               for item in states):
            for item in states:
                fleet.record("already-clean", {"node": item["node"], "cleanup": "PASS",
                    "unitStopped": item["service"].get("MainPID", "0") in {"", "0"},
                    "rootsRemoved": True})
            complete.append({"failedRunId": args.failed_run_id, "runId": plan["runId"],
                             "cleanup": "PASS", "originalVerdictRewritten": False,
                             "alreadyClean": True})
            continue
        current = fleet.snapshot("audited-physical-scene", quiescent=True)
        require(recovery_scene_matches(current, plan),
                "retained fleet drifted from its exact archived scene; no recovery performed")
        fleet.record("retained-service-logs", fleet.remote(plan["node"], "logs"))
        if plan.get("kind") == "performance-no-fault-applied":
            for node in plan["nodes"]:
                proof = fleet.record("audited-finalization", {"node": node,
                    **fleet.remote(node, "cleanup")})
                require(proof.get("cleanup") == "PASS" and proof.get("unitStopped") is True
                        and proof.get("rootsRemoved") is True,
                        "performance retained environment finalization failed")
        elif plan.get("kind") == "owned-quota":
            for fault in plan["faults"]:
                fleet.record("quota-original-shard-rescue", fleet.remote(plan["node"], "recover-fault", **fault))
            require(physical_content(fleet.snapshot("complete-pre-fault-rescue", quiescent=True)) == physical_content(plan["before"]),
                    "quota rescue did not restore every original file; preserve namespace")
            proof = fleet.record("audited-finalization", fleet.remote(plan["node"], "quota-audit-finalize", **{
                key: plan[key] for key in ("bucket", "binarySha256", "expectedSceneSha256")}))
        else:
            proof = fleet.record("audited-finalization", fleet.remote(plan["node"], "audit-finalize", **{
                key: plan[key] for key in ("disk", "rejectedFaultId", "binarySha256", "expectedSceneSha256")}))
        for node in plan["nodes"]:
            if node != plan["node"]:
                fleet.record("unchanged-peer-finalization", fleet.remote(node, "cleanup"))
        require(proof.get("cleanup") == "PASS", "audited environment finalization failed")
        complete.append({"failedRunId": args.failed_run_id, "runId": plan["runId"], "cleanup": "PASS", "originalVerdictRewritten": False})
    save(args.evidence / "recovery-summary.json", {"cleanup": "PASS", "environments": complete, "canonicalPassClaimed": False})


if __name__ == "__main__":
    main()
