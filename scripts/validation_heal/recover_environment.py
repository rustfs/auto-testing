#!/usr/bin/env python3
"""Audit a failed run's rejected overlength fault against its archived physical scene."""

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


def recovery_plan(artifact, failed_run, candidate):
    require(type(failed_run) is int and failed_run > 0, "exact failed workflow run required")
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
        fleet.record("archived-recovery-plan", {key: value for key, value in plan.items() if key != "before"})
        current = fleet.snapshot("audited-physical-scene", quiescent=True)
        require(physical_content(current) == physical_content(plan["before"]), "retained fleet drifted from archived pre-fault scene; no cleanup performed")
        fleet.record("retained-service-logs", fleet.remote(plan["node"], "logs"))
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
