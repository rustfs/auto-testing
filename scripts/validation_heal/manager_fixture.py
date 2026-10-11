"""Consume hosted manager evidence as scoped canonical-case prerequisites."""

import argparse
import json
from pathlib import Path
import re

try:
    from . import manager_native as suite
except ImportError:
    import manager_native as suite


SCOPES = {
    "heal.checkpoint-and-mrf-corruption": (
        "checkpoint-truncated", "checkpoint-future", "checkpoint-digest", "checkpoint-stale-manager",
        "checkpoint-page-order", "mrf-payload-integrity", "mrf-manifest-write-failure", "mrf-payload-write-failure",
        "mrf-migration-boundaries", "root-uncertain-publication"),
    "heal.bucket-incarnation-recreation": (
        "incarnation-root-replay", "incarnation-deferred-metadata", "incarnation-checkpoint"),
    "heal.outcome-counters-and-error-truth": ("outcome-truth", "outcome-wrong-identity"),
}


def verify(directory, candidate, case_id):
    suite.native.require(case_id in SCOPES, "manager native suite has no reviewed scope for this canonical case")
    directory = Path(directory)
    api = json.loads((directory / "tool-run.json").read_text())
    suite.native.require(type(api.get("id")) is int and api["id"] > 0
        and type(api.get("run_attempt")) is int and api["run_attempt"] > 0
        and api.get("status") == "completed" and api.get("conclusion") == "success"
        and isinstance(api.get("head_sha"), str) and re.fullmatch(r"[0-9a-f]{40}", api["head_sha"])
        and api.get("path") in {".github/workflows/rustfs-functional-chain.yml", ".github/workflows/rustfs-validation-tools.yml"},
        "manager artifact has no exact successful hosted tools build attempt")
    tools, evidence = directory / "manager-tools", directory / "manager-evidence"
    manifest = json.loads((tools / "manager-native.json").read_text())
    proof = suite.verify_evidence(tools, evidence, candidate, api["id"], api["run_attempt"], manifest.get("testingCommit"))
    scoped = []
    for key in SCOPES[case_id]:
        value = suite.REGRESSIONS[key]
        suite.native.require(value["relatedCases"] == [case_id], "manager native mapping changed canonical scope")
        scoped.append({"regression": key, **value})
    return {**proof, "caseId": case_id, "runId": api["id"], "runAttempt": api["run_attempt"],
        "driverCommit": api["head_sha"], "collectorCommit": manifest["testingCommit"],
        "casePrerequisiteTests": scoped, "originalEvidenceDirectory": str(evidence.resolve()),
        "individualFsyncHooksClaimed": False, "physicalPowerLossClaimed": False,
        "emittedBarrierLogsClaimed": False, "canonicalFleetBranchesExecuted": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--case-id", choices=tuple(SCOPES), required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory, json.loads(args.candidate.read_text()), args.case_id), indent=2))


if __name__ == "__main__": main()
