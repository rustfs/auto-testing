"""Verify C11's candidate-bound physical storage and final-rename prerequisites."""

import argparse
import json
from pathlib import Path
import re

try:
    from . import native_regressions as suite
except ImportError:
    import native_regressions as suite


CASE_ID = "heal.bucket-incarnation-recreation"


def verify(directory, candidate, helper_sha):
    directory = Path(directory)
    api = json.loads((directory / "tool-run.json").read_text())
    suite.require(type(api.get("id")) is int and api["id"] > 0
        and type(api.get("run_attempt")) is int and api["run_attempt"] > 0
        and api.get("status") == "completed" and api.get("conclusion") == "success"
        and isinstance(api.get("head_sha"), str) and re.fullmatch(r"[0-9a-f]{40}", api["head_sha"])
        and api.get("path") in {".github/workflows/rustfs-functional-chain.yml", ".github/workflows/rustfs-validation-tools.yml"},
        "incarnation artifact has no exact successful hosted build attempt")
    suite.require(candidate.get("source_sha") == suite.INCARNATION_CANDIDATE,
                  "incarnation commit barrier requires its exact reviewed source")
    tools, evidence = directory / "native-tools", directory / "native-evidence"
    manifest = json.loads((tools / "native-regressions.json").read_text())
    proof = suite.verify_evidence(tools, evidence, candidate, api["id"], api["run_attempt"], helper_sha)
    suite.require(proof["observedBarrierEvents"].get("incarnation-final-rename") == suite.INCARNATION_EVENTS,
                  "actual final rename, cancellation, physical drain and successor refusal were not observed")
    return {**proof, "caseId": CASE_ID, "runId": api["id"], "runAttempt": api["run_attempt"],
        "driverCommit": api["head_sha"], "collectorCommit": manifest["testingCommit"],
        "casePrerequisiteTests": [{"regression": key, **value} for key, value in suite.INCARNATION_REGRESSIONS.items()],
        "exactStorageCommitBarrierObserved": True, "canonicalFleetBranchesExecuted": False,
        "checkpointMrfReplayClaimed": False, "physicalPowerLossClaimed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--testing-commit", required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory, json.loads(args.candidate.read_text()), args.testing_commit), indent=2))


if __name__ == "__main__": main()
