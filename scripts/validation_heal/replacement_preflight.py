#!/usr/bin/env python3
"""Run the replacement cases' read-only endpoint admission check before changes."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import re
import sys

try:
    from .healthy import Fleet
    from .heal_probe import save
except ImportError:
    from healthy import Fleet
    from heal_probe import save


def checked_identity(value: object) -> bool:
    return (isinstance(value, dict) and value.get("readOnly") is True
            and value.get("mutated") is False and value.get("canonicalPassClaimed") is False
            and isinstance(value.get("checks"), dict)
            and set(value["checks"]) == {
                "exactEndpointMounts", "mountIdsPresent", "filesystemIdentitiesDistinct",
                "endpointPhysicalDevicesDistinct", "endpointDevicesSeparateFromRoot",
                "mountSwitchAdapterReviewed",
            }
            and type(value.get("admitted")) is bool)


REPLACEMENT_CASES = {
    "heal.disk-replacement-formatted",
    "heal.disk-replacement-unformatted",
}


def run_preflight(nodes: list[str], evidence: Path, workflow_run_id: str, attempt: str,
                  testing_commit: str, candidate: dict,
                  case_id: str = "heal.disk-replacement-formatted") -> dict:
    if case_id not in REPLACEMENT_CASES:
        raise ValueError(f"unsupported replacement admission case: {case_id}")
    if not re.fullmatch(r"[1-9][0-9]*", workflow_run_id) or not re.fullmatch(r"[1-9][0-9]*", attempt):
        raise ValueError("replacement preflight requires the immutable workflow run and attempt")
    if not re.fullmatch(r"[0-9a-f]{40}", testing_commit):
        raise ValueError("replacement preflight requires the exact auto-testing commit")
    if (not re.fullmatch(r"[0-9a-f]{40}", str(candidate.get("source_sha", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(candidate.get("package_sha256", "")))
            or not isinstance(candidate.get("package_url"), str) or not candidate["package_url"]):
        raise ValueError("replacement preflight candidate package provenance is incomplete")
    if len(nodes) != 4 or len(set(nodes)) != 4 or any(
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes):
        raise ValueError("replacement preflight requires the four current auto-testing Heal nodes")

    results = []
    infrastructure_errors = []
    blocked = False
    for topology, selected in (("H1", nodes[:1]), ("H2", nodes)):
        run_id = f"gh-{workflow_run_id}-{attempt}-{topology.lower()}"
        directory = evidence / "replacement-admission" / topology
        directory.mkdir(parents=True, exist_ok=True)
        fleet = Fleet(selected, directory, run_id)
        observed = {}
        with ThreadPoolExecutor(max_workers=len(selected)) as executor:
            pending = {executor.submit(fleet.remote, node, "replacement-admission-preflight"): node
                       for node in selected}
            for future in as_completed(pending):
                node = pending[future]
                try:
                    value = future.result()
                    if not checked_identity(value):
                        infrastructure_errors.append({"topology": topology, "node": node,
                            "reason": "remote preflight response is incomplete or malformed"})
                        observed[node] = {"invalidResponse": True}
                    else:
                        observed[node] = value
                        blocked = blocked or value["admitted"] is not True
                except Exception as exc:
                    infrastructure_errors.append({"topology": topology, "node": node,
                        "reason": f"{type(exc).__name__}: {exc}"})
                    observed[node] = {"preflightError": f"{type(exc).__name__}: {exc}"}
        ordered = {node: observed[node] for node in selected}
        fleet.record("replacement-admission-preflight", {"topology": topology, "runId": run_id,
                      "readOnly": True, "mutated": False, "nodes": ordered})
        results.append({"topology": topology, "runId": run_id, "nodes": ordered,
                        "deploymentSkipped": True, "fixtureSeedSkipped": True})

    infrastructure_errors.sort(key=lambda item: (item["topology"], item["node"], item["reason"]))
    status = "INFRA_ERROR" if infrastructure_errors else "BLOCKED" if blocked else "ADMITTED"
    summary = {
        "schemaVersion": 1,
        "caseId": case_id,
        "status": status,
        "executionStage": "pre-deployment",
        "workflowRunId": int(workflow_run_id),
        "workflowRunAttempt": int(attempt),
        "testingCommit": testing_commit,
        "candidate": {"sourceSha": candidate["source_sha"], "packageUrl": candidate["package_url"],
                      "packageSha256": candidate["package_sha256"]},
        "preflightHelperSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "fleetNodeSha256": hashlib.sha256(Path(__file__).with_name("fleet_node.py").read_bytes()).hexdigest(),
        "mutationStarted": False,
        "replacementAdmissionBlocked": status == "BLOCKED",
        "deploymentSkipped": status != "ADMITTED",
        "fixtureSeedSkipped": status != "ADMITTED",
        "canonicalPassClaimed": False,
        "cleanup": "PASS",
        "cleanupDisposition": "NOT_REQUIRED_NO_MUTATION",
        "results": results,
        "infrastructureErrors": infrastructure_errors,
    }
    save(evidence / "healthy-summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", choices=sorted(REPLACEMENT_CASES),
                        default="heal.disk-replacement-formatted")
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    try:
        nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
        candidate = json.loads(args.candidate.read_text(encoding="utf-8"))
        summary = run_preflight(nodes, args.evidence, os.environ.get("GITHUB_RUN_ID", ""),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"), args.testing_commit, candidate,
            args.case_id)
        print(json.dumps({"status": summary["status"], "mutationStarted": False,
                          "replacementAdmissionBlocked": summary["replacementAdmissionBlocked"]}, sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"B03 pre-deployment preflight failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
