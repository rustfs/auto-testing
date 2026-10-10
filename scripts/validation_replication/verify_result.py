#!/usr/bin/env python3
"""Check one replication case, fixture, and logical isolation outcome."""

from __future__ import annotations

import json
from pathlib import Path
import sys


def verify(case_id: str, evidence_dir: Path) -> None:
    result = json.loads((evidence_dir / "result.json").read_text(encoding="utf-8"))
    manifest = json.loads((evidence_dir / "run-manifest.json").read_text(encoding="utf-8"))
    cases = result["status"]["cases"]
    if result["status"]["state"] != "pass" or len(cases) != 1:
        raise ValueError("replication run did not pass exactly one case")
    case = cases[0]
    if case["id"] != case_id or case["state"] != "pass":
        raise ValueError("replication result does not pass selected case")
    if manifest["executionPlan"]["scoredCases"] != [case_id]:
        raise ValueError("replication execution plan differs from selected case")
    if manifest["isolationOutcome"]["state"] != "pass" or manifest["isolationOutcome"].get("trackedBuckets"):
        raise ValueError("replication bucket isolation did not pass")
    if any(outcome["state"] != "pass" for outcome in manifest["fixtureOutcomes"]):
        raise ValueError("replication fixture did not pass")
    if not case["evidence"]:
        raise ValueError("replication case has no assertion evidence")
    root = evidence_dir.resolve(strict=True)
    for name in case["evidence"]:
        path = (root / name).resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError(f"unsafe replication evidence path: {name}")


def main() -> int:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} CASE_ID EVIDENCE_DIR", file=sys.stderr)
        return 2
    try:
        verify(sys.argv[1], Path(sys.argv[2]))
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"replication verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
