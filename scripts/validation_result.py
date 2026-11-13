#!/usr/bin/env python3
"""Atomically write one canonical release-validation case result."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

from validation_catalog import catalog_modules, validate_snapshot


def write_result(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    os.replace(temporary, path)


def case_from_catalog(catalog: dict, case_id: str) -> dict:
    validate_snapshot(catalog)
    for module in catalog_modules(catalog):
        for case in module["cases"]:
            if case["id"] == case_id:
                return case
    raise ValueError(f"unknown validation case: {case_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=Path(__file__).resolve().parent.parent / "validation-cases.json")
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--result-file", type=Path, required=True)
    parser.add_argument("--verdict", choices=("RUNNING", "PASS", "FAIL", "INFRA_ERROR", "BLOCKED"), required=True)
    parser.add_argument("--reason", default="")
    parser.add_argument("--cleanup-status", default="PENDING")
    parser.add_argument("--evidence", action="append", default=[])
    parser.add_argument("--candidate-manifest", type=Path)
    parser.add_argument("--rc-bin", type=Path)
    parser.add_argument("--testing-commit")
    parser.add_argument("--binary-sha256")
    parser.add_argument("--predeployment-blocked", action="store_true",
                        help="record a BLOCKED result before any candidate package was installed")
    args = parser.parse_args()
    if args.predeployment_blocked and (args.verdict != "BLOCKED"
                                      or args.case_id not in {
                                          "heal.disk-replacement-formatted",
                                          "heal.disk-replacement-unformatted",
                                      }):
        parser.error("--predeployment-blocked is reserved for replacement admission results")
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    case = case_from_catalog(catalog, args.case_id)
    result = {
        "schema_version": 1,
        "case_id": args.case_id,
        "source_status": case["source_status"],
        "migration_status": "unverified" if args.verdict in {"RUNNING", "BLOCKED", "INFRA_ERROR"} else "verified",
        "profile": args.profile,
        "verdict": args.verdict,
        "cleanup_status": args.cleanup_status,
    }
    if args.verdict != "RUNNING":
        if not args.candidate_manifest or not args.rc_bin or not args.testing_commit:
            parser.error("terminal results require candidate manifest, rc binary, and testing commit")
        candidate = json.loads(args.candidate_manifest.read_text(encoding="utf-8"))
        binary_sha256 = (None if args.predeployment_blocked else
                         args.binary_sha256 or candidate.get("binary_sha256"))
        bootstrap_error = args.verdict == "INFRA_ERROR" and binary_sha256 is None
        if not bootstrap_error and not args.predeployment_blocked and (not isinstance(binary_sha256, str) or len(binary_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in binary_sha256
        )):
            parser.error("terminal results require a valid installed binary SHA-256")
        if not args.predeployment_blocked and candidate.get("binary_sha256") and candidate["binary_sha256"] != binary_sha256:
            parser.error("installed binary SHA-256 differs from candidate manifest")
        rc_path = args.rc_bin.resolve(strict=True)
        result["provenance"] = {
            "candidate_url": candidate["package_url"],
            "candidate_sha256": candidate["package_sha256"],
            "binary_sha256": binary_sha256,
            "source_commit": candidate["source_sha"],
            "testing_commit": args.testing_commit,
            "rc_path": str(rc_path),
            "rc_sha256": hashlib.sha256(rc_path.read_bytes()).hexdigest(),
        }
        if args.predeployment_blocked:
            result["execution_stage"] = "pre-deployment"
            result["provenance"]["binary_sha256"] = None
        result["evidence"] = args.evidence
        if args.verdict == "PASS":
            if not args.evidence or args.cleanup_status != "PASS":
                parser.error("PASS requires evidence and clean finalization")
            result["assertions"] = [
                {"source_assertion": text, "status": "PASS", "evidence": args.evidence}
                for text in case["assertions"]
            ]
        else:
            result["reason"] = args.reason or "case or environment failed; inspect evidence"
    write_result(args.result_file, result)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"validation result error: {exc}", file=sys.stderr)
        sys.exit(1)
