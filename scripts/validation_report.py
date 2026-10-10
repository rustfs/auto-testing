#!/usr/bin/env python3
"""Report migration progress and gate verified validation case results."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys

try:
    from .validation_catalog import validate_snapshot
except ImportError:
    from validation_catalog import validate_snapshot


TERMINAL = {"PASS", "FAIL", "INFRA_ERROR", "BLOCKED", "UNSUPPORTED"}
ALL_VERDICTS = TERMINAL | {"RUNNING"}
GATE_SOURCE_STATUSES = {"implemented", "procedure-ready", "planned"}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"[0-9a-f]{40}\Z")


def load_results(directory: Path) -> tuple[dict[str, dict], list[str]]:
    records: dict[str, dict] = {}
    errors: list[str] = []
    if not directory.is_dir():
        return records, [f"missing results directory: {directory}"]
    for path in sorted(directory.glob("*.json")):
        try:
            result = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            errors.append(f"invalid result {path.name}: {exc}")
            continue
        if not isinstance(result, dict):
            errors.append(f"result is not an object: {path.name}")
            continue
        case_id = result.get("case_id")
        if not isinstance(case_id, str) or not case_id:
            errors.append(f"result has no case_id: {path.name}")
        elif case_id in records:
            errors.append(f"duplicate result: {case_id}")
        else:
            records[case_id] = result
    return records, errors


def check_result(
    case: dict, result: dict, *, gate: bool, artifacts_root: Path,
    target_environments: list[str],
) -> list[str]:
    case_id = case["id"]
    errors: list[str] = []
    if result.get("schema_version") != 1:
        errors.append(f"{case_id}: unsupported result schema")
    verdict = result.get("verdict")
    if verdict not in ALL_VERDICTS:
        errors.append(f"{case_id}: invalid verdict {verdict!r}")
    elif verdict == "RUNNING":
        errors.append(f"{case_id}: result remained RUNNING")
    if result.get("source_status") != case["source_status"]:
        errors.append(f"{case_id}: source status does not match catalog")
    profile = result.get("profile")
    if not isinstance(profile, str) or not profile:
        errors.append(f"{case_id}: missing environment profile")
    elif target_environments and profile not in target_environments:
        errors.append(f"{case_id}: profile {profile!r} is outside source environments")
    if result.get("migration_status") not in {"unverified", "verified"}:
        errors.append(f"{case_id}: invalid migration status")
    if verdict == "PASS":
        if result.get("migration_status") != "verified":
            errors.append(f"{case_id}: PASS without verified migration")
        evidence = result.get("assertions")
        if not isinstance(evidence, list) or len(evidence) != len(case["assertions"]):
            errors.append(f"{case_id}: PASS lacks one result per source assertion")
        else:
            for index, (source_assertion, item) in enumerate(zip(case["assertions"], evidence), 1):
                if (
                    not isinstance(item, dict)
                    or item.get("source_assertion") != source_assertion
                    or item.get("status") != "PASS"
                    or not isinstance(item.get("evidence"), list)
                    or not item["evidence"]
                    or any(not isinstance(path, str) or not path for path in item["evidence"])
                ):
                    errors.append(f"{case_id}: assertion {index} lacks matching source text or evidence")
                elif any(
                    Path(path).is_absolute()
                    or ".." in Path(path).parts
                    or not (artifacts_root / path).is_file()
                    or not (artifacts_root / path).resolve().is_relative_to(artifacts_root.resolve())
                    for path in item["evidence"]
                ):
                    errors.append(f"{case_id}: assertion {index} has missing or unsafe evidence paths")
        if result.get("cleanup_status") != "PASS":
            errors.append(f"{case_id}: PASS requires clean environment")
        provenance = result.get("provenance")
        if not isinstance(provenance, dict) or not (
            isinstance(provenance.get("candidate_sha256"), str)
            and SHA256.fullmatch(provenance["candidate_sha256"])
            and isinstance(provenance.get("binary_sha256"), str)
            and SHA256.fullmatch(provenance["binary_sha256"])
            and isinstance(provenance.get("candidate_url"), str)
            and bool(provenance["candidate_url"])
            and isinstance(provenance.get("source_commit"), str)
            and COMMIT.fullmatch(provenance["source_commit"])
            and isinstance(provenance.get("testing_commit"), str)
            and COMMIT.fullmatch(provenance["testing_commit"])
            and isinstance(provenance.get("rc_path"), str)
            and Path(provenance["rc_path"]).is_absolute()
            and isinstance(provenance.get("rc_sha256"), str)
            and SHA256.fullmatch(provenance["rc_sha256"])
        ):
            errors.append(f"{case_id}: PASS lacks candidate and client provenance")
    if gate and verdict != "PASS":
        errors.append(f"{case_id}: gate verdict is {verdict!r}")
    if verdict in {"BLOCKED", "UNSUPPORTED", "INFRA_ERROR", "FAIL"} and not result.get("reason"):
        errors.append(f"{case_id}: {verdict} requires a reason")
    if verdict in {"PASS", "FAIL", "INFRA_ERROR"} and result.get("cleanup_status") not in {"PASS", "FAIL"}:
        errors.append(f"{case_id}: terminal result requires known cleanup status")
    return errors


def validate_results(catalog: dict, directory: Path, *, gate_statuses: set[str]) -> tuple[list[dict], list[str]]:
    validate_snapshot(catalog)
    cases = {
        case["id"]: (case, module["target_environments"])
        for module in catalog["modules"] for case in module["cases"]
    }
    records, errors = load_results(directory)
    for case_id in sorted(set(records) - set(cases)):
        errors.append(f"unexpected case ID: {case_id}")
    rows = []
    for case_id, (case, target_environments) in cases.items():
        result = records.get(case_id)
        gated = case["source_status"] in gate_statuses
        if result is None:
            if gated:
                errors.append(f"missing gate result: {case_id}")
            rows.append({"case_id": case_id, "source_status": case["source_status"], "verdict": "MISSING"})
            continue
        errors.extend(check_result(
            case, result, gate=gated, artifacts_root=directory.parent,
            target_environments=target_environments,
        ))
        rows.append({"case_id": case_id, "source_status": case["source_status"], "verdict": result.get("verdict")})
    return rows, errors


def render_report(rows: list[dict], errors: list[str]) -> str:
    counts = Counter(row["verdict"] for row in rows)
    lines = ["# Validation migration report", "", f"Cases: {len(rows)}", ""]
    lines.extend(f"- {status}: {counts.get(status, 0)}" for status in ("PASS", "FAIL", "INFRA_ERROR", "BLOCKED", "UNSUPPORTED", "RUNNING", "MISSING"))
    lines.extend(["", "| Case | Source status | Verdict |", "| --- | --- | --- |"])
    for row in rows:
        lines.append(f"| {row['case_id']} | {row['source_status']} | {row['verdict']} |")
    if errors:
        lines.extend(["", "## Validation errors", ""])
        lines.extend(f"- {error}" for error in errors)
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=Path(__file__).resolve().parent.parent / "validation-cases.json")
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--gate-status", action="append", choices=sorted(GATE_SOURCE_STATUSES), default=[])
    args = parser.parse_args()
    try:
        catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
        rows, errors = validate_results(catalog, args.results_dir, gate_statuses=set(args.gate_status))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(render_report(rows, errors), encoding="utf-8")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"validation report error: {exc}", file=sys.stderr)
        return 1
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
