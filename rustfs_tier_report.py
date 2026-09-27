#!/usr/bin/env python3
"""Build and gate the RustFS tier suite's structured per-case results."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


TOPOLOGIES = ("single-single", "single-multi")
CASES = (
    ("TIER-101", "TierAddValidation"),
    ("TIER-102", "TierListInfo"),
    ("TIER-103", "TierEditCredentials"),
    ("TIER-104", "TierRemove"),
    ("TIER-105", "TransitionRule"),
    ("TIER-106", "ManualTransitionJob"),
    ("TIER-107", "RestoreObject"),
    ("TIER-108", "NoncurrentTransition"),
    ("TIER-109", "ExpiryRules"),
    ("TIER-110", "TierStats"),
    ("TIER-111", "TransitionEvents"),
    ("TIER-112", "TieredObjectDelete"),
    ("TIER-113", "EmptyPrefixAddAfterTransition"),
    ("TIER-114", "ReferencedTierForceRemove"),
    ("EVT-101", "TargetCrud"),
    ("EVT-102", "BucketRuleCrud"),
    ("EVT-103", "ObjectCreatedDelivery"),
    ("EVT-104", "EventKinds"),
    ("EVT-105", "PrefixSuffixFilter"),
    ("EVT-106", "OfflineRetry"),
    ("EVT-107", "TargetReset"),
    ("EVT-108", "NotifyModuleSwitch"),
    ("AUD-101", "AuditTargetCrud"),
    ("AUD-102", "S3OpsAudited"),
    ("AUD-103", "FailedRequestAudited"),
    ("AUD-104", "AdminOpsAudited"),
    ("AUD-105", "AuditOfflineRetry"),
    ("AUD-106", "SensitiveRedaction"),
    ("AUD-107", "AuditModuleSwitch"),
)
VALID_STATUSES = {"PASS", "FAIL", "UNSUPPORTED", "RUNNING"}


@dataclass(frozen=True)
class Validation:
    rows: list[dict]
    errors: list[str]


def _key(result: dict) -> tuple[str, str]:
    return str(result.get("topology", "")), str(result.get("case_id", ""))


def load_results(results_dir: Path) -> tuple[dict[tuple[str, str], dict], list[str]]:
    records: dict[tuple[str, str], dict] = {}
    errors: list[str] = []
    if not results_dir.is_dir():
        return records, [f"results directory is missing: {results_dir}"]

    for path in sorted(results_dir.glob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            errors.append(f"invalid/truncated result {path.name}: {exc}")
            continue
        if not isinstance(value, dict):
            errors.append(f"result {path.name} is not a JSON object")
            continue
        key = _key(value)
        if not all(key):
            errors.append(f"result {path.name} has an empty topology/case_id")
            continue
        if key in records:
            errors.append(f"duplicate result key: {key[0]}/{key[1]}")
            continue
        records[key] = value
    return records, errors


def validate_results(
    results_dir: Path,
    topologies: Iterable[str] = TOPOLOGIES,
    allowed_unsupported: Iterable[str] = (),
) -> Validation:
    selected = tuple(topologies)
    allow = set(allowed_unsupported)
    expected = {(topology, case_id) for topology in selected for case_id, _ in CASES}
    names = dict(CASES)
    records, errors = load_results(results_dir)

    for topology in selected:
        if topology not in TOPOLOGIES:
            errors.append(f"unknown expected topology: {topology}")

    for key in sorted(set(records) - expected):
        errors.append(f"unexpected result key: {key[0]}/{key[1]}")
    for key in sorted(expected - set(records)):
        errors.append(f"missing result: {key[0]}/{key[1]}")

    rows: list[dict] = []
    for topology in selected:
        for case_id, expected_name in CASES:
            key = (topology, case_id)
            record = records.get(key)
            if record is None:
                rows.append(
                    {
                        "topology": topology,
                        "case_id": case_id,
                        "name": expected_name,
                        "status": "MISSING",
                        "cleanup_status": "MISSING",
                    }
                )
                continue

            status = record.get("status")
            cleanup_status = record.get("cleanup_status")
            if record.get("schema_version") != 1:
                errors.append(f"{topology}/{case_id}: unsupported schema_version")
            if status not in VALID_STATUSES:
                errors.append(f"{topology}/{case_id}: invalid status {status!r}")
            elif status == "FAIL":
                errors.append(f"{topology}/{case_id}: case failed")
            elif status == "RUNNING":
                errors.append(f"{topology}/{case_id}: result remained RUNNING")
            elif status == "UNSUPPORTED":
                qualified = f"{topology}:{case_id}"
                if case_id not in allow and qualified not in allow:
                    errors.append(f"{topology}/{case_id}: unsupported without allowlist")
            if cleanup_status != "PASS":
                errors.append(f"{topology}/{case_id}: cleanup_status={cleanup_status!r}")
            if status == "PASS" and record.get("case_rc") != 0:
                errors.append(f"{topology}/{case_id}: PASS has case_rc={record.get('case_rc')!r}")
            rows.append(record)
    return Validation(rows=rows, errors=errors)


def _escape(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_markdown(validation: Validation, provenance: dict | None = None) -> str:
    counts: dict[str, int] = {}
    for row in validation.rows:
        status = str(row.get("status", "MISSING"))
        counts[status] = counts.get(status, 0) + 1

    lines = [
        "## Case Summary",
        "",
        f"- Total: {len(validation.rows)}",
        f"- PASS: {counts.get('PASS', 0)}",
        f"- FAIL: {counts.get('FAIL', 0)}",
        f"- UNSUPPORTED: {counts.get('UNSUPPORTED', 0)}",
        f"- RUNNING/MISSING: {counts.get('RUNNING', 0) + counts.get('MISSING', 0)}",
    ]
    if provenance:
        rc = provenance.get("rc", {}) if isinstance(provenance, dict) else {}
        lines.extend(
            [
                "",
                "## Client provenance",
                "",
                f"- Path: `{_escape(rc.get('path', 'missing'))}`",
                f"- Version: `{_escape(rc.get('version', 'missing'))}`",
                f"- SHA-256: `{_escape(rc.get('sha256', 'missing'))}`",
                f"- Config dir: `{_escape(rc.get('config_dir', 'missing'))}`",
            ]
        )

    lines.extend(
        [
            "",
            "| Topology | Case | Name | Status | Cleanup |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in validation.rows:
        lines.append(
            "| {topology} | {case_id} | {name} | {status} | {cleanup_status} |".format(
                **{key: _escape(row.get(key, "")) for key in ("topology", "case_id", "name", "status", "cleanup_status")}
            )
        )
    if validation.errors:
        lines.extend(["", "## Gate failures", ""])
        lines.extend(f"- {_escape(error)}" for error in validation.errors)
    return "\n".join(lines) + "\n"


def read_provenance(path: Path | None) -> tuple[dict | None, list[str]]:
    if path is None:
        return None, []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return None, [f"invalid/missing provenance: {exc}"]
    if not isinstance(value, dict):
        return None, ["provenance is not a JSON object"]
    rc = value.get("rc")
    if not isinstance(rc, dict) or not all(rc.get(field) for field in ("path", "version", "sha256", "config_dir")):
        return value, ["provenance is missing rc path/version/sha256/config_dir"]
    return value, []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provenance", type=Path)
    parser.add_argument("--topology", action="append", choices=TOPOLOGIES)
    parser.add_argument("--allow-unsupported", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    topologies = tuple(args.topology or TOPOLOGIES)
    validation = validate_results(args.results_dir, topologies, args.allow_unsupported)
    provenance, provenance_errors = read_provenance(args.provenance)
    validation.errors.extend(provenance_errors)
    report = render_markdown(validation, provenance)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    return 1 if validation.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
