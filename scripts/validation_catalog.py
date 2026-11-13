#!/usr/bin/env python3
"""Snapshot and validate the release-validation case catalog."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


EXPECTED_STATUS_COUNTS = {
    "implemented": 66,
    "procedure-ready": 84,
    "planned": 8,
}
COMMIT = re.compile(r"[0-9a-f]{40}\Z")

# These are places to inspect, not evidence that a validation assertion passes.
MODULE_CANDIDATES = {
    "pool-lifecycle": ["rustfs_pool_expand.sh"],
    "heal-scanner": ["rustfs_heal_test.sh", "rustfs-fault-tolerance-test.sh"],
    "heal-extended": ["rustfs_heal_test.sh", "rustfs-fault-tolerance-test.sh"],
    "ilm-expiration": ["rustfs-tier-test.sh", "rustfs-s3-compat-test.sh"],
    "bucket-replication": ["rustfs-replication-test.sh"],
    "site-replication": ["rustfs-replication-test.sh"],
    "kms": ["rustfs-kms-test.sh"],
    "data-services": ["rustfs-tier-test.sh"],
    "identity-access": ["rustfs-security-test.sh"],
    "s3-compatibility": ["rustfs-s3-compat-test.sh"],
}

CASE_CANDIDATES = {
    "scanner.usage-convergence": ["STOR-134"],
    "ilm.configuration-roundtrip": ["S3C-704"],
    "ilm.transition-and-restore": ["TIER-105", "TIER-107"],
    "bucket-replication.source-versioning-required": ["REP-103"],
    "bucket-replication.put-object": ["REP-104"],
    "bucket-replication.delete-marker": ["REP-105"],
    "bucket-replication.existing-object-enabled": ["REP-107"],
    "bucket-replication.multipart-object": ["REP-108"],
    "site-replication.add-and-info": ["SITE-101"],
    "site-replication.object-bidirectional": ["SITE-103"],
    "site-replication.bucket-policy-metadata": ["SITE-104"],
    "site-replication.resync": ["SITE-105"],
    "site-replication.remove-all": ["SITE-106"],
    "kms.sse-kms-round-trip": ["KMS-104", "KMS-301", "KMS-214"],
    "tiering.transition-and-restore": ["TIER-105", "TIER-107"],
    "events.delivery": ["EVT-103", "EVT-104"],
    "audit.request-correlation": ["AUD-102", "AUD-104", "AUD-106"],
    "iam.policy-enforcement": ["IAM-105", "IAM-109"],
    "oidc.login-and-session": ["OIDC-103"],
    "s3.basic-object-round-trip": [
        "S3C-101", "S3C-201", "S3C-203", "S3C-205", "S3C-206", "S3C-207",
    ],
}

CASE_SUITES = {
    "scanner.usage-convergence": ["rustfs-storage-test.sh"],
}


def catalog_modules(snapshot: dict) -> list[dict]:
    """Include local executors without rewriting the immutable source snapshot."""
    return snapshot["modules"] + snapshot.get("local_modules", [])


def validate_snapshot(snapshot: dict) -> None:
    if snapshot.get("schema") != 1:
        raise ValueError("unsupported validation catalog schema")
    if not isinstance(snapshot.get("source_sha256"), str) or len(snapshot["source_sha256"]) != 64:
        raise ValueError("missing source catalog SHA-256")
    if not isinstance(snapshot.get("source_commit"), str) or not COMMIT.fullmatch(snapshot["source_commit"]):
        raise ValueError("missing source commit")

    modules = snapshot.get("modules")
    if not isinstance(modules, list):
        raise ValueError("modules must be a list")
    ids: set[str] = set()
    statuses: Counter[str] = Counter()
    for module in modules:
        if not module.get("id") or not isinstance(module.get("cases"), list):
            raise ValueError("invalid module")
        for case in module["cases"]:
            case_id = case.get("id")
            if not isinstance(case_id, str) or not case_id:
                raise ValueError("case without an ID")
            if case_id in ids:
                raise ValueError(f"duplicate case ID: {case_id}")
            ids.add(case_id)
            status = case.get("source_status")
            if status not in EXPECTED_STATUS_COUNTS:
                raise ValueError(f"invalid source status for {case_id}: {status}")
            statuses[status] += 1
            if not isinstance(case.get("assertions"), list) or not case["assertions"]:
                raise ValueError(f"missing assertions: {case_id}")
            candidates = case.get("reuse_candidates")
            if not isinstance(candidates, dict) or not isinstance(candidates.get("suites"), list):
                raise ValueError(f"missing reuse assessment: {case_id}")
            if not isinstance(candidates.get("case_ids"), list):
                raise ValueError(f"invalid reuse case IDs: {case_id}")
    if dict(statuses) != EXPECTED_STATUS_COUNTS:
        raise ValueError(f"unexpected case counts: {dict(statuses)}")
    local_modules = snapshot.get("local_modules", [])
    if not isinstance(local_modules, list):
        raise ValueError("local_modules must be a list")
    for module in local_modules:
        if module.get("origin") != "auto-testing" or not module.get("id") or not isinstance(module.get("cases"), list):
            raise ValueError("invalid local validation module")
        for case in module["cases"]:
            case_id = case.get("id")
            if not isinstance(case_id, str) or not case_id or case_id in ids:
                raise ValueError(f"duplicate or invalid local case ID: {case_id}")
            ids.add(case_id)
            if case.get("source_status") not in EXPECTED_STATUS_COUNTS or not case.get("assertions"):
                raise ValueError(f"invalid local assertion contract: {case_id}")
            if case.get("probes", {}).get("executionMode") != "configured-endpoint":
                raise ValueError(f"local executor must declare explicit endpoint admission: {case_id}")
            candidates = case.get("reuse_candidates")
            if not isinstance(candidates, dict) or not isinstance(candidates.get("suites"), list) or not isinstance(candidates.get("case_ids"), list):
                raise ValueError(f"missing local reuse assessment: {case_id}")


def validate_reuse_candidates(snapshot: dict, checkout: Path) -> None:
    for module in catalog_modules(snapshot):
        for case in module["cases"]:
            candidates = case["reuse_candidates"]
            suites = candidates["suites"]
            contents = []
            for suite in suites:
                if Path(suite).name != suite or not (checkout / suite).is_file():
                    raise ValueError(f"missing reuse suite for {case['id']}: {suite}")
                contents.append((checkout / suite).read_text(encoding="utf-8"))
            for old_id in candidates["case_ids"]:
                marker = re.compile(r"\brun_test\s+" + re.escape(old_id) + r"\b")
                if not any(marker.search(content) for content in contents):
                    raise ValueError(f"missing reuse case for {case['id']}: {old_id}")


def build_snapshot(source: Path) -> dict:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required only to refresh the source snapshot") from exc

    raw = source.read_bytes()
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=source.parent.parent,
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    catalog = yaml.safe_load(raw)
    if catalog.get("kind") != "ScenarioCatalog":
        raise ValueError("source is not a ScenarioCatalog")
    modules = []
    for module in catalog["spec"]["modules"]:
        cases = []
        for source_case in module["cases"]:
            case = {
                "id": source_case["id"],
                "source_status": source_case["status"],
                "priority": source_case["priority"],
                "channels": source_case["channels"],
                "assertions": source_case["assertions"],
                "probes": source_case.get("probes", {}),
                "reuse_candidates": {
                    "suites": CASE_SUITES.get(source_case["id"], MODULE_CANDIDATES[module["id"]]),
                    "case_ids": CASE_CANDIDATES.get(source_case["id"], []),
                },
            }
            cases.append(case)
        modules.append({
            "id": module["id"],
            "target_environments": module.get("targetEnvironments", []),
            "required_capabilities": module.get("requiredCapabilities", []),
            "cases": cases,
        })
    snapshot = {
        "schema": 1,
        "source": "rustfs-release-validation/scenarios/catalog.yaml",
        "source_commit": source_commit,
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "modules": modules,
    }
    validate_snapshot(snapshot)
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("sync", "check", "summary"))
    parser.add_argument("--source", type=Path, help="release-validation scenarios/catalog.yaml")
    parser.add_argument(
        "--catalog", type=Path,
        default=Path(__file__).resolve().parent.parent / "validation-cases.json",
    )
    args = parser.parse_args()
    if args.command == "sync":
        if args.source is None:
            parser.error("sync requires --source")
        snapshot = build_snapshot(args.source)
        if args.catalog.is_file():
            local_modules = json.loads(args.catalog.read_text()).get("local_modules")
            if local_modules is not None:
                snapshot["local_modules"] = local_modules
                validate_snapshot(snapshot)
        args.catalog.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n")
        print(f"wrote {args.catalog}")
        return 0

    snapshot = json.loads(args.catalog.read_text())
    validate_snapshot(snapshot)
    validate_reuse_candidates(snapshot, Path(__file__).resolve().parent.parent)
    if args.source is not None:
        current_sha = hashlib.sha256(args.source.read_bytes()).hexdigest()
        if snapshot["source_sha256"] != current_sha:
            raise ValueError("source catalog changed; refresh the snapshot")
    if args.command == "summary":
        for module in snapshot["modules"]:
            statuses = Counter(case["source_status"] for case in module["cases"])
            print(f"{module['id']}: {len(module['cases'])} {dict(statuses)}")
    else:
        source_note = "source hash matched" if args.source is not None else "source checkout not supplied"
        local_count = sum(len(module["cases"]) for module in snapshot.get("local_modules", []))
        print(f"validation catalog: 158 source cases, statuses 66/84/8, {local_count} local cases, {source_note}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"validation catalog error: {exc}", file=sys.stderr)
        sys.exit(1)
