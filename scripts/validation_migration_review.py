#!/usr/bin/env python3
"""Audit test implementation coverage independently of candidate live results."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import sys

try:
    from .validation_catalog import validate_snapshot
    from .validation_heal.execution_policy import ENVIRONMENT_GATED_CASES
except ImportError:
    from validation_catalog import validate_snapshot
    from validation_heal.execution_policy import ENVIRONMENT_GATED_CASES

ROOT = Path(__file__).resolve().parent.parent
IMPLEMENTATION_STATES = {"implemented", "environment-gated", "partial"}
LIVE_STATES = {"accepted", "retained-failure", "pending", "blocked", "disabled"}
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def require(condition: object, message: str) -> None:
    if not condition:
        raise ValueError(message)


def local_file(root: Path, name: str) -> Path:
    require(isinstance(name, str) and bool(name), "missing implementation reference")
    path = Path(name)
    require(not path.is_absolute() and ".." not in path.parts, f"nonlocal implementation reference: {name}")
    result = root / path
    require(result.is_file() and not result.is_symlink()
            and result.resolve().is_relative_to(root.resolve()), f"missing implementation reference: {name}")
    return result


def validate_review(review: dict, catalog: dict, root: Path = ROOT) -> None:
    """Validate an explicit review; never derive implementation state from FAIL."""
    require(isinstance(review, dict), "implementation review must be an object")
    validate_snapshot(catalog)
    require(isinstance(review.get("as_of"), str), "missing implementation review date")
    require(date.fromisoformat(review["as_of"]).isoformat() == review["as_of"], "invalid implementation review date")
    require(review.get("schema_version") == 1, "unsupported implementation review schema")
    require(review.get("source_commit") == catalog["source_commit"]
            and review.get("source_sha256") == catalog["source_sha256"], "implementation review uses another source catalog")
    require(isinstance(review.get("reviewed_testing_commit"), str)
            and HEX40.fullmatch(review["reviewed_testing_commit"]), "missing immutable review base")
    files = review.get("reviewed_files")
    require(isinstance(files, dict) and bool(files), "missing reviewed implementation files")
    for name, digest in files.items():
        require(isinstance(digest, str) and HEX64.fullmatch(digest), f"invalid review digest: {name}")
        require(hashlib.sha256(local_file(root, name).read_bytes()).hexdigest() == digest,
                f"implementation changed since review: {name}")
    recipes = json.loads((root / "scripts/validation_heal/recipes.json").read_text())
    variants = {case["id"]: set(case["variants"]) for case in recipes["cases"]}
    catalog_cases = {case["id"]: case for module in catalog["modules"] for case in module["cases"]}
    cases = review.get("cases")
    require(isinstance(cases, list), "missing implementation case reviews")
    seen = set()
    for case in cases:
        require(isinstance(case, dict), "implementation case review must be an object")
        case_id = case.get("case_id")
        require(isinstance(case_id, str) and case_id in catalog_cases, f"unknown implementation case: {case_id}")
        require(case_id not in seen, f"duplicate implementation case: {case_id}")
        seen.add(case_id)
        state = case.get("implementation")
        require(isinstance(state, str) and state in IMPLEMENTATION_STATES, f"invalid implementation state: {case_id}")
        live = case.get("live", {})
        require(isinstance(live, dict) and isinstance(live.get("state"), str) and live["state"] in LIVE_STATES, f"invalid live state: {case_id}")
        require(isinstance(case.get("basis"), str) and bool(case["basis"].strip()), f"missing review basis: {case_id}")
        require(isinstance(case.get("references"), list) and bool(case["references"]), f"missing implementation references: {case_id}")
        for name in case["references"]:
            local_file(root, name)
            require(name in files, f"unreviewed implementation reference: {case_id}: {name}")
        require(isinstance(case.get("gaps"), list), f"missing explicit gap list: {case_id}")
        if state == "partial":
            require(bool(case["gaps"]) and all(isinstance(gap, str) and gap.strip() for gap in case["gaps"]),
                    f"partial implementation needs actionable gaps: {case_id}")
        else:
            require(not case["gaps"], f"implemented case still has implementation gaps: {case_id}")
        if state == "environment-gated":
            policy = ENVIRONMENT_GATED_CASES.get(case_id)
            require(policy is not None and policy["disabledByDefault"] is True
                    and live["state"] == "disabled", f"invalid disabled environment procedure: {case_id}")
            require(case.get("required_capabilities") == policy["requiredCapabilities"]
                    and case.get("reason_code") == policy["reasonCode"], f"environment requirements drifted: {case_id}")
        else:
            require(case_id not in ENVIRONMENT_GATED_CASES, f"environment case lost its gate: {case_id}")
        if live["state"] == "accepted":
            require(state == "implemented", f"accepted live case lacks complete implementation: {case_id}")
            runs = live.get("runs")
            require(isinstance(runs, list) and bool(runs)
                    and all(type(run) is int and run > 0 for run in runs), f"missing accepted run identity: {case_id}")
            require(live.get("basis") == "independent-complete-live-audit", f"unsupported acceptance claim: {case_id}")
        elif state != "environment-gated":
            require(isinstance(live.get("finding"), str) and bool(live["finding"].strip()), f"missing failure scope: {case_id}")
            source_variants = case.get("source_variants")
            require(isinstance(source_variants, dict) and set(source_variants) == variants.get(case_id, {"catalog-assertions"}),
                    f"source branch coverage changed: {case_id}")
            for branch, references in source_variants.items():
                require(isinstance(references, list) and (bool(references) or state == "partial"),
                        f"source branch has no implementation: {case_id}: {branch}")
                require(all(name in case["references"] for name in references), f"unreviewed branch reference: {case_id}: {branch}")
    require(seen == set(catalog_cases), f"missing implementation reviews: {sorted(set(catalog_cases) - seen)}")


def summarize(review: dict) -> dict:
    cases = review["cases"]
    implementation = Counter(case["implementation"] for case in cases)
    live = Counter(case["live"]["state"] for case in cases)
    return {
        "as_of": review["as_of"],
        "total": len(cases),
        "implementation": {state: implementation[state] for state in sorted(IMPLEMENTATION_STATES)},
        "live": {state: live[state] for state in sorted(LIVE_STATES)},
        "remaining_implementation": [{"case_id": case["case_id"], "gaps": case["gaps"]}
                                     for case in cases if case["implementation"] == "partial"],
        "all_implementation_complete": implementation["partial"] == 0,
        "environment_adapters_required": [case["case_id"] for case in cases
                                          if case["implementation"] == "environment-gated"],
        "all_cases_live_accepted": live["accepted"] == len(cases),
        "candidate_pass_claimed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--review", type=Path, default=ROOT / "validation-migration-review.json")
    parser.add_argument("--catalog", type=Path, default=ROOT / "validation-cases.json")
    parser.add_argument("--require-complete", action="store_true",
                        help="fail on implementation gaps; candidate FAIL never changes this decision")
    args = parser.parse_args()
    try:
        review = json.loads(args.review.read_text())
        catalog = json.loads(args.catalog.read_text())
        validate_review(review, catalog)
        summary = summarize(review)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return int(args.require_complete and not summary["all_implementation_complete"])
    except (OSError, ValueError) as exc:
        print(f"implementation review invalid: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
