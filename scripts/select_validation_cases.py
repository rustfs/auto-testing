#!/usr/bin/env python3
"""Choose the catalog cases a nightly validation lane executes.

Sweepable cases are the implemented catalog plus planned cases that already
have a lane executor. Planned heal.* cases stay on the interactive validation
job: they need validation-tools artifacts built on rustfs/rustfs. A rotating
window keeps one nightly chain inside the fleet budget and still covers the
catalog over successive UTC days.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "validation-cases.json"


def _parse_int(value, default):
    text = "" if value is None else str(value).strip()
    if text in {"", "null", "None"}:
        return default
    return int(text)


def sweepable_cases(catalog):
    selected = []
    for module in catalog["modules"]:
        for case in module["cases"]:
            status = case.get("source_status")
            # Planned heal.* cases need reviewed native collectors from a
            # rustfs validation-tools run. They are not part of this lane.
            if status == "implemented" or (status == "planned" and not case["id"].startswith("heal.")):
                selected.append((case["id"], module["id"]))
    if not selected:
        raise ValueError("catalog has no sweepable cases")
    return selected


def select_validation_cases(catalog, *, case="", module="", limit=None, rotate=False, when=None):
    """Return (case id, module id) pairs in catalog order, possibly rotated."""
    selected = sweepable_cases(catalog)
    one_case = (case or "").strip()
    module_filter = (module or "").strip()
    known = {case_id: module_id for case_id, module_id in selected}
    if one_case:
        if one_case not in known:
            raise ValueError(f"unknown case or no lane executor for it: {one_case}")
        selected = [(one_case, known[one_case])]
    elif module_filter:
        modules = {module_id for _, module_id in selected}
        if module_filter not in modules:
            raise ValueError(f"unknown module or no implemented cases: {module_filter}")
        selected = [pair for pair in selected if pair[1] == module_filter]
    elif rotate:
        window = max(1, _parse_int(limit, 12))
        moment = when or datetime.now(timezone.utc)
        start = (moment.timetuple().tm_yday * window) % len(selected)
        selected = (selected[start:] + selected[:start])[:window]
    cap = _parse_int(limit, 0)
    if (one_case or module_filter or not rotate) and cap > 0:
        selected = selected[:cap]
    if not selected:
        raise ValueError("catalog selection is empty")
    return selected


def _flag(name):
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=CATALOG)
    parser.add_argument("--output", type=Path, help="Write one case id per line")
    parser.add_argument("--rotate", action="store_true")
    args = parser.parse_args()
    rotate = args.rotate or _flag("VALIDATION_ROTATE") or os.environ.get("SWEEP_EVENT") == "schedule"
    case = os.environ.get("SELECTED_CASE") or os.environ.get("VALIDATION_CASE") or ""
    module = os.environ.get("SELECTED_MODULE") or os.environ.get("VALIDATION_MODULE") or ""
    try:
        selected = select_validation_cases(
            json.loads(args.catalog.read_text()),
            case=case,
            module=module,
            limit=os.environ.get("CASE_LIMIT"),
            rotate=rotate,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("".join(f"{case_id}\n" for case_id, _ in selected))
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"count={len(selected)}\n")
    print(f"selected {len(selected)} case(s):", ", ".join(case_id for case_id, _ in selected))


if __name__ == "__main__":
    main()
