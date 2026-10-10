"""Select pool lifecycle cases from the canonical auto-testing catalog."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


CATALOG = Path(__file__).resolve().parents[2] / "validation-cases.json"


def pool_case_ids() -> tuple[str, ...]:
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    modules = [module for module in catalog["modules"] if module["id"] == "pool-lifecycle"]
    if len(modules) != 1:
        raise ValueError("canonical pool module is missing or duplicated")
    return tuple(case["id"] for case in modules[0]["cases"])


POOL_CASE_IDS = pool_case_ids()


def pool_harness_scored_cases(requested_cases: Iterable[str] | None) -> tuple[str, ...]:
    if requested_cases is None:
        return POOL_CASE_IDS
    selected = tuple(requested_cases)
    if len(selected) != len(set(selected)) or set(selected) - set(POOL_CASE_IDS):
        raise ValueError("duplicate or unknown canonical pool case")
    return tuple(case_id for case_id in POOL_CASE_IDS if case_id in selected)
