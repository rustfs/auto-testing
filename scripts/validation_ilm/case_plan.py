"""Select ILM cases from the canonical auto-testing catalog."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


CATALOG = Path(__file__).resolve().parents[2] / "validation-cases.json"
IMMEDIATE_TRANSITION_CASE = "ilm.immediate-transition-and-restore"
WORM_EXPIRATION_CASE = "ilm.worm-expiration"


def ilm_case_ids(module_id: str = "ilm-expiration") -> tuple[str, ...]:
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    modules = [module for module in catalog["modules"] + catalog.get("local_modules", []) if module["id"] == module_id]
    if len(modules) != 1:
        raise ValueError(f"ILM module is missing or duplicated: {module_id}")
    return tuple(case["id"] for case in modules[0]["cases"])


ILM_CASE_IDS = ilm_case_ids()
ILM_SELECTABLE_CASE_IDS = ILM_CASE_IDS + ilm_case_ids("ilm-regressions")


def ilm_harness_scored_cases(requested_cases: Iterable[str] | None) -> tuple[str, ...]:
    if requested_cases is None:
        return ILM_CASE_IDS
    selected = tuple(requested_cases)
    if len(selected) != len(set(selected)) or set(selected) - set(ILM_SELECTABLE_CASE_IDS):
        raise ValueError("duplicate or unknown ILM case")
    if IMMEDIATE_TRANSITION_CASE in selected and len(selected) > 1:
        raise ValueError("immediate transition requires an isolated scanner-disabled run")
    return tuple(case_id for case_id in ILM_SELECTABLE_CASE_IDS if case_id in selected)
