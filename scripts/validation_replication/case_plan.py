"""Select replication cases and their source setup fixture."""

from __future__ import annotations

import json
from pathlib import Path


CATALOG = Path(__file__).resolve().parents[2] / "validation-cases.json"


def module_cases(module_id: str) -> tuple[str, ...]:
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    modules = [module for module in catalog["modules"] if module["id"] == module_id]
    if len(modules) != 1:
        raise ValueError(f"canonical replication module missing or duplicated: {module_id}")
    return tuple(case["id"] for case in modules[0]["cases"])


BUCKET_CASE_IDS = module_cases("bucket-replication")
SITE_CASE_IDS = module_cases("site-replication")
SITE_GROUP_ACTIVE_FIXTURE = "site-group-active"
SITE_GROUP_ACTIVE_CASE_IDS = frozenset(SITE_CASE_IDS) - {
    "site-replication.unconfigured-info",
    "site-replication.invalid-credentials",
    "site-replication.add-and-info",
    "site-replication.service-account-rotation",
}
