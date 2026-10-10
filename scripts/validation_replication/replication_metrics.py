"""Strict helpers for consuming the stable rc replication output contract."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


U64_MAX = 2**64 - 1


class ReplicationMetricsContractError(ValueError):
    """The rc output is incompatible with the expected output-v3 contract."""


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReplicationMetricsContractError(f"{path} must be a JSON object")
    return value


def _root(document: Any) -> Mapping[str, Any]:
    root = _mapping(document, "document")
    schema_version = root.get("schema_version")
    if type(schema_version) is not int or schema_version != 3:
        raise ReplicationMetricsContractError(
            "document.schema_version must be the JSON integer 3"
        )
    if root.get("type") != "replication":
        raise ReplicationMetricsContractError(
            'document.type must be the JSON string "replication"'
        )
    return root


def _counter(container: Mapping[str, Any], name: str, path: str) -> int:
    if name not in container:
        return 0
    value = container[name]
    if type(value) is not int or not 0 <= value <= U64_MAX:
        raise ReplicationMetricsContractError(
            f"{path}.{name} must be an unsigned 64-bit JSON integer"
        )
    return value


def _data(document: Any) -> Mapping[str, Any]:
    root = _root(document)
    if "data" not in root:
        raise ReplicationMetricsContractError("document.data is required")
    return _mapping(root["data"], "document.data")


def replicated_object_count(document: Any) -> int:
    """Return the greatest aggregate or per-target replicated object count."""

    data = _data(document)
    totals_value = data.get("totals", {})
    totals = _mapping(totals_value, "document.data.totals")
    observed = _counter(totals, "replicated_count", "document.data.totals")

    targets_value = data.get("targets", [])
    if not isinstance(targets_value, list):
        raise ReplicationMetricsContractError("document.data.targets must be a JSON array")
    for index, value in enumerate(targets_value):
        target = _mapping(value, f"document.data.targets[{index}]")
        observed = max(
            observed,
            _counter(
                target,
                "replicated_count",
                f"document.data.targets[{index}]",
            ),
        )
    return observed


def replication_metrics_ready(document: Any) -> bool:
    """Return whether a successful output-v3 document proves replication progress."""

    root = _root(document)
    if root.get("status") != "success":
        return False
    observed = replicated_object_count(root)
    data = _data(root)
    targets = data.get("targets", [])
    return (
        data.get("availability") == "available"
        and bool(targets)
        and observed >= 1
    )
