"""Pure lifecycle state oracles shared by live checks and unit tests."""

from __future__ import annotations

from typing import Any


def version_inventory(response: dict[str, Any], key: str) -> dict[str, list[dict[str, Any]]]:
    """Return the exact version and delete-marker inventory for one key."""

    if not isinstance(response, dict):
        raise AssertionError("version listing is not an object")
    if response.get("IsTruncated") is True:
        raise AssertionError("version listing is truncated")
    inventory: dict[str, list[dict[str, Any]]] = {"versions": [], "deleteMarkers": []}
    source_fields = {"versions": "Versions", "deleteMarkers": "DeleteMarkers"}
    for field, source_field in source_fields.items():
        items = response.get(source_field, [])
        if not isinstance(items, list):
            raise AssertionError(f"{field} is not a list")
        for item in items:
            if not isinstance(item, dict) or item.get("Key") != key:
                continue
            version_id = item.get("VersionId")
            if not isinstance(version_id, str) or not version_id:
                raise AssertionError(f"{field} contains an invalid version ID")
            inventory[field].append(
                {
                    "versionId": version_id,
                    "isLatest": item.get("IsLatest") is True,
                    **({"size": item.get("Size")} if field == "versions" else {}),
                }
            )
    return inventory


def require_exact_version_ids(
    inventory: dict[str, list[dict[str, Any]]],
    expected_versions: set[str],
    expected_markers: set[str] | None = None,
) -> None:
    """Reject missing, extra, or duplicated version and marker records."""

    expected_markers = set() if expected_markers is None else expected_markers
    versions = [item["versionId"] for item in inventory["versions"]]
    markers = [item["versionId"] for item in inventory["deleteMarkers"]]
    if len(versions) != len(set(versions)) or len(markers) != len(set(markers)):
        raise AssertionError("version listing contains duplicate IDs")
    if set(versions) != expected_versions or set(markers) != expected_markers:
        raise AssertionError(
            "version listing differs from the expected state: "
            f"versions={versions!r}, markers={markers!r}"
        )


def require_single_latest_marker(inventory: dict[str, list[dict[str, Any]]]) -> str:
    """Return the only latest marker and reject ambiguous marker state."""

    latest = [item["versionId"] for item in inventory["deleteMarkers"] if item["isLatest"]]
    if len(latest) != 1:
        raise AssertionError(f"expected one latest delete marker, got {latest!r}")
    return latest[0]


def scanner_node_configuration_matches(
    data: dict[str, Any], expected_interval: int
) -> bool:
    """Return whether one scanner status node has the configured runtime.

    Only the leader schedules cycles, so followers report
    ``effective_interval_available=false`` with a zero effective interval by
    design. Comparing that zero with the configured interval makes a healthy
    cluster look permanently unconfigured; the effective interval is checked
    on the leader, which is also the node whose cycle the readiness gate waits
    for.
    """

    metrics = data.get("metrics", {})
    runtime = data.get("runtime_config", {})
    cycle = runtime.get("cycle_interval_seconds", {})
    start_delay = runtime.get("start_delay_seconds", {})
    schedule = data.get("cycle_schedule", {})
    if (
        data.get("enabled") is not True
        or cycle.get("value") != expected_interval
        or cycle.get("source") != "env"
        or start_delay.get("value") != 0
        or start_delay.get("source") != "env"
        or metrics.get("last_cycle_partial_reason") not in (None, "")
    ):
        return False
    current_cycle = metrics.get("current_cycle")
    if current_cycle is not None and (
        type(current_cycle) is not int or current_cycle < 0
    ):
        return False
    if metrics.get("leader_lock_held_by_this_process") is True:
        return schedule.get("effective_interval_seconds") == expected_interval
    return True
