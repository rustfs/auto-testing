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
