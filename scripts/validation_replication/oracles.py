"""Pure assertions for replicated object version inventories."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def version_inventory(
    response: Mapping[str, Any],
    *,
    key: str,
) -> dict[str, list[dict[str, Any]]]:
    """Return the complete version and delete-marker inventory for one key."""

    if response.get("IsTruncated") is True:
        raise AssertionError("version listing is truncated")
    versions: list[dict[str, Any]] = []
    for item in response.get("Versions", []) or []:
        if item.get("Key") == key:
            versions.append(
                {
                    "versionId": item.get("VersionId"),
                    "isLatest": item.get("IsLatest"),
                    "size": item.get("Size"),
                    "etag": item.get("ETag"),
                }
            )
    markers: list[dict[str, Any]] = []
    for item in response.get("DeleteMarkers", []) or []:
        if item.get("Key") == key:
            markers.append(
                {
                    "versionId": item.get("VersionId"),
                    "isLatest": item.get("IsLatest"),
                }
            )
    versions.sort(key=lambda item: str(item.get("versionId")))
    markers.sort(key=lambda item: str(item.get("versionId")))
    return {"versions": versions, "deleteMarkers": markers}


def require_version_ids(
    response: Mapping[str, Any],
    *,
    key: str,
    versions: Iterable[str],
    delete_markers: Iterable[str] = (),
) -> dict[str, list[dict[str, Any]]]:
    """Require exact version and marker IDs, with no hidden extra entries."""

    observed = version_inventory(response, key=key)
    expected_versions = sorted(str(value) for value in versions)
    expected_markers = sorted(str(value) for value in delete_markers)
    actual_versions = sorted(
        str(item.get("versionId")) for item in observed["versions"]
    )
    actual_markers = sorted(
        str(item.get("versionId")) for item in observed["deleteMarkers"]
    )
    if actual_versions != expected_versions or actual_markers != expected_markers:
        raise AssertionError(
            f"{key} version inventory mismatch: "
            f"expected versions={expected_versions}, markers={expected_markers}; "
            f"observed versions={actual_versions}, markers={actual_markers}"
        )
    return observed
