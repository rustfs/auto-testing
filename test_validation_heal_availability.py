"""Focused availability and recovery census contracts for Heal cases."""

from __future__ import annotations

import copy
import unittest

from scripts.validation_heal.heal_probe import (
    compare_inventories,
    compare_manifest_entries,
    validate_inventory_shape,
)
from scripts.validation_heal.shards import validate_fault, validate_repaired_physical


def inventory(*, count=1, size=5, pages=(1,), current=(1,)):
    entry = {
        "key": "objects/0001.bin",
        "versionId": "null",
        "isLatest": True,
        "deleteMarker": False,
        "payload": {"size": size, "sha256": "a" * 64},
        "properties": {"ETag": '"etag"'},
        "tags": [],
    }
    return {
        "bucket": "owned-bucket",
        "entries": [entry] if count else [],
        "objectsAndMarkers": count,
        "bytesRead": size if count else 0,
        "pagination": {"versionPageSizes": list(pages), "currentPageSizes": list(current)},
    }


class InventoryAvailabilityTests(unittest.TestCase):
    def test_empty_census_is_a_valid_recovery_boundary(self):
        value = inventory(count=0, size=0, pages=(0,), current=(0,))
        self.assertEqual(validate_inventory_shape(value)["entries"], 0)
        self.assertTrue(compare_inventories(value, copy.deepcopy(value))["equal"])

    def test_census_rejects_counter_byte_and_pagination_drift(self):
        for mutation, message in (
            (lambda value: value.update(objectsAndMarkers=2), "count"),
            (lambda value: value.update(bytesRead=4), "byte"),
            (lambda value: value["pagination"].update(versionPageSizes=[0]), "pagination"),
        ):
            value = inventory()
            mutation(value)
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                validate_inventory_shape(value)

    def test_census_rejects_duplicate_identities_and_mixed_summary_modes(self):
        value = inventory()
        value["entries"].append(copy.deepcopy(value["entries"][0]))
        value["objectsAndMarkers"] = 2
        value["bytesRead"] = 10
        value["pagination"]["versionPageSizes"] = [2]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_inventory_shape(value)
        legacy = {"bucket": "owned-bucket", "entries": [inventory()["entries"][0]]}
        self.assertFalse(validate_inventory_shape(legacy)["summaryChecked"])
        with self.assertRaisesRegex(ValueError, "completeness"):
            compare_inventories(legacy, inventory())

    def test_empty_legacy_census_requires_completeness_evidence(self):
        with self.assertRaisesRegex(ValueError, "completeness evidence"):
            validate_inventory_shape({"bucket": "owned-bucket", "entries": []})

    def test_current_pages_must_match_latest_live_entries(self):
        value = inventory(current=(0,))
        with self.assertRaisesRegex(ValueError, "pagination"):
            validate_inventory_shape(value)

    def test_expected_manifest_comparison_does_not_reuse_census_counters(self):
        expected = inventory()
        observed = inventory()
        expected["objectsAndMarkers"] = 99
        expected["bytesRead"] = 99
        expected["pagination"] = {"versionPageSizes": [99], "currentPageSizes": [99]}
        self.assertTrue(compare_manifest_entries(expected, observed)["equal"])
        with self.assertRaisesRegex(ValueError, "differ"):
            compare_inventories(expected, observed)


class SnapshotAvailabilityTests(unittest.TestCase):
    def scene(self):
        file_entry = {"disk": 0, "path": "owned/objects/0001.bin/part.1", "size": 10, "sha256": "a"}
        return {
            "node1": {"formats": ["format"], "files": [file_entry]},
            "node2": {"formats": ["format"], "files": []},
        }, {
            "node": "node1", "disk": 0, "relativePath": file_entry["path"],
            "sha256": "a", "size": 10,
        }

    def test_fault_and_repair_reject_hidden_or_missing_nodes(self):
        before, target = self.scene()
        receipt = {"relativePath": target["relativePath"],
                   "before": {"sha256": "a", "size": 10}, "fault": None}
        after = copy.deepcopy(before)
        after["node1"]["files"] = []
        with self.assertRaisesRegex(ValueError, "node membership"):
            validate_fault(before, {"node1": after["node1"]}, target, receipt)
        with self.assertRaisesRegex(ValueError, "node membership"):
            validate_repaired_physical(before, {**after, "node3": {"formats": [], "files": []}}, target)


if __name__ == "__main__":
    unittest.main()
