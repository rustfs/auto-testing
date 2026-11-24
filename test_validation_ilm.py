#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

from scripts.validation_ilm.case_plan import ILM_CASE_IDS, ilm_harness_scored_cases
from scripts.validation_ilm.oracles import (
    require_exact_version_ids,
    require_single_latest_marker,
    scanner_node_configuration_matches,
    version_inventory,
)
from scripts.validation_ilm.verify_result import verify


class IlmContractTest(unittest.TestCase):
    @staticmethod
    def scanner_node(
        *,
        leader: bool,
        available: bool,
        effective_interval: int,
        enabled: bool = True,
        cycle_value: int = 2,
        cycle_source: str = "env",
        partial_reason: str | None = None,
        current_cycle: int = 0,
    ) -> dict:
        return {
            "enabled": enabled,
            "runtime_config": {
                "cycle_interval_seconds": {"value": cycle_value, "source": cycle_source},
                "start_delay_seconds": {"value": 0, "source": "env"},
            },
            "cycle_schedule": {
                "execution_role": "leader" if leader else "follower",
                "effective_interval_available": available,
                "effective_interval_seconds": effective_interval,
            },
            "metrics": {
                "leader_lock_held_by_this_process": leader,
                "last_cycle_partial_reason": partial_reason,
                "current_cycle": current_cycle,
            },
        }

    def test_scanner_configuration_accepts_follower_zero_effective_interval(self) -> None:
        follower = self.scanner_node(leader=False, available=False, effective_interval=0)
        self.assertTrue(scanner_node_configuration_matches(follower, 2))

    def test_scanner_configuration_checks_leader_effective_interval(self) -> None:
        leader = self.scanner_node(leader=True, available=True, effective_interval=2)
        self.assertTrue(scanner_node_configuration_matches(leader, 2))
        leader["cycle_schedule"]["effective_interval_seconds"] = 5
        self.assertFalse(scanner_node_configuration_matches(leader, 2))

    def test_scanner_configuration_accepts_unknown_partial_reason(self) -> None:
        node = self.scanner_node(
            leader=True, available=True, effective_interval=2, partial_reason="unknown"
        )
        self.assertTrue(scanner_node_configuration_matches(node, 2))
        node["metrics"]["last_cycle_partial_reason"] = "objects"
        self.assertFalse(scanner_node_configuration_matches(node, 2))

    def test_scanner_configuration_rejects_invalid_runtime(self) -> None:
        disabled = self.scanner_node(
            leader=True, available=True, effective_interval=2, enabled=False
        )
        self.assertFalse(scanner_node_configuration_matches(disabled, 2))
        wrong_source = self.scanner_node(
            leader=False, available=False, effective_interval=0, cycle_source="config"
        )
        self.assertFalse(scanner_node_configuration_matches(wrong_source, 2))
        partial = self.scanner_node(
            leader=True, available=True, effective_interval=2, partial_reason="truncated"
        )
        self.assertFalse(scanner_node_configuration_matches(partial, 2))

    def test_version_inventory_rejects_extra_versions_and_markers(self) -> None:
        response = {
            "Versions": [
                {"Key": "object", "VersionId": "v2", "IsLatest": True, "Size": 2},
                {"Key": "object", "VersionId": "v1", "IsLatest": False, "Size": 1},
            ],
            "DeleteMarkers": [
                {"Key": "object", "VersionId": "m1", "IsLatest": False},
            ],
        }
        inventory = version_inventory(response, "object")
        self.assertEqual([item["versionId"] for item in inventory["versions"]], ["v2", "v1"])
        with self.assertRaisesRegex(AssertionError, "differs"):
            require_exact_version_ids(inventory, {"v2"})
        with self.assertRaisesRegex(AssertionError, "one latest"):
            require_single_latest_marker(inventory)

    def test_version_inventory_requires_one_latest_marker(self) -> None:
        inventory = version_inventory(
            {"DeleteMarkers": [{"Key": "object", "VersionId": "m1", "IsLatest": True}]},
            "object",
        )
        self.assertEqual(require_single_latest_marker(inventory), "m1")
        require_exact_version_ids(inventory, set(), {"m1"})

    def test_version_inventory_rejects_truncated_listing(self) -> None:
        with self.assertRaisesRegex(AssertionError, "truncated"):
            version_inventory({"IsTruncated": True}, "object")

    def test_catalog_drives_case_selection(self) -> None:
        self.assertEqual(15, len(ILM_CASE_IDS))
        self.assertEqual(("ilm.configuration-roundtrip",), ilm_harness_scored_cases(["ilm.configuration-roundtrip"]))
        with self.assertRaises(ValueError):
            ilm_harness_scored_cases(["missing.case"])

    def test_result_requires_selected_case_evidence_and_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case_id = "ilm.configuration-roundtrip"
            (root / "proof.json").write_text("{}", encoding="utf-8")
            result = {"status": {"state": "pass", "cases": [{
                "id": case_id, "state": "pass", "evidence": ["proof.json"],
            }]}}
            manifest = {"executionPlan": {"scoredCases": [case_id]},
                        "cleanupOutcome": {"state": "pass"}, "preservedBuckets": []}
            (root / "result.json").write_text(json.dumps(result), encoding="utf-8")
            (root / "run-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            verify(case_id, root)
            result["status"]["cases"][0]["evidence"] = ["../outside.json"]
            (root / "result.json").write_text(json.dumps(result), encoding="utf-8")
            with self.assertRaises(OSError):
                verify(case_id, root)
            result["status"]["cases"][0]["evidence"] = ["proof.json"]
            (root / "result.json").write_text(json.dumps(result), encoding="utf-8")
            manifest["preservedBuckets"] = ["leftover"]
            (root / "run-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "cleanup"):
                verify(case_id, root)


if __name__ == "__main__":
    unittest.main()
