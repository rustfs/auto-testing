#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

from scripts.validation_ilm.case_plan import ILM_CASE_IDS, ilm_harness_scored_cases
from scripts.validation_ilm.oracles import (
    require_exact_version_ids,
    require_single_latest_marker,
    version_inventory,
)
from scripts.validation_ilm.verify_result import verify


class IlmContractTest(unittest.TestCase):
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
