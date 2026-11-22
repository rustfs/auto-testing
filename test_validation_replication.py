#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

from scripts.validation_replication.case_plan import (
    BUCKET_CASE_IDS, SITE_CASE_IDS, SITE_GROUP_ACTIVE_CASE_IDS,
)
from scripts.validation_replication.verify_result import verify
from scripts.validation_replication.oracles import require_version_ids, version_inventory


class ReplicationContractTest(unittest.TestCase):
    def test_catalog_and_fixture_selection(self) -> None:
        self.assertEqual(18, len(BUCKET_CASE_IDS))
        self.assertEqual(20, len(SITE_CASE_IDS))
        self.assertEqual(16, len(SITE_GROUP_ACTIVE_CASE_IDS))
        self.assertNotIn("site-replication.unconfigured-info", SITE_GROUP_ACTIVE_CASE_IDS)
        self.assertIn("site-replication.bucket-create", SITE_GROUP_ACTIVE_CASE_IDS)

    def test_pass_requires_case_fixture_and_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case_id = "site-replication.bucket-create"
            (root / "proof.json").write_text("{}", encoding="utf-8")
            result = {"status": {"state": "pass", "cases": [{
                "id": case_id, "state": "pass", "evidence": ["proof.json"],
            }]}}
            manifest = {"executionPlan": {"scoredCases": [case_id]},
                        "isolationOutcome": {"state": "pass", "trackedBuckets": []},
                        "fixtureOutcomes": [{"state": "pass"}]}
            (root / "result.json").write_text(json.dumps(result), encoding="utf-8")
            (root / "run-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            verify(case_id, root)
            manifest["fixtureOutcomes"][0]["state"] = "infra_error"
            (root / "run-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "fixture"):
                verify(case_id, root)

    def test_version_inventory_filters_one_key_and_preserves_marker_state(self) -> None:
        response = {
            "Versions": [
                {"Key": "object.txt", "VersionId": "v2", "IsLatest": True, "Size": 2},
                {"Key": "other.txt", "VersionId": "other"},
                {"Key": "object.txt", "VersionId": "v1", "IsLatest": False, "Size": 1},
            ],
            "DeleteMarkers": [
                {"Key": "object.txt", "VersionId": "m1", "IsLatest": False},
                {"Key": "other.txt", "VersionId": "m-other"},
            ],
        }
        self.assertEqual(
            ["v1", "v2"],
            [item["versionId"] for item in version_inventory(response, key="object.txt")["versions"]],
        )
        self.assertEqual(
            ["m1"],
            [item["versionId"] for item in version_inventory(response, key="object.txt")["deleteMarkers"]],
        )

    def test_version_inventory_rejects_extra_versions_and_markers(self) -> None:
        response = {
            "Versions": [{"Key": "object.txt", "VersionId": "v1"}],
            "DeleteMarkers": [{"Key": "object.txt", "VersionId": "m1"}],
        }
        require_version_ids(
            response,
            key="object.txt",
            versions=["v1"],
            delete_markers=["m1"],
        )
        with self.assertRaisesRegex(AssertionError, "version inventory mismatch"):
            require_version_ids(response, key="object.txt", versions=["v1", "v2"])
        with self.assertRaisesRegex(AssertionError, "version inventory mismatch"):
            require_version_ids(response, key="object.txt", versions=["v1"])

    def test_version_inventory_rejects_truncated_listing(self) -> None:
        with self.assertRaisesRegex(AssertionError, "truncated"):
            version_inventory({"IsTruncated": True}, key="object.txt")


if __name__ == "__main__":
    unittest.main()
