#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

from scripts.validation_replication.case_plan import (
    BUCKET_CASE_IDS, SITE_CASE_IDS, SITE_GROUP_ACTIVE_CASE_IDS,
)
from scripts.validation_replication.verify_result import verify


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


if __name__ == "__main__":
    unittest.main()
