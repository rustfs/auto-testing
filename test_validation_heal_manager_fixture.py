"""Hosted manager evidence cannot be rebound to another run or case scope."""

import json
from pathlib import Path
import tempfile
import unittest

import test_validation_heal_manager_native as fixtures
from scripts.validation_heal import manager_fixture as consumer


class ManagerFixtureTests(unittest.TestCase):
    def fixture(self, root):
        tools, evidence, candidate, _ = fixtures.ManagerNativeEvidenceTests().fixture(root)
        tools.rename(root / "manager-tools"); evidence.rename(root / "manager-evidence")
        api = {"id": 12345, "run_attempt": 1, "status": "completed", "conclusion": "success",
               "path": ".github/workflows/rustfs-functional-chain.yml", "head_sha": "b" * 40}
        (root / "tool-run.json").write_text(json.dumps(api))
        return candidate, api

    def test_each_case_consumes_only_its_exact_original_prerequisite_tests(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate, _ = self.fixture(root)
            for case, keys in consumer.SCOPES.items():
                value = consumer.verify(root, candidate, case)
                self.assertEqual([row["regression"] for row in value["casePrerequisiteTests"]], list(keys))
                self.assertEqual(value["exactTests"], 15)
                self.assertFalse(value["canonicalCasePassClaimed"])
                self.assertFalse(value["canonicalFleetBranchesExecuted"])
                self.assertFalse(value["individualFsyncHooksClaimed"])
                self.assertFalse(value["physicalPowerLossClaimed"])

    def test_failed_partial_wrong_attempt_or_foreign_api_cannot_certify_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate, api = self.fixture(root)
            changes = [("id", 12346), ("id", True), ("run_attempt", 2), ("status", "in_progress"),
                       ("conclusion", "failure"), ("path", ".github/workflows/ci.yml"), ("head_sha", "main")]
            for field, new in changes:
                (root / "tool-run.json").write_text(json.dumps({**api, field: new}))
                with self.subTest(field=field), self.assertRaises(ValueError):
                    consumer.verify(root, candidate, next(iter(consumer.SCOPES)))

    def test_unrelated_case_and_changed_original_result_cannot_be_promoted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate, _ = self.fixture(root)
            with self.assertRaises(ValueError): consumer.verify(root, candidate, 'heal.admin-nolock-safety')
            (root / 'manager-evidence/outcome-truth.log').write_text('running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored;\n')
            with self.assertRaises(ValueError): consumer.verify(root, candidate, 'heal.outcome-counters-and-error-truth')


if __name__ == '__main__': unittest.main()
