#!/usr/bin/env python3

import unittest
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from scripts.validation_pool.case_plan import POOL_CASE_IDS, pool_harness_scored_cases
from scripts.validation_pool.pool_helpers import active_endpoint_indices, active_fixture_bytes, active_rebalance_fixture_bytes

sys.path.insert(0, str(Path(__file__).parent / "scripts/validation_pool"))
from pool_behavior import PoolHarness
from pool_status import rebalance_complete


class PoolContractTest(unittest.TestCase):
    def test_environment_hooks_handle_missing_service_and_file_logs(self) -> None:
        script = (Path(__file__).parent / "scripts/validation_pool/run_case.sh").read_text()
        self.assertIn("is not installed; stop skipped", script)
        self.assertIn("/var/log/rustfs/*.log", script)
        self.assertIn("--rawfile output0", script)
        self.assertIn("--rawfile output1", script)
        self.assertNotIn('--arg output0 "${output0}"', script)

    def test_catalog_drives_case_selection(self) -> None:
        self.assertEqual(13, len(POOL_CASE_IDS))
        self.assertEqual(("pool.status",), pool_harness_scored_cases(["pool.status"]))
        with self.assertRaises(ValueError):
            pool_harness_scored_cases(["pool.missing"])

    def test_two_node_layout_and_active_fixture_size(self) -> None:
        self.assertEqual((0,), active_endpoint_indices(2, "initial"))
        self.assertEqual((0, 1), active_endpoint_indices(2, "expanded"))
        self.assertEqual((1,), active_endpoint_indices(2, "retired"))
        self.assertEqual(32 * 1024 * 1024, active_fixture_bytes(1024))
        self.assertEqual(128 * 1024 * 1024, active_rebalance_fixture_bytes(1024))


class RebalanceWindowTests(unittest.TestCase):
    def document(self, status="Started", objects=0):
        return {"id": "operation-1", "pools": [{"id": 0, "status": status, "used": 0.5,
                "cleanupWarnings": {"count": 0}, "progress": {"objects": objects}}]}

    def harness(self, document):
        harness = PoolHarness.__new__(PoolHarness)
        harness.args = SimpleNamespace(endpoint="http://test.invalid", active_timeout=1, timeout=1, fixture_bytes=1024,
                                       fixture_objects=128, active_rebalance_fixture_bytes=128 * 1024**2)
        harness.signed_request = Mock(return_value=(200, {}, json.dumps(document).encode()))
        harness.save_json = Mock()
        harness.rebalance_status = Mock(return_value=document)
        harness.rebalance_fixture_size = Mock(return_value=128 * 1024**2)
        def wait_once(case, description, probe, **kwargs):
            matched, value = probe()
            if not matched:
                raise AssertionError("predicate did not match")
            return value
        harness.wait_until = wait_once
        return harness

    def test_started_with_progress_enters_active_window(self):
        document = self.document(objects=10)
        harness = self.harness(document)
        self.assertEqual(harness.wait_rebalance("case", "operation-1", "active"), document)

    def test_unpublished_progress_does_not_satisfy_the_source_oracle(self):
        harness = self.harness(self.document())
        with self.assertRaisesRegex(AssertionError, "predicate did not match"):
            harness.wait_rebalance("case", "operation-1", "active")

    def test_completed_does_not_replace_active_window(self):
        harness = self.harness(self.document("Completed", 10))
        with self.assertRaisesRegex(AssertionError, "completed before"):
            harness.wait_rebalance("case", "operation-1", "active")
        self.assertTrue(harness.save_json.called)

    def test_completion_during_parity_checks_fails_the_window(self):
        harness = self.harness(self.document(objects=10))
        harness.rebalance_status.return_value = self.document("Completed", 10)
        with self.assertRaisesRegex(AssertionError, "parity checks"):
            harness.wait_rebalance("case", "operation-1", "active")

    def test_final_completion_still_requires_migration_progress(self):
        self.assertFalse(rebalance_complete(self.document("Completed"), "operation-1"))
        self.assertTrue(rebalance_complete(self.document("Completed", 10), "operation-1"))

    def test_live_io_cannot_pass_when_operation_completed_during_io(self):
        harness = self.harness(self.document("Completed", 10))
        harness.seed_and_expand = Mock(return_value=("bucket", []))
        harness.start_rebalance = Mock(return_value="operation-1")
        harness.wait_rebalance = Mock()
        harness.exercise_live_io = Mock(return_value=[])
        harness.assert_inventory = Mock()
        with self.assertRaisesRegex(AssertionError, "did not remain active"):
            harness.case_rebalance_live_io("case")
        self.assertEqual(harness.wait_rebalance.call_count, 1)
        harness.assert_inventory.assert_not_called()

    def test_large_rebalance_fixture_has_a_capacity_admission_check(self):
        harness = self.harness(self.document())
        harness.admin_json = Mock(return_value={"disks": [
            {"state": "ok", "totalspace": 20 * 1024**3, "usedspace": 1024**3} for _ in range(4)
        ]})
        self.assertEqual(PoolHarness.rebalance_fixture_size(harness, "case"), 128 * 1024**2)
        harness.admin_json.return_value = {"disks": [
            {"state": "ok", "totalspace": 1024**3, "usedspace": 0} for _ in range(4)
        ]}
        with self.assertRaisesRegex(RuntimeError, "ENVIRONMENT-CAPACITY"):
            PoolHarness.rebalance_fixture_size(harness, "case")


if __name__ == "__main__":
    unittest.main()
