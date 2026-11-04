#!/usr/bin/env python3

import unittest
from pathlib import Path

from scripts.validation_pool.case_plan import POOL_CASE_IDS, pool_harness_scored_cases
from scripts.validation_pool.pool_helpers import active_endpoint_indices, active_fixture_bytes


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


if __name__ == "__main__":
    unittest.main()
