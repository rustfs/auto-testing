#!/usr/bin/env python3

import unittest

from scripts.validation_pool.case_plan import POOL_CASE_IDS, pool_harness_scored_cases
from scripts.validation_pool.pool_helpers import active_endpoint_indices, active_fixture_bytes


class PoolContractTest(unittest.TestCase):
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
