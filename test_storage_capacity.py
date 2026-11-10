import unittest

from scripts.storage_capacity import assess, object_bytes, sample


class CapacitySafetyTests(unittest.TestCase):
    def disks(self, *used, now=0):
        return sample({"info": {"disks": [{"state": "ok", "totalspace": 10000, "usedspace": value}
                                          for value in used]}}, now=now)

    def decision(self, current, previous=None, **kwargs):
        return assess(current, previous, min_free_bytes=100, in_flight_bytes=200, **kwargs)

    def test_pool_average_cannot_hide_an_almost_full_disk(self):
        result = self.decision(self.disks(9990, 1000, 1000, 1000))
        self.assertLess(result["usage_percent"], 40)
        self.assertEqual(result["action"], "capacity_guard")
        self.assertTrue(result["emergency"])

    def test_in_flight_reserve_stops_before_the_emergency_limit(self):
        result = assess(
            self.disks(9400), min_free_bytes=100, in_flight_bytes=600, max_disk_percent=99)
        self.assertEqual(result["action"], "capacity_guard")
        self.assertFalse(result["emergency"])

    def test_growth_reserves_the_next_sample_and_shutdown_window(self):
        previous = self.disks(5000, 5000, now=10)
        result = self.decision(self.disks(6000, 6000, now=12), previous)
        self.assertEqual(result["growth_bytes_per_second"], 1000)
        self.assertEqual(result["action"], "capacity_guard")

    def test_safe_target_reached_and_insufficient_load_are_distinct(self):
        self.assertEqual(self.decision(self.disks(8500, 8600))["action"], "target_reached")
        self.assertEqual(self.decision(self.disks(1000))["action"], "continue")

    def test_inventory_change_or_invalid_capacity_does_not_allow_more_writes(self):
        with self.assertRaises(ValueError):
            self.decision(self.disks(2000, 2000, now=2), self.disks(1000, now=1))
        for document in ({"disks": []}, {"disks": [{"state": "offline"}]},
                         {"disks": [{"totalspace": 10, "usedspace": -1}]},
                         {"disks": [{"totalspace": 10, "usedspace": 11}]},
                         {"disks": [{"totalspace": 10, "usedspace": "1"}]}):
            with self.subTest(document=document), self.assertRaises(ValueError):
                sample(document)

    def test_invalid_bounds_and_object_sizes_are_rejected(self):
        self.assertEqual(object_bytes("100MiB"), 100 * 1024**2)
        for invalid in ("0MiB", "-1", "100MB"):
            with self.assertRaises(ValueError):
                object_bytes(invalid)
        for kwargs in ({"threshold": 95}, {"poll_seconds": 0}, {"stop_seconds": -1}):
            with self.assertRaises(ValueError):
                self.decision(self.disks(1000), **kwargs)


if __name__ == "__main__":
    unittest.main()
