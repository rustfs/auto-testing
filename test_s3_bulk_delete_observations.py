import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ReadTimeoutError
from scripts import s3_bulk_delete_regressions as probe
from test_s3_bulk_delete_regressions import VersionedS3


class DeleteObservationsTests(unittest.TestCase):
    endpoints = ["http://node1.invalid", "http://node2.invalid"]

    def run_probe(self, fake, case="S3C-213", **kwargs):
        with tempfile.TemporaryDirectory() as temporary:
            return probe.run_case(case, "off", 3, self.endpoints, Path(temporary) / "run",
                                  workers=2, factory=lambda _: fake, **kwargs)

    def test_acknowledged_delete_records_manifest_entries_and_all_endpoint_views(self):
        result = self.run_probe(VersionedS3(), key_shape="long-deep")
        self.assertEqual(result["verdict"], "PASS", result)
        self.assertEqual(result["delete_observation"]["entries"], {"complete": True, "deleted_count": 3, "error_count": 0})
        self.assertEqual(result["delete_observation"]["read_timeout_seconds"], 180)
        self.assertEqual(result["key_shape_stats"]["max_utf8_bytes"], 1024)
        self.assertEqual(result["key_bytes"], 1024)
        for view in result["seed_inventory"]["views"]:
            self.assertEqual(view["visible"], 3)
            self.assertEqual(view["pages"], 1)
            self.assertTrue(view["matches_manifest"])
        views = result["visibility_observation"]
        self.assertEqual([v["visible"] for v in views["rounds"][0]], [0, 0])
        self.assertEqual(views["first_all_absent_at"], max(v["finished_at"] for v in views["rounds"][0]))
        self.assertEqual(result["physical_completion"], "not_measured")

    def test_custom_long_key_length_is_used_in_put_delete_inventory_and_results(self):
        for shape in ("long-shallow", "long-deep"):
            with self.subTest(shape=shape):
                fake = VersionedS3()
                result = self.run_probe(fake, key_shape=shape, key_bytes=129)
                self.assertEqual(result["verdict"], "PASS", result)
                self.assertEqual(result["key_bytes"], 129)
                self.assertEqual(result["key_shape_stats"]["min_utf8_bytes"], 129)
                self.assertEqual(result["key_shape_stats"]["max_utf8_bytes"], 129)
                manifest = [key for operation, key in fake.calls if operation == "put" and key.startswith("target/")]
                self.assertEqual([len(key.encode()) for key in manifest], [129] * 3)
                primary = next(rows for operation, rows in fake.calls if operation == "delete")
                self.assertEqual([row["Key"] for row in primary], manifest)

    def test_success_after_ten_seconds_keeps_complete_ack_and_reference(self):
        offset = [0]
        real_clock = time.monotonic

        class Slow(VersionedS3):
            def delete_objects(self, **kwargs):
                if "VersionId" not in kwargs["Delete"]["Objects"][0]:
                    offset[0] += 12
                return super().delete_objects(**kwargs)

        with patch.object(probe.time, "monotonic", side_effect=lambda: real_clock() + offset[0]):
            result = self.run_probe(Slow(), observation_budget=30)
        self.assertEqual(result["verdict"], "PASS", result)
        delete = result["delete_observation"]
        self.assertEqual(delete["read_timeout_seconds"], 30)
        self.assertEqual(delete["sdk_response"], "complete")
        self.assertTrue(delete["entries"]["complete"])
        self.assertGreaterEqual(delete["seconds"], 12)
        self.assertTrue(delete["exceeded_reference_timeout"])

    def test_missing_response_and_later_completion_are_distinct(self):
        class Late(VersionedS3):
            pending = None
            observations = 0
            primary_requests = 0

            def delete_objects(self, **kwargs):
                if "VersionId" not in kwargs["Delete"]["Objects"][0]:
                    self.primary_requests += 1
                    self.pending = kwargs
                    raise ReadTimeoutError(endpoint_url="http://node1.invalid")
                return super().delete_objects(**kwargs)

            def list_objects_v2(self, **kwargs):
                with self.lock:
                    if self.pending:
                        self.observations += 1
                        if self.observations == 3:
                            super().delete_objects(**self.pending)
                            self.pending = None
                    return super().list_objects_v2(**kwargs)

        fake = Late()
        with patch.object(probe.time, "sleep"):
            result = self.run_probe(fake, profile="client-10s", observation_budget=2)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["cleanup"], "PASS")
        self.assertEqual(fake.primary_requests, 1)
        delete = result["delete_observation"]
        self.assertEqual(delete["read_timeout_seconds"], 10)
        self.assertEqual(delete["sdk_response"], "not_received")
        self.assertEqual(delete["logical_outcome"], "unknown")
        self.assertIsNone(delete["entries"])
        views = result["visibility_observation"]
        self.assertEqual([[v["visible"] for v in row] for row in views["rounds"]], [[3, 3], [0, 0]])
        self.assertIsNotNone(views["first_all_absent_at"])

    def test_complete_ack_with_visible_target_is_a_consistency_failure(self):
        result = self.run_probe(VersionedS3("residue"))
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["error_code"], "DeleteResidue")
        self.assertTrue(result["delete_observation"]["entries"]["complete"])
        self.assertEqual([v["visible"] for v in result["visibility_observation"]["rounds"][0]], [1, 1])
        self.assertIsNone(result["visibility_observation"]["first_all_absent_at"])

    def test_only_primary_and_cleanup_deletes_get_their_own_socket_budgets(self):
        invoke = probe.Requests.invoke
        for profile, timeout in [("compatibility", 47), ("client-10s", 10)]:
            with self.subTest(profile=profile):
                observed = []

                def record(session, operation, **arguments):
                    observed.append((session.phase, operation, session.read_timeout))
                    return invoke(session, operation, **arguments)

                with patch.object(probe.Requests, "invoke", record):
                    result = self.run_probe(VersionedS3(), profile=profile, observation_budget=47, cleanup_budget=31)
                self.assertEqual(result["verdict"], "PASS", result)
                self.assertIn(("delete", "delete_objects", timeout), observed)
                self.assertIn(("cleanup", "delete_objects", 31), observed)
                self.assertTrue(all(seconds == 10 for _, operation, seconds in observed if operation != "delete_objects"))

    def test_http_error_and_incomplete_entry_response_are_distinguished(self):
        for fault, expected, status in [("http503", "http_error", 503), ("missing-ack", "incomplete", 200)]:
            with self.subTest(fault=fault):
                result = self.run_probe(VersionedS3(fault))
                self.assertEqual(result["verdict"], "FAIL")
                delete = result["delete_observation"]
                self.assertEqual(delete["sdk_response"], expected)
                self.assertEqual(delete["http_status"], status)
                if expected == "http_error":
                    self.assertEqual(delete["logical_outcome"], "request_error")
                else:
                    self.assertFalse(delete["entries"]["complete"])

    def test_first_visibility_does_not_wait_for_loaded_get_to_finish(self):
        first_list = threading.Event()

        class WaitingLoad(VersionedS3):
            def get_object(self, **kwargs):
                response = super().get_object(**kwargs)
                if threading.current_thread() is not threading.main_thread() and self.after_delete:
                    if not first_list.wait(2):
                        raise AssertionError("load worker joined before the first post-ack LIST")
                return response

            def list_objects_v2(self, **kwargs):
                if self.after_delete:
                    first_list.set()
                return super().list_objects_v2(**kwargs)

        result = self.run_probe(WaitingLoad(), case="S3C-1306", observation_budget=5)
        self.assertEqual(result["verdict"], "PASS", result)
        self.assertTrue(first_list.is_set())
        self.assertGreater(result["load_overlap"]["successful_requests"], 0)


if __name__ == "__main__":
    unittest.main()
