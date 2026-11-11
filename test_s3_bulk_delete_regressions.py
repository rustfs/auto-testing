#!/usr/bin/env python3
"""Mutation tests for the bulk-delete campaign, without a RustFS environment."""
from collections import Counter
import io
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
import xml.etree.ElementTree as ET

from scripts import s3_bulk_delete_regressions as probe


class S3Error(Exception):
    def __init__(self, code, status=503):
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {
            "HTTPStatusCode": status, "RequestId": "failed-request", "RetryAttempts": 0}}


class VersionedS3:
    def __init__(self, mutation=None):
        self.mutation = mutation
        self.status = "off"
        self.versions = {}
        self.calls = []
        self.counter = 0
        self.deleted_bucket = False
        self.cleanup_failures = 0
        self.after_delete = False
        self.lock = threading.RLock()
        self.load_counts = Counter()
        self.bulk_entered = threading.Event()
        self.load_active = 0
        self.observed_overlap = False

    def load_request(self):
        if threading.current_thread() is threading.main_thread():
            return
        with self.lock:
            worker = threading.get_ident()
            self.load_counts[worker] += 1
            warmed = self.load_counts[worker] > 1
        # A successful fixture supplies real post-warmup operations held open
        # until delete_objects enters, rather than relying on scheduler luck.
        if warmed:
            with self.lock:
                self.load_active += 1
            try:
                if not self.bulk_entered.wait(2):
                    raise S3Error("FixtureDeleteNotStarted", 500)
            finally:
                with self.lock:
                    self.load_active -= 1

    def append(self, key, body, marker=False):
        self.counter += 1
        version = f"v{self.counter}" if self.status == "on" else "null"
        rows = self.versions.setdefault(key, [])
        if version == "null":
            rows[:] = [row for row in rows if row[0] != "null"]
        rows.append((version, body, marker))
        return version

    def create_bucket(self, **kwargs):
        self.calls.append(("create", kwargs))
        return {}

    def put_bucket_versioning(self, **kwargs):
        self.status = "on" if kwargs["VersioningConfiguration"]["Status"] == "Enabled" else "suspended"
        self.calls.append(("versioning", self.status))
        return {}

    def put_object(self, **kwargs):
        self.load_request()
        with self.lock:
            self.calls.append(("put", kwargs["Key"]))
            version = self.append(kwargs["Key"], kwargs["Body"])
            if self.mutation == "lost-seed" and kwargs["Key"] == "target/0000":
                self.versions[kwargs["Key"]] = []
            return {} if self.status == "off" else {"VersionId": version}

    def get_object(self, **kwargs):
        self.load_request()
        with self.lock:
            rows = self.versions.get(kwargs["Key"], [])
            if "VersionId" in kwargs:
                rows = [row for row in rows if row[0] == kwargs["VersionId"]]
            if not rows or rows[-1][2]:
                raise S3Error("NoSuchKey", 404)
            body = rows[-1][1]
            if self.mutation == "corrupt" and kwargs["Key"] == probe.CONTROL:
                body = b"torn-payload"
            if self.after_delete and kwargs["Key"] == probe.CONTROL and self.mutation in ("read-timeout", "incomplete-read"):
                from botocore.exceptions import IncompleteReadError, ReadTimeoutError
                error = (ReadTimeoutError(endpoint_url="http://node.invalid") if self.mutation == "read-timeout"
                         else IncompleteReadError(actual_bytes=1, expected_bytes=len(body)))
                self.mutation = None
                stream = io.BytesIO(body)
                stream.read = Mock(side_effect=error)
                return {"Body": stream}
            return {"Body": io.BytesIO(body)}

    def head_object(self, **kwargs):
        with self.lock:
            rows = self.versions.get(kwargs["Key"], [])
            if not rows or rows[-1][2]:
                raise S3Error("404", 404)
            return {"ContentLength": len(rows[-1][1])}

    def list_objects_v2(self, **kwargs):
        with self.lock:
            rows = [{"Key": key} for key, versions in sorted(self.versions.items())
                    if key.startswith(kwargs.get("Prefix", "")) and versions and not versions[-1][2]]
            if self.mutation == "hidden-residue" and self.after_delete and kwargs.get("Prefix") == "target/":
                rows = []
            return {"Contents": rows, "IsTruncated": False}

    def list_object_versions(self, **kwargs):
        with self.lock:
            result = {"Versions": [], "DeleteMarkers": [], "IsTruncated": False}
            for key, rows in self.versions.items():
                if key.startswith(kwargs.get("Prefix", "")):
                    for version, _, marker in rows:
                        result["DeleteMarkers" if marker else "Versions"].append({"Key": key, "VersionId": version})
            return result

    def delete_objects(self, **kwargs):
        if "VersionId" not in kwargs["Delete"]["Objects"][0]:
            with self.lock:
                self.observed_overlap |= self.load_active > 0
            self.bulk_entered.set()
        with self.lock:
            objects = kwargs["Delete"]["Objects"]
            self.calls.append(("delete", objects))
            explicit = "VersionId" in objects[0]
            self.after_delete |= not explicit
            if not explicit and self.mutation == "http503":
                self.mutation = None
                raise S3Error("ServiceUnavailable")
            if not explicit and self.mutation == "interrupt":
                raise KeyboardInterrupt()
            failed = self.mutation in ("object-error", "incomplete-object-error") and not explicit
            if explicit and self.mutation in ("cleanup-retry", "cleanup-blocked"):
                failed = self.mutation == "cleanup-blocked" or self.cleanup_failures == 0
                self.cleanup_failures += 1
            response = {"Deleted": [], "Errors": [], "ResponseMetadata": {
                "HTTPStatusCode": 200, "RequestId": "bulk-request", "RetryAttempts": 0}}
            for index, obj in enumerate(objects):
                key = obj["Key"]
                if failed and (explicit or index == 0):
                    code = "AccessDenied" if self.mutation == "cleanup-blocked" else "ServiceUnavailable"
                    response["Errors"].append({**obj, "Code": code})
                    continue
                if explicit:
                    self.versions[key] = [row for row in self.versions.get(key, []) if row[0] != obj["VersionId"]]
                elif self.mutation in ("residue", "hidden-residue") and index == 0:
                    pass
                elif self.status == "off" or self.mutation == "lost-history":
                    self.versions[key] = []
                else:
                    self.append(key, b"", marker=True)
                response["Deleted"].append(dict(obj))
            if not explicit and self.mutation in ("missing-ack", "incomplete-object-error"):
                response["Deleted"].pop()
            return response

    def list_multipart_uploads(self, **kwargs):
        return {"Uploads": []}

    def delete_bucket(self, **kwargs):
        assert not any(self.versions.values())
        self.deleted_bucket = True
        return {}


class BulkDeleteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_probe(self, case_id="S3C-213", mode="off", mutation=None, count=3):
        fake = VersionedS3(mutation)
        directory = self.root / f"{case_id}-{mode}-{mutation}"
        result = probe.run_case(case_id, mode, count, ["http://node1.invalid", "http://node2.invalid"],
                                directory, workers=2, factory=lambda _: fake)
        events = [json.loads(line) for line in (directory / "s3-events.jsonl").read_text().splitlines()]
        return result, fake, events

    def test_all_groups_and_version_states_execute_and_remove_versions_and_markers(self):
        for case_id in probe.GROUPS:
            for mode in probe.MODES:
                with self.subTest(case_id=case_id, mode=mode):
                    result, fake, events = self.run_probe(case_id, mode)
                    self.assertEqual(result["verdict"], "PASS", result)
                    self.assertEqual(result["cleanup"], "PASS")
                    self.assertEqual(result["remaining_versions"], 0)
                    self.assertTrue(fake.deleted_bucket)
                    self.assertTrue(any(row.get("kind") == "operation_time" and row["phase"] == "delete" for row in events))
                    if case_id == "S3C-1307":
                        self.assertEqual(result["recovery"], "PASS")
                    if case_id in ("S3C-1306", "S3C-1307"):
                        self.assertTrue(fake.observed_overlap)
                        self.assertGreater(result["load_overlap"]["successful_requests"], 0)
                        overlapping = [row for row in events if row.get("kind") == "load_overlap" and row["success"]]
                        self.assertTrue(overlapping)
                        interval = result["load_overlap"]
                        for row in overlapping:
                            self.assertLess(row["started_monotonic"], interval["delete_finished_monotonic"])
                            self.assertGreater(row["finished_monotonic"], interval["delete_started_monotonic"])
                            self.assertGreater(row["overlap_seconds"], 0)
                    if case_id == "S3C-214":
                        batches = [row for operation, row in fake.calls if operation == "delete"]
                        self.assertEqual(Counter(row["Key"] for row in batches[0])["target/0000"], 16)
                        self.assertEqual(result["requested_entries"], 20)

    def test_http200_object_error_is_a_failure_even_when_cleanup_succeeds(self):
        result, fake, _ = self.run_probe(mutation="object-error")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["http_503_responses"], 0)
        self.assertEqual(result["service_unavailable_entries"], 1)
        self.assertEqual(result["delete_error_entries"], 1)
        self.assertEqual(result["failure_phase"], "delete")
        self.assertEqual(result["cleanup"], "PASS")
        self.assertTrue(fake.deleted_bucket)

    def test_http503_is_one_response_and_is_not_expanded_into_object_errors(self):
        result, _, events = self.run_probe(mutation="http503")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["failure_phase"], "delete")
        self.assertEqual(result["http_503_responses"], 1)
        self.assertEqual(result["service_unavailable_entries"], 0)
        response = next(row for row in events if row.get("http_status") == 503)
        self.assertEqual(response["request_id"], "failed-request")
        self.assertEqual(response["sdk_retries"], 0)
        self.assertEqual(result["cleanup"], "PASS")

    def test_incomplete_mixed_response_keeps_per_object_error_evidence(self):
        result, _, events = self.run_probe(mutation="incomplete-object-error")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["error_code"], "IncompleteDeleteResponse")
        self.assertEqual(result["service_unavailable_entries"], 1)
        self.assertEqual(result["http_503_responses"], 0)
        response = next(row for row in events if row.get("kind") == "delete_response" and row["phase"] == "delete")
        self.assertEqual(response["errors"], [{"Key": "target/0000", "Code": "ServiceUnavailable"}])
        self.assertEqual(response["deleted"], [{"Key": "target/0001"}])
        self.assertFalse(response["complete"])
        self.assertEqual(result["cleanup"], "PASS")

    def test_load_client_is_reused_until_its_read_timeout_exceeds_remaining_budget(self):
        factory = Mock(side_effect=lambda remaining: Mock(put_object=Mock(return_value={})))
        with patch.object(probe.time, "monotonic", return_value=0) as clock:
            requests = probe.Requests("http://node.invalid", self.root / "reuse-events", 30, "load", factory)
            for instant in (0, 1, 19, 20):
                clock.return_value = instant
                requests.call("put_object", Bucket="owned", Key="hot", Body=b"payload")
            self.assertEqual(factory.call_count, 1)
            original = requests.s3
            clock.return_value = 21
            requests.call("put_object", Bucket="owned", Key="hot", Body=b"payload")
            self.assertIsNot(requests.s3, original)
            self.assertEqual(factory.call_args.args, (9,))

    def test_loaded_case_with_only_warmup_cannot_pass(self):
        worker = probe.load_worker
        def warmup_only(*args):
            arguments = list(args)
            arguments[7] = 1
            return worker(*arguments)
        for case_id in ("S3C-1306", "S3C-1307"):
            with self.subTest(case_id=case_id), patch.object(probe, "load_worker", warmup_only):
                result, fake, _ = self.run_probe(case_id=case_id)
                self.assertEqual(result["verdict"], "FAIL")
                self.assertEqual(result["error_code"], "LoadNotActive")
                self.assertFalse(fake.observed_overlap)
                self.assertEqual(result["load_overlap"]["successful_requests"], 0)
                self.assertTrue(all(row["successes"] == 1 and row["post_warmup_requests"] == 0 for row in result["load"]))
                self.assertEqual(result["cleanup"], "PASS")

    def test_completed_post_warmup_requests_cannot_substitute_for_overlap(self):
        # Reproduce a scheduling gap before the actual delete invocation:
        # every load request has already returned when deletion begins.
        original = probe.LoadWindow.begin_delete
        def after_workers(window, deadline):
            with window.condition:
                finished = window.condition.wait_for(lambda: window.finished_workers == window.workers, timeout=2)
            self.assertTrue(finished)
            original(window, deadline)
        worker = probe.load_worker
        def two_rounds(*args):
            arguments = list(args)
            arguments[7] = 2
            return worker(*arguments)
        for case_id in ("S3C-1306", "S3C-1307"):
            with self.subTest(case_id=case_id), patch.object(probe, "load_worker", two_rounds), \
                    patch.object(probe.LoadWindow, "begin_delete", after_workers), \
                    patch.object(VersionedS3, "load_request", lambda _self: None):
                result, _, _ = self.run_probe(case_id=case_id)
                self.assertEqual(result["verdict"], "FAIL")
                self.assertEqual(result["load_overlap"]["successful_requests"], 0)
                self.assertTrue(all(row["post_warmup_requests"] == 1 for row in result["load"]))
                self.assertEqual(result["cleanup"], "PASS")

    def test_recorded_overlap_uses_intersection_not_total_request_duration(self):
        for started, finished, expected in ((1, 4, 2), (3, 6, 2), (0, 1, 0), (5, 7, 0)):
            with self.subTest(started=started, finished=finished):
                window = probe.LoadWindow(1)
                window.started, window.finished = 2, 5
                with patch.object(probe.time, "monotonic", return_value=started):
                    entered = window.begin_request(0)
                with patch.object(probe.time, "monotonic", return_value=finished):
                    overlap = window.finish_request(0, entered, True)
                self.assertEqual(overlap["overlap_seconds"] if overlap else 0, expected)

    def test_stream_remains_active_until_body_is_consumed_and_closed(self):
        fake = VersionedS3()
        fake.put_object(Key=probe.CONTROL, Body=probe.PAYLOAD)
        window = probe.LoadWindow(1)
        requests = probe.Requests("http://node.invalid", self.root / "stream-events", 30, "load", lambda _: fake)
        requests.window, requests.worker = window, 0
        body = io.BytesIO(probe.PAYLOAD)
        read = body.read
        def during_read(size):
            self.assertIn(0, window.active)
            window.begin_delete(time.monotonic() + 1)
            window.finish_delete()
            return read(size)
        body.read = during_read
        with patch.object(fake, "get_object", return_value={"Body": body}):
            probe.read_exact(requests, "owned", probe.CONTROL, probe.PAYLOAD)
        self.assertTrue(body.closed)
        self.assertFalse(window.active)
        self.assertEqual(window.results[0]["overlapping_successes"], 1)

    def test_overlap_diagnostic_failure_still_closes_the_stream(self):
        body = io.BytesIO(probe.PAYLOAD)
        requests = probe.Requests("http://node.invalid", self.root / "close-events", 30, "load")
        with patch.object(requests, "call", return_value={"Body": body}), \
                patch.object(requests, "finish_read", side_effect=OSError("fixture evidence failure")):
            with self.assertRaises(OSError):
                probe.read_exact(requests, "owned", probe.CONTROL, probe.PAYLOAD)
        self.assertTrue(body.closed)

    def test_recovery_failure_does_not_change_a_passed_primary_verdict(self):
        original = probe.Requests.call
        def fail_recovery(requests, operation, **arguments):
            if requests.phase == "recovery" and operation == "put_object":
                raise probe.RequestFailure("RecoveryFixtureFailure")
            return original(requests, operation, **arguments)
        with patch.object(probe.Requests, "call", fail_recovery):
            result, _, _ = self.run_probe(case_id="S3C-1307")
        self.assertEqual(result["primary_verdict"], "PASS")
        self.assertEqual(result["recovery"], "FAIL")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["failure_phase"], "recovery")
        self.assertEqual(result["cleanup"], "PASS")

    def test_streamed_verification_failures_still_run_same_key_recovery(self):
        for mutation, code in (("read-timeout", "ReadTimeoutError"), ("incomplete-read", "IncompleteReadError")):
            with self.subTest(mutation=mutation):
                result, _, events = self.run_probe(case_id="S3C-1307", mutation=mutation)
                self.assertEqual(result["failure_phase"], "verify")
                self.assertEqual(result["error_code"], code)
                self.assertEqual(result["primary_verdict"], "FAIL")
                self.assertEqual(result["verdict"], "FAIL")
                self.assertEqual(result["recovery"], "PASS")
                self.assertTrue(any(row.get("kind") == "s3_request" and row["phase"] == "recovery" for row in events))
                self.assertEqual(result["cleanup"], "PASS")

    def test_real_s3_responses_keep_entry_counts_and_disable_transport_retries(self):
        calls = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                body = ET.fromstring(self.rfile.read(int(self.headers["Content-Length"])))
                keys = [row.text for row in body.iter() if row.tag.rsplit("}", 1)[-1] == "Key"]
                calls.append(keys)
                if server.top_level_failure:
                    result = ET.Element("Error")
                    ET.SubElement(result, "Code").text = "ServiceUnavailable"
                else:
                    result = ET.Element("DeleteResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
                    for index, key in enumerate(keys):
                        row = ET.SubElement(result, "Error" if index == 0 else "Deleted")
                        ET.SubElement(row, "Key").text = key
                        if index == 0:
                            ET.SubElement(row, "Code").text = "ServiceUnavailable"
                response = ET.tostring(result)
                self.send_response(503 if server.top_level_failure else 200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("x-amz-request-id", "real-http200")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.top_level_failure = False
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test-access", "RUSTFS_SECRET_KEY": "test-secret",
                                         "RUSTFS_SESSION_TOKEN": ""}):
                session = probe.Requests(f"http://127.0.0.1:{server.server_port}", self.root / "http-events", 30, "delete")
                errors = probe.delete_once(session, "owned-bucket", [{"Key": "duplicate"},
                                                                   {"Key": "duplicate"}, {"Key": "cold"}])
                server.top_level_failure = True
                failed_session = probe.Requests(f"http://127.0.0.1:{server.server_port}", self.root / "http503-events", 30, "delete")
                with self.assertRaises(probe.RequestFailure) as failure:
                    probe.delete_once(failed_session, "owned-bucket", [{"Key": "cold"}])
                self.assertEqual(failure.exception.code, "ServiceUnavailable")
                self.assertEqual(failed_session.http_503, 1)
            self.assertEqual(errors, [{"Key": "duplicate", "Code": "ServiceUnavailable"}])
            self.assertEqual(calls, [["duplicate", "duplicate", "cold"], ["cold"]])
            self.assertEqual(session.http_503, 0)
            records = [json.loads(line) for line in (self.root / "http-events").read_text().splitlines()]
            request = next(row for row in records if row["kind"] == "s3_request")
            self.assertEqual(request["request_id"], "real-http200")
            self.assertEqual(request["sdk_retries"], 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_missing_acknowledgment_residue_corruption_and_lost_history_fail(self):
        for mutation in ("missing-ack", "residue", "hidden-residue", "corrupt", "lost-history"):
            with self.subTest(mutation=mutation):
                result, _, _ = self.run_probe(mode="on", mutation=mutation)
                self.assertEqual(result["verdict"], "FAIL")
                self.assertEqual(result["cleanup"], "PASS")

    def test_verify_budget_is_shared_across_endpoints(self):
        clock = [0.0]
        original = probe.Requests.call
        def consuming_call(requests, operation, **arguments):
            response = original(requests, operation, **arguments)
            if requests.phase == "verify" and requests.endpoint == "http://node1.invalid" and operation == "get_object":
                clock[0] = 61.0
            return response
        with patch.object(probe.time, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(probe.Requests, "call", consuming_call):
            result, _, events = self.run_probe()
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["failure_phase"], "verify")
        self.assertEqual(result["error_code"], "PhaseDeadlineExceeded")
        self.assertFalse(any(row.get("kind") == "s3_request" and row.get("phase") == "verify"
                             and row.get("endpoint") == "http://node2.invalid" for row in events))
        self.assertEqual(result["cleanup"], "PASS")

    def test_cleanup_retry_preserves_first_error_and_cannot_turn_case_green(self):
        result, _, _ = self.run_probe(mutation="cleanup-retry")
        self.assertEqual(result["primary_verdict"], "PASS")
        self.assertEqual(result["cleanup"], "PASS")
        self.assertTrue(result["cleanup_had_errors"])
        self.assertEqual(result["verdict"], "FAIL")
        self.assertGreater(result["cleanup_error_entries"], 0)

    def test_unrecoverable_cleanup_keeps_bucket_and_fails(self):
        result, fake, _ = self.run_probe(mutation="cleanup-blocked")
        self.assertEqual(result["cleanup"], "FAIL")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertFalse(fake.deleted_bucket)

    def test_interrupt_saves_primary_failure_and_runs_cleanup(self):
        result, fake, _ = self.run_probe(mutation="interrupt")
        self.assertTrue(result["interrupted"])
        self.assertEqual(result["failure_phase"], "delete")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertTrue(fake.deleted_bucket)

    def test_failed_get_load_is_not_mistaken_for_successful_warmup(self):
        result, fake, _ = self.run_probe(case_id="S3C-1306", mutation="corrupt")
        self.assertEqual(result["failure_phase"], "load-start")
        self.assertEqual(result["error_code"], "LoadNotReady")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertFalse(any(operation == "delete" and "VersionId" not in rows[0]
                             for operation, rows in fake.calls if operation == "delete"))

    def test_expired_phase_never_dispatches_a_request(self):
        fake = VersionedS3()
        requests = probe.Requests("http://node.invalid", self.root / "events", -1, "delete", lambda _: fake)
        with self.assertRaises(probe.RequestFailure) as error:
            requests.call("put_object", Bucket="owned", Key="key", Body=b"data")
        self.assertEqual(error.exception.code, "PhaseDeadlineExceeded")
        self.assertEqual(fake.calls, [])

    def test_missing_seed_cannot_make_an_idempotent_delete_look_like_a_regression_pass(self):
        result, fake, _ = self.run_probe(mutation="lost-seed")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["failure_phase"], "prepare")
        self.assertEqual(result["error_code"], "SeedInventoryMismatch")
        self.assertFalse(fake.after_delete)

    def test_empty_or_repeated_pagination_is_rejected(self):
        for rows in ([], [{"Key": "same", "VersionId": "v1"}]):
            requests = probe.Requests("http://node.invalid", self.root / "events", 30, "cleanup")
            with patch.object(requests, "call", return_value={"Versions": rows, "IsTruncated": True,
                                                              "NextKeyMarker": "same", "NextVersionIdMarker": "v1"}):
                with self.assertRaises(probe.RequestFailure):
                    probe.listed(requests, "owned", versions=True)

    def test_unowned_receipt_cannot_trigger_cleanup(self):
        fake = VersionedS3()
        state = self.root / "state.json"
        state.write_text(json.dumps({"created": False, "bucket": "other", "endpoint": "http://node.invalid"}))
        requests = probe.Requests("http://node.invalid", self.root / "events", 30, "cleanup", lambda _: fake)
        with self.assertRaises((probe.RequestFailure, ValueError)):
            probe.cleanup_bucket(requests, state, {})
        self.assertEqual(fake.calls, [])

    def test_failed_hotspot_delete_still_checks_recovery_and_preserves_failure(self):
        result, fake, _ = self.run_probe(case_id="S3C-1307", mode="on", mutation="http503")
        self.assertEqual(result["failure_phase"], "delete")
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["recovery"], "PASS")
        self.assertTrue(fake.deleted_bucket)

    def test_resumed_cleanup_keeps_previous_failure_history(self):
        fake = VersionedS3()
        requests = probe.Requests("http://node.invalid", self.root / "events", 30, "cleanup", lambda _: fake)
        state = self.root / "state.json"
        receipt = probe.prepare(requests, state, "test")
        receipt.update(cleanup="FAIL", cleanup_had_errors=True, cleanup_error_entries=7)
        probe.save(state, receipt)
        result = {}
        probe.cleanup_bucket(requests, state, result)
        self.assertEqual(result["cleanup"], "PASS")
        self.assertTrue(result["cleanup_had_errors"])
        self.assertEqual(result["cleanup_error_entries"], 7)

    def test_cli_records_every_unrun_case_after_failed_cleanup(self):
        old_handler = signal.getsignal(signal.SIGTERM)
        self.addCleanup(signal.signal, signal.SIGTERM, old_handler)
        directory = self.root / "cli"
        with patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test-access", "RUSTFS_SECRET_KEY": "test-secret"}), \
                patch.object(probe, "run_case", return_value={"verdict": "FAIL", "cleanup": "FAIL"}) as runner:
            self.assertEqual(probe.main(["--endpoint", "http://node.invalid", "--result-dir", str(directory)]), 1)
        document = json.loads((directory / "summary.json").read_text())
        self.assertEqual(document["expected_cases"], 24)
        self.assertEqual(len(document["results"]), 24)
        self.assertEqual(sum(row["verdict"] == "NOT_RUN" for row in document["results"]), 23)
        self.assertEqual(runner.call_count, 1)
        self.assertIn("NOT_RUN", (directory / "summary.md").read_text())

    def test_invalid_options_do_not_create_artifacts_or_contact_a_server(self):
        invalid = [["--endpoint", "http://user:secret@node.invalid"],
                   ["--endpoint", "ftp://node.invalid"], ["--endpoint", "http://node.invalid", "--workers", "0"],
                   ["--endpoint", "http://node.invalid", "--batch-sizes", "1001"]]
        with patch.object(probe, "run_case") as runner:
            for options in invalid:
                with self.subTest(options=options), self.assertRaises(SystemExit) as error:
                    probe.main(options + ["--result-dir", str(self.root / "invalid")])
                self.assertEqual(error.exception.code, 2)
            runner.assert_not_called()
        self.assertFalse((self.root / "invalid").exists())

    def test_explicit_credentials_do_not_mix_with_ambient_aws_credentials_or_tokens(self):
        with patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "explicit", "RUSTFS_SECRET_KEY": "explicit-secret",
                                     "AWS_SESSION_TOKEN": "ambient-token"}, clear=True):
            self.assertEqual(probe.credentials(), ("explicit", "explicit-secret", None))
        with patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "partial", "AWS_ACCESS_KEY_ID": "ambient",
                                     "AWS_SECRET_ACCESS_KEY": "ambient-secret"}, clear=True):
            with self.assertRaises(probe.RequestFailure):
                probe.credentials()

    def test_full_s3_suite_selects_new_cases_without_deploying_in_dry_run(self):
        script = Path(__file__).with_name("rustfs-s3-compat-test.sh")
        result = subprocess.run(["bash", str(script), "--topology", "single-single", "--dry-run", "-y",
                                 "--cases", ",".join(probe.GROUPS)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for case_id in probe.GROUPS:
            self.assertIn(f"[DRY-RUN] {case_id} ", result.stdout)

    def test_topology_wrapper_uses_configured_nodes_and_keeps_credentials_out_of_arguments(self):
        script = Path(__file__).with_name("rustfs-s3-compat-test.sh")
        command = r'''
source "$RUNNER"
ENDPOINT=http://primary.invalid:9000
NODES=(configured-a configured-b configured-c configured-d)
CURRENT_TOPOLOGY=multi-multi
S3_BULK_RESULTS_DIR="$RESULT_ROOT"
ACCESS_KEY=test-access
SECRET_KEY=must-not-appear-in-arguments
python3() { printf '%s\n' "$@" > "$CALL_LOG"; }
test_s3c213
'''
        result = subprocess.run(["bash", "-c", command], capture_output=True, text=True, timeout=10,
            env={**os.environ, "RUNNER": str(script), "RESULT_ROOT": str(self.root / "wrapper"),
                 "CALL_LOG": str(self.root / "call-log")})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        arguments = (self.root / "call-log").read_text()
        self.assertIn("http://primary.invalid:9000", arguments)
        for node in ("configured-b", "configured-c", "configured-d"):
            self.assertIn(f"http://{node}:9000", arguments)
        self.assertNotIn("must-not-appear-in-arguments", arguments)


if __name__ == "__main__":
    unittest.main()
