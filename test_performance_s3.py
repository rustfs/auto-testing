#!/usr/bin/env python3
"""Cleanup oracles, including an actual HTTP 200 S3 DeleteResult error body."""

from collections import deque
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import xml.etree.ElementTree as ET

from scripts.performance_s3 import (Session, RequestFailure, cleanup, delete_batch,
                                    new_bucket, prepare, remaining_objects, save)


class FakeS3:
    def __init__(self, keys=()):
        self.keys = set(keys)
        self.batches = []
        self.responses = deque()
        self.uploads = [{"Key": "unfinished", "UploadId": "upload-1"}]
        self.bucket_deleted = False

    def create_bucket(self, **kwargs):
        return {}

    def put_object(self, **kwargs):
        self.keys.add(kwargs["Key"])
        return {}

    def list_objects_v2(self, **kwargs):
        offset = int(kwargs.get("ContinuationToken", "0"))
        keys = sorted(self.keys)[offset:offset + 1000]
        more = len(self.keys) > offset + len(keys)
        return {"Contents": [{"Key": key} for key in keys], "IsTruncated": more,
                "NextContinuationToken": str(offset + len(keys))}

    def delete_objects(self, **kwargs):
        keys = [row["Key"] for row in kwargs["Delete"]["Objects"]]
        self.batches.append(keys)
        response = self.responses.popleft() if self.responses else {"Deleted": [{"Key": key} for key in keys]}
        if isinstance(response, Exception):
            raise response
        for row in response.get("Deleted", []):
            if isinstance(row, dict):
                self.keys.discard(row.get("Key"))
        return response

    def list_multipart_uploads(self, **kwargs):
        return {"Uploads": list(self.uploads)}

    def abort_multipart_upload(self, **kwargs):
        self.uploads.clear()
        return {}

    def delete_bucket(self, **kwargs):
        assert not self.keys and not self.uploads
        self.bucket_deleted = True
        return {}


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / "state.json"
        self.events = self.root / "events.jsonl"
        self.fake = FakeS3()
        self.session = Session("http://example.invalid", self.events, 30, lambda _: self.fake)
        self.document = prepare(self.session, self.state, "test")
        self.bucket = self.document["bucket"]

    def records(self):
        return [json.loads(line) for line in self.events.read_text().splitlines()]

    def test_2500_objects_use_three_batches_and_abort_uploads_before_bucket_delete(self):
        self.fake.keys = {f"key-{i:04}" for i in range(2500)}
        document = cleanup(self.session, self.state)
        self.assertEqual([len(batch) for batch in self.fake.batches], [1000, 1000, 500])
        self.assertEqual(document["cleanup"], "PASS")
        self.assertEqual(document["remaining_objects"], 0)
        self.assertEqual(document["cleanup_error_entries"], 0)
        self.assertTrue(self.fake.bucket_deleted)
        self.assertEqual(document, json.loads(self.state.read_text()))

    def test_retry_only_failed_keys_and_keep_error_evidence_after_recovery(self):
        self.fake.keys = {"a", "b"}
        self.fake.responses.append({"Deleted": [{"Key": "a"}],
                                    "Errors": [{"Key": "b", "Code": "ServiceUnavailable"}],
                                    "ResponseMetadata": {"HTTPStatusCode": 200}})
        with patch("scripts.performance_s3.time.sleep"):
            document = cleanup(self.session, self.state)
        self.assertEqual(self.fake.batches, [["a", "b"], ["b"]])
        self.assertEqual(document["cleanup"], "PASS")
        self.assertEqual(document["cleanup_error_entries"], 1)
        self.assertEqual(document["cleanup_http_503_attempts"], 0)
        self.assertEqual(document["cleanup_request_failures"], 0)

    def test_one_http_503_is_one_request_failure_and_has_1000_affected_keys(self):
        failure = RuntimeError()
        failure.response = {"Error": {"Code": "ServiceUnavailable"},
                            "ResponseMetadata": {"HTTPStatusCode": 503, "RetryAttempts": 0,
                                                 "RequestId": "test-id",
                                                 "HTTPHeaders": {"x-rustfs-error-cause": "lock", "retry-after": "1"}}}
        self.fake.keys = {f"key-{i:04}" for i in range(1000)}
        self.fake.responses.append(failure)
        with patch("scripts.performance_s3.time.sleep"):
            document = cleanup(self.session, self.state)
        self.assertEqual(document["cleanup_http_503_attempts"], 1)
        self.assertEqual(document["cleanup_request_failures"], 1)
        self.assertEqual(document["cleanup_error_entries"], 1000)
        request = next(row for row in self.records() if row.get("http_status") == 503)
        self.assertEqual(request["request_id"], "test-id")
        self.assertEqual(request["error_cause"], "lock")
        batch = next(row for row in self.records() if row["kind"] == "delete_batch")
        self.assertEqual(batch["error_origin"], "request_or_protocol_failure")

    def test_non_retryable_error_leaves_exact_residue_and_does_not_delete_bucket(self):
        self.fake.keys = {"a", "b"}
        self.fake.responses.append({"Deleted": [{"Key": "a"}], "Errors": [{"Key": "b", "Code": "AccessDenied"}]})
        document = cleanup(self.session, self.state)
        self.assertEqual(document["cleanup"], "FAIL")
        self.assertEqual(document["remaining_objects"], 1)
        self.assertEqual(document["remaining_sample"], ["b"])
        self.assertEqual(len(self.fake.batches), 1)
        self.assertFalse(self.fake.bucket_deleted)

    def test_finite_retries_exhaust_without_clearing_or_hiding_errors(self):
        self.fake.keys = {"a"}
        self.fake.responses.extend([{"Errors": [{"Key": "a", "Code": "ServiceUnavailable"}]}] * 3)
        with patch("scripts.performance_s3.time.sleep"):
            document = cleanup(self.session, self.state)
        self.assertEqual(document["cleanup_error_entries"], 3)
        self.assertEqual(document["remaining_objects"], 1)
        self.assertEqual(len(self.fake.batches), 3)

    def test_missing_duplicate_foreign_or_malformed_delete_acknowledgments_fail_closed(self):
        for response in ({}, {"Deleted": [{"Key": "a"}, {"Key": "a"}]},
                         {"Deleted": [{"Key": "foreign"}]}, {"Errors": [{"Key": "a"}]},
                         {"Deleted": None}, {"Deleted": [{}]}, {"Errors": "invalid"}):
            with self.subTest(response=response):
                self.fake.responses.append(response)
                pending, errors = delete_batch(self.session, self.bucket, ["a"], 3)
                self.assertEqual(pending, ["a"])
                self.assertEqual(errors, 1)
                self.assertEqual(len(self.fake.responses), 0)

    def test_invalid_receipt_never_calls_delete_or_list(self):
        for mutation in ({"created": False}, {"bucket": "existing-user-bucket"},
                         {"endpoint": "http://elsewhere.invalid"}, {"version": 9}):
            with self.subTest(mutation=mutation):
                save(self.state, {**self.document, **mutation})
                with self.assertRaises(ValueError):
                    cleanup(self.session, self.state)
                self.assertFalse(self.fake.bucket_deleted)
                self.assertEqual(self.fake.batches, [])

    def test_deadline_saves_failure_with_unknown_residue(self):
        self.session.deadline = time.monotonic() - 1
        document = cleanup(self.session, self.state)
        self.assertEqual(document["cleanup"], "FAIL")
        self.assertIsNone(document["remaining_objects"])
        self.assertEqual(document["cleanup_error_code"], "CleanupDeadlineExceeded")

    def test_resume_preserves_previous_error_counts(self):
        self.fake.keys = {"a"}
        self.fake.responses.append({"Errors": [{"Key": "a", "Code": "ServiceUnavailable"}]})
        first = cleanup(self.session, self.state, attempts=1)
        second = cleanup(Session(self.session.endpoint, self.events, 30, lambda _: self.fake), self.state)
        self.assertEqual(first["cleanup"], "FAIL")
        self.assertEqual(second["cleanup"], "PASS")
        self.assertEqual(second["cleanup_error_entries"], 1)

    def test_remaining_count_uses_validated_pagination_and_bounded_sample(self):
        self.fake.keys = {f"key-{i:04}" for i in range(2500)}
        count, sample = remaining_objects(self.session, self.bucket)
        self.assertEqual(count, 2500)
        self.assertEqual(len(sample), 20)
        self.fake.list_objects_v2 = lambda **_: {"Contents": [{"Key": "a"}], "IsTruncated": True}
        with self.assertRaisesRegex(RequestFailure, "InvalidListContinuation"):
            remaining_objects(self.session, self.bucket)

    def test_malformed_object_or_upload_lists_are_saved_as_failure(self):
        for operation, response in (("list_objects_v2", {"Contents": [{}]}),
                                    ("list_objects_v2", {"Contents": [], "IsTruncated": True}),
                                    ("list_multipart_uploads", {"Uploads": [{}]})):
            with self.subTest(operation=operation, response=response):
                fake = FakeS3()
                setattr(fake, operation, lambda **_: response)
                save(self.state, self.document)
                document = cleanup(Session(self.session.endpoint, self.events, 30, lambda _: fake), self.state)
                self.assertEqual(document["cleanup"], "FAIL")
                self.assertIn("Invalid", document["cleanup_error_code"])

    def test_invalid_batches_and_prefixes_never_reach_s3(self):
        for keys in ([], ["a"] * 2, list(map(str, range(1001))), [{}]):
            with self.assertRaises(ValueError):
                delete_batch(self.session, self.bucket, keys, 3)
        for prefix in ("Bad", "-bad", "a..b", "a.-b", "a/b", ""):
            with self.assertRaises(ValueError):
                new_bucket(prefix)
        self.assertLessEqual(len(new_bucket("a" * 63)), 63)

    def test_probe_reports_original_delete_failure_even_when_finally_cleanup_recovers(self):
        from scripts.performance_delete_probe import run_case
        self.fake.responses.append({"Errors": [{"Key": "delete/0000", "Code": "ServiceUnavailable"}],
                                    "ResponseMetadata": {"HTTPStatusCode": 200}})
        def session_factory(endpoint, events, budget):
            return Session(endpoint, events, budget, lambda _: self.fake)
        with patch("scripts.performance_delete_probe.Session", side_effect=session_factory):
            result = run_case([self.session.endpoint], self.session.endpoint, 1, False, self.root)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["delete_error_entries"], 1)
        self.assertEqual(result["delete_http_503_attempts"], 0)
        self.assertEqual(result["remaining_objects_before_cleanup"], 1)
        self.assertEqual(result["cleanup"], "PASS")
        self.assertEqual(result["remaining_objects_after_cleanup"], 0)

    def test_probe_cleanup_runs_after_setup_put_failure(self):
        from scripts.performance_delete_probe import run_case
        self.fake.put_object = lambda **_: (_ for _ in ()).throw(RequestFailure("PutFailed"))
        with patch("scripts.performance_delete_probe.Session", side_effect=lambda endpoint, events, budget:
                   Session(endpoint, events, budget, lambda _: self.fake)):
            result = run_case([self.session.endpoint], self.session.endpoint, 1, False, self.root)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["cleanup"], "PASS")
        self.assertTrue(self.fake.bucket_deleted)


class HttpDeleteTests(unittest.TestCase):
    def test_boto3_distinguishes_http_200_xml_key_errors_from_http_503(self):
        try:
            import boto3  # noqa: F401
        except ImportError:
            self.skipTest("boto3 required for HTTP integration test")
        replies = deque([
            (200, '<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
             '<Deleted><Key>a</Key></Deleted><Error><Key>b</Key><Code>ServiceUnavailable</Code>'
             '<Message>The service is unavailable. Please retry.</Message></Error></DeleteResult>'),
            (200, '<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
             '<Deleted><Key>b</Key></Deleted></DeleteResult>'),
            (503, '<Error><Code>ServiceUnavailable</Code><Message>Please retry</Message></Error>'),
        ])
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                requests.append([element.text for element in ET.fromstring(body).iter()
                                 if element.tag.split("}")[-1] == "Key"])
                status, payload = replies.popleft()
                self.send_response(status)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(payload.encode())))
                self.send_header("x-amz-request-id", "http-test")
                self.end_headers()
                self.wfile.write(payload.encode())

        with tempfile.TemporaryDirectory() as folder:
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                with patch.dict("os.environ", WARP_ACCESS_KEY="test", WARP_SECRET_KEY="test-secret"):
                    session = Session(f"http://127.0.0.1:{server.server_port}", Path(folder) / "events.jsonl", 10)
                    with patch("scripts.performance_s3.time.sleep"):
                        pending, errors = delete_batch(session, "test-bucket", ["a", "b"], 3)
                    self.assertEqual((pending, errors), ([], 1))
                    self.assertEqual(session.http_503, 0)
                    pending, errors = delete_batch(session, "test-bucket", ["c"], 1)
                    self.assertEqual((pending, errors), (["c"], 1))
                    self.assertEqual(session.http_503, 1)
                    self.assertEqual(session.request_failures, 1)
                    self.assertEqual(requests, [["a", "b"], ["b"], ["c"]])
                    self.assertFalse(replies)  # SDK did not retry the 503.
                    self.assertNotIn("test-secret", session.events.read_text())
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
