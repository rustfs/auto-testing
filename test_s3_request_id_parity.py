"""Tests for the real-listener request ID probe and its completion matching."""

import os
from pathlib import Path
import unittest

from scripts.s3_request_id_parity import exercise, response_id, verify_completions


class RequestIdTests(unittest.TestCase):
    def setUp(self):
        self.responses = [("/bucket/existing", 200, "first"), ("/bucket/missing", 404, "second")]
        self.events = [
            {"event": "http_request_completed", "method": "GET", "uri": path, "status_code": status, "request_id": rid}
            for path, status, rid in self.responses
        ]

    def test_matches_by_method_path_and_status(self):
        others = [dict(self.events[0], **change) for change in
                  ({"event": "started"}, {"method": "PUT"}, {"uri": "/unrelated"}, {"status_code": 304})]
        verify_completions(self.responses, others + list(reversed(self.events)))

    def test_swapped_log_ids_fail(self):
        self.events[0]["request_id"], self.events[1]["request_id"] = "second", "first"
        with self.assertRaisesRegex(ValueError, "completion request ID differs"):
            verify_completions(self.responses, self.events)

    def test_missing_or_duplicate_completion_fails(self):
        for events, count in [(self.events[:1], 0), (self.events + self.events, 2)]:
            with self.subTest(count=count), self.assertRaisesRegex(ValueError, f"found {count}"):
                verify_completions(self.responses, events)

    def test_reused_request_id_fails_even_if_each_log_matches(self):
        responses = [(p, s, "same-id") for p, s, _ in self.responses]
        events = [dict(e, request_id="same-id") for e in self.events]
        with self.assertRaisesRegex(ValueError, "reused a request ID"):
            verify_completions(responses, events)

    def test_error_xml_and_both_headers_must_agree(self):
        headers = {"x-amz-request-id": "second", "x-request-id": "second"}
        xml = b"<Error><Code>NoSuchKey</Code><RequestId>second</RequestId></Error>"
        self.assertEqual(response_id(404, headers, xml), "second")
        with self.assertRaisesRegex(ValueError, "XML request ID differs"):
            response_id(404, headers, xml.replace(b"second", b"first"))
        with self.assertRaisesRegex(ValueError, "missing or different"):
            response_id(404, {"x-amz-request-id": "second"}, xml)

    @unittest.skipUnless(os.environ.get("RUSTFS_BIN"), "set RUSTFS_BIN to test a local RustFS binary")
    def test_request_id_parity_on_local_binary(self):
        exercise(Path(os.environ["RUSTFS_BIN"]))


if __name__ == "__main__":
    unittest.main()
