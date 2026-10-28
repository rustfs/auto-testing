"""Contract checks for canonical webhook evidence parsing."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_webhooks import audit_summary, event_summary, records


class ValidationWebhooksTest(unittest.TestCase):
    def test_event_requires_exactly_one_matching_put(self):
        event = {"Records": [{
            "eventName": "s3:ObjectCreated:Put",
            "s3": {"bucket": {"name": "bucket"}, "object": {"key": "round+trip.bin"}},
        }]}
        self.assertEqual(event_summary([event], "bucket", "round trip.bin")["delivery_count"], 1)
        with self.assertRaisesRegex(ValueError, "found 2"):
            event_summary([event, event], "bucket", "round trip.bin")
        with self.assertRaisesRegex(ValueError, "unexpected event type"):
            event_summary([{"Records": [{**event["Records"][0], "eventName": "s3:ObjectRemoved:Delete"}]}], "bucket", "round trip.bin")

    def test_audit_requires_correlated_operations_and_no_secret(self):
        put = {"api": {"name": "PutObject", "bucket": "bucket", "object": "key", "status_code": 200},
               "requestID": "request-put"}
        get = {"api": {"name": "GetObject", "bucket": "bucket", "object": "key", "status_code": 200},
               "requestID": "request-get"}
        self.assertEqual(set(audit_summary([put, get], "bucket", "key")["operations"]), {"PutObject", "GetObject"})
        with self.assertRaisesRegex(ValueError, "share a request ID"):
            audit_summary([put, {**get, "requestID": "request-put"}], "bucket", "key")
        with self.assertRaisesRegex(ValueError, "lacks requestID"):
            audit_summary([put, {**get, "requestID": ""}], "bucket", "key")
        with patch.dict(os.environ, {"RUSTFS_SECRET_KEY": "private-value"}):
            with self.assertRaisesRegex(ValueError, "exposes RUSTFS_SECRET_KEY"):
                audit_summary([put, {**get, "requestHeader": {"x-secret": "private-value"}}], "bucket", "key")
            public_identity = {**put, "accessKey": "public-access-key"}
            self.assertEqual(
                audit_summary([public_identity, get], "bucket", "key")["secret_check"],
                "PASS",
            )

    def test_records_rejects_non_object_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text('{}\n[]\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "line 2"):
                records(path)

    def test_audit_accepts_s3_operation_names_and_keeps_boundaries(self):
        def entry(name, request_id, key="key"):
            return {"api": {"name": name, "bucket": "bucket", "object": key, "status_code": 200},
                    "requestID": request_id}
        put = entry("s3:PutObject", "put-id")
        get = entry("s3:GetObject", "get-id")
        result = audit_summary([put, get], "bucket", "key")
        self.assertEqual(result["operations"]["GetObject"]["api_name"], "s3:GetObject")
        wrapped = [{"Records": [put]}, {"Records": [get]}]
        self.assertEqual(audit_summary(wrapped, "bucket", "key"), result)
        with self.assertRaisesRegex(ValueError, "array of objects"):
            audit_summary([{"Records": [put, "invalid"]}], "bucket", "key")
        with self.assertRaisesRegex(ValueError, "not both delivered"):
            audit_summary([put, entry("s3:GetObject", "other-id", "other-key")], "bucket", "key")
        with self.assertRaisesRegex(ValueError, "not both delivered"):
            audit_summary([put, entry("admin:GetObject", "admin-id")], "bucket", "key")
        with patch.dict(os.environ, {"RUSTFS_SECRET_KEY": "private-value"}):
            with self.assertRaisesRegex(ValueError, "exposes RUSTFS_SECRET_KEY"):
                audit_summary([put, {**get, "accessKey": "private-value"}], "bucket", "key")
            with self.assertRaisesRegex(ValueError, "exposes RUSTFS_SECRET_KEY"):
                audit_summary([{"Records": [put, {**get, "accessKey": "private-value"}]}], "bucket", "key")


if __name__ == "__main__":
    unittest.main()
