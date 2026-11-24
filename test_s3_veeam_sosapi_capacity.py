from datetime import datetime, timezone
import hashlib
from io import BytesIO
import unittest

import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber

from scripts.s3_veeam_sosapi_capacity import BUCKET, CAPACITY_KEY, read_capacity


class CapacityResponseTests(unittest.TestCase):
    def setUp(self):
        self.client = boto3.client("s3", region_name="us-east-1",
                                   aws_access_key_id="fixture", aws_secret_access_key="disposable-secret")
        self.addCleanup(self.client.close)
        self.request = {"Bucket": BUCKET, "Key": CAPACITY_KEY}

    def response(self, available, used):
        body = (f"<CapacityInfo><Capacity>100</Capacity><Available>{available}</Available>"
                f"<Used>{used}</Used></CapacityInfo>").encode()
        return {
            "Body": StreamingBody(BytesIO(body), len(body)),
            "ContentType": "application/xml",
            "ETag": '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"',
            "LastModified": datetime.now(timezone.utc),
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

    def test_accepts_unknown_then_exact_usage_with_distinct_etag(self):
        with Stubber(self.client) as stub:
            for used in (-1, 5):
                stub.add_response("get_object", self.response(70, used), self.request)
            pending, pending_etag = read_capacity(self.client)
            certified, certified_etag = read_capacity(self.client)
            self.assertEqual(pending, {"Capacity": 100, "Available": 70, "Used": -1})
            self.assertEqual(certified["Used"], 5)
            self.assertNotEqual(pending_etag, certified_etag)

    def test_rejects_503_on_the_first_get(self):
        with Stubber(self.client) as stub:
            stub.add_client_error("get_object", service_error_code="ServiceUnavailable",
                                  http_status_code=503, expected_params=self.request)
            with self.assertRaisesRegex(RuntimeError, "expected HTTP 200, got HTTP 503"):
                read_capacity(self.client)
            stub.assert_no_pending_responses()

    def test_rejects_overstated_free_space_and_other_negative_values(self):
        for available, used in ((101, -1), (96, 5), (70, -2), (-1, -1)):
            with self.subTest(available=available, used=used), Stubber(self.client) as stub:
                stub.add_response("get_object", self.response(available, used), self.request)
                with self.assertRaisesRegex(RuntimeError, "invalid capacity figures"):
                    read_capacity(self.client)

    def test_rejects_wrong_etag(self):
        response = self.response(70, -1)
        response["ETag"] = '"wrong"'
        with Stubber(self.client) as stub:
            stub.add_response("get_object", response, self.request)
            with self.assertRaisesRegex(RuntimeError, "ETag did not match"):
                read_capacity(self.client)


if __name__ == "__main__":
    unittest.main()
