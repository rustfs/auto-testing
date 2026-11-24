#!/usr/bin/env python3
"""Exercise UploadPart body deadlines against one disposable loopback RustFS process."""

import argparse
import base64
import binascii
from contextlib import closing
from http.client import HTTPConnection
import json
from pathlib import Path
import secrets
import tempfile
import time
from urllib.parse import urlencode
from xml.etree import ElementTree

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials

if __package__:
    from .local_rustfs import HOST, local_rustfs
else:
    from local_rustfs import HOST, local_rustfs


REGION = "us-east-1"
BUCKET = "upload-part-regression"
KEY = "object"
IDLE_SECONDS = 2


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def short_body(payload):
    crc = base64.b64encode(binascii.crc32(payload).to_bytes(4, "big"))
    wire = (f"{len(payload):x}\r\n".encode() + payload + b"\r\n0\r\nx-amz-checksum-crc32:"
            + crc + b"\r\n\r\n")
    return wire, {"Content-Length": str(len(wire)), "Content-Encoding": "aws-chunked",
                  "x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                  "x-amz-trailer": "x-amz-checksum-crc32", "x-amz-decoded-content-length": str(len(payload) + 1)}


def put_wire(port, credentials, upload_id, body, headers):
    query = urlencode({"partNumber": "1", "uploadId": upload_id})
    path = f"/{BUCKET}/{KEY}?{query}"
    request = AWSRequest(method="PUT", url=f"http://{HOST}:{port}{path}", headers=headers)
    SigV4Auth(credentials, "s3", REGION).add_auth(request)
    # endheaders sends the prefix without padding to Content-Length. Keep the write side open
    # while reading the response so only the server's deadline can finish a stalled request.
    with closing(HTTPConnection(HOST, port, timeout=IDLE_SECONDS + 10)) as conn:
        conn.putrequest("PUT", path, skip_accept_encoding=True)
        for name, value in request.headers.items():
            conn.putheader(name, value)
        conn.endheaders(body)
        response = conn.getresponse()
        return response.status, response.read()


def expect_error(status, body, expected):
    require(status == 400, f"expected HTTP 400 {expected}, got HTTP {status}")
    root = ElementTree.fromstring(body)
    code = root.findtext("{*}Code")
    require(root.tag.rsplit("}", 1)[-1] == "Error" and code == expected,
            f"expected S3 {expected}, got {code!r}")


def exercise(client, port, credentials):
    client.create_bucket(Bucket=BUCKET)
    obj = {"Bucket": BUCKET, "Key": KEY}
    session = {**obj, "UploadId": client.create_multipart_upload(**obj)["UploadId"]}
    prefix = b"partial"
    stalled = (prefix, {"Content-Length": str(len(prefix) + 1), "x-amz-content-sha256": "UNSIGNED-PAYLOAD"})
    started = time.monotonic()
    expect_error(*put_wire(port, credentials, session["UploadId"], *stalled), "RequestTimeout")
    elapsed = time.monotonic() - started
    require(elapsed >= IDLE_SECONDS, "RustFS returned RequestTimeout before the configured idle period")
    require(not client.list_parts(**session).get("Parts"), "stalled upload committed a part")

    payload = b"same-number retry"
    etag = client.upload_part(**session, PartNumber=1, Body=payload)["ETag"]
    expect_error(*put_wire(port, credentials, session["UploadId"], *short_body(prefix)), "IncompleteBody")
    parts = client.list_parts(**session).get("Parts", [])
    observed = [(p["PartNumber"], p["Size"], p["ETag"]) for p in parts]
    require(observed == [(1, len(payload), etag)], "short overwrite changed the committed part")
    client.complete_multipart_upload(**session, MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]})
    with closing(client.get_object(**obj)["Body"]) as stream:
        require(stream.read() == payload, "completed object differs from retry bytes")
    return {"case_id": "upload-part-client-read-timeout", "stalled": "RequestTimeout", "idle_seconds_observed": round(elapsed, 3),
            "short": "IncompleteBody", "retry_bytes": len(payload)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustfs-bin", type=Path, required=True)
    args = parser.parse_args()
    credentials = Credentials("local-upload-test", secrets.token_hex(24))
    with tempfile.TemporaryDirectory(prefix="rustfs-upload-part-") as temporary, local_rustfs(
        args.rustfs_bin, credentials, Path(temporary), {"RUSTFS_HTTP_REQUEST_BODY_READ_TIMEOUT": str(IDLE_SECONDS)}
    ) as port:
        config = Config(signature_version="s3v4", s3={"addressing_style": "path"}, proxies={},
                        retries={"total_max_attempts": 1}, connect_timeout=3, read_timeout=12,
                        request_checksum_calculation="when_required")
        with closing(boto3.client("s3", endpoint_url=f"http://{HOST}:{port}", region_name=REGION,
                          aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key,
                          config=config)) as client:
            print(json.dumps(exercise(client, port, credentials)))


if __name__ == "__main__":
    main()
