#!/usr/bin/env python3
"""Check S3 response request IDs against the real listener's HTTP completion log."""

import argparse
from contextlib import closing
from http.client import HTTPConnection
import json
from pathlib import Path
import secrets
import tempfile
import time
from xml.etree import ElementTree

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials

if __package__:
    from .local_rustfs import HOST, local_rustfs
else:
    from local_rustfs import HOST, local_rustfs


BUCKET = "request-id-parity"
REGION = "us-east-1"
BODY = b"request ID parity"


def response_id(status, headers, body):
    request_id = headers.get("x-amz-request-id")
    if not request_id or headers.get("x-request-id") != request_id:
        raise ValueError(f"HTTP {status} has missing or different request ID headers")
    if status == 404:
        error = ElementTree.fromstring(body)
        if error.tag.rsplit("}", 1)[-1] != "Error" or error.findtext("{*}Code") != "NoSuchKey":
            raise ValueError("missing object did not return NoSuchKey")
        if error.findtext("{*}RequestId") != request_id:
            raise ValueError("NoSuchKey XML request ID differs from response header")
    elif body != BODY:
        raise ValueError("GET returned the wrong object body")
    return request_id


def read_object(port, credentials, key, expected_status):
    path = f"/{BUCKET}/{key}"
    request = AWSRequest(method="GET", url=f"http://{HOST}:{port}{path}")
    S3SigV4Auth(credentials, "s3", REGION).add_auth(request)
    with closing(HTTPConnection(HOST, port, timeout=10)) as connection:
        connection.request("GET", path, headers=dict(request.headers))
        response = connection.getresponse()
        if response.status != expected_status:
            raise ValueError(f"GET {path}: expected {expected_status}, observed {response.status}")
        return path, response.status, response_id(response.status, response.headers, response.read())


def verify_completions(responses, events):
    if len({request_id for _, _, request_id in responses}) != len(responses):
        raise ValueError("different GET requests reused a request ID")
    for path, status, request_id in responses:
        matches = [e for e in events if e.get("event") == "http_request_completed"
                   and e.get("method") == "GET" and e.get("uri") == path and e.get("status_code") == status]
        if len(matches) != 1:
            raise ValueError(f"GET {path} HTTP {status}: expected one completion event, found {len(matches)}")
        if matches[0].get("request_id") != request_id:
            raise ValueError(f"GET {path} HTTP {status}: completion request ID differs from response header")


def completion_events(log):
    if not log.exists():
        return []
    with log.open(encoding="utf-8") as lines:
        return [json.loads(line) for line in lines if line.endswith("\n") and "http_request_completed" in line]


def exercise(binary):
    credentials = Credentials("local-request-id", secrets.token_hex(24))
    with tempfile.TemporaryDirectory(prefix="rustfs-request-id-") as directory:
        root = Path(directory)
        log = root / "logs" / "rustfs.log"
        with local_rustfs(binary, credentials, root, {"RUST_LOG": "warn,rustfs::server::http=info"}) as port:
            client = boto3.client(
                "s3", endpoint_url=f"http://{HOST}:{port}", region_name=REGION,
                aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key,
                config=Config(s3={"addressing_style": "path"}, proxies={}),
            )
            with closing(client):
                client.create_bucket(Bucket=BUCKET)
                client.put_object(Bucket=BUCKET, Key="existing", Body=BODY)
            responses = [read_object(port, credentials, "existing", 200), read_object(port, credentials, "missing", 404)]
            # Wait for both events to reach the file before stopping the asynchronous logger.
            paths = {path for path, _, _ in responses}
            deadline = time.monotonic() + 5
            while not paths.issubset(e.get("uri") for e in completion_events(log) if e.get("method") == "GET"):
                if time.monotonic() >= deadline:
                    break
                time.sleep(0.05)
        verify_completions(responses, completion_events(log))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustfs-bin", required=True, type=Path)
    args = parser.parse_args()
    exercise(args.rustfs_bin)
    print(json.dumps({"case_id": "s3.request-id-parity", "verified_statuses": [200, 404]}))


if __name__ == "__main__":
    main()
