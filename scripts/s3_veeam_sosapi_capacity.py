#!/usr/bin/env python3
"""Verify Veeam SOSAPI capacity before and after scanner certification."""

import argparse
from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import secrets
import tempfile
import time
from xml.etree import ElementTree

import boto3
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

if __package__:
    from .local_rustfs import HOST, local_rustfs
else:
    from local_rustfs import HOST, local_rustfs


CASE_ID = "s3.veeam-sosapi-capacity"
BUCKET = "veeam-sosapi-capacity"
CAPACITY_KEY = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml"
PAYLOAD = b"capacity certification fixture"


@contextmanager
def scanner_client(binary, credentials, root, *, enabled):
    settings = {
        "RUSTFS_VEEAM_SOSAPI_ENABLE": "true",
        "RUSTFS_SCANNER_ENABLED": "true" if enabled else "false",
        "RUSTFS_SCANNER_CYCLE": "1",
        "RUSTFS_SCANNER_START_DELAY_SECS": "0",
    }
    with local_rustfs(binary, credentials, root, settings) as port:
        config = Config(signature_version="s3v4", s3={"addressing_style": "path"}, proxies={},
                        retries={"total_max_attempts": 1}, connect_timeout=3, read_timeout=10,
                        request_checksum_calculation="when_required")
        with closing(boto3.client("s3", endpoint_url=f"http://{HOST}:{port}", region_name="us-east-1",
                                  aws_access_key_id=credentials.access_key,
                                  aws_secret_access_key=credentials.secret_key, config=config)) as client:
            yield client


def read_capacity(client):
    try:
        response = client.get_object(Bucket=BUCKET, Key=CAPACITY_KEY)
    except ClientError as error:
        status = error.response["ResponseMetadata"]["HTTPStatusCode"]
        raise RuntimeError(f"capacity.xml expected HTTP 200, got HTTP {status}") from None
    with closing(response["Body"]) as stream:
        body = stream.read(4097)
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200 or response.get("ContentType") != "application/xml":
        raise RuntimeError("capacity.xml did not return HTTP 200 application/xml")
    if len(body) > 4096:
        raise RuntimeError("capacity.xml exceeded 4096 bytes")
    try:
        root = ElementTree.fromstring(body)
        numbers = {field: int(root.findtext(field, "")) for field in ("Capacity", "Available", "Used")}
    except (ElementTree.ParseError, ValueError):
        raise RuntimeError("capacity.xml did not contain three integer capacity fields") from None
    if root.tag != "CapacityInfo":
        raise RuntimeError("capacity.xml did not contain a CapacityInfo document")
    capacity, available, used = (numbers[field] for field in ("Capacity", "Available", "Used"))
    if capacity <= 0 or not -1 <= used <= capacity or not 0 <= available <= capacity - max(used, 0):
        raise RuntimeError(f"capacity.xml reported invalid capacity figures for a bucket without a quota: {numbers}")
    etag = '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'
    if response.get("ETag") != etag or response.get("LastModified") is None:
        raise RuntimeError("capacity.xml omitted Last-Modified or its ETag did not match the body")
    return numbers, etag


def exercise(binary, root, credentials):
    with scanner_client(binary, credentials, root, enabled=False) as client:
        client.create_bucket(Bucket=BUCKET)
        client.put_object(Bucket=BUCKET, Key="sample", Body=PAYLOAD)
        pending, pending_etag = read_capacity(client)
        if pending["Used"] != -1:
            raise RuntimeError(f"scanner disabled: expected Used=-1, got {pending['Used']}")

    with scanner_client(binary, credentials, root, enabled=True) as client:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            certified, certified_etag = read_capacity(client)
            if certified["Used"] != -1:
                break
            time.sleep(1)
        else:
            raise RuntimeError("scanner did not certify bucket usage within 120 seconds")
        if certified["Used"] != len(PAYLOAD) or certified_etag == pending_etag:
            raise RuntimeError("certification did not publish the exact object size and a new ETag")

    return {"case_id": CASE_ID, "pending": pending, "certified": certified,
            "pending_etag": pending_etag, "certified_etag": certified_etag}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustfs-bin", type=Path, required=True)
    args = parser.parse_args()
    credentials = Credentials("local-sosapi-test", secrets.token_hex(24))
    with tempfile.TemporaryDirectory(prefix="rustfs-sosapi-") as temporary:
        print(json.dumps(exercise(args.rustfs_bin, Path(temporary), credentials)))


if __name__ == "__main__":
    main()
