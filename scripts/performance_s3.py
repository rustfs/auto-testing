#!/usr/bin/env python3
"""Own one fresh benchmark bucket and verify bounded S3 cleanup."""

from __future__ import annotations

import argparse
import datetime
import json
import math
import os
from pathlib import Path
import re
import time
import uuid


RETRYABLE = {"ServiceUnavailable", "SlowDown", "InternalError", "RequestTimeout",
             "ReadTimeoutError", "ConnectTimeoutError", "EndpointConnectionError"}


class RequestFailure(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def save(path, document):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n")
    temporary.replace(path)


def new_bucket(prefix):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,61}[a-z0-9]|[a-z0-9]", prefix):
        raise ValueError("invalid bucket prefix")
    if any(part in prefix for part in ("..", ".-", "-.")):
        raise ValueError("invalid bucket prefix")
    return prefix[:30].rstrip(".-") + "-" + uuid.uuid4().hex


class Session:
    def __init__(self, endpoint, events, budget, factory=None):
        self.endpoint = endpoint
        self.events = events
        self.deadline = time.monotonic() + budget
        self.factory = factory or self.client
        self.s3 = None
        self.http_503 = 0
        self.request_failures = 0

    def client(self, remaining):
        import boto3
        from botocore.config import Config
        return boto3.client(
            "s3", endpoint_url=self.endpoint, region_name="us-east-1",
            aws_access_key_id=os.environ["WARP_ACCESS_KEY"],
            aws_secret_access_key=os.environ["WARP_SECRET_KEY"],
            aws_session_token=os.environ.get("WARP_SESSION_TOKEN") or None,
            config=Config(s3={"addressing_style": "path"}, proxies={},
                          connect_timeout=min(3, remaining), read_timeout=min(60, remaining),
                          retries={"total_max_attempts": 1}),
        )

    def emit(self, **record):
        record["time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self.events.open("a") as stream:
            stream.write(json.dumps(record) + "\n")

    def call(self, operation, **arguments):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RequestFailure("CleanupDeadlineExceeded")
        try:
            # Rebuild near the deadline so a call cannot retain a longer old timeout.
            if self.s3 is None or remaining < 60:
                self.s3 = self.factory(max(0.01, remaining))
            response = getattr(self.s3, operation)(**arguments)
        except Exception as exc:
            response = getattr(exc, "response", {})
            if not isinstance(response, dict):
                response = {}
            code = response.get("Error", {}).get("Code", type(exc).__name__)
            self.request_failures += 1
            self.record_response(operation, response, code)
            raise RequestFailure(code) from exc
        if not isinstance(response, dict):
            raise RequestFailure("InvalidS3Response")
        self.record_response(operation, response)
        return response

    def record_response(self, operation, response, code=None):
        metadata = response.get("ResponseMetadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        headers = metadata.get("HTTPHeaders", {})
        if not isinstance(headers, dict):
            headers = {}
        status = metadata.get("HTTPStatusCode")
        self.http_503 += int(status == 503)
        self.emit(kind="s3_request", operation=operation, endpoint=self.endpoint,
                  http_status=status, request_id=metadata.get("RequestId"),
                  sdk_retries=metadata.get("RetryAttempts"), error_code=code,
                  error_cause=headers.get("x-rustfs-error-cause"),
                  retry_after=headers.get("retry-after"))


def prepare(session, state, prefix):
    bucket = new_bucket(prefix)
    document = {"version": 1, "bucket": bucket, "endpoint": session.endpoint,
                "created": False, "prepare": "FAIL", "cleanup": "NOT_RUN"}
    save(state, document)
    session.call("create_bucket", Bucket=bucket)
    document.update(created=True, prepare="PASS")
    save(state, document)
    return document


def page_keys(response):
    contents = response.get("Contents", [])
    if not isinstance(contents, list) or any(not isinstance(row, dict) or not isinstance(row.get("Key"), str) for row in contents):
        raise RequestFailure("InvalidListResponse")
    keys = [row["Key"] for row in contents]
    if len(keys) != len(set(keys)) or (response.get("IsTruncated") and not keys):
        raise RequestFailure("InvalidListResponse")
    return keys


def delete_batch(session, bucket, keys, attempts):
    if (not 1 <= len(keys) <= 1000 or any(not isinstance(key, str) for key in keys)
            or len(set(keys)) != len(keys) or attempts < 1):
        raise ValueError("delete batch requires 1..1000 distinct keys and positive attempts")
    pending = list(keys)
    error_entries = 0
    for attempt in range(1, attempts + 1):
        origin = "per_object_response"
        try:
            response = session.call("delete_objects", Bucket=bucket,
                                    Delete={"Objects": [{"Key": key} for key in pending], "Quiet": False})
            deleted_rows, error_rows = response.get("Deleted", []), response.get("Errors", [])
            if (not isinstance(deleted_rows, list) or not isinstance(error_rows, list)
                    or any(not isinstance(row, dict) or not isinstance(row.get("Key"), str)
                           for row in deleted_rows + error_rows)
                    or any(not isinstance(row.get("Code"), str) for row in error_rows)):
                raise RequestFailure("InvalidDeleteResponse")
            deleted = [row["Key"] for row in deleted_rows]
            errors = [{"Key": row["Key"], "Code": row["Code"]} for row in error_rows]
            answered = deleted + [row["Key"] for row in errors]
            if set(answered) != set(pending) or len(answered) != len(pending):
                raise RequestFailure("IncompleteDeleteResponse")
        except RequestFailure as exc:
            origin = "request_or_protocol_failure"
            errors = [{"Key": key, "Code": exc.code} for key in pending]
        error_entries += len(errors)
        session.emit(kind="delete_batch", bucket=bucket, attempt=attempt,
                     requested=len(pending), error_entries=len(errors), errors=errors,
                     error_origin=origin)
        if not errors:
            return [], error_entries
        if any(row["Code"] not in RETRYABLE for row in errors):
            return [row["Key"] for row in errors], error_entries
        pending = [row["Key"] for row in errors]
        if attempt < attempts:
            time.sleep(min(0.25 * attempt, max(0, session.deadline - time.monotonic())))
    return pending, error_entries


def abort_uploads(session, bucket):
    # An interrupted PUT may leave no listed object but still leave an upload.
    while True:
        response = session.call("list_multipart_uploads", Bucket=bucket, MaxUploads=1000)
        uploads = response.get("Uploads", [])
        if (not isinstance(uploads, list)
                or any(not isinstance(row, dict) or not isinstance(row.get("Key"), str)
                       or not isinstance(row.get("UploadId"), str) for row in uploads)):
            raise RequestFailure("InvalidUploadListResponse")
        if not uploads:
            if response.get("IsTruncated"):
                raise RequestFailure("InvalidUploadListResponse")
            return
        for upload in uploads:
            session.call("abort_multipart_upload", Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"])


def remaining_objects(session, bucket):
    count, sample, token, seen = 0, [], None, set()
    while True:
        arguments = {"Bucket": bucket, "MaxKeys": 1000}
        if token:
            arguments["ContinuationToken"] = token
        response = session.call("list_objects_v2", **arguments)
        keys = page_keys(response)
        count += len(keys)
        sample.extend(keys[:max(0, 20 - len(sample))])
        if not response.get("IsTruncated"):
            return count, sample
        token = response.get("NextContinuationToken")
        if not isinstance(token, str) or not token or token in seen:
            raise RequestFailure("InvalidListContinuation")
        seen.add(token)


def read_state(state):
    document = json.loads(state.read_text())
    if (not isinstance(document, dict) or document.get("version") != 1
            or not isinstance(document.get("endpoint"), str)
            or not isinstance(document.get("bucket"), str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9.-]*-[a-f0-9]{32}", document["bucket"])
            or len(document["bucket"]) > 63
            or (document.get("prepare") == "PASS" and document.get("created") is not True)):
        raise ValueError("invalid benchmark bucket receipt")
    return document


def cleanup(session, state, attempts=3):
    document = read_state(state)
    bucket = document["bucket"]
    if document.get("created") is not True or document.get("endpoint") != session.endpoint:
        raise ValueError("cleanup requires a receipt for a freshly created benchmark bucket")
    if document.get("cleanup") == "PASS":
        return document
    previous_http_503 = document.get("cleanup_http_503_attempts", 0)
    previous_failures = document.get("cleanup_request_failures", 0)
    document.update(cleanup="FAIL", remaining_objects=None,
                    cleanup_error_entries=document.get("cleanup_error_entries", 0))
    save(state, document)
    started = time.monotonic()
    try:
        while True:
            response = session.call("list_objects_v2", Bucket=bucket, MaxKeys=1000)
            keys = page_keys(response)
            if not keys:
                break
            pending, errors = delete_batch(session, bucket, keys, attempts)
            document["cleanup_error_entries"] += errors
            document["cleanup_http_503_attempts"] = previous_http_503 + session.http_503
            document["cleanup_request_failures"] = previous_failures + session.request_failures
            # Checkpoint errors before any later list or deadline can fail.
            save(state, document)
            if pending:
                break
        count, sample = remaining_objects(session, bucket)
        document.update(remaining_objects=count, remaining_sample=sample)
        if count == 0:
            abort_uploads(session, bucket)
            session.call("delete_bucket", Bucket=bucket)
            document["cleanup"] = "PASS"
    except RequestFailure as exc:
        document["cleanup_error_code"] = exc.code
    finally:
        document["cleanup_http_503_attempts"] = previous_http_503 + session.http_503
        document["cleanup_request_failures"] = previous_failures + session.request_failures
        document["cleanup_seconds"] = round(time.monotonic() - started, 3)
        save(state, document)
    return document


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "cleanup", "bucket", "fields"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--events", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--prefix", default="warp-benchmark-bucket")
    parser.add_argument("--budget", type=float, default=180)
    parser.add_argument("--attempts", type=int, default=3)
    args = parser.parse_args()
    if args.action in {"bucket", "fields"}:
        document = read_state(args.state)
        if args.action == "bucket":
            print(document["bucket"])
        else:
            print("\t".join(str(document.get(key, "unknown")) for key in
                            ("bucket", "prepare", "cleanup", "cleanup_error_entries",
                             "remaining_objects", "cleanup_http_503_attempts",
                             "cleanup_request_failures")))
        return 0
    if not math.isfinite(args.budget) or args.budget <= 0 or args.attempts < 1 or not args.endpoint or not args.events:
        parser.error("positive budget/attempts, endpoint and events are required")
    os.umask(0o077)
    session = Session(args.endpoint, args.events, args.budget)
    try:
        document = (prepare(session, args.state, args.prefix) if args.action == "prepare"
                    else cleanup(session, args.state, args.attempts))
    except (RequestFailure, ValueError, OSError) as exc:
        print(f"S3 {args.action} failed: {getattr(exc, 'code', type(exc).__name__)}", flush=True)
        return 1
    print(f"S3 {args.action}: {document[args.action]}; bucket={document['bucket']}", flush=True)
    return int(document[args.action] != "PASS")


if __name__ == "__main__":
    raise SystemExit(main())
