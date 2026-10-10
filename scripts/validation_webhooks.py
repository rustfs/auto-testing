#!/usr/bin/env python3
"""Check canonical event and audit webhook evidence without exporting secrets."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import unquote_plus


class IncompleteAuditRecords(ValueError):
    """The requested operations have not both arrived yet."""


def records(path: Path) -> list[dict]:
    values = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"webhook line {number} is not an object")
        values.append(value)
    return values


def event_summary(values: list[dict], bucket: str, key: str) -> dict:
    matches = []
    for value in values:
        for event in value.get("Records", [value]):
            if not isinstance(event, dict):
                raise ValueError("event record is not an object")
            s3 = event.get("s3") or {}
            event_bucket = (s3.get("bucket") or {}).get("name") or event.get("Bucket")
            event_key = (s3.get("object") or {}).get("key") or event.get("Key")
            if event_bucket == bucket and isinstance(event_key, str) and unquote_plus(event_key) == key:
                matches.append(event)
    if len(matches) != 1:
        raise ValueError(f"expected one matching object event, found {len(matches)}")
    event = matches[0]
    name = event.get("eventName") or event.get("EventName")
    if name != "s3:ObjectCreated:Put":
        raise ValueError(f"unexpected event type: {name!r}")
    return {"bucket": bucket, "key": key, "delivery_count": 1, "event_name": name}


def audit_summary(values: list[dict], bucket: str, key: str) -> dict:
    serialized = json.dumps(values, ensure_ascii=False).lower()
    for name in ("RUSTFS_SECRET_KEY", "VALIDATION_AUDIT_TOKEN", "VALIDATION_NOTIFY_TOKEN"):
        secret = os.environ.get(name, "")
        if secret and secret.lower() in serialized:
            raise ValueError(f"audit payload exposes {name}")
    matches: dict[str, dict] = {}
    for value in values:
        api = value.get("api") or {}
        if api.get("bucket") != bucket or api.get("object") != key:
            continue
        api_name = api.get("name")
        name = {"PutObject": "PutObject", "GetObject": "GetObject",
                "s3:PutObject": "PutObject", "s3:GetObject": "GetObject"}.get(api_name)
        if name is None:
            continue
        request_id = value.get("requestID")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError(f"{name} audit record lacks requestID")
        if api.get("status_code") != 200:
            raise ValueError(f"{name} audit record lacks successful status")
        headers = value.get("requestHeader") or {}
        for header, content in headers.items():
            if header.lower() == "authorization" and isinstance(content, str) and content.strip("*"):
                raise ValueError("audit payload exposes authorization header")
        matches[name] = {"api_name": api_name, "request_id": request_id, "status_code": 200}
    if set(matches) != {"PutObject", "GetObject"}:
        raise IncompleteAuditRecords(
            "matching PUT and GET audit records were not both delivered; "
            f"observed operations: {sorted(matches)}"
        )
    if matches["PutObject"]["request_id"] == matches["GetObject"]["request_id"]:
        raise ValueError("PUT and GET audit records share a request ID")
    return {"bucket": bucket, "key": key, "operations": matches, "secret_check": "PASS"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("events", "audit"))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=0,
                        help="Wait this many seconds for both audit operations; invalid records fail immediately.")
    args = parser.parse_args()
    if args.timeout < 0 or args.timeout > 300:
        parser.error("--timeout must be between 0 and 300 seconds")
    deadline = time.monotonic() + args.timeout
    while True:
        values = records(args.input)
        try:
            result = (event_summary if args.mode == "events" else audit_summary)(values, args.bucket, args.key)
            break
        except IncompleteAuditRecords:
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(2, max(0, deadline - time.monotonic())))
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"validation webhook error: {exc}", file=sys.stderr)
        sys.exit(1)
