#!/usr/bin/env python3
"""Retain safe upgrade diagnostics and validate seed/delete evidence."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import xml.etree.ElementTree as ET

SENSITIVE_FIELD = r"(?:authorization|(?:secret|access)[ _-]?(?:access[ _-]?)?key(?:[ _-]?id)?|(?:session[ _-]?|vault[ _-]?|root[ _-]?)?token|password|client[ _-]?secret|master[ _-]?key)"


def redact(text: str, golden: Path | None = None) -> str:
    values = [value for key, value in os.environ.items()
              if re.search(r"SECRET|TOKEN|PASSWORD|MASTER_KEY|ACCESS_KEY|BACKUP_KEK", key) and value]
    if golden and golden.exists():
        for line in golden.read_text().splitlines():
            key, _, value = line.partition("\t")
            if key.startswith("creds:"):
                values.extend(value.split("|"))
    for value in sorted(set(values), key=len, reverse=True):
        if value:
            text = text.replace(value, "[REDACTED]")
    # Node logs arrive as strings, including JSON embedded in journal lines
    # and JSON-escaped quotes. Unknown credentials cannot rely on golden/env.
    return re.sub(r"(?im)^.*\b" + SENSITIVE_FIELD + r"(?:\\?[\"'])?\s*[=:].*$",
                  "[REDACTED SENSITIVE LINE]", text)


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def safe_value(value, golden=None):
    if isinstance(value, dict):
        return {redact(key, golden): ("[REDACTED]" if re.search(r"secret|token|password|authorization|master.?key|access.?key", key, re.I)
                      else safe_value(item, golden)) for key, item in value.items()}
    if isinstance(value, list):
        return [safe_value(item, golden) for item in value]
    return redact(value, golden) if isinstance(value, str) else value


def validate_delete(request: Path, response: Path, debug: Path, exit_code: int, output: Path) -> bool:
    metadata = {"http_status": None, "request_id": None}
    # Debug contains signed requests. Only retain these two response fields.
    if debug.exists():
        with debug.open(errors="replace") as stream:
            for line in stream:
                if match := re.search(r'"POST .*? HTTP/1\.1" (\d{3})\b', line):
                    metadata["http_status"] = int(match[1])
                if "Response headers:" in line:
                    try:
                        headers = ast.literal_eval(line.split("Response headers:", 1)[1].strip())
                        metadata["request_id"] = next((value for key, value in headers.items()
                                                       if key.lower() == "x-amz-request-id"), None)
                        if isinstance(metadata["request_id"], bytes):
                            metadata["request_id"] = metadata["request_id"].decode(errors="replace")
                    except (ValueError, SyntaxError, AttributeError):
                        pass
    receipt = {"schema": 1, "cli_exit": exit_code, **metadata, "valid": False}
    if exit_code and debug.exists():
        # The top-level S3 error is only in debug/stderr when the CLI exits
        # nonzero. Retain its allowlisted XML fields, never signed requests.
        lines = debug.read_text(errors="replace").splitlines()
        for index, line in enumerate(lines):
            if "Response body:" not in line:
                continue
            try:
                raw_body = line.split("Response body:", 1)[1].strip()
                if not raw_body and index + 1 < len(lines):
                    raw_body = lines[index + 1].strip()
                body = ast.literal_eval(raw_body)
                root = ET.fromstring(body)
                if root.tag.rsplit("}", 1)[-1] == "Error":
                    receipt["service_error"] = {child.tag.rsplit("}", 1)[-1]: child.text
                                                for child in root if child.tag.rsplit("}", 1)[-1]
                                                in ("Code", "Message", "RequestId")}
            except (ValueError, SyntaxError, TypeError, ET.ParseError):
                pass
    try:
        payload = json.loads(request.read_text())
        raw_response = response.read_text()
        # AWS CLI emits no stdout for a successful Quiet DeleteResult.
        result = json.loads(raw_response) if raw_response.strip() else {}
        receipt.update(request=payload, response=result)
        keys = [item["Key"] for item in payload["Objects"]]
        if not keys or len(keys) > 1000 or any(not isinstance(key, str) for key in keys):
            raise ValueError("invalid delete request")
        if not isinstance(result, dict) or not isinstance(result.get("Errors", []), list):
            raise ValueError("invalid delete response")
        errors = result.get("Errors", [])
        receipt["error_count"] = len(errors)
        receipt["valid"] = exit_code == 0 and not errors and metadata["http_status"] in (None, 200)
        receipt["reason"] = ("CLI failure" if exit_code else "per-object deletion failure" if errors
                             else "unexpected HTTP status" if not receipt["valid"] else "PASS")
    except (OSError, ValueError, KeyError, TypeError) as error:
        receipt["reason"] = str(error)
    write_json(output, safe_value(receipt))
    if not receipt["valid"]:
        print(f"delete validation failed: {redact(receipt['reason'])}; see {output}")
    return receipt["valid"]


def body_identity(body: Path):
    digest = hashlib.sha256()
    size = 0
    with body.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return {"bytes": size, "sha256": digest.hexdigest()}


def seed_record(manifest: Path, key: str, body: Path):
    rows = json.loads(manifest.read_text()) if manifest.exists() else []
    if any(row["key"] == key for row in rows):
        raise ValueError("duplicate seed key")
    rows.append({"key": key, **body_identity(body)})
    write_json(manifest, rows)


def check_seed(manifest: Path, keys_file: Path):
    rows, keys = json.loads(manifest.read_text()), json.loads(keys_file.read_text())
    expected = [row["key"] for row in rows]
    if not isinstance(keys, list) or len(keys) != len(expected) or set(keys) != set(expected):
        raise ValueError("seed key inventory changed")


def check_body(manifest: Path, key: str, body: Path):
    rows = json.loads(manifest.read_text())
    expected = next((row for row in rows if row["key"] == key), None)
    if expected is None or body_identity(body) != {name: expected[name] for name in ("bytes", "sha256")}:
        raise ValueError("seed content changed")


def check_cleanup(state_file: Path):
    # A missing or interrupted receipt must never authorize fleet release.
    if state_file.read_text().strip() not in ("PASS", "NOT_TOUCHED"):
        raise ValueError("upgrade cleanup incomplete; fleet remains quarantined")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    cleanup = commands.add_parser("check-cleanup")
    cleanup.add_argument("--state-file", type=Path, required=True)
    delete = commands.add_parser("validate-delete")
    for name in ("request", "response", "debug", "output"):
        delete.add_argument("--" + name, type=Path, required=True)
    delete.add_argument("--exit-code", type=int, required=True)
    sanitize = commands.add_parser("sanitize")
    sanitize.add_argument("--input", type=Path, required=True)
    sanitize.add_argument("--output", type=Path, required=True)
    sanitize.add_argument("--golden", type=Path)
    record = commands.add_parser("seed-record")
    record.add_argument("--manifest", type=Path, required=True)
    record.add_argument("--key", required=True)
    record.add_argument("--body", type=Path, required=True)
    inventory = commands.add_parser("check-seed")
    inventory.add_argument("--manifest", type=Path, required=True)
    inventory.add_argument("--keys-file", type=Path, required=True)
    body = commands.add_parser("check-body")
    body.add_argument("--manifest", type=Path, required=True)
    body.add_argument("--key", required=True)
    body.add_argument("--body", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "validate-delete":
            return 0 if validate_delete(args.request, args.response, args.debug, args.exit_code, args.output) else 1
        if args.command == "check-cleanup":
            check_cleanup(args.state_file)
        elif args.command == "sanitize":
            raw = args.input.read_text(errors="replace")
            try:
                raw = json.dumps(safe_value(json.loads(raw), args.golden), ensure_ascii=False, indent=2)
            except ValueError:
                raw = redact(raw, args.golden)
            args.output.write_text(raw)
        elif args.command == "seed-record":
            seed_record(args.manifest, args.key, args.body)
        elif args.command == "check-seed":
            check_seed(args.manifest, args.keys_file)
        elif args.command == "check-body":
            check_body(args.manifest, args.key, args.body)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"upgrade evidence failed: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
