#!/usr/bin/env python3
"""Keep response diagnostics from AWS CLI debug output without saving secrets."""

import ast
import json
import os
import re
import sys


HTTP_STATUS = re.compile(r'"[^"\r\n]+ HTTP/[0-9.]+" ([1-5][0-9]{2})\b')
SAFE_HEADERS = {
    "x-rustfs-error-cause": re.compile(r"[a-z][a-z0-9_]{0,99}"),
    "x-amz-request-id": re.compile(r"[A-Za-z0-9-]{1,200}"),
    "retry-after": re.compile(r"[0-9]{1,10}"),
}
ERROR_PREFIXES = (
    "An error occurred (", "Unable to locate credentials", "Unknown options:",
    "Parameter validation failed:", "Could not connect to the endpoint URL:",
    "Connect timeout on endpoint URL:", "Read timeout on endpoint URL:",
)


def capture(stream, errors, responses):
    secrets = [os.environ.get(name) for name in (
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
        "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY",
    ) if os.environ.get(name)]
    http_status = None
    for line in stream:
        if " - DEBUG - " in line:
            match = HTTP_STATUS.search(line)
            if match:
                http_status = int(match[1])
            marker = " - DEBUG - Response headers: "
            if marker in line:
                record = {"http_status": http_status, "headers": {}}
                raw = line.split(marker, 1)[1].strip()
                try:
                    headers = ast.literal_eval(raw) if len(raw) <= 65536 else None
                    if not isinstance(headers, dict):
                        raise ValueError("invalid headers")
                    for name, value in headers.items():
                        if not isinstance(name, str) or not isinstance(value, str):
                            continue
                        name = name.lower()
                        if name in SAFE_HEADERS and SAFE_HEADERS[name].fullmatch(value):
                            record["headers"][name] = value
                except (ValueError, SyntaxError, RecursionError):
                    record["headers_parse_error"] = True
                responses.write(json.dumps(record) + "\n")
                http_status = None
            continue
        # Debug continuation lines include canonical requests, signatures and
        # response bodies. Only the CLI's final diagnostic is public evidence.
        diagnostic = line.strip().removeprefix("aws: [ERROR]: ")
        if diagnostic.startswith(ERROR_PREFIXES):
            for secret in secrets:
                diagnostic = diagnostic.replace(secret, "[REDACTED]")
            errors.write(diagnostic + "\n")


if __name__ == "__main__":
    with open(sys.argv[1], "w", encoding="utf-8") as errors, open(
        sys.argv[2], "w", encoding="utf-8"
    ) as responses:
        capture(sys.stdin, errors, responses)
