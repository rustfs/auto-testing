#!/usr/bin/env python3
"""Run destructive-in-a-test-bucket RustFS ILM behavior checks."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import quote, unquote, urlparse

from ilm_timing import (
    BoundedS3Client,
    CycleProgress,
    DueWindow,
    ServerClock,
    expiry_window,
    parse_expiry_date,
    time_budget,
)

from case_plan import (
    ILM_CASE_IDS as CASE_IDS,
    ilm_harness_scored_cases,
)
from rc_binary import resolve_rc_binary

try:
    import boto3
    import botocore
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.config import Config
    from botocore.credentials import Credentials
    from botocore.exceptions import (
        ClientError,
        ConnectTimeoutError,
        EndpointConnectionError,
        ReadTimeoutError,
    )
except ImportError as exc:  # pragma: no cover - dependency guidance
    raise SystemExit(
        "boto3 is required for ILM validation"
    ) from exc


INFRA_EXCEPTIONS = (ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError)
INFRA_ERROR_CODES = {
    "AccessDenied",
    "InvalidAccessKeyId",
    "InvalidToken",
    "RequestTimeout",
    "ServiceUnavailable",
    "SignatureDoesNotMatch",
    "SlowDown",
}

S3_DELETE_RETRYABLE_ERROR_CODES = {
    "InternalError",
    "RequestTimeout",
    "ServiceUnavailable",
    "SlowDown",
}

def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, bytes):
        return {
            "bytes": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    return str(value)


def safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "-." else "-" for char in value)


@dataclass
class CaseResult:
    id: str
    state: str
    message: str
    started_at: str
    finished_at: str
    duration_seconds: float
    evidence: list[str] = field(default_factory=list)


class CaseExecutionAndCleanupError(Exception):
    def __init__(
        self,
        primary_phase: str,
        primary_error: Exception,
        cleanup_phase: str,
        cleanup_error: Exception,
    ) -> None:
        self.primary_phase = primary_phase
        self.primary_error = primary_error
        self.cleanup_phase = cleanup_phase
        self.cleanup_error = cleanup_error
        super().__init__(
            f"{primary_phase} failed: {type(primary_error).__name__}: "
            f"{primary_error}; cleanup {cleanup_phase} failed: "
            f"{type(cleanup_error).__name__}: {cleanup_error}"
        )


class EventRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event: str, **details: Any) -> None:
        item = {"timestamp": utc_now(), "monotonic": time.monotonic(), "event": event, "details": details}
        with self.lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(item, default=json_default, sort_keys=True) + "\n"
                )


class IlmHarness:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.evidence_dir = args.evidence_dir.resolve()
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self.case_dir = self.evidence_dir / "cases"
        self.case_dir.mkdir(parents=True, exist_ok=True)
        self.events = EventRecorder(self.evidence_dir / "events.jsonl")
        self.results: list[CaseResult] = []
        self.created_buckets: set[str] = set()
        self.uncertain_buckets: set[str] = set()
        self.state_lock = threading.Lock()
        self.rc_home = Path(tempfile.mkdtemp(prefix="rustfs-ilm-rc-"))
        alias_base = f"ilm-{safe_name(args.run_id)[-16:]}"
        self.rc_aliases = tuple(
            (f"{alias_base}-{index}", endpoint)
            for index, endpoint in enumerate(args.scanner_endpoint)
        )
        self.rc_alias = self.rc_aliases[0][0]
        self.rc_env = os.environ.copy()
        self.rc_env["RC_CONFIG_DIR"] = str(self.rc_home)
        self.rc_bin = args.rc_bin
        self.rc_version = "unavailable"
        self.rc_binary_sha256 = "unavailable"
        self.expected_rc_binary_sha256 = os.environ.get(
            "RUSTFS_EXPECTED_RC_SHA256", ""
        ).lower()
        self.clock: ServerClock | None = None
        self.readiness: dict[str, Any] = {"state": "not_started"}
        self.blocked_reason: str | None = None
        self.case_buckets: set[str] = set()
        self.client = boto3.client(
            "s3",
            endpoint_url=args.endpoint,
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key,
            region_name=args.region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"total_max_attempts": 1, "mode": "standard"},
                connect_timeout=5,
                read_timeout=args.request_timeout,
            ),
        )
        self.client = BoundedS3Client(self.client, args.request_timeout)
        self.node_clients = {args.endpoint: self.client}
        for endpoint in args.scanner_endpoint:
            if endpoint not in self.node_clients:
                self.node_clients[endpoint] = BoundedS3Client(boto3.client(
                    "s3", endpoint_url=endpoint, region_name=args.region,
                    aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key,
                    config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                  retries={"total_max_attempts": 1, "mode": "standard"},
                                  connect_timeout=5, read_timeout=args.request_timeout),
                ), args.request_timeout)
        self.tier_client = None
        if args.tier_endpoint and args.tier_access_key and args.tier_secret_key:
            self.tier_client = boto3.client(
                "s3",
                endpoint_url=args.tier_endpoint,
                aws_access_key_id=args.tier_access_key,
                aws_secret_access_key=args.tier_secret_key,
                region_name=args.region,
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    retries={"total_max_attempts": 1, "mode": "standard"},
                    connect_timeout=5,
                    read_timeout=args.request_timeout,
                ),
            )
            self.tier_client = BoundedS3Client(self.tier_client, args.request_timeout)

    def close(self) -> None:
        for client in self.node_clients.values():
            client.close()
        if self.tier_client is not None:
            self.tier_client.close()
        shutil.rmtree(self.rc_home, ignore_errors=True)

    def evidence_path(self, case_id: str, name: str) -> Path:
        directory = self.case_dir / safe_name(case_id)
        directory.mkdir(parents=True, exist_ok=True)
        return directory / name

    def save_json(self, case_id: str, name: str, value: Any) -> str:
        path = self.evidence_path(case_id, name)
        path.write_text(
            json.dumps(value, default=json_default, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return str(path.relative_to(self.evidence_dir))

    def save_text(self, case_id: str, name: str, value: str) -> str:
        path = self.evidence_path(case_id, name)
        path.write_text(value, encoding="utf-8")
        return str(path.relative_to(self.evidence_dir))

    def redact(self, value: str) -> str:
        redacted = value
        for name in (
            "access_key",
            "secret_key",
            "tier_access_key",
            "tier_secret_key",
        ):
            secret = getattr(self.args, name, None)
            if isinstance(secret, str) and secret:
                redacted = redacted.replace(secret, "[REDACTED]")
        return redacted

    def save_phase_error(
        self,
        case_id: str,
        name: str,
        phase: str,
        exc: Exception,
    ) -> str:
        state = self.classify_exception(exc)
        message = self.redact(f"{type(exc).__name__}: {exc}")
        self.events.record(
            name.removesuffix(".json").replace("-", "."),
            case=case_id,
            phase=phase,
            state=state,
            errorType=type(exc).__name__,
            message=message,
        )
        return self.save_json(
            case_id,
            name,
            {
                "phase": phase,
                "state": state,
                "errorType": type(exc).__name__,
                "message": message,
            },
        )

    def bucket_name(self, case_id: str) -> str:
        digest = hashlib.sha256(
            f"{self.args.run_id}:{case_id}".encode("utf-8")
        ).hexdigest()[:12]
        prefix = safe_name(self.args.bucket_prefix.lower()).strip("-")[:45]
        return f"{prefix}-{digest}"

    def create_bucket(self, bucket: str) -> None:
        self.events.record("bucket.create.start", bucket=bucket)
        try:
            self.client.create_bucket(Bucket=bucket)
        except Exception:
            # A lost response can leave a remote creation in progress. Do not
            # claim clean isolation or continue to another fixture in this run.
            self.uncertain_buckets.add(bucket)
            self.blocked_reason = f"bucket creation was not confirmed for {bucket}; isolation requires inspection"
            raise
        with self.state_lock:
            self.created_buckets.add(bucket)
            self.case_buckets.add(bucket)
        self.events.record("bucket.create.complete", bucket=bucket)

    def put_lifecycle(
        self, case_id: str, bucket: str, configuration: dict[str, Any]
    ) -> list[str]:
        evidence = [
            self.save_json(case_id, "lifecycle-request.json", configuration)
        ]
        response = self.client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=configuration,
        )
        evidence.append(self.save_json(case_id, "lifecycle-put-response.json", response))
        self.events.record(
            "lifecycle.put",
            bucket=bucket,
            rule_ids=[rule.get("ID") for rule in configuration["Rules"]],
        )
        return evidence

    def get_lifecycle(self, bucket: str) -> dict[str, Any]:
        return self.client.get_bucket_lifecycle_configuration(Bucket=bucket)

    def raw_s3_request(
        self,
        method: str,
        bucket: str,
        *,
        query: str = "",
        body: bytes | None = None,
        content_type: str | None = None,
        allow_error: bool = False,
    ) -> tuple[int, dict[str, str], bytes]:
        url = f"{self.args.endpoint.rstrip('/')}/{quote(bucket, safe='')}"
        if query:
            url = f"{url}?{query}"
        headers: dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = content_type
        payload = body if body is not None else b""
        headers["x-amz-content-sha256"] = hashlib.sha256(payload).hexdigest()
        if body is not None:
            digest = hashlib.md5(body, usedforsecurity=False).digest()
            headers["Content-MD5"] = base64.b64encode(digest).decode("ascii")
        signed = AWSRequest(method=method, url=url, data=body, headers=headers)
        SigV4Auth(
            Credentials(self.args.access_key, self.args.secret_key),
            "s3",
            self.args.region,
        ).add_auth(signed)
        outgoing = urllib_request.Request(
            url,
            data=body,
            headers=dict(signed.headers.items()),
            method=method,
        )
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        with time_budget(self.args.request_timeout, "raw S3 request", TimeoutError):
            try:
                with opener.open(outgoing, timeout=self.args.request_timeout) as response:
                    return response.status, dict(response.headers.items()), response.read()
            except urllib_error.HTTPError as exc:
                with exc:
                    response_body = exc.read()
                if allow_error:
                    return exc.code, dict(exc.headers.items()), response_body
                raise AssertionError(
                    f"raw S3 {method} request failed with HTTP {exc.code}: "
                    f"{response_body.decode('utf-8', errors='replace')[:500]}"
                ) from exc

    def configure_rc(self) -> None:
        requested_rc_bin = self.rc_bin
        try:
            self.rc_bin, self.rc_binary_sha256 = resolve_rc_binary(requested_rc_bin)
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"required rc binary not found: {requested_rc_bin}"
            ) from exc
        version = subprocess.run(
            [self.rc_bin, "--version"],
            env=self.rc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=getattr(self.args, "request_timeout", 10),
        )
        if version.returncode != 0:
            raise RuntimeError(
                f"rc --version failed with exit {version.returncode}: "
                f"{version.stderr.strip()}"
            )
        self.rc_version = (version.stdout or version.stderr).strip()
        if (
            self.expected_rc_binary_sha256
            and self.rc_binary_sha256.lower() != self.expected_rc_binary_sha256
        ):
            raise RuntimeError(
                "resolved rc binary SHA-256 differs from RUSTFS_EXPECTED_RC_SHA256"
            )
        self.events.record(
            "rc.client.identified",
            binaryPath=self.rc_bin,
            version=self.rc_version,
            binarySha256=self.rc_binary_sha256,
        )
        for alias, endpoint in self.rc_aliases:
            command = [
                self.rc_bin,
                "--json",
                "--no-color",
                "alias",
                "set",
                alias,
                endpoint,
                self.args.access_key,
                self.args.secret_key,
                "--bucket-lookup",
                "path",
            ]
            deadline = time.monotonic() + min(self.args.timeout, 60)
            attempts = 0
            while True:
                attempts += 1
                result = subprocess.run(
                    command,
                    env=self.rc_env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                    timeout=getattr(self.args, "request_timeout", 10),
                )
                if result.returncode == 0:
                    break
                error = result.stderr.strip()
                try:
                    error_document = json.loads(error)
                except json.JSONDecodeError:
                    error_document = {}
                retryable = (
                    isinstance(error_document, dict)
                    and isinstance(error_document.get("details"), dict)
                    and error_document["details"].get("retryable") is True
                )
                if not retryable or time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"rc alias setup failed for {endpoint}: {error}"
                    )
                self.events.record(
                    "rc.alias.retry",
                    alias=alias,
                    endpoint=endpoint,
                    attempt=attempts,
                    error=error[:500],
                )
                time.sleep(self.args.poll_interval)
            self.events.record(
                "rc.alias.configured",
                alias=alias,
                endpoint=endpoint,
                attempts=attempts,
            )

    def wait_for_s3_connectivity(self) -> None:
        deadline = time.monotonic() + min(self.args.timeout, 60)
        attempts = 0
        while True:
            attempts += 1
            try:
                self.client.list_buckets()
                self.events.record(
                    "s3.connectivity.ready",
                    endpoint=self.args.endpoint,
                    attempts=attempts,
                )
                return
            except (*INFRA_EXCEPTIONS, ClientError) as exc:
                retryable = isinstance(exc, INFRA_EXCEPTIONS)
                if isinstance(exc, ClientError):
                    response = exc.response
                    status = response.get("ResponseMetadata", {}).get(
                        "HTTPStatusCode", 0
                    )
                    code = response.get("Error", {}).get("Code", "")
                    retryable = status >= 500 or code in {
                        "RequestTimeout",
                        "ServiceUnavailable",
                        "SlowDown",
                    }
                if not retryable or time.monotonic() >= deadline:
                    raise
                self.events.record(
                    "s3.connectivity.retry",
                    endpoint=self.args.endpoint,
                    attempt=attempts,
                    error=f"{type(exc).__name__}: {exc}"[:500],
                )
                time.sleep(self.args.poll_interval)

    def rc_lifecycle_list(self, case_id: str, bucket: str) -> str:
        command = [
            self.rc_bin,
            "--json",
            "--no-color",
            "bucket",
            "lifecycle",
            "rule",
            "list",
            "--force",
            f"{self.rc_alias}/{bucket}",
        ]
        result = subprocess.run(
            command,
            env=self.rc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=getattr(self.args, "request_timeout", 10),
        )
        output = result.stdout
        path = self.evidence_path(case_id, "rc-lifecycle-list.json")
        path.write_text(output, encoding="utf-8")
        if result.returncode != 0:
            raise AssertionError(
                f"rc lifecycle list failed with exit {result.returncode}: "
                f"{result.stderr.strip()}"
            )
        self.events.record("rc.lifecycle.list", bucket=bucket)
        return output

    def rc_command(
        self,
        case_id: str,
        name: str,
        arguments: list[str],
        *,
        expect_success: bool = True,
        secrets_to_redact: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[str]:
        command = [self.rc_bin, "--json", "--no-color", *arguments]
        result = subprocess.run(
            command,
            env=self.rc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=getattr(self.args, "request_timeout", 10),
        )
        sanitized = [
            "[REDACTED]" if token in secrets_to_redact else token for token in command
        ]
        output = {
            "command": sanitized,
            "exitCode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        text = json.dumps(output, indent=2, sort_keys=True) + "\n"
        for secret in secrets_to_redact:
            text = text.replace(secret, "[REDACTED]")
        self.evidence_path(case_id, f"{name}.json").write_text(
            text, encoding="utf-8"
        )
        if expect_success and result.returncode != 0:
            error = result.stderr
            for secret in secrets_to_redact:
                error = error.replace(secret, "[REDACTED]")
            raise AssertionError(
                f"rc {name} failed with exit {result.returncode}: {error.strip()}"
            )
        if not expect_success and result.returncode == 0:
            raise AssertionError(f"rc {name} unexpectedly succeeded")
        return result

    @staticmethod
    def rc_error_document(stderr: str) -> dict[str, Any]:
        try:
            parsed = json.loads(stderr.strip())
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @classmethod
    def rc_error_mentions(cls, stderr: str, code: str) -> bool:
        if code in stderr:
            return True
        document = cls.rc_error_document(stderr)
        if document.get("error") == code:
            return True
        details = document.get("details")
        if isinstance(details, dict):
            return any(
                isinstance(value, str) and code in value
                for value in details.values()
            )
        return False

    def rc_tier_remove_eventually(
        self,
        case_id: str,
        tier_name: str,
        *,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            self.rc_bin,
            "--json",
            "--no-color",
            "ilm",
            "tier",
            "remove",
            tier_name,
            self.rc_alias,
            "--force",
        ]
        deadline = time.monotonic() + (self.args.timeout if timeout is None else timeout)
        attempts = 0
        attempt_summaries: list[dict[str, Any]] = []
        while True:
            attempts += 1
            remaining = deadline - time.monotonic()
            with time_budget(remaining, "rc tier removal"):
                result = subprocess.run(
                    command,
                    env=self.rc_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                    timeout=min(getattr(self.args, "request_timeout", 10), remaining),
                )
            attempt_name = f"tier-remove-attempt-{attempts}.json"
            attempt_summary = {
                "attempt": attempts,
                "exitCode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
            attempt_summaries.append(attempt_summary)
            self.evidence_path(case_id, attempt_name).write_text(
                json.dumps(
                    {"command": command, **attempt_summary},
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            self.evidence_path(case_id, "tier-remove.json").write_text(
                json.dumps(
                    {
                        "command": command,
                        "attempts": attempt_summaries,
                        "exitCode": result.returncode,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            if time.monotonic() >= deadline:
                raise AssertionError("rc tier removal completed after its deadline")
            if result.returncode == 0:
                self.events.record(
                    "rc.tier_remove.complete",
                    case=case_id,
                    tier=tier_name,
                    attempts=attempts,
                )
                return result

            retryable = self.rc_error_mentions(
                result.stderr,
                "XMinioAdminTierBackendInUse",
            )
            if not retryable or time.monotonic() >= deadline:
                error = result.stderr.strip()
                self.events.record(
                    "rc.tier_remove.failed",
                    case=case_id,
                    tier=tier_name,
                    attempts=attempts,
                    retryable=retryable,
                    error=error[:500],
                )
                raise AssertionError(
                    f"rc tier-remove failed with exit {result.returncode}: {error}"
                )

            self.events.record(
                "rc.tier_remove.retry",
                case=case_id,
                tier=tier_name,
                attempt=attempts,
                error=result.stderr.strip()[:500],
            )
            time.sleep(min(self.args.poll_interval, max(0, deadline - time.monotonic())))

    @staticmethod
    def client_error_code(exc: ClientError) -> str:
        response = getattr(exc, "response", {})
        if not isinstance(response, dict):
            return ""
        error = response.get("Error", {})
        if not isinstance(error, dict):
            return ""
        code = error.get("Code")
        return code if isinstance(code, str) else ""

    def delete_object_eventually(
        self,
        case_id: str,
        bucket: str,
        key: str,
        *,
        evidence_name: str,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + (self.args.timeout if timeout is None else timeout)
        attempts = 0
        attempt_summaries: list[dict[str, Any]] = []
        while True:
            attempts += 1
            try:
                with time_budget(deadline - time.monotonic(), "S3 object deletion"):
                    response = self.client.delete_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                code = self.client_error_code(exc)
                retryable = code in S3_DELETE_RETRYABLE_ERROR_CODES
                attempt_summary = {
                    "attempt": attempts,
                    "state": "failed",
                    "errorCode": code,
                    "errorMessage": str(exc),
                    "retryable": retryable,
                }
                attempt_summaries.append(attempt_summary)
                self.save_json(
                    case_id,
                    f"{evidence_name}-attempt-{attempts}.json",
                    attempt_summary,
                )
                self.save_json(
                    case_id,
                    f"{evidence_name}.json",
                    {
                        "bucket": bucket,
                        "key": key,
                        "attempts": attempt_summaries,
                        "state": "failed",
                    },
                )
                if not retryable or time.monotonic() >= deadline:
                    self.events.record(
                        "s3.delete_object.failed",
                        case=case_id,
                        bucket=bucket,
                        key=key,
                        attempts=attempts,
                        errorCode=code,
                        retryable=retryable,
                    )
                    raise
                self.events.record(
                    "s3.delete_object.retry",
                    case=case_id,
                    bucket=bucket,
                    key=key,
                    attempt=attempts,
                    errorCode=code,
                )
                time.sleep(min(self.args.poll_interval, max(0, deadline - time.monotonic())))
                continue

            attempt_summary = {
                "attempt": attempts,
                "state": "success",
                "response": response,
            }
            attempt_summaries.append(attempt_summary)
            self.save_json(
                case_id,
                f"{evidence_name}-attempt-{attempts}.json",
                attempt_summary,
            )
            summary = {
                "bucket": bucket,
                "key": key,
                "attempts": attempt_summaries,
                "state": "success",
                "response": response,
            }
            self.save_json(case_id, f"{evidence_name}.json", summary)
            if time.monotonic() >= deadline:
                raise AssertionError("S3 object deletion completed after its deadline")
            self.events.record(
                "s3.delete_object.complete",
                case=case_id,
                bucket=bucket,
                key=key,
                attempts=attempts,
            )
            return summary

    def rc_scanner_status(self, case_id: str, name: str) -> dict[str, Any]:
        nodes: list[dict[str, Any]] = []
        errors: list[str] = []
        for alias, endpoint in self.rc_aliases:
            command = [
                self.rc_bin,
                "--json",
                "--no-color",
                "admin",
                "scanner",
                "status",
                alias,
            ]
            result = subprocess.run(
                command,
                env=self.rc_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=getattr(self.args, "request_timeout", 10),
            )
            if result.returncode != 0:
                error = (
                    f"{endpoint}: rc scanner status exited {result.returncode}: "
                    f"{result.stderr.strip()}"
                )
                nodes.append({"endpoint": endpoint, "error": error})
                errors.append(error)
                continue
            try:
                status = json.loads(result.stdout)
            except json.JSONDecodeError:
                error = f"{endpoint}: rc scanner status did not return JSON"
                nodes.append({"endpoint": endpoint, "error": error})
                errors.append(error)
                continue
            nodes.append({"endpoint": endpoint, "status": status})
        cluster_status = {"nodes": nodes}
        self.save_json(case_id, name, cluster_status)
        leaders = [
            node["endpoint"]
            for node in nodes
            if node.get("status", {})
            .get("data", {})
            .get("metrics", {})
            .get("leader_lock_held_by_this_process")
            is True
        ]
        observations = []
        for node in nodes:
            data = node.get("status", {}).get("data", {})
            metrics = data.get("metrics", {})
            observations.append({
                "endpoint": node["endpoint"], "error": node.get("error"),
                "enabled": data.get("enabled"), "health": data.get("health"),
                "freshness": data.get("freshness", {}),
                "executionRole": data.get("cycle_schedule", {}).get("execution_role"),
                "leaderHeldHere": metrics.get("leader_lock_held_by_this_process"),
                "leaderLock": metrics.get("leader_lock_state"),
                "lastCycleResult": metrics.get("last_cycle_result"),
                "lifetimeCycles": metrics.get("life_time_ops", {}).get("scan_cycle"),
                "currentCycle": metrics.get("current_cycle"),
                "currentStarted": metrics.get("current_started"),
            })
        self.events.record(
            "rc.scanner.status",
            case=case_id,
            endpoints=[node["endpoint"] for node in nodes],
            leaders=leaders,
            observations=observations,
        )
        if errors:
            raise RuntimeError("; ".join(errors))
        return cluster_status

    def calibrate_clock(self, case_id: str) -> None:
        samples = []
        for endpoint, client in self.node_clients.items():
            started = time.time()
            response = client.list_buckets()
            finished = time.time()
            samples.append((endpoint, ServerClock.response_date(response), started, finished))
        self.clock = ServerClock(samples, max_skew=self.args.max_clock_skew)
        self.clock_sample_index = getattr(self, "clock_sample_index", 0) + 1
        self.save_json(case_id, f"clock-{self.clock_sample_index:04d}.json", self.clock.as_dict())

    def prepared_days(self, allowance: float = 0) -> int:
        # Independent of how long previous cases took; choose before creating data.
        setup = (self.args.setup_budget + allowance + 2 * self.args.max_clock_skew
                 + 2 * self.args.poll_interval)
        return max(1, math.ceil(setup / self.args.debug_day_seconds))

    def object_window(self, case_id: str, bucket: str, key: str, days: int) -> tuple[DueWindow, dict[str, Any]]:
        assert self.clock is not None
        started = time.time()
        head = self.client.head_object(Bucket=bucket, Key=key)
        self.clock.validate_response(head, started, time.time())
        due = expiry_window(head["LastModified"], days, self.args.debug_day_seconds, self.args.process_seconds)
        self.save_json(case_id, f"timing-{safe_name(bucket)}-{safe_name(key)}.json", {
            "bucket": bucket, "key": key, "days": days,
            "lastModified": head["LastModified"], "due": due.as_dict(),
        })
        return due, head

    def assert_body(self, bucket: str, key: str, expected: bytes, **kwargs: Any) -> None:
        assert self.clock is not None
        started = time.time()
        response = self.client.get_object(Bucket=bucket, Key=key, **kwargs)
        body = response["Body"].read()
        self.clock.validate_response(response, started, time.time())
        if body != expected:
            raise AssertionError(f"object content changed: {bucket}/{key}")

    def object_absent(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if self.client_error_code(exc) in {"NoSuchKey", "404", "NotFound"}:
                return True
            raise
        return False

    def confirm_configuration(
        self, case_id: str, bucket: str, expected: dict[str, Any] | None
    ) -> None:
        def matches() -> tuple[bool, Any]:
            views = {}
            for endpoint, client in self.node_clients.items():
                try:
                    response = client.get_bucket_lifecycle_configuration(Bucket=bucket)
                    views[endpoint] = response.get("Rules", [])
                except ClientError as exc:
                    if self.client_error_code(exc) not in {"NoSuchLifecycleConfiguration", "NoSuchLifecycle"}:
                        raise
                    views[endpoint] = None
            expected_rules = None if expected is None else expected["Rules"]
            return all(view == expected_rules for view in views.values()), views

        final = self.wait_until(case_id, f"lifecycle configuration for {bucket} on every source node", matches,
                                timeout=self.args.setup_budget)
        phase = "absent" if expected is None else "present"
        self.save_json(case_id, f"configuration-node-views-{bucket}-{phase}.json",
                       {"bucket": bucket, "expected": expected, "views": final})

    def observe_protected_object(
        self, case_id: str, bucket: str, key: str, body: bytes,
        due: DueWindow, control_bucket: str, control_key: str,
    ) -> list[str]:
        assert self.clock is not None
        progress = CycleProgress()
        observation_started: float | None = None

        def preserved() -> tuple[bool, Any]:
            nonlocal observation_started
            self.assert_body(bucket, key, body)
            due_passed = self.clock.passed(due)
            if due_passed and observation_started is None:
                observation_started = time.monotonic()
            cycles = 0
            control_expired = False
            if observation_started is not None:
                cluster = self.rc_scanner_status(case_id, "negative-scanner-latest.json")
                cycles = progress.observe(cluster)
                control_expired = self.object_absent(control_bucket, control_key)
            observed_for = 0 if observation_started is None else time.monotonic() - observation_started
            detail = {"pastDue": due_passed, "newSuccessfulCycles": cycles,
                      "positiveControlExpired": control_expired, "observedSeconds": observed_for}
            complete = (due_passed and cycles >= 2 and control_expired
                        and observed_for >= self.args.negative_window)
            if complete:
                self.assert_body(bucket, key, body)
            return complete, detail

        final = self.wait_until(case_id, "protected object with active expiry control", preserved,
                                timeout=self.clock.timeout_after(due, self.args.timeout))
        return [self.save_json(case_id, "negative-observation-final.json", final)]

    def wait_until(
        self,
        case_id: str,
        description: str,
        predicate: Callable[[], tuple[bool, Any]],
        timeout: float | None = None,
    ) -> Any:
        budget = self.args.timeout if timeout is None else timeout
        deadline = time.monotonic() + budget
        attempts = 0
        last_detail: Any = None
        while time.monotonic() < deadline:
            attempts += 1
            with time_budget(deadline - time.monotonic(), description):
                complete, last_detail = predicate()
            self.events.record(
                "poll.observation", case=case_id, description=description,
                attempts=attempts, complete=complete, detail=last_detail,
            )
            if time.monotonic() >= deadline:
                break
            if complete:
                self.events.record(
                    "poll.complete",
                    case=case_id,
                    description=description,
                    attempts=attempts,
                    detail=last_detail,
                )
                return last_detail
            time.sleep(min(self.args.poll_interval, max(0, deadline - time.monotonic())))
        self.events.record(
            "poll.timeout",
            case=case_id,
            description=description,
            attempts=attempts,
            detail=last_detail,
        )
        self.save_json(
            case_id,
            f"poll-timeout-{safe_name(description)}.json",
            {
                "description": description,
                "attempts": attempts,
                "lastDetail": last_detail,
            },
        )
        raise AssertionError(
            f"timed out waiting for {description}; last observation: {last_detail}"
        )

    def cleanup_bucket(self, bucket: str) -> None:
        self.events.record("bucket.cleanup.start", bucket=bucket)
        try:
            self.client.delete_bucket_lifecycle(Bucket=bucket)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {
                "NoSuchBucket",
                "NoSuchLifecycleConfiguration",
            }:
                raise

        try:
            uploads = self.client.list_multipart_uploads(Bucket=bucket).get(
                "Uploads", []
            )
            for upload in uploads:
                self.client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=upload["Key"],
                    UploadId=upload["UploadId"],
                )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "NoSuchBucket":
                raise

        deadline = time.monotonic() + max(30.0, min(self.args.timeout, 60.0))
        attempts = 0
        while True:
            attempts += 1
            version_items: list[dict[str, str]] = []
            object_items: list[dict[str, str]] = []
            try:
                version_response = self.client.list_object_versions(Bucket=bucket)
                version_items = [
                    {"Key": item["Key"], "VersionId": item["VersionId"]}
                    for group in ("Versions", "DeleteMarkers")
                    for item in version_response.get(group, [])
                ]
                if version_items:
                    self.client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": version_items, "Quiet": True},
                    )

                object_response = self.client.list_objects_v2(Bucket=bucket)
                object_items = [
                    {"Key": item["Key"]}
                    for item in object_response.get("Contents", [])
                ]
                if object_items:
                    self.client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": object_items, "Quiet": True},
                    )
                self.client.delete_bucket(Bucket=bucket)
                break
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code == "NoSuchBucket":
                    break
                if code != "BucketNotEmpty" or time.monotonic() >= deadline:
                    raise
                self.events.record(
                    "bucket.cleanup.retry",
                    bucket=bucket,
                    attempt=attempts,
                    errorCode=code,
                    visibleVersions=len(version_items),
                    visibleObjects=len(object_items),
                )
                time.sleep(self.args.poll_interval)
        with self.state_lock:
            self.created_buckets.discard(bucket)
        self.events.record(
            "bucket.cleanup.complete",
            bucket=bucket,
            attempts=attempts,
        )

    def case_scanner_status_readiness(
        self, case_id: str, bucket: str
    ) -> list[str]:
        del bucket
        progress = CycleProgress()

        def scanner_ready() -> tuple[bool, Any]:
            cluster = self.rc_scanner_status(
                case_id, "scanner-readiness-latest.json"
            )
            details: list[dict[str, Any]] = []
            configured = True
            leaders: list[dict[str, Any]] = []
            for node in cluster["nodes"]:
                data = node["status"].get("data", {})
                metrics = data.get("metrics", {})
                runtime = data.get("runtime_config", {})
                cycle = runtime.get("cycle_interval_seconds", {})
                start_delay = runtime.get("start_delay_seconds", {})
                schedule = data.get("cycle_schedule", {})
                node_detail = {
                    "endpoint": node["endpoint"],
                    "health": data.get("health"),
                    "enabled": data.get("enabled"),
                    "freshness": data.get("freshness", {}).get("state"),
                    "leaderLock": metrics.get("leader_lock_state"),
                    "leaderHeldHere": metrics.get(
                        "leader_lock_held_by_this_process"
                    ),
                    "lastCycleResult": metrics.get("last_cycle_result"),
                    "lastCyclePartialReason": metrics.get(
                        "last_cycle_partial_reason"
                    ),
                    "cycleInterval": cycle,
                    "startDelay": start_delay,
                    "executionRole": schedule.get("execution_role"),
                    "effectiveIntervalSeconds": schedule.get(
                        "effective_interval_seconds"
                    ),
                }
                details.append(node_detail)
                configured = configured and (
                    data.get("enabled") is True
                    and cycle.get("value") == self.args.scanner_cycle_seconds
                    and cycle.get("source") == "env"
                    and start_delay.get("value") == 0
                    and start_delay.get("source") == "env"
                )
                if metrics.get("leader_lock_held_by_this_process") is True:
                    leaders.append(node_detail)
            healthy_leader = (
                len(leaders) == 1
                and leaders[0]["health"] == "healthy"
                and leaders[0]["freshness"] == "fresh"
                and leaders[0]["leaderLock"] == "acquired"
                and leaders[0]["executionRole"] == "leader"
                and leaders[0]["lastCycleResult"] == "success"
            )
            cycles = progress.observe(cluster)
            detail = {"leaderCount": len(leaders), "nodes": details, "newSuccessfulCycles": cycles}
            return configured and healthy_leader and cycles >= 1, detail

        final = self.wait_until(
            case_id,
            "a successful scanner cycle with the configured runtime",
            scanner_ready,
        )
        return [self.save_json(case_id, "scanner-readiness-final.json", final)]

    def case_configuration_roundtrip(self, case_id: str, bucket: str) -> list[str]:
        configuration = {
            "Rules": [
                {
                    "ID": "roundtrip-expire",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "expire/"},
                    "Expiration": {"Days": 1},
                },
                {
                    "ID": "roundtrip-disabled",
                    "Status": "Disabled",
                    "Filter": {"Prefix": "disabled/"},
                    "Expiration": {"Days": 5},
                },
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        response = self.get_lifecycle(bucket)
        evidence.append(self.save_json(case_id, "lifecycle-get-response.json", response))
        observed = {
            (rule.get("ID"), rule.get("Status"), rule.get("Expiration", {}).get("Days"))
            for rule in response.get("Rules", [])
        }
        expected = {
            ("roundtrip-expire", "Enabled", 1),
            ("roundtrip-disabled", "Disabled", 5),
        }
        if not expected.issubset(observed):
            raise AssertionError(
                f"lifecycle API round-trip mismatch: expected {expected}, got {observed}"
            )
        rc_output = self.rc_lifecycle_list(case_id, bucket)
        for rule_id in ("roundtrip-expire", "roundtrip-disabled"):
            if rule_id not in rc_output:
                raise AssertionError(f"rc output does not contain rule {rule_id!r}")
        evidence.append(
            str(
                self.evidence_path(case_id, "rc-lifecycle-list.json").relative_to(
                    self.evidence_dir
                )
            )
        )
        self.client.delete_bucket_lifecycle(Bucket=bucket)
        try:
            self.get_lifecycle(bucket)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"NoSuchLifecycleConfiguration", "NoSuchLifecycle"}:
                raise
        else:
            raise AssertionError("deleted lifecycle configuration is still readable")
        return evidence

    def case_invalid_configuration(self, case_id: str, bucket: str) -> list[str]:
        fixtures = {
            "negative-days": """<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule><ID>invalid-days</ID><Status>Enabled</Status><Filter><Prefix/></Filter>
    <Expiration><Days>-1</Days></Expiration></Rule>
</LifecycleConfiguration>
""",
            "invalid-status": """<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule><ID>invalid-status</ID><Status>Sometimes</Status><Filter><Prefix/></Filter>
    <Expiration><Days>1</Days></Expiration></Rule>
</LifecycleConfiguration>
""",
        }
        evidence: list[str] = []
        observations: dict[str, Any] = {}
        for name, lifecycle_xml in fixtures.items():
            status, headers, body = self.raw_s3_request(
                "PUT",
                bucket,
                query="lifecycle",
                body=lifecycle_xml.encode("utf-8"),
                content_type="application/xml",
                allow_error=True,
            )
            observations[name] = {
                "httpStatus": status,
                "headers": headers,
                "body": body.decode("utf-8", errors="replace"),
            }
            if status < 400 or status >= 500:
                raise AssertionError(
                    f"invalid lifecycle {name} returned HTTP {status}, expected a 4xx rejection"
                )
        try:
            self.get_lifecycle(bucket)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"NoSuchLifecycleConfiguration", "NoSuchLifecycle"}:
                raise
            observations["storedConfiguration"] = {"error": code}
        else:
            raise AssertionError("an invalid lifecycle configuration was partially persisted")
        evidence.append(self.save_json(case_id, "invalid-configurations.json", observations))
        return evidence

    def case_current_expiration(self, case_id: str, bucket: str) -> list[str]:
        assert self.clock is not None
        days = self.prepared_days()
        expiring = {
            "expire/one.txt": b"current-expiration-one",
            "expire/two.txt": b"current-expiration-two",
        }
        control_key = "keep/control.txt"
        control_body = b"control-object-must-survive"
        due_by_key: dict[str, DueWindow] = {}
        for key, body in expiring.items():
            self.client.put_object(Bucket=bucket, Key=key, Body=body)
            due_by_key[key], _ = self.object_window(case_id, bucket, key, days)
        self.client.put_object(Bucket=bucket, Key=control_key, Body=control_body)
        configuration = {
            "Rules": [
                {
                    "ID": "expire-current",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "expire/"},
                    "Expiration": {"Days": days},
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        self.confirm_configuration(case_id, bucket, configuration)
        due = DueWindow(min(item.earliest for item in due_by_key.values()),
                        max(item.latest for item in due_by_key.values()))
        self.clock.require_before(due, margin=self.args.request_timeout)
        header_due, head = self.object_window(case_id, bucket, "expire/one.txt", days)
        evidence.append(self.save_json(case_id, "head-before-expiry.json", head))
        self.clock.require_before(due, margin=self.args.poll_interval)
        expiration = str(head.get("Expiration", ""))
        date_match = re.search(r'expiry-date="([^"]+)"', expiration)
        id_match = re.search(r'rule-id="([^"]+)"', expiration)
        if not date_match or not id_match or unquote(id_match.group(1)) != "expire-current":
            raise AssertionError(f"missing or incorrect expiration header: {expiration}")
        expires = parse_expiry_date(date_match.group(1))
        if not header_due.earliest <= expires <= header_due.latest:
            raise AssertionError("expiration header disagrees with independent Days calculation")

        def expired() -> tuple[bool, Any]:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix="expire/")
            keys = [item["Key"] for item in response.get("Contents", [])]
            for key, object_due in due_by_key.items():
                if key not in keys and self.clock.now().latest < object_due.earliest:
                    raise AssertionError(f"object expired before its Days deadline: {key}")
            return not keys, {"remaining": keys}

        final = self.wait_until(case_id, "current objects to expire", expired,
                                timeout=self.clock.timeout_after(due, self.args.timeout))
        evidence.append(self.save_json(case_id, "expiration-final.json", final))
        body = self.client.get_object(Bucket=bucket, Key=control_key)["Body"].read()
        if body != control_body:
            raise AssertionError("control object changed while expiring filtered objects")
        return evidence

    def case_tag_filter_expiration(self, case_id: str, bucket: str) -> list[str]:
        matching_key = "tag-filter/matching.txt"
        control_key = "tag-filter/control.txt"
        matching_body = b"tag-filter-matching"
        control_body = b"tag-filter-control"
        self.client.put_object(
            Bucket=bucket,
            Key=matching_key,
            Body=matching_body,
            Tagging="retention=short",
        )
        self.client.put_object(
            Bucket=bucket,
            Key=control_key,
            Body=control_body,
            Tagging="retention=long",
        )
        configuration = {
            "Rules": [
                {
                    "ID": "expire-by-tag",
                    "Status": "Enabled",
                    "Filter": {"Tag": {"Key": "retention", "Value": "short"}},
                    "Expiration": {"Days": 1},
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)

        def expired() -> tuple[bool, Any]:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix="tag-filter/")
            keys = [item["Key"] for item in response.get("Contents", [])]
            return matching_key not in keys and control_key in keys, {"keys": keys}

        final = self.wait_until(case_id, "tag-filtered object to expire", expired)
        evidence.append(self.save_json(case_id, "tag-filter-final.json", final))
        body = self.client.get_object(Bucket=bucket, Key=control_key)["Body"].read()
        if body != control_body:
            raise AssertionError("tag-filter control object changed")
        return evidence

    def case_disabled_rule(self, case_id: str, bucket: str) -> list[str]:
        assert self.clock is not None
        days = self.prepared_days()
        key = "disabled/object.txt"
        body = b"disabled-rule-must-not-run"
        self.client.put_object(Bucket=bucket, Key=key, Body=body)
        due, _ = self.object_window(case_id, bucket, key, days)
        control_key = "enabled/control.txt"
        self.client.put_object(Bucket=bucket, Key=control_key, Body=b"enabled-control")
        control_due, _ = self.object_window(case_id, bucket, control_key, days)
        configuration = {
            "Rules": [
                {
                    "ID": "disabled-expiration",
                    "Status": "Disabled",
                    "Filter": {"Prefix": "disabled/"},
                    "Expiration": {"Days": days},
                },
                {"ID": "enabled-positive-control", "Status": "Enabled",
                 "Filter": {"Prefix": "enabled/"}, "Expiration": {"Days": days}},
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        self.confirm_configuration(case_id, bucket, configuration)
        self.clock.require_before(due, margin=self.args.request_timeout)
        self.assert_body(bucket, key, body)
        self.clock.require_before(due, margin=self.args.poll_interval)
        window = DueWindow(due.earliest, max(due.latest, control_due.latest))
        evidence.extend(self.observe_protected_object(case_id, bucket, key, body, window, bucket, control_key))
        return evidence

    def case_rule_removal(self, case_id: str, bucket: str) -> list[str]:
        assert self.clock is not None
        days = self.prepared_days()
        key = "removed-rule/object.txt"
        body = b"removed-rule-must-not-run"
        self.client.put_object(Bucket=bucket, Key=key, Body=body)
        due, _ = self.object_window(case_id, bucket, key, days)
        control_bucket = self.bucket_name(f"{case_id}.control")
        self.create_bucket(control_bucket)
        self.client.put_object(Bucket=control_bucket, Key=key, Body=b"enabled-control")
        control_due, _ = self.object_window(case_id, control_bucket, key, days)
        configuration = {
            "Rules": [
                {
                    "ID": "remove-before-due",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "removed-rule/"},
                    "Expiration": {"Days": days},
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        self.confirm_configuration(case_id, bucket, configuration)
        self.client.put_bucket_lifecycle_configuration(Bucket=control_bucket, LifecycleConfiguration=configuration)
        self.confirm_configuration(case_id, control_bucket, configuration)
        self.clock.require_before(due, margin=self.args.poll_interval)
        deleted = self.client.delete_bucket_lifecycle(Bucket=bucket)
        evidence.append(self.save_json(case_id, "lifecycle-delete-response.json", deleted))
        self.confirm_configuration(case_id, bucket, None)
        self.clock.require_before(due, margin=self.args.request_timeout)
        self.assert_body(bucket, key, body)
        self.clock.require_before(due, margin=self.args.poll_interval)
        window = DueWindow(due.earliest, max(due.latest, control_due.latest))
        evidence.extend(self.observe_protected_object(case_id, bucket, key, body, window, control_bucket, key))
        return evidence

    def case_versioned_current_expiration(self, case_id: str, bucket: str) -> list[str]:
        self.enable_versioning(bucket)
        key = "versioned-current/object.txt"
        put = self.client.put_object(Bucket=bucket, Key=key, Body=b"versioned-current")
        configuration = {
            "Rules": [
                {
                    "ID": "expire-versioned-current",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "versioned-current/"},
                    "Expiration": {"Days": 1},
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)

        def marker_created() -> tuple[bool, Any]:
            response = self.client.list_object_versions(Bucket=bucket, Prefix=key)
            markers = [
                item
                for item in response.get("DeleteMarkers", [])
                if item["Key"] == key and item["IsLatest"]
            ]
            versions = [
                item for item in response.get("Versions", []) if item["Key"] == key
            ]
            return bool(markers) and any(
                item["VersionId"] == put.get("VersionId") for item in versions
            ), {"deleteMarkers": markers, "versions": versions}

        final = self.wait_until(
            case_id,
            "versioned current expiration delete marker",
            marker_created,
            timeout=max(self.args.timeout, self.args.debug_day_seconds * 12),
        )
        try:
            self.client.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                raise
        else:
            raise AssertionError("versioned current object remains visible after expiration")
        original = self.client.get_object(
            Bucket=bucket, Key=key, VersionId=put["VersionId"]
        )["Body"].read()
        if original != b"versioned-current":
            raise AssertionError("expiration changed the retained original version")
        evidence.append(self.save_json(case_id, "versioned-current-final.json", final))
        return evidence

    def enable_versioning(self, bucket: str) -> None:
        self.client.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        observed = self.client.get_bucket_versioning(Bucket=bucket).get("Status")
        if observed != "Enabled":
            raise AssertionError(f"versioning did not become enabled: {observed!r}")

    def case_noncurrent_expiration(self, case_id: str, bucket: str) -> list[str]:
        self.enable_versioning(bucket)
        key = "versioned/object.txt"
        v1 = self.client.put_object(Bucket=bucket, Key=key, Body=b"version-one")[
            "VersionId"
        ]
        v2 = self.client.put_object(Bucket=bucket, Key=key, Body=b"version-two")[
            "VersionId"
        ]
        configuration = {
            "Rules": [
                {
                    "ID": "expire-noncurrent",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "versioned/"},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        evidence.append(
            self.save_json(
                case_id,
                "versions-before.json",
                self.client.list_object_versions(Bucket=bucket, Prefix=key),
            )
        )

        def old_version_removed() -> tuple[bool, Any]:
            response = self.client.list_object_versions(Bucket=bucket, Prefix=key)
            versions = [
                {
                    "versionId": item["VersionId"],
                    "isLatest": item["IsLatest"],
                }
                for item in response.get("Versions", [])
                if item["Key"] == key
            ]
            ids = {item["versionId"] for item in versions}
            return v1 not in ids and v2 in ids, {"versions": versions}

        final = self.wait_until(
            case_id, "noncurrent version to expire", old_version_removed
        )
        evidence.append(self.save_json(case_id, "versions-final.json", final))
        body = self.client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if body != b"version-two":
            raise AssertionError("latest version changed during noncurrent expiration")
        return evidence

    def case_newer_noncurrent_retention(
        self, case_id: str, bucket: str
    ) -> list[str]:
        self.enable_versioning(bucket)
        key = "newer-noncurrent/object.txt"
        version_ids = [
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=f"version-{number}".encode("ascii"),
            )["VersionId"]
            for number in range(1, 5)
        ]
        configuration = {
            "Rules": [
                {
                    "ID": "retain-two-newer-noncurrent",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "newer-noncurrent/"},
                    "NoncurrentVersionExpiration": {
                        "NoncurrentDays": 1,
                        "NewerNoncurrentVersions": 2,
                    },
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        evidence.append(
            self.save_json(
                case_id,
                "newer-noncurrent-before.json",
                self.client.list_object_versions(Bucket=bucket, Prefix=key),
            )
        )
        expected = set(version_ids[-3:])

        def retained() -> tuple[bool, Any]:
            response = self.client.list_object_versions(Bucket=bucket, Prefix=key)
            observed = {
                item["VersionId"]
                for item in response.get("Versions", [])
                if item["Key"] == key
            }
            return observed == expected, {
                "expected": sorted(expected),
                "observed": sorted(observed),
                "expired": version_ids[0],
            }

        final = self.wait_until(
            case_id,
            "NewerNoncurrentVersions retention",
            retained,
            timeout=max(self.args.timeout, self.args.debug_day_seconds * 12),
        )
        latest = self.client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if latest != b"version-4":
            raise AssertionError("latest object changed while retaining noncurrent versions")
        evidence.append(self.save_json(case_id, "newer-noncurrent-final.json", final))
        return evidence

    def case_delete_all_versions(self, case_id: str, bucket: str) -> list[str]:
        self.enable_versioning(bucket)
        key = "all-versions/object.txt"
        version_ids = [
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=f"version-{number}".encode("ascii"),
            )["VersionId"]
            for number in range(1, 4)
        ]
        lifecycle_xml = """<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>delete-all-versions</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>all-versions/</Prefix></Filter>
    <Expiration>
      <Days>1</Days>
      <ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>
    </Expiration>
  </Rule>
</LifecycleConfiguration>
"""
        evidence = [
            self.save_text(case_id, "lifecycle-request.xml", lifecycle_xml)
        ]
        status, headers, _ = self.raw_s3_request(
            "PUT",
            bucket,
            query="lifecycle",
            body=lifecycle_xml.encode("utf-8"),
            content_type="application/xml",
        )
        evidence.append(
            self.save_json(
                case_id,
                "lifecycle-put-response.json",
                {"httpStatus": status, "headers": headers},
            )
        )
        _, _, stored_xml = self.raw_s3_request(
            "GET", bucket, query="lifecycle"
        )
        stored_text = stored_xml.decode("utf-8")
        evidence.append(
            self.save_text(case_id, "lifecycle-get-response.xml", stored_text)
        )
        if "<ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>" not in stored_text:
            raise AssertionError(
                "stored lifecycle configuration lost ExpiredObjectAllVersions"
            )
        evidence.append(
            self.save_json(
                case_id,
                "versions-before.json",
                self.client.list_object_versions(Bucket=bucket, Prefix=key),
            )
        )

        def all_versions_removed() -> tuple[bool, Any]:
            response = self.client.list_object_versions(Bucket=bucket, Prefix=key)
            versions = [
                item["VersionId"]
                for item in response.get("Versions", [])
                if item["Key"] == key
            ]
            markers = [
                item["VersionId"]
                for item in response.get("DeleteMarkers", [])
                if item["Key"] == key
            ]
            return not versions and not markers, {
                "expectedVersionIds": version_ids,
                "versions": versions,
                "deleteMarkers": markers,
            }

        final = self.wait_until(
            case_id,
            "all versions to expire",
            all_versions_removed,
            timeout=max(self.args.timeout, self.args.debug_day_seconds * 12),
        )
        evidence.append(self.save_json(case_id, "versions-final.json", final))
        return evidence

    def case_delete_marker_expiration(self, case_id: str, bucket: str) -> list[str]:
        self.enable_versioning(bucket)
        key = "markers/object.txt"
        configuration = {
            "Rules": [
                {
                    "ID": "expire-delete-marker",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "markers/"},
                    "Expiration": {"ExpiredObjectDeleteMarker": True},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        self.client.put_object(Bucket=bucket, Key=key, Body=b"marker-source")
        self.client.delete_object(Bucket=bucket, Key=key)
        evidence.append(
            self.save_json(
                case_id,
                "markers-before.json",
                self.client.list_object_versions(Bucket=bucket, Prefix=key),
            )
        )

        def marker_chain_removed() -> tuple[bool, Any]:
            response = self.client.list_object_versions(Bucket=bucket, Prefix=key)
            versions = [
                item["VersionId"]
                for item in response.get("Versions", [])
                if item["Key"] == key
            ]
            markers = [
                item["VersionId"]
                for item in response.get("DeleteMarkers", [])
                if item["Key"] == key
            ]
            return not versions and not markers, {
                "versions": versions,
                "deleteMarkers": markers,
            }

        final = self.wait_until(
            case_id,
            "noncurrent object and expired delete marker to be removed",
            marker_chain_removed,
            timeout=max(self.args.timeout, self.args.debug_day_seconds * 12),
        )
        evidence.append(self.save_json(case_id, "markers-final.json", final))
        return evidence

    def case_multipart_expiration(self, case_id: str, bucket: str) -> list[str]:
        key = "multipart/incomplete.bin"
        configuration = {
            "Rules": [
                {
                    "ID": "abort-incomplete-multipart",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "multipart/"},
                    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
                }
            ]
        }
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        created = self.client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = created["UploadId"]
        self.client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=b"x" * (5 * 1024 * 1024),
        )
        evidence.append(
            self.save_json(
                case_id,
                "multipart-before.json",
                self.client.list_multipart_uploads(Bucket=bucket, Prefix="multipart/"),
            )
        )

        def upload_removed() -> tuple[bool, Any]:
            response = self.client.list_multipart_uploads(
                Bucket=bucket, Prefix="multipart/"
            )
            uploads = [
                {"key": item["Key"], "uploadId": item["UploadId"]}
                for item in response.get("Uploads", [])
            ]
            return not any(item["uploadId"] == upload_id for item in uploads), {
                "uploads": uploads
            }

        final = self.wait_until(
            case_id,
            "incomplete multipart upload to be aborted",
            upload_removed,
            timeout=max(self.args.timeout, self.args.debug_day_seconds * 12),
        )
        evidence.append(self.save_json(case_id, "multipart-final.json", final))
        return evidence

    def case_transition_and_restore(self, case_id: str, bucket: str) -> list[str]:
        if self.tier_client is None:
            raise RuntimeError(
                "transition-and-restore requires tier endpoint and credentials"
            )
        digest = hashlib.sha256(
            f"{self.args.run_id}:{case_id}".encode("utf-8")
        ).hexdigest()[:12]
        tier_name = f"RV{digest.upper()}"
        tier_bucket = f"rv-tier-{digest}"
        key = "transition/archive.bin"
        body = (b"rustfs-transition-restore-" * 4096) + secrets.token_bytes(32)
        tier_created = False
        tier_added = False
        evidence: list[str] = []
        phase = "remote-tier-bucket-create"
        primary_error: Exception | None = None
        primary_phase = ""
        cleanup_error: Exception | None = None
        cleanup_phase = ""
        try:
            self.tier_client.create_bucket(Bucket=tier_bucket)
            tier_created = True
            phase = "tier-add"
            add = self.rc_command(
                case_id,
                "tier-add",
                [
                    "ilm",
                    "tier",
                    "add",
                    "rustfs",
                    tier_name,
                    self.rc_alias,
                    "--endpoint",
                    self.args.tier_endpoint,
                    "--access-key",
                    self.args.tier_access_key,
                    "--secret-key",
                    self.args.tier_secret_key,
                    "--bucket",
                    tier_bucket,
                ],
                secrets_to_redact=(self.args.tier_secret_key,),
            )
            tier_added = add.returncode == 0
            evidence.append(
                str(
                    self.evidence_path(case_id, "tier-add.json").relative_to(
                        self.evidence_dir
                    )
                )
            )
            phase = "source-put"
            put = self.client.put_object(Bucket=bucket, Key=key, Body=body)
            configuration = {
                "Rules": [
                    {
                        "ID": "transition-to-real-tier",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "transition/"},
                        "Transitions": [{"Days": 1, "StorageClass": tier_name}],
                    }
                ]
            }
            phase = "lifecycle-put"
            evidence.extend(self.put_lifecycle(case_id, bucket, configuration))
            evidence.append(
                self.save_json(
                    case_id,
                    "transition-source-put.json",
                    {
                        "versionId": put.get("VersionId"),
                        "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(),
                    },
                )
            )

            def transitioned() -> tuple[bool, Any]:
                head = self.client.head_object(Bucket=bucket, Key=key)
                observed = head.get("StorageClass") or head.get("storage-class")
                return str(observed or "").upper() == tier_name, {
                    "storageClass": observed,
                    "restore": head.get("Restore"),
                    "contentLength": head.get("ContentLength"),
                }

            phase = "transition-wait"
            final = self.wait_until(
                case_id,
                "object transition to remote tier",
                transitioned,
                timeout=max(self.args.timeout, self.args.debug_day_seconds * 18),
            )
            evidence.append(self.save_json(case_id, "transition-final.json", final))
            phase = "transparent-read"
            transparent = self.client.get_object(Bucket=bucket, Key=key)["Body"].read()
            if transparent != body:
                raise AssertionError("transparent read changed transitioned object bytes")
            evidence.append(
                self.save_json(
                    case_id,
                    "transition-transparent-read.json",
                    {
                        "bytes": len(transparent),
                        "sha256": hashlib.sha256(transparent).hexdigest(),
                    },
                )
            )
            phase = "remote-object-list"
            remote_objects = self.tier_client.list_objects_v2(Bucket=tier_bucket)
            if not remote_objects.get("Contents"):
                raise AssertionError("remote tier bucket contains no transitioned object")
            evidence.append(
                self.save_json(case_id, "tier-remote-objects.json", remote_objects)
            )
            phase = "restore-request"
            self.rc_command(
                case_id,
                "tier-restore",
                [
                    "ilm",
                    "restore",
                    f"{self.rc_alias}/{bucket}/{key}",
                    "--days",
                    "1",
                    "--force",
                ],
            )
            evidence.append(
                str(
                    self.evidence_path(case_id, "tier-restore.json").relative_to(
                        self.evidence_dir
                    )
                )
            )

            def restored() -> tuple[bool, Any]:
                head = self.client.head_object(Bucket=bucket, Key=key)
                restore = str(head.get("Restore", ""))
                return bool(restore) and 'ongoing-request="false"' in restore, {
                    "restore": restore,
                    "storageClass": head.get("StorageClass"),
                }

            phase = "restore-wait"
            restore_final = self.wait_until(
                case_id,
                "transitioned object restore completion",
                restored,
                timeout=max(self.args.timeout, self.args.debug_day_seconds * 18),
            )
            evidence.append(
                self.save_json(case_id, "restore-final.json", restore_final)
            )
            phase = "restored-read"
            restored_body = self.client.get_object(Bucket=bucket, Key=key)["Body"].read()
            if restored_body != body:
                raise AssertionError("restored object bytes differ from original")
        except Exception as exc:
            primary_error = exc
            primary_phase = phase
            evidence.append(
                self.save_phase_error(
                    case_id,
                    "transition-primary-error.json",
                    primary_phase,
                    exc,
                )
            )
        finally:
            cleanup_phase = "lifecycle-delete"
            try:
                try:
                    self.client.delete_bucket_lifecycle(Bucket=bucket)
                except ClientError:
                    pass
                if tier_added:
                    # A transitioned object is an authoritative reference to its tier.
                    # Remove the source object and wait for its remote payload cleanup
                    # before asking RustFS to remove the tier configuration.  --force
                    # bypasses lifecycle-config references, but deliberately does not
                    # bypass live object references.
                    cleanup_phase = "source-delete"
                    self.delete_object_eventually(
                        case_id,
                        bucket,
                        key,
                        evidence_name="transition-source-delete",
                        timeout=max(
                            self.args.timeout,
                            self.args.debug_day_seconds * 12,
                        ),
                    )
                    evidence.append(
                        str(
                            self.evidence_path(
                                case_id,
                                "transition-source-delete.json",
                            ).relative_to(self.evidence_dir)
                        )
                    )

                    def remote_tier_empty() -> tuple[bool, Any]:
                        response = self.tier_client.list_objects_v2(Bucket=tier_bucket)
                        remaining = [
                            {"key": item["Key"], "size": item.get("Size")}
                            for item in response.get("Contents", [])
                        ]
                        return not remaining, {"remaining": remaining}

                    cleanup_phase = "remote-object-cleanup-wait"
                    cleanup_final = self.wait_until(
                        case_id,
                        "transitioned object cleanup from remote tier",
                        remote_tier_empty,
                        timeout=max(
                            self.args.timeout,
                            self.args.debug_day_seconds * 12,
                        ),
                    )
                    evidence.append(
                        self.save_json(
                            case_id,
                            "tier-object-cleanup-final.json",
                            cleanup_final,
                        )
                    )
                    cleanup_phase = "tier-remove"
                    self.rc_tier_remove_eventually(
                        case_id,
                        tier_name,
                        timeout=max(
                            self.args.timeout,
                            self.args.debug_day_seconds * 12,
                        ),
                    )
                    evidence.append(
                        str(
                            self.evidence_path(
                                case_id, "tier-remove.json"
                            ).relative_to(self.evidence_dir)
                        )
                    )
                if tier_created:
                    cleanup_phase = "remote-tier-bucket-delete"
                    try:
                        versions = self.tier_client.list_object_versions(
                            Bucket=tier_bucket
                        )
                        version_items = [
                            {"Key": item["Key"], "VersionId": item["VersionId"]}
                            for group in ("Versions", "DeleteMarkers")
                            for item in versions.get(group, [])
                        ]
                        if version_items:
                            self.tier_client.delete_objects(
                                Bucket=tier_bucket,
                                Delete={"Objects": version_items, "Quiet": True},
                            )
                        objects = self.tier_client.list_objects_v2(
                            Bucket=tier_bucket
                        ).get("Contents", [])
                        if objects:
                            self.tier_client.delete_objects(
                                Bucket=tier_bucket,
                                Delete={
                                    "Objects": [
                                        {"Key": item["Key"]} for item in objects
                                    ],
                                    "Quiet": True,
                                },
                            )
                        self.tier_client.delete_bucket(Bucket=tier_bucket)
                    except ClientError:
                        pass
            except Exception as exc:
                cleanup_error = exc
                evidence.append(
                    self.save_phase_error(
                        case_id,
                        "transition-cleanup-error.json",
                        cleanup_phase,
                        exc,
                    )
                )

        if primary_error is not None:
            if cleanup_error is not None:
                raise CaseExecutionAndCleanupError(
                    primary_phase,
                    primary_error,
                    cleanup_phase,
                    cleanup_error,
                ) from cleanup_error
            raise primary_error
        if cleanup_error is not None:
            raise cleanup_error
        return evidence

    def case_restart_persistence(self, case_id: str, bucket: str) -> list[str]:
        assert self.clock is not None
        if not self.args.restart_hook:
            raise RuntimeError("restart-persistence requires --restart-hook")
        days = self.prepared_days(self.args.restart_timeout + self.args.setup_budget)
        key = "restart/expire.txt"
        body = b"survive-restart-then-expire"
        configuration = {"Rules": [{
            "ID": "restart-expiry", "Status": "Enabled",
            "Filter": {"Prefix": "restart/"}, "Expiration": {"Days": days},
        }]}
        evidence = self.put_lifecycle(case_id, bucket, configuration)
        self.confirm_configuration(case_id, bucket, configuration)
        self.client.put_object(Bucket=bucket, Key=key, Body=body)
        due, before = self.object_window(case_id, bucket, key, days)
        self.assert_body(bucket, key, body)
        self.clock.require_before(due, margin=self.args.restart_timeout + self.args.setup_budget)
        evidence.append(self.save_json(case_id, "object-before-restart.json", before))
        command = json.loads(self.args.restart_hook)
        if not isinstance(command, list) or not command or not all(
            isinstance(item, str) and item for item in command
        ):
            raise RuntimeError("--restart-hook must be a JSON array of command tokens")
        self.events.record("cluster.restart.start", case=case_id)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, start_new_session=True)
        output = ""
        try:
            with time_budget(self.args.restart_timeout, "cluster restart", TimeoutError):
                output, _ = process.communicate(timeout=self.args.restart_timeout)
            if process.returncode != 0:
                raise RuntimeError(f"cluster restart hook failed with exit {process.returncode}")
        except BaseException as primary_error:
            # Stop the local driver/SSH process group; never leave it running into
            # the next case. Remote service state remains unknown and is preserved.
            self.blocked_reason = "restart hook failed; service state requires inspection"
            if isinstance(primary_error, subprocess.TimeoutExpired) and primary_error.output:
                output = primary_error.output
                if isinstance(output, bytes):
                    output = output.decode("utf-8", errors="replace")
            try:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                output, _ = process.communicate(timeout=5)
            except Exception as cleanup_error:
                evidence.append(self.save_json(case_id, "restart-hook-termination-error.json", {
                    "errorType": type(cleanup_error).__name__,
                    "message": self.redact(str(cleanup_error)),
                }))
            raise
        finally:
            evidence.append(self.save_text(case_id, "restart-hook.log", self.redact(output)))
        self.events.record("cluster.restart.complete", case=case_id)
        self.calibrate_clock(case_id)
        self.confirm_configuration(case_id, bucket, configuration)
        self.clock.require_before(due, margin=self.args.request_timeout)
        self.assert_body(bucket, key, body)
        self.clock.require_before(due, margin=self.args.request_timeout)
        _, after = self.object_window(case_id, bucket, key, days)
        if (after["LastModified"], after["ETag"]) != (before["LastModified"], before["ETag"]):
            raise AssertionError("restart changed the object identity")
        evidence.append(self.save_json(case_id, "object-after-restart-before-expiry.json", after))
        evidence.extend(self.case_scanner_status_readiness(case_id, bucket))

        def object_expired() -> tuple[bool, Any]:
            absent = self.object_absent(bucket, key)
            if absent and self.clock.now().latest < due.earliest:
                raise AssertionError("object expired before its Days deadline")
            return absent, {"absent": absent, "due": due.as_dict()}

        final = self.wait_until(case_id, "object expiry after verified restart", object_expired,
                                timeout=self.clock.timeout_after(due, self.args.timeout))
        evidence.append(self.save_json(case_id, "restart-expiration-final.json", final))
        return evidence

    def classify_exception(self, exc: BaseException) -> str:
        if isinstance(exc, CaseExecutionAndCleanupError):
            states = {
                self.classify_exception(exc.primary_error),
                self.classify_exception(exc.cleanup_error),
            }
            return "infra_error" if "infra_error" in states else "fail"
        if isinstance(exc, AssertionError):
            return "fail"
        if isinstance(exc, INFRA_EXCEPTIONS):
            return "infra_error"
        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code")
            return "infra_error" if code in INFRA_ERROR_CODES else "fail"
        return "infra_error"

    def run_case(
        self,
        case_id: str,
        function: Callable[[str, str], list[str]],
        *,
        create_bucket: bool = True,
    ) -> None:
        bucket = self.bucket_name(case_id)
        self.case_buckets = set()
        started_at = utc_now()
        started = time.monotonic()
        state = "pass"
        message = "all assertions passed"
        evidence: list[str] = []
        bucket_label = f" bucket={bucket}" if create_bucket else ""
        print(f"[RUN ] {case_id}{bucket_label}", flush=True)
        self.events.record(
            "case.start",
            case=case_id,
            bucket=bucket if create_bucket else None,
        )
        try:
            with time_budget(self.args.case_timeout, f"case {case_id}"):
                self.rc_scanner_status(case_id, "scanner-before.json")
                self.calibrate_clock(case_id)
                if create_bucket:
                    self.create_bucket(bucket)
                evidence.extend(function(case_id, bucket))
        except Exception as exc:  # retain the failure before inspecting cleanup
            state = self.classify_exception(exc)
            message = self.redact(f"{type(exc).__name__}: {exc}")
            self.events.record(
                "case.error",
                case=case_id,
                state=state,
                errorType=type(exc).__name__,
                message=message,
            )
            self.save_json(
                case_id,
                "case-error.json",
                {
                    "state": state,
                    "errorType": type(exc).__name__,
                    "message": message,
                },
            )
        finally:
            try:
                self.rc_scanner_status(case_id, "scanner-after.json")
            except Exception as status_exc:
                if state == "pass":
                    state = "infra_error"
                    message = (
                        "scanner status collection failed: "
                        f"{type(status_exc).__name__}: {status_exc}"
                    )
                self.events.record(
                    "rc.scanner.status.error",
                    case=case_id,
                    message=self.redact(str(status_exc)),
                )
            should_cleanup = state == "pass" or self.args.cleanup_on_failure
            owned_buckets = self.case_buckets & self.created_buckets
            if owned_buckets and should_cleanup:
                try:
                    with time_budget(self.args.cleanup_timeout, f"cleanup {case_id}", TimeoutError):
                        for owned_bucket in sorted(owned_buckets):
                            self.cleanup_bucket(owned_bucket)
                except Exception as cleanup_exc:
                    cleanup_state = self.classify_exception(cleanup_exc)
                    if state == "pass":
                        state = cleanup_state
                    elif cleanup_state == "infra_error":
                        state = "infra_error"
                    cleanup_message = self.redact(
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                    )
                    message = f"cleanup failed after {message}: {cleanup_message}"
                    self.events.record(
                        "case.cleanup.error",
                        case=case_id,
                        state=cleanup_state,
                        message=cleanup_message,
                    )
                    self.save_json(
                        case_id,
                        "case-cleanup-error.json",
                        {
                            "state": cleanup_state,
                            "errorType": type(cleanup_exc).__name__,
                            "message": cleanup_message,
                        },
                    )
            elif owned_buckets:
                message += f"; preserved failed buckets {', '.join(sorted(owned_buckets))}"
            if self.case_buckets & self.created_buckets:
                self.blocked_reason = f"{case_id} left preserved or uncleaned test buckets"
            if state != "pass" and not self.args.cleanup_on_failure and self.blocked_reason is None:
                self.blocked_reason = f"{case_id} failed; the run is preserved for inspection"

        case_evidence_dir = self.evidence_path(case_id, "evidence-index").parent
        evidence = sorted(
            str(path.relative_to(self.evidence_dir))
            for path in case_evidence_dir.iterdir()
            if path.is_file() and path.name != "evidence-index"
        )

        duration = time.monotonic() - started
        result = CaseResult(
            id=case_id,
            state=state,
            message=message,
            started_at=started_at,
            finished_at=utc_now(),
            duration_seconds=round(duration, 3),
            evidence=evidence,
        )
        with self.state_lock:
            self.results.append(result)
        self.events.record("case.complete", **asdict(result))
        label = "PASS" if state == "pass" else state.upper()
        print(f"[{label:5}] {case_id}: {message}", flush=True)

    def cleanup_outcome(self) -> dict[str, Any]:
        """Describe the logical bucket cleanup completed by the ILM harness."""

        uncertain = sorted(getattr(self, "uncertain_buckets", set()))
        if uncertain:
            return {
                "attempted": False,
                "state": "infra_error",
                "message": "bucket creation outcomes are unknown; isolation was not verified",
                "uncertainBuckets": uncertain,
            }
        preserved = sorted(self.created_buckets)
        if not preserved:
            return {
                "attempted": bool(self.results),
                "state": "pass",
                "message": "no ILM test buckets remain after logical cleanup",
            }
        if self.args.cleanup_on_failure:
            return {
                "attempted": True,
                "state": "infra_error",
                "message": "ILM cleanup was requested but test buckets remain",
                "preservedBuckets": preserved,
            }
        return {
            "attempted": False,
            "state": "preserved",
            "message": "failed-case ILM buckets were preserved for investigation",
            "preservedBuckets": preserved,
        }

    def write_results(self, started_at: str, started: float) -> str:
        selected_cases = ilm_harness_scored_cases(self.args.case)
        selection_mode = "filtered" if self.args.case is not None else "full"
        case_order = {case_id: index for index, case_id in enumerate(CASE_IDS)}
        self.results.sort(key=lambda item: case_order.get(item.id, len(case_order)))
        states = {result.state for result in self.results}
        if "fail" in states:
            overall = "fail"
        elif "infra_error" in states:
            overall = "infra_error"
        else:
            overall = "pass"
        finished_at = utc_now()
        result_document = {
            "apiVersion": "validation.rustfs.io/v1alpha1",
            "kind": "TestRunResult",
            "metadata": {
                "runId": self.args.run_id,
                "environmentId": self.args.environment_id,
            },
            "status": {
                "state": overall,
                "startedAt": started_at,
                "finishedAt": finished_at,
                "cases": [
                    {
                        "id": item.id,
                        "state": item.state,
                        "message": item.message,
                        "evidence": item.evidence,
                    }
                    for item in self.results
                ],
                "evidence": ["events.jsonl", "run-manifest.json", "junit.xml"],
            },
        }
        (self.evidence_dir / "result.json").write_text(
            json.dumps(result_document, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest = {
            "runId": self.args.run_id,
            "environmentId": self.args.environment_id,
            "endpoint": self.args.endpoint,
            "scannerEndpoints": self.args.scanner_endpoint,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "durationSeconds": round(time.monotonic() - started, 3),
            "debugDaySeconds": self.args.debug_day_seconds,
            "processSeconds": self.args.process_seconds,
            "timingBudgets": {
                "requestSeconds": self.args.request_timeout,
                "caseSeconds": self.args.case_timeout,
                "cleanupSeconds": self.args.cleanup_timeout,
                "setupSeconds": self.args.setup_budget,
                "restartSeconds": self.args.restart_timeout,
                "negativeWindowSeconds": self.args.negative_window,
                "maxClockSkewSeconds": self.args.max_clock_skew,
            },
            "readinessOutcome": self.readiness,
            "executionMode": "serial",
            "scannerPollIntervalSeconds": self.args.poll_interval,
            "boto3Version": boto3.__version__,
            "botocoreVersion": botocore.__version__,
            "rcBinaryPath": self.rc_bin,
            "rcVersion": self.rc_version,
            "rcBinarySha256": self.rc_binary_sha256,
            "expectedRcBinarySha256": self.expected_rc_binary_sha256 or None,
            "executionPlan": {
                "selectionMode": selection_mode,
                "module": "ilm",
                "scoredCases": list(selected_cases),
                "scoredCaseCount": len(selected_cases),
                "fixtures": [],
            },
            "fixtureOutcomes": [],
            "cleanupOnFailure": self.args.cleanup_on_failure,
            "cleanupOutcome": self.cleanup_outcome(),
            "preservedBuckets": sorted(self.created_buckets),
        }
        (self.evidence_dir / "run-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.write_junit(overall, time.monotonic() - started)
        return overall

    def write_junit(self, overall: str, duration: float) -> None:
        failures = sum(item.state == "fail" for item in self.results)
        errors = sum(item.state == "infra_error" for item in self.results)
        suite = ET.Element(
            "testsuite",
            {
                "name": "rustfs-fixed-environment-ilm",
                "tests": str(len(self.results)),
                "failures": str(failures),
                "errors": str(errors),
                "skipped": "0",
                "time": f"{duration:.3f}",
            },
        )
        for item in self.results:
            case = ET.SubElement(
                suite,
                "testcase",
                {
                    "classname": "rustfs.ilm",
                    "name": item.id,
                    "time": f"{item.duration_seconds:.3f}",
                },
            )
            if item.state == "fail":
                ET.SubElement(case, "failure", {"message": item.message}).text = (
                    item.message
                )
            elif item.state == "infra_error":
                ET.SubElement(case, "error", {"message": item.message}).text = (
                    item.message
                )
            ET.SubElement(case, "system-out").text = "\n".join(item.evidence)
        tree = ET.ElementTree(suite)
        ET.indent(tree, space="  ")
        tree.write(
            self.evidence_dir / "junit.xml",
            encoding="utf-8",
            xml_declaration=True,
        )
        self.events.record("run.complete", state=overall)

    def record_blocked(self, case_id: str, reason: str, *, state: str = "infra_error") -> None:
        message = f"blocked: {reason}"
        evidence = self.save_json(case_id, "blocked.json", {"state": state, "message": message})
        now = utc_now()
        self.results.append(CaseResult(case_id, state, message, now, now, 0, [evidence]))
        self.events.record("case.blocked", case=case_id, state=state, message=message)

    def run(self) -> int:
        started_at = utc_now()
        started = time.monotonic()
        selected_cases = ilm_harness_scored_cases(self.args.case)
        self.events.record(
            "run.start",
            runId=self.args.run_id,
            environmentId=self.args.environment_id,
            endpoint=self.args.endpoint,
            selectionMode="filtered" if self.args.case is not None else "full",
            scoredCases=list(selected_cases),
            fixtures=[],
        )
        try:
            with time_budget(self.args.timeout, "ILM preflight"):
                self.wait_for_s3_connectivity()
                self.configure_rc()
                self.calibrate_clock("fixture.scanner-readiness")
                self.case_scanner_status_readiness("fixture.scanner-readiness", "")
            self.readiness = {"state": "pass", "message": "clock and new scanner cycle verified"}
        except Exception as exc:
            message = self.redact(f"precondition_error: {type(exc).__name__}: {exc}")
            self.readiness = {"state": self.classify_exception(exc), "message": message}
            for index, case_id in enumerate(selected_cases):
                self.record_blocked(case_id, message,
                                    state=self.classify_exception(exc) if index == 0 else "infra_error")
            overall = self.write_results(started_at, started)
            return 2 if overall == "infra_error" else 1

        cases: dict[str, Callable[[str, str], list[str]]] = {
            "scanner.status-readiness": self.case_scanner_status_readiness,
            "ilm.configuration-roundtrip": self.case_configuration_roundtrip,
            "ilm.invalid-configuration": self.case_invalid_configuration,
            "ilm.current-expiration": self.case_current_expiration,
            "ilm.tag-filter-expiration": self.case_tag_filter_expiration,
            "ilm.disabled-rule": self.case_disabled_rule,
            "ilm.rule-removal": self.case_rule_removal,
            "ilm.versioned-current-expiration": self.case_versioned_current_expiration,
            "ilm.noncurrent-expiration": self.case_noncurrent_expiration,
            "ilm.newer-noncurrent-retention": self.case_newer_noncurrent_retention,
            "ilm.delete-all-versions": self.case_delete_all_versions,
            "ilm.delete-marker-expiration": self.case_delete_marker_expiration,
            "ilm.multipart-expiration": self.case_multipart_expiration,
            "ilm.transition-and-restore": self.case_transition_and_restore,
            "ilm.restart-persistence": self.case_restart_persistence,
        }
        for case_id in selected_cases:
            if self.blocked_reason:
                self.record_blocked(case_id, self.blocked_reason)
                continue
            self.run_case(case_id, cases[case_id], create_bucket=case_id != "scanner.status-readiness")

        overall = self.write_results(started_at, started)
        print(
            f"ILM validation {overall}: result={self.evidence_dir / 'result.json'}",
            flush=True,
        )
        return {"pass": 0, "fail": 1, "infra_error": 2}[overall]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=os.environ.get("RUSTFS_ENDPOINT"))
    parser.add_argument(
        "--scanner-endpoint",
        action="append",
        help="node endpoint queried for scanner status; repeat for every node",
    )
    parser.add_argument("--access-key", default=os.environ.get("RUSTFS_ACCESS_KEY"))
    parser.add_argument("--secret-key", default=os.environ.get("RUSTFS_SECRET_KEY"))
    parser.add_argument("--region", default=os.environ.get("RUSTFS_REGION", "us-east-1"))
    parser.add_argument("--environment-id", default="ilm-2x4")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--bucket-prefix", default="rustfs-ilm-validation")
    parser.add_argument("--debug-day-seconds", type=int, default=10)
    parser.add_argument("--process-seconds", type=int, default=1)
    parser.add_argument("--scanner-cycle-seconds", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--poll-interval", type=float, default=2)
    parser.add_argument("--request-timeout", type=float, default=10)
    parser.add_argument("--case-timeout", type=float, default=900)
    parser.add_argument("--cleanup-timeout", type=float, default=120)
    parser.add_argument("--setup-budget", type=float, default=40)
    parser.add_argument("--restart-timeout", type=float, default=120)
    parser.add_argument("--negative-window", type=float, default=20)
    parser.add_argument("--max-clock-skew", type=float, default=5)
    parser.add_argument("--rc-bin", default="rc")
    parser.add_argument(
        "--tier-endpoint", default=os.environ.get("RUSTFS_ILM_TIER_ENDPOINT")
    )
    parser.add_argument(
        "--tier-access-key", default=os.environ.get("RUSTFS_ILM_TIER_ACCESS_KEY")
    )
    parser.add_argument(
        "--tier-secret-key", default=os.environ.get("RUSTFS_ILM_TIER_SECRET_KEY")
    )
    parser.add_argument(
        "--restart-hook",
        help="JSON array of command tokens used to restart the fixed cluster",
    )
    parser.add_argument(
        "--case",
        action="append",
        choices=CASE_IDS,
        help="run only the named case; repeat to select multiple cases",
    )
    parser.add_argument(
        "--cleanup-on-failure",
        action="store_true",
        help="delete failed-case buckets instead of preserving them for diagnosis",
    )
    args = parser.parse_args()
    if not args.endpoint or not args.access_key or not args.secret_key:
        parser.error("endpoint, access key, and secret key are required")
    tier_values = (args.tier_endpoint, args.tier_access_key, args.tier_secret_key)
    if any(tier_values) and not all(tier_values):
        parser.error("tier endpoint, access key, and secret key must be provided together")
    parsed = urlparse(args.endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        parser.error("endpoint must be an absolute HTTP(S) URL")
    if (
        args.debug_day_seconds < 1
        or args.process_seconds < 1
        or args.scanner_cycle_seconds < 1
        or args.timeout <= 0
        or args.poll_interval <= 0
    ):
        parser.error("timing values must be positive")
    for name in ("timeout", "poll_interval", "request_timeout", "case_timeout",
                 "cleanup_timeout", "setup_budget", "restart_timeout", "negative_window", "max_clock_skew"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            parser.error(f"{name.replace('_', '-')} must be finite and positive")
    args.scanner_endpoint = list(
        dict.fromkeys(args.scanner_endpoint or [args.endpoint])
    )
    scanner_hosts: list[str] = []
    for endpoint in args.scanner_endpoint:
        scanner_parsed = urlparse(endpoint)
        if (
            scanner_parsed.scheme not in {"http", "https"}
            or not scanner_parsed.netloc
        ):
            parser.error("scanner endpoints must be absolute HTTP(S) URLs")
        scanner_hosts.append(scanner_parsed.hostname or "")
    no_proxy = os.environ.get("NO_PROXY", os.environ.get("no_proxy", ""))
    additions = [parsed.hostname or "", *scanner_hosts, "127.0.0.1", "localhost"]
    if args.tier_endpoint:
        tier_parsed = urlparse(args.tier_endpoint)
        if tier_parsed.scheme not in {"http", "https"} or not tier_parsed.netloc:
            parser.error("tier endpoint must be an absolute HTTP(S) URL")
        additions.append(tier_parsed.hostname or "")
    os.environ["NO_PROXY"] = ",".join(filter(None, [no_proxy, *additions]))
    os.environ["no_proxy"] = os.environ["NO_PROXY"]
    return args


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    harness = IlmHarness(args)
    try:
        return harness.run()
    finally:
        harness.close()


if __name__ == "__main__":
    raise SystemExit(main())
