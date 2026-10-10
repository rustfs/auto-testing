#!/usr/bin/env python3
"""Run destructive fixed-environment RustFS pool lifecycle validation."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from typing import Any, Callable

from case_plan import POOL_CASE_IDS as CASE_IDS, pool_harness_scored_cases
from pool_helpers import active_endpoint_indices, active_fixture_bytes
from pool_status import (
    PoolStatusError,
    assert_pool_parity,
    canonical_pool,
    decommission_active,
    decommission_active_or_completed,
    decommission_canceled,
    decommission_cleared,
    decommission_complete,
    decommission_entry,
    decommission_multipart_blocked,
    multipart_drain_failure_records,
    pool_entries,
    rc_failure_is_retryable,
    rebalance_active,
    rebalance_active_or_completed,
    rebalance_complete,
    rebalance_entries,
    rebalance_stopped,
    validate_pool_inventory,
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
        "boto3 is required for pool validation"
    ) from exc


ADMIN_PREFIX = "/rustfs/admin/v3"
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
TRANSIENT_S3_CODES = {"InternalError", "RequestTimeout", "ServiceUnavailable", "SlowDown"}


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, bytes):
        return {"bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
    return str(value)


def safe_name(value: str) -> str:
    return "".join(character if character.isalnum() or character in "-." else "-" for character in value)


def deterministic_payload(seed: str, size: int) -> bytes:
    block = hashlib.sha256(seed.encode("utf-8")).digest()
    return (block * ((size + len(block) - 1) // len(block)))[:size]


@dataclass
class CaseResult:
    id: str
    state: str
    message: str
    started_at: str
    finished_at: str
    duration_seconds: float
    evidence: list[str] = field(default_factory=list)


class EventRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event: str, **details: Any) -> None:
        item = {"timestamp": utc_now(), "event": event, "details": details}
        with self.lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(item, default=json_default, sort_keys=True) + "\n")


class PoolHarness:
    def __init__(self, args: argparse.Namespace) -> None:
        args.endpoint = args.endpoint.rstrip("/")
        args.node_endpoint = [endpoint.rstrip("/") for endpoint in args.node_endpoint]
        self.args = args
        self.evidence_dir = args.evidence_dir.resolve()
        self.case_dir = self.evidence_dir / "cases"
        self.case_dir.mkdir(parents=True, exist_ok=True)
        self.events = EventRecorder(self.evidence_dir / "events.jsonl")
        self.results: list[CaseResult] = []
        self.created_buckets: set[str] = set()
        self.current_layout = "initial"
        self.isolation_failure: dict[str, str] | None = None
        self.cleanup_outcome_value: dict[str, Any] = {
            "attempted": False,
            "state": "preserved",
            "message": "physical cleanup has not run",
        }
        self.rc_home = Path(tempfile.mkdtemp(prefix="rustfs-pool-rc-"))
        self.rc_env = os.environ.copy()
        self.rc_env["RC_CONFIG_DIR"] = str(self.rc_home)
        self.rc_bin = args.rc_bin
        self.rc_version = "unavailable"
        self.rc_binary_sha256 = "unavailable"
        self.expected_rc_binary_sha256 = os.environ.get(
            "RUSTFS_EXPECTED_RC_SHA256", ""
        ).lower()
        self.rc_alias = f"pool-{safe_name(args.run_id)[-16:]}"
        self.initial_endpoint = args.endpoint
        self.rc_configured = False
        self.secrets = {args.access_key, args.secret_key}
        self.client = self.s3_client(args.endpoint)
        self.node_clients = {
            endpoint: self.s3_client(endpoint) for endpoint in args.node_endpoint
        }

    def close(self) -> None:
        shutil.rmtree(self.rc_home, ignore_errors=True)

    def redact(self, value: str) -> str:
        result = value
        for secret in sorted((item for item in self.secrets if item), key=len, reverse=True):
            result = result.replace(secret, "[REDACTED]")
        return result

    def redact_document(self, value: Any) -> Any:
        serialized = json.dumps(value, default=json_default, sort_keys=True)
        return json.loads(self.redact(serialized))

    def evidence_path(self, case_id: str, name: str) -> Path:
        directory = self.case_dir / safe_name(case_id)
        directory.mkdir(parents=True, exist_ok=True)
        return directory / name

    def save_json(self, case_id: str, name: str, value: Any) -> str:
        path = self.evidence_path(case_id, name)
        text = json.dumps(value, default=json_default, indent=2, sort_keys=True) + "\n"
        path.write_text(self.redact(text), encoding="utf-8")
        return str(path.relative_to(self.evidence_dir))

    def save_text(self, case_id: str, name: str, value: str) -> str:
        path = self.evidence_path(case_id, name)
        path.write_text(self.redact(value), encoding="utf-8")
        return str(path.relative_to(self.evidence_dir))

    def s3_client(self, endpoint: str) -> Any:
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=self.args.access_key,
            aws_secret_access_key=self.args.secret_key,
            region_name=self.args.region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
                connect_timeout=5,
                read_timeout=60,
            ),
        )

    def configure_rc(self) -> None:
        requested = self.rc_bin
        try:
            self.rc_bin, self.rc_binary_sha256 = resolve_rc_binary(requested)
        except FileNotFoundError as exc:
            raise RuntimeError(f"required rc binary not found: {requested}") from exc
        version = subprocess.run(
            [self.rc_bin, "--version"],
            env=self.rc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if version.returncode != 0:
            raise RuntimeError(
                f"rc --version failed with exit {version.returncode}: {version.stderr.strip()}"
            )
        self.rc_version = self.redact((version.stdout or version.stderr).strip())
        if (
            self.expected_rc_binary_sha256
            and self.rc_binary_sha256.lower() != self.expected_rc_binary_sha256
        ):
            raise RuntimeError(
                "resolved rc binary SHA-256 differs from RUSTFS_EXPECTED_RC_SHA256"
            )
        self.set_rc_alias(self.args.endpoint)
        self.rc_configured = True
        self.events.record(
            "rc.client.identified",
            binaryPath=self.rc_bin,
            version=self.rc_version,
            binarySha256=self.rc_binary_sha256,
        )

    def set_rc_alias(self, endpoint: str) -> None:
        command = [
            self.rc_bin,
            "--json",
            "--no-color",
            "alias",
            "set",
            self.rc_alias,
            endpoint,
            self.args.access_key,
            self.args.secret_key,
            "--bucket-lookup",
            "path",
        ]
        result = subprocess.run(
            command,
            env=self.rc_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"rc alias setup failed: {self.redact(result.stderr.strip())}")
        self.events.record(
            "rc.alias.configured",
            alias=self.rc_alias,
            endpoint=endpoint,
        )

    def control_endpoint(self, layout: str) -> str:
        if self.initial_endpoint not in self.args.node_endpoint:
            return self.initial_endpoint
        active = active_endpoint_indices(len(self.args.node_endpoint), layout)
        active_endpoints = tuple(self.args.node_endpoint[index] for index in active)
        if self.initial_endpoint in active_endpoints:
            return self.initial_endpoint
        return active_endpoints[0]

    def retained_pool_endpoints(self) -> tuple[str, ...]:
        indices = active_endpoint_indices(len(self.args.node_endpoint), "retired")
        return tuple(self.args.node_endpoint[index] for index in indices)

    def retained_pool_client(self) -> Any:
        return self.node_clients[self.retained_pool_endpoints()[0]]

    def retire_decommissioned_pool(self, case_id: str) -> Any:
        if self.current_layout != "expanded":
            raise AssertionError("pool retirement requires the expanded topology")
        self.wait_decommission(case_id, 0, "complete", label="before-retirement")
        expanded, _ = self.pool_snapshot(
            case_id, "before-retired-config-removal", 2, require_active=False
        )
        pools = {item["id"]: item for item in validate_pool_inventory(expanded, 2, require_active=False)}
        if set(pools) != {0, 1}:
            raise AssertionError("pool retirement requires the original pool identities")
        if (pools[0]["status"].lower() != "decommissioned"
                or pools[0]["decommissionStatus"].lower() != "complete"
                or pools[1]["status"].lower() != "active"):
            raise AssertionError("pool retirement requires a completed source and an active retained pool")
        retained_cmdline = pools[1]["cmdline"]
        self.census(case_id, "before-retired-config-removal")
        self.apply_topology(case_id, "retired")
        remaining, _ = self.pool_snapshot(case_id, "after-retired-config-removal", 1)
        if canonical_pool(pool_entries(remaining)[0])["cmdline"] != retained_cmdline:
            raise AssertionError("retired topology does not contain exactly the retained pool")
        self.census(case_id, "after-retired-config-removal")
        return self.retained_pool_client()

    def select_control_endpoint(self, layout: str) -> None:
        endpoint = self.control_endpoint(layout)
        self.args.endpoint = endpoint
        self.client = self.s3_client(endpoint)
        if self.rc_configured:
            self.set_rc_alias(endpoint)

    def run_rc(
        self,
        case_id: str,
        name: str,
        arguments: list[str],
        *,
        expect_success: bool = True,
        expected_error_code: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if not expect_success and expected_error_code not in {
            "InvalidArgument", "InvalidRequest", "OperationAborted", "NotImplemented"
        }:
            raise AssertionError(f"rc {name} requires an expected server error for a negative test")
        if expect_success and expected_error_code is not None:
            raise AssertionError(f"rc {name} cannot expect both success and a server error")
        command = [self.rc_bin, "--json", "--no-color", *arguments]
        result = subprocess.run(
            command,
            env=self.rc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.save_json(
            case_id,
            f"{name}.json",
            {
                "command": [
                    "[REDACTED]" if token in self.secrets else token
                    for token in command
                ],
                "exitCode": result.returncode,
                "stdout": self.redact(result.stdout),
                "stderr": self.redact(result.stderr),
            },
        )
        if expect_success and result.returncode != 0:
            raise AssertionError(
                f"rc {name} exited {result.returncode}: {self.redact(result.stderr.strip())}"
            )
        if not expect_success and result.returncode == 0:
            raise AssertionError(f"rc {name} unexpectedly succeeded")
        if not expect_success:
            try:
                document = json.loads(result.stderr)
            except json.JSONDecodeError as exc:
                raise AssertionError(f"rc {name} did not return a structured server rejection") from exc
            details = document.get("details") if isinstance(document, dict) else None
            message = document.get("error") if isinstance(document, dict) else None
            if not isinstance(details, dict) or details.get("retryable") is not False or not isinstance(message, str):
                raise AssertionError(f"rc {name} did not return a non-retryable server rejection")
            xml_start = message.find("<Error>")
            if xml_start >= 0:
                error_xml = message[xml_start:]
            elif (
                expected_error_code == "NotImplemented"
                and arguments == ["admin", "rebalance", "start", self.rc_alias]
                and details.get("type") == "unsupported_feature"
                and "Unsupported feature:" in message
            ):
                # rc strips the XML code from NotImplemented. Confirm the
                # single-pool server guard directly, not merely CLI capability.
                status, headers, body = self.signed_request(
                    self.args.endpoint, "POST", f"{ADMIN_PREFIX}/rebalance/start"
                )
                error_xml = body.decode("utf-8", errors="replace")
                self.save_json(case_id, f"{name}-server-rejection.json", {
                    "status": status, "requestId": headers.get("x-amz-request-id"),
                    "body": self.redact(error_xml),
                })
                if status != 501:
                    raise AssertionError(f"rc {name} server proof returned HTTP {status}, expected 501")
            else:
                raise AssertionError(f"rc {name} did not retain its expected server error code")
            try:
                root = ET.fromstring(error_xml)
            except ET.ParseError as exc:
                raise AssertionError(f"rc {name} returned malformed server error XML") from exc
            codes = root.findall("Code") if root.tag == "Error" else []
            if len(codes) != 1 or codes[0].text != expected_error_code:
                observed = [element.text for element in codes]
                raise AssertionError(f"rc {name} returned server errors {observed!r}, expected {expected_error_code}")
        return result

    @staticmethod
    def parse_json_output(result: subprocess.CompletedProcess[str], operation: str) -> Any:
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise AssertionError(f"{operation} did not return JSON") from exc

    def assert_no_secrets(self, document: Any, label: str) -> None:
        serialized = json.dumps(document, default=json_default, sort_keys=True)
        exposed = [secret for secret in self.secrets if secret and secret in serialized]
        if exposed:
            raise AssertionError(f"{label} exposed configured credentials")

    def signed_request(
        self,
        endpoint: str,
        method: str,
        path: str,
        *,
        query: str = "",
        body: bytes | None = None,
        timeout: float = 60,
    ) -> tuple[int, dict[str, str], bytes]:
        url = endpoint.rstrip("/") + urllib.parse.quote(path, safe="/-._~")
        if query:
            url += f"?{query}"
        payload = body or b""
        headers = {"x-amz-content-sha256": hashlib.sha256(payload).hexdigest()}
        request = AWSRequest(method=method, url=url, data=body, headers=headers)
        SigV4Auth(
            Credentials(self.args.access_key, self.args.secret_key),
            "s3",
            self.args.region,
        ).add_auth(request)
        outgoing = urllib.request.Request(
            url,
            data=body,
            headers=dict(request.headers.items()),
            method=method,
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(outgoing, timeout=timeout) as response:
                return response.status, dict(response.headers.items()), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers.items()), exc.read()

    def admin_json(
        self,
        case_id: str,
        name: str,
        subpath: str,
        *,
        endpoint: str | None = None,
        query: str = "",
        expected_status: int = 200,
    ) -> Any:
        status, headers, body = self.signed_request(
            endpoint or self.args.endpoint,
            "GET",
            f"{ADMIN_PREFIX}/{subpath}",
            query=query,
        )
        try:
            parsed = json.loads(body) if body.strip() else {}
        except json.JSONDecodeError:
            parsed = {"raw": body.decode("utf-8", errors="replace")}
        self.save_json(
            case_id,
            f"{name}.json",
            {
                "status": status,
                "requestId": headers.get("x-amz-request-id"),
                "body": parsed,
            },
        )
        if status != expected_status:
            raise AssertionError(
                f"GET {subpath} returned HTTP {status}, expected {expected_status}: {parsed!r}"
            )
        return parsed

    def hook(
        self,
        case_id: str,
        name: str,
        raw: str | None,
        extra: tuple[str, ...] = (),
        *,
        timeout: float | None = None,
    ) -> str:
        if not raw:
            raise RuntimeError(f"{name} requires a driver hook")
        try:
            command = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{name} hook is not valid JSON") from exc
        if not isinstance(command, list) or not command or not all(
            isinstance(token, str) and token for token in command
        ):
            raise RuntimeError(f"{name} hook must be a JSON command array")
        result = subprocess.run(
            [*command, *extra],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=timeout,
        )
        reference = self.save_text(case_id, f"{name}.log", result.stdout)
        self.events.record(
            "driver.hook",
            case=case_id,
            hook=name,
            exitCode=result.returncode,
            arguments=list(extra),
        )
        if result.returncode != 0:
            raise RuntimeError(f"{name} hook exited {result.returncode}")
        return reference

    def reset_case_environment(self, case_id: str) -> None:
        self.hook(case_id, "physical-reset-initial", self.args.reset_hook)
        self.current_layout = "initial"
        self.select_control_endpoint("initial")
        self.wait_s3(case_id, self.client, "stable initial endpoint")

    def apply_topology(self, case_id: str, layout: str) -> None:
        self.hook(
            case_id,
            f"topology-{layout}",
            self.args.topology_hook,
            ("--layout", layout),
        )
        self.current_layout = layout
        self.select_control_endpoint(layout)
        self.wait_s3(case_id, self.client, f"stable {layout} endpoint")
        indices = active_endpoint_indices(len(self.args.node_endpoint), layout)
        for index in indices:
            endpoint = self.args.node_endpoint[index]
            client = self.node_clients[endpoint]
            self.wait_s3(case_id, client, f"node {index} {layout} endpoint")

    def restart_topology(self, case_id: str, layout: str) -> None:
        self.hook(
            case_id,
            f"restart-{layout}",
            self.args.restart_hook,
            ("--layout", layout),
        )
        self.current_layout = layout
        self.select_control_endpoint(layout)
        self.wait_s3(case_id, self.client, f"stable endpoint after {layout} restart")

    def census(self, case_id: str, phase: str) -> str:
        return self.hook(
            case_id,
            f"physical-census-{safe_name(phase)}",
            self.args.census_hook,
            ("--phase", phase),
        )

    def capture_diagnostics(self, case_id: str, phase: str) -> None:
        raw = getattr(self.args, "diagnostic_hook", None)
        if not raw:
            return
        errors = []
        for index, endpoint in enumerate(self.args.node_endpoint):
            try:
                self.admin_json(
                    case_id,
                    f"diagnostic-{safe_name(phase)}-node-{index}-decommission",
                    "decommission/status",
                    endpoint=endpoint,
                )
            except Exception as exc:
                errors.append(self.redact(f"node {index}: {type(exc).__name__}: {exc}"))
        try:
            self.hook(
                case_id,
                f"diagnostic-{safe_name(phase)}-metadata",
                raw,
                ("--phase", phase),
                timeout=60,
            )
        except Exception as exc:
            errors.append(self.redact(f"metadata: {type(exc).__name__}: {exc}"))
        self.save_json(
            case_id,
            f"diagnostic-{safe_name(phase)}-collection.json",
            {"phase": phase, "errors": errors, "complete": not errors},
        )

    def wait_canceled_quiescence(self, case_id: str, pool_id: int, *, label: str) -> None:
        stable_since: float | None = None
        prior: tuple[int, int] | None = None
        history: list[dict[str, Any]] = []

        def probe() -> tuple[bool, Any]:
            nonlocal stable_since, prior
            counters = []
            for endpoint in self.args.node_endpoint:
                try:
                    status, _, body = self.signed_request(
                        endpoint, "GET", f"{ADMIN_PREFIX}/decommission/status"
                    )
                except INFRA_EXCEPTIONS as exc:
                    # A gap in observations is not evidence of quiescence.
                    # Preserve wait_until's infrastructure classification.
                    stable_since = None
                    history.append({
                        "capturedAt": utc_now(), "endpoint": endpoint,
                        "error": self.redact(f"{type(exc).__name__}: {exc}"),
                    })
                    raise
                if status != 200:
                    stable_since = None
                    return False, {"endpoint": endpoint, "httpStatus": status}
                document = json.loads(body)
                if not decommission_canceled(document, pool_id):
                    stable_since = None
                    return False, document
                counters.append(self.decommission_counters(document, pool_id))
            now = time.monotonic()
            sample = {"capturedAt": utc_now(), "counters": counters}
            history.append(sample)
            if len(set(counters)) != 1:
                stable_since = None
                return False, sample
            current = counters[0]
            if current != prior or stable_since is None:
                prior, stable_since = current, now
            return now - stable_since >= 3.0, sample

        try:
            self.wait_until(
                case_id,
                f"decommission pool {pool_id} canceled and quiescent on all nodes",
                probe,
                timeout=min(self.args.timeout, 30),
            )
        finally:
            self.save_json(case_id, f"{safe_name(label)}-quiescence-samples.json", history)

    def wait_until(
        self,
        case_id: str,
        description: str,
        probe: Callable[[], tuple[bool, Any]],
        *,
        timeout: float | None = None,
    ) -> Any:
        deadline = time.monotonic() + (timeout or self.args.timeout)
        attempts = 0
        last: Any = None
        last_was_infrastructure = False
        while time.monotonic() < deadline:
            attempts += 1
            try:
                complete, last = probe()
                last_was_infrastructure = False
            except INFRA_EXCEPTIONS as exc:
                complete, last = False, {"error": f"{type(exc).__name__}: {exc}"}
                last_was_infrastructure = True
            if complete:
                self.events.record(
                    "poll.complete",
                    case=case_id,
                    description=description,
                    attempts=attempts,
                    detail=self.redact_document(last),
                )
                return last
            time.sleep(self.args.poll_interval)
        self.save_json(
            case_id,
            f"poll-timeout-{safe_name(description)}.json",
            {"description": description, "attempts": attempts, "last": last},
        )
        error_type = RuntimeError if last_was_infrastructure else AssertionError
        raise error_type(f"timed out waiting for {description}; last={last!r}")

    def wait_s3(self, case_id: str, client: Any, description: str) -> None:
        def probe() -> tuple[bool, Any]:
            try:
                response = client.list_buckets()
                return True, {
                    "requestId": response.get("ResponseMetadata", {}).get("RequestId")
                }
            except INFRA_EXCEPTIONS:
                raise
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code not in TRANSIENT_S3_CODES:
                    raise
                return False, {"error": f"ClientError[{code}]: {exc}"[:500]}

        self.wait_until(case_id, description, probe, timeout=min(self.args.timeout, 180))

    def bucket_name(self, case_id: str) -> str:
        digest = hashlib.sha256(f"{self.args.run_id}:{case_id}".encode()).hexdigest()[:14]
        return f"rv-pool-{digest}"

    def create_bucket(self, case_id: str) -> str:
        bucket = self.bucket_name(case_id)
        self.client.create_bucket(Bucket=bucket)
        self.created_buckets.add(bucket)
        self.events.record("bucket.created", case=case_id, bucket=bucket)
        return bucket

    @staticmethod
    def delete_batches(client: Any, bucket: str, items: list[dict[str, str]]) -> None:
        for offset in range(0, len(items), 1000):
            batch = items[offset : offset + 1000]
            try:
                response = client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": batch, "Quiet": True},
                )
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code not in {"MethodNotAllowed", "NotImplemented", "XNotImplemented"}:
                    raise
                # Some S3-compatible candidates do not implement multi-delete
                # for explicit version IDs. Individual deletes are the portable
                # cleanup fallback and retain the same isolation guarantee.
                for item in batch:
                    client.delete_object(Bucket=bucket, **item)
                continue
            if response.get("Errors"):
                raise RuntimeError(f"delete_objects failed: {response['Errors'][:5]!r}")

    def cleanup_bucket(self, bucket: str) -> None:
        deadline = time.monotonic() + min(self.args.timeout, 120)
        while True:
            try:
                uploads: list[dict[str, Any]] = []
                for page in self.client.get_paginator("list_multipart_uploads").paginate(Bucket=bucket):
                    uploads.extend(page.get("Uploads", []))
                for upload in uploads:
                    self.client.abort_multipart_upload(
                        Bucket=bucket,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )
                versions: list[dict[str, str]] = []
                for page in self.client.get_paginator("list_object_versions").paginate(Bucket=bucket):
                    versions.extend(
                        {"Key": item["Key"], "VersionId": item["VersionId"]}
                        for group in ("Versions", "DeleteMarkers")
                        for item in page.get(group, [])
                    )
                if versions:
                    self.delete_batches(self.client, bucket, versions)
                objects: list[dict[str, str]] = []
                for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects.extend({"Key": item["Key"]} for item in page.get("Contents", []))
                if objects:
                    self.delete_batches(self.client, bucket, objects)
                self.client.delete_bucket(Bucket=bucket)
                self.created_buckets.discard(bucket)
                self.events.record("bucket.cleanup.complete", bucket=bucket)
                return
            except INFRA_EXCEPTIONS as exc:
                if time.monotonic() >= deadline:
                    raise
                self.events.record(
                    "bucket.cleanup.retry",
                    bucket=bucket,
                    errorType=type(exc).__name__,
                )
                time.sleep(self.args.poll_interval)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code == "NoSuchBucket":
                    self.created_buckets.discard(bucket)
                    return
                retryable = code == "BucketNotEmpty" or code in TRANSIENT_S3_CODES
                if not retryable or time.monotonic() >= deadline:
                    raise
                self.events.record(
                    "bucket.cleanup.retry",
                    bucket=bucket,
                    errorCode=code,
                )
                time.sleep(self.args.poll_interval)

    def s3_retrying(
        self,
        operation: str,
        function: Callable[[], Any],
        *,
        timeout: float | None = None,
    ) -> Any:
        deadline = time.monotonic() + min(timeout or self.args.timeout, 120)
        attempt = 0
        while True:
            attempt += 1
            try:
                return function()
            except (*INFRA_EXCEPTIONS, ClientError) as exc:
                retryable = isinstance(exc, INFRA_EXCEPTIONS)
                error_code: str | None = None
                if isinstance(exc, ClientError):
                    error_code = exc.response.get("Error", {}).get("Code")
                    retryable = error_code in TRANSIENT_S3_CODES
                if not retryable or time.monotonic() >= deadline:
                    response_metadata = (
                        exc.response.get("ResponseMetadata", {})
                        if isinstance(exc, ClientError) else {}
                    )
                    self.events.record(
                        "s3.operation.exhausted" if retryable else "s3.operation.failed",
                        operation=operation,
                        attempt=attempt,
                        errorType=type(exc).__name__,
                        errorCode=error_code,
                        httpStatus=response_metadata.get("HTTPStatusCode"),
                        requestId=response_metadata.get("RequestId"),
                        sdkRetryAttempts=response_metadata.get("RetryAttempts"),
                    )
                    raise
                self.events.record(
                    "s3.operation.retry",
                    operation=operation,
                    attempt=attempt,
                    errorType=type(exc).__name__,
                    errorCode=error_code,
                )
                time.sleep(self.args.poll_interval)

    def put_retrying(
        self,
        client: Any,
        bucket: str,
        key: str,
        body: bytes,
        metadata: dict[str, str],
    ) -> dict[str, Any]:
        return self.s3_retrying(
            f"PUT s3://{bucket}/{key}",
            lambda: client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                Metadata=metadata,
            ),
        )

    def put_inventory(
        self,
        case_id: str,
        bucket: str,
        *,
        count: int | None = None,
        size: int | None = None,
        prefix: str = "baseline",
    ) -> list[dict[str, Any]]:
        count = count or self.args.fixture_objects
        size = size or self.args.fixture_bytes

        def put(index: int) -> dict[str, Any]:
            key = f"{prefix}/{index:05d}.bin"
            actual_size = size if index >= 3 else (0, 1, 4097)[index]
            body = deterministic_payload(f"{case_id}:{key}", actual_size)
            metadata = {"case": safe_name(case_id), "ordinal": str(index)}
            response = self.put_retrying(self.client, bucket, key, body, metadata)
            return {
                "key": key,
                "size": actual_size,
                "sha256": hashlib.sha256(body).hexdigest(),
                "etag": str(response.get("ETag", "")).strip('"'),
                "metadata": metadata,
                "versionId": response.get("VersionId"),
            }

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.workers) as pool:
            inventory = list(pool.map(put, range(count)))
        inventory.sort(key=lambda item: item["key"])
        self.save_json(case_id, f"{prefix}-manifest.json", inventory)
        self.events.record(
            "fixture.inventory.created",
            case=case_id,
            bucket=bucket,
            objects=len(inventory),
            bytes=sum(item["size"] for item in inventory),
        )
        return inventory

    def assert_inventory(
        self,
        case_id: str,
        client: Any,
        bucket: str,
        inventory: list[dict[str, Any]],
        label: str,
    ) -> None:
        def check(item: dict[str, Any]) -> dict[str, Any]:
            def fetch() -> tuple[dict[str, Any], bytes]:
                response = client.get_object(Bucket=bucket, Key=item["key"])
                return response, response["Body"].read()

            response, body = self.s3_retrying(
                f"GET integrity object for {label}", fetch
            )
            observed = {
                "key": item["key"],
                "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "etag": str(response.get("ETag", "")).strip('"'),
                "metadata": response.get("Metadata", {}),
                "versionId": response.get("VersionId"),
            }
            for field in ("size", "sha256", "etag", "metadata"):
                if observed[field] != item[field]:
                    raise AssertionError(
                        f"{label} {item['key']} {field} mismatch: expected "
                        f"{item[field]!r}, got {observed[field]!r}"
                    )
            if item.get("versionId") is not None and observed["versionId"] != item["versionId"]:
                raise AssertionError(
                    f"{label} {item['key']} version ID changed: "
                    f"{item['versionId']!r} -> {observed['versionId']!r}"
                )
            return observed

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.workers) as pool:
            observed = list(pool.map(check, inventory))

        def list_keys() -> set[str]:
            return {
                item["Key"]
                for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket)
                for item in page.get("Contents", [])
            }

        listed = self.s3_retrying(f"LIST integrity objects for {label}", list_keys)
        missing = sorted(item["key"] for item in inventory if item["key"] not in listed)
        if missing:
            raise AssertionError(f"{label} listing omitted keys: {missing[:10]!r}")
        self.save_json(case_id, f"integrity-{safe_name(label)}.json", observed)

    def pool_snapshot(
        self,
        case_id: str,
        label: str,
        expected_count: int,
        *,
        require_active: bool = True,
    ) -> tuple[Any, Any]:
        deadline = time.monotonic() + min(self.args.active_timeout, 10)
        attempt = 0
        while True:
            attempt += 1
            suffix = "" if require_active else f"-{attempt}"
            api = self.admin_json(
                case_id, f"{label}{suffix}-admin-pool-list", "pools/list"
            )
            rc_result = self.run_rc(
                case_id,
                f"{label}{suffix}-rc-pool-list",
                ["admin", "pool", "list", self.rc_alias],
            )
            rc = self.parse_json_output(rc_result, "rc admin pool list")
            self.assert_no_secrets(api, f"{label} Admin pool response")
            self.assert_no_secrets(rc, f"{label} rc pool response")
            try:
                assert_pool_parity(
                    rc, api, expected_count, require_active=require_active
                )
            except PoolStatusError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(self.args.poll_interval)
                continue
            return api, rc

    def assert_pool_specific_status(self, case_id: str, expected_count: int) -> None:
        api_list, _ = self.pool_snapshot(case_id, "specific-source", expected_count)
        initial = validate_pool_inventory(api_list, expected_count)
        stable_fields = (
            "id", "cmdline", "status", "decommissionStatus", "rebalanceStatus", "totalSize",
        )

        def stable_values(entry: dict[str, Any]) -> tuple[Any, ...]:
            return tuple(entry[field] for field in stable_fields)

        for expected in initial:
            pool_id = expected["id"]
            deadline = time.monotonic() + min(self.args.active_timeout, 10)
            attempt = 0
            while True:
                attempt += 1
                suffix = "" if attempt == 1 else f"-{attempt}"
                observed = []
                for selector, by_id, label in (
                    (str(pool_id), True, f"pool-{pool_id}-status"),
                    (expected["cmdline"], False, f"pool-{pool_id}-expression-status"),
                ):
                    query = urllib.parse.urlencode({"pool": selector, "by-id": str(by_id).lower()})
                    api = self.admin_json(
                        case_id, f"admin-{label}{suffix}", "pools/status", query=query,
                    )
                    self.assert_no_secrets(api, f"{label} Admin status")
                    observed.extend(validate_pool_inventory(api, 1))
                    arguments = ["admin", "pool", "status", self.rc_alias, selector]
                    if by_id:
                        arguments.append("--by-id")
                    rc_result = self.run_rc(case_id, f"rc-{label}{suffix}", arguments)
                    rc = self.parse_json_output(rc_result, "rc admin pool status")
                    self.assert_no_secrets(rc, f"{label} rc status")
                    observed.extend(validate_pool_inventory(rc, 1))

                if any(stable_values(item) != stable_values(expected) for item in observed):
                    raise AssertionError(
                        f"pool {pool_id} selector identity/state/total capacity disagrees with pool list: "
                        f"list={expected!r} observed={observed!r}"
                    )
                if all(item == expected for item in observed):
                    break

                # Disk usage can change between the sequential requests even
                # without client writes. Reacquire the list and all selectors;
                # never ignore capacity fields or accept a lasting mismatch.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"pool {pool_id} selector capacity snapshots did not converge: "
                        f"list={expected!r} observed={observed!r}"
                    )
                self.events.record(
                    "pool.selector.capacity.resample", case=case_id, poolId=pool_id,
                    attempt=attempt, expected=expected, observed=observed,
                )
                time.sleep(min(self.args.poll_interval, remaining))
                if time.monotonic() >= deadline:
                    raise AssertionError(f"pool {pool_id} selector capacity snapshots did not converge before deadline")
                refreshed_api = self.admin_json(
                    case_id, f"specific-source-pool-{pool_id}-resample-{attempt + 1}-admin-pool-list",
                    "pools/list",
                )
                self.assert_no_secrets(refreshed_api, f"pool {pool_id} refreshed Admin pool list")
                refreshed = validate_pool_inventory(refreshed_api, expected_count)
                if tuple(map(stable_values, initial)) != tuple(map(stable_values, refreshed)):
                    raise AssertionError("pool inventory identity/state/total capacity changed during selector sampling")
                expected = next(item for item in refreshed if item["id"] == pool_id)

    def start_rebalance(self, case_id: str, *, command: str = "rebalance") -> str:
        deadline = time.monotonic() + min(self.args.active_timeout, 60)
        attempt = 0
        while True:
            attempt += 1
            arguments = ["admin", command, "start", self.rc_alias]
            result = subprocess.run(
                [self.rc_bin, "--json", "--no-color", *arguments],
                env=self.rc_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.save_json(
                case_id,
                f"{command}-start-{attempt}.json",
                {
                    "command": arguments,
                    "exitCode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
            )
            if result.returncode == 0:
                document = self.parse_json_output(
                    result, f"rc admin {command} start"
                )
                operation_id = (
                    document.get("id") if isinstance(document, dict) else None
                )
                if not isinstance(operation_id, str) or not operation_id:
                    raise AssertionError(
                        f"{command} start omitted operation ID: {document!r}"
                    )
                return operation_id
            error_text = f"{result.stdout}\n{result.stderr}"
            if (
                rc_failure_is_retryable(result.stdout, result.stderr)
                and time.monotonic() < deadline
            ):
                time.sleep(self.args.poll_interval)
                continue
            raise AssertionError(
                f"{command} start failed: {self.redact(error_text.strip())}"
            )

    def rebalance_status(
        self,
        case_id: str,
        operation_id: str,
        label: str,
        *,
        allow_stopped: bool = False,
    ) -> Any:
        deadline = time.monotonic() + min(self.args.active_timeout, 10)
        attempt = 0
        last_states: tuple[Any, Any] | None = None
        while True:
            attempt += 1
            api = self.admin_json(
                case_id, f"{label}-{attempt}-admin", "rebalance/status"
            )
            rc_result = self.run_rc(
                case_id,
                f"{label}-{attempt}-rc",
                ["admin", "rebalance", "status", self.rc_alias],
            )
            rc = self.parse_json_output(rc_result, "rc admin rebalance status")
            self.assert_no_secrets(api, f"{label} Admin rebalance response")
            self.assert_no_secrets(rc, f"{label} rc rebalance response")
            api_pools = rebalance_entries(
                api,
                operation_id,
                allow_stopped=allow_stopped,
            )
            rc_pools = rebalance_entries(
                rc,
                operation_id,
                allow_stopped=allow_stopped,
            )
            api_states = tuple(
                (item.get("id"), item.get("status")) for item in api_pools
            )
            rc_states = tuple(
                (item.get("id"), item.get("status")) for item in rc_pools
            )
            if api_states == rc_states:
                return api
            last_states = (rc_states, api_states)
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"rc/Admin rebalance state did not converge: {last_states!r}"
                )
            time.sleep(self.args.poll_interval)

    def wait_rebalance(
        self,
        case_id: str,
        operation_id: str,
        state: str,
        *,
        label: str | None = None,
        allow_fast_completion: bool = False,
    ) -> Any:
        predicates: dict[str, Callable[[Any, str], bool]] = {
            "active": (
                rebalance_active_or_completed
                if allow_fast_completion
                else lambda document, expected: rebalance_active(
                    document, expected, require_progress=True
                )
            ),
            "complete": rebalance_complete,
            "stopped": rebalance_stopped,
        }
        artifact_label = label or state

        def probe() -> tuple[bool, Any]:
            status, _, body = self.signed_request(
                self.args.endpoint, "GET", f"{ADMIN_PREFIX}/rebalance/status"
            )
            if status != 200:
                return False, {"httpStatus": status, "body": body.decode(errors="replace")[:500]}
            document = json.loads(body)
            try:
                matched = predicates[state](document, operation_id)
            except PoolStatusError:
                self.save_json(
                    case_id,
                    f"rebalance-{artifact_label}-failed-status.json",
                    document,
                )
                raise
            return matched, document

        result = self.wait_until(
            case_id,
            f"rebalance {operation_id} {state}",
            probe,
            timeout=self.args.active_timeout if state == "active" else self.args.timeout,
        )
        self.save_json(case_id, f"rebalance-{artifact_label}-final.json", result)
        if (
            state == "active"
            and allow_fast_completion
            and rebalance_complete(result, operation_id)
        ):
            self.events.record(
                "rebalance.fast-completion",
                case=case_id,
                operationId=operation_id,
                evidence="terminal status carried nonzero movement progress",
            )
        self.rebalance_status(
            case_id,
            operation_id,
            f"rebalance-{artifact_label}-parity",
            allow_stopped=state == "stopped",
        )
        if state in {"complete", "stopped"}:
            inventory, _ = self.pool_snapshot(
                case_id,
                f"rebalance-{artifact_label}-pool-inventory",
                2,
            )
            observed = {
                str(item.get("rebalanceStatus", "")).lower()
                for item in pool_entries(inventory)
            }
            allowed = (
                {"completed", "none"}
                if state == "complete"
                else {"stopped", "completed", "none"}
            )
            required = "completed" if state == "complete" else "stopped"
            if not observed.issubset(allowed) or required not in observed:
                raise AssertionError(
                    f"pool inventory disagrees with rebalance {state}: {observed}"
                )
        return result

    def stop_rebalance(self, case_id: str, *, command: str = "rebalance") -> None:
        self.run_rc(
            case_id,
            f"{command}-stop",
            ["admin", command, "stop", self.rc_alias],
        )

    def start_decommission(self, case_id: str, pool_id: int = 0, *, label: str = "decommission-start") -> None:
        self.capture_diagnostics(case_id, f"before-{label}")
        deadline = time.monotonic() + min(self.args.active_timeout, 60)
        attempt = 0
        while True:
            attempt += 1
            command = [
                "admin",
                "decommission",
                "start",
                self.rc_alias,
                str(pool_id),
                "--by-id",
            ]
            result = subprocess.run(
                [self.rc_bin, "--json", "--no-color", *command],
                env=self.rc_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.save_json(
                case_id,
                f"{label}-{attempt}.json",
                {
                    "command": command,
                    "exitCode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
            )
            if result.returncode == 0:
                return
            error_text = f"{result.stdout}\n{result.stderr}"
            if (
                rc_failure_is_retryable(result.stdout, result.stderr)
                and time.monotonic() < deadline
            ):
                time.sleep(self.args.poll_interval)
                continue
            raise AssertionError(
                f"decommission start failed: {self.redact(error_text.strip())}"
            )

    def decommission_status(
        self, case_id: str, pool_id: int, label: str, *,
        predicate: Callable[[Any, int], bool] | None = None,
    ) -> Any:
        deadline = time.monotonic() + min(self.args.active_timeout, 10)
        attempt = 0
        fields = ("id", "cmdline", "status", "poolStatus")
        last_states: tuple[Any, Any] | None = None
        while True:
            attempt += 1
            api = self.admin_json(
                case_id, f"{label}-{attempt}-admin", "decommission/status"
            )
            rc_result = self.run_rc(
                case_id,
                f"{label}-{attempt}-rc",
                ["admin", "decommission", "status", self.rc_alias],
            )
            rc = self.parse_json_output(rc_result, "rc admin decommission status")
            self.assert_no_secrets(api, f"{label} Admin decommission response")
            self.assert_no_secrets(rc, f"{label} rc decommission response")
            api_entry = decommission_entry(api, pool_id)
            rc_entry = decommission_entry(rc, pool_id)
            api_state = tuple(api_entry.get(field) for field in fields)
            rc_state = tuple(rc_entry.get(field) for field in fields)
            if api_state == rc_state:
                if predicate is not None and not all(predicate(document, pool_id) for document in (api, rc)):
                    raise PoolStatusError("rc/Admin decommission details violate the expected state")
                return api
            last_states = (rc_state, api_state)
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"rc/Admin decommission state did not converge: {last_states!r}"
                )
            time.sleep(self.args.poll_interval)

    def wait_decommission(
        self,
        case_id: str,
        pool_id: int,
        state: str,
        *,
        label: str | None = None,
        allow_fast_completion: bool = False,
    ) -> Any:
        predicates: dict[str, Callable[[Any, int], bool]] = {
            "active": (
                decommission_active_or_completed
                if allow_fast_completion
                else lambda document, expected: decommission_active(
                    document, expected, require_progress=True
                )
            ),
            "complete": decommission_complete,
            "canceled": decommission_canceled,
            "cleared": decommission_cleared,
        }
        artifact_label = label or state

        def probe() -> tuple[bool, Any]:
            status, _, body = self.signed_request(
                self.args.endpoint, "GET", f"{ADMIN_PREFIX}/decommission/status"
            )
            if status != 200:
                return False, {"httpStatus": status, "body": body.decode(errors="replace")[:500]}
            document = json.loads(body)
            try:
                matched = predicates[state](document, pool_id)
            except PoolStatusError:
                self.save_json(
                    case_id,
                    f"decommission-{artifact_label}-failed-status.json",
                    document,
                )
                raise
            return matched, document

        result = self.wait_until(
            case_id,
            f"decommission pool {pool_id} {state}",
            probe,
            timeout=self.args.active_timeout if state == "active" else self.args.timeout,
        )
        self.save_json(
            case_id, f"decommission-{artifact_label}-final.json", result
        )
        if (
            state == "active"
            and allow_fast_completion
            and decommission_complete(result, pool_id)
        ):
            self.events.record(
                "decommission.fast-completion",
                case=case_id,
                poolId=pool_id,
                evidence="terminal status carried nonzero movement progress",
            )
        self.decommission_status(
            case_id, pool_id, f"decommission-{artifact_label}-parity"
        )
        if state == "canceled":
            self.wait_canceled_quiescence(case_id, pool_id, label=artifact_label)
            result = self.decommission_status(
                case_id, pool_id, f"decommission-{artifact_label}-quiescent"
            )
            if not decommission_canceled(result, pool_id):
                raise AssertionError("decommission left canceled state after quiescence")
            self.save_json(case_id, f"decommission-{artifact_label}-final.json", result)
        if state == "active":
            self.capture_diagnostics(case_id, f"decommission-{artifact_label}")
        if state in {"complete", "canceled", "cleared"}:
            expected_count = len(pool_entries(result))
            require_active = state == "cleared"
            inventory, _ = self.pool_snapshot(
                case_id,
                f"decommission-{artifact_label}-pool-inventory",
                expected_count,
                require_active=require_active,
            )
            matches = [
                item for item in pool_entries(inventory) if item.get("id") == pool_id
            ]
            if len(matches) != 1:
                raise AssertionError(
                    f"pool inventory omitted decommission pool {pool_id}"
                )
            entry = matches[0]
            expected_states = {
                "complete": ("decommissioned", "complete", True),
                "canceled": ("blocked", "canceled", True),
                "cleared": ("active", "none", False),
            }
            pool_state, operation_state, has_info = expected_states[state]
            if (
                str(entry.get("status", "")).lower() != pool_state
                or str(entry.get("decommissionStatus", "")).lower()
                != operation_state
                or (entry.get("decommissionInfo") is not None) is not has_info
            ):
                raise AssertionError(
                    f"pool inventory disagrees with decommission {state}: {entry!r}"
                )
        return result

    def cancel_decommission(self, case_id: str, pool_id: int = 0) -> None:
        self.run_rc(
            case_id,
            "decommission-cancel",
            ["admin", "decommission", "cancel", self.rc_alias, str(pool_id), "--by-id"],
        )

    def clear_decommission(self, case_id: str, pool_id: int = 0) -> None:
        self.run_rc(
            case_id,
            "decommission-clear",
            ["admin", "decommission", "clear", self.rc_alias, str(pool_id), "--by-id"],
        )

    def seed_and_expand(
        self,
        case_id: str,
        *,
        count: int | None = None,
        size: int | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        bucket = self.create_bucket(case_id)
        inventory = self.put_inventory(case_id, bucket, count=count, size=size)
        self.assert_inventory(case_id, self.client, bucket, inventory, "before-expansion")
        self.census(case_id, "before-expansion")
        self.apply_topology(case_id, "expanded")
        self.pool_snapshot(case_id, "after-expansion", 2)
        self.assert_inventory(
            case_id,
            self.retained_pool_client(),
            bucket,
            inventory,
            "after-expansion-pool1-node0",
        )
        return bucket, inventory

    def case_status(self, case_id: str) -> None:
        bucket = self.create_bucket(case_id)
        body = deterministic_payload(case_id, 8193)
        self.client.put_object(Bucket=bucket, Key="status-probe.bin", Body=body)
        initial, _ = self.pool_snapshot(case_id, "initial", 1)
        self.assert_pool_specific_status(case_id, 1)
        self.census(case_id, "initial")
        self.apply_topology(case_id, "expanded")
        expanded, _ = self.pool_snapshot(case_id, "expanded", 2)
        self.assert_pool_specific_status(case_id, 2)
        self.census(case_id, "expanded")
        if pool_entries(initial)[0]["cmdline"] == pool_entries(expanded)[1]["cmdline"]:
            raise AssertionError("expansion pool reused the seed pool command line")
        initial_identity = canonical_pool(pool_entries(initial)[0])
        expanded_seed = canonical_pool(pool_entries(expanded)[0])
        if (
            initial_identity["id"],
            initial_identity["cmdline"],
        ) != (
            expanded_seed["id"],
            expanded_seed["cmdline"],
        ):
            raise AssertionError(
                "seed pool identity or ordering changed during expansion: "
                f"{initial_identity!r} -> {expanded_seed!r}"
            )
        for endpoint in self.retained_pool_endpoints():
            def read_probe() -> bytes:
                response = self.node_clients[endpoint].get_object(
                    Bucket=bucket, Key="status-probe.bin"
                )
                return response["Body"].read()

            observed = self.s3_retrying(
                f"GET status probe through {endpoint}", read_probe
            )
            if observed != body:
                raise AssertionError(f"status probe changed through {endpoint}")

    def case_expand_and_rebalance(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(case_id)
        operation_id = self.start_rebalance(case_id, command="expand")
        self.wait_rebalance(
            case_id,
            operation_id,
            "active",
            allow_fast_completion=True,
        )
        self.wait_rebalance(case_id, operation_id, "complete")
        self.census(case_id, "after-rebalance")
        self.assert_inventory(case_id, self.client, bucket, inventory, "after-rebalance")

    def case_expansion_restart_persistence(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(
            case_id, size=active_fixture_bytes(self.args.fixture_bytes)
        )
        before, _ = self.pool_snapshot(case_id, "before-restart", 2)
        operation_id = self.start_rebalance(case_id)
        active = self.wait_rebalance(case_id, operation_id, "active")
        active_counters = self.rebalance_counters(active)
        self.restart_topology(case_id, "expanded")
        after, _ = self.pool_snapshot(
            case_id, "after-restart", 2, require_active=False
        )
        unexpected_states = {
            str(item.get("status", "")).lower()
            for item in pool_entries(after)
        }.difference({"active", "rebalancing"})
        if unexpected_states:
            raise AssertionError(
                f"unexpected pool states after rebalance restart: {unexpected_states}"
            )
        before_identity = tuple((item["id"], item["cmdline"]) for item in pool_entries(before))
        after_identity = tuple((item["id"], item["cmdline"]) for item in pool_entries(after))
        if before_identity != after_identity:
            raise AssertionError(
                f"pool identity changed across restart: {before_identity!r} -> {after_identity!r}"
            )
        completed = self.wait_rebalance(case_id, operation_id, "complete")
        self.pool_snapshot(case_id, "after-rebalance-restart-complete", 2)
        completed_counters = self.rebalance_counters(completed)
        if any(
            final < initial
            for initial, final in zip(
                active_counters, completed_counters, strict=True
            )
        ):
            raise AssertionError(
                "rebalance counters regressed across restart: "
                f"{active_counters} -> {completed_counters}"
            )
        for index in range(len(self.args.node_endpoint)):
            self.assert_inventory(
                case_id,
                self.node_clients[self.args.node_endpoint[index]],
                bucket,
                inventory,
                f"restart-node-{index}",
            )

    @staticmethod
    def rebalance_counters(document: Any) -> tuple[int, int, int]:
        counters = [0, 0, 0]
        for pool in document.get("pools", []):
            progress = pool.get("progress") or {}
            for index, field in enumerate(("objects", "versions", "bytes")):
                counters[index] += int(progress.get(field, 0))
        return tuple(counters)

    def case_rebalance_stop_resume(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(
            case_id, size=active_fixture_bytes(self.args.fixture_bytes)
        )
        first_id = self.start_rebalance(case_id)
        self.wait_rebalance(case_id, first_id, "active")
        self.stop_rebalance(case_id)
        self.wait_rebalance(
            case_id,
            first_id,
            "stopped",
            label="stopped-before-restart",
        )
        self.assert_inventory(case_id, self.client, bucket, inventory, "after-rebalance-stop")
        self.restart_topology(case_id, "expanded")
        self.wait_rebalance(
            case_id,
            first_id,
            "stopped",
            label="stopped-after-restart",
        )
        second_id = self.start_rebalance(case_id)
        if second_id == first_id:
            raise AssertionError("rebalance resume reused the stopped operation ID")
        self.wait_rebalance(case_id, second_id, "complete")
        self.assert_inventory(case_id, self.client, bucket, inventory, "after-rebalance-resume")

    def exercise_live_io(
        self,
        case_id: str,
        bucket: str,
        *,
        prefix: str,
        client: Any | None = None,
    ) -> list[dict[str, Any]]:
        client = client or self.client

        def live_put(index: int) -> dict[str, Any]:
            key = f"{prefix}/put-{index:02d}.bin"
            body = deterministic_payload(f"{case_id}:{key}", 64 * 1024)
            response = self.put_retrying(client, bucket, key, body, {"phase": prefix})
            return {
                "key": key,
                "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "etag": str(response.get("ETag", "")).strip('"'),
                "metadata": {"phase": prefix},
                "versionId": response.get("VersionId"),
            }

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.args.workers
        ) as pool:
            committed = list(pool.map(live_put, range(self.args.live_objects)))
        committed.sort(key=lambda item: item["key"])
        first = committed[0]
        head = self.s3_retrying(
            f"HEAD live object for {prefix}",
            lambda: client.head_object(Bucket=bucket, Key=first["key"]),
        )
        if head.get("ContentLength") != first["size"]:
            raise AssertionError("HEAD returned the wrong live-object length")

        def read_live_object() -> bytes:
            response = client.get_object(Bucket=bucket, Key=first["key"])
            return response["Body"].read()

        got = self.s3_retrying(f"GET live object for {prefix}", read_live_object)
        if hashlib.sha256(got).hexdigest() != first["sha256"]:
            raise AssertionError("GET returned corrupt live-object bytes")
        listed = self.s3_retrying(
            f"LIST live objects for {prefix}",
            lambda: client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/"),
        ).get("Contents", [])
        if not any(item.get("Key") == first["key"] for item in listed):
            raise AssertionError("LIST omitted a committed live object")
        copy_key = f"{prefix}/copy.bin"
        copy_response = self.s3_retrying(
            f"COPY live object for {prefix}",
            lambda: client.copy_object(
                Bucket=bucket,
                Key=copy_key,
                CopySource={"Bucket": bucket, "Key": first["key"]},
                Metadata={"phase": prefix},
                MetadataDirective="REPLACE",
            ),
        )
        committed.append(
            {
                "key": copy_key,
                "size": first["size"],
                "sha256": first["sha256"],
                "etag": str(copy_response.get("CopyObjectResult", {}).get("ETag", "")).strip('"'),
                "metadata": {"phase": prefix},
                "versionId": copy_response.get("VersionId"),
            }
        )
        delete_key = f"{prefix}/delete-control.bin"
        self.s3_retrying(
            f"PUT delete control for {prefix}",
            lambda: client.put_object(Bucket=bucket, Key=delete_key, Body=b"delete-me"),
        )
        self.s3_retrying(
            f"DELETE control for {prefix}",
            lambda: client.delete_object(Bucket=bucket, Key=delete_key),
        )
        try:
            self.s3_retrying(
                f"HEAD deleted control for {prefix}",
                lambda: client.head_object(Bucket=bucket, Key=delete_key),
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {
                "404",
                "NoSuchKey",
                "NotFound",
            }:
                raise
        else:
            raise AssertionError("DELETE control object remains visible")
        self.save_json(case_id, f"{prefix}-live-manifest.json", committed)
        return committed

    def case_rebalance_live_io(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(
            case_id, size=active_fixture_bytes(self.args.fixture_bytes)
        )
        operation_id = self.start_rebalance(case_id)
        self.wait_rebalance(case_id, operation_id, "active")
        live = self.exercise_live_io(case_id, bucket, prefix="during-rebalance")
        status, _, body = self.signed_request(
            self.args.endpoint, "GET", f"{ADMIN_PREFIX}/rebalance/status"
        )
        if status != 200 or not rebalance_active(json.loads(body), operation_id, require_progress=True):
            raise AssertionError("rebalance did not remain active across the live-I/O window")
        self.wait_rebalance(case_id, operation_id, "complete")
        self.assert_inventory(case_id, self.client, bucket, inventory + live, "rebalance-live-final")

    def case_decommission(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(case_id)
        self.start_decommission(case_id)
        self.wait_decommission(
            case_id, 0, "active", allow_fast_completion=True
        )
        self.wait_decommission(case_id, 0, "complete")
        self.census(case_id, "after-decommission")
        self.assert_inventory(
            case_id,
            self.retire_decommissioned_pool(case_id),
            bucket,
            inventory,
            "decommission-survivor",
        )

    def case_decommission_cancel_clear_resume(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(
            case_id, size=active_fixture_bytes(self.args.fixture_bytes)
        )
        self.start_decommission(case_id)
        self.wait_decommission(case_id, 0, "active")
        self.cancel_decommission(case_id)
        canceled_before = self.wait_decommission(
            case_id,
            0,
            "canceled",
            label="canceled-before-restart",
        )
        self.assert_inventory(case_id, self.client, bucket, inventory, "after-decommission-cancel")
        self.restart_topology(case_id, "expanded")
        canceled_after = self.wait_decommission(
            case_id,
            0,
            "canceled",
            label="canceled-after-restart",
        )
        if self.decommission_counters(canceled_before, 0) != self.decommission_counters(canceled_after, 0):
            raise AssertionError("canceled decommission progress changed across restart")
        self.clear_decommission(case_id)
        self.wait_decommission(
            case_id,
            0,
            "cleared",
            label="cleared-before-restart",
        )
        self.restart_topology(case_id, "expanded")
        self.wait_decommission(
            case_id,
            0,
            "cleared",
            label="cleared-after-restart",
        )
        self.start_decommission(case_id)
        self.wait_decommission(case_id, 0, "complete")
        self.assert_inventory(
            case_id, self.retire_decommissioned_pool(case_id), bucket, inventory, "after-decommission-resume"
        )

    @staticmethod
    def decommission_counters(document: Any, pool_id: int) -> tuple[int, int]:
        info = decommission_entry(document, pool_id).get("decommissionInfo", {})
        return int(info.get("objectsDecommissioned", 0)), int(info.get("bytesDecommissioned", 0))

    def case_decommission_restart_recovery(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(
            case_id, size=active_fixture_bytes(self.args.fixture_bytes)
        )
        self.start_decommission(case_id)
        before = self.wait_decommission(case_id, 0, "active")
        before_counters = self.decommission_counters(before, 0)
        self.restart_topology(case_id, "expanded")
        final = self.wait_decommission(case_id, 0, "complete")
        final_counters = self.decommission_counters(final, 0)
        if any(after < prior for prior, after in zip(before_counters, final_counters, strict=True)):
            raise AssertionError(
                f"decommission counters regressed across restart: {before_counters} -> {final_counters}"
            )
        self.assert_inventory(
            case_id,
            self.retire_decommissioned_pool(case_id),
            bucket,
            inventory,
            "decommission-restart-final",
        )

    def assert_expanded_delete_marker_removal(self, case_id: str, bucket: str) -> None:
        if self.current_layout != "expanded":
            raise AssertionError("delete-marker API probes require the expanded topology")
        for operation in ("single", "batch"):
            key = f"delete-api-probe/{operation}.bin"
            payload = b"historical data behind a delete marker"
            original = self.client.put_object(Bucket=bucket, Key=key, Body=payload)
            marker = self.client.delete_object(Bucket=bucket, Key=key)
            marker_id = marker.get("VersionId")
            if not original.get("VersionId") or not marker_id or marker.get("DeleteMarker") is not True:
                raise AssertionError("delete-marker API probe did not create versioned history")
            self.save_json(case_id, f"delete-api-{operation}-fixture.json", {"original": original, "marker": marker})
            for method in (self.client.get_object, self.client.head_object):
                try:
                    method(Bucket=bucket, Key=key, VersionId=marker_id)
                except ClientError as exc:
                    if (exc.response.get("Error", {}).get("Code") not in {"405", "MethodNotAllowed"}
                            or exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 405):
                        raise
                else:
                    raise AssertionError("GET/HEAD of an explicit delete marker must remain 405")
            if operation == "single":
                response = self.client.delete_object(Bucket=bucket, Key=key, VersionId=marker_id)
                self.save_json(case_id, "delete-api-single-response.json", response)
                if response.get("VersionId") != marker_id or response.get("DeleteMarker") is not True:
                    raise AssertionError("single delete did not acknowledge the exact marker version")
            else:
                response = self.client.delete_objects(
                    Bucket=bucket, Delete={"Objects": [{"Key": key, "VersionId": marker_id}], "Quiet": False}
                )
                self.save_json(case_id, "delete-api-batch-response.json", response)
                deleted = response.get("Deleted", [])
                if (response.get("Errors") or len(deleted) != 1 or deleted[0].get("Key") != key
                        or deleted[0].get("VersionId") != marker_id or deleted[0].get("DeleteMarker") is not True):
                    raise AssertionError("batch delete did not acknowledge the exact marker version")
            versions = self.client.list_object_versions(Bucket=bucket, Prefix=key)
            self.save_json(case_id, f"delete-api-{operation}-remaining.json", versions)
            if any(item.get("Key") == key for item in versions.get("DeleteMarkers", [])):
                raise AssertionError("marker version remains after successful delete acknowledgement")
            remaining = [item for item in versions.get("Versions", []) if item.get("Key") == key]
            if (len(remaining) != 1 or remaining[0].get("VersionId") != original["VersionId"]
                    or remaining[0].get("IsLatest") is not True):
                raise AssertionError("marker removal changed the historical version identity")
            if self.client.get_object(Bucket=bucket, Key=key)["Body"].read() != payload:
                raise AssertionError("marker removal damaged the historical payload")

    def case_decommission_versioned_integrity(self, case_id: str) -> None:
        bucket = self.create_bucket(case_id)
        self.client.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        key = "history/versioned.bin"
        payloads = [
            deterministic_payload(f"{case_id}:v1", 37 * 1024),
            deterministic_payload(f"{case_id}:v2", 71 * 1024),
        ]
        version_ids: list[str] = []
        for payload in payloads:
            response = self.client.put_object(Bucket=bucket, Key=key, Body=payload)
            version_id = response.get("VersionId")
            if not isinstance(version_id, str) or not version_id:
                raise AssertionError("versioned PUT omitted VersionId")
            version_ids.append(version_id)
        deleted = self.client.delete_object(Bucket=bucket, Key=key)
        marker_id = deleted.get("VersionId")
        if deleted.get("DeleteMarker") is not True or not marker_id:
            raise AssertionError("versioned DELETE did not create a delete marker")
        self.save_json(case_id, "version-fixture.json", {
            "versionIds": version_ids, "deleteMarkerVersionId": marker_id,
            "payloads": [{"size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()} for payload in payloads],
        })
        inventory = self.put_inventory(case_id, bucket, count=max(32, self.args.fixture_objects // 2))
        self.apply_topology(case_id, "expanded")
        self.assert_expanded_delete_marker_removal(case_id, bucket)
        self.start_decommission(case_id)
        self.wait_decommission(case_id, 0, "complete")
        checker = self.retire_decommissioned_pool(case_id)
        versions = checker.list_object_versions(Bucket=bucket, Prefix=key)
        self.save_json(
            case_id, "version-timeline.json",
            {"versionIds": version_ids, "deleteMarkerVersionId": marker_id, "observed": versions},
        )
        observed_versions = {item["VersionId"] for item in versions.get("Versions", []) if item.get("Key") == key}
        observed_markers = {
            item["VersionId"]
            for item in versions.get("DeleteMarkers", [])
            if item.get("Key") == key
        }
        latest_markers = {
            item["VersionId"]
            for item in versions.get("DeleteMarkers", [])
            if item.get("Key") == key and item.get("IsLatest") is True
        }
        if (
            set(version_ids) != observed_versions
            or observed_markers != {marker_id}
            or latest_markers != {marker_id}
        ):
            raise AssertionError(
                "version timeline changed: "
                f"versions={observed_versions}, markers={observed_markers}, "
                f"latestMarkers={latest_markers}"
            )
        for version_id, expected in zip(version_ids, payloads, strict=True):
            body = checker.get_object(
                Bucket=bucket, Key=key, VersionId=version_id
            )["Body"].read()
            if body != expected:
                raise AssertionError(f"historical version {version_id} changed")
        marker_modified = next(
            item.get("LastModified") for item in versions["DeleteMarkers"]
            if item.get("Key") == key and item.get("VersionId") == marker_id
        )
        self.assert_delete_marker_reads(case_id, checker, bucket, key, marker_id, marker_modified)
        self.assert_inventory(case_id, checker, bucket, inventory, "versioned-decommission-final")

    def assert_delete_marker_reads(
        self, case_id: str, checker: Any, bucket: str, key: str,
        marker_id: str, marker_modified: datetime | None,
    ) -> None:
        for operation, explicit, filename in (
            ("get_object", False, "latest-delete-marker-response.json"),
            ("head_object", False, "latest-delete-marker-head-response.json"),
            ("get_object", True, "explicit-delete-marker-get-response.json"),
            ("head_object", True, "explicit-delete-marker-head-response.json"),
        ):
            request = {"Bucket": bucket, "Key": key}
            if explicit:
                request["VersionId"] = marker_id
            try:
                response = getattr(checker, operation)(**request)
            except ClientError as exc:
                self.save_json(case_id, filename, exc.response)
                expected_status = 405 if explicit else 404
                expected_codes = {"405", "MethodNotAllowed"} if explicit else {"404", "NoSuchKey", "NotFound"}
                if exc.response.get("Error", {}).get("Code") not in expected_codes:
                    raise
                metadata = exc.response.get("ResponseMetadata", {})
                if metadata.get("HTTPStatusCode") != expected_status:
                    raise AssertionError(f"{operation} delete marker returned the wrong HTTP status")
                headers = metadata.get("HTTPHeaders", {})
                if str(headers.get("x-amz-delete-marker", "")).lower() != "true":
                    raise AssertionError(
                        f"{'explicit' if explicit else 'latest'} version returned {expected_status} "
                        "without delete-marker identity"
                    )
                if headers.get("x-amz-version-id") != marker_id:
                    raise AssertionError("delete-marker response returned the wrong VersionId")
                if explicit:
                    if not isinstance(marker_modified, datetime) or marker_modified.utcoffset() is None:
                        raise AssertionError("version listing omitted the marker LastModified timestamp")
                    expected_date = format_datetime(marker_modified.astimezone(UTC).replace(microsecond=0), usegmt=True)
                    if headers.get("last-modified") != expected_date:
                        raise AssertionError("explicit delete-marker response returned the wrong Last-Modified")
            else:
                body = response.get("Body")
                if body is not None:
                    body.close()
                raise AssertionError("latest or explicit delete marker no longer hides the versioned object")

    def multipart_request(
        self, case_id: str, label: str, client: Any, operation: str, **params: Any,
    ) -> Any:
        request = dict(params)
        if "Body" in request:
            body = request.pop("Body")
            request.update(bodyBytes=len(body), bodySha256=hashlib.sha256(body).hexdigest())
        evidence = {"operation": operation, "request": request, "startedAt": utc_now()}
        try:
            response = getattr(client, operation)(**params)
        except Exception as exc:
            evidence.update(error=f"{type(exc).__name__}: {exc}", response=getattr(exc, "response", None))
            self.save_json(case_id, f"mpu-{label}.json", evidence)
            raise
        evidence["response"] = {key: value for key, value in response.items() if key != "Body"}
        try:
            self.save_json(case_id, f"mpu-{label}.json", evidence)
        except Exception:
            if "Body" in response:
                response["Body"].close()
            raise
        return response

    def multipart_entries(
        self, case_id: str, label: str, client: Any, operation: str, **params: Any,
    ) -> list[dict[str, Any]]:
        field = "Uploads" if operation == "list_multipart_uploads" else "Parts"
        entries: list[dict[str, Any]] = []
        identities: set[Any] = set()
        deadline = time.monotonic() + self.args.active_timeout
        last_truncated: bool | None = None
        try:
            pages = client.get_paginator(operation).paginate(**params, PaginationConfig={"PageSize": 1})
            for index, page in enumerate(pages, 1):
                self.save_json(case_id, f"mpu-{label}-page-{index}.json", {"request": params, "response": page})
                if index > 64 or time.monotonic() >= deadline:
                    raise AssertionError("MPU fixture pagination exceeded its bounded budget")
                if (not isinstance(page, dict) or type(page.get("IsTruncated")) is not bool
                        or not isinstance(page.get(field, []), list)):
                    raise AssertionError("malformed MPU listing page")
                last_truncated = page["IsTruncated"]
                for name in ("Bucket", "Key", "UploadId"):
                    if name in params and name in page and page[name] != params[name]:
                        raise AssertionError("MPU listing response has the wrong identity")
                for item in page.get(field, []):
                    if not isinstance(item, dict):
                        raise AssertionError("malformed MPU listing entry")
                    if field == "Uploads":
                        identity = (item.get("Key"), item.get("UploadId"))
                        valid = all(isinstance(value, str) and value for value in identity)
                    else:
                        identity = item.get("PartNumber")
                        valid = (
                            type(identity) is int and 1 <= identity <= 10000
                            and type(item.get("Size")) is int and item["Size"] >= 0
                            and isinstance(item.get("ETag"), str) and bool(item["ETag"])
                        )
                    if not valid or identity in identities:
                        raise AssertionError("invalid or duplicate MPU listing identity")
                    identities.add(identity)
                    entries.append(item)
        except Exception as exc:
            self.save_json(case_id, f"mpu-{label}-error.json", {
                "operation": operation, "request": params, "error": f"{type(exc).__name__}: {exc}",
                "response": getattr(exc, "response", None),
            })
            raise
        if last_truncated is None:
            raise AssertionError("MPU paginator returned no response")
        if last_truncated:
            raise AssertionError("MPU paginator stopped before the final page")
        return entries

    def assert_multipart_uploads(
        self, case_id: str, bucket: str, uploads: list[dict[str, Any]], label: str,
    ) -> None:
        expected = {(item["key"], item["uploadId"]) for item in uploads}
        for index, endpoint in enumerate(self.args.node_endpoint):
            client = self.node_clients[endpoint]
            prefix = f"{label}-node-{index}"
            listed = self.multipart_entries(case_id, prefix, client, "list_multipart_uploads", Bucket=bucket)
            if {(item["Key"], item["UploadId"]) for item in listed} != expected:
                raise AssertionError("pending MPU inventory changed or contains an unexpected upload")
            for upload in uploads:
                parts = self.multipart_entries(
                    case_id, f"{prefix}-{safe_name(upload['key'])}-parts", client, "list_parts",
                    Bucket=bucket, Key=upload["key"], UploadId=upload["uploadId"],
                )
                fields = ("PartNumber", "ETag", "Size")
                observed_parts = [tuple(item[name] for name in fields) for item in parts]
                expected_parts = [tuple(item[name] for name in fields) for item in upload["parts"]]
                if observed_parts != expected_parts:
                    raise AssertionError("pending MPU part number, ETag or length changed")

    def wait_multipart_drain_blocked(self, case_id: str, bucket: str, since: str) -> None:
        attempt = 0

        def probe() -> tuple[bool, Any]:
            nonlocal attempt
            attempt += 1
            status, _, body = self.signed_request(self.args.endpoint, "GET", f"{ADMIN_PREFIX}/decommission/status")
            if status != 200:
                raise PoolStatusError(f"MPU drain status returned HTTP {status}")
            document = json.loads(body)
            try:
                return decommission_multipart_blocked(document, 0), document
            finally:
                self.save_json(case_id, f"mpu-drain-status-{attempt}.json", document)
        result = self.wait_until(case_id, "owned MPU to block final decommission", probe)
        self.save_json(case_id, "mpu-drain-blocked.json", result)
        node_attempt = 0

        def nodes_probe() -> tuple[bool, Any]:
            nonlocal node_attempt
            node_attempt += 1
            documents = [
                self.admin_json(case_id, f"mpu-drain-blocked-{node_attempt}-node-{index}", "decommission/status", endpoint=endpoint)
                for index, endpoint in enumerate(self.args.node_endpoint)
            ]
            matched = [decommission_multipart_blocked(document, 0) for document in documents]
            return all(matched), documents

        self.wait_until(case_id, "MPU drain failure on every node", nodes_probe, timeout=min(self.args.active_timeout, 10))
        parity = self.decommission_status(case_id, 0, "mpu-drain-blocked-parity", predicate=decommission_multipart_blocked)
        if not decommission_multipart_blocked(parity, 0):
            raise PoolStatusError("MPU drain failure disagrees with rc/Admin parity")
        reference = self.hook(
            case_id, "mpu-drain-journal", self.args.diagnostic_hook,
            ("--phase", "mpu-drain-failure", "--since", since), timeout=60,
        )
        journal = json.loads((self.evidence_dir / reference).read_text(encoding="utf-8"))
        records = multipart_drain_failure_records(journal, bucket=bucket, since=since, node_count=len(self.args.node_endpoint))
        self.save_json(case_id, "mpu-drain-reason.json", records)

    def case_decommission_multipart_integrity(self, case_id: str) -> None:
        if not self.args.diagnostic_hook:
            raise RuntimeError("MPU drain verification requires the read-only journal hook")
        bucket = self.create_bucket(case_id)
        inventory = self.put_inventory(
            case_id, bucket, count=max(32, self.args.fixture_objects // 2),
            size=active_fixture_bytes(self.args.fixture_bytes),
        )
        first = deterministic_payload(f"{case_id}:multipart:first", 5 * 1024 * 1024)
        second = deterministic_payload(f"{case_id}:multipart:second", 1024 * 1024)

        def part(upload: dict[str, Any], number: int, data: bytes, client: Any) -> None:
            response = self.multipart_request(
                case_id, f"{upload['kind']}-part-{number}", client, "upload_part",
                Bucket=bucket, Key=upload["key"], UploadId=upload["uploadId"], PartNumber=number, Body=data,
            )
            if not isinstance(response.get("ETag"), str) or not response["ETag"]:
                raise AssertionError("MPU UploadPart omitted its ETag")
            upload["parts"].append({
                "PartNumber": number, "ETag": response["ETag"], "Size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            })

        def complete(upload: dict[str, Any], client: Any) -> None:
            parts = [{name: item[name] for name in ("ETag", "PartNumber")} for item in upload["parts"]]
            response = self.multipart_request(
                case_id, f"{upload['kind']}-complete", client, "complete_multipart_upload",
                Bucket=bucket, Key=upload["key"], UploadId=upload["uploadId"], MultipartUpload={"Parts": parts},
            )
            if not isinstance(response.get("ETag"), str) or not response["ETag"]:
                raise AssertionError("MPU Complete omitted its ETag")
            upload["etag"] = response["ETag"]

        uploads = []
        for kind in ("completed", "pending", "abort-active", "abort-failed", "resume-cleared"):
            key = f"multipart/{kind}.bin"
            created = self.multipart_request(
                case_id, f"{kind}-create", self.client, "create_multipart_upload",
                Bucket=bucket, Key=key, Metadata={"kind": kind},
            )
            if not isinstance(created.get("UploadId"), str) or not created["UploadId"]:
                raise AssertionError("MPU creation omitted its UploadId")
            upload = {"kind": kind, "key": key, "uploadId": created["UploadId"], "parts": []}
            uploads.append(upload)
            part(upload, 1, first, self.client)
            if kind == "completed":
                part(upload, 2, second, self.client)
                complete(upload, self.client)
        completed, pending, abort_active, abort_failed, resume_cleared = uploads
        self.save_json(case_id, "multipart-fixture.json", {"bucket": bucket, "uploads": uploads})
        self.apply_topology(case_id, "expanded")
        self.assert_multipart_uploads(case_id, bucket, uploads[1:], "before-start")
        since = utc_now()
        self.start_decommission(case_id)
        self.wait_decommission(case_id, 0, "active", label="mpu-active")
        # Both ends of the S3 window must be observed active. A fast terminal
        # transition is a coverage failure, never permission to resume afterward.
        self.assert_multipart_uploads(case_id, bucket, uploads[1:], "active")
        client = self.retained_pool_client()
        part(pending, 2, second, client)
        self.assert_multipart_uploads(case_id, bucket, uploads[1:], "active-two-parts")
        complete(pending, client)
        self.multipart_request(
            case_id, "abort-active", client, "abort_multipart_upload",
            Bucket=bucket, Key=abort_active["key"], UploadId=abort_active["uploadId"],
        )
        active = self.decommission_status(case_id, 0, "mpu-after-active-io")
        if not decommission_active(active, 0, require_progress=True):
            raise AssertionError("decommission did not remain active across MPU continuation")
        self.save_json(case_id, "multipart-after-active-io.json", {"bucket": bucket, "uploads": uploads})

        # Retain owned uploads to exercise the gate and both recovery paths.
        self.wait_multipart_drain_blocked(case_id, bucket, since)
        self.assert_multipart_uploads(case_id, bucket, [abort_failed, resume_cleared], "blocked")
        self.multipart_request(
            case_id, "abort-failed", client, "abort_multipart_upload",
            Bucket=bucket, Key=abort_failed["key"], UploadId=abort_failed["uploadId"],
        )
        self.assert_multipart_uploads(case_id, bucket, [resume_cleared], "before-clear")
        self.clear_decommission(case_id)
        self.wait_decommission(case_id, 0, "cleared", label="mpu-drained-clear")
        self.assert_multipart_uploads(case_id, bucket, [resume_cleared], "after-clear")
        # Clear does not abort MPU data. Completing this source-owned UploadId
        # before retry also supplies real movement work for its strict counters.
        part(resume_cleared, 2, second, client)
        self.assert_multipart_uploads(case_id, bucket, [resume_cleared], "cleared-two-parts")
        complete(resume_cleared, client)
        self.assert_multipart_uploads(case_id, bucket, [], "drained-before-retry")
        self.save_json(case_id, "multipart-after-clear.json", {"bucket": bucket, "uploads": uploads})
        self.start_decommission(case_id, label="mpu-drained-retry")
        self.wait_decommission(case_id, 0, "complete", label="mpu-drained-complete")
        checker = self.retire_decommissioned_pool(case_id)
        expected_body = first + second
        for upload in (completed, pending, resume_cleared):
            head = self.multipart_request(
                case_id, f"{upload['kind']}-final-head", checker, "head_object", Bucket=bucket, Key=upload["key"],
            )
            if (
                head.get("ContentLength") != len(expected_body)
                or str(head.get("ETag", "")).strip('"') != upload["etag"].strip('"')
                or head.get("Metadata") != {"kind": upload["kind"]}
            ):
                raise AssertionError("multipart length, ETag or metadata changed after decommission")
            response = self.multipart_request(
                case_id, f"{upload['kind']}-final-get", checker, "get_object", Bucket=bucket, Key=upload["key"],
            )
            stream = response["Body"]
            try:
                body = stream.read()
            finally:
                stream.close()
            self.save_json(case_id, f"mpu-{upload['kind']}-final-content.json", {
                "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            })
            if body != expected_body:
                raise AssertionError("multipart payload changed after decommission")
        if self.multipart_entries(case_id, "retired-pending", checker, "list_multipart_uploads", Bucket=bucket):
            raise AssertionError("drained multipart uploads reappeared after retirement")
        self.assert_inventory(case_id, checker, bucket, inventory, "multipart-decommission-final")
        self.save_json(case_id, "multipart-summary.json", {
            "bucket": bucket, "uploads": uploads, "sha256": hashlib.sha256(expected_body).hexdigest(),
            "drainRecoveryVerified": True,
        })

    def case_decommission_live_io(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(
            case_id, size=active_fixture_bytes(self.args.fixture_bytes)
        )
        self.start_decommission(case_id)
        self.wait_decommission(case_id, 0, "active")
        clients = [self.node_clients[endpoint] for endpoint in self.args.node_endpoint]

        def live_put(index: int) -> dict[str, Any]:
            client = clients[index % len(clients)]
            key = f"during-decommission/concurrent-{index:02d}.bin"
            body = deterministic_payload(f"{case_id}:{key}", 64 * 1024)
            response = self.put_retrying(client, bucket, key, body, {"phase": "decommission"})
            return {
                "key": key,
                "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "etag": str(response.get("ETag", "")).strip('"'),
                "metadata": {"phase": "decommission"},
                "versionId": response.get("VersionId"),
            }

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.workers) as pool:
            live = list(pool.map(live_put, range(self.args.live_objects)))
        live.extend(
            self.exercise_live_io(
                case_id,
                bucket,
                prefix="during-decommission-serial",
                client=self.retained_pool_client(),
            )
        )
        status, _, body = self.signed_request(
            self.args.endpoint, "GET", f"{ADMIN_PREFIX}/decommission/status"
        )
        if status != 200 or not decommission_active(json.loads(body), 0, require_progress=True):
            raise AssertionError("decommission did not remain active across concurrent I/O")
        self.wait_decommission(case_id, 0, "complete")
        self.assert_inventory(
            case_id,
            self.retire_decommissioned_pool(case_id),
            bucket,
            inventory + live,
            "decommission-live-final",
        )

    def case_retired_config_removal(self, case_id: str) -> None:
        bucket, inventory = self.seed_and_expand(case_id)
        self.start_decommission(case_id)
        checker = self.retire_decommissioned_pool(case_id)
        self.assert_inventory(case_id, checker, bucket, inventory, "retired-config-final")
        payload = b"written after retired pool removal"
        self.client.put_object(Bucket=bucket, Key="after-removal.bin", Body=payload)
        observed = self.client.get_object(Bucket=bucket, Key="after-removal.bin")["Body"].read()
        if observed != payload:
            raise AssertionError("post-removal object round-trip failed")
        self.census(case_id, "after-retired-config-removal")

    def case_operation_guards(self, case_id: str) -> None:
        bucket = self.create_bucket(case_id)
        inventory = self.put_inventory(
            case_id,
            bucket,
            size=active_fixture_bytes(self.args.fixture_bytes),
        )
        self.run_rc(
            case_id,
            "unknown-pool-status",
            ["admin", "pool", "status", self.rc_alias, "999999", "--by-id"],
            expect_success=False,
            expected_error_code="InvalidArgument",
        )
        self.run_rc(
            case_id,
            "unknown-pool-decommission",
            ["admin", "decommission", "start", self.rc_alias, "999999", "--by-id"],
            expect_success=False,
            expected_error_code="InvalidArgument",
        )
        self.run_rc(
            case_id,
            "unknown-pool-decommission-status",
            ["admin", "decommission", "status", self.rc_alias, "999999", "--by-id"],
            expect_success=False,
            expected_error_code="InvalidArgument",
        )
        for operation in ("cancel", "clear"):
            self.run_rc(
                case_id,
                f"unknown-pool-decommission-{operation}",
                [
                    "admin",
                    "decommission",
                    operation,
                    self.rc_alias,
                    "999999",
                    "--by-id",
                ],
                expect_success=False,
                expected_error_code="InvalidArgument",
            )
        self.run_rc(
            case_id,
            "only-pool-decommission",
            ["admin", "decommission", "start", self.rc_alias, "0", "--by-id"],
            expect_success=False,
            expected_error_code="InvalidArgument",
        )
        self.run_rc(
            case_id,
            "single-pool-rebalance",
            ["admin", "rebalance", "start", self.rc_alias],
            expect_success=False,
            expected_error_code="NotImplemented",
        )
        for operation in ("cancel", "clear"):
            self.run_rc(
                case_id,
                f"idle-pool-decommission-{operation}",
                [
                    "admin",
                    "decommission",
                    operation,
                    self.rc_alias,
                    "0",
                    "--by-id",
                ],
                expect_success=False,
                expected_error_code="InvalidArgument",
            )
        guarded, _ = self.pool_snapshot(case_id, "after-single-pool-guards", 1)
        guarded_pool = pool_entries(guarded)[0]
        if str(guarded_pool.get("decommissionStatus", "")).lower() not in {
            "",
            "none",
        } or guarded_pool.get("decommissionInfo") is not None:
            raise AssertionError(
                "rejected single-pool operations left partial decommission metadata"
            )
        if str(guarded_pool.get("rebalanceStatus", "")).lower() not in {
            "",
            "none",
        }:
            raise AssertionError(
                "rejected single-pool rebalance left partial operation metadata"
            )
        self.apply_topology(case_id, "expanded")
        rebalance_id = self.start_rebalance(case_id)
        self.wait_rebalance(case_id, rebalance_id, "active")
        self.run_rc(
            case_id,
            "duplicate-rebalance-start",
            ["admin", "rebalance", "start", self.rc_alias],
            expect_success=False,
            expected_error_code="OperationAborted",
        )
        self.run_rc(
            case_id,
            "decommission-during-rebalance",
            ["admin", "decommission", "start", self.rc_alias, "0", "--by-id"],
            expect_success=False,
            expected_error_code="OperationAborted",
        )
        self.wait_rebalance(case_id, rebalance_id, "active")
        self.stop_rebalance(case_id)
        self.wait_rebalance(case_id, rebalance_id, "stopped")
        self.start_decommission(case_id)
        self.wait_decommission(case_id, 0, "active")
        self.run_rc(
            case_id,
            "duplicate-decommission-start",
            ["admin", "decommission", "start", self.rc_alias, "0", "--by-id"],
            expect_success=False,
            expected_error_code="InvalidRequest",
        )
        self.run_rc(
            case_id,
            "rebalance-during-decommission",
            ["admin", "rebalance", "start", self.rc_alias],
            expect_success=False,
            expected_error_code="InvalidRequest",
        )
        self.wait_decommission(case_id, 0, "active")
        self.cancel_decommission(case_id)
        self.wait_decommission(case_id, 0, "canceled")
        self.run_rc(
            case_id,
            "restart-canceled-decommission-before-clear",
            ["admin", "decommission", "start", self.rc_alias, "0", "--by-id"],
            expect_success=False,
            expected_error_code="InvalidArgument",
        )
        self.clear_decommission(case_id)
        self.wait_decommission(case_id, 0, "cleared")
        self.assert_inventory(case_id, self.client, bucket, inventory, "operation-guards-final")

    def classify_exception(self, exc: BaseException, *, case_id: str | None = None) -> str:
        if isinstance(exc, (AssertionError, PoolStatusError)):
            return "fail"
        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "SlowDown" and case_id in {
                "pool.decommission-live-io", "pool.rebalance-live-io"
            }:
                return "fail"
            return "infra_error" if code in INFRA_ERROR_CODES else "fail"
        return "infra_error"

    def run_case(self, case_id: str, function: Callable[[str], None]) -> None:
        started_at = utc_now()
        started = time.monotonic()
        state = "pass"
        message = "all assertions passed"
        print(f"[RUN ] {case_id}", flush=True)
        self.events.record("case.start", case=case_id)
        try:
            self.reset_case_environment(case_id)
            function(case_id)
        except Exception as exc:
            state = self.classify_exception(exc, case_id=case_id)
            message = self.redact(f"{type(exc).__name__}: {exc}")
            self.events.record(
                "case.error",
                case=case_id,
                state=state,
                errorType=type(exc).__name__,
                message=self.redact(str(exc)),
            )
            try:
                self.capture_diagnostics(case_id, "case-failure-before-cleanup")
            except Exception as diagnostic_exc:
                diagnostic_message = self.redact(
                    f"{type(diagnostic_exc).__name__}: {diagnostic_exc}"
                )
                message += f"; diagnostic collection failed: {diagnostic_message}"
                self.events.record(
                    "diagnostic.collection.error", case=case_id, message=diagnostic_message
                )
            if not self.args.cleanup_on_failure:
                self.isolation_failure = {"caseId": case_id, "message": message}
        finally:
            bucket = self.bucket_name(case_id)
            if bucket in self.created_buckets and (state == "pass" or self.args.cleanup_on_failure):
                try:
                    self.cleanup_bucket(bucket)
                except Exception as exc:
                    cleanup_state = self.classify_exception(exc)
                    cleanup_message = self.redact(f"{type(exc).__name__}: {exc}")
                    self.save_json(
                        case_id, "cleanup-error.json", {"state": cleanup_state, "message": cleanup_message}
                    )
                    self.events.record(
                        "case.cleanup.error", case=case_id, state=cleanup_state, message=cleanup_message
                    )
                    if state == "pass":
                        state = cleanup_state
                    message = self.redact(
                        f"{message}; cleanup failed: {cleanup_message}"
                    )
                    if not self.args.cleanup_on_failure:
                        self.isolation_failure = {"caseId": case_id, "message": message}
            elif bucket in self.created_buckets:
                message += f"; preserved failed bucket {bucket} in {self.current_layout} topology"
        directory = self.evidence_path(case_id, "evidence-index").parent
        evidence = sorted(
            str(path.relative_to(self.evidence_dir))
            for path in directory.iterdir()
            if path.is_file() and path.name != "evidence-index"
        )
        if not evidence:
            evidence = [self.save_json(case_id, "case-summary.json", {"state": state, "message": message})]
        result = CaseResult(
            id=case_id,
            state=state,
            message=message,
            started_at=started_at,
            finished_at=utc_now(),
            duration_seconds=round(time.monotonic() - started, 3),
            evidence=evidence,
        )
        self.results.append(result)
        self.events.record("case.complete", **asdict(result))
        label = "PASS" if state == "pass" else state.upper()
        print(f"[{label:5}] {case_id}: {message}", flush=True)

    def block_case(self, case_id: str) -> None:
        assert self.isolation_failure is not None
        message = (
            "blocked to preserve the failed pool topology from "
            f"{self.isolation_failure['caseId']}: {self.isolation_failure['message']}"
        )
        evidence = [
            self.save_json(
                case_id,
                "blocked-by-isolation.json",
                {"state": "infra_error", "reason": self.isolation_failure},
            )
        ]
        now = utc_now()
        self.results.append(
            CaseResult(
                id=case_id,
                state="infra_error",
                message=message,
                started_at=now,
                finished_at=now,
                duration_seconds=0.0,
                evidence=evidence,
            )
        )
        self.events.record("case.blocked", case=case_id, reason=self.isolation_failure)
        print(f"[INFRA] {case_id}: {message}", flush=True)

    def finalize_physical_cleanup(self) -> None:
        states = {item.state for item in self.results}
        should_cleanup = not states.difference({"pass"}) or self.args.cleanup_on_failure
        if not should_cleanup:
            self.cleanup_outcome_value = {
                "attempted": False,
                "state": "preserved",
                "message": "non-pass pool topology and data were preserved for investigation",
                "preservedBuckets": sorted(self.created_buckets),
                "layout": self.current_layout,
            }
            return
        try:
            reference = self.hook("run-cleanup", "physical-cleanup", self.args.cleanup_hook)
        except Exception as exc:
            self.cleanup_outcome_value = {
                "attempted": True,
                "state": "infra_error",
                "message": self.redact(
                    f"physical cleanup failed: {type(exc).__name__}: {exc}"
                ),
            }
            for result in self.results:
                if result.state == "pass":
                    result.state = "infra_error"
                    result.message = self.cleanup_outcome_value["message"]
                    result.evidence.append(reference if "reference" in locals() else self.save_json(
                        result.id, "physical-cleanup-failure.json", self.cleanup_outcome_value
                    ))
                    break
            return
        self.created_buckets.clear()
        self.cleanup_outcome_value = {
            "attempted": True,
            "state": "pass",
            "message": "marker-guarded physical pool data cleanup completed",
            "evidence": reference,
        }

    def write_junit(self, overall: str, duration: float) -> None:
        suite = ET.Element(
            "testsuite",
            {
                "name": "rustfs-fixed-environment-pool-lifecycle",
                "tests": str(len(self.results)),
                "failures": str(sum(item.state == "fail" for item in self.results)),
                "errors": str(sum(item.state == "infra_error" for item in self.results)),
                "skipped": "0",
                "time": f"{duration:.3f}",
            },
        )
        for item in self.results:
            case = ET.SubElement(
                suite,
                "testcase",
                {
                    "classname": "rustfs.pool_lifecycle",
                    "name": item.id,
                    "time": f"{item.duration_seconds:.3f}",
                },
            )
            if item.state == "fail":
                ET.SubElement(case, "failure", {"message": item.message}).text = item.message
            elif item.state == "infra_error":
                ET.SubElement(case, "error", {"message": item.message}).text = item.message
            ET.SubElement(case, "system-out").text = "\n".join(item.evidence)
        tree = ET.ElementTree(suite)
        ET.indent(tree, space="  ")
        tree.write(self.evidence_dir / "junit.xml", encoding="utf-8", xml_declaration=True)
        self.events.record("run.complete", state=overall)

    def write_results(self, started_at: str, started: float) -> str:
        selected_cases = pool_harness_scored_cases(self.args.case)
        order = {case_id: index for index, case_id in enumerate(CASE_IDS)}
        self.results.sort(key=lambda item: order[item.id])
        states = {item.state for item in self.results}
        overall = "fail" if "fail" in states else "infra_error" if "infra_error" in states else "pass"
        finished_at = utc_now()
        manifest = {
            "runId": self.args.run_id,
            "environmentId": self.args.environment_id,
            "endpoint": self.initial_endpoint,
            "finalControlEndpoint": self.args.endpoint,
            "nodeEndpoints": self.args.node_endpoint,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "durationSeconds": round(time.monotonic() - started, 3),
            "fixtureObjects": self.args.fixture_objects,
            "fixtureBytes": self.args.fixture_bytes,
            "activeFixtureBytes": active_fixture_bytes(self.args.fixture_bytes),
            "liveObjects": self.args.live_objects,
            "boto3Version": boto3.__version__,
            "botocoreVersion": botocore.__version__,
            "rcBinaryPath": self.rc_bin,
            "rcVersion": self.rc_version,
            "rcBinarySha256": self.rc_binary_sha256,
            "expectedRcBinarySha256": self.expected_rc_binary_sha256 or None,
            "executionPlan": {
                "selectionMode": "filtered" if self.args.case is not None else "full",
                "module": "pool",
                "scoredCases": list(selected_cases),
                "scoredCaseCount": len(selected_cases),
                "fixtures": [],
            },
            "fixtureOutcomes": [],
            "cleanupOnFailure": self.args.cleanup_on_failure,
            "cleanupOutcome": self.cleanup_outcome_value,
            "preservedBuckets": sorted(self.created_buckets),
            "finalLayout": self.current_layout,
        }
        (self.evidence_dir / "run-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        result = {
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
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        self.write_junit(overall, time.monotonic() - started)
        return overall

    def preflight_failure(self, selected_cases: tuple[str, ...], exc: BaseException) -> None:
        message = self.redact(
            f"pool harness preflight failed: {type(exc).__name__}: {exc}"
        )
        for case_id in selected_cases:
            evidence = [self.save_json(case_id, "preflight-failure.json", {"message": message})]
            self.results.append(
                CaseResult(
                    id=case_id,
                    state="infra_error",
                    message=message,
                    started_at=utc_now(),
                    finished_at=utc_now(),
                    duration_seconds=0.0,
                    evidence=evidence,
                )
            )

    def run(self) -> int:
        started_at = utc_now()
        started = time.monotonic()
        selected_cases = pool_harness_scored_cases(self.args.case)
        self.events.record(
            "run.start",
            runId=self.args.run_id,
            environmentId=self.args.environment_id,
            selectionMode="filtered" if self.args.case is not None else "full",
            scoredCases=list(selected_cases),
        )
        try:
            self.wait_s3("run-preflight", self.client, "initial S3 connectivity")
            self.configure_rc()
        except Exception as exc:
            self.preflight_failure(selected_cases, exc)
            self.finalize_physical_cleanup()
            overall = self.write_results(started_at, started)
            return 2 if overall == "infra_error" else 1

        cases: dict[str, Callable[[str], None]] = {
            "pool.status": self.case_status,
            "pool.expand-and-rebalance": self.case_expand_and_rebalance,
            "pool.expansion-restart-persistence": self.case_expansion_restart_persistence,
            "pool.rebalance-stop-resume": self.case_rebalance_stop_resume,
            "pool.rebalance-live-io": self.case_rebalance_live_io,
            "pool.decommission": self.case_decommission,
            "pool.decommission-cancel-clear-resume": self.case_decommission_cancel_clear_resume,
            "pool.decommission-restart-recovery": self.case_decommission_restart_recovery,
            "pool.decommission-versioned-integrity": self.case_decommission_versioned_integrity,
            "pool.decommission-multipart-integrity": self.case_decommission_multipart_integrity,
            "pool.decommission-live-io": self.case_decommission_live_io,
            "pool.retired-config-removal": self.case_retired_config_removal,
            "pool.operation-guards": self.case_operation_guards,
        }
        for case_id in selected_cases:
            if self.isolation_failure is not None:
                self.block_case(case_id)
            else:
                self.run_case(case_id, cases[case_id])
        self.finalize_physical_cleanup()
        overall = self.write_results(started_at, started)
        print(
            f"Pool lifecycle validation {overall}: result={self.evidence_dir / 'result.json'}",
            flush=True,
        )
        return {"pass": 0, "fail": 1, "infra_error": 2}[overall]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=os.environ.get("RUSTFS_POOL_ENDPOINT"))
    parser.add_argument("--node-endpoint", action="append", required=True)
    parser.add_argument("--access-key", default=os.environ.get("RUSTFS_ACCESS_KEY"))
    parser.add_argument("--secret-key", default=os.environ.get("RUSTFS_SECRET_KEY"))
    parser.add_argument("--region", default=os.environ.get("RUSTFS_REGION", "us-east-1"))
    parser.add_argument("--environment-id", default="lifecycle-a")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--active-timeout", type=float, default=90)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--fixture-objects", type=int, default=128)
    parser.add_argument("--fixture-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--live-objects", type=int, default=16)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rc-bin", default="rc")
    parser.add_argument("--reset-hook", required=True)
    parser.add_argument("--topology-hook", required=True)
    parser.add_argument("--restart-hook", required=True)
    parser.add_argument("--census-hook", required=True)
    parser.add_argument("--diagnostic-hook")
    parser.add_argument("--cleanup-hook", required=True)
    parser.add_argument("--case", action="append", choices=CASE_IDS)
    parser.add_argument("--cleanup-on-failure", action="store_true")
    args = parser.parse_args()
    if not args.endpoint or not args.access_key or not args.secret_key:
        parser.error("endpoint, access key, and secret key are required")
    if len(args.node_endpoint) not in {2, 4}:
        parser.error("--node-endpoint must be supplied exactly two or four times")
    for value in (args.endpoint, *args.node_endpoint):
        parsed = urllib.parse.urlparse(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            parser.error(f"invalid absolute HTTP(S) endpoint: {value!r}")
    if any(character in args.access_key + args.secret_key for character in "\r\n"):
        parser.error("access key and secret key must not contain line breaks")
    if (
        args.timeout <= 0
        or args.active_timeout <= 0
        or args.poll_interval <= 0
        or args.fixture_objects < 4
        or args.fixture_bytes < 1
        or args.live_objects < 1
        or args.workers < 1
    ):
        parser.error("timeouts and fixture/worker sizes must be positive")
    return args


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    harness = PoolHarness(args)
    try:
        return harness.run()
    finally:
        harness.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130) from None
