#!/usr/bin/env python3
"""Run bucket- and site-replication black-box checks against two RustFS sites."""

from __future__ import annotations

import argparse
import base64
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
from pathlib import Path
from typing import Any, Callable

from case_plan import (
    BUCKET_CASE_IDS,
    SITE_CASE_IDS,
    SITE_GROUP_ACTIVE_CASE_IDS,
    SITE_GROUP_ACTIVE_FIXTURE,
)
from rc_binary import resolve_rc_binary
from replication_metrics import replication_metrics_ready

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
        "boto3 is required for replication validation"
    ) from exc


ADMIN_PREFIX = "/rustfs/admin/v3"
INFRA_EXCEPTIONS = (ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError)
INFRA_ERROR_CODES = {
    "InvalidAccessKeyId",
    "InvalidToken",
    "RequestTimeout",
    "ServiceUnavailable",
    "SignatureDoesNotMatch",
    "SlowDown",
}

def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "-." else "-" for char in value)


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, bytes):
        return {"bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
    return str(value)


def without_response_metadata(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "ResponseMetadata"}


@dataclass(frozen=True)
class Site:
    id: str
    endpoint: str
    access_key: str
    secret_key: str


@dataclass
class CaseResult:
    id: str
    state: str
    message: str
    started_at: str
    finished_at: str
    duration_seconds: float
    evidence: list[str] = field(default_factory=list)


class BucketCleanupResidual(RuntimeError):
    """A logical test bucket survived best-effort cleanup before phase reset."""

    def __init__(
        self,
        bucket: str,
        last_error: BaseException | None,
        inventory: dict[str, Any],
    ):
        self.bucket = bucket
        self.inventory = inventory
        super().__init__(f"{bucket} remained non-empty after cleanup retries: {last_error}")


class LogicalCleanupFailure(RuntimeError):
    """One case's logical cleanup failed, with isolation evidence persisted."""

    def __init__(
        self,
        *,
        residuals: list[dict[str, Any]],
        errors: list[dict[str, Any]],
        evidence: str,
        tracked_buckets: list[dict[str, str]],
        isolation_restored: bool,
    ) -> None:
        self.residuals = residuals
        self.errors = errors
        self.evidence = evidence
        self.tracked_buckets = tracked_buckets
        self.isolation_restored = isolation_restored
        super().__init__(
            "logical bucket cleanup failed: "
            f"residuals={len(residuals)} errors={len(errors)} "
            f"isolation_restored={isolation_restored} evidence={evidence}"
        )


class PreconditionError(RuntimeError):
    """A diagnostic fixture could not establish a trustworthy product state."""


def _site_endpoint_identity(endpoint: str) -> tuple[str, int] | None:
    """Return RustFS' product identity for one site endpoint.

    RustFS intentionally keys a site by lower-case host plus effective port;
    transport scheme and URL path are not part of the persisted identity.
    Keeping the test harness on that exact contract prevents it from trying to
    add two URLs that the product will collapse into one deployment.
    """

    if not isinstance(endpoint, str):
        return None
    try:
        parsed = urllib.parse.urlsplit(endpoint)
        scheme = parsed.scheme.lower()
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError:
        return None
    if scheme not in {"http", "https"} or not host:
        return None
    if port is None:
        port = 80 if scheme == "http" else 443
    return host, port


def classify_site_group_state(
    info_by_site: dict[str, Any],
    expected_endpoints: tuple[str, str],
) -> tuple[str, str]:
    """Classify the two-site control-plane snapshot without mutating it."""

    expected_identities = tuple(
        _site_endpoint_identity(endpoint) for endpoint in expected_endpoints
    )
    if any(identity is None for identity in expected_identities):
        return "malformed", "a configured validation endpoint is invalid"
    if len(set(expected_identities)) != 2:
        return "malformed", "the configured validation endpoints are not distinct"

    expected_sites = ("site-a", "site-b")
    if any(not isinstance(info_by_site.get(site_id), dict) for site_id in expected_sites):
        return "malformed", "site info is missing or is not an object"

    snapshots = [info_by_site[site_id] for site_id in expected_sites]
    enabled = [snapshot.get("enabled") for snapshot in snapshots]
    if all(value is False for value in enabled):
        stale = [
            expected_sites[index]
            for index, snapshot in enumerate(snapshots)
            if snapshot.get("serviceAccountAccessKey") not in (None, "")
            or snapshot.get("pendingOperation") is not None
            or snapshot.get("sites") not in (None, [])
            or _site_retry_state_is_degraded(snapshot.get("retryStats"))
        ]
        if stale:
            return "malformed", f"disabled site(s) retain replication state: {', '.join(stale)}"
        return "disabled", "both sites are cleanly unconfigured"

    if not all(value is True for value in enabled):
        return "partial", f"site enabled flags differ or are invalid: {enabled!r}"

    if any(snapshot.get("pendingOperation") is not None for snapshot in snapshots):
        return "pending", "a site-replication operation is still pending"

    degraded = [
        expected_sites[index]
        for index, snapshot in enumerate(snapshots)
        if _site_retry_state_is_degraded(snapshot.get("retryStats"))
    ]
    if degraded:
        return "degraded", f"site retry state is not clear: {', '.join(degraded)}"

    expected_identity_set = set(expected_identities)

    peer_maps: list[dict[tuple[str, int], str]] = []
    service_accounts: list[str] = []
    for site_id, snapshot in zip(expected_sites, snapshots, strict=True):
        service_account = snapshot.get("serviceAccountAccessKey")
        if not isinstance(service_account, str) or not service_account:
            return "malformed", f"{site_id} has no site-replication service account"
        service_accounts.append(service_account)
        peers = snapshot.get("sites")
        if not isinstance(peers, list) or len(peers) != 2:
            return "malformed", f"{site_id} peer membership does not contain exactly two entries"
        if not all(isinstance(peer, dict) for peer in peers):
            return "malformed", f"{site_id} peer membership contains a non-object entry"
        peer_map: dict[tuple[str, int], str] = {}
        for peer in peers:
            deployment_id = peer.get("deploymentID")
            endpoint = peer.get("endpoint")
            sync = peer.get("sync")
            if not isinstance(deployment_id, str) or not deployment_id:
                return "malformed", f"{site_id} has an invalid deployment ID"
            identity = _site_endpoint_identity(endpoint)
            if identity is None:
                return "malformed", f"{site_id} has an invalid peer endpoint"
            if not isinstance(sync, str) or sync.lower() != "enable":
                return "malformed", f"{site_id} has a peer without sync=enable"
            if identity in peer_map:
                return "malformed", f"{site_id} reports a duplicate peer endpoint"
            peer_map[identity] = deployment_id
        if len(set(peer_map.values())) != 2:
            return "malformed", f"{site_id} does not report two unique deployment IDs"
        if set(peer_map) != expected_identity_set:
            return "malformed", (
                f"{site_id} site-group endpoints do not match the configured "
                "validation sites"
            )
        peer_maps.append(peer_map)

    if peer_maps[0] != peer_maps[1]:
        return "malformed", "the two sites report different endpoint/deployment membership"
    if len(set(service_accounts)) != 1:
        return "malformed", "the two sites report different service-account access keys"
    return "active", "both sites report one stable two-deployment group"


def site_group_has_ilm_expiry_value(
    info_by_site: dict[str, Any],
    expected: bool,
) -> tuple[bool, str]:
    """Require every peer view to expose one exact ILM-expiry flag value."""

    for site_id in ("site-a", "site-b"):
        snapshot = info_by_site.get(site_id)
        if not isinstance(snapshot, dict):
            return False, f"{site_id} site info is missing or malformed"
        peers = snapshot.get("sites")
        if not isinstance(peers, list) or len(peers) != 2:
            return False, f"{site_id} does not expose exactly two site peers"
        for peer in peers:
            if not isinstance(peer, dict):
                return False, f"{site_id} contains a non-object site peer"
            if peer.get("replicate-ilm-expiry") is not expected:
                return False, (
                    f"{site_id} peer {peer.get('deploymentID')!r} does not expose "
                    f"replicate-ilm-expiry={str(expected).lower()}"
                )
    return True, (
        "all site peers expose "
        f"replicate-ilm-expiry={str(expected).lower()}"
    )


def _site_retry_state_is_degraded(value: Any) -> bool:
    if value is None:
        return False
    if not isinstance(value, dict):
        return True
    pending = value.get("pending", 0)
    failed = value.get("failed", 0)
    last_error = value.get("lastError", "")
    return (
        not isinstance(pending, int)
        or isinstance(pending, bool)
        or pending < 0
        or not isinstance(failed, int)
        or isinstance(failed, bool)
        or failed < 0
        or pending != 0
        or failed != 0
        or not isinstance(last_error, str)
        or bool(last_error)
    )


class EventRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event: str, **details: Any) -> None:
        item = {"timestamp": utc_now(), "event": event, "details": details}
        with self.lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(item, default=json_default, sort_keys=True) + "\n")


class ReplicationHarness:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.evidence_dir = args.evidence_dir.resolve()
        self.case_dir = self.evidence_dir / "cases"
        self.case_dir.mkdir(parents=True, exist_ok=True)
        self.events = EventRecorder(self.evidence_dir / "events.jsonl")
        self.results: list[CaseResult] = []
        self.rc_home = Path(tempfile.mkdtemp(prefix="rustfs-replication-rc-"))
        self.rc_env = os.environ.copy()
        self.rc_env["RC_CONFIG_DIR"] = str(self.rc_home)
        self.rc_bin = args.rc_bin
        self.rc_version = "unavailable"
        self.rc_binary_sha256 = "unavailable"
        self.expected_rc_binary_sha256 = os.environ.get(
            "RUSTFS_EXPECTED_RC_SHA256", ""
        ).lower()
        self.site_a = Site("site-a", args.endpoint_a.rstrip("/"), args.access_key, args.secret_key)
        self.site_b = Site("site-b", args.endpoint_b.rstrip("/"), args.access_key, args.secret_key)
        self.alias_a = f"rv-a-{safe_name(args.run_id)[-12:]}"
        self.alias_b = f"rv-b-{safe_name(args.run_id)[-12:]}"
        self.client_a = self.s3_client(self.site_a)
        self.client_b = self.s3_client(self.site_b)
        self.created_buckets: list[tuple[Any, str]] = []
        self.secrets: set[str] = {args.access_key, args.secret_key}
        self.isolation_failure: dict[str, Any] | None = None
        self.requested_fixtures = frozenset(getattr(args, "fixture", None) or ())
        self.site_group_tracker_state = "fresh"
        self.site_group_mutation_attempted = False
        self.fixture_outcomes: list[dict[str, Any]] = []

    def close(self) -> None:
        shutil.rmtree(self.rc_home, ignore_errors=True)

    def s3_client(self, site: Site, access_key: str | None = None, secret_key: str | None = None):
        return boto3.client(
            "s3",
            endpoint_url=site.endpoint,
            aws_access_key_id=access_key or site.access_key,
            aws_secret_access_key=secret_key or site.secret_key,
            region_name=self.args.region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
                connect_timeout=5,
                read_timeout=30,
            ),
        )

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

    def redact(self, value: str) -> str:
        result = value
        for secret in sorted((item for item in self.secrets if item), key=len, reverse=True):
            result = result.replace(secret, "[REDACTED]")
        return result

    def bucket_name(self, case_id: str, role: str = "src") -> str:
        digest = hashlib.sha256(f"{self.args.run_id}:{case_id}:{role}".encode()).hexdigest()[:12]
        return f"rv-{role}-{digest}"

    def identity_name(self, case_id: str, role: str) -> str:
        digest = hashlib.sha256(f"{self.args.run_id}:{case_id}:{role}".encode()).hexdigest()[:10]
        return f"rv-{role}-{digest}"

    def create_bucket(self, client: Any, bucket: str) -> None:
        client.create_bucket(Bucket=bucket)
        self.created_buckets.append((client, bucket))
        self.events.record("bucket.created", bucket=bucket)

    @staticmethod
    def enable_versioning(client: Any, bucket: str) -> None:
        client.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        status = client.get_bucket_versioning(Bucket=bucket).get("Status")
        if status != "Enabled":
            raise AssertionError(f"versioning did not become enabled: {status!r}")

    def wait_until(
        self,
        case_id: str,
        description: str,
        probe: Callable[[], tuple[bool, Any]],
        timeout: float | None = None,
    ) -> Any:
        deadline = time.monotonic() + (timeout or self.args.timeout)
        attempts = 0
        last: Any = None
        while time.monotonic() < deadline:
            attempts += 1
            try:
                complete, last = probe()
            except INFRA_EXCEPTIONS as exc:
                complete, last = False, {"error": f"{type(exc).__name__}: {exc}"}
            if complete:
                self.events.record(
                    "poll.complete",
                    case=case_id,
                    description=description,
                    attempts=attempts,
                    detail=last,
                )
                return last
            time.sleep(self.args.poll_interval)
        self.save_json(
            case_id,
            f"poll-timeout-{safe_name(description)}.json",
            {"description": description, "attempts": attempts, "last": last},
        )
        raise AssertionError(f"timed out waiting for {description}; last={last}")

    def wait_api(self, case_id: str, client: Any, label: str) -> None:
        def probe() -> tuple[bool, Any]:
            try:
                response = client.list_buckets()
                return True, {"requestId": response.get("ResponseMetadata", {}).get("RequestId")}
            except (*INFRA_EXCEPTIONS, ClientError) as exc:
                return False, {"error": f"{type(exc).__name__}: {exc}"[:400]}

        self.wait_until(case_id, f"{label} S3 API readiness", probe, timeout=120)

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
        )
        if version.returncode != 0:
            raise RuntimeError(f"rc --version failed: {version.stderr.strip()}")
        self.rc_version = (version.stdout or version.stderr).strip()
        if (
            self.expected_rc_binary_sha256
            and self.rc_binary_sha256.lower() != self.expected_rc_binary_sha256
        ):
            raise RuntimeError(
                "resolved rc binary SHA-256 differs from RUSTFS_EXPECTED_RC_SHA256"
            )
        for alias, site in ((self.alias_a, self.site_a), (self.alias_b, self.site_b)):
            result = subprocess.run(
                [
                    self.rc_bin,
                    "--no-color",
                    "alias",
                    "set",
                    alias,
                    site.endpoint,
                    site.access_key,
                    site.secret_key,
                    "--bucket-lookup",
                    "path",
                ],
                env=self.rc_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError(f"rc alias set failed for {site.id}: {result.stderr.strip()}")
        self.events.record(
            "rc.configured",
            binaryPath=self.rc_bin,
            version=self.rc_version,
            binarySha256=self.rc_binary_sha256,
        )

    def run_rc(
        self,
        case_id: str,
        name: str,
        arguments: list[str],
        *,
        expect_success: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        command = [self.rc_bin, "--json", "--no-color", *arguments]
        result = subprocess.run(
            command,
            env=self.rc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        sanitized = ["[REDACTED]" if token in self.secrets else token for token in command]
        self.save_json(
            case_id,
            f"{name}.json",
            {
                "command": sanitized,
                "exitCode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
        )
        if expect_success and result.returncode != 0:
            raise AssertionError(
                f"rc {name} exited {result.returncode}: {self.redact(result.stderr.strip())}"
            )
        if not expect_success and result.returncode == 0:
            raise AssertionError(f"rc {name} unexpectedly succeeded")
        return result

    def signed_request(
        self,
        site: Site,
        method: str,
        path: str,
        *,
        query: str = "",
        body: bytes | None = None,
        content_type: str | None = None,
        timeout: float = 30,
    ) -> tuple[int, dict[str, str], bytes]:
        url = f"{site.endpoint}{urllib.parse.quote(path, safe='/-._~')}"
        if query:
            url += f"?{query}"
        payload = body or b""
        headers = {"x-amz-content-sha256": hashlib.sha256(payload).hexdigest()}
        if content_type:
            headers["content-type"] = content_type
        request = AWSRequest(method=method, url=url, data=body, headers=headers)
        SigV4Auth(Credentials(site.access_key, site.secret_key), "s3", self.args.region).add_auth(request)
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

    def admin_request(
        self,
        site: Site,
        method: str,
        subpath: str,
        *,
        query: str = "",
        payload: Any | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        return self.signed_request(
            site,
            method,
            f"{ADMIN_PREFIX}/{subpath}",
            query=query,
            body=body,
            content_type="application/json" if body is not None else None,
        )

    @staticmethod
    def json_body(body: bytes) -> Any:
        if not body.strip():
            return {}
        return json.loads(body)

    @staticmethod
    def required_json_object(body: bytes, operation: str) -> dict[str, Any]:
        try:
            parsed = ReplicationHarness.json_body(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AssertionError(f"{operation} returned malformed JSON") from exc
        if not isinstance(parsed, dict):
            raise AssertionError(
                f"{operation} returned a non-object JSON value: {parsed!r}"
            )
        return parsed

    def hook(self, case_id: str, name: str, raw: str | None) -> str:
        if not raw:
            raise RuntimeError(f"{name} requires its driver hook")
        command = json.loads(raw)
        if not isinstance(command, list) or not command or not all(isinstance(x, str) and x for x in command):
            raise RuntimeError(f"{name} hook must be a JSON command array")
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        path = self.save_text(case_id, f"{name}.log", result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"{name} hook exited {result.returncode}")
        return path

    def wait_object(
        self,
        case_id: str,
        client: Any,
        bucket: str,
        key: str,
        expected: bytes,
        *,
        expected_version: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        expected_sha = hashlib.sha256(expected).hexdigest()

        def probe() -> tuple[bool, Any]:
            try:
                response = client.get_object(Bucket=bucket, Key=key)
                body = response["Body"].read()
                version = response.get("VersionId")
                detail = {
                    "bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "versionId": version,
                    "metadata": response.get("Metadata", {}),
                }
                matches = detail["sha256"] == expected_sha
                if expected_version is not None:
                    matches = matches and version == expected_version
                return matches, detail
            except ClientError as exc:
                return False, {"error": exc.response.get("Error", {}).get("Code", "unknown")}

        return self.wait_until(case_id, f"{bucket}/{key} replication", probe, timeout=timeout)

    @staticmethod
    def object_exists(client: Any, bucket: str, key: str) -> tuple[bool, Any]:
        try:
            response = client.head_object(Bucket=bucket, Key=key)
            return True, {"versionId": response.get("VersionId"), "contentLength": response.get("ContentLength")}
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "unknown")
            if code in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}:
                return False, {"error": code}
            raise

    def assert_absent_for(
        self, case_id: str, client: Any, bucket: str, key: str, duration: float
    ) -> dict[str, Any]:
        deadline = time.monotonic() + duration
        observations = 0
        last: Any = None
        while time.monotonic() < deadline:
            observations += 1
            exists, last = self.object_exists(client, bucket, key)
            if exists:
                raise AssertionError(f"unexpected object appeared at {bucket}/{key}: {last}")
            time.sleep(self.args.poll_interval)
        detail = {"durationSeconds": duration, "observations": observations, "last": last}
        self.events.record("negative-window.complete", case=case_id, **detail)
        return detail

    def setup_bucket_replication(
        self,
        case_id: str,
        *,
        flags: str | None = None,
        prefix: str | None = None,
        synchronous: bool = False,
        preexisting: tuple[str, bytes] | None = None,
    ) -> tuple[str, str, dict[str, Any]]:
        source = self.bucket_name(case_id, "src")
        target = self.bucket_name(case_id, "dst")
        self.create_bucket(self.client_a, source)
        self.create_bucket(self.client_b, target)
        self.enable_versioning(self.client_a, source)
        self.enable_versioning(self.client_b, target)
        if preexisting:
            key, body = preexisting
            self.client_a.put_object(Bucket=source, Key=key, Body=body)
        arguments = [
            "bucket",
            "replication",
            "add",
            f"{self.alias_a}/{source}",
            "--remote-bucket",
            f"{self.alias_b}/{target}",
            "--id",
            f"rule-{hashlib.sha256(case_id.encode()).hexdigest()[:8]}",
            "--healthcheck-seconds",
            "1",
            "--force",
        ]
        if flags:
            arguments.extend(("--replicate", flags))
        if prefix:
            arguments.extend(("--prefix", prefix))
        if synchronous:
            arguments.append("--sync")
        self.run_rc(case_id, "replication-add", arguments)
        config = self.client_a.get_bucket_replication(Bucket=source).get("ReplicationConfiguration", {})
        self.save_json(case_id, "replication-configuration.json", config)
        if not config.get("Rules"):
            raise AssertionError("replication configuration has no rules")
        return source, target, config

    @staticmethod
    def delete_replication_configuration(client: Any, bucket: str) -> None:
        try:
            client.delete_bucket_replication(Bucket=bucket)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {
                "NoSuchBucket",
                "ReplicationConfigurationNotFoundError",
                "NoSuchReplicationConfiguration",
            }:
                raise

    @staticmethod
    def delete_object_batch(client: Any, bucket: str, items: list[dict[str, str]]) -> None:
        for offset in range(0, len(items), 1000):
            response = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": items[offset : offset + 1000], "Quiet": True},
            )
            errors = response.get("Errors", [])
            if errors:
                raise RuntimeError(f"delete_objects reported errors: {errors[:5]}")

    def inspect_bucket_cleanup_inventory(
        self,
        client: Any,
        bucket: str,
    ) -> dict[str, Any]:
        inventory: dict[str, Any] = {"bucket": bucket}
        try:
            uploads = client.list_multipart_uploads(Bucket=bucket)
            inventory["multipartUploads"] = [
                {"key": item.get("Key"), "uploadId": item.get("UploadId")}
                for item in uploads.get("Uploads", [])[:20]
            ]
            inventory["multipartUploadsTruncated"] = bool(
                uploads.get("IsTruncated")
            )
        except Exception as exc:
            inventory["multipartInspectionError"] = f"{type(exc).__name__}: {exc}"
        try:
            version_count = 0
            marker_count = 0
            version_samples: list[dict[str, Any]] = []
            for page in client.get_paginator("list_object_versions").paginate(
                Bucket=bucket
            ):
                versions = page.get("Versions", [])
                markers = page.get("DeleteMarkers", [])
                version_count += len(versions)
                marker_count += len(markers)
                for kind, items in (("version", versions), ("delete-marker", markers)):
                    for item in items:
                        if len(version_samples) < 20:
                            version_samples.append(
                                {
                                    "kind": kind,
                                    "key": item.get("Key"),
                                    "versionId": item.get("VersionId"),
                                }
                            )
            inventory["versions"] = version_count
            inventory["deleteMarkers"] = marker_count
            inventory["versionSamples"] = version_samples
        except Exception as exc:
            inventory["versionInspectionError"] = f"{type(exc).__name__}: {exc}"
        try:
            object_count = 0
            object_samples: list[str] = []
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket
            ):
                objects = page.get("Contents", [])
                object_count += len(objects)
                for item in objects:
                    if len(object_samples) < 20:
                        object_samples.append(str(item.get("Key", "")))
            inventory["objects"] = object_count
            inventory["objectSamples"] = object_samples
        except Exception as exc:
            inventory["objectInspectionError"] = f"{type(exc).__name__}: {exc}"
        return inventory

    def cleanup_bucket_contents(self, client: Any, bucket: str) -> bool:
        """Empty one test bucket without deleting its namespace.

        Site-replicated peers with the same bucket name must all cross this
        barrier before either namespace is removed. Otherwise the first
        DeleteBucket hook can observe live versions on its peer and persist a
        destructive retry liability that contaminates later cases.
        """

        deadline = time.monotonic() + 30
        last_error: BaseException | None = None
        changed = False
        while time.monotonic() < deadline:
            try:
                uploads = client.list_multipart_uploads(Bucket=bucket).get(
                    "Uploads",
                    [],
                )
                for upload in uploads:
                    changed = True
                    client.abort_multipart_upload(
                        Bucket=bucket,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )

                version_items: list[dict[str, str]] = []
                for page in client.get_paginator("list_object_versions").paginate(
                    Bucket=bucket
                ):
                    version_items.extend(
                        {"Key": item["Key"], "VersionId": item["VersionId"]}
                        for group in ("Versions", "DeleteMarkers")
                        for item in page.get(group, [])
                    )
                if version_items:
                    changed = True
                    self.delete_object_batch(client, bucket, version_items)

                object_items: list[dict[str, str]] = []
                for page in client.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket
                ):
                    object_items.extend(
                        {"Key": item["Key"]} for item in page.get("Contents", [])
                    )
                if object_items:
                    changed = True
                    self.delete_object_batch(client, bucket, object_items)

                if not uploads and not version_items and not object_items:
                    return changed
                last_error = RuntimeError(
                    "bucket contents did not converge to empty"
                )
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code == "NoSuchBucket":
                    return changed
                if code != "BucketNotEmpty":
                    raise
                last_error = exc
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(self.args.poll_interval, remaining))
        raise BucketCleanupResidual(
            bucket,
            last_error,
            self.inspect_bucket_cleanup_inventory(client, bucket),
        )

    def delete_empty_bucket(self, client: Any, bucket: str) -> None:
        deadline = time.monotonic() + 30
        last_error: BaseException | None = None
        while time.monotonic() < deadline:
            try:
                client.delete_bucket(Bucket=bucket)
                return
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code == "NoSuchBucket":
                    return
                if code != "BucketNotEmpty":
                    raise
                last_error = exc
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(self.args.poll_interval, remaining))
        raise BucketCleanupResidual(
            bucket,
            last_error,
            self.inspect_bucket_cleanup_inventory(client, bucket),
        )

    def cleanup_bucket(self, client: Any, bucket: str) -> None:
        """Compatibility wrapper for callers cleaning one independent bucket."""

        self.delete_replication_configuration(client, bucket)
        self.cleanup_bucket_contents(client, bucket)
        self.delete_empty_bucket(client, bucket)

    def site_retry_stats_snapshot(self) -> dict[str, Any]:
        return {
            site.id: self.site_info(site).get("retryStats")
            for site in (self.site_a, self.site_b)
        }

    @staticmethod
    def normalized_retry_stats(value: Any) -> tuple[int, int, str] | None:
        if value is None or value == {}:
            return 0, 0, ""
        if not isinstance(value, dict):
            return None
        pending = value.get("pending", 0)
        failed = value.get("failed", 0)
        last_error = value.get("lastError", "")
        if (
            not isinstance(pending, int)
            or isinstance(pending, bool)
            or pending < 0
            or not isinstance(failed, int)
            or isinstance(failed, bool)
            or failed < 0
            or not isinstance(last_error, str)
        ):
            return None
        return pending, failed, last_error

    @classmethod
    def site_retry_state_regressions(
        cls,
        before: dict[str, Any],
        after: dict[str, Any],
    ) -> list[dict[str, Any]]:
        regressions: list[dict[str, Any]] = []
        for site_id in ("site-a", "site-b"):
            before_raw = before.get(site_id)
            after_raw = after.get(site_id)
            before_stats = cls.normalized_retry_stats(before_raw)
            after_stats = cls.normalized_retry_stats(after_raw)
            if after_stats is None:
                regressions.append(
                    {
                        "site": site_id,
                        "reason": "post-cleanup retryStats is malformed",
                        "before": before_raw,
                        "after": after_raw,
                    }
                )
                continue
            if before_raw == after_raw:
                continue
            after_pending, after_failed, after_error = after_stats
            if after_pending == 0 and after_failed == 0 and not after_error:
                continue
            if before_stats is None:
                regressions.append(
                    {
                        "site": site_id,
                        "reason": "retry state changed from an unverifiable baseline",
                        "before": before_raw,
                        "after": after_raw,
                    }
                )
                continue
            before_pending, before_failed, before_error = before_stats
            improved = (
                after_pending <= before_pending
                and after_failed <= before_failed
                and (after_pending < before_pending or after_failed < before_failed)
            )
            unchanged = (
                after_pending == before_pending
                and after_failed == before_failed
                and after_error == before_error
            )
            if not improved and not unchanged:
                regressions.append(
                    {
                        "site": site_id,
                        "reason": "cleanup introduced or worsened retry debt",
                        "before": before_raw,
                        "after": after_raw,
                    }
                )
        return regressions

    def cleanup_target(self, client: Any, bucket: str) -> dict[str, str]:
        if client is self.client_a:
            site = "site-a"
        elif client is self.client_b:
            site = "site-b"
        else:
            site = "unknown"
        return {"site": site, "bucket": bucket}

    def tracked_bucket_summaries(self) -> list[dict[str, str]]:
        summaries: list[dict[str, str]] = []
        seen: set[tuple[int, str]] = set()
        for client, bucket in self.created_buckets:
            identity = (id(client), bucket)
            if identity in seen:
                continue
            seen.add(identity)
            summaries.append(self.cleanup_target(client, bucket))
        return summaries

    def cleanup_created_buckets(self, case_id: str) -> None:
        errors: list[dict[str, Any]] = []
        residuals: list[dict[str, Any]] = []
        unique: list[tuple[Any, str]] = []
        seen: set[tuple[int, str]] = set()
        for client, bucket in self.created_buckets:
            identity = (id(client), bucket)
            if identity not in seen:
                seen.add(identity)
                unique.append((client, bucket))
        if self.args.module == "site":
            # A site bucket may reach its peer immediately before a failed
            # wait returns, so the peer is not necessarily in created_buckets.
            # Both deterministic test namespaces remain in cleanup scope.
            bucket_names = list(dict.fromkeys(bucket for _, bucket in unique))
            unique = [
                (client, bucket)
                for bucket in bucket_names
                for client in (self.client_a, self.client_b)
            ]
        remaining = {(id(client), bucket) for client, bucket in unique}
        retry_check_required = self.args.module == "site" and bool(unique)
        retry_before: dict[str, Any] | None = None
        retry_after: dict[str, Any] | None = None
        retry_regressions: list[dict[str, Any]] = []
        retry_isolation_unverified = False
        if retry_check_required:
            try:
                retry_before = self.site_retry_stats_snapshot()
            except Exception as exc:
                retry_isolation_unverified = True
                errors.append(
                    {
                        "site": "site-group",
                        "phase": "retry-state-before-cleanup",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    }
                )

        # Remove operator-authored replication configuration while every
        # tracked peer bucket still exists. RustFS deliberately retains the
        # derived site-replication rules, so this step alone is not an
        # all-clear for namespace deletion.
        for client, bucket in unique:
            try:
                self.delete_replication_configuration(client, bucket)
            except Exception as exc:
                errors.append(
                    {
                        **self.cleanup_target(client, bucket),
                        "phase": "disable-replication",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    }
                )

        bucket_sites: dict[str, set[int]] = {}
        for client, bucket in unique:
            bucket_sites.setdefault(bucket, set()).add(id(client))
        shared_site_bucket = any(len(clients) > 1 for clients in bucket_sites.values())
        contents_clean = True
        stable_empty_rounds = 0
        content_barrier_deadline = time.monotonic() + 30
        while contents_clean:
            round_changed = False
            for client, bucket in unique:
                try:
                    round_changed = (
                        self.cleanup_bucket_contents(client, bucket)
                        or round_changed
                    )
                except BucketCleanupResidual as exc:
                    contents_clean = False
                    residuals.append(
                        {
                            "phase": "empty-contents",
                            **exc.inventory,
                            **self.cleanup_target(client, bucket),
                        }
                    )
                except Exception as exc:
                    contents_clean = False
                    errors.append(
                        {
                            **self.cleanup_target(client, bucket),
                            "phase": "empty-contents",
                            "errorType": type(exc).__name__,
                            "message": str(exc),
                        }
                    )
            if not contents_clean or not shared_site_bucket:
                break
            stable_empty_rounds = 0 if round_changed else stable_empty_rounds + 1
            if stable_empty_rounds >= 2:
                break
            remaining_time = content_barrier_deadline - time.monotonic()
            if remaining_time <= 0:
                contents_clean = False
                errors.append(
                    {
                        "site": "site-group",
                        "phase": "empty-contents-stability",
                        "errorType": "BucketCleanupStabilityTimeout",
                        "message": (
                            "shared site bucket contents did not remain empty "
                            "across two consecutive cleanup rounds"
                        ),
                        "inventories": [
                            {
                                **self.inspect_bucket_cleanup_inventory(
                                    client,
                                    bucket,
                                ),
                                **self.cleanup_target(client, bucket),
                            }
                            for client, bucket in unique
                        ],
                    }
                )
                break
            time.sleep(min(self.args.poll_interval, remaining_time))

        # An all-content barrier prevents a peer DeleteBucket hook from seeing
        # live versions on the other side. If any side is uncertain, retain
        # every namespace and let the existing isolation failure path preserve
        # evidence instead of creating destructive retry debt.
        if contents_clean:
            for client, bucket in reversed(unique):
                identity = (id(client), bucket)
                try:
                    self.delete_empty_bucket(client, bucket)
                except BucketCleanupResidual as exc:
                    residuals.append(
                        {
                            "phase": "delete-bucket",
                            **exc.inventory,
                            **self.cleanup_target(client, bucket),
                        }
                    )
                except Exception as exc:
                    errors.append(
                        {
                            **self.cleanup_target(client, bucket),
                            "phase": "delete-bucket",
                            "errorType": type(exc).__name__,
                            "message": str(exc),
                        }
                    )
                else:
                    remaining.discard(identity)

        if retry_check_required:
            try:
                retry_after = self.site_retry_stats_snapshot()
            except Exception as exc:
                retry_isolation_unverified = True
                errors.append(
                    {
                        "site": "site-group",
                        "phase": "retry-state-after-cleanup",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    }
                )
            if retry_before is not None and retry_after is not None:
                retry_regressions = self.site_retry_state_regressions(
                    retry_before,
                    retry_after,
                )
                if retry_regressions:
                    errors.append(
                        {
                            "site": "site-group",
                            "phase": "retry-state-isolation",
                            "errorType": "CleanupRetryStateRegression",
                            "message": (
                                "logical cleanup introduced or worsened site "
                                "replication retry debt"
                            ),
                        }
                    )

        self.created_buckets = [
            (client, bucket)
            for client, bucket in unique
            if (id(client), bucket) in remaining
        ]
        if residuals or errors:
            tracked_buckets = self.tracked_bucket_summaries()
            retry_isolation_failed = bool(retry_regressions) or (
                retry_check_required and retry_isolation_unverified
            )
            isolation_restored = not tracked_buckets and not retry_isolation_failed
            if isolation_restored:
                self.isolation_failure = None
            evidence = self.save_json(
                case_id,
                "cleanup-failure.json",
                {
                    "classification": "logical-cleanup-failure",
                    "authoritativeCleanup": "replication phase data reset",
                    "isolationRestored": isolation_restored,
                    "trackedBuckets": tracked_buckets,
                    "residuals": residuals,
                    "errors": errors,
                    "retryState": (
                        {
                            "before": retry_before,
                            "after": retry_after,
                            "regressions": retry_regressions,
                            "verified": not retry_isolation_unverified,
                        }
                        if retry_check_required
                        else None
                    ),
                },
            )
            self.events.record(
                "case.cleanup.failure",
                case=case_id,
                evidence=evidence,
                isolationRestored=isolation_restored,
                residualBuckets=[
                    {"site": item["site"], "bucket": item["bucket"]}
                    for item in residuals
                ],
                errorCount=len(errors),
                trackedBuckets=tracked_buckets,
            )
            raise LogicalCleanupFailure(
                residuals=residuals,
                errors=errors,
                evidence=evidence,
                tracked_buckets=tracked_buckets,
                isolation_restored=isolation_restored,
            )
        self.isolation_failure = None

    # ------------------------------------------------------------------
    # Bucket replication cases
    # ------------------------------------------------------------------

    def case_bucket_source_versioning_required(self, case_id: str) -> list[str]:
        source = self.bucket_name(case_id, "src")
        target = self.bucket_name(case_id, "dst")
        self.create_bucket(self.client_a, source)
        self.create_bucket(self.client_b, target)
        self.enable_versioning(self.client_b, target)
        result = self.run_rc(
            case_id,
            "source-versioning-required",
            [
                "bucket",
                "replication",
                "add",
                f"{self.alias_a}/{source}",
                "--remote-bucket",
                f"{self.alias_b}/{target}",
                "--force",
            ],
            expect_success=False,
        )
        error = (result.stdout + result.stderr).lower()
        if "version" not in error and "replication" not in error:
            raise AssertionError(f"unexpected source-versioning error: {error[:500]}")
        try:
            self.client_a.get_bucket_replication(Bucket=source)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {
                "ReplicationConfigurationNotFoundError",
                "NoSuchReplicationConfiguration",
                "NoSuchConfiguration",
            }:
                raise
        else:
            raise AssertionError("rejected source acquired a replication configuration")
        return ["cases/%s/source-versioning-required.json" % safe_name(case_id)]

    def case_bucket_target_versioning_required(self, case_id: str) -> list[str]:
        source = self.bucket_name(case_id, "src")
        target = self.bucket_name(case_id, "dst")
        self.create_bucket(self.client_a, source)
        self.create_bucket(self.client_b, target)
        self.enable_versioning(self.client_a, source)
        result = self.run_rc(
            case_id,
            "target-versioning-required",
            [
                "bucket",
                "replication",
                "add",
                f"{self.alias_a}/{source}",
                "--remote-bucket",
                f"{self.alias_b}/{target}",
                "--force",
            ],
            expect_success=False,
        )
        error = (result.stdout + result.stderr).lower()
        if "version" not in error and "remote" not in error:
            raise AssertionError(f"unexpected target-versioning error: {error[:500]}")
        return ["cases/%s/target-versioning-required.json" % safe_name(case_id)]

    def case_bucket_configuration_roundtrip(self, case_id: str) -> list[str]:
        source, target, config = self.setup_bucket_replication(
            case_id, flags="delete,delete-marker,existing-objects", prefix="roundtrip/"
        )
        list_result = self.run_rc(
            case_id,
            "replication-list",
            ["bucket", "replication", "list", f"{self.alias_a}/{source}", "--force"],
        )
        combined = list_result.stdout
        rule = config["Rules"][0]
        rule_id = rule.get("ID", "")
        if source not in combined and rule_id not in combined:
            raise AssertionError("rc list did not identify the configured rule")
        destination = rule.get("Destination", {}).get("Bucket", "")
        if not destination or target not in destination:
            raise AssertionError(f"destination does not identify target bucket: {destination!r}")
        if rule.get("Priority") != 1 or rule.get("Status") != "Enabled":
            raise AssertionError(f"replication rule priority/status drifted: {rule}")
        if rule.get("Filter", {}).get("Prefix") != "roundtrip/":
            raise AssertionError(f"replication prefix did not round-trip: {rule}")
        if rule.get("DeleteMarkerReplication", {}).get("Status") != "Enabled":
            raise AssertionError("delete-marker flag did not round-trip")
        if rule.get("ExistingObjectReplication", {}).get("Status") != "Enabled":
            raise AssertionError("existing-object flag did not round-trip")
        listed = self.json_body(list_result.stdout.encode("utf-8"))
        listed_rules = listed.get("rules", []) if isinstance(listed, dict) else []
        listed_rule = next(
            (item for item in listed_rules if item.get("id") == rule_id), None
        )
        if not listed_rule:
            raise AssertionError(f"rc list omitted configured rule {rule_id!r}")
        expected_flags = {
            "deleteReplication": True,
            "deleteMarkerReplication": True,
            "existingObjectReplication": True,
        }
        if any(listed_rule.get(name) is not value for name, value in expected_flags.items()):
            raise AssertionError(f"rc list flag round-trip mismatch: {listed_rule}")
        self.client_a.delete_bucket_replication(Bucket=source)
        try:
            self.client_a.get_bucket_replication(Bucket=source)
        except ClientError:
            pass
        else:
            raise AssertionError("replication configuration remained after delete")
        return [
            self.save_json(case_id, "roundtrip-final.json", {"ruleId": rule_id, "destination": destination})
        ]

    def case_bucket_put_object(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id)
        key = "objects/basic.bin"
        body = secrets.token_bytes(64 * 1024)
        response = self.client_a.put_object(
            Bucket=source, Key=key, Body=body, Metadata={"validation": "bucket-put"}
        )
        version = response.get("VersionId")
        final = self.wait_object(
            case_id, self.client_b, target, key, body, expected_version=version
        )
        if final.get("metadata", {}).get("validation") != "bucket-put":
            raise AssertionError("object metadata did not replicate")
        return [self.save_json(case_id, "object-final.json", final)]

    def case_bucket_sync_put_object(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id, synchronous=True)
        key = "sync/immediate.txt"
        body = b"synchronous-replication-check"
        started = time.monotonic()
        response = self.client_a.put_object(Bucket=source, Key=key, Body=body)
        elapsed = time.monotonic() - started
        try:
            target_response = self.client_b.get_object(Bucket=target, Key=key)
        except ClientError as exc:
            raise AssertionError("synchronous PUT returned before target was readable") from exc
        target_body = target_response["Body"].read()
        if target_body != body or target_response.get("VersionId") != response.get("VersionId"):
            raise AssertionError("synchronous target object differs from source response")
        return [
            self.save_json(
                case_id,
                "sync-put.json",
                {
                    "elapsedSeconds": elapsed,
                    "sourceVersionId": response.get("VersionId"),
                    "targetVersionId": target_response.get("VersionId"),
                    "sha256": hashlib.sha256(target_body).hexdigest(),
                },
            )
        ]

    def case_bucket_prefix_filter(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id, prefix="included/")
        included_key = "included/object.txt"
        excluded_key = "excluded/control.txt"
        included = b"prefix-match"
        self.client_a.put_object(Bucket=source, Key=included_key, Body=included)
        self.client_a.put_object(Bucket=source, Key=excluded_key, Body=b"prefix-control")
        final = self.wait_object(case_id, self.client_b, target, included_key, included)
        negative = self.assert_absent_for(
            case_id, self.client_b, target, excluded_key, self.args.negative_window
        )
        return [
            self.save_json(case_id, "prefix-match.json", final),
            self.save_json(case_id, "prefix-control.json", negative),
        ]

    def case_bucket_tag_filter(self, case_id: str) -> list[str]:
        source, target, config = self.setup_bucket_replication(case_id)
        rule = config["Rules"][0]
        destination = rule["Destination"]["Bucket"]
        filtered = {
            "Role": config.get("Role", destination),
            "Rules": [
                {
                    "ID": "tag-filter",
                    "Priority": 1,
                    "Filter": {"Tag": {"Key": "class", "Value": "gold"}},
                    "Status": "Enabled",
                    "Destination": {"Bucket": destination},
                    "DeleteMarkerReplication": {"Status": "Disabled"},
                    "ExistingObjectReplication": {"Status": "Disabled"},
                }
            ],
        }
        self.client_a.put_bucket_replication(Bucket=source, ReplicationConfiguration=filtered)
        self.save_json(case_id, "tag-replication-configuration.json", filtered)
        matching_key = "tags/gold.txt"
        control_key = "tags/silver.txt"
        matching = b"gold-object"
        self.client_a.put_object(Bucket=source, Key=matching_key, Body=matching, Tagging="class=gold")
        self.client_a.put_object(Bucket=source, Key=control_key, Body=b"silver-object", Tagging="class=silver")
        final = self.wait_object(case_id, self.client_b, target, matching_key, matching)
        negative = self.assert_absent_for(
            case_id, self.client_b, target, control_key, self.args.negative_window
        )
        return [
            self.save_json(case_id, "tag-match.json", final),
            self.save_json(case_id, "tag-control.json", negative),
        ]

    def case_bucket_zero_byte_special_key(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id)
        fixtures = {
            "zero/empty.bin": b"",
            "special/space key.txt": b"space",
            "special/中文-对象.txt": "你好 RustFS".encode("utf-8"),
            "special/plus+percent%question?.txt": b"reserved",
        }
        manifest: list[dict[str, Any]] = []
        for key, body in fixtures.items():
            response = self.client_a.put_object(Bucket=source, Key=key, Body=body)
            final = self.wait_object(
                case_id,
                self.client_b,
                target,
                key,
                body,
                expected_version=response.get("VersionId"),
            )
            manifest.append({"key": key, **final})
        return [self.save_json(case_id, "special-key-manifest.json", manifest)]

    def case_bucket_multipart_object(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id)
        key = "multipart/complete.bin"
        parts = [b"a" * (5 * 1024 * 1024), b"b" * (1024 * 1024)]
        created = self.client_a.create_multipart_upload(Bucket=source, Key=key)
        uploaded = []
        for number, body in enumerate(parts, 1):
            response = self.client_a.upload_part(
                Bucket=source,
                Key=key,
                UploadId=created["UploadId"],
                PartNumber=number,
                Body=body,
            )
            uploaded.append({"PartNumber": number, "ETag": response["ETag"]})
        complete = self.client_a.complete_multipart_upload(
            Bucket=source,
            Key=key,
            UploadId=created["UploadId"],
            MultipartUpload={"Parts": uploaded},
        )
        body = b"".join(parts)
        final = self.wait_object(
            case_id,
            self.client_b,
            target,
            key,
            body,
            expected_version=complete.get("VersionId"),
        )
        return [
            self.save_json(
                case_id,
                "multipart-final.json",
                {"uploadId": created["UploadId"], "parts": uploaded, "object": final},
            )
        ]

    def case_bucket_copy_object(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id)
        original_key = "copy/original.txt"
        copy_key = "copy/clone.txt"
        body = b"copy-object-replication"
        self.client_a.put_object(Bucket=source, Key=original_key, Body=body)
        copied = self.client_a.copy_object(
            Bucket=source,
            Key=copy_key,
            CopySource={"Bucket": source, "Key": original_key},
            Metadata={"copied": "true"},
            MetadataDirective="REPLACE",
        )
        final = self.wait_object(
            case_id,
            self.client_b,
            target,
            copy_key,
            body,
            expected_version=copied.get("VersionId"),
        )
        if final.get("metadata", {}).get("copied") != "true":
            raise AssertionError("CopyObject metadata did not replicate")
        return [self.save_json(case_id, "copy-final.json", final)]

    def case_bucket_delete_marker(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id, flags="delete-marker")
        key = "delete-marker/object.txt"
        put = self.client_a.put_object(Bucket=source, Key=key, Body=b"delete-marker")
        self.wait_object(
            case_id, self.client_b, target, key, b"delete-marker", expected_version=put.get("VersionId")
        )
        deleted = self.client_a.delete_object(Bucket=source, Key=key)
        marker_id = deleted.get("VersionId")

        def probe() -> tuple[bool, Any]:
            response = self.client_b.list_object_versions(Bucket=target, Prefix=key)
            markers = [
                item["VersionId"]
                for item in response.get("DeleteMarkers", [])
                if item["Key"] == key
            ]
            versions = [
                item["VersionId"] for item in response.get("Versions", []) if item["Key"] == key
            ]
            return marker_id in markers and put.get("VersionId") in versions, {
                "deleteMarkers": markers,
                "versions": versions,
                "expectedMarker": marker_id,
            }

        final = self.wait_until(case_id, "delete marker replication", probe)
        return [self.save_json(case_id, "delete-marker-final.json", final)]

    def case_bucket_version_delete(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id, flags="delete")
        key = "version-delete/object.txt"
        first = self.client_a.put_object(Bucket=source, Key=key, Body=b"version-one")
        second = self.client_a.put_object(Bucket=source, Key=key, Body=b"version-two")
        self.wait_object(
            case_id, self.client_b, target, key, b"version-two", expected_version=second.get("VersionId")
        )
        self.client_a.delete_object(Bucket=source, Key=key, VersionId=first["VersionId"])

        def probe() -> tuple[bool, Any]:
            response = self.client_b.list_object_versions(Bucket=target, Prefix=key)
            versions = [
                item["VersionId"] for item in response.get("Versions", []) if item["Key"] == key
            ]
            return first["VersionId"] not in versions and second["VersionId"] in versions, {
                "versions": versions,
                "deleted": first["VersionId"],
                "retained": second["VersionId"],
            }

        final = self.wait_until(case_id, "permanent version delete replication", probe)
        return [self.save_json(case_id, "version-delete-final.json", final)]

    def case_bucket_existing_object_enabled(self, case_id: str) -> list[str]:
        key = "existing/before-rule.txt"
        body = b"existing-object-enabled"
        source, target, _ = self.setup_bucket_replication(
            case_id, flags="existing-objects", preexisting=(key, body)
        )
        source_version = self.client_a.head_object(Bucket=source, Key=key).get("VersionId")
        final = self.wait_object(
            case_id,
            self.client_b,
            target,
            key,
            body,
            expected_version=source_version,
            timeout=max(self.args.timeout, 240),
        )
        return [self.save_json(case_id, "existing-object-final.json", final)]

    def case_bucket_existing_object_disabled(self, case_id: str) -> list[str]:
        key = "existing/before-rule.txt"
        body = b"existing-object-disabled"
        source, target, _ = self.setup_bucket_replication(case_id, preexisting=(key, body))
        new_key = "existing/after-rule.txt"
        new_body = b"new-object-positive-control"
        self.client_a.put_object(Bucket=source, Key=new_key, Body=new_body)
        positive = self.wait_object(case_id, self.client_b, target, new_key, new_body)
        negative = self.assert_absent_for(
            case_id, self.client_b, target, key, max(self.args.negative_window, 20)
        )
        return [
            self.save_json(case_id, "new-object-control.json", positive),
            self.save_json(case_id, "existing-object-control.json", negative),
        ]

    def case_bucket_target_check(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(
            case_id, flags="delete,delete-marker"
        )
        status, headers, body = self.signed_request(
            self.site_a, "GET", f"/{source}", query="replication-check"
        )
        evidence = {
            "httpStatus": status,
            "headers": headers,
            "body": body.decode("utf-8", errors="replace"),
        }
        if status != 200:
            raise AssertionError(f"replication-check returned HTTP {status}: {evidence['body'][:500]}")
        if not body.strip():
            raise AssertionError("replication-check returned an empty response")
        parsed = self.json_body(body)
        evidence["parsed"] = parsed
        overall = str(parsed.get("Status", parsed.get("status", ""))).upper()
        if overall != "OK":
            raise AssertionError(f"replication-check reported {overall}: {parsed}")
        if parsed.get("ActiveMutation") is not True:
            raise AssertionError("replication-check did not identify its active mutation")
        if parsed.get("ProbeNamespace") != ".rustfs.sys/replication-check/":
            raise AssertionError(f"unexpected probe namespace: {parsed.get('ProbeNamespace')!r}")
        targets = parsed.get("Targets", [])
        if not targets:
            raise AssertionError("replication-check returned no target results")
        required_phases = {
            "Bucket",
            "Versioning",
            "ObjectLock",
            "Put",
            "VersionFidelity",
            "DeleteMarker",
            "VersionDelete",
            "Cleanup",
        }
        for item in targets:
            if str(item.get("Status", "")).upper() != "OK":
                raise AssertionError(f"replication-check target failed: {item}")
            phases = item.get("Phases", {})
            missing = required_phases - set(phases)
            failed = {
                name: value
                for name, value in phases.items()
                if name in required_phases and str(value.get("Status", "")).upper() != "OK"
            }
            if missing or failed:
                raise AssertionError(
                    f"replication-check phase mismatch: missing={sorted(missing)} failed={failed}"
                )
        leftovers = self.client_b.list_object_versions(
            Bucket=target, Prefix=".rustfs.sys/replication-check/"
        )
        if leftovers.get("Versions") or leftovers.get("DeleteMarkers"):
            raise AssertionError("replication-check left probe artifacts on the target")
        return [
            self.save_json(case_id, "replication-check.json", evidence),
            self.save_json(case_id, "replication-check-leftovers.json", leftovers),
        ]

    def case_bucket_status_observability(self, case_id: str) -> list[str]:
        source, target, config = self.setup_bucket_replication(case_id)
        body = b"status-observability"
        put = self.client_a.put_object(Bucket=source, Key="status/object.txt", Body=body)
        self.wait_object(
            case_id,
            self.client_b,
            target,
            "status/object.txt",
            body,
            expected_version=put.get("VersionId"),
        )
        listed = self.run_rc(
            case_id,
            "status-list",
            ["bucket", "replication", "list", f"{self.alias_a}/{source}", "--force"],
        )
        raw_status, raw_headers, raw_body = self.admin_request(
            self.site_a,
            "GET",
            "replicationmetrics",
            query=urllib.parse.urlencode({"bucket": source}),
        )
        raw_document = {
            "httpStatus": raw_status,
            "headers": raw_headers,
            "body": raw_body.decode("utf-8", errors="replace"),
        }
        if raw_status != 200:
            raise AssertionError(f"raw replication metrics returned HTTP {raw_status}")
        raw_document["parsed"] = self.json_body(raw_body)
        self.save_json(case_id, "raw-replication-metrics.json", raw_document)
        status_arguments = [
            "bucket",
            "replication",
            "status",
            f"{self.alias_a}/{source}",
            "--force",
        ]

        def metrics_probe() -> tuple[bool, Any]:
            observed = self.rc_probe(status_arguments)
            if observed.returncode != 0:
                try:
                    error_document = json.loads(observed.stderr)
                except json.JSONDecodeError:
                    error_document = {}
                details = error_document.get("error", {}) if isinstance(error_document, dict) else {}
                if details and details.get("retryable") is False:
                    raise AssertionError(
                        "rc could not decode the server replication metrics response: "
                        f"{details.get('message', observed.stderr[:1000])}"
                    )
                return False, {
                    "exitCode": observed.returncode,
                    "stderr": observed.stderr[:1000],
                }
            try:
                document = json.loads(observed.stdout)
            except json.JSONDecodeError:
                return False, {"stdout": observed.stdout[:1000]}
            return replication_metrics_ready(document), document

        metrics = self.wait_until(case_id, "non-zero replication status metrics", metrics_probe)
        self.save_json(case_id, "status-metrics.json", metrics)
        combined = listed.stdout + json.dumps(metrics)
        identifiers = [source, config["Rules"][0].get("ID", ""), target]
        if not any(identifier and identifier in combined for identifier in identifiers):
            raise AssertionError("rc replication output contains no configured identifier")
        if self.args.secret_key in combined:
            raise AssertionError("rc replication status leaked the administrator secret")
        return [
            self.save_json(
                case_id,
                "observability-summary.json",
                {
                    "identifiers": identifiers,
                    "outputBytes": len(combined),
                    "metrics": metrics,
                },
            )
        ]

    def case_bucket_outage_recovery(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id)
        key = "outage/recover.txt"
        body = b"target-outage-recovery"
        evidence = [self.hook(case_id, "site-b-stop", self.args.site_b_stop_hook)]
        try:
            response = self.client_a.put_object(Bucket=source, Key=key, Body=body)
        finally:
            evidence.append(self.hook(case_id, "site-b-start", self.args.site_b_start_hook))
            self.wait_api(case_id, self.client_b, "site-b")
        final = self.wait_object(
            case_id,
            self.client_b,
            target,
            key,
            body,
            expected_version=response.get("VersionId"),
            timeout=max(self.args.timeout, 180),
        )
        evidence.append(self.save_json(case_id, "outage-recovery-final.json", final))
        return evidence

    def case_bucket_source_restart_recovery(self, case_id: str) -> list[str]:
        source, target, _ = self.setup_bucket_replication(case_id)
        key = "mrf/restart.txt"
        body = b"source-restart-mrf-recovery"
        evidence = [self.hook(case_id, "site-b-stop", self.args.site_b_stop_hook)]
        b_started = False
        try:
            response = self.client_a.put_object(Bucket=source, Key=key, Body=body)
            evidence.append(self.hook(case_id, "site-a-restart", self.args.site_a_restart_hook))
            self.wait_api(case_id, self.client_a, "site-a")
            evidence.append(self.hook(case_id, "site-b-start", self.args.site_b_start_hook))
            b_started = True
            self.wait_api(case_id, self.client_b, "site-b")
        finally:
            if not b_started:
                self.hook(case_id, "site-b-start-recovery", self.args.site_b_start_hook)
                self.wait_api(case_id, self.client_b, "site-b")
        final = self.wait_object(
            case_id,
            self.client_b,
            target,
            key,
            body,
            expected_version=response.get("VersionId"),
            timeout=max(self.args.timeout, 240),
        )
        evidence.append(self.save_json(case_id, "source-restart-final.json", final))
        return evidence

    # ------------------------------------------------------------------
    # Site replication cases
    # ------------------------------------------------------------------

    def site_info(self, site: Site) -> dict[str, Any]:
        status, _, body = self.admin_request(site, "GET", "site-replication/info")
        if status != 200:
            raise AssertionError(
                f"{site.id} site-replication info returned HTTP {status}: "
                f"{body.decode(errors='replace')[:500]}"
            )
        parsed = self.json_body(body)
        if not isinstance(parsed, dict):
            raise AssertionError(f"{site.id} info is not an object")
        return parsed

    def site_group_snapshot(self) -> tuple[str, str, dict[str, Any]]:
        info = {
            site.id: self.site_info(site)
            for site in (self.site_a, self.site_b)
        }
        state, reason = classify_site_group_state(
            info,
            (self.site_a.endpoint, self.site_b.endpoint),
        )
        return state, reason, info

    def wait_site_group_active(self, case_id: str) -> dict[str, Any]:
        def probe() -> tuple[bool, Any]:
            try:
                state, reason, info = self.site_group_snapshot()
            except Exception as exc:
                return False, {
                    "state": "unavailable",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            detail = {"state": state, "reason": reason, "sites": info}
            return state == "active", detail

        return self.wait_until(
            case_id,
            "stable two-site replication group",
            probe,
            timeout=max(self.args.timeout, 120),
        )

    def wait_site_group_disabled(self, case_id: str) -> dict[str, Any]:
        def probe() -> tuple[bool, Any]:
            try:
                state, reason, info = self.site_group_snapshot()
            except Exception as exc:
                return False, {
                    "state": "unavailable",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            detail = {"state": state, "reason": reason, "sites": info}
            return state == "disabled", detail

        return self.wait_until(
            case_id,
            "cleanly disabled two-site replication group",
            probe,
            timeout=max(self.args.timeout, 120),
        )

    def wait_site_group_ilm_expiry_value(
        self,
        case_id: str,
        expected: bool,
    ) -> dict[str, Any]:
        def probe() -> tuple[bool, Any]:
            try:
                state, state_reason, info = self.site_group_snapshot()
            except Exception as exc:
                return False, {
                    "state": "unavailable",
                    "stateReason": f"{type(exc).__name__}: {exc}",
                }
            valid, flag_reason = site_group_has_ilm_expiry_value(info, expected)
            detail = {
                "state": state,
                "stateReason": state_reason,
                "flagReason": flag_reason,
                "sites": info,
            }
            return state == "active" and valid, detail

        return self.wait_until(
            case_id,
            f"site replicate-ilm-expiry={str(expected).lower()}",
            probe,
            timeout=max(self.args.timeout, 120),
        )

    def ensure_site_group_active(
        self,
        case_id: str,
        *,
        evidence_prefix: str,
        refuse_recreate: bool,
    ) -> tuple[str, dict[str, Any]]:
        self.events.record(
            "setup.start",
            case=case_id,
            fixture=SITE_GROUP_ACTIVE_FIXTURE,
        )
        try:
            state, reason, initial = self.site_group_snapshot()
            self.save_json(
                case_id,
                f"{evidence_prefix}-initial.json",
                {"state": state, "reason": reason, "sites": initial},
            )
            outcome = "reused"
            if state == "disabled":
                if refuse_recreate:
                    raise PreconditionError(
                        "site group became disabled after the diagnostic fixture was established"
                    )
                self.add_site_replication_group(
                    case_id,
                    f"{evidence_prefix}-add-response.json",
                    require_clean_initial_sync=True,
                )
                outcome = "created"
            elif state != "active":
                raise PreconditionError(
                    f"site-group fixture refuses {state} control-plane state: {reason}"
                )

            final = (
                {"state": state, "reason": reason, "sites": initial}
                if state == "active"
                else self.wait_site_group_active(case_id)
            )
            self.save_json(
                case_id,
                f"{evidence_prefix}-final.json",
                {"outcome": outcome, **final},
            )
            self.events.record(
                "setup.group.ready",
                case=case_id,
                fixture=SITE_GROUP_ACTIVE_FIXTURE,
                outcome=outcome,
            )
            return outcome, final
        except Exception as exc:
            self.events.record(
                "setup.error",
                case=case_id,
                fixture=SITE_GROUP_ACTIVE_FIXTURE,
                errorType=type(exc).__name__,
                message=self.redact(str(exc)),
            )
            if isinstance(exc, PreconditionError):
                raise
            raise PreconditionError(
                f"site-group fixture could not establish an active group: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    def ensure_planned_fixture(self, case_id: str) -> None:
        if (
            SITE_GROUP_ACTIVE_FIXTURE not in self.requested_fixtures
            or case_id not in SITE_GROUP_ACTIVE_CASE_IDS
        ):
            return
        try:
            if self.site_group_tracker_state in {"tainted", "removed"}:
                raise PreconditionError(
                    "site-group fixture refuses to repair a group after an earlier "
                    f"{self.site_group_tracker_state} transition"
                )
            outcome, final = self.ensure_site_group_active(
                case_id,
                evidence_prefix="setup-site-group",
                refuse_recreate=(
                    self.site_group_tracker_state != "fresh"
                    or self.site_group_mutation_attempted
                ),
            )
            self.site_group_tracker_state = "active"
            if case_id == "site-replication.lifecycle-expiry-toggle":
                valid, reason = site_group_has_ilm_expiry_value(
                    final["sites"],
                    False,
                )
                self.save_json(
                    case_id,
                    "setup-site-group-ilm-expiry.json",
                    {"valid": valid, "reason": reason, "sites": final["sites"]},
                )
                if not valid:
                    self.events.record(
                        "setup.constraint.error",
                        case=case_id,
                        fixture=SITE_GROUP_ACTIVE_FIXTURE,
                        message=reason,
                    )
                    raise PreconditionError(reason)
            self.events.record(
                "setup.complete",
                case=case_id,
                fixture=SITE_GROUP_ACTIVE_FIXTURE,
                outcome=outcome,
            )
            self.fixture_outcomes.append(
                {
                    "caseId": case_id,
                    "fixture": SITE_GROUP_ACTIVE_FIXTURE,
                    "state": "pass",
                    "outcome": outcome,
                }
            )
        except Exception as exc:
            message = (
                str(exc)
                if isinstance(exc, PreconditionError)
                else f"{type(exc).__name__}: {exc}"
            )
            redacted_message = self.redact(f"precondition_error: {message}")
            self.save_json(
                case_id,
                "setup-site-group-error.json",
                {
                    "fixture": SITE_GROUP_ACTIVE_FIXTURE,
                    "state": "infra_error",
                    "message": redacted_message,
                    "trackerState": self.site_group_tracker_state,
                },
            )
            self.events.record(
                "setup.blocked",
                case=case_id,
                fixture=SITE_GROUP_ACTIVE_FIXTURE,
                message=redacted_message,
            )
            self.fixture_outcomes.append(
                {
                    "caseId": case_id,
                    "fixture": SITE_GROUP_ACTIVE_FIXTURE,
                    "state": "infra_error",
                    "message": redacted_message,
                }
            )
            if isinstance(exc, PreconditionError):
                raise
            raise PreconditionError(message) from exc

    def site_peer(self, info: dict[str, Any], endpoint: str) -> dict[str, Any]:
        identity = _site_endpoint_identity(endpoint)
        if identity is None:
            raise AssertionError(f"invalid configured site endpoint: {endpoint}")
        matches = [
            peer
            for peer in info.get("sites", [])
            if _site_endpoint_identity(peer.get("endpoint")) == identity
        ]
        if len(matches) != 1:
            raise AssertionError(f"expected one peer for {endpoint}, got {len(matches)}")
        return matches[0]

    def wait_bucket(self, case_id: str, client: Any, bucket: str, present: bool = True) -> Any:
        def probe() -> tuple[bool, Any]:
            try:
                response = client.get_bucket_location(Bucket=bucket)
                exists = True
                detail: Any = without_response_metadata(response)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "unknown")
                exists = code not in {"404", "NoSuchBucket", "NotFound"}
                detail = {"error": code}
            return exists is present, detail

        return self.wait_until(case_id, f"bucket {bucket} present={present}", probe)

    def create_site_bucket(self, case_id: str, role: str = "site") -> str:
        bucket = self.bucket_name(case_id, role)
        self.create_bucket(self.client_a, bucket)
        self.wait_bucket(case_id, self.client_b, bucket)
        self.created_buckets.append((self.client_b, bucket))
        return bucket

    def wait_versioning(self, case_id: str, bucket: str, status: str) -> dict[str, Any]:
        def probe() -> tuple[bool, Any]:
            response = self.client_b.get_bucket_versioning(Bucket=bucket)
            observed = response.get("Status")
            return observed == status, {"expected": status, "observed": observed}

        return self.wait_until(case_id, f"versioning={status} on site-b", probe)

    def case_site_unconfigured_info(self, case_id: str) -> list[str]:
        info = {site.id: self.site_info(site) for site in (self.site_a, self.site_b)}
        state, reason = classify_site_group_state(
            info,
            (self.site_a.endpoint, self.site_b.endpoint),
        )
        if state != "disabled":
            raise AssertionError(
                f"fresh sites are not cleanly unconfigured: {state}: {reason}"
            )
        return [self.save_json(case_id, "unconfigured-info.json", info)]

    def case_site_invalid_credentials(self, case_id: str) -> list[str]:
        payload = [
            {
                "name": "site-b-invalid",
                "endpoints": self.site_b.endpoint,
                "accessKey": self.site_b.access_key,
                "secretKey": "definitely-invalid-site-secret",
            }
        ]
        status, headers, body = self.admin_request(
            self.site_a,
            "PUT",
            "site-replication/add",
            query="replicateILMExpiry=false",
            payload=payload,
        )
        self.secrets.add("definitely-invalid-site-secret")
        document = {
            "httpStatus": status,
            "headers": headers,
            "body": body.decode("utf-8", errors="replace"),
        }
        if status < 400:
            parsed = self.required_json_object(body, "invalid-credential site add")
            if parsed.get("success") is not False:
                raise AssertionError(f"site add with invalid credentials succeeded: {parsed}")
        info = {site.id: self.site_info(site) for site in (self.site_a, self.site_b)}
        state, reason = classify_site_group_state(
            info,
            (self.site_a.endpoint, self.site_b.endpoint),
        )
        if state != "disabled":
            raise AssertionError(
                "invalid site add did not preserve a clean disabled state: "
                f"{state}: {reason}"
            )
        return [
            self.save_json(case_id, "invalid-add-response.json", document),
            self.save_json(case_id, "invalid-add-info.json", info),
        ]

    def case_site_add_and_info(self, case_id: str) -> list[str]:
        result, response_evidence = self.add_site_replication_group(
            case_id, "add-response.json", require_clean_initial_sync=True
        )
        settled = self.wait_site_group_active(case_id)
        info = settled["sites"]
        serialized = json.dumps({"result": result, "info": info})
        if self.args.secret_key in serialized:
            raise AssertionError("site replication response leaked administrator secret")
        return [
            response_evidence,
            self.save_json(case_id, "paired-info.json", settled),
        ]

    def add_site_replication_group(
        self,
        case_id: str,
        evidence_name: str,
        *,
        require_clean_initial_sync: bool,
    ) -> tuple[dict[str, Any], str]:
        self.site_group_mutation_attempted = True
        peers = [
            {
                "name": site.id,
                "endpoints": site.endpoint,
                "accessKey": site.access_key,
                "secretKey": site.secret_key,
            }
            for site in (self.site_a, self.site_b)
        ]
        status, headers, body = self.admin_request(
            self.site_a,
            "PUT",
            "site-replication/add",
            query="replicateILMExpiry=false",
            payload=peers,
        )
        if status != 200:
            raise AssertionError(
                f"site-replication add HTTP {status}: {body.decode(errors='replace')[:500]}"
            )
        result = self.required_json_object(body, "site-replication add")
        response_evidence = self.save_json(
            case_id,
            evidence_name,
            {"httpStatus": status, "headers": headers, "result": result},
        )
        if result.get("success") is not True:
            raise AssertionError(f"site-replication add rejected: {result}")
        if require_clean_initial_sync and result.get("initialSyncErrorMessage"):
            raise AssertionError(f"site-replication initial sync was partial: {result}")
        return result, response_evidence

    def case_site_status_observability(self, case_id: str) -> list[str]:
        query = (
            "buckets=true&users=true&groups=true&policies=true&metrics=true&"
            "peer-state=true&ilm-expiry-rules=true"
        )
        status_documents: dict[str, Any] = {}
        for site in (self.site_a, self.site_b):
            status, _, body = self.admin_request(
                site, "GET", "site-replication/status", query=query
            )
            if status != 200:
                raise AssertionError(f"{site.id} status returned HTTP {status}")
            parsed = self.json_body(body)
            status_documents[site.id] = parsed
            if parsed.get("enabled") is not True:
                raise AssertionError(f"{site.id} status is not enabled")
            sites = parsed.get("Sites") or parsed.get("sites")
            if not isinstance(sites, dict) or len(sites) != 2:
                raise AssertionError(f"{site.id} status omitted peer sites")
            for bound in (
                "MaxBuckets",
                "MaxUsers",
                "MaxGroups",
                "MaxPolicies",
                "MaxILMExpiryRules",
            ):
                if not isinstance(parsed.get(bound), int):
                    raise AssertionError(f"{site.id} status omitted numeric {bound}")
            if not isinstance(parsed.get("Metrics"), dict):
                raise AssertionError(f"{site.id} status omitted Metrics scope")
        serialized = json.dumps(status_documents)
        if self.args.secret_key in serialized or "secretKey" in serialized:
            raise AssertionError("status response leaked credentials")
        return [self.save_json(case_id, "site-status.json", status_documents)]

    def case_site_bucket_create(self, case_id: str) -> list[str]:
        bucket = self.create_site_bucket(case_id, "bucket")
        return [
            self.save_json(
                case_id,
                "bucket-create.json",
                {
                    "bucket": bucket,
                    "siteA": [item["Name"] for item in self.client_a.list_buckets().get("Buckets", [])],
                    "siteB": [item["Name"] for item in self.client_b.list_buckets().get("Buckets", [])],
                },
            )
        ]

    def case_site_object_bidirectional(self, case_id: str) -> list[str]:
        bucket = self.create_site_bucket(case_id, "bidir")
        self.enable_versioning(self.client_a, bucket)
        self.wait_versioning(case_id, bucket, "Enabled")
        fixtures = [
            (self.client_a, self.client_b, "from-a.txt", b"active-active-a", "a-to-b"),
            (self.client_b, self.client_a, "from-b.txt", b"active-active-b", "b-to-a"),
        ]
        observations = []
        for source_client, target_client, key, body, direction in fixtures:
            response = source_client.put_object(
                Bucket=bucket, Key=key, Body=body, Metadata={"direction": direction}
            )
            final = self.wait_object(
                case_id,
                target_client,
                bucket,
                key,
                body,
                expected_version=response.get("VersionId"),
            )
            if final.get("metadata", {}).get("direction") != direction:
                raise AssertionError(f"metadata mismatch for {direction}")
            observations.append({"direction": direction, **final})
        time.sleep(max(2, self.args.poll_interval * 2))
        for client in (self.client_a, self.client_b):
            versions = client.list_object_versions(Bucket=bucket)
            relevant = [item for item in versions.get("Versions", []) if item["Key"] in {"from-a.txt", "from-b.txt"}]
            if len(relevant) != 2:
                raise AssertionError(f"replication loop or missing version detected: {relevant}")
        return [self.save_json(case_id, "bidirectional-final.json", observations)]

    def case_site_versioning_metadata(self, case_id: str) -> list[str]:
        bucket = self.create_site_bucket(case_id, "versioning")
        self.client_a.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        observations: dict[str, Any] = {
            "enabled": self.wait_versioning(case_id, bucket, "Enabled")
        }
        try:
            self.client_a.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Suspended"}
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code != "InvalidBucketState":
                raise
            observations["suspendRejected"] = {
                "error": code,
                "message": exc.response.get("Error", {}).get("Message", ""),
            }
        else:
            raise AssertionError(
                "site-replicated bucket allowed versioning suspension while a replication rule was active"
            )
        source_status = self.client_a.get_bucket_versioning(Bucket=bucket).get("Status")
        if source_status != "Enabled":
            raise AssertionError(f"source versioning changed after rejected suspension: {source_status!r}")
        observations["remainedEnabled"] = self.wait_versioning(
            case_id, bucket, "Enabled"
        )
        self.client_a.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        observations["idempotentEnable"] = self.wait_versioning(
            case_id, bucket, "Enabled"
        )
        return [self.save_json(case_id, "versioning-propagation.json", observations)]

    def case_site_lifecycle_expiry_toggle(self, case_id: str) -> list[str]:
        state, state_reason, initial_info = self.site_group_snapshot()
        initial_evidence = self.save_json(
            case_id,
            "lifecycle-initial-site-info.json",
            {"state": state, "reason": state_reason, "sites": initial_info},
        )
        if state != "active":
            raise PreconditionError(
                f"lifecycle case requires an active site group: {state}: {state_reason}"
            )
        flags_valid, flags_reason = site_group_has_ilm_expiry_value(
            initial_info,
            False,
        )
        if not flags_valid:
            raise PreconditionError(flags_reason)

        bucket = self.create_site_bucket(case_id, "ilm")
        initial = {
            "Rules": [
                {
                    "ID": "expiry-disabled-at-site-level",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "expiry/"},
                    "Expiration": {"Days": 30},
                }
            ]
        }
        self.client_a.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=initial
        )
        deadline = time.monotonic() + self.args.negative_window
        while time.monotonic() < deadline:
            try:
                remote = self.client_b.get_bucket_lifecycle_configuration(Bucket=bucket)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in {
                    "NoSuchLifecycleConfiguration",
                    "NoSuchLifecycle",
                }:
                    raise
            else:
                if remote.get("Rules"):
                    raise AssertionError("expiry rules replicated while global ILM expiry was disabled")
            time.sleep(self.args.poll_interval)
        enable_attempted = False
        primary_error: Exception | None = None
        rollback_error: Exception | None = None
        replicated: dict[str, Any] = {}
        enabled_info: dict[str, Any] = {}
        final_info: dict[str, Any] = {}
        enable_evidence = ""
        disable_evidence = ""
        try:
            enable_attempted = True
            status, headers, body = self.admin_request(
                self.site_a,
                "PUT",
                "site-replication/edit",
                query="enableILMExpiryReplication=true",
                payload={},
            )
            enable_result = self.required_json_object(
                body,
                "enable ILM expiry replication",
            )
            enable_evidence = self.save_json(
                case_id,
                "lifecycle-enable-response.json",
                {
                    "httpStatus": status,
                    "headers": headers,
                    "result": enable_result,
                },
            )
            if (
                status != 200
                or enable_result.get("success") is not True
            ):
                raise AssertionError(
                    f"enable ILM expiry replication failed: HTTP {status} {body!r}"
                )
            enabled_info = self.wait_site_group_ilm_expiry_value(case_id, True)
            updated = {
                "Rules": [
                    {
                        "ID": "expiry-enabled-at-site-level",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "expiry/"},
                        "Expiration": {"Days": 29},
                    }
                ]
            }
            self.client_a.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration=updated
            )

            def lifecycle_probe() -> tuple[bool, Any]:
                try:
                    response = self.client_b.get_bucket_lifecycle_configuration(
                        Bucket=bucket
                    )
                except ClientError as exc:
                    return False, {
                        "error": exc.response.get("Error", {}).get("Code")
                    }
                rules = response.get("Rules", [])
                return any(
                    rule.get("ID") == "expiry-enabled-at-site-level"
                    for rule in rules
                ), response

            replicated = self.wait_until(
                case_id,
                "ILM expiry metadata replication",
                lifecycle_probe,
            )
        except Exception as exc:
            primary_error = exc
        finally:
            if enable_attempted:
                try:
                    status, headers, body = self.admin_request(
                        self.site_a,
                        "PUT",
                        "site-replication/edit",
                        query="disableILMExpiryReplication=true",
                        payload={},
                    )
                    disable_result = self.required_json_object(
                        body,
                        "disable ILM expiry replication",
                    )
                    disable_evidence = self.save_json(
                        case_id,
                        "lifecycle-disable-response.json",
                        {
                            "httpStatus": status,
                            "headers": headers,
                            "result": disable_result,
                        },
                    )
                    if (
                        status != 200
                        or disable_result.get("success") is not True
                    ):
                        raise AssertionError(
                            "disable ILM expiry replication failed: "
                            f"HTTP {status} {body!r}"
                        )
                    final_info = self.wait_site_group_ilm_expiry_value(
                        case_id,
                        False,
                    )
                except Exception as exc:
                    rollback_error = exc

        if rollback_error is not None:
            self.site_group_tracker_state = "tainted"
            if primary_error is not None:
                raise RuntimeError(
                    f"{type(primary_error).__name__}: {primary_error}; "
                    "lifecycle rollback failed: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                ) from rollback_error
            raise rollback_error
        if primary_error is not None:
            raise primary_error
        return [
            initial_evidence,
            enable_evidence,
            self.save_json(case_id, "lifecycle-enabled-site-info.json", enabled_info),
            self.save_json(case_id, "lifecycle-replicated.json", replicated),
            disable_evidence,
            self.save_json(case_id, "lifecycle-toggle-final-info.json", final_info),
        ]

    def case_site_bucket_policy_metadata(self, case_id: str) -> list[str]:
        bucket = self.create_site_bucket(case_id, "policy")
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ReadObjects",
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"],
                }
            ],
        }
        self.client_a.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
        source_policy = json.loads(
            self.client_a.get_bucket_policy(Bucket=bucket)["Policy"]
        )

        def policy_probe() -> tuple[bool, Any]:
            try:
                response = self.client_b.get_bucket_policy(Bucket=bucket)
            except ClientError as exc:
                return False, {"error": exc.response.get("Error", {}).get("Code")}
            observed = json.loads(response["Policy"])
            return observed == source_policy, observed

        replicated = self.wait_until(case_id, "bucket policy replication", policy_probe)
        self.client_a.delete_bucket_policy(Bucket=bucket)

        def delete_probe() -> tuple[bool, Any]:
            try:
                self.client_b.get_bucket_policy(Bucket=bucket)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                return code in {"NoSuchBucketPolicy", "NoSuchPolicy", "NoSuchBucket"}, {"error": code}
            return False, {"policy": "still present"}

        deleted = self.wait_until(case_id, "bucket policy deletion replication", delete_probe)
        return [
            self.save_json(case_id, "policy-replicated.json", replicated),
            self.save_json(case_id, "policy-deleted.json", deleted),
        ]

    def case_site_object_tags(self, case_id: str) -> list[str]:
        bucket = self.create_site_bucket(case_id, "tags")
        key = "tagged/object.txt"
        body = b"site-replication-tags"
        self.client_a.put_object(Bucket=bucket, Key=key, Body=body)
        self.wait_object(case_id, self.client_b, bucket, key, body)
        expected_sets = [
            [{"Key": "stage", "Value": "one"}],
            [{"Key": "stage", "Value": "two"}, {"Key": "owner", "Value": "qa"}],
        ]
        observations = []
        for tags in expected_sets:
            self.client_a.put_object_tagging(Bucket=bucket, Key=key, Tagging={"TagSet": tags})

            def probe(tags=tags) -> tuple[bool, Any]:
                response = self.client_b.get_object_tagging(Bucket=bucket, Key=key)
                observed = sorted(response.get("TagSet", []), key=lambda item: item["Key"])
                expected = sorted(tags, key=lambda item: item["Key"])
                return observed == expected, observed

            observations.append(self.wait_until(case_id, "object tag replication", probe))
        return [self.save_json(case_id, "object-tags.json", observations)]

    def case_site_delete_convergence(self, case_id: str) -> list[str]:
        bucket = self.create_site_bucket(case_id, "delete")
        self.enable_versioning(self.client_a, bucket)
        self.wait_versioning(case_id, bucket, "Enabled")
        key = "delete/object.txt"
        first = self.client_a.put_object(Bucket=bucket, Key=key, Body=b"site-version-one")
        second = self.client_a.put_object(Bucket=bucket, Key=key, Body=b"site-version-two")
        self.wait_object(
            case_id,
            self.client_b,
            bucket,
            key,
            b"site-version-two",
            expected_version=second.get("VersionId"),
        )
        marker = self.client_a.delete_object(Bucket=bucket, Key=key).get("VersionId")

        def marker_probe() -> tuple[bool, Any]:
            response = self.client_b.list_object_versions(Bucket=bucket, Prefix=key)
            markers = [item["VersionId"] for item in response.get("DeleteMarkers", []) if item["Key"] == key]
            return marker in markers, {"markers": markers, "expected": marker}

        marker_final = self.wait_until(case_id, "site delete marker convergence", marker_probe)
        self.client_a.delete_object(Bucket=bucket, Key=key, VersionId=first["VersionId"])

        def purge_probe() -> tuple[bool, Any]:
            response = self.client_b.list_object_versions(Bucket=bucket, Prefix=key)
            versions = [item["VersionId"] for item in response.get("Versions", []) if item["Key"] == key]
            return first["VersionId"] not in versions and second["VersionId"] in versions, versions

        purge_final = self.wait_until(case_id, "site version purge convergence", purge_probe)
        return [
            self.save_json(case_id, "delete-marker.json", marker_final),
            self.save_json(case_id, "version-purge.json", purge_final),
        ]

    def rc_probe(self, arguments: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [self.rc_bin, "--json", "--no-color", *arguments],
            env=self.rc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

    def policy_file(self, case_id: str, name: str, actions: list[str]) -> Path:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": actions,
                    "Resource": ["arn:aws:s3:::*"],
                }
            ],
        }
        path = self.evidence_path(case_id, name)
        path.write_text(json.dumps(policy, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path

    def wait_rc_success(
        self, case_id: str, description: str, arguments: list[str]
    ) -> subprocess.CompletedProcess[str]:
        last: subprocess.CompletedProcess[str] | None = None

        def probe() -> tuple[bool, Any]:
            nonlocal last
            last = self.rc_probe(arguments)
            return last.returncode == 0, {
                "exitCode": last.returncode,
                "stdout": last.stdout[:1000],
                "stderr": last.stderr[:1000],
            }

        self.wait_until(case_id, description, probe)
        assert last is not None
        self.save_json(
            case_id,
            f"{safe_name(description)}.json",
            {"exitCode": last.returncode, "stdout": last.stdout, "stderr": last.stderr},
        )
        return last

    def assert_iam_credentials(
        self,
        case_id: str,
        access_key: str,
        secret_key: str,
        label: str,
    ) -> dict[str, Any]:
        client = self.s3_client(self.site_b, access_key, secret_key)

        def allowed_probe() -> tuple[bool, Any]:
            try:
                response = client.list_buckets()
                return True, {"bucketCount": len(response.get("Buckets", []))}
            except ClientError as exc:
                return False, {"error": exc.response.get("Error", {}).get("Code")}

        allowed = self.wait_until(case_id, f"{label} authorization on site-b", allowed_probe)
        denied_bucket = self.bucket_name(case_id, f"deny-{label}")
        try:
            client.create_bucket(Bucket=denied_bucket)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"AccessDenied", "Unauthorized", "InvalidAccessKeyId"}:
                raise
            denied = {"operation": "CreateBucket", "error": code}
        else:
            raise AssertionError(f"{label} credentials exceeded the policy boundary")
        return {"allowed": allowed, "denied": denied}

    def case_site_iam_user_policy(self, case_id: str) -> list[str]:
        user = self.identity_name(case_id, "user")
        policy = self.identity_name(case_id, "policy")
        secret = f"Rv-{secrets.token_hex(16)}"
        self.secrets.add(secret)
        policy_path = self.policy_file(case_id, "user-policy.json", ["s3:ListAllMyBuckets"])
        self.run_rc(
            case_id,
            "iam-policy-create",
            ["admin", "policy", "create", self.alias_a, policy, str(policy_path)],
        )
        self.run_rc(
            case_id,
            "iam-user-add",
            ["admin", "user", "add", self.alias_a, user, secret],
        )
        self.run_rc(
            case_id,
            "iam-user-policy-attach",
            ["admin", "policy", "attach", self.alias_a, policy, "--user", user],
        )
        self.wait_rc_success(
            case_id, "remote-user-info", ["admin", "user", "info", self.alias_b, user]
        )
        self.wait_rc_success(
            case_id, "remote-user-policy", ["admin", "policy", "info", self.alias_b, policy]
        )
        auth = self.assert_iam_credentials(case_id, user, secret, "user")
        return [self.save_json(case_id, "iam-user-authorization.json", auth)]

    def case_site_iam_group_policy(self, case_id: str) -> list[str]:
        user = self.identity_name(case_id, "group-user")
        group = self.identity_name(case_id, "group")
        policy = self.identity_name(case_id, "group-policy")
        secret = f"Rv-{secrets.token_hex(16)}"
        self.secrets.add(secret)
        policy_path = self.policy_file(case_id, "group-policy.json", ["s3:ListAllMyBuckets"])
        self.run_rc(
            case_id,
            "group-policy-create",
            ["admin", "policy", "create", self.alias_a, policy, str(policy_path)],
        )
        self.run_rc(
            case_id,
            "group-user-add",
            ["admin", "user", "add", self.alias_a, user, secret],
        )
        self.run_rc(
            case_id,
            "group-add",
            ["admin", "group", "add", self.alias_a, group, "--members", user],
        )
        self.run_rc(
            case_id,
            "group-policy-attach",
            ["admin", "policy", "attach", self.alias_a, policy, "--group", group],
        )
        self.wait_rc_success(
            case_id, "remote-group-info", ["admin", "group", "info", self.alias_b, group]
        )
        self.wait_rc_success(
            case_id, "remote-group-policy", ["admin", "policy", "info", self.alias_b, policy]
        )
        auth = self.assert_iam_credentials(case_id, user, secret, "group-user")
        return [self.save_json(case_id, "iam-group-authorization.json", auth)]

    def case_site_service_account(self, case_id: str) -> list[str]:
        access_key = self.identity_name(case_id, "svc")
        secret = f"Svc-{secrets.token_hex(18)}"
        self.secrets.add(secret)
        policy_path = self.policy_file(case_id, "service-account-policy.json", ["s3:ListAllMyBuckets"])
        self.run_rc(
            case_id,
            "service-account-create",
            [
                "admin",
                "service-account",
                "create",
                self.alias_a,
                access_key,
                secret,
                "--name",
                "release-validation",
                "--policy",
                str(policy_path),
            ],
        )
        self.wait_rc_success(
            case_id,
            "remote-service-account-info",
            ["admin", "service-account", "info", self.alias_b, access_key],
        )
        auth = self.assert_iam_credentials(case_id, access_key, secret, "service-account")
        return [self.save_json(case_id, "service-account-authorization.json", auth)]

    def case_site_peer_edit(self, case_id: str) -> list[str]:
        info_a = self.site_info(self.site_a)
        peer = dict(self.site_peer(info_a, self.site_b.endpoint))
        deployment_id = str(peer.get("deploymentID", ""))
        if not deployment_id:
            raise AssertionError("remote peer has no deployment ID")
        new_name = self.identity_name(case_id, "renamed-site")
        peer["name"] = new_name
        status, headers, body = self.admin_request(
            self.site_a,
            "PUT",
            "site-replication/edit",
            payload=peer,
        )
        if status != 200:
            raise AssertionError(f"site peer edit returned HTTP {status}: {body!r}")
        parsed = self.json_body(body)
        if parsed and parsed.get("success") is False:
            raise AssertionError(f"site peer edit was rejected: {parsed}")

        def probe() -> tuple[bool, Any]:
            infos = {site.id: self.site_info(site) for site in (self.site_a, self.site_b)}
            observed: dict[str, Any] = {}
            complete = True
            for site_id, info in infos.items():
                matches = [
                    item
                    for item in info.get("sites", [])
                    if str(item.get("deploymentID", "")) == deployment_id
                ]
                observed[site_id] = matches
                complete = complete and len(matches) == 1 and matches[0].get("name") == new_name
                if matches and str(matches[0].get("endpoint", "")).rstrip("/") != self.site_b.endpoint:
                    raise AssertionError("peer rename changed its endpoint")
            return complete, observed

        final = self.wait_until(case_id, "peer rename convergence", probe)
        return [
            self.save_json(
                case_id,
                "peer-edit.json",
                {"httpStatus": status, "headers": headers, "response": parsed, "final": final},
            )
        ]

    def case_site_resync(self, case_id: str) -> list[str]:
        info = self.site_info(self.site_a)
        peer = self.site_peer(info, self.site_b.endpoint)
        responses: dict[str, Any] = {}
        status, _, body = self.admin_request(
            self.site_a,
            "PUT",
            "site-replication/resync/op",
            query="operation=start",
            payload=peer,
        )
        if status != 200:
            raise AssertionError(f"site resync start returned HTTP {status}: {body!r}")
        responses["start"] = self.json_body(body)
        status, _, body = self.admin_request(
            self.site_a,
            "PUT",
            "site-replication/resync/op",
            query="operation=status",
            payload=peer,
        )
        if status != 200:
            raise AssertionError(f"site resync status returned HTTP {status}: {body!r}")
        responses["status"] = self.json_body(body)
        resync_id = responses["status"].get("id") or responses["start"].get("id")
        if not resync_id:
            raise AssertionError(f"site resync did not return a durable ID: {responses}")
        state = str(responses["status"].get("state", responses["status"].get("status", ""))).lower()
        if state not in {"completed", "complete", "success", "failed", "cancelled", "canceled"}:
            status, _, body = self.admin_request(
                self.site_a,
                "PUT",
                "site-replication/resync/op",
                query="operation=cancel",
                payload=peer,
            )
            if status != 200:
                raise AssertionError(f"site resync cancel returned HTTP {status}: {body!r}")
            responses["cancel"] = self.json_body(body)
        else:
            responses["cancel"] = {"notRequired": True, "terminalState": state}
        return [self.save_json(case_id, "site-resync.json", responses)]

    def case_site_outage_recovery(self, case_id: str) -> list[str]:
        bucket = self.bucket_name(case_id, "outage")
        key = "outage/site-retry.txt"
        body = b"site-replication-outage"
        evidence = [self.hook(case_id, "site-b-stop", self.args.site_b_stop_hook)]
        b_started = False
        try:
            self.create_bucket(self.client_a, bucket)
            self.client_a.put_object(Bucket=bucket, Key=key, Body=body)

            def retry_probe() -> tuple[bool, Any]:
                info = self.site_info(self.site_a)
                stats = info.get("retryStats")
                pending = int((stats or {}).get("pending", 0) or 0)
                failed = int((stats or {}).get("failed", 0) or 0)
                return bool(stats) and (pending > 0 or failed > 0 or bool(stats.get("lastError"))), info

            retry = self.wait_until(
                case_id, "source retry observability while peer is offline", retry_probe, timeout=60
            )
            evidence.append(self.save_json(case_id, "retry-visible.json", retry))
            evidence.append(self.hook(case_id, "site-b-start", self.args.site_b_start_hook))
            b_started = True
            self.wait_api(case_id, self.client_b, "site-b")
        finally:
            if not b_started:
                self.hook(case_id, "site-b-start-recovery", self.args.site_b_start_hook)
                self.wait_api(case_id, self.client_b, "site-b")
        self.wait_bucket(case_id, self.client_b, bucket)
        self.created_buckets.append((self.client_b, bucket))
        final = self.wait_object(
            case_id, self.client_b, bucket, key, body, timeout=max(self.args.timeout, 240)
        )
        evidence.append(self.save_json(case_id, "outage-final.json", final))

        def retry_cleared_probe() -> tuple[bool, Any]:
            info = self.site_info(self.site_a)
            stats = info.get("retryStats")
            if not stats:
                return True, info
            pending = int(stats.get("pending", 0) or 0)
            failed = int(stats.get("failed", 0) or 0)
            return pending == 0 and failed == 0 and not stats.get("lastError"), info

        cleared = self.wait_until(
            case_id,
            "source retry queue to clear after peer recovery",
            retry_cleared_probe,
            timeout=max(self.args.timeout, 240),
        )
        evidence.append(self.save_json(case_id, "retry-cleared.json", cleared))
        return evidence

    def site_smoke(self, case_id: str, prefix: str) -> dict[str, Any]:
        bucket = self.create_site_bucket(case_id, prefix)
        fixtures = [
            (self.client_a, self.client_b, "from-a.txt", f"{prefix}-a".encode()),
            (self.client_b, self.client_a, "from-b.txt", f"{prefix}-b".encode()),
        ]
        final = []
        for source, target, key, body in fixtures:
            response = source.put_object(Bucket=bucket, Key=key, Body=body)
            final.append(
                self.wait_object(
                    case_id,
                    target,
                    bucket,
                    key,
                    body,
                    expected_version=response.get("VersionId"),
                )
            )
        return {"bucket": bucket, "objects": final}

    def case_site_restart_persistence(self, case_id: str) -> list[str]:
        evidence = [
            self.hook(case_id, "site-a-restart", self.args.site_a_restart_hook),
            self.hook(case_id, "site-b-restart", self.args.site_b_restart_hook),
        ]
        self.wait_api(case_id, self.client_a, "site-a")
        self.wait_api(case_id, self.client_b, "site-b")
        info = self.wait_site_group_active(case_id)
        smoke = self.site_smoke(case_id, "restart")
        evidence.extend(
            (
                self.save_json(case_id, "restart-info.json", info),
                self.save_json(case_id, "restart-smoke.json", smoke),
            )
        )
        return evidence

    def case_site_service_account_rotation(self, case_id: str) -> list[str]:
        evidence: list[str] = []
        if (
            self.args.selection_mode == "filtered"
            and self.site_group_tracker_state == "tainted"
        ):
            raise PreconditionError(
                "service-account rotation refuses a site group tainted by an "
                "earlier selected case"
            )
        try:
            state, reason, current = self.site_group_snapshot()
        except Exception as exc:
            raise PreconditionError(
                "service-account rotation could not classify the initial site group: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        evidence.append(
            self.save_json(
                case_id,
                "rotation-initial-site-info.json",
                {"state": state, "reason": reason, "sites": current},
            )
        )
        if state == "disabled":
            if (
                self.args.selection_mode == "filtered"
                and self.site_group_tracker_state not in {"fresh", "removed"}
            ):
                raise PreconditionError(
                    "service-account rotation refuses to recreate a site group "
                    "lost after an earlier selected case"
                )
            _, setup_evidence = self.add_site_replication_group(
                case_id,
                "rotation-setup-add-response.json",
                require_clean_initial_sync=True,
            )
            evidence.append(setup_evidence)
            setup_final = self.wait_site_group_active(case_id)
            evidence.append(
                self.save_json(case_id, "rotation-setup-final.json", setup_final)
            )
        elif state != "active":
            raise PreconditionError(
                "service-account rotation refuses initial "
                f"{state} control-plane state: {reason}"
            )
        status, headers, body = self.admin_request(
            self.site_a, "POST", "site-replication/rotate-svc-acct"
        )
        result = self.required_json_object(body, "service account rotation")
        response_evidence = self.save_json(
            case_id,
            "rotation-response.json",
            {"httpStatus": status, "headers": headers, "result": result},
        )
        if status != 200:
            raise AssertionError(f"service account rotation returned HTTP {status}: {body!r}")
        if result.get("success") is not True:
            raise AssertionError(f"service account rotation was incomplete: {result}")

        settled = self.wait_site_group_active(case_id)
        smoke = self.site_smoke(case_id, "rotation")
        evidence.extend(
            [
                response_evidence,
                self.save_json(case_id, "rotation-settled.json", settled),
                self.save_json(case_id, "rotation-smoke.json", smoke),
            ]
        )
        return evidence

    def case_site_remove_all(self, case_id: str) -> list[str]:
        status, headers, body = self.admin_request(
            self.site_a,
            "PUT",
            "site-replication/remove",
            payload={"all": True},
        )
        response_text = body.decode("utf-8", errors="replace")
        response_evidence = self.save_json(
            case_id,
            "remove-response.json",
            {"httpStatus": status, "headers": headers, "body": response_text},
        )
        if status != 200:
            raise AssertionError(f"site remove-all returned HTTP {status}: {body!r}")
        result = self.required_json_object(body, "site remove-all")
        if result.get("errorDetail") or str(result.get("status", "")).lower() == "partial":
            raise AssertionError(f"site remove-all was partial: {result}")
        info = self.wait_site_group_disabled(case_id)
        bucket = self.bucket_name(case_id, "removed")
        self.create_bucket(self.client_a, bucket)
        self.client_a.put_object(Bucket=bucket, Key="control.txt", Body=b"must-not-replicate")
        deadline = time.monotonic() + max(self.args.negative_window, 15)
        observations = 0
        while time.monotonic() < deadline:
            observations += 1
            try:
                self.client_b.get_bucket_location(Bucket=bucket)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in {
                    "NoSuchBucket",
                    "404",
                    "NotFound",
                }:
                    raise
            else:
                raise AssertionError("bucket propagated after site replication was removed")
            time.sleep(self.args.poll_interval)
        return [
            response_evidence,
            self.save_json(
                case_id,
                "remove-all.json",
                {
                    "httpStatus": status,
                    "headers": headers,
                    "response": result,
                    "info": info,
                    "negativeObservations": observations,
                },
            )
        ]

    # ------------------------------------------------------------------
    # Execution and result contracts
    # ------------------------------------------------------------------

    def classify_exception(self, exc: BaseException) -> str:
        if isinstance(exc, AssertionError):
            return "fail"
        if isinstance(exc, INFRA_EXCEPTIONS):
            return "infra_error"
        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            return "infra_error" if code in INFRA_ERROR_CODES else "fail"
        return "infra_error"

    def update_site_group_tracker(self, case_id: str, result: CaseResult) -> None:
        if not case_id.startswith("site-replication."):
            return
        if case_id in {
            "site-replication.unconfigured-info",
            "site-replication.invalid-credentials",
        }:
            self.site_group_tracker_state = (
                "fresh" if result.state == "pass" else "tainted"
            )
            return
        if case_id == "site-replication.add-and-info":
            self.site_group_tracker_state = (
                "active" if result.state == "pass" else "tainted"
            )
            return
        if case_id == "site-replication.remove-all":
            self.site_group_tracker_state = (
                "removed" if result.state == "pass" else "tainted"
            )
            return
        if case_id == "site-replication.service-account-rotation":
            self.site_group_tracker_state = (
                "active" if result.state == "pass" else "tainted"
            )
            return
        if result.message.startswith("precondition_error:"):
            self.site_group_tracker_state = "tainted"
            return
        if result.state == "pass":
            self.site_group_tracker_state = "active"
            return
        if case_id == "site-replication.lifecycle-expiry-toggle":
            if self.site_group_tracker_state == "tainted":
                return
            try:
                state, reason, info = self.site_group_snapshot()
                flags_valid, flags_reason = site_group_has_ilm_expiry_value(
                    info,
                    False,
                )
                self.site_group_tracker_state = (
                    "active" if state == "active" and flags_valid else "tainted"
                )
                self.events.record(
                    "setup.tracker.probe",
                    case=case_id,
                    state=state,
                    reason=reason,
                    flagReason=flags_reason,
                )
            except Exception as exc:
                self.site_group_tracker_state = "tainted"
                self.events.record(
                    "setup.tracker.error",
                    case=case_id,
                    message=self.redact(f"{type(exc).__name__}: {exc}"),
                )
            return
        try:
            state, reason, _ = self.site_group_snapshot()
            self.site_group_tracker_state = (
                "active" if state == "active" else "tainted"
            )
            self.events.record(
                "setup.tracker.probe",
                case=case_id,
                state=state,
                reason=reason,
            )
        except Exception as exc:
            self.site_group_tracker_state = "tainted"
            self.events.record(
                "setup.tracker.error",
                case=case_id,
                message=self.redact(f"{type(exc).__name__}: {exc}"),
            )

    def record_isolation_blocked(self, case_id: str) -> CaseResult:
        failure = self.isolation_failure
        if failure is None:
            raise RuntimeError("module isolation is not blocked")
        started_at = utc_now()
        message = self.redact(
            "precondition_error: module isolation is blocked by logical cleanup "
            f"from {failure['sourceCase']}; authoritative reset is required"
        )
        blocked_evidence = self.save_json(
            case_id,
            "case-isolation-blocked.json",
            {
                "state": "infra_error",
                "message": message,
                "sourceCase": failure["sourceCase"],
                "sourceEvidence": failure["evidence"],
                "trackedBuckets": failure["trackedBuckets"],
            },
        )
        evidence = list(
            dict.fromkeys([blocked_evidence, *failure["evidence"]])
        )
        if (
            SITE_GROUP_ACTIVE_FIXTURE in self.requested_fixtures
            and case_id in SITE_GROUP_ACTIVE_CASE_IDS
        ):
            self.fixture_outcomes.append(
                {
                    "caseId": case_id,
                    "fixture": SITE_GROUP_ACTIVE_FIXTURE,
                    "state": "infra_error",
                    "message": message,
                }
            )
        result = CaseResult(
            id=case_id,
            state="infra_error",
            message=message,
            started_at=started_at,
            finished_at=utc_now(),
            duration_seconds=0.0,
            evidence=evidence,
        )
        self.results.append(result)
        if case_id.startswith("site-replication."):
            self.site_group_tracker_state = "tainted"
        self.events.record(
            "case.isolation.blocked",
            case=case_id,
            sourceCase=failure["sourceCase"],
            evidence=evidence,
            trackedBuckets=failure["trackedBuckets"],
        )
        self.events.record("case.complete", **asdict(result))
        print(f"[INFRA_ERROR] {case_id}: {message}", flush=True)
        return result

    def run_case(
        self,
        case_id: str,
        function: Callable[[str], list[str]],
    ) -> CaseResult:
        if self.isolation_failure is not None:
            return self.record_isolation_blocked(case_id)
        started_at = utc_now()
        started = time.monotonic()
        state = "pass"
        message = "all assertions passed"
        print(f"[RUN ] {case_id}", flush=True)
        self.events.record("case.start", case=case_id)
        try:
            self.ensure_planned_fixture(case_id)
            function(case_id)
        except Exception as exc:  # keep the remaining cases independent
            if isinstance(exc, PreconditionError):
                state = "infra_error"
                message = f"precondition_error: {exc}"
            else:
                state = self.classify_exception(exc)
                message = f"{type(exc).__name__}: {exc}"
            self.events.record(
                "case.error",
                case=case_id,
                state=state,
                errorType=type(exc).__name__,
                message=self.redact(message),
            )
            self.save_json(
                case_id,
                "case-error.json",
                {
                    "state": state,
                    "errorType": type(exc).__name__,
                    "message": self.redact(message),
                },
            )
        finally:
            try:
                # Replication phases intentionally continue after a failed case;
                # therefore client-visible evidence is retained while logical test
                # buckets are removed. The driver captures node logs before its
                # authoritative phase reset.
                self.cleanup_created_buckets(case_id)
            except Exception as cleanup_exc:
                cleanup_message = (
                    f"cleanup failed: {type(cleanup_exc).__name__}: {cleanup_exc}"
                )
                if state == "pass":
                    state = "infra_error"
                    message = cleanup_message
                else:
                    message = f"{message}; {cleanup_message}"
                self.events.record(
                    "case.cleanup.error", case=case_id, message=self.redact(str(cleanup_exc))
                )
                tracked_buckets = self.tracked_bucket_summaries()
                isolation_restored = (
                    cleanup_exc.isolation_restored
                    if isinstance(cleanup_exc, LogicalCleanupFailure)
                    else not tracked_buckets
                )
                cleanup_error_evidence = self.save_json(
                    case_id,
                    "case-cleanup-error.json",
                    {
                        "state": "infra_error",
                        "errorType": type(cleanup_exc).__name__,
                        "message": self.redact(cleanup_message),
                        "isolationRestored": isolation_restored,
                        "trackedBuckets": tracked_buckets,
                        "cleanupEvidence": (
                            cleanup_exc.evidence
                            if isinstance(cleanup_exc, LogicalCleanupFailure)
                            else None
                        ),
                    },
                )
                if not isolation_restored:
                    source_evidence = [cleanup_error_evidence]
                    if isinstance(cleanup_exc, LogicalCleanupFailure):
                        source_evidence.insert(0, cleanup_exc.evidence)
                    self.isolation_failure = {
                        "state": "blocked",
                        "sourceCase": case_id,
                        "evidence": list(dict.fromkeys(source_evidence)),
                        "trackedBuckets": tracked_buckets,
                    }
                    self.events.record(
                        "module.isolation.blocked",
                        case=case_id,
                        evidence=self.isolation_failure["evidence"],
                        trackedBuckets=tracked_buckets,
                    )

        directory = self.evidence_path(case_id, "evidence-index").parent
        evidence = sorted(
            str(path.relative_to(self.evidence_dir))
            for path in directory.iterdir()
            if path.is_file() and path.name != "evidence-index"
        )
        result = CaseResult(
            id=case_id,
            state=state,
            message=self.redact(message),
            started_at=started_at,
            finished_at=utc_now(),
            duration_seconds=round(time.monotonic() - started, 3),
            evidence=evidence,
        )
        self.results.append(result)
        if self.isolation_failure is not None and case_id.startswith(
            "site-replication."
        ):
            self.site_group_tracker_state = "tainted"
        else:
            self.update_site_group_tracker(case_id, result)
        self.events.record("case.complete", **asdict(result))
        label = "PASS" if state == "pass" else state.upper()
        print(f"[{label:5}] {case_id}: {result.message}", flush=True)
        return result

    def case_functions(self) -> dict[str, Callable[[str], list[str]]]:
        return {
            "bucket-replication.source-versioning-required": self.case_bucket_source_versioning_required,
            "bucket-replication.target-versioning-required": self.case_bucket_target_versioning_required,
            "bucket-replication.configuration-roundtrip": self.case_bucket_configuration_roundtrip,
            "bucket-replication.put-object": self.case_bucket_put_object,
            "bucket-replication.sync-put-object": self.case_bucket_sync_put_object,
            "bucket-replication.prefix-filter": self.case_bucket_prefix_filter,
            "bucket-replication.tag-filter": self.case_bucket_tag_filter,
            "bucket-replication.zero-byte-special-key": self.case_bucket_zero_byte_special_key,
            "bucket-replication.multipart-object": self.case_bucket_multipart_object,
            "bucket-replication.copy-object": self.case_bucket_copy_object,
            "bucket-replication.delete-marker": self.case_bucket_delete_marker,
            "bucket-replication.version-delete": self.case_bucket_version_delete,
            "bucket-replication.existing-object-enabled": self.case_bucket_existing_object_enabled,
            "bucket-replication.existing-object-disabled": self.case_bucket_existing_object_disabled,
            "bucket-replication.target-check": self.case_bucket_target_check,
            "bucket-replication.status-observability": self.case_bucket_status_observability,
            "bucket-replication.outage-recovery": self.case_bucket_outage_recovery,
            "bucket-replication.source-restart-recovery": self.case_bucket_source_restart_recovery,
            "site-replication.unconfigured-info": self.case_site_unconfigured_info,
            "site-replication.invalid-credentials": self.case_site_invalid_credentials,
            "site-replication.add-and-info": self.case_site_add_and_info,
            "site-replication.status-observability": self.case_site_status_observability,
            "site-replication.bucket-create": self.case_site_bucket_create,
            "site-replication.object-bidirectional": self.case_site_object_bidirectional,
            "site-replication.versioning-metadata": self.case_site_versioning_metadata,
            "site-replication.lifecycle-expiry-toggle": self.case_site_lifecycle_expiry_toggle,
            "site-replication.bucket-policy-metadata": self.case_site_bucket_policy_metadata,
            "site-replication.object-tags": self.case_site_object_tags,
            "site-replication.delete-convergence": self.case_site_delete_convergence,
            "site-replication.iam-user-policy": self.case_site_iam_user_policy,
            "site-replication.iam-group-policy": self.case_site_iam_group_policy,
            "site-replication.service-account": self.case_site_service_account,
            "site-replication.peer-edit": self.case_site_peer_edit,
            "site-replication.resync": self.case_site_resync,
            "site-replication.outage-recovery": self.case_site_outage_recovery,
            "site-replication.restart-persistence": self.case_site_restart_persistence,
            "site-replication.remove-all": self.case_site_remove_all,
            "site-replication.service-account-rotation": self.case_site_service_account_rotation,
        }

    def write_results(self, started_at: str, started: float) -> str:
        ordered = BUCKET_CASE_IDS if self.args.module == "bucket" else SITE_CASE_IDS
        selected_cases = tuple(
            case_id for case_id in ordered if case_id in set(self.args.case or ordered)
        )
        case_order = {case_id: index for index, case_id in enumerate(ordered)}
        self.results.sort(key=lambda item: case_order.get(item.id, len(case_order)))
        states = {result.state for result in self.results}
        if "fail" in states:
            overall = "fail"
        elif "infra_error" in states:
            overall = "infra_error"
        else:
            overall = "pass"
        finished_at = utc_now()
        document = {
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
            json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        manifest = {
            "runId": self.args.run_id,
            "environmentId": self.args.environment_id,
            "module": self.args.module,
            "endpointA": self.site_a.endpoint,
            "endpointB": self.site_b.endpoint,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "durationSeconds": round(time.monotonic() - started, 3),
            "pollIntervalSeconds": self.args.poll_interval,
            "negativeWindowSeconds": self.args.negative_window,
            "boto3Version": boto3.__version__,
            "botocoreVersion": botocore.__version__,
            "rcBinaryPath": self.rc_bin,
            "rcVersion": self.rc_version,
            "rcBinarySha256": self.rc_binary_sha256,
            "expectedRcBinarySha256": self.expected_rc_binary_sha256 or None,
            "executionPlan": {
                "selectionMode": self.args.selection_mode,
                "module": self.args.module,
                "scoredCases": list(selected_cases),
                "scoredCaseCount": len(selected_cases),
                "fixtures": sorted(self.requested_fixtures),
            },
            "fixtureOutcomes": self.fixture_outcomes,
            "siteGroupTrackerState": self.site_group_tracker_state,
            "isolationOutcome": self.isolation_failure
            or {
                "state": "pass",
                "message": "all tracked logical buckets were removed",
                "trackedBuckets": [],
            },
        }
        (self.evidence_dir / "run-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        self.write_junit(overall, time.monotonic() - started)
        return overall

    def write_junit(self, overall: str, duration: float) -> None:
        suite = ET.Element(
            "testsuite",
            {
                "name": f"rustfs-fixed-environment-{self.args.module}-replication",
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
                    "classname": f"rustfs.{self.args.module}_replication",
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

    def run(self) -> int:
        started_at = utc_now()
        started = time.monotonic()
        self.events.record(
            "run.start",
            runId=self.args.run_id,
            environmentId=self.args.environment_id,
            module=self.args.module,
            selectionMode=self.args.selection_mode,
            scoredCases=list(self.args.case or (
                BUCKET_CASE_IDS if self.args.module == "bucket" else SITE_CASE_IDS
            )),
            fixtures=sorted(self.requested_fixtures),
        )
        try:
            self.wait_api("preflight.connectivity", self.client_a, "site-a")
            self.wait_api("preflight.connectivity", self.client_b, "site-b")
            self.configure_rc()
        except Exception as exc:
            self.results.append(
                CaseResult(
                    id="preflight.connectivity",
                    state="infra_error",
                    message=self.redact(f"{type(exc).__name__}: {exc}"),
                    started_at=started_at,
                    finished_at=utc_now(),
                    duration_seconds=round(time.monotonic() - started, 3),
                )
            )
            overall = self.write_results(started_at, started)
            return 2 if overall == "infra_error" else 1

        functions = self.case_functions()
        ordered = BUCKET_CASE_IDS if self.args.module == "bucket" else SITE_CASE_IDS
        selected = set(self.args.case or ordered)
        for case_id in ordered:
            if case_id in selected:
                self.run_case(case_id, functions[case_id])
        overall = self.write_results(started_at, started)
        print(
            f"{self.args.module} replication validation {overall}: "
            f"result={self.evidence_dir / 'result.json'}",
            flush=True,
        )
        return {"pass": 0, "fail": 1, "infra_error": 2}[overall]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module", choices=("bucket", "site"), required=True)
    parser.add_argument("--endpoint-a", default=os.environ.get("RUSTFS_REPLICATION_ENDPOINT_A"))
    parser.add_argument("--endpoint-b", default=os.environ.get("RUSTFS_REPLICATION_ENDPOINT_B"))
    parser.add_argument("--access-key", default=os.environ.get("RUSTFS_ACCESS_KEY"))
    parser.add_argument("--secret-key", default=os.environ.get("RUSTFS_SECRET_KEY"))
    parser.add_argument("--region", default=os.environ.get("RUSTFS_REGION", "us-east-1"))
    parser.add_argument("--environment-id", default="replication-2site")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--selection-mode", choices=("full", "filtered"))
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--poll-interval", type=float, default=2)
    parser.add_argument("--negative-window", type=float, default=12)
    parser.add_argument("--rc-bin", default="rc")
    parser.add_argument("--site-a-stop-hook")
    parser.add_argument("--site-a-start-hook")
    parser.add_argument("--site-a-restart-hook")
    parser.add_argument("--site-b-stop-hook")
    parser.add_argument("--site-b-start-hook")
    parser.add_argument("--site-b-restart-hook")
    parser.add_argument(
        "--case",
        action="append",
        choices=BUCKET_CASE_IDS + SITE_CASE_IDS,
        help="run only the named case; repeat to select multiple cases",
    )
    parser.add_argument(
        "--fixture",
        action="append",
        choices=(SITE_GROUP_ACTIVE_FIXTURE,),
        help="non-scored setup fixture selected by the case planner",
    )
    parser.add_argument("--cleanup-on-failure", action="store_true")
    args = parser.parse_args()
    selection_mode_was_explicit = args.selection_mode is not None
    if not args.endpoint_a or not args.endpoint_b or not args.access_key or not args.secret_key:
        parser.error("both endpoints and the administrator access/secret keys are required")
    endpoint_identities: list[tuple[str, int]] = []
    for endpoint in (args.endpoint_a, args.endpoint_b):
        identity = _site_endpoint_identity(endpoint)
        if identity is None:
            parser.error("replication endpoints must be absolute HTTP(S) URLs")
        endpoint_identities.append(identity)
    if len(set(endpoint_identities)) != 2:
        parser.error("replication endpoints must be distinct")
    if args.timeout <= 0 or args.poll_interval <= 0 or args.negative_window <= 0:
        parser.error("timing values must be positive")
    selected = set(args.case or ())
    allowed = set(BUCKET_CASE_IDS if args.module == "bucket" else SITE_CASE_IDS)
    if selected - allowed:
        parser.error(f"selected case does not belong to module {args.module}")
    if args.selection_mode is None:
        args.selection_mode = "filtered" if args.case else "full"
    fixtures = set(args.fixture or ())
    if args.selection_mode == "full":
        if selected and selected != allowed:
            parser.error("full selection mode requires the complete module case set")
        if fixtures:
            parser.error("full selection mode must not enable diagnostic fixtures")
    else:
        if not selected:
            parser.error("filtered selection mode requires at least one case")
        expected_fixtures = (
            {SITE_GROUP_ACTIVE_FIXTURE}
            if args.module == "site"
            and SITE_GROUP_ACTIVE_CASE_IDS.intersection(selected)
            else set()
        )
        if not selection_mode_was_explicit and args.fixture is None:
            args.fixture = sorted(expected_fixtures)
            fixtures = set(args.fixture)
        if fixtures != expected_fixtures:
            parser.error(
                "filtered fixture set does not match the selected site-case "
                f"requirements: expected={sorted(expected_fixtures)} "
                f"actual={sorted(fixtures)}"
            )
    no_proxy = os.environ.get("NO_PROXY", os.environ.get("no_proxy", ""))
    hosts = [identity[0] for identity in endpoint_identities]
    os.environ["NO_PROXY"] = ",".join(filter(None, [no_proxy, *hosts, "127.0.0.1", "localhost"]))
    os.environ["no_proxy"] = os.environ["NO_PROXY"]
    return args


def main() -> int:
    if hasattr(os.sys.stdout, "reconfigure"):
        os.sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    harness = ReplicationHarness(args)
    try:
        return harness.run()
    finally:
        harness.close()


if __name__ == "__main__":
    raise SystemExit(main())
