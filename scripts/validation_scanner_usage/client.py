"""Bounded S3 CLI and signed Admin GET requests; never deploy or restart a node."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import time
from urllib import error, parse, request


class ProbeError(RuntimeError):
    def __init__(self, message, verdict="INFRA_ERROR", cleanup_status=None):
        super().__init__(message)
        self.verdict = verdict
        self.cleanup_status = cleanup_status


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_command(command, timeout, env=None):
    """Kill the process group on timeout, including a native test's children."""
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env, start_new_session=True)
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
    except BaseException:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise
    return {"command": [str(part) for part in command], "exit_code": process.returncode,
            "timed_out": timed_out, "duration_seconds": time.monotonic() - started,
            "stdout": stdout, "stderr": stderr}


class Evidence:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.sequence = 0

    def save(self, stem, value):
        self.sequence += 1
        name = f"{self.sequence:05d}-{stem}.json"
        (self.root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        return name


def endpoint_url(value):
    message = "endpoint must be an HTTP(S) origin without credentials, query or path"
    try:
        if not isinstance(value, str) or any(character.isspace() or ord(character) < 32 for character in value):
            raise ValueError(message)
        url = parse.urlsplit(value)
        if (url.scheme not in {"http", "https"} or not url.hostname
                or url.username is not None or url.password is not None
                or url.query or url.fragment or url.path not in {"", "/"}):
            raise ValueError(message)
        # Force port validation here; urllib otherwise defers it until I/O.
        _ = url.port
    except ValueError:
        # Parser errors can echo a malformed port or authority containing a
        # secret. Only this fixed diagnostic may reach logs and artifacts.
        raise ValueError(message) from None
    return parse.urlunsplit((url.scheme, url.netloc, "", "", ""))


class NoSignedRedirects(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # A signature authorizes only the admitted origin and request path.
        # Preserve every 3xx as HTTPError for the ordinary failure evidence.
        return None


class Client:
    def __init__(self, endpoints, admin_endpoints, evidence, *, aws_bin="aws", timeout=30,
                 region="us-east-1", environment=None):
        self.endpoints = [endpoint_url(value) for value in endpoints]
        self.admin_endpoints = [endpoint_url(value) for value in admin_endpoints] or self.endpoints
        self.evidence = evidence
        self.aws_bin = aws_bin
        self.timeout = timeout
        self.region = region
        self.environment = dict(os.environ if environment is None else environment)
        self.access = self.environment.get("RUSTFS_ACCESS_KEY") or self.environment.get("AWS_ACCESS_KEY_ID")
        self.secret = self.environment.get("RUSTFS_SECRET_KEY") or self.environment.get("AWS_SECRET_ACCESS_KEY")
        self.session = self.environment.get("AWS_SESSION_TOKEN")
        if not self.access or not self.secret:
            raise ProbeError("RUSTFS_ACCESS_KEY/RUSTFS_SECRET_KEY or AWS credentials are required", "BLOCKED")
        for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SECURITY_TOKEN"):
            self.environment.pop(name, None)
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            self.environment.pop(name, None)
        self.environment.update(AWS_ACCESS_KEY_ID=self.access, AWS_SECRET_ACCESS_KEY=self.secret,
                                AWS_DEFAULT_REGION=region, AWS_EC2_METADATA_DISABLED="true",
                                AWS_CONFIG_FILE=os.devnull, AWS_SHARED_CREDENTIALS_FILE=os.devnull,
                                AWS_MAX_ATTEMPTS="1", AWS_PAGER="", NO_PROXY="*", no_proxy="*")
        self.http = request.build_opener(request.ProxyHandler({}), NoSignedRedirects())
        self.last_evidence = None

    def _redact(self, text):
        for value in (self.access, self.secret, self.session):
            if value:
                text = text.replace(value, "[REDACTED]")
        return text

    def s3(self, operation, *arguments, endpoint_index=0):
        command = [self.aws_bin, "--endpoint-url", self.endpoints[endpoint_index], "--no-cli-pager",
                   "--region", self.region, "--cli-connect-timeout", str(max(1, int(self.timeout))),
                   "--cli-read-timeout", str(max(1, int(self.timeout))), "s3api", operation, *map(str, arguments)]
        result = run_command(command, self.timeout, self.environment)
        result["collected_at"] = utc_now()
        result["stdout"] = self._redact(result["stdout"])
        result["stderr"] = self._redact(result["stderr"])
        failure_code = re.search(r"An error occurred \(([^)]+)\)", result["stderr"])
        result["service_error_code"] = failure_code.group(1) if failure_code else None
        self.last_evidence = self.evidence.save("s3-" + operation, result)
        if result["timed_out"]:
            raise ProbeError(f"S3 {operation} exceeded the request deadline; inspect {self.last_evidence}")
        if result["exit_code"]:
            # The response is retained so a service 503 is not relabelled as a
            # fixture success. Request transport failures remain distinct.
            product = "An error occurred (" in result["stderr"]
            raise ProbeError(f"S3 {operation} failed; inspect {self.last_evidence}", "FAIL" if product else "INFRA_ERROR")
        try:
            value = json.loads(result["stdout"] or "{}")
        except ValueError as exc:
            raise ProbeError(f"S3 {operation} returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise ProbeError(f"S3 {operation} returned a non-object response")
        return value

    def admin(self, path="/rustfs/admin/v3/datausageinfo", endpoint_index=0):
        url = self.admin_endpoints[endpoint_index] + path
        now = datetime.now(timezone.utc)
        stamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
        host = parse.urlsplit(url).netloc
        payload = hashlib.sha256(b"").hexdigest()
        headers = {"host": host, "x-amz-content-sha256": payload, "x-amz-date": stamp}
        if self.session:
            headers["x-amz-security-token"] = self.session
        signed = ";".join(sorted(headers))
        canonical = "GET\n" + path + "\n\n" + "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
        canonical += "\n" + signed + "\n" + payload
        scope = f"{day}/{self.region}/s3/aws4_request"
        message = "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
        key = ("AWS4" + self.secret).encode()
        for part in (day, self.region, "s3", "aws4_request"):
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        headers["Authorization"] = f"AWS4-HMAC-SHA256 Credential={self.access}/{scope}, SignedHeaders={signed}, Signature={signature}"
        started = time.monotonic()
        try:
            try:
                response = self.http.open(request.Request(url, headers=headers), timeout=self.timeout)
            except error.HTTPError as exc:
                response = exc
            with response:
                status = response.code
                raw = response.read(8 * 1024 * 1024 + 1)
                request_id = response.headers.get("x-amz-request-id")
        except OSError as exc:
            self.last_evidence = self.evidence.save("admin-error", {
                "url": url, "collected_at": utc_now(), "duration_seconds": time.monotonic() - started,
                "error": self._redact(str(exc))})
            raise ProbeError("Admin request failed or timed out; inspect " + self.last_evidence) from exc
        record = {"url": url, "status": status, "request_id": request_id,
                  "collected_at": utc_now(), "duration_seconds": time.monotonic() - started}
        if len(raw) > 8 * 1024 * 1024:
            record["error"] = "response exceeded the 8 MiB evidence limit"
            self.last_evidence = self.evidence.save("admin", record)
            raise ProbeError(record["error"])
        try:
            value = json.loads(raw)
        except ValueError:
            value = None
        record["response"] = json.loads(self._redact(json.dumps(value))) if isinstance(value, dict) else self._redact(raw.decode(errors="replace"))
        self.last_evidence = self.evidence.save("admin", record)
        if status in (401, 403):
            raise ProbeError("Admin credentials lack the required read permissions", "BLOCKED")
        if status in (404, 405):
            raise ProbeError("Candidate does not expose the required Admin endpoint", "UNSUPPORTED")
        if status != 200:
            raise ProbeError(f"Admin returned HTTP {status}; inspect {self.last_evidence}", "FAIL")
        if not isinstance(value, dict):
            raise ProbeError("Admin returned invalid JSON")
        return value


def integer(value):
    return type(value) is int and value >= 0


def bucket_view(snapshot, bucket):
    """Missing fields remain None. Never turn unknown into an empty bucket."""
    mapping = snapshot.get("buckets_usage")
    if not isinstance(mapping, dict):
        raise ProbeError("Admin response has no buckets_usage object")
    entry = mapping.get(bucket)
    if entry is None:
        return {"present": False, "state": "unknown", "count": None, "size": None,
                "versions": None, "markers": None, "certificate": None, "converged": False}
    if not isinstance(entry, dict):
        raise ProbeError("Bucket usage is not an object")
    state = entry.get("usage_value_state")
    if state is not None and not isinstance(state, str):
        raise ProbeError("Bucket usage_value_state must be a string or null", "FAIL")
    known = state in {"certified", "carried", "observed"}
    certificate = entry.get("usage_certification")
    if known and certificate is not None:
        if (not isinstance(certificate, dict) or not integer(certificate.get("epoch"))
                or not integer(certificate.get("cycle"))):
            raise ProbeError("Malformed per-bucket certification", "FAIL")
        clock = certificate.get("certified_at")
        if (not isinstance(clock, dict) or not integer(clock.get("secs_since_epoch"))
                or not integer(clock.get("nanos_since_epoch")) or clock["nanos_since_epoch"] >= 10**9):
            raise ProbeError("Malformed per-bucket certification time", "FAIL")
        if state == "observed":
            raise ProbeError("Observed value incorrectly carries a certificate", "FAIL")
    if state == "certified" and certificate is None:
        raise ProbeError("Certified value has no per-bucket certificate", "FAIL")
    fields = {"count": "objects_count", "size": "size", "versions": "versions_count", "markers": "delete_markers_count"}
    values = {name: entry.get(field) for name, field in fields.items()}
    if any(value is not None and not integer(value) for value in values.values()):
        raise ProbeError("Bucket usage contains a negative or non-integer counter", "FAIL")
    pending = snapshot.get("usage_snapshot_pending_buckets", [])
    if not isinstance(pending, list) or any(not isinstance(item, str) for item in pending):
        raise ProbeError("Malformed usage_snapshot_pending_buckets")
    # Legacy fields must affirm completeness and convergence; absent booleans
    # are not a positive signal. New carried/observed metadata always vetoes it.
    converged = (snapshot.get("usage_snapshot_complete") is True
                 and snapshot.get("usage_snapshot_converged") is True
                 and snapshot.get("usage_snapshot_partial") is not True
                 and snapshot.get("usage_snapshot_observational") is not True
                 and snapshot.get("usage_snapshot_admin_projection") is not True
                 and bucket not in pending and state not in {"carried", "observed"}
                 and (state is None or state == "certified"))
    return {"present": True, "state": state if known else "unknown", **values,
            "certificate": certificate if known else None, "converged": converged,
            "incarnation": entry.get("bucket_incarnation"), "metadata_available": known,
            "pending": bucket in pending, "observational": snapshot.get("usage_snapshot_observational") is True,
            "partial": snapshot.get("usage_snapshot_partial") is True,
            "complete": snapshot.get("usage_snapshot_complete") is True}
