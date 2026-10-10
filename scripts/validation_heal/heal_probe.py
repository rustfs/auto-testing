#!/usr/bin/env python3
"""Bounded S3/Admin evidence collector for operator-assisted Heal fleet runs.

This is an operation tool, not an 84-case PASS generator. Run on the jump-host
runner, using a reviewed, expiring lab binding and credentials from environment.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid
from urllib import request
from urllib.error import HTTPError
from urllib.parse import unquote, urlencode, urlsplit


MAX_BODY = 64 * 1024**2
# Only compare monotonic observations from this collector process. Other
# processes/hosts have a different domain even if wall clocks look aligned.
CLOCK_DOMAIN = str(uuid.uuid4())


@contextmanager
def observation_phase(observation, name):
    """Measure an optional collector phase without adding evidence writes."""
    if observation is None:
        yield
        return
    phase = {'startedMonotonicNs': time.monotonic_ns(), 'completed': False}
    observation['phases'][name] = phase
    try:
        yield
        phase['completed'] = True
    finally:
        phase['finishedMonotonicNs'] = time.monotonic_ns()


S3_OPERATIONS = frozenset("""
create_bucket head_bucket delete_bucket put_object get_object head_object delete_object
delete_objects list_objects_v2 list_object_versions copy_object
put_bucket_versioning get_bucket_versioning put_bucket_tagging get_bucket_tagging
put_bucket_policy get_bucket_policy delete_bucket_policy
put_object_tagging get_object_tagging delete_object_tagging
create_multipart_upload upload_part list_parts complete_multipart_upload
abort_multipart_upload list_multipart_uploads
put_bucket_encryption get_bucket_encryption delete_bucket_encryption
put_object_retention get_object_retention put_object_legal_hold get_object_legal_hold
put_object_lock_configuration get_object_lock_configuration
put_bucket_lifecycle_configuration get_bucket_lifecycle_configuration
delete_bucket_lifecycle put_bucket_replication get_bucket_replication
delete_bucket_replication restore_object get_bucket_location
""".split())
SECRET_FIELDS = {"authorization", "ssecustomerkey", "ssecustomerkeymd5",
                 "copysourcessecustomerkey", "copysourcessecustomerkeymd5",
                 "aws_access_key_id", "aws_secret_access_key", "aws_session_token"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def redact(value):
    if isinstance(value, dict):
        return {key: "[REDACTED]" if key.lower() in SECRET_FIELDS else redact(item)
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        for name in ("HEAL_ACCESS_KEY", "HEAL_SECRET_KEY", "HEAL_SESSION_TOKEN", "HEAL_SSE_C_KEY"):
            secret = os.environ.get(name)
            if secret:
                value = value.replace(secret, "[REDACTED]")
    return value


def save(path: Path, value: dict) -> None:
    # Never overwrite earlier observations, including failed operations.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as output:
        output.write(json.dumps(redact(value), ensure_ascii=False, indent=2, default=str) + "\n")
        output.flush()
        os.fsync(output.fileno())
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def validate_binding(binding: dict, now: datetime | None = None) -> None:
    require(binding.get("schemaVersion") == 1, "unsupported lab binding")
    require(bool(re.fullmatch(r"(?:gh|manual)-[a-z0-9-]{5,60}", binding.get("runId", ""))),
            "invalid run ID")
    require(binding.get("jumpHostOnly") is True and binding.get("exclusiveHealService") is True,
            "an exclusive Heal service and jump-host route must be assigned")
    require(binding.get("assignmentRequired", False) is False, "example/unassigned lab cannot run")
    expiry = datetime.fromisoformat(binding["leaseExpiresAt"].replace("Z", "+00:00"))
    require(expiry.tzinfo is not None and expiry > (now or datetime.now(UTC)), "fleet lease expired")
    require(bool(re.fullmatch(r"rv-heal-[a-z0-9-]{6,35}", binding.get("bucketPrefix", ""))),
            "dedicated bucket prefix required")
    endpoints = binding.get("endpoints", {})
    require(bool(endpoints) and binding.get("defaultEndpoint") in endpoints, "missing endpoint binding")
    for name, endpoint in endpoints.items():
        require(bool(re.fullmatch(r"[a-zA-Z0-9_-]+", name)), "invalid endpoint name")
        parsed = urlsplit(endpoint)
        require(parsed.scheme in {"http", "https"} and parsed.hostname is not None
                and parsed.username is None and parsed.password is None and not parsed.query
                and not parsed.fragment and parsed.path in {"", "/"}, "invalid endpoint URL")
        require(parsed.port is not None and 1024 <= parsed.port <= 65535,
                "an explicitly allocated test port is required")
    for name in ("candidateCommit", "harnessCommit"):
        require(bool(re.fullmatch(r"[0-9a-f]{40}", binding.get(name, ""))), f"missing pinned {name}")
    for name in ("candidatePackageSha256", "candidateBinarySha256", "rcBinarySha256"):
        require(bool(re.fullmatch(r"[0-9a-f]{64}", binding.get(name, ""))), f"missing pinned {name}")
    require(type(binding.get("maxInventoryObjects")) is int
            and binding["maxInventoryObjects"] in range(1, 10001), "invalid inventory object budget")
    require(type(binding.get("maxInventoryBytes")) is int
            and 1 <= binding["maxInventoryBytes"] <= 64 * 1024**3, "invalid inventory byte budget")


def bucket_allowed(binding: dict, bucket: str) -> None:
    prefix = binding["bucketPrefix"]
    require(isinstance(bucket, str) and len(bucket) <= 63
            and (bucket == prefix or bucket.startswith(prefix + "-"))
            and bool(re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", bucket)), "bucket is outside this run")


def validate_s3_request(binding: dict, operation: str, params: dict) -> None:
    require(operation in S3_OPERATIONS, "S3 operation is not allowlisted")
    bucket_allowed(binding, params.get("Bucket", ""))
    source = params.get("CopySource")
    if source is not None:
        require(isinstance(source, dict), "CopySource must use the structured SDK form")
        bucket_allowed(binding, source.get("Bucket", ""))
    if operation == "delete_objects":
        items = params.get("Delete", {}).get("Objects", [])
        require(0 < len(items) <= 1000 and all(isinstance(item.get("Key"), str) for item in items),
                "invalid bounded batch deletion")
    require(not params.get("BypassGovernanceRetention"), "retention bypass is prohibited")
    require("Body" not in params, "use --size/--seed or --body-file; do not put payloads in JSON")


def payload(seed: str, size: int) -> bytes:
    require(type(size) is int and 0 <= size <= MAX_BODY, "payload exceeds 64 MiB budget")
    return hashlib.shake_256(seed.encode()).digest(size)


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Admin redirects are prohibited")


class Probe:
    def __init__(self, binding: dict, endpoint: str | None = None, *, credential_pair=None):
        validate_binding(binding)
        self.binding = binding
        self.endpoint_name = endpoint or binding["defaultEndpoint"]
        require(self.endpoint_name in binding["endpoints"], "endpoint is not bound")
        self.endpoint = binding["endpoints"][self.endpoint_name].rstrip("/")
        require(credential_pair is None or isinstance(credential_pair, tuple) and len(credential_pair) == 2
                and all(isinstance(value, str) and value for value in credential_pair), "invalid in-memory principal credentials")
        self.credential_pair = credential_pair

    def credentials(self):
        return self.credential_pair or (os.environ["HEAL_ACCESS_KEY"], os.environ["HEAL_SECRET_KEY"])

    def s3_client(self):
        import boto3
        from botocore.config import Config
        access, secret = self.credentials()
        return boto3.client("s3", endpoint_url=self.endpoint,
                            aws_access_key_id=access, aws_secret_access_key=secret,
                            aws_session_token=None if self.credential_pair else os.environ.get("HEAL_SESSION_TOKEN"),
                            region_name="us-east-1",
                            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                          retries={"max_attempts": 0}, connect_timeout=5,
                                          read_timeout=30, proxies={}))

    def s3(self, operation: str, params: dict, body: bytes | None = None) -> dict:
        validate_binding(self.binding)  # Recheck expiry before every mutation.
        validate_s3_request(self.binding, operation, params)
        outgoing = dict(params)
        for key in ("SSECustomerKey", "CopySourceSSECustomerKey"):
            if outgoing.get(key) == "$HEAL_SSE_C_KEY":
                outgoing[key] = os.environ["HEAL_SSE_C_KEY"]
        if body is not None:
            require(operation in {"put_object", "upload_part"}, "body not allowed for this operation")
            outgoing["Body"] = body
        started = time.monotonic()
        evidence = {"operation": operation, "params": redact(params), "endpoint": self.endpoint_name,
                    "startedAt": datetime.now(UTC).isoformat(), "clockDomain": CLOCK_DOMAIN,
                    "callStartedMonotonicNs": time.monotonic_ns(),
                    "clientCallInvoked": False,
                    "timingScope": "client-call; not a server commit/lock witness"}
        if body is not None:
            evidence["payload"] = {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        client = None
        try:
            client = self.s3_client()
            evidence['callStartedMonotonicNs'] = time.monotonic_ns()
            evidence['clientCallInvoked'] = True
            response = getattr(client, operation)(**outgoing)
            stream = response.pop("Body", None)
            if stream is not None:
                digest, count = hashlib.sha256(), 0
                try:
                    while chunk := stream.read(1024**2):
                        count += len(chunk)
                        require(count <= MAX_BODY, "GET exceeds per-object evidence budget")
                        digest.update(chunk)
                finally:
                    stream.close()
                evidence["payload"] = {"size": count, "sha256": digest.hexdigest()}
                require(response.get("ContentLength") == count, "GET length differs from response ContentLength")
            evidence.update(response=response, acknowledged=True)
        except Exception as exc:
            response = getattr(exc, "response", None)
            evidence.update(response=response, acknowledged=False,
                            resultUnknown=response is None,
                            error={"type": type(exc).__name__, "message": str(exc)})
        finally:
            evidence["callFinishedMonotonicNs"] = time.monotonic_ns()
            if client is not None:
                try:client.close()
                except Exception as exc:
                    evidence['clientCleanupError']={'type':type(exc).__name__,'message':str(exc)}
        evidence["elapsedSeconds"] = time.monotonic() - started
        evidence["finishedAt"] = datetime.now(UTC).isoformat()
        return evidence

    def admin(self, path: str, query: list[tuple[str, str]], body: bytes,
              method: str = "POST", auth: str = "signed", *, observation=None) -> dict:
        from botocore.awsrequest import AWSRequest
        try:
            from .heal_signing import sign_admin_request
        except ImportError:
            from heal_signing import sign_admin_request
        with observation_phase(observation, 'requestPreparation'):
            validate_binding(self.binding)
            require(path.startswith("/rustfs/admin/v3/heal/") or path in {
                "/rustfs/admin/v3/background-heal/status", "/rustfs/admin/v4/heal/replacement-recovery"},
                "only Heal Admin routes are allowed")
            require("?" not in path and "#" not in path and len(body) <= 2 * 1024**2,
                    "invalid Admin path or body budget")
            require(not any(part in {".", ".."} for part in unquote(path).split("/")),
                    "Admin route traversal is prohibited")
            # Root traversal is safe only because the binding attests a dedicated
            # service. Bucket paths are still independently bounded to this run.
            suffix = path.removeprefix("/rustfs/admin/v3/heal/")
            if path.startswith("/rustfs/admin/v3/heal/") and suffix:
                bucket_allowed(self.binding, unquote(suffix.split("/", 1)[0]))
            url = self.endpoint + path + ("?" + urlencode(query) if query else "")
            headers = {"Content-Type": "application/json", "x-amz-content-sha256": hashlib.sha256(body).hexdigest()}
        with observation_phase(observation, 'signing'):
            require(auth in {"signed", "anonymous", "bad-signature", "expired-signature"}, "unsupported Admin auth mode")
            signing_time = None
            if auth != "anonymous":
                access, secret = self.credentials()
                if auth == "bad-signature":
                    secret = "deliberately-invalid-heal-signature"
                if auth == "expired-signature":
                    signing_time = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y%m%dT%H%M%SZ")
                outgoing = AWSRequest(method=method, url=url, data=body, headers=headers)
                sign_admin_request(outgoing, access, secret,
                                   None if self.credential_pair else os.environ.get("HEAL_SESSION_TOKEN"), timestamp=signing_time)
                headers = dict(outgoing.headers.items())
        started = time.monotonic()
        evidence = {"path": path, "query": query, "requestBody": body.decode(errors="replace"),
                    "method": method, "auth": auth, "endpoint": self.endpoint_name,
                    "startedAt": datetime.now(UTC).isoformat(), "clockDomain": CLOCK_DOMAIN,
                    "callStartedMonotonicNs": time.monotonic_ns(),
                    "timingScope": "client-call; not a server commit/lock witness"}
        if signing_time:
            evidence["isolatedClientSigningTimestamp"] = signing_time
        if observation is not None:
            evidence['statusObservation'] = dict(sessionId=observation['sessionId'], sequence=observation['sequence'])
            observation['callStartedMonotonicNs'] = evidence['callStartedMonotonicNs']
        try:
            response = self._open_admin(request.Request(url, data=body if method != "GET" else None,
                                                       method=method, headers=headers))
        except HTTPError as exc:
            response = exc
        with observation_phase(observation, 'responseBody'):
            with response:
                raw = response.read(2 * 1024**2 + 1)
                require(len(raw) <= 2 * 1024**2, "Admin response exceeds evidence budget")
                length = response.headers.get('Content-Length')
                require(length is None or (isinstance(length, str) and length.isdigit() and int(length) == len(raw)),
                        'Admin response Content-Length differs from complete body')
                evidence["httpStatus"] = response.code
                evidence["responseHeaders"] = dict(response.headers.items())
        with observation_phase(observation, 'responseParsing'):
            try:
                evidence["response"] = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                evidence["response"] = raw.decode(errors="replace")
        evidence["elapsedSeconds"] = time.monotonic() - started
        evidence["callFinishedMonotonicNs"] = time.monotonic_ns()
        evidence["finishedAt"] = datetime.now(UTC).isoformat()
        if observation is not None:
            observation.update(callFinishedMonotonicNs=evidence['callFinishedMonotonicNs'], responseObserved=True)
        return evidence

    def _open_admin(self, outgoing):
        opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
        return opener.open(outgoing, timeout=30)

    def inventory(self, bucket: str, *, unversioned_current_tags: bool = False, page_size: int | None = None) -> dict:
        """Enumerate every version/marker, then hash complete version-addressed GETs.

        Only call before injection or after attribution/physical proof: a GET can
        itself trigger read repair. Never use this to watch scanner progress.
        """
        bucket_allowed(self.binding, bucket)
        require(page_size is None or type(page_size) is int and 1 <= page_size <= 1000,
                "invalid explicit inventory page size")
        client, entries, count, byte_count = self.s3_client(), [], 0, 0
        pagination = {"requestedPageSize": page_size, "versionPageSizes": [], "currentPageSizes": []}
        options = {"PaginationConfig": {"PageSize": page_size}} if page_size else {}
        if unversioned_current_tags:
            mode = self.s3("get_bucket_versioning", {"Bucket": bucket})
            require(mode["acknowledged"] and mode["response"].get("Status") is None,
                    "current-tag addressing requires a never-versioned fixture bucket")
        for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket, **options):
            page_count = len(page.get("Versions", [])) + len(page.get("DeleteMarkers", []))
            require(page_size is None or page_count <= page_size, "version LIST exceeded the requested page size")
            pagination["versionPageSizes"].append(page_count)
            require(not unversioned_current_tags or not page.get("DeleteMarkers"),
                    "unversioned fixture unexpectedly has delete markers")
            for marker in page.get("DeleteMarkers", []):
                entries.append({"key": marker["Key"], "versionId": marker["VersionId"],
                                "isLatest": marker["IsLatest"], "deleteMarker": True})
                count += 1
            for version in page.get("Versions", []):
                count += 1
                require(count <= self.binding["maxInventoryObjects"], "inventory object budget exceeded")
                params = {"Bucket": bucket, "Key": version["Key"], "VersionId": version["VersionId"]}
                result = self.s3("get_object", params)
                require(result["acknowledged"], "version GET failed: " + canonical(result))
                tag_params = dict(params)
                if unversioned_current_tags:
                    require(version["VersionId"] == "null" and version["IsLatest"] is True,
                            "current-tag addressing cannot read historical versions")
                    tag_params.pop("VersionId")
                tags = self.s3("get_object_tagging", tag_params)
                require(tags["acknowledged"], "version tag GET failed: " + canonical(tags))
                response = result["response"]
                byte_count += result["payload"]["size"]
                require(byte_count <= self.binding["maxInventoryBytes"], "inventory byte budget exceeded")
                entries.append({"key": version["Key"], "versionId": version["VersionId"],
                                "isLatest": version["IsLatest"], "deleteMarker": False,
                                "payload": result["payload"],
                                "properties": {key: response[key] for key in (
                                    "ETag", "ContentLength", "ContentType", "ContentEncoding",
                                    "ContentDisposition", "CacheControl", "Metadata", "StorageClass",
                                    "ServerSideEncryption", "ObjectLockMode", "ObjectLockRetainUntilDate",
                                    "ObjectLockLegalHoldStatus") if key in response},
                                "tags": sorted(tags["response"].get("TagSet", []), key=lambda item: item["Key"])})
            require(count <= self.binding["maxInventoryObjects"], "inventory object budget exceeded")
        identities = [(entry["key"], entry["versionId"]) for entry in entries]
        require(len(set(identities)) == len(identities), "duplicate version/marker in pagination")
        # Cross-check current LIST against the separately paginated version view.
        current = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, **options):
            require(page_size is None or len(page.get("Contents", [])) <= page_size,
                    "current LIST exceeded the requested page size")
            pagination["currentPageSizes"].append(len(page.get("Contents", [])))
            current.extend((item["Key"], item["Size"], item["ETag"]) for item in page.get("Contents", []))
            require(len(current) <= self.binding["maxInventoryObjects"], "current LIST exceeds budget")
        expected = [(item["key"], item["payload"]["size"], item["properties"]["ETag"])
                    for item in entries if item["isLatest"] and not item["deleteMarker"]]
        require(sorted(current) == sorted(expected), "current LIST differs from version manifest")
        if unversioned_current_tags:
            mode = self.s3("get_bucket_versioning", {"Bucket": bucket})
            require(mode["acknowledged"] and mode["response"].get("Status") is None,
                    "fixture versioning changed during inventory")
        return {"bucket": bucket, "entries": sorted(entries, key=lambda item: (item["key"], item["versionId"])),
                "objectsAndMarkers": count, "bytesRead": byte_count,
                "tagAddressing": "current-never-versioned-fixture" if unversioned_current_tags else "explicit-version",
                "pagination": pagination}


def compare_inventories(before: dict, after: dict) -> dict:
    require(before["bucket"] == after["bucket"] and bool(before["entries"]), "wrong or empty baseline")
    require(canonical(before["entries"]) == canonical(after["entries"]), "S3 versions/content/metadata/tags changed")
    return {"equal": True, "objectsAndMarkers": len(before["entries"]),
            "proof": "full-version-content-and-public-metadata", "physicalRepairProved": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--endpoint")
    parser.add_argument("--out", type=Path, required=True)
    subs = parser.add_subparsers(dest="command", required=True)
    admin = subs.add_parser("admin")
    admin.add_argument("--path", required=True)
    admin.add_argument("--method", choices=("POST", "GET"), default="POST")
    admin.add_argument("--query", action="append", default=[])
    admin.add_argument("--body", default="")
    admin.add_argument("--body-file", type=Path)
    admin.add_argument("--auth", choices=("signed", "anonymous", "bad-signature", "expired-signature"), default="signed")
    admin.add_argument("--expect-http", type=int, default=200)
    s3 = subs.add_parser("s3")
    s3.add_argument("--operation", choices=sorted(S3_OPERATIONS), required=True)
    s3.add_argument("--params", default="{}")
    s3.add_argument("--params-file", type=Path)
    s3.add_argument("--size", type=int)
    s3.add_argument("--seed", default="heal")
    s3.add_argument("--body-file", type=Path)
    s3.add_argument("--expect-http", type=int, default=200)
    inventory = subs.add_parser("inventory")
    inventory.add_argument("--bucket", required=True)
    compare = subs.add_parser("compare")
    compare.add_argument("--before", type=Path, required=True)
    compare.add_argument("--after", type=Path, required=True)
    subs.add_parser("binding-check")
    args = parser.parse_args()
    require(not args.out.exists(), "output already exists; preserve earlier evidence")
    binding = json.loads(args.binding.read_text())
    evidence = {"schemaVersion": 1, "runId": binding.get("runId"),
                "bindingSha256": hashlib.sha256(args.binding.read_bytes()).hexdigest(),
                "capturedAt": datetime.now(UTC).isoformat(), "command": args.command,
                "casePassClaimed": False}
    exit_code = 0
    try:
        probe = Probe(binding, args.endpoint)
        if args.command == "admin":
            query = []
            for item in args.query:
                require("=" in item, "query must be name=value; repeats are preserved")
                query.append(tuple(item.split("=", 1)))
            body = args.body_file.read_bytes() if args.body_file else args.body.encode()
            evidence["observation"] = probe.admin(args.path, query, body, args.method, args.auth)
            require(evidence["observation"]["httpStatus"] == args.expect_http, "unexpected Admin HTTP status")
        elif args.command == "s3":
            params = json.loads(args.params_file.read_text() if args.params_file else args.params)
            require(not (args.body_file and args.size is not None), "choose file or generated body")
            if args.body_file:
                require(args.body_file.stat().st_size <= MAX_BODY, "body file exceeds budget")
                body = args.body_file.read_bytes()
            else:
                body = payload(args.seed, args.size) if args.size is not None else None
            result = probe.s3(args.operation, params, body)
            evidence["observation"] = result
            actual = (result.get("response") or {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
            require(actual == args.expect_http, "unexpected S3 HTTP status (unknown writes require ACK reconciliation)")
        elif args.command == "inventory":
            evidence["inventory"] = probe.inventory(args.bucket)
        elif args.command == "compare":
            before, after = (json.loads(path.read_text())["inventory"] for path in (args.before, args.after))
            evidence["comparison"] = compare_inventories(before, after)
        else:
            evidence["bindingCheck"] = {"valid": True, "liveEnvironmentVerified": False}
        evidence["operationCheck"] = "PASS"
    except Exception as exc:
        evidence.update(operationCheck="FAIL", error={"type": type(exc).__name__, "message": str(exc)})
        exit_code = 1
    save(args.out, evidence)
    print(json.dumps({"evidence": str(args.out), "operationCheck": evidence["operationCheck"],
                      "casePassClaimed": False}))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
