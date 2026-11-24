#!/usr/bin/env python3
"""Verify MRF legacy responsibility refresh accepts >256-byte root keys while capping persisted risk acceptance."""

import argparse
from contextlib import closing, contextmanager
from datetime import UTC, datetime
import hashlib
import hmac
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import secrets
import socket
import struct
import subprocess
import tempfile
import time
import uuid
import zlib

if __package__:
    from .sts_root_regression import stop_process
else:
    from sts_root_regression import stop_process


HOST = "127.0.0.1"
REGION = "us-east-1"
BUCKET = "mrf-actor-proof"
OBJECT_KEY = "mrf-actor-probe"
ACTIONS_PATH = "/rustfs/admin/v4/heal/mrf/responsibilities/actions"
LIST_PATH = "/rustfs/admin/v4/heal/mrf/responsibilities"


def build_partial_write_snapshot(bucket: str = BUCKET, key: str = OBJECT_KEY, owner: uuid.UUID | None = None):
    owner_id = owner or uuid.uuid4()
    bucket_bytes = bucket.encode("utf-8")
    key_bytes = key.encode("utf-8")
    record = (
        struct.pack("<BBBBQBIIII", 1, 2, 1, 0, 100, 0, 0, 0, len(bucket_bytes), len(key_bytes))
        + bucket_bytes
        + key_bytes
    )
    payload = record + struct.pack("<I", zlib.crc32(record))
    manifest_body = (
        b"RFMRFC01\x01"
        + owner_id.bytes
        + struct.pack("<QQ", 1, len(payload))
        + hashlib.sha256(payload).digest()
    )
    manifest = manifest_body + hashlib.sha256(manifest_body).digest()
    return payload, manifest


def sigv4_headers(host_port: str, access_key: str, secret_key: str, method: str, path: str, body: bytes = b""):
    amz_date = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {
        "host": host_port,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if body:
        headers["content-type"] = "application/json"
    signed_names = sorted(headers)
    signed_headers = ";".join(signed_names)
    canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in signed_names)
    canonical_request = "\n".join([method, path, "", canonical_headers, signed_headers, payload_hash])
    scope = f"{date_stamp}/{REGION}/s3/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signing_key = ("AWS4" + secret_key).encode("utf-8")
    for part in (date_stamp, REGION, "s3", "aws4_request"):
        signing_key = hmac.new(signing_key, part.encode("utf-8"), hashlib.sha256).digest()
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers


def signed_request(port: int, access_key: str, secret_key: str, method: str, path: str, body: bytes = b""):
    headers = sigv4_headers(f"{HOST}:{port}", access_key, secret_key, method, path, body)
    with closing(HTTPConnection(HOST, port, timeout=15)) as conn:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        return response.status, response.read()


def verify_refresh_response(status: int, body: bytes, expected_responsibility_id: str):
    if status != 200:
        raise ValueError(f"refresh with 257-byte root actor returned HTTP {status}: {body.decode('utf-8', 'replace')}")
    parsed = json.loads(body.decode("utf-8"))
    if parsed.get("responsibilityId") != expected_responsibility_id or parsed.get("state") != "refreshed":
        raise ValueError(f"unexpected refresh payload: {parsed}")
    if parsed.get("dataVerified") is not False or parsed.get("durableResponsibilityRetained") is not True:
        raise ValueError(f"unexpected retention flags in refresh payload: {parsed}")
    return parsed


def verify_risk_acceptance_refusal(status: int, body: bytes):
    text = body.decode("utf-8", "replace")
    if status != 400 or "invalid authenticated actor" not in text:
        raise ValueError(f"expected HTTP 400 invalid authenticated actor on acceptUnverifiedRisk, got {status}: {text}")


@contextmanager
def four_disk_rustfs(binary: Path, access_key: str, secret_key: str, root: Path):
    with socket.socket() as listener:
        listener.bind((HOST, 0))
        port = listener.getsockname()[1]
    volumes = []
    for idx in range(4):
        disk = root / f"disk-{idx}"
        disk.mkdir(parents=True, exist_ok=True)
        volumes.append(str(disk))
    (root / "logs").mkdir(exist_ok=True)
    env = {
        "PATH": os.environ.get("PATH", os.defpath),
        "NO_PROXY": "*",
        "no_proxy": "*",
        "RUSTFS_UNSAFE_BYPASS_DISK_CHECK": "true",
        "RUSTFS_CONSOLE_ENABLE": "false",
        "RUSTFS_CONSOLE_ADDRESS": f"{HOST}:0",
        "RUSTFS_OBS_LOG_DIRECTORY": str(root / "logs"),
        "RUSTFS_OBS_LOGGER_LEVEL": "warn",
        "RUSTFS_HEAL_MRF_ENABLE": "true",
    }
    cmd = [
        str(binary.resolve()),
        "--address",
        f"{HOST}:{port}",
        "--access-key",
        access_key,
        "--secret-key",
        secret_key,
        "--region",
        REGION,
        *volumes,
    ]
    with (root / "server.log").open("ab") as log:
        process = subprocess.Popen(
            cmd, env=env, cwd=root, start_new_session=True, stdin=subprocess.DEVNULL, stdout=log, stderr=log
        )
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"RustFS exited during startup ({process.returncode})")
            try:
                with closing(HTTPConnection(HOST, port, timeout=1)) as conn:
                    conn.request("GET", "/health/ready")
                    if conn.getresponse().status == 200:
                        break
            except OSError:
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("RustFS readiness deadline expired")
        yield port, [Path(v) for v in volumes]
    finally:
        stop_process(process)


def exercise(binary: Path):
    access_key = "M" * 257
    secret_key = f"mrf-secret-{secrets.token_hex(12)}"
    with tempfile.TemporaryDirectory(prefix="rustfs-mrf-3194-") as directory:
        root = Path(directory)
        with four_disk_rustfs(binary, access_key, secret_key, root) as (port, disks):
            status, body = signed_request(port, access_key, secret_key, "PUT", f"/{BUCKET}")
            if status != 200:
                raise ValueError(f"CreateBucket failed with HTTP {status}: {body.decode('utf-8', 'replace')}")

        payload, manifest = build_partial_write_snapshot()
        for disk in disks:
            sys_dir = disk / ".rustfs.sys"
            sys_dir.mkdir(parents=True, exist_ok=True)
            (sys_dir / ".heal-mrf-snapshot.0.bin").write_bytes(payload)
            (sys_dir / ".heal-mrf-commit.0.bin").write_bytes(manifest)

        with four_disk_rustfs(binary, access_key, secret_key, root) as (port, _):
            deadline = time.monotonic() + 15
            entry = None
            while time.monotonic() < deadline:
                status, body = signed_request(port, access_key, secret_key, "GET", LIST_PATH)
                if status == 200:
                    rows = json.loads(body.decode("utf-8")).get("responsibilities", [])
                    matches = [r for r in rows if r.get("bucket") == BUCKET and r.get("object") == OBJECT_KEY]
                    if matches and matches[0].get("bucketIncarnationId"):
                        entry = matches[0]
                        break
                time.sleep(0.2)
            if entry is None:
                raise ValueError("seeded MRF legacy responsibility did not surface with an observed bucketIncarnationId")

            responsibility_id = entry["responsibilityId"]
            incarnation_id = entry["bucketIncarnationId"]
            refresh_body = json.dumps({
                "action": "refresh",
                "responsibilityId": responsibility_id,
                "expectedBucketIncarnationId": incarnation_id,
            }).encode("utf-8")
            status, body = signed_request(port, access_key, secret_key, "POST", ACTIONS_PATH, refresh_body)
            verify_refresh_response(status, body, responsibility_id)

            risk_body = json.dumps({
                "action": "acceptUnverifiedRisk",
                "responsibilityId": responsibility_id,
                "expectedBucketIncarnationId": incarnation_id,
                "acknowledgeUnverifiedDataRisk": True,
                "acknowledgeUnknownSourceIncarnation": True,
                "acknowledgeBucketIncarnationMismatch": False,
                "reason": "operator risk acceptance actor cap check",
                "reference": "INC-3194",
                "requestId": str(uuid.uuid4()),
            }).encode("utf-8")
            status, body = signed_request(port, access_key, secret_key, "POST", ACTIONS_PATH, risk_body)
            verify_risk_acceptance_refusal(status, body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustfs-bin", required=True, type=Path)
    args = parser.parse_args()
    exercise(args.rustfs_bin)
    print(json.dumps({"case_id": "admin.mrf-long-root-refresh", "verified_statuses": [200, 400]}))


if __name__ == "__main__":
    main()
