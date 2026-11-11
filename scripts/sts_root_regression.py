#!/usr/bin/env python3
"""Issue #9279: root credential startup boundaries and signed STS Query requests."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET


ASSUME_ROLE_BODY = (
    "Action=AssumeRole&Version=2011-06-15&"
    "RoleArn=arn:aws:iam::000000000000:role/user&"
    "RoleSessionName=repro&DurationSeconds=3600"
)
# Public, disposable fixtures; never use these credentials for a real deployment.
# Lengths are UTF-8 bytes, matching Rust's str::len().
CASES = {
    "minimum": ("STS-110", "abc", "rootroot", None),
    "secret-8": ("STS-111", "root", "rootroot", None),
    "secret-8-utf8": ("STS-112", "root", "éééé", None),
    "secret-4": ("STS-107", "root", "root", "secret"),
    "secret-7": ("STS-108", "root", "root123", "secret"),
    "access-2": ("STS-109", "ab", "rootroot", "access"),
}


class RegressionFailure(Exception):
    """A bounded diagnostic that contains no response bodies or credentials."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RegressionFailure(message)


def config_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace(
        "\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + '"'


def http(endpoint: str, access: str = "", secret: str = "", token: str = "",
         *, path: str = "/", body: str | None = None, timeout: float = 10) -> tuple[int, bytes]:
    # curl's signer reproduces the report's aws:amz:us-east-1:s3 scope. Keep
    # credentials in stdin, not argv, and never dump its raw response on failure.
    config = ""
    if access:
        config = 'aws-sigv4 = "aws:amz:us-east-1:s3"\n'
        config += "user = " + config_quote(access + ":" + secret) + "\n"
    if token:
        config += "header = " + config_quote("x-amz-security-token: " + token) + "\n"
    with tempfile.TemporaryDirectory(prefix="rustfs-sts-http-") as directory:
        output = Path(directory) / "response"
        command = ["curl", "--disable", "--silent", "--show-error", "--noproxy", "*",
                   "--max-time", str(timeout), "--config", "-", "--output", str(output),
                   "--write-out", "%{http_code}"]
        if body is not None:
            command += ["--request", "POST", "--header", "Content-Type: application/x-www-form-urlencoded",
                        "--data-raw", body]
        command += [endpoint.rstrip("/") + path]
        try:
            completed = subprocess.run(command, input=config, capture_output=True, text=True,
                                       timeout=timeout + 5)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RegressionFailure("curl could not complete the request") from exc
        require(completed.returncode == 0, f"curl transport failed (exit {completed.returncode})")
        require(bool(re.fullmatch(r"[1-5][0-9]{2}", completed.stdout)), "curl did not return an HTTP status")
        require(output.is_file() and output.stat().st_size <= 2 * 1024 * 1024,
                "HTTP response missing or larger than 2 MiB")
        return int(completed.stdout), output.read_bytes()


def xml(body: bytes) -> ET.Element:
    try:
        return ET.fromstring(body)
    except ET.ParseError as exc:
        raise RegressionFailure("response is not valid XML") from exc


def tag(element: ET.Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def sts_response(status: int, body: bytes, observation: dict) -> ET.Element:
    observation["sts_http_status"] = status
    root = xml(body)
    code = root.findtext(".//{*}Code", default="")
    # Only copy structured, bounded identifiers, never untrusted message text.
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", code):
        observation["sts_error_code"] = code
    request_id = root.findtext(".//{*}RequestId", default="")
    if re.fullmatch(r"[A-Za-z0-9-]{1,128}", request_id):
        observation["request_id"] = request_id
    return root


def check_assume_role(endpoint: str, access: str, secret: str, observation: dict) -> None:
    status, body = http(endpoint, access, secret, body=ASSUME_ROLE_BODY)
    root = sts_response(status, body, observation)
    require(status == 200, f"AssumeRole expected HTTP 200, received {status}")
    require(tag(root) == "AssumeRoleResponse" and root.find(".//{*}Error") is None,
            "AssumeRole did not return a success envelope")
    credentials = root.find("{*}AssumeRoleResult/{*}Credentials")
    require(credentials is not None, "AssumeRole omitted Credentials")
    fields = {name: credentials.findtext("{*}" + name, default="").strip()
              for name in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")}
    require(all(fields.values()), "AssumeRole omitted a credential field or Expiration")
    require(fields["AccessKeyId"] != access, "AssumeRole returned the parent access key")
    try:
        expiration = datetime.fromisoformat(fields["Expiration"].replace("Z", "+00:00"))
        require(expiration.tzinfo is not None, "STS Expiration omitted its timezone")
        remaining = (expiration - datetime.now(timezone.utc)).total_seconds()
    except ValueError as exc:
        raise RegressionFailure("STS Expiration is not an ISO-8601 timestamp") from exc
    require(0 < remaining <= 3660, "STS Expiration is outside the requested 3600 second lifetime")
    observation["session_lifetime_seconds"] = round(remaining)
    status, body = http(endpoint, fields["AccessKeyId"], fields["SecretAccessKey"], fields["SessionToken"])
    observation["s3_http_status"] = status
    require(status == 200, f"temporary credentials could not ListBuckets (HTTP {status})")
    require(tag(xml(body)) == "ListAllMyBucketsResult", "ListBuckets did not return an S3 success envelope")


def credential_rejection(log: str, field: str) -> bool:
    # An unrelated bind/configuration error or a crash must not pass a negative
    # startup case. Accept spelling differences across the CLI and config layer.
    key = re.compile(rf"{field}[_ -]?key", re.IGNORECASE)
    invalid = re.compile(r"invalid|error|too short|at least|minimum", re.IGNORECASE)
    length = re.compile(r"length|bytes?|too short|at least|minimum", re.IGNORECASE)
    return any(key.search(line) and invalid.search(line) and length.search(line)
               and not re.search(r"\bwarn(?:ing)?\b", line, re.IGNORECASE) for line in log.splitlines())


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
    except ProcessLookupError:
        process.wait(timeout=5)


def check_startup(binary: str, name: str, timeout: float, observation: dict) -> None:
    case_id, access, secret, invalid_field = CASES[name]
    observation.update(case_id=case_id, access_key_bytes=len(access.encode()), secret_key_bytes=len(secret.encode()),
                       expected="startup-rejected" if invalid_field else "usable-sts-session")
    directory = Path(tempfile.mkdtemp(prefix="rustfs-sts-root-")).resolve()
    process = None
    try:
        (directory / "data").mkdir()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}"
        # Do not inherit credentials, credential files, volumes, TLS, KMS, or
        # other cluster settings from the caller's environment.
        environment = {
            "PATH": os.environ.get("PATH", os.defpath),
            "RUSTFS_ACCESS_KEY": access, "RUSTFS_SECRET_KEY": secret,
            "RUSTFS_VOLUMES": str(directory / "data"), "RUSTFS_ADDRESS": f"127.0.0.1:{port}",
            "RUSTFS_CONSOLE_ENABLE": "false", "RUSTFS_CONSOLE_ADDRESS": "127.0.0.1:0",
            "RUSTFS_OBS_LOG_DIRECTORY": str(directory / "logs"), "RUSTFS_OBS_LOGGER_LEVEL": "warn",
        }
        log_path = directory / "server.log"
        with log_path.open("wb") as log:
            process = subprocess.Popen([binary], cwd=directory, env=environment, stdout=log, stderr=log,
                                       start_new_session=True)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = process.poll()
            if code is not None:
                observation["server_exit_code"] = code
                require(invalid_field is not None, "RustFS exited before the valid-credential instance was ready")
                require(0 < code < 128, "invalid credentials did not cause a normal nonzero startup exit")
                with log_path.open("rb") as log:
                    diagnostic = log.read(256 * 1024).decode(errors="replace")
                require(credential_rejection(diagnostic, invalid_field),
                        "startup failed without the expected credential-length diagnostic")
                observation["credential_length_rejected"] = True
                return
            try:
                status, _ = http(endpoint, path="/health/ready", timeout=1)
            except RegressionFailure:
                status = 0
            observation["readiness_http_status"] = status
            if status == 200 and process.poll() is None:
                observation["server_ready"] = True
                if invalid_field:
                    # Preserve the distinguishing 1.0.5 HTTP 500/InternalError
                    # evidence, but even HTTP 403 cannot excuse invalid startup.
                    try:
                        status, body = http(endpoint, access, secret, body=ASSUME_ROLE_BODY)
                        sts_response(status, body, observation)
                    except RegressionFailure:
                        observation["sts_probe_failed"] = True
                    raise RegressionFailure("RustFS accepted credentials below the startup minimum")
                check_assume_role(endpoint, access, secret, observation)
                require(process.poll() is None, "RustFS exited during the STS check")
                return
            time.sleep(0.2)
        raise RegressionFailure("RustFS neither rejected the credentials nor became ready before the deadline")
    finally:
        primary_error = sys.exc_info()[1]
        try:
            if process is not None:
                stop_process(process)
            shutil.rmtree(directory)
            observation["cleanup"] = "PASS"
        except (OSError, subprocess.TimeoutExpired) as exc:
            observation["cleanup"] = "FAIL"
            observation["cleanup_reason"] = "could not stop the isolated process or remove its private data"
            if primary_error is None:
                raise RegressionFailure(observation["cleanup_reason"]) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--endpoint", help="check the running server using RUSTFS_ACCESS_KEY/RUSTFS_SECRET_KEY")
    target.add_argument("--rustfs-bin", help="start disposable loopback instances of this candidate binary")
    parser.add_argument("--case", choices=["all", *CASES], default="all")
    parser.add_argument("--startup-timeout", type=float, default=60)
    args = parser.parse_args()
    if args.startup_timeout <= 0:
        parser.error("--startup-timeout must be positive")
    if args.endpoint and args.case != "all":
        parser.error("--case requires --rustfs-bin")
    cases = ["configured-root"] if args.endpoint else list(CASES) if args.case == "all" else [args.case]
    failed = False
    for name in cases:
        observation = {"issue": 9279, "case": name}
        try:
            if args.endpoint:
                access = os.environ.get("RUSTFS_ACCESS_KEY", "")
                secret = os.environ.get("RUSTFS_SECRET_KEY", "")
                require(bool(access and secret), "RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY are required")
                observation.update(case_id="STS-106", access_key_bytes=len(access.encode()), secret_key_bytes=len(secret.encode()))
                check_assume_role(args.endpoint, access, secret, observation)
            else:
                binary = shutil.which(args.rustfs_bin)
                require(binary is not None, "candidate RustFS binary is unavailable")
                check_startup(str(Path(binary).resolve()), name, args.startup_timeout, observation)
            observation["verdict"] = "PASS"
        except (RegressionFailure, OSError, subprocess.TimeoutExpired) as exc:
            observation["verdict"] = "FAIL"
            observation["reason"] = str(exc) if isinstance(exc, RegressionFailure) else "could not execute or clean up the probe"
            failed = True
        print(json.dumps(observation, sort_keys=True), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    # A runner cancellation still tears down the child process and its private
    # data. No system service, shared volume, or unrelated process is touched.
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    for interrupt_signal in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(interrupt_signal, interrupted)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
