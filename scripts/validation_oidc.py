#!/usr/bin/env python3
"""Validate a real Keycloak identity and STS session on the Security fleet."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import time
from urllib import error, parse, request
import uuid
import xml.etree.ElementTree as ET


KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak@sha256:6a7217a100bd3e5de4063a27a538ef999a3c5a88c4b4ec0ffc0a642aee7b2597"
HTTP = request.build_opener(request.ProxyHandler({}))


def save(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def docker(*arguments: str) -> str:
    result = subprocess.run(["docker", *arguments], capture_output=True, text=True, timeout=300)
    if result.returncode and "permission denied while trying to connect" in result.stderr.lower():
        result = subprocess.run(["sudo", "-n", "docker", *arguments], capture_output=True, text=True, timeout=300)
    if result.returncode:
        raise RuntimeError(f"Keycloak container operation {arguments[0]} failed ({result.returncode}): {result.stderr.strip()[:600]}")
    return result.stdout


def http(url: str, data: dict | None = None) -> tuple[int, bytes]:
    body = parse.urlencode(data).encode() if data is not None else None
    headers = {"Content-Type": "application/x-www-form-urlencoded"} if body is not None else {}
    try:
        with HTTP.open(request.Request(url, data=body, headers=headers), timeout=15) as response:
            return response.status, response.read(2 * 1024 * 1024)
    except error.HTTPError as response:
        return response.code, response.read(2 * 1024 * 1024)


def setup(directory: Path) -> None:
    with socket.socket() as sock:
        sock.bind(("0.0.0.0", 0))
        port = sock.getsockname()[1]
    origin = f"http://{os.environ.get('RUSTFS_HEAL_IP', '10.0.0.4')}:{port}"
    issuer = origin + "/realms/rustfs-validation"
    state = {"container": "rustfs-validation-oidc-" + secrets.token_hex(8),
             "issuer": issuer, "config_url": issuer + "/.well-known/openid-configuration",
             "client_secret": secrets.token_hex(24), "wrong_client_secret": secrets.token_hex(24),
             "password": secrets.token_hex(24), "subject": str(uuid.uuid4())}
    state_path = directory / "oidc-state.json"
    save(state_path, state)
    state_path.chmod(0o600)
    fixture = {"realm": "rustfs-validation", "enabled": True, "sslRequired": "none",
               "accessTokenLifespan": 300, "clients": [],
               "users": [{"id": state["subject"], "username": "validation-user", "enabled": True,
                          "firstName": "Validation", "lastName": "User", "emailVerified": True,
                          "email": "validation@example.test", "credentials": [
                              {"type": "password", "value": state["password"], "temporary": False}]}]}
    for client_id, secret_name in (("rustfs-validation", "client_secret"), ("wrong-audience", "wrong_client_secret")):
        fixture["clients"].append({"clientId": client_id, "secret": state[secret_name], "enabled": True,
                                   "protocol": "openid-connect", "publicClient": False,
                                   "standardFlowEnabled": False, "directAccessGrantsEnabled": True})
    fixture_path = directory / "oidc-realm.json"
    save(fixture_path, fixture)
    fixture_path.chmod(0o644)
    docker("run", "--detach", "--rm", "--name", state["container"], "--memory", "1g",
           "--publish", f"0.0.0.0:{port}:8080", "--env", f"KC_HOSTNAME={origin}",
           "--volume", f"{fixture_path}:/opt/keycloak/data/import/realm.json:ro",
           KEYCLOAK_IMAGE, "start-dev", "--import-realm")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            status, body = http(state["config_url"])
            discovery = json.loads(body) if status == 200 else {}
            if discovery.get("issuer") == issuer and discovery.get("token_endpoint") and discovery.get("jwks_uri"):
                save(directory / "validation-oidc-provider.json",
                     {"image": KEYCLOAK_IMAGE, "issuer": issuer, "discovery_sha256": hashlib.sha256(body).hexdigest(),
                      "token_endpoint": discovery["token_endpoint"], "jwks_uri": discovery["jwks_uri"]})
                return
        except (OSError, ValueError):
            pass
        if docker("inspect", "--format", "{{.State.Running}}", state["container"]).strip() != "true":
            raise RuntimeError("Keycloak exited before discovery was ready")
        time.sleep(2)
    raise RuntimeError("Keycloak discovery did not become ready within 180 seconds")


def xml_values(body: bytes) -> dict[str, str]:
    return {element.tag.rsplit("}", 1)[-1]: element.text or "" for element in ET.fromstring(body).iter()}


def token(state: dict, client_id: str, secret_name: str) -> tuple[str, dict]:
    status, body = http(state["issuer"] + "/protocol/openid-connect/token",
                        {"grant_type": "password", "client_id": client_id,
                         "client_secret": state[secret_name], "username": "validation-user",
                         "password": state["password"], "scope": "openid"})
    if status != 200:
        raise RuntimeError(f"Keycloak password login failed (HTTP {status})")
    encoded = json.loads(body)["id_token"]
    payload = encoded.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    audience = claims["aud"] if isinstance(claims["aud"], list) else [claims["aud"]]
    if claims["iss"] != state["issuer"] or client_id not in audience or claims["sub"] != state["subject"]:
        raise RuntimeError("Keycloak identity claims differ from the configured fixture")
    return encoded, {name: claims[name] for name in ("iss", "sub", "aud", "exp")}


def sts(endpoint: str, identity_token: str, subject: str, policy: dict | None = None) -> tuple[dict, dict]:
    data = {"Action": "AssumeRoleWithWebIdentity", "Version": "2011-06-15",
            "DurationSeconds": "900", "WebIdentityToken": identity_token}
    if policy is not None:
        data["Policy"] = json.dumps(policy, separators=(",", ":"))
    status, body = http(endpoint.rstrip("/") + "/", data)
    values = xml_values(body)
    if status != 200:
        raise RuntimeError(f"valid OIDC identity failed STS (HTTP {status}, code {values.get('Code', 'missing')})")
    required = ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration", "SubjectFromWebIdentityToken")
    if not all(values.get(name) for name in required) or values["SubjectFromWebIdentityToken"] != subject:
        raise RuntimeError("STS omitted credentials or returned a different identity subject")
    expiration = datetime.fromisoformat(values["Expiration"].replace("Z", "+00:00"))
    remaining = (expiration - datetime.now(timezone.utc)).total_seconds()
    if not 0 < remaining <= 960:
        raise RuntimeError("STS session expiration is outside the requested 900 second boundary")
    credentials = {"AWS_ACCESS_KEY_ID": values["AccessKeyId"], "AWS_SECRET_ACCESS_KEY": values["SecretAccessKey"],
                   "AWS_SESSION_TOKEN": values["SessionToken"]}
    return credentials, {"subject": subject, "expiration": values["Expiration"]}


def aws(endpoint: str, credentials: dict, *arguments: str, denied: bool = False) -> str:
    environment = os.environ.copy()
    for name in ("AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN", "AWS_PROFILE"):
        environment.pop(name, None)
    environment.update(credentials)
    environment.update(AWS_EC2_METADATA_DISABLED="true", AWS_CONFIG_FILE="/dev/null",
                       AWS_SHARED_CREDENTIALS_FILE="/dev/null", AWS_DEFAULT_REGION="us-east-1")
    result = subprocess.run(["aws", "--endpoint-url", endpoint, "--no-cli-pager", "s3api", *arguments],
                            env=environment, capture_output=True, text=True, timeout=60)
    if denied:
        match = re.search(r"An error occurred \(([A-Za-z0-9]+)\)", result.stderr)
        code = match.group(1) if match else "missing"
        if not result.returncode or code not in {"AccessDenied", "InvalidToken", "InvalidAccessKeyId"}:
            raise RuntimeError(f"expected access boundary rejection, received {code} (exit {result.returncode})")
        return code
    if result.returncode:
        raise RuntimeError(f"S3 operation {arguments[0]} failed (exit {result.returncode})")
    return result.stdout


def test(directory: Path, endpoint: str) -> None:
    state = json.loads((directory / "oidc-state.json").read_text())
    root = {"AWS_ACCESS_KEY_ID": os.environ["RUSTFS_ACCESS_KEY"], "AWS_SECRET_ACCESS_KEY": os.environ["RUSTFS_SECRET_KEY"]}
    prefix = "val-oidc-" + secrets.token_hex(8)
    buckets = [prefix + "-allowed", prefix + "-outside"]
    source = directory / "oidc-source.bin"
    body = secrets.token_bytes(65536)
    source.write_bytes(body)
    observations = {"identity": {}, "checks": {}, "cleanup": "PENDING"}
    created = []
    primary = None
    try:
        for bucket in buckets:
            aws(endpoint, root, "create-bucket", "--bucket", bucket)
            created.append(bucket)
            aws(endpoint, root, "put-object", "--bucket", bucket, "--key", "object.bin", "--body", str(source))
        encoded, claims = token(state, "rustfs-validation", "client_secret")
        observations["identity"] = claims
        identity_credentials, identity_session = sts(endpoint, encoded, state["subject"])
        observations["identity_session"] = identity_session
        identity_read = directory / "oidc-identity-read.bin"
        aws(endpoint, identity_credentials, "get-object", "--bucket", buckets[0], "--key", "object.bin", str(identity_read))
        if identity_read.read_bytes() != body:
            raise RuntimeError("OIDC identity GET returned different object bytes")
        identity_denial = aws(endpoint, identity_credentials, "put-object", "--bucket", buckets[0],
                              "--key", "forbidden.bin", "--body", str(source), denied=True)
        if identity_denial != "AccessDenied":
            raise RuntimeError("readonly identity boundary did not return AccessDenied")
        observations["checks"]["identity_write_denied"] = identity_denial
        policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:GetObject",
                   "Resource": f"arn:aws:s3:::{buckets[0]}/*"}]}
        session, session_observation = sts(endpoint, encoded, state["subject"], policy)
        observations["session"] = session_observation
        received = directory / "oidc-read.bin"
        aws(endpoint, session, "get-object", "--bucket", buckets[0], "--key", "object.bin", str(received))
        if received.read_bytes() != body:
            raise RuntimeError("OIDC session GET returned different object bytes")
        observations["checks"]["allowed_get_sha256"] = hashlib.sha256(body).hexdigest()
        outside_code = aws(endpoint, session, "get-object", "--bucket", buckets[1], "--key", "object.bin",
                           str(directory / "oidc-denied.bin"), denied=True)
        if outside_code != "AccessDenied":
            raise RuntimeError("session policy boundary did not return AccessDenied")
        observations["checks"]["session_outside_denied"] = outside_code
        for name, altered in (("missing_session_token", {key: value for key, value in session.items() if key != "AWS_SESSION_TOKEN"}),
                              ("invalid_session_token", {**session, "AWS_SESSION_TOKEN": "invalid-session-token"})):
            observations["checks"][name] = aws(endpoint, altered, "get-object", "--bucket", buckets[0], "--key", "object.bin",
                                                str(directory / "oidc-invalid.bin"), denied=True)
        parts = encoded.split(".")
        parts[2] = ("A" if parts[2][0] != "A" else "B") + parts[2][1:]
        wrong_token, _ = token(state, "wrong-audience", "wrong_client_secret")
        for name, invalid in (("tampered_identity", ".".join(parts)), ("wrong_audience", wrong_token)):
            status, response = http(endpoint.rstrip("/") + "/", {"Action": "AssumeRoleWithWebIdentity", "Version": "2011-06-15",
                                        "DurationSeconds": "900", "WebIdentityToken": invalid})
            code = xml_values(response).get("Code")
            if status != 403 or code != "AccessDenied":
                raise RuntimeError(f"invalid OIDC identity accepted or rejected incorrectly (HTTP {status}, code {code})")
            observations["checks"][name] = {"http_status": status, "code": code}
        for bucket in buckets:
            aws(endpoint, root, "get-object", "--bucket", bucket, "--key", "object.bin", str(directory / "oidc-root-read.bin"))
            if (directory / "oidc-root-read.bin").read_bytes() != body:
                raise RuntimeError("rejected session requests changed the root fixture")
        observations["checks"]["fixtures_preserved"] = True
    except Exception as exc:
        primary = exc
    finally:
        cleanup_errors = []
        for bucket in created:
            try:
                aws(endpoint, root, "delete-object", "--bucket", bucket, "--key", "object.bin")
                aws(endpoint, root, "delete-bucket", "--bucket", bucket)
            except Exception:
                cleanup_errors.append(bucket)
        observations["cleanup"] = "FAIL" if cleanup_errors else "PASS"
        save(directory / "validation-oidc-observations.json", observations)
    if primary is not None:
        raise primary
    if cleanup_errors:
        raise RuntimeError("OIDC fixture cleanup failed")


def teardown(directory: Path) -> None:
    state_path = directory / "oidc-state.json"
    if not state_path.exists():
        return
    state = json.loads(state_path.read_text())
    containers = docker("ps", "--all", "--format", "{{.Names}}").splitlines()
    if state["container"] in containers:
        docker("rm", "--force", state["container"])
    if state["container"] in docker("ps", "--all", "--format", "{{.Names}}").splitlines():
        raise RuntimeError("run-owned Keycloak container remains after cleanup")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("setup", "test", "teardown"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--endpoint")
    args = parser.parse_args()
    {"setup": lambda: setup(args.directory), "test": lambda: test(args.directory, args.endpoint),
     "teardown": lambda: teardown(args.directory)}[args.mode]()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"OIDC validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
