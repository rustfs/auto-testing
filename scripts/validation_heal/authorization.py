#!/usr/bin/env python3
"""A08 exercises actual IAM principals and every Heal control operation."""

import hashlib
import json
import secrets
import time
from urllib import request
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
import xml.etree.ElementTree as ET

try:
    from .heal_probe import Probe, compare_inventories, require, validate_binding
    from .heal_signing import sign_admin_request
    from .healthy import physical_content
except ImportError:
    from heal_probe import Probe, compare_inventories, require, validate_binding
    from heal_signing import sign_admin_request
    from healthy import physical_content


# The candidate's S3Router::check_access rejects unsigned Admin traffic
# before authorize_admin_request (whose missing-credential error is 400).
DENIALS = {"anonymous": (403, "AccessDenied"), "bad-signature": (403, "SignatureDoesNotMatch"),
           "expired-signature": (403, "RequestTimeTooSkewed"), "denied-user": (403, "AccessDenied")}


def response_code(observation):
    body = observation.get("response")
    if isinstance(body, dict):
        return body.get("Code") or body.get("code")
    if isinstance(body, str):
        return next((element.text for element in ET.fromstring(body).iter()
                     if element.tag.rsplit("}", 1)[-1] == "Code"), None)
    return None


def validate_denial(observation, mode):
    status, code = DENIALS[mode]
    require(observation.get("httpStatus") == status and response_code(observation) == code,
            f"{mode} did not satisfy its frozen {status}/{code} refusal")
    body = observation.get("response")
    forbidden = {"outcome", "objects", "progress", "clienttoken"}
    def disclosed(value):
        if isinstance(value, dict):
            return any(str(key).lower() in forbidden or disclosed(item) for key, item in value.items())
        if isinstance(value, list):
            return any(disclosed(item) for item in value)
        return False
    if isinstance(body, str):
        leaked = any(element.tag.rsplit("}", 1)[-1].lower() in forbidden for element in ET.fromstring(body).iter())
    else:
        leaked = disclosed(body)
    require(not leaked, "unauthorized response disclosed task responsibility or admitted a token")
    return {"status": status, "code": code, "noTaskDisclosure": True}


def iam_exchange(probe, fleet, operation, user, *, secret=None, policy=None, document=None):
    from botocore.awsrequest import AWSRequest
    validate_binding(probe.binding)
    require(user.startswith("rvheal") and user.isalnum(), "IAM principal is outside the run namespace")
    routes = {
        "add-user": ("PUT", "add-user", {"accessKey": user}, {"secretKey": secret, "status": "enabled"}),
        "remove-user": ("DELETE", "remove-user", {"accessKey": user}, None),
        "add-policy": ("PUT", "add-canned-policy", {"name": policy}, document),
        "remove-policy": ("DELETE", "remove-canned-policy", {"name": policy}, None),
        "attach": ("POST", "idp/builtin/policy/attach", {}, {"policies": [policy], "user": user}),
        "detach": ("POST", "idp/builtin/policy/detach", {}, {"policies": [policy], "user": user}),
    }
    require(operation in routes and (policy is None or policy.startswith("rvheal") and policy.isalnum()), "invalid owned IAM operation")
    method, route, query, body = routes[operation]
    raw = json.dumps(body).encode() if body is not None else b""
    url = probe.endpoint + "/rustfs/admin/v3/" + route + ("?" + urlencode(query) if query else "")
    outgoing = AWSRequest(method=method, url=url, data=raw,
        headers={"Content-Type": "application/json", "x-amz-content-sha256": "UNSIGNED-PAYLOAD"})
    outgoing.context["payload_signing_enabled"] = False
    sign_admin_request(outgoing, *probe.credentials())
    try:
        response = probe._open_admin(request.Request(url, data=raw if raw else None, method=method,
                                                    headers=dict(outgoing.headers.items())))
    except HTTPError as exc:
        response = exc
    with response:
        returned = response.read(64 * 1024 + 1)
        require(len(returned) <= 64 * 1024, "IAM response exceeds evidence budget")
        observed = {"operation": operation, "httpStatus": response.code,
                    "principal": "run-owned", "response": returned.decode(errors="replace"),
                    "requestBodyArchived": False, "credentialValuesArchived": False}
    fleet.record("iam-" + operation, observed)
    require(observed["httpStatus"] == 200, "owned IAM operation failed: " + operation)
    return observed


def admit(probe, fleet, path, options, active):
    started = probe.admin(path, [], json.dumps(options).encode())
    token = (started.get("response") or {}).get("clientToken") if isinstance(started.get("response"), dict) else None
    if isinstance(token, str) and token:
        active.append((path, token))
    fleet.record("authorization-admission", started)
    require(started.get("httpStatus") == 200 and token, "authorized principal could not admit its own task")
    return token


def authorization_requests(probe, fleet, bucket, before, baseline, active, options):
    suffix = hashlib.sha256(probe.binding["runId"].encode()).hexdigest()[:16]
    denied_user, allowed_user, policy = ("rvheal" + suffix + ending for ending in ("denied", "allowed", "policy"))
    denied_secret, allowed_secret = secrets.token_hex(24), secrets.token_hex(24)
    fleet.secret_values.update({*probe.credentials(), denied_user, allowed_user, denied_secret, allowed_secret})
    denied = Probe(probe.binding, credential_pair=(denied_user, denied_secret))
    allowed = Probe(probe.binding, credential_pair=(allowed_user, allowed_secret))
    path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="")
    task_options = {**options, "recursive": True, "dryRun": True}
    branches, cleanup_errors = {}, []
    users, policy_created, attached = [], False, False
    try:
        for user, secret in ((denied_user, denied_secret), (allowed_user, allowed_secret)):
            users.append(user)  # Preserve ownership even if admission loses its reply.
            iam_exchange(probe, fleet, "add-user", user, secret=secret)
        policy_created = True
        # The candidate admits unscoped Admin actions without an S3 Resource;
        # its resource parser correctly rejects a bare '*' as an invalid ARN.
        document = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["admin:Heal"]}]}
        iam_exchange(probe, fleet, "add-policy", allowed_user, policy=policy, document=document)
        attached = True
        iam_exchange(probe, fleet, "attach", allowed_user, policy=policy)
        for principal in (denied, allowed):
            result = fleet.record("least-privilege-s3-denial", principal.s3("get_object", {"Bucket": bucket, "Key": "objects/0008.bin"}))
            response = result.get("response") or {}
            require(result.get("acknowledged") is False and result.get("resultUnknown") is False and "payload" not in result
                    and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 403
                    and response.get("Error", {}).get("Code") == "AccessDenied", "Heal-only/denied principal unexpectedly has S3 read access")
        root_token = admit(probe, fleet, path, task_options, active)
        for mode in DENIALS:
            proofs, failures = {}, []
            client = denied if mode == "denied-user" else probe
            auth = "signed" if mode == "denied-user" else mode
            for operation, query, raw in (
                    ("start", [], json.dumps(task_options).encode()),
                    ("status", [("clientToken", root_token)], b""),
                    ("stop", [("clientToken", root_token), ("forceStop", "true")], b"")):
                try:
                    observed = client.admin(path, query, raw, auth=auth)
                    token = (observed.get("response") or {}).get("clientToken") if isinstance(observed.get("response"), dict) else None
                    if token and (path, token) not in active:
                        active.append((path, token))
                    fleet.record("auth-" + mode + "-" + operation, observed)
                    proofs[operation] = validate_denial(observed, mode)
                except Exception as exc:
                    failures.append({"operation": operation, "type": type(exc).__name__, "message": str(exc)})
            branches[mode] = {"verdict": "FAIL" if failures else "PASS", "operations": proofs, "failures": failures}
        fleet.record("authorization-negative-branches", branches)
        deadline = time.monotonic() + 30
        while True:
            root_status = fleet.record("root-task-status-after-denials", probe.admin(path, [("clientToken", root_token)], b""))
            require(root_status.get("httpStatus") == 200 and isinstance(root_status.get("response"), dict), "administrator lost its original token after denied requests")
            if root_status["response"].get("summary") == "finished":
                break
            require(root_status["response"].get("summary") == "running" and time.monotonic() < deadline,
                    "denied control requests stopped the original task or it did not finish before the independent authorized task")
            time.sleep(0.2)
        token = admit(allowed, fleet, path, task_options, active)
        queried = fleet.record("authorized-status", allowed.admin(path, [("clientToken", token)], b""))
        require(queried.get("httpStatus") == 200 and isinstance(queried.get("response"), dict)
                and queried["response"].get("summary") in {"running", "finished", "stopped"}, "Heal-only principal cannot query its admitted token")
        stopped = fleet.record("authorized-stop", allowed.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
        require(stopped.get("httpStatus") == 200 and isinstance(stopped.get("response"), dict), "Heal-only principal cannot stop its admitted token")
        require(stopped["response"].get("summary") in {"stopped", "finished"},
                "Heal-only stop did not return terminal task control")
        branches["least-privilege-user"] = {"verdict": "PASS", "admissionStatusStop": True,
                                           "s3ReadDenied": True, "policyActions": ["admin:Heal"]}
    finally:
        for task_path, token in list(active):
            try:
                observed = fleet.record("auth-final-stop", probe.admin(task_path, [("clientToken", token), ("forceStop", "true")], b""))
                require(observed.get("httpStatus") == 200, "authorization task finalization failed")
                active.remove((task_path, token))
            except Exception as exc:
                cleanup_errors.append(str(exc))
        if attached:
            try:
                iam_exchange(probe, fleet, "detach", allowed_user, policy=policy)
            except Exception as exc:
                cleanup_errors.append(str(exc))
        for user in users:
            try:
                iam_exchange(probe, fleet, "remove-user", user)
            except Exception as exc:
                cleanup_errors.append(str(exc))
        if policy_created:
            try:
                iam_exchange(probe, fleet, "remove-policy", allowed_user, policy=policy)
            except Exception as exc:
                cleanup_errors.append(str(exc))
        fleet.record("iam-finalization", {"cleanup": "FAIL" if cleanup_errors else "PASS", "errors": cleanup_errors})
    require(not cleanup_errors, "temporary principals, policy or task finalization failed")
    for principal in (denied, allowed):
        observed = fleet.record("revoked-credential-denial", principal.admin(path, [], json.dumps(task_options).encode()))
        require(observed.get("httpStatus") == 403 and response_code(observed) == "InvalidAccessKeyId",
                "removed temporary credentials still authenticate or return an unexpected outcome")
    after = fleet.snapshot("physical-after-authorization")
    require(physical_content(before) == physical_content(after), "authorization/dry-run controls mutated the object fixture")
    current = fleet.record("s3-after-authorization", probe.inventory(bucket, unversioned_current_tags=True))
    fleet.record("authorization-controls", {"physicalUnchanged": True, "s3": compare_inventories(baseline, current)})
    fleet.record("authorization-all-branches", branches)
    require(set(branches) == {*DENIALS, "least-privilege-user"}
            and all(branch["verdict"] == "PASS" for branch in branches.values()), "authorization requires every start/status/stop principal branch")
    return branches
