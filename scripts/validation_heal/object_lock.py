"""D09 protection proofs and S3 finalization after real Compliance expiry."""

from datetime import UTC, timedelta
from email.utils import parsedate_to_datetime
import hashlib
import secrets
import time

try:
    from .authorization import iam_exchange
    from .heal_probe import Probe, payload, require
except ImportError:
    from authorization import iam_exchange
    from heal_probe import Probe, payload, require


BRANCHES = ("governance-retention", "compliance-retention", "legal-hold", "historical-protected-version")
KEYS = ("locked/governance.bin", "locked/compliance.bin", "locked/hold.bin", "locked/historical.bin")
SIZE = 5 * 1024**2 + 123
RETENTION_MINUTES = 10
CLEANUP_SECONDS = (RETENTION_MINUTES + 2) * 60


def seed_locked(recorder, fleet, bucket, context):
    probe = recorder.probe
    suffix = hashlib.sha256(probe.binding["runId"].encode()).hexdigest()[:16]
    user, policy, secret = "rvheal" + suffix + "locked", "rvheal" + suffix + "lockpolicy", secrets.token_hex(24)
    context.update(user=user, policy=policy, principal=Probe(probe.binding, credential_pair=(user, secret)),
                   policyCreated=False, attached=False, bucketAttempted=False)
    fleet.secret_values.update({user, secret, *probe.credentials()})
    iam_exchange(probe, fleet, "add-user", user, secret=secret)
    context["policyCreated"] = True
    document = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:*"], "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]},
        {"Effect": "Deny", "Action": ["s3:BypassGovernanceRetention"], "Resource": [f"arn:aws:s3:::{bucket}/*"]}]}
    iam_exchange(probe, fleet, "add-policy", user, policy=policy, document=document)
    context["attached"] = True
    iam_exchange(probe, fleet, "attach", user, policy=policy)
    context["bucketAttempted"] = True
    recorder.call("create_bucket", {"Bucket": bucket, "ObjectLockEnabledForBucket": True})
    mode = recorder.call("get_bucket_versioning", {"Bucket": bucket})
    require(mode.get("Status") == "Enabled", "Object Lock bucket did not enable real versioning")
    lock = recorder.call("get_object_lock_configuration", {"Bucket": bucket})
    require(lock.get("ObjectLockConfiguration", {}).get("ObjectLockEnabled") == "Enabled", "Object Lock is not active")
    server_date = lock.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("date")
    require(isinstance(server_date, str), "protected fixture needs a server-time retention anchor")
    server_time = parsedate_to_datetime(server_date)
    require(server_time.tzinfo is not None, "retention server time is not timezone-aware")
    until = server_time.astimezone(UTC).replace(microsecond=0) + timedelta(minutes=RETENTION_MINUTES)
    context["retainUntil"] = until
    expected, protections = [], []
    for index, key in enumerate(KEYS):
        protection = {"ObjectLockLegalHoldStatus": "ON"} if index == 2 else {
            "ObjectLockMode": "COMPLIANCE" if index == 1 else "GOVERNANCE", "ObjectLockRetainUntilDate": until}
        for nonce in range(3):
            body = payload(f"{bucket}/{key}/{nonce}", SIZE)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "Metadata": {"heal-fixture": "object-lock", "generation": str(nonce)},
                "Tagging": f"suite=heal&generation=v{nonce}", **protection}, body)
            version = response.get("VersionId")
            require(version not in {None, "", "null"}, "protected PUT omitted its acknowledged version")
            expected.append({"key": key, "versionId": version, "deleteMarker": False, "isLatest": nonce == 2,
                             "payload": {"size": SIZE, "sha256": hashlib.sha256(body).hexdigest()}})
            protections.append({"key": key, "versionId": version, **protection})
    context["protections"] = protections
    fleet.record("lock-fixture", {"protections": protections, "complianceUsesRealExpiry": True,
                                  "principalBypassGovernanceDenied": True})
    return expected


def validate_protection(observed, spec):
    response = observed.get("response") or {}
    require(observed.get("acknowledged") is True, "protected metadata request failed")
    if "ObjectLockLegalHoldStatus" in spec:
        require(response.get("LegalHold", {}).get("Status") == spec["ObjectLockLegalHoldStatus"], "legal hold changed or disappeared")
    else:
        retention = response.get("Retention", {})
        require(retention.get("Mode") == spec["ObjectLockMode"] and retention.get("RetainUntilDate") == spec["ObjectLockRetainUntilDate"],
                "retention mode or exact UTC expiry changed")
        server_date = response.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("date")
        require(isinstance(server_date, str) and parsedate_to_datetime(server_date) < spec["ObjectLockRetainUntilDate"],
                "retention proof did not occur during the real server-time protection window")


def validate_protected_delete(observed):
    response = observed.get("response") or {}
    require(observed.get("acknowledged") is False and observed.get("resultUnknown") is False and "payload" not in observed
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 403
            and response.get("Error", {}).get("Code") == "AccessDenied", "protected DELETE was acknowledged or did not return exact AccessDenied")


def prove_locked(probe, fleet, bucket, context, *, key=None, label):
    selected = [spec for spec in context["protections"] if key is None or spec["key"] == key]
    require(selected, "protected branch has no acknowledged versions")
    for spec in selected:
        params = {"Bucket": bucket, "Key": spec["key"], "VersionId": spec["versionId"]}
        operation = "get_object_legal_hold" if "ObjectLockLegalHoldStatus" in spec else "get_object_retention"
        observed = fleet.record(label + "-protection", probe.s3(operation, params))
        validate_protection(observed, spec)
        # Read succeeds under the same non-bypass principal, so an IAM-wide
        # access denial cannot masquerade as enforced object protection.
        readable = fleet.record(label + "-authorized-head", context["principal"].s3("head_object", params))
        require(readable.get("acknowledged") is True and readable["response"].get("VersionId") == spec["versionId"],
                "non-bypass principal cannot read the protected version")
        denied = fleet.record(label + "-delete-denied", context["principal"].s3("delete_object", params))
        validate_protected_delete(denied)
        if spec.get("ObjectLockMode") == "GOVERNANCE":
            denied = fleet.record(label + "-bypass-denied", context["principal"].s3("delete_object", {**params, "BypassGovernanceRetention": True}))
            validate_protected_delete(denied)
    return {"protectedVersions": len(selected), "exactProtectionMetadata": True, "nonBypassDeleteDenied": True,
            "serverTimeBeforeExpiry": True}


def finalize_locked(probe, fleet, bucket, context):
    """Remove protection through APIs; retain the namespace on incomplete cleanup."""
    errors = []
    try:
        if fleet.reserved: fleet.stop()
        if fleet.faults:
            for registration in list(fleet.faults):
                node, params = registration
                fleet.record("locked-fault-rescue", fleet.remote(node, "recover-fault", **params))
                fleet.faults.remove(registration)
        if fleet.reserved: fleet.start()
        if context.get("bucketAttempted"):
            listing = fleet.record("locked-final-list", probe.s3("list_object_versions", {"Bucket": bucket}))
            response = listing.get("response") or {}
            absent = not listing.get("acknowledged") and response.get("Error", {}).get("Code") == "NoSuchBucket"
            require(absent or listing.get("acknowledged") and not response.get("IsTruncated"), "protected finalization inventory is incomplete")
            versions = [] if absent else [{"Key": entry["Key"], "VersionId": entry["VersionId"], "marker": marker}
                for field, marker in (("Versions", False), ("DeleteMarkers", True)) for entry in response.get(field, [])]
            require(len(versions) <= 32, "protected cleanup exceeded the owned fixture budget")
            deadline = time.monotonic() + CLEANUP_SECONDS
            for version in versions:
                params = {"Bucket": bucket, "Key": version["Key"], "VersionId": version["VersionId"]}
                require(version["Key"] in KEYS, "protected finalization found an unowned key")
                if not version["marker"]:
                    hold = fleet.record("locked-final-hold", probe.s3("get_object_legal_hold", params))
                    value = (hold.get("response") or {}).get("LegalHold", {})
                    if hold.get("acknowledged") and value.get("Status") == "ON":
                        changed = fleet.record("locked-final-hold-off", probe.s3("put_object_legal_hold", {**params, "LegalHold": {"Status": "OFF"}}))
                        require(changed.get("acknowledged") is True, "legal hold could not be released by the owner")
                    retention = fleet.record("locked-final-retention", probe.s3("get_object_retention", params))
                    if (retention.get("response") or {}).get("Retention", {}).get("Mode") == "GOVERNANCE":
                        params["BypassGovernanceRetention"] = True
                while True:
                    deleted = fleet.record("locked-final-delete", probe.s3("delete_object", params))
                    if deleted.get("acknowledged"):
                        require(deleted["response"].get("ResponseMetadata", {}).get("HTTPStatusCode") in {200, 204}, "protected final DELETE had an unexpected status")
                        break
                    # A Compliance version stays on disk until the server
                    # accepts its deletion after actual expiry. No clock edits,
                    # shortened retention or internal-file removal are used.
                    validate_protected_delete(deleted)
                    require(time.monotonic() < deadline, "protected data is still retained past the cleanup budget")
                    time.sleep(2)
            if not absent:
                empty = fleet.record("locked-final-empty", probe.s3("list_object_versions", {"Bucket": bucket}))
                require(empty.get("acknowledged") and not any(empty["response"].get(field) for field in ("Versions", "DeleteMarkers", "IsTruncated")),
                        "protected versions or markers remain after API finalization")
                removed = fleet.record("locked-final-bucket-delete", probe.s3("delete_bucket", {"Bucket": bucket}))
                require(removed.get("acknowledged") is True, "protected bucket finalization failed")
                absent = fleet.record("locked-final-bucket-absence", probe.s3("head_bucket", {"Bucket": bucket}))
                require(absent.get("acknowledged") is False and absent.get("resultUnknown") is False
                        and (absent.get("response") or {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 404,
                        "protected bucket absence is unproven")
    except Exception as exc:
        errors.append(str(exc))
    user, policy = context.get("user"), context.get("policy")
    for operation, enabled in (("detach", context.get("attached")), ("remove-user", user is not None), ("remove-policy", context.get("policyCreated"))):
        if enabled:
            try: iam_exchange(probe, fleet, operation, user, policy=policy if operation != "remove-user" else None)
            except Exception as exc: errors.append(str(exc))
    if errors:
        try:
            fleet.stop()
            for node in fleet.reserved: fleet.record("locked-retained-logs", {"node": node, **fleet.remote(node, "logs")})
            fleet.snapshot("locked-retained-scene", quiescent=True)
        except Exception as exc: errors.append(str(exc))
    fleet.record("locked-finalization", {"cleanup": "FAIL" if errors else "PASS", "errors": errors,
        "state": "RETAINED" if errors else "API_FINALIZED", "namespaceDeletionAllowed": not errors,
        "complianceExpiryShortened": False, "internalProtectedFileDeletionUsed": False})
    return not errors


def cleanup_locked_fleet(probe, fleet, bucket, context):
    # Physical namespace deletion is permitted only after protected versions
    # and temporary IAM resources have been finalized through their APIs.
    return finalize_locked(probe, fleet, bucket, context) and fleet.cleanup()
