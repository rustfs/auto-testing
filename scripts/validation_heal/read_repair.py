"""E03: one tracked read, a fresh ReadRepair admission and its exact task."""

import hashlib
import json
import time
from urllib.parse import quote

try:
    from .heal_probe import Probe, payload, require
    from .scanner_repair import JournalWindow
except ImportError:
    from heal_probe import Probe, payload, require
    from scanner_repair import JournalWindow


BRANCHES = ("get-missing", "range-bitrot", "historical-version-read")
ADMISSION_MESSAGE = "Read-repair heal request admitted"
ADMISSION_TARGET = "rustfs_ecstore::set_disk::core::io_primitives"


def source_tasks(logs, bucket, key):
    """Consume candidate JSON events; free-text mentions cannot certify a source."""
    found = set()
    for node, observed in logs.items():
        for line in observed.get("journal", "").splitlines():
            start = line.find("{")
            if start < 0: continue
            try: event = json.loads(line[start:])
            except json.JSONDecodeError: continue
            if (event.get("target") != ADMISSION_TARGET or event.get("level") != "DEBUG"
                    or event.get("message") != ADMISSION_MESSAGE
                    or event.get("bucket") != bucket or event.get("object") != key):
                continue
            require(event.get("admission") == "accepted" and type(event.get("pool_index")) is int
                    and event["pool_index"] == 0 and type(event.get("set_index")) is int and event["set_index"] == 0
                    and type(event.get("part_number")) is int and event["part_number"] == 1
                    and isinstance(event.get("request_id"), str) and event["request_id"],
                    "read-repair event lacks exact accepted task/set/part identity")
            found.add((node, event["request_id"]))
    return found


def validate_read(observed, entry, *, expected_range=None, body=None):
    response = observed.get("response") or {}
    require(observed.get("acknowledged") is True and observed.get("resultUnknown", False) is False,
            "tracked read failed or has an unknown transport outcome")
    headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    request_id = response.get("ResponseMetadata", {}).get("RequestId") or headers.get("x-request-id") or headers.get("x-amz-request-id")
    require(isinstance(request_id, str) and request_id, "tracked read omitted a server request ID")
    version = response.get("VersionId", "null")
    require(version == entry["versionId"], "tracked read returned another version")
    if expected_range is None:
        require(response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
                and observed.get("payload") == entry["payload"], "tracked GET differs from its acknowledged bytes")
    else:
        start, end = expected_range
        selected = body[start:end + 1]
        require(response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 206
                and response.get("ContentRange") == f"bytes {start}-{end}/{entry['payload']['size']}"
                and observed.get("payload") == {"size": len(selected), "sha256": hashlib.sha256(selected).hexdigest()},
                "tracked Range differs from its exact acknowledged interval")
    return request_id


def tracked_read_task(probe, fleet, bucket, branch, entry):
    require(fleet.runtime_profile == "read-repair", "read repair requires its explicit isolated runtime profile")
    window = JournalWindow(fleet, "read-source", event_targets={ADMISSION_TARGET})
    params = {"Bucket": bucket, "Key": entry["key"]}
    if entry["versionId"] != "null": params["VersionId"] = entry["versionId"]
    interval = (0, min(1024**2 + 16, entry["payload"]["size"]) - 1) if branch == "range-bitrot" else None
    body = payload(f"{bucket}/{entry['key']}/0", entry["payload"]["size"]) if interval else None
    if interval: params["Range"] = f"bytes={interval[0]}-{interval[1]}"
    read = fleet.record("single-read-trigger", probe.s3("get_object", params))
    request_id = validate_read(read, entry, expected_range=interval, body=body)
    deadline = time.monotonic() + 180
    fresh = set()
    while not fresh:
        events = window.capture()
        fresh = source_tasks({node: {"journal": "\n".join(json.dumps(event) for event in rows)}
                              for node, rows in events.items()}, bucket, entry["key"])
        require(time.monotonic() < deadline, "ATTRIBUTION_NOT_OBSERVED: no fresh accepted ReadRepair task for the read target")
        if not fresh: time.sleep(0.2)
    require(len(fresh) == 1, "read source has ambiguous task ownership")
    node, token = next(iter(fresh))
    # Status queries observe the automatically admitted request. No Admin
    # start, stop, retry or deep verifier contributes to this repair proof.
    endpoint = "node" + str(fleet.nodes.index(node) + 1)
    status_probe = Probe(probe.binding, endpoint)
    path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + quote(entry["key"], safe="/")
    while True:
        observed = fleet.record("read-source-task-status", status_probe.admin(path, [("clientToken", token)], b""))
        require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
                "ReadRepair task cannot be queried by its exact source ID")
        status = observed["response"]
        if status.get("summary") != "running": break
        require(time.monotonic() < deadline, "ReadRepair task did not finish before the physical repair budget")
        time.sleep(0.2)
    require(status.get("summary") == "finished", "ReadRepair task did not report finished execution")
    fleet.record("read-source-binding", {"source": "read-repair", "s3RequestId": request_id, "taskId": token,
        "sourceNode": node, "key": entry["key"], "versionId": entry["versionId"],
        "singleTargetRead": True, "adminMutationUsed": False, "scannerAutoHealAndBestEffortMrfDisabled": True,
        "durableMrf": "candidate consumer stays initialized; no degraded writes create responsibility in this fixture",
        "requirement": "still requires exact version receipt, physical bytes, streaming checksum and full S3 proof"})
    return status
