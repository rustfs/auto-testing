"""Bounded F07 faults on exact owned durable records; never search metadata."""

import base64
import hashlib
import json
import os
import re
import uuid


LIMIT = 4 * 1024**2


def require(value, message):
    if not value: raise ValueError(message)


def task_id(value):
    require(isinstance(value, str) and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0,
            "journal task must be a canonical nonzero UUID")
    return value


def names(family, token=None):
    if family == "mrf":
        require(token is None, "MRF census cannot invent a task-file binding")
        return [f".rustfs.sys/.heal-mrf-{kind}.{slot}.bin" for slot in (0, 1) for kind in ("snapshot", "commit", "lifecycle")]
    task_id(token)
    if family == "root":
        return [f".rustfs.sys/{prefix}{token}.json" for prefix in ("root-heal-", "terminal-root-heal-", "heal-terminal-report-")]
    require(family == "checkpoint", "unknown durable-record family")
    return [f".rustfs.sys/buckets/{token}_{suffix}" for suffix in
            ("ahm_checkpoint.json", "ahm_resume_state.json", "ahm_checkpoint.blocked", "ahm_checkpoint.sha256")]


def relative(request):
    path = request.get("relativePath")
    require(path in names(request.get("family"), request.get("taskId")), "journal target is outside the exact record allowlist")
    return path.split("/")


def entry(raw):
    return {"size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def read(root_fd, path, fault):
    with fault.parent(root_fd, ["data", *path[:-1]]) as directory:
        return fault.read_file(directory, path[-1], LIMIT * (4 if "lifecycle" in path[-1] else 1))


def census(roots, request, fault, *, quiescent=True):
    paths = names(request.get("family"), request.get("taskId"))
    rows = []
    for disk, root in enumerate(roots):
        with fault.directory(root) as fd:
            for path in paths:
                row = {"disk": disk, "relativePath": path, "present": False}
                try: raw = read(fd, path.split("/"), fault)
                except FileNotFoundError: rows.append(row); continue
                row.update(present=True, **entry(raw), rawBase64=base64.b64encode(raw).decode())
                rows.append(row)
    return {"readOnly": True, "quiescent": quiescent, "family": request["family"],
            "taskId": request.get("taskId"), "records": rows, "directoryEnumerationUsed": False}


def stable_census(roots, request, fault):
    first = census(roots, request, fault, quiescent=False)
    second = census(roots, request, fault, quiescent=False)
    require(first == second, "NOT_OBSERVED: exact durable records changed between the two bounded reads")
    return {**second, "stable": True, "independentReads": 2}


def decode_json(raw):
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "durable record contains duplicate JSON fields")
            result[key] = value
        return result
    def invalid_constant(value):
        raise ValueError("durable record contains a non-finite JSON value")
    value = json.loads(raw, object_pairs_hook=unique_pairs, parse_constant=invalid_constant)
    require(isinstance(value, dict), "durable record is not a JSON object")
    return value


def checkpoint_value(raw, token, bucket):
    value = decode_json(raw)
    require(value.get("task_id") == task_id(token) and value.get("schema_version") == 8,
            "checkpoint does not belong to the exact original Admin task")
    admin = value.get("admin") or {}
    buckets = admin.get("buckets")
    require(admin.get("completed") is False and isinstance(buckets, dict) and set(buckets) == {bucket}
            and isinstance(buckets[bucket], str) and str(uuid.UUID(buckets[bucket])) == buckets[bucket]
            and uuid.UUID(buckets[bucket]).int != 0, "checkpoint has no original owned bucket incarnation")
    unsigned = decode_json(raw)
    unsigned["integrity_digest"] = None
    for field in ("processed_objects", "failed_objects", "skipped_objects"):
        require(isinstance(unsigned.get(field), list) and all(isinstance(item, str) for item in unsigned[field])
                and len(set(unsigned[field])) == len(unsigned[field]),
                "checkpoint ledger has an unknown canonical representation")
        unsigned[field].sort()
    # Candidate serde_json::Value uses sorted object keys and UTF-8 strings.
    canonical = json.dumps(unsigned, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    expected = base64.b64encode(hashlib.sha256(canonical).digest()).decode()
    require(value.get("integrity_digest") == expected, "original checkpoint has no valid candidate integrity digest")
    return value


def transform(raw, request, owner, mrf):
    family, kind = request["family"], request.get("kind")
    require(kind in {"truncate", "future-schema", "checksum"}, "unknown durable-record byte fault")
    path = relative(request)
    if family == "checkpoint":
        require(path[-1].endswith("_ahm_checkpoint.json"), "only the exact validated Admin checkpoint may be corrupted")
        value = checkpoint_value(raw, request["taskId"], owner["bucket"])
        require(kind in {"truncate", "future-schema"}, "checkpoint checksum is independently collected by native guards")
    elif family == "root":
        require(path[-1] == "root-heal-" + task_id(request["taskId"]) + ".json" and kind == "truncate",
                "only the exact runnable root admission may be truncated")
        value = json.loads(raw)
        require(value.get("task_id") == request["taskId"] and value.get("schema") == 3
                and value.get("heal_type") == {"type": "bucket", "bucket": owner["bucket"]}
                and isinstance(value.get("bucket_incarnation_id"), str), "root admission does not belong to this owned bucket")
        task_id(value["bucket_incarnation_id"])
    else:
        require(family == "mrf" and kind == "checksum" and re.fullmatch(r"\.heal-mrf-snapshot\.[01]\.bin", path[-1]),
                "MRF mutation is restricted to the exact actual payload checksum")
        intents = mrf.decode_journal(raw)
        require(intents and all(item["bucket"] == owner["bucket"] and item["kind"] in {3, 4} for item in intents),
                "MRF payload includes a foreign or unregistered responsibility")
        # Corrupt a real record CRC without inventing, dropping or retagging ACK identities.
        return raw[:-1] + bytes([raw[-1] ^ 1])
    if kind == "truncate":
        require(len(raw) > 8, "cannot truncate an empty/unknown record")
        return raw[:len(raw) // 2]
    value["schema_version"] = 9
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def binding(root_fd, request, owner, fault):
    require(owner.get("runtimeProfile") in {"root-restart", "mrf-crash"}, "journal mutation requires an owned recovery runtime")
    require(owner["runId"] == request["runId"] and type(request.get("disk")) is int and 0 <= request["disk"] < 4,
            "journal run or disk identity differs")
    identifier = request.get("faultId")
    require(isinstance(identifier, str) and re.fullmatch(r"F07-[a-z0-9-]{1,48}", identifier), "journal fault identity is invalid")
    path = relative(request)
    require(request["family"] == "checkpoint" and path[-1] == task_id(request["taskId"]) + "_ahm_checkpoint.json"
            or request["family"] == "root" and path[-1] == "root-heal-" + task_id(request["taskId"]) + ".json"
            or request["family"] == "mrf" and re.fullmatch(r"\.heal-mrf-snapshot\.[01]\.bin", path[-1]),
            "journal mutation cannot replace a terminal, resume state, digest, blocked fence or commit record")
    return {"schemaVersion": 1, "runId": owner["runId"], "disk": request["disk"], "family": request["family"],
            "taskId": request.get("taskId"), "relativePath": "/".join(path), "faultId": identifier,
            "binarySha256": owner["binarySha256"], "device": os.fstat(root_fd).st_dev,
            "mountId": fault.mount_id(root_fd), "productRepairClaimed": False}


def replace(root_fd, path, raw, identifier, fault, *, rescue=False):
    with fault.parent(root_fd, ["data", *path[:-1]]) as directory:
        temporary = ".F07-" + identifier + (".rescue" if rescue else ".inject")
        try: fault.new_file(directory, temporary, raw)
        except FileExistsError:
            require(fault.read_file(directory, temporary, LIMIT * 4) == raw,
                    "interrupted durable-record temporary differs; retain scene")
        os.rename(temporary, path[-1], src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
        require(read(root_fd, path, fault) == raw, "durable-record replacement readback failed")


def install(root_fd, request, owner, fault, mrf):
    identity = binding(root_fd, request, owner, fault)
    path = relative(request)
    before = read(root_fd, path, fault)
    require(request.get("expectedBefore") == entry(before), "journal bytes drifted from their archived pre-fault identity")
    injected = transform(before, request, owner, mrf)
    intent = {**identity, "kind": request["kind"], "before": entry(before), "injected": entry(injected)}
    with fault.parent(root_fd, ["quarantine"]) as backup:
        fault.new_file(backup, identity["faultId"] + ".original", before)
        fault.new_file(backup, identity["faultId"] + ".intent.json", fault.encode(intent))
        # The candidate is stopped by the node adapter; validate once more immediately before replacement.
        require(read(root_fd, path, fault) == before, "durable record changed after backup; preserve the intent")
        replace(root_fd, path, injected, identity["faultId"], fault)
        fault.new_file(backup, identity["faultId"] + ".applied.json", fault.encode(intent))
    return {**intent, "applied": True, "originalBackupRetained": True}


def recover(root_fd, request, owner, fault):
    identity = binding(root_fd, request, owner, fault)
    with fault.parent(root_fd, ["quarantine"]) as backup:
        intent = json.loads(fault.read_file(backup, identity["faultId"] + ".intent.json", 16384))
        require(all(intent.get(key) == value for key, value in identity.items()), "journal recovery lost original run/candidate/device/record identity")
        before = fault.read_file(backup, identity["faultId"] + ".original", LIMIT * 4)
        require(entry(before) == intent["before"], "journal original backup is corrupt")
        path = relative(request)
        current = read(root_fd, path, fault)
        require(entry(current) in (intent["before"], intent["injected"]),
                "product changed or advanced the durable responsibility; refusing old-record resurrection")
        restored = current != before
        if restored:
            failed = identity["faultId"] + ".failed-" + entry(current)["sha256"]
            try: fault.new_file(backup, failed, current)
            except FileExistsError: require(fault.read_file(backup, failed, LIMIT * 4) == current, "preserved journal differs")
            replace(root_fd, path, before, identity["faultId"], fault, rescue=True)
        require(read(root_fd, path, fault) == before, "journal recovery did not preserve exact original bytes")
    return {**identity, "restoredFromBackup": restored, "backupRetained": True,
            "originalResponsibilityRecreated": False, "productRepairClaimed": False}
