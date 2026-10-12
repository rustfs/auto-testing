"""Bounded F07 faults on exact owned durable records; never search metadata."""

import base64
import hashlib
import json
import os
import re
import struct
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


def same_json(left, right):
    # JSON scope identities distinguish booleans, integers and floating-point values.
    def canonical(value):
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return canonical(left) == canonical(right)


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
    require(value.get("task_id") == task_id(token) and type(value.get("schema_version")) is int and value["schema_version"] == 8,
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
    # serde_json::Value sorts keys by default; the full server's unified
    # preserve_order feature instead retains the serialized struct/map order.
    # Both builds sort the three ledger arrays before hashing UTF-8 JSON.
    digests = {base64.b64encode(hashlib.sha256(json.dumps(unsigned, sort_keys=sorted_keys,
        ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).digest()).decode()
        for sorted_keys in (False, True)}
    require(value.get("integrity_digest") in digests, "original checkpoint has no valid candidate integrity digest")
    return value


def owned_mrf_intents(intents, bucket):
    """Bind owned business work while preserving its known internal companions."""
    owned = [item for item in intents if item.get("bucket") == bucket]
    system_keys = {"buckets/.rustfs.sys/.metadata.bin", "buckets/.rustfs.sys/.bucket-incarnation"}
    require(owned and all(item.get("kind") in {3, 4} and item.get("poolIndex") == 0
                and item.get("setIndex") == 0 for item in owned)
            and all(item in owned or item.get("bucket") == ".rustfs.sys" and item.get("kind") == 3
                and item.get("versionId") == "null" and item.get("poolIndex") == 0 and item.get("setIndex") == 0
                and (item.get("key") in system_keys or re.fullmatch(r"scanner/durable-dirty-producer-replay/[0-9a-f]{64}\.json",
                    item.get("key", "")) is not None) for item in intents),
            "MRF payload includes a foreign or unregistered responsibility")
    return owned


def corrupt_owned_mrf_crc(raw, intents, bucket):
    owned_mrf_intents(intents, bucket)
    cursor, selected = 0, None
    for intent in intents:
        start = cursor
        _, layout, kind, _, _, has_version = struct.unpack_from("<BBBBQB", raw, cursor)
        cursor += 13 + (16 if has_version else 0) + (8 if layout in {2, 3} else 0)
        bucket_size, key_size = struct.unpack_from("<II", raw, cursor)
        cursor += 8 + bucket_size + key_size
        if kind == 4:
            cursor += 64
            size = struct.unpack_from("<I", raw, cursor)[0]
            cursor += 4 + size
        require(hashlib.sha256(raw[start:cursor + 4]).hexdigest() == intent["intentDigest"],
                "MRF CRC position differs from its decoded original responsibility")
        if intent["bucket"] == bucket: selected = cursor + 3
        cursor += 4
    require(cursor == len(raw) and selected is not None, "MRF CRC target omitted its owned original record")
    # Preserve every internal companion and all ACK identity fields byte-for-byte.
    return raw[:selected] + bytes([raw[selected] ^ 1]) + raw[selected + 1:]


def transform(raw, request, owner, mrf, *, earlier=None):
    family, kind = request["family"], request.get("kind")
    require(kind in {"truncate", "future-schema", "checksum", "stale-generation"}, "unknown durable-record byte fault")
    path = relative(request)
    if family == "checkpoint":
        require(path[-1].endswith("_ahm_checkpoint.json"), "only the exact validated Admin checkpoint may be corrupted")
        value = checkpoint_value(raw, request["taskId"], owner["bucket"])
        require(kind in {"truncate", "future-schema", "stale-generation"}, "checkpoint checksum is independently collected by native guards")
        if kind == "stale-generation":
            require(isinstance(earlier, bytes), "stale checkpoint requires an original captured earlier record")
            old = checkpoint_value(earlier, request["taskId"], owner["bucket"])
            require(isinstance(value["admin"].get("options"), dict) and value["admin"].get("set_disk_id") == "pool_0_set_0"
                    and all(same_json(old["admin"].get(name), value["admin"].get(name))
                            for name in ("buckets", "options", "set_disk_id")),
                    "earlier checkpoint belongs to a different incarnation, set or admitted options")
            before = old["admin"].get("outcome", {}).get("counters", {})
            after = value["admin"].get("outcome", {}).get("counters", {})
            fields = ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures")
            require(all(type(before.get(name)) is int and type(after.get(name)) is int
                        and 0 <= before[name] <= after[name] < 2**64 for name in fields)
                    and 0 < before["processed"] < after["processed"]
                    and before.get("overflowed") is False and after.get("overflowed") is False,
                    "captured checkpoint is not an earlier proven durable progress generation")
            for checkpoint in (old, value):
                require(type(checkpoint.get("checkpoint_time")) is int and 0 <= checkpoint["checkpoint_time"] < 2**64
                        and all(type(checkpoint.get(name)) is int and 0 <= checkpoint[name] < 2**64
                                for name in ("current_bucket_index", "current_object_index"))
                        and checkpoint["admin"].get("outcome", {}).get("execution") == {"state": "running"}
                        and checkpoint["admin"]["outcome"].get("coverage") == "partial",
                        "stale-generation fault has no original runnable cursor/time")
                counters = checkpoint["admin"]["outcome"]["counters"]
                require(counters["processed"] == sum(counters[name] for name in ("healed", "unchanged", "skipped", "failed"))
                        and counters["unknown"] <= counters["skipped"] and counters["attemptFailures"] >= counters["failed"],
                        "stale checkpoint counters do not account for original durable dispositions")
            require(old["checkpoint_time"] <= value["checkpoint_time"]
                    and (old["current_bucket_index"], old["current_object_index"]) <=
                        (value["current_bucket_index"], value["current_object_index"]),
                    "earlier checkpoint has a future or incomparable scan position")
            return earlier
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
        return corrupt_owned_mrf_crc(raw, intents, owner["bucket"])
    if kind == "truncate":
        require(len(raw) > 8, "cannot truncate an empty/unknown record")
        return raw[:len(raw) // 2]
    value["schema_version"] = 9
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def binding(root_fd, request, owner, fault):
    require(owner.get("runtimeProfile") in {"root-restart", "mrf-crash", "journal-bounded"}, "journal mutation requires an owned recovery runtime")
    require(owner["runId"] == request["runId"] and type(request.get("disk")) is int and 0 <= request["disk"] < 4,
            "journal run or disk identity differs")
    identifier = request.get("faultId")
    require(isinstance(identifier, str) and re.fullmatch(r"F07-[a-z0-9-]{1,48}", identifier), "journal fault identity is invalid")
    path = relative(request)
    require(owner.get("runtimeProfile") != "journal-bounded" or request["family"] == "checkpoint",
            "bounded journal mutation cannot target root admissions or MRF payloads")
    require(request["family"] == "checkpoint" and path[-1] == task_id(request["taskId"]) + "_ahm_checkpoint.json"
            or request["family"] == "root" and path[-1] == "root-heal-" + task_id(request["taskId"]) + ".json"
            or request["family"] == "mrf" and re.fullmatch(r"\.heal-mrf-snapshot\.[01]\.bin", path[-1]),
            "journal mutation cannot replace a terminal, resume state, digest, blocked fence or commit record")
    return {"schemaVersion": 1, "runId": owner["runId"], "disk": request["disk"], "family": request["family"],
            "taskId": request.get("taskId"), "relativePath": "/".join(path), "faultId": identifier,
            "binarySha256": owner["binarySha256"], "device": os.fstat(root_fd).st_dev,
            "mountId": fault.mount_id(root_fd), "productRepairClaimed": False}


def capture(root_fd, request, owner, fault):
    require(request.get("family") == "checkpoint", "only an actual Admin checkpoint has an earlier progress capture")
    identity = binding(root_fd, request, owner, fault)
    raw = read(root_fd, relative(request), fault)
    checkpoint_value(raw, request["taskId"], owner["bucket"])
    require(same_json(request.get("expectedBefore"), entry(raw)), "NOT_OBSERVED: checkpoint changed before its earlier capture")
    proof = {**identity, "captured": entry(raw), "originalProductBytes": True, "productMetadataMutated": False}
    with fault.parent(root_fd, ["quarantine"]) as backup:
        # Capture identity and bytes are immutable and distinct from fault backups.
        fault.new_file(backup, identity["faultId"] + ".early.intent.json", fault.encode(proof))
        fault.new_file(backup, identity["faultId"] + ".early.checkpoint", raw)
    return proof


def captured(root_fd, request, owner, fault):
    identity = binding(root_fd, request, owner, fault)
    identifier = request.get("captureId")
    require(isinstance(identifier, str) and re.fullmatch(r"F07-[a-z0-9-]{1,48}", identifier)
            and identifier != identity["faultId"] and request["family"] == "checkpoint",
            "stale-generation capture has no separate original snapshot identity")
    with fault.parent(root_fd, ["quarantine"]) as backup:
        proof = decode_json(fault.read_file(backup, identifier + ".early.intent.json", 16384))
        require(all(same_json(proof.get(name), value) for name, value in {**identity, "faultId": identifier}.items())
                and proof.get("originalProductBytes") is True and proof.get("productMetadataMutated") is False,
                "earlier checkpoint capture changed task, run, candidate, physical owner or record identity")
        raw = fault.read_file(backup, identifier + ".early.checkpoint", LIMIT)
        require(same_json(entry(raw), proof.get("captured")), "captured earlier product checkpoint bytes changed")
        checkpoint_value(raw, request["taskId"], owner["bucket"])
        return raw


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
    require(same_json(request.get("expectedBefore"), entry(before)), "journal bytes drifted from their archived pre-fault identity")
    earlier = captured(root_fd, request, owner, fault) if request.get("kind") == "stale-generation" else None
    require(earlier is not None or "captureId" not in request, "only a stale-generation fault consumes an earlier checkpoint")
    injected = transform(before, request, owner, mrf, earlier=earlier)
    intent = {**identity, "kind": request["kind"], "before": entry(before), "injected": entry(injected)}
    if earlier is not None:
        intent.update(captureId=request["captureId"], capturedGeneration=entry(earlier))
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
        intent = decode_json(fault.read_file(backup, identity["faultId"] + ".intent.json", 16384))
        require(all(same_json(intent.get(key), value) for key, value in identity.items()), "journal recovery lost original run/candidate/device/record identity")
        before = fault.read_file(backup, identity["faultId"] + ".original", LIMIT * 4)
        require(entry(before) == intent["before"], "journal original backup is corrupt")
        if intent["kind"] == "stale-generation":
            require(intent.get("captureId") == request.get("captureId"), "stale recovery changed its original earlier snapshot")
            require(entry(captured(root_fd, request, owner, fault)) == intent.get("capturedGeneration"),
                    "stale recovery lost original earlier generation evidence")
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
