"""Read only the candidate's exact committed MRF files and decode whole records.

This is a source-bound observer. It never searches system metadata, repairs a
checkpoint, accepts operator risk, or treats a missing journal as repair proof.
"""

import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import uuid
import zlib


REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926", "sourceFiles": {
    "crates/heal/src/heal/mrf_queue.rs": "44fc2616024392ceef1e28816cb998d7e385618792545b26af3a3d7337ef9ca3",
    "crates/heal/src/heal/mrf_queue/snapshot.rs": "ecfcb5ee63edef5c5d3d6713edc127e16f489522afc5fc02dc1044ccca241d14",
    "crates/heal/src/heal/mrf_queue/partial_write.rs": "20415e0e985daea8fc62c5e152ed438e75008c33a2a592275e1dd9d1d4fee771",
    "crates/common/src/mrf_channel.rs": "075bb84bcbbe7d76674f730c64c8b69fe37d0790ec75da46365ba2e0cab34e0f",
    "crates/heal/src/heal/manager/scheduler.rs": "79ce1f9c278c664231b6a1eca7c521f8e98742fe6d17d8f852368da4e77cc33b"},
    "journal": "complete CRC32/ISO-HDLC records; format 1, layouts 1/2/3",
    "commit": "RFMRFC01 version 1; owner, sequence, payload length and two SHA-256 digests",
    "lifecycle": "RFMFLC01 version 1; checksummed JSON, same committed owner and sequence",
    "retirement": "exact kind/bucket/object/version/pool/set/incarnation and successful verified disposition"}
LIMIT = 4 * 1024**2
FILES = {f".rustfs.sys/.heal-mrf-{kind}.{slot}.bin": LIMIT * (4 if kind == "lifecycle" else 1)
         for slot in (0, 1) for kind in ("snapshot", "commit", "lifecycle")}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical_uuid(value):
    require(isinstance(value, str) and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0,
            "MRF identity is not a canonical nonzero UUID")
    return value


def verify_reference(candidate):
    require(candidate.get("source_sha") == REFERENCE["candidateCommit"], "unsupported candidate MRF record/proof contract")
    return REFERENCE


def read_records(state, roots, owner, checked):
    """Exact allowlist, no directory enumeration, no write descriptor."""
    require(owner.get("runtimeProfile") == "durable-mrf", "MRF records require the dedicated run-owned runtime")
    require(hashlib.sha256((state / "rustfs").read_bytes()).hexdigest() == owner["binarySha256"], "MRF candidate changed")

    def collect():
        rows = []
        for disk, root in enumerate(roots):
            data = root / "data"
            for relative, limit in FILES.items():
                path = checked(data / relative, data)
                row = {"disk": disk, "relativePath": relative, "present": False}
                try:
                    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                except FileNotFoundError:
                    rows.append(row)
                    continue
                with os.fdopen(fd, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= limit,
                            "MRF record exceeds its exact regular-file limit")
                    raw = stream.read(limit + 1)
                    after = os.fstat(stream.fileno())
                try:
                    current = path.lstat()
                except FileNotFoundError:
                    return None
                fields = ("st_dev", "st_ino", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
                if len(raw) != before.st_size or any(getattr(before, field) != getattr(other, field)
                        for other in (after, current) for field in fields):
                    return None
                row.update(present=True, size=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                           rawBase64=base64.b64encode(raw).decode())
                rows.append(row)
        return rows

    for _ in range(3):
        first, second = collect(), collect()
        if first is not None and first == second:
            return {"readOnly": True, "stable": True, "binarySha256": owner["binarySha256"], "records": second}
    return {"readOnly": True, "stable": False, "binarySha256": owner["binarySha256"], "records": []}


def decode_journal(raw):
    require(isinstance(raw, bytes) and len(raw) <= LIMIT, "MRF journal exceeds the decoder bound")
    records, cursor = [], 0
    while cursor < len(raw):
        start = cursor
        require(len(records) < 2048 and len(raw) - cursor >= 25, "MRF journal is torn or has too many records")
        format_tag, layout, kind, attempts, stamp, has_version = struct.unpack_from("<BBBBQB", raw, cursor)
        require(format_tag == 1 and layout in {1, 2, 3} and kind in {1, 2, 3, 4}
                and (kind == 4) == (layout == 3) and has_version in {0, 1}, "unsupported MRF journal record")
        cursor += 13

        def take(count):
            nonlocal cursor
            require(cursor + count <= len(raw), "MRF journal is truncated")
            value = raw[cursor:cursor + count]
            cursor += count
            return value

        version = str(uuid.UUID(bytes=take(16))) if has_version else "null"
        require(version != "00000000-0000-0000-0000-000000000000", "MRF record has a nil explicit version")
        scope = struct.unpack("<II", take(8)) if layout in {2, 3} else (None, None)
        bucket_size, key_size = struct.unpack("<II", take(8))
        require(0 < bucket_size <= 1024 and 0 < key_size <= 1024, "MRF journal identity exceeds its bound")
        bucket, key = take(bucket_size).decode("utf-8"), take(key_size).decode("utf-8")
        purge = None
        if kind == 4:
            incarnation = canonical_uuid(str(uuid.UUID(bytes=take(16))))
            marker_incarnation = canonical_uuid(str(uuid.UUID(bytes=take(16))))
            marker_digest = take(32).hex()
            marker_size = struct.unpack("<I", take(4))[0]
            require(version != "null" and incarnation == marker_incarnation and 0 < marker_size <= 1024**2,
                    "MRF marker purge lacks its exact incarnation or payload")
            marker = take(marker_size)
            purge = {"bucketIncarnationId": incarnation, "markerIncarnationId": marker_incarnation,
                     "markerIdentity": marker_digest, "markerSize": len(marker)}
        body_end = cursor
        checksum = struct.unpack("<I", take(4))[0]
        require(zlib.crc32(raw[start:body_end]) == checksum, "MRF journal CRC does not match the full record")
        records.append({"kind": kind, "bucket": bucket, "key": key, "versionId": version,
            "poolIndex": None if kind == 2 else scope[0], "setIndex": None if kind == 2 else scope[1],
            "attempts": attempts, "enqueuedAtMs": stamp, "deleteMarkerPurge": purge,
            "intentDigest": hashlib.sha256(raw[start:cursor]).hexdigest()})
    return records


def commit(raw, payload):
    require(len(raw) == 105 and raw[:9] == b"RFMRFC01\x01", "invalid MRF committed manifest")
    owner = canonical_uuid(str(uuid.UUID(bytes=raw[9:25])))
    sequence, length = struct.unpack("<QQ", raw[25:41])
    require(0 < sequence < 2**64 - 1 and length == len(payload) <= LIMIT
            and hashlib.sha256(payload).digest() == raw[41:73]
            and hashlib.sha256(raw[:73]).digest() == raw[73:], "MRF commit sequence, payload length or digest differs")
    return {"owner": owner, "sequence": sequence, "payloadSha256": raw[41:73].hex()}


def lifecycle(raw, binding):
    require(49 <= len(raw) <= LIMIT * 4 and raw[:9] == b"RFMFLC01\x01", "invalid MRF lifecycle header")
    require(struct.unpack("<Q", raw[9:17])[0] == len(raw) - 49
            and hashlib.sha256(raw[49:]).digest() == raw[17:49], "MRF lifecycle length or checksum differs")
    def unique_pairs(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, "MRF lifecycle contains a duplicate JSON field")
            value[key] = item
        return value
    value = json.loads(raw[49:], object_pairs_hook=unique_pairs)
    require(set(value) == {"format_version", "checkpoint_owner", "checkpoint_sequence", "records"}
            and type(value["format_version"]) is int and value["format_version"] == 1
            and value["checkpoint_owner"] == binding["owner"]
            and type(value["checkpoint_sequence"]) is int and value["checkpoint_sequence"] == binding["sequence"]
            and isinstance(value["records"], list) and len(value["records"]) <= 2048,
            "MRF lifecycle belongs to another committed epoch or schema")
    records, ids, digests = {}, set(), set()
    for record in value["records"]:
        require(set(record) == {"intent_digest", "responsibility_id", "source_bucket_incarnation_id", "last_operator_acceptance", "state"},
                "unsupported MRF lifecycle responsibility schema")
        responsibility_id = canonical_uuid(record["responsibility_id"])
        incarnation = record["source_bucket_incarnation_id"]
        if incarnation is not None: canonical_uuid(incarnation)
        digest = record["intent_digest"]
        require(isinstance(digest, list) and len(digest) == 32 and all(type(item) is int and 0 <= item <= 255 for item in digest),
                "MRF lifecycle has an invalid intent digest")
        digest = bytes(digest).hex()
        require(responsibility_id not in ids and (digest, incarnation) not in digests,
                "MRF lifecycle duplicates a responsibility generation")
        ids.add(responsibility_id); digests.add((digest, incarnation))
        records[digest] = {**record, "intent_digest": digest}
    return records


def checkpoint(observed):
    require(observed.get("readOnly") is True and observed.get("stable") is True,
            "NOT_OBSERVED: MRF publication changed throughout its bounded observation")
    rows = observed.get("records")
    expected = {(disk, path) for disk in range(4) for path in FILES}
    require(isinstance(rows, list) and len(rows) == len(expected)
            and {(row.get("disk"), row.get("relativePath")) for row in rows} == expected,
            "MRF file census omitted or duplicated an exact allowlisted record")
    indexed = {}
    for row in rows:
        if row.get("present") is False:
            require(not any(name in row for name in ("rawBase64", "sha256", "size")), "absent MRF file contains invented bytes")
            continue
        require(row.get("present") is True, "MRF file presence is unknown")
        raw = base64.b64decode(row["rawBase64"], validate=True)
        require(len(raw) == row.get("size") <= FILES[row["relativePath"]]
                and hashlib.sha256(raw).hexdigest() == row.get("sha256"), "MRF raw evidence differs from its file digest")
        indexed[(row["disk"], row["relativePath"])] = raw
    committed = []
    for disk in range(4):
        for slot in (0, 1):
            prefix = ".rustfs.sys/.heal-mrf-"
            manifest = indexed.get((disk, prefix + f"commit.{slot}.bin"))
            if manifest is None: continue
            payload = indexed.get((disk, prefix + f"snapshot.{slot}.bin"))
            require(payload is not None, "MRF committed manifest lost its payload")
            binding = commit(manifest, payload)
            intents = decode_journal(payload)
            companion = indexed.get((disk, prefix + f"lifecycle.{slot}.bin"))
            records = lifecycle(companion, binding) if companion is not None else {}
            require(not any(intent["kind"] in {3, 4} for intent in intents) or companion is not None,
                    "MRF durable responsibility lost its committed lifecycle companion")
            for intent in intents:
                if intent["kind"] in {3, 4}:
                    require(intent["intentDigest"] in records, "MRF durable intent has no exact generation checkpoint")
                    intent["lifecycle"] = records[intent["intentDigest"]]
            committed.append({**binding, "disk": disk, "slot": slot, "intents": intents})
    if not committed:
        require(not indexed, "MRF has uncommitted or orphan record files; absence is not established")
        return {"present": False, "intents": [], "readOnly": True, "allAllowlistedFilesAbsent": True}
    owners = {row["owner"] for row in committed}
    require(len(owners) == 1, "MRF committed files mixed process owners")
    sequence = max(row["sequence"] for row in committed)
    latest = [row for row in committed if row["sequence"] == sequence]
    require(len({row["payloadSha256"] for row in latest}) == 1
            and all(row["intents"] == latest[0]["intents"] for row in latest), "MRF same-sequence committed replicas disagree")
    return {**latest[0], "present": True, "replicas": [{"disk": row["disk"], "slot": row["slot"]} for row in latest]}
