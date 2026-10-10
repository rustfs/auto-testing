import base64
import copy
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest
import uuid
from unittest.mock import patch
import zlib

from scripts.validation_heal import mrf_records as records, mrf_replay as replay
from scripts.validation_heal.fleet_node import runtime_config
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES


class DurableMrfTests(unittest.TestCase):
    owner = "12345678-1234-4234-8234-123456789abc"
    version = "22345678-1234-4234-8234-123456789abc"
    incarnation = "32345678-1234-4234-8234-123456789abc"
    responsibility = "42345678-1234-4234-8234-123456789abc"

    def journal(self, *, layout=2, kind=3, key="offline/new.bin"):
        bucket = b"rv-heal-0123456789abcdef"
        raw = struct.pack("<BBBBQB", 1, layout, kind, 0, 100, 1) + uuid.UUID(self.version).bytes
        if layout in {2, 3}: raw += struct.pack("<II", 0, 0)
        raw += struct.pack("<II", len(bucket), len(key.encode())) + bucket + key.encode()
        if kind == 4:
            raw += uuid.UUID(self.incarnation).bytes * 2 + b"I" * 32 + struct.pack("<I", 3) + b"mpu"
        return raw + struct.pack("<I", zlib.crc32(raw))

    def manifest(self, payload, sequence=7):
        raw = b"RFMRFC01\x01" + uuid.UUID(self.owner).bytes + struct.pack("<QQ", sequence, len(payload)) + hashlib.sha256(payload).digest()
        return raw + hashlib.sha256(raw).digest()

    def companion(self, payload, **changes):
        item = {"intent_digest": list(hashlib.sha256(payload).digest()), "responsibility_id": self.responsibility,
                "source_bucket_incarnation_id": self.incarnation, "last_operator_acceptance": None, "state": {"state": "active"}}
        value = {"format_version": 1, "checkpoint_owner": self.owner, "checkpoint_sequence": 7, "records": [item]}
        value.update(changes)
        raw = json.dumps(value).encode()
        return b"RFMFLC01\x01" + struct.pack("<Q", len(raw)) + hashlib.sha256(raw).digest() + raw

    def observed(self, payload=None):
        payload = self.journal() if payload is None else payload
        raw = {".rustfs.sys/.heal-mrf-snapshot.0.bin": payload,
               ".rustfs.sys/.heal-mrf-commit.0.bin": self.manifest(payload),
               ".rustfs.sys/.heal-mrf-lifecycle.0.bin": self.companion(payload)}
        return {"readOnly": True, "stable": True, "records": [{"disk": disk, "relativePath": path,
            "present": disk in {0, 2} and path in raw,
            **({"size": len(raw[path]), "sha256": hashlib.sha256(raw[path]).hexdigest(),
                "rawBase64": base64.b64encode(raw[path]).decode()} if disk in {0, 2} and path in raw else {})}
            for disk in range(4) for path in records.FILES]}

    def obligation(self):
        return {"bucket": "rv-heal-0123456789abcdef", "key": "offline/new.bin", "versionId": self.version,
                "poolIndex": 0, "setIndex": 0, "kind": 3, "authoritativelyAbsentRequired": False}

    def test_whole_journal_preserves_utf8_version_set_and_complete_crc(self):
        raw = self.journal(key="中文/对象.bin") + self.journal(layout=1, kind=1, key="legacy")
        decoded = records.decode_journal(raw)
        self.assertEqual(decoded[0]["key"], "中文/对象.bin")
        self.assertEqual(decoded[0]["versionId"], self.version)
        self.assertEqual(decoded[0]["poolIndex"], 0)
        self.assertIsNone(decoded[1]["poolIndex"])
        for bad in (raw[:-1], raw + b"tail", bytes([0]) + raw[1:], raw[:30] + b"x" + raw[31:]):
            with self.subTest(raw=bad[:16]), self.assertRaises(ValueError): records.decode_journal(bad)

    def test_marker_purge_has_an_independent_incarnation_bound_layout(self):
        row = records.decode_journal(self.journal(layout=3, kind=4))[0]
        self.assertEqual(row["deleteMarkerPurge"]["bucketIncarnationId"], self.incarnation)
        self.assertEqual(row["deleteMarkerPurge"]["markerSize"], 3)
        with self.assertRaises(ValueError): records.decode_journal(self.journal(layout=2, kind=4))

    def test_committed_snapshot_binds_payload_and_lifecycle_on_exact_replicas(self):
        value = records.checkpoint(self.observed())
        self.assertEqual(value["sequence"], 7)
        self.assertEqual({row["disk"] for row in value["replicas"]}, {0, 2})
        self.assertEqual(value["intents"][0]["lifecycle"]["responsibility_id"], self.responsibility)
        for kind in ("sha", "payload", "missing", "duplicate", "path", "uncommitted", "unstable"):
            with self.subTest(kind=kind):
                bad = self.observed()
                row = next(row for row in bad["records"] if row["present"] and "snapshot" in row["relativePath"])
                if kind == "sha": row["sha256"] = "0" * 64
                elif kind == "payload":
                    raw = self.journal(key="other")
                    row.update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest(), rawBase64=base64.b64encode(raw).decode())
                elif kind == "missing": bad["records"].pop()
                elif kind == "duplicate": bad["records"][0] = copy.deepcopy(bad["records"][1])
                elif kind == "path": row["relativePath"] = ".rustfs.sys/config/iam/user.json"
                elif kind == "unstable": bad["stable"] = False
                else:
                    for item in bad["records"]:
                        if "commit" in item["relativePath"]:
                            item.clear(); item.update(disk=0, relativePath=".rustfs.sys/.heal-mrf-commit.0.bin", present=False)
                with self.assertRaises(ValueError): records.checkpoint(bad)

    def test_manifest_checksum_length_and_nonzero_epoch_are_required(self):
        raw = self.journal()
        self.assertEqual(records.commit(self.manifest(raw), raw)["owner"], self.owner)
        for bad in (self.manifest(raw)[:-1], self.manifest(raw, 0), self.manifest(raw, 2**64 - 1), self.manifest(raw)[:-1] + b"x"):
            with self.assertRaises(ValueError): records.commit(bad, raw)
        with self.assertRaises(ValueError): records.commit(self.manifest(raw), raw + b"x")

    def test_lifecycle_cannot_rebind_or_duplicate_the_committed_generation(self):
        raw = self.journal()
        binding = records.commit(self.manifest(raw), raw)
        valid = records.lifecycle(self.companion(raw), binding)
        self.assertIn(hashlib.sha256(raw).hexdigest(), valid)
        record = next(iter(valid.values()))
        record["intent_digest"] = list(hashlib.sha256(raw).digest())
        for changes in ({"checkpoint_sequence": 8}, {"checkpoint_owner": self.version},
                        {"format_version": True}, {"records": [record, record]}, {"unknown": 1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError): records.lifecycle(self.companion(raw, **changes), binding)

    def test_only_fresh_active_incarnation_bound_ack_responsibility_counts(self):
        observed = {"node1": records.checkpoint(self.observed())}
        obligations = [self.obligation()]
        binding = replay.pending_bindings(observed, obligations, 1)
        self.assertEqual(binding[replay.identity(obligations[0])][0]["responsibilityId"], self.responsibility)
        for kind in ("missing", "version", "scope", "stale", "held", "risk", "incarnation"):
            with self.subTest(kind=kind):
                bad = copy.deepcopy(observed)
                intent = bad["node1"]["intents"][0]
                if kind == "missing": bad["node1"]["intents"] = []
                elif kind == "version": intent["versionId"] = self.owner
                elif kind == "scope": intent["setIndex"] = 1
                elif kind == "stale": intent["enqueuedAtMs"] = 0
                elif kind == "held": intent["lifecycle"]["state"] = {"state": "held_unverified_legacy"}
                elif kind == "risk": intent["lifecycle"]["last_operator_acceptance"] = {"actor": "operator"}
                else: intent["lifecycle"]["source_bucket_incarnation_id"] = None
                with self.assertRaises(ValueError): replay.pending_bindings(bad, obligations, 1)

    def test_retirement_requires_every_original_owner_and_no_migrated_responsibility(self):
        obligation = self.obligation()
        pending = records.checkpoint(self.observed())
        binding = replay.pending_bindings({"node1": pending}, [obligation], 1)
        absent = {"present": False, "intents": [], "allAllowlistedFilesAbsent": True}
        self.assertFalse(replay.retirement({"node1": absent}, [obligation], binding)["queueDepthUsedAsProof"])
        for observed in ({"node1": pending}, {"node1": absent, "node2": pending},
                         {"node1": {**pending, "owner": self.version, "intents": []}},
                         {"node1": {**pending, "sequence": 6, "intents": []}}):
            with self.assertRaises(ValueError): replay.retirement(observed, [obligation], binding)

    def receipt(self, state="repaired"):
        item = self.obligation()
        return {"summary": "finished", "outcome": {"execution": {"state": "completed"}, "coverage": "complete",
            "objectsTruncated": False, "objects": [{"identity": {"kind": "object", "bucket": item["bucket"],
                "object": item["key"], "versionId": item["versionId"], "poolIndex": 0, "setIndex": 0,
                "bucketIncarnationId": self.incarnation}, "disposition": {"state": state}}],
            "counters": {"processed": 1, "healed": int(state == "repaired"), "unchanged": int(state != "repaired"),
                "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}}}

    def test_storage_verified_receipt_cannot_discharge_other_ack_or_unknown_result(self):
        obligation = self.obligation()
        owners = [{"bucketIncarnationId": self.incarnation}]
        self.assertEqual(replay.verified_receipt(self.receipt(), obligation, owners)["disposition"], "repaired")
        for state in ("unknown", "metadata_healthy", "deferred", "failed", "authoritatively_absent"):
            with self.assertRaises(ValueError): replay.verified_receipt(self.receipt(state), obligation, owners)
        for field in ("versionId", "bucketIncarnationId", "setIndex", "kind"):
            bad = self.receipt(); bad["outcome"]["objects"][0]["identity"][field] = "foreign"
            with self.assertRaises(ValueError): replay.verified_receipt(bad, obligation, owners)
        absent = {**obligation, "authoritativelyAbsentRequired": True}
        self.assertEqual(replay.verified_receipt(self.receipt("authoritatively_absent"), absent, owners)["disposition"], "authoritatively_absent")
        with self.assertRaises(ValueError): replay.verified_receipt(self.receipt(), absent, owners)

    def test_reader_never_enumerates_or_opens_credential_records(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            state = root / "state"; state.mkdir(); (state / "rustfs").write_bytes(b"candidate")
            roots = [root / str(disk) for disk in range(4)]
            for drive in roots: (drive / "data" / ".rustfs.sys").mkdir(parents=True)
            secret = roots[0] / "data" / ".rustfs.sys" / "credentials.json"; secret.write_text("private")
            owner = {"runtimeProfile": "durable-mrf", "binarySha256": hashlib.sha256(b"candidate").hexdigest()}
            with patch("scripts.validation_heal.mrf_records.os.walk", side_effect=AssertionError("no directory traversal")):
                result = records.read_records(state, roots, owner, lambda path, boundary: path)
            self.assertTrue(result["stable"])
            self.assertTrue(records.checkpoint(result)["allAllowlistedFilesAbsent"])
            self.assertNotIn("private", json.dumps(result))
            selected = roots[0] / "data" / ".rustfs.sys" / ".heal-mrf-snapshot.0.bin"
            selected.symlink_to(secret)
            with self.assertRaises(OSError): records.read_records(state, roots, owner, lambda path, boundary: path)

    def test_candidate_contract_and_runtime_keep_other_repair_sources_disabled(self):
        records.verify_reference({"source_sha": records.REFERENCE["candidateCommit"]})
        with self.assertRaises(ValueError): records.verify_reference({"source_sha": "0" * 40})
        config = runtime_config("durable-mrf")
        self.assertEqual(config["RUSTFS_HEAL_MRF_ENABLE"], "true")
        self.assertEqual(config["RUSTFS_SCANNER_ENABLED"], "false")
        self.assertEqual(config["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertEqual(select("E05")[0]["variants"], list(replay.BRANCHES))
        self.assertIn(replay.CASE_ID, AUTOMATIC_CASES)


if __name__ == "__main__": unittest.main()
