"""Exact durable-record corruption preserves ownership and refuses stale rescue."""

import base64
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch
import zlib

from scripts.validation_heal import journal_fault as journal, heal_fault as fault, mrf_records as mrf
from scripts.validation_heal.healthy import Fleet
from scripts.validation_heal import fleet_node


class DurableRecordFaultTests(unittest.TestCase):
    token = "12345678-1234-4234-8234-123456789abc"
    incarnation = "22345678-1234-4234-8234-123456789abc"
    bucket = "rv-heal-0123456789abcdef"

    def checkpoint(self):
        value = {"task_id": self.token, "schema_version": 8,
            "admin": {"completed": False, "buckets": {self.bucket: self.incarnation}},
            "processed_objects": ["objects/second", "objects/first"], "failed_objects": [], "skipped_objects": [],
            "integrity_digest": None, "current_object_index": 2, "successful_objects": 2}
        unsigned = {**value, "processed_objects": sorted(value["processed_objects"])}
        value["integrity_digest"] = base64.b64encode(hashlib.sha256(json.dumps(unsigned,
            sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).digest()).decode()
        return json.dumps(value).encode()

    def fixture(self, root, kind="truncate", family="checkpoint"):
        (root / "data/.rustfs.sys/buckets").mkdir(parents=True)
        (root / "quarantine").mkdir()
        owner = {"runtimeProfile": "root-restart", "runId": "gh-12345-1-h1", "bucket": self.bucket, "binarySha256": "a" * 64}
        request = {"runId": owner["runId"], "family": family, "taskId": self.token, "disk": 0,
            "faultId": "F07-checkpoint-0", "kind": kind, "relativePath": journal.names(family, self.token)[0]}
        raw = self.checkpoint() if family == "checkpoint" else json.dumps({"schema": 3, "task_id": self.token,
            "heal_type": {"type": "bucket", "bucket": self.bucket}, "bucket_incarnation_id": self.incarnation}).encode()
        path = root / "data" / request["relativePath"]
        path.write_bytes(raw)
        request["expectedBefore"] = journal.entry(raw)
        (root / "data/.rustfs.sys/unrelated.json").write_bytes(b"untouched control")
        return owner, request, path, raw

    def test_real_file_fault_and_idempotent_rescue_retain_original_and_failed_bytes(self):
        for kind in ["truncate", "future-schema"]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root, kind)
                with fault.directory(root) as fd:
                    result = journal.install(fd, request, owner, fault, mrf)
                    self.assertEqual(result["before"], journal.entry(raw))
                    self.assertNotEqual(path.read_bytes(), raw)
                    self.assertFalse(result["productRepairClaimed"])
                    if kind == "future-schema": self.assertEqual(json.loads(path.read_bytes())["schema_version"], 9)
                    rescued = journal.recover(fd, request, owner, fault)
                    self.assertTrue(rescued["restoredFromBackup"])
                    self.assertEqual(path.read_bytes(), raw)
                    self.assertFalse(journal.recover(fd, request, owner, fault)["restoredFromBackup"])
                self.assertEqual((root / "data/.rustfs.sys/unrelated.json").read_bytes(), b"untouched control")
                self.assertEqual((root / "quarantine/F07-checkpoint-0.original").read_bytes(), raw)
                self.assertEqual(len(list((root / "quarantine").glob("*.failed-*"))), 1)

    def test_root_fault_requires_exact_bucket_admission_and_preserves_terminal_names(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root, family="root")
            with fault.directory(root) as fd:
                journal.install(fd, request, owner, fault, mrf)
                journal.recover(fd, request, owner, fault)
                self.assertEqual(path.read_bytes(), raw)
            for field, value in [("task_id", str(uuid.uuid4())), ("bucket_incarnation_id", str(uuid.UUID(int=0))),
                                 ("heal_type", {"type": "bucket", "bucket": "foreign"})]:
                bad = json.loads(raw); bad[field] = value
                with self.subTest(field=field), self.assertRaises(ValueError):
                    journal.transform(json.dumps(bad).encode(), request, owner, mrf)

    def test_path_token_slot_profile_and_expected_bytes_cannot_escape_scope(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
            changes = [{"relativePath": ".rustfs.sys/config/config.json"}, {"relativePath": "../foreign"},
                {"relativePath": journal.names("checkpoint", self.token)[1]}, {"taskId": str(uuid.UUID(int=0))},
                {"taskId": "../escape"}, {"disk": True}, {"disk": 4}, {"runId": "gh-other-1-h1"},
                {"faultId": "C11-foreign"}, {"kind": "stale-generation"}, {"expectedBefore": {"sha256": "b" * 64, "size": len(raw)}}]
            with fault.directory(root) as fd:
                for change in changes:
                    with self.subTest(change=change), self.assertRaises(ValueError): journal.install(fd, {**request, **change}, owner, fault, mrf)
                    self.assertEqual(path.read_bytes(), raw)
                with self.assertRaises(ValueError): journal.install(fd, request, {**owner, "runtimeProfile": "manual"}, fault, mrf)

    def test_corrupt_or_foreign_original_checkpoint_is_never_mutated(self):
        owner = {"bucket": self.bucket}
        request = {"family": "checkpoint", "taskId": self.token, "kind": "truncate",
                   "relativePath": journal.names("checkpoint", self.token)[0]}
        raw = self.checkpoint()
        for field, value in [("task_id", str(uuid.uuid4())), ("schema_version", 9), ("integrity_digest", "wrong"),
                             ("admin", {"completed": True, "buckets": {self.bucket: self.incarnation}}),
                             ("admin", {"completed": False, "buckets": {"foreign": self.incarnation}})]:
            bad = json.loads(raw); bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): journal.transform(json.dumps(bad).encode(), request, owner, mrf)

    def test_successor_deleted_or_foreign_bytes_are_not_overwritten_by_rescue(self):
        for changed in ["advanced", "missing", "backup", "candidate", "device"]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, _ = self.fixture(root)
                with fault.directory(root) as fd:
                    journal.install(fd, request, owner, fault, mrf)
                    if changed == "advanced": path.write_bytes(b"new product responsibility")
                    elif changed == "missing": path.unlink()
                    elif changed == "backup": (root / "quarantine/F07-checkpoint-0.original").write_bytes(b"bad backup")
                    elif changed == "candidate": owner["binarySha256"] = "b" * 64
                    else:
                        p = root / "quarantine/F07-checkpoint-0.intent.json"; intent = json.loads(p.read_text()); intent["device"] = -1
                        p.write_text(json.dumps(intent))
                    current = path.read_bytes() if path.exists() else None
                    with self.subTest(changed=changed), self.assertRaises((ValueError, FileNotFoundError)):
                        journal.recover(fd, request, owner, fault)
                    self.assertEqual(path.read_bytes() if path.exists() else None, current)

    def test_record_symlink_hardlink_and_retained_backup_prevent_mutation(self):
        for changed in ["symlink", "hardlink", "backup"]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
                if changed == "symlink": path.unlink(); original = root / "original"; original.write_bytes(raw); path.symlink_to(original)
                elif changed == "hardlink": os.link(path, root / "alias")
                else: (root / "quarantine/F07-checkpoint-0.original").write_bytes(b"retained")
                with fault.directory(root) as fd, self.subTest(changed=changed), self.assertRaises((OSError, ValueError)):
                    journal.install(fd, request, owner, fault, mrf)
                self.assertEqual(path.read_bytes(), raw)

    def test_interrupted_rescue_temp_is_revalidated_without_erasing_injection_evidence(self):
        for changed in [False, True]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
                with fault.directory(root) as fd:
                    journal.install(fd, request, owner, fault, mrf)
                    injection = path.parent / ".F07-F07-checkpoint-0.inject"
                    injection.write_bytes(b"interrupted injection evidence")
                    rescue = path.parent / ".F07-F07-checkpoint-0.rescue"
                    rescue.write_bytes(b"wrong temporary" if changed else raw)
                    if changed:
                        current = path.read_bytes()
                        with self.assertRaises(ValueError): journal.recover(fd, request, owner, fault)
                        self.assertEqual(path.read_bytes(), current)
                    else:
                        journal.recover(fd, request, owner, fault)
                        self.assertEqual(path.read_bytes(), raw)
                    self.assertEqual(injection.read_bytes(), b"interrupted injection evidence")

    def test_mrf_crc_fault_retains_every_original_identity_and_refuses_foreign_payload(self):
        bucket, key = self.bucket.encode(), b"offline/new.bin"
        raw = struct.pack("<BBBBQB", 1, 2, 3, 0, 100, 1) + uuid.UUID(self.token).bytes
        raw += struct.pack("<II", 0, 0) + struct.pack("<II", len(bucket), len(key)) + bucket + key
        raw += struct.pack("<I", zlib.crc32(raw))
        request = {"family": "mrf", "kind": "checksum", "relativePath": ".rustfs.sys/.heal-mrf-snapshot.0.bin"}
        damaged = journal.transform(raw, request, {"bucket": self.bucket}, mrf)
        self.assertEqual(damaged[:-1], raw[:-1])
        with self.assertRaises(ValueError): mrf.decode_journal(damaged)
        with self.assertRaises(ValueError): journal.transform(raw, request, {"bucket": "foreign"}, mrf)

    def test_census_is_exact_and_never_enumerates_unrelated_system_metadata(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); _, request, _, raw = self.fixture(root)
            with patch.object(os, "listdir", side_effect=AssertionError("system metadata enumeration forbidden")):
                result = journal.census([root], request, fault)
            self.assertEqual(len(result["records"]), 4)
            present = [row for row in result["records"] if row["present"]]
            self.assertEqual(len(present), 1)
            self.assertEqual(base64.b64decode(present[0]["rawBase64"]), raw)
            self.assertFalse(result["directoryEnumerationUsed"])

    def test_stable_observation_rejects_atomic_replacement_between_reads(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); _, request, path, raw = self.fixture(root)
            stable = journal.stable_census([root], request, fault)
            self.assertFalse(stable["quiescent"])
            self.assertTrue(stable["stable"])
            original = journal.census
            calls = []
            def changed(*args, **kwargs):
                value = original(*args, **kwargs)
                if not calls: path.write_bytes(raw + b" ")
                calls.append(value)
                return value
            with patch.object(journal, "census", side_effect=changed), self.assertRaisesRegex(ValueError, "NOT_OBSERVED"):
                journal.stable_census([root], request, fault)

    def test_duplicate_and_nonfinite_json_never_become_candidate_checkpoint_bytes(self):
        raw = self.checkpoint()
        for bad in [raw[:-1] + b',"task_id":"foreign"}', raw.replace(b'"current_object_index": 2', b'"current_object_index": NaN')]:
            with self.subTest(raw=bad), self.assertRaises(ValueError): journal.checkpoint_value(bad, self.token, self.bucket)

    def test_live_adapter_binds_both_reads_to_original_process_and_cannot_mutate(self):
        identity = {name: name for name in fleet_node.PROCESS_FIELDS}
        identity.update(pid=50, state="T")
        owner = {"runtimeProfile": "root-restart", "binarySha256": "a" * 64}
        observed = {"readOnly": True, "quiescent": False, "stable": True, "independentReads": 2}
        request = {"runId": "gh-12345-1-h1", "action": "journal-observe", "family": "checkpoint",
                   "taskId": self.token, "expected": identity}
        backend = types.SimpleNamespace(stable_census=lambda *args: observed)
        with patch.object(fleet_node, "owned", return_value=(Path('/owned'), [], owner)), \
             patch.object(fleet_node, "digest", return_value="a" * 64), \
             patch.object(fleet_node, "fault_module", return_value=fault), \
             patch.dict(fleet_node.__dict__, journal_fault_backend=backend), \
             patch.object(fleet_node, "stopped", side_effect=AssertionError("read-only live observation must not stop service")):
            with patch.object(fleet_node, "process", side_effect=[identity, identity]):
                result = fleet_node.journal_operation(request)
                self.assertTrue(result["processBound"])
                self.assertTrue(result["serviceFrozen"])
                self.assertFalse(result["quiescent"])
            for changed in [{**identity, "pid": 60}, {**identity, "binarySha256": "b" * 64},
                            {**identity, "startTicks": 999}]:
                with self.subTest(changed=changed), patch.object(fleet_node, "process", side_effect=[identity, changed]), self.assertRaises(ValueError):
                    fleet_node.journal_operation(request)
            with patch.object(fleet_node, "process", return_value=identity), self.assertRaises(ValueError):
                fleet_node.journal_operation({**request, "kind": "truncate"})

    def test_remote_action_loads_exact_record_and_fault_dependencies(self):
        with tempfile.TemporaryDirectory() as temp, patch("scripts.validation_heal.healthy.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout='{}', stderr='')) as command:
            Fleet(["node"], Path(temp), "gh-12345-1-h1").remote("node", "journal-census", family="root", taskId=self.token)
            source = command.call_args.kwargs["input"].rsplit("\nmain(", 1)[0]
            scope = {"__name__": "validation_journal_remote"}
            exec(compile(source, "journal-remote", "exec"), scope)
            self.assertTrue(callable(scope["journal_fault_backend"].install))
            self.assertTrue(callable(scope["fault_module"]().read_file))
            self.assertTrue(callable(scope["mrf"].checkpoint))


if __name__ == "__main__": unittest.main()
