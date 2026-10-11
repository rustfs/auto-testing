"""Exact durable-record corruption preserves ownership and refuses stale rescue."""

import base64
import copy
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
from scripts.validation_heal.checkpoint import OPTIONS


class DurableRecordFaultTests(unittest.TestCase):
    token = "12345678-1234-4234-8234-123456789abc"
    incarnation = "22345678-1234-4234-8234-123456789abc"
    bucket = "rv-heal-0123456789abcdef"

    def signed(self, value):
        value = copy.deepcopy(value)
        value["integrity_digest"] = None
        unsigned = copy.deepcopy(value)
        for name in ("processed_objects", "failed_objects", "skipped_objects"): unsigned[name].sort()
        value["integrity_digest"] = base64.b64encode(hashlib.sha256(json.dumps(unsigned,
            sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).digest()).decode()
        return json.dumps(value).encode()

    def checkpoint(self, processed=2, time=100):
        return self.signed({"task_id": self.token, "schema_version": 8, "checkpoint_time": time,
            "current_bucket_index": 0, "current_object_index": processed,
            "admin": {"completed": False, "buckets": {self.bucket: self.incarnation}, "options": OPTIONS,
                "set_disk_id": "pool_0_set_0", "outcome": {"execution": {"state": "running"}, "coverage": "partial",
                    "objects": [], "objectsTruncated": False, "counters": {"processed": processed, "healed": 1,
                        "unchanged": processed - 1, "skipped": 0, "failed": 0, "unknown": 0,
                        "attemptFailures": 0, "overflowed": False}}},
            "processed_objects": [f"objects/{i}" for i in reversed(range(processed))],
            "failed_objects": [], "skipped_objects": [], "successful_objects": processed,
            "failed_object_count": 0, "skipped_object_count": 0, "skipped_new_versions": 0, "skipped_ilm_expired": 0,
            "processed_bytes": processed, "total_objects": 8, "total_bytes": 8,
            "baseline_generation": None, "baseline_known": False, "counter_unknown": False})

    def stale_fixture(self, root):
        owner, request, path, early = self.fixture(root, kind="stale-generation")
        capture_request = {key: value for key, value in request.items() if key != "kind"}
        capture_request["faultId"] = "F07-early"
        with fault.directory(root) as fd: journal.capture(fd, capture_request, owner, fault)
        newer = self.checkpoint(processed=3, time=101)
        path.write_bytes(newer)
        request.update(expectedBefore=journal.entry(newer), captureId=capture_request["faultId"])
        return owner, request, path, early, newer

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

    def test_stale_injection_uses_original_capture_and_rescue_restores_newer_pre_fault_bytes(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, early, newer = self.stale_fixture(root)
            with fault.directory(root) as fd:
                result = journal.install(fd, request, owner, fault, mrf)
                self.assertEqual(path.read_bytes(), early)
                self.assertEqual(result["capturedGeneration"], journal.entry(early))
                self.assertEqual(result["before"], journal.entry(newer))
                self.assertEqual((root / "quarantine/F07-checkpoint-0.original").read_bytes(), newer)
                self.assertTrue(journal.recover(fd, request, owner, fault)["restoredFromBackup"])
                self.assertEqual(path.read_bytes(), newer)
                self.assertFalse(journal.recover(fd, request, owner, fault)["restoredFromBackup"])
            self.assertEqual((root / "quarantine/F07-early.early.checkpoint").read_bytes(), early)
            self.assertEqual(next((root / "quarantine").glob("*.failed-*")).read_bytes(), early)
            self.assertEqual((root / "data/.rustfs.sys/unrelated.json").read_bytes(), b"untouched control")

    def test_stale_generation_rejects_valid_digests_with_different_scopes_or_unproven_progress(self):
        owner = {"bucket": self.bucket}
        request = {"family": "checkpoint", "taskId": self.token, "kind": "stale-generation",
                   "relativePath": journal.names("checkpoint", self.token)[0]}
        current = self.checkpoint(processed=3, time=101)
        for change in ("task", "incarnation", "options", "option-type", "set", "equal", "newer", "zero",
                       "counter-type", "regressed-counter", "dishonest-counter", "overflow", "time", "time-type",
                       "cursor", "cursor-type", "completed", "coverage", "schema-type"):
            old = json.loads(self.checkpoint())
            counters = old["admin"]["outcome"]["counters"]
            if change == "task": old["task_id"] = str(uuid.uuid4())
            elif change == "incarnation": old["admin"]["buckets"][self.bucket] = str(uuid.uuid4())
            elif change == "options": old["admin"]["options"]["nolock"] = True
            elif change == "option-type": old["admin"]["options"]["pool"] = False
            elif change == "set": old["admin"]["set_disk_id"] = "pool_0_set_1"
            elif change == "equal": old = json.loads(current)
            elif change == "newer": old = json.loads(self.checkpoint(processed=4, time=102))
            elif change == "zero": counters.update(processed=0, healed=0, unchanged=0)
            elif change == "counter-type": counters["healed"] = True
            elif change == "regressed-counter": counters.update(healed=2, unchanged=0)
            elif change == "dishonest-counter": counters["unchanged"] = 0
            elif change == "overflow": counters["overflowed"] = True
            elif change == "time": old["checkpoint_time"] = 102
            elif change == "time-type": old["checkpoint_time"] = True
            elif change == "cursor": old["current_object_index"] = 4
            elif change == "cursor-type": old["current_object_index"] = 2.0
            elif change == "completed": old["admin"]["completed"] = True
            elif change == "coverage": old["admin"]["outcome"]["coverage"] = "complete"
            else: old["schema_version"] = 8.0
            with self.subTest(change=change), self.assertRaises(ValueError):
                journal.transform(current, request, owner, mrf, earlier=self.signed(old))
        self.assertEqual(journal.transform(current, request, owner, mrf, earlier=self.checkpoint()), self.checkpoint())

    def test_missing_corrupt_or_foreign_capture_never_modifies_current_checkpoint(self):
        for change in ("missing-proof", "missing-bytes", "proof", "bytes", "candidate", "device", "mount",
                       "disk", "disk-type", "run", "task", "path", "capture-id", "same-id", "entry-type", "duplicate-key"):
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, _, newer = self.stale_fixture(root)
                proof_path = root / "quarantine/F07-early.early.intent.json"
                bytes_path = root / "quarantine/F07-early.early.checkpoint"
                proof = json.loads(proof_path.read_text())
                if change == "missing-proof": proof_path.unlink()
                elif change == "missing-bytes": bytes_path.unlink()
                elif change == "proof": proof_path.write_bytes(b'{"invalid":true}')
                elif change == "bytes": bytes_path.write_bytes(b"changed product bytes")
                elif change == "capture-id": request["captureId"] = "F07-foreign"
                elif change == "same-id": request["captureId"] = request["faultId"]
                elif change == "duplicate-key": proof_path.write_bytes(proof_path.read_bytes()[:-1] + b',"disk":0}')
                else:
                    if change == "candidate": proof["binarySha256"] = "b" * 64
                    elif change == "device": proof["device"] = -1
                    elif change == "mount": proof["mountId"] = 2
                    elif change == "disk": proof["disk"] = 1
                    elif change == "disk-type": proof["disk"] = False
                    elif change == "run": proof["runId"] = "gh-other-1-h1"
                    elif change == "task": proof["taskId"] = str(uuid.uuid4())
                    elif change == "path": proof["relativePath"] = ".rustfs.sys/foreign"
                    else: proof["captured"]["size"] = float(proof["captured"]["size"])
                    proof_path.write_text(json.dumps(proof))
                with fault.directory(root) as fd, self.subTest(change=change), self.assertRaises((ValueError, FileNotFoundError)):
                    journal.install(fd, request, owner, fault, mrf)
                self.assertEqual(path.read_bytes(), newer)
                self.assertFalse((root / "quarantine/F07-checkpoint-0.original").exists())

    def test_capture_is_immutable_and_rejects_drift_without_touching_product_bytes(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
            request.pop("kind")
            with fault.directory(root) as fd:
                with self.assertRaisesRegex(ValueError, "NOT_OBSERVED"):
                    journal.capture(fd, {**request, "expectedBefore": journal.entry(raw + b" ")}, owner, fault)
                self.assertEqual(list((root / "quarantine").iterdir()), [])
                captured = journal.capture(fd, request, owner, fault)
                self.assertTrue(captured["originalProductBytes"])
                self.assertFalse(captured["productMetadataMutated"])
                with self.assertRaises(FileExistsError): journal.capture(fd, request, owner, fault)
                self.assertEqual(path.read_bytes(), raw)

    def test_stale_rescue_rejects_advanced_deleted_checkpoint_or_lost_capture_identity(self):
        for change in ("advance", "delete", "capture-id", "capture-bytes", "capture-proof", "device-type"):
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, _, _ = self.stale_fixture(root)
                with fault.directory(root) as fd:
                    journal.install(fd, request, owner, fault, mrf)
                    if change == "advance": path.write_bytes(self.checkpoint(processed=4, time=102))
                    elif change == "delete": path.unlink()
                    elif change == "capture-id": request["captureId"] = "F07-other"
                    elif change == "capture-bytes": (root / "quarantine/F07-early.early.checkpoint").write_bytes(b"changed")
                    elif change == "capture-proof": (root / "quarantine/F07-early.early.intent.json").unlink()
                    else:
                        p = root / "quarantine/F07-checkpoint-0.intent.json"
                        intent = json.loads(p.read_text()); intent["device"] = float(intent["device"])
                        p.write_text(json.dumps(intent))
                    current = path.read_bytes() if path.exists() else None
                    with self.subTest(change=change), self.assertRaises((ValueError, FileNotFoundError)):
                        journal.recover(fd, request, owner, fault)
                    self.assertEqual(path.read_bytes() if path.exists() else None, current)

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

    def test_live_capture_requires_the_same_stable_record_and_service_birth(self):
        for changed in (None, "record", "process"):
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
                identity = {name: name for name in fleet_node.PROCESS_FIELDS}
                identity.update(pid=50, state="S")
                request.pop("kind"); request.update(action="journal-capture", expected=identity)
                if changed == "record": request["expectedBefore"] = journal.entry(raw + b" ")
                successor = {**identity, "startTicks": 999} if changed == "process" else identity
                with patch.object(fleet_node, "owned", return_value=(root, [root], owner)), \
                     patch.object(fleet_node, "digest", return_value=owner["binarySha256"]), \
                     patch.object(fleet_node, "fault_module", return_value=fault), \
                     patch.dict(fleet_node.__dict__, journal_fault_backend=journal), \
                     patch.object(fleet_node, "stopped", side_effect=AssertionError("capture must not stop service")), \
                     patch.object(fleet_node, "process", side_effect=[identity, successor]):
                    if changed:
                        with self.subTest(changed=changed), self.assertRaises(ValueError): fleet_node.journal_operation(request)
                    else:
                        result = fleet_node.journal_operation(request)
                        self.assertTrue(result["processBound"])
                        self.assertFalse(result["serviceFrozen"])
                        self.assertEqual(result["observation"]["independentReads"], 2)
                        self.assertEqual(result["captured"], journal.entry(raw))
                self.assertEqual(path.read_bytes(), raw)
                captured = root / "quarantine/F07-checkpoint-0.early.checkpoint"
                self.assertEqual(captured.exists(), changed != "record")

    def test_non_stale_fault_cannot_consume_a_capture_and_capture_cannot_mutate_records(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
            with fault.directory(root) as fd, self.assertRaises(ValueError):
                journal.install(fd, {**request, "captureId": "F07-early"}, owner, fault, mrf)
            identity = {name: name for name in fleet_node.PROCESS_FIELDS}; identity.update(pid=50, state="S")
            capture = {**request, "action": "journal-capture", "expected": identity}
            with patch.object(fleet_node, "owned", return_value=(root, [root], owner)), \
                 patch.object(fleet_node, "digest", return_value=owner["binarySha256"]), \
                 patch.object(fleet_node, "fault_module", return_value=fault), \
                 patch.dict(fleet_node.__dict__, journal_fault_backend=journal), self.assertRaises(ValueError):
                fleet_node.journal_operation(capture)
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(list((root / "quarantine").iterdir()), [])

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

    def test_fleet_cleanup_restores_newer_original_before_deleting_private_namespace(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, _, newer = self.stale_fixture(root)
            fleet = Fleet(["node"], root, owner["runId"]); fleet.reserved = ["node"]
            fleet.journal_faults.append(("node", request))
            with fault.directory(root) as fd: journal.install(fd, request, owner, fault, mrf)
            actions = []
            def remote(node, action, **params):
                actions.append(action)
                if action == "journal-recover":
                    with fault.directory(root) as fd: return journal.recover(fd, params, owner, fault)
                if action == "cleanup": self.assertEqual(path.read_bytes(), newer)
                return {}
            with patch.object(fleet, "remote", side_effect=remote): self.assertTrue(fleet.cleanup())
            self.assertLess(actions.index("stop"), actions.index("journal-recover"))
            self.assertLess(actions.index("journal-recover"), actions.index("cleanup"))
            self.assertEqual(fleet.journal_faults, [])

    def test_fleet_cleanup_keeps_every_namespace_and_pending_fault_when_product_advanced(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, _, _ = self.stale_fixture(root)
            fleet = Fleet(["node", "peer"], root, owner["runId"]); fleet.reserved = ["node", "peer"]
            fleet.journal_faults.append(("node", request))
            with fault.directory(root) as fd: journal.install(fd, request, owner, fault, mrf)
            advanced = self.checkpoint(processed=4, time=102); path.write_bytes(advanced)
            actions = []
            def remote(node, action, **params):
                actions.append(action)
                if action == "journal-recover":
                    with fault.directory(root) as fd: return journal.recover(fd, params, owner, fault)
                return {}
            with patch.object(fleet, "remote", side_effect=remote): self.assertFalse(fleet.cleanup())
            self.assertNotIn("cleanup", actions)
            self.assertEqual(path.read_bytes(), advanced)
            self.assertEqual(fleet.journal_faults, [("node", request)])
            recovery = json.loads(next(root.glob("*-recovery.json")).read_text())
            self.assertEqual(recovery["cleanup"], "FAIL")
            self.assertTrue(recovery["errors"][0]["namespacesRetained"])

    def test_bounded_journal_census_reads_verified_candidate_views_and_rejects_other_families(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, _, raw = self.fixture(root)
            owner["runtimeProfile"] = "journal-bounded"
            request = {key: request[key] for key in ("runId", "family", "taskId")}; request["action"] = "journal-census"
            with patch.object(fleet_node, "owned", return_value=(root, ['/original/unbound-root'], owner)), \
                 patch.object(fleet_node, "volume_views", return_value=[root]) as views, \
                 patch.object(fleet_node, "digest", return_value=owner['binarySha256']), \
                 patch.object(fleet_node, "fault_module", return_value=fault), patch.object(fleet_node, "stopped"), \
                 patch.dict(fleet_node.__dict__, journal_fault_backend=journal):
                result = fleet_node.journal_operation(request)
                self.assertEqual(base64.b64decode(next(row for row in result['records'] if row['present'])['rawBase64']), raw)
                views.assert_called_once()
                for family, action in (('mrf', 'journal-census'), ('root', 'journal-apply'), ('root', 'journal-capture')):
                    with self.subTest(family=family, action=action), self.assertRaises(ValueError):
                        fleet_node.journal_operation({**request, 'family': family, 'action': action})

    def test_bounded_root_admission_reads_the_actual_service_view_without_mutating_it(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, 'mount_id', return_value=1):
            root = Path(temp).resolve(); owner, request, _, _ = self.fixture(root)
            owner['runtimeProfile'] = 'journal-bounded'
            raw = json.dumps({'schema': 3, 'task_id': self.token, 'heal_type': {'type': 'erasure_set'}}).encode()
            path = root / 'data/.rustfs.sys' / ('root-heal-' + self.token + '.json'); path.write_bytes(raw)
            request = {'runId': owner['runId'], 'taskId': self.token, 'quiescent': True}
            with patch.object(fleet_node, 'owned', return_value=(root, ['/original/unbound'], owner)), \
                 patch.object(fleet_node, 'volume_views', return_value=[root]), \
                 patch.object(fleet_node, 'digest', return_value=owner['binarySha256']), patch.object(fleet_node, 'stopped'):
                observed = fleet_node.task_records(request)
            present = [row for row in observed['records'] if row['present']]
            self.assertEqual(len(present), 1)
            self.assertEqual(present[0]['sha256'], journal.entry(raw)['sha256'])
            self.assertEqual(path.read_bytes(), raw)

    def test_bounded_journal_cleanup_releases_filler_before_rescue_and_preserves_failed_release(self):
        for failed in (False, True):
            with tempfile.TemporaryDirectory() as temp:
                fleet = Fleet(['node'], Path(temp), 'gh-12345-1-h1', runtime_profile='journal-bounded')
                fleet.reserved = ['node']; fleet.journal_faults.append(('node', {'faultId': 'F07-checkpoint-0'}))
                actions = []
                def remote(node, action, **params):
                    actions.append(action)
                    if action == 'bounded-volumes-release':
                        self.assertTrue(params['quiescent'])
                        if failed: raise ValueError('owned filesystem identity changed')
                    if action == 'journal-recover':
                        return {'backupRetained': True, 'originalResponsibilityRecreated': False, 'productRepairClaimed': False}
                    return {}
                with patch.object(fleet, 'remote', side_effect=remote): self.assertEqual(fleet.cleanup(), not failed)
                if failed:
                    self.assertNotIn('journal-recover', actions); self.assertNotIn('cleanup', actions)
                    self.assertTrue(fleet.journal_faults)
                else:
                    self.assertLess(actions.index('bounded-volumes-release'), actions.index('journal-recover'))
                    self.assertLess(actions.index('journal-recover'), actions.index('cleanup'))

    def test_bounded_remote_journal_and_cleanup_embed_the_exact_capacity_backend(self):
        with tempfile.TemporaryDirectory() as temp, patch('scripts.validation_heal.healthy.subprocess.run',
                return_value=types.SimpleNamespace(returncode=0, stdout='{}', stderr='')) as command:
            fleet = Fleet(['node'], Path(temp), 'gh-12345-1-h1', runtime_profile='journal-bounded')
            for action in ('journal-census', 'cleanup'):
                fleet.remote('node', action, family='checkpoint', taskId=self.token)
                source = command.call_args.kwargs['input'].rsplit('\nmain(', 1)[0]
                scope = {'__name__': 'bounded_journal_remote'}
                exec(compile(source, 'bounded-journal-remote', 'exec'), scope)
                self.assertTrue(callable(scope['volume_module']().views))
                self.assertTrue(callable(scope['volume_module']().dispose))
                if action == 'journal-census': self.assertTrue(callable(scope['journal_fault_backend'].census))


if __name__ == "__main__": unittest.main()
