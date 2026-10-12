"""Original committed MRF corruption must remain distinct from safe rescan."""

import base64
import copy
from contextlib import ExitStack
import hashlib
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import struct
import zlib
from unittest.mock import patch

from scripts.validation_heal import journal_fault as journal, persistence_mrf as suite
import test_validation_heal_mrf as fixtures


class MrfCorruptionTests(unittest.TestCase):
    def fixture(self):
        original = {"node": fixtures.DurableMrfTests().observed()}
        current, receipts = copy.deepcopy(original), []
        for row in current["node"]["records"]:
            if not row["present"] or ".heal-mrf-snapshot." not in row["relativePath"]: continue
            raw = base64.b64decode(row["rawBase64"])
            corrupted = raw[:-1] + bytes([raw[-1] ^ 1])
            receipts.append({"node": "node", "disk": row["disk"], "relativePath": row["relativePath"],
                "family": "mrf", "taskId": None, "kind": "checksum", "applied": True,
                "originalBackupRetained": True, "productRepairClaimed": False,
                "before": journal.entry(raw), "injected": journal.entry(corrupted)})
            row.update(journal.entry(corrupted), rawBase64=base64.b64encode(corrupted).decode())
        return original, current, receipts

    def test_corrupt_crc_keeps_every_manifest_lifecycle_and_unknown_byte(self):
        proof = suite.retained_corruption(*self.fixture())
        self.assertEqual(proof["corruptedPayloads"], 2)
        self.assertTrue(proof["commitsAndLifecycleUnchanged"])
        self.assertFalse(proof["originalResponsibilityRetired"])
        self.assertFalse(proof["safeRescanIsOriginalMrfCompletion"])
        self.assertFalse(proof["backupRescueUsed"])

    def test_missing_rewritten_restored_rebound_or_extra_record_is_not_retention(self):
        for change in ("missing-payload", "manifest", "lifecycle", "restore", "foreign-path", "partial", "duplicate", "unstable", "absent-bytes"):
            original, current, receipts = self.fixture()
            rows = current["node"]["records"]
            payload = next(row for row in rows if row["present"] and "snapshot" in row["relativePath"])
            if change in {"missing-payload", "manifest", "lifecycle"}:
                kind = "commit" if change == "manifest" else change
                target = payload if change == "missing-payload" else next(row for row in rows if row["present"] and kind in row["relativePath"])
                identity = {key: target[key] for key in ("disk", "relativePath")}
                target.clear(); target.update(identity, present=False)
            elif change == "restore": payload.update(copy.deepcopy(next(row for row in original["node"]["records"] if row["disk"]==payload["disk"] and row["relativePath"]==payload["relativePath"])))
            elif change == "foreign-path": payload["relativePath"] = ".rustfs.sys/config/iam/user.json"
            elif change == "partial": rows.pop()
            elif change == "duplicate": rows[-1] = copy.deepcopy(rows[0])
            elif change == "unstable": current["node"]["stable"] = False
            else: next(row for row in rows if not row["present"])["rawBase64"] = ""
            with self.subTest(change=change), self.assertRaises(ValueError): suite.retained_corruption(original, current, receipts)

    def test_mutation_receipt_requires_original_bytes_exact_crc_scope_and_backup(self):
        for change in ("no-mutation", "duplicate", "owner", "before", "injected", "kind", "task", "repair", "backup"):
            original, current, receipts = self.fixture()
            if change == "no-mutation": receipts = []
            elif change == "duplicate": receipts.append(copy.deepcopy(receipts[0]))
            elif change == "owner": receipts[0]["node"] = "other"
            elif change in {"before", "injected"}: receipts[0][change]["sha256"] = "f" * 64
            elif change == "kind": receipts[0]["kind"] = "truncate"
            elif change == "task": receipts[0]["taskId"] = fixtures.DurableMrfTests.owner
            elif change == "repair": receipts[0]["productRepairClaimed"] = True
            else: receipts[0]["originalBackupRetained"] = False
            with self.subTest(change=change), self.assertRaises(ValueError): suite.retained_corruption(original, current, receipts)

    def test_original_corruption_diagnostic_is_bound_to_each_mutated_node_and_typed_error(self):
        event = {"target": "rustfs::heal::mrf", "message": "MRF committed replay checkpoint could not be inspected",
                 "error": "MRF checkpoint has an invalid or incomplete commit record"}
        self.assertEqual(suite.diagnostics({"node": [event]}, {"node"}), {"node": [event]})
        for change in ("target", "message", "error", "missing-node", "no-mutated-node"):
            value = copy.deepcopy(event)
            if change in value: value[change] = "permission denied" if change == "error" else "unrelated"
            nodes = {"node", "other"} if change == "missing-node" else set() if change == "no-mutated-node" else {"node"}
            with self.subTest(change=change), self.assertRaises(ValueError): suite.diagnostics({"node": [value]}, nodes)

    def test_lost_injection_ack_registers_recovery_before_mutation_and_keeps_namespace(self):
        with tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
            root = Path(temp); rc = root / "rc"; rc.write_bytes(b"client")
            args = SimpleNamespace(evidence=root, rc_bin=rc, tool_directory=root, testing_commit="a" * 40, binary_sha="b" * 64)
            bucket = "rv-heal-" + hashlib.sha256(b"gh-12345-1-h1").hexdigest()[:16]
            class ScopedFixture(fixtures.DurableMrfTests):
                def journal(self, **kwargs):
                    raw = super().journal(**kwargs)[:-4].replace(b"rv-heal-0123456789abcdef", bucket.encode())
                    return raw + struct.pack("<I", zlib.crc32(raw))
            original = ScopedFixture()
            expected = [{"key": "object", "versionId": original.version, "deleteMarker": False}]
            captured = []
            class Fleet(suite.Fleet):
                def deploy(self, *args): self.reserved = list(self.nodes); captured.append(self)
                def snapshot(self, *args, **kwargs): return {}
                def start(self): pass
                def stop(self): pass
                def remote(self, node, action, **params):
                    if action == "mrf-records": return original.observed()
                    if action == "drive-gate-probe": return {"errno": 13, "readProbe": {"errno": 13}, "dacBypassRemoved": True}
                    if action == "journal-apply":
                        assert (node, params) in self.journal_faults
                        raise OSError("SSH acknowledgement lost after original CRC mutation")
                    if action == "journal-recover": raise ValueError("original transaction state must be audited")
                    if action == "cleanup": raise AssertionError("dirty namespace cannot be deleted")
                    return {}
            class Probe:
                def __init__(self, *args): pass
                def inventory(self, *args, **kwargs): return {}
            for name, replacement in {"Fleet": Fleet, "Probe": Probe, "Recorder": lambda *args: None,
                "verify_inspector": lambda *args: {}, "verify_quorum_contract": lambda *args: {},
                "validate_topology": lambda *args: {}, "seed": lambda *args: expected,
                "verify_seed": lambda *args: {}, "run_task": lambda *args, **kwargs: {},
                "validate_version_receipts": lambda *args: {"bucketIncarnationId": original.incarnation},
                "idle_fleet": lambda *args: None, "map_target": lambda *args, **kwargs: {"cohort": [{"node": "node", "disk": 0}]}}.items():
                stack.enter_context(patch.object(suite, name, replacement))
            stack.enter_context(patch.dict(os.environ, {"GITHUB_RUN_ID": "12345"}))
            stack.enter_context(patch.object(suite.records, "verify_reference", return_value={}))
            stack.enter_context(patch.object(suite.mrf, "acknowledge", return_value=(expected, [original.obligation()], {})))
            stack.enter_context(patch.object(suite.mrf, "read_checkpoints", return_value={}))
            stack.enter_context(patch.object(suite.mrf, "pending_bindings", return_value={(): [{"node": "node", "bucketIncarnationId": original.incarnation}]}))
            result = suite.run(args, "H1", ["node"], "DV", {"source_sha": "c" * 40, "package_sha256": "d" * 64})
            self.assertEqual((result["verdict"], result["cleanup"]), ("FAIL", "FAIL"))
            self.assertEqual(result["failure"]["type"], "OSError")
            self.assertEqual(len(captured[0].journal_faults), 1)
            self.assertFalse(result["canonicalCasePassClaimed"])


if __name__ == "__main__": unittest.main()
