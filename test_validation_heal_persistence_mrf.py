"""Original committed MRF corruption must remain distinct from safe rescan."""

import base64
import copy
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
import struct
import zlib
from unittest.mock import patch

from scripts.validation_heal import journal_fault as journal, persistence_mrf as suite, drive_gate
import test_validation_heal_mrf as fixtures


class MrfCorruptionTests(unittest.TestCase):
    def bucket_gate_fixture(self, root):
        state, disk = root / "state", root / "disk"
        state.mkdir()
        data = disk / "data"; data.mkdir(parents=True, mode=0o700)
        system = data / ".rustfs.sys"; system.mkdir(mode=0o700)
        (system / "format.json").write_bytes(b"format-control")
        bucket = data / "rv-heal-0123456789abcdef"
        bucket.mkdir(mode=0o755)
        obj = bucket / "objects/0000.bin"; obj.mkdir(parents=True)
        (obj / "xl.meta").write_bytes(b"original-fixture-metadata")
        (state / "owner.json").write_text(json.dumps({"runtimeProfile": "mrf-crash", "bucket": bucket.name}))
        return state, disk, bucket, system

    def rooted_lstat(self):
        original = Path.lstat
        def observe(path):
            value = original(path)
            return SimpleNamespace(st_mode=value.st_mode, st_uid=0, st_nlink=value.st_nlink,
                st_size=value.st_size, st_dev=value.st_dev, st_ino=value.st_ino)
        return patch.object(Path, "lstat", observe)

    def test_bucket_gate_denies_only_owned_business_root_and_restores_original_permissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, disk, bucket, system = self.bucket_gate_fixture(Path(temporary))
            original = {path: (path.stat().st_mode, path.stat().st_ino) for path in (disk / "data", bucket, system)}
            with self.rooted_lstat():
                armed = drive_gate.apply_gate(state, [disk], 0, "permission-denied", scope="bucket")
                self.assertEqual((armed["scope"], armed["dataPath"]), ("bucket", str(bucket)))
                drive_gate.activate_gate(state, [disk])
                self.assertEqual(stat.S_IMODE(bucket.stat().st_mode), 0)
                self.assertEqual((system.stat().st_mode, system.stat().st_ino), original[system])
                self.assertEqual((disk / "data").stat().st_mode, original[disk / "data"][0])
                self.assertEqual((system / "format.json").read_bytes(), b"format-control")
                self.assertTrue(drive_gate.release_gate(state, [disk])["originalPermissionsRestored"])
            self.assertEqual((bucket.stat().st_mode, bucket.stat().st_ino), original[bucket])
            self.assertEqual((bucket / "objects/0000.bin/xl.meta").read_bytes(), b"original-fixture-metadata")

    def test_bucket_gate_refuses_foreign_runtime_path_links_and_changed_scope(self):
        for change in ("runtime", "bucket", "scope", "mode", "linked-bucket", "linked-objects", "linked-object", "hardlinked-meta"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                state, disk, bucket, _ = self.bucket_gate_fixture(Path(temporary))
                owner = json.loads((state / "owner.json").read_text())
                if change == "runtime": owner["runtimeProfile"] = "incarnation-mrf"
                elif change == "bucket": owner["bucket"] = "../foreign"
                elif change == "hardlinked-meta": os.link(bucket / "objects/0000.bin/xl.meta", bucket / "linked-meta")
                elif change.startswith("linked-"):
                    folder = {"linked-bucket": bucket, "linked-objects": bucket / "objects", "linked-object": bucket / "objects/0000.bin"}[change]
                    held = folder.with_name(folder.name + "-held"); folder.rename(held); folder.symlink_to(held, target_is_directory=True)
                (state / "owner.json").write_text(json.dumps(owner))
                with self.rooted_lstat(), self.assertRaises(ValueError):
                    drive_gate.apply_gate(state, [disk], 0, "read-only" if change == "mode" else "permission-denied",
                                          scope="foreign" if change == "scope" else "bucket")
                self.assertFalse((state / "drive-gate.json").exists())

    def test_bucket_gate_marker_cannot_redirect_activation_or_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, disk, bucket, system = self.bucket_gate_fixture(Path(temporary))
            with self.rooted_lstat():
                drive_gate.apply_gate(state, [disk], 0, "permission-denied", scope="bucket")
                marker = state / "drive-gate.json"; value = json.loads(marker.read_text())
                value["dataPath"] = str(system); marker.write_text(json.dumps(value)); marker.chmod(0o600)
                with self.assertRaises(ValueError): drive_gate.activate_gate(state, [disk])
                with self.assertRaises(ValueError): drive_gate.release_gate(state, [disk])
            self.assertEqual(stat.S_IMODE(bucket.stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE(system.stat().st_mode), 0o700)

    def test_bucket_probe_keeps_same_candidate_namespace_and_checks_system_read_and_write(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, disk, bucket, _ = self.bucket_gate_fixture(Path(temporary))
            for failure in (None, "system-read", "system-write", "system-identity"):
                denied = SimpleNamespace(returncode=0, stdout=json.dumps({"uid": 0, "gid": 0, "errno": 13}))
                control = {"uid": 0, "gid": 0, "errno": 0}
                write, read = copy.deepcopy(control), copy.deepcopy(control)
                if failure == "system-read": read["errno"] = 13
                elif failure == "system-write": write["errno"] = 13
                elif failure == "system-identity": write["uid"] = 1000
                outputs = [denied, denied, *(SimpleNamespace(returncode=0, stdout=json.dumps(x)) for x in (write, read))]
                value = {"mode": "permission-denied", "scope": "bucket", "phase": "active", "dataPath": str(bucket), "disk": 0}
                with self.rooted_lstat(), patch.object(drive_gate, "checked_gate", return_value=value), \
                     patch.object(drive_gate, "process_identity", return_value={"uid": 0, "gid": 0, "capEff": 0}), \
                     patch.object(Path, "readlink", side_effect=[Path("mnt:[candidate]"), Path("mnt:[host]")]), \
                     patch.object(drive_gate.subprocess, "run", side_effect=outputs) as invoke:
                    if failure is None:
                        proof = drive_gate.probe_gate(state, [disk], 123)
                        self.assertTrue(proof["systemMetadataAccessible"])
                        self.assertTrue(proof["systemMetadataControl"]["sameServiceIdentity"])
                    else:
                        with self.subTest(failure=failure), self.assertRaises(ValueError): drive_gate.probe_gate(state, [disk], 123)
                for args in invoke.call_args_list:
                    self.assertIn("--mount=/proc/123/ns/mnt", args.args[0])
                    self.assertIn("--bounding-set=-dac_override,-dac_read_search", args.args[0])

    def test_mrf_fixture_refuses_disk_wide_gate_or_failed_system_control(self):
        valid = {"scope": "bucket", "disk": 0, "errno": 13, "readProbe": {"errno": 13}, "dacBypassRemoved": True,
            "systemMetadataAccessible": True, "systemMetadataControl": {"sameServiceIdentity": True,
                "read": {"errno": 0}, "write": {"errno": 0}}}
        suite.validate_member_denial(valid, 0)
        for change in ("scope", "disk", "read", "write", "control", "identity", "accessible"):
            value = copy.deepcopy(valid)
            if change == "scope": value["scope"] = "disk"
            elif change == "disk": value["disk"] = 1
            elif change in {"read", "write"}: value["systemMetadataControl"][change]["errno"] = 13
            elif change == "control": value.pop("systemMetadataControl")
            elif change == "identity": value["systemMetadataControl"]["sameServiceIdentity"] = False
            else: value["systemMetadataAccessible"] = False
            with self.subTest(change=change), self.assertRaises(ValueError): suite.validate_member_denial(value, 0)

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
        return original, current, receipts, "rv-heal-0123456789abcdef"

    def add_older_slot(self, observed, *, foreign=False, empty=False):
        fixture = fixtures.DurableMrfTests()
        raw = fixture.journal()
        if foreign:
            body = raw[:-4].replace(b"rv-heal-0123456789abcdef", b"rv-heal-fedcba9876543210")
            raw = body + struct.pack("<I", zlib.crc32(body))
        if empty:
            raw = b""
        contents = {"snapshot": raw, "commit": fixture.manifest(raw, 6),
                    "lifecycle": fixture.companion(raw, checkpoint_sequence=6)}
        for row in observed["records"]:
            if row["disk"] not in {0, 2} or not row["relativePath"].endswith(".1.bin"):
                continue
            kind = row["relativePath"].split(".heal-mrf-", 1)[1].split(".", 1)[0]
            raw = contents[kind]
            row.update(present=True, **journal.entry(raw), rawBase64=base64.b64encode(raw).decode())

    def test_corruption_plan_covers_all_replicas_and_readable_older_slots(self):
        fixture = fixtures.DurableMrfTests()
        bucket = fixture.obligation()["bucket"]
        original = fixture.observed()
        self.add_older_slot(original)
        self.assertEqual(suite.records.checkpoint(original)["sequence"], 7)
        targets = suite.corruption_targets(original, bucket)
        self.assertEqual({(row["disk"], row["relativePath"]) for row in targets},
                         {(disk, f".rustfs.sys/.heal-mrf-snapshot.{slot}.bin") for disk in (0, 2) for slot in (0, 1)})
        self.assertEqual({row["sequence"] for row in targets}, {6, 7})
        current, receipts = copy.deepcopy(original), []
        for target in targets:
            row = next(row for row in current["records"] if
                       (row["disk"], row["relativePath"]) == (target["disk"], target["relativePath"]))
            raw = base64.b64decode(row["rawBase64"])
            receipt = {"node": "node", "disk": target["disk"], "relativePath": target["relativePath"],
                       "family": "mrf", "taskId": None, "kind": "checksum", "applied": True,
                       "originalBackupRetained": True, "productRepairClaimed": False, "before": journal.entry(raw)}
            corrupt = journal.transform(raw, receipt, {"bucket": bucket}, suite.records)
            receipt["injected"] = journal.entry(corrupt)
            row.update(**journal.entry(corrupt), rawBase64=base64.b64encode(corrupt).decode())
            receipts.append(receipt)
            manifest_path = row["relativePath"].replace("snapshot", "commit")
            manifest = next(item for item in current["records"] if
                            item["disk"] == row["disk"] and item["relativePath"] == manifest_path)
            with self.assertRaises(ValueError):
                suite.records.commit(base64.b64decode(manifest["rawBase64"]), corrupt)
        proof = suite.retained_corruption({"node": original}, {"node": current}, receipts, bucket)
        self.assertEqual(proof["corruptedPayloads"], 4)
        self.assertTrue(proof["commitsAndLifecycleUnchanged"])

    def test_corruption_plan_refuses_foreign_or_empty_older_replay_source(self):
        for change in ("foreign", "empty"):
            original = fixtures.DurableMrfTests().observed()
            self.add_older_slot(original, foreign=change == "foreign", empty=change == "empty")
            # The latest source remains owned; an older readable source still
            # prevents safe construction of this all-source corruption fault.
            self.assertEqual(suite.records.checkpoint(original)["sequence"], 7)
            before = copy.deepcopy(original)
            with self.subTest(change=change), self.assertRaises(ValueError):
                suite.corruption_targets(original, "rv-heal-0123456789abcdef")
            self.assertEqual(original, before)

    def test_corrupt_crc_keeps_every_manifest_lifecycle_and_unknown_byte(self):
        proof = suite.retained_corruption(*self.fixture())
        self.assertEqual(proof["corruptedPayloads"], 2)
        self.assertTrue(proof["commitsAndLifecycleUnchanged"])
        self.assertFalse(proof["originalResponsibilityRetired"])
        self.assertFalse(proof["safeRescanIsOriginalMrfCompletion"])
        self.assertFalse(proof["backupRescueUsed"])

    def test_missing_rewritten_restored_rebound_or_extra_record_is_not_retention(self):
        for change in ("missing-payload", "manifest", "lifecycle", "restore", "foreign-path", "partial", "duplicate", "unstable", "absent-bytes"):
            original, current, receipts, bucket = self.fixture()
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
            with self.subTest(change=change), self.assertRaises(ValueError): suite.retained_corruption(original, current, receipts, bucket)

    def test_mutation_receipt_requires_original_bytes_exact_crc_scope_and_backup(self):
        for change in ("no-mutation", "duplicate", "owner", "before", "injected", "kind", "task", "repair", "backup"):
            original, current, receipts, bucket = self.fixture()
            if change == "no-mutation": receipts = []
            elif change == "duplicate": receipts.append(copy.deepcopy(receipts[0]))
            elif change == "owner": receipts[0]["node"] = "other"
            elif change in {"before", "injected"}: receipts[0][change]["sha256"] = "f" * 64
            elif change == "kind": receipts[0]["kind"] = "truncate"
            elif change == "task": receipts[0]["taskId"] = fixtures.DurableMrfTests.owner
            elif change == "repair": receipts[0]["productRepairClaimed"] = True
            else: receipts[0]["originalBackupRetained"] = False
            with self.subTest(change=change), self.assertRaises(ValueError): suite.retained_corruption(original, current, receipts, bucket)

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
        self.run_until_corruption_failure('lost-ack')

    def test_corrupt_replay_preserves_the_same_active_business_gate_through_startup(self):
        actions = self.run_until_corruption_failure('replay')
        replay = actions[max(index for index, action in enumerate(actions) if action == 'journal-apply') + 1:]
        self.assertEqual(replay[:3], ['gate-state', 'start', 'drive-gate-probe'])
        self.assertNotIn('clear', replay[:replay.index('drive-gate-probe') + 1])
        self.assertNotIn('arm', replay)

    def test_changed_stopped_business_gate_keeps_rescue_and_does_not_restart(self):
        actions = self.run_until_corruption_failure('bad-state')
        replay = actions[max(index for index, action in enumerate(actions) if action == 'journal-apply') + 1:]
        self.assertNotIn('start', replay)

    def test_later_replica_lost_ack_is_registered_before_mutation_and_keeps_namespace(self):
        self.run_until_corruption_failure('later-lost-ack')

    def test_omitted_replica_receipt_cannot_restart_the_corrupt_replay(self):
        actions = self.run_until_corruption_failure('omitted-receipt')
        replay = actions[max(index for index, action in enumerate(actions) if action == 'journal-apply') + 1:]
        self.assertNotIn('start', replay)

    def run_until_corruption_failure(self, phase):
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
            captured, actions = [], []
            class Fleet(suite.Fleet):
                def deploy(self, *args): self.reserved = list(self.nodes); captured.append(self)
                def snapshot(self, *args, **kwargs): return {}
                def start(self): actions.append('start')
                def stop(self): pass
                def remote(self, node, action, **params):
                    if action == 'drive-gate':
                        actions.append('clear' if params['mode'] == 'clear' else 'arm')
                        if params['mode'] == 'clear':
                            return {'state': 'CLEAR', 'originalPermissionsRestored': True}
                        assert params['scope'] == 'bucket'
                        return {'state': 'ARMED', 'phase': 'armed', 'disk': 0, 'scope': 'bucket'}
                    if action == 'gate-state':
                        actions.append(action)
                        return {'mode': 'permission-denied', 'phase': 'active', 'disk': 0,
                            'scope': 'disk' if phase == 'bad-state' else 'bucket'}
                    if action == "mrf-records": return original.observed()
                    if action == "drive-gate-probe":
                        actions.append(action)
                        return {"errno": 13, "readProbe": {"errno": 13}, "dacBypassRemoved": True, "scope": "bucket", "disk": 0,
                            "systemMetadataAccessible": True, "systemMetadataControl": {"sameServiceIdentity": True,
                                "read": {"errno": 0}, "write": {"errno": 0}}}
                    if action == "journal-apply":
                        actions.append(action)
                        assert (node, params) in self.journal_faults
                        if phase == 'lost-ack' or phase == 'later-lost-ack' and params['disk'] == 2:
                            raise OSError("SSH acknowledgement lost after original CRC mutation")
                        if phase == 'omitted-receipt' and params['disk'] == 2:
                            return {"disk": 0, "relativePath": params["relativePath"]}
                        return {"disk": params["disk"], "relativePath": params["relativePath"]}
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
            if phase not in {'lost-ack', 'later-lost-ack'}:
                stack.enter_context(patch.object(suite, 'retained_corruption', return_value={}))
                stack.enter_context(patch.object(suite, 'JournalWindow', return_value=SimpleNamespace(capture=lambda: {})))
                stack.enter_context(patch.object(suite, 'diagnostics', side_effect=OSError('stop after replay readiness proof')))
            result = suite.run(args, "H1", ["node"], "DV", {"source_sha": "c" * 40, "package_sha256": "d" * 64})
            self.assertEqual((result["verdict"], result["cleanup"]), ("FAIL", "FAIL"))
            self.assertEqual(result["failure"]["type"], "ValueError" if phase in {'bad-state', 'omitted-receipt'} else "OSError")
            self.assertEqual(len(captured[0].journal_faults), 1 if phase == 'lost-ack' else 2)
            self.assertFalse(result["canonicalCasePassClaimed"])
            return actions


if __name__ == "__main__": unittest.main()
