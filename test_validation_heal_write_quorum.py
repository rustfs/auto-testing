import copy
import errno
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import quorum_gate as gate, write_quorum as executor
from scripts.validation_heal import healthy, shards
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
import test_validation_heal as oracle


class WriteQuorumTests(unittest.TestCase):
    contract = {"recognized": True, "writeQuorumRule": "data-plus-one-if-equal-parity"}

    def scene(self, total=4, branch=executor.BRANCHES[0]):
        source = oracle.HealReadQuorumBoundaryTest()
        snapshot, expected = source.scene(total=total, data=total // 2)
        target = shards.map_target(snapshot, source.bucket, expected[0]["key"], "data", 1)
        if branch == executor.BRANCHES[1] and total == 16:
            target = {**target, **next(row for row in target["cohort"] if row["node"] == "node4")}
        return snapshot, target, executor.plan_for(snapshot, target, branch, self.contract)

    def test_payload_loss_does_not_count_as_actual_write_or_lock_loss(self):
        for total in (4, 16):
            snapshot, target, plan = self.scene(total)
            self.assertEqual(plan["writableDrives"], plan["writeQuorum"] - 1)
            self.assertEqual(sum(len(disks) for disks in plan["gates"].values()), total - plan["writeQuorum"] + 1)
            self.assertIn(target["disk"], plan["gates"][target["node"]])
            self.assertGreaterEqual(plan["readablePayloadSources"], plan["readQuorum"])
            self.assertEqual(plan["readableMetadataAuthorities"], total)
            self.assertEqual(plan["lock"]["availableClients"], len(snapshot))

    def test_lock_negative_preserves_read_quorum_and_loses_actual_distinct_clients(self):
        _, _, plan = self.scene(16, executor.BRANCHES[1])
        self.assertEqual(plan["offlineNodes"], ["node3", "node4"])
        self.assertEqual(plan["readablePayloadSources"], plan["readQuorum"])
        self.assertEqual(plan["lock"]["availableClients"], plan["lock"]["sharedQuorum"])
        self.assertLess(plan["lock"]["availableClients"], plan["lock"]["exclusiveQuorum"])
        snapshot, target, _ = self.scene(16)
        with self.assertRaisesRegex(ValueError, "offline member|unavailable member"):
            executor.plan_for(snapshot, target, executor.BRANCHES[1], self.contract)

    def test_h1_distributed_loss_is_explicitly_inapplicable_and_retains_both_source_branches(self):
        _, _, plan = self.scene(4, executor.BRANCHES[1])
        self.assertFalse(plan["applicable"])
        self.assertEqual(plan["reason"], executor.LOCAL_LOCK_REASON)
        self.assertEqual(select(executor.CASE_ID)[0]["variants"], list(executor.BRANCHES))
        self.assertIn(executor.CASE_ID, AUTOMATIC_CASES)

    def test_layout_and_source_uncertainty_cannot_be_assumed_sufficient(self):
        snapshot, target, _ = self.scene(16)
        for kind in ("duplicate-member", "other-node", "unrecognized", "different-policy", "insufficient-read"):
            broken, contract = copy.deepcopy(target), dict(self.contract)
            if kind == "duplicate-member": broken["cohort"][-1] = copy.deepcopy(broken["cohort"][0])
            elif kind == "other-node": broken["cohort"][-1]["node"] = "foreign"
            elif kind == "unrecognized": contract["recognized"] = False
            elif kind == "different-policy": contract["writeQuorumRule"] = "assumed"
            else: broken["logical"]["erasure"].update(data_blocks=16, parity_blocks=0)
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                executor.plan_for(snapshot, broken, executor.BRANCHES[0], contract)

    def test_read_only_boundary_can_preserve_reads_when_numerical_r_equals_w(self):
        source = oracle.HealReadQuorumBoundaryTest()
        snapshot, expected = source.scene(total=16, data=12)
        target = shards.map_target(snapshot, source.bucket, expected[0]["key"], "data", 1)
        plan = executor.plan_for(snapshot, target, executor.BRANCHES[0], self.contract)
        self.assertEqual(plan["readQuorum"], plan["writeQuorum"])
        self.assertEqual(plan["readableMetadataAuthorities"], 16)
        self.assertEqual(plan["readablePayloadSources"], 15)
        self.assertEqual(plan["writableDrives"], 11)
        self.assertEqual(sum(map(len, plan["gates"].values())), 5)
        target = {**target, **next(row for row in target["cohort"] if row["node"] == "node4")}
        with self.assertRaisesRegex(ValueError, "preserve R"):
            executor.plan_for(snapshot, target, executor.BRANCHES[1], self.contract)

    def test_fresh_h2_lock_fixture_has_a_source_reviewed_balanced_layout(self):
        from scripts.validation_heal.fleet_node import runtime_config, BACKGROUND_CONFIG
        config = runtime_config("lock-quorum")
        self.assertEqual(config.pop("RUSTFS_STORAGE_CLASS_STANDARD"), "EC:8")
        self.assertEqual(config, BACKGROUND_CONFIG)
        self.assertNotIn("RUSTFS_STORAGE_CLASS_STANDARD", runtime_config("manual"))
        self.assertEqual(executor.STORAGE_REFERENCE["candidateCommit"], executor.LOCK_REFERENCE["candidateCommit"])

    def test_measured_read_and_write_refusals_must_match_every_root(self):
        _, _, plan = self.scene(4)
        observed = {"node1": {"disks": [{"disk": disk, "readableMetadata": True,
            "sameServiceUidAndMountNamespace": True, "writable": disk not in plan["gates"]["node1"],
            "writeErrno": errno.EROFS if disk in plan["gates"]["node1"] else 0} for disk in range(4)]}}
        executor.validate_gate_witness(observed, plan, ["node1"])
        for kind in ("missing", "duplicate", "still-writable", "not-readable", "eacces"):
            broken = copy.deepcopy(observed)
            rows = broken["node1"]["disks"]
            if kind == "missing": rows.pop()
            elif kind == "duplicate": rows[-1] = copy.deepcopy(rows[0])
            elif kind == "still-writable": rows[0].update(writable=True, writeErrno=0)
            elif kind == "not-readable": rows[0]["readableMetadata"] = False
            else: rows[0]["writeErrno"] = errno.EACCES
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                executor.validate_gate_witness(broken, plan, ["node1"])

    def test_unknown_put_is_resolved_without_retries_and_false_acks_are_rejected(self):
        unknown = {"acknowledged": False, "resultUnknown": True, "clientCallInvoked": True}
        self.assertFalse(executor.negative_put(unknown)["retryAllowed"])
        absent = {"acknowledged": False, "resultUnknown": False, "response": {
            "ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": "NoSuchKey"}}}
        body = {"size": 19, "sha256": "a" * 64}
        committed = {"acknowledged": True, "resultUnknown": False, "payload": body}
        self.assertEqual(executor.resolve_put(unknown, [absent, absent], body)["state"], "ABSENT")
        self.assertEqual(executor.resolve_put(unknown, [committed, committed], body)["state"], "COMMITTED")
        ack = {**unknown, "acknowledged": True, "resultUnknown": False}
        with self.assertRaises(ValueError): executor.negative_put(ack)
        with self.assertRaises(ValueError): executor.resolve_put(ack, [absent], body)
        self.assertEqual(executor.resolve_put(ack, [committed], body)["state"], "COMMITTED")
        for reads in ([], [absent, committed], [unknown], [{**committed, "payload": {**body, "size": 1}}]):
            with self.subTest(reads=reads), self.assertRaises(ValueError):
                executor.resolve_put(unknown, reads, body)

    def test_structured_quorum_refusal_cannot_resolve_to_a_committed_put(self):
        observed = {"acknowledged": False, "resultUnknown": False, "clientCallInvoked": True,
            "response": {"ResponseMetadata": {"HTTPStatusCode": 503}, "Error": {"Code": "XMinioWriteQuorum"}}}
        self.assertEqual(executor.negative_put(observed)["state"], "REFUSED")
        for code, http in (("AccessDenied", 403), ("NoSuchBucket", 404), ("InternalError", 200)):
            broken = {**observed, "response": {"ResponseMetadata": {"HTTPStatusCode": http}, "Error": {"Code": code}}}
            with self.assertRaises(ValueError): executor.negative_put(broken)
        with self.assertRaises(ValueError):
            executor.resolve_put(observed, [{"acknowledged": True, "resultUnknown": False, "payload": {}}], {})

    def test_probe_rejects_wrong_bytes_and_other_filesystem_errors(self):
        expected = {"size": 42, "sha256": "a" * 64}
        observed = {"uid": 0, "gid": 0, "read": {"errno": 0, **expected}, "writeErrno": errno.EROFS}
        gate.validate_probe(observed, expected, True)
        gate.validate_probe({**observed, "writeErrno": 0}, expected, False)
        for kind in ("uid", "read", "write", "enospc"):
            broken = copy.deepcopy(observed)
            if kind == "uid": broken["uid"] = 1000
            elif kind == "read": broken["read"]["sha256"] = "b" * 64
            else: broken["writeErrno"] = 0 if kind == "write" else errno.ENOSPC
            with self.subTest(kind=kind), self.assertRaises(ValueError): gate.validate_probe(broken, expected, True)

    def root_owned_stat(self, original):
        def result(path, *args, **kwargs):
            value = list(original(path, *args, **kwargs))
            value[4] = value[5] = 0
            return os.stat_result(value)
        return result

    def test_gate_preserves_host_permissions_and_refuses_inode_drift_and_other_mutations(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp).resolve()
            roots = tuple(state / f"disk-{disk}" for disk in range(4))
            for root in roots: (root / "data").mkdir(parents=True, mode=0o700)
            with patch.object(Path, "lstat", self.root_owned_stat(Path.lstat)):
                gate.apply(state, roots, [0, 2])
                properties = gate.properties(state, roots)
                self.assertIn("--property=PrivateMounts=true", properties)
                self.assertEqual(properties[-1], "--property=ReadOnlyPaths=" + " ".join(str(roots[disk] / "data") for disk in (0, 2)))
                self.assertTrue(all((root / "data").stat().st_mode & 0o777 == 0o700 for root in roots))
                (roots[0] / "data").rename(roots[0] / "saved")
                (roots[0] / "data").mkdir(mode=0o700)
                with self.assertRaisesRegex(ValueError, "drifted"): gate.release(state, roots)
                self.assertTrue((state / gate.MARKER).exists())
                (roots[0] / "data").rmdir()
                (roots[0] / "saved").rename(roots[0] / "data")
                gate.release(state, roots)
                (state / "drive-gate.json").write_text('{}')
                with self.assertRaises(ValueError): gate.apply(state, roots, [0])

    def test_cleanup_failure_retains_namespace_and_remote_script_includes_new_backend(self):
        with tempfile.TemporaryDirectory() as temp:
            fleet = healthy.Fleet(["node1"], Path(temp), "test-run", None)
            fleet.quorum_gates.add("node1")
            calls = []
            def remote(node, action, **kwargs):
                calls.append(action)
                if action == "quorum-gate": raise ValueError("mount identity drift")
                return {}
            fleet.remote = remote
            self.assertFalse(fleet.cleanup())
            self.assertNotIn("cleanup", calls)
            fleet.quorum_gates.clear()
            with patch.object(healthy.subprocess, "run") as execute:
                execute.return_value.returncode = 0
                execute.return_value.stdout = '{}'
                fleet.remote = healthy.Fleet.remote.__get__(fleet)
                for action in ("start", "cleanup", "audit-finalize", "drive-gate", "replace-drive", "quorum-gate", "quorum-gate-probe"):
                    fleet.remote("node1", action)
                    script = execute.call_args.kwargs["input"]
                    self.assertIn("quorum_gate_backend = types.ModuleType", script)
                    compile(script, "remote-quorum", "exec")


if __name__ == "__main__":
    unittest.main()
