import copy
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import flapping, fleet_node, drive_gate
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
from scripts.validation_heal.mrf_replay import identity
import test_validation_heal_coordinator_crash as crash_fixtures
import test_validation_heal_mrf as mrf_fixtures


class FlappingTests(unittest.TestCase):
    def scene(self):
        scene, authorities, bucket, obligations = crash_fixtures.CoordinatorCrashTests().mrf_scene()
        for value in (scene, authorities):
            for node in value.values():
                for row in node["files"]:
                    for version in row.get("metadata", {}).get("versions", []):
                        version["versionId"] = "null"
        for item in obligations: item["versionId"] = "null"
        return scene, authorities, bucket, obligations

    def test_node_partial_replay_requires_real_original_members_and_full_ack_roles(self):
        scene, authorities, bucket, obligations = self.scene()
        members = {("node4", disk) for disk in range(4)}
        proof = flapping.replay_progress(scene, authorities, bucket, obligations, members)
        self.assertTrue(proof["partial"])
        self.assertFalse(proof["finalStreamingProofClaimed"])
        for kind in ("role", "version", "authority", "part", "uuid", "member"):
            bad, original = copy.deepcopy(scene), copy.deepcopy(authorities)
            metadata = next(row for row in bad["node4"]["files"] if row.get("metadata"))["metadata"]
            version = metadata["versions"][0]
            selected = members
            if kind == "role": version["erasure"]["index"] = 1
            elif kind == "version": version["versionId"] = "other"
            elif kind == "authority": next(row for row in original["node1"]["files"] if row.get("metadata"))["metadata"]["versions"][0]["size"] += 1
            elif kind == "part": next(row for row in bad["node4"]["files"] if not row.get("metadata"))["sha256"] = "missing"
            elif kind == "uuid": bad["node4"]["formats"][0]["value"]["xl"]["this"] = "foreign"
            else: selected = {("node5", disk) for disk in range(4)}
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                flapping.replay_progress(bad, original, bucket, obligations, selected)

    def test_single_drive_progress_uses_fifteen_surviving_authorities(self):
        scene, authorities, bucket, obligations = self.scene()
        full = copy.deepcopy(scene["node4"]["files"])
        for row in copy.deepcopy(full):
            row["path"] = row["path"].replace("new-00", "new-01")
            if row.get("metadata"): row["metadata"]["key"] = row["metadata"]["key"].replace("new-00", "new-01")
            full.append(row)
        authorities["node4"]["files"] = [row for row in full if row["disk"] != 3]
        scene["node4"]["files"] = [row for row in full if row["disk"] != 3 or "new-00" in row["path"]]
        proof = flapping.replay_progress(scene, authorities, bucket, obligations, {("node4", 3)})
        self.assertTrue(proof["partial"])
        self.assertEqual(proof["pendingACKs"], 1)
        authorities["node4"]["files"] = [row for row in authorities["node4"]["files"]
            if not (row.get("metadata") and "new-01" in row["path"] and row["disk"] == 0)]
        with self.assertRaises(ValueError):
            flapping.replay_progress(scene, authorities, bucket, obligations, {("node4", 3)})

    def test_zero_and_complete_reconstruction_cannot_certify_an_interruption_window(self):
        scene, authorities, bucket, obligations = self.scene()
        members = {("node4", disk) for disk in range(4)}
        empty = copy.deepcopy(scene); empty["node4"]["files"] = []
        self.assertFalse(flapping.replay_progress(empty, authorities, bucket, obligations, members)["partial"])
        for row in copy.deepcopy(scene["node4"]["files"]):
            row["path"] = row["path"].replace("new-00", "new-01")
            if row.get("metadata"): row["metadata"]["key"] = row["metadata"]["key"].replace("new-00", "new-01")
            scene["node4"]["files"].append(row)
        self.assertFalse(flapping.replay_progress(scene, authorities, bucket, obligations, members)["partial"])

    def test_incomplete_ack_cannot_retire_rebind_or_rollback_during_repeated_loss(self):
        fixture = mrf_fixtures.DurableMrfTests()
        record = fixture.observed()
        from scripts.validation_heal.mrf_records import checkpoint
        value, item = checkpoint(record), fixture.obligation()
        lifecycle = value["intents"][0]["lifecycle"]
        owner = {"node": "node1", "checkpointOwner": value["owner"], "sequence": value["sequence"],
            "responsibilityId": lifecycle["responsibility_id"], "bucketIncarnationId": lifecycle["source_bucket_incarnation_id"]}
        wanted = identity(item)
        bindings = {wanted: [owner]}
        self.assertEqual(flapping.retained_pending({"node1": value}, [item], bindings, {wanted})["pendingOriginalResponsibilities"], 1)
        for kind in ("absent", "retired", "owner", "sequence", "responsibility", "incarnation", "accepted", "held"):
            bad = copy.deepcopy(value)
            if kind == "absent": bad["present"] = False
            elif kind == "retired": bad["intents"] = []
            elif kind == "owner": bad["owner"] = "other"
            elif kind == "sequence": bad["sequence"] -= 1
            elif kind == "responsibility": bad["intents"][0]["lifecycle"]["responsibility_id"] = "other"
            elif kind == "incarnation": bad["intents"][0]["lifecycle"]["source_bucket_incarnation_id"] = "other"
            elif kind == "accepted": bad["intents"][0]["lifecycle"]["last_operator_acceptance"] = {"accepted": True}
            else: bad["intents"][0]["lifecycle"]["state"] = {"state": "held"}
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                flapping.retained_pending({"node1": bad}, [item], bindings, {wanted})

    def events(self):
        events, obligations, _, _ = crash_fixtures.CoordinatorCrashTests().events()
        return events, obligations

    def test_task_storm_and_cross_ack_token_reuse_fail_without_counting_duplicates(self):
        events, obligations = self.events()
        proof = flapping.bounded_admissions(events, obligations)
        self.assertEqual(proof["distinctAcceptedTaskIds"], 1)
        events["node1"].append(copy.deepcopy(events["node1"][-1]))
        self.assertEqual(flapping.bounded_admissions(events, obligations)["distinctAcceptedTaskIds"], 1)
        for kind in ("version", "source", "key", "owner", "storm"):
            bad = copy.deepcopy(events)
            if kind == "version": bad["node1"][-1]["version_id"] = 'Some("other")'
            elif kind == "source": bad["node1"][0]["source"] = "scanner"
            elif kind == "key": bad["node1"][-1]["object"] = "foreign"
            elif kind == "owner": bad["node2"] = copy.deepcopy(bad["node1"])
            else:
                pair = bad["node1"][:2]
                for index in range(flapping.PROTOCOL["maxAcceptedTasksPerACKPerOwner"]):
                    admission, start = copy.deepcopy(pair)
                    admission["request_id"] = start["task_id"] = f"token-{index}"
                    bad["node1"].extend((admission, start))
            with self.subTest(kind=kind), self.assertRaises(ValueError): flapping.bounded_admissions(bad, obligations)

    def test_internal_system_mrf_tasks_remain_bounded_and_cannot_discharge_owned_acks(self):
        events, obligations = self.events()
        admission, start = copy.deepcopy(events["node1"][:2])
        admission["request_id"] = start["task_id"] = "system-token"
        start["bucket"], start["object"] = ".rustfs.sys", "scanner/durable-dirty-producer-replay/owned.json"
        events["node1"].extend((admission, start))
        proof = flapping.bounded_admissions(events, obligations)
        self.assertEqual(proof["internalSystemTasks"], 1)
        self.assertFalse(proof["internalSystemTasksCertifyOwnedACKs"])
        for index in range(flapping.PROTOCOL["maxSystemTasksPerOwner"]):
            admission, start = copy.deepcopy((admission, start))
            admission["request_id"] = start["task_id"] = f"system-token-{index}"
            start["object"] = f"scanner/durable-dirty-producer-replay/{index}.json"
            events["node1"].extend((admission, start))
        with self.assertRaises(ValueError): flapping.bounded_admissions(events, obligations)

    def process(self, pid=10):
        return {name: {"pid": pid, "startTicks": pid * 100}.get(name, name) for name in fleet_node.PROCESS_FIELDS}

    def test_drive_rearm_does_not_accept_recycled_process_or_dac_bypass(self):
        before = self.process()
        owner = {"runtimeProfile": "flapping"}
        calls = []
        gate = types.SimpleNamespace(validate_live_boundary=lambda pid: calls.append(("boundary", pid)),
            apply_gate=lambda state, roots, disk, mode: calls.append(("arm", disk, mode)) or {"state": "ARMED"})
        quorum = types.SimpleNamespace(checked=lambda state, roots: None)
        request = {"runId": "owned", "action": "drive-gate-arm-live", "disk": 1, "expected": before}
        with patch.object(fleet_node, "owned", return_value=(Path("/owned"), [Path("/disk")], owner)), \
             patch.object(fleet_node, "process", return_value=before), \
             patch.object(fleet_node, "gate_module", return_value=gate), \
             patch.object(fleet_node, "quorum_gate_module", return_value=quorum):
            self.assertTrue(fleet_node.drive_gate_arm_live(request)["sameOriginalProcess"])
            self.assertEqual(calls, [("boundary", 10), ("arm", 1, "permission-denied")])
            calls.clear()
            with self.assertRaises(ValueError):
                fleet_node.drive_gate_arm_live({**request, "expected": self.process(11)})
            self.assertEqual(calls, [])
            with patch.object(gate, "validate_live_boundary", side_effect=ValueError("DAC bypass")):
                with self.assertRaises(ValueError): fleet_node.drive_gate_arm_live(request)
            self.assertEqual(calls, [])

    def test_three_drive_gate_cycles_preserve_recovery_intent_and_original_inode(self):
        with tempfile.TemporaryDirectory() as folder:
            state, root = Path(folder) / "state", Path(folder) / "root"
            state.mkdir(); root.mkdir(); data = root / "data"; data.mkdir(mode=0o700)
            inode = data.stat().st_ino
            original = Path.lstat
            def rooted(path):
                observed = original(path)
                return types.SimpleNamespace(st_mode=observed.st_mode, st_uid=0, st_nlink=observed.st_nlink,
                    st_size=observed.st_size, st_dev=observed.st_dev, st_ino=observed.st_ino)
            for cycle in range(3):
                # Only ownership is simulated. The actual inode, recovery
                # marker and three permission changes persist on the filesystem.
                with patch.object(Path, "lstat", rooted):
                    drive_gate.apply_gate(state, [root], 0, "permission-denied")
                    drive_gate.activate_gate(state, [root])
                    self.assertEqual(data.stat().st_mode & 0o777, 0)
                    self.assertTrue((state / "drive-gate.json").exists())
                    drive_gate.release_gate(state, [root])
                self.assertEqual(data.stat().st_ino, inode)
                self.assertEqual(data.stat().st_mode & 0o777, 0o700)
                self.assertFalse((state / "drive-gate.json").exists())

    def test_original_node_return_requires_new_birth_without_host_candidate_or_settings_change(self):
        before, after = self.process(10), self.process(11)
        flapping.node_return(before, after)
        for name in ("pid", "bootId", "binarySha256", "environmentFileSha256", "settings"):
            bad = copy.deepcopy(after)
            if name == "pid": bad = copy.deepcopy(before)
            else: bad[name] = "changed"
            with self.subTest(name=name), self.assertRaises(ValueError): flapping.node_return(before, bad)
        with self.assertRaises(ValueError): flapping.same_process(before, after)

    def test_bounded_resources_reject_fd_rss_cpu_queue_and_total_deadline(self):
        resource = {"readOnly": True, "fileDescriptors": 10, "rssBytes": 100, "identity": self.process(),
            "monotonicNs": 2 * 10**9, "clockTicksPerSecond": 100, "cpuTicks": 100, "clockDomain": "bootId"}
        binding = {"endpoints": {"node1": "http://node1"}}
        status = {"httpStatus": 200, "response": {"healActiveTasks": 0, "healQueueLength": 0}}
        for kind in ("good", "fd", "rss", "cpu", "queue", "deadline", "peer"):
            value, result = copy.deepcopy(resource), copy.deepcopy(status)
            if kind == "fd": value["fileDescriptors"] = flapping.PROTOCOL["maxFDs"] + 1
            elif kind == "rss": value["rssBytes"] = flapping.PROTOCOL["maxRssBytes"] + 1
            elif kind == "cpu": value["cpuTicks"] = 100000
            elif kind == "queue": result["response"]["healQueueLength"] = flapping.PROTOCOL["maxQueuedTasks"] + 1
            elif kind == "peer": value["identity"] = self.process(11)
            fleet = types.SimpleNamespace(nodes=["node1", "node4"], record=lambda label, value: value,
                remote=lambda *args, **kwargs: value)
            with patch.object(flapping, "Probe", return_value=types.SimpleNamespace(admin=lambda *args, **kwargs: result)):
                budget = flapping.Budget(fleet, binding)
                previous = copy.deepcopy(resource); previous["monotonicNs"] = 10**9; previous["cpuTicks"] = 90
                budget.previous["node1"] = previous
                if kind == "deadline": budget.deadline = 0
                with self.subTest(kind=kind):
                    if kind == "good": budget.sample(["node1"])
                    else:
                        with self.assertRaises(ValueError): budget.sample(["node1"])

    def test_all_source_branches_are_dispatched_with_isolated_runtime_and_bounded_protocol(self):
        self.assertEqual(select(flapping.CASE_ID)[0]["variants"], list(flapping.BRANCHES))
        self.assertIn(flapping.CASE_ID, AUTOMATIC_CASES)
        config = fleet_node.runtime_config("flapping")
        self.assertEqual(config["RUSTFS_HEAL_MRF_ENABLE"], "true")
        self.assertEqual(config["RUSTFS_HEAL_MAX_CONCURRENT_HEALS"], "1")
        self.assertEqual(config["RUSTFS_HEAL_MRF_REPLAY_BATCH"], "1")
        self.assertEqual(config["RUSTFS_SCANNER_ENABLED"], "false")
        self.assertEqual(config["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertEqual(flapping.PROTOCOL["cycles"], 3)
        self.assertEqual(len(flapping.PROTOCOL["uniqueACKsPerCycle"]), 3)
        self.assertLess(sum(flapping.PROTOCOL["uniqueACKsPerCycle"]), 128)


if __name__ == "__main__":
    unittest.main()
