import copy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import tempfile
import sys
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import coordinator_crash as executor, replacement_journal, root_restart, mrf_replay
from scripts.validation_heal import healthy, fleet_node
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
import test_validation_heal as fixtures


class CoordinatorCrashTests(unittest.TestCase):
    token = "01234567-89ab-4def-8123-456789abcdef"

    def peers(self):
        return {node: {**{name: name + "-" + node for name in fleet_node.PROCESS_FIELDS},
            "pid": 100 + index, "startTicks": 1000 + index} for index, node in enumerate(("node1", "node2", "node3", "node4"))}

    def test_coordinator_restart_keeps_every_target_process_and_boot(self):
        before, after = self.peers(), self.peers()
        after["node1"].update(pid=200, startTicks=2000)
        self.assertTrue(executor.continuity(before, after, "node1")["everyTargetAndPeerKeptOriginalProcess"])
        for kind in ("target-pid", "target-mount", "owner-env", "owner-boot", "missing", "unchanged"):
            bad = copy.deepcopy(after)
            if kind == "target-pid": bad["node4"]["pid"] += 1
            elif kind == "target-mount": bad["node4"]["mountNamespace"] = "other"
            elif kind == "owner-env": bad["node1"]["environmentFileSha256"] = "other"
            elif kind == "owner-boot": bad["node1"]["bootId"] = "other"
            elif kind == "missing": bad.pop("node2")
            else: bad["node1"] = before["node1"]
            with self.subTest(kind=kind), self.assertRaises(ValueError): executor.continuity(before, bad, "node1", private_mounts=True)
        after["node1"]["mountNamespace"] = "mnt:[new]"
        with self.assertRaises(ValueError): executor.continuity(before, after, "node1")
        executor.continuity(before, after, "node1", private_mounts=True)

    def test_actual_kill_refuses_graceful_exit_platform_event_and_changed_peer(self):
        peers = self.peers()
        proof = {"actualSignal": 9, "signal": "SIGKILL", "graceful": False, "pidfdBound": True, "platformEventIssued": False}
        for kind in ("good", "graceful", "signal", "platform", "pidfd", "peer"):
            calls, changed = [], copy.deepcopy(proof)
            if kind == "graceful": changed["graceful"] = True
            elif kind == "signal": changed["actualSignal"] = 15
            elif kind == "platform": changed["platformEventIssued"] = True
            elif kind == "pidfd": changed["pidfdBound"] = False
            def remote(node, action, **params):
                calls.append((node, action, params))
                if action == "process-crash": return changed
                if action == "process": return {**peers[node], **({"pid": 999} if kind == "peer" else {})}
                return {}
            fleet = types.SimpleNamespace(nodes=list(peers), frozen_processes={"node1": peers["node1"]},
                remote=remote, record=lambda label, value: value)
            with self.subTest(kind=kind):
                if kind == "good":
                    executor.kill(fleet, "node1", peers["node1"], peers)
                    self.assertNotIn("node1", fleet.frozen_processes)
                    self.assertEqual(calls[0][2]["expected"], peers["node1"])
                    self.assertEqual([node for node, action, _ in calls if action == "process"], list(peers)[1:])
                else:
                    with self.assertRaises(ValueError): executor.kill(fleet, "node1", peers["node1"], peers)

    def test_crash_preserves_original_root_intent_without_fabricated_timeout_checkpoint(self):
        before = root_restart.root_intent(fixtures.HealRootRestartOracleTest().records(), self.token, "node1")
        proof = root_restart.crashed_owner(before, copy.deepcopy(before))
        self.assertFalse(proof["gracefulTimeoutCheckpointClaimed"])
        for kind in ("disk", "sha", "timeout", "scope", "token"):
            bad = copy.deepcopy(before)
            if kind == "disk": bad["disk"] += 1
            elif kind == "sha": bad["sha256"] = "other"
            elif kind == "timeout": bad["value"]["options"]["timeout"] = {"secs": 250, "nanos": 0}
            elif kind == "scope": bad["value"]["heal_type"] = {"type": "bucket", "bucket": "other"}
            else: bad["value"]["task_id"] = "another"
            with self.subTest(kind=kind), self.assertRaises(ValueError): root_restart.crashed_owner(before, bad)

    def checkpoint(self):
        endpoint = "http://rustfs-node4:9000/data/rustfs4/owned/data"
        generation = {"taskId": self.token, "generation": self.token, "setDiskId": "pool_0_set_0", "targetSlots": [endpoint]}
        intent = {"schema_version": 7, "task_id": self.token, "replacement_generation": self.token,
            "set_disk_id": generation["setDiskId"], "replacement_targets": [endpoint], "completed": False,
            "replacement_target_identities": [{"endpoint": endpoint, "canonical_path": "/data/rustfs4/owned/data",
                "physical_device_ids": ["uuid:disk"], "filesystem_identity": "filesystem"}]}
        rows = []
        for disk in range(4):
            for kind, (suffix, _) in replacement_journal.FILES.items():
                row = {"disk": disk, "kind": kind, "present": disk == 2 and kind != "proof",
                    "relativePath": f".rustfs.sys/buckets/ahm-replacement/{self.token}_{suffix}"}
                if row["present"]:
                    raw = b"sealed" if kind == "seal" else json.dumps(intent).encode()
                    row.update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
                    row.update({"sealed": True} if kind == "seal" else {"value": copy.deepcopy(intent)})
                rows.append(row)
        return {"taskId": self.token, "readOnly": True, "stable": True, "records": rows}, generation

    def test_replacement_checkpoint_requires_sealed_exact_generation_and_target_identity(self):
        observed, generation = self.checkpoint()
        self.assertEqual(executor.replacement_checkpoint(observed, generation)["sealedRecordDisks"], [2])
        for kind in ("missing", "duplicate", "schema", "generation", "target", "device", "complete", "proof", "path", "unsealed", "hash"):
            bad = copy.deepcopy(observed)
            intent = next(row for row in bad["records"] if row["present"] and row["kind"] == "intent")
            seal = next(row for row in bad["records"] if row["present"] and row["kind"] == "seal")
            if kind == "missing": intent["present"] = False
            elif kind == "duplicate": bad["records"][-1] = copy.deepcopy(bad["records"][0])
            elif kind == "schema": intent["value"]["schema_version"] = True
            elif kind == "generation": intent["value"]["replacement_generation"] = "another"
            elif kind == "target": intent["value"]["replacement_targets"] = ["another"]
            elif kind == "device": intent["value"]["replacement_target_identities"][0]["physical_device_ids"] = []
            elif kind == "complete": intent["value"]["completed"] = True
            elif kind == "proof": bad["records"][-1].update(present=True, size=10, sha256="a" * 64)
            elif kind == "path": intent["relativePath"] = "foreign"
            elif kind == "unsealed": seal["present"] = False
            else: seal["sha256"] = "a" * 64
            with self.subTest(kind=kind), self.assertRaises(ValueError): executor.replacement_checkpoint(bad, generation)

    def test_replacement_journal_reads_only_exact_names_and_rejects_symlink_hardlink_and_oversize(self):
        with tempfile.TemporaryDirectory() as folder:
            roots = [Path(folder).resolve() / str(disk) for disk in range(4)]
            for root in roots: (root / "data").mkdir(parents=True)
            parent = roots[2] / "data/.rustfs.sys/buckets/ahm-replacement"
            parent.mkdir(parents=True)
            path = parent / (self.token + "_ahm_replacement_intent.json")
            path.write_text('{"task_id":"' + self.token + '"}')
            (parent / "unrelated_ahm_replacement_intent.json").write_text("untrusted non-JSON control")
            observed = replacement_journal.read_records(roots, self.token, fleet_node.checked)
            self.assertEqual(sum(row["present"] for row in observed["records"]), 1)
            self.assertTrue(observed["readOnly"])
            raw = path.read_bytes()
            for kind in ("symlink", "hardlink", "oversize"):
                path.unlink()
                if kind == "symlink": path.symlink_to(parent / "unrelated_ahm_replacement_intent.json")
                elif kind == "hardlink": os.link(parent / "unrelated_ahm_replacement_intent.json", path)
                else: path.write_bytes(b"x" * (1024**2 + 1))
                with self.subTest(kind=kind), self.assertRaises((ValueError, OSError)):
                    replacement_journal.read_records(roots, self.token, fleet_node.checked)
            path.unlink(); path.write_bytes(raw)
            with self.assertRaises(ValueError): replacement_journal.read_records(roots, "../other", fleet_node.checked)

    def mrf_scene(self):
        bucket, _, source = fixtures.HealNodeOutageTest().snapshot()
        scene, obligations = {}, []
        for node, value in source.items():
            scene[node] = {"formats": copy.deepcopy(value["formats"]), "files": []}
            for index in range(2):
                key = f"offline/new-{index:02d}.bin"
                for entry in value["files"]:
                    row = copy.deepcopy(entry)
                    row["path"] = row["path"].replace("new.bin", key)
                    row["sha256"] = hashlib.sha256(row["path"].encode() + node.encode() + str(row["disk"]).encode()).hexdigest()
                    if row.get("metadata"):
                        row["metadata"]["key"] = key
                        row["metadata"]["versions"] = [row["metadata"]["versions"][0]]
                        row["metadata"]["versions"][0]["isLatest"] = True
                    scene[node]["files"].append(row)
                if node == "node1": obligations.append({"bucket": bucket, "key": key, "versionId": "new-version",
                    "poolIndex": 0, "setIndex": 0, "kind": 3})
        authorities = copy.deepcopy(scene)
        authorities["node4"]["files"] = []
        scene["node4"]["files"] = [row for row in scene["node4"]["files"] if "new-00" in row["path"]]
        return scene, authorities, bucket, obligations

    def test_mrf_partial_progress_is_not_a_final_hash_or_streaming_proof(self):
        scene, authorities, bucket, obligations = self.mrf_scene()
        proof = executor.mrf_progress(scene, authorities, bucket, obligations, "node4")
        self.assertTrue(proof["partial"])
        self.assertEqual(proof["pendingACKs"], 1)
        self.assertFalse(proof["finalStreamingProofClaimed"])
        empty = copy.deepcopy(scene); empty["node4"]["files"] = []
        self.assertFalse(executor.mrf_progress(empty, authorities, bucket, obligations, "node4")["partial"])
        for kind in ("role", "version", "authority", "part"):
            bad, originals = copy.deepcopy(scene), copy.deepcopy(authorities)
            version = next(row for row in bad["node4"]["files"] if row.get("metadata"))["metadata"]["versions"][0]
            if kind == "role": version["erasure"]["index"] = 1
            elif kind == "version": version["dataDir"] = "other"
            elif kind == "authority": next(row for row in originals["node1"]["files"] if row.get("metadata"))["metadata"]["versions"][0]["size"] += 1
            else: next(row for row in bad["node4"]["files"] if not row.get("metadata"))["sha256"] = "incomplete"
            with self.subTest(kind=kind), self.assertRaises(ValueError): executor.mrf_progress(bad, originals, bucket, obligations, "node4")

    def events(self):
        stamp = "2026-10-11T00:00:00+00:00"
        obligation = {"bucket": "owned", "key": "key", "versionId": "null", "poolIndex": 0, "setIndex": 0, "kind": 3}
        rows = [{"target": "rustfs::heal::manager", "message": "Heal queue request accepted", "source": "mrf",
            "result": "accepted", "request_id": self.token, "timestamp": stamp},
            {"target": "rustfs::heal::task", "message": "Heal object started", "stage": "start", "task_id": self.token,
             "bucket": "owned", "object": "key", "version_id": "None", "timestamp": stamp}]
        bindings = {mrf_replay.identity(obligation): [{"node": "node1"}]}
        return {"node1": rows}, [obligation], bindings, datetime.fromisoformat(stamp)

    def test_mrf_source_requires_exact_ack_and_admission_without_terminal_completion(self):
        events, obligations, bindings, stamp = self.events()
        self.assertEqual(executor.active_mrf_task(events, obligations, bindings, stamp, "node4")[0][:2], ("node1", self.token))
        for kind in ("source", "version", "key", "terminal", "missing-version", "target"):
            bad = copy.deepcopy(events)
            if kind == "source": bad["node1"][0]["source"] = "scanner"
            elif kind == "version": bad["node1"][1]["version_id"] = "foreign"
            elif kind == "key": bad["node1"][1]["object"] = "foreign"
            elif kind == "terminal": bad["node1"].append({"target": "rustfs::heal::manager", "task_id": self.token,
                "state": "task_completed", "timestamp": stamp.isoformat()})
            elif kind == "missing-version": del bad["node1"][1]["version_id"]
            self.assertEqual(executor.active_mrf_task(bad, obligations, bindings, stamp, "node1" if kind == "target" else "node4"), [])

    def test_rust_debug_version_representation_never_merges_distinct_versions(self):
        for raw in ("version", 'Some("version")'): self.assertTrue(mrf_replay.source_version(raw, "version"))
        for raw in ("None", "null", 'Some("other")', "other", 'Some("version") trailing'):
            self.assertFalse(mrf_replay.source_version(raw, "version"))
        for raw in (None, "None", "null", "00000000-0000-0000-0000-000000000000"):
            self.assertTrue(mrf_replay.source_version(raw, "null"))
        self.assertFalse(mrf_replay.source_version('Some("other")', "null"))

    def test_mrf_crash_admission_is_bounded_with_other_sources_disabled(self):
        config = fleet_node.runtime_config("mrf-crash")
        self.assertEqual(config["RUSTFS_HEAL_MRF_REPLAY_BATCH"], "1")
        self.assertEqual(config["RUSTFS_HEAL_MAX_CONCURRENT_HEALS"], "1")
        self.assertEqual(config["RUSTFS_HEAL_MAX_CONCURRENT_PER_SET"], "1")
        self.assertEqual(config["RUSTFS_HEAL_MRF_ENABLE"], "true")
        self.assertEqual(config["RUSTFS_SCANNER_ENABLED"], "false")
        self.assertEqual(config["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertNotIn("RUSTFS_HEAL_MAX_CONCURRENT_HEALS", fleet_node.runtime_config("durable-mrf"))

    def test_remote_checkpoint_module_is_carried_in_the_actual_ssh_program(self):
        sources = []
        def invoke(args, **options):
            sources.append(options["input"])
            compile(sources[-1], "node-source", "exec")
            return types.SimpleNamespace(returncode=0, stdout='{"readOnly":true}', stderr="")
        with tempfile.TemporaryDirectory() as folder, patch.object(healthy.subprocess, "run", side_effect=invoke):
            fleet = healthy.Fleet(["node1"], Path(folder), "gh-12345-1-h2")
            self.assertTrue(fleet.remote("node1", "replacement-records", taskId=self.token)["readOnly"])
        self.assertIn("ahm-replacement/", sources[0])
        self.assertIn("def read_records(roots, token, checked):", sources[0])

    def test_all_three_mechanisms_are_required_and_dirty_cleanup_stops_dispatch(self):
        self.assertEqual(select("F04")[0]["variants"], list(executor.BRANCHES))
        self.assertIn(executor.CASE_ID, AUTOMATIC_CASES)
        for dirty in (False, True):
            with tempfile.TemporaryDirectory() as folder:
                output = Path(folder)
                candidate = output / "candidate.json"
                candidate.write_text(json.dumps({"source_sha": executor.REFERENCE["candidateCommit"]}))
                argv = ["program", "--candidate", str(candidate), "--rc-bin", "rc", "--evidence", str(output),
                    "--tool-directory", "tools", "--case-id", executor.CASE_ID, "--testing-commit", "a" * 40,
                    "--binary-sha", "b" * 64]
                root = {"topology": "H2", "verdict": "FAIL", "cleanup": "FAIL" if dirty else "PASS"}
                mrf = {"topology": "H2", "branch": "offline-put", "verdict": "PASS", "cleanup": "PASS"}
                replacement_result = {"topology": "H2", "branch": executor.BRANCHES[2], "verdict": "PASS", "cleanup": "PASS"}
                with patch.object(sys, "argv", argv), patch.dict(os.environ, {"GITHUB_RUN_ID": "12345"}), \
                     patch.object(executor, "dx_topology", return_value=root) as root_run, \
                     patch.object(executor.mrf_replay, "run_branch", return_value=mrf) as mrf_run, \
                     patch.object(executor, "run_replacement", return_value=replacement_result) as replacement_run:
                    with self.assertRaises(ValueError): executor.main()
                summary = json.loads((output / "healthy-summary.json").read_text())
                self.assertFalse(summary["complete"])
                self.assertEqual(summary["cleanup"], "FAIL" if dirty else "PASS")
                self.assertEqual(len(summary["results"]), 1 if dirty else 3)
                self.assertEqual(root_run.call_args.kwargs["target_nodes"], ["vm001", "vm002", "vm003"])
                self.assertEqual(mrf_run.call_count, 0 if dirty else 1)
                self.assertEqual(replacement_run.call_count, 0 if dirty else 1)


if __name__ == "__main__": unittest.main()
