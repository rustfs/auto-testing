"""C11 original journal replay cannot fabricate or overwrite responsibility."""

import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import ExitStack

from scripts.validation_heal import incarnation as suite, incarnation_replay as backend
from scripts.validation_heal import checkpoint, journal_fault as journal, heal_fault as fault, mrf_records, healthy, fleet_node
import test_validation_heal_checkpoint as checkpoint_fixtures
from test_validation_heal_checkpoint import BUCKET, TOKEN, INCARNATION, encoded
import test_validation_heal_mrf as mrf_fixtures


SUCCESSOR = "52345678-1234-4234-8234-123456789abc"


class OriginalReplayTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state = Path(self.directory.name).resolve() / "state"
        self.state.mkdir()
        self.roots = [self.state.parent / str(disk) for disk in range(4)]
        for root in self.roots:
            (root / "data/.rustfs.sys/buckets").mkdir(parents=True)
            (root / "quarantine").mkdir()
        self.mount = patch.object(fault, "mount_id", return_value=7)
        self.mount.start()
        self.addCleanup(self.mount.stop)
        self.owner = {"runId": "gh-12345-1-h1", "binarySha256": "a" * 64,
                      "bucket": BUCKET, "runtimeProfile": "incarnation-checkpoint"}
        self.request = {"runId": self.owner["runId"], "family": "checkpoint", "taskId": TOKEN,
                        "bucketIncarnationId": INCARNATION}
        self.observed, _, _ = checkpoint_fixtures.AdminCheckpointTests().fixture()
        roots, _ = checkpoint_fixtures.AdminCheckpointTests().root_fixture()
        self.rows = self.observed["node"]["records"] + roots["node"]["records"]
        for row in self.rows:
            if row["present"]:
                (self.roots[row["disk"]] / "data" / row["relativePath"]).write_bytes(base64.b64decode(row["rawBase64"]))
        self.bind_expected()

    def bind_expected(self):
        binding = backend.identity(self.owner, self.request, journal)
        self.request["expectedRecords"] = suite.record_digests(backend.census(self.roots, binding, journal, fault))

    def capture(self):
        return backend.capture(self.state, self.roots, self.owner, self.request, journal, fault, mrf_records, fleet_node.checked)

    def install(self):
        return backend.install(self.state, self.roots, {**self.owner, "incarnationPhase": "transition"},
                               self.request, journal, fault)

    def test_exact_original_transaction_detaches_then_replays_and_exports_current_and_archived_bytes(self):
        archived = self.capture()
        self.assertTrue(archived["originalProductBytes"])
        self.assertFalse(archived["checkpointCursorClaimed"])
        self.assertTrue(all(not row["present"] for row in backend.census(self.roots, archived, journal, fault)))
        self.assertTrue(self.install()["originalArchiveRetained"])
        finalized = backend.finalize(self.state, self.roots, self.owner, self.request, journal, fault)
        self.assertEqual(finalized["currentRecords"], archived["records"])
        self.assertFalse(finalized["productRepairClaimed"])
        self.assertFalse(finalized["canonicalCasePassClaimed"])

    def test_archive_tampering_wrong_generation_and_existing_progress_cannot_be_installed(self):
        self.capture()
        for key, changed in (("bucketIncarnationId", SUCCESSOR), ("runId", "gh-99999-1-h1"), ("taskId", SUCCESSOR)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                backend.install(self.state, self.roots, self.owner, {**self.request, key: changed}, journal, fault)
        path = self.roots[1] / "data" / journal.names("root", TOKEN)[1]
        path.write_bytes(b"product-terminal")
        with self.assertRaisesRegex(ValueError, "overwrite"): self.install()
        self.assertEqual(path.read_bytes(), b"product-terminal")
        path.unlink()
        archive = self.roots[1] / "quarantine" / backend.archive_name(journal.names("checkpoint", TOKEN)[0])
        archive.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "archive changed"): self.install()

    def test_incomplete_capture_installation_and_lost_ack_keep_namespace_dirty(self):
        self.capture()
        with self.assertRaises(FileNotFoundError): backend.finalize(self.state, self.roots, self.owner, self.request, journal, fault)
        fleet = healthy.Fleet(["node"], self.state / "evidence", self.owner["runId"], runtime_profile="incarnation-checkpoint")
        fleet.incarnation_replays = [("node", self.request)]
        fleet.reserved = ["node"]
        def remote(node, action, **params):
            if action == "incarnation-finalize": raise FileNotFoundError("installation incomplete")
            if action == "cleanup": self.fail("dirty original archive must not be deleted")
            return {}
        with patch.object(fleet, "stop"), patch.object(fleet, "remote", side_effect=remote):
            self.assertFalse(fleet.cleanup())
        self.assertEqual(len(fleet.incarnation_replays), 1)
        self.assertTrue((self.state / "C11-replay.json").exists())

    def test_bad_digest_foreign_terminal_and_incomplete_pair_refuse_before_detachment(self):
        original = copy.deepcopy(self.request)
        self.request["expectedRecords"][0]["sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "digests"): self.capture()
        self.request = original
        terminal = self.roots[0] / "data" / journal.names("root", TOKEN)[1]
        terminal.write_bytes(b"terminal")
        self.bind_expected()
        with self.assertRaisesRegex(ValueError, "terminal"): self.capture()
        terminal.unlink()
        resume = self.roots[1] / "data" / journal.names("checkpoint", TOKEN)[1]
        resume.unlink()
        self.bind_expected()
        with self.assertRaisesRegex(ValueError, "incomplete"): self.capture()
        self.assertFalse((self.state / "C11-replay.json").exists())

    def test_symlinks_and_hardlinks_refuse_without_mutating_product_bytes(self):
        path = self.roots[1] / "data" / journal.names("checkpoint", TOKEN)[0]
        raw = path.read_bytes()
        saved = self.state / "outside"
        saved.write_bytes(raw)
        path.unlink()
        path.symlink_to(saved)
        with self.assertRaises(OSError): self.capture()
        path.unlink()
        os.link(saved, path)
        with self.assertRaises(ValueError): self.capture()
        self.assertEqual(saved.read_bytes(), raw)

    def test_real_committed_mrf_archive_keeps_payload_commit_and_lifecycle_on_all_original_replicas(self):
        for root in self.roots:
            for path in (root / "data/.rustfs.sys/buckets").iterdir(): path.unlink()
            for path in (root / "data/.rustfs.sys").glob("*.json"): path.unlink()
        fixture = mrf_fixtures.DurableMrfTests()
        self.owner["runtimeProfile"] = "incarnation-mrf"
        (self.state / "rustfs").write_bytes(b"candidate")
        self.owner["binarySha256"] = hashlib.sha256(b"candidate").hexdigest()
        self.request.update(family="mrf", taskId=None, bucketIncarnationId=fixture.incarnation)
        for row in fixture.observed()["records"]:
            if row["present"]:
                (self.roots[row["disk"]] / "data" / row["relativePath"]).write_bytes(base64.b64decode(row["rawBase64"]))
        for root in self.roots:
            for relative in backend.LEGACY_MRF_PATHS:
                path = root / "data" / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(fixture.journal() if relative.endswith("journal-scoped.bin") else b"")
        self.bind_expected()
        archived = self.capture()
        self.assertEqual(len([row for row in archived["records"] if row["present"]]), 14)
        self.assertTrue(all(not row["present"] for row in backend.census(self.roots, archived, journal, fault)))
        for root in self.roots:
            for relative in backend.LEGACY_MRF_PATHS:
                (root / "data" / relative).write_bytes(b"")
        receipt = self.install()
        self.assertEqual(len(receipt["discardedEmptyTransitionJournals"]), 8)
        self.assertEqual(backend.census(self.roots, archived, journal, fault), archived["records"])
        observed = mrf_records.read_records(self.state, self.roots, self.owner, fleet_node.checked)
        value = mrf_records.checkpoint(observed)
        self.assertEqual(value["intents"][0]["lifecycle"]["responsibility_id"], fixture.responsibility)
        self.assertEqual({row["disk"] for row in value["replicas"]}, {0, 2})

    def test_unbound_or_torn_fallback_refuses_before_archiving_or_detaching(self):
        self.owner["runtimeProfile"] = "incarnation-mrf"
        self.request.update(family="mrf", taskId=None)
        (self.state / "rustfs").write_bytes(b"candidate")
        self.owner["binarySha256"] = hashlib.sha256(b"candidate").hexdigest()
        relative = backend.LEGACY_MRF_PATHS[1]
        path = self.roots[0] / "data" / relative
        path.parent.mkdir(parents=True)
        fixture = mrf_fixtures.DurableMrfTests()
        for raw in (fixture.journal(), fixture.journal()[:-1]):
            path.write_bytes(raw)
            self.bind_expected()
            with self.assertRaises(ValueError): self.capture()
            self.assertEqual(path.read_bytes(), raw)
            self.assertFalse((self.state / "C11-replay.json").exists())

    def test_partial_install_keeps_original_archive(self):
        self.capture()
        original = fault.new_file
        def interrupted(fd, name, raw):
            if name.endswith("ahm_resume_state.json"): raise OSError("installation interrupted")
            original(fd, name, raw)
        with patch.object(fault, "new_file", side_effect=interrupted), self.assertRaises(OSError): self.install()
        self.assertFalse((self.state / "C11-replay-installed.json").exists())
        with self.assertRaisesRegex(ValueError, "overwrite"): self.install()
        self.assertTrue(backend.archived(self.state, self.roots, self.owner, self.request, journal, fault))

    def test_unresolved_s3_mutation_keeps_namespaces_and_logs(self):
        fleet = healthy.Fleet(["node"], self.state / "evidence", self.owner["runId"])
        fleet.reserved = ["node"]
        with patch.object(fleet, "remote", return_value={}) as remote:
            self.assertFalse(fleet.cleanup(preserve_namespaces=True))
        self.assertEqual([call.args[1] for call in remote.call_args_list], ["logs"])


class ReplayOutcomeTests(unittest.TestCase):
    def successor_fixture(self, size):
        expected = [{"key": f"objects/{index // 2:04d}", "versionId": str(__import__('uuid').UUID(int=index + 1)),
            "deleteMarker": index % 2 == 1, "isLatest": index % 2 == 1,
            **({"payload": {"size": 1, "sha256": "a" * 64}} if index % 2 == 0 else {})} for index in range(size)]
        objects = [{"identity": {"bucket": BUCKET, "object": row["key"], "versionId": row["versionId"],
            "bucketIncarnationId": SUCCESSOR, "poolIndex": 0, "setIndex": 0},
            "disposition": {"state": "metadata_healthy" if row["deleteMarker"] else "verified_healthy"}}
            for row in expected[:128]]
        current = {"summary": "finished", "truncated": size > 128, "outcome": {
            "execution": {"state": "completed"}, "coverage": "complete", "objectsTruncated": size > 128,
            "objects": objects, "counters": {"processed": size, "healed": 0, "unchanged": size,
                "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}}}
        return current, BUCKET, expected

    def test_successor_bounded_diagnostics_require_exact_independent_scope_and_typed_counters(self):
        for size in (128, 129, 280):
            self.assertEqual(suite.successor_receipts(*self.successor_fixture(size))[
                "boundedDiagnosticsAreCompleteManifest"], size <= 128)
        for change in ("counter", "truncated", "duplicate", "foreign", "old-generation", "unknown", "repair", "kind"):
            current, bucket, expected = self.successor_fixture(280)
            outcome = current["outcome"]
            if change == "counter": outcome["counters"]["failed"] = False
            elif change == "truncated": outcome["objectsTruncated"] = False
            elif change == "duplicate": outcome["objects"][-1] = copy.deepcopy(outcome["objects"][0])
            elif change == "foreign": outcome["objects"][0]["identity"]["bucket"] = "other"
            elif change == "old-generation": outcome["objects"][0]["identity"]["bucketIncarnationId"] = INCARNATION
            else: outcome["objects"][0]["disposition"]["state"] = {
                "unknown": "unknown", "repair": "repaired", "kind": "metadata_healthy"}[change]
            with self.subTest(change=change), self.assertRaises(ValueError):
                suite.successor_receipts(current, bucket, expected)

    def checkpoint_fixture(self):
        observed, _, _ = checkpoint_fixtures.AdminCheckpointTests().fixture()
        frozen = checkpoint.owned_checkpoint(observed, TOKEN, BUCKET)
        root, _ = checkpoint_fixtures.AdminCheckpointTests().root_fixture()
        frozen["rootAdmission"] = checkpoint.owned_root_admission(root, TOKEN)
        outcome = copy.deepcopy(frozen["value"]["admin"]["outcome"])
        outcome["execution"] = {"state": "aborted", "reason": "untraversable"}
        current = {"summary": "stopped", "settings": checkpoint.OPTIONS, "truncated": False,
                   "detail": "stale_bucket_incarnation: old admission", "outcome": outcome}
        names = journal.names("root", TOKEN)
        terminal = {"schema": 1, "task_id": TOKEN, "heal_type": frozen["rootAdmission"]["value"]["heal_type"],
            "options": frozen["rootAdmission"]["value"]["options"], "status": {"Failed": {"error": current["detail"]}}}
        report = {"schema": 1, "terminal": terminal, "outcome": outcome}
        root["node"]["records"][3] = {"disk": 1, "relativePath": names[0], "present": False}
        root["node"]["records"][4].update(encoded(terminal))
        root["node"]["records"][5].update(encoded(report))
        return current, frozen, root

    def test_checkpoint_rejection_keeps_exact_original_progress_and_durable_owner(self):
        current, frozen, roots = self.checkpoint_fixture()
        self.assertTrue(suite.checkpoint_rejection(current, frozen)["originalProgressRetained"])
        self.assertTrue(suite.checkpoint_terminal(roots, frozen, current)["durableFailureRetained"])
        for change in ("success", "generic-error", "counter", "foreign-object"):
            current, frozen, _ = self.checkpoint_fixture()
            if change == "success": current["summary"] = "finished"
            elif change == "generic-error": current["detail"] = "InternalError"
            elif change == "counter": current["outcome"]["counters"]["healed"] += 1
            else: current["outcome"]["objects"] = [{"identity": {"bucketIncarnationId": SUCCESSOR}}]
            with self.subTest(change=change), self.assertRaises(ValueError): suite.checkpoint_rejection(current, frozen)

    def mrf_fixture(self):
        fixture = mrf_fixtures.DurableMrfTests()
        original = mrf_records.checkpoint(fixture.observed())
        intent = original["intents"][0]
        owner = {"node": "node", "checkpointOwner": original["owner"], "sequence": original["sequence"],
            "responsibilityId": fixture.responsibility, "bucketIncarnationId": fixture.incarnation,
            "intentDigest": intent["intentDigest"]}
        value = copy.deepcopy(original)
        value["owner"] = SUCCESSOR
        value["sequence"] += 1
        value["intents"][0]["lifecycle"]["state"] = {"state": "bucket_incarnation_changed",
            "source_bucket_incarnation_id": fixture.incarnation, "observed_bucket_incarnation_id": SUCCESSOR, "detected_at_ms": 123}
        obligation = fixture.obligation()
        return {"node": value}, [obligation], {suite.mrf.identity(obligation): [owner]}, SUCCESSOR

    def test_old_mrf_requires_durable_held_original_responsibility_without_operator_acceptance(self):
        proof = suite.retained_mrf(*self.mrf_fixture())
        self.assertFalse(proof["oldResponsibilityRetired"])
        self.assertNotEqual(proof["retainedResponsibilities"][0]["originalCheckpointOwner"],
                            proof["retainedResponsibilities"][0]["currentCheckpointOwner"])
        for change in ("missing", "active", "same-sequence", "same-owner", "nil-owner", "successor-source", "accepted", "wrong-id", "duplicate"):
            value, obligations, bindings, incarnation = self.mrf_fixture()
            row = value["node"]["intents"][0]
            if change == "missing": value["node"]["intents"] = []
            elif change == "active": row["lifecycle"]["state"] = {"state": "active"}
            elif change == "same-sequence": value["node"]["sequence"] -= 1
            elif change == "same-owner": value["node"]["owner"] = bindings[suite.mrf.identity(obligations[0])][0]["checkpointOwner"]
            elif change == "nil-owner": value["node"]["owner"] = str(__import__('uuid').UUID(int=0))
            elif change == "successor-source": row["lifecycle"]["source_bucket_incarnation_id"] = SUCCESSOR
            elif change == "accepted": row["lifecycle"]["last_operator_acceptance"] = {"accepted": True}
            elif change == "wrong-id": row["lifecycle"]["responsibility_id"] = SUCCESSOR
            else: value["node"]["intents"].append(copy.deepcopy(row))
            with self.subTest(change=change), self.assertRaises(ValueError):
                suite.retained_mrf(value, obligations, bindings, incarnation)

    def test_old_replay_cannot_change_any_successor_physical_bytes(self):
        before = {"node": {"files": [{"sha256": "a" * 64}], "formats": []}}
        after = copy.deepcopy(before)
        after["node"]["files"][0]["sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "persisted work changed"): suite.replay_isolation(before, after)


class RuntimePhaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state = Path(self.directory.name).resolve()
        self.owner = {"runId": "gh-12345-1-h1", "unit": "owned-unit", "binarySha256": "a" * 64,
                      "runtimeProfile": "incarnation-mrf", "bucket": BUCKET}
        self.roots = [self.state / str(disk) for disk in range(4)]
        for root in self.roots: (root / "data").mkdir(parents=True)
        self.binding = backend.identity(self.owner, {"runId": self.owner["runId"], "family": "mrf",
            "taskId": None, "bucketIncarnationId": INCARNATION}, journal)
        self.manifest = {**self.binding, "records": [{"disk": disk, "relativePath": relative, "present": False}
            for disk in range(4) for relative in backend.paths(self.binding, journal)],
            "originalProductBytes": True, "replacementBytesAccepted": False}
        (self.state / "C11-replay.json").write_bytes(fault.encode(self.manifest))
        self.env = self.state / "service.env"
        self.env.write_text("\n".join(f"{key}={value}" for key, value in fleet_node.runtime_settings(self.owner).items()) + "\n")
        original_stat = Path.stat
        def root_stat(path, *args, **kwargs):
            info = original_stat(path, *args, **kwargs)
            if path != self.env: return info
            values = list(info); values[4] = 0
            return os.stat_result(values)
        self.patches = [patch.object(Path, "stat", root_stat),
            patch.object(fleet_node, "owned", side_effect=lambda _: (self.state, self.roots, self.owner.copy())),
            patch.object(fleet_node, "stopped"), patch.object(fleet_node, "digest", return_value="a" * 64),
            patch.dict(fleet_node.__dict__, {"incarnation_replay_backend": backend}),
            patch.dict(fleet_node.__dict__, {"journal_fault_backend": journal}),
            patch.object(fault, "mount_id", return_value=7)]
        for item in self.patches:
            item.start(); self.addCleanup(item.stop)

    def phase(self, value):
        return fleet_node.incarnation_operation({"runId": self.owner["runId"], "action": "incarnation-phase", "phase": value})

    def test_only_complete_capture_transition_replay_changes_mrf_and_keeps_one_worker(self):
        with self.assertRaisesRegex(ValueError, "complete"): self.phase("transition")
        (self.state / "C11-replay-detached.json").write_bytes(fault.encode(self.binding))
        receipt = self.phase("transition")
        self.assertEqual((receipt["workerLimit"], receipt["mrfEnabled"]), ("1", "false"))
        self.owner["incarnationPhase"] = "transition"
        with self.assertRaisesRegex(ValueError, "refund"): self.phase("transition")
        with self.assertRaisesRegex(ValueError, "complete"): self.phase("replay")
        (self.state / "C11-replay-installed.json").write_bytes(fault.encode(self.binding))
        self.assertEqual(self.phase("replay")["mrfEnabled"], "true")
        self.owner["incarnationPhase"] = "replay"
        fleet_node.incarnation_configuration(self.state, self.owner)
        with self.assertRaisesRegex(ValueError, "refund"): self.phase("transition")

    def test_configuration_commit_interruption_refuses_process_start_before_systemd(self):
        (self.state / "C11-replay-detached.json").write_bytes(fault.encode(self.binding))
        original = fault.new_file
        def interrupted(fd, name, value):
            if name == "C11-phase.new": raise OSError("phase acknowledgement interrupted")
            original(fd, name, value)
        with patch.object(fault, "new_file", side_effect=interrupted), self.assertRaises(OSError): self.phase("transition")
        self.assertFalse((self.state / "C11-phase.json").exists())
        with patch.object(fleet_node, "command") as command, self.assertRaisesRegex(ValueError, "incomplete"):
            fleet_node.start({"runId": self.owner["runId"]})
        self.assertFalse(any(call.args[0] == "systemd-run" for call in command.call_args_list))

    def test_substituted_markers_and_archive_refuse_before_changing_runtime(self):
        original = self.env.read_bytes()
        for change in ("runId", "binarySha256", "bucket", "family", "taskId", "bucketIncarnationId", "extra"):
            bad = {**self.binding, change: "wrong"}
            (self.state / "C11-replay-detached.json").write_bytes(fault.encode(bad))
            with self.subTest(change=change), self.assertRaises(ValueError): self.phase("transition")
            self.assertEqual(self.env.read_bytes(), original)
        (self.state / "C11-replay-detached.json").write_bytes(fault.encode(self.binding))
        (self.state / "C11-replay.json").write_bytes(fault.encode({**self.manifest, "bucketIncarnationId": SUCCESSOR}))
        with self.assertRaises(ValueError): self.phase("transition")
        self.assertEqual(self.env.read_bytes(), original)

    def test_duplicate_config_and_caller_runtime_override_refuse(self):
        with self.env.open("a") as stream: stream.write("RUSTFS_HEAL_MRF_ENABLE=true\n")
        with self.assertRaises(ValueError): fleet_node.incarnation_configuration(self.state, self.owner)
        with self.assertRaisesRegex(ValueError, "arbitrary"):
            fleet_node.incarnation_operation({"runId": self.owner["runId"], "action": "incarnation-phase",
                "phase": "transition", "environment": {"RUSTFS_HEAL_MRF_ENABLE": "false"}})

    def test_remote_record_operations_embed_all_real_backends_before_execution(self):
        fleet = healthy.Fleet(["node"], self.state / "evidence", self.owner["runId"], runtime_profile="incarnation-mrf")
        for action in ("incarnation-census", "incarnation-capture", "incarnation-install", "incarnation-finalize", "incarnation-phase"):
            with self.subTest(action=action), patch.object(healthy.subprocess, "run",
                return_value=SimpleNamespace(returncode=0, stdout="{}", stderr="")) as remote:
                fleet.remote("node", action)
                source = remote.call_args.kwargs["input"].rsplit("\nmain(", 1)[0]
                namespace = {"__name__": "fleet-test"}
                exec(compile(source, "remote-fleet", "exec"), namespace)
                for name in ("mrf", "fault", "journal_fault_backend", "incarnation_replay_backend"):
                    self.assertIn(name, namespace)
                self.assertTrue(callable(namespace["incarnation_replay_backend"].capture))


class ReplayOrderingTests(unittest.TestCase):
    def test_capture_and_install_lost_ack_are_registered_before_mutation_and_keep_incomplete_namespaces(self):
        for failing_action in ("incarnation-capture", "incarnation-install"):
            with self.subTest(action=failing_action), tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
                directory = Path(temp)
                rc = directory / "rc"; rc.write_bytes(b"client")
                args = SimpleNamespace(evidence=directory, rc_bin=rc, tool_directory=directory,
                    testing_commit="a" * 40, binary_sha="b" * 64)
                events, fleets = [], []
                old = [{"key": "old", "versionId": TOKEN, "deleteMarker": False, "payload": {"size": 1}},
                       {"key": "old", "versionId": SUCCESSOR, "deleteMarker": True}]
                obligation = {"bucket": BUCKET, "key": "old", "versionId": TOKEN, "kind": 3, "poolIndex": 0, "setIndex": 0}
                binding = {suite.mrf.identity(obligation): [{"bucketIncarnationId": INCARNATION}]}
                class FakeFleet(healthy.Fleet):
                    def __init__(self, *args, **kwargs):
                        super().__init__(*args, **kwargs); fleets.append(self)
                    def deploy(self, *args): self.reserved = list(self.nodes)
                    def snapshot(self, *args, **kwargs): return {}
                    def stop(self): events.append("stop")
                    def start(self): events.append("start")
                    def remote(self, node, action, **params):
                        events.append((node, action, params))
                        if action == failing_action:
                            assert (node, {key: params[key] for key in ("family", "taskId", "bucketIncarnationId")}) in self.incarnation_replays
                            raise OSError("SSH acknowledgement lost after mutation")
                        if action == "incarnation-finalize": raise ValueError("original transaction incomplete")
                        if action == "incarnation-capture": return {"detached": True, "originalProductBytes": True, "replacementBytesAccepted": False}
                        if action == "incarnation-census": return {"records": []}
                        if action == "drive-gate-probe": return {"readProbe": {"errno": 13}, "errno": 13, "dacBypassRemoved": True}
                        return {}
                class FakeProbe:
                    def __init__(self, *args): pass
                    def inventory(self, *args): return {}
                class FakeRecorder:
                    def __init__(self, probe, *args): self.probe = probe
                def deletion(*args): events.append("delete-old-and-recreate"); return {}
                for name, value in {"Fleet": FakeFleet, "Probe": FakeProbe, "Recorder": FakeRecorder,
                    "verify_inspector": lambda *args: {}, "verify_quorum_contract": lambda *args: None,
                    "verify_mrf_reference": lambda *args: {}, "validate_topology": lambda *args: {},
                    "seed": lambda *args: copy.deepcopy(old), "verify_seed": lambda *args: {},
                    "run_task": lambda *args, **kwargs: {}, "validate_version_receipts": lambda *args: {"bucketIncarnationId": INCARNATION},
                    "idle_fleet": lambda *args: None, "map_target": lambda *args, **kwargs: {"cohort": [{"node": "node2", "disk": 0}]},
                    "delete_old_bucket": deletion, "seed_successor": lambda *args: copy.deepcopy(old),
                    "config": lambda *args: {}, "successor_receipts": lambda *args: {"bucketIncarnationId": SUCCESSOR},
                    "successor_physical": lambda *args: []}.items():
                    stack.enter_context(patch.object(suite, name, value))
                stack.enter_context(patch.dict(os.environ, {"GITHUB_RUN_ID": "12345"}))
                stack.enter_context(patch.object(suite.mrf, "acknowledge", return_value=(old, [obligation], {})))
                stack.enter_context(patch.object(suite.mrf, "read_checkpoints", return_value={}))
                stack.enter_context(patch.object(suite.mrf, "pending_bindings", return_value=binding))
                result = suite.run_replay(args, "H2", ["node1", "node2"], "old-mrf-replay", {"source_sha": "c" * 40, "package_sha256": "d" * 64})
                self.assertEqual((result["verdict"], result["cleanup"]), ("FAIL", "FAIL"))
                self.assertTrue(fleets[0].incarnation_replays, result.get("failure"))
                self.assertFalse(any(isinstance(event, tuple) and event[1] == "cleanup" for event in events))
                if failing_action == "incarnation-capture": self.assertNotIn("delete-old-and-recreate", events)
                else:
                    captures = [i for i, event in enumerate(events) if isinstance(event, tuple) and event[1] == "incarnation-capture"]
                    released = next(i for i, event in enumerate(events) if isinstance(event, tuple) and event[1] == "drive-gate" and event[2]["mode"] == "clear")
                    self.assertLess(max(captures), released)
                    self.assertLess(released, events.index("delete-old-and-recreate"))
                    self.assertEqual(events[released - 1][1], "incarnation-phase")
                    self.assertEqual(events[released + 1], "start")


if __name__ == "__main__": unittest.main()
