"""Cancellation recovery rejects drift and retains every restored fixture byte."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import interrupted_recovery as interrupted, retained_scene as retained
from scripts.validation_heal import recover_environment as recovery
from scripts.validation_heal.healthy import physical_content
import test_validation_heal_retained_scene as scene_tests


class InterruptedPlanTests(unittest.TestCase):
    def fixture(self, root):
        candidate = {"source_sha": "a" * 40, "package_sha256": "b" * 64}
        artifact = root / "artifact"; case = artifact / "run" / interrupted.CASE
        evidence = case / "evidence" / interrupted.CASE
        result = case / "cases" / (interrupted.CASE + ".json")
        result.parent.mkdir(parents=True)
        result.write_text(json.dumps({"case_id": interrupted.CASE, "verdict": "RUNNING", "cleanup_status": "PENDING"}))
        (artifact / "candidate.json").write_text(json.dumps(candidate))
        for fixture in ("DB", "DV"):
            for topology in ("H1", "H2"):
                for branch in interrupted.BRANCHES:
                    if fixture == "DV" and topology == "H2" and branch in {"stale-generation", "journal-write-failure"}: continue
                    folder = evidence / fixture / topology / branch; folder.mkdir(parents=True)
                    (folder / "00001-checkpoint-branch-result.json").write_text(json.dumps({
                        "fixture": fixture, "topology": topology, "branch": branch, "verdict": "FAIL", "cleanup": "PASS"}))
                    nodes = ["vm000"] if topology == "H1" else [f"vm00{i}" for i in range(4)]
                    for i, node in enumerate(nodes):
                        (folder / f"{i+2:05d}-cleanup.json").write_text(json.dumps({"node": node, "cleanup": "PASS",
                            "unitStopped": True, "rootsRemoved": True}))
                    (folder / "00009-recovery.json").write_text(json.dumps({"cleanup": "PASS", "errors": []}))
        folder = evidence / "DV/H2/stale-generation"; folder.mkdir(parents=True)
        run_id, bucket = "gh-12345-1-h2", "rv-heal-0123456789abcdef"
        binding = {"runId": run_id, "bucketPrefix": bucket, "candidateCommit": candidate["source_sha"],
                   "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": "c" * 64,
                   "harnessCommit": "d" * 40}
        count = 0
        def emit(label, value):
            nonlocal count
            count += 1; (folder / f"{count:05d}-{label}.json").write_text(json.dumps(value))
        emit("binding", binding)
        for label in ("candidate-bound-inspector", "candidate-terminal-options-contract", "preflight", "integrity-fleet-attestation"):
            emit(label, {})
        nodes = [f"vm00{i}" for i in range(4)]
        owners = {n: {"runId": run_id, "bucket": bucket, "binarySha256": "c" * 64, "runtimeProfile": "root-restart",
            "unit": "rustfs-validation-heal-" + run_id + ".service", "mounts": [{}] * 4} for n in nodes}
        for node in nodes:
            emit("reserved", {"node": node, "reserved": True, "owner": owners[node]}); emit("inspector-upload", {})
        started = {n: {"unit": owners[n]["unit"], "started": True} for n in nodes}
        emit("started", started)
        for label in ("deployed-topology", "topology-proof", "ACK-bound-complete-S3", "complete-ACK-manifest"):
            emit(label, {})
        for node in nodes: emit("task-exit-background", {})
        emit("quiescent", {})
        before = {n: {"quiescent": True, "pid": 0, "binarySha256": "c" * 64, "formats": [{}] * 4, "files": []} for n in nodes}
        for i in range(4):
            before["vm000"]["files"].append({"disk": 0,
                "path": f"{bucket}/objects/{i:04d}.bin/00000000-0000-0000-0000-00000000000{i}/part.1",
                "size": 17, "sha256": str(i) * 64})
        emit("healthy-physical-baseline", before)
        current = copy.deepcopy(before)
        for i in range(4):
            for n in range(16): emit("baseline-full-cohort-bitrot", {})
            entry = current["vm000"]["files"].pop(0)
            emit("missing-original-shard", {"schemaVersion": 1, "runId": run_id, "faultId": f"F07-shard-{i}",
                "relativePath": entry["path"], "kind": "missing", "fault": None, "offset": None,
                "before": {k: entry[k] for k in ("size", "sha256")}, "backup": f"F07-shard-{i}.original",
                "backend": "logical-file-fault", "requiresQuiescentService": True})
            emit("actual-missing-shard", current)
        emit("started", started)
        fixture = folder / "fixture"; fixture.mkdir()
        for seq in range(1, 283):
            operation = "create_bucket" if seq == 1 else "put_bucket_versioning" if seq == 2 else "put_object" if seq <= 242 else "delete_object"
            params = {"Bucket": bucket}
            if seq > 2: params["Key"] = "objects/0001.bin"
            request = {"runId": run_id, "sequence": seq, "operation": operation, "params": params}
            request_path = fixture / f"{seq:06d}-request.json"; request_path.write_text(json.dumps(request))
            ack = {"runId": run_id, "sequence": seq, "requestFile": request_path.name,
                "requestSha256": hashlib.sha256(request_path.read_bytes()).hexdigest(), "operationCheck": "PASS",
                "observation": {"operation": operation, "params": params, "acknowledged": True,
                    "clientCallInvoked": True, "resultUnknown": False}}
            (fixture / f"{seq:06d}-ack.json").write_text(json.dumps(ack))
        return candidate, folder, before

    def test_exact_boundary_and_rescue_retry_only_allow_original_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); candidate, folder, before = self.fixture(root)
            plan = recovery.recovery_plan(root, 12345, candidate)[0]
            self.assertEqual(len(plan["faults"]), 4)
            self.assertEqual({f["node"] for f in plan["faults"]}, {"vm000"})
            partial = copy.deepcopy(plan["failure"])
            partial["vm000"]["files"] = before["vm000"]["files"][:2]
            self.assertTrue(recovery.recovery_scene_matches(partial, plan))
            partial["vm000"]["files"][0]["sha256"] = "f" * 64
            self.assertFalse(recovery.recovery_scene_matches(partial, plan))

    def test_boundary_mutations_fail_closed(self):
        for mutation in ("candidate", "pending", "prior-cleanup", "task", "unknown-dir", "unknown-fixture", "unknown-ack",
                         "receipt", "other-content", "format", "missing-root", "later-branch", "extra-case", "wrong-harness", "unfinished-extra-branch"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); candidate, folder, before = self.fixture(root)
                def edit(path, function):
                    value = json.loads(path.read_text()); function(value); path.write_text(json.dumps(value))
                if mutation == "candidate": candidate["package_sha256"] = "e" * 64
                elif mutation == "pending": edit(root / "artifact/run" / interrupted.CASE / "cases" / (interrupted.CASE + ".json"), lambda x: x.update(cleanup_status="FAIL"))
                elif mutation == "prior-cleanup": edit(next((folder.parents[1] / "H1/checkpoint-truncate").glob("*-cleanup.json")), lambda x: x.update(unitStopped=False))
                elif mutation == "task": (folder / "00098-trigger.json").write_text("{}")
                elif mutation == "unknown-dir": (folder / "admin").mkdir()
                elif mutation == "unknown-fixture": (folder / "fixture/unknown.json").write_text("{}")
                elif mutation == "unknown-ack": edit(folder / "fixture/000010-ack.json", lambda x: x["observation"].update(resultUnknown=True))
                elif mutation == "receipt": edit(next(folder.glob("*-missing-original-shard.json")), lambda x: x.update(relativePath=".rustfs.sys/config/iam"))
                elif mutation == "other-content": edit(folder / "00042-actual-missing-shard.json", lambda x: x["vm000"]["files"][0].update(size=19))
                elif mutation == "format": edit(folder / "00042-actual-missing-shard.json", lambda x: x["vm001"].update(formats=[{"foreign": True}] * 4))
                elif mutation == "missing-root": edit(folder / "00024-healthy-physical-baseline.json", lambda x: x.pop("vm003"))
                elif mutation == "later-branch": (folder.parent / "journal-write-failure").mkdir()
                elif mutation == "extra-case":
                    extra = root / "artifact/run/other/cases/other.json"; extra.parent.mkdir(parents=True); extra.write_text("{}")
                elif mutation == "unfinished-extra-branch": (folder.parents[2] / "DX/H2/other").mkdir(parents=True)
                elif mutation == "wrong-harness": edit(folder / "00001-binding.json", lambda x: x.update(harnessCommit="invalid"))
                with self.assertRaises((ValueError, KeyError)):
                    recovery.recovery_plan(root, 12345, candidate)


class RestoredArchiveTests(unittest.TestCase):
    def test_original_scene_bound_before_first_rename_and_each_partial_move_is_retryable(self):
        helper = scene_tests.RetainedSceneTests()
        for lost_after in (1, 2, 3, 4, 5):
            with self.subTest(lost_after=lost_after), tempfile.TemporaryDirectory() as directory:
                state, roots, node, request = helper.scene(directory)
                roots[0].mkdir(parents=True)
                owner = request["expectedOwner"]; owner["runtimeProfile"] = "root-restart"
                for folder in (*roots, state): (folder / "owner.json").write_text(json.dumps(owner))
                snapshot = {"files": [{"path": "original", "sha256": "a" * 64}], "formats": [{}] * 4,
                    "pid": 0, "quiescent": True, "binarySha256": owner["binarySha256"]}
                node["snapshot"] = unittest.mock.Mock(return_value=snapshot)
                request["expectedOriginalSceneSha256"] = interrupted.digest({k: snapshot[k] for k in ("files", "formats")})
                with helper.root_stats(), patch.object(retained, "rename_new", side_effect=helper.local_rename):
                    baseline = retained.run(request, node)["baseline"]
                    change = {**request, "action": "retained-scene-isolate", "expectedBaseline": baseline}
                    count = 0
                    def interrupted_move(source, target):
                        nonlocal count
                        helper.local_rename(source, target)
                        if target.is_dir():
                            count += 1
                            if count == lost_after: raise RuntimeError("lost ACK after rename")
                    with patch.object(retained, "rename_new", side_effect=interrupted_move), self.assertRaises(RuntimeError):
                        retained.run(change, node)
                    node["snapshot"].side_effect = AssertionError("must use durable intent after partial archive")
                    result = retained.run(change, node)
                    self.assertEqual(result["locations"], ["retained"] * 5)
                    self.assertEqual(result["baseline"], baseline)
                    self.assertFalse(result["canonicalPassClaimed"])
                    with self.assertRaisesRegex(ValueError, "durable isolation intent"):
                        retained.run({**request, "expectedOriginalSceneSha256": "b" * 64}, node)

    def test_unrestored_or_missing_scene_cannot_be_archived(self):
        helper = scene_tests.RetainedSceneTests()
        for mutation in ("digest", "missing-root", "live-pid", "unknown-pid"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                state, roots, node, request = helper.scene(directory)
                if mutation != "missing-root": roots[0].mkdir(parents=True)
                request["expectedOwner"]["runtimeProfile"] = "root-restart"
                for folder in (*roots, state):
                    if folder.exists(): (folder / "owner.json").write_text(json.dumps(request["expectedOwner"]))
                snapshot = {"files": [], "formats": [], "pid": 0, "quiescent": True,
                            "binarySha256": request["expectedOwner"]["binarySha256"]}
                request["expectedOriginalSceneSha256"] = interrupted.digest({"files": [], "formats": []})
                if mutation == "digest": request["expectedOriginalSceneSha256"] = "b" * 64
                elif mutation == "live-pid": snapshot["pid"] = 7
                elif mutation == "unknown-pid": snapshot["pid"] = False
                node["snapshot"] = lambda request: snapshot
                with helper.root_stats(), patch.object(retained, "rename_new") as move, self.assertRaises(ValueError):
                    retained.run(request, node)
                move.assert_not_called()


class InterruptedFleetTests(unittest.TestCase):
    def test_all_stops_precede_restore_and_all_audits_precede_moves(self):
        for variant in ("fresh", "retry-restored", "retry-partial-archive", "drift", "lost-root", "archive-drift"):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); candidate, folder, before = InterruptedPlanTests().fixture(root)
                plan = recovery.recovery_plan(root, 12345, candidate)[0]
                states = [{"node": n, "state": {"exists": True}, "roots": [{"exists": True}] * 4,
                           "retained": {"state": False, "roots": [False] * 4}} for n in plan["nodes"]]
                current = copy.deepcopy(before if variant == "retry-restored" else plan["failure"])
                if variant == "retry-partial-archive":
                    states[0]["retained"]["roots"][0] = True
                elif variant == "drift": current["vm001"]["formats"] = [{"foreign": True}] * 4
                elif variant == "lost-root": states[2]["roots"] = [{"exists": False}] * 4
                events = []
                def remote(node, action, **params):
                    events.append((node, action))
                    if action == "stop": return {"quiescent": True}
                    if action == "snapshot": return copy.deepcopy(current[node])
                    if action == "recover-fault":
                        fault = next(f for f in plan["faults"] if f["faultId"] == params["faultId"])
                        original = next(entry for entry in before[node]["files"] if entry["path"] == fault["receipt"]["relativePath"])
                        if original not in current[node]["files"]: current[node]["files"].append(copy.deepcopy(original))
                        current[node]["files"].sort(key=lambda row: (row["disk"], row["path"]))
                        return {"runId": plan["runId"], "faultId": params["faultId"], "physicalRepairPassAllowed": False}
                    if action == "retained-scene-audit" and variant == "archive-drift" and node == "vm003":
                        raise ValueError("archive drift")
                    baseline = {"originalSceneSha256": params["expectedOriginalSceneSha256"], "node": node}
                    return {"baseline": baseline, "inspectionOnly": action == "retained-scene-audit", "mutated": False,
                            "activeNamespaceIsolated": True, "canonicalPassClaimed": False}
                fleet = types.SimpleNamespace(remote=remote, record=lambda label, value: value)
                if variant in {"drift", "lost-root", "archive-drift"}:
                    with self.assertRaises(ValueError):
                        interrupted.restore_and_isolate(fleet, plan, states, recovery.recovery_scene_matches)
                    self.assertFalse(any(action == "retained-scene-isolate" for _, action in events))
                    if variant != "archive-drift": self.assertFalse(any(action == "recover-fault" for _, action in events))
                    continue
                result = interrupted.restore_and_isolate(fleet, plan, states, recovery.recovery_scene_matches)
                self.assertEqual(result["cleanup"], "ISOLATED")
                self.assertTrue(result["originalBusinessFixtureRestored"])
                self.assertFalse(result["originalSceneRestored"])
                first_move = next(i for i, (_, action) in enumerate(events) if action == "retained-scene-isolate")
                self.assertEqual(sum(action == "retained-scene-audit" for _, action in events[:first_move]), 4)
                if variant == "retry-partial-archive":
                    self.assertFalse(any(n == "vm000" and a in {"stop", "snapshot", "recover-fault"} for n, a in events))
                else:
                    first_rescue = next(i for i, (_, action) in enumerate(events) if action == "recover-fault")
                    self.assertEqual(sum(action == "stop" for _, action in events[:first_rescue]), 4)
                    self.assertEqual(physical_content(current), physical_content(before))
                self.assertFalse(any(a in {"start", "cleanup", "journal-recover"} for _, a in events))


class InterruptedAdapterTests(unittest.TestCase):
    def test_retained_remote_has_the_real_snapshot_encryption_dependency(self):
        from scripts.validation_heal.healthy import Fleet
        def execute(*args, **kwargs):
            source = kwargs["input"].rsplit("\nmain(", 1)[0]
            scope = {"__name__": "__node_test__"}
            exec(compile(source, "restored-archive-node", "exec"), scope)
            module = scope["encryption_module"]()
            with tempfile.TemporaryDirectory() as state:
                self.assertIsNone(module.attest(Path(state).resolve(), {"runtimeProfile": "root-restart"}, scope["checked"], None))
            self.assertTrue(callable(scope["snapshot"]))
            return types.SimpleNamespace(returncode=0, stdout="{}", stderr="")
        with tempfile.TemporaryDirectory() as directory, patch("scripts.validation_heal.healthy.subprocess.run", side_effect=execute):
            Fleet(["vm000"], Path(directory), "gh-12345-1-h2").remote("vm000", "retained-scene-audit", expectedOwner={})


class ArchivedFaultBindingTests(unittest.TestCase):
    def test_mismatched_intent_cannot_write_backup_and_exact_intent_is_retryable(self):
        from scripts.validation_heal import heal_fault
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); (root / "quarantine").mkdir()
            bucket = "rv-heal-0123456789abcdef"
            relative = bucket + "/objects/0001.bin/version/part.1"
            target = root / "data" / relative; target.parent.mkdir(parents=True)
            original = b"original-acknowledged-shard"
            receipt = {"runId": "gh-12345-1-h2", "faultId": "F07-shard-0", "backup": "F07-shard-0.original",
                "relativePath": relative, "kind": "missing", "fault": None,
                "before": {"size": len(original), "sha256": hashlib.sha256(original).hexdigest()}}
            (root / "quarantine/F07-shard-0.original").write_bytes(original)
            (root / "quarantine/F07-shard-0.intent.json").write_bytes(heal_fault.encode(receipt))
            owner = {"runId": receipt["runId"], "bucketPrefix": bucket}
            with patch.object(heal_fault, "mount_id", return_value=1), heal_fault.directory(root) as fd:
                with self.assertRaisesRegex(ValueError, "archived fault receipt"):
                    heal_fault.recover_fault(fd, owner, receipt["faultId"], expected_receipt_sha256="0" * 64)
                self.assertFalse(target.exists())
                proof = heal_fault.recover_fault(fd, owner, receipt["faultId"], expected_receipt_sha256=interrupted.digest(receipt))
                self.assertTrue(proof["restoredFromBackup"])
                self.assertEqual(target.read_bytes(), original)
                proof = heal_fault.recover_fault(fd, owner, receipt["faultId"], expected_receipt_sha256=interrupted.digest(receipt))
                self.assertFalse(proof["restoredFromBackup"])
                self.assertFalse(proof["physicalRepairPassAllowed"])
