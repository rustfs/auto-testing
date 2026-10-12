"""A rejected fault ID cannot bypass ownership, intent or physical-scene checks."""

import hashlib
import base64
import binascii
import copy
import json
import sys
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal.heal_fault import bounded_fault_id
from scripts.validation_heal.fleet_node import audit_finalize, quota_audit_finalize
from scripts.validation_heal.recover_environment import recovery_plan, recovery_scene_matches
from scripts.validation_heal.capacity import release_quota
from scripts.validation_heal.healthy import Fleet
from scripts.validation_heal import recover_environment as recovery
from scripts.validation_heal import fleet_node, heal_fault


class RejectedFaultRecoveryTest(unittest.TestCase):
    def test_expanded_branches_have_bounded_distinct_fault_ids(self):
        branch = "stripe-minus-exact-plus-s1048575-parity-missing"
        original = "D01-" + branch + "-0"
        self.assertEqual(len(original.split("-", 1)[1]), 49)
        identifier = bounded_fault_id("D01", branch, 0)
        self.assertRegex(identifier, r"^D01-[a-z0-9-]{1,48}$")
        self.assertNotEqual(identifier, bounded_fault_id("D01", branch, 1))
        self.assertNotEqual(identifier, bounded_fault_id("D01", branch + "-another", 0))
        with self.assertRaises(ValueError): bounded_fault_id("bad", branch, 0)

    def test_finalization_needs_no_intent_and_exact_archived_scene(self):
        snapshot = {"files": [{"sha256": "actual", "path": "owned/key/xl.meta"}], "formats": [{"disk": "original"}]}
        scene = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        request = {"runId": "gh-12345-1-h1", "disk": 0, "binarySha256": "a" * 64,
                   "rejectedFaultId": "D01-" + "a" * 49, "expectedSceneSha256": scene}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "quarantine").mkdir()
            with patch("scripts.validation_heal.fleet_node.owned", return_value=(None, None, {"binarySha256": "a" * 64})), \
                 patch("scripts.validation_heal.fleet_node.stopped"), \
                 patch("scripts.validation_heal.fleet_node.fault_root", return_value=(root, {})), \
                 patch("scripts.validation_heal.fleet_node.snapshot", return_value=snapshot), \
                 patch("scripts.validation_heal.fleet_node.cleanup", return_value={"cleanup": "PASS"}) as cleanup:
                result = audit_finalize(request)
                self.assertTrue(result["faultNeverApplied"])
                self.assertFalse(result["productRepairClaimed"])
                cleanup.reset_mock()
                for changed in ({**request, "expectedSceneSha256": "b" * 64},
                                {**request, "binarySha256": "b" * 64},
                                {**request, "rejectedFaultId": "D01-valid-length"}):
                    with self.assertRaises(ValueError): audit_finalize(changed)
                    cleanup.assert_not_called()
                (root / "quarantine" / (request["rejectedFaultId"] + ".intent.json")).write_text("intent")
                with self.assertRaisesRegex(ValueError, "intent"):
                    audit_finalize(request)
                cleanup.assert_not_called()


class RejectedMrfRecoveryTests(unittest.TestCase):
    def test_remote_rejection_audit_loads_every_real_snapshot_and_cleanup_backend(self):
        with tempfile.TemporaryDirectory() as directory:
            fleet = Fleet(["vm000"], Path(directory), "gh-12345-1-h2")
            def execute(*args, **kwargs):
                source = kwargs["input"].rsplit("\nmain(", 1)[0]
                namespace = {"__name__": "__validation_node_contract__"}
                with patch.dict(sys.modules):
                    exec(compile(source, "remote-node.py", "exec"), namespace)
                self.assertTrue({"fault", "mrf", "encryption_runtime", "gate", "quorum_gate_backend", "replacement"} <= set(namespace))
                return types.SimpleNamespace(returncode=0, stdout="{}", stderr="")
            with patch("scripts.validation_heal.healthy.subprocess.run", side_effect=execute):
                for action in ("journal-rejection-audit", "journal-rejection-audit-finalize"):
                    fleet.remote("vm000", action)

    def archive(self, root):
        case, bucket = "heal.checkpoint-and-mrf-corruption", "rv-heal-0123456789abcdef"
        base = root / "artifact" / "run" / case
        folder = base / "evidence" / case / "DB/H2/mrf-checksum"
        (base / "cases").mkdir(parents=True); (folder / "fixture").mkdir(parents=True)
        write = lambda p, v: p.write_text(json.dumps(v))
        candidate = {"source_sha": "a" * 40, "package_sha256": "b" * 64}
        provenance = {"source_commit": "a" * 40, "candidate_sha256": "b" * 64,
                      "binary_sha256": "c" * 64, "testing_commit": "d" * 40}
        write(base / "cases" / (case + ".json"), {"case_id": case, "verdict": "FAIL", "cleanup_status": "FAIL", "provenance": provenance})
        row = {"topology": "H2", "fixture": "DB", "branch": "mrf-checksum", "verdict": "FAIL", "cleanup": "FAIL",
            "failure": {"message": "MRF fault lacks an actual committed active owned responsibility on this exact slot"}}
        write(folder.parents[2] / "healthy-summary.json", {"caseId": case, "cleanup": "FAIL", "complete": False,
            "canonicalCasePassClaimed": False, "results": [row]})
        write(folder / "00001-binding.json", {"runId": "gh-12345-1-h2", "bucketPrefix": bucket,
            "candidateCommit": "a" * 40, "candidatePackageSha256": "b" * 64,
            "candidateBinarySha256": "c" * 64, "harnessCommit": "d" * 40})
        scene = {f"vm00{i}": {"quiescent": True, "pid": 0, "binarySha256": "c" * 64, "files": [], "formats": []} for i in range(4)}
        records = {node: {"readOnly": True, "stable": True, "records": []} for node in scene}
        for label, value in (("original-partial-write-physical", scene), ("original-physical-failure-scene", scene),
                             ("original-stopped-MRF-records", records), ("original-MRF-failure-scene", records)):
            write(folder / ("00002-" + label + ".json"), value)
        write(folder / "00003-gate-recovery.json", {"node": "vm003", "state": "CLEAR", "originalPermissionsRestored": True})
        for seq in range(1, 8):
            request = {"sequence": seq, "runId": "gh-12345-1-h2", "operation": "create_bucket" if seq == 1 else "put_object", "params": {"Bucket": bucket}}
            path = folder / "fixture" / f"{seq:06d}-request.json"; write(path, request)
            write(folder / "fixture" / f"{seq:06d}-ack.json", {"sequence": seq, "runId": request["runId"],
                "requestFile": path.name, "requestSha256": hashlib.sha256(path.read_bytes()).hexdigest(), "operationCheck": "PASS",
                "observation": {"operation": request["operation"], "params": request["params"], "clientCallInvoked": True,
                                "resultUnknown": False, "acknowledged": True}})
        return candidate, folder

    def test_unknown_mutation_drift_or_another_injection_keeps_namespace(self):
        for change in (None, "unknown", "missing-ack", "digest", "physical", "records", "injection", "earlier-dirty"):
            with tempfile.TemporaryDirectory() as directory, self.subTest(change=change):
                root = Path(directory); candidate, folder = self.archive(root)
                ack_path = folder / "fixture/000007-ack.json"
                ack = json.loads(ack_path.read_text())
                if change == "unknown": ack["observation"]["resultUnknown"] = True; ack_path.write_text(json.dumps(ack))
                elif change == "missing-ack": ack_path.unlink()
                elif change == "digest": ack["requestSha256"] = "e" * 64; ack_path.write_text(json.dumps(ack))
                elif change in {"physical", "records"}:
                    label = "original-physical-failure-scene" if change == "physical" else "original-MRF-failure-scene"
                    path = next(folder.glob("*-" + label + ".json")); value = json.loads(path.read_text()); value["vm000"]["extra"] = True
                    if change == "physical": value["vm000"]["files"] = [{"disk": 0, "path": "changed"}]
                    path.write_text(json.dumps(value))
                elif change == "injection": (folder / "00004-actual-original-MRF-CRC-injection.json").write_text("{}")
                elif change == "earlier-dirty":
                    path = folder.parents[2] / "healthy-summary.json"; value = json.loads(path.read_text())
                    value["results"].insert(0, {"cleanup": "FAIL"}); path.write_text(json.dumps(value))
                if change is None:
                    plan = recovery_plan(root, 12345, candidate)[0]
                    self.assertEqual(plan["kind"], "rejected-mrf-injection")
                else:
                    with self.assertRaises(ValueError): recovery_plan(root, 12345, candidate)

    def test_node_requires_empty_quarantine_and_exact_stopped_scene(self):
        scene = {"files": [], "formats": []}
        digest = hashlib.sha256(json.dumps(scene, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory).resolve(); roots = tuple(state / f"disk{i}" for i in range(4))
            for root in roots: (root / "quarantine").mkdir(parents=True)
            owner = {"runtimeProfile": "mrf-crash", "bucket": "rv-heal-0123456789abcdef", "binarySha256": "c" * 64}
            request = {"runId": "gh-12345-1-h2", "action": "journal-rejection-audit-finalize", "bucket": owner["bucket"],
                "binarySha256": "c" * 64, "expectedSceneSha256": digest, "rejectedFaultId": "F07-mrf-checksum-0-0-0",
                "expectedMrfSha256": hashlib.sha256(b'{}').hexdigest()}
            with patch.object(fleet_node, "owned", return_value=(state, roots, owner)), patch.object(fleet_node, "stopped"), \
                 patch.object(fleet_node, "digest", return_value="c" * 64), patch.object(fleet_node, "fault", heal_fault, create=True), \
                 patch.object(heal_fault, "mount_id", return_value=1), patch.object(fleet_node, "snapshot", return_value=scene), \
                 patch.object(fleet_node, "mrf_records", return_value={}), \
                 patch.object(fleet_node, "cleanup", return_value={"cleanup": "PASS"}) as cleanup:
                self.assertTrue(fleet_node.journal_rejection_audit_finalize(request)["faultNeverApplied"])
                cleanup.reset_mock()
                self.assertTrue(fleet_node.journal_rejection_audit_finalize({**request, "action": "journal-rejection-audit"})["readyToFinalize"])
                cleanup.assert_not_called()
                for change in ({"expectedSceneSha256": "e" * 64}, {"expectedMrfSha256": "e" * 64}, {"binarySha256": "e" * 64},
                               {"rejectedFaultId": "F07-mrf-checksum-0-0-1"}, {"path": "/shared"}):
                    cleanup.reset_mock()
                    with self.assertRaises(ValueError): fleet_node.journal_rejection_audit_finalize({**request, **change})
                    cleanup.assert_not_called()
                (roots[2] / "quarantine/interrupted.original").write_bytes(b"evidence")
                cleanup.reset_mock()
                with self.assertRaisesRegex(ValueError, "backup or intent"):
                    fleet_node.journal_rejection_audit_finalize(request)
                cleanup.assert_not_called()


class AdvancedMrfRecoveryTests(unittest.TestCase):
    def archive(self, root):
        from test_validation_heal_mrf import DurableMrfTests
        from scripts.validation_heal import journal_fault as journal
        candidate, folder = RejectedMrfRecoveryTests().archive(root)
        def one(label): return next(folder.glob("*-" + label + ".json"))
        summary = json.loads((folder.parents[2] / "healthy-summary.json").read_text())
        summary["results"][0]["failure"]["message"] = "NOT_OBSERVED: original MRF corruption has no candidate diagnostic on vm000"
        (folder.parents[2] / "healthy-summary.json").write_text(json.dumps(summary))
        helper = DurableMrfTests(); observed = helper.observed()
        observed["binarySha256"] = "c" * 64
        original = {node: copy.deepcopy(observed) for node in (f"vm00{i}" for i in range(4))}
        raw = helper.journal()
        receipt = {"schemaVersion": 1, "runId": "gh-12345-1-h2", "disk": 0, "family": "mrf", "taskId": None,
            "relativePath": ".rustfs.sys/.heal-mrf-snapshot.0.bin", "faultId": "F07-mrf-checksum-0-0-0",
            "binarySha256": "c" * 64, "device": 1, "mountId": 2, "productRepairClaimed": False, "kind": "checksum",
            "before": journal.entry(raw), "injected": journal.entry(raw[:-1] + bytes([raw[-1] ^ 1])),
            "node": "vm000", "applied": True, "originalBackupRetained": True}
        injected = copy.deepcopy(original)
        row = next(row for row in injected["vm000"]["records"] if row["disk"] == 0 and row["relativePath"] == receipt["relativePath"])
        row.update(receipt["injected"], rawBase64=base64.b64encode(raw[:-1] + bytes([raw[-1] ^ 1])).decode())
        for label, value in (("original-stopped-MRF-records", original), ("injected-stopped-MRF-records", injected),
                             ("original-MRF-failure-scene", original)):
            one(label).write_text(json.dumps(value)) if list(folder.glob("*-" + label + ".json")) else (folder / ("00004-" + label + ".json")).write_text(json.dumps(value))
        (folder / "00005-actual-original-MRF-CRC-injection.json").write_text(json.dumps(receipt))
        (folder / "00006-recovery.json").write_text(json.dumps({"cleanup": "FAIL", "errors": [{"state": "DIRTY",
            "namespacesRetained": True, "error": "product changed or advanced the durable responsibility; refusing old-record resurrection"}]}))
        return candidate, folder

    def test_plan_requires_complete_acks_exact_injection_and_stopped_failure(self):
        for change in (None, "unknown", "receipt", "records", "candidate", "extra-injection", "earlier-dirty"):
            with tempfile.TemporaryDirectory() as temp, self.subTest(change=change):
                root = Path(temp); candidate, folder = self.archive(root)
                if change == "unknown":
                    path = folder / "fixture/000007-ack.json"; value = json.loads(path.read_text())
                    value["observation"]["resultUnknown"] = True; path.write_text(json.dumps(value))
                elif change == "receipt":
                    path = next(folder.glob("*-actual-original-MRF-CRC-injection.json")); value = json.loads(path.read_text())
                    value["injected"]["sha256"] = "f" * 64; path.write_text(json.dumps(value))
                elif change == "records":
                    path = next(folder.glob("*-original-MRF-failure-scene.json")); value = json.loads(path.read_text())
                    value["vm000"]["records"][0]["rawBase64"] = "bad"; path.write_text(json.dumps(value))
                elif change == "candidate": candidate["package_sha256"] = "f" * 64
                elif change == "extra-injection": (folder / "00007-actual-original-MRF-CRC-injection.json").write_text("{}")
                elif change == "earlier-dirty":
                    path = folder.parents[2] / "healthy-summary.json"; value = json.loads(path.read_text())
                    value["results"].insert(0, {"cleanup": "FAIL"}); path.write_text(json.dumps(value))
                if change is None:
                    plan = recovery_plan(root, 12345, candidate)[0]
                    self.assertEqual(plan["kind"], "advanced-mrf-fixture")
                    self.assertEqual(set(plan["faults"]), {"vm000"})
                else:
                    with self.assertRaises((ValueError, binascii.Error)): recovery_plan(root, 12345, candidate)

    def test_remote_retained_audit_loads_real_guard_backends(self):
        with tempfile.TemporaryDirectory() as temp:
            fleet = Fleet(["vm000"], Path(temp), "gh-12345-1-h2")
            def execute(*args, **kwargs):
                namespace = {"__name__": "__validation_node_contract__"}
                with patch.dict(sys.modules): exec(compile(kwargs["input"].rsplit("\nmain(", 1)[0], "remote-node.py", "exec"), namespace)
                self.assertTrue({"fault", "mrf", "journal_fault_backend", "encryption_runtime", "gate", "quorum_gate_backend", "replacement"} <= set(namespace))
                return types.SimpleNamespace(returncode=0, stdout="{}", stderr="")
            with patch("scripts.validation_heal.healthy.subprocess.run", side_effect=execute):
                for action in ("journal-retained-audit", "journal-retained-audit-finalize"): fleet.remote("vm000", action)

    def test_node_exports_original_backup_and_rejects_drift_before_deletion(self):
        import base64
        from test_validation_heal_mrf import DurableMrfTests
        from scripts.validation_heal import journal_fault as journal, mrf_records as mrf
        for change in (None, "foreign-backup", "extra", "missing", "intent", "scene", "records", "gate", "path"):
            with tempfile.TemporaryDirectory() as temp, self.subTest(change=change):
                state = Path(temp).resolve(); roots = tuple(state / f"disk{i}" for i in range(4))
                for root in roots: (root / "quarantine").mkdir(parents=True)
                owner = {"runId": "gh-12345-1-h2", "runtimeProfile": "mrf-crash", "bucket": "rv-heal-0123456789abcdef", "binarySha256": "c" * 64}
                raw = DurableMrfTests().journal()
                params = {"runId": owner["runId"], "disk": 0, "family": "mrf", "taskId": None,
                    "relativePath": ".rustfs.sys/.heal-mrf-snapshot.0.bin", "faultId": "F07-mrf-checksum-0-0-0", "kind": "checksum"}
                with patch.object(heal_fault, "mount_id", return_value=2), heal_fault.directory(roots[0]) as fd:
                    intent = {**journal.binding(fd, params, owner, heal_fault), "kind": "checksum",
                        "before": journal.entry(raw), "injected": journal.entry(journal.transform(raw, params, owner, mrf))}
                backup = roots[0] / "quarantine"
                for suffix, data in ((".original", raw), (".intent.json", heal_fault.encode(intent)), (".applied.json", heal_fault.encode(intent))):
                    (backup / (params["faultId"] + suffix)).write_bytes(data)
                scene = {"files": [], "formats": []}; records = {"readOnly": True, "stable": True, "records": []}
                sha = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                request = {"runId": owner["runId"], "action": "journal-retained-audit-finalize", "bucket": owner["bucket"],
                    "binarySha256": owner["binarySha256"], "faults": [intent], "originalBackup": {params["faultId"]: base64.b64encode(raw).decode()},
                    "expectedSceneSha256": sha(scene), "expectedMrfSha256": sha(records)}
                if change == "foreign-backup": (backup / (params["faultId"] + ".original")).write_bytes(b"foreign")
                elif change == "extra": (roots[1] / "quarantine/unregistered.original").write_bytes(b"evidence")
                elif change == "missing": (backup / (params["faultId"] + ".applied.json")).unlink()
                elif change == "intent": (backup / (params["faultId"] + ".intent.json")).write_text("{}")
                elif change == "scene": request["expectedSceneSha256"] = "f" * 64
                elif change == "records": request["expectedMrfSha256"] = "f" * 64
                elif change == "path": intent["relativePath"] = ".rustfs.sys/foreign"
                with patch.object(fleet_node, "owned", return_value=(state, roots, owner)), patch.object(fleet_node, "stopped"), \
                     patch.object(fleet_node, "digest", return_value="c" * 64), patch.object(fleet_node, "fault", heal_fault, create=True), \
                     patch.object(heal_fault, "mount_id", return_value=2), patch.object(fleet_node, "journal_fault_backend", journal, create=True), \
                     patch.object(fleet_node, "mrf", mrf, create=True), patch.object(fleet_node, "mrf_records", return_value=records), \
                     patch.object(fleet_node, "snapshot", return_value=scene), patch.object(fleet_node, "gate_module") as gate, \
                     patch.object(fleet_node, "cleanup", return_value={"cleanup": "PASS", "unitStopped": True, "rootsRemoved": True}) as cleanup:
                    gate.return_value.checked_gate.return_value = {} if change == "gate" else None
                    if change is None:
                        audit = fleet_node.journal_retained_audit_finalize({**request, "action": "journal-retained-audit"})
                        cleanup.assert_not_called()
                        self.assertEqual(base64.b64decode(audit["originalJournalBackups"][0]["original"]["rawBase64"]), raw)
                        result = fleet_node.journal_retained_audit_finalize(request)
                        self.assertFalse(result["backupRestorationUsed"])
                        self.assertFalse(result["productRepairClaimed"])
                        cleanup.assert_called_once()
                    else:
                        with self.assertRaises(ValueError): fleet_node.journal_retained_audit_finalize(request)
                        cleanup.assert_not_called()


class RecreationRecoveryTests(unittest.TestCase):
    def archive(self, root):
        case, bucket = "heal.bucket-incarnation-recreation", "rv-heal-0123456789abcdef"
        base = root / "rustfs-validation-12345-1" / "run" / case
        folder = base / "evidence" / case / "H1/old-metadata-rejoin"
        (base / "cases").mkdir(parents=True)
        (folder / "fixture").mkdir(parents=True)
        candidate = {"source_sha": "a" * 40, "package_sha256": "b" * 64}
        provenance = {"source_commit": "a" * 40, "candidate_sha256": "b" * 64,
                      "binary_sha256": "c" * 64, "testing_commit": "d" * 40}
        write = lambda path, value: path.write_text(json.dumps(value))
        write(base / "cases" / (case + ".json"), {"case_id": case, "verdict": "FAIL",
            "cleanup_status": "FAIL", "provenance": provenance})
        write(folder.parent.parent / "healthy-summary.json", {"caseId": case, "cleanup": "FAIL",
            "complete": False, "canonicalCasePassClaimed": False, "results": [{"topology": "H1",
            "fixture": "DV", "branch": "old-metadata-rejoin", "verdict": "FAIL", "cleanup": "FAIL",
            "failure": {"type": "RecordedCallError", "message": "unexpected delete_bucket result; inspect ACK 000005, do not silently retry"},
            "unresolvedMutation": {"operation": "normal-S3-delete-and-recreate"}}]})
        write(folder / "00001-binding.json", {"runId": "gh-12345-1-h1", "bucketPrefix": bucket,
            "candidateCommit": "a" * 40, "candidatePackageSha256": "b" * 64,
            "candidateBinarySha256": "c" * 64, "harnessCommit": "d" * 40})
        write(folder / "00002-original-recreation-failure-scene.json", {"vm000": {"quiescent": True,
            "pid": 0, "binarySha256": "c" * 64, "formats": [], "files": [{"disk": 0,
            "path": bucket + "/objects/key/xl.meta", "size": 1, "sha256": "e" * 64}]}})
        write(folder / "00003-old-ACK-manifest.json", {"expected": [{"key": "objects/key", "versionId": version}
            for version in ("old-data", "old-marker")]})
        write(folder / "00004-gate-recovery.json", {"node": "vm000", "state": "CLEAR", "originalPermissionsRestored": True})
        calls = [("create_bucket", {"Bucket": bucket}),
                 ("delete_object", {"Bucket": bucket, "Key": "objects/key", "VersionId": "old-data"}),
                 ("delete_object", {"Bucket": bucket, "Key": "objects/key", "VersionId": "old-marker"}),
                 ("list_multipart_uploads", {"Bucket": bucket}), ("delete_bucket", {"Bucket": bucket})]
        for sequence, (operation, params) in enumerate(calls, 1):
            request_path = folder / "fixture" / f"{sequence:06d}-request.json"
            write(request_path, {"sequence": sequence, "runId": "gh-12345-1-h1", "operation": operation, "params": params})
            write(folder / "fixture" / f"{sequence:06d}-ack.json", {"sequence": sequence, "runId": "gh-12345-1-h1",
                "requestFile": request_path.name, "requestSha256": hashlib.sha256(request_path.read_bytes()).hexdigest(),
                "operationCheck": "FAIL" if sequence == 5 else "PASS", "observation": {"operation": operation,
                    "params": params, "clientCallInvoked": True, "resultUnknown": False, "acknowledged": sequence != 5,
                    "response": {"Error": {"Code": "InternalError", "Message": "File access denied"},
                                 "ResponseMetadata": {"HTTPStatusCode": 500}} if sequence == 5 else {}}})
        return candidate, folder

    def test_known_refusal_retains_original_failure_without_claiming_product_repair(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate, _ = self.archive(root)
            plan = recovery_plan(root, 12345, candidate)[0]
            self.assertEqual(plan["kind"], "rejected-incarnation-delete")
            self.assertEqual(plan["acknowledgedVersionDeletions"], 2)
            self.assertFalse(plan["unknownMutationClaimed"])
            self.assertEqual(plan["originalProductError"]["Code"], "InternalError")

    def test_unknown_calls_missing_ack_drifted_digest_and_different_refusal_are_rejected(self):
        for mutation in ("unknown-final", "unknown-prior", "digest", "missing", "different-error", "foreign-request"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp); candidate, folder = self.archive(root)
                path = folder / "fixture" / ("000002-ack.json" if mutation == "unknown-prior" else "000005-ack.json")
                value = json.loads(path.read_text())
                if mutation.startswith("unknown-"): value["observation"]["resultUnknown"] = True
                elif mutation == "digest": value["requestSha256"] = "f" * 64
                elif mutation == "different-error": value["observation"]["response"]["Error"]["Code"] = "BucketNotEmpty"
                elif mutation == "foreign-request": value["observation"]["params"]["Bucket"] = "unowned"
                if mutation == "missing": path.unlink()
                else: path.write_text(json.dumps(value))
                with self.assertRaises(ValueError): recovery_plan(root, 12345, candidate)

    def test_running_scene_additional_case_or_replay_transaction_cannot_be_finalized(self):
        for mutation in ("running", "source", "another-case", "replay", "successor", "unrecovered-gate"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp); candidate, folder = self.archive(root)
                if mutation == "running":
                    path = folder / "00002-original-recreation-failure-scene.json"
                    value = json.loads(path.read_text()); value["vm000"]["pid"] = 99; path.write_text(json.dumps(value))
                elif mutation == "source": candidate["source_sha"] = "f" * 40
                elif mutation == "another-case":
                    path = folder.parents[4] / "another/cases/another.json"
                    path.parent.mkdir(parents=True); path.write_text("{}")
                elif mutation == "unrecovered-gate":
                    path = folder / "00004-gate-recovery.json"
                    value = json.loads(path.read_text()); value["state"] = "ACTIVE"; path.write_text(json.dumps(value))
                else: (folder / ("00005-" + ("archive-and-detach-original-replay" if mutation == "replay" else "successor-ACK-bound-S3") + ".json")).write_text("{}")
                with self.assertRaises(ValueError): recovery_plan(root, 12345, candidate)

    def test_finalization_requires_stopped_unchanged_candidate_gate_and_credential_boundary(self):
        for mutation in (None, "scene-drift", "binary", "gate", "credential"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp); candidate, _ = self.archive(root)
                plan = recovery_plan(root, 12345, candidate)[0]
                events = []
                def snapshot(self, label, **kwargs):
                    events.append(label)
                    scene = copy.deepcopy(plan["before"])
                    if mutation == "scene-drift" and label == "physical-immediately-before-finalization": scene["vm000"]["files"] = []
                    if mutation == "binary": scene["vm000"]["binarySha256"] = "f" * 64
                    return scene
                def remote(self, node, action, **params):
                    events.append(action)
                    if action == "namespace-state": return {"state": {"exists": True}, "roots": [{"exists": True}]}
                    if action == "gate-state": return {"state": "ACTIVE" if mutation == "gate" else "CLEAR"}
                    if action == "bucket-record-snapshot": return {"credentialsRead": mutation == "credential"}
                    if action == "cleanup": return {"cleanup": "PASS", "unitStopped": True, "rootsRemoved": True}
                    return {}
                candidate_path = root / "candidate.json"; candidate_path.write_text(json.dumps(candidate))
                argv = ["recover_environment.py", "--failed-run-id", "12345", "--artifact", str(root),
                        "--candidate", str(candidate_path), "--evidence", str(root / "recovery")]
                with patch("sys.argv", argv), patch.object(Fleet, "remote", remote), patch.object(Fleet, "snapshot", snapshot), \
                     patch.dict("os.environ", {"RUSTFS_NODES": "vm000 vm001 vm002 vm003"}):
                    if mutation is None: recovery.main()
                    else:
                        with self.assertRaises(ValueError): recovery.main()
                if mutation is None:
                    self.assertLess(events.index("bucket-record-snapshot"), events.index("cleanup"))
                    summary = json.loads((root / "recovery/recovery-summary.json").read_text())
                    self.assertFalse(summary["canonicalPassClaimed"])
                    self.assertFalse(summary["environments"][0]["originalVerdictRewritten"])
                else: self.assertNotIn("cleanup", events)


class QuotaRecoveryTests(unittest.TestCase):
    def test_standalone_remote_recovery_loads_all_cleanup_backends(self):
        import types
        with tempfile.TemporaryDirectory() as temp, patch("scripts.validation_heal.healthy.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout='{}', stderr='')) as command:
            fleet = Fleet(["node"], Path(temp), "gh-12345-1-h1")
            fleet.remote("node", "quota-audit-finalize", bucket="rv-heal-0123456789abcdef",
                         binarySha256="a" * 64, expectedSceneSha256="b" * 64)
            source = command.call_args.kwargs["input"].rsplit("\nmain(", 1)[0]
            scope = {"__name__": "validation_remote_recovery"}
            exec(compile(source, "standalone-recovery", "exec"), scope)
            self.assertTrue(callable(scope["quota_audit_finalize"]))
            self.assertTrue(callable(scope["namespace_state"]))
            self.assertTrue(callable(scope["quorum_gate_backend"].release))
            self.assertTrue(callable(scope["replacement"].attest))
            self.assertTrue(callable(scope["network_gate_backend"].release))
            self.assertTrue(callable(scope["encryption_module"]().attest))

    def test_standalone_rejected_id_finalization_loads_snapshot_backend(self):
        import types
        with tempfile.TemporaryDirectory() as temp, patch("scripts.validation_heal.healthy.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout='{}', stderr='')) as command:
            Fleet(["node"], Path(temp), "gh-12345-1-h1").remote("node", "audit-finalize")
            source = command.call_args.kwargs["input"].rsplit("\nmain(", 1)[0]
            scope = {"__name__": "validation_remote_recovery"}
            exec(compile(source, "standalone-recovery", "exec"), scope)
            self.assertTrue(callable(scope["encryption_module"]().attest))

    def archive(self, root):
        case = "heal.enospc-inodes-and-quota"
        base = root / "rustfs-validation-12345-1" / "run" / case
        directory = base / "evidence" / case
        branch = directory / "H1/bucket-quota"
        branch.mkdir(parents=True)
        def write(path, value): path.write_text(json.dumps(value))
        candidate = {"source_sha": "a" * 40, "package_sha256": "b" * 64}
        provenance = {"source_commit": "a" * 40, "candidate_sha256": "b" * 64,
                      "binary_sha256": "c" * 64, "testing_commit": "d" * 40}
        (base / "cases").mkdir()
        write(base / "cases" / (case + ".json"), {"case_id": case, "verdict": "FAIL", "cleanup_status": "FAIL", "provenance": provenance})
        write(directory / "healthy-summary.json", {"cleanup": "FAIL", "results": [{"topology": "H1", "branch": "bucket-quota",
            "verdict": "FAIL", "cleanup": "FAIL", "failure": {"message": "peer did not observe the exact acknowledged quota limit"}, "recoveryError": "connection refused"}]})
        bucket = "rv-heal-0123456789abcdef"
        binding = {"runId": "gh-12345-1-h1", "bucketPrefix": bucket, "candidateBinarySha256": "c" * 64, "harnessCommit": "d" * 40}
        write(branch / "00001-binding.json", binding)
        entries = [{"disk": 0, "path": f"{bucket}/objects/{index}/part.1", "size": 100 + index, "sha256": str(index) * 64} for index in range(2)]
        control = {"disk": 1, "path": bucket + "/control/xl.meta", "size": 5, "sha256": "e" * 64}
        scene = {"node": {"quiescent": True, "formats": [{"disk": "actual-format"}], "files": [*entries, control], "backgroundConfig": None}}
        write(branch / "00002-original-complete-physical.json", scene)
        write(branch / "00003-physical-original-failure-before-any-rescue.json", {"node": {**scene["node"], "files": [control]}})
        write(branch / "00004-manual-source-isolation-before-fault.json", {"node": {"backgroundConfig": {"RUSTFS_SCANNER_ENABLED": "false"}, "binarySha256": "c" * 64}})
        write(branch / "00005-owned-bucket-quota-PUT.json", {"path": "/rustfs/admin/v3/quota/" + bucket, "method": "PUT", "httpStatus": 200,
            "response": {"quota": 1024**2, "quota_type": "HARD"}})
        write(branch / "00006-all-peer-owned-quota-GET.json", {"path": "/rustfs/admin/v3/quota/" + bucket, "method": "GET", "httpStatus": 503,
            "response": "authoritative bucket usage is not available yet"})
        for index, entry in enumerate(entries):
            write(branch / f"0001{index}-exact-missing-part.json", {"runId": binding["runId"], "faultId": f"I02-target-{index}",
                "kind": "missing", "relativePath": entry["path"], "before": {"size": entry["size"], "sha256": entry["sha256"]}})
        return candidate, branch, directory

    def test_recovery_is_bound_to_failed_workflow_candidate_and_both_backups(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            candidate, _, _ = self.archive(root)
            plan = recovery_plan(root, 12345, candidate)[0]
            self.assertEqual(plan["kind"], "owned-quota")
            self.assertEqual(len(plan["faults"]), 2)
            self.assertEqual(plan["nodes"], ["node"])
            for run, value in [(12346, candidate), (12345, {**candidate, "package_sha256": "f" * 64})]:
                with self.assertRaises(ValueError): recovery_plan(root, run, value)


    def test_retry_accepts_only_original_rescued_shards_and_no_other_drift(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            candidate, _, _ = self.archive(root)
            plan = recovery_plan(root, 12345, candidate)[0]
            self.assertTrue(recovery_scene_matches(plan["failure"], plan))
            self.assertTrue(recovery_scene_matches(plan["before"], plan))
            partial = json.loads(json.dumps(plan["failure"]))
            partial["node"]["files"].append(plan["before"]["node"]["files"][0])
            self.assertTrue(recovery_scene_matches(partial, plan))
            for changed in ["format", "control", "rescued", "missing-control", "extra", "duplicate", "node"]:
                current = json.loads(json.dumps(partial))
                if changed == "format": current["node"]["formats"] = []
                elif changed == "control": current["node"]["files"][0]["sha256"] = "wrong"
                elif changed == "rescued": current["node"]["files"][1]["sha256"] = "wrong"
                elif changed == "missing-control": current["node"]["files"].pop(0)
                elif changed == "extra": current["node"]["files"].append({"disk": 3, "path": "foreign"})
                elif changed == "duplicate": current["node"]["files"].append(current["node"]["files"][0])
                else: current["foreign"] = current.pop("node")
                with self.subTest(changed=changed):
                    self.assertFalse(recovery_scene_matches(current, plan))

    def test_uncertain_workload_and_collateral_drift_always_keep_namespace(self):
        for changed in ["invoked-PUT", "unknown-ACK", "collateral", "wrong-run", "duplicate-fault", "other-retained"]:
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temp:
                root = Path(temp); candidate, branch, directory = self.archive(root)
                if changed == "invoked-PUT": (branch / "negative-PUT").mkdir()
                elif changed == "unknown-ACK":
                    path = directory / "healthy-summary.json"; data = json.loads(path.read_text())
                    data["results"][0]["negativePUT"] = {"state": "UNKNOWN"}; path.write_text(json.dumps(data))
                elif changed == "collateral":
                    path = branch / "00003-physical-original-failure-before-any-rescue.json"; data = json.loads(path.read_text())
                    data["node"]["files"][0]["sha256"] = "new"; path.write_text(json.dumps(data))
                elif changed in {"wrong-run", "duplicate-fault"}:
                    path = branch / "00011-exact-missing-part.json"; data = json.loads(path.read_text())
                    data["runId" if changed == "wrong-run" else "faultId"] = "gh-12346-1-h1" if changed == "wrong-run" else "I02-target-0"
                    path.write_text(json.dumps(data))
                else:
                    path = directory / "healthy-summary.json"; data = json.loads(path.read_text())
                    data["results"].append({"cleanup": "FAIL", "branch": "another"}); path.write_text(json.dumps(data))
                with self.assertRaises(ValueError): recovery_plan(root, 12345, candidate)

    def test_node_finalization_requires_same_private_candidate_and_full_rescue(self):
        scene = {"files": [{"path": "owned/key/part.1", "sha256": "actual"}], "formats": [{"disk": "original"}]}
        scene_sha = hashlib.sha256(json.dumps(scene, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp); roots = [state / str(index) for index in range(4)]
            bucket = "rv-heal-0123456789abcdef"
            owner = {"runtimeProfile": "manual", "bucket": bucket, "binarySha256": "a" * 64,
                     "volumes": [str(root / "data") for root in roots]}
            request = {"runId": "gh-12345-1-h1", "action": "quota-audit-finalize", "bucket": bucket,
                       "binarySha256": "a" * 64, "expectedSceneSha256": scene_sha}
            with patch("scripts.validation_heal.fleet_node.owned", return_value=(state, roots, owner)), \
                 patch("scripts.validation_heal.fleet_node.stopped"), patch("scripts.validation_heal.fleet_node.digest", return_value="a" * 64), \
                 patch("scripts.validation_heal.fleet_node.snapshot", return_value=scene), \
                 patch("scripts.validation_heal.fleet_node.cleanup", return_value={"cleanup": "PASS"}) as cleanup:
                value = quota_audit_finalize(request)
                self.assertTrue(value["privateBucketQuotaNamespaceRemoved"])
                self.assertFalse(value["serviceStartedDuringRecovery"])
                self.assertFalse(value["productRepairClaimed"])
                cleanup.reset_mock()
                for changes in [{"bucket": "foreign"}, {"binarySha256": "b" * 64}, {"runId": "gh-12345-1-h2"}, {"expectedSceneSha256": "b" * 64}]:
                    with self.assertRaises(ValueError): quota_audit_finalize({**request, **changes})
                    cleanup.assert_not_called()
                (state / "drive-gate.json").write_text("active")
                with self.assertRaises(ValueError): quota_audit_finalize(request)
                cleanup.assert_not_called()

    def test_release_restarts_stopped_service_and_reads_config_without_usage_claim(self):
        import types
        events = []
        binding = {"endpoints": {"node1": "http://owned"}}
        def admin(path, query, body, method):
            events.append((method, path, query))
            if method == "PUT": return {"httpStatus": 200, "response": {"bucket": "owned", "quota": 100, "quota_type": "HARD"}}
            return {"httpStatus": 200, "response": {"quota": 100, "size": 100, "quotatype": "hard"}}
        probe = types.SimpleNamespace(binding=binding, admin=admin)
        fleet = types.SimpleNamespace(nodes=["node"], remote=lambda *args: {"pid": 0},
            start=lambda: events.append("start"), record=lambda label, value: value)
        with patch("scripts.validation_heal.capacity.Probe", return_value=probe):
            result = release_quota(probe, fleet, "owned", 100)
        self.assertEqual(events[0], "start")
        self.assertEqual(events[-1], ("GET", "/rustfs/admin/v3/get-bucket-quota", [("bucket", "owned")]))
        self.assertFalse(result["authoritativeUsageClaimed"])


class PerformanceBoundaryRecoveryTests(unittest.TestCase):
    def archive(self, root):
        case = "heal.performance-abba"
        base = root / "rustfs-validation-12345-1" / "run" / case
        evidence = base / "evidence" / case / "H2"
        (base / "cases").mkdir(parents=True)
        evidence.mkdir(parents=True)
        candidate = {"source_sha": "a" * 40, "package_sha256": "b" * 64}
        provenance = {"source_commit": "a" * 40, "candidate_sha256": "b" * 64,
                      "binary_sha256": "c" * 64, "testing_commit": "d" * 40}
        write = lambda path, value: path.write_text(json.dumps(value))
        write(base / "cases" / (case + ".json"), {"case_id": case, "verdict": "FAIL",
            "cleanup_status": "FAIL", "provenance": provenance})
        write(evidence.parent / "healthy-summary.json", {"cleanup": "FAIL", "results": [{"topology": "H2",
            "verdict": "FAIL", "cleanup": "FAIL", "failure": {"type": "ValueError",
            "message": "Heal node vm000 apply-fault failed: fault lease expired"}}]})
        scene = {node: {"formats": [{"disk": node}], "files": [
            {"path": "rv-heal-0123456789abcd/objects/key/part.1", "sha256": node, "size": 1},
            {"path": "rv-heal-0123456789abcd-load/load/00.bin/xl.meta", "sha256": node, "size": 1},
        ]} for node in ("vm000", "vm001", "vm002", "vm003")}
        binding = {"runId": "gh-12345-1-h2", "bucketPrefix": "rv-heal-0123456789abcd",
                   "candidateBinarySha256": "c" * 64, "harnessCommit": "d" * 40}
        write(evidence / "00001-binding.json", binding)
        write(evidence / "00002-phase-protocol-g3-p3-B.json", {"group": 3, "phase": 3, "mode": "B"})
        write(evidence / "00003-physical-phase-baseline-g3-p3-B.json", scene)
        write(evidence / "00004-physical-failure.json", scene)
        return candidate, evidence

    def test_only_unapplied_fault_boundary_can_be_finalized(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            candidate, evidence = self.archive(root)
            plan = recovery_plan(root, 12345, candidate)[0]
            self.assertEqual(plan["kind"], "performance-no-fault-applied")
            self.assertEqual(plan["nodes"], ["vm000", "vm001", "vm002", "vm003"])
            (evidence / "00005-fault-g3-p3-B.json").write_text("{}")
            with self.assertRaises(ValueError): recovery_plan(root, 12345, candidate)

    def test_full_performance_finalization_cleans_each_node_once(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            candidate, _ = self.archive(root)
            plan = recovery_plan(root, 12345, candidate)[0]
            candidate_path = root / "candidate.json"
            candidate_path.write_text(json.dumps(candidate))
            calls = []
            def remote(node, action, **params):
                calls.append((node, action))
                if action == "namespace-state":
                    return {"state": {"exists": True}, "roots": [{"exists": True}]}
                if action == "logs": return {}
                self.assertEqual(action, "cleanup")
                return {"cleanup": "PASS", "unitStopped": True, "rootsRemoved": True}
            fake = types.SimpleNamespace(record=lambda label, value: value,
                snapshot=lambda *args, **kwargs: plan["before"], remote=remote)
            argv = ["recover_environment.py", "--failed-run-id", "12345", "--artifact", str(root),
                    "--candidate", str(candidate_path), "--evidence", str(root / "recovery")]
            with patch.object(recovery, "Fleet", return_value=fake), patch("sys.argv", argv), \
                    patch.dict("os.environ", RUSTFS_NODES="vm000 vm001 vm002 vm003"):
                recovery.main()
            self.assertEqual([node for node, action in calls if action == "cleanup"], plan["nodes"])
            self.assertEqual(json.loads((root / "recovery/recovery-summary.json").read_text())["cleanup"], "PASS")

    def test_absent_namespace_with_active_or_unknown_service_cannot_claim_cleanup(self):
        for service in ({"MainPID": "7", "ActiveState": "active", "SubState": "running"},
                        {}, {"MainPID": "0", "ActiveState": "active", "SubState": "exited"}):
            with self.subTest(service=service), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                candidate, _ = self.archive(root)
                candidate_path = root / "candidate.json"
                candidate_path.write_text(json.dumps(candidate))
                def remote(node, action):
                    self.assertEqual(action, "namespace-state")
                    return {"state": {"exists": False}, "roots": [{"exists": False}], "service": service}
                fake = types.SimpleNamespace(record=lambda label, value: value, remote=remote)
                argv = ["recover_environment.py", "--failed-run-id", "12345", "--artifact", str(root),
                        "--candidate", str(candidate_path), "--evidence", str(root / "recovery")]
                with patch.object(recovery, "Fleet", return_value=fake), patch("sys.argv", argv), \
                        patch.dict("os.environ", RUSTFS_NODES="vm000 vm001 vm002 vm003"):
                    with self.assertRaisesRegex(ValueError, "active or unknown service"):
                        recovery.main()
                self.assertFalse((root / "recovery/recovery-summary.json").exists())


if __name__ == "__main__":
    unittest.main()
