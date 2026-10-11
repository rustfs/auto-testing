"""A rejected fault ID cannot bypass ownership, intent or physical-scene checks."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal.heal_fault import bounded_fault_id
from scripts.validation_heal.fleet_node import audit_finalize, quota_audit_finalize
from scripts.validation_heal.recover_environment import recovery_plan, recovery_scene_matches
from scripts.validation_heal.capacity import release_quota
from scripts.validation_heal.healthy import Fleet


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
        scene = {node: {"formats": [{"disk": node}], "files": [{"path": "owned/key/part.1",
            "sha256": node, "size": 1}]} for node in ("vm000", "vm001", "vm002", "vm003")}
        binding = {"runId": "gh-12345-1-h2", "candidateBinarySha256": "c" * 64, "harnessCommit": "d" * 40}
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


if __name__ == "__main__":
    unittest.main()
