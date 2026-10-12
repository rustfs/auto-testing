"""J02 rejects fabricated truth and checks faults before fixture recovery."""

import copy
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import outcome
from scripts.validation_heal.healthy import BASE_OPTIONS, physical_content
from scripts.validation_heal.shards import files
from test_validation_heal import versioned_scene


class MixedOutcomeTest(unittest.TestCase):
    bucket = "rv-heal-0123456789abcdef"
    incarnation = "12345678-1234-1234-1234-123456789abc"
    options = {**BASE_OPTIONS, "recursive": True}

    def mixed(self):
        expected = [{"key": "objects/0000.bin", "versionId": "v0", "state": "verified_healthy"},
                    {"key": "objects/0001.bin", "versionId": "v1", "state": "repaired"},
                    {"key": "objects/0002.bin", "versionId": "v2", "state": "failed"}]
        status = {"summary": "stopped", "settings": self.options, "truncated": False,
            "objectsHealed": 77, "outcome": {"execution": {"state": "completed_with_errors"},
            "coverage": "complete", "objectsTruncated": False,
            "counters": {"processed": 3, "healed": 1, "unchanged": 1, "skipped": 0, "failed": 1,
                         "unknown": 0, "attemptFailures": 4, "overflowed": False},
            "objects": [{"identity": {"kind": "object", "bucket": self.bucket, "object": entry["key"], "versionId": entry["versionId"],
                "poolIndex": 0, "setIndex": 0, "bucketIncarnationId": self.incarnation},
                "disposition": {"state": entry["state"], **({"details": "retry_exhausted"} if entry["state"] == "failed" else {})}}
                for entry in expected]}}
        return expected, status

    def check(self, expected, status, options=None, attempts=4):
        return outcome.reconcile(status, self.bucket, expected, options or self.options, self.incarnation,
                                 attempt_failures=attempts)

    def test_full_mixed_truth_keeps_failure_and_ignores_legacy_repair_count(self):
        expected, status = self.mixed()
        proof = self.check(expected, status)
        self.assertEqual(proof["canonicalCounters"]["failed"], 1)
        self.assertFalse(proof["failedIsRepair"])
        self.assertFalse(proof["legacyObjectsHealedUsedAsRepairEvidence"])

    def test_each_identity_boundary_rejects_wrong_or_ambiguous_receipts(self):
        expected, source = self.mixed()
        for field, values in {"kind": ["bucket", None], "bucket": ["foreign"], "object": ["foreign"], "versionId": [None, "v9"],
                              "poolIndex": [True, 1, None], "setIndex": [False, 1],
                              "bucketIncarnationId": [None, "", "new-incarnation"]}.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    status = copy.deepcopy(source)
                    status["outcome"]["objects"][1]["identity"][field] = value
                    with self.assertRaises(ValueError): self.check(expected, status)
        status = copy.deepcopy(source)
        status["outcome"]["objects"][1] = copy.deepcopy(status["outcome"]["objects"][0])
        with self.assertRaises(ValueError): self.check(expected, status)

    def test_wrong_terminals_or_partial_coverage_cannot_certify_negative_truth(self):
        expected, source = self.mixed()
        for name, value in (("summary", "finished"), ("summary", "running"), ("truncated", True),
                            ("settings", {**self.options, "remove": True})):
            status = copy.deepcopy(source); status[name] = value
            with self.subTest(name=name, value=value), self.assertRaises(ValueError): self.check(expected, status)
        for name, value in (("execution", {"state": "completed"}), ("coverage", "partial"), ("objectsTruncated", True)):
            status = copy.deepcopy(source); status["outcome"][name] = value
            with self.subTest(name=name, value=value), self.assertRaises(ValueError): self.check(expected, status)

    def test_unknown_skipped_deferred_and_false_health_never_substitute_for_failed(self):
        expected, source = self.mixed()
        for state in ("unknown", "skipped", "deferred", "verified_healthy", "metadata_healthy", "repaired"):
            status = copy.deepcopy(source); status["outcome"]["objects"][2]["disposition"]["state"] = state
            with self.subTest(state=state), self.assertRaises(ValueError): self.check(expected, status)
        for details in ("recoverable", "permanent", None):
            status = copy.deepcopy(source); status["outcome"]["objects"][2]["disposition"]["details"] = details
            with self.subTest(details=details), self.assertRaises(ValueError): self.check(expected, status)

    def test_every_counter_rejects_inflation_understatement_and_bool_coercion(self):
        expected, source = self.mixed()
        for name, original in source["outcome"]["counters"].items():
            for value in (None, True if name != "overflowed" else 0, original + 1 if type(original) is int else True):
                status = copy.deepcopy(source); status["outcome"]["counters"][name] = value
                with self.subTest(counter=name, value=value), self.assertRaises(ValueError): self.check(expected, status)

    def test_dry_run_requires_all_original_members_without_repairs_or_hidden_attempts(self):
        expected, status = self.mixed()
        for entry, receipt in zip(expected, status["outcome"]["objects"]):
            entry["state"] = "dry_run_observed"; receipt["disposition"] = {"state": "dry_run_observed"}
        options = {**self.options, "dryRun": True}
        status.update(summary="finished", settings=options)
        status["outcome"].update(execution={"state": "completed"},
            counters={"processed": 3, "healed": 0, "unchanged": 0, "skipped": 3, "failed": 0,
                      "unknown": 0, "attemptFailures": 0, "overflowed": False})
        self.check(expected, status, options, 0)
        status["outcome"]["objects"].pop()
        with self.assertRaises(ValueError): self.check(expected, status, options, 0)

    def test_key_level_deferral_cannot_invent_an_incarnation_or_version(self):
        expected = [{"key": "objects/0002.bin", "versionId": "null", "state": "deferred", "keyLevelRefusal": True}]
        options = {**self.options, "recursive": False}
        status = {"summary": "finished", "settings": options, "truncated": False, "outcome": {
            "execution": {"state": "completed"}, "coverage": "complete", "objectsTruncated": False,
            "counters": {"processed": 1, "healed": 0, "unchanged": 0, "skipped": 1,
                         "failed": 0, "unknown": 0, "attemptFailures": 1, "overflowed": False},
            "objects": [{"identity": {"kind": "object", "bucket": self.bucket, "object": expected[0]["key"], "versionId": None,
                        "bucketIncarnationId": None, "poolIndex": 0, "setIndex": 0}, "disposition": {
                        "state": "deferred", "details": {"reason": "transient_existence_check", "retry_not_before": None}}}]}}
        self.check(expected, status, options, 1)
        for field, value in (("bucketIncarnationId", self.incarnation), ("versionId", "null"), ("versionId", "v0")):
            changed = copy.deepcopy(status); changed["outcome"]["objects"][0]["identity"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): self.check(expected, changed, options, 1)
        changed = copy.deepcopy(status)
        changed["outcome"]["objects"][0]["disposition"]["details"]["reason"] = "dangling_delete_grace"
        with self.assertRaises(ValueError): self.check(expected, changed, options, 1)

    def test_planner_uses_actual_erasure_geometry_and_preserves_unselected_versions(self):
        scene, bucket, expected = versioned_scene([f"objects/{index:04d}.bin" for index in range(3)])
        for branch in outcome.BRANCHES[:3]:
            with self.subTest(branch=branch):
                plan = outcome.fault_plan(scene, bucket, expected, branch)
                self.assertEqual(len(plan["retainedMembers"]), plan["readQuorum"] - 1)
                self.assertEqual(len(plan["manifest"]), 1 if branch == "deferred" else len(expected))
                if branch == "mixed-healthy-repaired-failed":
                    states = {outcome.version_identity(entry): entry["state"] for entry in plan["manifest"]}
                    self.assertEqual(states[("objects/0002.bin", "v0")], "failed")
                    self.assertEqual(states[("objects/0002.bin", "v1")], "verified_healthy")
                if branch == "deferred":
                    self.assertTrue(all(target["relativePath"].endswith("/xl.meta") for target in plan["faults"]))

    def test_physical_oracle_rejects_false_repair_and_any_change_to_a_control(self):
        original, bucket, expected = versioned_scene([f"objects/{index:04d}.bin" for index in range(3)])
        plan = outcome.fault_plan(original, bucket, expected, "mixed-healthy-repaired-failed")
        faulted = copy.deepcopy(original)
        for target in plan["faults"]:
            faulted[target["node"]]["files"] = [entry for entry in faulted[target["node"]]["files"]
                if (entry["disk"], entry["path"]) != (target["disk"], target["relativePath"])]
        after = copy.deepcopy(faulted)
        target = plan["repair"]
        after[target["node"]]["files"].append(files(original)[target["node"], target["disk"], target["relativePath"]])
        before_digest = physical_content(after)
        proof = outcome.retained_failure_scene(original, faulted, after, plan, "mixed-healthy-repaired-failed")
        self.assertTrue(proof["oracleViewOnly"])
        self.assertEqual(physical_content(after), before_digest)
        with self.assertRaises(ValueError):
            outcome.retained_failure_scene(original, faulted, faulted, plan, "mixed-healthy-repaired-failed")
        altered = copy.deepcopy(after); altered[target["node"]]["files"][0]["sha256"] = "changed-control"
        with self.assertRaises(ValueError):
            outcome.retained_failure_scene(original, faulted, altered, plan, "mixed-healthy-repaired-failed")
        with self.assertRaises(ValueError):
            outcome.retained_failure_scene(original, faulted, original, plan, "mixed-healthy-repaired-failed")

    def test_dry_run_and_deferred_require_exact_physical_silence(self):
        original, bucket, expected = versioned_scene([f"objects/{index:04d}.bin" for index in range(3)])
        for branch in ("dry-run", "deferred"):
            plan = outcome.fault_plan(original, bucket, expected, branch)
            self.assertTrue(outcome.retained_failure_scene(original, original, original, plan, branch)["physicalUnchanged"])
            changed = copy.deepcopy(original); changed["node1"]["files"].pop()
            with self.assertRaises(ValueError): outcome.retained_failure_scene(original, original, changed, plan, branch)

    def test_lost_rescue_ack_keeps_registered_fault_for_independent_recovery(self):
        fleet = types.SimpleNamespace(faults=[("node1", {"disk": 0, "faultId": "J02-mixed-0"})],
            remote=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("lost ACK")), record=lambda *args: None)
        with self.assertRaises(RuntimeError): outcome.rescue(fleet)
        self.assertEqual(len(fleet.faults), 1)

    def test_native_prerequisite_rejection_happens_before_fleet_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": outcome.CANDIDATE}))
            args = types.SimpleNamespace(candidate=candidate)
            with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=args), \
                 patch.object(outcome, "verify_manager", side_effect=ValueError("wrong immutable evidence")), \
                 patch.object(outcome, "Fleet") as fleet, patch.object(outcome.signal, "signal"):
                with self.assertRaisesRegex(ValueError, "immutable evidence"): outcome.main()
            fleet.assert_not_called()

    def test_native_only_unknown_branch_keeps_full_case_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": outcome.CANDIDATE}))
            args = types.SimpleNamespace(candidate=candidate, evidence=root / "evidence", binary_sha="a" * 64)
            def branch(args, topology, nodes, fixture, name, candidate):
                return {"topology": topology, "fixture": fixture, "branch": name, "verdict": "PASS", "cleanup": "PASS"}
            with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=args), \
                 patch.object(outcome, "verify_manager", return_value={"canonicalFleetBranchesExecuted": False}), \
                 patch.object(outcome, "run_branch", side_effect=branch), patch.object(outcome.signal, "signal"), \
                 patch.dict(os.environ, {"GITHUB_RUN_ID": "1234", "RUSTFS_NODES": "vm000 vm001 vm002 vm003"}):
                self.assertEqual(outcome.main(), 1)
            summary = json.loads((args.evidence / "healthy-summary.json").read_text())
            self.assertEqual(len(summary["results"]), 16)
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["cleanup"], "PASS")
            negative = [row for row in summary["results"] if row["branch"] == "unknown-negative"]
            self.assertEqual(len(negative), 4)
            self.assertTrue(all(row["verdict"] == "BLOCKED" and not row["fleetUnknownWindowClaimed"] for row in negative))

    def test_dirty_cleanup_stops_before_next_fixture_or_topology(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": outcome.CANDIDATE}))
            args = types.SimpleNamespace(candidate=candidate, evidence=root / "evidence", binary_sha="a" * 64)
            row = {"topology": "H1", "fixture": "DB", "branch": outcome.BRANCHES[0], "verdict": "FAIL", "cleanup": "FAIL"}
            with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=args), \
                 patch.object(outcome, "verify_manager", return_value={}), \
                 patch.object(outcome, "run_branch", return_value=row) as run, patch.object(outcome.signal, "signal"), \
                 patch.dict(os.environ, {"GITHUB_RUN_ID": "1234", "RUSTFS_NODES": "vm000 vm001 vm002 vm003"}):
                self.assertEqual(outcome.main(), 1)
            self.assertEqual(run.call_count, 1)
            summary = json.loads((args.evidence / "healthy-summary.json").read_text())
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["cleanup"], "FAIL")


if __name__ == "__main__": unittest.main()
