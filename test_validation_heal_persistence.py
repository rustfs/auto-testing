"""F07 preserves actual corrupt responsibility and separates rescan success."""

import base64
import copy
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_heal import persistence, checkpoint, journal_fault as journal
import test_validation_heal_checkpoint as fixture_source
from test_validation_heal_checkpoint import BUCKET, TOKEN, INCARNATION, encoded


class PersistenceContractTests(unittest.TestCase):
    def test_observation_fixtures_fit_existing_inventory_inode_and_volume_limits(self):
        from scripts.validation_heal import bounded_volume
        for fixture in ('DB', 'DV'):
            for branch in (*persistence.IMPLEMENTED, 'journal-write-failure'):
                count, size = persistence.checkpoint_fixture(branch, fixture)
                data = count * (3 if fixture == 'DV' else 1)
                entries = data + ((count + 1) // 2 if fixture == 'DV' else 0)
                self.assertGreater(count, 4)
                self.assertGreater(size, 1024**2)
                self.assertLessEqual(entries, 512)
                self.assertLess(data * size, 2 * 1024**3)
                if branch == 'journal-write-failure':
                    # Conservatively allow per-version integrity and metadata,
                    # object directories, and 128 system/working inodes.
                    self.assertLess(3 * data + 2 * count + 128, bounded_volume.MAX_INODES)
                    self.assertLess(data * ((size + 1) // 2 + 16384) + 16 * 1024**2,
                                    bounded_volume.IMAGE_BYTES)

    def fixture(self, kind="truncate", *, admission_options=None, terminal_options=None):
        observed, _, _ = fixture_source.AdminCheckpointTests().fixture()
        frozen = checkpoint.owned_checkpoint(observed, TOKEN, BUCKET)
        root_value = {"schema": 3, "task_id": TOKEN, "heal_type": {
            "type": "erasure_set", "buckets": [], "set_disk_id": "pool_0_set_0", "all_buckets": True},
            "options": {"scan_mode": 2, "recursive": True}}
        if admission_options is not None: root_value["options"] = copy.deepcopy(admission_options)
        owner = {"node": "node", "disk": 1, "relativePath": journal.names("root", TOKEN)[0],
                 "value": root_value, **encoded(root_value)}
        frozen["rootAdmission"] = owner
        receipt = {"applied": True, "originalBackupRetained": True, "productRepairClaimed": False,
            "family": "checkpoint", "taskId": TOKEN, "disk": frozen["disk"],
            "relativePath": frozen["checkpoint"]["relativePath"], "kind": kind,
            "before": journal.entry(checkpoint.bytes_of(frozen["checkpoint"]))}
        corrupt = journal.transform(checkpoint.bytes_of(frozen["checkpoint"]), receipt, {"bucket": BUCKET}, None)
        receipt["injected"] = journal.entry(corrupt)
        observed["node"]["records"][4].update(present=True, **journal.entry(corrupt), rawBase64=base64.b64encode(corrupt).decode())
        observed["node"]["records"][6].update(present=True, **journal.entry(b"blocked"), rawBase64=base64.b64encode(b"blocked").decode())
        current = {"summary": "stopped", "settings": checkpoint.OPTIONS, "detail": "Failed to deserialize checkpoint",
            "outcome": {"execution": {"state": "aborted", "reason": "untraversable"}, "coverage": "partial",
                "objects": [], "objectsTruncated": False, "counters": dict(processed=0, healed=0, unchanged=0,
                    skipped=0, failed=0, unknown=0, attemptFailures=0, overflowed=False)}}
        terminal = {"schema": 1, "task_id": TOKEN, "heal_type": root_value["heal_type"],
            "options": root_value["options"], "status": {"Failed": {"error": current["detail"]}}}
        if terminal_options is not None: terminal["options"] = copy.deepcopy(terminal_options)
        report = {"schema": 1, "terminal": terminal, "outcome": current["outcome"]}
        names = journal.names("root", TOKEN)
        roots = {"node": {"readOnly": True, "quiescent": True, "family": "root", "taskId": TOKEN,
            "directoryEnumerationUsed": False, "records": [
                {"disk": disk, "relativePath": name, "present": False} for disk in range(4) for name in names]}}
        roots["node"]["records"][4].update(encoded(terminal))
        roots["node"]["records"][5].update(encoded(report))
        return observed, roots, frozen, receipt, current

    def test_corrupt_bytes_blocked_checkpoint_and_durable_failure_remain_separate_from_repair(self):
        for kind in ("truncate", "future-schema"):
            with self.subTest(kind=kind):
                proof = persistence.retained_corruption(*self.fixture(kind))
                self.assertTrue(proof["durableOriginalFailureRetained"])
                self.assertFalse(proof["originalTaskRepairClaimed"])
                self.assertFalse(proof["backupRescueUsed"])

    def recorded_options(self):
        return json.loads((Path(__file__).parent / "testdata/validation/root-terminal-options.json").read_text())

    def test_candidate_default_timeout_reconciles_original_corruption_and_completion(self):
        recorded = self.recorded_options()
        for kind in ("truncate", "future-schema"):
            data = self.fixture(kind, admission_options=recorded["admissionOptions"],
                                terminal_options=recorded["terminalOptions"])
            proof = persistence.retained_corruption(*data)
            self.assertTrue(proof["durableOriginalFailureRetained"])
            self.assertFalse(proof["originalTaskRepairClaimed"])
        roots = self.completion_roots(data[2], data[4])
        terminal = journal.decode_json(checkpoint.bytes_of(roots["node"]["records"][4]))
        terminal["options"] = recorded["terminalOptions"]
        roots["node"]["records"][4].update(encoded(terminal))
        roots["node"]["records"][5].update(encoded({"schema": 1, "terminal": terminal, "outcome": data[4]["outcome"]}))
        self.assertTrue(persistence.completed_original(roots, data[2], data[4])["sameOriginalTaskAndPhysicalOwner"])

    def test_default_timeout_never_normalizes_other_options_or_refunds_explicit_budgets(self):
        recorded = self.recorded_options()
        for change in ("shorter", "longer", "fractional", "float", "bool", "missing", "unknown-duration-field",
                       "missing-admission-timeout", "explicit-budget", "consumed-budget", "scope", "nolock", "extra-option"):
            admitted, terminal = copy.deepcopy(recorded["admissionOptions"]), copy.deepcopy(recorded["terminalOptions"])
            if change == "shorter": terminal["timeout"]["secs"] = 299
            elif change == "longer": terminal["timeout"]["secs"] = 301
            elif change == "fractional": terminal["timeout"]["nanos"] = 1
            elif change == "float": terminal["timeout"]["secs"] = 300.0
            elif change == "bool": terminal["timeout"]["nanos"] = False
            elif change == "missing": terminal.pop("timeout")
            elif change == "unknown-duration-field": terminal["timeout"]["extra"] = 0
            elif change == "missing-admission-timeout": admitted.pop("timeout")
            elif change == "explicit-budget": admitted["timeout"] = {"secs": 120, "nanos": 0}
            elif change == "consumed-budget": admitted["timeout"] = {"secs": 299, "nanos": 0}
            elif change == "scope": terminal["pool_index"] = 1
            elif change == "nolock": terminal["no_lock"] = True
            else: terminal["extra"] = "foreign"
            with self.subTest(change=change), self.assertRaises(ValueError):
                persistence.retained_corruption(*self.fixture(admission_options=admitted, terminal_options=terminal))
        for timeout in ({"secs": 120, "nanos": 0}, {"secs": 299, "nanos": 17}):
            options = {**recorded["admissionOptions"], "timeout": timeout}
            self.assertTrue(persistence.retained_corruption(*self.fixture(
                admission_options=options, terminal_options=options))["durableOriginalFailureRetained"])
        for change in ("float", "bool", "task", "scope", "options"):
            data = self.fixture(admission_options=recorded["admissionOptions"], terminal_options=recorded["terminalOptions"])
            row = data[1]["node"]["records"][5]
            report = journal.decode_json(checkpoint.bytes_of(row))
            if change == "float": report["terminal"]["options"]["timeout"]["secs"] = 300.0
            elif change == "bool": report["terminal"]["options"]["timeout"]["nanos"] = False
            elif change == "task": report["terminal"]["task_id"] = INCARNATION
            elif change == "scope": report["terminal"]["heal_type"]["set_disk_id"] = "pool_0_set_1"
            else: report["terminal"]["options"]["recursive"] = False
            row.update(encoded(report))
            with self.subTest(reportChange=change), self.assertRaises(ValueError): persistence.retained_corruption(*data)

    def test_unreviewed_candidate_options_contract_fails_before_fleet_allocation(self):
        with patch.object(persistence, "Fleet") as fleet, self.assertRaisesRegex(ValueError, "terminal-options contract"):
            persistence.run_checkpoint_fault(None, "H1", ["node"], "DB", "checkpoint-truncate", {"source_sha": "f" * 40})
        fleet.assert_not_called()

    def test_missing_rewritten_moved_or_duplicated_corruption_never_passes(self):
        for change in ("missing", "restored", "blocked", "resume", "moved", "duplicate", "partial-census"):
            data = self.fixture(); observed, _, frozen, _, _ = data
            rows = observed["node"]["records"]
            if change == "missing": rows[4] = {"disk": 1, "relativePath": rows[4]["relativePath"], "present": False}
            elif change == "restored": rows[4] = copy.deepcopy(frozen["checkpoint"])
            elif change == "blocked": rows[6].update(encoded({"diagnostic": "invented"}))
            elif change == "resume": rows[5].update(encoded({"completed": True}))
            elif change == "moved": rows[0] = {**rows[4], "disk": 0}; rows[4] = {"disk": 1, "relativePath": rows[4]["relativePath"], "present": False}
            elif change == "duplicate": observed["other"] = copy.deepcopy(observed["node"])
            else: rows.pop()
            with self.subTest(change=change), self.assertRaises(ValueError): persistence.retained_corruption(*data)

    def test_injected_bytes_must_match_original_reviewed_transformation(self):
        data = self.fixture(); data[3]["injected"] = journal.entry(b"unrelated corruption")
        with self.assertRaisesRegex(ValueError, "reviewed transformation"): persistence.retained_corruption(*data)

    def test_silent_success_cancelled_and_processed_object_claims_are_rejected(self):
        for change in ("success", "cancelled", "processed", "undocumented"):
            data = self.fixture(); current = data[-1]
            if change == "success": current.update(summary="finished"); current["outcome"]["execution"] = {"state": "completed"}
            elif change == "cancelled": current["outcome"]["execution"]["reason"] = "cancelled"
            elif change == "processed": current["outcome"]["counters"]["healed"] = 1
            else: current["detail"] = "generic failure"
            with self.subTest(change=change), self.assertRaises(ValueError): persistence.retained_corruption(*data)

    def test_original_terminal_report_identity_scope_and_failure_are_required(self):
        for change in ("missing", "success", "new-task", "scope", "report", "owner"):
            data = self.fixture(); roots = data[1]; rows = roots["node"]["records"]
            value = journal.decode_json(checkpoint.bytes_of(rows[4]))
            if change == "missing": rows[4] = {"disk": 1, "relativePath": rows[4]["relativePath"], "present": False}
            elif change == "success": value["status"] = "Completed"; rows[4].update(encoded(value))
            elif change == "new-task": value["task_id"] = INCARNATION; rows[4].update(encoded(value))
            elif change == "scope": value["heal_type"] = {"type": "cluster"}; rows[4].update(encoded(value))
            elif change == "report": rows[5].update(encoded({"schema": 1, "terminal": value, "outcome": {}}))
            else: rows[1] = {**rows[4], "disk": 0}; rows[4] = {"disk": 1, "relativePath": rows[4]["relativePath"], "present": False}
            with self.subTest(change=change), self.assertRaises(ValueError): persistence.retained_corruption(*data)

    def rescan(self):
        expected = [{"key": f"objects/{index:04d}.bin", "versionId": "null", "deleteMarker": False} for index in range(257)]
        repairs = {(expected[-1]["key"], "null")}
        current = {"summary": "finished", "settings": checkpoint.OPTIONS, "truncated": True,
            "outcome": {"execution": {"state": "completed"}, "coverage": "complete", "objectsTruncated": True,
                "counters": dict(processed=257, healed=1, unchanged=256, skipped=0, failed=0, unknown=0, attemptFailures=0, overflowed=False),
                "objects": [{"identity": {"kind": "object", "bucket": BUCKET, "object": entry["key"],
                    "versionId": None, "bucketIncarnationId": INCARNATION, "poolIndex": 0, "setIndex": 0},
                    "disposition": {"state": "verified_healthy"}} for entry in expected[:128]]}}
        return current, BUCKET, expected, repairs, INCARNATION

    def test_bounded_diagnostics_need_complete_independent_manifests_and_exact_counters(self):
        proof = persistence.rescan_outcome(*self.rescan())
        self.assertTrue(proof["completeIndependentManifestsRequired"])
        self.assertFalse(proof["boundedDiagnosticsAreCompleteManifest"])
        self.assertFalse(proof["originalFailedTaskReclassified"])
        for change in ("counter", "unknown", "duplicate", "foreign", "incarnation", "partial", "truncated"):
            data = self.rescan(); outcome = data[0]["outcome"]
            if change == "counter": outcome["counters"]["healed"] = True
            elif change == "unknown": outcome["objects"][0]["disposition"]["state"] = "unknown"
            elif change == "duplicate": outcome["objects"][-1] = copy.deepcopy(outcome["objects"][0])
            elif change == "foreign": outcome["objects"][0]["identity"]["object"] = "foreign"
            elif change == "incarnation": outcome["objects"][0]["identity"]["bucketIncarnationId"] = TOKEN
            elif change == "partial": outcome["coverage"] = "partial"
            else: outcome["objectsTruncated"] = False
            with self.subTest(change=change), self.assertRaises(ValueError): persistence.rescan_outcome(*data)

    def test_rescan_accepts_omitted_false_legacy_truncation_without_defaulting_invalid_values(self):
        data = self.rescan()
        del data[0]['truncated']
        before = copy.deepcopy(data[0])
        proof = persistence.rescan_outcome(*data)
        self.assertTrue(proof['completeIndependentManifestsRequired'])
        self.assertEqual(data[0], before)
        self.assertTrue(data[0]['outcome']['objectsTruncated'])
        for value in (None, 0, 1, 'false', []):
            invalid = self.rescan()
            invalid[0]['truncated'] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'truthful coverage'):
                persistence.rescan_outcome(*invalid)

    def test_incomplete_branches_never_enter_fleet_mutation(self):
        for branch in ("unknown", "native-persist-fence"):
            with self.subTest(branch=branch), self.assertRaisesRegex(ValueError, "cannot mutate"):
                persistence.run_checkpoint_fault(None, "H1", ["node"], "DB", branch, {})

    def test_native_branch_reuses_exact_evidence_without_claiming_fleet_or_full_case_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": "1aa9be87d18cf941e5020f19e8622c26ba4b10ac"}))
            args = ["persistence.py", "--case-id", persistence.CASE_ID, "--candidate", str(candidate),
                "--evidence", str(root), "--rc-bin", str(root / "rc"), "--tool-directory", str(root / "tools"),
                "--testing-commit", "a" * 40, "--binary-sha", "b" * 64]
            proof = {"caseId": persistence.CASE_ID, "casePrerequisiteTests": list(range(10)),
                "canonicalFleetBranchesExecuted": False, "canonicalCasePassClaimed": False}
            def passed(args, topology, nodes, fixture, branch, candidate):
                return {"topology": topology, "fixture": fixture, "branch": branch, "verdict": "PASS", "cleanup": "PASS"}
            with patch("sys.argv", args), patch.dict(os.environ, {"GITHUB_RUN_ID": "12345"}), \
                 patch.object(persistence, "verify_manager", return_value=proof) as verify, \
                 patch.object(persistence, "run_mrf_checksum", side_effect=lambda args, topology, nodes, fixture, candidate: {
                     "topology": topology, "fixture": fixture, "branch": "mrf-checksum", "verdict": "PASS", "cleanup": "PASS"}) as mrf_mutate, \
                 patch.object(persistence, "run_checkpoint_fault", side_effect=passed) as mutate:
                self.assertEqual(persistence.main(), 1)
            verify.assert_called_once()
            self.assertEqual(mutate.call_count, 16)
            self.assertEqual(mrf_mutate.call_count, 4)
            summary = json.loads((root / "healthy-summary.json").read_text())
            self.assertEqual(len(summary["results"]), 24)
            self.assertFalse(summary["complete"])
            self.assertFalse(summary["canonicalCasePassClaimed"])
            self.assertTrue(summary["diagnosticBranchesPassed"])
            prerequisites = [row for row in summary["results"] if row["branch"] == "native-persist-fence"]
            self.assertEqual(len(prerequisites), 4)
            for row in prerequisites:
                self.assertEqual(row["verdict"], "PREREQUISITE_PASS")
                self.assertIsNone(row["topology"]); self.assertIsNone(row["fixture"])
                self.assertFalse(row["fleetBranchExecuted"])
                self.assertFalse(row["individualFsyncHooksClaimed"])
                self.assertFalse(row["physicalPowerLossClaimed"])
                self.assertFalse(row["requiredNativeCoverageComplete"])
                self.assertEqual(row["nativeProof"], proof)
            self.assertEqual(sum(row["verdict"] == "BLOCKED" for row in summary["results"]), 0)
            def failed(*values):
                row = passed(*values)
                if row["branch"] == "stale-generation": row["verdict"] = "FAIL"
                return row
            failed_directory = root / "failed-diagnostic"; failed_directory.mkdir()
            failed_args = args.copy(); failed_args[failed_args.index("--evidence") + 1] = str(failed_directory)
            with patch("sys.argv", failed_args), patch.dict(os.environ, {"GITHUB_RUN_ID": "12345"}), \
                 patch.object(persistence, "verify_manager", return_value=proof), \
                 patch.object(persistence, "run_mrf_checksum", side_effect=lambda args, topology, nodes, fixture, candidate: {
                     "topology": topology, "fixture": fixture, "branch": "mrf-checksum", "verdict": "PASS", "cleanup": "PASS"}), \
                 patch.object(persistence, "run_checkpoint_fault", side_effect=failed):
                self.assertEqual(persistence.main(), 1)
            failed_summary = json.loads((failed_directory / "healthy-summary.json").read_text())
            self.assertEqual(failed_summary["cleanup"], "PASS")
            self.assertFalse(failed_summary["diagnosticBranchesPassed"])
            self.assertFalse(failed_summary["canonicalCasePassClaimed"])

    def stale(self):
        observed, value, _ = fixture_source.AdminCheckpointTests().fixture()
        value["admin"]["outcome"]["counters"].update(healed=0, unchanged=2)
        observed["node"]["records"][4].update(encoded(value, signed=True))
        early = checkpoint.owned_checkpoint(observed, TOKEN, BUCKET)
        early["captureId"] = "F07-earlier-generation"
        value["checkpoint_time"] += 1; value["current_object_index"] += 1
        value["admin"]["outcome"]["counters"]["processed"] += 1
        value["admin"]["outcome"]["counters"]["unchanged"] += 1
        observed["node"]["records"][4].update(encoded(value, signed=True))
        later = checkpoint.owned_checkpoint(observed, TOKEN, BUCKET)
        later["rootAdmission"] = self.fixture()[2]["rootAdmission"]
        receipt = {"family": "checkpoint", "taskId": TOKEN, "disk": 1,
            "relativePath": later["checkpoint"]["relativePath"], "kind": "stale-generation",
            "captureId": early["captureId"], "before": journal.entry(checkpoint.bytes_of(later["checkpoint"])),
            "injected": journal.entry(checkpoint.bytes_of(early["checkpoint"])),
            "capturedGeneration": journal.entry(checkpoint.bytes_of(early["checkpoint"])),
            "applied": True, "originalBackupRetained": True, "productRepairClaimed": False}
        return early, later, receipt

    def completion_roots(self, frozen, current):
        roots = self.fixture()[1]
        terminal = {"schema": 1, "task_id": TOKEN, "heal_type": frozen["rootAdmission"]["value"]["heal_type"],
            "options": frozen["rootAdmission"]["value"]["options"], "status": "Completed"}
        roots["node"]["records"][4].update(encoded(terminal))
        roots["node"]["records"][5].update(encoded({"schema": 1, "terminal": terminal, "outcome": current["outcome"]}))
        return roots

    def test_stale_valid_generation_is_exact_original_progress_without_cas_or_rejection_claim(self):
        proof = persistence.stale_receipt(*self.stale())
        self.assertTrue(proof["exactEarlierProductBytesInstalled"])
        self.assertFalse(proof["oldCheckpointManagerCasClaimed"])
        self.assertFalse(proof["staleRecordRejectionClaimed"])
        for change in ("same-progress", "owner", "capture", "injected", "backup", "already-healed"):
            early, later, receipt = self.stale()
            if change == "same-progress": later = copy.deepcopy(early)
            elif change == "owner": later["disk"] = 2
            elif change == "capture": receipt["captureId"] = "F07-other"
            elif change == "injected": receipt["injected"] = receipt["before"]
            elif change == "backup": receipt["originalBackupRetained"] = False
            else: early["value"]["admin"]["outcome"]["counters"].update(healed=1, unchanged=1)
            with self.subTest(change=change), self.assertRaises(ValueError): persistence.stale_receipt(early, later, receipt)

    def test_original_completion_needs_exact_admission_owner_scope_and_report(self):
        _, frozen, _ = self.stale(); current = self.rescan()[0]
        proof = persistence.completed_original(self.completion_roots(frozen, current), frozen, current)
        self.assertTrue(proof["sameOriginalTaskAndPhysicalOwner"])
        for change in ("runnable", "missing", "moved", "scope", "different-task", "different-report"):
            roots = self.completion_roots(frozen, current); rows = roots["node"]["records"]
            if change == "runnable": rows[3].update(encoded(frozen["rootAdmission"]["value"]))
            elif change == "missing": rows[4] = {key: rows[4][key] for key in ("disk", "relativePath")}; rows[4]["present"] = False
            elif change == "moved": rows[1] = {**rows[4], "disk": 0}; rows[4] = {key: rows[4][key] for key in ("disk", "relativePath")}; rows[4]["present"] = False
            elif change == "different-report": rows[5].update(encoded({"schema": 1, "terminal": {}, "outcome": current["outcome"]}))
            else:
                value = journal.decode_json(checkpoint.bytes_of(rows[4]))
                if change == "scope": value["heal_type"] = {"type": "cluster"}
                else: value["task_id"] = INCARNATION
                rows[4].update(encoded(value))
            with self.subTest(change=change), self.assertRaises(ValueError): persistence.completed_original(roots, frozen, current)

    def test_rejected_stale_generation_retains_original_bytes_and_failed_responsibility(self):
        early, frozen, receipt = self.stale()
        observed, roots, _, _, current = self.fixture()
        observed["node"]["records"][4] = copy.deepcopy(early["checkpoint"])
        proof = persistence.retained_corruption(observed, roots, frozen, receipt, current, earlier=early)
        self.assertTrue(proof["durableOriginalFailureRetained"])
        self.assertFalse(proof["originalTaskRepairClaimed"])
        self.assertFalse(proof["backupRescueUsed"])
        with self.assertRaises(ValueError): persistence.retained_corruption(observed, roots, frozen, receipt, current)

    def write_failure(self):
        observed, _, frozen, _, _ = self.fixture()
        value = frozen["value"]
        value["admin"]["outcome"]["counters"].update(healed=0, unchanged=2)
        frozen["checkpoint"].update(encoded(value, signed=True))
        observed["node"]["records"][4] = copy.deepcopy(frozen["checkpoint"])
        observed["node"]["records"][6] = {"disk": 1, "relativePath": journal.names("checkpoint", TOKEN)[2], "present": False}
        detail = "Task execution failed: Failed to save checkpoint: No space left on device (os error 28)"
        current = {"summary": "stopped", "settings": checkpoint.OPTIONS, "detail": detail,
            "outcome": {**copy.deepcopy(value["admin"]["outcome"]), "execution": {"state": "aborted", "reason": "untraversable"}}}
        root_observed, root_value = fixture_source.AdminCheckpointTests().root_fixture()
        frozen["rootAdmission"] = checkpoint.owned_root_admission(root_observed, TOKEN)
        events = {"node": [{"target": "rustfs::heal::task", "message": "Heal task failed", "task_id": TOKEN, "error": detail}]}
        return observed, root_observed, frozen, current, events

    def test_actual_write_error_keeps_original_healthy_generation_and_admitted_owner(self):
        proof = persistence.retained_write_failure(*self.write_failure())
        self.assertTrue(proof["lastDurableProgressRetained"])
        self.assertTrue(proof["originalAdmissionRetained"])
        self.assertFalse(proof["failedWriteAcknowledged"])
        self.assertFalse(proof["originalFailureReclassified"])
        for change in ("success", "wrong-error", "invented-progress", "missing-checkpoint", "rewrite-resume", "missing-owner", "wrong-task-event"):
            observed, roots, frozen, current, events = self.write_failure()
            if change == "success": current["summary"] = "finished"
            elif change == "wrong-error": current["detail"] = "Failed to save checkpoint: Permission denied"
            elif change == "invented-progress": current["outcome"]["counters"]["processed"] += 1
            elif change == "missing-checkpoint": observed["node"]["records"][4]["present"] = False
            elif change == "rewrite-resume": observed["node"]["records"][5].update(encoded({"completed": True}))
            elif change == "missing-owner": roots["node"]["records"][3]["present"] = False
            else: events["node"][0]["task_id"] = INCARNATION
            with self.subTest(change=change), self.assertRaises(ValueError): persistence.retained_write_failure(observed, roots, frozen, current, events)

    def test_known_write_failure_can_have_explicit_failed_terminal_without_silent_success(self):
        observed, roots, frozen, current, events = self.write_failure()
        roots["node"]["records"][3] = {"disk": 1, "relativePath": journal.names("root", TOKEN)[0], "present": False}
        terminal = {"schema": 1, "task_id": TOKEN, "heal_type": frozen["rootAdmission"]["value"]["heal_type"],
            "options": frozen["rootAdmission"]["value"]["options"], "status": {"Failed": {"error": current["detail"]}}}
        roots["node"]["records"][4].update(encoded(terminal))
        roots["node"]["records"][5].update(encoded({"schema": 1, "terminal": terminal, "outcome": current["outcome"]}))
        proof = persistence.retained_write_failure(observed, roots, frozen, current, events)
        self.assertTrue(proof["originalResponsibilityRetained"])
        self.assertFalse(proof["originalAdmissionRetained"])
        terminal["status"] = "Completed"; roots["node"]["records"][4].update(encoded(terminal))
        with self.assertRaises(ValueError): persistence.retained_write_failure(observed, roots, frozen, current, events)

    def test_checkpoint_write_failure_terminal_keeps_the_source_default_and_exact_scope(self):
        recorded = self.recorded_options()
        for change in (None, "timeout", "scope", "report"):
            observed, roots, frozen, current, events = self.write_failure()
            admission = frozen["rootAdmission"]["value"]
            admission["options"] = copy.deepcopy(recorded["admissionOptions"])
            frozen["rootAdmission"].update(**encoded(admission))
            roots["node"]["records"][3] = {"disk": 1, "relativePath": journal.names("root", TOKEN)[0], "present": False}
            terminal = {"schema": 1, "task_id": TOKEN, "heal_type": admission["heal_type"],
                "options": copy.deepcopy(recorded["terminalOptions"]), "status": {"Failed": {"error": current["detail"]}}}
            if change == "timeout": terminal["options"]["timeout"]["secs"] = 301
            elif change == "scope": terminal["options"]["set_index"] = 1
            report = {"schema": 1, "terminal": copy.deepcopy(terminal), "outcome": current["outcome"]}
            if change == "report": report["terminal"]["options"]["timeout"]["secs"] = 299
            roots["node"]["records"][4].update(encoded(terminal))
            roots["node"]["records"][5].update(encoded(report))
            if change is None:
                self.assertTrue(persistence.retained_write_failure(observed, roots, frozen, current, events)["originalResponsibilityRetained"])
            else:
                with self.subTest(change=change), self.assertRaises(ValueError):
                    persistence.retained_write_failure(observed, roots, frozen, current, events)

    def test_write_failure_can_consume_root_timeout_without_refunding_or_changing_intent(self):
        for change in (None, "refunded", "removed", "scope", "retry", "owner"):
            observed, roots, frozen, current, events = self.write_failure()
            before = copy.deepcopy(frozen["rootAdmission"]["value"])
            before["options"]["timeout"] = {"secs": 120, "nanos": 0}
            frozen["rootAdmission"].update(value=before, **encoded(before))
            after = copy.deepcopy(before)
            after["options"]["timeout"] = {"secs": 90, "nanos": 0}
            if change == "refunded": after["options"]["timeout"]["secs"] = 121
            elif change == "removed": after["options"].pop("timeout")
            elif change == "scope": after["options"]["pool_index"] = 1
            elif change == "retry": after["retry_attempts"] = 1
            roots["node"]["records"][3].update(encoded(after))
            if change == "owner":
                roots["node"]["records"][0].update(encoded(after))
                roots["node"]["records"][3] = {"disk": 1, "relativePath": journal.names("root", TOKEN)[0], "present": False}
            if change is None:
                self.assertTrue(persistence.retained_write_failure(observed, roots, frozen, current, events)["originalResponsibilityRetained"])
            else:
                with self.subTest(change=change), self.assertRaises(ValueError):
                    persistence.retained_write_failure(observed, roots, frozen, current, events)

    def test_write_failure_live_census_requires_proven_frozen_processes(self):
        for change in (None, "serviceFrozen", "stable", "processBound", "independentReads"):
            observed, roots, frozen, current, events = self.write_failure()
            for census in (observed["node"], roots["node"]):
                census.update(quiescent=False, stable=True, serviceFrozen=True, processBound=True, independentReads=2)
            if change is None:
                persistence.retained_write_failure(observed, roots, frozen, current, events)
            else:
                roots["node"][change] = 1 if change == "independentReads" else False
                with self.subTest(change=change), self.assertRaises(ValueError):
                    persistence.retained_write_failure(observed, roots, frozen, current, events)

    def test_write_refusal_release_rescan_and_real_original_retry_have_distinct_evidence(self):
        for change in (None, "wrong-mount-witness", "release-failure", "late-export-failure", "pending-owner"):
            observed, roots, frozen, failed, events = self.write_failure()
            expected = [{"key": f"objects/{i:04d}.bin", "versionId": "null", "deleteMarker": False} for i in range(3)]
            def finished(heal):
                return {"summary": "finished", "settings": checkpoint.OPTIONS, "truncated": False,
                    "outcome": {"execution": {"state": "completed"}, "coverage": "complete", "objectsTruncated": False,
                        "counters": dict(processed=3, healed=heal, unchanged=3-heal, skipped=0, failed=0, unknown=0, attemptFailures=0, overflowed=False),
                        "objects": [{"identity": {"kind": "object", "bucket": BUCKET, "object": entry["key"], "versionId": None,
                            "bucketIncarnationId": INCARNATION, "poolIndex": 0, "setIndex": 0},
                            "disposition": {"state": "repaired" if heal and i == 2 else "verified_healthy"}} for i, entry in enumerate(expected)]}}
            rescanned, final = finished(1), finished(0)
            final_roots = self.completion_roots(frozen, final)
            identity = {field: field for field in persistence.PROCESS_FIELDS}; identity["pid"] = 50
            target = {"node": "node", "disk": 1, "relativePath": "bound/part.1", "key": expected[-1]["key"], "versionId": "null"}
            repaired = {"node": {"files": [{"disk": 1, "path": "bound/part.1", "sha256": "a" * 64}], "formats": []}}
            issued = []
            class WriteFleet:
                nodes, frozen_processes, faults = ["node"], {}, [("node", {"faultId": "F07-shard-0"})]
                paused = False
                def start(self): issued.append("start")
                def stop(self): issued.append("stop")
                def record(self, label, value):
                    issued.append(label)
                    if change == "late-export-failure" and label == "complete-original-journal-write-reconciliation": raise OSError("export failed")
                    return value
                def snapshot(self, label, **kwargs):
                    if label == "original-journal-write-failure-physical": return {"node": {"files": [], "formats": []}}
                    return repaired
                def remote(self, node, action, **params):
                    issued.append(action)
                    if action == "bounded-volumes-fill": return {"quiescent": True, "hostSharedMountRemounted": False,
                        "candidateProcessWitnessRequired": True, "changes": [{"actualErrno": 28, "freeBlocks": 0}] * 4}
                    if action == "bounded-volumes-witness": return {"sameOriginalProcess": True, "hostSharedMountRemounted": False,
                        "disks": [{"writeErrno": 28, "sameServiceUidAndMountNamespace": change != "wrong-mount-witness"}] * 4}
                    if action == "bounded-volumes-release": return {"sameOriginalProcess": True, "hostSharedMountRemounted": False,
                        "disks": [{"writeErrno": 28 if change == "release-failure" else 0, "freeBlocks": 1}] * 4}
                    if action == "process": return {**identity, "state": "T" if self.paused else "S"}
                    if action == "process-signal": self.paused = params["signal"] == "SIGSTOP"; return {}
                    raise AssertionError(action)
            fleet = WriteFleet()
            active = [(checkpoint.PATH, TOKEN)]
            def records(fleet, token, label, **kwargs):
                if label == "retained-write-admission-after-reads": return final_roots
                return roots if kwargs.get("family") == "root" else observed
            def run_task(*args, **kwargs): kwargs["admissions"].append({"clientToken": INCARNATION}); return rescanned
            def recover_pending(*args):
                issued.append("recover-pending-original-before-new-task")
                if change == "pending-owner": raise ValueError("original durable scope remains owned")
                return None
            window = SimpleNamespace(events=events, capture=lambda: events)
            with patch.object(persistence, "JournalWindow", return_value=window), \
                 patch.object(persistence, "recover_write_before_rescan", create=True, side_effect=recover_pending), \
                 patch.object(persistence, "recovery_status", side_effect=[failed, failed, final]), \
                 patch.object(persistence.checkpoints, "records", side_effect=records), \
                 patch.object(persistence, "validate_repaired_physical"), patch.object(persistence, "verify_proof"), \
                 patch.object(persistence, "idle_fleet"), patch.object(persistence, "run_task", side_effect=run_task), \
                 patch.object(persistence, "status", return_value=final), \
                 patch.object(persistence, "Probe", return_value=SimpleNamespace(inventory=lambda *a, **kw: {})), \
                 patch.object(persistence, "verify_seed"), patch.object(persistence, "compare_inventories"):
                args = (fleet, None, {"endpoints": {"node1": "http://owned"}}, expected, {}, repaired, [target], frozen, active)
                if change is None:
                    result = persistence.journal_write_failure(*args)
                    self.assertTrue(result["originalWriteFailure"]["lastDurableProgressRetained"])
                    self.assertTrue(result["originalAfterRestart"]["realSameTaskReplayAfterSeparateRescan"])
                    self.assertFalse(result["originalAfterRestart"]["separateRescanIsOriginalTaskCompletion"])
                    self.assertFalse(result["canonicalCasePassClaimed"])
                    self.assertFalse(fleet.faults)
                else:
                    with self.assertRaises((ValueError, OSError)): persistence.journal_write_failure(*args)
                    self.assertTrue(fleet.faults)
                    if change == "wrong-mount-witness": self.assertNotIn("bounded-volumes-release", issued)
                    if change == "pending-owner":
                        self.assertIn("recover-pending-original-before-new-task", issued)
                        self.assertNotIn("journal-rescan-complete-physical-before-any-GET", issued)

    def test_pending_write_owner_recovers_before_new_rescan_without_reclassifying_first_failure(self):
        cases = (None, "failed-terminal", "still-owned", "wrong-terminal", "streaming-error", "rescan-heals",
                 "same-token", "peer-disagrees", "read-repair", "changed-terminal", "export-error",
                 "pre-recovery-drift", "recovery-timeout", "late-recovery-completion")
        for change in cases:
            with self.subTest(change=change):
                observed, failed_roots, frozen, failed, events = self.write_failure()
                current, _, expected, _, _ = self.rescan()
                final_roots = self.completion_roots(frozen, current)
                failed_terminal = copy.deepcopy(failed_roots)
                failed_terminal["node"]["records"][3] = {"disk": 1, "relativePath": journal.names("root", TOKEN)[0], "present": False}
                terminal = {"schema": 1, "task_id": TOKEN, "heal_type": frozen["rootAdmission"]["value"]["heal_type"],
                    "options": frozen["rootAdmission"]["value"]["options"], "status": {"Failed": {"error": failed["detail"]}}}
                failed_terminal["node"]["records"][4].update(encoded(terminal))
                failed_terminal["node"]["records"][5].update(encoded({"schema": 1, "terminal": terminal, "outcome": failed["outcome"]}))
                first_failed = copy.deepcopy(failed)
                failure_scene = {"node": {"files": [], "formats": []}}
                healed = {"node": {"files": [{"disk": 1, "path": "bound/part.1", "sha256": "a" * 64}], "formats": []}}
                issued, exports = [], {}
                class ReplayFleet:
                    faults = [("node", {"faultId": "F07-shard-0"})]
                    def start(self): issued.append("start")
                    def stop(self): issued.append("stop")
                    def record(self, label, value):
                        issued.append(label)
                        if change == "export-error" and label == "complete-original-journal-write-reconciliation": raise OSError("lost reconciliation export")
                        exports[label] = copy.deepcopy(value); return value
                    def snapshot(self, label, **kwargs):
                        issued.append(label)
                        if label == "write-failure-before-original-recovery" and change == "pre-recovery-drift": return copy.deepcopy(healed)
                        if label in ("write-failure-before-original-recovery", "write-original-failed-recovery-physical"): return copy.deepcopy(failure_scene)
                        if label == "write-replay-after-rescan-and-reads" and change == "read-repair": return copy.deepcopy(failure_scene)
                        return copy.deepcopy(healed)
                fleet = ReplayFleet()
                def records(fleet, token, label, **kwargs):
                    self.assertEqual(token, TOKEN)
                    if kwargs.get("family") != "root": return copy.deepcopy(observed)
                    if label == "write-failure-root-before-recovery": return copy.deepcopy(failed_roots)
                    if change == "still-owned": return copy.deepcopy(failed_roots)
                    if change == "failed-terminal": return copy.deepcopy(failed_terminal)
                    result = copy.deepcopy(final_roots)
                    if change == "wrong-terminal" or change == "changed-terminal" and label == "retained-write-original-completion":
                        report = journal.decode_json(checkpoint.bytes_of(result["node"]["records"][5]))
                        if change == "wrong-terminal": report["outcome"]["counters"]["healed"] = 0
                        else: report["unrelated"] = "changed after rescan"
                        result["node"]["records"][5].update(encoded(report))
                    return result
                def run_task(*args, **kwargs):
                    self.assertIn("write-original-replay-complete-physical-proof", issued)
                    self.assertIn("write-original-replay-full-shard-integrity", issued)
                    kwargs["admissions"].append({"clientToken": TOKEN if change == "same-token" else INCARNATION})
                    issued.append("independent-new-rescan")
                    rescan = copy.deepcopy(current)
                    if change != "rescan-heals": rescan["outcome"]["counters"].update(healed=0, unchanged=257)
                    return rescan
                def verify(*args):
                    issued.append("write-original-replay-full-shard-integrity")
                    if change == "streaming-error": raise ValueError("actual cohort failed streaming proof")
                proof = persistence.retained_write_failure(observed, failed_roots, frozen, failed, events)
                target = {"node": "node", "disk": 1, "relativePath": "bound/part.1", "key": expected[-1]["key"], "versionId": "null"}
                active = []
                with patch.object(persistence.checkpoints, "records", side_effect=records), \
                     patch.object(persistence, "validate_repaired_physical"), patch.object(persistence, "verify_proof", side_effect=verify), \
                     patch.object(persistence, "recovery_status", return_value={"summary": "running"} if change == "recovery-timeout" else failed if change in ("failed-terminal", "still-owned") else current), \
                     patch.object(persistence, "run_task", side_effect=run_task), \
                     patch.object(persistence, "status", return_value=failed if change == "peer-disagrees" else current), \
                     patch.object(persistence, "Probe", return_value=SimpleNamespace(inventory=lambda *a, **kw: {})), \
                     patch.object(persistence, "verify_seed"), patch.object(persistence, "compare_inventories"), \
                     patch.object(persistence.time, "monotonic", return_value=0,
                                  side_effect=[0, .01, 181] if change in ("recovery-timeout", "late-recovery-completion") else None):
                    args = (fleet, None, {"endpoints": {"node1": "http://owned"}}, expected, {}, healed, [target],
                            frozen, failed, proof, failure_scene, [target], events, active)
                    if change in (None, "failed-terminal"):
                        result = persistence.recover_write_before_rescan(*args)
                        if change is None:
                            self.assertTrue(result["originalAfterRestart"]["realSameTaskReplayBeforeSeparateRescan"])
                            self.assertFalse(result["originalAfterRestart"]["separateRescanIsOriginalTaskCompletion"])
                            self.assertFalse(result["canonicalCasePassClaimed"])
                            self.assertFalse(fleet.faults)
                            self.assertEqual(exports["complete-original-journal-write-reconciliation"]["firstFailedAttempt"], first_failed)
                        else:
                            self.assertIsNone(result); self.assertTrue(fleet.faults)
                            self.assertNotIn("independent-new-rescan", issued)
                    else:
                        with self.assertRaises((ValueError, OSError)): persistence.recover_write_before_rescan(*args)
                        self.assertTrue(fleet.faults)
                        if change in ("still-owned", "wrong-terminal", "streaming-error"):
                            self.assertNotIn("independent-new-rescan", issued)
                self.assertEqual(failed, first_failed)
                self.assertEqual(exports["original-write-failure-before-recovery"]["firstFailedAttempt"], first_failed)

    def test_successful_replay_cannot_discard_rescue_before_manifest_terminal_and_evidence_agree(self):
        for change in (None, "read-repair", "late-archive-failure", "rescan-relabels-original"):
            early, frozen, receipt = self.stale(); current, _, expected, _, _ = self.rescan()
            roots = self.completion_roots(frozen, current)
            repaired = {"node": {"files": [{"disk": 1, "path": "bound/part.1", "sha256": "a" * 64}], "formats": []}}
            events, active = [], [(checkpoint.PATH, TOKEN)]
            class ReplayFleet:
                journal_faults, faults = [("node", {"faultId": "F07-stale-generation"})], [("node", {"disk": 1})]
                def start(self): pass
                def stop(self): pass
                def record(self, label, value):
                    events.append(label)
                    if change == "late-archive-failure" and label == "complete-original-generation-reconciliation": raise OSError("evidence write failed")
                    return value
                def snapshot(self, label, **kwargs):
                    scene = copy.deepcopy(repaired)
                    if change == "read-repair" and label == "physical-after-rescan-and-business-reads": scene["node"]["files"] = []
                    return scene
            fleet = ReplayFleet()
            rescan = copy.deepcopy(current); rescan["outcome"]["counters"].update(healed=0, unchanged=257)
            def run_task(*args, **kwargs):
                kwargs["admissions"].append({"clientToken": TOKEN if change == "rescan-relabels-original" else INCARNATION})
                return rescan
            target = {"node": "node", "disk": 1, "relativePath": "bound/part.1",
                      "key": expected[-1]["key"], "versionId": "null"}
            binding = {"endpoints": {"node1": "http://owned"}}
            with patch.object(persistence, "rescan_outcome", wraps=persistence.rescan_outcome), \
                 patch.object(persistence.checkpoints, "records", return_value=roots), patch.object(persistence, "idle_fleet"), \
                 patch.object(persistence, "validate_repaired_physical"), patch.object(persistence, "verify_proof"), \
                 patch.object(persistence, "run_task", side_effect=run_task), \
                 patch.object(persistence, "recovery_status", return_value=current), patch.object(persistence, "status", return_value=current), \
                 patch.object(persistence, "Probe", return_value=SimpleNamespace(inventory=lambda *a, **kw: {})), \
                 patch.object(persistence, "verify_seed"), patch.object(persistence, "compare_inventories"):
                args = (fleet, None, binding, expected, {}, repaired, [target], frozen, early, receipt,
                        {"node": {"files": [], "formats": []}}, current, active)
                if change is None:
                    result = persistence.successful_stale_replay(*args)
                    self.assertFalse(result["canonicalCasePassClaimed"])
                    self.assertFalse(result["staleRecordRejectionClaimed"])
                    self.assertIn("complete-original-generation-reconciliation", events)
                    self.assertFalse(fleet.journal_faults); self.assertFalse(fleet.faults)
                else:
                    with self.assertRaises((ValueError, OSError)): persistence.successful_stale_replay(*args)
                    self.assertTrue(fleet.journal_faults); self.assertTrue(fleet.faults)

    def interrupted_injection(self, cleanup, mutation=None):
        frozen = self.write_failure()[2] if mutation == "fill" else self.fixture()[2]
        snapshots = {"node": {"files": [], "formats": []}}
        captured = {}

        class InterruptedFleet:
            def __init__(self, *args, **kwargs):
                self.faults, self.journal_faults = [], []
                self.nodes = ["node"]
                captured["fleet"] = self

            def record(self, label, value): return value
            def deploy(self, *args, **kwargs): pass
            def start(self): pass
            def stop(self): pass
            def snapshot(self, *args, **kwargs): return snapshots

            def remote(self, node, action, **params):
                if action == "journal-apply":
                    if mutation == "runtime": return {}
                    # The node may have committed the fault before the ACK was lost.
                    raise OSError("journal injection acknowledgement lost")
                if action == "process": return {"settings": {"RUSTFS_HEAL_TASK_TIMEOUT_SECS": "299"}}
                if action == "bounded-volumes-fill": raise OSError("private journal fill acknowledgement lost")
                return {}

            def cleanup(self, *, preserve_namespaces=False):
                captured["cleanupFaults"] = copy.deepcopy(self.faults)
                captured["cleanupJournals"] = copy.deepcopy(self.journal_faults)
                captured["preserveNamespaces"] = preserve_namespaces
                return cleanup

        class FixtureProbe:
            def __init__(self, *args): pass
            def inventory(self, *args, **kwargs): return {}

        target = {"node": "node", "disk": 1, "relativePath": "bound/part.1", "sha256": "0" * 64}
        expected = [{"key": f"objects/{index:04d}.bin", "versionId": "null", "deleteMarker": False}
                    for index in range(257)]
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            rc = directory / "rc"; rc.write_bytes(b"test-rc")
            args = SimpleNamespace(evidence=directory, tool_directory=directory,
                                   testing_commit="0" * 40, binary_sha="0" * 64, rc_bin=rc)
            with patch.dict(persistence.os.environ, {"GITHUB_RUN_ID": "1"}), \
                 patch.object(persistence, "Fleet", InterruptedFleet), patch.object(persistence, "Probe", FixtureProbe), \
                 patch.object(persistence, "verify_inspector", return_value={}), \
                 patch.object(persistence, "validate_topology", return_value={}), \
                 patch.object(persistence, "seed", return_value=expected,
                              side_effect=OSError("seed acknowledgement lost") if mutation == "seed" else None), \
                 patch.object(persistence, "verify_seed", return_value={}), \
                 patch.object(persistence, "idle_fleet"), patch.object(persistence, "map_target", return_value=target), \
                 patch.object(persistence, "verify_proof"), patch.object(persistence, "validate_fault"), \
                 patch.object(persistence.checkpoints, "admit", return_value=TOKEN,
                              side_effect=OSError("admission acknowledgement lost") if mutation == "admission" else None), \
                 patch.object(persistence.checkpoints, "freeze", return_value=(frozen, {})), \
                 patch.object(persistence, "JournalWindow"), \
                 patch.object(persistence.checkpoints, "stop_for_fault", return_value=frozen):
                result = persistence.run_checkpoint_fault(args, "H1", ["node"], "DB",
                    "journal-write-failure" if mutation == "fill" else "checkpoint-truncate",
                    {"source_sha": "1aa9be87d18cf941e5020f19e8622c26ba4b10ac", "package_sha256": "0" * 64})
        return result, captured

    def test_lost_injection_ack_keeps_original_journal_and_shard_rescue_registered(self):
        result, captured = self.interrupted_injection(True)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["cleanup"], "PASS")
        self.assertEqual(result["failure"]["message"], "journal injection acknowledgement lost")
        self.assertEqual(len(captured["cleanupFaults"]), 4)
        self.assertEqual(len(captured["cleanupJournals"]), 1)
        self.assertEqual(captured["cleanupJournals"][0][1]["taskId"], TOKEN)
        self.assertNotIn("separateRescan", result)

    def test_restart_runtime_change_preserves_rescue_and_refuses_terminal_normalization(self):
        with patch.object(persistence, "recovery_status") as queried:
            result, captured = self.interrupted_injection(True, "runtime")
        self.assertEqual((result["verdict"], result["cleanup"]), ("FAIL", "PASS"))
        self.assertIn("runtime timeout witness", result["failure"]["message"])
        self.assertEqual(len(captured["cleanupJournals"]), 1)
        self.assertEqual(len(captured["cleanupFaults"]), 4)
        queried.assert_not_called()

    def test_failed_cleanup_preserves_dirty_result_after_lost_injection_ack(self):
        result, captured = self.interrupted_injection(False)
        self.assertEqual((result["verdict"], result["cleanup"]), ("FAIL", "FAIL"))
        self.assertEqual(len(captured["cleanupJournals"]), 1)
        self.assertNotIn("completePhysicalAndS3ManifestsVerified", result)

    def test_unknown_fixture_or_original_admission_preserves_namespace_even_if_cleanup_reports_success(self):
        for mutation in ("seed", "admission"):
            with self.subTest(mutation=mutation):
                result, captured = self.interrupted_injection(True, mutation)
                self.assertEqual((result["verdict"], result["cleanup"]), ("FAIL", "FAIL"))
                self.assertTrue(captured["preserveNamespaces"])
                self.assertIn("unresolvedMutation", result)
                self.assertNotIn("separateRescan", result)

    def test_lost_private_fill_ack_keeps_shard_rescue_and_failure_before_any_positive_rescan(self):
        result, captured = self.interrupted_injection(False, "fill")
        self.assertEqual((result["verdict"], result["cleanup"]), ("FAIL", "FAIL"))
        self.assertEqual(result["failure"]["message"], "private journal fill acknowledgement lost")
        self.assertEqual(len(captured["cleanupFaults"]), 4)
        self.assertFalse(captured["cleanupJournals"])
        self.assertNotIn("separateRescan", result)


if __name__ == "__main__": unittest.main()
