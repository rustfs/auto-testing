import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
import tempfile

from scripts.validation_heal import race
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select


class RaceTests(unittest.TestCase):
    def test_source_case_branch_and_topology_contracts(self):
        for case_id, branches in race.CASE_BRANCHES.items():
            recipe = select(case_id)[0]
            self.assertEqual(recipe["variants"], list(branches))
            self.assertIn(case_id, AUTOMATIC_CASES)
            self.assertEqual(recipe["contract"]["probes"]["topology"], "H1,H2")

    def test_g01_plan_keeps_three_ack_ordered_overwrites(self):
        expected = [{"key": race.TARGET_KEY, "versionId": "null", "isLatest": True}]
        plan = race.operation_plan("rv-heal-0123456789abcdef", "overwrite-vs-read-repair", expected)
        self.assertEqual([step["params"]["Metadata"]["nonce"] for step in plan], ["0", "1", "2"])
        self.assertEqual(plan[-1]["nonce"], "rv-heal-0123456789abcdef/objects/0000.bin/0")

    def test_g03_selects_exact_old_or_current_version(self):
        expected = [{"key": race.TARGET_KEY, "versionId": value, "isLatest": value == "v2"}
                    for value in ("v0", "v1", "v2")]
        old = race.operation_plan("rv-heal-0123456789abcdef", "delete-old-version-vs-deep", expected)
        current = race.operation_plan("rv-heal-0123456789abcdef", "delete-current-version-vs-deep", expected)
        self.assertEqual(old[0]["params"]["VersionId"], "v0")
        self.assertEqual(current[0]["params"]["VersionId"], "v2")

    def test_g02_plan_creates_three_acknowledged_versions(self):
        expected = [{"key": race.TARGET_KEY, "versionId": value, "isLatest": value == "v2"}
                    for value in ("v0", "v1", "v2")]
        plan = race.operation_plan("rv-heal-0123456789abcdef", "read-all-versions-during-heal", expected)
        self.assertEqual(len(plan), 3)
        self.assertEqual([step['referenceKey'] for step in plan], [f'objects/{i:04d}.bin' for i in range(3)])
        self.assertTrue(all('readVersionId' not in step for step in plan))
        self.assertEqual([step["params"]["Metadata"]["nonce"] for step in plan], ["0", "1", "2"])

    def test_g06_keeps_copy_metadata_and_tag_operations_distinct(self):
        expected = [{"key": race.TARGET_KEY, "versionId": "null", "isLatest": True},
                    {"key": race.REFERENCE_KEY, "versionId": "null", "isLatest": True}]
        self.assertEqual(race.operation_plan("rv-heal-0123456789abcdef", "copy-source", expected)[0]["operation"], "copy_object")
        self.assertEqual(race.operation_plan("rv-heal-0123456789abcdef", "replace-metadata", expected)[0]["operation"], "copy_object")
        tags = race.operation_plan("rv-heal-0123456789abcdef", "update-tags", expected)
        self.assertEqual(len(tags), 16)
        self.assertEqual(tags[-1]["operation"], "put_object_tagging")
        self.assertEqual(race.target_key("copy-source"), race.G06_TARGET_KEY)
        self.assertEqual(race.reference_key("copy-source"), race.G06_REFERENCE_KEY)

    def test_g05_cannot_use_the_superseded_unversioned_fixture_or_generic_plan(self):
        recorder = Mock()
        for branch in race.G05_BRANCHES:
            with self.subTest(branch=branch), self.assertRaises(ValueError):
                race.seed_fixture(recorder, 'rv-heal-0123456789abcdef', branch)
            with self.subTest(branch=branch), self.assertRaises(ValueError):
                race.operation_plan('rv-heal-0123456789abcdef', branch, [])
        recorder.call.assert_not_called()
        for operation in ('create_multipart_upload', 'upload_part', 'complete_multipart_upload', 'abort_multipart_upload'):
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                race.foreground(recorder, [{'operation': operation}])
        recorder.call.assert_not_called()

    def test_g05_controller_delegates_all_public_branches_and_refuses_old_source_before_bootstrap(self):
        case = race.CASE_IDS[4]
        with tempfile.TemporaryDirectory() as d:
            candidate = Path(d) / 'candidate.json'
            args = SimpleNamespace(case_id=case, candidate=candidate)
            with patch('scripts.validation_heal.mpu_race_runner.run_branch', return_value={'owned': True}) as execute, \
                 patch.object(race, 'Fleet', side_effect=AssertionError('generic fleet must not run')):
                candidate.write_text(json.dumps({'source_sha': race.REFERENCE['candidateCommit']}))
                with self.assertRaisesRegex(ValueError, 'exact-source multipart'):
                    race.run_branch(args, 'H1', ['vm000'], 'upload-continue')
                execute.assert_not_called()
                candidate.write_text(json.dumps({'source_sha': race.COPY_CANDIDATE}))
                for branch in race.G05_BRANCHES:
                    self.assertEqual(race.run_branch(args, 'H1', ['vm000'], branch), {'owned': True})
                    execute.assert_called_with(args, 'H1', ['vm000'], branch)

    def test_g04_preserves_marker_and_batch_delete_semantics(self):
        expected = [{"key": race.TARGET_KEY, "versionId": value, "isLatest": value == "v2",
                     "deleteMarker": False} for value in ("v0", "v1", "v2")]
        with_marker = [*expected, {"key": race.TARGET_KEY, "versionId": "marker", "isLatest": True, "deleteMarker": True}]
        self.assertEqual(race.operation_plan("rv-heal-0123456789abcdef", "create-marker", expected)[0]["operation"],
                         "delete_object")
        self.assertEqual(race.operation_plan("rv-heal-0123456789abcdef", "delete-marker", with_marker)[0]["params"]["VersionId"],
                         "marker")
        batch = race.operation_plan("rv-heal-0123456789abcdef", "batch-version-delete", expected)[0]
        self.assertEqual(batch["operation"], "delete_objects")
        self.assertEqual(len(batch["params"]["Delete"]["Objects"]), 2)

    def test_native_branch_remains_in_denominator_for_each_case(self):
        for case_id, branches in race.CASE_BRANCHES.items():
            native = race.native_blocked({"source_sha": race.REFERENCE["candidateCommit"]}, "H1", case_id)
            rows = [{"topology": topology, "branch": branch, "verdict": "PASS", "cleanup": "PASS"}
                    for topology in ("H1", "H2") for branch in branches[:2]]
            self.assertFalse(race.complete_results([*rows, native], case_id))
            self.assertEqual(native["branch"], branches[-1])

    def test_dispatch_and_source_contract_are_present(self):
        dispatch = Path("scripts/validation_heal/run_case.sh").read_text()
        top_dispatch = Path("scripts/run_validation_case.sh").read_text()
        for case_id, code in (("heal.race-unversioned-overwrite", "G01"), ("heal.race-versioned-put", "G02"),
                              ("heal.race-version-delete", "G03"), ("heal.race-markers-and-batch-delete", "G04"),
                              ("heal.race-multipart-complete-abort", "G05"),
                              ("heal.race-copy-and-metadata-update", "G06")):
            self.assertIn(case_id, dispatch)
            self.assertIn(case_id, top_dispatch)
            catalog = json.loads(Path("validation-cases.json").read_text())
            source = {case["id"]: case for module in catalog["modules"] for case in module["cases"]}[case_id]
            self.assertEqual(source["probes"]["designCode"], code)


if __name__ == "__main__":
    unittest.main()
