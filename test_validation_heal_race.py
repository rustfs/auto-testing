import json
from pathlib import Path
import unittest

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
        self.assertEqual({step["readVersionId"] for step in plan}, {"v0"})
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
                              ("heal.race-version-delete", "G03"), ("heal.race-copy-and-metadata-update", "G06")):
            self.assertIn(case_id, dispatch)
            self.assertIn(case_id, top_dispatch)
            catalog = json.loads(Path("validation-cases.json").read_text())
            source = {case["id"]: case for module in catalog["modules"] for case in module["cases"]}[case_id]
            self.assertEqual(source["probes"]["designCode"], code)


if __name__ == "__main__":
    unittest.main()
