import copy
import json
from pathlib import Path
import unittest

from scripts.validation_heal import nolock
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select


class NolockTests(unittest.TestCase):
    def test_source_branches_and_fixture_mapping_are_preserved(self):
        recipe = select(nolock.CASE_ID)[0]
        self.assertEqual(recipe["variants"], list(nolock.BRANCHES))
        self.assertIn(nolock.CASE_ID, AUTOMATIC_CASES)
        self.assertEqual(nolock.REFERENCE["requested"]["nolock"], True)
        self.assertEqual(nolock.REFERENCE["effective"]["nolock"], False)
        self.assertIsNone(nolock.REFERENCE["requested"]["pool"])
        self.assertIsNone(nolock.REFERENCE["effective"]["set"])

    def test_settings_require_exact_lock_safe_normalization(self):
        status = {"httpStatus": 200, "response": {"summary": "running", "settings": nolock.REFERENCE["effective"]}}
        self.assertEqual(nolock.settings(status), nolock.REFERENCE["effective"])
        for changed in (
            {**nolock.REFERENCE["effective"], "nolock": True},
            {**nolock.REFERENCE["effective"], "recursive": False},
            {**nolock.REFERENCE["effective"], "pool": 0},
            {**nolock.REFERENCE["effective"], "set": False},
        ):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                nolock.settings({"httpStatus": 200, "response": {"summary": "finished", "settings": changed}})

    def test_blocked_native_fence_is_kept_in_the_denominator(self):
        native = nolock.native_blocked({"source_sha": nolock.REFERENCE["candidateCommit"]}, "H1")
        rows = [
            {"topology": topology, "branch": branch, "verdict": "PASS", "cleanup": "PASS"}
            for topology in ("H1", "H2") for branch in nolock.BLACK_BOX
        ]
        self.assertFalse(nolock.complete_results([*rows, native]))
        self.assertFalse(nolock.complete_results(rows))
        self.assertFalse(nolock.complete_results([*rows[:-1], native]))
        failed = copy.deepcopy(rows)
        failed[0]["verdict"] = "FAIL"
        self.assertFalse(nolock.complete_results([*failed, native]))
        self.assertFalse(native["canonicalCasePassClaimed"])

    def test_run_case_dispatch_and_source_contract(self):
        dispatch = Path("scripts/validation_heal/run_case.sh").read_text()
        self.assertIn("heal.admin-nolock-safety) driver=nolock.py", dispatch)
        catalog = json.loads(Path("validation-cases.json").read_text())
        source = {case["id"]: case for module in catalog["modules"] for case in module["cases"]}[nolock.CASE_ID]
        self.assertEqual(source["probes"]["designCode"], "A09")
        self.assertEqual(source["probes"]["topology"], "H1,H2")

    def test_foreground_plan_keeps_three_overwrites_or_one_exact_delete(self):
        expected = [{"key": nolock.TARGET_KEY, "versionId": value, "isLatest": value == "v2",
                     "deleteMarker": False} for value in ("v0", "v1", "v2")]
        overwrites = nolock.operation_plan("rv-heal-0123456789abcdef", "overwrite-nolock", expected)
        self.assertEqual(len(overwrites), 3)
        self.assertEqual([step["params"]["Metadata"]["nonce"] for step in overwrites], ["0", "1", "2"])
        deletion = nolock.operation_plan("rv-heal-0123456789abcdef", "delete-nolock", expected)
        self.assertEqual(deletion[0]["params"]["VersionId"], "v0")

    def test_native_blocked_rows_cannot_become_a_canonical_pass(self):
        row = nolock.native_blocked({"source_sha": nolock.REFERENCE["candidateCommit"]}, "H2")
        self.assertEqual(row["verdict"], "BLOCKED")
        self.assertFalse(row["canonicalCasePassClaimed"])
        self.assertFalse(row["nativeFenceProved"])


if __name__ == "__main__":
    unittest.main()
