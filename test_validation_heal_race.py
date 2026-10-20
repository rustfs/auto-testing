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

    def test_g05_binds_multipart_operations_to_acknowledged_upload(self):
        # The frozen DM fixture requires two nonfinal parts above 5 MiB.
        self.assertEqual(race.G05_PART_SIZES,
                         (5 * 1024 * 1024 + 17, 5 * 1024 * 1024 + 93, 1024 * 1024 + 47))
        expected = [{"key": race.G05_TARGET_KEY, "versionId": "null", "isLatest": True}]
        continued = race.operation_plan("rv-heal-0123456789abcdef", "upload-continue", expected)
        completed = race.operation_plan("rv-heal-0123456789abcdef", "complete", expected)
        aborted = race.operation_plan("rv-heal-0123456789abcdef", "abort", expected)
        self.assertEqual([step["operation"] for step in continued], ["create_multipart_upload", "upload_part"])
        self.assertEqual([step["operation"] for step in completed],
                         ["create_multipart_upload", "upload_part", "upload_part", "upload_part",
                          "complete_multipart_upload"])
        self.assertEqual([step["operation"] for step in aborted],
                         ["create_multipart_upload", "upload_part", "abort_multipart_upload"])
        self.assertEqual([step["size"] for step in completed[1:4]], list(race.G05_PART_SIZES))
        self.assertEqual(completed[-1]["params"]["MultipartUpload"]["Parts"], [])
        self.assertEqual(race.target_key("complete"), race.G05_TARGET_KEY)

    def test_g05_foreground_resolves_upload_id_and_part_etags_from_ack(self):
        class FakeRecorder:
            def __init__(self):
                self.calls = []

            def call(self, operation, params, body=None):
                self.calls.append((operation, params, body))
                if operation == "create_multipart_upload":
                    return {"UploadId": "upload-ack"}
                if operation == "upload_part":
                    return {"ETag": f"etag-{params['PartNumber']}"}
                return {"ETag": "complete-etag", "VersionId": "null"}

        recorder = FakeRecorder()
        plan = race.operation_plan("rv-heal-0123456789abcdef", "complete", [])
        responses, runtime = race.foreground(recorder, plan)
        self.assertEqual(runtime["uploadIds"]["complete"], "upload-ack")
        self.assertEqual(recorder.calls[1][1]["UploadId"], "upload-ack")
        self.assertEqual(recorder.calls[-1][1]["UploadId"], "upload-ack")
        self.assertEqual(recorder.calls[-1][1]["MultipartUpload"], {
            "Parts": [{"PartNumber": 1, "ETag": "etag-1"},
                      {"PartNumber": 2, "ETag": "etag-2"},
                      {"PartNumber": 3, "ETag": "etag-3"}]})
        self.assertEqual([len(call[2]) if call[2] is not None else None for call in recorder.calls],
                         [None, *race.G05_PART_SIZES, None])
        self.assertEqual(len(responses), 5)

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
