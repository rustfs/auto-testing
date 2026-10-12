"""C11 must prove real S3 deletion and preserve successor responsibility."""

import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from scripts.validation_heal import incarnation as suite


def version(number):
    return str(uuid.UUID(int=number))


def manifest(base=1):
    return [
        {"key": "objects/a", "versionId": version(base), "deleteMarker": False, "isLatest": False,
         "payload": {"size": 1, "sha256": "a" * 64}},
        {"key": "objects/a", "versionId": version(base + 1), "deleteMarker": True, "isLatest": True},
    ]


class DeletionRecorder:
    def __init__(self, *, wrong_version=False, remaining=False, unknown_absence=False, pending_upload=False):
        self.probe = self
        self.calls = []
        self.inventory_calls = 0
        self.wrong_version, self.remaining = wrong_version, remaining
        self.unknown_absence, self.pending_upload = unknown_absence, pending_upload

    def inventory(self, bucket):
        self.inventory_calls += 1
        return {"entries": manifest() if self.inventory_calls == 1 or self.remaining else []}

    def s3(self, op, params):
        return {"acknowledged": False, "resultUnknown": self.unknown_absence,
                "response": {"ResponseMetadata": {"HTTPStatusCode": 404}}}

    def call(self, op, params):
        self.calls.append((op, copy.deepcopy(params)))
        if op == "delete_object":
            return {"VersionId": version(99) if self.wrong_version else params["VersionId"],
                    "DeleteMarker": params["VersionId"] == version(2),
                    "ResponseMetadata": {"HTTPStatusCode": 204}}
        if op == "list_multipart_uploads":
            return {"IsTruncated": False, "Uploads": [{"UploadId": "still-pending"}] if self.pending_upload else []}
        if op == "delete_bucket": return {"ResponseMetadata": {"HTTPStatusCode": 204}}
        raise AssertionError("unexpected mutation " + op)


class IncarnationTests(unittest.TestCase):
    def test_normal_deletion_accounts_for_every_data_version_and_marker(self):
        recorder = DeletionRecorder()
        proof = suite.delete_old_bucket(recorder, "bucket", manifest())
        self.assertFalse(proof["internalNamespaceRemovalUsed"])
        self.assertEqual(proof["deleted"], [{k: row[k] for k in ("key", "versionId", "deleteMarker")} for row in manifest()])
        self.assertEqual([op for op, _ in recorder.calls],
                         ["delete_object", "delete_object", "list_multipart_uploads", "delete_bucket"])

    def test_wrong_identity_remaining_members_and_unknown_absence_fail_closed(self):
        for kwargs in ({"wrong_version": True}, {"remaining": True}, {"unknown_absence": True}, {"pending_upload": True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                suite.delete_old_bucket(DeletionRecorder(**kwargs), "bucket", manifest())

    def test_duplicate_null_nil_and_wrong_kind_cannot_be_old_responsibility(self):
        bad = [manifest() + [manifest()[0]], [*manifest(), {**manifest()[0], "versionId": "null"}],
               [{**manifest()[0], "versionId": version(0)}, manifest()[1]],
               [{**manifest()[0], "deleteMarker": 0}, manifest()[1]]]
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError): suite.old_manifest(value)

    def snapshot(self, expected):
        versions = [{"versionId": row["versionId"], "deleted": row["deleteMarker"], "isLatest": row["isLatest"]}
                    for row in expected]
        return {"vm000": {"formats": ["fixed-disk-identities"], "files": [
            {"disk": disk, "path": "bucket/objects/a/xl.meta", "size": 10, "sha256": "b" * 64,
             "metadata": {"bucket": "bucket", "key": "objects/a", "versions": copy.deepcopy(versions)}}
            for disk in range(4)]}}

    def test_rejoined_metadata_cannot_resurrect_an_old_version(self):
        expected = manifest(3)
        before = self.snapshot(expected)
        after = copy.deepcopy(before)
        after["vm000"]["files"][0]["metadata"]["versions"].append(
            {"versionId": version(1), "deleted": False, "isLatest": False})
        with self.assertRaisesRegex(ValueError, "resurrected"):
            suite.successor_physical(before, after, "bucket", expected, manifest(), ("vm000", 0))

    def test_missing_successor_marker_and_duplicate_version_are_rejected(self):
        expected = manifest(3)
        before = self.snapshot(expected)
        for change in ("missing", "duplicate"):
            after = copy.deepcopy(before)
            rows = after["vm000"]["files"][1]["metadata"]["versions"]
            rows.pop() if change == "missing" else rows.append(copy.deepcopy(rows[0]))
            with self.subTest(change=change), self.assertRaises(ValueError):
                suite.successor_physical(before, after, "bucket", expected, manifest(), ("vm000", 0))

    def test_original_gated_member_bytes_and_control_bucket_cannot_change(self):
        before = self.snapshot(manifest())
        after = copy.deepcopy(before)
        after["vm000"]["files"][0]["sha256"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "gated old metadata"):
            suite.stale_member_unchanged(before, after, ("vm000", 0))
        before = self.snapshot(manifest(3))
        before["vm000"]["files"].append({"disk": 0, "path": "bucket-control/a/part.1", "sha256": "a" * 64, "size": 1})
        after = copy.deepcopy(before)
        after["vm000"]["files"][-1]["sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "control bucket"):
            suite.successor_physical(before, after, "bucket", manifest(3), manifest(), ("vm000", 0))

    def test_successor_configuration_requires_known_responses_and_its_exact_tag(self):
        class ConfigProbe:
            def s3(self, op, params):
                return {"acknowledged": True, "resultUnknown": False, "response": {
                    "Status": "Enabled", "TagSet": [{"Key": "validation-generation", "Value": "successor"}]}}
        probe = ConfigProbe()
        self.assertIn("tagging", suite.config(probe, "bucket"))
        with patch.object(probe, "s3", return_value={"acknowledged": True, "resultUnknown": True, "response": {"Status": "Enabled"}}):
            with self.assertRaises(ValueError): suite.config(probe, "bucket")

    def test_partial_metadata_branch_never_promotes_full_canonical_case(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": "a" * 40}))
            args = ["incarnation.py", "--case-id", suite.CASE_ID, "--candidate", str(candidate),
                "--evidence", str(root), "--rc-bin", str(root / "rc"), "--tool-directory", str(root / "tools"),
                "--testing-commit", "a" * 40, "--binary-sha", "b" * 64]
            def passed(args, topology, nodes, candidate):
                return {"topology": topology, "fixture": "DV", "branch": "old-metadata-rejoin", "verdict": "PASS", "cleanup": "PASS"}
            with patch("sys.argv", args), patch.dict(os.environ, {"GITHUB_RUN_ID": "12345"}), \
                 patch.object(suite, "verify_manager", return_value={"canonicalCasePassClaimed": False}), \
                 patch.object(suite, "verify_storage", return_value={"exactStorageCommitBarrierObserved": True,
                     "canonicalFleetBranchesExecuted": False, "canonicalCasePassClaimed": False}), \
                 patch.object(suite, "run_metadata_rejoin", side_effect=passed), \
                 patch.object(suite, "run_replay", side_effect=lambda args, topology, nodes, branch, candidate: {
                     "topology": topology, "fixture": "DV", "branch": branch, "verdict": "PASS", "cleanup": "PASS"}):
                self.assertEqual(suite.main(), 1)
            result = json.loads((root / "healthy-summary.json").read_text())
            self.assertFalse(result["complete"])
            self.assertFalse(result["canonicalCasePassClaimed"])
            self.assertTrue(result["diagnosticBranchesPassed"])
            self.assertEqual(len(result["results"]), 8)
            self.assertEqual(sum(row["verdict"] == "PASS" for row in result["results"]), 6)
            native = [row for row in result['results'] if row['branch'] == 'native-incarnation-fence']
            self.assertEqual(len(native), 2)
            self.assertTrue(all(row['verdict'] == 'PREREQUISITE_PASS' and row['topology'] is None
                                and not row['fleetBranchExecuted'] for row in native))
            self.assertTrue((root / 'independent-native-storage-commit.json').is_file())

    def test_clean_finalization_cannot_hide_a_failed_replay_branch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": "a" * 40}))
            args = ["incarnation.py", "--case-id", suite.CASE_ID, "--candidate", str(candidate),
                "--evidence", str(root), "--rc-bin", str(root / "rc"), "--tool-directory", str(root / "tools"),
                "--testing-commit", "a" * 40, "--binary-sha", "b" * 64]
            with patch("sys.argv", args), patch.dict(os.environ, {"GITHUB_RUN_ID": "12345"}), \
                 patch.object(suite, "verify_manager", return_value={}), patch.object(suite, "verify_storage", return_value={}), \
                 patch.object(suite, "run_metadata_rejoin", side_effect=lambda args, topology, nodes, candidate: {
                     "topology": topology, "branch": "old-metadata-rejoin", "verdict": "PASS", "cleanup": "PASS"}), \
                 patch.object(suite, "run_replay", side_effect=lambda args, topology, nodes, branch, candidate: {
                     "topology": topology, "branch": branch, "verdict": "FAIL", "cleanup": "PASS"}):
                self.assertEqual(suite.main(), 1)
            result = json.loads((root / "healthy-summary.json").read_text())
            self.assertEqual(result["cleanup"], "PASS")
            self.assertFalse(result["diagnosticBranchesPassed"])
            self.assertFalse(result["complete"])
            self.assertFalse(result["canonicalCasePassClaimed"])

    def test_unverified_storage_artifact_refuses_before_fleet_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate = root / 'candidate.json'
            candidate.write_text(json.dumps({'source_sha': 'a' * 40}))
            args = ['incarnation.py', '--case-id', suite.CASE_ID, '--candidate', str(candidate),
                '--evidence', str(root), '--rc-bin', str(root / 'rc'), '--tool-directory', str(root / 'tools'),
                '--testing-commit', 'a' * 40, '--binary-sha', 'b' * 64]
            with patch('sys.argv', args), patch.object(suite, 'verify_manager', return_value={}), \
                 patch.object(suite, 'verify_storage', side_effect=ValueError('unverified artifact')), \
                 patch.object(suite, 'run_metadata_rejoin') as mutate:
                with self.assertRaisesRegex(ValueError, 'unverified artifact'): suite.main()
                mutate.assert_not_called()


if __name__ == "__main__": unittest.main()
