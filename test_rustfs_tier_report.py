#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

import rustfs_tier_report as report


class TierReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.results = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def write_matrix(self, status: str = "PASS") -> None:
        for topology in report.TOPOLOGIES:
            for case_id, name in report.CASES:
                self.write_result(topology, case_id, name, status)

    def write_result(
        self,
        topology: str,
        case_id: str,
        name: str,
        status: str,
        *,
        filename: str | None = None,
        extra: dict | None = None,
    ) -> Path:
        value = {
            "schema_version": 1,
            "topology": topology,
            "case_id": case_id,
            "name": name,
            "status": status,
            "case_rc": 0 if status == "PASS" else (2 if status == "UNSUPPORTED" else 1),
            "cleanup_status": "PASS",
        }
        if extra:
            value.update(extra)
        path = self.results / (filename or f"{topology}--{case_id}.json")
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_complete_two_topology_matrix_has_56_unique_rows(self) -> None:
        self.write_matrix()
        validation = report.validate_results(self.results)
        self.assertEqual([], validation.errors)
        self.assertEqual(56, len(validation.rows))
        self.assertIn("- Total: 56", report.render_markdown(validation))

    def test_first_topology_failure_is_not_hidden_by_second(self) -> None:
        self.write_matrix()
        self.write_result("single-single", "TIER-101", "TierAddValidation", "FAIL")
        validation = report.validate_results(self.results)
        self.assertIn("single-single/TIER-101: case failed", validation.errors)

    def test_missing_running_and_truncated_results_fail_closed(self) -> None:
        for defect in ("missing", "running", "truncated"):
            with self.subTest(defect=defect):
                for child in self.results.iterdir():
                    child.unlink()
                self.write_matrix()
                target = self.results / "single-single--TIER-101.json"
                if defect == "missing":
                    target.unlink()
                elif defect == "running":
                    self.write_result("single-single", "TIER-101", "TierAddValidation", "RUNNING")
                else:
                    target.write_text('{"schema_version":', encoding="utf-8")
                validation = report.validate_results(self.results)
                self.assertTrue(validation.errors)

    def test_duplicate_key_fails(self) -> None:
        self.write_matrix()
        self.write_result(
            "single-single",
            "TIER-101",
            "TierAddValidation",
            "PASS",
            filename="duplicate.json",
        )
        validation = report.validate_results(self.results)
        self.assertIn("duplicate result key: single-single/TIER-101", validation.errors)

    def test_unsupported_requires_explicit_allowlist(self) -> None:
        self.write_matrix()
        self.write_result("single-single", "TIER-101", "TierAddValidation", "UNSUPPORTED")
        rejected = report.validate_results(self.results)
        self.assertIn("single-single/TIER-101: unsupported without allowlist", rejected.errors)
        accepted = report.validate_results(self.results, allowed_unsupported=["single-single:TIER-101"])
        self.assertEqual([], accepted.errors)

    def test_bucket_not_empty_failure_remains_a_gate_failure(self) -> None:
        self.write_matrix()
        self.write_result(
            "single-single",
            "TIER-107",
            "RestoreObject",
            "FAIL",
            extra={
                "cleanup_status": "FAIL",
                "cleanup_detail": "remote-bucket:tier-warm:delete-bucket: BucketNotEmpty",
                "failure_kind": "case_failed",
                "failure_detail": "restore assertion failed",
                "log_path": "logs/single-single--TIER-107.log",
            },
        )
        validation = report.validate_results(self.results)
        self.assertIn("single-single/TIER-107: case failed", validation.errors)
        self.assertIn("single-single/TIER-107: cleanup_status='FAIL'", validation.errors)
        row = next(
            item
            for item in validation.rows
            if item["topology"] == "single-single" and item["case_id"] == "TIER-107"
        )
        self.assertIn("BucketNotEmpty", row["cleanup_detail"])


if __name__ == "__main__":
    unittest.main()
