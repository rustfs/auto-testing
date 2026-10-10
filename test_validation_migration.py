#!/usr/bin/env python3

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.validation_catalog import build_snapshot, validate_reuse_candidates, validate_snapshot
from scripts.validation_report import validate_results


ROOT = Path(__file__).resolve().parent
SOURCE = Path("/Volumes/Developer/KAI/rustfs-release-validation/scenarios/catalog.yaml")


class CatalogTest(unittest.TestCase):
    def test_snapshot_contains_all_source_cases(self) -> None:
        snapshot = json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))
        validate_snapshot(snapshot)
        validate_reuse_candidates(snapshot, ROOT)
        self.assertEqual(158, sum(len(module["cases"]) for module in snapshot["modules"]))
        if SOURCE.is_file():
            self.assertEqual(build_snapshot(SOURCE), snapshot)

    def test_duplicate_case_is_rejected(self) -> None:
        snapshot = json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))
        snapshot["modules"][0]["cases"].append(snapshot["modules"][0]["cases"][0])
        with self.assertRaisesRegex(ValueError, "duplicate case ID"):
            validate_snapshot(snapshot)


class ReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.results = Path(self.tmp.name) / "cases"
        self.results.mkdir()
        self.catalog = json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))
        self.case = self.catalog["modules"][0]["cases"][0]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_result(self, name: str, **changes: object) -> None:
        value = {
            "schema_version": 1,
            "case_id": self.case["id"],
            "source_status": self.case["source_status"],
            "migration_status": "unverified",
            "verdict": "BLOCKED",
            "reason": "executor not migrated",
            "profile": "lifecycle-a",
        }
        value.update(changes)
        (self.results / name).write_text(json.dumps(value), encoding="utf-8")

    def test_missing_implemented_case_fails_gate(self) -> None:
        _, errors = validate_results(self.catalog, self.results, gate_statuses={"implemented"})
        self.assertIn(f"missing gate result: {self.case['id']}", errors)

    def test_duplicate_and_running_fail(self) -> None:
        self.write_result("one.json", verdict="RUNNING")
        self.write_result("two.json", verdict="RUNNING")
        _, errors = validate_results(self.catalog, self.results, gate_statuses=set())
        self.assertIn(f"duplicate result: {self.case['id']}", errors)
        self.assertIn(f"{self.case['id']}: result remained RUNNING", errors)

    def test_pass_needs_verified_assertions_cleanup_and_provenance(self) -> None:
        self.write_result("one.json", verdict="PASS")
        _, errors = validate_results(self.catalog, self.results, gate_statuses=set())
        self.assertIn(f"{self.case['id']}: PASS without verified migration", errors)
        self.assertIn(f"{self.case['id']}: PASS lacks one result per source assertion", errors)
        self.assertIn(f"{self.case['id']}: PASS requires clean environment", errors)
        self.assertIn(f"{self.case['id']}: PASS lacks candidate and client provenance", errors)

    def test_complete_case_contract(self) -> None:
        evidence = self.results.parent / "case-evidence.json"
        evidence.write_text("{}", encoding="utf-8")
        self.write_result(
            "one.json",
            verdict="PASS",
            migration_status="verified",
            assertions=[
                {"source_assertion": text, "status": "PASS", "evidence": ["case-evidence.json"]}
                for text in self.case["assertions"]
            ],
            cleanup_status="PASS",
            provenance={
                "candidate_sha256": "a" * 64,
                "binary_sha256": "e" * 64,
                "candidate_url": "https://example.invalid/rustfs.deb",
                "source_commit": "b" * 40,
                "testing_commit": "c" * 40,
                "rc_path": "/opt/bin/rc",
                "rc_sha256": "d" * 64,
            },
        )
        _, errors = validate_results(self.catalog, self.results, gate_statuses=set())
        self.assertEqual([], errors)

    def test_pass_cannot_relabel_source_assertions(self) -> None:
        (self.results.parent / "evidence.json").write_text("{}", encoding="utf-8")
        self.write_result(
            "one.json",
            verdict="PASS",
            migration_status="verified",
            assertions=[
                {"source_assertion": "weaker assertion", "status": "PASS", "evidence": ["evidence.json"]}
                for _ in self.case["assertions"]
            ],
            cleanup_status="PASS",
            provenance={
                "candidate_sha256": "a" * 64,
                "binary_sha256": "e" * 64,
                "candidate_url": "https://example.invalid/rustfs.deb",
                "source_commit": "b" * 40,
                "testing_commit": "c" * 40,
                "rc_path": "/opt/bin/rc",
                "rc_sha256": "d" * 64,
            },
        )
        _, errors = validate_results(self.catalog, self.results, gate_statuses=set())
        self.assertIn(f"{self.case['id']}: assertion 1 lacks matching source text or evidence", errors)

    def test_missing_evidence_file_fails(self) -> None:
        self.write_result(
            "one.json",
            verdict="PASS",
            migration_status="verified",
            assertions=[
                {"source_assertion": text, "status": "PASS", "evidence": ["missing.json"]}
                for text in self.case["assertions"]
            ],
            cleanup_status="PASS",
            provenance={
                "candidate_sha256": "a" * 64,
                "binary_sha256": "e" * 64,
                "candidate_url": "https://example.invalid/rustfs.deb",
                "source_commit": "b" * 40,
                "testing_commit": "c" * 40,
                "rc_path": "/opt/bin/rc",
                "rc_sha256": "d" * 64,
            },
        )
        _, errors = validate_results(self.catalog, self.results, gate_statuses=set())
        self.assertIn(f"{self.case['id']}: assertion 1 has missing or unsafe evidence paths", errors)


class ResultWriterTest(unittest.TestCase):
    def test_writer_and_report_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results = root / "cases"
            results.mkdir()
            rc = root / "rc"
            rc.write_bytes(b"test-rc")
            candidate = root / "candidate.json"
            candidate.write_text(json.dumps({
                "package_url": "https://example.invalid/rustfs.deb",
                "package_sha256": "a" * 64,
                "source_sha": "c" * 40,
            }), encoding="utf-8")
            evidence = root / "evidence.json"
            evidence.write_text("{}", encoding="utf-8")
            command = [
                sys.executable, str(ROOT / "scripts/validation_result.py"),
                "--case-id", "s3.basic-object-round-trip",
                "--profile", "single-1disk",
                "--result-file", str(results / "s3.basic-object-round-trip.json"),
            ]
            subprocess.run(command + ["--verdict", "RUNNING"], check=True, capture_output=True)
            self.assertEqual("RUNNING", json.loads((results / "s3.basic-object-round-trip.json").read_text())["verdict"])
            subprocess.run(command + [
                "--verdict", "PASS", "--cleanup-status", "PASS",
                "--candidate-manifest", str(candidate),
                "--rc-bin", str(rc),
                "--testing-commit", "d" * 40,
                "--binary-sha256", "b" * 64,
                "--evidence", "evidence.json",
            ], check=True, capture_output=True)
            catalog = json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))
            _, errors = validate_results(catalog, results, gate_statuses=set())
            self.assertEqual([], errors)


if __name__ == "__main__":
    unittest.main()
