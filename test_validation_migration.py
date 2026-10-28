#!/usr/bin/env python3

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.validation_catalog import build_snapshot, validate_reuse_candidates, validate_snapshot
from scripts.validation_report import validate_results
from scripts.validation_migration_review import summarize, validate_review


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

    def test_selected_case_requires_live_pass(self) -> None:
        _, errors = validate_results(
            self.catalog, self.results, gate_statuses=set(), gate_cases={self.case["id"]},
        )
        self.assertIn(f"missing gate result: {self.case['id']}", errors)
        with self.assertRaisesRegex(ValueError, "unknown gate case"):
            validate_results(
                self.catalog, self.results, gate_statuses=set(), gate_cases={"missing.case"},
            )

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
            identity = ["--candidate-manifest", str(candidate), "--rc-bin", str(rc), "--testing-commit", "d" * 40]
            subprocess.run(command + identity + ["--verdict", "INFRA_ERROR", "--cleanup-status", "PASS",
                                                "--reason", "candidate install failed"], check=True, capture_output=True)
            bootstrap = json.loads((results / "s3.basic-object-round-trip.json").read_text())
            self.assertEqual(bootstrap["verdict"], "INFRA_ERROR")
            self.assertEqual(bootstrap["migration_status"], "unverified")
            self.assertIsNone(bootstrap["provenance"]["binary_sha256"])
            subprocess.run(command + identity + ["--verdict", "BLOCKED", "--cleanup-status", "PASS",
                "--binary-sha256", "b" * 64, "--reason", "diagnostic branches require independent canonical audit",
                "--evidence", "evidence.json"], check=True, capture_output=True)
            blocked = json.loads((results / "s3.basic-object-round-trip.json").read_text())
            self.assertEqual(blocked["verdict"], "BLOCKED")
            self.assertEqual(blocked["migration_status"], "unverified")
            self.assertNotIn("assertions", blocked)
            self.assertEqual(blocked["provenance"]["binary_sha256"], "b" * 64)
            catalog = json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))
            _, blocked_errors = validate_results(catalog, results, gate_statuses=set(),
                                                gate_cases={"s3.basic-object-round-trip"})
            self.assertEqual(blocked_errors, ["s3.basic-object-round-trip: gate verdict is 'BLOCKED'"])
            invalid_pass = subprocess.run(command + identity + ["--verdict", "PASS", "--cleanup-status", "PASS",
                                                                "--evidence", "evidence.json"], capture_output=True)
            self.assertNotEqual(invalid_pass.returncode, 0)
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

    def test_predeployment_blocked_result_does_not_claim_an_installed_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rc = root / "rc"
            rc.write_bytes(b"test-rc")
            candidate = root / "candidate.json"
            candidate.write_text(json.dumps({
                "package_url": "https://example.invalid/rustfs.deb",
                "package_sha256": "a" * 64,
                "source_sha": "c" * 40,
            }), encoding="utf-8")
            result = root / "b03.json"
            command = [sys.executable, str(ROOT / "scripts/validation_result.py"),
                "--case-id", "heal.disk-replacement-formatted", "--profile", "cluster-1pool",
                "--result-file", str(result), "--verdict", "BLOCKED", "--cleanup-status", "PASS",
                "--candidate-manifest", str(candidate), "--testing-commit", "d" * 40,
                "--rc-bin", str(rc), "--predeployment-blocked", "--reason", "mount gate failed"]
            subprocess.run(command, check=True, capture_output=True)
            value = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(value["execution_stage"], "pre-deployment")
            self.assertEqual(value["migration_status"], "unverified")
            self.assertIsNone(value["provenance"]["binary_sha256"])
            self.assertNotIn("assertions", value)
            invalid_command = command.copy()
            invalid_command[invalid_command.index("--verdict") + 1] = "PASS"
            invalid = subprocess.run(invalid_command, capture_output=True)
            self.assertNotEqual(invalid.returncode, 0)

            b04 = root / "b04.json"
            b04_command = command.copy()
            b04_command[b04_command.index("--case-id") + 1] = "heal.disk-replacement-unformatted"
            b04_command[b04_command.index("--result-file") + 1] = str(b04)
            subprocess.run(b04_command, check=True, capture_output=True)
            b04_value = json.loads(b04.read_text(encoding="utf-8"))
            self.assertEqual(b04_value["case_id"], "heal.disk-replacement-unformatted")
            self.assertIsNone(b04_value["provenance"]["binary_sha256"])


class ImplementationReviewTest(unittest.TestCase):
    def setUp(self):
        self.catalog = json.loads((ROOT / "validation-cases.json").read_text())
        self.review = json.loads((ROOT / "validation-migration-review.json").read_text())

    def test_explicit_review_covers_the_entire_catalog(self):
        validate_review(self.review, self.catalog)
        summary = summarize(self.review)
        self.assertEqual(summary["total"], 158)
        self.assertEqual(sum(summary["implementation"].values()), 158)
        self.assertEqual(sum(summary["live"].values()), 158)
        self.assertFalse(summary["candidate_pass_claimed"])
        self.assertFalse(summary["all_cases_live_accepted"])

    def test_retained_failure_is_independent_of_implementation_completion(self):
        # Candidate failures must never silently become missing test code.
        case = next(case for case in self.review["cases"]
                    if case["implementation"] == "implemented"
                    and case["live"]["state"] == "retained-failure")
        before = summarize(self.review)["implementation"]
        case["live"]["finding"] = "Candidate request returned a retained unexpected response; cause remains under review."
        validate_review(self.review, self.catalog)
        self.assertEqual(before, summarize(self.review)["implementation"])
        with tempfile.TemporaryDirectory() as directory:
            result = {"schema_version": 1, "case_id": case["case_id"],
                      "source_status": next(source["source_status"] for module in self.catalog["modules"]
                                            for source in module["cases"] if source["id"] == case["case_id"]),
                      "migration_status": "unverified", "profile": "cluster-1pool",
                      "verdict": "FAIL", "cleanup_status": "PASS", "reason": case["live"]["finding"]}
            result_path = Path(directory) / "retained.json"
            result_path.write_text(json.dumps(result))
            original = result_path.read_bytes()
            _, errors = validate_results(self.catalog, Path(directory), gate_statuses=set(),
                                         gate_cases={case["case_id"]})
            self.assertIn(f"{case['case_id']}: gate verdict is 'FAIL'", errors)
            self.assertEqual(original, result_path.read_bytes())

    def test_missing_or_duplicate_id_is_rejected(self):
        for change, reason in (("missing", "missing implementation reviews"),
                               ("duplicate", "duplicate implementation case")):
            with self.subTest(change=change):
                review = copy.deepcopy(self.review)
                if change == "missing":
                    review["cases"].pop()
                else:
                    review["cases"].append(copy.deepcopy(review["cases"][0]))
                with self.assertRaisesRegex(ValueError, reason):
                    validate_review(review, self.catalog)

    def test_source_or_code_drift_requires_another_review(self):
        for change, reason in (("source", "another source catalog"),
                               ("code", "implementation changed since review"),
                               ("branch", "source branch coverage changed")):
            with self.subTest(change=change):
                review = copy.deepcopy(self.review)
                if change == "source":
                    review["source_commit"] = "0" * 40
                elif change == "code":
                    review["reviewed_files"][next(iter(review["reviewed_files"]))] = "0" * 64
                else:
                    case = next(case for case in review["cases"] if "source_variants" in case)
                    case["source_variants"].pop(next(iter(case["source_variants"])))
                with self.assertRaisesRegex(ValueError, reason):
                    validate_review(review, self.catalog)

    def test_partial_implementation_must_name_a_concrete_gap(self):
        case = self.review["cases"][0]
        case.update(implementation="partial", gaps=[])
        with self.assertRaisesRegex(ValueError, "actionable gaps"):
            validate_review(self.review, self.catalog)

    def test_default_disabled_environment_requirements_cannot_disappear(self):
        case = next(case for case in self.review["cases"] if case["implementation"] == "environment-gated")
        case["required_capabilities"] = []
        with self.assertRaisesRegex(ValueError, "environment requirements drifted"):
            validate_review(self.review, self.catalog)

    def test_unreviewed_or_nonlocal_branch_references_are_rejected(self):
        for name, reason in (("../outside.py", "nonlocal implementation reference"),
                             ("validation-migration-review.json", "unreviewed implementation reference")):
            with self.subTest(name=name):
                review = copy.deepcopy(self.review)
                review["cases"][0]["references"] = [name]
                with self.assertRaisesRegex(ValueError, reason):
                    validate_review(review, self.catalog)

    def test_invalid_record_types_raise_a_review_error(self):
        for invalid in (None, [], {"implementation": []}, {"case_id": "unknown"}):
            with self.subTest(invalid=invalid):
                review = copy.deepcopy(self.review)
                review["cases"][0] = invalid
                with self.assertRaises(ValueError):
                    validate_review(review, self.catalog)

    def test_require_complete_checks_code_gaps_without_demanding_product_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.json"
            command = [sys.executable, "-B", str(ROOT / "scripts/validation_migration_review.py"),
                       "--review", str(path), "--require-complete"]
            path.write_text(json.dumps(self.review))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertGreater(json.loads(result.stdout)["live"]["retained-failure"], 0)
            self.assertFalse(json.loads(result.stdout)["candidate_pass_claimed"])
            # A real implementation gap must fail even with unchanged product results.
            case = next(case for case in self.review["cases"]
                        if case["implementation"] == "implemented" and case["live"]["state"] == "retained-failure")
            case.update(implementation="partial", gaps=["Missing source controller in this negative fixture."])
            path.write_text(json.dumps(self.review))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(json.loads(result.stdout)["remaining_implementation"][0]["case_id"], case["case_id"])


if __name__ == "__main__":
    unittest.main()
