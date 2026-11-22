#!/usr/bin/env python3

import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent


class SourceProcedureCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        catalog = json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))
        self.module = next(
            module for module in catalog["local_modules"]
            if module["id"] == "source-suite-procedures"
        )

    def test_procedure_entries_use_candidate_bound_configured_endpoints(self) -> None:
        self.assertGreaterEqual(len(self.module["cases"]), 100)
        for case in self.module["cases"]:
            self.assertEqual("configured-endpoint", case["probes"]["executionMode"])
            self.assertEqual(1, len(case["reuse_candidates"]["case_ids"]))
            self.assertTrue(case["probes"]["runner"].startswith("rustfs-"))

    def test_dispatcher_has_every_source_suite_family(self) -> None:
        dispatcher = (ROOT / "scripts/run_validation_case.sh").read_text(encoding="utf-8")
        for family in ("security", "tier", "storage", "kms", "table", "upgrade"):
            self.assertIn(f"source-procedure.{family}.*", dispatcher)

    def test_table_and_upgrade_procedures_are_catalogued(self) -> None:
        module = next(
            module for module in json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))["local_modules"]
            if module["id"] == "table-upgrade-procedures"
        )
        self.assertEqual(25, len(module["cases"]))
        self.assertEqual(7, sum(case["probes"]["runner"] == "rustfs-table-test.sh" for case in module["cases"]))
        self.assertEqual(18, sum(case["probes"]["runner"] == "rustfs-upgrade-test.sh" for case in module["cases"]))


if __name__ == "__main__":
    unittest.main()
