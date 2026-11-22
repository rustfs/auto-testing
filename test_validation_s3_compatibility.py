#!/usr/bin/env python3

import json
import re
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "rustfs-s3-compat-test.sh"


class S3CompatibilityCatalogTests(unittest.TestCase):
    def test_every_existing_compatibility_case_is_catalogued_and_reusable(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        source_cases = dict(
            re.findall(r'run_test\s+([A-Z0-9-]+)\s+"([^"]+)"', source)
        )
        catalog = json.loads((ROOT / "validation-cases.json").read_text(encoding="utf-8"))
        local = next(
            module for module in catalog["local_modules"]
            if module["id"] == "s3-compatibility-suite"
        )
        cases = {case["id"]: case for case in local["cases"]}
        self.assertEqual(set(cases), {
            f"s3.compatibility.{case_id.lower()}" for case_id in source_cases
        })
        for case_id, case in cases.items():
            source_id = case_id.rsplit(".", 1)[-1].upper()
            self.assertEqual([source_id], case["reuse_candidates"]["case_ids"])
            self.assertEqual("configured-endpoint", case["probes"]["executionMode"])
            self.assertEqual("rustfs-s3-compat-test.sh", case["probes"]["runner"])

    def test_validation_dispatch_accepts_compatibility_case_prefix(self) -> None:
        dispatcher = (ROOT / "scripts/run_validation_case.sh").read_text(encoding="utf-8")
        self.assertIn("s3.compatibility.*", dispatcher)
        self.assertIn("s3.compatibility.*", SCRIPT.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
