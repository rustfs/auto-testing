#!/usr/bin/env python3

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent
INVENTORY = ROOT / "docs/source-test-migration-inventory.md"


class SourceTestMigrationInventoryTests(unittest.TestCase):
    def test_inventory_retains_the_source_audit_totals(self) -> None:
        text = INVENTORY.read_text(encoding="utf-8")
        self.assertIn("18 workflow entries", text)
        self.assertIn("243 Go test files", text)
        self.assertIn("1,081 `Test*` entries", text)
        self.assertIn("872 benchmark entries", text)
        self.assertIn("cmd/data-scanner_test.go", text)
        self.assertIn("internal/store/store_test.go", text)

    def test_inventory_has_no_vendor_specific_product_name(self) -> None:
        self.assertNotRegex(INVENTORY.read_text(encoding="utf-8"), r"(?i)minio|mint|aws")


if __name__ == "__main__":
    unittest.main()
