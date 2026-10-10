"""A rejected fault ID cannot bypass ownership, intent or physical-scene checks."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal.heal_fault import bounded_fault_id
from scripts.validation_heal.fleet_node import audit_finalize


class RejectedFaultRecoveryTest(unittest.TestCase):
    def test_expanded_branches_have_bounded_distinct_fault_ids(self):
        branch = "stripe-minus-exact-plus-s1048575-parity-missing"
        original = "D01-" + branch + "-0"
        self.assertEqual(len(original.split("-", 1)[1]), 49)
        identifier = bounded_fault_id("D01", branch, 0)
        self.assertRegex(identifier, r"^D01-[a-z0-9-]{1,48}$")
        self.assertNotEqual(identifier, bounded_fault_id("D01", branch, 1))
        self.assertNotEqual(identifier, bounded_fault_id("D01", branch + "-another", 0))
        with self.assertRaises(ValueError): bounded_fault_id("bad", branch, 0)

    def test_finalization_needs_no_intent_and_exact_archived_scene(self):
        snapshot = {"files": [{"sha256": "actual", "path": "owned/key/xl.meta"}], "formats": [{"disk": "original"}]}
        scene = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        request = {"runId": "gh-12345-1-h1", "disk": 0, "binarySha256": "a" * 64,
                   "rejectedFaultId": "D01-" + "a" * 49, "expectedSceneSha256": scene}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "quarantine").mkdir()
            with patch("scripts.validation_heal.fleet_node.owned", return_value=(None, None, {"binarySha256": "a" * 64})), \
                 patch("scripts.validation_heal.fleet_node.stopped"), \
                 patch("scripts.validation_heal.fleet_node.fault_root", return_value=(root, {})), \
                 patch("scripts.validation_heal.fleet_node.snapshot", return_value=snapshot), \
                 patch("scripts.validation_heal.fleet_node.cleanup", return_value={"cleanup": "PASS"}) as cleanup:
                result = audit_finalize(request)
                self.assertTrue(result["faultNeverApplied"])
                self.assertFalse(result["productRepairClaimed"])
                cleanup.reset_mock()
                for changed in ({**request, "expectedSceneSha256": "b" * 64},
                                {**request, "binarySha256": "b" * 64},
                                {**request, "rejectedFaultId": "D01-valid-length"}):
                    with self.assertRaises(ValueError): audit_finalize(changed)
                    cleanup.assert_not_called()
                (root / "quarantine" / (request["rejectedFaultId"] + ".intent.json")).write_text("intent")
                with self.assertRaisesRegex(ValueError, "intent"):
                    audit_finalize(request)
                cleanup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
