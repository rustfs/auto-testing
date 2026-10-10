"""Check preservation of Heal branches and prevent evidence from becoming PASS."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import copy
from unittest.mock import patch

from scripts.validation_heal.matrix import STAGES, TOPOLOGIES, audit_evidence, cases, compile_plan, select
from scripts.validation_heal.healthy import validate_healthy, validate_topology, validate_rejection
from scripts.validation_heal.fleet_node import paths


class HealContractTest(unittest.TestCase):
    def test_preserves_all_source_cases_and_applicable_branches(self):
        inventory = cases()
        plan = compile_plan(inventory, TOPOLOGIES, "manual-heal-unit-test")
        self.assertEqual(len(inventory), 84)
        self.assertEqual(sum(len(item["variants"]) for item in inventory), 301)
        self.assertEqual(plan["branchTemplateCount"], 532)
        self.assertEqual(len(plan["nonApplicableCombinations"]), 4)
        self.assertFalse(plan["automaticExecution"])
        self.assertFalse(plan["livePassClaimed"])
        self.assertFalse(plan["environmentBinding"]["verified"])
        self.assertTrue(plan["environmentBinding"]["topologies"]["H4"]["requiresExistingAdditionalDrives"])

    def test_narrowed_topology_keeps_missing_scope_explicit(self):
        plan = compile_plan(select("A01,A03"), {"H1"}, "manual-heal-unit-test")
        self.assertEqual(len(plan["instances"]), 2)
        self.assertEqual({item["caseId"] for item in plan["scopeExclusions"]},
                         {"heal.admin-healthy-noop", "heal.admin-token-cross-node-status"})
        self.assertFalse(plan["livePassClaimed"])
        with self.assertRaisesRegex(ValueError, "same Heal case"):
            select("A01,heal.admin-healthy-noop")

    def fixture_archive(self, root, plan):
        for instance in plan["instances"]:
            directory = root / instance["instanceId"]
            directory.mkdir()
            proof = {"runId": plan["runId"], "instanceId": instance["instanceId"], "evidence": {}}
            for stage in STAGES:
                value = (stage + " live observation").encode()
                (directory / (stage + ".txt")).write_bytes(value)
                proof["evidence"][stage] = [{"path": stage + ".txt", "sha256": hashlib.sha256(value).hexdigest()}]
            (directory / "evidence.json").write_text(json.dumps(proof))

    def test_complete_archive_requires_review_and_never_manufactures_pass(self):
        plan = compile_plan(select("A01"), {"H1"}, "manual-heal-unit-test")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture_archive(root, plan)
            result = audit_evidence(plan, root)
            self.assertEqual(result["state"], "EVIDENCE_COMPLETE_REVIEW_REQUIRED")
            self.assertFalse(result["livePassClaimed"])
            self.assertIsNone(result["canonicalVerdict"])
            first = root / plan["instances"][0]["instanceId"]
            (first / "physical.txt").write_text("different evidence")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                audit_evidence(plan, root)


    def test_rejects_dropped_branch_and_escaped_evidence(self):
        plan = compile_plan(select("A01"), {"H1"}, "manual-heal-unit-test")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture_archive(root, plan)
            dropped = {**plan, "instances": plan["instances"][:1]}
            with self.assertRaisesRegex(ValueError, "omitted or changed"):
                audit_evidence(dropped, root)
            first = root / plan["instances"][0]["instanceId"]
            index = first / "evidence.json"
            proof = json.loads(index.read_text())
            proof["evidence"]["physical"][0]["path"] = "../outside.txt"
            index.write_text(json.dumps(proof))
            with self.assertRaisesRegex(ValueError, "unsafe Heal evidence"):
                audit_evidence(plan, root)


class HealHealthyOracleTest(unittest.TestCase):
    def test_request_rejection_needs_the_exact_server_code(self):
        good = {"httpStatus": 400, "response": "<Error><Code>InvalidArgument</Code></Error>"}
        validate_rejection(good, "InvalidArgument")
        for bad in ({**good, "httpStatus": 200}, {"httpStatus": 400, "response": "<Error><Code>AccessDenied</Code></Error>"},
                    {"httpStatus": 400, "response": {"Code": "InvalidArgument", "clientToken": "admitted"}}):
            with self.assertRaises(ValueError):
                validate_rejection(bad, "InvalidArgument")

    def receipt(self, disposition="metadata_healthy"):
        return {"summary": "finished", "outcome": {"execution": {"state": "completed"},
                "coverage": "complete", "objectsTruncated": False,
                "counters": {"processed": 1, "healed": 0, "unchanged": 1, "skipped": 0,
                             "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False},
                "objects": [{"identity": {"bucket": "fixture", "object": "object.bin", "poolIndex": 0,
                                          "setIndex": 0, "bucketIncarnationId": "incarnation"},
                             "disposition": {"state": disposition}}]}}

    def test_metadata_is_not_deep_integrity_and_legacy_finish_is_not_success(self):
        healthy = self.receipt()
        validate_healthy(healthy, "fixture", "object.bin", "metadata_healthy")
        with self.assertRaisesRegex(ValueError, "expected verified_healthy"):
            validate_healthy(healthy, "fixture", "object.bin", "verified_healthy")
        validate_healthy(self.receipt("verified_healthy"), "fixture", "object.bin", "verified_healthy")
        for changed in ({"summary": "finished"}, {**healthy, "truncated": True}):
            with self.assertRaises(ValueError):
                validate_healthy(changed, "fixture", "object.bin", "metadata_healthy")

    def test_rejects_wrong_identity_unknown_counters_and_duplicate_receipts(self):
        for kind in ("wrong-object", "unknown", "missing-incarnation", "duplicate", "boolean-counter"):
            value = copy.deepcopy(self.receipt())
            outcome = value["outcome"]
            if kind == "wrong-object": outcome["objects"][0]["identity"]["object"] = "other.bin"
            elif kind == "unknown": outcome["counters"]["unknown"] = 1
            elif kind == "missing-incarnation": outcome["objects"][0]["identity"].pop("bucketIncarnationId")
            elif kind == "duplicate": outcome["objects"].append(outcome["objects"][0])
            else: outcome["counters"]["processed"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate_healthy(value, "fixture", "object.bin", "metadata_healthy")

    def test_h2_requires_one_set_spanning_all_sixteen_disks(self):
        disks = [str(index) for index in range(16)]
        snapshot = {f"node{node}": {"formats": [{"value": {"format": "xl", "id": "pool",
                   "xl": {"sets": [disks], "this": disks[(node - 1) * 4 + disk]}}} for disk in range(4)]}
                    for node in range(1, 5)}
        self.assertEqual(validate_topology(snapshot, "H2")["independentDrives"], 16)
        bad = copy.deepcopy(snapshot)
        bad["node4"]["formats"][0]["value"]["xl"]["this"] = "0"
        with self.assertRaisesRegex(ValueError, "mapping differs"):
            validate_topology(bad, "H2")
        with self.assertRaisesRegex(ValueError, "node count"):
            validate_topology({"node1": snapshot["node1"]}, "H2")

    def test_node_paths_cannot_select_shared_roots_or_arbitrary_run(self):
        with tempfile.TemporaryDirectory() as directory, patch("scripts.validation_heal.fleet_node.STATE_ROOT", Path(directory).resolve()):
            state, roots = paths("gh-12345-1-h1")
        self.assertTrue(all(root.parts[-2:] == ("heal-validation", "gh-12345-1-h1") for root in roots))
        for value in ("../mnmd", "manual-arbitrary", "gh-12345-1-h4"):
            with self.assertRaisesRegex(ValueError, "run ID"):
                paths(value)



if __name__ == "__main__":
    unittest.main()
