"""Check preservation of Heal branches and prevent evidence from becoming PASS."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import copy
import types
from unittest.mock import patch

from scripts.validation_heal.matrix import STAGES, TOPOLOGIES, audit_evidence, cases, compile_plan, select
from scripts.validation_heal.healthy import validate_healthy, validate_topology, validate_rejection, Fleet
from scripts.validation_heal.fleet_node import paths, stopped
from scripts.validation_heal.inspector import verify as verify_inspector
from scripts.validation_heal.shards import BRANCHES, map_target, validate_fault, validate_repaired_physical, validate_repaired_status, peer_status_equal, validate_dry_run_status
from scripts.validation_heal.heal_fault import mutation
from scripts.validation_heal.shards import validate_conditional_response


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
    def test_inspector_rejects_other_candidate_tampered_binary_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            header = bytearray(20)
            header[:6] = b"\x7fELF\x02\x01"
            header[18:20] = b"\x3e\x00"
            (root / "heal-meta-inspect").write_bytes(header)
            (root / "inspector-Cargo.lock").write_text("unit fixture lock")
            helper = Path(__file__).parent / "scripts/validation_heal/tools/heal_meta_inspect.rs"
            manifest = {"built": True, "candidateCommit": "a" * 40, "candidateLockSha256": "b" * 64,
                        "role": "read-only decoder; not the RustFS service under test", "target": "x86_64-unknown-linux-gnu",
                        "binarySha256": hashlib.sha256(header).hexdigest(),
                        "helperSourceSha256": hashlib.sha256(helper.read_bytes()).hexdigest(),
                        "inspectorLockSha256": hashlib.sha256((root / "inspector-Cargo.lock").read_bytes()).hexdigest()}
            (root / "heal-meta-inspect.json").write_text(json.dumps(manifest))
            self.assertEqual(verify_inspector(root, {"source_sha": "a" * 40}), manifest)
            with self.assertRaisesRegex(ValueError, "candidate"):
                verify_inspector(root, {"source_sha": "c" * 40})
            (root / "heal-meta-inspect").write_bytes(header + b"different bytes")
            with self.assertRaisesRegex(ValueError, "digest"):
                verify_inspector(root, {"source_sha": "a" * 40})
            (root / "heal-meta-inspect").unlink()
            (root / "heal-meta-inspect").symlink_to(root / "inspector-Cargo.lock")
            with self.assertRaisesRegex(ValueError, "artifact file"):
                verify_inspector(root, {"source_sha": "a" * 40})

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


class HealShardOracleTest(unittest.TestCase):
    def test_condition_denial_is_not_a_transport_failure_or_success_payload(self):
        baseline = {"properties": {"ETag": '"etag"'}, "payload": {"size": 3, "sha256": "correct"}}
        good = {"response": {"ResponseMetadata": {"HTTPStatusCode": 412}, "Error": {"Code": "PreconditionFailed"}},
                "acknowledged": False, "resultUnknown": False}
        validate_conditional_response(good, 412, baseline, code="PreconditionFailed")
        for changed in ({**good, "payload": {"size": 3}}, {**good, "resultUnknown": True}, {**good, "acknowledged": True}):
            with self.assertRaises(ValueError): validate_conditional_response(changed, 412, baseline, code="PreconditionFailed")
        success = {"response": {"ResponseMetadata": {"HTTPStatusCode": 200}, "ETag": '"etag"'},
                   "acknowledged": True, "payload": baseline["payload"]}
        validate_conditional_response(success, 200, baseline, full_body=True)
        with self.assertRaisesRegex(ValueError, "full content"):
            validate_conditional_response({**success, "payload": {"size": 3, "sha256": "wrong"}}, 200, baseline, full_body=True)
    def test_ssh_script_compiles_with_fault_module_and_keeps_structured_request(self):
        observed = []
        def invoke(args, **params):
            source = params["input"]
            compile(source, "ssh-node-script", "exec")
            observed.append(source)
            return types.SimpleNamespace(returncode=0, stdout='{"checked":true}', stderr="")
        with tempfile.TemporaryDirectory() as directory, patch("scripts.validation_heal.healthy.subprocess.run", side_effect=invoke):
            fleet = Fleet(["node1"], Path(directory), "gh-12345-1-h1")
            for action in ("preflight", "verify-part", "apply-fault", "recover-fault"):
                self.assertTrue(fleet.remote("node1", action, disk=1, relativePath="literal$()'part.1")["checked"])
        self.assertEqual(len(observed), 4)

    def snapshot(self):
        bucket, key, data_dir = "rv-heal-0123456789abcdef", "objects/0008.bin", "12345678-1234-1234-1234-123456789abc"
        disks, distribution = [str(index) for index in range(4)], [4, 1, 2, 3]
        observation = {"formats": [], "files": []}
        for disk, index in enumerate(distribution):
            observation["formats"].append({"disk": disk, "value": {"format": "xl", "id": "pool",
                "xl": {"sets": [disks], "this": disks[disk]}}})
            metadata = {"bucket": bucket, "key": key, "versions": [{"versionId": None, "isLatest": True,
                "deleted": False, "dataDir": data_dir, "size": 100, "inline": None, "usesLegacyChecksum": False,
                "role": "data" if index <= 2 else "parity", "parts": [{"number": 1, "size": 100, "actualSize": 100}],
                "erasure": {"index": index, "data_blocks": 2, "parity_blocks": 2, "block_size": 1048576,
                            "distribution": distribution, "algorithm": "rs-vandermonde"}}]}
            observation["files"].extend([
                {"disk": disk, "path": f"{bucket}/{key}/xl.meta", "size": 600, "sha256": "m" + str(disk), "metadata": metadata},
                {"disk": disk, "path": f"{bucket}/{key}/{data_dir}/part.1", "size": 82, "sha256": "s" + str(disk)},
                {"disk": disk, "path": f"{bucket}/control/xl.meta", "size": 600, "sha256": "c" + str(disk)}])
        return {"node1": observation}

    def target(self, before, role="data"):
        return map_target(before, "rv-heal-0123456789abcdef", "objects/0008.bin", role, 1)

    def test_executor_preserves_each_source_branch_and_uses_distinct_objects(self):
        inventory = {item["id"]: item for item in cases()}
        for case_id, branches in BRANCHES.items():
            self.assertEqual([branch[0] for branch in branches], inventory[case_id]["variants"])
            self.assertEqual(len({branch[1] for branch in branches}), len(branches))
            self.assertEqual(inventory[case_id]["contract"]["probes"]["topology"],
                             "H2" if case_id == "heal.admin-token-cross-node-status" else "H1,H2")

    def test_peer_status_cannot_converge_with_wrong_task_unknown_or_incomplete_receipts(self):
        coordinator = {**HealHealthyOracleTest().receipt("repaired"), "startTime": "exact-task-start"}
        self.assertTrue(peer_status_equal(coordinator, copy.deepcopy(coordinator)))
        for invalid in ("task-start", "summary", "coverage", "outcome", "truncated"):
            peer = copy.deepcopy(coordinator)
            if invalid == "task-start": peer["startTime"] = "different task"
            elif invalid == "summary": peer["summary"] = "unknown"
            elif invalid == "coverage": peer["outcome"]["coverage"] = "partial"
            elif invalid == "outcome": peer["outcome"]["counters"]["unknown"] = 1
            else: peer["truncated"] = True
            with self.subTest(invalid=invalid): self.assertFalse(peer_status_equal(coordinator, peer))

    def test_target_role_comes_from_metadata_and_actual_format_slot(self):
        before = self.snapshot()
        self.assertEqual(self.target(before)["disk"], 1)
        self.assertEqual(self.target(before, "parity")["disk"], 0)
        for invalid in ("index", "role", "dataDir", "missing-part", "same-shards"):
            changed = copy.deepcopy(before)
            metadata = changed["node1"]["files"][0]["metadata"]["versions"][0]
            if invalid == "index": metadata["erasure"]["index"] = 1
            elif invalid == "role": metadata["role"] = "data"
            elif invalid == "dataDir": metadata["dataDir"] = "../control"
            elif invalid == "missing-part": changed["node1"]["files"].pop(1)
            else:
                for entry in changed["node1"]["files"]:
                    if entry["path"].endswith("part.1"): entry["sha256"] = "same"
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.target(changed)

    def test_fault_requires_exact_target_and_unchanged_controls_and_metadata(self):
        before = self.snapshot()
        target = self.target(before)
        after = copy.deepcopy(before)
        after["node1"]["files"] = [entry for entry in after["node1"]["files"]
            if not (entry["disk"] == target["disk"] and entry["path"] == target["relativePath"])]
        receipt = {"relativePath": target["relativePath"], "before": {"sha256": target["sha256"], "size": 82}, "fault": None}
        validate_fault(before, after, target, receipt)
        with self.assertRaisesRegex(ValueError, "baseline file"):
            validate_repaired_physical(before, after, target)
        changed = copy.deepcopy(after)
        changed["node1"]["files"][0]["sha256"] = "metadata changed"
        with self.assertRaisesRegex(ValueError, "metadata, controls"):
            validate_fault(before, changed, target, receipt)
        validate_repaired_physical(before, before, target)
        rewritten = copy.deepcopy(before)
        rewritten["node1"]["files"][0]["sha256"] = "legitimate rewrite"
        rewritten["node1"]["files"][0]["metadata"]["versions"][0]["erasure"]["checksums"] = ["new checksum record"]
        validate_repaired_physical(before, rewritten, target)
        rewritten["node1"]["files"][0]["metadata"]["versions"][0]["dataDir"] = "other generation"
        with self.assertRaisesRegex(ValueError, "version/part identity"):
            validate_repaired_physical(before, rewritten, target)
        changed = copy.deepcopy(before)
        changed["node1"]["files"][-1]["sha256"] = "control changed"
        with self.assertRaisesRegex(ValueError, "changed a control"):
            validate_repaired_physical(before, changed, target)

    def test_completed_mpu_sidecar_cleanup_is_bound_to_target_drive_and_manifest(self):
        before = self.snapshot()
        target = self.target(before)
        path = target["relativePath"] + ".meta"
        before["node1"]["files"].append({"disk": target["disk"], "path": path, "size": 100, "sha256": "old staging"})
        after = copy.deepcopy(before)
        after["node1"]["files"].pop()
        validate_repaired_physical(before, after, target)
        before["node1"]["files"][-1]["disk"] = 0
        with self.assertRaisesRegex(ValueError, "lost a baseline file"):
            validate_repaired_physical(before, after, target)

    def test_unknown_or_wrong_version_receipt_cannot_certify_physical_repair(self):
        target = self.target(self.snapshot())
        receipt = HealHealthyOracleTest().receipt("repaired")
        receipt["outcome"]["counters"].update(healed=1, unchanged=0)
        receipt["outcome"]["objects"][0]["identity"].update(bucket=target["bucket"], object=target["key"], versionId=None)
        validate_repaired_status(receipt, target)
        for invalid in ("unknown", "wrong-version", "missing-incarnation"):
            changed = copy.deepcopy(receipt)
            if invalid == "unknown": changed["outcome"]["counters"]["unknown"] = 1
            elif invalid == "wrong-version": changed["outcome"]["objects"][0]["identity"]["versionId"] = "another"
            else: changed["outcome"]["objects"][0]["identity"].pop("bucketIncarnationId")
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_repaired_status(changed, target)

    def test_dry_run_requires_all_version_marker_receipts_and_never_claims_repair(self):
        target = self.target(self.snapshot())
        expected = [{"key": target["key"], "versionId": version} for version in ("old", "new", "marker")]
        receipt = HealHealthyOracleTest().receipt("dry_run_observed")
        receipt["outcome"]["counters"].update(processed=3, unchanged=0, skipped=3)
        original = receipt["outcome"]["objects"][0]
        original["identity"].update(bucket=target["bucket"], object=target["key"], bucketIncarnationId=None)
        receipt["outcome"]["objects"] = [copy.deepcopy(original) for _ in expected]
        for entry, version in zip(receipt["outcome"]["objects"], expected): entry["identity"]["versionId"] = version["versionId"]
        validate_dry_run_status(receipt, target, expected)
        for invalid in ("missing-marker", "repaired", "unknown", "healed-counter"):
            value = copy.deepcopy(receipt)
            if invalid == "missing-marker": value["outcome"]["objects"].pop()
            elif invalid == "healed-counter": value["outcome"]["counters"]["healed"] = 1
            else: value["outcome"]["objects"][0]["disposition"]["state"] = invalid
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_dry_run_status(value, target, expected)

    def test_fault_requires_actual_service_quiescence_and_exact_truncate_length(self):
        for value in ("MainPID=99\nActiveState=inactive\nControlGroup=\n", "MainPID=0\nActiveState=active\n", ""):
            with patch("scripts.validation_heal.fleet_node.command", return_value=types.SimpleNamespace(stdout=value)):
                with self.assertRaisesRegex(ValueError, "service to be stopped"):
                    stopped({"unit": "owned.service"})
        with patch("scripts.validation_heal.fleet_node.command", return_value=types.SimpleNamespace(
                stdout="MainPID=0\nActiveState=inactive\nControlGroup=\n")):
            stopped({"unit": "owned.service"})
        self.assertEqual(mutation(b"abcdef", "truncate", 5), b"abcde")
        for length in (-1, 6, True):
            with self.assertRaisesRegex(ValueError, "truncate length"):
                mutation(b"abcdef", "truncate", length)


if __name__ == "__main__":
    unittest.main()
