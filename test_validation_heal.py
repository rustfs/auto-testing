"""Check preservation of Heal branches and prevent evidence from becoming PASS."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import copy
import gzip
from datetime import UTC, datetime, timedelta
import types
from unittest.mock import patch

from scripts.validation_heal.matrix import STAGES, TOPOLOGIES, audit_evidence, cases, compile_plan, select
from scripts.validation_heal.healthy import validate_healthy, validate_topology, validate_rejection, Fleet
from scripts.validation_heal.fleet_node import paths, stopped, inspect_metadata
from scripts.validation_heal.inspector import verify as verify_inspector
from scripts.validation_heal.shards import BRANCHES, map_target, validate_fault, validate_repaired_physical, validate_repaired_status, peer_status_equal, validate_dry_run_status
from scripts.validation_heal.heal_fault import mutation
from scripts.validation_heal.shards import validate_conditional_response
from scripts.validation_heal.shards import run_task, verify_ranges
from scripts.validation_heal.versions import (BRANCHES as VERSION_BRANCHES, validate_version_receipts,
    branch_plan, suspend_fixture, validate_failed_version, validate_unrecoverable_get, verify_quorum_contract,
    map_inline_target, verify_proof)
from scripts.validation_heal.versions import seed_semantics, verify_semantics
from scripts.validation_heal.versions import multi_loss_instances, multi_loss_plan, SPECIAL_KEYS, verify_pagination, boundary_plan
from scripts.validation_heal.heal_probe import Probe, validate_s3_request
from scripts.validation_heal.prepare_heal_meta_inspector import quorum_contract, size_contract
from scripts.validation_heal.boundaries import verify_size_contract, boundary_sizes, verify_boundary_ranges
from scripts.validation_heal.node_outage import quorum_plan, put_ack, delete_ack, replica_census, validate_deleted_read
from scripts.validation_heal.fleet_node import runtime_config, BACKGROUND_CONFIG
from scripts.validation_heal.object_lock import (validate_protection, validate_protected_delete, cleanup_locked_fleet,
    seed_locked, BRANCHES as LOCK_BRANCHES, KEYS as LOCK_KEYS, CLEANUP_SECONDS)
from scripts.validation_heal.versions import mismatch_plan, seed_mismatch
from scripts.validation_heal import heal_fault


def versioned_scene(keys, sizes=(100, 100, 61)):
    base = HealShardOracleTest().snapshot()
    bucket = "rv-heal-0123456789abcdef"
    expected = []
    snapshot = {"node1": {"formats": base["node1"]["formats"], "files": []}}
    for key in keys:
        for entry in base["node1"]["files"]:
            if "/control/" in entry["path"]: continue
            cloned = copy.deepcopy(entry)
            cloned["path"] = cloned["path"].replace("objects/0008.bin", key)
            if "metadata" in cloned:
                cloned["metadata"]["key"] = key
                versions = []
                for nonce, size in enumerate(sizes):
                    version = copy.deepcopy(cloned["metadata"]["versions"][0])
                    version.update(versionId=f"v{nonce}", isLatest=nonce == 2, size=size,
                                   dataDir=f"12345678-1234-1234-1234-{nonce:012d}")
                    version["parts"][0].update(size=size, actualSize=size)
                    versions.append(version)
                cloned["metadata"]["versions"] = versions
                snapshot["node1"]["files"].append(cloned)
            else:
                for nonce, size in enumerate(sizes):
                    part = copy.deepcopy(cloned)
                    part["path"] = part["path"].replace("12345678-1234-1234-1234-123456789abc", f"12345678-1234-1234-1234-{nonce:012d}")
                    part["sha256"] += f"v{nonce}"
                    part["size"] = (size + 1) // 2 + 32
                    snapshot["node1"]["files"].append(part)
        expected.extend({"key": key, "versionId": f"v{nonce}", "deleteMarker": False, "isLatest": nonce == 2,
                         "payload": {"size": size}} for nonce, size in enumerate(sizes))
    return snapshot, bucket, expected


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
    def test_ranges_at_one_megabyte_boundary_stay_inside_the_real_payload(self):
        for size in (1024**2 - 1, 1024**2, 1024**2 + 1):
            body = b"x" * size
            requested = []
            def s3(operation, params):
                start, end = map(int, params["Range"].removeprefix("bytes=").split("-"))
                self.assertTrue(0 <= start <= end < size)
                requested.append((start, end))
                value = body[start:end + 1]
                return {"acknowledged": True, "response": {"ResponseMetadata": {"HTTPStatusCode": 206},
                    "ContentRange": f"bytes {start}-{end}/{size}"},
                    "payload": {"size": len(value), "sha256": hashlib.sha256(value).hexdigest()}}
            verify_ranges(types.SimpleNamespace(s3=s3), types.SimpleNamespace(record=lambda label, value: value),
                          "fixture", "objects/0000.bin", expected_body=body)
            self.assertEqual(requested[-1][1], size - 1)
            if size > 1024**2:
                self.assertTrue(any(start < 1024**2 <= end for start, end in requested))

    def test_bucket_and_literal_object_routes_keep_the_original_token_for_status(self):
        for key, expected_path in (("", "/rustfs/admin/v3/heal/fixture"),
                                   ("literal%2F/key", "/rustfs/admin/v3/heal/fixture/literal%252F/key")):
            calls = []
            def admin(path, query, body):
                calls.append((path, copy.deepcopy(query), body))
                if not query:
                    return {"httpStatus": 200, "response": {"clientToken": "original-token"}}
                return {"httpStatus": 200, "response": HealHealthyOracleTest().receipt()}
            active, admissions = [], []
            run_task(types.SimpleNamespace(admin=admin), types.SimpleNamespace(record=lambda label, value: value),
                     {"bucket": "fixture", "key": key}, 2, active,
                     option_overrides={"recursive": True}, admissions=admissions)
            self.assertEqual(len(calls), 4)
            self.assertTrue(all(path == expected_path for path, _, _ in calls))
            self.assertTrue(all(query == [("clientToken", "original-token")] for _, query, _ in calls[1:]))
            self.assertTrue(json.loads(calls[0][2])["recursive"])
            self.assertEqual(active, [])
            self.assertEqual(admissions, [{"path": expected_path, "clientToken": "original-token"}])

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
            for action in ("preflight", "verify-part", "verify-inline", "apply-fault", "recover-fault"):
                self.assertTrue(fleet.remote("node1", action, disk=1, relativePath="literal$()'part.1")["checked"])
        self.assertEqual(len(observed), 5)

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
        for case_id, branches in VERSION_BRANCHES.items():
            self.assertEqual(list(branches), inventory[case_id]["variants"])

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

    def test_repeated_target_backup_requires_exact_current_metadata_and_target_identity(self):
        before = self.snapshot()
        target = self.target(before)
        meta = next(entry for entry in before["node1"]["files"]
                    if entry["disk"] == target["disk"] and entry["path"] == f"{target['bucket']}/{target['key']}/xl.meta")
        backup = {"disk": target["disk"], "path": f"{target['bucket']}/{target['key']}/{target['dataDir']}/xl.meta.bkp",
                  "size": 599, "sha256": "previous-repair-metadata"}
        before["node1"]["files"].append(backup)
        after = copy.deepcopy(before)
        after["node1"]["files"][-1].update(size=meta["size"], sha256=meta["sha256"])
        validate_repaired_physical(before, after, target)
        for invalid in ("bytes", "length", "other-drive", "other-generation", "payload"):
            changed_before, changed_after = copy.deepcopy(before), copy.deepcopy(after)
            if invalid == "bytes": changed_after["node1"]["files"][-1]["sha256"] = "not-authoritative"
            elif invalid == "length": changed_after["node1"]["files"][-1]["size"] += 1
            elif invalid == "other-drive":
                for value in (changed_before, changed_after): value["node1"]["files"][-1]["disk"] = 0
            elif invalid == "other-generation":
                for value in (changed_before, changed_after): value["node1"]["files"][-1]["path"] = backup["path"].replace(target["dataDir"], "other-generation")
            else:
                changed_after["node1"]["files"][1]["sha256"] = "changed-payload"
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_repaired_physical(changed_before, changed_after, target)
        added_before = self.snapshot()
        added_after = copy.deepcopy(added_before)
        added_after["node1"]["files"].append(after["node1"]["files"][-1])
        validate_repaired_physical(added_before, added_after, target)
        added_after["node1"]["files"][-1]["sha256"] = "unexpected"
        with self.assertRaisesRegex(ValueError, "unexpected physical file"):
            validate_repaired_physical(added_before, added_after, target)

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


class HealVersionOracleTest(unittest.TestCase):
    def test_size_contract_derives_distinct_actual_inline_limits_and_rejects_changed_policy(self):
        source = {
            "crates/ecstore/src/config/storageclass.rs": '''pub static DEFAULT_INLINE_BLOCK: usize = 128 * 1024;
                const DEFAULT_INLINE_OBJECT_BUDGET: usize = 2 * DEFAULT_INLINE_BLOCK;
                pub fn should_inline(&self, shard_size: i64, data_shards: usize, versioned: bool) -> bool {
                    if shard_size < 0 || data_shards == 0 { return false; }
                    let shard_size = shard_size as usize; let inline_block = self.effective_inline_block(data_shards);
                    if versioned { shard_size <= inline_block / 8 } else { shard_size <= inline_block }
                }
                pub(crate) fn effective_inline_block(&self, data_shards: usize) -> usize {
                    if data_shards == 0 { return 0; }
                    if self.initialized && self.inline_block_explicit { self.inline_block }
                    else { DEFAULT_INLINE_OBJECT_BUDGET.div_ceil(data_shards).min(DEFAULT_INLINE_BLOCK) }
                }''',
            "crates/ecstore/src/set_disk/mod.rs": '''fn inline_admission_shard_size(erasure: &coding::Erasure, stored_size: i64, actual_size: i64) -> i64 {
                if stored_size >= 0 { return erasure.shard_file_size(stored_size); } actual_size }''',
            "crates/ecstore/src/set_disk/ops/object.rs": '''let is_inline_buffer = storage_class_config.should_inline(
                inline_admission_shard_size(&erasure, put_object_size, data.actual_size()), erasure.data_shards, opts.versioned,
                ) && usize::try_from(inline_admission_size).is_ok_and(|size| size <= erasure.block_size);''',
            "crates/ecstore/src/erasure/coding/erasure.rs": '''pub fn calc_shard_size(block_size: usize, data_shards: usize) -> usize {
                block_size.div_ceil(data_shards) }'''}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for path, text in source.items():
                file = root / path
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(text)
            contract = size_contract(root, "a" * 40)
            verify_size_contract({"sizeContract": contract}, {"source_sha": "a" * 40})
            processes = {"node": {"storageClassEnvironment": {
                "RUSTFS_STORAGE_CLASS_INLINE_BLOCK": None, "RUSTFS_STORAGE_CLASS_STANDARD": None}}}
            for data, inline in ((2, 262144), (12, 262152)):
                target = {"logical": {"usesLegacyChecksum": False, "erasure": {"data_blocks": data, "block_size": 1048576}}}
                parameters = boundary_sizes(contract, target, processes)
                self.assertEqual(parameters["actualLogicalInlineLimit"], inline)
                self.assertEqual(parameters["groups"]["inline-minus-exact-plus"], [inline - 1, inline, inline + 1])
                self.assertEqual(parameters["groups"]["stripe-minus-exact-plus"], [1048575, 1048576, 1048577])
                with self.assertRaises(ValueError): boundary_sizes(contract, target, {"node": {"storageClassEnvironment": {}}})
            for field, value in (("recognized", False), ("extractorSourceSha256", "0" * 64), ("candidateCommit", "b" * 40)):
                with self.assertRaises(ValueError):
                    verify_size_contract({"sizeContract": {**contract, field: value}}, {"source_sha": "a" * 40})
            path = root / "crates/ecstore/src/erasure/coding/erasure.rs"
            path.write_text(source[str(path.relative_to(root))].replace("div_ceil", "div_floor"))
            self.assertFalse(size_contract(root, "a" * 40)["recognized"])

    def test_zero_boundary_faults_metadata_and_never_invents_a_part(self):
        snapshot = HealShardOracleTest().snapshot()
        observation = snapshot["node1"]
        observation["files"] = [entry for entry in observation["files"] if "metadata" in entry]
        for entry in observation["files"]:
            version = entry["metadata"]["versions"][0]
            version["size"] = 0
            version["parts"][0].update(size=0, actualSize=0)
        expected = [{"key": "objects/0008.bin", "versionId": "null", "deleteMarker": False,
                     "payload": {"size": 0, "sha256": hashlib.sha256(b"").hexdigest()}}]
        instance = {"key": expected[0]["key"], "size": 0, "role": "parity", "expectedShape": "zero-metadata", "kind": "bitflip"}
        plan = boundary_plan(instance, snapshot, "rv-heal-0123456789abcdef", expected)
        self.assertEqual(plan["kind"], "header")
        self.assertTrue(plan["proofs"][0]["zeroProof"])
        self.assertEqual(plan["faults"][0]["role"], "parity")
        self.assertTrue(plan["faults"][0]["relativePath"].endswith("/xl.meta"))
        observation["files"].append(next(entry for entry in HealShardOracleTest().snapshot()["node1"]["files"] if "metadata" not in entry))
        with self.assertRaisesRegex(ValueError, "zero object"):
            boundary_plan(instance, snapshot, "rv-heal-0123456789abcdef", expected)

    def test_actual_size_ranges_require_206_bytes_and_empty_range_requires_exact_416(self):
        from scripts.validation_heal.heal_probe import payload
        bucket, block = "fixture", 4096
        expected = [{"key": str(size), "payload": {"size": size}} for size in (0, 1, 4097)]
        calls = []
        def s3(operation, params):
            size = int(params["Key"])
            calls.append(params)
            if not size:
                return {"acknowledged": False, "resultUnknown": False, "response": {
                    "ResponseMetadata": {"HTTPStatusCode": 416}, "Error": {"Code": "InvalidRange"}}}
            start, end = map(int, params["Range"].removeprefix("bytes=").split("-"))
            value = payload(f"{bucket}/{params['Key']}/0", size)[start:end + 1]
            return {"acknowledged": True, "response": {"ResponseMetadata": {"HTTPStatusCode": 206},
                    "ContentRange": f"bytes {start}-{end}/{size}"},
                    "payload": {"size": len(value), "sha256": hashlib.sha256(value).hexdigest()}}
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        verify_boundary_ranges(types.SimpleNamespace(s3=s3), fleet, bucket, expected, block)
        self.assertTrue(any(params["Range"] == "bytes=4079-4096" for params in calls))
        for wrong in ({"acknowledged": False, "resultUnknown": True}, {"acknowledged": True, "payload": {"size": 0}}):
            with self.assertRaises(ValueError):
                verify_boundary_ranges(types.SimpleNamespace(s3=lambda *args: wrong), fleet, bucket, expected[:1], block)

    def test_explicit_small_list_pages_preserve_distinct_special_keys(self):
        bucket = "rv-heal-pages-unit"
        keys = [*SPECIAL_KEYS, *(f"pages/{index:04d}.bin" for index in range(257))]
        for failure in (None, "duplicate-version", "omitted-current", "oversized-page"):
            calls = []
            def paginator(operation):
                def paginate(**params):
                    calls.append((operation, params))
                    self.assertEqual(params["PaginationConfig"], {"PageSize": 17})
                    listed = list(keys)
                    if failure == "duplicate-version" and operation == "list_object_versions": listed[-1] = listed[0]
                    if failure == "omitted-current" and operation == "list_objects_v2": listed.pop()
                    width = 18 if failure == "oversized-page" else 17
                    for start in range(0, len(listed), width):
                        page = listed[start:start + width]
                        yield {"Versions": [{"Key": key, "VersionId": "null", "IsLatest": True} for key in page]} \
                            if operation == "list_object_versions" else {"Contents": [{"Key": key, "Size": 1, "ETag": key} for key in page]}
                return types.SimpleNamespace(paginate=paginate)
            def s3(operation, params):
                if operation == "get_object":
                    return {"acknowledged": True, "response": {"ETag": params["Key"]}, "payload": {"size": 1, "sha256": "digest"}}
                return {"acknowledged": True, "response": {}}
            probe = Probe.__new__(Probe)
            probe.binding = {"bucketPrefix": bucket, "maxInventoryObjects": 512, "maxInventoryBytes": 512}
            probe.s3_client = lambda: types.SimpleNamespace(get_paginator=paginator)
            probe.s3 = s3
            if failure:
                with self.subTest(failure=failure), self.assertRaises(ValueError):
                    probe.inventory(bucket, unversioned_current_tags=True, page_size=17)
            else:
                observed = probe.inventory(bucket, unversioned_current_tags=True, page_size=17)
                proof = verify_pagination(observed)
                self.assertEqual(proof["acknowledgedObjects"], 265)
                self.assertEqual(proof["currentPages"], 16)
                self.assertEqual(proof["versionPages"], 16)
                self.assertEqual({entry["key"] for entry in observed["entries"]}, set(keys))

    def test_multi_loss_covers_every_actual_loss_count_and_role_composition(self):
        cohort = {"logical": {"erasure": {"data_blocks": 12, "parity_blocks": 4}}, "cohort": list(range(16))}
        expected = [{"key": "object", "versionId": "old", "deleteMarker": False}]
        with patch("scripts.validation_heal.versions.choose_target", return_value=cohort):
            instances = multi_loss_instances({}, "fixture", expected, "DV")
        self.assertEqual(len(instances), 14)
        self.assertEqual({(item["loss"], item["dataLoss"], item["parityLoss"]) for item in instances},
                         {(loss, data, loss - data) for loss in range(1, 5) for data in range(loss + 1)})
        self.assertEqual(len({item["key"] for item in instances}), 14)
        self.assertEqual({item["sourceBranch"] for item in instances}, set(VERSION_BRANCHES["heal.multiple-shard-loss"]))
        for item in instances:
            self.assertGreaterEqual(item["remainingSameVersionShards"], item["readQuorum"])
        before = HealShardOracleTest().snapshot()
        entry = {"key": "objects/0008.bin", "versionId": "null", "deleteMarker": False}
        instance = {**instances[2], "key": entry["key"], "dataLoss": 1, "parityLoss": 1,
                    "loss": 2, "readQuorum": 2}
        plan = multi_loss_plan(instance, before, "rv-heal-0123456789abcdef", [entry], "DB")
        self.assertEqual({item["role"] for item in plan["faults"]}, {"data", "parity"})
        self.assertEqual(len({item["disk"] for item in plan["faults"]}), 2)
        self.assertEqual(plan["requiredRepairs"], {(entry["key"], "null")})
        self.assertTrue(all(item["versionId"] == "null" for item in plan["faults"]))
        with self.assertRaisesRegex(ValueError, "quorum"):
            multi_loss_plan({**instance, "dataLoss": 3, "loss": 4}, before,
                            "rv-heal-0123456789abcdef", [entry], "DB")

    def test_metadata_fixture_has_real_encoded_content_and_exact_distinct_version_tags(self):
        uploaded = []
        def call(operation, params, body=None):
            if operation == "put_object":
                self.assertEqual(params["ContentEncoding"], "gzip")
                self.assertTrue(gzip.decompress(body))
                self.assertEqual(len(gzip.decompress(body)), 5 * 1024**2 + 123)
                self.assertTrue(all(value.isascii() for value in params["Metadata"].values()))
                uploaded.append((params["Key"], params["Metadata"]["generation"]))
                return {"VersionId": "version-" + str(len(uploaded))}
            return {}
        expected, public = seed_semantics(types.SimpleNamespace(call=call), "fixture")
        self.assertEqual(len(expected), 9)
        self.assertEqual(len({entry["versionId"] for entry in expected}), 9)
        self.assertEqual(len({key for key, _ in uploaded}), 3)
        baseline = {"entries": [{"key": spec["key"], "versionId": spec["versionId"],
                   "properties": copy.deepcopy(spec["properties"]), "tags": copy.deepcopy(spec["tags"])} for spec in public]}
        verify_semantics(public, baseline)
        for kind in ("header", "metadata", "versioned-tags"):
            changed = copy.deepcopy(baseline)
            if kind == "header": changed["entries"][0]["properties"]["ContentEncoding"] = "identity"
            elif kind == "metadata": changed["entries"][0]["properties"]["Metadata"]["userkey"] = "another"
            else: changed["entries"][0]["tags"] = copy.deepcopy(changed["entries"][1]["tags"])
            with self.subTest(kind=kind), self.assertRaises(ValueError): verify_semantics(public, changed)

    def test_decoder_failure_is_evidence_only_for_the_exact_witnessed_corruption(self):
        entry = {"path": "fixture/key/xl.meta", "sha256": "changed-sha", "size": 9}
        fault = {entry["path"]: {"sha256": "changed-sha", "size": 9}}
        rejected = types.SimpleNamespace(returncode=1, stderr="invalid captured metadata", stdout="")
        with patch("scripts.validation_heal.fleet_node.command", return_value=rejected):
            observed = inspect_metadata("tool", "path", "fixture", "key", entry, fault)
            self.assertFalse(observed["metadataError"]["validMetadataClaimed"])
            self.assertNotIn("metadata", observed)
            for incorrect in ({}, {entry["path"]: {"sha256": "different", "size": 9}},
                              {entry["path"]: {"sha256": "changed-sha", "size": 10}}):
                with self.assertRaises(ValueError): inspect_metadata("tool", "path", "fixture", "key", entry, incorrect)
        decoded = {"bucket": "fixture", "key": "another", "physicalBitrotVerified": False}
        with patch("scripts.validation_heal.fleet_node.command", return_value=types.SimpleNamespace(
                returncode=0, stdout=json.dumps(decoded))):
            with self.assertRaises(ValueError): inspect_metadata("tool", "path", "fixture", "key", entry, fault)

    def test_inline_fault_uses_unique_candidate_decoded_slice_and_frozen_role_digest(self):
        original = HealShardOracleTest().snapshot()
        bucket, key = "rv-heal-0123456789abcdef", "objects/0008.bin"
        for entry in original["node1"]["files"]:
            if "metadata" in entry:
                entry["metadata"]["versions"][0]["inline"] = {"length": 82, "rawOffset": 200,
                    "rawOccurrenceCount": 1, "sha256": str(entry["disk"]) * 64}
        expected = {"key": key, "versionId": "null", "payload": {"size": 100}}
        target = map_inline_target(original, bucket, expected)
        self.assertEqual(target["disk"], 1)
        def remote(node, action, **params):
            entry = next(entry for entry in target["cohort"] if entry["disk"] == params["disk"])
            return {"physicalBitrotVerified": True, "inlineLength": entry["inlineValue"]["length"], "inlineSha256": entry["inlineValue"]["sha256"]}
        fleet = types.SimpleNamespace(record=lambda label, value: value, remote=remote)
        self.assertTrue(verify_proof(fleet, target, "inline")['originalInlineSha256Restored'])
        with patch.object(fleet, "remote", return_value={"inlineLength": 82, "inlineSha256": "wrong"}):
            with self.assertRaises(ValueError): verify_proof(fleet, target, "inline")
        for kind in ("ambiguous", "outside", "wrong-role", "wrong-digest"):
            bad = copy.deepcopy(original)
            value = next(entry["metadata"]["versions"][0] for entry in bad["node1"]["files"] if "metadata" in entry)
            if kind == "ambiguous": value["inline"]["rawOccurrenceCount"] = 2
            elif kind == "outside": value["inline"]["rawOffset"] = 590
            elif kind == "wrong-role": value["role"] = "data"
            else: value["inline"]["sha256"] = "invalid"
            with self.subTest(kind=kind), self.assertRaises(ValueError): map_inline_target(bad, bucket, expected)

    def test_below_quorum_needs_exact_failure_and_never_accepts_transport_or_false_repair(self):
        status, expected = self.fixture()
        outcome = status["outcome"]
        outcome["execution"]["state"] = "completed_with_errors"
        outcome["objects"][0]["disposition"] = {"state": "failed", "details": "retry_exhausted"}
        outcome["counters"].update(unchanged=3, failed=1, attemptFailures=2)
        validate_failed_version(status, "fixture", expected, expected[0])
        for kind in ("completed", "unknown", "repaired", "wrong-version", "hidden-attempts", "wrong-counter"):
            value = copy.deepcopy(status)
            if kind == "completed": value["outcome"]["execution"]["state"] = "completed"
            elif kind in {"unknown", "repaired"}: value["outcome"]["objects"][0]["disposition"]["state"] = kind
            elif kind == "wrong-version": value["outcome"]["objects"][0]["identity"]["versionId"] = "different"
            elif kind == "hidden-attempts": value["outcome"]["counters"]["attemptFailures"] = 0
            else: value["outcome"]["counters"]["failed"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate_failed_version(value, "fixture", expected, expected[0])
        contract = {"unrecoverableGet": {"httpStatus": 503, "code": "SlowDownRead"}}
        observation = {"acknowledged": False, "resultUnknown": False,
                       "response": {"ResponseMetadata": {"HTTPStatusCode": 503}, "Error": {"Code": "SlowDownRead"}}}
        validate_unrecoverable_get(observation, contract)
        for changed in ({**observation, "resultUnknown": True}, {**observation, "payload": {"size": 0}},
                        {**observation, "acknowledged": True}):
            with self.assertRaises(ValueError): validate_unrecoverable_get(changed, contract)

    def test_tool_freezes_recognized_candidate_quorum_and_rejects_stale_or_changed_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            error = root / "rustfs/src/error.rs"
            metadata = root / "crates/ecstore/src/set_disk/metadata.rs"
            error.parent.mkdir(parents=True)
            metadata.parent.mkdir(parents=True)
            error.write_text('const SLOW_DOWN_READ_CODE: &str = "SlowDownRead";\n'
                'StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _) => '
                '{ S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()) }\n'
                'S3ErrorCode::Custom(custom) if &**custom == SLOW_DOWN_READ_CODE => Some(StatusCode::SERVICE_UNAVAILABLE)')
            metadata.write_text('pub(super) fn object_quorum_from_meta() {\n'
                'let data_blocks = parts_metadata.len() as i32 - parity_blocks;\nOk((data_blocks, write_quorum))\n}')
            contract = quorum_contract(root, "a" * 40)
            self.assertTrue(contract["recognized"])
            verify_quorum_contract({"quorumContract": contract}, {"source_sha": "a" * 40})
            for changed in ({**contract, "candidateCommit": "b" * 40},
                            {**contract, "extractorSourceSha256": "0" * 64}, {**contract, "recognized": False}):
                with self.assertRaises(ValueError):
                    verify_quorum_contract({"quorumContract": changed}, {"source_sha": "a" * 40})
            metadata.write_text('pub(super) fn object_quorum_from_meta() {\n'
                'let data_blocks = parts_metadata.len() as i32 - parity_blocks;\n'
                '// Ok((data_blocks, write_quorum))\nOk((data_blocks + 1, write_quorum))\n}')
            self.assertFalse(quorum_contract(root, "a" * 40)["recognized"])

    def test_suspended_null_fixture_retains_historical_identities_and_requires_real_marker_acks(self):
        historical = [{"key": f"objects/{index:04d}.bin", "versionId": f"history-{index}-{version}",
                       "deleteMarker": False, "isLatest": version == 2}
                      for index in range(3) for version in range(3)]
        calls = []
        def invoke(operation, params, body=None):
            calls.append((operation, copy.deepcopy(params)))
            if operation == "get_bucket_versioning": return {"Status": "Suspended"}
            if operation == "put_object": return {"VersionId": "null"}
            if operation == "delete_object": return {"VersionId": "null", "DeleteMarker": True}
            return {}
        recorder = types.SimpleNamespace(call=invoke)
        expected = suspend_fixture(recorder, "fixture", historical)
        self.assertEqual(len(expected), 12)
        self.assertEqual({item["versionId"] for item in expected if item["versionId"] != "null"},
                         {item["versionId"] for item in historical})
        self.assertTrue(all(not item["isLatest"] for item in expected if item["versionId"] != "null"))
        markers = [item for item in expected if item["deleteMarker"]]
        self.assertEqual(markers, [{"key": "objects/0002.bin", "versionId": "null", "isLatest": True, "deleteMarker": True}])
        self.assertEqual(sum(operation == "delete_object" and params.get("VersionId") == "null"
                             for operation, params in calls), 3)
        for kind in ("fake-version", "missing-marker"):
            def invalid(operation, params, body=None):
                result = invoke(operation, params, body)
                if kind == "fake-version" and operation == "put_object": result["VersionId"] = "unexpected-new-version"
                elif kind == "missing-marker" and operation == "delete_object": result["DeleteMarker"] = False
                return result
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                suspend_fixture(types.SimpleNamespace(call=invalid), "fixture", historical)
        self.assertTrue(all(item["isLatest"] == (index % 3 == 2) for index, item in enumerate(historical)))

    def fixture(self):
        expected = [{"key": "object", "versionId": version, "deleteMarker": version == "marker"}
                    for version in ("old", "middle", "current", "marker")]
        status = HealHealthyOracleTest().receipt()
        original = status["outcome"]["objects"][0]
        original["identity"].update(bucket="fixture", object="object")
        status["outcome"]["objects"] = [copy.deepcopy(original) for _ in expected]
        for receipt, entry in zip(status["outcome"]["objects"], expected):
            receipt["identity"]["versionId"] = entry["versionId"]
            receipt["disposition"]["state"] = "metadata_healthy" if entry["deleteMarker"] else "verified_healthy"
        status["outcome"]["counters"].update(processed=4, unchanged=4)
        return status, expected

    def test_all_version_receipts_and_counters_are_required(self):
        status, expected = self.fixture()
        status["outcome"]["objects"][0]["disposition"]["state"] = "repaired"
        status["outcome"]["counters"].update(healed=1, unchanged=3)
        validate_version_receipts(status, "fixture", expected, 2, required_repairs={("object", "old")})
        for kind in ("missing-marker", "duplicate", "wrong-version", "unknown", "incarnation", "boolean-counter", "healthy-fault"):
            bad = copy.deepcopy(status)
            objects = bad["outcome"]["objects"]
            if kind == "missing-marker": objects.pop()
            elif kind == "duplicate": objects[-1] = copy.deepcopy(objects[0])
            elif kind == "wrong-version": objects[0]["identity"]["versionId"] = "another"
            elif kind == "unknown": objects[0]["disposition"]["state"] = "unknown"
            elif kind == "incarnation": objects[0]["identity"]["bucketIncarnationId"] = "other"
            elif kind == "boolean-counter": bad["outcome"]["counters"]["healed"] = True
            else: objects[0]["disposition"]["state"] = "verified_healthy"
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate_version_receipts(bad, "fixture", expected, 2, required_repairs={("object", "old")})

    def test_repeat_requires_mode_specific_health_and_zero_repairs(self):
        status, expected = self.fixture()
        validate_version_receipts(status, "fixture", expected, 2)
        with self.assertRaises(ValueError): validate_version_receipts(status, "fixture", expected, 1)
        for entry in status["outcome"]["objects"]: entry["disposition"]["state"] = "metadata_healthy"
        validate_version_receipts(status, "fixture", expected, 1)
        with self.assertRaises(ValueError): validate_version_receipts(status, "fixture", expected, 2)
        status["outcome"]["objects"][0]["disposition"]["state"] = "repaired"
        status["outcome"]["counters"].update(healed=1, unchanged=3)
        with self.assertRaises(ValueError): validate_version_receipts(status, "fixture", expected, 1)

    def test_admin_nil_uuid_maps_only_to_the_acknowledged_s3_null_version(self):
        status, expected = self.fixture()
        expected[0]["versionId"] = "null"
        status["outcome"]["objects"][0]["identity"]["versionId"] = "00000000-0000-0000-0000-000000000000"
        validate_version_receipts(status, "fixture", expected, 2)
        wrong = copy.deepcopy(status)
        wrong["outcome"]["objects"][0]["identity"]["versionId"] = "00000000-0000-0000-0000-000000000001"
        with self.assertRaises(ValueError):
            validate_version_receipts(wrong, "fixture", expected, 2)
        expected[0]["versionId"] = "old"
        with self.assertRaises(ValueError):
            validate_version_receipts(status, "fixture", expected, 2)

    def test_metadata_repair_may_repair_all_receipts_but_marker_cannot_claim_deep_payload(self):
        status, expected = self.fixture()
        with self.assertRaisesRegex(ValueError, "never attributed"):
            validate_version_receipts(status, "fixture", expected, 2, metadata_repairs={"object"})
        for entry in status["outcome"]["objects"]: entry["disposition"]["state"] = "repaired"
        status["outcome"]["counters"].update(healed=4, unchanged=0)
        validate_version_receipts(status, "fixture", expected, 2, metadata_repairs={"object"})
        status["outcome"]["objects"][-1]["disposition"]["state"] = "verified_healthy"
        status["outcome"]["counters"].update(healed=3, unchanged=1)
        with self.assertRaises(ValueError):
            validate_version_receipts(status, "fixture", expected, 2, metadata_repairs={"object"})

    def test_version_fault_plan_uses_ack_versions_distinct_disks_and_exact_recursive_scope(self):
        original = HealShardOracleTest().snapshot()
        observation = original["node1"]
        templates = {entry["disk"]: entry for entry in observation["files"] if "metadata" in entry}
        shards = {entry["disk"]: entry for entry in observation["files"] if entry["path"].endswith("part.1")}
        snapshot = {"node1": {"formats": observation["formats"], "files": []}}
        bucket, expected = "rv-heal-0123456789abcdef", []
        for key_index in range(4):
            key = f"objects/{key_index:04d}.bin"
            version_ids = [f"00000000-0000-0000-0000-{key_index * 3 + index + 1:012d}" for index in range(3)]
            for index, version in enumerate(version_ids):
                expected.append({"key": key, "versionId": version, "deleteMarker": False,
                                 "isLatest": index == 2 and key_index % 2 == 1})
            if key_index % 2 == 0:
                expected.append({"key": key, "versionId": "marker-" + str(key_index), "deleteMarker": True, "isLatest": True})
            for disk in range(4):
                meta = copy.deepcopy(templates[disk])
                meta["path"] = f"{bucket}/{key}/xl.meta"
                meta["metadata"]["key"] = key
                version_template = meta["metadata"]["versions"][0]
                meta["metadata"]["versions"] = []
                for index, version in enumerate(version_ids):
                    value = copy.deepcopy(version_template)
                    value.update(versionId=version, dataDir=version, isLatest=index == 2 and key_index % 2 == 1)
                    meta["metadata"]["versions"].append(value)
                    shard = copy.deepcopy(shards[disk])
                    shard.update(path=f"{bucket}/{key}/{version}/part.1", sha256=f"{key_index}-{index}-{disk}")
                    snapshot["node1"]["files"].append(shard)
                snapshot["node1"]["files"].append(meta)
        for branch in VERSION_BRANCHES["heal.historical-versions"]:
            plan = branch_plan("heal.historical-versions", branch, snapshot, bucket, expected, "DV")
            self.assertTrue(plan["recursive"])
            if branch == "recursive-all-versions":
                self.assertEqual(plan["scope"]["key"], "")
                self.assertEqual(plan["expected"], expected)
                self.assertTrue(any(item["relativePath"].endswith("xl.meta") for item in plan["faults"]))
            if len(plan["faults"]) == 2:
                self.assertEqual(len({(item["node"], item["disk"]) for item in plan["faults"]}), 2)
        plan = branch_plan("heal.repeat-idempotency", "versioned-repeat", snapshot, bucket, expected, "DV")
        self.assertEqual(len(plan["expected"]), 4)
        self.assertEqual(len(plan["proofs"]), 3)
        self.assertTrue(any(entry["deleteMarker"] for entry in plan["expected"]))
        plan = branch_plan("heal.below-read-quorum", "r-minus-1-historical", snapshot, bucket, expected, "DV")
        self.assertEqual(plan["readQuorum"], 2)
        self.assertEqual(len(plan["retainedShards"]), 1)
        self.assertEqual(len(plan["faults"]), 3)
        self.assertEqual(len({entry["versionId"] for entry in plan["faults"]}), 1)
        self.assertTrue(all(entry["versionId"] == "00000000-0000-0000-0000-000000000004" for entry in plan["faults"]))


class HealNodeOutageTest(unittest.TestCase):
    def snapshot(self):
        bucket = "rv-heal-0123456789abcdef"
        identities = ["00000000-0000-0000-0000-" + f"{index:012d}" for index in range(1, 17)]
        expected = [{"key": "new.bin", "versionId": "new-version", "deleteMarker": False, "isLatest": False},
                    {"key": "new.bin", "versionId": "marker-version", "deleteMarker": True, "isLatest": True}]
        observation = {}
        data_dir = "00000000-0000-0000-0000-000000000100"
        for node_index in range(4):
            node = "node" + str(node_index + 1)
            value = {"formats": [], "files": []}
            for disk in range(4):
                slot = node_index * 4 + disk
                value["formats"].append({"disk": disk, "value": {"format": "xl", "id": "pool",
                    "xl": {"sets": [identities], "this": identities[slot]}}})
                version = {"versionId": "new-version", "deleted": False, "isLatest": False, "inline": None,
                    "dataDir": data_dir, "size": 1048623, "usesLegacyChecksum": False,
                    "role": "data" if slot < 8 else "parity", "parts": [{"number": 1, "size": 1048623, "actualSize": 1048623}],
                    "erasure": {"data_blocks": 8, "parity_blocks": 8, "index": slot + 1,
                        "distribution": list(range(1, 17)), "block_size": 1048576, "algorithm": "rs-vandermonde"}}
                marker = {"versionId": "marker-version", "deleted": True, "isLatest": True}
                value["files"].extend([
                    {"disk": disk, "path": f"{bucket}/new.bin/xl.meta", "sha256": "meta" + str(slot), "size": 1000,
                     "metadata": {"bucket": bucket, "key": "new.bin", "versions": [version, marker]}},
                    {"disk": disk, "path": f"{bucket}/new.bin/{data_dir}/part.1", "sha256": "shard" + str(slot), "size": 131200}])
            observation[node] = value
        return bucket, expected, observation

    def test_mrf_profile_is_explicit_and_cannot_enable_unrelated_background_sources(self):
        self.assertEqual(runtime_config("manual"), BACKGROUND_CONFIG)
        replay = runtime_config("offline-replay")
        self.assertEqual(replay.pop("RUSTFS_HEAL_MRF_ENABLE"), "true")
        self.assertEqual(replay, {key: value for key, value in BACKGROUND_CONFIG.items() if key != "RUSTFS_HEAL_MRF_ENABLE"})
        with self.assertRaises(ValueError): runtime_config("arbitrary")

    def test_outage_calculates_same_set_quorum_and_rejects_unrecognized_write_policy(self):
        bucket, expected, snapshot = self.snapshot()
        target = map_target(snapshot, bucket, "new.bin", "data", 1, "new-version", latest_required=False)
        contract = {"recognized": True, "writeQuorumRule": "data-plus-one-if-equal-parity"}
        plan = quorum_plan(target, "node4", contract)
        self.assertEqual((plan["lostSameSetDrives"], plan["remainingSameSetDrives"], plan["readQuorum"], plan["writeQuorum"]), (4, 12, 8, 9))
        for changed in ({**contract, "writeQuorumRule": None}, {**contract, "recognized": False}):
            with self.assertRaises(ValueError): quorum_plan(target, "node4", changed)
        with self.assertRaises(ValueError): quorum_plan(target, "foreign-node", contract)
        changed = copy.deepcopy(target)
        changed["logical"]["erasure"].update(data_blocks=13, parity_blocks=3)
        with self.assertRaises(ValueError): quorum_plan(changed, "node4", contract)

    def test_ack_manifest_preserves_history_and_rejects_ambiguous_put_delete_receipts(self):
        old = [{"key": "key", "versionId": "v1", "deleteMarker": False, "isLatest": False},
               {"key": "key", "versionId": "v2", "deleteMarker": False, "isLatest": True}]
        updated = put_ack(old, "key", b"new bytes", {"VersionId": "v3"}, "DV")
        self.assertEqual([entry["versionId"] for entry in updated if entry["isLatest"]], ["v3"])
        self.assertEqual(old[-1]["isLatest"], True)
        marked = delete_ack(updated, "key", {"VersionId": "marker", "DeleteMarker": True}, "DV")
        self.assertEqual([entry["versionId"] for entry in marked if entry["isLatest"]], ["marker"])
        deleted = delete_ack(marked, "key", {"VersionId": "v1"}, "DV", version="v1")
        self.assertEqual({entry["versionId"] for entry in deleted}, {"v2", "v3", "marker"})
        for response in ({}, {"VersionId": "null"}, {"VersionId": "v2"}):
            with self.subTest(response=response), self.assertRaises(ValueError): put_ack(old, "key", b"x", response, "DV")
        for response in ({}, {"VersionId": "marker", "DeleteMarker": False}):
            with self.subTest(response=response), self.assertRaises(ValueError): delete_ack(updated, "key", response, "DV")
        with self.assertRaises(ValueError): delete_ack(old, "key", {"VersionId": "v2"}, "DV", version="v2")
        unversioned = [{"key": "key", "versionId": "null", "deleteMarker": False, "isLatest": True}]
        self.assertEqual(len(put_ack(unversioned, "key", b"x", {}, "DB")), 1)
        self.assertEqual(delete_ack(unversioned, "key", {}, "DB"), [])

    def test_physical_census_requires_each_drive_and_preserves_surviving_authority(self):
        bucket, expected, repaired = self.snapshot()
        offline = copy.deepcopy(repaired)
        offline["node4"]["files"] = []
        self.assertEqual(len(replica_census(repaired, bucket, expected, offline)), 1)
        for poison in ("missing-one-replica", "missing-marker", "resurrected-version", "changed-survivor", "role-mismatch"):
            changed = copy.deepcopy(repaired)
            if poison == "missing-one-replica": changed["node4"]["files"].pop(0)
            elif poison == "missing-marker": changed["node4"]["files"][0]["metadata"]["versions"].pop()
            elif poison == "resurrected-version": changed["node4"]["files"][0]["metadata"]["versions"].append({"versionId": "deleted-version", "deleted": False, "isLatest": False})
            elif poison == "changed-survivor": changed["node1"]["files"][1]["sha256"] = "different"
            else: changed["node4"]["files"][0]["metadata"]["versions"][0]["role"] = "data"
            with self.subTest(poison=poison), self.assertRaises(ValueError): replica_census(changed, bucket, expected, offline)

    def test_candidate_write_quorum_recognition_is_independent_of_read_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rustfs/src").mkdir(parents=True)
            (root / "crates/ecstore/src/set_disk").mkdir(parents=True)
            (root / "rustfs/src/error.rs").write_text('const SLOW_DOWN_READ_CODE: &str = "SlowDownRead";\n'
                'StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _) => { S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()) }\n'
                'S3ErrorCode::Custom(custom) if &**custom == SLOW_DOWN_READ_CODE => Some(StatusCode::SERVICE_UNAVAILABLE)')
            metadata = root / "crates/ecstore/src/set_disk/metadata.rs"
            source = 'pub(super) fn object_quorum_from_meta() { let data_blocks = parts_metadata.len() as i32 - parity_blocks; let write_quorum = if data_blocks == parity_blocks { data_blocks + 1 } else { data_blocks }; Ok((data_blocks, write_quorum)) }'
            metadata.write_text(source)
            self.assertEqual(quorum_contract(root, "a" * 40)["writeQuorumRule"], "data-plus-one-if-equal-parity")
            metadata.write_text(source.replace("data_blocks + 1", "data_blocks + 2"))
            changed = quorum_contract(root, "a" * 40)
            self.assertTrue(changed["recognized"])
            self.assertIsNone(changed["writeQuorumRule"])
            metadata.write_text(source.replace("Ok((data_blocks, write_quorum))", "let write_quorum = write_quorum + 1; Ok((data_blocks, write_quorum))"))
            self.assertIsNone(quorum_contract(root, "a" * 40)["writeQuorumRule"])

    def test_deleted_identity_requires_exact_s3_absence_without_payload_or_transport_ambiguity(self):
        for version, code in (("null", "NoSuchKey"), ("deleted-version", "NoSuchVersion")):
            good = {"acknowledged": False, "resultUnknown": False,
                    "response": {"ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": code}}}
            validate_deleted_read(good, version)
            for changed in ({**good, "acknowledged": True}, {**good, "resultUnknown": True}, {**good, "payload": {}},
                            {**good, "response": {"ResponseMetadata": {"HTTPStatusCode": 403}, "Error": {"Code": "AccessDenied"}}}):
                with self.subTest(version=version), self.assertRaises(ValueError): validate_deleted_read(changed, version)


class HealPayloadMismatchTest(unittest.TestCase):
    def test_length_and_payload_donors_keep_original_metadata_and_exact_same_role(self):
        branches = VERSION_BRANCHES["heal.metadata-payload-mismatch"]
        scene, bucket, expected = versioned_scene(["mismatch/" + branch + ".bin" for branch in branches])
        original = copy.deepcopy(scene)
        for branch in branches:
            plan = mismatch_plan(branch, scene, bucket, expected)
            self.assertEqual(len(plan["expected"]), 3)
            self.assertEqual(plan["proofs"][0]["versionId"], "v0")
            self.assertEqual(plan["donorProof"]["versionId"], "v2" if branch == "length-mismatch" else "v1")
            self.assertEqual(len(plan["faults"]), 3 if plan["negative"] else 1)
            if plan["negative"]:
                self.assertEqual(len(plan["retainedShards"]), plan["readQuorum"] - 1)
                self.assertTrue({shard["index"] for shard in plan["retainedShards"]}.isdisjoint(
                    {shard["index"] for shard in plan["faults"]}))
            for fault in plan["faults"]:
                self.assertNotEqual(fault["donorSha256"], fault["sha256"])
                self.assertNotEqual(fault["donorRelativePath"], fault["relativePath"])
                self.assertEqual(fault["donorSize"] != fault["size"], branch == "length-mismatch")
        self.assertEqual(scene, original)
        broken = copy.deepcopy(scene)
        for item in broken["node1"]["files"]:
            if "metadata" in item:
                for version in item["metadata"]["versions"]:
                    if version["versionId"] == "v1":
                        version["erasure"]["block_size"] //= 2
        with self.assertRaisesRegex(ValueError, "different EC layout"):
            mismatch_plan("payload-mismatch", broken, bucket, expected)

    def replacement(self, directory, *, invalid=None):
        root = Path(directory).resolve()
        prefix = "rv-heal-mismatch-unit"
        target = prefix + "/key/old/part.1"
        donor = prefix + "/key/new/part.1"
        before, replacement = b"authoritative-old-shard", b"other-generation-shard"
        (root / "quarantine").mkdir()
        for relative, value in ((target, before), (donor, replacement)):
            path = root / "data" / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(value)
        sha = hashlib.sha256(replacement).hexdigest()
        if invalid == "hash": sha = "0" * 64
        elif invalid == "traversal": donor = prefix + "/key/../new/part.1"
        elif invalid == "bucket": donor = donor.replace(prefix, "unowned-bucket")
        elif invalid == "metadata": donor = donor.removesuffix("part.1") + "xl.meta"
        elif invalid == "symlink":
            link = root / "data" / donor
            link.unlink()
            link.symlink_to(root / "data" / target)
        elif invalid == "hardlink":
            import os
            os.link(root / "data" / donor, root / "second-link")
        elif invalid == "identical": donor = target
        owner = {"runId": "manual-mismatch-unit", "bucketPrefix": prefix}
        with patch.object(heal_fault, "mount_id", return_value=1), heal_fault.directory(root) as fd:
            if invalid:
                with self.assertRaises((ValueError, OSError)):
                    heal_fault.apply_fault(fd, owner, target, "C08-test", "replace-part", hashlib.sha256(before).hexdigest(),
                                           donor_relative=donor, donor_sha=sha)
                self.assertEqual((root / "data" / target).read_bytes(), before)
                self.assertFalse(list((root / "quarantine").iterdir()))
                return
            receipt = heal_fault.apply_fault(fd, owner, target, "C08-test", "replace-part", hashlib.sha256(before).hexdigest(),
                                             donor_relative=donor, donor_sha=sha)
            self.assertEqual((root / "data" / target).read_bytes(), replacement)
            self.assertEqual((root / "data" / donor).read_bytes(), replacement)
            self.assertEqual(receipt["donor"]["sha256"], sha)
            self.assertEqual((root / "quarantine" / "C08-test.original").read_bytes(), before)
            # A later generation is preserved instead of overwritten by rescue.
            (root / "data" / target).write_bytes(b"new-committed-generation")
            with self.assertRaisesRegex(ValueError, "changed after fault"):
                heal_fault.recover_fault(fd, owner, "C08-test")
            (root / "data" / target).write_bytes(replacement)
            restored = heal_fault.recover_fault(fd, owner, "C08-test")
            self.assertFalse(restored["physicalRepairPassAllowed"])
            self.assertEqual((root / "data" / target).read_bytes(), before)

    def test_replacement_is_recoverable_and_cannot_escape_owned_part_files(self):
        for invalid in (None, "hash", "traversal", "bucket", "metadata", "symlink", "hardlink", "identical"):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as directory:
                self.replacement(directory, invalid=invalid)

    def test_three_version_fixture_has_real_different_length_and_content_acks(self):
        observed = []
        def invoke(operation, params, body=None):
            observed.append((operation, params, body))
            return {"VersionId": str(len(observed))} if operation == "put_object" else {}
        with patch("scripts.validation_heal.versions.payload", side_effect=lambda identity, size: identity.encode()):
            expected = seed_mismatch(types.SimpleNamespace(call=invoke), "rv-heal-mismatch")
        self.assertEqual(len(expected), 9)
        self.assertEqual(sum(entry["isLatest"] for entry in expected), 3)
        for key in {entry["key"] for entry in expected}:
            group = [entry for entry in expected if entry["key"] == key]
            self.assertEqual(group[0]["payload"]["size"], group[1]["payload"]["size"])
            self.assertNotEqual(group[0]["payload"]["size"], group[2]["payload"]["size"])
            self.assertNotEqual(group[0]["payload"]["sha256"], group[1]["payload"]["sha256"])
        with self.assertRaises(ValueError):
            seed_mismatch(types.SimpleNamespace(call=lambda *args: {"VersionId": "null"}), "rv-heal-mismatch")


class HealObjectLockTest(unittest.TestCase):
    def test_governance_bypass_requires_explicit_owned_version_and_reaches_the_sdk(self):
        from test_validation_heal_authorization import binding
        bound = binding()
        params = {"Bucket": bound["bucketPrefix"], "Key": "locked/governance.bin", "VersionId": "owned-version",
                  "BypassGovernanceRetention": True}
        with self.assertRaises(ValueError): validate_s3_request(bound, "delete_object", params)
        validate_s3_request(bound, "delete_object", params, allow_governance_bypass=True)
        for operation, changed in (("put_object", params), ("delete_objects", params),
                                   ("delete_object", {**params, "Bucket": "unowned"}),
                                   ("delete_object", {**params, "VersionId": "null"}),
                                   ("delete_object", {**params, "BypassGovernanceRetention": 1})):
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                validate_s3_request(bound, operation, changed, allow_governance_bypass=True)
        sdk = unittest.mock.Mock()
        sdk.delete_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 204}}
        probe = Probe(bound)
        with patch.object(probe, "s3_client", return_value=sdk):
            self.assertTrue(probe.s3("delete_object", params, allow_governance_bypass=True)["acknowledged"])
        sdk.delete_object.assert_called_once_with(**params)

    def seed(self, *, fail_after=None):
        calls, context = [], {}
        fleet = types.SimpleNamespace(secret_values=set(), record=lambda label, value: value)
        principal = types.SimpleNamespace(binding={"runId": "gh-12345-lock"}, credentials=lambda: ("owner", "secret"))
        def invoke(operation, params, body=None):
            calls.append((operation, copy.deepcopy(params)))
            if fail_after is not None and operation == "put_object" and len([x for x in calls if x[0] == operation]) > fail_after:
                raise ValueError("partial protected fixture")
            if operation == "get_bucket_versioning": return {"Status": "Enabled"}
            if operation == "get_object_lock_configuration":
                return {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"},
                        "ResponseMetadata": {"HTTPHeaders": {"date": "Sat, 10 Oct 2026 12:00:00 GMT"}}}
            if operation == "put_object": return {"VersionId": "version-" + str(len(calls))}
            return {}
        recorder = types.SimpleNamespace(probe=principal, call=invoke)
        with patch("scripts.validation_heal.object_lock.Probe", return_value=principal), \
             patch("scripts.validation_heal.object_lock.payload", return_value=b"protected"), \
             patch("scripts.validation_heal.object_lock.iam_exchange") as iam:
            if fail_after is None:
                expected = seed_locked(recorder, fleet, "rv-heal-lock", context)
            else:
                with self.assertRaisesRegex(ValueError, "partial protected fixture"):
                    seed_locked(recorder, fleet, "rv-heal-lock", context)
                expected = None
        return expected, context, calls, iam.call_args_list

    def test_protected_seed_uses_real_server_time_and_keeps_partial_ownership(self):
        expected, context, calls, iam = self.seed()
        self.assertEqual(len(expected), 12)
        self.assertEqual(len({(entry["key"], entry["versionId"]) for entry in expected}), 12)
        self.assertEqual(context["retainUntil"], datetime(2026, 10, 10, 12, 10, tzinfo=UTC))
        self.assertEqual(sum(entry["isLatest"] for entry in expected), 4)
        statements = next(call.kwargs["document"]["Statement"] for call in iam if "document" in call.kwargs)
        self.assertEqual(statements[-1]["Action"], ["s3:BypassGovernanceRetention"])
        self.assertEqual(statements[-1]["Effect"], "Deny")
        for fail_after in (0, 1, 10):
            _, partial, _, _ = self.seed(fail_after=fail_after)
            self.assertTrue(partial["bucketAttempted"] and partial["policyCreated"] and partial["attached"])
            self.assertTrue(partial["user"].startswith("rvheal"))

    def test_protected_plan_freezes_all_versions_on_the_lost_metadata_drive(self):
        snapshot, bucket, expected = versioned_scene(LOCK_KEYS, (100, 100, 100))
        for index, branch in enumerate(LOCK_BRANCHES):
            plan = branch_plan("heal.object-lock-retention", branch, snapshot, bucket, expected, "DO")
            self.assertEqual({item["key"] for item in plan["expected"]}, {LOCK_KEYS[index]})
            self.assertEqual(plan["faults"][0]["versionId"], "v0" if index == 3 else "v2")
            if index < 2:
                self.assertEqual(plan["faults"][0]["role"], "parity" if index == 1 else "data")
            else:
                self.assertEqual(len(plan["proofs"]), 3)
                self.assertEqual({item["versionId"] for item in plan["proofs"]}, {"v0", "v1", "v2"})
                self.assertEqual(len({(item["node"], item["disk"]) for item in plan["proofs"]}), 1)
                self.assertEqual(len({item["sha256"] for item in plan["proofs"]}), 3)
                self.assertTrue(plan["faults"][0]["relativePath"].endswith("/xl.meta"))

    def test_retention_proof_needs_exact_mode_expiry_and_active_server_time(self):
        expiry = datetime(2026, 10, 10, 12, 5, tzinfo=UTC)
        spec = {"ObjectLockMode": "COMPLIANCE", "ObjectLockRetainUntilDate": expiry}
        good = {"acknowledged": True, "response": {"Retention": {"Mode": "COMPLIANCE", "RetainUntilDate": expiry},
                "ResponseMetadata": {"HTTPHeaders": {"date": "Sat, 10 Oct 2026 12:00:00 GMT"}}}}
        validate_protection(good, spec)
        for invalid in ("mode", "expiry", "expired", "missing-server-time", "failed"):
            changed = copy.deepcopy(good)
            if invalid == "mode": changed["response"]["Retention"]["Mode"] = "GOVERNANCE"
            elif invalid == "expiry": changed["response"]["Retention"]["RetainUntilDate"] -= timedelta(seconds=1)
            elif invalid == "expired": changed["response"]["ResponseMetadata"]["HTTPHeaders"]["date"] = "Sat, 10 Oct 2026 12:05:00 GMT"
            elif invalid == "missing-server-time": changed["response"].pop("ResponseMetadata")
            else: changed["acknowledged"] = False
            with self.subTest(invalid=invalid), self.assertRaises(ValueError): validate_protection(changed, spec)
        validate_protection({"acknowledged": True, "response": {"LegalHold": {"Status": "ON"}}}, {"ObjectLockLegalHoldStatus": "ON"})
        with self.assertRaises(ValueError):
            validate_protection({"acknowledged": True, "response": {"LegalHold": {"Status": "OFF"}}}, {"ObjectLockLegalHoldStatus": "ON"})

    def test_protected_delete_cannot_pass_as_a_success_or_generic_transport_failure(self):
        good = {"acknowledged": False, "resultUnknown": False,
                "response": {"ResponseMetadata": {"HTTPStatusCode": 403}, "Error": {"Code": "AccessDenied"}}}
        validate_protected_delete(good)
        for changed in ({**good, "acknowledged": True}, {**good, "payload": {}}, {**good, "resultUnknown": True},
                        {**good, "response": {"ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": "NoSuchVersion"}}}):
            with self.assertRaises(ValueError): validate_protected_delete(changed)

    def cleanup(self, *, permanent_denial=False, transport_unknown=False, iam_failure=False):
        clock, calls, evidence, removed = [0], [], {}, [False]
        fleet = types.SimpleNamespace(reserved=["node1"], faults=[], stop=lambda: None, start=lambda: None,
            remote=lambda *args, **kwargs: {}, snapshot=lambda *args, **kwargs: {}, cleanup=unittest.mock.Mock(return_value=True))
        def record(label, value): evidence.setdefault(label, []).append(value); return value
        fleet.record = record
        def invoke(operation, params):
            calls.append((operation, copy.deepcopy(params)))
            if operation == "list_object_versions":
                return {"acknowledged": True, "response": {} if removed[0] else {"Versions": [{"Key": LOCK_KEYS[1], "VersionId": "protected-version"}]}}
            if operation == "get_object_legal_hold": return {"acknowledged": True, "response": {"LegalHold": {"Status": "OFF"}}}
            if operation == "get_object_retention": return {"acknowledged": True, "response": {"Retention": {"Mode": "COMPLIANCE"}}}
            if operation == "delete_object":
                self.assertNotIn("BypassGovernanceRetention", params)
                if permanent_denial or transport_unknown or clock[0] < 4:
                    return {"acknowledged": False, "resultUnknown": transport_unknown,
                            "response": {"ResponseMetadata": {"HTTPStatusCode": 403}, "Error": {"Code": "AccessDenied"}}}
                removed[0] = True
                return {"acknowledged": True, "response": {"ResponseMetadata": {"HTTPStatusCode": 204}}}
            if operation == "delete_bucket": return {"acknowledged": True, "response": {}}
            if operation == "head_bucket": return {"acknowledged": False, "resultUnknown": False,
                                                    "response": {"ResponseMetadata": {"HTTPStatusCode": 404}}}
            self.fail("unexpected cleanup API: " + operation)
        def sleep(seconds): clock[0] += CLEANUP_SECONDS + 1 if permanent_denial else seconds
        context = {"bucketAttempted": True}
        if iam_failure: context.update(user="rvhealowned", policy="rvhealpolicy", policyCreated=True, attached=True)
        with patch("scripts.validation_heal.object_lock.time.monotonic", side_effect=lambda: clock[0]), \
             patch("scripts.validation_heal.object_lock.time.sleep", side_effect=sleep), \
             patch("scripts.validation_heal.object_lock.iam_exchange", side_effect=ValueError("IAM cleanup failure") if iam_failure else None):
            result = cleanup_locked_fleet(types.SimpleNamespace(s3=invoke), fleet, "rv-heal-0123456789abcdef", context)
        return result, fleet, calls, evidence, clock

    def test_physical_cleanup_waits_for_server_ack_after_real_compliance_expiry(self):
        result, fleet, calls, evidence, clock = self.cleanup()
        self.assertTrue(result)
        fleet.cleanup.assert_called_once()
        self.assertEqual(clock[0], 4)
        self.assertEqual(len(evidence["locked-final-delete"]), 3)
        self.assertTrue(evidence["locked-finalization"][0]["namespaceDeletionAllowed"])
        self.assertFalse(any(operation == "put_object_retention" for operation, params in calls))

    def test_retained_or_unknown_protection_forbids_namespace_deletion(self):
        for options in ({"permanent_denial": True}, {"transport_unknown": True}, {"iam_failure": True}):
            with self.subTest(options=options):
                result, fleet, calls, evidence, clock = self.cleanup(**options)
                self.assertFalse(result)
                fleet.cleanup.assert_not_called()
                self.assertEqual(evidence["locked-finalization"][0]["state"], "RETAINED")
                self.assertFalse(evidence["locked-finalization"][0]["namespaceDeletionAllowed"])


if __name__ == "__main__":
    unittest.main()
