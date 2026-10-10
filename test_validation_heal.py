"""Check preservation of Heal branches and prevent evidence from becoming PASS."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import copy
import gzip
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
from scripts.validation_heal.versions import multi_loss_instances, multi_loss_plan
from scripts.validation_heal.prepare_heal_meta_inspector import quorum_contract


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


if __name__ == "__main__":
    unittest.main()
