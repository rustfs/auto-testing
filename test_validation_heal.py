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
from unittest.mock import patch, Mock

from scripts.validation_heal.matrix import STAGES, TOPOLOGIES, audit_evidence, cases, compile_plan, select
from scripts.validation_heal.healthy import validate_healthy, validate_topology, validate_rejection, Fleet, BASE_OPTIONS
from scripts.validation_heal.fleet_node import paths, stopped, inspect_metadata
from scripts.validation_heal.inspector import verify as verify_inspector
from scripts.validation_heal.shards import BRANCHES, files, map_target, validate_fault, validate_repaired_physical, validate_repaired_status, peer_status_equal, validate_dry_run_status
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
from scripts.validation_heal.read_repair import source_tasks, validate_read, tracked_read_task, ADMISSION_MESSAGE, ADMISSION_TARGET
from scripts.validation_heal.drive_gate import apply_gate, activate_gate, release_gate, checked_gate, start_properties, validate_probe, validate_unwritable
from scripts.validation_heal.scanner_repair import (source_tasks as scanner_sources, cycle_pairs,
    observe_task as observe_scanner, JournalWindow)
from scripts.validation_heal.fleet_node import logs as node_logs
from scripts.validation_heal.markers import (INSTANCES as MARKER_INSTANCES, seed_markers, marker_plan,
    stale_expected_scene, add_marker, remove_marker, validate_marker_error)
from scripts.validation_heal.drive_gate import probe_gate
from scripts.validation_heal.status import (validate_background, validate_replacement, validate_cli,
    reject_unsafe_completion, watch_startup, stable_status, Cli)
from scripts.validation_heal.cursor import (Window, OBJECTS as CURSOR_OBJECTS, SIZE as CURSOR_SIZE,
    BRANCHES as CURSOR_BRANCHES, terminal_proof as cursor_terminal, validate_missing_batch, verify_all_parts)
from scripts.validation_heal.fleet_node import resource_limits, verify_parts as node_verify_parts
from scripts.validation_heal.stop import (partial_window, validate_retired_stop, validate_unknown_token, validate_cancelled,
    cancelled_physical, idle_fleet as stop_idle_fleet, BRANCHES as STOP_BRANCHES)
from scripts.validation_heal.cancel_restart import validate_journal, BRANCHES as CANCEL_RESTART_BRANCHES
from scripts.validation_heal.fleet_node import task_records as node_task_records
from scripts.validation_heal import overlap
from scripts.validation_heal import root_restart, fleet_node
from scripts.validation_heal import quorum_boundary
from scripts.validation_heal import stale_rejoin
from scripts.validation_heal import disk_loss
from scripts.validation_heal import replace_drive, replacement
import errno
import stat


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
            for action in ("preflight", "verify-part", "verify-inline", "apply-fault", "recover-fault", "start", "cleanup", "drive-gate", "drive-gate-probe"):
                self.assertTrue(fleet.remote("node1", action, disk=1, relativePath="literal$()'part.1")["checked"])
        self.assertEqual(len(observed), 9)

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
        task = ("/rustfs/admin/v3/heal/fixture/object", "original-token")
        self.assertTrue(peer_status_equal(coordinator, copy.deepcopy(coordinator), task=task, peer_task=task))
        rerendered = {**copy.deepcopy(coordinator), "startTime": "fresh response timestamp"}
        self.assertTrue(peer_status_equal(coordinator, rerendered, task=task, peer_task=task))
        for invalid in ("task-start", "summary", "coverage", "outcome", "truncated"):
            peer = copy.deepcopy(coordinator)
            peer_task = task
            if invalid == "task-start": peer_task = (task[0], "different-task")
            elif invalid == "summary": peer["summary"] = "unknown"
            elif invalid == "coverage": peer["outcome"]["coverage"] = "partial"
            elif invalid == "outcome": peer["outcome"]["counters"]["unknown"] = 1
            else: peer["truncated"] = True
            with self.subTest(invalid=invalid): self.assertFalse(peer_status_equal(coordinator, peer, task=task, peer_task=peer_task))

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


class HealDriveGateTest(unittest.TestCase):
    def test_live_permission_activation_follows_actual_start_and_initial_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            fleet = Fleet(["node1"], Path(directory), "gh-12345-1-h1")
            fleet.drive_gates.add("node1")
            calls = []
            def remote(node, action, **kwargs):
                calls.append(action)
                return {"mode": "permission-denied", "phase": "armed"} if action == "gate-state" else {}
            with patch.object(fleet, "remote", side_effect=remote), \
                 patch.object(fleet, "wait_ready", side_effect=lambda: calls.append("ready")):
                fleet.start()
            self.assertEqual(calls, ["start", "ready", "gate-state", "drive-gate-activate"])
            calls.clear()
            with patch.object(fleet, "remote", side_effect=remote), \
                 patch.object(fleet, "wait_ready", side_effect=ValueError("startup failed")), self.assertRaises(ValueError):
                fleet.start()
            self.assertNotIn("drive-gate-activate", calls)

    def test_partial_live_activation_restores_from_original_armed_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            state, root = Path(directory) / "state", Path(directory) / "root"
            state.mkdir()
            (root / "data").mkdir(parents=True, mode=0o700)
            original = Path.lstat
            def root_owned(path):
                info = original(path)
                return types.SimpleNamespace(st_mode=info.st_mode, st_uid=0, st_nlink=info.st_nlink,
                    st_size=info.st_size, st_dev=info.st_dev, st_ino=info.st_ino)
            with patch.object(Path, "lstat", root_owned):
                apply_gate(state, (root,), 0, "permission-denied")
                with patch("scripts.validation_heal.drive_gate.os.replace", side_effect=OSError("interrupted")), self.assertRaises(OSError):
                    activate_gate(state, (root,))
                self.assertEqual(checked_gate(state, (root,))["phase"], "armed")
                self.assertEqual(stat.S_IMODE((root / "data").stat().st_mode), 0)
                self.assertTrue(release_gate(state, (root,))["originalPermissionsRestored"])
                self.assertEqual(stat.S_IMODE((root / "data").stat().st_mode), 0o700)

    def test_gate_only_changes_owned_permissions_and_restores_partial_intent(self):
        for mode in ("read-only", "permission-denied"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                state = Path(directory) / "state"
                root = Path(directory) / "root"
                state.mkdir()
                (root / "data").mkdir(parents=True, mode=0o700)
                other = root / "quarantine"
                other.mkdir(mode=0o700)
                (root / "data" / "original").write_bytes(b"owned-data")
                original_lstat = Path.lstat
                def root_owned(path):
                    observed = original_lstat(path)
                    return types.SimpleNamespace(st_mode=observed.st_mode, st_uid=0, st_nlink=observed.st_nlink,
                        st_size=observed.st_size, st_dev=observed.st_dev, st_ino=observed.st_ino)
                with patch.object(Path, "lstat", root_owned):
                    gate = apply_gate(state, (root,), 0, mode)
                    self.assertEqual(checked_gate(state, (root,))["mode"], mode)
                    self.assertFalse(gate["hostMountRemounted"])
                    self.assertEqual(stat.S_IMODE((root / "data").stat().st_mode), 0o700)
                    if mode == "permission-denied":
                        self.assertEqual(gate["phase"], "armed")
                        activated = activate_gate(state, (root,))
                        self.assertTrue(activated["injectedAfterActualServiceStart"])
                        self.assertEqual(stat.S_IMODE((root / "data").stat().st_mode), 0)
                        self.assertEqual(checked_gate(state, (root,))["phase"], "active")
                        with self.assertRaises(ValueError): activate_gate(state, (root,))
                    self.assertEqual(stat.S_IMODE(other.stat().st_mode), 0o700)
                    with self.assertRaisesRegex(ValueError, "already active"):
                        apply_gate(state, (root,), 0, mode)
                    props = start_properties(gate)
                    self.assertIn("--property=PrivateMounts=true", props)
                    self.assertIn("--property=ReadOnlyPaths=" + str(root / "data") if mode == "read-only" else
                                  "--property=CapabilityBoundingSet=~CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH", props)
                    self.assertTrue(release_gate(state, (root,))["originalPermissionsRestored"])
                    self.assertEqual(stat.S_IMODE((root / "data").stat().st_mode), 0o700)
                    self.assertEqual((root / "data" / "original").read_bytes(), b"owned-data")
                    self.assertFalse((state / "drive-gate.json").exists())
                    self.assertTrue(release_gate(state, (root,))["gateWasAbsent"])

    def test_gate_refusal_proof_requires_same_uid_gid_errno_and_no_root_bypass(self):
        identity = {"uid": 0, "gid": 0, "capEff": 0}
        for mode, error in (("read-only", errno.EROFS), ("permission-denied", errno.EACCES)):
            good = {"uid": 0, "gid": 0, "errno": error}
            self.assertTrue(validate_probe(good, mode, identity)["sameServiceIdentity"])
            for changed in ({**good, "errno": 0}, {**good, "errno": errno.ENOSPC}, {**good, "uid": 1000}, {**good, "gid": 1000}):
                with self.assertRaises(ValueError): validate_probe(changed, mode, identity)
        with self.assertRaisesRegex(ValueError, "bypasses"):
            validate_probe({"uid": 0, "gid": 0, "errno": errno.EACCES}, "permission-denied", {**identity, "capEff": 1 << 1})

    def test_unwritable_receipt_never_accepts_repaired_or_hidden_failures(self):
        status = HealHealthyOracleTest().receipt("failed")
        bucket, entry = "rv-heal-drive-unit", {"key": "target.bin", "versionId": "null"}
        receipt = status["outcome"]["objects"][0]
        receipt["identity"].update(bucket=bucket, object=entry["key"], versionId=None)
        receipt["disposition"]["details"] = "permanent"
        status["outcome"]["execution"]["state"] = "completed_with_errors"
        status["outcome"]["counters"].update(healed=0, unchanged=0, failed=1, attemptFailures=1)
        self.assertFalse(validate_unwritable(status, bucket, entry)["repairClaimedWhileUnwritable"])
        stopped = {**copy.deepcopy(status), "summary": "stopped"}
        validate_unwritable(stopped, bucket, entry)
        stopped["outcome"]["execution"]["state"] = "aborted"
        with self.assertRaises(ValueError): validate_unwritable(stopped, bucket, entry)
        for invalid in ("repaired", "metadata_healthy", "unknown", "wrong-version", "healed-count", "hidden-failure", "incarnation"):
            changed = copy.deepcopy(status)
            if invalid in {"repaired", "metadata_healthy", "unknown"}: changed["outcome"]["objects"][0]["disposition"]["state"] = invalid
            elif invalid == "wrong-version": changed["outcome"]["objects"][0]["identity"]["versionId"] = "foreign"
            elif invalid == "healed-count": changed["outcome"]["counters"]["healed"] = 1
            elif invalid == "hidden-failure": changed["outcome"]["execution"]["state"] = "completed"
            else: changed["outcome"]["objects"][0]["identity"].pop("bucketIncarnationId")
            with self.subTest(invalid=invalid), self.assertRaises(ValueError): validate_unwritable(changed, bucket, entry)
        deferred = copy.deepcopy(status)
        deferred["outcome"]["execution"]["state"] = "completed"
        deferred["outcome"]["counters"].update(failed=0, skipped=1)
        deferred["outcome"]["objects"][0]["disposition"] = {"state": "deferred", "details": {"reason": "deadline"}}
        validate_unwritable(deferred, bucket, entry)
        deferred["outcome"]["objects"][0]["disposition"]["details"]["reason"] = "dangling_delete_grace"
        with self.assertRaises(ValueError): validate_unwritable(deferred, bucket, entry)

    def test_gate_cleanup_precedes_fault_rescue_and_preserves_namespace_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            fleet = Fleet(["node1"], Path(directory), "gh-12345-1-h1")
            fleet.reserved.append("node1")
            fleet.drive_gates.add("node1")
            fleet.faults.append(("node1", {"disk": 1, "faultId": "B02-test"}))
            calls = []
            def remote(node, action, **params):
                calls.append(action)
                if action == "drive-gate": raise ValueError("gate root drifted")
                return {}
            with patch.object(fleet, "remote", side_effect=remote): self.assertFalse(fleet.cleanup())
            self.assertNotIn("recover-fault", calls)
            self.assertNotIn("cleanup", calls)
            self.assertTrue(fleet.drive_gates)


class HealReadRepairTest(unittest.TestCase):
    def event(self, **changes):
        return {"level": "DEBUG", "target": ADMISSION_TARGET, "message": ADMISSION_MESSAGE,
                "bucket": "rv-heal-read-unit", "object": "target.bin", "part_number": 1,
                "pool_index": 0, "set_index": 0, "request_id": "automatic-task", "admission": "accepted", **changes}

    def test_source_requires_structured_exact_scope_and_new_task_identity(self):
        event = self.event()
        self.assertEqual(source_tasks({"node1": {"journal": "journal-prefix " + json.dumps(event)}}, event["bucket"], event["object"]),
                         {("node1", "automatic-task")})
        for changed in ({**event, "target": "unrelated"}, {**event, "bucket": "another-bucket"},
                        {**event, "object": "other-version-key"}, {**event, "message": "Read-repair heal request not admitted"}):
            self.assertFalse(source_tasks({"node1": {"journal": json.dumps(changed)}}, event["bucket"], event["object"]))
        self.assertFalse(source_tasks({"node1": {"journal": str(event)}}, event["bucket"], event["object"]))
        for name, value in (("admission", "merged"), ("pool_index", 1), ("set_index", True), ("part_number", 2), ("request_id", "")):
            with self.subTest(field=name), self.assertRaises(ValueError):
                source_tasks({"node1": {"journal": json.dumps({**event, name: value})}}, event["bucket"], event["object"])

    def test_read_bytes_version_and_server_request_id_are_required(self):
        entry = {"versionId": "historical", "payload": {"size": 3, "sha256": hashlib.sha256(b"abc").hexdigest()}}
        good = {"acknowledged": True, "payload": entry["payload"], "response": {"VersionId": "historical",
                "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "server-read"}}}
        self.assertEqual(validate_read(good, entry), "server-read")
        for invalid in ("version", "bytes", "request-id", "transport", "status"):
            changed = copy.deepcopy(good)
            if invalid == "version": changed["response"]["VersionId"] = "latest"
            elif invalid == "bytes": changed["payload"]["sha256"] = "not-acknowledged"
            elif invalid == "request-id": changed["response"]["ResponseMetadata"].pop("RequestId")
            elif invalid == "transport": changed["resultUnknown"] = True
            else: changed["response"]["ResponseMetadata"]["HTTPStatusCode"] = 206
            with self.subTest(invalid=invalid), self.assertRaises(ValueError): validate_read(changed, entry)
        ranged = {**good, "payload": {"size": 2, "sha256": hashlib.sha256(b"bc").hexdigest()},
                  "response": {**good["response"], "ContentRange": "bytes 1-2/3",
                               "ResponseMetadata": {"HTTPStatusCode": 206, "RequestId": "server-range"}}}
        validate_read(ranged, entry, expected_range=(1, 2), body=b"abc")
        with self.assertRaises(ValueError): validate_read(ranged, entry, expected_range=(0, 1), body=b"abc")

    def execute(self, *, stale_only=False):
        bucket, key, calls = "rv-heal-read-unit", "target.bin", []
        entry = {"key": key, "versionId": "historical", "payload": {"size": 3, "sha256": hashlib.sha256(b"abc").hexdigest()}}
        old = self.event(request_id="old-task")
        fresh = self.event()
        logs = [json.dumps(old), json.dumps(old) + ("" if stale_only else "\n" + json.dumps(fresh))]
        fleet = types.SimpleNamespace(runtime_profile="read-repair", nodes=["host-one"],
            record=lambda label, value: value, remote=lambda *args: {"journal": logs.pop(0) if logs else json.dumps(old)})
        def read(operation, params):
            calls.append((operation, params))
            return {"acknowledged": True, "payload": entry["payload"], "response": {"VersionId": "historical",
                    "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "server-read"}}}
        def status(path, query, body):
            calls.append((path, query, body))
            self.assertEqual(query, [("clientToken", "automatic-task")])
            self.assertEqual(body, b"")
            return {"httpStatus": 200, "response": {"summary": "finished", "source-receipt": "validated-by-physical-driver"}}
        client = types.SimpleNamespace(binding={}, s3=read)
        observer = types.SimpleNamespace(admin=status)
        ticks = [0, 1, 181] if stale_only else [0, 1]
        with patch("scripts.validation_heal.read_repair.Probe", return_value=observer) as factory, \
             patch("scripts.validation_heal.read_repair.time.monotonic", side_effect=ticks), \
             patch("scripts.validation_heal.read_repair.time.sleep"):
            if stale_only:
                with self.assertRaisesRegex(ValueError, "ATTRIBUTION_NOT_OBSERVED"):
                    tracked_read_task(client, fleet, bucket, "historical-version-read", entry)
                factory.assert_not_called()
            else:
                self.assertEqual(tracked_read_task(client, fleet, bucket, "historical-version-read", entry)["summary"], "finished")
                factory.assert_called_once_with({}, "node1")
        self.assertEqual(sum(call[0] == "get_object" for call in calls), 1)
        self.assertEqual(calls[0][1], {"Bucket": bucket, "Key": key, "VersionId": "historical"})
        self.assertTrue(all(len(call) == 2 or call[1] == [("clientToken", "automatic-task")] for call in calls))

    def test_only_one_target_read_and_observation_of_the_fresh_automatic_task(self):
        self.execute()
        self.execute(stale_only=True)

    def test_read_profile_keeps_other_repair_sources_disabled(self):
        profile = runtime_config("read-repair")
        self.assertEqual(profile.pop("RUSTFS_OBS_LOGGER_LEVEL"), "debug")
        self.assertEqual(profile, {key: value for key, value in BACKGROUND_CONFIG.items() if key != "RUSTFS_OBS_LOGGER_LEVEL"})
        snapshot, bucket, expected = versioned_scene(["objects/0001.bin"])
        plan = branch_plan("heal.read-repair-attribution", "historical-version-read", snapshot, bucket, expected, "DV")
        self.assertEqual(plan["expected"][0]["versionId"], "v0")
        self.assertFalse(plan["expected"][0]["isLatest"])
        self.assertEqual(len(plan["expected"]), 1)


class HealScannerSourceTest(unittest.TestCase):
    bucket, key = "rv-heal-scanner-unit", "objects/0008.bin"

    def events(self):
        def row(second, **values):
            return {"timestamp": f"2026-10-10T12:00:{second:02d}Z", **values}
        return [
            row(1, target="rustfs::scanner", event="scanner_cycle_state", state="started", cycle=7, scan_mode="Deep"),
            row(2, target="rustfs::scanner::folder", event="scanner_heal_admission", state="request_started",
                bucket=self.bucket, object=self.key, version_id="00000000-0000-0000-0000-000000000000"),
            row(3, target="rustfs::heal::channel", state="start_received", request_id="task-one",
                bucket=self.bucket, object_prefix=self.key),
            row(4, target="rustfs::heal::manager", message="Heal queue request accepted", request_id="task-one",
                source="scanner", result="accepted"),
            row(5, target="rustfs::heal::task", message="Heal object started", stage="start", task_id="task-one",
                bucket=self.bucket, object=self.key, version_id="None"),
            row(6, target="rustfs::heal::manager", state="task_completed", task_id="task-one"),
            row(7, target="rustfs::scanner", event="scanner_cycle_state", state="completed", cycle=0, scan_mode="Deep"),
        ]

    def entry(self):
        return {"key": self.key, "versionId": "null", "deleteMarker": False}

    def test_source_chain_requires_exact_scanner_admission_version_cycle_and_task(self):
        rows = self.events()
        self.assertEqual(scanner_sources({"host": rows}, self.bucket, self.entry())[0]["taskId"], "task-one")
        mutations = ((1, "bucket", "other"), (1, "object", "other"), (1, "version_id", "historical"),
                     (2, "object_prefix", "other"), (3, "source", "read-repair"), (3, "result", "merged_duplicate"),
                     (4, "task_id", "another"), (4, "version_id", "historical"), (5, "task_id", "another"),
                     (6, "state", "failed"), (0, "scan_mode", "Normal"), (1, "timestamp", "2026-10-10T12:00:08Z"))
        for index, field, value in mutations:
            changed = copy.deepcopy(rows)
            changed[index][field] = value
            with self.subTest(index=index, field=field):
                self.assertFalse(scanner_sources({"host": changed}, self.bucket, self.entry()))
        for index in (1, 4):
            changed = copy.deepcopy(rows)
            changed[index].pop("version_id")
            self.assertFalse(scanner_sources({"host": changed}, self.bucket, self.entry()))
        self.assertFalse(scanner_sources({"host": rows[:1] + rows[-1:]}, self.bucket, self.entry()))
        self.assertFalse(scanner_sources({"host": rows[:4], "peer": rows[4:]}, self.bucket, self.entry()))

    def test_task_may_start_after_discovery_cycle_but_cannot_precede_admission(self):
        rows = self.events()
        rows[4]["timestamp"] = "2026-10-10T12:00:08Z"
        rows[5]["timestamp"] = "2026-10-10T12:00:09Z"
        self.assertTrue(scanner_sources({"host": rows}, self.bucket, self.entry()))
        rows[4]["timestamp"] = "2026-10-10T12:00:03Z"
        self.assertFalse(scanner_sources({"host": rows}, self.bucket, self.entry()))

    def test_cycle_pair_requires_ordered_positive_start_and_complete_boundary(self):
        rows = self.events()
        self.assertEqual(cycle_pairs(rows)[0]["cycle"], 7)
        for change in ({"cycle": 9}, {"timestamp": "2026-10-10T11:00:00Z"}, {"timestamp": "invalid"}):
            changed = copy.deepcopy(rows)
            changed[-1].update(change)
            self.assertFalse(cycle_pairs(changed))
        failed = {**rows[-1], "state": "failed", "timestamp": "2026-10-10T12:00:06Z"}
        self.assertFalse(cycle_pairs(rows[:-1] + [failed, rows[-1]]))

    def test_observer_uses_status_only_and_rejects_healthy_substitution_for_repair(self):
        fleet = types.SimpleNamespace(runtime_profile="scanner", nodes=["host"], record=lambda label, value: value)
        window = types.SimpleNamespace(capture=lambda: {"host": self.events()})
        def status(state):
            return {"summary": "finished", "outcome": {"objects": [{"disposition": {"state": state}}]}}
        client = types.SimpleNamespace(binding={})
        calls = []
        def query(path, args, body):
            calls.append((path, args, body))
            return {"httpStatus": 200, "response": status("repaired")}
        with patch("scripts.validation_heal.scanner_repair.Probe", return_value=types.SimpleNamespace(admin=query)):
            self.assertEqual(observe_scanner(client, fleet, window, self.bucket, self.entry(), "cold-missing"), status("repaired"))
        self.assertEqual(calls, [("/rustfs/admin/v3/heal/" + self.bucket + "/" + self.key,
                                [("clientToken", "task-one")], b"")])
        observer = types.SimpleNamespace(admin=lambda *args: {"httpStatus": 200, "response": status("verified_healthy")})
        with patch("scripts.validation_heal.scanner_repair.Probe", return_value=observer):
            with self.assertRaisesRegex(ValueError, "required exact"):
                observe_scanner(client, fleet, window, self.bucket, self.entry(), "cold-missing")
            observe_scanner(client, fleet, window, self.bucket, self.entry(), "cold-missing", healthy=True)

    def test_boundary_branch_requires_two_complete_post_fault_cycles(self):
        fleet = types.SimpleNamespace(runtime_profile="scanner", nodes=["host"], record=lambda label, value: value)
        window = types.SimpleNamespace(capture=lambda: {"host": self.events()})
        with patch("scripts.validation_heal.scanner_repair.time.monotonic", side_effect=[0, 241]), \
             patch("scripts.validation_heal.scanner_repair.Probe") as observer:
            with self.assertRaisesRegex(ValueError, "ATTRIBUTION_NOT_OBSERVED"):
                observe_scanner(types.SimpleNamespace(binding={}), fleet, window, self.bucket, self.entry(), "scanner-cycle-boundary")
            observer.assert_not_called()

    def test_cursor_windows_are_complete_and_old_events_are_excluded(self):
        calls = []
        observations = [{"complete": True, "afterCursor": "cursor-1", "journal": json.dumps(self.events()[0])},
                        {"complete": True, "beforeCursor": "cursor-1", "afterCursor": "cursor-2", "journal": json.dumps(self.events()[1])}]
        def remote(node, action, **params):
            calls.append(params)
            return observations.pop(0)
        fleet = types.SimpleNamespace(nodes=["host"], record=lambda label, value: value, remote=remote)
        window = JournalWindow(fleet, "unit")
        self.assertEqual(window.capture(), {"host": [self.events()[1]]})
        self.assertEqual(calls, [{"checkpoint": True}, {"afterCursor": "cursor-1"}])
        window.fleet.remote = lambda *args, **kwargs: {"complete": False, "beforeCursor": "cursor-2", "afterCursor": "cursor-3"}
        with self.assertRaisesRegex(ValueError, "completeness"): window.capture()

    def test_remote_log_window_rejects_truncation_and_invalid_cursor(self):
        owner = {"unit": "private.service"}
        with patch("scripts.validation_heal.fleet_node.owned", return_value=(None, None, owner)), \
             patch("scripts.validation_heal.fleet_node.command") as command:
            command.return_value = types.SimpleNamespace(stdout=json.dumps({"MESSAGE": '{"message":"fresh"}',
                "__CURSOR": "s=abc;i=def"}) + "\n")
            value = node_logs({"runId": "unit", "afterCursor": "s=previous;i=1"})
            self.assertTrue(value["complete"])
            self.assertEqual(value["afterCursor"], "s=abc;i=def")
            self.assertIn("--after-cursor", command.call_args.args)
            self.assertIn("+10000", command.call_args.args)
            self.assertIn("--all", command.call_args.args)
            self.assertFalse(value["hasMore"])
            with self.assertRaisesRegex(ValueError, "invalid journal cursor"):
                node_logs({"runId": "unit", "afterCursor": "invalid\n--help"})
            command.return_value = types.SimpleNamespace(stdout=json.dumps({"MESSAGE": list('完整消息'.encode()),
                "__CURSOR": "s=abc;i=bytes"}) + "\n")
            self.assertEqual(node_logs({"runId": "unit", "afterCursor": "s=previous;i=1"})["journal"], '完整消息')
            command.return_value = types.SimpleNamespace(stdout=json.dumps({"MESSAGE": None, "__CURSOR": "s=abc;i=omitted"}) + "\n")
            with self.assertRaisesRegex(ValueError, "complete text"): node_logs({"runId": "unit", "afterCursor": "s=previous;i=1"})
            rows = [json.dumps({"MESSAGE": "row", "__CURSOR": f"s=abc;i={index}"}) for index in range(10000)]
            command.return_value = types.SimpleNamespace(stdout="\n".join(rows))
            page = node_logs({"runId": "unit", "afterCursor": "s=previous;i=1"})
            self.assertTrue(page["hasMore"])
            self.assertEqual(page["afterCursor"], "s=abc;i=9999")
            command.return_value = types.SimpleNamespace(stdout="\n".join(rows + [json.dumps({"MESSAGE": "row", "__CURSOR": "s=abc;i=10000"})]))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                node_logs({"runId": "unit", "afterCursor": "s=previous;i=1"})

    def test_cursor_pagination_keeps_early_attribution_and_does_not_skip_pages(self):
        events = self.events()
        observations = [{"complete": True, "afterCursor": "checkpoint"},
            {"complete": True, "beforeCursor": "checkpoint", "afterCursor": "page-one", "hasMore": True,
             "journal": json.dumps(events[0])},
            {"complete": True, "beforeCursor": "page-one", "afterCursor": "page-two", "hasMore": False,
             "journal": json.dumps(events[1])}]
        calls = []
        def remote(node, action, **params):
            calls.append(params)
            return observations.pop(0)
        fleet = types.SimpleNamespace(nodes=["host"], record=lambda label, value: value, remote=remote)
        window = JournalWindow(fleet, "unit")
        self.assertEqual(window.capture(), {"host": events[:2]})
        self.assertEqual(calls[1:], [{"afterCursor": "checkpoint"}, {"afterCursor": "page-one"}])
        window.fleet.remote = lambda *args, **kwargs: {"complete": True, "beforeCursor": "page-two",
            "afterCursor": "page-two", "hasMore": True}
        with self.assertRaisesRegex(ValueError, "did not advance"): window.capture()

    def test_scanner_profile_enables_only_scanner_source_and_fixed_deep_timing(self):
        config = runtime_config("scanner")
        self.assertEqual(config["RUSTFS_SCANNER_ENABLED"], "true")
        for name in ("RUSTFS_HEAL_AUTO_HEAL_ENABLE", "RUSTFS_HEAL_MRF_ENABLE"):
            self.assertEqual(config[name], "false")
        self.assertEqual(config["RUSTFS_SCANNER_BITROT_CYCLE_SECS"], "0")
        self.assertEqual(config["RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS"], "0")
        self.assertEqual(config["RUSTFS_HEAL_OBJECT_SELECT_PROB"], "1")


class HealMarkerSemanticsTest(unittest.TestCase):
    def response(self, version, **changes):
        return {"VersionId": version, "DeleteMarker": True, "ResponseMetadata": {"HTTPStatusCode": 204}, **changes}

    def test_ack_manifest_retains_three_data_versions_and_exact_marker_order(self):
        expected = [{"key": "key", "versionId": f"v{nonce}", "deleteMarker": False, "isLatest": nonce == 2}
                    for nonce in range(3)]
        current = add_marker(expected, "key", self.response("marker"))
        self.assertEqual(current[-1], {"key": "key", "versionId": "marker", "deleteMarker": True, "isLatest": True})
        self.assertTrue(all(not entry["isLatest"] for entry in current[:-1]))
        self.assertEqual(remove_marker(current, "key", self.response("marker")), expected)
        for response in (self.response("marker", DeleteMarker=False), self.response("v1"), self.response("null"),
                         self.response("marker", ResponseMetadata={"HTTPStatusCode": 200})):
            with self.subTest(response=response), self.assertRaises(ValueError): add_marker(expected, "key", response)
        with self.assertRaises(ValueError): remove_marker(current, "key", self.response("another"))

    def test_seed_has_real_historical_and_current_marker_ack_positions(self):
        sequence = 0
        calls = []
        def call(operation, params, body=None):
            nonlocal sequence
            sequence += 1
            calls.append((operation, params))
            if operation == "put_object": return {"VersionId": f"data-{sequence}"}
            if operation == "delete_object": return self.response(f"marker-{sequence}")
            return {}
        recorder = types.SimpleNamespace(call=call)
        expected = seed_markers(recorder, "rv-heal-markers", 100)
        self.assertEqual(len([entry for entry in expected if not entry["deleteMarker"]]), 18)
        for instance in MARKER_INSTANCES:
            key = "markers/" + instance["name"] + ".bin"
            entries = [entry for entry in expected if entry["key"] == key]
            markers = [entry for entry in entries if entry["deleteMarker"]]
            self.assertEqual(len(markers), int(instance["mode"] != "create"))
            if markers:
                self.assertEqual(markers[0]["isLatest"], instance["mode"] != "historical")
                selected = [operation for operation, params in calls if params.get("Key") == key]
                self.assertEqual(selected, ["put_object", "put_object", "delete_object", "put_object"]
                                 if instance["mode"] == "historical" else ["put_object"] * 3 + ["delete_object"])

    def scene(self):
        instance = next(item for item in MARKER_INSTANCES if item["mode"] == "create")
        key = "markers/" + instance["name"] + ".bin"
        before, bucket, expected = versioned_scene([key])
        plan = marker_plan(instance, before, bucket, expected)
        target = plan["proofs"][0]
        marker = {"versionId": "new-marker", "deleted": True, "isLatest": True,
                  "dataDir": None, "inline": None, "parts": [], "erasure": {}, "size": 0}
        authorities = copy.deepcopy(before)
        for entry in authorities[target["node"]]["files"]:
            if "metadata" not in entry or entry["disk"] == target["disk"]: continue
            for version in entry["metadata"]["versions"]: version["isLatest"] = False
            entry["metadata"]["versions"].insert(0, copy.deepcopy(marker))
            entry["sha256"] += "marker-ack"
        manifest = add_marker(expected, key, self.response("new-marker"))
        return before, authorities, bucket, key, manifest, target

    def test_stale_oracle_uses_online_ack_authority_and_original_target_data_roles(self):
        before, authorities, bucket, key, manifest, target = self.scene()
        oracle = stale_expected_scene(before, authorities, bucket, key, manifest, target)
        identity = (target["node"], target["disk"], f"{bucket}/{key}/xl.meta")
        original = files(before)[identity]["metadata"]["versions"]
        wanted = files(oracle)[identity]["metadata"]["versions"]
        self.assertEqual(wanted[0]["versionId"], "new-marker")
        for old, new in zip(original, wanted[1:], strict=True):
            self.assertEqual({**old, "isLatest": False}, new)
        self.assertEqual(files(before)[identity]["sha256"], files(oracle)[identity]["sha256"])
        self.assertEqual(files(authorities)[identity]["metadata"]["versions"], original)

    def test_stale_oracle_rejects_target_writes_wrong_quorum_and_control_changes(self):
        before, authorities, bucket, key, manifest, target = self.scene()
        for mutation in ("target", "authority", "data", "format"):
            changed = copy.deepcopy(authorities)
            entries = changed[target["node"]]["files"]
            if mutation == "target":
                next(entry for entry in entries if entry["disk"] == target["disk"])["sha256"] += "unexpected"
            elif mutation == "authority":
                next(entry for entry in entries if entry.get("metadata") and entry["disk"] != target["disk"])["metadata"]["versions"][0]["versionId"] = "foreign"
            elif mutation == "data": next(entry for entry in entries if not entry.get("metadata"))["sha256"] += "changed"
            else: changed[target["node"]]["formats"][0]["value"]["id"] += "changed"
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                stale_expected_scene(before, changed, bucket, key, manifest, target)

    def test_marker_negative_requires_http_headers_and_no_payload_or_unknown(self):
        good = {"acknowledged": False, "resultUnknown": False, "response": {"ResponseMetadata": {
            "HTTPStatusCode": 405, "HTTPHeaders": {"x-amz-version-id": "marker", "x-amz-delete-marker": "true"}}}}
        validate_marker_error(good, 405, "marker")
        for kind in ("status", "version", "marker", "payload", "transport", "ack"):
            changed = copy.deepcopy(good)
            if kind == "status": changed["response"]["ResponseMetadata"]["HTTPStatusCode"] = 404
            elif kind in {"version", "marker"}: changed["response"]["ResponseMetadata"]["HTTPHeaders"]["x-amz-" + ("version-id" if kind == "version" else "delete-marker")] = "wrong"
            elif kind == "payload": changed["payload"] = {"size": 0}
            elif kind == "transport": changed["resultUnknown"] = True
            else: changed["acknowledged"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError): validate_marker_error(changed, 405, "marker")

    def test_permission_probe_checks_read_and_write_with_same_namespace_and_dac_limit(self):
        value = {"mode": "permission-denied", "dataPath": "/private/data", "disk": 1}
        output = types.SimpleNamespace(returncode=0, stdout=json.dumps({"uid": 0, "gid": 0, "errno": errno.EACCES}))
        with patch("scripts.validation_heal.drive_gate.checked_gate", return_value=value), \
             patch("scripts.validation_heal.drive_gate.process_identity", return_value={"uid": 0, "gid": 0, "capEff": 0}), \
             patch("pathlib.Path.readlink", side_effect=[Path("mnt:[private]"), Path("mnt:[host]")]), \
             patch("scripts.validation_heal.drive_gate.subprocess.run", return_value=output) as invoke:
            result = probe_gate(None, None, 123)
        self.assertEqual(result["readProbe"]["errno"], errno.EACCES)
        self.assertEqual(invoke.call_count, 2)
        for args in invoke.call_args_list:
            command = args.args[0]
            self.assertEqual(command.count("python3"), 1)
            self.assertIn("--mount=/proc/123/ns/mnt", command)
            self.assertIn("--bounding-set=-dac_override,-dac_read_search", command)
        self.assertEqual(invoke.call_args.args[0][-1], "/private/data/.rustfs.sys/format.json")


class HealRuntimeStatusTest(unittest.TestCase):
    def background(self, *, state="idle", missing=False):
        return {"state": state, "clusterStatusComplete": not missing,
                "coverage": {"expected": 4, "responded": 3 if missing else 4, "unknown": int(missing),
                             "reasons": ["peer_status_unavailable"] if missing else []},
                "healQueueLength": 0, "healActiveTasks": 0,
                "healOperations": {"queueLength": 0, "activeTasks": 0, "retryingTasks": 0}}

    def observation(self, *, state="uninitialized", missing=False):
        background = self.background(state=state, missing=missing)
        uncertain = missing or state in {"disabled", "uninitialized"}
        return {"node1": {"background": {"httpStatus": 200, "response": background},
            "replacement": {"httpStatus": 200, "response": {"contractVersion": 2, "scope": "cluster_survivor_disks",
                "cluster": {"records": [], "definitive": not uncertain, "reason": "runtime unavailable" if uncertain else None}}},
            "rcJson": {"exitCode": 0, "json": {"state": state, "clusterStatusComplete": not missing, "coverage": background["coverage"]}},
            "rcText": {"exitCode": 0, "stdout": "Heal Status: " + state.capitalize()}}}

    def test_raw_coverage_cannot_hide_peer_loss_retrying_work_or_disabled_state(self):
        validate_background(self.background(), "enabled-idle", 4)
        validate_background(self.background(state="degraded", missing=True), "missing-peer", 4)
        for branch, state in (("disabled", "disabled"), ("uninitialized", "uninitialized")):
            validate_background(self.background(state=state), branch, 4)
        for mutation in ("bool", "lost-peer", "false-idle", "retry", "disabled-idle", "reason"):
            missing = mutation in {"false-idle", "reason"}
            value = self.background(state="degraded" if missing else "idle", missing=missing)
            branch = "missing-peer" if missing else "disabled" if mutation == "disabled-idle" else "enabled-idle"
            if mutation == "bool": value["coverage"]["responded"] = True
            elif mutation == "lost-peer": value["coverage"]["unknown"] = 1
            elif mutation == "false-idle": value["state"] = "idle"
            elif mutation == "retry": value["healOperations"]["retryingTasks"] = 1
            elif mutation == "reason": value["coverage"]["reasons"] = []
            with self.subTest(mutation=mutation), self.assertRaises(ValueError): validate_background(value, branch, 4)

    def test_replacement_does_not_convert_disabled_uninitialized_or_partial_to_definitive(self):
        for branch, state in (("disabled", "disabled"), ("uninitialized", "uninitialized"), ("missing-peer", "degraded")):
            value = self.observation(state=state, missing=branch == "missing-peer")["node1"]["replacement"]["response"]
            validate_replacement(value, branch)
            value["cluster"]["definitive"] = True
            with self.assertRaises(ValueError): validate_replacement(value, branch)

    def test_rc_fields_and_text_must_preserve_raw_runtime_and_peer_truth(self):
        for branch, state in (("disabled", "disabled"), ("uninitialized", "uninitialized"), ("missing-peer", "degraded")):
            value = self.observation(state=state, missing=branch == "missing-peer")["node1"]
            validate_cli(value["rcJson"], value["rcText"], value["background"]["response"], branch, 4)
            changed = copy.deepcopy(value)
            changed["rcJson"]["json"]["state"] = "idle"
            with self.assertRaises(ValueError): validate_cli(changed["rcJson"], changed["rcText"], changed["background"]["response"], branch, 4)
            changed = copy.deepcopy(value)
            changed["rcJson"]["json"]["coverage"] = copy.deepcopy(changed["rcJson"]["json"]["coverage"])
            changed["rcJson"]["json"]["coverage"]["unknown"] += 1
            with self.assertRaises(ValueError): validate_cli(changed["rcJson"], changed["rcText"], changed["background"]["response"], branch, 4)
            with self.assertRaises(ValueError): validate_cli(value["rcJson"], {"exitCode": 0, "stdout": "Heal Status: Idle"}, value["background"]["response"], branch, 4)

    def test_unsafe_convergence_sample_fails_immediately_and_is_not_retried_to_green(self):
        observed = self.observation(state="degraded", missing=True)
        observed["node1"]["rcText"]["stdout"] = "Heal Status: Idle"
        with self.assertRaises(ValueError): reject_unsafe_completion(observed)
        with patch("scripts.validation_heal.status.observations", return_value=observed) as reads, \
             patch("scripts.validation_heal.status.time.sleep") as sleep:
            with self.assertRaises(ValueError): stable_status(None, {"endpoints": {"node1": "url"}}, None, "missing-peer", ["node1"])
            reads.assert_called_once()
            sleep.assert_not_called()

    def test_startup_needs_raw_rc_window_bracket_transport_failure_is_not_observation(self):
        good = self.observation()
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        binding = {"endpoints": {"node1": "one", "node2": "two", "node3": "three", "node4": "four"}}
        with patch("scripts.validation_heal.status.observations", return_value=good), \
             patch("scripts.validation_heal.status.TimedProbe", return_value=types.SimpleNamespace(admin=lambda *args: good["node1"]["background"])):
            result = watch_startup(fleet, binding, None, types.SimpleNamespace(is_set=lambda: False))
            self.assertEqual(result["verdict"], "PASS")
            self.assertTrue(result["rawBracketedCliWindow"])
        bad = copy.deepcopy(good)
        bad["node1"]["background"] = {"httpStatus": None, "error": "connection refused"}
        with patch("scripts.validation_heal.status.observations", return_value=bad), \
             patch("scripts.validation_heal.status.time.monotonic", side_effect=[0, 1, 31]), \
             patch("scripts.validation_heal.status.time.sleep"):
            result = watch_startup(fleet, binding, None, types.SimpleNamespace(is_set=lambda: False))
            self.assertEqual(result["verdict"], "NOT_OBSERVED")
        with patch("scripts.validation_heal.status.observations", return_value=good), \
             patch("scripts.validation_heal.status.TimedProbe", return_value=types.SimpleNamespace(admin=lambda *args: {"httpStatus": 200, "response": self.background()})), \
             patch("scripts.validation_heal.status.time.monotonic", side_effect=[0, 1, 31]), \
             patch("scripts.validation_heal.status.time.sleep"):
            self.assertEqual(watch_startup(fleet, binding, None, types.SimpleNamespace(is_set=lambda: False))["verdict"], "NOT_OBSERVED")

    def test_cli_private_credentials_are_removed_and_never_enter_process_argv(self):
        cli = Cli(Path("/usr/bin/rc"), {"endpoints": {"node1": "http://private:9000"}}, "runtime-access", "runtime-secret")
        root = Path(cli.folder.name)
        self.assertEqual(stat.S_IMODE((root / "config.toml").stat().st_mode), 0o600)
        try:
            with patch("scripts.validation_heal.status.subprocess.run", return_value=types.SimpleNamespace(
                    returncode=0, stdout='{"state":"disabled"}', stderr="")) as invoked:
                result = cli.read("node1", as_json=True)
                self.assertEqual(result["json"]["state"], "disabled")
                self.assertNotIn("runtime-access", repr(invoked.call_args.args))
                self.assertNotIn("runtime-secret", repr(invoked.call_args.args))
                self.assertEqual(invoked.call_args.kwargs["env"]["RC_CONFIG_DIR"], str(root))
        finally: cli.close()
        self.assertFalse(root.exists())
        config = runtime_config("disabled")
        self.assertEqual(config["RUSTFS_HEAL_ENABLED"], "false")
        self.assertEqual({**config, "RUSTFS_HEAL_ENABLED": "true"}, BACKGROUND_CONFIG)


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


class HealCursorOracleTest(unittest.TestCase):
    bucket = "rv-heal-cursor-unit"

    def expected(self):
        return [{"key": f"objects/{index:04d}.bin", "versionId": "null", "deleteMarker": False,
                 "payload": {"size": CURSOR_SIZE}} for index in range(CURSOR_OBJECTS)]

    def window(self, endpoint="node1"):
        return Window(self.bucket, self.expected(), "task", endpoint, "deployment/task")

    def status(self, start=0, stop=3, *, minimum=1, truncated=False, finished=False):
        items = [{"bucket": self.bucket, "object": f"objects/{index:04d}.bin", "versionId": "",
                  "objectSize": CURSOR_SIZE} for index in range(start, stop)]
        return {"summary": "finished" if finished else "running", "nextSeq": stop + 1, "minSeq": minimum,
                "items": items, "truncated": truncated, "outcome": {"counters": {"processed": stop}}}

    def consume(self, window, status, cursor):
        return window.consume(status, cursor, token="task", endpoint=window.endpoint, epoch="deployment/task")

    def test_exclusive_next_sequence_preserves_the_first_item_of_each_poll(self):
        window = self.window()
        self.assertEqual(self.consume(window, self.status(), 0)["exclusiveNextCursor"], 3)
        self.assertEqual(self.consume(window, self.status(3, 6), 3)["exclusiveNextCursor"], 6)
        self.assertEqual(len(window.seen), 6)
        self.assertEqual(window.running_windows, 2)
        with self.assertRaisesRegex(ValueError, "jumped"):
            self.consume(window, self.status(6, 9), 7)

    def test_truncated_payload_advances_only_over_transmitted_items(self):
        window, status = self.window(), self.status(0, 8, truncated=True)
        status["items"] = status["items"][:4]
        self.assertEqual(self.consume(window, status, 0)["exclusiveNextCursor"], 4)
        self.assertEqual(self.consume(window, self.status(4, 8, truncated=True), 4)["exclusiveNextCursor"], 8)
        self.assertEqual(len(window.seen), 8)
        status["truncated"] = False
        with self.assertRaisesRegex(ValueError, "silently skip"):
            self.window().check(status, 0)

    def test_retention_gap_requires_a_real_moved_minimum_and_flag(self):
        status = self.status(1, CURSOR_OBJECTS, minimum=2, truncated=True, finished=True)
        proof = self.window().check(status, 0)
        self.assertTrue(proof["retentionGap"])
        self.assertEqual(proof["returnedItems"], 1024)
        changed = copy.deepcopy(status)
        changed["truncated"] = False
        with self.assertRaisesRegex(ValueError, "silently omitted"):
            self.window().check(changed, 0)
        caught_up = self.status(CURSOR_OBJECTS, CURSOR_OBJECTS, minimum=2, truncated=True, finished=True)
        self.assertEqual(self.window().check(caught_up, CURSOR_OBJECTS)["returnedItems"], 0)
        self.assertFalse(self.window().check(caught_up, CURSOR_OBJECTS)["retentionGap"])

    def test_duplicate_backward_wrong_scope_and_epoch_are_not_convergence(self):
        for kind in ("duplicate", "backward", "bucket", "version", "size", "bounds", "flag", "terminal"):
            with self.subTest(kind=kind):
                window = self.window()
                self.consume(window, self.status(), 0)
                status = self.status(3, 6)
                if kind == "duplicate": status["items"][0]["object"] = "objects/0000.bin"
                if kind == "backward": status.update(nextSeq=3, items=[])
                if kind == "bucket": status["items"][0]["bucket"] = "unowned"
                if kind == "version": status["items"][0]["versionId"] = "different-generation"
                if kind == "size": status["items"][0]["objectSize"] += 1
                if kind == "bounds": status["nextSeq"] = True
                if kind == "flag": status["truncated"] = "false"
                if kind == "terminal": status["summary"] = "stopped"
                with self.assertRaises(ValueError): self.consume(window, status, 3)
        for name, value in (("token", "other-task"), ("endpoint", "node2"), ("epoch", "restarted/task")):
            values = {"token": "task", "endpoint": "node1", "epoch": "deployment/task", name: value}
            with self.assertRaisesRegex(ValueError, "crossed"):
                self.window().consume(self.status(), 0, **values)

    def terminal(self):
        status = self.status(1, CURSOR_OBJECTS, minimum=2, truncated=True, finished=True)
        receipts = [{"identity": {"bucket": self.bucket, "object": f"objects/{index:04d}.bin", "versionId": None,
            "poolIndex": 0, "setIndex": 0, "bucketIncarnationId": "incarnation"},
            "disposition": {"state": "repaired" if index == 1024 else "verified_healthy"}} for index in range(897, 1025)]
        status["outcome"].update(execution={"state": "completed"}, coverage="complete", objectsTruncated=True,
            objects=receipts, counters={"processed": CURSOR_OBJECTS, "healed": 1, "unchanged": CURSOR_OBJECTS - 1,
                "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False})
        return status

    def test_terminal_truncation_cannot_hide_error_or_claim_complete_diagnostics(self):
        repaired = {("objects/1024.bin", "null")}
        proof = cursor_terminal(self.terminal(), self.bucket, self.expected(), repaired, self.window())
        self.assertFalse(proof["diagnosticObjectsAreCompleteManifest"])
        for kind in ("unknown", "failure", "attempt", "counter", "coverage", "truncation", "identity", "repair", "gap"):
            status = self.terminal()
            if kind == "unknown": status["outcome"]["counters"]["unknown"] = 1
            if kind == "failure": status["outcome"]["counters"]["failed"] = 1
            if kind == "attempt": status["outcome"]["counters"]["attemptFailures"] = 1
            if kind == "counter": status["outcome"]["counters"]["processed"] -= 1
            if kind == "coverage": status["outcome"]["coverage"] = "partial"
            if kind == "truncation": status["outcome"]["objectsTruncated"] = False
            if kind == "identity": status["outcome"]["objects"][0]["identity"]["bucketIncarnationId"] = "other"
            if kind == "repair": status["outcome"]["objects"][-1]["disposition"]["state"] = "verified_healthy"
            if kind == "gap": status["minSeq"] = 1
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                cursor_terminal(status, self.bucket, self.expected(), repaired, self.window())

    def test_fault_group_checks_all_controls_and_all_exact_missing_parts(self):
        before = HealShardOracleTest().snapshot()
        target = map_target(before, "rv-heal-0123456789abcdef", "objects/0008.bin", "data", 1)
        second = {**target, **next(shard for shard in target["cohort"] if shard["disk"] != target["disk"])}
        targets = [target, second]
        receipts = [{"relativePath": item["relativePath"], "fault": None,
            "before": {"size": item["size"], "sha256": item["sha256"]}} for item in targets]
        after = copy.deepcopy(before)
        removed = {(item["disk"], item["relativePath"]) for item in targets}
        after["node1"]["files"] = [entry for entry in after["node1"]["files"] if (entry["disk"], entry["path"]) not in removed]
        self.assertEqual(validate_missing_batch(before, after, targets, receipts)["exactMissingParts"], 2)
        after["node1"]["files"][0]["sha256"] = "changed-control"
        with self.assertRaisesRegex(ValueError, "changed metadata"):
            validate_missing_batch(before, after, targets, receipts)

    def test_batch_bitrot_verifier_requires_every_exact_part_and_no_duplicates(self):
        with patch("scripts.validation_heal.fleet_node.owned", return_value=(Path("/state"), [], {"inspectorSha256": "sha"})), \
             patch("scripts.validation_heal.fleet_node.stopped"), patch("scripts.validation_heal.fleet_node.checked", side_effect=lambda path, boundary: path), \
             patch("scripts.validation_heal.fleet_node.digest", return_value="sha"), \
             patch("scripts.validation_heal.fleet_node.verify_part_owned", side_effect=lambda entry, *args: entry) as reads:
            entry = {"disk": 0, "relativePath": "bucket/key/data/part.1", "versionId": "null", "partNumber": 1}
            self.assertEqual(node_verify_parts({"runId": "run", "parts": [entry]})["parts"], [entry])
            self.assertEqual(reads.call_count, 1)
            for batch in ([], [entry, entry], [entry] * 129, [{**entry, "extra": "unbound"}]):
                with self.assertRaises(ValueError): node_verify_parts({"runId": "run", "parts": batch})

    def test_cursor_resources_are_explicit_bounded_and_source_branches_preserved(self):
        self.assertEqual(resource_limits("standard")["maxSnapshotFiles"], 4096)
        # The live candidate writes a part integrity sidecar on every root.
        self.assertGreater(resource_limits("cursor")["maxSnapshotFiles"], 1025 * 4 * 3)
        self.assertEqual(resource_limits("cursor")["maxSnapshotFiles"], 16384)
        self.assertEqual(resource_limits("cursor")["minimumDriveFreeBytes"], 2 * 1024**3)
        self.assertEqual(list(CURSOR_BRANCHES), select("J03")[0]["variants"])
        with self.assertRaises(ValueError): resource_limits("unbounded")

    def test_complete_part_manifest_rejects_omitted_or_wrong_original_bytes(self):
        cohorts = [{"cohort": [{"node": "node1", "disk": disk, "relativePath": f"bucket/{index}/part.1",
            "versionId": "null", "partNumber": 1, "sha256": f"sha-{index}-{disk}", "size": 100}
            for disk in range(4)]} for index in range(CURSOR_OBJECTS)]
        authority = {(item["disk"], item["relativePath"]): item for cohort in cohorts for item in cohort["cohort"]}
        def remote(node, action, *, parts):
            self.assertEqual(action, "verify-parts")
            self.assertLessEqual(len(parts), 128)
            return {"complete": True, "parts": [{**part, "sha256": authority[part["disk"], part["relativePath"]]["sha256"],
                "physicalBitrotVerified": True, "physicalSize": 100} for part in parts]}
        fleet = types.SimpleNamespace(nodes=["node1"], remote=remote, record=lambda label, value: value)
        self.assertEqual(verify_all_parts(fleet, cohorts, "proof")["allPartsStreamingVerified"], CURSOR_OBJECTS * 4)
        good = fleet.remote
        for kind in ("omitted", "wrong-sha", "checksum", "size"):
            def invalid(node, action, *, parts):
                result = good(node, action, parts=parts)
                if kind == "omitted": result["parts"].pop()
                if kind == "wrong-sha": result["parts"][0]["sha256"] = "donor"
                if kind == "checksum": result["parts"][0]["physicalBitrotVerified"] = False
                if kind == "size": result["parts"][0]["physicalSize"] += 1
                return result
            fleet.remote = invalid
            with self.subTest(kind=kind), self.assertRaises(ValueError): verify_all_parts(fleet, cohorts, "proof")


class HealStopOracleTest(unittest.TestCase):
    def expected(self):
        return HealCursorOracleTest().expected()

    def cancelled(self):
        status = HealCursorOracleTest().terminal()
        objects = status["outcome"]["objects"][-2:]
        objects[0]["disposition"]["state"] = "cancelled"
        status.update(summary="stopped")
        status["outcome"].update(execution={"state": "aborted", "reason": "cancelled"}, coverage="partial", objects=objects)
        status["outcome"]["counters"].update(processed=4, healed=1, unchanged=2, skipped=1)
        targets = [{"key": "objects/1023.bin", "versionId": "null"}, {"key": "objects/1024.bin", "versionId": "null"}]
        return status, targets

    def test_cancelled_receipts_cannot_claim_complete_or_repaired_unresolved_objects(self):
        status, targets = self.cancelled()
        proof = validate_cancelled(status, HealCursorOracleTest.bucket, self.expected(), targets)
        self.assertEqual(proof["coverage"], "partial")
        for kind in ("completed", "coverage", "unknown", "counter", "full-heal", "cancelled-as-repaired", "identity"):
            changed = copy.deepcopy(status)
            if kind == "completed": changed["outcome"]["execution"] = {"state": "completed"}
            if kind == "coverage": changed["outcome"]["coverage"] = "complete"
            if kind == "unknown": changed["outcome"]["counters"]["unknown"] = 1
            if kind == "counter": changed["outcome"]["counters"]["processed"] += 1
            if kind == "full-heal": changed["outcome"]["counters"]["healed"] = 2
            if kind == "cancelled-as-repaired": changed["outcome"]["objects"][0]["disposition"]["state"] = "unknown"
            if kind == "identity": changed["outcome"]["objects"][0]["identity"]["bucket"] = "other"
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate_cancelled(changed, HealCursorOracleTest.bucket, self.expected(), targets)

    def test_active_window_requires_actual_partial_canonical_progress_and_no_errors(self):
        status, targets = self.cancelled()
        status.update(summary="running")
        status["outcome"]["execution"] = {"state": "running"}
        self.assertTrue(partial_window(status, targets, repair_required=True))
        for name, value in (("processed", 0), ("processed", CURSOR_OBJECTS), ("healed", 0), ("healed", 2)):
            changed = copy.deepcopy(status)
            changed["outcome"]["counters"][name] = value
            self.assertFalse(partial_window(changed, targets, repair_required=True))
        changed = copy.deepcopy(status)
        changed["outcome"]["counters"]["unknown"] = 1
        with self.assertRaises(ValueError): partial_window(changed, targets, repair_required=True)

    def test_repeat_stop_requires_idempotent_ack_for_the_exact_retained_token(self):
        response = {"httpStatus": 200, "query": [["clientToken", "old-token"], ["forceStop", "true"]],
            "method": "POST", "auth": "signed", "requestBody": "", "path": "/rustfs/admin/v3/heal/bucket/objects/",
            "response": {"summary": "stopped", "detail": "", "startTime": "2026-10-10T22:16:17.150884876Z",
                         "settings": {**BASE_OPTIONS, "scanMode": 1, "pool": None, "set": None}}}
        self.assertEqual(validate_retired_stop(response, "old-token")["exactRetiredTaskId"], "old-token")
        self.assertFalse(validate_retired_stop(response, "old-token")["responseEchoesTaskIdentity"])
        for changed in ({**response, "httpStatus": 500}, {**response, "transportError": "timeout"},
                        {**response, "query": [["clientToken", "new-token"], ["forceStop", "true"]]},
                        {**response, "response": {**response["response"], "summary": "finished"}},
                        {**response, "response": {**response["response"], "outcome": {"execution": {"state": "completed"}}}},
                        {**response, "response": {**response["response"], "settings": BASE_OPTIONS}},
                        {**response, "response": "<Error><Code>InternalError</Code><Message>Heal task not found: old-token</Message></Error>"}):
            with self.assertRaises(ValueError): validate_retired_stop(changed, "old-token")

    def test_unknown_token_is_not_confused_with_retained_terminal_idempotency(self):
        response = {"httpStatus": 500, "response": "<Error><Code>InternalError</Code><Message>Heal task not found: old-token</Message></Error>"}
        self.assertEqual(validate_unknown_token(response, "old-token")["exactUnknownTaskId"], "old-token")
        for changed in ({**response, "httpStatus": 200}, {**response, "transportError": "timeout"},
                        {**response, "response": response["response"].replace("old-token", "new-token")},
                        {**response, "response": response["response"].replace("InternalError", "AccessDenied")}):
            with self.assertRaises(ValueError): validate_unknown_token(changed, "old-token")

    def test_cancelled_physical_count_must_match_actual_original_shards(self):
        before = HealShardOracleTest().snapshot()
        for entry in list(before["node1"]["files"]):
            if "objects/0008.bin" not in entry["path"]: continue
            cloned = copy.deepcopy(entry)
            cloned["path"] = cloned["path"].replace("objects/0008.bin", "objects/0017.bin")
            if "metadata" in cloned: cloned["metadata"]["key"] = "objects/0017.bin"
            before["node1"]["files"].append(cloned)
        target = map_target(before, "rv-heal-0123456789abcdef", "objects/0008.bin", "data", 1)
        second = map_target(before, "rv-heal-0123456789abcdef", "objects/0017.bin", "data", 1)
        quiet = copy.deepcopy(before)
        quiet["node1"]["files"] = [entry for entry in quiet["node1"]["files"] if
            (entry["disk"], entry["path"]) != (second["disk"], second["relativePath"])]
        responsibility = {"canonicalCounters": {"healed": 1}, "diagnosticDispositions": [
            {"key": target["key"], "versionId": "null", "state": "repaired"},
            {"key": second["key"], "versionId": "null", "state": "cancelled"}]}
        remaining, proof = cancelled_physical(before, quiet, [target, second], responsibility)
        self.assertEqual(remaining, [second])
        self.assertFalse(proof["backupRescueUsed"])
        with self.assertRaisesRegex(ValueError, "disagrees"):
            cancelled_physical(before, quiet, [target, second], {"canonicalCounters": {"healed": 0}})
        wrong_disposition = copy.deepcopy(responsibility)
        wrong_disposition["diagnosticDispositions"][0]["state"] = "cancelled"
        with self.assertRaisesRegex(ValueError, "diagnostic disposition"):
            cancelled_physical(before, quiet, [target, second], wrong_disposition)
        next(entry for entry in quiet["node1"]["files"] if "/control/" in entry["path"])["sha256"] = "changed-control"
        with self.assertRaises(ValueError): cancelled_physical(before, quiet, [target, second], responsibility)

    def test_every_stop_source_branch_is_retained(self):
        self.assertEqual(list(STOP_BRANCHES), select("A06")[0]["variants"])

    def test_task_exit_waits_for_actual_drain_and_never_hides_partial_idle(self):
        good = {"httpStatus": 200, "response": {"state": "idle", "clusterStatusComplete": True,
            "coverage": {"expected": 1, "responded": 1, "unknown": 0, "reasons": []},
            "healQueueLength": 0, "healActiveTasks": 0,
            "healOperations": {"queueLength": 0, "activeTasks": 0, "retryingTasks": 0}}}
        active = copy.deepcopy(good)
        active["response"].update(state="active", healActiveTasks=1)
        active["response"]["healOperations"]["activeTasks"] = 1
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        client = types.SimpleNamespace(admin=types.SimpleNamespace())
        from unittest.mock import Mock
        client.admin = Mock(side_effect=[active, good])
        binding = {"endpoints": {"node1": "url"}}
        with patch("scripts.validation_heal.stop.Probe", return_value=client), patch("scripts.validation_heal.stop.time.sleep"):
            self.assertEqual(stop_idle_fleet(fleet, binding)["node1"]["state"], "idle")
        unsafe = copy.deepcopy(good)
        unsafe["response"]["clusterStatusComplete"] = False
        client.admin = Mock(side_effect=[unsafe, good])
        with patch("scripts.validation_heal.stop.Probe", return_value=client):
            with self.assertRaisesRegex(ValueError, "partial fleet"):
                stop_idle_fleet(fleet, binding)
        self.assertEqual(client.admin.call_count, 1)


class HealCancelledRestartOracleTest(unittest.TestCase):
    token = "12345678-1234-1234-1234-123456789abc"

    def journal(self):
        status, _ = HealStopOracleTest().cancelled()
        terminal = {"schema": 1, "task_id": self.token, "status": "Cancelled",
            "heal_type": {"type": "prefix", "bucket": HealCursorOracleTest.bucket, "prefix": "objects/"}}
        report = {"schema": 1, "terminal": terminal, "outcome": status["outcome"], "min_seq": 1, "next_seq": 2,
            "seqed_items": [[1, {"bucket": HealCursorOracleTest.bucket, "object": "objects/0000.bin", "objectSize": CURSOR_SIZE}]]}
        records = [{"disk": disk, "kind": kind, "present": disk == 0 and kind != "intent",
            **({"value": terminal if kind == "terminal" else report, "sha256": "original", "size": 100}
               if disk == 0 and kind != "intent" else {})} for disk in range(4) for kind in ("intent", "terminal", "report")]
        return {"node1": {"taskId": self.token, "readOnly": True, "records": records}}, status

    def test_actual_cancelled_journal_rejects_runnable_duplicate_or_wrong_epoch_records(self):
        observed, status = self.journal()
        expected = HealCursorOracleTest().expected()
        self.assertTrue(validate_journal(observed, self.token, HealCursorOracleTest.bucket, status, expected)["runnableIntentAbsentOnEveryDrive"])
        for kind in ("intent", "duplicate", "scope", "status", "schema", "identity", "outcome", "cursor", "owner", "missing", "item-key", "item-version", "item-size", "item-duplicate", "item-empty"):
            changed = copy.deepcopy(observed)
            records = changed["node1"]["records"]
            terminal, report = records[1]["value"], records[2]["value"]
            if kind == "intent": records[0]["present"] = True
            if kind == "duplicate": records[4].update(present=True, value=terminal)
            if kind == "scope": terminal["heal_type"]["bucket"] = "unowned"
            if kind == "status": terminal["status"] = "Completed"
            if kind == "schema": terminal["schema"] = True
            if kind == "identity": terminal["task_id"] = "another-task"
            if kind == "outcome": report["outcome"]["counters"]["healed"] += 1
            if kind == "cursor": report["seqed_items"][0][0] = report["next_seq"]
            if kind == "owner": records[2]["disk"] = 1
            if kind == "missing": records[2]["present"] = False
            if kind == "item-key": report["seqed_items"][0][1]["object"] = "objects/unregistered.bin"
            if kind == "item-version": report["seqed_items"][0][1]["versionId"] = "another-version"
            if kind == "item-size": report["seqed_items"][0][1]["objectSize"] += 1
            if kind == "item-duplicate":
                report["next_seq"] = 3
                report["seqed_items"].append([2, copy.deepcopy(report["seqed_items"][0][1])])
            if kind == "item-empty": report["seqed_items"] = []
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                validate_journal(changed, self.token, HealCursorOracleTest.bucket, status, expected)

    def test_node_reader_is_read_only_and_only_accepts_exact_task_filenames(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            state = root / "state"
            state.mkdir()
            (state / "rustfs").write_bytes(b"candidate")
            roots = [root / f"disk{disk}" for disk in range(4)]
            for disk in roots: (disk / "data/.rustfs.sys").mkdir(parents=True)
            system = roots[0] / "data/.rustfs.sys"
            terminal = system / f"terminal-root-heal-{self.token}.json"
            terminal.write_text('{"task_id":"' + self.token + '"}')
            unrelated = system / "terminal-root-heal-unrelated.json"
            unrelated.write_text('{"unrelated":"must not enter evidence"}')
            owner = {"binarySha256": hashlib.sha256(b"candidate").hexdigest()}
            with patch("scripts.validation_heal.fleet_node.owned", return_value=(state, roots, owner)), \
                 patch("scripts.validation_heal.fleet_node.stopped"):
                observed = node_task_records({"runId": "run", "taskId": self.token, "quiescent": True})
                self.assertTrue(observed["readOnly"])
                self.assertEqual(len(observed["records"]), 12)
                self.assertEqual(sum(entry["present"] for entry in observed["records"]), 1)
                self.assertNotIn("must not enter evidence", json.dumps(observed))
                self.assertEqual(terminal.read_text(), '{"task_id":"' + self.token + '"}')
                for invalid in ("../other", self.token.upper(), "not-a-uuid"):
                    with self.assertRaises(ValueError): node_task_records({"runId": "run", "taskId": invalid, "quiescent": True})
                terminal.unlink()
                terminal.symlink_to(unrelated)
                with self.assertRaises(ValueError): node_task_records({"runId": "run", "taskId": self.token, "quiescent": True})

    def test_restart_preserves_every_source_branch(self):
        self.assertEqual(list(CANCEL_RESTART_BRANCHES), select("F06")[0]["variants"])


class HealOverlapOracleTest(unittest.TestCase):
    bucket = "rv-heal-overlap-unit"

    def expected(self):
        return [{"key": f"objects/a/child/{index:04d}.bin", "versionId": "null", "deleteMarker": False,
                 "payload": {"size": CURSOR_SIZE}} for index in range(overlap.SCOPE_OBJECTS)]

    def damaged(self):
        return {(self.expected()[index]["key"], "null") for index in overlap.MISSING}

    def state(self, *, cancelled=False, replacement=False, healed=None):
        total = 100 if cancelled else overlap.SCOPE_OBJECTS
        repaired = {0, 64} if cancelled else set(overlap.MISSING)
        if replacement: repaired -= {0, 64}
        return {"summary": "stopped" if cancelled else "finished", "outcome": {
            "execution": {"state": "aborted", "reason": "cancelled"} if cancelled else {"state": "completed"},
            "coverage": "partial" if cancelled else "complete", "objectsTruncated": total > 128,
            "objects": [{"identity": {"bucket": self.bucket, "object": self.expected()[index]["key"],
                         "versionId": None, "poolIndex": 0, "setIndex": 0, "bucketIncarnationId": "incarnation"},
                         "disposition": {"state": "repaired" if index in repaired else "verified_healthy"}}
                        for index in range(min(total, 128))],
            "counters": {"processed": total, "healed": len(repaired) if healed is None else healed,
                         "unchanged": total - (len(repaired) if healed is None else healed),
                         "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}}}

    def proof(self, current, **kwargs):
        return overlap.scope_proof(current, self.bucket, self.expected(), self.damaged(), **kwargs)

    def test_frozen_policy_rejects_arbitrary_http_and_reason(self):
        good = {"httpStatus": 409, "response": {"Code": "OperationAborted", "Message": overlap.POLICY["parent-child"]["message"]}}
        self.assertEqual(overlap.overlap_rejection(good)["code"], "OperationAborted")
        for changed in ({**good, "httpStatus": 200}, {**good, "transportError": "timeout"},
                        {**good, "response": {"Code": "OperationAborted", "Message": "already_running"}}):
            with self.assertRaises(ValueError): overlap.overlap_rejection(changed)

    def test_incomplete_or_cross_scope_receipts_and_unknowns_never_pass(self):
        self.proof(self.state(), healed=5)
        for kind in ("count", "unknown", "failed", "attempt", "coverage", "diagnostic", "key", "version", "incarnation", "role", "duplicate"):
            current = self.state()
            value = current["outcome"]
            if kind == "count": value["counters"]["healed"] = 4
            if kind in {"unknown", "failed"}: value["counters"][kind] = 1
            if kind == "attempt": value["counters"]["attemptFailures"] = 1
            if kind == "coverage": value["coverage"] = "partial"
            if kind == "diagnostic": value["objectsTruncated"] = False
            if kind == "key": value["objects"][0]["identity"]["object"] = "objects/b/0000.bin"
            if kind == "version": value["objects"][0]["identity"]["versionId"] = "other-version"
            if kind == "incarnation": value["objects"][0]["identity"]["bucketIncarnationId"] = "old-bucket"
            if kind == "role": value["objects"][1]["disposition"]["state"] = "repaired"
            if kind == "duplicate": value["objects"][1] = copy.deepcopy(value["objects"][0])
            with self.subTest(kind=kind), self.assertRaises(ValueError): self.proof(current, healed=5)

    def test_force_accounting_preserves_partial_truth_and_rejects_double_repair(self):
        old = self.proof(self.state(cancelled=True), cancelled=True)
        new = self.proof(self.state(replacement=True), healed=3, replacement=True)
        self.assertEqual(overlap.force_accounting(old, new, self.damaged())["exactTotalRepairs"], 5)
        for kind in ("count", "incarnation", "duplicate"):
            changed = copy.deepcopy(new)
            if kind == "count": changed["canonicalCounters"]["healed"] = 4
            if kind == "incarnation": changed["bucketIncarnationId"] = "new-bucket"
            if kind == "duplicate": changed["diagnosticDispositions"][self.expected()[0]["key"]] = "repaired"
            with self.subTest(kind=kind), self.assertRaises(ValueError): overlap.force_accounting(old, changed, self.damaged())
        with self.assertRaises(ValueError): self.proof(self.state(), cancelled=True)
        with self.assertRaises(ValueError): self.proof(self.state(cancelled=True), healed=2)

    def test_terminal_suffix_checks_every_ack_and_never_infers_a_retention_gap(self):
        current = self.state()
        pages = []
        for left, right in ((0, 100), (100, 257)):
            pages.append({**copy.deepcopy(current), "nextSeq": 258, "minSeq": 1, "truncated": right < 257,
                "items": [{"bucket": self.bucket, "object": entry["key"], "versionId": "", "objectSize": CURSOR_SIZE}
                          for entry in self.expected()[left:right]]})
        client = types.SimpleNamespace(admin=lambda *args: {"httpStatus": 200, "response": pages.pop(0)})
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        with patch.object(overlap, "status", return_value=current):
            _, proof = overlap.completed_scope(client, fleet, "/scope/", "task", self.bucket, self.expected(), self.damaged(), healed=5)
        self.assertEqual(proof["completeItemIdentities"], 257)
        self.assertFalse(proof["retentionGapClaimed"])
        for kind in ("duplicate", "gap", "empty", "changed-outcome"):
            page = {**copy.deepcopy(current), "nextSeq": 258, "minSeq": 1, "items": [
                {"bucket": self.bucket, "object": entry["key"], "versionId": "", "objectSize": CURSOR_SIZE} for entry in self.expected()]}
            if kind == "duplicate": page["items"][1] = copy.deepcopy(page["items"][0])
            if kind == "gap": page["minSeq"] = 2
            if kind == "empty": page["items"] = []
            if kind == "changed-outcome": page["outcome"]["counters"]["unknown"] = 1
            client.admin = lambda *args: {"httpStatus": 200, "response": page}
            with self.subTest(kind=kind), patch.object(overlap, "status", return_value=current), self.assertRaises(ValueError):
                overlap.completed_scope(client, fleet, "/scope/", "task", self.bucket, self.expected(), self.damaged(), healed=5)

    def test_finished_window_and_same_token_counter_rollback_cannot_pass(self):
        self.assertFalse(overlap.active_window(self.state()))
        before = self.state(cancelled=True)
        after = copy.deepcopy(before)
        after["outcome"]["counters"].update(processed=99, unchanged=97)
        with self.assertRaisesRegex(ValueError, "backwards"): overlap.monotonic(before, after)
        with patch.object(overlap, "status", return_value=self.state()), self.assertRaisesRegex(ValueError, "NOT_OBSERVED"):
            overlap.wait_window(None, None, "/scope/", "task")

    def test_all_eight_source_instances_and_cleanup_failure_stop_are_real_dispatches(self):
        with tempfile.TemporaryDirectory() as folder:
            args = types.SimpleNamespace(evidence=Path(folder))
            calls = []
            def topology(params, topology, nodes, **options):
                calls.append((topology, options["branches"][0], options["missing_indices"]))
                return {"verdict": "PASS", "cleanup": "PASS"}
            with patch.object(overlap, "dx_topology", side_effect=topology):
                for name in ("H1", "H2"):
                    self.assertEqual(overlap.run_topology(args, name, ["node1"])["verdict"], "PASS")
            self.assertEqual([(name, branch) for name, branch, _ in calls], [(name, branch) for name in ("H1", "H2") for branch in overlap.BRANCHES])
            self.assertTrue(all(len(indices) == (10 if branch == "disjoint" else 5) for _, branch, indices in calls))
            with patch.object(overlap, "dx_topology", return_value={"verdict": "FAIL", "cleanup": "FAIL"}) as operation:
                self.assertEqual(overlap.run_topology(args, "H1", ["node1"])["cleanup"], "FAIL")
                self.assertEqual(operation.call_count, 1)
        self.assertEqual(list(overlap.BRANCHES), select("A05")[0]["variants"])
        self.assertEqual(runtime_config("overlap")["RUSTFS_HEAL_OVERLAP_POLICY"], "merge")
        # Parameterizing A05 physical fixtures must not weaken J03 retention.
        with self.assertRaises(ValueError): Window(self.bucket, self.expected(), "task", "node1", "epoch")


class HealRootRestartOracleTest(unittest.TestCase):
    token = "01234567-89ab-4def-8123-456789abcdef"

    def intent(self):
        return {"schema": 3, "task_id": self.token, "heal_type": {"type": "cluster"}, "priority": "High",
                "created_at": {"secs_since_epoch": 10, "nanos_since_epoch": 0}, "retry_attempts": 0,
                "options": {"scan_mode": 2, "remove_corrupted": False, "recreate_missing": False,
                    "update_parity": False, "recursive": True, "dry_run": False, "no_lock": False,
                    "pool_index": None, "set_index": None, "timeout": None}}

    def records(self):
        entries = [{"disk": disk, "kind": kind, "present": disk == 2 and kind == "intent",
                    "relativePath": ".rustfs.sys/root-heal-" + self.token + ".json", "value": self.intent()}
                   for disk in range(4) for kind in ("intent", "terminal", "report")]
        return {"node1": {"readOnly": True, "taskId": self.token, "records": entries}}

    def process(self, pid=10, start=50):
        return {name: value for name, value in zip(fleet_node.PROCESS_FIELDS,
            (pid, start, "boot", "unit", "/state/rustfs", "sha", "mnt:[1]", "command", "env", {"budget": "300"}), strict=True)} | {"state": "T"}

    def test_root_owner_rejects_duplicate_missing_and_cross_scope_records(self):
        self.assertEqual(root_restart.root_intent(self.records(), self.token, "node1")["disk"], 2)
        for kind in ("missing", "duplicate", "scope", "schema", "selector", "read-repair", "retry", "coverage"):
            observed = self.records()
            entries = observed["node1"]["records"]
            entry = next(row for row in entries if row["present"])
            if kind == "missing": entry["present"] = False
            if kind == "duplicate": entries[0]["present"] = True
            if kind == "scope": entry["value"]["heal_type"] = {"type": "prefix", "bucket": "other", "prefix": "objects/"}
            if kind == "schema": entry["value"]["schema"] = 2
            if kind == "selector": entry["value"]["options"]["pool_index"] = 0
            if kind == "read-repair": entry["value"]["options"]["no_lock"] = True
            if kind == "retry": entry["value"]["retry_attempts"] = 1
            if kind == "coverage": entries.pop()
            with self.subTest(kind=kind), self.assertRaises(ValueError): root_restart.root_intent(observed, self.token, "node1")

    def test_checkpoint_budget_cannot_be_removed_refunded_or_move_owner(self):
        before = root_restart.root_intent(self.records(), self.token, "node1")
        after = copy.deepcopy(before)
        after["value"]["options"]["timeout"] = {"secs": 280, "nanos": 7}
        proof = root_restart.same_owner(before, after)
        self.assertEqual(proof["checkpointRemainingTimeoutNs"], 280_000_000_007)
        for kind in ("absent", "refund", "zero", "disk", "identity", "scope", "option", "bool", "nanos"):
            current = copy.deepcopy(after)
            if kind == "absent": current["value"]["options"]["timeout"] = None
            if kind == "refund": current["value"]["options"]["timeout"]["secs"] = 301
            if kind == "zero": current["value"]["options"]["timeout"] = {"secs": 0, "nanos": 0}
            if kind == "disk": current["disk"] = 3
            if kind == "identity": current["value"]["task_id"] = "new-task"
            if kind == "scope": current["value"]["heal_type"] = {"type": "bucket", "bucket": "other"}
            if kind == "option": current["value"]["options"]["remove_corrupted"] = True
            if kind == "bool": current["value"]["options"]["timeout"]["secs"] = True
            if kind == "nanos": current["value"]["options"]["timeout"]["nanos"] = 1_000_000_000
            with self.subTest(kind=kind), self.assertRaises(ValueError): root_restart.same_owner(before, current)
        with self.assertRaises(ValueError): root_restart.same_owner(after, before)

    def test_only_original_coordinator_changes_birth_identity(self):
        before = {"node1": self.process(), "node2": self.process(11, 51)}
        after = {"node1": self.process(12, 60), "node2": self.process(11, 51)}
        self.assertTrue(root_restart.restart_identity(before, after, "node1")["onlyCoordinatorProcessChanged"])
        for kind in ("no-restart", "peer-restart", "boot", "env", "namespace", "candidate", "missing"):
            current = copy.deepcopy(after)
            if kind == "no-restart": current["node1"] = copy.deepcopy(before["node1"])
            if kind == "peer-restart": current["node2"]["startTicks"] += 1
            if kind == "boot": current["node1"]["bootId"] = "new-boot"
            if kind == "env": current["node1"]["environmentFileSha256"] = "new-env"
            if kind == "namespace": current["node1"]["mountNamespace"] = "mnt:[2]"
            if kind == "candidate": current["node1"]["binarySha256"] = "new-build"
            if kind == "missing": del current["node2"]
            with self.subTest(kind=kind), self.assertRaises(ValueError): root_restart.restart_identity(before, current, "node1")

    def test_pidfd_revalidates_birth_identity_before_any_signal(self):
        expected = self.process()
        with patch.object(fleet_node.os, "pidfd_open", return_value=17, create=True), \
             patch.object(fleet_node.signal, "pidfd_send_signal", create=True) as send, \
             patch.object(fleet_node.os, "close") as close, patch.object(fleet_node, "process", return_value=expected):
            fleet_node.pidfd_signal({"expected": expected}, fleet_node.signal.SIGSTOP)
            send.assert_called_once_with(17, fleet_node.signal.SIGSTOP, None, 0)
            close.assert_called_once_with(17)
        changed = {**expected, "startTicks": expected["startTicks"] + 1}
        with patch.object(fleet_node.os, "pidfd_open", return_value=17, create=True), \
             patch.object(fleet_node.signal, "pidfd_send_signal", create=True) as send, \
             patch.object(fleet_node.os, "close") as close, patch.object(fleet_node, "process", return_value=changed), self.assertRaisesRegex(ValueError, "stale"):
            fleet_node.pidfd_signal({"expected": expected}, fleet_node.signal.SIGSTOP)
        send.assert_not_called(); close.assert_called_once_with(17)
        with self.assertRaises(ValueError): fleet_node.process_signal({"signal": "SIGKILL", "expected": expected})

    def test_graceful_exit_requires_actual_exit_zero_and_preserves_identity(self):
        before = self.process()
        exited = {"pid": 0, "bootId": "boot", "binarySha256": "sha", "environmentFileSha256": "env",
                  "service": {"ExecMainCode": "1", "ExecMainStatus": "0", "Result": "success"}}
        for kind in ("good", "signal", "failed", "timeout", "boot", "candidate", "environment"):
            current = copy.deepcopy(exited)
            if kind == "signal": current["service"]["ExecMainCode"] = "2"
            if kind == "failed": current["service"]["ExecMainStatus"] = "1"
            if kind == "timeout": current["service"]["Result"] = "timeout"
            if kind == "boot": current["bootId"] = "new-boot"
            if kind == "candidate": current["binarySha256"] = "new-build"
            if kind == "environment": current["environmentFileSha256"] = "new-env"
            with self.subTest(kind=kind), patch.object(fleet_node, "owned", return_value=(None, None, {"runtimeProfile": "root-restart"})), \
                 patch.object(fleet_node, "process", side_effect=[before, current]), patch.object(fleet_node, "pidfd_signal", return_value=before):
                if kind == "good":
                    self.assertEqual(fleet_node.process_graceful({"runId": "run", "expected": before})["actualExitCode"], 0)
                else:
                    with self.assertRaises(ValueError): fleet_node.process_graceful({"runId": "run", "expected": before})

    def test_recovery_absence_is_bounded_observation_and_never_completion(self):
        missing = {"httpStatus": 500, "response": {"Code": "InternalError", "Message": "Heal task not found: " + self.token}}
        good = {"httpStatus": 200, "response": {"summary": "running"}}
        client = types.SimpleNamespace(admin=Mock(side_effect=[missing, good]))
        fleet = types.SimpleNamespace(record=lambda name, value: value)
        with patch.object(root_restart.time, "sleep"):
            self.assertEqual(root_restart.recovery_status(client, fleet, self.token)["summary"], "running")
        client.admin = lambda *args: missing
        with patch.object(root_restart.time, "monotonic", side_effect=[0, 16]), self.assertRaisesRegex(ValueError, "15-second"):
            root_restart.recovery_status(client, fleet, self.token)
        self.assertEqual(list(root_restart.BRANCHES), select("F01")[0]["variants"])

    def test_completed_root_journal_retires_intent_and_preserves_checkpoint_options(self):
        owner = root_restart.root_intent(self.records(), self.token, "node1")
        owner["value"]["options"]["timeout"] = {"secs": 280, "nanos": 0}
        expected = HealCursorOracleTest().expected()
        bucket = HealCursorOracleTest.bucket
        current = HealCursorOracleTest().terminal()
        terminal = {"schema": 1, "task_id": self.token, "heal_type": {"type": "cluster"}, "status": "Completed",
                    "options": owner["value"]["options"]}
        report = {"schema": 1, "terminal": copy.deepcopy(terminal), "outcome": current["outcome"],
            "next_seq": 1027, "min_seq": 3, "seqed_items": [[index + 2,
                {"bucket": bucket, "object": entry["key"], "versionId": "", "objectSize": CURSOR_SIZE}]
                for index, entry in enumerate(expected[1:], 1)]}
        observed = self.records()
        for entry in observed["node1"]["records"]:
            entry["present"] = entry["disk"] == 2 and entry["kind"] in {"terminal", "report"}
            if entry["present"]: entry["value"] = terminal if entry["kind"] == "terminal" else report
        self.assertTrue(root_restart.root_terminal(observed, self.token, owner, current, bucket, expected)["runnableIntentRetiredOnEveryDrive"])
        for kind in ("intent", "owner", "scope", "state", "refund", "outcome", "duplicate", "wrong-version", "wrong-size"):
            changed = copy.deepcopy(observed)
            rows = changed["node1"]["records"]
            end = next(row for row in rows if row["present"] and row["kind"] == "terminal")
            durable = next(row for row in rows if row["present"] and row["kind"] == "report")
            if kind == "intent": rows[0]["present"] = True
            if kind == "owner": end["disk"] = 3
            if kind == "scope": end["value"]["heal_type"] = {"type": "bucket", "bucket": bucket}
            if kind == "state": end["value"]["status"] = "Cancelled"
            if kind == "refund": end["value"]["options"]["timeout"]["secs"] = 300
            if kind == "outcome": durable["value"]["outcome"]["counters"]["unknown"] = 1
            if kind == "duplicate": durable["value"]["seqed_items"][1][1] = copy.deepcopy(durable["value"]["seqed_items"][0][1])
            if kind == "wrong-version": durable["value"]["seqed_items"][0][1]["versionId"] = "unacknowledged"
            if kind == "wrong-size": durable["value"]["seqed_items"][0][1]["objectSize"] += 1
            with self.subTest(kind=kind), self.assertRaises(ValueError): root_restart.root_terminal(changed, self.token, owner, current, bucket, expected)

    def test_journal_reads_mark_the_actual_coordinator_as_stopped(self):
        calls = []
        fleet = types.SimpleNamespace(nodes=["node1", "node2", "node3", "node4"], record=lambda name, value: value,
            remote=lambda node, action, **params: calls.append((node, params["quiescent"])) or {})
        root_restart.records(fleet, self.token, "checkpoint", stopped_node="node3")
        self.assertEqual(calls, [("node1", False), ("node2", False), ("node3", True), ("node4", False)])

    def test_cleanup_resumes_only_the_proved_original_process_before_finalization(self):
        fleet = Fleet(["node1"], Path("/tmp/unit-root-restart"), "run")
        fleet.frozen_processes = {"node1": self.process()}
        fleet.reserved = ["node1"]
        calls = []
        def remote(node, action, **kwargs):
            calls.append((action, kwargs))
            return {"pid": 10} if action == "process" else {}
        fleet.remote = remote
        fleet.record = lambda name, value: value
        self.assertTrue(fleet.cleanup())
        self.assertEqual([action for action, _ in calls[:2]], ["process", "process-signal"])
        self.assertEqual(calls[1][1]["signal"], "SIGCONT")
        self.assertEqual(calls[1][1]["expected"]["startTicks"], 50)
        self.assertFalse(fleet.frozen_processes)
        fleet.frozen_processes = {"node1": self.process()}
        def stale(node, action, **kwargs):
            calls.append((action, kwargs))
            if action == "process-signal": raise ValueError("stale process")
            return {"pid": 10} if action == "process" else {}
        fleet.remote = stale; calls.clear()
        self.assertFalse(fleet.cleanup())
        self.assertFalse(any(action == "cleanup" for action, _ in calls))


class HealReadQuorumBoundaryTest(unittest.TestCase):
    contract = {"recognized": True, "writeQuorumRule": "data-plus-one-if-equal-parity"}
    bucket = "rv-heal-0123456789abcdef"

    def scene(self, branch="exact-r-read", *, total=4, data=2):
        original = HealShardOracleTest().snapshot()["node1"]
        snapshot = {}
        disk_ids = [f"disk-{index}" for index in range(total)]
        key = "boundary/" + branch + ".bin"
        for slot in range(total):
            node, disk = "node" + str(slot // 4 + 1), slot % 4
            value = snapshot.setdefault(node, {"formats": [], "files": []})
            value["formats"].append({"disk": disk, "value": {"format": "xl", "id": "pool",
                "xl": {"sets": [disk_ids], "this": disk_ids[slot]}}})
            metadata = copy.deepcopy(original["files"][0]["metadata"])
            metadata["key"] = key
            version = metadata["versions"][0]
            version["role"] = "data" if slot < data else "parity"
            version["erasure"].update(index=slot + 1, data_blocks=data, parity_blocks=total - data,
                                       distribution=list(range(1, total + 1)))
            value["files"].extend([
                {"disk": disk, "path": f"{self.bucket}/{key}/xl.meta", "size": 600,
                 "sha256": "metadata-" + str(slot), "metadata": metadata},
                {"disk": disk, "path": f"{self.bucket}/{key}/{version['dataDir']}/part.1", "size": 82,
                 "sha256": "shard-" + str(slot)}])
        expected = [{"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                     "payload": {"size": 100, "sha256": "acknowledged"}}]
        return snapshot, expected

    def plan(self, branch="exact-r-read", **kwargs):
        snapshot, expected = self.scene(branch, **kwargs)
        return snapshot, quorum_boundary.boundary_plan(branch, snapshot, self.bucket, expected, self.contract)

    def missing(self, snapshot, targets):
        changed = copy.deepcopy(snapshot)
        for target in targets:
            changed[target["node"]]["files"] = [entry for entry in changed[target["node"]]["files"]
                if (entry["disk"], entry["path"]) != (target["disk"], target["relativePath"])]
        return changed

    def test_source_quorum_is_distinct_from_writable_drives_and_host_locks(self):
        for total, data, write, clients in ((4, 2, 3, 1), (16, 12, 12, 4), (16, 8, 9, 4)):
            snapshot, plan = self.plan("exact-r-heal-write-limited", total=total, data=data)
            self.assertEqual((plan["readQuorum"], plan["writeQuorum"], plan["writableTargets"]), (data, write, total))
            self.assertEqual(plan["lock"]["clientCount"], clients)
            self.assertEqual(plan["lock"]["exclusiveQuorum"], clients // 2 + 1)
            self.assertFalse(plan["insufficientWriteOrLockApplicable"])
            witness = quorum_boundary.source_witness(self.missing(snapshot, plan["faults"]), plan, plan["faults"])
            self.assertEqual(witness["sameVersionPayloadSources"], data)

    def test_restore_to_w_leaves_real_targets_and_does_not_count_restored_sources_as_heal(self):
        for total, data, restored, remaining in ((4, 2, 2, 1), (16, 12, 1, 4), (16, 8, 2, 7)):
            snapshot, plan = self.plan("restored-w", total=total, data=data)
            self.assertEqual(len(plan["retained"]), data - 1)
            self.assertEqual(plan["restoreCount"], restored)
            targets = plan["faults"][restored:]
            self.assertEqual(len(targets), remaining)
            after = self.missing(snapshot, targets)
            self.assertEqual(quorum_boundary.source_witness(after, plan, targets)["sameVersionPayloadSources"], plan["writeQuorum"])

    def test_source_witness_rejects_changed_generation_extra_sources_and_good_shard_mutation(self):
        snapshot, plan = self.plan()
        faulted = self.missing(snapshot, plan["faults"])
        for kind in ("reappeared", "retained-bytes", "metadata", "foreign-target", "duplicate-target"):
            changed, targets = copy.deepcopy(faulted), copy.deepcopy(plan["faults"])
            if kind == "reappeared": changed = self.missing(snapshot, targets[1:])
            elif kind == "retained-bytes": next(entry for entry in changed["node1"]["files"] if entry["path"].endswith("/part.1"))["sha256"] = "corrupt"
            elif kind == "metadata": changed["node1"]["files"][0]["sha256"] = "another-generation"
            elif kind == "foreign-target": targets[0]["relativePath"] += ".wrong-version"
            else: targets.append(copy.deepcopy(targets[0]))
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                quorum_boundary.source_witness(changed, plan, targets)

    def test_read_boundary_requires_complete_acknowledged_body_and_records_interference(self):
        snapshot, plan = self.plan()
        expected = plan["expected"][0]
        good = {"acknowledged": True, "resultUnknown": False, "payload": expected["payload"],
                "response": {"ResponseMetadata": {"HTTPStatusCode": 200}}}
        quorum_boundary.read_observation(good, expected)
        for field, value in (("acknowledged", False), ("resultUnknown", True), ("payload", {"size": 0, "sha256": "empty"})):
            with self.subTest(field=field), self.assertRaises(ValueError):
                quorum_boundary.read_observation({**good, field: value}, expected)
        faulted = self.missing(snapshot, plan["faults"])
        self.assertFalse(quorum_boundary.read_interference(faulted, faulted, plan)["readRepairObserved"])
        proof = quorum_boundary.read_interference(faulted, snapshot, plan)
        self.assertTrue(proof["readRepairObserved"])
        self.assertFalse(proof["manualRepairClaimed"])
        changed = copy.deepcopy(snapshot)
        target = plan["faults"][0]
        next(entry for entry in changed[target["node"]]["files"] if entry["disk"] == target["disk"]
             and entry["path"] == target["relativePath"])["sha256"] = "bad-reconstruction"
        with self.assertRaises(ValueError): quorum_boundary.read_interference(faulted, changed, plan)

    def test_recovery_keeps_unresolved_registration_when_backup_oracle_is_rejected(self):
        registration = ("node1", {"disk": 0, "faultId": "B06-exact-r-read-0"})
        fleet = Mock(faults=[registration])
        fleet.remote.return_value = {"physicalRepairPassAllowed": False, "restoredFromBackup": True}
        fleet.record.side_effect = lambda label, value: value
        registrations = [registration]
        quorum_boundary.recover_registered(fleet, registrations, "rescue")
        self.assertFalse(fleet.faults or registrations)
        fleet.faults, registrations = [registration], [registration]
        fleet.remote.return_value = {"physicalRepairPassAllowed": True}
        with self.assertRaises(ValueError): quorum_boundary.recover_registered(fleet, registrations, "rescue")
        self.assertEqual(fleet.faults, registrations)

    def test_branch_and_geometry_contract_cannot_be_weakened(self):
        self.assertEqual(list(quorum_boundary.BRANCHES), select("B06")[0]["variants"])
        snapshot, expected = self.scene()
        for contract in ({}, {"recognized": True, "writeQuorumRule": "unknown"}):
            with self.assertRaises(ValueError): quorum_boundary.boundary_plan("exact-r-read", snapshot, self.bucket, expected, contract)
        with self.assertRaises(ValueError): quorum_boundary.boundary_plan("exact-r-read", snapshot, self.bucket, expected * 2, self.contract)


class HealStaleRejoinTest(unittest.TestCase):
    def scene(self):
        before, bucket, expected = versioned_scene(["objects/0001.bin", "control/object.bin"])
        for key in {entry["key"] for entry in expected}:
            for nonce, entry in enumerate(entry for entry in expected if entry["key"] == key): entry["payloadNonce"] = nonce
        target = map_target(before, bucket, "objects/0001.bin", "data", 1, "v0", latest_required=False)
        manifest = [entry for entry in expected if (entry["key"], entry["versionId"]) != (target["key"], "v0")]
        authority = copy.deepcopy(before)
        member = target["node"], target["disk"]
        for node, value in authority.items():
            for entry in value["files"]:
                if (node, entry["disk"]) == member: continue
                metadata = entry.get("metadata")
                if metadata and metadata["key"] == target["key"]:
                    metadata["versions"] = [version for version in metadata["versions"] if version["versionId"] != "v0"]
                    entry["sha256"] += "-deleted-v0"
            value["files"] = [entry for entry in value["files"] if (node, entry["disk"]) == member
                or not entry["path"].startswith(f"{bucket}/{target['key']}/12345678-1234-1234-1234-000000000000/")]
        witness = stale_rejoin.authority_witness(before, authority, bucket, target["key"], manifest, target)
        after = copy.deepcopy(authority)
        for entry in after[member[0]]["files"]:
            if entry["disk"] == member[1] and entry.get("metadata", {}).get("key") == target["key"]:
                entry["metadata"]["versions"] = [version for version in entry["metadata"]["versions"] if version["versionId"] != "v0"]
                entry["sha256"] += "-deleted-v0"
        return before, authority, after, bucket, manifest, target, witness

    def test_same_disk_stale_generation_requires_full_online_ack_authority(self):
        before, authority, after, bucket, expected, target, witness = self.scene()
        self.assertEqual((witness["readQuorum"], witness["writeQuorum"], witness["onlineAuthorities"]), (2, 3, 3))
        cohorts, proof = stale_rejoin.physical_rejoin(authority, after, bucket, target["key"], expected, target, witness)
        self.assertEqual({cohort["versionId"] for cohort in cohorts}, {"v1", "v2"})
        self.assertFalse(proof["backupRestoreUsed"])
        for kind in ("offline-write", "lost-online-version", "no-stale-generation", "control", "format"):
            changed = copy.deepcopy(authority)
            if kind == "offline-write": next(entry for entry in changed[target["node"]]["files"] if entry["disk"] == target["disk"])["sha256"] = "changed"
            elif kind == "lost-online-version":
                next(entry for entry in changed["node1"]["files"] if entry["disk"] != target["disk"]
                     and entry.get("metadata", {}).get("key") == target["key"])["metadata"]["versions"].pop()
            elif kind == "no-stale-generation": changed = copy.deepcopy(after)
            elif kind == "control": next(entry for entry in changed["node1"]["files"] if "/control/" in entry["path"])["sha256"] = "changed"
            else: changed["node1"]["formats"][0]["value"]["id"] = "foreign"
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                stale_rejoin.authority_witness(before, changed, bucket, target["key"], expected, target)

    def test_physical_convergence_cannot_change_retained_shards_or_quorum_controls(self):
        _, authority, after, bucket, expected, target, witness = self.scene()
        for kind in ("retained-target-shard", "quorum-shard", "control", "resurrected-metadata", "extra-file"):
            changed = copy.deepcopy(after)
            if kind in {"retained-target-shard", "quorum-shard"}:
                entry = next(entry for entry in changed["node1"]["files"] if entry["path"].endswith("/part.1")
                    and target["key"] in entry["path"] and "000000000001" in entry["path"]
                    and (entry["disk"] == target["disk"]) == (kind == "retained-target-shard"))
                entry["sha256"] = "corrupt"
            elif kind == "control": next(entry for entry in changed["node1"]["files"] if "/control/" in entry["path"])["sha256"] = "changed"
            elif kind == "resurrected-metadata":
                entry = next(entry for entry in changed["node1"]["files"] if entry["disk"] == target["disk"] and entry.get("metadata", {}).get("key") == target["key"])
                entry["metadata"] = copy.deepcopy(next(entry for entry in authority["node1"]["files"] if entry["disk"] == target["disk"] and entry.get("metadata", {}).get("key") == target["key"])["metadata"])
            else: changed["node1"]["files"].append({"disk": 0, "path": f"{bucket}/unrelated/new.bin", "size": 1, "sha256": "extra"})
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                stale_rejoin.physical_rejoin(authority, changed, bucket, target["key"], expected, target, witness)

    def test_historical_delete_keeps_original_payload_nonce_and_exact_ack(self):
        _, _, _, bucket, expected_after, target, _ = self.scene()
        _, _, expected = versioned_scene([target["key"]])
        for nonce, entry in enumerate(expected): entry["payloadNonce"] = nonce
        recorder = Mock()
        recorder.call.return_value = {"VersionId": "v0", "ResponseMetadata": {"HTTPStatusCode": 204}}
        manifest, ack = stale_rejoin.transition(recorder, bucket, "delete-historical-version", target["key"], expected)
        self.assertEqual([(entry["versionId"], entry["payloadNonce"]) for entry in manifest], [("v1", 1), ("v2", 2)])
        self.assertEqual(ack["deletedVersionId"], "v0")
        self.assertFalse(ack["backupRestorationAllowed"])
        for changed in ({"VersionId": "v1", "ResponseMetadata": {"HTTPStatusCode": 204}},
                        {"VersionId": "v0", "DeleteMarker": True, "ResponseMetadata": {"HTTPStatusCode": 204}}):
            recorder.call.return_value = changed
            with self.assertRaises(ValueError): stale_rejoin.transition(recorder, bucket, "delete-historical-version", target["key"], expected)

    def test_overwrite_uses_new_nonce_and_marker_preserves_all_acknowledged_versions(self):
        key, bucket = "objects/0001.bin", "rv-heal-0123456789abcdef"
        old = [{"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                "payload": {"size": 100, "sha256": "old"}, "payloadNonce": 0}]
        recorder = Mock()
        recorder.call.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        manifest, ack = stale_rejoin.transition(recorder, bucket, "unversioned-overwrite", key, old)
        self.assertEqual(manifest[0]["payloadNonce"], 1)
        self.assertEqual(manifest[0]["payload"]["sha256"], hashlib.sha256(stale_rejoin.payload(f"{bucket}/{key}/1", 100)).hexdigest())
        self.assertEqual(old[0]["payload"]["sha256"], "old")
        before, _, expected = versioned_scene([key])
        recorder.call.return_value = {"VersionId": "new-marker", "DeleteMarker": True, "ResponseMetadata": {"HTTPStatusCode": 204}}
        manifest, ack = stale_rejoin.transition(recorder, bucket, "delete-marker", key, expected)
        self.assertEqual(len(manifest), 4)
        self.assertTrue(manifest[-1]["isLatest"] and manifest[-1]["deleteMarker"])
        self.assertTrue(all(not entry["isLatest"] for entry in manifest[:-1]))

    def test_stale_receipts_reject_unknown_resurrection_and_false_convergence(self):
        _, _, _, bucket, expected, target, _ = self.scene()
        selected = [entry for entry in expected if entry["key"] == target["key"]]
        state = HealHealthyOracleTest().receipt("repaired")
        template = state["outcome"]["objects"][0]
        receipts = []
        for entry in selected:
            receipt = copy.deepcopy(template)
            receipt["identity"].update(bucket=bucket, object=target["key"], versionId=entry["versionId"])
            receipt["disposition"] = {"state": "repaired" if entry["versionId"] == "v1" else "verified_healthy"}
            receipts.append(receipt)
        absent = copy.deepcopy(template)
        absent["identity"].update(bucket=bucket, object=target["key"], versionId="v0")
        absent["disposition"] = {"state": "authoritatively_absent"}
        state["outcome"]["objects"] = [*receipts, absent]
        state["outcome"]["counters"].update(processed=3, healed=1, unchanged=2)
        self.assertTrue(stale_rejoin.responsibility(state, bucket, target["key"], expected, deleted="v0")["completeRetainedAndOptionalDeletedVersionReceipts"])
        for kind in ("unknown", "live-deleted", "no-repair", "missing-retained", "incarnation", "wrong-count"):
            changed = copy.deepcopy(state)
            if kind == "unknown": changed["outcome"]["objects"][0]["disposition"]["state"] = "unknown"
            elif kind == "live-deleted": changed["outcome"]["objects"][-1]["disposition"]["state"] = "verified_healthy"
            elif kind == "no-repair": changed["outcome"]["objects"][0]["disposition"]["state"] = "verified_healthy"
            elif kind == "missing-retained": changed["outcome"]["objects"].pop(1)
            elif kind == "incarnation": changed["outcome"]["objects"][1]["identity"]["bucketIncarnationId"] = "foreign"
            else: changed["outcome"]["counters"]["healed"] += 1
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                stale_rejoin.responsibility(changed, bucket, target["key"], expected, deleted="v0")

    def test_stale_runtime_and_source_branches_are_explicit(self):
        self.assertEqual(list(stale_rejoin.BRANCHES), select("C06")[0]["variants"])
        config = runtime_config("stale-rejoin")
        self.assertEqual(config["RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS"], "0")
        self.assertEqual(config["RUSTFS_HEAL_MRF_ENABLE"], "false")

    def test_range_oracle_uses_ack_nonce_after_historical_deletion(self):
        from scripts.validation_heal.versions import verify_version_s3
        bucket, key = "rv-heal-0123456789abcdef", "objects/0001.bin"
        expected = [{"key": key, "versionId": f"v{nonce}", "deleteMarker": False, "isLatest": nonce == 2,
                     "payload": {"size": 1024**2 + 47}, "payloadNonce": nonce} for nonce in (1, 2)]
        baseline = {"entries": [{**entry, "properties": {}} for entry in expected]}
        probe, fleet = Mock(), Mock()
        probe.inventory.return_value = baseline
        probe.s3.return_value = {"acknowledged": True, "response": {}}
        fleet.record.side_effect = lambda label, value: value
        with patch("scripts.validation_heal.versions.compare_inventories", return_value={}), \
             patch("scripts.validation_heal.versions.payload", side_effect=lambda label, size: label.encode()) as generator, \
             patch("scripts.validation_heal.versions.verify_ranges", return_value=[]) as ranges:
            verify_version_s3(probe, fleet, bucket, expected, baseline, "after-deletion")
        self.assertEqual([call.args[0] for call in generator.call_args_list], [f"{bucket}/{key}/1", f"{bucket}/{key}/2"])
        self.assertEqual([call.kwargs["expected_body"] for call in ranges.call_args_list],
                         [f"{bucket}/{key}/1".encode(), f"{bucket}/{key}/2".encode()])

    def test_nonce_bound_ack_reaches_inventory_checks_without_weakening_public_manifest(self):
        from scripts.validation_heal.heal_workload import verify_seed, FixtureStateMismatch
        entry = {"key": "objects/0001.bin", "versionId": "v1", "deleteMarker": False, "isLatest": True,
                 "payload": {"size": 100, "sha256": "original-ack"}}
        expected, inventory = [{**entry, "payloadNonce": 1}], {"entries": [entry]}
        self.assertTrue(verify_seed(expected, inventory)["seedAckAndFullGetAgree"])
        for field, value in (("payload", {"size": 100, "sha256": "wrong"}), ("versionId", "v2"),
                             ("deleteMarker", True), ("isLatest", False)):
            with self.subTest(field=field), self.assertRaises(FixtureStateMismatch):
                verify_seed(expected, {"entries": [{**entry, field: value}]})
        for nonce in (-1, True, "1"):
            with self.subTest(nonce=nonce), self.assertRaises(ValueError):
                verify_seed([{**entry, "payloadNonce": nonce}], inventory)


class HealDiskLossTest(unittest.TestCase):
    def scene(self, total=4, data=2):
        original, old = HealReadQuorumBoundaryTest().scene(total=total, data=data)
        bucket, old_key, new_key = HealReadQuorumBoundaryTest.bucket, old[0]["key"], "disk-loss/offline-ack-repair/new-00.bin"
        repaired = copy.deepcopy(original)
        for node, value in repaired.items():
            additions = []
            for entry in value["files"]:
                item = copy.deepcopy(entry)
                item["path"] = item["path"].replace(old_key, new_key)
                if item.get("metadata"):
                    item["metadata"]["key"] = new_key
                    item["metadata"]["versions"][0]["usesLegacyChecksum"] = False
                item["sha256"] += "-new-ack"
                additions.append(item)
            value["files"] += additions
        new = [{**old[0], "key": new_key}]
        target = map_target(original, bucket, old_key, "data", 1)
        target = {**target, **target["cohort"][-1]}
        member = target["node"], target["disk"]
        authority = copy.deepcopy(repaired)
        authority[member[0]]["files"] = [entry for entry in authority[member[0]]["files"]
            if entry["disk"] != member[1] or not entry["path"].startswith(f"{bucket}/{new_key}/")]
        witness = disk_loss.authority_witness(original, authority, bucket, target, new)
        return original, authority, repaired, bucket, target, new, witness

    def admin_info(self, target):
        return {"httpStatus": 200, "response": {"info": {"servers": [{"drives": [
            {"pool_index": 0, "set_index": 0, "disk_index": shard["setSlot"], "uuid": shard["diskUUID"],
             "endpoint": f"http://{shard['node']}:9000/data/{shard['disk']}", "state": "ok", "runtimeState": "online"}
            for shard in target["cohort"]]}]}}}

    def test_actual_single_member_quorum_and_repair_preserve_all_original_bytes(self):
        for total, data in ((4, 2), (16, 12), (16, 8)):
            original, authority, repaired, bucket, target, new, witness = self.scene(total, data)
            plan = disk_loss.quorum_plan(target, HealReadQuorumBoundaryTest.contract)
            self.assertEqual(plan["onlineSameSetDrives"], total - 1)
            self.assertEqual(plan["writeQuorum"], data + int(data == total - data))
            cohorts, proof = disk_loss.repaired_physical(authority, repaired, bucket, target, new, witness)
            self.assertEqual(len(cohorts), 1)
            self.assertTrue(proof["allDegradedACKTargetPartsCreated"])
            self.assertFalse(proof["backupRestorationUsed"])

    def test_offline_ack_authority_rejects_target_writes_loss_of_controls_and_donor_versions(self):
        original, authority, repaired, bucket, target, new, witness = self.scene()
        for kind in ("offline-write", "control", "donor-version", "format", "outside"):
            changed = copy.deepcopy(authority)
            if kind == "offline-write": changed = copy.deepcopy(repaired)
            elif kind == "control": changed["node1"]["files"][0]["sha256"] = "changed-original"
            elif kind == "donor-version":
                next(entry for entry in changed["node1"]["files"] if entry.get("metadata", {}).get("key") == new[0]["key"])["metadata"]["versions"][0]["versionId"] = "foreign"
            elif kind == "format": changed["node1"]["formats"][0]["value"]["xl"]["this"] = "new-disk"
            else: changed["node1"]["files"].append({"disk": 0, "path": "other/key/part.1", "size": 82, "sha256": "outside"})
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                disk_loss.authority_witness(original, changed, bucket, target, new)

    def test_ready_or_get_only_and_changed_surviving_shards_cannot_prove_repair(self):
        original, authority, repaired, bucket, target, new, witness = self.scene()
        for kind in ("no-repair", "donor", "missing-part", "foreign-generation", "extra"):
            changed = copy.deepcopy(repaired)
            if kind == "no-repair": changed = copy.deepcopy(authority)
            elif kind == "donor":
                next(entry for entry in changed["node1"]["files"] if entry["disk"] != target["disk"]
                     and entry["path"].startswith(f"{bucket}/{new[0]['key']}/") and entry["path"].endswith("part.1"))["sha256"] = "changed"
            elif kind == "missing-part": changed["node1"]["files"] = [entry for entry in changed["node1"]["files"]
                if not (entry["disk"] == target["disk"] and entry["path"].startswith(f"{bucket}/{new[0]['key']}/") and entry["path"].endswith("part.1"))]
            elif kind == "foreign-generation":
                next(entry for entry in changed["node1"]["files"] if entry["disk"] == target["disk"]
                     and entry.get("metadata", {}).get("key") == new[0]["key"])["metadata"]["versions"][0]["dataDir"] = "foreign"
            else: changed["node1"]["files"].append({"disk": target["disk"], "path": f"{bucket}/{new[0]['key']}/orphan", "size": 82, "sha256": "extra"})
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                disk_loss.repaired_physical(authority, changed, bucket, target, new, witness)

    def test_offline_admin_requires_exact_slot_endpoint_uuid_and_complete_healthy_peers(self):
        _, _, _, _, target, _, _ = self.scene()
        online = self.admin_info(target)
        baseline = disk_loss.member_state(online, target, 4)
        offline = copy.deepcopy(online)
        selected = offline["response"]["info"]["servers"][0]["drives"][target["setSlot"]]
        selected.update(state="disk access denied", runtimeState="offline")
        self.assertTrue(disk_loss.member_state(offline, target, 4, baseline)["exactOriginalMemberOffline"])
        for kind in ("wrong-slot", "unknown", "new-uuid", "new-endpoint", "peer-offline", "omitted", "duplicate"):
            changed = copy.deepcopy(offline)
            drives = changed["response"]["info"]["servers"][0]["drives"]
            if kind == "wrong-slot": drives[target["setSlot"]]["disk_index"] = 0
            elif kind == "unknown": drives[target["setSlot"]].update(state="unknown", runtimeState="unknown")
            elif kind == "new-uuid": drives[target["setSlot"]]["uuid"] = "another-disk"
            elif kind == "new-endpoint": drives[target["setSlot"]]["endpoint"] = "http://another-node:9000/data"
            elif kind == "peer-offline": drives[0].update(state="offline", runtimeState="offline")
            elif kind == "omitted": drives.pop()
            else: drives.append(copy.deepcopy(drives[0]))
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                disk_loss.member_state(changed, target, 4, baseline)

    def test_degraded_unique_puts_keep_each_ack_and_branches_match_source(self):
        self.assertEqual(list(disk_loss.BRANCHES), select("B01")[0]["variants"])
        bucket = HealReadQuorumBoundaryTest.bucket
        observed = []
        def invoke(operation, params, body):
            observed.append((params["Key"], body))
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}
        def get(operation, params):
            body = next(body for key, body in observed if key == params["Key"])
            return {"acknowledged": True, "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}}
        recorder = types.SimpleNamespace(call=invoke, probe=types.SimpleNamespace(s3=get))
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        expected = []
        for branch, count in zip(disk_loss.BRANCHES, (1, 6)):
            expected, new = disk_loss.offline_ack(recorder, fleet, bucket, branch, expected)
            self.assertEqual(len(new), count)
        self.assertEqual(len(expected), 7)
        with self.assertRaisesRegex(ValueError, "already has"):
            disk_loss.offline_ack(recorder, fleet, bucket, disk_loss.BRANCHES[0], expected)


class HealWholeDriveReplacementTest(unittest.TestCase):
    def test_partial_target_window_requires_actual_committed_shards_and_pending_versions(self):
        scene, bucket, expected = versioned_scene(["first", "second"])
        proofs = [map_target(scene, bucket, key, "data", 1, "v0", latest_required=False) for key in ("first", "second")]
        partial = copy.deepcopy(scene)
        missing = proofs[1]
        partial[missing["node"]]["files"] = [item for item in partial[missing["node"]]["files"]
            if not (item["disk"] == missing["disk"] and item["path"] == missing["relativePath"])]
        progress = replacement.target_progress(partial, proofs)
        self.assertTrue(progress["partial"])
        self.assertEqual(progress["completeDataVersions"], [("first", "v0")])
        self.assertFalse(replacement.target_progress(scene, proofs)["partial"])
        for kind in ("bytes", "version-authority", "role"):
            changed = copy.deepcopy(partial)
            target = proofs[0]
            if kind == "bytes":
                next(item for item in changed[target["node"]]["files"] if item["disk"] == target["disk"]
                     and item["path"] == target["relativePath"])["sha256"] = "wrong"
            else:
                version = next(item for item in changed[target["node"]]["files"] if item["disk"] == target["disk"]
                    and item["path"] == f"{bucket}/first/xl.meta")["metadata"]["versions"][0]
                if kind == "version-authority": version["dataDir"] = "foreign-generation"
                else: version["role"] = "wrong"
            with self.subTest(kind=kind), self.assertRaises(ValueError): replacement.target_progress(changed, proofs)

    def test_target_restart_keeps_other_peers_and_rechecks_private_namespace(self):
        before = {f"node{index}": HealRootRestartOracleTest().process(10 + index, 50 + index) for index in range(1, 5)}
        after = copy.deepcopy(before)
        after["node4"].update(pid=29, startTicks=99, mountNamespace="mnt:[new-private]")
        self.assertTrue(replacement.target_restart_identity(before, after, "node4")["onlyOriginalTargetProcessRestarted"])
        for node, field in (("node1", "pid"), ("node4", "binarySha256"), ("node4", "bootId"), ("node4", "settings")):
            changed = copy.deepcopy(after); changed[node][field] = "changed"
            with self.subTest(node=node, field=field), self.assertRaises(ValueError): replacement.target_restart_identity(before, changed, "node4")

    def test_target_crash_requires_exact_signal_nine_and_original_birth_identity(self):
        before = HealRootRestartOracleTest().process()
        exited = {"pid": 0, "bootId": "boot", "binarySha256": "sha", "environmentFileSha256": "env",
                  "service": {"ExecMainCode": "2", "ExecMainStatus": "9", "Result": "signal"}}
        for kind in ("good", "graceful", "other-signal", "timeout", "boot", "candidate", "environment"):
            current = copy.deepcopy(exited)
            if kind == "graceful": current["service"]["ExecMainCode"] = "1"
            elif kind == "other-signal": current["service"]["ExecMainStatus"] = "15"
            elif kind == "timeout": current["service"]["Result"] = "timeout"
            elif kind == "boot": current["bootId"] = "new-boot"
            elif kind == "candidate": current["binarySha256"] = "new-build"
            elif kind == "environment": current["environmentFileSha256"] = "new-env"
            with self.subTest(kind=kind), patch.object(fleet_node, "owned", return_value=(None, None, {"runtimeProfile": "replacement"})), \
                 patch.object(fleet_node, "process", side_effect=[before, current]), patch.object(fleet_node, "pidfd_signal", return_value=before) as send:
                if kind == "good":
                    proof = fleet_node.process_crash({"runId": "run", "expected": before})
                    self.assertEqual(proof["actualSignal"], 9)
                    self.assertFalse(proof["platformEventIssued"])
                    self.assertEqual(send.call_args.args[1], fleet_node.signal.SIGKILL)
                else:
                    with self.assertRaises(ValueError): fleet_node.process_crash({"runId": "run", "expected": before})
        with patch.object(fleet_node, "owned", return_value=(None, None, {"runtimeProfile": "manual"})), \
             patch.object(fleet_node, "pidfd_signal") as send, self.assertRaises(ValueError):
            fleet_node.process_crash({"runId": "run", "expected": before})
        send.assert_not_called()

    def test_fast_completed_replacement_cannot_certify_an_unobserved_interruption(self):
        endpoint = "/private/new/data"
        row = {"taskId": "12345678-1234-4234-8234-123456789abc", "generation": "12345678-1234-4234-8234-123456789abc",
               "setDiskId": "pool_0_set_0", "targetSlots": [endpoint], "state": "completed", "reason": None, "verifiedAt": 101}
        with patch("scripts.validation_heal.replacement.replacement_records", return_value={"node1": [row]}), \
             patch("scripts.validation_heal.replacement.time.time", return_value=102), self.assertRaisesRegex(ValueError, "NOT_OBSERVED"):
            replacement.automatic_completion(None, None, endpoint, 100, interruption="sigkill")
        self.assertEqual(list(replacement.GRACEFUL_BRANCHES), select("F02")[0]["variants"])
        self.assertEqual(list(replacement.CRASH_BRANCHES), select("F03")[0]["variants"])

    def test_target_interruption_needs_fresh_source_and_a_distinct_actual_coordinator(self):
        token = "12345678-1234-4234-8234-123456789abc"
        rows = [{"timestamp": "2026-10-10T10:00:01Z", "target": "rustfs::heal::manager",
                 "request_id": token, "source": "auto_heal", "result": "accepted", "message": "Heal queue request accepted"},
                {"timestamp": "2026-10-10T10:00:02Z", "target": "rustfs::heal::task", "task_id": token,
                 "set_disk_id": "pool_0_set_0", "stage": "start", "message": "Heal erasure set started"}]
        self.assertEqual(replacement.coordinator({"node1": rows, "node4": []}, token, "node4"), "node1")
        for kind in ("same-target", "duplicate", "source", "task", "order", "missing"):
            events = {"node1": copy.deepcopy(rows), "node4": []}
            if kind == "same-target": events = {"node4": rows}
            elif kind == "duplicate": events["node2"] = rows
            elif kind == "source": events["node1"][0]["source"] = "admin"
            elif kind == "task": events["node1"][1]["task_id"] = "other-task"
            elif kind == "order": events["node1"][1]["timestamp"] = "2026-10-10T10:00:00Z"
            else: events["node1"].pop()
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "NOT_OBSERVED"):
                replacement.coordinator(events, token, "node4")
        config = runtime_config("replacement-target")
        self.assertEqual(config["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertEqual(config["RUSTFS_HEAL_ENABLED"], "true")

    def test_live_permission_rejoin_restores_same_inode_and_requires_same_process(self):
        for changed_process in (False, True):
            with self.subTest(changed_process=changed_process), tempfile.TemporaryDirectory() as folder:
                state, root = self.fixture(folder)
                identity = HealRootRestartOracleTest().process()
                after = {**identity, "startTicks": identity["startTicks"] + int(changed_process)}
                with self.root_owned():
                    apply_gate(state, (root,), 0, "permission-denied")
                    activate_gate(state, (root,))
                    original_inode = (root / "data").stat().st_ino
                    with patch.object(fleet_node, "owned", return_value=(state, (root,), {})), \
                         patch.object(fleet_node, "process", side_effect=[identity, after]), \
                         patch.object(fleet_node, "stopped") as stopped_service:
                        if changed_process:
                            with self.assertRaisesRegex(ValueError, "process identity"):
                                fleet_node.drive_gate_release_live({"runId": "owned-run"})
                        else:
                            proof = fleet_node.drive_gate_release_live({"runId": "owned-run"})
                            self.assertTrue(proof["sameOriginalProcess"])
                            self.assertFalse(proof["serviceRestarted"])
                    stopped_service.assert_not_called()
                    self.assertEqual((root / "data").stat().st_ino, original_inode)
                    self.assertEqual(stat.S_IMODE((root / "data").stat().st_mode), 0o700)
                    self.assertIsNone(checked_gate(state, (root,)))

    def test_live_rejoin_rejects_unactivated_gate_without_mutating_root(self):
        with tempfile.TemporaryDirectory() as folder:
            state, root = self.fixture(folder)
            with self.root_owned():
                apply_gate(state, (root,), 0, "permission-denied")
                with patch.object(fleet_node, "owned", return_value=(state, (root,), {})), \
                     patch.object(fleet_node, "process") as process, self.assertRaisesRegex(ValueError, "active permission"):
                    fleet_node.drive_gate_release_live({"runId": "owned-run"})
                process.assert_not_called()
                self.assertEqual(checked_gate(state, (root,))["phase"], "armed")
                self.assertEqual(stat.S_IMODE((root / "data").stat().st_mode), 0o700)

    def replaced_scene(self):
        before = HealShardOracleTest().snapshot()
        member = "node1", 3
        after = copy.deepcopy(before)
        for entry in after["node1"]["formats"]:
            entry["value"]["xl"]["sets"][0][3] = "new-member"
            if entry["disk"] == 3: entry["value"]["xl"]["this"] = "new-member"
        return before, after, member

    def test_new_disk_requires_same_pool_set_slot_and_distinct_uuid(self):
        before, after, member = self.replaced_scene()
        self.assertEqual(replacement.format_identity(before, after, member)["newDiskUUID"], "new-member")
        for kind in ("same-uuid", "new-cluster", "wrong-slot", "changed-survivor"):
            changed = copy.deepcopy(after)
            if kind == "same-uuid": changed = copy.deepcopy(before)
            elif kind == "new-cluster":
                for entry in changed["node1"]["formats"]: entry["value"]["id"] = "another-cluster"
            elif kind == "wrong-slot": changed["node1"]["formats"][3]["value"]["xl"]["this"] = "0"
            else: changed["node1"]["formats"][0]["value"]["version"] = "future"
            with self.subTest(kind=kind), self.assertRaises(ValueError): replacement.format_identity(before, changed, member)

    def test_rebuild_requires_all_same_role_original_parts_and_unchanged_survivors(self):
        before, after, member = self.replaced_scene()
        self.assertFalse(replacement.physical_rebuild(before, after, member)["backupRestorationUsed"])
        for kind in ("missing-target", "bad-target", "donor", "version", "orphan"):
            changed = copy.deepcopy(after)
            target = next(entry for entry in changed["node1"]["files"] if entry["disk"] == member[1]
                          and entry["path"].endswith("part.1"))
            if kind == "missing-target": changed["node1"]["files"].remove(target)
            elif kind == "bad-target": target["sha256"] = "wrong-shard"
            elif kind == "donor": changed["node1"]["files"][0]["sha256"] = "changed-donor"
            elif kind == "version":
                next(entry for entry in changed["node1"]["files"] if entry["disk"] == member[1]
                     and entry.get("metadata"))["metadata"]["versions"][0]["versionId"] = "foreign-version"
            else: changed["node1"]["files"].append({"disk": member[1], "path": "run/key/orphan", "size": 82, "sha256": "orphan"})
            with self.subTest(kind=kind), self.assertRaises(ValueError): replacement.physical_rebuild(before, changed, member)

    def fixture(self, folder):
        state, root = Path(folder) / "state", Path(folder) / "root"
        state.mkdir(mode=0o700)
        (root / "data/.rustfs.sys").mkdir(parents=True, mode=0o700)
        (root / "data").chmod(0o700)
        (root / "quarantine").mkdir(mode=0o700)
        (root / "data/.rustfs.sys/format.json").write_bytes(b'{"original":"format"}')
        (root / "data/ack-part").write_bytes(b"original-ack-shard")
        return state, root

    def root_owned(self):
        original = Path.lstat
        def observed(path):
            value = original(path)
            return types.SimpleNamespace(st_mode=value.st_mode, st_uid=0, st_nlink=value.st_nlink,
                st_size=value.st_size, st_dev=value.st_dev, st_ino=value.st_ino)
        return patch.object(Path, "lstat", observed)

    def test_replacement_keeps_whole_original_and_rejects_drift_or_second_begin(self):
        with tempfile.TemporaryDirectory() as folder:
            state, root = self.fixture(folder)
            with self.root_owned():
                before = replace_drive.tree_identity(root / "data")
                proof = replace_drive.begin(state, (root,), 0, replace_drive.digest(root / "data/.rustfs.sys/format.json"))
                self.assertTrue(proof["newDataRootEmpty"])
                self.assertEqual(replace_drive.tree_identity(root / "quarantine/old-disk"), before)
                self.assertEqual((root / "quarantine/old-disk/ack-part").read_bytes(), b"original-ack-shard")
                self.assertTrue(replace_drive.attest(state, (root,))["originalArchiveUnchanged"])
                self.assertIn("--property=InaccessiblePaths=" + str(root / "quarantine"), replace_drive.start_properties(state, (root,)))
                with self.assertRaises(ValueError): replace_drive.begin(state, (root,), 0, proof["originalFormatSha256"])
                (root / "quarantine/old-disk/ack-part").write_bytes(b"changed-old-backup")
                with self.assertRaisesRegex(ValueError, "drifted"): replace_drive.attest(state, (root,))

    def test_partial_begin_retains_recovery_intent_and_never_copies_original_back(self):
        for boundary in ("rename", "mkdir"):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as folder:
                state, root = self.fixture(folder)
                with self.root_owned():
                    digest = replace_drive.digest(root / "data/.rustfs.sys/format.json")
                    if boundary == "rename": failure = patch.object(replace_drive.os, "rename", side_effect=OSError("interrupted rename"))
                    else: failure = patch.object(Path, "mkdir", side_effect=OSError("interrupted mkdir"))
                    with failure, self.assertRaises(OSError): replace_drive.begin(state, (root,), 0, digest)
                    proof = replace_drive.attest(state, (root,))
                    self.assertTrue(proof["originalArchiveUnchanged"])
                    self.assertEqual(proof["originalStillAtDataRoot"], boundary == "rename")
                    original = root / ("data" if boundary == "rename" else "quarantine/old-disk")
                    self.assertEqual((original / "ack-part").read_bytes(), b"original-ack-shard")

    def test_archive_rejects_links_and_wrong_format_before_mutation(self):
        for kind in ("symlink", "hardlink", "format"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as folder:
                state, root = self.fixture(folder)
                if kind == "symlink": (root / "data/link").symlink_to(root / "data/ack-part")
                elif kind == "hardlink": __import__("os").link(root / "data/ack-part", root / "data/another")
                with self.root_owned(), self.assertRaises(ValueError): replace_drive.begin(state, (root,), 0, "0" * 64)
                self.assertFalse((state / "disk-replacement.json").exists())
                self.assertFalse((root / "quarantine/old-disk").exists())
                self.assertEqual((root / "data/ack-part").read_bytes(), b"original-ack-shard")

    def test_replacement_format_wait_never_substitutes_service_ready_for_member_readiness(self):
        absent = {"readOnly": True, "originalArchiveUnchanged": True,
                  "formats": [{"disk": index, "present": index != 3} for index in range(4)]}
        present = copy.deepcopy(absent); present["formats"][3]["present"] = True
        fleet = types.SimpleNamespace(record=lambda label, value: value, remote=lambda *args: absent)
        with patch("scripts.validation_heal.replacement.time.monotonic", side_effect=[0, 91]), self.assertRaisesRegex(ValueError, "90-second"):
            replacement.wait_format(fleet, "node1", 3)
        rows = [absent, present]
        fleet.remote = lambda *args: rows.pop(0)
        with patch("scripts.validation_heal.replacement.time.sleep"), patch("scripts.validation_heal.replacement.time.monotonic", return_value=0):
            self.assertTrue(replacement.wait_format(fleet, "node1", 3)["formats"][3]["present"])
        fleet.remote = lambda *args: {**present, "originalArchiveUnchanged": False}
        with self.assertRaisesRegex(ValueError, "archive witness"): replacement.wait_format(fleet, "node1", 3)

    def test_automatic_replacement_generation_rejects_unknown_wrong_target_and_stale_completion(self):
        endpoint = "http://rustfs-node4:9000/private/new/data"
        row = {"taskId": "12345678-1234-4234-8234-123456789abc", "generation": "12345678-1234-4234-8234-123456789abc",
               "setDiskId": "pool_0_set_0", "targetSlots": [endpoint], "state": "completed", "reason": None, "verifiedAt": 101}
        with patch("scripts.validation_heal.replacement.time.time", return_value=102):
            binding = replacement.generation([row], endpoint, 100, completed=True)
            for kind in ("state", "reason", "stale", "target", "set", "generation", "task", "missing", "duplicate"):
                with self.subTest(kind=kind):
                    bad = copy.deepcopy(row)
                    rows = [bad]
                    if kind == "state": bad["state"] = "unknown"
                    elif kind == "reason": bad["reason"] = "incomplete"
                    elif kind == "stale": bad["verifiedAt"] = 99
                    elif kind == "target": bad["targetSlots"] = [endpoint + "-different"]
                    elif kind == "set": bad["setDiskId"] = "pool_1_set_0"
                    elif kind == "generation": bad["generation"] = "other"
                    elif kind == "task": bad["taskId"] = bad["generation"] = "ffffffff-ffff-4fff-8fff-ffffffffffff"
                    elif kind == "missing": rows = []
                    else: rows *= 2
                    with self.assertRaises(ValueError): replacement.generation(rows, endpoint, 100, original=binding, completed=True)
            running = {**row, "state": "running", "verifiedAt": None}
            replacement.generation([running], endpoint, 100, original=binding)
            with self.assertRaisesRegex(ValueError, "not completed"): replacement.generation([running], endpoint, 100, completed=True)

    def test_automatic_replacement_requires_every_peer_and_stable_completed_generation(self):
        endpoint = "/private/new/data"
        row = {"taskId": "12345678-1234-4234-8234-123456789abc", "generation": "12345678-1234-4234-8234-123456789abc",
               "setDiskId": "pool_0_set_0", "targetSlots": [endpoint], "state": "completed", "reason": None, "verifiedAt": 101}
        complete = {"node1": [row], "node2": [copy.deepcopy(row)]}
        with patch("scripts.validation_heal.replacement.replacement_records", side_effect=[{"node1": [row], "node2": []}, complete, complete, complete]), \
             patch("scripts.validation_heal.replacement.time.sleep"), \
             patch("scripts.validation_heal.replacement.time.monotonic", return_value=0), \
             patch("scripts.validation_heal.replacement.time.time", return_value=102):
            proof = replacement.automatic_completion(None, None, endpoint, 100)
            self.assertTrue(proof["completionObservedOnEveryPeer"])
            self.assertEqual(proof["adminHealMutations"], 0)
            self.assertEqual(proof["observations"], 2)
        changed = copy.deepcopy(complete); changed["node2"][0]["verifiedAt"] = 102
        with patch("scripts.validation_heal.replacement.replacement_records", return_value=changed), \
             patch("scripts.validation_heal.replacement.time.time", return_value=102), self.assertRaisesRegex(ValueError, "differs across peers"):
            replacement.automatic_completion(None, None, endpoint, 100)

    def test_automatic_replacement_profile_enables_only_its_registered_source(self):
        config = runtime_config("replacement")
        self.assertEqual(config["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "true")
        self.assertEqual(config["RUSTFS_HEAL_MRF_ENABLE"], "false")
        self.assertEqual(config["RUSTFS_SCANNER_ENABLED"], "false")
        self.assertEqual(config["RUSTFS_HEAL_INTERVAL_SECS"], "10")
        self.assertEqual(list(replacement.AUTOMATIC_BRANCHES), select("E06")[0]["variants"])

    def test_remote_replacement_module_initializes_and_all_source_branches_remain(self):
        import sys
        self.assertEqual(list(replacement.BRANCHES), select("B03")[0]["variants"])
        prior = sys.modules.get("drive_gate")
        observed = []
        def invoke(args, **kwargs):
            source = kwargs["input"].rsplit("\nmain(", 1)[0]
            scope = {"__name__": "private-node-script"}
            exec(source, scope)
            self.assertEqual(scope["replacement_module"]().DENY_DAC_CAPS, (1 << 1) | (1 << 2))
            self.assertTrue(callable(scope["replacement_module"]().begin))
            observed.append(source)
            return types.SimpleNamespace(returncode=0, stdout='{"compiled":true}', stderr="")
        try:
            with tempfile.TemporaryDirectory() as folder, patch("scripts.validation_heal.healthy.subprocess.run", side_effect=invoke):
                fleet = Fleet(["node1"], Path(folder), "gh-12345-1-h1")
                for action in ("start", "cleanup", "replace-drive", "replacement-state", "replacement-probe"):
                    self.assertTrue(fleet.remote("node1", action)["compiled"])
        finally:
            if prior is None: sys.modules.pop("drive_gate", None)
            else: sys.modules["drive_gate"] = prior
        self.assertEqual(len(observed), 5)


if __name__ == "__main__":
    unittest.main()
