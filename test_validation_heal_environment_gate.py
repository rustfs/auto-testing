import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal.environment_gate import decision, write_blocked
from scripts.validation_heal.execution_policy import (
    CAPABILITIES_ENV,
    ENABLE_ENV,
    ENVIRONMENT_GATED_CASES,
    EVIDENCE_BLOCKED_CASES,
)
from scripts.validation_heal.matrix import AUTOMATIC_CASES, DISPATCHABLE_CASES, cases
from scripts.validation_report import render_report, validate_results
from scripts.validation_heal import environment_cases as extended
from scripts.validation_heal.matrix import select, variant_topologies


class EnvironmentGateTest(unittest.TestCase):
    def test_first_and_second_batches_are_disjoint_and_classified(self):
        self.assertEqual(len(ENVIRONMENT_GATED_CASES), 13)
        self.assertEqual(len(EVIDENCE_BLOCKED_CASES), 4)
        self.assertTrue(set(ENVIRONMENT_GATED_CASES).isdisjoint(EVIDENCE_BLOCKED_CASES))
        self.assertTrue(set(ENVIRONMENT_GATED_CASES) <= DISPATCHABLE_CASES)
        self.assertFalse(set(EVIDENCE_BLOCKED_CASES) & DISPATCHABLE_CASES)
        self.assertEqual(len(AUTOMATIC_CASES), 67)

    def test_recipes_carry_policy_without_changing_source_case_count(self):
        inventory = {item["id"]: item for item in cases()}
        for case_id, policy in {**ENVIRONMENT_GATED_CASES, **EVIDENCE_BLOCKED_CASES}.items():
            self.assertEqual(inventory[case_id]["executionPolicy"], policy)
        self.assertEqual(sum(item.get("executionPolicy", {}).get("kind") == "environment-gated" for item in inventory.values()), 13)
        self.assertEqual(sum(item.get("executionPolicy", {}).get("kind") == "evidence-blocked" for item in inventory.values()), 4)

    def test_default_gate_is_blocked_without_touching_fleet(self):
        case_id = "heal.mixed-version-upgrade-rollback"
        result = decision(case_id, environ={})
        self.assertEqual(result["state"], "disabled")
        self.assertEqual(result["reasonCode"], "ENV_DISABLED_BY_DEFAULT")
        self.assertFalse(result["enabled"])
        self.assertEqual(set(result["missingCapabilities"]), set(ENVIRONMENT_GATED_CASES[case_id]["requiredCapabilities"]))

    def test_explicit_gate_requires_every_capability(self):
        case_id = "heal.ec84-multi-set-restart"
        result = decision(case_id, environ={ENABLE_ENV: "1"}, capabilities=[])
        self.assertEqual(result["state"], "blocked")
        self.assertEqual(result["reasonCode"], "ENV_CAPABILITY_MISSING")
        self.assertTrue(result["missingCapabilities"])

    def test_capability_manifest_alone_needs_bound_runtime(self):
        case_id = "heal.kms-and-tier-outage"
        policy = ENVIRONMENT_GATED_CASES[case_id]
        result = decision(case_id, environ={ENABLE_ENV: "1"}, capabilities=policy["requiredCapabilities"])
        self.assertEqual(result["state"], "ready")
        self.assertEqual(result["reasonCode"], "ENV_RUNTIME_REQUIRED")

    def test_blocked_result_is_reportable_with_reason_and_evidence(self):
        case_id = "heal.race-ilm-expiry-and-transition"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"package_url": "https://example.invalid/rustfs.deb", "package_sha256": "a" * 64}))
            artifacts = root / "artifacts"
            result = write_blocked(case_id, artifacts, candidate=candidate, testing_commit="b" * 40, environ={})
            self.assertEqual(result["verdict"], "BLOCKED")
            self.assertEqual(result["execution_policy"]["kind"], "environment-gated")
            self.assertFalse(json.loads((artifacts / "evidence/environment-gate.json").read_text())["fleetTouched"])
            catalog = json.loads(Path("validation-cases.json").read_text())
            rows, errors = validate_results(catalog, artifacts / "cases", gate_statuses=set())
            self.assertFalse(errors, errors)
            self.assertEqual(next(row for row in rows if row["case_id"] == case_id)["verdict"], "BLOCKED")

    def test_dispatchers_include_all_environment_gated_cases(self):
        for path in (Path("scripts/run_validation_case.sh"), Path("scripts/validation_heal/run_case.sh")):
            text = path.read_text()
            for case_id in ENVIRONMENT_GATED_CASES:
                self.assertIn(case_id, text)


class ExtendedScenariosTest(unittest.TestCase):
    def binding(self):
        return {"schemaVersion": 1, "runId": "manual-environment-test", "jumpHostOnly": True,
                "exclusiveHealService": True, "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(days=10)).isoformat(),
                "bucketPrefix": "rv-heal-environment-test", "candidateCommit": "a" * 40, "harnessCommit": "b" * 40,
                "candidatePackageSha256": "c" * 64, "candidateBinarySha256": "d" * 64, "rcBinarySha256": "e" * 64,
                "endpoints": {"node1": "http://localhost:19000"}, "defaultEndpoint": "node1",
                "maxInventoryObjects": 10000, "maxInventoryBytes": 8 * 1024**3}

    def topology(self, code="H1"):
        count = 24 if code == "H4" else 12 if code == "H3" else 4
        disks = [{"hostname": "node1", "mountDevice": str(index), "diskUUID": "disk-" + str(index),
                  "poolIndex": 0, "setIndex": index // 12 if code in {"H3", "H4"} else 0,
                  "setSlot": index % 12 if code in {"H3", "H4"} else index,
                  "dataShards": 8 if code in {"H3", "H4"} else 2,
                  "parityShards": 4 if code in {"H3", "H4"} else 2} for index in range(count)]
        return {"disks": disks, "processes": [{"hostname": "node1", "binarySha256": "d" * 64, "pid": 1234}]}

    def test_every_source_branch_has_executable_control_flow_or_explicit_native_block(self):
        templates = []
        for case_id, policy in ENVIRONMENT_GATED_CASES.items():
            recipe = select(case_id)[0]
            operations = extended.required_operations(case_id)
            self.assertTrue({"preflight", "prepare", "recover", "cleanup"} <= operations)
            mapping = variant_topologies(recipe)
            for branch in recipe["variants"]:
                templates.extend((case_id, topology, branch) for topology in mapping[branch])
                if branch in extended.NATIVE_BRANCHES: continue
                spec = extended.scenario(policy["code"], branch)
                self.assertTrue(set(spec.actions) <= operations)
                if policy["code"] != "J05": self.assertTrue(spec.actions)
        self.assertEqual(len(templates), len(set(templates)))
        h7 = [row for row in templates if row[0] == "heal.ec84-multi-set-restart"]
        self.assertEqual(len(h7), 4)

    def test_default_dispatch_all_thirteen_never_invokes_ssh_or_extended_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            forbidden = root / "ssh-called"
            ssh = root / "ssh"
            ssh.write_text("#!/bin/sh\ntouch '" + str(forbidden) + "'\nexit 99\n")
            ssh.chmod(0o755)
            candidate = root / "candidate.json"; candidate.write_text("{}")
            environ = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                       ENABLE_ENV: "0", CAPABILITIES_ENV: str(root / "invalid-capabilities"),
                       extended.RUNTIME_ENV: "/must-not-read-runtime"}
            for case_id in ENVIRONMENT_GATED_CASES:
                with self.subTest(case=case_id):
                    artifacts = root / case_id
                    command = ["bash", "scripts/validation_heal/run_case.sh", "--validation-case", case_id,
                               "--candidate-manifest", str(candidate), "--testing-commit", "b" * 40,
                               "--validation-artifacts-dir", str(artifacts)]
                    result = subprocess.run(command, env=environ, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    value = json.loads((artifacts / "cases" / (case_id + ".json")).read_text())
                    self.assertEqual(value["verdict"], "BLOCKED")
                    self.assertEqual(value["migration_status"], "unverified")
                    self.assertIn("ENV_DISABLED_BY_DEFAULT", value["reason"])
            self.assertFalse(forbidden.exists())

    def test_selected_blocked_case_fails_canonical_gate_and_report_shows_reason(self):
        case_id = "heal.slow-io-and-eio"
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            write_blocked(case_id, artifacts, candidate=None, testing_commit=None, environ={})
            catalog = json.loads(Path("validation-cases.json").read_text())
            rows, errors = validate_results(catalog, artifacts / "cases", gate_statuses=set(), gate_cases={case_id})
            self.assertEqual(errors, [case_id + ": gate verdict is 'BLOCKED'"])
            self.assertIn("ENV_DISABLED_BY_DEFAULT", render_report(rows, errors))
            self.assertIn("专用 faultfs", render_report(rows, errors))

    def test_runtime_requires_all_topologies_candidate_and_adapter_digest_before_commands(self):
        case_id = "heal.slow-io-and-eio"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); rc = root / "rc"; rc.write_text("fixture rc")
            adapter = root / "adapter"; adapter.write_text("#!/bin/sh\nexit 99\n"); adapter.chmod(0o755)
            binding = self.binding(); binding["rcBinarySha256"] = hashlib.sha256(rc.read_bytes()).hexdigest()
            runtime = {"schemaVersion": 1, "provider": "auto-testing", "binding": binding, "rcBinary": str(rc),
                       "sshNodes": os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split(),
                       "topologies": ["H1", "H2"], "inspectorSha256": "f" * 64, "faultDeviceId": "device", "capabilities": ENVIRONMENT_GATED_CASES[case_id]["requiredCapabilities"],
                       "adapter": {"executable": str(adapter), "sha256": hashlib.sha256(adapter.read_bytes()).hexdigest(),
                                   "operations": sorted(extended.required_operations(case_id))}}
            candidate = {"source_sha": "a" * 40, "package_sha256": "c" * 64}
            self.assertEqual(extended.runtime_binding(runtime, case_id, candidate, "b" * 40), binding)
            for kind in ("candidate", "topology", "adapter", "capability", "provider", "nodes"):
                changed = copy.deepcopy(runtime)
                if kind == "candidate": changed["binding"]["candidateCommit"] = "f" * 40
                elif kind == "topology": changed["topologies"] = ["H1"]
                elif kind == "adapter": changed["adapter"]["sha256"] = "f" * 64
                elif kind == "capability": changed["capabilities"] = []
                elif kind == "provider": changed["provider"] = "source-validation-lab"
                else: changed["sshNodes"] = ["unrelated-node"]
                with self.subTest(kind=kind), self.assertRaises(ValueError):
                    extended.runtime_binding(changed, case_id, candidate, "b" * 40)

    def test_physical_topology_rejects_directory_aliases_missing_sets_and_wrong_candidate(self):
        extended.assert_topology(self.topology("H4"), "H4", "d" * 64)
        for kind in ("alias", "slot", "count", "coding", "candidate"):
            observed = self.topology("H4")
            if kind == "alias": observed["disks"][1]["mountDevice"] = "0"
            elif kind == "slot": observed["disks"][1]["setSlot"] = 0
            elif kind == "count": observed["disks"].pop()
            elif kind == "coding": observed["disks"][0]["dataShards"] = 2
            else: observed["processes"][0]["binarySha256"] = "f" * 64
            with self.subTest(kind=kind), self.assertRaises(ValueError): extended.assert_topology(observed, "H4", "d" * 64)

    def test_overlap_requires_actual_original_task_and_common_activity_intersection(self):
        value = {"clockDomain": "server-1", "intervals": [
            {"kind": "heal", "taskId": "original", "startNs": 100, "endNs": 200, "evidenceId": "h"},
            {"kind": "pool", "taskId": "move", "startNs": 150, "endNs": 300, "evidenceId": "p"}]}
        extended.assert_overlap(value, task_id="original", kind="pool")
        value["intervals"][1]["startNs"] = 201
        with self.assertRaises(extended.NotObserved): extended.assert_overlap(value, task_id="original", kind="pool")
        value["clockDomain"] = ""
        with self.assertRaises(ValueError): extended.assert_overlap(value, task_id="original", kind="pool")

    def terminal(self):
        expected = [{"key": "key", "versionId": "v1", "deleteMarker": False}]
        status = {"summary": "finished", "outcome": {"coverage": "complete", "objectsTruncated": False,
            "execution": {"state": "completed"}, "counters": {"processed": 1, "healed": 1, "unchanged": 0, "skipped": 0,
                "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}, "objects": [
            {"identity": {"bucket": "bucket", "object": "key", "versionId": "v1", "bucketIncarnationId": "epoch", "poolIndex": 0, "setIndex": 0},
             "disposition": {"state": "repaired"}}]}}
        return status, expected

    def test_terminal_denominator_cannot_be_defined_by_returned_receipts_or_legacy_finish(self):
        status, expected = self.terminal(); extended.assert_terminal(status, "bucket", expected)
        for kind in ("missing", "wrong-version", "unknown", "metadata-only", "counter", "incarnation", "boolean-set"):
            changed = copy.deepcopy(status); row = changed["outcome"]["objects"][0]
            if kind == "missing": changed["outcome"]["objects"] = []
            elif kind == "wrong-version": row["identity"]["versionId"] = "foreign"
            elif kind == "unknown": row["disposition"]["state"] = "unknown"
            elif kind == "metadata-only": row["disposition"]["state"] = "metadata_healthy"
            elif kind == "counter": changed["outcome"]["counters"]["healed"] = 0
            elif kind == "incarnation": row["identity"]["bucketIncarnationId"] = ""
            else: row["identity"]["setIndex"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError): extended.assert_terminal(changed, "bucket", expected)

    def test_enabled_io_branch_runs_all_oracles_then_requires_review(self):
        from test_validation_heal import HealShardOracleTest
        original = HealShardOracleTest().snapshot()
        bucket = "rv-heal-0123456789abcdef"
        binding = self.binding(); binding["bucketPrefix"] = "rv-heal-0123456789"
        def remap(bucket_name):
            scene = copy.deepcopy(original)
            for row in scene["node1"]["files"]:
                row["path"] = row["path"].replace(bucket, bucket_name)
                if "metadata" in row: row["metadata"]["bucket"] = bucket_name
            return scene
        class FakeAdapter:
            def __init__(self): self.calls = []; self.injected = False; self.repaired = False; self.scene = None
            def record(self, label, value): return value
            def call(self, operation, **params):
                self.calls.append(operation)
                name = params.get("bucket")
                if operation == "preflight": return ExtendedScenariosTest().topology()
                if operation == "snapshot":
                    if self.scene is None: self.scene = remap(name)
                    scene = copy.deepcopy(self.scene)
                    if self.injected and not self.repaired:
                        scene["node1"]["files"] = [r for r in scene["node1"]["files"] if not (r["disk"] == self.target["disk"] and r["path"] == self.target["relativePath"])]
                    return {"snapshot": scene}
                if operation == "apply-fault":
                    self.target = params["target"]; self.injected = True
                    return {"kind": "missing", "relativePath": self.target["relativePath"], "before": {"sha256": self.target["sha256"], "size": self.target["size"]}, "fault": None}
                if operation == "checkpoint": return {"records": [{"taskId": "task", "bucketIncarnationId": "epoch", "sha256": "c" * 64, "durableProgress": 1}]}
                if operation == "io-eio": return {"probe": {"uid": 1, "mountNamespace": "ns", "path": "/owned", "errno": 5}, "serviceUid": 1, "serviceMountNamespace": "ns", "deviceId": "device"}
                if operation == "io-window": return {"clockDomain": "server", "intervals": [{"kind": "heal", "taskId": "task", "startNs": 1, "endNs": 4, "evidenceId": "h"}, {"kind": "io", "taskId": "fault", "startNs": 2, "endNs": 3, "evidenceId": "f"}]}
                if operation == "io-recover": self.repaired = True
                if operation == "verify-parts": return {"toolSha256": "f" * 64, "candidateCommit": "a" * 40, "parts": [{"key": "objects/0008.bin", "versionId": "null", "sha256": "part", "checksumVerified": True}]}
                if operation == "cleanup": return {"remainingOwnedPaths": [], "activeOwnedServices": [], "remainingFaults": [], "pendingMutations": []}
                return {}
        class FakeProbe:
            def __init__(self, binding, endpoint=None): self.binding = binding
            def inventory(self, bucket, **params): return {"bucket": bucket, "entries": [{"key": "objects/0008.bin", "versionId": "null", "deleteMarker": False, "isLatest": True, "payload": {"size": 100, "sha256": "payload"}}]}
            def admin(self, path, query, body):
                if not query: return {"httpStatus": 200, "response": {"clientToken": "task"}}
                status, expected = ExtendedScenariosTest().terminal()
                row = status["outcome"]["objects"][0]["identity"]
                from urllib.parse import unquote
                row.update(bucket=unquote(path.rsplit("/", 1)[-1]), object="objects/0008.bin", versionId="null")
                return {"httpStatus": 200, "response": status}
        with tempfile.TemporaryDirectory() as tmp:
            adapter = FakeAdapter()
            runtime = {"binding": binding, "faultDeviceId": "device", "inspectorSha256": "f" * 64}
            runner = extended.Runner(runtime, {}, "b" * 40, Path(tmp), adapter=adapter, probe_factory=FakeProbe)
            def fixture(probe, directory, bucket, spec, branch):
                baseline = probe.inventory(bucket)
                return object(), copy.deepcopy(baseline["entries"]), baseline
            with patch.object(runner, "fixture", side_effect=fixture):
                row = runner.branch("heal.slow-io-and-eio", "H1", "eio")
            self.assertEqual(row["cleanup"], "PASS", row)
            self.assertTrue(row.get("checksCompleted"), row)
            self.assertEqual(row["verdict"], "BLOCKED")
            self.assertLess(adapter.calls.index("verify-parts"), adapter.calls.index("cleanup"))
            for operation in ("apply-fault", "checkpoint", "io-eio", "io-window", "io-recover", "verify-parts", "recover", "cleanup"):
                self.assertIn(operation, adapter.calls)

    def test_power_restart_and_io_error_cannot_be_substituted_with_other_faults(self):
        with self.assertRaises(ValueError): extended.assert_action("dedicated-host-power-cycle", {"eventKind": "SIGKILL"}, task_id="task", runtime={"hostResourceId": "dedicated"})
        with self.assertRaises(ValueError): extended.assert_action("crash-service", {"pidBefore": 1, "pidAfter": 2, "rootBefore": "/a", "rootAfter": "/b", "signal": "SIGKILL"}, task_id="task", runtime={})
        with self.assertRaises(ValueError): extended.assert_action("io-eio", {"probe": {"uid": 1, "mountNamespace": "ns", "path": "/owned", "errno": 13},
            "serviceUid": 1, "serviceMountNamespace": "ns", "deviceId": "device"}, task_id="task", runtime={"faultDeviceId": "device"})

    def test_partial_prepare_is_always_recovered_and_dirty_cleanup_is_retained(self):
        test = self
        class FakeAdapter:
            def __init__(self, dirty): self.calls = []; self.dirty = dirty
            def record(self, label, value): return value
            def call(self, operation, **kwargs):
                self.calls.append(operation)
                if operation == "preflight": return test.topology()
                if operation == "prepare": raise RuntimeError("lost prepare ACK")
                if operation == "cleanup": return {"remainingOwnedPaths": ["owned"] if self.dirty else [], "activeOwnedServices": [], "remainingFaults": [], "pendingMutations": []}
                return {}
        for dirty in (False, True):
            with self.subTest(dirty=dirty), tempfile.TemporaryDirectory() as tmp:
                adapter = FakeAdapter(dirty)
                runtime = {"binding": self.binding()}
                runner = extended.Runner(runtime, {}, "b" * 40, Path(tmp), adapter=adapter,
                    probe_factory=lambda binding: types.SimpleNamespace(binding=binding))
                row = runner.branch("heal.slow-io-and-eio", "H1", "eio")
                self.assertEqual(row["verdict"], "FAIL")
                self.assertEqual(row["cleanup"], "FAIL" if dirty else "PASS")
                self.assertEqual(adapter.calls, ["preflight", "prepare", "recover", "cleanup"])
                self.assertIn("lost prepare ACK", row["reason"])

    def test_native_branches_do_not_invoke_resource_adapter(self):
        runner = extended.Runner({"binding": self.binding()}, {}, "b" * 40, Path("unused"), adapter=object())
        row = runner.branch("heal.race-ilm-expiry-and-transition", "H2", "native-ilm-fence")
        self.assertEqual(row["verdict"], "BLOCKED")
        self.assertEqual(row["cleanup"], "NOT_RUN")

    def test_synthetic_clock_checks_full_soak_window_without_shortening_production_duration(self):
        class Clock:
            def __init__(self): self.now = 0
            def clock(self): return self.now
            def sleep(self, seconds): self.now += seconds
        class Adapter:
            def call(self, action, **params):
                if action == "soak-fault": return {"faultId": "fault", "kind": params["kind"], "injectedAtNs": 1}
                if action == "soak-recover": return {"faultId": "fault", "remainingFaults": []}
                return {"rssBytes": 1, "openFds": 1, "queuedTasks": 1, "unresolvedRecords": 0, "temporaryBytes": 1, "panicCount": 0, "busyLoopCount": 0}
            def record(self, label, value): return value
        class Recorder:
            def __init__(self): self.sequence = 0
            def call(self, operation, params, body=None):
                self.sequence += 1
                return {"VersionId": params.get("VersionId", str(self.sequence))}
        clock = Clock(); expected = []
        runtime = {"soakLimits": {"rssBytes": 10, "openFds": 10, "queuedTasks": 10, "unresolvedRecords": 0, "temporaryBytes": 10}}
        runner = extended.Runner(runtime, {}, "b" * 40, Path("unused"), adapter=Adapter(), clock=clock.clock, sleep=clock.sleep)
        result = runner.soak({"branch": "24h"}, Recorder(), "bucket", expected, "H2")
        self.assertEqual(extended.SOAK_SECONDS, {"24h": 86400, "72h": 259200})
        self.assertGreaterEqual(result["actualElapsedSeconds"], 86400)
        self.assertLessEqual(sum(e["key"] == "soak.bin" for e in expected), 65)


if __name__ == "__main__":
    unittest.main()
