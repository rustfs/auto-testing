"""J02 rejects fabricated truth and checks faults before fixture recovery."""

import copy
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import outcome
from scripts.validation_heal.healthy import BASE_OPTIONS, physical_content
from scripts.validation_heal.shards import files
from test_validation_heal import versioned_scene


class MixedOutcomeTest(unittest.TestCase):
    bucket = "rv-heal-0123456789abcdef"
    incarnation = "12345678-1234-1234-1234-123456789abc"
    options = {**BASE_OPTIONS, "recursive": True}

    def mixed(self):
        expected = [{"key": "objects/0000.bin", "versionId": "v0", "state": "verified_healthy"},
                    {"key": "objects/0001.bin", "versionId": "v1", "state": "repaired"},
                    {"key": "objects/0002.bin", "versionId": "v2", "state": "failed"}]
        status = {"summary": "stopped", "settings": self.options, "truncated": False,
            "objectsHealed": 77, "outcome": {"execution": {"state": "completed_with_errors"},
            "coverage": "complete", "objectsTruncated": False,
            "counters": {"processed": 3, "healed": 1, "unchanged": 1, "skipped": 0, "failed": 1,
                         "unknown": 0, "attemptFailures": 4, "overflowed": False},
            "objects": [{"identity": {"kind": "object", "bucket": self.bucket, "object": entry["key"], "versionId": entry["versionId"],
                "poolIndex": 0, "setIndex": 0, "bucketIncarnationId": self.incarnation},
                "disposition": {"state": entry["state"], **({"details": "retry_exhausted"} if entry["state"] == "failed" else {})}}
                for entry in expected]}}
        return expected, status

    def check(self, expected, status, options=None, attempts=4):
        return outcome.reconcile(status, self.bucket, expected, options or self.options, self.incarnation,
                                 attempt_failures=attempts)

    def test_full_mixed_truth_keeps_failure_and_ignores_legacy_repair_count(self):
        expected, status = self.mixed()
        proof = self.check(expected, status)
        self.assertEqual(proof["canonicalCounters"]["failed"], 1)
        self.assertFalse(proof["failedIsRepair"])
        self.assertFalse(proof["legacyObjectsHealedUsedAsRepairEvidence"])

    def test_each_identity_boundary_rejects_wrong_or_ambiguous_receipts(self):
        expected, source = self.mixed()
        for field, values in {"kind": ["bucket", None], "bucket": ["foreign"], "object": ["foreign"], "versionId": [None, "v9"],
                              "poolIndex": [True, 1, None], "setIndex": [False, 1],
                              "bucketIncarnationId": [None, "", "new-incarnation"]}.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    status = copy.deepcopy(source)
                    status["outcome"]["objects"][1]["identity"][field] = value
                    with self.assertRaises(ValueError): self.check(expected, status)
        status = copy.deepcopy(source)
        status["outcome"]["objects"][1] = copy.deepcopy(status["outcome"]["objects"][0])
        with self.assertRaises(ValueError): self.check(expected, status)

    def test_wrong_terminals_or_partial_coverage_cannot_certify_negative_truth(self):
        expected, source = self.mixed()
        for name, value in (("summary", "finished"), ("summary", "running"), ("truncated", True),
                            ("settings", {**self.options, "remove": True})):
            status = copy.deepcopy(source); status[name] = value
            with self.subTest(name=name, value=value), self.assertRaises(ValueError): self.check(expected, status)
        for name, value in (("execution", {"state": "completed"}), ("coverage", "partial"), ("objectsTruncated", True)):
            status = copy.deepcopy(source); status["outcome"][name] = value
            with self.subTest(name=name, value=value), self.assertRaises(ValueError): self.check(expected, status)

    def test_unknown_skipped_deferred_and_false_health_never_substitute_for_failed(self):
        expected, source = self.mixed()
        for state in ("unknown", "skipped", "deferred", "verified_healthy", "metadata_healthy", "repaired"):
            status = copy.deepcopy(source); status["outcome"]["objects"][2]["disposition"]["state"] = state
            with self.subTest(state=state), self.assertRaises(ValueError): self.check(expected, status)
        for details in ("recoverable", "permanent", None):
            status = copy.deepcopy(source); status["outcome"]["objects"][2]["disposition"]["details"] = details
            with self.subTest(details=details), self.assertRaises(ValueError): self.check(expected, status)

    def test_every_counter_rejects_inflation_understatement_and_bool_coercion(self):
        expected, source = self.mixed()
        for name, original in source["outcome"]["counters"].items():
            for value in (None, True if name != "overflowed" else 0, original + 1 if type(original) is int else True):
                status = copy.deepcopy(source); status["outcome"]["counters"][name] = value
                with self.subTest(counter=name, value=value), self.assertRaises(ValueError): self.check(expected, status)

    def test_dry_run_requires_all_original_members_without_repairs_or_hidden_attempts(self):
        expected, status = self.mixed()
        for entry, receipt in zip(expected, status["outcome"]["objects"]):
            entry["state"] = "dry_run_observed"; receipt["disposition"] = {"state": "dry_run_observed"}
        options = {**self.options, "dryRun": True}
        status.update(summary="finished", settings=options)
        status["outcome"].update(execution={"state": "completed"},
            counters={"processed": 3, "healed": 0, "unchanged": 0, "skipped": 3, "failed": 0,
                      "unknown": 0, "attemptFailures": 0, "overflowed": False})
        self.check(expected, status, options, 0)
        status["outcome"]["objects"].pop()
        with self.assertRaises(ValueError): self.check(expected, status, options, 0)

    def test_key_level_deferral_cannot_invent_an_incarnation_or_version(self):
        expected = [{"key": "objects/0002.bin", "versionId": "null", "state": "deferred", "keyLevelRefusal": True}]
        options = {**self.options, "recursive": False}
        status = {"summary": "finished", "settings": options, "truncated": False, "outcome": {
            "execution": {"state": "completed"}, "coverage": "complete", "objectsTruncated": False,
            "counters": {"processed": 1, "healed": 0, "unchanged": 0, "skipped": 1,
                         "failed": 0, "unknown": 0, "attemptFailures": 1, "overflowed": False},
            "objects": [{"identity": {"kind": "object", "bucket": self.bucket, "object": expected[0]["key"], "versionId": None,
                        "bucketIncarnationId": None, "poolIndex": 0, "setIndex": 0}, "disposition": {
                        "state": "deferred", "details": {"reason": "transient_existence_check", "retryNotBefore": None}}}]}}
        self.check(expected, status, options, 1)
        for field, value in (("bucketIncarnationId", self.incarnation), ("versionId", "null"), ("versionId", "v0")):
            changed = copy.deepcopy(status); changed["outcome"]["objects"][0]["identity"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): self.check(expected, changed, options, 1)
        changed = copy.deepcopy(status)
        changed["outcome"]["objects"][0]["disposition"]["details"]["reason"] = "dangling_delete_grace"
        with self.assertRaises(ValueError): self.check(expected, changed, options, 1)
        for details in ({"reason": "transient_existence_check"},
                        {"reason": "transient_existence_check", "retry_not_before": None},
                        {"reason": "transient_existence_check", "retryNotBefore": {"secs_since_epoch": 123, "nanos_since_epoch": 0}},
                        {"reason": "transient_existence_check", "retryNotBefore": None, "retry_not_before": None}):
            changed = copy.deepcopy(status)
            changed["outcome"]["objects"][0]["disposition"]["details"] = details
            with self.subTest(details=details), self.assertRaises(ValueError):
                self.check(expected, changed, options, 1)

    def test_planner_uses_actual_erasure_geometry_and_preserves_unselected_versions(self):
        scene, bucket, expected = versioned_scene([f"objects/{index:04d}.bin" for index in range(3)])
        for branch in outcome.BRANCHES[:3]:
            with self.subTest(branch=branch):
                plan = outcome.fault_plan(scene, bucket, expected, branch)
                self.assertEqual(len(plan["retainedMembers"]), plan["readQuorum"] - 1)
                self.assertEqual(len(plan["manifest"]), 1 if branch == "deferred" else len(expected))
                if branch == "mixed-healthy-repaired-failed":
                    states = {outcome.version_identity(entry): entry["state"] for entry in plan["manifest"]}
                    self.assertEqual(states[("objects/0002.bin", "v0")], "failed")
                    self.assertEqual(states[("objects/0002.bin", "v1")], "verified_healthy")
                if branch == "deferred":
                    self.assertTrue(all(target["relativePath"].endswith("/xl.meta") for target in plan["faults"]))

    def test_physical_oracle_rejects_false_repair_and_any_change_to_a_control(self):
        original, bucket, expected = versioned_scene([f"objects/{index:04d}.bin" for index in range(3)])
        plan = outcome.fault_plan(original, bucket, expected, "mixed-healthy-repaired-failed")
        faulted = copy.deepcopy(original)
        for target in plan["faults"]:
            faulted[target["node"]]["files"] = [entry for entry in faulted[target["node"]]["files"]
                if (entry["disk"], entry["path"]) != (target["disk"], target["relativePath"])]
        after = copy.deepcopy(faulted)
        target = plan["repair"]
        after[target["node"]]["files"].append(files(original)[target["node"], target["disk"], target["relativePath"]])
        before_digest = physical_content(after)
        proof = outcome.retained_failure_scene(original, faulted, after, plan, "mixed-healthy-repaired-failed")
        self.assertTrue(proof["oracleViewOnly"])
        self.assertEqual(physical_content(after), before_digest)
        with self.assertRaises(ValueError):
            outcome.retained_failure_scene(original, faulted, faulted, plan, "mixed-healthy-repaired-failed")
        altered = copy.deepcopy(after); altered[target["node"]]["files"][0]["sha256"] = "changed-control"
        with self.assertRaises(ValueError):
            outcome.retained_failure_scene(original, faulted, altered, plan, "mixed-healthy-repaired-failed")
        with self.assertRaises(ValueError):
            outcome.retained_failure_scene(original, faulted, original, plan, "mixed-healthy-repaired-failed")

    def test_dry_run_and_deferred_require_exact_physical_silence(self):
        original, bucket, expected = versioned_scene([f"objects/{index:04d}.bin" for index in range(3)])
        for branch in ("dry-run", "deferred"):
            plan = outcome.fault_plan(original, bucket, expected, branch)
            self.assertTrue(outcome.retained_failure_scene(original, original, original, plan, branch)["physicalUnchanged"])
            changed = copy.deepcopy(original); changed["node1"]["files"].pop()
            with self.assertRaises(ValueError): outcome.retained_failure_scene(original, original, changed, plan, branch)

    def test_lost_rescue_ack_keeps_registered_fault_for_independent_recovery(self):
        fleet = types.SimpleNamespace(faults=[("node1", {"disk": 0, "faultId": "J02-mixed-0"})],
            remote=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("lost ACK")), record=lambda *args: None)
        with self.assertRaises(RuntimeError): outcome.rescue(fleet)
        self.assertEqual(len(fleet.faults), 1)

    def test_each_reviewed_candidate_requires_native_evidence_before_fleet_creation(self):
        self.assertEqual(outcome.REVIEWED_CANDIDATES,
            {outcome.CANDIDATE, "1c7fc818dd858247c45b39542abd8291b1e6e31b",
             "7951bb629821246868ee3761b321dd07c06db45c"})
        for commit in outcome.REVIEWED_CANDIDATES:
            with self.subTest(commit=commit), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); candidate = root / "candidate.json"
                manifest = {"source_sha": commit}; candidate.write_text(json.dumps(manifest))
                args = types.SimpleNamespace(candidate=candidate)
                with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=args), \
                     patch.object(outcome, "verify_manager", side_effect=ValueError("wrong immutable evidence")) as native, \
                     patch.object(outcome, "Fleet") as fleet, patch.object(outcome.signal, "signal"):
                    with self.assertRaisesRegex(ValueError, "immutable evidence"): outcome.main()
                native.assert_called_once_with(os.environ.get("VALIDATION_HEAL_MANAGER_DIRECTORY", ""), manifest, outcome.CASE_ID)
                fleet.assert_not_called()

    def test_unreviewed_candidate_is_rejected_before_native_or_fleet_activity(self):
        for commit in ("f" * 40, "1aa9be87d18cf941e5020f19e8622c26ba4b10ac", "main"):
            with self.subTest(commit=commit), tempfile.TemporaryDirectory() as tmp:
                candidate = Path(tmp) / "candidate.json"; candidate.write_text(json.dumps({"source_sha": commit}))
                with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=types.SimpleNamespace(candidate=candidate)), \
                     patch.object(outcome, "verify_manager") as native, patch.object(outcome, "Fleet") as fleet, \
                     patch.object(outcome.signal, "signal"):
                    with self.assertRaisesRegex(ValueError, "not reviewed"): outcome.main()
                native.assert_not_called(); fleet.assert_not_called()

    def test_unknown_branch_requires_real_executor_for_each_topology_and_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": outcome.CANDIDATE}))
            args = types.SimpleNamespace(candidate=candidate, evidence=root / "evidence", binary_sha="a" * 64)
            def branch(args, topology, nodes, fixture, name, candidate):
                return {"topology": topology, "fixture": fixture, "branch": name, "verdict": "BLOCKED" if name == "unknown-negative" else "PASS", "cleanup": "PASS"}
            with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=args), \
                 patch.object(outcome, "verify_manager", return_value={"canonicalFleetBranchesExecuted": False}), \
                 patch.object(outcome, "run_branch", side_effect=branch) as executed, patch.object(outcome.signal, "signal"), \
                 patch.dict(os.environ, {"GITHUB_RUN_ID": "1234", "RUSTFS_NODES": "vm000 vm001 vm002 vm003"}):
                self.assertEqual(outcome.main(), 1)
            summary = json.loads((args.evidence / "healthy-summary.json").read_text())
            self.assertEqual(executed.call_count, 20)
            self.assertEqual(len(summary["results"]), 20)
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["cleanup"], "PASS")
            negative = [row for row in summary["results"] if row["branch"] == "unknown-negative"]
            self.assertEqual(len(negative), 4)
            self.assertTrue(all(row["verdict"] == "BLOCKED" for row in negative))

    def test_all_diagnostic_branches_passing_cannot_omit_original_source_procedures(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": outcome.CANDIDATE}))
            args = types.SimpleNamespace(candidate=candidate, evidence=root / "evidence", binary_sha="a" * 64)
            def branch(args, topology, nodes, fixture, name, candidate):
                return {"topology": topology, "fixture": fixture, "branch": name, "verdict": "PASS", "cleanup": "PASS"}
            with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=args), \
                 patch.object(outcome, "verify_manager", return_value={}), patch.object(outcome, "run_branch", side_effect=branch), \
                 patch.object(outcome.signal, "signal"), patch.dict(os.environ, {"GITHUB_RUN_ID": "1234"}):
                self.assertEqual(outcome.main(), 1)
            summary = json.loads((args.evidence / "healthy-summary.json").read_text())
            self.assertTrue(summary["diagnosticBranchesPassed"])
            self.assertFalse(summary["complete"])
            self.assertFalse(summary["canonicalCasePassClaimed"])
            self.assertEqual(summary["sourceAssertionGaps"], [])
            self.assertFalse(summary["canonicalAdmissionEnabled"])
            self.assertTrue(summary["liveSourceProcedureAcceptancePending"])

    def test_dirty_cleanup_stops_before_next_fixture_or_topology(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": outcome.CANDIDATE}))
            args = types.SimpleNamespace(candidate=candidate, evidence=root / "evidence", binary_sha="a" * 64)
            row = {"topology": "H1", "fixture": "DB", "branch": outcome.BRANCHES[0], "verdict": "FAIL", "cleanup": "FAIL"}
            with patch.object(outcome.argparse.ArgumentParser, "parse_args", return_value=args), \
                 patch.object(outcome, "verify_manager", return_value={}), \
                 patch.object(outcome, "run_branch", return_value=row) as run, patch.object(outcome.signal, "signal"), \
                 patch.dict(os.environ, {"GITHUB_RUN_ID": "1234", "RUSTFS_NODES": "vm000 vm001 vm002 vm003"}):
                self.assertEqual(outcome.main(), 1)
            self.assertEqual(run.call_count, 1)
            summary = json.loads((args.evidence / "healthy-summary.json").read_text())
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["cleanup"], "FAIL")



class LegacyUnknownTests(unittest.TestCase):
    def physical(self):
        original, bucket, expected = versioned_scene(["objects/0000.bin", "legacy/0000.bin"])
        for scene in original.values():
            for row in scene["files"]:
                for version in row.get("metadata", {}).get("versions", []):
                    for part in version["parts"]:
                        part["integrityDescriptorPresent"] = not row["metadata"]["key"].startswith("legacy/")
        return original, bucket, expected

    def test_unknown_oracle_requires_independent_complete_physical_legacy_and_protected_control(self):
        original, bucket, expected = self.physical()
        plan = outcome.legacy_manifest(original, bucket, expected)
        incarnation = MixedOutcomeTest.incarnation
        options = {**BASE_OPTIONS, "recursive": True}
        receipts = [{"identity": {"kind": "object", "bucket": bucket, "object": row["key"], "versionId": row["versionId"],
                    "bucketIncarnationId": incarnation, "poolIndex": 0, "setIndex": 0},
                     "disposition": {"state": row["state"]}} for row in plan["manifest"]]
        counts = {"processed": 6, "healed": 0, "unchanged": 3, "skipped": 3, "failed": 0,
                  "unknown": 3, "attemptFailures": 0, "overflowed": False}
        status = {"summary": "finished", "truncated": False, "settings": options, "objectsHealed": 99,
                  "outcome": {"execution": {"state": "completed"}, "coverage": "complete", "objectsTruncated": False,
                              "objects": receipts, "counters": counts}}
        unknown = [tuple(pair) for pair in plan["unknownIdentities"]]
        proof = outcome.reconcile(status, bucket, plan["manifest"], options, incarnation, attempt_failures=0, legacy_unknown=unknown)
        self.assertFalse(proof["unknownIsRepair"])
        self.assertEqual(proof["canonicalCounters"]["healed"], 0)
        counters = proof["canonicalCounters"]
        self.assertEqual(counters["unknown"], 3)
        self.assertEqual(counters["skipped"], 3)
        self.assertEqual(counters["processed"], sum(counters[key] for key in ("healed", "unchanged", "skipped", "failed")))
        with self.assertRaises(ValueError): outcome.reconcile(status, bucket, plan["manifest"], options, incarnation, attempt_failures=0)
        for mutation in ("repaired", "identity", "failed", "incomplete", "unknown-count", "attempt-failure"):
            changed = copy.deepcopy(status)
            if mutation == "repaired": changed["outcome"]["objects"][-1]["disposition"]["state"] = "repaired"
            elif mutation == "identity": changed["outcome"]["objects"][-1]["identity"]["bucketIncarnationId"] = "foreign"
            elif mutation == "failed": changed["outcome"]["execution"]["state"] = "completed_with_errors"
            elif mutation == "incomplete": changed["outcome"]["coverage"] = "partial"
            elif mutation == "unknown-count": changed["outcome"]["counters"]["unknown"] = 0
            else: changed["outcome"]["counters"]["attemptFailures"] = 1
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                outcome.reconcile(changed, bucket, plan["manifest"], options, incarnation, attempt_failures=0, legacy_unknown=unknown)

    def test_descriptor_missing_true_integer_or_a_missing_cohort_never_creates_unknown_fixture(self):
        for mutation in ("missing", "protected", "coerced", "missing-member", "no-control"):
            original, bucket, expected = self.physical()
            row = next(x for x in original["node1"]["files"] if x.get("metadata", {}).get("key", "").startswith("legacy/"))
            if mutation == "missing": row["metadata"]["versions"][0]["parts"][0].pop("integrityDescriptorPresent")
            elif mutation == "protected": row["metadata"]["versions"][0]["parts"][0]["integrityDescriptorPresent"] = True
            elif mutation == "coerced": row["metadata"]["versions"][0]["parts"][0]["integrityDescriptorPresent"] = 0
            elif mutation == "missing-member": original["node1"]["files"].remove(row)
            else: expected = [x for x in expected if x["key"].startswith("legacy/")]
            with self.subTest(mutation=mutation), self.assertRaises(ValueError): outcome.legacy_manifest(original, bucket, expected)

    def test_legacy_append_uses_actual_sdk_calls_after_all_node_write_mode_switches(self):
        events = []
        def call(operation, params, body=None):
            events.append((operation, params))
            if operation == "put_object": return {"VersionId": str(len(events))}
            return {"VersionId": str(len(events)), "DeleteMarker": True}
        probe = types.SimpleNamespace()
        fleet = types.SimpleNamespace(nodes=["a", "b"], remote=lambda node, action: events.append((action, node)) or {
                "runId": "gh-12345-1-h1", "binarySha256": "a" * 64, "phase": "legacy", "oneWay": True,
                "candidateUnchanged": True, "otherRuntimeSettingsUnchanged": True, "credentialsDisclosed": False},
            record=lambda label, value: value, start=lambda: events.append(("start", None)),
            snapshot=lambda label: {node: {"backgroundConfig": {"RUSTFS_SHARD_INTEGRITY_WRITE": "false",
                "RUSTFS_HEAL_MRF_ENABLE": "false", "RUSTFS_SCANNER_ENABLED": "false"}} for node in ("a", "b")})
        with patch.object(outcome, "Recorder", return_value=types.SimpleNamespace(call=call)):
            value = outcome.append_legacy_fixture(probe, fleet, Path("/unused"),
                {"bucketPrefix": MixedOutcomeTest.bucket, "runId": "gh-12345-1-h1", "candidateBinarySha256": "a" * 64},
                [{"key": "protected-control"}], "DV")
        self.assertEqual([name for name,_ in events[:3]], ["outcome-legacy-phase", "outcome-legacy-phase", "start"])
        self.assertEqual(len(value), 8)
        self.assertEqual(sum(name == "put_object" for name,_ in events), 6)
        self.assertEqual(sum(name == "delete_object" for name,_ in events), 1)
        self.assertEqual(sum(row.get("deleteMarker", False) for row in value), 1)


class LegacyPhaseTests(unittest.TestCase):
    def test_one_way_private_configuration_commit_survives_each_completed_write_boundary(self):
        from scripts.validation_heal import fleet_node as node, heal_fault as fault
        from test_validation_heal_retained_scene import RetainedSceneTests
        for cut in (None, "J02-legacy-intent.json", "J02-service.env.new", "J02-phase.new", "rename-service.env", "rename-J02-phase.json"):
            with self.subTest(cut=cut), tempfile.TemporaryDirectory() as temp:
                state = Path(temp).resolve(); binary = b"candidate"; (state / "rustfs").write_bytes(binary)
                owner = {"runId": "gh-12345-1-h1", "unit": "owned.service", "runtimeProfile": "outcome-legacy",
                         "binarySha256": __import__('hashlib').sha256(binary).hexdigest()}
                config = {**node.runtime_settings(owner), "RUSTFS_ADDRESS": ":9000", "RUSTFS_SECRET_KEY": "private-must-not-escape"}
                env = state / "service.env"; env.write_text("".join(f"{key}={value}\n" for key,value in config.items())); env.chmod(0o600)
                request = {"runId": owner["runId"], "action": "outcome-legacy-phase"}
                def owned(run):
                    value = {**owner, "outcomePhase": "legacy"} if (state / "J02-phase.json").exists() else owner
                    return state, [], value
                original_stat = os.stat
                def root_stat(path, *args, **kwargs):
                    info = original_stat(path, *args, **kwargs)
                    values = {name: getattr(info,name) for name in dir(info) if name.startswith("st_")}
                    values["st_uid"] = 0
                    return types.SimpleNamespace(**values)
                original = fault.new_file
                original_rename = os.rename
                fired = False
                def write(fd, name, value):
                    nonlocal fired
                    original(fd, name, value)
                    if cut == name and not fired:
                        fired = True; raise RuntimeError("lost acknowledgement")
                def rename(source, target, **kwargs):
                    nonlocal fired
                    original_rename(source, target, **kwargs)
                    if cut == "rename-" + target and not fired:
                        fired = True; raise RuntimeError("lost acknowledgement after rename")
                with patch.object(node, "owned", side_effect=owned), patch.object(node, "stopped"), \
                     patch.object(node, "fault_module", return_value=fault), patch.object(fault, "mount_id", return_value=7), \
                     RetainedSceneTests().root_stats(), patch.object(os, "stat", side_effect=root_stat), \
                     patch.object(fault, "new_file", side_effect=write), patch.object(os, "rename", side_effect=rename):
                    if cut:
                        with self.assertRaises(RuntimeError): node.outcome_legacy_phase(request)
                        if not (state / "J02-phase.json").exists():
                            with patch.object(node, "command") as command, self.assertRaisesRegex(ValueError, "incomplete"):
                                node.start({"runId": owner["runId"], "action": "start"})
                            self.assertNotIn("systemd-run", [call.args[0] for call in command.call_args_list])
                    result = node.outcome_legacy_phase(request)
                    self.assertTrue(result["oneWay"])
                    self.assertNotIn(config["RUSTFS_SECRET_KEY"], json.dumps(result))
                    actual = dict(line.split("=",1) for line in env.read_text().splitlines())
                    self.assertEqual(actual, {**config, "RUSTFS_SHARD_INTEGRITY_WRITE": "false"})
                    self.assertEqual(node.runtime_settings(owned(owner["runId"])[2])["RUSTFS_SHARD_INTEGRITY_WRITE"], "false")
                    self.assertTrue(node.outcome_legacy_phase(request)["otherRuntimeSettingsUnchanged"])
                    env.write_text(env.read_text().replace("private-must-not-escape", "foreign"))
                    with self.assertRaises(ValueError): node.outcome_legacy_phase(request)

    def test_actual_owned_reader_rejects_uncommitted_or_foreign_phase_markers(self):
        from scripts.validation_heal import fleet_node as node
        from test_validation_heal_retained_scene import RetainedSceneTests
        with tempfile.TemporaryDirectory() as directory:
            state, roots, scope, request = RetainedSceneTests().scene(directory)
            owner = {**request["expectedOwner"], "runtimeProfile": "outcome-legacy"}
            for folder in (*roots, state):
                folder.mkdir(parents=True, exist_ok=True)
                (folder / "owner.json").write_text(json.dumps(owner))
            phase = state / "J02-phase.json"
            with RetainedSceneTests().root_stats(), patch.object(node, "paths", return_value=(state, roots)), \
                 patch.object(node, "mounts", return_value=owner["mounts"]):
                self.assertNotIn("outcomePhase", node.owned(owner["runId"])[2])
                phase.write_text(json.dumps({"runId": owner["runId"], "phase": "legacy"}))
                self.assertEqual(node.owned(owner["runId"])[2]["outcomePhase"], "legacy")
                for value in ({"runId": owner["runId"], "phase": "protected"},
                              {"runId": "foreign", "phase": "legacy"},
                              {"runId": owner["runId"], "phase": "legacy", "extra": True}):
                    phase.write_text(json.dumps(value))
                    with self.subTest(value=value), self.assertRaises(ValueError): node.owned(owner["runId"])

    def test_phase_adapter_loads_real_fault_backend(self):
        from scripts.validation_heal.healthy import Fleet
        def execute(*args, **kwargs):
            scope = {"__name__": "__J02_node__"}
            exec(compile(kwargs["input"].rsplit("\nmain(",1)[0], "J02-remote", "exec"), scope)
            self.assertTrue(callable(scope["fault"].new_file))
            self.assertTrue(callable(scope["outcome_legacy_phase"]))
            return types.SimpleNamespace(returncode=0, stdout="{}", stderr="")
        with tempfile.TemporaryDirectory() as directory, patch("scripts.validation_heal.healthy.subprocess.run", side_effect=execute):
            Fleet(["node"], Path(directory), "gh-12345-1-h1", runtime_profile="outcome-legacy").remote("node", "outcome-legacy-phase")



class ReadonlySourceFlowTests(unittest.TestCase):
    bucket = MixedOutcomeTest.bucket
    key = "objects/0001.bin"
    options = {**BASE_OPTIONS, "recursive": False}

    def failure(self):
        return {"summary": "stopped", "settings": self.options, "truncated": False, "outcome": {
            "execution": {"state": "completed_with_errors"}, "coverage": "complete", "objectsTruncated": False,
            "counters": {"processed": 1, "healed": 0, "unchanged": 0, "skipped": 0,
                         "failed": 1, "unknown": 0, "attemptFailures": 1, "overflowed": False},
            "objects": [{"identity": {"kind": "object", "bucket": self.bucket, "object": self.key,
                                      "versionId": None, "bucketIncarnationId": None, "poolIndex": 0, "setIndex": 0},
                         "disposition": {"state": "failed", "details": "permanent"}}]}}

    def test_permanent_terminal_failure_must_not_invent_a_verified_version_or_incarnation(self):
        status = self.failure()
        self.assertFalse(outcome.readonly_failure(status, self.bucket, self.key, self.options)["repairClaimed"])
        for mutation in ("version", "incarnation", "kind", "pool-bool", "attempts", "healed", "unknown", "deferred", "recoverable", "settings"):
            changed = copy.deepcopy(status); receipt = changed["outcome"]["objects"][0]
            if mutation == "version": receipt["identity"]["versionId"] = "null"
            elif mutation == "incarnation": receipt["identity"]["bucketIncarnationId"] = MixedOutcomeTest.incarnation
            elif mutation == "kind": receipt["identity"]["kind"] = "bucket"
            elif mutation == "pool-bool": receipt["identity"]["poolIndex"] = False
            elif mutation == "attempts": changed["outcome"]["counters"]["attemptFailures"] = 0
            elif mutation == "healed": changed["outcome"]["counters"]["healed"] = 1
            elif mutation == "unknown": receipt["disposition"] = {"state": "unknown"}
            elif mutation == "deferred": receipt["disposition"] = {"state": "deferred", "details": {"reason": "deadline"}}
            elif mutation == "recoverable": receipt["disposition"]["details"] = "retry_exhausted"
            else: changed["settings"] = {**self.options, "remove": True}
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                outcome.readonly_failure(changed, self.bucket, self.key, self.options)

    def test_lost_readonly_arm_ack_keeps_gate_registered_for_final_cleanup(self):
        original, bucket, expected = versioned_scene([f"objects/{i:04d}.bin" for i in range(3)])
        actual_bucket = "rv-heal-" + __import__("hashlib").sha256(b"gh-12345-1-h1").hexdigest()[:16]
        for scene in original.values():
            for row in scene["files"]:
                row["path"] = row["path"].replace(bucket,actual_bucket)
                if "metadata" in row: row["metadata"]["bucket"] = actual_bucket
        bucket = actual_bucket
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); rc = root / "rc"; rc.write_bytes(b"test-cli")
            args = types.SimpleNamespace(evidence=root / "evidence", rc_bin=rc, binary_sha="b" * 64,
                testing_commit="a" * 40, tool_directory=root / "tools")
            gates, current, cleanups = set(), copy.deepcopy(original), []
            def remote(node, action, **params):
                if action == "apply-fault":
                    current[node]["files"] = [row for row in current[node]["files"] if row["path"] != params["relativePath"] or row["disk"] != params["disk"]]
                    return {}
                if action == "drive-gate":
                    self.assertIn(node, gates)
                    raise RuntimeError("lost readonly gate ACK")
                return {}
            def cleanup():
                cleanups.append(set(gates)); return True
            fleet = types.SimpleNamespace(nodes=["node1"], drive_gates=gates, reserved=["node1"], faults=[], remote=remote,
                deploy=lambda *args: None, snapshot=lambda *args,**kwargs: copy.deepcopy(current),
                record=lambda label,value: value, stop=lambda: None, start=lambda: None, cleanup=cleanup)
            probe = types.SimpleNamespace(inventory=lambda *args,**kwargs: {"bucket":bucket,"entries":expected})
            with patch.object(outcome,"Fleet",return_value=fleet), patch.object(outcome,"Probe",return_value=probe), \
                 patch.object(outcome,"Recorder"), patch.object(outcome,"seed",return_value=expected), \
                 patch.object(outcome,"verify_inspector",return_value={}), patch.object(outcome,"verify_quorum_contract",return_value={}), \
                 patch.object(outcome,"run_task",return_value={}), patch.object(outcome,"validate_version_receipts",return_value={"bucketIncarnationId":MixedOutcomeTest.incarnation}), \
                 patch.object(outcome,"idle_fleet"), patch.object(outcome,"validate_fault"), patch.object(outcome,"verify_proof"), \
                 patch.dict(os.environ,{"GITHUB_RUN_ID":"12345","GITHUB_RUN_ATTEMPT":"1"}):
                result = outcome.run_branch(args,"H1",["node1"],"DV","readonly-terminal-failure",
                    {"source_sha":outcome.CANDIDATE,"package_sha256":"c" * 64})
            self.assertEqual(result["verdict"],"FAIL")
            self.assertIn("lost readonly gate ACK",result["failure"]["message"])
            self.assertEqual(result["cleanup"],"PASS")
            self.assertEqual(cleanups,[{"node1"}])

    def test_readonly_plan_selects_exact_current_version_without_a_below_R_fault(self):
        original, bucket, expected = versioned_scene([f"objects/{i:04d}.bin" for i in range(3)])
        plan = outcome.fault_plan(original, bucket, expected, "readonly-terminal-failure")
        self.assertEqual(len(plan["faults"]), 1)
        self.assertEqual(plan["repair"]["versionId"], "v2")
        self.assertTrue(plan["repair"]["logical"]["isLatest"])
        self.assertEqual(plan["faults"][0], plan["repair"])

    def test_compensation_observes_readonly_failure_then_distinct_new_token_without_replacing_original(self):
        from scripts.validation_heal.shards import files
        for variant in (None, "fake-gate", "same-token", "changed-original", "changed-control", "bad-control"):
            with self.subTest(variant=variant):
                original, bucket, expected = versioned_scene([f"objects/{i:04d}.bin" for i in range(3)])
                plan = outcome.fault_plan(original, bucket, expected, "readonly-terminal-failure")
                target = plan["repair"]; faulted = copy.deepcopy(original)
                faulted[target["node"]]["files"] = [row for row in faulted[target["node"]]["files"]
                    if (target["node"], row["disk"], row["path"]) != (target["node"], target["disk"], target["relativePath"])]
                repaired_scene = copy.deepcopy(original)
                if variant == "changed-control": repaired_scene["node1"]["files"][0]["sha256"] = "changed"
                negative = self.failure()
                positive = {"summary": "finished", "settings": self.options, "truncated": False, "outcome": {
                    "execution": {"state": "completed"}, "coverage": "complete", "objectsTruncated": False,
                    "counters": {"processed": 1, "healed": 1, "unchanged": 0, "skipped": 0,
                                 "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False},
                    "objects": [{"identity": {"kind": "object", "bucket": bucket, "object": self.key,
                        "versionId": target["versionId"], "bucketIncarnationId": MixedOutcomeTest.incarnation,
                        "poolIndex": 0, "setIndex": 0}, "disposition": {"state": "repaired"}}]}}
                baseline = {"bucket": bucket, "entries": expected}; events = []; queries = []
                def remote(node, action, **params):
                    events.append(action)
                    if action == "drive-gate-probe": return {"disk": target["disk"], "errno": 13 if variant == "fake-gate" else 30,
                        "name": "EROFS", "sameServiceIdentity": True, "privateMountNamespace": True, "hostMountRemounted": False}
                    if action == "drive-gate": return {"state": "CLEAR"}
                    return {"restored": False}
                def snapshot(label, **kwargs):
                    events.append(label)
                    return copy.deepcopy(faulted if label in {"readonly-original-failure-physical", "readonly-released-before-new-token", "readonly-after-gated-control-before-admission"} else repaired_scene)
                fleet = types.SimpleNamespace(nodes=["node1"], record=lambda label,value: value, remote=remote,
                    drive_gates={target["node"]}, faults=[], stop=lambda: events.append("stop"), start=lambda: events.append("start"), snapshot=snapshot)
                def admin(path, query, body):
                    queries.append(query); result = copy.deepcopy(negative)
                    if variant == "changed-original" and len(queries) == 2: result["outcome"]["counters"]["failed"] = 0
                    return {"httpStatus": 200, "response": result}
                probe = types.SimpleNamespace(admin=admin)
                def task(probe,fleet,scope,mode,active,**kwargs):
                    number = len([item for item in events if item == "admit"]); events.append("admit")
                    if number == 0:
                        control = copy.deepcopy(positive)
                        control["outcome"]["counters"].update(healed=0,unchanged=1)
                        control["outcome"]["objects"][0]["identity"]["object"] = "objects/0000.bin"
                        control["outcome"]["objects"][0]["disposition"] = {"state":"unknown" if variant == "bad-control" else "verified_healthy"}
                        return control
                    kwargs["admissions"].append({"path": "/heal", "clientToken": "old" if number == 1 or variant == "same-token" else "new"})
                    return negative if number == 1 else positive
                binding = {"bucketPrefix":bucket,"endpoints":{"node1":"http://unused"}}
                with patch.object(outcome,"run_task",side_effect=task), patch.object(outcome,"idle_fleet"), \
                     patch.object(outcome,"verify_proof",side_effect=lambda *args: events.append("bitrot-proof")), \
                     patch.object(outcome,"Probe",return_value=types.SimpleNamespace(inventory=lambda *args,**kwargs: baseline)):
                    if variant:
                        with self.assertRaises(ValueError): outcome.readonly_compensation(probe,fleet,binding,original,faulted,plan,expected,baseline,MixedOutcomeTest.incarnation,[])
                    else:
                        value=outcome.readonly_compensation(probe,fleet,binding,original,faulted,plan,expected,baseline,MixedOutcomeTest.incarnation,[])
                        self.assertFalse(value["sameTokenRecoveryClaimed"])
                        self.assertTrue(value["originalFailureUnchanged"])
                        self.assertNotEqual(value["originalToken"],value["compensatingToken"])
                        self.assertEqual(sum(item == "admit" for item in events),3)
                        self.assertLess(events.index("readonly-original-failure-physical"),events.index("drive-gate"))
                        self.assertLess(events.index("readonly-released-before-new-token"),len(events)-1)
                        self.assertEqual(len(queries),2)



class OriginalOpaqueMetadataTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'
    key = 'opaque/source.bin'
    options = {**outcome.BASE_OPTIONS, 'recursive': False}

    def status(self, state='unknown'):
        failed = state == 'failed'
        return {'summary': 'stopped' if failed else 'finished', 'settings': self.options, 'truncated': False,
            'detail': 'legacy incomplete outcome; unknown is not proof of repair' if state == 'unknown' else None,
            'outcome': {'execution': {'state': 'completed_with_errors' if failed else 'completed'},
                'coverage': 'complete', 'objectsTruncated': False,
                'counters': {'processed': 1, 'healed': 0, 'unchanged': 0, 'skipped': 0 if failed else 1,
                    'failed': int(failed), 'unknown': int(state == 'unknown'), 'attemptFailures': int(state != 'unknown'), 'overflowed': False},
                'objects': [{'identity': {'kind': 'object', 'bucket': self.bucket, 'object': self.key,
                    'versionId': None, 'bucketIncarnationId': None, 'poolIndex': 0, 'setIndex': 0},
                    'disposition': {'state': state}}]}}

    def test_original_metadata_unknown_observation_and_nonobservation_are_distinct(self):
        for state in ('unknown', 'deferred', 'failed'):
            result = outcome.opaque_metadata_outcome(self.status(state), self.bucket, self.key, self.options, "original-anchor")
            self.assertEqual(result['verdict'], 'PASS' if state == 'unknown' else 'NOT_OBSERVED')
            self.assertEqual(result['unknownObserved'], state == 'unknown')
            self.assertFalse(result['canonicalPassClaimed'])
            self.assertFalse(result['productRepairClaimed'])

    def test_known_original_anchor_is_allowed_without_inventing_a_new_incarnation(self):
        status = self.status()
        status["outcome"]["objects"][0]["identity"]["bucketIncarnationId"] = "original-anchor"
        self.assertTrue(outcome.opaque_metadata_outcome(status, self.bucket, self.key, self.options, "original-anchor")["unknownObserved"])

    def test_false_repair_unknown_counters_or_identity_are_rejected(self):
        for poison in ('healthy', 'absent', 'repaired', 'unknown-partition', 'boolean-counter', 'overflow', 'omitted',
                       'wrong-key', 'invented-version', 'invented-incarnation', 'boolean-pool', 'warning', 'running', 'partial', 'execution'):
            with self.subTest(poison=poison):
                value = self.status(); body = value['outcome']; obj = body['objects'][0]
                if poison in ('healthy', 'absent', 'repaired'):
                    obj['disposition']['state'] = {'healthy': 'verified_healthy', 'absent': 'authoritatively_absent', 'repaired': 'repaired'}[poison]
                elif poison == 'unknown-partition': body['counters']['skipped'] = 0
                elif poison == 'boolean-counter': body['counters']['processed'] = True
                elif poison == 'overflow': body['counters']['overflowed'] = True
                elif poison == 'omitted': body['objects'] = []
                elif poison == 'wrong-key': obj['identity']['object'] = 'other'
                elif poison == 'invented-version': obj['identity']['versionId'] = 'new-version'
                elif poison == 'invented-incarnation': obj['identity']['bucketIncarnationId'] = 'fabricated'
                elif poison == 'boolean-pool': obj['identity']['poolIndex'] = False
                elif poison == 'warning': value['detail'] = None
                elif poison == 'running': value['summary'] = 'running'
                elif poison == 'partial': body['coverage'] = 'partial'
                elif poison == 'execution': body['execution']['state'] = 'completed_with_errors'
                with self.assertRaises(ValueError):
                    outcome.opaque_metadata_outcome(value, self.bucket, self.key, self.options, "original-anchor")

    def test_original_probe_mutates_all_four_metadata_copies_and_restores_before_reads(self):
        original, bucket, entries = versioned_scene([self.key, 'objects/control.bin'])
        expected = [entry for entry in entries if entry['key'] == 'objects/control.bin']
        current = copy.deepcopy(original)
        events, registrations = [], {}
        active = []
        status = self.status()
        fleet = types.SimpleNamespace(nodes=['node1'], faults=[])
        fleet.start = lambda: events.append('start')
        fleet.stop = lambda: events.append('stop')
        def record(label, value):
            events.append(label); return value
        fleet.record = record
        def snapshot(label, **params):
            events.append(label)
            if any('metadataError' in row for row in current['node1']['files']):
                self.assertTrue(params.get('metadata_faults'))
                from scripts.validation_heal import fleet_node
                with patch.object(fleet_node, 'command', return_value=types.SimpleNamespace(returncode=1, stderr='Error: FileCorrupt\n')):
                    for row in current['node1']['files']:
                        if 'metadataError' in row:
                            witness = fleet_node.inspect_metadata(Path('/inspector'), Path('/xl.meta'), bucket, self.key,
                                row, params['metadata_faults']['node1'])
                            self.assertTrue(witness['metadataError']['exactWitnessedFault'])
            return copy.deepcopy(current)
        fleet.snapshot = snapshot
        def remote(node, action, **params):
            events.append(action)
            if action == 'apply-fault':
                self.assertEqual(fleet.faults[-1], (node, {'disk': params['disk'], 'faultId': params['faultId']}))
                self.assertEqual((params['kind'], params['offset']), ('bitflip', 0))
                self.assertTrue(params['relativePath'].endswith('/opaque/source.bin/xl.meta'))
                row = next(row for row in current[node]['files'] if row['disk'] == params['disk'] and row['path'] == params['relativePath'])
                registrations[params['faultId']] = copy.deepcopy(row)
                before = {key: row[key] for key in ('sha256', 'size')}
                row['sha256'] = 'corrupt-' + str(row['disk'])
                row.pop('metadata')
                row['metadataError'] = {'exactWitnessedFault': True, 'validMetadataClaimed': False}
                return {'kind': 'bitflip', 'offset': 0, 'relativePath': row['path'], 'before': before,
                        'fault': {key: row[key] for key in ('sha256', 'size')}}
            self.assertEqual(action, 'recover-fault')
            saved = registrations[params['faultId']]
            row = next(row for row in current[node]['files'] if row['disk'] == saved['disk'] and row['path'] == saved['path'])
            row.clear(); row.update(copy.deepcopy(saved))
            return {'physicalRepairPassAllowed': False}
        fleet.remote = remote
        def inventory(*args, **kwargs):
            events.append('business-GET-inventory')
            self.assertEqual(physical_content(current), physical_content(original))
            return {'entries': []}
        probe = types.SimpleNamespace(inventory=inventory, admin=lambda *args: {'httpStatus': 200, 'response': status})
        def task(probe, fleet, scope, mode, active, **params):
            events.append('task:' + scope['key'])
            if 'admissions' in params:
                params['admissions'].append({'path': '/heal/opaque', 'clientToken': 'original-opaque-token'})
                return status
            return {}
        binding = {'bucketPrefix': bucket, 'endpoints': {'node1': 'http://node1'}}
        with tempfile.TemporaryDirectory() as directory, patch.object(outcome, 'Recorder') as recorder, \
             patch.object(outcome, 'Probe', return_value=probe), patch.object(outcome, 'run_task', side_effect=task), \
             patch.object(outcome, 'validate_version_receipts', return_value={'bucketIncarnationId': 'incarnation'}), \
             patch.object(outcome, 'verify_seed', return_value={}), patch.object(outcome, 'compare_inventories'), \
             patch.object(outcome, 'verify_proof'), patch.object(outcome, 'payload', return_value=b'opaque-fixture'), \
             patch.object(outcome, 'idle_fleet'):
            recorder.return_value.call.return_value = {'VersionId': 'v2'}
            updated, baseline, result = outcome.original_opaque_probe(
                probe, fleet, binding, Path(directory), expected, 'DV', 'incarnation', active)
        self.assertEqual(result['originalMetadataCopiesCorrupted'], 4)
        self.assertTrue(result['unknownObserved'])
        self.assertTrue(result['originalTokenUnchanged'])
        self.assertEqual(len(updated), len(expected) + 1)
        self.assertEqual(fleet.faults, [])
        self.assertEqual(events.count('apply-fault'), 4)
        self.assertEqual(events.count('recover-fault'), 4)
        self.assertLess(events.index('original-opaque-terminal-before-rescue'), events.index('recover-fault'))
        self.assertLess(events.index('original-opaque-restored-original-bytes'), events.index('original-opaque-restored-full-S3'))


class OmittedFalseTruncationTests(unittest.TestCase):
    def test_original_wire_omission_does_not_relax_canonical_manifest_or_counter_checks(self):
        mixed = MixedOutcomeTest(); expected, status = mixed.mixed()
        readonly = ReadonlySourceFlowTests(); opaque = OriginalOpaqueMetadataTests()
        oracles = [(status, lambda value: mixed.check(expected, value)),
            (readonly.failure(), lambda value: outcome.readonly_failure(value, readonly.bucket, readonly.key, readonly.options)),
            (opaque.status(), lambda value: outcome.opaque_metadata_outcome(value, opaque.bucket, opaque.key, opaque.options, 'original-anchor'))]
        for source, check in oracles:
            source.pop('truncated'); check(source)
            for value in (None, True, 0, 'false'):
                with self.subTest(value=value), self.assertRaises(ValueError): check({**source, 'truncated': value})
            for field, value in (('objectsTruncated', True), ('coverage', 'partial')):
                changed = copy.deepcopy(source); changed['outcome'][field] = value
                with self.assertRaises(ValueError): check(changed)
            changed = copy.deepcopy(source); changed['outcome'].pop('objectsTruncated')
            with self.assertRaises(ValueError): check(changed)
            changed = copy.deepcopy(source); changed['outcome']['objects'].pop()
            with self.assertRaises(ValueError): check(changed)

    def test_repeated_original_token_accepts_only_omitted_false_or_explicit_false(self):
        status = ReadonlySourceFlowTests().failure(); status.pop('truncated')
        response = dict(httpStatus=200, response=copy.deepcopy(status))
        probe = types.SimpleNamespace(admin=lambda *args: response)
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        admission = dict(path='/heal', clientToken='original')
        outcome.unchanged_token(probe, fleet, admission, status, 'original')
        for value in (None, True):
            response['response']['truncated'] = value
            with self.assertRaises(ValueError): outcome.unchanged_token(probe, fleet, admission, status, 'changed')


if __name__ == "__main__": unittest.main()
