"""Manager native evidence must bind exact source, executable and real tests."""

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.validation_heal import manager_native as suite


class ManagerNativeEvidenceTests(unittest.TestCase):
    def fixture(self, root):
        tools, evidence = root / "tools", root / "evidence"
        tools.mkdir(); evidence.mkdir()
        helper = "a" * 40
        binary = tools / "heal-manager-native-tests"
        binary.write_bytes(b"original candidate unit executable")
        digest = hashlib.sha256(binary.read_bytes()).hexdigest()
        manifest = {"schemaVersion": 1, "candidateCommit": suite.native.CANDIDATE,
            "candidateSourceTree": suite.SOURCE_TREE, "candidateCargoLockSha256": suite.LOCK_SHA,
            "sourceFiles": suite.SOURCE_FILES, "collectorSources": suite.collectors(),
            "regressions": suite.REGRESSIONS, "sourceUnmodified": True, "optimized": True,
            "buildEnvironment": suite.BUILD_ENV, "binaryName": binary.name, "binarySha256": digest,
            "fleetPassClaimed": False, "runId": "12345", "runAttempt": "1", "testingCommit": helper}
        (tools / "manager-native.json").write_text(json.dumps(manifest))
        listing = "\n".join(value["test"] + ": test" for value in suite.REGRESSIONS.values())
        (tools / "test-inventory.log").write_text(listing)
        (evidence / "runtime-inventory.log").write_text(listing)
        rows = []
        for key, value in suite.REGRESSIONS.items():
            row = {"regression": key, **value, "candidateCommit": suite.native.CANDIDATE,
                "binarySha256": digest, "verdict": "PASS", "exitCode": 0, "elapsedNanoseconds": 100,
                "command": ["/hosted/tools/" + binary.name, value["test"], "--exact", "--nocapture", "--test-threads=1"]}
            (evidence / (key + ".json")).write_text(json.dumps(row))
            (evidence / (key + ".log")).write_text("running 1 test\ntest " + value["test"] + " ... ok\n"
                "test result: ok. 1 passed; 0 failed; 0 ignored; 99 filtered out; finished in 0.01s\n")
            rows.append(row)
        summary = {"results": rows, "complete": True, "canonicalCasePassClaimed": False,
                   "fleetPassClaimed": False, "physicalPowerLossClaimed": False}
        (evidence / "manager-native-summary.json").write_text(json.dumps(summary))
        return tools, evidence, {"source_sha": suite.native.CANDIDATE}, helper

    def verify(self, fixture, **changes):
        tools, evidence, candidate, helper = fixture
        return suite.verify_evidence(tools, evidence, changes.get("candidate", candidate),
            changes.get("run", 12345), changes.get("attempt", 1), changes.get("helper", helper))

    def test_complete_exact_native_evidence_preserves_separate_scope(self):
        with tempfile.TemporaryDirectory() as temp:
            result = self.verify(self.fixture(Path(temp)))
            self.assertEqual(result["exactTests"], 15)
            self.assertFalse(result["canonicalCasePassClaimed"])
            self.assertFalse(result["fleetPassClaimed"])

    def test_wrong_candidate_build_attempt_or_helper_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = self.fixture(Path(temp))
            for changes in [{"candidate": {"source_sha": "b" * 40}}, {"run": 12346}, {"attempt": 2}, {"helper": "b" * 40}]:
                with self.subTest(changes=changes), self.assertRaises(ValueError): self.verify(fixture, **changes)

    def test_source_lock_collector_and_scope_drift_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = self.fixture(Path(temp)); path = fixture[0] / "manager-native.json"
            original = json.loads(path.read_text())
            for field, value in [("candidateSourceTree", "b" * 40), ("candidateCargoLockSha256", "b" * 64),
                                 ("sourceFiles", {}), ("collectorSources", {}), ("regressions", {}),
                                 ("sourceUnmodified", False), ("optimized", False), ("fleetPassClaimed", True),
                                 ("buildEnvironment", {}), ("binaryName", "../foreign")]:
                path.write_text(json.dumps({**original, field: value}))
                with self.subTest(field=field), self.assertRaises(ValueError): self.verify(fixture)

    def test_changed_or_symlinked_binary_is_rejected(self):
        for changed in ["bytes", "symlink"]:
            with tempfile.TemporaryDirectory() as temp:
                fixture = self.fixture(Path(temp)); binary = fixture[0] / "heal-manager-native-tests"
                if changed == "bytes": binary.write_bytes(b"modified")
                else:
                    original = fixture[0] / "original"; binary.rename(original); binary.symlink_to(original)
                with self.subTest(changed=changed), self.assertRaises(ValueError): self.verify(fixture)

    def test_zero_ignored_failed_or_missing_original_test_never_passes(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = self.fixture(Path(temp)); key = next(iter(suite.REGRESSIONS)); path = fixture[1] / (key + ".log")
            original = path.read_text()
            for output in ["running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored;\n",
                           original.replace(" ... ok", " ... ignored").replace("1 passed; 0 failed; 0 ignored", "0 passed; 0 failed; 1 ignored"),
                           original.replace(" ... ok", " ... FAILED"), original.replace("running 1 test", "running 2 tests")]:
                path.write_text(output)
                with self.assertRaises(ValueError): self.verify(fixture)

    def test_missing_duplicate_or_scope_changed_results_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = self.fixture(Path(temp)); path = fixture[1] / "manager-native-summary.json"
            original = json.loads(path.read_text())
            for changed in ["missing", "duplicate", "scope", "power", "fleet"]:
                value = copy.deepcopy(original)
                if changed == "missing": value["results"].pop()
                elif changed == "duplicate": value["results"][-1] = value["results"][0]
                elif changed == "scope": value["results"][0]["emittedBarrierLogsClaimed"] = True
                elif changed == "power": value["physicalPowerLossClaimed"] = True
                else: value["fleetPassClaimed"] = True
                path.write_text(json.dumps(value))
                with self.subTest(changed=changed), self.assertRaises(ValueError): self.verify(fixture)

    def test_listing_and_invocation_require_the_complete_exact_test_name(self):
        for changed in ["missing", "duplicate", "filter", "parallel", "wrong-name"]:
            with tempfile.TemporaryDirectory() as temp:
                fixture = self.fixture(Path(temp)); key = next(iter(suite.REGRESSIONS))
                if changed in {"missing", "duplicate"}:
                    path = fixture[1] / "runtime-inventory.log"; listing = path.read_text()
                    path.write_text(listing.replace(suite.REGRESSIONS[key]["test"] + ": test", "") if changed == "missing"
                                    else listing + "\n" + suite.REGRESSIONS[key]["test"] + ": test")
                else:
                    path = fixture[1] / (key + ".json"); row = json.loads(path.read_text())
                    if changed == "filter": row["command"].remove("--exact")
                    elif changed == "parallel": row["command"][-1] = "--test-threads=8"
                    else: row["command"][1] = "checkpoint"
                    path.write_text(json.dumps(row))
                    summary_path = fixture[1] / "manager-native-summary.json"; summary = json.loads(summary_path.read_text())
                    summary["results"][0] = row; summary_path.write_text(json.dumps(summary))
                with self.subTest(changed=changed), self.assertRaises(ValueError): self.verify(fixture)


if __name__ == "__main__": unittest.main()
