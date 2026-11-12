"""Upgrade harness regression tests; no SSH, Docker or RustFS is contacted."""

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))
from scripts import functional_case_report as report
from scripts import functional_chain_evidence as chain
from scripts import upgrade_evidence as evidence
from scripts import issue_manager as issues


ROOT = Path(__file__).resolve().parent
SOURCE = (ROOT / "rustfs-upgrade-test.sh").read_text()
BASH = shutil.which("bash")
CANARY = "UPGRADE_DIAGNOSTIC_SECRET_CANARY"


def function(name):
    found = re.search(rf"(?ms)^{name}\(\) \{{.*?^\}}", SOURCE)
    if not found:
        raise AssertionError(f"real function {name} missing")
    return found.group(0)


class UpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def bash(self, body, **env):
        script = self.directory / "test.sh"
        script.write_text(body)
        child_env = {**os.environ, "UPGRADE_TEST_TOKEN": CANARY, **env}
        for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
            child_env.pop(name, None)
        return subprocess.run([BASH, str(script)], capture_output=True, text=True, timeout=45, env=child_env)

    def receipt(self, response, code=0, http=200, debug_extra=""):
        request = self.directory / "request.json"
        response_file = self.directory / "response.json"
        debug = self.directory / "debug.log"
        output = self.directory / "receipt.json"
        request.write_text(json.dumps({"Objects": [{"Key": "key"}], "Quiet": True}))
        response_file.write_text(response if isinstance(response, str) else json.dumps(response))
        debug.write_text(f'"POST /bucket?delete HTTP/1.1" {http} 0\nResponse headers: '
                         "{'x-amz-request-id': b'fixture-id'}\n" + debug_extra)
        with mock.patch.dict(os.environ, {"UPGRADE_TEST_TOKEN": CANARY}):
            valid = evidence.validate_delete(request, response_file, debug, code, output)
        self.assertNotIn(CANARY, output.read_text())
        return valid, json.loads(output.read_text())

    def test_delete_receipts_reject_failures_and_preserve_error_identity(self):
        for response in ("", {}, {"Deleted": [{"Key": "key"}]}):
            self.assertTrue(self.receipt(response)[0])
        for response, code, http in (({"Errors": [{"Key": "key", "Code": "ServiceUnavailable", "Message": CANARY}]}, 0, 200),
                                     ("", 255, 503), ("not json", 0, 200), ([], 0, 200), ({"Errors": {}}, 0, 200), ({}, 0, 503)):
            with self.subTest(response=response, code=code):
                valid, receipt = self.receipt(response, code, http)
                self.assertFalse(valid)
                self.assertEqual(receipt["http_status"], http)
                self.assertEqual(receipt["request_id"], "fixture-id")
        valid, receipt = self.receipt("", 255, 503, "Response body: b'<Error><Code>ServiceUnavailable</Code><Message>" + CANARY + "</Message><RequestId>fixture-id</RequestId></Error>'\n")
        self.assertFalse(valid)
        self.assertEqual(receipt["service_error"]["Code"], "ServiceUnavailable")

    def test_manifest_detects_replaced_keys_and_corrupt_contents(self):
        body, manifest, keys = (self.directory / name for name in ("body", "manifest.json", "keys.json"))
        body.write_bytes(b"baseline body")
        evidence.seed_record(manifest, "original", body)
        keys.write_text('["original"]')
        evidence.check_seed(manifest, keys)
        evidence.check_body(manifest, "original", body)
        keys.write_text('["replacement"]')
        with self.assertRaisesRegex(ValueError, "inventory"):
            evidence.check_seed(manifest, keys)
        body.write_bytes(b"corrupt! body")
        with self.assertRaisesRegex(ValueError, "content"):
            evidence.check_body(manifest, "original", body)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            evidence.seed_record(manifest, "original", body)

    def test_large_seed_is_bounded_and_does_not_call_warp_or_delete(self):
        body = "set -Eeuo pipefail\n" + function("seed_warp_bucket") + r'''
log() { :; }
die() { echo "$*" >&2; exit 1; }
new_bucket() { :; }
warp() { echo 'unexpected warp' >&2; exit 90; }
make_test_file() { printf '%s\n' "$2" >> "$TEST_RECORD"; printf fixture > "$1"; }
aws_run() { printf '%s\n' "$*" >> "$TEST_RECORD"; }
assert_aws_ok() { :; }
golden_set() { printf '%s %s\n' "$1" "$2" >> "$TEST_RECORD"; }
DRY_RUN=0
TEST_TMP="$TEST_DIRECTORY"
seed_warp_bucket upg-warp-100mib 100MiB 30s 2
'''
        record = self.directory / "calls"
        result = self.bash(body, TEST_RECORD=str(record), TEST_DIRECTORY=str(self.directory))
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = record.read_text()
        self.assertEqual(calls.count("s3api put-object"), 2)
        self.assertIn("104857600", calls)
        self.assertIn("warp:upg-warp-100mib 2", calls)
        self.assertNotIn("delete-objects", calls)

    def lifecycle(self, mode="pass", extra=()):
        artifacts = self.directory / "artifacts"
        # Source all real lifecycle/report functions; every external boundary
        # is stubbed to forbid accidental access to the shared environment.
        source = SOURCE.removesuffix('main "$@"\n')
        stubs = r'''
SCRIPT_DIR="$TEST_ROOT"
UPGRADE_PYTHON="$TEST_PYTHON"
ssh() { echo 'unexpected SSH' >&2; exit 90; }
docker() { echo 'unexpected Docker' >&2; exit 90; }
rc() { echo 'unexpected rc' >&2; exit 90; }
aws() { echo 'unexpected aws' >&2; exit 90; }
warp() { echo 'unexpected warp' >&2; exit 90; }
need_cmd() {
  if [ "$TEST_MODE" = preflight ] && [ "$1" = aws ]; then die 'fixture missing AWS CLI'; fi
  if [ "$TEST_MODE" = preflight-python ] && [ "$1" = "$UPGRADE_PYTHON" ]; then
    UPGRADE_PYTHON=fixture-nonexistent-python
    die 'fixture missing Python'
  fi
}
vault_stop() {
  printf 'vault-stop\n' >> "$TEST_TRACE"
  if [ "$TEST_MODE" = vault-cleanup ] && [ "$CURRENT_PHASE" = CLEANUP ]; then return 1; fi
}
vault_start() { :; }
env_setup_old() { OLD_VERSION_ACTUAL=old; }
seed_golden() {
  if [ "$TEST_MODE" = prepare ] && [ "$KMS_BACKEND" = vault-kv2 ]; then die 'fixture seed failure'; fi
  if [ "$TEST_MODE" = interrupted ]; then kill -TERM "$$"; fi
}
upgrade_in_place() {
  [ "$TEST_MODE" != upgrade ] || die 'fixture upgrade failure'
  NEW_VERSION_ACTUAL=new
}
collect_upgrade_diagnostics() {
  printf 'diagnostics %s/%s\n' "$CURRENT_TOPO" "$KMS_BACKEND" >> "$TEST_TRACE"
  printf 'private value %s\n' "$UPGRADE_TEST_TOKEN" > "$TEST_TMP/fixture.log"
  sanitize_upgrade_log "$TEST_TMP/fixture.log" "$GROUP_DIR/diagnostics.log"
  [ "$TEST_MODE" != diagnostics ]
}
cleanup_node() {
  printf 'cleanup %s\n' "$1" >> "$TEST_TRACE"
  [ "$TEST_MODE" != cleanup ]
}
fixture_verify() {
  printf 'case value %s\n' "$UPGRADE_TEST_TOKEN"
  [ "$TEST_MODE" != verify ] || [ "$CURRENT_CASE" != UPG-101 ]
}
run_verify_cases() {
  local i
  for i in "${!UPGRADE_IDS[@]}"; do
    CURRENT_CASE="${UPGRADE_IDS[i]}"
    run_test "${UPGRADE_IDS[i]}" "${UPGRADE_NAMES[i]}" fixture_verify
  done
}
main --all-topologies -y --artifacts-dir "$TEST_ARTIFACTS" "$@"
'''
        script = self.directory / "lifecycle.sh"
        script.write_text(source + stubs)
        trace = self.directory / "trace"
        result = subprocess.run([BASH, str(script), *extra], capture_output=True, text=True, timeout=30,
                                env={**os.environ, "TEST_ROOT": str(ROOT), "TEST_PYTHON": sys.executable,
                                     "TEST_MODE": mode, "TEST_ARTIFACTS": str(artifacts), "TEST_TRACE": str(trace),
                                     "UPGRADE_TEST_TOKEN": CANARY, "TMPDIR": str(self.directory)})
        self.assertNotIn(CANARY, result.stdout + result.stderr)
        self.assertFalse(list(self.directory.glob("rustfs-upgrade.*")), "private scratch must be removed")
        for path in artifacts.rglob("*"):
            if path.is_file():
                self.assertNotIn(CANARY, path.read_text())
        results = json.loads((artifacts / "results.json").read_text()) if (artifacts / "results.json").exists() else None
        return result, results, trace.read_text() if trace.exists() else ""

    def test_complete_matrix_and_preparation_failure(self):
        result, data, trace = self.lifecycle("pass")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(data["status"], "PASS")
        self.assertEqual(data["counts"]["PASS"], 108)
        self.assertEqual(len(data["groups"]), 6)
        self.assertLess(trace.index("diagnostics"), trace.index("cleanup"))
        shutil.rmtree(self.directory / "artifacts")
        result, data, trace = self.lifecycle("prepare")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(data["counts"]["PASS"], 54)
        self.assertEqual(data["counts"]["NOT_RUN"], 54)
        self.assertEqual(data["counts"]["FAIL"], 0)
        self.assertEqual([g["result"] for g in data["groups"]], ["PASS"] * 3 + ["PREPARE_FAILED", "NOT_RUN", "NOT_RUN"])
        self.assertEqual(len(data["cases"]), 108)
        parsed = issues.parse_cases(self.directory / "artifacts/cases.md", None)
        self.assertTrue(all(row["statuses"] == {"PASS", "NOT_RUN"} for row in parsed.values()))
        with mock.patch.object(issues, "search_open") as search:
            issues.handle_success(mock.Mock(), parsed)
            search.assert_not_called()
        self.assertEqual(issues.classify("failure", parsed, None)[0], "env")
        with self.assertRaises(ValueError):
            chain.verify_report("upgrade", self.directory / "artifacts/cases.md")

    def test_upgrade_interruption_and_recovery_failures_never_pass(self):
        for mode, state in (("upgrade", "UPGRADE_FAILED"), ("interrupted", "INTERRUPTED"),
                            ("cleanup", "CLEANUP_FAILED"), ("diagnostics", "DIAGNOSTICS_FAILED")):
            with self.subTest(mode=mode):
                shutil.rmtree(self.directory / "artifacts", ignore_errors=True)
                result, data, trace = self.lifecycle(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(data["groups"][0]["result"], state)
                self.assertEqual(data["groups"][1]["result"], "NOT_RUN")
                self.assertEqual(len(data["cases"]), 108)
                self.assertIn("cleanup vm003", trace, "teardown attempts all nodes even after failure")

    def test_failed_assertions_keep_all_cases_and_continue_after_cleanup(self):
        result, data, _ = self.lifecycle("verify")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(data["counts"]["FAIL"], 6)
        self.assertEqual(data["counts"]["PASS"], 102)
        self.assertEqual(data["counts"]["NOT_RUN"], 0)
        self.assertTrue(all(g["result"] == "VERIFY_FAILED" for g in data["groups"]))

    def test_keep_environment_and_existing_artifact_directory(self):
        result, data, trace = self.lifecycle(extra=("--keep-env",))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("cleanup vm", trace)
        self.assertTrue(all(g["cleanup"] == "RETAINED" for g in data["groups"]))
        old_report = (self.directory / "artifacts/results.json").read_text()
        (self.directory / "trace").unlink()
        result, _, trace = self.lifecycle()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(trace, "")
        self.assertEqual((self.directory / "artifacts/results.json").read_text(), old_report)

    def test_preflight_records_the_plan_without_touching_environment(self):
        for mode in ("preflight", "preflight-python"):
            with self.subTest(mode=mode):
                shutil.rmtree(self.directory / "artifacts", ignore_errors=True)
                result, data, trace = self.lifecycle(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(trace, "")
                artifacts = self.directory / "artifacts"
                self.assertEqual(len((artifacts / "plan.tsv").read_text().splitlines()), 6)
                self.assertEqual((artifacts / "fleet-cleanup.state").read_text().strip(), "NOT_TOUCHED")
                log = (artifacts / "events.log").read_text()
                self.assertEqual(log.count("[NOT_RUN]"), 108)
                self.assertIn("PREPARE_FAILED", log)
                if mode == "preflight-python":
                    self.assertIsNone(data)
                    self.assertFalse(report.generate_report(artifacts / "events.log", artifacts / "cases.md",
                                                           artifacts / "matrix.md", artifacts / "results.json"))
                    data = json.loads((artifacts / "results.json").read_text())
                self.assertEqual(data["counts"]["NOT_RUN"], 108)

    def test_cleanup_receipt_blocks_release_for_nodes_and_vault(self):
        for mode in ("pass", "cleanup", "vault-cleanup"):
            with self.subTest(mode=mode):
                shutil.rmtree(self.directory / "artifacts", ignore_errors=True)
                _, _, _ = self.lifecycle(mode)
                path = self.directory / "artifacts/fleet-cleanup.state"
                if mode == "pass":
                    evidence.check_cleanup(path)
                else:
                    self.assertEqual(path.read_text().strip(), "FAIL")
                    with self.assertRaises(ValueError):
                        evidence.check_cleanup(path)
        for state in ("PENDING", "RETAINED", "", "garbage"):
            path.write_text(state)
            with self.assertRaises(ValueError):
                evidence.check_cleanup(path)
        path.unlink()
        with self.assertRaises(OSError):
            evidence.check_cleanup(path)

    def test_workflow_release_gate_leaves_fleet_quarantined(self):
        workflow = (ROOT / ".github/workflows/rustfs-upgrade-test.yml").read_text()
        gate = workflow.split("      - name: Release environment only after successful cleanup", 1)[1].split("      - name:", 1)[0]
        block = gate.split("        run: |\n", 1)[1]
        command = "\n".join(line[10:] for line in block.splitlines())
        state = self.directory / "upgrade/fleet-cleanup.state"
        state.parent.mkdir()
        # Execute the actual workflow gate through the real transition function
        # with node operations replaced by an in-memory quarantine fixture.
        script = self.directory / "python3"
        script.write_text(f'''#!{sys.executable}
import json,os,sys
from pathlib import Path
sys.path.insert(0, {str(ROOT / 'scripts')!r})
import functional_environment as fleet
if sys.argv[1].endswith('functional_environment.py'):
    calls=[]
    try:
        fleet.transition('finish','upgrade',os.environ,sys.argv[-1],execute=lambda *args:calls.append(args))
    except ValueError:
        Path(os.environ['TEST_GATE_CALLS']).write_text(json.dumps(calls))
        raise SystemExit(1)
    Path(os.environ['TEST_GATE_CALLS']).write_text(json.dumps(calls))
else:
    os.execv(sys.executable,[sys.executable,*sys.argv[1:]])
''')
        script.chmod(0o755)
        for clean in ("PASS", "FAIL", "PENDING", None):
            if clean is None:
                state.unlink()
            else:
                state.write_text(clean)
            result = self.bash(command, PATH=str(self.directory) + os.pathsep + os.environ["PATH"],
                               FUNCTIONAL_ARTIFACTS_DIR=str(self.directory), CLEANUP_OUTCOME="success",
                               GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="1", RUSTFS_NODES="vm000 vm001 vm002 vm003",
                               FUNCTIONAL_ENVIRONMENT_RECEIPT=str(self.directory / "ownership.json"),
                               TEST_GATE_CALLS=str(self.directory / "calls.json"))
            receipt = json.loads((self.directory / "ownership.json").read_text())
            calls = json.loads((self.directory / "calls.json").read_text())
            if clean == "PASS":
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(receipt["state"], "clean")
                self.assertEqual(receipt["completed_nodes"], ["vm000", "vm001", "vm002", "vm003"])
            else:
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["state"], "quarantined")
                self.assertEqual(calls, [])

    def test_report_legacy_context_and_missing_cleanup_evidence(self):
        log, cases, matrix = (self.directory / name for name in ("log", "cases.md", "matrix.md"))
        log.write_text("[INFO] == topology: single kms-backend: local ==\n--- UPG-101 case ---\n[PASS] UPG-101\n[UPG-TOPO] single local old new PASS=1 FAIL=0\n")
        self.assertTrue(report.generate_report(log, cases, matrix))
        log.write_text(log.read_text() + "[UPG-STATE] single local old new PASS\n")
        self.assertFalse(report.generate_report(log, cases, matrix))

    def test_truncated_or_duplicate_matrix_never_reports_success(self):
        artifacts = self.directory / "artifacts"
        _, data, _ = self.lifecycle()
        log = artifacts / "events.log"
        original = log.read_text()
        second = original.index("[INFO] == topology: single-multi")
        last_state = original.rindex("[UPG-STATE]")
        mutations = {
            "later prepare": original[:second],
            "last cleanup": original[:last_state],
            "all missing states": "\n".join(line for line in original.splitlines() if not line.startswith("[UPG-STATE]")),
            "missing diagnostic": "\n".join(line for line in original.splitlines() if not line.startswith("[UPG-DIAGNOSTICS]")),
            "missing cleanup": "\n".join(line for line in original.splitlines() if not line.startswith("[UPG-CLEANUP]")),
            "duplicate group": original + "[UPG-STATE] single-single local old new PASS\n",
            "duplicate case": original + "--- UPG-101 duplicate ---\n[PASS] UPG-101\n",
            "truncated plan": original[:original.index("[UPG-PLAN-END]")],
        }
        for label, text in mutations.items():
            with self.subTest(label=label):
                log.write_text(text)
                self.assertFalse(report.generate_report(log, artifacts / "cases.md", artifacts / "matrix.md",
                                                       artifacts / "results.json"))
                data = json.loads((artifacts / "results.json").read_text())
                self.assertEqual(data["status"], "FAIL")
                self.assertGreaterEqual(len(data["cases"]), 108)
        log.write_text(original)
        self.assertTrue(report.generate_report(log, artifacts / "cases.md", artifacts / "matrix.md"))
        # With no sidecar, only a completely bounded embedded plan is usable.
        standalone = self.directory / "standalone.log"
        standalone.write_text(original)
        self.assertTrue(report.generate_report(standalone, artifacts / "cases.md", artifacts / "matrix.md"))
        standalone.write_text(original.replace("[UPG-PLAN-END] 6", ""))
        self.assertFalse(report.generate_report(standalone, artifacts / "cases.md", artifacts / "matrix.md"))

    def test_diagnostics_archive_is_sanitized_even_when_ssh_fails(self):
        golden = self.directory / "golden.tsv"
        golden.write_text("creds:sa\tsa-access-canary|sa-secret-canary\n")
        body = "set -Eeuo pipefail\n" + "\n".join(function(name) for name in ("collect_upgrade_diagnostics", "sanitize_upgrade_log")) + r'''
TEST_TMP="$TEST_DIRECTORY"
GROUP_DIR="$TEST_DIRECTORY"
GOLDEN_FILE="$TEST_DIRECTORY/golden.tsv"
SCRIPT_DIR="$TEST_ROOT"
UPGRADE_PYTHON="$TEST_PYTHON"
VAULT_TOKEN="$UPGRADE_TEST_TOKEN"
KMS_LOCAL_MASTER_KEY="$UPGRADE_TEST_TOKEN"
KMS_BACKUP_KEK="$UPGRADE_TEST_TOKEN"
ACCESS_KEY=fixture-access
SECRET_KEY="$UPGRADE_TEST_TOKEN"
DIAGNOSTICS_DONE=0
DIAGNOSTICS_STATUS=NOT_RUN
DRY_RUN=0
CURRENT_TOPO=multi-multi
SSH_OPTS=()
SSH_USER=fixture
topology_nodes() { echo 'node1 node2'; }
timeout() { shift; "$@"; }
ssh() {
  cat > /dev/null
  printf '{"journal": "credential %s sa-secret-canary", "file_logs": [{"content": "sa-access-canary"}]}\n' "$UPGRADE_TEST_TOKEN"
  return 1
}
printf 'warp value %s\n' "$UPGRADE_TEST_TOKEN" > "$TEST_TMP/warp-put-test.log"
collect_upgrade_diagnostics
'''
        result = self.bash(body, TEST_DIRECTORY=str(self.directory), TEST_ROOT=str(ROOT), TEST_PYTHON=sys.executable)
        self.assertEqual(result.returncode, 1)
        for name in ("diagnostics-node1.log", "diagnostics-node2.log", "warp-put-test.log"):
            text = (self.directory / name).read_text()
            for secret in (CANARY, "sa-access-canary", "sa-secret-canary"):
                self.assertNotIn(secret, text)
            self.assertIn("REDACTED", text)

    def test_unknown_quoted_credentials_and_generic_tokens_are_redacted(self):
        unknown = "UNKNOWN_NEW_CREDENTIAL_CANARY"
        for key in ("secretKey", "secret_key", "SecretAccessKey", "token", "root_token", "sessionToken",
                    "vaultToken", "password", "masterKey", "accessKeyId", "Authorization"):
            for line in (f'journal prefix {{"{key}":"{unknown}"}}',
                         f'journal prefix {{\\"{key}\\":\\"{unknown}\\"}}',
                         f'{key}={unknown}', f"'{key}': '{unknown}'"):
                with self.subTest(key=key, line=line):
                    self.assertNotIn(unknown, evidence.redact(line))
        raw = self.directory / "raw.log"
        safe = self.directory / "safe.log"
        raw.write_text(json.dumps({"journal": f'prefix {{"token":"{unknown}"}}',
                                   "file_logs": [{"content": f'{{"secretKey":"{unknown}"}}'}]}))
        result = subprocess.run([sys.executable, str(ROOT / "scripts/upgrade_evidence.py"), "sanitize",
                                 "--input", str(raw), "--output", str(safe)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(unknown, safe.read_text())

    def test_interrupted_diagnostics_do_not_become_pass_on_exit_reentry(self):
        source = SOURCE.removesuffix('main "$@"\n')
        body = source + r'''
SCRIPT_DIR="$TEST_ROOT"
UPGRADE_PYTHON="$TEST_PYTHON"
ARTIFACTS_DIR="$TEST_DIRECTORY/artifacts"
mkdir "$ARTIFACTS_DIR"
GROUP_DIR="$ARTIFACTS_DIR/current"
mkdir "$GROUP_DIR"
ARTIFACTS_READY=1
: > "$ARTIFACTS_DIR/events.log"
UPGRADE_GROUPS=(multi-multi/local)
GROUP_ACTIVE=1
CURRENT_TOPO=multi-multi
KMS_BACKEND=local
CURRENT_PHASE=CLEANUP
CURRENT_RESULT=PASS
DIAGNOSTICS_DONE=0
DIAGNOSTICS_STATUS=NOT_RUN
topology_nodes() { echo 'node1 node2'; }
timeout() { shift; "$@"; }
ssh() {
  cat > /dev/null
  printf 'first-node\n' >> "$TEST_DIRECTORY/trace"
  kill -TERM "$$"
}
env_teardown() { printf 'cleanup\n' >> "$TEST_DIRECTORY/trace"; }
record_upgrade_cleanup PENDING
finish_upgrade_group
'''
        result = self.bash(body, TEST_DIRECTORY=str(self.directory), TEST_ROOT=str(ROOT), TEST_PYTHON=sys.executable,
                           TMPDIR=str(self.directory))
        self.assertNotEqual(result.returncode, 0)
        log = (self.directory / "artifacts/events.log").read_text()
        self.assertIn("[UPG-DIAGNOSTICS] multi-multi local FAIL", log)
        self.assertNotIn("[UPG-DIAGNOSTICS] multi-multi local PASS", log)
        self.assertIn("INTERRUPTED", log)
        self.assertEqual((self.directory / "trace").read_text(), "first-node\ncleanup\n")
        self.assertFalse(list(self.directory.glob("rustfs-upgrade.*")))

    def test_real_seed_verifier_compares_baseline_beyond_etags(self):
        body = "set -Eeuo pipefail\n" + "\n".join(function(name) for name in ("capture_seed_manifest", "verify_warp_bucket", "assert_aws_ok")) + r'''
TEST_TMP="$TEST_DIRECTORY"
GROUP_DIR="$TEST_DIRECTORY"
SCRIPT_DIR="$TEST_ROOT"
UPGRADE_PYTHON="$TEST_PYTHON"
DRY_RUN=0
log() { :; }
die() { echo "$*" >&2; exit 1; }
golden_get() { echo 2; }
aws_run() {
  AWS_CODE=0
  case "$2" in
    list-objects-v2)
      if [[ "$*" == *'length(Contents)'* ]]; then AWS_OUT=2
      elif [ "${VERIFY_MODE:-}" = replace ]; then AWS_OUT='["one", "replacement"]'
      else AWS_OUT='["one", "two"]'; fi ;;
    get-object)
      if [ "${VERIFY_MODE:-}" = corrupt ]; then printf changed > "${@: -1}"
      else printf baseline > "${@: -1}"; fi
      AWS_OUT='{"ETag":"same-etag"}' ;;
    head-object) AWS_OUT='{"ETag":"same-etag"}' ;;
    *) exit 90 ;;
  esac
}
capture_seed_manifest fixture
verify_warp_bucket fixture
VERIFY_MODE=replace
if verify_warp_bucket fixture; then exit 91; fi
VERIFY_MODE=corrupt
if verify_warp_bucket fixture; then exit 92; fi
'''
        result = self.bash(body, TEST_DIRECTORY=str(self.directory), TEST_ROOT=str(ROOT), TEST_PYTHON=sys.executable)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("inventory changed", result.stdout)
        self.assertIn("content changed", result.stdout)

    def test_dry_run_skips_every_environment_boundary(self):
        log = self.directory / "prior.log"
        log.write_text("prior test evidence\n")
        result, data, trace = self.lifecycle(extra=("--dry-run", "--log-file", str(log)))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIsNone(data)
        self.assertEqual(trace, "")
        self.assertEqual(result.stdout.count("[DRY-RUN] UPG-"), 108)
        self.assertNotIn("[PASS]", result.stdout)
        self.assertEqual(log.read_text(), "prior test evidence\n")


@unittest.skipUnless(shutil.which("aws") and shutil.which("jq"), "real AWS CLI and jq required")
class DeleteProtocolTests(unittest.TestCase):
    setUp = UpgradeTests.setUp
    bash = UpgradeTests.bash
    # These tests use the real CLI against a loopback S3 XML fixture. Warp
    # uploads are stubbed; they do not claim product/VM verification.
    def test_real_cli_quiet_errors_and_batching(self):
        for mode in ("success", "errors", "partial", "http503", "jqfail"):
            with self.subTest(mode=mode):
                directory = self.directory / mode
                directory.mkdir()
                original = ['alpha/001', '/leading', 'quote"and\\slash', '中文/key'] + [f"key-{i}" for i in range(2501)]
                state = {"keys": original.copy(), "batches": []}

                class Handler(BaseHTTPRequestHandler):
                    def log_message(self, *_):
                        pass

                    def xml(self, status, value):
                        body = value.encode()
                        self.send_response(status)
                        self.send_header("Content-Length", str(len(body)))
                        self.send_header("Content-Type", "application/xml")
                        self.send_header("x-amz-request-id", "fixture-delete-id")
                        self.end_headers()
                        self.wfile.write(body)

                    def do_PUT(self):
                        self.rfile.read(int(self.headers.get("Content-Length", 0)))
                        self.xml(200, "")

                    def do_GET(self):
                        contents = "".join(f'<Contents><Key>{escape(key)}</Key><Size>1024</Size><LastModified>2026-01-01T00:00:00Z</LastModified><ETag>"fixture"</ETag></Contents>' for key in state["keys"])
                        self.xml(200, f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>upg-fixture</Name><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>')

                    def do_POST(self):
                        root = ET.fromstring(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                        keys = [e.text for e in root.iter() if e.tag.rsplit("}", 1)[-1] == "Key"]
                        state["batches"].append(keys)
                        if mode == "http503":
                            self.xml(503, '<Error><Code>ServiceUnavailable</Code><Message>The service is unavailable</Message><RequestId>fixture-delete-id</RequestId></Error>')
                            return
                        failed = keys if mode == "errors" else keys[:1] if mode == "partial" else []
                        state["keys"] = [key for key in state["keys"] if key not in keys or key in failed]
                        errors = "".join(f'<Error><Key>{escape(key)}</Key><Code>ServiceUnavailable</Code><Message>{CANARY}</Message></Error>' for key in failed)
                        self.xml(200, f'<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{errors}</DeleteResult>')

                server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                worker = threading.Thread(target=server.serve_forever, daemon=True)
                worker.start()
                variables = dict(TEST_TMP=str(directory), GOLDEN_FILE=str(directory / "golden.tsv"), GROUP_DIR=str(directory),
                                 SCRIPT_DIR=str(ROOT), UPGRADE_PYTHON=sys.executable, JQ_LIB=str(ROOT / "scripts/lib"),
                                 ENDPOINT=f"http://127.0.0.1:{server.server_port}", REGION="us-east-1", DRY_RUN="0", DELETE_BATCH="0",
                                 ACCESS_KEY="fixture-access", SECRET_KEY=CANARY, VAULT_TOKEN=CANARY,
                                 KMS_LOCAL_MASTER_KEY=CANARY, KMS_BACKUP_KEK=CANARY)
                body = "set -Eeuo pipefail\n" + "".join(f"{name}={shlex.quote(value)}\n" for name, value in variables.items())
                body += "\n".join(function(name) for name in ("aws_run", "assert_aws_ok", "new_bucket", "golden_set", "seed_warp_bucket"))
                body += '\nlog() { :; }\ndie() { echo "$*" >&2; exit 1; }\nwarp() { :; }\n'
                if mode == "jqfail":
                    body += 'jq() { if [[ "$*" == *--argjson* ]]; then return 42; else command jq "$@"; fi; }\n'
                body += 'seed_warp_bucket upg-fixture 1KiB 1s 2\n'
                env = {"AWS_ACCESS_KEY_ID": "fixture-access", "AWS_SECRET_ACCESS_KEY": CANARY,
                       "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
                       "AWS_EC2_METADATA_DISABLED": "true", "AWS_PAGER": "", "AWS_MAX_ATTEMPTS": "1",
                       "NO_PROXY": "*", "no_proxy": "*", "AWS_SESSION_TOKEN": "", "AWS_PROFILE": "", "AWS_DEFAULT_PROFILE": ""}
                try:
                    result = self.bash(body, **env)
                finally:
                    server.shutdown()
                    server.server_close()
                    worker.join()
                receipts = [json.loads(path.read_text()) for path in sorted(directory.glob("delete-upg-fixture-*.json"))]
                self.assertNotIn(CANARY, result.stdout + result.stderr + json.dumps(receipts))
                if mode == "success":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual([len(keys) for keys in state["batches"]], [1000, 1000, 503])
                    self.assertEqual(state["keys"], original[-2:])
                    self.assertTrue(all(receipt["valid"] for receipt in receipts))
                elif mode == "jqfail":
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(state["batches"], [])
                    self.assertIn("payload generation failed", result.stderr)
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(len(state["batches"]), 1, "stop on first failing batch")
                    self.assertFalse(receipts[0]["valid"])
                    self.assertEqual(receipts[0]["request_id"], "fixture-delete-id")
                    if mode == "http503":
                        self.assertEqual(receipts[0]["http_status"], 503)
                        self.assertEqual(receipts[0]["service_error"]["Code"], "ServiceUnavailable")
                    else:
                        self.assertEqual(receipts[0]["http_status"], 200)
                        self.assertEqual(receipts[0]["error_count"], 1000 if mode == "errors" else 1)
                        self.assertEqual(receipts[0]["response"]["Errors"][0]["Code"], "ServiceUnavailable")


if __name__ == "__main__":
    unittest.main()
