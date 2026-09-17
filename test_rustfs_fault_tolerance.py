"""Contract tests for the fault-tolerance shell harness."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "rustfs-fault-tolerance-test.sh"


class FaultToleranceHarnessTests(unittest.TestCase):
    def run_bash(self, body: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as directory:
            env = {**os.environ, "TMPDIR": directory}
            return subprocess.run(
                [
                    "bash",
                    "--noprofile",
                    "--norc",
                    "-c",
                    f"source {SCRIPT!s}\n{textwrap.dedent(body)}",
                ],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )

    def test_all_manifest_has_38_unique_cases(self) -> None:
        result = self.run_bash(
            """
            register_scenarios A B C C2 D
            printf '%s\n' "${#EXPECTED_CASES[@]}" "$(printf '%s\n' "${EXPECTED_CASES[@]}" | sort -u | wc -l)"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["38", "38"])

    def test_all_dry_run_reaches_every_scenario_without_remote_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [
                    str(SCRIPT),
                    "--all",
                    "--dry-run",
                    "-y",
                    "--package-url",
                    "https://example.invalid/rustfs.deb",
                ],
                cwd=ROOT,
                env={**os.environ, "TMPDIR": directory, "FT_SETTLE_SECS": "0"},
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        for scenario in ("=== A:", "=== B:", "=== C:", "=== C2:", "=== D:"):
            self.assertIn(scenario, result.stdout)
        self.assertIn("fault-tolerance dry-run complete", result.stdout)

    def test_results_reject_partial_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result_path = Path(directory) / "results.json"
            env = {**os.environ, "TMPDIR": directory, "RESULTS_FILE": str(result_path)}
            result = subprocess.run(
                [
                    "bash",
                    "--noprofile",
                    "--norc",
                    "-c",
                    f"""
                    source {SCRIPT!s}
                    register_scenarios A B C C2 D
                    for case_id in "${{EXPECTED_CASES[@]:0:17}}"; do
                      record pass "${{case_id}}" fixture >/dev/null
                    done
                    SUITE_FINISHED=1
                    write_results 17
                    """,
                ],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result_path.read_text())
        self.assertFalse(payload["complete"])
        self.assertEqual(payload["counts"]["expected"], 38)
        self.assertEqual(payload["counts"]["seen"], 17)
        self.assertEqual(len(payload["missing_cases"]), 21)

    def test_complete_results_require_every_unique_case(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result_path = Path(directory) / "results.json"
            env = {
                **os.environ,
                "TMPDIR": directory,
                "RESULTS_FILE": str(result_path),
                "AUTO_TESTING_SHA": "a" * 40,
            }
            result = subprocess.run(
                [
                    "bash",
                    "--noprofile",
                    "--norc",
                    "-c",
                    f"""
                    source {SCRIPT!s}
                    register_scenarios A B C C2 D
                    for case_id in "${{EXPECTED_CASES[@]}}"; do
                      record pass "${{case_id}}" fixture >/dev/null
                    done
                    SUITE_FINISHED=1
                    write_results 0
                    """,
                ],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result_path.read_text())
        self.assertTrue(payload["complete"])
        self.assertEqual(payload["selected_scenarios"], ["A", "B", "C", "C2", "D"])
        self.assertEqual(payload["auto_testing_sha"], "a" * 40)
        self.assertEqual(payload["counts"]["seen"], 38)

    def test_aws_cli_syntax_failure_is_a_harness_error(self) -> None:
        result = self.run_bash(
            """
            register_scenarios A
            s3op() { S3_RC=252; S3_CODE=unknown; }
            expect_read A1/read http://example.invalid ok ft-small-16k.bin
            printf '%s\n' "${HARNESS_ERRORS}" "$(jq -r '.verdict' "${RESULT_ROWS}")"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[-2:], ["1", "HARNESS_ERROR"])

    def test_get_object_uses_positional_output_and_checks_content(self) -> None:
        source = SCRIPT.read_text()
        self.assertNotIn("--outfile", source)
        result = self.run_bash(
            """
            register_scenarios A
            printf expected > "${TMP}/expected"
            printf corrupt > "${TMP}/actual"
            OBJECT_SHA256[ft-small-16k.bin]="$(sha256sum "${TMP}/expected" | awk '{print $1}')"
            s3op() {
              local output="${@: -1}"
              cp "${TMP}/actual" "${output}"
              S3_RC=0
              S3_CODE=""
            }
            expect_read A1/read http://example.invalid ok ft-small-16k.bin
            jq -r '.verdict + " " + .detail' "${RESULT_ROWS}"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("UNEXPECTED read ft-small-16k.bin: got=corrupt", result.stdout)

    def test_unrelated_s3_failure_cannot_satisfy_a_quorum_failure(self) -> None:
        result = self.run_bash(
            """
            register_scenarios A
            s3op() { S3_RC=254; S3_CODE=NoSuchKey; }
            expect_read A3/read http://example.invalid fail ft-big-8m.bin
            jq -r '.verdict' "${RESULT_ROWS}"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[-1], "UNEXPECTED")

    def test_c1_write_matches_product_quorum(self) -> None:
        result = self.run_bash(
            """
            install_and_start() { :; }
            seed_baseline() { :; }
            stop_node() { :; }
            start_nodes() { :; }
            settle() { :; }
            capture_health() { :; }
            expect_read() { :; }
            expect_write() { printf '%s=%s\n' "$1" "$3"; }
            do_cleanup() { :; }
            scenario_c
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("C1/write=ok", result.stdout)

    def test_fault_injection_reaches_mount_lease_and_recovery_is_nonblocking(self) -> None:
        result = self.run_bash(
            """
            calls="${TMP}/calls"
            run_remote() { printf '%s|%s\n' "$1" "$2" >> "${calls}"; }
            drop_drive /data/rustfs4/mnmd
            start_nodes node-a node-b
            cat "${calls}"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("chmod 000", result.stdout)
        self.assertNotIn("mount -t tmpfs", result.stdout)
        self.assertEqual(result.stdout.count("systemctl start --no-block rustfs"), 2)
        self.assertEqual(result.stdout.count("systemctl is-active --quiet rustfs"), 2)

    @unittest.skipUnless(Path("/proc/self/fd").is_dir() and os.geteuid() != 0, "requires non-root Linux")
    def test_permission_fault_applies_through_an_open_directory_lease(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sentinel = root / "sentinel"
            sentinel.write_text("fixture")
            lease = os.open(root, os.O_RDONLY)
            try:
                root.chmod(0)
                probe = subprocess.run(
                    ["cat", f"/proc/self/fd/{lease}/sentinel"],
                    pass_fds=(lease,),
                    capture_output=True,
                    text=True,
                )
            finally:
                root.chmod(0o700)
                os.close(lease)
        self.assertNotEqual(probe.returncode, 0)
        self.assertIn("Permission denied", probe.stderr)


if __name__ == "__main__":
    unittest.main()
