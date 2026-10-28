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

    def test_scenario_manifest_counts_are_unique(self) -> None:
        result = self.run_bash(
            """
            register_scenarios A B C C2 D
            printf '%s\\n' "${#EXPECTED_CASES[@]}" "$(printf '%s\\n' "${EXPECTED_CASES[@]}" | sort -u | wc -l)"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        # A=9, B=12 (B5 covers rustfs#8270), C=6, C2=6, D=6
        self.assertEqual(result.stdout.splitlines(), ["39", "39"])

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
        for scenario in ("=== A:", "=== B:", "=== C:", "=== C2:", "=== D:", "=== F:"):
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
        self.assertEqual(payload["counts"]["expected"], 39)
        self.assertEqual(payload["counts"]["seen"], 17)
        self.assertEqual(len(payload["missing_cases"]), 22)

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
        self.assertEqual(payload["counts"]["seen"], 39)

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
            await_write_quorum() { :; }
            capture_health() { :; }
            expect_read() { :; }
            expect_write() { printf '%s=%s\n' "$1" "$3"; }
            do_cleanup() { :; }
            scenario_c
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("C1/write=ok", result.stdout)

    def test_recovery_probes_wait_for_write_quorum_after_restart(self) -> None:
        # rustfs/backlog#2618: probing right after start_nodes measures the
        # drive re-admission hysteresis instead of the EC contract.
        result = self.run_bash(
            """
            install_and_start() { :; }
            seed_baseline() { :; }
            stop_node() { :; }
            start_nodes() { echo start; }
            settle() { :; }
            await_write_quorum() { echo "await $1"; }
            capture_health() { :; }
            expect_read() { echo "probe $1"; }
            expect_write() { echo "probe $1"; }
            do_cleanup() { :; }
            scenario_b; scenario_c; scenario_c2; scenario_d; scenario_e
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        for label, first_probe in (
            ("B4", "B4/read"),
            ("C3", "C3/read"),
            ("C2-3", "C2-3/read"),
            ("D3", "D3/read"),
            ("E3", "E3/read"),
        ):
            probe = lines.index(f"probe {first_probe}")
            self.assertEqual(lines[probe - 1], f"await {label}", lines)
            self.assertIn("start", lines[:probe])

    def await_with_health(self, responses: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
        return self.run_bash(
            f"""
            RECOVERY_TIMEOUT_SECS={timeout}
            printf '%s\\n' {" ".join(repr(r) for r in responses)} > "${{TMP}}/bodies"
            curl() {{
              local n; n=$(( $(cat "${{TMP}}/n" 2>/dev/null || echo 0) + 1 ))
              echo "$n" > "${{TMP}}/n"
              sed -n "${{n}}p" "${{TMP}}/bodies"
            }}
            sleep() {{ :; }}
            settle() {{ echo settled; }}
            await_write_quorum C3 http://example.invalid
            echo "polls=$(cat "${{TMP}}/n")"
            echo "evidence=$(cat "${{EVIDENCE}}/recovery-C3.secs" 2>/dev/null || echo none)"
            """
        )

    def test_await_write_quorum_polls_until_peers_are_readmitted(self) -> None:
        waiting = '{"details":{"storage":{"writeQuorum":false},"lock":{"ready":true}}}'
        lock_down = '{"details":{"storage":{"writeQuorum":true},"lock":{"ready":false}}}'
        ready = '{"details":{"storage":{"writeQuorum":true},"lock":{"ready":true}}}'
        result = self.await_with_health([waiting, lock_down, ready])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("polls=3", result.stdout)
        self.assertIn("write quorum restored", result.stdout)
        self.assertRegex(result.stdout, r"evidence=\d+")
        self.assertNotIn("settled", result.stdout)

    def test_await_write_quorum_timeout_is_recorded_not_fatal(self) -> None:
        waiting = '{"details":{"storage":{"writeQuorum":false}}}'
        result = self.await_with_health([waiting], timeout=0)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("evidence=timeout", result.stdout)
        self.assertIn("not restored within 0s", result.stderr)

    def test_await_write_quorum_falls_back_to_settle_without_readiness_details(self) -> None:
        result = self.await_with_health(['{"status":"ok"}'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("settled", result.stdout)
        self.assertIn("evidence=none", result.stdout)

    def test_matrix_topologies_are_single_pool_expressions(self) -> None:
        # Space-separated per-node ellipses would make each node its own
        # server pool (rustfs/backlog#2618); one expression keeps one pool.
        result = self.run_bash(
            """
            for spec in "1 4" "4 1" "4 2" "4 4"; do matrix_volumes_expr $spec; echo; done
            printf '%s\n' "${VOLUMES_SNMD}" "${VOLUMES_4X1}" "${VOLUMES_4X4}"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(lines[0], lines[4])  # 1x4 == scenario A layout
        self.assertEqual(lines[1], lines[5])  # 4x1 == scenario B/D layout
        self.assertEqual(lines[3], lines[6])  # 4x4 == scenario C/C2 layout
        self.assertEqual(lines[2], "http://rustfs-node{1...4}:9000/data/rustfs{1...2}/mnmd")
        for line in lines[:4]:
            self.assertNotIn(" ", line)

    def test_matrix_applies_parity_and_product_write_quorum(self) -> None:
        result = self.run_bash(
            """
            install_and_start() { echo "env=$2"; }
            seed_baseline() { :; }
            stop_node() { :; }
            start_nodes() { :; }
            settle() { :; }
            await_write_quorum() { :; }
            expect_read() { echo "$1=$3"; }
            expect_write() { echo "$1=$3"; }
            do_cleanup() { :; }
            for parity in 1 2 3 4; do matrix_run_case 4x2 4 2 "${parity}"; done
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        out = result.stdout
        for parity in (1, 2, 3, 4):
            self.assertIn(f'env=RUSTFS_STORAGE_CLASS_STANDARD="EC:{parity}"', out)
        # 4x2 with one node stopped leaves 6 of 8 drives online.
        # writeq = data + (data == parity): EC1 7, EC2 6, EC3 5, EC4 5.
        self.assertIn("M-4x2EC1-n3/write=fail", out)
        self.assertIn("M-4x2EC2-n3/write=ok", out)
        self.assertIn("M-4x2EC3-n3/write=ok", out)
        self.assertIn("M-4x2EC4-n3/write=ok", out)
        # Two nodes stopped leaves 4 drives: only EC4 still meets readq 4.
        self.assertIn("M-4x2EC4-n2/read=ok", out)
        self.assertIn("M-4x2EC4-n2/write=fail", out)
        self.assertIn("M-4x2EC3-n2/read=fail", out)

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
