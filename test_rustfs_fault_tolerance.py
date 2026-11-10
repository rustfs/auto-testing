"""Contract tests for the fault-tolerance shell harness."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import textwrap
import threading
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
        self.assertEqual([int(line) for line in result.stdout.splitlines()], [39, 39])

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

    def await_with_health(self, responses: list[tuple[int, str]], timeout: int = 120) -> subprocess.CompletedProcess[str]:
        return self.run_bash(
            f"""
            RECOVERY_TIMEOUT_SECS={timeout}
            printf '%s\\n' {" ".join(shlex.quote(r) for _, r in responses)} > "${{TMP}}/bodies"
            printf '%s\\n' {" ".join(str(code) for code, _ in responses)} > "${{TMP}}/codes"
            curl() {{
              local n out headers
              n=$(( $(cat "${{TMP}}/n" 2>/dev/null || echo 0) + 1 ))
              echo "$n" > "${{TMP}}/n"
              while [ "$#" -gt 0 ]; do
                case "$1" in
                  -o) out="$2"; shift ;;
                  -D) headers="$2"; shift ;;
                  http:*) [ "$1" = http://example.invalid/minio/health/cluster ] || return 99 ;;
                esac
                shift
              done
              sed -n "${{n}}p" "${{TMP}}/bodies" > "$out"
              printf 'HTTP/1.1 fixture\\n' > "$headers"
              sed -n "${{n}}p" "${{TMP}}/codes"
            }}
            date() {{
              local t; t=$(cat "${{TMP}}/clock" 2>/dev/null || echo 0)
              echo "$((t + 1))" > "${{TMP}}/clock"
              echo "$t"
            }}
            sleep() {{ :; }}
            settle() {{ echo settled; }}
            await_write_quorum C3 http://example.invalid
            echo "polls=$(cat "${{TMP}}/n")"
            echo "evidence=$(cat "${{EVIDENCE}}/recovery-C3.secs" 2>/dev/null || echo none)"
            echo "state=${{RECOVERY_STATE[C3]}}"
            cat "${{EVIDENCE}}/recovery-C3/poll-1.code"
            """
        )

    def test_await_write_quorum_polls_until_peers_are_readmitted(self) -> None:
        ready = {"ready": True, "details": {"storage": {"ready": True, "writeQuorum": True,
                 "readinessScope": "write_quorum_and_pool_metadata"}, "lock": {"ready": True},
                 "iam": {"ready": True}, "poolMetadata": {"ready": True}}}
        responses = []
        for component in ("storage", "lock", "iam", "poolMetadata"):
            waiting = json.loads(json.dumps(ready))
            waiting["details"][component]["ready"] = False
            responses.append((200, json.dumps(waiting)))
        responses.append((200, json.dumps(ready)))
        result = self.await_with_health(responses)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("polls=5", result.stdout)
        self.assertIn("write quorum restored", result.stdout)
        self.assertRegex(result.stdout, r"evidence=\d+")
        self.assertNotIn("settled", result.stdout)

    def test_await_write_quorum_timeout_is_recorded_not_fatal(self) -> None:
        waiting = '{"details":{"storage":{"writeQuorum":false}}}'
        result = self.await_with_health([(503, waiting)], timeout=0)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("evidence=timeout", result.stdout)
        self.assertIn("not restored within 0s", result.stderr)

    def test_await_write_quorum_accepts_minimal_cluster_write_response(self) -> None:
        result = self.await_with_health([(200, '{"ready":true}')])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("state=ready", result.stdout)
        self.assertNotIn("settled", result.stdout)

    def test_await_write_quorum_rejects_a_response_after_the_deadline(self) -> None:
        result = self.await_with_health([(200, '{"ready":true}')], timeout=0)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("state=timeout", result.stdout)
        self.assertNotIn("restored after", result.stdout)

    def test_await_write_quorum_rejects_false_or_misleading_readiness(self) -> None:
        responses = [
            (503, '{"ready":true}'), (200, '{"ready":false}'),
            (200, '{"status":"ok"}'), (200, 'not JSON'), (0, ''),
            (200, '{"ready":"true"}'), (200, '{"ready":true,"details":{}}'),
        ]
        details = {"ready": True, "details": {"storage": {"ready": True},
                   "iam": {"ready": True}, "lock": {"ready": True}}}
        for field, value in (("writeQuorum", False), ("readinessScope", "read_quorum")):
            body = json.loads(json.dumps(details))
            body["details"]["storage"][field] = value
            responses.append((200, json.dumps(body)))
        for response in responses:
            with self.subTest(response=response):
                result = self.await_with_health([response, (200, '{"ready":true}')], timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("state=ready", result.stdout)
                self.assertIn("polls=2", result.stdout)
                self.assertNotIn("settled", result.stdout)

    def test_recovery_timeout_cannot_pass_when_the_next_write_succeeds(self) -> None:
        result = self.run_bash(
            """
            register_scenarios C
            RECOVERY_STATE[C3]=timeout
            s3op() { S3_RC=0; S3_CODE=""; }
            expect_write C3/write http://example.invalid ok restored
            jq -r '.verdict + " " + .detail' "${RESULT_ROWS}"
            """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("UNEXPECTED write restored: got=ok", result.stdout)
        self.assertIn("recovery-timeout=120s", result.stdout)

    def test_s3_failure_keeps_retry_responses_without_sensitive_debug_output(self) -> None:
        result = self.run_bash(
            r'''
            register_scenarios D
            aws() {
              [[ "$*" == *--debug* ]] || return 252
              printf '%s\n' \
                "2026 - DEBUG - Making request: Authorization Credential=${ACCESS_KEY} Signature=canary-signature" \
                "canary-canonical-request" \
                '2026 - DEBUG - http://example.invalid "PUT /probe HTTP/1.1" 503 100' \
                "2026 - DEBUG - Response headers: {'x-rustfs-error-cause': 'namespace_lock_timeout', 'x-amz-request-id': 'request-1', 'retry-after': '1', 'Authorization': 'canary-auth'}" \
                '2026 - DEBUG - Response body:' \
                "b'canary-response-body ${SECRET_KEY}'" \
                '2026 - DEBUG - http://example.invalid "PUT /probe HTTP/1.1" 503 100' \
                "2026 - DEBUG - Response headers: {'x-rustfs-error-cause': 'namespace_lock_timeout', 'x-amz-request-id': 'request-2'}" \
                "aws: [ERROR]: An error occurred (ServiceUnavailable) when calling the PutObject operation: retry ${SECRET_KEY}" >&2
              return 254
            }
            expect_write D3/write http://example.invalid ok restored
            expect_write D1/write http://example.invalid ok repeated
            cat "${EVIDENCE}"/*.responses.jsonl "${EVIDENCE}"/*.stderr
            jq -r '.verdict + " " + .detail' "${RESULT_ROWS}"
            '''
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("http-status=503 cause=namespace_lock_timeout request-id=request-2", result.stdout)
        self.assertEqual(result.stdout.count('"http_status": 503'), 4)
        self.assertIn("UNEXPECTED", result.stdout)
        self.assertIn("[REDACTED]", result.stdout)
        for value in ("canary-auth", "canary-signature", "canary-canonical", "canary-response", "rustfs@test"):
            self.assertNotIn(value, result.stdout + result.stderr)

    def test_real_aws_cli_keeps_http_error_diagnostics(self) -> None:
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_PUT(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                body = b'<Error><Code>ServiceUnavailable</Code><Message>Please retry.</Message></Error>'
                self.send_response(503)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Type", "application/xml")
                self.send_header("x-rustfs-error-cause", "namespace_lock_timeout")
                self.send_header("x-amz-request-id", "fixture-request-id")
                self.send_header("Retry-After", "1")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            result = self.run_bash(f'''
                export NO_PROXY=127.0.0.1 AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard
                register_scenarios D
                expect_write D3/write http://127.0.0.1:{server.server_port} ok restored
                cat "${{EVIDENCE}}"/*.responses.jsonl
                jq -r '.verdict + " " + .detail' "${{RESULT_ROWS}}"
            ''')
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("UNEXPECTED", result.stdout)
        self.assertIn("http-status=503 cause=namespace_lock_timeout request-id=fixture-request-id", result.stdout)
        self.assertIn('"retry-after": "1"', result.stdout)
        self.assertNotIn("rustfs@test", result.stdout + result.stderr)

    def test_s3_pipeline_preserves_client_and_diagnostics_failures(self) -> None:
        result = self.run_bash('''
            aws() { echo 'aws: [ERROR]: Unknown options: --invalid' >&2; return 252; }
            s3op invalid http://example.invalid put-object
            echo "client=${S3_RC}/${S3_CODE}"
            aws() { echo '{}' ; }
            s3op valid http://example.invalid put-object
            echo "success=${S3_RC}/${S3_CODE}"
            python3() { return 3; }
            s3op broken http://example.invalid put-object
            echo "diagnostics=${S3_RC}/${S3_CODE}"
        ''')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("client=252/unknown", result.stdout)
        self.assertIn("success=0/", result.stdout)
        self.assertIn("diagnostics=253/unknown", result.stdout)

    def test_explicit_output_preserves_evidence_after_temp_storage_disappears(self) -> None:
        for destination in ("results", "log", "env"):
            with self.subTest(destination=destination), tempfile.TemporaryDirectory() as output, tempfile.TemporaryDirectory() as scratch:
                results = Path(output, "results.json")
                args = ["--results-file", str(results)] if destination == "results" else ["--log-file", str(Path(output, "suite.log"))]
                env = {**os.environ, "TMPDIR": scratch}
                if destination == "env":
                    env["EVIDENCE_DIR"] = str(Path(output, "explicit-evidence"))
                result = subprocess.run(
                    ["bash", "-c", f'''source {shlex.quote(str(SCRIPT))}
                    need_cmd() {{ :; }}
                    scenario_c() {{
                      for id in "${{EXPECTED_CASES[@]}}"; do record pass "$id" fixture; done
                    }}
                    main --scenario C -y --package-url https://example.invalid/rustfs.deb "$@"
                    ''', "bash", *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                shutil.rmtree(scratch)
                if destination != "results":
                    results = next(Path(output).rglob("results.json"))
                payload = json.loads(results.read_text())
                evidence = Path(payload["evidence_dir"])
                self.assertTrue(evidence.is_relative_to(output))
                self.assertTrue((evidence / "results.jsonl").is_file())
                self.assertTrue((evidence / "harness-errors.txt").is_file())
                self.assertTrue((evidence / "report.md").is_file())
                self.assertTrue(payload["complete"])
                self.assertEqual(payload["counts"]["seen"], 6)

    def test_recovery_timeout_fails_the_suite_after_all_probes_complete(self) -> None:
        result = self.run_bash('''
            need_cmd() { :; }
            scenario_c() {
              RECOVERY_STATE[C3]=timeout
              s3op() { S3_RC=0; S3_CODE=""; }
              for id in "${EXPECTED_CASES[@]}"; do
                if [ "$id" = C3/write ]; then
                  expect_write "$id" http://example.invalid ok restored
                else record pass "$id" fixture; fi
              done
            }
            RESULTS_FILE="${TMP}/results.json"
            trap 'finalize_results "$?"; cat "${RESULTS_FILE}"' EXIT
            main --scenario C -y --package-url https://example.invalid/rustfs.deb
        ''')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('"complete": true', result.stdout)
        self.assertIn('"process_exit_code": 1', result.stdout)
        self.assertIn('"unexpected": 1', result.stdout)
        self.assertIn('"seen": 6', result.stdout)

    def test_cleanup_and_help_do_not_erase_retained_evidence(self) -> None:
        for option in ("--cleanup", "--help", "--scenario"):
            with self.subTest(option=option), tempfile.TemporaryDirectory() as output:
                evidence = Path(output, "evidence")
                evidence.mkdir()
                retained = {"results.jsonl": "retained case\n", "harness-errors.txt": "retained error\n",
                            "results.json": "retained completion\n", "report.md": "retained report\n"}
                for name, contents in retained.items():
                    (evidence / name).write_text(contents)
                command = f'''source {shlex.quote(str(SCRIPT))}
                    do_cleanup() {{ :; }}
                    need_cmd() {{ :; }}
                    scenario_c() {{
                      for id in "${{EXPECTED_CASES[@]}}"; do record pass "$id" fixture; done
                    }}
                    main "$@"
                '''
                args = [option] if option != "--scenario" else ["--scenario", "C", "-y", "--package-url", "https://example.invalid/rustfs.deb"]
                result = subprocess.run(
                    ["bash", "-c", command, "bash", *args], cwd=ROOT,
                    env={**os.environ, "TMPDIR": output, "EVIDENCE_DIR": str(evidence)},
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                if option == "--scenario":
                    self.assertNotIn("retained case", (evidence / "results.jsonl").read_text())
                    self.assertEqual((evidence / "harness-errors.txt").read_text(), "")
                    self.assertTrue(json.loads((evidence / "results.json").read_text())["complete"])
                else:
                    for name, contents in retained.items():
                        self.assertEqual((evidence / name).read_text(), contents)

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
            install_and_start() { echo "env=$2 participants=$3"; }
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
        self.assertIn("participants=4", out)

    def test_install_starts_only_the_topology_participants(self) -> None:
        for count in (1, 2, 3, 4):
            with self.subTest(count=count):
                result = self.run_bash(f'''
                    calls="${{TMP}}/starts"
                    PACKAGE_URL=https://example.invalid/rustfs.deb
                    remote_all() {{ :; }}
                    run_remote() {{
                      case "$2" in *systemctl*) printf '%s|%s\\n' "$1" "$2" >> "${{calls}}";; esac
                    }}
                    ssh() {{ cat >/dev/null; }}
                    curl() {{ :; }}
                    install_and_start "$(matrix_volumes_expr {count} 2)" 'RUSTFS_STORAGE_CLASS_STANDARD="EC:1"' {count}
                    cat "${{calls}}"
                ''')
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                starts = [line.split("|")[0] for line in result.stdout.splitlines() if "|sudo -n systemctl start --no-block" in line]
                self.assertEqual(sorted(starts), [f"vm00{i}" for i in range(count)])
                self.assertEqual(result.stdout.count("systemctl is-active --quiet"), count)

    def test_invalid_participant_count_is_rejected_before_mutation(self) -> None:
        result = self.run_bash('''
            PACKAGE_URL=https://example.invalid/rustfs.deb
            remote_all() { echo UNEXPECTED_MUTATION; }
            install_and_start "$VOLUMES_4X4" "" 5
        ''')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid topology participant", result.stderr)
        self.assertNotIn("UNEXPECTED_MUTATION", result.stdout)

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
