#!/usr/bin/env python3
"""Exercise security case contracts without shared test VMs."""
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import threading
import unittest

SCRIPT = Path(__file__).with_name("rustfs-security-test.sh").read_text()

def shell_function(name):
    return re.search(rf"(?ms)^{name}\(\) \{{.*?^\}}", SCRIPT).group(0)


class StsRevocationTest(unittest.TestCase):
    def run_case(self, scenario, response='{"revoked":1}'):
        harness = r'''
set -Eeu -o pipefail
ACCESS_KEY=fixture-root
AWS_CALLS=0
assume_role() { printf 'temporary-key\ttemporary-secret\ttemporary-token\texpiration\n'; }
admin_api() {
  echo revoke >> "$TEST_TMP/steps"
  echo 200 > "$TEST_TMP/code"
  [ "$SCENARIO" != revoke-denied ] || echo 403 > "$TEST_TMP/code"
  printf '%s' "$TEST_RESPONSE"
}
admin_api_code() { cat "$TEST_TMP/code"; }
assert_admin_ok() { [ "$(admin_api_code)" = "$1" ]; }
assert_aws_ok() { [ "$AWS_CODE" -eq 0 ]; }
assert_aws_fails() { [ "$AWS_CODE" -ne 0 ]; }
aws_run_as() {
  AWS_CALLS=$((AWS_CALLS + 1))
  echo "aws-${AWS_CALLS}" >> "$TEST_TMP/steps"
  AWS_CODE=0
  AWS_OUT=''
  if [ "$AWS_CALLS" -eq 1 ]; then
    [ "$SCENARIO" != initial-denied ] || AWS_CODE=255
  elif [ "$SCENARIO" != still-valid ]; then
    AWS_CODE=255
    AWS_OUT='An error occurred (InvalidAccessKeyId) when calling the ListBuckets operation: rejected'
    [ "$SCENARIO" != access-denied ] || AWS_OUT='An error occurred (AccessDenied) when calling the ListBuckets operation: rejected'
    [ "$SCENARIO" != network-error ] || AWS_OUT='Could not connect to the endpoint URL'
  fi
}
'''
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                ["bash", "-c", harness + shell_function("case_sts_105_revoke") +
                 "\nif case_sts_105_revoke; then exit 0; else exit 1; fi"],
                env={**os.environ, "TEST_TMP": directory, "SCENARIO": scenario, "TEST_RESPONSE": response,
                     "STS_DIAGNOSTICS_FILE": str(Path(directory) / "diagnostics")},
                capture_output=True, text=True, timeout=10,
            )
            return result, (Path(directory) / "steps").read_text().splitlines()

    def test_revoked_credentials_are_rejected(self):
        for scenario in ("success", "access-denied"):
            result, steps = self.run_case(scenario)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(steps, ["aws-1", "revoke", "aws-2"])

    def test_does_not_continue_after_a_failed_prerequisite(self):
        for scenario, expected in (("initial-denied", ["aws-1"]), ("revoke-denied", ["aws-1", "revoke"])):
            result, steps = self.run_case(scenario)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(steps, expected)

    def test_requires_a_positive_revocation_count(self):
        for response in ('{"revoked":0}', '{}', 'not JSON', '{"revoked":1.5}', '{"revoked":"1"}'):
            with self.subTest(response=response):
                result, steps = self.run_case("success", response)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(steps, ["aws-1", "revoke"])

    def test_successful_access_and_network_failures_do_not_pass(self):
        for scenario in ("still-valid", "network-error"):
            self.assertNotEqual(self.run_case(scenario)[0].returncode, 0)


class AdminResponseDiagnosticsTest(unittest.TestCase):
    def test_preserves_json_xml_and_proxy_response_evidence(self):
        responses = [('{"Code":"AccessDenied","RequestId":"body-request"}', ""),
                     ('<Error><Code>SignatureDoesNotMatch</Code><RequestId>xml-request</RequestId></Error>', "{}"),
                     ('<html>Proxy rejected the request</html>', "")]
        for body, request_body in responses:
            with self.subTest(body=body), tempfile.TemporaryDirectory() as directory:
                requests = []

                class Handler(BaseHTTPRequestHandler):
                    def do_POST(self):
                        data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                        requests.append((self.path, self.headers, data))
                        self.send_response(403)
                        self.send_header("x-amz-request-id", "header-request")
                        self.send_header("x-request-id", "rustfs-request")
                        self.end_headers()
                        self.wfile.write(body.encode())

                    def log_message(self, *_args):
                        pass

                server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                worker = threading.Thread(target=server.serve_forever, daemon=True)
                worker.start()
                try:
                    functions = "\n".join(shell_function(name) for name in
                                          ("sigv4_uri_encode", "canonical_query", "admin_api"))
                    # The production signing helpers are one-line functions.
                    functions += "\n" + "\n".join(line for line in SCRIPT.splitlines()
                                                   if line.startswith(("sha256_hex()", "hmac_sha256_hex()", "hex_of_ascii()")))
                    result = subprocess.run(
                        ["bash", "-Eeu", "-o", "pipefail", "-c", functions +
                         '\nadmin_api POST "/rustfs/admin/v3/revoke-tokens/builtin?user=fixture-root&fullRevoke=true" "" "$REQUEST_BODY"'],
                        env={**os.environ, "DRY_RUN": "0", "API_REQUEST_TIMEOUT": "3", "ACCESS_KEY": "fixture-root",
                             "SECRET_KEY": "fixture-secret", "SIGV4_REGION": "us-east-1", "SIGV4_SERVICE": "s3",
                             "API_ENDPOINT": f"http://127.0.0.1:{server.server_port}", "REQUEST_BODY": request_body,
                             "ADMIN_API_CODE_FILE": str(Path(directory) / "code"), "TMPDIR": directory,
                             "STS_DIAGNOSTICS_FILE": str(Path(directory) / "diagnostics"),
                             "NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"},
                        capture_output=True, text=True, timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, body)
                    diagnostic = (Path(directory) / "diagnostics").read_text()
                    self.assertIn("http_status=403", diagnostic)
                    self.assertIn("x-amz-request-id: header-request", diagnostic)
                    self.assertIn("x-request-id: rustfs-request", diagnostic)
                    self.assertIn(body, diagnostic)
                    self.assertEqual((Path(directory) / "code").read_text(), "403")
                    self.assertEqual(requests[0][0], "/rustfs/admin/v3/revoke-tokens/builtin?fullRevoke=true&user=fixture-root")
                    self.assertEqual(requests[0][2].decode(), request_body)
                    self.assertTrue(requests[0][1]["Authorization"].startswith("AWS4-HMAC-SHA256 "))
                    self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ["code", "diagnostics"])
                finally:
                    server.shutdown()
                    server.server_close()
                    worker.join()


class CanonicalQueryEncodingTest(unittest.TestCase):
    """The server re-encodes every query pair for the canonical request; the helper must match it."""

    def canonical_query(self, query):
        functions = "\n".join(shell_function(name) for name in ("sigv4_uri_encode", "canonical_query"))
        result = subprocess.run(["bash", "-Eeu", "-o", "pipefail", "-c", functions + '\ncanonical_query "$QUERY"'],
                                env={**os.environ, "QUERY": query}, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        # admin_api consumes the helper through "$(...)", which drops the trailing newline.
        return result.stdout.rstrip("\n")

    def test_encodes_reserved_characters_and_sorts_pairs(self):
        cases = {
            "user=rustfs@test&fullRevoke=true": "fullRevoke=true&user=rustfs%40test",
            "accessKey=sec104u": "accessKey=sec104u",
            "flag": "flag=",
            "b=2&a=1&flag": "a=1&b=2&flag=",
            "arn=arn:aws:s3:::x/y": "arn=arn%3Aaws%3As3%3A%3A%3Ax%2Fy",
            "k=a b+c%d": "k=a%20b%2Bc%25d",
            "k=x=y": "k=x%3Dy",
            "k=\u00e9": "k=%C3%A9",
            "": "",
        }
        for query, expected in cases.items():
            with self.subTest(query=query):
                self.assertEqual(self.canonical_query(query), expected)

    def test_signed_request_carries_the_encoded_query(self):
        with tempfile.TemporaryDirectory() as directory:
            requests = []

            class Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    requests.append(self.path)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'{"revoked":1}')

                def log_message(self, *_args):
                    pass

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                functions = "\n".join(shell_function(name) for name in ("sigv4_uri_encode", "canonical_query", "admin_api"))
                functions += "\n" + "\n".join(line for line in SCRIPT.splitlines()
                                               if line.startswith(("sha256_hex()", "hmac_sha256_hex()", "hex_of_ascii()")))
                result = subprocess.run(
                    ["bash", "-Eeu", "-o", "pipefail", "-c", functions +
                     '\nadmin_api POST "/rustfs/admin/v3/revoke-tokens/builtin?user=${ACCESS_KEY}&fullRevoke=true"'],
                    env={**os.environ, "DRY_RUN": "0", "API_REQUEST_TIMEOUT": "3", "ACCESS_KEY": "rustfs@test",
                         "SECRET_KEY": "fixture-secret", "SIGV4_REGION": "us-east-1", "SIGV4_SERVICE": "s3",
                         "API_ENDPOINT": f"http://127.0.0.1:{server.server_port}",
                         "ADMIN_API_CODE_FILE": str(Path(directory) / "code"), "TMPDIR": directory,
                         "STS_DIAGNOSTICS_FILE": str(Path(directory) / "diagnostics"),
                         "NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"},
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(requests, ["/rustfs/admin/v3/revoke-tokens/builtin?fullRevoke=true&user=rustfs%40test"])
            finally:
                server.shutdown()
                server.server_close()
                worker.join()


class StsEvidenceRetentionTest(unittest.TestCase):
    def test_collects_failure_logs_and_retains_each_topology(self):
        harness = r'''
set -Eeu -o pipefail
DRY_RUN=0
PASS=0
FAIL=0
CASE_RESULTS=()
FAILED_TESTS=()
fixture_case() {
  echo "response-for-${CURRENT_TOPOLOGY}" >> "$STS_DIAGNOSTICS_FILE"
  [ "$CURRENT_TOPOLOGY" = single-multi ]
}
collect_sts_server_diagnostics() {
  echo "server-before-cleanup-${CURRENT_TOPOLOGY}"
  echo "journal-unavailable-${CURRENT_TOPOLOGY}" >&2
}
'''
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                ["bash", "-c", harness + shell_function("run_test") + "\n" + shell_function("record_topology_results") + r'''
for CURRENT_TOPOLOGY in single-single single-multi; do
  CASE_RESULTS=()
  run_test STS-105 fixture fixture_case
  record_topology_results "$CURRENT_TOPOLOGY"
done
test "$PASS" -eq 1 && test "$FAIL" -eq 1
'''], env={**os.environ, "TEST_TMP": directory, "STS_DIAGNOSTICS_FILE": str(Path(directory) / "diagnostics"),
           "REPORT_SECTIONS_FILE": str(Path(directory) / "report")}, capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            for output in (result.stdout, (Path(directory) / "report").read_text()):
                self.assertIn("response-for-single-single", output)
                self.assertIn("response-for-single-multi", output)
                self.assertIn("server-before-cleanup-single-single", output)
                self.assertIn("journal-unavailable-single-single", output)
                self.assertNotIn("server-before-cleanup-single-multi", output)

    def test_unavailable_ssh_does_not_abort_log_collection(self):
        functions = shell_function("collect_sts_server_diagnostics") + r'''
SSH_OPTS=()
SSH_USER=fixture
topology_nodes() { echo 'node1 node2'; }
ssh() { cat >/dev/null; return 255; }
collect_sts_server_diagnostics '2026-09-13T00:00:00Z'
'''
        result = subprocess.run(["bash", "-c", functions], env={**os.environ, "CURRENT_TOPOLOGY": "multi-multi"},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        for node in ("node1", "node2"):
            self.assertIn(f"server logs unavailable: {node}", result.stdout)


class DryRunTest(unittest.TestCase):
    def test_preview_does_not_require_runtime_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "bin"
            tools.mkdir()
            # Deliberately omit curl, aws, ssh, openssl, jq, python3, and git.
            for command in ("bash", "cat", "dirname", "head", "mkdir", "mktemp", "rm", "sed", "sort", "tee"):
                (tools / command).symlink_to(shutil.which(command))
            report = root / "report.md"
            log_file = root / "security.log"
            result = subprocess.run(
                [str(tools / "bash"), str(Path(__file__).with_name("rustfs-security-test.sh")),
                 "--dry-run", "--all-topologies", "-y", "--log-file", str(log_file)],
                env={**os.environ, "PATH": str(tools), "TMPDIR": directory, "REPORT_FILE": str(report),
                     "GITHUB_STEP_SUMMARY": str(root / "summary.md"), "RUSTFS_NODES": "fixture-node",
                     "RUSTFS_ACCESS_KEY": "fixture-root", "RUSTFS_SECRET_KEY": "fixture-secret"},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(result.stdout.count("[DRY-RUN] STS-105"), 3)
            for topology in ("single-single", "single-multi", "multi-multi"):
                self.assertIn(f"## Topology: {topology}", report.read_text())
            self.assertEqual(result.stdout, log_file.read_text())


if __name__ == "__main__":
    unittest.main()
