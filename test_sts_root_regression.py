#!/usr/bin/env python3
"""Exercise #9279 oracles, real curl signing, and isolated-process cleanup."""

from contextlib import contextmanager, redirect_stdout
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from scripts import sts_root_regression as probe


STS_NS = "https://sts.amazonaws.com/doc/2011-06-15/"
CANARY = "PRIVATE_FIXTURE_SECRET_OR_TOKEN"
LIST_BUCKETS = b'<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Buckets/></ListAllMyBucketsResult>'


def credentials_xml(**overrides):
    fields = {
        "AccessKeyId": "temporary-access",
        "SecretAccessKey": CANARY,
        "SessionToken": CANARY,
        "Expiration": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        **overrides,
    }
    root = ET.Element("AssumeRoleResponse", xmlns=STS_NS)
    credentials = ET.SubElement(ET.SubElement(root, "AssumeRoleResult"), "Credentials")
    for name, value in fields.items():
        if value is not None:
            ET.SubElement(credentials, name).text = value
    ET.SubElement(ET.SubElement(root, "ResponseMetadata"), "RequestId").text = "fixture-request-id"
    return ET.tostring(root)


def error_xml(code="InternalError"):
    return (f'<ErrorResponse xmlns="{STS_NS}"><Error><Type>Receiver</Type><Code>{code}</Code>'
            f'<Message>action not allowed {CANARY}</Message></Error><RequestId>repro-request</RequestId>'
            '</ErrorResponse>').encode()


@contextmanager
def server(sts_status=200, sts_body=None, s3_status=200, s3_body=LIST_BUCKETS):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, body):
            data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append((self.command, self.path, dict(self.headers), data))
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            self.respond(sts_status, credentials_xml() if sts_body is None else sts_body)

        def do_GET(self):
            self.respond(s3_status, s3_body)

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=lambda: httpd.serve_forever(poll_interval=0.01), daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", requests
    finally:
        httpd.shutdown()
        httpd.server_close()
        worker.join()


class QueryRequestTest(unittest.TestCase):
    def assert_signature(self, request, access, secret):
        method, path, headers, body = request
        headers = {name.lower(): value for name, value in headers.items()}
        match = re.fullmatch(r"AWS4-HMAC-SHA256 Credential=([^,]+), SignedHeaders=([^,]+), Signature=([a-f0-9]{64})",
                             headers["authorization"])
        self.assertIsNotNone(match)
        credential, signed_headers, actual = match.groups()
        actual_access, scope = credential.split("/", 1)
        self.assertEqual(actual_access, access)
        date, region, service, terminal = scope.split("/")
        self.assertEqual((region, service, terminal), ("us-east-1", "s3", "aws4_request"))
        canonical_headers = "".join(f"{name}:{' '.join(headers[name].split())}\n" for name in signed_headers.split(";"))
        canonical = "\n".join((method, path, "", canonical_headers, signed_headers, hashlib.sha256(body).hexdigest()))
        to_sign = "\n".join(("AWS4-HMAC-SHA256", headers["x-amz-date"], scope, hashlib.sha256(canonical.encode()).hexdigest()))
        key = ("AWS4" + secret).encode()
        for value in (date, region, service, terminal):
            key = hmac.new(key, value.encode(), hashlib.sha256).digest()
        self.assertEqual(actual, hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest())

    def test_original_query_and_temporary_s3_request_have_valid_s3_signatures(self):
        for secret in ("rootroot", "éééé", 'fixture-"quote\\slash'):
            with self.subTest(secret=secret), server() as (endpoint, requests):
                observation = {}
                # Parent AWS credentials/session tokens must not influence curl.
                with patch.dict(os.environ, {"AWS_SESSION_TOKEN": "unrelated", "AWS_PROFILE": "unrelated"}):
                    probe.check_assume_role(endpoint, "root", secret, observation)
                self.assertEqual(len(requests), 2)
                self.assertEqual(requests[0][:2], ("POST", "/"))
                self.assertEqual(requests[0][3], probe.ASSUME_ROLE_BODY.encode())
                self.assertEqual(requests[0][2]["Content-Type"], "application/x-www-form-urlencoded")
                self.assert_signature(requests[0], "root", secret)
                self.assertEqual(requests[1][:2], ("GET", "/"))
                self.assertEqual(requests[1][2]["x-amz-security-token"], CANARY)
                self.assert_signature(requests[1], "temporary-access", CANARY)
                self.assertEqual(observation["sts_http_status"], 200)
                self.assertEqual(observation["s3_http_status"], 200)
                self.assertNotIn(CANARY, json.dumps(observation))

    def test_500_action_not_allowed_and_403_are_not_success(self):
        for status, code in ((500, "InternalError"), (403, "AccessDenied")):
            with self.subTest(status=status), server(status, error_xml(code)) as (endpoint, requests):
                observation = {}
                with self.assertRaises(probe.RegressionFailure) as caught:
                    probe.check_assume_role(endpoint, "root", "rootroot", observation)
                self.assertEqual(observation["sts_http_status"], status)
                self.assertEqual(observation["sts_error_code"], code)
                self.assertEqual(observation["request_id"], "repro-request")
                self.assertEqual(len(requests), 1)
                self.assertNotIn(CANARY, str(caught.exception) + json.dumps(observation))

    def test_incomplete_credentials_and_error_envelopes_cannot_pass_http_200(self):
        bodies = [b"not XML", b"<WrongEnvelope/>", error_xml(), b"<AssumeRoleResponse/>"]
        for field in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"):
            bodies += [credentials_xml(**{field: None}), credentials_xml(**{field: " "})]
        bodies += [credentials_xml(AccessKeyId="root")]
        for body in bodies:
            with self.subTest(body=body[:60]), server(sts_body=body) as (endpoint, requests):
                with self.assertRaises(probe.RegressionFailure):
                    probe.check_assume_role(endpoint, "root", "rootroot", {})
                self.assertEqual(len(requests), 1)

    def test_expiration_must_be_valid_future_and_bounded(self):
        now = datetime.now(timezone.utc)
        for expiration in ("not-a-date", now.replace(tzinfo=None).isoformat(),
                           (now - timedelta(seconds=1)).isoformat(), (now + timedelta(hours=2)).isoformat()):
            with self.subTest(expiration=expiration), server(sts_body=credentials_xml(Expiration=expiration)) as (endpoint, requests):
                with self.assertRaises(probe.RegressionFailure):
                    probe.check_assume_role(endpoint, "root", "rootroot", {})
                self.assertEqual(len(requests), 1)

    def test_unusable_session_and_non_s3_success_bodies_fail(self):
        for status, body in ((403, error_xml("AccessDenied")), (500, error_xml()),
                             (200, b"<html/>"), (200, b"invalid XML")):
            with self.subTest(status=status, body=body), server(s3_status=status, s3_body=body) as (endpoint, requests):
                with self.assertRaises(probe.RegressionFailure):
                    probe.check_assume_role(endpoint, "root", "rootroot", {})
                self.assertEqual(len(requests), 2)

    def test_transport_failure_is_not_an_http_verdict(self):
        with server() as (endpoint, _):
            pass
        with self.assertRaisesRegex(probe.RegressionFailure, "transport failed"):
            probe.check_assume_role(endpoint, "root", "rootroot", {})

    def test_cli_failure_keeps_status_without_disclosing_response(self):
        with server(500, error_xml()) as (endpoint, _), io.StringIO() as output:
            with patch.object(sys, "argv", ["probe", "--endpoint", endpoint]), redirect_stdout(output), \
                    patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "root", "RUSTFS_SECRET_KEY": CANARY}):
                self.assertEqual(probe.main(), 1)
            result = json.loads(output.getvalue())
            self.assertEqual(result["verdict"], "FAIL")
            self.assertEqual(result["sts_http_status"], 500)
            self.assertNotIn(CANARY, output.getvalue())


class StartupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.receipt = self.root / "receipt.json"

    def binary(self, mode, diagnostic=""):
        binary = self.root / "fake-rustfs"
        # Real subprocesses/listeners exercise cleanup and readiness. Their
        # HTTP results deliberately include success and the historical defect.
        binary.write_text(f'''#!{sys.executable}
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import signal
import sys
import time
Path({str(self.receipt)!r}).write_text(json.dumps({{"pid": os.getpid(), "cwd": os.getcwd(), "env": dict(os.environ)}}))
mode = {mode!r}
if mode in ("exit-0", "exit-1", "crash"):
    print({diagnostic!r}, flush=True)
    if mode == "crash":
        os.kill(os.getpid(), signal.SIGKILL)
    sys.exit(0 if mode == "exit-0" else 1)
if mode == "timeout":
    time.sleep(600)
class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.path == "/health/ready":
            self.reply(200, b'{{"ready":true}}')
        elif self.path == "/":
            self.reply(200, {LIST_BUCKETS!r})
        else:
            self.reply(403, {error_xml("AccessDenied")!r})
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if mode == "sts-500":
            self.reply(500, {error_xml()!r})
        elif mode == "sts-403":
            self.reply(403, {error_xml("AccessDenied")!r})
        else:
            self.reply(200, {credentials_xml()!r})
    def log_message(self, *args):
        pass
host, port = os.environ["RUSTFS_ADDRESS"].split(":")
HTTPServer((host, int(port)), Handler).serve_forever()
''')
        binary.chmod(0o755)
        return str(binary)

    def assert_cleaned(self):
        receipt = json.loads(self.receipt.read_text())
        self.assertFalse(Path(receipt["cwd"]).exists(), receipt)
        with self.assertRaises(ProcessLookupError):
            os.kill(receipt["pid"], 0)
        return receipt

    def test_exact_short_secret_and_access_boundaries_require_length_errors(self):
        for case, field, access_bytes, secret_bytes in (("secret-4", "secret", 4, 4),
                                                       ("secret-7", "secret", 4, 7),
                                                       ("access-2", "access", 2, 8)):
            with self.subTest(case=case):
                observation = {}
                probe.check_startup(self.binary("exit-1", f"Error: invalid {field} key length"), case, 3, observation)
                self.assertTrue(observation["credential_length_rejected"])
                self.assertEqual((observation["access_key_bytes"], observation["secret_key_bytes"]),
                                 (access_bytes, secret_bytes))
                self.assertEqual(observation["cleanup"], "PASS")
                self.assert_cleaned()

    def test_unrelated_startup_errors_clean_exit_crashes_and_timeouts_fail(self):
        scenarios = [("exit-1", "Error: Address already in use"),
                     ("exit-1", "WARNING: secret key minimum length is 8 bytes\nError: permission denied"),
                     ("exit-1", "Error: invalid access key length"),
                     ("exit-0", "Error: invalid secret key length"),
                     ("crash", "Error: invalid secret key length"), ("timeout", "")]
        for mode, diagnostic in scenarios:
            with self.subTest(mode=mode, diagnostic=diagnostic):
                observation = {}
                with self.assertRaises(probe.RegressionFailure):
                    probe.check_startup(self.binary(mode, diagnostic), "secret-4", 0.7, observation)
                self.assertEqual(observation["cleanup"], "PASS")
                self.assert_cleaned()

    def test_accepting_short_credentials_fails_even_with_200_or_403_and_preserves_500(self):
        for mode, status in (("success", 200), ("sts-403", 403), ("sts-500", 500)):
            with self.subTest(mode=mode):
                observation = {}
                with self.assertRaisesRegex(probe.RegressionFailure, "accepted credentials below"):
                    probe.check_startup(self.binary(mode), "secret-4", 3, observation)
                self.assertEqual(observation["sts_http_status"], status)
                self.assertEqual(observation["cleanup"], "PASS")
                if status == 500:
                    self.assertEqual(observation["sts_error_code"], "InternalError")
                self.assertNotIn(CANARY, json.dumps(observation))
                self.assert_cleaned()

    def test_valid_and_utf8_byte_boundaries_require_usable_sessions_and_isolated_settings(self):
        for case, access_bytes in (("minimum", 3), ("secret-8", 4), ("secret-8-utf8", 4)):
            with self.subTest(case=case):
                observation = {}
                with patch.dict(os.environ, {"RUSTFS_VOLUMES": "/shared/do-not-touch", "RUSTFS_SECRET_KEY_FILE": "/private",
                                             "RUSTFS_KMS_ENABLE": "true", "AWS_SESSION_TOKEN": CANARY}):
                    probe.check_startup(self.binary("success"), case, 3, observation)
                self.assertEqual((observation["access_key_bytes"], observation["secret_key_bytes"]), (access_bytes, 8))
                self.assertEqual(observation["s3_http_status"], 200)
                receipt = self.assert_cleaned()
                environment = receipt["env"]
                self.assertEqual(environment["RUSTFS_VOLUMES"], str(Path(receipt["cwd"]) / "data"))
                self.assertTrue(environment["RUSTFS_ADDRESS"].startswith("127.0.0.1:"))
                for name in ("RUSTFS_SECRET_KEY_FILE", "RUSTFS_KMS_ENABLE", "AWS_SESSION_TOKEN"):
                    self.assertNotIn(name, environment)

    def test_valid_credentials_do_not_hide_sts_500(self):
        observation = {}
        with self.assertRaisesRegex(probe.RegressionFailure, "expected HTTP 200, received 500"):
            probe.check_startup(self.binary("sts-500"), "secret-8", 3, observation)
        self.assertEqual(observation["sts_error_code"], "InternalError")
        self.assert_cleaned()

    def test_cancellation_and_ssh_hangup_clean_the_child_and_private_data(self):
        for interrupt in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            with self.subTest(signal=interrupt):
                self.receipt.unlink(missing_ok=True)
                runner = subprocess.Popen([sys.executable, str(Path(probe.__file__).resolve()), "--rustfs-bin", self.binary("timeout"),
                                           "--case", "secret-4"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    deadline = time.monotonic() + 5
                    receipt = None
                    while time.monotonic() < deadline:
                        try:
                            receipt = json.loads(self.receipt.read_text())
                            break
                        except (FileNotFoundError, json.JSONDecodeError):
                            time.sleep(0.02)
                    self.assertIsNotNone(receipt, "child did not start")
                    runner.send_signal(interrupt)
                    stdout, stderr = runner.communicate(timeout=10)
                    self.assertEqual(runner.returncode, 130, stdout + stderr)
                    self.assertNotIn("PASS", stdout)
                    self.assert_cleaned()
                finally:
                    if runner.poll() is None:
                        runner.kill()
                        runner.communicate(timeout=5)

    def test_cleanup_failure_cannot_pass_or_replace_the_sts_failure(self):
        original_stop = probe.stop_process

        def failed_cleanup(process):
            original_stop(process)
            raise OSError("fixture cleanup failure")

        for mode, reason in (("success", "could not stop"), ("sts-500", "expected HTTP 200, received 500")):
            with self.subTest(mode=mode):
                observation = {}
                try:
                    with patch.object(probe, "stop_process", side_effect=failed_cleanup):
                        with self.assertRaisesRegex(probe.RegressionFailure, reason):
                            probe.check_startup(self.binary(mode), "secret-8", 3, observation)
                    self.assertEqual(observation["cleanup"], "FAIL")
                    if mode == "sts-500":
                        self.assertEqual(observation["sts_error_code"], "InternalError")
                finally:
                    receipt = json.loads(self.receipt.read_text())
                    shutil.rmtree(receipt["cwd"])
                self.assert_cleaned()

    def test_remote_stdin_entrypoint_runs_the_candidate_and_returns_its_verdict(self):
        result = subprocess.run([sys.executable, "-", "--rustfs-bin", self.binary("success"), "--case", "minimum"],
                                input=Path(probe.__file__).read_text(), capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        observation = json.loads(result.stdout)
        self.assertEqual(observation["case_id"], "STS-110")
        self.assertEqual(observation["verdict"], "PASS")
        self.assertEqual(observation["cleanup"], "PASS")
        self.assert_cleaned()

    def test_matrix_retains_a_failed_case_and_continues_remaining_cases(self):
        calls = []

        def startup(_binary, name, _timeout, observation):
            calls.append(name)
            observation["cleanup"] = "PASS"
            if name == "secret-4":
                raise probe.RegressionFailure("fixture regression")

        with patch.object(sys, "argv", ["probe", "--rustfs-bin", self.binary("success")]), \
                patch.object(probe, "check_startup", side_effect=startup), io.StringIO() as output, redirect_stdout(output):
            self.assertEqual(probe.main(), 1)
            rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(calls, list(probe.CASES))
        self.assertEqual(len(rows), 6)
        self.assertEqual([row["case"] for row in rows if row["verdict"] == "FAIL"], ["secret-4"])


if __name__ == "__main__":
    unittest.main()
