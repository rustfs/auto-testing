#!/usr/bin/env python3
"""Sensitive failure diagnostics must not publish rejected response values."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent
CANARY = "SYNTHETIC_KMS_DIAGNOSTIC_CANARY"
FIELDS = ("token", "secret_key", "secret_id", "master_key", "password", "client_secret")


class SensitiveDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.functions = {}
        for name, script in (("test_kms106", "rustfs-kms-test.sh"), ("verify_config_persisted", "rustfs-upgrade-test.sh"),
                             ("assert_aws_fails", "rustfs-security-test.sh")):
            source = (ROOT / script).read_text()
            function = re.search(rf"(?ms)^{name}\(\) \{{.*?^\}}", source)
            self.assertIsNotNone(function, f"the real {name} function must be tested")
            self.functions[name] = function.group(0)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.public_log = Path(self.temp.name) / "suite.log"
        self.valid = {field: "[redacted]" for field in FIELDS}
        self.valid["backend"] = "VaultKV2"

    def run_case(self, body, http, expected, context, function="test_kms106"):
        command = self.functions[function] + '''
admin_api() { printf '%s' "$TEST_RESPONSE"; }
admin_api_code() { printf '%s' "$TEST_HTTP"; }
ssh() { printf '%s' "$TEST_RESPONSE"; }
ACCESS_KEY=fixture-access
NODES=(fixture-node)
SSH_USER=fixture
SSH_OPTS=(-o BatchMode=yes)
AWS_CODE="$TEST_HTTP"
AWS_OUT="$TEST_RESPONSE"
exec > >(tee "$TEST_LOG") 2>&1
''' + function + " 'expired/garbage web identity token'\n"
        result = subprocess.run(["bash", "--noprofile", "--norc", "-uo", "pipefail", "-c", command],
                                env={**os.environ, "TEST_RESPONSE": json.dumps(body) if isinstance(body, dict) else body, "TEST_HTTP": str(http),
                                     "TEST_LOG": str(self.public_log)}, text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, expected, result.stderr)
        for output in (result.stdout, result.stderr, self.public_log.read_text()):
            self.assertNotIn(CANARY, output)
        self.assertIn(context, result.stdout)
        self.assertEqual(result.stdout, self.public_log.read_text())

    def test_rejected_response_never_prints_sensitive_values(self):
        cases = [({"token": CANARY}, 503, "HTTP 503"),
                 ({"token": CANARY}, 200, "expected >=6 [redacted] fields, got 0"),
                 ({**{field: "[redacted]" for field in FIELDS}, "diagnostic": CANARY}, 200, "non-sensitive config fields missing")]
        for field in FIELDS:
            cases.append(({**self.valid, field: CANARY, "extra": "[redacted]"}, 200, f"field {field} not redacted"))
        for body, http, context in cases:
            with self.subTest(context=context):
                self.run_case(body, http, 1, context)

    def test_upgrade_config_failures_do_not_print_secret_key(self):
        for config, context in ((f"RUSTFS_ACCESS_KEY=wrong\nRUSTFS_SECRET_KEY={CANARY}\nRUSTFS_VOLUMES=/data", "lost access key"),
                                (f"RUSTFS_ACCESS_KEY=fixture-access\nRUSTFS_SECRET_KEY={CANARY}", "lost volumes")):
            with self.subTest(context=context):
                self.run_case(config, 200, 1, context, "verify_config_persisted")
        self.run_case(f"RUSTFS_ACCESS_KEY=fixture-access\nRUSTFS_SECRET_KEY={CANARY}\nRUSTFS_VOLUMES=/data",
                      200, 0, "", "verify_config_persisted")

    def test_unexpected_sts_success_does_not_print_credentials(self):
        credentials = {"Credentials": {"AccessKeyId": "fixture", "SecretAccessKey": CANARY, "SessionToken": CANARY}}
        self.run_case(credentials, 0, 1, "aws unexpectedly succeeded (exit 0): expired/garbage web identity token", "assert_aws_fails")
        self.run_case(credentials, 42, 0, "", "assert_aws_fails")

    def test_valid_redacted_response_still_passes(self):
        self.run_case(self.valid, 200, 0, "")


if __name__ == "__main__":
    unittest.main()
