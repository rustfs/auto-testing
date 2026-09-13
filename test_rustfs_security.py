#!/usr/bin/env python3
"""Exercise security case contracts without shared test VMs."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("rustfs-security-test.sh").read_text()
CASE = SCRIPT[SCRIPT.index("case_iam_101_user_crud() {"):SCRIPT.index("case_iam_102_user_disable() {")]
HARNESS = r'''
set -uo pipefail
admin_user_add() { echo 200 > "$TEST_TMP/code"; }
admin_user_remove() { touch "$TEST_TMP/deleted"; echo 200 > "$TEST_TMP/code"; }
admin_policy_add() { echo 200 > "$TEST_TMP/code"; }
admin_policy_attach() { touch "$TEST_TMP/policy"; echo 200 > "$TEST_TMP/code"; }
admin_api_code() { cat "$TEST_TMP/code"; }
assert_admin_ok() { [ "$(admin_api_code)" = "$1" ]; }
assert_aws_ok() { [ "$AWS_CODE" -eq 0 ]; }
assert_aws_fails() { [ "$AWS_CODE" -ne 0 ]; }
admin_api() {
  echo 200 > "$TEST_TMP/code"
  if [ "$1" = DELETE ]; then return; fi
  if [[ "$2" == */list-users ]]; then echo '{"sec101u":{}}'; return; fi
  if [ -f "$TEST_TMP/deleted" ]; then
    [ "$SCENARIO" = deletion-ineffective ] || echo 404 > "$TEST_TMP/code"
    return
  fi
  case "$SCENARIO" in
    disabled) echo '{"status":"disabled"}' ;;
    malformed) echo 'not json' ;;
    missing-status) echo '{}' ;;
    secret-leak) echo '{"status":"enabled","secretKey":"sec101secret"}' ;;
    info-error) echo 500 > "$TEST_TMP/code"; echo '{"status":"enabled"}' ;;
    *) echo '{"status":"enabled","updatedAt":null}' ;;
  esac
}
aws_run_as() {
  AWS_CODE=1
  if [ -f "$TEST_TMP/policy" ]; then
    [ "$SCENARIO" = attached-denied ] || AWS_CODE=0
  elif [ "$SCENARIO" = unprivileged-allowed ]; then AWS_CODE=0
  fi
}
'''


class UserCrudContractTest(unittest.TestCase):
    def run_case(self, scenario):
        with tempfile.TemporaryDirectory() as directory:
            return subprocess.run(
                ["bash", "-c", HARNESS + CASE + "\nif case_iam_101_user_crud; then exit 0; else exit 1; fi"],
                env={**os.environ, "TEST_TMP": directory, "SCENARIO": scenario},
                capture_output=True, text=True,
            )

    def test_user_info_does_not_need_access_key(self):
        result = self.run_case("success")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_rejects_invalid_responses_and_permission_failures(self):
        for scenario in ("disabled", "malformed", "missing-status", "secret-leak", "info-error",
                         "attached-denied", "unprivileged-allowed", "deletion-ineffective"):
            with self.subTest(scenario=scenario):
                self.assertNotEqual(self.run_case(scenario).returncode, 0)


if __name__ == "__main__":
    unittest.main()
