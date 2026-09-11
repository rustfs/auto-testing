#!/usr/bin/env python3
"""Exercise the fresh-bucket usage case without a server-side snapshot."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('rustfs-s3-compat-test.sh').read_text()
CASE = SCRIPT[SCRIPT.index('test_s3c405() {'):SCRIPT.index('\n# ---------- 5.')]
HARNESS = r'''
set -uo pipefail
new_bucket() { :; }
rm_bucket() { :; }
aws_run() { AWS_CODE=0; if [ "$SCENARIO" = put-failure ]; then AWS_CODE=1; fi; }
assert_aws_ok() { [ "$AWS_CODE" -eq 0 ]; }
assert_rc_ok() { [ "$RC_CODE" -eq 0 ]; }
rc_run() {
  RC_CODE=0
  if [ "$1" = tree ]; then RC_OUT='a/x'; return; fi
  if [ "$*" != 'du local/s3c405-b1/a/ --fallback --json' ]; then
    RC_CODE=5; RC_OUT='Bucket is absent from the server usage snapshot'; return 5
  fi
  RC_OUT='{"data":{"object_count":1,"total_bytes":14,"partial":false}}'
  case "$SCENARIO" in
    wrong-count) RC_OUT='{"data":{"object_count":0,"total_bytes":14,"partial":false}}' ;;
    wrong-size) RC_OUT='{"data":{"object_count":1,"total_bytes":0,"partial":false}}' ;;
    partial) RC_OUT='{"data":{"object_count":1,"total_bytes":14,"partial":true}}' ;;
    malformed) RC_OUT='not JSON' ;;
  esac
}
'''

class UsageCaseTest(unittest.TestCase):
    def run_case(self, scenario):
        with tempfile.TemporaryDirectory() as tmp:
            return subprocess.run(['bash', '-c', HARNESS + CASE + '\nif test_s3c405; then exit 0; else exit 1; fi'], env={**os.environ, 'TEST_TMP': tmp, 'SCENARIO': scenario}, capture_output=True, text=True)

    def test_fresh_bucket_without_snapshot(self):
        result = self.run_case('success')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_reject_incorrect_usage(self):
        for scenario in ('put-failure', 'wrong-count', 'wrong-size', 'partial', 'malformed'):
            with self.subTest(scenario=scenario):
                self.assertNotEqual(self.run_case(scenario).returncode, 0)

if __name__ == '__main__':
    unittest.main()
