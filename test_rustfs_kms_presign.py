#!/usr/bin/env python3
"""Run the presigned KMS case against deterministic command fakes."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('rustfs-kms-test.sh').read_text()
CASE = SCRIPT[SCRIPT.index('test_kms317() {'):SCRIPT.index('\ntest_kms318() {')]
HARNESS = r'''
set -uo pipefail
kid=k317
ENDPOINT=http://localhost:9000
REGION=us-east-1
kms_create_key() { KMS_KEY_ID=k317; }
new_bucket() { :; }
rm_bucket() { :; }
make_test_file() { printf test > "$1"; }
kms_sse_put() { :; }
kms_sse_get() { cp "${TEST_TMP}/kms-f1" "$3"; }
md5_of_file() { cksum < "$1"; }
assert_aws_ok() { return 0; }
assert_rc_ok() { [ "$RC_CODE" -eq 0 ]; }
aws() { printf 'http://localhost:9000/get-only'; }
rc_run() {
  [ "$*" = 'object share local/kms317-b1/up --upload --expire 5m --json' ] || return 1
  RC_CODE=0
  RC_OUT='{"url":"http://localhost:9000/put-only"}'
  if [ "$SCENARIO" = missing-url ]; then RC_OUT='{}'; fi
}
aws_run() {
  case "$2" in
    put-bucket-encryption)
      printf '%s' "${@: -1}" | jq -e '.Rules[0].ApplyServerSideEncryptionByDefault == {SSEAlgorithm:"aws:kms",KMSMasterKeyID:"k317"}' >/dev/null || return 1
      ;;
    head-object)
      AWS_OUT='{"ServerSideEncryption":"aws:kms","SSEKMSKeyId":"k317"}'
      if [ "$SCENARIO" = plaintext ]; then AWS_OUT='{}'; fi
      if [ "$SCENARIO" = wrong-key ]; then AWS_OUT='{"ServerSideEncryption":"aws:kms","SSEKMSKeyId":"other"}'; fi
      ;;
    *) return 1 ;;
  esac
}
curl() {
  if [[ " $* " == *' -X PUT '* ]]; then
    [ "${@: -1}" = http://localhost:9000/put-only ] || return 1
    if [ "$SCENARIO" = upload-failure ]; then return 22; fi
  else
    cp "${TEST_TMP}/kms-f1" "${@: -1}"
  fi
}
'''

class PresignCaseTest(unittest.TestCase):
    def run_case(self, scenario):
        with tempfile.TemporaryDirectory() as tmp:
            return subprocess.run(['bash', '-c', HARNESS + CASE + '\nif test_kms317; then exit 0; else exit 1; fi'], env={**os.environ, 'TEST_TMP': tmp, 'SCENARIO': scenario}, capture_output=True, text=True)

    def test_put_signature_and_kms_metadata(self):
        result = self.run_case('success')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_reject_invalid_uploads(self):
        for scenario in ('missing-url', 'plaintext', 'wrong-key', 'upload-failure'):
            with self.subTest(scenario=scenario):
                self.assertNotEqual(self.run_case(scenario).returncode, 0)

if __name__ == '__main__':
    unittest.main()
