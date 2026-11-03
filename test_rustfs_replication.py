#!/usr/bin/env python3
"""Run the real replicated-delete case without a cluster."""

import os
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent


class ReplicatedDeleteTests(unittest.TestCase):
    def run_case(self, mode):
        source = (ROOT / "rustfs-replication-test.sh").read_text()
        case = source[source.index("test_rep110() {"):source.index("\ntest_rep111() {")]
        harness = r'''
set -u
SITE2_ENDPOINT=http://target:19000
TEST_TMP=/unused
new_versioned_bucket() { :; }
register_replication_target() { echo arn:test; }
put_replication_rule() { :; }
make_test_file() { :; }
rm_bucket_force() { :; }
aws_wait_ep() { [ "$1" = "$SITE2_ENDPOINT" ]; }
assert_aws_ok() { [ "$AWS_CODE" -eq 0 ]; }
aws_run() {
  case "$2" in
    put-object|delete-object) AWS_CODE=0; AWS_OUT='{}' ;;
    *) AWS_CODE=254; AWS_OUT='NoSuchBucket (source endpoint)' ;;
  esac
}
aws_run_site2() {
  case "$2" in
    head-object)
      AWS_CODE=254
      AWS_OUT='Not Found'
      [ "$CASE_MODE" != head_error ] || AWS_OUT='ServiceUnavailable'
      ;;
    list-objects-v2)
      AWS_CODE=0
      AWS_OUT='{}'
      [ "$CASE_MODE" != listed ] || AWS_OUT='{"Contents":[{"Key":"gone.txt"}]}'
      [ "$CASE_MODE" != list_error ] || AWS_CODE=254
      ;;
    *) return 99 ;;
  esac
  return 0
}
'''
        return subprocess.run(["bash", "-c", harness + case + "\ntest_rep110\n"],
                              env={**os.environ, "CASE_MODE": mode}, capture_output=True,
                              text=True, timeout=5)

    def test_checks_the_target_endpoint(self):
        result = self.run_case("deleted")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_rejects_remaining_object(self):
        self.assertNotEqual(self.run_case("listed").returncode, 0)

    def test_rejects_head_failure_other_than_not_found(self):
        self.assertNotEqual(self.run_case("head_error").returncode, 0)

    def test_rejects_failed_listing_even_with_empty_json(self):
        self.assertNotEqual(self.run_case("list_error").returncode, 0)


if __name__ == "__main__":
    unittest.main()
