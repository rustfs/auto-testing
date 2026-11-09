#!/usr/bin/env python3
"""Mutate S3 responses to verify historical regression assertions, not a server."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("rustfs-s3-compat-test.sh")
HARNESS = r'''
source "$RUNNER"
rc_run() { RC_CODE=0; RC_OUT='s3c103-b1'; }
VERSION_PRESENT=0
MARKER_PRESENT=0
aws_run() {
  printf '%s\n' "$*" >> "$CALL_LOG"
  local method="$2" version="" key="" previous="" argument
  for argument in "$@"; do
    if [ "$previous" = --version-id ]; then version="$argument"; fi
    if [ "$previous" = --key ]; then key="$argument"; fi
    previous="$argument"
  done
  AWS_CODE=0
  AWS_OUT='{}'
  if [ "$CASE" = 212 ] && [ "$key" != source ]; then
    case "$method" in
      put-object|copy-object|create-multipart-upload)
        AWS_CODE=254
        AWS_OUT='An error occurred (InvalidArgument): Object key contains a reserved storage filename'
        case "$SCENARIO:$method" in
          accepts-put:put-object|accepts-copy:copy-object|accepts-mpu:create-multipart-upload) AWS_CODE=0 ;;
          wrong-code:*) AWS_OUT='AccessDenied: Object key contains a reserved storage filename' ;;
          wrong-reason:*) AWS_OUT='InvalidArgument: something else is invalid' ;;
        esac
        case "$SCENARIO:$key" in
          accepts-nested:nested/*|accepts-uppercase:XL.META|accepts-parent:nested/Xl.MeTa/child) AWS_CODE=0 ;;
        esac
        return 0
        ;;
    esac
  fi
  case "$method" in
    create-bucket|put-bucket-versioning|upload-part)
      [ "$method" != upload-part ] || AWS_OUT='{"ETag":"\"part\""}'
      ;;
    put-object)
      if [ "$CASE" = 1407 ]; then
        VERSION_PRESENT=1
        AWS_OUT='{"VersionId":"v1"}'
        [ "$SCENARIO" != missing-version ] || AWS_OUT='{}'
      fi
      [ "$SCENARIO" != put-failure ] || { AWS_CODE=255; AWS_OUT='put failed'; }
      ;;
    list-buckets)
      local created
      created="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
      case "$SCENARIO" in
        year-one) created='0001-01-01T00:00:00Z' ;;
        invalid-date) created='2026-99-99T00:00:00Z' ;;
        stale-date) created='2024-01-01T00:00:00Z' ;;
        future-date) created='9999-01-01T00:00:00Z' ;;
        fractional-date) created="${created%Z}.123Z" ;;
        offset-date) created="${created%Z}+00:00" ;;
      esac
      AWS_OUT="$(jq -nc --arg date "$created" '{Buckets:[{Name:"s3c103-b1",CreationDate:$date}]}')"
      [ "$SCENARIO" != missing-date ] || AWS_OUT='{"Buckets":[{"Name":"s3c103-b1"}]}'
      ;;
    create-multipart-upload) AWS_OUT='{"UploadId":"upload"}' ;;
    complete-multipart-upload)
      AWS_OUT='{"ETag":"\"final-3\"","Bucket":"s3c301-b1","Key":"big","Location":"http://example.invalid/s3c301-b1/big"}'
      case "$SCENARIO" in
        missing-location) AWS_OUT='{"ETag":"\"final-3\"","Bucket":"s3c301-b1","Key":"big"}' ;;
        wrong-complete-key) AWS_OUT='{"ETag":"\"final-3\"","Bucket":"s3c301-b1","Key":"other","Location":"http://example.invalid/big"}' ;;
      esac
      ;;
    head-object)
      AWS_OUT='{"ETag":"\"final-3\"","ContentLength":15728640}'
      case "$SCENARIO" in
        wrong-head-etag) AWS_OUT='{"ETag":"\"different-3\"","ContentLength":15728640}' ;;
        wrong-head-size) AWS_OUT='{"ETag":"\"final-3\"","ContentLength":1}' ;;
        head-failure) AWS_CODE=255; AWS_OUT='HEAD failed' ;;
      esac
      ;;
    get-object)
      case "$CASE" in
        104) cp "${TEST_TMP}/s3c104-source" "${@: -1}" ;;
        212) cp "${TEST_TMP}/s3c212-source" "${@: -1}" ;;
        301)
          cp "${TEST_TMP}/s3c-mp" "${@: -1}"
          AWS_OUT='{"ETag":"\"final-3\"","ContentLength":15728640}'
          case "$SCENARIO" in
            wrong-get-etag) AWS_OUT='{"ETag":"\"different-3\"","ContentLength":15728640}' ;;
            wrong-get-size) AWS_OUT='{"ETag":"\"final-3\"","ContentLength":1}' ;;
          esac
          ;;
        401) cp "${TEST_TMP}/s3c401-source" "${@: -1}" ;;
        1407)
          [ "$version" = v1 ] && [ "$VERSION_PRESENT" -eq 1 ] || {
            AWS_CODE=255; AWS_OUT='NoSuchVersion'; return 0;
          }
          cp "${TEST_TMP}/s3c1407-source" "${@: -1}"
          ;;
        *) echo "unexpected case $CASE" >&2; return 1 ;;
      esac
      case "$SCENARIO" in
        corrupt-body) printf 'corrupt\n' > "${@: -1}" ;;
        get-failure) AWS_CODE=255; AWS_OUT='GET failed' ;;
      esac
      ;;
    list-objects|list-objects-v2)
      if [ "$CASE" = 1407 ]; then
        AWS_OUT='{"Contents":[]}'
        [ "$SCENARIO" != visible-marker ] || AWS_OUT='{"Contents":[{"Key":"f"}]}'
      elif [ "$CASE" = 212 ]; then
        AWS_OUT='{"Contents":[{"Key":"source"}]}'
        [ "$SCENARIO" != leaked-object ] || AWS_OUT='{"Contents":[{"Key":"source"},{"Key":"xl.meta"}]}'
      else
        AWS_OUT='{"Contents":[{"Key":"nested/xl.meta.txt"},{"Key":"obj-a"},{"Key":"obj-b"},{"Key":"obj-c"},{"Key":"obj-d"},{"Key":"obj-e"},{"Key":"xl.meta.txt"}]}'
        case "$SCENARIO:$method" in
          missing-v1:list-objects|missing-v2:list-objects-v2)
            AWS_OUT='{"Contents":[{"Key":"obj-a"},{"Key":"obj-b"},{"Key":"obj-c"},{"Key":"obj-d"},{"Key":"obj-e"}]}' ;;
          malformed-list:*) AWS_OUT='not JSON' ;;
          list-failure:*) AWS_CODE=255; AWS_OUT='LIST failed' ;;
          misordered:*) AWS_OUT='{"Contents":[{"Key":"xl.meta.txt"},{"Key":"obj-a"},{"Key":"obj-b"},{"Key":"obj-c"},{"Key":"obj-d"},{"Key":"obj-e"},{"Key":"nested/xl.meta.txt"}]}' ;;
          leaked-sidecar:*) AWS_OUT='{"Contents":[{"Key":"nested/xl.meta.txt"},{"Key":"obj-a"},{"Key":"obj-b"},{"Key":"obj-c"},{"Key":"obj-d"},{"Key":"obj-e"},{"Key":"xl.meta"},{"Key":"xl.meta.txt"}]}' ;;
        esac
      fi
      ;;
    list-multipart-uploads)
      AWS_OUT='{"Uploads":[]}'
      [ "$SCENARIO" != leaked-upload ] || AWS_OUT='{"Uploads":[{"Key":"xl.meta","UploadId":"leaked"}]}'
      ;;
    list-object-versions)
      AWS_OUT='{"Versions":[],"DeleteMarkers":[{"Key":"f","VersionId":"marker"}]}'
      [ "$SCENARIO" != lost-marker ] || AWS_OUT='{"Versions":[],"DeleteMarkers":[]}'
      ;;
    delete-object)
      if [ "$CASE" = 1407 ]; then
        case "$version" in
          v1) VERSION_PRESENT=0 ;;
          marker) MARKER_PRESENT=0 ;;
          "") MARKER_PRESENT=1; AWS_OUT='{"DeleteMarker":true,"VersionId":"marker"}' ;;
          *) echo "unexpected version $version" >&2; return 1 ;;
        esac
      fi
      ;;
    delete-bucket)
      if [ "$CASE" = 104 ] && [[ "$*" == *s3c104-b2* ]] && ! grep -q 'delete-object.*s3c104-b2' "$CALL_LOG"; then
        AWS_CODE=255; AWS_OUT='BucketNotEmpty'
      elif [ "$CASE" = 1407 ] && { [ "$VERSION_PRESENT" -eq 1 ] || [ "$MARKER_PRESENT" -eq 1 ]; }; then
        AWS_CODE=255; AWS_OUT='BucketNotEmpty'
        case "$SCENARIO" in
          accepts-history) AWS_CODE=0 ;;
          accepts-marker) [ "$VERSION_PRESENT" -ne 0 ] || AWS_CODE=0 ;;
          wrong-error) AWS_OUT='AccessDenied' ;;
        esac
      elif [ "$SCENARIO" = cleanup-failure ]; then
        AWS_CODE=255; AWS_OUT='cleanup failed'
      fi
      ;;
    *) echo "unexpected AWS operation $method" >&2; return 1 ;;
  esac
}
if "test_s3c${CASE}"; then exit 0; else exit 1; fi
'''


class HistoricalS3RegressionTest(unittest.TestCase):
    def run_case(self, case, scenario):
        with tempfile.TemporaryDirectory() as directory:
            call_log = Path(directory, "calls")
            result = subprocess.run(
                ["bash", "-c", HARNESS],
                env={**os.environ, "RUNNER": str(SCRIPT), "CASE": str(case),
                     "SCENARIO": scenario, "CALL_LOG": str(call_log)},
                capture_output=True, text=True, timeout=60,
            )
            return result, call_log.read_text()

    def assert_case(self, case, scenario, success):
        result, calls = self.run_case(case, scenario)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return calls

    def test_valid_responses(self):
        for case in (103, 104, 212, 301, 401, 1407):
            with self.subTest(case=case):
                self.assert_case(case, "success", True)
        for scenario in ("fractional-date", "offset-date"):
            with self.subTest(scenario=scenario):
                self.assert_case(103, scenario, True)

    def test_reject_invalid_creation_dates(self):
        for scenario in ("year-one", "invalid-date", "stale-date", "missing-date", "future-date"):
            with self.subTest(scenario=scenario):
                self.assert_case(103, scenario, False)

    def test_rejection_preserves_current_object(self):
        for scenario in ("corrupt-body", "get-failure"):
            with self.subTest(scenario=scenario):
                self.assert_case(104, scenario, False)

    def test_multipart_response_and_bytes(self):
        for scenario in ("missing-location", "wrong-complete-key", "wrong-head-etag",
                         "wrong-head-size", "head-failure", "wrong-get-etag",
                         "wrong-get-size", "get-failure", "corrupt-body"):
            with self.subTest(scenario=scenario):
                self.assert_case(301, scenario, False)

    def test_listing_keeps_ordinary_keys_and_rejects_missing_or_extra_entries(self):
        for scenario in ("missing-v1", "missing-v2", "malformed-list", "list-failure",
                         "put-failure", "get-failure", "corrupt-body", "misordered", "leaked-sidecar"):
            with self.subTest(scenario=scenario):
                self.assert_case(401, scenario, False)

    def test_reserved_names_are_rejected_at_each_new_write_boundary(self):
        calls = self.assert_case(212, "success", True)
        names = {"XL.META", "nested/Xl.MeTa/child"}
        for name in ("xl.meta", "xl.meta.bkp", "delete-prepared", "delete-quorum-committed"):
            names.update((name, "nested/" + name))
        observed = set()
        for line in calls.splitlines():
            args = shlex.split(line)
            if args[1] in ("put-object", "copy-object", "create-multipart-upload"):
                key = args[args.index("--key") + 1]
                if key != "source":
                    observed.add((args[1], key))
        self.assertEqual(observed, {(method, name) for name in names for method in (
            "put-object", "copy-object", "create-multipart-upload")})
        for scenario in ("accepts-put", "accepts-copy", "accepts-mpu", "accepts-nested",
                         "accepts-uppercase", "accepts-parent", "wrong-code", "wrong-reason",
                         "leaked-object", "leaked-upload", "put-failure", "get-failure",
                         "corrupt-body", "cleanup-failure"):
            with self.subTest(scenario=scenario):
                calls = self.assert_case(212, scenario, False)
                self.assertEqual(calls.splitlines()[-1], "s3api delete-bucket --bucket s3c212-b")

    def test_version_only_bucket_and_cleanup(self):
        for scenario in ("accepts-history", "accepts-marker", "wrong-error",
                         "visible-marker", "lost-marker", "corrupt-body",
                         "get-failure", "cleanup-failure"):
            with self.subTest(scenario=scenario):
                calls = self.assert_case(1407, scenario, False)
                self.assertIn("--version-id v1", calls)
                self.assertIn("--version-id marker", calls)
                self.assertEqual(calls.splitlines()[-1], "s3api delete-bucket --bucket s3c1407-b")
        self.assert_case(1407, "missing-version", False)

    def test_registered_in_topology_suite(self):
        self.assertIn('run_test S3C-1407 "VersionOnlyBucketNotEmpty" test_s3c1407',
                      SCRIPT.read_text())
        self.assertIn('run_test S3C-212 "ReservedStorageNames"    test_s3c212', SCRIPT.read_text())


if __name__ == "__main__":
    unittest.main()
