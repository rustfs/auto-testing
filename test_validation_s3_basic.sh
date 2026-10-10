#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/rustfs-s3-compat-test.sh"

MOCK_DELETED=0
MOCK_BUCKET_DELETED=0
MOCK_BAD_METADATA=0
MOCK_SOURCE=""

aws_run() {
  local method="${2:-}" argument
  AWS_CODE=0
  AWS_OUT='{}'
  case "${method}" in
    create-bucket) ;;
    put-object)
      for argument in "$@"; do
        if [ "${MOCK_EXPECT_BODY:-0}" -eq 1 ]; then MOCK_SOURCE="${argument}"; MOCK_EXPECT_BODY=0; break; fi
        if [ "${argument}" = "--body" ]; then MOCK_EXPECT_BODY=1; fi
      done
      AWS_OUT="$(printf '{\"ETag\":\"\\\"%s\\\"\"}' "$(md5_of_file "${MOCK_SOURCE}")")"
      ;;
    head-object)
      if [ "${MOCK_BAD_METADATA}" -eq 1 ]; then
        AWS_OUT='{"ContentLength":4096,"ETag":"wrong","Metadata":{"validation":"wrong"},"ContentType":"application/octet-stream"}'
      else
        AWS_OUT="$(printf '{\"ContentLength\":4096,\"ETag\":\"\\\"%s\\\"\",\"Metadata\":{\"validation\":\"roundtrip\"},\"ContentType\":\"application/octet-stream\"}' "$(md5_of_file "${MOCK_SOURCE}")")"
      fi
      ;;
    get-object)
      if [ "${MOCK_DELETED}" -eq 1 ]; then
        AWS_CODE=255
        AWS_OUT='NoSuchKey'
      else
        cp "${MOCK_SOURCE}" "${@: -1}"
      fi
      ;;
    list-objects-v2)
      AWS_OUT='{"Contents":[{"Key":"object","Size":4096}]}'
      ;;
    delete-object) MOCK_DELETED=1 ;;
    delete-bucket) MOCK_BUCKET_DELETED=1 ;;
    *) printf 'unexpected AWS call: %s\n' "${method}" >&2; return 1 ;;
  esac
}

test_validation_s3_basic_object_round_trip
[ "${MOCK_BUCKET_DELETED}" -eq 1 ]

MOCK_DELETED=0
MOCK_BUCKET_DELETED=0
MOCK_BAD_METADATA=1
if test_validation_s3_basic_object_round_trip; then
  printf 'validation case accepted bad HEAD metadata\n' >&2
  exit 1
fi
[ "${MOCK_BUCKET_DELETED}" -eq 1 ] || { printf 'failure did not clean bucket\n' >&2; exit 1; }

"${SCRIPT_DIR}/rustfs-s3-compat-test.sh" --validation-case s3.basic-object-round-trip \
  --dry-run --yes --package-url https://example.invalid/rustfs.deb \
  > "${TEST_TMP}/validation-dry-run.log"
grep -q '\[DRY-RUN\] s3.basic-object-round-trip BasicObjectRoundTrip' "${TEST_TMP}/validation-dry-run.log"
if grep -q '\[DRY-RUN\] S3C-' "${TEST_TMP}/validation-dry-run.log"; then
  printf 'validation mode scheduled legacy S3C cases\n' >&2
  exit 1
fi
if "${SCRIPT_DIR}/rustfs-s3-compat-test.sh" --validation-case unknown \
  --dry-run --yes > "${TEST_TMP}/validation-bad-case.log" 2>&1; then
  printf 'unknown validation case was accepted\n' >&2
  exit 1
fi
