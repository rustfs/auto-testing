#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/rustfs-kms-test.sh"

MOCK_SOURCE=""
MOCK_BUCKET_DELETED=0
MOCK_LEAK=0

kms_create_key() {
  KMS_KEY_ID="key-validation-1"
}

aws_run() {
  local operation="${2:-}" argument previous="" key="" body="" kid=""
  AWS_CODE=0
  AWS_OUT='{}'
  for argument in "$@"; do
    case "${previous}" in
      --key) key="${argument}" ;;
      --body) body="${argument}" ;;
      --ssekms-key-id) kid="${argument}" ;;
    esac
    previous="${argument}"
  done
  case "${operation}" in
    create-bucket) ;;
    put-object)
      if [[ "${kid}" == missing-* ]]; then
        AWS_CODE=255
        AWS_OUT='InvalidKMSKey'
        if [ "${MOCK_LEAK}" -eq 1 ]; then AWS_OUT="${AWS_OUT} ${marker}"; fi
      else
        MOCK_SOURCE="${body}"
        AWS_OUT="$(jq -nc --arg kid "${kid}" '{ServerSideEncryption:"aws:kms",SSEKMSKeyId:$kid}')"
      fi
      ;;
    head-object)
      if [ "${key}" = rejected ]; then
        AWS_CODE=255
        AWS_OUT='NotFound'
      else
        AWS_OUT="$(jq -nc --arg kid "${KMS_KEY_ID}" --argjson bytes "$(wc -c < "${MOCK_SOURCE}")" \
          '{ServerSideEncryption:"aws:kms",SSEKMSKeyId:$kid,ContentLength:$bytes,Metadata:{validation:"kms-roundtrip"}}')"
      fi
      ;;
    get-object)
      cp "${MOCK_SOURCE}" "${@: -1}"
      ;;
    delete-object) ;;
    delete-bucket) MOCK_BUCKET_DELETED=$((MOCK_BUCKET_DELETED + 1)) ;;
    *) printf 'unexpected AWS operation: %s\n' "${operation}" >&2; return 1 ;;
  esac
}

test_validation_kms_sse_kms_round_trip
[ "${VALIDATION_OBJECT_CLEANUP_STATUS}" = PASS ]
[ "${MOCK_BUCKET_DELETED}" -eq 1 ]
jq -e '.key_id == "key-validation-1" and .metadata.validation == "kms-roundtrip" and .bytes > 4096' \
  "${TEST_TMP}/validation-fixture.json" >/dev/null

MOCK_LEAK=1
if test_validation_kms_sse_kms_round_trip; then
  printf 'plaintext leak passed\n' >&2; exit 1
fi
[ "${MOCK_BUCKET_DELETED}" -eq 2 ] || { printf 'failed case did not clean bucket\n' >&2; exit 1; }

"${SCRIPT_DIR}/rustfs-kms-test.sh" --validation-case kms.sse-kms-round-trip \
  --dry-run --yes --package-url https://example.invalid/rustfs.deb \
  > "${TEST_TMP}/validation-dry-run.log"
grep -q '\[DRY-RUN\] kms.sse-kms-round-trip SseKmsRoundTrip' "${TEST_TMP}/validation-dry-run.log"
if grep -q '\[DRY-RUN\] KMS-' "${TEST_TMP}/validation-dry-run.log"; then
  printf 'validation mode scheduled legacy KMS cases\n' >&2; exit 1
fi
