#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/rustfs-storage-test.sh"

MOCK_OBJECTS_FILE="${TEST_TMP}/mock-objects.jsonl"
MOCK_BUCKET=""
MOCK_BUCKET_DELETED=0
MOCK_DELETE_FAIL=0
: > "${MOCK_OBJECTS_FILE}"

aws_run() {
  local operation="${2:-}" key="" body="" previous="" argument
  AWS_CODE=0
  AWS_OUT='{}'
  case "${operation}" in
    create-bucket)
      MOCK_BUCKET="$4"
      ;;
    put-object)
      for argument in "$@"; do
        case "${previous}" in
          --key) key="${argument}" ;;
          --body) body="${argument}" ;;
        esac
        previous="${argument}"
      done
      jq -nc --arg key "${key}" --argjson size "$(wc -c < "${body}")" '{Key:$key,Size:$size}' >> "${MOCK_OBJECTS_FILE}"
      ;;
    list-objects-v2)
      AWS_OUT="$(jq -s '{Contents:.}' "${MOCK_OBJECTS_FILE}")"
      ;;
    delete-object) ;;
    delete-bucket)
      MOCK_BUCKET_DELETED=$((MOCK_BUCKET_DELETED + 1))
      if [ "${MOCK_DELETE_FAIL}" -eq 1 ]; then AWS_CODE=1; AWS_OUT='mock cleanup failure'; fi
      ;;
    *) printf 'unexpected AWS operation: %s\n' "${operation}" >&2; return 1 ;;
  esac
}

admin_api() {
  local path="$2" count
  printf '200' > "${ADMIN_API_CODE_FILE}"
  count="$(wc -l < "${MOCK_OBJECTS_FILE}" | tr -d ' ')"
  case "${path}" in
    /rustfs/admin/v3/scanner/status)
      jq -nc --argjson now "$(date -u +%s)" \
        '{enabled:true,freshness:{state:"fresh",last_cycle_end_unix_secs:$now},metrics:{last_cycle_result:"success",leader_lock_held_by_this_process:true},cycle_schedule:{execution_role:"leader"}}'
      ;;
    /rustfs/admin/v3/datausageinfo)
      if [ "${count}" -eq 0 ]; then
        printf '{"scanner_cycle":0,"usage_snapshot_complete":true,"buckets_usage":{}}\n'
      else
        jq -nc --arg bucket "${MOCK_BUCKET}" --argjson bytes "$(jq -s 'map(.Size) | add' "${MOCK_OBJECTS_FILE}")" \
          '{scanner_cycle:1,usage_snapshot_complete:true,usage_snapshot_converged:true,buckets_usage:{($bucket):{objects_count:20,size:$bytes}}}'
      fi
      ;;
    *) printf 'unexpected Admin path: %s\n' "${path}" >&2; return 1 ;;
  esac
}

test_validation_scanner_usage_convergence
[ "${VALIDATION_OBJECT_CLEANUP_STATUS}" = PASS ]
[ "${MOCK_BUCKET_DELETED}" -eq 1 ]
jq -e '.expected_count == 20 and .expected_size == 20690 and (.objects | length) == 20' \
  "${TEST_TMP}/validation-fixture.json" >/dev/null
validation_scanner_usage_matches "${TEST_TMP}/validation-scanner.json" "${TEST_TMP}/validation-usage.json" \
  "${MOCK_BUCKET}" 0 20690 0

jq '.metrics.last_cycle_result = "partial"' "${TEST_TMP}/validation-scanner.json" > "${TEST_TMP}/bad-scanner.json"
if validation_scanner_usage_matches "${TEST_TMP}/bad-scanner.json" "${TEST_TMP}/validation-usage.json" "${MOCK_BUCKET}" 0 20690 0; then
  printf 'partial scanner cycle passed\n' >&2; exit 1
fi
jq '.scanner_cycle = 0' "${TEST_TMP}/validation-usage.json" > "${TEST_TMP}/bad-usage.json"
if validation_scanner_usage_matches "${TEST_TMP}/validation-scanner.json" "${TEST_TMP}/bad-usage.json" "${MOCK_BUCKET}" 0 20690 0; then
  printf 'stale usage snapshot passed\n' >&2; exit 1
fi
jq '.buckets_usage[].size = 1' "${TEST_TMP}/validation-usage.json" > "${TEST_TMP}/bad-usage.json"
if validation_scanner_usage_matches "${TEST_TMP}/validation-scanner.json" "${TEST_TMP}/bad-usage.json" "${MOCK_BUCKET}" 0 20690 0; then
  printf 'wrong fixture size passed\n' >&2; exit 1
fi
jq '.buckets_usage[].objects_count = 19' "${TEST_TMP}/validation-usage.json" > "${TEST_TMP}/bad-usage.json"
if validation_scanner_usage_matches "${TEST_TMP}/validation-scanner.json" "${TEST_TMP}/bad-usage.json" "${MOCK_BUCKET}" 0 20690 0; then
  printf 'wrong object count passed\n' >&2; exit 1
fi
jq '.usage_snapshot_complete = false' "${TEST_TMP}/validation-usage.json" > "${TEST_TMP}/bad-usage.json"
if validation_scanner_usage_matches "${TEST_TMP}/validation-scanner.json" "${TEST_TMP}/bad-usage.json" "${MOCK_BUCKET}" 0 20690 0; then
  printf 'incomplete usage snapshot passed\n' >&2; exit 1
fi

MOCK_DELETE_FAIL=1
: > "${MOCK_OBJECTS_FILE}"
if test_validation_scanner_usage_convergence; then
  printf 'failed cleanup passed\n' >&2; exit 1
fi
[ "${VALIDATION_OBJECT_CLEANUP_STATUS}" = FAIL ]

"${SCRIPT_DIR}/rustfs-storage-test.sh" --validation-case scanner.usage-convergence \
  --dry-run --yes --package-url https://example.invalid/rustfs.deb \
  > "${TEST_TMP}/validation-dry-run.log"
grep -q '\[DRY-RUN\] scanner.usage-convergence UsageConvergence' "${TEST_TMP}/validation-dry-run.log"
grep -q 'RUSTFS_SCANNER_CYCLE=5' "${TEST_TMP}/validation-dry-run.log"
if grep -q '\[DRY-RUN\] STOR-' "${TEST_TMP}/validation-dry-run.log"; then
  printf 'validation mode scheduled legacy storage cases\n' >&2; exit 1
fi
