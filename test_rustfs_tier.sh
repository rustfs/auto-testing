#!/usr/bin/env bash
# shellcheck disable=SC2034 # Globals are consumed by functions sourced below.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-tier-unit.XXXXXX")"
TEST_ROOT="$(cd -- "${TEST_ROOT}" && pwd -P)"
export RUSTFS_TIER_TEST_TMP="${TEST_ROOT}/source"
export TIER_SOURCE_ONLY=1
# shellcheck source=rustfs-tier-test.sh
source "${SCRIPT_DIR}/rustfs-tier-test.sh"
trap 'rm -rf -- "${TEST_ROOT}"' EXIT

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local expected="$1" actual="$2" label="$3"
  [ "${actual}" = "${expected}" ] || fail "${label}: expected '${expected}', got '${actual}'"
}

assert_contains() {
  local needle="$1" haystack="$2" label="$3"
  [[ "${haystack}" == *"${needle}"* ]] || fail "${label}: missing '${needle}' in '${haystack}'"
}

assert_not_contains() {
  local needle="$1" haystack="$2" label="$3"
  [[ "${haystack}" != *"${needle}"* ]] || fail "${label}: unexpectedly contained '${needle}'"
}

joined_args() {
  printf '<%s>' "$@"
}

reset_harness() {
  ARTIFACTS_DIR="${TEST_ROOT}/artifacts-$RANDOM"
  init_artifacts
  CURRENT_TOPOLOGY=single-single
  CURRENT_CASE_ID=TIER-104
  CURRENT_CASE_NAME=TierRemove
  RC_BIN=/usr/bin/true
  ENDPOINT=http://source.test:9000
  TARGET_ENDPOINT=http://target.test:9000
  PASS=0
  FAIL=0
  UNSUPPORTED=0
  FAILED_TESTS=()
}

test_pre_registration_before_failed_create() (
  reset_harness
  case_resources_reset
  aws_run() { AWS_OUT="ambiguous create failure"; AWS_CODE=1; return 1; }
  if new_bucket t104-ss-240101000000-1-src >/dev/null 2>&1; then
    fail "failed bucket create should not pass"
  fi
  assert_eq "t104-ss-240101000000-1-src" "${CASE_SOURCE_BUCKETS[0]}" "source pre-registration"
)

test_cleanup_order_idempotence_and_drain() (
  reset_harness
  case_resources_reset
  register_source_bucket t104-ss-240101000000-1-src
  register_remote_bucket t104-ss-240101000000-1-remote
  register_case_tier T104D2401010000001
  local order="${TEST_ROOT}/cleanup-order"
  cleanup_source_bucket() {
    printf 'source:%s\n' "$1" >> "${order}"
    record_cleanup_failure source-bucket "$1" forced source-failure
    return 1
  }
  cleanup_remote_data() { printf 'remote-data:%s\n' "$1" >> "${order}"; }
  cleanup_tier() {
    printf 'tier:%s\n' "$1" >> "${order}"
    record_cleanup_failure tier "$1" forced tier-failure
    return 1
  }
  cleanup_remote_bucket() { printf 'remote-bucket:%s\n' "$1" >> "${order}"; }

  if case_cleanup unit-test >/dev/null 2>&1; then fail "cleanup with forced failures should fail"; fi
  assert_eq $'source:t104-ss-240101000000-1-src\nremote-data:t104-ss-240101000000-1-remote\ntier:T104D2401010000001\nremote-bucket:t104-ss-240101000000-1-remote' "$(cat "${order}")" "cleanup order/drain"
  assert_eq "2" "$(wc -l < "${RESIDUE_FILE}" | tr -d ' ')" "residue count"
  case_cleanup second-call >/dev/null 2>&1 || fail "idempotent second cleanup should succeed"
  assert_eq "4" "$(wc -l < "${order}" | tr -d ' ')" "idempotent cleanup call count"
)

test_source_bucket_cleanup_retries_bucket_not_empty_then_succeeds() (
  reset_harness
  case_resources_reset
  SECONDS=0
  local bucket=t104-ss-240101000000-1-src delete_calls=0
  local -a attempt_timeouts=() request_times=()
  cleanup_aws() { return 0; }
  cleanup_bucket_data() { return 0; }
  aws_at_run_with_hard_timeout() {
    attempt_timeouts+=("$1")
    request_times+=("${SECONDS}")
    delete_calls=$((delete_calls + 1))
    assert_eq \
      "$(joined_args 5 "${ENDPOINT}" s3api delete-bucket --bucket "${bucket}")" \
      "$(joined_args "$@")" \
      "source delete request ${delete_calls}"
    if [ "${delete_calls}" -le 2 ]; then
      AWS_OUT='An error occurred (BucketNotEmpty) when calling the DeleteBucket operation: retry later'
      AWS_CODE=254
      return 254
    fi
    AWS_OUT=""
    AWS_CODE=0
    return 0
  }
  sleep() { SECONDS=$((SECONDS + $1)); }

  cleanup_source_bucket "${bucket}" || fail "source cleanup should converge"
  assert_eq 3 "${delete_calls}" "eventual-success delete request count"
  assert_eq "$(joined_args 5 5 5)" "$(joined_args "${attempt_timeouts[@]}")" "eventual-success attempt timeouts"
  assert_eq "$(joined_args 0 2 4)" "$(joined_args "${request_times[@]}")" "eventual-success cadence"
  assert_eq 0 "${#CASE_CLEANUP_ERRORS[@]}" "eventual-success diagnostics"
)

test_source_bucket_cleanup_stops_at_bucket_not_empty_deadline() (
  reset_harness
  case_resources_reset
  SECONDS=0
  local bucket=t104-ss-240101000000-1-src delete_calls=0
  local diagnostic="${TEST_ROOT}/source-delete-deadline-diagnostic"
  local -a attempt_timeouts=() request_times=()
  cleanup_aws() { return 0; }
  cleanup_bucket_data() { return 0; }
  aws_at_run_with_hard_timeout() {
    attempt_timeouts+=("$1")
    request_times+=("${SECONDS}")
    delete_calls=$((delete_calls + 1))
    AWS_OUT='An error occurred (BucketNotEmpty) when calling the DeleteBucket operation: retry later'
    AWS_CODE=254
    return 254
  }
  sleep() { SECONDS=$((SECONDS + $1)); }

  if cleanup_source_bucket "${bucket}" 2> "${diagnostic}"; then
    fail "persistent BucketNotEmpty should exhaust the source cleanup deadline"
  fi
  assert_eq 15 "${delete_calls}" "deadline delete request count"
  assert_eq \
    "$(joined_args 5 5 5 5 5 5 5 5 5 5 5 5 5 4 2)" \
    "$(joined_args "${attempt_timeouts[@]}")" \
    "remaining-time attempt bounds"
  assert_eq \
    "$(joined_args 0 2 4 6 8 10 12 14 16 18 20 22 24 26 28)" \
    "$(joined_args "${request_times[@]}")" \
    "deadline request cadence"
  assert_eq 30 "${SECONDS}" "source cleanup deadline"
  assert_eq 1 "${#CASE_CLEANUP_ERRORS[@]}" "deadline diagnostic count"
  assert_eq 1 "$(wc -l < "${diagnostic}" | tr -d ' ')" "deadline stderr diagnostic count"
  assert_eq 1 "$(wc -l < "${RESIDUE_FILE}" | tr -d ' ')" "deadline residue count"
  assert_contains 'reason=deadline_exceeded' "${CASE_CLEANUP_ERRORS[0]}" "deadline reason"
  assert_contains 'error_code=BucketNotEmpty' "${CASE_CLEANUP_ERRORS[0]}" "deadline error code"
  assert_contains 'attempts=15' "${CASE_CLEANUP_ERRORS[0]}" "deadline attempts"
  assert_contains 'deadline_seconds=30' "${CASE_CLEANUP_ERRORS[0]}" "deadline budget"
  assert_contains 'elapsed_seconds=30' "${CASE_CLEANUP_ERRORS[0]}" "deadline elapsed"
)

test_source_bucket_cleanup_does_not_retry_other_errors() (
  reset_harness
  case_resources_reset
  SECONDS=0
  local bucket=t104-ss-240101000000-1-src delete_calls=0
  local diagnostic="${TEST_ROOT}/source-delete-non-retryable-diagnostic"
  cleanup_aws() { return 0; }
  cleanup_bucket_data() { return 0; }
  aws_at_run_with_hard_timeout() {
    delete_calls=$((delete_calls + 1))
    AWS_OUT='An error occurred (AccessDenied) when calling the DeleteBucket operation: message mentions BucketNotEmpty'
    AWS_CODE=254
    return 254
  }
  sleep() { fail "non-retryable source deletion slept before failing"; }

  if cleanup_source_bucket "${bucket}" 2> "${diagnostic}"; then
    fail "AccessDenied source cleanup should fail"
  fi
  assert_eq 1 "${delete_calls}" "non-retryable delete request count"
  assert_eq 1 "${#CASE_CLEANUP_ERRORS[@]}" "non-retryable diagnostic count"
  assert_eq 1 "$(wc -l < "${diagnostic}" | tr -d ' ')" "non-retryable stderr diagnostic count"
  assert_contains 'reason=non_retryable' "${CASE_CLEANUP_ERRORS[0]}" "non-retryable reason"
  assert_contains 'error_code=AccessDenied' "${CASE_CLEANUP_ERRORS[0]}" "non-retryable exact error code"
  assert_contains 'attempts=1' "${CASE_CLEANUP_ERRORS[0]}" "non-retryable attempts"
)

test_source_bucket_cleanup_accepts_no_such_bucket() (
  reset_harness
  case_resources_reset
  SECONDS=0
  local bucket=t104-ss-240101000000-1-src delete_calls=0
  cleanup_aws() { return 0; }
  cleanup_bucket_data() { return 0; }
  aws_at_run_with_hard_timeout() {
    delete_calls=$((delete_calls + 1))
    AWS_OUT='An error occurred (NoSuchBucket) when calling the DeleteBucket operation: already absent'
    AWS_CODE=254
    return 254
  }
  sleep() { fail "NoSuchBucket source deletion slept before succeeding"; }

  cleanup_source_bucket "${bucket}" || fail "NoSuchBucket should satisfy source cleanup"
  assert_eq 1 "${delete_calls}" "NoSuchBucket delete request count"
  assert_eq 0 "${#CASE_CLEANUP_ERRORS[@]}" "NoSuchBucket diagnostics"
)

test_cleanup_upgrades_pass_and_unsupported() (
  reset_harness
  case_cleanup() {
    CASE_CLEANUP_ERRORS=("forced cleanup failure")
    CASE_CLEANUP_DONE=1
    CASE_ACTIVE=0
    return 1
  }
  pass_case() { return 0; }
  unsupported_case() { return 2; }
  failed_case() { return 1; }

  run_test TIER-104 pass pass_case >/dev/null
  assert_eq FAIL "$(jq -r .status "${RESULTS_DIR}/single-single--TIER-104.json")" "PASS cleanup upgrade"
  run_test TIER-105 unsupported unsupported_case >/dev/null
  assert_eq FAIL "$(jq -r .status "${RESULTS_DIR}/single-single--TIER-105.json")" "UNSUPPORTED cleanup upgrade"
  run_test TIER-106 failed failed_case >/dev/null
  assert_eq FAIL "$(jq -r .status "${RESULTS_DIR}/single-single--TIER-106.json")" "FAIL remains fail"
  assert_eq "3" "${FAIL}" "upgraded failure count"
)

test_term_uses_cleanup_fallback() {
  local child_tmp="${TEST_ROOT}/term-child" marker="${TEST_ROOT}/term-cleanup"
  set +e
  RUSTFS_TIER_TEST_TMP="${child_tmp}" TIER_SOURCE_ONLY=1 \
    bash -c '
      set -Eeuo pipefail
      source "$1"
      ARTIFACTS_DIR="$2/artifacts"
      init_artifacts
      CURRENT_TOPOLOGY=single-single
      CURRENT_CASE_ID=TIER-104
      CURRENT_CASE_NAME=TierRemove
      RC_BIN=/usr/bin/true
      case_resources_reset
      CASE_SOURCE_BUCKETS=(t104-ss-240101000000-1-src)
      write_case_result RUNNING 0 PENDING 0
      MARKER_FILE="$3"
      cleanup_source_bucket() { printf "cleaned:%s\n" "$1" >> "${MARKER_FILE}"; }
      kill -TERM $$
    ' bash "${SCRIPT_DIR}/rustfs-tier-test.sh" "${child_tmp}" "${marker}"
  local rc=$?
  set -e
  assert_eq "143" "${rc}" "TERM exit code"
  grep -Fq 'cleaned:t104-ss-240101000000-1-src' "${marker}" \
    || fail "TERM did not execute the registered cleanup"
  assert_eq RUNNING "$(jq -r .status "${child_tmp}/artifacts/cases/single-single--TIER-104.json")" "interrupted result"
}

test_rc_provenance_with_spaced_path_and_digest() (
  reset_harness
  local bin_dir="${TEST_ROOT}/rc path" mock_rc="${TEST_ROOT}/rc path/rc exact"
  mkdir -p "${bin_dir}"
  printf '%s\n' '#!/usr/bin/env bash' \
    'if [ "${1:-}" = "--version" ]; then echo "rc test-version"; else printf "%s" "${RC_CONFIG_DIR}"; fi' \
    > "${mock_rc}"
  chmod +x "${mock_rc}"
  RC_BIN_REQUEST="${mock_rc}"
  RC_CONFIG_DIR="${TEST_ROOT}/isolated config"
  DRY_RUN=0
  resolve_rc_binary
  write_provenance
  assert_eq "$(python3 -c 'import os,sys;print(os.path.realpath(sys.argv[1]))' "${mock_rc}")" "${RC_BIN}" "canonical rc path"
  assert_eq "rc test-version" "${RC_VERSION}" "rc version"
  assert_eq "${RC_CONFIG_DIR}" "$(rc_run probe >/dev/null 2>&1 || true; printf '%s' "${RC_OUT}")" "isolated RC_CONFIG_DIR"
  assert_eq "${RC_SHA256}" "$(jq -r .rc.sha256 "${PROVENANCE_FILE}")" "provenance digest"
  RUSTFS_EXPECTED_RC_SHA256="${RC_SHA256}"
  verify_rc_digest
  if (RUSTFS_EXPECTED_RC_SHA256=deadbeef; verify_rc_digest) >/dev/null 2>&1; then
    fail "digest mismatch should fail"
  fi
)

test_wire_oracles_reject_http_500() (
  reset_harness
  case_resources_reset
  MOCK_STATUS=409
  MOCK_CODE=XMinioAdminTierBackendInUse
  MOCK_MESSAGE='Specified remote tier is already in use'
  admin_api() {
    printf '%s' "${MOCK_STATUS}" > "${ADMIN_API_CODE_FILE}"
    printf '<Error><Code>%s</Code><Message>%s</Message></Error>' "${MOCK_CODE}" "${MOCK_MESSAGE}"
  }
  add_tier() { RC_CODE=1; RC_OUT="${MOCK_CODE}"; }
  tier_is_configured() { return 1; }
  expect_add_tier_error T113C2401010000001 occupied-bucket "" 409 XMinioAdminTierBackendInUse \
    || fail "exact add backend-in-use response should pass"
  MOCK_STATUS=500
  MOCK_CODE='TierNameBackendInUse!'
  MOCK_MESSAGE='tier backend is already in use'
  if expect_add_tier_error T113B2401010000001 occupied-bucket "" 409 XMinioAdminTierBackendInUse >/dev/null 2>&1; then
    fail "HTTP 500/legacy add code must not pass the wire oracle"
  fi

  MOCK_STATUS=400
  MOCK_CODE=XMinioAdminTierBackendNotEmpty
  MOCK_MESSAGE='Specified remote backend is not empty'
  remove_tier() { RC_CODE=1; RC_OUT="${MOCK_CODE}"; }
  tier_is_configured() { return 0; }
  expect_remove_tier_error T104D2401010000001 400 XMinioAdminTierBackendNotEmpty \
    || fail "exact remove backend-not-empty response should pass"
  MOCK_STATUS=500
  MOCK_CODE=TierNameBackendInUse
  MOCK_MESSAGE='tier backend is not empty'
  if expect_remove_tier_error T104D2401010000001 400 XMinioAdminTierBackendNotEmpty >/dev/null 2>&1; then
    fail "HTTP 500/legacy remove code must not pass the wire oracle"
  fi
)

test_admin_api_empty_payload_is_safe_under_nounset() (
  reset_harness
  local args_file="${TEST_ROOT}/admin-api-args" body
  API_ENDPOINT="${ENDPOINT}"
  curl() {
    printf '%s\n' "$@" > "${args_file}"
    while [ "$#" -gt 0 ]; do
      if [ "$1" = -o ]; then printf '{}' > "$2"; break; fi
      shift
    done
    printf 200
  }
  body="$(admin_api POST /rustfs/admin/v3/ilm/transition/run 'bucket=source')"
  assert_eq '{}' "${body}" "empty-payload admin response"
  assert_eq 200 "$(admin_api_code)" "empty-payload admin status"
  assert_not_contains --data-binary "$(cat "${args_file}")" "empty payload has no body argument"
)

test_versioned_inventory_requires_complete_exact_lists() (
  reset_harness
  local versions='[{"Key":"key","VersionId":"v1"},{"Key":"key","VersionId":"v2"}]'
  local markers='[{"Key":"key","VersionId":"marker"}]'
  local current='{"IsTruncated":false,"KeyCount":0}'
  local inventory='{"IsTruncated":false,"Versions":[{"Key":"key","VersionId":"v2"},{"Key":"key","VersionId":"v1"}],"DeleteMarkers":[{"Key":"key","VersionId":"marker"}]}'
  aws_run() {
    AWS_CODE=0
    case "$2" in
      list-objects-v2) AWS_OUT="${current}" ;;
      list-object-versions) AWS_OUT="${inventory}" ;;
      *) fail "unexpected inventory operation: $*" ;;
    esac
    [[ "$(joined_args "$@")" == *'<--no-paginate>'* ]] || fail "inventory must inspect truncation explicitly"
  }
  assert_versioned_bucket_state bucket "${versions}" "${markers}" || fail "exact inventory should pass"
  local invalid
  for invalid in '{"IsTruncated":true}' '{"IsTruncated":false,"Versions":{}}' \
    '{"IsTruncated":false}' '{"IsTruncated":false,"Versions":[{"Key":"key","VersionId":"other"}]}' 'not-json'; do
    inventory="${invalid}"
    if assert_versioned_bucket_state bucket "${versions}" "${markers}" >/dev/null 2>&1; then
      fail "incomplete or malformed version inventory passed: ${invalid}"
    fi
  done
  inventory='{"IsTruncated":false}'
  assert_versioned_bucket_state bucket '[]' '[]' || fail "empty version listing should pass"
  for invalid in '{"KeyCount":0}' '{"IsTruncated":true,"KeyCount":0}' \
    '{"IsTruncated":false,"KeyCount":1,"Contents":[{"Key":"key"}]}' 'not-json'; do
    current="${invalid}"
    if assert_versioned_bucket_state bucket '[]' '[]' >/dev/null 2>&1; then
      fail "invalid current listing passed: ${invalid}"
    fi
  done
)

test_version_delete_checks_individual_results() (
  reset_harness
  local objects='[{"Key":"key","VersionId":"v1"},{"Key":"key","VersionId":"marker"}]'
  local response='{"Deleted":[{"Key":"key","VersionId":"marker"},{"Key":"key","VersionId":"v1"}]}'
  aws_run() {
    assert_eq 'delete-objects' "$2" "version removal operation"
    assert_eq "${objects}" "$(printf '%s' "$6" | jq -c .Objects)" "exact deletion set"
    AWS_CODE=0
    AWS_OUT="${response}"
  }
  delete_object_versions_and_assert bucket "${objects}" || fail "all version results should pass"
  local invalid
  for invalid in '{"Errors":[{"Key":"key","VersionId":"v1","Code":"AccessDenied"}]}' \
    '{"Deleted":[{"Key":"key","VersionId":"v1"}]}' '{}' 'not-json'; do
    response="${invalid}"
    if delete_object_versions_and_assert bucket "${objects}" >/dev/null 2>&1; then
      fail "HTTP 200 with failed/incomplete version deletions passed: ${invalid}"
    fi
  done
)

test_cleanup_tier_remove_retries_only_expected_conflict() (
  reset_harness
  case_resources_reset
  register_case_tier T115UNIT
  local calls="${TEST_ROOT}/tier115-remove-calls"
  : > "${calls}"
  admin_api() {
    printf '%s %s %s\n' "$1" "$3" "${API_REQUEST_TIMEOUT}" >> "${calls}"
    if [ "$(wc -l < "${calls}")" -eq 1 ]; then
      printf 409 > "${ADMIN_API_CODE_FILE}"
      printf '<Error><Code>XMinioAdminTierBackendInUse</Code></Error>'
    else
      printf 200 > "${ADMIN_API_CODE_FILE}"
    fi
  }
  tier_is_configured() { return 1; }
  sleep() { :; }
  wait_tier_removed_after_cleanup T115UNIT || fail "expected conflict should converge"
  assert_eq 2 "$(wc -l < "${calls}" | tr -d ' ')" "bounded remove attempts"
  assert_eq 0 "${#CASE_TIERS[@]}" "tier unregistered after proven removal"
  assert_not_contains force=true "$(cat "${calls}")" "ordinary removal only"
)

test_cleanup_tier_remove_rejects_unexpected_responses() (
  reset_harness
  local status error_code calls="${TEST_ROOT}/tier115-remove-error-calls"
  admin_api() {
    printf 'call\n' >> "${calls}"
    printf '%s' "${status}" > "${ADMIN_API_CODE_FILE}"
    printf '<Error><Code>%s</Code></Error>' "${error_code}"
  }
  tier_is_configured() { fail "failed removal must not check absence"; }
  for status in 500 400 202 000 409; do
    error_code=XMinioAdminTierBackendInUse
    if [ "${status}" = 409 ]; then error_code=UnexpectedConflict; fi
    : > "${calls}"
    if wait_tier_removed_after_cleanup T115UNIT >/dev/null 2>&1; then
      fail "unexpected status/code passed: ${status}/${error_code}"
    fi
    assert_eq 1 "$(wc -l < "${calls}" | tr -d ' ')" "unexpected response is not retried"
  done
  status=409
  error_code=''
  if wait_tier_removed_after_cleanup T115UNIT >/dev/null 2>&1; then fail "malformed conflict passed"; fi
)

test_cleanup_tier_remove_deadline_and_absence_proof() (
  reset_harness
  case_resources_reset
  register_case_tier T115UNIT
  SECONDS=0
  local REMOTE_STATE_TIMEOUT=5 REMOTE_PROBE_INTERVAL=3 API_REQUEST_TIMEOUT=120
  local timeouts="${TEST_ROOT}/tier115-remove-timeouts"
  : > "${timeouts}"
  admin_api() {
    printf '%s\n' "${API_REQUEST_TIMEOUT}" >> "${timeouts}"
    printf 409 > "${ADMIN_API_CODE_FILE}"
    printf '<Error><Code>XMinioAdminTierBackendInUse</Code></Error>'
  }
  sleep() { SECONDS=$((SECONDS + $1)); }
  if wait_tier_removed_after_cleanup T115UNIT >/dev/null 2>&1; then fail "persistent conflict passed"; fi
  assert_eq $'5\n2' "$(cat "${timeouts}")" "each request stays within remaining budget"
  assert_eq 5 "${SECONDS}" "wall-clock deadline"
  assert_eq 120 "${API_REQUEST_TIMEOUT}" "request budget restored"
  assert_eq 1 "${#CASE_TIERS[@]}" "timed out tier remains registered"
  SECONDS=0
  admin_api() { printf 200 > "${ADMIN_API_CODE_FILE}"; }
  tier_is_configured() { return 0; }
  if wait_tier_removed_after_cleanup T115UNIT >/dev/null 2>&1; then fail "HTTP 200 without config disappearance passed"; fi
  assert_eq 1 "${#CASE_TIERS[@]}" "unproven tier remains registered"
)

test_tier_query_valid_contract() (
  reset_harness
  case_resources_reset
  local mock_status=200 mock_body call_file="${TEST_ROOT}/tier-query-valid-calls"
  local jq_call_file="${TEST_ROOT}/tier-query-valid-jq-calls"
  admin_api() {
    printf '%s\t%s\t%s\n' "$1" "$2" "${3:-}" >> "${call_file}"
    printf '%s' "${mock_status}" > "${ADMIN_API_CODE_FILE}"
    printf '%s' "${mock_body}"
  }
  jq() {
    printf 'jq\n' >> "${jq_call_file}"
    command jq "$@"
  }
  assert_query_status() {
    local expected="$1" name="$2" label="$3" actual
    : > "${call_file}"
    : > "${jq_call_file}"
    case_resources_reset
    if tier_is_configured "${name}"; then actual=0; else actual=$?; fi
    assert_eq "${expected}" "${actual}" "${label} status"
    assert_eq 1 "$(wc -l < "${call_file}" | tr -d ' ')" "${label} request count"
    assert_eq 1 "$(wc -l < "${jq_call_file}" | tr -d ' ')" "${label} jq count"
    assert_eq $'GET\t/rustfs/admin/v3/tier\t' "$(< "${call_file}")" "${label} request"
    assert_eq "" "${CASE_FAILURE_KIND}" "${label} neutral diagnostic"
  }

  mock_body='[{"type":"rustfs","rustfs":{"name":"TARGET","secretKey":"not-returned-in-diagnostics"}}]'
  assert_query_status 0 TARGET "provider-only found"
  assert_query_status 1 OTHER "provider-only absent"

  mock_body='[]'
  assert_query_status 1 TARGET "empty list absent"

  mock_body='[{"type":"rustfs","name":"TARGET","rustfs":{}}]'
  assert_query_status 0 TARGET "top-level-only found"

  mock_body='[{"type":"rustfs","name":"TARGET","rustfs":{"name":"TARGET"}}]'
  assert_query_status 0 TARGET "matching dual names found"

  mock_body='[{"type":"future-provider","future-provider":{"name":"TARGET"}}]'
  assert_query_status 0 TARGET "future provider found"

  mock_body='[{"type":"future-provider","future-provider":{"name":"OTHER","metadata":{"name":"TARGET"}}}]'
  assert_query_status 1 TARGET "unrelated nested name ignored"
)

test_tier_query_rejects_invalid_payloads() (
  reset_harness
  case_resources_reset
  local mock_status=200 mock_body call_file="${TEST_ROOT}/tier-query-invalid-calls"
  local jq_call_file="${TEST_ROOT}/tier-query-invalid-jq-calls"
  local actual payload
  local -a invalid_payloads=(
    'not-json'
    '{}'
    '[1]'
    '[{}]'
    '[{"type":"","":{"name":"TARGET"}}]'
    '[{"type":7,"name":"TARGET"}]'
    '[{"type":"rustfs","name":"TARGET"}]'
    '[{"type":"rustfs","rustfs":null,"name":"TARGET"}]'
    '[{"type":"rustfs","rustfs":{}}]'
    '[{"type":"rustfs","name":"","rustfs":{"name":"TARGET"}}]'
    '[{"type":"rustfs","name":7,"rustfs":{"name":"TARGET"}}]'
    '[{"type":"rustfs","rustfs":{"name":""}}]'
    '[{"type":"rustfs","rustfs":{"name":7}}]'
    '[{"type":"rustfs","name":"TOP","rustfs":{"name":"PROVIDER"}}]'
    '[{"type":"rustfs","rustfs":{"metadata":{"name":"TARGET"}}}]'
    '[{"type":"rustfs","rustfs":{"name":"TARGET"}},{"type":"future-provider","future-provider":null}]'
    '[{"type":"rustfs","rustfs":{"name":"TARGET"}},{"type":"minio","minio":{"name":"TARGET"}}]'
  )
  admin_api() {
    printf '%s\t%s\t%s\n' "$1" "$2" "${3:-}" >> "${call_file}"
    printf '%s' "${mock_status}" > "${ADMIN_API_CODE_FILE}"
    printf '%s' "${mock_body}"
  }
  jq() {
    printf 'jq\n' >> "${jq_call_file}"
    command jq "$@"
  }

  for payload in "${invalid_payloads[@]}"; do
    mock_body="${payload}"
    : > "${call_file}"
    : > "${jq_call_file}"
    case_resources_reset
    if tier_is_configured TARGET 2>/dev/null; then actual=0; else actual=$?; fi
    assert_eq 2 "${actual}" "invalid tier-list payload status (${payload})"
    assert_eq 1 "$(wc -l < "${call_file}" | tr -d ' ')" "invalid payload request count"
    assert_eq 1 "$(wc -l < "${jq_call_file}" | tr -d ' ')" "invalid payload jq count"
    assert_eq tier_list_decode "${CASE_FAILURE_KIND}" "invalid payload failure kind"
    assert_contains 'response_length=' "${CASE_FAILURE_DETAIL}" "invalid payload response length"
    assert_contains 'parse_error=' "${CASE_FAILURE_DETAIL}" "invalid payload parser context"
    assert_contains 'output_excerpt=' "${CASE_FAILURE_DETAIL}" "invalid payload excerpt"
  done
)

test_tier_query_failure_diagnostics_are_safe_and_bounded() (
  reset_harness
  case_resources_reset
  ACCESS_KEY=AKIATESTONLY-TIER-LIST
  SECRET_KEY=secret-tier-list-test-value
  local mock_status=503 mock_body call_file="${TEST_ROOT}/tier-query-diagnostic-calls"
  local jq_call_file="${TEST_ROOT}/tier-query-diagnostic-jq-calls"
  local actual diagnostic="${TEST_ROOT}/tier-query-diagnostic"
  admin_api() {
    printf '%s\n' "$1" >> "${call_file}"
    printf '%s' "${mock_status}" > "${ADMIN_API_CODE_FILE}"
    printf '%s' "${mock_body}"
  }
  jq() {
    printf 'jq\n' >> "${jq_call_file}"
    command jq "$@"
  }

  mock_body="Authorization: credential=${ACCESS_KEY}; secret=${SECRET_KEY}"
  : > "${call_file}"
  : > "${jq_call_file}"
  if tier_is_configured TARGET 2> "${diagnostic}"; then actual=0; else actual=$?; fi
  assert_eq 2 "${actual}" "HTTP tier-list failure status"
  assert_eq 1 "$(wc -l < "${call_file}" | tr -d ' ')" "HTTP failure request count"
  assert_eq 0 "$(wc -l < "${jq_call_file}" | tr -d ' ')" "HTTP failure jq count"
  assert_eq tier_list_http "${CASE_FAILURE_KIND}" "HTTP failure kind"
  assert_contains '<redacted: credential-like content>' "${CASE_FAILURE_DETAIL}" "HTTP credential redaction marker"
  assert_not_contains "${ACCESS_KEY}" "${CASE_FAILURE_DETAIL}$(< "${diagnostic}")" "HTTP access key redaction"
  assert_not_contains "${SECRET_KEY}" "${CASE_FAILURE_DETAIL}$(< "${diagnostic}")" "HTTP secret key redaction"

  printf -v mock_body 'upstream-error:%0700d' 0
  : > "${call_file}"
  : > "${jq_call_file}"
  case_resources_reset
  if tier_is_configured TARGET 2> "${diagnostic}"; then actual=0; else actual=$?; fi
  assert_eq 2 "${actual}" "long HTTP tier-list failure status"
  assert_eq 1 "$(wc -l < "${call_file}" | tr -d ' ')" "long HTTP failure request count"
  assert_eq 0 "$(wc -l < "${jq_call_file}" | tr -d ' ')" "long HTTP failure jq count"
  assert_eq tier_list_http "${CASE_FAILURE_KIND}" "long HTTP failure kind"
  assert_contains '...<truncated>' "${CASE_FAILURE_DETAIL}" "HTTP response truncation"

  mock_status=200
  printf -v mock_body '[{"type":"rustfs","rustfs":{"metadata":"%0700d"}}]' 0
  : > "${call_file}"
  : > "${jq_call_file}"
  case_resources_reset
  if tier_is_configured TARGET 2> "${diagnostic}"; then actual=0; else actual=$?; fi
  assert_eq 2 "${actual}" "long schema failure status"
  assert_eq 1 "$(wc -l < "${call_file}" | tr -d ' ')" "long schema failure request count"
  assert_eq 1 "$(wc -l < "${jq_call_file}" | tr -d ' ')" "long schema failure jq count"
  assert_eq tier_list_decode "${CASE_FAILURE_KIND}" "long schema failure kind"
  assert_contains '...<truncated>' "${CASE_FAILURE_DETAIL}" "schema response truncation"
  [ "${#CASE_FAILURE_DETAIL}" -lt 1400 ] || fail "tier-list schema failure detail is too large"

  mock_body="[{\"type\":\"rustfs\",\"rustfs\":{\"name\":\"TARGET\",\"secretKey\":\"${SECRET_KEY}\"}"
  : > "${call_file}"
  : > "${jq_call_file}"
  case_resources_reset
  if tier_is_configured TARGET 2> "${diagnostic}"; then actual=0; else actual=$?; fi
  assert_eq 2 "${actual}" "credential-bearing decode failure status"
  assert_eq 1 "$(wc -l < "${jq_call_file}" | tr -d ' ')" "credential-bearing decode jq count"
  assert_eq tier_list_decode "${CASE_FAILURE_KIND}" "credential-bearing decode failure kind"
  assert_contains '<redacted: credential-like content>' "${CASE_FAILURE_DETAIL}" "decode credential redaction marker"
  assert_not_contains "${ACCESS_KEY}" "${CASE_FAILURE_DETAIL}$(< "${diagnostic}")" "decode access key redaction"
  assert_not_contains "${SECRET_KEY}" "${CASE_FAILURE_DETAIL}$(< "${diagnostic}")" "decode secret key redaction"
)

test_tier_query_rejects_unexpected_parser_output() (
  reset_harness
  case_resources_reset
  local mock_status=200 mock_body='[]' call_file="${TEST_ROOT}/tier-query-parser-output-calls" actual
  local jq_call_file="${TEST_ROOT}/tier-query-parser-output-jq-calls"
  admin_api() {
    printf '%s\n' "$1" >> "${call_file}"
    printf '%s' "${mock_status}" > "${ADMIN_API_CODE_FILE}"
    printf '%s' "${mock_body}"
  }
  jq() {
    command cat >/dev/null
    printf 'jq\n' >> "${jq_call_file}"
    printf 'maybe\n'
  }

  : > "${call_file}"
  : > "${jq_call_file}"
  if tier_is_configured TARGET 2>/dev/null; then actual=0; else actual=$?; fi
  assert_eq 2 "${actual}" "unexpected parser output status"
  assert_eq 1 "$(wc -l < "${call_file}" | tr -d ' ')" "unexpected parser output request count"
  assert_eq 1 "$(wc -l < "${jq_call_file}" | tr -d ' ')" "unexpected parser output jq count"
  assert_eq tier_list_decode "${CASE_FAILURE_KIND}" "unexpected parser output failure kind"
  assert_contains 'unexpected jq result' "${CASE_FAILURE_DETAIL}" "unexpected parser output context"
)

test_tier_presence_assertion_contract() (
  reset_harness
  case_resources_reset
  local mock_query_status actual
  tier_is_configured() {
    if [ "${mock_query_status}" -eq 2 ]; then
      record_case_failure tier_list_http 'operation=ListTiers; status=503'
    fi
    return "${mock_query_status}"
  }

  mock_query_status=1
  assert_tier_absent TARGET || fail "valid absence should satisfy assert_tier_absent"
  assert_eq "" "${CASE_FAILURE_KIND}" "valid absence diagnostic"

  mock_query_status=0
  case_resources_reset
  if assert_tier_absent TARGET 2>/dev/null; then actual=0; else actual=$?; fi
  assert_eq 1 "${actual}" "configured tier violates absence assertion"
  assert_eq tier_unexpectedly_configured "${CASE_FAILURE_KIND}" "absence mismatch failure kind"

  mock_query_status=2
  case_resources_reset
  if assert_tier_absent TARGET 2>/dev/null; then actual=0; else actual=$?; fi
  assert_eq 1 "${actual}" "query error violates absence assertion"
  assert_eq tier_list_http "${CASE_FAILURE_KIND}" "absence query failure preserved"

  mock_query_status=0
  case_resources_reset
  assert_tier_present TARGET || fail "configured tier should satisfy assert_tier_present"
  assert_eq "" "${CASE_FAILURE_KIND}" "valid presence diagnostic"

  mock_query_status=1
  case_resources_reset
  if assert_tier_present TARGET 2>/dev/null; then actual=0; else actual=$?; fi
  assert_eq 1 "${actual}" "absent tier violates presence assertion"
  assert_eq tier_unexpectedly_absent "${CASE_FAILURE_KIND}" "presence mismatch failure kind"

  mock_query_status=2
  case_resources_reset
  if assert_tier_present TARGET 2>/dev/null; then actual=0; else actual=$?; fi
  assert_eq 1 "${actual}" "query error violates presence assertion"
  assert_eq tier_list_http "${CASE_FAILURE_KIND}" "presence query failure preserved"

  mock_query_status=2
  query_error_case() { assert_tier_absent TARGET; }
  run_test TIER-104 query-error query_error_case >/dev/null
  assert_eq FAIL "$(jq -r .status "${RESULTS_DIR}/single-single--TIER-104.json")" "query error case status"
  assert_eq 1 "$(jq -r .case_rc "${RESULTS_DIR}/single-single--TIER-104.json")" "query error case rc"
  assert_eq tier_list_http "$(jq -r .failure_kind "${RESULTS_DIR}/single-single--TIER-104.json")" "query error result failure kind"
)

test_remote_probe_single_page_success() (
  reset_harness
  case_resources_reset
  local mock_key_count=0 mock_call_count=0
  local -a mock_args=()
  aws_target_run_with_timeout() {
    mock_call_count=$((mock_call_count+1))
    mock_args=("$@")
    AWS_OUT="{\"KeyCount\":${mock_key_count},\"Contents\":[]}"
    AWS_CODE=0
    return 0
  }

  remote_key_count tier-probe-bucket "" || fail "empty root probe should succeed"
  assert_eq 0 "${REMOTE_KEY_COUNT}" "empty KeyCount"
  assert_eq 1 "${mock_call_count}" "empty probe request count"
  assert_eq \
    "$(joined_args 15 s3api list-objects-v2 --bucket tier-probe-bucket --prefix "" --max-keys 1 --output json --no-paginate)" \
    "$(joined_args "${mock_args[@]}")" \
    "single-page root probe arguments"

  mock_key_count=1
  remote_key_count tier-probe-bucket occupied/ || fail "non-empty prefix probe should succeed"
  assert_eq 1 "${REMOTE_KEY_COUNT}" "non-empty KeyCount"
  assert_eq 2 "${mock_call_count}" "prefix probe request count"
  assert_eq \
    "$(joined_args 15 s3api list-objects-v2 --bucket tier-probe-bucket --prefix occupied/ --max-keys 1 --output json --no-paginate)" \
    "$(joined_args "${mock_args[@]}")" \
    "single-page prefix probe arguments"

  mock_key_count=0
  mock_call_count=0
  wait_remote_state tier-probe-bucket "" empty || fail "already-empty state should succeed"
  assert_eq 1 "${mock_call_count}" "successful wait request count"
)

test_aws_timeout_wrapper_preserves_exit_and_arguments() (
  reset_harness
  local call_file="${TEST_ROOT}/aws-timeout-wrapper-args"
  local mock_output='bounded aws failure' mock_code=23
  run_bounded() {
    printf '%s\n' "$@" > "${call_file}"
    printf '%s' "${mock_output}"
    return "${mock_code}"
  }

  if aws_at_run_with_timeout 7 http://probe.test:9000 s3api list-objects-v2 --bucket tier-probe-bucket; then
    fail "bounded AWS wrapper should preserve a non-zero exit"
  fi
  assert_eq 23 "${AWS_CODE}" "bounded AWS exit code"
  assert_eq "${mock_output}" "${AWS_OUT}" "bounded AWS output"
  assert_eq \
    "$(printf '%s\n' 7 aws --endpoint-url http://probe.test:9000 --region us-east-1 s3api list-objects-v2 --bucket tier-probe-bucket)" \
    "$(< "${call_file}")" \
    "bounded AWS arguments"
)

test_remote_probe_command_diagnostics_are_safe_and_bounded() (
  reset_harness
  ACCESS_KEY=AKIATESTONLY123
  SECRET_KEY=secret-test-value-456
  local mock_code=42 mock_output='network connection refused' mock_call_count=0
  local diagnostic="${TEST_ROOT}/remote-command-diagnostic"
  aws_target_run_with_timeout() {
    mock_call_count=$((mock_call_count+1))
    AWS_OUT="${mock_output}"
    AWS_CODE="${mock_code}"
    return "${mock_code}"
  }

  case_resources_reset
  if remote_key_count tier-probe-bucket "" 2> "${diagnostic}"; then fail "failed AWS probe should fail closed"; fi
  assert_eq 1 "${mock_call_count}" "failed probe request count"
  assert_eq remote_probe_command "${CASE_FAILURE_KIND}" "command failure kind"
  assert_contains 'operation=ListObjectsV2' "${CASE_FAILURE_DETAIL}" "command operation context"
  assert_contains 'endpoint=http://target.test:9000' "${CASE_FAILURE_DETAIL}" "command endpoint context"
  assert_contains 'bucket=tier-probe-bucket' "${CASE_FAILURE_DETAIL}" "command bucket context"
  assert_contains 'prefix=<root>' "${CASE_FAILURE_DETAIL}" "command root context"
  assert_contains 'exit=42' "${CASE_FAILURE_DETAIL}" "command exit context"
  assert_contains 'network connection refused' "${CASE_FAILURE_DETAIL}" "command output context"

  mock_output="Authorization: credential=${ACCESS_KEY}; secret=${SECRET_KEY}"
  mock_code=2
  mock_call_count=0
  case_resources_reset
  if remote_key_count tier-probe-bucket private/ 2> "${diagnostic}"; then fail "credential-bearing AWS failure should fail"; fi
  assert_eq 1 "${mock_call_count}" "credential failure request count"
  assert_contains '<redacted: credential-like content>' "${CASE_FAILURE_DETAIL}" "credential redaction marker"
  assert_not_contains "${ACCESS_KEY}" "${CASE_FAILURE_DETAIL}$(< "${diagnostic}")" "access key redaction"
  assert_not_contains "${SECRET_KEY}" "${CASE_FAILURE_DETAIL}$(< "${diagnostic}")" "secret key redaction"

  for mock_code in 124 137; do
    mock_output="probe exceeded deadline with exit ${mock_code}"
    mock_call_count=0
    case_resources_reset
    if remote_key_count tier-probe-bucket "" 2> "${diagnostic}"; then fail "timeout ${mock_code} should fail"; fi
    assert_eq 1 "${mock_call_count}" "timeout ${mock_code} request count"
    assert_eq remote_probe_timeout "${CASE_FAILURE_KIND}" "timeout ${mock_code} failure kind"
    assert_contains "exit=${mock_code}" "${CASE_FAILURE_DETAIL}" "timeout ${mock_code} exit"
    assert_contains 'timeout=yes' "${CASE_FAILURE_DETAIL}" "timeout ${mock_code} marker"
  done

  printf -v mock_output 'network-error:%0600d' 0
  mock_code=55
  case_resources_reset
  if remote_key_count tier-probe-bucket "" 2> "${diagnostic}"; then fail "long error should fail"; fi
  assert_contains '...<truncated>' "${CASE_FAILURE_DETAIL}" "bounded output marker"
  [ "${#CASE_FAILURE_DETAIL}" -lt 800 ] || fail "bounded failure detail is too large"
)

test_remote_probe_rejects_invalid_key_counts() (
  reset_harness
  local mock_output mock_call_count diagnostic="${TEST_ROOT}/remote-decode-diagnostic"
  aws_target_run_with_timeout() {
    mock_call_count=$((mock_call_count+1))
    AWS_OUT="${mock_output}"
    AWS_CODE=0
    return 0
  }

  for mock_output in \
    'not-json' \
    '{}' \
    '{"KeyCount":"0"}' \
    '{"KeyCount":-1}' \
    '{"KeyCount":2}' \
    '{"KeyCount":0.5}'; do
    mock_call_count=0
    case_resources_reset
    if remote_key_count tier-probe-bucket occupied/ 2> "${diagnostic}"; then
      fail "invalid KeyCount payload should fail: ${mock_output}"
    fi
    assert_eq 1 "${mock_call_count}" "decode failure request count"
    assert_eq remote_probe_decode "${CASE_FAILURE_KIND}" "decode failure kind"
    assert_eq "" "${REMOTE_KEY_COUNT}" "invalid KeyCount result"
    assert_contains 'response_length=' "${CASE_FAILURE_DETAIL}" "decode response length"
    assert_contains 'parse_error=' "${CASE_FAILURE_DETAIL}" "decode parser context"
    assert_contains 'output_excerpt=' "${CASE_FAILURE_DETAIL}" "decode output excerpt"
  done
)

test_remote_wait_uses_wall_clock_deadline() (
  reset_harness
  case_resources_reset
  REMOTE_STATE_TIMEOUT=5
  REMOTE_PROBE_TIMEOUT=15
  REMOTE_PROBE_INTERVAL=2
  SECONDS=0
  local probe_calls=0 diagnostic="${TEST_ROOT}/remote-wait-diagnostic"
  local -a probe_timeouts=()
  remote_key_count() {
    probe_calls=$((probe_calls+1))
    probe_timeouts+=("$3")
    REMOTE_KEY_COUNT=1
    return 0
  }
  sleep() { SECONDS=$((SECONDS+$1)); }

  if wait_remote_state tier-probe-bucket occupied/ empty 2> "${diagnostic}"; then
    fail "never-empty remote state should time out"
  fi
  assert_eq 3 "${probe_calls}" "deadline probe count"
  assert_eq "$(joined_args 5 3 1)" "$(joined_args "${probe_timeouts[@]}")" "remaining-time probe bounds"
  assert_eq remote_state_timeout "${CASE_FAILURE_KIND}" "state timeout failure kind"
  assert_contains 'elapsed=5' "${CASE_FAILURE_DETAIL}" "wall-clock elapsed time"
  assert_contains 'last_key_count=1' "${CASE_FAILURE_DETAIL}" "state timeout last count"
)

test_remote_wait_hard_timeout_stays_inside_total_budget() (
  reset_harness
  case_resources_reset
  local mock_bin="${TEST_ROOT}/hard-timeout-bin" call_file="${TEST_ROOT}/hard-timeout-calls"
  local diagnostic="${TEST_ROOT}/hard-timeout-diagnostic" elapsed
  mkdir -p "${mock_bin}"
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'printf "call\n" >> "${MOCK_AWS_CALL_FILE}"' \
    'printf probe-started' \
    "trap '' TERM" \
    'while :; do sleep 10; done' \
    > "${mock_bin}/aws"
  chmod +x "${mock_bin}/aws"
  : > "${call_file}"
  export MOCK_AWS_CALL_FILE="${call_file}"
  PATH="${mock_bin}:${PATH}"
  export PATH
  REMOTE_STATE_TIMEOUT=1
  REMOTE_PROBE_TIMEOUT=15
  REMOTE_PROBE_INTERVAL=1
  SECONDS=0

  if wait_remote_state tier-probe-bucket "" empty 2> "${diagnostic}"; then
    fail "TERM-ignoring probe should hit the hard deadline"
  fi
  elapsed="${SECONDS}"
  [ "${elapsed}" -le "${REMOTE_STATE_TIMEOUT}" ] \
    || fail "hard timeout exceeded total budget: elapsed=${elapsed}, budget=${REMOTE_STATE_TIMEOUT}"
  assert_eq 1 "$(wc -l < "${call_file}" | tr -d ' ')" "hard-timeout probe request count"
  assert_eq 137 "${AWS_CODE}" "hard-timeout AWS exit code"
  assert_contains probe-started "${AWS_OUT}" "hard-timeout AWS output"
  assert_eq remote_probe_timeout "${CASE_FAILURE_KIND}" "hard-timeout failure kind"
  assert_contains 'exit=137' "${CASE_FAILURE_DETAIL}" "hard-timeout exit evidence"
  assert_contains 'timeout=yes' "${CASE_FAILURE_DETAIL}" "hard-timeout marker"
)

test_failure_result_keeps_primary_and_cleanup_evidence() (
  reset_harness
  case_cleanup() {
    record_cleanup_failure remote-bucket tier-probe-bucket delete-bucket BucketNotEmpty
    CASE_CLEANUP_DONE=1
    CASE_ACTIVE=0
    return 1
  }
  silent_case() { return 1; }

  run_test TIER-104 silent silent_case >/dev/null
  local result="${RESULTS_DIR}/single-single--TIER-104.json" log_path
  assert_eq FAIL "$(jq -r .status "${result}")" "silent failure status"
  assert_eq case_failed_without_diagnostic "$(jq -r .failure_kind "${result}")" "silent failure kind"
  assert_contains 'without diagnostic output' "$(jq -r .failure_detail "${result}")" "silent failure detail"
  assert_contains 'BucketNotEmpty' "$(jq -r .cleanup_detail "${result}")" "cleanup detail"
  assert_eq FAIL "$(jq -r .cleanup_status "${result}")" "cleanup failure status"
  log_path="$(jq -r .log_path "${result}")"
  assert_eq logs/single-single--TIER-104.log "${log_path}" "relative failure log path"
  [ -s "${ARTIFACTS_DIR}/${log_path}" ] || fail "silent failure log is missing or empty"
  assert_contains 'without diagnostic output' "$(< "${ARTIFACTS_DIR}/${log_path}")" "generic log diagnostic"
  assert_contains 'BucketNotEmpty' "$(< "${RESIDUE_FILE}")" "cleanup residue"

  reset_harness
  case_cleanup() {
    record_cleanup_failure remote-bucket tier-probe-bucket delete-bucket BucketNotEmpty
    CASE_CLEANUP_DONE=1
    CASE_ACTIVE=0
    return 1
  }
  primary_case() {
    record_case_failure wire_assertion 'expected HTTP 409, got HTTP 500'
    return 1
  }
  run_test TIER-113 primary-and-cleanup primary_case >/dev/null
  result="${RESULTS_DIR}/single-single--TIER-113.json"
  assert_eq wire_assertion "$(jq -r .failure_kind "${result}")" "primary failure kind preserved"
  assert_contains 'expected HTTP 409' "$(jq -r .failure_detail "${result}")" "primary failure detail preserved"
  assert_contains 'BucketNotEmpty' "$(jq -r .cleanup_detail "${result}")" "separate cleanup detail preserved"
)

LOGGED_OUTPUT=""
LOGGED_RC=0

run_logged_child() {
  local requested="$1" token="$2" suite_rc="$3" child_tmp="$4"
  local mode="${5:-normal}" phase="${6:-}" fallback_dir="${7:-${TEST_ROOT}/fallback}"
  local output_file="${child_tmp}.output" term_trigger=""
  if [ "${mode}" = full-stopped-logger ]; then
    term_trigger="${child_tmp}/full-stopped-ready"
  fi
  mkdir -p "${child_tmp}"
  if [ ! -e "${fallback_dir}" ]; then mkdir -p "${fallback_dir}"; fi
  set +e
  TIER_SOURCE_ONLY=1 \
    RUSTFS_TIER_TEST_TMP="${child_tmp}" \
    TMPDIR="${fallback_dir}" \
    LOG_SWAP_TARGET="${LOG_SWAP_TARGET:-}" \
    python3 -c '
import errno
import os
import signal
import subprocess
import sys
import time

timeout = float(sys.argv[1])
trigger = sys.argv[2]
command = sys.argv[3:]
process = subprocess.Popen(command, pass_fds=(8, 9), start_new_session=True)
deadline = time.monotonic() + timeout

if trigger:
    while not os.path.exists(trigger):
        status = process.poll()
        if status is not None:
            raise SystemExit(status if status >= 0 else 128 - status)
        if time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    if os.path.exists(trigger) and process.poll() is None:
        os.kill(process.pid, signal.SIGTERM)

try:
    remaining = max(deadline - time.monotonic(), 0.001)
    status = process.wait(timeout=remaining)
except subprocess.TimeoutExpired:
    print("[TEST WATCHDOG] logged child exceeded its hard deadline", file=sys.stderr)
    for action in (signal.SIGCONT, signal.SIGTERM):
        try:
            os.killpg(process.pid, action)
        except (ProcessLookupError, PermissionError):
            pass
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        process.wait(timeout=1)
    raise SystemExit(124)

status = status if status >= 0 else 128 - status
raise SystemExit(status)
' "${LOGGED_CHILD_TIMEOUT_SECONDS:-20}" "${term_trigger}" /bin/bash -c '
      set -Eeuo pipefail
      source "$1"
      LOG_FILE="$2"
      if [ -n "$6" ]; then
        TEST_LOG_PHASE="$6"
        log_capture_phase() {
          if [ "${TEST_LOG_PHASE}" = launched-stopped ] && [ "$1" = launched ]; then
            kill -STOP "${LOG_CAPTURE_PID}"
            kill -TERM "$$"
          elif [ "${TEST_LOG_PHASE}" = ready-dead ] && [ "$1" = ready ]; then
            kill -TERM "${LOG_CAPTURE_PID}"
            wait_for_log_process_exit_bounded "${LOG_CAPTURE_PID}" 20 2>/dev/null || true
          elif [ "${TEST_LOG_PHASE}" = redirected-dead ] && [ "$1" = redirected ]; then
            kill -TERM "${LOG_CAPTURE_PID}"
            wait_for_log_process_exit_bounded "${LOG_CAPTURE_PID}" 20 2>/dev/null || true
          elif [ "$1" = "${TEST_LOG_PHASE}" ]; then
            kill -TERM "$$"
          fi
        }
      fi
      start_log_capture
      printf "fd8-preserved\n" >&8
      printf "fd9-preserved\n" >&9
      case "$5" in
        normal) ;;
        inherited-writer) sleep 3 & ;;
        signal-with-writer) sleep 3 & kill -TERM "$$" ;;
        stopped-logger)
          kill -STOP "${LOG_CAPTURE_PID}"
          (sleep 1; kill -CONT "${LOG_CAPTURE_PID}") </dev/null >/dev/null 2>&1 &
          ;;
        killed-logger) ;;
        full-stopped-logger)
          LOG_CAPTURE_FAILURE_TICKS=20
          CASE_ACTIVE=1
          case_cleanup() {
            printf "full-stopped-cleanup-ran\n" > "${TEST_TMP}/full-stopped-cleanup-ran"
            CASE_ACTIVE=0
          }
          kill -STOP "${LOG_CAPTURE_PID}"
          python3 -c "import fcntl, os; flags = fcntl.fcntl(1, fcntl.F_GETFL); fcntl.fcntl(1, fcntl.F_SETFL, flags | os.O_NONBLOCK); chunk = b\"x\" * 4096
while True:
    try: os.write(1, chunk)
    except BlockingIOError: break"
          : > "${TEST_TMP}/full-stopped-ready"
          while :; do sleep 1; done
          ;;
        pipe-cleanup)
          CASE_ACTIVE=1
          case_cleanup() {
            printf "pipe-cleanup-ran\n" > "${TEST_TMP}/pipe-cleanup-ran"
            CASE_ACTIVE=0
          }
          ;;
        swap-symlink)
          mv -- "$2" "$2.opened"
          ln -s -- "${LOG_SWAP_TARGET}" "$2"
          ;;
        swap-fifo)
          mv -- "$2" "$2.opened"
          mkfifo "$2"
          ;;
        *) printf "unknown child mode: %s\n" "$5" >&2; exit 97 ;;
      esac
      printf "%s\n" "$3"
      if [ "$5" = killed-logger ]; then
        trap '' PIPE
        kill -KILL "${LOG_CAPTURE_PID}"
        sleep 0.1
      fi
      exit "$4"
    ' bash "${SCRIPT_DIR}/rustfs-tier-test.sh" "${requested}" "${token}" \
      "${suite_rc}" "${mode}" "${phase}" \
      8>"${child_tmp}.fd8" 9>"${child_tmp}.fd9" >"${output_file}" 2>&1
  LOGGED_RC=$?
  set -e
  LOGGED_OUTPUT="$(< "${output_file}")"
}

test_log_capture_truncates_and_waits_between_invocations() {
  local log_file="${TEST_ROOT}/reused-tier.log"
  printf 'stale-sentinel\n' > "${log_file}"

  run_logged_child "${log_file}" first-run-token 0 "${TEST_ROOT}/log-child-1"
  assert_eq 0 "${LOGGED_RC}" "first logged invocation exit"
  assert_contains first-run-token "$(< "${log_file}")" "first logged invocation"
  assert_not_contains stale-sentinel "$(< "${log_file}")" "stale log truncation"
  assert_eq fd8-preserved "$(< "${TEST_ROOT}/log-child-1.fd8")" "caller fd8 preservation"
  assert_eq fd9-preserved "$(< "${TEST_ROOT}/log-child-1.fd9")" "caller fd9 preservation"

  run_logged_child "${log_file}" second-run-token 0 "${TEST_ROOT}/log-child-2"
  assert_eq 0 "${LOGGED_RC}" "second logged invocation exit"
  assert_contains second-run-token "$(< "${log_file}")" "second logged invocation"
  assert_not_contains first-run-token "$(< "${log_file}")" "previous invocation isolation"

  local slow_log="${TEST_ROOT}/slow-tier.log"
  SECONDS=0
  run_logged_child "${slow_log}" slow-flush-token 0 \
    "${TEST_ROOT}/log-child-slow" stopped-logger
  local elapsed="${SECONDS}"
  assert_eq 0 "${LOGGED_RC}" "stopped logger invocation exit"
  [ "${elapsed}" -ge 1 ] || fail "logged invocation returned before the logger resumed and flushed"
  assert_contains slow-flush-token "$(< "${slow_log}")" "resumed logger persisted output"
}

test_log_capture_preserves_suite_and_logger_failures() {
  local log_file="${TEST_ROOT}/status-tier.log"
  run_logged_child "${log_file}" suite-failed-token 23 "${TEST_ROOT}/log-child-suite-fail"
  assert_eq 23 "${LOGGED_RC}" "suite failure exit code"

  run_logged_child "${log_file}" logger-failed-token 0 \
    "${TEST_ROOT}/log-child-logger-fail" killed-logger
  [ "${LOGGED_RC}" -ne 0 ] || fail "logger failure was not propagated"
  run_logged_child "${log_file}" both-failed-token 23 \
    "${TEST_ROOT}/log-child-both-fail" killed-logger
  case "${LOGGED_RC}" in
    23|141) ;;
    *) fail "combined suite/logger failures must stay nonzero: got ${LOGGED_RC}" ;;
  esac
}

test_log_capture_does_not_wait_for_inherited_writers() {
  local log_file="${TEST_ROOT}/inherited-writer.log"
  SECONDS=0
  run_logged_child "${log_file}" inherited-writer-token 0 \
    "${TEST_ROOT}/log-child-inherited" inherited-writer
  local elapsed="${SECONDS}"
  assert_eq 0 "${LOGGED_RC}" "inherited writer invocation exit"
  [ "${elapsed}" -lt 3 ] || fail "inherited FIFO writer delayed normal completion"
  assert_contains inherited-writer-token "$(< "${log_file}")" "inherited writer log flush"

  SECONDS=0
  run_logged_child "${log_file}" signal-writer-token 0 \
    "${TEST_ROOT}/log-child-signal-writer" signal-with-writer
  elapsed="${SECONDS}"
  assert_eq 143 "${LOGGED_RC}" "TERM status with inherited writer"
  [ "${elapsed}" -lt 3 ] || fail "inherited FIFO writer delayed TERM completion"
}

test_full_fifo_and_stopped_logger_has_bounded_term() {
  local log_file="${TEST_ROOT}/full-stopped-logger.log" child_tmp
  child_tmp="${TEST_ROOT}/log-child-full-stopped"
  SECONDS=0
  LOGGED_CHILD_TIMEOUT_SECONDS=6 \
  run_logged_child "${log_file}" unreachable-token 0 \
    "${child_tmp}" full-stopped-logger
  local elapsed="${SECONDS}"
  assert_eq 143 "${LOGGED_RC}" "TERM status with a full FIFO and stopped logger"
  [ "${elapsed}" -lt 4 ] || fail "full FIFO blocked stop-marker delivery beyond its deadline"
  assert_eq full-stopped-cleanup-ran \
    "$(< "${child_tmp}/full-stopped-cleanup-ran")" \
    "full FIFO TERM case cleanup"
  if find "${child_tmp}" -maxdepth 1 -type d \
    -name 'rustfs-tier-log.*' -print -quit | grep -q .; then
    fail "full FIFO TERM left its control directory behind"
  fi
  if pgrep -f -- "${child_tmp}" >/dev/null 2>&1; then
    fail "full FIFO TERM left a logger process behind"
  fi
}

test_full_fifo_stopped_logger_parallel_stress() {
  local index child_tmp log_file worker
  local -a workers=()
  for index in 1 2 3 4 5 6 7 8; do
    child_tmp="${TEST_ROOT}/log-child-full-stopped-${index}"
    log_file="${TEST_ROOT}/full-stopped-logger-${index}.log"
    (
      LOGGED_CHILD_TIMEOUT_SECONDS=6 \
        run_logged_child "${log_file}" "unreachable-token-${index}" 0 \
          "${child_tmp}" full-stopped-logger
      printf '%s\n' "${LOGGED_RC}" > "${child_tmp}.rc"
    ) &
    workers+=("$!")
  done

  for worker in "${workers[@]}"; do
    wait "${worker}"
  done
  for index in 1 2 3 4 5 6 7 8; do
    child_tmp="${TEST_ROOT}/log-child-full-stopped-${index}"
    if [ "$(< "${child_tmp}.rc")" != 143 ]; then
      fail "parallel full FIFO TERM status ${index}: $(tail -n 20 "${child_tmp}.output")"
    fi
    assert_eq full-stopped-cleanup-ran \
      "$(< "${child_tmp}/full-stopped-cleanup-ran")" \
      "parallel full FIFO cleanup ${index}"
    if find "${child_tmp}" -maxdepth 1 -type d \
      -name 'rustfs-tier-log.*' -print -quit | grep -q .; then
      fail "parallel full FIFO run ${index} left residue: $(find "${child_tmp}" -maxdepth 2 -print | sort)"
    fi
    if pgrep -f -- "${child_tmp}" >/dev/null 2>&1; then
      fail "parallel full FIFO run ${index} left a logger process behind"
    fi
  done
}

test_log_capture_startup_signal_phases_are_safe() {
  local phase log_file
  for phase in begin control-dir fifo anchor saved active launched ready redirected saved-closed; do
    log_file="${TEST_ROOT}/startup-${phase}.log"
    SECONDS=0
    run_logged_child "${log_file}" "startup-${phase}-token" 0 \
      "${TEST_ROOT}/log-child-startup-${phase}" normal "${phase}"
    assert_eq 143 "${LOGGED_RC}" "TERM status during ${phase} phase"
    [ "${SECONDS}" -lt 3 ] || fail "TERM during ${phase} phase was not bounded"
  done

  log_file="${TEST_ROOT}/startup-launched-stopped.log"
  SECONDS=0
  run_logged_child "${log_file}" startup-launched-stopped-token 0 \
    "${TEST_ROOT}/log-child-startup-launched-stopped" normal launched-stopped
  assert_eq 143 "${LOGGED_RC}" "TERM status while pre-ready logger is stopped"
  [ "${SECONDS}" -lt 3 ] || fail "stopped pre-ready logger delayed TERM completion"

  log_file="${TEST_ROOT}/startup-ready-dead.log"
  SECONDS=0
  run_logged_child "${log_file}" should-not-enter-workload 0 \
    "${TEST_ROOT}/log-child-startup-ready-dead" normal ready-dead
  [ "${LOGGED_RC}" -ne 0 ] || fail "dead post-ready logger did not fail startup"
  [ "${LOGGED_RC}" -ne 137 ] || fail "dead post-ready logger hit the external hard cutoff"
  [ "${SECONDS}" -lt 3 ] || fail "dead post-ready logger blocked FIFO writer setup"
  assert_not_contains should-not-enter-workload "${LOGGED_OUTPUT}" "post-ready crash workload boundary"
  if find "${TEST_ROOT}/log-child-startup-ready-dead" -maxdepth 1 \
    -type d -name 'rustfs-tier-log.*' -print -quit | grep -q .; then
    fail "post-ready logger crash left its control directory behind"
  fi

  log_file="${TEST_ROOT}/startup-redirected-dead.log"
  SECONDS=0
  run_logged_child "${log_file}" should-hit-sigpipe 0 \
    "${TEST_ROOT}/log-child-startup-redirected-dead" pipe-cleanup redirected-dead
  assert_eq 141 "${LOGGED_RC}" "SIGPIPE status after post-preflight logger death"
  [ "${SECONDS}" -lt 3 ] || fail "post-preflight logger death was not bounded"
  assert_eq pipe-cleanup-ran \
    "$(< "${TEST_ROOT}/log-child-startup-redirected-dead/pipe-cleanup-ran")" \
    "SIGPIPE case cleanup"
  if find "${TEST_ROOT}/log-child-startup-redirected-dead" -maxdepth 1 \
    -type d -name 'rustfs-tier-log.*' -print -quit | grep -q .; then
    fail "post-preflight logger crash left its control directory behind"
  fi
}

test_log_file_fallback_rejects_unsafe_paths() {
  local fallback_dir="${TEST_ROOT}/fallback-files" fallback_path
  local target="${TEST_ROOT}/symlink-target" requested="${TEST_ROOT}/requested-symlink"
  mkdir -p "${fallback_dir}"
  printf 'protected-sentinel\n' > "${target}"
  ln -s "${target}" "${requested}"
  run_logged_child "${requested}" symlink-fallback-token 0 \
    "${TEST_ROOT}/log-child-symlink" normal "" "${fallback_dir}"
  assert_eq 0 "${LOGGED_RC}" "symlink fallback invocation"
  assert_eq protected-sentinel "$(< "${target}")" "symlink target preservation"
  [ -L "${requested}" ] || fail "requested symlink was replaced"
  fallback_path="$(printf '%s\n' "${LOGGED_OUTPUT}" | sed -n 's/.*; using //p' | tail -1)"
  [ -n "${fallback_path}" ] || fail "symlink fallback warning omitted selected path"
  [ "${fallback_path}" != "${requested}" ] || fail "symlink path did not use fallback"
  [ -f "${fallback_path}" ] || fail "selected symlink fallback log was not created"
  assert_contains symlink-fallback-token "$(< "${fallback_path}")" "symlink fallback output"

  requested="${TEST_ROOT}/requested-directory"
  mkdir -p "${requested}"
  run_logged_child "${requested}" directory-fallback-token 0 \
    "${TEST_ROOT}/log-child-directory" normal "" "${fallback_dir}"
  assert_eq 0 "${LOGGED_RC}" "directory fallback invocation"
  assert_contains "${requested}" "${LOGGED_OUTPUT}" "directory fallback warning path"

  requested="${TEST_ROOT}/requested-fifo"
  mkfifo "${requested}"
  SECONDS=0
  run_logged_child "${requested}" fifo-fallback-token 0 \
    "${TEST_ROOT}/log-child-fifo" normal "" "${fallback_dir}"
  assert_eq 0 "${LOGGED_RC}" "FIFO fallback invocation"
  [ "${SECONDS}" -lt 3 ] || fail "substituted FIFO blocked secure open"
  [ -p "${requested}" ] || fail "requested FIFO was replaced"

  local parent_file="${TEST_ROOT}/parent-is-file"
  printf 'parent\n' > "${parent_file}"
  requested="${parent_file}/tier.log"
  run_logged_child "${requested}" parent-fallback-token 0 \
    "${TEST_ROOT}/log-child-parent" normal "" "${fallback_dir}"
  assert_eq 0 "${LOGGED_RC}" "invalid parent fallback invocation"
  assert_contains "${requested}" "${LOGGED_OUTPUT}" "fallback warning requested path"

  requested="${TEST_ROOT}/fallback-failure-symlink"
  ln -s "${target}" "${requested}"
  run_logged_child "${requested}" fallback-failure-token 0 \
    "${TEST_ROOT}/log-child-fallback-failure" normal "" "${parent_file}"
  [ "${LOGGED_RC}" -ne 0 ] || fail "fallback creation failure did not fail closed"
  assert_eq protected-sentinel "$(< "${target}")" "fallback failure target preservation"
}

test_log_destination_swap_keeps_open_inode() {
  local target="${TEST_ROOT}/swap-protected" requested="${TEST_ROOT}/swap-requested.log"
  printf 'protected-sentinel\n' > "${target}"
  printf 'stale\n' > "${requested}"
  LOG_SWAP_TARGET="${target}" run_logged_child "${requested}" swap-symlink-token 0 \
    "${TEST_ROOT}/log-child-swap-symlink" swap-symlink
  assert_eq 0 "${LOGGED_RC}" "post-open symlink swap invocation"
  assert_eq protected-sentinel "$(< "${target}")" "post-open symlink target preservation"
  assert_contains swap-symlink-token "$(< "${requested}.opened")" "open inode received log output"
  [ -L "${requested}" ] || fail "post-open symlink substitution was followed or replaced"

  rm -- "${requested}"
  printf 'stale\n' > "${requested}"
  SECONDS=0
  run_logged_child "${requested}" swap-fifo-token 0 \
    "${TEST_ROOT}/log-child-swap-fifo" swap-fifo
  assert_eq 0 "${LOGGED_RC}" "post-open FIFO swap invocation"
  [ "${SECONDS}" -lt 3 ] || fail "post-open FIFO substitution blocked logging"
  assert_contains swap-fifo-token "$(< "${requested}.opened")" "FIFO swap kept open inode"
  [ -p "${requested}" ] || fail "post-open FIFO substitution was reopened"
}

test_secure_log_owner_link_and_mode_contract() {
  python3 - "${SCRIPT_DIR}/rustfs-tier-log-capture.py" "${TEST_ROOT}" <<'PY'
import importlib.util
import os
import stat
import sys
from unittest import mock

spec = importlib.util.spec_from_file_location("tier_log_capture", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
root = sys.argv[2]

mode_path = os.path.join(root, "existing-mode.log")
with open(mode_path, "wb") as stream:
    stream.write(b"stale")
os.chmod(mode_path, 0o644)
descriptor = module._open_requested(mode_path)
try:
    actual_mode = stat.S_IMODE(os.fstat(descriptor).st_mode)
    if actual_mode != 0o600:
        raise AssertionError(f"existing log mode is {actual_mode:o}, expected 600")
finally:
    os.close(descriptor)

foreign_path = os.path.join(root, "foreign-owner.log")
with open(foreign_path, "wb") as stream:
    stream.write(b"protected-owner")
actual_uid = os.geteuid()
with mock.patch.object(module.os, "geteuid", return_value=actual_uid + 1):
    try:
        module._open_requested(foreign_path)
    except module.LogOpenError:
        pass
    else:
        raise AssertionError("foreign-owned log destination was accepted")
with open(foreign_path, "rb") as stream:
    if stream.read() != b"protected-owner":
        raise AssertionError("foreign-owned log was truncated before rejection")

hardlink_target = os.path.join(root, "hardlink-target.log")
hardlink_path = os.path.join(root, "hardlink-requested.log")
with open(hardlink_target, "wb") as stream:
    stream.write(b"protected-hardlink")
os.link(hardlink_target, hardlink_path)
try:
    module._open_requested(hardlink_path)
except module.LogOpenError:
    pass
else:
    raise AssertionError("multiply-linked log destination was accepted")
with open(hardlink_target, "rb") as stream:
    if stream.read() != b"protected-hardlink":
        raise AssertionError("hard-linked target was truncated before rejection")
PY
}

run_versioned_cleanup_contract_tests() {
  test_admin_api_empty_payload_is_safe_under_nounset
  test_versioned_inventory_requires_complete_exact_lists
  test_version_delete_checks_individual_results
  test_cleanup_tier_remove_retries_only_expected_conflict
  test_cleanup_tier_remove_rejects_unexpected_responses
  test_cleanup_tier_remove_deadline_and_absence_proof
}

if [ "${RUSTFS_TIER_TEST_MODE:-all}" = versioned-cleanup ]; then
  run_versioned_cleanup_contract_tests
  printf 'PASS: versioned tier deletion inventories, per-object results, retry, deadline, and removal proof\n'
  exit 0
fi

if [ "${RUSTFS_TIER_TEST_MODE:-all}" = log-stress ]; then
  test_full_fifo_and_stopped_logger_has_bounded_term
  test_full_fifo_stopped_logger_parallel_stress
  printf 'PASS: focused full-FIFO/SIGSTOP log lifecycle stress\n'
  exit 0
fi

python3 -m unittest discover -s "${SCRIPT_DIR}" -p test_rustfs_tier_mqtt.py

test_pre_registration_before_failed_create
test_cleanup_order_idempotence_and_drain
test_source_bucket_cleanup_retries_bucket_not_empty_then_succeeds
test_source_bucket_cleanup_stops_at_bucket_not_empty_deadline
test_source_bucket_cleanup_does_not_retry_other_errors
test_source_bucket_cleanup_accepts_no_such_bucket
test_cleanup_upgrades_pass_and_unsupported
test_term_uses_cleanup_fallback
test_rc_provenance_with_spaced_path_and_digest
test_wire_oracles_reject_http_500
run_versioned_cleanup_contract_tests
test_tier_query_valid_contract
test_tier_query_rejects_invalid_payloads
test_tier_query_failure_diagnostics_are_safe_and_bounded
test_tier_query_rejects_unexpected_parser_output
test_tier_presence_assertion_contract
test_remote_probe_single_page_success
test_aws_timeout_wrapper_preserves_exit_and_arguments
test_remote_probe_command_diagnostics_are_safe_and_bounded
test_remote_probe_rejects_invalid_key_counts
test_remote_wait_uses_wall_clock_deadline
test_remote_wait_hard_timeout_stays_inside_total_budget
test_failure_result_keeps_primary_and_cleanup_evidence
test_log_capture_truncates_and_waits_between_invocations
test_log_capture_preserves_suite_and_logger_failures
test_log_capture_does_not_wait_for_inherited_writers
test_full_fifo_and_stopped_logger_has_bounded_term
test_full_fifo_stopped_logger_parallel_stress
test_log_capture_startup_signal_phases_are_safe
test_log_file_fallback_rejects_unsafe_paths
test_log_destination_swap_keeps_open_inode
test_secure_log_owner_link_and_mode_contract
printf 'PASS: rustfs-tier-test.sh cleanup retry, result, signal, provenance, tier query, probe, diagnostic, and log-isolation tests\n'
