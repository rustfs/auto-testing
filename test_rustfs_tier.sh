#!/usr/bin/env bash
# shellcheck disable=SC2034 # Globals are consumed by functions sourced below.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-tier-unit.XXXXXX")"
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

test_pre_registration_before_failed_create
test_cleanup_order_idempotence_and_drain
test_cleanup_upgrades_pass_and_unsupported
test_term_uses_cleanup_fallback
test_rc_provenance_with_spaced_path_and_digest
test_wire_oracles_reject_http_500
test_remote_probe_single_page_success
test_aws_timeout_wrapper_preserves_exit_and_arguments
test_remote_probe_command_diagnostics_are_safe_and_bounded
test_remote_probe_rejects_invalid_key_counts
test_remote_wait_uses_wall_clock_deadline
test_remote_wait_hard_timeout_stays_inside_total_budget
test_failure_result_keeps_primary_and_cleanup_evidence
printf 'PASS: rustfs-tier-test.sh cleanup, result, signal, provenance, probe, and diagnostic tests\n'
