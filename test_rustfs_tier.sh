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

test_pre_registration_before_failed_create
test_cleanup_order_idempotence_and_drain
test_cleanup_upgrades_pass_and_unsupported
test_term_uses_cleanup_fallback
test_rc_provenance_with_spaced_path_and_digest
test_wire_oracles_reject_http_500
printf 'PASS: rustfs-tier-test.sh cleanup, result, signal, and provenance tests\n'
