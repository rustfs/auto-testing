#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_TMP="$(mktemp -d)"
trap 'rm -rf "${TEST_TMP}"' EXIT

export TMPDIR="${TEST_TMP}"
export RUSTFS_POOL_SCRIPT_SOURCE_ONLY=1
unset RUSTFS_API_ENDPOINT RUSTFS_RC_ENDPOINT RUSTFS_ADMIN_ENDPOINT
unset RUSTFS_CONTROL_ENDPOINT RUSTFS_WARP_ENDPOINT RUSTFS_PROXY_ENDPOINT RUSTFS_PROXY_STAGE_HOOK
# shellcheck source=rustfs_pool_expand.sh
source "${SCRIPT_DIR}/rustfs_pool_expand.sh"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local expected="$1" actual="$2" label="$3"
  [ "${actual}" = "${expected}" ] \
    || fail "${label}: expected '${expected}', got '${actual}'"
}

reset_endpoint_state() {
  LEGACY_ENDPOINT="http://legacy.test:9000"
  ADMIN_ENDPOINT=""
  WARP_ENDPOINT=""
  PROXY_ENDPOINT=""
  PROXY_STAGE_HOOK=""
  NODE_ENDPOINTS=(
    "http://node1.test:9000"
    "http://node2.test:9000"
    "http://node3.test:9000"
  )
  WARP_CONCURRENT=32
  STORAGEINFO_PROBE_RETRIES=3
  STORAGEINFO_PROBE_RETRY_DELAY=0
}

test_endpoint_resolution() {
  reset_endpoint_state
  resolve_endpoints
  assert_eq "http://legacy.test:9000" "${ADMIN_ENDPOINT}" "legacy admin fallback"
  assert_eq "http://legacy.test:9000" "${WARP_ENDPOINT}" "legacy warp fallback"

  reset_endpoint_state
  ADMIN_ENDPOINT="http://admin.test:9000/"
  WARP_ENDPOINT="http://warp.test:9000"
  PROXY_ENDPOINT="http://proxy.test:9000"
  resolve_endpoints
  assert_eq "http://admin.test:9000" "${ADMIN_ENDPOINT}" "admin normalization"
  assert_eq "http://warp.test:9000" "${WARP_ENDPOINT}" "warp override"
  assert_eq "http://proxy.test:9000" "${PROXY_ENDPOINT}" "proxy override"

  if (normalize_http_endpoint "https://admin.test:9000" >/dev/null 2>&1); then
    fail "HTTPS endpoint should be rejected until warp TLS is implemented"
  fi
  if (normalize_http_endpoint "http://admin.test:9000/path" >/dev/null 2>&1); then
    fail "endpoint paths should be rejected"
  fi
  if (normalize_http_endpoint $'http://admin.test:9000\nX-Injected: yes' >/dev/null 2>&1); then
    fail "endpoint control characters should be rejected"
  fi
}

MOCK_STATUS_FILE="${TEST_TMP}/curl-statuses"
MOCK_CALL_FILE="${TEST_TMP}/curl-calls"

curl() {
  local output_file="" write_format="" method="GET" url="" host_header="" arg status body
  while [ "$#" -gt 0 ]; do
    arg="$1"
    shift
    case "${arg}" in
      -o) output_file="$1"; shift ;;
      -w) write_format="$1"; shift ;;
      -X) method="$1"; shift ;;
      -H)
        case "$1" in Host:*) host_header="$1" ;; esac
        shift
        ;;
      http://*) url="${arg}" ;;
    esac
  done

  status="$(head -n 1 "${MOCK_STATUS_FILE}")"
  sed '1d' "${MOCK_STATUS_FILE}" > "${MOCK_STATUS_FILE}.next"
  mv "${MOCK_STATUS_FILE}.next" "${MOCK_STATUS_FILE}"
  printf '%s %s %s\n' "${method}" "${host_header}" "${url}" >> "${MOCK_CALL_FILE}"

  if [[ "${url}" == */rustfs/admin/v3/pools/list* ]]; then
    body="${MOCK_POOL_BODY:-[]}"
  elif [ "${status}" = "200" ]; then
    body='{"info":{"disks":[]}}'
  else
    body="mock response ${status}"
  fi
  if [ -n "${output_file}" ]; then
    printf '%s' "${body}" > "${output_file}"
  fi
  if [ -n "${write_format}" ]; then
    if [[ "${write_format}" == *time_connect* ]]; then
      printf '%s\t0.010\t0.020\t0.030' "${status}"
    else
      printf '%s' "${status}"
    fi
  fi
  [ "${status}" != "000" ]
}

set_mock_statuses() {
  printf '%s\n' "$@" > "${MOCK_STATUS_FILE}"
  : > "${MOCK_CALL_FILE}"
}

test_independent_routing_and_signing() {
  reset_endpoint_state
  ADMIN_ENDPOINT="http://admin.test:9000"
  WARP_ENDPOINT="http://warp.test:9000"
  PROXY_ENDPOINT="http://proxy.test:9000"
  resolve_endpoints

  set_mock_statuses 200 200
  admin_api GET /rustfs/admin/v3/storageinfo "" >/dev/null
  proxy_api GET /rustfs/admin/v3/storageinfo "" >/dev/null

  grep -Fq 'Host: admin.test:9000 http://admin.test:9000/rustfs/admin/v3/storageinfo' "${MOCK_CALL_FILE}" \
    || fail "admin request was not signed and sent with the direct authority"
  grep -Fq 'Host: proxy.test:9000 http://proxy.test:9000/rustfs/admin/v3/storageinfo' "${MOCK_CALL_FILE}" \
    || fail "proxy request was not signed and sent with the proxy authority"
}

test_storageinfo_retry_is_sticky() {
  reset_endpoint_state
  ADMIN_ENDPOINT="http://admin.test:9000"
  resolve_endpoints
  rm -f "${ADMIN_ANOMALY_FILE}"

  set_mock_statuses 502 502 200
  storageinfo_with_retry admin >/dev/null 2>"${TEST_TMP}/retry.log" \
    || fail "502,502,200 should recover for continued diagnostics"
  assert_eq "3" "$(wc -l < "${MOCK_CALL_FILE}" | tr -d ' ')" "retry count"
  grep -Fq 'direct admin storageinfo attempt 3/3 succeeded at http://admin.test:9000 (HTTP 200; http=200 connect=0.010 ttfb=0.020 total=0.030 curl_exit=0)' \
    "${TEST_TMP}/retry.log" || fail "successful storageinfo probes must record endpoint, status, and timings"
  grep -Fq '{"info":{"disks":[]}}' "${TEST_TMP}/retry.log" \
    || fail "successful storageinfo probes must retain the response body"
  [ -s "${ADMIN_ANOMALY_FILE}" ] || fail "recovered failures must remain sticky"
  if (assert_no_endpoint_anomalies >/dev/null 2>&1); then
    fail "a recovered storageinfo failure must not qualify as a clean pass"
  fi

  rm -f "${ADMIN_ANOMALY_FILE}"
  set_mock_statuses 403 200
  if storageinfo_with_retry admin >/dev/null 2>"${TEST_TMP}/forbidden.log"; then
    fail "403 storageinfo response should fail"
  fi
  assert_eq "1" "$(wc -l < "${MOCK_CALL_FILE}" | tr -d ' ')" "non-retryable status count"

  rm -f "${ADMIN_ANOMALY_FILE}"
  set_mock_statuses 502 502 502
  if storageinfo_with_retry admin >/dev/null 2>"${TEST_TMP}/persistent.log"; then
    fail "persistent 502 responses should fail"
  fi
  assert_eq "3" "$(wc -l < "${MOCK_CALL_FILE}" | tr -d ' ')" "persistent retry count"
}

test_warp_must_reach_threshold() {
  if (
    storage_usage_percent() { printf '10\n'; }
    monitor_storage 50 999999
  ) >/dev/null 2>&1; then
    fail "warp completion below the storage threshold must fail"
  fi
}

test_stage_readiness_and_exact_pool_count() {
  reset_endpoint_state
  ADMIN_ENDPOINT="http://admin.test:9000"
  resolve_endpoints

  set_mock_statuses 200 000 000
  verify_stage_readiness 1 >/dev/null \
    || fail "one-node stage should accept one ready and two inactive direct endpoints"

  set_mock_statuses 200 200
  if (verify_stage_readiness 1) >/dev/null 2>&1; then
    fail "a future-stage node reporting ready must fail the topology barrier"
  fi

  PROXY_ENDPOINT=""
  MOCK_POOL_BODY='[{"id":0,"status":"active"},{"id":1,"status":"active"}]'
  set_mock_statuses 200
  if (verify_pools 1) >/dev/null 2>&1; then
    fail "pool verification must reject more pools than the stage expects"
  fi
  set_mock_statuses 200
  verify_pools 2 >/dev/null || fail "pool verification should accept an exact active count"
  unset MOCK_POOL_BODY
}

test_nginx_stage_rendering() {
  local config
  config="$("${SCRIPT_DIR}/rustfs_pool_nginx_stage.sh" render 2 \
    http://rustfs-node1:9000 http://rustfs-node2:9000)"
  grep -Fq 'server rustfs-node1:9000;' <<<"${config}" \
    || fail "stage proxy is missing node 1"
  grep -Fq 'server rustfs-node2:9000;' <<<"${config}" \
    || fail "stage proxy is missing node 2"
  if grep -Fq 'rustfs-node3:9000' <<<"${config}"; then
    fail "stage proxy must not include a future-stage node"
  fi
  grep -Fq 'proxy_next_upstream off;' <<<"${config}" \
    || fail "stage proxy must not hide upstream failures"
  grep -Fq 'proxy_set_header Host $http_host;' <<<"${config}" \
    || fail "stage proxy must preserve the SigV4 Host authority"
  grep -Fq 'proxy_request_buffering off;' <<<"${config}" \
    || fail "stage proxy must stream large request bodies"
  grep -Fq 'upstream_status="$upstream_status"' <<<"${config}" \
    || fail "stage proxy access log must record upstream status"
  if "${SCRIPT_DIR}/rustfs_pool_nginx_stage.sh" render 1 https://rustfs-node1:9000 >/dev/null 2>&1; then
    fail "stage proxy must reject unsupported upstream schemes"
  fi
}

test_warp_error_classification() {
  local warp_log="${TEST_TMP}/warp-classification.log"
  printf '%s\n' 'SlowDown: foreground write concurrency limit reached' > "${warp_log}"
  report_warp_errors "${warp_log}" >/dev/null \
    || fail "application-level SlowDown must not be classified as a proxy failure"

  printf '%s\n' '<title>502 Bad Gateway</title>' > "${warp_log}"
  if report_warp_errors "${warp_log}" >/dev/null 2>&1; then
    fail "Nginx 502 must fail warp transport classification"
  fi

  printf '%s\n' '<title>504 Gateway Time-out</title>' > "${warp_log}"
  if report_warp_errors "${warp_log}" >/dev/null 2>&1; then
    fail "Nginx 504 must fail warp transport classification"
  fi

  printf '%s\n' 'Put "http://proxy/bucket/object": connection closed' > "${warp_log}"
  if report_warp_errors "${warp_log}" >/dev/null 2>&1; then
    fail "connection closure must fail warp transport classification"
  fi

  printf '%s\n' 'net/http: timeout awaiting response headers' > "${warp_log}"
  if report_warp_errors "${warp_log}" >/dev/null 2>&1; then
    fail "response-header timeouts must fail warp transport classification"
  fi
}

test_endpoint_resolution
test_independent_routing_and_signing
test_storageinfo_retry_is_sticky
test_warp_must_reach_threshold
test_stage_readiness_and_exact_pool_count
test_nginx_stage_rendering
test_warp_error_classification
printf 'PASS: rustfs_pool_expand.sh endpoint and probe tests\n'
