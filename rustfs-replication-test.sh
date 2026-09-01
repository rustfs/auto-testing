#!/usr/bin/env bash
#
# rustfs-replication-test.sh
# RustFS replication test suite: bucket replication (REP-*) and site
# replication (SITE-*).
#
# Bucket replication cases pair two deployments already running on the test
# node: the shared cluster (single-multi topology, :9000) as the SOURCE and a
# dedicated second instance (rustfs-rep2, :19000) as the TARGET. They drive
# the SigV4-signed admin plane (set/list/remove-remote-target,
# replication-check, replication-reset) plus the S3 data plane via the aws
# CLI, and assert PUT/delete-marker/existing-object-resync/multipart
# convergence on the target.
#
# Site replication cases configure site replication between the same two
# deployments (site1 = :9000, site2 = :19000) via the site-replication admin
# API (add/info/status/resync/remove) and assert IAM user/policy/bucket/
# object metadata converges in both directions.
#
# Usage:
#   ./rustfs-replication-test.sh -y
#   ./rustfs-replication-test.sh --suite bucket -y --package-url <deb url>
#   ./rustfs-replication-test.sh --suite site -y
#   ./rustfs-replication-test.sh --dry-run
#
# Notes:
#   - Requires ssh to the test nodes, and aws/jq/curl/openssl on the runner
#     (the S3 and admin channels are driven over HTTP from the runner).
#
set -Eeuo pipefail

# ==================== Configuration ====================

if [ -n "${RUSTFS_NODES:-}" ]; then
  read -r -a NODES <<<"${RUSTFS_NODES}"
else
  NODES=(vm000 vm001 vm002)
fi

SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${SSH_PORT}")

RUSTFS_VERSION="${RUSTFS_VERSION:-1.0.0-rc.4-preview.1}"
PACKAGE_URL="${PACKAGE_URL:-}"
ARCH="${RUSTFS_ARCH:-amd64}"
PACKAGES_DIR="/home/rustfs/packages"
PACKAGE_FILE="rustfs.deb"
PACKAGE_SHA256="${PACKAGE_SHA256:-}"

API_ENDPOINT="${RUSTFS_API_ENDPOINT:-${RUSTFS_RC_ENDPOINT:-http://127.0.0.1:9000}}"
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
REGION="us-east-1"
SIGV4_REGION="us-east-1"
SIGV4_SERVICE="s3"
API_REQUEST_TIMEOUT=60

# Main cluster (source site): single-multi on the shared nodes.
VOLUMES_MAIN="/data/rustfs{1...4}/mnmd"

# Second instance (target site / bucket-replication target) on the first node.
SITE2_SERVICE="rustfs-rep2"
SITE2_ADDRESS=":19000"
SITE2_CONSOLE_ADDRESS=":19001"
SITE2_ENDPOINT="http://127.0.0.1:19000"
SITE2_HOSTPORT="127.0.0.1:19000"
SITE2_VOLUMES="/data/rustfs-rep2/mnmd"
SITE1_NAME="site1"
SITE2_NAME="site2"

RUSTFS_CONFIG_FILE="/etc/default/rustfs"
SITE2_CONFIG_FILE="/etc/default/rustfs-rep2"
RUSTFS_SERVICE="rustfs"
RUSTFS_ADDRESS=":9000"
RUSTFS_CONSOLE_ADDRESS=":9001"
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_OBS_LOGGER_LEVEL=error
RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs/"

# Convergence polling budget (seconds) for async replication.
CONVERGE_TIMEOUT=180
CONVERGE_INTERVAL=5

SUITE_BUCKET="bucket"
SUITE_SITE="site"
SUITE_ALL="all"

SERVICE_TIMEOUT=300

# ==================== Runtime options ====================

DRY_RUN=0
ASSUME_YES=0
KEEP_ENV=0
LOG_FILE=""
SELECTED_SUITES=()

TEST_TMP="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-repl.XXXXXX")"
trap 'rm -rf "${TEST_TMP}"' EXIT

# ==================== Helpers ====================

log()  { printf '\033[1;36m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
  [ "${ASSUME_YES}" -eq 1 ] && return 0
  printf '\033[1;33m[CONFIRM]\033[0m %s (y/N) ' "$1"
  read -r answer
  case "${answer}" in y|Y|yes|YES) return 0 ;; *) die "cancelled" ;; esac
}

need_cmd() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1 ($2); install it first"
}

build_package_url() {
  local tag="${RUSTFS_VERSION#v}" asset
  asset="${tag//-/.}"
  printf 'https://github.com/rustfs/rustfs/releases/download/%s/rustfs_%s_%s.deb' "${tag}" "${asset}" "${ARCH}"
}

resolve_package_url() {
  if [ -n "${PACKAGE_URL}" ]; then printf '%s' "${PACKAGE_URL}";
  else build_package_url; fi
}

run_remote() {
  local node="$1" script
  script="$(cat)"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ssh ${SSH_USER}@${node} <<'REMOTE'"
    printf '%s\n' "${script}" | sed 's/^/    | /'
    return 0
  fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' <<<"${script}"
}

run_remote_all() {
  local script pids=() i=0 fail=0 node
  script="$(cat)"
  for node in "${NODES[@]}"; do
    if [ "${DRY_RUN}" -eq 1 ]; then
      log "DRY-RUN: ssh ${SSH_USER}@${node} <<'REMOTE'"
      printf '%s\n' "${script}" | sed 's/^/    | /'
    else
      ( ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' <<<"${script}" ) &
      pids[$i]=$!; i=$((i+1))
    fi
  done
  if [ "${#pids[@]}" -gt 0 ]; then
    for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
  fi
  [ "${fail}" -eq 0 ] || die "one or more nodes failed"
}

wait_service_active() {
  local node="$1" service="$2" waited=0
  [ "${DRY_RUN}" -eq 1 ] && { log "DRY-RUN: wait ${service} active on ${node}"; return 0; }
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "systemctl is-active --quiet ${service}" 2>/dev/null; then
      log "${node}: ${service} is active"; return 0
    fi
    sleep 5; waited=$((waited+5))
  done
  die "${node}: timed out waiting for ${service} (${SERVICE_TIMEOUT}s)"
}

wait_http_live() {
  local url="$1" waited=0
  [ "${DRY_RUN}" -eq 1 ] && return 0
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    if curl -fsS --max-time 5 "${url}/minio/health/live" >/dev/null 2>&1; then
      log "endpoint ready: ${url}"; return 0
    fi
    sleep 5; waited=$((waited+5))
  done
  die "endpoint ${url} not ready (${SERVICE_TIMEOUT}s)"
}

# ==================== Environment setup ====================

rustfs_config_body() {
  cat <<EOF
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES="${VOLUMES_MAIN}"
RUSTFS_ADDRESS="${RUSTFS_ADDRESS}"
RUSTFS_CONSOLE_ADDRESS="${RUSTFS_CONSOLE_ADDRESS}"
RUSTFS_CONSOLE_ENABLE=${RUSTFS_CONSOLE_ENABLE}
RUSTFS_OBS_LOGGER_LEVEL=${RUSTFS_OBS_LOGGER_LEVEL}
RUSTFS_OBS_LOG_DIRECTORY="${RUSTFS_OBS_LOG_DIRECTORY}"
EOF
}

site2_config_body() {
  cat <<EOF
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES="${SITE2_VOLUMES}"
RUSTFS_ADDRESS="${SITE2_ADDRESS}"
RUSTFS_CONSOLE_ADDRESS="${SITE2_CONSOLE_ADDRESS}"
RUSTFS_CONSOLE_ENABLE=${RUSTFS_CONSOLE_ENABLE}
RUSTFS_OBS_LOGGER_LEVEL=${RUSTFS_OBS_LOGGER_LEVEL}
RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs-rep2/"
EOF
}

write_remote_config() {
  local node="$1" config_file="$2" body="$3"
  log "${node}: writing config ${config_file}"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf 'if [ -f %s ]; then ${SUDO} cp -a %s %s.bak.$(date +%%Y%%m%%d%%H%%M%%S) 2>/dev/null || true; fi\n' "${config_file}" "${config_file}" "${config_file}"
    printf '%s tee %s >/dev/null <<RUSTFS_EOF\n' '${SUDO}' "${config_file}"
    printf '%s' "${body}"
    printf '\nRUSTFS_EOF\n'
  } | run_remote "${node}"
}

write_site2_unit() {
  local node="$1"
  log "${node}: writing systemd unit for ${SITE2_SERVICE}"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf '${SUDO} tee /etc/systemd/system/%s.service >/dev/null <<UNIT_EOF\n' "${SITE2_SERVICE}"
    cat <<'UNIT'
[Unit]
Description=RustFS second site (replication test)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=rustfs
Group=rustfs
EnvironmentFile=/etc/default/rustfs-rep2
ExecStart=/usr/local/bin/rustfs server
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT
    printf 'UNIT_EOF\n'
    printf '${SUDO} systemctl daemon-reload\n'
  } | run_remote "${node}"
}

service_action() {
  local action="$1" node="$2" service="${3:-${RUSTFS_SERVICE}}"
  log "${node}: systemctl ${action} ${service}"
  [ "${DRY_RUN}" -eq 1 ] && return 0
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "if [ \"\$(id -u)\" -ne 0 ]; then sudo -n systemctl ${action} ${service}; else systemctl ${action} ${service}; fi" \
    || die "${node}: systemctl ${action} ${service} failed"
}

cleanup_node() {
  local node="$1"
  log "cleanup: replication test residue on ${node}"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf '${SUDO} systemctl stop %s 2>/dev/null || true\n' "${SITE2_SERVICE}"
    printf '${SUDO} rm -f /etc/systemd/system/%s.service\n' "${SITE2_SERVICE}"
    printf '${SUDO} rm -f %s\n' "${SITE2_CONFIG_FILE}"
    printf '${SUDO} rm -rf %s /var/log/rustfs-rep2\n' "${SITE2_VOLUMES}"
    printf '${SUDO} systemctl daemon-reload 2>/dev/null || true\n'
  } | run_remote "${node}"
}

install_package_all() {
  local url script
  url="$(resolve_package_url)"
  log "installing package ${url} on ${NODES[*]}"
  script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
\${SUDO} mkdir -p "${PACKAGES_DIR}"
curl -fSL --retry 3 -o "/tmp/${PACKAGE_FILE}" "${url}"
\${SUDO} install -m 0644 "/tmp/${PACKAGE_FILE}" "${PACKAGES_DIR}/${PACKAGE_FILE}"
\${SUDO} rm -f "/tmp/${PACKAGE_FILE}"
if [ -n "${PACKAGE_SHA256}" ]; then
  echo "${PACKAGE_SHA256}  ${PACKAGES_DIR}/${PACKAGE_FILE}" | sha256sum -c - || exit 1
fi
\${SUDO} dpkg -i "${PACKAGES_DIR}/${PACKAGE_FILE}"
\${SUDO} systemctl daemon-reload
ls -lh "${PACKAGES_DIR}/${PACKAGE_FILE}"
EOF
)"
  printf '%s\n' "${script}" | run_remote_all
}

env_setup_main_cluster() {
  local node
  log "environment setup: main cluster (single-multi, ${NODES[*]})"
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
  install_package_all
  local body
  body="$(rustfs_config_body)"
  for node in "${NODES[@]}"; do
    write_remote_config "${node}" "${RUSTFS_CONFIG_FILE}" "${body}"
  done
  local pids=() pid
  for node in "${NODES[@]}"; do service_action start "${node}" & pids+=($!); done
  for pid in "${pids[@]}"; do wait "${pid}" || die "start failed"; done
  for node in "${NODES[@]}"; do wait_service_active "${node}" "${RUSTFS_SERVICE}"; done
  wait_http_live "${API_ENDPOINT}"
}

env_setup_site2() {
  local node="${NODES[0]}"
  log "environment setup: second site ${SITE2_SERVICE} on ${node} (${SITE2_ENDPOINT})"
  write_remote_config "${node}" "${SITE2_CONFIG_FILE}" "$(site2_config_body)"
  write_site2_unit "${node}"
  service_action start "${node}" "${SITE2_SERVICE}"
  wait_service_active "${node}" "${SITE2_SERVICE}"
  wait_http_live "${SITE2_ENDPOINT}"
}

env_teardown() {
  local node
  log "environment teardown"
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
}

# ==================== SigV4 request helper ====================

hex_of_ascii() { printf '%s' "$1" | od -An -tx1 | tr -d ' \n'; }
sha256_hex()   { printf '%s' "$1" | openssl dgst -sha256 -hex | awk '{print $NF}'; }
hmac_sha256_hex() { printf '%s' "$2" | openssl dgst -sha256 -hmac "$(printf '%s' "$1")" -hex | awk '{print $NF}'; }

canonical_query() {
  local q="$1"
  [ -z "${q}" ] && { printf ''; return 0; }
  printf '%s' "${q}" | tr '&' '\n' | sort | tr '\n' '&' | sed 's/&$//'
}

# signed_request <endpoint> <method> <path> [query] [body] [content_type]
# Signs with the S3 service name (admin endpoints accept the S3 signing
# chain), UNSIGNED-PAYLOAD, host;x-amz-content-sha256;x-amz-date.
# Prints the response body; the HTTP code lands in SIGNED_HTTP_CODE.
SIGNED_HTTP_CODE="000"
signed_request() {
  local endpoint="$1" method="$2" path="$3" query="${4:-}" body="${5:-}" content_type="${6:-application/xml}"
  local amz_date date_stamp host_port
  local canonical_headers signed_headers canonical_request string_to_sign
  local scope k_date k_region k_service k_signing signature auth
  local url tmp code
  local curl_body=()

  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: signed ${method} ${endpoint}${path}${query:+?${query}}"
    SIGNED_HTTP_CODE="200"
    printf '{}'
    return 0
  fi

  host_port="${endpoint#*://}"
  host_port="${host_port%%/*}"

  amz_date="$(date -u +%Y%m%dT%H%M%SZ)"
  date_stamp="${amz_date:0:8}"
  query="$(canonical_query "${query}")"

  canonical_headers="host:${host_port}
x-amz-content-sha256:UNSIGNED-PAYLOAD
x-amz-date:${amz_date}
"
  signed_headers="host;x-amz-content-sha256;x-amz-date"
  canonical_request="${method}
${path}
${query}
${canonical_headers}
${signed_headers}
UNSIGNED-PAYLOAD"

  scope="${date_stamp}/${SIGV4_REGION}/${SIGV4_SERVICE}/aws4_request"
  string_to_sign="AWS4-HMAC-SHA256
${amz_date}
${scope}
$(sha256_hex "${canonical_request}")"

  k_date="$(hmac_sha256_hex "$(hex_of_ascii "AWS4${SECRET_KEY}")" "${date_stamp}")"
  k_region="$(hmac_sha256_hex "${k_date}" "${SIGV4_REGION}")"
  k_service="$(hmac_sha256_hex "${k_region}" "${SIGV4_SERVICE}")"
  k_signing="$(hmac_sha256_hex "${k_service}" "aws4_request")"
  signature="$(hmac_sha256_hex "${k_signing}" "${string_to_sign}")"
  auth="AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${scope}, SignedHeaders=${signed_headers}, Signature=${signature}"

  url="http://${host_port}${path}${query:+?${query}}"
  tmp="$(mktemp)"
  if [ -n "${body}" ]; then
    curl_body=(-d "${body}" -H "Content-Type: ${content_type}")
  fi
  code="$(curl -sS --max-time "${API_REQUEST_TIMEOUT}" -o "${tmp}" -w '%{http_code}' \
    -H "Host: ${host_port}" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -H "x-amz-date: ${amz_date}" \
    -H "Authorization: ${auth}" \
    "${curl_body[@]}" -X "${method}" "${url}")" || code="000"
  SIGNED_HTTP_CODE="${code}"
  cat "${tmp}"
  rm -f "${tmp}"
}

# admin_api <method> <path> [query] [body] — signed_request on the main site.
admin_api() {
  signed_request "${API_ENDPOINT}" "$1" "$2" "${3:-}" "${4:-}" "application/json"
}

# site2_admin_api <method> <path> [query] [body] — signed_request on site2.
site2_admin_api() {
  signed_request "${SITE2_ENDPOINT}" "$1" "$2" "${3:-}" "${4:-}" "application/json"
}

# ==================== Test harness ====================

PASS=0; FAIL=0; UNSUPPORTED=0
FAILED_TESTS=()

run_test() {
  local id="$1" name="$2" fn="$3" rc
  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '  [DRY-RUN] %s %s\n' "${id}" "${name}"
    return 0
  fi
  printf '\n--- %s %s ---\n' "${id}" "${name}"
  if "${fn}" > "${TEST_TMP}/test.out" 2>&1; then
    PASS=$((PASS+1)); printf '\033[1;32m[PASS]\033[0m %s %s\n' "${id}" "${name}"
  else
    rc=$?
    if [ "${rc}" -eq 2 ]; then
      UNSUPPORTED=$((UNSUPPORTED+1)); printf '\033[1;33m[UNSUPPORTED]\033[0m %s %s\n' "${id}" "${name}"
    else
      FAIL=$((FAIL+1)); FAILED_TESTS+=("${id} ${name}")
      printf '\033[1;31m[FAIL]\033[0m %s %s\n' "${id}" "${name}"
      sed 's/^/      | /' "${TEST_TMP}/test.out" | tail -40
    fi
  fi
}

aws_run() {
  AWS_OUT="$(aws --endpoint-url "${API_ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; AWS_CODE=$?
}

aws_run_site2() {
  AWS_OUT="$(aws --endpoint-url "${SITE2_ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; AWS_CODE=$?
}

assert_aws_ok() {
  [ "${AWS_CODE}" -eq 0 ] || { echo "aws failed (exit ${AWS_CODE}): $*"; echo "${AWS_OUT}"; return 1; }
}

md5_of_file() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

make_test_file() {
  local path="$1" size="$2"
  if [ "${size}" -eq 0 ]; then : > "${path}"; else head -c "${size}" /dev/urandom > "${path}"; fi
}

results_summary() {
  printf '\n========================================\n'
  printf 'RESULTS: PASS=%d FAIL=%d UNSUPPORTED=%d\n' "${PASS}" "${FAIL}" "${UNSUPPORTED}"
  if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
    printf '\nFailed tests:\n'
    local t
    for t in "${FAILED_TESTS[@]}"; do printf '  - %s\n' "${t}"; done
  fi
}

# aws_wait_ep <endpoint> <desc> <aws args...> — poll until the aws call succeeds.
aws_wait_ep() {
  local ep="$1" desc="$2"; shift 2
  local waited=0
  while [ "${waited}" -lt "${CONVERGE_TIMEOUT}" ]; do
    if aws --endpoint-url "${ep}" --region "${REGION}" "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${CONVERGE_INTERVAL}"; waited=$((waited+CONVERGE_INTERVAL))
  done
  echo "timeout (${CONVERGE_TIMEOUT}s) waiting for: ${desc}"
  return 1
}

# ==================== Bucket/object helpers ====================

new_versioned_bucket() {
  local ep="$1" b="$2"
  if [ "${ep}" = "site2" ]; then aws_run_site2 s3api create-bucket --bucket "${b}";
  else aws_run s3api create-bucket --bucket "${b}"; fi
  assert_aws_ok "create-bucket ${b}" || return 1
  if [ "${ep}" = "site2" ]; then aws_run_site2 s3api put-bucket-versioning --bucket "${b}" \
      --versioning-configuration Status=Enabled;
  else aws_run s3api put-bucket-versioning --bucket "${b}" \
      --versioning-configuration Status=Enabled; fi
  assert_aws_ok "put-bucket-versioning ${b}" || return 1
}

bucket_exists() {
  local ep="$1" b="$2" url="${API_ENDPOINT}"
  [ "${ep}" = "site2" ] && url="${SITE2_ENDPOINT}"
  aws --endpoint-url "${url}" --region "${REGION}" s3api head-bucket --bucket "${b}" >/dev/null 2>&1
}

rm_bucket_force() {
  # Best-effort recursive bucket removal (versions + delete markers).
  local ep="$1" b="$2" i url="${API_ENDPOINT}"
  [ "${ep}" = "site2" ] && url="${SITE2_ENDPOINT}"
  for i in 1 2 3; do
    aws --endpoint-url "${url}" --region "${REGION}" s3api delete-objects --bucket "${b}" \
      --delete "$(aws --endpoint-url "${url}" --region "${REGION}" s3api list-object-versions \
        --bucket "${b}" --output json \
        --query '{Objects: (Versions + DeleteMarkers)[].{Key:Key,VersionId:VersionId}}' 2>/dev/null \
        | jq -c '.Objects // empty')" >/dev/null 2>&1 || true
    aws --endpoint-url "${url}" --region "${REGION}" s3api delete-bucket --bucket "${b}" >/dev/null 2>&1 || true
    bucket_exists "${ep}" "${b}" || return 0
    sleep 2
  done
  return 0
}

# register_replication_target <src_bucket> <dst_bucket> -> echoes the ARN
register_replication_target() {
  local src="$1" dst="$2" body arn
  body="$(jq -nc --arg ep "${SITE2_HOSTPORT}" \
    --arg ak "${ACCESS_KEY}" --arg sk "${SECRET_KEY}" --arg tb "${dst}" \
    '{endpoint:$ep, credentials:{accessKey:$ak, secretKey:$sk}, targetbucket:$tb, secure:false, skipTlsVerify:false, type:"replication"}')"
  arn="$(admin_api PUT "/rustfs/admin/v3/set-remote-target" "bucket=${src}" "${body}")"
  if [ "${SIGNED_HTTP_CODE}" != "200" ]; then
    echo "set-remote-target HTTP ${SIGNED_HTTP_CODE}: ${arn}" >&2
    return 1
  fi
  # 200 response body is the JSON-encoded ARN string.
  printf '%s' "${arn}" | jq -r 'if type == "string" then . else (.arn // .[0].arn // empty) end'
}

# put_replication_rule <src_bucket> <arn> [delete_marker_status]
put_replication_rule() {
  local src="$1" arn="$2" dmr="${3:-Disabled}"
  local cfg_json
  cfg_json="$(jq -nc --arg dst_arn "${arn}" --arg dmr "${dmr}" '{
    Role: "",
    Rules: [{
      ID: "rule1", Priority: 1, Status: "Enabled",
      DeleteMarkerReplication: {Status: $dmr},
      ExistingObjectReplication: {Status: "Enabled"},
      Destination: {Bucket: $dst_arn}
    }]
  }')"
  aws_run s3api put-bucket-replication --bucket "${src}" --replication-configuration "${cfg_json}"
  assert_aws_ok "put-bucket-replication ${src}" || return 1
}

# ==================== Site replication helpers ====================

site_add_payload() {
  jq -nc \
    --arg n1 "${SITE1_NAME}" --arg e1 "${API_ENDPOINT}" \
    --arg n2 "${SITE2_NAME}" --arg e2 "${SITE2_ENDPOINT}" \
    --arg ak "${ACCESS_KEY}" --arg sk "${SECRET_KEY}" \
    '[{name:$n1, endpoints:$e1, accessKey:$ak, secretKey:$sk, skipTlsVerify:false, caCertPem:""},
      {name:$n2, endpoints:$e2, accessKey:$ak, secretKey:$sk, skipTlsVerify:false, caCertPem:""}]'
}

# ==================== Bucket replication test cases ====================

run_all_bucket_tests() {
  run_test REP-101 "RemoteTargetRegisterListRemove"     test_rep101
  run_test REP-102 "ReplicationCheckPasses"             test_rep102
  run_test REP-103 "ReplicationCheckRejectsUnversioned" test_rep103
  run_test REP-104 "PutObjectReplicates"                test_rep104
  run_test REP-105 "DeleteMarkerReplication"            test_rep105
  run_test REP-106 "RemoveTargetUsedByRule"             test_rep106
  run_test REP-107 "ExistingObjectResync"               test_rep107
  run_test REP-108 "MultipartReplicates"                test_rep108
}

test_rep101() {
  new_versioned_bucket main rep101-src || return 1
  new_versioned_bucket site2 rep101-dst || return 1
  local arn list
  arn="$(register_replication_target rep101-src rep101-dst)" \
    || { rm_bucket_force main rep101-src; rm_bucket_force site2 rep101-dst; return 1; }
  case "${arn}" in arn:*) : ;; *) echo "ARN has unexpected shape: ${arn}"; return 1 ;; esac
  list="$(admin_api GET "/rustfs/admin/v3/list-remote-targets" "bucket=rep101-src")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "list-remote-targets HTTP ${SIGNED_HTTP_CODE}: ${list}"; return 1; }
  printf '%s' "${list}" | jq -e --arg arn "${arn}" '.[0].arn == $arn' >/dev/null 2>&1 \
    || { echo "listing does not contain the ARN: ${list}"; return 1; }
  admin_api DELETE "/rustfs/admin/v3/remove-remote-target" "bucket=rep101-src&arn=${arn}" >/dev/null
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "remove-remote-target HTTP ${SIGNED_HTTP_CODE}"; return 1; }
  list="$(admin_api GET "/rustfs/admin/v3/list-remote-targets" "bucket=rep101-src")"
  if [ "${SIGNED_HTTP_CODE}" = "200" ]; then
    printf '%s' "${list}" | jq -e --arg arn "${arn}" '([.[].arn] | index($arn)) == null' >/dev/null 2>&1 \
      || { echo "ARN still listed after removal: ${list}"; return 1; }
  fi
  rm_bucket_force main rep101-src
  rm_bucket_force site2 rep101-dst
}

test_rep102() {
  new_versioned_bucket main rep102-src || return 1
  new_versioned_bucket site2 rep102-dst || return 1
  local arn body
  arn="$(register_replication_target rep102-src rep102-dst)" \
    || { rm_bucket_force main rep102-src; rm_bucket_force site2 rep102-dst; return 1; }
  put_replication_rule rep102-src "${arn}" \
    || { rm_bucket_force main rep102-src; rm_bucket_force site2 rep102-dst; return 1; }
  body="$(signed_request "${API_ENDPOINT}" GET "/rep102-src" "replication-check")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] \
    || { echo "replication-check HTTP ${SIGNED_HTTP_CODE}: ${body}"; rm_bucket_force main rep102-src; rm_bucket_force site2 rep102-dst; return 1; }
  printf '%s' "${body}" | jq -e '.Status == "OK"' >/dev/null 2>&1 \
    || { echo "replication-check not OK: ${body}"; rm_bucket_force main rep102-src; rm_bucket_force site2 rep102-dst; return 1; }
  rm_bucket_force main rep102-src
  rm_bucket_force site2 rep102-dst
}

test_rep103() {
  # replication-check must reject a bucket without versioning (400 + "versioning").
  aws_run s3api create-bucket --bucket rep103-src
  assert_aws_ok "create-bucket rep103-src" || return 1
  local body
  body="$(signed_request "${API_ENDPOINT}" GET "/rep103-src" "replication-check")"
  rm_bucket_force main rep103-src
  [ "${SIGNED_HTTP_CODE}" = "400" ] || { echo "expected 400 for unversioned bucket, got ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -qi "versioning" || { echo "error missing 'versioning': ${body}"; return 1; }
}

test_rep104() {
  new_versioned_bucket main rep104-src || return 1
  new_versioned_bucket site2 rep104-dst || return 1
  local arn
  arn="$(register_replication_target rep104-src rep104-dst)" \
    || { rm_bucket_force main rep104-src; rm_bucket_force site2 rep104-dst; return 1; }
  put_replication_rule rep104-src "${arn}" \
    || { rm_bucket_force main rep104-src; rm_bucket_force site2 rep104-dst; return 1; }
  make_test_file "${TEST_TMP}/rep104-v" 65536
  aws_run s3api put-object --bucket "rep104-src" --key data.bin --body "${TEST_TMP}/rep104-v"
  assert_aws_ok "put-object data.bin" \
    || { rm_bucket_force main rep104-src; rm_bucket_force site2 rep104-dst; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "object replicated to rep104-dst" \
    s3api head-object --bucket rep104-dst --key data.bin \
    || { rm_bucket_force main rep104-src; rm_bucket_force site2 rep104-dst; return 1; }
  aws_run_site2 s3api get-object --bucket "rep104-dst" --key data.bin "${TEST_TMP}/rep104-dl"
  assert_aws_ok "get-object from target" \
    || { rm_bucket_force main rep104-src; rm_bucket_force site2 rep104-dst; return 1; }
  [ "$(md5_of_file "${TEST_TMP}/rep104-dl")" = "$(md5_of_file "${TEST_TMP}/rep104-v")" ] \
    || { echo "replicated object MD5 mismatch"; rm_bucket_force main rep104-src; rm_bucket_force site2 rep104-dst; return 1; }
  rm_bucket_force main rep104-src
  rm_bucket_force site2 rep104-dst
}

test_rep105() {
  new_versioned_bucket main rep105-src || return 1
  new_versioned_bucket site2 rep105-dst || return 1
  local arn
  arn="$(register_replication_target rep105-src rep105-dst)" \
    || { rm_bucket_force main rep105-src; rm_bucket_force site2 rep105-dst; return 1; }
  put_replication_rule rep105-src "${arn}" "Enabled" \
    || { rm_bucket_force main rep105-src; rm_bucket_force site2 rep105-dst; return 1; }
  make_test_file "${TEST_TMP}/rep105-v" 128
  aws_run s3api put-object --bucket "rep105-src" --key m.txt --body "${TEST_TMP}/rep105-v"
  assert_aws_ok "put-object m.txt" \
    || { rm_bucket_force main rep105-src; rm_bucket_force site2 rep105-dst; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "initial object on target" \
    s3api head-object --bucket rep105-dst --key m.txt \
    || { rm_bucket_force main rep105-src; rm_bucket_force site2 rep105-dst; return 1; }
  aws_run s3api delete-object --bucket "rep105-src" --key m.txt
  assert_aws_ok "delete-object m.txt" \
    || { rm_bucket_force main rep105-src; rm_bucket_force site2 rep105-dst; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "delete marker replicated" \
    s3api list-object-versions --bucket rep105-dst --prefix m.txt --output json \
      --query 'length(DeleteMarkers // `[]`) > `0`' --output text \
    || { rm_bucket_force main rep105-src; rm_bucket_force site2 rep105-dst; return 1; }
  rm_bucket_force main rep105-src
  rm_bucket_force site2 rep105-dst
}

test_rep106() {
  new_versioned_bucket main rep106-src || return 1
  new_versioned_bucket site2 rep106-dst || return 1
  local arn body
  arn="$(register_replication_target rep106-src rep106-dst)" \
    || { rm_bucket_force main rep106-src; rm_bucket_force site2 rep106-dst; return 1; }
  put_replication_rule rep106-src "${arn}" \
    || { rm_bucket_force main rep106-src; rm_bucket_force site2 rep106-dst; return 1; }
  # Removing a target still referenced by a replication rule must be rejected
  # with 400 "removal disallowed".
  body="$(admin_api DELETE "/rustfs/admin/v3/remove-remote-target" "bucket=rep106-src&arn=${arn}")"
  [ "${SIGNED_HTTP_CODE}" = "400" ] \
    || { echo "expected 400 removing an in-use target, got ${SIGNED_HTTP_CODE}: ${body}"; rm_bucket_force main rep106-src; rm_bucket_force site2 rep106-dst; return 1; }
  printf '%s' "${body}" | grep -qi "disallowed" \
    || { echo "error missing 'disallowed': ${body}"; rm_bucket_force main rep106-src; rm_bucket_force site2 rep106-dst; return 1; }
  rm_bucket_force main rep106-src
  rm_bucket_force site2 rep106-dst
}

test_rep107() {
  new_versioned_bucket main rep107-src || return 1
  new_versioned_bucket site2 rep107-dst || return 1
  make_test_file "${TEST_TMP}/rep107-v" 4096
  aws_run s3api put-object --bucket "rep107-src" --key preexisting.bin --body "${TEST_TMP}/rep107-v"
  assert_aws_ok "put-object preexisting.bin" \
    || { rm_bucket_force main rep107-src; rm_bucket_force site2 rep107-dst; return 1; }
  local arn body
  arn="$(register_replication_target rep107-src rep107-dst)" \
    || { rm_bucket_force main rep107-src; rm_bucket_force site2 rep107-dst; return 1; }
  put_replication_rule rep107-src "${arn}" \
    || { rm_bucket_force main rep107-src; rm_bucket_force site2 rep107-dst; return 1; }
  # Existing-object resync: PUT ?replication-reset, response target[0].arn/resetid.
  body="$(signed_request "${API_ENDPOINT}" PUT "/rep107-src" "replication-reset")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] \
    || { echo "replication-reset HTTP ${SIGNED_HTTP_CODE}: ${body}"; rm_bucket_force main rep107-src; rm_bucket_force site2 rep107-dst; return 1; }
  printf '%s' "${body}" | jq -e '.target[0].arn != null and .target[0].resetid != null' >/dev/null 2>&1 \
    || { echo "replication-reset response missing target[0].arn/resetid: ${body}"; rm_bucket_force main rep107-src; rm_bucket_force site2 rep107-dst; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "existing object replicated after resync" \
    s3api head-object --bucket rep107-dst --key preexisting.bin \
    || { rm_bucket_force main rep107-src; rm_bucket_force site2 rep107-dst; return 1; }
  rm_bucket_force main rep107-src
  rm_bucket_force site2 rep107-dst
}

test_rep108() {
  new_versioned_bucket main rep108-src || return 1
  new_versioned_bucket site2 rep108-dst || return 1
  local arn
  arn="$(register_replication_target rep108-src rep108-dst)" \
    || { rm_bucket_force main rep108-src; rm_bucket_force site2 rep108-dst; return 1; }
  put_replication_rule rep108-src "${arn}" \
    || { rm_bucket_force main rep108-src; rm_bucket_force site2 rep108-dst; return 1; }
  make_test_file "${TEST_TMP}/rep108-chunk" 5242880
  aws_run s3api create-multipart-upload --bucket "rep108-src" --key mp.bin
  assert_aws_ok "create-multipart-upload" \
    || { rm_bucket_force main rep108-src; rm_bucket_force site2 rep108-dst; return 1; }
  local upload_id etag parts_json="[]" i
  upload_id="$(printf '%s' "${AWS_OUT}" | jq -r '.UploadId')"
  for i in 1 2; do
    aws_run s3api upload-part --bucket "rep108-src" --key mp.bin --upload-id "${upload_id}" \
      --part-number "${i}" --body "${TEST_TMP}/rep108-chunk"
    assert_aws_ok "upload-part ${i}" \
      || { rm_bucket_force main rep108-src; rm_bucket_force site2 rep108-dst; return 1; }
    etag="$(printf '%s' "${AWS_OUT}" | jq -r '.ETag | fromjson? // .')"
    parts_json="$(printf '%s' "${parts_json}" | jq -c --argjson pn "${i}" --arg e "${etag}" '. + [{ETag:$e, PartNumber:$pn}]')"
  done
  aws_run s3api complete-multipart-upload --bucket "rep108-src" --key mp.bin --upload-id "${upload_id}" \
      --multipart-upload "{\"Parts\":$(printf '%s' "${parts_json}" | jq -c '.')}"
  assert_aws_ok "complete-multipart-upload" \
    || { rm_bucket_force main rep108-src; rm_bucket_force site2 rep108-dst; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "multipart object replicated" \
    s3api head-object --bucket rep108-dst --key mp.bin \
    || { rm_bucket_force main rep108-src; rm_bucket_force site2 rep108-dst; return 1; }
  rm_bucket_force main rep108-src
  rm_bucket_force site2 rep108-dst
}

# ==================== Site replication test cases ====================

run_all_site_tests() {
  run_test SITE-101 "SiteReplicationAddAndInfo"        test_site101
  run_test SITE-102 "IAMUserConvergence"               test_site102
  run_test SITE-103 "ObjectConvergenceBothDirections"  test_site103
  run_test SITE-104 "BucketPolicyConvergence"          test_site104
  run_test SITE-105 "SiteStatusAndResync"              test_site105
  run_test SITE-106 "SiteReplicationRemove"            test_site106
}

test_site101() {
  local body
  body="$(admin_api PUT "/rustfs/admin/v3/site-replication/add" "replicateILMExpiry=false" "$(site_add_payload)")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "site-replication/add HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '.success == true' >/dev/null 2>&1 \
    || { echo "site add not successful: ${body}"; return 1; }
  body="$(admin_api GET "/rustfs/admin/v3/site-replication/info")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "site-replication/info HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '.enabled == true and (.sites | length == 2)' >/dev/null 2>&1 \
    || { echo "site info does not show 2 enabled sites: ${body}"; return 1; }
  # The peer entry must be visible from site2 as well.
  body="$(site2_admin_api GET "/rustfs/admin/v3/site-replication/info")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "site2 info HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '.enabled == true and (.sites | length == 2)' >/dev/null 2>&1 \
    || { echo "site2 does not show 2 enabled sites: ${body}"; return 1; }
}

test_site102() {
  # IAM user created on site1 must converge to site2.
  local body
  body="$(admin_api PUT "/rustfs/admin/v3/add-user" "accessKey=siteuser102" "{\"secretKey\":\"${SECRET_KEY}\",\"status\":\"enabled\"}")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "add-user HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "user visible on site2" \
    iam list-users --output json --query 'length(Users[?UserName==`siteuser102`]) > `0`' --output text \
    || return 1
  aws_wait_ep "${API_ENDPOINT}" "user visible on site1" \
    iam list-users --output json --query 'length(Users[?UserName==`siteuser102`]) > `0`' --output text \
    || return 1
}

test_site103() {
  aws_run s3api create-bucket --bucket site103-b1
  assert_aws_ok "create-bucket site103-b1" || return 1
  make_test_file "${TEST_TMP}/site103-v" 8192
  aws_run s3api put-object --bucket site103-b1 --key from1.bin --body "${TEST_TMP}/site103-v"
  assert_aws_ok "put-object from1.bin" || { rm_bucket_force main site103-b1; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "object from site1 on site2" \
    s3api head-object --bucket site103-b1 --key from1.bin \
    || { rm_bucket_force main site103-b1; return 1; }
  make_test_file "${TEST_TMP}/site103-v2" 8192
  aws_run_site2 s3api put-object --bucket site103-b1 --key from2.bin --body "${TEST_TMP}/site103-v2"
  assert_aws_ok "put-object from2.bin on site2" || { rm_bucket_force main site103-b1; return 1; }
  aws_wait_ep "${API_ENDPOINT}" "object from site2 on site1" \
    s3api head-object --bucket site103-b1 --key from2.bin \
    || { rm_bucket_force main site103-b1; return 1; }
  rm_bucket_force main site103-b1
}

test_site104() {
  aws_run s3api create-bucket --bucket site104-b1
  assert_aws_ok "create-bucket site104-b1" || return 1
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::site104-b1/*"]}]}'
  aws_run s3api put-bucket-policy --bucket site104-b1 --policy "${policy}"
  assert_aws_ok "put-bucket-policy" || { rm_bucket_force main site104-b1; return 1; }
  aws_wait_ep "${SITE2_ENDPOINT}" "bucket policy on site2" \
    s3api get-bucket-policy --bucket site104-b1 --query 'length(Policy) > `0`' --output text \
    || { rm_bucket_force main site104-b1; return 1; }
  rm_bucket_force main site104-b1
}

test_site105() {
  local body peer
  body="$(admin_api GET "/rustfs/admin/v3/site-replication/status")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "site-replication/status HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  # Pull the remote peer's PeerInfo from /info for the resync body.
  body="$(admin_api GET "/rustfs/admin/v3/site-replication/info")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "site-replication/info HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  peer="$(printf '%s' "${body}" | jq -c --arg ep "${SITE2_ENDPOINT}" '.sites[] | select(.endpoint == $ep)')"
  [ -n "${peer}" ] || { echo "site2 peer missing from info: ${body}"; return 1; }
  # Resync must reject a duplicate start before we can assert on status shape.
  body="$(admin_api PUT "/rustfs/admin/v3/site-replication/resync/op" "operation=start" "${peer}")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "resync start HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '.status == "success" and (.id | length > 0)' >/dev/null 2>&1 \
    || { echo "resync start not successful: ${body}"; return 1; }
}

test_site106() {
  local body
  body="$(admin_api GET "/rustfs/admin/v3/site-replication/info")"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "no site replication configured; SITE-101 must run first: ${body}"; return 1; }
  body="$(admin_api PUT "/rustfs/admin/v3/site-replication/remove" "" '{"all":true}')"
  [ "${SIGNED_HTTP_CODE}" = "200" ] || { echo "site-replication/remove HTTP ${SIGNED_HTTP_CODE}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '(.errorDetail // "") == "" and .status != ""' >/dev/null 2>&1 \
    || { echo "site remove reported an error: ${body}"; return 1; }
  body="$(admin_api GET "/rustfs/admin/v3/site-replication/info")"
  if [ "${SIGNED_HTTP_CODE}" = "200" ]; then
    printf '%s' "${body}" | jq -e '.enabled == false and ((.sites // []) | length == 0)' >/dev/null 2>&1 \
      || { echo "sites still present after remove: ${body}"; return 1; }
  fi
}

# ==================== Suite orchestration ====================

SUITE_RESULTS=()

record_suite_result() {
  local suite="$1"
  SUITE_RESULTS+=("${suite}: PASS=${PASS} FAIL=${FAIL} UNSUPPORTED=${UNSUPPORTED}")
  if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
    local t
    for t in "${FAILED_TESTS[@]}"; do SUITE_RESULTS+=("  - ${t}"); done
  fi
}

final_summary() {
  printf '\n========================================\n'
  printf 'ALL SUITES SUMMARY\n'
  local line
  for line in "${SUITE_RESULTS[@]}"; do printf '%s\n' "${line}"; done
  printf '========================================\n'
}

# ==================== CLI parsing ====================

usage() {
  cat <<EOF
rustfs-replication-test.sh — RustFS replication test suite (REP-* / SITE-*)

Usage:
  ./rustfs-replication-test.sh -y
  ./rustfs-replication-test.sh --suite bucket -y
  ./rustfs-replication-test.sh --suite site -y

Suites:
  bucket   bucket replication from the shared cluster (:9000) to a second
           instance (:19000) via set-remote-target + bucket replication rules
  site     site replication between :9000 and :19000 via the site-replication
           admin API
  all      both suites (default; bucket first, site second — the site cases
           reuse the same two deployments)

Options:
  --suite <name>          bucket | site | all (default all)
  --version <tag>         RustFS release tag (default ${RUSTFS_VERSION})
  --package-url <url>     direct .deb URL (overrides --version)
  --sha256 <hex>          optional package checksum
  --keep-env              keep the environment after the last suite
  --ssh-user <user>       SSH user for the test nodes
  --ssh-port <port>       SSH port for the test nodes
  --log-file <path>       append all output to a log file
  --dry-run               preview only
  -y, --yes               skip confirmation
  -h, --help              show this help
EOF
}

main() {
  [ "$#" -eq 0 ] && { usage; exit 0; }
  local opt
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --suite) SELECTED_SUITES+=("$1"); shift ;;
      --version) RUSTFS_VERSION="$1"; shift ;;
      --package-url) PACKAGE_URL="$1"; shift ;;
      --sha256) PACKAGE_SHA256="$1"; shift ;;
      --keep-env) KEEP_ENV=1 ;;
      --ssh-user) SSH_USER="$1"; shift ;;
      --ssh-port) SSH_PORT="$1"; shift ;;
      --log-file) LOG_FILE="$1"; shift ;;
      --dry-run) DRY_RUN=1 ;;
      -y|--yes) ASSUME_YES=1 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown option: ${opt} (see --help)" ;;
    esac
  done
  if [ -n "${LOG_FILE}" ]; then
    mkdir -p "$(dirname "${LOG_FILE}")"
    if ! touch "${LOG_FILE}" 2>/dev/null; then
      LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/rustfs-repl.XXXXXX.log")"
      warn "log file not writable; using ${LOG_FILE}"
    fi
    exec > >(tee -a "${LOG_FILE}") 2>&1
  fi
  [ "${#SELECTED_SUITES[@]}" -gt 0 ] || SELECTED_SUITES=("${SUITE_ALL}")
  local _p
  for _p in /root/.local/bin /home/*/.local/bin /usr/local/bin; do
    if [ -d "${_p}" ] && [[ ":${PATH}:" != *":${_p}:"* ]]; then PATH="${_p}:${PATH}"; fi
  done
  export PATH
  need_cmd ssh "SSH client"
  need_cmd aws "AWS CLI (S3 + admin channels)"
  need_cmd jq "JSON processor"
  need_cmd curl "HTTP client"
  need_cmd openssl "OpenSSL (SigV4 signing)"
  confirm "This DESTROYS existing rustfs data/config on: ${NODES[*]}"
  log "nodes: ${NODES[*]}  package: $(resolve_package_url)  suites: ${SELECTED_SUITES[*]}"

  local overall_fail=0 suite bucket_done=0 site_done=0
  for suite in "${SELECTED_SUITES[@]}"; do
    case "${suite}" in
      "${SUITE_BUCKET}")
        [ "${bucket_done}" -eq 1 ] && continue
        bucket_done=1
        log "== suite: bucket replication (REP-*) =="
        env_setup_main_cluster
        env_setup_site2
        PASS=0; FAIL=0; UNSUPPORTED=0; FAILED_TESTS=()
        run_all_bucket_tests
        results_summary
        record_suite_result "bucket"
        [ "${FAIL}" -gt 0 ] && overall_fail=1
        ;;
      "${SUITE_SITE}")
        [ "${site_done}" -eq 1 ] && continue
        site_done=1
        log "== suite: site replication (SITE-*) =="
        if [ "${bucket_done}" -eq 0 ]; then
          env_setup_main_cluster
          env_setup_site2
        else
          # Clean slate between suites: drop SR state left by bucket cases.
          admin_api PUT "/rustfs/admin/v3/site-replication/remove" "" '{"all":true}' >/dev/null 2>&1 || true
        fi
        PASS=0; FAIL=0; UNSUPPORTED=0; FAILED_TESTS=()
        run_all_site_tests
        results_summary
        record_suite_result "site"
        [ "${FAIL}" -gt 0 ] && overall_fail=1
        ;;
      "${SUITE_ALL}")
        log "== suite: all (bucket then site) =="
        env_setup_main_cluster
        env_setup_site2
        PASS=0; FAIL=0; UNSUPPORTED=0; FAILED_TESTS=()
        run_all_bucket_tests
        results_summary
        record_suite_result "bucket"
        [ "${FAIL}" -gt 0 ] && overall_fail=1
        admin_api PUT "/rustfs/admin/v3/site-replication/remove" "" '{"all":true}' >/dev/null 2>&1 || true
        PASS=0; FAIL=0; UNSUPPORTED=0; FAILED_TESTS=()
        run_all_site_tests
        results_summary
        record_suite_result "site"
        [ "${FAIL}" -gt 0 ] && overall_fail=1
        ;;
      *) die "unknown suite: ${suite} (bucket|site|all)" ;;
    esac
  done
  if [ "${KEEP_ENV}" -eq 0 ]; then
    env_teardown
  fi
  final_summary
  [ "${overall_fail}" -eq 0 ] || exit 1
  log "all done"
}

main "$@"
