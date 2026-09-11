#!/usr/bin/env bash
#
# rustfs-storage-test.sh
# RustFS storage engine test suite (STOR-*, backlog #2024): disk compression,
# inline data, object data cache, stored-representation preservation, storage
# checksum detection, and physical capacity accounting.
#
# The suite drives the S3 API (aws/rc) for user-visible behavior and the
# SigV4-signed admin API for cache/storage-info checks. Disk-level assertions
# (part file sizes, inline representation) run over SSH against the node data
# dirs (/data/rustfs*/mnmd/<bucket>/...).
#
# Server-side knobs written into /etc/default/rustfs on every node:
#   RUSTFS_COMPRESSION_ENABLED=true
#   RUSTFS_COMPRESSION_MULTIPART_ENABLED=true
#   RUSTFS_OBJECT_DATA_CACHE_ENABLE=true
#   RUSTFS_COMPRESS_ENABLE=true            (HTTP response compression)
#   RUSTFS_SSE_S3_MASTER_KEY=<32-byte b64> (SSE-S3)
#
# Usage:
#   ./rustfs-storage-test.sh --topology multi-multi -y
#   ./rustfs-storage-test.sh --all-topologies -y --package-url <nightly deb URL>
#   ./rustfs-storage-test.sh --topology multi-multi --dry-run
#
# Notes:
#   - Requires ssh to the test nodes, and rc/aws/jq/curl/openssl on the runner.
#   - rc exit code 7 means the command is gated/unsupported; those cases are
#     reported as UNSUPPORTED, not FAIL.
#
set -Eeuo pipefail

# ==================== Configuration ====================

if [ -n "${RUSTFS_NODES:-}" ]; then
  read -r -a NODES <<<"${RUSTFS_NODES}"
else
  NODES=(vm000 vm001 vm002 vm003)
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

API_ENDPOINT="${RUSTFS_API_ENDPOINT:-${RUSTFS_RC_ENDPOINT:-http://rustfs-node1:9000}}"
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
REGION="us-east-1"
SIGV4_REGION="us-east-1"
SIGV4_SERVICE="s3"
API_REQUEST_TIMEOUT=60
ADMIN_API_CODE_FILE="${TMPDIR:-/tmp}/rustfs-storage-api-code.$$"

SSE_S3_MASTER_KEY="${RUSTFS_SSE_S3_MASTER_KEY:-MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=}"

RUSTFS_CONFIG_FILE="/etc/default/rustfs"
RUSTFS_SERVICE="rustfs"
RUSTFS_PACKAGE_NAME="rustfs"
RUSTFS_USER="rustfs"
RUSTFS_ADDRESS=":9000"
RUSTFS_CONSOLE_ADDRESS=":9001"
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_OBS_LOGGER_LEVEL=error
RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs/"

TOPO_SINGLE_SINGLE="single-single"
TOPO_SINGLE_MULTI="single-multi"
TOPO_MULTI_MULTI="multi-multi"

ENDPOINT_SINGLE_SINGLE="http://rustfs-node1:9000"
ENDPOINT_SINGLE_MULTI="http://rustfs-node1:9000"
ENDPOINT_MULTI_MULTI="http://rustfs-node1:9000"

VOLUMES_SINGLE_SINGLE="/data/rustfs1/mnmd"
VOLUMES_SINGLE_MULTI="/data/rustfs{1...4}/mnmd"
VOLUMES_MULTI_MULTI="http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd"

SERVICE_TIMEOUT=300

# ==================== Runtime options ====================
DRY_RUN=0
ASSUME_YES=0
KEEP_ENV=0
LOG_FILE=""
SELECTED_TOPOLOGIES=()
CURRENT_TOPOLOGY=""
ENDPOINT=""
VOLUMES=""

TEST_TMP="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-storage.XXXXXX")"
trap 'rm -rf "${TEST_TMP}"; rm -f "${ADMIN_API_CODE_FILE}"' EXIT

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
  local script pids=() i=0 fail=0
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
  local node="$1" waited=0
  [ "${DRY_RUN}" -eq 1 ] && { log "DRY-RUN: wait ${RUSTFS_SERVICE} active on ${node}"; return 0; }
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "systemctl is-active --quiet ${RUSTFS_SERVICE}" 2>/dev/null; then
      log "${node}: ${RUSTFS_SERVICE} is active"; return 0
    fi
    sleep 5; waited=$((waited+5))
  done
  die "${node}: timed out waiting for ${RUSTFS_SERVICE} (${SERVICE_TIMEOUT}s)"
}

rustfs_config_body() {
  local volumes="$1"
  cat <<EOF
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES="${volumes}"
RUSTFS_SSE_S3_MASTER_KEY=${SSE_S3_MASTER_KEY}
RUSTFS_COMPRESSION_ENABLED=true
RUSTFS_COMPRESSION_MULTIPART_ENABLED=true
RUSTFS_OBJECT_DATA_CACHE_ENABLE=true
RUSTFS_COMPRESS_ENABLE=true
RUSTFS_ADDRESS="${RUSTFS_ADDRESS}"
RUSTFS_CONSOLE_ADDRESS="${RUSTFS_CONSOLE_ADDRESS}"
RUSTFS_CONSOLE_ENABLE=${RUSTFS_CONSOLE_ENABLE}
RUSTFS_OBS_LOGGER_LEVEL=${RUSTFS_OBS_LOGGER_LEVEL}
RUSTFS_OBS_LOG_DIRECTORY="${RUSTFS_OBS_LOG_DIRECTORY}"
EOF
}

write_rustfs_config() {
  local node="$1" volumes="$2" body
  body="$(rustfs_config_body "${volumes}")"
  log "${node}: writing config ${RUSTFS_CONFIG_FILE} (volumes=${volumes})"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf 'if [ -f %s ]; then ${SUDO} cp -a %s %s.bak.$(date +%%Y%%m%%d%%H%%M%%S); fi\n' "${RUSTFS_CONFIG_FILE}" "${RUSTFS_CONFIG_FILE}" "${RUSTFS_CONFIG_FILE}"
    printf '%s tee %s >/dev/null <<RUSTFS_EOF\n' '${SUDO}' "${RUSTFS_CONFIG_FILE}"
    printf '%s' "${body}"
    printf '\nRUSTFS_EOF\n'
    printf '${SUDO} systemctl daemon-reload\n'
  } | run_remote "${node}"
}

service_action() {
  local action="$1" node="$2"
  log "${node}: systemctl ${action} ${RUSTFS_SERVICE}"
  [ "${DRY_RUN}" -eq 1 ] && return 0
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "if [ \"\$(id -u)\" -ne 0 ]; then sudo -n systemctl ${action} ${RUSTFS_SERVICE}; else systemctl ${action} ${RUSTFS_SERVICE}; fi" \
    || die "${node}: systemctl ${action} failed"
}

start_and_wait_nodes() {
  local nodes=("$@") pids=() i=0 fail=0
  for node in "${nodes[@]}"; do service_action start "${node}" & pids[$i]=$!; i=$((i+1)); done
  if [ "${#pids[@]}" -gt 0 ]; then
    for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
  fi
  [ "${fail}" -eq 0 ] || die "systemctl start failed on one or more nodes"
  for node in "${nodes[@]}"; do wait_service_active "${node}"; done
}

cleanup_node() {
  local node="$1"
  log "cleanup: purge ${RUSTFS_PACKAGE_NAME} and remove data dirs on ${node}"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf '${SUDO} systemctl stop %s 2>/dev/null || true\n' "${RUSTFS_SERVICE}"
    printf 'if ${SUDO} dpkg -l %s 2>/dev/null | grep -q "^ii"; then ${SUDO} dpkg -P %s; fi\n' "${RUSTFS_PACKAGE_NAME}" "${RUSTFS_PACKAGE_NAME}"
    printf 'for i in 1 2 3 4; do ${SUDO} rm -rf /data/rustfs${i}/mnmd; done\n'
    printf '${SUDO} rm -rf /var/log/rustfs /var/lib/rustfs/kms /var/lib/rustfs/kms-backup\n'
  } | run_remote "${node}"
}

ensure_volume_dirs() {
  local node="$1" volumes="$2" dirs=() d
  while IFS= read -r d; do dirs+=("${d}"); done < <(volume_dirs "${volumes}" | sort -u)
  log "${node}: ensuring data dirs (${dirs[*]})"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf 'id -u %s >/dev/null 2>&1 || ${SUDO} useradd -r -s /bin/false -d /opt/%s %s\n' "${RUSTFS_USER}" "${RUSTFS_USER}" "${RUSTFS_USER}"
    for d in "${dirs[@]}"; do
      printf '${SUDO} mkdir -p %s\n' "${d}"
      printf '${SUDO} chown -R %s:%s %s\n' "${RUSTFS_USER}" "${RUSTFS_USER}" "${d}"
    done
  } | run_remote "${node}"
}

volume_dirs() {
  local volumes="$1" expr path prefix suffix i start end
  for expr in ${volumes}; do
    path="${expr#*://}"; path="${path#*/}"; path="/${path}"
    if [[ "${path}" =~ \{([0-9]+)\.\.\.([0-9]+)\} ]]; then
      start="${BASH_REMATCH[1]}"; end="${BASH_REMATCH[2]}"
      prefix="${path%%\{*}"; suffix="${path#*\}}"
      for ((i=start; i<=end; i++)); do printf '%s%s%s\n' "${prefix}" "${i}" "${suffix}"; done
    else
      printf '%s\n' "${path}"
    fi
  done
}

# ==================== Topology / environment ====================

resolve_topology() {
  case "$1" in
    "${TOPO_SINGLE_SINGLE}") ENDPOINT="${ENDPOINT_SINGLE_SINGLE}"; VOLUMES="${VOLUMES_SINGLE_SINGLE}"; return 0 ;;
    "${TOPO_SINGLE_MULTI}")  ENDPOINT="${ENDPOINT_SINGLE_MULTI}";  VOLUMES="${VOLUMES_SINGLE_MULTI}";  return 0 ;;
    "${TOPO_MULTI_MULTI}")   ENDPOINT="${ENDPOINT_MULTI_MULTI}";   VOLUMES="${VOLUMES_MULTI_MULTI}";   return 0 ;;
    *) die "unknown topology: $1 (single-single|single-multi|multi-multi)" ;;
  esac
}

topology_nodes() {
  case "$1" in
    "${TOPO_SINGLE_SINGLE}"|"${TOPO_SINGLE_MULTI}") printf '%s' "${NODES[0]}" ;;
    *) printf '%s' "${NODES[*]}" ;;
  esac
}

env_setup() {
  local topo="$1" nodes_str nodes=() url script
  resolve_topology "${topo}"
  read -r -a nodes <<<"$(topology_nodes "${topo}")"
  url="$(resolve_package_url)"
  log "environment setup: topology=${topo} nodes=${nodes[*]} endpoint=${ENDPOINT}"

  for node in "${NODES[@]}"; do cleanup_node "${node}"; done

  script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
\${SUDO} mkdir -p "${PACKAGES_DIR}"
pkg_tmp="\$(mktemp /tmp/rustfs-pkg.XXXXXX.deb)"
curl -fSL --retry 3 -o "\${pkg_tmp}" "${url}"
\${SUDO} install -m 0644 "\${pkg_tmp}" "${PACKAGES_DIR}/${PACKAGE_FILE}"
rm -f "\${pkg_tmp}"
if [ -n "${PACKAGE_SHA256}" ]; then
  echo "${PACKAGE_SHA256}  ${PACKAGES_DIR}/${PACKAGE_FILE}" | sha256sum -c - || exit 1
fi
ls -lh "${PACKAGES_DIR}/${PACKAGE_FILE}"
EOF
)"
  printf '%s\n' "${script}" | run_remote_all

  script="$(cat <<'EOF'
set -euo pipefail
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"
${SUDO} dpkg -i /home/rustfs/packages/rustfs.deb
${SUDO} systemctl daemon-reload
EOF
)"
  for node in "${nodes[@]}"; do printf '%s\n' "${script}" | run_remote "${node}"; done

  for node in "${nodes[@]}"; do
    write_rustfs_config "${node}" "${VOLUMES}"
    ensure_volume_dirs "${node}" "${VOLUMES}"
  done

  start_and_wait_nodes "${nodes[@]}"

  if [ "${DRY_RUN}" -eq 0 ]; then
    local waited=0
    while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
      if curl -fsS --max-time 5 "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        log "S3 endpoint ready: ${ENDPOINT}"; break
      fi
      sleep 5; waited=$((waited+5))
    done
    [ "${waited}" -lt "${SERVICE_TIMEOUT}" ] || die "S3 endpoint ${ENDPOINT} not ready"

    rc alias set local "${ENDPOINT}" "${ACCESS_KEY}" "${SECRET_KEY}" >/dev/null 2>&1 \
      || die "rc alias set failed for ${ENDPOINT}"
    export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
    export AWS_DEFAULT_REGION="${REGION}"
    export AWS_EC2_METADATA_DISABLED=true
    log "rc alias 'local' -> ${ENDPOINT}; aws env configured (region ${REGION})"
  fi
}

env_teardown() {
  local topo="$1" nodes_str nodes=()
  read -r -a nodes <<<"$(topology_nodes "${topo}")"
  log "environment teardown: ${topo}"
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
}

# ==================== SigV4 admin API ====================

sha256_hex() {
  printf '%s' "$1" | openssl dgst -sha256 -hex 2>/dev/null | awk '{print $NF}'
}

hmac_sha256_hex() {
  # $1: key in hex, $2: ascii data
  printf '%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" -hex 2>/dev/null | awk '{print $NF}'
}

hex_of_ascii() {
  # $1: ascii string -> hex
  printf '%s' "$1" | od -An -vtx1 | tr -d ' \n'
}

canonical_query() {
  local q="$1"
  [ -z "${q}" ] && return 0
  printf '%s\n' "${q}" | tr '&' '\n' | while IFS= read -r pair; do
    printf '%s=%s\n' "${pair%%=*}" "${pair#*=}"
  done | sort | paste -sd '&' -
}

# admin_api <method> <path> [<query>] [<body>] [discard]
admin_api() {
  local method="$1" path="$2" query="${3:-}" body="${4:-}" discard="${5:-}"
  local amz_date date_stamp host_port
  local canonical_headers signed_headers canonical_request string_to_sign
  local scope k_date k_region k_service k_signing signature auth
  local url tmp code

  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: admin API ${method} ${API_ENDPOINT}${path}${query:+?${query}}"
    printf '200' > "${ADMIN_API_CODE_FILE}"
    return 0
  fi

  host_port="${API_ENDPOINT#*://}"
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
  local curl_body=()
  if [ -n "${body}" ]; then
    curl_body=(-d "${body}" -H "Content-Type: application/json")
  fi
  local out_file="${tmp}"
  [ "${discard}" = "discard" ] && out_file="/dev/null"
  code="$(curl -sS --max-time "${API_REQUEST_TIMEOUT}" -o "${out_file}" -w '%{http_code}' \
    -H "Host: ${host_port}" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -H "x-amz-date: ${amz_date}" \
    -H "Authorization: ${auth}" \
    "${curl_body[@]}" -X "${method}" "${url}")" || code="000"
  printf '%s' "${code}" > "${ADMIN_API_CODE_FILE}"
  if [ "${discard}" != "discard" ]; then
    cat "${tmp}"
  fi
  rm -f "${tmp}"
}

admin_api_code() {
  cat "${ADMIN_API_CODE_FILE}"
}

# ==================== Test harness ====================

PASS=0; FAIL=0; UNSUPPORTED=0
FAILED_TESTS=()

run_test() {
  local id="$1" name="$2" fn="$3" out rc
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

rc_run() {
  RC_OUT="$(rc "$@" 2>&1)"; RC_CODE=$?
}

assert_rc_ok() {
  if [ "${RC_CODE}" -eq 7 ]; then echo "rc gated (exit 7): $*"; return 2; fi
  [ "${RC_CODE}" -eq 0 ] || { echo "rc failed (exit ${RC_CODE}): $*"; echo "${RC_OUT}"; return 1; }
}

aws_run() {
  AWS_OUT="$(aws --endpoint-url "${ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; AWS_CODE=$?
}

assert_aws_ok() {
  [ "${AWS_CODE}" -eq 0 ] || { echo "aws failed (exit ${AWS_CODE}): $*"; echo "${AWS_OUT}"; return 1; }
}

assert_aws_fails_with() {
  local needle="$1"; shift
  [ "${AWS_CODE}" -ne 0 ] || { echo "aws unexpectedly succeeded: $*"; return 1; }
  printf '%s' "${AWS_OUT}" | grep -qiE "${needle}" \
    || { echo "aws error did not contain '${needle}': ${AWS_OUT}"; return 1; }
}

md5_of_file() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

new_bucket() {
  local b="$1"
  aws_run s3api create-bucket --bucket "${b}"
  assert_aws_ok || return 1
}

rm_bucket() {
  local b="$1"
  aws_run s3api delete-bucket --bucket "${b}" 2>/dev/null || true
}

make_test_file() {
  local path="$1" size="$2"
  if [ "${size}" -eq 0 ]; then : > "${path}"; else head -c "${size}" /dev/urandom > "${path}"; fi
}

# Compressible payload (repeated pattern) of the requested byte size.
make_compressible_file() {
  local path="$1" size="$2"
  yes "compressible rustfs storage test pattern. " 2>/dev/null | head -c "${size}" > "${path}"
}

results_summary() {
  printf '\n========================================\n'
  printf 'RESULTS: PASS=%d FAIL=%d UNSUPPORTED=%d\n' "${PASS}" "${FAIL}" "${UNSUPPORTED}"
  if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
    printf '\nFailed tests:\n'
    for t in "${FAILED_TESTS[@]}"; do printf '  - %s\n' "${t}"; done
  fi
}

TOPO_RESULTS=()

record_topology_result() {
  local topo="$1"
  TOPO_RESULTS+=("${topo}: PASS=${PASS} FAIL=${FAIL} UNSUPPORTED=${UNSUPPORTED}")
  if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
    local t
    for t in "${FAILED_TESTS[@]}"; do TOPO_RESULTS+=("  - ${t}"); done
  fi
}

final_summary() {
  printf '\n========================================\n'
  printf 'ALL TOPOLOGIES SUMMARY\n'
  local line
  for line in "${TOPO_RESULTS[@]}"; do printf '%s\n' "${line}"; done
  printf '========================================\n'
}

# ==================== Disk helpers (SSH to nodes) ====================

# Sum all stored file bytes for a bucket across the current topology nodes.
bucket_physical_bytes() {
  local bucket="$1" nodes_str nodes=() total=0 out
  read -r -a nodes <<<"$(topology_nodes "${CURRENT_TOPOLOGY}")"
  for node in "${nodes[@]}"; do
    if [ "${DRY_RUN}" -eq 1 ]; then
      printf '0\n'; return 0
    fi
    out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      "if [ \"\$(id -u)\" -ne 0 ]; then SUDO=\"sudo -n\"; else SUDO=\"\"; fi; \${SUDO} find /data/rustfs*/mnmd/${bucket} -type f -printf '%s\\n' 2>/dev/null | awk '{s+=\$1} END{print s+0}'")"
    total=$((total + ${out:-0}))
  done
  printf '%s' "${total}"
}

# Count part.* files for a bucket across the current topology nodes.
bucket_part_count() {
  local bucket="$1" nodes_str nodes=() total=0 out
  read -r -a nodes <<<"$(topology_nodes "${CURRENT_TOPOLOGY}")"
  for node in "${nodes[@]}"; do
    if [ "${DRY_RUN}" -eq 1 ]; then
      printf '0\n'; return 0
    fi
    out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      "if [ \"\$(id -u)\" -ne 0 ]; then SUDO=\"sudo -n\"; else SUDO=\"\"; fi; \${SUDO} find /data/rustfs*/mnmd/${bucket} -name 'part.*' 2>/dev/null | wc -l")"
    total=$((total + ${out:-0}))
  done
  printf '%s' "${total}"
}

# ==================== Test cases ====================

run_all_tests() {
  run_test STOR-101 "SingleObjectCompression"      test_stor101
  run_test STOR-102 "CompressionThresholdNegatives" test_stor102
  run_test STOR-104 "ContentEncodingSkip"           test_stor104
  run_test STOR-105 "MultipartCompression"          test_stor105
  run_test STOR-106 "CompressedRangeGet"            test_stor106
  run_test STOR-107 "CompressionEncryption"         test_stor107
  run_test STOR-108 "CompressionVersioning"         test_stor108
  run_test STOR-110 "HttpResponseCompression"       test_stor110
  run_test STOR-111 "InlineDefaultThreshold"        test_stor111
  run_test STOR-113 "InlineOverwriteDelete"         test_stor113
  run_test STOR-115 "InlineEncryption"              test_stor115
  run_test STOR-117 "CacheStats"                    test_stor117
  run_test STOR-118 "CacheFlush"                    test_stor118
  run_test STOR-119 "CacheInvalidation"             test_stor119
  run_test STOR-125 "CopyRepresentation"            test_stor125
  run_test STOR-129 "BitrotSelfTest"                test_stor129
  run_test STOR-130 "StorageInfo"                   test_stor130
  run_test STOR-131 "RcInfoConsistency"             test_stor131
  run_test STOR-132 "CompressedPhysicalVsLogical"   test_stor132
  run_test STOR-133 "InlineCapacity"                test_stor133
}

test_stor101() {
  new_bucket "stor101-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor101-v" 1048576
  aws_run s3api put-object --bucket "stor101-b1" --key c.txt --body "${TEST_TMP}/stor101-v"
  assert_aws_ok || return 1
  aws_run s3api head-object --bucket "stor101-b1" --key c.txt
  printf '%s' "${AWS_OUT}" | jq -e '.ContentLength == 1048576' >/dev/null \
    || { echo "HEAD length wrong: ${AWS_OUT}"; return 1; }
  aws_run s3api get-object --bucket "stor101-b1" --key c.txt "${TEST_TMP}/stor101-dl"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor101-dl")" = "$(md5_of_file "${TEST_TMP}/stor101-v")" ] \
    || { echo "compressed roundtrip MD5 mismatch"; return 1; }
  local phys
  phys="$(bucket_physical_bytes "stor101-b1")"
  [ "${phys}" -gt 0 ] || { echo "no on-disk data found"; return 1; }
  [ "${phys}" -lt 524288 ] || { echo "compressed physical ${phys} >= logical half"; return 1; }
  rm_bucket "stor101-b1"
}

test_stor102() {
  # below MIN_DISK_COMPRESSIBLE_SIZE -> no compression
  new_bucket "stor102-small" || return 1
  make_test_file "${TEST_TMP}/stor102-small" 1024
  aws_run s3api put-object --bucket "stor102-small" --key small.txt --body "${TEST_TMP}/stor102-small"
  assert_aws_ok || return 1
  local small_phys
  small_phys="$(bucket_physical_bytes "stor102-small")"
  [ "${small_phys}" -ge 1024 ] || { echo "small object shrank below logical size (compressed)"; return 1; }
  rm_bucket "stor102-small"

  # incompressible data above the threshold -> no meaningful compression
  new_bucket "stor102-rand" || return 1
  make_test_file "${TEST_TMP}/stor102-rand" 1048576
  aws_run s3api put-object --bucket "stor102-rand" --key rand.bin --body "${TEST_TMP}/stor102-rand"
  assert_aws_ok || return 1
  local rand_phys
  rand_phys="$(bucket_physical_bytes "stor102-rand")"
  [ "${rand_phys}" -ge 1048576 ] || { echo "incompressible object shrank (unexpected)"; return 1; }
  rm_bucket "stor102-rand"

  # compressible content with a non-whitelisted extension (zip) -> not compressed
  new_bucket "stor102-zip" || return 1
  make_compressible_file "${TEST_TMP}/stor102-zip" 1048576
  aws_run s3api put-object --bucket "stor102-zip" --key arch.zip --body "${TEST_TMP}/stor102-zip"
  assert_aws_ok || return 1
  local zip_phys
  zip_phys="$(bucket_physical_bytes "stor102-zip")"
  [ "${zip_phys}" -ge 1048576 ] || { echo "zip object was compressed (phys=${zip_phys})"; return 1; }
  rm_bucket "stor102-zip"
}

test_stor104() {
  new_bucket "stor104-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor104-v" 1048576
  aws_run s3api put-object --bucket "stor104-b1" --key c.gz --body "${TEST_TMP}/stor104-v" \
    --content-encoding gzip
  assert_aws_ok || return 1
  aws_run s3api head-object --bucket "stor104-b1" --key c.gz
  printf '%s' "${AWS_OUT}" | jq -e '.ContentEncoding == "gzip"' >/dev/null \
    || { echo "Content-Encoding not preserved: ${AWS_OUT}"; return 1; }
  local phys
  phys="$(bucket_physical_bytes "stor104-b1")"
  [ "${phys}" -ge 1048576 ] || { echo "user-encoded object was re-compressed (phys=${phys})"; return 1; }
  rm_bucket "stor104-b1"
}

test_stor105() {
  new_bucket "stor105-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor105-mp" 15728640
  local upload_id etag1 etag2 etag3
  aws_run s3api create-multipart-upload --bucket "stor105-b1" --key big.txt
  assert_aws_ok || return 1
  upload_id="$(printf '%s' "${AWS_OUT}" | jq -r '.UploadId')"
  head -c 5242880 "${TEST_TMP}/stor105-mp" > "${TEST_TMP}/stor105-p1"
  head -c 10485760 "${TEST_TMP}/stor105-mp" | tail -c 5242880 > "${TEST_TMP}/stor105-p2"
  tail -c 5242880 "${TEST_TMP}/stor105-mp" > "${TEST_TMP}/stor105-p3"
  aws_run s3api upload-part --bucket "stor105-b1" --key big.txt --upload-id "${upload_id}" --part-number 1 --body "${TEST_TMP}/stor105-p1"
  assert_aws_ok || return 1; etag1="$(printf '%s' "${AWS_OUT}" | jq -r '.ETag')"
  aws_run s3api upload-part --bucket "stor105-b1" --key big.txt --upload-id "${upload_id}" --part-number 2 --body "${TEST_TMP}/stor105-p2"
  assert_aws_ok || return 1; etag2="$(printf '%s' "${AWS_OUT}" | jq -r '.ETag')"
  aws_run s3api upload-part --bucket "stor105-b1" --key big.txt --upload-id "${upload_id}" --part-number 3 --body "${TEST_TMP}/stor105-p3"
  assert_aws_ok || return 1; etag3="$(printf '%s' "${AWS_OUT}" | jq -r '.ETag')"
  aws_run s3api complete-multipart-upload --bucket "stor105-b1" --key big.txt --upload-id "${upload_id}" \
    --multipart-upload "{\"Parts\":[{\"ETag\":${etag1},\"PartNumber\":1},{\"ETag\":${etag2},\"PartNumber\":2},{\"ETag\":${etag3},\"PartNumber\":3}]}"
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket "stor105-b1" --key big.txt "${TEST_TMP}/stor105-dl"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor105-dl")" = "$(md5_of_file "${TEST_TMP}/stor105-mp")" ] \
    || { echo "multipart compressed full GET mismatch"; return 1; }
  # range crossing the part boundary
  local start=$((5242880 - 131072))
  aws_run s3api get-object --bucket "stor105-b1" --key big.txt "${TEST_TMP}/stor105-range" --range "bytes=${start}-$((start + 262143))"
  assert_aws_ok || return 1
  dd if="${TEST_TMP}/stor105-mp" of="${TEST_TMP}/stor105-slice" bs=1 skip="${start}" count=262144 2>/dev/null
  cmp -s "${TEST_TMP}/stor105-range" "${TEST_TMP}/stor105-slice" || { echo "cross-part range mismatch"; return 1; }
  local phys
  phys="$(bucket_physical_bytes "stor105-b1")"
  [ "${phys}" -lt 7864320 ] || { echo "multipart compressed physical ${phys} not below half"; return 1; }
  rm_bucket "stor105-b1"
}

test_stor106() {
  new_bucket "stor106-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor106-v" 1048576
  aws_run s3api put-object --bucket "stor106-b1" --key c.txt --body "${TEST_TMP}/stor106-v"
  assert_aws_ok || return 1
  local spec start count
  for spec in "0-0" "100-199" "65535-65536" "1048575-1048575" "-100"; do
    aws_run s3api get-object --bucket "stor106-b1" --key c.txt "${TEST_TMP}/stor106-dl" --range "bytes=${spec}"
    assert_aws_ok || { echo "range ${spec} failed: ${AWS_OUT}"; return 1; }
    if [ "${spec}" = "-100" ]; then
      start=1048476; count=100
    else
      start="${spec%-*}"; count=$(( ${spec#*-} - start + 1 ))
    fi
    dd if="${TEST_TMP}/stor106-v" of="${TEST_TMP}/stor106-slice" bs=1 skip="${start}" count="${count}" 2>/dev/null
    cmp -s "${TEST_TMP}/stor106-dl" "${TEST_TMP}/stor106-slice" || { echo "range ${spec} mismatch"; return 1; }
  done
  aws_run s3api get-object --bucket "stor106-b1" --key c.txt "${TEST_TMP}/stor106-dl" --range "bytes=1048576-1048577"
  assert_aws_fails_with "416|InvalidRange" || return 1
  rm_bucket "stor106-b1"
}

test_stor107() {
  new_bucket "stor107-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor107-v" 1048576
  aws_run s3api put-object --bucket "stor107-b1" --key c.txt --body "${TEST_TMP}/stor107-v" \
    --server-side-encryption AES256
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket "stor107-b1" --key c.txt "${TEST_TMP}/stor107-dl"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor107-dl")" = "$(md5_of_file "${TEST_TMP}/stor107-v")" ] \
    || { echo "compressed+encrypted roundtrip MD5 mismatch"; return 1; }
  local phys
  phys="$(bucket_physical_bytes "stor107-b1")"
  [ "${phys}" -lt 524288 ] || { echo "compressed+encrypted physical ${phys} not below half"; return 1; }
  rm_bucket "stor107-b1"
}

test_stor108() {
  new_bucket "stor108-b1" || return 1
  aws_run s3api put-bucket-versioning --bucket "stor108-b1" --versioning-configuration Status=Enabled
  assert_aws_ok || return 1
  make_compressible_file "${TEST_TMP}/stor108-v1" 1048576
  make_compressible_file "${TEST_TMP}/stor108-v2" 1048576
  printf '\nsecond version' >> "${TEST_TMP}/stor108-v2"
  aws_run s3api put-object --bucket "stor108-b1" --key c.txt --body "${TEST_TMP}/stor108-v1"
  assert_aws_ok || return 1
  local vid1
  vid1="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId')"
  aws_run s3api put-object --bucket "stor108-b1" --key c.txt --body "${TEST_TMP}/stor108-v2"
  assert_aws_ok || return 1
  local vid2
  vid2="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId')"
  aws_run s3api get-object --bucket "stor108-b1" --key c.txt "${TEST_TMP}/stor108-d1" --version-id "${vid1}"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor108-d1")" = "$(md5_of_file "${TEST_TMP}/stor108-v1")" ] \
    || { echo "old compressed version mismatch"; return 1; }
  aws_run s3api get-object --bucket "stor108-b1" --key c.txt "${TEST_TMP}/stor108-d2" --version-id "${vid2}"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor108-d2")" = "$(md5_of_file "${TEST_TMP}/stor108-v2")" ] \
    || { echo "new compressed version mismatch"; return 1; }
  rm_bucket "stor108-b1"
}

test_stor110() {
  new_bucket "stor110-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor110-v" 2048
  aws_run s3api put-object --bucket "stor110-b1" --key c.txt --body "${TEST_TMP}/stor110-v" --content-type text/plain
  assert_aws_ok || return 1
  local headers body code url
  headers="$(mktemp)"
  # plain GET is unsigned and gets 403 on a private bucket; use a presigned URL
  url="$(aws --endpoint-url "${ENDPOINT}" --region "${REGION}" s3 presign "s3://stor110-b1/c.txt" --expires-in 60 2>/dev/null)"
  [ -n "${url}" ] || { echo "presign produced no URL"; return 1; }
  code="$(curl -s --max-time 30 -D "${headers}" -o "${TEST_TMP}/stor110-body" -H "Accept-Encoding: gzip" \
    "${url}" -w '%{http_code}')"
  [ "${code}" = "200" ] || { echo "GET returned ${code}"; return 1; }
  grep -qi '^Content-Encoding: gzip' "${headers}" || { echo "response not gzip-compressed: $(cat "${headers}")"; return 1; }
  gunzip -c "${TEST_TMP}/stor110-body" > "${TEST_TMP}/stor110-decompressed" 2>/dev/null || {
    echo "response body is not valid gzip"; return 1; }
  cmp -s "${TEST_TMP}/stor110-decompressed" "${TEST_TMP}/stor110-v" || { echo "decompressed body mismatch"; return 1; }
  # small response below RUSTFS_COMPRESS_MIN_SIZE is not compressed
  make_test_file "${TEST_TMP}/stor110-small" 64
  aws_run s3api put-object --bucket "stor110-b1" --key small.txt --body "${TEST_TMP}/stor110-small" --content-type text/plain
  assert_aws_ok || return 1
  url="$(aws --endpoint-url "${ENDPOINT}" --region "${REGION}" s3 presign "s3://stor110-b1/small.txt" --expires-in 60 2>/dev/null)"
  code="$(curl -s --max-time 30 -D "${headers}" -o /dev/null -H "Accept-Encoding: gzip" "${url}" -w '%{http_code}')"
  if grep -qi '^Content-Encoding:' "${headers}"; then
    echo "note: small response was compressed (min-size not applied)"
  fi
  rm_bucket "stor110-b1"
}

test_stor111() {
  new_bucket "stor111-b1" || return 1
  make_test_file "${TEST_TMP}/stor111-v" 1024
  aws_run s3api put-object --bucket "stor111-b1" --key small.txt --body "${TEST_TMP}/stor111-v"
  assert_aws_ok || return 1
  local parts
  parts="$(bucket_part_count "stor111-b1")"
  [ "${parts}" -eq 0 ] || { echo "small object stored as ${parts} part files (expected inline)"; return 1; }
  aws_run s3api get-object --bucket "stor111-b1" --key small.txt "${TEST_TMP}/stor111-dl"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor111-dl")" = "$(md5_of_file "${TEST_TMP}/stor111-v")" ] \
    || { echo "inline object MD5 mismatch"; return 1; }
  rm_bucket "stor111-b1"
}

test_stor113() {
  new_bucket "stor113-b1" || return 1
  make_test_file "${TEST_TMP}/stor113-v1" 1024
  make_test_file "${TEST_TMP}/stor113-v2" 2048
  aws_run s3api put-object --bucket "stor113-b1" --key f --body "${TEST_TMP}/stor113-v1"
  assert_aws_ok || return 1
  aws_run s3api put-object --bucket "stor113-b1" --key f --body "${TEST_TMP}/stor113-v2"
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket "stor113-b1" --key f "${TEST_TMP}/stor113-dl"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor113-dl")" = "$(md5_of_file "${TEST_TMP}/stor113-v2")" ] \
    || { echo "inline overwrite returned stale content"; return 1; }
  aws_run s3api delete-object --bucket "stor113-b1" --key f
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket "stor113-b1" --key f "${TEST_TMP}/stor113-gone"
  assert_aws_fails_with "404|NoSuchKey" || { echo "deleted inline object still readable"; return 1; }
  local parts
  parts="$(bucket_part_count "stor113-b1")"
  [ "${parts}" -eq 0 ] || { echo "inline part data not reclaimed after delete (${parts} parts)"; return 1; }
  rm_bucket "stor113-b1"
}

test_stor115() {
  new_bucket "stor115-b1" || return 1
  make_test_file "${TEST_TMP}/stor115-v" 1024
  aws_run s3api put-object --bucket "stor115-b1" --key small.txt --body "${TEST_TMP}/stor115-v" \
    --server-side-encryption AES256
  assert_aws_ok || return 1
  local parts
  parts="$(bucket_part_count "stor115-b1")"
  [ "${parts}" -eq 0 ] || { echo "small encrypted object stored as part files"; return 1; }
  aws_run s3api get-object --bucket "stor115-b1" --key small.txt "${TEST_TMP}/stor115-dl"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor115-dl")" = "$(md5_of_file "${TEST_TMP}/stor115-v")" ] \
    || { echo "inline encrypted object MD5 mismatch"; return 1; }
  rm_bucket "stor115-b1"
}

test_stor117() {
  local body code
  body="$(admin_api GET "/rustfs/admin/v3/object-data-cache/stats" "")"
  code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "cache stats returned HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '.mode != null and .disabled == false and (.entries | type == "number") and (.lookups | type == "number") and (.hits | type == "number")' >/dev/null \
    || { echo "cache stats fields wrong: ${body}"; return 1; }
}

test_stor118() {
  local body code
  body="$(admin_api POST "/rustfs/admin/v3/object-data-cache/flush" "")"
  code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "cache flush returned HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api GET "/rustfs/admin/v3/object-data-cache/stats" "")"
  code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "cache stats after flush returned HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '.entries == 0' >/dev/null || { echo "cache entries not reset: ${body}"; return 1; }
}

test_stor119() {
  new_bucket "stor119-b1" || return 1
  make_test_file "${TEST_TMP}/stor119-v1" 1048576
  make_test_file "${TEST_TMP}/stor119-v2" 1048576
  aws_run s3api put-object --bucket "stor119-b1" --key f --body "${TEST_TMP}/stor119-v1"
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket "stor119-b1" --key f "${TEST_TMP}/stor119-d1"
  assert_aws_ok || return 1
  # overwrite invalidates the cached body; the next read must be the new content
  aws_run s3api put-object --bucket "stor119-b1" --key f --body "${TEST_TMP}/stor119-v2"
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket "stor119-b1" --key f "${TEST_TMP}/stor119-d2"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor119-d2")" = "$(md5_of_file "${TEST_TMP}/stor119-v2")" ] \
    || { echo "stale cached body served after overwrite"; return 1; }
  rm_bucket "stor119-b1"
}

test_stor125() {
  new_bucket "stor125-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor125-v" 1048576
  aws_run s3api put-object --bucket "stor125-b1" --key src --body "${TEST_TMP}/stor125-v"
  assert_aws_ok || return 1
  aws_run s3api copy-object --bucket "stor125-b1" --key dst --copy-source "stor125-b1/src"
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket "stor125-b1" --key dst "${TEST_TMP}/stor125-dl"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/stor125-dl")" = "$(md5_of_file "${TEST_TMP}/stor125-v")" ] \
    || { echo "copied compressed object mismatch"; return 1; }
  # copy target keeps the compressed representation (physical < logical)
  aws_run s3api delete-object --bucket "stor125-b1" --key src
  local phys
  phys="$(bucket_physical_bytes "stor125-b1")"
  [ "${phys}" -lt 524288 ] || { echo "copy target lost compression (phys=${phys})"; return 1; }
  rm_bucket "stor125-b1"
}

test_stor129() {
  local body code
  body="$(admin_api GET "/rustfs/admin/v3/info" "")"
  code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "server info returned HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '(.bitrotSelftest // .bitrot_selftest) == "passed"' >/dev/null \
    || { echo "bitrot selftest not passed: ${body}"; return 1; }
}

test_stor130() {
  local body code
  body="$(admin_api GET "/rustfs/admin/v3/storageinfo" "")"
  code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "storageinfo returned HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '(.info.disks // .disks) | length > 0' >/dev/null || { echo "no disks in storageinfo: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '[(.info.disks // .disks)[] | select(.state != "ok")] | length == 0' >/dev/null \
    || { echo "some disks not ok: ${body}"; return 1; }
  printf '%s' "${body}" | jq -e '[(.info.disks // .disks)[] | ((.totalspace // .total_space) > 0 and (.usedspace // .used_space) >= 0 and (.availspace // .available_space) >= 0)] | all' >/dev/null \
    || { echo "disk capacity fields invalid: ${body}"; return 1; }
}

test_stor131() {
  local body code rc_out used_api used_rc tolerance
  body="$(admin_api GET "/rustfs/admin/v3/storageinfo" "")"
  code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "storageinfo returned HTTP ${code}"; return 1; }
  used_api="$(printf '%s' "${body}" | jq '[((.info.disks // .disks)[] | (.usedspace // .used_space))] | add')"
  rc_run admin info storage local --json
  assert_rc_ok "rc admin info storage local" || return 1
  used_rc="$(printf '%s' "${RC_OUT}" | jq -r '.data.summary.used_capacity_bytes // .data.summary.usedCapacityBytes // empty')"
  [ -n "${used_rc}" ] || { echo "rc admin info storage output missing usedCapacityBytes: ${RC_OUT}"; return 1; }
  used_rc="${used_rc%.*}"
  tolerance=$(( used_api / 10 + 1 ))
  local diff
  diff=$(( used_api - used_rc )); [ "${diff}" -lt 0 ] && diff=$(( -diff ))
  [ "${diff}" -le "${tolerance}" ] || { echo "rc used ${used_rc} differs from storageinfo ${used_api}"; return 1; }
}

test_stor132() {
  new_bucket "stor132-b1" || return 1
  make_compressible_file "${TEST_TMP}/stor132-v" 1048576
  aws_run s3api put-object --bucket "stor132-b1" --key c.txt --body "${TEST_TMP}/stor132-v"
  assert_aws_ok || return 1
  aws_run s3api list-objects --bucket "stor132-b1"
  local logical phys
  logical="$(printf '%s' "${AWS_OUT}" | jq '[.Contents[]?.Size] | add // 0')"
  [ "${logical}" = "1048576" ] || { echo "logical size wrong: ${AWS_OUT}"; return 1; }
  phys="$(bucket_physical_bytes "stor132-b1")"
  [ "${phys}" -lt "${logical}" ] || { echo "physical ${phys} >= logical ${logical}"; return 1; }
  rm_bucket "stor132-b1"
}

test_stor133() {
  new_bucket "stor133-b1" || return 1
  make_test_file "${TEST_TMP}/stor133-v" 1024
  aws_run s3api put-object --bucket "stor133-b1" --key small.txt --body "${TEST_TMP}/stor133-v"
  assert_aws_ok || return 1
  aws_run s3api list-objects --bucket "stor133-b1"
  local logical parts
  logical="$(printf '%s' "${AWS_OUT}" | jq '[.Contents[]?.Size] | add // 0')"
  [ "${logical}" = "1024" ] || { echo "inline object not counted in listing: ${AWS_OUT}"; return 1; }
  parts="$(bucket_part_count "stor133-b1")"
  [ "${parts}" -eq 0 ] || { echo "inline object has part files"; return 1; }
  rm_bucket "stor133-b1"
}

# ==================== CLI parsing ====================

usage() {
  cat <<EOF
rustfs-storage-test.sh — RustFS storage engine test suite (STOR-*)

Usage:
  ./rustfs-storage-test.sh --topology <topo> [-y] [options]
  ./rustfs-storage-test.sh --all-topologies [-y] [options]

Options:
  --all-topologies        run single-single, single-multi and multi-multi
  --topology <topo>       one of: single-single, single-multi, multi-multi
  --version <tag>         RustFS release tag (default ${RUSTFS_VERSION})
  --package-url <url>     direct .deb URL (overrides --version)
  --sha256 <hex>          optional package checksum
  --keep-env              keep the environment after the last topology
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
  local opt all=0
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --all-topologies) all=1 ;;
      --topology) SELECTED_TOPOLOGIES+=("$1"); shift ;;
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
      LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/rustfs-storage.XXXXXX.log")"
      warn "log file not writable; using ${LOG_FILE}"
    fi
    exec > >(tee -a "${LOG_FILE}") 2>&1
  fi
  if [ "${all}" -eq 1 ]; then
    SELECTED_TOPOLOGIES=("${TOPO_SINGLE_SINGLE}" "${TOPO_SINGLE_MULTI}" "${TOPO_MULTI_MULTI}")
  fi
  [ "${#SELECTED_TOPOLOGIES[@]}" -gt 0 ] || die "no topology selected (--all-topologies / --topology)"
  local _p
  for _p in /root/.local/bin /home/*/.local/bin /usr/local/bin; do
    if [ -d "${_p}" ] && [[ ":${PATH}:" != *":${_p}:"* ]]; then PATH="${_p}:${PATH}"; fi
  done
  export PATH
  need_cmd ssh "SSH client"
  need_cmd rc "RustFS client CLI"
  need_cmd aws "AWS CLI (s3api channel)"
  need_cmd jq "JSON processor"
  need_cmd curl "HTTP client"
  need_cmd openssl "OpenSSL (SigV4 signing)"
  log "nodes: ${NODES[*]}  package: $(resolve_package_url)"
  local overall_fail=0 topo
  for topo in "${SELECTED_TOPOLOGIES[@]}"; do
    log "== topology: ${topo} =="
    env_setup "${topo}"
    CURRENT_TOPOLOGY="${topo}"
    PASS=0; FAIL=0; UNSUPPORTED=0; FAILED_TESTS=()
    run_all_tests
    results_summary
    record_topology_result "${topo}"
    if [ "${KEEP_ENV}" -eq 0 ] || [ "${topo}" != "${SELECTED_TOPOLOGIES[${#SELECTED_TOPOLOGIES[@]}-1]}" ]; then
      env_teardown "${topo}"
    fi
    if [ "${FAIL}" -gt 0 ]; then
      overall_fail=1
    fi
  done
  final_summary
  [ "${overall_fail}" -eq 0 ] || exit 1
  log "all done"
}

main "$@"
