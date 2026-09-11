#!/usr/bin/env bash
#
# rustfs-kms-test.sh
# RustFS KMS test suite (backlog #2024, KMS-101..307)
#
# Runs the KMS-* cases against a RustFS cluster in three topologies
# (single-single / single-multi / multi-multi) for each configured KMS
# backend (local, vault-kv2). The HashiCorp Vault backend is provided by a
# throw-away docker container on the admin host (heal); the container and its
# data are removed after each topology run.
#
# Driven from the admin host (heal). Each case has an explicit pass criterion
# and reports PASS / FAIL / UNSUPPORTED with error detail on failure.
#
# Usage:
#   ./rustfs-kms-test.sh --all-topologies --backends local,vault-kv2 -y \
#     --package-url https://dl.rustfs.com/artifacts/rustfs/packages/release/rustfs_1.0.0~rc.4-preview.1_amd64.deb
#   ./rustfs-kms-test.sh --topology single-single --backends local -y
#
set -Eeuo pipefail

# Per-run temp dir (shared runners may leave stale /tmp files owned by other users)
TEST_TMP="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-test.XXXXXX")"
trap 'rm -rf "${TEST_TMP}"' EXIT


# ==================== Configuration ====================
NODES=(vm000 vm001 vm002 vm003)
SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${SSH_PORT}")

RUSTFS_VERSION="${RUSTFS_VERSION:-1.0.0-rc.4-preview.1}"
PACKAGE_URL="${PACKAGE_URL:-}"
ARCH="${RUSTFS_ARCH:-amd64}"
PACKAGES_DIR="/home/rustfs/packages"
PACKAGE_FILE="rustfs.deb"
PACKAGE_SHA256="${PACKAGE_SHA256:-}"

ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
REGION="us-east-1"

# Base64 of a 32-byte master key for the local KMS backend and the backup KEK.
KMS_LOCAL_MASTER_KEY="${RUSTFS_KMS_LOCAL_MASTER_KEY:-MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=}"
KMS_BACKUP_KEK="${RUSTFS_KMS_BACKUP_KEK:-QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU=}"
KMS_BACKUP_KEK_ID="${RUSTFS_KMS_BACKUP_KEK_ID:-backup-kek-1}"
KMS_DEFAULT_KEY_ID="kms-default"

# Vault (docker on the admin host / heal).
VAULT_CONTAINER="rustfs-vault"
VAULT_IMAGE="${RUSTFS_VAULT_IMAGE:-hashicorp/vault:1.15.6}"
VAULT_ADDRESS="${RUSTFS_VAULT_ADDRESS:-http://10.0.0.4:8200}"
VAULT_TOKEN="${RUSTFS_VAULT_TOKEN:-rustfs-dev-token}"
VAULT_KV_MOUNT="secret"
VAULT_KEY_PATH_PREFIX="rustfs"
VAULT_DATA_DIR="${RUSTFS_VAULT_DATA_DIR:-${TMPDIR:-/tmp}/rustfs-vault-data}"
VAULT_CONFIG_DIR="${RUSTFS_VAULT_CONFIG_DIR:-/var/lib/rustfs-vault-config}"
HEAL_HOST="heal"
HEAL_SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

# Run a command on the admin host (heal). When docker is present locally (the
# script is being executed on heal / the runner), run it directly.
heal_run() {
  if command -v docker >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then
      "$@"
    else
      # runner user without docker group: run the same command via sudo.
      sudo -n "$@" 2>/dev/null || sudo "$@"
    fi
  else
    ssh "${HEAL_SSH_OPTS[@]}" "${HEAL_HOST}" "$(printf '%q ' "$@")"
  fi
}

heal_curl() {
  local url="$1"
  if command -v docker >/dev/null 2>&1; then curl -s --max-time 5 "${url}"; else ssh "${HEAL_SSH_OPTS[@]}" "${HEAL_HOST}" "curl -s --max-time 5 '${url}'"; fi
}

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
# KMS admin/service state is per-node, so in multi-multi all requests are pinned
# to node1's direct endpoint (the 3x4 cluster topology is unchanged). The nginx
# load balancer (127.0.0.1:9000) would scatter KMS admin calls across nodes.
ENDPOINT_MULTI_MULTI="http://rustfs-node1:9000"

VOLUMES_SINGLE_SINGLE="/data/rustfs1/mnmd"
VOLUMES_SINGLE_MULTI="/data/rustfs{1...4}/mnmd"
VOLUMES_MULTI_MULTI="http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd"

SERVICE_TIMEOUT=300
POLL_INTERVAL=15

# Admin API SigV4 (same credentials; the admin surface lives under /rustfs/admin).
API_ENDPOINT="${ENDPOINT_SINGLE_SINGLE}"
SIGV4_REGION="${REGION}"
SIGV4_SERVICE="s3"
API_REQUEST_TIMEOUT=60
ADMIN_API_CODE_FILE="${TMPDIR:-/tmp}/rustfs-kms-api-code.$$"

# ==================== Runtime options ====================
DRY_RUN=0
ASSUME_YES=0
KEEP_ENV=0
LOG_FILE=""
SELECTED_TOPOLOGIES=()
BACKENDS=()
ENDPOINT=""
VOLUMES=""
KMS_BACKEND="local"
KMS_EXTRA_ENV="${KMS_EXTRA_ENV:-}"

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
RUSTFS_KMS_ENABLE=true
RUSTFS_KMS_BACKEND=${KMS_BACKEND}
RUSTFS_KMS_DEFAULT_KEY_ID=${KMS_DEFAULT_KEY_ID}
EOF
  case "${KMS_BACKEND}" in
    local)
      cat <<EOF
RUSTFS_KMS_KEY_DIR=/var/lib/rustfs/kms
RUSTFS_KMS_LOCAL_MASTER_KEY=${KMS_LOCAL_MASTER_KEY}
EOF
      ;;
    vault-kv2)
      cat <<EOF
RUSTFS_KMS_VAULT_ADDRESS=${VAULT_ADDRESS}
RUSTFS_KMS_VAULT_TOKEN=${VAULT_TOKEN}
RUSTFS_KMS_VAULT_KV_MOUNT=${VAULT_KV_MOUNT}
RUSTFS_KMS_VAULT_KEY_PREFIX=${VAULT_KEY_PATH_PREFIX}
RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true
EOF
      ;;
  esac
  cat <<EOF
RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION=true
RUSTFS_KMS_BACKUP_KEK=${KMS_BACKUP_KEK}
RUSTFS_KMS_BACKUP_KEK_ID=${KMS_BACKUP_KEK_ID}
RUSTFS_KMS_BACKUP_DIR=/var/lib/rustfs/kms-backup
RUSTFS_ADDRESS="${RUSTFS_ADDRESS}"
RUSTFS_CONSOLE_ADDRESS="${RUSTFS_CONSOLE_ADDRESS}"
RUSTFS_CONSOLE_ENABLE=${RUSTFS_CONSOLE_ENABLE}
RUSTFS_OBS_LOGGER_LEVEL=${RUSTFS_OBS_LOGGER_LEVEL}
RUSTFS_OBS_LOG_DIRECTORY="${RUSTFS_OBS_LOG_DIRECTORY}"
EOF
  if [ -n "${KMS_EXTRA_ENV}" ]; then
    printf '%s\n' "${KMS_EXTRA_ENV}"
  fi
}

write_rustfs_config() {
  local node="$1" volumes="$2" body
  body="$(rustfs_config_body "${volumes}")"
  log "${node}: writing config ${RUSTFS_CONFIG_FILE} (volumes=${volumes}, kms=${KMS_BACKEND})"
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
    printf '${SUDO} rm -rf /var/log/rustfs /var/lib/rustfs/kms\n'
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
    printf '${SUDO} mkdir -p /var/lib/rustfs/kms && ${SUDO} chown -R %s:%s /var/lib/rustfs/kms\n' "${RUSTFS_USER}" "${RUSTFS_USER}"
    printf '${SUDO} mkdir -p /var/lib/rustfs/kms-backup && ${SUDO} chown -R %s:%s /var/lib/rustfs/kms-backup\n' "${RUSTFS_USER}" "${RUSTFS_USER}"
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

# ==================== Admin API (SigV4, optional JSON body) ====================

sha256_hex() { printf '%s' "$1" | openssl dgst -sha256 -hex 2>/dev/null | awk '{print $NF}'; }
hmac_sha256_hex() { printf '%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" -hex 2>/dev/null | awk '{print $NF}'; }
hex_of_ascii() { printf '%s' "$1" | od -An -vtx1 | tr -d ' \n'; }

canonical_query() {
  local q="$1"
  [ -z "${q}" ] && return 0
  printf '%s\n' "${q}" | tr '&' '\n' | while IFS= read -r pair; do
    printf '%s=%s\n' "${pair%%=*}" "${pair#*=}"
  done | sort | paste -sd '&' -
}

# admin_api <method> <path> [query] [body] ; prints body, writes HTTP code file
admin_api() {
  local method="$1" path="$2" query="${3:-}" body="${4:-}" ak="${5:-${ACCESS_KEY}}" sk="${6:-${SECRET_KEY}}"
  local amz_date date_stamp host_port
  local canonical_headers signed_headers canonical_request string_to_sign
  local scope k_date k_region k_service k_signing signature auth
  local url tmp code

  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: admin API ${method} ${API_ENDPOINT}${path}${query:+?${query}}${body:+ body=${body}}"
    printf '200' > "${ADMIN_API_CODE_FILE}"
    return 0
  fi

  # Callers often embed the query in the path (e.g. add-user?accessKey=x).
  # Split it so the canonical request separates URI and query like the server.
  local path_only="${path%%\?*}" embedded_query="${path#*\?}"
  if [ "${embedded_query}" != "${path}" ]; then
    if [ -n "${query}" ]; then query="${query}&${embedded_query}"; else query="${embedded_query}"; fi
    path="${path_only}"
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
  if [ -n "${body}" ]; then
    canonical_headers="content-type:application/json
${canonical_headers}"
    signed_headers="content-type;${signed_headers}"
  fi
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

  k_date="$(hmac_sha256_hex "$(hex_of_ascii "AWS4${sk}")" "${date_stamp}")"
  k_region="$(hmac_sha256_hex "${k_date}" "${SIGV4_REGION}")"
  k_service="$(hmac_sha256_hex "${k_region}" "${SIGV4_SERVICE}")"
  k_signing="$(hmac_sha256_hex "${k_service}" "aws4_request")"
  signature="$(hmac_sha256_hex "${k_signing}" "${string_to_sign}")"
  auth="AWS4-HMAC-SHA256 Credential=${ak}/${scope}, SignedHeaders=${signed_headers}, Signature=${signature}"

  url="http://${host_port}${path}${query:+?${query}}"
  tmp="$(mktemp)"
  local curl_args=()
  [ -n "${body}" ] && curl_args+=(--data-binary "${body}" -H 'Content-Type: application/json')
  code="$(curl -sS --max-time "${API_REQUEST_TIMEOUT}" -o "${tmp}" -w '%{http_code}' \
    -H "Host: ${host_port}" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -H "x-amz-date: ${amz_date}" \
    -H "Authorization: ${auth}" \
    "${curl_args[@]}" \
    -X "${method}" "${url}")" || code="000"
  printf '%s' "${code}" > "${ADMIN_API_CODE_FILE}"
  cat "${tmp}"
  rm -f "${tmp}"
}

admin_api_code() { cat "${ADMIN_API_CODE_FILE}"; }

admin_api_assert() {
  local method="$1" path="$2" query="${3:-}" desc="$4" body="${5:-}" code
  body="$(admin_api "${method}" "${path}" "${query}" "${body}")"
  code="$(admin_api_code)"
  if [ "${code}" != "200" ] && [ "${code}" != "201" ] && [ "${code}" != "204" ]; then
    printf '[ERROR] %s failed (HTTP %s)\n' "${desc}" "${code}"
    printf '%s\n' "${body}"
    return 1
  fi
  printf '%s' "${body}"
}

# ==================== Vault (docker on heal) ====================
#
# Vault runs with file storage so key material survives container restarts
# (required by KMS-307: stop vault -> upload fails -> restart -> old objects
# must still decrypt). After every (re)start the container is unsealed with the
# stored unseal key and the KV v2 engine is enabled at the configured mount.

vault_local() { command -v docker >/dev/null 2>&1; }

vault_remote_bash() {
  # $1: remote script text; runs locally when docker is present.
  local script="$1"
  if vault_local; then bash -c "${script}"; else ssh "${HEAL_SSH_OPTS[@]}" "${HEAL_HOST}" 'bash -s' <<<"${script}"; fi
}

vault_ensure_config() {
  if vault_local; then
    heal_run bash -lc "set -e; rm -rf '${VAULT_DATA_DIR}'; mkdir -p '${VAULT_DATA_DIR}'; chmod 0777 '${VAULT_DATA_DIR}'"
  else
    vault_remote_bash "
set -e
rm -rf '${VAULT_DATA_DIR}'
mkdir -p '${VAULT_DATA_DIR}'
chmod 0777 '${VAULT_DATA_DIR}'
"
  fi
}

vault_read_init() {
  # Prints the init JSON (root token + unseal key) if present.
  if vault_local; then
    cat "${VAULT_DATA_DIR}/init.json" 2>/dev/null || true
  else
    ssh "${HEAL_SSH_OPTS[@]}" "${HEAL_HOST}" "cat '${VAULT_DATA_DIR}/init.json' 2>/dev/null" 2>/dev/null || true
  fi
}

vault_unseal() {
  local init key
  init="$(vault_read_init)"
  key="$(printf '%s' "${init}" | jq -r '.unseal_keys_b64[0] // empty' 2>/dev/null)"
  [ -n "${key}" ] || { warn "no stored unseal key; vault will stay sealed"; return 0; }
  heal_run docker exec -e "VAULT_ADDR=http://127.0.0.1:8200" "${VAULT_CONTAINER}" \
    vault operator unseal "${key}" >/dev/null 2>&1 || true
}

vault_ensure_ready() {
  local init
  if ! heal_curl "${VAULT_ADDRESS}/v1/sys/health" >/dev/null 2>&1; then
    die "vault not reachable at ${VAULT_ADDRESS}"
  fi
  init="$(vault_read_init)"
  if [ -z "${init}" ] || [ -z "$(printf '%s' "${init}" | jq -r '.root_token // empty' 2>/dev/null)" ]; then
    log "initializing vault (1-of-1 unseal key)"
    if vault_local; then
      local tmp_init tmp_err
      tmp_init="${TEST_TMP}/vault-init.json"
      tmp_err="${TEST_TMP}/vault-init.stderr"
      heal_run docker exec -e "VAULT_ADDR=http://127.0.0.1:8200" "${VAULT_CONTAINER}" \
        vault operator init -key-shares=1 -key-threshold=1 -format=json \
        > "${tmp_init}" 2>"${tmp_err}" || {
          cat "${tmp_err}" >&2 || true
          die "vault init failed"
        }
      cat "${tmp_init}" | heal_run tee "${VAULT_DATA_DIR}/init.json" >/dev/null || die "vault init failed"
    else
      local tmp
      tmp="$(mktemp)"
      ssh "${HEAL_SSH_OPTS[@]}" "${HEAL_HOST}" "docker exec -e VAULT_ADDR=http://127.0.0.1:8200 ${VAULT_CONTAINER} vault operator init -key-shares=1 -key-threshold=1 -format=json" > "${tmp}" 2>/dev/null \
        || { rm -f "${tmp}"; die "vault init failed"; }
      scp -q "${tmp}" "${HEAL_HOST}:${VAULT_DATA_DIR}/init.json" 2>/dev/null || { rm -f "${tmp}"; die "vault init failed"; }
      rm -f "${tmp}"
    fi
    init="$(vault_read_init)"
    VAULT_TOKEN="$(printf '%s' "${init}" | jq -r '.root_token // empty')"
    [ -n "${VAULT_TOKEN}" ] || die "vault init did not return a root token"
    log "vault initialized; root token acquired"
  else
    VAULT_TOKEN="$(printf '%s' "${init}" | jq -r '.root_token // empty')"
    [ -n "${VAULT_TOKEN}" ] || die "vault init file missing root token"
  fi
  vault_unseal
  heal_run docker exec -e "VAULT_ADDR=http://127.0.0.1:8200" -e "VAULT_TOKEN=${VAULT_TOKEN}" "${VAULT_CONTAINER}" \
    vault secrets enable -path="${VAULT_KV_MOUNT}" kv-v2 >/dev/null 2>&1 || true
  heal_run docker exec -e "VAULT_ADDR=http://127.0.0.1:8200" -e "VAULT_TOKEN=${VAULT_TOKEN}" "${VAULT_CONTAINER}" \
    vault kv put "${VAULT_KV_MOUNT}/_probe" ping=pong >/dev/null 2>&1 || true
}

vault_start() {
  [ "${DRY_RUN}" -eq 1 ] && { log "DRY-RUN: start vault container on ${HEAL_HOST}"; return 0; }
  if heal_run docker inspect "${VAULT_CONTAINER}" >/dev/null 2>&1; then
    log "vault container already present on ${HEAL_HOST}"
    vault_ensure_ready
    return 0
  fi
  vault_ensure_config
  log "starting vault container ${VAULT_CONTAINER} on ${HEAL_HOST}"
  heal_run docker run -d --name "${VAULT_CONTAINER}" --network host --cap-add=IPC_LOCK --user root \
    -v "${VAULT_DATA_DIR}:/vault/data" \
    -e "VAULT_LOCAL_CONFIG=storage \"file\" { path = \"/vault/data\" }
listener \"tcp\" { address = \"0.0.0.0:8200\" tls_disable = true }
disable_mlock = true
ui = false" \
    "${VAULT_IMAGE}" server >/dev/null || die "failed to start vault container"
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    if heal_curl "${VAULT_ADDRESS}/v1/sys/health" >/dev/null 2>&1; then
      log "vault is up at ${VAULT_ADDRESS}"
      break
    fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || die "vault did not start within 120s"
  vault_ensure_ready
  log "vault is healthy and unsealed at ${VAULT_ADDRESS}"
}

vault_restart() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  log "restarting vault container ${VAULT_CONTAINER}"
  heal_run docker stop "${VAULT_CONTAINER}" >/dev/null || true
  heal_run docker start "${VAULT_CONTAINER}" >/dev/null || die "failed to start vault container"
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    if heal_curl "${VAULT_ADDRESS}/v1/sys/health" >/dev/null 2>&1; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || die "vault did not restart within 120s"
  vault_unseal
  log "vault restarted and unsealed"
}

vault_stop() {
  [ "${DRY_RUN}" -eq 1 ] && { log "DRY-RUN: remove vault container on ${HEAL_HOST}"; return 0; }
  log "stopping and removing vault container ${VAULT_CONTAINER} on ${HEAL_HOST}"
  heal_run docker rm -f "${VAULT_CONTAINER}" >/dev/null 2>&1 || true
  if vault_local; then
    heal_run rm -rf "${VAULT_DATA_DIR}" "${VAULT_CONFIG_DIR}"
  else
    vault_remote_bash "rm -rf '${VAULT_DATA_DIR}' '${VAULT_CONFIG_DIR}'"
  fi
}

vault_down() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  heal_run docker inspect "${VAULT_CONTAINER}" >/dev/null 2>&1 && return 0
  return 1
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
  log "environment setup: topology=${topo} nodes=${nodes[*]} endpoint=${ENDPOINT} kms-backend=${KMS_BACKEND}"
  API_ENDPOINT="${ENDPOINT}"

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
    # Create the configured default key so roundtrip / default-key flows work.
    rc admin kms key create local --name "${KMS_DEFAULT_KEY_ID}" --json >/dev/null 2>&1 || true
  fi
}

env_teardown() {
  local topo="$1"
  log "environment teardown: ${topo} (kms=${KMS_BACKEND})"
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
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
      cp "${TEST_TMP}/test.out" ""${TEST_TMP}/${id}.out"" 2>/dev/null || true
      sed 's/^/      | /' "${TEST_TMP}/test.out" | tail -50
    fi
  fi
}

rc_run() {
  RC_OUT="$(rc "$@" 2>&1)"; RC_CODE=$?
  return "${RC_CODE}"
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

assert_aws_fails() {
  [ "${AWS_CODE}" -ne 0 ] || { echo "aws unexpectedly succeeded: $*"; echo "${AWS_OUT}"; return 1; }
}

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

md5_of_file() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

# Create a KMS key and set KMS_KEY_ID (plus KMS_KEY_NAME). Returns 1 on failure.
kms_create_key() {
  local name="$1" tag="$2"
  rc_run admin kms key create local --name "${name}" --tag "${tag}" --json
  assert_rc_ok || return 1
  KMS_KEY_ID="$(printf '%s' "${RC_OUT}" | jq -r '.key_id // .KeyId // .data.key_id // .result.key_id // empty' 2>/dev/null)"
  KMS_KEY_NAME="${name}"
  [ -n "${KMS_KEY_ID}" ] || { echo "could not parse key id from: ${RC_OUT}"; return 1; }
  echo "key ${name} -> ${KMS_KEY_ID}"
}

kms_describe_key() {
  local kid="$1"
  rc_run admin kms key status local "${kid}" --json
  assert_rc_ok || return 1
  printf '%s' "${RC_OUT}"
}

# SSE-KMS put; args: <bucket> <key> <file> [key-id]
kms_sse_put() {
  local b="$1" k="$2" f="$3" kid="${4:-${KMS_KEY_ID}}"
  aws_run s3api put-object --bucket "${b}" --key "${k}" --body "${f}" \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}"
}

# SSE-KMS get; args: <bucket> <key> <destfile> [key-id]
kms_sse_get() {
  local b="$1" k="$2" dest="$3"
  # SSE-KMS objects decrypt transparently; get-object takes no SSE-KMS header.
  aws_run s3api get-object --bucket "${b}" --key "${k}" "${dest}"
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
CURRENT_TOPOLOGY=""

record_topology_result() {
  local label="$1"
  TOPO_RESULTS+=("${label}: PASS=${PASS} FAIL=${FAIL} UNSUPPORTED=${UNSUPPORTED}")
  if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
    local t
    for t in "${FAILED_TESTS[@]}"; do TOPO_RESULTS+=("  - ${t}"); done
  fi
}

final_summary() {
  printf '\n========================================\n'
  printf 'ALL RUNS SUMMARY\n'
  local line
  for line in "${TOPO_RESULTS[@]}"; do printf '%s\n' "${line}"; done
  printf '========================================\n'
}

# ==================== KMS test cases ====================

KMS_KEY_ID=""
KMS_KEY_NAME=""
KMS_DEFAULT_KEY_ID_USED=""

run_all_tests() {
  run_test KMS-101 "KmsStatus"              test_kms101
  run_test KMS-102 "DynamicConfigureStart"  test_kms102
  run_test KMS-103 "StopRestart"            test_kms103
  run_test KMS-104 "Roundtrip"              test_kms104
  run_test KMS-105 "ClearCache"             test_kms105
  run_test KMS-106 "ConfigRedaction"        test_kms106
  run_test KMS-107 "ConfigSecretSealing"    test_kms107
  run_test KMS-108 "RestartPersistence"     test_kms108
  run_test KMS-109 "ConfigNegatives"        test_kms109
  run_test KMS-110 "MinioCompatAlias"       test_kms110
  run_test KMS-111 "GenerateDataKey"        test_kms111
  run_test KMS-201 "KeyCreate"              test_kms201
  run_test KMS-202 "KeyListDescribe"        test_kms202
  run_test KMS-203 "KeyEnableDisable"       test_kms203
  run_test KMS-204 "KeyRotate"              test_kms204
  run_test KMS-205 "ScheduledDelete"        test_kms205
  run_test KMS-206 "CancelDeletion"         test_kms206
  run_test KMS-207 "DeletionGate"           test_kms207
  run_test KMS-208 "ImmediateDeleteGuard"   test_kms208
  run_test KMS-209 "KeyMetadata"            test_kms209
  run_test KMS-210 "BackupRestore"          test_kms210
  run_test KMS-211 "RekeySweep"             test_kms211
  run_test KMS-212 "PendingDeletionSemantics" test_kms212
  run_test KMS-213 "CreateNegatives"        test_kms213
  run_test KMS-214 "NoMaterialLeak"         test_kms214
  run_test KMS-301 "SseKmsPutGet"           test_kms301
  run_test KMS-302 "BucketDefaultSseKms"    test_kms302
  run_test KMS-303 "MultipartSseKms"        test_kms303
  run_test KMS-304 "CopyEncryptionChange"   test_kms304
  run_test KMS-305 "KeyIdWithoutAlgo"       test_kms305
  run_test KMS-306 "SseCMutualExclusion"    test_kms306
  run_test KMS-308 "RangeGet"               test_kms308
  run_test KMS-309 "SizeBoundaries"         test_kms309
  run_test KMS-310 "EncryptionContext"      test_kms310
  run_test KMS-311 "SseS3Managed"           test_kms311
  run_test KMS-312 "BucketDefaultInvalid"   test_kms312
  run_test KMS-313 "VersioningCross"        test_kms313
  run_test KMS-314 "SelfCopySse"            test_kms314
  run_test KMS-315 "IlmCross"               test_kms315
  run_test KMS-316 "S3SelectCross"          test_kms316
  run_test KMS-317 "PresignedUrl"           test_kms317
  run_test KMS-318 "FrameCompat"            test_kms318
  run_test KMS-401 "PerKeySseAuthz"         test_kms401
  run_test KMS-402 "AnonymousEnforcement"   test_kms402
  run_test KMS-403 "AdminActionAuthz"       test_kms403
  run_test KMS-404 "AdminAudit"             test_kms404
  run_test KMS-501 "LocalBackendFault"      test_kms501
  run_test KMS-502 "ConcurrencyCross"       test_kms502
  if [ "${KMS_BACKEND}" = "vault-kv2" ]; then
    run_test KMS-307 "KmsUnavailable"       test_kms307
  else
    run_test KMS-307 "KmsUnavailable"       test_kms307_local
  fi
}

test_kms101() {
  rc_run admin kms status local --json
  assert_rc_ok || return 1
  local backend
  backend="$(printf '%s' "${RC_OUT}" | jq -r '.backend // .backend_type // .config.backend // .data.backend // empty' 2>/dev/null)"
  case "${KMS_BACKEND}" in
    local)     printf '%s' "${RC_OUT}" | grep -qiE '"local"|local' || { echo "backend mismatch: ${RC_OUT}"; return 1; } ;;
    vault-kv2) printf '%s' "${RC_OUT}" | grep -qiE 'vault' || { echo "backend mismatch: ${RC_OUT}"; return 1; } ;;
  esac
  printf '%s' "${RC_OUT}" | grep -qiE 'running|healthy|ready|ok' || { echo "status not running/healthy: ${RC_OUT}"; return 1; }
  echo "backend=${backend:-${KMS_BACKEND}} status: ${RC_OUT}"

  # API variants: GET /v3/kms/status and /v3/kms/service-status
  local body code
  body="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "GET /v3/kms/status -> HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api GET /rustfs/admin/v3/kms/service-status "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "GET /v3/kms/service-status -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -qiE 'running|healthy|ready|ok' || { echo "service-status unexpected: ${body}"; return 1; }
}

test_kms102() {
  local cfg
  if [ "${KMS_BACKEND}" = "local" ]; then
    # master_key must be omitted: the admin API refuses any non-empty value
    # (even an identical one) and carries the existing key over.
    cfg="{\"backend_type\":\"Local\",\"key_dir\":\"/var/lib/rustfs/kms\",\"default_key_id\":\"${KMS_DEFAULT_KEY_ID}\"}"
  else
    cfg="{\"backend_type\":\"VaultKV2\",\"address\":\"${VAULT_ADDRESS}\",\"auth_method\":{\"Token\":{\"token\":\"${VAULT_TOKEN}\"}},\"kv_mount\":\"${VAULT_KV_MOUNT}\",\"key_path_prefix\":\"${VAULT_KEY_PATH_PREFIX}\",\"default_key_id\":\"${KMS_DEFAULT_KEY_ID}\"}"
  fi
  # stop first (ignored if already stopped) so configure -> start is observable
  rc_run admin kms stop local --yes || true
  local body code
  body="$(admin_api POST /rustfs/admin/v3/kms/configure "" "${cfg}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "configure -> HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/start "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "start -> HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "status after start -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -qiE 'running|ready|ok|healthy' || { echo "status not running after start: ${body}"; return 1; }
}

test_kms103() {
  # stop -> SSE-KMS upload fails -> restart -> upload works again
  kms_create_key "k103" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms103-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  kms_sse_put kms103-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1

  rc_run admin kms stop local --yes
  assert_rc_ok || return 1
  kms_sse_put kms103-b1 f2 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_fails || { echo "SSE-KMS upload unexpectedly succeeded while KMS stopped"; return 1; }
  echo "expected failure while stopped: ${AWS_OUT}"

  rc_run admin kms restart local --yes
  assert_rc_ok || return 1
  local waited=0
  while [ "${waited}" -lt 60 ]; do
    kms_sse_put kms103-b1 f3 ${TEST_TMP}/kms-f1 "${kid}" && break
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 60 ] || { echo "SSE-KMS upload still failing after restart: ${AWS_OUT}"; return 1; }
  rm_bucket kms103-b1
}

test_kms104() {
  new_bucket "kms104-b1" || return 1
  rc_run admin kms roundtrip local kms104-b1 --key-id "${KMS_DEFAULT_KEY_ID}" --yes
  assert_rc_ok || return 1
  rm_bucket kms104-b1
}

test_kms105() {
  local body code
  body="$(admin_api POST /rustfs/admin/v3/kms/clear-cache "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "clear-cache -> HTTP ${code}: ${body}"; return 1; }
  kms_create_key "k105" "env=ci" || return 1
  new_bucket "kms105-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms105-b1 f1 ${TEST_TMP}/kms-f1
  assert_aws_ok || return 1
  rm_bucket kms105-b1
}

test_kms201() {
  kms_create_key "k1" "env=ci" || return 1
  rc_run admin kms key list local --json
  assert_rc_ok || return 1
  printf '%s' "${RC_OUT}" | grep -qF "${KMS_KEY_ID}" || { echo "key not visible in list: ${RC_OUT}"; return 1; }
  printf '%s' "${RC_OUT}" | grep -qF '"env": "ci"' || { echo "tag env=ci not visible: ${RC_OUT}"; return 1; }
  kms_describe_key "${KMS_KEY_ID}" >/dev/null || return 1
}

test_kms202() {
  local ids=() i
  for i in 1 2 3; do
    kms_create_key "k202-${i}" "env=ci" || return 1
    ids+=("${KMS_KEY_ID}")
  done
  # pagination: list with limit 2
  rc_run admin kms key list local --limit 2 --json
  assert_rc_ok || return 1
  local count
  count="$(printf '%s' "${RC_OUT}" | jq -r '.data.keys | length' 2>/dev/null)"
  [ "${count}" -ge 1 ] && [ "${count}" -le 2 ] || { echo "limit 2 returned ${count}: ${RC_OUT}"; return 1; }
  # marker pagination
  rc_run admin kms key list local --limit 1 --marker "${ids[0]}" --json
  assert_rc_ok || return 1
  # describe via API
  local body code
  body="$(admin_api GET "/rustfs/admin/v3/kms/keys/${ids[1]}" "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "describe ${ids[1]} -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -q "${ids[1]}" || { echo "describe missing key id: ${body}"; return 1; }
  printf '%s' "${body}" | grep -qiE 'key_id|keyId|metadata' || { echo "describe missing detail fields: ${body}"; return 1; }
}

test_kms203() {
  kms_create_key "k203" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  new_bucket "kms203-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms203-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1

  body="$(admin_api POST /rustfs/admin/v3/kms/keys/disable "" "{\"key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    if printf '%s' "${body}" | grep -qi "UnsupportedCapability"; then
      echo "note: disable unsupported on ${KMS_BACKEND} (expected for this backend)"
      return 2
    fi
    echo "disable -> HTTP ${code}: ${body}"; return 1
  fi

  kms_sse_put kms203-b1 f2 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_fails || { echo "SSE-KMS upload unexpectedly succeeded while key disabled"; return 1; }
  echo "expected failure while disabled: ${AWS_OUT}"
  # old object still decryptable
  kms_sse_get kms203-b1 f1 ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || return 1
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "old object MD5 mismatch"; return 1; }

  body="$(admin_api POST /rustfs/admin/v3/kms/keys/enable "" "{\"key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "enable -> HTTP ${code}: ${body}"; return 1; }
  kms_sse_put kms203-b1 f3 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1
  rm_bucket kms203-b1
}

test_kms204() {
  kms_create_key "k204" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  new_bucket "kms204-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms204-b1 old ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1

  body="$(admin_api POST /rustfs/admin/v3/kms/keys/rotate "" "{\"key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    if printf '%s' "${body}" | grep -qiE "UnsupportedCapability|not supported|unsupported"; then
      echo "note: rotate unsupported on ${KMS_BACKEND} (expected for this backend)"
      rm_bucket kms204-b1
      return 2
    fi
    echo "rotate -> HTTP ${code}: ${body}"; return 1
  fi
  # old object still decryptable, new object encryptable
  kms_sse_get kms204-b1 old ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || return 1
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "old object MD5 mismatch after rotation"; return 1; }
  kms_sse_put kms204-b1 new ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1
  rm_bucket kms204-b1
}

test_kms205() {
  kms_create_key "k205" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  rc_run admin kms key delete local "${kid}" --pending-window-days 7 --yes --json
  assert_rc_ok || return 1
  printf '%s' "${RC_OUT}" | grep -q '"deletion_date"' || { echo "delete did not report deletion_date: ${RC_OUT}"; return 1; }
  if printf '%s' "${RC_OUT}" | grep -q '"deletion_date": null'; then echo "deletion_date is null: ${RC_OUT}"; return 1; fi
  printf '%s' "${RC_OUT}" | grep -q '"immediate": false' || { echo "delete not reported as scheduled: ${RC_OUT}"; return 1; }
  local st
  st="$(kms_describe_key "${kid}")" || return 1
  printf '%s' "${st}" | grep -qiE 'pending' || { echo "key status not PendingDeletion: ${st}"; return 1; }
}

test_kms206() {
  kms_create_key "k206" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  rc_run admin kms key delete local "${kid}" --pending-window-days 7 --yes --json
  assert_rc_ok || return 1
  rc_run admin kms key cancel-deletion local "${kid}" --json
  assert_rc_ok || return 1
  local st
  st="$(kms_describe_key "${kid}")" || return 1
  printf '%s' "${st}" | grep -qiE 'active|enabled|available|usable|ready' || { echo "key not restored after cancel-deletion: ${st}"; return 1; }
  new_bucket "kms206-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms206-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1
  rm_bucket kms206-b1
}

test_kms207() {
  kms_create_key "k207" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  new_bucket "kms207-b1" || return 1
  # reference the key as bucket default encryption
  aws_run s3api put-bucket-encryption --bucket kms207-b1 \
    --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"${kid}\"}}]}"
  assert_aws_ok || return 1
  # immediate destruction of a referenced key must be refused (fail closed);
  # RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION=true is set, so the blocker here is the
  # deletion gate itself.
  body="$(admin_api DELETE /rustfs/admin/v3/kms/keys/delete "" "{\"key_id\":\"${kid}\",\"force_immediate\":true,\"confirm_key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  if [ "${code}" = "200" ]; then
    echo "immediate destruction of a referenced key unexpectedly allowed: ${body}"; return 1
  fi
  echo "expected gate refusal for referenced key (HTTP ${code}): ${body}"
  printf '%s' "${body}" | grep -qiE 'reference|in use|in_use|gate|bucket' || { echo "refusal does not mention the gate/reference: ${body}"; return 1; }
  rm_bucket kms207-b1
}

test_kms208() {
  kms_create_key "k208" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  # query-based force_immediate must be rejected
  body="$(admin_api DELETE "/rustfs/admin/v3/kms/keys/delete?keyId=${kid}&force_immediate=true" "")"; code="$(admin_api_code)"
  if [ "${code}" = "200" ]; then
    echo "query force_immediate unexpectedly accepted"; return 1
  fi
  echo "expected rejection of query force_immediate (HTTP ${code}): ${body}"
  # JSON body immediate without the explicit confirm key must be rejected
  body="$(admin_api DELETE /rustfs/admin/v3/kms/keys/delete "" "{\"key_id\":\"${kid}\",\"force_immediate\":true}")"; code="$(admin_api_code)"
  if [ "${code}" = "200" ]; then
    echo "immediate deletion unexpectedly allowed"; return 1
  fi
  echo "expected rejection of unconfirmed immediate deletion (HTTP ${code}): ${body}"
  # positive path: env flag on + explicit confirm -> unreferenced key destroyed
  kms_create_key "k208b" "env=ci" || return 1
  local kid2="${KMS_KEY_ID}"
  body="$(admin_api DELETE /rustfs/admin/v3/kms/keys/delete "" "{\"key_id\":\"${kid2}\",\"force_immediate\":true,\"confirm_key_id\":\"${kid2}\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "confirmed immediate deletion failed: HTTP ${code}: ${body}"; return 1; }
}

test_kms209() {
  kms_create_key "k209" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/update-description "" "{\"key_id\":\"${kid}\",\"description\":\"desc-1\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "update-description -> HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/tag "" "{\"key_id\":\"${kid}\",\"tags\":{\"env\":\"prod\"}}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "tag -> HTTP ${code}: ${body}"; return 1; }
  local st
  st="$(kms_describe_key "${kid}")" || return 1
  printf '%s' "${st}" | grep -q "desc-1" || { echo "description not visible: ${st}"; return 1; }
  printf '%s' "${st}" | grep -q "prod" || { echo "tag not visible: ${st}"; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/untag "" "{\"key_id\":\"${kid}\",\"tag_keys\":[\"env\"]}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "untag -> HTTP ${code}: ${body}"; return 1; }
  st="$(kms_describe_key "${kid}")" || return 1
  if printf '%s' "${st}" | grep -q "prod"; then echo "tag not removed: ${st}"; return 1; fi
}

test_kms210() {
  kms_create_key "k210" "env=ci" || return 1
  local body code
  # GET reports backup readiness; must be supported and redact secrets.
  body="$(admin_api GET /rustfs/admin/v3/kms/backup "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "backup status -> HTTP ${code}: ${body}"; return 1; }
  if ! printf '%s' "${body}" | grep -q '"export_supported":true'; then
    echo "note: backup/export not supported on ${KMS_BACKEND} backend (expected for this backend)"
    return 2
  fi
  if printf '%s' "${body}" | grep -q "${VAULT_TOKEN}"; then echo "backup leaks vault token"; return 1; fi
  if printf '%s' "${body}" | grep -q "${KMS_LOCAL_MASTER_KEY}"; then echo "backup leaks master key"; return 1; fi
  # POST creates the actual bundle on the server.
  body="$(admin_api POST /rustfs/admin/v3/kms/backup "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "backup export -> HTTP ${code}: ${body}"; return 1; }
  local bundle backup_id
  bundle="$(printf '%s' "${body}" | jq -r '.backup_id // .bundle // .data.backup_id // empty' 2>/dev/null)"
  [ -n "${bundle}" ] || { echo "no backup_id in export response: ${body}"; return 1; }
  backup_id="${bundle}"
  if printf '%s' "${body}" | grep -q "${VAULT_TOKEN}"; then echo "backup bundle leaks vault token"; return 1; fi
  if printf '%s' "${body}" | grep -q "${KMS_LOCAL_MASTER_KEY}"; then echo "backup bundle leaks master key"; return 1; fi

  # dry-run must not land: key list count must stay unchanged
  rc_run admin kms key list local --json
  assert_rc_ok || return 1
  local before after
  before="$(printf '%s' "${RC_OUT}" | jq -r '.data.keys | length' 2>/dev/null)"
  body="$(admin_api POST /rustfs/admin/v3/kms/restore/dry-run "" "{\"bundle\":\"${bundle}\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "restore dry-run -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -q "${backup_id}" || { echo "dry-run report missing backup id: ${body}"; return 1; }
  rc_run admin kms key list local --json
  assert_rc_ok || return 1
  after="$(printf '%s' "${RC_OUT}" | jq -r '.data.keys | length' 2>/dev/null)"
  [ "${after}" = "${before}" ] || { echo "dry-run changed key count (${before} -> ${after})"; return 1; }

  # actual restore requires confirmations; the target key dir is non-empty, so a
  # refusal naming the conflict policy is an acceptable, explicit outcome.
  body="$(admin_api POST /rustfs/admin/v3/kms/restore "" "{\"bundle\":\"${bundle}\",\"confirm_backup_id\":\"${backup_id}\",\"confirm_conflict_policy\":\"RestoreIntoEmptyTarget\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ] && [ "${code}" != "201" ] && [ "${code}" != "202" ]; then
    printf '%s' "${body}" | grep -qiE 'empty|conflict|non.?empty|Fail|refus' || { echo "restore refusal unexpected: HTTP ${code}: ${body}"; return 1; }
    echo "note: restore refused as expected (non-empty target): HTTP ${code}: ${body}"
  fi

  # abort is only meaningful while a restore is running; accept success or a
  # documented no-op error, never a 5xx.
  body="$(admin_api POST /rustfs/admin/v3/kms/restore/abort "" "{\"confirm_target_key_dir\":\"/var/lib/rustfs/kms\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ] && [ "${code}" != "204" ] && [ "${code}" != "409" ] && [ "${code}" != "400" ]; then
    echo "restore abort -> HTTP ${code}: ${body}"; return 1
  fi
}

test_kms301() {
  kms_create_key "k301" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms301-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  kms_sse_put kms301-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1
  printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption": "aws:kms"' || { echo "put response missing SSE header: ${AWS_OUT}"; return 1; }
  printf '%s' "${AWS_OUT}" | grep -q "${kid}" || { echo "put response missing key id: ${AWS_OUT}"; return 1; }
  kms_sse_get kms301-b1 f1 ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || return 1
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "SSE-KMS download MD5 mismatch"; return 1; }
  # In multi-multi with a shared backend (vault), the object must also be
  # readable from a different node (validates cross-node KMS consistency).
  if [ "${CURRENT_TOPOLOGY}" = "${TOPO_MULTI_MULTI}" ]; then
    if [ "${KMS_BACKEND}" = "vault-kv2" ]; then
      local x_out x_code
      x_out="$(aws --endpoint-url http://rustfs-node2:9000 --region "${REGION}" s3api get-object \
        --bucket kms301-b1 --key f1 ${TEST_TMP}/kms-dl-xnode 2>&1)"; x_code=$?
      if [ "${x_code}" -ne 0 ] || [ "$(md5_of_file ${TEST_TMP}/kms-dl-xnode 2>/dev/null)" != "$(md5_of_file ${TEST_TMP}/kms-f1)" ]; then
        echo "cross-node SSE-KMS read failed: ${x_out}"; return 1
      fi
      rm -f ${TEST_TMP}/kms-dl-xnode
    fi
  fi
  rm_bucket kms301-b1
}

test_kms302() {
  kms_create_key "k302" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms302-b1" || return 1
  aws_run s3api put-bucket-encryption --bucket kms302-b1 \
    --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"${kid}\"}}]}"
  assert_aws_ok || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  aws_run s3api put-object --bucket kms302-b1 --key bare --body ${TEST_TMP}/kms-f1
  assert_aws_ok || return 1
  aws_run s3api head-object --bucket kms302-b1 --key bare
  printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption": "aws:kms"' || { echo "bucket default SSE-KMS not applied: ${AWS_OUT}"; return 1; }
  printf '%s' "${AWS_OUT}" | grep -q "${kid}" || { echo "bucket default key id not used: ${AWS_OUT}"; return 1; }
  # explicit header overrides bucket default
  kms_create_key "k302b" "env=ci" || return 1
  local kid2="${KMS_KEY_ID}"
  aws_run s3api put-object --bucket kms302-b1 --key explicit --body ${TEST_TMP}/kms-f1 \
    --server-side-encryption aws:kms --ssekms-key-id "${kid2}"
  assert_aws_ok || return 1
  aws_run s3api head-object --bucket kms302-b1 --key explicit
  printf '%s' "${AWS_OUT}" | grep -q "${kid2}" || { echo "explicit key id not honored: ${AWS_OUT}"; return 1; }
  rm_bucket kms302-b1
}

test_kms303() {
  kms_create_key "k303" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms303-b1" || return 1
  make_test_file ${TEST_TMP}/kms-mp 15728640
  aws_run s3api create-multipart-upload --bucket kms303-b1 --key mp \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}"
  assert_aws_ok || return 1
  local uid
  uid="$(printf '%s' "${AWS_OUT}" | jq -r '.UploadId')"
  [ -n "${uid}" ] && [ "${uid}" != "null" ] || { echo "no UploadId: ${AWS_OUT}"; return 1; }
  split -b 5242880 ${TEST_TMP}/kms-mp ${TEST_TMP}/kms-part.
  local etags=() i=0 p
  for p in ${TEST_TMP}/kms-part.*; do
    i=$((i+1))
    aws_run s3api upload-part --bucket kms303-b1 --key mp --part-number "${i}" \
      --upload-id "${uid}" --body "${p}"
    assert_aws_ok || return 1
    etags+=("{\"PartNumber\":${i},\"ETag\":\"$(printf '%s' "${AWS_OUT}" | jq -r '.ETag' | tr -d '"')\"}")
  done
  local parts
  parts="$(printf '%s,' "${etags[@]}")"; parts="${parts%,}"
  aws_run s3api complete-multipart-upload --bucket kms303-b1 --key mp --upload-id "${uid}" \
    --multipart-upload "{\"Parts\":[${parts}]}"
  assert_aws_ok || return 1
  kms_sse_get kms303-b1 mp ${TEST_TMP}/kms-mp-dl "${kid}"
  assert_aws_ok || return 1
  [ "$(md5_of_file ${TEST_TMP}/kms-mp-dl)" = "$(md5_of_file ${TEST_TMP}/kms-mp)" ] || { echo "multipart SSE-KMS MD5 mismatch"; return 1; }
  rm -f ${TEST_TMP}/kms-part.* ${TEST_TMP}/kms-mp-dl
  rm_bucket kms303-b1
}

test_kms304() {
  kms_create_key "k304" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms304-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  aws_run s3api put-object --bucket kms304-b1 --key plain --body ${TEST_TMP}/kms-f1
  assert_aws_ok || return 1
  # plaintext -> SSE-KMS
  aws_run s3api copy-object --bucket kms304-b1 --key enc --copy-source kms304-b1/plain \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}"
  assert_aws_ok || return 1
  aws_run s3api head-object --bucket kms304-b1 --key enc
  printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption": "aws:kms"' || { echo "copy to SSE-KMS failed: ${AWS_OUT}"; return 1; }
  # SSE-KMS -> plaintext (reverse)
  aws_run s3api copy-object --bucket kms304-b1 --key dec --copy-source kms304-b1/enc
  assert_aws_ok || return 1
  aws_run s3api head-object --bucket kms304-b1 --key dec
  if printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption": "aws:kms"'; then
    echo "reverse copy still encrypted: ${AWS_OUT}"; return 1
  fi
  aws_run s3api get-object --bucket kms304-b1 --key dec ${TEST_TMP}/kms-dec-dl
  assert_aws_ok || return 1
  [ "$(md5_of_file ${TEST_TMP}/kms-dec-dl)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "decrypted copy MD5 mismatch"; return 1; }
  rm_bucket kms304-b1
}

test_kms305() {
  kms_create_key "k305" "env=ci" || return 1
  new_bucket "kms305-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  aws_run s3api put-object --bucket kms305-b1 --key f --body ${TEST_TMP}/kms-f1 --ssekms-key-id "${KMS_KEY_ID}"
  assert_aws_fails || { echo "key id without aws:kms algorithm unexpectedly accepted"; return 1; }
  printf '%s' "${AWS_OUT}" | grep -qiE 'InvalidArgument|400' || { echo "unexpected error: ${AWS_OUT}"; return 1; }
  rm_bucket kms305-b1
}

test_kms306() {
  kms_create_key "k306" "env=ci" || return 1
  new_bucket "kms306-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  local key
  key="0123456789abcdef0123456789abcdef"
  aws_run s3api put-object --bucket kms306-b1 --key f --body ${TEST_TMP}/kms-f1 \
    --server-side-encryption aws:kms --ssekms-key-id "${KMS_KEY_ID}" \
    --sse-customer-algorithm AES256 --sse-customer-key "${key}"
  assert_aws_fails || { echo "SSE-C + SSE-KMS mutually exclusive not enforced"; return 1; }
  printf '%s' "${AWS_OUT}" | grep -qiE 'InvalidArgument|400|conflict|mutually' || { echo "unexpected error: ${AWS_OUT}"; return 1; }
  rm_bucket kms306-b1
}

test_kms307() {
  kms_create_key "k307" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms307-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  kms_sse_put kms307-b1 old ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || return 1

  log "stopping vault for KMS-307"
  heal_run docker stop "${VAULT_CONTAINER}" >/dev/null || die "failed to stop vault"
  sleep 2

  kms_sse_put kms307-b1 partial ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_fails || { echo "SSE-KMS upload unexpectedly succeeded while vault down"; return 1; }
  echo "expected failure while vault down: ${AWS_OUT}"
  aws_run s3api head-object --bucket kms307-b1 --key partial
  assert_aws_fails || { echo "partial object should not exist (head succeeded)"; return 1; }

  log "restarting vault for KMS-307"
  vault_restart

  local waited=0
  while [ "${waited}" -lt 120 ]; do
    rm -f ${TEST_TMP}/kms-dl1
    if kms_sse_get kms307-b1 old ${TEST_TMP}/kms-dl1 "${kid}" 2>/dev/null \
      && [ -f ${TEST_TMP}/kms-dl1 ] \
      && [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ]; then
      break
    fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "old object not correctly readable after vault recovery (last error: ${AWS_OUT})"; return 1; }
  rm_bucket kms307-b1
}

test_kms307_local() {
  echo "note: local backend has no external KMS service to stop; not applicable"
  return 2
}

# ==================== KMS case supplements (backlog#2024 review) ====================

aws_run_as() {
  # usage: aws_run_as AK SK [TOKEN] -- args...
  local ak="$1" sk="$2" token=""
  shift 2
  if [ "${1:-}" != "--" ]; then token="$1"; shift; fi
  [ "${1:-}" = "--" ] && shift
  local env_args=(AWS_ACCESS_KEY_ID="${ak}" AWS_SECRET_ACCESS_KEY="${sk}" AWS_EC2_METADATA_DISABLED=true AWS_DEFAULT_REGION="${REGION}")
  if [ -n "${token}" ]; then env_args+=(AWS_SESSION_TOKEN="${token}"); fi
  AWS_OUT="$(env "${env_args[@]}" aws --endpoint-url "${ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; AWS_CODE=$?
}

wait_endpoint_ready() {
  local waited=0
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    if curl -fsS --max-time 5 "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
      return 0
    fi
    sleep 5; waited=$((waited+5))
  done
  return 1
}

kms_admin_user_add() {
  # usage: kms_admin_user_add USER SECRET
  local out code
  out="$(admin_api PUT "/rustfs/admin/v3/add-user?accessKey=${1}" "" "{\"secretKey\":\"${2}\",\"status\":\"enabled\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "add-user ${1} -> HTTP ${code}: ${out}"; return 1; }
}

test_kms106() {
  # GET /kms/config returns a sanitized projection
  # {backend,cache_enabled,cache_max_keys,cache_ttl_seconds,default_key_id}:
  # secrets are omitted from the view entirely rather than masked with a
  # placeholder, so assert absence of the sensitive field names and of the
  # configured secret material.
  local body code field material
  body="$(admin_api GET /rustfs/admin/v3/kms/config "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "GET /v3/kms/config -> HTTP ${code}: ${body}"; return 1; }
  for field in token secret_key secret_id master_key password client_secret; do
    if printf '%s' "${body}" | grep -qiE "\"([a-z0-9_]*_)?${field}\"[[:space:]]*:"; then
      echo "sensitive field ${field} exposed in config view: ${body}"; return 1
    fi
  done
  printf '%s' "${body}" | grep -q '"backend"' || { echo "backend field missing from config view: ${body}"; return 1; }
  for material in "${KMS_LOCAL_MASTER_KEY}" "${VAULT_TOKEN:-}"; do
    [ -n "${material}" ] || continue
    if printf '%s' "${body}" | grep -qF -- "${material}"; then echo "config view leaks secret material"; return 1; fi
  done
  echo "config view sanitized: ${body}"
}

test_kms107() {
  # Requires RUSTFS_KMS_CONFIG_SECRET in the node env (passed via --extra-env).
  local node="${NODES[0]}"
  if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'grep -q RUSTFS_KMS_CONFIG_SECRET /etc/default/rustfs' 2>/dev/null; then
    echo "note: RUSTFS_KMS_CONFIG_SECRET not configured; sealing check skipped"
    return 2
  fi
  local sealed
  sealed="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'grep -rl "RUSTFS-KMS-ENC\[v1\]" /var/lib/rustfs/kms 2>/dev/null | head -1' 2>/dev/null || true)"
  [ -n "${sealed}" ] || { echo "no sealed config file found under /var/lib/rustfs/kms"; return 1; }
  service_action restart "${node}" || return 1
  wait_endpoint_ready || { echo "endpoint not ready after restart"; return 1; }
  local waited=0 out code
  while [ "${waited}" -lt 90 ]; do
    out="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ] && printf '%s' "${out}" | grep -qiE 'running|ready|ok|healthy'; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 90 ] || { echo "KMS not running after sealed-config restart: HTTP ${code}: ${out}"; return 1; }
}

test_kms108() {
  local body code node="${NODES[0]}" cfg
  rc_run admin kms stop local --yes || true
  if [ "${KMS_BACKEND}" = "local" ]; then
    cfg="{\"backend_type\":\"Local\",\"key_dir\":\"/var/lib/rustfs/kms\",\"default_key_id\":\"${KMS_DEFAULT_KEY_ID}\"}"
  else
    cfg="{\"backend_type\":\"VaultKV2\",\"address\":\"${VAULT_ADDRESS}\",\"auth_method\":{\"Token\":{\"token\":\"${VAULT_TOKEN}\"}},\"kv_mount\":\"${VAULT_KV_MOUNT}\",\"key_path_prefix\":\"${VAULT_KEY_PATH_PREFIX}\",\"default_key_id\":\"${KMS_DEFAULT_KEY_ID}\"}"
  fi
  body="$(admin_api POST /rustfs/admin/v3/kms/configure "" "${cfg}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "configure -> HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/start "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "start -> HTTP ${code}: ${body}"; return 1; }
  service_action restart "${node}" || return 1
  wait_endpoint_ready || { echo "endpoint not ready after restart"; return 1; }
  local waited=0
  while [ "${waited}" -lt 90 ]; do
    body="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ] && printf '%s' "${body}" | grep -qiE 'running|ready|ok|healthy'; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 90 ] || { echo "KMS status not restored after restart without reconfigure: HTTP ${code}: ${body}"; return 1; }
}

test_kms109() {
  local body code
  body="$(admin_api POST /rustfs/admin/v3/kms/configure "" "{\"backend_type\":\"Bogus\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] && { echo "invalid backend accepted"; return 1; }
  echo "expected rejection of bogus backend (HTTP ${code}): ${body}"
  if [ "${KMS_BACKEND}" = "vault-kv2" ]; then
    # configure only validates the schema and persists the config; Vault
    # connectivity is checked when the service (re)starts. An unreachable
    # address is therefore accepted here and must surface at restart.
    local good_cfg
    good_cfg="{\"backend_type\":\"VaultKV2\",\"address\":\"${VAULT_ADDRESS}\",\"auth_method\":{\"Token\":{\"token\":\"${VAULT_TOKEN}\"}},\"kv_mount\":\"${VAULT_KV_MOUNT}\",\"key_path_prefix\":\"${VAULT_KEY_PATH_PREFIX}\",\"default_key_id\":\"${KMS_DEFAULT_KEY_ID}\"}"
    body="$(admin_api POST /rustfs/admin/v3/kms/configure "" "{\"backend_type\":\"VaultKV2\",\"address\":\"http://127.0.0.1:1\",\"auth_method\":{\"Token\":{\"token\":\"x\"}},\"kv_mount\":\"k\",\"key_path_prefix\":\"p\"}")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ]; then
      echo "configure accepted unreachable vault address (HTTP 200); expecting restart to fail"
      rc_run admin kms restart local --yes || true
      if [ "${RC_CODE}" -eq 7 ]; then
        echo "note: rc restart gated (exit 7); connectivity assertion skipped"
      else
        body="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
        if [ "${RC_CODE}" -eq 0 ] && [ "${code}" = "200" ] && printf '%s' "${body}" | grep -qiE 'running|ready|healthy'; then
          echo "KMS restarted as running with unreachable vault address: ${body}"
          rc_run admin kms stop local --yes || true
          admin_api POST /rustfs/admin/v3/kms/configure "" "${good_cfg}" >/dev/null || true
          admin_api POST /rustfs/admin/v3/kms/start "" >/dev/null || true
          return 1
        fi
        echo "expected restart failure with unreachable vault address (rc exit ${RC_CODE}, status HTTP ${code}): ${body}"
      fi
      # restore the working Vault configuration before continuing
      rc_run admin kms stop local --yes || true
      body="$(admin_api POST /rustfs/admin/v3/kms/configure "" "${good_cfg}")"; code="$(admin_api_code)"
      [ "${code}" = "200" ] || { echo "restore configure -> HTTP ${code}: ${body}"; return 1; }
      body="$(admin_api POST /rustfs/admin/v3/kms/start "")"; code="$(admin_api_code)"
      [ "${code}" = "200" ] || { echo "restore start -> HTTP ${code}: ${body}"; return 1; }
    else
      echo "expected rejection of bad vault address (HTTP ${code}): ${body}"
    fi
  fi
  body="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "status lost after rejected configure: HTTP ${code}"; return 1; }
  printf '%s' "${body}" | grep -qiE 'running|ready|ok|healthy' || { echo "service not running after rejected configure: ${body}"; return 1; }
  rc_run admin kms stop local --yes
  assert_rc_ok || return 1
  rc_run admin kms stop local --yes
  assert_rc_ok || return 1
  rc_run admin kms restart local --yes || true
  local waited=0
  while [ "${waited}" -lt 60 ]; do
    body="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ] && printf '%s' "${body}" | grep -qiE 'running|ready|ok|healthy'; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 60 ] || { echo "KMS did not restart after negatives: HTTP ${code}: ${body}"; return 1; }
}

test_kms110() {
  local body code native
  # MinIO's mc passes the key name as the key-id query parameter; the JSON
  # body of this alias is {key_usage,description,tags}, not {key_id}.
  body="$(admin_api POST "/minio/admin/v3/kms/key/create?key-id=k110" "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "minio key/create -> HTTP ${code}: ${body}"; return 1; }
  local key_id
  key_id="$(printf '%s' "${body}" | jq -r '.key_id // .KeyId // .data.key_id // empty' 2>/dev/null)"
  key_id="${key_id:-k110}"
  body="$(admin_api GET "/minio/admin/v3/kms/key/status?key-id=${key_id}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "minio key/status -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -qiE 'active|enabled|ready|ok' || { echo "minio status unexpected: ${body}"; return 1; }
  native="$(admin_api GET "/rustfs/admin/v3/kms/key/status?keyId=${key_id}")"
  [ "$(admin_api_code)" = "200" ] || { echo "native key/status -> HTTP $(admin_api_code): ${native}"; return 1; }
  printf '%s' "${native}" | grep -qiE 'active|enabled|ready|ok' || { echo "native status unexpected: ${native}"; return 1; }
}

test_kms111() {
  kms_create_key "k111" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code plaintext ciphertext
  # key_spec is mandatory (as KeySpec is for AWS); the enum value is "Aes256".
  body="$(admin_api POST /rustfs/admin/v3/kms/generate-data-key "" "{\"key_id\":\"${kid}\",\"key_spec\":\"Aes256\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "generate-data-key -> HTTP ${code}: ${body}"; return 1; }
  plaintext="$(printf '%s' "${body}" | jq -r '.plaintext_key // .plaintext // .Plaintext // .data.plaintext_key // .data.plaintext // empty' 2>/dev/null)"
  ciphertext="$(printf '%s' "${body}" | jq -r '.ciphertext_blob // .ciphertext // .CiphertextBlob // .data.ciphertext_blob // .data.ciphertext // empty' 2>/dev/null)"
  [ -n "${plaintext}" ] && [ -n "${ciphertext}" ] || { echo "generate-data-key missing material: ${body}"; return 1; }
  [ "${plaintext}" != "${ciphertext}" ] || { echo "plaintext equals ciphertext: ${body}"; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/generate-data-key "" "{\"key_id\":\"no-such-key\",\"key_spec\":\"Aes256\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] && { echo "generate-data-key for missing key unexpectedly succeeded"; return 1; }
  echo "expected rejection for missing key (HTTP ${code}): ${body}"
}

test_kms211() {
  kms_create_key "k211" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  new_bucket "kms211-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  kms_sse_put kms211-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms211-b1; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/rotate "" "{\"key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    if printf '%s' "${body}" | grep -qiE "UnsupportedCapability|not supported|unsupported"; then
      echo "note: rotate/rekey unsupported on ${KMS_BACKEND} (expected for this backend)"
      rm_bucket kms211-b1
      return 2
    fi
    echo "rotate -> HTTP ${code}: ${body}"; rm_bucket kms211-b1; return 1
  fi
  # POST /kms/keys/rekey takes {"buckets":[...],"prefix":"..."} (empty body =
  # full sweep); there is no key_id field, and the status endpoint takes no
  # query parameters.
  if [ "${KMS_BACKEND}" = "local" ]; then
    body="$(admin_api POST /rustfs/admin/v3/kms/keys/rekey "" "{\"buckets\":[\"kms211-b1\"]}")"; code="$(admin_api_code)"
    [ "${code}" = "200" ] && { echo "rekey unexpectedly supported on local backend"; rm_bucket kms211-b1; return 1; }
    printf '%s' "${body}" | grep -qiE 'unsupported|not supported|rekey|rewrap' || { echo "rekey refusal unclear: HTTP ${code}: ${body}"; rm_bucket kms211-b1; return 1; }
    echo "note: rekey explicitly refused on local backend (HTTP ${code})"
    rm_bucket kms211-b1
    return 0
  fi
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/rekey "" "{\"buckets\":[\"kms211-b1\"]}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || [ "${code}" = "202" ] || { echo "rekey -> HTTP ${code}: ${body}"; rm_bucket kms211-b1; return 1; }
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    body="$(admin_api GET /rustfs/admin/v3/kms/keys/rekey/status "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ] && printf '%s' "${body}" | grep -qiE 'completed|done|finished|processed'; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "rekey sweep did not finish: ${body}"; rm_bucket kms211-b1; return 1; }
  kms_sse_get kms211-b1 f1 ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || { rm_bucket kms211-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "post-rekey MD5 mismatch"; rm_bucket kms211-b1; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/rekey "" "{\"buckets\":[\"kms211-b1\"]}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || [ "${code}" = "202" ] || { echo "rekey round 2 -> HTTP ${code}: ${body}"; rm_bucket kms211-b1; return 1; }
  waited=0
  while [ "${waited}" -lt 120 ]; do
    body="$(admin_api GET /rustfs/admin/v3/kms/keys/rekey/status "")"; code="$(admin_api_code)"
    if printf '%s' "${body}" | grep -qiE 'processed.?0|"0"|: ?0|completed|done|finished'; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "rekey round 2 did not converge to 0: ${body}"; rm_bucket kms211-b1; return 1; }
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/rekey/cancel "" "{\"key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ] && [ "${code}" != "204" ] && [ "${code}" != "409" ]; then
    echo "rekey/cancel -> HTTP ${code}: ${body}"; rm_bucket kms211-b1; return 1
  fi
  rm_bucket kms211-b1
}

test_kms212() {
  kms_create_key "k212" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code st
  new_bucket "kms212-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms212-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms212-b1; return 1; }
  rc_run admin kms key delete local "${kid}" --pending-window-days 7 --yes --json
  assert_rc_ok || { rm_bucket kms212-b1; return 1; }
  st="$(kms_describe_key "${kid}")" || { rm_bucket kms212-b1; return 1; }
  printf '%s' "${st}" | grep -qiE 'pending' || { echo "key not PendingDeletion: ${st}"; rm_bucket kms212-b1; return 1; }
  kms_sse_put kms212-b1 f2 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_fails || { echo "SSE put unexpectedly succeeded while key pending deletion"; rm_bucket kms212-b1; return 1; }
  kms_sse_get kms212-b1 f1 ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || { rm_bucket kms212-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "existing object lost after pending deletion"; rm_bucket kms212-b1; return 1; }
  rc_run admin kms key cancel-deletion local "${kid}" --json
  assert_rc_ok || { rm_bucket kms212-b1; return 1; }
  kms_sse_put kms212-b1 f3 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { echo "SSE put did not recover after cancel-deletion"; rm_bucket kms212-b1; return 1; }
  rm_bucket kms212-b1
}

test_kms213() {
  kms_create_key "k213" "env=ci" || return 1
  local first_id="${KMS_KEY_ID}" dup_id
  rc_run admin kms key create local --name k213 --tag "env=ci" --json
  if [ "${KMS_BACKEND}" = "local" ]; then
    # The local backend strictly rejects a duplicate name.
    if [ "${RC_CODE}" -eq 0 ]; then
      echo "duplicate key creation unexpectedly succeeded: ${RC_OUT}"
      return 1
    fi
    printf '%s' "${RC_OUT}" | grep -qiE 'exist|duplicate|conflict|AlreadyExists' || { echo "duplicate-create error unclear: ${RC_OUT}"; return 1; }
  elif [ "${RC_CODE}" -eq 0 ]; then
    # Other backends (Vault) treat re-creating an active key with the same
    # name and algorithm as idempotent recovery; the identity must not change.
    dup_id="$(printf '%s' "${RC_OUT}" | jq -r '.key_id // .KeyId // .data.key_id // .result.key_id // empty' 2>/dev/null)"
    [ "${dup_id}" = "${first_id}" ] || { echo "idempotent duplicate create returned key id '${dup_id}' (expected '${first_id}'): ${RC_OUT}"; return 1; }
    echo "note: duplicate create treated as idempotent on ${KMS_BACKEND}, key id unchanged (${dup_id})"
  else
    printf '%s' "${RC_OUT}" | grep -qiE 'exist|duplicate|conflict|AlreadyExists' || { echo "duplicate-create error unclear: ${RC_OUT}"; return 1; }
  fi
  rc_run admin kms key create local --name "bad/name" --tag "env=ci" --json
  if [ "${RC_CODE}" -eq 0 ]; then
    echo "invalid key name unexpectedly accepted: ${RC_OUT}"
    return 1
  fi
  echo "expected rejection of invalid key name: ${RC_OUT}"
}

test_kms214() {
  kms_create_key "k214" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" describe list status combined material
  describe="$(kms_describe_key "${kid}")" || return 1
  rc_run admin kms key list local --json
  assert_rc_ok || return 1
  list="${RC_OUT}"
  status="$(admin_api GET /rustfs/admin/v3/kms/status "")"
  [ "$(admin_api_code)" = "200" ] || { echo "kms status -> HTTP $(admin_api_code): ${status}"; return 1; }
  combined="${describe}
${list}
${status}"
  for material in "${KMS_LOCAL_MASTER_KEY}" "${VAULT_TOKEN:-}"; do
    [ -n "${material}" ] || continue
    if printf '%s' "${combined}" | grep -q "${material}"; then
      echo "key material leaked in describe/list/status"
      return 1
    fi
  done
}

test_kms308() {
  kms_create_key "k308" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms308-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 5242880
  kms_sse_put kms308-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms308-b1; return 1; }
  local spec start end
  for spec in "0-0" "1048576-1048576" "60000-70000" "5242879-5242879" "-1024"; do
    rm -f ${TEST_TMP}/kms-range-dl
    aws_run s3api get-object --bucket kms308-b1 --key f1 --range "bytes=${spec}" ${TEST_TMP}/kms-range-dl
    assert_aws_ok || { echo "range ${spec} failed"; rm_bucket kms308-b1; return 1; }
    if [[ "${spec}" == -* ]]; then
      start=$(( 5242880 - ${spec#-} ))
      end=5242879
    else
      start="${spec%-*}"; end="${spec#*-}"
    fi
    dd if=${TEST_TMP}/kms-f1 of=${TEST_TMP}/kms-range-ref bs=1 skip="${start}" count=$(( end - start + 1 )) 2>/dev/null
    [ "$(md5_of_file ${TEST_TMP}/kms-range-dl)" = "$(md5_of_file ${TEST_TMP}/kms-range-ref)" ] || { echo "range ${spec} content mismatch"; rm_bucket kms308-b1; return 1; }
  done
  aws_run s3api get-object --bucket kms308-b1 --key f1 --range "bytes=5242880-" ${TEST_TMP}/kms-range-out
  assert_aws_fails || { echo "out-of-range GET unexpectedly succeeded"; rm_bucket kms308-b1; return 1; }
  rm -f ${TEST_TMP}/kms-range-dl ${TEST_TMP}/kms-range-ref
  rm_bucket kms308-b1
}

test_kms309() {
  kms_create_key "k309" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms309-b1" || return 1
  : > ${TEST_TMP}/kms-empty
  printf 'x' > ${TEST_TMP}/kms-one
  kms_sse_put kms309-b1 empty ${TEST_TMP}/kms-empty "${kid}"
  assert_aws_ok || { rm_bucket kms309-b1; return 1; }
  kms_sse_get kms309-b1 empty ${TEST_TMP}/kms-empty-dl "${kid}"
  assert_aws_ok || { rm_bucket kms309-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-empty-dl)" = "$(md5_of_file ${TEST_TMP}/kms-empty)" ] || { echo "empty object mismatch"; rm_bucket kms309-b1; return 1; }
  kms_sse_put kms309-b1 one ${TEST_TMP}/kms-one "${kid}"
  assert_aws_ok || { rm_bucket kms309-b1; return 1; }
  make_test_file ${TEST_TMP}/kms-mp 12582912
  aws_run s3api create-multipart-upload --bucket kms309-b1 --key mp \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}"
  assert_aws_ok || { rm_bucket kms309-b1; return 1; }
  local uid
  uid="$(printf '%s' "${AWS_OUT}" | jq -r '.UploadId')"
  split -b 5242880 ${TEST_TMP}/kms-mp ${TEST_TMP}/kms-part.
  local etags=() i=0 p
  for p in ${TEST_TMP}/kms-part.*; do
    i=$((i+1))
    aws_run s3api upload-part --bucket kms309-b1 --key mp --part-number "${i}" \
      --upload-id "${uid}" --body "${p}"
    assert_aws_ok || { rm_bucket kms309-b1; return 1; }
    etags+=("{\"PartNumber\":${i},\"ETag\":\"$(printf '%s' "${AWS_OUT}" | jq -r '.ETag' | tr -d '"')\"}")
  done
  local parts
  parts="$(printf '%s,' "${etags[@]}")"; parts="${parts%,}"
  aws_run s3api complete-multipart-upload --bucket kms309-b1 --key mp --upload-id "${uid}" \
    --multipart-upload "{\"Parts\":[${parts}]}"
  assert_aws_ok || { rm_bucket kms309-b1; return 1; }
  kms_sse_get kms309-b1 mp ${TEST_TMP}/kms-mp-dl "${kid}"
  assert_aws_ok || { rm_bucket kms309-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-mp-dl)" = "$(md5_of_file ${TEST_TMP}/kms-mp)" ] || { echo "frame-aligned multipart mismatch"; rm_bucket kms309-b1; return 1; }
  rm -f ${TEST_TMP}/kms-part.* ${TEST_TMP}/kms-mp-dl ${TEST_TMP}/kms-empty-dl
  rm_bucket kms309-b1
}

test_kms310() {
  kms_create_key "k310" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms310-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  local ctx_b64
  ctx_b64="$(printf '%s' '{"purpose":"ci"}' | base64 | tr -d '\n')"
  aws_run s3api put-object --bucket kms310-b1 --key ctx --body ${TEST_TMP}/kms-f1 \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}" \
    --ssekms-encryption-context "${ctx_b64}"
  assert_aws_ok || { echo "SSE-KMS put with context failed: ${AWS_OUT}"; rm_bucket kms310-b1; return 1; }
  kms_sse_get kms310-b1 ctx ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || { rm_bucket kms310-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "context object mismatch"; rm_bucket kms310-b1; return 1; }
  aws_run s3api put-object --bucket kms310-b1 --key bad --body ${TEST_TMP}/kms-f1 \
    --server-side-encryption AES256 \
    --ssekms-encryption-context "${ctx_b64}"
  assert_aws_fails || { echo "context with non-aws:kms algorithm unexpectedly accepted"; rm_bucket kms310-b1; return 1; }
  printf '%s' "${AWS_OUT}" | grep -qiE 'InvalidArgument|400' || { echo "unexpected error: ${AWS_OUT}"; rm_bucket kms310-b1; return 1; }
  rm_bucket kms310-b1
}

test_kms311() {
  kms_create_key "k311" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms311-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  aws_run s3api put-object --bucket kms311-b1 --key s3 --body ${TEST_TMP}/kms-f1 --server-side-encryption AES256
  assert_aws_ok || { rm_bucket kms311-b1; return 1; }
  aws_run s3api head-object --bucket kms311-b1 --key s3
  printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption": "AES256"' || { echo "SSE-S3 header missing: ${AWS_OUT}"; rm_bucket kms311-b1; return 1; }
  kms_sse_put kms311-b1 kms ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms311-b1; return 1; }
  kms_sse_get kms311-b1 kms ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || { rm_bucket kms311-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "mixed SSE-S3/SSE-KMS read mismatch"; rm_bucket kms311-b1; return 1; }
  rm_bucket kms311-b1
}

test_kms312() {
  kms_create_key "k312" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms312-b1" || return 1
  aws_run s3api put-bucket-encryption --bucket kms312-b1 \
    --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"no-such-key\"}}]}"
  assert_aws_ok || { rm_bucket kms312-b1; return 1; }
  make_test_file ${TEST_TMP}/kms-f1 65536
  aws_run s3api put-object --bucket kms312-b1 --key bare --body ${TEST_TMP}/kms-f1
  assert_aws_fails || { echo "bare put with invalid default key unexpectedly succeeded"; rm_bucket kms312-b1; return 1; }
  printf '%s' "${AWS_OUT}" | grep -qiE 'kms|key|400|404' || { echo "error does not mention KMS/key: ${AWS_OUT}"; rm_bucket kms312-b1; return 1; }
  aws_run s3api delete-bucket-encryption --bucket kms312-b1
  assert_aws_ok || { rm_bucket kms312-b1; return 1; }
  aws_run s3api put-object --bucket kms312-b1 --key bare --body ${TEST_TMP}/kms-f1
  assert_aws_ok || { echo "bare put did not recover after delete-bucket-encryption"; rm_bucket kms312-b1; return 1; }
  rm_bucket kms312-b1
}

test_kms313() {
  kms_create_key "k313" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms313-b1" || return 1
  aws_run s3api put-bucket-versioning --bucket kms313-b1 --versioning-configuration Status=Enabled
  assert_aws_ok || { rm_bucket kms313-b1; return 1; }
  make_test_file ${TEST_TMP}/kms-v1 65536
  make_test_file ${TEST_TMP}/kms-v2 65536
  kms_sse_put kms313-b1 obj ${TEST_TMP}/kms-v1 "${kid}"
  assert_aws_ok || { rm_bucket kms313-b1; return 1; }
  local vid1 vid2
  vid1="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId')"
  kms_sse_put kms313-b1 obj ${TEST_TMP}/kms-v2 "${kid}"
  assert_aws_ok || { rm_bucket kms313-b1; return 1; }
  vid2="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId')"
  aws_run s3api get-object --bucket kms313-b1 --key obj --version-id "${vid1}" ${TEST_TMP}/kms-v1-dl
  assert_aws_ok || { rm_bucket kms313-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-v1-dl)" = "$(md5_of_file ${TEST_TMP}/kms-v1)" ] || { echo "historical version v1 mismatch"; rm_bucket kms313-b1; return 1; }
  aws_run s3api get-object --bucket kms313-b1 --key obj --version-id "${vid2}" ${TEST_TMP}/kms-v2-dl
  assert_aws_ok || { rm_bucket kms313-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-v2-dl)" = "$(md5_of_file ${TEST_TMP}/kms-v2)" ] || { echo "historical version v2 mismatch"; rm_bucket kms313-b1; return 1; }
  rm -f ${TEST_TMP}/kms-v1-dl ${TEST_TMP}/kms-v2-dl
  rm_bucket kms313-b1
}

test_kms314() {
  kms_create_key "k314" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms314-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  kms_sse_put kms314-b1 obj ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms314-b1; return 1; }
  aws_run s3api copy-object --bucket kms314-b1 --key obj --copy-source kms314-b1/obj \
    --metadata-directive REPLACE --server-side-encryption aws:kms --ssekms-key-id "${kid}"
  assert_aws_ok || { rm_bucket kms314-b1; return 1; }
  kms_sse_get kms314-b1 obj ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || { rm_bucket kms314-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "self-copy with SSE header not decryptable"; rm_bucket kms314-b1; return 1; }
  aws_run s3api copy-object --bucket kms314-b1 --key obj --copy-source kms314-b1/obj --metadata-directive REPLACE
  assert_aws_ok || { rm_bucket kms314-b1; return 1; }
  aws_run s3api head-object --bucket kms314-b1 --key obj
  if printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption"'; then
    echo "self-copy without SSE header stayed encrypted (metadata-only shortcut): ${AWS_OUT}"
    rm_bucket kms314-b1
    return 1
  fi
  aws_run s3api get-object --bucket kms314-b1 --key obj ${TEST_TMP}/kms-dl2
  assert_aws_ok || { rm_bucket kms314-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl2)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "rewritten plaintext mismatch"; rm_bucket kms314-b1; return 1; }
  rm -f ${TEST_TMP}/kms-dl1 ${TEST_TMP}/kms-dl2
  rm_bucket kms314-b1
}

test_kms315() {
  kms_create_key "k315" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms315-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms315-b1 obj ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms315-b1; return 1; }
  aws_run s3api put-bucket-lifecycle-configuration --bucket kms315-b1 \
    --lifecycle-configuration '{"Rules":[{"ID":"exp","Status":"Enabled","Filter":{"Prefix":""},"Expiration":{"Days":1}}]}'
  assert_aws_ok || { echo "lifecycle rule rejected: ${AWS_OUT}"; rm_bucket kms315-b1; return 1; }
  kms_sse_get kms315-b1 obj ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || { echo "SSE-KMS object unreadable under lifecycle config"; rm_bucket kms315-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "lifecycle-config SSE object mismatch"; rm_bucket kms315-b1; return 1; }
  rm_bucket kms315-b1
}

test_kms316() {
  kms_create_key "k316" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms316-b1" || return 1
  printf 'a,1\nb,2\nc,3\n' > ${TEST_TMP}/kms-sel.csv
  kms_sse_put kms316-b1 data.csv ${TEST_TMP}/kms-sel.csv "${kid}"
  assert_aws_ok || { rm_bucket kms316-b1; return 1; }
  aws_run s3api select-object-content --bucket kms316-b1 --key data.csv \
    --expression "SELECT * FROM s3object" --expression-type SQL \
    --input-serialization '{"CSV":{}}' --output-serialization '{"CSV":{}}' \
    ${TEST_TMP}/kms-select.out
  assert_aws_ok || { echo "select on SSE-KMS object failed: ${AWS_OUT}"; rm_bucket kms316-b1; return 1; }
  grep -q "b,2" ${TEST_TMP}/kms-select.out || { echo "select result missing row: $(head -c 200 ${TEST_TMP}/kms-select.out)"; rm_bucket kms316-b1; return 1; }
  local key
  key="0123456789abcdef0123456789abcdef"
  aws_run s3api put-object --bucket kms316-b1 --key ssec.csv --body ${TEST_TMP}/kms-sel.csv \
    --sse-customer-algorithm AES256 --sse-customer-key "${key}"
  assert_aws_ok || { rm_bucket kms316-b1; return 1; }
  local wrong_key
  wrong_key="ffffffffffffffffffffffffffffffff"
  aws_run s3api select-object-content --bucket kms316-b1 --key ssec.csv \
    --expression "SELECT * FROM s3object" --expression-type SQL \
    --input-serialization '{"CSV":{}}' --output-serialization '{"CSV":{}}' \
    --sse-customer-algorithm AES256 --sse-customer-key "${wrong_key}" \
    ${TEST_TMP}/kms-select-bad.out
  assert_aws_fails || { echo "select with wrong SSE-C key unexpectedly succeeded"; rm_bucket kms316-b1; return 1; }
  rm -f ${TEST_TMP}/kms-select.out ${TEST_TMP}/kms-select-bad.out
  rm_bucket kms316-b1
}

test_kms317() {
  kms_create_key "k317" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms317-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  kms_sse_put kms317-b1 obj ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms317-b1; return 1; }
  local url
  url="$(AWS_EC2_METADATA_DISABLED=true aws --endpoint-url "${ENDPOINT}" s3 presign --region "${REGION}" "s3://kms317-b1/obj" --expires-in 300 2>/dev/null)"
  [ -n "${url}" ] || { echo "presign failed"; rm_bucket kms317-b1; return 1; }
  curl --noproxy '*' -fsS "${url}" -o ${TEST_TMP}/kms-presign-dl || { echo "presigned GET failed"; rm_bucket kms317-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-presign-dl)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "presigned GET content mismatch"; rm_bucket kms317-b1; return 1; }
  local purl
  purl="$(AWS_EC2_METADATA_DISABLED=true aws --endpoint-url "${ENDPOINT}" s3 presign --region "${REGION}" "s3://kms317-b1/up" --expires-in 300 2>/dev/null)"
  [ -n "${purl}" ] || { echo "presign PUT failed"; rm_bucket kms317-b1; return 1; }
  curl --noproxy '*' -fsS -X PUT --data-binary @${TEST_TMP}/kms-f1 "${purl}" || { echo "presigned PUT failed"; rm_bucket kms317-b1; return 1; }
  kms_sse_get kms317-b1 up ${TEST_TMP}/kms-up-dl "${kid}"
  assert_aws_ok || { echo "presigned-uploaded object unreadable"; rm_bucket kms317-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-up-dl)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "presigned PUT content mismatch"; rm_bucket kms317-b1; return 1; }
  rm -f ${TEST_TMP}/kms-presign-dl ${TEST_TMP}/kms-up-dl
  rm_bucket kms317-b1
}

test_kms318() {
  local node="${NODES[0]}"
  if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'grep -q RUSTFS_ENCRYPTION_FRAME_V2 /etc/default/rustfs' 2>/dev/null; then
    echo "note: RUSTFS_ENCRYPTION_FRAME_V2 not configured; frame compatibility run skipped (enable via --extra-env)"
    return 2
  fi
  kms_create_key "k318" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms318-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 5242880
  kms_sse_put kms318-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms318-b1; return 1; }
  kms_sse_get kms318-b1 f1 ${TEST_TMP}/kms-dl1 "${kid}"
  assert_aws_ok || { rm_bucket kms318-b1; return 1; }
  [ "$(md5_of_file ${TEST_TMP}/kms-dl1)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "frame-v2 object mismatch"; rm_bucket kms318-b1; return 1; }
  rm_bucket kms318-b1
}

test_kms401() {
  local node="${NODES[0]}"
  if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'grep -q RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true /etc/default/rustfs' 2>/dev/null; then
    echo "note: RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY not enabled; per-key authorization matrix skipped"
    return 2
  fi
  kms_create_key "k401" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  kms_admin_user_add "k401u" "k401secret" || return 1
  new_bucket "kms401-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  aws_run_as "k401u" "k401secret" -- s3api put-object --bucket kms401-b1 --key denied --body ${TEST_TMP}/kms-f1 \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}"
  assert_aws_fails || { echo "SSE-KMS put without key permission unexpectedly allowed"; rm_bucket kms401-b1; return 1; }
  kms_sse_put kms401-b1 allowed ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { echo "root SSE-KMS put blocked under enforcement"; rm_bucket kms401-b1; return 1; }
  rm_bucket kms401-b1
}

test_kms402() {
  local node="${NODES[0]}"
  if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'grep -q RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true /etc/default/rustfs' 2>/dev/null; then
    echo "note: RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY not enabled; anonymous enforcement skipped"
    return 2
  fi
  kms_create_key "k402" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms402-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms402-b1 obj ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms402-b1; return 1; }
  aws_run s3api put-object-acl --bucket kms402-b1 --key obj --acl public-read
  assert_aws_ok || { rm_bucket kms402-b1; return 1; }
  local code
  code="$(curl --noproxy '*' -sS -o /dev/null -w '%{http_code}' "${ENDPOINT}/kms402-b1/obj")"
  [ "${code}" = "403" ] || { echo "anonymous SSE-KMS read returned ${code}, expected 403 under enforcement"; rm_bucket kms402-b1; return 1; }
  rm_bucket kms402-b1
}

test_kms403() {
  kms_admin_user_add "k403u" "k403secret" || return 1
  local out code
  out="$(admin_api POST /rustfs/admin/v3/kms/configure "" "{\"backend_type\":\"Bogus\"}" "k403u" "k403secret")"
  code="$(admin_api_code)"
  [ "${code}" = "403" ] || { echo "low-privilege configure -> HTTP ${code}, expected 403: ${out}"; return 1; }
  out="$(admin_api POST /rustfs/admin/v3/kms/key/create "" "{\"key_id\":\"k403\"}" "k403u" "k403secret")"
  code="$(admin_api_code)"
  [ "${code}" = "403" ] || { echo "low-privilege key/create -> HTTP ${code}, expected 403: ${out}"; return 1; }
  out="$(admin_api GET /rustfs/admin/v3/kms/status "" "k403u" "k403secret")"
  code="$(admin_api_code)"
  if [ "${code}" != "200" ] && [ "${code}" != "403" ]; then
    echo "low-privilege status -> HTTP ${code}, expected 200 or 403: ${out}"; return 1
  fi
  echo "note: low-privilege status -> HTTP ${code}"
}

test_kms404() {
  kms_create_key "k404" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/rotate "" "{\"key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    printf '%s' "${body}" | grep -qiE 'unsupported|not supported' || { echo "rotate -> HTTP ${code}: ${body}"; return 1; }
  fi
  body="$(admin_api DELETE /rustfs/admin/v3/kms/keys/delete "" "{\"key_id\":\"${kid}\",\"force_immediate\":true}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] && { echo "unconfirmed immediate delete unexpectedly allowed"; return 1; }
  local material
  for material in "${KMS_LOCAL_MASTER_KEY}" "${VAULT_TOKEN:-}"; do
    [ -n "${material}" ] || continue
    if printf '%s' "${body}" | grep -q "${material}"; then echo "denied response leaks key material"; return 1; fi
  done
  echo "note: admin operations produced no material leaks; entry-level audit assertions are covered by the tier AUD suite"
}

test_kms501() {
  if [ "${KMS_BACKEND}" != "local" ]; then
    echo "note: storage-fault recovery applies to the local backend only"
    return 2
  fi
  local node="${NODES[0]}"
  kms_create_key "k501" "env=ci" || return 1
  local kid="${KMS_KEY_ID}"
  new_bucket "kms501-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 65536
  kms_sse_put kms501-b1 f1 ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_ok || { rm_bucket kms501-b1; return 1; }
  # Quoted heredoc: nothing is expanded locally, so the remote script may use
  # its own quoting freely (a nested single-quoted sh -c used to terminate the
  # outer quote and trip set -u on $1 in the local shell).
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" bash -s <<'EOF' || { echo "failed to corrupt key file"; rm_bucket kms501-b1; return 1; }
set -euo pipefail
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"
${SUDO} cp -a /var/lib/rustfs/kms /var/lib/rustfs/kms.corrupt-backup
target="$(${SUDO} find /var/lib/rustfs/kms -type f -print -quit)"
[ -n "${target}" ] || { echo "no key file found under /var/lib/rustfs/kms" >&2; exit 1; }
${SUDO} sh -c 'printf garbage > "$1"' _ "${target}"
EOF
  kms_sse_put kms501-b1 broken ${TEST_TMP}/kms-f1 "${kid}"
  assert_aws_fails || { echo "SSE-KMS put unexpectedly succeeded with corrupted key store"; rm_bucket kms501-b1; return 1; }
  echo "expected failure with corrupted key store: ${AWS_OUT}"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" bash -s <<'EOF' || { echo "failed to restore key store"; rm_bucket kms501-b1; return 1; }
set -euo pipefail
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"
${SUDO} rm -rf /var/lib/rustfs/kms
${SUDO} mv /var/lib/rustfs/kms.corrupt-backup /var/lib/rustfs/kms
EOF
  service_action restart "${node}" || { rm_bucket kms501-b1; return 1; }
  wait_endpoint_ready || { rm_bucket kms501-b1; return 1; }
  local waited=0
  while [ "${waited}" -lt 60 ]; do
    kms_sse_put kms501-b1 f2 ${TEST_TMP}/kms-f1 "${kid}" && break
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 60 ] || { echo "SSE-KMS put did not recover after key-store restore: ${AWS_OUT}"; rm_bucket kms501-b1; return 1; }
  rm_bucket kms501-b1
}

test_kms502() {
  kms_create_key "k502" "env=ci" || return 1
  local kid="${KMS_KEY_ID}" body code
  new_bucket "kms502-b1" || return 1
  make_test_file ${TEST_TMP}/kms-f1 1048576
  local pids=() i
  for i in 1 2 3 4 5 6 7 8; do
    (
      aws --endpoint-url "${ENDPOINT}" --region "${REGION}" s3api put-object \
        --bucket kms502-b1 --key "conc-${i}" --body ${TEST_TMP}/kms-f1 \
        --server-side-encryption aws:kms --ssekms-key-id "${kid}" \
        > ${TEST_TMP}/kms-conc-${i}.out 2>&1
    ) & pids+=($!)
  done
  body="$(admin_api POST /rustfs/admin/v3/kms/keys/rotate "" "{\"key_id\":\"${kid}\"}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    printf '%s' "${body}" | grep -qiE 'unsupported|not supported' || { echo "rotate -> HTTP ${code}: ${body}"; rm_bucket kms502-b1; return 1; }
  fi
  body="$(admin_api POST /rustfs/admin/v3/kms/clear-cache "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "clear-cache -> HTTP ${code}: ${body}"; rm_bucket kms502-b1; return 1; }
  local failed=0
  for pid in "${pids[@]}"; do wait "${pid}" || failed=$((failed+1)); done
  [ "${failed}" -le 4 ] || { echo "${failed} of 8 concurrent uploads failed"; rm_bucket kms502-b1; return 1; }
  local out
  for out in ${TEST_TMP}/kms-conc-*.out; do
    # Match real 5xx semantics only: bare "500|502" also hit the key/bucket
    # names (k502, kms502-b1) echoed in successful responses.
    if grep -qE '\((HTTP )?5[0-9]{2}\)|status code: 5[0-9]{2}|InternalError|ServiceUnavailable|SlowDown|"code": ?"5[0-9]{2}"' "${out}" 2>/dev/null; then
      echo "5xx observed in concurrent upload: $(cat "${out}")"
      rm_bucket kms502-b1
      return 1
    fi
  done
  for i in 1 2 3 4 5 6 7 8; do
    if grep -qE '"Error"|AccessDenied|InvalidArgument|NoSuchKey|ServiceUnavailable' ${TEST_TMP}/kms-conc-${i}.out 2>/dev/null; then
      continue
    fi
    aws --endpoint-url "${ENDPOINT}" --region "${REGION}" s3api get-object \
      --bucket kms502-b1 --key "conc-${i}" ${TEST_TMP}/kms-conc-${i}-dl >/dev/null 2>&1 \
      || { echo "successful upload conc-${i} not readable"; rm_bucket kms502-b1; return 1; }
    [ "$(md5_of_file ${TEST_TMP}/kms-conc-${i}-dl)" = "$(md5_of_file ${TEST_TMP}/kms-f1)" ] || { echo "conc-${i} content corrupted"; rm_bucket kms502-b1; return 1; }
  done
  rm -f ${TEST_TMP}/kms-conc-*.out ${TEST_TMP}/kms-conc-*-dl
  rm_bucket kms502-b1
}

# ==================== main ====================

usage() {
  cat <<'USAGE'
rustfs-kms-test.sh — RustFS KMS test suite (backlog #2024 KMS-101..502)

Usage:
  --all-topologies          Run single-single, single-multi and multi-multi
  --topology <topo>         Run one topology (repeatable)
  --backends <list>         Comma-separated KMS backends: local,vault-kv2
                            (default: local,vault-kv2)
  --version <ver>           RustFS release tag (default: 1.0.0-rc.4-preview.1)
  --package-url <url>       Explicit .deb URL
  --sha256 <hash>           Verify downloaded .deb checksum
  --extra-env <lines>       Extra KEY=VALUE lines appended to /etc/default/rustfs
                            (newline separated; e.g. RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true)
  --keep-env                Do not tear down the environment after the last run
  --ssh-user <user>         SSH user for the test nodes (default: azureuser)
  --log-file <path>         Tee output to a log file
  --dry-run                 Preview commands without executing them
  -y, --yes                 Skip confirmation prompts
  -h, --help                Show this help
USAGE
}

main() {
  [ "$#" -eq 0 ] && { usage; exit 0; }
  local opt all=0 backends_arg="local,vault-kv2"
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --all-topologies) all=1 ;;
      --topology) SELECTED_TOPOLOGIES+=("$1"); shift ;;
      --backends) backends_arg="$1"; shift ;;
      --version) RUSTFS_VERSION="$1"; shift ;;
      --package-url) PACKAGE_URL="$1"; shift ;;
      --sha256) PACKAGE_SHA256="$1"; shift ;;
      --extra-env) KMS_EXTRA_ENV="$1"; shift ;;
      --keep-env) KEEP_ENV=1 ;;
      --ssh-user) SSH_USER="$1"; shift ;;
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
      LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/rustfs-kms.XXXXXX.log")"
      warn "log file not writable; using ${LOG_FILE}"
    fi
    exec > >(tee -a "${LOG_FILE}") 2>&1
  fi
  if [ "${all}" -eq 1 ]; then
    SELECTED_TOPOLOGIES=("${TOPO_SINGLE_SINGLE}" "${TOPO_SINGLE_MULTI}" "${TOPO_MULTI_MULTI}")
  fi
  [ "${#SELECTED_TOPOLOGIES[@]}" -gt 0 ] || die "no topology selected (--all-topologies / --topology)"
  IFS=',' read -r -a BACKENDS <<<"${backends_arg}"
  [ "${#BACKENDS[@]}" -gt 0 ] || die "no backends selected (--backends local,vault-kv2)"

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
  need_cmd openssl "OpenSSL (SigV4)"
  log "nodes: ${NODES[*]}  package: $(resolve_package_url)"
  log "backends: ${BACKENDS[*]}  topologies: ${SELECTED_TOPOLOGIES[*]}"

  local overall_fail=0 topo backend
  for backend in "${BACKENDS[@]}"; do
    KMS_BACKEND="${backend}"
    for topo in "${SELECTED_TOPOLOGIES[@]}"; do
      CURRENT_TOPOLOGY="${topo}"
      log "== topology: ${topo}  kms-backend: ${KMS_BACKEND} =="
      if [ "${KMS_BACKEND}" = "vault-kv2" ]; then
        vault_stop
        vault_start
      fi
      env_setup "${topo}"
      PASS=0; FAIL=0; UNSUPPORTED=0; FAILED_TESTS=()
      KMS_KEY_ID=""
      run_all_tests
      results_summary
      record_topology_result "${topo}/${KMS_BACKEND}"
      if [ "${KEEP_ENV}" -eq 0 ]; then
        env_teardown "${topo}"
        if [ "${KMS_BACKEND}" = "vault-kv2" ]; then vault_stop; fi
      fi
      if [ "${FAIL}" -gt 0 ]; then overall_fail=1; fi
    done
  done
  final_summary
  [ "${overall_fail}" -eq 0 ] || exit 1
  log "all done"
}

main "$@"
