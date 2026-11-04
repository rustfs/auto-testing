#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# rustfs-upgrade-test.sh
# RustFS upgrade compatibility test suite (UPG-*).
#
# Verifies that an in-place package upgrade preserves data, identity and
# configuration. Every topology runs the same cycle:
#
#   clean VMs -> install OLD deb -> start cluster -> seed golden state
#   -> snapshot -> upgrade in place to NEW deb (keep /data + /etc) -> verify
#   -> tear environment down
#
# Topologies (same as the S3/KMS/Tier suites):
#   1. single-single  (vm000, /data/rustfs1/mnmd)
#   2. single-multi   (vm000, /data/rustfs{1...4}/mnmd)
#   3. multi-multi    (vm000+vm001+vm002, 3x4)
#
# KMS backends mirror the KMS suite (local, vault-kv2); KMS keys created on
# the old version must stay usable after the upgrade.
#
# Usage:
#   ./rustfs-upgrade-test.sh --all-topologies -y \
#       --from-version 1.0.0-rc.3 --to-url <nightly-deb-url>
#   ./rustfs-upgrade-test.sh --topology single-single -y \
#       --from-url <old.deb> --to-url <new.deb>
#
# Environment:
#   RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY   admin credentials
#   RUSTFS_NODES / RUSTFS_SSH_USER          shared test VMs (default vm000..002 / azureuser)
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JQ_LIB="${SCRIPT_DIR}/scripts/lib"

TEST_TMP="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-upgrade.XXXXXX")"
trap 'rm -rf "${TEST_TMP}"' EXIT

# ==================== Configuration ====================
NODES=(vm000 vm001 vm002 vm003)
SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${SSH_PORT}")

FROM_VERSION="${FROM_VERSION:-1.0.0-rc.3}"
FROM_URL="${FROM_URL:-}"
TO_VERSION="${TO_VERSION:-}"
TO_URL="${TO_URL:-}"
ARCH="${RUSTFS_ARCH:-amd64}"
PACKAGES_DIR="/home/rustfs/packages"
PACKAGE_FILE="rustfs.deb"
FROM_SHA256="${FROM_SHA256:-}"
TO_SHA256="${TO_SHA256:-}"
TO_NIGHTLY_URL="${TO_NIGHTLY_URL:-https://dl.rustfs.com/artifacts/rustfs/packages/nightly/rustfs-nightly-latest.deb}"

ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
REGION="us-east-1"

KMS_LOCAL_MASTER_KEY="${RUSTFS_KMS_LOCAL_MASTER_KEY:-MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=}"
KMS_BACKUP_KEK="${RUSTFS_KMS_BACKUP_KEK:-QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU=}"
KMS_BACKUP_KEK_ID="${RUSTFS_KMS_BACKUP_KEK_ID:-backup-kek-1}"
KMS_DEFAULT_KEY_ID="kms-default"

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

heal_run() {
  if command -v docker >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then
      "$@"
    else
      # runner user without docker group: run the same command via sudo.
      # Pass "$@" verbatim (like the KMS suite) — the first word is already
      # the full binary path; prefixing `docker` here would execute
      # `docker docker run -d ...` and fail with "unknown shorthand flag".
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
ENDPOINT_MULTI_MULTI="http://rustfs-node1:9000"

VOLUMES_SINGLE_SINGLE="/data/rustfs1/mnmd"
VOLUMES_SINGLE_MULTI="/data/rustfs{1...4}/mnmd"
VOLUMES_MULTI_MULTI="http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd"

SERVICE_TIMEOUT=300

API_ENDPOINT="${ENDPOINT_SINGLE_SINGLE}"
SIGV4_REGION="${REGION}"
SIGV4_SERVICE="s3"
API_REQUEST_TIMEOUT=120
ADMIN_API_CODE_FILE="${TEST_TMP}/api-code"

GOLDEN_FILE="${TEST_TMP}/golden.tsv"

# ==================== Runtime options ====================
DRY_RUN=0
ASSUME_YES=0
KEEP_ENV=0
SELECTED_TOPOLOGIES=()
BACKENDS=()
KMS_BACKEND="local"
ENDPOINT=""
VOLUMES=""

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
  local tag="${1#v}" asset
  asset="${tag//-/.}"
  printf 'https://github.com/rustfs/rustfs/releases/download/%s/rustfs_%s_%s.deb' "${tag}" "${asset}" "${ARCH}"
}

resolve_from_url() {
  if [ -n "${FROM_URL}" ]; then printf '%s' "${FROM_URL}"; else build_package_url "${FROM_VERSION}"; fi
}

resolve_to_url() {
  if [ -n "${TO_URL}" ]; then printf '%s' "${TO_URL}";
  elif [ -n "${TO_VERSION}" ]; then build_package_url "${TO_VERSION}";
  else printf '%s' "${TO_NIGHTLY_URL}"; fi
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
  local script pids=() i=0 fail=0 pid
  script="$(cat)"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ssh to all nodes: ${NODES[*]}"
    return 0
  fi
  for node in "${NODES[@]}"; do
    ( printf '%s\n' "${script}" | ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' ) & pids[$i]=$!; i=$((i+1))
  done
  for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
  [ "${fail}" -eq 0 ] || die "remote command failed on one or more nodes"
}

service_action() {
  local action="$1" node="$2"
  log "${node}: systemctl ${action} ${RUSTFS_SERVICE}"
  [ "${DRY_RUN}" -eq 1 ] && return 0
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "if [ \"\$(id -u)\" -ne 0 ]; then sudo -n systemctl ${action} ${RUSTFS_SERVICE}; else systemctl ${action} ${RUSTFS_SERVICE}; fi" \
    || die "${node}: systemctl ${action} failed"
}

wait_service_active() {
  local node="$1" waited=0
  [ "${DRY_RUN}" -eq 1 ] && return 0
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "systemctl is-active --quiet ${RUSTFS_SERVICE}" 2>/dev/null; then
      log "${node}: ${RUSTFS_SERVICE} is active"; return 0
    fi
    sleep 5; waited=$((waited+5))
  done
  die "${node}: timed out waiting for ${RUSTFS_SERVICE} (${SERVICE_TIMEOUT}s)"
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
    printf '${SUDO} mkdir -p /var/lib/rustfs/kms /var/lib/rustfs/kms-backup\n'
    printf '${SUDO} chown -R %s:%s /var/lib/rustfs/kms /var/lib/rustfs/kms-backup\n' "${RUSTFS_USER}" "${RUSTFS_USER}"
  } | run_remote "${node}"
}

rustfs_config_body() {
  local volumes="$1"
  cat <<EOF
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES="${volumes}"
RUSTFS_SSE_S3_MASTER_KEY=QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU=
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

# ==================== Vault (docker on heal) ====================

vault_local() { command -v docker >/dev/null 2>&1; }

vault_remote_bash() {
  local script="$1"
  if vault_local; then bash -c "${script}"; else ssh "${HEAL_SSH_OPTS[@]}" "${HEAL_HOST}" 'bash -s' <<<"${script}"; fi
}

vault_ensure_config() {
  # chmod 0777 like the KMS suite: the vault process inside the container
  # (running as its own uid) must be able to create /vault/data/core on this
  # host-mounted dir, or `vault operator init` fails with
  # "failed to persist keyring: mkdir /vault/data/core: permission denied".
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
      # Mirror the KMS suite: capture init output in a temp file and its
      # stderr separately (so a failure shows the real vault error instead
      # of swallowing it into /dev/null), then write init.json via
      # heal_run tee — VAULT_DATA_DIR may not be writable by the runner
      # user directly, but heal_run's sudo path is.
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

vault_stop() {
  [ "${DRY_RUN}" -eq 1 ] && { log "DRY-RUN: remove vault container on ${HEAL_HOST}"; return 0; }
  log "stopping and removing vault container ${VAULT_CONTAINER} on ${HEAL_HOST}"
  heal_run docker rm -f "${VAULT_CONTAINER}" >/dev/null 2>&1 || true
  # rm through heal_run: vault creates core/sys/logical with restrictive
  # modes inside the data dir, so the runner user cannot remove them
  # directly (rm exits 1 under set -e and kills the suite mid-loop).
  if vault_local; then
    heal_run rm -rf "${VAULT_DATA_DIR}" "${VAULT_CONFIG_DIR}"
  else
    vault_remote_bash "rm -rf '${VAULT_DATA_DIR}' '${VAULT_CONFIG_DIR}'"
  fi
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

wait_cluster_ready() {
  # Liveness only proves that a process is running. Upgrade reads need every
  # participating node to have joined the cluster and passed its read quorum
  # checks before the endpoint is considered usable.
  local nodes=("$@") waited=0 node all_ready
  [ "${DRY_RUN}" -eq 1 ] && return 0
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    all_ready=1
    for node in "${nodes[@]}"; do
      if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
          "curl -fsS --max-time 5 http://127.0.0.1:9000/minio/health/ready >/dev/null" 2>/dev/null; then
        all_ready=0
        break
      fi
    done
    if [ "${all_ready}" -eq 1 ] && curl -fsS --max-time 5 "${ENDPOINT}/minio/health/ready" >/dev/null 2>&1; then
      log "S3 cluster ready: ${ENDPOINT} (nodes: ${nodes[*]})"
      return 0
    fi
    sleep 5; waited=$((waited+5))
  done
  return 1
}

node_rustfs_version() {
  # Print the version string reported by the installed rustfs binary on a
  # node, e.g. "rustfs 1.0.0-rc.3 (built ...)" -> "1.0.0-rc.3". Falls back to
  # the dpkg package version when the binary is missing (e.g. installed but
  # service not on PATH over ssh).
  local node="$1" out
  out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" '
    SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"
    if command -v rustfs >/dev/null 2>&1; then
      rustfs --version 2>/dev/null || true
    elif [ -x /usr/local/bin/rustfs ]; then
      /usr/local/bin/rustfs --version 2>/dev/null || true
    elif [ -x /opt/rustfs/bin/rustfs ]; then
      /opt/rustfs/bin/rustfs --version 2>/dev/null || true
    fi
  ' 2>/dev/null || true)"
  if [ -z "${out}" ]; then
    out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      "dpkg-query -W -f='\${Version}' ${RUSTFS_PACKAGE_NAME} 2>/dev/null" 2>/dev/null || true)"
  fi
  [ -n "${out}" ] || out="unknown"
  out="$(printf '%s' "${out}" | head -1)"
  # "rustfs 1.0.0-rc.3 (built ...)" -> "1.0.0-rc.3"; keep the line as-is
  # for any other shape.
  if [[ "${out}" == rustfs\ * ]]; then
    out="$(printf '%s' "${out}" | awk '{print $2}')"
    [ -n "${out}" ] || out="unknown"
  fi
  printf '%s' "${out}"
}

install_deb_on_nodes() {
  # usage: install_deb_on_nodes "URL" [SHA256] nodes...
  local url="$1" sha="${2:-}"; shift 2
  local nodes=("$@") node
  for node in "${nodes[@]}"; do
    {
      printf 'set -euo pipefail\n'
      printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
      printf '${SUDO} mkdir -p %s\n' "${PACKAGES_DIR}"
      printf 'PKG_TMP="$(mktemp /tmp/rustfs-upgrade-pkg.XXXXXX.deb)"\n'
      printf 'curl -fSL --retry 3 -o "${PKG_TMP}" "%s"\n' "${url}"
      printf '${SUDO} install -m 0644 "${PKG_TMP}" "%s/%s"\n' "${PACKAGES_DIR}" "${PACKAGE_FILE}"
      printf 'rm -f "${PKG_TMP}"\n'
      if [ -n "${sha}" ]; then
        printf 'echo "%s  %s/%s" | sha256sum -c - || exit 1\n' "${sha}" "${PACKAGES_DIR}" "${PACKAGE_FILE}"
      fi
      printf '${SUDO} dpkg -i --force-confold "%s/%s"\n' "${PACKAGES_DIR}" "${PACKAGE_FILE}"
      printf '${SUDO} systemctl daemon-reload\n'
    } | run_remote "${node}"
  done
}

env_setup_old() {
  local topo="$1"
  resolve_topology "${topo}"
  read -r -a nodes <<<"$(topology_nodes "${topo}")"
  local from_url
  from_url="$(resolve_from_url)"
  log "environment setup (OLD): topology=${topo} nodes=${nodes[*]} endpoint=${ENDPOINT} kms=${KMS_BACKEND}"
  API_ENDPOINT="${ENDPOINT}"

  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
  install_deb_on_nodes "${from_url}" "${FROM_SHA256}" "${nodes[@]}"

  for node in "${nodes[@]}"; do
    write_rustfs_config "${node}" "${VOLUMES}"
    ensure_volume_dirs "${node}" "${VOLUMES}"
  done

  start_and_wait_nodes "${nodes[@]}"
  wait_cluster_ready "${nodes[@]}" || die "OLD cluster ${ENDPOINT} not ready"

  OLD_VERSION_ACTUAL="$(node_rustfs_version "${nodes[0]}")"
  log "OLD version in place: ${OLD_VERSION_ACTUAL}"

  export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
  export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
  export AWS_DEFAULT_REGION="${REGION}"
  export AWS_EC2_METADATA_DISABLED=true
  rc alias set local "${ENDPOINT}" "${ACCESS_KEY}" "${SECRET_KEY}" >/dev/null 2>&1 \
    || die "rc alias set failed for ${ENDPOINT}"
  if rc admin kms key create local --name "${KMS_DEFAULT_KEY_ID}" --json >/dev/null 2>&1; then
    log "created default KMS key ${KMS_DEFAULT_KEY_ID}"
  else
    log "default KMS key ${KMS_DEFAULT_KEY_ID} already present or create skipped"
  fi
  if ! rc admin kms key status local "${KMS_DEFAULT_KEY_ID}" --json >/dev/null 2>&1; then
    die "default KMS key ${KMS_DEFAULT_KEY_ID} is not usable after setup"
  fi
}

upgrade_in_place() {
  local topo="$1"
  read -r -a nodes <<<"$(topology_nodes "${topo}")"
  local to_url
  to_url="$(resolve_to_url)"
  log "upgrading in place to ${to_url} on ${nodes[*]} (keeping /data and ${RUSTFS_CONFIG_FILE})"
  install_deb_on_nodes "${to_url}" "${TO_SHA256}" "${nodes[@]}"
  # dpkg postinst usually restarts the service; make sure it is up and healthy.
  start_and_wait_nodes "${nodes[@]}"
  wait_cluster_ready "${nodes[@]}" || die "NEW cluster ${ENDPOINT} not ready after upgrade"

  NEW_VERSION_ACTUAL="$(node_rustfs_version "${nodes[0]}")"
  log "NEW version in place: ${NEW_VERSION_ACTUAL}"
  if [ "${OLD_VERSION_ACTUAL}" = "${NEW_VERSION_ACTUAL}" ]; then
    warn "version reported by rustfs --version did not change across the upgrade (${OLD_VERSION_ACTUAL})"
  fi
  printf '\n=== UPGRADE MATRIX ===\n'
  printf 'old_version: %s\n' "${OLD_VERSION_ACTUAL}"
  printf 'new_version: %s\n' "${NEW_VERSION_ACTUAL}"
}

env_teardown() {
  local topo="$1"
  log "environment teardown: ${topo} (kms=${KMS_BACKEND})"
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
}

# ==================== Admin API (SigV4, optional JSON body) ====================

sha256_hex() { printf '%s' "$1" | openssl dgst -sha256 -hex 2>/dev/null | awk '{print $NF}'; }
hmac_sha256_hex() { printf '%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" -hex 2>/dev/null | awk '{print $NF}'; }
hex_of_ascii() { printf '%s' "$1" | od -An -vtx1 | tr -d ' \n'; }

# SigV4 URI-encode one query name or value: keep RFC 3986 unreserved
# characters, percent-encode every other byte as uppercase %XX.
sigv4_uri_encode() {
  local LC_ALL=C
  local s="$1" out="" i c
  for (( i = 0; i < ${#s}; i++ )); do
    c="${s:i:1}"
    case "${c}" in
      [A-Za-z0-9_.~-]) out+="${c}" ;;
      *) out+="$(printf '%%%02X' "$(( $(printf '%d' "'${c}") & 255 ))")" ;;
    esac
  done
  printf '%s' "${out}"
}

canonical_query() {
  local q="$1"
  [ -z "${q}" ] && return 0
  # The server percent-decodes the raw query and re-encodes every pair for
  # the canonical request (name and value, RFC 3986 unreserved only), and a
  # valueless param (?flag) comes back as "flag=". Callers pass raw values,
  # so encode here and send the same encoded string on the wire; anything
  # signed verbatim (e.g. "@" in an access key, ":" in an ARN) fails with
  # SignatureDoesNotMatch.
  local pairs=() pair name value
  IFS='&' read -r -a pairs <<< "${q}"
  for pair in "${pairs[@]}"; do
    [ -n "${pair}" ] || continue
    name="${pair%%=*}"
    if [ "${name}" = "${pair}" ]; then value=""; else value="${pair#*=}"; fi
    printf '%s=%s\n' "$(sigv4_uri_encode "${name}")" "$(sigv4_uri_encode "${value}")"
  done | LC_ALL=C sort | paste -sd '&' -
}

admin_api() {
  local method="$1" path="$2" query="${3:-}" body="${4:-}"
  local amz_date date_stamp host_port
  local canonical_headers signed_headers canonical_request string_to_sign
  local scope k_date k_region k_service k_signing signature auth
  local url tmp code

  if [ "${DRY_RUN}" -eq 1 ]; then
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

  # The S3-compatible admin channel on these builds accepts only the
  # UNSIGNED-PAYLOAD signing form (matching the KMS suite); body-hash signing
  # is rejected with SignatureDoesNotMatch on some versions.
  local payload_hash="UNSIGNED-PAYLOAD"

  host_port="${API_ENDPOINT#*://}"
  host_port="${host_port%%/*}"
  amz_date="$(date -u +%Y%m%dT%H%M%SZ)"
  date_stamp="${amz_date:0:8}"
  query="$(canonical_query "${query}")"

  canonical_headers="host:${host_port}
x-amz-content-sha256:${payload_hash}
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
${payload_hash}"

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
  local curl_args=()
  [ -n "${body}" ] && curl_args+=(--data-binary "${body}" -H 'Content-Type: application/json')
  code="$(curl -sS --max-time "${API_REQUEST_TIMEOUT}" -o "${tmp}" -w '%{http_code}' \
    -H "Host: ${host_port}" \
    -H "x-amz-content-sha256: ${payload_hash}" \
    -H "x-amz-date: ${amz_date}" \
    -H "Authorization: ${auth}" \
    "${curl_args[@]}" \
    -X "${method}" "${url}")" || code="000"
  printf '%s' "${code}" > "${ADMIN_API_CODE_FILE}"
  cat "${tmp}"
  rm -f "${tmp}"
}

admin_api_code() { cat "${ADMIN_API_CODE_FILE}"; }

assert_admin_ok() {
  local expected="$1" context="$2" code
  code="$(admin_api_code)"
  [ "${code}" = "${expected}" ] || {
    echo "admin ${context}: expected HTTP ${expected}, got ${code}" >&2
    return 1
  }
}

# ==================== AWS / S3 helpers ====================

aws_run() {
  # The if-context suppresses `set -e` so a failing aws CLI cannot silently
  # kill the suite; the exit code is captured explicitly.
  if AWS_OUT="$(aws --endpoint-url "${ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; then
    AWS_CODE=0
  else
    AWS_CODE=$?
  fi
}

aws_run_as() {
  # usage: aws_run_as AK SK [TOKEN] -- args...
  local ak="$1" sk="$2" token=""
  shift 2
  if [ "${1:-}" != "--" ]; then token="$1"; shift; fi
  [ "${1:-}" = "--" ] && shift
  local env_args=(AWS_ACCESS_KEY_ID="${ak}" AWS_SECRET_ACCESS_KEY="${sk}" AWS_EC2_METADATA_DISABLED=true AWS_DEFAULT_REGION="${REGION}")
  if [ -n "${token}" ]; then env_args+=(AWS_SESSION_TOKEN="${token}"); fi
  if AWS_OUT="$(env "${env_args[@]}" aws --endpoint-url "${ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; then
    AWS_CODE=0
  else
    AWS_CODE=$?
  fi
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

golden_set() {
  # golden_set KEY VALUE
  printf '%s\t%s\n' "$1" "$2" >> "${GOLDEN_FILE}"
}

golden_get() {
  # golden_get KEY ; prints value
  awk -F '\t' -v k="$1" '$1 == k { print $2; exit }' "${GOLDEN_FILE}"
}

seed_warp_bucket() {
  # seed_warp_bucket <bucket> <obj-size> <duration> <keep-objects>
  # Fill a bucket with warp at a fixed object size. warp is duration-driven
  # (no object-count flag) and small sizes write thousands of objects per
  # second — a 10s 1KiB run produced 22k objects and the per-object post-
  # upgrade verification then needed 5h per topology. So run warp only long
  # enough to prove warp semantics (multi-threaded PUTs with warp-chosen
  # keys/content), then trim the bucket down to <keep-objects> and record
  # that exact count as golden state; verification lists and compares counts.
  local bucket="$1" size="$2" duration="$3" keep="$4"
  new_bucket "${bucket}" || die "seed: ${bucket}"
  log "seeding ${bucket} via warp (obj.size=${size}, duration=${duration}, keep=${keep})"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: warp put skipped"
    golden_set "warp:${bucket}" "0"
    return 0
  fi
  warp put --host "${ENDPOINT#http://}" \
    --bucket "${bucket}" \
    --access-key "${ACCESS_KEY}" \
    --secret-key "${SECRET_KEY}" \
    --obj.size "${size}" \
    --duration "${duration}" \
    --autoterm --autoterm.dur 10s \
    --concurrent 4 \
    --noprefix --noclear >"${TEST_TMP}/warp-put-${bucket}.log" 2>&1 \
    || die "seed: warp put ${bucket} failed (see ${TEST_TMP}/warp-put-${bucket}.log)"
  aws_run s3api list-objects-v2 --bucket "${bucket}" --query 'Contents[].Key' --output json
  assert_aws_ok || die "seed: list ${bucket}"
  local keys_json written remaining
  keys_json="${AWS_OUT}"
  written="$(printf '%s' "${keys_json}" | jq 'length')"
  [ "${written}" -gt 0 ] \
    || die "seed: ${bucket}: warp wrote 0 objects"
  if [ "${written}" -gt "${keep}" ]; then
    # Trim to <keep> objects: delete the extras in batches of 1000 (the S3
    # delete-objects limit). jq builds one compact JSON payload per batch.
    while IFS= read -r payload; do
      [ -n "${payload}" ] || continue
      aws_run s3api delete-objects --bucket "${bucket}" --delete "${payload}"
      assert_aws_ok || die "seed: trim ${bucket} (delete batch failed)"
    done < <(printf '%s' "${keys_json}" \
             | jq -c --argjson k "${keep}" -L "${JQ_LIB}" \
               'include "jq_nwise"; .[0:(length - $k)] | nwise(1000) | {Objects: [.[] | {Key: .}], Quiet: true}')
    aws_run s3api list-objects-v2 --bucket "${bucket}" --query 'length(Contents)'
    assert_aws_ok || die "seed: post-trim list ${bucket}"
    remaining="$(printf '%s' "${AWS_OUT}" | tr -d '[:space:]')"
    [ "${remaining}" = "${keep}" ] \
      || die "seed: ${bucket}: trim left ${remaining} objects (wanted exactly ${keep})"
    golden_set "warp:${bucket}" "${keep}"
    log "seeded ${bucket}: ${keep} objects of ${size} bytes (warp wrote ${written}, trimmed)"
  else
    golden_set "warp:${bucket}" "${written}"
    log "seeded ${bucket}: ${written} objects of ${size} bytes (warp wrote fewer than keep=${keep}, kept all)"
  fi
}

# ==================== Golden seed (on OLD version) ====================

seed_golden() {
  : > "${GOLDEN_FILE}"
  log "seeding golden state on old version (kms=${KMS_BACKEND})"

  # KMS keys created on the old version must survive the upgrade.
  if rc admin kms key create local --name "upg-key" --json >/dev/null 2>&1; then
    log "seeded KMS key upg-key"
  else
    warn "KMS key create returned non-zero (proceeding; key may pre-exist)"
  fi
  local upg_kid
  upg_kid="$(rc admin kms key status local upg-key --json 2>/dev/null | jq -r '.key_id // .data.key_id // empty' 2>/dev/null || true)"
  upg_kid="${upg_kid:-upg-key}"
  golden_set "kms:upg-key" "${upg_kid}"
  log "seeded KMS key id ${upg_kid}"

  # Data buckets.
  new_bucket upg-plain || die "seed: upg-plain"
  new_bucket upg-versioned || die "seed: upg-versioned"
  new_bucket upg-mp || die "seed: upg-mp"
  new_bucket upg-large || die "seed: upg-large"
  new_bucket upg-sse || die "seed: upg-sse"
  new_bucket upg-kms || die "seed: upg-kms"
  new_bucket upg-ctx || die "seed: upg-ctx"
  new_bucket upg-def || die "seed: upg-def"
  log "seeded buckets"

  aws_run s3api put-bucket-versioning --bucket upg-versioned --versioning-configuration Status=Enabled
  assert_aws_ok || die "seed: enable versioning"

  # UPG-101 plain object.
  make_test_file "${TEST_TMP}/plain.bin" 1048576
  aws_run s3api put-object --bucket upg-plain --key plain --body "${TEST_TMP}/plain.bin"
  assert_aws_ok || die "seed: plain put"

  # UPG-102 versioned object: two versions + delete marker.
  make_test_file "${TEST_TMP}/v1.bin" 65536
  make_test_file "${TEST_TMP}/v2.bin" 65536
  aws_run s3api put-object --bucket upg-versioned --key obj --body "${TEST_TMP}/v1.bin"
  assert_aws_ok || die "seed: version v1"
  local vid1 vid2
  vid1="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId' 2>/dev/null || true)"
  [ -n "${vid1}" ] || die "seed: no version id for v1: ${AWS_OUT}"
  aws_run s3api put-object --bucket upg-versioned --key obj --body "${TEST_TMP}/v2.bin"
  assert_aws_ok || die "seed: version v2"
  vid2="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId' 2>/dev/null || true)"
  [ -n "${vid2}" ] || die "seed: no version id for v2: ${AWS_OUT}"
  aws_run s3api delete-object --bucket upg-versioned --key obj
  assert_aws_ok || die "seed: delete marker"
  golden_set "versions:upg-versioned:obj" "${vid1}|${vid2}"
  log "seeded versioned object"

  # UPG-103 multipart (5MiB frame-aligned parts + small tail).
  make_test_file "${TEST_TMP}/mp.bin" 12582912
  aws_run s3api create-multipart-upload --bucket upg-mp --key mp
  assert_aws_ok || die "seed: mp create"
  local uid
  uid="$(printf '%s' "${AWS_OUT}" | jq -r '.UploadId' 2>/dev/null || true)"
  [ -n "${uid}" ] || die "seed: no UploadId: ${AWS_OUT}"
  split -b 5242880 "${TEST_TMP}/mp.bin" "${TEST_TMP}/mp-part."
  local etags=() i=0 p
  for p in "${TEST_TMP}"/mp-part.*; do
    i=$((i+1))
    aws_run s3api upload-part --bucket upg-mp --key mp --part-number "${i}" --upload-id "${uid}" --body "${p}"
    assert_aws_ok || die "seed: mp part ${i}"
    etags+=("{\"PartNumber\":${i},\"ETag\":\"$(printf '%s' "${AWS_OUT}" | jq -r '.ETag' | tr -d '"')\"}")
  done
  local parts
  parts="$(printf '%s,' "${etags[@]}")"; parts="${parts%,}"
  aws_run s3api complete-multipart-upload --bucket upg-mp --key mp --upload-id "${uid}" \
    --multipart-upload "{\"Parts\":[${parts}]}"
  assert_aws_ok || die "seed: mp complete"
  log "seeded multipart object"

  # UPG-104 large object.
  make_test_file "${TEST_TMP}/large.bin" 20971520
  aws_run s3api put-object --bucket upg-large --key large --body "${TEST_TMP}/large.bin"
  assert_aws_ok || die "seed: large put"
  log "seeded large object"

  # UPG-108/109/110 warp-written data at mixed object sizes. warp generates
  # its own content and key names; each bucket is trimmed to a handful of
  # objects after seeding (see seed_warp_bucket) so post-upgrade per-object
  # verification stays fast. The golden state records the final count per
  # bucket, and the verification side re-reads everything.
  seed_warp_bucket "upg-warp-1kib"  1KiB   2s 5
  seed_warp_bucket "upg-warp-1mib"  1MiB   2s 5
  seed_warp_bucket "upg-warp-100mib" 100MiB 30s 2

  # UPG-105 SSE-S3.
  make_test_file "${TEST_TMP}/sse3.bin" 65536
  aws_run s3api put-object --bucket upg-sse --key sse3 --body "${TEST_TMP}/sse3.bin" --server-side-encryption AES256
  assert_aws_ok || die "seed: SSE-S3 put"
  log "seeded SSE-S3 object"

  # UPG-106 SSE-KMS with a pre-upgrade key.
  make_test_file "${TEST_TMP}/ssek.bin" 65536
  aws_run s3api put-object --bucket upg-kms --key ssek --body "${TEST_TMP}/ssek.bin" \
    --server-side-encryption aws:kms --ssekms-key-id "${upg_kid:-upg-key}"
  assert_aws_ok || die "seed: SSE-KMS put"
  log "seeded SSE-KMS object"

  # UPG-107 encryption context.
  local ctx_b64
  ctx_b64="$(printf '%s' '{"purpose":"upgrade"}' | base64 | tr -d '\n')"
  aws_run s3api put-object --bucket upg-ctx --key ctx --body "${TEST_TMP}/ssek.bin" \
    --server-side-encryption aws:kms --ssekms-key-id "${upg_kid:-upg-key}" \
    --ssekms-encryption-context "${ctx_b64}"
  assert_aws_ok || die "seed: context put"
  log "seeded context object"

  # UPG-201 user with a scoped policy.
  local out code
  out="$(admin_api PUT "/rustfs/admin/v3/add-user?accessKey=upguser" "" "{\"secretKey\":\"upgsecret\",\"status\":\"enabled\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || die "seed: add-user upguser: ${out}"
  out="$(admin_api PUT "/rustfs/admin/v3/add-canned-policy?name=upgpolicy" "" "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::upg-plain\",\"arn:aws:s3:::upg-plain/*\"]}]}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || die "seed: add policy: ${out}"
  out="$(admin_api POST /rustfs/admin/v3/idp/builtin/policy/attach "" "{\"policies\":[\"upgpolicy\"],\"user\":\"upguser\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || die "seed: attach policy: ${out}"
  golden_set "creds:user" "upguser|upgsecret"
  log "seeded IAM user"

  # UPG-202 service account.
  out="$(admin_api PUT /rustfs/admin/v3/add-service-accounts "" "{\"targetUser\":\"${ACCESS_KEY}\"}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || die "seed: add service account: ${out}"
  local sa_ak sa_sk
  sa_ak="$(printf '%s' "${out}" | jq -r '.credentials.accessKey')"
  sa_sk="$(printf '%s' "${out}" | jq -r '.credentials.secretKey')"
  golden_set "creds:sa" "${sa_ak}|${sa_sk}"
  log "seeded service account"

  # UPG-203 STS credentials.
  aws_run sts assume-role --role-arn arn:aws:iam::123456789012:role/test \
    --role-session-name upgsts --duration-seconds 900
  if [ "${AWS_CODE}" -eq 0 ]; then
    local sts_ak sts_sk sts_tok
    sts_ak="$(printf '%s' "${AWS_OUT}" | jq -r '.Credentials.AccessKeyId')"
    sts_sk="$(printf '%s' "${AWS_OUT}" | jq -r '.Credentials.SecretAccessKey')"
    sts_tok="$(printf '%s' "${AWS_OUT}" | jq -r '.Credentials.SessionToken')"
    golden_set "creds:sts" "${sts_ak}|${sts_sk}|${sts_tok}"
    log "seeded STS credentials"
  fi

  # UPG-303 bucket default encryption.
  aws_run s3api put-bucket-encryption --bucket upg-def \
    --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"${upg_kid:-upg-key}\"}}]}"
  assert_aws_ok || die "seed: bucket default encryption"
  log "seeded bucket default encryption"

  log "golden state seeded (${GOLDEN_FILE})"
}

# ==================== Verification cases (on NEW version) ====================

run_test() {
  local id="$1" name="$2" fn="$3" out
  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '  [DRY-RUN] %s %s\n' "${id}" "${name}"
    return 0
  fi
  printf '\n--- %s %s ---\n' "${id}" "${name}"
  if "${fn}" > "${TEST_TMP}/${id}.out" 2>&1; then
    PASS=$((PASS+1)); printf '\033[1;32m[PASS]\033[0m %s %s\n' "${id}" "${name}"
  else
    FAIL=$((FAIL+1)); FAILED_TESTS+=("${id} ${name}")
    printf '\033[1;31m[FAIL]\033[0m %s %s\n' "${id}" "${name}"
    cp "${TEST_TMP}/${id}.out" "${TEST_TMP}/${id}.out.detail" 2>/dev/null || true
    sed 's/^/      | /' "${TEST_TMP}/${id}.out" | tail -50
  fi
}

verify_plain() {
  aws_run s3api get-object --bucket upg-plain --key plain "${TEST_TMP}/plain-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/plain-dl.bin")" = "$(md5_of_file "${TEST_TMP}/plain.bin")" ] || { echo "plain object mismatch after upgrade"; return 1; }
}

verify_versioned() {
  local versions vid1 vid2
  versions="$(golden_get "versions:upg-versioned:obj")"
  vid1="${versions%%|*}"; vid2="${versions#*|}"
  aws_run s3api get-object --bucket upg-versioned --key obj --version-id "${vid1}" "${TEST_TMP}/v1-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/v1-dl.bin")" = "$(md5_of_file "${TEST_TMP}/v1.bin")" ] || { echo "version v1 mismatch"; return 1; }
  aws_run s3api get-object --bucket upg-versioned --key obj --version-id "${vid2}" "${TEST_TMP}/v2-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/v2-dl.bin")" = "$(md5_of_file "${TEST_TMP}/v2.bin")" ] || { echo "version v2 mismatch"; return 1; }
}

verify_multipart() {
  aws_run s3api get-object --bucket upg-mp --key mp "${TEST_TMP}/mp-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/mp-dl.bin")" = "$(md5_of_file "${TEST_TMP}/mp.bin")" ] || { echo "multipart object mismatch"; return 1; }
}

verify_large() {
  aws_run s3api get-object --bucket upg-large --key large "${TEST_TMP}/large-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/large-dl.bin")" = "$(md5_of_file "${TEST_TMP}/large.bin")" ] || { echo "large object mismatch"; return 1; }
}

verify_sse_s3() {
  aws_run s3api head-object --bucket upg-sse --key sse3
  assert_aws_ok || return 1
  printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption": "AES256"' || { echo "SSE-S3 header lost: ${AWS_OUT}"; return 1; }
  aws_run s3api get-object --bucket upg-sse --key sse3 "${TEST_TMP}/sse3-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/sse3-dl.bin")" = "$(md5_of_file "${TEST_TMP}/sse3.bin")" ] || { echo "SSE-S3 object mismatch"; return 1; }
}

verify_sse_kms() {
  local kid
  kid="$(golden_get "kms:upg-key")"
  aws_run s3api get-object --bucket upg-kms --key ssek "${TEST_TMP}/ssek-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/ssek-dl.bin")" = "$(md5_of_file "${TEST_TMP}/ssek.bin")" ] || { echo "SSE-KMS object mismatch"; return 1; }
  # key still usable for new writes after upgrade.
  aws_run s3api put-object --bucket upg-kms --key new --body "${TEST_TMP}/ssek.bin" \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}"
  assert_aws_ok || { echo "SSE-KMS write with pre-upgrade key failed"; return 1; }
}

verify_context() {
  aws_run s3api get-object --bucket upg-ctx --key ctx "${TEST_TMP}/ctx-dl.bin"
  assert_aws_ok || return 1
  [ "$(md5_of_file "${TEST_TMP}/ctx-dl.bin")" = "$(md5_of_file "${TEST_TMP}/ssek.bin")" ] || { echo "context object mismatch"; return 1; }
}

verify_identity_user() {
  local creds user secret out code
  creds="$(golden_get "creds:user")"
  user="${creds%%|*}"; secret="${creds#*|}"
  aws_run_as "${user}" "${secret}" -- s3api get-object --bucket upg-plain --key plain "${TEST_TMP}/plain-user.bin"
  assert_aws_ok || { echo "pre-upgrade user credential failed after upgrade"; return 1; }
  aws_run_as "${user}" "${secret}" -- s3api put-object --bucket upg-plain --key denied --body "${TEST_TMP}/plain.bin"
  assert_aws_fails || { echo "pre-upgrade user policy boundary lost after upgrade"; return 1; }
  # The policy itself and its attachment must survive the upgrade.
  out="$(admin_api GET /rustfs/admin/v3/list-canned-policies "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "list-canned-policies -> HTTP ${code}: ${out}"; return 1; }
  printf '%s' "${out}" | jq -e 'has("upgpolicy")' >/dev/null \
    || { echo "pre-upgrade policy upgpolicy missing after upgrade: ${out}"; return 1; }
  out="$(admin_api GET "/rustfs/admin/v3/user-info?accessKey=${user}" "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "user-info -> HTTP ${code}: ${out}"; return 1; }
  printf '%s' "${out}" | grep -q "upgpolicy" \
    || { echo "policy upgpolicy no longer attached to ${user} after upgrade: ${out}"; return 1; }
}

verify_identity_sa() {
  local creds sa_ak sa_sk
  creds="$(golden_get "creds:sa")"
  sa_ak="${creds%%|*}"; sa_sk="${creds#*|}"
  aws_run_as "${sa_ak}" "${sa_sk}" -- s3 ls; assert_aws_ok || { echo "pre-upgrade service account failed after upgrade"; return 1; }
  local out code
  out="$(admin_api DELETE "/rustfs/admin/v3/delete-service-account?accessKey=${sa_ak}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "delete-service-account -> HTTP ${code}: ${out}"; return 1; }
  aws_run_as "${sa_ak}" "${sa_sk}" -- s3 ls; assert_aws_fails || { echo "deleted service account still works"; return 1; }
}

verify_identity_sts() {
  local creds
  creds="$(golden_get "creds:sts")"
  [ -n "${creds}" ] || { echo "no STS creds seeded; skipping"; return 0; }
  local sts_ak sts_sk sts_tok
  sts_ak="${creds%%|*}"; creds="${creds#*|}"
  sts_sk="${creds%%|*}"; sts_tok="${creds#*|}"
  aws_run_as "${sts_ak}" "${sts_sk}" "${sts_tok}" -- s3 ls
  if [ "${AWS_CODE}" -ne 0 ]; then
    printf '%s' "${AWS_OUT}" | grep -qiE 'AccessDenied|ExpiredToken|InvalidToken' \
      || { echo "STS credential failure after upgrade is unclear: ${AWS_OUT}"; return 1; }
  fi
}

verify_config_persisted() {
  local node="${NODES[0]}" out
  out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'grep -E "^(RUSTFS_ACCESS_KEY|RUSTFS_SECRET_KEY|RUSTFS_VOLUMES)=" /etc/default/rustfs' 2>/dev/null || true)"
  printf '%s' "${out}" | grep -q "RUSTFS_ACCESS_KEY=${ACCESS_KEY}" || { echo "/etc/default/rustfs lost access key"; return 1; }
  printf '%s' "${out}" | grep -q 'RUSTFS_VOLUMES=' || { echo "/etc/default/rustfs lost volumes"; return 1; }
}

verify_kms_config() {
  local body code
  body="$(admin_api GET /rustfs/admin/v3/kms/status "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "KMS status -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -qiE 'running|ready|ok|healthy' || { echo "KMS not running after upgrade: ${body}"; return 1; }
  rc_run admin kms key list local --json
  assert_rc_ok || return 1
  local kid
  kid="$(golden_get "kms:upg-key")"
  printf '%s' "${RC_OUT}" | grep -qF "${kid}" || { echo "pre-upgrade KMS key missing after upgrade: ${RC_OUT}"; return 1; }
}

verify_bucket_default_encryption() {
  make_test_file "${TEST_TMP}/def.bin" 65536
  aws_run s3api put-object --bucket upg-def --key bare --body "${TEST_TMP}/def.bin"
  assert_aws_ok || { echo "bare put under bucket default encryption failed after upgrade"; return 1; }
  aws_run s3api head-object --bucket upg-def --key bare
  printf '%s' "${AWS_OUT}" | grep -q '"ServerSideEncryption": "aws:kms"' || { echo "bucket default encryption lost after upgrade: ${AWS_OUT}"; return 1; }
}

verify_smoke_crud() {
  new_bucket "upg-smoke" || return 1
  aws_run s3api put-object --bucket upg-smoke --key hello.txt --body "${TEST_TMP}/plain.bin"
  assert_aws_ok || { echo "post-upgrade put failed"; rm_bucket upg-smoke; return 1; }
  aws_run s3api get-object --bucket upg-smoke --key hello.txt "${TEST_TMP}/smoke-dl.bin"
  assert_aws_ok || { echo "post-upgrade get failed"; rm_bucket upg-smoke; return 1; }
  [ "$(md5_of_file "${TEST_TMP}/smoke-dl.bin")" = "$(md5_of_file "${TEST_TMP}/plain.bin")" ] || { echo "post-upgrade CRUD mismatch"; rm_bucket upg-smoke; return 1; }
  rm_bucket upg-smoke
}

verify_smoke_kms_write() {
  local kid
  kid="$(golden_get "kms:upg-key")"
  aws_run s3api put-object --bucket upg-kms --key smoke2 --body "${TEST_TMP}/plain.bin" \
    --server-side-encryption aws:kms --ssekms-key-id "${kid}"
  assert_aws_ok || { echo "post-upgrade SSE-KMS write failed"; return 1; }
}

verify_warp_bucket() {
  # verify_warp_bucket <bucket> — after the upgrade, every warp-written
  # object must still be there (count matches the seeded golden value) and
  # readable with an intact ETag (head vs get ETag + full-body download
  # with S3 checksum validation on the aws CLI side).
  local bucket="$1" expected listed key etag_head etag_get
  expected="$(golden_get "warp:${bucket}")"
  [ -n "${expected}" ] || { echo "${bucket}: no seeded count in golden state"; return 1; }
  aws_run s3api list-objects-v2 --bucket "${bucket}" --query 'length(Contents)'
  assert_aws_ok || { echo "${bucket}: list failed after upgrade"; return 1; }
  listed="$(printf '%s' "${AWS_OUT}" | tr -d '[:space:]')"
  [ "${listed}" = "${expected}" ] \
    || { echo "${bucket}: object count changed across the upgrade: seeded=${expected} listed=${listed}"; return 1; }

  # Sample-read every object: full-body GET + ETag comparison between HEAD
  # and GET. Keys come from one JSON listing (jq emits one per line, with a
  # trailing newline), so the iteration cannot drop the last key.
  local keys_json key etag_head etag_get i=0
  aws_run s3api list-objects-v2 --bucket "${bucket}" --query 'Contents[].Key' --output json
  assert_aws_ok || { echo "${bucket}: key listing failed after upgrade"; return 1; }
  keys_json="${AWS_OUT}"
  while IFS= read -r key; do
    [ -n "${key}" ] || continue
    aws_run s3api head-object --bucket "${bucket}" --key "${key}"
    assert_aws_ok || { echo "${bucket}/${key}: HEAD failed after upgrade"; return 1; }
    etag_head="$(printf '%s' "${AWS_OUT}" | jq -r '.ETag')"
    aws_run s3api get-object --bucket "${bucket}" --key "${key}" "${TEST_TMP}/warp-dl.bin"
    assert_aws_ok || { echo "${bucket}/${key}: GET failed after upgrade"; return 1; }
    etag_get="$(printf '%s' "${AWS_OUT}" | jq -r '.ETag')"
    [ "${etag_head}" = "${etag_get}" ] \
      || { echo "${bucket}/${key}: ETag mismatch HEAD=${etag_head} GET=${etag_get}"; return 1; }
    i=$((i+1))
  done < <(printf '%s' "${keys_json}" | jq -r '.[]')
  log "${bucket}: ${i} objects readable after upgrade (count=${listed})"
}

verify_warp_1kib()  { verify_warp_bucket "upg-warp-1kib"; }
verify_warp_1mib()  { verify_warp_bucket "upg-warp-1mib"; }
verify_warp_100mib() { verify_warp_bucket "upg-warp-100mib"; }


# ==================== Harness / runner ====================

PASS=0; FAIL=0
FAILED_TESTS=()

emit_topo_result() {
  # Machine-readable line for the workflow report generator:
  # [UPG-TOPO] <topology> <backend> <old_version> <new_version> PASS=<n> FAIL=<m>
  local topo="$1" backend="$2"
  printf '[UPG-TOPO] %s %s %s %s PASS=%d FAIL=%d\n' \
    "${topo}" "${backend}" \
    "${OLD_VERSION_ACTUAL:-unknown}" "${NEW_VERSION_ACTUAL:-unknown}" \
    "${PASS}" "${FAIL}"
}

rc_run() {
  RC_OUT="$(rc "$@" 2>&1)"; RC_CODE=$?
  return "${RC_CODE}"
}

assert_rc_ok() {
  [ "${RC_CODE}" -eq 0 ] || { echo "rc failed (exit ${RC_CODE}): $*"; echo "${RC_OUT}"; return 1; }
}

run_verify_cases() {
  run_test UPG-101 "plain object preserved"          verify_plain
  run_test UPG-102 "versioned object preserved"      verify_versioned
  run_test UPG-103 "multipart object preserved"      verify_multipart
  run_test UPG-104 "large object preserved"          verify_large
  run_test UPG-105 "SSE-S3 object preserved"         verify_sse_s3
  run_test UPG-106 "SSE-KMS object + key preserved"  verify_sse_kms
  run_test UPG-107 "encryption-context object"       verify_context
  run_test UPG-201 "IAM user + policy survive"       verify_identity_user
  run_test UPG-202 "service account survives"        verify_identity_sa
  run_test UPG-203 "STS credential behavior"         verify_identity_sts
  run_test UPG-301 "node config persisted"           verify_config_persisted
  run_test UPG-302 "KMS config + keys persisted"     verify_kms_config
  run_test UPG-303 "bucket default encryption"       verify_bucket_default_encryption
  run_test UPG-401 "post-upgrade S3 CRUD smoke"      verify_smoke_crud
  run_test UPG-402 "post-upgrade SSE-KMS write"      verify_smoke_kms_write
  run_test UPG-108 "warp 1KiB objects readable"      verify_warp_1kib
  run_test UPG-109 "warp 1MiB objects readable"      verify_warp_1mib
  run_test UPG-110 "warp 100MiB objects readable"    verify_warp_100mib
}

results_summary() {
  printf '\n========================================\n'
  printf 'RESULTS: PASS=%d FAIL=%d\n' "${PASS}" "${FAIL}"
  if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
    printf '\nFailed tests:\n'
    for t in "${FAILED_TESTS[@]}"; do printf '  - %s\n' "${t}"; done
  fi
}

TOPO_RESULTS=()

record_topology_result() {
  local label="$1"
  TOPO_RESULTS+=("${label}: PASS=${PASS} FAIL=${FAIL} | ${OLD_VERSION_ACTUAL:-unknown} -> ${NEW_VERSION_ACTUAL:-unknown}")
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

# ==================== main ====================

usage() {
  cat <<'USAGE'
rustfs-upgrade-test.sh — RustFS upgrade compatibility test suite (UPG-*)

Usage:
  --all-topologies          Run single-single, single-multi and multi-multi
  --topology <topo>         Run one topology (repeatable)
  --backends <list>         Comma-separated KMS backends: local,vault-kv2
                            (default: local,vault-kv2)
  --from-version <ver>      OLD RustFS release tag (default: 1.0.0-rc.3; must ship a .deb asset)
  --from-url <url>          OLD .deb URL (overrides --from-version)
  --to-version <ver>        NEW RustFS release tag (overrides --to-url default)
  --to-url <url>            NEW .deb URL (default: latest nightly deb)
  --sha256-from <hash>      Verify OLD .deb checksum
  --sha256-to <hash>        Verify NEW .deb checksum
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
      --from-version) FROM_VERSION="$1"; shift ;;
      --from-url) FROM_URL="$1"; shift ;;
      --to-version) TO_VERSION="$1"; shift ;;
      --to-url) TO_URL="$1"; shift ;;
      --sha256-from) FROM_SHA256="$1"; shift ;;
      --sha256-to) TO_SHA256="$1"; shift ;;
      --keep-env) KEEP_ENV=1 ;;
      --ssh-user) SSH_USER="$1"; shift ;;
      --log-file) LOG_FILE="$1"; shift ;;
      --dry-run) DRY_RUN=1 ;;
      -y|--yes) ASSUME_YES=1 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown option: ${opt} (see --help)" ;;
    esac
  done
  if [ -n "${LOG_FILE:-}" ]; then
    mkdir -p "$(dirname "${LOG_FILE}")"
    if ! touch "${LOG_FILE}" 2>/dev/null; then
      LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/rustfs-upgrade.XXXXXX.log")"
      warn "log file not writable; using ${LOG_FILE}"
    fi
    exec > >(tee "${LOG_FILE}") 2>&1
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
  need_cmd warp "warp benchmark tool (mixed-size data seeding)"
  need_cmd jq "JSON processor"
  need_cmd curl "HTTP client"
  need_cmd openssl "OpenSSL (SigV4)"
  log "nodes: ${NODES[*]}  from: $(resolve_from_url)  to: $(resolve_to_url)"
  log "backends: ${BACKENDS[*]}  topologies: ${SELECTED_TOPOLOGIES[*]}"

  confirm "Run upgrade compatibility suite (${SELECTED_TOPOLOGIES[*]}, ${BACKENDS[*]})?"

  local overall_fail=0 topo backend
  for backend in "${BACKENDS[@]}"; do
    KMS_BACKEND="${backend}"
    for topo in "${SELECTED_TOPOLOGIES[@]}"; do
      log "== topology: ${topo}  kms-backend: ${KMS_BACKEND} =="
      if [ "${KMS_BACKEND}" = "vault-kv2" ]; then
        vault_stop
        vault_start
      fi
      env_setup_old "${topo}"
      seed_golden
      upgrade_in_place "${topo}"
      PASS=0; FAIL=0; FAILED_TESTS=()
      run_verify_cases
      results_summary
      record_topology_result "${topo}/${KMS_BACKEND}"
      emit_topo_result "${topo}" "${KMS_BACKEND}"
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
