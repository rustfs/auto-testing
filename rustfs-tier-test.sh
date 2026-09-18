#!/usr/bin/env bash
#
# rustfs-tier-test.sh
# RustFS data tiering / event notification / security audit test suite
# (backlog #2024: TIER-101..112, EVT-101..108, AUD-101..107)
#
# Topologies (peer RustFS as the warm tier, both installed from the same deb):
#   1. single-single  (vm000 = source 1 disk, vm001 = target 1 disk)
#   2. single-multi   (vm000 = source 4 disks, vm001 = target 4 disks)
#
# External services on the admin host (heal):
#   - webhook receiver (python3 HTTP server on :18080, /hook + /audit paths)
#   - MQTT broker (mosquitto on :1883) with a subscriber on rustfs/events
#
# Each case has an explicit pass criterion and reports PASS / FAIL /
# UNSUPPORTED with error detail on failure.
#
# Usage:
#   ./rustfs-tier-test.sh --all-topologies -y --package-url <deb url>
#   ./rustfs-tier-test.sh --topology single-single -y --version 1.0.0-rc.4-preview.1
#
set -Eeuo pipefail

# Per-run temp dir (shared runners may leave stale /tmp files owned by other users).
# Tests may inject an isolated directory while sourcing this file.
if [ -n "${RUSTFS_TIER_TEST_TMP:-}" ]; then
  TEST_TMP="${RUSTFS_TIER_TEST_TMP}"
  TEST_TMP_OWNED=0
  mkdir -p "${TEST_TMP}"
else
  TEST_TMP="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-test.XXXXXX")"
  TEST_TMP_OWNED=1
fi

CASE_ACTIVE=0
CASE_CLEANUP_RUNNING=0
CASE_CLEANUP_DONE=0
LOG_CAPTURE_ACTIVE=0
LOG_CAPTURE_PID=""
LOG_CAPTURE_DIR=""
LOG_PIPE=""
LOG_CAPTURE_STOP_FILE=""
LOG_CAPTURE_OUTPUT_REDIRECTED=0
LOG_CAPTURE_ANCHOR_OPEN=0
LOG_CAPTURE_SAVED_FDS_OPEN=0
LOG_CAPTURE_PENDING_STATUS=0
LOG_CAPTURE_ANCHOR_FD=""
LOG_CAPTURE_STDOUT_FD=""
LOG_CAPTURE_STDERR_FD=""
LOG_CAPTURE_SUCCESS_TICKS=600
LOG_CAPTURE_FAILURE_TICKS=100

log_capture_phase() { :; }

next_free_log_fd() {
  local candidate
  for ((candidate=10; candidate<100; candidate++)); do
    if ! { : >&"${candidate}"; } 2>/dev/null; then
      printf '%s' "${candidate}"
      return 0
    fi
  done
  return 1
}

close_log_fd() {
  local descriptor="$1"
  [ -n "${descriptor}" ] || return 0
  case "${descriptor}" in
    *[!0-9]*) return 1 ;;
  esac
  eval "exec ${descriptor}>&-"
}

log_capture_job_is_active() {
  local expected="$1" job_pid snapshot
  [ -n "${LOG_CAPTURE_DIR:-}" ] && [ -d "${LOG_CAPTURE_DIR}" ] || return 1
  snapshot="${LOG_CAPTURE_DIR}/jobs"
  # This must execute in the current shell: only it owns the authoritative job
  # table. Completed jobs disappear, while running and stopped jobs remain.
  jobs -p > "${snapshot}" || return 1
  while IFS= read -r job_pid; do
    [ "${job_pid}" = "${expected}" ] && return 0
  done < "${snapshot}"
  return 1
}

reap_log_process_if_done() {
  local pid="$1" process_status
  log_capture_job_is_active "${pid}" && return 124
  # If the OS still has this PID but Bash does not own it as an active job, do
  # not wait or signal: fail bounded instead of risking a recycled PID.
  kill -0 "${pid}" 2>/dev/null && return 125
  if wait "${pid}"; then process_status=0; else process_status=$?; fi
  return "${process_status}"
}

wait_for_log_process_exit_bounded() {
  local pid="$1" ticks="$2" i process_status
  for ((i=0; i<ticks; i++)); do
    if ! log_capture_job_is_active "${pid}"; then
      if reap_log_process_if_done "${pid}"; then process_status=0; else process_status=$?; fi
      return "${process_status}"
    fi
    sleep 0.05
  done
  return 124
}

abort_pending_log_start() {
  local pending_status="${LOG_CAPTURE_PENDING_STATUS:-0}"
  [ "${pending_status}" -ne 0 ] || return 0
  trap 'exit 130' INT
  trap 'exit 143' TERM
  if [ -n "${LOG_CAPTURE_PID:-}" ] \
    && log_capture_job_is_active "${LOG_CAPTURE_PID}"; then
    kill -CONT "${LOG_CAPTURE_PID}" 2>/dev/null || true
    kill -TERM "${LOG_CAPTURE_PID}" 2>/dev/null || true
  fi
  exit "${pending_status}"
}

request_log_capture_stop() {
  python3 -c '
import os, sys
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
flags |= getattr(os, "O_CLOEXEC", 0)
descriptor = os.open(sys.argv[1], flags, 0o600)
os.close(descriptor)
' "$1"
}

finish_log_process_bounded() {
  local pid="$1" suite_status="$2" stop_status="$3"
  local ticks="${LOG_CAPTURE_SUCCESS_TICKS}" i process_status
  [ "${suite_status}" -eq 0 ] || ticks="${LOG_CAPTURE_FAILURE_TICKS}"

  for ((i=0; i<ticks; i++)); do
    if ! log_capture_job_is_active "${pid}"; then
      if reap_log_process_if_done "${pid}"; then process_status=0; else process_status=$?; fi
      [ "${process_status}" -lt 124 ] || return 124
      [ "${process_status}" -ne 0 ] || process_status="${stop_status}"
      return "${process_status}"
    fi
    sleep 0.05
  done

  if ! log_capture_job_is_active "${pid}"; then
    if reap_log_process_if_done "${pid}"; then process_status=0; else process_status=$?; fi
    [ "${process_status}" -lt 124 ] || return 124
    [ "${process_status}" -ne 0 ] || process_status="${stop_status}"
    return "${process_status}"
  fi
  kill -CONT "${pid}" 2>/dev/null || true
  kill -TERM "${pid}" 2>/dev/null || true
  for ((i=0; i<20; i++)); do
    if ! log_capture_job_is_active "${pid}"; then
      reap_log_process_if_done "${pid}" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 0.05
  done
  log_capture_job_is_active "${pid}" || return 124
  kill -CONT "${pid}" 2>/dev/null || true
  kill -KILL "${pid}" 2>/dev/null || true
  for ((i=0; i<20; i++)); do
    if ! log_capture_job_is_active "${pid}"; then
      reap_log_process_if_done "${pid}" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 0.05
  done
  return 124
}

finish_log_capture() {
  local suite_status="${1:-0}" logger_status=0 stop_status=0
  if [ "${LOG_CAPTURE_ACTIVE:-0}" -eq 1 ]; then
    # Publish the stop request before closing the last shell-owned FIFO writer,
    # so the logger cannot observe EOF before it sees the authenticated request.
    if [ -n "${LOG_CAPTURE_PID:-}" ] && [ -n "${LOG_CAPTURE_STOP_FILE:-}" ]; then
      request_log_capture_stop "${LOG_CAPTURE_STOP_FILE}" || stop_status=74
    fi
    if [ "${LOG_CAPTURE_OUTPUT_REDIRECTED:-0}" -eq 1 ]; then
      exec 1>&- 2>&-
      LOG_CAPTURE_OUTPUT_REDIRECTED=0
    fi
    if [ -n "${LOG_CAPTURE_PID:-}" ] && [ -n "${LOG_CAPTURE_STOP_FILE:-}" ]; then
      if finish_log_process_bounded \
        "${LOG_CAPTURE_PID}" "${suite_status}" "${stop_status}"; then
        logger_status=0
      else
        logger_status=$?
      fi
    elif [ -n "${LOG_CAPTURE_PID:-}" ]; then
      logger_status=74
    fi
    if [ "${LOG_CAPTURE_ANCHOR_OPEN:-0}" -eq 1 ]; then
      close_log_fd "${LOG_CAPTURE_ANCHOR_FD}" || true
      LOG_CAPTURE_ANCHOR_OPEN=0
    fi
    if [ "${LOG_CAPTURE_SAVED_FDS_OPEN:-0}" -eq 1 ]; then
      close_log_fd "${LOG_CAPTURE_STDOUT_FD}" || true
      close_log_fd "${LOG_CAPTURE_STDERR_FD}" || true
      LOG_CAPTURE_SAVED_FDS_OPEN=0
    fi
    LOG_CAPTURE_ACTIVE=0
    if [ -n "${LOG_CAPTURE_DIR:-}" ] && [ -d "${LOG_CAPTURE_DIR}" ]; then
      rm -f -- "${LOG_CAPTURE_DIR}/stream" "${LOG_CAPTURE_DIR}/ready" \
        "${LOG_CAPTURE_DIR}/stop" "${LOG_CAPTURE_DIR}/jobs" \
        "${LOG_CAPTURE_DIR}"/ready.*.tmp 2>/dev/null || true
      rmdir -- "${LOG_CAPTURE_DIR}" 2>/dev/null || true
    fi
  fi

  if [ "${suite_status}" -ne 0 ]; then
    return "${suite_status}"
  fi
  return "${logger_status}"
}

suite_exit() {
  local exit_code="${1:-$?}"
  trap - EXIT INT TERM
  trap '' PIPE
  if [ "${CASE_ACTIVE:-0}" -eq 1 ] && declare -F case_cleanup >/dev/null 2>&1; then
    case_cleanup "signal-or-exit" >> "${TEST_TMP}/test.out" 2>&1 || true
  fi
  if finish_log_capture "${exit_code}"; then
    exit_code=0
  else
    exit_code=$?
  fi
  if [ "${TEST_TMP_OWNED:-0}" -eq 1 ] && [ -n "${TEST_TMP:-}" ] && [ -d "${TEST_TMP}" ]; then
    rm -rf -- "${TEST_TMP}"
  fi
  exit "${exit_code}"
}

trap 'suite_exit $?' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM


# ==================== Configuration ====================
NODES=(vm000 vm001 vm002 vm003)
SOURCE_NODE="${NODES[0]}"
TARGET_NODE="${NODES[1]}"
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

# Heal-local receiver + MQTT broker (reachable from vm000/001/002 at 10.0.0.4).
HEAL_IP="${RUSTFS_HEAL_IP:-10.0.0.4}"
RECEIVER_PORT="${RUSTFS_RECEIVER_PORT:-18080}"
NOTIFY_ENDPOINT="http://${HEAL_IP}:${RECEIVER_PORT}/hook"
AUDIT_ENDPOINT="http://${HEAL_IP}:${RECEIVER_PORT}/audit"
NOTIFY_TOKEN="notify-token"
AUDIT_TOKEN="audit-token"
MQTT_BROKER="tcp://${HEAL_IP}:1883"
MQTT_TOPIC="rustfs/events"
RECEIVER_SCRIPT="${TEST_TMP}/rustfs-receiver.py"
RECEIVER_PIDFILE="${TEST_TMP}/rustfs-receiver.pid"
MQTT_PIDFILE="${TEST_TMP}/rustfs-mqtt-sub.pid"
NOTIFY_EVENTS="${TEST_TMP}/rustfs-notify-events.jsonl"
AUDIT_EVENTS="${TEST_TMP}/rustfs-audit-events.jsonl"
MQTT_EVENTS="${TEST_TMP}/rustfs-mqtt-events.jsonl"

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

SOURCE_ENDPOINT_SINGLE_SINGLE="http://rustfs-node1:9000"
SOURCE_ENDPOINT_SINGLE_MULTI="http://rustfs-node1:9000"
TARGET_ENDPOINT="http://rustfs-node2:9000"

VOLUMES_SINGLE_SINGLE="/data/rustfs1/mnmd"
VOLUMES_SINGLE_MULTI="/data/rustfs{1...4}/mnmd"

SERVICE_TIMEOUT=300

TIER_NAME="WARM"
TIER_BUCKET="tier-warm"

API_ENDPOINT="${SOURCE_ENDPOINT_SINGLE_SINGLE}"
SIGV4_REGION="${REGION}"
SIGV4_SERVICE="s3"
API_REQUEST_TIMEOUT=120
COMMAND_TIMEOUT=360
CLEANUP_TIMEOUT=60
SOURCE_BUCKET_DELETE_TIMEOUT=30
SOURCE_BUCKET_DELETE_ATTEMPT_TIMEOUT=5
SOURCE_BUCKET_DELETE_RETRY_INTERVAL=2
REMOTE_PROBE_TIMEOUT=15
REMOTE_STATE_TIMEOUT=60
REMOTE_PROBE_INTERVAL=3
ADMIN_API_CODE_FILE="${TEST_TMP}/rustfs-tier-api-code"

# ==================== Runtime options ====================
DRY_RUN=0
ASSUME_YES=0
KEEP_ENV=0
LOG_FILE=""
SELECTED_TOPOLOGIES=()
ENDPOINT=""
VOLUMES=""
CURRENT_TOPOLOGY=""
CURRENT_CASE_ID=""
CURRENT_CASE_NAME=""
CASE_FAILURE_KIND=""
CASE_FAILURE_DETAIL=""
CASE_LOG_PATH=""
REMOTE_KEY_COUNT=""
AWS_OUT=""
AWS_CODE=0
RUN_TOKEN="$(date -u +%y%m%d%H%M%S)-$$"

RC_BIN_REQUEST="${RUSTFS_RC_BIN:-rc}"
RC_BIN=""
RC_SHA256=""
RC_VERSION=""
RC_CONFIG_DIR="${TEST_TMP}/rc-config"

ARTIFACTS_DIR="${RUSTFS_TIER_ARTIFACTS_DIR:-${TEST_TMP}/artifacts}"
RESULTS_DIR=""
RESULT_LOG_DIR=""
PROVENANCE_FILE=""
RESIDUE_FILE=""

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

start_log_capture() {
  local helper ready=0 i logger_status=0
  [ -n "${LOG_FILE}" ] || return 0
  command -v python3 >/dev/null 2>&1 || die "missing command: python3 (secure log capture); install it first"
  helper="$(python3 -c 'import os,sys; print(os.path.join(os.path.dirname(os.path.realpath(sys.argv[1])), "rustfs-tier-log-capture.py"))' "${BASH_SOURCE[0]}")"
  [ -f "${helper}" ] || die "missing secure log capture helper: ${helper}"

  # During initialization, remember INT/TERM and finish constructing a state
  # that suite_exit can always tear down. The pending status is delivered as
  # soon as the saved output descriptors have been closed.
  LOG_CAPTURE_PENDING_STATUS=0
  trap 'LOG_CAPTURE_PENDING_STATUS=130' INT
  trap 'LOG_CAPTURE_PENDING_STATUS=143' TERM
  log_capture_phase begin
  abort_pending_log_start

  if ! LOG_CAPTURE_DIR="$(mktemp -d "${TEST_TMP}/rustfs-tier-log.XXXXXX")"; then
    trap 'exit 130' INT
    trap 'exit 143' TERM
    if [ "${LOG_CAPTURE_PENDING_STATUS}" -ne 0 ]; then
      exit "${LOG_CAPTURE_PENDING_STATUS}"
    fi
    die "cannot create log capture state under ${TEST_TMP}"
  fi
  LOG_CAPTURE_ACTIVE=1
  log_capture_phase control-dir
  abort_pending_log_start
  chmod 700 "${LOG_CAPTURE_DIR}" || die "cannot secure log capture state: ${LOG_CAPTURE_DIR}"
  LOG_PIPE="${LOG_CAPTURE_DIR}/stream"
  LOG_CAPTURE_STOP_FILE="${LOG_CAPTURE_DIR}/stop"
  mkfifo -m 600 "${LOG_PIPE}" || die "cannot create the log synchronization pipe: ${LOG_PIPE}"
  log_capture_phase fifo
  abort_pending_log_start

  LOG_CAPTURE_ANCHOR_FD="$(next_free_log_fd)" || die "cannot reserve a descriptor for log capture"
  eval "exec ${LOG_CAPTURE_ANCHOR_FD}<>\"\${LOG_PIPE}\"" \
    || die "cannot open the log synchronization pipe"
  LOG_CAPTURE_ANCHOR_OPEN=1
  log_capture_phase anchor
  abort_pending_log_start
  LOG_CAPTURE_STDOUT_FD="$(next_free_log_fd)" || die "cannot reserve the original stdout"
  eval "exec ${LOG_CAPTURE_STDOUT_FD}>&1" || die "cannot preserve the original stdout"
  LOG_CAPTURE_STDERR_FD="$(next_free_log_fd)" || die "cannot reserve the original stderr"
  eval "exec ${LOG_CAPTURE_STDERR_FD}>&2" || die "cannot preserve the original stderr"
  LOG_CAPTURE_SAVED_FDS_OPEN=1
  log_capture_phase saved
  abort_pending_log_start
  log_capture_phase active
  abort_pending_log_start

  (
    close_log_fd "${LOG_CAPTURE_ANCHOR_FD}"
    close_log_fd "${LOG_CAPTURE_STDOUT_FD}"
    close_log_fd "${LOG_CAPTURE_STDERR_FD}"
    exec python3 -u "${helper}" \
      --log-file "${LOG_FILE}" \
      --status-file "${LOG_CAPTURE_DIR}/ready" \
      --stop-file "${LOG_CAPTURE_STOP_FILE}"
  ) < "${LOG_PIPE}" 1>&"${LOG_CAPTURE_STDOUT_FD}" 2>&"${LOG_CAPTURE_STDERR_FD}" &
  LOG_CAPTURE_PID=$!
  log_capture_phase launched
  abort_pending_log_start

  for ((i=0; i<100; i++)); do
    abort_pending_log_start
    if [ -s "${LOG_CAPTURE_DIR}/ready" ]; then
      ready=1
      break
    fi
    if ! log_capture_job_is_active "${LOG_CAPTURE_PID}"; then
      if reap_log_process_if_done "${LOG_CAPTURE_PID}"; then
        logger_status=0
      else
        logger_status=$?
      fi
      [ "${logger_status}" -eq 124 ] || LOG_CAPTURE_PID=""
      break
    fi
    sleep 0.05 || true
  done
  if [ "${ready}" -ne 1 ]; then
    trap 'exit 130' INT
    trap 'exit 143' TERM
    [ "${logger_status}" -ne 0 ] || logger_status=124
    die "secure log capture failed to initialize (status=${logger_status})"
  fi
  log_capture_phase ready
  abort_pending_log_start

  # Bash 3.2 can terminate immediately when a builtin writes to a dead FIFO,
  # bypassing EXIT. Convert SIGPIPE to an ordinary exit so suite_exit always
  # runs case cleanup and removes the capture state.
  trap 'exit 141' PIPE
  exec > "${LOG_PIPE}" 2>&1
  LOG_CAPTURE_OUTPUT_REDIRECTED=1
  close_log_fd "${LOG_CAPTURE_ANCHOR_FD}"
  LOG_CAPTURE_ANCHOR_OPEN=0

  # The current shell job table is the stable direct-child identity. Never call
  # wait while that job is running or stopped.
  if log_capture_job_is_active "${LOG_CAPTURE_PID}"; then
    logger_status=124
    kill -CONT "${LOG_CAPTURE_PID}" 2>/dev/null || true
  else
    if reap_log_process_if_done "${LOG_CAPTURE_PID}"; then
      logger_status=0
    else
      logger_status=$?
    fi
    [ "${logger_status}" -ne 0 ] && [ "${logger_status}" -ne 127 ] || logger_status=74
    LOG_CAPTURE_PID=""
    exec 1>&"${LOG_CAPTURE_STDOUT_FD}"
    exec 2>&"${LOG_CAPTURE_STDERR_FD}"
    LOG_CAPTURE_OUTPUT_REDIRECTED=0
    trap 'exit 130' INT
    trap 'exit 143' TERM
    printf '\033[1;31m[ERROR]\033[0m secure log capture exited during startup (status=%s)\n' \
      "${logger_status}" >&2
    exit "${logger_status}"
  fi
  log_capture_phase redirected
  abort_pending_log_start
  close_log_fd "${LOG_CAPTURE_STDOUT_FD}"
  close_log_fd "${LOG_CAPTURE_STDERR_FD}"
  LOG_CAPTURE_SAVED_FDS_OPEN=0
  log_capture_phase saved-closed
  abort_pending_log_start

  trap 'exit 130' INT
  trap 'exit 143' TERM
  if [ "${LOG_CAPTURE_PENDING_STATUS}" -ne 0 ]; then
    exit "${LOG_CAPTURE_PENDING_STATUS}"
  fi
}

sha256_file() {
  openssl dgst -sha256 -r "$1" 2>/dev/null | awk '{print $1}'
}

run_bounded() {
  local seconds="$1"
  shift
  timeout --signal=TERM --kill-after=5 "${seconds}" "$@"
}

run_hard_bounded() {
  local seconds="$1"
  shift
  timeout --signal=KILL "${seconds}" "$@"
}

bounded_safe_excerpt() {
  local value="$1" limit="${2:-512}"
  if { [ -n "${ACCESS_KEY:-}" ] && [[ "${value}" == *"${ACCESS_KEY}"* ]]; } \
    || { [ -n "${SECRET_KEY:-}" ] && [[ "${value}" == *"${SECRET_KEY}"* ]]; } \
    || printf '%s' "${value}" | grep -qiE 'authorization:|x-amz-credential|aws_access_key_id|aws_secret_access_key|accesskeyid|secretaccesskey'; then
    printf '<redacted: credential-like content>'
    return 0
  fi
  value="${value//$'\r'/ }"
  value="${value//$'\n'/ }"
  value="${value//$'\t'/ }"
  while [[ "${value}" == *"  "* ]]; do value="${value//  / }"; done
  if [ "${#value}" -gt "${limit}" ]; then
    printf '%s...<truncated>' "${value:0:${limit}}"
  else
    printf '%s' "${value}"
  fi
}

record_case_failure() {
  local kind="$1" detail="$2"
  [ -z "${CASE_FAILURE_KIND}" ] || return 0
  [[ "${kind}" =~ ^[a-z0-9_]+$ ]] || kind=unknown_failure
  CASE_FAILURE_KIND="${kind}"
  CASE_FAILURE_DETAIL="$(bounded_safe_excerpt "${detail}" 2048)"
  printf 'case failure: kind=%s detail=%s\n' "${CASE_FAILURE_KIND}" "${CASE_FAILURE_DETAIL}" >&2
}

init_artifacts() {
  RESULTS_DIR="${ARTIFACTS_DIR}/cases"
  RESULT_LOG_DIR="${ARTIFACTS_DIR}/logs"
  PROVENANCE_FILE="${ARTIFACTS_DIR}/provenance.json"
  RESIDUE_FILE="${ARTIFACTS_DIR}/residue.jsonl"
  mkdir -p "${RESULTS_DIR}" "${RESULT_LOG_DIR}"
  if find "${RESULTS_DIR}" -maxdepth 1 -type f -name '*.json' -print -quit | grep -q .; then
    die "artifacts case directory is not empty; use a unique --artifacts-dir: ${RESULTS_DIR}"
  fi
  : > "${RESIDUE_FILE}"
}

resolve_rc_binary() {
  local candidate
  if [[ "${RC_BIN_REQUEST}" == */* ]]; then
    candidate="${RC_BIN_REQUEST}"
  else
    candidate="$(command -v "${RC_BIN_REQUEST}" 2>/dev/null || true)"
  fi

  if [ "${DRY_RUN}" -eq 1 ] && [ -z "${candidate}" ]; then
    RC_BIN="${RC_BIN_REQUEST}"
    RC_VERSION="dry-run (binary not resolved)"
    RC_SHA256=""
  else
    [ -n "${candidate}" ] || die "missing command: ${RC_BIN_REQUEST} (RustFS client CLI)"
    [ -x "${candidate}" ] || die "rc binary is not executable: ${candidate}"
    RC_BIN="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${candidate}")"
    RC_SHA256="$(sha256_file "${RC_BIN}")"
    [ -n "${RC_SHA256}" ] || die "failed to calculate rc SHA-256: ${RC_BIN}"
    RC_VERSION="$(run_bounded 30 "${RC_BIN}" --version 2>&1 | head -1)" \
      || die "failed to read rc version from ${RC_BIN}"
  fi

  mkdir -p "${RC_CONFIG_DIR}"
  export RC_CONFIG_DIR
}

write_provenance() {
  jq -n \
    --arg schema_version "1" \
    --arg run_token "${RUN_TOKEN}" \
    --arg rc_path "${RC_BIN}" \
    --arg rc_version "${RC_VERSION}" \
    --arg rc_sha256 "${RC_SHA256}" \
    --arg rc_config_dir "${RC_CONFIG_DIR}" \
    --arg expected_rc_sha256 "${RUSTFS_EXPECTED_RC_SHA256:-}" \
    --arg package_url "$(resolve_package_url)" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{
      schema_version: ($schema_version | tonumber),
      run_token: $run_token,
      generated_at: $generated_at,
      package_url: $package_url,
      rc: {
        path: $rc_path,
        version: $rc_version,
        sha256: $rc_sha256,
        config_dir: $rc_config_dir,
        expected_sha256: $expected_rc_sha256
      }
    }' > "${PROVENANCE_FILE}"
}

verify_rc_digest() {
  local expected="${RUSTFS_EXPECTED_RC_SHA256:-}"
  [ -z "${expected}" ] && return 0
  [ "${DRY_RUN}" -eq 0 ] || die "RUSTFS_EXPECTED_RC_SHA256 cannot be verified in dry-run mode"
  expected="$(printf '%s' "${expected}" | tr '[:upper:]' '[:lower:]')"
  if [ "${RC_SHA256}" != "${expected}" ]; then
    die "rc SHA-256 mismatch: expected ${expected}, got ${RC_SHA256} (${RC_BIN})"
  fi
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

rustfs_config_body() {
  local volumes="$1"
  cat <<EOF
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES="${volumes}"
RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY=true
RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY=${NOTIFY_ENDPOINT}
RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN_PRIMARY=${NOTIFY_TOKEN}
RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY=/var/lib/rustfs/notify-webhook
RUSTFS_NOTIFY_MQTT_ENABLE_PRIMARY=true
RUSTFS_NOTIFY_MQTT_BROKER_PRIMARY=${MQTT_BROKER}
RUSTFS_NOTIFY_MQTT_TOPIC_PRIMARY=${MQTT_TOPIC}
RUSTFS_NOTIFY_MQTT_QOS_PRIMARY=1
RUSTFS_NOTIFY_MQTT_QUEUE_DIR_PRIMARY=/var/lib/rustfs/notify-mqtt
RUSTFS_AUDIT_WEBHOOK_ENABLE_PRIMARY=true
RUSTFS_AUDIT_WEBHOOK_ENDPOINT_PRIMARY=${AUDIT_ENDPOINT}
RUSTFS_AUDIT_WEBHOOK_AUTH_TOKEN_PRIMARY=${AUDIT_TOKEN}
RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_PRIMARY=/var/lib/rustfs/audit-webhook
RUSTFS_OUTBOUND_ALLOW_ORIGINS=http://${HEAL_IP}:${RECEIVER_PORT},http://${HEAL_IP}:19999,http://rustfs-node2:9000,http://10.0.0.5:9000
RUSTFS_KMS_ENABLE=true
RUSTFS_KMS_BACKEND=local
RUSTFS_KMS_KEY_DIR=/var/lib/rustfs/kms
RUSTFS_KMS_LOCAL_MASTER_KEY=MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=
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
    printf '${SUDO} rm -rf /var/log/rustfs /var/lib/rustfs/notify-webhook /var/lib/rustfs/notify-mqtt /var/lib/rustfs/audit-webhook /var/lib/rustfs/kms\n'
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
    printf '${SUDO} mkdir -p /var/lib/rustfs/notify-webhook /var/lib/rustfs/notify-mqtt /var/lib/rustfs/audit-webhook /var/lib/rustfs/kms\n'
    printf '${SUDO} chown -R %s:%s /var/lib/rustfs/notify-webhook /var/lib/rustfs/notify-mqtt /var/lib/rustfs/audit-webhook /var/lib/rustfs/kms\n' "${RUSTFS_USER}" "${RUSTFS_USER}"
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

admin_api() {
  local method="$1" path="$2" query="${3:-}" body="${4:-}"
  local amz_date date_stamp host_port
  local canonical_headers signed_headers canonical_request string_to_sign
  local scope k_date k_region k_service k_signing signature auth
  local url tmp code

  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: admin API ${method} ${API_ENDPOINT}${path}${query:+?${query}}${body:+ body=${body}}"
    printf '200' > "${ADMIN_API_CODE_FILE}"
    return 0
  fi

  local payload_hash="UNSIGNED-PAYLOAD"
  if [ -n "${body}" ]; then
    payload_hash="$(sha256_hex "${body}")"
  fi

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
  [ -n "${body}" ] && curl_args+=(--data-binary "${body}")
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

get_first_arn() {
  local raw arn
  raw="$(admin_api GET /rustfs/admin/v3/target/arns "")"
  # Prefer the webhook target so assertions run against the webhook receiver.
  arn="$(printf '%s' "${raw}" | jq -r '.[] | select(test("webhook"; "i")) | .' 2>/dev/null | head -1)"
  if [ -z "${arn}" ]; then
    arn="$(printf '%s' "${raw}" | jq -r '.[0] // empty' 2>/dev/null)"
  fi
  if [ -z "${arn}" ]; then
    echo "target/arns returned: ${raw}" >&2
  fi
  printf '%s' "${arn}"
}

# ==================== Webhook receiver + MQTT subscriber (heal-local) ====================

receiver_install() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  cat > "${RECEIVER_SCRIPT}" <<'PYEOF'
#!/usr/bin/env python3
import json, os, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NOTIFY = os.environ.get("RUSTFS_NOTIFY_EVENTS", "/tmp/rustfs-notify-events.jsonl")
AUDIT = os.environ.get("RUSTFS_AUDIT_EVENTS", "/tmp/rustfs-audit-events.jsonl")

class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def _record(self, path):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace") if length else ""
        if path == "/hook":
            with open(NOTIFY, "a") as f:
                f.write(body + "\n")
        elif path == "/audit":
            with open(AUDIT, "a") as f:
                f.write(body + "\n")
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()
    def do_POST(self):
        self._record(self.path)
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

port = int(sys.argv[1]) if len(sys.argv) > 1 else 18080
ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()
PYEOF
  chmod +x "${RECEIVER_SCRIPT}"
  : > "${NOTIFY_EVENTS}"
  : > "${AUDIT_EVENTS}"
  : > "${MQTT_EVENTS}"
}

receiver_start() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  receiver_stop
  RUSTFS_NOTIFY_EVENTS="${NOTIFY_EVENTS}" RUSTFS_AUDIT_EVENTS="${AUDIT_EVENTS}" \
    nohup python3 "${RECEIVER_SCRIPT}" "${RECEIVER_PORT}" >"${TEST_TMP}/rustfs-receiver.log" 2>&1 &
  echo $! > "${RECEIVER_PIDFILE}"
  local waited=0
  while [ "${waited}" -lt 30 ]; do
    if curl -sf --max-time 3 "http://127.0.0.1:${RECEIVER_PORT}/health" >/dev/null 2>&1; then
      log "webhook receiver up on :${RECEIVER_PORT}"
      return 0
    fi
    sleep 1; waited=$((waited+1))
  done
  die "webhook receiver failed to start"
}

receiver_stop() {
  [ -f "${RECEIVER_PIDFILE}" ] && kill "$(cat "${RECEIVER_PIDFILE}")" 2>/dev/null || true
  rm -f "${RECEIVER_PIDFILE}"
  pkill -f "rustfs-receiver.py" 2>/dev/null || true
  sleep 1
}

mqtt_start() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  mqtt_stop
  nohup mosquitto_sub -h "${HEAL_IP}" -p 1883 -t "${MQTT_TOPIC}" -v >> "${MQTT_EVENTS}" 2>/dev/null &
  echo $! > "${MQTT_PIDFILE}"
  sleep 2
  log "mqtt subscriber started (${MQTT_TOPIC})"
}

mqtt_stop() {
  [ -f "${MQTT_PIDFILE}" ] && kill "$(cat "${MQTT_PIDFILE}")" 2>/dev/null || true
  rm -f "${MQTT_PIDFILE}"
  pkill -f "mosquitto_sub" 2>/dev/null || true
  sleep 1
}

events_reset() {
  : > "${NOTIFY_EVENTS}"
  : > "${AUDIT_EVENTS}"
  : > "${MQTT_EVENTS}"
}

notify_count() { wc -l < "${NOTIFY_EVENTS}" 2>/dev/null || echo 0; }
audit_count()  { wc -l < "${AUDIT_EVENTS}" 2>/dev/null || echo 0; }
notify_grep()  { grep -E "$1" "${NOTIFY_EVENTS}" 2>/dev/null | head -20; }
audit_grep()   { grep -E "$1" "${AUDIT_EVENTS}" 2>/dev/null | head -20; }

# Wait until the notify log contains at least n lines matching the pattern.
wait_notify() {
  local n="$1" pattern="${2:-.}" waited=0
  while [ "${waited}" -lt 300 ]; do
    if [ "$(grep -cE "${pattern}" "${NOTIFY_EVENTS}" 2>/dev/null || true)" -ge "${n}" ]; then return 0; fi
    sleep 2; waited=$((waited+2))
  done
  echo "timed out waiting for notify events (want ${n} matching ${pattern}); have:"
  tail -5 "${NOTIFY_EVENTS}" 2>/dev/null | sed 's/^/    | /'
  return 1
}

wait_audit() {
  local n="$1" pattern="${2:-.}" waited=0
  while [ "${waited}" -lt 300 ]; do
    if [ "$(grep -cE "${pattern}" "${AUDIT_EVENTS}" 2>/dev/null || true)" -ge "${n}" ]; then return 0; fi
    sleep 2; waited=$((waited+2))
  done
  echo "timed out waiting for audit events (want ${n} matching ${pattern}); have:"
  tail -5 "${AUDIT_EVENTS}" 2>/dev/null | sed 's/^/    | /'
  return 1
}

# Wait for the notification runtime to converge (env targets online) so target
# mutation APIs stop returning "lifecycle generation has not converged".
wait_targets_ready() {
  local body code waited=0
  while [ "${waited}" -lt 120 ]; do
    body="$(admin_api GET /rustfs/admin/v3/target/list "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ]; then
      if printf '%s' "${body}" | grep -q '"status":"online"'; then
        log "notification targets online"
        return 0
      fi
    fi
    sleep 3; waited=$((waited+3))
  done
  echo "notification targets did not come online (last HTTP ${code}): ${body}"
  return 1
}

# ==================== Topology / environment ====================

resolve_topology() {
  case "$1" in
    "${TOPO_SINGLE_SINGLE}") ENDPOINT="${SOURCE_ENDPOINT_SINGLE_SINGLE}"; VOLUMES="${VOLUMES_SINGLE_SINGLE}"; return 0 ;;
    "${TOPO_SINGLE_MULTI}")  ENDPOINT="${SOURCE_ENDPOINT_SINGLE_MULTI}";  VOLUMES="${VOLUMES_SINGLE_MULTI}";  return 0 ;;
    *) die "unknown topology: $1 (single-single|single-multi)" ;;
  esac
}

env_setup() {
  local topo="$1" url script
  resolve_topology "${topo}"
  url="$(resolve_package_url)"
  log "environment setup: topology=${topo} source=${SOURCE_NODE} target=${TARGET_NODE} endpoint=${ENDPOINT}"
  API_ENDPOINT="${ENDPOINT}"

  for node in "${NODES[@]}"; do cleanup_node "${node}"; done

  # Download + install on source and target.
  for node in "${SOURCE_NODE}" "${TARGET_NODE}"; do
    script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
\${SUDO} mkdir -p "${PACKAGES_DIR}"
PKG_TMP="\$(mktemp /tmp/rustfs-tier-pkg.XXXXXX.deb)"
curl -fSL --retry 3 -o "\${PKG_TMP}" "${url}"
\${SUDO} install -m 0644 "\${PKG_TMP}" "${PACKAGES_DIR}/${PACKAGE_FILE}"
rm -f "\${PKG_TMP}"
if [ -n "${PACKAGE_SHA256}" ]; then
  echo "${PACKAGE_SHA256}  ${PACKAGES_DIR}/${PACKAGE_FILE}" | sha256sum -c - || exit 1
fi
\${SUDO} dpkg -i "${PACKAGES_DIR}/${PACKAGE_FILE}"
\${SUDO} systemctl daemon-reload
EOF
)"
    printf '%s\n' "${script}" | run_remote "${node}"
  done

  for node in "${SOURCE_NODE}" "${TARGET_NODE}"; do
    write_rustfs_config "${node}" "${VOLUMES}"
    ensure_volume_dirs "${node}" "${VOLUMES}"
  done

  start_and_wait_nodes "${SOURCE_NODE}" "${TARGET_NODE}"

  if [ "${DRY_RUN}" -eq 0 ]; then
    local waited=0
    while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
      if curl -fsS --max-time 5 "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        log "source endpoint ready: ${ENDPOINT}"; break
      fi
      sleep 5; waited=$((waited+5))
    done
    [ "${waited}" -lt "${SERVICE_TIMEOUT}" ] || die "source endpoint ${ENDPOINT} not ready"
    waited=0
    while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
      if curl -fsS --max-time 5 "${TARGET_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        log "target endpoint ready: ${TARGET_ENDPOINT}"; break
      fi
      sleep 5; waited=$((waited+5))
    done
    [ "${waited}" -lt "${SERVICE_TIMEOUT}" ] || die "target endpoint ${TARGET_ENDPOINT} not ready"

    run_bounded "${COMMAND_TIMEOUT}" "${RC_BIN}" alias set local "${ENDPOINT}" "${ACCESS_KEY}" "${SECRET_KEY}" >/dev/null 2>&1 \
      || die "rc alias set failed for ${ENDPOINT}"
    run_bounded "${COMMAND_TIMEOUT}" "${RC_BIN}" alias set target "${TARGET_ENDPOINT}" "${ACCESS_KEY}" "${SECRET_KEY}" >/dev/null 2>&1 \
      || die "rc alias set failed for ${TARGET_ENDPOINT}"
    export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
    export AWS_DEFAULT_REGION="${REGION}"
    export AWS_EC2_METADATA_DISABLED=true

    # Create the warm-tier bucket on the target instance.
    aws_target_run s3api create-bucket --bucket "${TIER_BUCKET}" \
      || die "failed to create unique warm-tier bucket ${TIER_BUCKET}: ${AWS_OUT}"
    receiver_start
    mqtt_start
    # Enable the notify + audit modules via the (API-managed) module switch so
    # EVT-108 / AUD-107 can toggle them later. Not env-managed.
    local mbody mcode
    mbody="$(admin_api PUT /rustfs/admin/v3/module-switches "" '{"notify_enabled":true,"audit_enabled":true}')"; mcode="$(admin_api_code)"
    [ "${mcode}" = "200" ] || { echo "enable modules -> HTTP ${mcode}: ${mbody}"; die "failed to enable notify/audit modules"; }
    wait_targets_ready
    log "rc aliases local/target; warm bucket ${TIER_BUCKET} ready"
  fi
}

env_teardown() {
  local topo="$1"
  log "environment teardown: ${topo}"
  receiver_stop
  mqtt_stop
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
}

# ==================== Test harness ====================

PASS=0; FAIL=0; UNSUPPORTED=0
FAILED_TESTS=()
CASE_SOURCE_BUCKETS=()
CASE_REMOTE_BUCKETS=()
CASE_TIERS=()
CASE_CLEANUP_ERRORS=()

case_resources_reset() {
  CASE_SOURCE_BUCKETS=()
  CASE_REMOTE_BUCKETS=()
  CASE_TIERS=()
  CASE_CLEANUP_ERRORS=()
  CASE_FAILURE_KIND=""
  CASE_FAILURE_DETAIL=""
  CASE_LOG_PATH=""
  REMOTE_KEY_COUNT=""
  CASE_CLEANUP_RUNNING=0
  CASE_CLEANUP_DONE=0
  CASE_ACTIVE=1
}

register_source_bucket() {
  local bucket="$1" item
  [[ "${bucket}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] \
    || { echo "unsafe source bucket name: ${bucket}" >&2; return 1; }
  if [ "${#CASE_SOURCE_BUCKETS[@]}" -gt 0 ]; then
    for item in "${CASE_SOURCE_BUCKETS[@]}"; do [ "${item}" = "${bucket}" ] && return 0; done
  fi
  CASE_SOURCE_BUCKETS+=("${bucket}")
}

register_remote_bucket() {
  local bucket="$1" item
  [[ "${bucket}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] \
    || { echo "unsafe remote bucket name: ${bucket}" >&2; return 1; }
  if [ "${#CASE_REMOTE_BUCKETS[@]}" -gt 0 ]; then
    for item in "${CASE_REMOTE_BUCKETS[@]}"; do [ "${item}" = "${bucket}" ] && return 0; done
  fi
  CASE_REMOTE_BUCKETS+=("${bucket}")
}

register_case_tier() {
  local tier="$1" item
  [ -n "${tier}" ] && [[ "${tier}" =~ ^[A-Za-z0-9_-]+$ ]] \
    || { echo "unsafe tier name: ${tier}" >&2; return 1; }
  [ "${tier}" = "${TIER_NAME}" ] && return 0
  if [ "${#CASE_TIERS[@]}" -gt 0 ]; then
    for item in "${CASE_TIERS[@]}"; do [ "${item}" = "${tier}" ] && return 0; done
  fi
  CASE_TIERS+=("${tier}")
}

unregister_source_bucket() {
  local bucket="$1" item kept=()
  if [ "${#CASE_SOURCE_BUCKETS[@]}" -gt 0 ]; then
    for item in "${CASE_SOURCE_BUCKETS[@]}"; do
      [ "${item}" = "${bucket}" ] || kept+=("${item}")
    done
  fi
  CASE_SOURCE_BUCKETS=("${kept[@]+"${kept[@]}"}")
}

unregister_case_tier() {
  local tier="$1" item kept=()
  if [ "${#CASE_TIERS[@]}" -gt 0 ]; then
    for item in "${CASE_TIERS[@]}"; do
      [ "${item}" = "${tier}" ] || kept+=("${item}")
    done
  fi
  CASE_TIERS=("${kept[@]+"${kept[@]}"}")
}

record_cleanup_failure() {
  local resource_type="$1" resource="$2" action="$3" detail="$4"
  local summary="${resource_type}:${resource}:${action}: ${detail}"
  CASE_CLEANUP_ERRORS+=("${summary}")
  printf 'cleanup failure: %s\n' "${summary}" >&2
  if [ -n "${RESIDUE_FILE}" ]; then
    jq -nc \
      --arg topology "${CURRENT_TOPOLOGY}" \
      --arg case_id "${CURRENT_CASE_ID}" \
      --arg resource_type "${resource_type}" \
      --arg resource "${resource}" \
      --arg action "${action}" \
      --arg error "${detail}" \
      --arg recorded_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      '{topology:$topology,case_id:$case_id,resource_type:$resource_type,resource:$resource,action:$action,error:$error,recorded_at:$recorded_at}' \
      >> "${RESIDUE_FILE}" 2>/dev/null || true
  fi
}

aws_at_run_with_timeout() {
  local timeout_seconds="$1" endpoint="$2"
  shift 2
  if AWS_OUT="$(run_bounded "${timeout_seconds}" aws --endpoint-url "${endpoint}" --region "${REGION}" "$@" 2>&1)"; then
    AWS_CODE=0
  else
    AWS_CODE=$?
  fi
  return "${AWS_CODE}"
}

aws_at_run_with_hard_timeout() {
  local timeout_seconds="$1" endpoint="$2"
  shift 2
  if AWS_OUT="$(run_hard_bounded "${timeout_seconds}" aws --endpoint-url "${endpoint}" --region "${REGION}" "$@" 2>&1)"; then
    AWS_CODE=0
  else
    AWS_CODE=$?
  fi
  return "${AWS_CODE}"
}

aws_at_run() {
  local endpoint="$1"
  shift
  aws_at_run_with_timeout "${COMMAND_TIMEOUT}" "${endpoint}" "$@"
}

cleanup_aws() {
  local endpoint="$1"
  shift
  run_bounded "${CLEANUP_TIMEOUT}" aws --endpoint-url "${endpoint}" --region "${REGION}" "$@"
}

cleanup_bucket_data() {
  local endpoint="$1" bucket="$2" out payload count key upload_id round=0 fail=0

  out="$(cleanup_aws "${endpoint}" s3api list-multipart-uploads --bucket "${bucket}" --output json 2>&1)" || {
    if printf '%s' "${out}" | grep -qiE 'NoSuchBucket|Not Found|404'; then return 0; fi
    record_cleanup_failure bucket "${bucket}" list-multipart-uploads "${out}"
    fail=1
    out='{}'
  }
  while IFS=$'\t' read -r key upload_id; do
    [ -n "${key}" ] && [ -n "${upload_id}" ] || continue
    cleanup_aws "${endpoint}" s3api abort-multipart-upload --bucket "${bucket}" --key "${key}" --upload-id "${upload_id}" >/dev/null 2>&1 \
      || { record_cleanup_failure bucket "${bucket}" abort-multipart-upload "key=${key} upload_id=${upload_id}"; fail=1; }
  done < <(printf '%s' "${out}" | jq -r '.Uploads[]? | [.Key,.UploadId] | @tsv' 2>/dev/null)

  cleanup_aws "${endpoint}" s3 rm "s3://${bucket}" --recursive >/dev/null 2>&1 || true

  while [ "${round}" -lt 10 ]; do
    if ! out="$(cleanup_aws "${endpoint}" s3api list-object-versions --bucket "${bucket}" --output json 2>&1)"; then
      if printf '%s' "${out}" | grep -qiE 'NoSuchBucket|Not Found|404'; then return "${fail}"; fi
      record_cleanup_failure bucket "${bucket}" list-object-versions "${out}"
      return 1
    fi
    payload="$(printf '%s' "${out}" | jq -c '{Objects: ([.Versions[]? | {Key:.Key,VersionId:.VersionId}] + [.DeleteMarkers[]? | {Key:.Key,VersionId:.VersionId}]), Quiet:true}' 2>/dev/null)" \
      || { record_cleanup_failure bucket "${bucket}" parse-object-versions "invalid JSON"; return 1; }
    count="$(printf '%s' "${payload}" | jq '.Objects | length')"
    [ "${count}" -eq 0 ] && return "${fail}"
    cleanup_aws "${endpoint}" s3api delete-objects --bucket "${bucket}" --delete "${payload}" >/dev/null 2>&1 \
      || { record_cleanup_failure bucket "${bucket}" delete-object-versions "count=${count}"; fail=1; }
    round=$((round+1))
  done

  record_cleanup_failure bucket "${bucket}" drain "object versions remain after 10 rounds"
  return 1
}

delete_bucket_error_code() {
  local output="$1"
  if [[ "${output}" =~ An[[:space:]]error[[:space:]]occurred[[:space:]]\(([[:alnum:]_.-]+)\)[[:space:]]when[[:space:]]calling[[:space:]]the[[:space:]]DeleteBucket[[:space:]]operation: ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

cleanup_delete_source_bucket() {
  local bucket="$1" started deadline now remaining attempt_timeout sleep_for
  local attempts=0 error_code="" last_error_code="" last_out="" last_code=0
  local elapsed error_display output_excerpt

  started="${SECONDS}"
  deadline=$((started + SOURCE_BUCKET_DELETE_TIMEOUT))
  while :; do
    now="${SECONDS}"
    remaining=$((deadline - now))
    [ "${remaining}" -gt 0 ] || break

    attempt_timeout="${SOURCE_BUCKET_DELETE_ATTEMPT_TIMEOUT}"
    if [ "${attempt_timeout}" -gt "${remaining}" ]; then
      attempt_timeout="${remaining}"
    fi
    attempts=$((attempts + 1))
    if aws_at_run_with_hard_timeout "${attempt_timeout}" "${ENDPOINT}" \
      s3api delete-bucket --bucket "${bucket}"; then
      return 0
    fi

    last_code="${AWS_CODE}"
    last_out="${AWS_OUT}"
    error_code=""
    if error_code="$(delete_bucket_error_code "${AWS_OUT}")"; then
      last_error_code="${error_code}"
    fi
    case "${error_code}" in
      NoSuchBucket)
        return 0
        ;;
      BucketNotEmpty)
        ;;
      *)
        elapsed=$((SECONDS - started))
        error_display="${error_code:-unparsed}"
        output_excerpt="$(bounded_safe_excerpt "${last_out}" 512)"
        record_cleanup_failure source-bucket "${bucket}" delete-bucket \
          "reason=non_retryable; error_code=${error_display}; attempts=${attempts}; elapsed_seconds=${elapsed}; exit=${last_code}; output_excerpt=${output_excerpt}"
        return 1
        ;;
    esac

    now="${SECONDS}"
    remaining=$((deadline - now))
    [ "${remaining}" -gt 0 ] || break
    sleep_for="${SOURCE_BUCKET_DELETE_RETRY_INTERVAL}"
    if [ "${sleep_for}" -gt "${remaining}" ]; then sleep_for="${remaining}"; fi
    sleep "${sleep_for}"
  done

  elapsed=$((SECONDS - started))
  error_display="${last_error_code:-unparsed}"
  output_excerpt="$(bounded_safe_excerpt "${last_out}" 512)"
  record_cleanup_failure source-bucket "${bucket}" delete-bucket \
    "reason=deadline_exceeded; error_code=${error_display}; attempts=${attempts}; deadline_seconds=${SOURCE_BUCKET_DELETE_TIMEOUT}; elapsed_seconds=${elapsed}; exit=${last_code}; output_excerpt=${output_excerpt}"
  return 1
}

cleanup_source_bucket() {
  local bucket="$1" out fail=0
  if ! out="$(cleanup_aws "${ENDPOINT}" s3api delete-bucket-lifecycle --bucket "${bucket}" 2>&1)"; then
    if ! printf '%s' "${out}" | grep -qiE 'NoSuchBucket|NoSuchLifecycle|Not Found|404'; then
      record_cleanup_failure source-bucket "${bucket}" delete-lifecycle "${out}"
      fail=1
    fi
  fi
  cleanup_bucket_data "${ENDPOINT}" "${bucket}" || fail=1
  cleanup_delete_source_bucket "${bucket}" || fail=1
  return "${fail}"
}

cleanup_remote_data() {
  cleanup_bucket_data "${TARGET_ENDPOINT}" "$1"
}

cleanup_remote_bucket() {
  local bucket="$1" out
  if out="$(cleanup_aws "${TARGET_ENDPOINT}" s3api delete-bucket --bucket "${bucket}" 2>&1)"; then
    return 0
  fi
  if printf '%s' "${out}" | grep -qiE 'NoSuchBucket|Not Found|404'; then return 0; fi
  record_cleanup_failure remote-bucket "${bucket}" delete-bucket "${out}"
  return 1
}

cleanup_tier() {
  local tier="$1" out
  if out="$(run_bounded "${CLEANUP_TIMEOUT}" "${RC_BIN}" ilm tier remove "${tier}" local 2>&1)"; then
    return 0
  fi
  if printf '%s' "${out}" | grep -qiE 'TierNotFound|not found|does not exist'; then return 0; fi
  if out="$(run_bounded "${CLEANUP_TIMEOUT}" "${RC_BIN}" ilm tier remove "${tier}" local --force 2>&1)"; then
    return 0
  fi
  if printf '%s' "${out}" | grep -qiE 'TierNotFound|not found|does not exist'; then return 0; fi
  record_cleanup_failure tier "${tier}" remove "${out}"
  return 1
}

case_cleanup() {
  local reason="${1:-case-finalizer}" i fail=0
  [ "${CASE_CLEANUP_DONE}" -eq 0 ] || return 0
  [ "${CASE_CLEANUP_RUNNING}" -eq 0 ] || return 0
  CASE_CLEANUP_RUNNING=1
  printf 'cleanup start (%s): topology=%s case=%s\n' "${reason}" "${CURRENT_TOPOLOGY}" "${CURRENT_CASE_ID}"

  for ((i=${#CASE_SOURCE_BUCKETS[@]}-1; i>=0; i--)); do
    cleanup_source_bucket "${CASE_SOURCE_BUCKETS[$i]}" || fail=1
  done
  for ((i=${#CASE_REMOTE_BUCKETS[@]}-1; i>=0; i--)); do
    cleanup_remote_data "${CASE_REMOTE_BUCKETS[$i]}" || fail=1
  done
  for ((i=${#CASE_TIERS[@]}-1; i>=0; i--)); do
    cleanup_tier "${CASE_TIERS[$i]}" || fail=1
  done
  for ((i=${#CASE_REMOTE_BUCKETS[@]}-1; i>=0; i--)); do
    cleanup_remote_bucket "${CASE_REMOTE_BUCKETS[$i]}" || fail=1
  done

  CASE_CLEANUP_RUNNING=0
  CASE_CLEANUP_DONE=1
  CASE_ACTIVE=0
  printf 'cleanup end: case=%s status=%s\n' "${CURRENT_CASE_ID}" "$([ "${fail}" -eq 0 ] && printf PASS || printf FAIL)"
  return "${fail}"
}

write_case_result() {
  local status="$1" case_rc="$2" cleanup_status="$3" duration="$4" cleanup_detail="${5:-}"
  local target tmp
  target="${RESULTS_DIR}/${CURRENT_TOPOLOGY}--${CURRENT_CASE_ID}.json"
  tmp="$(mktemp "${RESULTS_DIR}/.case.XXXXXX")"
  jq -n \
    --arg schema_version "1" \
    --arg topology "${CURRENT_TOPOLOGY}" \
    --arg case_id "${CURRENT_CASE_ID}" \
    --arg name "${CURRENT_CASE_NAME}" \
    --arg status "${status}" \
    --arg case_rc "${case_rc}" \
    --arg cleanup_status "${cleanup_status}" \
    --arg cleanup_detail "${cleanup_detail}" \
    --arg failure_kind "${CASE_FAILURE_KIND}" \
    --arg failure_detail "${CASE_FAILURE_DETAIL}" \
    --arg log_path "${CASE_LOG_PATH}" \
    --arg duration_seconds "${duration}" \
    --arg updated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{schema_version:($schema_version|tonumber),topology:$topology,case_id:$case_id,name:$name,status:$status,case_rc:($case_rc|tonumber),cleanup_status:$cleanup_status,cleanup_detail:$cleanup_detail,failure_kind:$failure_kind,failure_detail:$failure_detail,log_path:$log_path,duration_seconds:($duration_seconds|tonumber),updated_at:$updated_at}' \
    > "${tmp}"
  mv "${tmp}" "${target}"
}

run_test() {
  local id="$1" name="$2" fn="$3" rc=0 cleanup_rc=0 status cleanup_detail started primary_excerpt log_name
  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '  [DRY-RUN] %s %s\n' "${id}" "${name}"
    return 0
  fi
  CURRENT_CASE_ID="${id}"
  CURRENT_CASE_NAME="${name}"
  case_resources_reset
  started="${SECONDS}"
  write_case_result RUNNING 0 PENDING 0
  printf '\n--- %s %s ---\n' "${id}" "${name}"
  : > "${TEST_TMP}/test.out"
  if "${fn}" >> "${TEST_TMP}/test.out" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  if [ "${rc}" -ne 0 ] && [ "${rc}" -ne 2 ] && [ -z "${CASE_FAILURE_KIND}" ]; then
    primary_excerpt="$(sed '/^[[:space:]]*$/d' "${TEST_TMP}/test.out" | tail -1)"
    if [ -n "${primary_excerpt}" ]; then
      primary_excerpt="$(bounded_safe_excerpt "${primary_excerpt}" 512)"
      record_case_failure case_failed "case_exit=${rc}; last_output=${primary_excerpt}" >> "${TEST_TMP}/test.out" 2>&1
    else
      record_case_failure case_failed_without_diagnostic "case_exit=${rc}; test returned non-zero without diagnostic output" >> "${TEST_TMP}/test.out" 2>&1
    fi
  fi
  if case_cleanup "case-finalizer" >> "${TEST_TMP}/test.out" 2>&1; then cleanup_rc=0; else cleanup_rc=$?; fi
  cleanup_detail=""
  if [ "${#CASE_CLEANUP_ERRORS[@]}" -gt 0 ]; then
    cleanup_detail="$(printf '%s\n' "${CASE_CLEANUP_ERRORS[@]}" | sed '/^$/d' | paste -sd ';' -)"
  fi

  if [ "${cleanup_rc}" -ne 0 ] && { [ "${rc}" -eq 0 ] || [ "${rc}" -eq 2 ]; }; then
    rc=1
    record_case_failure cleanup_failed "case completed but cleanup failed; see cleanup_detail" >> "${TEST_TMP}/test.out" 2>&1
  fi
  if [ "${rc}" -eq 0 ]; then
    status=PASS; PASS=$((PASS+1)); printf '\033[1;32m[PASS]\033[0m %s %s\n' "${id}" "${name}"
  elif [ "${rc}" -eq 2 ]; then
    status=UNSUPPORTED; UNSUPPORTED=$((UNSUPPORTED+1)); printf '\033[1;33m[UNSUPPORTED]\033[0m %s %s\n' "${id}" "${name}"
  else
    status=FAIL; FAIL=$((FAIL+1)); FAILED_TESTS+=("${id} ${name}")
    printf '\033[1;31m[FAIL]\033[0m %s %s\n' "${id}" "${name}"
    log_name="${CURRENT_TOPOLOGY}--${id}.log"
    if cp "${TEST_TMP}/test.out" "${RESULT_LOG_DIR}/${log_name}" 2>/dev/null; then
      CASE_LOG_PATH="logs/${log_name}"
    fi
    sed 's/^/      | /' "${TEST_TMP}/test.out" | tail -50
  fi
  write_case_result "${status}" "${rc}" "$([ "${cleanup_rc}" -eq 0 ] && printf PASS || printf FAIL)" "$((SECONDS-started))" "${cleanup_detail}"
}

rc_run() {
  if RC_OUT="$(run_bounded "${COMMAND_TIMEOUT}" "${RC_BIN}" "$@" 2>&1)"; then RC_CODE=0; else RC_CODE=$?; fi
  return "${RC_CODE}"
}

assert_rc_ok() {
  if [ "${RC_CODE}" -eq 7 ]; then echo "rc gated (exit 7)"; return 2; fi
  [ "${RC_CODE}" -eq 0 ] || { echo "rc failed (exit ${RC_CODE})"; echo "${RC_OUT}"; return 1; }
}

aws_run() {
  aws_at_run "${ENDPOINT}" "$@"
}

aws_target_run() {
  aws_at_run "${TARGET_ENDPOINT}" "$@"
}

aws_target_run_with_timeout() {
  local timeout_seconds="$1"
  shift
  # This path is only used by the read-only namespace probe. A hard timeout
  # keeps the complete process-group lifetime inside wait_remote_state's budget.
  aws_at_run_with_hard_timeout "${timeout_seconds}" "${TARGET_ENDPOINT}" "$@"
}

assert_aws_ok() {
  [ "${AWS_CODE}" -eq 0 ] || { echo "aws failed (exit ${AWS_CODE})"; echo "${AWS_OUT}"; return 1; }
}

assert_aws_fails() {
  [ "${AWS_CODE}" -ne 0 ] || { echo "aws unexpectedly succeeded"; echo "${AWS_OUT}"; return 1; }
}

new_bucket() {
  local b="$1"
  register_source_bucket "${b}" || return 1
  aws_run s3api create-bucket --bucket "${b}"
  assert_aws_ok || return 1
}

new_remote_bucket() {
  local b="$1"
  register_remote_bucket "${b}" || return 1
  aws_target_run s3api create-bucket --bucket "${b}"
  assert_aws_ok || return 1
}

rm_bucket() {
  local b="$1"
  if cleanup_source_bucket "${b}"; then
    unregister_source_bucket "${b}"
    return 0
  fi
  return 1
}

make_test_file() {
  local path="$1" size="$2"
  if [ "${size}" -eq 0 ]; then : > "${path}"; else head -c "${size}" /dev/urandom > "${path}"; fi
}

md5_of_file() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

add_tier() {
  local type="$1" name="$2" endpoint="$3" bucket="$4" ak="$5" sk="$6"
  local prefix="${7:-}"
  register_case_tier "${name}" || return 1
  rc_run ilm tier add "${type}" "${name}" local --endpoint "${endpoint}" \
    --access-key "${ak}" --secret-key "${sk}" --bucket "${bucket}" --prefix "${prefix}" || true
}

remove_tier() {
  local tier="$1" rc
  if rc_run ilm tier remove "${tier}" local; then rc=0; else rc=$?; fi
  if [ "${RC_CODE}" -eq 0 ]; then unregister_case_tier "${tier}"; fi
  return "${rc}"
}

tier_config_json() {
  local name="$1" bucket="$2" prefix="${3:-}"
  jq -nc \
    --arg name "${name}" \
    --arg endpoint "${TARGET_ENDPOINT}" \
    --arg access_key "${ACCESS_KEY}" \
    --arg secret_key "${SECRET_KEY}" \
    --arg bucket "${bucket}" \
    --arg prefix "${prefix}" \
    '{type:"rustfs",rustfs:{name:$name,endpoint:$endpoint,accessKey:$access_key,secretKey:$secret_key,bucket:$bucket,prefix:$prefix,region:"",storageClass:""}}'
}

xml_error_code() {
  printf '%s' "$1" | tr -d '\r\n' | sed -n 's:.*<Code>\([^<]*\)</Code>.*:\1:p'
}

xml_error_message() {
  printf '%s' "$1" | tr -d '\r\n' | sed -n 's:.*<Message>\([^<]*\)</Message>.*:\1:p'
}

expected_admin_error_message() {
  case "$1" in
    XMinioAdminTierBackendInUse) printf 'Specified remote tier is already in use' ;;
    XMinioAdminTierBackendNotEmpty) printf 'Specified remote backend is not empty' ;;
    *) return 1 ;;
  esac
}

tier_is_configured() {
  local name="$1" body code membership parse_error response_excerpt
  body="$(admin_api GET /rustfs/admin/v3/tier "")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    response_excerpt="$(bounded_safe_excerpt "${body}" 512)"
    record_case_failure tier_list_http \
      "operation=ListTiers; endpoint=${API_ENDPOINT}; status=${code}; response_length=${#body}; output_excerpt=${response_excerpt}"
    return 2
  fi

  if membership="$(printf '%s' "${body}" | jq -r --arg name "${name}" '
      def nonempty_string: type == "string" and length > 0;
      if type != "array" then
        error("tier list is not an array")
      else
        map(
          . as $entry
          | if ($entry | type) != "object" then
              error("tier entry is not an object")
            elif (($entry.type | type) != "string") or (($entry.type | length) == 0) then
              error("tier type is not a non-empty string")
            else
              $entry.type as $provider_type
              | if (($entry[$provider_type] | type) != "object") then
                  error("tier provider does not match type")
                else
                  ($entry | has("name")) as $has_top_name
                  | ($entry[$provider_type] | has("name")) as $has_provider_name
                  | if $has_top_name and (($entry.name | nonempty_string) | not) then
                      error("top-level tier name is not a non-empty string")
                    elif $has_provider_name and (($entry[$provider_type].name | nonempty_string) | not) then
                      error("provider tier name is not a non-empty string")
                    elif (($has_top_name or $has_provider_name) | not) then
                      error("tier name is missing")
                    elif $has_top_name and $has_provider_name and ($entry.name != $entry[$provider_type].name) then
                      error("top-level and provider tier names conflict")
                    elif $has_top_name then
                      $entry.name
                    else
                      $entry[$provider_type].name
                    end
                end
            end
        ) as $names
        | if ($names | length) != ($names | unique | length) then
            error("duplicate tier name")
          else
            (($names | index($name)) != null)
          end
      end
    ' 2>&1)"; then
    case "${membership}" in
      true) return 0 ;;
      false) return 1 ;;
      *) parse_error="unexpected jq result: ${membership}" ;;
    esac
  else
    parse_error="${membership}"
  fi

  response_excerpt="$(bounded_safe_excerpt "${body}" 512)"
  parse_error="$(bounded_safe_excerpt "${parse_error}" 512)"
  record_case_failure tier_list_decode \
    "operation=ListTiers; endpoint=${API_ENDPOINT}; response_length=${#body}; parse_error=${parse_error}; output_excerpt=${response_excerpt}"
  return 2
}

assert_tier_absent() {
  local name="$1" rc
  if tier_is_configured "${name}"; then
    rc=0
  else
    rc=$?
  fi
  case "${rc}" in
    0)
      record_case_failure tier_unexpectedly_configured \
        "tier=${name}; expected=absent; actual=configured"
      return 1
      ;;
    1) return 0 ;;
    2) return 1 ;;
    *)
      record_case_failure tier_list_decode \
        "operation=ListTiers; tier=${name}; unexpected_query_status=${rc}"
      return 1
      ;;
  esac
}

assert_tier_present() {
  local name="$1" rc
  if tier_is_configured "${name}"; then
    rc=0
  else
    rc=$?
  fi
  case "${rc}" in
    0) return 0 ;;
    1)
      record_case_failure tier_unexpectedly_absent \
        "tier=${name}; expected=configured; actual=absent"
      return 1
      ;;
    2) return 1 ;;
    *)
      record_case_failure tier_list_decode \
        "operation=ListTiers; tier=${name}; unexpected_query_status=${rc}"
      return 1
      ;;
  esac
}

expect_add_tier_error() {
  local name="$1" bucket="$2" prefix="$3" expected_status="$4" expected_code="$5"
  local body status actual_code actual_message expected_message
  expected_message="$(expected_admin_error_message "${expected_code}")" || return 1
  register_case_tier "${name}" || return 1
  body="$(admin_api PUT /rustfs/admin/v3/tier "" "$(tier_config_json "${name}" "${bucket}" "${prefix}")")"
  status="$(admin_api_code)"
  actual_code="$(xml_error_code "${body}")"
  [ "${status}" = "${expected_status}" ] || {
    echo "tier add ${name}: expected HTTP ${expected_status}, got ${status}: ${body}"
    return 1
  }
  [ "${actual_code}" = "${expected_code}" ] || {
    echo "tier add ${name}: expected XML code ${expected_code}, got ${actual_code:-<missing>}: ${body}"
    return 1
  }
  actual_message="$(xml_error_message "${body}")"
  [ "${actual_message}" = "${expected_message}" ] || {
    echo "tier add ${name}: expected XML message '${expected_message}', got '${actual_message:-<missing>}': ${body}"
    return 1
  }

  add_tier rustfs "${name}" "${TARGET_ENDPOINT}" "${bucket}" "${ACCESS_KEY}" "${SECRET_KEY}" "${prefix}"
  [ "${RC_CODE}" -ne 0 ] || { echo "rc unexpectedly added occupied tier ${name}"; return 1; }
  printf '%s' "${RC_OUT}" | grep -Fq "${expected_code}" \
    || { echo "rc did not surface ${expected_code}: ${RC_OUT}"; return 1; }
  assert_tier_absent "${name}"
}

expect_remove_tier_error() {
  local name="$1" expected_status="$2" expected_code="$3"
  local body status actual_code actual_message expected_message
  expected_message="$(expected_admin_error_message "${expected_code}")" || return 1
  body="$(admin_api DELETE "/rustfs/admin/v3/tier/${name}" "")"
  status="$(admin_api_code)"
  actual_code="$(xml_error_code "${body}")"
  [ "${status}" = "${expected_status}" ] || {
    echo "tier remove ${name}: expected HTTP ${expected_status}, got ${status}: ${body}"
    return 1
  }
  [ "${actual_code}" = "${expected_code}" ] || {
    echo "tier remove ${name}: expected XML code ${expected_code}, got ${actual_code:-<missing>}: ${body}"
    return 1
  }
  actual_message="$(xml_error_message "${body}")"
  [ "${actual_message}" = "${expected_message}" ] || {
    echo "tier remove ${name}: expected XML message '${expected_message}', got '${actual_message:-<missing>}': ${body}"
    return 1
  }

  remove_tier "${name}" || true
  [ "${RC_CODE}" -ne 0 ] || { echo "rc unexpectedly removed protected tier ${name}"; return 1; }
  printf '%s' "${RC_OUT}" | grep -Fq "${expected_code}" \
    || { echo "rc did not surface ${expected_code}: ${RC_OUT}"; return 1; }
  assert_tier_present "${name}"
}

remote_key_count() {
  local bucket="$1" prefix="${2:-}" probe_timeout="${3:-${REMOTE_PROBE_TIMEOUT}}"
  local prefix_display parsed parse_error response_length output_excerpt timeout_marker=no
  REMOTE_KEY_COUNT=""
  prefix_display="${prefix:-<root>}"

  if ! aws_target_run_with_timeout "${probe_timeout}" s3api list-objects-v2 \
    --bucket "${bucket}" --prefix "${prefix}" --max-keys 1 --output json --no-paginate; then
    case "${AWS_CODE}" in 124|137) timeout_marker=yes ;; esac
    output_excerpt="$(bounded_safe_excerpt "${AWS_OUT}" 512)"
    record_case_failure "$([ "${timeout_marker}" = yes ] && printf remote_probe_timeout || printf remote_probe_command)" \
      "operation=ListObjectsV2; endpoint=${TARGET_ENDPOINT}; bucket=${bucket}; prefix=${prefix_display}; exit=${AWS_CODE}; timeout=${timeout_marker}; output_excerpt=${output_excerpt}"
    return 1
  fi

  if parsed="$(printf '%s' "${AWS_OUT}" | jq -er '
      if type != "object" then error("response is not an object")
      elif (has("KeyCount") | not) then error("KeyCount is missing")
      elif (.KeyCount | type) != "number" then error("KeyCount is not numeric")
      elif (.KeyCount | floor) != .KeyCount then error("KeyCount is not an integer")
      elif .KeyCount < 0 or .KeyCount > 1 then error("KeyCount is outside 0..1")
      else .KeyCount
      end
    ' 2>&1)"; then
    REMOTE_KEY_COUNT="${parsed}"
    return 0
  fi

  parse_error="$(bounded_safe_excerpt "${parsed}" 256)"
  response_length="${#AWS_OUT}"
  output_excerpt="$(bounded_safe_excerpt "${AWS_OUT}" 512)"
  record_case_failure remote_probe_decode \
    "operation=ListObjectsV2; endpoint=${TARGET_ENDPOINT}; bucket=${bucket}; prefix=${prefix_display}; exit=0; response_length=${response_length}; parse_error=${parse_error}; output_excerpt=${output_excerpt}"
  return 1
}

wait_remote_state() {
  local bucket="$1" prefix="$2" expected="$3" started="${SECONDS}" count=unknown
  local deadline=$((SECONDS + REMOTE_STATE_TIMEOUT)) attempted=0 now remaining probe_timeout sleep_for
  while true; do
    now="${SECONDS}"
    if [ "${attempted}" -eq 1 ] && [ "${now}" -ge "${deadline}" ]; then break; fi
    remaining=$((deadline - now))
    probe_timeout="${REMOTE_PROBE_TIMEOUT}"
    if [ "${probe_timeout}" -gt "${remaining}" ]; then probe_timeout="${remaining}"; fi
    [ "${probe_timeout}" -gt 0 ] || break
    remote_key_count "${bucket}" "${prefix}" "${probe_timeout}" || return 1
    count="${REMOTE_KEY_COUNT}"
    attempted=1
    if { [ "${expected}" = "empty" ] && [ "${count}" -eq 0 ]; } \
      || { [ "${expected}" = "nonempty" ] && [ "${count}" -gt 0 ]; }; then
      return 0
    fi
    now="${SECONDS}"
    [ "${now}" -lt "${deadline}" ] || break
    remaining=$((deadline - now))
    sleep_for="${REMOTE_PROBE_INTERVAL}"
    if [ "${sleep_for}" -gt "${remaining}" ]; then sleep_for="${remaining}"; fi
    [ "${sleep_for}" -gt 0 ] && sleep "${sleep_for}"
  done
  record_case_failure remote_state_timeout \
    "operation=wait_remote_state; endpoint=${TARGET_ENDPOINT}; bucket=${bucket}; prefix=${prefix:-<root>}; expected=${expected}; elapsed=$((SECONDS-started)); last_key_count=${count}"
  return 1
}

delete_lifecycle_and_assert_absent() {
  local bucket="$1"
  aws_run s3api delete-bucket-lifecycle --bucket "${bucket}"
  assert_aws_ok || return 1
  aws_run s3api get-bucket-lifecycle-configuration --bucket "${bucket}"
  [ "${AWS_CODE}" -ne 0 ] || { echo "lifecycle still present on ${bucket}: ${AWS_OUT}"; return 1; }
  printf '%s' "${AWS_OUT}" | grep -qiE 'NoSuchLifecycle|not exist|404' \
    || { echo "unexpected lifecycle absence response for ${bucket}: ${AWS_OUT}"; return 1; }
}

wait_object_transitioned() {
  local bucket="$1" key="$2" tier="$3" waited=0
  while [ "${waited}" -lt 120 ]; do
    aws_run s3api head-object --bucket "${bucket}" --key "${key}"
    if [ "${AWS_CODE}" -eq 0 ] && printf '%s' "${AWS_OUT}" | grep -Fq "${tier}"; then return 0; fi
    sleep 3
    waited=$((waited+3))
  done
  echo "object ${bucket}/${key} did not transition to ${tier}: ${AWS_OUT}"
  return 1
}

run_transition() {
  # POST /v3/ilm/transition/run; prints the job id.
  local body code query=""
  [ -n "${1:-}" ] && query="bucket=${1}"
  body="$(admin_api POST /rustfs/admin/v3/ilm/transition/run "${query}")"; code="$(admin_api_code)"
  if [ "${code}" != "200" ] && [ "${code}" != "201" ] && [ "${code}" != "202" ]; then
    echo "transition run -> HTTP ${code}: ${body}" >&2
    return 1
  fi
  printf '%s' "${body}"
}

wait_transition_job() {
  local job_id="$1" body code waited=0 state
  while [ "${waited}" -lt 180 ]; do
    body="$(admin_api GET "/rustfs/admin/v3/ilm/transition/jobs/${job_id}" "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ]; then
      state="$(printf '%s' "${body}" | jq -r '.status // .state // empty' 2>/dev/null)"
      if [ "${state}" = "completed" ] || [ "${state}" = "done" ] || [ "${state}" = "succeeded" ]; then
        printf '%s' "${body}"
        return 0
      fi
      if [ "${state}" = "failed" ] || [ "${state}" = "error" ]; then
        echo "transition job failed: ${body}" >&2
        return 1
      fi
    fi
    sleep 3; waited=$((waited+3))
  done
  echo "transition job ${job_id} did not complete: ${body}" >&2
  return 1
}

wait_transition_job_terminal() {
  local job_id="$1" body code waited=0 state
  while [ "${waited}" -lt 180 ]; do
    body="$(admin_api GET "/rustfs/admin/v3/ilm/transition/jobs/${job_id}" "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ]; then
      state="$(printf '%s' "${body}" | jq -r '.status // .state // empty' 2>/dev/null)"
      if [ "${state}" = "completed" ] || [ "${state}" = "partial" ] \
        || [ "${state}" = "failed" ] || [ "${state}" = "error" ]; then
        printf '%s' "${body}"
        return 0
      fi
    fi
    sleep 3; waited=$((waited+3))
  done
  echo "transition job ${job_id} did not reach a terminal state: ${body}" >&2
  return 1
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

# ==================== Tier / Event / Audit test cases ====================

run_all_tests() {
  run_test TIER-101 "TierAddValidation"    test_tier101
  run_test TIER-102 "TierListInfo"         test_tier102
  run_test TIER-103 "TierEditCredentials"  test_tier103
  run_test TIER-104 "TierRemove"           test_tier104
  run_test TIER-105 "TransitionRule"       test_tier105
  run_test TIER-106 "ManualTransitionJob"  test_tier106
  run_test TIER-107 "RestoreObject"        test_tier107
  run_test TIER-108 "NoncurrentTransition" test_tier108
  run_test TIER-109 "ExpiryRules"          test_tier109
  run_test TIER-110 "TierStats"            test_tier110
  run_test TIER-111 "TransitionEvents"     test_tier111
  run_test TIER-112 "TieredObjectDelete"   test_tier112
  run_test TIER-113 "EmptyPrefixAddAfterTransition" test_tier113
  run_test EVT-101 "TargetCrud"            test_evt101
  run_test EVT-102 "BucketRuleCrud"        test_evt102
  run_test EVT-103 "ObjectCreatedDelivery" test_evt103
  run_test EVT-104 "EventKinds"            test_evt104
  run_test EVT-105 "PrefixSuffixFilter"    test_evt105
  run_test EVT-106 "OfflineRetry"          test_evt106
  run_test EVT-107 "TargetReset"           test_evt107
  run_test EVT-108 "NotifyModuleSwitch"    test_evt108
  run_test AUD-101 "AuditTargetCrud"       test_aud101
  run_test AUD-102 "S3OpsAudited"          test_aud102
  run_test AUD-103 "FailedRequestAudited"  test_aud103
  run_test AUD-104 "AdminOpsAudited"       test_aud104
  run_test AUD-105 "AuditOfflineRetry"     test_aud105
  run_test AUD-106 "SensitiveRedaction"    test_aud106
  run_test AUD-107 "AuditModuleSwitch"     test_aud107
}

test_tier101() {
  add_tier rustfs "${TIER_NAME}" "${TARGET_ENDPOINT}" "${TIER_BUCKET}" "${ACCESS_KEY}" "${SECRET_KEY}"
  assert_rc_ok || return 1
  # lowercase tier name must be rejected
  add_tier rustfs warm "${TARGET_ENDPOINT}" "${TIER_BUCKET}" "${ACCESS_KEY}" "${SECRET_KEY}"
  if [ "${RC_CODE}" -eq 0 ]; then
    echo "lowercase tier name unexpectedly accepted"; return 1
  fi
  printf '%s' "${RC_OUT}" | grep -qiE 'uppercase|upper|ERR_TIER_NAME' || { echo "unexpected error for lowercase: ${RC_OUT}"; return 1; }
  # reserved name must be rejected
  add_tier rustfs STANDARD "${TARGET_ENDPOINT}" "${TIER_BUCKET}" "${ACCESS_KEY}" "${SECRET_KEY}"
  if [ "${RC_CODE}" -eq 0 ]; then
    remove_tier STANDARD || true
    echo "reserved tier name STANDARD unexpectedly accepted"; return 1
  fi
  printf '%s' "${RC_OUT}" | grep -qiE 'reserved|ERR_TIER_RESERVED' || { echo "unexpected error for reserved name: ${RC_OUT}"; return 1; }
}

test_tier102() {
  rc_run ilm tier list local --json
  assert_rc_ok || return 1
  printf '%s' "${RC_OUT}" | grep -q "${TIER_NAME}" || { echo "tier ${TIER_NAME} not in list: ${RC_OUT}"; return 1; }
  printf '%s' "${RC_OUT}" | grep -q '"secretKey": "REDACTED"' || { echo "secretKey not redacted in tier list: ${RC_OUT}"; return 1; }
  if printf '%s' "${RC_OUT}" | grep -q "\"secretKey\": \"${SECRET_KEY}\""; then echo "secret key leaked in tier list"; return 1; fi
  local body code
  body="$(admin_api GET "/rustfs/admin/v3/tier/${TIER_NAME}" "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "GET /v3/tier/${TIER_NAME} -> HTTP ${code}: ${body}"; return 1; }
  if [ -n "${body}" ]; then
    printf '%s' "${body}" | grep -q 'REDACTED' || { echo "secretKey not redacted in tier detail: ${body}"; return 1; }
    if printf '%s' "${body}" | grep -q "\"secretKey\": \"${SECRET_KEY}\""; then echo "secret key leaked in tier detail"; return 1; fi
  fi
  # invalid credentials must be rejected by the connectivity check
  add_tier rustfs BADTIER "${TARGET_ENDPOINT}" "${TIER_BUCKET}" "wrong-key" "wrong-secret"
  if [ "${RC_CODE}" -eq 0 ]; then
    echo "note: tier add with invalid credentials succeeded (no verify on add); removing"
  else
    printf '%s' "${RC_OUT}" | grep -qiE 'credential|verify|denied|invalid|401|403' || { echo "unexpected error for bad creds: ${RC_OUT}"; return 1; }
  fi
  # A failed tier add can leave a partial tier entry behind; clean it so later
  # tier mutations are not poisoned ("missing field prefix").
  remove_tier BADTIER || true
}

test_tier103() {
  rc_run ilm tier edit "${TIER_NAME}" local --access-key "${ACCESS_KEY}" --secret-key "${SECRET_KEY}"
  assert_rc_ok || return 1
  # verify the edited tier still works end-to-end via a transition.
  new_bucket "t103-b1" || return 1
  make_test_file ${TEST_TMP}/tier-f1 1048576
  aws_run s3api put-object --bucket t103-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  rc_run ilm rule add local/t103-b1 --storage-class "${TIER_NAME}" --transition-days 0
  assert_rc_ok || return 1
  local out job
  out="$(run_transition t103-b1)" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    aws_run s3api head-object --bucket t103-b1 --key f1
    if printf '%s' "${AWS_OUT}" | grep -q "${TIER_NAME}"; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "object not transitioned after edit: ${AWS_OUT}"; return 1; }
  rm_bucket t103-b1
}

test_tier104() {
  local topo_tag token out job
  local empty_remote data_remote ref_remote data_source ref_source
  local empty_tier data_tier ref_tier
  token="${RUN_TOKEN//-/}"
  case "${CURRENT_TOPOLOGY}" in single-single) topo_tag=ss ;; single-multi) topo_tag=sm ;; *) topo_tag=na ;; esac
  empty_remote="t104-${topo_tag}-${RUN_TOKEN}-empty"
  data_remote="t104-${topo_tag}-${RUN_TOKEN}-data"
  ref_remote="t104-${topo_tag}-${RUN_TOKEN}-ref"
  data_source="t104-${topo_tag}-${RUN_TOKEN}-srcd"
  ref_source="t104-${topo_tag}-${RUN_TOKEN}-srcr"
  empty_tier="T104E${token}"
  data_tier="T104D${token}"
  ref_tier="T104R${token}"

  # Empty backend: add/remove succeeds and leaves no config entry.
  new_remote_bucket "${empty_remote}" || return 1
  wait_remote_state "${empty_remote}" "" empty || return 1
  add_tier rustfs "${empty_tier}" "${TARGET_ENDPOINT}" "${empty_remote}" "${ACCESS_KEY}" "${SECRET_KEY}"
  assert_rc_ok || return 1
  remove_tier "${empty_tier}"
  assert_rc_ok || return 1
  assert_tier_absent "${empty_tier}" || return 1

  # Physical data: remove the lifecycle reference first so the only blocker is
  # the non-empty remote namespace.
  new_remote_bucket "${data_remote}" || return 1
  add_tier rustfs "${data_tier}" "${TARGET_ENDPOINT}" "${data_remote}" "${ACCESS_KEY}" "${SECRET_KEY}"
  assert_rc_ok || return 1
  new_bucket "${data_source}" || return 1
  make_test_file "${TEST_TMP}/tier-f1" 65536
  aws_run s3api put-object --bucket "${data_source}" --key f1 --body "${TEST_TMP}/tier-f1"
  assert_aws_ok || return 1
  rc_run ilm rule add "local/${data_source}" --storage-class "${data_tier}" --transition-days 0
  assert_rc_ok || return 1
  out="$(run_transition "${data_source}")" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  wait_object_transitioned "${data_source}" f1 "${data_tier}" || return 1
  wait_remote_state "${data_remote}" "" nonempty || return 1
  delete_lifecycle_and_assert_absent "${data_source}" || return 1
  expect_remove_tier_error "${data_tier}" 400 XMinioAdminTierBackendNotEmpty || return 1
  wait_remote_state "${data_remote}" "" nonempty || { echo "rejected remove damaged remote data"; return 1; }

  # Lifecycle-only reference: the remote namespace is provably empty. This is
  # RustFS's fail-closed extension, not a claim about native MinIO behavior.
  new_remote_bucket "${ref_remote}" || return 1
  wait_remote_state "${ref_remote}" "" empty || return 1
  add_tier rustfs "${ref_tier}" "${TARGET_ENDPOINT}" "${ref_remote}" "${ACCESS_KEY}" "${SECRET_KEY}"
  assert_rc_ok || return 1
  new_bucket "${ref_source}" || return 1
  rc_run ilm rule add "local/${ref_source}" --storage-class "${ref_tier}" --transition-days 0
  assert_rc_ok || return 1
  wait_remote_state "${ref_remote}" "" empty || return 1
  expect_remove_tier_error "${ref_tier}" 409 XMinioAdminTierBackendInUse || return 1
  delete_lifecycle_and_assert_absent "${ref_source}" || return 1
  remove_tier "${ref_tier}"
  assert_rc_ok || return 1
  assert_tier_absent "${ref_tier}"
}

test_tier105() {
  new_bucket "t105-b1" || return 1
  rc_run ilm rule add local/t105-b1 --storage-class "${TIER_NAME}" --transition-days 0
  assert_rc_ok || return 1
  aws_run s3api get-bucket-lifecycle-configuration --bucket t105-b1
  assert_aws_ok || return 1
  printf '%s' "${AWS_OUT}" | grep -q "${TIER_NAME}" || { echo "lifecycle rule missing tier: ${AWS_OUT}"; return 1; }
  make_test_file ${TEST_TMP}/tier-f1 1048576
  aws_run s3api put-object --bucket t105-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  local out job
  out="$(run_transition t105-b1)" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    aws_run s3api head-object --bucket t105-b1 --key f1
    if printf '%s' "${AWS_OUT}" | grep -q "${TIER_NAME}"; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "object not transitioned: ${AWS_OUT}"; return 1; }
  aws_run s3api get-object --bucket t105-b1 --key f1 ${TEST_TMP}/tier-dl1
  assert_aws_ok || return 1
  [ "$(md5_of_file ${TEST_TMP}/tier-dl1)" = "$(md5_of_file ${TEST_TMP}/tier-f1)" ] || { echo "transitioned GET content mismatch"; return 1; }
  rm_bucket t105-b1
}

test_tier106() {
  new_bucket "t106-b1" || return 1
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket t106-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  rc_run ilm rule add local/t106-b1 --storage-class "${TIER_NAME}" --transition-days 0
  assert_rc_ok || return 1
  local out job body code state skipped_in_flight attempt=0
  while [ "${attempt}" -lt 5 ]; do
    out="$(run_transition t106-b1)" || return 1
    job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
    if [ -n "${job}" ]; then
      body="$(wait_transition_job_terminal "${job}")" || return 1
      printf '%s' "${body}" | grep -q "${job}" || { echo "job status missing job id: ${body}"; return 1; }
      state="$(printf '%s' "${body}" | jq -r '.status // .state // empty')"
      out="${body}"
      body="$(admin_api DELETE "/rustfs/admin/v3/ilm/transition/jobs/${job}" "")"; code="$(admin_api_code)"
      [ "${code}" = "200" ] || [ "${code}" = "204" ] || { echo "job delete -> HTTP ${code}: ${body}"; return 1; }
    else
      # Small synchronous runs complete inline and return a report without job_id.
      state="$(printf '%s' "${out}" | jq -r '.state // empty')"
      printf '%s' "${out}" | grep -q 'report' || { echo "transition run missing report: ${out}"; return 1; }
    fi

    [ "${state}" = "completed" ] && break
    [ "${state}" = "partial" ] || { echo "transition run failed: ${out}"; return 1; }
    skipped_in_flight="$(printf '%s' "${out}" | jq -r '.report.skipped_already_in_flight // 0')"
    [ "${skipped_in_flight}" -gt 0 ] || { echo "transition run returned unexpected partial state: ${out}"; return 1; }
    printf '%s' "${out}" | jq -e '
      (.report.skipped_queue_full // 0) == 0 and
      (.report.skipped_queue_closed // 0) == 0 and
      (.report.skipped_queue_timeout // 0) == 0 and
      (.report.tier_failure // 0) == 0 and
      (.report.transition_failed // 0) == 0
    ' >/dev/null || { echo "transition run returned non-retryable partial state: ${out}"; return 1; }
    attempt=$((attempt + 1))
    sleep 2
  done
  [ "${state}" = "completed" ] || { echo "transition run remained partial after retries: ${out}"; return 1; }
  rm_bucket t106-b1
}

test_tier107() {
  new_bucket "t107-b1" || return 1
  make_test_file ${TEST_TMP}/tier-f1 1048576
  aws_run s3api put-object --bucket t107-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  rc_run ilm rule add local/t107-b1 --storage-class "${TIER_NAME}" --transition-days 0
  assert_rc_ok || return 1
  local out job
  out="$(run_transition t107-b1)" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    aws_run s3api head-object --bucket t107-b1 --key f1
    if printf '%s' "${AWS_OUT}" | grep -q "${TIER_NAME}"; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "object not transitioned: ${AWS_OUT}"; return 1; }
  aws_run s3api restore-object --bucket t107-b1 --key f1 --restore-request '{"Days":1}'
  assert_aws_ok || return 1
  waited=0
  while [ "${waited}" -lt 90 ]; do
    aws_run s3api head-object --bucket t107-b1 --key f1
    if printf '%s' "${AWS_OUT}" | grep -q "x-amz-restore\|Restore"; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 90 ] || { echo "no x-amz-restore header after restore: ${AWS_OUT}"; return 1; }
  aws_run s3api get-object --bucket t107-b1 --key f1 ${TEST_TMP}/tier-dl1
  assert_aws_ok || return 1
  [ "$(md5_of_file ${TEST_TMP}/tier-dl1)" = "$(md5_of_file ${TEST_TMP}/tier-f1)" ] || { echo "restored content mismatch"; return 1; }
  rm_bucket t107-b1
}

test_tier108() {
  new_bucket "t108-b1" || return 1
  aws_run s3api put-bucket-versioning --bucket t108-b1 --versioning-configuration Status=Enabled
  assert_aws_ok || return 1
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket t108-b1 --key f --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  local vid1
  vid1="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId')"
  aws_run s3api put-object --bucket t108-b1 --key f --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  local vid2
  vid2="$(printf '%s' "${AWS_OUT}" | jq -r '.VersionId')"
  rc_run ilm rule add local/t108-b1 --noncurrent-transition-days 0 --noncurrent-transition-storage-class "${TIER_NAME}"
  assert_rc_ok || return 1
  local out job
  out="$(run_transition t108-b1)" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    aws_run s3api head-object --bucket t108-b1 --key f --version-id "${vid1}"
    if printf '%s' "${AWS_OUT}" | grep -q "${TIER_NAME}"; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "noncurrent version not transitioned: ${AWS_OUT}"; return 1; }
  aws_run s3api head-object --bucket t108-b1 --key f --version-id "${vid2}"
  if printf '%s' "${AWS_OUT}" | grep -q "${TIER_NAME}"; then echo "current version unexpectedly transitioned: ${AWS_OUT}"; return 1; fi
  rm_bucket t108-b1
}

test_tier109() {
  new_bucket "t109-b1" || return 1
  local expdate
  expdate="$(date -u -d 'yesterday 00:00:00' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-1d -v0H -v0M -v0S +%Y-%m-%dT%H:%M:%SZ)"
  aws_run s3api put-bucket-lifecycle-configuration --bucket t109-b1 \
    --lifecycle-configuration "{\"Rules\":[{\"ID\":\"exp\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"\"},\"Expiration\":{\"Date\":\"${expdate}\"}}]}"
  assert_aws_ok || return 1
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket t109-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  local out job
  out="$(run_transition t109-b1)" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  local waited=0
  while [ "${waited}" -lt 180 ]; do
    aws_run s3api head-object --bucket t109-b1 --key f1
    if [ "${AWS_CODE}" -ne 0 ] && printf '%s' "${AWS_OUT}" | grep -qi "404\|NoSuchKey"; then
      echo "object expired"; break
    fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 180 ] || { echo "object not expired: ${AWS_OUT}"; return 1; }
  # AbortIncompleteMultipartUpload: rule acceptance + lifecycle read-back are
  # verified; the day-based abort itself cannot complete inside a CI window.
  aws_run s3api put-bucket-lifecycle-configuration --bucket t109-b1 \
    --lifecycle-configuration "{\"Rules\":[{\"ID\":\"abort\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"\"},\"AbortIncompleteMultipartUpload\":{\"DaysAfterInitiation\":1}}]}"
  assert_aws_ok || return 1
  aws_run s3api get-bucket-lifecycle-configuration --bucket t109-b1
  printf '%s' "${AWS_OUT}" | grep -q "AbortIncompleteMultipartUpload" || { echo "abort rule not persisted: ${AWS_OUT}"; return 1; }
  echo "note: AbortIncompleteMultipartUpload day-based abort verified at config level only"
  rm_bucket t109-b1
}

test_tier110() {
  local body code
  body="$(admin_api GET /rustfs/admin/v3/tier-stats "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "tier-stats -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -qiE 'DailyAllTierStats|tier|stats|WARM' || { echo "tier-stats unexpected: ${body}"; return 1; }
}

test_tier111() {
  # Requires a notification rule on the bucket before the transition.
  new_bucket "t111-b1" || return 1
  local arn
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { echo "no target arn available: ${arn}"; rm_bucket t111-b1; return 1; }
  rc_run event add local/t111-b1 "${arn}" --event s3:ObjectCreated:*,s3:ObjectRemoved:*,s3:ObjectTransition:*,s3:ObjectRestore:*,s3:LifecycleExpiration:*
  assert_rc_ok || return 1
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket t111-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  rc_run ilm rule add local/t111-b1 --storage-class "${TIER_NAME}" --transition-days 0
  assert_rc_ok || return 1
  local out job
  out="$(run_transition t111-b1)" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  wait_notify 1 "ObjectTransition" || return 1
  aws_run s3api restore-object --bucket t111-b1 --key f1 --restore-request '{"Days":1}'
  assert_aws_ok || return 1
  wait_notify 1 "ObjectRestore" || return 1
  rm_bucket t111-b1
}

test_tier112() {
  # Dedicated warm bucket so remote-data assertions are not confounded by other
  # tests sharing the main warm tier.
  local topo_tag token tb tier
  token="${RUN_TOKEN//-/}"
  case "${CURRENT_TOPOLOGY}" in single-single) topo_tag=ss ;; single-multi) topo_tag=sm ;; *) topo_tag=na ;; esac
  tb="t112-${topo_tag}-${RUN_TOKEN}-warm"
  tier="T112${token}"
  new_remote_bucket "${tb}" || return 1
  add_tier rustfs "${tier}" "${TARGET_ENDPOINT}" "${tb}" "${ACCESS_KEY}" "${SECRET_KEY}"
  assert_rc_ok || return 1
  new_bucket "t112-b1" || return 1
  make_test_file ${TEST_TMP}/tier-f1 1048576
  aws_run s3api put-object --bucket t112-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  rc_run ilm rule add local/t112-b1 --storage-class "${tier}" --transition-days 0
  assert_rc_ok || return 1
  local out job
  out="$(run_transition t112-b1)" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  local waited=0
  while [ "${waited}" -lt 120 ]; do
    aws_run s3api head-object --bucket t112-b1 --key f1
    if printf '%s' "${AWS_OUT}" | grep -Fq "${tier}"; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 120 ] || { echo "object not transitioned: ${AWS_OUT}"; return 1; }
  # remote copy must exist on the warm tier (prefix layout is backend-owned)
  local remote_out
  waited=0
  while [ "${waited}" -lt 60 ]; do
    aws_target_run s3api list-objects --bucket "${tb}" || true
    remote_out="${AWS_OUT}"
    if printf '%s' "${remote_out}" | grep -q '"Key"'; then break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 60 ] || { echo "remote copy not found: ${remote_out}"; return 1; }
  aws_run s3api delete-object --bucket t112-b1 --key f1
  assert_aws_ok || return 1
  aws_run s3api head-object --bucket t112-b1 --key f1
  assert_aws_fails || { echo "local stub still present after delete"; return 1; }
  waited=0
  while [ "${waited}" -lt 60 ]; do
    aws_target_run s3api list-objects --bucket "${tb}" || true
    remote_out="${AWS_OUT}"
    if ! printf '%s' "${remote_out}" | grep -qE '"Key"'; then echo "remote data cleaned"; break; fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 60 ] || { echo "remote data not cleaned: ${remote_out}"; return 1; }
  rm_bucket t112-b1
  remove_tier "${tier}" || true
}

test_tier113() {
  local topo_tag token out job
  local used_remote fresh_remote prefix_remote source
  local used_tier occupied_candidate fresh_tier prefix_ok prefix_bad
  token="${RUN_TOKEN//-/}"
  case "${CURRENT_TOPOLOGY}" in single-single) topo_tag=ss ;; single-multi) topo_tag=sm ;; *) topo_tag=na ;; esac
  used_remote="t113-${topo_tag}-${RUN_TOKEN}-used"
  fresh_remote="t113-${topo_tag}-${RUN_TOKEN}-fresh"
  prefix_remote="t113-${topo_tag}-${RUN_TOKEN}-prefix"
  source="t113-${topo_tag}-${RUN_TOKEN}-src"
  used_tier="T113U${token}"
  occupied_candidate="T113C${token}"
  fresh_tier="T113F${token}"
  prefix_ok="T113P${token}"
  prefix_bad="T113O${token}"

  # Build an occupied backend without relying on the suite-wide WARM tier.
  new_remote_bucket "${used_remote}" || return 1
  add_tier rustfs "${used_tier}" "${TARGET_ENDPOINT}" "${used_remote}" "${ACCESS_KEY}" "${SECRET_KEY}"
  assert_rc_ok || return 1
  new_bucket "${source}" || return 1
  make_test_file "${TEST_TMP}/tier-f1" 65536
  aws_run s3api put-object --bucket "${source}" --key f1 --body "${TEST_TMP}/tier-f1"
  assert_aws_ok || return 1
  rc_run ilm rule add "local/${source}" --storage-class "${used_tier}" --transition-days 0
  assert_rc_ok || return 1
  out="$(run_transition "${source}")" || return 1
  job="$(printf '%s' "${out}" | jq -r '.job_id // empty' 2>/dev/null)"
  if [ -n "${job}" ]; then wait_transition_job "${job}" >/dev/null || return 1; fi
  wait_object_transitioned "${source}" f1 "${used_tier}" || return 1
  wait_remote_state "${used_remote}" "" nonempty || return 1

  # An exact occupied namespace must report the compatibility error, not a
  # permission/decode error and never "missing field prefix".
  expect_add_tier_error "${occupied_candidate}" "${used_remote}" "" 409 XMinioAdminTierBackendInUse || return 1

  # A prior expected rejection must not poison a subsequent add to a fresh
  # backend.
  new_remote_bucket "${fresh_remote}" || return 1
  wait_remote_state "${fresh_remote}" "" empty || return 1
  add_tier rustfs "${fresh_tier}" "${TARGET_ENDPOINT}" "${fresh_remote}" "${ACCESS_KEY}" "${SECRET_KEY}"
  assert_rc_ok || return 1
  remove_tier "${fresh_tier}"
  assert_rc_ok || return 1
  assert_tier_absent "${fresh_tier}" || return 1

  # Same bucket, disjoint prefixes are valid; a prefix containing data is not.
  new_remote_bucket "${prefix_remote}" || return 1
  aws_target_run s3api put-object --bucket "${prefix_remote}" --key occupied/sentinel --body "${TEST_TMP}/tier-f1"
  assert_aws_ok || return 1
  wait_remote_state "${prefix_remote}" occupied/ nonempty || return 1
  wait_remote_state "${prefix_remote}" fresh/ empty || return 1
  add_tier rustfs "${prefix_ok}" "${TARGET_ENDPOINT}" "${prefix_remote}" "${ACCESS_KEY}" "${SECRET_KEY}" fresh/
  assert_rc_ok || return 1
  remove_tier "${prefix_ok}"
  assert_rc_ok || return 1
  assert_tier_absent "${prefix_ok}" || return 1
  expect_add_tier_error "${prefix_bad}" "${prefix_remote}" occupied/ 409 XMinioAdminTierBackendInUse
}

test_evt101() {
  local body code
  body="$(admin_api PUT "/rustfs/admin/v3/target/notify_webhook/secondary" "" "{\"key_values\":[{\"key\":\"endpoint\",\"value\":\"${NOTIFY_ENDPOINT}\"},{\"key\":\"auth_token\",\"value\":\"secondary-token\"}]}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "PUT target notify_webhook/secondary -> HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api GET /rustfs/admin/v3/target/list "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "GET target/list -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -q "secondary" || { echo "secondary target not in list: ${body}"; return 1; }
  body="$(admin_api GET /rustfs/admin/v3/target/arns "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "GET target/arns -> HTTP ${code}: ${body}"; return 1; }
  [ -n "${body}" ] || { echo "target/arns empty"; return 1; }
}

test_evt102() {
  new_bucket "evt102-b1" || return 1
  local arn
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { rm_bucket evt102-b1; return 1; }
  rc_run event add local/evt102-b1 "${arn}" --event s3:ObjectCreated:*
  assert_rc_ok || return 1
  rc_run event list local/evt102-b1
  assert_rc_ok || return 1
  printf '%s' "${RC_OUT}" | grep -q "${arn}" || { echo "rule not visible: ${RC_OUT}"; return 1; }
  rc_run event remove local/evt102-b1 "${arn}"
  assert_rc_ok || return 1
  rc_run event list local/evt102-b1
  assert_rc_ok || return 1
  if printf '%s' "${RC_OUT}" | grep -q "${arn}"; then echo "rule still present after remove: ${RC_OUT}"; return 1; fi
  rm_bucket evt102-b1
}

test_evt103() {
  new_bucket "evt103-b1" || return 1
  local arn
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { echo "no target arn"; rm_bucket evt103-b1; return 1; }
  rc_run event add local/evt103-b1 "${arn}" --event s3:ObjectCreated:*
  assert_rc_ok || return 1
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket evt103-b1 --key a/b.txt --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  # poke: a second event forces the delivery queue to flush promptly
  aws_run s3api put-object --bucket evt103-b1 --key poke-103 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  wait_notify 1 "ObjectCreated" || return 1
  notify_grep "ObjectCreated" | grep -q "evt103-b1" || { echo "event missing bucket: $(notify_grep 'ObjectCreated')"; return 1; }
  notify_grep "ObjectCreated" | grep -q "a/b.txt" || { echo "event missing key: $(notify_grep 'ObjectCreated')"; return 1; }
  notify_grep "ObjectCreated" | grep -qiE 'etag|size|region' || { echo "event missing fields: $(notify_grep 'ObjectCreated')"; return 1; }
  rm_bucket evt103-b1
}

test_evt104() {
  new_bucket "evt104-b1" || return 1
  local arn
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { echo "no target arn"; rm_bucket evt104-b1; return 1; }
  rc_run event add local/evt104-b1 "${arn}" --event s3:ObjectCreated:*,s3:ObjectRemoved:*,s3:ObjectTagging:*
  assert_rc_ok || return 1
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket evt104-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  aws_run s3api copy-object --bucket evt104-b1 --key f2 --copy-source evt104-b1/f1
  assert_aws_ok || return 1
  aws_run s3api create-multipart-upload --bucket evt104-b1 --key mp
  assert_aws_ok || return 1
  local uid
  uid="$(printf '%s' "${AWS_OUT}" | jq -r '.UploadId')"
  aws_run s3api upload-part --bucket evt104-b1 --key mp --part-number 1 --upload-id "${uid}" --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  local etag
  etag="$(printf '%s' "${AWS_OUT}" | jq -r '.ETag' | tr -d '"')"
  aws_run s3api complete-multipart-upload --bucket evt104-b1 --key mp --upload-id "${uid}" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":\"${etag}\"}]}"
  assert_aws_ok || return 1
  aws_run s3api put-object-tagging --bucket evt104-b1 --key f1 --tagging 'TagSet=[{Key=k,Value=v}]'
  assert_aws_ok || return 1
  aws_run s3api delete-object --bucket evt104-b1 --key f1
  assert_aws_ok || return 1
  wait_notify 1 "ObjectRemoved" || return 1
  notify_grep "ObjectCreated" | grep -q "Copy" || { echo "no copy event: $(notify_grep 'ObjectCreated')"; return 1; }
  notify_grep "ObjectCreated" | grep -q "CompleteMultipartUpload" || { echo "no multipart event: $(notify_grep 'ObjectCreated')"; return 1; }
  notify_grep "ObjectTagging" | grep -q "Put" || { echo "no tagging event: $(notify_grep 'ObjectTagging')"; return 1; }
  rm_bucket evt104-b1
}

test_evt105() {
  new_bucket "evt105-b1" || return 1
  local arn
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { echo "no target arn"; rm_bucket evt105-b1; return 1; }
  aws_run s3api put-bucket-notification-configuration --bucket evt105-b1 \
    --notification-configuration "{\"QueueConfigurations\":[{\"Id\":\"f\",\"QueueArn\":\"${arn}\",\"Events\":[\"s3:ObjectCreated:*\"],\"Filter\":{\"Key\":{\"FilterRules\":[{\"Name\":\"prefix\",\"Value\":\"logs/\"},{\"Name\":\"suffix\",\"Value\":\".jpg\"}]}}}]}"
  assert_aws_ok || return 1
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket evt105-b1 --key "logs/x.jpg" --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  aws_run s3api put-object --bucket evt105-b1 --key "other/y.png" --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  aws_run s3api put-object --bucket evt105-b1 --key "logs/poke.jpg" --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  wait_notify 1 "logs/x.jpg" || return 1
  sleep 5
  if notify_grep "ObjectCreated" | grep -q "other/y.png"; then echo "filtered object still delivered"; return 1; fi
  rm_bucket evt105-b1
}

test_evt106() {
  new_bucket "evt106-b1" || return 1
  local arn
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { echo "no target arn"; rm_bucket evt106-b1; return 1; }
  rc_run event add local/evt106-b1 "${arn}" --event s3:ObjectCreated:*
  assert_rc_ok || return 1
  events_reset
  receiver_stop
  local i
  for i in 1 2 3; do
    aws_run s3api put-object --bucket evt106-b1 --key "off-${i}" --body ${TEST_TMP}/tier-f1
    assert_aws_ok || return 1
  done
  sleep 3
  receiver_start
  # poke: a new event after recovery forces the queue to flush
  aws_run s3api put-object --bucket evt106-b1 --key poke --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  wait_notify 3 "off-" || return 1
  rm_bucket evt106-b1
}

test_evt107() {
  local body code
  # The discard target must be reachable so the notify runtime converges; the
  # reset below removes it and stops delivery for rules referencing it.
  body="$(admin_api PUT "/rustfs/admin/v3/target/notify_webhook/discard" "" "{\"key_values\":[{\"key\":\"endpoint\",\"value\":\"http://${HEAL_IP}:${RECEIVER_PORT}/hook2\"}]}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "PUT discard target -> HTTP ${code}: ${body}"; return 1; }
  wait_targets_ready || return 1
  new_bucket "evt107-b1" || return 1
  local arn raw
  raw="$(admin_api GET /rustfs/admin/v3/target/arns "")"
  arn="$(printf '%s' "${raw}" | jq -r '.[] | select(test("discard"; "i")) | .' 2>/dev/null | head -1)"
  [ -n "${arn}" ] || { echo "discard arn not found"; rm_bucket evt107-b1; return 1; }
  rc_run event add local/evt107-b1 "${arn}" --event s3:ObjectCreated:*
  assert_rc_ok || return 1
  body="$(admin_api DELETE "/rustfs/admin/v3/target/notify_webhook/discard/reset" "")"; code="$(admin_api_code)"
  # The server refuses to remove a target while a rule references it (500 /
  # convergence failure), so drop the referencing rule first and retry.
  if [ "${code}" != "200" ] && [ "${code}" != "204" ]; then
    echo "note: reset while rule references target -> HTTP ${code}; removing rule and retrying"
    rc_run event remove local/evt107-b1 "${arn}"
    sleep 2
    body="$(admin_api DELETE "/rustfs/admin/v3/target/notify_webhook/discard/reset" "")"; code="$(admin_api_code)"
  fi
  # Acceptance: the target disappears from the list.
  local waited=0
  while [ "${waited}" -lt 60 ]; do
    body="$(admin_api GET /rustfs/admin/v3/target/list "")"; code="$(admin_api_code)"
    if [ "${code}" = "200" ] && ! printf '%s' "${body}" | grep -q "discard"; then
      echo "note: discard target removed (reset HTTP ${code})"
      break
    fi
    sleep 3; waited=$((waited+3))
  done
  [ "${waited}" -lt 60 ] || { echo "discard target still present after reset (reset HTTP ${code}): ${body}"; return 1; }
  # delivery for the referencing rule must stop (rule was removed as required)
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket evt107-b1 --key after --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  sleep 10
  if [ "$(notify_count)" -ne 0 ]; then echo "events delivered after target reset: $(notify_grep .)"; return 1; fi
  rm_bucket evt107-b1
}

test_evt108() {
  new_bucket "evt108-b1" || return 1
  local arn body code
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { echo "no target arn"; rm_bucket evt108-b1; return 1; }
  rc_run event add local/evt108-b1 "${arn}" --event s3:ObjectCreated:*
  assert_rc_ok || return 1
  body="$(admin_api PUT /rustfs/admin/v3/module-switches "" '{"notify_enabled":false,"audit_enabled":true}')"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "disable notify -> HTTP ${code}: ${body}"; return 1; }
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket evt108-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  sleep 5
  if [ "$(notify_count)" -ne 0 ]; then echo "events delivered while notify disabled: $(notify_grep .)"; return 1; fi
  body="$(admin_api PUT /rustfs/admin/v3/module-switches "" '{"notify_enabled":true,"audit_enabled":true}')"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "enable notify -> HTTP ${code}: ${body}"; return 1; }
  wait_targets_ready || return 1
  aws_run s3api put-object --bucket evt108-b1 --key f2 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  local waited=0
  while [ "${waited}" -lt 300 ]; do
    if grep -q "f2" "${NOTIFY_EVENTS}" 2>/dev/null; then break; fi
    if [ $((waited % 30)) -eq 0 ]; then
      aws_run s3api put-object --bucket evt108-b1 --key "poke-${waited}" --body ${TEST_TMP}/tier-f1
    fi
    sleep 5; waited=$((waited+5))
  done
  [ "${waited}" -lt 300 ] || { echo "notify delivery did not resume after module re-enable"; tail -3 "${NOTIFY_EVENTS}"; return 1; }
  rm_bucket evt108-b1
}

test_aud101() {
  local body code
  body="$(admin_api PUT "/rustfs/admin/v3/audit/target/audit_webhook/audit1" "" "{\"key_values\":[{\"key\":\"endpoint\",\"value\":\"${AUDIT_ENDPOINT}\"},{\"key\":\"auth_token\",\"value\":\"${AUDIT_TOKEN}\"}]}")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "PUT audit target (audit_webhook) -> HTTP ${code}: ${body}"; return 1; }
  body="$(admin_api GET /rustfs/admin/v3/audit/target/list "")"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "GET audit target list -> HTTP ${code}: ${body}"; return 1; }
  printf '%s' "${body}" | grep -q "audit1" || { echo "audit1 not listed: ${body}"; return 1; }
}

test_aud102() {
  new_bucket "aud102-b1" || return 1
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket aud102-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  aws_run s3api get-object --bucket aud102-b1 --key f1 ${TEST_TMP}/tier-dl1
  assert_aws_ok || return 1
  wait_audit 2 "aud102-b1" || return 1
  audit_grep "aud102-b1" | grep -qiE 'api|bucket|object|status|request_id|remote_host|time_to_response' \
    || { echo "audit entry missing required fields: $(audit_grep 'aud102-b1')"; return 1; }
  rm_bucket aud102-b1
}

test_aud103() {
  new_bucket "aud103-b1" || return 1
  events_reset
  aws_run s3api get-object --bucket aud103-b1 --key missing ${TEST_TMP}/tier-nope
  assert_aws_fails || return 1
  # wait for the failed GET entry specifically (requestPath contains the key)
  wait_audit 1 "aud103-b1/missing" || return 1
  audit_grep "aud103-b1/missing" | grep -q "404" || { echo "failed request audited without status_code 404: $(audit_grep 'aud103-b1/missing')"; return 1; }
  rm_bucket aud103-b1
}

test_aud104() {
  new_bucket "aud104-b1" || return 1
  local arn body code
  arn="$(get_first_arn)"
  [ -n "${arn}" ] || { echo "no target arn"; rm_bucket aud104-b1; return 1; }
  rc_run event add local/aud104-b1 "${arn}" --event s3:ObjectCreated:*
  assert_rc_ok || return 1
  events_reset
  rc_run admin kms key create local --name aud104k --tag env=ci --json
  assert_rc_ok || return 1
  wait_audit 1 "KmsKeyCreated|aud104k" || { echo "kms key create not audited"; rm_bucket aud104-b1; return 1; }
  sleep 3
  if notify_grep "Kms\|aud104k" | grep -q .; then echo "admin op leaked into bucket notifications"; return 1; fi
  rm_bucket aud104-b1
}

test_aud105() {
  new_bucket "aud105-b1" || return 1
  events_reset
  receiver_stop
  make_test_file ${TEST_TMP}/tier-f1 65536
  local i
  for i in 1 2 3; do
    aws_run s3api put-object --bucket aud105-b1 --key "a-${i}" --body ${TEST_TMP}/tier-f1
    assert_aws_ok || return 1
  done
  sleep 3
  receiver_start
  wait_audit 3 "aud105-b1" || return 1
  rm_bucket aud105-b1
}

test_aud106() {
  new_bucket "aud106-b1" || return 1
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket aud106-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  sleep 3
  # authorization headers must be redacted; target tokens must never appear
  if grep -qE '"authorization":"[^*]' "${AUDIT_EVENTS}" 2>/dev/null; then
    echo "audit leaks raw authorization header:"; grep -oE '"authorization":"[^"]*"' "${AUDIT_EVENTS}" | head -2; return 1
  fi
  if grep -q "${NOTIFY_TOKEN}\|${AUDIT_TOKEN}" "${AUDIT_EVENTS}" 2>/dev/null; then
    echo "audit leaks target auth token"; return 1
  fi
  if grep -qiE '"secretkey"[^:]*:[[:space:]]*"[^"*]' "${AUDIT_EVENTS}" 2>/dev/null; then
    echo "audit may contain sensitive fields:"; grep -iE 'secret' "${AUDIT_EVENTS}" | head -3; return 1
  fi
  rm_bucket aud106-b1
}

test_aud107() {
  new_bucket "aud107-b1" || return 1
  local body code
  body="$(admin_api PUT /rustfs/admin/v3/module-switches "" '{"notify_enabled":true,"audit_enabled":false}')"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "disable audit -> HTTP ${code}: ${body}"; return 1; }
  events_reset
  make_test_file ${TEST_TMP}/tier-f1 65536
  aws_run s3api put-object --bucket aud107-b1 --key f1 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  sleep 5
  if [ "$(audit_count)" -ne 0 ]; then echo "audit entries written while disabled: $(audit_grep .)"; return 1; fi
  body="$(admin_api PUT /rustfs/admin/v3/module-switches "" '{"notify_enabled":true,"audit_enabled":true}')"; code="$(admin_api_code)"
  [ "${code}" = "200" ] || { echo "enable audit -> HTTP ${code}: ${body}"; return 1; }
  wait_targets_ready || return 1
  aws_run s3api put-object --bucket aud107-b1 --key f2 --body ${TEST_TMP}/tier-f1
  assert_aws_ok || return 1
  wait_audit 1 "aud107-b1" || return 1
  rm_bucket aud107-b1
}

# ==================== main ====================

usage() {
  cat <<'USAGE'
rustfs-tier-test.sh — RustFS tier / event / audit suite (backlog #2024)

Usage:
  --all-topologies          Run single-single and single-multi
  --topology <topo>         Run one topology (repeatable)
  --version <ver>           RustFS release tag (default: 1.0.0-rc.4-preview.1)
  --package-url <url>       Explicit .deb URL
  --sha256 <hash>           Verify downloaded .deb checksum
  --rc-bin <path>           Exact rc binary to execute (default: resolve rc from PATH once)
  --expected-rc-sha256 <h>  Fail before setup unless the rc binary matches this SHA-256
  --artifacts-dir <path>    Persist structured case results, provenance, logs, and residue
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
  local opt all=0
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --all-topologies) all=1 ;;
      --topology) SELECTED_TOPOLOGIES+=("$1"); shift ;;
      --version) RUSTFS_VERSION="$1"; shift ;;
      --package-url) PACKAGE_URL="$1"; shift ;;
      --sha256) PACKAGE_SHA256="$1"; shift ;;
      --rc-bin) RC_BIN_REQUEST="$1"; shift ;;
      --expected-rc-sha256) RUSTFS_EXPECTED_RC_SHA256="$1"; shift ;;
      --artifacts-dir) ARTIFACTS_DIR="$1"; shift ;;
      --keep-env) KEEP_ENV=1 ;;
      --ssh-user) SSH_USER="$1"; shift ;;
      --log-file) LOG_FILE="$1"; shift ;;
      --dry-run) DRY_RUN=1 ;;
      -y|--yes) ASSUME_YES=1 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown option: ${opt} (see --help)" ;;
    esac
  done
  start_log_capture
  if [ "${all}" -eq 1 ]; then
    SELECTED_TOPOLOGIES=("${TOPO_SINGLE_SINGLE}" "${TOPO_SINGLE_MULTI}")
  fi
  [ "${#SELECTED_TOPOLOGIES[@]}" -gt 0 ] || die "no topology selected (--all-topologies / --topology)"

  local _p
  for _p in /root/.local/bin /home/*/.local/bin /usr/local/bin; do
    if [ -d "${_p}" ] && [[ ":${PATH}:" != *":${_p}:"* ]]; then PATH="${_p}:${PATH}"; fi
  done
  export PATH
  need_cmd ssh "SSH client"
  need_cmd aws "AWS CLI (s3api channel)"
  need_cmd jq "JSON processor"
  need_cmd python3 "Python 3 (path canonicalization and receiver)"
  need_cmd curl "HTTP client"
  need_cmd openssl "OpenSSL (SigV4)"
  need_cmd mosquitto_sub "mosquitto clients"
  need_cmd timeout "GNU timeout (bounded CLI/network operations)"
  init_artifacts
  resolve_rc_binary
  write_provenance
  verify_rc_digest
  log "nodes: ${NODES[*]}  package: $(resolve_package_url)"
  log "rc: path=${RC_BIN} version=${RC_VERSION} sha256=${RC_SHA256:-dry-run} config=${RC_CONFIG_DIR}"

  receiver_install
  local overall_fail=0 topo
  for topo in "${SELECTED_TOPOLOGIES[@]}"; do
    CURRENT_TOPOLOGY="${topo}"
    log "== topology: ${topo} =="
    env_setup "${topo}"
    PASS=0; FAIL=0; UNSUPPORTED=0; FAILED_TESTS=()
    run_all_tests
    results_summary
    record_topology_result "${topo}"
    if [ "${KEEP_ENV}" -eq 0 ]; then
      env_teardown "${topo}"
    fi
    if [ "${FAIL}" -gt 0 ]; then overall_fail=1; fi
  done
  final_summary
  [ "${overall_fail}" -eq 0 ] || exit 1
  log "all done"
}

if [ "${TIER_SOURCE_ONLY:-0}" != "1" ]; then
  main "$@"
fi
