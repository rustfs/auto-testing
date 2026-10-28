#!/usr/bin/env bash
# RustFS fault-tolerance (degradation) suite.
#
# Verifies read/write behavior under drive and node loss against the erasure
# coding contract, and captures health-endpoint responses at every tier.
# Expectations are derived from product source:
#   - crates/ecstore/src/config/storageclass.rs  default_parity_count():
#       4-5 drives -> parity 2 (EC:2); 8+ drives -> parity 4 (EC:4)
#   - crates/ecstore/src/layout/disks_layout.rs  16 drives -> one set of 16
#   - RUSTFS_STORAGE_CLASS_STANDARD=EC:N overrides parity
#   - distributed lock quorum gates (lock_quorum_unavailable degradation)
#
# Scenarios:
#   A  single-node 4 drives (SNMD on vm000)   EC:2  readq 2  writeq 3
#      revoke access to 1/2/3 drive roots, then restore
#   B  multi-node 4x1 (1 drive per node)      one set of 4, EC:2
#      stop 1/2/3 nodes, then restore
#   C  multi-node 4x4 (16 drives, one set)    EC:4  readq 12 writeq 12
#      stop 1 node (12 online == read-quorum boundary), stop 2 nodes, restore
#   C2 multi-node 4x4 with EC:8               readq 8  writeq 9  lock majority 9
#      2 nodes down = 8 online: reads satisfy the EC read quorum while the
#      lock majority is broken. This is the divergence window reported in the
#      2026-09 degradation report (reads 503 with lock_quorum_unavailable).
#
# Verdicts per probe:
#   pass             observed behavior matches the EC contract
#   known-divergence read quorum met but read refused (fails only in --strict)
#   UNEXPECTED       anything else; fails the suite
#   HARNESS_ERROR    the probe did not reach a trustworthy product verdict
#
# Destructive on the shared fleet: always run --cleanup before and after
# (the workflow does exactly that).
#
# Usage:
#   rustfs-fault-tolerance-test.sh --cleanup -y
#   rustfs-fault-tolerance-test.sh --all -y --package-url URL --log-file LOG
#   rustfs-fault-tolerance-test.sh --scenario A -y --package-url URL

set -euo pipefail

LOG_FILE=""
DRY_RUN=0
ASSUME_YES=0
PACKAGE_URL="${PACKAGE_URL:-}"
FT_STRICT="${FT_STRICT:-0}"
SETTLE_SECS="${FT_SETTLE_SECS:-45}"
RECOVERY_TIMEOUT_SECS="${FT_RECOVERY_TIMEOUT_SECS:-120}"

# ---------------------------------------------------------------- config ----
NODES_ENV="${RUSTFS_NODES:-}"
if [ -n "${NODES_ENV}" ]; then
  read -r -a NODES <<<"${NODES_ENV}"
else
  NODES=(vm000 vm001 vm002 vm003)
fi
SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${SSH_PORT}")
RUSTFS_SERVICE="${RUSTFS_SERVICE:-rustfs}"
RUSTFS_CONFIG_FILE="${RUSTFS_CONFIG_FILE:-/etc/default/rustfs}"

ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
REGION="us-east-1"

SNMD_NODE="${NODES[0]}"                       # ssh target for scenario A
SNMD_DRIVE_BASE="/data/rustfs"
SNMD_DRIVES=("/data/rustfs1/mnmd" "/data/rustfs2/mnmd" "/data/rustfs3/mnmd" "/data/rustfs4/mnmd")

# client-facing endpoints (must resolve from the runner host)
EP1="http://rustfs-node1:9000"
EP2="http://rustfs-node2:9000"
EP3="http://rustfs-node3:9000"
EP4="http://rustfs-node4:9000"
VOLUMES_SNMD="http://rustfs-node1:9000/data/rustfs{1...4}/mnmd"
VOLUMES_4X1="http://rustfs-node{1...4}:9000/data/rustfs1/mnmd"
VOLUMES_3X4="http://rustfs-node{1...3}:9000/data/rustfs{1...4}/mnmd"
VOLUMES_4X4="http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd"

BASELINE_BUCKET="ft-baseline"
BASELINE_OBJECTS=(ft-small-16k.bin ft-mid-1m.bin ft-big-8m.bin)
PROBE_PREFIX="ft-probe"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-ft.XXXXXX")"
EVIDENCE="${EVIDENCE_DIR:-${TMP}/evidence}"
mkdir -p "${EVIDENCE}"
REPORT_FILE="${REPORT_FILE:-${TMP}/report.md}"
RESULTS_FILE="${RESULTS_FILE:-${TMP}/results.json}"
RESULT_ROWS="${TMP}/results.jsonl"
HARNESS_ERROR_FILE="${TMP}/harness-errors.txt"
: > "${RESULT_ROWS}"
: > "${HARNESS_ERROR_FILE}"

# ---------------------------------------------------------------- helpers ---
log()  { printf '\033[1;36m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
  if [ "${ASSUME_YES}" -eq 1 ] || [ "${DRY_RUN}" -eq 1 ]; then return 0; fi
  printf '\033[1;33m[CONFIRM]\033[0m %s (y/N) ' "$1"
  read -r answer
  case "${answer}" in y|Y) return 0 ;; *) die "aborted" ;; esac
}

run_remote() {
  local node="$1" cmd="$2"
  if [ "${DRY_RUN}" -eq 1 ]; then log "DRY-RUN: ssh ${SSH_USER}@${node}: ${cmd}"; return 0; fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "${cmd}"
}

remote_all() {
  local cmd="$1" pids=() i=0 fail=0 pid
  for node in "${NODES[@]}"; do
    run_remote "${node}" "${cmd}" &
    pids[$i]=$!; i=$((i+1))
  done
  # explicit PIDs only: a bare wait also joins the `exec > >(tee ...)` child
  for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
  [ "${fail}" -eq 0 ] || die "one or more remote executions failed"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1 ($2); install it first"
}

UNEXPECTED=0
KNOWN_DIVERGENCE=0
HARNESS_ERRORS=0
SUITE_FINISHED=0
RESULTS_ENABLED=0
RESULT_COMPLETE=0
declare -a EXPECTED_CASES=()
declare -a SELECTED_SCENARIOS=()
declare -A EXPECTED_CASE_SET=()
declare -A SEEN_CASE_SET=()
declare -A OBJECT_SHA256=()

CASES_A=(A1/read A1/write A2/read A2/write A3/read A3/write A4/read A4/read-a1-probe A4/write)
CASES_B=(B1/read B1/read-node2 B1/write B2/read B2/read-node2 B2/write B3/read B3/write B4/read B4/read-b1-probe B4/write B5/recovery-traffic)
CASES_C=(C1/read C1/write C2/read C2/write C3/read C3/write)
CASES_C2=(C2-1/read C2-1/write C2-2/read C2-2/write C2-3/read C2-3/write)
CASES_D=(D1/read D1/write D2/read D2/write D3/read D3/write)
CASES_E=(E1/read E1/write E2/read E2/write E3/read E3/write)
CASES_F=(F1/read F1/write F2/ready F2/ready-fresh F2/ready-untouched F2/read F2/write F3/read F3/write)

register_expected_cases() {
  local case_id
  for case_id in "$@"; do
    [ -z "${EXPECTED_CASE_SET[${case_id}]+x}" ] || die "duplicate expected case: ${case_id}"
    EXPECTED_CASES+=("${case_id}")
    EXPECTED_CASE_SET["${case_id}"]=1
  done
}

register_scenarios() {
  local scenario
  for scenario in "$@"; do
    SELECTED_SCENARIOS+=("${scenario}")
    case "${scenario}" in
      A)  register_expected_cases "${CASES_A[@]}" ;;
      B)  register_expected_cases "${CASES_B[@]}" ;;
      C)  register_expected_cases "${CASES_C[@]}" ;;
      C2) register_expected_cases "${CASES_C2[@]}" ;;
      D)  register_expected_cases "${CASES_D[@]}" ;;
      E)  register_expected_cases "${CASES_E[@]}" ;;
      F)  register_expected_cases "${CASES_F[@]}" ;;
      *)  die "unknown scenario: ${scenario}" ;;
    esac
  done
}

add_harness_error() {
  local detail="$1"
  HARNESS_ERRORS=$((HARNESS_ERRORS + 1))
  printf '%s\n' "${detail}" >> "${HARNESS_ERROR_FILE}"
  warn "harness error: ${detail}"
}

record() { # record <verdict> <case-id> <detail>
  local verdict="$1" case_id="$2" detail="$3"
  if [ -z "${EXPECTED_CASE_SET[${case_id}]+x}" ]; then
    add_harness_error "unregistered case ${case_id}"
  fi
  if [ -n "${SEEN_CASE_SET[${case_id}]+x}" ]; then
    add_harness_error "duplicate case result ${case_id}"
  else
    SEEN_CASE_SET["${case_id}"]=1
  fi
  printf 'FT-CASE: %-28s verdict=%-16s %s\n' "${case_id}" "${verdict}" "${detail}" | tee -a "${REPORT_FILE}"
  jq -cn --arg case_id "${case_id}" --arg verdict "${verdict}" --arg detail "${detail}" \
    '{case_id:$case_id, verdict:$verdict, detail:$detail}' >> "${RESULT_ROWS}"
  case "${verdict}" in
    "UNEXPECTED")       UNEXPECTED=$((UNEXPECTED + 1)) ;;
    "known-divergence") KNOWN_DIVERGENCE=$((KNOWN_DIVERGENCE + 1)) ;;
    "HARNESS_ERROR")    add_harness_error "${case_id}: ${detail}" ;;
  esac
}

write_results() { # write_results <process-exit-code>
  local process_rc="$1" missing_file="${TMP}/missing-cases.txt" expected_file="${TMP}/expected-cases.txt"
  local seen_file="${TMP}/seen-cases.txt" selected_file="${TMP}/selected-scenarios.txt"
  local result_tmp="${RESULTS_FILE}.tmp" case_id scenario
  : > "${missing_file}"
  : > "${expected_file}"
  : > "${seen_file}"
  : > "${selected_file}"
  for scenario in "${SELECTED_SCENARIOS[@]}"; do printf '%s\n' "${scenario}" >> "${selected_file}"; done
  for case_id in "${EXPECTED_CASES[@]}"; do
    printf '%s\n' "${case_id}" >> "${expected_file}"
    [ -n "${SEEN_CASE_SET[${case_id}]+x}" ] || printf '%s\n' "${case_id}" >> "${missing_file}"
  done
  for case_id in "${!SEEN_CASE_SET[@]}"; do printf '%s\n' "${case_id}"; done | sort > "${seen_file}"
  RESULT_COMPLETE=0
  if [ "${SUITE_FINISHED}" -eq 1 ] && [ ! -s "${missing_file}" ] && [ "${HARNESS_ERRORS}" -eq 0 ]; then
    RESULT_COMPLETE=1
  fi
  mkdir -p "$(dirname "${RESULTS_FILE}")"
  jq -n \
    --argjson complete "$([ "${RESULT_COMPLETE}" -eq 1 ] && printf true || printf false)" \
    --argjson process_exit_code "${process_rc}" \
    --argjson unexpected "${UNEXPECTED}" \
    --argjson known_divergence "${KNOWN_DIVERGENCE}" \
    --argjson harness_error_count "${HARNESS_ERRORS}" \
    --arg auto_testing_sha "${AUTO_TESTING_SHA:-unknown}" \
    --slurpfile cases "${RESULT_ROWS}" \
    --rawfile selected "${selected_file}" \
    --rawfile expected "${expected_file}" \
    --rawfile seen "${seen_file}" \
    --rawfile missing "${missing_file}" \
    --rawfile harness_errors "${HARNESS_ERROR_FILE}" \
    '{schema_version:1, auto_testing_sha:$auto_testing_sha,
      selected_scenarios:($selected|split("\n")|map(select(length>0))),
      complete:$complete, process_exit_code:$process_exit_code,
      expected_cases:($expected|split("\n")|map(select(length>0))),
      seen_cases:($seen|split("\n")|map(select(length>0))),
      missing_cases:($missing|split("\n")|map(select(length>0))),
      harness_errors:($harness_errors|split("\n")|map(select(length>0))),
      counts:{expected:($expected|split("\n")|map(select(length>0))|length),
              seen:($seen|split("\n")|map(select(length>0))|length),
              unexpected:$unexpected, known_divergence:$known_divergence,
              harness_errors:$harness_error_count}, cases:$cases}' > "${result_tmp}"
  mv "${result_tmp}" "${RESULTS_FILE}"
}

finalize_results() {
  local process_rc="$1"
  [ "${RESULTS_ENABLED}" -eq 1 ] || return 0
  write_results "${process_rc}" || warn "failed to write ${RESULTS_FILE}"
}

trap 'finalize_results "$?"' EXIT

# aws s3 helper: s3op <operation-id> <endpoint> <s3api args...> -> S3_RC, S3_CODE
S3_RC=0
S3_CODE=""
s3op() {
  local operation="$1" ep="$2" safe_operation out err
  shift 2
  safe_operation="${operation//\//_}"
  out="${EVIDENCE}/s3-${safe_operation}.stdout"
  err="${EVIDENCE}/s3-${safe_operation}.stderr"
  if [ "${DRY_RUN}" -eq 1 ]; then log "DRY-RUN: aws s3api $* @ ${ep}"; S3_RC=0; S3_CODE=""; return 0; fi
  if AWS_EC2_METADATA_DISABLED=true \
     AWS_ACCESS_KEY_ID="${ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${SECRET_KEY}" AWS_DEFAULT_REGION="${REGION}" \
     aws s3api "$@" --endpoint-url "${ep}" --output json >"${out}" 2>"${err}"; then
    S3_RC=0; S3_CODE=""
  else
    S3_RC=$?
    # extraction must never kill the suite: with no match the grep pipeline
    # is empty and pipefail would exit 1 under set -e (observed: the first
    # degraded-mode aws failure aborted the whole suite with zero FT-CASEs)
    S3_CODE="$(grep -oE '\(([^)]+)\) when calling' "${err}" | head -1 | sed 's/ when calling//' | tr -d '()' || true)"
    [ -n "${S3_CODE}" ] || S3_CODE="unknown"
  fi
}

s3_harness_failed() {
  [ "${S3_RC}" -eq 252 ] || [ "${S3_RC}" -eq 253 ] || [ "${S3_CODE}" = "unknown" ]
}

s3_availability_failed() {
  case "${S3_CODE}" in
    SlowDown|SlowDownRead|ServiceUnavailable|InternalError|RequestTimeout|InsufficientStorage) return 0 ;;
    *) return 1 ;;
  esac
}
export AWS_EC2_METADATA_DISABLED=true
export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
export AWS_DEFAULT_REGION="${REGION}"

capture_health() { # capture_health <label> <endpoint-base>
  local label="$1" ep="$2" path out code
  [ "${DRY_RUN}" -eq 1 ] && return 0
  for path in "health" "health/ready" "minio/health/live"; do
    out="${EVIDENCE}/health-${label//\//_}-${path//\//_}"
    code="$(curl -s -o "${out}.body" -w '%{http_code}' --max-time 10 "${ep}/${path}" 2>/dev/null || echo 000)"
    printf '%s\n' "${code}" > "${out}.code"
  done
  log "health snapshot [${label}] saved"
}

settle() {
  log "settling ${SETTLE_SECS}s for failure/recovery detection"
  [ "${DRY_RUN}" -eq 1 ] || sleep "${SETTLE_SECS}"
}

# Recovery probes must wait until the probe endpoint re-admits the restored
# peers. systemd READY on a restarted node only means that node serves
# requests; EP1 keeps the peer's drives offline until its recovery monitor
# sees RUSTFS_DRIVE_RETURNING_SUCCESS_THRESHOLD (3) consecutive probes spaced
# RUSTFS_DRIVE_RETURNING_PROBE_INTERVAL_SECS (2s) apart. Probing inside that
# window measures the hysteresis, not the EC contract (rustfs/backlog#2618).
# A timeout is not fatal: the probes that follow report the non-convergence,
# and the elapsed time lands in the evidence directory either way.
await_write_quorum() { # await_write_quorum <label> <endpoint-base>
  local label="$1" ep="$2" started now state
  local out="${EVIDENCE}/recovery-${label//\//_}.secs"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ${label} waits for write quorum @ ${ep}"
    return 0
  fi
  started="$(date +%s)"
  while :; do
    state="$(curl -s --max-time 5 "${ep}/health/ready" 2>/dev/null | jq -r '
      if (.details.storage.writeQuorum | type) != "boolean" then "absent"
      elif .details.storage.writeQuorum and (.details.lock.ready != false) then "ready"
      else "waiting" end' 2>/dev/null || true)"
    now="$(date +%s)"
    case "${state}" in
      ready)
        printf '%s\n' "$((now - started))" > "${out}"
        log "[${label}] write quorum restored after $((now - started))s"
        return 0
        ;;
      absent)
        # Builds without readiness details: keep the historical settle.
        warn "[${label}] /health/ready has no storage.writeQuorum; settling instead"
        settle
        return 0
        ;;
    esac
    if [ "$((now - started))" -ge "${RECOVERY_TIMEOUT_SECS}" ]; then
      printf 'timeout\n' > "${out}"
      warn "[${label}] write quorum not restored within ${RECOVERY_TIMEOUT_SECS}s (last state: ${state:-unreachable})"
      return 0
    fi
    sleep 1
  done
}

expect_ready_stable() { # expect_ready_stable <case-id> <endpoint-base>
  # /health/ready must answer 200 on every poll across a sustained window.
  # The sequential-restart regression (rustfs#8669) surfaced as previously
  # healthy nodes flipping to 503 InsufficientReadQuorum right after a peer
  # restart, so a single spot check is not enough here.
  local case_id="$1" ep="$2" i code
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ${case_id} ready stability @ ${ep}"
    return 0
  fi
  for i in 1 2 3 4 5; do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${ep}/health/ready" 2>/dev/null || echo 000)"
    if [ "${code}" != "200" ]; then
      record "UNEXPECTED" "${case_id}" "ready round ${i}: /health/ready returned ${code} (want 200)"
      return 0
    fi
    if [ "${i}" -lt 5 ]; then sleep 2; fi
  done
  record "pass" "${case_id}" "ready stayed 200 across 5 rounds"
}

sustained_recovery_traffic() { # sustained_recovery_traffic <case-id> <endpoint> <rounds>
  # After all drives return, sustained write traffic plus readiness probes
  # must stay green: recovery starvation leaves remote disks stuck in
  # `Suspect` (rustfs#8270), which shows up as failed writes or a degraded
  # /health/ready under exactly this load.
  local case_id="$1" ep="$2" rounds="${3:-6}" i code key
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ${case_id} sustained traffic @ ${ep}"
    return 0
  fi
  local probe="${TMP}/probe.bin"
  for ((i = 1; i <= rounds; i++)); do
    key="${PROBE_PREFIX}-traffic-${i}"
    s3op "${case_id}" "${ep}" put-object --bucket "${BASELINE_BUCKET}" --key "${key}" --body "${probe}"
    if s3_harness_failed; then
      record "HARNESS_ERROR" "${case_id}" "traffic write round ${i}"
      return 0
    fi
    if [ "${S3_RC}" -ne 0 ]; then
      record "UNEXPECTED" "${case_id}" "write round ${i}: aws-cli-rc=${S3_RC} code=${S3_CODE}"
      return 0
    fi
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${ep}/health/ready" 2>/dev/null || echo 000)"
    if [ "${code}" != "200" ]; then
      record "UNEXPECTED" "${case_id}" "ready round ${i}: /health/ready returned ${code} (want 200)"
      return 0
    fi
    sleep 2
  done
  record "pass" "${case_id}" "${rounds} write+ready rounds OK after recovery"
}

expect_read() { # expect_read <case-id> <endpoint> <ok|fail> <key>
  local case_id="$1" ep="$2" expect="$3" key="$4"
  local got verdict detail actual_sha expected_sha outfile="${TMP}/get-${case_id//\//_}.out"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ${case_id} expects read=${expect} key=${key} @ ${ep}"
    return 0
  fi
  s3op "${case_id}" "${ep}" get-object --bucket "${BASELINE_BUCKET}" --key "${key}" "${outfile}"
  if s3_harness_failed; then
    record "HARNESS_ERROR" "${case_id}" "read ${key}: aws-cli-rc=${S3_RC} code=${S3_CODE}"
    return 0
  fi
  if [ "${S3_RC}" -eq 0 ]; then
    if [ -z "${OBJECT_SHA256[${key}]+x}" ]; then
      record "HARNESS_ERROR" "${case_id}" "read ${key}: no expected content digest registered"
      return 0
    fi
    actual_sha="$(sha256sum "${outfile}" | awk '{print $1}')"
    expected_sha="${OBJECT_SHA256[${key}]}"
    if [ "${actual_sha}" = "${expected_sha}" ]; then
      got="ok"; detail="sha256=${actual_sha}"
    else
      record "UNEXPECTED" "${case_id}" \
        "read ${key}: got=corrupt expected-sha256=${expected_sha} actual-sha256=${actual_sha}"
      return 0
    fi
  else
    got="fail"; detail="aws-cli-rc=${S3_RC} code=${S3_CODE}"
  fi
  if [ "${got}" = "ok" ] && [ "${expect}" = "ok" ]; then
    verdict="pass"
  elif [ "${expect}" = "ok" ] && [ "${got}" = "fail" ]; then
    if s3_availability_failed; then
      verdict="known-divergence"
      detail+=" (read quorum met but read refused; article-reported lock-gated read path)"
    else
      verdict="UNEXPECTED"
    fi
  elif [ "${expect}" = "fail" ] && [ "${got}" = "fail" ] && s3_availability_failed; then
    verdict="pass"
  else
    verdict="UNEXPECTED"
  fi
  record "${verdict}" "${case_id}" "read ${key}: got=${got} ${detail}"
}

expect_write() { # expect_write <case-id> <endpoint> <ok|fail> <key>
  local case_id="$1" ep="$2" expect="$3" key="$4"
  local got verdict detail probe="${TMP}/probe.bin"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ${case_id} expects write=${expect} key=${key} @ ${ep}"
    return 0
  fi
  head -c 4096 /dev/zero > "${probe}"
  s3op "${case_id}" "${ep}" put-object --bucket "${BASELINE_BUCKET}" --key "${key}" --body "${probe}"
  if s3_harness_failed; then
    record "HARNESS_ERROR" "${case_id}" "write ${key}: aws-cli-rc=${S3_RC} code=${S3_CODE}"
    return 0
  fi
  if [ "${S3_RC}" -eq 0 ]; then
    got="ok"; detail=""
    OBJECT_SHA256["${key}"]="$(sha256sum "${probe}" | awk '{print $1}')"
  else
    got="fail"; detail="aws-cli-rc=${S3_RC} code=${S3_CODE}"
  fi
  if [ "${got}" = "ok" ] && [ "${expect}" = "ok" ]; then
    verdict="pass"
  elif [ "${got}" = "fail" ] && [ "${expect}" = "fail" ] && s3_availability_failed; then
    verdict="pass"
  else
    verdict="UNEXPECTED"
  fi
  record "${verdict}" "${case_id}" "write ${key}: got=${got} ${detail}"
}

seed_baseline() { # seed_baseline <endpoint>
  local ep="$1" o
  log "seeding baseline bucket + objects on ${ep}"
  s3op "setup-create-bucket" "${ep}" create-bucket --bucket "${BASELINE_BUCKET}"
  if s3_harness_failed; then
    add_harness_error "create-bucket: aws-cli-rc=${S3_RC} code=${S3_CODE}"
    die "create-bucket harness failure"
  fi
  if [ "${S3_RC}" -ne 0 ] && [ "${S3_CODE}" != "BucketAlreadyOwnedByYou" ] && [ "${S3_CODE}" != "BucketAlreadyExists" ]; then
    die "create-bucket failed: ${S3_CODE}"
  fi
  head -c 16384   /dev/urandom > "${TMP}/ft-small-16k.bin"
  head -c 1048576 /dev/urandom > "${TMP}/ft-mid-1m.bin"
  head -c 8388608 /dev/urandom > "${TMP}/ft-big-8m.bin"
  for o in "${BASELINE_OBJECTS[@]}"; do
    OBJECT_SHA256["${o}"]="$(sha256sum "${TMP}/${o}" | awk '{print $1}')"
    s3op "setup-seed-${o}" "${ep}" put-object --bucket "${BASELINE_BUCKET}" --key "${o}" --body "${TMP}/${o}"
    if s3_harness_failed; then
      add_harness_error "baseline put ${o}: aws-cli-rc=${S3_RC} code=${S3_CODE}"
      die "baseline put ${o} harness failure"
    fi
    [ "${S3_RC}" -eq 0 ] || die "baseline put ${o} failed: ${S3_CODE}"
  done
}

# cluster lifecycle ----------------------------------------------------------
install_and_start() { # install_and_start <volumes> [extra env line]
  local volumes="$1" extra_env="${2:-}" node
  [ -n "${PACKAGE_URL}" ] || die "--package-url is required for test runs"
  log "installing package on ${NODES[*]}"
  remote_all "set -e; sudo -n systemctl stop ${RUSTFS_SERVICE} 2>/dev/null || true; sudo -n dpkg --configure -a 2>/dev/null || true; curl -fsSL --retry 3 -o /tmp/rustfs-ft.deb '${PACKAGE_URL}' && sudo -n env DEBIAN_FRONTEND=noninteractive dpkg -i --force-confdef --force-confnew /tmp/rustfs-ft.deb && sudo -n systemctl daemon-reload"
  for node in "${NODES[@]}"; do
    log "${node}: writing ${RUSTFS_CONFIG_FILE}"
    {
      printf 'RUSTFS_ACCESS_KEY=%q\n' "${ACCESS_KEY}"
      printf 'RUSTFS_SECRET_KEY=%q\n' "${SECRET_KEY}"
      printf 'RUSTFS_VOLUMES=%q\n' "${volumes}"
      printf 'RUSTFS_ADDRESS=":9000"\n'
      printf 'RUSTFS_CONSOLE_ADDRESS=":9001"\n'
      printf 'RUSTFS_CONSOLE_ENABLE=false\n'
      printf 'RUSTFS_OBS_LOGGER_LEVEL=error\n'
      [ -n "${extra_env}" ] && printf '%s\n' "${extra_env}"
      :
    } > "${TMP}/envfile"
    [ "${DRY_RUN}" -eq 1 ] && continue
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "sudo -n tee ${RUSTFS_CONFIG_FILE} >/dev/null" < "${TMP}/envfile"
    run_remote "${node}" 'sudo -n mkdir -p /data/rustfs{1..4}/mnmd && for i in 1 2 3 4; do sudo -n chown rustfs:rustfs "/data/rustfs${i}" "/data/rustfs${i}/mnmd"; done 2>/dev/null || true'
  done
  if [ "${volumes}" = "${VOLUMES_SNMD}" ]; then
    log "single-node topology: starting ${RUSTFS_SERVICE} on ${SNMD_NODE} only"
    if run_remote "${SNMD_NODE}" "sudo -n systemctl start ${RUSTFS_SERVICE}"; then
      :
    else
      dump_service_diagnostics "${SNMD_NODE}"
      die "start failed on ${SNMD_NODE}"
    fi
  else
    log "starting ${RUSTFS_SERVICE} on ${NODES[*]}"
    local start_pids=() start_i=0 start_pid start_fail=0
    for node in "${NODES[@]}"; do
      run_remote "${node}" "sudo -n systemctl start ${RUSTFS_SERVICE}" &
      start_pids[${start_i}]=$!; start_i=$((start_i+1))
    done
    for start_pid in "${start_pids[@]}"; do wait "${start_pid}" || start_fail=1; done
    if [ "${start_fail}" -ne 0 ]; then
      dump_service_diagnostics "${NODES[@]}"
      die "cluster start failed on one or more nodes"
    fi
  fi
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: skipping cluster readiness polling"
    return 0
  fi
  local waited=0
  while [ "${waited}" -lt 180 ]; do
    if curl -fsS --max-time 5 "${EP1}/minio/health/live" >/dev/null 2>&1; then
      log "cluster endpoint ready"; return 0
    fi
    sleep 5; waited=$((waited + 5))
  done
  die "cluster endpoint not ready after 180s"
}

dump_service_diagnostics() { # best-effort journal tail after a failed service start
  local node
  for node in "$@"; do
    log "collecting ${RUSTFS_SERVICE} journal tail on ${node}"
    run_remote "${node}" "sudo -n journalctl -u ${RUSTFS_SERVICE} -n 80 --no-pager 2>&1 | tail -n 80" || true
  done
}

stop_node()  { log "stopping ${RUSTFS_SERVICE} on $1"; run_remote "$1" "sudo -n systemctl stop ${RUSTFS_SERVICE}"; }
start_nodes() {
  local pids=() i=0 fail=0 node pid
  for node in "$@"; do
    log "starting ${RUSTFS_SERVICE} on ${node} without waiting for READY"
    run_remote "${node}" "sudo -n systemctl start --no-block ${RUSTFS_SERVICE}" &
    pids[$i]=$!; i=$((i + 1))
  done
  for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
  [ "${fail}" -eq 0 ] || die "one or more node starts could not be submitted"

  pids=(); i=0; fail=0
  for node in "$@"; do
    run_remote "${node}" \
      "for attempt in \$(seq 1 36); do sudo -n systemctl is-active --quiet ${RUSTFS_SERVICE} && exit 0; sleep 5; done; exit 1" &
    pids[$i]=$!; i=$((i + 1))
  done
  for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
  if [ "${fail}" -ne 0 ]; then
    dump_service_diagnostics "$@"
    die "one or more restored nodes did not become active within 180s"
  fi
}

drive_state_file() {
  local drive="$1" parent
  parent="$(basename "$(dirname "${drive}")")"
  printf '/run/rustfs-ft-%s.mode\n' "${parent}"
}

drop_drive() {
  local drive="$1" state
  state="$(drive_state_file "${drive}")"
  log "${SNMD_NODE}: revoking access to ${drive}"
  # RustFS pins the opened mount through /proc/self/fd. Changing the inode mode
  # reaches that lease; covering the configured path with a new mount does not.
  run_remote "${SNMD_NODE}" \
    "sudo -n sh -c 'set -eu; [ -d \"${drive}\" ] && [ ! -L \"${drive}\" ]; if [ ! -e \"${state}\" ]; then stat -Lc \"%a\" \"${drive}\" > \"${state}\"; fi; chmod 000 \"${drive}\"; [ \"\$(stat -Lc \"%a\" \"${drive}\")\" = \"0\" ]'"
}

restore_drive() {
  local drive="$1" state
  state="$(drive_state_file "${drive}")"
  log "${SNMD_NODE}: restoring access to ${drive}"
  run_remote "${SNMD_NODE}" \
    "sudo -n sh -c 'set -eu; [ -d \"${drive}\" ] && [ ! -L \"${drive}\" ] && [ -f \"${state}\" ]; mode=\$(cat \"${state}\"); case \"\${mode}\" in \"\"|*[!0-7]*) exit 1;; esac; chmod \"\${mode}\" \"${drive}\"; unlink \"${state}\"'"
}

clear_drive_faults() {
  local drive state
  for drive in "${SNMD_DRIVES[@]}"; do
    state="$(drive_state_file "${drive}")"
    run_remote "${SNMD_NODE}" \
      "sudo -n sh -c 'set -eu; if [ \"\$(findmnt -n -o FSTYPE -M \"${drive}\" 2>/dev/null || true)\" = tmpfs ]; then umount \"${drive}\"; fi; if [ -f \"${state}\" ]; then [ -d \"${drive}\" ] && [ ! -L \"${drive}\" ]; mode=\$(cat \"${state}\"); case \"\${mode}\" in \"\"|*[!0-7]*) exit 1;; esac; chmod \"\${mode}\" \"${drive}\"; unlink \"${state}\"; fi'"
  done
}

do_cleanup() {
  log "cleanup: stopping services, restoring drive access, purging data"
  remote_all "sudo -n systemctl stop ${RUSTFS_SERVICE} 2>/dev/null; sudo -n systemctl stop rustfs-rep2 2>/dev/null; true"
  clear_drive_faults
  remote_all "sudo -n rm -rf /data/rustfs{1..4}/mnmd /data/rustfs-rep2 2>/dev/null; true"
  log "cleanup complete"
}

# ---------------------------------------------------------------- scenario A
scenario_a() {
  log "=== A: single-node 4 drives on ${SNMD_NODE}, EC:2 (readq 2, writeq 3) ==="
  install_and_start "${VOLUMES_SNMD}"
  seed_baseline "${EP1}"

  drop_drive "${SNMD_DRIVES[3]}"; settle              # A1: 3 drives online
  capture_health "A1" "${EP1}"
  expect_read  "A1/read"          "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "A1/write"         "${EP1}" "ok"   "${PROBE_PREFIX}-a1"

  drop_drive "${SNMD_DRIVES[2]}"; settle              # A2: 2 online == readq
  capture_health "A2" "${EP1}"
  expect_read  "A2/read"          "${EP1}" "ok"   "ft-mid-1m.bin"
  expect_write "A2/write"         "${EP1}" "fail" "${PROBE_PREFIX}-a2"

  drop_drive "${SNMD_DRIVES[1]}"; settle              # A3: 1 online
  capture_health "A3" "${EP1}"
  expect_read  "A3/read"          "${EP1}" "fail" "ft-big-8m.bin"
  expect_write "A3/write"         "${EP1}" "fail" "${PROBE_PREFIX}-a3"

  restore_drive "${SNMD_DRIVES[1]}"; restore_drive "${SNMD_DRIVES[2]}"; restore_drive "${SNMD_DRIVES[3]}"
  settle                                              # A4: recovery
  capture_health "A4-recovered" "${EP1}"
  expect_read  "A4/read"          "${EP1}" "ok" "ft-mid-1m.bin"
  expect_read  "A4/read-a1-probe" "${EP1}" "ok" "${PROBE_PREFIX}-a1"
  expect_write "A4/write"         "${EP1}" "ok" "${PROBE_PREFIX}-a4"
  do_cleanup
}

# ---------------------------------------------------------------- scenario B
scenario_b() {
  log "=== B: multi-node 4x1 (one drive per node), EC:2 (readq 2, writeq 3) ==="
  install_and_start "${VOLUMES_4X1}"
  seed_baseline "${EP1}"

  stop_node "${NODES[3]}"; settle                     # B1: 3 drives online
  capture_health "B1" "${EP1}"
  expect_read  "B1/read"       "${EP1}" "ok" "ft-small-16k.bin"
  expect_read  "B1/read-node2" "${EP2}" "ok" "ft-small-16k.bin"
  expect_write "B1/write"      "${EP1}" "ok" "${PROBE_PREFIX}-b1"

  stop_node "${NODES[2]}"; settle                     # B2: 2 online == readq (article case)
  capture_health "B2" "${EP1}"
  expect_read  "B2/read"       "${EP1}" "ok"   "ft-mid-1m.bin"
  expect_read  "B2/read-node2" "${EP2}" "ok"   "ft-mid-1m.bin"
  expect_write "B2/write"      "${EP1}" "fail" "${PROBE_PREFIX}-b2"

  stop_node "${NODES[1]}"; settle                     # B3: 1 online
  capture_health "B3" "${EP1}"
  expect_read  "B3/read"  "${EP1}" "fail" "ft-big-8m.bin"
  expect_write "B3/write" "${EP1}" "fail" "${PROBE_PREFIX}-b3"

  start_nodes "${NODES[1]}" "${NODES[2]}" "${NODES[3]}" # B4: recovery
  await_write_quorum "B4" "${EP1}"
  capture_health "B4-recovered" "${EP1}"
  expect_read  "B4/read"          "${EP1}" "ok" "ft-mid-1m.bin"
  expect_read  "B4/read-b1-probe" "${EP1}" "ok" "${PROBE_PREFIX}-b1"
  expect_write "B4/write"         "${EP1}" "ok" "${PROBE_PREFIX}-b4"
  # B5: rustfs#8270 — sustained traffic after recovery must keep writes and
  # readiness green; drives must not get stuck in `Suspect`.
  sustained_recovery_traffic "B5/recovery-traffic" "${EP1}" 6
  do_cleanup
}

scenario_f() {
  # rustfs#8669: restarting members one at a time must never cost the
  # cluster more than the one member that is down. The regression had peers
  # keep treating the just-restarted member as unreachable, so stopping the
  # NEXT member left observers effectively two members short: untouched
  # nodes answered /health/ready 503 with internal InsufficientReadQuorum.
  log "=== F: sequential node restarts on multi-node 4x1, EC:2 (readq 2, writeq 3) ==="
  seed_baseline "${EP1}"
  # F1: restart one member and let it rejoin fully.
  stop_node "${NODES[1]}"; settle
  start_nodes "${NODES[1]}"
  await_write_quorum "F1" "${EP1}"
  capture_health "F1-restarted" "${EP1}"
  expect_read  "F1/read"  "${EP1}" "ok" "ft-mid-1m.bin"
  expect_write "F1/write" "${EP1}" "ok" "${PROBE_PREFIX}-f1"

  # F2: stop the next member IMMEDIATELY after F1's restart converged.
  # NODES[1] maps to rustfs-node2 (EP2, freshly restarted), NODES[2] to
  # rustfs-node3, NODES[3] to rustfs-node4 (untouched).
  stop_node "${NODES[2]}"; settle
  capture_health "F2" "${EP1}"
  expect_ready_stable "F2/ready"           "${EP1}"
  expect_ready_stable "F2/ready-fresh"     "${EP2}"
  expect_ready_stable "F2/ready-untouched" "${EP4}"
  expect_read  "F2/read"  "${EP4}" "ok" "ft-mid-1m.bin"
  expect_write "F2/write" "${EP1}" "ok" "${PROBE_PREFIX}-f2"

  # F3: bring the stopped member back; everything must converge.
  start_nodes "${NODES[2]}"
  await_write_quorum "F3" "${EP1}"
  capture_health "F3-recovered" "${EP1}"
  expect_read  "F3/read"  "${EP1}" "ok" "ft-mid-1m.bin"
  expect_write "F3/write" "${EP1}" "ok" "${PROBE_PREFIX}-f3"
  do_cleanup
}

# ---------------------------------------------------------------- scenario C
scenario_c() {
  log "=== C: multi-node 4x4 (16 drives, one set), EC:4 (readq 12, writeq 12) ==="
  install_and_start "${VOLUMES_4X4}"
  seed_baseline "${EP1}"

  stop_node "${NODES[3]}"; settle                     # C1: 12 online == readq
  capture_health "C1" "${EP1}"
  expect_read  "C1/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "C1/write" "${EP1}" "ok" "${PROBE_PREFIX}-c1"

  stop_node "${NODES[2]}"; settle                     # C2: 8 online
  capture_health "C2" "${EP1}"
  expect_read  "C2/read"  "${EP1}" "fail" "ft-mid-1m.bin"
  expect_write "C2/write" "${EP1}" "fail" "${PROBE_PREFIX}-c2"

  start_nodes "${NODES[2]}" "${NODES[3]}"
  await_write_quorum "C3" "${EP1}"
  capture_health "C3-recovered" "${EP1}"
  expect_read  "C3/read"  "${EP1}" "ok" "ft-small-16k.bin"
  expect_write "C3/write" "${EP1}" "ok" "${PROBE_PREFIX}-c3"
  do_cleanup
}

# --------------------------------------------------------------- scenario C2
scenario_c2() {
  log "=== C2: multi-node 4x4 with EC:8 (readq 8, writeq 9, lock majority 9) ==="
  log "=== C2 tier '2 nodes down' is the article divergence window ==="
  install_and_start "${VOLUMES_4X4}" 'RUSTFS_STORAGE_CLASS_STANDARD="EC:8"'
  seed_baseline "${EP1}"

  stop_node "${NODES[3]}"; settle                     # C2-1: 12 online
  capture_health "C2-1" "${EP1}"
  expect_read  "C2-1/read"  "${EP1}" "ok" "ft-small-16k.bin"
  expect_write "C2-1/write" "${EP1}" "ok" "${PROBE_PREFIX}-c2-1"

  stop_node "${NODES[2]}"; settle                     # C2-2: 8 online == readq, lock broken
  capture_health "C2-2" "${EP1}"
  expect_read  "C2-2/read"  "${EP1}" "ok"   "ft-mid-1m.bin"
  expect_write "C2-2/write" "${EP1}" "fail" "${PROBE_PREFIX}-c2-2"

  start_nodes "${NODES[2]}" "${NODES[3]}"
  await_write_quorum "C2-3" "${EP1}"
  capture_health "C2-3-recovered" "${EP1}"
  expect_read  "C2-3/read"  "${EP1}" "ok" "ft-mid-1m.bin"
  expect_write "C2-3/write" "${EP1}" "ok" "${PROBE_PREFIX}-c2-3"
  do_cleanup
}

# --------------------------------------------------------------- scenario D
# Multi-NODE outage on the 4x1 fleet (EC:2, readq 2 / writeq 3) — node-level
# failure, not drive-level. Mirrors the migration-review finding that MinIO
# serves degraded reads with two of four nodes down while RustFS was reported
# to brick. The article notes this may be intentional design; the case exists
# to make the behavior explicit and regression-tracked.
scenario_d() {
  log "=== D: multi-node 4x1 outage, EC:2 (readq 2, writeq 3) ==="
  install_and_start "${VOLUMES_4X1}"
  seed_baseline "${EP1}"

  stop_node "${NODES[2]}"; settle                     # D1: 3 nodes online
  capture_health "D1" "${EP1}"
  expect_read  "D1/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "D1/write" "${EP1}" "ok"   "${PROBE_PREFIX}-d1"

  stop_node "${NODES[3]}"; settle                     # D2: 2 nodes == readq, writeq lost
  capture_health "D2" "${EP1}"
  expect_read  "D2/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "D2/write" "${EP1}" "fail" "${PROBE_PREFIX}-d2"

  start_nodes "${NODES[2]}" "${NODES[3]}"
  await_write_quorum "D3" "${EP1}"
  capture_health "D3-recovered" "${EP1}"
  expect_read  "D3/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "D3/write" "${EP1}" "ok"   "${PROBE_PREFIX}-d3"
  do_cleanup
}

# --------------------------------------------------------------- scenario E
# Multi-NODE outage on the 3x4 fleet (12 drives one set, EC:4, readq 8 /
# writeq 8): the 1.0 article-style degraded boundary one node earlier than
# the 4x1 case - 3 nodes is the minimum for this parity scheme.
scenario_e() {
  log "=== E: multi-node 3x4 (12 drives one set), EC:4 (readq 8, writeq 8) ==="
  install_and_start "${VOLUMES_3X4}"
  seed_baseline "${EP1}"

  stop_node "${NODES[2]}"; settle                     # E1: 8 drives == readq
  capture_health "E1" "${EP1}"
  expect_read  "E1/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "E1/write" "${EP1}" "ok"   "${PROBE_PREFIX}-e1"

  stop_node "${NODES[1]}"; settle                     # E2: 4 drives < readq
  capture_health "E2" "${EP1}"
  expect_read  "E2/read"  "${EP1}" "fail" "ft-big-8m.bin"
  expect_write "E2/write" "${EP1}" "fail" "${PROBE_PREFIX}-e2"

  start_nodes "${NODES[1]}" "${NODES[2]}"; settle
  await_write_quorum "E3" "${EP1}"
  capture_health "E3-recovered" "${EP1}"
  expect_read  "E3/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "E3/write" "${EP1}" "ok"   "${PROBE_PREFIX}-e3"
  do_cleanup
}

# ------------------------------------------------------------------- main ---
usage() {
  cat <<EOF
Usage: $0 [options]
  --cleanup              stop services, unhide drives, purge data, then exit
  --scenario A|B|C|C2|D|E|F|M  run one scenario (M = matrix sweep)
  --all                  run all scenarios (A, B, C, C2, D, E)
  --package-url URL      .deb to install (required for test runs)
  --results-file FILE    write the machine-readable completeness result
  -y                     assume yes
  --dry-run              print planned actions only
  --log-file FILE        also tee output into FILE
  --strict               known-divergence also fails the suite (FT_STRICT=1)
  --matrix               full topology x EC outage sweep (38 combinations)
  --matrix-topo KEY [EC] sweep one topology (like 4x2), optionally one EC level
Environment: RUSTFS_NODES, RUSTFS_SSH_USER, FT_SETTLE_SECS, FT_RECOVERY_TIMEOUT_SECS,
             FT_STRICT, MATRIX_EC
EOF
}

# ── matrix: topology x EC exhaustive outage sweep ───────────────────────────
# For each topology (nodes x drives-per-node) and each EC level P in 1..N/2:
#   install with RUSTFS_STORAGE_CLASS_STANDARD=EC:P + seed, full-capacity
#   read/write asserts, then stop nodes one by one. Per-phase expectations
#   derive from the product quorum arithmetic (WriteLayout::from_parity):
#     online drives D >= data (N-P)                   -> reads OK
#     online drives D >= data + (data == parity)      -> writes OK
#     otherwise                                       -> reads and writes fail
# Case ids: M-<topo>EC<P>-full | -n<k> | -rec. Register new topologies by
# appending to MATRIX_TOPOS.
MATRIX_TOPOS=("1x2" "1x4" "2x2" "2x4" "3x2" "3x4" "4x1" "4x2" "4x3" "4x4")

# One ellipses expression spanning every node, so each topology is a single
# pool with one N-drive erasure set. Space-separated per-node expressions
# would make every node its own server pool (rustfs/backlog#2618): stopping a
# node then takes a whole pool offline, and single-drive pools (4x1) cannot
# start at all.
matrix_volumes_expr() { # <nodes> <drives-per>
  local host="rustfs-node1" drive="rustfs1"
  [ "$1" -gt 1 ] && host="rustfs-node{1...$1}"
  [ "$2" -gt 1 ] && drive="rustfs{1...$2}"
  printf 'http://%s:9000/data/%s/mnmd' "${host}" "${drive}"
}

matrix_run_case() { # <topo-key> <nodes> <drives-per> <parity>
  local key="$1" nodes="$2" per="$3" parity="$4"
  # Split declarations: in one `local` line the later expansion would read
  # the earlier variable before its assignment lands (set -u unbound).
  local total=$(( nodes * per ))
  local data=$(( total - parity ))
  local readq=$(( data ))
  local writeq=$(( data + (data == parity ? 1 : 0) ))
  local tag="M-${key}EC${parity}"
  log "matrix[${tag}]: total=${total} readq=${readq} writeq=${writeq}"
  # Register this combination's case ids up front: unregistered verdicts
  # would count as harness errors and invalidate the results file.
  local mc=() k
  mc+=("${tag}-full/read" "${tag}-full/write")
  for ((k = nodes - 1; k >= 1; k--)); do
    mc+=("${tag}-n${k}/read" "${tag}-n${k}/write")
  done
  mc+=("${tag}-rec/read" "${tag}-rec/write")
  register_expected_cases "${mc[@]}"
  install_and_start "$(matrix_volumes_expr "${nodes}" "${per}")" "RUSTFS_STORAGE_CLASS_STANDARD=\"EC:${parity}\""
  seed_baseline "${EP1}"

  # full capacity
  expect_read  "${tag}-full/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "${tag}-full/write" "${EP1}" "ok"   "${PROBE_PREFIX}-${tag}-f"

  # progressive node outages: stop the highest-index node first and probe
  # after each stop, so phase k is asserted with exactly k*per drives online.
  # NODES[0] never stops and stays the probe endpoint (EP1). Below readq the
  # probes assert the expected unavailability instead of flagging an anomaly -
  # quorum math says those states must fail, and a pass there is the finding.
  local k online exp_read exp_write
  for ((k = nodes - 1; k >= 1; k--)); do
    stop_node "${NODES[$((k))]}"
    settle
    online=$(( k * per ))
    exp_read="fail"; exp_write="fail"
    if [ "${online}" -ge "${writeq}" ]; then exp_read="ok"; exp_write="ok"
    elif [ "${online}" -ge "${readq}" ]; then exp_read="ok"; fi
    expect_read  "${tag}-n${k}/read"  "${EP1}" "${exp_read}"  "ft-small-16k.bin"
    expect_write "${tag}-n${k}/write" "${EP1}" "${exp_write}" "${PROBE_PREFIX}-${tag}-n${k}"
  done

  # recovery: hand start_nodes the whole stopped set in one call. It starts
  # its arguments concurrently and dies on the first failed wait, so per-node
  # calls leave later nodes unstarted while an earlier one waits with its
  # peers still down. NODES[0] never stops and stays the probe endpoint.
  local idx restart=()
  for ((idx = 1; idx < nodes; idx++)); do restart+=("${NODES[$((idx))]}"); done
  if [ "${#restart[@]}" -gt 0 ]; then start_nodes "${restart[@]}"; fi
  settle
  await_write_quorum "${tag}-rec" "${EP1}"
  expect_read  "${tag}-rec/read"  "${EP1}" "ok" "ft-small-16k.bin"
  expect_write "${tag}-rec/write" "${EP1}" "ok" "${PROBE_PREFIX}-${tag}-rec"
  do_cleanup
}

run_matrix() { # [topo-filter like 4x2] - sweep EC 1..N/2 per topology (38 combos)
  local filter="${1:-}" spec nodes_n per parity combos=0
  for spec in "${MATRIX_TOPOS[@]}"; do
    [ -n "${filter}" ] && [ "${spec}" != "${filter}" ] && continue
    nodes_n="${spec%x*}"
    per="${spec#*x}"
    for ((parity = 1; parity * 2 <= nodes_n * per; parity++)); do
      [ -n "${MATRIX_EC}" ] && [ "${parity}" != "${MATRIX_EC}" ] && continue
      matrix_run_case "${spec}" "${nodes_n}" "${per}" "${parity}"
      combos=$((combos + 1))
    done
  done
  # A filter typo would otherwise run nothing and report a clean sweep.
  [ "${combos}" -gt 0 ] || die "matrix filters matched no combinations (topo=${filter:-any} EC=${MATRIX_EC:-any})"
}

main() {
  [ "$#" -eq 0 ] && { usage; exit 0; }
  local opt scenario="" all=0 cleanup=0 matrix=0 matrix_topo="" MATRIX_EC="${MATRIX_EC:-}"
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --cleanup)     cleanup=1 ;;
      --scenario)    scenario="$1"; shift ;;
      --all)         all=1 ;;
      --package-url) PACKAGE_URL="$1"; shift ;;
      --results-file) RESULTS_FILE="$1"; shift ;;
      -y)            ASSUME_YES=1 ;;
      --dry-run)     DRY_RUN=1 ;;
      --log-file)    LOG_FILE="$1"; shift ;;
      --strict)      FT_STRICT=1 ;;
      --matrix)      matrix=1 ;;
      --matrix-topo)
        [ "$#" -ge 1 ] || die "--matrix-topo requires a topology like 4x2"
        case "$1" in
          ''|-*) die "--matrix-topo requires a topology like 4x2" ;;
        esac
        matrix_topo="$1"; shift
        # A purely numeric follower is the optional EC level; anything else is
        # the next option and must not be swallowed as EC.
        case "${1:-}" in
          ''|*[!0-9]*) ;;
          *) MATRIX_EC="$1"; shift ;;
        esac
        matrix=1 ;;
      -h|--help)     usage; exit 0 ;;
      *)             die "unknown option: ${opt}" ;;
    esac
  done
  if [ -n "${LOG_FILE}" ]; then
    mkdir -p "$(dirname "${LOG_FILE}")"
    exec > >(tee -a "${LOG_FILE}") 2>&1
  fi

  if [ "${cleanup}" -eq 1 ]; then do_cleanup; exit 0; fi

  need_cmd ssh "SSH client"
  need_cmd curl "HTTP client"
  need_cmd jq "JSON processor"
  need_cmd aws "AWS CLI (s3api)"
  if [ "${all}" -eq 1 ] || [ -n "${scenario}" ]; then
    [ -n "${PACKAGE_URL}" ] || die "--package-url is required for test runs"
  fi

  RESULTS_ENABLED=1
  printf 'FT-REPORT: fault-tolerance suite\n' > "${REPORT_FILE}"
  if [ "${all}" -eq 1 ]; then
    # E must be registered, not just executed: unregistered verdicts land in
    # harness_errors and fail the evidence contract (#96 ran scenario_e via
    # --all but never registered its cases).
    register_scenarios A B C C2 D E F
    scenario_a; scenario_b; scenario_c; scenario_c2; scenario_d; scenario_e; scenario_f
  elif [ "${matrix}" -eq 1 ]; then
    run_matrix "${matrix_topo}"
  elif [ -n "${scenario}" ]; then
    register_scenarios "${scenario}"
    case "${scenario}" in
      A)  scenario_a  ;;
      B)  scenario_b  ;;
      C)  scenario_c  ;;
      C2) scenario_c2 ;;
      D)  scenario_d  ;;
      E)  scenario_e  ;;
      F)  scenario_f  ;;
      M)  run_matrix "${matrix_topo}" ;;
      *)  die "unknown scenario: ${scenario}" ;;
    esac
  else
    usage; exit 0
  fi

  if [ "${DRY_RUN}" -eq 1 ]; then
    RESULTS_ENABLED=0
    log "fault-tolerance dry-run complete"
    return 0
  fi
  SUITE_FINISHED=1
  write_results 0
  printf 'FT-COMPLETE: complete=%s expected=%s seen=%s harness-errors=%s\n' \
    "${RESULT_COMPLETE}" "${#EXPECTED_CASES[@]}" "${#SEEN_CASE_SET[@]}" "${HARNESS_ERRORS}" | tee -a "${REPORT_FILE}"
  printf 'FT-SUMMARY: unexpected=%s known-divergence=%s strict=%s\n' \
    "${UNEXPECTED}" "${KNOWN_DIVERGENCE}" "${FT_STRICT}" | tee -a "${REPORT_FILE}"
  if [ "${RESULT_COMPLETE}" -ne 1 ]; then
    die "fault-tolerance harness did not complete"
  fi
  if [ "${UNEXPECTED}" -gt 0 ]; then
    die "fault-tolerance suite: ${UNEXPECTED} unexpected result(s)"
  fi
  if [ "${FT_STRICT}" = "1" ] && [ "${KNOWN_DIVERGENCE}" -gt 0 ]; then
    die "fault-tolerance suite (strict): ${KNOWN_DIVERGENCE} known divergence(s)"
  fi
  log "fault-tolerance suite complete: ${KNOWN_DIVERGENCE} known divergence(s) reported"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
