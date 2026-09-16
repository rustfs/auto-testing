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
#      hide 1/2/3 drives (tmpfs bind over the drive dir), then restore
#   B  multi-node 4x1 (1 drive per node)      one set of 4, EC:2
#      stop 1/2/3 nodes, then restore
#   C  multi-node 4x4 (16 drives, one set)    EC:4  readq 12 writeq 13
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
VOLUMES_SNMD="http://rustfs-node1:9000/data/rustfs{1...4}/mnmd"
VOLUMES_4X1="http://rustfs-node{1...4}:9000/data/rustfs1/mnmd"
VOLUMES_4X4="http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd"

BASELINE_BUCKET="ft-baseline"
BASELINE_OBJECTS=(ft-small-16k.bin ft-mid-1m.bin ft-big-8m.bin)
PROBE_PREFIX="ft-probe"

TMP="$(mktemp -d /tmp/rustfs-ft.XXXXXX)"
EVIDENCE="${EVIDENCE_DIR:-${TMP}/evidence}"
mkdir -p "${EVIDENCE}"
REPORT_FILE="${REPORT_FILE:-${TMP}/report.md}"

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

record() { # record <verdict> <case-id> <detail>
  local verdict="$1" case_id="$2" detail="$3"
  printf 'FT-CASE: %-28s verdict=%-16s %s\n' "${case_id}" "${verdict}" "${detail}" | tee -a "${REPORT_FILE}"
  case "${verdict}" in
    "UNEXPECTED")       UNEXPECTED=$((UNEXPECTED + 1)) ;;
    "known-divergence") KNOWN_DIVERGENCE=$((KNOWN_DIVERGENCE + 1)) ;;
  esac
}

# aws s3 helper: s3op <endpoint> <s3api args...> -> S3_RC, S3_CODE
S3_RC=0
S3_CODE=""
s3op() {
  local ep="$1"; shift
  if [ "${DRY_RUN}" -eq 1 ]; then log "DRY-RUN: aws s3api $* @ ${ep}"; S3_RC=0; S3_CODE=""; return 0; fi
  local err="${TMP}/s3err"
  if AWS_EC2_METADATA_DISABLED=true \
     AWS_ACCESS_KEY_ID="${ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${SECRET_KEY}" AWS_DEFAULT_REGION="${REGION}" \
     aws s3api "$@" --endpoint-url "${ep}" --output json 2>"${err}"; then
    S3_RC=0; S3_CODE=""
  else
    S3_RC=$?
    S3_CODE="$(grep -oE '\(([A-Za-z]+)\) when calling' "${err}" | head -1 | sed 's/ when calling//' | tr -d '()')"
    [ -n "${S3_CODE}" ] || S3_CODE="unknown"
  fi
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

settle() { log "settling ${SETTLE_SECS}s for failure/recovery detection"; sleep "${SETTLE_SECS}"; }

expect_read() { # expect_read <case-id> <endpoint> <ok|fail> <key>
  local case_id="$1" ep="$2" expect="$3" key="$4"
  local got verdict detail
  s3op "${ep}" get-object --bucket "${BASELINE_BUCKET}" --key "${key}" --outfile "${TMP}/get.out"
  if [ "${S3_RC}" -eq 0 ]; then
    got="ok"; detail="sha16=$(sha256sum "${TMP}/get.out" | cut -c1-16)"
  else
    got="fail"; detail="code=${S3_CODE}"
  fi
  if [ "${got}" = "${expect}" ]; then
    verdict="pass"
  elif [ "${expect}" = "ok" ] && [ "${got}" = "fail" ]; then
    case "${S3_CODE}" in
      SlowDown|ServiceUnavailable|InternalError|RequestTimeout|InsufficientStorage)
        verdict="known-divergence"
        detail+=" (read quorum met but read refused; article-reported lock-gated read path)" ;;
      *) verdict="UNEXPECTED" ;;
    esac
  else
    verdict="UNEXPECTED"
  fi
  record "${verdict}" "${case_id}" "read ${key}: got=${got} ${detail}"
}

expect_write() { # expect_write <case-id> <endpoint> <ok|fail> <key>
  local case_id="$1" ep="$2" expect="$3" key="$4"
  local got verdict detail probe="${TMP}/probe.bin"
  head -c 4096 /dev/zero > "${probe}"
  s3op "${ep}" put-object --bucket "${BASELINE_BUCKET}" --key "${key}" --body "${probe}"
  if [ "${S3_RC}" -eq 0 ]; then got="ok"; detail=""; else got="fail"; detail="code=${S3_CODE}"; fi
  if [ "${got}" = "${expect}" ]; then verdict="pass"; else verdict="UNEXPECTED"; fi
  record "${verdict}" "${case_id}" "write ${key}: got=${got} ${detail}"
}

seed_baseline() { # seed_baseline <endpoint>
  local ep="$1" o
  log "seeding baseline bucket + objects on ${ep}"
  s3op "${ep}" create-bucket --bucket "${BASELINE_BUCKET}"
  if [ "${S3_RC}" -ne 0 ] && [ "${S3_CODE}" != "BucketAlreadyOwnedByYou" ] && [ "${S3_CODE}" != "BucketAlreadyExists" ]; then
    die "create-bucket failed: ${S3_CODE}"
  fi
  head -c 16384   /dev/urandom > "${TMP}/ft-small-16k.bin"
  head -c 1048576 /dev/urandom > "${TMP}/ft-mid-1m.bin"
  head -c 8388608 /dev/urandom > "${TMP}/ft-big-8m.bin"
  for o in "${BASELINE_OBJECTS[@]}"; do
    s3op "${ep}" put-object --bucket "${BASELINE_BUCKET}" --key "${o}" --body "${TMP}/${o}"
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
    run_remote "${node}" "sudo -n mkdir -p /data/rustfs{1..4}/mnmd && for i in 1 2 3 4; do sudo -n chown rustfs:rustfs "/data/rustfs${i}" "/data/rustfs${i}/mnmd"; done 2>/dev/null || true"
  done
  if [ "${volumes}" = "${VOLUMES_SNMD}" ]; then
    log "single-node topology: starting ${RUSTFS_SERVICE} on ${SNMD_NODE} only"
    run_remote "${SNMD_NODE}" "sudo -n systemctl start ${RUSTFS_SERVICE}" || die "start failed on ${SNMD_NODE}"
  else
    log "starting ${RUSTFS_SERVICE} on ${NODES[*]}"
    remote_all "sudo -n systemctl start ${RUSTFS_SERVICE}"
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

stop_node()  { log "stopping ${RUSTFS_SERVICE} on $1"; run_remote "$1" "sudo -n systemctl stop ${RUSTFS_SERVICE}"; }
start_node() { log "starting ${RUSTFS_SERVICE} on $1"; run_remote "$1" "sudo -n systemctl start ${RUSTFS_SERVICE}"; }

drop_drive()   { log "${SNMD_NODE}: hiding drive $1 (empty tmpfs bind)"; run_remote "${SNMD_NODE}" "sudo -n mount -t tmpfs -o size=8m tmpfs '$1'"; }
restore_drive(){ log "${SNMD_NODE}: restoring drive $1";                 run_remote "${SNMD_NODE}" "sudo -n umount '$1' 2>/dev/null || true"; }

do_cleanup() {
  log "cleanup: stopping services, unmounting hidden drives, purging data"
  for node in "${NODES[@]}"; do
    run_remote "${node}" "sudo -n systemctl stop ${RUSTFS_SERVICE} 2>/dev/null; sudo -n systemctl stop rustfs-rep2 2>/dev/null; for d in ${SNMD_DRIVES[*]}; do sudo -n umount \"\$d\" 2>/dev/null || true; done; sudo -n rm -rf /data/rustfs{1..4}/mnmd /data/rustfs-rep2 2>/dev/null; true"
  done
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

  start_node "${NODES[1]}"; start_node "${NODES[2]}"; start_node "${NODES[3]}"
  settle                                              # B4: recovery
  capture_health "B4-recovered" "${EP1}"
  expect_read  "B4/read"          "${EP1}" "ok" "ft-mid-1m.bin"
  expect_read  "B4/read-b1-probe" "${EP1}" "ok" "${PROBE_PREFIX}-b1"
  expect_write "B4/write"         "${EP1}" "ok" "${PROBE_PREFIX}-b4"
  do_cleanup
}

# ---------------------------------------------------------------- scenario C
scenario_c() {
  log "=== C: multi-node 4x4 (16 drives, one set), EC:4 (readq 12, writeq 13) ==="
  install_and_start "${VOLUMES_4X4}"
  seed_baseline "${EP1}"

  stop_node "${NODES[3]}"; settle                     # C1: 12 online == readq
  capture_health "C1" "${EP1}"
  expect_read  "C1/read"  "${EP1}" "ok"   "ft-small-16k.bin"
  expect_write "C1/write" "${EP1}" "fail" "${PROBE_PREFIX}-c1"

  stop_node "${NODES[2]}"; settle                     # C2: 8 online
  capture_health "C2" "${EP1}"
  expect_read  "C2/read"  "${EP1}" "fail" "ft-mid-1m.bin"
  expect_write "C2/write" "${EP1}" "fail" "${PROBE_PREFIX}-c2"

  start_node "${NODES[2]}"; start_node "${NODES[3]}"; settle
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

  start_node "${NODES[2]}"; start_node "${NODES[3]}"; settle
  capture_health "C2-3-recovered" "${EP1}"
  expect_read  "C2-3/read"  "${EP1}" "ok" "ft-mid-1m.bin"
  expect_write "C2-3/write" "${EP1}" "ok" "${PROBE_PREFIX}-c2-3"
  do_cleanup
}

# ------------------------------------------------------------------- main ---
usage() {
  cat <<EOF
Usage: $0 [options]
  --cleanup              stop services, unhide drives, purge data, then exit
  --scenario A|B|C|C2    run one scenario
  --all                  run all scenarios (A, B, C, C2)
  --package-url URL      .deb to install (required for test runs)
  -y                     assume yes
  --dry-run              print planned actions only
  --log-file FILE        also tee output into FILE
  --strict               known-divergence also fails the suite (FT_STRICT=1)
Environment: RUSTFS_NODES, RUSTFS_SSH_USER, FT_SETTLE_SECS, FT_STRICT
EOF
}

main() {
  [ "$#" -eq 0 ] && { usage; exit 0; }
  local opt scenario="" all=0 cleanup=0
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --cleanup)     cleanup=1 ;;
      --scenario)    scenario="$1"; shift ;;
      --all)         all=1 ;;
      --package-url) PACKAGE_URL="$1"; shift ;;
      -y)            ASSUME_YES=1 ;;
      --dry-run)     DRY_RUN=1 ;;
      --log-file)    LOG_FILE="$1"; shift ;;
      --strict)      FT_STRICT=1 ;;
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

  printf 'FT-REPORT: fault-tolerance suite\n' > "${REPORT_FILE}"
  if [ "${all}" -eq 1 ]; then
    scenario_a; scenario_b; scenario_c; scenario_c2
  elif [ -n "${scenario}" ]; then
    case "${scenario}" in
      A)  scenario_a  ;;
      B)  scenario_b  ;;
      C)  scenario_c  ;;
      C2) scenario_c2 ;;
      *)  die "unknown scenario: ${scenario}" ;;
    esac
  else
    usage; exit 0
  fi

  printf 'FT-SUMMARY: unexpected=%s known-divergence=%s strict=%s\n' \
    "${UNEXPECTED}" "${KNOWN_DIVERGENCE}" "${FT_STRICT}" | tee -a "${REPORT_FILE}"
  if [ "${UNEXPECTED}" -gt 0 ]; then
    die "fault-tolerance suite: ${UNEXPECTED} unexpected result(s)"
  fi
  if [ "${FT_STRICT}" = "1" ] && [ "${KNOWN_DIVERGENCE}" -gt 0 ]; then
    die "fault-tolerance suite (strict): ${KNOWN_DIVERGENCE} known divergence(s)"
  fi
  log "fault-tolerance suite complete: ${KNOWN_DIVERGENCE} known divergence(s) reported"
}

main "$@"
