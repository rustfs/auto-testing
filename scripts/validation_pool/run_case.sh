#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUSTFS_POOL_SCRIPT_SOURCE_ONLY=1 source "${ROOT}/rustfs_pool_expand.sh"
export RUSTFS_POOL_VALIDATION_MODE=1
RUSTFS_OBS_LOGGER_LEVEL=info

[ "${#NODES[@]}" -ge 2 ] || die "pool validation needs two auto-testing nodes"
NODES=("${NODES[0]}" "${NODES[1]}")
NODE_ENDPOINTS=(http://rustfs-node1:9000 http://rustfs-node2:9000)
LEGACY_ENDPOINT="${NODE_ENDPOINTS[0]}"
ADMIN_ENDPOINT="${NODE_ENDPOINTS[0]}"
WARP_ENDPOINT="${NODE_ENDPOINTS[0]}"
ASSUME_YES=1
resolve_endpoints

POOL1_VOLUMES="http://rustfs-node2:9000/data/rustfs{1...4}/mnmd"
MARKER=/etc/rustfs-pool-validation-id

check_marker() {
  local node
  [ -n "${POOL_VALIDATION_RUN_ID:-}" ] || die "pool run identity missing"
  for node in "${NODES[@]}"; do
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      "test \"\$(cat ${MARKER})\" = \"${POOL_VALIDATION_RUN_ID}\"" \
      || die "pool environment marker does not match on ${node}"
  done
}

mark_nodes() {
  local node
  for node in "${NODES[@]}"; do
    printf 'printf %%s %q | sudo -n tee %s >/dev/null\n' \
      "${POOL_VALIDATION_RUN_ID}" "${MARKER}" | run_remote "${node}"
  done
}

reset_initial() {
  local node
  for node in "${NODES[@]}"; do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
        "systemctl cat ${RUSTFS_SERVICE} >/dev/null 2>&1"; then
      service_action stop "${node}"
    else
      log "${node}: ${RUSTFS_SERVICE} is not installed; stop skipped"
    fi
  done
  for node in "${NODES[@]}"; do
    run_remote "${node}" <<'REMOTE'
set -euo pipefail
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"
for i in 1 2 3 4; do
  ${SUDO} rm -rf "/data/rustfs${i}/mnmd"
  ${SUDO} install -d -o rustfs -g rustfs "/data/rustfs${i}/mnmd"
done
REMOTE
  done
  write_rustfs_config "${NODES[0]}" "${VOLUMES_1}"
  start_and_wait_nodes "${NODES[0]}"
  wait_node_ready 0
}

retire_pool_config() {
  service_action stop "${NODES[0]}"
  service_action stop "${NODES[1]}"
  write_rustfs_config "${NODES[1]}" "${POOL1_VOLUMES}"
  ensure_volume_dirs "${NODES[1]}" "${POOL1_VOLUMES}"
  start_and_wait_nodes "${NODES[1]}"
  wait_node_ready 1
}

disk_probe() {
  local mode="$1" phase="$2" node index path
  printf 'phase=%s mode=%s\n' "${phase}" "${mode}"
  for node in "${NODES[@]}"; do
    for index in 1 2 3 4; do
      path="/data/rustfs${index}/mnmd"
      printf 'node=%s path=%s\n' "${node}" "${path}"
      {
        printf 'set -euo pipefail\n'
        printf 'findmnt -rn -T %q -o TARGET,SOURCE,FSTYPE,UUID\n' "${path}"
        printf "python3 - %q %q <<'POOL_DISK_PROBE'\n" "${mode}" "${path}"
        cat "${ROOT}/scripts/validation_pool/pool_disk_probe.py"
        printf '\nPOOL_DISK_PROBE\n'
      } | run_remote "${node}"
    done
  done
}

collect_decommission_logs() {
  local node="$1" since="$2"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "
    SUDO=''; [ \"\$(id -u)\" -ne 0 ] && SUDO='sudo -n'
    {
      \${SUDO} journalctl -u ${RUSTFS_SERVICE} --since $(printf '%q' "${since}") \
        --no-pager --output=cat -n 5000
      for log_file in /var/log/rustfs/*.log; do
        [ -f \"\${log_file}\" ] || continue
        \${SUDO} tail -n 5000 \"\${log_file}\"
      done
    } 2>/dev/null || true
  "
}

pool_hook() {
  local action="${1:-}" layout="" phase="" since="" node output0 output1 journal_since
  shift || true
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --layout) layout="$2"; shift 2 ;;
      --phase) phase="$2"; shift 2 ;;
      --since) since="$2"; shift 2 ;;
      *) die "unknown pool hook option: $1" ;;
    esac
  done
  check_marker
  case "${action}" in
    reset) reset_initial ;;
    topology)
      case "${layout}" in
        initial) reset_initial ;;
        expanded) step5_expand_pool2 ;;
        retired) retire_pool_config ;;
        *) die "unknown pool layout: ${layout}" ;;
      esac ;;
    restart)
      case "${layout}" in
        expanded)
          service_action_all restart
          wait_node_ready 0
          wait_node_ready 1 ;;
        retired)
          service_action restart "${NODES[1]}"
          wait_node_ready 1 ;;
        *) die "unknown pool restart layout: ${layout}" ;;
      esac ;;
    census) disk_probe census "${phase}" ;;
    diagnostic)
      if [ -n "${since}" ]; then
        journal_since="$(python3 -c 'from datetime import datetime,timezone; import sys; value=datetime.fromisoformat(sys.argv[1].replace("Z","+00:00")); assert value.tzinfo is not None; print(value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC"))' "${since}")"
        output0="$(collect_decommission_logs "${NODES[0]}" "${journal_since}")"
        output1="$(collect_decommission_logs "${NODES[1]}" "${journal_since}")"
        jq -n --arg phase "${phase}" --arg since "${since}" \
          --arg node0 "${NODES[0]}" --arg node1 "${NODES[1]}" \
          --arg output0 "${output0}" --arg output1 "${output1}" \
          '{environmentId:"lifecycle-a",phase:$phase,mode:"decommission-journal",since:$since,nodes:{($node0):$output0,($node1):$output1}}'
      else
        disk_probe metadata "${phase}"
      fi ;;
    cleanup) step0_reset ;;
    *) die "unknown pool hook: ${action}" ;;
  esac
}

hook_json() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' \
    bash "${ROOT}/scripts/validation_pool/run_case.sh" hook "$1"
}

setup_environment() {
  step0_reset
  step1_download
  step2_install
  step3_start_first_pool
  mark_nodes
}

run_validation() {
  local case_id="" candidate="" testing_commit="" artifacts="" setup_rc=0 harness_rc=0 cleanup_rc=0
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --validation-case) case_id="$2"; shift 2 ;;
      --candidate-manifest) candidate="$2"; shift 2 ;;
      --testing-commit) testing_commit="$2"; shift 2 ;;
      --validation-artifacts-dir) artifacts="$2"; shift 2 ;;
      --dry-run) DRY_RUN=1; shift ;;
      -y|--yes) shift ;;
      *) die "unknown pool validation option: $1" ;;
    esac
  done
  [[ "${case_id}" == pool.* ]] || die "pool validation case required"
  python3 -B -c 'import sys; sys.path.insert(0,sys.argv[1]); from scripts.validation_pool.case_plan import POOL_CASE_IDS; sys.exit(0 if sys.argv[2] in POOL_CASE_IDS else 2)' \
    "${ROOT}" "${case_id}" || die "unknown canonical pool case: ${case_id}"
  if [ "${DRY_RUN}" -eq 1 ]; then
    POOL_VALIDATION_RUN_ID="${case_id}-dry-run"
    setup_environment
    return 0
  fi
  [ -f "${candidate}" ] || die "candidate manifest missing"
  [ -n "${artifacts}" ] && [ ! -e "${artifacts}" ] || die "fresh artifact directory required"
  [ "${testing_commit}" = "$(git -C "${ROOT}" rev-parse HEAD)" ] || die "testing commit differs from checkout"
  PACKAGE_URL="$(jq -er '.package_url' "${candidate}")"
  PACKAGE_SHA256="$(jq -er '.package_sha256' "${candidate}")"
  [[ "${PACKAGE_SHA256}" =~ ^[0-9a-f]{64}$ ]] || die "invalid candidate SHA-256"
  export RUSTFS_NODES="${NODES[*]}" RUSTFS_ADMIN_ENDPOINT="${NODE_ENDPOINTS[0]}"
  export POOL_VALIDATION_RUN_ID="${case_id}-$(date +%s)-$$"
  mkdir -p "${artifacts}/cases" "${artifacts}/evidence/${case_id}/pool"
  local evidence="${artifacts}/evidence/${case_id}" harness="${artifacts}/evidence/${case_id}/pool"
  python3 "${ROOT}/scripts/validation_result.py" --case-id "${case_id}" \
    --profile lifecycle-a --result-file "${artifacts}/cases/${case_id}.json" \
    --verdict RUNNING --cleanup-status PENDING
  (setup_environment) > "${evidence}/setup.log" 2>&1 || setup_rc=$?
  if [ "${setup_rc}" -ne 0 ]; then
    cat "${evidence}/setup.log"
    (step0_reset) > "${evidence}/cleanup.log" 2>&1 || true
    return 1
  fi
  local binary_sha
  binary_sha="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${NODES[0]}" sha256sum /usr/bin/rustfs | awk '{print $1}')"
  [[ "${binary_sha}" =~ ^[0-9a-f]{64}$ ]] || die "installed RustFS binary SHA-256 unavailable"
  python3 -B "${ROOT}/scripts/validation_pool/pool_behavior.py" \
    --case "${case_id}" --endpoint "${NODE_ENDPOINTS[0]}" \
    --node-endpoint "${NODE_ENDPOINTS[0]}" --node-endpoint "${NODE_ENDPOINTS[1]}" \
    --access-key "${ACCESS_KEY}" --secret-key "${SECRET_KEY}" \
    --run-id "${POOL_VALIDATION_RUN_ID}" --evidence-dir "${harness}" \
    --rc-bin "$(command -v rc)" --cleanup-on-failure \
    --reset-hook "$(hook_json reset)" --topology-hook "$(hook_json topology)" \
    --restart-hook "$(hook_json restart)" --census-hook "$(hook_json census)" \
    --diagnostic-hook "$(hook_json diagnostic)" --cleanup-hook "$(hook_json cleanup)" \
    > "${evidence}/test.log" 2>&1 || harness_rc=$?
  cat "${evidence}/test.log"
  if ! jq -e '.cleanupOutcome.state == "pass"' "${harness}/run-manifest.json" >/dev/null 2>&1; then
    (pool_hook cleanup) > "${evidence}/fallback-cleanup.log" 2>&1 || cleanup_rc=$?
  fi
  local verdict=FAIL reason="pool behavior assertions failed; inspect evidence" cleanup_status=FAIL
  if jq -e '.cleanupOutcome.state == "pass"' "${harness}/run-manifest.json" >/dev/null 2>&1; then
    cleanup_status=PASS
  fi
  if [ "${harness_rc}" -eq 0 ] && [ "${cleanup_rc}" -eq 0 ] && \
     python3 -B "${ROOT}/scripts/validation_ilm/verify_result.py" "${case_id}" "${harness}"; then
    verdict=PASS; reason=""
  elif jq -e '.status.state == "infra_error"' "${harness}/result.json" >/dev/null 2>&1; then
    verdict=INFRA_ERROR; reason="pool environment or execution infrastructure failed; inspect evidence"
  fi
  local args=("${ROOT}/scripts/validation_result.py" --case-id "${case_id}"
    --profile lifecycle-a --result-file "${artifacts}/cases/${case_id}.json"
    --verdict "${verdict}" --cleanup-status "${cleanup_status}"
    --candidate-manifest "${candidate}" --rc-bin "$(command -v rc)"
    --testing-commit "${testing_commit}" --binary-sha256 "${binary_sha}"
    --evidence "evidence/${case_id}/setup.log" --evidence "evidence/${case_id}/test.log")
  if [ -n "${reason}" ]; then args+=(--reason "${reason}"); fi
  if [ "${verdict}" = PASS ]; then
    local file
    while IFS= read -r -d '' file; do
      args+=(--evidence "${file#"${artifacts}/"}")
    done < <(find "${harness}" -type f -print0 | sort -z)
  fi
  python3 "${args[@]}"
  [ "${verdict}" = PASS ]
}

case "${1:-}" in
  hook) shift; pool_hook "$@" ;;
  *) run_validation "$@" ;;
esac
