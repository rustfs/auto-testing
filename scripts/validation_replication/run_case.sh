#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/rustfs-replication-test.sh"
endpoint_host_prefix() { printf 'rustfs-node'; }
SITE2_ENDPOINT="http://rustfs-node1:19000"

module_for_case() {
  case "$1" in
    bucket-replication.*) printf bucket ;;
    site-replication.*) printf site ;;
    *) return 2 ;;
  esac
}

site_action() {
  local site="$1" action="$2" node pid fail=0
  case "${site}" in
    a)
      local pids=()
      for node in "${NODES[@]}"; do
        service_action "${action}" "${node}" & pids+=("$!")
      done
      for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
      [ "${fail}" -eq 0 ] || die "site A ${action} failed"
      if [ "${action}" != stop ]; then
        for node in "${NODES[@]}"; do wait_service_active "${node}" "${RUSTFS_SERVICE}"; done
        wait_http_live "${API_ENDPOINT}"
      fi ;;
    b)
      service_action "${action}" "${NODES[0]}" "${SITE2_SERVICE}"
      if [ "${action}" != stop ]; then
        wait_service_active "${NODES[0]}" "${SITE2_SERVICE}"
        wait_http_live "${SITE2_ENDPOINT}"
      fi ;;
    *) die "unknown replication site: ${site}" ;;
  esac
}

hook_json() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' \
    bash "${ROOT}/scripts/validation_replication/run_case.sh" hook "$1" "$2"
}

setup_environment() {
  env_setup_main_cluster
  env_setup_site2
}

run_validation() {
  local case_id="" candidate="" testing_commit="" artifacts="" setup_rc=0 harness_rc=0 teardown_rc=0
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --validation-case) case_id="$2"; shift 2 ;;
      --candidate-manifest) candidate="$2"; shift 2 ;;
      --testing-commit) testing_commit="$2"; shift 2 ;;
      --validation-artifacts-dir) artifacts="$2"; shift 2 ;;
      --dry-run) DRY_RUN=1; shift ;;
      -y|--yes) shift ;;
      *) die "unknown replication validation option: $1" ;;
    esac
  done
  local module
  module="$(module_for_case "${case_id}")" || die "replication case required"
  python3 -B -c 'import sys; sys.path.insert(0,sys.argv[1]); from scripts.validation_replication.case_plan import BUCKET_CASE_IDS,SITE_CASE_IDS; valid=BUCKET_CASE_IDS if sys.argv[2]=="bucket" else SITE_CASE_IDS; sys.exit(0 if sys.argv[3] in valid else 2)' \
    "${ROOT}" "${module}" "${case_id}" || die "unknown canonical replication case: ${case_id}"
  if [ "${DRY_RUN}" -eq 1 ]; then
    setup_environment
    return 0
  fi
  [ -f "${candidate}" ] || die "candidate manifest missing"
  [ -n "${artifacts}" ] && [ ! -e "${artifacts}" ] || die "fresh artifact directory required"
  [ "${testing_commit}" = "$(git -C "${ROOT}" rev-parse HEAD)" ] || die "testing commit differs from checkout"
  PACKAGE_URL="$(jq -er '.package_url' "${candidate}")"
  PACKAGE_SHA256="$(jq -er '.package_sha256' "${candidate}")"
  [[ "${PACKAGE_SHA256}" =~ ^[0-9a-f]{64}$ ]] || die "invalid candidate SHA-256"
  mkdir -p "${artifacts}/cases" "${artifacts}/evidence/${case_id}/replication"
  local evidence="${artifacts}/evidence/${case_id}" harness="${artifacts}/evidence/${case_id}/replication"
  python3 "${ROOT}/scripts/validation_result.py" --case-id "${case_id}" \
    --profile replication-2site --result-file "${artifacts}/cases/${case_id}.json" \
    --verdict RUNNING --cleanup-status PENDING
  (setup_environment) > "${evidence}/setup.log" 2>&1 || setup_rc=$?
  if [ "${setup_rc}" -ne 0 ]; then
    cat "${evidence}/setup.log"
    (env_teardown) > "${evidence}/teardown.log" 2>&1 || true
    return 1
  fi
  local binary_sha
  binary_sha="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${NODES[0]}" sha256sum /usr/bin/rustfs | awk '{print $1}')"
  [[ "${binary_sha}" =~ ^[0-9a-f]{64}$ ]] || die "installed RustFS binary SHA-256 unavailable"
  python3 -B "${ROOT}/scripts/validation_replication/replication_behavior.py" \
    --module "${module}" --case "${case_id}" --cleanup-on-failure \
    --endpoint-a "${API_ENDPOINT}" --endpoint-b "${SITE2_ENDPOINT}" \
    --access-key "${ACCESS_KEY}" --secret-key "${SECRET_KEY}" \
    --run-id "${case_id}-$(date +%s)-$$" --evidence-dir "${harness}" \
    --rc-bin "$(command -v rc)" \
    --site-a-stop-hook "$(hook_json a stop)" \
    --site-a-start-hook "$(hook_json a start)" \
    --site-a-restart-hook "$(hook_json a restart)" \
    --site-b-stop-hook "$(hook_json b stop)" \
    --site-b-start-hook "$(hook_json b start)" \
    --site-b-restart-hook "$(hook_json b restart)" \
    > "${evidence}/test.log" 2>&1 || harness_rc=$?
  cat "${evidence}/test.log"
  (env_teardown) > "${evidence}/teardown.log" 2>&1 || teardown_rc=$?
  local verdict=FAIL reason="replication behavior assertions failed; inspect evidence" cleanup_status=FAIL
  if [ "${teardown_rc}" -eq 0 ]; then cleanup_status=PASS; fi
  if [ "${harness_rc}" -eq 0 ] && [ "${teardown_rc}" -eq 0 ] && \
     python3 -B "${ROOT}/scripts/validation_replication/verify_result.py" "${case_id}" "${harness}"; then
    verdict=PASS; reason=""
  elif jq -e '.status.state == "infra_error"' "${harness}/result.json" >/dev/null 2>&1; then
    verdict=INFRA_ERROR; reason="replication environment or execution infrastructure failed; inspect evidence"
  fi
  local args=("${ROOT}/scripts/validation_result.py" --case-id "${case_id}"
    --profile replication-2site --result-file "${artifacts}/cases/${case_id}.json"
    --verdict "${verdict}" --cleanup-status "${cleanup_status}"
    --candidate-manifest "${candidate}" --rc-bin "$(command -v rc)"
    --testing-commit "${testing_commit}" --binary-sha256 "${binary_sha}"
    --evidence "evidence/${case_id}/setup.log" --evidence "evidence/${case_id}/test.log"
    --evidence "evidence/${case_id}/teardown.log")
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
  hook) shift; site_action "$@" ;;
  *) run_validation "$@" ;;
esac
