#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUSTFS_HEAL_SCRIPT_SOURCE_ONLY=1 source "${ROOT}/rustfs_heal_test.sh"
ASSUME_YES=1
candidate="" testing_commit="" artifacts="" case_id=""
driver=healthy.py
while [ "$#" -gt 0 ]; do
  case "$1" in
    --validation-case) case_id="$2"; shift 2 ;;
    --candidate-manifest) candidate="$2"; shift 2 ;;
    --testing-commit) testing_commit="$2"; shift 2 ;;
    --validation-artifacts-dir) artifacts="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -y|--yes) shift ;;
    *) die "unknown Heal validation option: $1" ;;
  esac
done
case "${case_id}" in
  heal.admin-healthy-noop|heal.admin-invalid-request) ;;
  heal.historical-versions|heal.repeat-idempotency|heal.versioning-suspended-null|heal.below-read-quorum|heal.missing-or-corrupt-xlmeta|heal.object-metadata-and-tags|heal.multiple-shard-loss|heal.special-keys-and-pagination) driver=versions.py ;;
  heal.admin-token-cross-node-status|heal.admin-dry-run|heal.missing-part-file|heal.data-shard-bitrot|heal.parity-shard-bitrot|heal.truncated-and-zeroed-shard|heal.completed-multipart-object|heal.range-head-and-conditions|heal.manual-normal-attribution|heal.manual-deep-attribution)
    driver=shards.py ;;
  *) die "automatic Heal executor is not migrated: ${case_id}" ;;
esac
[ "${#NODES[@]}" -ge 4 ] || die "H1/H2 requires four current auto-testing Heal nodes"
NODES=("${NODES[0]}" "${NODES[1]}" "${NODES[2]}" "${NODES[3]}")
if [ "${DRY_RUN}" -eq 1 ]; then
  log "${case_id}: current Heal nodes ${NODES[*]}, immutable deb install, run-owned H1/H2 service and four roots per node"
  log "Source branches, ACK ledger, candidate-bound physical evidence, exact task outcomes, full S3 verification and cleanup"
  exit 0
fi
for program in python3 ssh curl jq rc; do need_cmd "${program}" "Heal validation"; done
[ -f "${candidate}" ] || die "candidate manifest missing"
[ -n "${artifacts}" ] && [ ! -e "${artifacts}" ] || die "fresh artifact directory required"
[ "${testing_commit}" = "$(git -C "${ROOT}" rev-parse HEAD)" ] || die "testing commit differs from checkout"
[[ "${GITHUB_RUN_ID:-}" =~ ^[1-9][0-9]*$ ]] || die "Functional Chain workflow identity required"
PACKAGE_URL="$(jq -er '.package_url' "${candidate}")"
PACKAGE_SHA256="$(jq -er '.package_sha256' "${candidate}")"
[[ "${PACKAGE_SHA256}" =~ ^[0-9a-f]{64}$ ]] || die "invalid candidate package SHA-256"
export RUSTFS_NODES="${NODES[*]}" RUSTFS_SSH_USER="${SSH_USER}" RUSTFS_SSH_PORT="${SSH_PORT}"
tool_args=()
if [ -n "${VALIDATION_HEAL_TOOL_DIRECTORY:-}" ]; then
  tool_args=(--tool-directory "${VALIDATION_HEAL_TOOL_DIRECTORY}")
fi
[ "${driver}" = healthy.py ] || [ "${#tool_args[@]}" -gt 0 ] || die "physical Heal cases require candidate-bound bitrot tools"
evidence="${artifacts}/evidence/${case_id}"
mkdir -p "${artifacts}/cases" "${evidence}"
python3 -B "${ROOT}/scripts/validation_result.py" --case-id "${case_id}" --profile cluster-1pool \
  --result-file "${artifacts}/cases/${case_id}.json" --verdict RUNNING --cleanup-status PENDING

bootstrap() {
  local node
  for node in "${NODES[@]}"; do
    stop_existing "${node}"
  done
  step1_download
  for node in "${NODES[@]}"; do
    {
      printf 'set -euo pipefail\n'
      printf "printf '%%s  %%s\\n' %q /home/rustfs/packages/rustfs.deb | sha256sum -c -\n" "${PACKAGE_SHA256}"
      printf 'sudo -n dpkg -i --force-confold /home/rustfs/packages/rustfs.deb\n'
      printf 'sudo -n systemctl stop rustfs\n'
    } | run_remote "${node}"
  done
}

stop_existing() {
  local node="$1"
  run_remote "${node}" <<'REMOTE'
set -euo pipefail
load_state="$(systemctl show rustfs -p LoadState --value)"
if [ "${load_state}" != not-found ]; then
  sudo -n systemctl stop rustfs
fi
! systemctl is-active --quiet rustfs
REMOTE
}

finalize_package() {
  local node failed=0
  for node in "${NODES[@]}"; do
    (stop_existing "${node}") || failed=1
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      'if dpkg-query -W -f="${Status}" rustfs 2>/dev/null | grep -q "install ok installed"; then sudo -n dpkg -P rustfs; fi' || failed=1
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      '! systemctl is-active --quiet rustfs' || failed=1
  done
  return "${failed}"
}

bootstrap_rc=0 case_rc=0 cleanup_status=PASS binary_sha=""
package_active=1
on_exit() {
  local exit_code=$?
  trap - EXIT
  if [ "${package_active}" -eq 1 ]; then
    (finalize_package) >> "${evidence}/cleanup.log" 2>&1 || true
  fi
  exit "${exit_code}"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
(bootstrap) > "${evidence}/setup.log" 2>&1 || bootstrap_rc=$?
cat "${evidence}/setup.log"
if [ "${bootstrap_rc}" -eq 0 ]; then
  for node in "${NODES[@]}"; do
    node_sha="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" sha256sum /usr/bin/rustfs | awk '{print $1}')"
    [[ "${node_sha}" =~ ^[0-9a-f]{64}$ ]] || die "installed candidate digest unavailable on ${node}"
    if [ -n "${binary_sha}" ] && [ "${binary_sha}" != "${node_sha}" ]; then die "node candidate digests differ"; fi
    binary_sha="${node_sha}"
  done
  python3 -B "${ROOT}/scripts/validation_heal/${driver}" --candidate "${candidate}" \
    --case-id "${case_id}" \
    "${tool_args[@]}" \
    --testing-commit "${testing_commit}" --binary-sha "${binary_sha}" --rc-bin "$(command -v rc)" \
    --evidence "${evidence}" > "${evidence}/test.log" 2>&1 || case_rc=$?
  cat "${evidence}/test.log"
  jq -e '.cleanup == "PASS"' "${evidence}/healthy-summary.json" >/dev/null 2>&1 || cleanup_status=FAIL
else
  case_rc=1
fi
(finalize_package) > "${evidence}/cleanup.log" 2>&1 || cleanup_status=FAIL
package_active=0
cat "${evidence}/cleanup.log"
[ -n "${binary_sha}" ] || {
  python3 -B "${ROOT}/scripts/validation_result.py" --case-id "${case_id}" --profile cluster-1pool \
    --result-file "${artifacts}/cases/${case_id}.json" --verdict INFRA_ERROR --cleanup-status "${cleanup_status}" \
    --candidate-manifest "${candidate}" --testing-commit "${testing_commit}" --rc-bin "$(command -v rc)" \
    --evidence "evidence/${case_id}/setup.log" --evidence "evidence/${case_id}/cleanup.log" \
    --reason "Heal candidate bootstrap failed before a verified binary was installed"
  exit 1
}
verdict=FAIL reason="Heal source assertions did not pass on every required H1/H2 branch"
if [ "${case_rc}" -eq 0 ] && [ "${cleanup_status}" = PASS ]; then
  verdict=PASS; reason=""
elif [ "${case_rc}" -eq 0 ]; then
  verdict=INFRA_ERROR; reason="Heal environment finalization failed"
fi
args=("${ROOT}/scripts/validation_result.py" --case-id "${case_id}" --profile cluster-1pool
  --result-file "${artifacts}/cases/${case_id}.json" --verdict "${verdict}" --cleanup-status "${cleanup_status}"
  --candidate-manifest "${candidate}" --testing-commit "${testing_commit}" --binary-sha256 "${binary_sha}"
  --rc-bin "$(command -v rc)" --evidence "evidence/${case_id}/setup.log"
  --evidence "evidence/${case_id}/test.log" --evidence "evidence/${case_id}/cleanup.log"
  --evidence "evidence/${case_id}/healthy-summary.json")
if [ -n "${reason}" ]; then args+=(--reason "${reason}"); fi
python3 -B "${args[@]}"
[ "${verdict}" = PASS ]
