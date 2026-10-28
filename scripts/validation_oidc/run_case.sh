#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUSTFS_SECURITY_SCRIPT_SOURCE_ONLY=1 source "${ROOT}/rustfs-security-test.sh"

candidate="" testing_commit="" artifacts=""
environment_active=0 provider_active=0 finalized=0
binary_sha="" evidence="" cleanup_status=PASS

finalize_environment() {
  local cleanup_rc=0
  if [ "${environment_active}" -eq 1 ]; then
    environment_active=0
    (env_teardown single-single) >> "${evidence}/cleanup.log" 2>&1 || cleanup_rc=1
  fi
  if [ "${provider_active}" -eq 1 ]; then
    provider_active=0
    python3 -B "${ROOT}/scripts/validation_oidc.py" teardown --directory "${TEST_TMP}" \
      >> "${evidence}/cleanup.log" 2>&1 || cleanup_rc=1
  fi
  [ "${cleanup_rc}" -eq 0 ] || cleanup_status=FAIL
  return "${cleanup_rc}"
}

on_exit() {
  local status=$?
  trap - EXIT
  if [ "${DRY_RUN}" -eq 0 ] && [ "${finalized}" -eq 0 ] && [ -n "${evidence}" ]; then
    finalize_environment || true
  fi
  cleanup_tmp
  exit "${status}"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

while [ "$#" -gt 0 ]; do
  case "$1" in
    --validation-case) VALIDATION_CASE="$2"; shift 2 ;;
    --candidate-manifest) candidate="$2"; shift 2 ;;
    --testing-commit) testing_commit="$2"; shift 2 ;;
    --validation-artifacts-dir) artifacts="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -y|--yes) ASSUME_YES=1; shift ;;
    *) die "unknown OIDC validation option: $1" ;;
  esac
done
[ "${VALIDATION_CASE}" = oidc.login-and-session ] || die "OIDC validation case required"
if [ "${DRY_RUN}" -eq 1 ]; then
  VALIDATION_OIDC_ORIGIN="http://${RUSTFS_HEAL_IP:-10.0.0.4}:18090"
  VALIDATION_OIDC_ISSUER="${VALIDATION_OIDC_ORIGIN}/realms/rustfs-validation"
  VALIDATION_OIDC_CONFIG_URL="${VALIDATION_OIDC_ISSUER}/.well-known/openid-configuration"
  VALIDATION_OIDC_CLIENT_SECRET=dry-run
  env_setup single-single
  env_teardown single-single
  exit 0
fi
for command in docker ssh curl openssl aws jq python3 rc; do need_cmd "${command}" "OIDC validation"; done
[ -f "${candidate}" ] || die "candidate manifest missing"
[ -n "${artifacts}" ] && [ ! -e "${artifacts}" ] || die "fresh artifact directory required"
[ "${testing_commit}" = "$(git -C "${ROOT}" rev-parse HEAD)" ] || die "testing commit differs from checkout"
PACKAGE_URL="$(jq -er '.package_url' "${candidate}")"
PACKAGE_SHA256="$(jq -er '.package_sha256' "${candidate}")"
[[ "${PACKAGE_SHA256}" =~ ^[0-9a-f]{64}$ ]] || die "invalid candidate SHA-256"
export RUSTFS_ACCESS_KEY="${ACCESS_KEY}" RUSTFS_SECRET_KEY="${SECRET_KEY}"
evidence="${artifacts}/evidence/${VALIDATION_CASE}"
mkdir -p "${artifacts}/cases" "${evidence}"
python3 "${ROOT}/scripts/validation_result.py" --case-id "${VALIDATION_CASE}" \
  --profile single-1disk --result-file "${artifacts}/cases/${VALIDATION_CASE}.json" \
  --verdict RUNNING --cleanup-status PENDING

provider_active=1
python3 -B "${ROOT}/scripts/validation_oidc.py" setup --directory "${TEST_TMP}" \
  > "${evidence}/provider-setup.log" 2>&1 || {
    cat "${evidence}/provider-setup.log"; exit 1;
  }
VALIDATION_OIDC_ISSUER="$(jq -er '.issuer' "${TEST_TMP}/oidc-state.json")"
VALIDATION_OIDC_ORIGIN="${VALIDATION_OIDC_ISSUER%/realms/*}"
VALIDATION_OIDC_CONFIG_URL="$(jq -er '.config_url' "${TEST_TMP}/oidc-state.json")"
VALIDATION_OIDC_CLIENT_SECRET="$(jq -er '.client_secret' "${TEST_TMP}/oidc-state.json")"
environment_active=1
ENDPOINT="${RUSTFS_API_ENDPOINT:-${ENDPOINT_SINGLE_SINGLE}}"
(env_setup single-single) > "${evidence}/setup.log" 2>&1 || {
  cat "${evidence}/setup.log"; exit 1;
}
binary_sha="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${NODES[0]}" sha256sum /usr/bin/rustfs | awk '{print $1}')"
[[ "${binary_sha}" =~ ^[0-9a-f]{64}$ ]] || die "installed RustFS binary SHA-256 unavailable"
case_rc=0
python3 -B "${ROOT}/scripts/validation_oidc.py" test --directory "${TEST_TMP}" --endpoint "${ENDPOINT}" \
  > "${evidence}/test.log" 2>&1 || case_rc=$?
cat "${evidence}/test.log"
if ! jq -e '.cleanup == "PASS"' "${TEST_TMP}/validation-oidc-observations.json" >/dev/null 2>&1; then
  cleanup_status=FAIL
fi
finalize_environment || true
finalized=1
for item in validation-oidc-provider.json validation-oidc-observations.json; do
  if [ -f "${TEST_TMP}/${item}" ]; then cp "${TEST_TMP}/${item}" "${evidence}/${item}"; fi
done
verdict=FAIL reason="OIDC identity or session assertions failed; inspect evidence"
if [ "${case_rc}" -eq 0 ] && [ "${cleanup_status}" = PASS ]; then
  verdict=PASS; reason=""
elif [ "${case_rc}" -eq 0 ]; then
  verdict=INFRA_ERROR; reason="OIDC environment finalization failed"
fi
args=("${ROOT}/scripts/validation_result.py" --case-id "${VALIDATION_CASE}"
  --profile single-1disk --result-file "${artifacts}/cases/${VALIDATION_CASE}.json"
  --verdict "${verdict}" --cleanup-status "${cleanup_status}"
  --candidate-manifest "${candidate}" --rc-bin "$(command -v rc)"
  --testing-commit "${testing_commit}" --binary-sha256 "${binary_sha}"
  --evidence "evidence/${VALIDATION_CASE}/test.log"
  --evidence "evidence/${VALIDATION_CASE}/setup.log"
  --evidence "evidence/${VALIDATION_CASE}/cleanup.log")
if [ -n "${reason}" ]; then args+=(--reason "${reason}"); fi
if [ "${verdict}" = PASS ]; then
  args+=(--evidence "evidence/${VALIDATION_CASE}/validation-oidc-provider.json"
    --evidence "evidence/${VALIDATION_CASE}/validation-oidc-observations.json")
fi
python3 "${args[@]}"
[ "${verdict}" = PASS ]
