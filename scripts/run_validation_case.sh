#!/usr/bin/env bash
set -Eeuo pipefail

if [ "$#" -ne 4 ]; then
  printf 'usage: %s CASE_ID CANDIDATE_MANIFEST TESTING_COMMIT ARTIFACTS_DIR\n' "$0" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CASE_ID="$1"
CANDIDATE_MANIFEST="$2"
TESTING_COMMIT="$3"
ARTIFACTS_DIR="$4"

case "${CASE_ID}" in
  pool.*)
    SUITE=scripts/validation_pool/run_case.sh; EXTRA_ARGS=() ;;
  bucket-replication.*|site-replication.*)
    SUITE=scripts/validation_replication/run_case.sh; EXTRA_ARGS=() ;;
  s3.basic-object-round-trip)
    SUITE=rustfs-s3-compat-test.sh; EXTRA_ARGS=() ;;
  scanner.usage-convergence|scanner.status-readiness|ilm.configuration-roundtrip|ilm.invalid-configuration|ilm.current-expiration|ilm.tag-filter-expiration|ilm.disabled-rule|ilm.rule-removal|ilm.versioned-current-expiration|ilm.noncurrent-expiration|ilm.newer-noncurrent-retention|ilm.delete-all-versions|ilm.delete-marker-expiration|ilm.multipart-expiration|ilm.restart-persistence|ilm.transition-and-restore)
    SUITE=rustfs-storage-test.sh; EXTRA_ARGS=() ;;
  kms.sse-kms-round-trip)
    SUITE=rustfs-kms-test.sh; EXTRA_ARGS=() ;;
  tiering.transition-and-restore|events.delivery|audit.request-correlation)
    SUITE=rustfs-tier-test.sh; EXTRA_ARGS=(--topology single-single) ;;
  iam.policy-enforcement)
    SUITE=rustfs-security-test.sh; EXTRA_ARGS=() ;;
  oidc.login-and-session)
    SUITE=scripts/validation_oidc/run_case.sh; EXTRA_ARGS=() ;;
  heal.admin-healthy-noop|heal.admin-invalid-request|heal.admin-token-cross-node-status|heal.admin-dry-run|heal.missing-part-file|heal.data-shard-bitrot|heal.parity-shard-bitrot|heal.truncated-and-zeroed-shard|heal.completed-multipart-object|heal.range-head-and-conditions|heal.manual-normal-attribution|heal.manual-deep-attribution|heal.historical-versions|heal.repeat-idempotency|heal.versioning-suspended-null|heal.below-read-quorum|heal.missing-or-corrupt-xlmeta|heal.object-metadata-and-tags|heal.multiple-shard-loss|heal.special-keys-and-pagination|heal.object-size-boundaries|heal.admin-authorization|heal.node-outage-rejoin|heal.object-lock-retention|heal.metadata-payload-mismatch|heal.read-repair-attribution|heal.disk-readonly-and-permission|heal.scanner-cold-object-repair|heal.delete-marker-semantics|heal.status-partial-disabled-unknown|heal.incremental-cursor-and-truncation|heal.admin-stop-and-stale-token|heal.cancelled-task-restart|heal.admin-overlap-and-force-start|heal.root-graceful-coordinator-restart|heal.read-quorum-boundary|heal.stale-version-rejoin|heal.disk-loss-and-repair)
    SUITE=scripts/validation_heal/run_case.sh; EXTRA_ARGS=() ;;
  *)
    printf 'validation executor is not migrated: %s\n' "${CASE_ID}" >&2
    exit 2 ;;
esac

if [[ "${CASE_ID}" == pool.* || "${CASE_ID}" == bucket-replication.* || "${CASE_ID}" == site-replication.* ]]; then
  python3 -B -c 'import json,sys; catalog=json.load(open(sys.argv[1])); valid={case["id"] for module in catalog["modules"] for case in module["cases"]}; sys.exit(0 if sys.argv[2] in valid else 2)' \
    "${SCRIPT_DIR}/validation-cases.json" "${CASE_ID}" || {
      printf 'unknown canonical validation case: %s\n' "${CASE_ID}" >&2
      exit 2
    }
fi

if [ "${SUITE}" = rustfs-tier-test.sh ]; then
  EXTRA_ARGS+=(--log-file "${ARTIFACTS_DIR}/logs/${CASE_ID}.log")
fi

[ -f "${CANDIDATE_MANIFEST}" ] || { printf 'candidate manifest missing\n' >&2; exit 2; }
[[ "${TESTING_COMMIT}" =~ ^[0-9a-f]{40}$ ]] || { printf 'invalid testing commit\n' >&2; exit 2; }
[ "${TESTING_COMMIT}" = "$(git -C "${SCRIPT_DIR}" rev-parse HEAD)" ] || {
  printf 'testing commit does not match checkout\n' >&2; exit 2;
}
[ ! -e "${ARTIFACTS_DIR}" ] || { printf 'artifacts directory already exists\n' >&2; exit 2; }

mkdir -p -- "${ARTIFACTS_DIR}"
case_dir="${ARTIFACTS_DIR}/${CASE_ID}"
suite_rc=0
bash "${SCRIPT_DIR}/${SUITE}" --validation-case "${CASE_ID}" "${EXTRA_ARGS[@]}" \
  --candidate-manifest "${CANDIDATE_MANIFEST}" \
  --testing-commit "${TESTING_COMMIT}" \
  --validation-artifacts-dir "${case_dir}" -y \
  || suite_rc=$?

report_rc=0
python3 "${SCRIPT_DIR}/scripts/validation_report.py" \
  --catalog "${SCRIPT_DIR}/validation-cases.json" \
  --results-dir "${case_dir}/cases" \
  --output "${ARTIFACTS_DIR}/report.md" \
  --gate-case "${CASE_ID}" || report_rc=$?

if [ "${suite_rc}" -ne 0 ]; then exit "${suite_rc}"; fi
exit "${report_rc}"
