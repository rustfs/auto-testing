#!/usr/bin/env bash
set -Eeuo pipefail

TEST_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-validation-tier.XXXXXX")"
export RUSTFS_TIER_TEST_TMP="${TEST_ROOT}/source"
export TIER_SOURCE_ONLY=1
# shellcheck source=rustfs-tier-test.sh
source "${TEST_SCRIPT_DIR}/rustfs-tier-test.sh"
[ "${SCRIPT_DIR}" = "${TEST_SCRIPT_DIR}" ]
trap 'rm -rf -- "${TEST_ROOT}"' EXIT

RUN_TOKEN=unit-1
MOCK_CORRUPT=0
MOCK_GET_COUNT=0

new_bucket() { :; }
add_tier() { RC_CODE=0; }
rc_run() { RC_CODE=0; }
run_transition() { printf '{"job_id":"job-1"}\n'; }
wait_transition_job() { [ "$1" = job-1 ]; }
sleep() { :; }

aws_run() {
  local operation="${2:-}" argument previous="" output=""
  AWS_CODE=0
  AWS_OUT='{}'
  for argument in "$@"; do
    if [ "${previous}" = "--bucket" ]; then
      [ "${argument}" = "val-tier-unit-1" ] || return 1
    fi
    previous="${argument}"
  done
  case "${operation}" in
    put-object) ;;
    head-object)
      if [ "${MOCK_GET_COUNT}" -ge 1 ]; then
        AWS_OUT='{"StorageClass":"WARM","Restore":"ongoing-request=\"false\""}'
      else
        AWS_OUT='{"StorageClass":"WARM"}'
      fi
      ;;
    get-object)
      MOCK_GET_COUNT=$((MOCK_GET_COUNT + 1))
      output="${@: -1}"
      if [ "${MOCK_CORRUPT}" -eq 1 ]; then
        printf 'corrupt' > "${output}"
      else
        cp "${TEST_TMP}/validation-source.bin" "${output}"
      fi
      ;;
    restore-object) ;;
    *) printf 'unexpected AWS operation: %s\n' "${operation}" >&2; return 1 ;;
  esac
}

test_validation_tiering_transition_and_restore
[ "${MOCK_GET_COUNT}" -eq 2 ]
jq -e '.tier == "WARM" and .restore_complete == true and
  (.source_sha256 | test("^[0-9a-f]{64}$")) and
  .source_sha256 == .transitioned_sha256 and .source_sha256 == .restored_sha256' \
  "${TEST_TMP}/validation-object.json" >/dev/null

MOCK_CORRUPT=1
MOCK_GET_COUNT=0
if test_validation_tiering_transition_and_restore; then
  printf 'corrupted transitioned object passed\n' >&2
  exit 1
fi

TIER_SOURCE_ONLY=0 "${TEST_SCRIPT_DIR}/rustfs-tier-test.sh" --validation-case tiering.transition-and-restore \
  --topology single-single --dry-run --yes --package-url https://example.invalid/rustfs.deb \
  > "${TEST_ROOT}/validation-dry-run.log"
grep -q '\[DRY-RUN\] tiering.transition-and-restore TransitionAndRestore' "${TEST_ROOT}/validation-dry-run.log"
if grep -q '\[DRY-RUN\] TIER-' "${TEST_ROOT}/validation-dry-run.log"; then
  printf 'validation mode scheduled legacy tier cases\n' >&2
  exit 1
fi
