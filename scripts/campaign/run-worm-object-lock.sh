#!/usr/bin/env bash
#
# Deep WORM / Object Lock campaign entry.
#
# Runs S3C-602..605 from rustfs-s3-compat-test.sh against an endpoint that is
# already serving. It does not install RustFS, does not SSH to the fleet, and
# does not tear the cluster down. S3C-604 leaves its COMPLIANCE object in a
# bucket named s3c604-lk-<random>-<pid> until the one-day retain-until; the
# other three cases remove their buckets before returning.
#
# This is the campaign replacement for the shallow full-feature result
# "[PASS] WORM/object-lock retention set". That check created an at-worm-*
# lock bucket, set a one-day GOVERNANCE default, and had already written its
# objects before the default was in effect. It did not prove per-version
# delete or overwrite protection, and it did not run COMPLIANCE or legal hold.
#
# Wire this into scripts/run-auto-testing-full.sh (campaign tree) once the
# endpoint is healthy:
#
#   "${AUTO_TESTING_DIR}/scripts/campaign/run-worm-object-lock.sh" \
#     --endpoint "${S3_ENDPOINT}"
#
# Credentials default to the suite's rustfs@test / rustfs@test. Override with
# --access-key/--secret-key, or export RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
# or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY before invoking.
#
set -Eeuo pipefail

CAMPAIGN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${CAMPAIGN_DIR}/../.." && pwd)"

want_endpoint="${S3_ENDPOINT:-${RUSTFS_ENDPOINT:-}}"
want_region=""
want_dry=0
want_access="${RUSTFS_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:-rustfs@test}}"
want_secret="${RUSTFS_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-rustfs@test}}"

die() { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'USAGE'
Usage: scripts/campaign/run-worm-object-lock.sh --endpoint URL [options]

Run deep Object Lock cases S3C-602..605 against an existing S3 endpoint.

Options:
  --endpoint URL       S3 endpoint (or set S3_ENDPOINT / RUSTFS_ENDPOINT)
  --region REGION      AWS region passed to the CLI (default us-east-1)
  --access-key KEY     Access key (default rustfs@test, or AWS_ACCESS_KEY_ID)
  --secret-key SECRET  Secret key (default rustfs@test, or AWS_SECRET_ACCESS_KEY)
  --dry-run            List S3C-602..605 without calling the endpoint
  -h, --help           Show this help
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --endpoint) [ "$#" -ge 2 ] || die "--endpoint requires a URL"; want_endpoint="$2"; shift 2 ;;
    --region) [ "$#" -ge 2 ] || die "--region requires a value"; want_region="$2"; shift 2 ;;
    --access-key) [ "$#" -ge 2 ] || die "--access-key requires a value"; want_access="$2"; shift 2 ;;
    --secret-key) [ "$#" -ge 2 ] || die "--secret-key requires a value"; want_secret="$2"; shift 2 ;;
    --dry-run) want_dry=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

[ -n "${want_endpoint}" ] || die "missing --endpoint (or S3_ENDPOINT / RUSTFS_ENDPOINT)"
[ -n "${want_access}" ] || die "missing access key"
[ -n "${want_secret}" ] || die "missing secret key"

export RUSTFS_ACCESS_KEY="${want_access}"
export RUSTFS_SECRET_KEY="${want_secret}"

# shellcheck source=../../rustfs-s3-compat-test.sh
source "${ROOT}/rustfs-s3-compat-test.sh"

ENDPOINT="${want_endpoint}"
DRY_RUN="${want_dry}"
if [ -n "${want_region}" ]; then REGION="${want_region}"; fi
export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
export AWS_DEFAULT_REGION="${REGION}"
export AWS_EC2_METADATA_DISABLED=true

if [ "${DRY_RUN}" -eq 0 ]; then
  need_cmd aws "AWS CLI (s3api channel)"
  need_cmd jq "JSON processor"
fi

log "deep WORM suite: S3C-602 DefaultRetention, S3C-603 GovernanceProtection, S3C-604 ComplianceProtection, S3C-605 LegalHold"
log "endpoint: ${ENDPOINT}"
PASS=0; FAIL=0; UNSUPPORTED=0; SCHEDULED=0; FAILED_TESTS=()
worm_object_lock_tests
results_summary
[ "${SCHEDULED}" -eq 4 ] || die "expected 4 deep WORM cases, scheduled ${SCHEDULED}"
if [ "${FAIL}" -ne 0 ] || [ "${UNSUPPORTED}" -ne 0 ]; then
  exit 1
fi
log "deep WORM suite done"
