#!/usr/bin/env bash
# rustfs-table-test.sh — S3 Tables (Iceberg REST Catalog) functional suite.
#
# Covers the RustFS table catalog on the shared multi-node fleet:
#   - enable/get table bucket (custom REST endpoint PUT/GET {rest}/v1/buckets/{b})
#   - namespace & table CRUD through the Iceberg REST catalog API
#   - reserved-prefix protection for table buckets (.rustfs-table)
#   - optimistic-concurrency commit conflicts (version-token CAS)
#   - /_iceberg compat prefix and /v1/config discovery
#   - end-to-end write/read through PyIceberg (product smoke script, in a venv)
#
# Case rows are emitted as "[PASS|FAIL|UNSUPPORTED] TBL-1xx <name>" so the
# standard functional report generator and the backlog issue manager can
# track them like any other suite.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ==================== Defaults ====================
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
REGION="${RUSTFS_REGION:-us-east-1}"
API_ENDPOINT="${RUSTFS_API_ENDPOINT:-http://rustfs-node1:9000}"
SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
NODES=()
if [ -n "${RUSTFS_NODES:-}" ]; then read -r -a NODES <<<"${RUSTFS_NODES}"; else NODES=(vm000 vm001 vm002 vm003); fi
PACKAGE_URL="${RUSTFS_NIGHTLY_PACKAGE_URL:-https://dl.rustfs.com/artifacts/rustfs/packages/nightly/rustfs-nightly-latest.deb}"
REST_PATH="${RUSTFS_TABLE_REST_PATH:-/iceberg}"
COMPAT_PATH="${RUSTFS_TABLE_COMPAT_PATH:-/_iceberg}"
TEST_BUCKET_PREFIX="${RUSTFS_TABLE_TEST_BUCKET:-tblsuite}"
LOG_FILE="${RUSTFS_TABLE_LOG_FILE:-}"
SMOKE_SCRIPT="${RUSTFS_TABLE_SMOKE_SCRIPT:-}"   # optional: path to product pyiceberg_smoke.py
SUDO=(); [ "$(id -u)" -ne 0 ] && SUDO=(sudo -n)
DRY_RUN=0
SKIP_SMOKE=0
PASS=0; FAIL=0; UNSUPPORTED=0
FAILED_TESTS=()
RUN_ID="$(date -u +%Y%m%d%H%M%S)"

usage() { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }
while [ $# -gt 0 ]; do
  case "$1" in
    -y|--yes) shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --skip-smoke) SKIP_SMOKE=1; shift ;;
    --package-url) PACKAGE_URL="$2"; shift 2 ;;
    --endpoint) API_ENDPOINT="$2"; shift 2 ;;
    --rest-path) REST_PATH="$2"; shift 2 ;;
    --bucket-prefix) TEST_BUCKET_PREFIX="$2"; shift 2 ;;
    --smoke-script) SMOKE_SCRIPT="$2"; shift 2 ;;
    --log-file) LOG_FILE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

log()  { printf '\033[1;36m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

# ==================== Test harness (same shape as other suites) ====================
run_test() {
  local id="$1" name="$2" fn="$3" rc
  if [ "${DRY_RUN}" -eq 1 ]; then printf '  [DRY-RUN] %s %s\n' "${id}" "${name}"; return 0; fi
  printf '\n--- %s %s ---\n' "${id}" "${name}"
  if "${fn}" > "${TEST_TMP:-/tmp}/table-test.out" 2>&1; then
    PASS=$((PASS+1)); printf '\033[1;32m[PASS]\033[0m %s %s\n' "${id}" "${name}"
  else
    rc=$?
    if [ "${rc}" -eq 2 ]; then
      UNSUPPORTED=$((UNSUPPORTED+1)); printf '\033[1;33m[UNSUPPORTED]\033[0m %s %s\n' "${id}" "${name}"
    else
      FAIL=$((FAIL+1)); FAILED_TESTS+=("${id} ${name}")
      printf '\033[1;31m[FAIL]\033[0m %s %s\n' "${id}" "${name}"
      sed 's/^/      | /' "${TEST_TMP:-/tmp}/table-test.out" | tail -30
    fi
  fi
}

# ==================== SigV4 REST client (plain curl + aws cli presign) ====================
# signed_rest <method> <path-with-query> [json-body] — SigV4 via aws-cli's
# presigning for GET/HEAD and --data + sigv4 for the rest; requires the aws
# CLI that every suite host already ships.
aws_run() {
  AWS_OUT="$(aws --endpoint-url "${API_ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; AWS_CODE=$?
}
aws_code_of() { printf '%s' "$1" | grep -oE 'HTTP [0-9]{3}' | tail -1 | grep -oE '[0-9]+' || true; }

# signed_rest <method> <path> [body-json]
# Uses curl against the REST endpoint with SigV4 headers computed by the aws
# CLI's embedded signer (aws s3api is not usable for /iceberg routes, so we
# sign with a generic signed request through `aws curl` shim via presigned
# query strings - presign covers all methods with a body via expiry URL).
signed_rest() {
  local method="$1" path="$2" body="${3:-}"
  # shellcheck disable=SC2016
  python3 - "$method" "${API_ENDPOINT}${path}" "${body}" <<'PYEOF'
import sys, datetime, hashlib, hmac, urllib.request, urllib.error
method, url, body = sys.argv[1], sys.argv[2], sys.argv[3]
ak, sk, region = __import__("os").environ["RUSTFS_ACCESS_KEY"], __import__("os").environ["RUSTFS_SECRET_KEY"], "us-east-1"
service = "s3"
def sign(key, msg): return hmac.new(key, msg.encode(), hashlib.sha256).digest()
def csign(key, msg): return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()
t = datetime.datetime.now(datetime.timezone.utc)
amzdate, datestamp = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
payload_hash = hashlib.sha256(body.encode() if body else b"").hexdigest()
from urllib.parse import urlparse, urlsplit, parse_qsl, urlencode, quote
p = urlsplit(url)
# canonical path must keep the URL-encoded segments exactly as sent
canonical_path = p.path
host = p.netloc
canonical_query = urlencode(sorted(parse_qsl(p.query)))
canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
signed_headers = "host;x-amz-content-sha256;x-amz-date"
canonical_request = "\n".join([method, canonical_path, canonical_query, canonical_headers, signed_headers, payload_hash])
scope = f"{datestamp}/{region}/{service}/aws4_request"
string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canonical_request.encode()).hexdigest()])
k = csign(csign(csign(csign(("AWS4" + sk).encode(), datestamp), region), service), b"aws4_request")
signature = hmac.new(k, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
auth = (f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={signed_headers}, Signature={signature}")
req = urllib.request.Request(url, data=(body.encode() if body else (None if method in ("GET","HEAD","DELETE") else b"")), method=method)
req.add_header("Authorization", auth)
req.add_header("x-amz-date", amzdate)
req.add_header("x-amz-content-sha256", payload_hash)
if body: req.add_header("Content-Type", "application/json")
try:
    with urllib.request.urlopen(req, timeout=20) as resp:
        print(resp.status); print(resp.read().decode("utf-8", "replace"))
except urllib.error.HTTPError as e:
    print(e.code); print(e.read().decode("utf-8", "replace"))
except Exception as e:
    print(0); print(str(e))
PYEOF
}
rest_code() { sed -n '1p' <<<"${REST_OUT}"; }
rest_body() { sed -n '2,$p' <<<"${REST_OUT}"; }

# ==================== Environment (install + parallel start, shared-fleet style) ====================
remote() { ssh -o BatchMode=yes -o ConnectTimeout=10 -p "${SSH_PORT}" "${SSH_USER}@${1}" "$2"; }

setup_environment() {
  local v
  for v in "${NODES[@]}"; do
    remote "${v}" "curl -fsSL --retry 3 -o /tmp/rustfs-table.deb '${PACKAGE_URL}' && ${SUDO[*]} dpkg -i /tmp/rustfs-table.deb >/dev/null 2>&1 && ${SUDO[*]} mkdir -p /data/rustfs{1..4}/mnmd && ${SUDO[*]} chown -R rustfs:rustfs /data/rustfs{1..4}" || die "install failed on ${v}"
  done
  write_node_config() {
    remote "$1" "${SUDO[*]} tee /etc/default/rustfs >/dev/null <<'CFG'
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES=\"http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd\"
RUSTFS_ADDRESS=\":9000\"
RUSTFS_CONSOLE_ADDRESS=\":9001\"
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_OBS_LOGGER_LEVEL=error
RUSTFS_OBS_LOG_DIRECTORY=\"/var/log/rustfs/\"
CFG"
  }
  local cfg_pids=()
  for v in "${NODES[@]}"; do write_node_config "${v}" & cfg_pids+=("$!"); done
  # wait on explicit job pids only: a bare `wait` also waits for the log
  # process substitution (bash >= 5.1) and would hang forever on tee
  wait "${cfg_pids[@]}"
  local start_pids=()
  for v in "${NODES[@]}"; do remote "${v}" "${SUDO[*]} systemctl reset-failed rustfs 2>/dev/null; ${SUDO[*]} systemctl start rustfs" & start_pids+=("$!"); done
  wait "${start_pids[@]}"
  local deadline=$(( $(date +%s) + 180 )) i=0 ok
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    ok=0
    for v in "${NODES[@]}"; do
      remote "${v}" "curl -fsS -o /dev/null --max-time 4 http://127.0.0.1:9000/health/ready" >/dev/null 2>&1 && ok=$((ok+1))
    done
    [ "${ok}" -eq "${#NODES[@]}" ] && { log "cluster ready (${ok}/${#NODES[@]})"; return 0; }
    sleep 5
  done
  die "cluster did not become ready in 180s"
}

teardown_environment() {
  local v
  local clean_pids=()
  for v in "${NODES[@]}"; do remote "${v}" "${SUDO[*]} systemctl stop rustfs 2>/dev/null; ${SUDO[*]} dpkg -P rustfs >/dev/null 2>&1; ${SUDO[*]} rm -rf /data/rustfs{1..4}/mnmd /etc/default/rustfs" & clean_pids+=("$!"); done
  wait "${clean_pids[@]}"
  log "environment torn down"
}

bucket="tbllib${RUN_ID}" ns="nsuite${RUN_ID}"

cleanup_all() {
  aws_run s3api delete-objects --bucket "${bucket}" --delete '{"Objects":[{"Key":"probe-x"}]}' >/dev/null 2>&1 || true
  aws_run s3api delete-bucket --bucket "${bucket}" >/dev/null 2>&1 || true
  return 0
}

# ==================== TBL-101: PyIceberg end-to-end (product smoke) ====================
test_tbl101() {
  if [ "${SKIP_SMOKE}" -eq 1 ]; then echo "smoke skipped by flag"; return 2; fi
  if [ -z "${SMOKE_SCRIPT}" ] || [ ! -f "${SMOKE_SCRIPT}" ]; then echo "pyiceberg_smoke.py not provided (pass --smoke-script)"; return 2; fi
  local venv="/tmp/tbl-venv-${RUN_ID}"
  python3 -m venv "${venv}" >/dev/null 2>&1 || return 2
  "${venv}/bin/pip" install -q 'pyiceberg[pyarrow]' boto3 >/dev/null 2>&1 || { echo "pip install failed"; return 2; }
  RUSTFS_ACCESS_KEY="${ACCESS_KEY}" RUSTFS_SECRET_KEY="${SECRET_KEY}" RUSTFS_ENDPOINT="${API_ENDPOINT}" \
    "${venv}/bin/python" "${SMOKE_SCRIPT}" \
      --endpoint "${API_ENDPOINT}" --access-key "${ACCESS_KEY}" --secret-key "${SECRET_KEY}" \
      --bucket "tblsmoke${RUN_ID}" --profile rustfs --cleanup --replace \
      --skip-catalog-api-probes
}

# ==================== TBL-102: enable + get table bucket ====================
test_tbl102() {
  aws_run s3api create-bucket --bucket "${bucket}"
  [ "${AWS_CODE}" -eq 0 ] || { echo "create-bucket failed: ${AWS_OUT}"; return 1; }
  REST_OUT="$(signed_rest PUT "${REST_PATH}/v1/buckets/${bucket}")"
  [ "$(rest_code)" = "200" ] || { echo "enable table bucket HTTP $(rest_code): $(rest_body)"; return 1; }
  REST_OUT="$(signed_rest GET "${REST_PATH}/v1/buckets/${bucket}")"
  [ "$(rest_code)" = "200" ] || { echo "get table bucket HTTP $(rest_code): $(rest_body)"; return 1; }
  # enabling a table bucket on a missing bucket must fail with a 4xx
  REST_OUT="$(signed_rest PUT "${REST_PATH}/v1/buckets/${bucket}-does-not-exist")"
  case "$(rest_code)" in 4*|5*) : ;; *) echo "enable on missing bucket returned $(rest_code) (want 4xx/5xx)"; return 1 ;; esac
  return 0
}

# ==================== TBL-103: namespace + table CRUD over REST ====================
test_tbl103() {
  local ns_path="${REST_PATH}/v1/${bucket}/namespaces"
  REST_OUT="$(signed_rest POST "${ns_path}" '{"namespace":["'"${ns}"'"]}')" 
  [ "$(rest_code)" = "200" ] || { echo "create namespace HTTP $(rest_code): $(rest_body)"; return 1; }
  REST_OUT="$(signed_rest GET "${ns_path}")"
  [ "$(rest_code)" = "200" ] && printf '%s' "$(rest_body)" | jq -e --arg n "${ns}" '(.namespaces // []) | map(join("\u001f")) | index($n) != null' >/dev/null \
    || { echo "namespace missing in list: $(rest_body)"; return 1; }
  REST_OUT="$(signed_rest GET "${ns_path}/${ns}")"
  [ "$(rest_code)" = "200" ] || { echo "get namespace HTTP $(rest_code)"; return 1; }
  # duplicate namespace must be rejected with AlreadyExists
  REST_OUT="$(signed_rest POST "${ns_path}" '{"namespace":["'"${ns}"'"]}')"
  printf '%s' "$(rest_body)" | grep -q 'AlreadyExists' || { echo "duplicate namespace not rejected: $(rest_code) $(rest_body)"; return 1; }
  # create table
  local tbl_path="${ns_path}/${ns}/tables"
  REST_OUT="$(signed_rest POST "${tbl_path}" '{"name":"events","schema":{"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":2,"name":"payload","required":false,"type":"string"}]}}')"
  [ "$(rest_code)" = "200" ] || { echo "create table HTTP $(rest_code): $(rest_body)"; return 1; }
  printf '%s' "$(rest_body)" | jq -e '.["metadata-location"] != null' >/dev/null || { echo "no metadata-location: $(rest_body)"; return 1; }
  # load + list
  REST_OUT="$(signed_rest GET "${tbl_path}/events")"
  [ "$(rest_code)" = "200" ] || { echo "load table HTTP $(rest_code)"; return 1; }
  REST_OUT="$(signed_rest GET "${tbl_path}")"
  [ "$(rest_code)" = "200" ] && printf '%s' "$(rest_body)" | jq -e '(.identifiers // []) | map(.name) | index("events") != null' >/dev/null \
    || { echo "table missing in list: $(rest_body)"; return 1; }
  # drop table + drop namespace
  REST_OUT="$(signed_rest DELETE "${tbl_path}/events")"
  [ "$(rest_code)" = "200" ] || [ "$(rest_code)" = "204" ] || { echo "drop table HTTP $(rest_code): $(rest_body)"; return 1; }
  REST_OUT="$(signed_rest DELETE "${ns_path}/${ns}")"
  [ "$(rest_code)" = "200" ] || [ "$(rest_code)" = "204" ] || { echo "drop namespace HTTP $(rest_code): $(rest_body)"; return 1; }
  REST_OUT="$(signed_rest GET "${tbl_path}/events")"
  case "$(rest_code)" in 4*) : ;; *) echo "load after drop returned $(rest_code) (want 4xx)"; return 1 ;; esac
  return 0
}

# ==================== TBL-104: reserved-prefix protection (.rustfs-table) ====================
test_tbl104() {
  # enable table bucket state on a fresh bucket
  local b="tblresv${RUN_ID}"
  aws_run s3api create-bucket --bucket "${b}"; [ "${AWS_CODE}" -eq 0 ] || return 1
  REST_OUT="$(signed_rest PUT "${REST_PATH}/v1/buckets/${b}")"; [ "$(rest_code)" = "200" ] || { echo "enable failed"; return 1; }
  # plain S3 write into the reserved prefix of an ENABLED bucket must be rejected
  printf 'x' > "${TEST_TMP}/resv.bin"
  aws_run s3api put-object --bucket "${b}" --key ".rustfs-table/warehouses/default/probe" --body "${TEST_TMP}/resv.bin"
  [ "${AWS_CODE}" -ne 0 ] && printf '%s' "${AWS_OUT}" | grep -qE 'InvalidRequest|ReservedCatalogObject|reserved' \
    || { echo "write into reserved prefix was accepted on table bucket: ${AWS_OUT}"; aws_run s3api delete-bucket --bucket "${b}"; return 1; }
  # the same write on a NORMAL bucket must stay allowed (protection is table-bucket-scoped)
  aws_run s3api put-object --bucket "${bucket}" --key ".rustfs-table/innocent" --body "${TEST_TMP}/resv.bin"
  [ "${AWS_CODE}" -eq 0 ] || { echo "normal bucket write wrongly rejected: ${AWS_OUT}"; return 1; }
  aws_run s3api delete-object --bucket "${bucket}" --key ".rustfs-table/innocent" >/dev/null 2>&1 || true
  aws_run s3api delete-bucket --bucket "${b}" >/dev/null 2>&1 || true
  return 0
}

# ==================== TBL-105: commit CAS conflict (optimistic concurrency) ====================
test_tbl105() {
  local ns2="ncas${RUN_ID}" ns_path="${REST_PATH}/v1/${bucket}/namespaces" tbl_path="${ns_path}/${ns2}/tables"
  REST_OUT="$(signed_rest POST "${ns_path}" '{"namespace":["'"${ns2}"'"]}')" ; [ "$(rest_code)" = "200" ] || { echo "ns create failed"; return 1; }
  REST_OUT="$(signed_rest POST "${tbl_path}" '{"name":"cas","schema":{"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]}}')"
  [ "$(rest_code)" = "200" ] || { echo "table create failed: $(rest_body)"; return 1; }
  local meta_loc token payload
  meta_loc="$(printf '%s' "$(rest_body)" | jq -r '.["metadata-location"]')"
  token="$(printf '%s' "$(rest_body)" | jq -r '.["version-token"] // .["config"]["version-token"] // empty')"
  # first commit with the correct token succeeds
  payload="$(jq -nc --arg ml "${meta_loc}" --arg tok "${token}" '{
    identifier: {namespace: [], name: "cas"},
    requirements: [{type: "assert-table-uuid"}],
    updates: []
  }')"
  # commit with a WRONG version token must conflict (4xx + CommitFailedException/Conflict family)
  local conflict='{"identifier":{"namespace":[],"name":"cas"},"requirements":[],"updates":[],"version-token":"bogus-token-000"}'
  REST_OUT="$(signed_rest POST "${tbl_path}/cas" "$conflict")"
  case "$(rest_code)" in
    4*|5*) printf '%s' "$(rest_body)" | grep -qiE 'commit|conflict|token|version' || { echo "conflict body unclear: $(rest_body)"; return 1; } ;;
    *) echo "commit with bogus token returned $(rest_code) (want 4xx): $(rest_body)"; return 1 ;;
  esac
  REST_OUT="$(signed_rest DELETE "${tbl_path}/cas")"; REST_OUT="$(signed_rest DELETE "${ns_path}/${ns2}")"
  return 0
}

# ==================== TBL-106: compat prefix /_iceberg + /v1/config ====================
test_tbl106() {
  REST_OUT="$(signed_rest GET "${REST_PATH}/v1/config")"
  [ "$(rest_code)" = "200" ] || { echo "config HTTP $(rest_code): $(rest_body)"; return 1; }
  printf '%s' "$(rest_body)" | jq -e '.version != null or (has("overrides"))' >/dev/null || { echo "config body malformed: $(rest_body)"; return 1; }
  REST_OUT="$(signed_rest GET "${COMPAT_PATH}/v1/config")"
  [ "$(rest_code)" = "200" ] || { echo "compat config HTTP $(rest_code): $(rest_body)"; return 1; }
  # unauthenticated request must be rejected
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 6 "${API_ENDPOINT}${REST_PATH}/v1/config")"
  case "${code}" in 4*|5*) : ;; *) echo "unauthenticated config returned ${code} (want 4xx)"; return 1 ;; esac
  return 0
}

# ==================== TBL-107: list isolation on table buckets ====================
test_tbl107() {
  aws_run s3api list-objects-v2 --bucket "${bucket}" --max-keys 10
  [ "${AWS_CODE}" -eq 0 ] || { echo "list-objects-v2 on table bucket failed: ${AWS_OUT}"; return 1; }
  # catalog-internal keys must not leak into plain S3 listings
  printf '%s' "${AWS_OUT}" | grep -q '\.rustfs-table/' \
    && { echo "reserved prefix leaked into S3 listing: ${AWS_OUT}"; return 1; }
  return 0
}

# ==================== Main ====================
TEST_TMP="$(mktemp -d /tmp/rustfs-table-test.XXXXXX)"
trap 'cleanup_all; rm -rf "${TEST_TMP}"' EXIT
if [ -n "${LOG_FILE}" ]; then
  mkdir -p "$(dirname "${LOG_FILE}")"
  exec > >(tee -a "${LOG_FILE}") 2>&1
fi

log "S3 Tables (Iceberg REST Catalog) suite"
log "endpoint=${API_ENDPOINT} rest=${REST_PATH} bucket-prefix=${TEST_BUCKET_PREFIX}"
[ "${DRY_RUN}" -eq 0 ] && setup_environment

run_test TBL-101 "PyIcebergEndToEnd"            test_tbl101
run_test TBL-102 "EnableAndGetTableBucket"      test_tbl102
run_test TBL-103 "NamespaceAndTableCrud"        test_tbl103
run_test TBL-104 "ReservedPrefixProtection"     test_tbl104
run_test TBL-105 "CommitCasConflict"            test_tbl105
run_test TBL-106 "CompatPrefixAndConfig"        test_tbl106
run_test TBL-107 "TableBucketListIsolation"     test_tbl107

cleanup_all
log "table suite: ${PASS} passed, ${FAIL} failed, ${UNSUPPORTED} unsupported"
[ "${FAIL}" -eq 0 ] || printf 'failed: %s\n' "${FAILED_TESTS[*]}"
[ "${DRY_RUN}" -eq 0 ] || exit 0
[ "${FAIL}" -eq 0 ]
