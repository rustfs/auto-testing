#!/usr/bin/env bash
#
# rustfs-performance-testing.sh
# RustFS object storage benchmark script (fixed): methodology + execution + result parsing
#
# Methodology
#   1) Methods: GET / PUT / MIXED (warp default mix 45% GET + 55% PUT)
#   2) Object sizes: 1KiB 4KiB 16KiB 128KiB 1MiB 4MiB 8MiB 16MiB 32MiB 64MiB
#   3) Concurrency: 64; duration per round: 5m; sleep between rounds: 60s; GET objects: 2500
#   4) Order: GET all sizes -> PUT all sizes -> MIXED all sizes
#   5) Result parsing: after each round, parse the warp output into
#      summary.tsv (machine-readable) and summary.md (Markdown tables)
#
# Requirements: warp >= v1.6 (MinIO warp), bash, awk/sed/grep
# Note: warp v1.6.1's put does not support --objects; handled automatically
#       (the flag is only passed for get/mixed)
#
# Env overrides (fixed defaults are used when unset):
#   WARP_HOST WARP_ACCESS_KEY WARP_SECRET_KEY WARP_BUCKET
#   WARP_CONCURRENCY WARP_DURATION WARP_GET_OBJECTS WARP_SLEEP_BETWEEN_ROUNDS
#   WARP_RESULT_DIR
#   WARP_METHODS WARP_SIZES   # restrict methods/sizes (comma/space separated); empty = full run
#   WARP_ROUND_TIMEOUT WARP_MAX_CONSECUTIVE_FAILURES

set -u -o pipefail

HOST="${WARP_HOST:-rustfs-node1:9000,rustfs-node2:9000,rustfs-node3:9000,rustfs-node4:9000}"
ACCESS_KEY="${WARP_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${WARP_SECRET_KEY:-rustfs@test}"
BUCKET="${WARP_BUCKET:-warp-benchmark-bucket}"
CONCURRENCY="${WARP_CONCURRENCY:-64}"
DURATION="${WARP_DURATION:-5m}"
GET_OBJECTS="${WARP_GET_OBJECTS:-2500}"
SLEEP_BETWEEN_ROUNDS="${WARP_SLEEP_BETWEEN_ROUNDS:-60}"
ROUND_TIMEOUT="${WARP_ROUND_TIMEOUT:-900}"
MAX_CONSECUTIVE_FAILURES="${WARP_MAX_CONSECUTIVE_FAILURES:-0}"
RESULT_DIR="${WARP_RESULT_DIR:-$(pwd)/warp-bench-results-$(date +%Y%m%d-%H%M%S)}"

if [ -n "${WARP_SIZES:-}" ] && [ "${WARP_SIZES}" != "all" ] && [ "${WARP_SIZES}" != "ALL" ]; then
  read -r -a SIZES <<<"${WARP_SIZES//,/ }"
else
  SIZES=(1KiB 4KiB 16KiB 128KiB 1MiB 4MiB 8MiB 16MiB 32MiB 64MiB)
fi
if [ -n "${WARP_METHODS:-}" ] && [ "${WARP_METHODS}" != "all" ] && [ "${WARP_METHODS}" != "ALL" ]; then
  read -r -a METHODS <<<"${WARP_METHODS//,/ }"
else
  METHODS=(get put mixed)
fi
TOTAL_ROUNDS=$(( ${#METHODS[@]} * ${#SIZES[@]} ))
ROUND=0
PARSED_ROUNDS=0
FAILED_ROUNDS=0
CONSECUTIVE_FAILURES=0

# --parse-only <result-dir>: parse an existing result dir (${method}_${size}.txt) without running
# Only round files with a successful terminal execution status can be certified.
if [[ "${1:-}" == "--parse-only" && -n "${2:-}" ]]; then
  RESULT_DIR="$2"
fi

LOG_FILE="${RESULT_DIR}/master.log"
SUMMARY_TSV="${RESULT_DIR}/summary.tsv"
SUMMARY_MD="${RESULT_DIR}/summary.md"
ROUNDS_TSV="${RESULT_DIR}/rounds.tsv"
PARSE_FAILURE_REASON=""

if [[ "${1:-}" != "--parse-only" ]] && ! command -v warp >/dev/null 2>&1; then
  echo "error: warp command not found; install MinIO warp first" >&2
  exit 1
fi

log() {
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') $*" | tee -a "${LOG_FILE}"
}

# ---- Result parsing ----

round_error_lines() {
  awk '
    { line = tolower($0)
      if (line ~ /<error>|serviceunavailable|the service is unavailable|errors:[[:space:]]*[1-9][0-9]*/) count++ }
    END { print count + 0 }
  ' "$1"
}

record_round() {
  local method="$1" size="$2" file="$3" verdict="$4" reason="$5" warp_rc="$6" capture_rc="$7" elapsed="$8"
  local reported=0 errors=0
  if [[ "${verdict}" != NOT_RUN && -f "${file}" ]]; then
    grep -q '^Report: ' "${file}" && reported=1
    errors=$(round_error_lines "${file}")
  fi
  if [[ "${warp_rc}" == 124 ]]; then
    reason=timeout_before_report
    [[ "${reported}" == 0 ]] || reason=timeout_after_report
  elif [[ "${warp_rc}" == 137 ]]; then
    reason=killed_before_report
    [[ "${reported}" == 0 ]] || reason=killed_after_report
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${method}" "${size}" "${verdict}" "${reason}" "${warp_rc}" "${capture_rc}" "${elapsed}" "${reported}" "${errors}" >> "${ROUNDS_TSV}"
}

# Extract the raw Average / Reqs / TTFB lines of a section (GET/PUT/Total etc.)
section_lines() {
  awk -v sec="$2" '
    /^Report: / { cur = $2; sub(/\.$/, "", cur) }
    cur == sec && /^ *\* Average:/ { avg = $0 }
    cur == sec && /^ *\* Reqs:/   { reqs = $0 }
    cur == sec && /^ *\* TTFB:/   { ttfb = $0 }
    END {
      if (avg != "") print avg
      if (reqs != "") print reqs
      if (ttfb != "") print ttfb
    }
  ' "$1"
}

# Extract fields from a stats block: tp objs avg p50 p90 p99 ttfb_avg ttfb_p99 ttfb_worst
field() {
  case "$2" in
    tp)         echo "$1" | sed -n 's/^ *\* Average: \(.*\), \([0-9.]*\) obj\/s.*/\1/p' ;;
    objs)       echo "$1" | sed -n 's/^ *\* Average: .*, \([0-9.]*\) obj\/s.*/\1/p' ;;
    avg)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: \([^,]*\),.*/\1/p' ;;
    p50)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: [^,]*, 50%: \([^,]*\),.*/\1/p' ;;
    p90)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: [^,]*, 50%: [^,]*, 90%: \([^,]*\),.*/\1/p' ;;
    p99)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: [^,]*, 50%: [^,]*, 90%: [^,]*, 99%: \([^,]*\),.*/\1/p' ;;
    ttfb_avg)   echo "$1" | sed -n 's/^ *\* TTFB: Avg: \([^,]*\),.*/\1/p' ;;
    ttfb_p99)   echo "$1" | sed -n 's/^ *\* TTFB: .*99th: \([^,]*\),.*/\1/p' ;;
    ttfb_worst) echo "$1" | sed -n 's/^ *\* TTFB: .*Worst: \([^ ]*\).*/\1/p' ;;
    *) echo "" ;;
  esac
}

# Parse one round's output and append a row to summary.tsv
parse_round() {
  local method="$1" size="$2" file="$3"
  PARSE_FAILURE_REASON=""
  if [[ "$(tail -n 1 "$file")" != "RustFS round status: warp=0 capture=0" ]]; then
    PARSE_FAILURE_REASON=missing_or_failed_round_status
    log "PARSE_FAILED method=${method} size=${size} reason=missing_or_failed_round_status file=${file}"
    return 1
  fi
  if [[ "$(round_error_lines "$file")" -gt 0 ]]; then
    PARSE_FAILURE_REASON=logged_errors
    log "PARSE_FAILED method=${method} size=${size} reason=logged_errors file=${file} (metrics retained in raw output)"
    return 1
  fi
  local line
  if [[ "${method}" == "mixed" ]]; then
    local total get put
    total=$(section_lines "$file" Total)
    get=$(section_lines "$file" GET)
    put=$(section_lines "$file" PUT)
    line=$(printf 'mixed\t%s\t%s\t%s\t%s\t%s' \
      "$size" \
      "$(field "${total}" tp)" \
      "$(field "${total}" objs)" \
      "$(field "${get}" avg)" \
      "$(field "${put}" avg)")
  else
    local sec
    sec=$(printf '%s' "${method}" | tr '[:lower:]' '[:upper:]')
    local stats
    stats=$(section_lines "$file" "${sec}")
    line=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
      "${method}" "${size}" \
      "$(field "${stats}" tp)" \
      "$(field "${stats}" objs)" \
      "$(field "${stats}" avg)" \
      "$(field "${stats}" p50)" \
      "$(field "${stats}" p90)" \
      "$(field "${stats}" p99)" \
      "$(field "${stats}" ttfb_avg)" \
      "$(field "${stats}" ttfb_p99)" \
      "$(field "${stats}" ttfb_worst)")
  fi
  # TTFB is optional in warp; throughput and request statistics define a result.
  if ! printf '%s\n' "${line}" | awk -F '\t' '
    { last = $1 == "mixed" ? 6 : 8
      for (i = 3; i <= last; i++) if ($i !~ /[^[:space:]]/) exit 1 }
  '; then
    PARSE_FAILURE_REASON=missing_required_metrics
    log "PARSE_FAILED method=${method} size=${size} reason=missing_required_metrics file=${file}"
    return 1
  fi
  printf '%s\n' "${line}" >> "${SUMMARY_TSV}"
}

# Build summary.md (Markdown tables) from summary.tsv
gen_summary_md() {
  {
    echo "# RustFS Performance Test Results"
    echo ""
    echo "- Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "- Target: ${HOST}"
    echo "- Concurrency: ${CONCURRENCY}; duration: ${DURATION}; sleep: ${SLEEP_BETWEEN_ROUNDS}s; GET objects: ${GET_OBJECTS}"
    echo "- Methods: ${METHODS[*]-}; sizes: ${SIZES[*]-}"
    echo "- Rounds: selected=${TOTAL_ROUNDS}; parsed=${PARSED_ROUNDS}; failed=${FAILED_ROUNDS}"
    echo "- Timeout covers preparation, workload and teardown. Report presence only distinguishes timeout before/after a report; it does not prove the internal phase."
    echo ""
    echo "## Every selected round"
    echo ""
    echo "| Method | Size | Verdict | Reason | Warp exit | Capture exit | Elapsed seconds | Report present | Error lines |"
    echo "|---|---|---|---|---|---|---|---|---|"
    awk -F '\t' 'NR > 1 { printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", $1,$2,$3,$4,$5,$6,$7,$8,$9 }' "${ROUNDS_TSV}"
    echo ""
  } > "${SUMMARY_MD}"

  for m in get put; do
    {
      echo "## $(printf '%s' "$m" | tr '[:lower:]' '[:upper:]') Results"
      echo ""
      echo "| Object Size | Avg Throughput | Avg obj/s | Avg Latency | P50 | P90 | P99 | TTFB Avg | TTFB P99 | TTFB Worst |"
      echo "|-------------|----------------|-----------|-------------|-----|-----|-----|----------|----------|------------|"
    } >> "${SUMMARY_MD}"
    while IFS=$'\t' read -r method size tp objs avg p50 p90 p99 ttfb_avg ttfb_p99 ttfb_worst; do
      [[ "${method}" == "${m}" ]] && \
        echo "| ${size} | ${tp} | ${objs} | ${avg} | ${p50} | ${p90} | ${p99} | ${ttfb_avg} | ${ttfb_p99} | ${ttfb_worst} |" >> "${SUMMARY_MD}"
    done < "${SUMMARY_TSV}"
    echo "" >> "${SUMMARY_MD}"
  done

  {
    echo "## MIXED Results (Total)"
    echo ""
    echo "| Object Size | Total Avg Throughput | Total Avg obj/s | MIXED GET Avg | MIXED PUT Avg |"
    echo "|-------------|----------------------|-----------------|---------------|---------------|"
  } >> "${SUMMARY_MD}"
  while IFS=$'\t' read -r method size tp objs gavg pavg rest; do
    [[ "${method}" == "mixed" ]] && \
      echo "| ${size} | ${tp} | ${objs} | ${gavg} | ${pavg} |" >> "${SUMMARY_MD}"
  done < "${SUMMARY_TSV}"
  echo "" >> "${SUMMARY_MD}"
}

# ---- Main flow ----

mkdir -p "${RESULT_DIR}" || exit 1
log "CONFIG host=${HOST} bucket=${BUCKET} concurrency=${CONCURRENCY} duration=${DURATION} get_objects=${GET_OBJECTS} sleep_between_rounds=${SLEEP_BETWEEN_ROUNDS}s" || exit 1
printf 'method\tsize\tthroughput\tobj_per_s\treq_avg\treq_p50\treq_p90\treq_p99\tttfb_avg\tttfb_p99\tttfb_worst\n' > "${SUMMARY_TSV}" || exit 1
printf 'method\tsize\tverdict\treason\twarp_exit\tcapture_exit\telapsed_seconds\treport_present\terror_lines\n' > "${ROUNDS_TSV}" || exit 1

if [[ ${TOTAL_ROUNDS} -eq 0 ]]; then
  log "NO_ROUNDS_SELECTED"
  gen_summary_md
  exit 1
fi

if [[ "${1:-}" == "--parse-only" ]]; then
  for method in "${METHODS[@]}"; do
    for size in "${SIZES[@]}"; do
      outfile="${RESULT_DIR}/${method}_${size}.txt"
      PARSE_FAILURE_REASON=missing_output
      warp_rc=unknown; capture_rc=unknown
      if [[ -s "${outfile}" ]]; then
        terminal=$(tail -n 1 "${outfile}")
        if [[ "${terminal}" =~ ^RustFS\ round\ status:\ warp=([0-9]+)\ capture=([0-9]+)$ ]]; then
          warp_rc=${BASH_REMATCH[1]}; capture_rc=${BASH_REMATCH[2]}
        fi
      fi
      if [[ -s "${outfile}" ]] && parse_round "${method}" "${size}" "${outfile}"; then
        PARSED_ROUNDS=$((PARSED_ROUNDS + 1))
        record_round "${method}" "${size}" "${outfile}" PASS clean "${warp_rc}" "${capture_rc}" unknown
      else
        FAILED_ROUNDS=$((FAILED_ROUNDS + 1))
        log "PARSE_FAILED method=${method} size=${size} file=${outfile}"
        record_round "${method}" "${size}" "${outfile}" FAIL "${PARSE_FAILURE_REASON}" "${warp_rc}" "${capture_rc}" unknown
      fi
    done
  done
  gen_summary_md || exit 1
  echo "parsed from ${RESULT_DIR}"
  echo ""
  cat "${SUMMARY_MD}" || exit 1
  [[ ${FAILED_ROUNDS} -eq 0 && ${PARSED_ROUNDS} -eq ${TOTAL_ROUNDS} ]]
  exit $?
fi

for method in "${METHODS[@]}"; do
  for size in "${SIZES[@]}"; do
    ROUND=$((ROUND + 1))
    outfile="${RESULT_DIR}/${method}_${size}.txt"

    log "START round=${ROUND}/${TOTAL_ROUNDS} method=${method} size=${size} concurrency=${CONCURRENCY} duration=${DURATION}"

    extra_args=()
    if [[ "${method}" != "put" ]]; then
      extra_args=(--objects "${GET_OBJECTS}")
    fi

    warp_args=("${method}" \
      --host "${HOST}" \
      --access-key "${ACCESS_KEY}" \
      --secret-key "${SECRET_KEY}" \
      --bucket "${BUCKET}" \
      --concurrent "${CONCURRENCY}" \
      --duration "${DURATION}" \
      --obj.size "${size}" \
      ${extra_args[@]+"${extra_args[@]}"} \
      --no-color)
    start_epoch=$(date +%s)
    if [[ "${ROUND_TIMEOUT}" =~ ^[1-9][0-9]*$ ]]; then
      timeout --signal=TERM --kill-after=30 "${ROUND_TIMEOUT}" \
        bash -c 'warp "$@"' bash "${warp_args[@]}" 2>&1 | tee "${outfile}"
    else
      warp "${warp_args[@]}" 2>&1 | tee "${outfile}"
    fi
    round_status=("${PIPESTATUS[@]}")
    rc=${round_status[0]}
    capture_rc=${round_status[1]}
    end_epoch=$(date +%s)
    printf '\nRustFS round status: warp=%s capture=%s\n' "${rc}" "${capture_rc}" >> "${outfile}" || capture_rc=1
    PARSE_FAILURE_REASON=warp_exit
    [[ ${capture_rc} -eq 0 ]] || PARSE_FAILURE_REASON=output_capture

    if [[ ${rc} -eq 0 && ${capture_rc} -eq 0 ]] && parse_round "${method}" "${size}" "${outfile}"; then
      PARSED_ROUNDS=$((PARSED_ROUNDS + 1))
      CONSECUTIVE_FAILURES=0
      log "END round=${ROUND}/${TOTAL_ROUNDS} method=${method} size=${size} rc=${rc} elapsed=$((end_epoch - start_epoch))s parsed=ok"
      record_round "${method}" "${size}" "${outfile}" PASS clean "${rc}" "${capture_rc}" "$((end_epoch - start_epoch))"
    else
      FAILED_ROUNDS=$((FAILED_ROUNDS + 1))
      CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
      log "END round=${ROUND}/${TOTAL_ROUNDS} method=${method} size=${size} rc=${rc} capture_rc=${capture_rc} elapsed=$((end_epoch - start_epoch))s parsed=failed"
      record_round "${method}" "${size}" "${outfile}" FAIL "${PARSE_FAILURE_REASON}" "${rc}" "${capture_rc}" "$((end_epoch - start_epoch))"
      if [[ "${MAX_CONSECUTIVE_FAILURES}" =~ ^[1-9][0-9]*$ ]] \
        && (( CONSECUTIVE_FAILURES >= MAX_CONSECUTIVE_FAILURES )); then
        log "ABORT consecutive_failures=${CONSECUTIVE_FAILURES} limit=${MAX_CONSECUTIVE_FAILURES}"
        break 2
      fi
    fi

    if [[ ${ROUND} -lt ${TOTAL_ROUNDS} ]]; then
      log "SLEEP ${SLEEP_BETWEEN_ROUNDS}s before next round"
      sleep "${SLEEP_BETWEEN_ROUNDS}"
    fi
  done
done

# An early abort leaves explicit unexecuted rows rather than shortening the
# selected matrix silently. Raw metrics from failed rounds remain diagnostic.
for method in "${METHODS[@]}"; do
  for size in "${SIZES[@]}"; do
    if ! awk -F '\t' -v m="${method}" -v s="${size}" 'NR>1 && $1==m && $2==s {found=1} END {exit !found}' "${ROUNDS_TSV}"; then
      record_round "${method}" "${size}" "" NOT_RUN aborted unknown unknown 0
    fi
  done
done

gen_summary_md || exit 1
log "ALL_ROUNDS_COMPLETE selected=${TOTAL_ROUNDS} parsed=${PARSED_ROUNDS} failed=${FAILED_ROUNDS} summary_tsv=${SUMMARY_TSV} summary_md=${SUMMARY_MD}" || exit 1
echo ""
echo "==== Results summary ===="
cat "${SUMMARY_MD}" || exit 1
[[ ${FAILED_ROUNDS} -eq 0 && ${PARSED_ROUNDS} -eq ${TOTAL_ROUNDS} ]]
