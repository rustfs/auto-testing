#!/usr/bin/env bash
set -Eeuo pipefail

CONFIG_PATH="${RUSTFS_POOL_NGINX_CONFIG_PATH:-/etc/nginx/conf.d/rustfs-pool-test.conf}"
LISTEN_ADDRESS="${RUSTFS_POOL_NGINX_LISTEN:-127.0.0.1:19000}"
ACCESS_LOG="${RUSTFS_POOL_NGINX_ACCESS_LOG:-/var/log/nginx/rustfs-pool-test-access.log}"
ERROR_LOG="${RUSTFS_POOL_NGINX_ERROR_LOG:-/var/log/nginx/rustfs-pool-test-error.log}"
SUDO=()
TEMP_FILES=()
MANAGED_MARKER='# Managed by rustfs_pool_nginx_stage.sh'

cleanup_temp_files() {
  if [ "${#TEMP_FILES[@]}" -gt 0 ]; then
    rm -f "${TEMP_FILES[@]}"
  fi
}

trap cleanup_temp_files EXIT

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  printf '%s\n' \
    'Usage:' \
    '  rustfs_pool_nginx_stage.sh render ACTIVE_COUNT ENDPOINT...' \
    '  rustfs_pool_nginx_stage.sh apply ACTIVE_COUNT ENDPOINT...' \
    '  rustfs_pool_nginx_stage.sh cleanup'
}

validate_paths() {
  [[ "${CONFIG_PATH}" =~ ^/etc/nginx/conf\.d/[A-Za-z0-9._-]+\.conf$ ]] \
    || die "unsafe Nginx config path: ${CONFIG_PATH}"
  [[ "${LISTEN_ADDRESS}" =~ ^127\.0\.0\.1:[1-9][0-9]{0,4}$ ]] \
    || die "test listener must bind 127.0.0.1 on an explicit port: ${LISTEN_ADDRESS}"
  [[ "${ACCESS_LOG}" =~ ^/var/log/nginx/[A-Za-z0-9._-]+\.log$ ]] \
    || die "unsafe Nginx access log path: ${ACCESS_LOG}"
  [[ "${ERROR_LOG}" =~ ^/var/log/nginx/[A-Za-z0-9._-]+\.log$ ]] \
    || die "unsafe Nginx error log path: ${ERROR_LOG}"
  local port="${LISTEN_ADDRESS##*:}"
  [ "${port}" -le 65535 ] || die "invalid Nginx listener port: ${port}"
}

endpoint_authority() {
  local endpoint="${1%/}" authority
  [[ "${endpoint}" =~ ^http://[A-Za-z0-9._-]+:[1-9][0-9]{0,4}$ ]] \
    || die "upstream endpoint must be http://host:port: ${endpoint}"
  authority="${endpoint#http://}"
  [ "${authority##*:}" -le 65535 ] || die "invalid upstream port: ${endpoint}"
  printf '%s' "${authority}"
}

render_config() {
  local active_count="$1"
  shift
  [[ "${active_count}" =~ ^[1-9][0-9]*$ ]] || die "active count must be a positive integer"
  [ "$#" -eq "${active_count}" ] \
    || die "active count ${active_count} does not match the $# supplied endpoints"

  local endpoint authority
  local -a authorities=()
  for endpoint in "$@"; do
    if ! authority="$(endpoint_authority "${endpoint}")"; then
      return 1
    fi
    authorities+=("${authority}")
  done

  printf '%s\n' "${MANAGED_MARKER}" 'upstream rustfs_pool_test_backend {'
  for authority in "${authorities[@]}"; do
    printf '    server %s;\n' "${authority}"
  done
  printf '%s\n' '}'
  printf '\n'
  printf '%s%s%s%s\n' \
    "log_format rustfs_pool_test '\$time_iso8601 request_id=\$request_id request=\"\$request\" status=\$status " \
    "upstream_addr=\"\$upstream_addr\" upstream_status=\"\$upstream_status\" " \
    "upstream_connect_time=\"\$upstream_connect_time\" upstream_header_time=\"\$upstream_header_time\" " \
    "upstream_response_time=\"\$upstream_response_time\" request_time=\"\$request_time\"';"
  printf '%s\n' \
    '' \
    'server {' \
    "    listen ${LISTEN_ADDRESS};" \
    '    client_max_body_size 0;' \
    "    access_log ${ACCESS_LOG} rustfs_pool_test;" \
    "    error_log ${ERROR_LOG} info;" \
    '' \
    '    location / {' \
    '        proxy_pass http://rustfs_pool_test_backend;' \
    '        proxy_http_version 1.1;' \
    '        proxy_set_header Host $http_host;' \
    '        proxy_set_header X-Request-ID $request_id;' \
    '        proxy_set_header Connection "";' \
    '        proxy_request_buffering off;' \
    '        proxy_buffering off;' \
    '        proxy_next_upstream off;' \
    '        proxy_connect_timeout 10s;' \
    '        proxy_send_timeout 1h;' \
    '        proxy_read_timeout 1h;' \
    '    }' \
    '}'
}

ensure_privilege() {
  if [ "$(id -u)" -ne 0 ]; then
    command -v sudo >/dev/null 2>&1 || die "sudo is required to manage the test Nginx listener"
    SUDO=(sudo -n)
  fi
}

restore_config() {
  local backup="$1"
  if [ -n "${backup}" ] && [ -f "${backup}" ]; then
    "${SUDO[@]}" install -m 0644 "${backup}" "${CONFIG_PATH}"
  else
    "${SUDO[@]}" rm -f "${CONFIG_PATH}"
  fi
  "${SUDO[@]}" nginx -t >/dev/null
  "${SUDO[@]}" systemctl reload nginx
}

apply_config() {
  local active_count="$1"
  shift
  local tmp backup=""
  ensure_privilege
  tmp="$(mktemp)"
  TEMP_FILES+=("${tmp}")
  render_config "${active_count}" "$@" > "${tmp}"
  if "${SUDO[@]}" test -L "${CONFIG_PATH}"; then
    die "refusing to replace a symlinked Nginx config: ${CONFIG_PATH}"
  fi
  if "${SUDO[@]}" test -e "${CONFIG_PATH}"; then
    "${SUDO[@]}" grep -Fqx "${MANAGED_MARKER}" "${CONFIG_PATH}" \
      || die "refusing to overwrite an unmanaged Nginx config: ${CONFIG_PATH}"
    backup="$(mktemp)"
    TEMP_FILES+=("${backup}")
    "${SUDO[@]}" cat "${CONFIG_PATH}" > "${backup}"
  fi
  "${SUDO[@]}" install -m 0644 "${tmp}" "${CONFIG_PATH}"
  if ! "${SUDO[@]}" nginx -t; then
    restore_config "${backup}"
    die "Nginx rejected the pool test configuration"
  fi
  if ! "${SUDO[@]}" systemctl reload nginx; then
    restore_config "${backup}"
    die "Nginx reload failed; the previous configuration was restored"
  fi
  printf 'Nginx pool test stage applied: active=%s listen=%s upstreams=%s\n' \
    "${active_count}" "${LISTEN_ADDRESS}" "$*"
}

cleanup_config() {
  ensure_privilege
  if "${SUDO[@]}" test -L "${CONFIG_PATH}"; then
    die "refusing to remove a symlinked Nginx config: ${CONFIG_PATH}"
  fi
  if "${SUDO[@]}" test -e "${CONFIG_PATH}"; then
    "${SUDO[@]}" grep -Fqx "${MANAGED_MARKER}" "${CONFIG_PATH}" \
      || die "refusing to remove an unmanaged Nginx config: ${CONFIG_PATH}"
    "${SUDO[@]}" rm -f "${CONFIG_PATH}"
  fi
  "${SUDO[@]}" nginx -t
  "${SUDO[@]}" systemctl reload nginx
  "${SUDO[@]}" rm -f "${ACCESS_LOG}" "${ERROR_LOG}"
  printf 'Nginx pool test listener removed: %s\n' "${CONFIG_PATH}"
}

main() {
  validate_paths
  [ "$#" -gt 0 ] || { usage; exit 1; }
  local command="$1"
  shift
  case "${command}" in
    render)
      [ "$#" -ge 2 ] || die "render requires ACTIVE_COUNT and at least one endpoint"
      render_config "$@"
      ;;
    apply)
      [ "$#" -ge 2 ] || die "apply requires ACTIVE_COUNT and at least one endpoint"
      apply_config "$@"
      ;;
    cleanup)
      [ "$#" -eq 0 ] || die "cleanup takes no arguments"
      cleanup_config
      ;;
    -h|--help)
      usage
      ;;
    *) die "unknown command: ${command}" ;;
  esac
}

main "$@"
