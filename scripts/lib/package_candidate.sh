# Verify the running systemd service against an explicitly identified Debian package.
# Caller provides run_remote for access to the owned fleet.
verify_running_package_candidate() {
  local node="$1" package="$2" expected_sha="$3" receipt binary_sha pid
  [[ "${expected_sha}" =~ ^[0-9A-Fa-f]{64}$ ]] || return 1
  receipt="$({
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -eq 0 ] || SUDO="sudo -n"\n'
    printf 'pkg="%s"\n' "${package}"
    printf 'printf "%%s  %%s\\n" "%s" "$pkg" | sha256sum --check --status\n' "${expected_sha,,}"
    cat <<'REMOTE'
candidate="$(dpkg-deb --fsys-tarfile "$pkg" | tar -xOf - ./usr/bin/rustfs | sha256sum)"
pid="$(systemctl show --property=MainPID --value rustfs)"
[[ "$pid" =~ ^[1-9][0-9]*$ ]]
running="$(${SUDO} sha256sum "/proc/${pid}/exe")"
[[ "${candidate%% *}" == "${running%% *}" ]]
printf '%s\t%s\n' "${running%% *}" "${pid}"
REMOTE
  } | run_remote "${node}")" || return 1
  IFS=$'\t' read -r binary_sha pid <<< "${receipt}"
  [[ "${binary_sha}" =~ ^[0-9a-f]{64}$ && "${pid}" =~ ^[1-9][0-9]*$ ]] || return 1
  printf '%s\t%s\t%s\t%s\n' "${node}" "${expected_sha,,}" "${binary_sha}" "${pid}"
}
