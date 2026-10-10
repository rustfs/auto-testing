#!/usr/bin/env bash
set -Eeuo pipefail

if [ "$#" -ne 2 ]; then
  printf 'usage: %s NODE1 NODE2\n' "$0" >&2
  exit 2
fi

ssh_user="${RUSTFS_SSH_USER:-azureuser}"
ssh_port="${RUSTFS_SSH_PORT:-22}"
ssh_opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${ssh_port}")
pids=()
for node in "$@"; do
  ssh "${ssh_opts[@]}" "${ssh_user}@${node}" \
    'if [ "$(id -u)" -ne 0 ]; then sudo -n systemctl restart rustfs; else systemctl restart rustfs; fi' &
  pids+=("$!")
done
for pid in "${pids[@]}"; do wait "${pid}"; done

for endpoint in http://rustfs-node1:9000 http://rustfs-node2:9000; do
  ready=0
  for ((attempt=0; attempt<24; attempt++)); do
    if curl --noproxy '*' -fsS --max-time 3 "${endpoint}/minio/health/live" >/dev/null; then
      ready=1
      break
    fi
    sleep 2
  done
  [ "${ready}" -eq 1 ] || { printf 'node not ready after restart: %s\n' "${endpoint}" >&2; exit 1; }
done
