# RustFS Pool Expansion / Decommission Test

End-to-end test for **storage pool expansion**, **data rebalancing** and
**pool decommission**, driven by
[`rustfs_pool_expand.sh`](rustfs_pool_expand.sh).

All pool / rebalance / decommission checks talk to a currently active RustFS
node through the **direct admin endpoint**. Requests are SigV4-signed and the
JSON responses are asserted with `jq`, so the results are exact and independent
of client CLI output formatting. A shared load balancer can be supplied as an
independent proxy probe, but it is never used as a silent failover path.
**`rc` is not required.**

## What it does

1. Downloads the RustFS `.deb` package on all nodes (a release tag, or a
   direct URL such as a nightly/R2 package).
2. Installs it (`dpkg -i`).
3. Starts the first pool and verifies it via
   `GET /rustfs/admin/v3/pools/list`.
4. Writes data with `warp` and monitors storage usage
   (`GET /rustfs/admin/v3/storageinfo`) until the threshold.
5. Expands to a second pool (nodes started in parallel).
6. Starts rebalance (`POST /rustfs/admin/v3/rebalance/start`) and waits for
   **all** pools to report `Completed`.
7. (3-pool mode) Expands to a third pool, rebalances again.
8. (optional) Decommissions pool 0
   (`POST /rustfs/admin/v3/pools/decommission`), with automatic clear + retry
   on failure, and waits for `decommissionInfo.complete == true`.

When an assertion fails, the script prints a per-pool summary **and the full
JSON response** (pool state, progress, failure counters, `waitingReason`,
`unresolvedEntries`, last rebalance error), so the GitHub Actions log shows
exactly where the test stopped. Credentials never appear in the logs.

## Self-hosted runner prerequisites

- Register the admin host (e.g. `heal`) as a runner with the
  `smoke-testing` label.
- Install `jq`, `openssl`, `curl` and `warp` (only needed for `--with-warp`)
  on the runner. `rc` is **not** required.
- The runner user must be able to SSH to all nodes without a password prompt
  (`~/.ssh/config` with keys).
- The nodes need passwordless `sudo` for the SSH user, resolvable
  `rustfs-node*` hostnames in `/etc/hosts`, and writable data directories.
- The admin API credentials must have the `admin:server-info`,
  `admin:decommission` and `admin:rebalance` actions.

## Configuration

Set these in the repository (secrets/variables):

| Kind   | Name                       | Purpose                                        |
| ------ | -------------------------- | ---------------------------------------------- |
| Secret | `RUSTFS_ACCESS_KEY`        | RustFS access key                              |
| Secret | `RUSTFS_SECRET_KEY`        | RustFS secret key                              |
| Var    | `RUSTFS_ADMIN_ENDPOINT`    | Direct endpoint of the node kept active for control operations |
| Var    | `RUSTFS_WARP_ENDPOINT`     | Data endpoint used for the pool-fill workload |
| Var    | `RUSTFS_PROXY_ENDPOINT`    | Optional Nginx/LB endpoint used only for comparison probes |
| Var    | `RUSTFS_NODE_ENDPOINTS`    | Space-separated direct endpoints in `RUSTFS_NODES` order |
| Var    | `RUSTFS_API_ENDPOINT`      | Legacy fallback for both admin and warp endpoints (`RUSTFS_RC_ENDPOINT` is also accepted) |
| Var    | `RUSTFS_NODES`             | Space-separated node names, e.g. `vm000 vm001 vm002` |
| Var    | `RUSTFS_SSH_USER`          | SSH user for the nodes, e.g. `azureuser`       |

The pool workflow keeps `http://rustfs-node1:9000` as its direct control anchor
across the current expansion and decommission flow. Warp runs through the
dedicated stage-aware proxy so the test exercises Nginx under the same C32
multipart load while storageinfo remains independently observable over the
direct path. The test waits for every intended node's direct `/health/ready`
response and verifies that future-stage nodes are not ready before issuing
stage traffic.

When `RUSTFS_PROXY_ENDPOINT` is set, every proxy storageinfo failure is sticky.
The script may retry `000`, `502`, `503`, and `504` responses briefly to keep
collecting evidence, but a recovered request still makes the final result fail.
Every direct and proxy probe records its endpoint, status, response body, and
connect/TTFB/total timings. Mutating admin calls and Warp PUTs are never retried
across endpoints.

The GitHub workflow uses `rustfs_pool_nginx_stage.sh` as a stage hook and a
dedicated loopback listener. After the direct readiness barrier, the hook
atomically renders and validates an upstream containing only the active nodes,
reloads Nginx, and then allows the proxy probe to run. The listener disables
hidden upstream retries and records `upstream_addr`, `upstream_status`, and
connect/header/response timing fields. Use `--proxy-stage-hook FILE` to enable
the same behavior in a manual run.

Warp logs may contain bounded RustFS `503 SlowDown` responses; their capacity
impact is reported separately. Nginx `502`/`504`, connection
closure/reset/refusal, response-header/I/O timeouts, unexpected EOF, and
broken-pipe errors fail the pool test even if the storage threshold was reached.

## Workflow inputs

| Input               | Default       | Meaning                                  |
| ------------------- | ------------- | ---------------------------------------- |
| `rustfs_version`    | `1.0.0-rc.3`  | GitHub release tag to test               |
| `package_url`       | *(empty)*     | Direct `.deb` URL (e.g. nightly/R2); overrides `rustfs_version` |
| `pools`             | `3`           | Expand to 2 or 3 pools                   |
| `storage_threshold` | `50`          | Stop warp writes at N% usage             |
| `warp_duration`     | `10m`         | warp write duration                      |
| `warp_concurrent`   | `32`          | concurrent warp operations               |
| `run_decommission`  | `true`        | Run decommission (3-pool mode only)      |
| `cleanup_before`    | `true`        | Reset nodes before the test              |
| `cleanup_after`     | `true`        | Reset nodes after the test               |

> ⚠️ `cleanup_before` / `cleanup_after` run the script's `--reset` mode, which
> **stops the services and deletes the data directories and config** on all
> nodes. Only use this workflow against a dedicated test environment.

## Manual usage

```bash
# Full workflow with a release tag
./rustfs_pool_expand.sh --all --with-warp -y \
  --version 1.0.0-rc.3 \
  --admin-endpoint http://rustfs-node1:9000 \
  --warp-endpoint http://rustfs-node1:9000

# Use a direct .deb URL (e.g. nightly package on R2)
./rustfs_pool_expand.sh --all --with-warp -y \
  --package-url https://dl.rustfs.com/artifacts/rustfs/packages/nightly/rustfs-nightly-latest.deb \
  --admin-endpoint http://rustfs-node1:9000 \
  --warp-endpoint http://rustfs-node1:9000

# Preflight / reset / single step
./rustfs_pool_expand.sh --preflight --version 1.0.0-rc.3
./rustfs_pool_expand.sh --reset -y
./rustfs_pool_expand.sh --step 9 --finalize-decommission -y
```

## Known issues and caveats

- RustFS `1.0.0-rc.3` fails decommission with a large `warp`-written bucket
  ("metacache listing quorum failed / timeout"). If decommission repeatedly
  fails, reduce the written data (lower `storage_threshold`) or remove the test
  bucket, then re-run step 9. The script detects the failure, clears metadata
  and retries `DECOMMISSION_RETRIES` times before giving up.
- Multi-pool nodes must start **simultaneously** (the script does this) or the
  first node dies with `not first disk`.
- The direct admin endpoint must identify a node that is active at every point
  where the script issues an admin request. Do not point it at a static load
  balancer containing nodes that are intentionally offline for a future pool
  stage.
- A proxy used for clean acceptance must expose only the nodes active in the
  current stage: node 1 for one pool, nodes 1-2 for two pools, and nodes 1-3 for
  three pools. Application-level S3 `503 SlowDown` responses must be forwarded
  unchanged instead of being retried as upstream health failures.
- Warp finishing before `storage_threshold` is an error. This prevents an
  expansion run with insufficient test data from being reported as successful.
- The admin API is SigV4-signed (`host`, `x-amz-content-sha256:
  UNSIGNED-PAYLOAD`, `x-amz-date`), matching the signer the RustFS server
  itself trusts. If the cluster requires a non-default region, set
  `SIGV4_REGION` in the script.

## Local script checks

```bash
bash -n rustfs_pool_expand.sh test_rustfs_pool_expand.sh
./test_rustfs_pool_expand.sh
./rustfs_pool_nginx_stage.sh render 2 \
  http://rustfs-node1:9000 http://rustfs-node2:9000
```
