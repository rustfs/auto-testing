# Retest after test-suite and environment fixes

This change is the test-side attribution for [backlog #3173](https://github.com/rustfs/backlog/issues/3173), based on the merged auto-testing #144. It does not mean the server 503, the namespace lock timeout, or the performance anomaly has been fixed.

| Original failure | Fix behavior | Retest acceptance |
|---|---|---|
| TBL-101 import failed | Prepare the whole `scripts/table-catalog` from the RustFS checkout that matches the candidate. Record the source SHA and file hashes. Check the local modules first, then create the venv | End-to-end reads and writes actually succeed. A missing module must be reported as a tool-preparation error |
| M-1x2EC1 started a non-participating node | Pass the participating node count explicitly. Submit a non-blocking start only for those nodes and wait until they are active | Keep every topology, EC, and fault probe. An unexecuted probe cannot stand in for a pass |
| Rebalance activity criterion timed out | Increase the Rebalance activity fixture on its own and check capacity first. Keep the `Started` assertion and the non-zero progress assertion. An early completion explicitly reports that the activity window was insufficient | stop/resume, live I/O, and operation guards must run inside the activity window. The completed state still requires non-zero migration progress and a data check |
| Pool-expansion load filled a single disk | Sample capacity every 2 seconds. Protect single-disk utilization and remaining bytes. Reserve in-flight objects and the observed growth, and shorten the stop wait | Pass only when the target is reached. Stopping early because of capacity protection is an environment-capacity failure, and MPU cleanup still runs |
| A performance round contained service errors but was counted in the summary | Error logs fail that round. The raw metrics are retained for diagnosis. Each round records whether the Report appeared, the exit code, and the error-line count | `rounds.tsv` and the Markdown include every selected round. Timeouts and rounds that did not run do not count as a pass |
| AppleDouble input and a gap in the syntax report | Exclude `._*`. Run `bash -n` independently on every real script and save the per-file result | If any real script fails, the overall result is non-zero. An empty selection does not pass either |

## Preparing Table source

```bash
python3 scripts/prepare_table_smoke.py prepare \
  --source-root /path/to/rustfs-checkout \
  --destination /path/to/new-run/table-catalog \
  --expected-sha FULL_CANDIDATE_SOURCE_SHA
```

`--destination` must be a new directory, so old-version modules are not mixed in. A chained workflow uses the source SHA from the manifest. A standalone workflow can pass `source_ref` to name the source that matches the artifact. When it is omitted, the release tag or that checkout's main is used, and `source-manifest.json` retains the actual SHA.

The Table workflow uploads `table-catalog/source-manifest.json` together with the report, the case table, and the logs. It retains the actual smoke source SHA and file hashes so they can still be traced after the runner's temporary directory is cleaned up.

## Capacity configuration

The Rebalance source case explicitly requires non-zero migration progress during the activity phase. The previous run, at 32 MiB per object, finished before the first cycle saved progress, so it cannot pass. The dedicated activity fixture now defaults to 128 MiB per object (128 objects by default, of which 3 boundary objects are smaller). It is configured with `RUSTFS_ACTIVE_REBALANCE_FIXTURE_BYTES` / `--active-rebalance-fixture-bytes` and recorded in the run manifest. Before writing, capacity is checked against the upper bound of EC usage and at least 1 GiB free on each disk. Other Decommission activity fixtures keep their original size. A faster machine may still need a larger fixture. When the window is insufficient, the case fails rather than relaxing the assertion.

Capacity sampling is separate from the Rebalance / Decommission `--poll-interval`. The default sample interval is 2 seconds, and a single direct API request is capped at 3 seconds. There is no long retry or proxy probe during the activity write. The proxy is still probed before and after the write, and errors are still retained.

The reserve is the minimum free bytes per disk (512 MiB by default) plus `object size × concurrency`, plus the observed cluster growth rate multiplied by the sampling, request, and stop windows. Cluster growth is a conservative upper bound on single-disk growth. Uniform data placement is not assumed. An empty, invalid, or changed capacity inventory stops further writes.

`RUSTFS_CAPACITY_POLL_INTERVAL`, `RUSTFS_CAPACITY_SAMPLE_TIMEOUT`, `RUSTFS_CAPACITY_MIN_FREE_BYTES`, `RUSTFS_CAPACITY_MAX_DISK_PERCENT`, `RUSTFS_WARP_STOP_GRACE`, and `RUSTFS_WARP_OBJ_SIZE` are configurable. A small-capacity environment should first lower concurrency or the target utilization according to free disk space. The sidecar `.capacity.jsonl` retains the inputs, the margin, and the stop reason for every decision. This is a load protection. It cannot guarantee that capacity is never exhausted under an unpredictable write burst.

An MPU list error retains the HTTP status, `x-rustfs-error-cause`, the request ID, and retry-after when the server returns it. The full AWS debug output and response cookies are not collected.

## Result boundaries

The overall performance-round timeout still covers preparation, load, and teardown. `timeout_before_report` / `timeout_after_report` only say whether a Warp Report appeared. They cannot infer which internal Warp stage was stuck. Do not use a disk-full event during pool expansion to explain an earlier performance timeout.

`--parse-only` writes separate `rounds.reparsed.tsv`, `summary.reparsed.tsv`, `summary.reparsed.md`, and `master.reparsed.log` files, and keeps the original execution ledger, summary, and logs. Reparsing cannot recover elapsed time or the original run configuration. The `NOT_RUN / aborted` rows and the elapsed time in the original `rounds.tsv` are the execution evidence. The performance workflow uploads both the original execution ledger and the reparsed artifacts. When step 6 has no original summary, it shows the reparsed summary. A reparse failure still returns non-zero.

A retest must retain the product source SHA, the CI artifact identity, the binary SHA-256, the test commit SHA, the actual configuration, and the complete per-item results. VM writes and the final recovery use the same shared lock. After recovery, check the four nodes, the 24 data mounts, and the object verification.
