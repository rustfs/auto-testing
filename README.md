# auto-testing
Auto testing scripts

## Fault-tolerance suite

`rustfs-fault-tolerance-test.sh` writes a machine-readable completion result
with `--results-file`. A complete `--all` run contains 38 unique probe results;
AWS CLI/parser failures, duplicate probes, missing probes, and incomplete runs
are harness failures rather than RustFS product verdicts.

Recovery phases poll the probe endpoint's `/health/ready` until
`details.storage.writeQuorum` is true before probing, so they assert the EC
contract rather than the peer drive re-admission window. The wait is bounded by
`FT_RECOVERY_TIMEOUT_SECS` (default 120); the elapsed seconds (or `timeout`)
land in `evidence/recovery-<phase>.secs`. The matrix sweep (`--matrix`,
`--matrix-topo`) runs every topology as a single pool and applies each EC level
through `RUSTFS_STORAGE_CLASS_STANDARD`.

```bash
bash -n rustfs-fault-tolerance-test.sh
python3 -B -m unittest -v test_rustfs_fault_tolerance.py
```

## Security suite diagnostics

`rustfs-security-test.sh` includes STS-105 request timing, the full revoke
response headers/body, and the subsequent S3 result in its console output and
each topology's Markdown report. The existing security workflow uploads both
outputs. On failure, the suite collects the RustFS version, recent systemd
journal entries, and the last 64 KiB of up to three log files from each active
node before cleaning the environment. An unavailable SSH connection is recorded
without replacing the test failure.

The default log filter is `error,rustfs::admin::auth=debug`; set
`RUSTFS_OBS_LOGGER_LEVEL` to override it. The report records the test repository
commit, while console output includes Bash, curl, and AWS CLI versions.

```bash
python3 -B -m unittest -v test_rustfs_security.py test_sensitive_diagnostics.py
```

## Tier suite artifacts

`rustfs-tier-test.sh` can persist machine-readable evidence with
`--artifacts-dir <unique-directory>`. The directory contains one atomic JSON
result per `(topology, case_id)`, the exact `rc` path/version/SHA-256 and
isolated config directory in `provenance.json`, failed-case logs, and a
`residue.jsonl` cleanup manifest. Failed results include a bounded,
credential-safe `failure_kind`/`failure_detail`, a relative `log_path`, and a
separate `cleanup_detail` so the primary defect is not overwritten by cleanup.

When copying the tier runner outside this checkout, keep
`rustfs-tier-log-capture.py` beside `rustfs-tier-test.sh`. `--log-file` fails
closed if that secure log helper is missing.

Use `rustfs_tier_report.py` as the final gate. A complete two-topology run must
contain 56 unique terminal results; FAIL, RUNNING, missing, duplicate, corrupt,
or non-allowlisted UNSUPPORTED results make the command exit non-zero while it
still writes the Markdown report.

```bash
./rustfs-tier-test.sh --all-topologies -y \
  --package-url https://example.invalid/rustfs.deb \
  --rc-bin /absolute/path/to/rc \
  --expected-rc-sha256 <sha256> \
  --artifacts-dir /tmp/rustfs-tier-artifacts-unique

python3 rustfs_tier_report.py \
  --results-dir /tmp/rustfs-tier-artifacts-unique/cases \
  --provenance /tmp/rustfs-tier-artifacts-unique/provenance.json \
  --output /tmp/rustfs-tier-report.md
```

Local contract tests:

```bash
./test_rustfs_tier.sh
python3 -m unittest -v test_rustfs_tier_report.py
```
