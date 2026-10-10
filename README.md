# auto-testing
Auto testing scripts

## Release validation migration

`validation-cases.json` snapshots all 158 canonical cases from
`rustfs-release-validation/scenarios/catalog.yaml` (66 implemented, 84
procedure-ready, 8 planned). The source status is historical context. A run
result carries `migration_status: verified` only after its assertions, topology,
and live evidence are checked in this repository. `reuse_candidates` names
existing suites to inspect and never grants coverage by itself.

```bash
python3 scripts/validation_catalog.py check
python3 scripts/validation_catalog.py summary
python3 -B -m unittest -v test_validation_migration.py
```

To refresh after an intentional source catalog change:

```bash
python3 scripts/validation_catalog.py sync \
  --source /absolute/path/to/rustfs-release-validation/scenarios/catalog.yaml
```

`scripts/validation_report.py` accepts one JSON result per canonical case ID.
Results must separately report source status, migration status, verdict,
cleanup, candidate/rc provenance, and evidence for each source assertion.
Pass `--gate-status implemented` only after the 66 migrated executors are
ready; missing or unverified cases fail that gate. Without a gate status, the
report shows migration progress and still rejects malformed or false PASS
results.

The executable canonical cases currently include `s3.basic-object-round-trip`,
`scanner.usage-convergence`, `kms.sse-kms-round-trip`,
`tiering.transition-and-restore`, `events.delivery`,
`audit.request-correlation`, and `iam.policy-enforcement`. The S3, KMS, and IAM
cases use single-disk
deployment. The scanner case uses the storage suite's single-node four-disk
deployment, waits for a post-upload successful scanner cycle, and requires a
complete usage snapshot with exactly 20 objects and 20,690 logical bytes.
Each requires a candidate manifest with `source_sha`, `package_url`, and
`package_sha256`. The installed binary
SHA-256 is measured on the test node and included in result provenance:

```bash
./rustfs-s3-compat-test.sh \
  --validation-case s3.basic-object-round-trip \
  --candidate-manifest /absolute/path/candidate.json \
  --testing-commit "$(git rev-parse HEAD)" \
  --validation-artifacts-dir /absolute/path/new-artifacts-dir \
  --yes

./rustfs-storage-test.sh \
  --validation-case scanner.usage-convergence \
  --candidate-manifest /absolute/path/candidate.json \
  --testing-commit "$(git rev-parse HEAD)" \
  --validation-artifacts-dir /absolute/path/another-new-artifacts-dir \
  --yes

./rustfs-kms-test.sh \
  --validation-case kms.sse-kms-round-trip \
  --candidate-manifest /absolute/path/candidate.json \
  --testing-commit "$(git rev-parse HEAD)" \
  --validation-artifacts-dir /absolute/path/kms-new-artifacts-dir \
  --yes

python3 scripts/validation_report.py \
  --results-dir /absolute/path/new-artifacts-dir/cases \
  --output /absolute/path/new-artifacts-dir/report.md
```

The report's full implemented-case gate is intentionally not enabled by these
runs. Run it with `--gate-status implemented` only after all 66
implemented source cases have verified executors and live results.

S3, scanner, KMS, and tiering have live PASS evidence against nightly build
`37958901959` attempt `1` on the current auto-testing fleet. Events and audit
have local executor tests but still need live verification. IAM also awaits a
live run.

## Fault-tolerance suite

`rustfs-fault-tolerance-test.sh` writes a machine-readable completion result
with `--results-file`. A complete `--all` run contains 38 unique probe results;
AWS CLI/parser failures, duplicate probes, missing probes, and incomplete runs
are harness failures rather than RustFS product verdicts.

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
