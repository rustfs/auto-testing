# auto-testing
Auto testing scripts

## Portable execution environments

The suite defaults and CI topology remain unchanged. Local fleets can run the
read-only compatibility preflight before any mutation:

```bash
python3 scripts/check_execution_environment.py \
  --command aws --command curl --command jq --command openssl --command ssh \
  --minimum-bash-major 4 \
  --check-aws-list-objects \
  --data-root /data/rustfs --drive-count 4 \
  --minimum-free-gib 40 --require-distinct-devices \
  --require-port 18080
```

The command prints JSON and exits `2` when a requirement is blocked. It does
not create files, install packages, or change test behavior. A local VM that
maps separate data disks to `/data/rustfs1` through `/data/rustfs4` can remove
the root-filesystem disk-check bypass used by ad hoc single-disk deployments.

The tier webhook receiver is configurable with `RUSTFS_RECEIVER_PORT`; choose a
port that does not conflict with the local package server or another service.
All top-level suite entrypoints are directly executable; `bash <script>` also
works when a local checkout or transport drops executable modes.

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
`audit.request-correlation`, `iam.policy-enforcement`, and
`scanner.status-readiness`. The S3, KMS, and IAM
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

The legacy S3 topology suite also checks historical regressions: fresh bucket
creation timestamps (`S3C-103`), preservation of object bytes after rejected
bucket deletion (`S3C-104`), multipart completion Location and ETag agreement
with HEAD/GET (`S3C-301`), and V1/V2 listing and downloads of user keys named
`xl.meta` (`S3C-401`). `S3C-1407` rejects deletion of buckets containing only
historical versions or only a delete marker, verifies historical bytes, and
explicitly removes its versions during cleanup. These cases run through the
existing topology suite; `--validation-case` does not select legacy case IDs.

Run `python3 -m unittest test_rustfs_s3_regressions.py` to exercise the assertions
with mutated responses without a cluster. This validates the test oracles and
cleanup logic, not live RustFS behavior.

## Deep WORM / Object Lock

A bucket-default retention write is not WORM coverage. The campaign
full-feature line `[PASS] WORM/object-lock retention set` only created an
`at-worm-*` lock bucket and set GOVERNANCE for one day. Its objects were
written before that default was in effect, so the check never proved that a
version could not be deleted or overwritten, and it never ran COMPLIANCE or
legal hold.

Deep coverage is `S3C-602` through `S3C-605` in `rustfs-s3-compat-test.sh`:

| Case | Proves |
| --- | --- |
| `S3C-602` DefaultRetention | GOVERNANCE default is configured before the object is written. That version keeps its bytes across a same-key PUT, and version delete is `AccessDenied`. |
| `S3C-603` GovernanceProtection | Per-version GOVERNANCE blocks version delete. Bypass can remove it. A same-key PUT does not replace the locked bytes. A delete without a version id only adds a delete marker. |
| `S3C-604` ComplianceProtection | COMPLIANCE blocks version delete, including `--bypass-governance-retention`. |
| `S3C-605` LegalHold | Legal hold `ON` blocks version delete. `OFF` allows that version to be removed. |

`S3C-602` writes the object only after `put-object-lock-configuration`.
`S3C-603`, `S3C-604`, and `S3C-605` attach protection to the returned version
id before any delete or overwrite probe. `S3C-604` cannot remove its
COMPLIANCE object before the one-day retain-until. Each case uses a fresh
bucket name (`s3c60N-lk-<random>-<pid>`), so a later run does not collide
with that leftover bucket.

Campaign `scripts/run-auto-testing-full.sh` should call the lighter harness
after the endpoint is healthy. That harness reuses the same four functions
and does not deploy a cluster:

```bash
./scripts/campaign/run-worm-object-lock.sh \
  --endpoint "http://127.0.0.1:29000"
```

A topology run that installs RustFS selects the same cases with:

```bash
./rustfs-s3-compat-test.sh --topology single-single -y \
  --package-url <deb url> --suite worm
```

`python3 -m unittest test_rustfs_worm_object_lock.py` checks the oracles,
the write-after-default-retention order, and that both entry points schedule
exactly `S3C-602`..`S3C-605`.

The report's full implemented-case gate is intentionally not enabled by these
runs. Run it with `--gate-status implemented` only after all 66
implemented source cases have verified executors and live results.

S3, scanner, KMS, tiering, and IAM have live PASS evidence against nightly
build `37958901959` attempt `1` on the current auto-testing fleet. Events and
audit have local executor tests but still need live verification.

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

The full Tier suite requires `mosquitto_sub` and `mosquitto_pub`. Before changing any RustFS node, it checks MQTT publish/subscribe delivery through `${RUSTFS_HEAL_IP}:1883`. An existing broker is reused. If the endpoint is local and has no listener, the suite starts an isolated `eclipse-mosquitto:2` Docker container and removes only that container on exit, including failed setup. Docker must be available directly or through `sudo -n`. Set `RUSTFS_TIER_MQTT_MODE=external` to require an already running broker without creating one; use `RUSTFS_MQTT_PORT` to override port 1883. Remote brokers must be provisioned separately. Canonical tiering, events, and audit validation cases do not require MQTT.

MQTT startup, subscriber, broker, and VM connectivity diagnostics are stored under the artifact `logs/` directory. A managed container's `mqtt-container-id` receipt remains in the artifact directory until cleanup succeeds; CI uses the same cleanup function to recover that container after cancellation. A failed module-enable request also captures bounded, credential-filtered notification startup logs from the source and target before teardown. The test servers use the `warn` log level so lifecycle failure details are available. Always pass a unique persistent `--artifacts-dir` when investigating a setup failure; the default temporary directory is removed on exit.

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
contain 60 unique terminal results; FAIL, RUNNING, missing, duplicate, corrupt,
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
RUSTFS_TIER_TEST_MODE=versioned-cleanup ./test_rustfs_tier.sh
python3 -m unittest -v test_rustfs_tier_report.py
```

`TIER-115` (`VersionedTierDeleteCleanup`) runs in both topologies. It transitions
both versions of one key into a fresh unversioned remote bucket, removes the lifecycle rule, and confirms that an
ordinary delete hides the key while historical versions still block even a
forced tier removal. Both versions must remain readable. After deleting every
version and delete marker (checking per-object `DeleteObjects` errors), the case
requires natural remote cleanup and successful ordinary tier removal. The source
bucket and remote objects stay untouched by teardown until these assertions
finish. Cleanup and removal each have a 180-second deadline; only the expected
in-use response is retried after the remote namespace becomes empty.
