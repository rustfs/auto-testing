# Scanner usage regression suite

This suite registers the twenty USAGE acceptance contracts from backlog issue #3178 and implements public probes plus a strict adapter for candidate-native tests. Registration alone is not test implementation or acceptance: each native assertion needs an executable, reviewed candidate binding before the complete case is available.
It runs against **explicitly configured endpoints**. It never installs RustFS,
changes scanner settings, restarts a node, acquires a VM deployment lock, or
deletes system metadata. It creates random `usage-<run-id>-*` buckets and deletes
only buckets that this invocation successfully created. `owned-buckets.json`
retains the exact names and create intents if cleanup fails. A separate
`acknowledged-requests.jsonl` appends each acknowledged PUT and DELETE without
rewriting previous requests. An unacknowledged create remains in `unconfirmed_creates` and forces
`cleanup_status=FAIL`, even when the create may have committed remotely.
Ownership is unconfirmed, so the runner does not delete that bucket
automatically; the environment owner must inspect
the retained request and deployment before any scoped manual recovery.

The existing `scanner.usage-convergence` 20-object smoke test is unchanged.
The new cases are a `local_modules` extension in `validation-cases.json`; the
158-case upstream source snapshot and its hash retain their original meaning.
They are available through explicit case/module selection and the canonical
runner, but do not enter a scheduled fleet rotation without endpoint admission.

## Run the public API cases

Requirements: Python 3.10+, AWS CLI, `rc` for the shared report's client
provenance, and credentials allowed to create/delete test buckets and read the
Admin data-usage API. Credentials are read from the environment and are not
written into reports. TLS verification is enabled and requests do not use an
ambient proxy. No endpoint is inferred from an old lab inventory.

The candidate manifest must contain `package_url`, `package_sha256`,
`source_sha`, and `binary_sha256`. The operator must verify that this manifest
describes the running deployment before execution. This endpoint-only runner
does not attest a remote executable's identity; that limitation is recorded in
the result provenance. Preserve the VM deployment's binary hashes, topology,
features, configuration and base/candidate identities beside this report.

```bash
export RUSTFS_ACCESS_KEY='your-test-access-key'
export RUSTFS_SECRET_KEY='your-test-secret-key'
bash rustfs-scanner-usage-test.sh \
  --endpoint http://test-node:9000 \
  --admin-endpoint http://test-node:9000 \
  --candidate-manifest /path/to/deployed-candidate.json \
  --artifacts-dir /path/to/new-usage-results \
  --cases blackbox
```

`blackbox` runs USAGE-01 and USAGE-20. These have complete public API assertions.
Object, prefix, historical version and marker accounting uses acknowledged
requests plus an independent, explicitly paginated S3 listing; payload readback
uses SHA-256. The version case counts keys with retained historical versions
even when their latest version is a delete marker. Marker-only keys are not
covered by this initial fixture. Missing counters remain unknown, and neither
pending nor observed values can pass an exact-convergence assertion.

Use `--cases USAGE-03,USAGE-05,USAGE-07,USAGE-08,USAGE-09,USAGE-11,USAGE-18`
to execute additional public probes. Supply multiple `--endpoint` and
`--admin-endpoint` options for USAGE-07. These cases also require internal
assertions; successful public probes alone leave their overall verdict
`BLOCKED`, with the successful assertions and the missing evidence listed
separately. Endpoint count is never treated as physical set/source count.

The C1 metadata adapter recognizes the existing
`GET /rustfs/admin/v3/datausageinfo` endpoint and these optional bucket fields:

- `usage_value_state`: `certified`, `carried`, or `observed`.
- `usage_certification`: null or `{epoch, cycle, certified_at}`;
  `certified_at` uses `{secs_since_epoch, nanos_since_epoch}`.
- Top-level pending, partial, observational and admin-projection flags veto a
  false convergence claim. Unknown future state values remain unknown.

Candidates without these optional fields can still run the legacy-compatible
static/version accounting probes. Metadata-specific probes return
`UNSUPPORTED`; a missing carried/observed sampling window is `BLOCKED`.
The adapter never derives a certificate from `last_update` or collection time.

Default request timeout is 30 seconds, convergence timeout 600 seconds, and
poll interval 5 seconds. `--objects` defaults to 32 and is bounded at 100000.
These are test deadlines and fixture controls, not RustFS runtime changes.
Timeout, malformed JSON, missing evidence and cleanup failure cannot pass.
An S3 service error is retained with its error code; Admin evidence includes
the observed HTTP status and request ID. AWS CLI stderr does not always expose
an HTTP status or request ID, so a `ServiceUnavailable` code is not relabelled
as a directly observed HTTP 503.

USAGE-17 is disabled unless `--load-seconds` is at least 3600. Its public probe
performs bounded sequential PUT/GET/DELETE operations and retains each command's
duration and error. That duration includes AWS CLI startup. It is **not** a
saturation benchmark or a matched A/B throughput claim; its comparison/native
assertion remains blocked without the corresponding evidence.

## Native capability adapter

Public APIs cannot prove a scanned frontier, a budget-stop reason, an epoch
fence, source membership, an ACK race, producer-watch coverage or quota/cache
projection isolation. Those assertions call a candidate-native runner.
No native manifest means `BLOCKED`; a missing case/test capability is
`UNSUPPORTED`. An exact test that passes without the required assertion
milestones is `INFRA_ERROR`, not PASS. A failed test is FAIL.

Provide `--native-manifest /path/to/native-manifest.json`, or set
`RUSTFS_USAGE_NATIVE_MANIFEST`. To run native tests without endpoint requests,
use `--execution native`. The resulting profile is `native-test`, not a VM run.
This adapter does not compile RustFS or modify its checkout. Obtain reviewed
test binaries from the candidate build, retaining their build provenance.

```json
{
  "schema_version": 1,
  "source_commit": "the_same_40_hex_commit_as_the_candidate_manifest",
  "cases": {
    "USAGE-13": {
      "tests": [
        {
          "binary": "/absolute/path/to/candidate-scanner-test-binary",
          "binary_sha256": "the_actual_64_hex_binary_sha256",
          "test": "scanner::tests::exact_reviewed_regression_name",
          "assertions": ["USAGE-13-A01"]
        }
      ]
    }
  }
}
```

The manifest is an explicit integration contract, **not a bundled claim that
these hooks already exist**. The suite ships no fabricated native pass fixture.
The adapter verifies the binary hash, matching candidate source identity fields, exact libtest
inventory, exactly one executed and non-ignored test, every declared assertion,
and fixture cleanup. Multiple exact tests may contribute to one case, but all
required assertion IDs must be covered. The case shares one native timeout;
inventory and individual tests cannot reset it.
Each assertion ID has exactly one complete test owner; duplicate claims across
tests are rejected instead of treating one subcondition as complete coverage.
Each exact test belongs to one case. A milestone for a different case rejects
the execution; separate case wrappers are required for shared fixture logic.
If only some native assertions have reviewed hooks, their real PASS evidence
is retained and the uncovered assertions remain UNSUPPORTED; the case cannot pass.

### Preparing and using the manifest

1. Build the reviewed native tests from the candidate's committed checkout. Retain the source SHA, clean checkout status, lockfile digest, toolchain, features and build command beside the executables. A dirty working tree is development evidence and must not be labelled as a build of its unchanged HEAD.
2. Obtain executable paths from the build's structured output, enumerate each binding with `BINARY EXACT_TEST --exact --list`, and calculate its SHA-256. Review each test's assertions against `contracts.py`; similar names or related unit tests do not establish complete coverage.
3. Write the manifest using those exact paths, hashes, source SHA and reviewed assertion IDs. Include only available bindings. The adapter compares provenance fields but cannot reconstruct or attest the compilation, so the build receipt remains part of the acceptance evidence.
4. Run `--execution native --cases all` to produce the full twenty-row capability report. Keep partial PASS assertions, BLOCKED/UNSUPPORTED rows and cleanup failures in the report. This profile proves candidate-local regressions only; it does not establish S3 endpoint or distributed VM acceptance.
5. After the environment owner verifies deployment identity and admits the shared environment, run the relevant public and combined cases with that same candidate identity. Preserve the native report and VM report as separate results. Rebuild and regenerate hashes whenever the candidate source or test binaries change.

Native-only execution still requires the candidate manifest and `rc` for the shared report's client provenance. It does not require endpoints, AWS credentials or a deployment. Without a native manifest, all twenty native-profile rows remain BLOCKED and no public fixture is created. A partial manifest never supplies missing capabilities by implication.

After the real assertions and durable fences have been checked, the native
test emits one line for each claimed assertion and one cleanup line:

```text
usage_assertion: {"case_id":"USAGE-13","assertion_id":"USAGE-13-A01","status":"PASS","evidence":{"observed_budget_reason":"objects","measured_drain_ms":30}}
usage_cleanup: {"case_id":"USAGE-13","status":"PASS"}
```

Evidence values must come from that test execution. A test name or a process
exit code does not demonstrate the broader assertion by itself. Keep native
test scopes aligned with the exact assertion texts in `contracts.py`.
If a test proves only one subcondition, do not use it to mark a broader row
complete. Missing producer, mutation or remote hooks remain visible gaps.
USAGE-13 separates the count-stop drain, remote parent limits, drain completion,
lease/epoch loss, root CAS failure, companion failure, sealed-candidate
immutability and durable-root ACK into eight assertion IDs. The example above
covers only A01; A02 through A08 still need their own reviewed tests and markers.

## Twenty-row acceptance mapping

The public-probe column identifies executable probes in this repository. The additional-evidence column states the remaining contract, not a claim that a native test already implements it. USAGE-01 and USAGE-20 have complete public probes; every other case requires candidate-native implementation and bindings for all of its native assertions. A missing binding is an implementation/integration gap even when the report correctly records BLOCKED or UNSUPPORTED.

| Design case | Public probe | Additional native or controlled-fleet evidence |
| --- | --- | --- |
| USAGE-01 | Create/write/delete/empty and absent values | None for the stated public contract |
| USAGE-02 | None | Three-cycle long sweep plus unrelated remote writes and watch proof |
| USAGE-03 | Insert/overwrite/delete after a measured baseline | Already-scanned mutation boundary, mixed non-certification and ACK |
| USAGE-04 | Nested/prefix-shaped keys and final exactness | Continuous hot prefix, cold durable frontier and no prefix ACK |
| USAGE-05 | Hot writes plus three cold buckets | Actual shared budget, quantum cancellation and durable fairness |
| USAGE-06 | None | 100000-entry wide/deep directories and bounded checkpoints |
| USAGE-07 | Writes/readback/admin reads through multiple endpoints | Actual pools/sets beyond concurrency limit and all-source coverage |
| USAGE-08 | Unique absent bucket and eventual exactness | Cold cluster, no LKG, legacy primary/quota isolation |
| USAGE-09 | Carried/observed metadata sampling | Multi-round timestamps, source mixing and certificate provenance |
| USAGE-10 | None | Crash before/after EOF CAS and successor fencing |
| USAGE-11 | Same-name owned bucket recreation and exact successor | Old-incarnation carry/overlay/ACK rejection |
| USAGE-12 | None | Watch gaps, install race, expiry, restart, overflow and ABA |
| USAGE-13 | None | Lease/CAS/companion failure and budget/terminal/drain races |
| USAGE-14 | None | Lost/reordered/duplicate ACK and post-walk mutation |
| USAGE-15 | None | Movement, topology and tier-generation guardrails |
| USAGE-16 | None | Old readers/peers, independent capabilities and rollback |
| USAGE-17 | Explicit 60-minute sequential workload and exact convergence | Same-condition base/candidate metrics and workload comparison |
| USAGE-18 | Per-bucket metadata with separate collection timestamps | Metrics failures and admin/quota/cache/writer separation |
| USAGE-19 | None | Quota v1/v2 and legacy fallback decisions |
| USAGE-20 | Object/prefix collision, versions, markers and payloads | Marker-only keys remain an explicit follow-up fixture |

USAGE-02 remains open until C5 and VM acceptance. Default-disabled quantum is
not completion. Independent prefix certification/replace/ACK is deferred.
No test in this suite promises ten-minute convergence for arbitrary data size.

## Reports and canonical execution

Each run writes `run-configuration.json`, `summary.json`, `report.md`, a result
under `cases/` for each selected row, and evidence under `evidence/<case-id>/`.
Each assertion has its own verdict and concrete artifact paths. Native output,
S3 acknowledgements, independent inventories, raw Admin samples, collection
times, exact commands/exit codes, cleanup and ownership journals are retained.
Every non-PASS case makes the runner exit nonzero. Reports never convert a
capability gap or a successful public subset into a complete PASS.

The canonical four-argument runner supports these IDs:

```bash
export RUSTFS_USAGE_ENDPOINT=http://test-node:9000
export RUSTFS_USAGE_ADMIN_ENDPOINT=http://test-node:9000
bash scripts/run_validation_case.sh \
  scanner.usage-01-static-accounting \
  /path/to/deployed-candidate.json \
  "$(git rev-parse HEAD)" \
  /path/to/new-canonical-artifacts
```

The existing host/shared-lock procedure must be applied by the environment
owner before using this endpoint-only adapter on a shared VM lab. The adapter
does not take ownership of deployments or other users' buckets.

Tool-only verification, with no endpoint or RustFS binary:

```bash
python3 -B -m unittest -v test_validation_scanner_usage.py
python3 -B -m unittest -v test_validation_migration.py
python3 -B scripts/validation_catalog.py check
bash test_validation_scanner_usage.sh
```

The local test doubles verify error handling and assertion gates; their PASS
results are not product or VM acceptance results.
