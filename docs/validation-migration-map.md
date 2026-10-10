# Release validation case migration

Source: `rustfs-release-validation/scenarios/catalog.yaml`. The generated
`validation-cases.json` preserves all 158 canonical IDs, source statuses,
assertions, probes, capabilities, and target environments. Its source commit
and SHA-256 bind the snapshot to the source catalog. `reuse_candidates` lists code to inspect;
it is not a coverage claim. A case's run result remains `unverified` until a
live run proves each source assertion on the required topology.

| Source module | Cases | Existing auto-testing code to inspect | Current gap |
| --- | ---: | --- | --- |
| Pool lifecycle | 13 | `rustfs_pool_expand.sh` | Current steps combine operations; each validation ID needs its own assertions, object manifest, and result. |
| ILM/scanner | 15 | `rustfs-tier-test.sh`, `rustfs-s3-compat-test.sh` | Existing tier/lifecycle CRUD tests do not prove the accelerated scanner deadlines and all 15 expiration outcomes. |
| Bucket replication | 18 | `rustfs-replication-test.sh` REP-101..109 | Existing cases cover some operations; source filters, exact topology, sync behavior, failure recovery, and result evidence need individual checks. |
| Site replication | 20 | `rustfs-replication-test.sh` SITE-101..106 | Existing cases cover part of setup, IAM, objects, policy, status, and removal; canonical case boundaries and missing assertions remain. |
| Heal | 84 | `rustfs_heal_test.sh`, `rustfs-fault-tolerance-test.sh` | Existing fault flows are useful helpers, but each A-J procedure requires its own fixture, fault, public assertion, and cleanup evidence. |
| Planned | 8 | KMS, Tier/Event/Audit, Security, S3, Storage suites | Existing tests may implement the behavior; compare each source assertion before promoting it. |

`s3.basic-object-round-trip` now has a dedicated executor on `single-1disk`.
Its Bash function combines bucket creation/removal, object PUT/HEAD/GET/LIST/
DELETE, payload SHA-256, metadata, ETag, and `NoSuchKey` checks on one fixture.
It has local mock coverage; live evidence against an immutable candidate is
still required before calling the migration verified.

`scanner.usage-convergence` has a dedicated executor on `single-4disk`. It
uses the storage suite's deployment and SigV4 Admin client, accelerates the
scanner cycle, records the 20-object fixture, and checks a successful leader
cycle after upload plus a newer complete usage snapshot with exact object and
logical-byte counts. The old STOR-134 threshold remains a separate regression
test. This case also awaits a live candidate run.

`kms.sse-kms-round-trip` has a dedicated executor using the existing local
KMS single-disk deployment. It checks the same object's encryption key,
metadata, and payload, then requires an invalid-key PUT to fail without
exposing plaintext or key material. Its live candidate run is pending.

`tiering.transition-and-restore` has a dedicated executor using the Tier
suite's existing `single-single` source and warm-tier nodes. It verifies the
same object's transition state, readable bytes after transition, restore
state, and SHA-256 after restore. The case is recorded under the source
`lifecycle-b` environment profile. Its live candidate run is pending.

"Existing validation environment" means the fleet used by `auto-testing`
and the RustFS `smoke-testing` workflows, with `RUSTFS_NODES` and
`RUSTFS_SSH_USER` supplied there. For Heal, reuse the nodes deployed by
`rustfs_heal_test.sh`; the source H1-H6 names describe required topology and
fault capabilities, not a separate SSH fleet. The ten `heal-extended` cases
have no source target-environment field, so their runtime binding must record
the actual auto-testing Heal nodes and topology before any result is scored.
The source's standalone ZIP, port 9100/9200, and dedicated systemd profiles
are reference implementations for assertions, not deployment targets here.

The RustFS Functional Chain workflow has a manual validation lane on the same
`smoke-testing` runner and `rustfs-shared-functional-tests-v2` concurrency
group. Set `build_run_id` and `build_run_attempt` to one successful nightly
build with its retained candidate artifact, `auto_testing_sha` to an exact
commit, and `validation_case` to one migrated ID. The lane resolves the
immutable package URL and SHA-256 from that build attempt, then calls
`scripts/run_validation_case.sh`. The dispatcher rejects an ID without an
executor before touching the fleet and runs the result report with
`--gate-case`, so a missing, failed, or incomplete selected case fails the
workflow. Each run uploads its candidate identity, canonical result, and
case evidence. The four currently dispatchable IDs are the S3, scanner, KMS,
and tiering cases above; the other 154 IDs remain unverified.

The eight planned cases have these concrete reuse candidates:

| Validation ID | Candidate IDs | Specific item to verify |
| --- | --- | --- |
| `scanner.usage-convergence` | STOR-134 | Dedicated executor added; live candidate evidence is pending. |
| `kms.sse-kms-round-trip` | KMS-104, KMS-301, KMS-214 | Dedicated executor added; live candidate evidence is pending. |
| `tiering.transition-and-restore` | TIER-105, TIER-107 | Dedicated executor added; live candidate evidence is pending. |
| `events.delivery` | EVT-103, EVT-104 | Freeze and verify the exact delivery count contract. |
| `audit.request-correlation` | AUD-102, AUD-104, AUD-106 | Verify request correlation and secret redaction in the same event trail. |
| `iam.policy-enforcement` | IAM-105, IAM-109 | Verify explicit deny and allowed action on the same principal/policy fixture. |
| `oidc.login-and-session` | OIDC-103 | The live Keycloak path can be skipped by the old suite; validation requires an actual session-boundary result. |
| `s3.basic-object-round-trip` | S3C-101, 201, 203, 205, 206, 207 | Tie CRUD, payload, metadata, ETag, and error semantics to the same fixture. |

Migration work on an ID is complete only when its result contains the exact
source assertion text in order, evidence paths for each assertion, the
candidate/source/auto-testing/rc provenance, and a clean finalizer result.
The report rejects a PASS that lacks any of those fields. Existing suite
results cannot be renamed or counted automatically.

The runner should use the catalog's target environment as a profile, keep
one unique artifact directory per run, write `RUNNING` before invoking a case,
and retain the primary failure when cleanup also fails. Tests that mutate the
shared fleet need the RustFS workflow concurrency group and an environment
lease. Special Heal topologies and source-bound tools are provided by
separate RustFS build/environment jobs; ordinary case execution consumes
their immutable artifacts.
