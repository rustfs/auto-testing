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
It has local mock coverage and live PASS evidence in RustFS Actions run
`38042083422`.

`scanner.usage-convergence` has a dedicated executor on `single-4disk`. It
uses the storage suite's deployment and SigV4 Admin client, accelerates the
scanner cycle, records the 20-object fixture, and checks a successful leader
cycle after upload plus a newer complete usage snapshot with exact object and
logical-byte counts. The old STOR-134 threshold remains a separate regression
test. Its live candidate run passed in RustFS Actions run `38042200106`.

`scanner.status-readiness` has a dedicated executor on two of the existing
auto-testing nodes, each with four disks. It checks both nodes' accelerated
scanner runtime settings and requires exactly one leader to finish a new
successful cycle within 120 seconds. Its live candidate run passed in RustFS
Actions run `38044125099`.

The 14 remaining ILM IDs now dispatch to the source ILM black-box behavior
checks under `scripts/validation_ilm`. The Storage suite deploys the same
two-node/four-disk source topology on current auto-testing nodes with the
source's accelerated scanner/ILM timing, and supplies a cluster restart hook
for `ilm.restart-persistence`. `ilm.transition-and-restore` uses the third
auto-testing node as an independent warm-tier target. Each selected case must
pass its own harness result, evidence, and bucket-cleanup verification before
the canonical result can become PASS. `ilm.configuration-roundtrip`,
`ilm.invalid-configuration`, `ilm.current-expiration`,
`ilm.tag-filter-expiration`, `ilm.disabled-rule`, and `ilm.rule-removal` have
live PASS evidence in RustFS Actions runs `38044956285`, `38045047518`,
`38045154019`, `38045412209`, `38045496042`, and `38045714246` respectively.
The other eight ILM executors have local contract and dry-run checks but no
live PASS evidence yet.

All 13 Pool lifecycle IDs now dispatch to the source black-box pool checks.
The driver reuses the existing auto-testing Pool suite's two-node seed and
expansion operations, supplies marker-guarded reset/topology/restart/physical
census hooks, and requires source case evidence plus physical cleanup before
canonical PASS. These cases have not yet completed live validation.

The 18 bucket-replication and 20 site-replication IDs now dispatch to the
source black-box checks. The driver uses the existing auto-testing four-node
main cluster and second RustFS instance on port 19000, including the source's
site-group fixture and site outage hooks. Logical bucket isolation and
physical teardown are required for PASS. These cases have not yet completed
live validation.

`kms.sse-kms-round-trip` has a dedicated executor using the existing local
KMS single-disk deployment. It checks the same object's encryption key,
metadata, and payload, then requires an invalid-key PUT to fail without
exposing plaintext or key material. Its live candidate run passed in RustFS
Actions run `38042359938`.

`tiering.transition-and-restore` has a dedicated executor using the Tier
suite's existing `single-single` source and warm-tier nodes. It verifies the
same object's transition state, readable bytes after transition, restore
state, and SHA-256 after restore. The case is recorded under the source
`lifecycle-b` environment profile. Its live candidate run passed in RustFS
Actions run `38042759074`.

`events.delivery` and `audit.request-correlation` now have dedicated executors
on the Tier suite's `single-single` topology. Event delivery is counted for a
unique object after a quiet window; audit checks matching PUT/GET records,
distinct request IDs, successful status, and absence of known secrets.
`events.delivery` passed live in RustFS Actions run `38046470999`. The audit
attempt in run `38046574073` failed its secret check because the environment
used the same value for the public access key and secret key. The audit case
now generates a distinct secret key before deployment; it still requires a
live rerun before it can be counted as verified.

`iam.policy-enforcement` has a dedicated executor on the Security suite's
`single-single` deployment. It uses one principal with a policy that allows
PUT/GET and explicitly denies DELETE, then checks the denied DELETE did not
remove the object. Its live candidate run passed in RustFS Actions run
`38043548625`.

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
case evidence. The 73 currently dispatchable IDs are the S3, two scanner,
KMS, tiering, events, audit, IAM, 14 additional ILM, 13 Pool, and 38
replication cases above. Thirteen have live PASS evidence; the other 145 IDs
remain unverified.

The eight planned cases have these concrete reuse candidates:

| Validation ID | Candidate IDs | Specific item to verify |
| --- | --- | --- |
| `scanner.usage-convergence` | STOR-134 | Dedicated executor and live PASS evidence. |
| `kms.sse-kms-round-trip` | KMS-104, KMS-301, KMS-214 | Dedicated executor and live PASS evidence. |
| `tiering.transition-and-restore` | TIER-105, TIER-107 | Dedicated executor and live PASS evidence. |
| `events.delivery` | EVT-103, EVT-104 | Dedicated executor and live PASS evidence. |
| `audit.request-correlation` | AUD-102, AUD-104, AUD-106 | Dedicated executor; live evidence pending. |
| `iam.policy-enforcement` | IAM-105, IAM-109 | Dedicated executor and live PASS evidence. |
| `oidc.login-and-session` | OIDC-103 | The live Keycloak path can be skipped by the old suite; validation requires an actual session-boundary result. |
| `s3.basic-object-round-trip` | S3C-101, 201, 203, 205, 206, 207 | Dedicated executor and live PASS evidence. |

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
