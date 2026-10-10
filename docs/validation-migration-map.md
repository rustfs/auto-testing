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

The eight planned cases have these concrete reuse candidates:

| Validation ID | Candidate IDs | Specific item to verify |
| --- | --- | --- |
| `scanner.usage-convergence` | STOR-134 | Existing test checks 20 objects appear in Admin usage; add scanner result and fixture agreement. |
| `kms.sse-kms-round-trip` | KMS-104, KMS-301, KMS-214 | Compare key/metadata semantics and failure redaction. |
| `tiering.transition-and-restore` | TIER-105, TIER-107 | Compare remote tier restore and object checksum. |
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
