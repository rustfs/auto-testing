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
`ilm.delete-all-versions`, `ilm.delete-marker-expiration`,
`ilm.multipart-expiration`, and `ilm.newer-noncurrent-retention` also passed
live in runs `38049740183`, `38049817172`, `38049890130`, and `38049965929`.
`ilm.noncurrent-expiration`, `ilm.restart-persistence`,
`ilm.transition-and-restore`, and `ilm.versioned-current-expiration` passed in
runs `38050040606`, `38050434607`, `38050720293`, and `38051148291`.
All 16 ILM/scanner cases now have live PASS evidence.

All 13 Pool lifecycle IDs now dispatch to the source black-box pool checks.
The driver reuses the existing auto-testing Pool suite's two-node seed and
expansion operations, supplies marker-guarded reset/topology/restart/physical
census hooks, and requires source case evidence plus physical cleanup before
canonical PASS. `pool.status` passed live in RustFS Actions run `38047001285`;
`pool.decommission` and `pool.decommission-cancel-clear-resume` also passed in runs `38051227313` and `38052304396`. `pool.decommission-live-io`, `pool.decommission-restart-recovery`, `pool.decommission-versioned-integrity`, `pool.expand-and-rebalance`, and `pool.expansion-restart-persistence` passed in runs `38053444515`, `38053814812`, `38054128863`, `38054208114`, and `38054287896`. `pool.decommission-multipart-integrity` failed in run `38053715867`: no fresh owned-bucket MPU drain failure evidence was found; cleanup passed. `pool.operation-guards` and `pool.rebalance-live-io` passed in runs `38054540360` and `38054696080`. Two Pool cases still await live evidence; the multipart failure remains visible.

The 18 bucket-replication and 20 site-replication IDs now dispatch to the
source black-box checks. The driver uses the existing auto-testing four-node
main cluster and second RustFS instance on port 19000, including the source's
site-group fixture and site outage hooks. Logical bucket isolation and
physical teardown are required for PASS.
`bucket-replication.source-versioning-required` passed live in run
`38049256196`; the other 37 replication cases still require live evidence.

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
`events.delivery` passed live in RustFS Actions run `38046470999`.
`audit.request-correlation` passed in run `38050138583` with the candidate's
`Records` envelope and exact S3 operation names. The audit fixture uses a
distinct random secret and retains the secret-leak, request-ID,
object-identity and successful-status assertions.

`oidc.login-and-session` uses the Security suite's current single-node
deployment and a run-owned Keycloak container with a pinned image. It performs
password login, verifies issuer/audience/subject, exchanges the identity token
for STS credentials, and checks the readonly identity policy plus a narrower
session policy on existing objects. Missing/invalid session tokens, tampered
identity tokens, and wrong audiences must fail. Fixtures, containers, and the
RustFS deployment must all be cleaned before PASS. Runtime credentials are
excluded from evidence. This executor does not require a RustFS checkout.
Its complete live candidate run passed in RustFS Actions run `38051425083`.
Policy denials require `AccessDenied`; absent or mismatched temporary session
tokens require the candidate authentication contract's `InvalidRequest`.

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
case evidence. The 106 currently dispatchable IDs include all 74 non-Heal cases and thirty-two Heal cases. Forty-two have live PASS evidence; the other 116 have no live PASS claim.

All 84 Heal recipes, 301 operator branches and 532 applicable topology branch
templates are preserved under `scripts/validation_heal`, with the public
contract, operation book, signed probe, ACK-logged workload, guarded file-fault
tool and source-bound metadata inspector source. `matrix.py plan` binds current
auto-testing SSH nodes and required resources; its audit checks every branch
and nine evidence stages without manufacturing PASS.
`heal.admin-healthy-noop` now has an automatic H1/H2 executor using the existing
Heal suite's immutable deb download, installed binary digest, independent disk
mounts, run-owned services/data roots, exact normal/deep responsibility
receipts, repeated status, physical hashes and complete S3 inventories. It
failed live in run `38052673980`: the candidate returned an `unknown` receipt for a fresh zero-byte object with no bucket-incarnation identity, violating the source healthy-object contract. Cleanup passed; the oracle remains strict.
`heal.admin-invalid-request` passed all six branches on both H1 and H2 in run `38052868969`, with complete peer coverage and unchanged physical/S3 fixtures.
The external-shard executor also dispatches C01–C04, D02, E01 and E02 with all 24 source branches on H1/H2. It freezes exact version/dataDir/part, actual erasure role/set slot/disk UUID and full original shard hashes, stops all run-owned services before fault injection, witnesses the same fault immediately before the manual token, and checks complete physical repair plus streaming HighwayHash before any target GET. Full S3 inventories, cross-part Range, HEAD and pending MPU controls retain source semantics. Backups are used only for failed-case rescue or finalization; an unexpected new generation keeps the namespace DIRTY. Protected-mode C01 passed all ten H1/H2 branches in run `38057373420`; the other six executors still require live evidence. A03 also has both H2 branches: a non-coordinator shard fault, exact token queries through coordinator/peer/fixed current-fleet entry, a 30-second convergence budget for task start and full terminal outcome, physical proof and S3 inventories through all four entries.
A04 dispatches all six dry-run branches with separate DB/DV fixtures on H1/H2. It freezes version and marker identities, requires complete dry_run_observed receipts and zero repaired counters, proves byte-for-byte no mutation, then rescues the injected fault before full S3 verification. It does not require a dry-run incarnation or claim backup restoration as product repair. The first C01/C02 runs (`38055041726`, `38055254447`) stopped before injection because the remote script had an invalid future-import placement; their FAIL records and clean finalization are retained. The script compiler regression is fixed, and fresh runs use the corrected executor.
`heal.range-head-and-conditions` preserves all seven D07 branches, with real baseline/post-repair HEAD, cross-part Range, exact ETag conditions, and server-time-derived conditions including 304/412/416 negatives. Completed MPU target-side stale `part.N.meta` cleanup is allowed only on the repaired drive/dataDir and for parts still represented by the unchanged authoritative xl.meta; controls and payload shards remain exact.
`heal.historical-versions` now preserves all four D03 branches: historical and
current part faults on distinct physical roles/drives, a recursive bucket fault
with missing historical part and another drive's complete xl.meta, and multiple
versions faulted together. All acknowledged versions and markers must have
exact, complete responsibility receipts and unchanged S3/physical references.
`heal.repeat-idempotency` preserves all four J04 branches. It freezes the first
repair's physical/S3 result, repeats normal/deep with independent tokens, and
restarts the same run-owned units for the process branch. The versioned branch
removes a whole xl.meta with three data versions and a marker; first repair may
attribute all identities or a mixture of repaired and positive health, while
repeat permits zero repairs and mode-specific health only. Counters must agree
with the complete receipt set. D05 `heal.versioning-suspended-null` now uses real
S3 versioning suspension, consecutive null overwrites and acknowledged null
marker creation/deletion. It faults a live null data/parity part or an entire
xl.meta containing the current null marker and historical versions, verifies
every exact identity, then re-enables versioning and checks a new non-null
version plus the unchanged original version set. These three executors still
require live evidence.
B07 `heal.below-read-quorum` now retains exactly R-1 same-version authorities
and isolates all other part files, separately for never-versioned and historical
fixtures. The exact candidate tool build freezes the recognized metadata read
quorum rule, S3 HTTP/error mapping and source digests. Unrecognized or stale
contracts cannot dispatch the negative case. GET must return the frozen refusal
without payload or an unknown transport outcome; manual deep Heal must retain
the exact failed version and complete truthful counters. All good parts and
metadata must remain unchanged. Only after that failure proof may backups be
restored and a new token verify recovered integrity; recovery never proves
product repair during the negative window. This executor still requires live
evidence and a fresh tool build containing the quorum contract.
C05 `heal.missing-or-corrupt-xlmeta` also has all five automatic branches on
H1/H2: missing/truncated/header-corrupt xl.meta, a flip inside the unique decoded
inline value, and whole metadata loss with three historical data versions and a
marker. The tool now freezes each inline value's physical offset/length/SHA-256
and verifies its streaming HighwayHash framing. Complete original inline shard
digests, version/dataDir/part/role references and all S3 versions must be restored.
A decoder error can be captured only when its file length/hash exactly matches
an injected fault receipt; it never becomes valid metadata or a repair proof.
Legal metadata re-encoding may move an inline value while its exact length/hash
stay fixed. The extended helper passed the 24/12 external shard oracle and one
frozen released-process inline fixture with three corruption negatives locally;
this executor still needs a new candidate-bound tool build and live evidence.
D06 `heal.object-metadata-and-tags` preserves all three H1/H2 branches with
three historical payloads per Unicode/space/literal-percent key, actual gzip
content, encoded Chinese user metadata and two distinct tag groups. It checks
the initial normalized metadata/Content-Type/Encoding/Disposition/Cache-Control
and precise versioned tags, then faults an external data part, an entire xl.meta
header, or a parity part. Exact physical/version references, complete S3
inventories, HEAD and ranges must agree after repair. This executor still
requires live evidence.
The other 52 Heal cases remain
operator-assisted and are not claimed as migrated automatic executors.
H4 needs at least 24 independent disk endpoints; H6 and native-fence branches
retain their platform and source-bound evidence requirements.

The RustFS tool-build lane produced the initial candidate-bound metadata inspector in run `38052536366`; A07 also passed with that tool in run `38053214984`. The extended bitrot helper was successfully built against the same candidate in run `38054812014`, including the released-shard oracle checks. Set `validation_tools_only=true` with the pinned build and auto-testing SHA to build the current helper. Heal live runs consume `validation_tools_run_id`/`validation_tools_run_attempt`; the workflow verifies the successful build attempt, candidate/helper identities and binary/lock digests. The node adapter installs the verified tool under its run-owned root. Plain metadata decoding makes no payload-integrity claim; the new explicit part-verification mode validates current and legacy streaming HighwayHash. The build checks 24 healthy shards captured from external released processes and rejects 12 controlled corruptions. Ordinary live execution needs no RustFS source checkout.

The eight planned cases have these concrete reuse candidates:

| Validation ID | Candidate IDs | Specific item to verify |
| --- | --- | --- |
| `scanner.usage-convergence` | STOR-134 | Dedicated executor and live PASS evidence. |
| `kms.sse-kms-round-trip` | KMS-104, KMS-301, KMS-214 | Dedicated executor and live PASS evidence. |
| `tiering.transition-and-restore` | TIER-105, TIER-107 | Dedicated executor and live PASS evidence. |
| `events.delivery` | EVT-103, EVT-104 | Dedicated executor and live PASS evidence. |
| `audit.request-correlation` | AUD-102, AUD-104, AUD-106 | Dedicated executor and live PASS evidence. |
| `iam.policy-enforcement` | IAM-105, IAM-109 | Dedicated executor and live PASS evidence. |
| `oidc.login-and-session` | OIDC-103 | Dedicated executor using real Keycloak login and STS session boundaries; live PASS evidence. |
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


The corrected default-mode C01 run `38055597354` executed every H1/H2 fault branch with clean finalization. Single-part bytes and HighwayHash were restored, but the candidate returned unknown responsibility with no incarnation. The candidate explicitly defaults independent-shard writes off; current positive fixtures now enable both shard-integrity write and fleet-confirmation knobs only after every private peer is bound to the same candidate. Their values are read back from the actual processes and archived. The default-mode failure remains separate and never becomes a positive proof. New protected-mode runs are required before any new PASS claim.

Protected-mode A01 run `38056636793` passed all 512 exact H1/H2 normal/deep
object responsibility receipts and clean finalization. This proves only the
explicit, uniformly enabled shard-integrity fixture configuration; the default
deployment failure remains retained. A04 run `38056884022` passed DB branches
but failed DV receipt completeness with cleanup PASS. The executor had used a
nonrecursive object request, which gives latest-object responsibility rather
than all historical versions. DV requests now explicitly set recursive=true
for the exact object prefix; the original FAIL remains and a new run is required.
The corrected recursive DV request passed all DB/DV H1/H2 branches in run
`38057908339` with clean finalization, including complete dry_run_observed
version/marker receipts and byte-for-byte no-mutation proof.

D03 run `38058420193` passed historical/current/multiple-version branches on
both H1/H2, but its bucket-recursive branch used an extra trailing slash and
received HTTP 501 before admission. Cleanup passed. The task URL now uses the
registered exact bucket route; this harness failure remains retained and D03
needs a fresh full run before a canonical PASS claim.

J04 run `38058699913` retained FAIL with clean finalization. Its DB checks hit
an executor Range bug on exact/near-1MiB controls; positive ranges now use
actual bounded intervals and have boundary regression coverage. The DV H1
branch passed first repair and both normal/deep repeats. DV H2 physically
restored all original bytes, decoded version references and S3 manifests, but
the candidate attributed the current delete marker as unknown while three data
versions were repaired. That genuine receipt failure remains strict and visible.
Tool build `38058949922` successfully produced the current decoder plus the
recognized exact-candidate quorum/error contract for B07.

The corrected D03 run `38059278988` admitted every source branch, passed H1
completely and passed H2 historical/current/recursive branches. H2's simultaneous
multiple-version fault left one original current-version parity part missing.
The exact physical oracle retained FAIL and clean backup-assisted finalization;
that restoration does not count as product repair.

Fresh corrected J04 run `38059567206` passed every DB/DV H1/H2 source branch,
normal/deep independent-token repeats and same-unit process restarts, with clean
finalization. The earlier same-candidate H2 unknown-marker failure in run
`38058699913` is still retained; the new PASS does not rewrite that evidence or
claim a product fix. Tool build `38060217084` successfully built the extended
inline verifier and checked the 24 healthy/12 corrupt external shards plus the
one healthy/three corrupt released-process inline samples.

D05 run `38060400818` exercised all H1/H2 suspended/null branches and cleaned
up successfully, but its receipt oracle compared the Admin typed nil UUID with
the S3 `null` spelling literally. The candidate source defines these as the same
null identity. The oracle now normalizes only those representations and still
rejects every other version mismatch. The original FAIL remains; a fresh full
run is required. Auto-testing CI passed all six lanes at commit `f9c153f`.

B05 `heal.multiple-shard-loss` now expands 1..M losses from the actual decoded
EC geometry, including every possible data/parity role count. Each parameter
instance uses a distinct acknowledged object; DB and DV both run on H1/H2.
All drives remain online for the write/lock condition while selected same-version
parts are isolated. Exact full original shard hashes, streaming checksums,
complete version/marker responsibility and S3 controls must all pass. B05 has
not yet been validated live.

B07 run `38060686737` retained FAIL with cleanup PASS. At R-1 shards H1
returned HTTP 500/InternalError instead of the recognized candidate read-quorum
refusal. H2 returned 503/SlowDownRead, but Heal recorded a grace-window deferred
receipt and completed execution rather than the source's required failed
responsibility. Good-shard preservation and the raw task outcome remain
archived; neither branch is promoted to PASS.

D10 `heal.special-keys-and-pagination` now creates 265 unique objects, including
Unicode, spaces, literal percent/encoded slash, real slash, nested paths and
adjacent `a/`/`ab/` controls. Both version and current LIST explicitly request
MaxKeys=17 and retain observed page sizes; the complete ACK manifest must match
without missing or duplicate keys. Exact logical-key physical mappings precede
part/xl.meta faults and signed object requests. Every control remains exact.
D10 has not yet been validated live.

C05 run `38061075879` exercised all ten DB/DV H1/H2 branches and retained FAIL
with cleanup PASS. Nine branches passed. H1's truncated xl.meta remained at the
exact injected 1866-byte length instead of its 3732-byte baseline and could not
decode; the physical oracle rejected it before target GET.

D06 run `38061589839` passed all H1/H2 semantic branches with cleanup PASS:
three distinct versions for each Unicode/space/percent key retained exact public
metadata, actual gzip bytes/headers and versioned tags through data, metadata
and parity faults. Physical original-shard/bitrot and full S3/HEAD/Range checks
passed before receipt promotion.

D01 `heal.object-size-boundaries` now has a complete automatic parameter matrix
for zero/one, 4KiB, actual inline and EC stripe boundaries plus a multi-block
external object. A tool-build contract freezes recognized candidate policy and
source/extractor digests; live process environment and actual EC metadata derive
the limits. H1/H2 need different inline limits under the recognized current
policy (the runtime derives these rather than guessing 256KiB). Each size tests
both actual data/parity roles with missing and bitflip faults. Zero objects use
metadata loss/header faults and never invent a payload part. Exact byte hashes,
complete responsibility, HEAD/LIST and actual first/tail/stripe Range checks
are required; zero Range is a strict S3 416/InvalidRange negative. Tool build
`38062137032` successfully produced the recognized size and quorum contracts
for the fixed candidate, with current extractor/source and binary identities.

Corrected D05 run `38061843290` passed every H1/H2 suspended/null branch and
clean finalization. Real enabled-period history, null overwrites and markers
retain precise S3 identities; the typed Admin nil UUID is mapped only to that
acknowledged null version. The candidate then re-enabled versioning and created
a distinct non-null version without changing old payloads or version/marker
identities. The original literal-identity harness failure is still retained.

A08 `heal.admin-authorization` now dispatches every H1/H2 source principal
branch. Anonymous, deliberately bad, correctly signed but expired, and denied
IAM requests each test start/status/stop with exact HTTP/error refusals and
recursive JSON/XML task-disclosure rejection. A temporary principal with only
`admin:Heal` admits, queries and stops its independent token while S3 reads stay
denied. Temporary users/policy/tasks are finalized and credential revocation is
verified; original physical/S3 fixtures must remain unchanged. Evidence redacts
root and temporary credentials without altering oracle inputs. Nine focused
tests cover independent SigV4 verification, denial leakage, lost IAM admission,
cleanup failures and revocation. A08 still requires full live evidence.

D01 run `38062610870` retained FAIL after 35 H1 parameter attempts. Three
physical comparisons rejected changed target-side `xl.meta.bkp` files; archived
per-drive comparison shows each new backup is the exact pre-repair authoritative
`xl.meta`, while payload shards and decoded logical identities agree. The
original verdict remains retained pending a new complete run with a bounded
target-backup oracle. A fourth attempt was rejected before injection because
its expanded fault ID exceeded the tool limit; retrying that invalid ID during
finalization caused cleanup FAIL. Fault IDs now use branch/index digests within
the allowed length. Recovery run `38063798238` independently proved stopped
owned service, no injection intent, exact archived pre-fault physical scene and
removed run roots. It records cleanup PASS, `originalVerdictRewritten=false`
and no canonical/product-repair PASS claim. Serial deployment resumed only
after inspecting that recovery evidence.

B09 `heal.node-outage-rejoin` now has both source H2 branches with independent
DB and DV deployments on the existing four nodes. The candidate tool freezes
the recognized same-set read/write quorum policy; actual metadata maps the
four lost drives before admitting degraded I/O. Exact unit exit and refused
endpoint connections witness the outage. PUT/overwrite/marker/historical-delete
ACKs define the new manifest, while the stopped node's complete physical scene
stays unchanged. Only the explicit run-owned `offline-replay` profile enables
MRF; scanner and automatic disk-rebuild remain disabled. After the same node
and roots rejoin, physical polling precedes any target S3 reads or manual task.
Every drive must contain the exact acknowledged version/marker set; surviving
shard bytes, committed streaming checksums and all-node S3 manifests must agree.
Deleted identities receive exact S3 absence checks. No copied backup contributes
to repair. Six focused tests cover profile boundaries, source-quorum recognition,
ACK ambiguity, per-drive omissions/resurrection and deleted-read refusals.
B09 requires the fresh write-quorum tool artifact and full live evidence.

D01 run `38064187494` completed all 96 H1/H2 parameters: 76 passed and 20
failed the then-strict existing-backup oracle; cleanup passed. Independent
forensic comparison of all 20 scenes found only target `xl.meta.bkp` updates
to the exact pre-repair authoritative metadata. The corrected oracle now
bounds both new and existing backup updates to that drive/key/dataDir and
exact size/hash. The original FAIL is retained; a fresh full run is required.

A08 runs `38066188001` and `38066437827` failed policy creation because bare
`Resource:["*"]` is not an ARN. The fixed candidate accepts `admin:Heal`
without Resource. Corrected run `38066970195` reached every branch: bad and
expired signatures, denied user and least-privilege user passed on H1/H2.
Anonymous traffic returned 403/AccessDenied from S3Router::check_access before
the handler, exposing an incorrect 400/InvalidRequest harness expectation.
The route-level expectation is now exact; all original FAILs retain cleanup
PASS and none is promoted without a fresh complete run.

Tool build `38065628505` succeeded against the same fixed candidate and carries
the recognized `data-plus-one-if-equal-parity` write-quorum policy. Its helper,
extractor, candidate and binary digests were verified locally; it is used by
the serial live queue for D01/B07/B09. Ordinary runs still need no RustFS checkout.

D09 `heal.object-lock-retention` now dispatches all four H1/H2 source branches
on real Object Lock/versioning buckets. Three versions per key retain exact
Governance/Compliance metadata or ON legal hold; the ten-minute UTC window is
anchored to the server HTTP Date. A run-owned principal can read but explicitly
cannot bypass Governance. Baseline and pre/post-fault DELETE must return exact
403/AccessDenied while retention is active. Data/parity faults and whole xl.meta
loss cover current/historical versions; all affected version shards, public
properties, tags and responsibility receipts remain independently checked.
Finalization releases hold and Governance only through normal APIs, waits up to
twelve minutes for real Compliance expiry and deletion ACK, proves the bucket
empty/absent, and removes IAM resources. Only then may physical roots be removed.
Incomplete finalization retains a stopped namespace with cleanup FAIL. Partial
seed ownership, real server time, same-drive multi-version proof selection and
protected cleanup failure boundaries have focused tests. Full live evidence
is still required.

C08 `heal.metadata-payload-mismatch` dispatches every H1/H2 branch with three
real versions per distinct key. The donor is another acknowledged version's
same-role/same-slot part: a different physical length for length mismatch,
the same length with different bytes for payload mismatch. Candidate-bound
streaming verification proves both original and donor cohorts before injection.
The descriptor-relative fault layer admits only another owned same-number part
on the same disk, exact donor/target hashes and quiescent service; it rejects
metadata, traversal, symlinks, hardlinks, unowned buckets and identical bytes.
Intent and original bytes are persisted before replacement. Every physical
fault receipt must equal the frozen donor; metadata and controls stay unchanged.
The negative branch leaves exactly R-1 correct same-version shards and requires
exact read refusal, truthful failed responsibility and no physical mutation
before rescue. Restored backups certify cleanup only; a later-generation target
cannot be overwritten. Positive branches require original shard bytes, full
version metadata/contents/tags, streaming checksums and repaired receipts.
This executor has local fault/recovery/authority tests and still needs live proof.

E03 `heal.read-repair-attribution` dispatches GET-missing and Range-bitrot DB
branches plus an exact historical-version DV read on H1/H2. Its explicit
profile leaves scanner, automatic rebuild and best-effort MRF ingress off,
while enabling candidate JSON debug events; the candidate's durable consumer
stays initialized independently of that ingress switch. Before each single
GET/Range the exact fault remains physically present. Correct bytes, exact
version/ContentRange and a server request ID are required. A new structured
`Read-repair heal request admitted` event must match bucket/key/part/pool/set
and accepted task ID; stale events, text mentions, merges and ambiguous tasks
cannot prove attribution. Only status queries observe that automatic task;
no Admin start/deep verifier repairs the fixture. The task's exact version
receipt, physical original shard hashes, streaming checksums and full public
S3/version controls must all agree. Runtime credentials are redacted from
archived debug journals. Focused tests cover source identity, stale-task
refusal, single-read/no-mutation dispatch, exact content/range/request identity
and runtime profile isolation. Full live proof remains pending.

Corrected D01 run `38067139720` passed all 96 H1/H2 parameters (48 per
topology), with canonical PASS, complete summary and cleanup PASS. Its exact
candidate/package/binary/harness identities and each parameter's evidence were
downloaded and independently checked by the canonical report gate. It covers
zero/one/4KiB, actual inline and EC-block boundaries, multi-block payloads,
both roles, missing/bitflip faults and exact Range outcomes. The three prior
D01 failures and independent recovery record remain unchanged.

B02 `heal.disk-readonly-and-permission` dispatches both H1/H2 branches. The
read-only gate uses systemd PrivateMounts/ReadOnlyPaths for the selected
run-owned data root; host mounts are never remounted. The permission gate
records/restores that root's original 0700 mode and drops both DAC override
capabilities from the root service, so chmod cannot be bypassed. A write probe
enters the actual service mount namespace with the same effective UID/GID and
DAC boundary and requires exact EROFS/EACCES. Original shard isolation precedes
the gate. Failed/deferred exact-version responsibility must show zero repaired
counters and the complete physical fault scene must remain unchanged. After
service exit and gate release, a distinct task must actually repair the
original target; full physical, streaming and public checks follow. Gate intent
precedes chmod/start and is recovered before any fault rescue or physical
cleanup. Root drift or release failure retains the namespace. Focused tests
exercise permission restoration, write refusal identity/errno, no root bypass,
truthful unresolved receipts and cleanup retention. Live proof is pending.

E04 `heal.scanner-cold-object-repair` now dispatches all three H1/H2 branches.
It reads back the actual private process settings for five-second scanner
cycles, every-cycle bitrot, zero deep cooldown and full object selection, while
replacement auto-heal and best-effort MRF stay disabled. Before any fault, each
cold target needs an exact healthy scanner task and unchanged physical files.
Faults are injected with all run-owned processes stopped; the stopped scene
proves damage immediately before restart. Cursor-bounded journal windows bind
the exact object discovery to an accepted Scanner source request, task start,
completed cycle and terminal task. The cycle-boundary branch requires two
complete post-fault cycles. Observation issues status queries only; no target
GET/HEAD or Admin mutation contributes to the repair. Exact repaired version
responsibility, original physical shard bytes, streaming checksum and complete
S3 inventories still gate PASS. Truncated logs, another source, a later healthy
receipt or cycle counters alone cannot certify repair. The executor passed its
local source/timing/cursor negatives; full live evidence remains pending.

D04 `heal.delete-marker-semantics` now dispatches its four H1/H2 source
branches as six real instances. Each key has three separately acknowledged
data versions and, where applicable, a current or historical marker. Missing
replicas cover both physical roles. Stale creation and deletion isolate one
original drive with a service-identity read/write EACCES gate, perform one
exact marker mutation, and prove the gated replica stayed unchanged while all
other members carry the ACK authority. The expected metadata is derived only
for comparison from that authority and the original target-drive data
references; the harness never writes synthesized metadata. Formats, data
shards, controls, exact version receipts and streaming checksums must agree.
Every peer checks current-marker 404 and explicit-marker 405 with exact marker
identity headers. Explicit current-marker deletion must reveal the precise
previous data version, remove the marker identity and preserve the full
remaining version/public-property manifest. A failed or ambiguous stale ACK
stops the topology and retains its result; gate cleanup precedes physical
finalization. Local ACK/order/authority/header/gate negatives passed; full live
evidence remains pending.

J01 `heal.status-partial-disabled-unknown` now has a dedicated current-fleet
executor for H1/H2. Fresh manual and disabled deployments retain actual process
configuration, complete physical fixtures and both raw Admin and rc JSON/text
outputs. Peer coverage must balance exactly; absent peers need an explicit
unknown count/reason and cannot report idle. Any raw or CLI sample that erases
uncertainty fails immediately, including during bounded convergence. H2 stops
one original peer, witnesses its process exit and connection refusal, and
queries every survivor before restarting the same member. The source's step 2
explicitly restricts that branch to H2; H1 records its applicability reason
and instead proves an original-service restart followed by complete idle.
A startup observer begins before any service starts and requires raw
uninitialized status to bracket matching rc/replacement observations. A
transport error or an already initialized service cannot prove that window;
NOT_OBSERVED makes the complete case fail. Original formats/object files and
full S3 manifests must remain unchanged. Temporary rc credentials use a 0600
configuration outside artifacts and are removed during finalization. Local
coverage/uncertainty/startup/config-cleanup negatives passed; live evidence
remains pending.


J03 `heal.incremental-cursor-and-truncation` now has a current-fleet DX
executor with 1025 pre-registered external objects of 1MiB+73 bytes. The
reference candidate retains 1024 result items, separately from its 128-object
outcome diagnostics; a 257-object task cannot prove the retention-gap branch.
The actual terminal minSeq must move and an old cursor must carry truncation.
Incremental queries treat sinceSeq as exclusive and advance only over returned
items, including when the response payload itself is cropped. Same-task bounds
cannot move backwards or duplicate object/version identities. At least two
useful running windows are mandatory; NOT_OBSERVED prevents a complete PASS.
H2 queries every actual node with the admitted path/token and uninterrupted
deployment epoch; H1 records why a cross-node query is inapplicable. The
candidate renders startTime anew on each response, so A03 also binds its
original query path/token instead of comparing that timestamp. Canonical
counters must cover all 1025 ACKs with the exact 17 real missing-shard repairs,
no failures/unknowns, and complete traversal. Bounded diagnostic receipts remain
explicitly incomplete. Full physical manifests, every original same-role shard
and streaming checksum, and all-peer S3 manifests independently cover every
object before PASS. The case has a fixed 12288-file snapshot budget, 128-part
verification batches, a 45-minute service/lease budget and 2GiB free per drive.
Local cursor/gap/counter/scope/part-omission negatives passed; live evidence
remains pending.


A06 `heal.admin-stop-and-stale-token` now uses the same pre-registered 1025
external-object DX fixture on H1/H2. Cancellation requires actual running,
nonzero partial canonical progress and both repaired and still-missing targets.
The frozen candidate policy requires active stop acknowledgement, retained
aborted/cancelled partial responsibility, and exact 500/InternalError with the
retired token for repeat/stale stops. Three complete physical snapshots with
five-second gaps keep the service running after cancellation; stopping the
process cannot substitute for task write quiescence. Actual restored shard
counts must equal the retained cancellation counters, and remaining shards
must still be missing. A distinct new token must be running before and after
old-token STATUS/STOP; unobserved brackets prevent PASS. The new task repairs
only the remaining defects, and its completion must preserve the original
cancelled outcome. Full streaming/physical and all-peer S3 verification happens
before backup rescue. All four source branches remain explicit. Local
cancellation/counter/quiet-physical/token/error negatives passed; full live
proof remains pending.
