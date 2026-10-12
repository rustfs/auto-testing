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
`pool.decommission` and `pool.decommission-cancel-clear-resume` also passed in runs `38051227313` and `38052304396`. `pool.decommission-live-io`, `pool.decommission-restart-recovery`, `pool.decommission-versioned-integrity`, `pool.expand-and-rebalance`, and `pool.expansion-restart-persistence` passed in runs `38053444515`, `38053814812`, `38054128863`, `38054208114`, and `38054287896`. `pool.decommission-multipart-integrity` failed in run `38053715867`: no fresh owned-bucket MPU drain failure evidence was found; cleanup passed. `pool.operation-guards` and `pool.rebalance-live-io` passed in runs `38054540360` and `38054696080`. `pool.rebalance-stop-resume` and `pool.retired-config-removal` subsequently passed in independently verified runs `38110959836` and `38111198632`. The multipart failure remains visible.

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
case evidence. The 154 currently dispatchable IDs include all 74 non-Heal cases, 67 automatic Heal executors, and 13 disabled-by-default environment gates. The gates preserve the operator procedures and emit explicit BLOCKED results; they are not automatic live executors or PASS claims. Independent census currently confirms 101 unique complete canonical live PASS cases: 85 on source `072b57b37559fbe626aeda8abf61617fdcbdd926`, 12 on the older immutable source, two on source `16cf19817e0c81e24d0d787a916f734a34517ec5`, and two on source `e224eb48e5311cff022b04fd78c97a8572d5c270`. The latest additions are C08 metadata/payload mismatch `38235978152` and E03 read-repair attribution `38237270640`. The independently verified Pool tail runs `38110959836` and `38111198632`, A03 run `38198122041`, C04 run `38203454300`, the completed multipart run `38204339623`, D07 run `38205235080`, D09 run `38205854024`, D10 run `38207381816`, and manual-normal attribution run `38207956101` are included. Native, read-only, recovery and duplicate runs are excluded; the other 57 cases have no complete canonical live PASS claim.

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
Four Heal cases remain native/evidence-blocked and outside dispatch: C11, F07, I05 and J02. The 13 environment-gated cases have disabled-by-default control flows and oracles in `environment_cases.py`; actual execution requires an assigned and reviewed auto-testing resource adapter, and canonical results remain review-gated. See [the environment gate reference](validation-heal-environment-gates.md) for each missing resource, capability and native barrier.
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
object before PASS. The case has a fixed 16384-file snapshot budget, 128-part
verification batches, a 45-minute service/lease budget and 2GiB free per drive.
Local cursor/gap/counter/scope/part-omission negatives passed; live evidence
remains pending.


A06 `heal.admin-stop-and-stale-token` now uses the same pre-registered 1025
external-object DX fixture on H1/H2. Cancellation requires actual running,
nonzero partial canonical progress and both repaired and still-missing targets.
The frozen candidate policy requires active stop acknowledgement, retained
aborted/cancelled partial responsibility, and exact HTTP 200/stopped
idempotent acknowledgements for repeat/stale stops of a durable terminal token.
Unknown tokens remain a separate exact 500/InternalError contract. Three complete physical snapshots with
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


F06 `heal.cancelled-task-restart` extends the same real partial cancellation
and write-quiescence proof with a graceful restart of every original H1/H2
unit. A read-only node action accepts only a canonical admitted task UUID and
three exact candidate journal names under the private run roots; it records
all four drives on every node, bounds terminal/intent to 64KiB and report to
8MiB, and rejects links, hardlinks, oversized or changing files. The actual
cancelled terminal and report need one matching physical owner, exact task and
prefix scope, no runnable intent anywhere, and the settled partial outcome and
valid sequence window. It never synthesizes a checkpoint or edits system
records. After same-root restart, every actual process must be new while its
unit/candidate/formats/objects remain original, all peers must be idle, old
status must still be cancelled, and the exact original journal bytes must be
preserved. A fresh forceStart task remains independent under old STATUS/STOP
and repairs only the remaining witnessed defects. Full physical/streaming/S3
proof remains mandatory. A06 also now preserves both its initial cancellation
and allowed monotonic in-flight report refinement before freezing the report
after actual drain; later quiet/restart/new-task checks cannot rewrite it.
Local journal-owner/schema/scope/cursor/read-boundary negatives passed; full
live evidence remains pending.

A05 `heal.admin-overlap-and-force-start` now dispatches all four source
branches on both H1 and H2, with an independent deployment/ACK ledger/fault
set for each instance. Two real prefix scopes contain 257 external objects
each; five missing shards per selected scope retain positive authority.
The actual process environment freezes `merge`: equivalent starts must return
the same token, parent/child requests must return exact 409/OperationAborted
with `overlapping_paths`, and disjoint requests must receive independent
tokens. Every control request needs a nonzero running bracket; a missed
window remains NOT_OBSERVED. forceStart must cancel the old task with partial
coverage, admit an independent replacement token, and resist old-token STOP.
Initial and settled cancellation reports are retained; exact old/new repair
counters, incarnations and diagnostic identities cannot leak or duplicate
responsibility. All 257 original item sequences are retained while running
and reconciled with the exact terminal outcome and retained suffix. The
completed-report byte budget can evict an early prefix below the 1024-item
count cap; an unobserved prefix still fails completeness. Both disjoint tasks
are sampled concurrently, and replacement drain samples are preserved. Three
live physical samples after actual all-peer drain must remain unchanged,
with the same processes. Complete same-role shard bytes, streaming checksums
and all-peer full S3 manifests remain mandatory before PASS. Local
scope/error/window/duplicate-repair/item-omission/cleanup-stop negatives
passed. Run `38199371461` retained FAIL with clean finalization: H2's first
three branches exposed the early-prefix accounting gap, H1 force-start had a
typed cancelled receipt counted as skipped, and H2 force-start contained a
real unknown receipt. Replaying the original running records verifies all
three 257-item primary ledgers; H1 cancellation now requires its exact typed
receipt count, while H2 unknown remains rejected. This diagnostic replay
does not change the original FAIL or claim a complete new live PASS.

F01 `heal.root-graceful-coordinator-restart` now dispatches both source
branches on H1/H2 with the same fixed DX physical/S3 oracles. The coordinator
comes from the unique admitted physical root intent, rather than a guessed
node alias. Actual nonzero task progress and both restored/pending target
shards must precede interruption. Linux pidfd binds pause, SIGTERM and resume
to the original process birth; the original run-owned unit retains actual
normal-exit-zero evidence. All peers retain their original process except
that coordinator. The exact schema-3 root intent stays on the same drive,
with original task/scope/options/priority/admission time and a finite
remaining timeout that cannot increase. Pending peer writes must settle
before same-unit/environment/volumes/candidate restart. The original token
must recover, report complete exact responsibility and repair the remaining
physical defects; no new START compensates for lost responsibility. Runnable
intent must retire on every drive, while completed terminal/report remain on
the original owner. A second coordinator restart preserves their bytes,
completed outcome and all user files and remains idle. This proves durable
admission replay, and makes no cursor-resume or record-disk failover claim.
Local owner/budget/process-birth/exit/old-token/journal negatives passed;
full live evidence remains pending.


B06 `heal.read-quorum-boundary` now dispatches all three source branches on
H1/H2 with three independent 5MiB+123-byte never-versioned ACK objects. Its
read and write quorums come from the recognized candidate artifact and actual
decoded geometry; the reviewed candidate lock policy counts pool host:port
clients, so four H2 hosts have exclusive quorum three, not nine disk locks.
The exact-R GET archives read-repair interference and never certifies manual
repair. The separate exact-R manual object must reconstruct every missing
original shard and return exact completed responsibility. F1 only isolates
payload files: all drives, metadata and locks remain online, so it cannot
represent insufficient writable-drive or lock quorum. That condition is
explicitly non-applicable here; B08 retains its independent real fault
requirements. The restored-W branch first proves the candidate's exact R-1
read refusal with no physical changes, then restores only W-(R-1) original
backups. Those sources never count as product repairs. A distinct manual
token must reconstruct all N-W remaining targets, followed by full physical,
streaming checksum and all-peer S3 checks. Live evidence is pending.

A08 `heal.admin-authorization` passed live in RustFS Actions run
`38069648003`, harness `0789743d439fd91b967c2d6fba85c06e29f844c5`. All five
source branches passed independently on H1 and H2; the canonical gate,
complete summary, cleanup and workflow all passed with the fixed candidate.
The prior A08 failures remain recorded. Thirty-seven Heal executors remain
to be migrated; automatic dispatch and local tests do not claim live PASS.


C06 `heal.stale-version-rejoin` now dispatches all three source branches on
H1/H2, with never-versioned overwrite and independent versioned historical
delete/marker fixtures. A private permission gate removes both DAC bypass
capabilities and activates only after the original candidate has started.
Same-process read/write EACCES proves that the unchanged member cannot
participate in the subsequent ACK transition. Every online metadata replica
must agree on the complete acknowledged namespace, the isolated member's
entire physical disk remains exact, and controls retain their original bytes.
The same disk UUID and set slot must rejoin without harness metadata writes
or restoring old-generation backups. An independent deep token must correct
the stale namespace; exact outcome counters, retained same-drive shards,
streaming checksums, deleted-version absence, current marker headers and
all-peer S3 manifests are required. Original payload nonces remain attached
to ACK versions after deletion so Range checks cannot silently renumber
history. The candidate's dangling grace is explicitly zero for this rejoin
profile; C10 retains the independent grace-policy requirements. Complete
cursor-bounded generation-selection logs are archived. Live evidence is pending.

B02 run `38079787495` retained FAIL and cleanup PASS on both H1/H2. The
readonly window produced `completed_with_errors` with one failed receipt,
zero repaired targets and `summary=stopped`, but omitted bucket incarnation.
The stopped summary is now interpreted only with explicit failed execution,
complete coverage and exact counters; missing incarnation still fails. The
permission setup previously chmod'ed the data root before service startup,
causing the candidate to exit with EACCES before injection could be witnessed.
Permission gates now retain an armed recovery marker during initial startup
and inject chmod only into the same live candidate with DAC bypass already
removed. This fixes the injection window without certifying the old run or
relaxing physical/refusal/responsibility assertions. A fresh run is required.


B01 `heal.disk-loss-and-repair` now dispatches both source branches on H1/H2
with the full 128-object DB manifest, including zero/inline/external objects.
The same-service permission gate must deny reads and writes on one exact
original disk, and signed read-only Admin info must independently identify
that exact formatted UUID/set slot/endpoint as offline while all other
members remain online. The double witness brackets every unique-key PUT ACK
and degraded GET. Original member files, controls and formats remain exact
through the offline window; all N-1 online new-key metadata replicas and
payload shards must form one protected ACK authority. Original-drive-return
admits one new key and offline-ack-repair admits six independent new keys.
The same disk returns without restoring backups; each ACK needs its own
manual task, complete repaired receipt, actual new target metadata/part,
unchanged donor shards, protected streaming checks and full all-peer S3
manifests. Ready/GET alone never prove repair. Live evidence is pending.

B06 run `38081025407` retained FAIL and cleanup PASS. Its exact-R GETs on
both H1/H2 returned correct full payloads, but the collector omitted
`resultUnknown=false` on successful responses and the boundary check rejected
them. Successful complete S3 responses now explicitly expose known results;
partial bodies still fail. Both exact-R manual branches and H2 restored-W
passed within that failed run. H1's R-1 read returned HTTP 500/InternalError
instead of the frozen 503/SlowDownRead contract; that failure remains visible.
No failed run is promoted to PASS.

C06 run `38081269532` retained FAIL and cleanup PASS before injection. Its
original ACK nonce was incorrectly compared as if it were an S3 property.
Inventory checks now validate the nonce as private collector state while
retaining exact version/marker/currentness/length/digest comparisons. A
regression check exercises nonce-bearing ACKs against real public inventory
shape, including wrong content and version negatives. Fresh live evidence
is required for both corrected collectors.


B03 `heal.disk-replacement-formatted` now dispatches both source branches on
H1/H2, with full DB and historical/marker DV fixtures. A stopped, owned adapter
moves exactly one complete private data root into same-mount quarantine and
creates a genuinely empty new root. Before/after whole-tree digest and original
inode/device preserve every old file, including system records; no shared
mount is detached. The actual service uses a private namespace with quarantine
masked and both DAC bypass capabilities removed. Same-process EACCES proves
that the original cannot supply rebuild bytes. The candidate must initialize
a distinct new disk UUID in the same original pool/set slot without replacing
another member or changing geometry. Every original target version, marker,
inline value and external part must rebuild through one exact deep task, with
truthful complete receipts/counters, original same-role hashes, protected
streaming checks and full all-peer S3 manifests. Missing/unchanged UUID or early
automatic repair cannot satisfy this manual case. Partial rename/mkdir intent
remains recoverable; archive drift retains the namespace. Live proof is pending.

B01 run `38081858808` retained FAIL and cleanup PASS. Both H1/H2 had real
same-service read/write EACCES, but the exact Admin disk never became explicitly
offline within the registered 90-second observation window. Degraded ACKs were
therefore not started. The missing double witness remains NOT_OBSERVED in the
failure evidence; ready, cached online state and an inaccessible directory do
not substitute for the required Admin offline proof.


B06 corrected run `38082447868` retained FAIL and cleanup PASS. Both exact-R
GET and independent manual branches now pass on H1/H2; all three H2 branches
pass. H1's R-1 GET still returns 500/InternalError rather than the frozen
503/SlowDownRead, so no complete canonical PASS is claimed.

C06 corrected run `38082227039` retained FAIL and cleanup PASS after proving
ACK authorities: full fleet restart allowed physical changes before the manual
token and the attribution check stopped execution. Rejoin now restores only
the exact active permission gate in the same live process. Two identical live
physical observations freeze the settled ACK authority; every peer's actual
process birth/unit/candidate/environment must remain unchanged, and the fault
must still exist immediately before the independent manual token. Startup
reconciliation cannot contribute to the manual proof. Fresh live evidence is
pending; the original attribution failures remain unchanged.

E04 journal evidence now reads the first bounded page after each exact cursor
using `journalctl -n +10000` and per-entry journal cursors. Full pages are drained
in order, with six pages per capture and a hard failure on incomplete or
nonadvancing windows. Raw pages remain evidence; only source-attribution targets
are retained in the scanner's in-memory index. The healthy checkpoint starts
after seed, baseline and the initial physical snapshot, so a fresh complete
scanner cycle must actually occur in the registered observation window. Earlier
log-budget failures remain FAIL; this harness change has no live PASS claim.

F05 `heal.full-cluster-restart` now has an H2 executor for all three source
branches. It seeds DB, historical DV/markers, completed multipart objects,
exact pending UploadIds/parts and a 1,025-object DX observation fixture on the
current four Heal peers. A root task must have physically repaired some but not
all injected shards before the original coordinator is paused. Every original
peer then exits with witnessed SIGTERM/normal code zero in a registered order.
The same task ID, physical intent owner, non-timeout options and remaining
budget must survive all units being stopped. Recovery uses no new root start.
Complete canonical counters, bounded exact diagnostics, durable terminal/report,
full same-role physical manifests, streaming shard checks and all-peer S3
inventories must agree. All four processes restart again after completion;
terminal records and user files must remain unchanged. Pending MPU listings
remain exact across both restarts and their original parts are then completed
and checked by full payload hash on every peer. Multi-bucket snapshots and
streaming verification bind each run-owned bucket explicitly. F05 has no live
PASS claim until that complete sequence succeeds.

C06 run `38083035055` retained FAIL and cleanup PASS: all three H1 branches
passed with the same live service rejoin, but H2 failed the older 10,000-line
journal bound. The paginated journal implementation must be validated in a fresh
run. B03 run `38083328049` retained FAIL and cleanup PASS on all H1/H2 DB/DV
instances: the candidate had no replacement `format.json` at the first physical
snapshot. The executor now observes a registered 90-second, read-only format
initialization window before requiring distinct new UUID/set-slot evidence;
service health still cannot certify replacement acceptance. Neither result
increases the complete live PASS count.

E06 `heal.replacement-auto-rebuild` now dispatches both H1/H2 DB/DV branches
using the same whole-root replacement isolation and complete shard oracles as
B03. Its isolated runtime enables only automatic disk replacement (10-second
interval), with scanner and MRF disabled. The current lab's private directory
roots explicitly use the candidate's directory-backed replacement fallback;
physical-device replacement readiness is not claimed. Every peer must expose
one fresh, stable generation bound to the exact target endpoint and set, with
completed state and a fresh verification timestamp. Missing/partial/unknown
status, a changed generation or divergent terminal records fail. No Admin heal
mutation or target S3 read occurs before full same-role physical reconstruction
and streaming verification. The old archive remains inaccessible to the service
and unchanged. A healthy service or empty queue cannot finish the case. This
executor still requires complete live evidence.

E04 `38083897141` and C06 `38084060749` retained FAIL/cleanup PASS because the
first paginated collector used journald's default JSON field-size limit, which
can omit a long MESSAGE. The collector now requests `--all`, decodes complete
UTF-8 byte-array messages and rejects missing fields; original failures remain.

F02/F03 now execute both H2 source branches using automatic replacement. The
unversioned observation fixture has 1,025 external objects; the versioned
fixture has 64 keys, 192 data versions and their delete markers, with a 45-minute
private-service lease. Only node1 enables automatic disk discovery; the other
three nodes retain enabled Heal with discovery disabled. Before interrupting
node4, fresh cursor-bounded logs must bind the original generation to accepted
`auto_heal` admission and exact erasure-set task start on one distinct
coordinator. Physical census must show both committed original same-role
external shards and pending data versions. The target is paused through its
exact MainPID/birth identity and the partial window is checked again. F02 then
requires witnessed SIGTERM/normal exit zero; F03 requires pidfd-bound SIGKILL,
signal 9 and a non-graceful systemd exit, with no platform power event. No other
peer process may change. Restart uses the same unit, environment, roots and new
disk UUID, reproves old-archive refusal and finishes the same automatic
generation. Complete physical/streaming and all-peer S3 proof remain mandatory.
A fast completed task, missing distinct coordinator, changed generation or
missed physical window cannot PASS. External target verification uses bounded
128-part batches to keep the DX run within its registered resource budget.

B03 bounded run `38084312163` retained FAIL/cleanup PASS in all four instances:
the candidate still did not initialize the replacement format within 90 seconds.
That explicit acceptance failure is not relabeled as a completed reconstruction.


E05 `heal.mrf-offline-write-replay` now has a dedicated H2 executor for all
three source branches: unique version PUT, exact historical-version DELETE,
and marker creation while the original fourth peer is stopped and its endpoint
refuses connections. Every ACK must have a fresh active durable responsibility
in the candidate's committed MRF snapshot and matching lifecycle companion.
The read-only observer opens only six exact snapshot/commit/lifecycle filenames
per private root. It validates full journal CRCs, both commit SHA-256 bindings,
owner/sequence, responsibility ID and bucket incarnation; it never enumerates
IAM or credential records, changes journals, or accepts operator risk.

After the same node and disk identities rejoin, fresh manager admission with
source `mrf`, an exact-version automatic object task, complete successful
responsibility and `repaired`/`verified_healthy`/`authoritatively_absent` must
agree with committed responsibility retirement. Missing/held/unknown results,
queue emptiness, copied backup bytes or a manual verifier cannot certify this
case. All 16 replicas, retained same-role bytes, deleted data directories,
streaming checks and complete all-peer S3 ACK inventories precede PASS. The
record/proof decoder is explicitly bound to candidate source
`072b57b37559fbe626aeda8abf61617fdcbdd926` and reviewed source file digests;
ordinary execution consumes package/tools without a RustFS source checkout.
E05 has no live PASS claim yet.

F05 run `38084904969` retained FAIL/cleanup PASS before any restart because its
combined DB/DV/DM/DX physical census exceeded the cursor resource profile. It
now registers a distinct bounded 24,576-file census with the original 2 GiB
per-drive free-space requirement and 45-minute service lease. E04
`38085241447` verified complete journal collection on the corrected `--all`
reader, then failed its healthy task's missing public pool/set responsibility
identity; cleanup passed. The original failed runs remain unchanged.


C06 `heal.stale-version-rejoin` passed complete H1/H2 unversioned overwrite,
historical-version deletion and marker creation in run `38085554143` at harness
`8d78ff32188412644a55776be7a794024a4f5614`. All six applicable instances have
PASS and cleanup PASS; downloaded canonical evidence passed the independent
selected-case gate. This raises the full live PASS total to 44. Older incomplete
journal-window runs remain FAIL.


B04 `heal.disk-replacement-unformatted` now dispatches both H1/H2 source
branches with independent 128-object ACK fixtures. A second released-process
H1 service generates the actual foreign format; the read-only exporter retains
its exact bytes, digest, candidate binary and cluster/member identities before
complete donor cleanup. The replacement adapter copies only those verified
format bytes into the witnessed empty run-owned root, rejects reused original
identities, and never manufactures a disk UUID or edits the old archive.

Blank initialization must expose the candidate's new UUID in its exact original
slot, complete the same automatic replacement generation, reconstruct every
original same-role shard, pass streaming verification and preserve all peer
ACK inventories. Refusal instead needs the exact Admin slot to remain explicitly
offline/unformatted/corrupt throughout a registered 90-second window, with all
other UUIDs/formats/user files unchanged and no target user writes. Foreign
format bytes must stay exact. The negative branch then retains the rejected
scene and restores the original archive only for recovery; it explicitly makes
no product-rebuild claim. Complete all-peer S3 and physical checks are still
required. B04 has no live PASS claim yet.

E06 `38085937937` retained FAIL/cleanup PASS in every H1/H2 DB/DV instance.
Three instances generated a format without a distinct replacement UUID; the
H2 unversioned instance instead exposed partial/unknown/incompatible cluster
replacement status. Neither directory-root fallback nor a format file alone
certifies actual replacement readiness or responsibility.


The cursor resource budget now includes the candidate's `part.N.integrity.*`
sidecars: 1,025 external objects on four roots already require at least 12,300
files. The explicit limit is 16,384 and the registered DX budget reads the same
configuration. F02 `38086220066` and F03 `38086565751` retain FAIL/cleanup PASS:
their DB branches exceeded the old 12,288-file allowance; the DV branches
failed to expose a distinct replacement UUID. Budget correction permits fresh
verification and does not change these original results.

E05 `38086919036` retained FAIL/cleanup PASS on all three branches. PUT/marker
reached durable ACK binding but the replay caller passed an unsupported label
to `JournalWindow.capture`; the actual shared interface and replay observer
are now covered together by a regression test. Exact historical deletion
failed to expose committed durable responsibility within the registered
30-second persistence deadline. That failure remains; no substitute manual
mutation or inferred queue record is accepted.

J06 `heal.performance-abba` now dispatches three complete H2 ABBA groups on
the same candidate, rc, four nodes, original roots and disk UUIDs. A frozen
protocol sets 30-second phases, two workers, four requests per second, a 1:1
GET/PUT ratio and the same 16 logical 64KiB load objects. Every scheduled request
has durable intent/receipt evidence; unknowns, failed samples and missed
admission deadlines remain in both error and latency denominators. Each phase
restarts the same services and uses the same fixed warmup without changing host
caches. Every A phase proves idle throughout and unchanged repair files. Each
B phase injects the same 17 missing data/parity shards in an independent
1,025-object repair bucket and binds one actual task's partial running progress
to both foreground operations. No observed overlap means NOT_OBSERVED and a
complete-case failure.

Exact owned-process birth, boot and settings bind read-only CPU, logical/storage
IO, RSS and FD samples. Candidate completion, actual reconstructed shard bytes,
streaming verification and full peer S3 manifests must agree. Repair throughput
uses physical target bytes, separately from logical S3 throughput. Frozen gates
require zero errors, GET/PUT P95 <= 5s and P99 <= 10s, paired B/A ratios <= 3,
positive verified repair throughput and bounded resources. All twelve phases
and every paired difference are reported; no phase may be omitted or reordered
and functional success cannot substitute for performance acceptance. The first
candidate run expired its 45-minute fault lease before group 3 phase 3; its
physical failure scene showed no applied fault and the retained namespace was
later independently finalized. The executor now binds a 120-minute lease for
the full evidence window. J06 has no live PASS claim yet and needs a fresh
candidate run.


F05 `38087158034` passed the expanded physical census and reached the real
all-peer graceful exit, then retained FAIL/cleanup PASS when the original
process exited between systemd MainPID and `/proc/PID/stat` reads. The node
observer now refreshes the complete service observation up to three times
only for disappearing `/proc/PID` entries. It never infers PID zero from
absence, accepts arbitrary file loss, or relaxes exact normal exit code zero,
service identity and pidfd birth checks. A fresh run is required.


B04 `38087657754` retained overall FAIL/cleanup PASS. H1 foreign-format passed
its complete structured refusal window, unchanged original format/controls,
rejected foreign bytes and all 128 ACK/S3 checks, with archive restoration
explicitly excluded from product repair. Both blank branches lacked a distinct
new candidate-generated disk UUID. H2 foreign-format did not make every bound
peer ready; a startup failure alone is not relabeled as structured safe refusal.

A02 `heal.admin-scope-isolation` now dispatches all five selectors with fresh
H1/H2 fixtures, two owned buckets and exact `a/`, `ab/`, `b/`, nested, Unicode
and literal encoded-slash keys. The candidate-bound H2 runtime preserves the
current four nodes and sixteen independent mounts, explicitly configures two
8-drive sets and interleaves original endpoints so each set spans all four
hosts. Actual format UUIDs, complete set membership, metadata distribution and
same-role shard identities bind every ACK to its physical set. Existing
single-set executors retain their strict default and reject this layout unless
it is explicitly requested. No format, UUID or second control set is invented.
H1 retains its original four-drive set and records the physical applicability
boundary for a nonexistent unselected set.

Root covers the entire owned service manifest; bucket, exact object and `a/`
prefix selectors preserve other buckets, adjacent keys/prefixes and control
faults; pool/set selectors require real selected-set reconstruction and an
unchanged missing shard in the other actual set. The complete responsibility
set and canonical counters must match all selected ACKs, including healthy
objects. On this exact candidate, unrestricted request selectors may remain
null in raw receipts; independent complete physical mapping resolves their
scope in separate evidence, without rewriting public fields or weakening
shared validators. Explicit selectors require exact integer receipt scope;
partial nulls, wrong sets, mixed incarnations, duplicates and unknowns fail.

Full physical comparison and selected streaming integrity precede any target
GET or unselected-control backup rescue. Rescue cannot certify product repair.
Both complete S3 manifests through every peer and a final unchanged physical
scene are required. A02 has no live PASS claim yet. Ten scope/topology/receipt/
control negatives join the focused local checks; 226 checks now pass.


D11 now dispatches all four source branches separately on H1/H2 (eight fresh
instances). Its dedicated node backend derives only two owned bucket-config
object paths and their incarnation controls. It never widens the general
user-object fault allowlist, walks IAM records, or prints bucket-policy/inline
payload bytes from physical metadata. The candidate startup persists a non-sensitive
`config/iam/format.json` object even in a fresh root-only service. Its exact
physical files and streaming proofs are read-only controls before and after
execution; mutation never targets that object. The six exact sensitive IAM
subtrees are checked for absence without traversal or payload reads. The reviewed
IAM manager source hash is bound alongside the bucket/XL2 contracts. Exact system object geometry
binds each shard index to the original disk UUID/set slot; current inline or
external bytes require candidate-bound streaming verification.

The fixture preserves Enabled versioning, bucket-only policy, tags, an Object
Lock default set after fixture writes, and all acknowledged historical business
versions. One config-envelope replica is missing or truncated for positive
recovery; the candidate's startup/config reads and explicit bucket Heal are
recorded as allowed mechanisms. Every original system file must be restored
byte-for-byte before backup rescue or business GET, while incarnation/control
records, business files and formats remain unchanged. The unknown-record
negative changes only outer XL2 major 1 to 2 on the exact primary config
replicas; it claims no inner msgpack schema coverage. It requires structured
public refusal, zero successful repair responsibility, preserved unknown
bytes and healthy control-bucket APIs. A startup/readiness failure alone is
not safe-refusal evidence. Rescue archives remain separate from product
repair, and an unrecognized config rewrite retains the stopped namespace.
D11 still requires live evidence.

F02 corrected-budget run `38088240693` retained FAIL/cleanup PASS in both H2
DB/DV branches: neither reached partial-rebuild interruption because the
candidate did not expose a distinct new replacement UUID. F03 corrected-budget
run `38088777413` also retained FAIL/cleanup PASS in both H2 DB/DV branches:
replacement source status was partial, unknown or incompatible. The bounded
file census no longer prevented either run; these product/evidence failures
are not converted into successful restart coverage.


The optional native-regression tools job builds the unmodified candidate's
optimized ecstore test executable on hosted Linux, retains Cargo/lock/toolchain
identities, enumerates actual full test names, and runs each selected name with
`--exact` in a fresh process and a fixed 120-second deadline. It currently
collects two real read-repair/namespace barrier regressions and two uncontrolled
overwrite/delete concurrency regressions. Zero-test filters, ignored tests,
foreign source/collector identities and other-name success are rejected. The
four regression verdicts have their own archive and never increment a fleet or
canonical-case PASS count. Existing tests assert captured lock events internally;
the collector does not fabricate emitted barrier logs. Missing critical-point
fixtures, Admin nolock semantics and required fleet overlap remain explicit gaps.
The current implementation has 310 focused Heal/catalog checks, including the
Linux-only ownership/mount/recovery checks. Hosted native compilation and all
four exact regressions completed in run `38096433036`; their independently
verified identities and limits are recorded below.


J03 corrected-budget run `38089238767` passed every applicable within-window,
past-retention-gap and terminal-truncation branch on H1/H2. The cross-node
branch is explicitly NOT_APPLICABLE on single-node H1 and passed on H2. It retained complete DX responsibility, real running-status windows,
physical/streaming integrity, all-peer S3 evidence, cleanup PASS and the
independent canonical selected-case gate. This increases complete live PASS
to 45/158; branch-only successes and native regressions remain outside that
count. A06/F06/F01 and A02/E05/F05/J06/D11 queues continue serially on the
current auto-testing fleet. The optional native build uses the existing
Functional Chain tools-only driver on hosted Linux, because the reusable
workflow is still in a draft PR and cannot be dispatched directly from the
default branch.


B08 now has an automatic current-fleet executor for both source branches.
The write-negative branch measures every original metadata read and root write
inside the exact candidate process mount namespace. Precisely N-W+1 private
roots return EROFS, including the missing payload target; all N metadata
members and all lock clients remain online. The H2 lock-negative branch
isolates two exact run-owned peer processes, witnesses their exited MainPIDs
and refused endpoints, and keeps R metadata/payload sources and shared-lock
quorum while losing exclusive-lock and object-write quorum. It does not claim
host network partition or physical disk loss. H1 uses an in-process local
manager; its distributed peer-loss branch is explicitly NOT_APPLICABLE with
the reviewed candidate locking policy, while its write-negative branch remains
mandatory. Both original branch IDs and H1/H2 contracts are retained.

Every negative scene is frozen before removing the fault. A single unique-key
PUT has no automatic retry; all unknown outcomes and success ACKs are queried
on every recovered peer. Unresolved outcomes retain the stopped namespace and
fail cleanup. The negative Heal must preserve complete failed/deferred exact
responsibility with zero repaired counters and no changed physical control.
After gate removal/source return, an independent task must reconstruct the
original missing target before any target GET or backup rescue. Streaming
verification, all-peer complete S3 manifests and clean finalization remain
required. Ten boundary, identity, mount, cleanup and unknown-ACK negatives join
the focused checks. B08 has no live PASS claim yet.

A06 run `38090151509` retained FAIL/cleanup PASS on H1/H2 after its repeat-stop
assertion. It exposed a harness contract defect: the reviewed manager's
`root_recovery::cancel_pending` treats a retained terminal record as an
idempotent stop, so the public response is HTTP 200/stopped, rather than the
unknown-token HTTP 500 assumed by the harness. The original result remains
unchanged. Fixing the candidate-bound assertion and repeating the complete
canonical run are required; the observed acknowledgement alone is not PASS.


The A06/F06 durable-retired-token assertion is corrected against all four
reviewed source hashes, including `root_recovery::cancel_pending`. It now
requires the exact signed old-token request and the candidate's HTTP 200,
stopped acknowledgement shape. Its default acknowledgement settings and new
response timestamp are not mistaken for task identity or original task options.
The old settled canonical outcome must remain byte-equivalent immediately
after both repeat and stale stops, and the new task must stay running across
the stale request. Unknown-token queries and forceStart-retired tokens without
a retained durable terminal keep their independent exact 500/InternalError
checks. Both archived A06 acknowledgement samples satisfy the corrected shape;
that replay is not fresh live acceptance. Complete A06/F06 reruns remain required.


C09 `heal.foreign-format-and-unknown-schema` dispatches all four canonical
branches on H1/H2. An isolated second released-process deployment exports a
real foreign format. The disk branch replaces only `xl.this` with its witnessed
donor UUID; the set branch copies the exact donor bytes, including its real
four-member set when the target has sixteen members. This topology difference
is retained explicitly. The future-format branch changes only the top-level
V1 semantic version to `999`. Only one empty, owned replacement root is
modified, while its complete original root is quarantined and inaccessible to
the candidate. Shared formats and IAM are outside the mutation scope.

A registered 90-second window requires exact Admin slot/endpoint refusal and
stable original peer identities, followed by deep Heal and target GET. Startup
failure alone cannot earn PASS. Manual Heal may verify the surviving object or
report its complete failure, but must never claim reconstruction of the refused
member. All surviving files/formats and the injected bytes stay frozen.

The object branch changes only the reviewed XL2 envelope's little-endian major
from 1 to 2 on every member of one ACKed target. It does not fabricate a future
inner msgpack schema. Per-disk metadata hashes bind each actual decoder refusal;
all peer GETs must return a structured server refusal, and deep Heal must retain
complete failed responsibility without a missing/healthy/repaired claim. All
payloads, control objects and formats remain exact. Only after the negative
proof may original bytes/identity be rescued. All 128 ACKs, every external shard
with streaming checksums, and full S3 inventories through every peer are checked
again. Backup rescue never earns product-repair credit; failed recovery retains
the namespace and stops subsequent fleet use. C09 still requires fresh live
evidence. Eleven targeted oracle/guard tests are added; its actual Linux
FD/mount guard test runs in CI, not as a simulated macOS syscall claim.


F01 `heal.root-graceful-coordinator-restart` run `38091619866` retained
canonical FAIL with cleanup PASS. H1 passed both branches; H2 exceeded the
collector's 2 MiB response limit during original-token recovery. The reviewed
candidate `crates/heal/src/heal/channel.rs` permits an 8 MiB status payload
(source SHA-256 `7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0`).
The collector now uses that explicit 8 MiB bound, records complete byte counts
and still rejects overflow or Content-Length mismatch. A boundary regression
covers a complete 8 MiB JSON response, one-byte overflow and truncated bodies.
No F01 canonical PASS is claimed until a fresh complete rerun.

A06 run `38092515125` retained FAIL/cleanup PASS on both H1/H2 because the
collector represents signed query pairs as Python tuples in memory and as
JSON arrays on disk. The exact durable-stop validator incorrectly accepted
only the latter. It now normalizes either pair representation while retaining
the exact pair count/order/token, signed POST, empty body, response shape and
old/new responsibility assertions. Tests reject duplicated tokens and mapping
queries. All six CI lanes passed at `62d0ca9`; fresh complete A06/F06 runs are
still required. Earlier FAIL results are preserved.

D08 `heal.encrypted-objects` dispatches SSE-S3, SSE-KMS, SSE-C and encrypted
historical versions on both H1/H2. It configures one fresh AES-256 static KMS
key across the run-owned stopped services, passes material through SSH stdin
and retains it only in a private 0600 file inside each run namespace. Every
snapshot attests the file/configuration identity and the actual process
settings, including explicit per-key authorization enforcement. Shared KMS
resources are not modified and secret material is excluded from evidence.

Each branch independently ACKs sixteen external encrypted objects; the
versioned branch ACKs three distinct generations per key. It removes a data
shard and corrupts a parity shard, including historical and current versions,
then requires a manual deep token with complete exact responsibility. Before
any decrypted target GET, the original same-role ciphertext hashes/lengths and
streaming integrity must be reconstructed. An encrypted-only mapper preserves
the distinct plaintext actualSize and ciphertext part size without relaxing
the ordinary plaintext mapper. Every peer then verifies the entire plaintext
ACK/version/tag/property inventory and re-freezes the ciphertext scene.

SSE-C missing/wrong keys must produce the reviewed 400/InvalidRequest refusal.
SSE-KMS principals have the same working S3 GET rights to an independently
verified plaintext control, but no decrypt grant or only an unrelated key
grant; the encrypted GET must return 403/AccessDenied before and after repair.
SSE-S3 records that its service-managed key has no caller decryption-key
argument. Failed IAM cleanup retains a DIRTY stopped namespace. Nine targeted
geometry, authority, version, redaction and Linux file-ownership tests pass;
D08 still requires fresh complete live evidence and has no live PASS claim.


A02 `38094094085` passed every applicable H1/H2 root, bucket, object, prefix,
recursive and actual two-set selector branch. H1 keeps the source's selector
inapplicability explicit; H2 tests the actual independent sets. Its complete
physical/streaming and all-peer ACK manifests, cleanup PASS and independent
canonical gate increase the complete live total to 46/158. F06 `38093199055`
retained FAIL/cleanup PASS on the already queued old harness; its durable-stop
query assertion is fixed at `62d0ca9` and remains queued for a fresh run.
B08 `38093907057` retained FAIL/cleanup PASS: H1 lacks the exact incarnation
receipt, while H2's actual default layout cannot preserve R with the selected
write/lock-loss plan. No layout or missing identity is assumed sufficient.

F04 `heal.coordinator-process-crash` now dispatches its three separate source
mechanisms on actual H2. It records a candidate-bound persistence support table
and only sends SIGKILL through Linux pidfd after freezing the unique original
coordinator at nonzero and unfinished physical progress. Target and other peer
process birth identities must remain unchanged throughout recovery. No host or
platform restart is issued, and an unobserved window cannot pass.

The root branch ACKs the full DX fixture, places missing original data shards
outside the coordinator, freezes the exact durable admission owner and requires
the original token to replay with complete responsibility. SIGKILL must leave
the original intent bytes unchanged; it makes no graceful elapsed-time or cursor
checkpoint claim. Repeated terminal restart, original record disk, full physical
hashes/streaming and all-peer S3 inventories remain mandatory.

The MRF branch ACKs sixty-four independent never-versioned DB writes while one
original node is offline. Its isolated profile enables only MRF, with one worker,
one task per set and a replay batch of one. Actual process settings and the
configuration source digest are retained. Fresh accepted MRF task/start logs,
exact version/null representation and a partial target census bind the paused
coordinator to a still-committed original ACK responsibility. The complete
CRC/commit/lifecycle checkpoint remains byte-identical across the crash.
The lost volatile token must return its explicit unknown-token error; a separate
new automatic task must prove the interrupted ACK, while every ACK's original
responsibility retires with a verified receipt. Automatic rescan success is never
reported as original-token recovery. The final census, all surviving original
shards, streaming checks and S3 inventories gate the branch.

The replacement branch independently ACKs 1025 external objects and preserves
an inaccessible original drive archive. Its coordinator is distinct from the
live target. An exact-name read-only journal captures the schema-seven sealed
intent for the original generation, endpoint, physical device and filesystem
identity before and after SIGKILL. The original generation must finish on every
peer after the same coordinator unit restarts, with the original replacement
UUID/set slot, full same-role shard hashes, streaming and ACK inventories.
No manual replacement compensation or backup rescue earns repair credit.

Eleven focused F04 tests cover process continuity/exit, exact root intent,
replacement seals/name/identity and filesystem guards, partial MRF authority,
source attribution, bounded configuration and mandatory three-mechanism scoring.
F04 has no live PASS claim yet. The source-bound MRF log reader also accepts the
reviewed Rust Option debug spelling without merging different version identities;
existing E05 receipts and every original branch remain independently required.


B08's read-only plan now measures writable drives independently of readable
sources, including the candidate's actual default H2 R=12/W=12 geometry. Five
owned read-only roots leave fifteen payload sources and all sixteen metadata
authorities readable while only eleven roots remain writable. Numerical R<W
is unnecessary for that distinct write-admission fault. The H2 distributed
lock-loss branch now seeds a fresh explicitly configured EC:8 run, and attests
that setting in every actual process before PUT. Its decoded eight-data/eight-
parity authority must still preserve R=8 after two exact peer stops while losing
W=9 and exclusive distributed lock quorum. No existing object's parity is
rewritten, and ordinary profiles keep their own observed layout. The storage
configuration source hash is archived. Two regression tests cover the default
R=W read-only plan and fresh balanced lock fixture. H1's missing incarnation
receipt remains a product/evidence FAIL; the identity oracle is not relaxed.
A fresh full B08 run is required and no new live PASS is claimed.

B10 `heal.disk-and-node-flapping` now executes all three H2 branches on the
current auto-testing nodes. Each branch preregisters three cycles, 16/16/64
unique degraded PUT ACKs, four foreground workers, exact fault/persistence/
replay windows, a total deadline, queue/task/retry limits and CPU/FD/RSS
budgets. The foreground ACK ledger records concurrent unique PUT, HEAD,
control GET and LIST without retrying uncertain writes. Live drive rearming
requires the exact original process birth, an independent service mount
namespace and removal of DAC bypass from all capability sets. Every cycle
has service read/write refusal plus exact-member Admin offline/online
witnesses, or an owned-unit stop plus an exact connection refusal. Original
host, candidate, runtime, disk UUID and set-slot identities remain bound.

The third `interrupt-rebuild-again` cycle additionally requires an exact
running MRF task and both completed and incomplete physical ACK targets;
it loses the original node again, reproves incomplete physical progress
and requires the original pending durable responsibility IDs/incarnations
and nondecreasing owner sequences to survive. Every cycle then needs actual
MRF admission/start/terminal proof, original responsibility retirement,
complete physical cohorts and streaming integrity before target GET. The
final gate rechecks every peer's full ACK inventory and unchanged physical
results. Manual compensation and backups cannot satisfy convergence. This
executor has no canonical live PASS yet; 31 Heal executors remain.

E05 `38094551700` retains FAIL/cleanup PASS at `6354298`. Its PUT and marker
branches exhausted the in-memory journal budget with unrelated HTTP/HTTP2
frame debug events before they could prove complete replay. Its historical
version DELETE branch had no exact committed durable MRF obligation within
the registered persistence window; the final checkpoint held only unrelated
system-bucket records, which cannot discharge the owned ACKs. The corrected
MRF profiles use the candidate's reviewed tracing filter grammar to enable
only Heal manager/task/MRF debug over an info baseline, attest both
`RUST_LOG` and `RUSTFS_OBS_LOGGER_LEVEL`, and retain complete raw journal
pages while selecting the required attribution targets in memory. The
60,000-event/page budgets and exact durable responsibility assertions stay
in force. All three E05 branches still require a fresh complete live run.


I02 `heal.enospc-inodes-and-quota` now executes all three source branches
separately on H1 and H2. The two capacity branches allocate exactly one
128 MiB ext4 image with at most 1,024 inodes on each original independent
backing drive. Images, filesystem UUIDs, mounts and undo paths are registered
before allocation; only these bounded regular files are formatted. Each
owned service binds the allocated data roots in a private mount namespace.
The bounded volumes do not add physical drives to the H1/H2 topology.

Block or inode exhaustion fills only the dedicated padding directory and
must produce actual ENOSPC in both the filler and a probe with the original
candidate process birth, mount namespace and effective UID/GID. The original
shared drives must retain at least 1 GiB of free space. Missing DB and completed
MPU parts remain unresolved during the exhausted interval; all other business
parts and metadata remain frozen. Removing only this run's filler permits
independent manual repair, exact responsibility reconciliation, same-role
streaming bitrot verification and complete all-peer S3/MPU ACK checks.

The hard-quota branch uses only the owned bucket and the candidate's reviewed
1 MiB and 256 MiB limits. It requires the exact InvalidRequest quota refusal
for a single new logical PUT, preserves existing object/MPU ACKs and verifies
existing-object shard repair under the quota. Quota is logical object
admission, so a successful repair of an existing shard is not a new logical
PUT. Both capacity modes also require a distinct, acknowledged PUT after
release. Every uncertain negative or released-capacity PUT is resolved across
all peers without retries; unresolved receipts or recovery drift retain a
DIRTY namespace. File-backup rescue never earns product repair credit.

The prior B10 commit omitted its top-level dispatcher arm even though the
Heal sub-dispatcher was present. The arm is corrected together with I02. A
catalog-wide automatic-dispatch contract now checks that all 54 registered
Heal IDs reach candidate preflight through the top-level entry. The B10 live
queue has not started and must use the corrected immutable harness revision.
This change leaves 30 Heal executors to implement and makes no new canonical
live PASS claim. The focused suite has 310 tests with five platform skips;
the catalog still preserves all 158 exact source IDs and hashes, and the
84/301/532 source matrix is unchanged.

F05 `38095216675` and J06 `38095795125` retain FAIL/cleanup PASS at the old
harness revision because Admin evidence exceeded its previous response
budget. J06 completed only the first no-heal phase, which cannot certify an
ABBA group. The queued fresh F05/J06 runs use the reviewed 8 MiB response
contract. D11 `38096389824` also retains FAIL/cleanup PASS for all eight
H1/H2 instances: versioning/lock configuration byte restoration did not match
the frozen original; policy/tag record decoding failed outside its witnessed
mutation; unknown-record startup did not become ready. No D11 PASS or
successful unknown-record refusal is claimed.

Native tools-only run `38096433036` binds driver
`ae9b5cbabfeaff440d9b8ee0b2fe805f17aff5b3` and auto-testing
`acdd69218406b0596d79b6aab15481f8271e35c6`. Its candidate resolver and tool/native
jobs use hosted ubuntu-latest with independent tools-only concurrency and
empty fleet credentials. The workflow, metadata inspector build, optimized
native compilation and all four exact-name regression processes completed
successfully. Independent verification matched the downloaded executable
SHA-256 `0c9e005ae877f741a62100f0d70af6c2256d1078d546ac97e963d6979aa05bc1`,
candidate source tree `8071fb0a9e5353eb7d3fa58c8a58c8622473f2bd`, Cargo.lock
SHA-256 `d8f7bc416ba67d19873d34e9389e5aa823b22d7d7552706a5a810cee26f6eda2`
and collector SHA-256
`b095d4e1cdb26be9d16cc82f589e29137ec84666e4675a9acc7dfbed761a0f28`.
Each original listing and result log proves exactly one non-ignored selected
test ran and passed. The final-rename and lock-owner tests contain actual
deterministic candidate assertions; overwrite and version-delete use
uncontrolled concurrency and do not prove deterministic commit fences.
Admin nolock, PUT cleanup-tail, other critical points and fleet overlap remain
unproven. These four native results do not increase the 46/158 complete
canonical live PASS count. This exact audited hosted run is excluded from
the local fleet-busy classifier; unknown active runs still block dispatch.

F01 `38096950043` passed both original-root graceful-interruption and same-record
disk-resume branches on H1/H2 at immutable harness
`21b4ff9656710867dbcbaced08e1804110c6bdf7`. Independent artifact inspection and
the canonical selected-case gate verified every branch, cleanup PASS, actual
partial physical repair, original task/record ownership, non-refunded timeout,
unchanged peer processes and complete physical/streaming/all-peer S3 evidence.
Complete canonical live PASS is now 47/158. The older F01 response-budget FAIL
remains in its original archive.

I03 `heal.network-partition-and-lock-quorum` now has all five H2 source branches:
minority partition, read boundary, write boundary, lock boundary and rejoin.
Each uses a fresh run-owned four-node/sixteen-member EC:8 service and an exact
versioned ACK fixture. The actual R=8/W=9 geometry and shared/exclusive lock
quorums are frozen independently. A two/two split proves the exact readable
boundary and unavailable exclusive lock quorum; a three/one split retains
three lock clients while private read-only gates leave precisely eight writable
roots. Historical and damaged versions use their own available-shard counts.

All four independent 20-minute expiry timers and root-owned recovery intents
precede the first atomic nftables transaction. Each table matches only the
registered peer IPv4 addresses and TCP source/destination port 9000; no shared
table is flushed. Every peer direction is probed with actual drop-counter
increments, and management TCP 22 remains reachable. Each group independently
executes exact-version GET, one unique PUT without retries, Heal and partial
background-coverage checks. Every original version/control must remain intact;
unknown writes require all-peer resolution after network recovery. Owned rules
must be removed before independent repair, exact version responsibility,
same-role streaming checks and complete S3 manifests. A distinct recovered PUT
must redeem its actual VersionId, bytes, metadata and tags on every peer.
Foreign rules, lost expiry guards, unknown ACKs or failed recovery retain DIRTY
evidence. No I03 live PASS is claimed. The implementation now has 129 automatic
executors (74 non-Heal, 55 Heal); 29 Heal executors remain. The focused suite
contains 333 tests, including the real Linux private-network nftables gate;
six platform-specific tests are skipped locally on macOS.

All six I03 checks passed at `7698230c53184168d0174e21e4aa34c0ca158aad`.
The Linux gate installed, inspected and removed real kernel rules inside a
private network namespace and rejected an injected foreign rule. The adapter
uses nft's canonical IP/TCP payload expressions, retains exact address/port/drop
checks, and refuses aliases with an unguarded IPv6 route before mutation.
The serial I03 live plan binds this immutable SHA and the existing candidate
package/tool runs; it has no live verdict yet. Current RustFS CI has runner
shutdown failures in Workspace Test and Lint, Connect Short Credential Boundary
and S3 Implemented Tests; their original job logs show shutdown/cancellation,
not a completed passing check. The two optional workflow files remain the only
RustFS changes.


H06 `heal.multi-pool-target-isolation` now dispatches all three original H5
branches: selected pool, selected set and unselected control fault. It reuses
the four current Heal nodes and sixteen original independent mounts. Two
separate endpoint-expansion arguments create two actual pools with two
four-peer sets each; the candidate layout/parser source binds the argument
order and exact public pool cmdlines. The runtime explicitly uses set width
four and EC:2. Full format/UUID/set mappings and decoded object geometry must
prove actual cross-node EC 2+2 before any fault.

Bounded DB and three-version DV fixtures must populate all four observed
pool/set combinations. Every acknowledged version is mapped without an
assumed placement hash or internal PUT selector. A measured stop of one owned
peer must leave three physical slots per set and redeem every original
version from all three survivors; this is process loss, not platform power
loss. Every selected and control scope contains a distinct DB fault and a
historical DV fault. The control branch also gates one exact unselected root
read-only inside the candidate service namespace, witnessing EROFS before
and after selected Heal without remounting the host's shared drive.

Exact selected version responsibility, counters, settings, pool/set/disk
identities, public pool capacity and logical quota policy are checked.
Unselected missing parts must remain missing before any target GET or rescue.
Only after that physical proof may independent control tasks repair them.
Complete physical, same-role streaming and all-peer version/metadata/tag
manifests are required. Uncertain seed requests are registered before calls,
retain durable request/ACK files, stop further mutation and require exact
all-peer resolution; unresolved bucket mutations or ACK/version drift retain
DIRTY namespaces and block later branches. No H06 live PASS is claimed.

The current implementation has 130 automatic executors (74 non-Heal, 56 Heal);
28 Heal executors remain. The complete local validation suite ran 352 tests
successfully with six Linux-only skips on macOS. Catalog source IDs/hashes,
84/301/532 Heal matrix, Python syntax, Bash syntax, ShellCheck error checks,
scoped actionlint and whitespace checks passed. The pre-H06 documentation
HEAD `9b93e7570db7b4af39608ba4876e0852952261d9` has all six CI checks passing;
H06 still needs its own CI and serial live run.

C09 run `38098069878` completed FAIL with cleanup PASS. All eight H1/H2
branches retain failures: some injected foreign/future formats prevented
readiness or repair admission, while other negative tasks did not satisfy
failed-execution/incomplete-responsibility evidence. D08 run `38099278528`
completed FAIL with cleanup PASS: SSE-S3, SSE-KMS and encrypted-versioned
branches passed on both topologies, but SSE-C ACK/GET classification failed
on both. Neither run increases the 47/158 complete canonical live PASS count.
The RustFS aggregate Test and Lint check also failed because its three
runner-shutdown lanes failed; its final summary preserves those exact failures.


H06 read-only pool observations now have an explicit Probe allowlist: signed
GET only, four-node H5 binding, exact pool list or numeric pool 0/1 by-id
selectors, empty body and no duplicate/extra queries. All six CI checks passed
at `f63f3ece5f78670b6880a30390802dd658ebfb6d`. The pending H06 serial live plan
now binds that corrected immutable revision; the active plan was unchanged.

A06 run `38099706345` passed all four stop/stale-token branches on H1/H2 using
the source's DX window fixture, at exact harness
`f248cb63d032cfc1658d32c508bce745f96b885f`. Independently inspected source branch
sets, topology coverage, nonzero interrupted progress, cancellation/physical
quietness, fresh task ownership and old-token behavior, complete recovered
physical/S3 manifests and cleanup all passed. The independent canonical gate
passed again. Complete canonical live PASS is now 48/158. Original A06 failures
remain archived and are not overwritten by this fresh successful run.

The native collector additionally selects the candidate's two exact C07
conflicting-authority/preservation regressions, with reviewed original source
assertions. These are controlled guard decisions, not emitted concurrent
runtime-pause fences or fleet PASS. The corrected hosted tools-only run
`38100407691` binds collector `6f4901573967c53b88239af361c1ab543f44fd4d` and is
still building native evidence. Earlier dispatch `38100369019` used an
incorrect complete collector revision and was cancelled; it is not a success
or native evidence source. C07 fleet migration is still in progress and does
not change the 130 automatic executors / 28 remaining Heal executor count.

C07 now has all three registered source branches on H1/H2. Its isolated EC:1
profile establishes actual 3+1/R3 and 15+1/R15 sets; each exact half-generation
group is below its observed read quorum. Full A/B original-member trees are
archived while the owned units are stopped, then replayed through descriptor
relative operations with durable rename intents. The versioned branch creates
three distinct acknowledged generations before capturing A and explicitly
deletes prior versions so hidden historical metadata cannot create an authority
quorum. The unversioned branch tests distinct physical generations as in the
source gate; it does not claim a bucket-incarnation split.

The complete census counts every live identity, binds original format UUID,
slot, role and geometry, and compares every chosen-generation byte. Every peer
must return the exact candidate-derived negative quorum GET error; a Heal task
must either receive the same typed admission refusal or prove explicit failed
or untraversable responsibility. Partial negative coverage remains partial,
and all peers have one 30-second budget to converge on the original path/token
and exact outcome. Original mixed/control bytes must remain unchanged before
fixture recovery. Only the complete last acknowledged B generation is rescued;
streaming bitrot and full all-peer S3 manifests then prove that ACK. Unknown
mutation receipts or partial recovery retain DIRTY namespaces.

The driver downloads C07's native artifact from the same pinned successful
tools attempt, verifies original source/tree/Cargo.lock/collector/executable
identities and all six exact test listings/results before fleet deployment,
and maps only the two original authority guards. Their actual native fixture
has four members; repeating that independent proof in an H2 result does not
claim sixteen-member native execution or an emitted runtime-pause fence.

The implementation now has 131 automatic executors (74 non-Heal, 57 Heal),
with 27 Heal executors remaining. The complete local suite ran 375 tests with
seven macOS skips, including 21 C07 census, native-evidence, pending-ACK,
recovery and filesystem tests. Linux CI additionally requires real mount
descriptors, root-owned generation recovery and nested bind-mount refusal.
The 158 IDs/hashes and 84/301/532 matrix, Bash syntax, scoped actionlint and
whitespace checks passed. C07 has no canonical live PASS claim; hosted native
run `38100407691` is still building.

Hosted build `38100407691` has now completed successfully. Independent local
artifact verification confirmed all six exact tests against unchanged source
tree `8071fb0a9e5353eb7d3fa58c8a58c8622473f2bd`, candidate Cargo.lock
`d8f7bc416ba67d19873d34e9389e5aa823b22d7d7552706a5a810cee26f6eda2`,
collector `6f4901573967c53b88239af361c1ab543f44fd4d` / SHA-256
`ca6d2087416761ba3b5398366391b9f5d8108b59272f5759ea4b67752d9caf45`,
and executable `0c9e005ae877f741a62100f0d70af6c2256d1078d546ac97e963d6979aa05bc1`.
The original collector resolves its executable path before recording commands;
C07 verifies that exact hosted path, without replacing it with a downloaded
local path. Both authority guards passed, independently of any fleet result.
No new canonical PASS is claimed.

All six auto-testing checks passed at `07627ade0667e67e5560fccbc08215458d506b04`.
The Linux contract job ran all 375 validation tests without skips, including
real original mount-descriptor recovery and nested bind-mount refusal. The
C07 pending serial plan uses this successful native tools attempt; the active
queue resumed F06 in run `38102031029` after the hosted build completed. Shared
fleet locks, cleanup guards and the active plan are retained.

C07's complete generation archive includes the fixed candidate's actual
`part.N.integrity.<uuid>` commitments, independently observed in the retained
A06 physical evidence. Only canonical non-nil commitment UUIDs, one commitment
per existing part and the bounded part/commitment budget are accepted. Every
commitment's exact bytes and name follow its chosen archived generation;
payload-less, repeated or ambiguous commitments are rejected. Two additional
regressions verify complete A/B commitment preservation and invalid-leaf
refusal. The updated local suite runs 377 tests with seven macOS skips; the
real Linux generation test also includes a commitment file.

C10's shared-version branch requires the candidate's real metadata codec,
two independent same-payload ACK versions and physically identical original
parts. Its encoder is built as an optional immutable helper alongside the
read-only inspector, without modifying the candidate source. It reads bounded
original metadata from stdin and emits a separately encoded result on stdout;
it has no filesystem mutation API and makes no physical byte-equality claim.
Only the destination data-directory reference and its source commitment
descriptor may change; all version identities and unrelated metadata are
reloaded and compared before output. Three actual codec tests must pass against
the candidate before the artifact can be consumed. Python guards additionally
bind source/tree/Cargo.lock/helper/binary/role and the successful build attempt,
and reject incomplete, ignored or zero-test output. This artifact is the prerequisite for the complete C10 executor described below;
tool success remains independent of the canonical live verdict.


C10 now dispatches all five source branches on both H1 and H2. Runtime profiles
freeze the source's 0/300/30-second dangling policy, and each trigger records
server LastModified, original-node clock agreement and the actual elapsed age.
All independent original roots archive their complete objects, including real
integrity sidecars and absent post-DELETE trees. Durable arm records precede
controlled replay or shared conversion. Rescue uses the last acknowledged
post-DELETE authority on every original root, preserves the failed scene and
cannot restore the deleted old generation. A shared seed is retired before
later ACKs; unknown writes, lost arm receipts and partial recovery retain DIRTY
namespaces and block cleanup.

The shared branch proves two independently acknowledged original generations
have identical physical parts before the candidate codec changes only their
shared data-directory/commitment references. Both full peer S3 inventories and
all physical references must agree before the later third version is created.
The retained shared references and delete-marker controls survive orphan
removal. Pending MPU uses a bounded, descriptor-relative, read-only census of
the isolated run's complete multipart tree, exact all-peer UploadId/list_parts
checks, and an acknowledged completion followed by full payload verification.
Product physical and S3 assertions run before any fixture rescue. Every live
part receives candidate-bound streaming verification before final S3 checks.

Hosted tools run `38103474555` attempt 1 succeeded at driver
`9b4cc40abd5ea3a0a5ee6c52fb7e113b57102d6e` and helper
`6cc8cb79a0f8024b0089a43d31b0708a46bbae4c`. Independent original artifact/API
verification confirms all three codec tests, source/tree/Cargo.lock/helper/role
and binary `eb2991cf6f84b6998a474c51cd142c2b1c84106fd7f49f569fce7a241dc4ecea`.
Runs `38102872003` and `38103273662` failed on invalid synthetic timestamp and
non-persisted/non-marker rejection inputs; these failures remain retained.
The corrected inputs keep all transformation/refusal assertions. The earlier
wrong-SHA dispatch `38102848357` was cancelled and is not tool evidence.
Ordinary C10 execution consumes this artifact without a RustFS checkout.

The complete local validation suite now runs 397 tests with eight macOS skips.
C10 has 20 tests covering source completeness, typed deferred/refusal results,
all-member live reference/part/commitment/control safety, absent-authority
recovery, foreign device/candidate refusal, uncertain ACK retention, shared
seed retirement and pending-MPU alias refusal. Its real Linux descriptor,
root-owned recovery and nested bind-mount test is mandatory in CI. C10 still
requires canonical live evidence; there are 132 automatic executors and 26
remaining Heal executors.

F06 `heal.cancelled-task-restart` passed in run `38102031029` at harness
`f248cb63d032cfc1658d32c508bce745f96b885f`: both H1/H2 partial cancellations,
quiet physical windows, persisted cancellation after restart, no old-task
replay, new-token repair, all-peer S3 and cleanup passed. Its independently
rerun canonical gate passed, increasing complete live PASS to 49.
F04's coordinator process-crash run `38103118033` remains canonical FAIL with
cleanup PASS: root restart readiness, the MRF collector event budget and
replacement disk-UUID observation did not satisfy their gates. The serial fleet queue continues under its existing lock and
unclean-environment guard.


All six auto-testing checks passed at C10 executor commit
`b0fc7d15ba170bd592e6f4012c1ba15125186afb`. Original Linux contract logs from
run `38104232517` show 397 Python tests in total with no skips, including real
C10 absent post-DELETE recovery and nested bind-mount refusal. The serial C10
live plan binds this harness and successful shared tools run `38103474555`;
it waits for the existing queue and does not change an active plan or guard.


C10 verifies each ACK version separately when two versions share one physical
part path. The general scope verifier keeps its independent-path requirement;
C10 reports version-addressed streaming checks and distinct physical parts
separately, without inventing independent disks. New regressions verify both
VersionId calls and complete shared-conversion/seed recovery with retained
original metadata, parts and commitments. The updated complete local suite
passes 399 tests with eight macOS skips; C10 now has 22 tests. Its queued live
plan is updated to this corrected revision before fleet dispatch.


A10 now dispatches all nine source option branches on H1/H2, with fresh DB and
DV namespaces for both baseline and selected requests: 72 distinct instances.
Each pair changes only the reviewed option, or the inseparable pool/set
selector pair. The exact candidate Admin/selector source hashes and supported
fields are recorded in the contract; this is API coverage, not an rc claim.
H2 scope probes use two actual eight-drive sets across the original four
nodes. Recursive probes include a child prefix, an adjacent prefix and outside
controls; selectors bind each ACK version to its measured physical set.
Every historical version and marker must have complete, exact responsibility,
truthful counters and retained settings. `readRepair=true` and invalid selectors
must return HTTP 400 `InvalidArgument`, leave no active responsibility and
preserve both missing and bitflipped control parts.

The remove pair reuses C10 descriptor capture, complete original-member census,
post-DELETE authority and safe recovery. Both DB and DV delete every original
target generation through separately recorded S3 ACKs, prove no live metadata
or payload reference, then replay only one owned data directory. Both phases
use the same data-only fault; only `remove` changes. Product physical and all
peer S3 checks precede recovery. An accepted parameter which fails its registered
physical behavior remains FAIL; it is never counted as default repair or as
option rejection. Unknown, wrong or duplicate mutation ACKs and incomplete
post-DELETE restoration retain DIRTY namespaces. This executor has 18 local
contract tests and still requires canonical live evidence.

B08 live run `38104014806` remains FAIL with cleanup PASS: both write-loss
branches produced incomplete target identities; H2 lock loss also failed a
snapshot that incorrectly required a live PID on stopped peers. The collector
now attests online processes normally and reads every stopped original member
only with the quiescent check. Two regressions retain all physical members and
reject foreign stopped-node bindings. No quorum or identity assertion changed.
A new immutable live run is required; the old failure stays visible.

The current flapping/MRF queue retains `38104306522` and `38104630791` as
canonical FAIL with cleanup PASS. F03 drive-offline observation, MRF admission
and source ownership checks failed. E03 offline PUT and marker branches passed,
but its exact offline version DELETE had no committed durable MRF responsibility
within the registered window. These partial results do not add canonical PASS.
All six checks passed at `23953a96b7c565da3bee9f7c9442d803ba9ab7cc`; Linux run
`38104493427` executed all 399 then-current tests without skips. The current
A10/B08 local suite passes 419 tests with eight macOS skips. There are now 133
automatic executors (74 non-Heal, 59 Heal), 25 remaining Heal executors and
49/158 complete canonical live PASS results. All original 158 IDs/hashes and
84/301/532 Heal recipes/branches/topology templates remain unchanged.


All six A10/B08 checks passed at `7128b50818de3206f6fb368d4ea696e56da7206b`.
Original Linux run `38105302558` executed all 419 tests with no skips. The
A10/B08 live plan retains that exact revision and successful inspector attempt
`38103474555`, under the original fleet lock and cleanup guard.

`environment.inspect` is a separate read-only workflow operation, outside the
158 canonical IDs. It records the current four Heal hosts, all eight possible
mount endpoints on each, filesystem and physical-device identities, available
space and kernel/command interfaces. A shared partition, stacked/virtual
device, ordinary directory, duplicate filesystem identity or insufficient
space cannot certify an independent endpoint. H3/H4 resource shapes remain
separate from actual EC8+4 layout and fault proof. An installed cloud command
cannot certify platform restart permission or a compatible release pair.
No namespace is reserved, no service is stopped or installed, and no case is
scored. Five regressions guard these boundaries; the updated complete local
suite passes 424 tests with eight macOS skips.

The six pending live plans now write their downloaded original evidence to
`/Volumes/Developer/KAI/validation-migration-evidence`; their candidate, tool
and harness identities are unchanged. This moves only future outputs because
the system disk has about one GiB free. The currently active flapping/MRF plan
and every existing artifact remain at their original locations. The local
`pending-output-locations.json` records each prior/new destination.


I04 `heal.queue-pressure-and-admission` now has all four source branches on
H1/H2. Its existing private Heal services freeze a two-slot queue and one
worker. The 769/129/129/129-object independent buckets have full ACK and
physical manifests; Merged returns the same token, Dropped carries its exact
structured overlap reason, and Full requires HTTP 503 SlowDown with
`admission=full, reason=none`. A live private drive refusal creates three
exact degraded-write ACKs. Actual Normal-priority MRF Full events must occur
while all three original committed responsibilities remain active. Recovery
keeps the original process, and retirement requires each successful exact
source task followed by all-member physical checks and every peer's complete
S3 manifest. Queue/source/priority counts, request latency, RSS, FD and thread
curves remain bounded by frozen budgets. Twelve negative oracle tests reject
false admission, wrong identities, premature retirement and resource drift.
No I04 canonical live PASS is claimed. There are now 134 automatic executors
(74 non-Heal, 60 Heal) and 24 remaining Heal executors.

Read-only current-fleet inspection `38106333882` at helper
`b75f4a7602dca463c5aa532133ddc022f132051f` found 16 independent eligible
endpoints: four on each of the existing four Heal nodes. H1/H2/H3 resource
shapes are available; H4's required 24 endpoints are unavailable. This does
not prove actual EC layout or an H07 PASS. H6 restart authority and a
compatible immutable release pair remain unverified. Original Linux contract
run `38106020097` executed all 424 tests without skips.

F04 `38105011828` remains FAIL with cleanup PASS: root canonical counters,
MRF partial replay admission and replacement disk identity failed their
independent gates. I02 `38106408335` remains FAIL with cleanup FAIL: bounded
block exhaustion was not witnessed, inode repair admission failed, and the
quota GET returned usage-unavailable before workload/Heal admission. Failure
recovery incorrectly called the stopped service. Quota rescue now restarts
only its owned service and independently checks config without claiming
usage or product repair. The narrowly supported archived H1 quota failure
can also restore both original missing parts against an unchanged complete
scene and remove its private bucket/quota namespace without starting a
service. Invoked/uncertain PUTs, changed candidate/run/versions, missing fault
receipts or another dirty instance reject finalization. Original failure
results remain immutable; any subsequent recovery has separate evidence.

Recovery run `38107394081` rescued both exact original shards but failed
before namespace removal because its remote finalization snapshot lacked the
encryption attestation module. Both audited finalization actions now carry
that dependency. An interrupted quota rescue may resume only when every
remaining file and format matches the archived failed scene and each added
file is one of the exact original missing shards. The original fault intents
and backups are revalidated even for already restored bytes. Unexpected
content, missing controls, duplicate files or another host still retain the
namespace. Two regressions exercise remote dependency loading and every
accepted/rejected retry scene. The fleet remains guarded until a separate
successful recovery proves namespace removal; neither original FAIL changes.
Linux contract run `38107374734` independently confirms all 441 tests passed
with no skips at `e40fd9d74a902b9b9733c7bddfdc0b9d1b3f5c03`.

Independent quota recovery `38107681061` at
`6ef733d6d62e6a9fe6f9e0d6e8f4f01cdb947165` completed with the owned service
stopped, both original shards restored and the complete archived scene hash
`979e39a78712ed39bba8af43acbd1187d57c9e5edd730f474cad3c2f2936d2d4`.
Its original finalization evidence proves private roots and bucket/quota
namespace removal. Original I02 canonical FAIL and cleanup FAIL are retained.
The serial queue resumed after independent successful-attempt API identity,
artifact hashes and exact failure/guard binding were checked. Rejected
short-SHA dispatch `38107655997` never entered recovery. Linux contract
`38107650661` executed all 443 tests without skips; all six checks passed.

`manager_native.py` adds a separately collected exact-source manager suite
for F07/C11/J02 prerequisites. Fifteen original candidate tests cover corrupt,
future and stale checkpoints, real MRF payload/manifest write failures,
four deterministic migration publication interruptions, uncertain root
terminal publication, incarnation replay/checkpoint guards and outcome truth.
Hosted `build_manager_native` builds the unmodified `rustfs-heal` library with
the immutable candidate tree, lock and eight reviewed source hashes. Every
test runs by its complete name in a fresh process. Verification retains both
inventories, exact commands, executable and collector digests, original logs
and individual results. Seven negative evidence tests reject zero/ignored
execution, scope inflation, wrong build attempts, filtered names and modified
source or executable identities. This suite does not inject individual fsync
hooks, demonstrate physical power loss or execute canonical fleet branches;
it adds no automatic executor or live PASS count by itself. Existing C07
native collector and pinned artifacts retain their exact contract.

The F07 durable-record backend now reads only exact root/checkpoint task names
or the six allowlisted MRF slot files, without enumerating system metadata.
Injection requires the stopped owned recovery service, unchanged candidate,
actual run/disk/mount identity and archived complete file digest. An Admin
checkpoint must retain its original task, owned bucket incarnation and valid
candidate integrity digest. MRF injection additionally proves an actual
committed active responsibility on the selected slot. Controlled truncation,
future schema and CRC corruption retain original backups and intents before
mutation. Terminal reports, resume states, blocked fences, commit manifests
and unrelated records are read-only. Rescue revalidates backups and exact
injected/original bytes; advanced or removed product responsibility is never
resurrected. Distinct injection/rescue temporaries preserve interrupted
evidence. Ten file/remote-boundary regressions cover these guarantees. This
backend remains an F07 prerequisite; the six canonical branches and their
full live execution are still outstanding, with no count or PASS increase.

Hosted manager run `38108127382` attempt 1 succeeded with driver
`b8126e50f78e40dfd108c7bf32162c751d4a87dd` and collector
`aba6f5c6a36121adcc5a67663b767d0de5667ba9`. Independent artifact verification
confirmed the immutable source tree/lock/eight source hashes, both collector
digests, executable SHA-256
`3ec2d5cfb22db74ae44ddacb6548e0b30fbf0fc35359e0fa04bc4c88e83c431b`,
the complete build/runtime listings and fifteen original one-test commands,
logs and PASS results. The original four publication interruption hooks are
distinct from individual fsync hooks and physical power loss. This adds no
canonical fleet PASS.

`checkpoint.py` locates the actual Admin erasure-set checkpoint selected by
an empty root request with explicit pool/set selectors. Ordinary bucket and
cluster scans cannot substitute for that record. Exact task/disk paths must
contain one schema-8 checkpoint and one schema-7 Admin resume state on the
same physical owner, plus one schema-3 root admission on its exact owner, with
unchanged scope/options, valid candidate digest, owned bucket incarnation and
truthful durable progress. Read-only live observation brackets two identical
bounded record reads with the original service birth, candidate, command and
environment. The bounded profile exposes root records only for read-only census
and observation; root mutation and MRF mutation remain rejected. The
freeze/crash boundary requires pidfd-bound SIGSTOP/SIGKILL and unchanged
stopped record bytes before any fault; no platform power event is claimed.
Duplicate JSON fields, non-finite values and duplicate dedup identities reject
injection. Root admission, live replacement/process drift, missing/duplicate
owners, foreign scope, typed options and fabricated progress are covered by
negative tests. These are F07/C11 prerequisites; the remaining 24 complete
executors are still outstanding.

The Functional Chain consumer downloads the manager artifact from the exact
successful tools run attempt before fleet mutation. `manager_fixture.py`
independently rechecks all fifteen original results, then exposes only the
ten F07, three C11 or two J02 prerequisite tests assigned to the selected case.
A failed/partial attempt, foreign workflow, wrong candidate, changed result
or unrelated case rejects the artifact. The same consumer independently
verified the retained `38108127382` artifact for all three scopes. It exports
no canonical fleet execution, individual fsync-hook, emitted barrier-log or
physical power-loss claim. These required evidence inputs do not make the
three incomplete canonical executors dispatchable.

F07 stale-generation injection now consumes an immutable earlier checkpoint
captured from the actual candidate process, rather than synthesizing a record.
The capture brackets two stable exact-path reads with the original service
birth and retains the original bytes and run/candidate/disk/device/mount/task
binding in separate quarantine files. A later generation must prove strictly
greater durable progress in the same incarnation, set and JSON-typed options,
with truthful monotonic disposition counters and an original runnable cursor.
The bounded orchestration waits for this actual advance before freezing the
owner. Capture does not mutate product metadata and does not claim a live CAS
barrier; the independently collected native stale-manager test retains that
separate responsibility.

Fault backups preserve the newer pre-fault generation. Rescue reinstalls that
newer record, retains the injected older bytes and original capture, and
refuses to overwrite any advanced or deleted product responsibility. Fleet
cleanup now restores registered journal faults before namespace deletion;
failed restoration marks cleanup FAIL and retains every private namespace.
Eleven additional regressions exercise real-file capture/advance/inject/rescue,
immutable evidence, strict JSON identities, process drift, missing or foreign
capture proof, counter/cursor drift and retained failed cleanup. The complete
local contract suite executes 498 tests with eight macOS platform skips. These
are execution prerequisites: F07's six branches on both H1 and H2, including
bounded write failure and full retained-responsibility diagnostics/rescan,
remain required. Automatic coverage remains 134/158 with 24 Heal executors
outstanding; no canonical fleet PASS is added by these changes.

F07 also has a dedicated `journal-bounded` runtime using the existing
fixed 128-MiB-per-drive filesystem backend. Only fixed block exhaustion is
accepted for this profile; caller-supplied limits, arbitrary paths, inode
faults, root admissions and MRF payload mutations are rejected. The candidate
receives verified private service bindings on the same four original physical
drives, with scanner/MRF/automatic repair disabled. Exact checkpoint reads use
those actual bounded views, and capacity witnesses retain the original process
birth, namespace, UID, device and precise ENOSPC result. Filler is released
before registered journal rescue; failed release retains all namespaces.
Six new boundary regressions pass. The complete local suite now executes
498 tests with eight macOS skips. The current Linux contract run
`38111929721` passes its catalog/matrix/shell/scanner gates and the Heal
contract step executes 479 tests. The prior Linux run `38111242118`
independently confirms 490 tests without skips at the preceding helper; no
result is relabeled to the current helper. No bounded-journal live execution or
complete F07 executor is claimed yet.

Current reconciliation: helper `6f91210f224dc0a9e460b8240c4a4cf73d7e64c1` is pushed to auto-testing PR #113 and contains the A09/G01/G02/G03/G04/G05/G06 race executors. The current local contract suite runs 518 tests with eight macOS skips; the preceding Linux contract run `38109944413` ran 473 tests without skips. The independent canonical census remains the source of live PASS totals; native, read-only, recovery, duplicate and incomplete runs are excluded, and the new G04/G05 executors add no canonical live PASS claim while their native marker/MPU fences are BLOCKED. A09 and G01-G06 preserve their source branch IDs, ACK-bound foreground operations, H1/H2 physical evidence, peer convergence and run-owned cleanup; native commit/version/delete/marker/MPU/copy fences remain in the denominator until a reviewed candidate barrier exists. The matrix has 141 registered dispatch IDs (74 non-Heal and 67 Heal), with 17 Heal cases still lacking automatic dispatch. Registration includes executors whose required native branches remain BLOCKED; it does not certify complete canonical migration or live PASS.


The environment-gated batch now registers F08/F09, G07/G08, H01-H05/H07, I01/I06 and J05. The default gate writes a per-case BLOCKED reason and environment evidence before any SSH/install/fault operation. Explicit opt-in still requires exact candidate/client/runtime provenance, every required topology and a digest-bound resource adapter. The enabled control flows reuse auto-testing Probe/Recorder and physical fault oracles, retain every source branch and actual 24h/72h soak duration, stop on dirty cleanup, and require source-assertion review instead of manufacturing PASS from adapter responses. No live run or current-fleet adapter deployment is claimed. Second-batch blockers are recorded accurately: C11/F07/J02 already have 3/10/2 candidate native prerequisites but still need full live orchestration/evidence; I05 lacks an exact reviewed lock-owner/abort fence. See `docs/validation-heal-environment-gates.md`.

This batch passes all 36 test modules selected by Validation Catalog CI: 537 local tests with eight macOS platform skips, plus exact source-catalog hash comparison, the 84/301/532 matrix, shell syntax and scanner mock gates. The 19 new environment tests cover disabled dispatch across all 13 IDs without SSH, explicit runtime provenance, topology and counter negatives, complete enabled IO control flow, actual overlap, full-duration soak control, and recovery after a lost setup ACK. These are local contract checks; no live environment-gated result is claimed.


The 2026-10-12 reconciliation rechecks both retained Pool tail results, every referenced original file digest, and the current canonical report gate. The derived census now contains 88 unique PASS cases; it preserves the preceding 86-case census and excludes cleanup/native/read-only runs. Of the 141 automatic executors, 53 still lack complete canonical PASS: 31 have retained live failures, seven (A09/G01-G06) require reviewed native barriers, and 15 have no complete independently audited live evidence. The 13 environment gates remain disabled and C11/F07/I05/J02 remain outside dispatch. Fresh F05/J06 verification must use a new immutable harness pin containing the durable bucket-event validator and the 120-minute performance lease, followed by the 15 evidence-gap cases on the same serialized current auto-testing fleet. Old failures and recoveries are retained separately.

Fresh read-only inspections `38193642346` and `38194452552` confirm the same four current Heal nodes and 16 independent eligible endpoints. F05 run `38193742108` pins helper `0334abfefeba0d7ea85855da1fe9aa8a287d2940` and tools attempt `38103474555/1`; it retained FAIL/cleanup PASS. Inspection of the final pending-MPU consumer found a separate harness defect: `Probe.s3` streams and hashes GET bodies into the top-level `payload`, but F05 read `response.payload`. The consumer now verifies the actual streamed receipt on every original peer. A regression uses real Probe streaming receipts, proves the original nested-payload check rejects healthy bytes, and rejects corrupted bytes from the fourth peer. All 22 F05/J06 focused tests pass. The original local queue was stopped at its clean paused case boundary; its immutable plan and original FAIL remain intact.

The replacement 17-case queue pins helper `ccef050df3b28416c51356951e78cb46dd74d10b`. F05 retry `38194557163` also retained FAIL/cleanup PASS: the original root token returned `stopped`, `aborted/untraversable`, partial coverage and zero processed objects. Its detail names three bucket failures and `Storage error: erasure write quorum`. The fleet starts peers concurrently and waits for readiness, but the recovered task runs during startup. Candidate `heal_bucket` wraps its typed storage error into `TaskExecutionFailed`; the message fallback does not classify this write-quorum string as recoverable, and cluster traversal records a permanent untraversable failure. This is a candidate recovery-path diagnosis, not a failure of the later MPU consumer and not a reason to issue a replacement task or accept an aborted result. J06 retry `38195131282` then started on the same immutable queue; its result remains pending.

Original A10 `38120735805` and C10 `38120042167` negative receipts exposed another harness mismatch. The candidate returns `stopped/completed_with_errors` and one exact permanent not-found receipt for a nonrecursive absent prefix or protected metadata-less orphan. The old convergence helper required `finished`; A10 additionally demanded an empty receipt list and C10 demanded `finished` even for a failed traversal. Convergence now compares the original path/token, settings and complete matching terminal outcomes, including truthful failed terminals. The negative oracle requires exactly one original failed object identity, unchanged selectors, an unversioned/unbound absent identity, complete coverage, exact 1/0/0/0/1/0 counters, one visible failed attempt, zero overflow and the exact not-found detail. Normal repair branches still require successful execution and their full ACK manifests. The 199 focused tests pass (one macOS skip); read-only replay of both original C10 refusals passes the corrected oracle without changing either retained FAIL or adding canonical PASS. Fresh full A10/C10 runs are still required.

B07/C07 negative task oracles now require `stopped` for their already-required failed or aborted canonical execution, matching the candidate's `legacy_execution_status` projection. They still reject successful, unknown, incomplete or wrongly attributed responsibility; the four original C07 task receipts from `38119830051` remain rejected because they are completed deferred existence checks with no incarnation, rather than exact failed/aborted conflict receipts. The 254 focused contract tests pass with two macOS platform skips, and the source matrix remains 84/301/532. These oracle repairs add no canonical PASS and require new immutable live runs.

J06 retry `38195131282` completed all twelve fixed ABBA phases with 1,440 scheduled requests and zero request failures, then retained FAIL/cleanup PASS at the original maximum paired latency ratio of 3. The second group's PUT p99 means were 67.45 ms without Heal and 353.32 ms with Heal (5.24 times); the third group's were 84.04 ms and 292.13 ms (3.48 times). The other ten paired comparisons met the gate. No acceptance threshold changed and successful physical repair or completed phases cannot replace the failed performance gate. The same serialized queue then started A03 `38198122041` at helper `ccef050df3b28416c51356951e78cb46dd74d10b`.

A03 run `38198122041` passes every required H2 cross-node branch at helper `ccef050df3b28416c51356951e78cb46dd74d10b` and driver `03fb3d78a0ffc4213583dd3bcb44c7ac38f42ead`. Independent verification of the original artifact members, canonical result gate, complete branch outcomes and immutable candidate provenance increases the unique canonical census to 89. The remaining serialized backlog continues on the same candidate; no prior failure is overwritten.

J02 now has a non-dispatched orchestration prerequisite in `outcome.py`. It reuses the existing Fleet, Probe, Recorder and physical fault backends for H1/H2 and DB/DV: one mixed healthy/repaired/below-R task, a separately admitted dry-run task, and an exact metadata-dependency refusal. Complete ACK identities, the actual R-1 geometry, original incarnation, typed dispositions and exact canonical counters are reconciled before fixture rescue; the original token/outcome is checked again after rescue. The reviewed candidate's four failed bucket-object attempts remain visible, and legacy `objectsHealed` is never used as proof of repair. Unknown native evidence remains a prerequisite and its branch is explicitly BLOCKED pending a reviewed live unknown/lock-deferred fixture. These control flows have local negative-contract coverage but no live execution claim; J02 remains outside dispatch and the source assertion requiring a mixed deferred population is still outstanding. The 84 cases, 301 branches and 532 topology templates are unchanged.


The corrected live census now contains 99 unique complete canonical PASS cases: 85 on candidate source `072b57b37559fbe626aeda8abf61617fdcbdd926`, 12 on the older immutable source and two on source `16cf19817e0c81e24d0d787a916f734a34517ec5`. C04 `heal.truncated-and-zeroed-shard` run `38203454300` and `heal.completed-multipart-object` run `38204339623` were independently audited with clean finalization. The derived ledger records 35 retained live failures, 7 required native barriers, 4 orchestration/evidence blockers, and 13 environment scenarios disabled by default.

D07 `heal.range-head-and-conditions` run `38205235080`, D09 `heal.object-lock-retention` run `38205854024`, D10 `heal.special-keys-and-pagination` run `38207381816`, and `heal.manual-normal-attribution` run `38207956101` passed every required source branch and were independently audited against the original canonical results, referenced member digests and immutable provenance. Both finalizations passed. A distinct validation-channel package was built successfully in run `38204362745/1` from source `16cf19817e0c81e24d0d787a916f734a34517ec5`, with package SHA-256 `531965f2c009a5a6ef1c02cc6aa1ba82a82b6fbe4aa441d89613afc8a3f91d40`. It contains the typed bucket-heal error and SSE-C response fixes. The D08 and manager native collectors explicitly bind this reviewed source and its actual source tree; the original candidate remains separately supported. Evidence for one candidate cannot certify the other. No new-candidate canonical PASS is claimed before complete fresh live execution.

Manual-deep attribution run `38208251180` passed every required source branch and independent canonical audit with clean finalization. Hosted tools run `38207334642/1` also completed: the new candidate inspector and all fifteen exact manager regression results passed local independent artifact verification. Source tree `7edb1a2d02d60323cdfab4173485c33cb06b297a`, candidate lock, collector source digests, original logs and exact one-test invocations were checked. These native results are scoped as ten F07, three C11 and two J02 prerequisites and add no canonical PASS.

F07 now has a non-dispatched partial orchestrator in `persistence.py` for actual checkpoint truncation and future schema on H1/H2 with DB/DV fixtures. It admits an explicit Admin erasure-set task, observes real durable progress, freezes and crashes the exact process, validates stopped root/checkpoint ownership, and mutates only that original checkpoint. Restart must preserve corrupt bytes and resume ownership, emit the blocked fence, and retain a durable original failure terminal/report. A distinct rescan is checked against complete ACK/physical/S3 manifests without reclassifying the original failed token. Original corrupt record and failure-report bytes must remain unchanged through rescan and business reads; cleanup restores only unchanged injected journal bytes. The MRF checksum, earlier-generation replay, bounded journal-write failure and exact persist-fence branches remain explicitly BLOCKED, and F07 stays outside automatic dispatch. No F07 live result is claimed. Nine new contract tests reject changed records, missing or substituted responsibility, silent success and invented counters; 61 focused persistence/native prerequisite tests pass, and the 84/301/532 matrix remains unchanged.

D04 run `38204764944` has a digest-bound diagnostic comparing the stopped stale-authority scene with the pre-manual-admission witness: only the target member xl.meta changes on each of H1 and H2, adding the exact newly acknowledged marker. Every other file and disk format remains unchanged. The candidate keeps durable MRF replay active when best-effort ingress is disabled; exact source-task attribution still needs investigation. The manual attribution gate and original FAIL remain intact. B09 run `38202851374` likewise has a complete identity-difference diagnostic: no expected ACK identity is missing; vm003 retains deleted `objects/0006.bin` null data in DB and historical `objects/0001.bin` version `de87efd5-e1ef-42a2-a86a-a1e7a18fe46c` in DV on all four disks. External resurrection is not established.

The seventeen-case corrected live queue completed with eight PASS and nine retained FAIL results; every finalization passed. J01 run `38209400619` did not witness the full raw/rc uninitialized startup window on H1, and H2 rejected rc text that erased status uncertainty during convergence. Both disabled branches passed. E03 run `38208556719` retained range-repair physical differences and missing fresh ReadRepair attribution in H2. These partial results do not change either canonical verdict.

C10 `heal.dangling-and-orphan-safety` run `38209676009` subsequently passed complete canonical audit at helper `dbe74ec9eb3288e32923d8b8777c581c904ba78f`; cleanup passed. A10 run `38210630408` retained FAIL with clean finalization after its corrected negative prefix oracle passed: other branches still reported incomplete version/marker diagnostics or unknown/incomplete integrity. The followup completed and discharged its fleet guard. The separate F05/D08 validation-package plan on driver `16cf19817e0c81e24d0d787a916f734a34517ec5` also completed and discharged its guard. F05 `38227684435` and D08 `38229589497` passed independent complete canonical audit and clean finalization after read-only inspection `38227616934`. These are the first two PASS cases for this distinct candidate. D08 covered SSE-C, SSE-KMS, SSE-S3 and encrypted historical versions on H1/H2, including exact ciphertext/physical references, plaintext reads, classification headers and key-refusal checks. The downloaded validation DEB matches its full package SHA and contains RustFS binary SHA-256 `d0d0fce93845728ef00b3b9406210b5bbf728c9262c329e2522319b40bc61917`; rc is an independently bound runner binary and is not included in that DEB.

E03's physical diagnostic for run `38208556719` proves that the Range bitrot target retained the exact injected damaged bytes on H1/H2. Its ReadRepair task used Normal scan mode and returned typed `metadata_healthy` with healed=0/unchanged=1; legacy progress counters cannot certify payload repair. The H2 missing/historical branches recovered bytes but lacked fresh admission evidence in the old bounded journal tail. The executor now collects the complete journal cursor window with pagination, preserving exact source/task attribution. This harness change passed 176 focused Heal/recovery checks and all six CI lanes at `7c3b114c0e611870b0844987b69e8dca059689ca`; E03 still requires a new complete live run on the product repair candidate. Its original FAIL remains retained.

C11 now has partial H1/H2 DV orchestration in `incarnation.py` for `old-metadata-rejoin`. It verifies an original incarnation and complete DV ACKs, denies one actual member's reads/writes, deletes every data version and marker through S3, confirms successful bucket deletion and absence, and creates a same-name successor with distinct bytes/version IDs and explicit bucket configuration. Rejoin retains the original processes; successor responsibility must bind a different incarnation, preserve surviving new shards and contain no old version/marker references. Physical system records, full shard checksums, every peer's complete S3 inventory and config, old-version refusal and an independent control bucket remain separate evidence. Unknown S3 mutation state cannot become clean finalization. Eight new local tests cover refusal and partial-case gates; 52 focused tests, AST/YAML, actionlint, whitespace and the unchanged 84/301/532 matrix passed. This implementation has no live result. Old checkpoint/MRF replay and the exact storage commit barrier remain BLOCKED, and C11 stays outside dispatch.

E03 followup `38235099954` uses package `38231599023/1` from exact source `e224eb48e5311cff022b04fd78c97a8572d5c270`. Five of six H1/H2 DB/DV branch instances passed, including both Range bitrot and historical-version instances with restored physical bytes and Deep typed repair receipts. H2 ordinary GET remains FAIL because no fresh source admission was observed; its original journal contains `Suppressed 13097 messages` for the exact test unit. The read-repair runtime now enables DEBUG only for `rustfs_ecstore` and `rustfs::heal`, keeping the default at INFO and other repair sources disabled. This prevents unrelated HTTP/HTTP2 debug traffic from consuming the journal budget. The 176 focused Heal/recovery checks passed. A complete fresh E03 run is required; the original FAIL and the 99-case canonical census remain unchanged.

The complete E03 followup `38237270640` passed all six required H1/H2 DB/DV instances with clean finalization. Independent audit rechecked the original canonical result, every referenced member digest and exact package/binary/helper provenance. It uses the same source `e224eb48e5311cff022b04fd78c97a8572d5c270` and package `38231599023/1`, with helper `8ea38a9d8044e512de91ec788d79ecb7aa2f0fd8`; the preceding suppression failure remains retained. C08 `38235978152` also passed independent complete canonical audit. The derived ledger now records 101 complete PASS, 33 retained live failures, seven required native barriers, four orchestration/evidence blockers, and 13 disabled environment scenarios. B07 `38236702473` remains FAIL with clean finalization: H1's below-quorum S3 response and H2's negative terminal responsibility violate the frozen oracles.

C11's native collector now binds reviewed source `1aa9be87d18cf941e5020f19e8622c26ba4b10ac`, its exact tree, Cargo.lock, six storage sources, helper digest and an optimized independent test executable. The nine-test suite preserves all six original native regressions and adds three real storage incarnation tests. The final-rename test pauses actual shard reconstruction before publication, aborts its caller while the lifecycle writer remains pending, drains and physically verifies the exact repaired version, and rejects obsolete admission after normal delete/recreate. Four original ordered milestones are mandatory; omitted, duplicated, reordered or invented rows cannot certify the barrier. Execution uses captured successful output and the candidate's bounded larger ecstore test stack. Local nextest passed 28 focused tests; the direct libtest check with captured output also passed. These local results are not hosted optimized native evidence. C11 independently verifies any hosted storage artifact before fleet mutation and records its scope as a native prerequisite with no fleet topology claim. Old checkpoint/MRF replay is still blocked, the metadata branch has no live result, and C11 remains outside dispatch.


C11 original-record replay implementation (2026-10-12)

C11 now implements both remaining H1/H2 DV replay flows through the existing Fleet, Probe, Recorder, checkpoint and read-only MRF observers. Stopped descriptor-relative transactions bind run, candidate binary, old incarnation and original task, durably archive all original bytes before detachment, and restore only those bytes. The MRF allowlist also includes both source-defined legacy fallback journals; whole-record CRC and exact original intent bindings prevent omitted or foreign replay sources. Checkpoint terminals and new product responsibility cannot be overwritten. Empty transition fallback files carry no responsibility and are recorded before removal. Phase changes verify archive and transaction identities and fail closed on interrupted configuration commits. Incomplete capture/install or unresolved S3 mutation keeps every private namespace and original archive.

The old MRF fault gate is released while stopped after all original replay sources are detached and before normal S3 bucket deletion/recreation. Successor and control objects are written on a healthy fleet, independently verified, and frozen before original records are installed. Replayed checkpoints must reject the old generation while retaining original progress and durable terminal/report ownership. Replayed MRF must retain the exact responsibility ID, intent digest and source incarnation under a new process checkpoint owner with greater sequence, durably held as bucket_incarnation_changed without operator acceptance. Complete successor/control physical bytes, formats, system records, full shard integrity and every peer's S3/configuration remain independent evidence.

Immutable package run 38240858236/1 and tool run 38244986993/1 succeeded for source 1aa9be87d18cf941e5020f19e8622c26ba4b10ac. Independent Git-object checks verified the exact source tree, lock and all source contracts; all seven MRF sources match the reviewed reference byte-for-byte. The complete 68,625,642-byte DEB digest is b99dd915376b51f0896fc346ca4642b7d26bf3352135bb5c6f60f8179d865761 and its contained binary digest is 0103ca46386396ed15f27b9efbe4196fe62bd9e93033671a88f3b3e944602cf4. Inspector, nine native storage regressions and fifteen manager regressions passed independent artifact consumers for harness d5a41739a7a544d93ba71f51583d45ad4e6161eb. A new immutable tool build is required for the updated replay harness; older harness provenance is not accepted.

The full Heal contract suite passed 591 tests with eight platform skips before the additional finalization-verdict regression; all 29 focused incarnation tests, the unchanged 84/301/532 matrix, AST, bash syntax, actionlint and whitespace passed on the final code. Two sequential final reviews covered correctness, simplicity, test coverage, path/artifact security and persistence/cancellation. Explicit diagnostic execution cannot claim canonical PASS; clean finalization does not hide a failed branch. C11 remains outside automatic dispatch and complete canonical PASS remains 101/158 pending live execution and independent audit.
