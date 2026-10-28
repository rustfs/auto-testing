# Release validation case migration

Source: `rustfs-release-validation/scenarios/catalog.yaml`. The generated
`validation-cases.json` preserves all 158 canonical IDs, source statuses,
assertions, probes, capabilities, and target environments. Its source commit
and SHA-256 bind the snapshot to the source catalog. `reuse_candidates` lists code to inspect;
it is not a coverage claim. A case's run result remains `unverified` until a
live run proves each source assertion on the required topology.

## Current implementation scope (2026-10-28)

The migration delivers test procedures and original oracles in auto-testing.
Candidate live failures are recorded independently from missing test code.
The explicit [implementation review](validation-migration-implementation-review.md)
and `validation-migration-review.json` record **145 implemented cases,
13 default-disabled environment procedures and zero partial implementations**.
The 13 procedures require reviewed resource adapters to execute. F07 composes
same-candidate manager interruption and ECStore directory-fsync evidence. I05
runs timeout and queued-successor on H1/H2 in one source plan, with separate
same-artifact native owner and full-content/physical generation checks. All
30 implemented cases without full live acceptance retain their candidate
failures or pending evidence independently. Source-path review and local
contract tests do not certify every live branch.

Run `python3 -B scripts/validation_migration_review.py --require-complete` for
the independent counts and implementation gate. It exits zero when the source
implementation has no recorded gaps; retained product FAIL does not change
that decision. `validation_report.py` continues to enforce original assertions,
cleanup and provenance for product acceptance. Unknown failure causes remain
unclassified. Detailed execution prerequisites and evidence scopes are in the
[implementation review](validation-migration-implementation-review.md).

## Current live acceptance and retained evidence

Count reconciliation (2026-10-23; updated 2026-10-28): the append-only ledger
`canonical-migration-ledger-20261023-b08-39915022106.json` follows the
B01 ledger and attaches the complete B08 live acceptance plus the latest B02
failure evidence. The B02 complete acceptance below is a later independent
record; it does not replace that retained failure or replay all 158 cases on
one candidate.

All 158 canonical IDs are retained. Independent complete live acceptance is
**115/158**. The current review retains 22 live failures, four pending
native/live race acceptances, four orchestration/evidence blockers and 13
environment cases disabled by default. G01 and G02's actual failed PUTs are
included in retained failures; historical ledger bytes are unchanged. These
30 unaccepted active cases are a live acceptance backlog, not 30 missing test
implementations. Counts span immutable candidates and historical audits;
diagnostic, native, isolation and partial results do not increment PASS.

| Reconciled case | Complete live run | Required coverage | Original result SHA-256 |
| --- | --- | --- | --- |
| C02 `heal.data-shard-bitrot` | [39281951263/1](https://github.com/rustfs/rustfs/actions/runs/39281951263) | Eight H1/H2 branch instances, 3,334 archive files | `009c96a4bbd1b8c711009c0223519f6f12d8762890d597db749f9e415ca23f1a` |
| C03 `heal.parity-shard-bitrot` | [39286997859/1](https://github.com/rustfs/rustfs/actions/runs/39286997859) | Six H1/H2 branch instances, 2,904 archive files | `da2df2e15aa973b6a04a0cb40491413db17426b63d6367f11682ef579b84235a` |
| C05 `heal.missing-or-corrupt-xlmeta` | [39322217398/1](https://github.com/rustfs/rustfs/actions/runs/39322217398) | Ten H1/H2 DB/DV instances, 3,132 archive files | `489b8179ee90a7412b223e669db8fd4f6520b9e69051cbf927b6f33299de423b` |
| I04 `heal.queue-pressure-and-admission` | [39354651483/1](https://github.com/rustfs/rustfs/actions/runs/39354651483) | Four branches per topology, six exact durable tasks, 5,565 archive files | `4dcde2f34bb7df52f03f5a3ec1e87663145a7134d3ef1a513fda2da52d1709a2` |
| B05 `heal.multiple-shard-loss` | [39843970314/1](https://github.com/rustfs/rustfs/actions/runs/39843970314) | All 38 DB/DV and H1/H2 instances, 9,059 archive files, ten node cleanups and four package purges | `2624f43fc8581eb1b533a477b1a869f9c72208861d7a268ea35ae8f36153fd7e` |
| E04 `heal.scanner-cold-object-repair` | [39863505276/1](https://github.com/rustfs/rustfs/actions/runs/39863505276) | All six H1/H2 instances, 2,307 archive files, five node cleanups and four package purges | `8bfcfb03bfa5cc88649e98c4f9a6fa7a27d64f9fd0cdbe8d851f57fc00bc0702` |
| A05 `heal.admin-overlap-and-force-start` | [39892106641/1](https://github.com/rustfs/rustfs/actions/runs/39892106641) | All eight H1/H2 overlap and force-start branches, 20 cleanup receipts | `4a8073b6fe129a5bf696ffbc74b0024f1b23e66232606df13c7f606e0511b036` |
| B01 `heal.disk-loss-and-repair` | [39899825839/1](https://github.com/rustfs/rustfs/actions/runs/39899825839) | H1/H2 original-drive-return and offline-ACK repair, 272 fixture ACK/request pairs, five cleanups | `fe89fbb0e4e2d93aa5c4408b9218c66d6caa9120f067f3aca3560f4ca360c4e5` |
| B08 `heal.below-write-quorum` | [39915022106/1](https://github.com/rustfs/rustfs/actions/runs/39915022106) | H1/H2 write/lock quorum loss, H1 lock-loss not applicable, 3,463 archive members, ten cleanups | `b5fbc87b2a9e7dab2b07301c8703f0d1733ee44f11ef984b3bce55b65b5df87e` |
| B02 `heal.disk-readonly-and-permission` | [39927051614/1](https://github.com/rustfs/auto-testing/actions/runs/39927051614) | H1/H2 read-only and permission-denied, same-process gate release, exact shard restoration and five cleanups | `ba2f2569f3799a2aed014c9b89d37a80075774e76dfbfd1b3a06c04c38cb533e` |

| Acceptance state | Cases | Remaining requirement |
| --- | ---: | --- |
| Independently accepted complete live PASS | 115 | Preserve original provenance and complete results. |
| Retained live failures with existing executors | 22 | Preserve original results and attribute the cause; product failures do not imply missing test implementation. |
| Native and live race acceptance pending | 4 | A09 and G03–G05 await complete live acceptance; existing native/runtime paths retain their candidate gates. |
| Orchestration or evidence blocked | 4 | C11/F07/I05/J02 have source execution paths awaiting independent complete live acceptance; no implementation gaps remain in the current review. |
| Environment gated, disabled by default | 13 | Control flows, oracles and unmet reasons are implemented; dedicated resource adapters and real execution remain conditional on the required resources. |

The current delivery keeps the 13 environment cases disabled with explicit reasons. The other 30 unaccepted cases retain their original live states independently of their completed implementations. C11/F07/I05/J02 have explicit execution routes, while canonical admission stays blocked until independent complete live acceptance. J02's real four-disk regression reproduced scheduler-level whole-batch retries overwriting an earlier repair receipt. RustFS commit `7951bb629821246868ee3761b321dd07c06db45c` retains exhausted Admin batch results; its 210 manager tests and five process-isolated receipt integrations pass.

### Native prerequisites and runtime admission (2026-10-26)

Hosted-only tools [40086026324/1](https://github.com/rustfs/rustfs/actions/runs/40086026324) completed successfully on candidate `757b751482bb970bc321e92059690a7698554358`, auto-testing `002473b51f5ceeccadedf7325a1b1691e4c71dcd` and driver `b1f7fb276941d7128b5ddbbda9c3e6536931c89d`. Its immutable package is [40081956660/1](https://github.com/rustfs/rustfs/actions/runs/40081956660), SHA-256 `1a52387a8cbdef586be2616b02298f351aec4ac2d5c0f7a76d094409c89c0b3c`. Independent read-only verification checked the candidate source tree, registered source-file digests, Cargo.lock, executable digest, ten exact nonignored passing test invocations, all 104 ordered milestones and the inspector identity. The original artifacts and audit are retained in the migration evidence store. No downloaded native executable was run during this audit.

The registry exposes eight native prerequisite groups: A09, G01–G06 and part of I05. This is separate from runtime admission. The pinned helper admits A09, G05 and G06 on this candidate; it intentionally rejects G01–G04 on the same source before bootstrap because their runtime candidate contracts remain pinned to other reviewed sources. G01–G03 may use their reviewed version candidate `bd0671ba781d8ebf509f4f03533228dfb44496f1`, and G04 uses marker candidate `8380466f3a27c3c3ef4115084178a0ab99a78f32`, each with its own exact package and matching helper/tools. Alternatively, a separate source review and matching tools build must precede extending runtime admission. Do not infer admission from registry membership or mix artifacts from different candidates or helpers.

All ten native tests pass. G06 subsequently passed all eight H1/H2 public branches in [40427815826/1](https://github.com/rustfs/rustfs/actions/runs/40427815826); an independent audit verified its ACK, physical, peer S3, overlap and 20 cleanup receipts, adding one canonical PASS. A09 and G01/G03–G05 still need their complete source branches and clean finalization. G02's native/runtime prerequisites were admitted, but its first live run failed before completing the required matrix and is recorded below. I05 now composes the timeout/disconnect controllers and separate full-content/physical native generation checks through `lock_source.py`; complete independent source-plan live acceptance remains pending.

### G02 retained live failure (2026-10-27)

G02 `heal.race-versioned-put` run [40444207551/1](https://github.com/rustfs/rustfs/actions/runs/40444207551)
used RustFS candidate `bd0671ba781d8ebf509f4f03533228dfb44496f1`,
auto-testing `7094c16a5a5cf3b0f818c7be463f3bc0f8fd7645`, and the audited
candidate-bound native tools from [40311010126/1](https://github.com/rustfs/rustfs/actions/runs/40311010126).
The H1 `new-version-vs-historical-heal` branch failed at foreground ACK 000003:
PUT `objects/0256.bin` returned HTTP 503 `ServiceUnavailable` after 5.026 s,
with zero retries. The service log binds request
`2b5289c8-0faa-472d-b5c6-fe9db521659d` to `storage_error_code=Some(Lock)` and
records a contemporaneous `rename_data` operation pending for over 5 s on
`/data/rustfs4`. This supports a Heal/PUT lock-contention diagnosis; the log
does not correlate that rename operation to the request ID, so it is not a
complete call-level causal trace. This was a real failed PUT, not a recorder
false positive or successful ACK.

The branch result is `FAIL/cleanup FAIL`; its last-ACK receipt records
`unresolvedMutation=true` and
`lastACKPhysicalAuthorityVerified=false`. Evidence-based recovery run
[40444795627](https://github.com/rustfs/rustfs/actions/runs/40444795627)
isolated the active namespace. It did not restore the scene, delete the
namespace or rewrite the verdict, and it claimed no canonical PASS. The
original failed scene remains retained and must not be replayed in place.

The Auto Testing PR [#113](https://github.com/rustfs/auto-testing/pull/113)
contains the migrated G02 executor and remains Draft. RustFS PR
[#8455](https://github.com/rustfs/rustfs/pull/8455) completed its distributed
4-node/4-disk E2E in run [40631115072](https://github.com/rustfs/rustfs/actions/runs/40631115072)
on candidate `4454b029dd462e0f58f1b3acc0f4cd62137c24d6`: 92 tests passed and
873 were skipped. The PR includes Heal lock-wait observation and extensive
Heal regressions, but the E2E result does not bind those changes to the failed
PUT request or establish that the G02 lock contention is fixed. It is
supporting candidate evidence, not an immutable G02 package or case result.
Retest only after reviewing a concrete source correction, building and
auditing a package pinned to its exact candidate SHA with matching
tools/helper, and confirming the current Auto Testing fleet is safe for a
fresh run. A complete H1/H2 matrix, physical and peer S3 verification, and
clean run-owned finalization are required before G02 can add a canonical PASS.
Until then the 115/158 total is unchanged.

### Hosted runner inspection (2026-10-26)

Read-only [40256930895/1](https://github.com/rustfs/rustfs/actions/runs/40256930895) completed on a GitHub-hosted runner with driver `31c9d85645c1a8051562983f18631098b9bc34e7`. At `2026-10-26T02:00:24Z`, GitHub reported the only matching `smoke-testing` runner, `rustfs-smoke-testing` (ID 139653), as **offline**, with `busy=false`. The report retained two queued fleet jobs, including the Functional Chain summary and the health collector, plus pending workflow runs without jobs. This establishes the runner registration state at observation time; it does not diagnose the host's service, disk, network or power state.

The new `validation_runner_inspect_only=true` dispatch path uses a separate workflow concurrency group, reads only GitHub API metadata and uploads an incomplete report if API access fails. It rejects simultaneous candidate, build, fleet, tools or recovery inputs; all fleet jobs were skipped in the real inspection. No queued workflow was cancelled or replaced, no test-node connection was made, and `mutated`, `cleanStateCertified`, `sharedWorkflowIdleCertified` and `canonicalCasePassClaimed` remain false. Live resumption requires a working maintenance entry to restore this runner, followed by a new resource inspection and the existing retained-scene and shared-workflow admission checks.

### Interrupted G06 evidence and recovery boundary (2026-10-26)

The re-registered `rustfs-smoke-testing` runner (ID 139936) was online in hosted inspections [40277791903](https://github.com/rustfs/rustfs/actions/runs/40277791903) and [40282750869](https://github.com/rustfs/rustfs/actions/runs/40282750869). Resource inspection [40278133051/1](https://github.com/rustfs/rustfs/actions/runs/40278133051) observed the current auto-testing fleet's 16 independent endpoints. Neither inspection certifies global workflow idleness or restores an interrupted scene.

G06 [40281440070/1](https://github.com/rustfs/rustfs/actions/runs/40281440070) was cancelled during its selected-case step. The only annotation is `The operation was canceled.`; its cause is unconfirmed. The workflow uploaded no original artifact. Read-only collection [40284040202/1](https://github.com/rustfs/rustfs/actions/runs/40284040202) subsequently retained 1,777 original files (38,765,135 bytes), independently checked against every collection-receipt size and SHA-256. Collection provenance remains distinct from the original run; its unfinished result stays `RUNNING/PENDING`, unaccepted.

H1 `copy-source` finalized as `NOT_OBSERVED` with cleanup PASS. H1 `copy-destination` retained all 258 seed request/ACK pairs and its one COPY pair, complete last-ACK physical authority and 1,028 streamed part proofs. It restarted before final peer S3 inspection, then was interrupted. Its recorded public-call overlap is false. No H2 or later source branch was completed, so this evidence adds no canonical PASS.

`interrupted_race.py` admits only this bounded H1 COPY boundary: one unchanged unfinished case, one finalized predecessor, complete deterministic SDK journals, recomputed ACK-derived final state and physical/streaming proofs, and no later operation. Recovery checks all five original owner markers before stopping the exact owned unit, then requires the current stopped business scene to match the archived last-ACK scene. Existing same-filesystem private isolation preserves the original bytes, backup files, inodes and credentials without reading credential contents. Drift refuses isolation; no old-generation restoration or SDK replay is permitted. A new recovery receipt never rewrites the original verdict or supplies missing canonical assertions.

The driver may explicitly select `validation_cleanup_retained_evidence=true` together with an exact failed cleanup run. It verifies that run/attempt and same-runner storage ownership, copies through no-follow file descriptors, checks every original and copied digest and records a new collection identity. This is not an original-run artifact fallback. The ordinary artifact-based recovery path remains the default.

Isolation [40286516720/1](https://github.com/rustfs/rustfs/actions/runs/40286516720) completed with helper `8efc57d7ed89531a8f63cc4f98d06dafa2361499` and driver `3e337bad97a0263fa663f7909b33f247d6f76f17`. Independent audit verified official archive SHA-256 `11f691947bcbf6d707f0ed04ff7bfd625eeb4994221ad0f690bcc8c63cf43645`, all 1,777 original file bytes, the unchanged unfinished-case digest, stopped owned service and identical pre/move/post baselines for all five trees. All archive parents are root/root 0700 on their original filesystems. The disposition is ISOLATED; no namespace was deleted, no backup restored and no canonical verdict rewritten. Follow-up inspection [40287577171/1](https://github.com/rustfs/rustfs/actions/runs/40287577171) observed 16 eligible endpoints, inactive standard services, no port 9000 listeners and no active validation service. The original failed unit remains recorded; inspection does not certify global idleness.

The original COPY SDK calls lasted approximately 23 ms and 17 ms. Their nearest running samples began before those calls or finished after them, so the frozen strict interval oracle correctly refused overlap. G06 schedules observation from botocore's actual `before-send.s3` notification inside the SDK call, after client preparation and durable request intent. The callback only queues a timestamp; it never waits, delays a response, repeats a mutation or alters the SDK call interval. A qualifying original-token running sample must still fit strictly inside one actual acknowledged SDK call. Real local HTTP/SDK tests cover nested running acceptance, finished-task refusal and response completion without any observer. The previous polling scheduler fails the positive regression.

Complete G06 replay [40427815826/1](https://github.com/rustfs/rustfs/actions/runs/40427815826), pinned to helper `7094c16`, passed all eight H1/H2 public branches. Independent replay with the executing helper recomputed all six seeded branches' deterministic journals, original seed properties/tags, final manifests, physical transitions, complete streaming receipts, full peer S3 inventories, terminal outcomes and public overlap. All 20 cleanup receipts match the reserved hosts. G06 is now canonical PASS; the earlier failed run [40299213418/1](https://github.com/rustfs/rustfs/actions/runs/40299213418) remains retained unchanged.

The next executor reuses the existing same-token status transport for G06, preserving its strict overlap oracle, and records read-only endpoint resolution before allocation and restarts. G01/G02 separately restore the frozen source's per-role overlap rule while requiring every PUT ACK and complete concurrent reader evidence. New exact-helper tools and complete H1/H2 live evidence remain required for acceptance.

The source's `heal_h1_concurrency_batch.py::observe_race` at clean commit `524eb5b32c3236497fdd8a9271df0494fa3d9974` requires every planned request to have an ACK and returns `bool(matches)` for its nested running samples. The frozen G06 tag branch has exactly 16 distinct serial updates. The migrated controller incorrectly required all 16 calls to overlap. G06 tag aggregation now restores the source's requirement of at least one qualifying call, while retaining all 16 original requests, all 16 ACKs, exact intervals, final tags and full physical/S3 checks. The evidence retains every per-call overlap flag and explicitly records the required aggregation, so no match still yields NOT_OBSERVED. Other race aggregation is unchanged. Real local SDK tests verify one matching tag call plus fifteen nonmatching calls, all 16 durable ACKs and complete refusal when every sample is finished. The preceding migrated helper fails that positive test; no historical verdict is rewritten.

### B08 complete live acceptance

[B08 run 39915022106/1](https://github.com/rustfs/rustfs/actions/runs/39915022106) is independently accepted on candidate `1a3f038c90704f9ad9693eddcf7d861c9530f13c`, package `39898678364/1`, tools `39914732144/1` and helper `a3d3c62a9f6d29667a1a2b57d13f5cb9e166de3f`. The official 3,463-member archive has SHA-256 `59ddf692cdca495abcd37a1affacf25e4cd3da24c931509bb9b4facc386c2f11`; the independent audit has SHA-256 `63f143d278c5e6a43f0068a31c692cae1064fd2612b54060198c448f52b7e640`. H1 read-present/write-absent and H2 read-present/write-absent plus lock-quorum-absent passed; H1 lock-quorum-absent is explicitly NOT_APPLICABLE under the candidate's in-process local manager. The audit verified all peer inventories, unique PUT absence, physical target restoration, streaming shard proofs and ten cleanup receipts. The prior B08 failure remains retained in the append-only ledger.

### B02 earlier live failure remains retained

[B02 run 39916358715/1](https://github.com/rustfs/rustfs/actions/runs/39916358715) used the same immutable candidate package `39898678364/1` and helper `a3d3c62a9f6d29667a1a2b57d13f5cb9e166de3f`. The complete official artifact `12485117739` has SHA-256 `435fe989937adbad376c93506dfe7db7eec914f3fafb68ea98068d8c79ea5d7a`; its case result SHA-256 is `a92ab5c1e312f0e5f25b83f7f8802b1de3e43c5fd93bff05dd3651c9d1e1f978`. Read-only passed on H1 and H2, cleanup passed, and both permission-denied branches failed with `faulted version lacks a repaired responsibility`. The independent artifact audit `B02-live-39916358715-independent-failure-artifact-audit.json` (SHA-256 `8c9b2e03e437542f1d3bd6894cb8dab225e46e0e9f65aecf4b16f4a3db5d27eb`) confirms the follow-up queried the expected `objects/0017.bin` and returned `progress.objectsHealed=1`, while terminal responsibility reports `verified_healthy`, `healed=0` and `unchanged=1`. Physical snapshots show the faulted shard absent after the denied attempt and present with its baseline SHA after the writable follow-up sequence. The runner stops and restarts RustFS after releasing the gate and before that follow-up; there is no physical snapshot between service startup and the new Admin token, so the artifact cannot attribute the restoration to that token or establish a terminal receipt classification bug. The strict source assertion remained unsatisfied in that run; this original failure record remains unchanged.

### B02 complete live acceptance

Run [39927051614/1](https://github.com/rustfs/auto-testing/actions/runs/39927051614) independently passed both source assertions across H1/H2 read-only and permission-denied branches. It used candidate commit `1a3f038c90704f9ad9693eddcf7d861c9530f13c`, package `39898678364/1` (SHA-256 `33f97e40add31ebe9bc4e923f68bc11d2e8855b4eedf5aad7bc8fce1f07da9d6`), tools `39925186107/1` and helper commit `c98028b697f8f9349a2faca277c14810d9b3e466`. Official artifact `12490700181` has SHA-256 `ba2f2569f3799a2aed014c9b89d37a80075774e76dfbfd1b3a06c04c38cb533e`; the independent complete audit has SHA-256 `6e7a2c9e21ed3bd031d7189da0c2d8cfd607c56626745cb4a797e0099070771b`.

For each permission-denied branch, the initial request was rejected before task admission and had no client token. After the gate cleared, candidate process birth identity remained unchanged and the pre-token physical snapshot matched the denied-attempt snapshot. A fresh Admin token then repaired the exact planned shard to its baseline size and SHA-256. The read-only branches also restored their exact planned shards. S3 inventories matched, all five H1/H2 cleanup receipts passed, and both source assertions were present as PASS. This adds one canonical acceptance while retaining the prior failed run.

A05 scoped live [39875125045/1](https://github.com/rustfs/rustfs/actions/runs/39875125045)
was interrupted by the self-hosted runner's exhausted filesystem. The official
check annotation reports `No space left on device` while writing the runner's
worker log. The selected-case step remained in progress in the completed job;
cleanup and artifact upload stayed pending, and the official artifact listing
contains no files. Neither product failure nor clean finalization can be
concluded from that interruption. Its fixed candidate `c95bb396`, package
`39867606969/1` and matching tools `39874908888/1` retain their independent
byte/provenance audits. The J01 chain stopped before dispatch when A05 evidence
collection refused the empty artifact listing. Its released-client fix remains pending a fresh live acceptance; the B01
explicit disk-error correction is covered by the accepted B01 run above. The administrator has since deleted the interrupted runner temporary evidence;
the user explicitly accepts retaining this attempt as unaccepted. A05 later
passed on a separate complete run 39892106641; that pass does not reconstruct
or change the status of run 39875125045. No product verdict or cleanup PASS is
inferred for the deleted scene.

### J01 startup window diagnosis (2026-10-23)

J01 run [39929436131/1](https://github.com/rustfs/rustfs/actions/runs/39929436131)
used the current auto-testing fleet, candidate `1a3f038c90704f9ad9693eddcf7d861c9530f13c`,
and helper `c98028b697f8f9349a2faca277c14810d9b3e466`. Its official artifact
`12491201856` has SHA-256
`bd703e00ed88297794a6adb72760ede313f7e9177a30978cac45eb60eddbae5d`.
Enabled-idle, disabled, missing-peer, recovery and all four cleanups passed;
the complete case remains FAIL because H1 and H2 did not expose the required
raw/rc `uninitialized` window. The artifact and original verdict remain intact.

The H1 startup journal narrows the cause. RustFS began listening at
`09:07:12.377958Z`. The background-heal endpoint returned HTTP 503 with
`server context is not ready` at `.400036Z` and `.411255Z`. Heal runtime
initialization followed at `.438696Z`, and `server_ready` at `.438747Z`. The
first successful signed status response in the collector evidence arrived at
`09:07:12.514298Z` and already reported `idle`. Thus the startup listener
rejects status requests until the heal runtime is initialized; the source
required HTTP 200 `uninitialized` state is not externally observable on this
candidate during that transition. Connection refusal or pre-ready 503 remains
non-evidence. Keep this branch `NOT_OBSERVED` and the canonical J01 result
unaccepted unless a product/source change supplies the required truthful
observable window; do not weaken the source oracle or promote this run.

### B03 current auto-testing result and mount-admission diagnosis (2026-10-23)

B03 run [39931810677/1](https://github.com/rustfs/rustfs/actions/runs/39931810677)
used candidate `1a3f038c90704f9ad9693eddcf7d861c9530f13c`, package
`39898678364/1` (SHA-256
`33f97e40add31ebe9bc4e923f68bc11d2e8855b4eedf5aad7bc8fce1f07da9d6`),
and auto-testing commit `dd53393e6ee713f5831254be92a7a90ea40bdf62`.
Official artifact `12491823352` has SHA-256
`df3770cf8f2b9a8ba22e6cc2e5064f96c030121e5bfe105f5de2a894adacad01`.
All four H1/H2 × DB/DV instances retained FAIL and cleanup PASS. Their
registered 90-second format observations never found the replacement
`format.json`; candidate journals repeatedly report
`Replacement target is not ready: target mount admission failed`.

I re-downloaded the official artifact and checked its runner storage preflight,
H1 deployment preflight, replacement receipt, post-start format observations,
and candidate journal. The runner had about 258 GB free at preflight, and the
four parent mounts were four distinct NVMe devices; this rules out the old
disk-space concern for this run. The deployment record lists the actual
endpoint roots under each `/data/rustfsN` mount, but does not establish a mount
lease for those run-private endpoint paths. The service journal confirms that
`renew_disk` attached the empty endpoint and queued the replacement request,
which then failed admission. Thus the missing capability is endpoint mount
binding and safe mount switching, rather than capacity or missing replacement
bytes.

The runner gives RustFS endpoints under
`/data/rustfsN/heal-validation/<run>/data`. The parent `/data/rustfsN`
mounts are independent devices, but the candidate's replacement admission
requires a valid mount lease for the exact target endpoint and each local
sibling endpoint, plus target physical-device IDs disjoint from `/` and those
siblings. The run-private endpoint directories do not meet that exact-mount
contract. The service logs show the actual sequence: `renew_disk` attached the
empty endpoint and submitted an automatic replacement request; the task then
started and repeatedly failed with `Replacement target is not ready: target
mount admission failed`. `RUSTFS_HEAL_AUTO_HEAL_ENABLE=false` only disables the
periodic scanner; it does not suppress this replacement request generated by
`renew_disk`. The executor's `wait_format()` therefore expired after 90 seconds
and the explicit manual `run_task()` call was never reached. The source
procedure and strict UUID/set-slot and physical-byte oracles remain unchanged;
do not lengthen the wait, copy from quarantine, or infer a product verdict from
this environment mismatch.

Before another canonical attempt, add a read-only preflight that checks every
local endpoint's exact mount lease, mount ID, filesystem identity, and physical
device separation from `/` and its local siblings. The replacement adapter
must then demonstrate a candidate-supported way to switch the target endpoint
to a blank mount, let the candidate obtain a new lease and initialize it before
the manual token, while keeping the old archive unreadable to the service. A
bind mount may be used only if the adapter proves this full lease switch and
all candidate device-identity checks; a bind mount by itself is not evidence.
The Functional Chain now runs a read-only `replacement-admission-preflight`
before `stop_existing`, candidate package download/install, the normal
parent-mount preflight, reservation, service start, or fixture seed. It
inspects all H1/H2 endpoints and records each exact mountpoint, mount ID,
filesystem identity, nearest existing ancestor, physical-device IDs and
root/sibling overlap, along with whether a reviewed mount-switch adapter is
registered. If admission is unavailable, the result is `BLOCKED` with
candidate package and client provenance, `binary_sha256: null`, and an
explicit `NOT_REQUIRED_NO_MUTATION` cleanup disposition; the existing RustFS
package and service remain untouched. If admission passes, `Fleet.deploy()`
repeats the read-only check immediately before reservation. The current runner
reports ordinary run directories and no adapter, so B03 stops before bootstrap
and any 90-second format wait. Once an adapter is installed and independently
reviewed, the gate must pass before recording mount identities before and
after replacement and rerunning the full H1/H2 DB/DV matrix. The source oracle
and prior FAIL evidence remain unchanged.

The new pre-deployment gate was exercised on the current auto-testing
environment in B03 run
[39942624978/1](https://github.com/rustfs/rustfs/actions/runs/39942624978),
using the same candidate package and helper commit
`49443e7559578cba8be285ed723bb68dfbaf8656`. H1 and H2 both returned
`BLOCKED` before deployment: all endpoint paths were absent ordinary
directories beneath the four distinct NVMe parent mounts, with no exact
endpoint mount IDs or filesystem identities and no reviewed mount-switch
adapter. Root and sibling physical-device separation passed, but that does
not satisfy the endpoint lease contract. The case result records
`execution_stage=pre-deployment`, `binary_sha256=null`,
`mutationStarted=false`, deployment and fixture seeding skipped, and
`cleanup=PASS` with `NOT_REQUIRED_NO_MUTATION`. The case result SHA-256 is
`68ed1713fa3eab0e0d2d972369fca4af00ae58bdb4e6a1e64b072cfae6194621`; the
preflight summary SHA-256 is
`fb47d91ee77d21104be7a400bbfda1aa0d73b30e090476c853a9a53b5036ea12`.
This is verified BLOCKED evidence only: it adds no canonical PASS and does
not replace the retained B03 live failures. The next valid B03 attempt still
depends on installing and reviewing the mount-switch adapter.

B04 `heal.disk-replacement-unformatted` now uses the same replacement
admission contract. Its runner checks H1/H2 exact endpoint mounts before
stopping any service or installing the candidate, and its executor repeats
the read-only admission immediately before reservation. If the current
ordinary-directory layout remains in place, B04 is recorded as `BLOCKED`
with `binary_sha256=null`, `cleanup=PASS`, and
`NOT_REQUIRED_NO_MUTATION`; no blank or foreign-format fixture is created.
If the first gate passes but the reservation-time recheck observes environment
drift, the executor records the same `BLOCKED` verdict while retaining the
installed candidate provenance, skips reservation and fixture seeding, and
finishes cleanup. That post-bootstrap block is distinct from the
pre-deployment `NOT_REQUIRED_NO_MUTATION` result and cannot claim canonical PASS.

B05 acceptance uses candidate `7ed6c9320db491c84b54e8232152d7b1282c1b3f`, helper `76dbf7029e82c1e3878c9160090804d4afc38078`, package `39827676705/1` and tools `39841369555/1`. The complete official ZIP digest is `4eb3405af1f47583cd7c2d8e5b9ce9fa6910b76665df529d7dfbe4f1399a20d8`. Independent recomputation covers seed ACKs, full versions/markers, the actual 2+2 and 12+4 role/loss matrices, every endpoint's full online membership before each required boundary, sequential faults, one unique START/token per instance, two identical terminal outcome queries, strict integer scope, exact repaired/healthy responsibility, all physical shard and bitrot proofs, complete S3/HEAD/Range evidence before finalization, ten private-node cleanups and four package purges. No backup restoration was used. Original failure `38200772548` remains unchanged; its vm001 offline root cause is not established by the later pass.

E04 live [39842352595/1](https://github.com/rustfs/rustfs/actions/runs/39842352595) retains FAIL/cleanup PASS. All 1,402 official files, three H1 branches and five node cleanups were independently verified. H2's healthy Scanner task completed on vm002 while the exact node3 token query was forwarded to vm000 and returned notFound. RustFS commit `553be4b4c62ae0c590e82f91817bc018bdfdc660` lets authenticated original local automatic-task tokens return the source/path-bound in-memory snapshot; Admin/Internal/unknown/alias identities retain coordinator routing. Fixed candidate `9d90884c304dcfbf97057c263c6f2ee110230ce7` adds only that reviewed patch to `7ed6c932`, with unchanged Cargo.lock. Package [39851206784/1](https://github.com/rustfs/rustfs/actions/runs/39851206784) and matching tools [39860218641/1](https://github.com/rustfs/rustfs/actions/runs/39860218641) passed independent complete provenance and byte audits.

The subsequent E04 live [39860501067/1](https://github.com/rustfs/rustfs/actions/runs/39860501067) retains FAIL/cleanup PASS. All 1,795 official files, H1's three branches, H2's cycle-boundary branch, five node cleanup receipts and four package purges were independently verified. H2's missing-shard and bitrot branches failed attribution: real repair tasks ran on remote disk workers while complete Scanner cycles were logged on vm003. The old same-node cycle requirement omitted both exact tasks and queried later healthy tasks. Independent replay binds each missed task to its original remote Scanner RPC request and cycle, and verifies restored physical bytes before rescue; neither missing structured receipt is retroactively accepted. The corrected executor retains the task owner for exact token queries and requires the remote RPC identity, Deep disk scan, matching positive cycle number and one unambiguous complete leader cycle. Invalid or duplicate identities, wrong/failed/ambiguous cycles and healthy substitution remain rejected. The later complete six-branch acceptance of run 39863505276, detailed below, raises canonical acceptance to 110/158 while retaining this original failure.

I05 progress (2026-10-22): four-node startup scene `39764496504/1` was
independently verified and isolated by
[39766457981/1](https://github.com/rustfs/rustfs/actions/runs/39766457981),
preserving all 102 original artifact files and twenty same-device private
archive parents. Read-only inspection identified broken kernel-selecting
bpftool wrappers on vm002/vm003; the exact already-installed executable is
now digest-bound and checked before reservations. Fresh diagnostic
[39767336741/1](https://github.com/rustfs/rustfs/actions/runs/39767336741)
completed H1 and H2 on helper `8cbdd51`, with all five node cleanups passing.
Independent audit of 211 artifact files verified exact 1001 ms write-lock
timeouts, closed caller sockets, unchanged physical content, full diagnostic
seed ACK/peer inventories and final FD counts at or below baseline. The
original canonical result remains `BLOCKED/cleanup PASS`; source DL/DV,
exact disconnect/native fences and stale-generation coverage remain outstanding.
Independent queued-successor diagnostic acceptance is recorded below. Exact-helper native/tools
[39783997956/1](https://github.com/rustfs/rustfs/actions/runs/39783997956)
passed independent audit. The initial successor live
[39788321908/1](https://github.com/rustfs/rustfs/actions/runs/39788321908)
failed at internal bucket metadata-read classification despite one
unretried 200 PUT ACK. Its stopped H1 scene was preserved by
[39789732916/1](https://github.com/rustfs/rustfs/actions/runs/39789732916).
The parser fix retains exact request/owner/budget/lifecycle checks and has
a regression over the original fifteen events. Fresh helper-bound tools
[39791663588/1](https://github.com/rustfs/rustfs/actions/runs/39791663588)
passed independent archive/source/native audits. Live
[39798325977/1](https://github.com/rustfs/rustfs/actions/runs/39798325977)
then reached the new acknowledged generation but invoked the stopped-only
shard verifier on a running service. Its original FAIL/cleanup FAIL and
71 files were preserved unchanged by isolation
[39799657074/1](https://github.com/rustfs/rustfs/actions/runs/39799657074).
The controller now finishes all online peer/FD observations before stopping
the service and checking streaming shard integrity, with matching physical
snapshots before and after verification. Tools
[39803149039/1](https://github.com/rustfs/rustfs/actions/runs/39803149039),
bound to helper `6fbc3c4`, passed independent official archive/source/native
and inspector audits. Live
[39810035277/1](https://github.com/rustfs/rustfs/actions/runs/39810035277)
completed H1 including stopped integrity and cleanup. H2 started and queued
its PUT, then the parser treated a same-target slow-acquire WARN as a lock
lifecycle row. The original result stays FAIL/cleanup FAIL. Isolation
[39811143272/1](https://github.com/rustfs/rustfs/actions/runs/39811143272)
preserves all 180 original files and twenty same-device private archive
parents; every four-node pre/move/post tree, device and inode baseline
matches. The parser now recognizes only the exact auxiliary warning bound
to an already acquired target write; it cannot replace lifecycle evidence.
The original sixteen-event H2 trace and incremental prefixes are covered by
positive and negative regression tests. Exact helper `3012a65` tools
[39812960916/1](https://github.com/rustfs/rustfs/actions/runs/39812960916)
passed independent archive/source/native and inspector audits. Fresh successor
[39817606025/1](https://github.com/rustfs/rustfs/actions/runs/39817606025)
completed H1/H2 and all five node cleanups. Independent audit byte-matched all
246 official files, nine seed request/ACK pairs and eight final versions/markers
per topology, one successor ACK, historical generations, lock events, every
peer inventory, ACK-bound stopped physical invariance and four/sixteen streaming
shard receipts. FD counts returned to or below each peer's baseline. The original
result remains `BLOCKED/cleanup PASS`; exact disconnect-edge ownership,
distributed release, global no-leak and full source DL/DV coverage are unproven.
That I05 diagnostic added no acceptance. The separate complete B05 audit brought the historical total to 109/158; the subsequent E04 audit brings the current total to 110/158.

Fresh D03 [39801936817/1](https://github.com/rustfs/rustfs/actions/runs/39801936817)
retains FAIL/cleanup PASS. Independent H1 re-evaluation verified all four
branch physical and strict version-responsibility oracles, sixteen seed
request/ACK pairs, fourteen versions/markers, 48 HEAD observations and 144
range payloads. H2 failed before seeding when a `pool.bin` write returned
`SystemMetadataRepairRequired` and the pool transaction remained blocked
at `transaction_unknown/prepare_cas`. All 392 original files and five node
cleanup receipts were independently checked. The exact candidate's bounded
owner-repair retries only cover identity metadata, so ordinary init retries
do not discharge this `pool.bin` responsibility. The initial replica/network
cause remains unproven; no D03 historical failure is disproved or promoted.

[J02 diagnostic 38874900846/1](https://github.com/rustfs/rustfs/actions/runs/38874900846) completed all 20 H1/H2 × DB/DV branch instances on that exact candidate. Sixteen branches report PASS; the four original all-metadata-corruption probes report permanent Failed rather than Unknown and retain NOT_OBSERVED. Independent archive and provenance audits passed. Independent mixed-branch audits verified all four repaired shards against original complete bytes, exact receipts and counters, surviving below-quorum failures, original terminal preservation and post-rescue S3 inventories. All 50 node namespace finalization receipts passed audit. Real legacy writes separately produce Unknown with zero repairs and unchanged physical files, but do not replace the original source procedure. Full J02 canonical acceptance remains blocked; these partial results add no canonical acceptance.

A10 run `39197088183/1` uses immutable helper `b16e6072679d6a60a044b9d4520b30b4a79b5557`, candidate `072b57b37559fbe626aeda8abf61617fdcbdd926`, package `38043283428/1` and inspector `38715612825/1`. All nine source variants across H1/H2 and DB/DV passed their 68 required execution instances. Independent audit checked original SDK request/ACK hashes and version/marker identities, physical set selection, exact task options/receipts/counter partitions, 27,096 complete shard-streaming proofs, physical closure before target GET or control rescue, complete peer inventories and all 170 node cleanup receipts. Full inventories are checked against ACK manifests; their endpoint iteration is established by the immutable executed helper source, since the inventory documents omit endpoint labels. The remove=true branch replays one complete below-quorum original object after exact DELETE and requires whole-orphan removal with unchanged live controls. Rescue restores only post-DELETE authority. The original failed A10 result remains byte-identical.

The sections below retain earlier implementation and run history. Historical failed or partial results are not promoted by this summary.

## E04 complete acceptance and A05 cancellation regression

Fresh E04 live [39863505276/1](https://github.com/rustfs/rustfs/actions/runs/39863505276) passes complete independent acceptance on helper `cd36287f1abe42662333fab31cf45ac23de91c8e`, candidate `9d90884c304dcfbf97057c263c6f2ee110230ce7`, package `39851206784/1` and tools `39863244404/1`. All 2,307 official archive files were byte-checked against ZIP SHA-256 `ecbd537cb6422cc3af631d4a7d1145040ffd30c20da0008778e442752e0f193e`. All six source branches passed independently recomputed seed ACKs, healthy preflight, exact Scanner source/token/RPC/cycle binding, strict repaired responsibility, complete physical and bitrot proofs before S3 reads, 768 HEADs, 672 deterministic Range payloads, unchanged inventories and finalization without backup restoration. All five node cleanups and four package purges passed. Independent complete audit SHA-256 is `c96ab5fc91b13fecb475a0cee86c85a3f5c983ce125b912b4485f7cd080d3243`. This raises canonical acceptance to 110/158 and preserves every earlier failure.

A05's cancellation after successful bucket storage I/O is deterministically reproduced at the receipt boundary: the unchanged code rejects the late positive receipt but falls back to Unknown. RustFS commit `78c803e1efd67cc04edc8fc4d08a8ea75af77ddf`, pushed to the existing PR, retains the positive-receipt rejection and routes observed cancellation through the existing typed Cancelled outcome, with partial coverage and zero positive or Unknown responsibility. The exact regression failed before the fix and passes after it; all 153 task tests and Rust formatting pass. This establishes the code defect and local regression coverage, not the exact historical live interleaving or a new A05 canonical result. Fresh immutable candidate and complete A05 live acceptance remain required.

## Supplemental J02 evidence audit

The 2026-10-23 supplemental independent audit closes twelve positive branch instances from original J02 run 38874900846: grace Deferred/new-token cleanup, read-only terminal failure/new-token repair and dry-run across H1/H2 and DB/DV. Every original archive member was byte-checked again, all fixture requests/ACK digests and version/marker manifests were rebuilt, and the exact executed helper was pinned.

Grace uses both original ACK versions, natural 120-second expiry under unchanged candidate processes, the exact typed future retry time, two unchanged original-token terminal queries and every H2 peer, a distinct compensating token, physical post-DELETE authority before GET/rescue and complete peer inventories with old-version refusal. Read-only verifies actual private-namespace EROFS, the protected-control incarnation, unchanged failed and released scenes, a distinct repair token, original full shard/bitrot evidence before GET/rescue and all peer inventories. Dry-run retains every original typed disposition, the actual R-1 fault geometry and unchanged fault bytes. All twelve node cleanup groups bind their original reservations.

Together with the earlier four mixed-branch audits, sixteen branch instances now have independent acceptance; the four original opaque-metadata Unknown probes still prevent whole-case acceptance. The supplemental report SHA-256 is `4424b3e681931527d244a4ca0211dde534a8145cd95ea0b460f388262ccb8c10`. This J02 audit preserves historical results and adds no canonical acceptance; the current total is 110/158 after the separate E04 audit.

## Original reuse assessment

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
case evidence. The 154 currently dispatchable IDs include all 74 non-Heal cases, 67 automatic Heal executors, and 13 disabled-by-default environment gates. The gates preserve the operator procedures and emit explicit BLOCKED results; they are not automatic live executors or PASS claims. The preceding census confirmed 101 unique complete canonical live PASS cases: 85 on source `072b57b37559fbe626aeda8abf61617fdcbdd926`, 12 on the older immutable source, two on source `16cf19817e0c81e24d0d787a916f734a34517ec5`, and two on source `e224eb48e5311cff022b04fd78c97a8572d5c270`. Its latest additions were C08 metadata/payload mismatch `38235978152` and E03 read-repair attribution `38237270640`. The independently verified Pool tail runs `38110959836` and `38111198632`, A03 run `38198122041`, C04 run `38203454300`, the completed multipart run `38204339623`, D07 run `38205235080`, D09 run `38205854024`, D10 run `38207381816`, and manual-normal attribution run `38207956101` are included. Native, read-only, recovery and duplicate runs are excluded; B07 run `38727279347` subsequently raises complete acceptance to 102, leaving 56 cases without complete canonical live PASS.

All 84 Heal recipes, 302 operator branches and 534 applicable topology branch
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
Four Heal cases retain a canonical acceptance gate: C11, F07, I05 and J02. All have dispatch routes. All four have explicit diagnostic routes that preserve their blocking canonical verdicts; I05's H1/H2 timeout and queued-successor diagnostics have independently audited live results, exact-source native prerequisites and stopped-scene isolation. I05 now composes both H1/H2 controllers with separate same-artifact native owner and full-content/physical generation checks. The native proof does not establish that the fleet request held a distributed lock at disconnect, and the composed source plan still requires independent complete live audit. The 13 environment-gated cases have disabled-by-default control flows and oracles in `environment.py`; actual execution requires an assigned and reviewed auto-testing resource adapter, and canonical results remain review-gated. See [the environment gate reference](validation-heal-environment-gates.md) for each missing resource, capability and native barrier.
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

J01 selects the official rc v0.1.36 Linux amd64 musl release in a new private
artifact directory. Both archive and executable SHA-256 values are fixed and
checked before execution; its manifest records the release source, target,
version output and exact bytes. This release includes the existing CLI fix
that preserves incomplete/unknown status in human-readable output. The musl
build avoids depending on the runner's glibc version. Other Heal cases retain
their existing client selection. The original failed J01 result is retained;
the complete raw/CLI startup, disabled, missing-peer and recovery observations
still require live acceptance with the fixed client.


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

B01 run `38081858808` retains its original FAIL and cleanup PASS. Both H1/H2
had real same-service read/write EACCES. Independent replay of all 46 H1 and
45 H2 original Admin responses finds the exact original slot, endpoint and UUID with
`state="drive access denied"` and cached `runtimeState="online"`. The candidate's
`DiskAccessDenied` error renders that exact state, and its Admin disk-count
logic counts it as offline. The executor omitted this spelling from its
explicit error set, so it rejected the recorded witness and never started
degraded ACKs. The state mapping now recognizes that exact error while still
requiring the original identity and every other member's healthy UUID. Unknown,
initializing, unrecognized error and online-only observations remain refused.
Replaying this offline check does not supply the unexecuted ACK or repair
branches; complete fresh H1/H2 live evidence is still required.


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
the focused checks. This implementation section predates the complete B08 acceptance above; its earlier no-PASS statement remains historical.

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


A10 dispatches all nine source option branches on H1/H2 and fresh DB/DV
namespaces. The frozen source `524eb5b32c3236497fdd8a9271df0494fa3d9974`
executes its proven orphan only with `recursive=true,remove=true`; its
`remove=false` branch independently heals a repairable ACK-bound object.
The former migration added four synthetic remove=false orphan baseline
instances. Those extra instances are outside the source procedure and have
been removed; all original branches, both topologies and both fixture types
remain mandatory, for 68 execution instances. Other option probes retain
baseline/selected pairs changing one option or the pool/set selector pair.
Exact candidate Admin/selector and validation-source hashes are recorded;
this is API coverage. H2 scope probes use two actual eight-drive sets across
the original four nodes. Recursive probes include child/adjacent/outside
controls; selectors bind every ACK version to its measured physical set.
Historical versions, markers, exact responsibility, counters, retained settings
and structured rejections remain required.

The source-aligned remove=true probe seeds one orphan ACK version and retains
all control versions/markers, streams every original shard, archives the full
original tree, performs an exact DELETE, and proves S3 and all-member physical
absence. It then replays the complete original object including xl.meta and
integrity sidecars on exactly one below-quorum member. A recursive bucket
sweep must remove the whole orphan object before GET or rescue and leave
all post-DELETE controls/formats unchanged. Every original orphan/control
version must have an exact complete canonical receipt and counter partition;
only the removed ACK may report repaired or authoritatively_absent. Rescue
restores only post-DELETE authority. Lost mutation ACKs and incomplete restore
retain DIRTY namespaces. The old data-only/exact-object construction and its
original live failures remain archived; they do not establish a source remove
product failure. No new live or complete canonical PASS is claimed here.

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

J02 now has a non-dispatched orchestration prerequisite in `outcome.py`. It reuses the existing Fleet, Probe, Recorder and physical fault backends for H1/H2 and DB/DV: one mixed healthy/repaired/below-R task, a separately admitted dry-run task, and an exact metadata-dependency refusal. Complete ACK identities, the actual R-1 geometry, original incarnation, typed dispositions and exact canonical counters are reconciled before fixture rescue; the original token/outcome is checked again after rescue. The reviewed candidate's four failed bucket-object attempts remain visible, and legacy `objectsHealed` is never used as proof of repair. Unknown native evidence remains a prerequisite. Its new live executor first verifies an ACK-bound protected control and original incarnation, switches only the owned candidate write mode through a durable one-way transaction, writes real SDK legacy objects, and requires every physical replica to show absent integrity descriptors. The original Deep task must report exact Unknown identities/counters and zero repairs for those objects, preserve the protected control and all original physical bytes, and remain unchanged after subsequent GETs. A fresh candidate-bound inspector build and complete live evidence remain required; this implementation is not a canonical PASS. These control flows have local negative-contract coverage but no live execution claim; J02 remains outside dispatch and the original corrupt-metadata Unknown probe, version-specific grace Deferred source procedures remain outstanding. The source readonly failure/new-token compensation branch is now implemented and requires live verification. Unknown is counted within skipped, not as repair; even if every current diagnostic branch passes, this executor emits complete=false and canonicalCasePassClaimed=false until those source procedures are covered. The 84 cases, 301 branches and 532 topology templates are unchanged.


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

Immutable package run 38240858236/1 and tool run 38244986993/1 succeeded for source 1aa9be87d18cf941e5020f19e8622c26ba4b10ac. Independent Git-object checks verified the exact source tree, lock and all source contracts; all seven MRF sources match the reviewed reference byte-for-byte. The complete 68,625,642-byte DEB digest is b99dd915376b51f0896fc346ca4642b7d26bf3352135bb5c6f60f8179d865761 and its contained binary digest is 0103ca46386396ed15f27b9efbe4196fe62bd9e93033671a88f3b3e944602cf4. Inspector, nine native storage regressions and fifteen manager regressions passed independent artifact consumers for harness d5a41739a7a544d93ba71f51583d45ad4e6161eb. A new immutable tool build is required for the updated replay harness; older harness provenance is not accepted. The BLOCKED writer preserves unverified migration status, original provenance and no PASS assertions. A phase identity in the process environment detects interrupted checkpoint transitions even when product configuration knobs are identical. Independent stopped censuses are retained before capture, after detachment and after installation to compare each original byte and absence witness. Read-only environment run 38251226817 confirms 16 distinct eligible endpoints on the four current Heal hosts for H1/H2; H4 remains unavailable.

The final full Heal contract suite passed 593 tests with eight platform skips; all 30 focused incarnation tests, the unchanged 84/301/532 matrix, AST, bash syntax, actionlint and whitespace passed on the final code. Two sequential final reviews covered correctness, simplicity, test coverage, path/artifact security and persistence/cancellation. Explicit diagnostic execution cannot claim canonical PASS; clean finalization does not hide a failed branch. C11 remains outside automatic dispatch and complete canonical PASS remains 101/158 pending live execution and independent audit.

F07 now reuses its independently verified ten-test candidate-bound native scope as PREREQUISITE_PASS without a fleet topology or DB/DV claim. The remaining three live flows and full review of the required fsync/commit cuts remain blocked; existing publication interruptions are not relabelled as individual fsync or power-loss proof. The exact 1aa9be87 candidate is accepted only after all six checkpoint route/source contracts were independently proven byte-identical to the reviewed reference. Thirteen focused persistence/artifact tests passed. Canonical PASS remains 101/158.

F07 committed MRF corruption now has a non-dispatched H1/H2 DB/DV executor in persistence_mrf.py. It creates real member permission denial and ACK-bound original durable obligations, freezes complete committed payload/manifest/lifecycle bytes, corrupts only each actual payload CRC through the existing stopped journal adapter, and requires a source-matched corruption diagnostic on every mutated process. All unknown bytes and original commitment/lifecycle responsibility must survive startup, independent Admin rescan, complete shard checks and all-peer business verification. Rescan repair never retires or relabels the original corrupted MRF responsibility. Lost injection acknowledgements register recovery before mutation and retain dirty namespaces. Sixty-three focused contracts and the final 599-test Heal suite passed with eight platform skips; AST, actionlint, whitespace and 84/301/532 passed. Two sequential reviews found no unresolved correctness, simplicity, security or persistence/cancellation issues after the focused failure probes. This flow has not run live; F07 still lacks stale-generation, journal-write-failure and complete native cut review. C11 live execution uses the separately frozen 891f264f39ffb608f1898fef151b273e205321b5 harness and its own rebuilt tools. Canonical PASS remains 101/158.

C11 first live diagnostic and retained-scene recovery (2026-10-12)

Tools 38253155969/1 passed inspector, nine storage regressions and fifteen manager regressions; exact source/tree/lock, collector bytes, binary digests and original results were independently verified against harness 891f264f39ffb608f1898fef151b273e205321b5. Diagnostic 38257435494/1 stopped in H1 old-metadata-rejoin: all eleven original versions/markers had explicit deletion ACKs, but DeleteBucket returned 500/InternalError with File access denied while one member was isolated. Its FAIL and retained namespace are preserved; no later branch ran and canonical PASS stays 101/158. Recovery accepts only this exact known refusal, a complete digest-bound request/ACK ledger, a restored member gate and the unchanged stopped candidate scene. It exports the retained system records and rechecks the physical manifest immediately before finalization. Unknown calls, missing ACKs, changed provenance, other bound namespaces, replay transactions and scene drift are rejected. Recovery never converts the product failure into PASS.

C11 recovery 38259016213/1 completed with clean finalization. Independent audit compared both stopped physical censuses with the original failed artifact, checked the candidate binary, restored gate, retained system-record integrity and credential boundary, and verified the cleanup receipt. The driver's new merge commit preserves the Functional Chain and validation-tool workflow bytes exactly. The original DeleteBucket FAIL remains unchanged.

F07 original stale-generation replay now captures an actual earlier product checkpoint before repair, proves later durable progress in the same task/incarnation/physical owner, crashes the exact owner and installs only the saved earlier bytes through the existing journal adapter. An independently stopped census verifies the injected checkpoint and unchanged resume companion. A valid record may replay safely; the oracle requires complete ACK accounting, original durable terminal/report, full physical repair and shard integrity before business reads, an independent unchanged rescan and every peer's original task/S3 view. Explicit rejection instead follows the retained-record failure/rescan path. Neither path claims the stale in-memory manager CAS or individual fsync barriers. Unknown fixture/admission acknowledgements retain the namespace. Cleanup never resurrects a completed checkpoint; original-byte evidence and complete repair proof precede dropping rescue registrations. F07 has an explicit diagnostic route, while its canonical matrix remains evidence-blocked because journal write-failure and complete native cut coverage are incomplete. A clean recovery cannot hide an implemented branch's FAIL.

The final stale-generation change passed 608 Heal contracts with eight platform skips, 91 focused persistence/checkpoint/journal/manager/environment/migration checks, the unchanged 84/301/532 matrix, Python AST, shell syntax and whitespace. Two sequential final reviews retained exact bytes, task and owner bindings, rejected unknown calls and delayed rescue removal until complete evidence export. No unresolved finding remains for this executor diff; no live F07 PASS is claimed.

F07 original checkpoint write failure now has a diagnostic executor using the existing four-node `journal-bounded` profile. It fills only the four fixed private journal volumes while the candidate is stopped, then proves a candidate UID and mount namespace witness for the real `ENOSPC` write. The original checkpoint, resume bytes, root owner, exact failed task outcome and source-matched `Heal task failed` event remain durable. The executor pauses and records every owner before releasing only its private filler, performs a distinct Admin rescan with a new token, verifies complete physical, shard and peer S3 evidence, and preserves the first failed attempt separately from any later same-task retry. Cleanup keeps rescue state when a fill ACK, release, evidence export or reconciliation step is incomplete. The branch is implemented but remains outside canonical dispatch until a live run and the remaining reviewed native persistence cuts complete. The final full Heal suite passes 615 tests with eight platform skips; 21 focused persistence contracts, the catalog, 84/301/532 matrix, Python AST, shell syntax and whitespace pass. Two fresh sequential reviews cover correctness, simplicity, test coverage, path security and durability. Frozen process-bound censuses and decreasing root timeout budgets retain their original identity checks. Canonical PASS remains 101/158.

F07 diagnostic 38261190390/1 retained FAIL with cleanup FAIL. Checkpoint observation rejected the candidate's actual ordered JSON digest; the MRF windows failed on H1, and H2's first CRC injection was rejected because its actual committed payload also contains system metadata responsibility. No live branch PASS is claimed. The rejected injection left all original MRF bytes and object files unchanged and created no intent or backup. Recovery now accepts only that exact first H2 DB refusal, seven continuous digest-bound create/PUT ACKs, unchanged original MRF and physical censuses, and a cleared member gate. Every node is audited before finalization; each node rechecks its stopped candidate, empty private quarantine, exact MRF digest and full physical scene immediately before removal. Unknown calls, missing ACKs, changed scene, any injection evidence or backup retain the namespace. The original FAIL is never rewritten.

F07 recovery 38266595027/1 passed independent audit: all four stopped physical scenes and original MRF bytes matched, every node passed its no-injection audit before finalization, and every unit/root cleanup receipt passed. The original failed result is byte-identical and remains FAIL. The checkpoint observer now verifies both source-defined serde_json map representations, including the full server's unified preserve_order feature, while sorting the source-defined ledger arrays before SHA-256. A pinned unmodified candidate fixture rejects counter, cursor, incarnation and digest tampering. The MRF adapter now accepts the exact known internal metadata companions alongside active original business work, rejects foreign business or IAM responsibility, and corrupts one latest committed replica per process with every untouched replica retained as a control. Stopped original bindings replace earlier live sequence observations. The final 621-test Heal suite passes with eight platform skips; 55 focused persistence/MRF contracts, AST, catalog, 84/301/532 matrix and whitespace pass. Two sequential final reviews found no unresolved issue in these ownership, digest, injection and retention changes. H1's unobserved durable MRF window and all original live failures remain retained pending a fresh run; canonical PASS remains 101/158.

The F07 future-schema fault now recomputes the embedded integrity digest after changing schema 8 to 9, using the exact map representation verified in the original candidate checkpoint and the source-defined sorted ledger arrays. Original task, incarnation, options, cursor, counters and ledger order remain unchanged; an invalid original cannot be re-signed. Both the unmodified full-server fixture and sorted-map fixture independently verify the future record's digest. Diagnostic 38268715777/1 pins the preceding harness 6db679f and cannot validate this correction. Its future-schema result must not be credited as valid unknown-schema coverage even if its retained-record checks pass. Canonical PASS remains 101/158.

The fixed `1aa9` candidate already contains six deterministic directory-fsync failure regressions in `disk::local::test`, and the verified original ecstore binary inventory includes all six exact names. `persistence_native.py` now executes them as an additive F07 prerequisite using that verified full candidate binary and its original nine-test suite evidence. The collector retains original one-test logs, exact commands, candidate/tree/lock/binary/run/attempt/helper identity, its own digest and strict scope limitations; source, command, artifact, inventory and log substitutions are refused. The hosted tools lane archives the supplemental evidence separately. These tests cover actual post-rename or post-delete directory-fsync errors, manifest `StorageFull`, recovery-anchor preservation and the relaxed-mode system metadata durability rule. They do not cover individual file-fsync interruptions, physical power loss, all checkpoint/commit cuts or canonical fleet branches. Hosted execution and independent original artifact audit remain pending; `requiredNativeCoverageComplete` stays false.

Mixed MRF payload faults select only an owned business record's CRC byte. Record boundaries are independently matched to each decoded original intent digest; internal metadata companions remain byte-identical even when they appear at the end of the payload. UTF-8 keys and variable-length marker purge records have explicit framing coverage. Retention verification recomputes the same owned target from the frozen bucket binding. The final 621 Heal tests pass with eight platform skips after this scope restriction; both final reviews retain exact original bytes, incarnation and recovery boundaries.


F07 diagnostic `38268715777/1` completed with FAIL and clean finalization. Independent audit verified all twenty live branch instances, fifty node cleanup receipts and 240 embedded original-byte witnesses; no diagnostic branch or canonical PASS is credited. The checkpoint task can finish in roughly three seconds: an extra live root census missed its actual checkpoint window. The observer now freezes the exact checkpoint process before reading and validating the runnable root, and rechecks both original records before crash/mutation. Live read/capture races retain a structured `NOT_OBSERVED` refusal across SSH within the existing deadline; stopped operations, changed process identity and path/ownership failures remain fatal. MRF corruption restart now re-arms its same member permission gate while stopped and reactivates it after readiness, avoiding disk initialization with an already active chmod denial. Original corruption, physical scene and responsibility checks still gate all later steps. Seventy-four focused contracts and 631 Heal contracts pass with eight platform skips; two fresh sequential reviews found no unresolved findings. A new immutable diagnostic run is required; H1's missing committed MRF window remains unverified.

Hosted tools `38272891095/1` passed the original nine exact storage regressions and all six supplemental directory-fsync tests. Independent consumers verified candidate `1aa9`, source tree/lock and source-file Git objects, executable/collector identity, exact one-test commands and original logs. These are scoped native prerequisites; individual file fsync, physical power loss, complete required native cuts and canonical PASS remain unclaimed. Isolated package publication `38271092352/1` also passed: fresh hosted publisher bytes equal the opaque build artifact, the complete public DEB digest matches schema 3, and the existing candidate resolver accepts its source/run/attempt binding. Current fleet candidate `38240858236/1` remains fixed.

B07 fixed-candidate replay `38275626918/1` remains FAIL with clean finalization. Independent audit binds all four H1/H2 DB/DV instances to `1aa9` source, package/binary and helper `e62c3cc`. H1 GET still returns `500/InternalError`: reader setup reports one available shard against two required shards and reduces missing local files to a generic I/O error. H2 DB GET returns `503/SlowDownRead`, but Admin completion records `deferred/dangling_delete_grace`, zero failed objects and completed execution instead of the frozen failed responsibility. H2 DV startup misses readiness with system metadata read quorum unavailable. Original failures remain retained. Canonical PASS stays 101/158.


F07 diagnostic `38277312274/1` pins helper `c0d69a3` and the unchanged `1aa9` candidate. It retains FAIL/cleanup FAIL after twelve live branch instances and two native prerequisites; later H2 instances were not executed. Independent audit verified 156 embedded original-byte witnesses. H1 checkpoint truncation and future-schema injection now reach actual stopped original records and durable failed terminals, but report reconciliation still fails; this is not canonical PASS. H2 DB MRF restart advances its committed records and repairs nine missing target files through its untouched companion replica; its exact corruption diagnostic is not observed. Cleanup correctly refuses old-record resurrection. Recovery now audits the complete acknowledged fixture, exact original corruption, current stopped scene and all private backup/intent/applied bytes on all four nodes before finalization, and repeats those checks before removing each dedicated namespace. It exports evidence without restoring advanced product records, preserves the original FAIL and rejects unknown writes or other mutations. The 634 Heal contracts pass with eight platform skips; two fresh sequential final reviews found no unresolved correctness, simplicity, coverage, security, durability or compatibility finding. Live retained-scene recovery remains pending.

B07 read-quorum error propagation now has a real external-part regression in the companion RustFS PR. Reverting the dominant-I/O return reproduces `Io(NotFound)` at R-1; the fix preserves object-bound `InsufficientReadQuorum` in both legacy and streaming setup. Exactly R and healthy reads return every original payload byte. All 123 reader tests pass on Rust 1.99. This is local product regression evidence; fixed-candidate live run `38275626918/1`, H2 deferred responsibility and startup failures remain retained, and canonical PASS stays 101/158.

F07 terminal reconciliation now accepts only the source-defined scheduler transition from an explicit null admission timeout to exactly 300 seconds with zero nanoseconds. The two reviewed candidate commits share the exact scheduler/config sources, and the dedicated fleet verifies `RUSTFS_HEAL_TASK_TIMEOUT_SECS=300` in the actual process environment, including a fresh witness after fault restart. Every other option remains exact; explicit or checkpoint-consumed budgets cannot be refunded, and terminal/report JSON types must agree. The original H1 DB/DV truncation and future-schema records from `38277312274/1` reproduce all four prior reconciliation refusals and pass this derived retained-record check after the fix. Their original FAIL results remain unchanged; complete rescan, physical/S3 reconciliation and new live execution are still required. The 639 Heal contracts pass with eight platform skips. Two fresh sequential final reviews cover the exact normalization, source/runtime boundary, budget preservation and negative regression inputs without unresolved findings. Retained-scene recovery `38281391802/1` remains queued behind the existing shared fleet chain; no later fleet replay is started before clean recovery. Canonical PASS stays 101/158.

The original H1 DB/DV MRF logs show repeated committed-checkpoint publication failures and durable-admission fallback under whole-member DAC denial. The denial also hid system metadata before any readable committed anchor existed; extending the observation deadline cannot manufacture an original durable MRF responsibility. F07 now prepares its partial-write fixture with a permission gate on only the exact run-owned business bucket in the dedicated `mrf-crash` runtime. Actual candidate-namespace probes must show business read/write EACCES and successful system-format read plus exclusive system probe creation/removal under the same UID/GID and removed DAC capabilities, both before ACKs and after restart. Scope/path/owner/link checks and original device/inode/permissions fence activation and recovery; the existing whole-drive gate remains available to its other cases. Real committed ACK-bound responsibility, CRC injection, retained original records, diagnostics and complete physical/S3 reconciliation remain required. The 644 Heal contracts pass with eight platform skips; two fresh sequential final reviews have no unresolved correctness, simplicity, coverage, security, durability or compatibility finding. No new live result or canonical PASS is claimed before clean recovery and serialized replay.

Independent examination of the original F07 H2 DB stopped records in `38277312274/1` establishes eight valid committed replay sources on vm000, across four disks and both sequence 10 and 11 slots. The old single-replica fault corrupts only disk 0 / slot 0; seven sources remain complete with the same six responsibilities. The fixed candidate intentionally selects a healthy committed replica even when another is damaged, explaining the missing inspection-failure diagnostic. F07 now validates every original replay source on every bound responsibility owner before applying any CRC fault, including readable older slots. Foreign or empty responsibilities refuse construction; every apply is registered for rescue before invocation, and a missing or substituted receipt prevents restart. Only owned business-record CRC bytes change, while manifests, lifecycle companions and other records remain exact retention controls. The original run remains FAIL/cleanup FAIL. The orchestration regression fails with the old single-replica loop; 108 focused contracts and the 648-test Heal suite pass with eight platform skips. Complete live execution, all native barriers and canonical PASS remain unclaimed.

The F07 node adapter now validates each exact disk/slot against its frozen committed owner, sequence, payload digest and active bucket incarnation. The all-source plan is validated before the first apply; the selected target still needs its original valid manifest/lifecycle, unchanged before-bytes and stopped candidate. Previously, the adapter decoded the whole cohort on every apply, so the first intended CRC fault prevented the second injection. A real filesystem regression fails at that second call with global revalidation restored, then passes all four disk/slot mutations and byte-exact original rescue after the fix. Global MRF observation remains strict and rejects the same damaged cohort; replica-scoped observations are explicitly marked and cannot certify complete responsibility. Held/accepted/unbound/foreign-incarnation older sources refuse preflight. All 112 focused contracts, 652 Heal tests (eight platform skips), ten migration tests, AST and whitespace checks pass. No new live or canonical PASS is claimed.

Recovery diagnostic `38704914473/1` ran on helper `9c01cb8` after the independent Fault-Tolerance workflow released the shared fleet lock. It refused before the first physical snapshot: vm000 disk 0 no longer contained its owner marker. Its original namespace records show nine absent run roots (disks 0-2 on vm000-vm002), seven surviving roots and all four state directories; every service is stopped. No MRF export, restoration or finalization occurred. This is externally drifted/incomplete evidence, not clean recovery or a case PASS. Namespace inspection now emits bounded regular-file owner fingerprints (raw/canonical digests and UID/GID/mode/inode/device), without marker contents or credentials, so surviving state can be compared with the original reserved receipts. Tables installation previously recursively changed ownership of the entire mount; its chown is now restricted to its own mnmd subtree. That known interference is not asserted to explain the nine missing roots. Original failures, surviving roots and native/canonical gates remain retained pending forensic inspection.


The 2026-10-15 source reconciliation restores the omitted J02 `readonly-terminal-failure` branch from frozen source script `524eb5b32c3236497fdd8a9271df0494fa3d9974`. Heal retains 84 canonical cases; its operation inventory is now 302 branches and 534 applicable topology templates. The current-token missing-shard fixture uses an actual private service-namespace read-only gate and proves EROFS before admission. Its original task must retain one permanent, unbound key-level failure and exactly one attempt; release must leave the shard missing. A distinct compensating token must repair the original current ACK version at the protected-control incarnation, with physical reconstruction and bitrot evidence captured before GETs. Full public versions/tags and the original failed token are checked again after compensation. Lost gate acknowledgements retain the gate registration for cleanup. This is local implementation evidence, not a live PASS; original corrupt-metadata Unknown and grace Deferred/new-token source procedures remain incomplete, so all current diagnostics passing still cannot claim full J02 completion.

### I03 fixture completeness follow-up (2026-10-19)

The frozen source is `524eb5b32c3236497fdd8a9271df0494fa3d9974`.
Its I03 fixture contract includes DB, DV and DL. Run `39232916911/1`
(helper `4d43a6e96f8b8cfb2e45e1aebc99772d83ae23dc`, candidate
`1c7fc818dd858247c45b39542abd8291b1e6e31b`) reported five passing
branches. Independent archive, request, ACK/task/physical/public and cleanup
checks accept that replay's four-key, two-version scope. It does not complete
the frozen fixture contract. The accepted migration total remains **104/158**.
Original failures and original reports remain unchanged.

| Fixture | Implementation and evidence | Remaining acceptance work |
| --- | --- | --- |
| DV | Reuses auto-test `seed(DV)` and `suspend_fixture`: four keys with three distinct non-null data versions each; real suspended overwrites, null marker creation/removal ACKs; retained live/null marker identities; current and historical markers. Versioning returns to Enabled before partition. The fault targets a retained null data version. Every selected key version participates in GET, negative Heal and complete recursive recovery. All 15 retained data versions receive full physical shard checks; markers receive exact S3 refusal/header checks. HEAD preserves version identity, length, ETag, Last-Modified and metadata. | This expanded fixture still needs a new live run and independent evidence acceptance. It does not claim a partition while the bucket remains Suspended. |
| DB | Adds a separate never-versioned run-owned bucket with the default 128 controls. The exact candidate size contract and actual EC:8 process/object geometry calibrate zero/one, 4 KiB, inline, EC block and source-bound 5-MiB MPU minimum minus/exact/plus sizes. All 128 controls require decoded full-set shape witnesses; every distinct size has partition GET, physical checksum/no-part and Range evidence. Original/fault/rejoin/repaired snapshots explicitly include both primary and DB buckets. All peers must preserve complete inventories and stable original HEADs. | Fresh live execution and independent acceptance remain required. The MPU-size controls are single PUT objects; they do not claim completed multipart protocol coverage, which remains part of DL. |
| DL | `network_load.py` supplies three distinct control versions per endpoint, response-bound MPU fixtures and four independent worker journals for its concurrent 28-call matrix. `network_load_session.py` registers seed/workers before SDK invocation, reconciles original versions and pending MPU effects after rejoin, and withholds cleanup until a stopped physical MPU census proves no files remain. Original request and ACK byte hashes remain frozen. | The complete adapter now invokes the session, physical version recovery, MPU closure and unresolved-scene isolation for the fixed `1c7fc818dd858247c45b39542abd8291b1e6e31b` candidate. Fresh same-candidate live execution and independent full-fixture acceptance remain required. Modeled matrix/reconciliation tests are not DL live or canonical acceptance. |

The expanded DV fixture stays within the existing 128-identity/512-MiB
inventory limits. Network expiry remains 1,200 seconds; partition task deadlines,
request retry policy, quorum error mapping and complete-version receipt checks
are preserved. Source fixture completeness must be checked separately from a
raw five-branch PASS before accepting I03 as a migrated canonical PASS.


I03 DB implementation detail (2026-10-19): the existing `boundary_sizes`
helper accepts the explicit EC:8 calibration only when every live process
confirms the exact storage class and the decoded object has eight data and
eight parity members. Default callers keep their original environment guard.
The reviewed multipart minimum is bound to the fixed candidate's full
`set_disk/mod.rs` digest. Current candidate/prior physical calibration yields
15 distinct sizes and 200,127,457 payload bytes for 128 controls, within the
existing 512-MiB budget. This read-only preparation is not fresh DB live evidence.
The existing node-side snapshot bucket guard continues to require the primary
bucket and allows only its owned suffixes; no broader host path is introduced.


I03 DL request layer (2026-10-19): `LoadJournal` reuses the auto-test
`Recorder` and serializes one worker's calls; readers and writers never share
mutable sequence state. Two endpoint pairs execute eight mutations and six
reads each, within a frozen 180-second admission budget and the existing SDK
request limits. The controller alone writes Fleet evidence. Every prepared
request stays bound to its original ACK SHA-256; missing ACKs and unknown
results remain unresolved, and a failed worker admits no subsequent calls.

The fixed candidate maps write-quorum/lock errors to HTTP 503
`ServiceUnavailable`; that refusal does not prove rollback, especially for
MPU Abort. Partition LIST responses are bounded observations, not global
completeness. A finite HTTP-status tuple (at most three explicit codes) extends
the code-level Recorder API for these LIST observations; existing integer and
default callers retain their checks, and CLI traces still require one integer.
Operation-specific code, payload, version and selector validators remain
mandatory. HEAD properties use the same datetime representation as the durable
ACK JSON. Only actual invoked SDK intervals in one clock domain can prove
client overlap; collector setup windows cannot establish it, and no server
commit fence is claimed.

`LoadSession` now durably registers the seed Recorder and all four worker
journals before invocation. Seed failures retain their original unknown
request even when no complete fixture returns. Recovery first verifies all
original business versions on every peer, binds discovered pending upload IDs
only to original prepared Create requests, and records distinct recovery Abort
ACKs. No PUT/Complete retry or fabricated original ACK is allowed. Public
absence alone cannot permit cleanup: a stopped four-node census must also
find no remaining MPU files. The original stopped business snapshot is frozen
before load; the final stopped snapshot must preserve all original files and
formats, with only the existing physical repair oracle's exact acknowledged
version exceptions. Hidden business files also block cleanup. Both original
request and ACK byte hashes must remain unchanged. Unexpected positive mutation ACKs, changed/missing
business versions and hidden MPU orphans retain the namespace.

`mpu_scene.py` hashes all files under eight exact run-owned bucket/key SHA-256
prefixes on the original sixteen drives, using the immutable candidate's
multipart layout. It neither decodes opaque UploadIds nor reads credential or
other system-key contents. It requires stopped original services, matching
candidate/owner/mount identities and bounded stable regular files. The census
does not claim payload integrity or EC/bitrot correctness.

The live adapter now preserves original object and system MPU failure scenes,
checks partial refused deletes without dropping version assertions, and routes
unresolved effects into the strict isolation/recovery protocol. The earlier
807-check session delivery remains scoped evidence; current full-adapter
regressions and fresh live acceptance are recorded separately below.

I03 unresolved-scene isolation (2026-10-19): `network_retention.py` now freezes
the original four reservation owners and stopped business/MPU scene digests.
All four read-only preflights precede any move. The existing durable isolation
backend supports `lock-quorum` with private per-run archive parents and
non-overwriting same-filesystem renames. It hashes the primary and DB fixture,
original DL MPU key prefixes and original I03 shard-injection backups while
retaining credential files by inode/metadata without reading their contents.
Interrupted moves resume against the same durable census; missing roots,
changed archived bytes, active gates, owner/mount drift and destination
collisions block progress. Every original file and inode is retained; no shard
restoration, S3 Abort or namespace deletion is performed by isolation.

The recovery entry point accepts this path only for one archived I03
FAIL/cleanup-FAIL branch with an exact candidate/package/binary/helper/run
binding and four matching original reservations. It reports `ISOLATED`, never
`PASS` or `alreadyClean`, including retries after all active paths are absent.
Historical expiry-only recovery keeps its existing strict boundary. The live
I03 adapter now emits this intent when a seed or original DL effect remains
unresolved. Local filesystem/controller tests do not certify live isolation
or increase the canonical count; full-fixture live acceptance remains pending.


I03 complete DL adapter (2026-10-19): the fixed `1c7fc818...` candidate now
executes all DB/DV/DL stages in one original branch. The session exists before
any DL SDK call. Its six acknowledged data versions join the primary baseline,
all original HEADs, recursive version responsibility and all sixteen-drive
streaming checks. Four workers issue the 28-call matrix inside the actual
partition, between independent network witnesses. DL stable reads use their
own intact data and actual shared-lock quorum, independently of the separate
missing DV shard. The 128-object DB fixture remains included in every complete
business snapshot.

`network_delete_records.py` reads only bounded `delete-prepared` JSON files
under the two original stopped DL control keys. It exports selectors and
hashes, never raw records or nested metadata. Exact source hashes bind the
version-1 journal, FileInfo serialization, delete rollback and Heal settlement
behavior. Duplicate JSON fields, unsafe files, wrong runtime/owner, unbound
versions/data directories/metadata hashes/target bytes and changed
incarnations fail closed. The exact original refused historical-version
request can explain only its reachable writable slots. Other versions, DB/DV
controls, readonly or disconnected roots, new business commits and altered
part bytes keep their original strict assertions.

A complete prepared original backup is a product settlement obligation, not
proof of commit or rollback. After rejoin, independent recursive product tasks
must report all three original versions per DL key. Unbacked version loss
requires an actual repaired receipt; source-bound product delete settlement
may precede a healthy shard receipt only for that exact original generation
and incarnation. All final physical files/formats, original shard bytes and
public versions/metadata/tags still must match before cleanup. MPU retirement
uses separate recovery ACKs and stopped final MPU/business checks. These occur
before the recovered unique PUT. An incomplete seed/load or unresolved effect
instead stops services, captures the original scene, releases owned gates and
isolates unchanged inodes; the case and cleanup remain FAIL until separately audited.

Failure capture order is explicit: stop services, save original stopped
business/MPU bytes and logs while fault intents still exist, release only
owned network/private-write gates, and recapture the stopped scene. Byte
hashes must remain identical across gate recovery before isolation can begin.

The full adapter intentionally requires the exact fixed candidate above.
Historical `072b57b...` scoped source/contracts are retained, but its different
delete protocol does not certify this expanded adapter. No product source,
compiled collector, inspector pin, native barrier, request retry or time budget
changes in this delivery. Local modeled orchestration and filesystem tests
remain distinct from new full-fixture live evidence. Canonical acceptance
remains **104/158** until that evidence is independently verified.


I03 full-fixture live run [39254429889/1](https://github.com/rustfs/rustfs/actions/runs/39254429889)
used helper `3b064c90bc24a11003c1721a964a9c30b831751a`, fixed candidate
`1c7fc818dd858247c45b39542abd8291b1e6e31b`, package `38288322026/1`
and inspector `39214576404/1`. Its first minority-partition branch failed:
19 DL requests were actually invoked and fully drained, with zero unknown
results. The node1 historical DELETE and node4 GET returned definite
`500/InternalError`. Original request IDs, exact version selectors, seed
payloads and current server PIDs were independently correlated. The remaining
four branches did not run. These responses retain their original FAIL;
no expected-status relaxation or product root-cause claim is made.

The original stopped business/MPU/log scene was captured before owned-gate
removal, with unchanged business and MPU hashes afterward. All original
root/state trees on the four nodes were isolated by inode-preserving same-filesystem moves into
private per-run archives. Separate recovery re-audit
[39257565469/1](https://github.com/rustfs/rustfs/actions/runs/39257565469)
verified all four preflight, no-op isolation and post-audit receipts. Independent
audit checked the official ZIP digests, all 723 unchanged original evidence
members, all 20 original root/state content and inode censuses, unchanged
private archive parents and the exact executed stopped-service/binary/mount
guards. Two sequential reviews passed before fleet ownership was released.
Recovery reports `ISOLATED`; it performs no S3 requests, namespace deletion,
old-shard restoration or original verdict rewrite. Original case SHA-256 is
`13d06d16e8a05d544ec5dd6df5ae8bdabcd6c204c85f8ca4f411196569807099`.
The complete I03 case remains unaccepted; the canonical total stays **104/158**.

I03 diagnostic candidate `85c706cac09d53d0649487b8f1865f18beb53225`
adds bounded GET/DELETE error classification at the existing S3 handler return
boundary. It preserves the ingress request ID and original response. The
classifier uses `S3Error`'s inherent source accessor, including nested wrappers;
its standard error trait does not expose that payload. Two actual development
regressions caught empty source/RPC classifications before this correction.
The final three classification checks and one actual JSON-event check pass,
including nested/cyclic errors, numeric errno, unchanged response properties
and sensitive-message/RPC-metadata exclusion. Two fresh final-diff source
reviews passed. This diagnoses errors without changing HTTP mapping, storage,
RPC/authentication, retries, timeouts or the old live result.

The adapter explicitly admits this additional full commit after checking all
20 referenced product source files against committed Git objects. Eighteen
original contract files remain byte-identical; the error source hash is
updated and the diagnostic S3 boundary is additionally bound. Independent
extractor checks cover 21 distinct files in total and retain the original
`503/SlowDownRead` and size rules. Original `1c7fc818...` contracts are unchanged;
unknown neighboring SHAs and the historical incompatible delete protocol stay
rejected. The 825 Heal checks pass with nine platform skips.

Separate hosted package-only run
[39267250075/1](https://github.com/rustfs/rustfs/actions/runs/39267250075)
is building this exact source. The complete published package, opaque build
bytes and a newly built matching inspector still require independent audits
before a new full-fixture live run. No previous tool artifact is treated as a
new-candidate build, and no original `500` or canonical count is rewritten.


C02/C03 DB boundary completion (2026-10-19): the shard executor retains all
128 original DB controls and every frozen first/middle/last corruption branch,
including C02's completed multipart-data branch. Before any fault, the exact
candidate size contract, actual default-class process environment and decoded
full-set calibration object now derive inline and EC block minus/exact/plus
sizes. Supplemental public PUT controls also cover zero/one, 4-KiB and the
source-bound 5-MiB minimum-part-size boundary. These single-PUT size controls do
not claim completed multipart protocol coverage or a Cartesian C03/DM matrix.
The original count, bytes and MPU controls remain in the full baseline; the
combined fixture must fit the existing 256-object and 512-MiB budgets. Every
supplemental control requires a decoded full-set shape and original physical
checksum/no-part proof, repeated after each repair, plus independent full GET
and Range verification. The runtime integration regression rejects the previous
uncalibrated C02 and C03 callers. Six focused tests and the 831-test Heal suite
pass (nine platform skips). Fresh candidate-bound H1/H2 execution and independent
acceptance remain required; the original C02/C03 failures and canonical
104/158 count are unchanged. I03's inspector and replay remain pinned to the
separately reviewed `536a0a13b7a428a557dbe20d03faf72808da4abd` helper.


I04 physical-scope correction (2026-10-19): original run `38119569421/1`
recorded successful 769/129/129/129-object queue manifests on both H1 and H2,
but its physical baselines contain zero business files on every node. The exact
original helper calls `Fleet.snapshot` without a bucket list, selecting only the
unseeded primary owner bucket instead of its four queue buckets and durable
control bucket. All six baseline, fault, attribution, pre-GET, post-GET and
failure snapshots now explicitly include the primary prefix and those five
owned buckets through the existing frozen fleet binding and bounded scope validator. No queue admission,
resource limit, MRF retention/retirement, fairness, physical comparison, candidate
source or cleanup oracle changes. Runtime regression tests execute the actual
H1/H2 caller and `Fleet.snapshot` adapter; the previous caller fails. All 13
focused checks and the 832-test Heal suite pass (nine platform skips). The
original FAIL remains unchanged; later pressure behavior and complete fresh
candidate-bound H1/H2 execution remain unproven. Canonical acceptance stays
104/158, and I03 still uses its separate frozen helper.


I05 manual timeout deployment (2026-10-22): `lock_timeout_runner.py` now joins
the existing four-argument dispatcher and Functional Chain manual case input.
It validates the exact 757b candidate, immutable helper, inspector and current
vm000-vm003 binding before package bootstrap, then runs independent H1 and H2
timeout diagnostics with original versioned ACKs, full peer content/metadata/tag
verification, physical comparisons and original-process FD closure. Dirty
H1, unknown writes or failed finalization stop the sequence and retain the
original stopped namespace/log scene. SIGTERM follows that retention path.
The wrapper's pre-existing log directory is accepted; prior topology or summary
files are refused before any new deployment. A clean diagnostic returns
canonical BLOCKED, never PASS; the automatic matrix remains unchanged.

Final focused verification ran 106 tests (104 passed, two Linux-only skips);
31 existing shell/outcome/nolock compatibility tests also passed. All 14 new
runner tests passed, including the actual CLI, real SIGTERM, retained receipts,
credential redaction and executed shell verdict block. The first focused run
had an earlier receive-buffer prerequisite failure in the empty-send-queue
negative test; the single test and complete focused rerun passed. A separate
300-connection macOS probe observed one kernel buffer increase after connect,
so this local transport limitation remains explicit; the bounded guard is not
relaxed and actual Linux component coverage remains in hosted CI. Catalog
158 and matrix 84/302/534 checks, shell syntax, actionlint and whitespace pass.
These checks do not establish live or canonical I05 acceptance.

Read-only environment run [39710469108/1](https://github.com/rustfs/rustfs/actions/runs/39710469108)
completed against candidate package 39364893106/1 and helper fa79b52. It
observed sixteen independent eligible endpoints, H1/H2 mounts and no port-9000
listeners. vm001-vm003 retain failed units from cancelled F07 38715204273/1;
its separately recorded successful recovery is [38726570148/1](https://github.com/rustfs/rustfs/actions/runs/38726570148).
The inspection performs no mutation and certifies neither historical-scene
recovery nor global workflow idleness. Fresh ownership/cleanup and immutable
artifact checks still precede any new fleet diagnostic. H4's twenty-four
endpoints and H6 platform restart authority remain unavailable.


I05 first manual live diagnostic [39711742431/1](https://github.com/rustfs/rustfs/actions/runs/39711742431)
used helper `704bd606071676dded130dca91d3d5278455bcaa`, candidate
`757b751482bb970bc321e92059690a7698554358`, package build 39364893106/1 and
inspector build 39566504743/1. H1 retained FAIL/cleanup FAIL after all nine seed
requests received successful bound ACKs and the original full S3 baseline
passed. The single historical GET was written and its client socket closed;
no contender PUT was invoked and H2 did not start. Official artifact
12397696964 has digest `e9ab49593f0423f0649066aa71325d59fadc0e7e6d0328071e2f06f45790325f`.

The observer incorrectly required an absent diagnostic version field. It now
accepts an absent field or the exact unique HTTP version selector and rejects
any other version. The three original production events are regression
fixtures: they correlate to the exact GET but already show `guard_dropped`
within approximately 0.36 ms, so they cannot prove a held timeout window. The
reviewed source passes no version to this diagnostic while this log carries
one; that difference is not yet explained. Neither a wrong package nor a
source mix is inferred from it. Do not replay the same warmed baseline premise
or claim that the field correction establishes lock contention.

`timeout_retention.py` recognizes three exact I05 failure boundaries: the first
helper's H1 failure before a contender, the reviewed `d100abeb` H1 failure
after one contender PUT received a known 503, and current helper `1b88cad` H2
failure after H1 completed and cleaned successfully. The H1 paths require the
exact candidate/package/binary/inspector and owner, all nine successful seed
request/ACK pairs, deterministic bytes, public metadata/tags/versions, closed
read-only historical GET and unchanged stopped four-drive physical scene. The
H2 path also requires all four owners and stopped physical scenes, inspector
uploads, preflight and deployment proof, per-node resource baselines, lock
checkpoints, three complete lock journal windows, and service logs. Every
contender boundary checks its only durable request/ACK, request digest and
clock ordering, structured `ServiceUnavailable` response, request-correlated
bucket-incarnation read-lock lifecycle, and exact failed `put_object_commit`
write-lock lifecycle. Unknown or successful writes, changed physical evidence,
missing peers, an unclean H1, another case or extra actions refuse recovery.

Recovery reuses retained-scene isolation with same-filesystem non-overwriting
renames, durable intent, preflight, interrupted-move retries and five private
archive parents. It makes no S3 request and never restores backups, deletes the
namespace or rewrites the original FAIL. The first failure recovery
[39715499193/1](https://github.com/rustfs/rustfs/actions/runs/39715499193)
completed `ISOLATED`; all 58 original artifact files matched byte-for-byte and
the five scene roots moved into private archive parents. The second diagnostic
[39733064688/1](https://github.com/rustfs/rustfs/actions/runs/39733064688)
also remains FAIL/cleanup FAIL. Its server returned the expected 503 after a
1001 ms target write-lock timeout while the GET read lock was held for 4.148 s
and its service socket retained an 8.17 MB send queue. The validator rejected
the event sequence because it grouped the PUT's bucket-incarnation read lock
with its target write lock. The before/after stopped four-drive physical
census matched; the complete S3 inventory was captured before the contender,
and the server returned a known 503 with no successful VersionId. A
post-contender S3 inventory was not captured. The new recovery planner accepts
the exact archived H1 boundary for preservation; it does not make a canonical
PASS claim or authorize replay. Read-only environment inspection
[39738071753/1](https://github.com/rustfs/rustfs/actions/runs/39738071753)
completed with stable observations of all four current Heal nodes and no
RustFS service, validation unit or port-9000 listener on the target vm000.
Evidence-based isolation [39738426191/1](https://github.com/rustfs/rustfs/actions/runs/39738426191)
completed `ISOLATED` for the exact vm000 scene: all 65 original artifact files
match byte-for-byte; preflight, move and post-audit physical digests match;
four data roots and private state remain in five root-owned mode-0700
same-device archive parents. No S3 request, backup restoration, namespace
deletion or original verdict rewrite occurred. The inspection also observed
previous failed F07 units on vm001-vm003, which were outside this plan. The
original run remains FAIL/cleanup FAIL. Canonical acceptance and its 104/158
count remain unchanged.


I05 lock-window diagnosis (2026-10-22): exact candidate source
`757b751482bb970bc321e92059690a7698554358` defaults
`RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE` to `true`. Its `get_object` path
acquires the ECStore read guard, marks downstream options `no_lock`, then
returns the reader without attaching that guard when optimization is enabled.
The original isolated-service attestation does not set this variable, so the
candidate default applies. The archived GET's exact `acquired` and
`guard_dropped` events share request `1ba2503b-78ea-40cd-b2ff-003f54662c9c` and
lock attempt `3a86c2cc-ada5-4e8a-9e0d-dbe3bb7c1489`; they are 0.367104 ms apart.
This explains the missing backpressured lock window without attributing it to
the caller socket or optional version field.

The `lock-timeout` runtime profile now explicitly sets
`RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=false`, causing the streaming GET to
retain its read guard until EOF or reader drop. The candidate binary and other
runtime profiles are unchanged. The existing remote process observer checks
every configured setting, so it will reject a deployment that omits or changes
this value. Focused verification ran 50 observer/timeout/runner tests: 48
passed and two existing Linux-only tests were skipped on macOS; `git diff
--check` passed. The previous FAIL and its isolated scene remain retained; this
local runtime correction is not live or canonical acceptance.

I05 H2 retained-scene recovery (2026-10-22): `timeout_retention.py` now accepts
the exact `1b88cad` H2 contender failure only after H1 completed with cleanup
PASS. The H2 plan binds all four reserved owners, inspector uploads, preflight
mounts, service identities/settings, journal checkpoints and balanced peer
sampling rounds, plus the four stopped physical scenes and failure logs. It
still requires the exact nine successful seed ACKs, complete public baseline,
closed historical GET and known contender 503/lock lifecycle. Unknown,
successful, foreign-peer, partial or physically changed evidence refuses
before creating a Fleet or moving any root. H2 isolation therefore keeps the
four peers together and never changes the original FAIL. An H2 archive fixture
and negative evidence cases were added. The matching local CI group ran 117
tests successfully with two existing Linux-only tests skipped on macOS; the
previous single timeout FD test also passed when rerun in isolation. No new
live diagnostic or canonical acceptance is established by these checks.

Post-isolation read-only inspection [39745040693/1](https://github.com/rustfs/rustfs/actions/runs/39745040693)
used helper `22589bc00836b5d47f382429313e967c1dfc38a9` and candidate package
39364893106/1. It observed 16 independent eligible endpoints and available H1/H2
mount shapes, with stable runtime observations and no port-9000 listeners.
vm000 had no loaded validation unit; vm001-vm003 still reported the failed H2
unit from cancelled F07 run 38715204273/1. The inspection recorded
`mutated=false`, `cleanStateCertified=false` and
`globalWorkflowIdleCertified=false`; it neither changed that scene nor adds
canonical acceptance.

I05 drain-budget failure [39746002648/1](https://github.com/rustfs/rustfs/actions/runs/39746002648)
is now recognized by the retention planner under helper `0442d4d`. The plan
accepts only its H1 `NOT_OBSERVED` boundary: one closed, non-retried historical
GET with an admitted lock owner, one known 503 contender PUT whose target write
lock reached timeout without acquisition, the exact caller marker and lock
lifecycle, the two backpressure socket samples, all repeated read-only drain
socket/resource/journal samples, unchanged four-drive scene and the original
nine seed ACK pairs. Unknown or successful writes, extra requests, missing
samples, changed process identity or physical drift refuse before Fleet
creation. The archived failure remains `FAIL/cleanup FAIL`; no canonical PASS,
backup restoration, namespace deletion or verdict rewrite is authorized.

I05 drain-budget scene isolation [39750591531/1](https://github.com/rustfs/rustfs/actions/runs/39750591531)
completed `ISOLATED` using helper `b56b848`. Independent audit compared all 101
original artifact files byte-for-byte, rechecked the unchanged original
`FAIL/cleanup FAIL`, and verified identical preflight/move/post-audit tree,
owner, device and inode baselines. The original business scene digest remains
`c438f5bf295f56ddd168e9e840b41281863c3da06c0973eb9abead95edd63e89`.
Four data roots and private state remain in five root-owned mode-0700
same-device archive parents. No S3 request, backup restoration, namespace
deletion or original verdict rewrite occurred. This preservation result adds
no canonical acceptance; the documented count remains 104/158.

The retained run proves the caller socket disappeared and its diagnostic read
guard dropped, while the process FD count remained 23 against baseline 22.
It does not identify the extra FD. I05 resource and socket samples now include
bounded descriptor kinds and target fingerprints plus TCP rows joined to the
original process's owned socket inodes. File contents and raw descriptor paths
are excluded. The same original process birth and network namespace must
survive collection; snapshots still make no global no-leak claim. The FD
threshold and thirty-second drain budget are unchanged. A real local HTTP/1.1
503 integration confirms the actual `Probe.s3` SDK path closes its keepalive
socket without retry, but does not identify the original production FD. Fresh
live descriptor evidence is required before attributing or correcting it.

Verification: all 70 focused observer/timeout/transport/performance checks
passed in Linux, including actual `/proc` descriptors and network-namespace
refusal. The nine direct socket/census components also passed without network
access in Linux. All 63 retention/runner/migration checks passed on macOS;
catalog 158, matrix 84/302/534 and whitespace checks passed. The macOS focused
suite encountered the previously recorded receive-buffer prerequisite in one
physical-change negative test; that test passed independently and in the full
Linux suite. The prerequisite and failure oracle were not relaxed.

I05 descriptor diagnosis `39754893467/1` retains FAIL/cleanup FAIL under exact
helper `dd7e54e6672cab76b5a6245ab3b4fbd051ddd39d`; H2 was not started. Complete
FD snapshots attribute the 22-to-23 increase to an external TCP connection
from `168.63.129.16` while the intentional GET socket disappears and its read
lock drops. The concrete platform probe configuration remains unverified.
The preservation planner now validates this helper's complete census and
original bounded refusal, caller, journal and physical evidence. The real
101-file fixture and corrupt-evidence regressions exercise preservation
without S3 access, restoration, verdict change or canonical acceptance.
