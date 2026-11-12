# Case-migration implementation review

Review date: 2026-10-28. The frozen source catalog commit is
`524eb5b32c3236497fdd8a9271df0494fa3d9974`, and its SHA-256 is
`463fdd02dee43e42fef4f3aacf7506709ce87af535e1a53d91c521bc2e361cad`.
This delivery covers the test flow, the original assertions, and evidence collection in auto-testing. Product execution results are retained separately.

## Implementation and execution results

| Test implementation status | Canonical cases | Meaning |
| --- | ---: | --- |
| Implemented | 145 | Every source branch already has an execution path, the original assertions, and evidence collection. 115 of them have a complete live audit. 30 failed at runtime or have not yet been fully accepted. |
| Limited by environment conditions, disabled by default | 13 | The operation flow, capability checks, original assertions, and unmet reasons are retained. Execution needs an explicit resource and a reviewed resource adapter. |
| Implementation partially complete | 0 | Test wiring and source-flow control for F07 and I05 have been filled in. |
| Total | 158 | Every source canonical ID is retained. |

The independent live status is still 115 accepted, 22 retained-failure, 4 pending,
4 blocked, and 13 disabled. 43 items do not yet have a complete live acceptance, of which 13 are disabled by default as agreed.
These results span multiple immutable candidates and historical batches. They are not a product certification that one candidate passed all 158 items.
The 22 failures need per-case attribution. They cannot be announced together as product defects, and they cannot by themselves be used to conclude that the tests were not migrated.

`validation-migration-review.json` lists all 158 IDs, the implementation status, code/test references,
source branches, gaps, and the independent live status. `reviewed_files` pins file digests. After a change, the review must be repeated.
File existence and a passing local test do not replace source-semantic review or a live audit. This file does not overwrite the original results.

```bash
python3 -B scripts/validation_migration_review.py --require-complete
```

This gate fails only because of a test-implementation gap. A product FAIL does not affect the implementation count. The 13 environment flows separately report the adapter manifest they require.
The product gate `validation_report.py` still requires the source assertions, cleanup, provenance, and a complete live PASS.

## F07: persistence boundary

The frozen source `docs/heal-cases.md#F07` / `scenarios/heal-recipes.json` requires an exact checkpoint/MRF fault for this run,
retention of persisted responsibility, retention of the corrupt record, and supplementary evidence for the independent native fsync/commit
window. The entry point `persistence.py` combines the following existing capabilities:

| Capability | What it proves |
| --- | --- |
| `persistence.py` / `persistence_mrf.py` | Five live byte-corruption, write-failure, and rescan branches, retaining the original responsibility, failure, and recovery evidence. |
| manager checkpoint-page-order / checkpoint-stale-manager | Normal publication and reload order. An old manager cannot overwrite a new cursor. A normal reload is not presented as a commit interruption. |
| manager mrf-migration-boundaries | The four real interruption windows AfterPayload, BeforeManifest, AfterManifest, and AfterReadback, plus the retention and retry assertions. |
| manager root-uncertain-publication | A fault injected after the real terminal write, retaining the pending owner and isolating the old task across restart. |
| `persistence_native.py` | Six ECStore CAS/MRF directory fsync-fault regressions, covering update, first creation, deletion, and the recovery anchor. |

Checkpoints are persisted through `EcstoreDiskAPI::compare_and_update_file`. The manager's publication order,
interruption/restart responsibility, and ECStore CAS persistence failures are evidenced separately. No extra test requirement was invented for every fsync.
The reviewed source is the allowed immutable candidates `072b57b37559fbe626aeda8abf61617fdcbdd926` and
`1aa9be87d18cf941e5020f19e8622c26ba4b10ac`. The complete directory-fsync combination is allowed only on the latter.

`collect_native_persistence` checks the native manifest against the fleet candidate before execution, runs the six regressions,
verifies them independently, and checks the candidate again. `native_branch_result` then checks that the manager and native builds are the same candidate.
Success and failure are written to `independent-native-persistence.json` and
`independent-native-persistence-failure.json` respectively. The raw native logs are retained under `native-persistence/`.
When an artifact is missing, only the original diagnostic subset is allowed. Complete native coverage must not be claimed.

When the current auto-testing environment is used, the existing workflow passes in the following directories:

```text
VALIDATION_HEAL_MANAGER_DIRECTORY/
  tool-run.json
  manager-tools/
  manager-evidence/
VALIDATION_HEAL_NATIVE_DIRECTORY/
  tool-run.json
  native-tools/
  native-evidence/
```

The companion workflow downloads the existing native artifact on the exact directory-fsync candidate and passes the directory to the entry point.
A native result for the same candidate is still an independent prerequisite. It does not mean the H1/H2 fleet hit the internal window, and it does not claim
that a physical power loss or every file fsync hook was executed. F07's complete independent live audit is still pending, and the canonical gate remains.

## I05: source-flow control and generation proof

`lock_source.py` reuses `lock_timeout_runner.py`, `lock_timeout.py`, `lock_successor.py`,
and `lock_observer.py`. The default `source-plan` runs timeout/H1, timeout/H2,
queued-successor/H1, and queued-successor/H2 in order, each with its own run ID, bucket, and evidence directory.
Any branch, evidence-chain, or cleanup failure stops later deployment immediately and retains the original request, ACK, logs, and the stopped scene.

Before deployment, read-only validation checks three exact-source native subsets in the same successful hosted artifact:

| Subset | What it proves |
| --- | --- |
| I05 owner/unlink/stale | The historical read-repair owner, the cancelled physical-unlink owner, and the existing stale final-rename regression. The old stale test checks only the ETag. |
| G02 explicit-version append | Real missing-shard reconstruction pauses at the final-rename fence. It is released after the new-version ACK and the physical tail drain. The typed result, every surviving body, the version, dataDir, and the per-disk shard are checked. |
| G03 explicit-version delete | The race between historical/current delete and the final commit, retaining the same complete content and physical-generation assertions. |

The stronger proof for G02/G03 comes from existing native tests. Capability was not added to the old ETag test. The two groups cover 4/2 and
16/4, retaining 6 and 12 ordered fence milestones respectively. Append may safely repair the originally selected version and keep the new version.
Not every typed result is described uniformly as a stale rejection. The candidate is pinned to
`757b751482bb970bc321e92059690a7698554358`. The package, source tree,
Cargo.lock, binary, workflow attempt, helper, and contract identity of the three proofs must match.

The native prerequisite can be checked on its own. The command does not execute the downloaded binary:

```bash
python3 -B scripts/validation_heal/lock_source.py \
  --directory "$VALIDATION_HEAL_NATIVE_DIRECTORY" \
  --candidate candidate.json --testing-commit "$AUTO_TESTING_SHA"
```

The fleet controller retains the DV historical versions and the DL single-concurrency request/ACK ledger, and continues to check every peer's
complete object/version, the stopped physical shards, lock events, and bounded FD cleanup. DL is a workload semantic, not a new storage format.
The new evidence chain associates request write, timeout/queue, TCP disconnect, the original guard drop, successor acquisition, and the end of observation.
It pins a SHA-256 of each original byte sequence and checks the request, lock owner, binding, and order on the same coordinator clock.
Monotonic clocks on different hosts are not compared, and a local guard drop is not treated as release of the distributed owner.

`source-plan-summary.json` records the fleet/native proof scope of the four source branches.
`sourceProceduresComplete` is true only when all four H1/H2 instances are complete and cleanup passed. Selecting only timeout
or queued-successor is still a diagnostic subset. Completing every controller does not automatically add a canonical PASS. Independent live audit of the complete
source-plan is still pending.

## Failure attribution and later acceptance

C11's old metadata, checkpoint, MRF replay, and native incarnation each have an execution path.
J02's mixed outcome, dry-run, deferred, and unknown-negative paths each have a controller. Their historical product
results and evidence limits are retained. In particular, J02's Failed/Unknown oracle must not be changed in order to get an all-green result.

A failed PUT in G01/G02 is not retried. An unknown mutation does not count as success. B03/B04 still lack an exact mount/switch
adapter and are blocked before deployment. From here on, attribute each item as a product defect, an environment prerequisite, missing evidence, or a test-implementation problem.
Reopen an implementation gap only when there is a definite test-implementation problem. Completing the implementation delivery does not replace later regression of the candidate package or product acceptance.
