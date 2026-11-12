# Heal validation procedures

These source procedures require live execution and branch evidence on the current auto-testing Heal fleet.
Use [the operations reference](validation-heal-operations.md) before injecting a fault. A procedure card or complete archive is not a PASS.

<a id="A01"></a>

## A01: heal.admin-healthy-noop

Topologies: H1,H2. Branches: normal, deep.

1. Generate a complete manifest with the DB fixture; first, for each object, save the S3 baseline and the four-disk shard digests, and disable scanner/MRF/auto-heal.
2. In sequence, run START object (scanMode=1 and 2), WAIT the original token until the terminal state; then STATUS the same token twice.
3. For each object's outcome, distinguish Normal's metadata_healthy from Deep's verified_healthy; both require healed=0 and unchanged>0, and Normal must not be passed off as proof of payload integrity. Compare the physical digests of the target and control objects with the S3 inventory, and a repeated request must not create a new version.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [STATUS](validation-heal-operations.md#STATUS) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- On healthy objects, run normal and deep separately, then query again.
- Do not modify user data or versions. Normal accepts only `metadata_healthy`, and only Deep requires `verified_healthy`. Count healthy and repaired separately, and do not treat the number of disks checked as the number of disks repaired.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A02"></a>

## A02: heal.admin-scope-isolation

Topologies: H1,H2. Branches: root, bucket, prefix, object, pool-set.

1. Use DS to write different content at a/, ab/, and b/ in two buckets; also place a control object in an unselected set; record the actual pool/set of each object in advance.
2. Separately isolate one part of the selected object, of an adjacent prefix, and of another bucket or an unselected set. Each selector uses a new fixture and a new fault ID.
3. The object request path is the full key; the prefix path is a/ with recursive=true; the bucket path contains only the bucket; root is /; pool/set is written in the body. WAIT after START.
4. First PHYSICAL: everything inside the selected range is rebuilt, and control faults outside the range are still missing; then verify with GET, so the verification request does not repair the control fault early. root must include the entire dedicated-service manifest.

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Start separately with the root, bucket, prefix, object, and pool/set selectors.
- The target range is actually traversed. Faults on an adjacent prefix, on other buckets, and on an unselected set stay as they were. Accept by the object manifest rather than by the directory count.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A03"></a>

## A03: heal.admin-token-cross-node-status

Topologies: H2. Branches: coordinator-to-peer, coordinator-to-stable-endpoint.

1. Write DB on the same cross-node set in H2; save the endpoints for the coordinator, the target, and the stable entry respectively, and do not treat two aliases of the same node as cross-node.
2. Isolate the target node's part; START from n0 and record the original clientToken; run STATUS through n1 and stable on the same path with the same token.
3. Align the three endpoints' state by the same task identity, waiting at most the frozen state-convergence budget; the terminal state and the core counts must match, and 404/unknown cannot be treated as finished.
4. After WAIT, first physically prove that the target was rebuilt, then VERIFY from every S3 entry; save the log of the node each request actually connected to.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [STATUS](validation-heal-operations.md#STATUS) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Start from node A, and query with the original token through node B and the stable entry.
- Task identity, scope, terminal state, and core counts converge within bounds. A missing or unknown state must not be interpreted as completion.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A04"></a>

## A04: heal.admin-dry-run

Topologies: H1,H2. Branches: missing-remove0-recreate0, missing-remove1-recreate0, bitflip-remove0-recreate1, bitflip-remove1-recreate1, versioned-missing, versioned-bitflip.

1. Build fixtures with DB and DV respectively, and freeze every versionId/marker; disable background sources, and do not GET after injection.
2. Apply missing/bitflip on the original part, with an independent new directory for each group; save the post-fault PHYSICAL snapshot as the baseline before the dry-run.
3. The START body passes dryRun=true, scanMode=2, and that instance's remove/recreate together; WAIT accepts only dry_run_observed, and must not require the dry-run to hold an incarnation.
4. All shard, metadata, and marker bytes and the version manifest are the same before and after; there is no repaired ACK. After the no-change proof is complete, clear the fault, then VERIFY.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Run dryRun on objects with missing shards and on corrupt objects, with remove/recreate parameterized.
- There is an explicit dry_run_observed diagnosis. The user object's shards, xl.meta, versions, and delete markers stay unchanged. Interference from background repair must be ruled out.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A05"></a>

## A05: heal.admin-overlap-and-force-start

Topologies: H1,H2. Branches: same-scope, parent-child, disjoint, force-start.

1. Use a DX external-shard fixture of more than 128 entries to inject missing shards across multiple objects; record the overlap policy from the fixed candidate in advance (token reuse or a conflict error), and do not accept an arbitrary result.
2. START a recursive range, and use WINDOW to prove 0<processed<expected and that the task is running; within the window, resend the same range, a parent or child prefix, or a disjoint range.
3. Save every token and HTTP error; the forceStart instance adds forceStart=true to the query. Observe that the old task stops and the new task independently takes over responsibility, and that the two do not both commit a stale result.
4. Wait for every accepted task to reach a terminal state; check shard repair and versions object by object, check that active tasks and in-flight writes have exited, and count a missed active window as NOT_OBSERVED.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY) → [LOGS](validation-heal-operations.md#LOGS)

Source assertions:

- While the task is active, resend the same range, the parent and child prefixes, and disjoint requests, then forceStart.
- Per the overlap policy already fixed, verify that a merge returns the original token or an explicit conflict. forceStart must not leak the old task's responsibility or damage objects concurrently.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A06"></a>

## A06: heal.admin-stop-and-stale-token

Topologies: H1,H2. Branches: stop-active, repeat-stop, old-status, old-stop-new-task.

1. After the DX batch of missing shards, START; use WINDOW to observe nonzero, incomplete progress, then STOP the original token.
2. After the status shows aborted/cancelled, still observe a quiescence window for physical writes; successful acceptance of the stop cannot substitute for in-flight exit.
3. A new START yields a different token, then STATUS/STOP the old token; the new task must not be terminated by the old request, and a repeated stop is checked against the frozen HTTP status and error code.
4. After the new task completes, run a full VERIFY; items recorded as cancelled must not count as repaired, and every transient error keeps its original body.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [STOP](validation-heal-operations.md#STOP) → [STATUS](validation-heal-operations.md#STATUS) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- Stop while repair progress is non-zero, create a new task after the state is stable, then send a query and a stop for the old token.
- A stop must wait until in-flight writes finish and the state is stable. The new task's identity is independent, and an old request must not terminate the new task. A repeated stop is handled according to the public contract.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A07"></a>

## A07: heal.admin-invalid-request

Topologies: H1,H2. Branches: duplicate-query, invalid-bool, invalid-scan, conflicting-flags, invalid-selector, oversize-body.

1. Establish a healthy DB and PHYSICAL baseline; save the background and task state and the object version set, to check that a negative request did not unexpectedly start a task.
2. Send the server-side negative cases one by one: a repeated clientToken; forceStart=wat; scanMode=99; forceStart=true&forceStop=true; pool=-1 or set out of range; a body that exceeds the candidate limit. Each case has a fixed expected HTTP status and structured error code.
3. Use ADMIN_RAW's repeated --query and the raw --body-file, and do not rely only on rc's parameter validation; do not use a request outside the dedicated Heal route.
4. No response accepts a valid new task, and the service remains available; PHYSICAL, versions, and background responsibility are unchanged; if a 2xx is returned or an error has no clear cause, keep FAIL.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [ADMIN_RAW](validation-heal-operations.md#ADMIN_RAW) → [STATUS](validation-heal-operations.md#STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Send an illegal selector, a duplicate query, an illegal boolean or scanMode, conflicting flags, and an oversized body.
- The server returns a verified structured error. There is no mistaken start, no widened range, and no persisted side effect. A CLI parameter error cannot take the place of a server-side negative case.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A08"></a>

## A08: heal.admin-authorization

Topologies: H1,H2. Branches: anonymous, bad-signature, expired-signature, denied-user, least-privilege-user.

1. Per the IAM manual, create two temporary users on the dedicated service, one with no HealAdminAction and one with only HealAdminAction; run in a controlled credential environment, and first prove that the restricted user cannot read an unauthorized object.
2. The administrator creates a task to be queried and stopped; for start/status/stop, send an anonymous request, a bad signature, a valid signature whose time has expired, and an unauthorized-user request; the expired request is produced by the isolated client's signature-time setting, without changing the server time.
3. Check the frozen AccessDenied, signature, and time errors; the response does not expose object results. The least-privilege user completes acceptance, query, and stop, proving the interface itself works.
4. Remove the temporary users and policies and verify that the credentials are invalid; redact all raw evidence before archiving, and do not expose the administrator token to an unrelated principal.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [IAM](validation-heal-operations.md#IAM) → [START](validation-heal-operations.md#START) → [ADMIN_RAW](validation-heal-operations.md#ADMIN_RAW) → [STATUS](validation-heal-operations.md#STATUS) → [STOP](validation-heal-operations.md#STOP) → [VERIFY](validation-heal-operations.md#VERIFY) → [RECOVER](validation-heal-operations.md#RECOVER)

Source assertions:

- Send the request separately as anonymous, with a bad signature, with an expired signature, as a user without HealAdminAction, and as a minimally authorized user.
- Unauthorized start, status, and stop must not escalate privilege or leak object information. An authorized user can complete the specified operations. Credentials must not be written into the evidence.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A09"></a>

## A09: heal.admin-nolock-safety

Topologies: H1,H2. Branches: overwrite-nolock, delete-nolock, native-lock-fence.

1. Use DB/DV to prepare the same target with a missing shard; the two clients use the same key, and freeze a unique nonce and the ACK ledger for each PUT/DELETE.
2. One side sends a START body with nolock=true, and the other side submits an overwrite or an exact versionId delete per WORKLOAD; record the run interval that actually overlapped.
3. Rebuild the expected manifest in ACK causal order and VERIFY; the last successful overwrite does not roll back, and a deleted version does not come back.
4. Also run the NATIVE lock/commit-fence regression and save the exact test name, source SHA, and hit log; the black-box race and native-lock-fence are separate results, and an accidental overlap does not prove the time at which the lock is mandatory.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Pass nolock=true through the signed API, and create deterministic contention with a commit of the same object.
- The compatibility field must not skip the storage-layer object lock. The last successful write and the confirmed deletes stay correct. Keep native-barrier evidence separate from live contention evidence.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="A10"></a>

## A10: heal.admin-option-contract

Topologies: H1,H2. Branches: recursive-false, recursive-true, recreate, remove-false, remove-true, update-parity, read-repair, selector-valid, selector-invalid.

1. Write the candidate HealOpts support table and the side effects of dangerous parameters into this batch's contract.json; do not infer unexposed API fields from CLI help.
2. Use a new DB/DV for each variant: a fixed range, sufficient authority, and control objects; remove=true is applied only to this run's orphans that C10 has proved have no live reference.
3. START each one and compare it with the baseline that changes only one field; recursive checks child prefixes, pool/set checks the actual target identity, and an illegal combination requires a structured rejection with no side effects.
4. Accept on all three of WAIT, PHYSICAL, and VERIFY; if the candidate does not support updateParity/readRepair, it must show an explicit rejection, and a default repair must not quietly be counted as coverage.

Source implementation detail: frozen `heal_h1_options_batch.py` and
`heal_h1_options_node.py` at `524eb5b32c3236497fdd8a9271df0494fa3d9974`
run the remove=true orphan with a recursive bucket sweep after exact-version
DELETE and one-member full-object replay (including metadata). Its false
baseline is an option reference; remove=false is a separate repairable-object
branch. The adapter keeps every original H1/H2 DB/DV branch, with 68 registered
execution instances, and rejects the four formerly invented orphan baseline
instances. Original failures remain archived. Physical closure requires the
whole orphan to disappear, every post-DELETE control/format to remain exact,
and complete version-addressed task receipts/counters before reads or rescue.

Complete live acceptance: [run 39197088183/1](https://github.com/rustfs/rustfs/actions/runs/39197088183)
on helper `b16e6072679d6a60a044b9d4520b30b4a79b5557` passed all 68 instances
and independent ACK/task/physical/public/cleanup audit, including 170 node
finalizations. Candidate `072b57b37559fbe626aeda8abf61617fdcbdd926`, package
`38043283428/1` and inspector `38715612825/1` remain immutable. Earlier failures
are retained; control rescue is not counted as product repair.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Parameterize recursive, recreate, remove, updateParity, and readRepair, together with legal and illegal pool/set combinations.
- Test only candidate semantics that have been verified. The side effects of normal/deep and of a dangerous remove are explicit. An option that exists only in the API must not be presented as though rc already covered it.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B01"></a>

## B01: heal.disk-loss-and-repair

Topologies: H1,H2. Branches: original-drive-return, offline-ack-repair.

1. Select one member disk from layout.json and record N/R/W for the same version; use DRIVE_GATE so that only this service cannot see that logical disk, without unmounting the shared XFS.
2. Witness the fault window both by a read failure in the test process's mount namespace and by the Admin offline state; within the range quorum allows, run GET and a unique-key PUT, and record an ACK each time.
3. After DRIVE_GATE restores the original disk identity, wait for the selected manual or automatic source to handle responsibility from the offline period; do not fill the disk by copying a backup.
4. Verify every pre-fault object and every successfully ACKed object, and that the target disk actually gained new shards; node ready or a successful GET is not evidence of shard repair.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Make one confirmed set-member disk unavailable to the heal service, keep reading, and restore the original disk.
- Record the real fault window and the source and target disk identities. When the fault is recoverable, finally reconstruct the target shards and the complete manifest; verifying a degraded GET alone is not enough.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B02"></a>

## B02: heal.disk-readonly-and-permission

Topologies: H1,H2. Branches: read-only, permission-denied.

1. In the dedicated test service's mount namespace, set the target root read-only; the permission instance uses a dedicated non-root service UID and an independent directory ACL, and must not merely chmod as root and then claim it took effect.
2. Under the same identity and namespace as the service, run a bounded write probe and require EROFS/EACCES; first save and isolate the target part, then trigger deep heal.
3. While the target is not writable, the failed or pending-repair responsibility must be kept, and it must not be reported repaired; PHYSICAL confirms that nothing was written to the parent directory or the system disk.
4. Clear the read-only state or restore the original ACL, repair again, and VERIFY; keep the errors from the fault period and the physical shard-repair evidence after recovery.

After the permission-denied branch releases the ACL gate, keep the same candidate process running and prove the state with the process birth, the candidate digest, and the physical snapshot taken after release. After release, and before a new unique Admin token is submitted, the target shard must still be faulty. If automatic recovery changes the shard first, record the branch as `NOT_OBSERVED`; the recovery must not be attributed to the new manual task. After the new repair token reaches a terminal state, stop the service and verify the original shard digest. The read-only branch clears the gate according to its unit mount-gate lifecycle, then runs an independent repair task.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Make only the exclusive test volume read-only or access-denied, and retry after it is restored.
- An unwritable target must not be reported repaired. The error and the responsibility still awaiting repair are visible. After recovery, the write actually goes to the target and does not land on the system disk or in another directory.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B03"></a>

## B03: heal.disk-replacement-formatted

Topologies: H1,H2. Branches: new-disk-data, new-disk-versioned.

1. After a complete baseline, stop this case's service and isolate the target logical disk's data as a whole into this run's quarantine; use REPLACE to create an empty new data root.
2. Start according to the candidate initialization contract, and read format.json to prove the new this UUID belongs to the original pool/set/slot; other disks' formats must not become a new cluster.
3. Explicitly START root/deep or the rebuild source this instance specifies, and the old-disk backup must not be readable by the test process; WAIT for all responsibility.
4. For each object and version, verify that the target disk's shards match the same-role baseline digest; confirm they were not copied back from quarantine; do recovery only after evidence collection and mark it as rescue.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [BASELINE](validation-heal-operations.md#BASELINE) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT](validation-heal-operations.md#FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Swap in an empty new logical disk, and after confirming the new disk UUID and a correct format, trigger rebuild.
- The new disk is distinguishable from the old disk and belongs to the correct pool/set/slot. Rebuild completes for every version. Copying the data back from an isolated backup must not be passed off as heal.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B04"></a>

## B04: heal.disk-replacement-unformatted

Topologies: H1,H2. Branches: blank-unformatted, foreign-format.

The Functional Chain runs the shared read-only replacement admission gate
before package/service changes and the executor repeats it before reservation.
If exact endpoint mounts or the reviewed mount-switch adapter are unavailable,
the case is recorded as pre-deployment `BLOCKED` without creating either
the blank or foreign-format fixture.

1. Use REPLACE to create an empty logical disk with no format; record the empty-directory evidence, start, and observe the safe initialization the candidate supports or an explicit rejection.
2. The second instance uses this run's second isolated small cluster to generate a foreign format, installs it into the replacement root per the FORMAT_BAD manual, and keeps the original copy and every UUID.
3. A foreign identity must not be silently overwritten or attached to the wrong set; if it is rejected, wait for a bounded deadline and confirm that data on the other healthy disks is unchanged.
4. Stop this service, withdraw the replacement root, and restore the original identity, then VERIFY; an instance that normally accepts an empty disk must also prove the rebuild object by object.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT_BAD](validation-heal-operations.md#FORMAT_BAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Introduce an empty unformatted test disk, and compare that case separately with a disk that carries a foreign cluster's format.
- Per the initialization contract the candidate supports, accept it safely or reject it explicitly. Do not overwrite a foreign identity, and do not mark a disk that is not ready as writable and healthy.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B05"></a>

## B05: heal.multiple-shard-loss

Topologies: H1,H2. Branches: loss-1-data, loss-1-parity, loss-2-through-m-data-parity.

Before seeding, before each fault and after each faulted restart, the executor
observes signed read-only Admin info from every H1/H2 endpoint for at most
90 seconds. All 4/16 slots must match the frozen format UUIDs and report online;
read readiness alone does not prove this prerequisite. Every observation is
retained. Incomplete or foreign identities fail immediately; persistent offline
members expire as `NOT_OBSERVED` without retrying the Heal task. The original
`38200772548` unknown-version receipts remain a retained failure. This startup
check alone does not establish its underlying cause or a new live PASS.

1. From the candidate and the actual metadata, determine M=N-R, enumerate the legal missing-shard counts 1..M and the data/parity combinations, and write them into this batch's expanded parameter table; do not run only loss=1.
2. Each instance uses a new object of the same version and applies FAULT missing, separately, to the part on each specified member; multiple metadata copies must also be shown to point at the same version/data_dir.
3. When the conditions for committing a repair are met, START deep/WAIT; first check that every target shard matches the role baseline, then VERIFY.
4. After each restore of the isolated backup, switch to a new object before the next combination; combinations below R are counted as negative cases by B07 on their own, and the results are not merged.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Following the actual same-version EC layout, drop 1..M shards in turn, covering the data and parity role combinations.
- Each parameter instance on its own meets read quorum and write quorum. When the repair conditions are met, rebuild every target, and do not merge shards across versions or across sets.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B06"></a>

## B06: heal.read-quorum-boundary

Topologies: H1,H2. Branches: exact-r-read, exact-r-heal-write-limited, restored-w.

The executor accepts only `072b57b37559fbe626aeda8abf61617fdcbdd926`, reviewed file by file and against the lock policy, and the GET-quorum repair candidate `1c7fc818dd858247c45b39542abd8291b1e6e31b`. Record the exact lock contract of the selected candidate. The inspector must match both the candidate and the current helper source digest; an unknown candidate is rejected before deployment. A below-R GET must still be the candidate-frozen `503/SlowDownRead`. The historical `500/InternalError` failure record is retained. Supporting a new candidate is not a new live acceptance.

1. Record the source of read R, of repair-write W, and of the lock quorum; for the same version, keep exactly R authoritative shards and isolate the others.
2. The first request uses GET to verify the read boundary and records read-repair interference; with a second new object of the same shape, run a manual-only deep heal.
3. If only R are present but the write or lock quorum is insufficient, require that a persisted success be rejected; if that combination cannot be realized in the topology, state the exact reason before the run and choose a topology that meets the conditions, without fabricating an offline count.
4. After restoring to W, start the repair again and save an independent token, and compare the exact target shards with the full content; a successful read cannot count as a successful repair.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [GET](validation-heal-operations.md#GET) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Keep exactly R authority shards of the same version, test the read and the repair commit, then restore write quorum.
- Judge the read boundary and the repair-write boundary separately. Do not require a successful commit when write quorum is short merely because GET still succeeds.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B07"></a>

## B07: heal.below-read-quorum

Topologies: H1,H2. Branches: r-minus-1-unversioned, r-minus-1-historical.

1. For the chosen exact versionId, keep R-1 authoritative copies of the same version; isolate the remaining parts, and ensure that no other pool or old-version fallback is treated as a same-version source.
2. GET must return the frozen unrecoverable error, and must not return 200 with the wrong content or an empty object; deep heal's execution/disposition must truthfully be failed or unresolved.
3. Before recovery, snapshot every good shard and xl.meta, and confirm that the negative case did not spread the damage or execute remove; keep the failed task's original token.
4. Only after negative-case evidence collection is complete, RECOVER the original shards, then explicitly heal and VERIFY; a green result after recovery must not cover the errors from the fault period.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [GET](validation-heal-operations.md#GET) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Reduce the authority sources to fewer than R, run GET and deep heal, then restore the isolated sources.
- During the fault, do not return forged successful content, an empty object, or a mistaken delete record. State unresolved or failure explicitly. After recovery, the original object and its versions are complete.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B08"></a>

## B08: heal.below-write-quorum

Topologies: H1,H2. Branches: read-present-write-absent, lock-quorum-absent.

1. Use layout.json to compute the object-write quorum and the lock quorum separately; use DRIVE_GATE/NETWORK to construct a test-service visibility that can still read but cannot commit safely.
2. Run a unique-key PUT and a heal of an existing object that is missing a shard; save every ACK/HTTP/body, a timed-out write must query that unique key's resolution, and a direct retry that changes the key is forbidden.
3. During this period it must not be reported repaired or return a successful write ACK that cannot be honored; collect quorum and lock-failure logs from every peer.
4. After the fault is cleared, rebuild the manifest from every confirmed or resolved write, actually complete the target shards, and VERIFY; an ordinary low-quorum error is not an infrastructure error.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [NETWORK](validation-heal-operations.md#NETWORK) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Construct a case where read quorum still exists but write quorum and lock quorum are insufficient, and run heal and a new PUT.
- Do not falsely report a persisted repair or a successful write. Record the object, metadata, and distributed-lock quorums separately. After recovery, check every confirmed write.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B09"></a>

## B09: heal.node-outage-rejoin

Topologies: H2. Branches: one-node-stop, rejoin-with-new-versions.

1. Confirm that the H2 target set is cross-node, count the disks actually lost by stopping one node, and do not substitute H5, which gives each machine its own pool.
2. PROCESS stop the exact test unit, and witness that MainPID is gone and the endpoint is unreachable; keep the services on the other nodes running.
3. Within the quorum the calculation allows, run degraded PUT/DELETE/GET and save the versioned ACKs; re-enter the same node and the same data root.
4. Observe MRF and repair responsibility until the objects that succeeded during the offline period are fully repaired; VERIFY from every node and PHYSICAL the target, and do not look only at process ready.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [PROCESS](validation-heal-operations.md#PROCESS) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Stop one node of the same cross-node set, perform the degraded I/O that is allowed, then rejoin.
- Judge availability by the number of disks actually lost. Reconstruct the objects that were successfully committed during the outage. A node becoming ready again does not mean every object has been rebuilt.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="B10"></a>

## B10: heal.disk-and-node-flapping

Topologies: H2. Branches: drive-flap-3, node-flap-3, interrupt-rebuild-again.

1. Pre-register three fault cycles, the maximum window of each, and the total deadline; prepare an ACK ledger with nonces, and set a drive gate or unit operation that belongs only to this service.
2. On each offline and online round, witness the actual state first; on the third round, go offline again when WINDOW proves a partial rebuild, and do not substitute a fixed sleep.
3. Record each round's responsibility and queue, task identity, error rate, and CPU/FD; exceeding the pre-registered retry or resource budget is a failure, and do not keep looping to wait for green.
4. After the fault is fully cleared, verify that every confirmed object, the target shards, and the persistent queue have converged; if the active window was not hit, record NOT_OBSERVED.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [PROCESS](validation-heal-operations.md#PROCESS) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [METRICS](validation-heal-operations.md#METRICS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Go briefly offline and online repeatedly, and drop offline again while a rebuild is in progress.
- Retries and the queue stay bounded, and identity stays stable, with no storm of duplicate tasks. After the fault is finally cleared, converge, and every confirmed object stays correct.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C01"></a>

## C01: heal.missing-part-file

Topologies: H1,H2. Branches: data-single-part, parity-single-part, multipart-first, multipart-middle, multipart-last.

1. After the DB/DM writes, use FORMAT/PHYSICAL to map each version/part to its real disk role; each variant selects a different target, and does not guess data/parity by disk index.
2. After writes to the injection target are stopped, FAULT missing removes only the part whose digest was frozen, while xl.meta and format stay; once it is running again, GET of the target is forbidden.
3. START deep and WAIT; first verify that the target part of that role is restored to the full pre-fault SHA-256 and length, then check GET/Range and the version manifest.
4. Restoring only xl.meta, rebuilding the wrong part, or changing the control is a failure; the original backup may be used only at cleanup.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Isolate only one part/shard of the specified object, keep format and the other metadata, and run deep heal
- The corresponding data or parity shard is regenerated, and the part/version identity is accurate; do not pass merely because xl.meta reappears.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C02"></a>

## C02: heal.data-shard-bitrot

Topologies: H1,H2. Branches: data-first-byte, data-middle-byte, data-last-byte, multipart-data.

1. Record the data shard the candidate resolved; use non-repeating SHAKE content so the data-shard and parity-shard digests are not the same, and verify the healthy baseline.
2. FAULT bitflip flips one bit at each of the frozen first, middle, and last byte offsets; save the offsets, the hashes before and after injection, and the target physical role.
3. Keep enough correct sources, and run this batch's pre-registered deep and read-repair triggers separately; the manual instance must not pre-read the target.
4. The target is restored to the complete original shard digest, and the decoded full content and Range are correct; a correct GET alone, while the bad shard remains, must fail.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Flip bytes at known offsets of the specified data shard, including the head, middle, and tail positions
- Digests before and after the fault prove the injection took effect; deep or read-repair finds the corruption and repairs it; independent bitrot/EC and full-content S3 verification pass.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C03"></a>

## C03: heal.parity-shard-bitrot

Topologies: H1,H2. Branches: parity-first-byte, parity-middle-byte, parity-last-byte.

1. Select a parity shard proved by FORMAT, and keep enough data; whether an ordinary read needs that shard is a separate control instance, and it must not be triggered and then claimed to be pure deep.
2. FAULT bitflip covers the first, middle, and last positions; after injection, run PHYSICAL only, with no GET or HEAD.
3. START deep/WAIT, and require the repaired responsibility to correspond to the correct object, version, pool, and set; compare it with the same-role SHA-256 from before the repair.
4. It passes only when the full GET and the version/metadata comparison pass and the bad parity is gone; if the background repairs the shard early, that trigger instance was not observed.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Corrupt a parity shard so that an ordinary GET may still succeed, then run deep heal
- The corrupted parity must be identified and reconstructed; a correct GET alone, with no repair of the target shard, cannot pass.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C04"></a>

## C04: heal.truncated-and-zeroed-shard

Topologies: H1,H2. Branches: truncate-half, truncate-near-tail, zero-all, checksum-header.

1. Save a complete backup of the external part and a correct metadata quorum; each kind of corruption uses a new independent object.
2. FAULT truncate keeps half by default; near-tail, per the FAULT_FORMAT manual, removes only the last byte and records the original length; zero clears everything to zero; header flips the first 32 bytes.
3. After witnessing the actual length and digest difference, START deep, and capture the short-read and checksum diagnostics and the responsibility result.
4. The target's original length and complete shard digest are restored, GET has no truncation or zero-fill, and the control object is unchanged; do not treat one parseable metadata copy as healthy content.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Separately truncate the shard, zero it, and corrupt the checksum header, keeping a correct metadata-arbitration source
- Short reads, all-zero content, and checksum corruption are identified accurately; repair must not accept truncated content or a wrong length.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C05"></a>

## C05: heal.missing-or-corrupt-xlmeta

Topologies: H1,H2. Branches: missing-xlmeta, truncated-xlmeta, corrupt-xlmeta, inline-payload, versioned-xlmeta.

1. First read the DB small objects and the DV historical versions in full; FORMAT parses the version/inline/data_dir linkage in each disk's xl.meta and saves the parser source SHA.
2. On a minority of members, apply missing/truncate/header to the exact object's xl.meta; the inline instance flips a bit only after the parser that matches the candidate has located the payload, and must not pick an arbitrary offset and call it inline.
3. Keep correct sources of the same version, and START deep/WAIT; after recovery, parse xl.meta again and check every version, marker, and data reference.
4. A full GET of every versionId agrees with the version set; legal re-encoding is allowed, but it must not create empty metadata, omit a historical version, or move delete semantics.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- On a minority of members, remove or corrupt xl.meta/inline content, keeping enough authority sources
- Restore every version that should exist for the same object, together with its data references and delete markers; do not substitute newly created empty metadata for a real repair.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C06"></a>

## C06: heal.stale-version-rejoin

Topologies: H1,H2. Branches: unversioned-overwrite, delete-historical-version, delete-marker.

1. Save the DV and the old generation; DRIVE_GATE takes one member offline while its old data is kept as-is.
2. The online quorum performs a new-content overwrite, a delete of the exact old versionId, or marker creation; serial ACKs determine the causal order, and the commit is confirmed on the online side.
3. Restore the same disk that carries the old data, check the original disk UUID, then observe manual or automatic repair, and save the authority-selection log for the old and new generations.
4. The final latest value and the full history and markers strictly match the ACK manifest; a deleted version does not come back; metadata and content for the same key on the target old disk are corrected.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [START](validation-heal-operations.md#START) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- After a disk goes offline, perform an overwrite or a version delete, then let the disk that carries the old metadata rejoin
- The confirmed new generation/version is authoritative; the old current value and deleted versions are not resurrected, and the old disk is corrected rather than becoming a new incorrect authority.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C07"></a>

## C07: heal.conflicting-subquorum-authority

Topologies: H1,H2. Branches: disjoint-version-quorums, disjoint-incarnations, native-authority-fence.

1. Serially write two different versions or generations to this case's same key, and save each member's copy; per FORMAT_BAD, with the service at rest, combine them into two sets each smaller than R, keeping every original shard.
2. Parse each member's version identity and produce a count table grouped by version/incarnation; even if the total reaches R, prove that every group has no quorum.
3. GET and deep heal must reject safely, and must not commit from a newer mtime or from the union of shards across versions; check that evidence which was still recoverable has not had remove applied.
4. Run the NATIVE authority-arbitration regression that matches the source code; on the fleet, after the mixed scene is cleared, restore the original fixture and VERIFY, and count the two kinds of evidence separately.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FORMAT_BAD](validation-heal-operations.md#FORMAT_BAD) → [GET](validation-heal-operations.md#GET) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Use a controlled native fixture to construct version/generation sets that do not form a quorum with each other, then run the corresponding live counterexample on the fleet
- Do not treat the union of shard counts from different versions as authority; when it cannot be proven, reject safely and retain recoverable evidence, and do not guess a newer timestamp.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C08"></a>

## C08: heal.metadata-payload-mismatch

Topologies: H1,H2. Branches: length-mismatch, payload-mismatch, insufficient-authority.

1. First save the full metadata and the hash of the versioned external part; take the replacement payload from another version of this case with a different length or content.
2. With the service stopped, per FAULT_FORMAT, replace the content of or truncate only a minority of parts, keep the length and digest claimed by the original xl.meta, and do not change healthy members.
3. deep heal compares the metadata with the actual content; an instance with sufficient authority should rebuild the shard, and an instance with insufficient authority should reject and keep the good copies.
4. After evidence collection, recover, and verify that the exact version has no cross-generation content; keep the actual parse result, and do not treat an arbitrary invalid-header injection as an overwrite of the length field.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Construct a minority of replicas whose metadata claims a length or digest that the actual part does not match
- Judge metadata and content together; do not treat a parseable xl.meta as healthy; when authority is insufficient, refuse to overwrite a correct replica.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C09"></a>

## C09: heal.foreign-format-and-unknown-schema

Topologies: H1,H2. Branches: foreign-disk, foreign-set, future-format, future-object-schema.

1. Use a second isolated test cluster to generate a real foreign disk/set format; the current instance has only one replacement logical root that may be modified.
2. While the service is stopped, FORMAT_BAD separately installs a foreign format or writes a future schema using the version header the candidate parser supports; record the original bytes/digest, and do not touch the shared disk's format.
3. Start, then GET and deep heal, and check for explicit isolation or unsupported; an unknown format must not be initialized as missing or written out as a new cluster.
4. Stop the service and collect physical evidence that the original records are still present; withdraw the bad root, restore the original identity, then run a full VERIFY; a format field that cannot be confirmed must not be passed off as injected.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT_BAD](validation-heal-operations.md#FORMAT_BAD) → [PROCESS](validation-heal-operations.md#PROCESS) → [START](validation-heal-operations.md#START) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Provide a foreign disk/set identity, a future schema, or an unsupported record format
- Explicitly isolate or reject without damaging the original record, and do not treat unknown as missing and then rebuild immediately; after the fault is cleared, the original data is usable.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C10"></a>

## C10: heal.dangling-and-orphan-safety

Topologies: H1,H2. Branches: remove-false, remove-true-before-grace, remove-true-after-grace, shared-version-reference, pending-mpu.

1. Create DV/DM and form candidate orphans by a legal overwrite or delete; with the matching format parser, list every version→data_dir reference and pending UploadId.
2. Form dangling only in this test's directories that have no live reference; save the orphan's cause and its real age, and have the grace-period instance wait for the configured window without changing the global time.
3. START remove=false and remove=true separately; items still within the grace period, shared historical references, and a pending MPU must be kept, and only an orphan that has expired and has sufficient delete proof may be cleaned up.
4. Compare the physical reference set: every version that should remain, and list_parts of incomplete uploads, are complete; if remove deletes any shared reference by mistake, it fails, and rescue happens only after evidence collection.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Construct a dangling part, an old data_dir, and a shared historical reference, and check remove=false and remove=true separately, before and after the grace period
- By default, do not destroy data that can still be recovered; an explicit remove clears only this orphan, and only when it has sufficient delete proof and has expired, and does not delete a shared version or an MPU.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="C11"></a>

## C11: heal.bucket-incarnation-recreation

Topologies: H1,H2. Branches: old-metadata-rejoin, old-checkpoint-replay, old-mrf-replay, native-incarnation-fence.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`EVIDENCE_INCARNATION_LIVE_ORCHESTRATION`): H1/H2 old-metadata rejoin, real checkpoint/resume replay, ACK-bound committed MRF replay, and the precise native evidence gate are implemented; an independent audit of every live branch is still missing, diagnostic execution stays FAIL/BLOCKED, and a passing native prerequisite cannot replace canonical acceptance.

1. Create an old DV bucket and take one disk offline; list and delete every version and marker through normal S3, confirm that delete_bucket succeeds, and do not wipe internally to bypass deletion.
2. After create_bucket with the same name, write new content and save the new incarnation; restore the old disk and let the old task and the persistent MRF replay.
3. Compare the task identity and the generation guard: old responsibility must not delete or overwrite the new object, and must not resurrect an old marker or old data into the new bucket.
4. Per NATIVE, verify the commit fence of the exact incarnation; on the fleet, the manifest and the new bucket configuration always match the successful ACKs, and the old and new physical ranges are kept as separate evidence.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [S3](validation-heal-operations.md#S3) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [START](validation-heal-operations.md#START) → [FORMAT](validation-heal-operations.md#FORMAT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- While a disk is offline, complete a legitimate empty or delete of the old bucket, then create a bucket of the same name and write new objects, and bring the old disk and the old tasks back
- The old bucket incarnation's checkpoint/MRF/metadata must not contaminate the new bucket; old tasks must not delete or restore objects of the new incarnation.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D01"></a>

## D01: heal.object-size-boundaries

Topologies: H1,H2. Branches: zero, one, 4kib-minus-exact-plus, inline-minus-exact-plus, stripe-minus-exact-plus, large-external.

1. SEED_DB explicitly includes 0, 1, 4095, 4096, and 4097; obtain the inline and EC block thresholds from the fixed candidate configuration and FORMAT, and add threshold±1 and one multi-block external object.
2. For each size, first confirm the actual form from the physical record; when a zero-byte or inline object has no external part, switch to a legal metadata or inline fault, and do not invent a part.
3. On data and on parity, apply missing and then bitflip, and START deep/WAIT; count each form as an independent variant.
4. Check every length, digest, ETag, HEAD, and LIST; a Range that crosses a boundary and a Range of the last byte are a correct 206, and a range on an empty object is rejected per the frozen contract.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RANGE](validation-heal-operations.md#RANGE) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Cover 0-byte and 1-byte objects, page/inline/EC block boundaries ±1, and large objects
- Inject only after each actual storage form is confirmed; length, SHA-256, ETag, HEAD/LIST, and boundary Range are correct, with no assumed threshold.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D02"></a>

## D02: heal.completed-multipart-object

Topologies: H1,H2. Branches: first-part, middle-part, last-part, cross-part-range.

1. SEED_DM creates at least three parts, with every non-final part ≥5 MiB, unequal content lengths, and independent nonces; save the UploadId, each part's ETag, and the Complete ACK and final version.
2. Each instance corrupts one physical shard corresponding to the first, the middle, or the last part; map the actual part per FORMAT, and do not guess the logical position from the file name.
3. After START deep/WAIT, first verify the corresponding target shard, then do a full GET; the final ETag should match the baseline, but do not compute the whole-object MD5 from the multipart ETag.
4. RANGE covers two part boundaries and the tail; compare byte-by-byte/digest, metadata, and version; other incomplete MPUs can still list_parts.

Operations: [SEED_DM](validation-heal-operations.md#SEED_DM) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RANGE](validation-heal-operations.md#RANGE) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Repair a completed multipart object with unequal part lengths, corrupting the first, middle, and last part separately
- Full content, part order, the original version/ETag/metadata, and cross-part Range stay unchanged; a multipart ETag must not be treated as the whole-object MD5.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D03"></a>

## D03: heal.historical-versions

Topologies: H1,H2. Branches: old-version-part, current-version-part, recursive-all-versions, multiple-version-targets.

1. SEED_DV writes three different payload versions per key and saves each versionId/ETag; a marker may also be written. The manifest must not substitute the current GET for historical reads.
2. Select different disks and parts for different historical/current versions; the metadata-fault instance retains a same-version quorum.
3. START/WAIT separately for the object and the recursive bucket; physical parsing must show every version/data_dir that should be stored, not only the latest object.
4. Full GET of every original versionId; LIST versions has no additions or omissions; isLatest identity and marker order are preserved.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Place different corruption locations on different versions, and repair by object and by recursive scope
- Every original versionId is addressable and its content is correct, the latest version identity does not drift, and there are no extra versions or omitted historical versions.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D04"></a>

## D04: heal.delete-marker-semantics

Topologies: H1,H2. Branches: current-marker, historical-marker, missing-marker-replica, stale-disk-marker.

1. Create three data versions and current/historical markers, and record the VersionId/DeleteMarker returned by delete_object.
2. The instance missing a marker uses a minority of xl.meta replicas missing; the stale instance is formed by legitimately creating/deleting a marker after a member goes offline, and arbitrary metadata must not be edited directly.
3. After heal, current GET/HEAD of the current marker should be 404 with the correct marker identity headers; GET of a specified marker versionId is 405 per the S3 contract and retains the version headers.
4. The exact LIST versions set matches the latest marker; after the current marker is explicitly deleted, what is exposed should be the expected historical object, and no extra data version may be produced.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [S3](validation-heal-operations.md#S3) → [FAULT](validation-heal-operations.md#FAULT) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- After marker replicas are missing or stale, heal, and query by the current value and the exact marker versionId
- The status codes and identity headers of the current GET/HEAD and of the specified marker match the contract; the marker does not resurrect data, and LIST versions has no duplicates.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D05"></a>

## D05: heal.versioning-suspended-null

Topologies: H1,H2. Branches: enabled-to-suspended, null-overwrite, null-delete-marker.

1. Enable versioning and write three generations, then put_bucket_versioning with Status=Suspended, write the null version twice in succession, and create/delete a null marker.
2. The baseline saves both the non-null history and the current null identity; induce a minority missing on the target external part or xl.meta.
3. START deep/WAIT; record that null must not be converted into a random versionId, and must not have its shards mixed with historical versions from the versioning-enabled period.
4. VERIFY every original versionId, the null content, and the marker; after versioning is enabled again, write a new object and verify the old history is still addressable.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [S3](validation-heal-operations.md#S3) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Build the fixture across versioning enabled, suspended, and null-version overwrite, then repair
- Null and non-null versions are not confused, and no fake version is added; historical objects from before suspension remain accurately readable.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D06"></a>

## D06: heal.object-metadata-and-tags

Topologies: H1,H2. Branches: user-metadata, content-headers, versioned-tags.

1. Use DS to set metadata, Content-Type, Content-Encoding, Content-Disposition, Cache-Control, and two tag sets to encodable Chinese values; the content itself uses the declared encoding.
2. First record GET/HEAD/get_object_tagging exactly; fault the external part and a minority of xl.meta separately.
3. START deep/WAIT, and compare every externally visible attribute and the per-version tags; repair adds no version and does not drop the case-normalization result of user keys.
4. Legitimate rewrite of internal metadata is allowed, but it must be proven by FORMAT references and physical content; do not treat Content-Encoding as server-side compression being enabled.

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [BASELINE](validation-heal-operations.md#BASELINE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Repair objects that include user metadata, Content-Type/Encoding/Disposition, and tags
- External metadata, tags, and the related response headers match item by item; unrelated internal metadata bytes are not required to be identical.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D07"></a>

## D07: heal.range-head-and-conditions

Topologies: H1,H2. Branches: head, range-valid, range-outside, if-match, if-none-match, if-modified-since, if-unmodified-since.

1. The DB/DM baseline saves ETag/LastModified and cross-part content; first issue all conditional requests and freeze the expected 200/206/304/412/416.
2. Induce missing/bad checksum on the exact part and repair; issue the same set of HEAD, Range, IfMatch/IfNoneMatch, and time-condition requests again.
3. For 304/412/416, verify the response contains no erroneous success content; for a valid range, compare the exact length, ContentRange, and the original fragment digest.
4. Fully VERIFY the version and metadata; conditional requests must be passed as API parameters, and status codes must not be simulated by a local short-circuit.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [RANGE](validation-heal-operations.md#RANGE) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [RANGE](validation-heal-operations.md#RANGE) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Before and after repair, verify HEAD, valid and out-of-range Range, If-Match/If-None-Match, and time conditionals
- Judge 200/206/304/412/416 and similar codes against the per-request contract, and never return old content, a wrong range, or a wrong ETag.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D08"></a>

## D08: heal.encrypted-objects

Topologies: H1,H2. Branches: sse-s3, sse-kms, sse-c, encrypted-versioned.

1. DEPENDENCY first verifies that every SSE the candidate supports and the dedicated KMS/key are ready; unsupported product capabilities are recorded separately under an explicit error contract, and that parameter instance must not be deleted.
2. Create the object with S3 ServerSideEncryption/SSEKMSKeyId or SSECustomerAlgorithm/SSECustomerKey; inject the key from the environment into memory, and do not place it on the command line or in the manifest.
3. At the ciphertext physical layer, select a part for missing/bitflip; after deep heal, compare the original ciphertext shard of the same role, or verify by re-encoding that matches the candidate.
4. With correct authorization, decrypt-GET each version to check the plaintext; with a missing/wrong key, verify the request is rejected. Do not write a plaintext substitute, and do not classify a key error as not-found.

Operations: [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [SEED_DE](validation-heal-operations.md#SEED_DE) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Create encrypted objects with the SSE-S3/SSE-KMS/SSE-C modes the candidate supports, and repair them
- After authorized decryption, the original content and version match, and physical verification uses the correct encoding/ciphertext layer; when the key is unavailable, do not delete by mistake or downgrade to plaintext, and redact the evidence.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D09"></a>

## D09: heal.object-lock-retention

Topologies: H1,H2. Branches: governance-retention, compliance-retention, legal-hold, historical-protected-version.

1. Create a dedicated bucket with ObjectLock enabled. SEED_DO uses a short retention that has genuinely not expired, or legal hold ON; record the UTC deadline and the exclusive-resource expiry schedule.
2. DELETE of a protected versionId by a role without bypass permission must be AccessDenied, confirming the protection was effective to begin with.
3. After a missing shard or missing xl.meta on a protected version, repair and check the content and get_object_retention/legal_hold; a further delete within the retention period is still rejected.
4. Legal hold is released only through the normal API; Compliance is cleaned up only after genuine expiry. Changing the clock or deleting files internally is forbidden. An unexpired scene is marked RETAINED and must not be handed back as an empty environment.

Operations: [SEED_DO](validation-heal-operations.md#SEED_DO) → [S3](validation-heal-operations.md#S3) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [RECOVER](validation-heal-operations.md#RECOVER)

Source assertions:

- Repair a protected version with retention/legal hold enabled, and test an illegal delete during that interval
- Heal does not bypass retention/hold and does not lose their metadata; both the rejected delete and the legitimate repair have explicit evidence.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D10"></a>

## D10: heal.special-keys-and-pagination

Topologies: H1,H2. Branches: unicode-space-percent, encoded-slash, adjacent-prefixes, multi-page-list.

1. DS creates keys with Chinese, spaces, a literal %2F, a real /, and multi-level prefixes; at least 257 objects, with List MaxKeys=17, to ensure the listing crosses pages.
2. FORMAT builds the logical key→physical path mapping and confirms %2F is not confused with /; file faults use only a constrained relative-path tool, and do not concatenate a shell command.
3. For the selected keys, apply missing part/xl.meta separately, using the URL percent-encoded Admin scope, then deep heal.
4. Paginated LIST/versions and the manifest have no omissions or duplicates; the control prefix is unchanged; exact GET and metadata checks for every special key.

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Cover Chinese, spaces, percent signs, encoded slashes, multi-level prefixes, and multi-page object listings
- The logical key maps accurately to the physical path and does not cross buckets or prefixes; pagination has no omissions or duplicates, and a user key must not be treated as an executable shell command or a bare file path.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="D11"></a>

## D11: heal.bucket-metadata-integrity

Topologies: H1,H2. Branches: versioning-config, bucket-policy-tags, lock-config, unknown-record-negative.

1. DS/DV creates an isolated bucket configuration: versioning, a policy that authorizes only this bucket, bucket tags, and an expirable lock configuration; save the baseline by S3 GET of the configuration.
2. The FORMAT_BAD allowlist contains only the system bucket metadata files for that bucket; after the matching format/recoverable contract has been verified, a minority of replicas are missing/truncated.
3. Do not use the generic heal_fault to modify .rustfs.sys; operate per the dedicated system-record runbook with the service stopped, an exact backup, and an approved manifest. Unknown-format instances require only a safe rejection.
4. After repair, read and compare the configuration through each API; business-object versions are complete. Confirm that the IAM/credential configuration and the system records of other buckets are completely unchanged.

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [S3](validation-heal-operations.md#S3) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- Within the isolated-bucket allowlist, test supported bucket-metadata recovery, and check the external bucket configuration
- Versioning settings and policy/tag/retention configuration are kept per the contract; do not walk credential configuration or treat an unknown internal record as rebuildable data.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="E01"></a>

## E01: heal.manual-normal-attribution

Topologies: H1,H2. Branches: normal-missing-data, normal-missing-parity.

1. Deploy the manual configuration: turn off scanner, auto-heal, and MRF; keep format, and record the service's actual environment and PID, not only the intended env file.
2. After FAULT missing, the target cannot be GET/HEAD; first confirm the missing state is still present, then START scanMode=1.
3. WAIT for the exact token/outcome; PHYSICAL first proves the target was newly generated, then VERIFY. Source attribution needs to combine the configuration, the timeline, and the task object.
4. A normal scan that does not find silent bitrot must not fail E01 for that reason; that is the deep instance of E02/C02/C03.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Do not pre-read the target, keep format, isolate background interference, then explicitly run normal heal
- Prove that this Admin token actually covers the target and repairs the missing data that normal can detect; normal is not required to find all silent bitrot.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="E02"></a>

## E02: heal.manual-deep-attribution

Topologies: H1,H2. Branches: deep-parity-bitrot, deep-data-bitrot.

1. The manual configuration excludes other background sources; FORMAT confirms the shard role, injects a bitflip, and saves evidence that the bad shard still exists.
2. Do not read the target first; START scanMode=2; retain the request body, clientToken, incarnation/pool/set, and the final object disposition.
3. First verify the bad shard is fully restored, then read the original content; if restoration already occurred before the task started, record ATTRIBUTION_NOT_OBSERVED.
4. Check that the healthy control was not modified unexpectedly; source must not be inferred only from the last command issued.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- On parity corruption where an ordinary read can still succeed, explicitly run deep heal
- Prove the deep check and the repair by token, object, version, and physical change, and do not credit a background repair that finished early to the manual task.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="E03"></a>

## E03: heal.read-repair-attribution

Topologies: H1,H2. Branches: get-missing, range-bitrot, historical-version-read.

1. Turn off scanner/auto-heal; per the candidate's read-repair implementation, configure the MRF queue it requires and record why it is enabled; do not send an Admin heal.
2. After FAULT, physically prove the corruption still exists; send only one GET or Range, with a request ID, for one exact key/version.
3. It is not enough that the GET content is already correct: observe only PHYSICAL/background state/logs until that shard is rebuilt; the async path needs to correlate read-repair enqueue and completion of the responsibility.
4. Run a full VERIFY only after shard repair completes; if only a successful read is visible and source evidence cannot be found, that path remains NOT_OBSERVED and must not count as PASS.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [CONFIG](validation-heal-operations.md#CONFIG) → [FAULT](validation-heal-operations.md#FAULT) → [GET](validation-heal-operations.md#GET) → [RANGE](validation-heal-operations.md#RANGE) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [LOGS](validation-heal-operations.md#LOGS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Do not issue an Admin heal; issue one traceable GET/Range against the confirmed, specifically corrupted object
- The read is correct and a real on-disk shard reconstruction follows; if it goes through the async queue, source=read-repair must be proven, and a successful GET alone is not enough.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="E04"></a>

## E04: heal.scanner-cold-object-repair

Topologies: H1,H2. Branches: cold-missing, cold-bitrot, scanner-cycle-boundary.

1. Enable the scanner and the required automatic repair; pre-register cycle/start delay/bitrot cycle and the budget; first use a healthy cold object to prove the scanner actually inspects.
2. During injection, use PROCESS for a short stop to hold a quiescent window; after restart, do not send a target GET/HEAD and do not send an Admin heal.
3. Use only disk snapshots, scanner traversal logs, and background responsibility observation to prove the current cycle inspects the target, and record discovery→enqueue→shard repair.
4. Run a full VERIFY only after PHYSICAL completes; a mere cycle-count increase, global idle, or another automatic replacement repairing first must not be recorded as the scanner passing.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [PROCESS](validation-heal-operations.md#PROCESS) → [FAULT](validation-heal-operations.md#FAULT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [LOGS](validation-heal-operations.md#LOGS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Do not GET/HEAD the corrupted target and do not issue a manual heal; wait only for the scanner path that is already enabled
- The cold object is discovered and repaired, with the source and the traversal window made explicit; it cannot pass if the scanner cycle ends without the target being checked.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="E05"></a>

## E05: heal.mrf-offline-write-replay

Topologies: H2. Branches: offline-put, offline-version-delete, offline-marker.

1. On H2, enable durable MRF; first verify that, when the target node is offline, the remaining members of the same set satisfy the commit condition; DRIVE_GATE or PROCESS affects only the target member.
2. Online nodes perform a unique-key PUT, an exact-version DELETE, and marker operations; save each ACK and responsibility identity.
3. While the target is still offline, collect the journal/state to prove the responsibility has been persisted; restore the same disk identity, and do not issue a manual heal.
4. Observe MRF replay and the retire reason; retire only after the result is actually repaired/verified_healthy/authoritatively_absent; VERIFY every ACK and the target shard, and do not use an empty queue as the sole proof.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [CONFIG](validation-heal-operations.md#CONFIG) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- While a member is offline, complete a quorum PUT/DELETE, confirm the pending-repair responsibility, then rejoin
- The durable MRF responsibility persists and its identity is exact; release it only after a proven result such as repaired/verified healthy/authoritatively absent, and a successful commit is not lost.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="E06"></a>

## E06: heal.replacement-auto-rebuild

Topologies: H1,H2. Branches: replacement-unversioned, replacement-versioned-marker.

1. Enable replacement auto-heal; REPLACE injects an empty logical disk, and the old disk's backup is unreachable to the service; save the new UUID/slot.
2. After restart/acceptance, do not send an Admin heal and do not read the target; query only BACKGROUND/REPLACEMENT, logs, and PHYSICAL.
3. From the replacement generation, ready, and responsibility-completion state, correlate the shard-repair timeline of every manifest object/version.
4. VERIFY only after every target shard has actually been rebuilt; service ready, a counter reset, or an empty queue must not end the run early.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [CONFIG](validation-heal-operations.md#CONFIG) → [REPLACE](validation-heal-operations.md#REPLACE) → [FORMAT](validation-heal-operations.md#FORMAT) → [REPLACEMENT](validation-heal-operations.md#REPLACEMENT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- After the replacement disk is accepted, do not issue an Admin or target GET; observe automatic replacement heal
- The replacement generation, ready/completion, and every target version on disk agree; service ready, an empty queue, or a counter reset cannot substitute for completion.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F01"></a>

## F01: heal.root-graceful-coordinator-restart

Topologies: H1,H2. Branches: root-partial-graceful, same-record-disk-resume.

1. Before DX root heal starts, save the original coordinator, topology, record disk, and budget; collect every root checkpoint through the CHECKPOINT read-only allowlist.
2. WINDOW proves 0 < repaired target count < total and that the task is running, then PROCESS graceful-stop of the original coordinator; record the exit signal and the persistent record.
3. Restart the original coordinator with the original unit/env/data root/candidate, without replacing the record disk; observe recovery of the original task ID/remaining responsibility, and do not treat a new scan as the original task continuing.
4. In the end every object/shard is correct, the root activity record is retired, and repeated restarts produce no duplicate task; unobserved partial progress remains NOT_OBSERVED.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [STATUS](validation-heal-operations.md#STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- While root heal has non-zero progress, gracefully restart the original coordinator, keeping the original topology and the record disk
- Within the explicitly supported boundary, restore the original task ID and the remaining responsibility/budget, and leave no active record after completion; do not expand this into failover after the coordinator disk is lost.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F02"></a>

## F02: heal.target-graceful-restart

Topologies: H2. Branches: target-partial-graceful, versioned-target-partial.

1. REPLACE the target logical disk and enable automatic rebuild, using DX/DV so the target total is observable; the coordinator is not the restart target.
2. PHYSICAL and REPLACEMENT both confirm the target has completed only part, then PROCESS gracefully stops the target unit; save MainPID, the exit code, and boot_id before and after.
3. Keep the same new disk UUID/generation and start the original target service normally; do not switch to a new directory and retest.
4. Observe that the remaining targets continue to build with consistent task responsibility, and that every version/marker/shard is complete; the backup must not participate in the rebuild.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [REPLACE](validation-heal-operations.md#REPLACE) → [REPLACEMENT](validation-heal-operations.md#REPLACEMENT) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- When the replacement target has completed only part of the reconstruction, gracefully restart the target node
- The fault actually occurs inside the partial-completion window; after the restart, reconstruction continues on the same target disk/generation, and the final manifest and all target shards are correct.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F03"></a>

## F03: heal.target-process-crash

Topologies: H2. Branches: target-partial-sigkill, target-partial-sigkill-versioned.

1. Prepare the replacement disk and observable partial progress the same way as F02, and first freeze the MainPID/process start time of the unit that will be killed.
2. Within the WINDOW, PROCESS kill, exactly systemctl kill --signal=SIGKILL --kill-whom=main; record signal=9 and a non-graceful exit, and state explicitly that this is not a physical power loss.
3. Start normally with the same unit/root/new disk UUID; record the recovered responsibility/generation, and do not allow objects that are not yet complete to be discarded.
4. Full VERIFY/PHYSICAL; check that temporary files and persistent responsibilities are not leaked. A final pass does not erase the evidence of this real interruption.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [REPLACE](validation-heal-operations.md#REPLACE) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- During partial reconstruction, SIGKILL the test process, then start it normally
- There is real evidence of the unclean shutdown and of the repair progress; committed objects are not lost, and an unfinished target is not treated as complete; this is explicitly not a power-loss test.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F04"></a>

## F04: heal.coordinator-process-crash

Topologies: H2. Branches: root-coordinator-kill, mrf-coordinator-kill, replacement-coordinator-kill.

1. The three mechanisms use independent scenarios and a frozen persistence-support table; collect CHECKPOINT while the coordinator has nonzero, incomplete progress.
2. PROCESS SIGKILL the coordinator; the target node stays up. Restore the original root/original topology.
3. Check each mechanism's promise separately: a durable task must continue its responsibility; loss of a volatile token must not count as completed. It should show incomplete and allow an explicit new-scan compensation.
4. Save the original task and the compensation task separately; finally VERIFY all ACKs, versions, and shards. A successful safe rescan must not be recorded as automatic recovery of the original token.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DB](validation-heal-operations.md#SEED_DB) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- The coordinator exits ungracefully and then recovers; observe root, MRF, and replacement separately
- Judge by each mechanism's verified persistence boundary; a lost volatile token must not be treated as success, remaining responsibility must be recorded explicitly, and recovery by a safe rescan must be possible.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F05"></a>

## F05: heal.full-cluster-restart

Topologies: H2. Branches: all-graceful-restart, all-restart-with-mpu, repeat-full-restart.

1. Prepare DB/DV/DM, a pending UploadId, and an incomplete durable repair responsibility; freeze every node/disk/slot.
2. After the WINDOW is hit, stop only all units of this case, in a fixed order; save each node's exit code, checkpoint/MRF, and the full timeline.
3. Restart on the original topology, simultaneously or in the pre-registered order, and wait for every peer; do not recover by automatic reformat or a new data root.
4. After completion, verify every version/marker/MPU and physical shard, then repeat one restart to verify idempotence; any lost already-promised responsibility counts as a failure.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- While unfinished repair responsibility remains, restart all test nodes, with the original topology and disk identity fixed
- Responsibility that has a persistence guarantee is not lost; all endpoints recover consistently, old versions/markers/MPUs and the target shards are complete, and repeated restarts do not spread the error.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F06"></a>

## F06: heal.cancelled-task-restart

Topologies: H1,H2. Branches: cancel-restart, old-token-after-restart, force-new-after-cancel.

1. STOP the DX missing-shard task during the WINDOW; STATUS and the disk quiescence window prove that cancellation and in-flight exit have completed.
2. Save the old token/checkpoint and PROCESS a graceful restart; confirm the cancelled task does not revive on its own or report repaired.
3. A new START/forceStart obtains an independent token; send stop/status to the old token. It must not affect the new task.
4. After the new task completes, VERIFY/PHYSICAL; the cancellation record and the new task's responsibility must not cross generations.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [STOP](validation-heal-operations.md#STOP) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [STATUS](validation-heal-operations.md#STATUS) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL)

Source assertions:

- Restart after cancellation and in-flight exit have finished, then forceStart or a new task repairs normally
- A cancelled task does not revive on its own; an old checkpoint/token cannot cancel or overwrite a new task, and old responsibility is not marked repair-complete by mistake.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F07"></a>

## F07: heal.checkpoint-and-mrf-corruption

Topologies: H1,H2. Branches: checkpoint-truncate, mrf-checksum, future-schema, stale-generation, journal-write-failure, native-persist-fence.

When a live read meets an old file handle whose link count is zero after an atomic replace, first check the file type, size, and mount boundary, then reject the old handle's bytes and report that the read changed. Only the original bounded observation loop may retry. Reads and fault injection after stop still fail immediately. Hard links, non-regular files, over-limit files, and mount escapes are still rejected. The original pidfd, the 60-second advance budget, and the complete checkpoint/resume/root checks are unchanged. Reproducing the Linux atomic-replace component only proves a read race. It does not prove the sole cause of a previous live failure, and it does not replace RustFS persistence-fence evidence.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`EVIDENCE_DURABLE_JOURNAL_ORCHESTRATION`): Implemented H1/H2 checkpoint truncation, future schema, replay of a real earlier generation, a committed MRF CRC fault, real journal ENOSPC on a private quota volume, and an independent safe rescan; the 10 native regressions are only prerequisite evidence. Still missing are review of every required fsync/commit boundary and an independent live audit of the complete live branches. Diagnostic execution cannot raise canonical acceptance.

Journal ENOSPC recovery preserves the first failed checkpoint/terminal publication
attempt before restarting the original durable owner after filler release. A new
rescan is permitted only after that same responsibility either completes with
full physical and durable terminal proof or retains an exact failed terminal
without a runnable owner. A still-pending scope remains a failure. Successful
original replay must repair the complete remaining manifest before any target
GET; the later distinct rescan must report zero repairs and preserve original
completion bytes. No forceStart, owner deletion or old-record restoration is
used to bypass overlap admission. This flow still requires fresh live evidence.

1. First create and prove a real durable root/MRF responsibility; CHECKPOINT locates the exact task/record of this run, and other system records must not be touched.
2. Stop the test service and, per FAULT_FORMAT, apply truncation, a bad checksum, a future schema, and an older generation separately to these reviewed records; the write-failure instance uses an exclusive quota fault layer and must not fill a real shared disk.
3. After restart, check the parse/error state: unknown or damaged records are retained and diagnosable, and must not be silently deleted and ACKed; the responsibility can still be handled by a safe rescan.
4. Run the NATIVE regression in the exact fsync/commit crash window and save evidence that the barrier was hit; do not count corruption recorded on the fleet together with the native boundary.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [START](validation-heal-operations.md#START) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PROCESS](validation-heal-operations.md#PROCESS) → [FAULT_FORMAT](validation-heal-operations.md#FAULT_FORMAT) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Under control, construct checkpoint/MRF truncation, a bad checksum, a future schema, a stale generation, and a write failure
- Confirmed persisted responsibility must not be silently deleted or given a false ACK; unknown records are retained and diagnosable; the exact crash window is proven by a native barrier and is not falsely claimed as a hit on the live fleet.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F08"></a>

## F08: heal.mixed-version-upgrade-rollback

Topologies: H2. Branches: rolling-old-new, mixed-peer-status, rollback-new-old, mixed-restart.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_IMMUTABLE_RELEASE_PAIR`): Requires two pinned, mutually compatible RustFS releases, mixed-version peer state, and a reviewed rollback-compatibility matrix; the current auto-testing Heal node does not have this immutable dual-version resource set.

1. Pin the commit/tree/binary/format-compatibility notes of two release optimized packages; UPGRADE first verifies a record version that supports rollback, and must not use an arbitrary old package.
2. When a durable repair responsibility exists, upgrade node by node, keeping the data root and unit; during the mixed phase, pass STATUS/BACKGROUND on each endpoint and continue unique-version writes.
3. A peer that lacks the capability must show partial/unsupported and must not be aggregated into complete; new records must not be retired by mistake by an old node.
4. Roll back and restart according to the compatibility plan, and verify all ACKs and responsibilities; incompatible formats are tested only for explicit rejection/protection. A risky downgrade overwrite is forbidden.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [START](validation-heal-operations.md#START) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [UPGRADE](validation-heal-operations.md#UPGRADE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [STATUS](validation-heal-operations.md#STATUS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Pin two explicitly compatible candidates, one before and one after, covering rolling upgrade, mixed peers, rollback, and restart
- Insufficient capability must be shown as partial/unsupported rather than complete success; retain responsibility under the format-compatibility contract, and an older version must not mistakenly retire a newer version's records.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="F09"></a>

## F09: heal.dedicated-host-forced-restart

Topologies: H6. Branches: dedicated-vm-forced-stop, dedicated-host-power-cycle.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_H6_HOST`): Requires a dedicated H6 host, platform force stop/start permission, out-of-band recovery, and boot_id/platform-event evidence; a shared Heal node cannot prove a real power cycle.

1. First obtain the H6 dedicated resource ID, proof of exclusive use, and out-of-band recovery approval; an ordinary fixed fleet must not run this case.
2. Continue limited-volume writes and, in the replacement partial-rebuild window, record the set of writes with a successful ACK or an unknown result, and the latest persistence evidence.
3. HOST_POWER uses an approved platform command to force stop and start, retaining the platform activity ID, timeline, and boot_id; VM stop and a real power loss are named as separate instances.
4. After recovery, restart on the original disk/root and resolve the unknown writes; accept every version/shard and durable responsibility. Do not treat SIGKILL or a software reboot as evidence for this item.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [REPLACE](validation-heal-operations.md#REPLACE) → [WINDOW](validation-heal-operations.md#WINDOW) → [HOST_POWER](validation-heal-operations.md#HOST_POWER) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Under specific authorization, force-restart the exclusive test VM/host during partial repair or continuous writes
- Retain platform events and the ACK ledger; verify the on-disk write and recovery, and do not treat SIGKILL as power loss; a forced VM stop is not equivalent to power loss of the physical controller either.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G01"></a>

## G01: heal.race-unversioned-overwrite

Topologies: H1,H2. Branches: overwrite-vs-deep, overwrite-vs-read-repair, native-commit-cleanup.

1. The overwrite client, with a per-key serial nonce, is independent of the heal client; WORKLOAD records request-start/end, ACK, and payload hash every time, and does not decide the last version from cross-machine wall clocks.
2. After the old object has a missing shard/bitflip, run both sides at the same time; WINDOW proves actual overlap. Same-key PUTs of each worker must be serial, so the expected value is not itself unordered.
3. After writes stop, resolve every unknown ACK and check the latest GET and data_dir against the last provable commit; do not roll back to the old content.
4. Run the NATIVE commit/cleanup barrier instance; a real race that misses the exact window counts only as a black-box instance. Record the no-lock/lock-revalidation logs.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Successive overwrites of the same key proceed concurrently with heal, including the commit-tail cleanup window
- In a provable commit order, verify that the final result is the last successful write; do not roll back to the old data_dir, and resolve an unconfirmed timed-out write separately.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G02"></a>

## G02: heal.race-versioned-put

Topologies: H1,H2. Branches: new-version-vs-historical-heal, read-all-versions-during-heal, native-version-fence.

1. When a DV historical version is missing a shard, the foreground serially writes a new version with an independent payload and saves every successful versionId.
2. START deep overlaps WORKLOAD; the reader always specifies versionId to verify old and new content, and must not substitute a current GET.
3. After quiescence, rebuild the expected version set from the complete ACK ledger; the latest value is determined by serial commit order. A full PHYSICAL checks that different data_dirs have no mixed shards.
4. Run NATIVE on the exact-version commit barrier; any unresolved timed-out write or missing ACK blocks the final pass.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- While repairing a historical version, create a new version and read each versionId repeatedly
- Old and new versions do not mix shards or cross content; every version that has already returned success is retained, and the latest value is accepted according to a determined order.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G03"></a>

## G03: heal.race-version-delete

Topologies: H1,H2. Branches: delete-old-version-vs-deep, delete-current-version-vs-deep, native-delete-fence.

1. Select an exact versionId from DV and induce a recoverable missing shard; the foreground delete request explicitly carries that ID, and does not only create a marker.
2. Within the WINDOW, run delete_object concurrently with deep heal, and save the DELETE success receipt and the heal scope/identity.
3. In the terminal state, GET of the exact version should find it absent, and LIST versions must not resurrect it; the content of every other version is preserved.
4. Run NATIVE at the delete/repair commit interleaving point; cleanup of the old shard is only final physical evidence, and a backup must not be used first to resurrect a deleted version.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Issue DELETE for the exact corrupted historical versionId, racing deep heal
- A version whose deletion is confirmed cannot be resurrected, and other historical versions are unaffected; the native boundary test and the live race on the fleet each keep their own evidence.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G04"></a>

## G04: heal.race-markers-and-batch-delete

Topologies: H1,H2. Branches: create-marker, delete-marker, batch-version-delete, unversioned-delete, native-marker-fence.

1. Create a versioned bucket and an unversioned bucket separately; run marker create/delete, batch delete with versionId, and versionless DELETE separately, without mixing the API semantics.
2. Start heal with a minority of xl.meta missing, and submit the frozen delete list at the same time; delete_objects must check Deleted/Errors item by item. HTTP 200 does not mean every item succeeded.
3. From per-item ACKs, compute the final sets of data versions, markers, and latest values; exact GET verifies absent items and retained items.
4. native-marker-fence is evidenced independently; recovering the fault must not overwrite back old metadata that was already confirmed deleted.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DB](validation-heal-operations.md#SEED_DB) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Interleave separately with marker create/delete, batch version delete, and unversioned delete
- Check the version/marker set against each DELETE receipt, with no resurrection, no mistaken deletion, and no omitted batch error.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G05"></a>

## G05: heal.race-multipart-complete-abort

Topologies: H1,H2. Branches: upload-continue, complete, abort, native-mpu-fence.

1. DM prepares, at the same time, a committed object and two identifiable pending UploadIds, each with an independent part/ETag.
2. The committed object is missing a shard and is healed; concurrently resume upload-A and Complete it, and Abort upload-B. WINDOW saves the actual overlap.
3. The version returned by Complete appears only once, and the full content is the ACKed parts sorted by PartNumber; the Aborted UploadId can no longer list_parts, and must not harm the other upload or the committed object.
4. Check every MPU listing and physical directory; the native commit/cancel barrier uses NATIVE separately. Issuing concurrent requests alone must not be claimed as a hit.

Operations: [SEED_DM](validation-heal-operations.md#SEED_DM) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- During heal, separately resume, Complete, and Abort different known UploadIds
- Content from a successful Complete appears only once; Abort does not damage a committed object or another upload; an unfinished part is not forged into a complete object.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G06"></a>

## G06: heal.race-copy-and-metadata-update

Topologies: H1,H2. Branches: copy-source, copy-destination, replace-metadata, update-tags, native-copy-fence.

1. The DS source and target use different content and metadata; the copy request's CopySource uses structured Bucket/Key/VersionId and does not concatenate a path string.
2. The source or the target is missing a shard and is healed; in the same window, run COPY, a self-copy with MetadataDirective=REPLACE, and put_object_tagging separately.
3. Save the corresponding commit ETag/versionId and the update order, and verify the final content matches that commit's metadata/tags combination; an old read-repair must revalidate the generation.
4. Check that the source, the target, and the control have no cross-overwrite; NATIVE records the exact window of lock upgrade/stale-commit rejection.

Operations: [SEED_DS](validation-heal-operations.md#SEED_DS) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [VERIFY](validation-heal-operations.md#VERIFY) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Interleave COPY of the source/target and tag/metadata updates with repair
- The final object content matches the metadata of the corresponding commit; when read-repair finds a new generation, it re-verifies or abandons the stale commit instead of overwriting.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G07"></a>

## G07: heal.race-ilm-expiry-and-transition

Topologies: H2. Branches: current-expiry, noncurrent-expiry, transition, restore, native-ilm-fence.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_ILM_TIER_WINDOW`): Requires an isolated ILM/tier tenant, a real Expire/Transition/Restore window, and remote version mapping; a shared ILM environment cannot safely create an overlap window.

1. Use the existing ILM dedicated remote-tier capability to create this case's bucket and rules; record the real expiry window/remote version mapping, and do not change rules on a shared bucket.
2. Start the missing-shard heal when Expire/Transition/Restore progress actually appears; WINDOW must prove overlap from logs/state. Finishing early can only be recorded as NOT_OBSERVED.
3. Expiry ACKs/ILM delete proofs decide which versions should be absent; heal must not resurrect them. Migrated objects verify the original version content through a real remote GET/restore.
4. Double-check local physical data and the remote reference; a successful source-side GET is not sufficient. Cleanup deletes only this case's rules/remote data.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DE](validation-heal-operations.md#SEED_DE) → [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [ILM](validation-heal-operations.md#ILM) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [S3](validation-heal-operations.md#S3) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Run in the same window as expiry of the current or historical version, and as remote transition/restore, respectively
- An authorized expiry is not restored; migration state and remote version references are correct; concurrent coverage counts only when overlap is actually observed, and an unconfigured tier must explicitly block the corresponding instance.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="G08"></a>

## G08: heal.race-replication-resync

Topologies: H2. Branches: replicate-put, replicate-delete, resync, native-replication-fence.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_ISOLATED_REPLICATION_PAIR`): Requires isolated replication endpoints, resync control, cross-endpoint version mapping, and an actual Heal/resync overlap window; the current fixed fleet has no exclusive replication pair.

1. Prepare two isolated replication endpoints/this case's buckets, an explicit source-target version mapping, and sync rules; first run one healthy replication control.
2. While the source heals a missing shard, concurrently submit PUT/DELETE and start the specified resync; save logs on both sides and the cross-endpoint version mapping to prove the operations overlap in time.
3. Fully list versions, markers, and content from both ends and compare them with the replication contract; if the source is correct but the remote side is missing or retries forever, the case must fail.
4. Restore the temporary rules and credentials, and confirm there is no infinite loop or repeated propagation; precise interleaving may be supplied only by matching NATIVE evidence.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [REPLICATION](validation-heal-operations.md#REPLICATION) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [WINDOW](validation-heal-operations.md#WINDOW) → [VERIFY](validation-heal-operations.md#VERIFY) → [LOGS](validation-heal-operations.md#LOGS) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- On both isolated replication ends, run create, delete, and resync concurrently with heal
- Peer versions, delete semantics, and source data end up consistent, with no infinite loop-back and no duplicate propagation; do not claim replication is correct from a source-side GET alone.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="H01"></a>

## H01: heal.with-expansion-rebalance

Topologies: H5. Branches: expand-while-data-heal, expand-while-parity-heal, versioned-rebalance.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_EXPANSION_REBALANCE`): Requires a complete topology in which H5 pool expansion, rebalance, and Heal are active at the same time, with pool/set migration evidence saved per object; current auto-testing has not bound the layout before and after expansion or a dedicated operation window.

1. Using the approved H5, first write DB/DV in the source pool and prove placement with FORMAT/PHYSICAL; the new pool is empty and is not included in the old selector.
2. After the target data/parity is missing or bitflipped, expand and restart per the POOL manual, confirm the new topology, and start rebalance and heal.
3. WINDOW saves evidence that both tasks are active and tracks source/target pool/set/version identity per object; looking only at global counts is not enough.
4. After both tasks complete normally, physically prove the target authority replicas, VERIFY every version, and retain the scanner backlog's persisted commit and retirement evidence.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [POOL](validation-heal-operations.md#POOL) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- First prove the source pool holds the fixture, then expand and rebalance concurrently with repair
- The repair target must not point at the wrong pool/set; old and new data are complete and the move terminal state is correct, and the scanner-backlog commit authority check is retained.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="H02"></a>

## H02: heal.with-rebalance-stop-resume

Topologies: H5. Branches: rebalance-stop, rebalance-resume, rebalance-service-restart.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_REBALANCE_LIFECYCLE`): Requires observable H5 rebalance, stop/resume/service-restart control, and independent task identities; current auto-testing has not bound the independent identities and operation windows of these two task classes.

1. On H5, use enough objects for movement to be observable, with missing-shard heal and rebalance active at the same time; record an independent task ID for each of the two classes.
2. Stop only rebalance, and observe that heal is not cancelled by mistake; then resume rebalance. The restart instance restarts only this case's unit.
3. Keep writing unique keys and save the ACKs; physical state and task state must stay continuous across stop and resume, and a new data root must not stand in for recovery.
4. In the end, each of the two task classes meets its completion contract, and the objects, versions, and move targets are correct; stopping again does not affect the new generation.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [POOL](validation-heal-operations.md#POOL) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- While heal is active, stop/restart/resume rebalance
- The two kinds of tasks have independent state and identity; stopping or resuming the move does not terminate repair by mistake or lose objects, and after resume both satisfy their own contracts.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="H03"></a>

## H03: heal.with-decommission

Topologies: H5. Branches: heal-decommission-source, heal-survivor-target, versioned-multipart.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_DECOMMISSION`): Requires H5 pool decommission, a survivor target, write restrictions, and move-ACK and physical-authority evidence; current auto-testing has not bound this case's topology before and after decommission, or its recovery plan.

1. First confirm that the fixture sits on the source pending decommission, and write a separate survivor control; on the source and on the target, inject recoverable faults that remain within quorum.
2. Start decommission and heal according to existing Pool behavior; during decommission, check rejections against the current write restrictions that match MinIO, and do not treat a rejected write as data loss.
3. Source data may be deleted only after the target commits authority; record the move ACK and target physical, not the number of temporary replicas.
4. After decommission completes, stop and remove the source, and read only the survivor for every version, MPU, and the content; if the result still depends on the source as a fallback, the case must fail.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [POOL](validation-heal-operations.md#POOL) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- On the source pending decommission and on the surviving target, separately create recoverable missing shards, then run decommission
- Do not delete the source early or pass a temporary replica off as a successful migration; accept according to the Pool's established write restrictions; after only the survivor remains, the data is still fully readable and there is physical proof.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="H04"></a>

## H04: heal.with-decommission-cancel-clear

Topologies: H5. Branches: cancel-during-heal, clear-after-inflight-exit, resume-new-generation, capacity-lock-retry-window.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_DECOMMISSION_CANCEL`): Requires decommission cancellation, cleanup, and new-generation recovery, plus the capacity-lock retry window required by #2423; an ordinary cancellation result cannot take the place of this boundary.

1. Cancel decommission while both the move and heal have partial progress; save the persisted cancellation record, the notifications, and the in-flight exit order.
2. After confirming that it has exited, Clear, then resume or create a new decommission; an old token or generation must not cancel the new task by mistake, and the continuous ACK ledger must not be lost.
3. The capacity-lock timeout instance requires a LOCK_WINDOW or a matching NATIVE barrier, and must actually witness the retry path involved in #2423; an ordinary successful cancellation cannot take its place.
4. After both flows converge, run VERIFY/PHYSICAL; if the lock window was not hit, record NOT_OBSERVED on its own, and do not claim coverage of the #2423 boundary.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [POOL](validation-heal-operations.md#POOL) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [LOCK_WINDOW](validation-heal-operations.md#LOCK_WINDOW) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- During heal/move contention, cancel decommission, Clear, and resume, and cover recovery from capacity-lock timeout
- Keep the order of notify-after-persist and in-flight exit; do not cancel a new generation by mistake; for the #2423 retry window, separately note whether the live run on the fleet actually triggered it, and do not look only at the final green result.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="H05"></a>

## H05: heal.after-retired-pool-removal

Topologies: H5. Branches: survivor-missing-part, survivor-missing-metadata, retired-source-offline.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_H5_RETIRED_POOL_REMOVAL`): Requires the retired pool to be removed after a complete migration, the source to stay offline, and survivor-only access; current auto-testing has not provided bound evidence for this case that the source stays offline and that access is survivor-only.

1. Following existing Pool behavior, finish a full migration and removal of the retirement configuration, stop the source service, and prove the entry point is bound only to the survivor; retain evidence that the source is unreachable.
2. Inject missing shards or missing metadata only on selected objects or versions on the survivor; START root, or deep on the correct pool/set.
3. Record the source authority selection. Do not write to a pool that has already been retired, and do not overwrite without authority or fall back incorrectly (#2422).
4. Check that the survivor actually reconstructed the on-disk shards, and that every version and marker and the source root are unchanged; with the source kept down, VERIFY in full.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [POOL](validation-heal-operations.md#POOL) → [PROCESS](validation-heal-operations.md#PROCESS) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [LOGS](validation-heal-operations.md#LOGS)

Source assertions:

- After the source pool is fully decommissioned, removed, and stopped, trigger repair only on the survivor
- Do not write to the removed source or rely on it as a fallback; source authority and version are correct; the #2422 path must not show an overwrite without authority or an incorrect rollback.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="H06"></a>

## H06: heal.multi-pool-target-isolation

Topologies: H5. Branches: selected-pool, selected-set, unselected-control-fault.

1. Place different objects on two or more pool/sets, and confirm ownership from the actual layout; register the healthy control and the control fault separately.
2. START only one pool/set; leave the missing-shard controls on the other pools, and do not GET them first.
3. PHYSICAL first confirms that the selected target is fully reconstructed and that unselected faults remain as they were; the task, capacity, quota, and drive identity all point to the correct target.
4. Restore the control fault and run VERIFY only after the evidence has been collected; an object that is readable on another pool does not prove that the selected disk finished repair.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Corrupt only the selected pool/set, and in another pool keep different objects and a control fault
- The selector, the task, capacity/quota, and the target drive agree; an unselected pool is not affected by an incorrect repair, and presence across pools does not mean that target has been repaired.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="H07"></a>

## H07: heal.ec84-multi-set-restart

Topologies: H3,H4. Branches: ec84-one-set-restart, ec84-one-set-crash, ec84-two-sets-restart, ec84-two-sets-crash.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_EC84_MULTI_SET_TOPOLOGY`): Requires 12 independent disks for H3 and at least 24 independent endpoints for H4, with the restart and crash windows recorded per set; H4 resources are currently unavailable.

1. Bind 12 disks for H3 and at least 24 disks for H4. FORMAT must prove that every set is 8data+4parity; multiple directories on the same physical device do not count as independent fault disks.
2. Prepare DB/DV/DX on every set and prove object ownership, REPLACE the selected target, and gracefully restart or send SIGKILL while that target is only partly rebuilt.
3. Save the actual interruption window, the disk generation, and the repaired and unrepaired objects of each set; do not merge shard counts across sets.
4. Each set completes its own physical check and full-version check; the four-disk pilot does not enter this item's numerator, and the native EC8+4 regression is independent supplementary evidence.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [REPLACE](validation-heal-operations.md#REPLACE) → [WINDOW](validation-heal-operations.md#WINDOW) → [PROCESS](validation-heal-operations.md#PROCESS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Pin EC8+4 for a single set and for at least two sets, and restart or crash the target during partial reconstruction
- Prove fault coverage and reconstruction separately for each set, and do not assemble shards across sets; the real disk count and mapping must be met, and a 4-disk case cannot substitute for EC8+4.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="I01"></a>

## I01: heal.slow-io-and-eio

Topologies: H1,H2. Branches: bounded-latency, short-read, eio.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_FAULT_IO`): Requires a dedicated faultfs or fault device, recoverable slow/short-read/EIO injection, and namespace isolation; simulating on a shared disk would contaminate other runs.

1. Apply IO_FAULT only on a preallocated dedicated fault device or a restricted faultfs, and freeze the latency and error rate, the IO range, the maximum window, and the command that restores the original state.
2. Under the same namespace and UID as the service, first prove that the delay, short read, and EIO really take effect, then start the target missing-shard heal; do not let dd fill the shared disk.
3. In the error window, record retries, CPU, the queue, and the physical shards, and do not let the result be reported healthy early. Record transient errors and permanent errors separately.
4. After the fault is removed, observe completion of responsibility again and VERIFY; missing fault-layer evidence counts as INFRA_ERROR, and a simulated EIO must not be described as real media damage.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DX](validation-heal-operations.md#SEED_DX) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [METRICS](validation-heal-operations.md#METRICS) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- At the exclusive fault layer, inject bounded delay, short reads, and EIO, then observe recovery after they are cleared
- Keep transient and permanent errors separate; retries are bounded and there is no busy loop; after the fault is cleared, repair actually occurs, and health must not be marked early while the fault is in effect.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="I02"></a>

## I02: heal.enospc-inodes-and-quota

Topologies: H1,H2. Branches: bounded-volume-enospc, bounded-inode-exhaustion, bucket-quota.

1. IO_FAULT uses an independent loop/fault volume with a capacity limit, preregisters the byte and inode limits and this case's bucket quota, and does not change the shared XFS global quota.
2. First prove that a real service write returns ENOSPC or a quota error; prepare a missing-shard target and a DM write, then trigger heal/PUT, and keep the ACK and the error of every attempt.
3. During the unwritable period, do not damage healthy sources, falsely report shard reconstruction, or release capacity that is still required; snapshot both metadata and parts.
4. Release this case's usage or raise the approved limit, then continue the repair, and VERIFY the full content and the target shards; keep a safe margin of free space on the shared disk.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DM](validation-heal-operations.md#SEED_DM) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [QUOTA](validation-heal-operations.md#QUOTA) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Limit space and inodes on the dedicated test volume, and parameterize this run's bucket quota
- Fill only the dedicated constrained volume, and do not exhaust the shared real disk; on failure, do not damage good replicas or release capacity that is still needed, and continue the repair correctly after space is freed.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="I03"></a>

## I03: heal.network-partition-and-lock-quorum

Topologies: H2. Branches: minority-partition, read-boundary, write-boundary, lock-boundary, rejoin.

Candidate support matches B06, but the storage class and the lock policy are bound separately to the exact candidate. The lock-client scope, quorum thresholds, and EC:8 configuration of `1c7fc81` have been reviewed. Its RPC control-channel factory has changed, so real bidirectional network probes, admission, partial coverage, and rejoin evidence must be collected again. Source equivalence, or a passing B07, must not be taken to mean this case passed. The candidate scope for B08 stays independent.

The adapter additionally binds exact source `62ff96774fa4139d45f14d70b70519aae8070ac0` and its typed RPC `Unavailable` response mapping to `503/ServiceUnavailable`. Every referenced lock, EC, multipart and delete-recovery source file remains independently checked against that committed source; neighboring or abbreviated revisions are rejected. This source requires its matching immutable package and source-bound inspector before live execution. All five original branches, complete DB/DV/DL fixtures, refused-mutation accounting, physical-before-GET proofs, task responsibility and cleanup requirements remain mandatory; the source admission alone does not establish live acceptance.

1. On H2, first freeze the actual node/slot/object R/W quorum and the lock quorum. NETWORK rules match only this case's service port and carry the run ID and timeout recovery.
2. Construct a minority partition and a boundary partition, and confirm the isolation with probes from both ends; SSH, the jump host, and the other service ports stay connected.
3. Both sides run PUT/GET/heal with a nonce. The minority must not promise an unsafe commit; BACKGROUND must expose missing peer/partial coverage.
4. Remove the rules and check that the rule count returns to zero. Reconcile every version, the content, and the target shards by ACK causality, with no fork, no old-generation overwrite, and no lost responsibility.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [FORMAT](validation-heal-operations.md#FORMAT) → [NETWORK](validation-heal-operations.md#NETWORK) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [METRICS](validation-heal-operations.md#METRICS) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Isolate only traffic among the test service nodes, covering the minority, the read/write/lock quorum boundaries, and recovery
- A minority must not promise a repair or write that lacks safety; incomplete cluster coverage must not be shown as global health; recovery has no fork and no cross-generation overwrite.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="I04"></a>

## I04: heal.queue-pressure-and-admission

Topologies: H1,H2. Branches: full-queue, dropped-admission, merged-duplicates, cross-bucket-fairness.

1. The dedicated service freezes queue capacity, concurrency, the maximum request count, and the time limit; the DX fixture and the multi-bucket fixtures apply a bounded number of Admin/read-repair requests through WORKLOAD.
2. Record the actual return and the task ID of Full, Dropped, and Merged separately; a repeated token or a merge is not scored as an independent repair.
3. Persistent responsibility must not be retired because enqueue failed. Record the journal, queue depth, wait latency, and resource curves, and stop injection as soon as the budget is exceeded.
4. After the load stops, converge within the deadline, and actually reconstruct every repairable target; the case must not pass on CLI exit0 or global idle alone.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [CONFIG](validation-heal-operations.md#CONFIG) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [METRICS](validation-heal-operations.md#METRICS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Requests from multiple sources stress the bounded queue, mixing duplicate tasks, different priorities, and multiple buckets
- Full/Dropped/Merged semantics match the actual tasks; a drop must not drop durable responsibility; within the predeclared load window there is no unbounded buildup and no false health report.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="I05"></a>

## I05: heal.lock-timeout-and-caller-abort

Topologies: H1,H2. Branches: object-lock-timeout, caller-disconnect, stale-read-repair, native-lock-owner-fence.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`NATIVE_LOCK_OWNER_ABORT_BARRIER`): A native subset from the same candidate source already exists for lock owner, cancelled physical unlink, and stale read-repair. The H1/H2 timeout and queued-successor diagnostics have both passed an independent live audit (39767336741/1, 39817606025/1), including the original ACKs, historical generations, real lock events, each peer inventory, new-shard integrity after shutdown, and bounded FD cleanup. Still missing are precise ownership at the actual disconnect boundary, a complete native timeout/stale barrier, the stale-generation controller, and the complete H1/H2 DL/DV object/ACK/physical/lock/FD closed loop. Precise source cfg(test) native evidence is retained on its own; the diagnostic subset and the native subset cannot stand in for a complete canonical PASS.

1. LOCK_WINDOW holds the target object lock with this case's slow IO or a controlled native lock barrier, and first proves that the real contention exceeds the preregistered budget.
2. At the same time, heal, overwrite, or delete. The caller-disconnect instance drops only this TCP connection and does not kill the server or shut down the jump host; record request start, disconnect, and the end of in-flight work on the service.
3. After the lock is released, observe that cancel and timeout do not release ownership early and do not commit a second time, and that a stale read-repair revalidates the version.
4. Check every ACK and the final generation, and check that locks and FDs do not leak. When a black box cannot prove lock ownership, supply independent evidence with a NATIVE window; do not guess that the window was hit.

Operations: [SEED_DV](validation-heal-operations.md#SEED_DV) → [IO_FAULT](validation-heal-operations.md#IO_FAULT) → [LOCK_WINDOW](validation-heal-operations.md#LOCK_WINDOW) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [START](validation-heal-operations.md#START) → [WINDOW](validation-heal-operations.md#WINDOW) → [METRICS](validation-heal-operations.md#METRICS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [NATIVE](validation-heal-operations.md#NATIVE)

Source assertions:

- Real object-lock contention exceeds the budget and the caller disconnects, then release the lock and recover
- Cancel or timeout must not release in-flight ownership early or commit twice; a stale read-repair must re-verify the generation; after recovery there is no leak and no incorrect overwrite.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="I06"></a>

## I06: heal.kms-and-tier-outage

Topologies: H2. Branches: kms-unavailable, kms-denied, tier-unavailable, tier-version-unavailable.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_KMS_TIER_TENANT`): Requires a dedicated KMS/tier tenant, network isolation of the dependency, and safe key/tier version evidence; do not cut the network globally on a shared dependency.

1. On a dedicated KMS/tier tenant, first verify that the DE/DV fixtures decrypt and restore normally, and save the non-sensitive key/tier version identifiers.
2. DEPENDENCY cuts only this test client's or service's dedicated namespace route to that dependency, or revokes this case's tenant permission; do not stop the shared KMS globally.
3. Issue GET, restore, and deep heal, and observe responsibility. A dependency error must not be judged as the object not existing, as remove being allowed, or as already repaired; keys must not enter error logs.
4. After the dependency and the permissions are restored, the original version can be decrypted and restored, and the physical state and responsibility are complete; judge a remote object that does not exist separately from a remote that is temporarily unreachable.

Operations: [SEED_DE](validation-heal-operations.md#SEED_DE) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [DEPENDENCY](validation-heal-operations.md#DEPENDENCY) → [NETWORK](validation-heal-operations.md#NETWORK) → [S3](validation-heal-operations.md#S3) → [START](validation-heal-operations.md#START) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Make only the assigned test KMS/remote tier temporarily unavailable, and keep querying and repairing
- A dependency failure must not be interpreted as the object being absent, deletable, or already repaired, and must not expose the key; after recovery the original version can be decrypted or restored, and responsibility is not lost.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="J01"></a>

## J01: heal.status-partial-disabled-unknown

Topologies: H1,H2. Branches: enabled-idle, disabled, uninitialized, missing-peer, recovered-complete.

1. On an independent service configuration, observe separately enabled with no task, HEAL disabled, and initialization not finished, and keep the actual startup configuration and timeline.
2. On H2, PROCESS stops one peer and queries BACKGROUND/REPLACEMENT across nodes; complete coverage must include the expected/responded/unknown detail.
3. disabled/uninitialized/partial/degraded must not be interpreted by rc or by an aggregator as global completion; collect the original rc output and compare it field by field with raw Admin.
4. A complete idle is allowed only after every peer is restored, and PHYSICAL/manifest is still used to check that no responsibility remains; an unobserved initialization window is recorded on its own as NOT_OBSERVED.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [CONFIG](validation-heal-operations.md#CONFIG) → [PROCESS](validation-heal-operations.md#PROCESS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [REPLACEMENT](validation-heal-operations.md#REPLACEMENT) → [RC_STATUS](validation-heal-operations.md#RC_STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Separately check the status for enabled/disabled/uninitialized, for all peers reachable, and for a missing peer
- idle holds only when coverage is complete and there is truly no activity; partial/unknown/degraded/disabled are not converted by the client into all-complete.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="J02"></a>

## J02: heal.outcome-counters-and-error-truth

Topologies: H1,H2. Branches: mixed-healthy-repaired-failed, dry-run, deferred, readonly-terminal-failure, unknown-negative.

Execution policy: `evidence-blocked`; disabled by default: `false`.

Blocked reason (`EVIDENCE_MIXED_OUTCOME_ORCHESTRATION`): Execution flows already exist for mixed healthy/repaired/failed, an independent dry-run, dependency deferral, a real SDK legacy Unknown, an original all-replica corrupt-metadata Unknown, read-only permanent failure with new-token compensation, and version-level grace Deferred with new-token cleanup, all reconciled per object against the manifest and attemptFailures. Complete serial live evidence and independent acceptance are still required, so default dispatch and canonical admission stay prohibited; a local check cannot serve as a complete PASS.

1. In the same batch of independent keys, create healthy objects, objects with recoverable missing shards, and unrecoverable objects below R; deferred is produced by a frozen lock or dependency fault, and dry-run is a separate task.
2. Save the per-object physical baseline and the expected disposition, and reconcile the result's execution/coverage/counters/object identities item by item.
3. unknown/skipped/completed_with_errors do not count as a successful repair. Explain legacy objectsHealed separately from canonical healed; attemptFailures must not be hidden.
4. Use an independent complete manifest to make up for the bounded outcome window, and keep the correct failure record of each unrepairable object; a repair by another task after recovery must not overwrite the original result.
5. The grace branch first uses SDK PUT to create two real versions and verifies that the shards are complete. After saving one member's original object tree, SDK DELETE the exact historical version, and verify disk by disk that only the current version exists and that its logical metadata, data shards, and control objects are unchanged. Replay the original object tree only onto the original member. The actual candidate process fixes grace=120 seconds and does not change metadata age or the clock. The original task must finish inside the window and report exactly the old version as deferred, the current version as verified_healthy, and retryNotBefore. After naturally waiting out the expiry, the original token is still unchanged, and only a new token may clean up the old version. After cleanup, first check that every physical byte equals the authoritative scene after DELETE, then run cross-node GET/404 and recheck the old token, and only then perform fixture finalization. If the window is missed, retain NOT_OBSERVED. On an exception, preserve the scene and mark DIRTY. Rescue restores only the authoritative tree from after DELETE and does not resurrect the deleted version.
6. Manual diagnostic entry: explicitly select heal.outcome-counters-and-error-truth through Functional Chain with the frozen candidate and candidate-bound manager/inspector tools. The default catalog policy stays evidence-blocked; even all passing diagnostic branches retain complete=false, canonicalCasePassClaimed=false and a blocking gate verdict until independent acceptance.
7. For a pre-replay public-manifest refusal, recover only after the complete SDK request/ACK ledger proves all writes acknowledged and no DELETE or replay occurred. Bind all four original physical members and the reserved owner, stop the exact candidate, then preserve all roots and state using durable same-filesystem non-overwriting rename. Partial archive retries require the same durable inventory; missing roots or drift retain DIRTY. Isolation is not product repair or restoration and never rewrites the failed case.
8. Manual J02 candidate admission is limited to the original 072b57b37559fbe626aeda8abf61617fdcbdd926 and corrected 1c7fc818dd858247c45b39542abd8291b1e6e31b sources. Each execution requires the matching immutable package and independently audited inspector/manager bundle. A corrected-candidate run is new evidence; it cannot replace an original failure. The full exact-version, physical, timing, retry and responsibility oracles remain unchanged, and all passing diagnostics still require independent canonical acceptance.
9. The deferred source branch follows Batch.deferred in frozen validation script 524eb5b32c3236497fdd8a9271df0494fa3d9974: complete the exact-version grace, natural-expiry and new-token cleanup procedure, then finalize this branch. An additional R-1 metadata existence-check diagnostic is not part of that source procedure and does not gate its verdict. Its historical failures and strict replay assertions remain retained; original source failures, missed-window NOT_OBSERVED and uncertain-mutation DIRTY are unchanged.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [LOCK_WINDOW](validation-heal-operations.md#LOCK_WINDOW) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [RECOVER](validation-heal-operations.md#RECOVER) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Mix healthy, repairable, unrepairable, deferred, and dry-run objects in the same batch
- execution/coverage/disposition, the counts, and the per-object physical results agree; unknown, skipped, or completed_with_errors cannot count as a successful repair.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="J03"></a>

## J03: heal.incremental-cursor-and-truncation

Topologies: H1,H2. Branches: within-window-cursor, past-retention-gap, cross-node-cursor, terminal-truncation.

1. DX has at least 257 targets, past the 128 diagnostic window. While starting recursive heal, query with sinceSeq, and save every request, nextSeq, minSeq, and items.
2. Continuous reads from a single end must not repeat or move backward. Deliberately using an overly old cursor must explicitly indicate truncated/gap. Compare cross-node queries by task/epoch rather than by a global number.
3. At the task's terminal state, also read the old cursor, and verify that a bounded list is not treated as a complete manifest; every error and gap enters the report unchanged.
4. Finally accept coverage with a full independent S3/physical manifest and the canonical counters. If the task is too fast, truncation at the terminal state is still allowed, but it must not be claimed as an observation of a running cursor.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [CURSOR](validation-heal-operations.md#CURSOR) → [STATUS](validation-heal-operations.md#STATUS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- Generate tasks past the result-retention limit, poll sinceSeq with pagination, and read across nodes and terminal states
- nextSeq/minSeq are monotonic and gap/truncation is explicit; bounded items are not the complete responsibility manifest, the final result must be corroborated by an independent manifest and counts, and errors are not dropped silently.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="J04"></a>

## J04: heal.repeat-idempotency

Topologies: H1,H2. Branches: normal-repeat, deep-repeat, repeat-after-restart, versioned-repeat.

1. Repair missing DB/DV shards first. When an entire versioned xl.meta is missing, every identity may be repaired, or at least one repaired with the rest in a positive healthy state. A marker may only be repaired/metadata_healthy. counters.healed matches the repaired count, counters.unchanged matches the count of the rest, and unknown/skipped/failed=0. Save the complete post-heal S3/physical result as the second-round baseline, and do not use the original pre-fault xl.meta to require identical internal bytes.
2. Run normal/deep again: normal accepts only metadata_healthy, and only deep requires verified_healthy. Both have healed=0 and unknown/skipped/failed=0, must not add any repaired, and every original version and marker keeps the same identity and count.
3. After PROCESS restarts the original unit, repeat. Check that the new task has an independent token and does not create a new object version, unbounded responsibility, or a duplicate repair count.
4. Changes to internal maintenance records are allowed when format evidence proves them. User content and references stay unchanged, and the target-shard digests stay stable.

Operations: [SEED_DB](validation-heal-operations.md#SEED_DB) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [WAIT](validation-heal-operations.md#WAIT) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY) → [PROCESS](validation-heal-operations.md#PROCESS)

Source assertions:

- After the first repair, repeat normal/deep, restart, and check again
- For the first versioned run, allow all identities repaired, or at least one repaired plus the rest in a positive healthy state; a marker is only repaired/metadata_healthy, and the counts must match the receipt set; a normal repeat accepts `metadata_healthy`, a deep repeat requires `verified_healthy`, a repeat marker is `metadata_healthy` with healed=0; unknown/skipped/failed must not become PASS.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="J05"></a>

## J05: heal.long-soak

Topologies: H2,H5. Branches: 24h, 72h.

Execution policy: `environment-gated`; disabled by default: `true`.

Blocked reason (`ENV_DEDICATED_LONG_SOAK_LANE`): Must actually run for 24 hours and for 72 hours on an exclusive H2/H5 lane, including rate limiting, monitoring, expiry reclamation, and a complete resource budget. A short run cannot substitute for soak evidence.

1. Only on a specially assigned exclusive H2/H5 lane, freeze the full 24h or 72h wall-clock window, the cyclic fault schedule, the concurrency, rate-limit, and storage caps, and the expiry-reclamation plan.
2. WORKLOAD continuously records the ACK of every request. Periodic faults use only this run's DRIVE_GATE/PROCESS/dependency rules; witness and recover every round, and do not add data without a bound.
3. Continuously sample METRICS, logs, and queue/MRF/checkpoint. As soon as resource growth, a panic, or unexplained responsibility reaches a preregistered threshold, retain the failure evidence.
4. When the window expires, stop writes and injection, clear every fault, and after unknown ACKs are resolved run a full VERIFY and PHYSICAL. A short run cannot stand in for a 24h or 72h result.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [SEED_DV](validation-heal-operations.md#SEED_DV) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [DRIVE_GATE](validation-heal-operations.md#DRIVE_GATE) → [PROCESS](validation-heal-operations.md#PROCESS) → [BACKGROUND](validation-heal-operations.md#BACKGROUND) → [CHECKPOINT](validation-heal-operations.md#CHECKPOINT) → [METRICS](validation-heal-operations.md#METRICS) → [LOGS](validation-heal-operations.md#LOGS) → [RECOVER](validation-heal-operations.md#RECOVER) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- In an exclusive environment, run a preregistered 24h/72h window, mixing repair, offline events, writes, and version deletes
- Every ACK is traceable; the final full manifest is correct; queue, memory, FDs, and temporary space do not grow without bound; and there is no background panic or unexplained persisted responsibility.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.

<a id="J06"></a>

## J06: heal.performance-abba

Topologies: H2. Branches: abba-repeat-1, abba-repeat-2, abba-repeat-3.

1. On the same hardware, candidate, and rc, preregister the warmup, each round's duration, concurrency, the read/write ratio, the data volume and cache policy, and the P95/P99 and error-rate thresholds. Do not set the threshold after the results are in.
2. Follow the order A without heal, B with heal, B with heal, and A without heal. Each round starts from the same logical content and the same queue baseline. Complete at least three independent ABBA groups.
3. WORKLOAD records latency, success, error, and timeout for every request. METRICS records CPU, IO, FD, and the bytes actually repaired. Do not remove errors from the total-request denominator.
4. Report each round and the paired difference, repair throughput, and the P95/P99 of GET and of PUT separately. Functional VERIFY must pass, and the performance thresholds must also be met on their own.

Operations: [SEED_DX](validation-heal-operations.md#SEED_DX) → [PERFORMANCE](validation-heal-operations.md#PERFORMANCE) → [WORKLOAD](validation-heal-operations.md#WORKLOAD) → [FAULT](validation-heal-operations.md#FAULT) → [START](validation-heal-operations.md#START) → [METRICS](validation-heal-operations.md#METRICS) → [LOGS](validation-heal-operations.md#LOGS) → [PHYSICAL](validation-heal-operations.md#PHYSICAL) → [VERIFY](validation-heal-operations.md#VERIFY)

Source assertions:

- On the same hardware and the same candidate, run the no-heal baseline and the with-heal load in ABBA order, repeat, and keep the rate limit fixed
- Report repair throughput, GET/PUT P95/P99, error rate, and CPU/IO separately; freeze the thresholds in advance, and do not cherry-pick rounds afterward or treat a functional PASS as performance acceptance.

First save the failure scene and the physical and state evidence, stop this case's writes and in-flight tasks, then undo this case's fault per RECOVER. When a concurrent version has already changed, do not blindly restore the old xl.meta; recovery must not serve as a product fix. Mark an unrestored environment DIRTY, and keep this case's backup and the original failure result.

Manually review after each item satisfies the design's common contract, that branch's steps, and the additional assertions. Missing resources are BLOCKED, a window miss is NOT_OBSERVED, a product error is FAIL, and a tool or recovery error is INFRA_ERROR; a case that did not run is not recorded as PASS.
