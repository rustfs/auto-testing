# Version deletion validation matrix

This matrix records the executable coverage for lifecycle retention, ordinary S3 version deletion, and replicated version purge.

## Lifecycle retention

`ilm.newer-noncurrent-retention` creates ten versions and configures `NewerNoncurrentVersions=3`. The expected state is one current version plus the three newest noncurrent versions. Six older noncurrent versions are deleted. The case checks exact version IDs, payloads, absence of delete markers, and the latest payload.

`ilm.noncurrent-expiration` checks age-based deletion of one noncurrent version while preserving the current version and payload.

`ilm.versioned-current-expiration` checks current-version expiration through a delete marker while retaining the original version.

`ilm.delete-marker-expiration` and `ilm.delete-all-versions` cover marker cleanup and full history cleanup.

## Ordinary version deletion

`rustfs-s3-compat-test.sh` covers explicit `versionId` deletion, deleting a delete-marker version, repeated deletes while a marker is current, suspended null-version behavior, and Object Lock deletion gates.

`bucket-replication.version-delete` exercises the same explicit version deletion through the source and replicated target. It verifies that only the addressed version disappears, no delete marker is created, and the retained payload is unchanged on both sites.

## Replicated purge

`bucket-replication.version-delete` verifies eventual target convergence after a version purge. The source and target inventories are recorded before and after the operation.

The RustFS replication and lifecycle suites separately cover pending purge visibility, per-target completion, target-already-absent handling, retry state, and source cleanup. A pending purge is hidden from public version listings while its internal metadata and data directory remain available to the replication worker.

## Evidence requirements

Version-list absence is a logical visibility assertion. Physical cleanup requires source metadata and data-directory evidence from an ECStore or native validation fixture. Tests must record the exact version IDs, delete-marker IDs, retained payload hashes, replication target statuses, and cleanup outcome.
