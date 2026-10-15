"""J02: preserve exact version grace responsibility through new-token cleanup."""

from datetime import datetime
import hashlib
import math
import time
import uuid

try:
    from . import orphan
    from .authority import object_files
    from .fleet_node import PROCESS_FIELDS
    from .heal_probe import require
    from .heal_workload import Recorder, payload, verify_seed
    from .healthy import BASE_OPTIONS, physical_content
    from .shards import files, map_target, run_task
    from .stop import idle_fleet
    from .versions import validate_version_receipts, verify_proof
except ImportError:
    import orphan
    from authority import object_files
    from fleet_node import PROCESS_FIELDS
    from heal_probe import require
    from heal_workload import Recorder, payload, verify_seed
    from healthy import BASE_OPTIONS, physical_content
    from shards import files, map_target, run_task
    from stop import idle_fleet
    from versions import validate_version_receipts, verify_proof


GRACE_SECONDS = 120
KEY = orphan.KEY
SIZE = 5 * 1024**2 + 123


def finite_epoch(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def grace_outcome(status, bucket, old, current, options, incarnation, observed_at, *, cleanup=False):
    require(finite_epoch(observed_at) and finite_epoch(old.get('lastModifiedEpoch'))
            and old['versionId'] != current['versionId'] and isinstance(incarnation, str) and incarnation,
            'J02 grace lacks distinct ACK versions, original incarnation or a real observation clock')
    outcome = status.get('outcome') or {}
    require(status.get('summary') == 'finished' and status.get('settings') == options and status.get('truncated', False) is False
            and outcome.get('execution', {}).get('state') == 'completed' and outcome.get('coverage') == 'complete'
            and outcome.get('objectsTruncated') is False, 'J02 grace task lacks complete original terminal truth')
    wanted = dict(processed=2, healed=1 if cleanup else 0, unchanged=1, skipped=0 if cleanup else 1,
                  failed=0, unknown=0, attemptFailures=0 if cleanup else 1, overflowed=False)
    counters = outcome.get('counters') or {}
    require(set(counters) == set(wanted) and all(type(counters[name]) is type(value) and counters[name] == value
            for name, value in wanted.items()), 'J02 grace counters do not match both original versions')
    objects = outcome.get('objects')
    require(isinstance(objects, list) and len(objects) == 2, 'J02 grace omitted a version responsibility')
    seen, due = set(), None
    for item in objects:
        identity = item.get('identity') or {}
        version = identity.get('versionId')
        require(version in {old['versionId'], current['versionId']} and version not in seen
                and identity == dict(kind='object', bucket=bucket, object=KEY, versionId=version,
                    bucketIncarnationId=incarnation, poolIndex=0, setIndex=0)
                and type(identity.get('poolIndex')) is int and type(identity.get('setIndex')) is int,
                'J02 grace rebound, duplicated or omitted an original version/incarnation')
        seen.add(version)
        disposition = item.get('disposition') or {}
        if version == current['versionId']:
            require(disposition == {'state': 'verified_healthy'}, 'J02 grace current ACK lost its Deep integrity')
        elif cleanup:
            require(disposition == {'state': 'repaired'}, 'J02 post-grace cleanup is not a distinct product repair')
        else:
            details = disposition.get('details') or {}
            timestamp = details.get('retryNotBefore')
            require(disposition.get('state') == 'deferred' and set(details) == {'reason', 'retryNotBefore'}
                    and details['reason'] == 'dangling_delete_grace' and isinstance(timestamp, dict)
                    and set(timestamp) == {'secs_since_epoch', 'nanos_since_epoch'}
                    and type(timestamp.get('secs_since_epoch')) is int and 0 <= timestamp['secs_since_epoch'] < 2**64
                    and type(timestamp.get('nanos_since_epoch')) is int and 0 <= timestamp['nanos_since_epoch'] < 10**9,
                    'J02 grace lacks the exact typed future retry timestamp')
            due = timestamp['secs_since_epoch'] + timestamp['nanos_since_epoch'] / 10**9
            require(due > observed_at and abs(due - old['lastModifiedEpoch'] - GRACE_SECONDS) <= 5,
                    'J02 grace retry timestamp is past or unrelated to the original server version age')
    require(not cleanup or observed_at >= old['lastModifiedEpoch'] + GRACE_SECONDS,
            'J02 cleanup was attributed before natural grace expiry')
    return {'bothExactVersionResponsibilities': True, 'canonicalCounters': counters, 'retryNotBeforeEpoch': due,
            'newTaskProductCleanup': cleanup, 'sameTokenRecoveryClaimed': False}


def grace_fixture(initial, authority, injected, bucket, old, current, target, directory):
    initial_refs = orphan.references(initial, bucket, KEY)
    authority_refs = orphan.references(authority, bucket, KEY)
    require(set(initial_refs) == set(authority_refs) and target in initial_refs
            and all(set(value) == {old['versionId'], current['versionId']} for value in initial_refs.values())
            and all(set(value) == {current['versionId']} for value in authority_refs.values()),
            'J02 grace DELETE did not establish complete exact-version authority on every original member')
    require(all(values[current['versionId']] == authority_refs[member][current['versionId']]
                and values[current['versionId']].get('deleted') is False
                and values[current['versionId']].get('isLatest') is True
                and values[old['versionId']].get('dataDir') == directory
                and values[current['versionId']].get('dataDir') != directory
                for member, values in initial_refs.items()),
            'J02 grace DELETE changed the acknowledged current logical version or its original dataDir')
    left, right = files(initial), files(authority)
    old_prefix = bucket + '/' + KEY + '/' + directory + '/'
    metadata = bucket + '/' + KEY + '/xl.meta'
    require({n: v['formats'] for n, v in initial.items()} == {n: v['formats'] for n, v in authority.items()}
            and not any(identity[2].startswith(old_prefix) for identity in right),
            'J02 grace DELETE changed formats or retained an unacknowledged old payload')
    require({key: value for key, value in left.items() if key[2] != metadata and not key[2].startswith(old_prefix)}
            == {key: value for key, value in right.items() if key[2] != metadata},
            'J02 grace DELETE changed current/control payload bytes or sidecars')
    if injected is not None:
        observed = files(injected)
        prefix = bucket + '/' + KEY + '/'
        require({n: v['formats'] for n, v in injected.items()} == {n: v['formats'] for n, v in authority.items()}
                and {key: value for key, value in observed.items() if not (key[:2] == target and key[2].startswith(prefix))}
                == {key: value for key, value in right.items() if not (key[:2] == target and key[2].startswith(prefix))}
                and {key: value for key, value in observed.items() if key[:2] == target and key[2].startswith(prefix)}
                == {key: value for key, value in left.items() if key[:2] == target and key[2].startswith(prefix)},
                'J02 grace replay is not exactly one original member object tree')
    return {'allOriginalMembers': len(initial_refs), 'exactHistoricalDeleteAcknowledged': True,
            'originalCurrentAndControlBytesPreserved': True, 'singleOriginalMemberReplay': injected is not None}


def clocks(fleet, binding, label, previous=None):
    samples = {}
    for node in fleet.nodes:
        start = time.time()
        sample = fleet.record(label, {'node': node, **fleet.remote(node, 'process-clock')})
        finish = time.time()
        identity = sample.get('identity') or {}
        require(sample.get('readOnly') is True and sample.get('clockChanged') is False
                and finite_epoch(sample.get('nodeEpoch')) and start - 3 <= sample['nodeEpoch'] <= finish + 3
                and type(identity.get('pid')) is int and identity['pid'] > 1
                and identity.get('binarySha256') == binding['candidateBinarySha256']
                and isinstance(identity.get('settings'), dict)
                and identity.get('settings', {}).get('RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS') == str(GRACE_SECONDS),
                'J02 grace lacks the actual candidate process policy or synchronized node clock')
        if previous is not None:
            require(all(identity.get(key) == previous[node]['identity'].get(key) for key in PROCESS_FIELDS),
                    'J02 original grace process changed between original and compensating tasks')
        samples[node] = sample
    return samples


def archive_member(fleet, snapshot, bucket, target, label):
    node, disk = target
    params = dict(disk=disk, bucket=bucket, key=KEY)
    expected = object_files(snapshot, bucket, KEY)[target]
    start = time.time()
    observed = fleet.record('J02-grace-stopped-member', {'node': node, **fleet.remote(node, 'orphan-observe', **params)})
    require(observed.get('present') is True and observed.get('files') == expected
            and finite_epoch(observed.get('nodeEpoch')) and start - 3 <= observed['nodeEpoch'] <= time.time() + 3,
            'J02 stopped grace object/clock differs from the original physical witness')
    return fleet.record('J02-grace-archive-' + label, {'node': node, **fleet.remote(node, 'orphan-capture',
        **params, generation=label, expectedFiles=expected, expectedPresent=True)})


def run(probe, fleet, binding, directory, expected, incarnation, active, preserve_token):
    """Use real SDK version history and natural wall-clock grace, never aged bytes."""
    bucket = binding['bucketPrefix']
    recorder = Recorder(probe, directory / 'original-grace-fixture')
    fleet.start()
    recorder.call('put_bucket_versioning', {'Bucket': bucket, 'VersioningConfiguration': {'Status': 'Enabled'}})
    versions = []
    for label in ('old', 'current'):
        body = payload(bucket + '/J02-grace/' + label, SIZE)
        response = recorder.call('put_object', {'Bucket': bucket, 'Key': KEY, 'ContentType': 'application/octet-stream'}, body)
        version = response.get('VersionId')
        require(isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int != 0
                and version not in {value['versionId'] for value in versions}, 'J02 grace PUT lacks a distinct real ACK version')
        head = fleet.record('J02-grace-ACK-server-age', probe.s3('head_object', {'Bucket': bucket, 'Key': KEY, 'VersionId': version}))
        info = head.get('response') or {}
        require(head.get('acknowledged') is True and head.get('resultUnknown') is False and info.get('VersionId') == version
                and info.get('ContentLength') == len(body) and isinstance(response.get('ETag'), str) and response['ETag']
                and info.get('ETag') == response['ETag'],
                'J02 grace server version age does not bind the actual PUT ACK')
        modified = info.get('LastModified')
        epoch = modified.timestamp() if isinstance(modified, datetime) else datetime.fromisoformat(modified).timestamp()
        require(finite_epoch(epoch) and abs(epoch - time.time()) <= 5, 'J02 grace server version clock is not current')
        if versions: versions[0]['isLatest'] = False
        versions.append(dict(key=KEY, versionId=version, isLatest=True, deleteMarker=False,
            payload={'sha256': hashlib.sha256(body).hexdigest(), 'size': len(body)}, lastModifiedEpoch=epoch))
    old, current = versions
    # Server age belongs to the grace witness, not the public S3 manifest.
    public_versions = [{key: value for key, value in version.items() if key != 'lastModifiedEpoch'} for version in versions]
    seeded = [*expected, *public_versions]
    fleet.record('J02-grace-initial-ACK-S3', verify_seed(seeded, probe.inventory(bucket)))
    healthy = run_task(probe, fleet, {'bucket': bucket, 'key': KEY}, 2, active,
        option_overrides={'recursive': True}, status_endpoints=tuple(binding['endpoints'])[1:])
    require(validate_version_receipts(healthy, bucket, versions, 2)['bucketIncarnationId'] == incarnation,
            'J02 grace initial versions escaped the independently anchored bucket incarnation')
    idle_fleet(fleet, binding); fleet.stop()
    initial = fleet.snapshot('J02-grace-original-two-version-physical', quiescent=True)
    old_target = map_target(initial, bucket, KEY, 'data', 1, old['versionId'], latest_required=False)
    current_target = map_target(initial, bucket, KEY, 'data', 1, current['versionId'])
    verify_proof(fleet, old_target, 'J02-grace-original-old-full-bitrot')
    verify_proof(fleet, current_target, 'J02-grace-original-current-full-bitrot')
    target = (fleet.nodes[-1], 0)
    archive_member(fleet, initial, bucket, target, 'old')
    fleet.start()
    deleted = recorder.call('delete_object', {'Bucket': bucket, 'Key': KEY, 'VersionId': old['versionId']})
    require(deleted.get('VersionId') == old['versionId'] and deleted.get('DeleteMarker') in {None, False}
            and deleted.get('ResponseMetadata', {}).get('HTTPStatusCode') == 204, 'J02 grace DELETE lacks its exact historical ACK')
    expected = [*expected, public_versions[1]]
    baseline = probe.inventory(bucket)
    fleet.record('J02-grace-post-delete-ACK-S3', {'inventory': baseline, **verify_seed(expected, baseline)})
    idle_fleet(fleet, binding); fleet.stop()
    authority = fleet.snapshot('J02-grace-post-delete-all-member-authority', quiescent=True)
    grace_fixture(initial, authority, None, bucket, old, current, target, old_target['dataDir'])
    archive_member(fleet, authority, bucket, target, 'authority')
    params = dict(disk=target[1], bucket=bucket, key=KEY, generation='authority')
    fleet.orphan_faults.append((target[0], params))
    fleet.record('J02-grace-arm-post-delete-authority', {'node': target[0], **fleet.remote(target[0], 'orphan-arm', **params)})
    fleet.record('J02-grace-single-member-old-replay', {'node': target[0], **fleet.remote(target[0], 'orphan-install',
        disk=target[1], bucket=bucket, key=KEY, dataDir=old_target['dataDir'], mode='object')})
    faulted = fleet.snapshot('J02-grace-original-replayed-fault', quiescent=True)
    physical = grace_fixture(initial, authority, faulted, bucket, old, current, target, old_target['dataDir'])
    fleet.start()
    before_clocks = clocks(fleet, binding, 'J02-grace-actual-process-clock-before-original')
    options = {**BASE_OPTIONS, 'recursive': True, 'remove': True}
    scope = dict(bucket=bucket, key=KEY)
    result = {'originalGraceSourceProcedure': True, 'physicalFixture': physical, 'globalClockChanged': False,
              'metadataAgeRewritten': False, 'sameTokenRecoveryClaimed': False, 'canonicalPassClaimed': False}
    trigger_at = time.time()
    expires = old['lastModifiedEpoch'] + GRACE_SECONDS
    fleet.record('J02-grace-original-admission-window', {'observedAt': trigger_at, 'graceSeconds': GRACE_SECONDS,
        'oldServerModifiedEpoch': old['lastModifiedEpoch'], 'releaseAt': expires + 5})
    require(physical_content(fleet.snapshot('J02-grace-immediately-before-original-task')) == physical_content(faulted),
            'J02 grace fault changed before original admission')
    if expires - trigger_at < 10:
        result.update(verdict='NOT_OBSERVED', reason='Original pre-grace admission window elapsed')
    else:
        admissions = []
        status = run_task(probe, fleet, scope, 2, active, option_overrides=options,
            status_endpoints=tuple(binding['endpoints'])[1:], admissions=admissions)
        observed_at = time.time()
        fleet.record('J02-grace-original-terminal-before-compensation', {'observedAt': observed_at, 'status': status})
        require(len(admissions) == 1, 'J02 grace original task has ambiguous admission')
        idle_fleet(fleet, binding)
        protected = fleet.snapshot('J02-grace-original-task-physical-before-any-GET')
        if observed_at >= expires:
            unchanged = physical_content(protected) == physical_content(faulted)
            cleaned = physical_content(protected) == physical_content(authority)
            require(unchanged or cleaned, 'J02 late grace observation lost current/control bytes')
            grace_outcome(status, bucket, old, current, options, incarnation,
                trigger_at if unchanged else observed_at, cleanup=not unchanged)
            result.update(verdict='NOT_OBSERVED', reason='Original terminal observation missed the pre-grace window')
        else:
            result['original'] = grace_outcome(status, bucket, old, current, options, incarnation, observed_at)
            require(physical_content(protected) == physical_content(faulted), 'J02 pre-grace task modified protected old/current bytes')
            deadline = time.monotonic() + GRACE_SECONDS + 10
            while time.time() < expires + 5:
                require(time.monotonic() < deadline, 'J02 natural grace clock did not converge within its bounded wait')
                time.sleep(min(1, max(0, expires + 5 - time.time())))
            clocks(fleet, binding, 'J02-grace-actual-process-clock-after-expiry', before_clocks)
            preserve_token(probe, fleet, admissions[0], status, 'J02-grace-original-token-after-natural-expiry')
            require(physical_content(fleet.snapshot('J02-grace-original-fault-before-new-token')) == physical_content(protected),
                    'J02 completed original token resumed or altered bytes after grace')
            next_admissions = []
            recovered = run_task(probe, fleet, scope, 2, active, option_overrides=options,
                status_endpoints=tuple(binding['endpoints'])[1:], admissions=next_admissions)
            require(len(next_admissions) == 1 and next_admissions[0]['clientToken'] != admissions[0]['clientToken'],
                    'J02 grace compensation reused the original completed token')
            fleet.record('J02-grace-new-token-product-terminal', recovered)
            result['compensation'] = grace_outcome(recovered, bucket, old, current, options, incarnation, time.time(), cleanup=True)
            idle_fleet(fleet, binding)
            clocks(fleet, binding, 'J02-grace-actual-process-clock-after-compensation', before_clocks)
            product = fleet.snapshot('J02-grace-product-cleaned-before-any-GET-or-rescue')
            require(physical_content(product) == physical_content(authority), 'J02 new token did not restore exact post-DELETE physical authority')
            orphan.verify_public(fleet, binding, bucket, expected, [old['versionId']], 'J02-grace-product-S3-before-rescue')
            preserve_token(probe, fleet, admissions[0], status, 'J02-grace-original-token-after-new-task-and-GET')
            clocks(fleet, binding, 'J02-grace-actual-process-clock-after-public-and-token-checks', before_clocks)
            result.update(verdict='PASS', originalToken=admissions[0]['clientToken'], compensatingToken=next_admissions[0]['clientToken'],
                originalTokenUnchanged=True, currentPayloadMatchesIndependentlyVerifiedOriginal=True,
                productCleanupBeforeFixtureRescue=True, oldVersionAbsentAfterProductCleanup=True)
    idle_fleet(fleet, binding); fleet.stop()
    final = fleet.snapshot('J02-grace-complete-result-physical-before-fixture-rescue', quiescent=True)
    if result['verdict'] == 'PASS':
        require(physical_content(final) == physical_content(authority), 'J02 business reads changed the product cleanup scene')
    orphan.recover_all(fleet)
    require(physical_content(fleet.snapshot('J02-grace-post-delete-authority-after-fixture-finalization', quiescent=True)) == physical_content(authority),
            'J02 grace finalization failed to preserve exact acknowledged DELETE authority')
    result.update(fixtureRescueIsProductRepair=False, sourceDiagnosticComplete=True)
    return expected, baseline, result
