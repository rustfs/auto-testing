#!/usr/bin/env python3
"""C10: actual grace, exact shared references and pending-MPU preservation."""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
import uuid

try:
    from .authority import object_files
    from .heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from .heal_workload import Recorder, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .native_regressions import CANDIDATE
    from .shared_fixture import verify as verify_shared
    from .shards import files, map_target, run_task
    from .scopes import verify_parts
    from .stop import idle_fleet
except ImportError:
    from authority import object_files
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from native_regressions import CANDIDATE
    from shared_fixture import verify as verify_shared
    from shards import files, map_target, run_task
    from scopes import verify_parts
    from stop import idle_fleet


CASE_ID = 'heal.dangling-and-orphan-safety'
BRANCHES = ('remove-false', 'remove-true-before-grace', 'remove-true-after-grace', 'shared-version-reference', 'pending-mpu')
SIZE = 2 * 1024**2 + 123
KEY = 'orphan.bin'


def members(snapshot):
    return {(node, row['disk']) for node, value in snapshot.items() for row in value['formats']}


def references(snapshot, bucket, key):
    indexed = files(snapshot)
    result = {}
    for node, disk in members(snapshot):
        metadata = indexed.get((node, disk, f'{bucket}/{key}/xl.meta'), {}).get('metadata', {})
        require(not metadata or metadata.get('bucket') == bucket and metadata.get('key') == key,
                'C10 physical metadata changed bucket/key identity')
        values = metadata.get('versions', [])
        require(len({value['versionId'] for value in values}) == len(values), 'C10 duplicate physical version identity')
        result[(node, disk)] = {value['versionId']: value for value in values}
    return result


def require_shared(snapshot, bucket, versions):
    require(len(versions) == 2 and len(set(versions)) == 2, 'C10 shared fixture needs two independent ACK versions')
    for values in references(snapshot, bucket, KEY).values():
        live = [values.get(version) for version in versions]
        require(all(value and value.get('deleted') is False and value.get('inline') is None for value in live)
                and live[0].get('dataDir') and live[0]['dataDir'] == live[1].get('dataDir'),
                'C10 lost a shared live version reference')


def physical_safety(before, after, authority, bucket, expected, branch, doomed, directory, target):
    require(members(before) == members(after) == members(authority), 'C10 lost an original physical member')
    require({node: value['formats'] for node, value in before.items()} ==
            {node: value['formats'] for node, value in after.items()}, 'C10 changed original format/UUID/set identity')
    left, right = files(before), files(after)
    prefix = bucket + '/' + KEY + '/'
    controls = lambda values: {identity: value for identity, value in values.items() if not identity[2].startswith(prefix)}
    require(controls(left) == controls(right), 'C10 changed control bytes or decoded metadata')
    original_refs, final_refs = references(authority, bucket, KEY), references(after, bucket, KEY)
    selected = [entry for entry in expected if entry['key'] == KEY]
    wanted = {entry['versionId']: entry for entry in selected}
    for member, values in final_refs.items():
        allowed = set(wanted)
        if branch == 'remove-true-before-grace' and member == target: allowed.add(doomed)
        require(set(values) == allowed, 'C10 lost, invented or resurrected a version/marker reference')
        for version, entry in wanted.items():
            old, new = original_refs[member].get(version), values.get(version)
            require(old and new and new.get('deleted') is entry['deleteMarker']
                    and new.get('dataDir') == old.get('dataDir'), 'C10 live version/marker or dataDir changed')
            if entry['deleteMarker']: continue
            data_prefix = prefix + old['dataDir'] + '/'
            original = {identity: value for identity, value in left.items() if identity[:2] == member and identity[2].startswith(data_prefix)}
            actual = {identity: value for identity, value in right.items() if identity[:2] == member and identity[2].startswith(data_prefix)}
            require(original and actual == original, 'C10 changed referenced payloads or integrity sidecars')
    if branch == 'pending-mpu':
        require(physical_content(before) == physical_content(after), 'C10 Heal changed an unfaulted pending-MPU control object')
    elif branch == 'remove-false':
        require(physical_content(before) == physical_content(after), 'C10 remove=false changed protected orphan evidence')
    elif branch == 'remove-true-before-grace':
        old = {identity: value for identity, value in left.items() if identity[:2] == target and identity[2].startswith(prefix + directory + '/')}
        new = {identity: value for identity, value in right.items() if identity[:2] == target and identity[2].startswith(prefix + directory + '/')}
        require(old and old == new and final_refs[target][doomed].get('dataDir') == directory,
                'C10 pre-grace deleted protected stale metadata or shards')
    else:
        require(not any(identity[2].startswith(prefix + directory + '/') for identity in right),
                'C10 expired or unreferenced replay was not actually removed')
    if branch == 'shared-version-reference':
        require_shared(after, bucket, [entry['versionId'] for entry in selected if not entry['deleteMarker']])
    return {'allOriginalMembersChecked': len(members(after)), 'liveReferencesAndPartsPreserved': True,
            'healthyControlsAndFormatsPreserved': True, 'backupRescueIsProductRepair': False}


def terminal_safety(status, branch, bucket, options):
    require(status.get('summary') == 'finished' and status.get('truncated', False) is False
            and status.get('settings') == options, 'C10 terminal receipt is absent/truncated or changed its request')
    outcome = status.get('outcome') or {}
    counters = outcome.get('counters') or {}
    require(outcome.get('coverage') == 'complete' and outcome.get('objectsTruncated', False) is False
            and all(type(counters.get(name)) is int and counters[name] >= 0 for name in ('processed', 'healed', 'failed', 'unknown'))
            and counters['unknown'] == 0, 'C10 responsibility is missing, partial or unknown')
    if branch == 'remove-false':
        require(outcome.get('execution', {}).get('state') == 'completed_with_errors'
                and counters['healed'] == 0 and counters['failed'] == 1
                and 'Object not found: ' + bucket + '/' + KEY in (status.get('detail') or ''),
                'C10 metadata-less protected orphan lacks its exact not-found refusal')
    elif branch == 'remove-true-before-grace':
        deferred = [value for value in outcome.get('objects', [])
                    if value.get('identity', {}).get('bucket') == bucket and value.get('identity', {}).get('object') == KEY
                    and value.get('disposition', {}).get('state') == 'deferred'
                    and value['disposition'].get('details', {}).get('reason') == 'dangling_delete_grace']
        require(deferred and counters['failed'] == 0 and outcome.get('execution', {}).get('state') == 'completed',
                'C10 pre-grace preservation lacks an exact typed deferred responsibility')
    else:
        require(counters['failed'] == 0 and outcome.get('execution', {}).get('state') == 'completed',
                'C10 physical safety has no successful terminal responsibility')
    return {'canonicalCounters': counters, 'fullOriginalResponsibility': True}


def capture(fleet, snapshot, bucket, label):
    result = {}
    for (node, disk), expected in sorted(object_files(snapshot, bucket, KEY).items()):
        params = {'disk': disk, 'bucket': bucket, 'key': KEY}
        observed = fleet.record('C10-stopped-object-presence', {'node': node, **fleet.remote(node, 'orphan-observe', **params)})
        require(observed['files'] == expected and abs(observed['nodeEpoch'] - time.time()) <= 3,
                'C10 stopped archive or real host clock differs from the physical witness')
        result[(node, disk)] = fleet.record('C10-archive-' + label, {'node': node, **fleet.remote(node, 'orphan-capture',
            **params, generation=label, expectedFiles=expected, expectedPresent=observed['present'])})
    return result


def arm_all(fleet, snapshot, bucket, label):
    capture(fleet, snapshot, bucket, label)
    for node, disk in sorted(members(snapshot)):
        params = {'disk': disk, 'bucket': bucket, 'key': KEY, 'generation': label}
        # Register before SSH: a lost arm receipt leaves the whole namespace DIRTY.
        fleet.orphan_faults.append((node, params))
        fleet.record('C10-arm-original-member', {'node': node, **fleet.remote(node, 'orphan-arm', **params)})


def recover_all(fleet):
    for node, params in tuple(fleet.orphan_faults):
        receipt = fleet.record('C10-recover-original-member', {'node': node, **fleet.remote(node, 'orphan-recover', **params)})
        require(receipt.get('state') == 'CLEAR' and receipt.get('lastACKAuthorityRestored') is True,
                'C10 original member is not completely recovered')
        fleet.orphan_faults.remove((node, params))


def pending_snapshot(fleet):
    return {(node, disk): fleet.record('C10-read-only-MPU-census', {'node': node, 'disk': disk,
        **fleet.remote(node, 'orphan-multipart', disk=disk)})['files'] for node in fleet.nodes for disk in range(4)}


def verify_live_parts(fleet, mapping, label):
    require(mapping, 'C10 streaming manifest is empty')
    total, identities = 0, set()
    for identity, target in mapping.items():
        require(identity == (target['bucket'], target['key'], target['versionId']),
                'C10 streaming manifest lost its exact version-addressed authority')
        # Two independently ACKed shared versions deliberately reference the
        # same path. Each version must validate its own metadata commitment;
        # the general selector verifier still rejects aliases within a cohort.
        proof = verify_parts(fleet, {identity: target}, label)
        total += proof['exactStreamingParts']
        identities.update((member['node'], member['disk'], member['relativePath']) for member in target['cohort'])
    return {'versionAddressedStreamingParts': total, 'distinctPhysicalParts': len(identities),
            'exactACKVersions': len(mapping), 'sharedPathsCountedAsIndependentDisks': False}


def verify_public(fleet, binding, bucket, expected, deleted, label):
    for endpoint in binding['endpoints']:
        peer = Probe(binding, endpoint)
        inventory = fleet.record(label + '-full-inventory', peer.inventory(bucket))
        fleet.record(label + '-ACK-comparison', verify_seed(expected, inventory))
        for version in deleted:
            observed = fleet.record(label + '-deleted-version', peer.s3('get_object', {'Bucket': bucket, 'Key': KEY, 'VersionId': version}))
            response = observed.get('response') or {}
            require(observed['acknowledged'] is False and observed['resultUnknown'] is False and 'payload' not in observed
                    and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404
                    and response.get('Error', {}).get('Code') in {'NoSuchVersion', 'NoSuchKey'}, 'C10 deleted version resurrected')
        current = next((entry for entry in expected if entry['key'] == KEY and entry['isLatest']), None)
        if current is None or current['deleteMarker']:
            observed = fleet.record(label + '-current-absence', peer.s3('get_object', {'Bucket': bucket, 'Key': KEY}))
            response = observed.get('response') or {}
            headers = response.get('ResponseMetadata', {}).get('HTTPHeaders', {})
            require(not observed['acknowledged'] and not observed['resultUnknown'] and 'payload' not in observed
                    and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404
                    and response.get('Error', {}).get('Code') == 'NoSuchKey', 'C10 absent current/marker resurrected')
            if current:
                require(headers.get('x-amz-delete-marker') == 'true'
                        and headers.get('x-amz-version-id') in {None, current['versionId']}, 'C10 current marker header/identity changed')


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True)
    candidate = json.loads(args.candidate.read_text())
    require(candidate['source_sha'] == CANDIDATE, 'C10 requires its reviewed candidate codec/grace policy')
    result = {'topology': topology, 'branch': branch, 'verdict': 'RUNNING', 'cleanup': 'PENDING'}
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    runtime = 'orphan-before' if branch == BRANCHES[1] else 'orphan-after' if branch == BRANCHES[2] else 'orphan-preserve'
    grace = 300 if runtime == 'orphan-before' else 30 if runtime == 'orphan-after' else 0
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile=runtime)
    bucket = 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {'schemaVersion': 1, 'runId': run_id, 'jumpHostOnly': True, 'exclusiveHealService': True,
        'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), 'bucketPrefix': bucket,
        'endpoints': {f'node{index}': f'http://rustfs-node{index}:9000' for index in range(1, len(nodes) + 1)},
        'defaultEndpoint': 'node1', 'candidateCommit': candidate['source_sha'], 'harnessCommit': args.testing_commit,
        'candidatePackageSha256': candidate['package_sha256'], 'candidateBinarySha256': args.binary_sha,
        'rcBinarySha256': hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), 'maxInventoryObjects': 128,
        'maxInventoryBytes': 512 * 1024**2}
    fleet.record('binding', binding)
    fleet.record('inspector-build', verify_inspector(args.tool_directory, candidate))
    shared = fleet.record('shared-encoder-build', verify_shared(args.shared_directory, candidate))
    probe, active, pending_mutation = Probe(binding), [], None
    expected, deleted, recovery_snapshot = [], [], None
    try:
        def install_tool():
            if branch != 'shared-version-reference': return
            raw = (args.shared_directory / 'heal-shared-fixture').read_bytes()
            for node in nodes:
                path = '/var/lib/rustfs-auto-testing/heal/' + run_id + '/heal-shared-fixture'
                receipt = subprocess.run([*fleet.ssh(node), 'sudo -n dd of=' + path + ' conv=excl,fsync oflag=nofollow bs=1M status=none'],
                    input=raw, capture_output=True, timeout=120)
                require(receipt.returncode == 0, 'C10 exclusive private encoder upload failed')
                fleet.record('C10-install-owned-encoder', {'node': node, **fleet.remote(node, 'orphan-install-tool', sha256=shared['binarySha256'])})
        fleet.deploy(args.binary_sha, bucket, access, secret, before_start=install_tool)
        recorder = Recorder(probe, directory / 'fixture')
        def call(operation, params, body=None, check=None):
            nonlocal pending_mutation
            pending_mutation = {'operation': operation, 'params': params}
            response = recorder.call(operation, params, body)
            if check is not None: check(response)
            pending_mutation = None
            return response
        def unique_version(response):
            version = response.get('VersionId')
            require(isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int != 0
                    and version not in {entry['versionId'] for entry in expected} | set(deleted),
                    'C10 ACK lost its distinct exact non-null version identity')
        def put(key, label, raw=None):
            body = payload('C10/' + label, SIZE) if raw is None else raw
            response = call('put_object', {'Bucket': bucket, 'Key': key, 'ContentType': 'application/octet-stream',
                'Metadata': {'heal-fixture': 'C10', 'ack': label}}, body, unique_version)
            for entry in expected:
                if entry['key'] == key: entry['isLatest'] = False
            entry = {'key': key, 'versionId': response['VersionId'], 'isLatest': True, 'deleteMarker': False,
                     'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}}
            expected.append(entry)
            head = fleet.record('C10-server-generation-age', probe.s3('head_object', {'Bucket': bucket, 'Key': key, 'VersionId': entry['versionId']}))
            require(head['acknowledged'], 'C10 generation age has no successful server HEAD')
            info = head['response']
            require(info.get('ContentLength') == len(body) and info.get('ETag') == response.get('ETag')
                    and info.get('ContentType') == 'application/octet-stream'
                    and info.get('Metadata') == {'heal-fixture': 'C10', 'ack': label}, 'C10 HEAD differs from original PUT ACK/properties')
            entry.update(properties={name: info[name] for name in (
                'ETag', 'ContentLength', 'ContentType', 'ContentEncoding', 'ContentDisposition', 'CacheControl', 'Metadata',
                'StorageClass', 'ServerSideEncryption', 'SSEKMSKeyId', 'SSECustomerAlgorithm', 'SSECustomerKeyMD5',
                'ObjectLockMode', 'ObjectLockRetainUntilDate', 'ObjectLockLegalHoldStatus') if name in info}, tags=[])
            value = head['response']['LastModified']
            epoch = value.timestamp() if isinstance(value, datetime) else datetime.fromisoformat(value).timestamp()
            require(abs(epoch - time.time()) <= 5, 'C10 fixture server clock/age precondition failed')
            return entry, epoch
        call('create_bucket', {'Bucket': bucket})
        call('put_bucket_versioning', {'Bucket': bucket, 'VersioningConfiguration': {'Status': 'Enabled'}})
        put('control.bin', 'control')
        # A separate acknowledged marker control exercises DM preservation in every branch.
        marker_data, _ = put('marker-control.bin', 'marker-control')
        def marker_ack(response):
            unique_version(response)
            require(response.get('DeleteMarker') is True and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 204,
                    'C10 marker DELETE lacks its exact successful ACK')
        marker = call('delete_object', {'Bucket': bucket, 'Key': 'marker-control.bin'}, check=marker_ack)
        marker_data['isLatest'] = False
        expected.append({'key': 'marker-control.bin', 'versionId': marker['VersionId'], 'deleteMarker': True, 'isLatest': True})
        raw = payload('C10/shared-first', SIZE)
        first, first_time = put(KEY, 'first', raw)
        if branch == 'shared-version-reference':
            second, _ = put(KEY, 'second', raw)
            baseline = fleet.record('C10-independent-original-ACK-S3', probe.inventory(bucket))
            verify_seed(expected, baseline)
            fleet.stop()
            seed = fleet.snapshot('C10-stopped-shared-seed', quiescent=True)
            validate_topology(seed, topology)
            source_map = map_target(seed, bucket, KEY, 'data', 1, first['versionId'], latest_required=False)
            dest_map = map_target(seed, bucket, KEY, 'data', 1, second['versionId'])
            recovery_snapshot = seed
            arm_all(fleet, seed, bucket, 'seed')
            for left, right in zip(source_map['cohort'], dest_map['cohort'], strict=True):
                require((left['node'], left['disk']) == (right['node'], right['disk']), 'C10 shared member ordering changed')
                fleet.record('C10-exact-codec-shared-conversion', {'node': left['node'], **fleet.remote(left['node'], 'orphan-convert',
                    disk=left['disk'], bucket=bucket, key=KEY, sourceVersion=first['versionId'], destinationVersion=second['versionId'],
                    sourceDataDir=left['dataDir'], destinationDataDir=right['dataDir'])})
            converted = fleet.snapshot('C10-shared-all-original-physical', quiescent=True)
            require_shared(converted, bucket, [first['versionId'], second['versionId']])
            fleet.start()
            for endpoint in binding['endpoints']:
                compare_inventories(baseline, fleet.record('C10-shared-all-peer-S3', Probe(binding, endpoint).inventory(bucket)))
            fleet.stop()
            confirmed = fleet.snapshot('C10-shared-before-later-ACK', quiescent=True)
            require(physical_content(converted) == physical_content(confirmed), 'C10 shared representation drifted after GET')
            for node, params in tuple(fleet.orphan_faults):
                receipt = fleet.record('C10-retire-seed-rescue-before-later-ACK', {'node': node, **fleet.remote(node, 'orphan-disarm',
                    **params, expectedFiles=object_files(confirmed, bucket, KEY)[(node, params['disk'])])})
                require(receipt.get('state') == 'RETIRED' and receipt.get('laterACKMayProceed') is True, 'C10 seed rescue was not retired')
                fleet.orphan_faults.remove((node, params))
            recovery_snapshot = None
            fleet.start()
            doomed, doomed_time = put(KEY, 'doomed-third')
        else:
            doomed, doomed_time = first, first_time
            if branch.startswith('remove-true-'): put(KEY, 'second-retained')
        fleet.record('C10-full-baseline-S3', verify_seed(expected, probe.inventory(bucket)))
        fleet.stop()
        original = fleet.snapshot('C10-all-original-generations', quiescent=True)
        validate_topology(original, topology)
        doomed_map = map_target(original, bucket, KEY, 'data', 1, doomed['versionId'], latest_required=not branch.startswith('remove-true-'))
        target = doomed_map['node'], doomed_map['disk']
        data_dir = doomed_map['dataDir']
        if branch == 'pending-mpu':
            fleet.start()
            upload = call('create_multipart_upload', {'Bucket': bucket, 'Key': KEY}, check=lambda response:
                require(isinstance(response.get('UploadId'), str) and response['UploadId'], 'C10 MPU create ACK omitted UploadId'))['UploadId']
            pending_body = payload('C10/pending-part', SIZE)
            part = call('upload_part', {'Bucket': bucket, 'Key': KEY, 'UploadId': upload, 'PartNumber': 1}, pending_body,
                lambda response: require(isinstance(response.get('ETag'), str) and response['ETag'], 'C10 part ACK omitted ETag'))
            fleet.stop()
            before = authority = fleet.snapshot('C10-pending-MPU-object-before', quiescent=True)
            pending_before = pending_snapshot(fleet)
            require(len(pending_before) == (4 if topology == 'H1' else 16) and all(pending_before.values()),
                    'C10 did not witness actual pending-MPU files on every original root')
            fleet.start()
        else:
            capture(fleet, original, bucket, 'old')
            fleet.start()
            def delete_ack(response):
                require(response.get('VersionId') == doomed['versionId'] and response.get('DeleteMarker') in {None, False}
                        and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 204, 'C10 exact-version DELETE ACK mismatch')
            call('delete_object', {'Bucket': bucket, 'Key': KEY, 'VersionId': doomed['versionId']}, check=delete_ack)
            deleted.append(doomed['versionId']); expected.remove(doomed)
            if branch == 'shared-version-reference':
                marker = call('delete_object', {'Bucket': bucket, 'Key': KEY}, check=marker_ack)
                for entry in expected:
                    if entry['key'] == KEY: entry['isLatest'] = False
                expected.append({'key': KEY, 'versionId': marker['VersionId'], 'deleteMarker': True, 'isLatest': True})
            fleet.record('C10-post-delete-all-S3-authority', verify_seed(expected, probe.inventory(bucket)))
            fleet.stop()
            authority = fleet.snapshot('C10-post-delete-all-physical-authority', quiescent=True)
            require(all(doomed['versionId'] not in values for values in references(authority, bucket, KEY).values())
                    and not any(identity[2].startswith(bucket + '/' + KEY + '/' + data_dir + '/') for identity in files(authority)),
                    'C10 DELETE did not remove the exact stale reference/payload before controlled replay')
            recovery_snapshot = authority
            arm_all(fleet, authority, bucket, 'authority')
            fleet.record('C10-controlled-single-root-replay', {'node': target[0], **fleet.remote(target[0], 'orphan-install',
                disk=target[1], bucket=bucket, key=KEY, dataDir=data_dir,
                mode='object' if branch.startswith('remove-true-') else 'data')})
            before = fleet.snapshot('C10-fault-before-GET', quiescent=True)
            present = {identity[:2] for identity in files(before) if identity[2].startswith(bucket + '/' + KEY + '/' + data_dir + '/')}
            require(present == {target}, 'C10 replay is not confined to one exact original member')
            fleet.start()
        if branch == 'remove-true-after-grace':
            while time.time() < doomed_time + grace + 4: time.sleep(min(1, max(0, doomed_time + grace + 4 - time.time())))
        age = time.time() - doomed_time
        require(branch != 'remove-true-before-grace' or 0 <= age < grace - 5, 'C10 missed the real pre-grace window')
        require(branch != 'remove-true-after-grace' or age >= grace + 3, 'C10 real grace period has not elapsed')
        fleet.record('C10-real-age-trigger', {'ageSeconds': age, 'serverLastModifiedEpoch': doomed_time, 'graceSeconds': grace,
            'globalClockChanged': False, 'remove': branch != 'remove-false'})
        from_options = {'recursive': branch != 'remove-false', 'remove': branch != 'remove-false'}
        try: from .healthy import BASE_OPTIONS
        except ImportError: from healthy import BASE_OPTIONS
        options = {**BASE_OPTIONS, 'scanMode': 2, **from_options}
        status = run_task(probe, fleet, {'bucket': bucket, 'key': KEY if branch == 'remove-false' else ''}, 2, active,
            status_endpoints=tuple(binding['endpoints'])[1:], option_overrides=from_options)
        idle_fleet(fleet, binding)
        fleet.stop()
        after = fleet.snapshot('C10-product-physical-before-target-GET', quiescent=True)
        result['physical'] = physical_safety(before, after, authority, bucket, expected, branch,
            doomed['versionId'], data_dir, target)
        result['terminal'] = terminal_safety(status, branch, bucket, options)
        # Product S3 behavior must be observed before fixture rescue can hide
        # lost references, resurrected versions or changed current semantics.
        fleet.start()
        verify_public(fleet, binding, bucket, expected, deleted, 'C10-product-before-rescue')
        fleet.stop()
        if branch == 'pending-mpu':
            require(pending_before == pending_snapshot(fleet), 'C10 Heal mutated pending-MPU bytes')
            fleet.start()
            for endpoint in binding['endpoints']:
                observed = fleet.record('C10-all-peer-list-parts', Probe(binding, endpoint).s3('list_parts',
                    {'Bucket': bucket, 'Key': KEY, 'UploadId': upload}))
                response = observed.get('response') or {}
                require(observed['acknowledged'] and response.get('IsTruncated', False) is False
                        and [{'number': row['PartNumber'], 'size': row['Size'], 'etag': row['ETag']} for row in response.get('Parts', [])]
                        == [{'number': 1, 'size': SIZE, 'etag': part['ETag']}], 'C10 pending UploadId/part ACK changed')
            completed = call('complete_multipart_upload', {'Bucket': bucket, 'Key': KEY, 'UploadId': upload,
                'MultipartUpload': {'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]}}, check=unique_version)
            for entry in expected:
                if entry['key'] == KEY: entry['isLatest'] = False
            expected.append({'key': KEY, 'versionId': completed['VersionId'], 'isLatest': True, 'deleteMarker': False,
                'payload': {'size': SIZE, 'sha256': hashlib.sha256(pending_body).hexdigest()}})
            result['pendingMPUPhysicalAndListPartsPreserved'] = True
        else:
            # Preserve the product result above, then undo only the controlled
            # replay using all original post-DELETE authorities before GET.
            recover_all(fleet)
            recovered = fleet.snapshot('C10-restored-last-DELETE-before-GET', quiescent=True)
            require(physical_content(recovered) == physical_content(authority), 'C10 restoration changed last DELETE authority')
            recovery_snapshot = None
            fleet.start()
        fleet.stop()
        restored = fleet.snapshot('C10-final-current-ACK-physical', quiescent=True)
        mapping = {(bucket, entry['key'], entry['versionId']): map_target(restored, bucket, entry['key'], 'data', 1,
                    entry['versionId'], latest_required=entry['isLatest']) for entry in expected if not entry['deleteMarker']}
        result['streaming'] = verify_live_parts(fleet, mapping, 'C10-all-live-ACK-streaming')
        fleet.start()
        verify_public(fleet, binding, bucket, expected, deleted, 'C10-all-peer-final-S3')
        result.update(verdict='PASS', postDeleteAuthorityRestored=True, backupRestorationIsProductRepair=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict='FAIL', failure={'type': type(exc).__name__, 'message': str(exc)})
    finally:
        for path, token in active:
            try: fleet.record('C10-stop-unresolved-task', probe.admin(path, [('clientToken', token), ('forceStop', 'true')], b''))
            except Exception as exc: fleet.record('C10-stop-error', {'error': str(exc)})
        if pending_mutation is not None:
            try: fleet.stop()
            except Exception as exc: fleet.record('C10-retain-stop-error', {'error': str(exc)})
            fleet.record('C10-uncertain-ACK-retained', {'state': 'DIRTY', 'request': pending_mutation,
                'namespacesRetained': True, 'oldAuthorityRestorationProhibited': True})
            clean = False
        else:
            recovery_ok = True
            if fleet.orphan_faults:
                try:
                    fleet.stop()
                    for node in fleet.nodes:
                        fleet.record('C10-before-rescue-original-logs', {'node': node, **fleet.remote(node, 'logs')})
                    recover_all(fleet)
                    restored = fleet.snapshot('C10-failure-restored-authority', quiescent=True)
                    require(recovery_snapshot is not None and physical_content(restored) == physical_content(recovery_snapshot),
                            'C10 failed-case restoration differs from the last known ACK authority')
                    fleet.start()
                    verify_public(fleet, binding, bucket, expected, deleted, 'C10-failure-rescued-S3')
                except Exception as exc:
                    fleet.record('C10-recovery-DIRTY', {'state': 'DIRTY', 'namespacesRetained': True, 'error': str(exc)})
                    try: fleet.stop()
                    except Exception as stop_error: fleet.record('C10-recovery-stop-error', {'error': str(stop_error)})
                    recovery_ok = False
            clean = fleet.cleanup() if recovery_ok else False
        result['cleanup'] = 'PASS' if clean else 'FAIL'
        fleet.record('branch-result', result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f'C10 interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('candidate', 'rc-bin', 'evidence', 'tool-directory'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--shared-directory', type=Path, default=os.environ.get('VALIDATION_HEAL_SHARED_DIRECTORY'))
    parser.add_argument('--case-id', choices=[CASE_ID], required=True)
    parser.add_argument('--testing-commit', required=True)
    parser.add_argument('--binary-sha', required=True)
    args = parser.parse_args()
    require(re.fullmatch(r'[0-9a-f]{64}', args.binary_sha) and re.fullmatch(r'[1-9][0-9]*', os.environ.get('GITHUB_RUN_ID', '')),
            'C10 requires immutable workflow/binary identities')
    require(select(CASE_ID)[0]['variants'] == list(BRANCHES), 'C10 omitted a source branch')
    require(args.shared_directory is not None, 'C10 requires its successful exact-candidate encoder before deployment')
    save(args.evidence / 'shared-preflight.json', verify_shared(args.shared_directory, json.loads(args.candidate.read_text())))
    nodes = os.environ.get('RUSTFS_NODES', 'vm000 vm001 vm002 vm003').split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in nodes),
            'C10 requires four distinct current Heal nodes')
    results = []
    try:
        for topology in ('H1', 'H2'):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, nodes[:1] if topology == 'H1' else nodes[:4], branch))
                if results[-1]['cleanup'] != 'PASS': break
            if results[-1]['cleanup'] != 'PASS': break
    finally:
        complete = len(results) == 10 and all(row['verdict'] == row['cleanup'] == 'PASS' for row in results)
        save(args.evidence / 'healthy-summary.json', {'caseId': CASE_ID, 'results': results, 'complete': complete,
            'cleanup': 'PASS' if results and all(row['cleanup'] == 'PASS' for row in results) else 'FAIL'})
    require(complete, 'C10 requires all five actual H1/H2 branches and complete original-member cleanup')


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        print(f'C10 orphan validation failed: {exc}', file=sys.stderr)
        sys.exit(1)
