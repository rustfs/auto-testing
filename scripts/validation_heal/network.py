#!/usr/bin/env python3
"""I03: actual bidirectional partitions, independent quorum witnesses and ACK reconciliation."""

import argparse
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys
import time
from urllib.parse import quote

try:
    from .heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from .heal_workload import Recorder, RecordedCallError, seed, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .shards import files, map_target, run_task, validate_fault, validate_repaired_physical, verify_cohort
    from .versions import public_version_id, suspend_fixture, validate_version_receipts, verify_quorum_contract, validate_unrecoverable_get
    from .markers import add_marker, verify_markers
    from .write_quorum import expected_reads, resolve_put, negative_put, STORAGE_REFERENCE
    from .quorum_boundary import LOCK_REFERENCE, lock_reference
    from .stop import idle_fleet
    from .network_gate import TTL_SECONDS
    from .scanner_repair import JournalWindow
    from .network_fixtures import db_parameters, seed_db, map_db, prove_db, verify_db
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, RecordedCallError, seed, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import files, map_target, run_task, validate_fault, validate_repaired_physical, verify_cohort
    from versions import public_version_id, suspend_fixture, validate_version_receipts, verify_quorum_contract, validate_unrecoverable_get
    from markers import add_marker, verify_markers
    from write_quorum import expected_reads, resolve_put, negative_put, STORAGE_REFERENCE
    from quorum_boundary import LOCK_REFERENCE, lock_reference
    from stop import idle_fleet
    from network_gate import TTL_SECONDS
    from scanner_repair import JournalWindow
    from network_fixtures import db_parameters, seed_db, map_db, prove_db, verify_db


CASE_ID = 'heal.network-partition-and-lock-quorum'
BRANCHES = ('minority-partition', 'read-boundary', 'write-boundary', 'lock-boundary', 'rejoin')
SIZE = 5 * 1024**2 + 97
REFERENCE = {'candidateCommit': LOCK_REFERENCE['candidateCommit'], 'lock': LOCK_REFERENCE,
    'storage': STORAGE_REFERENCE, 'topology': 'four original nodes, sixteen original members, explicit EC:8',
    'network': {'servicePort': 9000, 'managementPort': 22, 'ttlSeconds': TTL_SECONDS,
        'rules': 'one owned nft table per host; exact peer IPv4 and TCP source/destination port',
        'mutation': 'all expiry timers armed before the first atomic table transaction'},
    'load': {'fixtureObjects': 4, 'versionsPerObject': 3, 'retainedNullVersions': 4, 'DBObjects': 128,
        'currentAndHistoricalMarkers': True, 'partitionVersioning': 'Enabled after witnessed Suspended/null operations',
        'partitionPUTs': 2, 'recoveredPUTs': 1,
        'automaticRetries': 0, 'negativeTaskDeadlineSeconds': 330},
    'writePolicy': 'three-peer groups retain R but use independent private gates to measure W-1; two-peer groups lose W and exclusive lock quorum'}


# Storage geometry is reviewed independently of the namespace lock policy.
STORAGE_REFERENCES = {
    STORAGE_REFERENCE['candidateCommit']: STORAGE_REFERENCE,
    '1c7fc818dd858247c45b39542abd8291b1e6e31b': {
        **STORAGE_REFERENCE, 'candidateCommit': '1c7fc818dd858247c45b39542abd8291b1e6e31b'},
}


GET_SOURCE_FILES = {
    '072b57b37559fbe626aeda8abf61617fdcbdd926': {
        'rustfs/src/error.rs': '656d38972c06339015c174405571a94f19d8dd64a680e1a7ca631f80205f99a7',
        'crates/ecstore/src/set_disk/ops/object.rs': 'f0f407509b5a12803a191cc1c3a9eb079a15a80a4bbcacca70141150364628ca',
        'crates/heal/src/heal/task/heal_bucket.rs': '2f89bc694d78b68cc2c505fdd854770176fb87cc97d31ea80a1b810f47d8c881'},
    '1c7fc818dd858247c45b39542abd8291b1e6e31b': {
        'rustfs/src/error.rs': 'e7cff0e28978b49d3ee0e8c24da1fa439b3118a7d3f7304edf00dc0535691598',
        'crates/ecstore/src/set_disk/ops/object.rs': 'fdcfda1a8811a93537f9cd47cadac7380bee66b3d14242ca91500bbf38e6bedb',
        'crates/heal/src/heal/task/heal_bucket.rs': 'fa38b21ee7997a909fc5f0671d777fd49983ccaaa18ced85bae8de76b75835ad'},
}


def source_reference(candidate_commit):
    require(isinstance(candidate_commit, str) and candidate_commit in STORAGE_REFERENCES,
            'network storage policy is not reviewed for this candidate')
    return {**copy.deepcopy(REFERENCE), 'candidateCommit': candidate_commit,
            'lock': lock_reference(candidate_commit),
            'storage': copy.deepcopy(STORAGE_REFERENCES[candidate_commit]),
            'getPolicy': {'lockBeforeObjectMetadata': True,
                'lockUnavailableGet': {'httpStatus': 503, 'code': 'ServiceUnavailable'},
                'allVersionRecovery': {'recursive': True},
                'sourceFiles': {**GET_SOURCE_FILES[candidate_commit],
                    'crates/heal/src/heal/channel.rs': '7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0'}}}


def negative_get_contract(contract, side, plan):
    policy = source_reference(contract['candidateCommit'])['getPolicy']
    require(contract.get('recognized') is True and contract['sourceFiles'].get('rustfs/src/error.rs')
            == policy['sourceFiles']['rustfs/src/error.rs'], 'partition GET error mapping differs from exact candidate')
    require(type(side.get('lockClients')) is int and 0 <= side['lockClients'] <= 4
            and plan.get('sharedLockQuorum') == 2, 'partition GET has an unknown shared-lock domain')
    if side['lockClients'] < plan['sharedLockQuorum']:
        return {**contract, 'unrecoverableGet': policy['lockUnavailableGet']}
    return contract


def recover_versions(probe, fleet, binding, bucket, expected, target, active):
    require(len(expected) >= 3 and len({entry['key'] for entry in expected}) == 1
            and len({entry['versionId'] for entry in expected}) == len(expected)
            and (target['key'], target['versionId']) in {(entry['key'], entry['versionId']) for entry in expected}
            and sum(not entry['deleteMarker'] and entry['versionId'] != 'null' for entry in expected) >= 3,
            'network recovery requires all acknowledged versions of one exact three-version key')
    current = run_task(probe, fleet, {'bucket': bucket, 'key': target['key']}, 2, active,
        option_overrides={'recursive': True}, status_endpoints=tuple(binding['endpoints'])[1:])
    return validate_version_receipts(current, bucket, expected, 2,
        required_repairs={(target['key'], target['versionId'])})


def seed_versions(recorder, fleet, bucket):
    expected = seed(recorder, bucket, 'DV', 4, [SIZE])
    expected = suspend_fixture(recorder, bucket, expected)
    suspended = fleet.record('suspended-null-S3-baseline', recorder.probe.inventory(bucket))
    fleet.record('suspended-null-ACK-manifest', {'expected': expected, **verify_seed(expected, suspended)})
    # Null overwrites and marker removals are real S3 operations. Re-enable
    # before the partition so new writes must return distinct non-null IDs.
    recorder.call('put_bucket_versioning', {'Bucket': bucket, 'VersioningConfiguration': {'Status': 'Enabled'}})
    require(recorder.call('get_bucket_versioning', {'Bucket': bucket}).get('Status') == 'Enabled',
            'network fixture did not persist re-enabled versioning')
    key = 'objects/0000.bin'
    expected = add_marker(expected, key, recorder.call('delete_object', {'Bucket': bucket, 'Key': key}))
    fleet.record('version-fixture-coverage', version_coverage(expected))
    return expected


def version_coverage(expected):
    keys = {f'objects/{index:04d}.bin' for index in range(4)}
    require({entry['key'] for entry in expected} == keys
            and len({(entry['key'], entry['versionId']) for entry in expected}) == len(expected),
            'network fixture omits a key or duplicates a version identity')
    counts = {}
    for key in sorted(keys):
        entries = [entry for entry in expected if entry['key'] == key]
        data = [entry for entry in entries if not entry['deleteMarker'] and entry['versionId'] != 'null']
        require(len(data) == 3 and all(isinstance(entry['versionId'], str) and entry['versionId'] for entry in data)
                and len({entry['payload']['sha256'] for entry in data}) == 3
                and sum(entry['isLatest'] is True for entry in entries) == 1
                and sum(entry['versionId'] == 'null' for entry in entries) == 1,
                'network fixture lacks three distinct data ACKs, one latest identity or its retained null version')
        counts[key] = len(entries)
    require(any(entry['deleteMarker'] and entry['isLatest'] for entry in expected)
            and any(entry['deleteMarker'] and not entry['isLatest'] for entry in expected)
            and any(entry['versionId'] == 'null' and entry['deleteMarker'] for entry in expected)
            and any(entry['versionId'] == 'null' and not entry['deleteMarker'] for entry in expected),
            'network fixture lacks current/historical markers or live/deleted null identities')
    return {'acknowledgedIdentitiesPerKey': counts, 'threeDistinctDataVersionsPerKey': True,
            'currentAndHistoricalMarkers': True, 'retainedLiveAndMarkerNullVersions': True,
            'partitionVersioning': 'Enabled', 'suspendedPartitionClaimed': False,
            'remainingSourceFixtures': ['DB boundary matrix', 'DL concurrent operation matrix'],
            'canonicalCasePassClaimed': False}


def version_heads(probe, fleet, bucket, expected, label):
    result = []
    for entry in expected:
        if entry['deleteMarker']:
            continue
        observed = fleet.record(label, probe.s3('head_object', {
            'Bucket': bucket, 'Key': entry['key'], 'VersionId': entry['versionId']}))
        response = observed.get('response') or {}
        require(observed.get('acknowledged') is True and observed.get('resultUnknown') is False
                and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200
                and response.get('ContentLength') == entry['payload']['size']
                and response.get('ETag') and response.get('LastModified')
                and response.get('VersionId', 'null') == entry['versionId'],
                'version HEAD lost its ACK identity, length, ETag or Last-Modified')
        result.append({'key': entry['key'], 'versionId': entry['versionId'],
            'properties': {name: response.get(name) for name in
                ('ContentLength', 'ETag', 'LastModified', 'ContentType', 'Metadata')}})
    return result


def plan_for(snapshot, target, branch):
    nodes = list(snapshot)
    geometry = target['logical']['erasure']
    cohort = target['cohort']
    require(branch in BRANCHES and len(nodes) == 4 and geometry['data_blocks'] == geometry['parity_blocks'] == 8
            and len(cohort) == len({(row['node'], row['disk']) for row in cohort}) == 16
            and all({row['disk'] for row in cohort if row['node'] == node} == set(range(4)) for node in nodes),
            'I03 requires the observed balanced sixteen-member cross-node set')
    groups = [[0, 1], [2, 3]] if branch in {'read-boundary', 'lock-boundary'} else [[0, 1, 2], [3]]
    require(target['node'] == nodes[3], 'partition defect must be on the isolated fourth original member')
    gates = {nodes[0]: [0, 1, 2, 3]} if len(groups[0]) == 3 else {}
    sides = []
    for group in groups:
        slots = [row for row in cohort if nodes.index(row['node']) in group]
        sources = len(slots) - int(3 in group)
        writable = len(slots) - sum(len(gates.get(nodes[index], [])) for index in group)
        require(writable < 9 and len(group) < 3 or writable == 8 and len(group) == 3,
                'partition side still has safe write admission')
        sides.append({'members': group, 'endpoint': f'node{group[0] + 1}', 'readableSources': sources,
            'metadataAuthorities': len(slots), 'writableDrives': writable, 'lockClients': len(group),
            'GETAllowed': sources >= 8, 'PUTAllowed': False, 'exclusiveLockQuorumPresent': len(group) >= 3})
    require(sides[0]['readableSources'] >= 8 and (len(groups[0]) != 2 or sides[0]['readableSources'] == 8),
            'read boundary does not preserve the exact actual R sources')
    return {'groups': groups, 'sides': sides, 'readQuorum': 8, 'writeQuorum': 9,
        'sharedLockQuorum': 2, 'exclusiveLockQuorum': 3, 'gates': gates,
        'blocked': {nodes[index]: [other for other in range(4) if not any(index in group and other in group for group in groups)]
                    for index in range(4)}}


def validate_witness(observed, plan, nodes):
    require(set(observed) == set(nodes), 'network witness omitted an original node')
    for index, node in enumerate(nodes):
        value = observed[node]
        require(value.get('state') == 'ACTIVE' and value.get('localIndex') == index
                and value.get('expiryArmed') is True and value.get('process', {}).get('pid', 0) > 1,
                'network witness has no actual original service or active expiry')
        rows = value.get('probes', [])
        require(len(rows) == 3 and {row['peerIndex'] for row in rows} == set(range(4)) - {index},
                'network probes omitted or duplicated a peer direction')
        require(all(row['service']['connected'] is (row['peerIndex'] not in plan['blocked'][node])
                    and row['management']['connected'] is True for row in rows),
                'partition matrix or management-channel preservation differs')
        require(sum(row['packets'] for row in value['countersAfter']) > sum(row['packets'] for row in value['countersBefore']),
                'network probe has no packet-counter evidence')
    return {'allTwelvePeerDirectionsVerified': True, 'actualPacketCounters': True,
            'allManagementDirectionsReachable': True, 'servicesNotStoppedToSimulatePartition': True}


def validate_partial(value, peers):
    coverage = value.get('coverage') or {}
    require(value.get('clusterStatusComplete') is False and value.get('state') in {'degraded', 'active'}
            and all(type(coverage.get(name)) is int for name in ('expected', 'responded', 'unknown'))
            and coverage['expected'] == 4 and coverage['responded'] == peers and coverage['unknown'] == 4 - peers
            and isinstance(coverage.get('reasons'), list) and coverage['reasons'],
            'partitioned endpoint falsely reports complete/global health or hides actual peer coverage')
    return {'responded': peers, 'unknown': 4 - peers, 'globalCompleteClaimed': False}


def validate_negative_task(current, bucket, expected, target):
    outcome = current.get('outcome') or {}
    execution = outcome.get('execution', {})
    require(current.get('summary') in {'finished', 'stopped'} and execution.get('state') in {
        'completed', 'completed_with_errors', 'aborted'} and outcome.get('coverage') in {'partial', 'complete', 'unknown'}
        and current.get('truncated', False) is False and outcome.get('objectsTruncated') is False,
        'partition task has no bounded explicit unresolved terminal result')
    wanted = {(row['key'], row['versionId']) for row in expected}
    target_pair = target['key'], target['versionId']
    counters = outcome.get('counters', {})
    for name in ('processed', 'healed', 'unchanged', 'skipped', 'failed', 'unknown', 'attemptFailures'):
        require(type(counters.get(name)) is int and counters[name] >= 0, 'partition task omits typed error counters')
    require(counters.get('overflowed') is False and counters['healed'] == 0
            and counters['processed'] == sum(counters[name] for name in ('healed', 'unchanged', 'skipped', 'failed'))
            and counters['unknown'] <= counters['skipped'] and counters['processed'] <= len(wanted),
            'partition task hides unresolved work or claims an unsafe repair')
    seen, unresolved, states = set(), set(), []
    for row in outcome.get('objects', []):
        identity = row.get('identity', {})
        pair = identity.get('object'), public_version_id(identity.get('versionId'))
        require(identity.get('bucket') == bucket and pair in wanted and pair not in seen,
                'partition task reports foreign or duplicate responsibility')
        state = row.get('disposition', {}).get('state')
        require(state in {'verified_healthy', 'failed', 'deferred', 'unknown', 'cancelled'},
                'partition task reports repaired/absent or an unrelated disposition')
        if state == 'verified_healthy':
            require(pair != target_pair and identity.get('bucketIncarnationId')
                    and type(identity.get('poolIndex')) is int and identity['poolIndex'] == 0
                    and type(identity.get('setIndex')) is int and identity['setIndex'] == 0,
                    'missing target was reported healthy, or a healthy receipt lacks authority')
        else:
            unresolved.add(pair)
        seen.add(pair)
        states.append(state)
    require(counters['processed'] == len(seen), 'bounded partition counters differ from exact version receipts')
    require(counters['unchanged'] == states.count('verified_healthy') and counters['failed'] == states.count('failed')
            and counters['skipped'] == sum(states.count(state) for state in ('deferred', 'unknown', 'cancelled'))
            and counters['unknown'] == states.count('unknown') and counters['attemptFailures'] >= counters['failed'],
            'partition counters conceal the actual unresolved receipt dispositions or failed attempts')
    require(target_pair in unresolved or execution == {'state': 'aborted', 'reason': 'untraversable'}
            and outcome['coverage'] != 'complete' and counters['processed'] == 0,
            'partition task lost the damaged version obligation or silently completed it')
    require(not counters['failed'] or execution['state'] == 'completed_with_errors',
            'partition task erases its failed execution status')
    return {'missingVersionUnresolved': True, 'unsafeRepairClaimed': False,
            'execution': execution, 'coverage': outcome['coverage'], 'counters': counters}


def negative_heal(probe, fleet, bucket, expected, target, active):
    path = '/rustfs/admin/v3/heal/' + quote(bucket, safe='') + '/' + quote(target['key'], safe='/')
    admission = fleet.record('partition-Heal-admission', probe.admin(path, [], json.dumps({**BASE_OPTIONS, 'recursive': True}).encode()))
    if admission.get('httpStatus') != 200:
        # No retry: require an actual structured storage/lock refusal. An
        # absent token or transport exception alone cannot prove safe refusal.
        body = admission.get('response')
        if isinstance(body, str):
            import xml.etree.ElementTree as ET
            body = {item.tag.rsplit('}', 1)[-1]: item.text for item in ET.fromstring(body).iter()}
        require(admission.get('httpStatus') in {500, 503} and isinstance(body, dict)
                and (body.get('Code') or body.get('code')) in {'InternalError', 'XMinioWriteQuorum', 'XMinioLockTimeout', 'ServiceUnavailable'}
                and not body.get('clientToken'), 'partition Heal admission did not return a structured safe refusal')
        return {'admissionRefused': True, 'repairClaimed': False, 'requestRetried': False}
    token = admission.get('response', {}).get('clientToken')
    require(isinstance(token, str) and token, 'partition Heal admitted without an actual token')
    active.append((probe.endpoint_name, path, token))
    deadline = time.monotonic() + 330
    while True:
        response = fleet.record('partition-Heal-status', probe.admin(path, [('clientToken', token)], b''))
        require(response.get('httpStatus') == 200 and isinstance(response.get('response'), dict),
                'partition task status disappeared or has an unknown response')
        current = response['response']
        if current.get('summary') != 'running':
            break
        require(time.monotonic() < deadline, 'partition task exceeded its frozen unresolved-task deadline')
        time.sleep(0.2)
    proof = validate_negative_task(current, bucket, expected, target)
    repeated = fleet.record('partition-terminal-repeat', probe.admin(path, [('clientToken', token)], b''))
    require(repeated.get('httpStatus') == 200 and canonical(repeated.get('response', {}).get('outcome')) == canonical(current['outcome']),
            'negative partition responsibility changed at its original token')
    active.remove((probe.endpoint_name, path, token))
    return {**proof, 'taskId': token, 'requestRetried': False}


def project(snapshot, bucket, keys):
    result = copy.deepcopy(snapshot)
    prefixes = tuple(bucket + '/' + key + '/' for key in keys)
    for value in result.values():
        value['files'] = [row for row in value['files'] if not row['path'].startswith(prefixes)]
    return result


def unique_put(probe, directory, bucket, key, rows):
    body = payload(f'{bucket}/{key}/0', SIZE)
    entry = {'endpoint': probe.endpoint_name, 'key': key, 'payload': {'size': SIZE, 'sha256': hashlib.sha256(body).hexdigest()}}
    rows.append(entry)  # Register before invocation, including collector failures.
    recorder = Recorder(probe, directory / key.replace('/', '-'))
    try:
        response = recorder.call('put_object', {'Bucket': bucket, 'Key': key,
            'ContentType': 'application/octet-stream', 'Metadata': {'heal-fixture': 'I03', 'nonce': key},
            'Tagging': 'suite=heal&fixture=I03'}, body)
    except RecordedCallError as exc:
        entry['observed'] = exc.observation
    else:
        entry['observed'] = {'acknowledged': True, 'resultUnknown': False, 'clientCallInvoked': True,
                             'response': response, 'payload': entry['payload']}
    return entry


def resolve_writes(fleet, binding, bucket, rows):
    for row in rows:
        reads = [fleet.record('all-peer-unique-write-resolution', Probe(binding, endpoint).s3('get_object', {
            'Bucket': bucket, 'Key': row['key']})) for endpoint in binding['endpoints']]
        observed = row.get('observed', {'acknowledged': False, 'resultUnknown': True})
        resolution = resolve_put(observed, reads, row['payload'])
        if resolution['state'] == 'COMMITTED':
            versions = {read.get('response', {}).get('VersionId') for read in reads}
            require(len(versions) == 1 and next(iter(versions)) not in {None, 'null'},
                    'peers disagree on the unique committed version identity')
            row['versionId'] = next(iter(versions))
            require(not observed.get('acknowledged') or observed['response'].get('VersionId') == row['versionId'],
                    'unique PUT ACK was replaced by a different committed version')
        row['resolution'] = resolution
    return {'allOriginalRequestsResolved': True, 'automaticRetries': 0}


def release_network(fleet):
    for node in tuple(fleet.network_gates):
        value = fleet.record('release-owned-network-table', {'node': node, **fleet.remote(node, 'network-release')})
        require(value['state'] == 'CLEAR' and value.get('rulesRemaining') == 0,
                'network recovery did not remove every owned rule')
        fleet.network_gates.remove(node)
        require(value.get('expiryTriggered') is False, 'partition expired before its complete evidence window')


def partition_get(probe, fleet, params):
    """Retain one GET's raw response and journal window before its oracle can fail."""
    identity_fields = ('pid', 'startTicks', 'bootId', 'unit', 'executable', 'binarySha256',
                       'mountNamespace', 'commandSha256', 'environmentFileSha256', 'settings')
    first_sequence = fleet.sequence + 1
    before = fleet.record('partition-GET-process-before', {node: fleet.remote(node, 'process') for node in fleet.nodes})
    require(all(all(field in value for field in identity_fields) and type(value['pid']) is int and value['pid'] > 1
                and value['binarySha256'] == probe.binding['candidateBinarySha256'] for value in before.values()),
            'partition GET diagnostic lacks a live candidate process')
    # Raw pages are retained; parsed events are not a causal proof or oracle.
    window = JournalWindow(fleet, 'partition-GET-journal', event_targets=set())
    observed, request_sequence = None, None
    try:
        observed = fleet.record('partition-explicit-version-GET', probe.s3('get_object', params))
        request_sequence = fleet.sequence
    finally:
        errors, after = [], {}
        try:
            window.capture()
        except Exception as exc:
            errors.append({'stage': 'journal', 'type': type(exc).__name__, 'message': str(exc)})
        for node in fleet.nodes:
            try:
                after[node] = fleet.remote(node, 'process')
                require(all(field in after[node] and after[node][field] == before[node][field] for field in identity_fields),
                        'partition GET crossed a candidate process identity change')
            except Exception as exc:
                errors.append({'stage': 'process', 'node': node, 'type': type(exc).__name__, 'message': str(exc)})
        fleet.record('partition-GET-process-after', after)
        if observed is None:
            errors.append({'stage': 'request', 'message': 'GET raised before returning a raw observation'})
        response = (observed or {}).get('response') or {}
        diagnostics = fleet.record('partition-GET-diagnostics', {
            'complete': not errors, 'endpoint': probe.endpoint_name, 'params': params,
            'requestId': response.get('ResponseMetadata', {}).get('RequestId'),
            'firstEvidenceSequence': first_sequence, 'requestEvidenceSequence': request_sequence,
            'lastEvidenceSequence': fleet.sequence, 'errors': errors,
            'causalAttributionClaimed': False, 'automaticRequestRetries': 0})
    return observed, diagnostics


def partition_db(probe, fleet, bucket, representatives, side, plan, contract):
    results = []
    for row in representatives:
        entry = row['expected']
        observed, diagnostics = partition_get(probe, fleet, {
            'Bucket': bucket, 'Key': entry['key'], 'VersionId': entry['versionId']})
        read_quorum = row['target']['logical']['erasure']['data_blocks']
        readable = side['lockClients'] >= plan['sharedLockQuorum'] and side['metadataAuthorities'] >= read_quorum
        if readable:
            require(observed.get('acknowledged') is True and observed.get('resultUnknown') is False
                    and observed.get('response', {}).get('ResponseMetadata', {}).get('HTTPStatusCode') == 200
                    and observed.get('payload') == entry['payload'],
                    'R-capable partition lost an acknowledged DB boundary control')
        else:
            validate_unrecoverable_get(observed, negative_get_contract(contract, side, plan))
        require(diagnostics['complete'], 'DB boundary GET diagnostics are incomplete')
        results.append({'key': entry['key'], 'shape': row['shape'], 'size': entry['payload']['size'],
            'readQuorum': read_quorum, 'availableSources': side['metadataAuthorities'], 'readable': readable})
    return results


def run_branch(args, nodes, branch):
    directory = args.evidence / 'H2' / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h2"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile='lock-quorum')
    candidate = json.loads(args.candidate.read_text())
    reference = source_reference(candidate['source_sha'])
    bucket = 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {'schemaVersion': 1, 'runId': run_id, 'jumpHostOnly': True, 'exclusiveHealService': True,
        'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), 'bucketPrefix': bucket,
        'endpoints': {f'node{index}': f'http://rustfs-node{index}:9000' for index in range(1, 5)}, 'defaultEndpoint': 'node1',
        'candidateCommit': candidate['source_sha'], 'harnessCommit': args.testing_commit,
        'candidatePackageSha256': candidate['package_sha256'], 'candidateBinarySha256': args.binary_sha,
        'rcBinarySha256': hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), 'maxInventoryObjects': 128,
        'maxInventoryBytes': 512 * 1024**2}
    fleet.record('binding', binding)
    fleet.record('network-source-contract', reference)
    manifest = fleet.record('inspector-build', verify_inspector(args.tool_directory, candidate))
    contract = verify_quorum_contract(manifest, candidate)
    result = {'topology': 'H2', 'branch': branch, 'verdict': 'RUNNING', 'cleanup': 'PENDING'}
    probe, active, recovery_active, rows = Probe(binding), [], [], []
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        recorder = Recorder(probe, directory / 'fixture')
        expected = seed_versions(recorder, fleet, bucket)
        key_versions = [entry for entry in expected if entry['key'] == 'objects/0001.bin']
        selected = next(entry for entry in key_versions if not entry['deleteMarker'] and entry['versionId'] == 'null')
        calibration = fleet.snapshot('DB-size-calibration-live')
        calibration_target = map_target(calibration, bucket, selected['key'], 'data', 1, selected['versionId'])
        db_plan = fleet.record('DB-source-size-plan', db_parameters(manifest, candidate, calibration_target, calibration))
        db_bucket = bucket + '-db'
        db_expected = seed_db(Recorder(probe, directory / 'DB-fixture'), db_bucket, db_plan)
        db_baseline = fleet.record('DB-S3-baseline', probe.inventory(db_bucket, unversioned_current_tags=True))
        fleet.record('DB-ACK-manifest', {'expected': db_expected, **verify_seed(db_expected, db_baseline)})
        db_heads = version_heads(probe, fleet, db_bucket, db_expected, 'DB-original-HEAD')
        baseline = fleet.record('all-version-S3-baseline', probe.inventory(bucket))
        fleet.record('exact-ACK-manifest', {'expected': expected, **verify_seed(expected, baseline)})
        original_heads = version_heads(probe, fleet, bucket, expected, 'original-version-HEAD')
        for key in sorted({entry['key'] for entry in expected if entry['deleteMarker']}):
            verify_markers(probe, fleet, bucket, key, expected)
        fleet.stop()
        physical_buckets = [bucket, db_bucket]
        original = fleet.snapshot('physical-baseline', quiescent=True, buckets=physical_buckets)
        validate_topology(original, 'H2')
        target = map_target(original, bucket, selected['key'], 'data', 1, selected['versionId'], latest_required=False)
        target = {**target, **next(row for row in target['cohort'] if row['node'] == nodes[3])}
        cohorts = [map_target(original, bucket, entry['key'], 'data', 1, entry['versionId'], latest_required=False)
                   for entry in expected if not entry['deleteMarker']]
        db_representatives, db_witnesses = map_db(original, db_bucket, db_expected, db_plan)
        fleet.record('DB-all128-physical-shapes', {'objects': db_witnesses, 'allBoundariesObserved': True})
        plan = fleet.record('actual-partition-quorum-plan', plan_for(original, target, branch))
        params = {'disk': target['disk'], 'faultId': 'I03-missing-target'}
        registration = target['node'], params
        fleet.faults.append(registration)
        receipt = fleet.record('exact-missing-target', fleet.remote(target['node'], 'apply-fault', **params,
            relativePath=target['relativePath'], kind='missing', expectedSha256=target['sha256']))
        faulted = fleet.snapshot('physical-exact-fault', quiescent=True, buckets=physical_buckets)
        validate_fault(original, faulted, target, receipt)
        for node, disks in plan['gates'].items():
            fleet.quorum_gates.add(node)
            fleet.record('independent-W-minus-one-gate', {'node': node, **fleet.remote(node, 'quorum-gate', disks=disks)})
        fleet.start()
        preflight = fleet.record('network-actual-address-process-attestation', {node: fleet.remote(node, 'network-preflight') for node in nodes})
        addresses = preflight[nodes[0]]['addresses']
        require(all(value['addresses'] == addresses and value['localIndex'] == index
                    and value['exclusiveServicePort'] == 9000 for index, value in enumerate(preflight.values())),
                'actual host address, service ownership or peer alias order differs')
        # Every independent expiry is ready before the first rule is installed.
        for index, node in enumerate(nodes):
            fleet.network_gates.add(node)
            fleet.record('network-expiry-and-rule-intent', {'node': node, **fleet.remote(node, 'network-prepare',
                addresses=addresses, localIndex=index, blockedIndices=plan['blocked'][node])})
        for node in nodes:
            fleet.record('atomic-owned-table-activation', {'node': node, **fleet.remote(node, 'network-activate')})
        def witness(label):
            raw = fleet.record(label, {node: fleet.remote(node, 'network-observe') for node in nodes})
            return validate_witness(raw, plan, nodes)
        result['networkBefore'] = witness('all-directed-peer-probes-before')
        gates = fleet.record('actual-service-readable-writable-roots', {node: fleet.remote(node, 'quorum-gate-probe',
            relativePath=f"{bucket}/{selected['key']}/xl.meta", expectedReads=expected_reads(original, node, f"{bucket}/{selected['key']}/xl.meta")) for node in nodes})
        for side in plan['sides']:
            require(sum(row['writable'] for index in side['members'] for row in gates[nodes[index]]['disks']) == side['writableDrives']
                    and all(row['readableMetadata'] and row['sameServiceUidAndMountNamespace'] for index in side['members']
                            for row in gates[nodes[index]]['disks']), 'actual independent R/W witnesses differ from partition plan')
        for side in plan['sides']:
            endpoint = side['endpoint']
            local_probe = Probe(binding, endpoint)
            for version in key_versions:
                read, diagnostics = partition_get(local_probe, fleet, {
                    'Bucket': bucket, 'Key': version['key'], 'VersionId': version['versionId']})
                available = side['metadataAuthorities'] - int(3 in side['members'] and version['versionId'] == selected['versionId'])
                if available >= plan['readQuorum']:
                    require(read.get('acknowledged') is True and read.get('resultUnknown') is False
                            and read.get('payload') == version['payload'], 'R-capable partition cannot redeem its exact original ACK')
                else:
                    scoped_contract = negative_get_contract(contract, side, plan)
                    fleet.record('partition-GET-error-contract', {'endpoint': endpoint, 'versionId': version['versionId'],
                        'lockClients': side['lockClients'], 'sharedLockQuorum': plan['sharedLockQuorum'],
                        'availableSources': available, 'readQuorum': plan['readQuorum'],
                        'requiredError': scoped_contract['unrecoverableGet']})
                    validate_unrecoverable_get(read, scoped_contract)
                require(diagnostics['complete'], 'partition GET diagnostics are incomplete; inspect retained raw response')
            row = unique_put(local_probe, directory, bucket, 'partition/' + endpoint + '.bin', rows)
            row['negativeProof'] = negative_put(row['observed'])
            result.setdefault('DB-partition-controls', {})[endpoint] = partition_db(
                local_probe, fleet, db_bucket, db_representatives, side, plan, contract)
            result.setdefault('partitionTasks', {})[endpoint] = negative_heal(local_probe, fleet, bucket, key_versions, target, active)
            status = fleet.record('partition-raw-background', local_probe.admin('/rustfs/admin/v3/background-heal/status', [], b''))
            require(status.get('httpStatus') == 200 and isinstance(status.get('response'), dict), 'partition background status unavailable')
            result.setdefault('coverage', {})[endpoint] = validate_partial(status['response'], len(side['members']))
        result['networkAfter'] = witness('all-directed-peer-probes-after')
        fleet.record('partition-process-resource-observations', {node: fleet.remote(node, 'process-metrics') for node in nodes})
        fleet.stop()
        scene = fleet.snapshot('negative-scene-before-release', quiescent=True, buckets=physical_buckets)
        require(physical_content(faulted) == physical_content(project(scene, bucket, [row['key'] for row in rows])),
                'partition writes/Heal changed an original version, control or damaged target')
        for node in nodes:
            fleet.record('partition-peer-logs', {'node': node, **fleet.remote(node, 'logs')})
        release_network(fleet)
        for node in tuple(fleet.quorum_gates):
            fleet.record('release-owned-W-gate', fleet.remote(node, 'quorum-gate', mode='clear'))
            fleet.quorum_gates.remove(node)
        fleet.start()
        require(physical_content(scene) == physical_content(fleet.snapshot('all-peer-return-before-independent-repair', buckets=physical_buckets)),
                'network recovery repaired a target before the independent product task')
        result['recoveryResponsibility'] = recover_versions(probe, fleet, binding, bucket, key_versions, selected, recovery_active)
        idle_fleet(fleet, binding)
        fleet.stop()
        after = fleet.snapshot('physical-rebuilt-before-any-recovered-GET', quiescent=True, buckets=physical_buckets)
        result['physical'] = validate_repaired_physical(original, project(after, bucket, [row['key'] for row in rows]), target)
        result['streaming'] = [verify_cohort(fleet, cohort, 'every-original-version-streaming') for cohort in cohorts]
        result['DB-physical'] = prove_db(fleet, db_representatives)
        fleet.faults.remove(registration)
        fleet.start()
        result['writeResolution'] = resolve_writes(fleet, binding, bucket, rows)
        require(all(row['resolution']['state'] == 'ABSENT' for row in rows), 'a W/lock-negative partition nevertheless committed a new version')
        released = unique_put(probe, directory, bucket, 'recovered/unique.bin', rows)
        require(released['observed']['acknowledged'] is True and released['observed']['response'].get('VersionId') not in {None, 'null'},
                'fully recovered cluster cannot acknowledge a distinct safe PUT')
        resolve_writes(fleet, binding, bucket, [released])
        augmented = expected + [{'key': released['key'], 'versionId': released['versionId'], 'isLatest': True,
                                'deleteMarker': False, 'payload': released['payload']}]
        inventories = []
        for endpoint in binding['endpoints']:
            peer = Probe(binding, endpoint)
            inventory = fleet.record('all-peer-complete-final-S3', peer.inventory(bucket))
            verify_seed(augmented, inventory)
            require(version_heads(peer, fleet, bucket, expected, 'all-peer-original-version-HEAD') == original_heads,
                    'network recovery changed an original version HEAD or Last-Modified')
            subset = {**inventory, 'entries': [row for row in inventory['entries'] if row['key'] != released['key']]}
            compare_inventories(baseline, subset)
            inventories.append(inventory)
        for inventory in inventories[1:]:
            compare_inventories(inventories[0], inventory)
        result['markers'] = [verify_markers(probe, fleet, bucket, key, expected)
            for key in sorted({entry['key'] for entry in expected if entry['deleteMarker']})]
        result['DB-final'] = verify_db(probe, fleet, db_bucket, db_expected, db_baseline, db_representatives, db_plan)
        for endpoint in binding['endpoints']:
            require(version_heads(Probe(binding, endpoint), fleet, db_bucket, db_expected, 'DB-final-HEAD') == db_heads,
                    'network recovery changed an acknowledged DB HEAD or Last-Modified')
        require(all(row['properties'].get('Metadata') == {'heal-fixture': 'I03', 'nonce': released['key']}
                    and row['tags'] == [{'Key': 'fixture', 'Value': 'I03'}, {'Key': 'suite', 'Value': 'heal'}]
                    for inventory in inventories for row in inventory['entries'] if row['key'] == released['key']),
                'recovered PUT lost its public metadata or tags')
        result.update(verdict='PASS', everyOriginalVersionPreserved=True, noForkOrStaleOverwrite=True)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict='FAIL', failure={'type': type(exc).__name__, 'message': str(exc)})
    finally:
        dirty = False
        active.extend(('node1', path, token) for path, token in recovery_active)
        for endpoint, path, token in active:
            try:
                fleet.record('stop-original-partition-task', Probe(binding, endpoint).admin(path, [('clientToken', token), ('forceStop', 'true')], b''))
            except Exception as exc:
                fleet.record('stop-partition-task-error', {'error': str(exc)})
        try:
            if fleet.network_gates or fleet.quorum_gates:
                fleet.stop()
                release_network(fleet)
                for node in tuple(fleet.quorum_gates):
                    fleet.record('failed-branch-W-release', fleet.remote(node, 'quorum-gate', mode='clear'))
                    fleet.quorum_gates.remove(node)
                fleet.start()
            pending = [row for row in rows if 'resolution' not in row]
            if pending:
                fleet.stop()
                fleet.start()
                resolve_writes(fleet, binding, bucket, pending)
        except Exception as exc:
            dirty = True
            result['recoveryError'] = str(exc)
        if dirty:
            fleet.stop()
            fleet.record('retain-unresolved-network-or-write', {'state': 'DIRTY', 'namespacesRetained': True, 'writes': rows})
            clean = False
        else:
            clean = fleet.cleanup()
        result.update(cleanup='PASS' if clean else 'FAIL', writes=rows)
        fleet.record('branch-result', result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'I03 interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('candidate', 'rc-bin', 'evidence', 'tool-directory'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--case-id', choices=[CASE_ID], required=True)
    parser.add_argument('--testing-commit', required=True)
    parser.add_argument('--binary-sha', required=True)
    args = parser.parse_args()
    require(re.fullmatch(r'[0-9a-f]{64}', args.binary_sha) and re.fullmatch(r'[1-9][0-9]*', os.environ.get('GITHUB_RUN_ID', '')),
            'immutable workflow candidate identity required')
    require(select(CASE_ID)[0]['variants'] == list(BRANCHES), 'I03 source branch omitted')
    nodes = os.environ.get('RUSTFS_NODES', 'vm000 vm001 vm002 vm003').split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in nodes),
            'four distinct current Heal nodes required')
    results = []
    try:
        for branch in BRANCHES:
            results.append(run_branch(args, nodes[:4], branch))
            if results[-1]['cleanup'] != 'PASS':
                break
    finally:
        complete = len(results) == len(BRANCHES) and all(row['verdict'] == row['cleanup'] == 'PASS' for row in results)
        save(args.evidence / 'healthy-summary.json', {'caseId': CASE_ID, 'results': results, 'complete': complete,
            'cleanup': 'PASS' if results and all(row['cleanup'] == 'PASS' for row in results) else 'FAIL'})
    require(complete, 'I03 requires all five real partition/quorum/recovery branches and complete ACK resolution')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'I03 network validation failed: {exc}', file=sys.stderr)
        sys.exit(1)
