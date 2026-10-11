#!/usr/bin/env python3
"""A10: paired Admin option probes on fresh DB/DV and owned C10 orphans."""

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
import uuid

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content, validate_rejection, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .heal_fault import bounded_fault_id
    from .shards import files, map_target, validate_fault, validate_repaired_physical, peer_status_equal
    from .versions import public_version_id
    from .scopes import verify_parts
    from .stop import idle_fleet
    from . import orphan
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content, validate_rejection, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from heal_fault import bounded_fault_id
    from shards import files, map_target, validate_fault, validate_repaired_physical, peer_status_equal
    from versions import public_version_id
    from scopes import verify_parts
    from stop import idle_fleet
    import orphan


CASE_ID = 'heal.admin-option-contract'
BRANCHES = ('recursive-false', 'recursive-true', 'recreate', 'remove-false', 'remove-true',
            'update-parity', 'read-repair', 'selector-valid', 'selector-invalid')
FIXTURES = ('DB', 'DV')
SIZE = 1024**2 + 123
KEYS = ('scope/target.bin', 'scope/child/target.bin', 'scope-extra/control.bin', 'other/control.bin',
        *tuple(f'set-control/{number:02d}.bin' for number in range(12)))
REFERENCE = {'candidateCommit': orphan.CANDIDATE,
    'sourceFiles': {'rustfs/src/admin/handlers/heal.rs': '7e4007c29a8329ced8e48b88111400e79cffd845bd6ddc6243b79f3cd48e8199',
                    'crates/heal/src/heal/channel.rs': '7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0'},
    'support': {'recursive': 'supported; false is exact object, true traverses child keys',
        'recreate': 'supported; request and retained settings must agree',
        'remove': 'supported; restricted to this run C10-proven unreferenced orphan',
        'updateParity': 'supported; deep parity corruption must be physically repaired',
        'readRepair': 'unsupported for Admin; HTTP 400 InvalidArgument before execution',
        'pool/set': 'supported paired selector; exact independently measured physical set'},
    'dangerousEffects': {'remove': 'may delete unreferenced persisted bytes; never applied to a live fixture',
        'recreate': 'may reconstruct missing data; original ACK identities and complete controls stay fixed'},
    'cliCoverageClaimed': False}


def paired_options(branch):
    require(branch in BRANCHES, 'unknown A10 option branch')
    baseline = {**BASE_OPTIONS, 'pool': None, 'set': None}
    selected = dict(baseline)
    if branch == 'recursive-false': baseline['recursive'] = True
    elif branch == 'recursive-true': selected['recursive'] = True
    elif branch == 'recreate': selected['recreate'] = True
    elif branch == 'remove-true': selected['remove'] = True
    elif branch == 'update-parity': selected['updateParity'] = True
    elif branch == 'read-repair': selected['readRepair'] = True
    elif branch.startswith('selector-'):
        baseline['recursive'] = selected['recursive'] = True
        if branch == 'selector-valid': selected.update(pool=0, set=0)
        else:
            baseline.update(pool=0, set=0)
            selected.update(pool=99, set=99)
    changed = {name for name in baseline if baseline[name] != selected[name]}
    expected = {'recursive-false': {'recursive'}, 'recursive-true': {'recursive'}, 'recreate': {'recreate'},
        'remove-false': set(), 'remove-true': {'remove'}, 'update-parity': {'updateParity'},
        'read-repair': {'readRepair'}, 'selector-valid': {'pool', 'set'}, 'selector-invalid': {'pool', 'set'}}[branch]
    require(changed == expected, 'A10 paired request changed unrelated defaults')
    return baseline, selected


class FixtureRecorder(Recorder):
    """Retain the namespace unless all mutation receipts and identities resolve."""
    def __init__(self, *args, fixture='DB', **kwargs):
        super().__init__(*args, **kwargs)
        require(fixture in FIXTURES, 'unknown A10 ACK versioning mode')
        self.fixture = fixture
        self.versions = set()
        self.pending = None

    def call(self, operation, params, body=None, **kwargs):
        self.pending = {'operation': operation, 'params': copy.deepcopy(params)}
        value = super().call(operation, params, body, **kwargs)
        # A successful HTTP status alone is insufficient for a durable identity.
        if operation in {'put_object', 'delete_object'}:
            version = value.get('VersionId')
            require(version in {None, 'null'} or isinstance(version, str) and str(uuid.UUID(version)) == version
                    and uuid.UUID(version).int != 0,
                    'A10 mutation receipt has an invalid version identity')
        if operation == 'put_object':
            require(isinstance(value.get('ETag'), str) and value['ETag'], 'A10 PUT ACK omitted ETag')
            require((self.fixture == 'DB' and version in {None, 'null'})
                    or (self.fixture == 'DV' and version not in {None, 'null'}), 'A10 PUT ACK changed its DB/DV mode')
            identity = params['Key'], public_version_id(version)
            require(identity not in self.versions, 'A10 PUT ACK reused an exact version identity')
            self.versions.add(identity)
        elif operation == 'delete_object':
            if 'VersionId' in params:
                require(version == params['VersionId'] and value.get('DeleteMarker') in {None, False},
                        'A10 exact data-version DELETE ACK changed identity or kind')
            elif self.fixture == 'DV':
                identity = params['Key'], public_version_id(version)
                require(value.get('DeleteMarker') is True and version not in {None, 'null'}
                        and identity not in self.versions, 'A10 DELETE ACK omitted its independent marker identity')
                self.versions.add(identity)
            else:
                require(version in {None, 'null'} and value.get('DeleteMarker') in {None, False},
                        'A10 DB DELETE ACK created a version or marker')
        self.pending = None
        return value


def seed_fixture(recorder, bucket, fixture, keys=KEYS):
    require(fixture in FIXTURES and len(keys) == len(set(keys)), 'invalid A10 fixture')
    recorder.call('create_bucket', {'Bucket': bucket})
    if fixture == 'DV':
        recorder.call('put_bucket_versioning', {'Bucket': bucket, 'VersioningConfiguration': {'Status': 'Enabled'}})
    expected = []
    for key in keys:
        for number in range(3 if fixture == 'DV' else 1):
            body = payload(f'A10/{fixture}/{key}/{number}', SIZE)
            response = recorder.call('put_object', {'Bucket': bucket, 'Key': key,
                'Metadata': {'heal-fixture': 'A10', 'generation': str(number)}, 'Tagging': 'suite=heal&fixture=' + fixture}, body)
            recorder.pending = {'operation': 'put_object', 'bucket': bucket, 'key': key, 'reason': 'fixture identity unresolved'}
            version = public_version_id(response.get('VersionId'))
            require((fixture == 'DB' and version == 'null') or (fixture == 'DV' and version != 'null'),
                    'A10 candidate ACK disagrees with DB/DV versioning')
            require((key, version) not in {(entry['key'], entry['versionId']) for entry in expected},
                    'A10 PUT ACK reused a version')
            expected.append({'key': key, 'versionId': version, 'isLatest': number == (2 if fixture == 'DV' else 0),
                'deleteMarker': False, 'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}})
            recorder.pending = None
    if fixture == 'DV':
        key = keys[-1]
        response = recorder.call('delete_object', {'Bucket': bucket, 'Key': key})
        recorder.pending = {'operation': 'delete_object', 'bucket': bucket, 'key': key, 'reason': 'marker identity unresolved'}
        version = public_version_id(response.get('VersionId'))
        require(response.get('DeleteMarker') is True and version != 'null'
                and version not in {entry['versionId'] for entry in expected}, 'A10 marker ACK is incomplete or duplicate')
        next(entry for entry in expected if entry['key'] == key and entry['isLatest'])['isLatest'] = False
        expected.append({'key': key, 'versionId': version, 'isLatest': True, 'deleteMarker': True})
        recorder.pending = None
    return expected


def map_fixture(snapshot, bucket, expected, topology, *, multi_set=True):
    validate_topology(snapshot, topology, expected_sets=2 if topology == 'H2' and multi_set else 1)
    indexed, mapped = files(snapshot), {}
    for entry in expected:
        scopes = set()
        for node, value in snapshot.items():
            for row in value['formats']:
                if (node, row['disk'], f"{bucket}/{entry['key']}/xl.meta") in indexed:
                    layout = row['value']['xl']
                    scopes.update(number for number, members in enumerate(layout['sets']) if layout['this'] in members)
        require(len(scopes) == 1, 'A10 ACK has an ambiguous physical set')
        set_index = next(iter(scopes))
        identity = (bucket, entry['key'], entry['versionId'])
        if entry['deleteMarker']:
            mapped[identity] = {'bucket': bucket, 'key': entry['key'], 'versionId': entry['versionId'],
                                'poolIndex': 0, 'setIndex': set_index, 'deleteMarker': True}
        else:
            mapped[identity] = {**map_target(snapshot, bucket, entry['key'], 'data', 1, entry['versionId'],
                latest_required=entry['isLatest'], multi_set=topology == 'H2' and multi_set, set_index=set_index), 'deleteMarker': False}
    if topology == 'H2' and multi_set:
        require({target['setIndex'] for target in mapped.values()} == {0, 1}, 'A10 fixture did not populate both actual sets')
    require(len(mapped) == len(expected), 'A10 physical mapping omitted an ACK')
    return mapped


def selection(branch, options, mapped):
    if branch.startswith('recursive-'):
        return {identity for identity in mapped if identity[1].startswith('scope/')} if options['recursive'] else set()
    if branch.startswith('selector-'):
        return {identity for identity, target in mapped.items() if options['pool'] is None
                or (target['poolIndex'], target['setIndex']) == (options['pool'], options['set'])}
    return {identity for identity in mapped if identity[1] == KEYS[0]}


def request_path(branch, bucket):
    if branch.startswith('selector-'): return '/rustfs/admin/v3/heal/'
    key = 'scope/' if branch.startswith('recursive-') else KEYS[0]
    return '/rustfs/admin/v3/heal/' + quote(bucket, safe='') + '/' + quote(key, safe='/')


def run_request(probe, fleet, path, options, active, *, rejected=False):
    raw = fleet.record('A10-admission', probe.admin(path, [], json.dumps(options).encode()))
    if raw.get('httpStatus') == 200 and isinstance(raw.get('response'), dict) and raw['response'].get('clientToken'):
        active.append((path, raw['response']['clientToken']))
    if rejected:
        validate_rejection(raw, 'InvalidArgument')
        return raw
    require(raw.get('httpStatus') == 200 and isinstance(raw.get('response'), dict)
            and active and active[-1][0] == path, 'A10 admission omitted its exact token')
    token = active[-1][1]
    deadline = time.monotonic() + 180
    while True:
        raw = fleet.record('A10-status', probe.admin(path, [('clientToken', token)], b''))
        require(raw.get('httpStatus') == 200 and isinstance(raw.get('response'), dict), 'A10 original status is unavailable')
        status = raw['response']
        if status.get('summary') != 'running': break
        require(time.monotonic() < deadline, 'A10 task exceeded its frozen budget')
        time.sleep(.2)
    require(status.get('settings') == options, 'A10 candidate silently changed or ignored request fields')
    for endpoint in probe.binding['endpoints']:
        peer = Probe(probe.binding, endpoint)
        convergence = time.monotonic() + 30
        while True:
            repeat = fleet.record('A10-retained-status', peer.admin(path, [('clientToken', token)], b''))
            require(repeat.get('httpStatus') == 200 and isinstance(repeat.get('response'), dict), 'A10 retained task status is missing')
            if (repeat['response'].get('settings') == options and peer_status_equal(status, repeat['response'],
                    task=(path, token), peer_task=(path, token))): break
            require(time.monotonic() < convergence, 'A10 original responsibility did not converge on every peer')
            time.sleep(.2)
    active.remove((path, token))
    return status


def validate_outcome(status, mapped, selected, repaired, options):
    outcome = status.get('outcome') or {}
    require(status.get('summary') == 'finished' and status.get('settings') == options
            and status.get('truncated', False) is False and outcome.get('coverage') == 'complete'
            and outcome.get('objectsTruncated') is False, 'A10 lacks complete original task coverage/settings')
    counters = outcome.get('counters') or {}
    if not selected:
        require(not repaired and outcome.get('execution', {}).get('state') == 'completed_with_errors'
                and counters.get('healed') == 0 and counters.get('failed') == 1 and counters.get('unknown') == 0
                and 'Object not found: ' in (status.get('detail') or '') and not outcome.get('objects'),
                'A10 exact absent prefix was repaired, unknown or incorrectly claimed successful')
        return {'exactPrefixNotFound': True, 'selectedACKs': 0, 'repairedACKs': 0}
    require(repaired <= selected <= set(mapped) and outcome.get('execution', {}).get('state') == 'completed',
            'A10 responsibility escaped the physical scope or failed')
    objects, seen, incarnations = outcome.get('objects'), set(), {}
    require(isinstance(objects, list) and len(objects) == len(selected), 'A10 diagnostics omitted a version/marker or added a control')
    for receipt in objects:
        item = receipt.get('identity') or {}
        identity = (item.get('bucket'), item.get('object'), public_version_id(item.get('versionId')))
        require(identity in selected and identity not in seen and item.get('kind') == 'object', 'A10 wrong or duplicate ACK identity')
        target, scope = mapped[identity], (item.get('poolIndex'), item.get('setIndex'))
        if (options['pool'], options['set']) != (None, None):
            require(all(type(value) is int for value in scope) and scope == (options['pool'], options['set'])
                    == (target['poolIndex'], target['setIndex']), 'A10 explicit selector identity was lost or changed')
        elif scope != (None, None):
            require(all(type(value) is int for value in scope) and scope == (target['poolIndex'], target['setIndex']),
                    'A10 unrestricted receipt contradicts its physical set')
        incarnation = item.get('bucketIncarnationId')
        require(isinstance(incarnation, str) and incarnation and incarnations.setdefault(identity[0], incarnation) == incarnation,
                'A10 lost or mixed a bucket incarnation')
        state = 'repaired' if identity in repaired else 'metadata_healthy' if target['deleteMarker'] else 'verified_healthy'
        require(receipt.get('disposition', {}).get('state') == state, 'A10 false repair, unknown or incomplete integrity')
        seen.add(identity)
    expected = {'processed': len(selected), 'healed': len(repaired), 'unchanged': len(selected) - len(repaired),
                'failed': 0, 'unknown': 0, 'skipped': 0, 'attemptFailures': 0, 'overflowed': False}
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in expected.items()),
            'A10 counters disagree with the complete independent ACK/physical manifest')
    return {'selectedACKs': len(selected), 'repairedACKs': len(repaired), 'completeVersionsAndMarkers': True}


def physical_selection(original, after, targets, selected):
    controls = [target for target in targets if (target['bucket'], target['key'], target['versionId']) not in selected]
    repaired = [target for target in targets if target not in controls]
    expected = copy.deepcopy(original)
    actual = files(after)
    for target in controls:
        identity = target['node'], target['disk'], target['relativePath']
        # Bitflipped control bytes are witnessed separately below. The product
        # physical oracle takes the exact fault scene as its control authority.
        if target['faultKind'] == 'missing':
            require(identity not in actual, 'A10 unselected missing control was silently repaired')
        else:
            require(identity in actual and {name: actual[identity][name] for name in ('sha256', 'size')} == target['faultBytes'],
                    'A10 unselected corrupted control was silently repaired or changed')
        for entry in expected[target['node']]['files']:
            if (target['disk'], target['relativePath']) == (entry['disk'], entry['path']):
                if target['faultKind'] == 'missing': expected[target['node']]['files'].remove(entry)
                else: entry.update(sha256=target['faultBytes']['sha256'], size=target['faultBytes']['size'])
                break
    if repaired:
        proof = validate_repaired_physical(expected, after, repaired[0], repair_targets=repaired)
    else:
        require(physical_content(expected) == physical_content(after), 'A10 no-repair/rejected request changed persisted bytes')
        proof = {'allPersistedBytesUnchanged': True}
    return {**proof, 'unselectedFaultsPreserved': len(controls), 'selectedPhysicalRepairs': len(repaired),
            'beforeAnyTargetGETOrBackupRescue': True}


def verify_public(fleet, binding, bucket, expected, baseline, fixture, label):
    for endpoint in binding['endpoints']:
        observed = fleet.record(label, Probe(binding, endpoint).inventory(bucket, unversioned_current_tags=fixture == 'DB'))
        verify_seed(expected, observed)
        compare_inventories(baseline, observed)


def scope_probe(fleet, probe, binding, recorder, bucket, topology, fixture, branch, options, active):
    expected = seed_fixture(recorder, bucket, fixture)
    baseline = fleet.record('A10-full-ACK-baseline', probe.inventory(bucket, unversioned_current_tags=fixture == 'DB'))
    verify_seed(expected, baseline)
    fleet.stop()
    original = fleet.snapshot('A10-original-physical', quiescent=True)
    mapped = map_fixture(original, bucket, expected, topology)
    data = {identity: target for identity, target in mapped.items() if not target['deleteMarker']}
    verify_parts(fleet, data, 'A10-all-original-streaming')
    selected = selection(branch, options, mapped)
    targets = []
    for index, key in enumerate(KEYS[:4]):
        identity = next(identity for identity in data if identity[1] == key)
        target = data[identity]
        role = 'parity' if index % 2 or branch == 'update-parity' and index == 0 else 'data'
        member = next(row for row in target['cohort'] if row['role'] == role)
        targets.append({**target, **member, 'faultKind': 'bitflip' if role == 'parity' else 'missing'})
    if topology == 'H2':
        for number in (0, 1):
            if any(target['setIndex'] == number for target in targets): continue
            target = next(target for identity, target in data.items() if target['setIndex'] == number)
            targets.append({**target, 'faultKind': 'missing'})
    prior, registrations = original, []
    for index, target in enumerate(targets):
        params = {'disk': target['disk'], 'faultId': bounded_fault_id('A10', branch, index)}
        fleet.faults.append((target['node'], params))
        registrations.append((target['node'], params, target))
        raw = fleet.record('A10-fault', fleet.remote(target['node'], 'apply-fault', **params,
            relativePath=target['relativePath'], expectedSha256=target['sha256'], kind=target['faultKind'],
            **({'offset': target['size'] // 2} if target['faultKind'] == 'bitflip' else {})))
        scene = fleet.snapshot('A10-witness-fault', quiescent=True)
        validate_fault(prior, scene, target, raw)
        target['faultBytes'] = raw.get('fault')
        prior = scene
    fleet.start()
    require(physical_content(prior) == physical_content(fleet.snapshot('A10-fault-immediately-before-START')),
            'NOT_OBSERVED: A10 faults changed before original task admission')
    rejected = options['readRepair'] or options['pool'] == 99
    status = run_request(probe, fleet, request_path(branch, bucket), options, active, rejected=rejected)
    idle_fleet(fleet, binding)
    fleet.stop()
    after = fleet.snapshot('A10-product-before-GET-or-rescue', quiescent=True)
    if rejected:
        require(physical_content(prior) == physical_content(after), 'A10 structured rejection mutated physical controls')
        selected = set()
        terminal = {'httpStatus': status['httpStatus'], 'structuredInvalidArgument': True, 'noActiveResponsibility': True}
    else:
        repaired = {(target['bucket'], target['key'], target['versionId']) for target in targets} & selected
        terminal = validate_outcome(status, mapped, selected, repaired, options)
        if not selected:
            require('Object not found: ' + bucket + '/scope/' in status.get('detail', ''), 'A10 exact prefix failure names another key')
    physical = physical_selection(original, after, targets, selected)
    verify_parts(fleet, {identity: target for identity, target in data.items() if identity in selected},
                 'A10-selected-physical-streaming') if selected else None
    # Restore only excluded controls after the product result has been frozen.
    for node, params, target in registrations:
        if (target['bucket'], target['key'], target['versionId']) not in selected:
            receipt = fleet.record('A10-excluded-control-rescue', fleet.remote(node, 'recover-fault', **params))
            require(receipt.get('physicalRepairPassAllowed') is False, 'A10 backup rescue claimed product repair')
            fleet.faults.remove((node, params))
    restored = fleet.snapshot('A10-all-physical-after-control-rescue', quiescent=True)
    validate_repaired_physical(original, restored, targets[0], repair_targets=targets)
    verify_parts(fleet, data, 'A10-all-final-physical-streaming')
    fleet.start()
    verify_public(fleet, binding, bucket, expected, baseline, fixture, 'A10-all-peer-final-S3')
    fleet.stop()
    require(physical_content(restored) == physical_content(fleet.snapshot('A10-physical-after-S3', quiescent=True)),
            'A10 GET changed the already-proven physical result')
    return {'terminal': terminal, 'physical': physical, 'fixture': fixture, 'originalACKs': len(expected)}


def orphan_probe(fleet, probe, binding, recorder, bucket, topology, fixture, options, active):
    # A10 reuses C10's complete descriptor archives and post-DELETE authority,
    # while keeping the same data-only replay in both remove option phases.
    keys = (orphan.KEY, 'control.bin', 'marker-control.bin')
    expected = seed_fixture(recorder, bucket, fixture, keys)
    doomed = next(entry for entry in expected if entry['key'] == orphan.KEY and entry['isLatest'])
    if fixture == 'DV':
        # Remove every orphan-key generation, with separate exact durable ACKs.
        doomed_entries = [entry for entry in expected if entry['key'] == orphan.KEY]
    else: doomed_entries = [doomed]
    fleet.record('A10-orphan-original-S3', verify_seed(expected, probe.inventory(bucket, unversioned_current_tags=fixture == 'DB')))
    fleet.stop()
    original = fleet.snapshot('A10-orphan-original-all-members', quiescent=True)
    mapped = map_fixture(original, bucket, expected, topology, multi_set=False)
    target = mapped[(bucket, orphan.KEY, doomed['versionId'])]
    orphan.capture(fleet, original, bucket, 'old')
    fleet.start()
    for entry in doomed_entries:
        params = {'Bucket': bucket, 'Key': orphan.KEY}
        if fixture == 'DV': params['VersionId'] = entry['versionId']
        response = recorder.call('delete_object', params)
        recorder.pending = {'operation': 'delete_object', 'params': params, 'reason': 'ACK identity not yet resolved'}
        require(response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 204
                and response.get('DeleteMarker') in {None, False}
                and public_version_id(response.get('VersionId')) == entry['versionId'], 'A10 orphan DELETE ACK changed exact identity')
        recorder.pending = None
        expected.remove(entry)
    baseline = fleet.record('A10-last-DELETE-S3-authority', probe.inventory(bucket, unversioned_current_tags=fixture == 'DB'))
    verify_seed(expected, baseline)
    fleet.stop()
    authority = fleet.snapshot('A10-last-DELETE-all-physical-authority', quiescent=True)
    require(all(not values for values in orphan.references(authority, bucket, orphan.KEY).values())
            and not any(identity[2].startswith(bucket + '/' + orphan.KEY + '/') for identity in files(authority)),
            'A10 remove target has a surviving version/marker/data reference')
    fleet.a10_orphan_authority = authority
    fleet.a10_orphan_restored = False
    orphan.arm_all(fleet, authority, bucket, 'authority')
    try:
        fleet.record('A10-owned-unreferenced-data-only-replay', fleet.remote(target['node'], 'orphan-install',
            disk=target['disk'], bucket=bucket, key=orphan.KEY, dataDir=target['dataDir'], mode='data'))
        before = fleet.snapshot('A10-orphan-fault-before-GET', quiescent=True)
        require({identity[:2] for identity in files(before) if identity[2].startswith(bucket + '/' + orphan.KEY + '/')}
                == {(target['node'], target['disk'])}, 'A10 replay escaped its one original member')
        require(all(not values for values in orphan.references(before, bucket, orphan.KEY).values()),
                'A10 controlled orphan still has a live metadata reference')
        fleet.start()
        require(physical_content(before) == physical_content(fleet.snapshot('A10-orphan-immediately-before-START')),
                'NOT_OBSERVED: A10 orphan changed before admission')
        status = run_request(probe, fleet, '/rustfs/admin/v3/heal/' + bucket + '/' + orphan.KEY, options, active)
        idle_fleet(fleet, binding)
        fleet.stop()
        after = fleet.snapshot('A10-orphan-product-before-GET-or-rescue', quiescent=True)
        branch = 'a10-remove-true' if options['remove'] else 'remove-false'
        physical = orphan.physical_safety(before, after, authority, bucket, expected, branch,
            doomed['versionId'], target['dataDir'], (target['node'], target['disk']))
        terminal = orphan.terminal_safety(status, branch, bucket, options)
        fleet.start()
        verify_public(fleet, binding, bucket, expected, baseline, fixture, 'A10-orphan-product-before-rescue-S3')
        for endpoint in binding['endpoints']:
            peer = Probe(binding, endpoint)
            for entry in doomed_entries:
                params = {'Bucket': bucket, 'Key': orphan.KEY}
                if fixture == 'DV': params['VersionId'] = entry['versionId']
                observed = fleet.record('A10-deleted-orphan-not-resurrected', peer.s3('get_object', params))
                response = observed.get('response') or {}
                require(not observed['acknowledged'] and not observed['resultUnknown'] and 'payload' not in observed
                        and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404
                        and response.get('Error', {}).get('Code') in {'NoSuchKey', 'NoSuchVersion'}, 'A10 deleted ACK resurrected')
    finally:
        # These archives are the last DELETE authority, never the old PUT tree.
        fleet.stop()
        orphan.recover_all(fleet)
        restored = fleet.snapshot('A10-orphan-last-DELETE-restored', quiescent=True)
        require(physical_content(authority) == physical_content(restored), 'A10 orphan rescue changed last DELETE authority')
        fleet.a10_orphan_restored = True
    mapping = map_fixture(restored, bucket, expected, topology, multi_set=False)
    verify_parts(fleet, {identity: target for identity, target in mapping.items() if not target['deleteMarker']},
                 'A10-orphan-all-live-controls-streaming')
    fleet.start()
    verify_public(fleet, binding, bucket, expected, baseline, fixture, 'A10-orphan-all-peer-final-S3')
    return {'fixture': fixture, 'terminal': terminal, 'physical': physical, 'allOriginalPostDeleteAuthoritiesRestored': True,
            'C10UnreferencedOrphanProof': True, 'deletedACKs': len(doomed_entries), 'rescueIsProductRepair': False}


def run_instance(args, topology, nodes, branch, fixture, phase):
    directory = args.evidence / topology / branch / fixture / phase
    directory.mkdir(parents=True)
    candidate = json.loads(args.candidate.read_text())
    require(candidate['source_sha'] == REFERENCE['candidateCommit'], 'A10 option contract is not reviewed for this candidate')
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    is_orphan = branch.startswith('remove-')
    runtime = 'orphan-preserve' if is_orphan else 'scope-isolation' if topology == 'H2' else 'manual'
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
    baseline_options, selected_options = paired_options(branch)
    options = baseline_options if phase == 'baseline' else selected_options
    fleet.record('A10-source-bound-contract', {**REFERENCE, 'branch': branch, 'fixture': fixture, 'phase': phase,
        'baselineOptions': baseline_options, 'selectedOptions': selected_options,
        'singleLogicalFieldDelta': 'pool/set selector pair' if branch.startswith('selector-') else branch})
    fleet.record('binding', binding)
    fleet.record('inspector-build', verify_inspector(args.tool_directory, candidate))
    probe, active, recorder = Probe(binding), [], None
    result = {'topology': topology, 'branch': branch, 'fixture': fixture, 'phase': phase, 'options': options,
              'verdict': 'RUNNING', 'cleanup': 'PENDING'}
    seed_complete = False
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        recorder = FixtureRecorder(probe, directory / 'fixture-ACKs', fixture=fixture)
        # Each phase owns a fresh namespace; failed/incomplete seeding cannot
        # be hidden by a later default repair or by final namespace removal.
        if is_orphan:
            proof = orphan_probe(fleet, probe, binding, recorder, bucket, topology, fixture, options, active)
        else:
            proof = scope_probe(fleet, probe, binding, recorder, bucket, topology, fixture, branch, options, active)
        seed_complete = True
        result.update(verdict='PASS', proof=proof)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict='FAIL', failure={'type': type(exc).__name__, 'message': str(exc)})
    finally:
        for path, token in active:
            try: fleet.record('A10-stop-unresolved-original-token', probe.admin(path, [('clientToken', token), ('forceStop', 'true')], b''))
            except Exception as exc: fleet.record('A10-stop-error', {'error': str(exc)})
        unresolved = recorder is not None and recorder.pending is not None
        incomplete_restore = hasattr(fleet, 'a10_orphan_authority') and not fleet.a10_orphan_restored
        if unresolved or incomplete_restore:
            try: fleet.stop()
            except Exception as exc: fleet.record('A10-retain-stop-error', {'error': str(exc)})
            fleet.record('A10-unresolved-authority-retained', {'state': 'DIRTY',
                'request': recorder.pending if recorder is not None else None, 'namespacesRetained': True,
                'originalPostDeleteRestorationIncomplete': incomplete_restore})
            clean = False
        else:
            clean = fleet.cleanup()
        result.update(cleanup='PASS' if clean else 'FAIL', fullProbeCompleted=seed_complete)
        fleet.record('instance-result', result)
    return result


def complete_results(results):
    wanted = {(topology, branch, fixture, phase) for topology in ('H1', 'H2') for branch in BRANCHES
              for fixture in FIXTURES for phase in ('baseline', 'selected')}
    actual = {(row['topology'], row['branch'], row['fixture'], row['phase']) for row in results}
    require(len(actual) == len(results) and actual <= wanted, 'A10 duplicate or foreign parameter instance')
    return actual == wanted and all(row['verdict'] == row['cleanup'] == 'PASS' and row.get('fullProbeCompleted') is True
                                   for row in results)


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f'A10 interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('candidate', 'rc-bin', 'evidence', 'tool-directory'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--case-id', choices=[CASE_ID], required=True)
    parser.add_argument('--testing-commit', required=True)
    parser.add_argument('--binary-sha', required=True)
    args = parser.parse_args()
    require(re.fullmatch(r'[0-9a-f]{64}', args.binary_sha) and re.fullmatch(r'[1-9][0-9]*', os.environ.get('GITHUB_RUN_ID', '')),
            'A10 requires immutable workflow/binary identities')
    require(select(CASE_ID)[0]['variants'] == list(BRANCHES), 'A10 omitted a source option')
    nodes = os.environ.get('RUSTFS_NODES', 'vm000 vm001 vm002 vm003').split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in nodes),
            'A10 requires four distinct current Heal nodes')
    results = []
    try:
        for topology in ('H1', 'H2'):
            for branch in BRANCHES:
                for fixture in FIXTURES:
                    for phase in ('baseline', 'selected'):
                        row = run_instance(args, topology, nodes[:1] if topology == 'H1' else nodes[:4], branch, fixture, phase)
                        results.append(row)
                        if row['cleanup'] != 'PASS': return
    finally:
        complete = complete_results(results)
        save(args.evidence / 'healthy-summary.json', {'caseId': CASE_ID, 'results': results, 'complete': complete,
            'cleanup': 'PASS' if results and all(row['cleanup'] == 'PASS' for row in results) else 'FAIL'})
        require(complete, 'A10 requires every H1/H2 DB/DV baseline/selected option instance and complete cleanup')


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        print(f'A10 option validation failed: {exc}', file=sys.stderr)
        sys.exit(1)
