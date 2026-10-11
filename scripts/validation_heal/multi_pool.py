#!/usr/bin/env python3
"""H06: real two-pool/two-set selection with unselected faults preserved until proof."""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, RecordedCallError, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .multi_pool_layout import validate as validate_layout, volumes
    from .shards import files, map_target, run_task, validate_fault, validate_repaired_physical
    from .scopes import physical_selection, verify_parts, REFERENCE as SELECTOR_REFERENCE
    from .stop import idle_fleet
    from .versions import public_version_id
    from .write_quorum import endpoint_refused, expected_reads
    from ..validation_pool.pool_status import validate_pool_inventory
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, RecordedCallError, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content
    from inspector import verify as verify_inspector
    from matrix import select
    from multi_pool_layout import validate as validate_layout, volumes
    from shards import files, map_target, run_task, validate_fault, validate_repaired_physical
    from scopes import physical_selection, verify_parts, REFERENCE as SELECTOR_REFERENCE
    from stop import idle_fleet
    from versions import public_version_id
    from write_quorum import endpoint_refused, expected_reads
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'validation_pool'))
    from pool_status import validate_pool_inventory


CASE_ID = 'heal.multi-pool-target-isolation'
BRANCHES = ('selected-pool', 'selected-set', 'unselected-control-fault')
SIZE = 1024**2 + 73
REFERENCE = {'candidateCommit': SELECTOR_REFERENCE['candidateCommit'], 'selectors': SELECTOR_REFERENCE['sourceFiles'],
    'layoutSourceFiles': ['crates/ecstore/src/layout/disks_layout.rs', 'crates/utils/src/string.rs'],
    'topology': 'H5: two endpoint-expansion arguments, eight original independent mounts per pool, two four-peer sets per pool',
    'storageClass': 'explicit EC:2; every actual object must decode as 2+2',
    'boundedFixture': {'DBKeys': 48, 'DVKeys': 24, 'versionsPerDVKey': 3, 'bytesPerVersion': SIZE},
    'placement': 'observe actual full-set XL2/version authority; no assumed pool hash or internal PUT selector',
    'scope': 'pool selector must match admitted pool; an unrestricted set may be null in the raw receipt, with independent exact physical binding',
    'control': 'unselected defects stay missing before any target GET or rescue; repair them in independent tasks only after proof'}


def fixture_call(recorder, ledger, operation, params, body=None):
    row = {'operation': operation, 'params': params, 'resolved': False}
    if body is not None:
        row['payload'] = {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
    ledger.append(row)  # Register before the durable recorder or the network can fail.
    try:
        response = recorder.call(operation, params, body)
    except RecordedCallError as exc:
        row['observation'] = exc.observation
        raise
    row['observation'] = {'acknowledged': True, 'resultUnknown': False, 'response': response}
    if operation == 'put_object':
        version_id = response.get('VersionId', 'null')
        require((params['Bucket'].endswith('-dv') and version_id not in {None, 'null'})
                or (not params['Bucket'].endswith('-dv') and version_id == 'null'),
                'H06 versioning mode differs from its exact PUT ACK')
        row['versionId'] = version_id
    row['resolved'] = True
    return response


def resolve_fixture_inventory(rows, inventory):
    """Resolve an interrupted seed against exact prior ACKs, never retry it."""
    entries = inventory['entries']
    require(all(not entry['deleteMarker'] for entry in entries), 'seed resolution contains unexpected markers')
    consumed, resolutions = set(), []
    for row in rows:
        require(row['operation'] == 'put_object', 'uncertain bucket mutation needs independent resolution; retain namespace')
        params = row['params']
        matches = [entry for entry in entries if entry['key'] == params['Key'] and entry['payload'] == row['payload']
                   and entry['properties'].get('ContentType') == params['ContentType']
                   and entry['properties'].get('Metadata') == params['Metadata']
                   and entry['tags'] == [{'Key': 'fixture', 'Value': 'H06'}, {'Key': 'suite', 'Value': 'heal'}]]
        require(len(matches) <= 1, 'seed request has multiple or ambiguous committed versions')
        observed = row.get('observation', {'acknowledged': False, 'resultUnknown': True})
        if matches:
            entry = matches[0]
            version = entry['versionId']
            require((params['Bucket'].endswith('-dv') and version not in {None, 'null'})
                    or (not params['Bucket'].endswith('-dv') and version == 'null'), 'resolved seed versioning drift')
            if observed.get('acknowledged'):
                require(version == observed['response'].get('VersionId', 'null'), 'resolved seed replaced its PUT ACK version')
            else:
                require(observed.get('resultUnknown') is True, 'refused fixture PUT committed')
            identity = entry['key'], version
            require(identity not in consumed, 'seed resolution reused one version for different requests')
            consumed.add(identity)
            resolutions.append({'key': params['Key'], 'state': 'COMMITTED', 'versionId': version})
        else:
            require(not observed.get('acknowledged') and not row['resolved'], 'a fixture PUT ACK cannot be redeemed')
            resolutions.append({'key': params['Key'], 'state': 'ABSENT'})
    require(consumed == {(entry['key'], entry['versionId']) for entry in entries},
            'seed resolution omitted foreign, altered or unexplained versions')
    latest = {row['key']: row['versionId'] for row in resolutions if row['state'] == 'COMMITTED'}
    require(all(entry['isLatest'] is (latest[entry['key']] == entry['versionId']) for entry in entries),
            'resolved fixture latest-version identity drifted')
    return resolutions


def resolve_pending_fixture(fleet, binding, ledger):
    pending = [row for row in ledger if not row['resolved']]
    require(all(row['operation'] == 'put_object' for row in pending),
            'uncertain bucket mutation needs independent resolution; retain namespace')
    for bucket in sorted({row['params']['Bucket'] for row in pending}):
        rows = [row for row in ledger if row['operation'] == 'put_object' and row['params']['Bucket'] == bucket]
        inventories = [fleet.record('failed-seed-all-peer-resolution', Probe(binding, endpoint).inventory(bucket,
            unversioned_current_tags=not bucket.endswith('-dv'))) for endpoint in binding['endpoints']]
        proofs = [resolve_fixture_inventory(rows, inventory) for inventory in inventories]
        require(all(proof == proofs[0] for proof in proofs), 'seed resolution disagrees across original peers')
        for inventory in inventories[1:]:
            compare_inventories(inventories[0], inventory)
        for row in pending:
            if row['params']['Bucket'] == bucket:
                row['resolved'] = True
                row['resolution'] = proofs[0]
    return {'allOriginalRequestsResolved': True, 'automaticRetries': 0}


def seed_fixtures(probe, directory, buckets, ledger):
    recorder = Recorder(probe, directory / 'fixture')
    expected = {}
    for bucket, count, versions in ((buckets[0], 48, 1), (buckets[1], 24, 3)):
        fixture_call(recorder, ledger, 'create_bucket', {'Bucket': bucket})
        if versions == 3:
            fixture_call(recorder, ledger, 'put_bucket_versioning', {'Bucket': bucket, 'VersioningConfiguration': {'Status': 'Enabled'}})
        expected[bucket] = []
        for index in range(count):
            key = f'objects/{index:04d}.bin'
            for version in range(versions):
                body = payload(f'{bucket}/{key}/{version}', SIZE)
                response = fixture_call(recorder, ledger, 'put_object', {'Bucket': bucket, 'Key': key,
                    'ContentType': 'application/octet-stream', 'Metadata': {'heal-fixture': 'H06', 'nonce': f'{index}-{version}'},
                    'Tagging': 'suite=heal&fixture=H06'}, body)
                version_id = response.get('VersionId', 'null')
                require((versions == 1 and version_id == 'null') or (versions == 3 and version_id not in {None, 'null'}),
                        'H06 versioning mode differs from its exact PUT ACK')
                expected[bucket].append({'key': key, 'versionId': version_id, 'deleteMarker': False,
                    'isLatest': version == versions - 1, 'payload': {'size': SIZE, 'sha256': hashlib.sha256(body).hexdigest()}})
    return expected


def witness_peer_loss(fleet, binding, expected, mapped):
    peer = fleet.nodes[-1]
    before = fleet.record('H5-peer-process-before', fleet.remote(peer, 'process'))
    require(before['pid'] > 1 and before['binarySha256'] == binding['candidateBinarySha256'], 'H5 peer lacks candidate process identity')
    stopped = fleet.record('H5-one-owned-peer-stop', fleet.remote(peer, 'stop'))
    require(stopped['quiescent'] is True and stopped['service']['MainPID'] == '0', 'H5 peer stop was not witnessed')
    fleet.record('H5-stopped-peer-endpoint-refusal', endpoint_refused(binding, len(fleet.nodes) - 1))
    for identity, target in mapped.items():
        survivors = [member for member in target['cohort'] if member['node'] != peer]
        require(len(survivors) == 3 and len({member['node'] for member in survivors}) == 3,
                'H5 peer loss did not leave exactly three independent slots')
        bucket, key, version = identity
        manifest = next(entry for entry in expected[bucket] if entry['key'] == key and entry['versionId'] == version)
        for endpoint in list(binding['endpoints'])[:-1]:
            read = fleet.record('H5-survivor-exact-version-read', Probe(binding, endpoint).s3('get_object',
                {'Bucket': bucket, 'Key': key, 'VersionId': version}))
            require(read.get('acknowledged') is True and read.get('resultUnknown') is False
                    and read.get('payload') == manifest['payload']
                    and read['response'].get('VersionId', 'null') == version, 'H5 surviving peer lost acknowledged version/bytes')
    fleet.record('H5-original-peer-return', fleet.remote(peer, 'start'))
    fleet.wait_ready()
    after = fleet.record('H5-peer-process-after', fleet.remote(peer, 'process'))
    require(after['pid'] > 1 and after['bootId'] == before['bootId'] and after['unit'] == before['unit']
            and after['binarySha256'] == before['binarySha256']
            and (after['pid'], after['startTicks']) != (before['pid'], before['startTicks']), 'H5 peer return changed candidate/unit or did not restart')
    return {'oneOwnedPeerStopped': True, 'exactVersions': len(mapped), 'survivingReadEndpoints': 3,
            'remainingSlotsPerActualSet': 3, 'fullNodePowerLossClaimed': False}


def map_versions(snapshot, expected):
    layout = validate_layout(snapshot)
    indexed, mapped = files(snapshot), {}
    for bucket, entries in expected.items():
        for entry in entries:
            scopes = set()
            for node, value in snapshot.items():
                for fmt in value['formats']:
                    row = indexed.get((node, fmt['disk'], f"{bucket}/{entry['key']}/xl.meta"), {})
                    versions = [version for version in row.get('metadata', {}).get('versions', [])
                                if public_version_id(version.get('versionId')) == entry['versionId']]
                    require(len(versions) <= 1, 'H06 metadata duplicates an acknowledged version')
                    if not versions:
                        continue
                    groups = fmt['value']['xl']['sets']
                    set_ids = [index for index, group in enumerate(groups) if fmt['value']['xl']['this'] in group]
                    require(len(set_ids) == 1, 'H06 physical slot belongs to ambiguous sets')
                    scopes.add((fmt['disk'] // 2, set_ids[0]))
            require(len(scopes) == 1, 'acknowledged version lacks a unique actual pool/set authority')
            pool_index, set_index = next(iter(scopes))
            target = map_target(snapshot, bucket, entry['key'], 'data', 1, entry['versionId'],
                latest_required=False, pool_index=pool_index, set_index=set_index)
            require(target['logical']['erasure']['data_blocks'] == target['logical']['erasure']['parity_blocks'] == 2
                    and len(target['cohort']) == len({row['node'] for row in target['cohort']}) == 4,
                    'H06 object geometry is not actual cross-node EC 2+2')
            mapped[(bucket, entry['key'], entry['versionId'])] = target
    for bucket in expected:
        require({(target['poolIndex'], target['setIndex']) for identity, target in mapped.items() if identity[0] == bucket}
                == {(pool, group) for pool in range(2) for group in range(2)},
                'NOT_OBSERVED: bounded DB/DV writes did not populate every actual pool/set')
    by_key = {}
    for (bucket, key, _), target in mapped.items():
        by_key.setdefault((bucket, key), set()).add((target['poolIndex'], target['setIndex']))
    require(all(len(scopes) == 1 for scopes in by_key.values()), 'fixture key versions span different pools/sets; require a new reviewed scope fixture')
    return layout, mapped


def selection(mapped, branch):
    require(branch in BRANCHES, 'unregistered H06 branch')
    pool, group = (0, 0) if branch == 'unselected-control-fault' else (1, None if branch == 'selected-pool' else 1)
    selected = {identity for identity, target in mapped.items() if target['poolIndex'] == pool
                and (group is None or target['setIndex'] == group)}
    require(selected and set(mapped) - selected, 'H06 selection has no target or unselected controls')
    scopes = [(pool, index) for index in range(2)] if group is None else [(pool, group)]
    scopes += [(1 - pool, 0)]
    if group is not None:
        scopes += [(pool, 1 - group)]
    targets, keys = [], set()
    for wanted_pool, wanted_group in scopes:
        for versioned in (False, True):
            candidates = sorted((identity, target) for identity, target in mapped.items()
                                if (target['poolIndex'], target['setIndex']) == (wanted_pool, wanted_group)
                                and identity[:2] not in keys and identity[0].endswith('-dv') is versioned
                                and (not versioned or target['logical']['isLatest'] is False))
            require(candidates, 'H06 scope lacks a distinct DB or historical DV target/control key')
            identity, target = candidates[0]
            targets.append(target)
            keys.add(identity[:2])
    return {'recursive': True, 'pool': pool, 'set': group}, selected, targets


def responsibility(current, mapped, selected, repaired, options):
    outcome = current.get('outcome') or {}
    require(current.get('summary') == 'finished' and current.get('truncated', False) is False
            and outcome.get('execution', {}).get('state') == 'completed' and outcome.get('coverage') == 'complete'
            and outcome.get('objectsTruncated') is False and current.get('settings') == options,
            'H06 task lost exact selectors, complete execution or diagnostics')
    objects = outcome.get('objects', [])
    require(len(objects) == len(selected) and repaired and repaired <= selected, 'H06 lacks exact selected version responsibility')
    seen, incarnations = set(), {}
    for row in objects:
        item = row.get('identity', {})
        identity = item.get('bucket'), item.get('object'), public_version_id(item.get('versionId'))
        require(identity in selected and identity not in seen and item.get('kind') == 'object',
                'H06 outcome includes foreign, duplicate or unselected responsibility')
        target = mapped[identity]
        require(type(item.get('poolIndex')) is int and item['poolIndex'] == options['pool'] == target['poolIndex'],
                'H06 pool selector/receipt disagrees with its actual drive group')
        reported_set = item.get('setIndex')
        require(reported_set is None and options['set'] is None or type(reported_set) is int
                and reported_set == target['setIndex'] and (options['set'] is None or reported_set == options['set']),
                'H06 set selector/receipt contradicts the actual version authority')
        incarnation = item.get('bucketIncarnationId')
        require(isinstance(incarnation, str) and incarnation and incarnations.setdefault(identity[0], incarnation) == incarnation,
                'H06 version receipts lack or mix bucket incarnations')
        require(row.get('disposition', {}).get('state') == ('repaired' if identity in repaired else 'verified_healthy'),
                'H06 selected version is unknown, falsely healthy or falsely repaired')
        seen.add(identity)
    counts = {'processed': len(selected), 'healed': len(repaired), 'unchanged': len(selected) - len(repaired),
              'skipped': 0, 'failed': 0, 'unknown': 0, 'attemptFailures': 0, 'overflowed': False}
    require(seen == selected and all(type(outcome.get('counters', {}).get(name)) is type(value)
            and outcome['counters'][name] == value for name, value in counts.items()),
            'H06 canonical counters do not cover every exact selected ACK version')
    return {'exactSelectedVersionResponsibility': True, 'repaired': len(repaired), 'versions': len(selected),
            'rawUnrestrictedSetNotRewritten': True, 'physicalScopeBindings': [
                {'bucket': identity[0], 'key': identity[1], 'versionId': identity[2],
                 'poolIndex': mapped[identity]['poolIndex'], 'setIndex': mapped[identity]['setIndex'],
                 'diskUUIDs': [row['diskUUID'] for row in mapped[identity]['cohort']]} for identity in sorted(selected)]}


def pool_status(probe, fleet, endpoint_expressions, label):
    observed = fleet.record(label, probe.admin('/rustfs/admin/v3/pools/list', [], b'', method='GET'))
    require(observed.get('httpStatus') == 200, 'H06 pool capacity/list observation failed')
    values = validate_pool_inventory(observed.get('response'), 2)
    require([row['id'] for row in values] == [0, 1]
            and [row['cmdline'] for row in values] == endpoint_expressions and all(row['totalSize'] > 0 for row in values),
            'H06 public pool identities/capacity differ from its original endpoint arguments')
    for row in values:
        specific = fleet.record(label + '-selector', probe.admin('/rustfs/admin/v3/pools/status',
            [('pool', str(row['id'])), ('by-id', 'true')], b'', method='GET'))
        require(specific.get('httpStatus') == 200, 'H06 selected public pool status failed')
        selected = validate_pool_inventory(specific.get('response'), 1)[0]
        require(all(selected[name] == row[name] for name in ('id', 'cmdline', 'status', 'totalSize')),
                'H06 selected pool identity/state/total capacity disagrees with its pool list')
    return [{name: row[name] for name in ('id', 'cmdline', 'status', 'totalSize')} for row in values]


def quota_status(probe, fleet, buckets, label):
    result = {}
    for bucket in buckets:
        value = fleet.record(label, probe.admin('/rustfs/admin/v3/quota/' + bucket, [], b'', method='GET'))
        response = value.get('response') or {}
        require(value.get('httpStatus') == 200 and response.get('bucket') == bucket
                and response.get('quota') is None and response.get('quota_type') == 'HARD',
                'H06 changed the owned bucket quota or queried a different logical bucket')
        result[bucket] = {name: response[name] for name in ('bucket', 'quota', 'quota_type')}
    return result


def run_branch(args, nodes, branch):
    directory = args.evidence / 'H5' / branch
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-h5"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile='multi-pool')
    candidate = json.loads(args.candidate.read_text())
    require(candidate['source_sha'] == REFERENCE['candidateCommit'], 'H06 selector/placement semantics are not reviewed for this candidate')
    bucket = 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    buckets = [bucket, bucket + '-dv']
    fleet.snapshot_buckets = buckets
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {'schemaVersion': 1, 'runId': run_id, 'jumpHostOnly': True, 'exclusiveHealService': True,
        'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), 'bucketPrefix': bucket,
        'endpoints': {f'node{index}': f'http://rustfs-node{index}:9000' for index in range(1, 5)}, 'defaultEndpoint': 'node1',
        'candidateCommit': candidate['source_sha'], 'harnessCommit': args.testing_commit,
        'candidatePackageSha256': candidate['package_sha256'], 'candidateBinarySha256': args.binary_sha,
        'rcBinarySha256': hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), 'maxInventoryObjects': 256,
        'maxInventoryBytes': 512 * 1024**2}
    fleet.record('binding', binding)
    fleet.record('multi-pool-source-contract', REFERENCE)
    fleet.record('inspector-build', verify_inspector(args.tool_directory, candidate))
    probe, active, ledger = Probe(binding), [], []
    result = {'topology': 'H5', 'branch': branch, 'verdict': 'RUNNING', 'cleanup': 'PENDING'}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        expected = seed_fixtures(probe, directory, buckets, ledger)
        baseline = {name: fleet.record('full-baseline-S3', probe.inventory(name,
            unversioned_current_tags=name == bucket)) for name in buckets}
        for name in buckets:
            fleet.record('fixture-ACK-oracle', {'bucket': name, **verify_seed(expected[name], baseline[name])})
        before_capacity = pool_status(probe, fleet, volumes(run_id), 'baseline-capacity-pool-binding')
        before_quota = quota_status(probe, fleet, buckets, 'baseline-logical-quota-binding')
        fleet.stop()
        original = fleet.snapshot('all-pool-physical-baseline', quiescent=True)
        layout, mapped = map_versions(original, expected)
        fleet.record('all-pool-set-version-authorities', {'layout': layout, 'versions': [
            {'bucket': identity[0], 'key': identity[1], 'versionId': identity[2],
             'poolIndex': target['poolIndex'], 'setIndex': target['setIndex'], 'cohort': target['cohort']}
            for identity, target in mapped.items()]})
        fleet.start()
        result['nodeRedundancy'] = witness_peer_loss(fleet, binding, expected, mapped)
        fleet.stop()
        require(physical_content(original) == physical_content(fleet.snapshot('after-H5-peer-loss-before-fault', quiescent=True)),
                'healthy H5 peer loss modified the physical baseline')
        deltas, selected, targets = selection(mapped, branch)
        options = {**BASE_OPTIONS, **deltas}
        result['selectors'] = options
        fleet.record('selected-and-unselected-fault-plan', {'options': options, 'targets': targets,
            'selectedVersions': [list(identity) for identity in sorted(selected)]})
        current = original
        registrations = {}
        for index, target in enumerate(targets):
            params = {'disk': target['disk'], 'faultId': f'H06-part-{index}'}
            registration = target['node'], params
            fleet.faults.append(registration)
            registrations[(target['bucket'], target['key'], target['versionId'])] = registration
            receipt = fleet.record('precise-pool-set-part-fault', fleet.remote(target['node'], 'apply-fault', **params,
                relativePath=target['relativePath'], kind='missing', expectedSha256=target['sha256']))
            after = fleet.snapshot('physical-exact-fault', quiescent=True)
            validate_fault(current, after, target, receipt)
            current = after
        readonly = None
        if branch == 'unselected-control-fault':
            readonly = next(target for target in targets if target['poolIndex'] != options['pool'])
            fleet.quorum_gates.add(readonly['node'])
            fleet.record('F4-unselected-private-readonly-intent', fleet.remote(readonly['node'], 'quorum-gate', disks=[readonly['disk']]))
        repaired = {(target['bucket'], target['key'], target['versionId']) for target in targets
                    if (target['bucket'], target['key'], target['versionId']) in selected}
        fleet.start()
        def witness_readonly(label):
            relative = f"{readonly['bucket']}/{readonly['key']}/xl.meta"
            raw = fleet.record(label, fleet.remote(readonly['node'], 'quorum-gate-probe', relativePath=relative,
                expectedReads=expected_reads(current, readonly['node'], relative)))
            require({row['disk'] for row in raw['disks'] if not row['writable']} == {readonly['disk']},
                    'F4 unselected control did not witness the exact service-private read-only root')
            return {'poolIndex': readonly['poolIndex'], 'setIndex': readonly['setIndex'], 'diskUUID': readonly['diskUUID'], **raw}
        if readonly:
            result['F4BeforeSelectedHeal'] = witness_readonly('F4-service-namespace-before-selected-Heal')
        observed = run_task(probe, fleet, {'bucket': '', 'key': ''}, 2, active,
            option_overrides=options, status_endpoints=tuple(binding['endpoints'])[1:])
        result['selectedResponsibility'] = responsibility(observed, mapped, selected, repaired, options)
        if readonly:
            result['F4AfterSelectedHeal'] = witness_readonly('F4-service-namespace-after-selected-Heal')
        idle_fleet(fleet, binding)
        fleet.stop()
        isolated = fleet.snapshot('selected-repaired-unselected-still-missing-before-GET', quiescent=True)
        selected_keys = {identity[:2] for identity in selected}
        result['physicalIsolation'] = physical_selection(original, isolated, targets, selected_keys)
        for identity in repaired:
            fleet.faults.remove(registrations[identity])
        if readonly:
            fleet.record('F4-release-after-unselected-physical-proof', fleet.remote(readonly['node'], 'quorum-gate', mode='clear'))
            fleet.quorum_gates.remove(readonly['node'])
        fleet.start()
        require(pool_status(probe, fleet, volumes(run_id), 'selected-capacity-pool-binding') == before_capacity
                and quota_status(probe, fleet, buckets, 'selected-logical-quota-binding') == before_quota,
                'selected Heal changed public pool capacity identity or logical quota policy')
        for target in targets:
            identity = target['bucket'], target['key'], target['versionId']
            if identity in repaired:
                continue
            local_scope = {key for key in mapped if key[:2] == identity[:2]}
            control_options = {**BASE_OPTIONS, 'pool': target['poolIndex'], 'set': target['setIndex']}
            control = run_task(probe, fleet, {'bucket': target['bucket'], 'key': target['key']}, 2, active,
                option_overrides=control_options, status_endpoints=tuple(binding['endpoints'])[1:])
            fleet.record('independent-unselected-control-repair', responsibility(control, mapped, local_scope, {identity}, control_options))
        idle_fleet(fleet, binding)
        fleet.stop()
        final = fleet.snapshot('all-pool-rebuilt-before-business-GET', quiescent=True)
        result['allPoolPhysical'] = validate_repaired_physical(original, final, targets[0], repair_targets=targets)
        result['allVersionStreaming'] = verify_parts(fleet, mapped, 'complete-all-pool-version-streaming')
        for identity, registration in registrations.items():
            if identity not in repaired:
                fleet.faults.remove(registration)
        fleet.start()
        result['s3'] = {}
        for endpoint in binding['endpoints']:
            result['s3'][endpoint] = {}
            for name in buckets:
                observed = fleet.record('all-peer-all-pool-full-S3', Probe(binding, endpoint).inventory(name,
                    unversioned_current_tags=name == bucket))
                result['s3'][endpoint][name] = {**verify_seed(expected[name], observed), **compare_inventories(baseline[name], observed)}
        result.update(verdict='PASS', unselectedControlsPreservedBeforeRecovery=True, crossPoolCopyCountedAsRepair=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict='FAIL', failure={'type': type(exc).__name__, 'message': str(exc)})
    finally:
        for path, token in active:
            try: fleet.record('stop-unresolved', probe.admin(path, [('clientToken', token), ('forceStop', 'true')], b''))
            except Exception as exc: fleet.record('stop-unresolved-error', {'error': str(exc)})
        pending = any(not row['resolved'] for row in ledger)
        if pending:
            try:
                fleet.stop()
                for node in tuple(fleet.quorum_gates):
                    fleet.record('failed-seed-gate-release', fleet.remote(node, 'quorum-gate', mode='clear'))
                    fleet.quorum_gates.remove(node)
                fleet.start()
                result['seedResolution'] = resolve_pending_fixture(fleet, binding, ledger)
            except Exception as exc:
                result['seedResolution'] = {'state': 'UNRESOLVED', 'error': str(exc)}
        if any(not row['resolved'] for row in ledger):
            try: fleet.stop()
            except Exception as exc: fleet.record('retain-stop-error', {'error': str(exc)})
            fleet.record('retained-unresolved-seed', {'state': 'DIRTY', 'namespacesRetained': True, 'requests': ledger})
            clean = False
        else:
            clean = fleet.cleanup()
        result['cleanup'] = 'PASS' if clean else 'FAIL'
        fleet.record('branch-result', result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'H06 interrupted by signal {signum}')
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
    require(select(CASE_ID)[0]['variants'] == list(BRANCHES), 'H06 source branch omitted')
    nodes = os.environ.get('RUSTFS_NODES', 'vm000 vm001 vm002 vm003').split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in nodes),
            'four distinct current Heal nodes required')
    results = []
    try:
        for branch in BRANCHES:
            results.append(run_branch(args, nodes[:4], branch))
            if results[-1]['cleanup'] != 'PASS': break
    finally:
        complete = len(results) == 3 and all(row['verdict'] == row['cleanup'] == 'PASS' for row in results)
        save(args.evidence / 'healthy-summary.json', {'caseId': CASE_ID, 'results': results, 'complete': complete,
            'cleanup': 'PASS' if results and all(row['cleanup'] == 'PASS' for row in results) else 'FAIL'})
    require(complete, 'H06 requires all actual H5 pool/set selectors, preserved control faults and full version/physical proof')


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        print(f'H06 multi-pool validation failed: {exc}', file=sys.stderr)
        sys.exit(1)
