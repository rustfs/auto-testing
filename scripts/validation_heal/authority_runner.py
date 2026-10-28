#!/usr/bin/env python3
"""Execute C07 live authority splits and independently bound native guard evidence."""

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
import time
from urllib.parse import quote
import xml.etree.ElementTree as ET

try:
    from .authority import CASE_ID, BRANCHES, census, object_files, validate_split, validate_negative_task, verify_native
    from .heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from .heal_workload import Recorder, verify_seed
    from .healthy import BASE_OPTIONS, Fleet, physical_content
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .native_regressions import CANDIDATE
    from .scopes import verify_parts
    from .shards import map_target
    from .stop import idle_fleet
    from .versions import validate_unrecoverable_get, verify_quorum_contract
except ImportError:
    from authority import CASE_ID, BRANCHES, census, object_files, validate_split, validate_negative_task, verify_native
    from heal_probe import Probe, canonical, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import BASE_OPTIONS, Fleet, physical_content
    from inspector import verify as verify_inspector
    from matrix import select
    from native_regressions import CANDIDATE
    from scopes import verify_parts
    from shards import map_target
    from stop import idle_fleet
    from versions import validate_unrecoverable_get, verify_quorum_contract


SIZE = 1024**2 + 73


def negative_heal(probe, fleet, bucket, options, active, contract):
    path = '/rustfs/admin/v3/heal/' + quote(bucket, safe='') + '/conflict.bin'
    started = fleet.record('negative-authority-admission', probe.admin(path, [], json.dumps(options).encode()))
    expected_error = contract['unrecoverableGet']
    if started.get('httpStatus') == expected_error['httpStatus']:
        body = started.get('response')
        if isinstance(body, dict):
            code = body.get('Code') or body.get('code')
            require(not body.get('clientToken'), 'refused authority admission returned a task token')
        else:
            require(isinstance(body, str) and '<!DOCTYPE' not in body and '<!ENTITY' not in body,
                    'authority admission refusal has no bounded structured S3 error')
            code = next((item.text for item in ET.fromstring(body).iter() if item.tag.rsplit('}', 1)[-1] == 'Code'), None)
        require(code == expected_error['code'], 'authority admission used a different frozen public quorum error')
        idle_fleet(fleet, probe.binding)
        return {'safeExplicitRefusal': True, 'admissionRejected': True, 'httpStatus': expected_error['httpStatus'], 'code': code,
                'taskCompletionClaimed': False, 'healed': 0}
    require(started.get('httpStatus') == 200 and isinstance(started.get('response'), dict), 'C07 Heal admission is neither a task nor a typed quorum refusal')
    token = started['response'].get('clientToken')
    require(isinstance(token, str) and token, 'C07 admission omitted original task ownership')
    active.append((path, token))
    deadline = time.monotonic() + 180
    while True:
        observed = fleet.record('negative-authority-task', probe.admin(path, [('clientToken', token)], b''))
        require(observed.get('httpStatus') == 200 and isinstance(observed.get('response'), dict), 'C07 task status became unknown')
        status = observed['response']
        if status.get('summary') != 'running': break
        require(time.monotonic() < deadline, 'C07 negative task exceeded its fixed deadline')
        time.sleep(0.2)
    proof = validate_negative_task(status, bucket, options)
    convergence_deadline = time.monotonic() + 30
    for endpoint in probe.binding['endpoints']:
        peer = Probe(probe.binding, endpoint)
        while True:
            repeat = fleet.record('same-original-authority-task-all-peer', peer.admin(path, [('clientToken', token)], b''))
            require(repeat.get('httpStatus') == 200 and isinstance(repeat.get('response'), dict),
                    'C07 peer status is missing; 404/unknown cannot certify refusal')
            value = repeat['response']
            if (value.get('summary') == status['summary'] and value.get('truncated', False) is False
                    and value.get('settings') == options and canonical(value.get('outcome')) == canonical(status['outcome'])):
                validate_negative_task(value, bucket, options)
                break
            require(time.monotonic() < convergence_deadline, 'C07 peers did not converge to original refusal within 30 seconds')
            time.sleep(0.2)
    fleet.record('negative-authority-convergence', {'path': path, 'clientToken': token,
        'queriedEndpoints': list(probe.binding['endpoints']), 'budgetSeconds': 30, 'sameOriginalPathAndToken': True,
        'completeOutcomeEqual': True, 'fullCoverageClaimed': status['outcome']['coverage'] == 'complete'})
    active.remove((path, token))
    return proof


def capture(fleet, snapshot, bucket, label):
    values = object_files(snapshot, bucket, 'conflict.bin')
    result = {}
    for (node, disk), entries in sorted(values.items()):
        result[(node, disk)] = fleet.record('archive-exact-generation-' + label,
            {'node': node, **fleet.remote(node, 'generation-capture', disk=disk, bucket=bucket, key='conflict.bin',
                generation=label, expectedFiles=entries)})
    return result


def recover_generations(fleet):
    for node, params in tuple(fleet.generation_faults):
        receipt = fleet.record('restore-last-ACK-after-conflict-proof', {'node': node, **fleet.remote(node, 'generation-recover', **params)})
        require(receipt.get('state') == 'CLEAR' and receipt.get('originalLastACKGenerationRestored') is True,
                'C07 recovery did not restore its complete last acknowledged generation')
        fleet.generation_faults.remove((node, params))


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / topology / branch
    directory.mkdir(parents=True)
    candidate = json.loads(args.candidate.read_text())
    require(candidate['source_sha'] == CANDIDATE, 'unreviewed C07 candidate authority policy')
    result = {'topology': topology, 'branch': branch, 'verdict': 'RUNNING', 'cleanup': 'PENDING'}
    if branch == 'native-authority-fence':
        try:
            require(args.native_directory is not None, 'C07 requires immutable separately collected native evidence')
            proof = verify_native(args.native_directory, candidate)
            save(directory / 'independent-native-guard.json', proof)
            result.update(verdict='PASS', cleanup='PASS', evidenceKind='separate-native-guard-regression',
                actualNativeUnitMembers=4, fleetDeploymentPerformed=False, native=proof)
        except Exception as exc:
            result.update(verdict='FAIL', cleanup='PASS', failure={'type': type(exc).__name__, 'message': str(exc)})
        save(directory / 'branch-result.json', result)
        return result
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile='authority-split')
    primary = 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    bucket = primary + '-dv' if branch == 'disjoint-version-quorums' else primary
    fleet.snapshot_buckets = [primary, bucket] if primary != bucket else [primary]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {'schemaVersion': 1, 'runId': run_id, 'jumpHostOnly': True, 'exclusiveHealService': True,
        'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), 'bucketPrefix': primary,
        'endpoints': {f'node{index}': f'http://rustfs-node{index}:9000' for index in range(1, len(nodes) + 1)},
        'defaultEndpoint': 'node1', 'candidateCommit': candidate['source_sha'], 'harnessCommit': args.testing_commit,
        'candidatePackageSha256': candidate['package_sha256'], 'candidateBinarySha256': args.binary_sha,
        'rcBinarySha256': hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), 'maxInventoryObjects': 128,
        'maxInventoryBytes': 512 * 1024**2}
    fleet.record('binding', binding)
    inspector = fleet.record('inspector-build', verify_inspector(args.tool_directory, candidate))
    contract = verify_quorum_contract(inspector, candidate)
    probe, active, pending_mutation = Probe(binding), [], None
    try:
        fleet.deploy(args.binary_sha, primary, access, secret)
        recorder = Recorder(probe, directory / 'fixture')
        def call(operation, params, body=None, check=None):
            nonlocal pending_mutation
            pending_mutation = {'operation': operation, 'params': params}
            response = recorder.call(operation, params, body)
            if check is not None:
                check(response)
            pending_mutation = None
            return response
        call('create_bucket', {'Bucket': bucket})
        versioned = branch == 'disjoint-version-quorums'
        if versioned:
            call('put_bucket_versioning', {'Bucket': bucket, 'VersioningConfiguration': {'Status': 'Enabled'}})
        def put(key, label):
            body = payload(f'C07/{bucket}/{key}/{label}', SIZE)
            def check(response):
                version = response.get('VersionId', 'null')
                require((versioned and version not in {None, 'null'}) or (not versioned and version == 'null'),
                        'C07 PUT ACK lost versioning identity')
            response = call('put_object', {'Bucket': bucket, 'Key': key, 'ContentType': 'application/octet-stream',
                'Metadata': {'heal-fixture': 'C07', 'generation': label}, 'Tagging': 'suite=heal&fixture=C07'}, body, check)
            version = response.get('VersionId', 'null')
            require((versioned and version not in {None, 'null'}) or (not versioned and version == 'null'), 'C07 PUT ACK lost versioning identity')
            return {'key': key, 'versionId': version, 'isLatest': True, 'deleteMarker': False,
                    'payload': {'size': SIZE, 'sha256': hashlib.sha256(body).hexdigest()}}
        control = put('control.bin', 'healthy-control')
        if versioned:
            for index in range(2):
                previous = put('conflict.bin', f'prior-{index}')
                deletion = call('delete_object', {'Bucket': bucket, 'Key': 'conflict.bin', 'VersionId': previous['versionId']},
                    check=lambda response: require(response.get('VersionId') == previous['versionId'], 'C07 prior-version DELETE ACK lost identity'))
                require(deletion.get('VersionId') == previous['versionId'], 'C07 prior-version DELETE ACK lost its exact identity')
        old = put('conflict.bin', 'generation-a')
        first = fleet.record('generation-a-full-S3', probe.inventory(bucket, unversioned_current_tags=not versioned))
        verify_seed([control, old], first)
        fleet.stop()
        original_a = fleet.snapshot('complete-generation-a', quiescent=True)
        first_census = census(original_a, bucket, 'conflict.bin', topology)
        require(len(first_census['groups']) == 1 and first_census['groups'][0]['identity']['versionId'] == old['versionId']
                and first_census['groups'][0]['identity']['size'] == old['payload']['size'],
                'C07 first physical generation differs from its exact ACK')
        for entry in (control, old):
            map_target(original_a, bucket, entry['key'], 'data', 1, entry['versionId'])
        capture(fleet, original_a, bucket, 'a')
        fleet.start()
        if versioned:
            deletion = call('delete_object', {'Bucket': bucket, 'Key': 'conflict.bin', 'VersionId': old['versionId']},
                check=lambda response: require(response.get('VersionId') == old['versionId'], 'C07 old-generation DELETE ACK lost identity'))
            require(deletion.get('VersionId') == old['versionId'], 'C07 old generation DELETE ACK lost identity')
        newest = put('conflict.bin', 'generation-b')
        baseline = fleet.record('last-ACK-generation-b-full-S3', probe.inventory(bucket, unversioned_current_tags=not versioned))
        verify_seed([control, newest], baseline)
        fleet.stop()
        original_b = fleet.snapshot('complete-generation-b', quiescent=True)
        baseline_census = census(original_b, bucket, 'conflict.bin', topology)
        require(baseline_census['R'] == baseline_census['N'] - 1, 'C07 explicit EC:1 runtime did not establish its actual sub-quorum split boundary')
        require(len(baseline_census['groups']) == 1 and baseline_census['groups'][0]['identity']['versionId'] == newest['versionId']
                and baseline_census['groups'][0]['identity']['size'] == newest['payload']['size'],
                'C07 last physical generation differs from its exact ACK')
        mapping = {(bucket, entry['key'], entry['versionId']): map_target(original_b, bucket, entry['key'], 'data', 1, entry['versionId'])
                   for entry in (control, newest)}
        saved_b = capture(fleet, original_b, bucket, 'b')
        members = sorted(saved_b)
        for node, disk in members[:len(members) // 2]:
            params = {'disk': disk, 'bucket': bucket, 'key': 'conflict.bin'}
            fleet.generation_faults.append((node, params))
            fleet.record('exact-archived-generation-replay', {'node': node, **fleet.remote(node, 'generation-install', **params,
                expectedSceneSha256=saved_b[(node, disk)]['treeSha256'])})
        mixed = fleet.snapshot('two-disjoint-subquorum-authorities', quiescent=True)
        result['split'] = validate_split(mixed, original_a, original_b, bucket, 'conflict.bin', topology, branch)
        fleet.start()
        for endpoint in binding['endpoints']:
            observed = fleet.record('all-peer-conflict-current-GET', Probe(binding, endpoint).s3('get_object', {'Bucket': bucket, 'Key': 'conflict.bin'}))
            validate_unrecoverable_get(observed, contract)
        options = {**BASE_OPTIONS, 'scanMode': 2, 'recursive': False, 'pool': 0, 'set': 0, 'remove': False}
        result['negativeResponsibility'] = negative_heal(probe, fleet, bucket, options, active, contract)
        idle_fleet(fleet, binding)
        fleet.stop()
        refused = fleet.snapshot('all-conflicting-authorities-after-refusal-before-recovery', quiescent=True)
        require(physical_content(mixed) == physical_content(refused), 'C07 negative GET/Heal modified a generation, control or original disk format')
        result['allConflictEvidencePreserved'] = True
        recover_generations(fleet)
        recovered = fleet.snapshot('restored-last-ACK-before-GET', quiescent=True)
        require(physical_content(recovered) == physical_content(original_b), 'C07 fixture recovery changed original last-ACK authority or healthy controls')
        result['streaming'] = verify_parts(fleet, mapping, 'all-last-ACK-original-cohorts-streaming')
        fleet.start()
        result['s3'] = {}
        for endpoint in binding['endpoints']:
            observed = fleet.record('all-peer-restored-full-S3', Probe(binding, endpoint).inventory(bucket, unversioned_current_tags=not versioned))
            result['s3'][endpoint] = {**verify_seed([control, newest], observed), **compare_inventories(baseline, observed)}
        result.update(verdict='PASS', oldDeletedVersionResurrected=False, backupRestorationIsProductRepair=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict='FAIL', failure={'type': type(exc).__name__, 'message': str(exc)})
    finally:
        for path, token in active:
            try: fleet.record('stop-unresolved', probe.admin(path, [('clientToken', token), ('forceStop', 'true')], b''))
            except Exception as exc: fleet.record('stop-unresolved-error', {'error': str(exc)})
        if pending_mutation is not None:
            try: fleet.stop()
            except Exception as exc: fleet.record('retain-stop-error', {'error': str(exc)})
            fleet.record('retain-uncertain-C07-mutation', {'state': 'DIRTY', 'namespacesRetained': True, 'request': pending_mutation,
                'reason': 'the original durable request has no complete known receipt; independent resolution is required'})
            clean = False
        else:
            clean = fleet.cleanup()
        result['cleanup'] = 'PASS' if clean else 'FAIL'
        fleet.record('branch-result', result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f'C07 interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('candidate', 'rc-bin', 'evidence', 'tool-directory'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--native-directory', type=Path, default=os.environ.get('VALIDATION_HEAL_NATIVE_DIRECTORY'))
    parser.add_argument('--case-id', choices=[CASE_ID], required=True)
    parser.add_argument('--testing-commit', required=True)
    parser.add_argument('--binary-sha', required=True)
    args = parser.parse_args()
    require(re.fullmatch(r'[0-9a-f]{64}', args.binary_sha) and re.fullmatch(r'[1-9][0-9]*', os.environ.get('GITHUB_RUN_ID', '')),
            'C07 requires immutable workflow/binary identities')
    require(select(CASE_ID)[0]['variants'] == list(BRANCHES), 'C07 source branch omitted')
    require(args.native_directory is not None, 'C07 requires a pinned successful native artifact before fleet deployment')
    save(args.evidence / 'native-preflight.json', verify_native(args.native_directory, json.loads(args.candidate.read_text())))
    nodes = os.environ.get('RUSTFS_NODES', 'vm000 vm001 vm002 vm003').split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes)) and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in nodes),
            'C07 requires four distinct current Heal nodes')
    results = []
    try:
        for topology in ('H1', 'H2'):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, nodes[:1] if topology == 'H1' else nodes[:4], branch))
                if results[-1]['cleanup'] != 'PASS': break
            if results[-1]['cleanup'] != 'PASS': break
    finally:
        complete = len(results) == 6 and all(row['verdict'] == row['cleanup'] == 'PASS' for row in results)
        save(args.evidence / 'healthy-summary.json', {'caseId': CASE_ID, 'results': results, 'complete': complete,
            'cleanup': 'PASS' if results and all(row['cleanup'] == 'PASS' for row in results) else 'FAIL'})
    require(complete, 'C07 needs both actual live topologies, both split branches, independent native guards and complete cleanup')


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        print(f'C07 authority validation failed: {exc}', file=sys.stderr)
        sys.exit(1)
