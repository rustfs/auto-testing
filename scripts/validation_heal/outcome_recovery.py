"""Restore and retain exactly archived J02 failures before negative task admission."""

import copy
import hashlib
import json
import re
import uuid

try:
    from .healthy import physical_content, validate_topology
    from .heal_probe import require
    from .native_regressions import CANDIDATE
    from .orphan import KEY, references
    from .shards import files, map_target, validate_fault
    from .heal_fault import bounded_fault_id
    from .heal_workload import verify_seed
    from .versions import public_version_id, metadata_target
except ImportError:
    from healthy import physical_content, validate_topology
    from heal_probe import require
    from native_regressions import CANDIDATE
    from orphan import KEY, references
    from shards import files, map_target, validate_fault
    from heal_fault import bounded_fault_id
    from heal_workload import verify_seed
    from versions import public_version_id, metadata_target


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def acknowledged_grace_plan(artifact, failed_run, candidate):
    case = 'heal.outcome-counters-and-error-truth'
    paths = list(artifact.glob('*/run/*/cases/*.json'))
    require(len(paths) == 1 and paths[0].name == case + '.json', 'J02 recovery requires one unmixed original result')
    result = json.loads(paths[0].read_text()); provenance = result.get('provenance') or {}
    require(result.get('case_id') == case and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and candidate.get('source_sha') == provenance.get('source_commit') == CANDIDATE
            and candidate.get('package_sha256') == provenance.get('candidate_sha256'), 'J02 recovery candidate/result differs')
    evidence = paths[0].parents[1] / 'evidence' / case
    summary = json.loads((evidence / 'healthy-summary.json').read_text()); rows = summary.get('results') or []
    require(summary.get('caseId') == case and summary.get('cleanup') == 'FAIL' and summary.get('complete') is False
            and summary.get('canonicalCasePassClaimed') is False and len(rows) == 3
            and [(row.get('topology'), row.get('fixture'), row.get('branch')) for row in rows] == [
                ('H1', 'DB', branch) for branch in ('mixed-healthy-repaired-failed', 'dry-run', 'deferred')]
            and all(row.get('cleanup') == 'PASS' for row in rows[:-1]) and rows[-1].get('cleanup') == 'FAIL'
            and rows[-1].get('verdict') == 'FAIL' and rows[-1].get('failure') == {
                'type': 'FixtureStateMismatch', 'message': 'baseline content/version/marker differs from deterministic fixture ACK'},
            'J02 recovery supports only its pre-replay public-manifest refusal')
    folder = evidence / 'DB/H1/deferred'
    def one(label):
        found = [p for p in folder.glob('*-' + label + '.json')
                 if re.fullmatch(r'[0-9]{5}-' + re.escape(label) + r'\.json', p.name)]
        require(len(found) == 1, 'J02 recovery lacks exact ' + label)
        return json.loads(found[0].read_text())
    binding, failure, reservation = one('binding'), one('failure-scene-before-fault-rescue'), one('reserved')
    run_id, bucket = binding.get('runId', ''), binding.get('bucketPrefix', '')
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-h1', run_id) is not None
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}', bucket) is not None
            and binding.get('harnessCommit') == provenance.get('testing_commit')
            and binding.get('candidateCommit') == candidate['source_sha']
            and binding.get('candidatePackageSha256') == candidate['package_sha256']
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and set(failure) == {'vm000'} and reservation.get('node') == 'vm000' and reservation.get('reserved') is True,
            'J02 archived run/helper/node binding differs')
    owner = reservation.get('owner') or {}
    require(owner.get('runId') == run_id and owner.get('bucket') == bucket and owner.get('runtimeProfile') == 'outcome-grace'
            and owner.get('binarySha256') == provenance['binary_sha256'] and len(owner.get('mounts', [])) == 4,
            'J02 original reservation differs')
    require(one('retain-unknown-seed-receipt') == dict(state='DIRTY', namespacesRetained=True,
            request=dict(operation='original-version-specific-grace-source-procedure', bucket=bucket)), 'J02 pending boundary differs')
    require(not any('grace-' in path.name and not path.name.endswith('-J02-grace-ACK-server-age.json')
                    for path in folder.glob('*.json')),
            'J02 grace advanced beyond original version PUTs')
    wanted = list(one('complete-ACK-manifest')['expected'])
    for name, operations in (('fixture', ['create_bucket'] + ['put_object'] * 3),
                             ('original-grace-fixture', ['put_bucket_versioning', 'put_object', 'put_object'])):
        requests = sorted((folder / name).glob('*-request.json')); acks = sorted((folder / name).glob('*-ack.json'))
        require(len(requests) == len(acks) == len(operations), 'J02 source ledger is incomplete or contains a later mutation')
        for sequence, (request_path, ack_path, operation) in enumerate(zip(requests, acks, operations, strict=True), 1):
            request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
            observed = ack.get('observation') or {}; params = request.get('params') or {}
            require(type(request.get('sequence')) is int and request['sequence'] == ack.get('sequence') == sequence
                    and type(ack.get('sequence')) is int and request.get('runId') == ack.get('runId') == run_id
                    and ack.get('requestFile') == request_path.name
                    and ack.get('requestSha256') == hashlib.sha256(request_path.read_bytes()).hexdigest()
                    and request.get('operation') == observed.get('operation') == operation
                    and params.get('Bucket') == bucket and params == observed.get('params')
                    and ack.get('operationCheck') == 'PASS' and observed.get('acknowledged') is True
                    and observed.get('clientCallInvoked') is True and observed.get('resultUnknown') is False
                    and observed.get('response', {}).get('ResponseMetadata', {}).get('HTTPStatusCode') == 200,
                    'J02 source mutation has an unknown or rebound ACK')
            if operation == 'put_object':
                require(request.get('payload') == observed.get('payload') and re.fullmatch(r'[0-9a-f]{64}', request['payload']['sha256']),
                        'J02 payload ACK differs from its durable request')
                if name == 'original-grace-fixture':
                    version = observed.get('response', {}).get('VersionId')
                    require(params.get('Key') == KEY and isinstance(version, str) and str(uuid.UUID(version)) == version
                            and uuid.UUID(version).int != 0, 'J02 original grace PUT lacks a real version')
                    wanted.append(dict(key=KEY, versionId=version, payload=request['payload']))
                else:
                    matches = [entry for entry in wanted if entry['key'] == params.get('Key')]
                    require(len(matches) == 1 and matches[0]['versionId'] == 'null'
                            and matches[0]['payload'] == request['payload'], 'J02 control manifest differs from original ACK')
    require(len(wanted) == 5 and len({(entry['key'], entry['versionId']) for entry in wanted}) == 5,
            'J02 source lacks five distinct acknowledged objects/versions')
    validate_topology(failure, 'H1')
    files(failure)
    require({entry['metadata']['key'] for entry in failure['vm000']['files'] if 'metadata' in entry}
            == {entry['key'] for entry in wanted}
            and all(any(entry['path'].startswith(bucket + '/' + version['key'] + '/') for version in wanted)
                    for entry in failure['vm000']['files']), 'J02 physical scene has another key/bucket')
    for key in {entry['key'] for entry in wanted}:
        expected = {entry['versionId'] for entry in wanted if entry['key'] == key}
        require(all({public_version_id(version) for version in values} == expected
                    for values in references(failure, bucket, key).values()), 'J02 physical version authority differs from ACKs')
    for entry in wanted:
        target = map_target(failure, bucket, entry['key'], 'data', 1, entry['versionId'], latest_required=False)
        require(target['logical']['size'] == entry['payload']['size'] and len(target['cohort']) == 4,
                'J02 ACK lacks its complete original physical cohort')
    return [dict(kind='acknowledged-j02-before-replay', runId=run_id, nodes=['vm000'], owners={'vm000': owner},
        before=failure, binarySha256=provenance['binary_sha256'], buckets=[bucket],
        originalCaseSha256=hashlib.sha256(paths[0].read_bytes()).hexdigest())]


def isolate_acknowledged_grace(fleet, plan, states):
    require(len(states) == 1 and states[0].get('node') == 'vm000', 'J02 recovery fleet differs')
    state = states[0]; retained = state.get('retained') or {}
    if not retained.get('state') and not any(retained.get('roots', [])):
        require(state['state']['exists'] and len(state['roots']) == 4 and all(root['exists'] for root in state['roots']),
                'J02 acknowledged fixture lost a root; retain remaining scene')
        fleet.record('J02-acknowledged-recovery-stop', fleet.remote('vm000', 'stop'))
        current = fleet.record('J02-acknowledged-current-physical', fleet.snapshot('J02-stopped-preserved-scene', quiescent=True))
        require(physical_content(current) == physical_content(plan['before'])
                and all(value.get('pid') == 0 and type(value.get('pid')) is int and value.get('quiescent') is True
                    and value.get('binarySha256') == plan['binarySha256'] for value in current.values()),
                'J02 acknowledged fixture drifted; no isolation or restoration performed')
    params = dict(expectedOwner=plan['owners']['vm000'],
                  expectedOriginalSceneSha256=digest(physical_content(plan['before'])['vm000']))
    audit = fleet.record('J02-acknowledged-archive-preflight', fleet.remote('vm000', 'retained-scene-audit', **params))
    require(audit.get('inspectionOnly') is True and audit.get('mutated') is False
            and audit.get('baseline', {}).get('originalSceneSha256') == params['expectedOriginalSceneSha256'], 'J02 archive preflight differs')
    isolated = fleet.record('J02-acknowledged-archive-isolation', fleet.remote('vm000', 'retained-scene-isolate',
        **params, expectedBaseline=audit['baseline']))
    checked = fleet.record('J02-acknowledged-archive-post-audit', fleet.remote('vm000', 'retained-scene-audit', **params))
    require(all(value.get('activeNamespaceIsolated') is True and value.get('canonicalPassClaimed') is False
                and value.get('baseline') == audit['baseline'] for value in (isolated, checked))
            and checked.get('inspectionOnly') is True and checked.get('mutated') is False, 'J02 retained scene isolation drifted')
    return dict(runId=plan['runId'], cleanup='ISOLATED', activeNamespaceIsolated=True, backupRestorationUsed=False,
        originalSceneRestored=False, originalBusinessFixtureRestored=False, canonicalPassClaimed=False,
        originalVerdictRewritten=False, originalCaseSha256=plan['originalCaseSha256'])


def interrupted_opaque_plan(artifact, failed_run, candidate):
    case = 'heal.outcome-counters-and-error-truth'
    paths = list(artifact.glob('*/run/*/cases/*.json'))
    require(len(paths) == 1 and paths[0].name == case + '.json', 'opaque recovery requires one unmixed J02 result')
    raw = paths[0].read_bytes(); result = json.loads(raw); provenance = result.get('provenance') or {}
    require(result.get('case_id') == case and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and candidate.get('source_sha') == provenance.get('source_commit') == CANDIDATE
            and candidate.get('package_sha256') == provenance.get('candidate_sha256'), 'opaque recovery candidate differs')
    evidence = paths[0].parents[1] / 'evidence' / case
    summary = json.loads((evidence / 'healthy-summary.json').read_text()); rows = summary.get('results') or []
    require(summary.get('caseId') == case and summary.get('cleanup') == 'FAIL' and summary.get('complete') is False
            and summary.get('canonicalCasePassClaimed') is False and len(rows) == 5
            and [(r.get('topology'), r.get('fixture'), r.get('branch')) for r in rows] == [
                ('H1', 'DB', branch) for branch in ('mixed-healthy-repaired-failed', 'dry-run', 'deferred',
                                                  'readonly-terminal-failure', 'unknown-negative')]
            and all(r.get('cleanup') == 'PASS' for r in rows[:-1]) and rows[-1].get('cleanup') == 'FAIL'
            and rows[-1].get('verdict') == 'FAIL' and rows[-1].get('failure', {}).get('type') == 'ValueError'
            and rows[-1]['failure']['message'].startswith('Heal node vm000 snapshot failed:')
            and rows[-1]['failure']['message'].endswith('ValueError: metadata decoder failed outside an exact witnessed file fault\n'),
            'opaque recovery only supports its second-file witness refusal before task admission')
    folder = evidence / 'DB/H1/unknown-negative'
    records = sorted(folder.glob('*.json'))
    require({p.name for p in folder.iterdir() if p.is_dir()} == {'fixture', 'original-opaque-fixture'},
            'opaque recovery contains another task or mutation directory')
    labels = ['binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation', 'reserved', 'inspector-upload',
        'started', 'deployed-topology', 'topology-proof', 'ACK-bound-full-S3', 'complete-ACK-manifest', 'trigger',
        'control', 'control-repeat', 'control-repeat', 'task-exit-background', 'quiescent', 'started',
        'original-opaque-ACK-full-S3', 'original-opaque-complete-ACK-manifest', 'trigger', 'control',
        'control-repeat', 'control-repeat', 'task-exit-background', 'quiescent', 'original-opaque-complete-physical',
        *(['original-opaque-full-cohort-bitrot'] * 4), 'original-opaque-metadata-fault-plan',
                'original-opaque-byte-zero-bitflip', 'original-opaque-physical-injection', 'original-opaque-byte-zero-bitflip',
                'failure-scene-error', 'quiescent', 'retain-unknown-seed-receipt', 'branch-result']
    require([p.name for p in records] == [f'{index:05d}-{label}.json' for index, label in enumerate(labels, 1)]
            and set(folder.iterdir()) == set(records) | {folder / name for name in ('fixture', 'original-opaque-fixture')},
            'opaque recovery has a later task, mutation or unknown boundary')
    def one(label):
        found = [p for p in records if p.name[6:-5] == label]
        require(len(found) == 1, 'opaque recovery lacks exact ' + label)
        return json.loads(found[0].read_text())
    binding, original, reservation = one('binding'), one('original-opaque-complete-physical'), one('reserved')
    run_id, bucket = binding.get('runId', ''), binding.get('bucketPrefix', '')
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-h1', run_id) is not None
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}', bucket) is not None and set(original) == {'vm000'}
            and binding.get('candidateCommit') == candidate['source_sha']
            and binding.get('candidatePackageSha256') == candidate['package_sha256']
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and binding.get('harnessCommit') == provenance.get('testing_commit'), 'opaque archived ownership differs')
    owner = reservation.get('owner') or {}
    require(reservation.get('node') == 'vm000' and reservation.get('reserved') is True
            and owner.get('runId') == run_id and owner.get('bucket') == bucket
            and owner.get('runtimeProfile') == 'outcome-legacy' and len(owner.get('mounts', [])) == 4
            and owner.get('binarySha256') == binding['candidateBinarySha256']
            and original['vm000'].get('quiescent') is True and type(original['vm000'].get('pid')) is int
            and original['vm000']['pid'] == 0 and original['vm000'].get('binarySha256') == binding['candidateBinarySha256'],
            'opaque original reservation or stopped candidate differs')
    require(one('retain-unknown-seed-receipt') == dict(state='DIRTY', namespacesRetained=True,
        request=dict(operation='original-corrupt-metadata-source-probe', bucket=bucket)), 'opaque pending source boundary differs')
    wanted = one('original-opaque-complete-ACK-manifest')['expected']
    require(len(wanted) == 2 and {e['key'] for e in wanted} == {'objects/0000.bin', 'opaque/source.bin'}
            and all(e['versionId'] == 'null' for e in wanted), 'opaque source ACK manifest differs')
    verify_seed(wanted, one('original-opaque-ACK-full-S3'))
    validate_opaque_ledger(folder, run_id, bucket, wanted)
    validate_topology(original, 'H1')
    target = map_target(original, bucket, 'opaque/source.bin', 'data', 1, 'null', latest_required=False)
    targets = [metadata_target(original, {**target, **member}) for member in target['cohort']]
    require(one('original-opaque-metadata-fault-plan') == targets and len(targets) == 4, 'opaque original target plan differs')
    receipts = [json.loads(p.read_text()) for p in records if p.name[6:-5] == 'original-opaque-byte-zero-bitflip']
    first = one('original-opaque-physical-injection'); validate_fault(original, first, targets[0], receipts[0])
    first_error = files(first)[('vm000', targets[0]['disk'], targets[0]['relativePath'])].get('metadataError') or {}
    require(first_error.get('exactWitnessedFault') is True and first_error.get('validMetadataClaimed') is False,
            'opaque first metadata corruption lacks its exact decoder witness')
    failure, faults, witnesses = copy.deepcopy(original), [], {'vm000': {}}
    for index, (receipt, selected) in enumerate(zip(receipts, targets[:2], strict=True)):
        fault_id = bounded_fault_id('J02', 'original-opaque', index)
        require(receipt.get('node') == selected['node'] == 'vm000' and receipt.get('runId') == run_id
                and receipt.get('faultId') == fault_id and receipt.get('kind') == 'bitflip'
                and type(receipt.get('offset')) is int and receipt['offset'] == 0
                and receipt.get('relativePath') == selected['relativePath'] and receipt.get('backup') == fault_id + '.original'
                and receipt.get('before') == {'sha256': selected['sha256'], 'size': selected['size']}
                and receipt.get('requiresQuiescentService') is True and receipt.get('backend') == 'logical-file-fault'
                and receipt.get('fault', {}).get('size') == selected['size']
                and re.fullmatch(r'[0-9a-f]{64}', receipt.get('fault', {}).get('sha256', '')) is not None,
                'opaque injected fault differs from the original planned file')
        changed = copy.deepcopy(failure)
        entry = next(e for e in changed['vm000']['files'] if e['disk'] == selected['disk'] and e['path'] == selected['relativePath'])
        entry.pop('metadata'); entry.update(receipt['fault'], metadataError=first_error)
        validate_fault(failure, changed, selected, receipt); failure = changed
        faults.append(dict(node='vm000', disk=selected['disk'], faultId=fault_id,
                           receipt={k:v for k,v in receipt.items() if k != 'node'}))
        witnesses['vm000'][f"{selected['disk']}:{selected['relativePath']}"] = receipt['fault']
    return [dict(kind='interrupted-j02-opaque', runId=run_id, nodes=['vm000'], owners={'vm000': owner}, before=original,
        failure=failure, faults=faults, metadataFaults=witnesses, buckets=[bucket], binarySha256=binding['candidateBinarySha256'],
        originalCaseSha256=hashlib.sha256(raw).hexdigest())]


def validate_opaque_ledger(folder, run_id, bucket, wanted):
    for name, operations in (('fixture', ('create_bucket', 'put_object')), ('original-opaque-fixture', ('put_object',))):
        requests = sorted((folder / name).glob('*-request.json')); acks = sorted((folder / name).glob('*-ack.json'))
        require(len(requests) == len(acks) == len(operations)
                and set((folder / name).iterdir()) == set(requests + acks), 'opaque original SDK ledger is incomplete')
        for sequence, (request_path, ack_path, operation) in enumerate(zip(requests, acks, operations, strict=True), 1):
            request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text()); observed = ack.get('observation') or {}
            require(type(request.get('sequence')) is int and type(ack.get('sequence')) is int
                    and request['sequence'] == ack['sequence'] == sequence and request.get('runId') == ack.get('runId') == run_id
                    and ack.get('requestFile') == request_path.name
                    and ack.get('requestSha256') == hashlib.sha256(request_path.read_bytes()).hexdigest()
                    and observed.get('operation') == request.get('operation') == operation
                    and observed.get('params') == request.get('params') and request['params'].get('Bucket') == bucket
                    and ack.get('operationCheck') == 'PASS' and observed.get('acknowledged') is True
                    and observed.get('clientCallInvoked') is True and observed.get('resultUnknown') is False
                    and observed.get('response', {}).get('ResponseMetadata', {}).get('HTTPStatusCode') == 200,
                    'opaque original mutation is unknown or rebound')
            if operation == 'put_object':
                key = 'opaque/source.bin' if name == 'original-opaque-fixture' else 'objects/0000.bin'
                expected = next(e for e in wanted if e['key'] == key)
                require(request['params'].get('Key') == key and request.get('payload') == observed.get('payload') == expected['payload']
                        and public_version_id(observed['response'].get('VersionId')) == expected['versionId'], 'opaque original PUT differs from ACK')


def acknowledged_legacy_plan(artifact, failed_run, candidate):
    case = 'heal.outcome-counters-and-error-truth'
    paths = list(artifact.glob('*/run/*/cases/*.json'))
    require(len(paths) == 1 and paths[0].name == case + '.json', 'legacy recovery requires one unmixed J02 result')
    raw = paths[0].read_bytes(); result = json.loads(raw); provenance = result.get('provenance') or {}
    require(result.get('case_id') == case and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and candidate.get('source_sha') == provenance.get('source_commit') == '1c7fc818dd858247c45b39542abd8291b1e6e31b'
            and candidate.get('package_sha256') == provenance.get('candidate_sha256'), 'legacy recovery candidate differs')
    evidence = paths[0].parents[1] / 'evidence' / case
    summary = json.loads((evidence / 'healthy-summary.json').read_text()); rows = summary.get('results') or []
    require(summary.get('caseId') == case and summary.get('cleanup') == 'FAIL' and summary.get('complete') is False
            and summary.get('canonicalCasePassClaimed') is False and len(rows) == 5
            and [(r.get('topology'), r.get('fixture'), r.get('branch')) for r in rows] == [
                ('H1', 'DB', branch) for branch in ('mixed-healthy-repaired-failed', 'dry-run', 'deferred',
                                                  'readonly-terminal-failure', 'unknown-negative')]
            and all(r.get('cleanup') == 'PASS' for r in rows[:-1]) and rows[-1].get('cleanup') == 'FAIL'
            and rows[-1].get('verdict') == 'FAIL' and rows[-1].get('failure', {}).get('type') == 'ValueError'
            and rows[-1]['failure']['message'].startswith('Heal node vm000 start failed:')
            and rows[-1]['failure']['message'].endswith('ImportError: attempted relative import with no known parent package\n'),
            'legacy recovery requires the exact pre-SDK start refusal')
    folder = evidence / 'DB/H1/unknown-negative'; records = sorted(folder.glob('*.json'))
    require(len(records) == 71 and all(p.name.startswith(f'{i:05d}-') for i, p in enumerate(records, 1))
            and [p.name[6:-5] for p in records[-7:]] == ['original-opaque-after-restored-business-reads',
                'protected-control-before-legacy-writing', 'one-way-legacy-write-phase', 'failure-scene-error',
                'quiescent', 'retain-unknown-seed-receipt', 'branch-result']
            and set(folder.iterdir()) == set(records) | {folder / name for name in ('fixture', 'original-opaque-fixture')},
            'legacy recovery has another task or SDK mutation boundary')
    def one(label):
        found = [p for p in records if p.name[6:-5] == label]
        require(len(found) == 1, 'legacy recovery lacks exact ' + label)
        return json.loads(found[0].read_text())
    binding, original, reserved = one('binding'), one('original-opaque-complete-physical'), one('reserved')
    run_id, bucket = binding.get('runId', ''), binding.get('bucketPrefix', '')
    owner = reserved.get('owner') or {}
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-h1', run_id) and set(original) == {'vm000'}
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}', bucket)
            and binding.get('candidateCommit') == candidate['source_sha']
            and binding.get('candidatePackageSha256') == candidate['package_sha256']
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and binding.get('harnessCommit') == provenance.get('testing_commit')
            and reserved.get('node') == 'vm000' and reserved.get('reserved') is True
            and owner.get('runId') == run_id and owner.get('bucket') == bucket and owner.get('runtimeProfile') == 'outcome-legacy'
            and owner.get('binarySha256') == binding['candidateBinarySha256'] and len(owner.get('mounts', [])) == 4,
            'legacy recovery reservation differs')
    require(one('retain-unknown-seed-receipt') == dict(state='DIRTY', namespacesRetained=True,
            request=dict(operation='append-real-legacy-fixture', bucket=bucket)), 'legacy pending boundary differs')
    require(one('one-way-legacy-write-phase') == dict(node='vm000', runId=run_id, binarySha256=owner['binarySha256'],
            phase='legacy', oneWay=True, candidateUnchanged=True, credentialsDisclosed=False, otherRuntimeSettingsUnchanged=True),
            'legacy original phase acknowledgement differs')
    wanted = one('original-opaque-complete-ACK-manifest')['expected']
    require(len(wanted) == 2 and {e['key'] for e in wanted} == {'objects/0000.bin', 'opaque/source.bin'}
            and all(e['versionId'] == 'null' for e in wanted), 'legacy pre-PUT manifest differs')
    validate_opaque_ledger(folder, run_id, bucket, wanted)
    verify_seed(wanted, one('original-opaque-ACK-full-S3')); verify_seed(wanted, one('original-opaque-restored-full-S3'))
    validate_topology(original, 'H1')
    for entry in wanted:
        target = map_target(original, bucket, entry['key'], 'data', 1, 'null', latest_required=False)
        require(len(target['cohort']) == 4 and target['logical']['size'] == entry['payload']['size'], 'legacy original cohort differs')
    target = map_target(original, bucket, 'opaque/source.bin', 'data', 1, 'null', latest_required=False)
    targets = [metadata_target(original, {**target, **member}) for member in target['cohort']]
    require(one('original-opaque-metadata-fault-plan') == targets, 'legacy original metadata target plan differs')
    injected = [json.loads(p.read_text()) for p in records if p.name[6:-5] == 'original-opaque-byte-zero-bitflip']
    rescued = [json.loads(p.read_text()) for p in records if p.name[6:-5] == 'restore-fixture-without-relabeling-original-task']
    require(len(injected) == len(rescued) == 4, 'legacy original rescue receipt census differs')
    for i, (receipt, proof, target) in enumerate(zip(injected, rescued, targets, strict=True)):
        fault_id = bounded_fault_id('J02', 'original-opaque', i)
        require(receipt.get('faultId') == proof.get('faultId') == fault_id and receipt.get('node') == proof.get('node') == 'vm000'
                and receipt.get('runId') == proof.get('runId') == run_id and receipt.get('relativePath') == target['relativePath']
                and receipt.get('before') == {k:target[k] for k in ('sha256', 'size')}
                and receipt.get('kind') == 'bitflip' and type(receipt.get('offset')) is int and receipt['offset'] == 0
                and proof.get('restoredFromBackup') is True and proof.get('physicalRepairPassAllowed') is False,
                'legacy original rescue is not bound to its source fault')
    for label in ('original-opaque-restored-original-bytes', 'original-opaque-after-restored-business-reads',
                  'protected-control-before-legacy-writing'):
        value = one(label)
        require(physical_content(value) == physical_content(original)
                and value['vm000'].get('quiescent') is True and type(value['vm000'].get('pid')) is int
                and value['vm000']['pid'] == 0 and value['vm000'].get('binarySha256') == owner['binarySha256'],
                'legacy original business authority advanced before phase')
    return [dict(kind='acknowledged-j02-before-legacy-put', runId=run_id, nodes=['vm000'], owners={'vm000':owner},
        before=original, binarySha256=owner['binarySha256'], buckets=[bucket],
        originalCaseSha256=hashlib.sha256(raw).hexdigest())]
