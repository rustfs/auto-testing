"""Preserve the fully acknowledged J02 fixture that failed before grace replay."""

import hashlib
import json
import re
import uuid

try:
    from .healthy import physical_content, validate_topology
    from .heal_probe import require
    from .native_regressions import CANDIDATE
    from .orphan import KEY, references
    from .shards import files, map_target
    from .versions import public_version_id
except ImportError:
    from healthy import physical_content, validate_topology
    from heal_probe import require
    from native_regressions import CANDIDATE
    from orphan import KEY, references
    from shards import files, map_target
    from versions import public_version_id


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
