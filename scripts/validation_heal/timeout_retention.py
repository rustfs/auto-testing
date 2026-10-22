"""Isolate the archived I05 H1 failure before any contender was invoked."""

import hashlib
import json
import re
import uuid

try:
    from .heal_probe import canonical, payload, require
    from .heal_workload import verify_seed
    from .healthy import physical_content
    from .lock_observer import CANDIDATE
    from .race_retention import digest, validate
except ImportError:
    from heal_probe import canonical, payload, require
    from heal_workload import verify_seed
    from healthy import physical_content
    from lock_observer import CANDIDATE
    from race_retention import digest, validate


CASE = 'heal.lock-timeout-and-caller-abort'
HELPER = '704bd606071676dded130dca91d3d5278455bcaa'
FAILURE = {'type': 'ValueError',
           'message': 'caller lock diagnostic escaped its exact GET identity or runtime budget'}
LABELS = ('binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation', 'reserved',
          'inspector-upload', 'started', 'deployed-topology', 'actual-topology', 'original-ACK-bound-S3',
          'original-ACK-proof', 'I05-timeout-original-physical', 'I05-timeout-original-S3',
          'I05-timeout-baseline-physical-after-public-read', 'I05-caller-baseline-resources',
          'I05-caller-checkpoint', 'I05-caller-window', 'quiescent', 'original-failure-scene',
          'original-failure-logs', 'diagnostic-result')


def archived_plan(artifact, failed_run, candidate):
    """Refuse uncertain seed effects, later actions, mixed cases and physical drift.

    This is preservation, with no S3 call, backup restoration or verdict change.
    The old helper's exact pre-contender boundary is deliberately the only route.
    """
    require(type(failed_run) is int and failed_run > 0, 'I05 requires an exact failed run')
    paths = list(artifact.glob('*/run/*/cases/*.json'))
    require(len(paths) == 1 and paths[0].stem == CASE, 'I05 cannot omit another failed case')
    def read(path):
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 2 * 1024**2,
                'I05 archive member is missing, unsafe or oversized')
        return json.loads(path.read_text())
    result = read(paths[0])
    provenance = result.get('provenance') or {}
    require(result.get('case_id') == CASE and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and provenance.get('source_commit') == candidate.get('source_sha') == CANDIDATE
            and provenance.get('candidate_sha256') == candidate.get('package_sha256')
            and provenance.get('testing_commit') == HELPER,
            'I05 requires its original failed candidate and pre-contender helper')
    evidence = paths[0].parents[1] / 'evidence' / CASE
    summary = read(evidence / 'healthy-summary.json')
    row = {'topology': 'H1', 'diagnosticComplete': False, 'cleanup': 'FAIL',
           'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False,
           'failure': FAILURE, 'namespacesRetained': True}
    require(summary.get('caseId') == CASE and summary.get('cleanup') == 'FAIL'
            and summary.get('diagnostics') == [row] and summary.get('complete') is False
            and summary.get('diagnosticComplete') is False
            and summary.get('requestedTopologies') == ['H1', 'H2']
            and summary.get('canonicalCasePassClaimed') is False
            and not (evidence / 'H2').exists(), 'I05 requires the sole retained pre-contender H1')
    folder = evidence / 'H1'
    numbered = {label: f'{index:05d}-{label}.json' for index, label in enumerate(LABELS, 1)}
    require({path.name for path in folder.iterdir()} == set(numbered.values()) | {'fixture', 'timeout'},
            'I05 archive contains another action or lacks its original boundary')
    def one(label):
        return read(folder / numbered[label])
    binding, reservation, scene = one('binding'), one('reserved'), one('original-failure-scene')
    run_id, bucket = binding.get('runId', ''), binding.get('bucketPrefix', '')
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-h1', run_id)
            and bucket == 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
            and binding.get('candidateCommit') == CANDIDATE
            and binding.get('candidatePackageSha256') == candidate['package_sha256']
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and binding.get('harnessCommit') == HELPER
            and binding.get('endpoints') == {'node1': 'http://rustfs-node1:9000'}
            and binding.get('defaultEndpoint') == 'node1' and one('diagnostic-result') == row,
            'I05 original run, package, helper, binary or failure boundary differs')
    owner = reservation.get('owner') or {}
    require(reservation.get('node') == 'vm000' and reservation.get('reserved') is True
            and owner.get('runtimeProfile') == 'lock-timeout' and set(scene) == {'vm000'},
            'I05 requires the sole original H1 reserved owner')
    inspector = one('inspector-build')
    require(inspector.get('candidateCommit') == CANDIDATE
            and inspector.get('binarySha256') == owner.get('inspectorSha256')
            and inspector.get('built') is True, 'I05 original inspector identity differs')
    original = scene['vm000']
    require(original.get('quiescent') is True and type(original.get('pid')) is int and original['pid'] == 0
            and original.get('hostname') == owner.get('hostname') and original.get('unit') == owner.get('unit')
            and original.get('binarySha256') == owner.get('binarySha256') == binding['candidateBinarySha256']
            and len(original.get('formats', [])) == 4
            and {part.get('disk') for part in original['formats']} == set(range(4))
            and physical_content(scene) == physical_content(one('I05-timeout-original-physical'))
                == physical_content(one('I05-timeout-baseline-physical-after-public-read')),
            'I05 stopped four-drive original physical scene changed')

    # Replay only the bounded seed ledger locally. No result is inferred from
    # current LISTs, and a missing, unknown or failed mutation refuses recovery.
    fixture = folder / 'fixture'
    names = {f'{i:06d}-{kind}.json' for i in range(1, 10) for kind in ('request', 'ack')}
    require({path.name for path in fixture.iterdir()} == names, 'I05 requires all nine seed request/ACK pairs')
    expected, journal, public_properties, seen, domain = [], {}, {}, set(), None
    for sequence in range(1, 10):
        request_path = fixture / f'{sequence:06d}-request.json'
        ack_path = fixture / f'{sequence:06d}-ack.json'
        request, ack = read(request_path), read(ack_path)
        operation = 'create_bucket' if sequence == 1 else 'put_bucket_versioning' if sequence == 2 else (
            'delete_object' if sequence == 6 else 'put_object')
        params = {'Bucket': bucket}
        if sequence == 2:
            params['VersioningConfiguration'] = {'Status': 'Enabled'}
        elif sequence >= 3:
            index, generation = (0, sequence - 3) if sequence <= 6 else (1, sequence - 7)
            key = f'objects/{index:04d}.bin'
            params['Key'] = key
            if operation == 'put_object':
                params.update(ContentType='application/octet-stream', Metadata={'heal-fixture': 'DV',
                              'nonce': f'{index}-{generation}'}, Tagging='suite=heal&fixture=DV')
        domain = domain or request.get('clockDomain')
        require(isinstance(domain, str) and bool(domain) and request.get('schemaVersion') == ack.get('schemaVersion') == 2
                and request.get('runId') == ack.get('runId') == run_id
                and request.get('sequence') == ack.get('sequence') == sequence
                and request.get('operation') == operation and request.get('params') == params
                and request.get('expectHttp') is None and request.get('identity') is None
                and request.get('clockDomain') == ack.get('clockDomain') == domain
                and ack.get('requestFile') == request_path.name
                and ack.get('requestSha256') == hashlib.sha256(request_path.read_bytes()).hexdigest()
                and ack.get('operationCheck') == 'PASS', 'I05 seed request/ACK binding differs')
        observation = ack.get('observation') or {}
        response = observation.get('response') or {}
        require(observation.get('operation') == operation and observation.get('params') == params
                and observation.get('endpoint') == 'node1' and observation.get('clockDomain') == domain
                and observation.get('clientCallInvoked') is True and observation.get('acknowledged') is True
                and observation.get('resultUnknown') is False and not observation.get('clientCleanupError')
                and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == (204 if sequence == 6 else 200)
                and response.get('ResponseMetadata', {}).get('RetryAttempts') == 0,
                'I05 seed contains an uncertain or rejected mutation')
        if sequence >= 3:
            version = response.get('VersionId', '')
            require(str(uuid.UUID(version)) == version and uuid.UUID(version).int and version not in seen,
                    'I05 seed has an invalid or repeated version')
            seen.add(version)
            entry = {'key': key, 'versionId': version, 'deleteMarker': sequence == 6,
                     'isLatest': sequence in (6, 9)}
            if sequence == 6:
                require(response.get('DeleteMarker') is True, 'I05 marker DELETE lacks its acknowledged marker')
            else:
                body = payload(f'{bucket}/{key}/{generation}', 32 * 1024**2 if index == 0 else 1024)
                expected_payload = {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
                require(request.get('payload') == observation.get('payload') == expected_payload,
                        'I05 seed payload differs from the deterministic request')
                require(isinstance(response.get('ETag'), str) and bool(response['ETag']),
                        'I05 seed PUT lacks its acknowledged ETag')
                public_properties[(key, version)] = {'ETag': response['ETag'], 'ContentLength': len(body),
                    'ContentType': params['ContentType'], 'Metadata': params['Metadata']}
                entry['payload'] = expected_payload
            expected.append(entry)
        for path in (request_path, ack_path):
            journal[path.relative_to(folder).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    baseline = one('original-ACK-bound-S3')
    require(baseline.get('bucket') == bucket and baseline == one('I05-timeout-original-S3'),
            'I05 original public baseline differs')
    verify_seed(expected, baseline)
    for entry in baseline['entries']:
        if entry['deleteMarker']:
            continue
        require(all(entry.get('properties', {}).get(field) == value
                    for field, value in public_properties[(entry['key'], entry['versionId'])].items())
                and entry.get('tags') == [{'Key': 'fixture', 'Value': 'DV'}, {'Key': 'suite', 'Value': 'heal'}],
                'I05 original public metadata, ETag or tags differ from seed request/ACK')

    timeout = folder / 'timeout'
    require({p.name for p in timeout.iterdir()} == {'scope.json', 'failure.json', 'caller'}
            and {p.name for p in (timeout / 'caller').iterdir()} == {'scope.json', 'failure.json', 'transport'},
            'I05 contender or later controller action may have been invoked')
    scope = read(timeout / 'scope.json')
    entry = scope.get('entry')
    require(entry in expected and entry.get('key') == 'objects/0000.bin' and not entry['isLatest']
            and entry.get('deleteMarker') is False and scope.get('runId') == run_id
            and scope.get('candidateCommit') == CANDIDATE
            and scope.get('bindingSha256') == hashlib.sha256(canonical(binding).encode()).hexdigest(),
            'I05 caller target is not its acknowledged historical seed')
    transport = timeout / 'caller/transport'
    require({p.name for p in transport.iterdir()} == {'request.json', 'request-written.json', 'disconnected.json'},
            'I05 caller transport contains another request or result')
    request, sent, closed = (read(transport / name) for name in ('request.json', 'request-written.json', 'disconnected.json'))
    target = f'/{bucket}/{entry["key"]}?versionId={entry["versionId"]}'
    require(request.get('operation') == 'get_object' and request.get('params') == {
                'Bucket': bucket, 'Key': entry['key'], 'VersionId': entry['versionId']}
            and request.get('endpoint') == 'node1' and request.get('endpointUrl') == binding['endpoints']['node1']
            and all(request.get(field) == binding[field] for field in (
                'candidateCommit', 'harnessCommit', 'candidatePackageSha256', 'candidateBinarySha256'))
            and request.get('bindingSha256') == scope['bindingSha256']
            and closed.get('clientSocketClosed') is True and closed.get('connectionAttempts') == 1
            and type(closed.get('clientClosedMonotonicNs')) is int
            and closed['clientClosedMonotonicNs'] >= sent.get('requestWrittenMonotonicNs', 0),
            'I05 transport is not the sole closed read-only historical GET')
    for value in (request, sent, closed):
        require(value.get('runId') == run_id and value.get('requestTarget') == target
                and value.get('callerMarker') == request.get('callerMarker')
                and re.fullmatch(r'auto-testing-i05/[0-9a-f-]{36}', value.get('callerMarker', ''))
                and value.get('clockDomain') == domain and value.get('retryAllowed') is False
                and value.get('canonicalCasePassClaimed') is False,
                'I05 closed caller lost its exact request identity')
    for value in (sent, closed):
        require(value.get('requestSha256') == hashlib.sha256((transport / 'request.json').read_bytes()).hexdigest()
                and value.get('requestWriteCompleted') is True
                and value.get('resultUnknown') is True and value.get('acknowledged') is False
                and value.get('serverOwnershipProven') is False,
                'I05 closed caller was retried or promoted to an acknowledged result')
    for path in timeout.rglob('*.json'):
        read(path)
        journal[path.relative_to(folder).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {'kind': 'unresolved-i05-timeout', 'runId': run_id, 'nodes': ['vm000'], 'bucket': bucket,
            'owners': {'vm000': owner}, 'candidateCommit': CANDIDATE, 'harnessCommit': HELPER,
            'binarySha256': binding['candidateBinarySha256'], 'ackFilesSha256': digest(journal),
            'businessSceneSha256': {'vm000': digest(physical_content(scene)['vm000'])}}
    validate(plan)
    return [plan]
