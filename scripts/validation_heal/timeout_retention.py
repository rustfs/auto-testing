"""Isolate archived I05 H1/H2 failures with bounded, understood request effects."""

import hashlib
import json
import re
import uuid

try:
    from .heal_probe import canonical, payload, require
    from .heal_workload import verify_seed
    from .healthy import physical_content
    from .fleet_node import runtime_config
    from .lock_observer import CANDIDATE, same_process
    from .lock_timeout import CONTENDER_BYTES, timeout_proof, validate_timeout_ack
    from .race_retention import digest, validate
except ImportError:
    from heal_probe import canonical, payload, require
    from heal_workload import verify_seed
    from healthy import physical_content
    from fleet_node import runtime_config
    from lock_observer import CANDIDATE, same_process
    from lock_timeout import CONTENDER_BYTES, timeout_proof, validate_timeout_ack
    from race_retention import digest, validate


CASE = 'heal.lock-timeout-and-caller-abort'
PRE_CONTENDER_HELPER = '704bd606071676dded130dca91d3d5278455bcaa'
CONTENDER_HELPER = 'd100abeb8e85c4000a493aa994031e4a5d01518e'
H2_CONTENDER_HELPER = '1b88cad3581e755d8e904697cb2ec471e04b5f8f'
FAILURES = {
    PRE_CONTENDER_HELPER: {'type': 'ValueError',
        'message': 'caller lock diagnostic escaped its exact GET identity or runtime budget'},
    CONTENDER_HELPER: {'type': 'ValueError',
        'message': 'I05 contender acquired a lock or lost its exact timeout lifecycle'},
    H2_CONTENDER_HELPER: {'type': 'ValueError',
        'message': 'I05 contender acquired a lock or lost its exact timeout lifecycle'},
}
COMMON_LABELS = ('binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation', 'reserved',
          'inspector-upload', 'started', 'deployed-topology', 'actual-topology', 'original-ACK-bound-S3',
          'original-ACK-proof', 'I05-timeout-original-physical', 'I05-timeout-original-S3',
          'I05-timeout-baseline-physical-after-public-read', 'I05-caller-baseline-resources',
          'I05-caller-checkpoint')
LABELS = {
    PRE_CONTENDER_HELPER: COMMON_LABELS + ('I05-caller-window', 'quiescent',
        'original-failure-scene', 'original-failure-logs', 'diagnostic-result'),
    CONTENDER_HELPER: COMMON_LABELS + ('I05-caller-window', 'I05-caller-before-disconnect-socket',
        'I05-caller-window', 'I05-caller-admission-resources', 'I05-caller-window', 'quiescent',
        'original-failure-scene', 'original-failure-logs', 'diagnostic-result'),
    H2_CONTENDER_HELPER: COMMON_LABELS + ('I05-caller-window', 'I05-caller-before-disconnect-socket',
        'I05-caller-window', 'I05-caller-admission-resources', 'I05-caller-window', 'quiescent',
        'original-failure-scene', 'original-failure-logs', 'diagnostic-result'),
}


def h2_labels(window_rounds=(1, 1, 1)):
    """Exact journal shape when H1 cleans successfully and H2 fails in its contender window."""
    return ('binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation',
            *('reserved' for _ in range(4)), *('inspector-upload' for _ in range(4)),
            'started', 'deployed-topology', 'actual-topology', 'original-ACK-bound-S3',
            'original-ACK-proof', 'I05-timeout-original-physical', 'I05-timeout-original-S3',
            'I05-timeout-baseline-physical-after-public-read',
            *('I05-caller-baseline-resources' for _ in range(4)),
            *('I05-caller-checkpoint' for _ in range(4)),
            *('I05-caller-window' for _ in range(4 * window_rounds[0])), 'I05-caller-before-disconnect-socket',
            *('I05-caller-window' for _ in range(4 * window_rounds[1])),
            *('I05-caller-admission-resources' for _ in range(4)),
            *('I05-caller-window' for _ in range(4 * window_rounds[2])), 'quiescent', 'original-failure-scene',
            *('original-failure-logs' for _ in range(4)), 'diagnostic-result')


def archived_plan(artifact, failed_run, candidate):
    """Refuse uncertain effects, later actions, mixed cases and physical drift.

    This is preservation, with no S3 call, backup restoration or verdict change.
    A contender is accepted only for the reviewed helper's exact refused PUT.
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
            and provenance.get('testing_commit') in FAILURES,
            'I05 requires its original failed candidate and reviewed helper boundary')
    helper = provenance['testing_commit']
    failure = FAILURES[helper]
    evidence = paths[0].parents[1] / 'evidence' / CASE
    summary = read(evidence / 'healthy-summary.json')
    row = {'topology': 'H1', 'diagnosticComplete': False, 'cleanup': 'FAIL',
           'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False,
           'failure': failure, 'namespacesRetained': True}
    diagnostics = summary.get('diagnostics') or []
    if len(diagnostics) == 2 and diagnostics[0].get('topology') == 'H1':
        topology = 'H2'
        row = {**row, 'topology': 'H2'}
        h1 = diagnostics[0]
        h1_dir = evidence / 'H1'
        h1_results = list(h1_dir.glob('*-diagnostic-result.json'))
        require(helper in (CONTENDER_HELPER, H2_CONTENDER_HELPER)
                and h1.get('diagnosticComplete') is True and h1.get('cleanup') == 'PASS'
                and h1.get('wholeCasePassClaimed') is False and h1.get('canonicalCasePassClaimed') is False
                and len(h1_results) == 1,
                'I05 H2 isolation requires a completed and cleaned H1 diagnostic')
        h1_result = read(h1_results[0])
        require(h1_result.get('topology') == 'H1' and h1_result.get('diagnosticComplete') is True
                and h1_result.get('cleanup') == 'PASS'
                and h1_result.get('wholeCasePassClaimed') is False
                and h1_result.get('canonicalCasePassClaimed') is False,
                'I05 H1 diagnostic result does not prove completed cleanup')
        require(diagnostics[1] == row, 'I05 H2 failure differs from its exact known boundary')
    else:
        topology = 'H1'
        require(diagnostics == [row], 'I05 H1 failure differs from its exact known boundary')
    require(summary.get('caseId') == CASE and summary.get('cleanup') == 'FAIL'
            and summary.get('diagnostics') == diagnostics and summary.get('complete') is False
            and summary.get('diagnosticComplete') is False
            and summary.get('requestedTopologies') == ['H1', 'H2']
            and summary.get('canonicalCasePassClaimed') is False
            and (topology == 'H2' or not (evidence / 'H2').exists()),
            'I05 requires one retained failure and no canonical acceptance claim')
    folder = evidence / topology
    if topology == 'H2':
        archived_names = sorted(path.name for path in folder.iterdir())
        actual_labels = [re.sub(r'^\d{5}-', '', name)[:-5] for name in archived_names if name.endswith('.json')]
        checkpoint_end = actual_labels.index('I05-caller-checkpoint') + 4
        first_socket = actual_labels.index('I05-caller-before-disconnect-socket')
        first_admission = actual_labels.index('I05-caller-admission-resources')
        last_admission = first_admission + 4
        first_quiescent = actual_labels.index('quiescent')
        window_rounds = ((first_socket - checkpoint_end) // 4,
                         (first_admission - first_socket - 1) // 4,
                         (first_quiescent - last_admission) // 4)
        require(all(rounds >= 1 for rounds in window_rounds)
                and first_socket - checkpoint_end == 4 * window_rounds[0]
                and first_admission - first_socket - 1 == 4 * window_rounds[1]
                and first_quiescent - last_admission == 4 * window_rounds[2],
                'I05 H2 lock windows do not contain complete four-peer sampling rounds')
        labels = h2_labels(window_rounds)
    else:
        labels = LABELS[helper]
    numbered = [(label, f'{index:05d}-{label}.json') for index, label in enumerate(labels, 1)]
    require({path.name for path in folder.iterdir()} == {name for _, name in numbered} | {'fixture', 'timeout'},
            'I05 archive contains another action or lacks its original boundary')
    def one(label):
        matches = [name for current, name in numbered if current == label]
        require(len(matches) == 1, 'I05 archive lacks one exact ' + label)
        return read(folder / matches[0])
    def many(label):
        return [read(folder / name) for current, name in numbered if current == label]
    binding, scene = one('binding'), one('original-failure-scene')
    run_id, bucket = binding.get('runId', ''), binding.get('bucketPrefix', '')
    suffix = topology.lower()
    nodes = ['vm000'] if topology == 'H1' else ['vm000', 'vm001', 'vm002', 'vm003']
    endpoints = {f'node{index}': f'http://rustfs-node{index}:9000'
                 for index in range(1, len(nodes) + 1)}
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-{suffix}', run_id)
            and bucket == 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
            and binding.get('candidateCommit') == CANDIDATE
            and binding.get('candidatePackageSha256') == candidate['package_sha256']
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and binding.get('harnessCommit') == helper
            and binding.get('endpoints') == endpoints
            and binding.get('defaultEndpoint') == 'node1' and one('diagnostic-result') == row,
            'I05 original run, package, helper, binary or failure boundary differs')
    reservations = many('reserved')
    owners = {item.get('node'): item.get('owner') for item in reservations}
    require(len(reservations) == len(nodes) and set(owners) == set(nodes)
            and all(item.get('reserved') is True for item in reservations)
            and all(owner.get('runtimeProfile') == 'lock-timeout' for owner in owners.values())
            and set(scene) == set(nodes),
            'I05 requires every original reserved node and retained scene')
    owner = owners['vm000']
    windows = many('I05-caller-window')
    sample_counts = {node: sum(window.get('node') == node for window in windows) for node in nodes}
    baselines = {item.get('node'): item for item in many('I05-caller-baseline-resources')}
    if topology == 'H2':
        attestation = one('integrity-fleet-attestation')
        actual = one('actual-topology')
        uploads = {item.get('node'): item for item in many('inspector-upload')}
        require(attestation.get('nodes') == nodes and attestation.get('allParticipatingNodesUseCandidate') is True
                and actual.get('topology') == 'H2' and actual.get('nodes') == 4
                and actual.get('independentDrives') == 16
                and set(uploads) == set(nodes)
                and all(uploads[node].get('installed') is True
                        and uploads[node].get('inspectorSha256') == owners[node].get('inspectorSha256')
                        for node in nodes)
                and set(many('preflight')[0]) == set(nodes)
                and all(many('preflight')[0][node].get('hostname') == owners[node].get('hostname')
                        and many('preflight')[0][node].get('mounts') == owners[node].get('mounts')
                        for node in nodes)
                and set(many('deployed-topology')[0]) == set(nodes)
                and set(many('started')[0]) == set(nodes)
                and {item.get('node') for item in many('original-failure-logs')} == set(nodes)
                and {item.get('node') for item in baselines.values()} == set(nodes)
                and {item.get('node') for item in many('I05-caller-checkpoint')} == set(nodes)
                and {item.get('node') for item in many('I05-caller-admission-resources')} == set(nodes)
                and len(set(sample_counts.values())) == 1 and min(sample_counts.values()) >= 3,
                'I05 H2 evidence omits a peer, inspector upload, lock checkpoint or service log')
        admission = {item.get('node'): item for item in many('I05-caller-admission-resources')}
        logs = {item.get('node'): item for item in many('original-failure-logs')}
        require(set(baselines) == set(admission) == set(logs) == set(nodes)
                and all(type(baselines[node].get('fileDescriptors')) is int
                        and type(admission[node].get('fileDescriptors')) is int
                        and baselines[node].get('identity', {}).get('binarySha256') == binding['candidateBinarySha256']
                        and admission[node].get('identity', {}).get('binarySha256') == binding['candidateBinarySha256']
                        and baselines[node].get('identity', {}).get('settings') == runtime_config('lock-timeout')
                        and admission[node].get('identity', {}).get('settings') == runtime_config('lock-timeout')
                        and logs[node].get('unit') == owners[node].get('unit')
                        and isinstance(logs[node].get('journal'), str) and bool(logs[node]['journal'])
                        for node in nodes),
                'I05 H2 resource identity, runtime settings or original service logs differ')
        for node in nodes:
            same_process(admission[node].get('identity'), baselines[node].get('identity'))
    inspector = one('inspector-build')
    require(inspector.get('candidateCommit') == CANDIDATE
            and all(inspector.get('binarySha256') == value.get('inspectorSha256') for value in owners.values())
            and inspector.get('built') is True, 'I05 original inspector identity differs')
    for node in nodes:
        original = scene[node]
        node_owner = owners[node]
        require(original.get('quiescent') is True and type(original.get('pid')) is int and original['pid'] == 0
                and original.get('hostname') == node_owner.get('hostname') and original.get('unit') == node_owner.get('unit')
                and original.get('binarySha256') == node_owner.get('binarySha256') == binding['candidateBinarySha256']
                and len(original.get('formats', [])) == 4
                and {part.get('disk') for part in original['formats']} == set(range(4)),
                'I05 stopped four-drive original physical scene changed on ' + node)
    require(physical_content(scene) == physical_content(one('I05-timeout-original-physical'))
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
    expected_timeout_items = {'scope.json', 'failure.json', 'caller'}
    expected_caller_items = {'scope.json', 'failure.json', 'transport'}
    has_contender = helper in (CONTENDER_HELPER, H2_CONTENDER_HELPER)
    if has_contender:
        expected_timeout_items.add('contender')
        expected_caller_items.add('admission.json')
    require({p.name for p in timeout.iterdir()} == expected_timeout_items
            and {p.name for p in (timeout / 'caller').iterdir()} == expected_caller_items,
            'I05 archive contains an unreviewed action or lacks its exact failure boundary')
    timeout_failure = read(timeout / 'failure.json')
    caller_failure = read(timeout / 'caller/failure.json')
    require(timeout_failure.get('diagnosticComplete') is False
            and timeout_failure.get('errorType') == failure['type']
            and timeout_failure.get('state') == 'INCOMPLETE'
            and timeout_failure.get('canonicalCasePassClaimed') is False
            and timeout_failure.get('originalRequestsAndResponsesRetained') is True
            and caller_failure.get('observationComplete') is False
            and caller_failure.get('errorType') == failure['type']
            and caller_failure.get('state') == 'INCOMPLETE'
            and caller_failure.get('canonicalCasePassClaimed') is False
            and caller_failure.get('originalTransportEvidenceRetained') is True,
            'I05 nested failure records differ from the exact retained diagnostic failure')
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
    if has_contender:
        contender = timeout / 'contender'
        require({p.name for p in contender.iterdir()} == {'000001-request.json', '000001-ack.json'},
                'I05 contender archive is not the sole one-shot request and ACK')
        request_path, ack_path = contender / '000001-request.json', contender / '000001-ack.json'
        request_bytes = request_path.read_bytes()
        request, ack = read(request_path), read(ack_path)
        body = payload(f'{run_id}/I05-timeout-contender', CONTENDER_BYTES)
        response = validate_timeout_ack(ack, request, request_bytes, run_id, 'node1', bucket,
                                        entry['key'], body, clock_domain=domain)
        require(ack.get('operationCheck') == 'PASS'
                and ack.get('observation', {}).get('acknowledged') is False
                and ack.get('observation', {}).get('resultUnknown') is False,
                'I05 contender result is not a durable, known refusal')
        holder = (read(timeout / 'caller/admission.json').get('attempt') or {})
        require(holder.get('admitted') is True and holder.get('observedAcquiredWithoutDrop') is True,
                'I05 contender lacks its original admitted GET lock holder')
        require(set(baselines) == set(nodes) and len(windows) == sample_counts['vm000'] * len(nodes),
                'I05 contender archive lacks each peer journal window')
        rows = {node: [] for node in nodes}
        previous_cursors = {}
        window_counts = {node: 0 for node in nodes}
        for window in windows:
            node = window.get('node')
            require(node in rows and window.get('complete') is True
                    and window.get('unit') == owners[node]['unit'] and not window.get('sourceMessageLoss')
                    and isinstance(window.get('beforeCursor'), str)
                    and isinstance(window.get('afterCursor'), str)
                    and (node not in previous_cursors or window['beforeCursor'] == previous_cursors[node])
                    and isinstance(window.get('journal'), str),
                    'I05 contender lock journal is incomplete or belongs to another service')
            previous_cursors[node] = window['afterCursor']
            window_counts[node] += 1
            for line in window['journal'].splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError('I05 contender lock journal contains malformed JSON') from error
                require(isinstance(event, dict), 'I05 contender lock journal contains a non-object event')
                rows[node].append(event)
        require(len(set(window_counts.values())) == 1 and min(window_counts.values()) >= 3,
                'I05 contender lock windows are incomplete or imbalanced across H2 peers')
        require(all(not any(event.get('request_id') == response.get('ResponseMetadata', {}).get('RequestId')
                            for event in events) for node, events in rows.items() if node != 'vm000'),
                'I05 contender request ID appeared on a foreign endpoint')
        proof = timeout_proof(rows['vm000'], response, holder, bucket, entry['key'])
        require(proof is not None and proof.get('typedTimeoutBeyondBudgetObserved') is True
                and proof.get('writeLockAcquired') is False,
                'I05 contender ACK lacks its exact failed target write-lock attempt')
    for path in timeout.rglob('*.json'):
        read(path)
        journal[path.relative_to(folder).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {'kind': 'unresolved-i05-timeout', 'runId': run_id, 'nodes': nodes, 'bucket': bucket,
            'owners': owners, 'candidateCommit': CANDIDATE, 'harnessCommit': helper,
            'binarySha256': binding['candidateBinarySha256'], 'ackFilesSha256': digest(journal),
            'businessSceneSha256': {node: digest(physical_content(scene)[node]) for node in nodes}}
    validate(plan)
    return [plan]
