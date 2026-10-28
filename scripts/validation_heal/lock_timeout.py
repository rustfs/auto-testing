#!/usr/bin/env python3
"""I05 real PUT lock timeout behind an observed GET; diagnostic subset only."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

try:
    from . import lock_observer
    from .heal_probe import CLOCK_DOMAIN, MAX_BODY, Probe, canonical, compare_inventories, payload, require, save, validate_binding
    from .heal_workload import Recorder
    from .healthy import Fleet, physical_content
except ImportError:
    import lock_observer
    from heal_probe import CLOCK_DOMAIN, MAX_BODY, Probe, canonical, compare_inventories, payload, require, save, validate_binding
    from heal_workload import Recorder
    from healthy import Fleet, physical_content


OBSERVATION_SECONDS = 10
CONTENDER_BYTES = 257 * 1024 + 123
MIN_BACKPRESSURE_BYTES = 32 * 1024**2
CALLER_RECEIVE_BUFFER_BYTES = 64 * 1024


def require_exact_inventory_entry(inventory, bucket, entry):
    require(isinstance(inventory, dict), 'I05 timeout requires an S3 inventory object')
    entries = inventory.get('entries')
    pagination = inventory.get('pagination')
    require(inventory.get('bucket') == bucket and isinstance(entries, list) and entries
            and all(isinstance(row, dict) and isinstance(row.get('key'), str) and row['key']
                    and isinstance(row.get('versionId'), str) and row['versionId']
                    and type(row.get('deleteMarker')) is bool and type(row.get('isLatest')) is bool
                    for row in entries), 'I05 timeout inventory has incomplete version identities')
    identities = [(row['key'], row['versionId']) for row in entries]
    require(len(set(identities)) == len(entries)
            and all(sum(row['isLatest'] for row in entries if row['key'] == key) == 1
                    for key in {row['key'] for row in entries}),
            'I05 timeout inventory has duplicate versions or incomplete latest generations')
    for row in entries:
        content = row.get('payload')
        require(content is None if row['deleteMarker'] else
                isinstance(content, dict) and set(content) == {'size', 'sha256'}
                and type(content.get('size')) is int and 0 <= content['size'] <= MAX_BODY
                and isinstance(content.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', content['sha256']),
                'I05 timeout inventory lacks complete bounded payload digests')
    require(isinstance(pagination, dict)
            and all(isinstance(pagination.get(name), list) and pagination[name]
                    and all(type(size) is int and 0 <= size <= 1000 for size in pagination[name])
                    for name in ('versionPageSizes', 'currentPageSizes')),
            'I05 timeout inventory lacks complete typed pagination')
    require(type(inventory.get('objectsAndMarkers')) is int
            and inventory['objectsAndMarkers'] == len(entries)
            and type(inventory.get('bytesRead')) is int
            and inventory['bytesRead'] == sum(row.get('payload', {}).get('size', 0)
                for row in entries if not row.get('deleteMarker'))
            and sum(pagination['versionPageSizes']) == len(entries)
            and sum(pagination['currentPageSizes']) == sum(
                bool(row.get('isLatest') and not row.get('deleteMarker')) for row in entries),
            'I05 timeout requires a structurally complete cross-checked S3 inventory')
    matches = [row for row in entries if row.get('key') == entry.get('key')
               and row.get('versionId') == entry.get('versionId')]
    require(len(matches) == 1 and matches[0].get('deleteMarker') is False
            and matches[0].get('payload') == entry.get('payload'),
            'I05 timeout target is not the exact content verified in its complete S3 inventory')
    return matches[0]


def validate_timeout_ack(ack, request, request_bytes, run_id, endpoint_name, bucket, key, body,
                         *, clock_domain=CLOCK_DOMAIN):
    expected_params = {'Bucket': bucket, 'Key': key,
                       'Metadata': {'heal-fixture': 'I05-timeout-contender'}}
    expected_payload = {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
    require(ack.get('schemaVersion') == 2 and ack.get('runId') == run_id
            and ack.get('sequence') == 1 and ack.get('requestFile') == '000001-request.json'
            and ack.get('requestSha256') == hashlib.sha256(request_bytes).hexdigest()
            and ack.get('operationCheck') == 'PASS'
            and ack.get('casePassClaimed') is False
            and request.get('schemaVersion') == 2 and request.get('runId') == run_id
            and request.get('sequence') == 1 and request.get('operation') == 'put_object'
            and request.get('params') == expected_params and request.get('identity') is None
            and request.get('expectHttp') == 503
            and request.get('casePassClaimed') is False
            and request.get('payload') == expected_payload
            and ack.get('clockDomain') == request.get('clockDomain') == clock_domain,
            'I05 timeout ACK is not durably bound to the exact contender PUT')
    observation = ack.get('observation')
    require(isinstance(observation, dict) and observation.get('endpoint') == endpoint_name
            and observation.get('operation') == 'put_object' and observation.get('params') == expected_params
            and observation.get('payload') == expected_payload and observation.get('clockDomain') == clock_domain
            and observation.get('clientCallInvoked') is True
            and observation.get('acknowledged') is False and observation.get('resultUnknown') is False
            and observation.get('clientCleanupError') is None
            and type(observation.get('callStartedMonotonicNs')) is int
            and type(observation.get('callFinishedMonotonicNs')) is int
            and all(type(value) is int and value > 0 for value in (
                request.get('preparedMonotonicNs'), ack.get('collectorCallStartedMonotonicNs'),
                ack.get('collectorCallFinishedMonotonicNs'), ack.get('recordedMonotonicNs')))
            and request['preparedMonotonicNs'] <= ack['collectorCallStartedMonotonicNs']
                <= observation['callStartedMonotonicNs'] <= observation['callFinishedMonotonicNs']
                <= ack['collectorCallFinishedMonotonicNs'] <= ack['recordedMonotonicNs'],
            'I05 timeout PUT has an unknown transport result or incomplete durable refusal')
    response = observation.get('response')
    require(isinstance(response, dict), 'I05 timeout PUT ACK lacks its structured refusal')
    return response


def timeout_proof(rows, response, holder, bucket, key):
    """Reconcile one structured S3 refusal with its actual write-lock attempt."""
    metadata = response.get('ResponseMetadata', {})
    request_id = metadata.get('RequestId')
    # Exact candidate error.rs attaches this header to typed namespace timeouts.
    require(metadata.get('HTTPStatusCode') == 503 and type(metadata.get('RetryAttempts')) is int
            and metadata['RetryAttempts'] == 0
            and response.get('Error', {}).get('Code') == 'ServiceUnavailable'
            and metadata.get('HTTPHeaders', {}).get('retry-after') == '1'
            and isinstance(request_id, str) and str(uuid.UUID(request_id)) == request_id and uuid.UUID(request_id).int
            and request_id != holder['request_id'] and response.get('VersionId') is None,
            'I05 contender lacks an exact structured lock-timeout refusal')
    request_rows = [row for row in rows if row.get('target') == lock_observer.TARGET
                    and row.get('request_id') == request_id]
    require(all(row.get('mode') in ('read', 'write') for row in request_rows),
            'I05 contender emitted an unknown namespace lock mode')
    read_rows = [row for row in request_rows if row.get('mode') == 'read']
    write_rows = [row for row in request_rows if row.get('mode') == 'write']
    selected = [row for row in write_rows if row.get('op') == 'put_object_commit'
                and row.get('bucket') == bucket and row.get('object') == key]
    require(len(selected) == len(write_rows),
            'I05 contender made another namespace write-lock attempt')
    if read_rows:
        require([row.get('state') for row in read_rows] == ['acquiring', 'acquired', 'guard_dropped']
                and len({(row.get('owner'), row.get('namespace'), row.get('lock_attempt_id'))
                         for row in read_rows}) == 1,
                'I05 contender bucket-incarnation read lock has an incomplete lifecycle')
        for row in read_rows:
            require(row.get('event') == 'object_namespace_lock' and row.get('component') == 'ecstore'
                    and row.get('subsystem') == 'object_lock' and row.get('op') == 'get_object'
                    and row.get('bucket') == '.rustfs.sys'
                    and row.get('object') == f'buckets/{bucket}/.bucket-incarnation'
                    and row.get('namespace') == holder['namespace'] and row.get('owner') == holder['owner']
                    and row.get('requested_version_id') is None
                    and type(row.get('timeout_ms')) is int and row['timeout_ms'] == 1000
                    and type(row.get('acquire_ms')) is int and row['acquire_ms'] >= 0
                    and isinstance(row.get('lock_attempt_id'), str)
                    and str(uuid.UUID(row['lock_attempt_id'])) == row['lock_attempt_id']
                    and uuid.UUID(row['lock_attempt_id']).int,
                    'I05 contender bucket-incarnation read lock changed identity or budget')
    if not selected:
        return None
    require([row.get('state') for row in selected] in (['acquiring'], ['acquiring', 'failed']),
            'I05 contender acquired a lock or lost its exact timeout lifecycle')
    for row in selected:
        require(row.get('event') == 'object_namespace_lock' and row.get('component') == 'ecstore'
                and row.get('subsystem') == 'object_lock' and row.get('op') == 'put_object_commit'
                and row.get('mode') == 'write' and row.get('bucket') == bucket and row.get('object') == key
                and row.get('namespace') == holder['namespace'] and row.get('requested_version_id') is None
                and type(row.get('timeout_ms')) is int and row['timeout_ms'] == 1000
                and type(row.get('acquire_ms')) is int and row['acquire_ms'] >= 0
                # new_ns_lock reuses this SetDisks owner for same-node requests;
                # the server request and attempt UUIDs distinguish contenders.
                and row.get('owner') == holder['owner']
                and isinstance(row.get('lock_attempt_id'), str)
                and str(uuid.UUID(row['lock_attempt_id'])) == row['lock_attempt_id']
                and uuid.UUID(row['lock_attempt_id']).int and row['lock_attempt_id'] != holder['lock_attempt_id'],
                'I05 contender has another lock budget, object, namespace or owner')
        http_spans = [span for span in [row.get('span'), *row.get('spans', [])]
                      if isinstance(span, dict) and span.get('name') == 'http-request']
        if http_spans:
            unique_http_spans = {canonical(span): span for span in http_spans}
            require(len(unique_http_spans) == 1 and all(
                    span.get('request_id') == request_id and span.get('method') == 'PUT'
                    and span.get('uri') == f'/{bucket}/{key}' for span in unique_http_spans.values()),
                    'I05 contender lock attempt is not bound to its exact PUT request')
    require(len({(row['owner'], row['lock_attempt_id']) for row in selected}) == 1,
            'I05 contender changed its original write owner')
    if len(selected) == 1:
        return None
    start, end = selected
    require(start.get('level') == 'TRACE' and end.get('level') == 'ERROR' and end.get('failure') == 'timeout'
            and type(end.get('acquire_ms')) is int and end['acquire_ms'] >= end['timeout_ms'],
            'I05 contender did not observe actual contention beyond its registered budget')
    return {'requestId': request_id, 'lockAttemptId': end['lock_attempt_id'], 'owner': end['owner'],
            'namespace': end['namespace'], 'events': selected, 'typedTimeoutBeyondBudgetObserved': True,
            'writeLockAcquired': False, 'canonicalCasePassClaimed': False}


def observe_timeout(probe, fleet, bucket, entry, output):
    """Use existing owned service and fixtures; preserve every failed request."""
    lock_observer.validate_scope(probe, fleet, bucket, entry)
    output = Path(output)
    output.mkdir(exist_ok=False)
    bound = canonical(probe.binding)
    save(output / 'scope.json', {'runId': fleet.run_id, 'entry': entry,
        'candidateCommit': probe.binding['candidateCommit'],
        'bindingSha256': hashlib.sha256(bound.encode()).hexdigest(),
        'minimumCallerPayloadBytes': MIN_BACKPRESSURE_BYTES,
        'receiveBufferBytesRequested': CALLER_RECEIVE_BUFFER_BYTES,
        'canonicalCasePassClaimed': False})
    try:
        return _observe_timeout(probe, fleet, bucket, entry, output, bound)
    except BaseException as error:
        save(output / 'failure.json', {'diagnosticComplete': False, 'errorType': type(error).__name__,
            'state': 'NOT_OBSERVED' if str(error).startswith('NOT_OBSERVED:') else 'INCOMPLETE',
            'canonicalCasePassClaimed': False, 'originalRequestsAndResponsesRetained': True})
        raise


def _observe_timeout(probe, fleet, bucket, entry, output, bound):
    def binding():
        validate_binding(probe.binding)
        require(canonical(probe.binding) == bound, 'I05 timeout binding changed during observation')
    def unchanged_physical(label):
        binding()
        after = fleet.snapshot(label)
        require(physical_content(after) == physical_content(physical),
                'I05 rejected timeout PUT changed an original physical file or object generation')
        return {'originalFilesUnchanged': True, 'canonicalCasePassClaimed': False}
    physical = fleet.snapshot('I05-timeout-original-physical')
    before = fleet.record('I05-timeout-original-S3', probe.inventory(bucket))
    target = require_exact_inventory_entry(before, bucket, entry)
    require(target['payload']['size'] >= MIN_BACKPRESSURE_BYTES,
            'NOT_OBSERVED: I05 timeout target is too small to maintain response backpressure')
    unchanged_physical('I05-timeout-baseline-physical-after-public-read')
    recorder = Recorder(probe, output / 'contender')
    body = payload(f'{probe.binding["runId"]}/I05-timeout-contender', CONTENDER_BYTES)
    def contend(sent, admitted, window):
        binding()
        node, holder = admitted['node'], admitted['attempt']
        recorder.call('put_object', {'Bucket': bucket, 'Key': entry['key'],
            'Metadata': {'heal-fixture': 'I05-timeout-contender'}}, body, expect_http=503)
        ack_path = recorder.output / '000001-ack.json'
        request_path = recorder.output / '000001-request.json'
        request_bytes = request_path.read_bytes()
        response = validate_timeout_ack(json.loads(ack_path.read_text()),
            json.loads(request_bytes), request_bytes, probe.binding['runId'], probe.endpoint_name,
            bucket, entry['key'], body)
        deadline = time.monotonic() + OBSERVATION_SECONDS
        while True:
            binding()
            all_rows = window.capture()
            rows = all_rows[node]
            current = lock_observer.caller_attempt(rows, sent, bucket, entry['key'])
            require(current is not None and current['observedAcquiredWithoutDrop']
                    and all(current[name] == holder[name] for name in ('request_id', 'lock_attempt_id', 'owner', 'namespace')),
                    'NOT_OBSERVED: I05 original GET owner ended before contention was observed')
            foreign_request = [row for host, host_rows in all_rows.items() if host != node
                               for row in host_rows if row.get('request_id') == response.get(
                                   'ResponseMetadata', {}).get('RequestId')]
            require(not foreign_request, 'I05 contender request ID appeared on a foreign endpoint')
            proof = timeout_proof(rows, response, holder, bucket, entry['key'])
            require(time.monotonic() <= deadline, 'NOT_OBSERVED: I05 contender timeout observation exceeded its budget')
            if proof is not None:
                socket_state = fleet.record('I05-timeout-caller-backpressure-socket', {
                    'node': node, **fleet.remote(node, 'caller-socket',
                        expectedProcess=admitted['socket']['process'],
                        clientAddress=sent['localAddress'], serverAddress=sent['peerAddress'])})
                samples = socket_state.get('samples', [])
                require(socket_state.get('readOnly') is True
                        and socket_state.get('process') == admitted['socket']['process']
                        and socket_state.get('clockDomain') == admitted['socket']['clockDomain']
                        and socket_state.get('networkNamespace') == admitted['socket']['networkNamespace']
                        and len(samples) == 2
                        and all(len(sample.get('ownedMatches', [])) == 1
                                and sample['ownedMatches'][0].get('state') == '01'
                                and type(sample['ownedMatches'][0].get('txQueueBytes')) is int
                                and sample['ownedMatches'][0]['txQueueBytes'] > 0
                                and sample['vanishedDescriptors'] == 0 for sample in samples),
                        'NOT_OBSERVED: timed-out PUT did not overlap a backpressured original GET socket')
                current = lock_observer.caller_attempt(window.capture()[node], sent, bucket, entry['key'])
                require(current is not None and current['observedAcquiredWithoutDrop']
                        and all(current[name] == holder[name] for name in (
                            'request_id', 'lock_attempt_id', 'owner', 'namespace')),
                        'NOT_OBSERVED: original GET ended while sampling post-timeout backpressure')
                require(time.monotonic() <= deadline,
                        'NOT_OBSERVED: I05 backpressure census exceeded its observation budget')
                proof['backpressureSocket'] = socket_state
                proof['readResponseBackpressureObservedAfterTimeout'] = True
                proof.update(clockDomain=CLOCK_DOMAIN, observedMonotonicNs=time.monotonic_ns())
                save(output / 'timeout.json', proof)
                return proof
            time.sleep(.1)
    caller = lock_observer.observe_disconnect(probe, fleet, bucket, entry, output / 'caller',
        while_admitted=contend, receive_buffer_bytes=CALLER_RECEIVE_BUFFER_BYTES,
        before_verification=lambda: unchanged_physical('I05-timeout-final-physical-before-public-read'))
    binding()
    public = fleet.record('I05-timeout-final-S3', probe.inventory(bucket))
    require_exact_inventory_entry(public, bucket, entry)
    agreement = compare_inventories(before, public)
    unchanged_physical('I05-timeout-final-physical-after-public-read')
    deadline = time.monotonic() + lock_observer.DRAIN_SECONDS
    while True:
        binding()
        final_resources = {}
        for node in fleet.nodes:
            value = fleet.record('I05-timeout-final-resources', {
                'node': node, **fleet.remote(node, 'process-metrics')})
            lock_observer.same_process(value.get('identity'), caller['finalResources'][node]['identity'])
            require(type(value.get('fileDescriptors')) is int and value['fileDescriptors'] >= 0,
                    'I05 final timeout descriptor census is incomplete')
            final_resources[node] = value
        require(time.monotonic() <= deadline, 'NOT_OBSERVED: I05 final timeout FD census exceeded its budget')
        if all(value['fileDescriptors'] <= caller['baselineResources'][node]['fileDescriptors']
               for node, value in final_resources.items()):
            break
        time.sleep(.2)
    result = {'diagnosticComplete': True, 'callerObservation': caller, 'inventoryAgreement': agreement,
        'finalResources': final_resources, 'rejectedWriteChangedOriginalFiles': False,
        'timeoutContenderAcknowledged': False, 'canonicalCasePassClaimed': False, 'wholeCasePassClaimed': False,
        'remaining': ['same-source native exact cancellation/timeout/stale fences',
                      'complete source DL/DV workload and H1/H2 final ACK/physical/lock/FD closure']}
    save(output / 'diagnostic.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('--nodes', nargs='+', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--entry', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--endpoint')
    args = parser.parse_args()
    probe = Probe(json.loads(args.binding.read_text()), args.endpoint)
    args.output.mkdir(parents=True, exist_ok=False)
    fleet = Fleet(args.nodes, args.output, probe.binding['runId'], runtime_profile='lock-timeout')
    fleet.secret_values.update(probe.credentials())
    observe_timeout(probe, fleet, args.bucket, json.loads(args.entry.read_text()), args.output / 'timeout')


if __name__ == '__main__':
    main()
