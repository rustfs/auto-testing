#!/usr/bin/env python3
"""I05 queued PUT after one caller disconnect; diagnostic subset only."""

import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote
import uuid

try:
    from . import lock_observer
    from .heal_probe import CLOCK_DOMAIN, Probe, canonical, payload, require, save, validate_binding
    from .heal_workload import Recorder, verify_seed
    from .healthy import physical_content
    from .lock_timeout import CALLER_RECEIVE_BUFFER_BYTES, MIN_BACKPRESSURE_BYTES, require_exact_inventory_entry
    from .shards import files, map_target
except ImportError:
    import lock_observer
    from heal_probe import CLOCK_DOMAIN, Probe, canonical, payload, require, save, validate_binding
    from heal_workload import Recorder, verify_seed
    from healthy import physical_content
    from lock_timeout import CALLER_RECEIVE_BUFFER_BYTES, MIN_BACKPRESSURE_BYTES, require_exact_inventory_entry
    from shards import files, map_target


SUCCESSOR_BYTES = 1024 * 1024 + 197
QUEUE_SECONDS = 10
COMPLETE_SECONDS = 30
PUT_TIMEOUT_MS = 60000


class SuccessorProbe(Probe):
    def __init__(self, original, marker):
        super().__init__(original.binding, original.endpoint_name, credential_pair=original.credential_pair)
        self.marker = marker

    def s3_client(self):
        client = super().s3_client()
        client.meta.events.register('before-sign.s3.PutObject', self.mark)
        return client

    def mark(self, request, **kwargs):
        value = request.headers.get('User-Agent', '')
        if isinstance(value, bytes):
            value = value.decode('ascii')
        request.headers['User-Agent'] = value + ' ' + self.marker


def successor_attempt(rows, marker, holder, bucket, key):
    """The caller marker selects one server request, never supplies its ID."""
    require(marker.startswith('auto-testing-i05-successor/')
            and str(uuid.UUID(marker.split('/')[-1])) == marker.split('/')[-1],
            'I05 successor marker differs')
    selected, request_ids, metadata_attempts = [], set(), {}
    for row in rows:
        if row.get('target') != lock_observer.TARGET:
            continue
        spans = [span for span in [row.get('span'), *row.get('spans', [])]
                 if isinstance(span, dict) and span.get('name') == 'http-request'
                 and marker in str(span.get('user_agent', '')).split()]
        if not spans:
            continue
        require(len({canonical(span) for span in spans}) == 1,
                'I05 successor has ambiguous HTTP ancestry')
        span = spans[0]
        request_id = span.get('request_id')
        require(isinstance(request_id, str) and str(uuid.UUID(request_id)) == request_id
                and uuid.UUID(request_id).int and request_id != holder['request_id']
                and row.get('request_id') == request_id and span.get('method') == 'PUT'
                and span.get('uri') == '/' + quote(bucket, safe='') + '/' + quote(key, safe='/~'),
                'I05 successor server request, method or exact URI differs')
        request_ids.add(request_id)
        # Slow-acquire warnings share the target but carry no lock lifecycle.
        # Bind this auxiliary row to the preceding acquired write observation.
        if row.get('message') == 'object namespace lock acquisition exceeded threshold':
            require(row.get('level') == 'WARN' and row.get('op') == 'put_object_commit'
                    and row.get('bucket') == bucket and row.get('object') == key
                    and row.get('mode') == 'write' and row.get('owner') == holder['owner']
                    and not any(name in row for name in ('event', 'component', 'subsystem', 'state',
                        'namespace', 'lock_attempt_id', 'timeout_ms', 'hold_ms', 'requested_version_id'))
                    and all(isinstance(row.get(name), str) and row[name].isascii()
                            and row[name].isdecimal() and str(int(row[name])) == row[name]
                            for name in ('acquire_ms', 'threshold_ms'))
                    and int(row['acquire_ms']) >= int(row['threshold_ms']) > 0
                    and selected and selected[-1]['state'] == 'acquired'
                    and selected[-1]['request_id'] == request_id
                    and int(row['acquire_ms']) >= selected[-1]['acquire_ms'],
                    'I05 successor slow-acquire warning lacks its exact acquired write')
            continue
        # Bucket fencing reads both persisted metadata objects within this PUT.
        # They share its HTTP identity, but cannot establish write admission.
        metadata_read = (row.get('mode') == 'read' and row.get('op') == 'get_object'
                         and row.get('bucket') == '.rustfs.sys'
                         and row.get('object') in {f'buckets/{bucket}/.bucket-incarnation',
                                                  f'buckets/{bucket}/.metadata.bin'})
        require(row.get('event') == 'object_namespace_lock' and row.get('component') == 'ecstore'
                and row.get('subsystem') == 'object_lock'
                and (metadata_read or row.get('op') == 'put_object_commit'
                     and row.get('bucket') == bucket and row.get('object') == key and row.get('mode') == 'write')
                and row.get('requested_version_id') is None and row.get('owner') == holder['owner']
                and row.get('namespace') == holder['namespace'] and type(row.get('timeout_ms')) is int
                and row['timeout_ms'] == (1000 if metadata_read else PUT_TIMEOUT_MS)
                and isinstance(row.get('lock_attempt_id'), str)
                and str(uuid.UUID(row['lock_attempt_id'])) == row['lock_attempt_id']
                and uuid.UUID(row['lock_attempt_id']).int
                and row['lock_attempt_id'] != holder['lock_attempt_id']
                and type(row.get('acquire_ms')) is int and row['acquire_ms'] >= 0,
                'I05 successor lock attempt escaped its exact namespace or budget')
        require(row.get('state') in {'acquiring', 'acquired', 'guard_dropped'}
                and row.get('level') == 'TRACE'
                and type(row.get('hold_ms')) is int and row['hold_ms'] >= 0,
                'I05 successor failed, cancelled or lost its diagnostic lifecycle')
        if metadata_read:
            metadata_attempts.setdefault(row['lock_attempt_id'], []).append(row)
        else:
            selected.append(row)
    for attempt_id, events in metadata_attempts.items():
        require(len({row['object'] for row in events}) == 1
                and not any(row['lock_attempt_id'] == attempt_id for row in selected)
                and [row['state'] for row in events] in (['acquiring'], ['acquiring', 'acquired'],
                                                       ['acquiring', 'acquired', 'guard_dropped']),
                'I05 successor metadata read reused an attempt or lost its lifecycle')
    require(len(request_ids) <= 1, 'I05 successor marker was retried or reused')
    if not selected:
        return None
    require(len({row['lock_attempt_id'] for row in selected}) == 1,
            'I05 successor made multiple target write attempts')
    states = [row['state'] for row in selected]
    require(states in (['acquiring'], ['acquiring', 'acquired'],
                       ['acquiring', 'acquired', 'guard_dropped']),
            'I05 successor write lifecycle is missing, duplicated or out of order')
    return {'requestId': selected[0]['request_id'], 'lockAttemptId': selected[0]['lock_attempt_id'],
            'states': states, 'events': selected, 'canonicalCasePassClaimed': False}


def validate_successor_ack(directory, probe, bucket, key, body, attempt):
    request_path = directory / '000001-request.json'
    ack_path = directory / '000001-ack.json'
    require({path.name for path in directory.iterdir()} == {request_path.name, ack_path.name},
            'I05 successor lacks exactly one original request and receipt')
    request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
    params = {'Bucket': bucket, 'Key': key, 'ContentType': 'application/octet-stream',
              'Metadata': {'heal-fixture': 'I05-queued-successor'}}
    digest = {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
    require(request.get('schemaVersion') == ack.get('schemaVersion') == 2
            and request.get('runId') == ack.get('runId') == probe.binding['runId']
            and request.get('sequence') == ack.get('sequence') == 1
            and request.get('operation') == 'put_object' and request.get('params') == params
            and request.get('payload') == digest and request.get('expectHttp') is None
            and request.get('identity') is None and ack.get('identity') is None
            and ack.get('requestFile') == request_path.name
            and ack.get('requestSha256') == hashlib.sha256(request_path.read_bytes()).hexdigest()
            and ack.get('operationCheck') == 'PASS'
            and request.get('casePassClaimed') is False and ack.get('casePassClaimed') is False,
            'I05 successor receipt differs from its exact durable PUT')
    observation = ack.get('observation', {})
    response = observation.get('response') or {}
    metadata = response.get('ResponseMetadata', {})
    version = response.get('VersionId')
    require(observation.get('operation') == 'put_object' and observation.get('params') == params
            and observation.get('endpoint') == probe.endpoint_name and observation.get('payload') == digest
            and observation.get('clientCallInvoked') is True and observation.get('acknowledged') is True
            and observation.get('resultUnknown') is False and not observation.get('clientCleanupError')
            and metadata.get('HTTPStatusCode') == 200 and type(metadata.get('RetryAttempts')) is int
            and metadata['RetryAttempts'] == 0 and metadata.get('RequestId') == attempt['requestId']
            and isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int
            and response.get('ETag') == '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"',
            'I05 successor lacks one known, unretried, content-bound version ACK')
    times = [request.get('preparedMonotonicNs'), ack.get('collectorCallStartedMonotonicNs'),
             observation.get('callStartedMonotonicNs'), observation.get('callFinishedMonotonicNs'),
             ack.get('collectorCallFinishedMonotonicNs'), ack.get('recordedMonotonicNs')]
    require(request.get('clockDomain') == ack.get('clockDomain') == observation.get('clockDomain') == CLOCK_DOMAIN
            and all(type(value) is int and value > 0 for value in times) and times == sorted(times),
            'I05 successor receipt lost its collector clock ordering')
    return {'key': key, 'versionId': version, 'isLatest': True, 'deleteMarker': False,
            'payload': digest, 'properties': {'ETag': response['ETag'], 'ContentLength': len(body),
                'ContentType': params['ContentType'], 'Metadata': params['Metadata']}, 'tags': []}


def physical_append(before, after, bucket, baseline, added):
    """Bind all old shards and the exact new version before any public read."""
    require(set(before) == set(after) and all(before[node]['formats'] == after[node]['formats'] for node in before),
            'I05 successor changed physical topology or formats')
    old, new = files(before), files(after)
    metadata_path = f"{bucket}/{added['key']}/xl.meta"
    for identity, entry in old.items():
        require(identity in new, 'I05 successor removed an original physical file')
        if identity[2] != metadata_path:
            require(new[identity] == entry, 'I05 successor changed an original shard or control')
    target = map_target(after, bucket, added['key'], 'data', 1, added['versionId'])
    old_dirs = set()
    for entry in baseline['entries']:
        if entry['key'] == added['key'] and not entry['deleteMarker']:
            original = map_target(before, bucket, entry['key'], 'data', 1, entry['versionId'], latest_required=False)
            old_dirs.add(original['dataDir'])
            retained = map_target(after, bucket, entry['key'], 'data', 1, entry['versionId'], latest_required=False)
            require(original['dataDir'] == retained['dataDir']
                    and {name: value for name, value in original['logical'].items() if name != 'isLatest'}
                    == {name: value for name, value in retained['logical'].items() if name != 'isLatest'}
                    and [(p['index'], p['sha256'], p['size']) for p in original['cohort']]
                    == [(p['index'], p['sha256'], p['size']) for p in retained['cohort']],
                    'I05 successor rewrote an acknowledged historical generation')
    require(target['dataDir'] not in old_dirs and target['logical']['size'] == added['payload']['size'],
            'I05 successor reused an old physical generation or wrong logical size')
    expected = sorted((entry['versionId'], entry['deleteMarker'], False if entry['key'] == added['key'] else entry['isLatest'])
                      for entry in baseline['entries'] if entry['key'] == added['key'])
    expected.append((added['versionId'], False, True))
    for identity, entry in new.items():
        if identity[2] == metadata_path:
            versions = entry['metadata']['versions']
            require(sorted((v['versionId'], v['deleted'], v['isLatest']) for v in versions) == sorted(expected),
                    'I05 physical version population differs from original and successor ACKs')
        if identity not in old:
            prefix = f"{bucket}/{added['key']}/{target['dataDir']}/"
            leaf = identity[2].removeprefix(prefix)
            require(identity[2].startswith(prefix)
                    and (leaf == 'part.1' or leaf.startswith('part.1.integrity.')
                         and str(uuid.UUID(leaf.removeprefix('part.1.integrity.')))
                         == leaf.removeprefix('part.1.integrity.')),
                    'I05 successor left an unacknowledged physical key or generation')
    return target


def observe_successor(probe, fleet, bucket, entry, output):
    lock_observer.validate_scope(probe, fleet, bucket, entry)
    require(fleet.runtime_profile == 'lock-successor', 'I05 queued successor requires its separate runtime budget')
    output = Path(output)
    output.mkdir(exist_ok=False)
    bound = canonical(probe.binding)
    marker = 'auto-testing-i05-successor/' + str(uuid.uuid4())
    save(output / 'scope.json', {'runId': fleet.run_id, 'entry': entry, 'marker': marker,
        'bindingSha256': hashlib.sha256(bound.encode()).hexdigest(), 'putTimeoutMs': PUT_TIMEOUT_MS,
        'queueSeconds': QUEUE_SECONDS, 'completeSeconds': COMPLETE_SECONDS,
        'canonicalCasePassClaimed': False})
    def binding():
        validate_binding(probe.binding)
        require(canonical(probe.binding) == bound, 'I05 successor binding changed')
    physical = fleet.snapshot('I05-successor-original-physical')
    baseline = fleet.record('I05-successor-original-S3', probe.inventory(bucket))
    require_exact_inventory_entry(baseline, bucket, entry)
    require(entry['payload']['size'] >= MIN_BACKPRESSURE_BYTES,
            'NOT_OBSERVED: I05 successor target cannot maintain response backpressure')
    require(physical_content(physical) == physical_content(fleet.snapshot('I05-successor-baseline-after-reads')),
            'I05 successor baseline reads changed original physical content')
    body = payload(f"{fleet.run_id}/I05-queued-successor", SUCCESSOR_BYTES)
    successor = SuccessorProbe(probe, marker)
    recorder = Recorder(successor, output / 'successor')
    future, final, target, after_ack = None, None, None, None
    def capture(window, sent, admitted):
        binding()
        rows = window.capture()
        node, holder = admitted['node'], admitted['attempt']
        attempts = {host: successor_attempt(events, marker, holder, bucket, entry['key'])
                    for host, events in rows.items()}
        require(all(value is None for host, value in attempts.items() if host != node),
                'I05 successor request appeared on another endpoint')
        return rows[node], lock_observer.caller_attempt(rows[node], sent, bucket, entry['key']), attempts[node]
    with ThreadPoolExecutor(max_workers=1) as pool:
        def queued(sent, admitted, window):
            nonlocal future
            binding()
            future = pool.submit(recorder.call, 'put_object', {'Bucket': bucket, 'Key': entry['key'],
                'ContentType': 'application/octet-stream', 'Metadata': {'heal-fixture': 'I05-queued-successor'}}, body)
            deadline = time.monotonic() + QUEUE_SECONDS
            while True:
                _, holder, attempt = capture(window, sent, admitted)
                require(holder is not None and holder['observedAcquiredWithoutDrop'],
                        'NOT_OBSERVED: I05 original GET ended before successor queue observation')
                if future.done():
                    future.result()
                    raise ValueError('NOT_OBSERVED: I05 successor completed before caller disconnect')
                require(time.monotonic() <= deadline, 'NOT_OBSERVED: I05 successor queue observation exceeded its budget')
                if attempt is not None:
                    require(attempt['states'] == ['acquiring'],
                            'I05 successor acquired before caller disconnect')
                    save(output / 'queued.json', attempt)
                    return attempt
                time.sleep(.02)
        def complete(sent, admitted, window):
            nonlocal final, target, after_ack
            require(sent.get('clientSocketClosed') is True and sent.get('shutdownCompleted') is True
                    and sent.get('resultUnknown') is True, 'I05 successor lost the original caller disconnect')
            deadline = time.monotonic() + COMPLETE_SECONDS
            while True:
                rows, holder, attempt = capture(window, sent, admitted)
                require(holder is not None, 'I05 successor lost its original GET request')
                require(time.monotonic() <= deadline, 'NOT_OBSERVED: I05 successor completion exceeded its budget')
                if future.done():
                    future.result()
                    if attempt is not None and attempt['states'] == ['acquiring', 'acquired', 'guard_dropped']:
                        require(holder['diagnosticGuardDropped'], 'I05 successor acquired without original GET guard drop')
                        drop = next(i for i, row in enumerate(rows) if row is holder['events'][-1])
                        acquire = next(i for i, row in enumerate(rows) if row is attempt['events'][1])
                        require(drop < acquire, 'I05 successor acquired before original GET diagnostic release')
                        added = validate_successor_ack(recorder.output, probe, bucket, entry['key'], body, attempt)
                        require(not any(e['versionId'] == added['versionId'] for e in baseline['entries']),
                                'I05 successor ACK reused an original version')
                        final = copy.deepcopy(baseline)
                        for item in final['entries']:
                            if item['key'] == entry['key']:
                                item['isLatest'] = False
                        final['entries'].append(added)
                        final['entries'].sort(key=lambda item: (item['key'], item['versionId']))
                        after = fleet.snapshot('I05-successor-after-ACK-before-public-read')
                        after_ack = physical_content(after)
                        target = physical_append(physical, after, bucket, baseline, added)
                        save(output / 'successor-complete.json', {**attempt, 'added': added,
                            'queuedBeforeDisconnectObserved': True, 'originalDiagnosticDropBeforeAcquire': True,
                            'heldAtDisconnectProven': False, 'canonicalCasePassClaimed': False})
                        return attempt
                time.sleep(.05)
        try:
            caller = lock_observer.observe_disconnect(probe, fleet, bucket, entry, output / 'caller',
                while_admitted=queued, after_disconnect=complete, receive_buffer_bytes=CALLER_RECEIVE_BUFFER_BYTES)
            before_reads = fleet.snapshot('I05-successor-before-all-peer-reads')
            require(after_ack == physical_content(before_reads),
                    'I05 successor first historical verification read changed physical content')
            inventories = {}
            for endpoint in probe.binding['endpoints']:
                binding()
                peer = Probe(probe.binding, endpoint, credential_pair=probe.credential_pair)
                observed = fleet.record('I05-successor-all-peer-S3', {'endpoint': endpoint, **peer.inventory(bucket)})
                retained = [item for item in final['entries'] if item['versionId'] != target['versionId']]
                actual_retained = {**observed, 'entries': [item for item in observed['entries']
                    if item['versionId'] != target['versionId']]}
                verify_seed(retained, actual_retained)
                wanted = next(item for item in final['entries'] if item['versionId'] == target['versionId'])
                actual = require_exact_inventory_entry(observed, bucket, wanted)
                require(actual.get('isLatest') is True and actual.get('tags') == []
                        and all(actual.get('properties', {}).get(name) == value
                                for name, value in wanted['properties'].items()),
                        'I05 successor public content, metadata, tags or current version differs from its ACK')
                inventories[endpoint] = observed
            require(physical_content(before_reads) == physical_content(fleet.snapshot('I05-successor-after-all-peer-reads')),
                    'I05 successor verification reads changed physical content')
            deadline = time.monotonic() + lock_observer.DRAIN_SECONDS
            while True:
                binding()
                resources = {node: fleet.record('I05-successor-final-resources', {
                    'node': node, **fleet.remote(node, 'process-metrics')}) for node in fleet.nodes}
                for node, resource in resources.items():
                    lock_observer.same_process(resource.get('identity'), caller['baselineResources'][node]['identity'])
                    require(type(resource.get('fileDescriptors')) is int and resource['fileDescriptors'] >= 0,
                            'I05 successor final descriptor census is incomplete')
                require(time.monotonic() <= deadline, 'NOT_OBSERVED: I05 successor final resource budget expired')
                if all(resources[node]['fileDescriptors'] <= caller['baselineResources'][node]['fileDescriptors']
                       for node in fleet.nodes):
                    break
                time.sleep(.2)
            result = {'diagnosticComplete': True, 'callerObservation': caller, 'expectedEntries': final['entries'],
                'verifiedEndpoints': list(inventories), 'finalResources': resources,
                'heldAtDisconnectProven': False, 'distributedReleaseProven': False,
                'noGlobalFdLeakProven': False, 'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False}
            save(output / 'diagnostic.json', result)
            return result
        except BaseException as error:
            save(output / 'failure.json', {'diagnosticComplete': False, 'errorType': type(error).__name__,
                'state': 'NOT_OBSERVED' if str(error).startswith('NOT_OBSERVED:') else 'INCOMPLETE',
                'canonicalCasePassClaimed': False, 'originalRequestsAndResponsesRetained': True})
            raise
        finally:
            # The original caller is closed by observe_disconnect even on an
            # observation failure. Drain the single SDK call before capture/stop.
            if future is not None:
                try:
                    future.result()
                except BaseException:
                    pass
