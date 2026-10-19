"""I03 DL request-bound fixtures and bounded concurrent calls; no repair verdict."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import threading
import time

try:
    from .heal_probe import payload, require, validate_s3_request
    from .heal_workload import Recorder
    from .network_fixtures import MPU_MINIMUM, MPU_SOURCE
except ImportError:
    from heal_probe import payload, require, validate_s3_request
    from heal_workload import Recorder
    from network_fixtures import MPU_MINIMUM, MPU_SOURCE


BODY_SIZE = 512 * 1024 + 97
FIRST_PART_SIZE = MPU_MINIMUM + 17
FINAL_PART_SIZE = 211
ADMISSION_SECONDS = 180
SOURCE_FILES = {
    '072b57b37559fbe626aeda8abf61617fdcbdd926': {
        'rustfs/src/error.rs': '656d38972c06339015c174405571a94f19d8dd64a680e1a7ca631f80205f99a7',
        'crates/ecstore/src/set_disk/ops/object.rs': 'f0f407509b5a12803a191cc1c3a9eb079a15a80a4bbcacca70141150364628ca'},
    '1c7fc818dd858247c45b39542abd8291b1e6e31b': {
        'rustfs/src/error.rs': 'e7cff0e28978b49d3ee0e8c24da1fa439b3118a7d3f7304edf00dc0535691598',
        'crates/ecstore/src/set_disk/ops/object.rs': 'fdcfda1a8811a93537f9cd47cadac7380bee66b3d14242ca91500bbf38e6bedb'},
}
MULTIPART_SOURCE = '1ac5c17a94845fb16f6d75b8d3692cb18f81556086d4cc4a96b63deae106ab8d'
MPU_LAYOUT_SOURCE = '479a3b3f8c9f7b43b25b3f41d1b6769a67c6dc58c15845331da87e1fbc762679'
DISK_SOURCE = {
    '072b57b37559fbe626aeda8abf61617fdcbdd926': '7ab3137947d0a4f9007a40abc3e1735ec83ca820f6dc03a3a9426ae1b4f096d7',
    '1c7fc818dd858247c45b39542abd8291b1e6e31b': 'cd045a322ad7e41e2253ac08554f10634192345cbd923f2854553a229c036f4b',
}
MUTATIONS = frozenset({'put_object', 'copy_object', 'delete_object', 'create_multipart_upload',
    'upload_part', 'complete_multipart_upload', 'abort_multipart_upload'})
READS = frozenset({'get_object', 'head_object', 'list_objects_v2', 'list_object_versions',
    'list_multipart_uploads', 'list_parts'})


SOURCE_FILES['85c706cac09d53d0649487b8f1865f18beb53225'] = {
    **SOURCE_FILES['1c7fc818dd858247c45b39542abd8291b1e6e31b'],
    'rustfs/src/error.rs': 'c6a8d569b295659cf5a26a44bb3d8ba809c4ca3405d87525624699d161e08970',
    'rustfs/src/storage/ecfs.rs': '68d8ae2428504010adeb7bc51d2309496260898e4c74e5577a0af4f1bfe13c85'}
DISK_SOURCE['85c706cac09d53d0649487b8f1865f18beb53225'] = DISK_SOURCE['1c7fc818dd858247c45b39542abd8291b1e6e31b']


def source_reference(candidate):
    require(candidate in SOURCE_FILES, 'DL operations are not reviewed for this exact candidate')
    return {'candidateCommit': candidate, 'sourceFiles': {**SOURCE_FILES[candidate],
        'crates/ecstore/src/set_disk/mod.rs': MPU_SOURCE[candidate],
        'crates/ecstore/src/set_disk/metadata.rs': MPU_LAYOUT_SOURCE,
        'crates/ecstore/src/disk/mod.rs': DISK_SOURCE[candidate],
        'crates/ecstore/src/set_disk/ops/multipart.rs': MULTIPART_SOURCE},
        'multipartLayout': {'systemBucket': '.rustfs.sys/multipart', 'keyRoot': 'sha256(bucket + "/" + object)',
                            'uploadIdDecodedForScope': False},
        'negativeObjectMutation': {'httpStatus': 503, 'code': 'ServiceUnavailable'},
        'abortRefusalIsNotRollback': True, 'listDuringPartitionIsNotGlobalCompleteness': True,
        'maximumWorkers': 4, 'admissionSeconds': ADMISSION_SECONDS,
        'automaticMutationRetries': 0, 'serverCommitFenceClaimed': False}


def digest(body):
    return {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}


def binding_digest(binding):
    return hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def control_body(bucket, key, nonce):
    return payload(f'{bucket}/{key}/{nonce}', BODY_SIZE)


def part_body(bucket, key, number, size):
    return payload(f'{bucket}/{key}/part-{number}', size)


def seed_load(recorder, bucket, endpoints):
    reference = source_reference(recorder.probe.binding['candidateCommit'])
    require(len(endpoints) == len(set(endpoints)) == 2
            and set(endpoints) <= set(recorder.probe.binding['endpoints']), 'DL requires the two actual partition-side endpoints')
    require(recorder.call('get_bucket_versioning', {'Bucket': bucket}).get('Status') == 'Enabled',
            'DL requires the original enabled versioned bucket')
    expected, sides, seen_uploads = [], {}, set()
    for endpoint in endpoints:
        key = f'dl/{endpoint}/control.bin'
        versions, heads = [], {}
        for nonce in range(3):
            body = control_body(bucket, key, nonce)
            response = recorder.call('put_object', {'Bucket': bucket, 'Key': key,
                'Metadata': {'heal-fixture': 'DL', 'nonce': str(nonce)}, 'Tagging': 'suite=heal&fixture=DL',
                'ContentType': 'application/octet-stream'}, body, identity={'endpoint': endpoint, 'phase': 'seed', 'nonce': nonce})
            version = response.get('VersionId')
            require(isinstance(version, str) and version not in {'', 'null'}
                    and version not in {entry['versionId'] for entry in versions} and response.get('ETag'),
                    'DL control lacks a distinct exact PUT version/ETag ACK')
            entry = {'key': key, 'versionId': version, 'deleteMarker': False,
                'isLatest': nonce == 2, 'payload': digest(body)}
            versions.append(entry)
            expected.append(entry)
            head = recorder.call('head_object', {'Bucket': bucket, 'Key': key, 'VersionId': version})
            require(head.get('VersionId') == version and head.get('ContentLength') == len(body)
                    and head.get('ETag') == response['ETag'] and head.get('LastModified')
                    and head.get('Metadata') == {'heal-fixture': 'DL', 'nonce': str(nonce)},
                    'DL original HEAD does not redeem its exact data-version ACK')
            heads[version] = json.loads(json.dumps({name: head.get(name) for name in
                ('ContentLength', 'ContentType', 'ETag', 'LastModified', 'Metadata')}, default=str))
        uploads = {}
        for name, sizes in [('complete', [FIRST_PART_SIZE, FINAL_PART_SIZE]),
                            ('upload', [FIRST_PART_SIZE]), ('abort', [FINAL_PART_SIZE])]:
            upload_key = f'dl/{endpoint}/{name}.bin'
            response = recorder.call('create_multipart_upload', {'Bucket': bucket, 'Key': upload_key,
                'Metadata': {'heal-fixture': 'DL', 'nonce': name}, 'ContentType': 'application/octet-stream'})
            upload_id = response.get('UploadId')
            require(isinstance(upload_id, str) and upload_id and upload_id not in seen_uploads,
                    'DL MPU lacks a distinct exact CreateMultipartUpload ACK')
            seen_uploads.add(upload_id)
            parts, full_digest, total = [], hashlib.sha256(), 0
            for number, size in enumerate(sizes, 1):
                body = part_body(bucket, upload_key, number, size)
                response = recorder.call('upload_part', {'Bucket': bucket, 'Key': upload_key,
                    'UploadId': upload_id, 'PartNumber': number}, body)
                require(isinstance(response.get('ETag'), str) and response['ETag'], 'DL part ACK omitted its ETag')
                parts.append({'PartNumber': number, 'ETag': response['ETag'], 'payload': digest(body)})
                full_digest.update(body)
                total += size
            uploads[name] = {'key': upload_key, 'uploadId': upload_id, 'parts': parts,
                'completePayload': {'size': total, 'sha256': full_digest.hexdigest()}}
        sides[endpoint] = {'controlKey': key, 'versions': versions, 'heads': heads, 'uploads': uploads}
    return {'bucket': bucket, 'runId': recorder.probe.binding['runId'], 'bindingSha256': binding_digest(recorder.probe.binding),
        'sourceContract': reference,
        'expected': expected, 'sides': sides,
        'seedPayloadBytes': recorder.bytes_written, 'canonicalCasePassClaimed': False}


def mutation_steps(fixture, endpoint):
    bucket, side = fixture['bucket'], fixture['sides'][endpoint]
    key, versions, uploads = side['controlKey'], side['versions'], side['uploads']
    steps = []
    for nonce in (3, 4):
        steps.append({'operation': 'put_object', 'params': {'Bucket': bucket, 'Key': key,
            'ContentType': 'application/octet-stream', 'Metadata': {'heal-fixture': 'DL', 'nonce': str(nonce)},
            'Tagging': 'suite=heal&fixture=DL'}, 'body': control_body(bucket, key, nonce), 'nonce': nonce})
    steps.extend([
        {'operation': 'copy_object', 'params': {'Bucket': bucket, 'Key': f'dl/{endpoint}/copy.bin',
            'CopySource': {'Bucket': bucket, 'Key': key, 'VersionId': versions[0]['versionId']},
            'MetadataDirective': 'REPLACE', 'Metadata': {'heal-fixture': 'DL', 'nonce': '5'},
            'TaggingDirective': 'REPLACE', 'Tagging': 'suite=heal&fixture=DL'}, 'nonce': 5},
        {'operation': 'delete_object', 'params': {'Bucket': bucket, 'Key': key, 'VersionId': versions[0]['versionId']}, 'nonce': 6},
        {'operation': 'create_multipart_upload', 'params': {'Bucket': bucket, 'Key': f'dl/{endpoint}/create.bin',
            'Metadata': {'heal-fixture': 'DL', 'nonce': '7'}}, 'nonce': 7},
        {'operation': 'upload_part', 'params': {'Bucket': bucket, 'Key': uploads['upload']['key'],
            'UploadId': uploads['upload']['uploadId'], 'PartNumber': 2},
            'body': part_body(bucket, uploads['upload']['key'], 2, FINAL_PART_SIZE), 'nonce': 8},
        {'operation': 'complete_multipart_upload', 'params': {'Bucket': bucket, 'Key': uploads['complete']['key'],
            'UploadId': uploads['complete']['uploadId'], 'MultipartUpload': {'Parts': [
                {name: part[name] for name in ('PartNumber', 'ETag')} for part in uploads['complete']['parts']]}}, 'nonce': 9},
        {'operation': 'abort_multipart_upload', 'params': {'Bucket': bucket, 'Key': uploads['abort']['key'],
            'UploadId': uploads['abort']['uploadId']}, 'nonce': 10},
    ])
    return steps


def read_steps(fixture, endpoint):
    bucket, side = fixture['bucket'], fixture['sides'][endpoint]
    current = side['versions'][-1]
    params = {'Bucket': bucket, 'Key': current['key'], 'VersionId': current['versionId']}
    upload = side['uploads']['complete']
    return [
        {'operation': 'get_object', 'params': params},
        {'operation': 'head_object', 'params': params},
        {'operation': 'list_objects_v2', 'params': {'Bucket': bucket, 'Prefix': f'dl/{endpoint}/', 'MaxKeys': 32}},
        {'operation': 'list_object_versions', 'params': {'Bucket': bucket, 'Prefix': side['controlKey'], 'MaxKeys': 32}},
        {'operation': 'list_multipart_uploads', 'params': {'Bucket': bucket, 'Prefix': f'dl/{endpoint}/', 'MaxUploads': 32}},
        {'operation': 'list_parts', 'params': {'Bucket': bucket, 'Key': upload['key'], 'UploadId': upload['uploadId'], 'MaxParts': 32}},
    ]


def read_receipts(directory, binding, endpoint):
    """Reconstruct from original durable bytes, including requests with no ACK."""
    require(endpoint in binding['endpoints'], 'unknown DL endpoint ownership')
    directory = Path(directory)
    requests, acks = sorted(directory.glob('*-request.json')), sorted(directory.glob('*-ack.json'))
    require({p.name for p in acks} <= {p.name.replace('-request.json', '-ack.json') for p in requests},
            'DL has a detached ACK without its exact request')
    rows = []
    for index, path in enumerate(requests, 1):
        request = json.loads(path.read_text())
        require(path.name == f'{index:06d}-request.json' and type(request['sequence']) is int and request['sequence'] == index
                and request.get('schemaVersion') == 2
                and request['runId'] == binding['runId'], 'DL request sequence/run ownership differs')
        validate_s3_request(binding, request['operation'], request['params'])
        row = {'request': request, 'requestFile': path.name, 'requestSha256': hashlib.sha256(path.read_bytes()).hexdigest(),
               'prepared': True, 'ackPresent': False, 'resolution': 'UNKNOWN_PREPARED_REQUEST'}
        ack_path = path.with_name(path.name.replace('-request.json', '-ack.json'))
        if ack_path.exists():
            ack_bytes = ack_path.read_bytes()
            ack = json.loads(ack_bytes)
            require(ack.get('schemaVersion') == 2 and ack['requestFile'] == path.name and ack['requestSha256'] == row['requestSha256']
                    and ack['runId'] == request['runId'] and type(ack['sequence']) is int and ack['sequence'] == index
                    and ack['identity'] == request['identity'] and ack['clockDomain'] == request['clockDomain'],
                    'DL ACK is not bound to its exact durable request')
            observed = ack['observation']
            require(observed.get('resultUnknown') is True or observed.get('operation') == request['operation']
                    and observed.get('params') == request['params'] and observed.get('endpoint') == endpoint,
                    'DL known observation lost its actual operation/selector/endpoint')
            require(all(type(value) is int for value in (request['preparedMonotonicNs'],
                        ack['collectorCallStartedMonotonicNs'], ack['collectorCallFinishedMonotonicNs'], ack['recordedMonotonicNs']))
                    and request['preparedMonotonicNs'] < ack['collectorCallStartedMonotonicNs']
                    <= ack['collectorCallFinishedMonotonicNs'] <= ack['recordedMonotonicNs'],
                    'DL collector timing differs from its original clock domain')
            if observed.get('clientCallInvoked') is True:
                require(observed.get('clockDomain') == request['clockDomain']
                        and type(observed.get('callStartedMonotonicNs')) is int
                        and type(observed.get('callFinishedMonotonicNs')) is int
                        and ack['collectorCallStartedMonotonicNs'] <= observed['callStartedMonotonicNs']
                        < observed['callFinishedMonotonicNs'] <= ack['collectorCallFinishedMonotonicNs'],
                        'DL actual client interval escaped the original collection window')
            row.update(ackPresent=True, ackFile=ack_path.name, ackSha256=hashlib.sha256(ack_bytes).hexdigest(),
                       ack=ack, resolution='UNKNOWN_RESULT' if observed.get('resultUnknown') is True
                       else 'REQUIRES_INDEPENDENT_RECONCILIATION')
        rows.append(row)
    return rows


def read_journal(directory, binding, endpoint, role):
    require(role in {'writer', 'reader'}, 'unknown DL worker role')
    rows = read_receipts(directory, binding, endpoint)
    for index, row in enumerate(rows, 1):
        request, identity = row['request'], row['request']['identity']
        require(isinstance(identity, dict) and identity.get('endpoint') == endpoint and identity.get('role') == role
                and type(identity.get('step')) is int and identity['step'] == index,
                'DL request worker ownership or ordinal differs')
        require(request['operation'] in (MUTATIONS if role == 'writer' else READS), 'DL operation escaped its worker role')
    return rows


class LoadJournal(Recorder):
    """One worker serializes calls; the base Recorder persists every raw ACK."""
    def __init__(self, probe, output, role, *, writer=None):
        require(role in {'writer', 'reader'}, 'invalid DL worker role')
        super().__init__(probe, output, writer=writer)
        self.role, self.serial, self.attempted = role, threading.Lock(), 0

    def request(self, step, expected_http, deadline):
        with self.serial:
            self.attempted += 1
            if self.mutations.poisoned is not None:
                return {'attempt': self.attempted, 'operation': step['operation'], 'row': None,
                        'error': {'type': 'NotAdmitted', 'message': 'DL worker stopped after its original failure'}}
            error = None
            try:
                super().call(step['operation'], step['params'], step.get('body'),
                    identity={'endpoint': self.probe.endpoint_name, 'role': self.role,
                        'step': self.attempted, 'nonce': step.get('nonce')},
                    expect_http=expected_http, admission_deadline=deadline)
            except Exception as exc:
                error = {'type': type(exc).__name__, 'message': str(exc)}
                self.mutations.poison('DL original call failed; no further calls')
            rows = read_journal(self.output, self.probe.binding, self.probe.endpoint_name, self.role)
            # No new durable request means no invocation by Recorder. Do not
            # reuse the previous call's receipt after admission/validation fails.
            row = rows[-1] if rows and rows[-1]['request']['identity']['step'] == self.attempted else None
            return {'attempt': self.attempted, 'operation': step['operation'], 'error': error, 'row': row}


def validate_planned_request(request, fixture, endpoint, *, readable):
    operation = request['operation']
    role, ordinal = request['identity']['role'], request['identity']['step']
    steps = mutation_steps(fixture, endpoint) if role == 'writer' else read_steps(fixture, endpoint)
    require(type(ordinal) is int and 1 <= ordinal <= len(steps), 'DL request escaped its exact planned ordinal')
    step = steps[ordinal - 1]
    expected_http = 503 if role == 'writer' else [200, 503] if operation.startswith('list_') else 200 if readable else 503
    require(operation == step['operation'] and request['params'] == step['params']
            and request['identity']['nonce'] == step.get('nonce') and request['expectHttp'] == expected_http
            and (request.get('payload') == digest(step['body']) if 'body' in step else 'payload' not in request),
            'DL original selector/nonce/body/status policy differs from its planned request')


def validate_call(result, fixture, endpoint, *, readable):
    row = result['row']
    require(row is not None and row['ackPresent'], 'DL call has no exact durable ACK; retain its original request')
    observation = row['ack']['observation']
    request, operation = row['request'], row['request']['operation']
    validate_planned_request(request, fixture, endpoint, readable=readable)
    response = observation.get('response') or {}
    status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    require(observation.get('clientCallInvoked') is True and observation.get('resultUnknown') is False
            and row['ack'].get('operationCheck') == 'PASS'
            and not observation.get('clientCleanupError') and type(status) is int
            and observation.get('acknowledged') is (200 <= status < 300),
            'DL request is uninvoked, uncertain, inconsistent or incompletely collected')
    if operation in MUTATIONS:
        require(status == 503 and response.get('Error', {}).get('Code') == 'ServiceUnavailable',
                'DL W/lock-negative mutation returned an unsafe ACK or a different frozen refusal')
        return {'kind': 'REFUSED_REQUIRES_RECONCILIATION', 'originalInvocationProved': True,
                'rollbackClaimed': False}
    if status == 503:
        code = response.get('Error', {}).get('Code')
        require((operation == 'head_object' and code == '503')
                or operation == 'get_object' and code == 'ServiceUnavailable'
                or operation.startswith('list_') and code in {'ServiceUnavailable', 'SlowDownRead'},
                'DL read lacks its exact structured candidate refusal')
        require(not readable or operation.startswith('list_'), 'DL lost its R-capable stable current control')
        require('payload' not in observation, 'refused DL read returned a payload')
        return {'kind': 'READ_REFUSED', 'globalCompletenessClaimed': False}
    require(status == 200, 'DL read returned an unexpected S3 status')
    current = fixture['sides'][endpoint]['versions'][-1]
    if operation in {'get_object', 'head_object'}:
        require(readable and request['params']['VersionId'] == current['versionId']
                and response.get('VersionId') == current['versionId']
                and response.get('ContentLength') == current['payload']['size'], 'DL stable version identity or length differs')
        if operation == 'get_object':
            require(observation.get('payload') == current['payload'], 'DL GET changed the complete original payload')
        else:
            require({name: response.get(name) for name in fixture['sides'][endpoint]['heads'][current['versionId']]}
                    == fixture['sides'][endpoint]['heads'][current['versionId']],
                    'DL HEAD changed an original ACK property or Last-Modified')
    else:
        require(operation in READS and response.get('IsTruncated') is False, 'DL scoped listing is incomplete or unrecognized')
        if operation == 'list_parts':
            wanted = fixture['sides'][endpoint]['uploads']['complete']
            require(response.get('Key') == wanted['key'] and response.get('UploadId') == wanted['uploadId'],
                    'DL parts list refers to another upload')
            known = {p['PartNumber']: p for p in wanted['parts']}
            identities = []
            for part in response.get('Parts', []):
                number = part['PartNumber'];identities.append(number)
                require(number in known and part.get('ETag') == known[number]['ETag']
                        and part.get('Size') == known[number]['payload']['size'], 'DL parts list changed an acknowledged part')
        else:
            items = response.get('Contents', []) if operation == 'list_objects_v2' else (
                response.get('Versions', []) + response.get('DeleteMarkers', []) if operation == 'list_object_versions'
                else response.get('Uploads', []))
            identities = []
            for item in items:
                require(isinstance(item.get('Key'), str) and item['Key'].startswith(f'dl/{endpoint}/'), 'DL list escaped its exact owned prefix')
                identities.append((item['Key'], item.get('VersionId'), item.get('UploadId')))
        require(len(identities) <= 32 and len(set(identities)) == len(identities), 'DL list exceeded its cap or duplicated an identity')
    return {'kind': 'READ_OBSERVED', 'globalCompletenessClaimed': False}


def client_overlap_pairs(readers, writers):
    pairs = []
    for a in readers:
        for b in writers:
            if a['ackPresent'] and b['ackPresent']:
                x, y = a['ack']['observation'], b['ack']['observation']
                if (x.get('clientCallInvoked') is True and y.get('clientCallInvoked') is True
                        and x['clockDomain'] == y['clockDomain']
                        and max(x['callStartedMonotonicNs'], y['callStartedMonotonicNs'])
                        < min(x['callFinishedMonotonicNs'], y['callFinishedMonotonicNs'])):
                    pairs.append({'readerRequestSha256': a['requestSha256'], 'writerRequestSha256': b['requestSha256']})
    return pairs


def concurrent_load(journals, fixture, readable):
    """The controller owns Fleet evidence. Workers only append their own ACKs."""
    require(len(journals) == 4 and set(journals) == {(endpoint, role) for endpoint in fixture['sides']
            for role in ('reader', 'writer')}, 'DL requires exactly four independent original endpoint/role journals')
    require(len({j.output for j in journals.values()}) == 4 and len({id(j) for j in journals.values()}) == 4,
            'DL workers must not share request sequences or mutable journal state')
    require(set(readable) == set(fixture['sides']) and all(type(value) is bool for value in readable.values())
            and all(journal.probe.endpoint_name == endpoint and journal.role == role
                and journal.probe.binding['runId'] == fixture['runId']
                and binding_digest(journal.probe.binding) == fixture['bindingSha256']
                and source_reference(journal.probe.binding['candidateCommit']) == fixture['sourceContract']
                for (endpoint, role), journal in journals.items()), 'DL original run/source/endpoint binding differs')
    gate = threading.Barrier(4)
    deadline = time.monotonic() + ADMISSION_SECONDS
    def worker(endpoint, role):
        journal = journals[endpoint, role]
        steps = mutation_steps(fixture, endpoint) if role == 'writer' else read_steps(fixture, endpoint)
        results, error = [], None
        try:
            gate.wait(timeout=5)
            for step in steps:
                expected = 503 if role == 'writer' else (200, 503) if step['operation'].startswith('list_') else 200 if readable[endpoint] else 503
                result = journal.request(step, expected, deadline)
                results.append(result)
                result['observationProof'] = validate_call(result, fixture, endpoint, readable=readable[endpoint])
                require(result['error'] is None, 'DL collector failed; preserve the original bound observation')
        except Exception as exc:
            journal.mutations.poison('DL worker stopped; drain only')
            error = {'type': type(exc).__name__, 'message': str(exc)}
        return {'endpoint': endpoint, 'role': role, 'plannedCalls': len(steps), 'results': results,
                'complete': error is None and len(results) == len(steps), 'error': error}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker, endpoint, role) for endpoint, role in journals]
        results = [future.result() for future in futures]
    overlaps = []
    for endpoint in fixture['sides']:
        rows = {role: read_journal(journals[endpoint, role].output, journals[endpoint, role].probe.binding,
                                  endpoint, role) for role in ('reader', 'writer')}
        pairs = client_overlap_pairs(rows['reader'], rows['writer'])
        overlaps.append({'endpoint': endpoint, 'overlappingClientCalls': pairs, 'serverCommitFenceClaimed': False})
    return {'workers': results, 'overlaps': overlaps,
        'complete': all(r['complete'] for r in results) and all(r['overlappingClientCalls'] for r in overlaps),
        'allCallsDrained': True, 'automaticRetries': 0, 'mutationReconciliationRequired': True,
        'canonicalCasePassClaimed': False}
