"""DL lifecycle fencing, exact business ACK preservation and post-Abort physical closure."""

import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from scripts.validation_heal import network_load_session as sessions, network_load as load
from scripts.validation_heal.heal_probe import CLOCK_DOMAIN


class Server:
    def __init__(self, directory):
        self.directory = directory
        self.objects, self.uploads, self.calls, self.count = {}, {}, [], 0
        self.stopped, self.fault, self.orphan, self.fail_create = False, False, False, False
        self.lock = threading.Lock()
        self.events = {endpoint: {role: threading.Event() for role in ('reader', 'writer')} for endpoint in ('node1', 'node3')}
        self.seen = set()

    def factory(self, binding, endpoint=None):
        server = self
        class Probe:
            def __init__(self):
                self.binding, self.endpoint_name = binding, endpoint or binding['defaultEndpoint']
            def s3(self, operation, params, body=None):
                assert not server.stopped, 'S3 observation while stopped'
                assert (server.directory / 'intent.json').exists()
                assert all((server.directory / (ep + '-' + role)).is_dir() for ep in ('node1', 'node3') for role in ('reader', 'writer'))
                started = time.monotonic_ns()
                with server.lock:
                    server.calls.append((self.endpoint_name, operation, copy.deepcopy(params)))
                    server.count += 1
                    serial = server.count
                role = 'writer' if operation in load.MUTATIONS else 'reader'
                event_key = self.endpoint_name, role
                if server.fault and event_key not in server.seen:
                    server.seen.add(event_key)
                    server.events[self.endpoint_name][role].set()
                    assert server.events[self.endpoint_name]['reader' if role == 'writer' else 'writer'].wait(2)
                code, status, response, unknown = None, 200, {}, False
                key = params.get('Key')
                if server.fault and operation in load.MUTATIONS:
                    status, code = 503, 'ServiceUnavailable'
                elif operation == 'get_bucket_versioning': response = {'Status': 'Enabled'}
                elif operation == 'put_object':
                    version = f'v{serial}'
                    for (k, v), item in server.objects.items():
                        if k == key: item['isLatest'] = False
                    properties = {'ETag': hashlib.md5(body).hexdigest(), 'ContentLength': len(body),
                        'ContentType': params['ContentType'], 'Metadata': params['Metadata']}
                    server.objects[key, version] = {'key': key, 'versionId': version, 'isLatest': True,
                        'deleteMarker': False, 'payload': load.digest(body), 'properties': properties,
                        'tags': [{'Key': 'fixture', 'Value': 'DL'}, {'Key': 'suite', 'Value': 'heal'}]}
                    response = {'VersionId': version, 'ETag': properties['ETag']}
                elif operation in {'head_object', 'get_object'}:
                    item = server.objects[key, params['VersionId']]
                    response = {**item['properties'], 'VersionId': item['versionId'], 'LastModified': datetime(2026, 10, 19, tzinfo=timezone.utc)}
                elif operation == 'create_multipart_upload':
                    upload_id = f'upload-{serial}'
                    server.uploads[key, upload_id] = {}
                    response = {'UploadId': upload_id}
                    if server.fail_create: unknown, server.fail_create = True, False
                elif operation == 'upload_part':
                    number = params['PartNumber'];etag = hashlib.md5(body).hexdigest()
                    server.uploads[key, params['UploadId']][number] = {'PartNumber': number, 'ETag': etag, 'Size': len(body)}
                    response = {'ETag': etag}
                elif operation == 'list_multipart_uploads':
                    response = {'IsTruncated': False, 'Uploads': [{'Key': k, 'UploadId': u} for k, u in server.uploads if k.startswith(params['Prefix'])]}
                elif operation == 'list_parts':
                    parts = server.uploads.get((key, params['UploadId']))
                    if parts is None: status, code = 404, 'NoSuchUpload'
                    else: response = {'Key': key, 'UploadId': params['UploadId'], 'IsTruncated': False, 'Parts': list(parts.values())}
                elif operation == 'abort_multipart_upload':
                    if (key, params['UploadId']) in server.uploads:
                        del server.uploads[key, params['UploadId']];status = 204
                    else: status, code = 404, 'NoSuchUpload'
                elif operation in {'list_objects_v2', 'list_object_versions'}: response = {'IsTruncated': False}
                else: raise AssertionError('Unplanned operation: ' + operation)
                if code: response = {'Error': {'Code': code}}
                response['ResponseMetadata'] = {'HTTPStatusCode': status}
                observed = {'operation': operation, 'params': copy.deepcopy(params), 'endpoint': self.endpoint_name,
                    'clockDomain': CLOCK_DOMAIN, 'callStartedMonotonicNs': started,
                    'clientCallInvoked': True, 'acknowledged': status < 300 and not unknown, 'resultUnknown': unknown,
                    'response': None if unknown else response, 'callFinishedMonotonicNs': time.monotonic_ns()}
                if body is not None: observed['payload'] = load.digest(body)
                if operation == 'get_object': observed['payload'] = item['payload']
                return observed
            def inventory(self, bucket):
                assert not server.stopped
                return {'bucket': bucket, 'entries': sorted(copy.deepcopy(list(server.objects.values())), key=lambda r: (r['key'], r['versionId']))}
        return Probe()

    def business_scene(self, binding):
        return {node: {'quiescent': self.stopped, 'pid': 0 if self.stopped else 101,
            'unit': 'rustfs-validation-heal-' + binding['runId'] + '.service',
            'binarySha256': binding['candidateBinarySha256'],
            'formats': [{'disk': disk, 'value': {'original': node + str(disk)}} for disk in range(4)],
            'files': [{'disk': 0, 'path': binding['bucketPrefix'] + '/dl/node1/control.bin/xl.meta',
                       'size': 10, 'sha256': 'c' * 64}]}
                for node in ('vm000', 'vm001', 'vm002', 'vm003')}

    def fleet(self, binding):
        server = self
        class Fleet:
            nodes = ['vm000', 'vm001', 'vm002', 'vm003']
            def record(self, label, value): return value
            def remote(self, node, action, **params):
                assert server.stopped and action == 'mpu-scene'
                keys, scopes, files = params['keys'], [], []
                for disk in range(4):
                    for key in keys:
                        sha = hashlib.sha256((params['bucket'] + '/' + key).encode()).hexdigest()
                        existing = [(k, u) for k, u in server.uploads if k == key]
                        scopes.append({'disk': disk, 'key': key, 'keySha256': sha, 'present': bool(existing)})
                        for k, u in existing:
                            files.append({'disk': disk, 'key': key, 'path': f'.rustfs.sys/multipart/{sha}/{u}/xl.meta', 'size': 10, 'sha256': 'a' * 64})
                if server.orphan:
                    key = keys[0];sha = hashlib.sha256((params['bucket'] + '/' + key).encode()).hexdigest()
                    files.append({'disk': 0, 'key': key, 'path': f'.rustfs.sys/multipart/{sha}/orphan/part.1', 'size': 1, 'sha256': 'b' * 64})
                value = {'bucket': params['bucket'], 'keys': keys, 'scopes': scopes, 'directories': [],
                         'files': files, 'bytesHashed': sum(x['size'] for x in files)}
                return {**value, 'runId': binding['runId'], 'binarySha256': binding['candidateBinarySha256'],
                    'quiescent': True, 'readOnly': True, 'originalPhysicalDrives': 4,
                    'payloadIntegrityClaimed': False, 'sceneSha256': sessions.json_hash(value)}
        return Fleet()


class LoadSessionTests(unittest.TestCase):
    def session(self, root):
        directory = root / 'DL';server = Server(directory)
        binding = {'runId': 'gh-12345-1-h2', 'candidateCommit': '1c7fc818dd858247c45b39542abd8291b1e6e31b',
            'bucketPrefix': 'rv-heal-owned', 'maxInventoryBytes': 512 * 1024**2, 'maxInventoryObjects': 128,
            'candidateBinarySha256': 'a' * 64, 'defaultEndpoint': 'node1',
            'endpoints': {f'node{i}': f'http://node{i}:9000' for i in range(1, 5)}}
        value = sessions.LoadSession(binding, binding['bucketPrefix'], ['node1', 'node3'], directory, probe_factory=server.factory)
        self.assertEqual(server.calls, [])
        return value, server, binding

    def prepared(self, root):
        session, server, binding = self.session(root)
        fixture = session.seed()
        session.register_baseline(server.factory(binding).inventory(binding['bucketPrefix']))
        server.stopped = True
        session.register_business_baseline(server.business_scene(binding))
        server.stopped = False
        server.fault = True
        self.assertTrue(session.run_load({'node1': True, 'node3': True})['complete'])
        server.fault, server.stopped = False, True
        session.capture_mpu(server.fleet(binding), 'pre-reconcile')
        server.stopped = False
        return session, server, binding

    def test_full_closure_requires_distinct_recovery_acks_and_stopped_empty_mpu_scene(self):
        with tempfile.TemporaryDirectory() as d:
            value, server, binding = self.prepared(Path(d))
            original = sessions.json_hash(value.registered_receipts())
            public = value.reconcile()
            self.assertFalse(public['namespaceCleanupAllowed'] or value.safe_to_cleanup)
            self.assertEqual(len(public['retirements']), 6)
            self.assertTrue(all(r['newRecoveryAbortAck'] and not r['originalAbortAckClaimed'] for r in public['retirements']))
            with self.assertRaises(ValueError): value.finish_reconciliation(server.business_scene(binding), repair_targets=[])
            server.stopped = True
            value.capture_mpu(server.fleet(binding), 'post-reconcile')
            closure = value.finish_reconciliation(server.business_scene(binding), repair_targets=[])
            self.assertTrue(value.safe_to_cleanup and closure['namespaceCleanupAllowed'])
            self.assertEqual(len(closure['requestEffectResolutions']), 36)
            self.assertTrue(closure['allOriginalRequestsResolved'])
            self.assertFalse(closure['canonicalCasePassClaimed'] or closure['originalUnknownResultsRelabeledAcknowledged'])
            self.assertEqual(original, sessions.json_hash(value.registered_receipts()))
            self.assertEqual(server.uploads, {})
            count = len(server.calls)
            with self.assertRaises(ValueError): value.run_load({'node1': True, 'node3': True})
            self.assertEqual(len(server.calls), count)

    def test_seed_failure_keeps_original_unknown_create_even_without_returned_fixture(self):
        with tempfile.TemporaryDirectory() as d:
            value, server, binding = self.session(Path(d));server.fail_create = True
            with self.assertRaises(ValueError): value.seed()
            self.assertIsNone(value.fixture)
            self.assertEqual(value.phase, 'SEED_FAILED')
            receipts = value.registered_receipts()
            self.assertEqual(receipts['seed'][-1]['request']['operation'], 'create_multipart_upload')
            self.assertEqual(receipts['seed'][-1]['resolution'], 'UNKNOWN_RESULT')
            self.assertTrue(all(not r['rows'] for r in receipts['workers']))
            count = len(server.calls)
            with self.assertRaises(ValueError): value.reconcile()
            self.assertFalse(value.safe_to_cleanup)
            self.assertEqual(count, len(server.calls))
            self.assertTrue(server.uploads)

    def test_missing_original_version_or_unexpected_version_blocks_before_abort(self):
        for kind in ('missing', 'extra'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                value, server, binding = self.prepared(Path(d))
                pair = next(iter(server.objects))
                if kind == 'missing': server.objects.pop(pair)
                else:
                    row = copy.deepcopy(server.objects[pair]);row['versionId'] = 'unexpected';server.objects[row['key'], row['versionId']] = row
                before = set(server.uploads)
                with self.assertRaisesRegex(ValueError, 'versions/content/metadata/tags'): value.reconcile()
                self.assertFalse(value.safe_to_cleanup)
                self.assertEqual(set(server.uploads), before)
                self.assertTrue((value.directory / 'reconcile-01/unresolved.json').exists())

    def test_unknown_create_discovery_is_bound_to_original_request_and_never_fabricates_ack(self):
        with tempfile.TemporaryDirectory() as d:
            value, server, binding = self.prepared(Path(d))
            server.uploads['dl/node1/create.bin', 'unacknowledged-original-id'] = {}
            public = value.reconcile()
            self.assertEqual(public['discoveredUploadIdsAreNotCreateAcks'], [('dl/node1/create.bin', 'unacknowledged-original-id')])
            server.stopped = True;value.capture_mpu(server.fleet(binding), 'post-reconcile')
            closure = value.finish_reconciliation(server.business_scene(binding), repair_targets=[])
            create = [r for r in closure['requestEffectResolutions'] if r['actor'] == 'node1' and r['operation'] == 'create_multipart_upload']
            self.assertEqual(len(create), 1)
            self.assertFalse(create[0]['originalAcknowledged'])
            self.assertTrue(value.safe_to_cleanup)

    def test_unbound_pending_id_and_post_abort_orphan_never_allow_cleanup(self):
        for kind in ('unbound-id', 'physical-orphan'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                value, server, binding = self.prepared(Path(d))
                if kind == 'unbound-id':
                    server.uploads['dl/node1/complete.bin', 'foreign-id'] = {}
                    with self.assertRaisesRegex(ValueError, 'unbound'): value.reconcile()
                else:
                    value.reconcile();server.stopped, server.orphan = True, True
                    value.capture_mpu(server.fleet(binding), 'post-reconcile')
                    with self.assertRaisesRegex(ValueError, 'multipart files remain'): value.finish_reconciliation(server.business_scene(binding), repair_targets=[])
                self.assertFalse(value.safe_to_cleanup)

    def test_hidden_business_orphan_or_changed_format_blocks_even_after_public_and_mpu_closure(self):
        for kind in ('hidden-business-file', 'format', 'live', 'foreign-candidate'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                value, server, binding = self.prepared(Path(d));value.reconcile()
                server.stopped = True;value.capture_mpu(server.fleet(binding), 'post-reconcile')
                after = server.business_scene(binding)
                if kind == 'hidden-business-file':
                    after['vm001']['files'].append({'disk': 1,
                        'path': binding['bucketPrefix'] + '/dl/node1/copy.bin/uncommitted/part.1',
                        'size': 1, 'sha256': 'd' * 64})
                elif kind == 'format': after['vm001']['formats'][0]['value'] = {'foreign': True}
                elif kind == 'live': after['vm001']['quiescent'] = False
                else: after['vm001']['binarySha256'] = 'f' * 64
                with self.assertRaises(ValueError):
                    value.finish_reconciliation(after, repair_targets=[])
                self.assertFalse(value.safe_to_cleanup)

    def test_changed_original_journal_or_missing_stopped_capture_blocks_closure(self):
        with tempfile.TemporaryDirectory() as d:
            value, server, binding = self.session(Path(d));value.seed()
            value.register_baseline(server.factory(binding).inventory(binding['bucketPrefix']))
            count = len(server.calls)
            with self.assertRaises(ValueError): value.reconcile()
            self.assertEqual(count, len(server.calls))
        for suffix in ('request', 'ack'):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as d:
                value, server, binding = self.prepared(Path(d));value.reconcile()
                server.stopped = True;value.capture_mpu(server.fleet(binding), 'post-reconcile')
                path = value.seed_recorder.output / ('000001-' + suffix + '.json')
                path.write_text(path.read_text() + ' ')
                with self.assertRaises(ValueError): value.finish_reconciliation(server.business_scene(binding), repair_targets=[])
                self.assertFalse(value.safe_to_cleanup)


if __name__ == '__main__':
    unittest.main()
