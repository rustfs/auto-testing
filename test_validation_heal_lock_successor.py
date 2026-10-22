"""I05 successor admission, one ACK, physical generations and failure retention."""

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from scripts.validation_heal import fleet_node, lock_successor as successor
from scripts.validation_heal.heal_workload import Recorder
import test_validation_heal_lock_observer as fixture
import test_validation_heal as physical_fixture


REQUEST = '30000000-0000-4000-8000-000000000001'
ATTEMPT = '40000000-0000-4000-8000-000000000001'
VERSION = '50000000-0000-4000-8000-000000000001'
MARKER = 'auto-testing-i05-successor/' + REQUEST
HOLDER = {'request_id': fixture.REQUEST, 'lock_attempt_id': fixture.ATTEMPT,
          'owner': 'owner-one', 'namespace': 'namespace-one'}


def events(marker=MARKER, states=('acquiring',)):
    return [{'target': fixture.observer.TARGET, 'event': 'object_namespace_lock',
             'component': 'ecstore', 'subsystem': 'object_lock', 'state': state, 'level': 'TRACE',
             'op': 'put_object_commit', 'mode': 'write', 'bucket': fixture.BUCKET, 'object': 'target.bin',
             'namespace': 'namespace-one', 'owner': 'owner-one', 'lock_attempt_id': ATTEMPT,
             'request_id': REQUEST, 'timeout_ms': successor.PUT_TIMEOUT_MS,
             'acquire_ms': 0 if state == 'acquiring' else 15, 'hold_ms': 0,
             'span': {'name': 'http-request', 'user_agent': 'SDK ' + marker,
                      'request_id': REQUEST, 'method': 'PUT', 'uri': f'/{fixture.BUCKET}/target.bin'}}
            for state in states]


def observation(probe, params, body):
    return {'operation': 'put_object', 'params': params, 'endpoint': probe.endpoint_name,
            'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()},
            'clientCallInvoked': True, 'acknowledged': True, 'resultUnknown': False,
            'clockDomain': successor.CLOCK_DOMAIN, 'callStartedMonotonicNs': time.monotonic_ns(),
            'callFinishedMonotonicNs': time.monotonic_ns(), 'response': {
                'ResponseMetadata': {'HTTPStatusCode': 200, 'RetryAttempts': 0, 'RequestId': REQUEST},
                'VersionId': VERSION, 'ETag': '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'}}


class SuccessorEvidenceTests(unittest.TestCase):
    def test_exact_request_queue_and_complete_lifecycle(self):
        for states in (('acquiring',), ('acquiring', 'acquired'), ('acquiring', 'acquired', 'guard_dropped')):
            proof = successor.successor_attempt(events(states=states), MARKER, HOLDER, fixture.BUCKET, 'target.bin')
            self.assertEqual(proof['states'], list(states))
            self.assertFalse(proof['canonicalCasePassClaimed'])
        self.assertIsNone(successor.successor_attempt([], MARKER, HOLDER, fixture.BUCKET, 'target.bin'))

    def test_forged_request_owner_version_budget_and_duplicate_attempt_refuse(self):
        mutations = [lambda r: r.update(request_id=fixture.REQUEST), lambda r: r.update(owner='foreign'),
            lambda r: r.update(namespace='foreign'), lambda r: r.update(timeout_ms=1000),
            lambda r: r.update(requested_version_id=VERSION), lambda r: r.update(object='other.bin'),
            lambda r: r.update(state='failed'), lambda r: r.update(acquire_ms=True),
            lambda r: r['span'].update(method='GET'), lambda r: r['span'].update(uri='/other'),
            lambda r: r.update(lock_attempt_id=fixture.ATTEMPT)]
        for mutate in mutations:
            rows = events(); mutate(rows[0])
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                successor.successor_attempt(rows, MARKER, HOLDER, fixture.BUCKET, 'target.bin')
        for states in (('acquired',), ('acquiring', 'acquiring'), ('acquiring', 'guard_dropped')):
            with self.subTest(states=states), self.assertRaises(ValueError):
                successor.successor_attempt(events(states=states), MARKER, HOLDER, fixture.BUCKET, 'target.bin')
        rows = events(); other = copy.deepcopy(rows[0]); other['request_id'] = VERSION
        other['span']['request_id'] = VERSION; rows.append(other)
        with self.assertRaisesRegex(ValueError, 'retried or reused'):
            successor.successor_attempt(rows, MARKER, HOLDER, fixture.BUCKET, 'target.bin')

    def test_original_live_metadata_reads_keep_one_exact_target_write_attempt(self):
        path = Path(__file__).with_name('testdata') / 'i05-successor-metadata-39788321908.ndjson'
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        marker = 'auto-testing-i05-successor/73a9d02c-6579-44ce-8152-011f367dc891'
        holder = {'request_id': 'c10cb980-5924-4e9f-bbba-14d180d56436',
                  'lock_attempt_id': 'd3e2e124-e6cc-46c4-897a-cbbc7b2dba66',
                  'owner': ':::9000', 'namespace': 'pool-0'}
        bucket = 'rv-heal-5dd2430e083c6140'
        for length in range(1, len(rows) + 1):
            proof = successor.successor_attempt(rows[:length], marker, holder, bucket, 'objects/0000.bin')
            writes = [row for row in rows[:length] if row['mode'] == 'write']
            if not writes:
                self.assertIsNone(proof)
            else:
                self.assertEqual(proof['events'], writes)
                self.assertEqual(proof['lockAttemptId'], 'b31b1a30-9431-425e-9a92-909b47ac6bfc')
                self.assertFalse(proof['canonicalCasePassClaimed'])
        metadata = next(i for i, row in enumerate(rows) if row['object'].endswith('/.metadata.bin'))
        mutations = [lambda r: r.update(object='buckets/foreign/.metadata.bin'),
            lambda r: r.update(object=f'buckets/{bucket}/unreviewed'),
            lambda r: r.update(mode='write'), lambda r: r.update(op='put_object_commit'),
            lambda r: r.update(owner='foreign'), lambda r: r.update(namespace='foreign'),
            lambda r: r.update(timeout_ms=successor.PUT_TIMEOUT_MS),
            lambda r: r.update(requested_version_id=VERSION),
            lambda r: r.update(component='foreign'), lambda r: r.update(event='foreign'),
            lambda r: r.update(state='failed'), lambda r: r.update(hold_ms=True),
            lambda r: r.update(lock_attempt_id=holder['lock_attempt_id']),
            lambda r: r.update(lock_attempt_id='b31b1a30-9431-425e-9a92-909b47ac6bfc')]
        for mutate in mutations:
            changed = copy.deepcopy(rows); mutate(changed[metadata])
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                successor.successor_attempt(changed, marker, holder, bucket, 'objects/0000.bin')
        for changed in [rows[:metadata] + rows[metadata + 1:],
                        rows[:metadata] + [rows[metadata]] + rows[metadata:],
                        rows[:metadata + 1] + [dict(rows[metadata + 1], object=f'buckets/{bucket}/.bucket-incarnation')]
                        + rows[metadata + 2:]]:
            with self.subTest(rows=changed), self.assertRaises(ValueError):
                successor.successor_attempt(changed, marker, holder, bucket, 'objects/0000.bin')

    def test_real_recorder_ack_binding_rejects_unknown_retry_wrong_content_and_time(self):
        with tempfile.TemporaryDirectory() as folder:
            probe = type('Probe', (), {'binding': {
                'schemaVersion': 1, 'runId': 'gh-12345-1-h1', 'jumpHostOnly': True, 'exclusiveHealService': True,
                'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                'candidateCommit': 'a' * 40, 'harnessCommit': 'b' * 40,
                'candidatePackageSha256': 'c' * 64, 'candidateBinarySha256': 'd' * 64, 'rcBinarySha256': 'e' * 64,
                'bucketPrefix': fixture.BUCKET, 'defaultEndpoint': 'node1',
                'endpoints': {'node1': 'http://127.0.0.1:9000'},
                'maxInventoryObjects': 32, 'maxInventoryBytes': 512 * 1024**2}, 'endpoint_name': 'node1'})()
            probe.s3 = lambda op, params, body: observation(probe, params, body)
            recorder = Recorder(probe, Path(folder) / 'receipt')
            body = b'one-successor'
            recorder.call('put_object', {'Bucket': fixture.BUCKET, 'Key': 'target.bin',
                'ContentType': 'application/octet-stream', 'Metadata': {'heal-fixture': 'I05-queued-successor'}}, body)
            attempt = {'requestId': REQUEST}
            proof = successor.validate_successor_ack(recorder.output, probe, fixture.BUCKET, 'target.bin', body, attempt)
            self.assertEqual(proof['versionId'], VERSION)
            ack_path = recorder.output / '000001-ack.json'; original = json.loads(ack_path.read_text())
            for mutate in (lambda a: a['observation'].update(resultUnknown=True),
                lambda a: a['observation']['response']['ResponseMetadata'].update(RetryAttempts=1),
                lambda a: a['observation']['response'].update(ETag='"wrong"'),
                lambda a: a['observation']['response'].update(VersionId='null'),
                lambda a: a['observation']['response']['ResponseMetadata'].update(RequestId=fixture.REQUEST),
                lambda a: a.update(requestSha256='0' * 64),
                lambda a: a.update(recordedMonotonicNs=1)):
                value = copy.deepcopy(original); mutate(value); ack_path.write_text(json.dumps(value))
                with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                    successor.validate_successor_ack(recorder.output, probe, fixture.BUCKET, 'target.bin', body, attempt)

    def scene(self):
        before = physical_fixture.HealShardOracleTest().snapshot()
        original = {'key': 'target.bin', 'versionId': fixture.VERSION, 'deleteMarker': False, 'isLatest': True}
        added = {'key': 'target.bin', 'versionId': VERSION, 'deleteMarker': False, 'isLatest': True,
                 'payload': {'size': 1024, 'sha256': 'a' * 64}}
        for value in before.values():
            keep = []
            for metadata, part in zip(value['files'][::3], value['files'][1::3], strict=True):
                metadata = copy.deepcopy(metadata)
                metadata['path'] = f'{fixture.BUCKET}/target.bin/xl.meta'
                metadata['metadata']['key'] = 'target.bin'
                version = metadata['metadata']['versions'][0]
                version.update(versionId=fixture.VERSION, isLatest=True)
                keep += [metadata, {**part, 'path': f"{fixture.BUCKET}/target.bin/{version['dataDir']}/part.1"}]
            value['files'] = keep
        after = copy.deepcopy(before)
        for value in after.values():
            new_parts = []
            for metadata, part in zip(value['files'][::2], value['files'][1::2], strict=True):
                old = metadata['metadata']['versions'][0]; old['isLatest'] = False
                new = copy.deepcopy(old); new.update(versionId=VERSION, isLatest=True, size=1024,
                    dataDir=str(uuid.UUID(int=777)))
                new['parts'] = [{**p, 'size': 1024, 'actualSize': 1024} for p in new['parts']]
                metadata['metadata']['versions'].append(new)
                new_parts.append({**part, 'path': f"{fixture.BUCKET}/target.bin/{new['dataDir']}/part.1",
                                  'sha256': part['sha256'] + '-new'})
            value['files'] += new_parts
        return before, after, {'entries': [original]}, added

    def test_real_physical_map_preserves_old_generation_and_exact_version_population(self):
        before, after, baseline, added = self.scene()
        target = successor.physical_append(before, after, fixture.BUCKET, baseline, added)
        self.assertEqual(target['versionId'], VERSION)
        for mutate in (lambda s: s['files'].pop(),
                       lambda s: s['files'][1].update(sha256='wrong'),
                       lambda s: s['files'][0]['metadata']['versions'].pop(0),
                       lambda s: s['files'].append({'disk': 0, 'path': 'unacknowledged', 'sha256': 'wrong'})):
            changed = copy.deepcopy(after); mutate(next(iter(changed.values())))
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                successor.physical_append(before, changed, fixture.BUCKET, baseline, added)

    def test_runtime_keeps_one_second_timeout_separate_from_sixty_second_put_queue(self):
        timeout = fleet_node.runtime_config('lock-timeout')
        queued = fleet_node.runtime_config('lock-successor')
        self.assertNotIn('RUSTFS_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS', timeout)
        self.assertEqual(queued.pop('RUSTFS_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS'), '60000')
        self.assertEqual(queued, timeout)


class SuccessorFlowTests(unittest.TestCase):
    def setUp(self):
        self.scene = fixture.ObserverFlowTests('test_complete_component_keeps_original_unknown_and_all_missing_case_proofs')
        self.scene.setUp(); self.addCleanup(self.scene.doCleanups)
        self.probe, self.fleet, self.entry = self.scene.probe, self.scene.fleet, self.scene.entry
        self.fleet.runtime_profile = 'lock-successor'
        self.entry['payload']['size'] = successor.MIN_BACKPRESSURE_BYTES
        self.probe.credential_pair = ('test-access', 'test-secret')
        self.change = None
        self.done = threading.Event(); self.calls = 0; self.labels = []
        self.final_entry = None; self.original = self.inventory(fixture.BUCKET)
        self.trace = []
        self.output = self.scene.root / 'successor'
        owner = self
        class Writer(successor.Probe):
            def __init__(self, probe, marker):
                self.binding = probe.binding; self.endpoint_name = probe.endpoint_name
                self.marker = marker
            def s3(self, operation, params, body):
                owner.calls += 1
                owner.scene.contender_rows = events(self.marker,
                    ('acquiring', 'acquired') if owner.change == 'early-acquire' else ('acquiring',))
                if not owner.scene.wire.server.disconnected.wait(3):
                    raise ValueError('caller was never disconnected')
                if owner.change == 'unknown':
                    return {'acknowledged': False, 'resultUnknown': True, 'response': None}
                owner.scene.contender_rows = events(self.marker, ('acquiring', 'acquired', 'guard_dropped'))
                response = observation(self, params, body)
                owner.final_entry = {'key': params['Key'], 'versionId': VERSION,
                    'deleteMarker': False, 'isLatest': True, 'payload': response['payload'],
                    'properties': {'ETag': response['response']['ETag'], 'ContentLength': len(body),
                        'ContentType': params['ContentType'], 'Metadata': params['Metadata']}, 'tags': []}
                owner.done.set()
                return response
        peer = type('Peer', (), {'inventory': lambda _, bucket: owner.inventory(bucket)})
        for name, value in [('SuccessorProbe', Writer), ('Probe', lambda *a, **k: peer()),
                            ('physical_append', self.physical), ('verify_cohort', lambda *a: {'allShardBitrotVerified': True})]:
            p = patch.object(successor, name, value); p.start(); self.addCleanup(p.stop)
        self.probe.inventory = self.inventory
        def snapshot(label):
            self.trace.append(label)
            changed = self.change == 'historical-read-mutation' and label == 'I05-successor-before-all-peer-reads'
            return {'vm000': {'files': ['unexpected-read-repair'] if changed else [], 'formats': []}}
        self.fleet.snapshot = snapshot
        original_remote = self.fleet.remote
        def remote(node, action, **params):
            value = original_remote(node, action, **params)
            if action in {'process-metrics', 'caller-socket'}:
                if action == 'caller-socket':
                    value['process']['settings'] = fleet_node.runtime_config('lock-successor')
                else:
                    value['identity']['settings'] = fleet_node.runtime_config('lock-successor')
            return value
        self.fleet.remote = remote

    def inventory(self, bucket):
        entries = [{**copy.deepcopy(self.entry), 'isLatest': self.final_entry is None}]
        if self.final_entry is not None:
            entries.append(copy.deepcopy(self.final_entry))
        return {'bucket': bucket, 'entries': entries, 'objectsAndMarkers': len(entries),
                'bytesRead': sum(row['payload']['size'] for row in entries),
                'pagination': {'versionPageSizes': [len(entries)], 'currentPageSizes': [1]}}

    def physical(self, before, after, bucket, baseline, added):
        self.assertTrue(self.done.is_set())
        self.trace.append('physical-append')
        return {'versionId': added['versionId']}

    def invoke(self):
        return successor.observe_successor(self.probe, self.fleet, fixture.BUCKET, self.entry, self.output)

    def test_one_queued_put_drains_after_real_caller_socket_closes_and_retains_exact_ack(self):
        result = self.invoke()
        self.assertEqual(self.calls, 1)
        self.assertTrue(result['diagnosticComplete'])
        self.assertFalse(result['canonicalCasePassClaimed'] or result['heldAtDisconnectProven'])
        self.assertEqual(result['verifiedEndpoints'], ['node1'])
        self.assertEqual(len(result['expectedEntries']), 2)
        self.assertTrue((self.output / 'queued.json').is_file())
        self.assertTrue((self.output / 'successor-complete.json').is_file())
        self.assertLess(self.trace.index('physical-append'), self.trace.index('I05-successor-before-all-peer-reads'))
        transport = json.loads((self.output / 'caller/transport/disconnected.json').read_text())
        self.assertTrue(transport['resultUnknown'] and transport['clientSocketClosed'])

    def test_unknown_ack_retains_original_call_and_stops_before_public_verification(self):
        self.change = 'unknown'
        with self.assertRaisesRegex(ValueError, 'unexpected put_object'):
            self.invoke()
        self.assertEqual(self.calls, 1)
        ack = json.loads((self.output / 'successor/000001-ack.json').read_text())
        self.assertTrue(ack['observation']['resultUnknown'])
        self.assertNotIn('physical-append', self.trace)
        self.assertFalse((self.output / 'diagnostic.json').exists())

    def test_first_historical_verification_read_cannot_hide_physical_mutation(self):
        self.change = 'historical-read-mutation'
        with self.assertRaisesRegex(ValueError, 'first historical verification read changed physical content'):
            self.invoke()
        self.assertEqual(self.calls, 1)
        self.assertTrue((self.output / 'successor/000001-ack.json').is_file())
        self.assertFalse((self.output / 'diagnostic.json').exists())

    def test_early_write_acquisition_is_rejected_and_original_tcp_socket_still_closes(self):
        self.change = 'early-acquire'
        with self.assertRaisesRegex(ValueError, 'acquired before caller disconnect'):
            self.invoke()
        self.assertTrue(self.scene.wire.server.disconnected.wait(3))
        self.assertEqual(self.calls, 1)
        self.assertTrue((self.output / 'successor/000001-ack.json').is_file())
        self.assertFalse((self.output / 'diagnostic.json').exists())


if __name__ == '__main__':
    unittest.main()
