"""DL ledger binding and actual client overlap; mocks are not live certification."""

import copy
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from scripts.validation_heal import network_load as load
from scripts.validation_heal.heal_probe import CLOCK_DOMAIN, save
from scripts.validation_heal.heal_workload import Recorder


class Probe:
    def __init__(self, binding, endpoint='node1', *, fixture=None, role=None, rendezvous=None, unknown=False, readable=True):
        self.binding, self.endpoint_name = binding, endpoint
        self.fixture, self.role, self.rendezvous, self.unknown = fixture, role, rendezvous, unknown
        self.readable = readable
        self.calls, self.objects, self.sequence = [], {}, 0

    def s3(self, operation, params, body=None):
        self.calls.append((operation, copy.deepcopy(params)))
        self.sequence += 1
        started = time.monotonic_ns()
        if self.rendezvous and self.sequence == 1:
            mine, other = self.rendezvous[self.role], self.rendezvous['reader' if self.role == 'writer' else 'writer']
            mine.set()
            if not other.wait(2): raise ValueError('test client calls did not overlap')
        status, result, uncertain = 200, {}, False
        if self.role == 'writer':
            status = 503
            result = {'Error': {'Code': 'ServiceUnavailable'}}
            if self.unknown: result, uncertain = {}, True
        elif self.role == 'reader':
            side = self.fixture['sides'][self.endpoint_name]
            current = side['versions'][-1]
            if operation in {'get_object', 'head_object'}:
                result = {**side['heads'][current['versionId']], 'VersionId': current['versionId']}
                if not self.readable:
                    status = 503
                    result = {'Error': {'Code': '503' if operation == 'head_object' else 'ServiceUnavailable'}}
            else:
                result = {'IsTruncated': False}
                if operation == 'list_parts':
                    item = side['uploads']['complete']
                    result.update(Key=item['key'], UploadId=item['uploadId'], Parts=[
                        {'PartNumber': p['PartNumber'], 'ETag': p['ETag'], 'Size': p['payload']['size']} for p in item['parts']])
        elif operation == 'get_bucket_versioning': result = {'Status': 'Enabled'}
        elif operation == 'put_object':
            version, etag = f'v{self.sequence}', hashlib.md5(body).hexdigest()
            result = {'VersionId': version, 'ETag': etag}
            self.objects[params['Key'], version] = {'VersionId': version, 'ETag': etag,
                'LastModified': datetime(2026, 10, 19, tzinfo=timezone.utc) + timedelta(seconds=self.sequence), 'ContentLength': len(body),
                'ContentType': params['ContentType'], 'Metadata': params['Metadata']}
        elif operation == 'head_object': result = copy.deepcopy(self.objects[params['Key'], params['VersionId']])
        elif operation == 'create_multipart_upload': result = {'UploadId': f'upload-{self.sequence}'}
        elif operation == 'upload_part': result = {'ETag': hashlib.md5(body).hexdigest()}
        response = {**result, 'ResponseMetadata': {'HTTPStatusCode': status}}
        out = {'operation': operation, 'params': copy.deepcopy(params), 'endpoint': self.endpoint_name,
            'clientCallInvoked': True, 'acknowledged': status < 300 and not uncertain,
            'resultUnknown': uncertain, 'response': None if uncertain else response,
            'clockDomain': CLOCK_DOMAIN, 'callStartedMonotonicNs': started, 'callFinishedMonotonicNs': time.monotonic_ns()}
        if body is not None: out['payload'] = load.digest(body)
        if operation == 'get_object' and self.fixture and status == 200: out['payload'] = current['payload']
        return out


class NetworkLoadTests(unittest.TestCase):
    def binding(self):
        return {'runId': 'gh-12345-1-h2', 'candidateCommit': '1c7fc818dd858247c45b39542abd8291b1e6e31b',
            'bucketPrefix': 'rv-heal-owned', 'maxInventoryBytes': 512 * 1024**2,
            'maxInventoryObjects': 128, 'endpoints': {'node1': 'http://one:9000', 'node3': 'http://three:9000'}}

    def fixture(self, root):
        probe = Probe(self.binding())
        recorder = Recorder(probe, root / 'seed')
        fixture = load.seed_load(recorder, 'rv-heal-owned', ['node1', 'node3'])
        return fixture, probe

    def journals(self, root, fixture, *, unknown_endpoint=None):
        journals, probes = {}, {}
        for endpoint in fixture['sides']:
            rendezvous = {'writer': threading.Event(), 'reader': threading.Event()}
            for role in ('reader', 'writer'):
                probe = Probe(self.binding(), endpoint, fixture=fixture, role=role, rendezvous=rendezvous,
                              unknown=endpoint == unknown_endpoint and role == 'writer')
                probes[endpoint, role] = probe
                journals[endpoint, role] = load.LoadJournal(probe, root / f'{endpoint}-{role}', role)
        return journals, probes

    def test_real_seed_preserves_three_distinct_versions_and_all_actual_mpu_ack_ids(self):
        with tempfile.TemporaryDirectory() as d:
            fixture, probe = self.fixture(Path(d))
            self.assertEqual(len(fixture['expected']), 6)
            self.assertEqual(len({row['payload']['sha256'] for row in fixture['expected']}), 6)
            self.assertLess(fixture['seedPayloadBytes'], 32 * 1024**2)
            uploads = [u for side in fixture['sides'].values() for u in side['uploads'].values()]
            self.assertEqual(len({u['uploadId'] for u in uploads}), 6)
            for side in fixture['sides'].values():
                complete = side['uploads']['complete']
                body = b''.join(load.part_body(fixture['bucket'], complete['key'], p['PartNumber'], p['payload']['size']) for p in complete['parts'])
                self.assertEqual(complete['completePayload'], load.digest(body))
                self.assertEqual([p['PartNumber'] for p in complete['parts']], [1, 2])
                self.assertEqual(len(side['heads']), 3)
            steps = load.mutation_steps(fixture, 'node1')
            self.assertEqual([s['nonce'] for s in steps], list(range(3, 11)))
            self.assertEqual(steps[2]['params']['CopySource']['VersionId'], fixture['sides']['node1']['versions'][0]['versionId'])
            self.assertEqual(steps[3]['params']['VersionId'], fixture['sides']['node1']['versions'][0]['versionId'])
            self.assertEqual(steps[6]['params']['MultipartUpload']['Parts'], [
                {k: p[k] for k in ('PartNumber', 'ETag')} for p in fixture['sides']['node1']['uploads']['complete']['parts']])

    def test_four_independent_journals_drain_full_matrix_with_actual_client_overlap(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d);fixture, _ = self.fixture(root)
            journals, probes = self.journals(root, fixture)
            result = load.concurrent_load(journals, fixture, {'node1': True, 'node3': True})
            self.assertTrue(result['complete'])
            self.assertTrue(result['allCallsDrained'])
            self.assertTrue(result['mutationReconciliationRequired'])
            self.assertFalse(result['canonicalCasePassClaimed'])
            self.assertEqual(sum(len(r['results']) for r in result['workers']), 28)
            self.assertTrue(all(row['overlappingClientCalls'] and not row['serverCommitFenceClaimed'] for row in result['overlaps']))
            for (endpoint, role), journal in journals.items():
                rows = load.read_journal(journal.output, journal.probe.binding, endpoint, role)
                self.assertEqual(len(rows), 8 if role == 'writer' else 6)
                self.assertEqual(len(probes[endpoint, role].calls), len(rows))
                self.assertTrue(all(r['ackPresent'] and r['requestSha256'] == r['ack']['requestSha256'] for r in rows))
                self.assertTrue(all(r['resolution'] == 'REQUIRES_INDEPENDENT_RECONCILIATION' for r in rows))

    def test_unknown_write_stops_that_worker_without_reusing_or_retrying_its_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d);fixture, _ = self.fixture(root)
            journals, probes = self.journals(root, fixture, unknown_endpoint='node3')
            result = load.concurrent_load(journals, fixture, {'node1': True, 'node3': True})
            self.assertFalse(result['complete'])
            journal = journals['node3', 'writer']
            rows = load.read_journal(journal.output, journal.probe.binding, 'node3', 'writer')
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['resolution'], 'UNKNOWN_RESULT')
            follow = journal.request(load.mutation_steps(fixture, 'node3')[1], 503, time.monotonic() + 1)
            self.assertIsNone(follow['row'])
            self.assertIsNotNone(follow['error'])
            self.assertEqual(len(probes['node3', 'writer'].calls), 1)

    def test_minority_head_uses_bodyless503_and_current_get_requires_lock_error(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d);fixture, _ = self.fixture(root)
            journals, probes = self.journals(root, fixture)
            probes['node3', 'reader'].readable = False
            result = load.concurrent_load(journals, fixture, {'node1': True, 'node3': False})
            self.assertTrue(result['complete'])
            rows = load.read_journal(journals['node3', 'reader'].output, self.binding(), 'node3', 'reader')
            for row in rows[:2]:
                self.assertEqual(load.validate_call({'row': row}, fixture, 'node3', readable=False)['kind'], 'READ_REFUSED')
            wrong = copy.deepcopy(rows[0]);wrong['ack']['observation']['response']['Error']['Code'] = 'SlowDownRead'
            with self.assertRaises(ValueError): load.validate_call({'row': wrong}, fixture, 'node3', readable=False)

    def test_changed_url_source_or_run_binding_stops_before_any_invocation(self):
        for kind in ('url', 'source', 'run', 'role'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                root = Path(d);fixture, _ = self.fixture(root)
                journals, probes = self.journals(root, fixture)
                probe = probes['node3', 'writer']
                if kind == 'url': probe.binding['endpoints']['node3'] = 'http://foreign:9000'
                elif kind == 'source': probe.binding['candidateCommit'] = '072b57b37559fbe626aeda8abf61617fdcbdd926'
                elif kind == 'run': probe.binding['runId'] = 'gh-54321-1-h2'
                else: journals['node3', 'writer'].role = 'reader'
                with self.assertRaises(ValueError): load.concurrent_load(journals, fixture, {'node1': True, 'node3': True})
                self.assertTrue(all(not p.calls for p in probes.values()))

    def test_lost_ack_file_remains_unknown_and_cannot_become_a_refusal(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d);fixture, _ = self.fixture(root)
            probe = Probe(self.binding(), fixture=fixture, role='writer')
            def fail_ack(path, value):
                if path.name.endswith('-ack.json'): raise OSError('receipt disk unavailable')
                save(path, value)
            journal = load.LoadJournal(probe, root / 'lost-ack', 'writer', writer=fail_ack)
            result = journal.request(load.mutation_steps(fixture, 'node1')[0], 503, time.monotonic() + 1)
            self.assertFalse(result['row']['ackPresent'])
            self.assertEqual(result['row']['resolution'], 'UNKNOWN_PREPARED_REQUEST')
            with self.assertRaises(ValueError): load.validate_call(result, fixture, 'node1', readable=True)
            self.assertEqual(len(probe.calls), 1)

    def test_wrong_hash_selector_endpoint_or_actual_clock_cannot_bind_an_ack(self):
        for kind in ('hash', 'endpoint', 'selector', 'clock', 'clock-range', 'missing-operation'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                root = Path(d);fixture, _ = self.fixture(root)
                probe = Probe(self.binding(), fixture=fixture, role='writer')
                journal = load.LoadJournal(probe, root / 'writer', 'writer')
                journal.request(load.mutation_steps(fixture, 'node1')[0], 503, time.monotonic() + 1)
                path = journal.output / '000001-ack.json';ack = json.loads(path.read_text())
                if kind == 'hash': ack['requestSha256'] = 'f' * 64
                elif kind == 'endpoint': ack['observation']['endpoint'] = 'node3'
                elif kind == 'selector': ack['observation']['params']['Key'] = 'other'
                elif kind == 'clock': ack['observation']['clockDomain'] = 'foreign'
                elif kind == 'clock-range': ack['observation']['callStartedMonotonicNs'] = ack['collectorCallFinishedMonotonicNs'] + 1
                else: ack['observation'].pop('operation')
                path.write_text(json.dumps(ack))
                with self.assertRaises(ValueError): load.read_journal(journal.output, journal.probe.binding, 'node1', 'writer')

    def test_success_500_wrong_error_and_uninvoked_write_are_never_safe_refusals(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d);fixture, _ = self.fixture(root)
            journal = load.LoadJournal(Probe(self.binding(), fixture=fixture, role='writer'), root / 'writer', 'writer')
            result = journal.request(load.mutation_steps(fixture, 'node1')[0], 503, time.monotonic() + 1)
            self.assertFalse(load.validate_call(result, fixture, 'node1', readable=True)['rollbackClaimed'])
            for kind in ('success', '500', 'read-code', 'uninvoked', 'unknown', 'collector-failed'):
                bad = copy.deepcopy(result);obs = bad['row']['ack']['observation']
                if kind == 'success': obs['acknowledged'] = True;obs['response']['ResponseMetadata']['HTTPStatusCode'] = 200
                elif kind == '500': obs['response']['ResponseMetadata']['HTTPStatusCode'] = 500;obs['response']['Error']['Code'] = 'InternalError'
                elif kind == 'read-code': obs['response']['Error']['Code'] = 'SlowDownRead'
                elif kind == 'uninvoked': obs['clientCallInvoked'] = False
                elif kind == 'collector-failed': bad['row']['ack']['operationCheck'] = 'FAIL'
                else: obs['resultUnknown'] = True
                with self.subTest(kind=kind), self.assertRaises(ValueError): load.validate_call(bad, fixture, 'node1', readable=True)

    def test_rehashed_ack_cannot_substitute_an_unplanned_key_nonce_or_mpu_id(self):
        for kind in ('key', 'nonce', 'body', 'mpu'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                root = Path(d);fixture, _ = self.fixture(root)
                journal = load.LoadJournal(Probe(self.binding(), fixture=fixture, role='writer'), root / 'writer', 'writer')
                steps = load.mutation_steps(fixture, 'node1')[:6 if kind == 'mpu' else 1]
                for step in steps: journal.request(step, 503, time.monotonic() + 1)
                sequence = len(steps)
                request_path = journal.output / f'{sequence:06d}-request.json';ack_path = journal.output / f'{sequence:06d}-ack.json'
                request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
                if kind == 'key': request['params']['Key'] = 'dl/node1/unplanned.bin'
                elif kind == 'nonce': request['identity']['nonce'] = 4;ack['identity']['nonce'] = 4
                elif kind == 'mpu': request['params']['UploadId'] = 'unacknowledged-upload-id'
                else: request['payload']['sha256'] = 'f' * 64
                ack['observation']['params'] = copy.deepcopy(request['params'])
                request_path.write_text(json.dumps(request));ack['requestSha256'] = hashlib.sha256(request_path.read_bytes()).hexdigest()
                ack_path.write_text(json.dumps(ack))
                row = load.read_journal(journal.output, self.binding(), 'node1', 'writer')[-1]
                with self.assertRaisesRegex(ValueError, 'planned request'): load.validate_call({'row': row}, fixture, 'node1', readable=True)

    def test_finite_status_tuple_is_preserved_and_invalid_sets_fail_before_invocation(self):
        with tempfile.TemporaryDirectory() as d:
            probe = Probe(self.binding(), role='writer');recorder = Recorder(probe, Path(d) / 'statuses')
            params = {'Bucket': 'rv-heal-owned', 'Key': 'one'}
            recorder.call('put_object', params, b'body', expect_http=(200, 503))
            request = json.loads((recorder.output / '000001-request.json').read_text())
            self.assertEqual(request['expectHttp'], [200, 503])
            self.assertEqual(len(probe.calls), 1)
            for value in ((), (503, 503), (True,), (99,), (200, 201, 202, 503), [200, 503], '503'):
                with self.subTest(value=value), self.assertRaises(ValueError): recorder.call('put_object', params, b'body', expect_http=value)
            self.assertEqual(len(probe.calls), 1)

    def test_wrapper_overlap_and_distinct_clock_domains_cannot_prove_sdk_overlap(self):
        def row(name, start, end, clock=CLOCK_DOMAIN):
            return {'requestSha256': name, 'ackPresent': True, 'ack': {'collectorCallStartedMonotonicNs': 1,
                'collectorCallFinishedMonotonicNs': 100, 'observation': {'clientCallInvoked': True,
                'clockDomain': clock, 'callStartedMonotonicNs': start, 'callFinishedMonotonicNs': end}}}
        reader, writer = row('read', 10, 20), row('write', 21, 30)
        self.assertEqual(load.client_overlap_pairs([reader], [writer]), [])
        writer['ack']['observation']['callStartedMonotonicNs'] = 19
        self.assertEqual(load.client_overlap_pairs([reader], [writer]), [{'readerRequestSha256': 'read', 'writerRequestSha256': 'write'}])
        writer['ack']['observation']['clockDomain'] = 'foreign'
        self.assertEqual(load.client_overlap_pairs([reader], [writer]), [])


if __name__ == '__main__':
    unittest.main()
