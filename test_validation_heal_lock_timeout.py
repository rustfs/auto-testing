"""I05 timeout controller: real caller socket, durable PUT receipts, honest scope."""

import copy
import json
import time
import unittest
from unittest.mock import patch

from scripts.validation_heal import lock_timeout
import test_validation_heal_lock_observer as fixture


CONTENDER = '30000000-0000-4000-8000-000000000001'


class TimeoutFlowTests(unittest.TestCase):
    def setUp(self):
        self.scene = fixture.ObserverFlowTests(
            'test_complete_component_keeps_original_unknown_and_all_missing_case_proofs')
        self.scene.setUp()
        self.addCleanup(self.scene.doCleanups)
        self.probe, self.fleet = self.scene.probe, self.scene.fleet
        self.entry = self.scene.entry
        self.entry['payload']['size'] = lock_timeout.MIN_BACKPRESSURE_BYTES
        self.trace, self.change, self.puts = [], None, 0
        self.output = self.scene.root / 'timeout'
        self.fleet.snapshot = self.snapshot
        self.probe.inventory = self.inventory
        self.probe.s3 = self.s3
        original_remote = self.fleet.remote
        def remote(node, action, **params):
            value = original_remote(node, action, **params)
            if self.change == 'no-queue' and action == 'caller-socket' and self.puts:
                for sample in value['samples']:
                    for match in sample['ownedMatches']: match['txQueueBytes'] = 0
            if self.change == 'late-fd-leak' and action == 'process-metrics' and (
                    'I05-timeout-final-physical-after-public-read' in self.trace):
                value['fileDescriptors'] += 1
            return value
        self.fleet.remote = remote

    def inventory(self, bucket):
        self.trace.append('final-inventory' if self.puts else 'baseline-inventory')
        entry = {**copy.deepcopy(self.entry), 'isLatest': True}
        return {'bucket': bucket, 'entries': [entry], 'objectsAndMarkers': 1,
            'bytesRead': entry['payload']['size'],
            'pagination': {'versionPageSizes': [1], 'currentPageSizes': [1]}}

    def snapshot(self, label):
        self.trace.append(label)
        changed = self.change == 'physical-write' and self.puts
        return {node: {'formats': [], 'files': [{'path': 'original',
                    'sha256': 'changed' if changed else 'original'}]} for node in self.fleet.nodes}

    def s3(self, operation, params, body=None):
        if operation == 'get_object':
            self.trace.append('exact-version-get')
            return {'acknowledged': True, 'resultUnknown': False,
                'response': {'VersionId': fixture.VERSION}, 'payload': self.entry['payload']}
        self.assertEqual(operation, 'put_object')
        self.assertEqual(params['Bucket'], fixture.BUCKET)
        self.assertEqual(params['Key'], self.entry['key'])
        self.puts += 1
        self.trace.append('put')
        self.assertFalse(self.scene.wire.server.disconnected.is_set())
        self.scene.contender_rows = [{
            'target': fixture.observer.TARGET, 'request_id': CONTENDER,
            'event': 'object_namespace_lock', 'component': 'ecstore', 'subsystem': 'object_lock',
            'op': 'put_object_commit', 'mode': 'write', 'bucket': fixture.BUCKET,
            'object': self.entry['key'], 'namespace': 'namespace-one', 'owner': 'owner-one',
            'lock_attempt_id': '40000000-0000-4000-8000-000000000001',
            'state': state, 'level': 'TRACE' if state == 'acquiring' else 'ERROR',
            'timeout_ms': 1000, 'acquire_ms': 0 if state == 'acquiring' else 1001,
            **({} if state == 'acquiring' else {'failure': 'timeout'}),
        } for state in ('acquiring', 'failed')]
        if self.change == 'no-journal': self.scene.contender_rows = []
        response = {'ResponseMetadata': {'HTTPStatusCode': 503, 'RetryAttempts': 0, 'RequestId': CONTENDER,
            'HTTPHeaders': {'retry-after': '1'}}, 'Error': {'Code': 'ServiceUnavailable'}}
        if self.change == 'unexpected-success':
            response = {'ResponseMetadata': {'HTTPStatusCode': 200, 'RetryAttempts': 0},
                        'VersionId': '50000000-0000-4000-8000-000000000001'}
        return {'endpoint': self.probe.endpoint_name, 'operation': operation, 'params': params,
            'payload': {'size': len(body), 'sha256': lock_timeout.hashlib.sha256(body).hexdigest()},
            'clockDomain': lock_timeout.CLOCK_DOMAIN,
            'clientCallInvoked': True, 'acknowledged': self.change == 'unexpected-success',
            'resultUnknown': self.change == 'unknown', 'response': response,
            'callStartedMonotonicNs': time.monotonic_ns(), 'callFinishedMonotonicNs': time.monotonic_ns()}

    def invoke(self):
        return lock_timeout.observe_timeout(self.probe, self.fleet, fixture.BUCKET, self.entry, self.output)

    def test_complete_timeout_keeps_real_caller_unknown_and_physical_check_precedes_public_get(self):
        proof = self.invoke()
        self.assertTrue(proof['diagnosticComplete'])
        self.assertFalse(proof['wholeCasePassClaimed'] or proof['canonicalCasePassClaimed'])
        timeout = proof['callerObservation']['admittedWork']
        self.assertTrue(timeout['typedTimeoutBeyondBudgetObserved'])
        self.assertTrue(timeout['readResponseBackpressureObservedAfterTimeout'])
        self.assertEqual(self.puts, 1)
        self.assertLess(self.trace.index('I05-timeout-original-physical'),
                        self.trace.index('baseline-inventory'))
        self.assertLess(self.trace.index('I05-timeout-final-physical-before-public-read'),
                        self.trace.index('exact-version-get'))
        transport = json.loads((self.output / 'caller/transport/disconnected.json').read_text())
        self.assertTrue(transport['clientSocketClosed'] and transport['resultUnknown'])
        self.assertEqual(transport['receiveBufferBytesRequested'], lock_timeout.CALLER_RECEIVE_BUFFER_BYTES)
        self.assertTrue((self.output / 'contender/000001-request.json').is_file())
        self.assertTrue((self.output / 'contender/000001-ack.json').is_file())

    def test_small_target_is_not_observed_without_sending_contender_or_get(self):
        self.entry['payload']['size'] = 1
        with self.assertRaisesRegex(ValueError, 'too small'): self.invoke()
        self.assertEqual(self.puts, 0)
        self.assertEqual(self.scene.wire.server.requests, [])
        self.assertEqual(json.loads((self.output / 'failure.json').read_text())['state'], 'NOT_OBSERVED')

    def test_h2_preserves_all_four_peer_resource_checks(self):
        self.fleet.nodes = ['vm000', 'vm001', 'vm002', 'vm003']
        self.fleet.run_id = self.probe.binding['runId'] = 'gh-12345-1-h2'
        # JSON object order does not define the endpoint-to-host binding.
        self.probe.binding['endpoints'] = {f'node{index}': f'http://rustfs-node{index}:9000'
                                           for index in range(4, 0, -1)}
        proof = self.invoke()
        self.assertEqual(set(proof['finalResources']), set(self.fleet.nodes))
        self.assertEqual(set(proof['callerObservation']['baselineResources']), set(self.fleet.nodes))
        self.assertFalse(proof['wholeCasePassClaimed'])

    def test_unexpected_put_success_retains_ack_and_closes_caller_without_public_read(self):
        self.change = 'unexpected-success'
        with self.assertRaisesRegex(ValueError, 'unexpected put_object result'): self.invoke()
        ack = json.loads((self.output / 'contender/000001-ack.json').read_text())
        self.assertEqual(ack['operationCheck'], 'FAIL')
        self.assertTrue(ack['observation']['acknowledged'])
        self.assertTrue(self.scene.wire.server.disconnected.wait(3))
        self.assertEqual(self.puts, 1)
        self.assertNotIn('exact-version-get', self.trace)
        self.assertFalse((self.output / 'diagnostic.json').exists())

    def test_unknown_put_retains_receipt_closes_caller_and_does_not_retry(self):
        self.change = 'unknown'
        with self.assertRaisesRegex(ValueError, 'unknown transport'): self.invoke()
        ack = json.loads((self.output / 'contender/000001-ack.json').read_text())
        self.assertTrue(ack['observation']['resultUnknown'])
        self.assertTrue(self.scene.wire.server.disconnected.wait(3))
        self.assertEqual(self.puts, 1)
        self.assertFalse((self.output / 'diagnostic.json').exists())

    def test_holder_dropped_before_timeout_observation_never_proves_contention(self):
        self.scene.change = 'timeout-drop'
        with self.assertRaisesRegex(ValueError, 'owner ended'): self.invoke()
        self.assertTrue(self.scene.wire.server.disconnected.wait(3))
        self.assertFalse((self.output / 'timeout.json').exists())

    def test_empty_post_timeout_send_queue_cannot_prove_backpressure(self):
        self.change = 'no-queue'
        with self.assertRaisesRegex(ValueError, 'backpressured original GET socket'): self.invoke()
        self.assertTrue(self.scene.wire.server.disconnected.wait(3))
        self.assertFalse((self.output / 'timeout.json').exists())

    def test_physical_change_stops_before_any_verification_get(self):
        self.change = 'physical-write'
        with self.assertRaisesRegex(ValueError, 'changed an original physical file'): self.invoke()
        self.assertNotIn('exact-version-get', self.trace)
        self.assertNotIn('final-inventory', self.trace)
        self.assertFalse((self.output / 'diagnostic.json').exists())

    def test_missing_timeout_events_exhaust_budget_without_retry(self):
        self.change = 'no-journal'
        with patch.object(lock_timeout, 'OBSERVATION_SECONDS', 0), \
                self.assertRaisesRegex(ValueError, 'observation exceeded its budget'):
            self.invoke()
        self.assertEqual(self.puts, 1)
        self.assertTrue(self.scene.wire.server.disconnected.wait(3))

    def test_inventory_cannot_leave_unclosed_fds_after_caller_drain(self):
        self.change = 'late-fd-leak'
        clock = [0]
        def advance(seconds): clock[0] += fixture.observer.DRAIN_SECONDS + 1
        with patch.object(lock_timeout.time, 'monotonic', side_effect=lambda: clock[0]), \
                patch.object(lock_timeout.time, 'sleep', side_effect=advance), \
                self.assertRaisesRegex(ValueError, 'final timeout FD census exceeded its budget'):
            self.invoke()
        self.assertIn('final-inventory', self.trace)
        self.assertFalse((self.output / 'diagnostic.json').exists())

    def test_unreviewed_source_cannot_even_inventory(self):
        self.probe.binding['candidateCommit'] = 'a' * 40
        with self.assertRaisesRegex(ValueError, 'reviewed candidate'): self.invoke()
        self.assertEqual(self.trace, [])
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
