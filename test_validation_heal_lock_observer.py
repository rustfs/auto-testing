"""I05 server admission, process-bound socket census and honest evidence limits."""

import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_heal import caller_socket as sockets
from scripts.validation_heal import fleet_node, lock_observer as observer
import test_validation_heal_caller_abort as wire_fixture

VERSION = wire_fixture.VERSION


REQUEST = '10000000-0000-4000-8000-000000000001'
ATTEMPT = '20000000-0000-4000-8000-000000000001'
SENT = {'callerMarker': 'auto-testing-i05/' + REQUEST,
        'requestTarget': '/rv-heal-0123456789abcdef/target.bin?versionId=' + VERSION,
        'localAddress': ['127.0.0.1', 39001], 'peerAddress': ['127.0.0.1', 9000]}
BUCKET = 'rv-heal-0123456789abcdef'
HEADER = '  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode'


def event(state, sent=SENT):
    return {'target': observer.TARGET, 'level': 'TRACE', 'event': 'object_namespace_lock',
        'component': 'ecstore', 'subsystem': 'object_lock', 'state': state, 'op': 'get_object',
        'bucket': BUCKET, 'object': 'target.bin', 'namespace': 'namespace-one', 'owner': 'owner-one',
        'lock_attempt_id': ATTEMPT, 'request_id': REQUEST, 'mode': 'read', 'timeout_ms': 1000,
        'acquire_ms': 1, 'hold_ms': 1, 'span': {'name': 'http-request', 'user_agent': sent['callerMarker'],
            'request_id': REQUEST, 'method': 'GET', 'uri': sent['requestTarget']}}


def identity():
    return {**dict.fromkeys(fleet_node.PROCESS_FIELDS, 'unchanged'), 'pid': 123, 'startTicks': 456,
        'bootId': 'original-boot', 'binarySha256': 'd' * 64,
        'settings': fleet_node.runtime_config('lock-timeout')}


class LockEventTests(unittest.TestCase):
    def test_incomplete_or_non_json_journal_page_cannot_complete_i05_observation(self):
        from scripts.validation_heal.scanner_repair import JournalWindow
        class Fleet:
            nodes = ['vm000']
            journal = 'truncated candidate JSON'
            complete = True
            def record(self, label, value): return value
            def remote(self, node, action, **params):
                return {'complete': self.complete, 'beforeCursor': params.get('afterCursor'),
                        'afterCursor': 'cursor-next', 'journal': self.journal, 'hasMore': False}
        for raw, complete in [('truncated candidate JSON', True), ('[]', True), ('{}', False)]:
            with self.subTest(raw=raw):
                fleet = Fleet()
                window = JournalWindow(fleet, 'I05', event_targets={observer.TARGET}, strict_json=True)
                fleet.journal, fleet.complete = raw, complete
                with self.assertRaises(ValueError): window.capture()
    def test_flattened_production_event_binds_server_id_and_exact_version_uri(self):
        rows = [event('acquiring'), event('acquired')]
        rows[0]['spans'] = [rows[0]['span']]
        proof = observer.caller_attempt(rows, SENT, BUCKET, 'target.bin')
        self.assertEqual(proof['request_id'], REQUEST)
        self.assertTrue(proof['admitted'] and proof['observedAcquiredWithoutDrop'])
        rows.append(event('guard_dropped'))
        proof = observer.caller_attempt(rows, SENT, BUCKET, 'target.bin')
        self.assertTrue(proof['diagnosticGuardDropped'])
        self.assertFalse(proof['distributedReleaseProven'] or proof['heldAtDisconnectProven'])

    def test_rejects_forged_request_id_foreign_key_version_owner_or_incomplete_events(self):
        mutations = [lambda e: e.update(request_id=ATTEMPT), lambda e: e.update(object='foreign.bin'),
            lambda e: e.update(owner='another-owner'), lambda e: e.update(timeout_ms=5000),
            lambda e: e.update(acquire_ms=True), lambda e: e.update(hold_ms=-1),
            lambda e: e.update(requested_version_id=VERSION),
            lambda e: e['span'].update(uri='/other-version'), lambda e: e['span'].update(method='PUT'),
            lambda e: e.update(lock_attempt_id=REQUEST)]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                rows = [event('acquiring'), event('acquired')]
                mutate(rows[1])
                with self.assertRaises(ValueError):
                    observer.caller_attempt(rows, SENT, BUCKET, 'target.bin')
        for states in (['acquired'], ['acquiring', 'acquiring'], ['acquiring', 'disabled'],
                       ['acquiring', 'guard_dropped'], ['acquiring', 'acquired', 'acquired']):
            with self.subTest(states=states), self.assertRaises(ValueError):
                observer.caller_attempt([event(s) for s in states], SENT, BUCKET, 'target.bin')

    def test_free_text_client_markers_and_native_nested_fields_do_not_prove_admission(self):
        for row in ({'message': str(SENT)}, {'target': 'foreign', **{'fields': event('acquiring')}},
                    {'target': observer.TARGET, 'fields': event('acquiring')},
                    {**event('acquiring'), 'span': {'name': 'http-request', 'user_agent': 'different'}}):
            self.assertIsNone(observer.caller_attempt([row], SENT, BUCKET, 'target.bin'))

    def test_cancelled_and_failed_attempts_never_count_as_acquired_or_dropped(self):
        for state in ('cancelled', 'failed'):
            end = event(state)
            if state == 'failed': end.update(level='ERROR', failure='timeout', acquire_ms=1001)
            proof = observer.caller_attempt([event('acquiring'), end], SENT, BUCKET, 'target.bin')
            self.assertFalse(proof['diagnosticGuardDropped'] or proof['observedAcquiredWithoutDrop'])


class CallerSocketTests(unittest.TestCase):
    def request(self):
        return {'runId': 'gh-12345-1-h1', 'action': 'caller-socket', 'expectedProcess': identity(),
                'clientAddress': SENT['localAddress'], 'serverAddress': SENT['peerAddress']}

    def node(self):
        return {'owned': lambda run: (None, None, {'runtimeProfile': 'lock-timeout'}),
                'process': lambda request: identity(), 'PROCESS_FIELDS': fleet_node.PROCESS_FIELDS}

    def test_frozen_linux_tables_include_zero_port_listeners_and_mapped_ipv6(self):
        ipv4 = HEADER + '\n 0: 0100007F:2328 0100007F:9859 01 0:0 00:0 0 1000 0 678\n'
        ipv4 += ' 1: 00000000:2328 00000000:0000 0A 0:0 00:0 0 1000 0 123\n'
        self.assertEqual(sockets.connections(ipv4, False, ('127.0.0.1', 39001), ('127.0.0.1', 9000)),
                         [{'inode': 678, 'state': '01'}])
        self.assertEqual(sockets.proc_address('0000000000000000FFFF00000100007F:2328', True), ('127.0.0.1', 9000))
        self.assertEqual(sockets.proc_address('00000000000000000000000001000000:0000', True), ('::1', 0))
        for raw in ('', HEADER + '\n truncated', ipv4 + ipv4.splitlines()[1]):
            with self.assertRaises(ValueError):
                sockets.connections(raw, False, ('127.0.0.1', 39001), ('127.0.0.1', 9000))

    def test_namespace_tcp_entry_without_owned_fd_does_not_count_as_owned_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'fd').mkdir(); (root / 'net').mkdir()
            (root / 'ns').mkdir(); (root / 'ns/net').symlink_to('net:[123]')
            (root / 'net/tcp').write_text(HEADER + '\n0: 0100007F:2328 0100007F:9859 01 0:0 00:0 0 1000 0 678\n')
            (root / 'net/tcp6').write_text(HEADER + '\n')
            (root / 'fd/3').symlink_to('socket:[999]')
            with patch.object(sockets, 'Path', return_value=root):
                proof = sockets.run(self.request(), self.node())
                self.assertTrue(all(not row['ownedMatches'] for row in proof['samples']))
                (root / 'fd/4').symlink_to('socket:[678]')
                proof = sockets.run(self.request(), self.node())
                self.assertTrue(all(row['ownedMatches'][0]['descriptors'] == [4] for row in proof['samples']))
                changed = self.node(); calls = [identity(), identity(), {**identity(), 'startTicks': 999}]
                changed['process'] = lambda req: calls.pop(0)
                with self.assertRaisesRegex(ValueError, 'process birth'):
                    sockets.run(self.request(), changed)

    def test_path_pid_port_and_profile_injection_fail_before_proc_access(self):
        for change in ({'path': '/etc/shadow'}, {'pid': 1}, {'serverAddress': ['127.0.0.1', 22]},
                       {'clientAddress': ['hostname', 39001]}, {'clientAddress': ['127.0.0.1', True]}):
            with self.subTest(change=change), patch.object(sockets, 'Path', side_effect=AssertionError('no proc read')):
                with self.assertRaises(ValueError): sockets.run({**self.request(), **change}, self.node())
        node = self.node(); node['owned'] = lambda run: (None, None, {'runtimeProfile': 'manual'})
        with self.assertRaises(ValueError): sockets.run(self.request(), node)

    def test_fleet_transport_embeds_the_actual_read_only_socket_backend(self):
        def execute(*args, **kwargs):
            source = kwargs['input'].rsplit('\nmain(', 1)[0]
            scope = {'__name__': '__i05_socket_test__'}
            exec(compile(source, 'i05-socket-node', 'exec'), scope)
            self.assertTrue(callable(scope['caller_socket_backend'].run))
            self.assertEqual(scope['runtime_config']('lock-timeout')['RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT'], '1')
            return SimpleNamespace(returncode=0, stdout='{}', stderr='')
        with tempfile.TemporaryDirectory() as tmp, \
                patch('scripts.validation_heal.healthy.subprocess.run', side_effect=execute):
            fleet = observer.Fleet(['vm000'], Path(tmp), 'gh-12345-1-h1', runtime_profile='lock-timeout')
            self.assertEqual(fleet.remote('vm000', 'caller-socket', **{k: v for k, v in self.request().items()
                                                                   if k not in {'runId', 'action'}}), {})

    @unittest.skipUnless(sys.platform == 'linux', 'requires actual Linux /proc TCP tables')
    def test_actual_linux_socket_is_bound_to_accepted_descriptor_and_disappears_after_close(self):
        with socket.socket() as listener, socket.socket() as client:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('127.0.0.1', 9000)); listener.listen(1)
            client.connect(listener.getsockname())
            accepted, _ = listener.accept()
            node = self.node()
            actual = {**identity(), 'pid': os.getpid()}
            node['process'] = lambda req: actual
            req = {**self.request(), 'expectedProcess': actual, 'clientAddress': list(client.getsockname())}
            try:
                proof = sockets.run(req, node)
                self.assertTrue(all(row['ownedMatches'][0]['descriptors'] == [accepted.fileno()] for row in proof['samples']))
            finally:
                accepted.close()
            proof = sockets.run(req, node)
            self.assertTrue(all(not row['ownedMatches'] for row in proof['samples']))


class ObserverFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        # Reuse the existing real local wire fixture without inheriting its tests.
        self.wire = wire_fixture.CallerAbortTests('test_one_signed_exact_version_get_closes_real_socket_and_retains_unknown')
        self.wire.setUp()
        self.addCleanup(self.wire.tearDown); self.addCleanup(self.tmp.cleanup)
        self.probe = self.wire.probe
        self.probe.binding.update(candidateCommit=observer.CANDIDATE)
        self.probe.binding['endpoints']['node1'] = 'http://rustfs-node1:9000'
        self.probe.endpoint = 'http://rustfs-node1:9000'
        connect = socket.create_connection
        def local_wire(address, *args, **kwargs):
            self.assertEqual(address, ('rustfs-node1', 9000))
            return connect(('127.0.0.1', self.wire.server.server_port), *args, **kwargs)
        self.connect_patch = patch('scripts.validation_heal.caller_abort.socket.create_connection', side_effect=local_wire)
        self.connect_patch.start(); self.addCleanup(self.connect_patch.stop)
        self.entry = {'key': 'target.bin', 'versionId': VERSION, 'deleteMarker': False,
                      'payload': {'size': 1, 'sha256': hashlib.sha256(b'x').hexdigest()}}
        self.stage = 'before'
        self.sent = None
        self.change = None
        self.labels = []
        owner = self
        class Fleet:
            nodes = ['vm000']; run_id = owner.probe.binding['runId']; runtime_profile = 'lock-timeout'
            def record(self, label, value):
                owner.labels.append(label)
                return value
            def remote(self, node, action, **params):
                if action == 'process-metrics':
                    return {'identity': identity(), 'fileDescriptors': 10 if owner.change != 'leak' else
                            11 if owner.stage == 'after' else 10}
                if action == 'caller-socket':
                    matches = [{'state': '01', 'inode': 678, 'descriptors': [4]}] if owner.stage == 'before' else []
                    return {'process': identity(), 'clockDomain': identity()['bootId'], 'readOnly': True,
                            'networkNamespace': 'net:[2]' if owner.change == 'namespace' and owner.stage == 'after' else 'net:[1]',
                            'samples': [{'ownedMatches': matches, 'vanishedDescriptors': 0},
                                        {'ownedMatches': matches, 'vanishedDescriptors': 0}]}
                raise AssertionError(action)
        class Window:
            def __init__(self, *args, **kwargs): pass
            def capture(self):
                rows = [event('acquiring', owner.sent), event('acquired', owner.sent)]
                if owner.stage == 'after' or owner.change == 'early-drop': rows.append(event('guard_dropped', owner.sent))
                if owner.change == 'owner' and owner.stage == 'after': rows[-1]['owner'] = 'rebound'
                return {'vm000': rows}
        self.fleet = Fleet()
        self.window_patch = patch.object(observer, 'JournalWindow', Window); self.window_patch.start()
        self.addCleanup(self.window_patch.stop)
        original = observer.disconnect_get
        def transport(probe, bucket, key, version, output, *, before_disconnect):
            def on_sent(sent):
                owner.sent = sent
                before_disconnect(sent)
            value = original(probe, bucket, key, version, output, before_disconnect=on_sent)
            self.stage = 'after'
            return value
        self.transport_patch = patch.object(observer, 'disconnect_get', side_effect=transport); self.transport_patch.start()
        self.addCleanup(self.transport_patch.stop)
        self.probe.s3 = lambda *args: {'acknowledged': True, 'resultUnknown': False,
            'response': {'VersionId': VERSION}, 'payload': self.entry['payload']}

    def invoke(self):
        return observer.observe_disconnect(self.probe, self.fleet, BUCKET, self.entry, self.root / 'observe')

    def test_complete_component_keeps_original_unknown_and_all_missing_case_proofs(self):
        proof = self.invoke()
        self.assertTrue(proof['observationComplete'] and proof['exactHistoricalContentPreserved'])
        for name in ('heldAtDisconnectProven', 'distributedReleaseProven', 'noGlobalFdLeakProven', 'canonicalCasePassClaimed'):
            self.assertFalse(proof[name])
        transport = json.loads((self.root / 'observe/transport/disconnected.json').read_text())
        self.assertTrue(transport['resultUnknown'])
        self.assertFalse(transport['acknowledged'] or transport['serverOwnershipProven'])
        self.assertTrue((self.root / 'observe/admission.json').is_file())

    def test_closed_before_disconnect_is_not_observed_and_closes_original_socket(self):
        self.change = 'early-drop'
        with self.assertRaisesRegex(ValueError, 'NOT_OBSERVED'):
            self.invoke()
        self.assertTrue(self.wire.server.disconnected.wait(3))
        self.assertFalse(json.loads((self.root / 'observe/failure.json').read_text())['observationComplete'])
        self.assertEqual(len(self.wire.server.requests), 1)

    def test_rebound_owner_fd_leak_or_wrong_final_content_cannot_complete_observation(self):
        self.change = 'owner'
        with self.assertRaises(ValueError): self.invoke()
        self.assertFalse((self.root / 'observe/observation.json').exists())

    def test_network_namespace_move_cannot_hide_original_socket_during_drain(self):
        self.change = 'namespace'
        with self.assertRaisesRegex(ValueError, 'network namespace between'):
            self.invoke()
        self.assertFalse((self.root / 'observe/observation.json').exists())

    def test_fd_count_must_return_to_baseline_and_original_failure_is_kept(self):
        self.change = 'leak'
        clock = [0]
        def advance(seconds): clock[0] += observer.DRAIN_SECONDS + 1
        with patch.object(observer.time, 'monotonic', side_effect=lambda: clock[0]), \
                patch.object(observer.time, 'sleep', side_effect=advance), \
                self.assertRaisesRegex(ValueError, 'caller drain observation exceeded its budget'):
            self.invoke()
        self.assertEqual(self.labels.count('I05-caller-drain-resources'), 2)
        self.assertFalse((self.root / 'observe/observation.json').exists())

    def test_late_admission_cannot_turn_an_expired_observation_into_completion(self):
        with patch.object(observer, 'ADMISSION_SECONDS', 0), self.assertRaisesRegex(ValueError, 'exceeded its budget'):
            self.invoke()
        self.assertTrue(self.wire.server.disconnected.wait(3))
        self.assertFalse((self.root / 'observe/observation.json').exists())

    def test_final_version_must_match_the_original_ack_content(self):
        self.probe.s3 = lambda *args: {'acknowledged': True, 'resultUnknown': False,
            'response': {'VersionId': VERSION}, 'payload': {'size': 1, 'sha256': '0' * 64}}
        with self.assertRaisesRegex(ValueError, 'historical version'):
            self.invoke()

    def test_unreviewed_source_cannot_read_or_send_any_request(self):
        self.probe.binding['candidateCommit'] = 'a' * 40
        with patch.object(self.fleet, 'remote', side_effect=AssertionError('no remote call')):
            with self.assertRaisesRegex(ValueError, 'reviewed candidate'):
                self.invoke()
        self.assertFalse(self.wire.server.requests)
