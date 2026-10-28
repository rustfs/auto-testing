"""Real TCP evidence for the I05 client subset; no server-lock PASS claim."""

from datetime import datetime, timedelta, timezone
import hashlib
import http.server
import json
from pathlib import Path
import threading
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import caller_abort
from scripts.validation_heal.heal_probe import Probe


VERSION = '00000000-0000-4000-8000-000000000001'


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.connection.settimeout(3)
        self.server.requests.append((self.path, dict(self.headers)))
        try:
            self.server.eof.append(self.rfile.read(1))
        finally:
            self.server.disconnected.set()


class CallerAbortTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.requests = []
        self.server.eof = []
        self.server.disconnected = threading.Event()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.binding = {
            'schemaVersion': 1, 'runId': 'gh-12345-1-h1',
            'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            'jumpHostOnly': True, 'exclusiveHealService': True, 'candidateCommit': 'a' * 40,
            'harnessCommit': 'b' * 40, 'candidatePackageSha256': 'c' * 64,
            'candidateBinarySha256': 'd' * 64, 'rcBinarySha256': 'e' * 64,
            'bucketPrefix': 'rv-heal-0123456789abcdef', 'defaultEndpoint': 'node1',
            'endpoints': {'node1': f'http://127.0.0.1:{self.server.server_port}'},
            'maxInventoryObjects': 512, 'maxInventoryBytes': 512 * 1024**2}
        self.probe = Probe(self.binding, credential_pair=('private-test-access', 'private-test-secret'))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def invoke(self, output=None, **changes):
        return caller_abort.disconnect_get(self.probe, changes.get('bucket', self.binding['bucketPrefix']),
            changes.get('key', '中文/literal%2F space.bin'), changes.get('version', VERSION),
            output or self.root / 'call', before_disconnect=changes.get('before_disconnect'),
            receive_buffer_bytes=changes.get('receive_buffer_bytes'))

    def test_one_signed_exact_version_get_closes_real_socket_and_retains_unknown(self):
        proof = self.invoke()
        self.assertTrue(self.server.disconnected.wait(3), 'server must observe this caller EOF')
        self.assertEqual(self.server.eof, [b''])
        self.assertEqual(len(self.server.requests), 1)
        path, headers = self.server.requests[0]
        self.assertEqual(path, '/rv-heal-0123456789abcdef/%E4%B8%AD%E6%96%87/literal%252F%20space.bin?versionId=' + VERSION)
        self.assertTrue(headers['Authorization'].startswith('AWS4-HMAC-SHA256 Credential=private-test-access/'))
        self.assertEqual(headers['Host'], f'127.0.0.1:{self.server.server_port}')
        self.assertEqual(headers['User-Agent'], proof['callerMarker'])
        self.assertEqual(path, proof['requestTarget'])
        from botocore.auth import S3SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials
        signed = headers['Authorization'].split('SignedHeaders=')[1].split(',')[0].split(';')
        checked = AWSRequest(method='GET', url=self.probe.endpoint + path,
                             headers={k: v for k, v in headers.items() if k.lower() in signed})
        checked.context['timestamp'] = headers['X-Amz-Date']
        auth = S3SigV4Auth(Credentials('private-test-access', 'private-test-secret'), 's3', 'us-east-1')
        expected = auth.signature(auth.string_to_sign(checked, auth.canonical_request(checked)), checked)
        self.assertEqual(headers['Authorization'].split('Signature=')[1], expected)
        self.assertEqual(proof['connectionAttempts'], 1)
        self.assertIsNone(proof['receiveBufferBytesRequested'])
        self.assertIsNone(proof['receiveBufferBytesEffective'])
        self.assertTrue(proof['requestWriteCompleted'] and proof['clientSocketClosed'] and proof['shutdownCompleted'])
        self.assertTrue(proof['resultUnknown'])
        for field in ('responseObserved', 'acknowledged', 'retryAllowed', 'serverOwnershipProven', 'canonicalCasePassClaimed'):
            self.assertFalse(proof[field], field)
        intent = self.root / 'call/request.json'
        self.assertEqual(proof['requestSha256'], hashlib.sha256(intent.read_bytes()).hexdigest())
        written = json.loads((self.root / 'call/request-written.json').read_text())
        self.assertTrue(written['requestWriteCompleted'])
        self.assertLessEqual(written['requestWrittenMonotonicNs'], proof['disconnectStartedMonotonicNs'])
        self.assertLessEqual(proof['disconnectStartedMonotonicNs'], proof['clientClosedMonotonicNs'])
        self.assertEqual(json.loads((self.root / 'call/disconnected.json').read_text()), proof)
        text = ''.join(p.read_text() for p in (self.root / 'call').iterdir())
        for secret in ('private-test-access', 'private-test-secret', 'Authorization'):
            self.assertNotIn(secret, text)

    def test_observer_runs_after_durable_write_while_caller_socket_is_open(self):
        observations = []
        def observer(sent):
            self.assertEqual(json.loads((self.root / 'call/request-written.json').read_text()), sent)
            self.assertFalse(self.server.disconnected.is_set())
            observations.append(sent['callerMarker'])
            sent['serverOwnershipProven'] = True
        proof = self.invoke(before_disconnect=observer)
        self.assertEqual(observations, [proof['callerMarker']])
        self.assertFalse(proof['serverOwnershipProven'])
        self.assertTrue(self.server.disconnected.wait(3))

    def test_small_bounded_receive_buffer_is_persisted_and_applied_to_real_socket(self):
        proof = self.invoke(receive_buffer_bytes=4096)
        intent = json.loads((self.root / 'call/request.json').read_text())
        self.assertEqual(intent['receiveBufferBytesRequested'], 4096)
        self.assertEqual(proof['receiveBufferBytesRequested'], 4096)
        self.assertGreaterEqual(proof['receiveBufferBytesEffective'], 4096)
        self.assertLessEqual(proof['receiveBufferBytesEffective'], 4096 * 4)
        self.assertTrue(self.server.disconnected.wait(3))

    def test_invalid_receive_buffer_is_rejected_before_output_or_network(self):
        with self.assertRaises(ValueError):
            self.invoke(receive_buffer_bytes=1)
        self.assertFalse((self.root / 'call').exists())
        self.assertEqual(self.server.requests, [])

    def test_observer_failure_closes_only_the_caller_and_retains_unknown(self):
        def observer(sent):
            raise ValueError('no service ownership window')
        with self.assertRaisesRegex(ValueError, 'no service ownership'):
            self.invoke(before_disconnect=observer)
        self.assertTrue(self.server.disconnected.wait(3))
        proof = json.loads((self.root / 'call/disconnected.json').read_text())
        self.assertTrue(proof['clientSocketClosed'] and proof['resultUnknown'])
        self.assertNotIn('disconnectStartedMonotonicNs', proof)
        self.assertEqual(len(self.server.requests), 1)

    def test_observer_binding_drift_aborts_and_closes_without_another_request(self):
        def observer(sent):
            self.probe.binding['candidateCommit'] = 'f' * 40
        with self.assertRaisesRegex(ValueError, 'binding changed during'):
            self.invoke(before_disconnect=observer)
        self.assertTrue(self.server.disconnected.wait(3))
        self.assertEqual(len(self.server.requests), 1)

    def test_existing_evidence_directory_prevents_replay_without_another_connection(self):
        self.invoke()
        self.assertTrue(self.server.disconnected.wait(3))
        original = (self.root / 'call/disconnected.json').read_bytes()
        with patch.object(caller_abort.socket, 'create_connection', side_effect=AssertionError('no replay')):
            with self.assertRaises(FileExistsError):
                self.invoke()
        self.assertEqual((self.root / 'call/disconnected.json').read_bytes(), original)
        self.assertEqual(len(self.server.requests), 1)

    def test_intent_persistence_failure_prevents_any_network_request(self):
        with patch.object(caller_abort, 'save', side_effect=OSError('fsync failed')), \
             patch.object(caller_abort.socket, 'create_connection', side_effect=AssertionError('network before intent')):
            with self.assertRaisesRegex(OSError, 'fsync failed'):
                self.invoke()
        self.assertEqual(self.server.requests, [])

    def test_evidence_failure_after_send_still_closes_socket_without_retry(self):
        original_save = caller_abort.save
        def fail_written(path, value):
            if path.name == 'request-written.json':
                raise OSError('written receipt unavailable')
            original_save(path, value)
        with patch.object(caller_abort, 'save', side_effect=fail_written):
            with self.assertRaisesRegex(OSError, 'written receipt unavailable'):
                self.invoke()
        self.assertTrue(self.server.disconnected.wait(3))
        self.assertEqual(self.server.eof, [b''])
        proof = json.loads((self.root / 'call/disconnected.json').read_text())
        self.assertEqual(proof['errorType'], 'OSError')
        self.assertTrue(proof['resultUnknown'] and proof['clientSocketClosed'])
        self.assertEqual(proof['connectionAttempts'], 1)
        self.assertEqual(len(self.server.requests), 1)

    def test_connection_failure_is_recorded_and_cannot_retry(self):
        with patch.object(caller_abort.socket, 'create_connection', side_effect=ConnectionRefusedError('private detail')) as connect:
            with self.assertRaises(ConnectionRefusedError):
                self.invoke()
        self.assertEqual(connect.call_count, 1)
        proof = json.loads((self.root / 'call/disconnected.json').read_text())
        self.assertEqual(proof['errorType'], 'ConnectionRefusedError')
        self.assertEqual(proof['connectionAttempts'], 1)
        self.assertFalse(proof['requestInvocationStarted'])
        self.assertFalse(proof['requestWriteCompleted'])
        self.assertNotIn('private detail', json.dumps(proof))

    def test_directory_sync_failure_prevents_network(self):
        with patch.object(caller_abort.os, 'fsync', side_effect=OSError('directory sync failed')), \
             patch.object(caller_abort.socket, 'create_connection', side_effect=AssertionError('network before durable directory')):
            with self.assertRaisesRegex(OSError, 'directory sync failed'):
                self.invoke()
        self.assertEqual(self.server.requests, [])

    def test_shutdown_failure_still_closes_original_socket_and_retains_unknown(self):
        original = caller_abort.socket.create_connection
        sockets = []
        class FailingShutdown:
            def __init__(self, sock):
                self.sock = sock
            def __getattr__(self, name):
                return getattr(self.sock, name)
            def shutdown(self, how):
                raise OSError('shutdown failed')
        def connect(*args, **kwargs):
            sock = FailingShutdown(original(*args, **kwargs))
            sockets.append(sock)
            return sock
        with patch.object(caller_abort.socket, 'create_connection', side_effect=connect):
            with self.assertRaisesRegex(OSError, 'shutdown failed'):
                self.invoke()
        self.assertTrue(self.server.disconnected.wait(3))
        self.assertEqual(self.server.eof, [b''])
        self.assertEqual(sockets[0].fileno(), -1)
        proof = json.loads((self.root / 'call/disconnected.json').read_text())
        self.assertTrue(proof['requestWriteCompleted'] and proof['clientSocketClosed'] and proof['resultUnknown'])
        self.assertEqual(proof['errorType'], 'OSError')
        self.assertNotIn('shutdownCompleted', proof)
        self.assertEqual(proof['connectionAttempts'], 1)

    def test_changed_endpoint_or_binding_is_rejected_before_request(self):
        self.binding['endpoints']['node1'] = 'http://127.0.0.1:9400'
        with patch.object(caller_abort.socket, 'create_connection', side_effect=AssertionError('endpoint drift')):
            with self.assertRaisesRegex(ValueError, 'endpoint differs'):
                self.invoke()
        self.binding['endpoints']['node1'] = self.probe.endpoint
        original_save = caller_abort.save
        def drift_after_intent(path, value):
            original_save(path, value)
            if path.name == 'request.json':
                self.binding['candidateBinarySha256'] = 'f' * 64
        with patch.object(caller_abort, 'save', side_effect=drift_after_intent), \
             patch.object(caller_abort.socket, 'create_connection', side_effect=AssertionError('binding drift')):
            with self.assertRaisesRegex(ValueError, 'binding changed'):
                self.invoke()
        proof = json.loads((self.root / 'call/disconnected.json').read_text())
        self.assertEqual(proof['connectionAttempts'], 0)
        self.assertFalse(proof['requestInvocationStarted'])

    def test_foreign_bucket_null_or_missing_version_and_expired_lease_never_connect(self):
        with patch.object(caller_abort.socket, 'create_connection', side_effect=AssertionError('invalid request connected')):
            for changes in ({'bucket': 'foreign-bucket'}, {'version': 'null'}, {'version': ''},
                            {'version': '00000000-0000-0000-0000-000000000000'}, {'key': ''}):
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    self.invoke(**changes)
            self.binding['leaseExpiresAt'] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
            with self.assertRaisesRegex(ValueError, 'lease expired'):
                self.invoke()
        self.assertEqual(self.server.requests, [])


if __name__ == '__main__':
    unittest.main()
