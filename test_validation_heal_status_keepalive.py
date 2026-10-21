import http.server
import copy
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import threading
import tempfile
import time
import unittest
from unittest.mock import patch

from datetime import datetime, timedelta, timezone
from scripts.validation_heal.status_keepalive import KeepAliveStatusProbe
from scripts.validation_heal.heal_probe import observation_phase, ADMIN_RESPONSE_LIMIT


def binding():
    return {"schemaVersion":1,"runId":"gh-12345-1-h1","leaseExpiresAt":(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat(),
            "jumpHostOnly":True,"exclusiveHealService":True,"candidateCommit":"a"*40,"harnessCommit":"b"*40,
            "candidatePackageSha256":"c"*64,"candidateBinarySha256":"d"*64,"rcBinarySha256":"e"*64,
            "bucketPrefix":"rv-heal-0123456789abcdef","defaultEndpoint":"node1","endpoints":{"node1":"http://127.0.0.1:9400"},
            "maxInventoryObjects":512,"maxInventoryBytes":512*1024**2}

TOKEN = '00000000-0000-4000-8000-000000000001'


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def do_POST(self):
        self.server.peers.append(self.client_address)
        self.server.requests.append((self.path, dict(self.headers)))
        self.rfile.read(int(self.headers.get('Content-Length', '0')))
        if self.server.mode == 'disconnect':
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        body = json.dumps({'summary': 'running', 'clientToken': TOKEN}).encode()
        if self.server.mode == 'oversize':
            body = b'x' * (ADMIN_RESPONSE_LIMIT + 1)
        status = 302 if self.server.mode == 'redirect' else 503 if self.server.mode == 'error' else 200
        self.send_response(status)
        self.send_header('Content-Length', str(len(body) + int(self.server.mode == 'truncated')))
        if self.server.mode in ('truncated', 'close'):
            self.send_header('Connection', 'close')
        self.send_header('Location', 'http://not-followed.invalid/')
        self.end_headers()
        self.wfile.write(body)


class KeepaliveTests(unittest.TestCase):
    def setUp(self):
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.peers = []; self.server.requests = []; self.server.mode = 'ok'
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        lab = binding(); lab['endpoints'][lab['defaultEndpoint']] = f'http://127.0.0.1:{self.server.server_port}'
        self.probe = KeepAliveStatusProbe(lab)

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()

    def call(self, query=None, body=b''):
        return self.probe.admin('/rustfs/admin/v3/heal/rv-heal-0123456789abcdef', [('clientToken', TOKEN)] if query is None else query,
                                body, auth='anonymous')

    def test_real_socket_reused_and_full_evidence_retained(self):
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef") as state:
            a = self.call(); b = self.call()
            self.assertEqual(state['requests'], 2)
            self.assertEqual(len(set(self.server.peers)), 1)
            self.assertLess(a['callFinishedMonotonicNs'], b['callStartedMonotonicNs'])
            self.assertEqual(a['clockDomain'], b['clockDomain'])
            self.assertEqual(b['response']['summary'], 'running')
            self.assertIn('Content-Length', b['responseHeaders'])
            self.assertEqual((state['connectionAttempts'], state['connectionsOpened']), (1, 1))
            first, second = state['observations']
            self.assertTrue(first['transport']['newConnection'])
            self.assertFalse(second['transport']['newConnection'])
            self.assertEqual(first['transport']['connectionId'], second['transport']['connectionId'])
            self.assertEqual(first['transport']['localAddress'], list(self.server.peers[0]))
            self.assertEqual(first['transport']['peerAddress'], ['127.0.0.1', self.server.server_port])
            self.assertIn('connectionEstablishment', first['phases'])
            self.assertNotIn('connectionEstablishment', second['phases'])
            for sample, measured in ((a, first), (b, second)):
                self.assertEqual(sample['callStartedMonotonicNs'], measured['callStartedMonotonicNs'])
                self.assertEqual(sample['callFinishedMonotonicNs'], measured['callFinishedMonotonicNs'])
                for name in ('requestWrite', 'responseHeaders', 'responseBody', 'responseParsing'):
                    phase = measured['phases'][name]
                    self.assertTrue(phase['completed'])
                    self.assertLessEqual(sample['callStartedMonotonicNs'], phase['startedMonotonicNs'])
                    self.assertLessEqual(phase['finishedMonotonicNs'], sample['callFinishedMonotonicNs'])
        self.assertIsNone(self.probe._status)

    def test_successful_server_close_records_a_new_connection_for_the_next_request(self):
        self.server.mode = 'close'
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef") as state:
            self.call(); self.call()
            self.assertEqual((state['requests'], state['connectionAttempts'], state['connectionsOpened']), (2, 2, 2))
            first, second = [row['transport'] for row in state['observations']]
            self.assertNotEqual(first['connectionId'], second['connectionId'])
            self.assertTrue(first['newConnection'] and second['newConnection'])
            self.assertTrue(first['serverWillClose'] and second['serverWillClose'])
            self.assertFalse(state['poisoned'])

    def test_connection_failure_is_retained_without_reconnect_or_false_write_receipt(self):
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef") as state, \
                patch('http.client.HTTPConnection.connect', side_effect=OSError('local connect failure')) as connect:
            with self.assertRaises(OSError): self.call()
            self.assertEqual((state['connectionAttempts'], state['connectionsOpened']), (1, 0))
            measured = state['observations'][0]
            self.assertFalse(measured['requestWriteCompleted'])
            self.assertFalse(measured['responseObserved'])
            self.assertFalse(measured['phases']['connectionEstablishment']['completed'])
            self.assertEqual(measured['failureType'], 'OSError')
            with self.assertRaisesRegex(ValueError, 'poisoned'): self.call()
            connect.assert_called_once()
            self.assertEqual(state['failureType'], 'OSError')
            self.assertEqual(measured['failureType'], 'OSError')
        self.assertEqual(self.server.requests, [])

    def test_observation_budget_fails_before_an_additional_request(self):
        with patch('scripts.validation_heal.status_keepalive.MAX_STATUS_OBSERVATIONS', 2), self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef") as state:
            self.call(); self.call()
            with self.assertRaisesRegex(ValueError, 'budget exhausted'): self.call()
            self.assertEqual(len(state['observations']), 2)
            self.assertEqual(state['requests'], 2)
            self.assertTrue(state['poisoned'])
        self.assertEqual(len(self.server.requests), 2)

    def test_one_observation_cannot_hide_two_http_requests(self):
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef") as state:
            with self.assertRaisesRegex(ValueError, 'already invoked'):
                with self.probe.status_observation():
                    self.call(); self.call()
            self.assertEqual(len(state['observations']), 1)
            self.assertEqual(state['requests'], 1)
            self.assertTrue(state['poisoned'])
        self.assertEqual(len(self.server.requests), 1)

    def test_poll_measurement_rejects_foreign_clock_token_session_or_sequence(self):
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef"):
            sample = self.call()
            for field, value in [('clockDomain', 'foreign'), ('query', [('clientToken', 'foreign')]),
                                 ('statusObservation', {'sessionId': 'foreign', 'sequence': 1}),
                                 ('statusObservation', {**sample['statusObservation'], 'sequence': True}),
                                 ('statusObservation', {**sample['statusObservation'], 'sequence': 2})]:
                altered = copy.deepcopy(sample); altered[field] = value
                with self.assertRaises(ValueError): self.probe.observation_for(altered)
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef"):
            self.call()
            with self.assertRaises(ValueError): self.probe.observation_for(sample)



    def test_signed_headers_preserved(self):
        with patch.dict('os.environ', HEAL_ACCESS_KEY='test-access', HEAL_SECRET_KEY='test-secret'):
            with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef"):
                self.probe.admin('/rustfs/admin/v3/heal/rv-heal-0123456789abcdef', [('clientToken', TOKEN)], b'')
        headers = {k.lower(): v for k, v in self.server.requests[0][1].items()}
        self.assertIn('AWS4-HMAC-SHA256', headers['authorization'])
        self.assertIn('x-amz-content-sha256', headers)

    def test_control_or_different_token_not_sent(self):
        for query, body in [([], b'{}'), ([('clientToken', TOKEN), ('forceStop', 'true')], b''),
                            ([('clientToken', 'different')], b''), ([('clientToken', TOKEN)] * 2, b'')]:
            with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef") as state:
                with self.assertRaises(ValueError): self.call(query, body)
                self.assertTrue(state['poisoned'])
        self.assertEqual(self.server.requests, [])

    def test_disconnect_redirect_and_oversize_poison_without_retry(self):
        for mode in ('disconnect', 'redirect', 'oversize', 'truncated'):
            self.server.mode = mode
            before = len(self.server.requests)
            with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef") as state:
                with self.assertRaises(Exception): self.call()
                self.assertTrue(state['poisoned'])
                with self.assertRaisesRegex(ValueError, 'poisoned'): self.call()
            self.assertEqual(len(self.server.requests), before + 1)

    def test_http_error_kept_as_observation_not_retried(self):
        self.server.mode = 'error'
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef"):
            self.assertEqual(self.call()['httpStatus'], 503)
        self.assertEqual(len(self.server.requests), 1)

    def test_other_thread_cannot_issue_a_status_request(self):
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef"):
            with patch('scripts.validation_heal.status_keepalive.threading.get_ident', return_value=-1):
                with self.assertRaisesRegex(ValueError, 'thread changed'): self.call()
        self.assertEqual(self.server.requests, [])

    def test_default_transport_unchanged_and_nested_session_rejected(self):
        with patch('scripts.validation_heal.status_keepalive.Probe._open_admin', return_value='default') as original:
            self.assertEqual(self.probe._open_admin(object()), 'default')
            original.assert_called_once()
        with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef"):
            with self.assertRaisesRegex(ValueError, 'nested'):
                with self.probe.status_session(TOKEN, "/rustfs/admin/v3/heal/rv-heal-0123456789abcdef"): pass

    def test_outside_status_session_has_no_timing_schema_or_sample_budget(self):
        sample = self.call()
        self.assertNotIn('statusObservation', sample)
        self.assertIsNone(getattr(self.probe, '_status', None))
        self.assertEqual(len(self.server.requests), 1)


if __name__ == '__main__': unittest.main()
