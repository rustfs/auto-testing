#!/usr/bin/env python3
"""Verify raw S3 requests with botocore and exercise SNW-114 error handling."""
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from urllib.parse import parse_qs, urlsplit

from botocore.auth import S3SigV4Auth, S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

SCRIPT = Path(__file__).with_name('rustfs-s3-compat-test.sh').read_text()
SIGNER = SCRIPT[SCRIPT.index('s3_signed_curl() {'):SCRIPT.index('\nmd5_of_file()')]
CASE = SCRIPT[SCRIPT.index('test_snw114() {'):SCRIPT.index('\n# ==================== CLI parsing')]
ACCESS = 'test@example.com'
SECRET = 'test-secret'


class SigningTest(unittest.TestCase):
    def capture_request(self, presign=False, extra_headers=True):
        captured = {}

        class Handler(BaseHTTPRequestHandler):
            def do_PUT(self):
                captured.update(path=self.path, headers=dict(self.headers),
                                body=self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        with HTTPServer(('127.0.0.1', 0), Handler) as server, tempfile.TemporaryDirectory() as tmp:
            endpoint = f'http://127.0.0.1:{server.server_port}'
            server.timeout = 5
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            payload = Path(tmp, 'archive.tar')
            payload.write_bytes(b'test archive\x00\xff')
            command = ('s3_signed_curl PUT "/bucket/archive.tar?z=last&x-rustfs-max-content-length=1&a=%2F" '
                       '--data "$TEST_TMP/archive.tar"')
            if extra_headers:
                command += (' -H "X-Amz-Meta-Snowball-Auto-Extract: true"'
                            ' -H "X-Amz-Meta-Test:   spaced   value "'
                            ' -H "x-amz-meta-test: second" -H "If-None-Match: *"')
            if presign:
                command += ' --presign'
            result = subprocess.run(['bash', '-c', SIGNER + '\n' + command], capture_output=True,
                                    text=True, timeout=15, env={**os.environ, 'ENDPOINT': endpoint,
                                    'REGION': 'us-east-1', 'TEST_TMP': tmp, 'ACCESS_KEY': ACCESS,
                                    'SECRET_KEY': SECRET, 'NO_PROXY': '*', 'no_proxy': '*'})
            thread.join(timeout=6)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, '200')
            self.assertEqual(captured['body'], payload.read_bytes())
        headers = {key.lower(): value for key, value in captured['headers'].items()}
        query = parse_qs(urlsplit(captured['path']).query)
        if presign:
            self.assertNotIn('authorization', headers)
            self.assertEqual(query['X-Amz-Credential'], [f'{ACCESS}/{query["X-Amz-Date"][0][:8]}/us-east-1/s3/aws4_request'])
            self.assertEqual(query['x-rustfs-max-content-length'], ['1'])
            signature = query['X-Amz-Signature'][0]
            signed = query['X-Amz-SignedHeaders'][0]
            timestamp = query['X-Amz-Date'][0]
            path = captured['path'].split('&X-Amz-Signature=')[0]
        else:
            authorization = headers['authorization']
            signature = authorization.split('Signature=')[1]
            signed = authorization.split('SignedHeaders=')[1].split(',')[0]
            timestamp = headers['x-amz-date']
            path = captured['path']
        if extra_headers:
            self.assertIn('x-amz-meta-snowball-auto-extract', signed.split(';'))
            self.assertIn('if-none-match', signed.split(';'))
            self.assertEqual(headers['x-amz-meta-test'], 'spaced value,second')
        request = AWSRequest(method='PUT', url=endpoint + path,
                             headers={name: headers[name] for name in signed.split(';')})
        request.context['timestamp'] = timestamp
        request.context['payload_signing_enabled'] = False
        signer_type = S3SigV4QueryAuth if presign else S3SigV4Auth
        verifier = signer_type(Credentials(ACCESS, SECRET), 's3', 'us-east-1')
        canonical = verifier.canonical_request(request)
        expected = verifier.signature(verifier.string_to_sign(request, canonical), request)
        self.assertEqual(signature, expected)

    def test_header_signed_request(self):
        self.capture_request()

    def test_presigned_request(self):
        self.capture_request(presign=True)

    def test_existing_request_without_extra_headers(self):
        self.capture_request(extra_headers=False)


class SnowballErrorTest(unittest.TestCase):
    def run_case(self, code, body):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, 'curl.out').write_text(body)
            harness = r'''
set -uo pipefail
new_bucket() { :; }
rm_bucket() { :; }
snw_build_basic_tar() { :; }
md5_of_file() { printf fixture-md5; }
aws_run() { AWS_OUT='{"ETag":"\"fixture-md5\""}'; }
assert_aws_ok() { :; }
s3_signed_curl() {
  case " $* " in *" --presign "*) ;; *) return 1;; esac
  printf '%s' "$HTTP_CODE"
}
'''
            return subprocess.run(['bash', '-c', harness + CASE + '\nif test_snw114; then exit 0; else exit 1; fi'],
                                  capture_output=True, text=True,
                                  env={**os.environ, 'TEST_TMP': tmp, 'HTTP_CODE': code})

    def test_specific_extraction_error(self):
        for namespace in ('', ' xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'):
            result = self.run_case('400', f'<Error{namespace}><Code>InvalidRequest</Code><Message>'
                                   'x-rustfs-max-content-length is not supported for archive extraction'
                                   '</Message></Error>')
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_reject_wrong_error_path(self):
        for code, body in (
            ('403', '<Error><Code>AccessDenied</Code></Error>'),
            ('400', '<Error><Code>InvalidRequest</Code><Message>x-rustfs-max-content-length requires a SigV4 presigned request</Message></Error>'),
            ('400', '<Error><Code>EntityTooLarge</Code></Error>'),
            ('400', 'not XML'),
        ):
            with self.subTest(code=code, body=body):
                self.assertNotEqual(self.run_case(code, body).returncode, 0)


if __name__ == '__main__':
    unittest.main()
