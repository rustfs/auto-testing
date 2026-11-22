import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

from botocore.credentials import Credentials

from scripts.s3_upload_part_timeout import expect_error, put_wire, short_body


class UploadPartWireTests(unittest.TestCase):
    def test_partial_body_stays_open_while_client_waits_for_response(self):
        seen = {}

        class Capture(BaseHTTPRequestHandler):
            def do_PUT(self):
                seen.update(path=self.path, length=self.headers["Content-Length"],
                            authorization=self.headers["Authorization"], prefix=self.rfile.read(3))
                self.connection.settimeout(0.15)
                try:
                    seen["remainder"] = self.rfile.read(1)
                except socket.timeout:
                    seen["open"] = True
                body = b"<Error><Code>RequestTimeout</Code></Error>"
                self.send_response(400)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        with HTTPServer(("127.0.0.1", 0), Capture) as server:
            server.timeout = 2
            worker = threading.Thread(target=server.handle_request)
            worker.start()
            try:
                response = put_wire(server.server_port, Credentials("fixture", "disposable-secret"),
                                    "an/id+with=symbols", b"abc", {"Content-Length": "4",
                                    "x-amz-content-sha256": "UNSIGNED-PAYLOAD"})
            finally:
                worker.join(timeout=3)
        expect_error(*response, "RequestTimeout")
        self.assertTrue(seen.get("open"), seen)
        self.assertEqual((seen["prefix"], seen["length"]), (b"abc", "4"))
        self.assertEqual(parse_qs(urlsplit(seen["path"]).query),
                         {"partNumber": ["1"], "uploadId": ["an/id+with=symbols"]})
        self.assertIn("AWS4-HMAC-SHA256 Credential=fixture/", seen["authorization"])

    def test_short_s3_payload_is_a_complete_http_entity_with_a_valid_checksum(self):
        wire, headers = short_body(b"abc")
        self.assertEqual(wire, b"3\r\nabc\r\n0\r\nx-amz-checksum-crc32:NSRBwg==\r\n\r\n")
        self.assertEqual(int(headers["Content-Length"]), len(wire))
        self.assertEqual(headers["x-amz-decoded-content-length"], "4")

    def test_wrong_s3_code_or_http_status_fails(self):
        body = b'<Error xmlns="urn:test"><Code>IncompleteBody</Code></Error>'
        expect_error(400, body, "IncompleteBody")
        for status, expected in [(400, "RequestTimeout"), (408, "IncompleteBody")]:
            with self.subTest(status=status, expected=expected), self.assertRaises(RuntimeError):
                expect_error(status, body, expected)


if __name__ == "__main__":
    unittest.main()
