"""Offline checks of scripts/sse_contract_cases.py.

A small in-memory S3 stands in for the server. Left alone it behaves the way the cases require, so every case must
hold on it; with one behavior changed, the case written for that behavior must fail and say why. This validates the
oracles and the wiring into the suites, not RustFS.
"""

import base64
import datetime
import hashlib
import importlib.util
import io
import json
import re
import unittest
import urllib.parse
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("sse_contract_cases", ROOT / "scripts" / "sse_contract_cases.py")
cc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cc)

DEFAULT_KEY = "kms-default"
XMLNS = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'


def _error(status, code, message="refused"):
    return status, {"content-type": "application/xml"}, f"<Error><Code>{code}</Code><Message>{message}</Message></Error>".encode()


class FakeS3:
    """The subset of S3 and of the KMS admin API the cases use. `mutations` names behaviors to get wrong."""

    admin = cc.Client.admin

    def __init__(self, master_key=False, kms=False, mutations=()):
        self.master_key, self.kms = master_key, kms
        self.mutations = set(mutations)
        self.buckets = {}       # name -> {"objects": {key: object}, "default": (algorithm, key id) | None}
        self.uploads = {}       # upload id -> {"scheme": ..., "parts": {number: bytes}}
        self.keys = {DEFAULT_KEY} if kms else set()

    # -- request parsing ----------------------------------------------------

    @staticmethod
    def _customer_key(headers, prefix=cc.SSEC):
        """(key bytes, error) for an SSE-C triple; (None, None) when the request carries none of it."""
        triple = {name[len(prefix):]: value for name, value in headers.items() if name.startswith(prefix)}
        if not triple:
            return None, None
        try:
            key = base64.b64decode(triple.get("key", ""), validate=True)
        except ValueError:
            return None, "InvalidRequest"
        complete = triple.get("algorithm") == "AES256" and len(key) == 32 and triple.get("key-md5") == cc.key_md5(key)
        return (key, None) if complete else (None, "InvalidRequest")

    def _write_scheme(self, bucket, headers):
        """The scheme a write resolves to, as (scheme dict, error response)."""
        algorithm, key_id, context = headers.get(cc.SSE), headers.get(cc.SSE_KEY_ID), headers.get(cc.SSE_CONTEXT)
        key, key_error = self._customer_key(headers)
        has_customer = any(name.startswith(cc.SSEC) for name in headers)
        if has_customer and (algorithm or key_id):
            return None, _error(400, "InvalidArgument")
        if key_error:
            code = "InvalidArgument" if "refusal-wrong-code" in self.mutations else key_error
            return None, _error(400, code)
        if key:
            return {"scheme": "sse-c", "customer_md5": cc.key_md5(key)}, None
        if algorithm not in (None, "AES256", "aws:kms") or (algorithm != "aws:kms" and (key_id or context)):
            return None, _error(400, "InvalidArgument")
        if context is not None:
            try:
                parsed = json.loads(base64.b64decode(context, validate=True))
            except ValueError:
                return None, _error(400, "InvalidArgument")
            if not isinstance(parsed, dict) or bucket in parsed:
                return None, _error(400, "InvalidArgument")
        if algorithm is None and self.buckets[bucket]["default"]:
            algorithm, key_id = self.buckets[bucket]["default"]
            if "bucket-default-ignored" in self.mutations:
                algorithm = None
        if algorithm == "aws:kms":
            if not self.kms:
                if "downgrade-kms-to-s3" in self.mutations:
                    return {"scheme": "sse-s3"}, None
                return None, _error(400, "InvalidRequest")
            key_id = key_id or DEFAULT_KEY
            if key_id not in self.keys:
                return None, _error(400, "KMS.NotFoundException")
            return {"scheme": "sse-kms", "key_id": key_id, "context": context}, None
        if algorithm == "AES256":
            if not (self.kms or self.master_key):
                return None, _error(400, "InvalidRequest")
            return {"scheme": "sse-s3"}, None
        return {"scheme": "plain"}, None

    def _sse_headers(self, stored, operation=""):
        scheme = stored["scheme"]
        if scheme == "sse-c":
            return {cc.SSEC + "algorithm": "AES256", cc.SSEC + "key-md5": stored["customer_md5"]}
        if scheme == "sse-kms":
            key_id = DEFAULT_KEY if "wrong-kms-key-in-response" in self.mutations else stored["key_id"]
            headers = {cc.SSE: "aws:kms", cc.SSE_KEY_ID: key_id}
            if "context-echoed" in self.mutations and stored.get("context"):
                headers[cc.SSE_CONTEXT] = stored["context"]
            return headers
        if scheme == "sse-s3":
            if operation == "HEAD" and "head-drops-sse" in self.mutations:
                return {}
            headers = {cc.SSE: "AES256"}
            if operation == "GET" and "extra-sse-header-on-get" in self.mutations:
                headers[cc.SSE_KEY_ID] = DEFAULT_KEY
            return headers
        return {}

    def _etag(self, data, scheme):
        plaintext_md5 = hashlib.md5(data).hexdigest()
        if scheme == "plain" or "etag-is-plaintext-md5" in self.mutations:
            return plaintext_md5
        return hashlib.md5(b"ciphertext of " + data).hexdigest()

    def _store(self, bucket, key, data, scheme, etag=None):
        stored = {**scheme, "data": data, "etag": etag or self._etag(data, scheme["scheme"])}
        self.buckets[bucket]["objects"][key] = stored
        return stored

    def _read_refusal(self, stored, headers, prefix=cc.SSEC):
        """Why a read with these headers must be refused, or None."""
        key, key_error = self._customer_key(headers, prefix)
        has_customer = any(name.startswith(prefix) for name in headers)
        if stored["scheme"] == "sse-c":
            if key is None or key_error or cc.key_md5(key) != stored["customer_md5"]:
                relabelled = "copy-relabels" in self.mutations and stored.get("copied") and key is not None
                return None if relabelled else "InvalidRequest"
            return None
        if has_customer:
            return "InvalidRequest" if stored["scheme"] == "plain" else "InvalidArgument"
        if prefix == cc.SSEC and cc.SSE in headers:
            return "InvalidArgument"
        return None

    # -- the endpoint --------------------------------------------------------

    def request(self, method, path, query="", body=None, headers=None):
        headers = {name.lower(): value for name, value in (headers or {}).items()}
        params = dict(urllib.parse.parse_qsl(query, keep_blank_values=True))
        if path.startswith(cc.ADMIN_PREFIX + "/"):
            return self._admin(method, path[len(cc.ADMIN_PREFIX) + 1:], body)
        bucket, _, key = path.lstrip("/").partition("/")
        if not key:
            return self._bucket(method, bucket, params, body)
        if bucket not in self.buckets:
            return _error(404, "NoSuchBucket")
        if "uploads" in params or "uploadId" in params:
            return self._multipart(method, bucket, key, params, body, headers)
        if method == "PUT" and "x-amz-copy-source" in headers:
            return self._copy(bucket, key, headers)
        if method == "PUT":
            scheme, refusal = self._write_scheme(bucket, headers)
            if refusal:
                if "store-refused-write" in self.mutations:
                    self._store(bucket, key, body or b"", {"scheme": "plain"})
                return refusal
            stored = self._store(bucket, key, body or b"", scheme)
            return 200, {"etag": f'"{stored["etag"]}"', **self._sse_headers(stored, "PUT")}, b""
        stored = self.buckets[bucket]["objects"].get(key)
        if method == "DELETE":
            self.buckets[bucket]["objects"].pop(key, None)
            return 204, {}, b""
        if stored is None:
            return _error(404, "NoSuchKey")
        refusal = self._read_refusal(stored, headers)
        if refusal:
            if method == "GET" and "refused-get-serves-bytes" in self.mutations:
                return 400, {}, _error(400, refusal)[2] + stored["data"]
            return _error(400, refusal) if method == "GET" else (400, {}, b"")
        data = stored["data"]
        response = {"etag": f'"{stored["etag"]}"', "content-length": str(len(data)), **self._sse_headers(stored, method)}
        if method == "HEAD":
            return 200, response, b""
        if "range" in headers:
            first, _, last = headers["range"][len("bytes="):].partition("-")
            start = len(data) - int(last) if first == "" else int(first)
            end = len(data) - 1 if first == "" or last == "" else int(last)
            part = data[start:end + 1]
            return 206, {**response, "content-length": str(len(part)), "content-range": f"bytes {start}-{end}/{len(data)}"}, part
        return 200, response, data

    def _bucket(self, method, bucket, params, body):
        if "encryption" in params:
            if method == "PUT":
                text = (body or b"").decode()
                algorithm = re.search(r"<SSEAlgorithm>(.*?)</SSEAlgorithm>", text).group(1)
                key_id = re.search(r"<KMSMasterKeyID>(.*?)</KMSMasterKeyID>", text)
                self.buckets[bucket]["default"] = (algorithm, key_id.group(1) if key_id else
                                                   (DEFAULT_KEY if algorithm == "aws:kms" else None))
                return 200, {}, b""
            if method == "DELETE":
                self.buckets[bucket]["default"] = None
                return 204, {}, b""
            algorithm, key_id = self.buckets[bucket]["default"]
            key_xml = f"<KMSMasterKeyID>{key_id}</KMSMasterKeyID>" if key_id else ""
            return 200, {}, (f"<ServerSideEncryptionConfiguration {XMLNS}><Rule><ApplyServerSideEncryptionByDefault>"
                             f"<SSEAlgorithm>{algorithm}</SSEAlgorithm>{key_xml}</ApplyServerSideEncryptionByDefault></Rule>"
                             "</ServerSideEncryptionConfiguration>").encode()
        if method == "PUT":
            self.buckets[bucket] = {"objects": {}, "default": None}
            return 200, {}, b""
        if method == "DELETE":
            self.buckets.pop(bucket, None)
            return 204, {}, b""
        entry = "Version" if "versions" in params else "Contents"
        items = []
        for key, stored in sorted(self.buckets[bucket]["objects"].items()):
            size = len(stored["data"]) + (32 if "listing-ciphertext-size" in self.mutations and stored["scheme"] != "plain" else 0)
            items.append(f'<{entry}><Key>{key}</Key><Size>{size}</Size><ETag>"{stored["etag"]}"</ETag></{entry}>')
        return 200, {}, f"<ListResult {XMLNS}>{''.join(items)}</ListResult>".encode()

    def _copy(self, bucket, key, headers):
        source_bucket, _, source_key = urllib.parse.unquote(headers["x-amz-copy-source"]).lstrip("/").partition("/")
        source = self.buckets[source_bucket]["objects"].get(source_key)
        if source is None:
            return _error(404, "NoSuchKey")
        if self._read_refusal(source, headers, cc.COPY_SSEC):
            return _error(400, "InvalidRequest")
        destination = {name: value for name, value in headers.items() if not name.startswith(cc.COPY_SSEC)}
        scheme, refusal = self._write_scheme(bucket, destination)
        if refusal:
            return refusal
        stored = self._store(bucket, key, source["data"], {**scheme, "copied": True})
        return 200, self._sse_headers(stored, "PUT"), f"<CopyObjectResult {XMLNS}><ETag>{stored['etag']}</ETag></CopyObjectResult>".encode()

    def _multipart(self, method, bucket, key, params, body, headers):
        if "uploads" in params:
            scheme, refusal = self._write_scheme(bucket, headers)
            if refusal:
                return refusal
            upload_id = f"upload-{len(self.uploads) + 1}"
            self.uploads[upload_id] = {"scheme": scheme, "parts": {}}
            return 200, self._sse_headers(scheme), (f"<InitiateMultipartUploadResult {XMLNS}><UploadId>{upload_id}</UploadId>"
                                                     "</InitiateMultipartUploadResult>").encode()
        upload = self.uploads[params["uploadId"]]
        scheme = upload["scheme"]
        if method == "GET":
            return 200, {}, f"<ListPartsResult {XMLNS}><UploadId>{params['uploadId']}</UploadId></ListPartsResult>".encode()
        customer_key, key_error = self._customer_key(headers)
        has_customer = any(name.startswith(cc.SSEC) for name in headers)
        matches = customer_key is not None and not key_error and cc.key_md5(customer_key) == scheme.get("customer_md5")
        if (scheme["scheme"] == "sse-c") != has_customer or (has_customer and not matches):
            return _error(400, "InvalidRequest")
        if method == "PUT":
            upload["parts"][int(params["partNumber"])] = body or b""
            return 200, {"etag": f'"{hashlib.md5(body or b"").hexdigest()}"', **self._sse_headers(scheme)}, b""
        data = b"".join(part for _, part in sorted(upload["parts"].items()))
        stored = self._store(bucket, key, data, scheme, etag=f"{hashlib.md5(data).hexdigest()}-{len(upload['parts'])}")
        return 200, self._sse_headers(stored), f"<CompleteMultipartUploadResult {XMLNS}><Key>{key}</Key></CompleteMultipartUploadResult>".encode()

    def _admin(self, method, subpath, body):
        if not self.kms:
            return 503, {}, json.dumps({"success": False, "message": "kms service is not running"}).encode()
        if subpath == "kms/status":
            return 200, {}, json.dumps({"backend_status": "healthy", "default_key_id": DEFAULT_KEY}).encode()
        if subpath == "kms/keys" and method == "POST":
            self.keys.add(json.loads(body)["tags"]["name"])
            return 200, {}, json.dumps({"success": True}).encode()
        name = urllib.parse.unquote(subpath[len("kms/keys/"):])
        return (200 if name in self.keys else 404), {}, b"{}"


def run(case_id, **server):
    kms = cc.CASES[case_id][1]
    fake = FakeS3(master_key=not kms, kms=kms, **server)
    return cc.run_case(fake, case_id, DEFAULT_KEY if kms else "")


class ConformingServerTest(unittest.TestCase):
    def test_every_case_holds(self):
        for case_id in cc.CASES:
            with self.subTest(case=case_id):
                failures, evaluated = run(case_id)
                self.assertEqual(failures, [])
                self.assertGreater(evaluated, 5)

    def test_cases_do_not_hold_without_their_key_source(self):
        """ENC-201 needs the master key and KMS-602 needs KMS: on a server with neither they must not pass."""
        for case_id in ("ENC-201", "ENC-203", "ENC-204", "ENC-210", "KMS-602"):
            with self.subTest(case=case_id):
                failures, _ = cc.run_case(FakeS3(), case_id, DEFAULT_KEY)
                self.assertNotEqual(failures, [])


class MutatedServerTest(unittest.TestCase):
    """One wrong behavior at a time: the named case fails, and its message names what it saw."""

    EXPECTED = (
        ("extra-sse-header-on-get", "ENC-201", "expected exactly"),
        ("extra-sse-header-on-get", "KMS-601", "the response carries " + cc.SSE_KEY_ID),
        ("head-drops-sse", "ENC-201", "HEAD: SSE headers {}"),
        ("listing-ciphertext-size", "ENC-201", "expected the plaintext size"),
        ("refused-get-serves-bytes", "ENC-209", "the refused GET returned object bytes"),
        ("bucket-default-ignored", "ENC-203", "[default-applies]"),
        ("downgrade-kms-to-s3", "ENC-204", "SSE-KMS PUT: HTTP 200"),
        ("downgrade-kms-to-s3", "ENC-204", "nothing may be stored"),
        ("refusal-wrong-code", "ENC-205", "expected 400 'InvalidRequest'"),
        ("store-refused-write", "ENC-205", "nothing may be stored"),
        ("store-refused-write", "KMS-607", "nothing may be stored"),
        ("copy-relabels", "ENC-206", "destination GET with the source key"),
        ("copy-relabels", "KMS-605", "[ssec-to-ssec]"),
        ("etag-is-plaintext-md5", "ENC-210", "is the MD5 of the plaintext"),
        ("etag-is-plaintext-md5", "KMS-609", "is the MD5 of the plaintext"),
        ("wrong-kms-key-in-response", "KMS-602", "expected exactly"),
        ("wrong-kms-key-in-response", "KMS-603", "expected exactly"),
        ("wrong-kms-key-in-response", "KMS-606", "expected exactly"),
        ("context-echoed", "KMS-609", "repeats the encryption context"),
    )

    def test_each_mutation_is_caught(self):
        for mutation, case_id, evidence in self.EXPECTED:
            with self.subTest(mutation=mutation, case=case_id):
                failures, _ = run(case_id, mutations=[mutation])
                self.assertTrue(any(evidence in failure for failure in failures),
                                f"{case_id} did not report {evidence!r} under {mutation}: {failures}")

    def test_every_mutation_the_fake_knows_is_exercised(self):
        known = set(re.findall(r'"([a-z0-9-]+)" in self\.mutations', Path(__file__).read_text()))
        self.assertEqual(known, {mutation for mutation, _, _ in self.EXPECTED})


class OracleTest(unittest.TestCase):
    def test_a_case_without_expectations_does_not_pass(self):
        cc.CASES["ENC-299"] = ("Vacuous", False, lambda c: None)
        try:
            failures, evaluated = cc.run_case(FakeS3(master_key=True), "ENC-299")
        finally:
            del cc.CASES["ENC-299"]
        self.assertEqual((failures, evaluated), (["the case evaluated no expectation"], 0))

    def test_a_broken_row_does_not_hide_the_next_one(self):
        ctx = cc.Ctx(FakeS3(master_key=True), "ENC-000")
        with ctx.row("first"):
            ctx.need(False, "stops this row")
        with ctx.row("second"):
            ctx.check(False, "still evaluated")
        self.assertEqual(ctx.failures, ["[first] stops this row", "[second] still evaluated"])

    def test_no_response_ends_the_case_as_a_failure(self):
        class Down(FakeS3):
            def request(self, method, path, query="", body=None, headers=None):
                raise cc.NoResponse(f"{method} {path}: no HTTP response")

        failures, _ = cc.run_case(Down(master_key=True), "ENC-205")
        self.assertEqual(len(failures), 1)
        self.assertIn("no HTTP response", failures[0])


class CommandLineTest(unittest.TestCase):
    def invoke(self, *argv, env=None):
        saved = {name: cc.os.environ.get(name) for name in ("RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY")}
        cc.os.environ.update(env if env is not None else {"RUSTFS_ACCESS_KEY": "ak", "RUSTFS_SECRET_KEY": "sk"})
        out, err = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(out), redirect_stderr(err):
                try:
                    status = cc.main(list(argv))
                except SystemExit as exit_:
                    status = exit_.code
        finally:
            for name, value in saved.items():
                if value is None:
                    cc.os.environ.pop(name, None)
                else:
                    cc.os.environ[name] = value
        return status, out.getvalue(), err.getvalue()

    def test_usage_errors_are_not_read_as_unsupported(self):
        """The suites take exit status 2 for UNSUPPORTED; a wrong command line must be a failure instead."""
        for argv in (["--case", "ENC-201"], ["--endpoint", "http://node:9000", "--case", "ENC-999"],
                     ["--endpoint", "http://node:9000", "--case", "KMS-601"],
                     ["--endpoint", "node:9000", "--case", "ENC-201"]):
            with self.subTest(argv=argv):
                status, _, err = self.invoke(*argv)
                self.assertEqual(status, cc.EXIT_USAGE, err)

    def test_missing_credentials_are_a_usage_error(self):
        saved = {name: cc.os.environ.pop(name, None) for name in ("RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY")}
        try:
            status, _, err = self.invoke("--endpoint", "http://node:9000", "--case", "ENC-201", env={})
        finally:
            cc.os.environ.update({name: value for name, value in saved.items() if value is not None})
        self.assertEqual(status, cc.EXIT_USAGE)
        self.assertIn("RUSTFS_ACCESS_KEY", err)

    def test_unreachable_endpoint_fails_the_case(self):
        status, out, _ = self.invoke("--endpoint", "http://127.0.0.1:1", "--case", "ENC-205")
        self.assertEqual(status, 1)
        self.assertIn("no HTTP response", out)

    def test_list_names_every_case(self):
        status, out, _ = self.invoke("--list")
        self.assertEqual(status, 0)
        self.assertEqual([line.split()[0] for line in out.splitlines()], list(cc.CASES))


class SuiteWiringTest(unittest.TestCase):
    """Every case is registered in the suite that starts the server it needs, under the helper's own name."""

    def registered(self, script):
        text = (ROOT / script).read_text()
        rows = re.findall(r'^\s*run_test (\S+)\s+"([^"]+)"\s+(\S+)$', text, re.MULTILINE)
        functions = dict(re.findall(r"^(test_\w+)\(\) \{ sse_contract_case (\S+); \}$", text, re.MULTILINE))
        return {case_id: (name, functions.get(function)) for case_id, name, function in rows}, text

    def test_registration_matches_the_helper(self):
        for script, needs_kms in (("rustfs-s3-compat-test.sh", False), ("rustfs-kms-test.sh", True)):
            registered, text = self.registered(script)
            for case_id, (name, kms, _) in cc.CASES.items():
                with self.subTest(script=script, case=case_id):
                    if kms == needs_kms:
                        self.assertEqual(registered.get(case_id), (name, case_id))
                    else:
                        self.assertNotIn(case_id, registered)
            self.assertIn('[ -f "${SCRIPT_DIR}/scripts/sse_contract_cases.py" ]', text)

    def test_kms_suite_passes_its_default_key(self):
        text = (ROOT / "rustfs-kms-test.sh").read_text()
        self.assertIn('--kms-default-key "${KMS_DEFAULT_KEY_ID}"', text)


class SignatureTest(unittest.TestCase):
    def test_signature_matches_an_independent_signer(self):
        try:
            from botocore.auth import S3SigV4Auth
            from botocore.awsrequest import AWSRequest
            from botocore.credentials import Credentials
        except ImportError:
            self.skipTest("botocore is not installed")
        now = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)  # noqa: UP017 - Python 3.8 has no datetime.UTC
        path, query = "/bucket/a key/with+plus", "partNumber=1&uploadId=" + urllib.parse.quote("a/b+c=", safe="")
        headers = {cc.SSE: "aws:kms", cc.SSE_KEY_ID: "key with  two spaces", "content-type": "application/octet-stream"}
        signed = cc.sign("PUT", "node:9000", path, query, headers, "ak@id", "sk", now)
        authorization = signed.pop("authorization")
        self.assertEqual(sorted(signed), authorization.split("SignedHeaders=")[1].split(",")[0].split(";"))

        url = "http://node:9000" + urllib.parse.quote(path, safe="/-._~") + "?" + query
        request = AWSRequest(method="PUT", url=url, headers=signed)
        request.context["timestamp"] = signed["x-amz-date"]
        request.context["payload_signing_enabled"] = False
        verifier = S3SigV4Auth(Credentials("ak@id", "sk"), cc.SERVICE, cc.REGION)
        expected = verifier.signature(verifier.string_to_sign(request, verifier.canonical_request(request)), request)
        self.assertEqual(authorization.split("Signature=")[1], expected)


if __name__ == "__main__":
    unittest.main()
