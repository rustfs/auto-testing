#!/usr/bin/env python3
"""Strict server-side-encryption contract cases against an existing endpoint.

The cluster suites call one case at a time and take the exit status as its verdict:

    RUSTFS_ACCESS_KEY=... RUSTFS_SECRET_KEY=... \
      python3 scripts/sse_contract_cases.py --endpoint http://node:9000 --case ENC-201

A case states what a client must observe, exactly:

- a response carries the SSE headers of its scheme and no other SSE header;
- a refusal has the expected HTTP status and S3 error code;
- a refused write is followed by a HEAD that must answer 404, so nothing was stored;
- bytes read back are compared by SHA-256 and by Content-Length.

The `ENC-2xx` cases need a server with `RUSTFS_SSE_S3_MASTER_KEY` and no KMS (the S3 compatibility suite). The
`KMS-6xx` cases need a running KMS with a default key (the KMS suite); they create two more keys through the admin
API. Nothing is deployed, restarted or reconfigured. Standard library only.

Exit status: 0 the case holds, 1 it does not (an unreachable endpoint included), 64 the command line is wrong.
Status 2 is never used: the suites read it as "unsupported".
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import datetime
import hashlib
import hmac
import http.client
import json
import os
import secrets
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET

ADMIN_PREFIX = "/rustfs/admin/v3"
REGION = "us-east-1"
SERVICE = "s3"
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
REQUEST_TIMEOUT = 120.0
EXIT_USAGE = 64

KIB = 1024
MIB = 1024 * 1024
SIZE = 70 * KIB
PART = 5 * MIB

SSE = "x-amz-server-side-encryption"
SSE_KEY_ID = SSE + "-aws-kms-key-id"
SSE_CONTEXT = SSE + "-context"
SSEC = SSE + "-customer-"
COPY_SSEC = "x-amz-copy-source-server-side-encryption-customer-"

# Keys the KMS cases create next to the suite's default key. Two, so a copy between keys cannot pass by reuse.
KEY_1 = "sse-contract-key-1"
KEY_2 = "sse-contract-key-2"
MISSING_KEY = "sse-contract-key-that-was-never-created"


class CaseFailed(Exception):
    """An expectation later steps depend on does not hold; the case ends here."""


class NoResponse(CaseFailed):
    """The endpoint did not answer. The suites have no verdict for that, so it fails the case."""


# ---------------------------------------------------------------------------
# Deterministic test data
# ---------------------------------------------------------------------------

def plain(seed, size):
    """Incompressible bytes fully determined by `seed`."""
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"sse-contract plaintext {seed} {counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def derived_key(label, size=32):
    """SSE-C key material derived at run time; the repository carries no key literal."""
    return hashlib.sha256(f"sse-contract key {label}".encode()).digest()[:size]


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


KEY_A = derived_key("ssec a")
KEY_B = derived_key("ssec b")


# ---------------------------------------------------------------------------
# SigV4 client
# ---------------------------------------------------------------------------

def _hmac(key, message):
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def _uri_encode(value, encode_slash):
    return urllib.parse.quote(value, safe="-._~" + ("" if encode_slash else "/"))


def _canonical_query(query):
    pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
    return "&".join(f"{k}={v}" for k, v in sorted((_uri_encode(k, True), _uri_encode(v, True)) for k, v in pairs))


def sign(method, host, path, query, headers, access_key, secret_key, now):
    """Header-signed SigV4 with an unsigned payload: the complete request headers, lower-cased."""
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    signed = {"host": host, "x-amz-content-sha256": UNSIGNED_PAYLOAD, "x-amz-date": amz_date}
    signed.update({name.lower(): value for name, value in (headers or {}).items()})
    names = ";".join(sorted(signed))
    canonical = "\n".join([method, _uri_encode(path, False), _canonical_query(query),
                           "".join(f"{name}:{' '.join(signed[name].split())}\n" for name in sorted(signed)), names, UNSIGNED_PAYLOAD])
    scope = f"{date_stamp}/{REGION}/{SERVICE}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, sha256_hex(canonical.encode())])
    key = _hmac(_hmac(_hmac(_hmac(f"AWS4{secret_key}".encode(), date_stamp), REGION), SERVICE), "aws4_request")
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    signed["authorization"] = f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={names}, Signature={signature}"
    return signed


class Client:
    """One endpoint and one identity. `request` returns (status, lower-cased headers, body) for every HTTP answer."""

    def __init__(self, endpoint, access_key, secret_key):
        parts = urllib.parse.urlsplit(endpoint)
        if parts.scheme not in ("http", "https") or not parts.netloc or parts.path not in ("", "/"):
            raise ValueError(f"endpoint must be http(s)://host[:port], got {endpoint!r}")
        self.scheme, self.netloc = parts.scheme, parts.netloc
        self.access_key, self.secret_key = access_key, secret_key

    def request(self, method, path, query="", body=None, headers=None):
        now = datetime.datetime.now(datetime.timezone.utc)  # noqa: UP017 - the runner may have Python 3.8
        signed = sign(method, self.netloc, path, query, headers, self.access_key, self.secret_key, now)
        url = urllib.parse.quote(path, safe="/-._~") + (f"?{query}" if query else "")
        connection_type = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        # A direct connection: the endpoint is a cluster node, and a proxy from the environment must not answer for it.
        connection = connection_type(self.netloc, timeout=REQUEST_TIMEOUT)
        try:
            connection.request(method, url, body=body, headers={k: v for k, v in signed.items() if k != "host"})
            response = connection.getresponse()
            response_headers = {name.lower(): value for name, value in response.getheaders()}
            try:
                return response.status, response_headers, response.read()
            except http.client.IncompleteRead as err:
                raise CaseFailed(f"{method} {path}: HTTP {response.status}, then the body ended after {len(err.partial)} "
                                 f"of {response_headers.get('content-length')} bytes") from err
        except (OSError, http.client.HTTPException) as err:
            raise NoResponse(f"{method} {path}: no HTTP response from {self.netloc} ({err})") from err
        finally:
            connection.close()

    def admin(self, method, subpath, query="", payload=None):
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {"content-type": "application/json"} if body is not None else None
        status, _, raw = self.request(method, f"{ADMIN_PREFIX}/{subpath}", query, body, headers)
        try:
            return status, json.loads(raw) if raw else None
        except ValueError:
            return status, raw.decode(errors="replace")


def s3_error(body):
    """(Code, Message) of an S3 XML error document; ('', '') when the body is not one."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return "", ""
    return root.findtext("{*}Code") or root.findtext("Code") or "", root.findtext("{*}Message") or root.findtext("Message") or ""


# ---------------------------------------------------------------------------
# SSE request headers and the response headers each scheme must produce
# ---------------------------------------------------------------------------

def sse_s3():
    return {SSE: "AES256"}


def sse_kms(key_id=None, context=None):
    headers = {SSE: "aws:kms"}
    if key_id:
        headers[SSE_KEY_ID] = key_id
    if context is not None:
        headers[SSE_CONTEXT] = base64.b64encode(json.dumps(context, separators=(",", ":")).encode()).decode()
    return headers


def key_md5(key):
    return base64.b64encode(hashlib.md5(key).digest()).decode()


def ssec(key, prefix=SSEC):
    """The SSE-C triple; `COPY_SSEC` as the prefix gives the copy-source variant."""
    return {prefix + "algorithm": "AES256", prefix + "key": base64.b64encode(key).decode(), prefix + "key-md5": key_md5(key)}


def projection(headers):
    """The SSE headers actually present in a response."""
    return {name: value for name, value in headers.items() if name.startswith(SSE)}


def projection_s3():
    return {SSE: "AES256"}


def projection_kms(key_id):
    return {SSE: "aws:kms", SSE_KEY_ID: key_id}


def projection_c(key):
    return {SSEC + "algorithm": "AES256", SSEC + "key-md5": key_md5(key)}


# ---------------------------------------------------------------------------
# S3 operations
# ---------------------------------------------------------------------------

def put(c, bucket, key, data, headers=None):
    return c.request("PUT", f"/{bucket}/{key}", body=data, headers={"content-type": "application/octet-stream", **(headers or {})})


def get(c, bucket, key, headers=None, range_=None):
    request_headers = dict(headers or {})
    if range_:
        request_headers["range"] = range_
    return c.request("GET", f"/{bucket}/{key}", headers=request_headers or None)


def head(c, bucket, key, headers=None):
    status, response_headers, _ = c.request("HEAD", f"/{bucket}/{key}", headers=headers)
    return status, response_headers


def copy(c, bucket, key, source_bucket, source_key, headers=None):
    """CopyObject; `headers` carries the destination SSE headers and the copy-source SSE-C triple."""
    return c.request("PUT", f"/{bucket}/{key}",
                     headers={"x-amz-copy-source": f"/{source_bucket}/{urllib.parse.quote(source_key)}", **(headers or {})})


def create_multipart(c, bucket, key, headers=None):
    status, response_headers, body = c.request("POST", f"/{bucket}/{key}", query="uploads=", headers=headers)
    upload_id = None
    if status == 200:
        with contextlib.suppress(ET.ParseError):
            upload_id = ET.fromstring(body).findtext("{*}UploadId")
    return status, response_headers, upload_id, body


def upload_part(c, bucket, key, upload_id, number, data, headers=None):
    return c.request("PUT", f"/{bucket}/{key}", query=f"partNumber={number}&uploadId={_uri_encode(upload_id, True)}",
                     body=data, headers=headers)


def complete_multipart(c, bucket, key, upload_id, parts, headers=None):
    parts_xml = "".join(f"<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>" for number, etag in parts)
    return c.request("POST", f"/{bucket}/{key}", query=f"uploadId={_uri_encode(upload_id, True)}",
                     body=f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode(),
                     headers={"content-type": "application/xml", **(headers or {})})


def list_parts(c, bucket, key, upload_id):
    return c.request("GET", f"/{bucket}/{key}", query=f"uploadId={_uri_encode(upload_id, True)}")


def listing(c, bucket, query, entry):
    """{key: (size, ETag without quotes)} from one page of a listing; None when the listing is refused."""
    status, _, body = c.request("GET", f"/{bucket}", query=query)
    if status != 200:
        return None
    return {item.findtext("{*}Key"): (int(item.findtext("{*}Size") or -1), (item.findtext("{*}ETag") or "").strip('"'))
            for item in ET.fromstring(body).findall("{*}" + entry)}


def put_bucket_encryption(c, bucket, algorithm, kms_key_id=None):
    key_xml = f"<KMSMasterKeyID>{kms_key_id}</KMSMasterKeyID>" if kms_key_id else ""
    payload = ('<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule>'
               f"<ApplyServerSideEncryptionByDefault><SSEAlgorithm>{algorithm}</SSEAlgorithm>{key_xml}"
               "</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>").encode()
    return c.request("PUT", f"/{bucket}", query="encryption=", body=payload, headers={"content-type": "application/xml"})


def get_bucket_encryption(c, bucket):
    """(status, {'algorithm', 'kms_key_id'})."""
    status, _, body = c.request("GET", f"/{bucket}", query="encryption=")
    config = {}
    if status == 200:
        root = ET.fromstring(body)
        config = {"algorithm": root.findtext(".//{*}SSEAlgorithm"), "kms_key_id": root.findtext(".//{*}KMSMasterKeyID")}
    return status, config


def delete_bucket_encryption(c, bucket):
    return c.request("DELETE", f"/{bucket}", query="encryption=")


def ensure_kms_key(c, name):
    """The named KMS key exists afterwards, or the case cannot start."""
    quoted = urllib.parse.quote(name, safe="")
    if c.admin("GET", f"kms/keys/{quoted}")[0] == 200:
        return
    created, response = c.admin("POST", "kms/keys", payload={"tags": {"name": name}})
    if c.admin("GET", f"kms/keys/{quoted}")[0] != 200:
        raise CaseFailed(f"KMS key {name} could not be created: HTTP {created} {str(response)[:200]}")


# ---------------------------------------------------------------------------
# Expectations
# ---------------------------------------------------------------------------

class Ctx:
    """One case: the client, the failures found so far, and how many expectations were evaluated."""

    def __init__(self, client, case_id, default_key=""):
        self.client = client
        self.case_id = case_id
        self.default_key = default_key
        self.failures = []
        self.evaluated = 0
        self.buckets = []
        self._row = ""

    def check(self, condition, message):
        self.evaluated += 1
        if not condition:
            self.failures.append(f"[{self._row}] {message}" if self._row else message)
        return bool(condition)

    def need(self, condition, message):
        self.evaluated += 1
        if not condition:
            raise CaseFailed(message)

    @contextlib.contextmanager
    def row(self, name):
        """One input of a table-driven case: a broken row is reported and the next one still runs."""
        self._row = name
        try:
            yield
        except NoResponse:
            raise
        except CaseFailed as err:
            self.failures.append(f"[{name}] {err}")
        finally:
            self._row = ""

    def ok(self, response, what, want=200):
        """The exchange answered `want`; anything else ends the case or the row, since later steps depend on it."""
        status, headers, body = response
        code, message = s3_error(body)
        self.need(status == want, f"{what}: HTTP {status} {code} {message[:200]!r}, expected {want}")
        return headers, body

    def refused(self, response, status, code, what):
        """An explicit refusal: the exact status and the exact S3 error code."""
        got_status, _, body = response
        got_code, got_message = s3_error(body)
        return self.check(got_status == status and got_code == code,
                          f"{what}: HTTP {got_status} {got_code!r} {got_message[:200]!r}, expected {status} {code!r}")

    def projection(self, headers, want, what):
        """The SSE headers present are exactly `want`: nothing missing and nothing extra."""
        got = projection(headers)
        self.check(got == want, f"{what}: SSE headers {got}, expected exactly {want}")

    def bytes_equal(self, headers, body, data, what):
        self.check(sha256_hex(body) == sha256_hex(data),
                   f"{what}: body sha256 {sha256_hex(body)[:16]} len {len(body)}, expected {sha256_hex(data)[:16]} len {len(data)}")
        self.check(headers.get("content-length") == str(len(data)),
                   f"{what}: Content-Length {headers.get('content-length')}, expected {len(data)}")

    def absent(self, bucket, key, what, headers=None):
        """A refused write left nothing behind."""
        status, _ = head(self.client, bucket, key, headers)
        self.check(status == 404, f"{what}: HEAD after the refused write is {status}, expected 404 (nothing may be stored)")

    def bucket(self):
        """A bucket of this run only, so a rerun on a kept environment starts from nothing."""
        name = f"sse-{self.case_id.lower()}-{secrets.token_hex(4)}"
        deadline = time.monotonic() + 30.0
        while True:
            status, _, body = self.client.request("PUT", f"/{name}")
            if status < 500 or time.monotonic() > deadline:
                break
            time.sleep(0.5)
        self.need(status == 200, f"CreateBucket {name}: HTTP {status} {s3_error(body)}")
        self.buckets.append(name)
        return name

    def roundtrip(self, bucket, key, data, write_headers, read_headers, want, what):
        """PUT, GET and HEAD one object: the SSE headers hold on all three and the bytes come back unchanged."""
        put_headers, _ = self.ok(put(self.client, bucket, key, data, write_headers), f"{what} PUT")
        self.projection(put_headers, want, f"{what} PUT")
        self.read_back(bucket, key, data, read_headers, want, what)
        status, head_headers = head(self.client, bucket, key, read_headers)
        if self.check(status == 200, f"{what} HEAD: HTTP {status}, expected 200"):
            self.projection(head_headers, want, f"{what} HEAD")
            self.check(head_headers.get("content-length") == str(len(data)),
                       f"{what} HEAD: Content-Length {head_headers.get('content-length')}, expected {len(data)}")

    def read_back(self, bucket, key, data, read_headers, want, what):
        get_headers, body = self.ok(get(self.client, bucket, key, read_headers), f"{what} GET")
        self.projection(get_headers, want, f"{what} GET")
        self.bytes_equal(get_headers, body, data, f"{what} GET")

    def upload(self, bucket, key, parts, create_headers, part_headers, complete_headers):
        """A full multipart upload; returns the CreateMultipartUpload and CompleteMultipartUpload response headers."""
        status, create_response, upload_id, body = create_multipart(self.client, bucket, key, create_headers)
        self.need(status == 200 and upload_id, f"CreateMultipartUpload: HTTP {status} {s3_error(body)}")
        etags = []
        for number, data in enumerate(parts, start=1):
            part_response, _ = self.ok(upload_part(self.client, bucket, key, upload_id, number, data, part_headers),
                                       f"UploadPart {number}")
            etags.append((number, part_response.get("etag", "")))
        complete_response, _ = self.ok(complete_multipart(self.client, bucket, key, upload_id, etags, complete_headers),
                                       "CompleteMultipartUpload")
        return create_response, complete_response

    def cleanup(self):
        """Best effort: the verdict is already decided, and the suites remove the data directories afterwards."""
        for bucket in self.buckets:
            with contextlib.suppress(Exception):
                for key in listing(self.client, bucket, "list-type=2", "Contents") or {}:
                    self.client.request("DELETE", f"/{bucket}/{key}")
                self.client.request("DELETE", f"/{bucket}")


# ---------------------------------------------------------------------------
# Cases for a server with a local master key and no KMS
# ---------------------------------------------------------------------------

def _sizes(c, bucket, schemes, seed):
    """Sizes 0, 1 and 70 KiB under each scheme, and the listing reports the plaintext sizes."""
    expected = {}
    for name, write_headers, read_headers, want in schemes:
        for size in (0, 1, SIZE):
            key = f"{name}-{size}"
            with c.row(key):
                c.roundtrip(bucket, key, plain(f"{seed} {key}", size), write_headers, read_headers, want, key)
                expected[key] = size
    listed = listing(c.client, bucket, "list-type=2", "Contents")
    c.need(listed is not None, "ListObjectsV2 was refused")
    for key, size in expected.items():
        c.check(listed.get(key, (None, ""))[0] == size,
                f"ListObjectsV2 reports size {listed.get(key, (None, ''))[0]} for {key}, expected the plaintext size {size}")


def enc_201(c):
    bucket = c.bucket()
    _sizes(c, bucket, [("sse-s3", sse_s3(), None, projection_s3())], "enc-201")


def enc_202(c):
    bucket = c.bucket()
    _sizes(c, bucket, [("sse-c", ssec(KEY_A), ssec(KEY_A), projection_c(KEY_A))], "enc-202")
    c.refused(get(c.client, bucket, f"sse-c-{SIZE}"), 400, "InvalidRequest", "GET without the customer key")


def enc_203(c):
    bucket = c.bucket()
    before, after = plain("enc-203 before", SIZE), plain("enc-203 after", SIZE)
    c.ok(put_bucket_encryption(c.client, bucket, "AES256"), "PutBucketEncryption")
    with c.row("default-applies"):
        c.roundtrip(bucket, "before", before, None, None, projection_s3(), "PUT that names no scheme")
    with c.row("sse-c-stays-sse-c"):
        c.roundtrip(bucket, "customer", plain("enc-203 customer", SIZE), ssec(KEY_A), ssec(KEY_A), projection_c(KEY_A), "SSE-C")
        c.refused(get(c.client, bucket, "customer"), 400, "InvalidRequest", "GET without the customer key")
    c.ok(delete_bucket_encryption(c.client, bucket), "DeleteBucketEncryption", want=204)
    with c.row("default-removed"):
        c.roundtrip(bucket, "after", after, None, None, {}, "PUT after the default was removed")
        c.read_back(bucket, "before", before, None, projection_s3(), "object written under the default, read afterwards")


def enc_204(c):
    bucket = c.bucket()
    c.roundtrip(bucket, "sse-s3", plain("enc-204 s3", SIZE), sse_s3(), None, projection_s3(), "SSE-S3")
    # An object that says aws:kms while only the local master key protects it would be a silent downgrade.
    c.refused(put(c.client, bucket, "sse-kms", plain("enc-204 kms", SIZE), sse_kms()), 400, "InvalidRequest", "SSE-KMS PUT")
    c.absent(bucket, "sse-kms", "SSE-KMS PUT")


def enc_205(c):
    bucket = c.bucket()
    data = plain("enc-205", KIB)
    triple = ssec(KEY_A)
    algorithm, key, md5 = SSEC + "algorithm", SSEC + "key", SSEC + "key-md5"
    rows = {
        "algorithm-only": {algorithm: "AES256"},
        "key-only": {key: triple[key]},
        "md5-missing": {algorithm: "AES256", key: triple[key]},
        "key-not-base64": {**triple, key: "!!! not base64 !!!"},
        "key-16-bytes": ssec(derived_key("short", 16)),
        "md5-of-another-key": {**triple, md5: key_md5(KEY_B)},
        "algorithm-aes128": {**triple, algorithm: "AES128"},
    }
    for name, headers in rows.items():
        with c.row(name):
            # A partial triple that is treated as "no SSE-C" would store the object in plaintext.
            c.refused(put(c.client, bucket, name, data, headers), 400, "InvalidRequest", "PUT")
            c.absent(bucket, name, "PUT")


def enc_206(c):
    bucket = c.bucket()
    c.ok(put(c.client, bucket, "src", plain("enc-206", SIZE), ssec(KEY_A)), "source PUT")
    for name, copy_source in (("no-source-key", {}), ("wrong-source-key", ssec(KEY_B, COPY_SSEC))):
        with c.row(name):
            status, _, body = copy(c.client, bucket, f"dst-{name}", bucket, "src", copy_source)
            code, message = s3_error(body)
            c.check(status == 400 and code != "", f"CopyObject: HTTP {status} {code!r} {message[:160]!r}, expected an explicit 400")
            c.absent(bucket, f"dst-{name}", "CopyObject")
    with c.row("right-source-key"):
        data = plain("enc-206", SIZE)
        c.ok(copy(c.client, bucket, "dst-ok", bucket, "src", {**ssec(KEY_A, COPY_SSEC), **ssec(KEY_B)}), "CopyObject")
        c.read_back(bucket, "dst-ok", data, ssec(KEY_B), projection_c(KEY_B), "destination")
        # The data was re-encrypted, not relabelled: the source key no longer opens the destination.
        c.refused(get(c.client, bucket, "dst-ok", ssec(KEY_A)), 400, "InvalidRequest", "destination GET with the source key")


def enc_207(c):
    bucket = c.bucket()
    parts = [plain("enc-207 part 1", PART), plain("enc-207 part 2", MIB + 17)]
    triple, want = ssec(KEY_A), projection_c(KEY_A)
    create_response, complete_response = c.upload(bucket, "object", parts, triple, triple, triple)
    # An SSE-C upload answers with its customer-key headers only: `x-amz-server-side-encryption` names a managed algorithm.
    c.projection(create_response, want, "CreateMultipartUpload")
    c.projection(complete_response, want, "CompleteMultipartUpload")
    c.read_back(bucket, "object", b"".join(parts), triple, want, "completed object")
    c.refused(get(c.client, bucket, "object"), 400, "InvalidRequest", "GET without the customer key")


def enc_208(c):
    bucket = c.bucket()
    triple = ssec(KEY_A)
    part_1, part_2 = plain("enc-208 part 1", PART), plain("enc-208 part 2", KIB)
    status, _, upload_id, body = create_multipart(c.client, bucket, "ssec", triple)
    c.need(status == 200 and upload_id, f"CreateMultipartUpload (SSE-C): HTTP {status} {s3_error(body)}")
    with c.row("part-without-key"):
        c.refused(upload_part(c.client, bucket, "ssec", upload_id, 1, part_1), 400, "InvalidRequest", "UploadPart")
    with c.row("part-with-another-key"):
        c.refused(upload_part(c.client, bucket, "ssec", upload_id, 1, part_1, ssec(KEY_B)), 400, "InvalidRequest", "UploadPart")
    status, _, plain_upload, body = create_multipart(c.client, bucket, "plain")
    c.need(status == 200 and plain_upload, f"CreateMultipartUpload (plaintext): HTTP {status} {s3_error(body)}")
    with c.row("key-on-plaintext-upload"):
        c.refused(upload_part(c.client, bucket, "plain", plain_upload, 1, part_1, triple), 400, "InvalidRequest", "UploadPart")
    with c.row("upload-still-usable"):
        etags = []
        for number, data in ((1, part_1), (2, part_2)):
            response, _ = c.ok(upload_part(c.client, bucket, "ssec", upload_id, number, data, triple), f"UploadPart {number}")
            etags.append((number, response.get("etag", "")))
        c.ok(complete_multipart(c.client, bucket, "ssec", upload_id, etags, triple), "CompleteMultipartUpload")
        c.read_back(bucket, "ssec", part_1 + part_2, triple, projection_c(KEY_A), "completed object")


def _read_mismatches(c, bucket, objects, rows):
    """GET with read headers that do not match how the object was stored: refused, and no object bytes returned."""
    data = {name: plain(f"{c.case_id} {name}", SIZE) for name in objects}
    for name, headers in objects.items():
        c.ok(put(c.client, bucket, name, data[name], headers), f"{name} PUT")
    for name, (target, headers, code) in rows.items():
        with c.row(name):
            response = get(c.client, bucket, target, headers)
            c.refused(response, 400, code, "GET")
            c.check(data[target][:64] not in response[2], "the refused GET returned object bytes")


def _ssec_read_rows():
    md5_only_matches = {**ssec(KEY_B), SSEC + "key-md5": key_md5(KEY_A)}
    return {
        "ssec-without-key": ("ssec", None, "InvalidRequest"),
        "ssec-with-another-key": ("ssec", ssec(KEY_B), "InvalidRequest"),
        "ssec-right-md5-wrong-key": ("ssec", md5_only_matches, "InvalidRequest"),
        "plain-with-ssec": ("plain", ssec(KEY_A), "InvalidRequest"),
        "plain-with-aes256": ("plain", sse_s3(), "InvalidArgument"),
        # No customer key came with the request, and that is what the refusal reports; the aws:kms header is not reached.
        "ssec-with-aws-kms": ("ssec", sse_kms(), "InvalidRequest"),
    }


def enc_209(c):
    _read_mismatches(c, c.bucket(), {"ssec": ssec(KEY_A), "plain": None}, _ssec_read_rows())


def _listed_etags(c, bucket, schemes):
    """No listing shows the MD5 of an encrypted object's plaintext as its ETag."""
    md5 = {}
    for name, headers in schemes.items():
        data = plain(f"{c.case_id} etag {name}", SIZE)
        md5[name] = hashlib.md5(data).hexdigest()
        c.ok(put(c.client, bucket, name, data, headers), f"{name} PUT")
    listings = {"list-objects-v2": ("list-type=2", "Contents"), "list-objects-v1": ("", "Contents"),
                "list-object-versions": ("versions=", "Version")}
    for name, (query, entry) in listings.items():
        with c.row(name):
            listed = listing(c.client, bucket, query, entry)
            c.need(listed is not None, "the listing was refused")
            etags = {key: etag for key, (_, etag) in listed.items()}
            c.check(sorted(etags) == sorted(schemes), f"the listing names {sorted(etags)}, expected {sorted(schemes)}")
            # The plaintext object is the control: its ETag is its MD5, which is what a leak would look like.
            c.check(etags.get("plain") == md5["plain"],
                    f"plaintext object: listed ETag {etags.get('plain')!r} is not its MD5, so the comparison proves nothing")
            for scheme in schemes:
                if scheme != "plain":
                    c.check(etags.get(scheme) not in (None, "", md5[scheme]),
                            f"{scheme} object: the listed ETag {etags.get(scheme)!r} is missing or is the MD5 of the plaintext")


def enc_210(c):
    _listed_etags(c, c.bucket(), {"plain": None, "sse-s3": sse_s3(), "sse-c": ssec(KEY_A)})


# ---------------------------------------------------------------------------
# Cases for a server with a running KMS and a default key
# ---------------------------------------------------------------------------

def kms_601(c):
    bucket = c.bucket()
    data, part = plain("kms-601", SIZE), plain("kms-601 part", PART)
    c.roundtrip(bucket, "object", data, sse_s3(), None, projection_s3(), "SSE-S3")
    # SSE-S3 is "a key the service manages": no response may say which KMS key wraps the data key.
    responses = {"PutObject": put(c.client, bucket, "named", data, sse_s3()), "GetObject": get(c.client, bucket, "named")}
    status, head_headers = head(c.client, bucket, "named")
    responses["HeadObject"] = (status, head_headers, b"")
    responses["CopyObject"] = copy(c.client, bucket, "copy", bucket, "named", sse_s3())
    status, create_headers, upload_id, body = create_multipart(c.client, bucket, "multipart", sse_s3())
    responses["CreateMultipartUpload"] = (status, create_headers, body)
    c.need(status == 200 and upload_id, f"CreateMultipartUpload: HTTP {status} {s3_error(body)}")
    responses["UploadPart"] = upload_part(c.client, bucket, "multipart", upload_id, 1, part)
    responses["ListParts"] = list_parts(c.client, bucket, "multipart", upload_id)
    etag = responses["UploadPart"][1].get("etag", "")
    responses["CompleteMultipartUpload"] = complete_multipart(c.client, bucket, "multipart", upload_id, [(1, etag)])
    for name, (status, headers, body) in responses.items():
        with c.row(name):
            c.check(status == 200, f"HTTP {status}, expected 200")
            c.check(SSE_KEY_ID not in headers, f"the response carries {SSE_KEY_ID}: {headers.get(SSE_KEY_ID)!r}")
            if name != "GetObject":
                c.check(c.default_key.encode() not in body, f"the response body names the default key {c.default_key}")
            for header, value in headers.items():
                c.check(c.default_key not in value, f"header {header} names the default key {c.default_key}")


def kms_602(c):
    bucket = c.bucket()
    status, body = c.client.admin("GET", "kms/status")
    c.check(status == 200 and c.default_key in json.dumps(body),
            f"KMS status does not report the default key {c.default_key}: HTTP {status} {str(body)[:200]}")
    with c.row("no-key-id"):
        c.roundtrip(bucket, "default", plain("kms-602 default", SIZE), sse_kms(), None, projection_kms(c.default_key), "SSE-KMS")
    # KEY_1 differs from the default key, so a response naming the wrong key cannot pass.
    _sizes(c, bucket, [("sse-kms", sse_kms(KEY_1), None, projection_kms(KEY_1))], "kms-602")


def kms_603(c):
    bucket = c.bucket()
    data = plain("kms-603", SIZE)
    c.ok(put(c.client, bucket, "object", data, sse_kms(KEY_1)), "PUT")
    last = SIZE - 1
    ranges = {"first-byte": ("bytes=0-0", 0, 0), "last-byte": (f"bytes={last}-{last}", last, last),
              "suffix-100": ("bytes=-100", SIZE - 100, last), "across-8k": ("bytes=8000-8500", 8000, 8500)}
    for name, (spec, first, end) in ranges.items():
        with c.row(name):
            status, headers, body = get(c.client, bucket, "object", range_=spec)
            c.check(status == 206, f"Range {spec}: HTTP {status}, expected 206")
            c.check(headers.get("content-range") == f"bytes {first}-{end}/{SIZE}",
                    f"Range {spec}: Content-Range {headers.get('content-range')!r}, expected 'bytes {first}-{end}/{SIZE}'")
            c.check(body == data[first:end + 1], f"Range {spec}: body sha256 {sha256_hex(body)[:16]} len {len(body)} is not the plaintext slice")
            c.projection(headers, projection_kms(KEY_1), f"Range {spec}")


def kms_604(c):
    with c.row("default-without-key-id"):
        bucket = c.bucket()
        c.ok(put_bucket_encryption(c.client, bucket, "aws:kms"), "PutBucketEncryption")
        status, config = get_bucket_encryption(c.client, bucket)
        c.check(status == 200 and config.get("algorithm") == "aws:kms" and config.get("kms_key_id") == c.default_key,
                f"GetBucketEncryption: HTTP {status} {config}, expected aws:kms with KMSMasterKeyID {c.default_key}")
        c.roundtrip(bucket, "object", plain("kms-604 default", SIZE), None, None, projection_kms(c.default_key), "PUT that names no scheme")
        c.roundtrip(bucket, "customer", plain("kms-604 customer", SIZE), ssec(KEY_A), ssec(KEY_A), projection_c(KEY_A), "SSE-C")
        c.refused(get(c.client, bucket, "customer"), 400, "InvalidRequest", "GET without the customer key")
    with c.row("explicit-aes256-wins"):
        bucket = c.bucket()
        c.ok(put_bucket_encryption(c.client, bucket, "aws:kms", KEY_1), "PutBucketEncryption")
        # No key id in the expected headers: the request did not inherit the bucket's KMS key.
        c.roundtrip(bucket, "object", plain("kms-604 explicit", SIZE), sse_s3(), None, projection_s3(), "explicit AES256")


def kms_605(c):
    bucket = c.bucket()
    # name: (source write headers, copy-source headers, destination headers, destination read headers, expected headers)
    combos = {
        "plain-to-kms": (None, {}, sse_kms(KEY_2), None, projection_kms(KEY_2)),
        "kms-to-ssec": (sse_kms(KEY_1), {}, ssec(KEY_B), ssec(KEY_B), projection_c(KEY_B)),
        "ssec-to-s3": (ssec(KEY_A), ssec(KEY_A, COPY_SSEC), sse_s3(), None, projection_s3()),
        "ssec-to-ssec": (ssec(KEY_A), ssec(KEY_A, COPY_SSEC), ssec(KEY_B), ssec(KEY_B), projection_c(KEY_B)),
        "s3-to-none": (sse_s3(), {}, {}, None, {}),
        "kms-to-kms": (sse_kms(KEY_1), {}, sse_kms(KEY_2), None, projection_kms(KEY_2)),
    }
    for name, (source_headers, copy_source, destination, read_headers, want) in combos.items():
        with c.row(name):
            data = plain(f"kms-605 {name}", SIZE)
            c.ok(put(c.client, bucket, f"{name}-src", data, source_headers), "source PUT")
            c.ok(copy(c.client, bucket, f"{name}-dst", bucket, f"{name}-src", {**copy_source, **destination}), "CopyObject")
            c.read_back(bucket, f"{name}-dst", data, read_headers, want, "destination")
            status, head_headers = head(c.client, bucket, f"{name}-dst", read_headers)
            if c.check(status == 200, f"destination HEAD: HTTP {status}, expected 200"):
                c.projection(head_headers, want, "destination HEAD")
            if name == "ssec-to-ssec":
                c.refused(get(c.client, bucket, f"{name}-dst", ssec(KEY_A)), 400, "InvalidRequest", "destination GET with the source key")


def kms_606(c):
    bucket = c.bucket()
    parts = [plain("kms-606 part 1", PART), plain("kms-606 part 2", PART), plain("kms-606 part 3", MIB + 17)]
    data = b"".join(parts)
    want = projection_kms(KEY_1)
    create_response, complete_response = c.upload(bucket, "object", parts, sse_kms(KEY_1), None, None)
    c.projection(create_response, want, "CreateMultipartUpload")
    c.projection(complete_response, want, "CompleteMultipartUpload")
    c.read_back(bucket, "object", data, None, want, "completed object")
    status, head_headers = head(c.client, bucket, "object")
    if c.check(status == 200, f"HEAD: HTTP {status}, expected 200"):
        c.projection(head_headers, want, "HEAD")
        c.check(head_headers.get("content-length") == str(len(data)),
                f"HEAD: Content-Length {head_headers.get('content-length')}, expected {len(data)}")
        etag = head_headers.get("etag", "").strip('"')
        c.check(len(etag) == 34 and etag.endswith("-3"), f"HEAD: ETag {etag!r}, expected 32 hex digits followed by -3")


def _b64(text):
    return base64.b64encode(text.encode()).decode()


def kms_607(c):
    bucket = c.bucket()
    data = plain("kms-607", KIB)
    rows = {
        "algorithm-aes128": ({SSE: "AES128"}, "InvalidArgument"),
        "algorithm-lowercase": ({SSE: "aes256"}, "InvalidArgument"),
        "ssec-with-aes256": ({**ssec(KEY_A), SSE: "AES256"}, "InvalidArgument"),
        "ssec-with-key-id": ({**ssec(KEY_A), SSE_KEY_ID: KEY_1}, "InvalidArgument"),
        "key-id-without-algorithm": ({SSE_KEY_ID: KEY_1}, "InvalidArgument"),
        "aes256-with-key-id": ({SSE: "AES256", SSE_KEY_ID: KEY_1}, "InvalidArgument"),
        "context-without-algorithm": ({SSE_CONTEXT: _b64('{"dept":"a"}')}, "InvalidArgument"),
        "context-not-base64": ({SSE: "aws:kms", SSE_CONTEXT: "!!! not base64 !!!"}, "InvalidArgument"),
        "context-json-array": ({SSE: "aws:kms", SSE_CONTEXT: _b64('["dept","a"]')}, "InvalidArgument"),
        "context-key-equals-bucket": ({SSE: "aws:kms", SSE_CONTEXT: _b64(json.dumps({bucket: "elsewhere"}))}, "InvalidArgument"),
        "key-that-does-not-exist": (sse_kms(MISSING_KEY), "KMS.NotFoundException"),
    }
    for name, (headers, code) in rows.items():
        with c.row(name):
            c.refused(put(c.client, bucket, name, data, headers), 400, code, "PUT")
            c.absent(bucket, name, "PUT")


def kms_608(c):
    rows = {**_ssec_read_rows(), "kms-with-ssec": ("kms", ssec(KEY_A), "InvalidArgument")}
    _read_mismatches(c, c.bucket(), {"ssec": ssec(KEY_A), "kms": sse_kms(KEY_1), "plain": None}, rows)


def kms_609(c):
    bucket = c.bucket()
    # One pair only: the server stores the context as a map and writes it back in no particular order.
    write_headers = sse_kms(KEY_1, {"department": "finance"})
    encoded = write_headers[SSE_CONTEXT]
    data = plain("kms-609", SIZE)
    want = projection_kms(KEY_1)
    put_headers, _ = c.ok(put(c.client, bucket, "context", data, write_headers), "PUT with an encryption context")
    get_headers, body = c.ok(get(c.client, bucket, "context"), "GET")
    c.bytes_equal(get_headers, body, data, "GET")
    status, head_headers = head(c.client, bucket, "context")
    c.need(status == 200, f"HEAD: HTTP {status}, expected 200")
    for what, headers in (("PUT", put_headers), ("GET", get_headers), ("HEAD", head_headers)):
        with c.row(what.lower()):
            c.projection(headers, want, what)
            for name, value in headers.items():
                c.check(encoded not in value and "finance" not in value, f"{what}: header {name} repeats the encryption context")
    _listed_etags(c, c.bucket(), {"plain": None, "sse-s3": sse_s3(), "sse-kms": sse_kms(KEY_1), "sse-c": ssec(KEY_A)})


# id: (name, needs KMS, function)
CASES = {
    "ENC-201": ("SseS3ExactHeaders", False, enc_201),
    "ENC-202": ("SseCExactHeaders", False, enc_202),
    "ENC-203": ("BucketDefaultExact", False, enc_203),
    "ENC-204": ("SseKmsNotDowngraded", False, enc_204),
    "ENC-205": ("SseCTripleNegatives", False, enc_205),
    "ENC-206": ("SseCCopySourceKey", False, enc_206),
    "ENC-207": ("SseCMultipartExact", False, enc_207),
    "ENC-208": ("SseCPartKeyMismatch", False, enc_208),
    "ENC-209": ("SseReadHeaderMismatch", False, enc_209),
    "ENC-210": ("ListingEtagNotPlaintextMd5", False, enc_210),
    "KMS-601": ("SseS3KeyNotNamed", True, kms_601),
    "KMS-602": ("SseKmsKeyResolutionExact", True, kms_602),
    "KMS-603": ("SseKmsRangeExact", True, kms_603),
    "KMS-604": ("BucketDefaultKmsExact", True, kms_604),
    "KMS-605": ("CopySchemeMatrix", True, kms_605),
    "KMS-606": ("MultipartSseKmsExact", True, kms_606),
    "KMS-607": ("SsePutHeaderNegatives", True, kms_607),
    "KMS-608": ("SseReadHeaderMismatch", True, kms_608),
    "KMS-609": ("ContextAndEtagNotEchoed", True, kms_609),
}


def run_case(client, case_id, default_key=""):
    """(failures, expectations evaluated) for one case."""
    _, needs_kms, function = CASES[case_id]
    ctx = Ctx(client, case_id, default_key)
    try:
        if needs_kms:
            for name in (KEY_1, KEY_2):
                ensure_kms_key(client, name)
        function(ctx)
    except CaseFailed as err:
        ctx.failures.append(str(err))
    finally:
        with contextlib.suppress(Exception):
            ctx.cleanup()
    if not ctx.failures and ctx.evaluated == 0:
        ctx.failures.append("the case evaluated no expectation")
    return ctx.failures, ctx.evaluated


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(EXIT_USAGE, f"{self.prog}: error: {message}\n")


def main(argv=None):
    parser = _Parser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--endpoint", help="http(s)://host[:port] of one node")
    parser.add_argument("--case", choices=sorted(CASES), help="the case to run")
    parser.add_argument("--kms-default-key", default="", help="the KMS default key id of the server; required by KMS-6xx")
    parser.add_argument("--list", action="store_true", help="print the case ids and names, then exit")
    args = parser.parse_args(argv)
    if args.list:
        for case_id, (name, _, _) in CASES.items():
            print(f"{case_id} {name}")
        return 0
    if not args.endpoint or not args.case:
        parser.error("--endpoint and --case are required")
    name, needs_kms, _ = CASES[args.case]
    if needs_kms and not args.kms_default_key:
        parser.error(f"{args.case} requires --kms-default-key")
    access_key, secret_key = os.environ.get("RUSTFS_ACCESS_KEY"), os.environ.get("RUSTFS_SECRET_KEY")
    if not access_key or not secret_key:
        parser.error("RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY must be set")
    try:
        client = Client(args.endpoint, access_key, secret_key)
    except ValueError as err:
        parser.error(str(err))
    failures, evaluated = run_case(client, args.case, args.kms_default_key)
    for failure in failures:
        print(f"{args.case} {name}: {failure}")
    print(f"{args.case} {name}: {evaluated} expectations evaluated, {len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
