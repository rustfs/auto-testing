#!/usr/bin/env python3
"""Disconnect one signed, exact-version GET; never infer server ownership."""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import time
from urllib.parse import quote, urlencode, urlsplit
import uuid

try:
    from .heal_probe import CLOCK_DOMAIN, Probe, canonical, require, save, validate_binding, validate_s3_request
except ImportError:
    from heal_probe import CLOCK_DOMAIN, Probe, canonical, require, save, validate_binding, validate_s3_request


def disconnect_get(probe, bucket, key, version_id, output):
    """Record intent, send once, close the caller socket, retain UNKNOWN.

    The caller must independently establish server admission, lock ownership,
    drain and final object/FD state. Sending bytes alone proves none of these.
    A populated output directory prevents accidental replay of this request.
    """
    validate_binding(probe.binding)
    require(isinstance(key, str) and 0 < len(key.encode()) <= 1024, 'bounded exact object key required')
    require(isinstance(version_id, str) and str(uuid.UUID(version_id)) == version_id
            and uuid.UUID(version_id).int != 0, 'exact nonnil historical version required')
    params = {'Bucket': bucket, 'Key': key, 'VersionId': version_id}
    validate_s3_request(probe.binding, 'get_object', params)
    bound = canonical(probe.binding)
    require(probe.endpoint_name in probe.binding['endpoints']
            and probe.endpoint == probe.binding['endpoints'][probe.endpoint_name].rstrip('/'),
            'caller endpoint differs from the current binding')
    endpoint = urlsplit(probe.endpoint)
    path = '/' + quote(bucket, safe='') + '/' + quote(key, safe='/~')
    target = path + '?' + urlencode({'versionId': version_id})
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials
    access, secret = probe.credentials()
    token = None if probe.credential_pair else os.environ.get('HEAL_SESSION_TOKEN')
    outgoing = AWSRequest(method='GET', url=probe.endpoint + target, headers={
        'Host': endpoint.netloc, 'x-amz-content-sha256': hashlib.sha256(b'').hexdigest(),
        'Connection': 'close', 'Content-Length': '0'})
    S3SigV4Auth(Credentials(access, secret, token), 's3', 'us-east-1').add_auth(outgoing)
    output = Path(output)
    output.mkdir(exist_ok=False)
    parent = os.open(output.parent, os.O_RDONLY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
    intent_path = output / 'request.json'
    save(intent_path, {'schemaVersion': 1, 'runId': probe.binding['runId'],
        'candidateCommit': probe.binding['candidateCommit'], 'harnessCommit': probe.binding['harnessCommit'],
        'candidatePackageSha256': probe.binding['candidatePackageSha256'],
        'candidateBinarySha256': probe.binding['candidateBinarySha256'],
        'bindingSha256': hashlib.sha256(bound.encode()).hexdigest(),
        'endpoint': probe.endpoint_name, 'endpointUrl': probe.endpoint,
        'operation': 'get_object', 'params': params,
        'clockDomain': CLOCK_DOMAIN, 'preparedMonotonicNs': time.monotonic_ns(),
        'retryAllowed': False, 'canonicalCasePassClaimed': False})
    evidence = {'schemaVersion': 1, 'runId': probe.binding['runId'],
        'requestSha256': hashlib.sha256(intent_path.read_bytes()).hexdigest(),
        'clockDomain': CLOCK_DOMAIN, 'requestInvocationStarted': False, 'requestWriteCompleted': False,
        'responseObserved': False, 'resultUnknown': True, 'acknowledged': False,
        'retryAllowed': False, 'serverOwnershipProven': False, 'canonicalCasePassClaimed': False}
    connection_type = http.client.HTTPSConnection if endpoint.scheme == 'https' else http.client.HTTPConnection
    class OnceConnection(connection_type):
        attempts = 0
        def connect(self):
            self.attempts += 1
            require(self.attempts == 1, 'caller-disconnect request cannot reconnect')
            super().connect()
    connection = OnceConnection(endpoint.hostname, endpoint.port, timeout=5)
    sock = None
    try:
        validate_binding(probe.binding)
        require(canonical(probe.binding) == bound, 'caller binding changed before connection')
        connection.connect()
        sock = connection.sock
        require(sock is not None, 'caller socket unavailable')
        evidence.update(socketDescriptor=sock.fileno(), localAddress=list(sock.getsockname()),
                        peerAddress=list(sock.getpeername()))
        validate_binding(probe.binding)
        require(canonical(probe.binding) == bound, 'caller binding changed before request')
        evidence.update(requestInvocationStarted=True, callStartedMonotonicNs=time.monotonic_ns())
        connection.request('GET', target, headers=dict(outgoing.headers.items()))
        evidence.update(requestWriteCompleted=True, requestWrittenMonotonicNs=time.monotonic_ns())
        save(output / 'request-written.json', evidence)
        evidence['disconnectStartedMonotonicNs'] = time.monotonic_ns()
        sock.shutdown(socket.SHUT_RDWR)
        evidence['shutdownCompleted'] = True
    except BaseException as error:
        evidence['errorType'] = type(error).__name__
        raise
    finally:
        try:
            connection.close()
        finally:
            evidence.update(connectionAttempts=connection.attempts, clientClosedMonotonicNs=time.monotonic_ns(),
                            clientSocketClosed=sock is None or sock.fileno() == -1)
            save(output / 'disconnected.json', evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--version-id', required=True)
    parser.add_argument('--endpoint')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    proof = disconnect_get(Probe(json.loads(args.binding.read_text()), args.endpoint),
                           args.bucket, args.key, args.version_id, args.output)
    print(json.dumps(proof))


if __name__ == '__main__':
    main()
