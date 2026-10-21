#!/usr/bin/env python3
"""Observe one I05 caller admission and drain; never certify the whole case."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

try:
    from .caller_abort import disconnect_get
    from .fleet_node import PROCESS_FIELDS, runtime_config
    from .heal_probe import Probe, canonical, require, save, validate_binding
    from .healthy import Fleet
    from .scanner_repair import JournalWindow
except ImportError:
    from caller_abort import disconnect_get
    from fleet_node import PROCESS_FIELDS, runtime_config
    from heal_probe import Probe, canonical, require, save, validate_binding
    from healthy import Fleet
    from scanner_repair import JournalWindow


TARGET = 'rustfs_ecstore::object_lock_diag'
CANDIDATE = '757b751482bb970bc321e92059690a7698554358'
ADMISSION_SECONDS = 10
DRAIN_SECONDS = 30


def caller_attempt(rows, sent, bucket, key):
    """Bind flattened production logs through the actual HTTP ancestor span.

    External S3 request IDs are server-owned. The client User-Agent is only a
    unique correlation marker; a client request-id header cannot stand in for
    server admission. GET's lock diagnostic has no requested_version_id; the
    exact version must instead match the HTTP span URI.
    """
    require(re.fullmatch(r'auto-testing-i05/[0-9a-f-]{36}', sent.get('callerMarker', '')),
            'missing caller correlation marker')
    selected = []
    for row in rows:
        if row.get('target') != TARGET:
            continue
        spans = [row.get('span'), *row.get('spans', [])]
        spans = [span for span in spans if isinstance(span, dict) and span.get('name') == 'http-request']
        marked = [span for span in spans if span.get('user_agent') == sent['callerMarker']]
        if not marked:
            continue
        require(len({canonical(span) for span in marked}) == 1, 'caller has ambiguous HTTP ancestor spans')
        span = marked[0]
        request_id = span.get('request_id')
        require(isinstance(request_id, str) and str(uuid.UUID(request_id)) == request_id
                and uuid.UUID(request_id).int != 0 and row.get('request_id') == request_id
                and span.get('method') == 'GET' and span.get('uri') == sent['requestTarget'],
                'caller server request ID, method or exact version URI differs')
        require(row.get('event') == 'object_namespace_lock' and row.get('component') == 'ecstore'
                and row.get('subsystem') == 'object_lock' and row.get('op') == 'get_object'
                and row.get('mode') == 'read' and row.get('bucket') == bucket and row.get('object') == key
                and row.get('requested_version_id') is None and row.get('timeout_ms') == 1000,
                'caller lock diagnostic escaped its exact GET identity or runtime budget')
        require(all(isinstance(row.get(name), str) and 0 < len(row[name]) <= 2048
                    for name in ('owner', 'namespace', 'lock_attempt_id'))
                and str(uuid.UUID(row['lock_attempt_id'])) == row['lock_attempt_id']
                and uuid.UUID(row['lock_attempt_id']).int != 0
                and type(row.get('acquire_ms')) is int and row['acquire_ms'] >= 0,
                'caller lock owner, attempt or timing is incomplete')
        if row.get('state') == 'failed':
            require(row.get('level') == 'ERROR' and isinstance(row.get('failure'), str),
                    'caller lock failure lost its error classification')
        else:
            require(row.get('level') == 'TRACE' and type(row.get('hold_ms')) is int and row['hold_ms'] >= 0,
                    'caller lock lifecycle lost its diagnostic level or hold timing')
        selected.append(row)
    if not selected:
        return None
    identity = ('request_id', 'lock_attempt_id', 'owner', 'namespace')
    require(len({tuple(row[name] for name in identity) for row in selected}) == 1,
            'caller marker maps to multiple requests or lock owners')
    states = [row.get('state') for row in selected]
    allowed = (['acquiring'], ['acquiring', 'acquired'], ['acquiring', 'acquired', 'guard_dropped'],
               ['acquiring', 'failed'], ['acquiring', 'cancelled'])
    require(states in allowed, 'caller lock lifecycle is missing, duplicated or out of order')
    return {**{name: selected[0][name] for name in identity}, 'states': states, 'events': selected,
            'admitted': True, 'observedAcquiredWithoutDrop': states == ['acquiring', 'acquired'],
            'diagnosticGuardDropped': states == ['acquiring', 'acquired', 'guard_dropped'],
            'distributedReleaseProven': False, 'heldAtDisconnectProven': False}


def same_process(observed, expected):
    require(isinstance(observed, dict) and all(name in expected and observed.get(name) == expected[name]
            for name in PROCESS_FIELDS), 'I05 observation crossed a service process birth or runtime change')


def observe_disconnect(probe, fleet, bucket, entry, output):
    """Use an already deployed exclusive I05 service; do not alter the fleet."""
    validate_binding(probe.binding)
    require(fleet.runtime_profile == 'lock-timeout' and fleet.run_id == probe.binding['runId']
            and probe.binding['candidateCommit'] == CANDIDATE,
            'caller observation requires its reviewed candidate and isolated runtime')
    endpoints = {f'node{index}': f'http://rustfs-node{index}:9000' for index in range(1, len(fleet.nodes) + 1)}
    require(probe.binding['endpoints'] == endpoints and len(set(fleet.nodes)) == len(fleet.nodes)
            and ((len(fleet.nodes) == 1 and fleet.run_id.endswith('-h1'))
                 or (len(fleet.nodes) == 4 and fleet.run_id.endswith('-h2'))),
            'I05 caller endpoints differ from the H1/H2 fleet')
    require(entry.get('deleteMarker') is False and isinstance(entry.get('payload'), dict)
            and set(entry['payload']) == {'size', 'sha256'} and type(entry['payload']['size']) is int
            and 0 < entry['payload']['size'] <= probe.binding['maxInventoryBytes']
            and re.fullmatch(r'[0-9a-f]{64}', entry['payload']['sha256']),
            'caller drain requires an acknowledged historical content manifest')
    output = Path(output)
    output.mkdir(exist_ok=False)
    bound = canonical(probe.binding)
    save(output / 'scope.json', {'candidateCommit': CANDIDATE, 'bindingSha256': hashlib.sha256(bound.encode()).hexdigest(),
        'runId': fleet.run_id, 'entry': entry, 'admissionSeconds': ADMISSION_SECONDS, 'drainSeconds': DRAIN_SECONDS,
        'canonicalCasePassClaimed': False, 'mutatesServiceConfiguration': False})
    expected, baseline = {}, {}
    def metrics(label):
        values = {}
        for node in fleet.nodes:
            value = fleet.record(label, {'node': node, **fleet.remote(node, 'process-metrics')})
            identity = value['identity']
            require(identity.get('binarySha256') == probe.binding['candidateBinarySha256']
                    and all(identity.get('settings', {}).get(k) == v for k, v in runtime_config('lock-timeout').items()),
                    'I05 resource sample has another candidate or process configuration')
            if node in expected:
                same_process(identity, expected[node])
            else:
                expected[node] = identity
            require(type(value.get('fileDescriptors')) is int and value['fileDescriptors'] >= 0,
                    'I05 descriptor count is incomplete')
            values[node] = value
        return values
    baseline = metrics('I05-caller-baseline-resources')
    window = JournalWindow(fleet, 'I05-caller', event_targets={TARGET}, strict_json=True)
    node = fleet.nodes[list(endpoints).index(probe.endpoint_name)]
    admitted = None
    network_namespace = None
    def capture(sent):
        validate_binding(probe.binding)
        require(canonical(probe.binding) == bound, 'I05 caller binding drifted during observation')
        events = window.capture()
        attempts = {host: caller_attempt(rows, sent, bucket, entry['key']) for host, rows in events.items()}
        require(all(value is None for host, value in attempts.items() if host != node),
                'caller marker appeared on a foreign endpoint')
        return attempts[node]
    def sockets(sent, label):
        nonlocal network_namespace
        value = fleet.record(label, {'node': node, **fleet.remote(node, 'caller-socket',
            expectedProcess=expected[node], clientAddress=sent['localAddress'], serverAddress=sent['peerAddress'])})
        same_process(value.get('process'), expected[node])
        require(value.get('readOnly') is True and value.get('clockDomain') == expected[node]['bootId']
                and len(value.get('samples', [])) == 2, 'caller socket census lacks its original process or samples')
        namespace = value.get('networkNamespace')
        require(isinstance(namespace, str) and re.fullmatch(r'net:\[[0-9]+\]', namespace),
                'caller socket census lost its network namespace')
        require(network_namespace is None or namespace == network_namespace,
                'caller socket moved to another network namespace between admission and drain')
        network_namespace = namespace
        return value
    def before_disconnect(sent):
        nonlocal admitted
        deadline = time.monotonic() + ADMISSION_SECONDS
        while True:
            observed = capture(sent)
            if observed is not None:
                require(observed['states'] in (['acquiring'], ['acquiring', 'acquired']),
                        'NOT_OBSERVED: caller lock already ended before controlled disconnect')
                if observed['observedAcquiredWithoutDrop']:
                    connection = sockets(sent, 'I05-caller-before-disconnect-socket')
                    require(all(sample['vanishedDescriptors'] == 0 and len(sample['ownedMatches']) == 1
                                and sample['ownedMatches'][0]['state'] == '01'
                                for sample in connection['samples']),
                            'NOT_OBSERVED: admitted caller lacks its exact established service descriptor')
                    # Re-read the journal after the socket census. A later
                    # native fence must still prove the exact disconnect edge.
                    observed = capture(sent)
                    require(observed is not None and observed['observedAcquiredWithoutDrop'],
                            'NOT_OBSERVED: caller lock ended while sampling its service descriptor')
                    metrics('I05-caller-admission-resources')
                    require(time.monotonic() <= deadline, 'NOT_OBSERVED: caller admission observation exceeded its budget')
                    admitted = {'node': node, 'attempt': observed, 'socket': connection}
                    save(output / 'admission.json', admitted)
                    return
            require(time.monotonic() < deadline, 'NOT_OBSERVED: caller lock acquisition window was not observed')
            time.sleep(.1)
    try:
        sent = disconnect_get(probe, bucket, entry['key'], entry['versionId'], output / 'transport',
                              before_disconnect=before_disconnect)
        deadline = time.monotonic() + DRAIN_SECONDS
        while True:
            observed = capture(sent)
            require(observed is not None and all(observed[name] == admitted['attempt'][name]
                    for name in ('request_id', 'lock_attempt_id', 'owner', 'namespace')),
                    'caller drain lost its original service request or lock owner')
            connection = sockets(sent, 'I05-caller-drain-socket')
            resources = metrics('I05-caller-drain-resources')
            require(time.monotonic() <= deadline, 'NOT_OBSERVED: caller drain observation exceeded its budget')
            if (observed['diagnosticGuardDropped'] and all(sample['vanishedDescriptors'] == 0
                    and not sample['ownedMatches'] for sample in connection['samples'])
                    and all(resources[host]['fileDescriptors'] <= baseline[host]['fileDescriptors'] for host in fleet.nodes)):
                break
            require(time.monotonic() < deadline, 'NOT_OBSERVED: caller diagnostic, socket or FD count did not drain')
            time.sleep(.2)
        read = fleet.record('I05-caller-final-exact-version', probe.s3('get_object',
            {'Bucket': bucket, 'Key': entry['key'], 'VersionId': entry['versionId']}))
        require(read.get('acknowledged') is True and read.get('resultUnknown') is False
                and read.get('response', {}).get('VersionId') == entry['versionId'] and read.get('payload') == entry['payload'],
                'caller drain changed the acknowledged historical version or its complete content')
        deadline = time.monotonic() + DRAIN_SECONDS
        while True:
            capture(sent)
            verified_resources = metrics('I05-caller-after-verification-resources')
            require(time.monotonic() <= deadline, 'NOT_OBSERVED: I05 verification resource observation exceeded its budget')
            if all(verified_resources[host]['fileDescriptors'] <= baseline[host]['fileDescriptors'] for host in fleet.nodes):
                break
            require(time.monotonic() < deadline, 'NOT_OBSERVED: I05 verification did not return to its FD count baseline')
            time.sleep(.2)
        result = {'observationComplete': True, 'admission': admitted, 'finalAttempt': observed,
            'finalSocket': connection, 'finalResources': resources, 'afterVerificationResources': verified_resources,
            'exactHistoricalContentPreserved': True,
            'callerResultUnknown': True, 'heldAtDisconnectProven': False, 'distributedReleaseProven': False,
            'noGlobalFdLeakProven': False, 'canonicalCasePassClaimed': False,
            'remaining': ['exact native disconnect/timeout/stale-generation fences',
                          'complete H1/H2 DL/DV ACK and physical closure', 'queued-successor lock and FD closure']}
        save(output / 'observation.json', result)
        return result
    except BaseException as error:
        save(output / 'failure.json', {'observationComplete': False, 'errorType': type(error).__name__,
            'state': 'NOT_OBSERVED' if str(error).startswith('NOT_OBSERVED:') else 'INCOMPLETE',
            'canonicalCasePassClaimed': False, 'originalTransportEvidenceRetained': True})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('--nodes', nargs='+', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--entry', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--endpoint')
    args = parser.parse_args()
    probe = Probe(json.loads(args.binding.read_text()), args.endpoint)
    args.output.mkdir(parents=True, exist_ok=False)
    # The component uses an existing owned deployment and never calls cleanup.
    # Keep its SSH/log evidence separate from the exclusive request directory.
    fleet = Fleet(args.nodes, args.output, probe.binding['runId'], runtime_profile='lock-timeout')
    fleet.secret_values.update(probe.credentials())
    observe_disconnect(probe, fleet, args.bucket, json.loads(args.entry.read_text()), args.output / 'caller')


if __name__ == '__main__':
    main()
