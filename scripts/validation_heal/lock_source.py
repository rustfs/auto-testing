#!/usr/bin/env python3
"""Compose I05 source procedures without converting diagnostics to canonical PASS."""

import copy
import hashlib
import json
from pathlib import Path

try:
    from .heal_probe import CLOCK_DOMAIN, canonical, require, save
    from .race_native_evidence import verify as verify_native
except ImportError:
    from heal_probe import CLOCK_DOMAIN, canonical, require, save
    from race_native_evidence import verify as verify_native


CASE_ID = 'heal.lock-timeout-and-caller-abort'
GROUPS = {
    CASE_ID: ('versioned-read-repair-lock-owner', 'cancelled-physical-unlink-owner',
              'stale-read-repair-final-rename'),
    'heal.race-versioned-put': ('versioned-read-repair-lock-owner', 'explicit-version-append-final-rename'),
    'heal.race-version-delete': ('explicit-version-delete-final-rename',),
}
IDENTITY = ('candidateCommit', 'candidatePackageSha256', 'candidateSourceTree',
            'candidateCargoLockSha256', 'binarySha256', 'runId', 'runAttempt',
            'driverCommit', 'testingCommit', 'selectedContractSha256')
LIVE_BRANCHES = ('timeout', 'queued-successor')


def verify_source_native(args, candidate):
    """Reuse three reviewed subsets from one immutable successful artifact.

    The old stale test asserts ETag only. The append/delete subsets additionally
    bind real reconstruction barriers, foreground ACK/tail drain, exact typed
    outcomes, full surviving bodies and physical generations in 4/2 and 16/4.
    Their native geometry does not claim a fleet request hit the same barrier.
    """
    directory = getattr(args, 'native_directory', None)
    require(directory is not None, 'I05 source-plan requires exact-source native evidence')
    groups = {}
    for case_id, labels in GROUPS.items():
        proof = verify_native(directory, candidate, args.testing_commit, case_id)
        require(proof.get('caseId') == case_id and proof.get('requiredRegressionLabels') == list(labels)
                and set(proof.get('observedRegressions', {})) == set(labels)
                and proof.get('requiredNativeSubsetPassed') is True
                and all(proof.get(key) is False for key in (
                    'wholeCasePassClaimed', 'fleetExecutionClaimed', 'canonicalCasePassClaimed')),
                'I05 source native proof lacks its exact reviewed subset or changes scope')
        require(proof.get('candidateCommit') == candidate['source_sha']
                and proof.get('candidatePackageSha256') == candidate['package_sha256']
                and proof.get('testingCommit') == args.testing_commit,
                'I05 source native proof changed candidate or helper')
        groups[case_id] = proof
    first = groups[CASE_ID]
    require(all(key in first and first[key] is not None for key in IDENTITY)
            and all(type(proof.get(key)) is type(first[key]) and proof[key] == first[key]
                    for proof in groups.values() for key in IDENTITY),
            'I05 source native subsets mix candidates, binaries or workflow attempts')
    return {**{key: first[key] for key in IDENTITY}, 'groups': groups,
            'nativeOwnerAndStaleFencesPassed': True,
            'staleContentAndPhysicalAuthorityScope': 'independent native append/delete in 4/2 and 16/4',
            'fleetExecutionClaimed': False, 'wholeCasePassClaimed': False,
            'canonicalCasePassClaimed': False}


def link_fleet_evidence(directory, branch, binding, diagnostic):
    """Bind durable requests to coordinator timing and actual server identities.

    Controllers already verify original server logs, full peer inventories,
    physical state and bounded resource drain. This joins their retained bytes;
    it never compares a remote monotonic clock to the coordinator clock.
    """
    require(branch in LIVE_BRANCHES, 'I05 source evidence has an unknown live branch')
    directory = Path(directory)
    hashes = {}

    def read(name):
        path = directory / name
        require(path.is_file() and not path.is_symlink() and path.stat().st_size < 16 * 1024**2,
                'I05 source chain lacks an original evidence file: ' + name)
        raw = path.read_bytes()
        hashes[name] = hashlib.sha256(raw).hexdigest()
        return json.loads(raw)

    prefix = branch + '/'
    scope = read(prefix + 'scope.json')
    caller_prefix = prefix + 'caller/'
    intent = read(caller_prefix + 'transport/request.json')
    written = read(caller_prefix + 'transport/request-written.json')
    closed = read(caller_prefix + 'transport/disconnected.json')
    admission = read(caller_prefix + 'admission.json')
    caller = read(caller_prefix + 'observation.json')
    stored = read(prefix + 'diagnostic.json')
    require(stored == diagnostic and diagnostic.get('diagnosticComplete') is True
            and caller == diagnostic.get('callerObservation') and caller.get('admission') == admission
            and caller.get('observationComplete') is True and caller.get('exactHistoricalContentPreserved') is True
            and caller.get('callerResultUnknown') is True
            and all(caller.get(key) is False for key in (
                'heldAtDisconnectProven', 'distributedReleaseProven', 'canonicalCasePassClaimed')),
            'I05 source chain changed caller observation or invents ownership')
    digest = hashlib.sha256(canonical(binding).encode()).hexdigest()
    require(scope.get('runId') == intent.get('runId') == written.get('runId') == closed.get('runId') == binding['runId']
            and scope.get('bindingSha256') == intent.get('bindingSha256') == digest
            and all(intent.get(key) == binding[key] for key in (
                'candidateCommit', 'harnessCommit', 'candidatePackageSha256', 'candidateBinarySha256'))
            and intent.get('params') == {'Bucket': binding['bucketPrefix'], 'Key': scope['entry']['key'],
                                         'VersionId': scope['entry']['versionId']}
            and intent.get('retryAllowed') is False and intent.get('operation') == 'get_object'
            and intent.get('endpoint') == binding['defaultEndpoint']
            and intent.get('endpointUrl') == binding['endpoints'][binding['defaultEndpoint']].rstrip('/'),
            'I05 source chain changed binding, candidate, run or exact historical request')
    request_sha = hashes[caller_prefix + 'transport/request.json']
    require(all(row.get('requestSha256') == request_sha and row.get('callerMarker') == intent.get('callerMarker')
                and row.get('requestTarget') == intent.get('requestTarget')
                and row.get('requestInvocationStarted') is True and row.get('requestWriteCompleted') is True
                and all(row.get(key) is False for key in ('responseObserved', 'acknowledged', 'retryAllowed', 'serverOwnershipProven'))
                and row.get('resultUnknown') is True for row in (written, closed))
            and closed.get('connectionAttempts') == 1 and closed.get('clientSocketClosed') is True
            and closed.get('shutdownCompleted') is True
            and all(closed.get(key) == written.get(key) for key in (
                'callStartedMonotonicNs', 'requestWrittenMonotonicNs', 'localAddress', 'peerAddress')),
            'I05 source chain lost its single unretried TCP request or unknown outcome')
    holder, final = admission['attempt'], caller['finalAttempt']
    require(all(holder.get(key) == final.get(key) and holder.get(key) is not None for key in (
                'request_id', 'lock_attempt_id', 'owner', 'namespace'))
            and holder.get('observedAcquiredWithoutDrop') is True and final.get('diagnosticGuardDropped') is True,
            'I05 source chain lost original server request, owner or local guard drain')
    edge_name = 'timeout.json' if branch == 'timeout' else 'queued.json'
    edge = read(prefix + edge_name)
    ack_prefix = prefix + ('contender/' if branch == 'timeout' else 'successor/')
    request = read(ack_prefix + '000001-request.json')
    ack = read(ack_prefix + '000001-ack.json')
    require(ack.get('requestSha256') == hashes[ack_prefix + '000001-request.json']
            and request.get('runId') == ack.get('runId') == binding['runId']
            and request.get('params', {}).get('Bucket') == intent['params']['Bucket']
            and request.get('params', {}).get('Key') == intent['params']['Key']
            and request.get('operation') == 'put_object' and ack.get('operationCheck') == 'PASS'
            and ack.get('observation', {}).get('response', {}).get('ResponseMetadata', {}).get('RequestId') == edge.get('requestId'),
            'I05 source chain lost the exact foreground request or original ACK')
    times = [intent.get('preparedMonotonicNs'), written.get('callStartedMonotonicNs'),
             written.get('requestWrittenMonotonicNs'), edge.get('observedMonotonicNs'),
             closed.get('disconnectStartedMonotonicNs'), closed.get('clientClosedMonotonicNs'),
             caller.get('observationCompletedMonotonicNs')]
    timed = [intent, written, closed, edge, caller, request, ack, ack.get('observation', {})]
    require(all(row.get('clockDomain') == CLOCK_DOMAIN for row in timed)
            and all(type(value) is int and value > 0 for value in times) and times == sorted(times),
            'I05 source chain lost coordinator clock order around the actual TCP disconnect')
    if branch == 'timeout':
        require(edge.get('typedTimeoutBeyondBudgetObserved') is True and edge.get('writeLockAcquired') is False
                and edge.get('readResponseBackpressureObservedAfterTimeout') is True
                and diagnostic.get('timeoutContenderAcknowledged') is False,
                'I05 source chain has no typed contention beyond its registered timeout')
    else:
        completion = read(prefix + 'successor-complete.json')
        require(edge.get('states') == ['acquiring']
                and all(completion.get(key) == edge.get(key) for key in ('requestId', 'lockAttemptId'))
                and completion.get('states') == ['acquiring', 'acquired', 'guard_dropped']
                and completion.get('queuedBeforeDisconnectObserved') is True
                and completion.get('originalDiagnosticDropBeforeAcquire') is True
                and completion.get('heldAtDisconnectProven') is False
                and completion.get('clockDomain') == CLOCK_DOMAIN
                and type(completion.get('observedMonotonicNs')) is int
                and times[-2] <= completion['observedMonotonicNs'] <= times[-1],
                'I05 source chain lacks the queued successor and ordered original guard release')
    return {'sourceEvidenceComplete': True, 'branch': branch, 'runId': binding['runId'],
            'bindingSha256': digest, 'originalEvidenceSha256': hashes,
            'callerRequestId': holder['request_id'], 'callerLockAttemptId': holder['lock_attempt_id'],
            'foregroundRequestId': edge['requestId'], 'coordinatorClockDomain': CLOCK_DOMAIN,
            'coordinatorTimes': times, 'heldAtDisconnectProven': False, 'distributedReleaseProven': False,
            'nativeOwnerProofRequiredSeparately': True, 'canonicalCasePassClaimed': False}


def run_source_plan(args, candidate, inspector, run_topology, native_proof):
    """Run real timeout/disconnect controllers serially, retaining the first dirty scene."""
    save(args.evidence / 'source-native-prerequisite.json', native_proof)
    results = []
    remaining = [f'{branch}/{topology}' for branch in LIVE_BRANCHES for topology in args.topologies]
    for branch in LIVE_BRANCHES:
        child = copy.copy(args)
        child.diagnostic_branch, child.source_plan = branch, True
        child.evidence = args.evidence / 'source-plan' / branch
        for topology in args.topologies:
            result = run_topology(child, candidate, inspector, topology)
            results.append(result)
            if not (result.get('diagnosticComplete') is True and result.get('cleanup') == 'PASS'
                    and result.get('sourceEvidenceChain', {}).get('sourceEvidenceComplete') is True):
                break
            remaining.remove(f'{branch}/{topology}')
        if remaining and remaining[0].startswith(branch + '/'):
            break
    passed = not remaining
    summary = {'caseId': CASE_ID, 'diagnosticBranch': 'source-plan', 'diagnostics': results,
               'requestedTopologies': args.topologies, 'diagnosticComplete': passed,
               'sourceProceduresComplete': passed and args.topologies == ['H1', 'H2'],
               'sourceBranches': {
                   'object-lock-timeout': {'scope': 'fleet', 'controller': 'timeout'},
                   'caller-disconnect': {'scope': 'fleet', 'controller': 'queued-successor'},
                   'stale-read-repair': {'scope': 'independent-native',
                       'groups': ['heal.race-versioned-put', 'heal.race-version-delete']},
                   'native-lock-owner-fence': {'scope': 'independent-native', 'group': CASE_ID}},
               'nativeProof': native_proof, 'remainingLiveInstances': remaining,
               'cleanup': 'PASS' if results and all(row['cleanup'] == 'PASS' for row in results) else 'FAIL',
               'complete': False, 'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False,
               'heldAtDisconnectProven': False, 'distributedReleaseProven': False,
               'remaining': ['independent complete live audit of the source plan']}
    save(args.evidence / 'source-plan-summary.json', summary)
    save(args.evidence / 'healthy-summary.json', summary)
    return summary


def main():
    import argparse
    from types import SimpleNamespace
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--testing-commit', required=True)
    args = parser.parse_args()
    proof = verify_source_native(SimpleNamespace(native_directory=args.directory,
        testing_commit=args.testing_commit), json.loads(args.candidate.read_text()))
    print(json.dumps(proof, indent=2))


if __name__ == '__main__':
    main()
