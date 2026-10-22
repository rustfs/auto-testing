#!/usr/bin/env python3
"""Deploy and retain the I05 timeout diagnostic; never certify the whole case."""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time

try:
    from .heal_probe import Probe, compare_inventories, require, save, validate_binding
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .lock_observer import CANDIDATE, DRAIN_SECONDS, same_process
    from .lock_timeout import MIN_BACKPRESSURE_BYTES, observe_timeout
    from .lock_successor import observe_successor
    from .race_native_evidence import verify as verify_native
    from .shards import map_target, verify_cohort
    from .timeout_retention import seal_successor_scene
except ImportError:
    from heal_probe import Probe, compare_inventories, require, save, validate_binding
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from lock_observer import CANDIDATE, DRAIN_SECONDS, same_process
    from lock_timeout import MIN_BACKPRESSURE_BYTES, observe_timeout
    from lock_successor import observe_successor
    from race_native_evidence import verify as verify_native
    from shards import map_target, verify_cohort
    from timeout_retention import seal_successor_scene


NODES = ('vm000', 'vm001', 'vm002', 'vm003')
CASE_ID = 'heal.lock-timeout-and-caller-abort'
NATIVE_I05_LABELS = ('versioned-read-repair-lock-owner', 'cancelled-physical-unlink-owner',
                     'stale-read-repair-final-rename')


def interrupted(signum, frame):
    raise KeyboardInterrupt(f'I05 diagnostic interrupted by signal {signum}')


def preflight(args):
    """Validate immutable local inputs before reserving or starting a node."""
    candidate = json.loads(args.candidate.read_text())
    require(candidate.get('source_sha') == CANDIDATE,
            'I05 timeout diagnostic requires the exact reviewed candidate')
    require(re.fullmatch(r'[0-9a-f]{64}', candidate.get('package_sha256', '')),
            'I05 package digest is missing')
    if args.binary_sha is not None:
        require(re.fullmatch(r'[0-9a-f]{64}', args.binary_sha), 'I05 binary digest is missing')
    require(re.fullmatch(r'[0-9a-f]{40}', args.testing_commit), 'I05 requires an immutable helper commit')
    root = Path(__file__).resolve().parents[2]
    head = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'],
                          capture_output=True, text=True, check=True).stdout.strip()
    require(head == args.testing_commit, 'I05 helper differs from the selected checkout')
    changes = subprocess.run(['git', '-C', str(root), 'status', '--porcelain', '--untracked-files=normal',
                              '--', 'scripts/validation_heal'], capture_output=True, text=True, check=True)
    require(not changes.stdout.strip(), 'I05 diagnostic source differs from its immutable helper commit')
    for name in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT'):
        value = os.environ.get(name, '1' if name == 'GITHUB_RUN_ATTEMPT' else '')
        require(re.fullmatch(r'[1-9][0-9]*', value), 'I05 requires the owning workflow identity')
    nodes = (os.environ.get('RUSTFS_NODES') or ' '.join(NODES)).split()
    require(nodes[:4] == list(NODES), 'I05 requires the current vm000-vm003 Heal node binding')
    inspector = verify_inspector(args.tool_directory, candidate)
    require(args.rc_bin.is_file(), 'I05 requires the pinned client binary')
    if getattr(args, 'diagnostic_branch', 'timeout') == 'queued-successor':
        verify_native_prerequisite(args, candidate)
    return candidate, inspector


def verify_native_prerequisite(args, candidate):
    directory = getattr(args, 'native_directory', None)
    require(directory is not None, 'I05 queued-successor requires immutable exact-source native evidence')
    proof = verify_native(directory, candidate, args.testing_commit, CASE_ID)
    require(proof.get('requiredRegressionLabels') == list(NATIVE_I05_LABELS)
            and proof.get('requiredNativeSubsetPassed') is True
            and proof.get('wholeCasePassClaimed') is False
            and proof.get('fleetExecutionClaimed') is False
            and proof.get('canonicalCasePassClaimed') is False,
            'I05 queued-successor exact-source native prerequisite is incomplete')
    return proof


def run_topology(args, candidate, inspector, topology):
    require(topology in ('H1', 'H2'), 'I05 timeout supports only H1/H2')
    # Repeat the source/tool guard for callers which bypass main().
    require(candidate.get('source_sha') == inspector.get('candidateCommit') == CANDIDATE,
            'I05 timeout candidate and inspector differ')
    branch = getattr(args, 'diagnostic_branch', 'timeout')
    require(branch in {'timeout', 'queued-successor'}, 'I05 diagnostic branch is not implemented')
    if branch == 'queued-successor':
        verify_native_prerequisite(args, candidate)
    directory = args.evidence / topology
    directory.mkdir(parents=True, exist_ok=False)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    nodes = list(NODES[:1] if topology == 'H1' else NODES)
    bucket = 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    fleet = Fleet(nodes, directory, run_id, args.tool_directory,
                  runtime_profile='lock-successor' if branch == 'queued-successor' else 'lock-timeout')
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    fleet.secret_values.update((access, secret))
    def error_info(error):
        message = str(error)
        for value in (access, secret):
            message = message.replace(value, '[REDACTED]')
        return {'type': type(error).__name__, 'message': message}
    binding = {'schemaVersion': 1, 'runId': run_id, 'jumpHostOnly': True, 'exclusiveHealService': True,
        'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
        'bucketPrefix': bucket, 'endpoints': {f'node{i}': f'http://rustfs-node{i}:9000'
            for i in range(1, len(nodes) + 1)}, 'defaultEndpoint': 'node1',
        'candidateCommit': candidate['source_sha'], 'harnessCommit': args.testing_commit,
        'candidatePackageSha256': candidate['package_sha256'], 'candidateBinarySha256': args.binary_sha,
        'rcBinarySha256': hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        'maxInventoryObjects': 32, 'maxInventoryBytes': 512 * 1024**2}
    validate_binding(binding)
    fleet.record('binding', binding)
    fleet.record('inspector-build', inspector)
    probe = Probe(binding, credential_pair=(access, secret))
    result = {'topology': topology, 'branch': branch, 'diagnosticComplete': False, 'cleanup': 'PENDING',
              'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False}
    retained_scene = None
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record('actual-topology', validate_topology(fleet.snapshot('deployed-topology'), topology))
        recorder = Recorder(probe, directory / 'fixture')
        # A bounded diagnostic fixture, not the full source DL/DV workload.
        expected = seed(recorder, bucket, 'DV', 2, [MIN_BACKPRESSURE_BYTES, 1024])
        target = next(row for row in expected if not row['deleteMarker'] and not row['isLatest']
                      and row['payload']['size'] >= MIN_BACKPRESSURE_BYTES)
        baseline = fleet.record('original-ACK-bound-S3', probe.inventory(bucket))
        fleet.record('original-ACK-proof', verify_seed(expected, baseline))
        controller = observe_successor if branch == 'queued-successor' else observe_timeout
        diagnostic = controller(probe, fleet, bucket, target, directory / branch)
        require(diagnostic.get('diagnosticComplete') is True
                and diagnostic.get('canonicalCasePassClaimed') is False
                and diagnostic.get('wholeCasePassClaimed') is False,
                'I05 timeout did not complete its diagnostic-only contract')
        physical = fleet.snapshot('all-peer-verification-before-public-reads')
        for endpoint in binding['endpoints']:
            peer = Probe(binding, endpoint, credential_pair=(access, secret))
            observed = fleet.record('all-peer-S3', {'endpoint': endpoint, **peer.inventory(bucket)})
            if branch == 'timeout':
                fleet.record('all-peer-ACK-proof', {'endpoint': endpoint,
                    **verify_seed(expected, observed), **compare_inventories(baseline, observed)})
            else:
                # The successor controller retains exact public properties and
                # separately verifies its new ACK before these outer reads.
                require(endpoint in diagnostic['verifiedEndpoints'], 'I05 successor omitted a peer')
                wanted = diagnostic['expectedEntries']
                wanted = [{name: value for name, value in item.items() if name not in {'properties', 'tags'}}
                          for item in wanted]
                fleet.record('all-peer-ACK-proof', {'endpoint': endpoint, **verify_seed(wanted, observed)})
        require(physical_content(physical) == physical_content(fleet.snapshot('physical-after-all-peer-reads')),
                'I05 peer verification changed the diagnostic physical scene')
        baseline_resources = diagnostic['callerObservation']['baselineResources']
        require(set(baseline_resources) == set(nodes), 'I05 diagnostic omitted a peer resource baseline')
        deadline = time.monotonic() + DRAIN_SECONDS
        while True:
            validate_binding(binding)
            resources = {}
            for node in nodes:
                resources[node] = fleet.record('after-peer-verification-resources', {
                    'node': node, **fleet.remote(node, 'process-metrics')})
                same_process(resources[node].get('identity'), baseline_resources[node]['identity'])
                require(type(resources[node].get('fileDescriptors')) is int
                        and resources[node]['fileDescriptors'] >= 0, 'I05 final peer FD census is incomplete')
            require(time.monotonic() <= deadline, 'NOT_OBSERVED: I05 peer verification FD census exceeded its budget')
            if all(resources[node]['fileDescriptors'] <= baseline_resources[node]['fileDescriptors'] for node in nodes):
                break
            time.sleep(.2)
        if branch == 'queued-successor':
            # Stream checksums only after all live ownership/FD observations.
            # The stopped oracle must still be the ACK-bound pre-read scene.
            fleet.stop()
            stopped = fleet.snapshot('I05-successor-stopped-before-integrity', quiescent=True)
            require(physical_content(physical) == physical_content(stopped),
                    'I05 successor stopping the service changed the verified physical scene')
            original_versions = {item['versionId'] for item in baseline['entries']}
            added = [item for item in diagnostic['expectedEntries'] if item['versionId'] not in original_versions]
            require(len(added) == 1 and added[0]['key'] == target['key']
                    and added[0]['isLatest'] is True and added[0]['deleteMarker'] is False,
                    'I05 successor stopped verification lacks one exact new data version')
            new_target = map_target(stopped, bucket, target['key'], 'data', 1, added[0]['versionId'])
            fleet.record('I05-successor-new-shard-integrity', verify_cohort(fleet, new_target, 'I05-successor-shard'))
            require(physical_content(stopped) == physical_content(
                    fleet.snapshot('I05-successor-stopped-after-integrity', quiescent=True)),
                    'I05 successor streaming verification changed physical content')
        result['diagnosticComplete'] = True
    except (Exception, KeyboardInterrupt) as error:
        result['failure'] = error_info(error)
    finally:
        if result['diagnosticComplete']:
            try:
                result['cleanup'] = 'PASS' if fleet.cleanup() else 'FAIL'
            except (Exception, KeyboardInterrupt) as error:
                result['cleanup'] = 'FAIL'
                result['cleanupError'] = error_info(error)
        else:
            # A successful contender or uncertain seed must not be erased by
            # namespace cleanup. Stop and retain the original files and ACKs.
            result.update(cleanup='FAIL', namespacesRetained=True)
            try:
                if fleet.reserved:
                    fleet.stop()
                    retained_scene = fleet.snapshot('original-failure-scene', quiescent=True)
                    for node in fleet.reserved:
                        fleet.record('original-failure-logs', {'node': node, **fleet.remote(node, 'logs')})
            except (Exception, KeyboardInterrupt) as error:
                result['failureCaptureError'] = error_info(error)
        fleet.record('diagnostic-result', result)
        if branch == 'queued-successor' and retained_scene is not None and 'failureCaptureError' not in result:
            try:
                save(directory / 'successor-retention.json', seal_successor_scene(directory, binding, retained_scene))
            except (Exception, KeyboardInterrupt) as error:
                result['retentionCaptureError'] = error_info(error)
                fleet.record('retention-capture-error', result['retentionCaptureError'])
    return result


def run(args):
    require(args.topologies in (['H1'], ['H2'], ['H1', 'H2']), 'I05 topology order must be H1 then H2')
    require(isinstance(args.binary_sha, str) and re.fullmatch(r'[0-9a-f]{64}', args.binary_sha),
            'I05 deployment requires the installed binary digest')
    candidate, inspector = preflight(args)
    branch = getattr(args, 'diagnostic_branch', 'timeout')
    native_proof = verify_native_prerequisite(args, candidate) if branch == 'queued-successor' else None
    # run_case.sh creates this parent and opens test.log before invoking us.
    # Reuse only those wrapper files; a prior topology or summary is a replay.
    require(not args.evidence.is_symlink(), 'I05 evidence directory must not be a symlink')
    args.evidence.mkdir(parents=True, exist_ok=True)
    require(all(path.name in {'timeout-preflight.json', 'setup.log', 'test.log'}
                and path.is_file() and not path.is_symlink() for path in args.evidence.iterdir()),
            'I05 requires fresh topology and summary evidence before deployment')
    if native_proof is not None:
        save(args.evidence / 'native-prerequisite.json', native_proof)
    results = []
    for topology in args.topologies:
        result = run_topology(args, candidate, inspector, topology)
        results.append(result)
        if not result['diagnosticComplete'] or result['cleanup'] != 'PASS':
            break
    summary = {'diagnostics': results, 'requestedTopologies': args.topologies,
        'diagnosticBranch': getattr(args, 'diagnostic_branch', 'timeout'),
        'diagnosticComplete': len(results) == len(args.topologies)
            and all(row['diagnosticComplete'] and row['cleanup'] == 'PASS' for row in results),
        'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False,
        'remaining': ['exact disconnect-edge ownership and complete native timeout/stale fences',
                      'stale-generation controller and independent queued-successor live acceptance',
                      'complete H1/H2 source DL/DV ACK, physical, lock and FD acceptance']}
    save(args.evidence / 'timeout-diagnostic-summary.json', summary)
    save(args.evidence / 'healthy-summary.json', {**summary, 'caseId': CASE_ID, 'complete': False,
        'diagnosticBranchesPassed': summary['diagnosticComplete'],
        'cleanup': 'PASS' if results and all(row['cleanup'] == 'PASS' for row in results) else 'FAIL'})
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    argv = sys.argv[1:]
    preflight_only = argv[:1] == ['preflight']
    if preflight_only:
        argv = argv[1:]
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--case-id', choices=(CASE_ID,), default=CASE_ID)
    parser.add_argument('--testing-commit', required=True)
    parser.add_argument('--binary-sha', required=not preflight_only)
    parser.add_argument('--tool-directory', type=Path, required=True)
    parser.add_argument('--rc-bin', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=not preflight_only)
    parser.add_argument('--topologies', nargs='+', choices=('H1', 'H2'), default=['H1', 'H2'])
    parser.add_argument('--diagnostic-branch', choices=('timeout', 'queued-successor'),
                        default=os.environ.get('I05_DIAGNOSTIC_BRANCH', 'timeout'))
    parser.add_argument('--native-directory', type=Path, default=os.environ.get('VALIDATION_HEAL_NATIVE_DIRECTORY'))
    args = parser.parse_args(argv)
    require(args.topologies in (['H1'], ['H2'], ['H1', 'H2']), 'I05 topology order must be H1 then H2')
    if preflight_only:
        preflight(args)
        print(json.dumps({'preflightPassed': True, 'candidateCommit': CANDIDATE,
                          'diagnosticBranch': args.diagnostic_branch,
                          'nativePrerequisitePassed': args.diagnostic_branch == 'queued-successor',
                          'diagnosticOnly': True, 'canonicalCasePassClaimed': False}))
        return 0
    signal.signal(signal.SIGTERM, interrupted)
    return 0 if run(args)['diagnosticComplete'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
