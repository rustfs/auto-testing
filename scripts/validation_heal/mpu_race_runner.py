"""Frozen G05 same-key multipart execution; runtime admission is owned by race.py."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import secrets
import time
from urllib.parse import quote
import uuid

try:
    from . import race, mpu_race_fixture as model, mpu_race_schedule as schedule
    from . import mpu_race_physical as staging, mpu_race_committed as business
    from .heal_probe import Probe, canonical, compare_inventories, observation_phase, payload, require, save
    from .heal_workload import Recorder, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .cursor import validate_missing_batch, verify_part_manifest
    from .shards import files, validate_repaired_physical
    from .heal_fault import bounded_fault_id
    from .status_keepalive import KeepAliveStatusProbe
    from .checkpoint import records as journal_records
    from .persistence import exact_records
except ImportError:
    import race, mpu_race_fixture as model, mpu_race_schedule as schedule
    import mpu_race_physical as staging, mpu_race_committed as business
    from heal_probe import Probe, canonical, compare_inventories, observation_phase, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from cursor import validate_missing_batch, verify_part_manifest
    from shards import files, validate_repaired_physical
    from heal_fault import bounded_fault_id
    from status_keepalive import KeepAliveStatusProbe
    from checkpoint import records as journal_records
    from persistence import exact_records


CASE = 'heal.race-multipart-complete-abort'


def observe_status(probe, fleet, path, token, reference):
    with probe.status_observation() as measured:
        require(measured is not None, 'G05 warm/race status requires its original keepalive session')
        request_value = {'path': path, 'query': [('clientToken', token)], 'method': 'POST', 'body': '',
                         'statusObservation': {'sessionId': measured['sessionId'], 'sequence': measured['sequence']}}
        with observation_phase(measured, 'requestPersistence'):
            fleet.record('G05-status-request', request_value)
            request_file = f'{fleet.sequence:05d}-G05-status-request.json'
        with observation_phase(measured, 'requestEvidenceHash'):
            request_sha = hashlib.sha256((fleet.evidence / request_file).read_bytes()).hexdigest()
        value = probe.admin(path, [('clientToken', token)], b'', observation=measured)
        value['requestSha256'] = request_sha
        with observation_phase(measured, 'responsePersistence'):
            fleet.record('G05-status-response', value)
            response_file = f'{fleet.sequence:05d}-G05-status-response.json'
        measured.update(requestFile=request_file, responseFile=response_file, responsePersisted=True)
        race.settings(value, reference)
        return value


def ack_records(recorder, expected=None):
    requests = sorted(recorder.output.glob('*-request.json'))
    acks = sorted(recorder.output.glob('*-ack.json'))
    count = recorder.sequence if expected is None else expected
    require(len(requests) == len(acks) == recorder.sequence == count,
            'G05 SDK journal has an orphan, missing request/ACK or wrong population')
    result = []
    for sequence, (request_path, ack_path) in enumerate(zip(requests, acks, strict=True), 1):
        request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
        observation = ack.get('observation') or {}
        require(request_path.name == f'{sequence:06d}-request.json' and ack_path.name == f'{sequence:06d}-ack.json'
                and request.get('runId') == ack.get('runId') == recorder.probe.binding['runId']
                and request.get('sequence') == ack.get('sequence') == sequence
                and ack.get('requestFile') == request_path.name
                and ack.get('requestSha256') == hashlib.sha256(request_path.read_bytes()).hexdigest()
                and ack.get('operationCheck') == 'PASS'
                and observation.get('operation') == request.get('operation')
                and observation.get('params') == request.get('params') and observation.get('resultUnknown') is False,
                'G05 original SDK request/ACK pairing, identity or result differs')
        result.append(ack)
    return result


def seed(recorder, bucket, branch):
    require(branch in schedule.SELECTED, 'unregistered G05 branch')
    recorder.call('create_bucket', {'Bucket': bucket}, expect_http=200)
    recorder.call('put_bucket_versioning', {'Bucket': bucket, 'VersioningConfiguration': {'Status': 'Enabled'}}, expect_http=200)
    controls = []
    for index in range(256):
        key = f'objects-{index:04d}.bin'; body = payload(bucket + '/' + key, race.SIZE)
        response = recorder.call('put_object', {'Bucket': bucket, 'Key': key}, body, expect_http=200)
        version = response.get('VersionId')
        require(isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int != 0
                and response.get('ETag') == model.etag(body), 'G05 control lacks its real version and exact payload ETag')
        controls.append({'key': key, 'versionId': version, 'isLatest': True, 'deleteMarker': False,
                         'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}})
    fixture = model.MpuRaceFixture(recorder, bucket)
    prepared = schedule.prepare(fixture, branch)
    ack_records(recorder, 270 + int(branch != 'upload-continue'))
    return controls, fixture, prepared


def identities(manifest):
    return [{'role': role, 'uploadId': item['uploadId'], 'bucket': manifest['bucket'], 'key': manifest['key']}
            for role, item in manifest['items'].items()]


def capture_staging(fleet, manifest, binary_sha, *, quiescent):
    return {node: fleet.remote(node, 'mpu-race-scene', bucket=manifest['bucket'], items=identities(manifest),
                              expectedBinarySha256=binary_sha, quiescent=quiescent) for node in fleet.nodes}


def observe_io(fleet, branch):
    for node in fleet.nodes:
        process = fleet.record('G05-io-process', {'node': node, **fleet.remote(node, 'process')})
        value = fleet.record('G05-kernel-no-cap-sample', {'node': node, **fleet.remote(node, 'race-io-observe',
            caseId=CASE, branch=branch, expectedProcess={k: v for k, v in process.items() if k != 'node'})})
        require(value.get('inspectionOnly') is True and value.get('mutated') is False
                and value.get('actualKernelNoLimitVerifiedAtSamples') is True
                and value.get('continuousNoLimitProved') is False, 'G05 kernel IO observation is incomplete')


def foreground(fixture, prepared, probe, fleet, path, token, reference, binary_sha):
    """Only the selected first SDK call needs a contained original-token sample."""
    samples = []
    def checkpoint(label, manifest):
        physical = capture_staging(fleet, manifest, binary_sha, quiescent=False)
        value = {'phase': label, 'manifest': manifest, 'physical': physical}
        # Worker uses independent names; Fleet.record's sequence stays on the controller.
        save(fleet.evidence / 'mpu-checkpoints' / (label + '.json'), value)
        return value
    with ThreadPoolExecutor(max_workers=1) as executor:
        worker = executor.submit(schedule.execute, fixture, prepared['branch'], prepared, checkpoint)
        deadline = time.monotonic() + 90
        try:
            while not worker.done() and time.monotonic() < deadline and len(samples) < 1024:
                sample = observe_status(probe, fleet, path, token, reference); samples.append(sample)
                with observation_phase(probe.observation_for(sample), 'pollPause'):
                    time.sleep(.005)
            if not worker.done() and time.monotonic() >= deadline:
                fixture.recorder.mutations.poison('G05 original observation deadline exceeded; drain only')
        except BaseException:
            fixture.recorder.mutations.poison('G05 status observation failed; drain only')
            raise
        result = worker.result(timeout=90)
    records = ack_records(fixture.recorder, 3 if prepared['branch'] == 'upload-continue' else 2)
    selected = records[0]
    require(selected['sequence'] == result['selectedAckSequence']
            and selected['observation']['operation'] == prepared['selectedOperation']
            and fixture.poisoned is None, 'G05 selected first mutation differs from original schedule')
    matches = [sample['statusObservation'] for sample in samples
               if race.sample_overlap(selected, sample, path, token, reference)]
    proof = {'selectedOperation': prepared['selectedOperation'], 'selectedAckSequence': selected['sequence'],
             'matchingStatusSamples': matches, 'sampleCount': len(samples), 'sampleBudget': 1024,
             'pollPauseSeconds': .005, 'allCompanionACKsVerified': True, 'publicCallOverlapObserved': bool(matches),
             'nativeFenceProved': False, 'serverCommitOverlapProved': False, 'canonicalCasePassClaimed': False}
    fleet.record('G05-selected-public-overlap', proof)
    return result, bool(matches)


def wait_terminal(probe, fleet, path, token, reference):
    deadline = time.monotonic() + 180
    while True:
        value = observe_terminal(probe, fleet, path, token, reference)
        if value['response']['summary'] != 'running': return value['response']
        require(time.monotonic() < deadline, 'G05 original root completion exceeded180seconds')
        time.sleep(.1)


def observe_terminal(probe, fleet, path, token, reference):
    value = fleet.record('G05-original-terminal-status', probe.admin(path, [('clientToken', token)], b''))
    require(value.get('httpStatus') == 200 and isinstance(value.get('response'), dict)
            and value['response'].get('summary') in {'running', 'finished', 'stopped'}
            and canonical(value['response'].get('settings')) == canonical(reference['effective']),
            'G05 original terminal status identity/settings unavailable')
    # A stopped root remains a failure in terminal_proof. Preserve it so the
    # frozen source's independent final diagnostics still run.
    return value


def simple_entries(controls, manifest):
    return controls + [{key: value for key, value in row.items() if key != 'multipartParts'}
                       for row in business.entries(manifest)]


def physical_population(snapshot, bucket, controls):
    keys = {row['key'] for row in controls} | {model.KEY}
    require(all(row['path'].startswith(bucket + '/') and row['path'].split('/', 2)[1] in keys
                for value in snapshot.values() for row in value['files']),
            'G05 physical population contains an unacknowledged object')


def final_checks(probe, fleet, binding, controls, fixture, original, executed, before, before_staging,
                 mapped_controls, baseline, terminal, path, token, reference, binary_sha, inspector_sha):
    """Keep independent frozen-source diagnostics after assertion failures.

    Evidence I/O errors remain fatal. No failed assertion can authorize cleanup,
    blind restoration or a later success claim.
    """
    violations = []
    def check(label, operation):
        try:
            proof = operation()
        except ValueError as error:
            proof = {'state': 'FAIL', 'error': str(error), 'passClaimed': False}
            violations.append({'stage': label, 'error': str(error)})
        fleet.record(label, proof)
        return proof

    def repeat():
        for _ in range(2):
            repeated = observe_terminal(probe, fleet, path, token, reference)['response']
            require(repeated['summary'] == terminal['summary']
                    and canonical(repeated.get('outcome')) == canonical(terminal.get('outcome')),
                    'G05 original terminal responsibility changed on repetition')
        return {'terminalStable': True}
    check('G05-terminal-repeat', repeat)
    fleet.stop(); after = fleet.snapshot('product-physical-before-GET-or-rescue', quiescent=True)
    check('G05-final-physical-population', lambda: physical_population(after, fixture.bucket, controls))
    def verify_controls():
        for target in mapped_controls.values():
            validate_repaired_physical(race.object_scene(before, fixture.bucket, target['key']),
                                       race.object_scene(after, fixture.bucket, target['key']), target)
        return {'physicalControlsReconstructed': 256}
    check('G05-control-reconstruction', verify_controls)
    backups = fleet.record('G05-original-backup-decoding', {node: fleet.remote(node, 'mpu-race-backups', bucket=fixture.bucket,
        expectedBinarySha256=binary_sha, expectedInspectorSha256=inspector_sha) for node in fleet.nodes})
    final = executed['manifest']
    def verify_committed():
        _, proof = business.transition(before, after, original, final, backups, binary_sha, inspector_sha)
        return proof
    physical = check('G05-committed-transition', verify_committed)
    previous = before_staging
    for step in executed['steps']:
        checkpoint = step['checkpoint']
        check('G05-staging-' + step['phase'], lambda: staging.verify(checkpoint['manifest'], checkpoint['physical'],
              fleet.nodes, binary_sha, previous=previous, committed=after))
        previous = checkpoint['physical']
    def full_streams():
        final_controls = race.map_fixture(after, fixture.bucket, controls)
        # Independently map even when another committed-transition assertion failed.
        old_dir = business.mapped(before, fixture.bucket, business.entries(original))[0][
            original['items']['committed']['versionId'], 1]['dataDir']
        final_mpu = business.mapped(after, fixture.bucket, business.entries(final), backup_directories=[old_dir])[0]
        return verify_part_manifest(fleet, list(final_controls.values()) + list(final_mpu.values()), 'final-streaming', count=262)
    check('G05-final-full-streams', full_streams)
    fleet.start(); inventory = None
    for endpoint in binding['endpoints']:
        peer = Probe(binding, endpoint)
        reader = Recorder(peer, fleet.evidence / ('final-read-ACKs-' + endpoint))
        check('G05-all-peer-multipart-S3-' + endpoint, lambda: fixture.verify_s3(reader))
        current = fleet.record('all-peer-final-S3', {'endpoint': endpoint, **peer.inventory(fixture.bucket)})
        check('G05-final-payload-' + endpoint, lambda: verify_seed(simple_entries(controls, final), current))
        check('G05-control-properties-' + endpoint, lambda: compare_inventories(
            {'bucket': fixture.bucket, 'entries': [row for row in baseline['entries'] if row['key'] != model.KEY]},
            {'bucket': fixture.bucket, 'entries': [row for row in current['entries'] if row['key'] != model.KEY]}))
        if inventory is not None:
            check('G05-peer-inventory-' + endpoint, lambda: compare_inventories(inventory, current))
        inventory = current
        check('G05-final-read-ledger-' + endpoint, lambda: ack_records(reader))
    fleet.stop()
    def unchanged():
        require(physical_content(after) == physical_content(fleet.snapshot('physical-after-S3', quiescent=True)),
                'G05 final full GET changed the already-proven physical result')
        return {'postGetPhysicalUnchanged': True}
    check('G05-post-GET-physical', unchanged)
    terminal_proof = check('G05-original-terminal-proof', lambda: race.terminal_proof(terminal, fixture.bucket, baseline, inventory))
    def retired():
        roots = exact_records(journal_records(fleet, token, 'G05-original-root-retirement', quiescent=True, family='root'), token, 'root')
        require(not roots,
                'G05 original runnable root responsibility was not retired')
        return {'originalRootRetired': True}
    check('G05-root-retirement-proof', retired)
    fleet.record('G05-contract-violations', violations)
    require(not violations, 'G05 final contracts failed; independent diagnostics retained')
    return physical, terminal_proof


def run_branch(args, topology, nodes, branch):
    require(args.case_id == CASE and branch in schedule.SELECTED and len(nodes) == (1 if topology == 'H1' else 4),
            'G05 runner requires its original case/branch and H1/H2 hosts')
    candidate = json.loads(args.candidate.read_text())
    require(candidate['source_sha'] == race.COPY_CANDIDATE, 'G05 same-key runner requires its reviewed native candidate')
    reference = race.COPY_REFERENCE
    directory = args.evidence / topology / branch; directory.mkdir(parents=True, exist_ok=False)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile='manual')
    inspector = fleet.record('inspector-build', verify_inspector(args.tool_directory, candidate))
    bucket = 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {'schemaVersion': 1, 'runId': run_id, 'jumpHostOnly': True, 'exclusiveHealService': True,
        'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), 'bucketPrefix': bucket,
        'endpoints': {f'node{i}': f'http://rustfs-node{i}:9000' for i in range(1, len(nodes) + 1)},
        'defaultEndpoint': 'node1', 'candidateCommit': candidate['source_sha'], 'harnessCommit': args.testing_commit,
        'candidatePackageSha256': candidate['package_sha256'], 'candidateBinarySha256': args.binary_sha,
        'rcBinarySha256': hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        'maxInventoryObjects': 512, 'maxInventoryBytes': 512 * 1024**2}
    fleet.record('binding', binding)
    fleet.record('source-contract', {'caseId': CASE, 'branch': branch, 'objects': 257, 'fixture': 'DM', 'fault': 'F1',
        'sameKey': model.KEY, 'requested': reference['requested'], 'effective': reference['effective'],
        'processIoCapRequested': False, 'nativeFenceProved': False, 'canonicalCasePassClaimed': False})
    probe = KeepAliveStatusProbe(binding)
    result = {'caseId': CASE, 'topology': topology, 'branch': branch, 'fixture': 'DM', 'fault': 'F1',
              'verdict': 'RUNNING', 'cleanup': 'PENDING', 'canonicalCasePassClaimed': False}
    seed_recorder, target_authority = None, False
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record('topology-proof', validate_topology(fleet.snapshot('deployed-topology'), topology))
        seed_recorder = Recorder(probe, directory / 'seed-ACKs')
        controls, fixture, prepared = seed(seed_recorder, bucket, branch)
        original = prepared['manifest']
        reader = Recorder(probe, directory / 'mpu-read-ACKs')
        fleet.record('G05-original-multipart-S3', fixture.verify_s3(reader))
        fleet.record('G05-baseline-read-ledger', ack_records(reader))
        baseline = fleet.record('complete-baseline-S3', probe.inventory(bucket))
        verify_seed(simple_entries(controls, original), baseline)
        before_staging = fleet.record('G05-original-staging', capture_staging(fleet, original, args.binary_sha, quiescent=False))
        staging.verify(original, before_staging, nodes, args.binary_sha)
        fleet.stop(); before = fleet.snapshot('complete-baseline-physical', quiescent=True)
        physical_population(before, bucket, controls)
        mapped_controls = race.map_fixture(before, bucket, controls)
        mapped_mpu, _ = business.mapped(before, bucket, business.entries(original))
        baseline_parts = list(mapped_controls.values()) + list(mapped_mpu.values())
        fleet.record('G05-baseline-full-streams', verify_part_manifest(fleet, baseline_parts, 'baseline-streaming', count=259))
        committed = mapped_mpu[original['items']['committed']['versionId'], 1]
        selected = next(part for part in committed['cohort'] if part['node'] == nodes[0] and part['disk'] == 3)
        targets = list(mapped_controls.values()) + [{**committed, **selected}]
        require(len(targets) == 257 and all(target['node'] == nodes[0] and target['disk'] == 3 for target in targets),
                'G05 faults must cover all 257 original objects on the fourth drive of the first host')
        receipts = []
        for index, target in enumerate(targets):
            params = {'disk': target['disk'], 'faultId': bounded_fault_id('G05', branch, index)}
            fleet.faults.append((target['node'], params))
            receipts.append(fleet.record('exact-missing-part', {'node': target['node'], **fleet.remote(target['node'],
                'apply-fault', **params, relativePath=target['relativePath'], expectedSha256=target['sha256'], kind='missing')}))
        damaged = fleet.snapshot('complete-fault-physical', quiescent=True)
        fleet.record('fault-batch-proof', validate_missing_batch(before, damaged, targets, receipts))
        fleet.start(); observe_io(fleet, branch)
        require(physical_content(damaged) == physical_content(fleet.snapshot('attribution-before-START')),
                'G05 missing shards changed before original manual admission')
        path = '/rustfs/admin/v3/heal/' + quote(bucket, safe='')
        start = fleet.record('original-Admin-start', probe.admin(path, [], json.dumps(reference['requested']).encode()))
        token = start.get('response', {}).get('clientToken') if isinstance(start.get('response'), dict) else None
        require(start.get('httpStatus') == 200 and isinstance(token, str) and str(uuid.UUID(token)) == token,
                'G05 admission lacks one original canonical token')
        with probe.status_session(token, path) as transport:
            try:
                observe_status(probe, fleet, path, token, reference)
                witness = fleet.snapshot('G05-target-window')
                missing = (selected['node'], selected['disk'], selected['relativePath']) not in files(witness)
                fixture.recorder = Recorder(probe, directory / 'foreground-ACKs')
                executed, overlapped = foreground(fixture, prepared, probe, fleet, path, token, reference, args.binary_sha)
            finally:
                fleet.record('G05-status-transport', transport)
        observe_io(fleet, branch)
        terminal = wait_terminal(probe, fleet, path, token, reference)
        physical, result['terminal'] = final_checks(probe, fleet, binding, controls, fixture, original, executed, before,
            before_staging, mapped_controls, baseline, terminal, path, token, reference, args.binary_sha, inspector['binarySha256'])
        target_authority = True; fleet.faults.clear()
        fleet.record('last-ACK-physical-proof', physical)
        if not (missing and overlapped): raise race.WindowNotObserved('G05 selected missing part and SDK running-token overlap not both observed')
        result.update(verdict='PASS', publicCallOverlapObserved=True, nativeFenceProved=False, physical=physical)
    except (Exception, KeyboardInterrupt) as error:
        result.update(verdict='NOT_OBSERVED' if isinstance(error, race.WindowNotObserved) else 'FAIL',
                      failure={'type': type(error).__name__, 'message': str(error)})
    finally:
        issued = seed_recorder is not None and seed_recorder.sequence > 0
        if issued and not target_authority:
            try:
                fleet.stop(); fleet.snapshot('retained-failure-scene', quiescent=True)
                fleet.record('raw-mpu-failure-scene', {node: fleet.remote(node, 'mpu-scene', bucket=bucket,
                    keys=[model.KEY], expectedBinarySha256=args.binary_sha) for node in nodes})
            except Exception as error:
                fleet.record('failure-scene-error', {'type': type(error).__name__, 'message': str(error)})
            fleet.record('last-ACK-rescue-required', {'state': 'DIRTY', 'namespacesRetained': True,
                'lastACKPhysicalAuthorityVerified': False, 'originalGenerationBlindRestorationProhibited': True})
            result['cleanup'] = 'FAIL'
        else:
            result['cleanup'] = 'PASS' if fleet.cleanup() else 'FAIL'
        fleet.record('branch-result', result)
    return result
