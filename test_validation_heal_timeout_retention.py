"""Original live I05 evidence selects preservation only, before any contender."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from scripts.validation_heal import timeout_retention as controller, retained_scene as retained
from scripts.validation_heal import race_retention, recover_environment as recovery
from scripts.validation_heal.heal_probe import canonical, payload
from scripts.validation_heal.lock_timeout import CONTENDER_BYTES
import test_validation_heal_race_retention as race_tests


FIXTURE = Path(__file__).parent / 'testdata/validation/i05-pre-contender-39711742431.zip'
DRAIN_FIXTURE = Path(__file__).parent / 'testdata/validation/i05-drain-budget-39746002648.zip'
DESCRIPTOR_FIXTURE = Path(__file__).parent / 'testdata/validation/i05-descriptor-drain-39754893467.zip'
FILTER_STARTUP_FIXTURE = Path(__file__).parent / 'testdata/validation/i05-filter-startup-39761856012.zip'
H2_FILTER_STARTUP_FIXTURE = Path(__file__).parent / 'testdata/validation/i05-h2-filter-startup-39764496504.zip'


class TimeoutRetentionTests(unittest.TestCase):
    def h2_startup_archive(self, directory):
        root = Path(directory) / 'artifact'
        archive_root = root / 'rustfs-validation-39764496504-1'
        with zipfile.ZipFile(H2_FILTER_STARTUP_FIXTURE) as archive:
            archive.extractall(archive_root)
        return root, json.loads((archive_root / 'candidate.json').read_text()), archive_root

    def test_exact_h2_startup_failure_retains_all_peers_after_actual_h1_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, archive = self.h2_startup_archive(directory)
            before = {p: p.read_bytes() for p in archive.rglob('*') if p.is_file()}
            with patch.object(recovery, 'Fleet') as fleet:
                plan, = recovery.recovery_plan(root, 39764496504, candidate)
            fleet.assert_not_called()
            self.assertEqual(plan['runId'], 'gh-39764496504-1-h2')
            self.assertEqual(plan['nodes'], ['vm000', 'vm001', 'vm002', 'vm003'])
            self.assertEqual(plan['harnessCommit'], controller.H2_FILTER_STARTUP_HELPER)
            self.assertEqual(plan['ackFilesSha256'], controller.H2_FILTER_STARTUP_ARCHIVE_SHA256)
            race_retention.validate(plan)
            self.assertEqual(before, {p: p.read_bytes() for p in archive.rglob('*') if p.is_file()})

    def test_h2_startup_refuses_unreviewed_run_effects_cleanup_peers_and_archive_drift(self):
        variants = ('run', 'candidate', 'helper', 'h1-cleanup', 'h1-result', 'scene',
                    'missing-peer', 'workload', 'extra-case', 'format', 'symlink')
        for variant in variants:
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, archive = self.h2_startup_archive(directory)
                evidence = archive / 'run' / controller.CASE / 'evidence' / controller.CASE
                failed_run = 39764496504
                if variant == 'run': failed_run += 1
                elif variant == 'candidate': candidate['package_sha256'] = '0' * 64
                elif variant == 'helper':
                    path = archive / 'run' / controller.CASE / 'cases' / (controller.CASE + '.json')
                    value = json.loads(path.read_text())
                    value['provenance']['testing_commit'] = controller.FILTER_STARTUP_HELPER
                    path.write_text(json.dumps(value))
                elif variant in ('h1-cleanup', 'h1-result'):
                    name = '00040-cleanup.json' if variant == 'h1-cleanup' else '00042-diagnostic-result.json'
                    (evidence / 'H1' / name).write_text('{}')
                elif variant == 'missing-peer': (evidence / 'H2/00009-reserved.json').unlink()
                elif variant == 'workload':
                    (evidence / 'H2/fixture').mkdir()
                    (evidence / 'H2/fixture/000001-request.json').write_text('{}')
                elif variant == 'extra-case':
                    (archive / 'run' / controller.CASE / 'cases/other.json').write_text('{}')
                else:
                    path = evidence / 'H2/00014-original-failure-scene.json'
                    if variant == 'symlink':
                        copy = Path(directory) / 'scene.json'
                        copy.write_bytes(path.read_bytes())
                        path.unlink()
                        path.symlink_to(copy)
                    else:
                        value = json.loads(path.read_text())
                        if variant == 'scene': value['vm002']['files'].append({'path': 'unknown'})
                        else: value['vm002']['formats'][0]['sha256'] = '0' * 64
                        path.write_text(json.dumps(value))
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises(ValueError):
                    recovery.recovery_plan(root, failed_run, candidate)
                fleet.assert_not_called()

    def startup_archive(self, directory):
        root = Path(directory) / 'artifact'
        archive_root = root / 'rustfs-validation-39761856012-1'
        with zipfile.ZipFile(FILTER_STARTUP_FIXTURE) as archive:
            archive.extractall(archive_root)
        candidate = json.loads((archive_root / 'candidate.json').read_text())
        return root, candidate, archive_root

    def test_exact_filter_startup_failure_preserves_empty_stopped_scene_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, archive = self.startup_archive(directory)
            before = {p: p.read_bytes() for p in archive.rglob('*') if p.is_file()}
            with patch.object(recovery, 'Fleet') as fleet:
                plan, = recovery.recovery_plan(root, 39761856012, candidate)
            fleet.assert_not_called()
            self.assertEqual(plan['runId'], 'gh-39761856012-1-h1')
            self.assertEqual(plan['kind'], 'unresolved-i05-timeout')
            self.assertEqual(plan['harnessCommit'], controller.FILTER_STARTUP_HELPER)
            self.assertEqual(plan['ackFilesSha256'], controller.FILTER_STARTUP_ARCHIVE_SHA256)
            self.assertEqual(before, {p: p.read_bytes() for p in archive.rglob('*') if p.is_file()})

    def test_startup_isolation_refuses_changed_or_additional_effects_before_fleet(self):
        for variant in ('run', 'candidate', 'missing', 'changed', 'request', 'H2', 'symlink'):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, archive = self.startup_archive(directory)
                folder = archive / 'run' / controller.CASE / 'evidence' / controller.CASE / 'H1'
                failed_run = 39761856012
                if variant == 'run': failed_run += 1
                elif variant == 'candidate': candidate['package_sha256'] = '0' * 64
                elif variant == 'missing': (folder / '00008-original-failure-scene.json').unlink()
                elif variant == 'changed': (folder / '00010-diagnostic-result.json').write_text('{}')
                elif variant == 'request':
                    (folder / 'fixture').mkdir()
                    (folder / 'fixture/000001-request.json').write_text('{}')
                elif variant == 'H2': (folder.parent / 'H2').mkdir()
                else:
                    target = folder / '00008-original-failure-scene.json'
                    copy = Path(directory) / 'scene.json'
                    copy.write_bytes(target.read_bytes())
                    target.unlink()
                    target.symlink_to(copy)
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises(ValueError):
                    recovery.recovery_plan(root, failed_run, candidate)
                fleet.assert_not_called()

    def archive(self, directory):
        root = Path(directory) / 'artifact'
        with zipfile.ZipFile(FIXTURE) as archive:
            archive.extractall(root)
        artifact = root / 'rustfs-validation-39711742431-1'
        candidate = json.loads((artifact / 'candidate.json').read_text())
        folder = artifact / 'run' / controller.CASE / 'evidence' / controller.CASE / 'H1'
        return root, candidate, folder

    def drain_archive(self, directory, run=39746002648):
        root = Path(directory) / 'drain-artifact'
        with zipfile.ZipFile(DRAIN_FIXTURE if run == 39746002648 else DESCRIPTOR_FIXTURE) as archive:
            archive.extractall(root)
        artifact = root / f'rustfs-validation-{run}-1'
        candidate = json.loads((artifact / 'candidate.json').read_text())
        folder = artifact / 'run' / controller.CASE / 'evidence' / controller.CASE / 'H1'
        return root, candidate, folder

    def add_contender_archive(self, root, candidate, folder):
        def read(path): return json.loads(path.read_text())
        def write(path, value): path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))

        result_path = folder.parents[2] / 'cases' / f'{controller.CASE}.json'
        result = read(result_path)
        result['provenance']['testing_commit'] = controller.CONTENDER_HELPER
        write(result_path, result)
        binding_path = folder / '00001-binding.json'
        binding = read(binding_path)
        binding['harnessCommit'] = controller.CONTENDER_HELPER
        write(binding_path, binding)
        binding_hash = hashlib.sha256(canonical(binding).encode()).hexdigest()

        summary_path = folder.parent / 'healthy-summary.json'
        summary = read(summary_path)
        row = dict(summary['diagnostics'][0])
        row['failure'] = controller.FAILURES[controller.CONTENDER_HELPER]
        summary['diagnostics'] = [row]
        write(summary_path, summary)
        diagnostic_path = folder / '00021-diagnostic-result.json'
        diagnostic = read(diagnostic_path)
        diagnostic['failure'] = controller.FAILURES[controller.CONTENDER_HELPER]
        write(diagnostic_path, diagnostic)

        renamed = (
            ('00018-quiescent.json', '00022-quiescent.json'),
            ('00019-original-failure-scene.json', '00023-original-failure-scene.json'),
            ('00020-original-failure-logs.json', '00024-original-failure-logs.json'),
            ('00021-diagnostic-result.json', '00025-diagnostic-result.json'),
        )
        for old, new in renamed: (folder / old).rename(folder / new)

        first = read(folder / '00017-I05-caller-window.json')
        first_after = first['afterCursor']
        middle_after = first_after + '-middle'
        last_after = middle_after + '-last'
        for name, before, after in (
                ('00019-I05-caller-window.json', first_after, middle_after),
                ('00021-I05-caller-window.json', middle_after, last_after)):
            write(folder / name, {'node': 'vm000', 'unit': 'rustfs-validation-heal-' +
                binding['runId'] + '.service', 'complete': True, 'beforeCursor': before,
                'afterCursor': after, 'hasMore': False, 'sourceMessageLoss': [], 'journal': ''})
        write(folder / '00018-I05-caller-before-disconnect-socket.json', {'readOnly': True})
        write(folder / '00020-I05-caller-admission-resources.json', {'readOnly': True})

        scope_path = folder / 'timeout/scope.json'
        scope = read(scope_path)
        scope['bindingSha256'] = binding_hash
        write(scope_path, scope)
        run_id, bucket, key = scope['runId'], binding['bucketPrefix'], scope['entry']['key']
        caller_path = folder / 'timeout/caller'
        for name in ('request.json', 'request-written.json', 'disconnected.json'):
            request_path = caller_path / 'transport' / name
            caller_request = read(request_path)
            caller_request['harnessCommit'] = controller.CONTENDER_HELPER
            caller_request['bindingSha256'] = binding_hash
            write(request_path, caller_request)
        caller_request_bytes = (caller_path / 'transport/request.json').read_bytes()
        for name in ('request-written.json', 'disconnected.json'):
            request_path = caller_path / 'transport' / name
            caller_request = read(request_path)
            caller_request['requestSha256'] = hashlib.sha256(caller_request_bytes).hexdigest()
            write(request_path, caller_request)
        caller_request = read(caller_path / 'transport/request.json')
        domain = caller_request['clockDomain']
        contender = folder / 'timeout/contender'
        contender.mkdir()
        body = payload(f'{run_id}/I05-timeout-contender', CONTENDER_BYTES)
        params = {'Bucket': bucket, 'Key': key, 'Metadata': {'heal-fixture': 'I05-timeout-contender'}}
        request = {'schemaVersion': 2, 'runId': run_id, 'sequence': 1, 'operation': 'put_object',
            'params': params, 'identity': None, 'expectHttp': 503, 'clockDomain': domain,
            'preparedMonotonicNs': 1000000, 'preparedAt': '2026-10-22T00:00:00+00:00',
            'casePassClaimed': False,
            'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}}
        request_bytes = json.dumps(request, sort_keys=True, separators=(',', ':')).encode()
        (contender / '000001-request.json').write_bytes(request_bytes)
        request_id = '30000000-0000-4000-8000-000000000001'
        response = {'Error': {'Code': 'ServiceUnavailable', 'Message': 'retry'},
            'ResponseMetadata': {'RequestId': request_id, 'HostId': '', 'HTTPStatusCode': 503,
                'HTTPHeaders': {'retry-after': '1', 'x-amz-request-id': request_id}, 'RetryAttempts': 0}}
        ack = {'schemaVersion': 2, 'runId': run_id, 'sequence': 1, 'requestFile': '000001-request.json',
            'requestSha256': hashlib.sha256(request_bytes).hexdigest(), 'operationCheck': 'PASS',
            'casePassClaimed': False, 'clockDomain': domain,
            'collectorCallStartedMonotonicNs': 1001000, 'collectorCallFinishedMonotonicNs': 2001000,
            'recordedMonotonicNs': 2002000, 'observation': {'endpoint': 'node1', 'operation': 'put_object',
                'params': params, 'payload': request['payload'], 'clockDomain': domain,
                'clientCallInvoked': True, 'acknowledged': False, 'resultUnknown': False,
                'clientCleanupError': None, 'callStartedMonotonicNs': 1002000,
                'callFinishedMonotonicNs': 2000000, 'response': response}}
        write(contender / '000001-ack.json', ack)

        admission = {'node': 'vm000', 'attempt': {'request_id': '20000000-0000-4000-8000-000000000001',
            'lock_attempt_id': '20000000-0000-4000-8000-000000000002', 'owner': ':::9000',
            'namespace': 'pool-0', 'admitted': True, 'observedAcquiredWithoutDrop': True}}
        write(caller_path / 'admission.json', admission)
        rows = []
        def lock_row(mode, op, obj, attempt, state, **extra):
            return {'target': 'rustfs_ecstore::object_lock_diag', 'request_id': request_id,
                'event': 'object_namespace_lock', 'component': 'ecstore', 'subsystem': 'object_lock',
                'op': op, 'mode': mode, 'bucket': '.rustfs.sys' if mode == 'read' else bucket,
                'object': obj, 'namespace': 'pool-0', 'owner': ':::9000', 'lock_attempt_id': attempt,
                'requested_version_id': None, 'timeout_ms': 1000, 'acquire_ms': 1001 if state == 'failed' else 0,
                'state': state, 'level': 'ERROR' if state == 'failed' else 'TRACE',
                'span': {'name': 'put_object_with_old_current_size'},
                'spans': [{'name': 'http-request', 'request_id': request_id, 'method': 'PUT',
                           'uri': f'/{bucket}/{key}'}], **extra}
        read_attempt = '20000000-0000-4000-8000-000000000003'
        for state in ('acquiring', 'acquired', 'guard_dropped'):
            rows.append(lock_row('read', 'get_object', f'buckets/{bucket}/.bucket-incarnation', read_attempt, state))
        write_attempt = '20000000-0000-4000-8000-000000000004'
        rows.append(lock_row('write', 'put_object_commit', key, write_attempt, 'acquiring'))
        rows.append(lock_row('write', 'put_object_commit', key, write_attempt, 'failed', failure='timeout'))
        write(folder / '00021-I05-caller-window.json', {'node': 'vm000', 'unit': 'rustfs-validation-heal-' +
            binding['runId'] + '.service', 'complete': True, 'beforeCursor': middle_after,
            'afterCursor': last_after, 'hasMore': False, 'sourceMessageLoss': [],
            'journal': '\n'.join(json.dumps(row, sort_keys=True, separators=(',', ':')) for row in rows)})
        return contender

    def add_h2_failure_archive(self, root, candidate, h1_folder, *, window_rounds=1):
        self.add_contender_archive(root, candidate, h1_folder)
        evidence = h1_folder.parent
        h2_folder = evidence / 'H2'
        shutil.copytree(h1_folder, h2_folder)

        def read(path): return json.loads(path.read_text())
        def write(path, value): path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))
        old_binding = read(h2_folder / '00001-binding.json')
        old_run, old_bucket = old_binding['runId'], old_binding['bucketPrefix']
        run_id = old_run[:-2] + 'h2'
        bucket = 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]

        def replace(value):
            if isinstance(value, dict): return {replace(key): replace(item) for key, item in value.items()}
            if isinstance(value, list): return [replace(item) for item in value]
            if isinstance(value, str): return value.replace(old_run, run_id).replace(old_bucket, bucket)
            return value

        for path in h2_folder.rglob('*.json'):
            write(path, replace(read(path)))
        nodes = ['vm000', 'vm001', 'vm002', 'vm003']
        binding = read(h2_folder / '00001-binding.json')
        binding.update(runId=run_id, bucketPrefix=bucket, harnessCommit=controller.H2_CONTENDER_HELPER,
                       endpoints={f'node{i}': f'http://rustfs-node{i}:9000' for i in range(1, 5)})
        binding_hash = hashlib.sha256(canonical(binding).encode()).hexdigest()
        write(h2_folder / '00001-binding.json', binding)

        requests = {}
        for sequence in range(1, 10):
            request_path = h2_folder / f'fixture/{sequence:06d}-request.json'
            ack_path = h2_folder / f'fixture/{sequence:06d}-ack.json'
            request, ack = read(request_path), read(ack_path)
            request['runId'] = ack['runId'] = run_id
            request['params']['Bucket'] = ack['observation']['params']['Bucket'] = bucket
            if sequence >= 3 and sequence != 6:
                index, generation = (0, sequence - 3) if sequence <= 6 else (1, sequence - 7)
                size = 32 * 1024**2 if index == 0 else 1024
                body = payload(f'{bucket}/objects/{index:04d}.bin/{generation}', size)
                item = {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
                request['payload'] = ack['observation']['payload'] = item
                etag = hashlib.md5(body, usedforsecurity=False).hexdigest()
                ack['observation']['response']['ETag'] = etag
                requests[ack['observation']['response']['VersionId']] = (item, etag)
            raw = json.dumps(request, sort_keys=True, separators=(',', ':')).encode()
            request_path.write_bytes(raw)
            ack['requestSha256'] = hashlib.sha256(raw).hexdigest()
            write(ack_path, ack)

        for name in ('00010-original-ACK-bound-S3.json', '00013-I05-timeout-original-S3.json'):
            path = h2_folder / name
            value = read(path)
            value['bucket'] = bucket
            for entry in value['entries']:
                if not entry['deleteMarker']:
                    entry['payload'], etag = requests[entry['versionId']]
                    entry['properties']['ETag'] = etag
            write(path, value)

        scope_path = h2_folder / 'timeout/scope.json'
        scope = read(scope_path)
        scope.update(runId=run_id, candidateCommit=controller.CANDIDATE, bindingSha256=binding_hash)
        scope_entry = next(entry for entry in read(h2_folder / '00010-original-ACK-bound-S3.json')['entries']
                           if entry['key'] == scope['entry']['key']
                           and entry['versionId'] == scope['entry']['versionId'])
        scope['entry']['payload'] = requests[scope_entry['versionId']][0]
        write(scope_path, scope)
        caller_dir = h2_folder / 'timeout/caller'
        transport = caller_dir / 'transport'
        request_path = transport / 'request.json'
        caller_request = read(request_path)
        caller_request.update(runId=run_id, harnessCommit=controller.H2_CONTENDER_HELPER,
                              candidateCommit=controller.CANDIDATE, bucket=bucket,
                              bindingSha256=binding_hash,
                              requestTarget=f"/{bucket}/{scope_entry['key']}?versionId={scope_entry['versionId']}")
        caller_request['params']['Bucket'] = bucket
        raw = json.dumps(caller_request, sort_keys=True, separators=(',', ':')).encode()
        request_path.write_bytes(raw)
        for name in ('request-written.json', 'disconnected.json'):
            path = transport / name
            value = read(path)
            value.update(runId=run_id, harnessCommit=controller.H2_CONTENDER_HELPER,
                         candidateCommit=controller.CANDIDATE, bucket=bucket,
                         bindingSha256=binding_hash,
                         requestTarget=caller_request['requestTarget'],
                         requestSha256=hashlib.sha256(raw).hexdigest())
            write(path, value)

        contender = h2_folder / 'timeout/contender'
        contender_request_path = contender / '000001-request.json'
        contender_ack_path = contender / '000001-ack.json'
        contender_request = read(contender_request_path)
        contender_ack = read(contender_ack_path)
        contender_body = payload(f'{run_id}/I05-timeout-contender', CONTENDER_BYTES)
        contender_request.update(runId=run_id, payload={'size': len(contender_body),
            'sha256': hashlib.sha256(contender_body).hexdigest()})
        contender_request['params']['Bucket'] = bucket
        raw = json.dumps(contender_request, sort_keys=True, separators=(',', ':')).encode()
        contender_request_path.write_bytes(raw)
        contender_ack.update(runId=run_id, requestSha256=hashlib.sha256(raw).hexdigest())
        contender_ack['observation']['params']['Bucket'] = bucket
        contender_ack['observation']['payload'] = contender_request['payload']
        write(contender_ack_path, contender_ack)

        records = {}
        for path in sorted(h2_folder.glob('*.json')):
            match = path.name.split('-', 1)
            label = match[1][:-5]
            records.setdefault(label, []).append(read(path))
        for path in h2_folder.glob('*.json'):
            path.unlink()

        def clone(value): return json.loads(json.dumps(value))
        def by_node(value, node):
            result = clone(value)
            if isinstance(result, dict): result['node'] = node
            return result
        old_reservation = records['reserved'][0]
        old_owner = old_reservation['owner']
        owners = {}
        reservations = []
        uploads = []
        for node in nodes:
            owner = clone(old_owner)
            owner.update(runId=run_id, bucket=bucket, hostname=node,
                         unit=f'rustfs-validation-heal-{run_id}.service')
            owners[node] = owner
            reservation = clone(old_reservation)
            reservation.update(node=node, owner=owner)
            reservations.append(reservation)
            uploads.append(by_node(records['inspector-upload'][0], node))

        baseline_resources = records['I05-caller-baseline-resources'][0]
        resources = [by_node(baseline_resources, node) for node in nodes]
        for value, node in zip(resources, nodes, strict=True):
            value['identity']['binarySha256'] = binding['candidateBinarySha256']
            value['identity']['settings'] = controller.runtime_config('lock-timeout')
        original_scene = records['original-failure-scene'][0]['vm000']
        scenes = {}
        for node in nodes:
            scenes[node] = clone(original_scene)
            scenes[node].update(hostname=node, unit=owners[node]['unit'],
                                binarySha256=binding['candidateBinarySha256'])
        physical = records['I05-timeout-original-physical'][0]
        old_physical = physical['vm000']
        snapshots = {node: clone(old_physical) for node in nodes}
        started = {node: {'started': True} for node in nodes}
        deployed = records['deployed-topology'][0]
        deployed = {node: clone(deployed['vm000']) for node in nodes}
        preflight = records['preflight'][0]
        preflight = {node: {**clone(preflight['vm000']), 'hostname': node} for node in nodes}
        checkpoint = records['I05-caller-checkpoint'][0]
        checkpoints = [by_node(checkpoint, node) for node in nodes]
        cursor_by_node = {node: f'{node}-checkpoint' for node in nodes}
        windows = []
        source_windows = records['I05-caller-window']
        for stage in range(3):
            for round_index in range(window_rounds):
                for node in nodes:
                    source = source_windows[stage]
                    window = by_node(source, node)
                    window['unit'] = owners[node]['unit']
                    window['beforeCursor'] = cursor_by_node[node]
                    cursor_by_node[node] = f'{node}-window-{stage}-{round_index + 1}'
                    window['afterCursor'] = cursor_by_node[node]
                    if node != 'vm000' or stage != 2 or round_index != 0: window['journal'] = ''
                    windows.append(window)
        admission_resources = [clone(value) for value in resources]
        for value in admission_resources:
            value['identity']['binarySha256'] = binding['candidateBinarySha256']
            value['identity']['settings'] = controller.runtime_config('lock-timeout')
        quiescent_source = records['quiescent'][0].get('vm000', {'stopped': True})
        quiescent = {node: clone(quiescent_source) for node in nodes}
        logs = [by_node(records['original-failure-logs'][0], node) for node in nodes]

        attestation = records['integrity-fleet-attestation'][0]
        attestation['nodes'] = nodes
        attestation['allParticipatingNodesUseCandidate'] = True
        actual = {'topology': 'H2', 'nodes': 4, 'independentDrives': 16}
        diagnostic_result = {'topology': 'H2', 'diagnosticComplete': False, 'cleanup': 'FAIL',
            'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False,
            'failure': controller.FAILURES[controller.H2_CONTENDER_HELPER], 'namespacesRetained': True}
        labels = controller.h2_labels((window_rounds, window_rounds, window_rounds))
        by_label = {
            'binding': [binding], 'inspector-build': records['inspector-build'],
            'preflight': [preflight], 'integrity-fleet-attestation': [attestation],
            'reserved': reservations, 'inspector-upload': uploads, 'started': [started],
            'deployed-topology': [deployed], 'actual-topology': [actual],
            'original-ACK-bound-S3': records['original-ACK-bound-S3'],
            'original-ACK-proof': records['original-ACK-proof'],
            'I05-timeout-original-physical': [snapshots], 'I05-timeout-original-S3': records['I05-timeout-original-S3'],
            'I05-timeout-baseline-physical-after-public-read': [clone(snapshots)],
            'I05-caller-baseline-resources': resources, 'I05-caller-checkpoint': checkpoints,
            'I05-caller-window': windows, 'I05-caller-before-disconnect-socket': records['I05-caller-before-disconnect-socket'],
            'I05-caller-admission-resources': admission_resources, 'quiescent': [quiescent],
            'original-failure-scene': [scenes], 'original-failure-logs': logs,
            'diagnostic-result': [diagnostic_result],
        }
        indexes = {}
        for label in labels:
            index = indexes.get(label, 0)
            value = by_label[label][index]
            indexes[label] = index + 1
            write(h2_folder / f'{sum(indexes.values()):05d}-{label}.json', value)

        shutil.rmtree(h1_folder)
        h1_folder.mkdir()
        write(h1_folder / '00001-diagnostic-result.json', {'topology': 'H1', 'diagnosticComplete': True,
            'cleanup': 'PASS', 'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False})
        case_path = root / 'rustfs-validation-39711742431-1' / 'run' / controller.CASE / 'cases' / f'{controller.CASE}.json'
        case_result = read(case_path)
        case_result['provenance']['testing_commit'] = controller.H2_CONTENDER_HELPER
        write(case_path, case_result)
        summary_path = evidence / 'healthy-summary.json'
        summary = read(summary_path)
        summary['diagnostics'] = [
            {'topology': 'H1', 'diagnosticComplete': True, 'cleanup': 'PASS',
             'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False},
            diagnostic_result]
        summary.update(diagnosticComplete=False, diagnosticBranchesPassed=False, cleanup='FAIL', complete=False,
                       wholeCasePassClaimed=False, canonicalCasePassClaimed=False)
        write(summary_path, summary)
        return h2_folder

    def test_original_live_failure_yields_exact_pre_contender_preservation_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, _ = self.archive(directory)
            plans = recovery.recovery_plan(root, 39711742431, candidate)
            self.assertEqual(len(plans), 1)
            plan = plans[0]
            self.assertEqual(plan['kind'], 'unresolved-i05-timeout')
            self.assertEqual(plan['runId'], 'gh-39711742431-1-h1')
            self.assertEqual(plan['nodes'], ['vm000'])
            self.assertEqual(plan['ackFilesSha256'], 'fc8576efabec0e2163f8b434d0971f307b3334b01c3afae90459e2280722b70c')
            self.assertEqual(plan['businessSceneSha256'], {'vm000':
                '6c9e808ff0c6d03cb5dfd5546bd588c3d3e1bcee3c167877dec21c4399fc1e6d'})

    def test_new_helper_failed_refusal_yields_exact_contender_preservation_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, folder = self.archive(directory)
            self.add_contender_archive(root, candidate, folder)
            plan, = recovery.recovery_plan(root, 39711742431, candidate)
            self.assertEqual(plan['kind'], 'unresolved-i05-timeout')
            self.assertEqual(plan['harnessCommit'], controller.CONTENDER_HELPER)
            self.assertEqual(plan['nodes'], ['vm000'])
            self.assertEqual(plan['businessSceneSha256'], {'vm000':
                '6c9e808ff0c6d03cb5dfd5546bd588c3d3e1bcee3c167877dec21c4399fc1e6d'})

    def test_drain_budget_failure_yields_exact_bounded_preservation_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, _ = self.drain_archive(directory)
            plan, = recovery.recovery_plan(root, 39746002648, candidate)
            self.assertEqual(plan['kind'], 'unresolved-i05-timeout')
            self.assertEqual(plan['runId'], 'gh-39746002648-1-h1')
            self.assertEqual(plan['harnessCommit'], controller.DRAIN_BUDGET_HELPER)
            self.assertEqual(plan['nodes'], ['vm000'])
            self.assertEqual(plan['ackFilesSha256'], '21e9471c9e9133186b88fb664f8a4deff4c93044761a9c399915a0b1db9d6f32')
            self.assertEqual(plan['businessSceneSha256'], {'vm000':
                'c438f5bf295f56ddd168e9e840b41281863c3da06c0973eb9abead95edd63e89'})

    def test_descriptor_failure_preserves_exact_scene_without_leak_or_pass_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, folder = self.drain_archive(directory, 39754893467)
            plan, = recovery.recovery_plan(root, 39754893467, candidate)
            self.assertEqual(plan['harnessCommit'], controller.DESCRIPTOR_HELPER)
            self.assertEqual(plan['kind'], 'unresolved-i05-timeout')
            self.assertEqual(plan['ackFilesSha256'],
                'f114c1f2f4d8ddbbc5c98549e1e32f8da61241b4ccf3123bf89f9f3aed88879d')
            self.assertEqual(plan['businessSceneSha256'], {'vm000':
                'f921b69ea51622f89e9ad4a78de8c890caf0b7ffefa0b401457d6f9796fc7d31'})
            result = json.loads((folder.parents[2] / 'cases' / f'{controller.CASE}.json').read_text())
            self.assertEqual((result['verdict'], result['cleanup_status']), ('FAIL', 'FAIL'))

    def test_drain_window_boundary_follows_labels_after_variable_caller_wait(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, folder = self.drain_archive(directory, 39754893467)
            empty = folder / '00023-I05-caller-window.json'
            value = json.loads(empty.read_text())
            self.assertEqual(value['journal'], '')
            self.assertEqual(value['beforeCursor'], value['afterCursor'])
            empty.unlink()
            for path in sorted(folder.glob('*.json')):
                index = int(path.name[:5])
                if index > 23:
                    path.rename(path.with_name(f'{index - 1:05d}' + path.name[5:]))
            plan, = recovery.recovery_plan(root, 39754893467, candidate)
            self.assertEqual(plan['runId'], 'gh-39754893467-1-h1')

    def test_descriptor_failure_refuses_corrupt_census_and_unbounded_effects(self):
        variants = ('missing-census', 'count', 'inode', 'duplicate-fd', 'foreign-tcp', 'tcp-join',
                    'namespace', 'process', 'leak-claim', 'raw-path', 'vanished', 'socket-match',
                    'success', 'unknown', 'extra-action', 'physical-drift')
        for variant in variants:
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, folder = self.drain_archive(directory, 39754893467)
                path = next(folder.glob('*-I05-caller-drain-resources.json'))
                value = json.loads(path.read_text())
                census = value['descriptorCensus']
                if variant == 'missing-census': value.pop('descriptorCensus')
                elif variant == 'count': census['fileDescriptors'] += 1
                elif variant == 'inode': next(row for row in census['descriptors'] if row['kind'] == 'socket')['inode'] += 1
                elif variant == 'duplicate-fd': census['descriptors'][-1]['descriptor'] = 0
                elif variant == 'foreign-tcp': census['ownedTcpConnections'][0]['inode'] = 999999999
                elif variant == 'tcp-join': census['ownedTcpConnections'][0]['descriptors'] = [0]
                elif variant == 'namespace': value['networkNamespace'] = 'net:[1]'
                elif variant == 'process': value['identity']['startTicks'] += 1
                elif variant == 'leak-claim': census['noLeakProven'] = True
                elif variant == 'raw-path': census['descriptors'][0]['target'] = '/private/path'
                elif variant == 'vanished': census['vanishedDescriptors'] = 1
                elif variant == 'socket-match':
                    path = next(folder.glob('*-I05-caller-drain-socket.json'))
                    value = json.loads(path.read_text())
                    value['samples'][0]['ownedTcpConnections'][0]['descriptors'] = [0]
                elif variant in ('success', 'unknown'):
                    path = folder / 'timeout/contender/000001-ack.json'
                    value = json.loads(path.read_text())
                    if variant == 'success': value['observation']['response']['ResponseMetadata']['HTTPStatusCode'] = 200
                    else: value['observation']['resultUnknown'] = True
                elif variant == 'extra-action': (folder / 'timeout/contender/000002-request.json').write_text('{}')
                else:
                    path = next(folder.glob('*-original-failure-scene.json'))
                    value = json.loads(path.read_text())
                    value['vm000']['formats'][0]['sha256'] = '0' * 64
                path.write_text(json.dumps(value))
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises((ValueError, KeyError)):
                    recovery.recovery_plan(root, 39754893467, candidate)
                fleet.assert_not_called()

    def test_drain_budget_failure_refuses_unknown_mutation_or_incomplete_sampling(self):
        for variant in ('success', 'unknown', 'extra-action', 'missing-sample', 'physical-drift'):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, folder = self.drain_archive(directory)
                if variant == 'success':
                    ack = json.loads((folder / 'timeout/contender/000001-ack.json').read_text())
                    ack['observation']['response']['ResponseMetadata']['HTTPStatusCode'] = 200
                    (folder / 'timeout/contender/000001-ack.json').write_text(json.dumps(ack))
                elif variant == 'unknown':
                    ack = json.loads((folder / 'timeout/contender/000001-ack.json').read_text())
                    ack['observation']['resultUnknown'] = True
                    (folder / 'timeout/contender/000001-ack.json').write_text(json.dumps(ack))
                elif variant == 'extra-action':
                    (folder / 'timeout/contender/000002-request.json').write_text('{}')
                elif variant == 'missing-sample':
                    next(folder.glob('*-I05-caller-drain-resources.json')).unlink()
                else:
                    scene = json.loads((folder / '00058-original-failure-scene.json').read_text())
                    scene['vm000']['formats'][0]['sha256'] = '0' * 64
                    (folder / '00058-original-failure-scene.json').write_text(json.dumps(scene))
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises((ValueError, KeyError)):
                    recovery.recovery_plan(root, 39746002648, candidate)
                fleet.assert_not_called()

    def test_h2_failure_after_clean_h1_yields_exact_four_node_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, h1_folder = self.archive(directory)
            self.add_h2_failure_archive(root, candidate, h1_folder, window_rounds=2)
            plan, = recovery.recovery_plan(root, 39711742431, candidate)
            self.assertEqual(plan['kind'], 'unresolved-i05-timeout')
            self.assertEqual(plan['runId'], 'gh-39711742431-1-h2')
            self.assertEqual(plan['harnessCommit'], controller.H2_CONTENDER_HELPER)
            self.assertEqual(plan['nodes'], ['vm000', 'vm001', 'vm002', 'vm003'])
            self.assertEqual(set(plan['owners']), set(plan['businessSceneSha256']))

    def test_h2_retention_refuses_partial_peer_or_unclean_h1_evidence(self):
        for variant in ('missing-reservation', 'missing-peer-window', 'missing-peer-log', 'h1-dirty', 'peer-contender'):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, h1_folder = self.archive(directory)
                h2_folder = self.add_h2_failure_archive(root, candidate, h1_folder)
                if variant == 'missing-reservation':
                    (h2_folder / '00005-reserved.json').unlink()
                elif variant == 'missing-peer-window':
                    window = next(path for path in h2_folder.glob('*-I05-caller-window.json')
                                  if json.loads(path.read_text()).get('node') == 'vm002')
                    window.unlink()
                elif variant == 'missing-peer-log':
                    log = next(path for path in h2_folder.glob('*-original-failure-logs.json')
                               if json.loads(path.read_text()).get('node') == 'vm002')
                    log.unlink()
                elif variant == 'h1-dirty':
                    summary_path = h1_folder.parent / 'healthy-summary.json'
                    summary = json.loads(summary_path.read_text())
                    summary['diagnostics'][0]['cleanup'] = 'FAIL'
                    summary_path.write_text(json.dumps(summary))
                else:
                    window = next(path for path in h2_folder.glob('*-I05-caller-window.json')
                                  if json.loads(path.read_text()).get('node') == 'vm001')
                    value = json.loads(window.read_text())
                    value['journal'] = json.dumps({'request_id': '30000000-0000-4000-8000-000000000001'})
                    window.write_text(json.dumps(value))
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises((ValueError, KeyError)):
                    recovery.recovery_plan(root, 39711742431, candidate)
                fleet.assert_not_called()

    def test_new_helper_refuses_unknown_success_or_corrupt_lock_lifecycle(self):
        for variant in ('unknown', 'success', 'acquired', 'read-lifecycle', 'extra-action'):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, folder = self.archive(directory)
                contender = self.add_contender_archive(root, candidate, folder)
                ack_path = contender / '000001-ack.json'
                ack = json.loads(ack_path.read_text())
                if variant == 'unknown': ack['observation']['resultUnknown'] = True
                elif variant == 'success':
                    ack['observation']['response']['ResponseMetadata']['HTTPStatusCode'] = 200
                elif variant == 'extra-action':
                    (contender / '000002-request.json').write_text('{}')
                elif variant == 'acquired':
                    window_path = folder / '00021-I05-caller-window.json'
                    window = json.loads(window_path.read_text())
                    events = [json.loads(line) for line in window['journal'].splitlines()]
                    events[-1]['state'] = 'acquired'
                    window['journal'] = '\n'.join(json.dumps(event) for event in events)
                    window_path.write_text(json.dumps(window))
                elif variant == 'read-lifecycle':
                    window_path = folder / '00021-I05-caller-window.json'
                    window = json.loads(window_path.read_text())
                    events = [json.loads(line) for line in window['journal'].splitlines()]
                    events[2]['object'] = 'buckets/foreign/.bucket-incarnation'
                    window['journal'] = '\n'.join(json.dumps(event) for event in events)
                    window_path.write_text(json.dumps(window))
                ack_path.write_text(json.dumps(ack))
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises((ValueError, KeyError)):
                    recovery.recovery_plan(root, 39711742431, candidate)
                fleet.assert_not_called()

    def test_tampered_or_unresolved_evidence_refuses_before_fleet_creation(self):
        variants = ('candidate', 'helper', 'missing-ack', 'ack-hash', 'ack-unknown', 'ack-operation',
                    'request-mutation', 'payload', 'marker', 'owner', 'physical', 'live', 'other-node',
                    'contender', 'extra-fixture', 'other-case', 'H2', 'failure', 'open-socket', 'GET-mutation',
                    'GET-version', 'GET-hash', 'GET-retry', 'extra-transport', 'inspector', 'extra-action',
                    'metadata', 'tags')
        for variant in variants:
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, folder = self.archive(directory)
                def change(name, mutate):
                    path = folder / name; value = json.loads(path.read_text()); mutate(value)
                    path.write_text(json.dumps(value))
                if variant == 'candidate': candidate['source_sha'] = '1' * 40
                elif variant == 'helper': change('00001-binding.json', lambda v: v.update(harnessCommit='2' * 40))
                elif variant == 'missing-ack': (folder / 'fixture/000009-ack.json').unlink()
                elif variant.startswith('ack-'):
                    field, value = {'ack-hash': ('requestSha256', '0' * 64),
                                    'ack-unknown': ('observation', {'resultUnknown': True}),
                                    'ack-operation': ('observation', {'operation': 'complete_multipart_upload'})}[variant]
                    change('fixture/000003-ack.json', lambda v: v.update({field: value}))
                elif variant in ('request-mutation', 'payload'):
                    change('fixture/000003-request.json', lambda v: v.update(
                        operation='delete_object') if variant == 'request-mutation' else v.update(payload={'size': 1}))
                    request = folder / 'fixture/000003-request.json'
                    change('fixture/000003-ack.json', lambda v: v.update(requestSha256=hashlib.sha256(request.read_bytes()).hexdigest()))
                elif variant == 'marker':
                    change('fixture/000006-ack.json', lambda v: v['observation']['response'].update(DeleteMarker=False))
                elif variant == 'owner': change('00005-reserved.json', lambda v: v['owner'].update(runtimeProfile='manual'))
                elif variant in ('physical', 'live', 'other-node'):
                    change('00019-original-failure-scene.json', lambda v:
                        v['vm000']['files'][0].update(sha256='0' * 64) if variant == 'physical' else
                        v['vm000'].update(pid=7) if variant == 'live' else v.update(vm001=v['vm000']))
                elif variant == 'contender': (folder / 'timeout/contender').mkdir()
                elif variant == 'extra-fixture': (folder / 'fixture/000010-request.json').write_text('{}')
                elif variant == 'other-case':
                    (folder.parents[2] / 'cases/other.json').write_text('{}')
                elif variant == 'H2': (folder.parent / 'H2').mkdir()
                elif variant == 'failure': change('00021-diagnostic-result.json', lambda v: v.update(failure={}))
                elif variant in ('open-socket', 'GET-retry'):
                    change('timeout/caller/transport/disconnected.json', lambda v: v.update(
                        clientSocketClosed=False) if variant == 'open-socket' else v.update(connectionAttempts=2))
                elif variant in ('GET-mutation', 'GET-version'):
                    change('timeout/caller/transport/request.json', lambda v: v.update(operation='put_object')
                        if variant == 'GET-mutation' else v['params'].update(VersionId='foreign'))
                elif variant == 'GET-hash':
                    change('timeout/caller/transport/request-written.json', lambda v: v.update(requestSha256='0' * 64))
                elif variant == 'extra-transport': (folder / 'timeout/caller/transport/ack.json').write_text('{}')
                elif variant == 'inspector': change('00002-inspector-build.json', lambda v: v.update(binarySha256='0' * 64))
                elif variant in ('metadata', 'tags'):
                    def mutate(value):
                        if variant == 'metadata': value['entries'][0]['properties']['Metadata']['nonce'] = 'foreign'
                        else: value['entries'][0]['tags'] = []
                    for name in ('00010-original-ACK-bound-S3.json', '00013-I05-timeout-original-S3.json'):
                        change(name, mutate)
                else: (folder / '00022-another-action.json').write_text('{}')
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises((ValueError, KeyError)):
                    recovery.recovery_plan(root, 39711742431, candidate)
                fleet.assert_not_called()

    def test_main_routes_preservation_before_generic_cleanup_and_never_deploys(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, _ = self.archive(directory)
            plan = recovery.recovery_plan(root, 39711742431, candidate)[0]
            path = Path(directory) / 'candidate.json'; path.write_text(json.dumps(candidate))
            evidence = Path(directory) / 'recovery'
            fleet = SimpleNamespace(nodes=plan['nodes'], run_id=plan['runId'], record=lambda label, value: value)
            proof = {'runId': plan['runId'], 'cleanup': 'ISOLATED', 'canonicalPassClaimed': False}
            with patch('sys.argv', ['recover', '--failed-run-id', '39711742431', '--artifact', str(root),
                    '--candidate', str(path), '--evidence', str(evidence)]), \
                    patch.object(recovery, 'Fleet', return_value=fleet), \
                    patch.object(recovery, 'isolate_race_scene', return_value=proof) as isolate:
                recovery.main()
            isolate.assert_called_once_with(fleet, plan)
            summary = json.loads((evidence / 'recovery-summary.json').read_text())
            self.assertIn('ISOLATED', json.dumps(summary))

    def scene(self, directory):
        helper, state, roots, node, request, scene = race_tests.RaceRetentionTests().scene(directory)
        owner = request['expectedOwner']; owner['runtimeProfile'] = 'lock-timeout'
        for folder in (*roots, state):
            (folder / 'owner.json').write_text(json.dumps(owner))
        for root in roots:
            for path in (root / 'quarantine').iterdir(): path.unlink()
        return helper, state, roots, node, request, scene

    def test_all_five_interrupted_moves_preserve_metadata_content_and_unread_credentials(self):
        for interrupt_after in range(1, 6):
            with self.subTest(interrupt_after=interrupt_after), tempfile.TemporaryDirectory() as directory:
                helper, state, roots, node, request, _ = self.scene(directory)
                original = {path: (path.stat(), path.read_bytes()) for folder in (*roots, state)
                            for path in folder.rglob('*') if path.is_file()}
                opened, moves = [], 0
                original_open = os.open
                def record(path, *args, **kwargs):
                    opened.append(str(path)); return original_open(path, *args, **kwargs)
                def interrupted(source, target):
                    nonlocal moves
                    helper.local_rename(source, target)
                    if target.is_dir():
                        moves += 1
                        if moves == interrupt_after: raise RuntimeError('lost connection after rename')
                with helper.root_stats(), patch.object(os, 'open', side_effect=record):
                    before = retained.run(request, node)
                    change = {**request, 'action': 'retained-scene-isolate', 'expectedBaseline': before['baseline']}
                    with patch.object(retained, 'rename_new', side_effect=interrupted), self.assertRaises(RuntimeError):
                        retained.run(change, node)
                    with patch.object(retained, 'rename_new', side_effect=helper.local_rename):
                        proof = retained.run(change, node)
                    self.assertTrue(proof['activeNamespaceIsolated'])
                    self.assertEqual(proof['baseline'], before['baseline'])
                    self.assertEqual(proof['locations'], ['retained'] * 5)
                    self.assertEqual(len(proof['privateArchiveParents']), 5)
                    self.assertFalse(retained.run(change, node)['mutated'])
                retained_state, retained_roots = retained.destinations(state, roots, per_run_parent=True)
                for old, (info, raw) in original.items():
                    source = next(folder for folder in (*roots, state) if old.is_relative_to(folder))
                    dest = retained_state if source == state else retained_roots[roots.index(source)]
                    moved = dest / old.relative_to(source)
                    actual = moved.stat()
                    self.assertEqual(moved.read_bytes(), raw)
                    for field in ('st_ino', 'st_dev', 'st_mode', 'st_uid', 'st_gid', 'st_size', 'st_mtime_ns'):
                        self.assertEqual(getattr(actual, field), getattr(info, field))
                self.assertFalse(any(path.endswith('service.env') or path.endswith('/credential') for path in opened))

    def test_i05_does_not_open_other_profiles_quarantine_or_multipart_files(self):
        with tempfile.TemporaryDirectory() as directory:
            helper, state, roots, node, request, _ = self.scene(directory)
            extra = roots[0] / 'quarantine/G01-target.original'
            extra.write_bytes(b'foreign backup must stay opaque')
            opened = []; original_open = os.open
            def record(path, *args, **kwargs):
                opened.append(str(path)); return original_open(path, *args, **kwargs)
            with helper.root_stats(), patch.object(os, 'open', side_effect=record): retained.run(request, node)
            self.assertNotIn(str(extra), opened)

    def test_plan_profile_cannot_be_reused_for_another_isolation_kind(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, _ = self.archive(directory)
            plan = recovery.recovery_plan(root, 39711742431, candidate)[0]
            plan['kind'] = 'unresolved-g01-race'
            with self.assertRaises(ValueError): race_retention.validate(plan)

    def successor_archive(self, directory):
        root, candidate, folder = self.archive(directory)
        for path in folder.glob('*-reserved.json'):
            value = json.loads(path.read_text())
            value['owner']['runtimeProfile'] = 'lock-successor'
            path.write_text(json.dumps(value))
        result_path = next(folder.glob('*-diagnostic-result.json'))
        result = json.loads(result_path.read_text())
        result['branch'] = 'queued-successor'
        result_path.write_text(json.dumps(result))
        summary_path = folder.parent / 'healthy-summary.json'
        summary = json.loads(summary_path.read_text())
        summary.update(diagnosticBranch='queued-successor', diagnostics=[result],
                       wholeCasePassClaimed=False)
        summary_path.write_text(json.dumps(summary))
        receipt = folder / 'queued-successor/successor'
        receipt.mkdir(parents=True)
        (receipt / '000001-request.json').write_text('{"operation":"put_object"}')
        (receipt / '000001-ack.json').write_text('{"resultUnknown":true,"acknowledged":false}')
        binding = json.loads(next(folder.glob('*-binding.json')).read_text())
        scene = json.loads(next(folder.glob('*-original-failure-scene.json')).read_text())
        seal = controller.seal_successor_scene(folder, binding, scene)
        (folder / 'successor-retention.json').write_text(json.dumps(seal))
        return root, candidate, folder

    def test_successor_unknown_receipt_selects_only_stopped_scene_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, folder = self.successor_archive(directory)
            original = {p: p.read_bytes() for p in root.rglob('*') if p.is_file()}
            with patch.object(recovery, 'Fleet') as fleet:
                plan, = recovery.recovery_plan(root, 39711742431, candidate)
            fleet.assert_not_called()
            self.assertEqual(plan['kind'], 'unresolved-i05-successor')
            self.assertEqual(plan['owners']['vm000']['runtimeProfile'], 'lock-successor')
            race_retention.validate(plan)
            self.assertEqual(original, {p: p.read_bytes() for p in root.rglob('*') if p.is_file()})
            candidate_path = Path(directory) / 'candidate.json'
            candidate_path.write_text(json.dumps(candidate))
            evidence = Path(directory) / 'recovery'
            fake = SimpleNamespace(nodes=plan['nodes'], run_id=plan['runId'], record=lambda label, value: value)
            proof = {'runId': plan['runId'], 'cleanup': 'ISOLATED', 'canonicalPassClaimed': False}
            with patch('sys.argv', ['recover', '--failed-run-id', '39711742431', '--artifact', str(root),
                    '--candidate', str(candidate_path), '--evidence', str(evidence)]), \
                    patch.object(recovery, 'Fleet', return_value=fake), \
                    patch.object(recovery, 'isolate_race_scene', return_value=proof) as isolate:
                recovery.main()
            isolate.assert_called_once_with(fake, plan)

    def test_successor_seal_refuses_changed_or_missing_original_evidence_and_foreign_profile(self):
        for variant in ('receipt', 'missing', 'extra', 'symlink', 'scene', 'candidate', 'kind', 'profile', 'dirty-predecessor'):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                root, candidate, folder = self.successor_archive(directory)
                receipt = folder / 'queued-successor/successor/000001-ack.json'
                if variant == 'receipt': receipt.write_text('{}')
                elif variant == 'missing': receipt.unlink()
                elif variant == 'extra': (folder / 'unrecorded-write.json').write_text('{}')
                elif variant == 'symlink':
                    target = Path(directory) / 'receipt.json'; target.write_bytes(receipt.read_bytes())
                    receipt.unlink(); receipt.symlink_to(target)
                elif variant == 'candidate': candidate['package_sha256'] = '0' * 64
                elif variant == 'dirty-predecessor':
                    path = folder.parent / 'healthy-summary.json'
                    summary = json.loads(path.read_text()); summary['diagnostics'] *= 2
                    path.write_text(json.dumps(summary))
                else:
                    path = folder / 'successor-retention.json'
                    seal = json.loads(path.read_text())
                    if variant == 'scene': seal['plan']['businessSceneSha256']['vm000'] = '0' * 64
                    elif variant == 'kind': seal['plan']['kind'] = 'unresolved-i05-timeout'
                    else: seal['plan']['owners']['vm000']['runtimeProfile'] = 'manual'
                    path.write_text(json.dumps(seal))
                with patch.object(recovery, 'Fleet') as fleet, self.assertRaises(ValueError):
                    recovery.recovery_plan(root, 39711742431, candidate)
                fleet.assert_not_called()

    def test_successor_isolation_preserves_all_five_original_trees(self):
        with tempfile.TemporaryDirectory() as directory:
            helper, state, roots, node, request, _ = self.scene(directory)
            request['expectedOwner']['runtimeProfile'] = 'lock-successor'
            for folder in (*roots, state):
                (folder / 'owner.json').write_text(json.dumps(request['expectedOwner']))
            original = {p: (p.stat().st_ino, p.read_bytes()) for folder in (*roots, state)
                        for p in folder.rglob('*') if p.is_file()}
            with helper.root_stats():
                before = retained.run(request, node)
                change = {**request, 'action': 'retained-scene-isolate', 'expectedBaseline': before['baseline']}
                with patch.object(retained, 'rename_new', side_effect=helper.local_rename):
                    proof = retained.run(change, node)
                    self.assertFalse(retained.run(change, node)['mutated'])
            self.assertEqual(proof['locations'], ['retained'] * 5)
            self.assertFalse(proof['originalSceneRestored'] or proof['backupRestorationUsed'] or proof['canonicalPassClaimed'])
            target_state, target_roots = retained.destinations(state, roots, per_run_parent=True)
            for path, expected in original.items():
                source = next(folder for folder in (*roots, state) if path.is_relative_to(folder))
                target = target_state if source == state else target_roots[roots.index(source)]
                moved = target / path.relative_to(source)
                self.assertEqual((moved.stat().st_ino, moved.read_bytes()), expected)


if __name__ == '__main__':
    unittest.main()
