"""Original live I05 evidence selects preservation only, before any contender."""

import hashlib
import json
import os
from pathlib import Path
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


class TimeoutRetentionTests(unittest.TestCase):
    def archive(self, directory):
        root = Path(directory) / 'artifact'
        with zipfile.ZipFile(FIXTURE) as archive:
            archive.extractall(root)
        artifact = root / 'rustfs-validation-39711742431-1'
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


if __name__ == '__main__':
    unittest.main()
