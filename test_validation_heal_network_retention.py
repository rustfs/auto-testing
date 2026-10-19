"""Stopped I03 isolation binds business/MPU bytes and retains interrupted moves."""

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import retained_scene as retained, mpu_scene, network_retention as controller, recover_environment as recovery
import test_validation_heal_retained_scene as retained_tests


class NetworkRetentionTests(unittest.TestCase):
    def scene(self, directory):
        helper = retained_tests.RetainedSceneTests()
        state, roots, node, request = helper.scene(directory)
        shutil.copytree(roots[1], roots[0])
        owner = request['expectedOwner'];owner['runtimeProfile'] = 'lock-quorum'
        for folder in (*roots, state): (folder / 'owner.json').write_text(json.dumps(owner))
        keys = mpu_scene.keys_for(['node1', 'node3'])
        for root in roots:
            db = root / 'data' / (owner['bucket'] + '-db');db.mkdir()
            (db / 'part.1').write_bytes(b'original DB payload')
            for key in keys:
                key_hash = hashlib.sha256((owner['bucket'] + '/' + key).encode()).hexdigest()
                upload = root / 'data/.rustfs.sys/multipart' / key_hash / 'original-opaque-upload'
                upload.mkdir(parents=True)
                (upload / 'xl.meta').write_bytes(b'original uncompleted MPU metadata')
                (upload / 'part.1').write_bytes(b'original full MPU part')
            for suffix in ('original', 'intent.json', 'applied.json'):
                (root / 'quarantine' / ('I03-missing-target.' + suffix)).write_bytes(('original fault ' + suffix).encode())
        node['owned'] = lambda run: (state, roots, owner)
        node['mpu_scene_backend'] = mpu_scene
        def snapshot(value):
            self.assertEqual(value['buckets'], [owner['bucket'], owner['bucket'] + '-db'])
            files = []
            for disk, root in enumerate(roots):
                for bucket in value['buckets']:
                    for path in sorted((root / 'data' / bucket).rglob('*')):
                        if path.is_file(): files.append({'disk': disk, 'path': path.relative_to(root / 'data').as_posix(),
                            'size': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            return {'quiescent': True, 'pid': 0, 'unit': owner['unit'], 'binarySha256': owner['binarySha256'],
                    'files': files, 'formats': [{'disk': disk, 'original': True} for disk in range(4)]}
        node['snapshot'] = snapshot
        scene = snapshot({'buckets': [owner['bucket'], owner['bucket'] + '-db']})
        mpu = mpu_scene.run({'runId': request['runId'], 'action': 'mpu-scene', 'bucket': owner['bucket'],
                             'keys': keys, 'expectedBinarySha256': owner['binarySha256']}, node)
        request.update(expectedOriginalSceneSha256=retained.sha({name: scene[name] for name in ('files', 'formats')}),
                       expectedMpuSceneSha256=mpu['sceneSha256'], mpuKeys=keys)
        return helper, state, roots, node, request

    def test_original_business_mpu_backups_and_unread_credentials_survive_each_interrupted_move(self):
        for interrupt_after in range(1, 6):
            with self.subTest(interrupt_after=interrupt_after), tempfile.TemporaryDirectory() as directory:
                helper, state, roots, node, request = self.scene(directory)
                original = {p: (p.stat().st_ino, p.read_bytes()) for folder in (*roots, state)
                            for p in folder.rglob('*') if p.is_file()}
                opened, real_open = [], os.open
                def tracked(path, *args, **kwargs):
                    opened.append(str(path));return real_open(path, *args, **kwargs)
                with helper.root_stats(), patch.object(os, 'open', side_effect=tracked):
                    before = retained.run(request, node)
                    self.assertEqual(before['baseline']['originalMpuSceneSha256'], request['expectedMpuSceneSha256'])
                    self.assertEqual(before['baseline']['archiveLayout'], 'per-run-parent')
                    change = {**request, 'action': 'retained-scene-isolate', 'expectedBaseline': before['baseline']}
                    count = 0
                    def interrupted(source, target):
                        nonlocal count
                        helper.local_rename(source, target)
                        if target.is_dir():
                            count += 1
                            if count == interrupt_after: raise RuntimeError('lost SSH after atomic rename')
                    with patch.object(retained, 'rename_new', side_effect=interrupted), self.assertRaises(RuntimeError):
                        retained.run(change, node)
                    with patch.object(retained, 'rename_new', side_effect=helper.local_rename):
                        result = retained.run(change, node)
                        self.assertFalse(retained.run(change, node)['mutated'])
                    self.assertTrue(result['activeNamespaceIsolated'])
                    self.assertFalse(result['originalSceneRestored'] or result['canonicalPassClaimed'] or result['credentialContentsRead'])
                    self.assertEqual(result['baseline'], before['baseline'])
                    self.assertEqual(result['locations'], ['retained'] * 5)
                    retained_state, retained_roots = retained.destinations(state, roots, per_run_parent=True)
                    for old, expected in original.items():
                        source = next(folder for folder in (*roots, state) if old.is_relative_to(folder))
                        target = retained_state if source == state else retained_roots[roots.index(source)]
                        moved = target / old.relative_to(source)
                        self.assertEqual((moved.stat().st_ino, moved.read_bytes()), expected)
                    self.assertTrue(all(p['uid'] == 0 and p['mode'] == 0o700 for p in result['privateArchiveParents']))
                self.assertFalse(any(p.endswith('service.env') or p.endswith('/credential') for p in opened))
                self.assertTrue(any('/multipart/' in p for p in opened))
                self.assertTrue(any('-db/' in p for p in opened))
                self.assertTrue(any('I03-missing-target.original' in p for p in opened))

    def test_missing_or_changed_archived_business_mpu_scope_and_active_gates_cannot_move(self):
        for kind in ('business-sha', 'mpu-sha', 'keys', 'missing-field', 'db-drift', 'mpu-drift', 'backup-drift', 'gate', 'missing-root'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                helper, state, roots, node, request = self.scene(directory)
                with helper.root_stats():
                    before = retained.run(request, node)
                    change = {**request, 'action': 'retained-scene-isolate', 'expectedBaseline': before['baseline']}
                    if kind == 'business-sha': change['expectedOriginalSceneSha256'] = 'f' * 64
                    elif kind == 'mpu-sha': change['expectedMpuSceneSha256'] = 'f' * 64
                    elif kind == 'keys': change['mpuKeys'] = change['mpuKeys'][:-1]
                    elif kind == 'missing-field': change.pop('expectedMpuSceneSha256')
                    elif kind == 'db-drift': (roots[1] / 'data' / (request['expectedOwner']['bucket'] + '-db') / 'part.1').write_bytes(b'changed')
                    elif kind == 'mpu-drift': next((roots[1] / 'data/.rustfs.sys/multipart').rglob('part.1')).write_bytes(b'changed')
                    elif kind == 'backup-drift': (roots[1] / 'quarantine/I03-missing-target.original').write_bytes(b'changed')
                    elif kind == 'gate': (state / 'network-gate.json').write_text('{}')
                    else: shutil.rmtree(roots[1])
                    with patch.object(retained, 'rename_new') as move, self.assertRaises(ValueError): retained.run(change, node)
                    move.assert_not_called()
                    self.assertTrue(state.exists())

    def test_mpu_content_drift_in_retained_archive_is_not_reclassified_as_clean(self):
        with tempfile.TemporaryDirectory() as directory:
            helper, state, roots, node, request = self.scene(directory)
            with helper.root_stats(), patch.object(retained, 'rename_new', side_effect=helper.local_rename):
                baseline = retained.run(request, node)['baseline']
                retained.run({**request, 'action': 'retained-scene-isolate', 'expectedBaseline': baseline}, node)
                retained_roots = retained.destinations(state, roots, per_run_parent=True)[1]
                next((retained_roots[0] / 'data/.rustfs.sys/multipart').rglob('part.1')).write_bytes(b'drift')
                with self.assertRaisesRegex(ValueError, 'durable isolation intent'): retained.run(request, node)


class NetworkRetentionControllerTests(unittest.TestCase):
    def plan(self, folder):
        helper, state, roots, node, request = NetworkRetentionTests().scene(str(folder))
        owner = request['expectedOwner'];nodes = [f'vm00{i}' for i in range(4)]
        binding = {'runId': request['runId'], 'candidateCommit': '1' * 40, 'harnessCommit': '2' * 40,
            'candidateBinarySha256': owner['binarySha256'], 'candidatePackageSha256': '3' * 64,
            'bucketPrefix': owner['bucket']}
        evidence = folder / 'evidence';evidence.mkdir()
        for i, name in enumerate(nodes):
            (evidence / f'{i:05d}-reserved.json').write_text(json.dumps({'node': name, 'reserved': True,
                'owner': {**copy.deepcopy(owner), 'hostname': 'original-host-' + str(i)}}))
        value = node['snapshot']({'buckets': [owner['bucket'], owner['bucket'] + '-db']})
        mpu = mpu_scene.run({'runId': request['runId'], 'action': 'mpu-scene', 'bucket': owner['bucket'],
            'keys': request['mpuKeys'], 'expectedBinarySha256': owner['binarySha256']}, node)
        fleet = types.SimpleNamespace(run_id=request['runId'], nodes=nodes, evidence=evidence,
            network_gates=set(), quorum_gates=set(), record=lambda label, value: value)
        plan = controller.prepare(fleet, binding, request['mpuKeys'],
            {n: copy.deepcopy(value) for n in nodes}, {n: copy.deepcopy(mpu) for n in nodes})
        return fleet, plan, binding

    def test_all_four_preflights_precede_any_move_and_no_restoration_or_cleanup_is_issued(self):
        for failure in (None, 'preflight-vm003', 'move-vm001', 'post-drift-vm002'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                fleet, plan, binding = self.plan(Path(directory));events, moved = [], set()
                def remote(node, action, **params):
                    self.assertEqual(params['expectedOwner'], plan['owners'][node])
                    self.assertEqual(params['expectedMpuSceneSha256'], plan['mpuSceneSha256'][node])
                    events.append((node, action))
                    if failure == 'preflight-' + node and action.endswith('audit') and not moved:
                        raise ValueError('last peer preflight failed')
                    if action.endswith('isolate'):
                        moved.add(node)
                        if failure == 'move-' + node: raise ValueError('lost SSH after move')
                    return {'baseline': {'original': node if failure != 'post-drift-' + node or len(moved) < 4 else 'drift'},
                        'inspectionOnly': action.endswith('audit'), 'mutated': action.endswith('isolate'),
                        'credentialContentsRead': False, 'activeNamespaceIsolated': node in moved,
                        'originalSceneRestored': False, 'backupRestorationUsed': False, 'canonicalPassClaimed': False,
                        'privateArchiveParents': [{'exists': True, 'uid': 0, 'mode': 0o700}] * 5}
                fleet.remote = remote
                if failure:
                    with self.assertRaises(ValueError): controller.isolate(fleet, plan)
                else:
                    result = controller.isolate(fleet, plan)
                    self.assertEqual(result['cleanup'], 'ISOLATED')
                    self.assertFalse(result['originalSceneRestored'] or result['namespaceDeletionPerformed'] or result['canonicalPassClaimed'])
                self.assertEqual(events[:4], [(n, 'retained-scene-audit') for n in fleet.nodes])
                self.assertTrue(all(action in {'retained-scene-audit', 'retained-scene-isolate'} for n, action in events))
                if failure == 'preflight-vm003': self.assertFalse(moved)

    def test_archived_plan_requires_original_failed_case_binding_and_all_reserved_owners(self):
        for kind in ('valid', 'source', 'helper', 'run', 'result', 'owner', 'other-retained'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory);fleet, plan, binding = self.plan(root)
                case = 'heal.network-partition-and-lock-quorum';base = root / 'artifact/run' / case
                folder = base / 'evidence' / case / 'H2/read-boundary';folder.mkdir(parents=True)
                (base / 'cases').mkdir()
                result = {'case_id': case, 'verdict': 'FAIL', 'cleanup_status': 'FAIL', 'provenance': {
                    'source_commit': binding['candidateCommit'], 'candidate_sha256': binding['candidatePackageSha256'],
                    'binary_sha256': binding['candidateBinarySha256'], 'testing_commit': binding['harnessCommit']}}
                candidate = {'source_sha': binding['candidateCommit'], 'package_sha256': binding['candidatePackageSha256']}
                summary = {'cleanup': 'FAIL', 'results': [{'topology': 'H2', 'branch': 'read-boundary', 'verdict': 'FAIL', 'cleanup': 'FAIL'}]}
                if kind == 'source': candidate['source_sha'] = 'f' * 40
                elif kind == 'helper': binding['harnessCommit'] = 'e' * 40
                elif kind == 'run': plan['runId'] = 'gh-54321-1-h2'
                elif kind == 'result': result['verdict'] = 'PASS'
                elif kind == 'other-retained': summary['results'].append({'branch': 'rejoin', 'cleanup': 'FAIL'})
                (base / 'cases' / (case + '.json')).write_text(json.dumps(result))
                (folder.parent.parent / 'healthy-summary.json').write_text(json.dumps(summary))
                (folder / '00001-network-retention-intent.json').write_text(json.dumps(plan))
                (folder / '00002-binding.json').write_text(json.dumps(binding))
                for p in fleet.evidence.glob('*-reserved.json'): shutil.copy2(p, folder / p.name)
                if kind == 'owner':
                    p = next(folder.glob('*-reserved.json'));row = json.loads(p.read_text());row['owner']['runtimeProfile'] = 'manual';p.write_text(json.dumps(row))
                if kind == 'valid': self.assertEqual(recovery.network_recovery_plan(root, 12345, candidate), [plan])
                else:
                    with self.assertRaises(ValueError): recovery.network_recovery_plan(root, 12345, candidate)

    def test_recovery_never_enters_already_clean_or_deletes_an_isolated_scene(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory);fleet, plan, binding = self.plan(root)
            candidate = root / 'candidate.json';candidate.write_text('{}')
            argv = ['recover', '--failed-run-id', '12345', '--artifact', str(root), '--candidate', str(candidate), '--evidence', str(root / 'out')]
            result = {'cleanup': 'ISOLATED', 'originalSceneRestored': False, 'canonicalPassClaimed': False}
            with patch('sys.argv', argv), patch.object(recovery, 'recovery_plan', return_value=[plan]), \
                 patch.object(recovery, 'isolate_network_scene', return_value=result) as isolate, \
                 patch.object(recovery.Fleet, 'remote', side_effect=AssertionError('ordinary cleanup/namespace shortcut')):
                recovery.main()
            isolate.assert_called_once()
            output = json.loads((root / 'out/recovery-summary.json').read_text())
            self.assertEqual(output['cleanup'], 'ISOLATED')
            self.assertFalse(output['canonicalPassClaimed'])


if __name__ == '__main__': unittest.main()
