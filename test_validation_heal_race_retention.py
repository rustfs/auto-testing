"""Stopped G01 isolation preserves ACK uncertainty, bytes and credentials."""

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_heal import race_retention as controller, retained_scene as retained, recover_environment as recovery
import test_validation_heal_retained_scene as retained_tests


class RaceRetentionTests(unittest.TestCase):
    def scene(self, directory):
        helper = retained_tests.RetainedSceneTests()
        state, roots, node, request = helper.scene(directory)
        shutil.copytree(roots[1], roots[0])
        owner = request['expectedOwner']; owner['runtimeProfile'] = 'manual'
        for folder in (*roots, state): (folder / 'owner.json').write_text(json.dumps(owner))
        for root in roots:
            for suffix in ('original', 'intent.json', 'applied.json'):
                (root / 'quarantine' / ('G01-object-0256.' + suffix)).write_bytes(('old shard ' + suffix).encode())
        def snapshot(value):
            self.assertEqual(value, {'runId': request['runId'], 'action': 'snapshot', 'quiescent': True})
            files = []
            for disk, root in enumerate(roots):
                for path in sorted((root / 'data' / owner['bucket']).rglob('*')):
                    if path.is_file(): files.append({'disk': disk, 'path': path.relative_to(root / 'data').as_posix(),
                        'size': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            return {'hostname': owner['hostname'], 'quiescent': True, 'pid': 0, 'unit': owner['unit'],
                    'binarySha256': owner['binarySha256'], 'files': files,
                    'formats': [{'disk': disk, 'original': True} for disk in range(4)]}
        node['snapshot'] = snapshot
        scene = snapshot({'runId': request['runId'], 'action': 'snapshot', 'quiescent': True})
        request['expectedOriginalSceneSha256'] = retained.sha({key: scene[key] for key in ('files', 'formats')})
        return helper, state, roots, node, request, scene

    def test_all_five_interrupted_moves_preserve_real_bytes_inodes_and_unread_credentials(self):
        for interrupt_after in range(1, 6):
            with self.subTest(interrupt_after=interrupt_after), tempfile.TemporaryDirectory() as directory:
                helper, state, roots, node, request, _ = self.scene(directory)
                original = {path: (path.stat().st_ino, path.read_bytes()) for folder in (*roots, state)
                            for path in folder.rglob('*') if path.is_file()}
                opened, real_open = [], os.open
                def tracked(path, *args, **kwargs):
                    opened.append(str(path)); return real_open(path, *args, **kwargs)
                with helper.root_stats(), patch.object(os, 'open', side_effect=tracked):
                    before = retained.run(request, node)
                    change = {**request, 'action': 'retained-scene-isolate', 'expectedBaseline': before['baseline']}
                    count = 0
                    def interrupted(source, target):
                        nonlocal count
                        helper.local_rename(source, target)
                        if target.is_dir():
                            count += 1
                            if count == interrupt_after: raise RuntimeError('SSH response lost')
                    with patch.object(retained, 'rename_new', side_effect=interrupted), self.assertRaises(RuntimeError):
                        retained.run(change, node)
                    with patch.object(retained, 'rename_new', side_effect=helper.local_rename):
                        proof = retained.run(change, node)
                        self.assertFalse(retained.run(change, node)['mutated'])
                    self.assertEqual(proof['locations'], ['retained'] * 5)
                    self.assertEqual(proof['baseline'], before['baseline'])
                    self.assertFalse(proof['originalSceneRestored'] or proof['backupRestorationUsed'] or proof['canonicalPassClaimed'])
                    target_state, target_roots = retained.destinations(state, roots, per_run_parent=True)
                    for path, expected in original.items():
                        source = next(folder for folder in (*roots, state) if path.is_relative_to(folder))
                        target = target_state if source == state else target_roots[roots.index(source)]
                        moved = target / path.relative_to(source)
                        self.assertEqual((moved.stat().st_ino, moved.read_bytes()), expected)
                    self.assertTrue(all(row['uid'] == 0 and row['mode'] == 0o700 for row in proof['privateArchiveParents']))
                self.assertTrue(any(path.endswith('G01-object-0256.original') for path in opened))
                self.assertFalse(any(path.endswith('service.env') or path.endswith('/credential') for path in opened))

    def test_changed_business_backup_scope_gate_and_missing_root_refuse_before_move(self):
        for kind in ('scene-sha', 'missing-field', 'business-drift', 'backup-drift', 'gate', 'missing-root'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                helper, state, roots, node, request, _ = self.scene(directory)
                with helper.root_stats():
                    baseline = retained.run(request, node)['baseline']
                    change = {**request, 'action': 'retained-scene-isolate', 'expectedBaseline': baseline}
                    if kind == 'scene-sha': change['expectedOriginalSceneSha256'] = '0' * 64
                    elif kind == 'missing-field': del change['expectedOriginalSceneSha256']
                    elif kind == 'business-drift': (roots[0] / 'data' / request['expectedOwner']['bucket'] / 'part.1').write_bytes(b'new ACK')
                    elif kind == 'backup-drift': (roots[0] / 'quarantine/G01-object-0256.original').write_bytes(b'changed backup')
                    elif kind == 'gate': (state / 'drive-gate.json').write_text('{}')
                    else: shutil.rmtree(roots[0])
                    with patch.object(retained, 'rename_new') as move, self.assertRaises(ValueError): retained.run(change, node)
                    move.assert_not_called()

    def archive(self, directory):
        source = Path(directory) / 'source'; source.mkdir()
        _, _, _, _, request, scene = self.scene(source)
        owner = request['expectedOwner']; run = Path(directory) / 'artifact/rustfs-validation-12345-1/run' / controller.CASE
        evidence = run / 'evidence' / controller.CASE; folder = evidence / 'H2/overwrite-vs-deep'
        folder.mkdir(parents=True); (run / 'cases').mkdir()
        nodes = [f'vm00{i}' for i in range(4)]
        binding = {'runId': request['runId'], 'candidateCommit': '1' * 40, 'harnessCommit': '2' * 40,
                   'candidateBinarySha256': owner['binarySha256'], 'candidatePackageSha256': '3' * 64, 'bucketPrefix': owner['bucket']}
        candidate = {'source_sha': binding['candidateCommit'], 'package_sha256': binding['candidatePackageSha256']}
        def write(path, value): path.write_text(json.dumps(value))
        write(run / 'cases' / (controller.CASE + '.json'), {'case_id': controller.CASE, 'verdict': 'FAIL',
            'cleanup_status': 'FAIL', 'provenance': {'source_commit': binding['candidateCommit'],
                'candidate_sha256': binding['candidatePackageSha256'], 'binary_sha256': owner['binarySha256'],
                'testing_commit': binding['harnessCommit']}})
        write(evidence / 'healthy-summary.json', {'caseId': controller.CASE, 'cleanup': 'FAIL', 'results': [
            {'caseId': controller.CASE, 'cleanup': 'FAIL', 'verdict': 'FAIL', 'topology': 'H2', 'branch': 'overwrite-vs-deep'}]})
        write(folder / '00001-binding.json', binding)
        failure = {}
        for i, name in enumerate(nodes):
            host = 'original-host-' + str(i)
            write(folder / f'{i:05d}-reserved.json', {'node': name, 'reserved': True, 'owner': {**owner, 'hostname': host}})
            failure[name] = {**copy.deepcopy(scene), 'hostname': host}
        write(folder / '00005-retained-failure-scene.json', failure)
        write(folder / '00006-last-ACK-rescue-required.json', {'state': 'DIRTY', 'namespacesRetained': True,
            'lastACKPhysicalAuthorityVerified': False, 'originalGenerationBlindRestorationProhibited': True})
        for name in ('seed-ACKs', 'foreground-ACKs'):
            journal = folder / name; journal.mkdir()
            write(journal / '000001-request.json', {'runId': binding['runId'], 'operation': 'put_object'})
            # Unknown/missing results are retained, never reconstructed or retried.
        root = Path(directory) / 'artifact'
        return root, candidate, folder

    def test_archived_unknown_or_explicit_refusal_can_only_select_isolation_with_exact_identity(self):
        for kind in ('valid', 'unknown', 'refusal', 'candidate', 'binary', 'host', 'live', 'mpu', 'omitted-owner', 'mixed-case', 'extra-failure'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root, candidate, folder = self.archive(directory)
                if kind in ('unknown', 'refusal'):
                    (folder / 'foreground-ACKs/000001-ack.json').write_text(json.dumps({'runId': 'gh-12345-1-h2',
                        'observation': {'operation': 'put_object', 'acknowledged': False, 'resultUnknown': kind == 'unknown',
                                        'response': {'ResponseMetadata': {'HTTPStatusCode': 503}}}}))
                elif kind == 'candidate': candidate['source_sha'] = '4' * 40
                elif kind in ('binary', 'host', 'live'):
                    path = folder / '00005-retained-failure-scene.json'; value = json.loads(path.read_text())
                    value['vm003'][{'binary': 'binarySha256', 'host': 'hostname', 'live': 'pid'}[kind]] = 1 if kind == 'live' else 'foreign'
                    path.write_text(json.dumps(value))
                elif kind == 'mpu':
                    (folder / 'foreground-ACKs/000001-request.json').write_text(json.dumps({'runId': 'gh-12345-1-h2', 'operation': 'complete_multipart_upload'}))
                elif kind == 'omitted-owner': (folder / '00003-reserved.json').unlink()
                elif kind == 'mixed-case':
                    path = next(root.glob('*/run/*/cases')).parent / 'cases/another-case.json'; path.write_text('{}')
                elif kind == 'extra-failure':
                    path = folder.parent.parent / 'healthy-summary.json'; value = json.loads(path.read_text()); value['results'] *= 2
                    path.write_text(json.dumps(value))
                if kind in ('valid', 'unknown', 'refusal'):
                    plan = recovery.recovery_plan(root, 12345, candidate)[0]
                    self.assertEqual(plan['kind'], 'unresolved-g01-race'); self.assertEqual(plan['nodes'], [f'vm00{i}' for i in range(4)])
                else:
                    with self.assertRaises(ValueError): recovery.recovery_plan(root, 12345, candidate)

    def test_all_peers_preflight_before_any_move_and_failed_checks_never_issue_cleanup_or_restart(self):
        for failure in (None, 'preflight-vm003', 'isolate-vm001', 'post-vm002'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root, candidate, _ = self.archive(directory); plan = recovery.recovery_plan(root, 12345, candidate)[0]
                calls, audits = [], {}
                def remote(node, action, **params):
                    calls.append((node, action))
                    if action == 'logs': return {'retained': True}
                    if action == 'retained-scene-isolate':
                        self.assertEqual(len(audits), 4)
                        if failure == 'isolate-' + node: raise RuntimeError('SSH response lost')
                        return {'baseline': audits[node], 'activeNamespaceIsolated': True, 'originalSceneRestored': False,
                                'backupRestorationUsed': False, 'canonicalPassClaimed': False, 'credentialContentsRead': False}
                    self.assertEqual(action, 'retained-scene-audit')
                    post = node in audits
                    if failure == ('post-' if post else 'preflight-') + node: raise RuntimeError('drift')
                    audits[node] = {'node': node}
                    return {'baseline': audits[node], 'inspectionOnly': True, 'mutated': False, 'credentialContentsRead': False,
                            'activeNamespaceIsolated': post, 'privateArchiveParents': [{'exists': True, 'uid': 0, 'mode': 0o700}] * 5}
                fleet = SimpleNamespace(run_id=plan['runId'], nodes=plan['nodes'], remote=remote, record=lambda label, value: value)
                if failure is None:
                    proof = controller.isolate(fleet, plan); self.assertEqual(proof['cleanup'], 'ISOLATED')
                    self.assertFalse(proof['namespaceDeletionPerformed'] or proof['backupRestorationUsed'] or proof['canonicalPassClaimed'])
                else:
                    with self.assertRaises(RuntimeError): controller.isolate(fleet, plan)
                self.assertTrue(all(action in {'retained-scene-audit', 'retained-scene-isolate', 'logs'} for _, action in calls))
                if failure == 'preflight-vm003': self.assertFalse(any(action == 'retained-scene-isolate' for _, action in calls))

    def test_recovery_routes_isolated_scene_before_absent_namespace_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, _ = self.archive(directory)
            plan = recovery.recovery_plan(root, 12345, candidate)[0]
            candidate_path = Path(directory) / 'candidate.json'; candidate_path.write_text(json.dumps(candidate))
            evidence = Path(directory) / 'recovery'
            fleet = SimpleNamespace(nodes=plan['nodes'], run_id=plan['runId'], record=lambda label, value: value)
            proof = {'runId': plan['runId'], 'cleanup': 'ISOLATED', 'canonicalPassClaimed': False}
            with patch('sys.argv', ['recover', '--failed-run-id', '12345', '--artifact', str(root),
                '--candidate', str(candidate_path), '--evidence', str(evidence)]), \
                    patch.object(recovery, 'Fleet', return_value=fleet), \
                    patch.object(recovery, 'isolate_race_scene', return_value=proof) as isolate:
                recovery.main()
            isolate.assert_called_once_with(fleet, plan)
            summary = json.loads((evidence / 'recovery-summary.json').read_text())
            self.assertIn('ISOLATED', json.dumps(summary))

    def test_version_append_failure_keeps_unknown_put_and_complete_historical_journal_for_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, folder = self.archive(directory)
            original = next(root.glob('*/run/*'))
            case = 'heal.race-versioned-put'
            run = original.with_name(case); original.rename(run)
            old_case = run / 'cases' / (controller.CASE + '.json')
            value = json.loads(old_case.read_text()); value['case_id'] = case
            old_case.rename(old_case.with_name(case + '.json'))
            (run / 'cases' / (case + '.json')).write_text(json.dumps(value))
            evidence = run / 'evidence' / controller.CASE
            evidence.rename(evidence.with_name(case)); evidence = evidence.with_name(case)
            summary = json.loads((evidence / 'healthy-summary.json').read_text()); summary['caseId'] = case
            summary['results'][0].update(caseId=case, branch='read-all-versions-during-heal')
            (evidence / 'healthy-summary.json').write_text(json.dumps(summary))
            branch = evidence / 'H2/overwrite-vs-deep'; branch.rename(branch.with_name('read-all-versions-during-heal'))
            branch = branch.with_name('read-all-versions-during-heal')
            reads = branch / 'historical-read-ACKs'; reads.mkdir()
            path = reads / '000001-request.json'
            path.write_text(json.dumps({'runId': 'gh-12345-1-h2', 'operation': 'get_object'}))
            plan = recovery.recovery_plan(root, 12345, candidate)[0]
            self.assertEqual(plan['kind'], 'unresolved-g02-race')
            before = plan['ackFilesSha256']
            path.write_text(json.dumps({'runId': 'gh-12345-1-h2', 'operation': 'get_object', 'identity': {'uncertain': True}}))
            self.assertNotEqual(recovery.recovery_plan(root, 12345, candidate)[0]['ackFilesSha256'], before)
            path.write_text(json.dumps({'runId': 'gh-12345-1-h2', 'operation': 'abort_multipart_upload'}))
            with self.assertRaises(ValueError): recovery.recovery_plan(root, 12345, candidate)

    def test_a09_source_summary_without_public_case_field_keeps_uncertain_delete_for_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            root, candidate, folder = self.archive(directory)
            original = next(root.glob('*/run/*'))
            case = 'heal.admin-nolock-safety'
            run = original.with_name(case); original.rename(run)
            old_case = run / 'cases' / (controller.CASE + '.json')
            value = json.loads(old_case.read_text()); value['case_id'] = case
            old_case.rename(old_case.with_name(case + '.json'))
            (run / 'cases' / (case + '.json')).write_text(json.dumps(value))
            evidence = run / 'evidence' / controller.CASE
            evidence.rename(evidence.with_name(case)); evidence = evidence.with_name(case)
            summary = json.loads((evidence / 'healthy-summary.json').read_text()); summary['caseId'] = case
            summary['results'][0].update(branch='delete-nolock'); del summary['results'][0]['caseId']
            (evidence / 'healthy-summary.json').write_text(json.dumps(summary))
            branch = evidence / 'H2/overwrite-vs-deep'; branch.rename(branch.with_name('delete-nolock'))
            branch = branch.with_name('delete-nolock')
            path = branch / 'foreground-ACKs/000001-request.json'
            path.write_text(json.dumps({'runId': 'gh-12345-1-h2', 'operation': 'delete_object'}))
            plan = recovery.recovery_plan(root, 12345, candidate)[0]
            self.assertEqual(plan['kind'], 'unresolved-a09-race')
            before = plan['ackFilesSha256']
            path.write_text(json.dumps({'runId': 'gh-12345-1-h2', 'operation': 'delete_object', 'identity': {'uncertain': True}}))
            self.assertNotEqual(recovery.recovery_plan(root, 12345, candidate)[0]['ackFilesSha256'], before)
            summary['results'][0]['caseId'] = 'foreign-case'
            (evidence / 'healthy-summary.json').write_text(json.dumps(summary))
            with self.assertRaises(ValueError): recovery.recovery_plan(root, 12345, candidate)
            del summary['results'][0]['caseId']
            (evidence / 'healthy-summary.json').write_text(json.dumps(summary))
            path.write_text(json.dumps({'runId': 'gh-12345-1-h2', 'operation': 'abort_multipart_upload'}))
            with self.assertRaises(ValueError): recovery.recovery_plan(root, 12345, candidate)

    def test_g02_original_quarantine_contents_are_hashed_without_reading_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            helper, state, roots, node, request, _ = self.scene(directory)
            for root in roots:
                for path in (root / 'quarantine').glob('G01-*'):
                    path.rename(path.with_name(path.name.replace('G01-', 'G02-', 1)))
            opened, original_open = [], os.open
            def tracked(path, *args, **kwargs):
                opened.append(str(path)); return original_open(path, *args, **kwargs)
            with helper.root_stats(), patch.object(os, 'open', side_effect=tracked):
                result = retained.run(request, node)
            self.assertEqual(sum(path.endswith('G02-object-0256.original') for path in opened), 4)
            self.assertFalse(any(path.endswith('service.env') or path.endswith('/credential') for path in opened))
            self.assertFalse(result['canonicalPassClaimed'] or result['backupRestorationUsed'])

    def test_a09_original_quarantine_contents_are_hashed_without_reading_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            helper, state, roots, node, request, _ = self.scene(directory)
            for root in roots:
                for path in (root / 'quarantine').glob('G01-*'):
                    path.rename(path.with_name(path.name.replace('G01-', 'A09-', 1)))
            opened, original_open = [], os.open
            def tracked(path, *args, **kwargs):
                opened.append(str(path)); return original_open(path, *args, **kwargs)
            with helper.root_stats(), patch.object(os, 'open', side_effect=tracked):
                result = retained.run(request, node)
            self.assertEqual(sum(path.endswith('A09-object-0256.original') for path in opened), 4)
            self.assertFalse(any(path.endswith('service.env') or path.endswith('/credential') for path in opened))
            self.assertFalse(result['canonicalPassClaimed'] or result['backupRestorationUsed'])


if __name__ == '__main__':
    unittest.main()
