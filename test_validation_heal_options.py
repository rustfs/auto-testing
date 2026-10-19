import copy
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import options
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES
from scripts.validation_heal.healthy import Fleet
import test_validation_heal_scopes as scope_fixtures


class OptionsTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def mapped(self):
        rows = [(options.KEYS[0], 'null', 0, False), (options.KEYS[0], 'historic', 0, False),
                (options.KEYS[1], 'child', 1, False), (options.KEYS[2], 'control', 1, False),
                (options.KEYS[-1], 'marker', 0, True)]
        return {(self.bucket, key, version): {'poolIndex': 0, 'setIndex': number, 'deleteMarker': marker,
                                            'isLatest': version != 'historic'}
                for key, version, number, marker in rows}

    def status(self, branch='selector-valid'):
        _, request = options.paired_options(branch)
        mapped = self.mapped()
        selected = options.selection(branch, request, mapped)
        repaired = {identity for identity in selected if identity[2] == 'historic'}
        receipts = []
        for identity in sorted(selected):
            state = 'repaired' if identity in repaired else 'metadata_healthy' if mapped[identity]['deleteMarker'] else 'verified_healthy'
            receipts.append({'identity': {'kind': 'object', 'bucket': identity[0], 'object': identity[1],
                'versionId': identity[2], 'poolIndex': request['pool'], 'setIndex': request['set'],
                'bucketIncarnationId': 'current-incarnation'}, 'disposition': {'state': state}})
        status = {'summary': 'finished', 'settings': request, 'outcome': {'execution': {'state': 'completed'},
            'coverage': 'complete', 'objectsTruncated': False, 'objects': receipts,
            'counters': {'processed': len(selected), 'healed': len(repaired), 'unchanged': len(selected) - len(repaired),
                         'failed': 0, 'unknown': 0, 'skipped': 0, 'attemptFailures': 0, 'overflowed': False}}}
        return status, mapped, selected, repaired, request

    def test_all_source_options_topologies_and_two_fixture_types_are_preserved(self):
        self.assertEqual(select(options.CASE_ID)[0]['variants'], list(options.BRANCHES))
        self.assertIn(options.CASE_ID, AUTOMATIC_CASES)
        self.assertEqual(options.FIXTURES, ('DB', 'DV'))
        self.assertEqual(options.REFERENCE['support']['readRepair'],
                         'unsupported for Admin; HTTP 400 InvalidArgument before execution')

    def test_request_pairs_do_not_change_dangerous_or_unrelated_defaults(self):
        for branch in options.BRANCHES:
            baseline, selected = options.paired_options(branch)
            self.assertFalse(baseline['remove'])
            self.assertEqual(selected['remove'], branch == 'remove-true')
            self.assertFalse(baseline['nolock'])
            self.assertFalse(selected['nolock'])
            self.assertEqual(baseline['scanMode'], selected['scanMode'])
            changed = {name for name in baseline if baseline[name] != selected[name]}
            self.assertEqual(len(changed), 2 if branch.startswith('selector-') else 0 if branch == 'remove-false' else 1)

    def test_remove_true_preserves_the_frozen_recursive_bucket_source_procedure(self):
        baseline, selected = options.paired_options('remove-true')
        self.assertTrue(baseline['recursive'])
        self.assertTrue(selected['recursive'])
        self.assertEqual(options.request_path('remove-true', self.bucket),
                         '/rustfs/admin/v3/heal/' + self.bucket)
        self.assertEqual(options.source_phases('remove-true'), ('selected',))
        self.assertEqual(options.source_phases('remove-false'), ('baseline', 'selected'))
        with self.assertRaisesRegex(ValueError, 'source procedure'):
            options.run_instance(None, 'H1', ['node'], 'remove-true', 'DV', 'baseline')

    def orphan_scene(self):
        import uuid
        directory = str(uuid.UUID(int=11)); version = str(uuid.UUID(int=12))
        authority = {'node': {'formats': [{'disk': disk, 'sha256': str(disk)} for disk in range(4)],
            'files': [{'disk': disk, 'path': self.bucket + '/control.bin/xl.meta', 'size': 7, 'sha256': 'control'}
                      for disk in range(4)]}}
        original = copy.deepcopy(authority)
        for disk in range(4):
            for name in ('xl.meta', directory + '/part.1', directory + '/part.1.integrity'):
                original['node']['files'].append({'disk': disk, 'path': self.bucket + '/' + options.orphan.KEY + '/' + name,
                                                'size': 100, 'sha256': str(disk) + name})
        before = copy.deepcopy(authority)
        before['node']['files'] += [copy.deepcopy(r) for r in original['node']['files']
                                    if r['disk'] == 0 and '/' + options.orphan.KEY + '/' in r['path']]
        target = {'node': 'node', 'disk': 0, 'dataDir': directory, 'versionId': version,
                  'logical': {'erasure': {'data_blocks': 2, 'parity_blocks': 2}}}
        return original, before, authority, target

    def test_full_original_orphan_tree_and_one_below_quorum_member_are_required(self):
        original, before, authority, target = self.orphan_scene()
        options.validate_orphan_replay(original, before, authority, self.bucket, target)
        for kind in ('data-only', 'sidecar', 'second-member', 'control', 'format', 'original-format', 'original-control', 'not-below-quorum'):
            scene, changed, source = copy.deepcopy(before), copy.deepcopy(target), copy.deepcopy(original)
            if kind == 'data-only': scene['node']['files'] = [r for r in scene['node']['files']
                if not r['path'].endswith(options.orphan.KEY + '/xl.meta')]
            elif kind == 'sidecar': scene['node']['files'] = [r for r in scene['node']['files']
                if not r['path'].endswith('part.1.integrity')]
            elif kind == 'second-member':
                row = copy.deepcopy(scene['node']['files'][-1]); row['disk'] = 1; scene['node']['files'].append(row)
            elif kind == 'control': scene['node']['files'][0]['sha256'] = 'changed'
            elif kind == 'format': scene['node']['formats'][0]['sha256'] = 'changed'
            elif kind == 'original-format': source['node']['formats'][0]['sha256'] = 'changed'
            elif kind == 'original-control': source['node']['files'][0]['sha256'] = 'changed'
            else: changed['logical']['erasure']['data_blocks'] = 1
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                options.validate_orphan_replay(source, scene, authority, self.bucket, changed)

    def test_source_orphan_replay_uses_actual_h2_sixteen_member_read_quorum(self):
        original, before, authority, target = self.orphan_scene()
        source = {}; injected = {}; deleted = {}
        for index in range(4):
            name = 'node' + str(index)
            source[name] = copy.deepcopy(original['node'])
            deleted[name] = copy.deepcopy(authority['node'])
            injected[name] = copy.deepcopy(before['node'] if index == 0 else authority['node'])
        target['node'] = 'node0'
        target['logical']['erasure'] = {'data_blocks': 8, 'parity_blocks': 8}
        result = options.validate_orphan_replay(source, injected, deleted, self.bucket, target)
        self.assertEqual(result['replayedMembers'], 1)
        self.assertEqual(result['independentlyDecodedReadQuorum'], 8)
        injected['node1'] = copy.deepcopy(before['node'])
        with self.assertRaises(ValueError): options.validate_orphan_replay(source, injected, deleted, self.bucket, target)

    def test_single_deleted_orphan_generation_keeps_all_dv_controls_and_marker_acks(self):
        calls = []
        def call(op, params, body=None):
            calls.append((op, dict(params)))
            return {'VersionId': str(__import__('uuid').UUID(int=len(calls))),
                    **({'DeleteMarker': True} if op == 'delete_object' else {})}
        recorder = types.SimpleNamespace(call=call, pending=None)
        keys = (options.orphan.KEY, 'control.bin', 'marker-control.bin')
        manifest = options.seed_fixture(recorder, self.bucket, 'DV', keys,
                                        single_version_key=options.orphan.KEY)
        orphan_rows = [r for r in manifest if r['key'] == options.orphan.KEY]
        self.assertEqual(len(orphan_rows), 1)
        self.assertTrue(orphan_rows[0]['isLatest'])
        self.assertFalse(orphan_rows[0]['deleteMarker'])
        self.assertEqual(len([r for r in manifest if r['key'] == 'control.bin']), 3)
        self.assertEqual(len([r for r in manifest if r['key'] == 'marker-control.bin']), 4)
        self.assertEqual(sum(r['deleteMarker'] for r in manifest), 1)

    def test_source_orphan_execution_removes_whole_tree_before_reads_and_rescues_only_delete_authority(self):
        for state, failure in (('repaired', None), ('authoritatively_absent', None),
                               ('repaired', 'leftover-metadata'), ('repaired', 'lost-install-ack'),
                               ('repaired', 'failed-authority-restore')):
            original, before, authority, target = self.orphan_scene()
            _, request = options.paired_options('remove-true')
            version = target['versionId']; control_version = str(__import__('uuid').UUID(int=13))
            manifest = [{'key': options.orphan.KEY, 'versionId': version, 'isLatest': True, 'deleteMarker': False},
                        {'key': 'control.bin', 'versionId': control_version, 'isLatest': True, 'deleteMarker': False}]
            mapped = {(self.bucket, r['key'], r['versionId']): {
                **(target if r['key'] == options.orphan.KEY else {}), 'poolIndex': 0, 'setIndex': 0,
                'deleteMarker': False, 'isLatest': True} for r in manifest}
            status = {'summary': 'finished', 'settings': request, 'outcome': {
                'execution': {'state': 'completed'}, 'coverage': 'complete', 'objectsTruncated': False,
                'counters': {'processed': 2, 'healed': int(state == 'repaired'),
                    'unchanged': 2 - int(state == 'repaired'), 'failed': 0, 'unknown': 0,
                    'skipped': 0, 'attemptFailures': 0, 'overflowed': False},
                'objects': [{'identity': {'kind': 'object', 'bucket': self.bucket, 'object': r['key'],
                    'versionId': r['versionId'], 'poolIndex': None, 'setIndex': None,
                    'bucketIncarnationId': 'one-incarnation'}, 'disposition': {
                    'state': state if r['key'] == options.orphan.KEY else 'verified_healthy'}} for r in manifest]}}
            events = []
            after = copy.deepcopy(authority)
            if failure == 'leftover-metadata':
                after['node']['files'].append(copy.deepcopy(next(r for r in before['node']['files']
                    if r['path'].endswith(options.orphan.KEY + '/xl.meta'))))
            def snapshot(label, **kw):
                events.append(label)
                if label == 'A10-orphan-original-all-members': return original
                if label in ('A10-orphan-fault-before-GET', 'A10-orphan-immediately-before-START'): return before
                if label == 'A10-orphan-product-before-GET-or-rescue': return after
                if label == 'A10-orphan-last-DELETE-restored' and failure == 'failed-authority-restore': return before
                return authority
            def remote(node, action, **params):
                events.append((action, params))
                self.assertEqual(action, 'orphan-install')
                self.assertEqual(params['mode'], 'object')
                if failure == 'lost-install-ack': raise RuntimeError('lost original install ACK')
                return {'state': 'APPLIED', 'mode': 'object'}
            fleet = types.SimpleNamespace(record=lambda label, value: value, snapshot=snapshot, remote=remote,
                stop=lambda: None, start=lambda: None, orphan_faults=[])
            recorder = types.SimpleNamespace(pending=None, call=unittest.mock.Mock(return_value={
                'VersionId': version, 'ResponseMetadata': {'HTTPStatusCode': 204}}))
            probe = types.SimpleNamespace(inventory=lambda *a, **k: {})
            peer = types.SimpleNamespace(s3=unittest.mock.Mock(return_value={
                'acknowledged': False, 'resultUnknown': False, 'response': {
                    'ResponseMetadata': {'HTTPStatusCode': 404}, 'Error': {'Code': 'NoSuchVersion'}}}))
            with self.subTest(state=state, failure=failure), ExitStack() as stack:
                for name, value in [('seed_fixture', lambda *a, **k: copy.deepcopy(manifest)),
                                    ('verify_seed', lambda *a: None), ('verify_parts', lambda *a: {}),
                                    ('verify_public', lambda *a: None), ('idle_fleet', lambda *a: None),
                                    ('Probe', lambda *a: peer)]: stack.enter_context(patch.object(options, name, value))
                stack.enter_context(patch.object(options, 'map_fixture', side_effect=[mapped,
                    {k: v for k, v in mapped.items() if k[1] != options.orphan.KEY}]))
                task = stack.enter_context(patch.object(options, 'run_request', return_value=status))
                captured = stack.enter_context(patch.object(options.orphan, 'capture'))
                armed = stack.enter_context(patch.object(options.orphan, 'arm_all'))
                recovered = stack.enter_context(patch.object(options.orphan, 'recover_all'))
                if failure:
                    with self.assertRaises((ValueError, RuntimeError)):
                        options.orphan_probe(fleet, probe, {'endpoints': ['peer']}, recorder,
                                             self.bucket, 'H1', 'DV', request, [])
                else:
                    result = options.orphan_probe(fleet, probe, {'endpoints': ['peer']}, recorder,
                                                  self.bucket, 'H1', 'DV', request, [])
                    self.assertTrue(result['physical']['wholeOrphanObjectRemoved'])
                    self.assertFalse(result['rescueIsProductRepair'])
                captured.assert_called_once_with(fleet, original, self.bucket, 'old')
                armed.assert_called_once_with(fleet, authority, self.bucket, 'authority')
                recovered.assert_called_once_with(fleet)
                self.assertEqual(fleet.a10_orphan_restored, failure != 'failed-authority-restore')
                if failure in ('leftover-metadata', 'lost-install-ack'): peer.s3.assert_not_called()
                if failure == 'lost-install-ack': task.assert_not_called()
                else: self.assertEqual(task.call_args.args[2], '/rustfs/admin/v3/heal/' + self.bucket)
                self.assertEqual(recorder.call.call_args.args, ('delete_object', {
                    'Bucket': self.bucket, 'Key': options.orphan.KEY, 'VersionId': version}))

    def test_remove_false_uses_original_repairable_scope_and_only_source_remove_true_uses_orphan_runtime(self):
        for branch, phase in (('remove-false', 'baseline'), ('remove-false', 'selected'), ('remove-true', 'selected')):
            with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                root = Path(directory); candidate = root / 'candidate.json'; rc = root / 'rc'; rc.write_bytes(b'runner')
                candidate.write_text(json.dumps({'source_sha': options.REFERENCE['candidateCommit'], 'package_sha256': 'a' * 64}))
                args = types.SimpleNamespace(evidence=root, candidate=candidate, rc_bin=rc, binary_sha='b' * 64,
                                             tool_directory=root, testing_commit='c' * 40)
                fleet = types.SimpleNamespace(record=lambda label, value: value, deploy=lambda *a: None, cleanup=lambda: True)
                factory = stack.enter_context(patch.object(options, 'Fleet', return_value=fleet))
                stack.enter_context(patch.object(options, 'verify_inspector', return_value={}))
                stack.enter_context(patch.object(options, 'Probe', return_value=object()))
                stack.enter_context(patch.object(options, 'FixtureRecorder', return_value=types.SimpleNamespace(pending=None)))
                stack.enter_context(patch.dict(options.os.environ, {'GITHUB_RUN_ID': '12345', 'GITHUB_RUN_ATTEMPT': '1'}))
                scope = stack.enter_context(patch.object(options, 'scope_probe', return_value={}))
                orphan = stack.enter_context(patch.object(options, 'orphan_probe', return_value={}))
                result = options.run_instance(args, 'H1', ['node'], branch, 'DB', phase)
                self.assertEqual(result['verdict'], 'PASS')
                self.assertEqual(factory.call_args.kwargs['runtime_profile'],
                                 'orphan-preserve' if branch == 'remove-true' else 'manual')
                self.assertEqual(orphan.call_count, int(branch == 'remove-true'))
                self.assertEqual(scope.call_count, int(branch == 'remove-false'))

    def test_prefix_exact_and_recursive_scopes_do_not_include_adjacent_control(self):
        mapped = self.mapped()
        baseline, selected = options.paired_options('recursive-true')
        self.assertEqual(options.selection('recursive-true', baseline, mapped), set())
        identities = options.selection('recursive-true', selected, mapped)
        self.assertEqual({identity[1] for identity in identities}, {options.KEYS[0], options.KEYS[1]})
        self.assertEqual(len(identities), 3)
        self.assertNotIn((self.bucket, options.KEYS[2], 'control'), identities)

    def test_selector_uses_measured_sets_and_retains_historical_and_marker_identities(self):
        status, mapped, selected, repaired, request = self.status()
        proof = options.validate_outcome(status, mapped, selected, repaired, request)
        self.assertEqual(proof['selectedACKs'], 3)
        self.assertEqual(proof['repairedACKs'], 1)
        self.assertIn((self.bucket, options.KEYS[-1], 'marker'), selected)

    def test_exact_object_request_selects_only_the_acknowledged_current_version(self):
        mapped = self.mapped()
        for branch in ('recreate', 'update-parity', 'read-repair'):
            for request in options.paired_options(branch):
                with self.subTest(branch=branch, request=request):
                    selected = options.selection(branch, request, mapped)
                    self.assertEqual(selected, {(self.bucket, options.KEYS[0], 'null')})
        self.assertIn((self.bucket, options.KEYS[0], 'historic'),
                      options.selection('recursive-true', {'recursive': True}, mapped))

    def test_exact_object_scope_rejects_missing_or_ambiguous_current_ack(self):
        for kind in ('missing', 'duplicate', 'untyped'):
            mapped = self.mapped()
            if kind == 'missing': mapped[(self.bucket, options.KEYS[0], 'null')]['isLatest'] = False
            elif kind == 'duplicate': mapped[(self.bucket, options.KEYS[0], 'historic')]['isLatest'] = True
            else: mapped[(self.bucket, options.KEYS[0], 'null')]['isLatest'] = 1
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, 'current ACK'):
                options.selection('recreate', options.paired_options('recreate')[0], mapped)

    def test_exact_object_fault_targets_current_ack_before_remote_mutation(self):
        mapped = self.mapped()
        mapped.pop((self.bucket, options.KEYS[-1], 'marker'))
        mapped[(self.bucket, options.KEYS[3], 'other')] = {
            'poolIndex': 0, 'setIndex': 0, 'deleteMarker': False, 'isLatest': True}
        # A historical generation appears first, as it does in the real PUT ledger.
        current = (self.bucket, options.KEYS[0], 'null')
        historical = (self.bucket, options.KEYS[0], 'historic')
        mapped = {historical: mapped[historical], **{k: v for k, v in mapped.items() if k != historical}}
        for number, (identity, target) in enumerate(mapped.items()):
            target.update(bucket=identity[0], key=identity[1], versionId=identity[2], node='vm000', disk=0,
                          relativePath='generation-' + str(number), sha256='sha-' + identity[2], size=128,
                          cohort=[{'role': 'data'}, {'role': 'parity'}])
        snapshots = types.SimpleNamespace(stop=lambda: None, snapshot=lambda *a, **k: {}, faults=[],
                                         record=lambda label, value: value)
        snapshots.remote = unittest.mock.Mock(side_effect=RuntimeError('remote mutation boundary'))
        with patch.object(options, 'seed_fixture', return_value=[]), patch.object(options, 'verify_seed'), \
                patch.object(options, 'map_fixture', return_value=mapped), patch.object(options, 'verify_parts'):
            probe = types.SimpleNamespace(inventory=lambda *a, **k: {})
            with self.assertRaisesRegex(RuntimeError, 'remote mutation boundary'):
                options.scope_probe(snapshots, probe, {}, None, self.bucket, 'H1', 'DV', 'recreate',
                                    options.paired_options('recreate')[0], [])
        self.assertEqual(snapshots.remote.call_args.kwargs['relativePath'], mapped[current]['relativePath'])
        self.assertEqual(snapshots.remote.call_args.kwargs['expectedSha256'], mapped[current]['sha256'])

    def test_complete_oracle_rejects_lost_versions_wrong_scopes_markers_and_false_counters(self):
        for kind in ('version', 'missing', 'duplicate', 'unknown', 'scope', 'null-scope', 'bool-scope',
                     'incarnation', 'marker', 'counter', 'overflow', 'coverage', 'settings', 'truncated'):
            with self.subTest(kind=kind):
                status, mapped, selected, repaired, request = self.status()
                receipt = status['outcome']['objects'][0]
                if kind == 'version': receipt['identity']['versionId'] = 'unacknowledged'
                elif kind == 'missing': status['outcome']['objects'].pop()
                elif kind == 'duplicate': status['outcome']['objects'][-1] = copy.deepcopy(receipt)
                elif kind == 'unknown': receipt['disposition']['state'] = 'unknown'
                elif kind == 'scope': receipt['identity']['setIndex'] = 1
                elif kind == 'null-scope': receipt['identity']['poolIndex'] = None
                elif kind == 'bool-scope': receipt['identity']['setIndex'] = False
                elif kind == 'incarnation': receipt['identity']['bucketIncarnationId'] = ''
                elif kind == 'marker':
                    marker = next(row for row in status['outcome']['objects'] if row['identity']['versionId'] == 'marker')
                    marker['disposition']['state'] = 'verified_healthy'
                elif kind == 'counter': status['outcome']['counters']['healed'] += 1
                elif kind == 'overflow': status['outcome']['counters']['overflowed'] = True
                elif kind == 'coverage': status['outcome']['coverage'] = 'partial'
                elif kind == 'settings': status['settings'] = {**request, 'updateParity': True}
                else: status['truncated'] = True
                with self.assertRaises(ValueError): options.validate_outcome(status, mapped, selected, repaired, request)

    def test_source_cleanup_receipts_keep_exact_versions_controls_and_counter_partition(self):
        for state in ('repaired', 'authoritatively_absent'):
            status, mapped, selected, repaired, request = self.status()
            request = {**request, 'remove': True}; status['settings'] = request
            target = next(iter(repaired))
            receipt = next(r for r in status['outcome']['objects']
                           if r['identity']['versionId'] == target[2] and r['identity']['object'] == target[1])
            receipt['disposition']['state'] = state
            absent = {target} if state == 'authoritatively_absent' else set()
            if absent:
                status['outcome']['counters']['healed'] -= 1
                status['outcome']['counters']['unchanged'] += 1
            proof = options.validate_outcome(status, mapped, selected, repaired - absent, request, absent=absent)
            self.assertEqual(proof['authoritativelyAbsentACKs'], len(absent))
            for kind in ('missing-target', 'wrong-version', 'unknown', 'false-counter', 'marker', 'wrong-options'):
                bad = copy.deepcopy(status)
                if kind == 'missing-target': bad['outcome']['objects'].remove(bad['outcome']['objects'][status['outcome']['objects'].index(receipt)])
                elif kind == 'wrong-version': bad['outcome']['objects'][status['outcome']['objects'].index(receipt)]['identity']['versionId'] = 'unacknowledged'
                elif kind == 'unknown': bad['outcome']['objects'][status['outcome']['objects'].index(receipt)]['disposition']['state'] = 'unknown'
                elif kind == 'false-counter': bad['outcome']['counters']['healed'] += 1
                elif kind == 'marker':
                    next(r for r in bad['outcome']['objects'] if r['identity']['versionId'] == 'marker')['disposition']['state'] = 'authoritatively_absent'
                else: bad['settings'] = {**request, 'remove': False}
                with self.subTest(state=state, kind=kind), self.assertRaises(ValueError):
                    options.validate_outcome(bad, mapped, selected, repaired - absent, request, absent=absent)

    def test_nullable_unrestricted_scope_keeps_the_independent_physical_binding(self):
        status, mapped, selected, repaired, request = self.status('recursive-true')
        before = copy.deepcopy(status)
        options.validate_outcome(status, mapped, selected, repaired, request)
        self.assertEqual(before, status)
        receipt = status['outcome']['objects'][0]['identity']
        identity = receipt['bucket'], receipt['object'], receipt['versionId']
        receipt.update(poolIndex=0, setIndex=1 - mapped[identity]['setIndex'])
        with self.assertRaises(ValueError): options.validate_outcome(status, mapped, selected, repaired, request)

    def test_exact_missing_prefix_retains_the_requested_failure_instead_of_ack_objects(self):
        baseline, request = options.paired_options('recursive-false')
        detail = 'Heal task execution failed: Object not found: ' + self.bucket + '/scope/'
        status = {'summary': 'stopped', 'settings': request, 'detail': detail,
            'outcome': {'coverage': 'complete', 'objectsTruncated': False, 'execution': {'state': 'completed_with_errors'},
                        'objects': [{'identity': {'kind': 'object', 'bucket': self.bucket, 'object': 'scope/',
                            'versionId': None, 'bucketIncarnationId': None, 'poolIndex': None, 'setIndex': None},
                            'disposition': {'state': 'failed', 'details': 'permanent'}, 'detail': detail}],
                        'counters': {'processed': 1, 'healed': 0, 'unchanged': 0, 'skipped': 0,
                                     'failed': 1, 'unknown': 0, 'attemptFailures': 1, 'overflowed': False}}}
        proof = options.validate_outcome(status, self.mapped(), set(), set(), request)
        self.assertEqual(proof['selectedACKs'], 0)
        self.assertFalse(proof['repairPassClaimed'])
        for kind in ('success', 'legacy-success', 'repair', 'unknown', 'receipt', 'missing-receipt', 'detail',
                     'wrong-key', 'incarnation', 'selector', 'attempts', 'false-absence', 'missing-identity'):
            changed = copy.deepcopy(status)
            if kind == 'success': changed['outcome']['execution']['state'] = 'completed'
            elif kind == 'legacy-success': changed['summary'] = 'finished'
            elif kind == 'repair': changed['outcome']['counters']['healed'] = 1
            elif kind == 'unknown': changed['outcome']['counters']['unknown'] = 1
            elif kind == 'receipt': changed['outcome']['objects'] = [{'disposition': {'state': 'unknown'}}]
            elif kind == 'missing-receipt': changed['outcome']['objects'] = []
            elif kind == 'detail': changed['detail'] = ''
            elif kind == 'wrong-key': changed['outcome']['objects'][0]['identity']['object'] = options.KEYS[0]
            elif kind == 'incarnation': changed['outcome']['objects'][0]['identity']['bucketIncarnationId'] = 'invented'
            elif kind == 'selector': changed['outcome']['objects'][0]['identity']['poolIndex'] = False
            elif kind == 'attempts': changed['outcome']['counters']['attemptFailures'] = 0
            elif kind == 'false-absence': changed['outcome']['objects'][0]['disposition']['state'] = 'authoritatively_absent'
            else: changed['outcome']['objects'][0]['identity'].pop('bucketIncarnationId')
            with self.assertRaises(ValueError): options.validate_outcome(changed, self.mapped(), set(), set(), request)

    def physical_scene(self):
        fixture = scope_fixtures.ScopeTests()
        original, mapped = fixture.snapshot(), fixture.mapping()
        targets = []
        after = copy.deepcopy(original)
        for number, target in enumerate(list(mapped.values())[:2]):
            target = {**target, 'faultKind': 'missing' if number == 0 else 'bitflip',
                      'faultBytes': None if number == 0 else {'sha256': 'flipped', 'size': target['size']}}
            targets.append(target)
            entries = after[target['node']]['files']
            entry = next(row for row in entries if (row['disk'], row['path']) == (target['disk'], target['relativePath']))
            if number == 0: entries.remove(entry)
            else: entry['sha256'] = 'flipped'
        return original, after, targets

    def test_rejected_request_keeps_missing_and_corrupted_controls_exact(self):
        original, after, targets = self.physical_scene()
        proof = options.physical_selection(original, after, targets, set())
        self.assertEqual(proof['selectedPhysicalRepairs'], 0)
        self.assertEqual(proof['unselectedFaultsPreserved'], 2)
        self.assertTrue(proof['beforeAnyTargetGETOrBackupRescue'])

    def test_unselected_control_repair_is_not_hidden_by_selected_repair_or_backup_rescue(self):
        for control in (0, 1):
            original, after, targets = self.physical_scene()
            target = targets[control]
            entries = after[target['node']]['files']
            entries[:] = [row for row in entries if (row['disk'], row['path']) != (target['disk'], target['relativePath'])]
            entry = next(row for row in original[target['node']]['files']
                         if (row['disk'], row['path']) == (target['disk'], target['relativePath']))
            entries.append(copy.deepcopy(entry))
            with self.assertRaisesRegex(ValueError, 'unselected'): options.physical_selection(original, after, targets, set())

    def test_selected_physical_repair_leaves_corrupted_unselected_control_unchanged(self):
        original, after, targets = self.physical_scene()
        target = targets[0]
        entry = next(row for row in original[target['node']]['files']
                     if (row['disk'], row['path']) == (target['disk'], target['relativePath']))
        after[target['node']]['files'].append(copy.deepcopy(entry))
        selected = {(target['bucket'], target['key'], target['versionId'])}
        proof = options.physical_selection(original, after, targets, selected)
        self.assertEqual(proof['selectedPhysicalRepairs'], 1)
        self.assertEqual(proof['unselectedFaultsPreserved'], 1)

    def test_wrong_or_absent_put_identity_remains_pending(self):
        for response in ({'VersionId': 'not-a-version', 'ETag': 'etag'}, {'VersionId': 'null'},
                         {'VersionId': '00000000-0000-0000-0000-000000000000', 'ETag': 'etag'}):
            with tempfile.TemporaryDirectory() as name, patch.object(options.Recorder, 'call', return_value=response):
                recorder = options.FixtureRecorder(types.SimpleNamespace(), Path(name) / 'ACKs')
                with self.assertRaises(ValueError): recorder.call('put_object', {'Bucket': self.bucket, 'Key': 'owned.bin'}, b'body')
                self.assertIsNotNone(recorder.pending)

    def test_transport_or_receipt_failure_retains_original_request(self):
        with tempfile.TemporaryDirectory() as name, patch.object(options.Recorder, 'call', side_effect=RuntimeError('lost ACK')):
            recorder = options.FixtureRecorder(types.SimpleNamespace(), Path(name) / 'ACKs')
            with self.assertRaises(RuntimeError): recorder.call('delete_object', {'Bucket': self.bucket, 'Key': 'owned.bin'})
            self.assertEqual(recorder.pending['operation'], 'delete_object')

    def test_wrong_exact_delete_or_marker_ack_never_clears_mutation_guard(self):
        version = '11111111-1111-4111-8111-111111111111'
        for params, response in (({'VersionId': version}, {'VersionId': '22222222-2222-4222-8222-222222222222'}),
                                 ({}, {'VersionId': version, 'DeleteMarker': False}),
                                 ({}, {'VersionId': 'null', 'DeleteMarker': True})):
            with tempfile.TemporaryDirectory() as name, patch.object(options.Recorder, 'call', return_value=response):
                recorder = options.FixtureRecorder(types.SimpleNamespace(), Path(name) / 'ACKs', fixture='DV')
                with self.assertRaises(ValueError): recorder.call('delete_object', {'Bucket': self.bucket, 'Key': 'owned.bin', **params})
                self.assertIsNotNone(recorder.pending)

    def test_duplicate_put_ack_is_retained_even_with_successful_http(self):
        response = {'VersionId': '11111111-1111-4111-8111-111111111111', 'ETag': 'etag'}
        with tempfile.TemporaryDirectory() as name, patch.object(options.Recorder, 'call', return_value=response):
            recorder = options.FixtureRecorder(types.SimpleNamespace(), Path(name) / 'ACKs', fixture='DV')
            params = {'Bucket': self.bucket, 'Key': 'owned.bin'}
            recorder.call('put_object', params, b'one')
            self.assertIsNone(recorder.pending)
            with self.assertRaises(ValueError): recorder.call('put_object', params, b'two')
            self.assertIsNotNone(recorder.pending)

    def test_positive_http_with_wrong_dv_mode_is_not_a_resolved_fixture_ack(self):
        fake = types.SimpleNamespace(pending=None)
        fake.call = lambda operation, params, body=None: {'VersionId': 'null', 'ETag': 'etag'}
        with self.assertRaisesRegex(ValueError, 'DB/DV'): options.seed_fixture(fake, self.bucket, 'DV')
        self.assertIsNotNone(fake.pending)

    def test_no_single_branch_checkpoint_or_duplicate_can_certify_canonical_completion(self):
        rows = [{'topology': topology, 'branch': branch, 'fixture': fixture, 'phase': phase,
                 'verdict': 'PASS', 'cleanup': 'PASS', 'fullProbeCompleted': True}
                for topology in ('H1', 'H2') for branch in options.BRANCHES for fixture in options.FIXTURES
                for phase in options.source_phases(branch)]
        self.assertEqual(len(rows), 68)
        self.assertEqual({r['branch'] for r in rows}, set(options.BRANCHES))
        for topology in ('H1', 'H2'):
            for fixture in options.FIXTURES:
                self.assertEqual({r['branch'] for r in rows if r['topology'] == topology
                                  and r['fixture'] == fixture and r['phase'] == 'selected'}, set(options.BRANCHES))
        with self.assertRaises(ValueError):
            options.complete_results([*rows, {**rows[0], 'branch': 'remove-true', 'phase': 'baseline'}])
        self.assertTrue(options.complete_results(rows))
        self.assertFalse(options.complete_results(rows[:-1]))
        for name, value in (('verdict', 'FAIL'), ('cleanup', 'FAIL'), ('fullProbeCompleted', False)):
            changed = copy.deepcopy(rows); changed[-1][name] = value
            self.assertFalse(options.complete_results(changed))
        with self.assertRaises(ValueError): options.complete_results([*rows, rows[0]])

    def test_rejected_successful_admission_is_retained_for_original_token_cancellation(self):
        probe = types.SimpleNamespace(admin=lambda *args: {'httpStatus': 200, 'response': {'clientToken': 'wrong-admission'}})
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        active = []
        with self.assertRaises(ValueError): options.run_request(probe, fleet, '/rustfs/admin/v3/heal/',
            options.paired_options('read-repair')[1], active, rejected=True)
        self.assertEqual(active, [('/rustfs/admin/v3/heal/', 'wrong-admission')])

    def test_a10_orphan_and_scope_node_programs_compile_with_the_actual_adapter(self):
        def invoke(argv, **params):
            compile(params['input'], 'remote-option-node', 'exec')
            return types.SimpleNamespace(returncode=0, stdout='{"checked":true}', stderr='')
        for profile in ('scope-isolation', 'orphan-preserve'):
            with tempfile.TemporaryDirectory() as name, patch('scripts.validation_heal.healthy.subprocess.run', side_effect=invoke):
                fleet = Fleet(['n0', 'n1', 'n2', 'n3'], Path(name), 'gh-1234-1-h2', runtime_profile=profile)
                self.assertTrue(fleet.remote('n0', 'process-metrics')['checked'])


if __name__ == '__main__': unittest.main()
