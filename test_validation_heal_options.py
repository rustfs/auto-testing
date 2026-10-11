import copy
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
        return {(self.bucket, key, version): {'poolIndex': 0, 'setIndex': number, 'deleteMarker': marker}
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

    def test_nullable_unrestricted_scope_keeps_the_independent_physical_binding(self):
        status, mapped, selected, repaired, request = self.status('recursive-true')
        before = copy.deepcopy(status)
        options.validate_outcome(status, mapped, selected, repaired, request)
        self.assertEqual(before, status)
        receipt = status['outcome']['objects'][0]['identity']
        identity = receipt['bucket'], receipt['object'], receipt['versionId']
        receipt.update(poolIndex=0, setIndex=1 - mapped[identity]['setIndex'])
        with self.assertRaises(ValueError): options.validate_outcome(status, mapped, selected, repaired, request)

    def test_exact_missing_prefix_requires_truthful_not_found_and_no_object_receipts(self):
        baseline, request = options.paired_options('recursive-false')
        status = {'summary': 'finished', 'settings': request, 'detail': 'Object not found: ' + self.bucket + '/scope/',
            'outcome': {'coverage': 'complete', 'objectsTruncated': False, 'execution': {'state': 'completed_with_errors'},
                        'objects': [], 'counters': {'healed': 0, 'failed': 1, 'unknown': 0}}}
        options.validate_outcome(status, self.mapped(), set(), set(), request)
        for kind in ('success', 'repair', 'unknown', 'receipt', 'detail'):
            changed = copy.deepcopy(status)
            if kind == 'success': changed['outcome']['execution']['state'] = 'completed'
            elif kind == 'repair': changed['outcome']['counters']['healed'] = 1
            elif kind == 'unknown': changed['outcome']['counters']['unknown'] = 1
            elif kind == 'receipt': changed['outcome']['objects'] = [{'disposition': {'state': 'unknown'}}]
            else: changed['detail'] = ''
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
                for phase in ('baseline', 'selected')]
        self.assertEqual(len(rows), 72)
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
