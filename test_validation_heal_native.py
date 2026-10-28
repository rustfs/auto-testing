import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

from scripts.validation_heal import native_regressions as native
from scripts.validation_heal import persistence_native as persistence


def evidence_fixture(root, commit=native.INCARNATION_CANDIDATE):
    tools, evidence = root / 'native-tools', root / 'native-evidence'
    tools.mkdir(); evidence.mkdir()
    binary = tools / 'ecstore-native-tests'
    binary.write_bytes(b'original exact candidate test executable')
    contract = native.REVIEWED_CANDIDATES[commit]
    manifest = {'schemaVersion': 1, 'candidateCommit': commit, 'candidateSourceTree': contract['sourceTree'],
        'candidateCargoLockSha256': native.CARGO_LOCK_SHA, 'sourceFiles': contract['sourceFiles'],
        'sourceUnmodified': True, 'regressions': contract['regressions'], 'optimized': True,
        'collectorSha256': native.sha(Path(native.__file__)), 'binaryName': binary.name,
        'binarySha256': native.sha(binary), 'binaryKind': 'native-regression-not-released-service',
        'buildCommand': native.BUILD_COMMAND, 'buildEnvironment': native.BUILD_ENV,
        'runtimeEnvironment': native.runtime_environment(commit), 'fleetPassClaimed': False,
        'runId': '12345', 'runAttempt': '1', 'testingCommit': 'a' * 40}
    (tools / 'native-regressions.json').write_text(json.dumps(manifest))
    listing = '\n'.join(spec['test'] + ': test' for spec in contract['regressions'].values())
    (tools / 'test-inventory.log').write_text(listing)
    (evidence / 'runtime-inventory.log').write_text(listing)
    rows = []
    for key, spec in contract['regressions'].items():
        events = spec.get('barrierEvents', [])
        row = {'regression': key, 'test': spec['test'], 'sourceAssertions': spec['sourceAssertions'],
            'fenceKind': spec['fenceKind'], 'limitations': spec['limits'], 'candidateCommit': commit,
            'binarySha256': manifest['binarySha256'], 'command': native.exact_command(binary, spec['test'], commit),
            'runtimeEnvironment': manifest['runtimeEnvironment'], 'verdict': 'PASS', 'exitCode': 0,
            'fleetPassClaimed': False, 'emittedBarrierLogsClaimed': bool(events), 'elapsedNanoseconds': 100}
        if events: row['observedBarrierEvents'] = events
        output = f'running 1 test\ntest {spec["test"]} ... ok\n'
        output += '\n'.join('incarnation_commit_barrier: ' + event for event in events) + '\n'
        output += 'test result: ok. 1 passed; 0 failed; 0 ignored; 99 filtered out;\n'
        (evidence / (key + '.log')).write_text(output)
        (evidence / (key + '.json')).write_text(json.dumps(row))
        rows.append(row)
    (evidence / 'native-summary.json').write_text(json.dumps({'results': rows, 'complete': True,
        'sourceUnmodified': True, 'fleetPassClaimed': False, 'canonicalCasePassClaimed': False}))
    return tools, evidence, {'source_sha': commit}, 'a' * 40


class NativeRegressionTests(unittest.TestCase):
    name = native.PREFIX + 'read_repair_commit_aborts_when_object_changes_before_final_rename'

    def test_full_test_name_must_appear_once_in_actual_inventory(self):
        native.exact_listing(self.name + ': test\n', self.name)
        for listing in ('0 tests, 0 benchmarks\n', self.name.split('::')[-1] + ': test\n',
                        (self.name + ': test\n') * 2, self.name + '-other: test\n'):
            with self.subTest(listing=listing), self.assertRaises(ValueError):
                native.exact_listing(listing, self.name)

    def test_zero_filtered_ignored_failed_and_other_test_success_are_not_pass(self):
        passed = f'running 1 test\ntest {self.name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 200 filtered out;\n'
        proof = native.test_result(passed, self.name, 0)
        self.assertTrue(proof['exactTestExecuted'])
        self.assertFalse(proof['fleetPassClaimed'])
        self.assertFalse(proof['emittedBarrierLogsClaimed'])
        for output, code in ((passed, 1), (passed, False), (passed + passed, 0),
                             (passed + 'test unrelated ... ok\n', 0),
                             (passed.replace('running 1 test', 'running 0 tests'), 0),
                             (passed.replace('1 passed; 0 failed; 0 ignored', '0 passed; 0 failed; 1 ignored'), 0),
                             (passed.replace(self.name, self.name + '-other'), 0),
                             (passed.replace('... ok', '... FAILED'), 0)):
            with self.subTest(output=output, code=code), self.assertRaises(ValueError):
                native.test_result(output, self.name, code)

    def test_uncontrolled_concurrency_is_explicitly_not_a_fence(self):
        for key in ('overwrite-concurrency', 'version-delete-concurrency'):
            self.assertIn('not deterministic fence', native.REGRESSIONS[key]['fenceKind'])
        self.assertIn('cannot invent emitted barrier logs', native.REGRESSIONS['read-repair-lock-owner']['limits'])

    def test_authority_guards_bind_exact_candidate_tests_without_claiming_runtime_pause(self):
        for key, suffix in (
                ('conflicting-authority-guard', 'dangling_delete_guard_preserves_conflicting_identities_without_writing_metadata'),
                ('reconstructable-authority-preservation', 'heal_meta_quorum_failure_preserves_reconstructable_uncommitted_candidate')):
            spec = native.REGRESSIONS[key]
            self.assertEqual(spec['test'], native.PREFIX + suffix)
            self.assertEqual(spec['relatedCases'], ['heal.conflicting-subquorum-authority'])
            self.assertIn('deterministic', spec['fenceKind'])
            self.assertIn('pause', spec['limits'])

    def test_wrong_source_modified_contract_or_collector_fail_before_execution(self):
        base = {'schemaVersion': 1, 'candidateCommit': native.CANDIDATE, 'sourceFiles': {native.SOURCE: native.SOURCE_SHA},
            'sourceUnmodified': True, 'regressions': native.REGRESSIONS, 'optimized': True,
            'collectorSha256': native.sha(Path(native.__file__)), 'binaryName': 'ecstore-native-tests', 'binarySha256': '0' * 64}
        for field, value in (('candidateCommit', '0' * 40), ('sourceUnmodified', False),
                             ('regressions', {}), ('collectorSha256', '0' * 64), ('binaryName', '../foreign')):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                (directory / 'native-regressions.json').write_text(json.dumps({**base, field: value}))
                with self.assertRaises(ValueError):
                    native.run(directory, ['read-repair-final-rename'], directory / 'evidence')
                self.assertFalse((directory / 'evidence').exists())


class PersistenceNativeTests(unittest.TestCase):
    def fixture(self, root):
        tools, original, _, _ = evidence_fixture(root)
        inventory = tools / 'test-inventory.log'
        inventory.write_text(inventory.read_text() + '\n' + '\n'.join(
            persistence.PREFIX + suffix + ': test' for suffix, _ in persistence.REGRESSIONS.values()))
        return tools, original, root / 'persistence'

    def execute(self, tools, fail=None):
        def child(command, **kwargs):
            if command[-1] == '--list':
                return subprocess.CompletedProcess(command, 0, (tools / 'test-inventory.log').read_text(), '')
            self.assertEqual(command[2:], ['--exact', '--show-output', '--test-threads=1'])
            self.assertEqual(kwargs['env']['RUST_MIN_STACK'], '33554432')
            if fail == 'timeout': raise subprocess.TimeoutExpired(command, 120, output=b'original timeout output\n')
            name = command[1]
            output = f'running 1 test\ntest {name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n'
            if fail == 'zero-test': output = 'running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored;\n'
            return subprocess.CompletedProcess(command, 0, output, '')
        return child

    def run_fixture(self, root):
        tools, original, out = self.fixture(root)
        with patch.object(persistence.subprocess, 'run', side_effect=self.execute(tools)):
            persistence.run(tools, original, out, 'b' * 40)
        return tools, original, out

    def test_executes_existing_exact_tests_without_claiming_complete_cuts(self):
        with tempfile.TemporaryDirectory() as temp:
            tools, original, out = self.run_fixture(Path(temp))
            proof = persistence.verify(tools, original, out, 'b' * 40)
            self.assertEqual(proof['exactTests'], 6)
            self.assertEqual(proof['nativeTestingCommit'], 'a' * 40)
            self.assertEqual(proof['collectorCommit'], 'b' * 40)
            for flag in ('requiredNativeCoverageComplete', 'individualFileFsyncCovered', 'physicalPowerLossClaimed',
                         'canonicalCasePassClaimed', 'fleetPassClaimed', 'emittedBarrierLogsClaimed'):
                self.assertIs(proof[flag], False)

    def test_unverified_original_suite_cannot_launch_supplement(self):
        for change in ('source', 'binary', 'original-log', 'missing-test'):
            with tempfile.TemporaryDirectory() as temp, self.subTest(change=change):
                tools, original, out = self.fixture(Path(temp))
                if change == 'source':
                    path = tools / 'native-regressions.json'
                    value = json.loads(path.read_text()); value['candidateCommit'] = native.CANDIDATE
                    path.write_text(json.dumps(value))
                elif change == 'binary': (tools / 'ecstore-native-tests').write_bytes(b'changed executable')
                elif change == 'original-log': (original / 'incarnation-final-rename.log').write_text('running 0 tests')
                else:
                    path = tools / 'test-inventory.log'
                    path.write_text(path.read_text().replace(persistence.PREFIX + next(iter(persistence.REGRESSIONS.values()))[0] + ': test', ''))
                with patch.object(persistence.subprocess, 'run') as execute, self.assertRaises(ValueError):
                    persistence.run(tools, original, out, 'b' * 40)
                execute.assert_not_called()
                self.assertFalse(out.exists())

    def test_zero_test_and_timeout_keep_original_failed_diagnostics(self):
        for failure in ('zero-test', 'timeout'):
            with tempfile.TemporaryDirectory() as temp, self.subTest(failure=failure):
                tools, original, out = self.fixture(Path(temp))
                with patch.object(persistence.subprocess, 'run', side_effect=self.execute(tools, failure)), self.assertRaises(ValueError):
                    persistence.run(tools, original, out, 'b' * 40)
                summary = json.loads((out / 'persistence-native-summary.json').read_text())
                self.assertIs(summary['complete'], False)
                self.assertEqual(len(summary['results']), 6)
                self.assertTrue(all(row['verdict'] == 'FAIL' for row in summary['results']))
                self.assertIn('original timeout output' if failure == 'timeout' else 'running 0 tests',
                              (out / 'mrf-manifest-directory-sync.log').read_text())
                with self.assertRaises(ValueError): persistence.verify(tools, original, out, 'b' * 40)

    def test_tampered_source_commands_logs_or_scope_cannot_certify_fsync(self):
        for change in ('candidate', 'attempt', 'collector', 'powerloss', 'complete-cuts', 'duplicate', 'command', 'assertion', 'log', 'inventory'):
            with tempfile.TemporaryDirectory() as temp, self.subTest(change=change):
                tools, original, out = self.run_fixture(Path(temp))
                path = out / 'persistence-native-summary.json'
                summary = json.loads(path.read_text())
                if change == 'candidate': summary['candidateCommit'] = 'f' * 40
                elif change == 'attempt': summary['runAttempt'] = '2'
                elif change == 'collector': summary['collectorSha256'] = '0' * 64
                elif change == 'powerloss': summary['physicalPowerLossClaimed'] = True
                elif change == 'complete-cuts': summary['requiredNativeCoverageComplete'] = True
                elif change == 'duplicate': summary['results'][0] = copy.deepcopy(summary['results'][1])
                elif change in ('command', 'assertion'):
                    row = summary['results'][0]
                    if change == 'command': row['command'][2] = '--ignored'
                    else: row['sourceAssertion'] = 'all fsync and powerloss cuts'
                    (out / (row['regression'] + '.json')).write_text(json.dumps(row))
                elif change == 'log': (out / 'cas-update-directory-sync.log').write_text('running 0 tests')
                else: (out / 'runtime-inventory.log').write_text('0 tests')
                path.write_text(json.dumps(summary))
                with self.assertRaises(ValueError): persistence.verify(tools, original, out, 'b' * 40)


class NativeEvidenceTests(unittest.TestCase):
    def verify(self, fixture, **changes):
        tools, evidence, candidate, helper = fixture
        return native.verify_evidence(tools, evidence, changes.get('candidate', candidate),
            changes.get('run', 12345), changes.get('attempt', 1), changes.get('helper', helper))

    def test_complete_suites_preserve_original_six_and_exact_nine_scopes(self):
        for commit, count in ((native.CANDIDATE, 6), (native.INCARNATION_CANDIDATE, 9)):
            with self.subTest(commit=commit), tempfile.TemporaryDirectory() as temp:
                proof = self.verify(evidence_fixture(Path(temp), commit))
                self.assertEqual(proof['exactTests'], count)
                self.assertFalse(proof['canonicalCasePassClaimed'])
                self.assertFalse(proof['fleetPassClaimed'])
                self.assertEqual(bool(proof['observedBarrierEvents']), count == 9)

    def test_runner_executes_full_candidate_suite_and_retains_original_milestones(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = evidence_fixture(Path(temp)); tools, evidence, _, _ = fixture
            calls = []
            def execute(command, **kwargs):
                calls.append(command)
                if command[-1] == '--list':
                    return subprocess.CompletedProcess(command, 0, (tools / 'test-inventory.log').read_text(), '')
                self.assertEqual(kwargs['env']['RUST_MIN_STACK'], '33554432')
                self.assertEqual(command[2:], ['--exact', '--show-output', '--test-threads=1'])
                key = next(key for key, spec in native.REVIEWED_CANDIDATES[native.INCARNATION_CANDIDATE]['regressions'].items()
                           if spec['test'] == command[1])
                return subprocess.CompletedProcess(command, 0, (evidence / (key + '.log')).read_text(), '')
            out = Path(temp) / 'executed'
            with patch.object(native.subprocess, 'run', side_effect=execute): native.run(tools, None, out)
            self.assertEqual(len(calls), 10)
            proof = native.verify_evidence(tools, out, fixture[2], 12345, 1, fixture[3])
            self.assertEqual(proof['observedBarrierEvents']['incarnation-final-rename'], native.INCARNATION_EVENTS)

    def test_runner_retains_missing_barrier_failure_and_refuses_complete_success(self):
        with tempfile.TemporaryDirectory() as temp:
            tools, evidence, _, _ = evidence_fixture(Path(temp))
            def execute(command, **kwargs):
                if command[-1] == '--list':
                    output = (tools / 'test-inventory.log').read_text()
                else:
                    output = (evidence / 'incarnation-final-rename.log').read_text().replace(
                        'incarnation_commit_barrier: final_rename_paused', '')
                return subprocess.CompletedProcess(command, 0, output, '')
            out = Path(temp) / 'failed'
            with patch.object(native.subprocess, 'run', side_effect=execute), self.assertRaises(ValueError):
                native.run(tools, ['incarnation-final-rename'], out)
            self.assertIn('caller_aborted_writer_pending', (out / 'incarnation-final-rename.log').read_text())
            summary = json.loads((out / 'native-summary.json').read_text())
            self.assertFalse(summary['complete'])
            self.assertEqual(summary['results'][0]['verdict'], 'FAIL')

    def test_source_tree_lock_build_runtime_collector_and_scope_drift_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = evidence_fixture(Path(temp)); path = fixture[0] / 'native-regressions.json'
            original = json.loads(path.read_text())
            for field, value in [('candidateCommit', native.CANDIDATE), ('candidateSourceTree', 'b' * 40),
                ('candidateCargoLockSha256', 'b' * 64), ('sourceFiles', {}), ('collectorSha256', 'b' * 64),
                ('regressions', {}), ('sourceUnmodified', False), ('optimized', False), ('buildCommand', []),
                ('buildEnvironment', {}), ('runtimeEnvironment', {}), ('fleetPassClaimed', True),
                ('binaryKind', 'released-service'), ('binaryName', '../foreign')]:
                path.write_text(json.dumps({**original, field: value}))
                with self.subTest(field=field), self.assertRaises(ValueError): self.verify(fixture)

    def test_candidate_run_attempt_helper_and_binary_are_independently_bound(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = evidence_fixture(Path(temp))
            for changes in [{'candidate': {'source_sha': 'b' * 40}}, {'run': 12346}, {'attempt': 2}, {'helper': 'b' * 40}]:
                with self.subTest(changes=changes), self.assertRaises(ValueError): self.verify(fixture, **changes)
            (fixture[0] / 'ecstore-native-tests').write_bytes(b'wrong executable')
            with self.assertRaises(ValueError): self.verify(fixture)

    def test_symlinked_binary_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = evidence_fixture(Path(temp)); binary = fixture[0] / 'ecstore-native-tests'
            original = fixture[0] / 'original'; binary.rename(original); binary.symlink_to(original)
            with self.assertRaises(ValueError): self.verify(fixture)

    def test_summary_cannot_omit_duplicate_or_promote_tests(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = evidence_fixture(Path(temp)); path = fixture[1] / 'native-summary.json'
            original = json.loads(path.read_text())
            for change in ('missing', 'duplicate', 'incomplete', 'fleet', 'canonical'):
                value = copy.deepcopy(original)
                if change == 'missing': value['results'].pop()
                elif change == 'duplicate': value['results'][-1] = value['results'][0]
                elif change == 'incomplete': value['complete'] = False
                elif change == 'fleet': value['fleetPassClaimed'] = True
                else: value['canonicalCasePassClaimed'] = True
                path.write_text(json.dumps(value))
                with self.subTest(change=change), self.assertRaises(ValueError): self.verify(fixture)

    def test_original_milestones_must_be_complete_unique_and_ordered(self):
        with tempfile.TemporaryDirectory() as temp:
            fixture = evidence_fixture(Path(temp)); path = fixture[1] / 'incarnation-final-rename.log'
            original = path.read_text()
            first, second = ['incarnation_commit_barrier: ' + e for e in native.INCARNATION_EVENTS[:2]]
            for output in (original.replace(first, ''), original + first + '\n',
                           original.replace(first, 'TEMP').replace(second, first).replace('TEMP', second)):
                path.write_text(output)
                with self.subTest(output=output), self.assertRaises(ValueError): self.verify(fixture)

    def test_forged_rows_and_commands_cannot_replace_original_proof(self):
        for change in ('row-only', 'events', 'claim', 'command', 'runtime', 'scope', 'exit', 'binary'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                fixture = evidence_fixture(Path(temp)); key = 'incarnation-final-rename'
                path = fixture[1] / (key + '.json'); row = json.loads(path.read_text())
                if change in ('row-only', 'events'): row['observedBarrierEvents'] = []
                elif change == 'claim': row['emittedBarrierLogsClaimed'] = False
                elif change == 'command': row['command'][3] = '--nocapture'
                elif change == 'runtime': row['runtimeEnvironment'] = {}
                elif change == 'scope': row['fleetPassClaimed'] = True
                elif change == 'exit': row['exitCode'] = 1
                else: row['binarySha256'] = 'b' * 64
                path.write_text(json.dumps(row))
                if change != 'row-only':
                    summary_path = fixture[1] / 'native-summary.json'
                    summary = json.loads(summary_path.read_text())
                    summary['results'][-1] = row; summary_path.write_text(json.dumps(summary))
                with self.assertRaises(ValueError): self.verify(fixture)

    def test_original_inventory_and_test_execution_cannot_be_missing_or_ignored(self):
        for change in ('missing', 'duplicate', 'zero', 'ignored', 'wrong-test'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                fixture = evidence_fixture(Path(temp)); key = 'incarnation-final-rename'
                name = native.INCARNATION_REGRESSIONS[key]['test']
                if change in ('missing', 'duplicate'):
                    path = fixture[1] / 'runtime-inventory.log'; original = path.read_text()
                    path.write_text(original.replace(name + ': test', '') if change == 'missing' else original + '\n' + name + ': test')
                else:
                    path = fixture[1] / (key + '.log'); original = path.read_text()
                    if change == 'zero': original = original.replace('running 1 test', 'running 0 tests')
                    elif change == 'ignored': original = original.replace('... ok', '... ignored')
                    else: original = original.replace(name, name + '-other')
                    path.write_text(original)
                with self.assertRaises(ValueError): self.verify(fixture)


if __name__ == '__main__':
    unittest.main()
