"""Negative admission checks; synthetic files never constitute native evidence."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import race_native as suite
from scripts.validation_heal import race_native_evidence as consumer
from scripts.validation_heal import race


SOURCE = '6afdec9fe1b0b88007f2e5b445c19bf73d51d3ca'
HELPER = 'a' * 40
CASE = 'heal.race-unversioned-overwrite'
CANDIDATE = {'source_sha': SOURCE, 'package_sha256': 'b' * 64}


class RaceNativeTests(unittest.TestCase):
    def test_new_source_g01_requires_exact_bundle_and_other_race_cases_remain_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            proof = race.native_preflight(CANDIDATE, HELPER, CASE, root)
            self.assertTrue(proof['requiredNativeSubsetPassed'])
            with self.assertRaises(ValueError): race.native_preflight(CANDIDATE, HELPER, CASE, '')
            for case in race.CASE_IDS[1:]:
                with self.subTest(case=case), self.assertRaises(ValueError):
                    race.native_preflight(CANDIDATE, HELPER, case, root)
            self.assertIsNone(race.native_preflight({'source_sha': race.REFERENCE['candidateCommit']}, HELPER, CASE, ''))
            self.assertFalse(race.complete_results([], CASE))
        shell = Path('scripts/validation_heal/run_case.sh').read_text()
        self.assertLess(shell.index('race.py" preflight'), shell.index('(bootstrap)'))

    def test_source_specific_settings_keep_exact_types_selectors_and_lock_policy(self):
        reference = race.source_contract(CANDIDATE, CASE)
        value = {'httpStatus': 200, 'response': {'summary': 'running', 'settings': copy.deepcopy(reference['effective'])}}
        self.assertEqual(race.settings(value, reference), reference['effective'])
        for key, changed in [('nolock', True), ('pool', 0), ('set', 0), ('recursive', 1), ('readRepair', True)]:
            rejected = copy.deepcopy(value)
            rejected['response']['settings'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): race.settings(rejected, reference)

    def fixture(self, root):
        tools, evidence = root / 'native-tools', root / 'native-evidence'
        tools.mkdir()
        evidence.mkdir()
        contract = suite.contract_for(SOURCE)
        binary = tools / suite.BINARY
        binary.write_bytes(b'synthetic nonexecutable artifact; must never run')
        manifest = {'schemaVersion': 1, 'candidateCommit': SOURCE, 'artifactKind': suite.KIND,
            'candidateSourceTree': contract['sourceTree'], 'candidateCargoLockSha256': contract['cargoLockSha256'],
            'sourceFiles': contract['sourceFiles'], 'regressions': contract['regressions'], 'cases': contract['cases'],
            'buildEnvironment': suite.BUILD_ENV, 'buildCommand': suite.BUILD_COMMAND,
            'runtimeEnvironment': suite.RUNTIME_ENV, 'optimized': True, 'sourceUnmodified': True,
            'binaryName': suite.BINARY, 'binarySha256': suite.sha(binary),
            'collectorSha256': suite.sha(Path(suite.__file__)),
            'selectedContractSha256': hashlib.sha256(suite.canonical(contract)).hexdigest(),
            'canonicalCasePassClaimed': False, 'fleetExecutionClaimed': False, 'testingCommit': HELPER,
            'driverCommit': 'c' * 40, 'registrySourceSha256': suite.sha(suite.CONTRACTS),
            'runId': '123', 'runAttempt': '2'}
        self.write(tools / 'race-native.json', manifest)
        rows = []
        for label, spec in contract['regressions'].items():
            output = f"running 1 test\ntest {spec['test']} ... ok\n"
            output += '\n'.join(spec['milestones']) + '\n'
            output += 'test result: ok. 1 passed; 0 failed; 0 ignored; 99 filtered out; finished in 0.1s\n'
            log = evidence / (label + '.log')
            log.write_text(output)
            rows.append({'label': label, 'test': spec['test'], 'nativeRegressionPassed': True,
                'exitCode': 0, 'runtimeEnvironment': suite.RUNTIME_ENV,
                'command': ['/synthetic/' + suite.BINARY, spec['test'], '--exact', '--show-output', '--test-threads=1'],
                'milestones': spec['milestones'], 'originalLogSha256': suite.sha(log)})
        self.write(evidence / 'race-native-results.json', {'schemaVersion': 1, 'candidateCommit': SOURCE,
            'candidateSourceTree': manifest['candidateSourceTree'], 'binarySha256': manifest['binarySha256'],
            'nativeManifestSha256': suite.sha(tools / 'race-native.json'), 'results': rows,
            'requiredNativeSubsetsComplete': True, 'canonicalCasePassClaimed': False, 'fleetExecutionClaimed': False})
        self.write(root / 'tool-run.json', {'id': 123, 'run_attempt': 2, 'status': 'completed',
            'conclusion': 'success', 'event': 'workflow_dispatch', 'repository': {'full_name': 'rustfs/rustfs'},
            'head_sha': 'c' * 40, 'path': '.github/workflows/rustfs-functional-chain.yml'})

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value) + '\n')

    def test_read_only_admission_selects_only_reviewed_subset_and_never_claims_case_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            with patch.object(suite.subprocess, 'run', side_effect=AssertionError('must not execute')):
                proof = consumer.verify(root, CANDIDATE, HELPER, CASE)
            self.assertEqual(len(proof['observedRegressions']), 2)
            self.assertTrue(proof['requiredNativeSubsetPassed'])
            self.assertFalse(proof['wholeCasePassClaimed'])
            self.assertFalse(proof['canonicalCasePassClaimed'])
            self.assertFalse(proof['fleetExecutionClaimed'])

    def test_wrong_api_source_attempt_driver_helper_or_package_is_rejected(self):
        mutations = [('id', 124), ('run_attempt', 1), ('run_attempt', True), ('head_sha', 'd' * 40),
            ('status', 'in_progress'), ('conclusion', 'failure'), ('event', 'push'),
            ('path', '.github/workflows/other.yml'), ('repository', {'full_name': 'other/rustfs'})]
        for key, value in mutations:
            with self.subTest(key=key, value=value), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self.fixture(root)
                api = json.loads((root / 'tool-run.json').read_text())
                api[key] = value
                self.write(root / 'tool-run.json', api)
                with self.assertRaises(ValueError): consumer.verify(root, CANDIDATE, HELPER, CASE)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            for candidate, helper, case in [({**CANDIDATE, 'source_sha': 'd' * 40}, HELPER, CASE),
                ({**CANDIDATE, 'package_sha256': 'invalid'}, HELPER, CASE),
                (CANDIDATE, 'd' * 40, CASE), (CANDIDATE, HELPER, 'heal.race-version-delete')]:
                with self.subTest(candidate=candidate, helper=helper, case=case), self.assertRaises(ValueError):
                    consumer.verify(root, candidate, helper, case)

    def test_incomplete_duplicate_false_scope_wrong_command_and_log_tampering_rejected(self):
        for kind in ('missing', 'duplicate', 'case-pass', 'fleet-pass', 'scope', 'exit', 'bool-exit',
                     'command', 'test', 'environment', 'milestones', 'log', 'binary', 'manifest', 'symlink'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self.fixture(root)
                path = root / 'native-evidence/race-native-results.json'
                value = json.loads(path.read_text())
                row = value['results'][0]
                if kind == 'missing': value['results'].pop()
                elif kind == 'duplicate': value['results'][-1] = copy.deepcopy(row)
                elif kind == 'case-pass': value['canonicalCasePassClaimed'] = True
                elif kind == 'fleet-pass': value['fleetExecutionClaimed'] = True
                elif kind == 'scope': value['requiredNativeSubsetsComplete'] = False
                elif kind == 'exit': row['exitCode'] = 1
                elif kind == 'bool-exit': row['exitCode'] = False
                elif kind == 'command': row['command'].remove('--exact')
                elif kind == 'test': row['test'] = 'set_disk::other'
                elif kind == 'environment': row['runtimeEnvironment'] = {}
                elif kind == 'milestones': row['milestones'].reverse()
                elif kind == 'log': (root / 'native-evidence' / (row['label'] + '.log')).write_text('changed')
                elif kind == 'binary': (root / 'native-tools' / suite.BINARY).write_bytes(b'changed')
                elif kind == 'manifest': value['nativeManifestSha256'] = '0' * 64
                else:
                    log = root / 'native-evidence' / (row['label'] + '.log')
                    moved = log.with_suffix('.original')
                    log.rename(moved)
                    log.symlink_to(moved)
                self.write(path, value)
                with self.assertRaises(ValueError): consumer.verify(root, CANDIDATE, HELPER, CASE)

    def test_exact_output_parser_rejects_zero_multiple_ignored_and_wrong_tests(self):
        name = 'set_disk::ops::test'
        valid = f'running 1 test\ntest {name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n'
        suite.exact_result(valid, name, 0)
        for output, code in [(valid, 1), (valid, False), (valid.replace('running 1 test', 'running 0 tests'), 0),
            (valid.replace('0 ignored', '1 ignored'), 0), (valid + valid, 0), (valid.replace(name, name + '_other'), 0)]:
            with self.subTest(output=output, code=code), self.assertRaises(ValueError):
                suite.exact_result(output, name, code)

    def test_ordered_events_cannot_be_missing_duplicated_reordered_or_invented(self):
        spec = suite.contract_for(SOURCE)['regressions']['committed-overwrite-cleanup']
        events = spec['milestones']
        self.assertEqual(suite.milestones('\n'.join(events), spec), events)
        for rows in [events[:-1], list(reversed(events)), events + events[:1], events + ['native_g01: invented']]:
            with self.subTest(rows=rows), self.assertRaises(ValueError): suite.milestones('\n'.join(rows), spec)

    def test_registry_additions_preserve_selected_contract_but_edits_invalidate_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            registry = json.loads(suite.CONTRACTS.read_text())
            registry['candidates']['e' * 40] = copy.deepcopy(registry['candidates'][SOURCE])
            p = root / 'registry.json'
            self.write(p, registry)
            with patch.object(suite, 'CONTRACTS', p): consumer.verify(root, CANDIDATE, HELPER, CASE)
            registry['candidates'][SOURCE]['sourceTree'] = 'f' * 40
            self.write(p, registry)
            with patch.object(suite, 'CONTRACTS', p), self.assertRaises(ValueError):
                consumer.verify(root, CANDIDATE, HELPER, CASE)

    def test_supports_unknown_and_other_case_are_distinct_from_malformed_input(self):
        for commit, case, code in [(SOURCE, CASE, 0), (SOURCE, 'heal.race-version-delete', 2),
                                  ('e' * 40, CASE, 2), ('main', CASE, 1)]:
            with self.subTest(commit=commit, case=case):
                result = subprocess.run([sys.executable, '-B', suite.__file__, 'supports', '--commit', commit,
                    '--case-id', case], capture_output=True, text=True)
                self.assertEqual(result.returncode, code, result.stderr)


if __name__ == '__main__':
    unittest.main()
