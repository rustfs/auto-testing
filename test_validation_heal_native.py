import json
from pathlib import Path
import tempfile
import unittest

from scripts.validation_heal import native_regressions as native


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
        for output, code in ((passed, 1), (passed.replace('running 1 test', 'running 0 tests'), 0),
                             (passed.replace('1 passed; 0 failed; 0 ignored', '0 passed; 0 failed; 1 ignored'), 0),
                             (passed.replace(self.name, self.name + '-other'), 0),
                             (passed.replace('... ok', '... FAILED'), 0)):
            with self.subTest(output=output, code=code), self.assertRaises(ValueError):
                native.test_result(output, self.name, code)

    def test_uncontrolled_concurrency_is_explicitly_not_a_fence(self):
        for key in ('overwrite-concurrency', 'version-delete-concurrency'):
            self.assertIn('not deterministic fence', native.REGRESSIONS[key]['fenceKind'])
        self.assertIn('cannot invent emitted barrier logs', native.REGRESSIONS['read-repair-lock-owner']['limits'])

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


if __name__ == '__main__':
    unittest.main()
