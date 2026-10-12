"""C11's hosted native proof cannot be promoted to fleet or replay coverage."""

import json
from pathlib import Path
import tempfile
import unittest

from scripts.validation_heal import incarnation_native as suite
import test_validation_heal_native as fixtures


class IncarnationNativeTests(unittest.TestCase):
    def fixture(self, root):
        fixtures.evidence_fixture(root)
        api = {'id': 12345, 'run_attempt': 1, 'status': 'completed', 'conclusion': 'success',
            'head_sha': 'b' * 40, 'path': '.github/workflows/rustfs-validation-tools.yml'}
        (root / 'tool-run.json').write_text(json.dumps(api))
        return {'source_sha': suite.suite.INCARNATION_CANDIDATE}, api

    def test_full_hosted_storage_suite_is_scoped_to_native_prerequisites(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate, _ = self.fixture(root)
            proof = suite.verify(root, candidate, 'a' * 40)
            self.assertEqual(proof['exactTests'], 9)
            self.assertEqual(len(proof['casePrerequisiteTests']), 3)
            self.assertTrue(proof['exactStorageCommitBarrierObserved'])
            for field in ('canonicalFleetBranchesExecuted', 'checkpointMrfReplayClaimed',
                          'physicalPowerLossClaimed', 'fleetPassClaimed', 'canonicalCasePassClaimed'):
                self.assertFalse(proof[field])

    def test_hosted_api_attempt_candidate_and_external_helper_must_match(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate, api = self.fixture(root)
            path = root / 'tool-run.json'
            for field, value in [('id', True), ('id', 12346), ('run_attempt', 2), ('status', 'in_progress'),
                ('conclusion', 'failure'), ('head_sha', 'main'), ('path', '.github/workflows/ci.yml')]:
                path.write_text(json.dumps({**api, field: value}))
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    suite.verify(root, candidate, 'a' * 40)
            path.write_text(json.dumps(api))
            for wrong_candidate, helper in [({'source_sha': suite.suite.CANDIDATE}, 'a' * 40), (candidate, 'c' * 40)]:
                with self.assertRaises(ValueError): suite.verify(root, wrong_candidate, helper)


if __name__ == '__main__': unittest.main()
