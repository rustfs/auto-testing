"""I05 source orchestration and links to real caller transport/ACK evidence."""

import copy
import json
import subprocess
import sys
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_heal import lock_source as source, lock_timeout_runner as runner
from scripts.validation_heal import race_native as native
import test_validation_heal_lock_timeout as timeout_tests
import test_validation_heal_lock_successor as successor_tests
import test_validation_heal_race_native as native_tests


class SourceNativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        native_tests.RaceNativeTests().fixture(self.root, source=runner.CANDIDATE)
        self.args = SimpleNamespace(native_directory=self.root, testing_commit=native_tests.HELPER)
        self.candidate = {'source_sha': runner.CANDIDATE, 'package_sha256': 'a' * 64}

    def test_real_readonly_verifier_composes_owner_and_full_stale_authority(self):
        with patch.object(native.subprocess, 'run', side_effect=AssertionError('never execute downloaded binary')):
            proof = source.verify_source_native(self.args, self.candidate)
        self.assertEqual(set(proof['groups']), set(source.GROUPS))
        self.assertTrue(proof['nativeOwnerAndStaleFencesPassed'])
        self.assertFalse(proof['fleetExecutionClaimed'] or proof['canonicalCasePassClaimed'])
        for case, label, count in (
                ('heal.race-versioned-put', 'explicit-version-append-final-rename', 6),
                ('heal.race-version-delete', 'explicit-version-delete-final-rename', 12)):
            regression = proof['groups'][case]['observedRegressions'][label]
            self.assertEqual(regression['witnessKind'], 'ordered-barrier-milestones')
            self.assertEqual(len(regression['milestones']), count)
            self.assertIn('full_bodies_verified', regression['milestones'][-1])

    def test_workflow_cli_verifies_all_native_subsets_without_running_binary(self):
        candidate = self.root / 'candidate.json'
        candidate.write_text(json.dumps(self.candidate))
        result = subprocess.run([sys.executable, '-B', str(Path(source.__file__)),
            '--directory', str(self.root), '--candidate', str(candidate),
            '--testing-commit', self.args.testing_commit], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        proof = json.loads(result.stdout)
        self.assertEqual(set(proof['groups']), set(source.GROUPS))
        self.assertTrue(proof['nativeOwnerAndStaleFencesPassed'])
        self.assertFalse(proof['canonicalCasePassClaimed'])

    def test_mixed_workflow_attempt_binary_or_source_cannot_form_one_proof(self):
        proofs = source.verify_source_native(self.args, self.candidate)['groups']
        for key, value in (('runId', 999), ('runAttempt', 99), ('binarySha256', 'f' * 64),
                           ('candidateSourceTree', 'f' * 40), ('selectedContractSha256', 'f' * 64)):
            with self.subTest(key=key):
                changed = copy.deepcopy(proofs)
                changed['heal.race-version-delete'][key] = value
                with patch.object(source, 'verify_native', side_effect=lambda d, c, h, case: changed[case]):
                    with self.assertRaisesRegex(ValueError, 'mix candidates'):
                        source.verify_source_native(self.args, self.candidate)

    def test_missing_stale_body_milestone_or_original_log_blocks_before_fleet(self):
        log = self.root / 'native-evidence/explicit-version-append-final-rename.log'
        log.write_text(log.read_text().replace('full_bodies_verified', 'etag_only'))
        with self.assertRaisesRegex(ValueError, 'original native log bytes differ'):
            source.verify_source_native(self.args, self.candidate)

    def test_missing_artifact_does_not_execute_a_partial_source_plan(self):
        self.args.native_directory = None
        with self.assertRaisesRegex(ValueError, 'requires exact-source native evidence'):
            source.verify_source_native(self.args, self.candidate)


class SourceEvidenceTests(unittest.TestCase):
    def scene(self, branch):
        case = (timeout_tests.TimeoutFlowTests() if branch == 'timeout' else successor_tests.SuccessorFlowTests())
        case.setUp()
        self.addCleanup(case.doCleanups)
        diagnostic = case.invoke()
        # Components execute inside a topology directory in the production runner.
        parent = case.output.parent
        desired = parent / branch
        if case.output != desired:
            case.output.rename(desired)
        return parent, case.probe.binding, diagnostic

    def test_actual_tcp_timeout_and_queued_successor_bytes_form_separate_chains(self):
        # Each test fixture supplies a real single-use HTTP connection; server
        # lock observations are controlled test data, never live native proof.
        for branch in source.LIVE_BRANCHES:
            with self.subTest(branch=branch):
                parent, binding, diagnostic = self.scene(branch)
                chain = source.link_fleet_evidence(parent, branch, binding, diagnostic)
                self.assertTrue(chain['sourceEvidenceComplete'])
                self.assertFalse(chain['heldAtDisconnectProven'] or chain['distributedReleaseProven'])
                self.assertTrue(chain['nativeOwnerProofRequiredSeparately'])
                self.assertEqual(chain['coordinatorTimes'], sorted(chain['coordinatorTimes']))
                self.assertIn(branch + '/caller/transport/request.json', chain['originalEvidenceSha256'])
                self.assertNotEqual(chain['callerRequestId'], chain['foregroundRequestId'])
                # Prevent shared fixture patches from nesting across subtests.
                self.doCleanups()

    def test_foreign_binding_and_changed_request_digest_are_rejected(self):
        parent, binding, diagnostic = self.scene('timeout')
        foreign = {**binding, 'candidateCommit': 'f' * 40}
        with self.assertRaisesRegex(ValueError, 'changed binding'):
            source.link_fleet_evidence(parent, 'timeout', foreign, diagnostic)
        path = parent / 'timeout/caller/transport/request.json'
        intent = json.loads(path.read_text())
        intent['preparedMonotonicNs'] += 1
        path.write_text(json.dumps(intent))
        with self.assertRaisesRegex(ValueError, 'single unretried TCP'):
            source.link_fleet_evidence(parent, 'timeout', binding, diagnostic)

    def test_clock_inversion_and_invented_disconnect_ownership_are_rejected(self):
        parent, binding, diagnostic = self.scene('queued-successor')
        path = parent / 'queued-successor/queued.json'
        original = path.read_text()
        queued = json.loads(original)
        queued['observedMonotonicNs'] = 1
        path.write_text(json.dumps(queued))
        with self.assertRaisesRegex(ValueError, 'clock order'):
            source.link_fleet_evidence(parent, 'queued-successor', binding, diagnostic)
        path.write_text(original)
        path = parent / 'queued-successor/caller/observation.json'
        caller = json.loads(path.read_text())
        caller['heldAtDisconnectProven'] = True
        path.write_text(json.dumps(caller))
        with self.assertRaisesRegex(ValueError, 'invent.*ownership'):
            source.link_fleet_evidence(parent, 'queued-successor', binding, diagnostic)


class SourcePlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.args = SimpleNamespace(evidence=Path(self.tmp.name), topologies=['H1', 'H2'])
        self.calls = []
        self.failure = None
        self.proof = {'nativeOwnerAndStaleFencesPassed': True}

    def topology(self, args, candidate, inspector, topology):
        label = (args.diagnostic_branch, topology)
        self.calls.append((label, args.evidence, args.source_plan))
        return {'branch': label[0], 'topology': topology, 'diagnosticComplete': label != self.failure,
                'cleanup': 'FAIL' if label == self.failure else 'PASS',
                'sourceEvidenceChain': {'sourceEvidenceComplete': label != self.failure},
                'canonicalCasePassClaimed': False}

    def test_serial_source_plan_preserves_each_controller_and_native_scope(self):
        summary = source.run_source_plan(self.args, {}, {}, self.topology, self.proof)
        self.assertEqual([row[0] for row in self.calls], [
            ('timeout', 'H1'), ('timeout', 'H2'), ('queued-successor', 'H1'), ('queued-successor', 'H2')])
        self.assertTrue(all(row[2] for row in self.calls))
        self.assertNotEqual(self.calls[0][1], self.calls[-1][1])
        self.assertTrue(summary['sourceProceduresComplete'] and summary['diagnosticComplete'])
        self.assertEqual(summary['remainingLiveInstances'], [])
        self.assertFalse(summary['complete'] or summary['wholeCasePassClaimed'] or summary['canonicalCasePassClaimed'])
        self.assertEqual(set(summary['sourceBranches']), {
            'object-lock-timeout', 'caller-disconnect', 'stale-read-repair', 'native-lock-owner-fence'})
        self.assertEqual(summary['sourceBranches']['stale-read-repair']['scope'], 'independent-native')
        self.assertEqual(json.loads((self.args.evidence / 'healthy-summary.json').read_text()), summary)

    def test_product_or_cleanup_failure_retains_first_scene_and_never_runs_next(self):
        self.failure = ('timeout', 'H2')
        summary = source.run_source_plan(self.args, {}, {}, self.topology, self.proof)
        self.assertEqual([row[0] for row in self.calls], [('timeout', 'H1'), ('timeout', 'H2')])
        self.assertFalse(summary['sourceProceduresComplete'])
        self.assertEqual(summary['cleanup'], 'FAIL')
        self.assertEqual(summary['remainingLiveInstances'], ['timeout/H2', 'queued-successor/H1', 'queued-successor/H2'])

    def test_h1_only_cannot_claim_h2_source_procedures_complete(self):
        self.args.topologies = ['H1']
        summary = source.run_source_plan(self.args, {}, {}, self.topology, self.proof)
        self.assertTrue(summary['diagnosticComplete'])
        self.assertFalse(summary['sourceProceduresComplete'])


if __name__ == '__main__':
    unittest.main()
