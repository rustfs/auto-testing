"""Candidate selection must bind the real executors before any fleet mutation."""

from contextlib import ExitStack
import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import network, quorum_boundary, write_quorum


ORIGINAL = '072b57b37559fbe626aeda8abf61617fdcbdd926'
FIXED = '1c7fc818dd858247c45b39542abd8291b1e6e31b'
DIAGNOSTIC = '85c706cac09d53d0649487b8f1865f18beb53225'
RPC_FIXED = '62ff96774fa4139d45f14d70b70519aae8070ac0'


class RecordedSourceContract(Exception):
    def __init__(self, records):
        self.records = copy.deepcopy(records)
        super().__init__("source contract exported")


class QuorumCandidateTests(unittest.TestCase):
    def execute_until_contract(self, module, source, *, allow_inspector=True):
        records = {}
        def record(label, value):
            records[label] = value
            if label in {'quorum-source-contract', 'network-source-contract'}:
                raise RecordedSourceContract(records)
            return value
        fleet = types.SimpleNamespace(record=record)
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root = Path(temporary)
            candidate = root / 'candidate.json'
            candidate.write_text(json.dumps({'source_sha': source, 'package_sha256': 'a' * 64}))
            rc = root / 'rc'
            rc.write_bytes(b'client')
            args = types.SimpleNamespace(evidence=root / 'evidence', candidate=candidate,
                rc_bin=rc, tool_directory=root, testing_commit='b' * 40, binary_sha='c' * 64)
            stack.enter_context(patch.dict('os.environ', {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}))
            stack.enter_context(patch.object(module, 'Fleet', return_value=fleet))
            stack.enter_context(patch.object(module, 'verify_inspector', return_value={},
                side_effect=None if allow_inspector else AssertionError('unreviewed candidate reached inspector')))
            stack.enter_context(patch.object(module, 'verify_quorum_contract', return_value={
                'recognized': True, 'writeQuorumRule': 'data-plus-one-if-equal-parity'}))
            # Reaching the contract is the last allowed action; no deploy method
            # exists on this fleet and no Probe is constructed by these tests.
            if module is quorum_boundary:
                module.run_topology(args, 'H1', ['vm000'])
            else:
                module.run_branch(args, ['vm000', 'vm001', 'vm002', 'vm003'], network.BRANCHES[0])
        return records

    def test_actual_executors_record_the_selected_candidate_before_deploy(self):
        for module in (quorum_boundary, network):
            for source in (ORIGINAL, FIXED, DIAGNOSTIC, RPC_FIXED):
                with self.subTest(executor=module.CASE_ID, source=source):
                    with self.assertRaises(RecordedSourceContract) as completed:
                        self.execute_until_contract(module, source)
                    captured = completed.exception.records
                    reference = captured['quorum-source-contract']['locks'] if module is quorum_boundary else captured['network-source-contract']
                    self.assertEqual(reference['candidateCommit'], source)
                    if module is network:
                        self.assertEqual(reference['lock']['candidateCommit'], source)
                        self.assertEqual(reference['storage']['candidateCommit'], source)
                    self.assertEqual(captured['binding']['candidateCommit'], source)

    def test_unknown_and_malformed_sources_never_reach_inspector_or_deploy(self):
        for module in (quorum_boundary, network):
            for source in ('0' * 40, FIXED[:7], FIXED.upper(), RPC_FIXED[:7], RPC_FIXED.upper(),
                           RPC_FIXED[:-1] + '1', None, [], True):
                with self.subTest(executor=module.CASE_ID, source=source):
                    with self.assertRaises(ValueError):
                        self.execute_until_contract(module, source, allow_inspector=False)

    def test_network_storage_review_is_separate_from_lock_review(self):
        source = 'f' * 40
        with patch.dict(quorum_boundary.LOCK_REFERENCES, {source: {
                **quorum_boundary.LOCK_REFERENCE, 'candidateCommit': source}}):
            with self.assertRaises(ValueError):
                network.source_reference(source)

    def test_reference_exports_cannot_mutate_original_or_other_candidate(self):
        original = copy.deepcopy(quorum_boundary.LOCK_REFERENCE)
        fixed = quorum_boundary.lock_reference(FIXED)
        fixed['sourceFiles'].clear()
        fixed['candidateCommit'] = ORIGINAL
        reference = network.source_reference(FIXED)
        reference['storage']['sourceFiles'].clear()
        reference['lock']['sourceFiles'].clear()
        self.assertTrue(quorum_boundary.lock_reference(FIXED)['sourceFiles'])
        self.assertTrue(network.source_reference(FIXED)['storage']['sourceFiles'])
        self.assertEqual(quorum_boundary.LOCK_REFERENCE, original)
        self.assertEqual(write_quorum.LOCK_REFERENCE['candidateCommit'], ORIGINAL)
        self.assertEqual(write_quorum.STORAGE_REFERENCE['candidateCommit'], ORIGINAL)


if __name__ == '__main__':
    unittest.main()
