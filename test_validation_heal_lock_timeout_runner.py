"""I05 deployment boundary and retained failure scenes through real Recorder."""

import copy
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from scripts.validation_heal import lock_timeout_runner as runner
from test_validation_heal_lock_observer import identity


class TimeoutRunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.rc = self.root / 'rc'; self.rc.write_bytes(b'pinned-client')
        self.candidate = {'source_sha': runner.CANDIDATE, 'package_sha256': 'a' * 64}
        self.inspector = {'candidateCommit': runner.CANDIDATE, 'binarySha256': 'b' * 64}
        self.candidate_file = self.root / 'candidate.json'
        self.candidate_file.write_text(json.dumps(self.candidate))
        self.args = SimpleNamespace(candidate=self.candidate_file, testing_commit='c' * 40,
            binary_sha='d' * 64, tool_directory=self.root / 'tools', rc_bin=self.rc,
            evidence=self.root / 'evidence', topologies=['H1', 'H2'])
        self.events, self.fleets, self.entries = [], [], []
        self.change = None
        owner = self
        class Probe:
            def __init__(self, binding, endpoint=None, *, credential_pair=None):
                self.binding = binding; self.endpoint_name = endpoint or binding['defaultEndpoint']
                self.credential_pair = credential_pair
            def s3(self, operation, params, body=None):
                owner.events.append(operation)
                response = {'ResponseMetadata': {'HTTPStatusCode': 200}}
                if operation == 'create_bucket': owner.entries = []
                if operation == 'put_object':
                    version = str(uuid.uuid4()); response['VersionId'] = version
                    for row in owner.entries:
                        if row['key'] == params['Key']: row['isLatest'] = False
                    owner.entries.append({'key': params['Key'], 'versionId': version,
                        'isLatest': True, 'deleteMarker': False,
                        'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}})
                if operation == 'delete_object':
                    version = str(uuid.uuid4()); response.update(VersionId=version, DeleteMarker=True)
                    for row in owner.entries:
                        if row['key'] == params['Key']: row['isLatest'] = False
                    owner.entries.append({'key': params['Key'], 'versionId': version,
                        'isLatest': True, 'deleteMarker': True})
                return {'acknowledged': True, 'resultUnknown': False, 'response': response}
            def inventory(self, bucket):
                owner.events.append('inventory-' + self.endpoint_name)
                if owner.change == 'peer-error' and 'diagnostic' in owner.events:
                    raise ValueError('incomplete peer inventory')
                return {'bucket': bucket, 'entries': copy.deepcopy(owner.entries)}
        class Fleet:
            def __init__(self, nodes, evidence, run_id, *args, **kwargs):
                self.nodes = nodes; self.evidence = evidence; self.run_id = run_id
                self.runtime_profile = kwargs.get('runtime_profile')
                self.secret_values = set(); self.reserved = []; self.sequence = 0
                self.stopped = False
                owner.fleets.append(self)
            def record(self, label, value):
                self.sequence += 1
                runner.save(self.evidence / f'{self.sequence:05d}-{label}.json', value)
                return value
            def deploy(self, *args):
                owner.events.append('deploy'); self.reserved.extend(self.nodes)
                if owner.change == 'reserve-error': raise ValueError('partial reservation')
            def snapshot(self, label, **kwargs):
                owner.events.append(label)
                if kwargs.get('quiescent'):
                    owner.assertTrue(self.stopped)
                changed = ((owner.change == 'stopped-drift' and label == 'I05-successor-stopped-before-integrity')
                           or (owner.change == 'integrity-drift' and label == 'I05-successor-stopped-after-integrity'))
                return {node: {'files': ['changed'] if changed else [], 'formats': []} for node in self.nodes}
            def remote(self, node, action):
                owner.events.append(action + '-' + node)
                if action == 'process-metrics':
                    owner.assertFalse(self.stopped)
                    return {'identity': identity(), 'fileDescriptors': 11 if owner.change == 'fd-leak' else 10}
                return {'journal': 'original log'}
            def stop(self):
                owner.events.append('stop'); self.stopped = True
            def cleanup(self): owner.events.append('cleanup'); return owner.change != 'cleanup-fail'
        def diagnostic(probe, fleet, bucket, entry, output):
            owner.events.append('diagnostic')
            self.assertFalse(entry['isLatest'] or entry['deleteMarker'])
            self.assertEqual(entry['payload']['size'], 2048)
            if owner.change == 'diagnostic-error': raise ValueError('original timeout not observed')
            if owner.change == 'secret-error': raise ValueError(str(probe.credential_pair))
            if owner.change == 'sigterm': signal.raise_signal(signal.SIGTERM)
            return {'diagnosticComplete': True, 'wholeCasePassClaimed': False,
                'canonicalCasePassClaimed': False, 'callerObservation': {'baselineResources': {
                    node: {'identity': identity(), 'fileDescriptors': 10} for node in fleet.nodes}}}
        for name, value in [('Fleet', Fleet), ('Probe', Probe), ('observe_timeout', diagnostic),
                            ('MIN_BACKPRESSURE_BYTES', 2048), ('validate_topology', lambda *a: {'topology': a[1]})]:
            p = patch.object(runner, name, value); p.start(); self.addCleanup(p.stop)
        p = patch.dict(os.environ, {'GITHUB_RUN_ID': '12345', 'GITHUB_RUN_ATTEMPT': '1',
                                   'RUSTFS_NODES': ' '.join(runner.NODES)})
        p.start(); self.addCleanup(p.stop)

    def invoke(self, topology='H1'):
        return runner.run_topology(self.args, self.candidate, self.inspector, topology)

    def test_real_recorder_seeds_history_then_checks_every_peer_before_cleanup(self):
        result = self.invoke('H2')
        self.assertTrue(result['diagnosticComplete'])
        self.assertEqual(result['cleanup'], 'PASS')
        self.assertFalse(result['canonicalCasePassClaimed'] or result['wholeCasePassClaimed'])
        self.assertEqual(self.fleets[0].nodes, list(runner.NODES))
        requests = sorted((self.args.evidence / 'H2/fixture').glob('*-request.json'))
        acks = sorted((self.args.evidence / 'H2/fixture').glob('*-ack.json'))
        self.assertEqual(len(requests), 9)
        self.assertEqual(len(acks), 9)
        for request, ack in zip(requests, acks):
            self.assertEqual(json.loads(ack.read_text())['requestSha256'], hashlib.sha256(request.read_bytes()).hexdigest())
        self.assertEqual(sum(not e['deleteMarker'] for e in self.entries), 6)
        self.assertEqual(sum(e['deleteMarker'] for e in self.entries), 1)
        for endpoint in ('node1', 'node2', 'node3', 'node4'):
            self.assertIn('inventory-' + endpoint, self.events)
        self.assertLess(self.events.index('all-peer-verification-before-public-reads'),
                        self.events.index('inventory-node2'))
        self.assertLess(self.events.index('physical-after-all-peer-reads'), self.events.index('cleanup'))

    def test_unreviewed_source_or_mismatched_tool_refuses_before_fleet_creation(self):
        self.inspector['candidateCommit'] = 'e' * 40
        with self.assertRaises(ValueError): self.invoke()
        self.assertFalse(self.fleets or self.events)

    def test_failure_keeps_original_journal_stopped_scene_and_skips_cleanup(self):
        for change in ('diagnostic-error', 'reserve-error', 'peer-error'):
            with self.subTest(change=change):
                self.args.evidence = self.root / change; self.change = change
                self.events.clear()
                result = self.invoke()
                self.assertFalse(result['diagnosticComplete'])
                self.assertEqual(result['cleanup'], 'FAIL')
                self.assertTrue(result['namespacesRetained'])
                self.assertNotIn('cleanup', self.events)
                self.assertLess(self.events.index('stop'), self.events.index('original-failure-scene'))
                self.assertIn('logs-vm000', self.events)
                if change != 'reserve-error':
                    self.assertEqual(len(list((self.args.evidence / 'H1/fixture').glob('*-ack.json'))), 9)

    def test_post_peer_fd_leak_retains_scene_and_never_cleans(self):
        self.change = 'fd-leak'
        clock = [0]
        def advance(_): clock[0] += runner.DRAIN_SECONDS + 1
        with patch.object(runner.time, 'monotonic', side_effect=lambda: clock[0]), \
                patch.object(runner.time, 'sleep', side_effect=advance):
            result = self.invoke()
        self.assertFalse(result['diagnosticComplete'])
        self.assertIn('exceeded its budget', result['failure']['message'])
        self.assertNotIn('cleanup', self.events)

    def test_serial_driver_stops_before_h2_after_dirty_h1_and_retains_summary(self):
        self.change = 'diagnostic-error'
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
            summary = runner.run(self.args)
        self.assertFalse(summary['diagnosticComplete'] or summary['canonicalCasePassClaimed'])
        self.assertEqual([row['topology'] for row in summary['diagnostics']], ['H1'])
        self.assertFalse((self.args.evidence / 'H2').exists())
        self.assertTrue((self.args.evidence / 'timeout-diagnostic-summary.json').is_file())

    def test_cleanup_failure_blocks_next_topology_even_after_complete_diagnostic(self):
        self.change = 'cleanup-fail'
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
            result = runner.run(self.args)
        self.assertFalse(result['diagnosticComplete'])
        self.assertEqual(len(result['diagnostics']), 1)
        self.assertEqual(result['diagnostics'][0]['cleanup'], 'FAIL')

    def test_preflight_checks_exact_source_helper_tools_and_workflow_before_remote_use(self):
        with patch.object(runner.subprocess, 'run', side_effect=[SimpleNamespace(stdout='c' * 40 + '\n'),
                                                               SimpleNamespace(stdout='')]), \
                patch.object(runner, 'verify_inspector', return_value=self.inspector):
            self.assertEqual(runner.preflight(self.args), (self.candidate, self.inspector))
            self.candidate_file.write_text(json.dumps({**self.candidate, 'source_sha': 'e' * 40}))
            with self.assertRaisesRegex(ValueError, 'exact reviewed'): runner.preflight(self.args)
        self.assertFalse(self.fleets or self.events)

    def test_source_plan_runs_four_isolated_instances_through_real_recorder(self):
        self.args.diagnostic_branch = 'source-plan'
        proof = {'nativeOwnerAndStaleFencesPassed': True}
        def link(directory, branch, binding, diagnostic):
            self.assertEqual(directory, self.fleets[-1].evidence)
            self.assertEqual(binding['runId'], self.fleets[-1].run_id)
            self.assertTrue(list((directory / 'fixture').glob('*-ack.json')))
            self.events.append('source-chain')
            return {'sourceEvidenceComplete': True}
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)), \
                patch.object(runner.lock_source, 'verify_source_native', return_value=proof), \
                patch.object(runner, 'verify_native_prerequisite', return_value=proof), \
                patch.object(runner, 'observe_successor', side_effect=self.successor), \
                patch.object(runner, 'map_target', side_effect=self.new_target), \
                patch.object(runner, 'verify_cohort', side_effect=self.integrity), \
                patch.object(runner.lock_source, 'link_fleet_evidence', side_effect=link) as chain:
            result = runner.run(self.args)
        self.assertTrue(result['sourceProceduresComplete'])
        self.assertFalse(result['canonicalCasePassClaimed'] or result['wholeCasePassClaimed'])
        self.assertEqual(chain.call_count, 4)
        self.assertEqual([fleet.runtime_profile for fleet in self.fleets],
                         ['lock-timeout'] * 2 + ['lock-successor'] * 2)
        self.assertEqual(len({fleet.run_id for fleet in self.fleets}), 4)
        self.assertEqual({fleet.evidence.relative_to(self.args.evidence).as_posix() for fleet in self.fleets},
            {'source-plan/timeout/H1', 'source-plan/timeout/H2',
             'source-plan/queued-successor/H1', 'source-plan/queued-successor/H2'})
        bindings = [json.loads(next(fleet.evidence.glob('*-binding.json')).read_text()) for fleet in self.fleets]
        self.assertEqual(len({binding['bucketPrefix'] for binding in bindings}), 4)
        self.assertEqual([event for event in self.events if event in ('source-chain', 'cleanup')],
                         ['source-chain', 'cleanup'] * 4)

    def test_source_chain_failure_preserves_original_scene_before_cleanup(self):
        self.args.diagnostic_branch = 'source-plan'
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)), \
                patch.object(runner.lock_source, 'verify_source_native', return_value={}), \
                patch.object(runner.lock_source, 'link_fleet_evidence', side_effect=ValueError('broken original ACK')):
            result = runner.run(self.args)
        self.assertFalse(result['sourceProceduresComplete'])
        self.assertEqual(len(self.fleets), 1)
        self.assertIn('broken original ACK', result['diagnostics'][0]['failure']['message'])
        self.assertTrue(result['diagnostics'][0]['namespacesRetained'])
        self.assertNotIn('cleanup', self.events)
        self.assertIn('original-failure-scene', self.events)
        self.assertTrue(list(self.fleets[0].evidence.glob('*-original-failure-logs.json')))

    def test_source_plan_requires_native_before_deployment_and_evidence_creation(self):
        self.args.diagnostic_branch = 'source-plan'
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
            with self.assertRaisesRegex(ValueError, 'requires exact-source native evidence'):
                runner.run(self.args)
        self.assertFalse(self.fleets or self.events or self.args.evidence.exists())

    def test_cli_defaults_to_complete_source_plan(self):
        argv = self.cli()
        start = argv.index('--diagnostic-branch')
        del argv[start:start + 2]
        with patch.dict(os.environ, {}, clear=True), patch.object(runner.sys, 'argv', argv), \
                patch.object(runner, 'run', return_value={'diagnosticComplete': True}) as run:
            self.assertEqual(runner.main(), 0)
        self.assertEqual(run.call_args.args[0].diagnostic_branch, 'source-plan')

    def test_queued_successor_preflight_requires_exact_candidate_native_group(self):
        self.args.diagnostic_branch = 'queued-successor'
        self.args.native_directory = self.root / 'native'
        proof = {'requiredRegressionLabels': list(runner.NATIVE_I05_LABELS),
                 'requiredNativeSubsetPassed': True, 'wholeCasePassClaimed': False,
                 'fleetExecutionClaimed': False, 'canonicalCasePassClaimed': False}
        with patch.object(runner.subprocess, 'run', side_effect=[SimpleNamespace(stdout='c' * 40 + '\n'),
                SimpleNamespace(stdout='')]), patch.object(runner, 'verify_inspector', return_value=self.inspector), \
                patch.object(runner, 'verify_native', return_value=proof) as verify:
            self.assertEqual(runner.preflight(self.args), (self.candidate, self.inspector))
            verify.assert_called_once_with(self.args.native_directory, self.candidate,
                                           self.args.testing_commit, runner.CASE_ID)
        self.assertFalse(self.fleets or self.events)

    def test_queued_successor_without_native_artifact_refuses_before_fleet_or_evidence(self):
        self.args.diagnostic_branch = 'queued-successor'
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
            with self.assertRaisesRegex(ValueError, 'immutable exact-source native evidence'):
                runner.run(self.args)
        self.assertFalse(self.fleets or self.events)
        self.assertFalse(self.args.evidence.exists())

    def test_queued_successor_driver_binds_runtime_new_ack_and_every_peer(self):
        self.args.diagnostic_branch = 'queued-successor'
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)), \
                patch.object(runner, 'verify_native_prerequisite', return_value={'native': True}) as native, \
                patch.object(runner, 'observe_successor', side_effect=self.successor), \
                patch.object(runner, 'map_target', side_effect=self.new_target), \
                patch.object(runner, 'verify_cohort', side_effect=self.integrity):
            summary = runner.run(self.args)
        self.assertTrue(summary['diagnosticComplete'])
        self.assertFalse(summary['canonicalCasePassClaimed'] or summary['wholeCasePassClaimed'])
        self.assertEqual(native.call_count, 3)
        self.assertEqual([f.runtime_profile for f in self.fleets], ['lock-successor'] * 2)
        self.assertEqual(self.events.count('successor'), 2)
        self.assertNotIn('diagnostic', self.events)
        proof = json.loads((self.args.evidence / 'native-prerequisite.json').read_text())
        self.assertEqual(proof, {'native': True})
        for fleet in self.fleets:
            peers = list(fleet.evidence.glob('*-all-peer-ACK-proof.json'))
            self.assertEqual(len(peers), len(fleet.nodes))
            self.assertTrue(all(json.loads(p.read_text())['seedAckAndFullGetAgree'] for p in peers))
            integrity = list(fleet.evidence.glob('*-I05-successor-new-shard-integrity.json'))
            self.assertEqual(len(integrity), 1)
            self.assertTrue(json.loads(integrity[0].read_text())['allShardBitrotVerified'])
        for start, end in ((0, self.events.index('cleanup') + 1),
                           (self.events.index('cleanup') + 1, len(self.events))):
            events = self.events[start:end]
            self.assertLess(events.index('physical-after-all-peer-reads'), events.index('stop'))
            self.assertLess(events.index('process-metrics-vm000'), events.index('stop'))
            self.assertLess(events.index('stop'), events.index('I05-successor-stopped-before-integrity'))
            self.assertLess(events.index('I05-successor-stopped-before-integrity'), events.index('verify-integrity'))
            self.assertLess(events.index('verify-integrity'), events.index('I05-successor-stopped-after-integrity'))
            self.assertLess(events.index('I05-successor-stopped-after-integrity'), events.index('cleanup'))

    def successor(self, probe, fleet, bucket, entry, output):
        self.events.append('successor')
        probe.s3('put_object', {'Bucket': bucket, 'Key': entry['key']}, b'new-generation')
        return {'diagnosticComplete': True, 'canonicalCasePassClaimed': False,
            'wholeCasePassClaimed': False, 'expectedEntries': copy.deepcopy(self.entries),
            'verifiedEndpoints': list(probe.binding['endpoints']),
            'callerObservation': {'baselineResources': {node: {
                'identity': identity(), 'fileDescriptors': 10} for node in fleet.nodes}}}

    def new_target(self, snapshot, bucket, key, role, part, version):
        self.assertEqual(set(snapshot), set(self.fleets[-1].nodes))
        self.assertTrue(self.fleets[-1].stopped)
        added = next(item for item in self.entries if item['versionId'] == version)
        self.assertEqual((added['key'], role, part), (key, 'data', 1))
        self.assertTrue(added['isLatest'])
        self.assertFalse(added['deleteMarker'])
        return {'versionId': version}

    def integrity(self, fleet, target, label):
        self.assertTrue(fleet.stopped)
        self.assertEqual(label, 'I05-successor-shard')
        self.events.append('verify-integrity')
        if self.change == 'integrity-error':
            raise ValueError('corrupt acknowledged successor shard')
        return {'allShardBitrotVerified': True}

    def test_successor_stopped_drift_or_checksum_failure_retains_scene_before_cleanup(self):
        self.args.diagnostic_branch = 'queued-successor'
        for change, message in (('stopped-drift', 'stopping the service changed'),
                                ('integrity-error', 'corrupt acknowledged successor shard'),
                                ('integrity-drift', 'streaming verification changed')):
            with self.subTest(change=change):
                self.change = change; self.events.clear()
                self.args.evidence = self.root / change
                with patch.object(runner, 'verify_native_prerequisite'), \
                        patch.object(runner, 'observe_successor', side_effect=self.successor), \
                        patch.object(runner, 'map_target', side_effect=self.new_target), \
                        patch.object(runner, 'verify_cohort', side_effect=self.integrity), \
                        patch.object(runner, 'seal_successor_scene', return_value={'sealed': True}) as seal:
                    result = self.invoke()
                self.assertFalse(result['diagnosticComplete'])
                self.assertEqual(result['cleanup'], 'FAIL')
                self.assertIn(message, result['failure']['message'])
                self.assertTrue(result['namespacesRetained'])
                self.assertNotIn('cleanup', self.events)
                self.assertIn('original-failure-scene', self.events)
                seal.assert_called_once()

    def test_successor_unknown_outcome_seals_only_after_stop_scene_logs_and_result(self):
        self.args.diagnostic_branch = 'queued-successor'
        def seal(folder, binding, scene):
            self.assertIn('stop', self.events)
            self.assertIn('original-failure-scene', self.events)
            self.assertIn('logs-vm000', self.events)
            result = list(folder.glob('*-diagnostic-result.json'))
            self.assertEqual(len(result), 1)
            self.assertTrue(json.loads(result[0].read_text())['namespacesRetained'])
            return {'sealed': True}
        with patch.object(runner, 'verify_native_prerequisite'), \
                patch.object(runner, 'observe_successor', side_effect=ValueError('unknown PUT result')), \
                patch.object(runner, 'seal_successor_scene', side_effect=seal) as sealed:
            result = self.invoke()
        self.assertFalse(result['diagnosticComplete'])
        self.assertEqual(result['cleanup'], 'FAIL')
        self.assertNotIn('cleanup', self.events)
        sealed.assert_called_once()
        self.assertEqual(json.loads((self.args.evidence / 'H1/successor-retention.json').read_text()),
                         {'sealed': True})

    def test_successor_failed_stop_never_seals_incomplete_scene(self):
        self.args.diagnostic_branch = 'queued-successor'
        with patch.object(runner, 'verify_native_prerequisite'), \
                patch.object(runner, 'observe_successor', side_effect=ValueError('unknown PUT result')), \
                patch.object(runner.Fleet, 'stop', side_effect=ValueError('stop failed')), \
                patch.object(runner, 'seal_successor_scene') as sealed:
            result = self.invoke()
        self.assertIn('failureCaptureError', result)
        sealed.assert_not_called()
        self.assertNotIn('cleanup', self.events)

    def test_uncommitted_or_untracked_controller_refuses_before_inspector_and_fleet(self):
        with patch.object(runner.subprocess, 'run', side_effect=[SimpleNamespace(stdout='c' * 40 + '\n'),
                SimpleNamespace(stdout=' M scripts/validation_heal/lock_timeout.py')]), \
                patch.object(runner, 'verify_inspector', side_effect=AssertionError('no inspector read')):
            with self.assertRaisesRegex(ValueError, 'immutable helper'): runner.preflight(self.args)
        self.assertFalse(self.fleets or self.events)

    def test_in_memory_credentials_cannot_escape_into_failure_or_serial_summary(self):
        self.change = 'secret-error'
        with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
            result = runner.run(self.args)
        self.assertIn('[REDACTED]', result['diagnostics'][0]['failure']['message'])
        for value in self.fleets[0].secret_values:
            for evidence in self.args.evidence.rglob('*.json'):
                self.assertNotIn(value, evidence.read_text())

    def cli(self):
        return ['lock_timeout_runner.py', '--candidate', str(self.candidate_file), '--case-id', runner.CASE_ID,
                '--testing-commit', self.args.testing_commit, '--binary-sha', self.args.binary_sha,
                '--tool-directory', str(self.args.tool_directory), '--rc-bin', str(self.rc),
                '--evidence', str(self.args.evidence), '--diagnostic-branch', 'timeout']

    def test_actual_cli_accepts_wrapper_directory_and_keeps_canonical_summary_blocked(self):
        self.args.evidence.mkdir()
        for name in ('timeout-preflight.json', 'setup.log', 'test.log'):
            (self.args.evidence / name).write_text('wrapper evidence')
        with patch.object(runner.sys, 'argv', self.cli()), patch.object(runner.signal, 'signal'), \
                patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
            self.assertEqual(runner.main(), 0)
        summary = json.loads((self.args.evidence / 'healthy-summary.json').read_text())
        self.assertTrue(summary['diagnosticComplete'])
        self.assertFalse(summary['complete'] or summary['canonicalCasePassClaimed'] or summary['wholeCasePassClaimed'])
        self.assertEqual(summary['cleanup'], 'PASS')
        self.assertEqual([row['topology'] for row in summary['diagnostics']], ['H1', 'H2'])
        self.assertEqual((self.args.evidence / 'setup.log').read_text(), 'wrapper evidence')

    def test_old_topology_or_summary_refuses_before_any_fleet_deployment(self):
        self.args.evidence.mkdir()
        for name in ('H1', 'H2', 'healthy-summary.json', 'timeout-diagnostic-summary.json'):
            with self.subTest(name=name):
                path = self.args.evidence / name; path.write_text('original evidence')
                with patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
                    with self.assertRaisesRegex(ValueError, 'fresh topology'): runner.run(self.args)
                self.assertFalse(self.events or self.fleets)
                self.assertEqual(path.read_text(), 'original evidence')
                path.unlink()

    def test_actual_sigterm_retains_h1_and_stops_before_h2(self):
        self.change = 'sigterm'
        self.addCleanup(signal.signal, signal.SIGTERM, signal.getsignal(signal.SIGTERM))
        with patch.object(runner.sys, 'argv', self.cli()), \
                patch.object(runner, 'preflight', return_value=(self.candidate, self.inspector)):
            self.assertEqual(runner.main(), 1)
        summary = json.loads((self.args.evidence / 'healthy-summary.json').read_text())
        self.assertEqual(len(summary['diagnostics']), 1)
        self.assertEqual(summary['diagnostics'][0]['failure']['type'], 'KeyboardInterrupt')
        self.assertTrue(summary['diagnostics'][0]['namespacesRetained'])
        self.assertNotIn('cleanup', self.events)
        self.assertIn('original-failure-scene', self.events)

    def test_foreign_node_binding_refuses_before_tools_or_fleet(self):
        with patch.dict(os.environ, {'RUSTFS_NODES': 'other000 other001 other002 other003'}), \
                patch.object(runner.subprocess, 'run', side_effect=[SimpleNamespace(stdout='c' * 40),
                    SimpleNamespace(stdout='')]), patch.object(runner, 'verify_inspector') as inspector:
            with self.assertRaisesRegex(ValueError, 'node binding'): runner.preflight(self.args)
        inspector.assert_not_called()
        self.assertFalse(self.events or self.fleets)

    def test_real_shell_diagnostic_verdict_never_certifies_the_canonical_case(self):
        root = Path(__file__).resolve().parent
        dry = subprocess.run(['bash', str(root / 'scripts/validation_heal/run_case.sh'),
            '--validation-case', runner.CASE_ID, '--dry-run'], capture_output=True, text=True)
        self.assertEqual(dry.returncode, 0, dry.stderr)
        shell = (root / 'scripts/validation_heal/run_case.sh').read_text()
        self.assertLess(shell.index(' preflight', shell.index('if [ "${driver}" = lock_timeout_runner.py')),
                        shell.index('(bootstrap)'))
        verdict = shell[shell.index('verdict=FAIL reason='):shell.index('args=("${ROOT}/scripts/validation_result.py"')]
        self.args.evidence.mkdir()
        path = self.args.evidence / 'healthy-summary.json'
        for code, complete, cleanup, expected in ((0, True, 'PASS', 'BLOCKED'), (1, True, 'PASS', 'FAIL'),
                (0, False, 'PASS', 'INFRA_ERROR'), (0, True, 'FAIL', 'INFRA_ERROR'),
                (1, False, 'FAIL', 'FAIL'), (0, None, 'PASS', 'INFRA_ERROR')):
            if complete is None: path.unlink()
            else: path.write_text(json.dumps(dict(diagnosticComplete=complete, complete=False,
                                                 canonicalCasePassClaimed=False)))
            checked = subprocess.run(['bash', '-euc', verdict + '\nprintf "%s" "$verdict"\n'],
                capture_output=True, text=True, env={**os.environ, 'case_id': runner.CASE_ID,
                    'cleanup_status': cleanup, 'case_rc': str(code), 'evidence': str(self.args.evidence)})
            self.assertEqual(checked.returncode, 0, checked.stderr)
            self.assertEqual(checked.stdout, expected)


if __name__ == '__main__':
    unittest.main()
