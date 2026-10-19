"""Execute live-adapter control flow with modeled services; no live acceptance."""

import copy
from contextlib import ExitStack
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import network
from scripts.validation_heal.mpu_scene import keys_for
from scripts.validation_heal.network_load_repair import CANDIDATE


class NetworkAdapterTests(unittest.TestCase):
    def run_mode(self, directory, mode):
        events, state = [], {'running': False, 'recovered': None, 'session': None}
        root = Path(directory);candidate = root / 'candidate.json';binary = root / 'candidate'
        candidate.write_text(json.dumps({'source_sha': CANDIDATE, 'package_sha256': 'b' * 64}));binary.write_bytes(b'candidate')
        args = types.SimpleNamespace(evidence=root / 'evidence', candidate=candidate, rc_bin=binary,
            tool_directory=root, testing_commit='c' * 40, binary_sha='d' * 64)
        nodes = ['vm000', 'vm001', 'vm002', 'vm003']
        payload = {'size': 100, 'sha256': 'original'}
        def entry(key, version, latest):
            return {'key': key, 'versionId': version, 'deleteMarker': False, 'isLatest': latest, 'payload': payload.copy()}
        dv = [entry('objects/0001.bin', version, version == 'null') for version in ('v0', 'v1', 'v2', 'null')]
        dl_expected = [entry(f'dl/{endpoint}/control.bin', f'{endpoint}-v{i}', i == 2)
                       for endpoint in ('node1', 'node3') for i in range(3)]
        fixture = {'expected': dl_expected, 'sides': {endpoint: {'controlKey': f'dl/{endpoint}/control.bin',
            'versions': [row for row in dl_expected if row['key'] == f'dl/{endpoint}/control.bin']}
            for endpoint in ('node1', 'node3')}}
        class Fleet:
            def __init__(self, nodes, evidence, run_id, *args, **kwargs):
                self.nodes, self.evidence, self.run_id = nodes, evidence, run_id
                self.network_gates, self.quorum_gates, self.faults = set(), set(), []
                self.reserved = nodes[:]
            def record(self, label, value): events.append(('record', label));return value
            def deploy(self, *args): state['running'] = True;events.append(('deploy',))
            def start(self): state['running'] = True;events.append(('start',))
            def stop(self): state['running'] = False;events.append(('stop',))
            def snapshot(self, label, **kwargs):
                events.append(('snapshot', label, copy.deepcopy(kwargs)))
                return {node: {'files': [], 'formats': []} for node in nodes}
            def remote(self, node, action, **params):
                events.append(('remote', node, action))
                if action == 'network-preflight':
                    return {'addresses': ['10.0.0.' + str(i) for i in range(1, 5)], 'localIndex': nodes.index(node), 'exclusiveServicePort': 9000}
                if action == 'quorum-gate-probe':
                    return {'disks': [{'writable': True, 'readableMetadata': True, 'sameServiceUidAndMountNamespace': True}] * 4}
                return {'state': 'CLEAR', 'rulesRemaining': 0, 'expiryTriggered': False}
            def cleanup(self):
                assert state['session'].safe_to_cleanup
                events.append(('cleanup',));return True
        class Probe:
            def __init__(self, binding, endpoint='node1'): self.endpoint_name, self.binding = endpoint, binding
            def inventory(self, bucket, **kwargs):
                events.append(('inventory', bucket))
                rows = [] if bucket.endswith('-db') else copy.deepcopy(dv + dl_expected)
                if state['recovered'] and not bucket.endswith('-db'):
                    row = state['recovered']
                    rows.append({**entry(row['key'], row['versionId'], True), 'payload': row['payload'],
                        'properties': {'Metadata': {'heal-fixture': 'I03', 'nonce': row['key']}},
                        'tags': [{'Key': 'fixture', 'Value': 'I03'}, {'Key': 'suite', 'Value': 'heal'}]})
                return {'bucket': bucket, 'entries': rows}
            def admin(self, *args): return {'httpStatus': 200, 'response': {}}
        test = self
        class Session:
            def __init__(self, binding, bucket, endpoints, directory):
                self.binding, self.bucket, self.endpoints = binding, bucket, tuple(endpoints)
                self.keys, self.safe_to_cleanup = keys_for(endpoints), False
                self.fixture, self.stages = fixture, []
                state['session'] = self;events.append(('register-session',))
            def seed(self):
                events.append(('seed-DL',))
                if mode == 'seed-unknown': raise ValueError('original seed Create result UNKNOWN')
                return fixture
            def register_baseline(self, value):
                test.assertEqual({(r['key'], r['versionId']) for r in value['entries']},
                                 {(r['key'], r['versionId']) for r in dv + dl_expected})
                events.append(('freeze-public',))
            def register_business_baseline(self, value):
                test.assertFalse(state['running']);events.append(('freeze-business',))
            def capture_mpu(self, fleet, stage):
                test.assertFalse(state['running']);self.stages.append(stage)
                events.append(('capture-MPU', stage));return {'nodes': {n: {} for n in nodes}}
            def run_load(self, readable):
                test.assertTrue(state['running']);test.assertEqual(readable, {'node1': True, 'node3': True})
                events.append(('load-inside-partition',))
                return {'complete': mode == 'pass'}
            def registered_receipts(self): return {'original': 'preserved'}
            def reconcile(self):
                test.assertTrue(state['running']);test.assertEqual(self.stages[-1], 'pre-reconcile')
                events.append(('reconcile-original-effects',));return {'namespaceCleanupAllowed': False}
            def finish_reconciliation(self, scene, *, repair_targets):
                test.assertFalse(state['running']);test.assertEqual(self.stages[-1], 'post-reconcile')
                events.append(('close-original-effects',));self.safe_to_cleanup = True
                return {'namespaceCleanupAllowed': True}
        def map_target(snapshot, bucket, key, role, number, version, **kwargs):
            return {'bucket': bucket, 'key': key, 'versionId': version,
                'node': 'vm000', 'disk': 0, 'relativePath': bucket + '/' + key + '/part.1', 'sha256': 'original',
                'cohort': [{'node': n, 'disk': 0} for n in nodes]}
        def unique(probe, directory, bucket, key, rows):
            events.append(('unique-PUT', key));test.assertTrue(state['running'])
            if key.startswith('recovered/'):
                test.assertTrue(state['session'].safe_to_cleanup)
            row = {'key': key, 'versionId': 'new-original-ACK', 'endpoint': probe.endpoint_name, 'payload': payload.copy(),
                'observed': {'acknowledged': key.startswith('recovered/'), 'response': {'VersionId': 'new-original-ACK'}}}
            rows.append(row)
            if key.startswith('recovered/'): state['recovered'] = row
            return row
        def resolve(fleet, binding, bucket, rows):
            for row in rows: row['resolution'] = {'state': 'REDEEMED' if row['observed']['acknowledged'] else 'ABSENT'}
            return rows
        def retain(fleet, binding, keys, business, mpu):
            test.assertFalse(state['running']);test.assertFalse(fleet.network_gates or fleet.quorum_gates)
            events.append(('freeze-retention-intent',));return {'original': 'intent'}
        def isolate(fleet, plan):
            events.append(('isolate-original-inodes',))
            if mode == 'isolation-error': raise ValueError('retained bytes drifted')
            return {'cleanup': 'ISOLATED', 'originalSceneRestored': False}
        plan = {'readQuorum': 8, 'sharedLockQuorum': 2, 'exclusiveLockQuorum': 3, 'gates': {},
            'blocked': {node: [] for node in nodes}, 'sides': [
                {'endpoint': endpoint, 'members': members, 'metadataAuthorities': 8,
                 'readableSources': 8 if index == 0 else 7, 'writableDrives': 8, 'lockClients': 2}
                for index, (endpoint, members) in enumerate((('node1', [0, 1]), ('node3', [2, 3])))]}
        replacements = {'Fleet': Fleet, 'Probe': Probe, 'LoadSession': Session, 'Recorder': lambda *args: None,
            'verify_inspector': lambda *args: {}, 'verify_quorum_contract': lambda *args: {
                'candidateCommit': CANDIDATE, 'recognized': True, 'sourceFiles': network.GET_SOURCE_FILES[CANDIDATE],
                'unrecoverableGet': {'httpStatus': 503, 'code': 'SlowDownRead'}},
            'seed_versions': lambda *args: copy.deepcopy(dv), 'db_parameters': lambda *args: {'count': 128},
            'seed_db': lambda *args: [], 'verify_seed': lambda *args: {}, 'version_heads': lambda *args: [],
            'validate_topology': lambda *args: {}, 'map_target': map_target, 'map_db': lambda *args: ([], []),
            'plan_for': lambda *args: plan, 'validate_fault': lambda *args: {},
            'validate_witness': lambda *args: {}, 'expected_reads': lambda *args: [],
            'partition_get': lambda *args: ({'acknowledged': True, 'resultUnknown': False, 'payload': payload.copy()}, {'complete': True}),
            'validate_unrecoverable_get': lambda *args: None, 'unique_put': unique, 'negative_put': lambda *args: {},
            'partition_db': lambda *args: {}, 'negative_heal': lambda *args: {}, 'validate_partial': lambda *args: {},
            'classify_deletes': lambda *args: {'policies': {}, 'repairTargets': []},
            'recover_versions': lambda *args: {}, 'recover_controls': lambda *args: {}, 'idle_fleet': lambda *args: None,
            'validate_repaired_physical': lambda *args, **kwargs: {},
            'verify_cohort': lambda fleet, cohort, label: events.append(('streaming-cohort', cohort['key'], cohort['versionId'])),
            'prove_db': lambda *args: {}, 'resolve_writes': resolve, 'compare_inventories': lambda *args: {},
            'verify_db': lambda *args: {}, 'prepare_retention': retain, 'isolate_retention': isolate}
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {'GITHUB_RUN_ID': '12345', 'GITHUB_RUN_ATTEMPT': '1'}))
            for name, value in replacements.items(): stack.enter_context(patch.object(network, name, value))
            result = network.run_branch(args, nodes, 'read-boundary')
        return result, events

    def test_complete_adapter_keeps_all_dl_versions_physical_then_closes_before_new_safe_put(self):
        with tempfile.TemporaryDirectory() as d: result, events = self.run_mode(d, 'pass')
        self.assertEqual((result['verdict'], result['cleanup']), ('PASS', 'PASS'))
        cohorts = [event for event in events if event[0] == 'streaming-cohort' and event[1].startswith('dl/')]
        self.assertEqual(len(cohorts), 6)
        self.assertLess(events.index(('freeze-business',)), events.index(('load-inside-partition',)))
        self.assertLess(events.index(('load-inside-partition',)), events.index(('record', 'all-directed-peer-probes-after')))
        self.assertLess(events.index(('streaming-cohort', 'dl/node3/control.bin', 'node3-v2')),
                        events.index(('reconcile-original-effects',)))
        self.assertLess(events.index(('close-original-effects',)), events.index(('unique-PUT', 'recovered/unique.bin')))
        self.assertEqual([e for e in events if e[0] == 'cleanup'], [('cleanup',)])
        self.assertFalse(any(e[0] == 'isolate-original-inodes' for e in events))
        for event in events:
            if event[0] == 'snapshot' and event[1] != 'DB-size-calibration-live':
                self.assertEqual(len(event[2]['buckets']), 2)

    def test_unknown_seed_or_load_never_runs_ordinary_cleanup_or_retries_original_mutation(self):
        for mode in ('seed-unknown', 'unknown-load', 'isolation-error'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                result, events = self.run_mode(d, mode)
                self.assertEqual((result['verdict'], result['cleanup']), ('FAIL', 'FAIL'))
                self.assertNotIn(('cleanup',), events)
                self.assertNotIn(('unique-PUT', 'recovered/unique.bin'), events)
                self.assertNotIn(('reconcile-original-effects',), events)
                self.assertIn(('isolate-original-inodes',), events)
                if mode == 'seed-unknown':
                    self.assertFalse(any(event[0] == 'inventory' for event in events))
                if mode == 'isolation-error': self.assertIn('retentionError', result)
                else: self.assertEqual(result['retention']['cleanup'], 'ISOLATED')


if __name__ == '__main__': unittest.main()
