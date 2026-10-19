"""Network source fixture completeness, real boundaries and fixed resource caps."""

import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_heal import network, network_fixtures as fixtures
from scripts.validation_heal.boundaries import boundary_sizes
from scripts.validation_heal.healthy import Fleet
from scripts.validation_heal.fleet_node import snapshot_buckets


class NetworkBoundaryFixturesTest(unittest.TestCase):
    def parameters(self):
        source = '1c7fc818dd858247c45b39542abd8291b1e6e31b'
        files = {name: 'a' * 64 for name in ('crates/ecstore/src/config/storageclass.rs',
            'crates/ecstore/src/set_disk/mod.rs', 'crates/ecstore/src/set_disk/ops/object.rs',
            'crates/ecstore/src/erasure/coding/erasure.rs')}
        files['crates/ecstore/src/set_disk/mod.rs'] = fixtures.MPU_SOURCE[source]
        manifest = {'sizeContract': {'recognized': True, 'candidateCommit': source,
            'extractorSourceSha256': hashlib.sha256(Path('scripts/validation_heal/prepare_heal_meta_inspector.py').read_bytes()).hexdigest(),
            'inlineRule': 'ceil(size/dataBlocks)<=effectiveInlineBlock AND size<=actualECBlock',
            'effectiveDefaultRule': 'min(ceil(2*defaultInlineBlock/dataBlocks),defaultInlineBlock)',
            'defaultInlineBlock': 128 * 1024, 'defaultObjectBudgetFactor': 2, 'sourceFiles': files}}
        target = {'logical': {'usesLegacyChecksum': False, 'erasure': {
            'data_blocks': 8, 'parity_blocks': 8, 'block_size': 1024**2, 'distribution': list(range(1, 17))}}}
        processes = {f'node{i}': {'storageClassEnvironment': {
            'RUSTFS_STORAGE_CLASS_INLINE_BLOCK': None, 'RUSTFS_STORAGE_CLASS_STANDARD': 'EC:8'}} for i in range(4)}
        candidate = {'source_sha': source}
        return manifest, candidate, target, processes

    def test_actual_ec8_size_contract_keeps_all_boundaries_and_default128(self):
        manifest, candidate, target, processes = self.parameters()
        parameters = fixtures.db_parameters(manifest, candidate, target, processes)
        self.assertEqual(parameters['objectCount'], 128)
        self.assertEqual(parameters['actualLogicalInlineLimit'], 262144)
        self.assertEqual(parameters['groups']['inline-minus-exact-plus'], [262143, 262144, 262145])
        self.assertEqual(parameters['groups']['stripe-minus-exact-plus'], [1048575, 1048576, 1048577])
        self.assertEqual(parameters['groups']['multipart-minimum-minus-exact-plus'], [5242879, 5242880, 5242881])
        self.assertTrue({0, 1, 4095, 4096, 4097} <= set(parameters['sizeCycle']))
        self.assertLess(parameters['payloadBytes'], 512 * 1024**2)
        self.assertFalse(parameters['completedMultipartProtocolClaimed'])

    def test_missing_environment_wrong_geometry_or_foreign_source_refuses_calibration(self):
        for kind in ('default', 'foreign-class', 'inline-override', 'geometry', 'distribution', 'source-hash', 'candidate'):
            manifest, candidate, target, processes = self.parameters()
            if kind == 'default': processes['node0']['storageClassEnvironment']['RUSTFS_STORAGE_CLASS_STANDARD'] = None
            elif kind == 'foreign-class': processes['node0']['storageClassEnvironment']['RUSTFS_STORAGE_CLASS_STANDARD'] = 'EC:4'
            elif kind == 'inline-override': processes['node0']['storageClassEnvironment']['RUSTFS_STORAGE_CLASS_INLINE_BLOCK'] = '1MiB'
            elif kind == 'geometry': target['logical']['erasure']['parity_blocks'] = 4
            elif kind == 'distribution': target['logical']['erasure']['distribution'].pop()
            elif kind == 'source-hash': manifest['sizeContract']['sourceFiles']['crates/ecstore/src/set_disk/mod.rs'] = 'f' * 64
            else:
                candidate['source_sha'] = 'f' * 40
                manifest['sizeContract']['candidateCommit'] = 'f' * 40
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                fixtures.db_parameters(manifest, candidate, target, processes)

    def test_default_boundary_callers_cannot_silently_use_ec8_environment(self):
        manifest, candidate, target, processes = self.parameters()
        with self.assertRaises(ValueError): boundary_sizes(manifest['sizeContract'], target, processes)
        for value in ('EC:4', True, 'EC:16'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                boundary_sizes(manifest['sizeContract'], target, processes, standard_class=value)

    def test_seed_uses_real_db128_cycle_and_refuses_budget_before_first_call(self):
        params = fixtures.db_parameters(*self.parameters())
        calls = []
        def call(operation, values, body=None):
            calls.append((operation, values, len(body) if body is not None else None))
            return {'ETag': 'ack'}
        recorder = SimpleNamespace(probe=SimpleNamespace(binding={'bucketPrefix': 'rv-heal-owned',
            'maxInventoryBytes': 512 * 1024**2, 'maxInventoryObjects': 128}), call=call)
        expected = fixtures.seed_db(recorder, 'rv-heal-owned-db', params)
        self.assertEqual(len(expected), 128)
        self.assertEqual(calls[0][0], 'create_bucket')
        self.assertEqual(sum(c[2] for c in calls if c[0] == 'put_object'), params['payloadBytes'])
        self.assertEqual({x['payload']['size'] for x in expected}, set(params['sizeCycle']))
        calls.clear()
        recorder.probe.binding['maxInventoryBytes'] = params['payloadBytes'] - 1
        with self.assertRaises(ValueError): fixtures.seed_db(recorder, 'rv-heal-owned-db', params)
        self.assertFalse(calls)

    def test_each128_control_is_shape_mapped_and_all_unique_sizes_have_representatives(self):
        params = fixtures.db_parameters(*self.parameters())
        expected = [{'key': f'objects/{i:04d}.bin', 'versionId': 'null', 'isLatest': True,
            'deleteMarker': False, 'payload': {'size': params['sizeCycle'][i % len(params['sizeCycle'])]}} for i in range(128)]
        mapped = []
        def target(entry, shape):
            mapped.append((entry['key'], shape))
            return {'cohort': [{}] * 16, 'logical': {'size': entry['payload']['size'], 'erasure': {'data_blocks': 8, 'parity_blocks': 8}}}
        def external(snapshot, bucket, key, role, part):
            entry = next(x for x in expected if x['key'] == key)
            self.assertGreater(entry['payload']['size'], params['actualLogicalInlineLimit'])
            return target(entry, 'external')
        def inline(snapshot, bucket, entry, *, allow_empty):
            self.assertEqual(allow_empty, entry['payload']['size'] == 0)
            self.assertLessEqual(entry['payload']['size'], params['actualLogicalInlineLimit'])
            return target(entry, 'zero' if allow_empty else 'inline')
        with patch.object(fixtures, 'map_target', side_effect=external), patch.object(fixtures, 'map_inline_target', side_effect=inline):
            representatives, witnessed = fixtures.map_db({}, 'owned-db', expected, params)
        self.assertEqual((len(mapped), len(witnessed)), (128, 128))
        self.assertEqual({r['expected']['payload']['size'] for r in representatives}, set(params['sizeCycle']))
        self.assertEqual(set(shape for key, shape in mapped), {'zero', 'inline', 'external'})
        for wrong in (expected[:-1], [expected[0]] + expected[:-1]):
            with self.assertRaises(ValueError): fixtures.map_db({}, 'owned-db', wrong, params)

    def test_full_physical_snapshot_explicitly_includes_the_owned_db_bucket(self):
        fleet = Fleet(['node1', 'node2'], Path('/unused'), 'gh-12345-1-h2')
        owner = {'bucket': 'rv-heal-owned'}
        def remote(node, action, **params):
            self.assertEqual(action, 'snapshot')
            return {'buckets': snapshot_buckets(owner, params), 'quiescent': params['quiescent']}
        with patch.object(fleet, 'record', side_effect=lambda label, value: value), patch.object(fleet, 'remote', side_effect=remote) as call:
            current = fleet.snapshot('baseline', quiescent=True, buckets=['rv-heal-owned', 'rv-heal-owned-db'])
            self.assertEqual(set(current), {'node1', 'node2'})
            self.assertTrue(all(value == {'buckets': ['rv-heal-owned', 'rv-heal-owned-db'], 'quiescent': True}
                                for value in current.values()))
            self.assertEqual(call.call_count, 2)
            default = fleet.snapshot('legacy-default')
            self.assertTrue(all(value['buckets'] == ['rv-heal-owned'] for value in default.values()))
            for buckets in (['rv-heal-owned-db'], ['rv-heal-owned', 'foreign-bucket']):
                with self.assertRaises(ValueError): fleet.snapshot('refuse-escape', buckets=buckets)

    def test_control_read_requires_full_payload_or_exact_source_bound_lock_refusal(self):
        source = '1c7fc818dd858247c45b39542abd8291b1e6e31b'
        contract = {'candidateCommit': source, 'recognized': True,
            'sourceFiles': {'rustfs/src/error.rs': network.GET_SOURCE_FILES[source]['rustfs/src/error.rs']},
            'unrecoverableGet': {'httpStatus': 503, 'code': 'SlowDownRead'}}
        row = {'expected': {'key': 'control', 'versionId': 'null', 'payload': {'size': 1, 'sha256': 'digest'}},
               'target': {'logical': {'erasure': {'data_blocks': 1}}}, 'shape': 'inline'}
        good = {'acknowledged': True, 'resultUnknown': False, 'payload': row['expected']['payload'],
                'response': {'ResponseMetadata': {'HTTPStatusCode': 200}}}
        error = {'acknowledged': False, 'resultUnknown': False,
                 'response': {'ResponseMetadata': {'HTTPStatusCode': 503}, 'Error': {'Code': 'ServiceUnavailable'}}}
        for clients, observed, okay in [(2, good, True), (1, error, True), (1, good, False),
                                        (2, {**good, 'payload': {'size': 0}}, False), (2, {**good, 'resultUnknown': True}, False)]:
            with self.subTest(clients=clients, observation=observed), patch.object(network, 'partition_get', return_value=(observed, {'complete': True})):
                args = (None, None, 'owned', [row], {'lockClients': clients, 'metadataAuthorities': 4}, {'sharedLockQuorum': 2}, contract)
                if okay: self.assertEqual(len(network.partition_db(*args)), 1)
                else:
                    with self.assertRaises(ValueError): network.partition_db(*args)


if __name__ == '__main__':
    unittest.main()
