"""Default-class C02/C03 boundaries retain the full original DB and strict witnesses."""

from contextlib import ExitStack
import copy
import json
import hashlib
from types import SimpleNamespace
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import test_validation_heal as physical_fixtures
import test_validation_heal_network_fixtures as contract_fixtures
from scripts.validation_heal import shard_boundaries as controls, shards


class ShardBoundaryControlsTest(unittest.TestCase):
    def inputs(self, data=2):
        manifest, candidate, target, processes = contract_fixtures.NetworkBoundaryFixturesTest().parameters()
        target['logical']['erasure'].update(data_blocks=data, parity_blocks=data, distribution=list(range(1, data * 2 + 1)))
        for value in processes.values():
            value['storageClassEnvironment']['RUSTFS_STORAGE_CLASS_STANDARD'] = None
        original = [{'key': f'objects/{i:04d}.bin'} for i in range(128)]
        recorder = SimpleNamespace(bytes_written=100 * 1024**2,
            probe=SimpleNamespace(binding={'maxInventoryBytes': 512 * 1024**2, 'maxInventoryObjects': 256}))
        return manifest, candidate, target, processes, recorder, original

    def test_actual_default_class_geometry_preserves128_and_all_required_boundaries(self):
        for data, inline in ((2, 262144), (6, 262146)):
            with self.subTest(data=data):
                plan = controls.parameters(*self.inputs(data))
                self.assertEqual(plan['actualLogicalInlineLimit'], inline)
                self.assertEqual(plan['groups']['inline-minus-exact-plus'], [inline - 1, inline, inline + 1])
                self.assertEqual(plan['groups']['stripe-minus-exact-plus'], [1048575, 1048576, 1048577])
                self.assertEqual(plan['groups']['multipart-minimum-minus-exact-plus'], [5242879, 5242880, 5242881])
                self.assertEqual(plan['originalDefaultDBCount'], 128)
                self.assertFalse(plan['faultBranchMatrixChanged'])
                self.assertFalse(plan['completedMultipartProtocolClaimed'])

    def test_source_configuration_original_controls_and_resource_refusals_precede_writes(self):
        for kind in ('unknown-source', 'source-hash', 'class', 'inline-override', 'missing-original', 'bytes', 'count'):
            values = self.inputs()
            manifest, candidate, target, processes, recorder, original = values
            if kind == 'unknown-source':
                candidate['source_sha'] = 'f' * 40
                manifest['sizeContract']['candidateCommit'] = candidate['source_sha']
            elif kind == 'source-hash': manifest['sizeContract']['sourceFiles']['crates/ecstore/src/set_disk/mod.rs'] = 'f' * 64
            elif kind == 'class': processes['node0']['storageClassEnvironment']['RUSTFS_STORAGE_CLASS_STANDARD'] = 'EC:8'
            elif kind == 'inline-override': processes['node0']['storageClassEnvironment']['RUSTFS_STORAGE_CLASS_INLINE_BLOCK'] = '1MiB'
            elif kind == 'missing-original': original.pop()
            elif kind == 'bytes': recorder.probe.binding['maxInventoryBytes'] = recorder.bytes_written
            else: recorder.probe.binding['maxInventoryObjects'] = 128
            with self.subTest(kind=kind), self.assertRaises(ValueError): controls.parameters(*values)

    def test_seed_is_public_put_with_independent_payloads_and_disjoint_keys(self):
        plan = controls.parameters(*self.inputs())
        calls = []
        def call(operation, params, body):
            calls.append((operation, params, body))
            return {'ETag': 'ack'}
        expected = controls.seed_controls(SimpleNamespace(call=call), 'owned', plan)
        self.assertEqual(len(expected), plan['supplementalObjectCount'])
        self.assertEqual(sum(len(body) for _, _, body in calls), plan['supplementalPayloadBytes'])
        for entry, (operation, params, body) in zip(expected, calls):
            self.assertEqual(operation, 'put_object')
            self.assertEqual(params['Key'], entry['key'])
            self.assertTrue(entry['key'].startswith('boundaries/'))
            self.assertEqual(entry['payload'], {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()})

    def scene(self):
        original = physical_fixtures.HealShardOracleTest().snapshot()
        bucket = 'rv-heal-0123456789abcdef'
        size = 1048577
        key = f'boundaries/0000-{size}.bin'
        for row in original['node1']['files']:
            row['path'] = row['path'].replace('objects/0008.bin', key)
            if 'metadata' in row:
                row['metadata']['key'] = key
                version = row['metadata']['versions'][0]
                version['size'] = size
                version['parts'][0].update(size=size, actualSize=size)
        plan = {'groups': {'stripe-plus': [size]}, 'supplementalObjectCount': 1,
                'actualLogicalInlineLimit': 262144, 'actualDataBlocks': 2, 'actualECBlockSize': 1048576}
        expected = [{'key': key, 'versionId': 'null', 'isLatest': True, 'deleteMarker': False, 'payload': {'size': size}}]
        return original, bucket, expected, plan

    def test_real_decoded_full_set_mapping_rejects_missing_parts_changed_shapes_and_ack(self):
        snapshot, bucket, expected, plan = self.scene()
        targets = controls.map_controls(snapshot, bucket, expected, plan)
        self.assertEqual((targets[0]['shape'], len(targets[0]['target']['cohort'])), ('external', 4))
        for kind in ('missing-part', 'size', 'geometry', 'key', 'version', 'duplicate'):
            value, ack, parameters = copy.deepcopy(snapshot), copy.deepcopy(expected), copy.deepcopy(plan)
            if kind == 'missing-part': value['node1']['files'] = [row for row in value['node1']['files'] if not row['path'].endswith('part.1')]
            elif kind == 'size': ack[0]['payload']['size'] -= 1
            elif kind == 'geometry': parameters['actualDataBlocks'] = 6
            elif kind == 'key': ack[0]['key'] += '-other'
            elif kind == 'version': ack[0]['versionId'] = 'other'
            else: ack += copy.deepcopy(ack)
            with self.subTest(kind=kind), self.assertRaises(ValueError): controls.map_controls(value, bucket, ack, parameters)

    def test_original_byte_proof_refuses_different_surviving_shard(self):
        snapshot, bucket, expected, plan = self.scene()
        targets = controls.map_controls(snapshot, bucket, expected, plan)
        fleet = SimpleNamespace(record=lambda label, value: value)
        def remote(node, action, **params):
            shard = next(row for row in targets[0]['target']['cohort'] if row['disk'] == params['disk'])
            return {'sha256': shard['sha256'], 'physicalSize': shard['size']}
        fleet.remote = remote
        self.assertTrue(controls.prove_controls(fleet, targets, 'proof')[0]['physical']['sameRoleOriginalShardBytesRestored'])
        fleet.remote = lambda *args, **kwargs: {'sha256': 'changed', 'physicalSize': 82}
        with self.assertRaises(ValueError): controls.prove_controls(fleet, targets, 'proof')


    def test_real_c02_and_c03_callers_require_calibrated_controls_before_any_fault(self):
        class ReachedControls(BaseException):
            pass
        for case in ("heal.data-shard-bitrot", "heal.parity-shard-bitrot"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                root = Path(directory)
                candidate = root / "candidate.json"
                candidate.write_text(json.dumps({"source_sha": "1c7fc818dd858247c45b39542abd8291b1e6e31b", "package_sha256": "a" * 64}))
                rc = root / "rc"
                rc.write_bytes(b"client")
                args = SimpleNamespace(evidence=root, candidate=candidate, rc_bin=rc, tool_directory=root,
                    testing_commit="b" * 40, binary_sha="c" * 64, case_id=case)
                records = {}
                def record(label, value):
                    records[label] = value
                    return value
                fleet = SimpleNamespace(record=record, deploy=lambda *args: None,
                    snapshot=lambda label: {"calibration": label}, cleanup=lambda: True)
                stack.enter_context(patch.dict('os.environ', {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}))
                stack.enter_context(patch.object(shards, 'Fleet', return_value=fleet))
                stack.enter_context(patch.object(shards, 'Probe', return_value=None))
                stack.enter_context(patch.object(shards, 'Recorder', return_value="recorder"))
                stack.enter_context(patch.object(shards, 'verify_inspector', return_value="exact-manifest"))
                stack.enter_context(patch.object(shards, 'validate_topology', return_value={}))
                def seed(recorder, bucket, fixture, count, sizes, **kwargs):
                    return [{'key': f'objects/{i:04d}.bin'} for i in range(128)] if fixture == 'DB' else []
                stack.enter_context(patch.object(shards, 'seed', side_effect=seed))
                stack.enter_context(patch.object(shards, 'map_target', return_value="actual-target"))
                plan = stack.enter_context(patch.object(controls, 'parameters', return_value="source-size-plan"))
                seeded = stack.enter_context(patch.object(controls, 'seed_controls', side_effect=ReachedControls))
                with self.assertRaises(ReachedControls): shards.run_topology(args, 'H1', ['vm000'])
                self.assertEqual(plan.call_args.args[:3], ("exact-manifest", json.loads(candidate.read_text()), "actual-target"))
                self.assertEqual(plan.call_args.args[3], {"calibration": "DB-size-calibration-live"})
                self.assertEqual(plan.call_args.args[4], "recorder")
                self.assertEqual(len(plan.call_args.args[5]), 128)
                self.assertEqual(seeded.call_args.args[2], "source-size-plan")
                self.assertEqual(records["DB-source-size-plan"], "source-size-plan")


if __name__ == '__main__':
    unittest.main()
