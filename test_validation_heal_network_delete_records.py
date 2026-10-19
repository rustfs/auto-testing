"""Real stopped fixture journal bytes bind safe selectors without exporting metadata."""

import copy
from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import network_delete_records as records
from scripts.validation_heal.healthy import Fleet
import test_validation_heal_mpu_scene as mpu_tests


class DeleteRecordTests(unittest.TestCase):
    def scene(self, directory):
        request, node, roots, ignored = mpu_tests.MultipartSceneTests().fixture(Path(directory))
        keys = ['dl/node1/control.bin', 'dl/node3/control.bin']
        request.update(action='network-delete-records', keys=keys)
        transaction = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
        versions = [f'12345678-1234-1234-1234-{i + 1:012d}' for i in range(3)]
        dirs = [f'22345678-1234-1234-1234-{i + 1:012d}' for i in range(3)]
        paths = []
        for root in roots:
            for key in keys:
                folder = root / 'data' / request['bucket'] / key / transaction;folder.mkdir(parents=True)
                value = {'protocol_version': 1, 'volume': request['bucket'], 'object': key, 'transaction': transaction,
                    'bucket_incarnation': '33333333-3333-3333-3333-333333333333',
                    'original_digest': [1] * 32, 'target_digest': [2] * 32,
                    'data_dirs': dirs[:1], 'deleted_versions': versions[:1],
                    'requests': [{'name': key, 'version_id': versions[0], 'deleted': False, 'mark_deleted': False,
                                  'metadata': {'fixture-private-value': 'must-not-be-exported'}}],
                    'original_versions': [{'version_id': v, 'data_dir': d, 'deleted': False, 'metadata_digest': [3] * 32}
                                          for v, d in zip(versions, dirs)], 'predecessors': []}
                path = folder / 'delete-prepared';path.write_text(json.dumps(value));paths.append(path)
                (folder / 'do-not-read').write_bytes(b'private payload')
        return request, node, roots, paths

    def test_all_four_original_disks_two_keys_raw_shas_are_retained_without_exporting_nested_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            request, node, roots, paths = self.scene(directory)
            before = {p: p.read_bytes() for p in paths};opened, real_open = [], os.open
            def checked(path, *args, **kwargs):
                self.assertIn(Path(path), paths);opened.append(Path(path));return real_open(path, *args, **kwargs)
            with patch.object(records.os, 'open', side_effect=checked): value = records.run(request, node)
            self.assertEqual(len(value['records']), 8)
            self.assertEqual(set(opened), set(paths))
            for row in value['records']:
                path = roots[row['disk']] / 'data' / row['path']
                self.assertEqual(row['sha256'], hashlib.sha256(before[path]).hexdigest())
                self.assertEqual(row['size'], len(before[path]))
                self.assertEqual(row['originalMetadataSha256'], '01' * 32)
                self.assertEqual(row['targetMetadataSha256'], '02' * 32)
            self.assertNotIn('must-not-be-exported', json.dumps(value))
            self.assertTrue(value['readOnly'] and value['quiescent'])
            self.assertFalse(value['commitOrRollbackClaimed'] or value['rawContentsExported'] or value['credentialContentsRead'])
            self.assertEqual({p: p.read_bytes() for p in paths}, before)

    def test_wrong_scope_schema_digest_selector_type_duplicate_fields_links_and_live_service_refuse(self):
        for kind in ('bucket', 'keys', 'runtime', 'live', 'schema', 'duplicate-json', 'digest', 'extra-field', 'symlink', 'hardlink', 'oversize'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                request, node, roots, paths = self.scene(directory);path = paths[0]
                if kind == 'bucket': request['bucket'] = 'foreign'
                elif kind == 'keys': request['keys'][1] = 'dl/node2/control.bin'
                elif kind == 'runtime': node['owned'](request['runId'])[2]['runtimeProfile'] = 'manual'
                elif kind == 'live': node['stopped'] = lambda owner: (_ for _ in ()).throw(ValueError('running'))
                elif kind == 'symlink': path.unlink();path.symlink_to(Path(directory).resolve() / 'state/credentials.env')
                elif kind == 'hardlink': os.link(path, path.with_name('extra-link'))
                elif kind == 'oversize': path.write_bytes(b'x' * (1024**2 + 1))
                else:
                    value = json.loads(path.read_text())
                    if kind == 'schema': value['protocol_version'] = True
                    elif kind == 'digest': value['original_digest'][0] = True
                    elif kind == 'extra-field': value['unknown'] = 'unbound'
                    if kind == 'duplicate-json': path.write_text(json.dumps(value)[:-1] + ',"protocol_version":1}')
                    else: path.write_text(json.dumps(value))
                with self.assertRaises(ValueError): records.run(request, node)

    def test_owner_change_after_read_refuses_original_scene_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            request, node, roots, paths = self.scene(directory);original = node['owned'];calls = []
            def changed(run_id):
                state, roots, owner = original(run_id);calls.append(run_id)
                return state, roots, owner if len(calls) == 1 else {**owner, 'bucket': 'foreign'}
            node['owned'] = changed
            with self.assertRaisesRegex(ValueError, 'owner/roots changed'): records.run(request, node)

    def test_actual_remote_source_injection_and_action_dispatch_use_the_read_only_backend(self):
        with tempfile.TemporaryDirectory() as directory:
            request, node, roots, paths = self.scene(directory)
            def execute(*args, **kwargs):
                source = kwargs['input'].rsplit('\nmain(', 1)[0]
                scope = {'__name__': '__owned_node_test__'}
                exec(compile(source, 'network-delete-node', 'exec'), scope)
                scope.update(node)
                out = io.StringIO()
                with redirect_stdout(out), patch.object(scope['os'], 'geteuid', return_value=0):
                    scope['main'](request)
                return types.SimpleNamespace(returncode=0, stdout=out.getvalue(), stderr='')
            with patch('scripts.validation_heal.healthy.subprocess.run', side_effect=execute):
                result = Fleet(['vm000'], Path(directory), request['runId'], runtime_profile='lock-quorum').remote(
                    'vm000', 'network-delete-records', bucket=request['bucket'], keys=request['keys'],
                    expectedBinarySha256=request['expectedBinarySha256'])
            self.assertEqual(len(result['records']), 8)
            self.assertFalse(result['credentialContentsRead'] or result['commitOrRollbackClaimed'])


if __name__ == '__main__': unittest.main()
