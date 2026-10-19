"""Exact stopped MPU census; read-only fixture checks do not certify EC/bitrot."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import mpu_scene as scene, fleet_node


class MultipartSceneTests(unittest.TestCase):
    def fixture(self, root, *, running=False):
        root = root.resolve()
        state = root / 'state';state.mkdir()
        binary = b'exact candidate fixture';(state / 'rustfs').write_bytes(binary)
        owner = {'runtimeProfile': 'lock-quorum', 'bucket': 'rv-heal-owned',
                 'binarySha256': hashlib.sha256(binary).hexdigest(), 'runId': 'gh-12345-1-h2'}
        roots = tuple(root / f'disk-{i}' for i in range(4))
        keys = scene.keys_for(['node1', 'node3'])
        raw = {}
        for disk, folder in enumerate(roots):
            data = folder / 'data';data.mkdir(parents=True)
            for key in keys:
                if key.endswith('/create.bin'): continue
                digest = hashlib.sha256(f"{owner['bucket']}/{key}".encode()).hexdigest()
                upload = data / '.rustfs.sys/multipart' / digest / f'opaque-upload-{disk}'
                upload.mkdir(parents=True)
                for name, body in [('xl.meta', f'original metadata {key}'.encode()), ('part.1', b'original full part' + bytes([disk]))]:
                    p = upload / name;p.write_bytes(body)
                    raw[p] = body
            # Another key, state credentials and another system namespace are
            # deliberately unreadable to the census's digest/open paths.
            (data / '.rustfs.sys/config').mkdir()
            (data / '.rustfs.sys/config/credentials').write_bytes(b'out of scope secret')
        (state / 'credentials.env').write_bytes(b'state secret')
        def stopped(value):
            if running: raise ValueError('owned process still active')
        node = {'owned': lambda run_id: (state, roots, owner), 'stopped': stopped,
                'checked': fleet_node.checked, 'digest': fleet_node.digest}
        request = {'runId': owner['runId'], 'action': 'mpu-scene', 'bucket': owner['bucket'],
                   'keys': keys, 'expectedBinarySha256': owner['binarySha256']}
        return request, node, roots, raw

    def test_census_covers_exact_eight_key_prefixes_all_four_drives_without_reading_credentials(self):
        with tempfile.TemporaryDirectory() as d:
            request, node, roots, raw = self.fixture(Path(d))
            opened, real_open = [], os.open
            def checked_open(path, *args, **kwargs):
                opened.append(Path(path))
                self.assertIn(Path(path), raw)
                return real_open(path, *args, **kwargs)
            with patch.object(scene.os, 'open', side_effect=checked_open): value = scene.run(request, node)
            self.assertEqual(len(value['scopes']), 32)
            self.assertEqual(len(value['files']), 48)
            self.assertEqual(len(opened), 48)
            self.assertEqual(len([r for r in value['scopes'] if not r['present']]), 8)
            self.assertEqual(value['bytesHashed'], sum(len(b) for b in raw.values()))
            self.assertTrue(value['quiescent'] and value['readOnly'])
            self.assertFalse(value['rawContentsExported'] or value['payloadIntegrityClaimed'])
            expected = {(disk, str(p.relative_to(root / 'data'))): hashlib.sha256(body).hexdigest()
                        for disk, root in enumerate(roots) for p, body in raw.items() if p.is_relative_to(root)}
            self.assertEqual({(r['disk'], r['path']): r['sha256'] for r in value['files']}, expected)
            self.assertNotIn('out of scope secret', json.dumps(value))
            self.assertEqual({p: p.read_bytes() for p in raw}, raw)

    def test_capture_distinguishes_partial_refused_abort_and_empty_directories(self):
        with tempfile.TemporaryDirectory() as d:
            request, node, roots, raw = self.fixture(Path(d));before = scene.run(request, node)
            chosen = next(p for p in raw if '/abort.bin' in str(raw[p]) and p.name == 'xl.meta')
            chosen.unlink()
            after = scene.run(request, node)
            self.assertNotEqual(before['sceneSha256'], after['sceneSha256'])
            self.assertEqual(len(before['files']) - len(after['files']), 1)
            self.assertEqual(before['directories'], after['directories'])
            self.assertFalse(after['payloadIntegrityClaimed'])

    def test_live_process_wrong_binary_bucket_runtime_or_key_matrix_refuses_before_file_reads(self):
        for kind in ('live', 'binary', 'bucket', 'runtime', 'foreign-key', 'missing-key', 'duplicate-key', 'extra-field'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                request, node, roots, raw = self.fixture(Path(d), running=kind == 'live')
                if kind == 'binary': request['expectedBinarySha256'] = 'f' * 64
                elif kind == 'bucket': request['bucket'] = 'rv-heal-foreign'
                elif kind == 'runtime': node['owned'](request['runId'])[2]['runtimeProfile'] = 'manual'
                elif kind == 'foreign-key': request['keys'][0] = '../credentials'
                elif kind == 'missing-key': request['keys'].pop()
                elif kind == 'duplicate-key': request['keys'][0] = request['keys'][1]
                elif kind == 'extra-field': request['path'] = '/etc'
                with patch.object(scene.os, 'open', side_effect=AssertionError('census read outside validated scope')):
                    with self.assertRaises(ValueError): scene.run(request, node)

    def test_symlinks_hardlinks_special_files_and_budget_overflow_are_not_hashed(self):
        for kind in ('symlink', 'hardlink', 'fifo', 'file-cap', 'byte-cap'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                request, node, roots, raw = self.fixture(Path(d));part = next(p for p in raw if p.name == 'part.1')
                if kind == 'symlink': part.unlink();part.symlink_to(Path(d).resolve() / 'state/credentials.env')
                elif kind == 'hardlink': os.link(part, part.with_name('extra-link'))
                elif kind == 'fifo': part.unlink();os.mkfifo(part)
                with patch.object(scene, 'MAX_FILES', 1 if kind == 'file-cap' else scene.MAX_FILES), \
                     patch.object(scene, 'MAX_BYTES', 1 if kind == 'byte-cap' else scene.MAX_BYTES):
                    with self.assertRaises(ValueError): scene.run(request, node)

    def test_changed_owner_or_process_during_capture_keeps_scene_unaccepted(self):
        with tempfile.TemporaryDirectory() as d:
            request, node, roots, raw = self.fixture(Path(d));original = node['owned'];calls = []
            def changing(run_id):
                value = original(run_id);calls.append(run_id)
                if len(calls) > 1: return value[0], value[1], {**value[2], 'binarySha256': 'f' * 64}
                return value
            node['owned'] = changing
            with self.assertRaisesRegex(ValueError, 'owner or original roots changed'): scene.run(request, node)
            self.assertEqual(len(calls), 2)


if __name__ == '__main__':
    unittest.main()
