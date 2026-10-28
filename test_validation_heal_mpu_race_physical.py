"""Real read-only G05 filesystem census and cross-host continuation oracles."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
import uuid

from scripts.validation_heal import fleet_node, mpu_race_physical as scene
from scripts.validation_heal.healthy import Fleet


class MultipartRacePhysicalTests(unittest.TestCase):
    def test_actual_remote_adapter_embeds_the_local_module(self):
        def execute(*args, **kwargs):
            source = kwargs['input'].rsplit('\nmain(', 1)[0]
            scope = {'__name__': '__g05_node_test__'}
            exec(compile(source, 'g05-node', 'exec'), scope)
            self.assertTrue(callable(scope['mpu_race_backend'].run))
            self.assertEqual(scope['mpu_race_backend'].KEY, scene.KEY)
            return SimpleNamespace(returncode=0, stdout='{}', stderr='')
        with tempfile.TemporaryDirectory() as d, patch('scripts.validation_heal.healthy.subprocess.run', side_effect=execute):
            Fleet(['vm000'], Path(d), 'gh-12345-1-h1').remote('vm000', 'mpu-race-scene')

    def fixture(self, root, *, live=False):
        root = root.resolve()
        state = root / 'state'; state.mkdir()
        binary = b'exact owned G05 candidate'
        (state / 'rustfs').write_bytes(binary)
        owner = {'runtimeProfile': 'manual', 'bucket': 'rv-heal-0123456789abcdef',
                 'binarySha256': hashlib.sha256(binary).hexdigest()}
        items = []
        for i, role in enumerate(scene.ROLES):
            raw = f'{uuid.UUID(int=9)}.{uuid.UUID(int=i+1)}x123456'.encode()
            items.append({'role': role, 'uploadId': base64.urlsafe_b64encode(raw).decode().rstrip('='),
                          'bucket': owner['bucket'], 'key': scene.KEY})
        selected = scene.selected(owner['bucket'], items)
        roots, raw = tuple(root / f'disk-{i}' for i in range(4)), {}
        for disk, folder in enumerate(roots):
            (folder / 'data').mkdir(parents=True)
            for item in selected:
                if item['role'] == 'committed': continue
                upload = folder / 'data' / item['relative']
                upload.mkdir(parents=True)
                p = upload / 'xl.meta'; p.write_bytes(b'owned pending metadata'); raw[p] = p.read_bytes()
                for number in range(1, 3 if item['role'] == 'a' else 4):
                    part = upload / str(uuid.UUID(int=100 if item['role'] == 'a' else 200)) / f'part.{number}'
                    part.parent.mkdir(exist_ok=True)
                    body = (f'{item["role"]}/{number}/{disk}'.encode() * 20)
                    part.write_bytes(body); raw[part] = body
                    sidecar = part.with_name(part.name + '.meta')
                    sidecar.write_bytes(b'original part ACK metadata'); raw[sidecar] = sidecar.read_bytes()
        (state / 'credentials.env').write_bytes(b'never export this secret')
        process = {k: k for k in fleet_node.PROCESS_FIELDS}
        process.update(pid=1234, binarySha256=owner['binarySha256'])
        def stopped(value):
            if live: raise ValueError('owned service active')
        node = {'owned': lambda run: (state, roots, owner), 'checked': fleet_node.checked,
                'digest': fleet_node.digest, 'stopped': stopped,
                'process': lambda request: copy.deepcopy(process), 'PROCESS_FIELDS': fleet_node.PROCESS_FIELDS}
        request = {'runId': 'gh-12345-1-h1', 'action': 'mpu-race-scene', 'bucket': owner['bucket'],
                   'items': items, 'expectedBinarySha256': owner['binarySha256'], 'quiescent': not live}
        manifest = {'bucket': owner['bucket'], 'key': scene.KEY, 'items': {
            item['role']: {'uploadId': item['uploadId'], 'state': 'complete' if item['role'] == 'committed' else 'pending',
                           'parts': [{'number': n} for n in range(1, 3 if item['role'] == 'a' else 4)]}
            for item in items}}
        return request, node, roots, raw, manifest

    def test_real_census_hashes_only_three_ack_selected_scopes_and_preserves_all_bytes(self):
        for live in (False, True):
            with self.subTest(live=live), tempfile.TemporaryDirectory() as d:
                request, node, roots, raw, manifest = self.fixture(Path(d), live=live)
                real_open, opened = os.open, []
                def checked_open(path, *args, **kwargs):
                    self.assertIn(Path(path), raw); opened.append(Path(path))
                    return real_open(path, *args, **kwargs)
                with patch.object(scene.os, 'open', side_effect=checked_open):
                    value = scene.run(request, node)
                self.assertEqual(len(value['rows']), 12)
                self.assertEqual(len(opened), 48)
                self.assertEqual(value['bytes'], sum(len(body) for body in raw.values()))
                self.assertEqual(value['quiescent'], not live)
                self.assertEqual({p: p.read_bytes() for p in raw}, raw)
                self.assertNotIn('never export', json.dumps(value))
                proof = scene.verify(manifest, {'vm000': value}, ['vm000'], request['expectedBinarySha256'])
                self.assertEqual(proof['pendingAndTerminalWitnesses'], 12)
                self.assertFalse(proof['codingLayoutProved'] or proof['canonicalCasePassClaimed'])

    def test_cross_host_sibling_and_terminal_continuity_requires_every_original_drive(self):
        with tempfile.TemporaryDirectory() as d:
            request, node, roots, raw, manifest = self.fixture(Path(d))
            value = scene.run(request, node)
            nodes = [f'vm00{i}' for i in range(4)]
            baseline = {n: copy.deepcopy(value) for n in nodes}
            continued = copy.deepcopy(baseline)
            manifest['items']['a']['parts'].append({'number': 3})
            for n, observed in continued.items():
                for row in observed['rows']:
                    if row['role'] != 'a': continue
                    for suffix in ['', '.meta']:
                        body = f'A/3/{n}/{row["disk"]}/{suffix}'.encode()
                        row['files'][f'{uuid.UUID(int=100)}/part.3{suffix}'] = {
                            'size': 64 if not suffix else len(body), 'sha256': hashlib.sha256(body).hexdigest()}
            scene.verify(manifest, continued, nodes, request['expectedBinarySha256'], previous=baseline)
            committed, completed = {n: {'files': []} for n in nodes}, copy.deepcopy(continued)
            manifest['items']['a']['state'] = 'complete'; manifest['items']['b']['state'] = 'aborted'
            for n, observed in completed.items():
                for prefix in observed['keyPrefixes']: prefix['children'] = []
                for row in observed['rows']:
                    if row['role'] == 'a':
                        for path, entry in row['files'].items():
                            if path.endswith(('/part.1', '/part.2', '/part.3')):
                                committed[n]['files'].append({'disk': row['disk'], 'path': manifest['bucket']+'/'+scene.KEY+'/'+path, **entry})
                    row.update(exists=False, files={}, directories=[])
            proof = scene.verify(manifest, completed, nodes, request['expectedBinarySha256'], previous=continued, committed=committed)
            self.assertEqual(proof['completedMembersCompared'], 16)
            for fault in ('host', 'drive', 'duplicate', 'retired', 'candidate', 'completed-bytes', 'unlisted-upload'):
                broken, live = copy.deepcopy(completed), copy.deepcopy(committed)
                if fault == 'host': broken.pop('vm003')
                elif fault == 'drive': broken['vm000']['rows'].pop()
                elif fault == 'duplicate': broken['vm000']['rows'][-1] = copy.deepcopy(broken['vm000']['rows'][0])
                elif fault == 'retired': broken['vm000']['rows'][1]['exists'] = True
                elif fault == 'candidate': broken['vm000']['binarySha256'] = 'f'*64
                elif fault == 'unlisted-upload': broken['vm000']['keyPrefixes'][0]['children'].append('unknown-upload')
                else: live['vm000']['files'][0]['sha256'] = 'f'*64
                with self.subTest(fault=fault), self.assertRaises(ValueError):
                    scene.verify(manifest, broken, nodes, request['expectedBinarySha256'], previous=continued, committed=live)

    def test_scope_escape_links_budgets_and_changed_process_are_rejected(self):
        for fault in ('role', 'bucket', 'runtime', 'binary', 'unknown-field', 'upload-path',
                      'symlink', 'hardlink', 'fifo', 'entries', 'bytes', 'process', 'unlisted-upload'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as d:
                request, node, roots, raw, manifest = self.fixture(Path(d), live=fault=='process')
                part = next(p for p in raw if p.name=='part.1')
                if fault=='role': request['items'][1]['role']='committed'
                elif fault=='bucket': request['bucket']='rv-heal-ffffffffffffffff'
                elif fault=='runtime': node['owned'](request['runId'])[2]['runtimeProfile']='lock-quorum'
                elif fault=='binary': request['expectedBinarySha256']='f'*64
                elif fault=='unknown-field': request['path']='/etc'
                elif fault=='upload-path': request['items'][1]['uploadId']='../outside'
                elif fault=='symlink': part.unlink(); part.symlink_to(Path(d).resolve()/'state/credentials.env')
                elif fault=='hardlink': os.link(part,part.with_name('extra-link'))
                elif fault=='fifo': part.unlink();os.mkfifo(part)
                elif fault=='unlisted-upload':
                    selected=scene.selected(request['bucket'],request['items'])
                    (roots[0]/'data'/selected[0]['relative']).parent.joinpath('unknown-upload').mkdir()
                elif fault=='process':
                    first=node['process'](request);second={**first,'startTicks':'another birth'}
                    node['process']=Mock(side_effect=[first,second])
                with patch.object(scene,'MAX_ENTRIES',1 if fault=='entries' else scene.MAX_ENTRIES), \
                     patch.object(scene,'MAX_BYTES',1 if fault=='bytes' else scene.MAX_BYTES):
                    with self.assertRaises(ValueError): scene.run(request,node)


if __name__ == '__main__':
    unittest.main()
