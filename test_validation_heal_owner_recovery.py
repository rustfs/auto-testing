"""Owner drift never authorizes a business write or bypasses archived byte checks."""
from contextlib import ExitStack, contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import socket
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import fleet_node, heal_fault, healthy, owner_recovery as recovery, retained_scene
from test_validation_heal import versioned_scene


class OwnerRecoveryTests(unittest.TestCase):
    @contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            base = Path(directory).resolve(); run = 'gh-12345-1-h1'
            state = base / 'state' / run; state.mkdir(parents=True, mode=0o700)
            mounts = [base / f'disk{i}' for i in range(4)]
            roots = [p / 'heal-validation' / run for p in mounts]
            before, bucket, _ = versioned_scene(['objects/0000.bin', 'opaque/source.bin'], sizes=(100,))
            before = before['node1']; before.update(pid=0, quiescent=True)
            owner = dict(runId=run, hostname=socket.gethostname(), unit='rustfs-validation-heal-' + run + '.service',
                runtimeProfile='outcome-legacy', bucket=bucket, binarySha256=hashlib.sha256(b'candidate').hexdigest(),
                inspectorSha256=hashlib.sha256(b'inspector').hexdigest(), mounts=[dict(
                    target=str(p), source=str(i), fstype='xfs', uuid=str(i), **{'maj:min': str(i)}) for i, p in enumerate(mounts)])
            before['binarySha256'] = owner['binarySha256']
            (state / 'owner.json').write_text(json.dumps(owner)); (state / 'rustfs').write_bytes(b'candidate')
            (state / 'heal-meta-inspect').write_bytes(b'inspector'); (state / 'service.env').write_bytes(b'credential-not-to-read')
            markers = {}
            for disk, root in enumerate(roots):
                root.mkdir(parents=True, mode=0o700); (root / 'quarantine').mkdir()
                (root / 'owner.json').write_text(json.dumps(owner))
                info = (root / 'owner.json').stat(); markers[(info.st_dev, info.st_ino)] = (999, 988)
                for entry in (e for e in before['files'] if e['disk'] == disk):
                    path = root / 'data' / entry['path']; path.parent.mkdir(parents=True, exist_ok=True)
                    raw = json.dumps(entry.get('metadata', {'shard': disk})).encode()
                    path.write_bytes(raw); entry.update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
                path = root / 'data/.rustfs.sys/format.json'; path.parent.mkdir(parents=True)
                raw = json.dumps(before['formats'][disk]['value']).encode(); path.write_bytes(raw)
                before['formats'][disk]['sha256'] = hashlib.sha256(raw).hexdigest()
            stack.enter_context(patch.object(heal_fault, 'mount_id', return_value=1))
            faults = []
            for disk in (0, 1):
                with heal_fault.directory(roots[disk]) as fd:
                    binding = dict(schemaVersion=1, runId=run, bucketPrefix=bucket, mountId=1,
                        device=os.fstat(fd).st_dev, leaseExpiresAt='2020-01-01T00:00:00+00:00')
                    heal_fault.new_file(fd, 'fault-owner.json', heal_fault.encode(binding))
                    relative = bucket + '/opaque/source.bin/xl.meta'
                    original = next(e for e in before['files'] if e['disk'] == disk and e['path'] == relative)
                    fault_id = heal_fault.bounded_fault_id('J02', 'original-opaque', disk)
                    receipt = heal_fault.apply_fault(fd, binding, relative, fault_id, 'bitflip', original['sha256'], 0)
                    faults.append(dict(disk=disk, faultId=fault_id, receipt=receipt))
            original_fstat, original_stat, original_lstat = os.fstat, Path.stat, Path.lstat
            def mapped(info):
                row = {key: getattr(info, key) for key in dir(info) if key.startswith('st_')}
                row['st_uid'], row['st_gid'] = markers.get((info.st_dev, info.st_ino), (0, 0))
                return types.SimpleNamespace(**row)
            stack.enter_context(patch.object(os, 'fstat', side_effect=lambda fd: mapped(original_fstat(fd))))
            stack.enter_context(patch.object(Path, 'stat', lambda p, **kw: mapped(original_stat(p, **kw))))
            stack.enter_context(patch.object(Path, 'lstat', lambda p: mapped(original_lstat(p))))
            stack.enter_context(patch.object(recovery.pwd, 'getpwnam', return_value=types.SimpleNamespace(pw_uid=999, pw_gid=988)))
            stack.enter_context(patch.object(retained_scene, 'mounted_paths', return_value=[]))
            changed = []
            def chown(fd, uid, gid):
                info = original_fstat(fd); key = (info.st_dev, info.st_ino)
                self.assertIn(key, markers); self.assertEqual((uid, gid), (0, 0))
                markers[key] = (0, 0); changed.append(key)
            stack.enter_context(patch.object(os, 'fchown', side_effect=chown))
            node = dict(paths=lambda run_id: (state, roots), checked=fleet_node.checked, MOUNTS=mounts,
                unit=lambda run_id: owner['unit'], socket=socket, mounts=lambda: copy.deepcopy(owner['mounts']),
                stopped=lambda value: dict(MainPID='0', ActiveState='inactive'), digest=fleet_node.digest,
                fault_module=lambda: heal_fault, retained_scene_backend=retained_scene)
            node['testMarkerOwners'] = markers
            request = dict(runId=run, action='owner-markers-audit', expectedOwner=owner, before=before, faults=faults)
            yield state, roots, node, request, changed, chown

    def test_audit_and_recovery_preserve_all_bytes_and_durable_prechange_evidence(self):
        with self.fixture() as (state, roots, node, request, changed, chown):
            original = {p: (p.stat().st_ino, p.read_bytes()) for folder in (state, *roots) for p in folder.rglob('*') if p.is_file()}
            opened = []; original_open = os.open
            def observed_open(path, *args, **kw):
                opened.append(str(path)); return original_open(path, *args, **kw)
            with patch.object(os, 'open', side_effect=observed_open):
                audit = recovery.run(request, node)
                self.assertTrue(audit['markerRecoveryRequired']); self.assertEqual(changed, [])
                self.assertFalse((state / 'owner-marker-recovery.intent.json').exists())
                result = recovery.run(dict(request, action='owner-markers-recover', expectedAudit=audit), node)
            self.assertEqual(len(changed), 4); self.assertTrue(result['ownerMetadataRestored'])
            self.assertFalse(result['businessBytesChanged']); self.assertFalse(result['canonicalPassClaimed'])
            self.assertFalse(any('service.env' in p for p in opened))
            intent = json.loads((state / 'owner-marker-recovery.intent.json').read_text())
            self.assertEqual(intent['markers'], audit['markers']); self.assertEqual(intent['rootCensus'], audit['rootCensus'])
            self.assertEqual(result['intentSha256'], recovery.sha(intent))
            for path, saved in original.items(): self.assertEqual((path.stat().st_ino, path.read_bytes()), saved)
            again = recovery.run(request, node)
            self.assertFalse(again['markerRecoveryRequired']); self.assertEqual(again['priorIntent'], intent)

    def test_recovery_retries_after_each_marker_change_without_rewriting_the_original_intent(self):
        for lost_after in range(1, 5):
            with self.subTest(lost_after=lost_after), self.fixture() as (state, roots, node, request, changed, chown):
                audit = recovery.run(request, node)
                def interrupted(fd, uid, gid):
                    chown(fd, uid, gid)
                    if len(changed) == lost_after: raise RuntimeError('lost remote acknowledgement')
                with patch.object(os, 'fchown', side_effect=interrupted), self.assertRaises(RuntimeError):
                    recovery.run(dict(request, action='owner-markers-recover', expectedAudit=audit), node)
                raw_intent = (state / 'owner-marker-recovery.intent.json').read_bytes()
                current = recovery.run(request, node)
                result = recovery.run(dict(request, action='owner-markers-recover', expectedAudit=current), node)
                self.assertTrue(result['ownerMetadataRestored']); self.assertEqual(len(changed), 4)
                self.assertEqual((state / 'owner-marker-recovery.intent.json').read_bytes(), raw_intent)

    def test_drift_and_unknown_authority_refuse_before_chown(self):
        for poison in ('owner', 'binary', 'inspector', 'empty-prior', 'format', 'business', 'missing', 'extra', 'backup', 'intent', 'applied',
                       'foreign-backup', 'mount', 'nested-mount', 'foreign-uid', 'central-uid', 'live', 'legacy-phase', 'gate', 'symlink', 'hardlink', 'audit-race'):
            with self.subTest(poison=poison), self.fixture() as (state, roots, node, request, changed, _):
                audit = recovery.run(request, node)
                target = roots[0] / 'data' / request['expectedOwner']['bucket'] / 'objects/0000.bin/xl.meta'
                if poison == 'owner': (roots[0] / 'owner.json').write_text('{}')
                elif poison == 'binary': (state / 'rustfs').write_bytes(b'foreign')
                elif poison == 'inspector': (state / 'heal-meta-inspect').write_bytes(b'foreign')
                elif poison == 'empty-prior':
                    (state / 'owner-marker-recovery.intent.json').write_text('{}')
                    with self.assertRaises(ValueError): recovery.run(request, node)
                    self.assertEqual(changed, [])
                    continue
                elif poison == 'format': (roots[0] / 'data/.rustfs.sys/format.json').write_text('{}')
                elif poison == 'business': target.write_bytes(b'later acknowledged data')
                elif poison == 'missing': target.unlink()
                elif poison == 'extra': (target.parent / 'other').write_bytes(b'foreign')
                elif poison in ('backup', 'intent', 'applied'):
                    suffix = {'backup': 'original', 'intent': 'intent.json', 'applied': 'applied.json'}[poison]
                    next((roots[0] / 'quarantine').glob('*.' + suffix)).write_bytes(b'foreign')
                elif poison == 'foreign-backup': (roots[2] / 'quarantine/foreign').write_bytes(b'unknown')
                elif poison == 'mount': node['mounts'] = lambda: []
                elif poison == 'nested-mount': retained_scene.mounted_paths.return_value = [roots[0] / 'data']
                elif poison in ('foreign-uid', 'central-uid'):
                    path = roots[0] / 'owner.json' if poison == 'foreign-uid' else state / 'owner.json'
                    info = path.stat(); node['testMarkerOwners'][(info.st_dev, info.st_ino)] = (777, 777) if poison == 'foreign-uid' else (999, 988)
                elif poison == 'live': node['stopped'] = lambda owner: (_ for _ in ()).throw(ValueError('live PID'))
                elif poison == 'legacy-phase': (state / 'J02-phase.json').write_text('{}')
                elif poison == 'gate': (state / 'drive-gate.json').write_text('{}')
                elif poison == 'symlink': target.unlink(); target.symlink_to(state / 'service.env')
                elif poison == 'hardlink': os.link(target, target.parent / 'alias')
                elif poison == 'audit-race': audit['rawPhysicalSha256'] = 'f' * 64
                with self.assertRaises((ValueError, KeyError)): recovery.run(dict(request, action='owner-markers-recover', expectedAudit=audit), node)
                self.assertEqual(changed, [])

    def test_remote_adapter_bundles_both_auditors_and_descriptor_backend(self):
        def execute(*args, **kwargs):
            source = kwargs['input'].rsplit('\nmain(', 1)[0]; scope = dict(__name__='__test_node__')
            exec(compile(source, 'owner-recovery-node', 'exec'), scope)
            self.assertTrue(callable(scope['owner_recovery_backend'].run))
            self.assertTrue(callable(scope['retained_scene_backend'].census))
            self.assertTrue(callable(scope['fault_module']().read_file))
            return types.SimpleNamespace(returncode=0, stdout='{}', stderr='')
        with tempfile.TemporaryDirectory() as directory, patch.object(healthy.subprocess, 'run', side_effect=execute):
            healthy.Fleet(['vm000'], Path(directory), 'gh-12345-1-h1').remote('vm000', 'owner-markers-audit')
