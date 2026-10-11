"""C10 controlled-codec build identity and preservation guards."""

import copy
import json
import os
import shutil
import subprocess
import sys
import types
import uuid
from unittest.mock import patch
from pathlib import Path
import tempfile
import unittest

from scripts.validation_heal import shared_fixture as shared
from scripts.validation_heal import orphan, orphan_fault as backend, generation_fault as generation, heal_fault as fault
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES
from scripts.validation_heal.healthy import BASE_OPTIONS, Fleet


class SharedFixtureArtifactTests(unittest.TestCase):
    def fixture(self, root):
        binary = root / 'heal-shared-fixture'
        binary.write_bytes(b'\x7fELF\x02\x01' + b'\0' * 12 + b'\x3e\x00' + b'fixed-codec')
        lock = root / 'shared-Cargo.lock'; lock.write_text('frozen helper graph\n')
        (root / 'shared-fixture-tests.log').write_text('running 3 tests\n' + ''.join(
            'test ' + name + ' ... ok\n' for name in shared.TESTS) + 'test result: ok. 3 passed; 0 failed; 0 ignored;\n')
        run = {'id': 123, 'run_attempt': 1, 'status': 'completed', 'conclusion': 'success',
            'path': '.github/workflows/rustfs-validation-tools.yml'}
        (root / 'tool-run.json').write_text(json.dumps(run))
        manifest = {'schemaVersion': 1, 'built': True, 'sourceUnmodified': True, 'candidateCommit': shared.CANDIDATE,
            'candidateSourceTree': shared.SOURCE_TREE, 'candidateLockSha256': shared.CARGO_LOCK,
            'target': 'x86_64-unknown-linux-gnu', 'role': shared.ROLE, 'filesystemMutationPerformed': False,
            'physicalByteEqualityClaimed': False, 'runId': '123', 'runAttempt': '1',
            'helperSourceSha256': shared.sha(Path(shared.__file__).with_name('tools') / 'heal_shared_version_fixture.rs'),
            'binarySha256': shared.sha(binary), 'sharedLockSha256': shared.sha(lock)}
        (root / 'heal-shared-fixture.json').write_text(json.dumps(manifest))
        return manifest, run

    def test_verified_codec_is_distinct_from_read_only_inspector_and_live_proof(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); self.fixture(root)
            proof = shared.verify(root, {'source_sha': shared.CANDIDATE})
            self.assertTrue(proof['exactCodecTestsPassed'])
            self.assertFalse(proof['filesystemMutationPerformed'])
            self.assertFalse(proof['physicalByteEqualityClaimed'])
            self.assertFalse(proof['fleetPassClaimed'])

    def test_foreign_source_artifact_role_commit_and_build_attempt_cannot_be_installed(self):
        for field, value in (('role', 'read-only decoder; not the RustFS service under test'), ('candidateSourceTree', '0' * 40),
                             ('candidateLockSha256', '0' * 64), ('helperSourceSha256', '0' * 64), ('built', False),
                             ('physicalByteEqualityClaimed', True), ('runAttempt', '2')):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as folder:
                root = Path(folder); manifest, _ = self.fixture(root)
                manifest[field] = value
                (root / 'heal-shared-fixture.json').write_text(json.dumps(manifest))
                with self.assertRaises(ValueError): shared.verify(root, {'source_sha': shared.CANDIDATE})


    def test_zero_filtered_ignored_or_incomplete_codec_tests_do_not_certify_a_build(self):
        for log in ('running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored;\n',
                    'running 3 tests\ntest result: ok. 2 passed; 0 failed; 1 ignored;\n',
                    'running 3 tests\ntest result: ok. 3 passed; 0 failed; 0 ignored;\n'):
            with self.subTest(log=log), tempfile.TemporaryDirectory() as folder:
                root = Path(folder); self.fixture(root)
                (root / 'shared-fixture-tests.log').write_text(log)
                with self.assertRaises(ValueError): shared.verify(root, {'source_sha': shared.CANDIDATE})

    def test_changed_executable_or_failed_remote_build_is_refused(self):
        for kind in ('binary', 'run', 'symlink'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as folder:
                root = Path(folder); _, run = self.fixture(root)
                binary = root / 'heal-shared-fixture'
                if kind == 'binary': binary.write_bytes(b'foreign')
                elif kind == 'run':
                    run['conclusion'] = 'failure'; (root / 'tool-run.json').write_text(json.dumps(run))
                else:
                    binary.rename(root / 'outside'); binary.symlink_to(root / 'outside')
                with self.assertRaises(ValueError): shared.verify(root, {'source_sha': shared.CANDIDATE})




class OrphanDescriptorTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'
    old_dir, live_dir = str(uuid.UUID(int=11)), str(uuid.UUID(int=12))

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.object = self.root / 'data' / self.bucket / orphan.KEY
        self.object.mkdir(parents=True)
        (self.root / 'quarantine').mkdir()
        self.owner = {'runId': 'gh-123-1-h1', 'bucket': self.bucket, 'runtimeProfile': 'orphan-preserve', 'binarySha256': 'a' * 64}
        self.request = {'disk': 0, 'bucket': self.bucket, 'key': orphan.KEY}
        self.mount = patch.object(fault, 'mount_id', return_value=7); self.mount.start()
        self.seed(self.old_dir, b'old')

    def tearDown(self):
        self.mount.stop(); self.temporary.cleanup()

    def seed(self, directory, raw):
        (self.object / 'xl.meta').write_bytes(b'XL2 ' + raw)
        (self.object / directory).mkdir(exist_ok=True)
        (self.object / directory / 'part.1').write_bytes(raw * 256)
        (self.object / directory / ('part.1.integrity.' + str(uuid.UUID(int=77)))).write_bytes(raw + b'-commitment')

    def invoke(self, name, **kwargs):
        with fault.directory(self.root) as root:
            return getattr(backend, name)(root, self.owner, {**self.request, **kwargs}, fault, generation)

    def capture(self, label):
        with fault.directory(self.root) as root:
            present, _, files = backend.scene(root, self.owner, self.request, fault, generation)
        return self.invoke('capture', generation=label, expectedFiles=files, expectedPresent=present)

    def authority(self, absent=False):
        self.capture('old')
        shutil.rmtree(self.object)
        if not absent:
            self.object.mkdir(); self.seed(self.live_dir, b'live')
        saved = self.capture('authority')
        self.invoke('arm', generation='authority')
        return saved

    def recover(self, label='authority'):
        actual = os.fstat
        def root_owned(fd):
            info = actual(fd)
            fields = {name: getattr(info, name) for name in dir(info) if name.startswith('st_')}
            return types.SimpleNamespace(**{**fields, 'st_uid': 0})
        with patch.object(backend.os, 'fstat', side_effect=root_owned):
            return self.invoke('recover', generation=label)

    def test_post_delete_recovery_never_reinstates_old_version_and_preserves_sidecars(self):
        saved = self.authority()
        self.invoke('install', mode='object', dataDir=self.old_dir)
        self.assertEqual((self.object / 'xl.meta').read_bytes(), b'XL2 old')
        receipt = self.recover()
        self.assertTrue(receipt['lastACKAuthorityRestored'])
        self.assertEqual(receipt['originalTreeSha256'], saved['treeSha256'])
        self.assertEqual((self.object / 'xl.meta').read_bytes(), b'XL2 live')
        self.assertFalse((self.object / self.old_dir).exists())
        self.assertTrue((self.root / 'quarantine/C10-authority-failed-current' / self.old_dir).is_dir())
        self.assertEqual(receipt, self.recover())

    def test_absent_post_delete_authority_remains_absent_after_metadata_less_replay(self):
        self.authority(absent=True)
        self.invoke('install', mode='data', dataDir=self.old_dir)
        self.assertFalse((self.object / 'xl.meta').exists())
        self.assertTrue((self.object / self.old_dir).exists())
        self.recover()
        self.assertFalse(self.object.exists())
        self.assertTrue((self.root / 'quarantine/C10-authority-failed-current' / self.old_dir).is_dir())

    def test_live_reference_and_commitment_remain_exact_during_data_only_replay(self):
        self.authority()
        original = (self.object / self.live_dir / 'part.1').read_bytes()
        self.invoke('install', mode='data', dataDir=self.old_dir)
        self.assertEqual((self.object / 'xl.meta').read_bytes(), b'XL2 live')
        self.assertEqual((self.object / self.live_dir / 'part.1').read_bytes(), original)
        self.assertEqual((self.object / self.old_dir / ('part.1.integrity.' + str(uuid.UUID(int=77)))).read_bytes(), b'old-commitment')

    def test_old_generation_is_never_a_recovery_authority_and_foreign_identity_is_refused(self):
        self.authority()
        with self.assertRaises(ValueError): self.invoke('arm', generation='old')
        for field, value in (('key', '../escape'), ('bucket', 'foreign'), ('disk', 4)):
            with self.subTest(field=field), self.assertRaises(ValueError): self.invoke('capture', **{field: value},
                generation='authority', expectedFiles=[], expectedPresent=False)
        with patch.object(fault, 'mount_id', return_value=8), self.assertRaises(ValueError): self.recover()
        self.owner['binarySha256'] = 'b' * 64
        with self.assertRaises(ValueError): self.recover()

    def test_partial_restore_is_retained_and_never_certified_clear(self):
        self.authority()
        self.invoke('install', mode='object', dataDir=self.old_dir)
        self.object.rename(self.root / 'quarantine/C10-authority-failed-current')
        self.object.mkdir(); (self.object / 'xl.meta').write_bytes(b'partial')
        with self.assertRaises(ValueError): self.recover()
        self.assertFalse((self.root / 'quarantine/C10-authority-recovered.json').exists())
        self.assertTrue((self.root / 'quarantine/C10-authority-failed-current' / self.old_dir).exists())

    def test_sparse_mode_does_not_relax_the_original_c07_generation_contract(self):
        shutil.rmtree(self.object / self.old_dir)
        with fault.directory(self.object) as fd:
            with self.assertRaises(ValueError): generation.tree(fd, fault)
            self.assertEqual(set(generation.tree(fd, fault, allow_sparse=True)[0]), {'xl.meta'})
        (self.object / 'xl.meta').unlink()
        with fault.directory(self.object) as fd:
            with self.assertRaises(ValueError): generation.tree(fd, fault)
            self.assertEqual(generation.tree(fd, fault, allow_sparse=True), ({}, []))

    def test_symlinks_hardlinks_and_payload_less_commitments_are_not_archived(self):
        part = self.object / self.old_dir / 'part.1'
        for kind in ('symlink', 'hardlink', 'missing-part'):
            raw = part.read_bytes()
            if kind == 'symlink':
                part.unlink(); part.symlink_to(self.root / 'outside')
            elif kind == 'hardlink': os.link(part, self.root / 'outside')
            else: part.unlink()
            with self.subTest(kind=kind), self.assertRaises((ValueError, OSError)): self.capture('seed')
            if kind == 'hardlink': (self.root / 'outside').unlink()
            else:
                if part.is_symlink(): part.unlink()
                part.write_bytes(raw)

    def test_encoder_refuses_nonidentical_physical_parts_before_it_can_execute(self):
        self.seed(self.live_dir, b'different')
        self.capture('seed'); self.invoke('arm', generation='seed')
        with fault.directory(self.root) as root, patch.object(backend.subprocess, 'run') as execute, self.assertRaisesRegex(ValueError, 'identical physical'):
            backend.convert(root, self.owner, {**self.request, 'sourceDataDir': self.old_dir, 'destinationDataDir': self.live_dir,
                'sourceVersion': str(uuid.UUID(int=1)), 'destinationVersion': str(uuid.UUID(int=2))}, fault, generation, self.root / 'unavailable')
        execute.assert_not_called()

    def test_retired_shared_seed_cannot_be_recovered_after_later_ack_is_allowed(self):
        self.capture('seed'); self.invoke('arm', generation='seed')
        with fault.directory(self.root) as root:
            manifest = backend.scene(root, self.owner, self.request, fault, generation)[2]
        self.invoke('disarm', generation='seed', expectedFiles=manifest)
        with self.assertRaisesRegex(ValueError, 'retired'): self.recover('seed')

    def test_codec_conversion_preserves_retired_bytes_and_seed_rescue_can_restore_both_ack_generations(self):
        self.seed(self.live_dir, b'old')
        self.capture('seed'); self.invoke('arm', generation='seed')
        source, destination = str(uuid.UUID(int=1)), str(uuid.UUID(int=2))
        receipt = {'sourceVersion': source, 'destinationVersion': destination, 'sharedDataDir': self.old_dir,
            'retiredDataDir': self.live_dir, 'filesystemMutationPerformed': False, 'physicalByteEqualityClaimed': False}
        result = subprocess.CompletedProcess([], 0, b'controlled-encoded-meta', json.dumps(receipt).encode())
        with fault.directory(self.root) as root, patch.object(backend.subprocess, 'run', return_value=result):
            proof = backend.convert(root, self.owner, {**self.request, 'sourceDataDir': self.old_dir, 'destinationDataDir': self.live_dir,
                'sourceVersion': source, 'destinationVersion': destination}, fault, generation, self.root / 'verified-codec')
        self.assertTrue(proof['physicalByteEqualityVerified'])
        self.assertEqual((self.object / 'xl.meta').read_bytes(), b'controlled-encoded-meta')
        self.assertFalse((self.object / self.live_dir).exists())
        self.assertEqual((self.root / 'quarantine/C10-retired-shared-data/part.1').read_bytes(), b'old' * 256)
        self.recover('seed')
        self.assertEqual((self.object / 'xl.meta').read_bytes(), b'XL2 old')
        self.assertEqual((self.object / self.live_dir / 'part.1').read_bytes(), b'old' * 256)

    def test_mpu_census_is_complete_read_only_and_refuses_aliases(self):
        folder = self.root / 'data/.rustfs.sys/multipart/hash/upload'; folder.mkdir(parents=True)
        (folder / 'part.1').write_bytes(b'pending-part')
        (folder / 'xl.meta').write_bytes(b'pending-metadata')
        with fault.directory(self.root) as root:
            result = backend.multipart(root, fault)
            self.assertTrue(result['readOnly']); self.assertEqual(len(result['files']), 2)
            self.assertEqual(result, backend.multipart(root, fault))
            (folder / 'alias').symlink_to(folder / 'part.1')
            with self.assertRaises(OSError): backend.multipart(root, fault)


class OrphanOracleTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'
    def status(self, branch):
        status = {'summary': 'finished', 'truncated': False, 'settings': {**BASE_OPTIONS, 'scanMode': 2,
            'recursive': branch != 'remove-false', 'remove': branch != 'remove-false'}, 'outcome': {
                'execution': {'state': 'completed'}, 'coverage': 'complete', 'objectsTruncated': False,
                'counters': {'processed': 1, 'healed': 0, 'failed': 0, 'unknown': 0}, 'objects': []}}
        if branch == 'remove-false':
            status['detail'] = 'Object not found: ' + self.bucket + '/' + orphan.KEY
            status['outcome']['counters']['failed'] = 1
            status['outcome']['execution']['state'] = 'completed_with_errors'
        if branch == 'remove-true-before-grace':
            status['outcome']['objects'] = [{'identity': {'bucket': self.bucket, 'object': orphan.KEY},
                'disposition': {'state': 'deferred', 'details': {'reason': 'dangling_delete_grace'}}}]
        return status

    def test_all_exact_terminal_branches_and_typed_grace_are_required(self):
        for branch in orphan.BRANCHES:
            status = self.status(branch)
            orphan.terminal_safety(status, branch, self.bucket, status['settings'])
        for mutation in ('reason', 'key', 'partial', 'unknown', 'truncated', 'settings'):
            status = self.status('remove-true-before-grace'); options = copy.deepcopy(status['settings'])
            if mutation == 'reason': status['outcome']['objects'][0]['disposition']['details']['reason'] = 'retry'
            elif mutation == 'key': status['outcome']['objects'][0]['identity']['object'] = 'other'
            elif mutation == 'partial': status['outcome']['coverage'] = 'partial'
            elif mutation == 'unknown': status['outcome']['counters']['unknown'] = 1
            elif mutation == 'truncated': status['outcome']['objectsTruncated'] = True
            else: status['settings']['remove'] = False
            with self.subTest(mutation=mutation), self.assertRaises(ValueError): orphan.terminal_safety(status, 'remove-true-before-grace', self.bucket, options)

    def test_source_case_has_all_five_branches_on_both_required_topologies(self):
        source = select(orphan.CASE_ID)[0]
        self.assertEqual(source['variants'], list(orphan.BRANCHES))
        self.assertEqual(source['contract']['probes']['topology'], 'H1,H2')
        self.assertIn(orphan.CASE_ID, AUTOMATIC_CASES)

    def test_shared_paths_are_streamed_through_both_exact_ack_version_ids(self):
        mapping = {}
        for version in (str(uuid.UUID(int=1)), str(uuid.UUID(int=2))):
            mapping[(self.bucket, orphan.KEY, version)] = {'bucket': self.bucket, 'key': orphan.KEY, 'versionId': version,
                'cohort': [{'node': 'node1', 'disk': disk, 'relativePath': 'shared/part.1'} for disk in range(4)]}
        with patch.object(orphan, 'verify_parts', return_value={'exactStreamingParts': 4}) as verify:
            proof = orphan.verify_live_parts(object(), mapping, 'exact-shared-ACK')
        self.assertEqual(verify.call_count, 2)
        self.assertEqual({next(iter(call.args[1]))[2] for call in verify.call_args_list}, {identity[2] for identity in mapping})
        self.assertEqual(proof['versionAddressedStreamingParts'], 8)
        self.assertEqual(proof['distinctPhysicalParts'], 4)
        self.assertFalse(proof['sharedPathsCountedAsIndependentDisks'])
        next(iter(mapping.values()))['versionId'] = 'foreign'
        with self.assertRaises(ValueError): orphan.verify_live_parts(object(), mapping, 'foreign-ACK')

    def scene(self, branch):
        live, stale = str(uuid.UUID(int=12)), str(uuid.UUID(int=11))
        version, doomed = str(uuid.UUID(int=1)), str(uuid.UUID(int=2))
        expected = [] if branch == 'remove-false' else [{'key': orphan.KEY, 'versionId': version, 'deleteMarker': False}]
        versions = [] if not expected else [{'versionId': version, 'deleted': False, 'dataDir': live, 'inline': None}]
        if branch == 'shared-version-reference':
            shared_version = str(uuid.UUID(int=3))
            versions.append({'versionId': shared_version, 'deleted': False, 'dataDir': live, 'inline': None})
            expected.append({'key': orphan.KEY, 'versionId': shared_version, 'deleteMarker': False})
        authority = {'node1': {'formats': [{'disk': disk, 'sha256': str(disk)} for disk in range(4)], 'files': []}}
        for disk in range(4):
            authority['node1']['files'].append({'disk': disk, 'path': self.bucket + '/control.bin/xl.meta', 'sha256': 'control', 'size': 7})
            if versions:
                authority['node1']['files'] += [
                    {'disk': disk, 'path': self.bucket + '/' + orphan.KEY + '/xl.meta', 'sha256': 'authority', 'size': 7,
                     'metadata': {'bucket': self.bucket, 'key': orphan.KEY, 'versions': copy.deepcopy(versions)}},
                    {'disk': disk, 'path': self.bucket + '/' + orphan.KEY + '/' + live + '/part.1', 'sha256': 'live-part', 'size': 256},
                    {'disk': disk, 'path': self.bucket + '/' + orphan.KEY + '/' + live + '/part.1.integrity.' + str(uuid.UUID(int=77)), 'sha256': 'live-commitment', 'size': 100}]
        before = copy.deepcopy(authority)
        if branch != 'pending-mpu':
            before['node1']['files'].append({'disk': 0, 'path': self.bucket + '/' + orphan.KEY + '/' + stale + '/part.1',
                'sha256': 'stale-part', 'size': 256})
            if branch.startswith('remove-true-'):
                meta = next(row for row in before['node1']['files'] if row['disk'] == 0 and row['path'].endswith(orphan.KEY + '/xl.meta'))
                meta['metadata']['versions'].append({'versionId': doomed, 'deleted': False, 'dataDir': stale, 'inline': None})
                meta['sha256'] = 'old-authority'
        after = copy.deepcopy(before if branch in {'remove-false', 'remove-true-before-grace', 'pending-mpu'} else authority)
        return before, after, authority, expected, doomed, stale

    def test_actual_cleanup_preservation_and_all_original_controls_are_required(self):
        for branch in orphan.BRANCHES:
            before, after, authority, expected, doomed, stale = self.scene(branch)
            orphan.physical_safety(before, after, authority, self.bucket, expected, branch, doomed, stale, ('node1', 0))
            for kind in ('control', 'format', 'member', 'sidecar', 'orphan'):
                changed = copy.deepcopy(after)
                if kind == 'control': changed['node1']['files'][0]['sha256'] = 'modified'
                elif kind == 'format': changed['node1']['formats'][0]['sha256'] = 'modified'
                elif kind == 'member': changed['node1']['formats'].pop()
                elif kind == 'sidecar':
                    if branch == 'remove-false': continue
                    next(row for row in changed['node1']['files'] if '.integrity.' in row['path'])['sha256'] = 'modified'
                else:
                    if branch == 'pending-mpu': continue
                    if branch in {'remove-true-after-grace', 'shared-version-reference'}:
                        changed['node1']['files'].append(copy.deepcopy(before['node1']['files'][-1]))
                    else: changed['node1']['files'] = [row for row in changed['node1']['files'] if '/' + stale + '/' not in row['path']]
                with self.subTest(branch=branch, kind=kind), self.assertRaises(ValueError):
                    orphan.physical_safety(before, changed, authority, self.bucket, expected, branch, doomed, stale, ('node1', 0))

    def test_partial_original_member_recovery_blocks_cleanup_and_retains_the_namespace(self):
        with tempfile.TemporaryDirectory() as folder:
            fleet = Fleet(['node1'], Path(folder), 'gh-123-1-h1', runtime_profile='orphan-preserve')
            fleet.reserved = ['node1']
            fleet.orphan_faults = [('node1', {'disk': 0, 'bucket': self.bucket, 'key': orphan.KEY, 'generation': 'authority'})]
            calls = []
            def remote(node, action, **params):
                calls.append(action)
                if action == 'orphan-recover': return {'state': 'DIRTY', 'lastACKAuthorityRestored': False}
                return {}
            fleet.remote = remote
            self.assertFalse(fleet.cleanup())
            self.assertNotIn('cleanup', calls)
            self.assertEqual(len(fleet.orphan_faults), 1)

    def test_unknown_request_or_invalid_version_ack_never_restores_old_authority_or_cleans(self):
        for failure in ('transport', 'wrong-version'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                candidate = root / 'candidate.json'; candidate.write_text(json.dumps({'source_sha': orphan.CANDIDATE, 'package_sha256': 'a' * 64}))
                rc = root / 'rc'; rc.write_bytes(b'rc')
                args = types.SimpleNamespace(evidence=root / 'evidence', candidate=candidate, rc_bin=rc,
                    tool_directory=root, shared_directory=root, testing_commit='b' * 40, binary_sha='c' * 64)
                with patch.dict(os.environ, {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}), \
                        patch.object(orphan, 'verify_inspector', return_value={}), patch.object(orphan, 'verify_shared', return_value={}), \
                        patch.object(orphan, 'Fleet') as fleet, patch.object(orphan, 'Recorder') as recorder:
                    fleet.return_value.record.side_effect = lambda label, value: value
                    recorder.return_value.call.side_effect = [TimeoutError('unknown original request')] if failure == 'transport' else [{}, {}, {'VersionId': 'foreign'}]
                    result = orphan.run_branch(args, 'H1', ['node1'], 'remove-false')
                self.assertEqual(result['verdict'], 'FAIL'); self.assertEqual(result['cleanup'], 'FAIL')
                fleet.return_value.cleanup.assert_not_called()
                fleet.return_value.stop.assert_called_once()
                self.assertFalse(any(call.args[1] == 'orphan-recover' for call in fleet.return_value.remote.call_args_list))


class OrphanLinuxTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'linux', 'requires real Linux descriptors, root ownership and private bind mounts')
    def test_actual_absent_authority_recovery_and_nested_mount_refusal(self):
        program = '''import json,shutil,subprocess,tempfile,uuid
from pathlib import Path
from scripts.validation_heal import orphan_fault as backend,generation_fault as generation,heal_fault as fault
with tempfile.TemporaryDirectory() as folder:
 root=Path(folder).resolve();bucket='rv-heal-0123456789abcdef';obj=root/'data'/bucket/'orphan.bin'
 obj.mkdir(parents=True);(root/'quarantine').mkdir();directory=str(uuid.UUID(int=11));(obj/directory).mkdir()
 (obj/'xl.meta').write_bytes(b'old-meta');(obj/directory/'part.1').write_bytes(b'old-part')
 (obj/directory/('part.1.integrity.'+str(uuid.UUID(int=77)))).write_bytes(b'old-commitment')
 owner={'runId':'gh-123-1-h1','bucket':bucket,'runtimeProfile':'orphan-preserve','binarySha256':'a'*64}
 request={'disk':0,'bucket':bucket,'key':'orphan.bin'}
 with fault.directory(root) as fd:
  present,_,files=backend.scene(fd,owner,request,fault,generation)
  backend.capture(fd,owner,{**request,'generation':'old','expectedPresent':present,'expectedFiles':files},fault,generation)
  shutil.rmtree(obj)
  backend.capture(fd,owner,{**request,'generation':'authority','expectedPresent':False,'expectedFiles':[]},fault,generation)
  backend.arm(fd,owner,{**request,'generation':'authority'},fault,generation)
  backend.install(fd,owner,{**request,'mode':'data','dataDir':directory},fault,generation)
  assert not (obj/'xl.meta').exists() and (obj/directory/'part.1').read_bytes()==b'old-part'
  receipt=backend.recover(fd,owner,{**request,'generation':'authority'},fault,generation)
  assert receipt['lastACKAuthorityRestored'] and not obj.exists()
  assert backend.recover(fd,owner,{**request,'generation':'authority'},fault,generation)==receipt
  assert (root/'quarantine/C10-authority-failed-current'/directory/('part.1.integrity.'+str(uuid.UUID(int=77)))).read_bytes()==b'old-commitment'
 obj.mkdir();outside=root/'outside';outside.mkdir()
 subprocess.run(['mount','--bind',str(outside),str(obj)],check=True)
 try:
  with fault.directory(root) as fd:
   try: backend.scene(fd,owner,request,fault,generation)
   except ValueError as error: assert 'mount' in str(error)
   else: raise AssertionError('nested mount accepted')
 finally: subprocess.run(['umount',str(obj)],check=True)
print(json.dumps({'realPostDeleteAuthorityRestored':True,'nestedBindRefused':True}))
'''
        result = subprocess.run(['sudo', '-n', 'unshare', '-m', sys.executable, '-B', '-c', program], capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(json.loads(result.stdout)['realPostDeleteAuthorityRestored'])




if __name__ == '__main__':
    unittest.main()
