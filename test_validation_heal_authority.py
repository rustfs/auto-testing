import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch

from scripts.validation_heal import generation_fault as generation, heal_fault as fault
from scripts.validation_heal import authority, authority_runner as runner, native_regressions as native
from scripts.validation_heal.healthy import BASE_OPTIONS, Fleet
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
import test_validation_heal as oracle


class AuthorityCensusTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def scene(self, total=4, branch='disjoint-version-quorums'):
        first, _ = oracle.HealReadQuorumBoundaryTest().scene(total=total, data=total - 1)
        for value in first.values():
            for entry in value['files']:
                entry['path'] = entry['path'].replace('boundary/exact-r-read.bin', 'conflict.bin')
                if 'metadata' in entry:
                    entry['metadata']['key'] = 'conflict.bin'
                    version = entry['metadata']['versions'][0]
                    version['versionId'] = str(uuid.UUID(int=11)) if branch == authority.BRANCHES[0] else None
        second = copy.deepcopy(first)
        for value in second.values():
            for entry in value['files']:
                entry['sha256'] += '-generation-b'
                if 'metadata' in entry:
                    version = entry['metadata']['versions'][0]
                    old_dir = version['dataDir']
                    version['dataDir'] = str(uuid.UUID(int=2))
                    version['versionId'] = str(uuid.UUID(int=12)) if branch == authority.BRANCHES[0] else None
                else:
                    entry['path'] = entry['path'].replace(old_dir, str(uuid.UUID(int=2)))
        mixed = copy.deepcopy(second)
        members = sorted((node, row['disk']) for node, value in first.items() for row in value['formats'])
        for node, disk in members[:total // 2]:
            mixed[node]['files'] = [row for row in mixed[node]['files'] if row['disk'] != disk]
            mixed[node]['files'].extend(copy.deepcopy([row for row in first[node]['files'] if row['disk'] == disk]))
        return first, second, mixed

    def check(self, first, second, mixed, total=4, branch='disjoint-version-quorums'):
        return authority.validate_split(mixed, first, second, self.bucket, 'conflict.bin', 'H1' if total == 4 else 'H2', branch)

    def test_both_original_topologies_and_real_identity_branches_have_no_authority_quorum(self):
        for total in (4, 16):
            for branch in authority.BRANCHES[:2]:
                with self.subTest(total=total, branch=branch):
                    proof = self.check(*self.scene(total, branch), total, branch)
                    self.assertEqual(proof['R'], total - 1)
                    self.assertEqual([row['members'] for row in proof['groups']], [total // 2] * 2)
                    self.assertFalse(proof['crossVersionUnionClaimedAsAuthority'])
                    self.assertFalse(proof['bucketIncarnationSplitClaimed'])

    def test_hidden_live_history_in_other_group_cannot_be_ignored_by_latest_only_census(self):
        first, second, mixed = self.scene()
        for disk in (2, 3):
            row = next(row for row in mixed['node1']['files'] if row['disk'] == disk and 'metadata' in row)
            old = copy.deepcopy(next(row for row in first['node1']['files'] if row['disk'] == disk and 'metadata' in row)['metadata']['versions'][0])
            old['isLatest'] = False
            row['metadata']['versions'].append(old)
        with self.assertRaisesRegex(ValueError, 'sub-quorum'):
            self.check(first, second, mixed)

    def test_changed_bytes_uuid_slot_role_geometry_and_missing_members_are_not_split_proofs(self):
        for kind in ('bytes', 'format', 'slot', 'role', 'geometry', 'missing', 'duplicate', 'same-generation', 'wrong-half'):
            first, second, mixed = self.scene()
            value = mixed['node1']
            version = next(row['metadata']['versions'][0] for row in value['files'] if row['disk'] == 0 and 'metadata' in row)
            if kind == 'bytes': value['files'][0]['sha256'] = 'changed'
            elif kind == 'format': value['formats'][0]['value']['xl']['this'] = 'foreign'
            elif kind == 'slot': version['erasure']['index'] = 2
            elif kind == 'role': version['role'] = 'parity'
            elif kind == 'geometry': version['erasure']['data_blocks'] = 2
            elif kind == 'missing': value['files'] = [row for row in value['files'] if row['disk'] != 0]
            elif kind == 'duplicate': value['files'].append(copy.deepcopy(value['files'][0]))
            elif kind == 'same-generation': second = copy.deepcopy(first)
            else:
                value['files'] = [row for row in value['files'] if row['disk'] != 0]
                value['files'].extend(copy.deepcopy([row for row in second['node1']['files'] if row['disk'] == 0]))
            with self.subTest(kind=kind), self.assertRaises(ValueError): self.check(first, second, mixed)

    def test_generation_sources_must_be_independent_and_branch_identity_exact(self):
        first, second, mixed = self.scene()
        frozen = copy.deepcopy((first, second))
        self.check(first, second, mixed)
        self.assertEqual((first, second), frozen)
        for branch in ('native-authority-fence', 'disjoint-incarnations'):
            with self.subTest(branch=branch), self.assertRaises(ValueError): self.check(first, second, mixed, branch=branch)
        with self.assertRaises(ValueError): self.check(second, second, mixed)


def negative_status(aborted=False):
    objects = [] if aborted else [{'identity': {'bucket': 'owned', 'object': 'conflict.bin', 'kind': 'object',
        'versionId': 'null', 'bucketIncarnationId': 'incarnation', 'poolIndex': 0, 'setIndex': 0}, 'disposition': {'state': 'failed'}}]
    return {'summary': 'finished', 'truncated': False, 'settings': BASE_OPTIONS,
        'outcome': {'execution': {'state': 'aborted', 'reason': 'untraversable'} if aborted else {'state': 'completed_with_errors'},
            'coverage': 'partial' if aborted else 'complete', 'objectsTruncated': False, 'objects': objects,
            'counters': {'processed': len(objects), 'healed': 0, 'unchanged': 0, 'skipped': 0, 'failed': len(objects),
                'unknown': 0, 'attemptFailures': len(objects), 'overflowed': False}}}


class AuthorityResponsibilityTests(unittest.TestCase):
    def test_explicit_failed_object_and_typed_untraversable_abort_preserve_negative_responsibility(self):
        for aborted in (False, True):
            value = negative_status(aborted)
            proof = authority.validate_negative_task(value, 'owned', BASE_OPTIONS)
            self.assertTrue(proof['safeExplicitRefusal'])
            self.assertFalse(proof['runtimeCommitPauseClaimed'])

    def test_false_health_missing_identity_duplicate_counters_and_false_complete_abort_fail(self):
        for kind in ('healthy', 'healed', 'foreign', 'pool', 'incarnation', 'duplicate', 'counters', 'empty', 'options', 'overflow', 'abort-coverage'):
            value = negative_status(kind == 'abort-coverage')
            outcome = value['outcome']
            if kind == 'healthy': outcome['objects'][0]['disposition']['state'] = 'verified_healthy'
            elif kind == 'healed': outcome['counters']['healed'] = 1
            elif kind == 'foreign': outcome['objects'][0]['identity']['object'] = 'other.bin'
            elif kind == 'pool': outcome['objects'][0]['identity']['poolIndex'] = True
            elif kind == 'incarnation': outcome['objects'][0]['identity']['bucketIncarnationId'] = ''
            elif kind == 'duplicate': outcome['objects'] *= 2; outcome['counters'].update(processed=2, failed=2, attemptFailures=2)
            elif kind == 'counters': outcome['counters']['failed'] = 0
            elif kind == 'empty': outcome['objects'] = []; outcome['counters'].update(processed=0, failed=0, attemptFailures=0)
            elif kind == 'options': value['settings'] = {**BASE_OPTIONS, 'remove': True}
            elif kind == 'overflow': outcome['counters']['overflowed'] = True
            else: outcome['coverage'] = 'complete'
            with self.subTest(kind=kind), self.assertRaises(ValueError): authority.validate_negative_task(value, 'owned', BASE_OPTIONS)

    def test_peer_propagation_can_converge_for_partial_abort_without_claiming_full_coverage(self):
        value = negative_status(True)
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        probe = types.SimpleNamespace(binding={'endpoints': {'node1': 'url'}}, admin=unittest.mock.Mock(side_effect=[
            {'httpStatus': 200, 'response': {'clientToken': 'token'}}, {'httpStatus': 200, 'response': value}]))
        active = []
        with patch.object(runner, 'Probe') as remote, patch.object(runner.time, 'sleep'):
            remote.return_value.admin.side_effect = [{'httpStatus': 200, 'response': {'summary': 'running'}},
                {'httpStatus': 200, 'response': copy.deepcopy(value)}]
            proof = runner.negative_heal(probe, fleet, 'owned', BASE_OPTIONS, active,
                {'unrecoverableGet': {'httpStatus': 503, 'code': 'SlowDownRead'}})
        self.assertEqual(proof['coverage'], 'partial')
        self.assertFalse(active)

    def test_missing_peer_status_and_wrong_public_error_cannot_be_success(self):
        fleet = types.SimpleNamespace(record=lambda label, value: value)
        contract = {'unrecoverableGet': {'httpStatus': 503, 'code': 'SlowDownRead'}}
        probe = types.SimpleNamespace(binding={'endpoints': {'node1': 'url'}}, admin=unittest.mock.Mock(side_effect=[
            {'httpStatus': 200, 'response': {'clientToken': 'token'}}, {'httpStatus': 200, 'response': negative_status()}]))
        active = []
        with patch.object(runner, 'Probe') as remote, self.assertRaisesRegex(ValueError, '404/unknown'):
            remote.return_value.admin.return_value = {'httpStatus': 404, 'response': {}}
            runner.negative_heal(probe, fleet, 'owned', BASE_OPTIONS, active, contract)
        self.assertEqual(len(active), 1)
        probe.admin = lambda *args: {'httpStatus': 503, 'response': {'Code': 'SlowDown'}}
        with self.assertRaises(ValueError): runner.negative_heal(probe, fleet, 'owned', BASE_OPTIONS, [], contract)


class AuthorityNativeEvidenceTests(unittest.TestCase):
    def fixture(self, directory):
        tools, evidence = directory / 'native-tools', directory / 'native-evidence'
        tools.mkdir(); evidence.mkdir()
        binary = tools / 'ecstore-native-tests'
        binary.write_bytes(b'fixed-test-executable')
        manifest = {'schemaVersion': 1, 'candidateCommit': native.CANDIDATE, 'candidateSourceTree': authority.SOURCE_TREE,
            'candidateCargoLockSha256': authority.CARGO_LOCK, 'sourceFiles': {native.SOURCE: native.SOURCE_SHA},
            'sourceUnmodified': True, 'optimized': True, 'collectorSha256': native.sha(Path(native.__file__)),
            'regressions': native.REGRESSIONS, 'fleetPassClaimed': False, 'binaryName': binary.name,
            'binarySha256': native.sha(binary), 'runId': '123', 'runAttempt': '1'}
        (tools / 'native-regressions.json').write_text(json.dumps(manifest))
        run = {'id': 123, 'run_attempt': 1, 'status': 'completed', 'conclusion': 'success',
            'path': '.github/workflows/rustfs-functional-chain.yml'}
        (directory / 'tool-run.json').write_text(json.dumps(run))
        listing = ''.join(spec['test'] + ': test\n' for spec in native.REGRESSIONS.values())
        (tools / 'test-inventory.log').write_text(listing)
        (evidence / 'runtime-inventory.log').write_text(listing)
        results = []
        for key, spec in native.REGRESSIONS.items():
            row = {'regression': key, 'test': spec['test'], 'verdict': 'PASS', 'candidateCommit': native.CANDIDATE,
                'binarySha256': manifest['binarySha256'], 'fleetPassClaimed': False, 'emittedBarrierLogsClaimed': False,
                'command': [authority.HOSTED_BINARY, spec['test'], '--exact', '--nocapture', '--test-threads=1'],
                'sourceAssertions': spec['sourceAssertions'], 'fenceKind': spec['fenceKind'], 'exitCode': 0}
            (evidence / (key + '.json')).write_text(json.dumps(row))
            (evidence / (key + '.log')).write_text(f"running 1 test\ntest {spec['test']} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
            results.append(row)
        summary = {'complete': True, 'fleetPassClaimed': False, 'canonicalCasePassClaimed': False,
            'sourceUnmodified': True, 'results': results}
        (evidence / 'native-summary.json').write_text(json.dumps(summary))
        return manifest, summary, run

    def verify(self, directory):
        return authority.verify_native(directory, {'source_sha': native.CANDIDATE})

    def test_all_six_original_tests_are_checked_and_only_two_authority_guards_are_mapped(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            self.fixture(directory)
            proof = self.verify(directory)
            self.assertEqual({row['regression'] for row in proof['results']}, set(authority.NATIVE_KEYS))
            self.assertEqual(proof['toolsBuildRun'], 123)
            self.assertFalse(proof['runtimeCommitPauseClaimed'])
            self.assertFalse(proof['fleetPassClaimed'])

    def test_source_artifact_attempt_zero_test_forged_complete_and_non_c07_row_failures_reject(self):
        for kind in ('source', 'tree', 'lock', 'collector', 'binary', 'run', 'attempt', 'summary', 'duplicate', 'row', 'filtered', 'listing', 'path'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as folder:
                directory = Path(folder)
                manifest, summary, run = self.fixture(directory)
                evidence = directory / 'native-evidence'
                key = next(iter(native.REGRESSIONS))
                if kind == 'source': manifest['sourceUnmodified'] = False
                elif kind == 'tree': manifest['candidateSourceTree'] = '0' * 40
                elif kind == 'lock': manifest['candidateCargoLockSha256'] = '0' * 64
                elif kind == 'collector': manifest['collectorSha256'] = '0' * 64
                elif kind == 'binary': (directory / 'native-tools/ecstore-native-tests').write_bytes(b'changed')
                elif kind == 'run': run['conclusion'] = 'failure'
                elif kind == 'attempt': run['run_attempt'] = 2
                elif kind == 'summary': summary['results'][0]['verdict'] = 'FAIL'
                elif kind == 'duplicate': summary['results'][-1] = summary['results'][0]
                elif kind == 'row': (evidence / (key + '.json')).write_text('{}')
                elif kind == 'filtered': (evidence / (key + '.log')).write_text('running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored;\n')
                elif kind == 'listing': (evidence / 'runtime-inventory.log').write_text('0 tests\n')
                else:
                    summary['results'][0]['command'][0] = '/tmp/download/ecstore-native-tests'
                    (evidence / (key + '.json')).write_text(json.dumps(summary['results'][0]))
                (directory / 'native-tools/native-regressions.json').write_text(json.dumps(manifest))
                (directory / 'tool-run.json').write_text(json.dumps(run))
                (evidence / 'native-summary.json').write_text(json.dumps(summary))
                with self.assertRaises(ValueError): self.verify(directory)


class AuthorityRecoveryTests(unittest.TestCase):
    def test_failed_generation_recovery_retains_namespace_and_does_not_run_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            fleet = Fleet(['vm000'], Path(folder), 'gh-123-1-h1', runtime_profile='authority-split')
            fleet.reserved = ['node1']
            fleet.generation_faults.append(('node1', {'disk': 0, 'bucket': 'owned', 'key': 'conflict.bin'}))
            with patch.object(fleet, 'stop'), patch.object(fleet, 'remote', side_effect=ValueError('partial archive')) as remote:
                self.assertFalse(fleet.cleanup())
            self.assertTrue(fleet.generation_faults)
            self.assertFalse(any(call.args[1] == 'cleanup' for call in remote.call_args_list))

    def test_wrong_put_ack_keeps_durable_request_pending_and_never_deletes_namespace(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            candidate = directory / 'candidate.json'
            candidate.write_text(json.dumps({'source_sha': native.CANDIDATE, 'package_sha256': 'a' * 64}))
            binary = directory / 'rc'; binary.write_bytes(b'rc')
            args = types.SimpleNamespace(evidence=directory / 'evidence', candidate=candidate, rc_bin=binary,
                tool_directory=directory, testing_commit='b' * 40, binary_sha='c' * 64)
            with patch.dict(os.environ, {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}), \
                    patch.object(runner, 'verify_inspector', return_value={}), patch.object(runner, 'verify_quorum_contract', return_value={}), \
                    patch.object(runner, 'Fleet') as fleet, patch.object(runner, 'Recorder') as recorder:
                fleet.return_value.record.side_effect = lambda label, value: value
                recorder.return_value.call.side_effect = [{}, {'VersionId': 'foreign-version'}]
                result = runner.run_branch(args, 'H1', ['vm000'], 'disjoint-incarnations')
            self.assertEqual(result['verdict'], 'FAIL')
            self.assertEqual(result['cleanup'], 'FAIL')
            fleet.return_value.cleanup.assert_not_called()
            fleet.return_value.stop.assert_called_once()
            self.assertEqual(recorder.return_value.call.call_count, 2)

    def test_native_proof_uses_no_fleet_and_cannot_invent_h2_native_members(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            AuthorityNativeEvidenceTests().fixture(directory)
            candidate = directory / 'candidate.json'; candidate.write_text(json.dumps({'source_sha': native.CANDIDATE}))
            args = types.SimpleNamespace(evidence=directory / 'evidence', candidate=candidate, native_directory=directory)
            with patch.object(runner, 'Fleet') as fleet:
                result = runner.run_branch(args, 'H2', ['vm000', 'vm001', 'vm002', 'vm003'], 'native-authority-fence')
            fleet.assert_not_called()
            self.assertEqual(result['actualNativeUnitMembers'], 4)
            self.assertFalse(result['fleetDeploymentPerformed'])
            self.assertEqual(result['verdict'], 'PASS')

    def test_registered_source_branches_and_both_dispatch_paths_remain_complete(self):
        self.assertEqual(select(authority.CASE_ID)[0]['variants'], list(authority.BRANCHES))
        self.assertIn(authority.CASE_ID, AUTOMATIC_CASES)
        self.assertIn(authority.CASE_ID + ') driver=authority_runner.py', Path('scripts/validation_heal/run_case.sh').read_text())
        self.assertIn(authority.CASE_ID, Path('scripts/run_validation_case.sh').read_text())


class GenerationFaultTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'
    data_dir = str(uuid.UUID(int=1))

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.object = self.root / 'data' / self.bucket / 'conflict.bin'
        self.object.mkdir(parents=True)
        (self.root / 'quarantine').mkdir()
        self.owner = {'runId': 'gh-123-1-h1', 'bucket': self.bucket, 'runtimeProfile': 'authority-split'}
        self.request = {'disk': 0, 'bucket': self.bucket, 'key': 'conflict.bin'}
        self.mount = patch.object(fault, 'mount_id', return_value=7)
        self.mount.start()
        self.seed('a')

    def tearDown(self):
        self.mount.stop()
        self.temporary.cleanup()

    def seed(self, label):
        (self.object / 'xl.meta').write_bytes(('XL2 ' + label).encode())
        (self.object / self.data_dir).mkdir(exist_ok=True)
        (self.object / self.data_dir / 'part.1').write_bytes((label * 123).encode())

    def capture(self, label):
        with fault.directory(self.root) as root, fault.parent(root, ['data', self.bucket, 'conflict.bin']) as obj:
            manifest = generation.tree(obj, fault)[1]
            return generation.capture(root, self.owner, {**self.request, 'generation': label, 'expectedFiles': manifest}, fault)

    def pair(self):
        a = self.capture('a')
        self.seed('b')
        b = self.capture('b')
        return a, b

    def install(self, b):
        with fault.directory(self.root) as root:
            return generation.install(root, self.owner, {**self.request, 'expectedSceneSha256': b['treeSha256']}, fault)

    def recover(self):
        actual_stat = os.stat
        def root_owned(path, **kwargs):
            value = actual_stat(path, **kwargs)
            return types.SimpleNamespace(st_mode=value.st_mode, st_uid=0, st_dev=value.st_dev)
        with fault.directory(self.root) as root, patch.object(generation.os, 'stat', side_effect=root_owned):
            return generation.recover(root, self.owner, self.request, fault)

    def test_complete_old_and_new_trees_are_preserved_then_only_last_ack_is_restored(self):
        a, b = self.pair()
        receipt = self.install(b)
        self.assertEqual(receipt['injectedTreeSha256'], a['treeSha256'])
        self.assertEqual((self.object / 'xl.meta').read_bytes(), b'XL2 a')
        self.assertEqual((self.root / 'quarantine' / 'C07-pre-split' / 'xl.meta').read_bytes(), b'XL2 b')
        result = self.recover()
        self.assertEqual(result['state'], 'CLEAR')
        self.assertFalse(result['productRepairClaimed'])
        self.assertEqual((self.object / 'xl.meta').read_bytes(), b'XL2 b')
        self.assertEqual((self.root / 'quarantine' / 'C07-failed-current' / 'xl.meta').read_bytes(), b'XL2 a')
        self.assertEqual(self.recover(), result)

    def test_changed_live_bytes_or_archive_authority_refuse_before_first_rename(self):
        _, b = self.pair()
        self.seed('drift')
        with self.assertRaises(ValueError): self.install(b)
        self.assertFalse((self.root / 'quarantine' / 'C07-replay-intent.json').exists())
        self.seed('b')
        (self.root / 'quarantine' / 'C07-a' / 'object' / 'xl.meta').write_bytes(b'foreign')
        with self.assertRaises(ValueError): self.install(b)
        self.assertFalse((self.root / 'quarantine' / 'C07-replay-intent.json').exists())

    def test_hardlinks_symlinks_foreign_leaves_and_unknown_directories_cannot_enter_archive(self):
        for kind in ('hardlink', 'symlink', 'leaf', 'directory'):
            with self.subTest(kind=kind):
                target = self.object / self.data_dir / 'part.1'
                original = target.read_bytes()
                if kind == 'hardlink':
                    alias = self.root / 'outside'
                    os.link(target, alias)
                elif kind == 'symlink':
                    alias = self.root / 'outside'
                    alias.write_bytes(original)
                    target.unlink()
                    target.symlink_to(alias)
                elif kind == 'leaf':
                    alias = self.object / self.data_dir / 'credential.json'
                    alias.write_bytes(b'foreign')
                else:
                    alias = self.object / 'foreign-directory'
                    alias.mkdir()
                with self.assertRaises((ValueError, OSError)): self.capture('a')
                if kind == 'symlink':
                    target.unlink()
                    target.write_bytes(original)
                if alias.is_dir(): alias.rmdir()
                else: alias.unlink()

    def test_capture_rejects_an_incomplete_caller_manifest_and_duplicate_archive(self):
        with fault.directory(self.root) as root:
            with self.assertRaises(ValueError): generation.capture(root, self.owner,
                {**self.request, 'generation': 'a', 'expectedFiles': []}, fault)
        self.assertFalse((self.root / 'quarantine' / 'C07-a').exists())
        self.capture('a')
        with self.assertRaises(FileExistsError): self.capture('a')

    def test_full_protected_generation_preserves_exact_candidate_integrity_sidecar(self):
        name = 'part.1.integrity.' + str(uuid.UUID(int=77))
        sidecar = self.object / self.data_dir / name
        sidecar.write_bytes(b'candidate-shard-integrity-a')
        a = self.capture('a')
        self.seed('b'); sidecar.unlink()
        newest_sidecar = self.object / self.data_dir / ('part.1.integrity.' + str(uuid.UUID(int=78)))
        newest_sidecar.write_bytes(b'candidate-shard-integrity-b')
        b = self.capture('b')
        self.assertTrue(any(row['path'].endswith(name) for row in a['files']))
        self.install(b)
        self.assertEqual(sidecar.read_bytes(), b'candidate-shard-integrity-a')
        self.assertFalse(newest_sidecar.exists())
        self.recover()
        self.assertEqual(newest_sidecar.read_bytes(), b'candidate-shard-integrity-b')
        self.assertFalse(sidecar.exists())

    def test_commitment_without_payload_duplicate_nonce_and_nil_nonce_are_rejected(self):
        for name in ('part.2.integrity.' + str(uuid.UUID(int=77)), 'part.1.integrity.' + str(uuid.UUID(int=0)),
                     'part.1.integrity.' + str(uuid.UUID(int=78))):
            with self.subTest(name=name):
                target = self.object / self.data_dir / name
                target.write_bytes(b'commitment')
                first = self.object / self.data_dir / ('part.1.integrity.' + str(uuid.UUID(int=77)))
                if name.endswith(str(uuid.UUID(int=78))): first.write_bytes(b'original-commitment')
                with self.assertRaises(ValueError): self.capture('a')
                target.unlink()
                if first.exists(): first.unlink()

    def test_bucket_key_profile_and_device_drift_cannot_restore_an_unrelated_generation(self):
        _, b = self.pair()
        self.install(b)
        for field, value in (('bucket', 'rv-heal-fedcba9876543210'), ('key', '../conflict.bin'), ('disk', 1)):
            with self.subTest(field=field), fault.directory(self.root) as root, self.assertRaises((ValueError, FileNotFoundError)):
                generation.recover(root, self.owner, {**self.request, field: value}, fault)
        with fault.directory(self.root) as root, self.assertRaises(ValueError):
            generation.recover(root, {**self.owner, 'runtimeProfile': 'manual'}, self.request, fault)
        with patch.object(fault, 'mount_id', return_value=8), fault.directory(self.root) as root, self.assertRaises(ValueError):
            generation.recover(root, self.owner, self.request, fault)

    def test_partial_recovery_remains_dirty_and_does_not_remove_the_failed_scene(self):
        _, b = self.pair()
        self.install(b)
        failed = self.root / 'quarantine' / 'C07-failed-current'
        self.object.rename(failed)
        self.object.mkdir()
        (self.object / 'xl.meta').write_bytes(b'partial')
        with self.assertRaises(ValueError): self.recover()
        self.assertEqual((failed / 'xl.meta').read_bytes(), b'XL2 a')
        self.assertFalse((self.root / 'quarantine' / 'C07-replay-recovered.json').exists())


class GenerationLinuxTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'linux', 'requires original Linux mount descriptors and root-owned recovery')
    def test_real_descriptors_restore_last_ack_and_reject_nested_bind_mount(self):
        program = '''import json,subprocess,tempfile,uuid
from pathlib import Path
from scripts.validation_heal import generation_fault as generation,heal_fault as fault
with tempfile.TemporaryDirectory() as folder:
    root=Path(folder).resolve()
    bucket='rv-heal-0123456789abcdef'
    obj=root/'data'/bucket/'conflict.bin'
    obj.mkdir(parents=True)
    (root/'quarantine').mkdir()
    data_dir=str(uuid.UUID(int=1))
    (obj/data_dir).mkdir()
    owner={'runId':'gh-123-1-h1','bucket':bucket,'runtimeProfile':'authority-split'}
    request={'disk':0,'bucket':bucket,'key':'conflict.bin'}
    captured={}
    for label in ('a','b'):
        (obj/'xl.meta').write_bytes(('XL2 '+label).encode())
        (obj/data_dir/'part.1').write_bytes(label.encode()*512)
        (obj/data_dir/('part.1.integrity.'+str(uuid.UUID(int=77)))).write_bytes(label.encode()*32)
        with fault.directory(root) as fd,fault.parent(fd,['data',bucket,'conflict.bin']) as child:
            manifest=generation.tree(child,fault)[1]
            captured[label]=generation.capture(fd,owner,{**request,'generation':label,'expectedFiles':manifest},fault)
    with fault.directory(root) as fd:
        generation.install(fd,owner,{**request,'expectedSceneSha256':captured['b']['treeSha256']},fault)
        assert (obj/'xl.meta').read_bytes()==b'XL2 a'
        restored=generation.recover(fd,owner,request,fault)
        assert restored['state']=='CLEAR' and restored['originalLastACKGenerationRestored']
        assert (obj/'xl.meta').read_bytes()==b'XL2 b'
        assert generation.recover(fd,owner,request,fault)==restored
    outside=root/'outside'
    outside.mkdir()
    subprocess.run(['mount','--bind',str(outside),str(obj)],check=True)
    try:
        with fault.directory(root) as fd:
            try: generation.capture(fd,owner,{**request,'generation':'a','expectedFiles':[]},fault)
            except ValueError as error: assert 'mount' in str(error)
            else: raise AssertionError('nested bind mount was accepted')
    finally: subprocess.run(['umount',str(obj)],check=True)
print(json.dumps({'realMountDescriptors':True,'lastACKRestored':True,'nestedBindRefused':True}))
'''
        result = subprocess.run(['sudo', '-n', 'unshare', '-m', sys.executable, '-B', '-c', program],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(json.loads(result.stdout)['nestedBindRefused'])


if __name__ == '__main__':
    unittest.main()
