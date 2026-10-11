"""C10 controlled-codec build identity and preservation guards."""

import json
from pathlib import Path
import tempfile
import unittest

from scripts.validation_heal import shared_fixture as shared


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


if __name__ == '__main__':
    unittest.main()
