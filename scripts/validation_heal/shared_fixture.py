#!/usr/bin/env python3
"""Verify a separately built C10 encoder; ordinary execution needs no checkout."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import stat

try:
    from .authority import SOURCE_TREE, CARGO_LOCK
    from .heal_probe import require
    from .native_regressions import CANDIDATE
    from .prepare_heal_shared_fixture import ROLE
except ImportError:
    from authority import SOURCE_TREE, CARGO_LOCK
    from heal_probe import require
    from native_regressions import CANDIDATE
    from prepare_heal_shared_fixture import ROLE


TESTS = ('tests::exact_existing_versions_share_one_directory_without_changing_other_metadata',
         'tests::malformed_absent_same_and_already_shared_versions_are_refused',
         'tests::incompatible_part_and_deleted_destination_are_refused')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(directory, candidate):
    directory = Path(directory)
    paths = {name: directory / name for name in ('heal-shared-fixture', 'heal-shared-fixture.json',
        'shared-Cargo.lock', 'shared-fixture-tests.log', 'tool-run.json')}
    for path in paths.values():
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= 256 * 1024**2,
                'C10 fixture artifact must contain bounded original regular files')
    manifest = json.loads(paths['heal-shared-fixture.json'].read_text())
    run = json.loads(paths['tool-run.json'].read_text())
    require(type(run.get('id')) is int and run['id'] > 0 and type(run.get('run_attempt')) is int and run['run_attempt'] > 0
            and run.get('status') == 'completed' and run.get('conclusion') == 'success'
            and run.get('path') in {'.github/workflows/rustfs-functional-chain.yml', '.github/workflows/rustfs-validation-tools.yml'}
            and manifest.get('runId') == str(run['id']) and manifest.get('runAttempt') == str(run['run_attempt']),
            'C10 encoder is not bound to one pinned successful source build attempt')
    helper = Path(__file__).with_name('tools') / 'heal_shared_version_fixture.rs'
    require(candidate['source_sha'] == CANDIDATE and manifest.get('schemaVersion') == 1
            and manifest.get('candidateCommit') == CANDIDATE and manifest.get('candidateSourceTree') == SOURCE_TREE
            and manifest.get('candidateLockSha256') == CARGO_LOCK and manifest.get('built') is True
            and manifest.get('sourceUnmodified') is True and manifest.get('target') == 'x86_64-unknown-linux-gnu'
            and manifest.get('role') == ROLE and manifest.get('filesystemMutationPerformed') is False
            and manifest.get('physicalByteEqualityClaimed') is False
            and manifest.get('helperSourceSha256') == sha(helper)
            and manifest.get('binarySha256') == sha(paths['heal-shared-fixture'])
            and manifest.get('sharedLockSha256') == sha(paths['shared-Cargo.lock']),
            'C10 encoder changed its source/codec/build/artifact or claimed unobserved physical equality')
    with paths['heal-shared-fixture'].open('rb') as stream:
        header = stream.read(20)
    require(header[:6] == b'\x7fELF\x02\x01' and header[18:20] == b'\x3e\x00', 'C10 encoder is not Linux x86_64 ELF')
    log = paths['shared-fixture-tests.log'].read_text()
    require(re.search(r'(?m)^running 3 tests\s*$', log)
            and re.search(r'test result: ok\. 3 passed; 0 failed; 0 ignored;', log)
            and all(len(re.findall(r'(?m)^test ' + re.escape(name) + r' \.\.\. ok\s*$', log)) == 1 for name in TESTS),
            'C10 codec build did not execute all three original transformation/refusal tests')
    return {**manifest, 'toolsBuildRun': run['id'], 'toolsBuildAttempt': run['run_attempt'],
        'exactCodecTestsPassed': True, 'fleetPassClaimed': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory, json.loads(args.candidate.read_text())), indent=2))
