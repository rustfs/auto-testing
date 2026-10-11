#!/usr/bin/env python3
"""Build/list/run exact candidate native regressions; never certify fleet cases."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


CANDIDATE = "072b57b37559fbe626aeda8abf61617fdcbdd926"
SOURCE = "crates/ecstore/src/set_disk/ops/heal.rs"
SOURCE_SHA = "767c468b780cf5f4e863b078f46c63440549e75d88b6b43bb59ed83c3b4a47b2"
PREFIX = "set_disk::ops::heal::heal_result_report_tests::"
REGRESSIONS = {
    "read-repair-final-rename": {
        "test": PREFIX + "read_repair_commit_aborts_when_object_changes_before_final_rename",
        "sourceAssertions": ["arrived.notified() reaches the real final commit pause within five seconds",
            "overwrite ACK precedes release.notify_one()", "stale repair returns typed DiskError::Timeout",
            "current ETag remains the overwrite ACK"],
        "fenceKind": "deterministic assertion in the exact unmodified candidate test",
        "relatedCases": ["heal.race-unversioned-overwrite", "heal.lock-timeout-and-caller-abort"],
        "limits": "does not prove PUT cleanup-tail, Admin nolock compatibility or a fleet overlap window"},
    "read-repair-lock-owner": {
        "test": PREFIX + "object_lock_diagnostics_distinguish_read_repair_from_queued_put",
        "sourceAssertions": ["read-repair owns its actual shared data-phase lock before PUT queues",
            "PUT namespace remains pending while a historical GET times out",
            "captured acquired lock event has no premature guard_dropped event",
            "release permits acknowledged PUT and historical repair; all three versions retain bytes"],
        "fenceKind": "deterministic assertions and in-test captured lock events",
        "relatedCases": ["heal.race-versioned-put", "heal.lock-timeout-and-caller-abort"],
        "limits": "captured event rows are asserted internally; this runner cannot invent emitted barrier logs"},
    "overwrite-concurrency": {
        "test": PREFIX + "heal_racing_unversioned_overwrites_preserves_the_last_commit",
        "sourceAssertions": ["eight joined overwrite/Heal rounds preserve the last ACK ETag"],
        "fenceKind": "uncontrolled concurrency, not deterministic fence evidence",
        "relatedCases": ["heal.race-unversioned-overwrite"],
        "limits": "best-effort damage and tokio::join! do not prove a commit/cleanup critical point"},
    "version-delete-concurrency": {
        "test": PREFIX + "heal_racing_version_delete_never_resurrects_the_deleted_version",
        "sourceAssertions": ["confirmed deleted version remains VersionNotFound", "surviving version remains readable"],
        "fenceKind": "uncontrolled concurrency, not deterministic fence evidence",
        "relatedCases": ["heal.race-version-delete"],
        "limits": "does not prove a deterministic delete/repair commit fence"},
    "conflicting-authority-guard": {
        "test": PREFIX + "dangling_delete_guard_preserves_conflicting_identities_without_writing_metadata",
        "sourceAssertions": ["conflicting candidates share one explicit version ID but different data directories",
            "both original and reversed candidate order return UnsafeToDelete",
            "explicit-version regeneration refuses both conflicting authorities",
            "missing drives retain FileNotFound; both original data-directory identities remain"],
        "fenceKind": "deterministic authority decision assertions in the unmodified candidate test",
        "relatedCases": ["heal.conflicting-subquorum-authority"],
        "limits": "controlled guard decisions; does not emit or prove a concurrent runtime commit-pause fence"},
    "reconstructable-authority-preservation": {
        "test": PREFIX + "heal_meta_quorum_failure_preserves_reconstructable_uncommitted_candidate",
        "sourceAssertions": ["the actual read_all_fileinfo result is UnsafeToDelete",
            "no_lock Heal returns typed FileNotFound without deleting the surviving metadata",
            "surviving data-directory identity is unchanged; missing drives receive no manufactured metadata"],
        "fenceKind": "deterministic preservation assertions in the unmodified candidate test",
        "relatedCases": ["heal.conflicting-subquorum-authority"],
        "limits": "separate native regression; does not certify a live split, overlap or runtime pause"},
}


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024**2):
            result.update(chunk)
    return result.hexdigest()


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def exact_listing(output, name):
    tests = [line.removesuffix(': test') for line in output.splitlines() if line.endswith(': test')]
    require(tests.count(name) == 1, "native test inventory must contain exactly one complete requested name")
    require(len(tests) == len(set(tests)), "native inventory repeats a test identity")
    return len(tests)


def test_result(output, name, code):
    # A successful zero-test filter or an ignored test is not a regression run.
    require(code == 0 and len(re.findall(r"(?m)^running 1 test\s*$", output)) == 1
            and re.search(r"(?m)^test " + re.escape(name) + r" \.\.\. ok\s*$", output)
            and re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output),
            "exact native execution did not report one real passing test")
    return {"exactTestExecuted": True, "nativeRegressionPassed": True,
            "fleetPassClaimed": False, "emittedBarrierLogsClaimed": False}


def build(source, commit, out):
    require(commit == CANDIDATE and subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip() == commit,
            "native regression contract requires the reviewed immutable candidate checkout")
    require(subprocess.run(['git', '-C', str(source), 'diff', '--quiet']).returncode == 0
            and subprocess.run(['git', '-C', str(source), 'diff', '--cached', '--quiet']).returncode == 0,
            "native source has tracked modifications")
    require(not subprocess.check_output(['git', '-C', str(source), 'ls-files', '--others', '--exclude-standard'], text=True).strip(),
            "native candidate checkout has untracked source/configuration")
    require(sha(source / SOURCE) == SOURCE_SHA, "native test source hash differs from its reviewed fence assertions")
    require(not out.exists() and shutil.disk_usage(source).free >= 12 * 1024**3,
            "fresh output and at least 12 GiB build space required")
    out.mkdir(parents=True)
    save(out / 'native-build-request.json', {'candidateCommit': commit, 'sourceFiles': {SOURCE: SOURCE_SHA},
        'candidateCargoLockSha256': sha(source / 'Cargo.lock'), 'regressions': REGRESSIONS,
        'runId': os.environ.get('GITHUB_RUN_ID'), 'runAttempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
        'fleetPassClaimed': False})
    target = out / 'target'
    build_env = {'CARGO_BUILD_JOBS': '2', 'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_RELEASE_DEBUG': '0',
                 'CARGO_PROFILE_RELEASE_STRIP': 'debuginfo'}
    env = {**os.environ, **build_env, 'CARGO_TARGET_DIR': str(target)}
    cargo = ['cargo', 'test', '--locked', '--release', '-p', 'rustfs-ecstore', '--lib', '--no-run', '--message-format=json']
    # Preserve diagnostics in files; native artifacts never require modifying
    # candidate source or adding a fake fence/event to an existing test.
    with (out / 'cargo.jsonl').open('x') as stdout, (out / 'cargo.stderr').open('x') as stderr:
        result = subprocess.run(cargo, cwd=source, env=env, stdout=stdout, stderr=stderr, timeout=150 * 60)
    require(result.returncode == 0, "native regression build failed; retained Cargo diagnostics")
    require(subprocess.run(['git', '-C', str(source), 'diff', '--quiet']).returncode == 0
            and subprocess.run(['git', '-C', str(source), 'diff', '--cached', '--quiet']).returncode == 0,
            "native build changed tracked candidate source")
    executables = []
    for line in (out / 'cargo.jsonl').read_text().splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get('reason') == 'compiler-artifact' and row.get('profile', {}).get('test') is True \
                and row.get('target', {}).get('name') == 'rustfs_ecstore' and row.get('executable'):
            executables.append(Path(row['executable']))
    require(len(executables) == 1 and executables[0].resolve().is_relative_to(target.resolve()),
            "Cargo did not expose one owned candidate ecstore test executable")
    binary = out / 'ecstore-native-tests'
    shutil.copyfile(executables[0], binary)
    binary.chmod(0o500)
    listing = subprocess.run([str(binary), '--list'], capture_output=True, text=True, timeout=60)
    (out / 'test-inventory.log').write_text(listing.stdout + listing.stderr)
    require(listing.returncode == 0, "candidate native inventory failed")
    for spec in REGRESSIONS.values():
        exact_listing(listing.stdout, spec['test'])
    manifest = {'schemaVersion': 1, 'candidateCommit': commit, 'candidateSourceTree': subprocess.check_output(
        ['git', '-C', str(source), 'rev-parse', 'HEAD^{tree}'], text=True).strip(),
        'sourceFiles': {SOURCE: SOURCE_SHA}, 'candidateCargoLockSha256': sha(source / 'Cargo.lock'),
        'binarySha256': sha(binary), 'binaryName': binary.name, 'binaryKind': 'native-regression-not-released-service',
        'collectorSha256': sha(Path(__file__)), 'testingCommit': os.environ.get('AUTO_TESTING_SHA'),
        'sourceUnmodified': True, 'buildCommand': cargo, 'buildEnvironment': build_env, 'optimized': True, 'buildJobs': 2,
        'toolchain': subprocess.check_output(['rustc', '-vV'], cwd=source, text=True),
        'runId': os.environ.get('GITHUB_RUN_ID'), 'runAttempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
        'regressions': REGRESSIONS, 'fleetPassClaimed': False,
        'limits': 'native and fleet evidence are independently scored; emitted fence logs and unmapped critical points remain gaps'}
    save(out / 'native-regressions.json', manifest)
    # Remove only the build cache created under this fresh task output after
    # copying and identifying the executable and every build diagnostic.
    shutil.rmtree(target)
    return manifest


def run(directory, selected, out):
    manifest = json.loads((directory / 'native-regressions.json').read_text())
    require(manifest.get('schemaVersion') == 1 and manifest.get('candidateCommit') == CANDIDATE
            and manifest.get('sourceFiles') == {SOURCE: SOURCE_SHA} and manifest.get('sourceUnmodified') is True
            and manifest.get('regressions') == REGRESSIONS and manifest.get('optimized') is True
            and manifest.get('collectorSha256') == sha(Path(__file__)),
            "native artifact identity or reviewed contract differs")
    require(manifest.get('binaryName') == 'ecstore-native-tests', "native binary path is not allowlisted")
    binary = directory / manifest['binaryName']
    require(binary.is_file() and not binary.is_symlink() and sha(binary) == manifest['binarySha256'], "native binary changed")
    require(selected and len(selected) == len(set(selected)) and set(selected) <= set(REGRESSIONS), "invalid native selection")
    require(not out.exists(), "native evidence directory must be fresh")
    out.mkdir(parents=True)
    listing = subprocess.run([str(binary), '--list'], capture_output=True, text=True, timeout=60)
    (out / 'runtime-inventory.log').write_text(listing.stdout + listing.stderr)
    require(listing.returncode == 0, "native runtime inventory failed")
    results = []
    for key in selected:
        spec = REGRESSIONS[key]
        exact_listing(listing.stdout, spec['test'])
        require(not binary.is_symlink() and sha(binary) == manifest['binarySha256'], "native executable changed before exact execution")
        args = [str(binary), spec['test'], '--exact', '--nocapture', '--test-threads=1']
        started = time.monotonic_ns()
        record = {'regression': key, 'test': spec['test'], 'command': args, 'sourceAssertions': spec['sourceAssertions'],
            'binarySha256': manifest['binarySha256'], 'candidateCommit': CANDIDATE, 'fleetPassClaimed': False,
            'emittedBarrierLogsClaimed': False, 'fenceKind': spec['fenceKind'], 'limitations': spec['limits']}
        try:
            child = subprocess.run(args, capture_output=True, text=True, timeout=120)
            output = child.stdout + child.stderr
            (out / (key + '.log')).write_text(output)
            record['exitCode'] = child.returncode
            test_result(output, spec['test'], child.returncode)
            record['verdict'] = 'PASS'
        except subprocess.TimeoutExpired as error:
            def text_part(value):
                return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
            output = text_part(error.stdout) + text_part(error.stderr)
            (out / (key + '.log')).write_text(output)
            record.update(verdict='FAIL', reason='native test exceeded its fixed 120-second deadline')
        except ValueError as error:
            record.update(verdict='FAIL', reason=str(error))
        except OSError as error:
            (out / (key + '.log')).write_text(str(error))
            record.update(verdict='INFRA_ERROR', reason='native test executable could not start: ' + str(error))
        record['elapsedNanoseconds'] = time.monotonic_ns() - started
        save(out / (key + '.json'), record)
        results.append(record)
    save(out / 'native-summary.json', {'results': results, 'fleetPassClaimed': False,
        'complete': len(results) == len(selected) and all(row['verdict'] == 'PASS' for row in results),
        'canonicalCasePassClaimed': False, 'sourceUnmodified': True})
    require(all(row['verdict'] == 'PASS' for row in results), 'native regression failure remains in separate evidence')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    compiling = commands.add_parser('build')
    compiling.add_argument('--source', type=Path, required=True)
    compiling.add_argument('--commit', required=True)
    compiling.add_argument('--out', type=Path, required=True)
    running = commands.add_parser('run')
    running.add_argument('--directory', type=Path, required=True)
    running.add_argument('--selection', default=','.join(REGRESSIONS))
    running.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'build':
        build(args.source.resolve(strict=True), args.commit, args.out.resolve())
    else:
        run(args.directory.resolve(strict=True), args.selection.split(','), args.out.resolve())


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'native validation failed: {error}', file=sys.stderr)
        sys.exit(1)
