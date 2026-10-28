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
INCARNATION_CANDIDATE = "1aa9be87d18cf941e5020f19e8622c26ba4b10ac"
INCARNATION_EVENTS = ["final_rename_paused", "caller_aborted_writer_pending",
                      "repair_drained_writer_acquired", "successor_rejects_obsolete_admission"]
STORE_PREFIX = "store::heal::tests::"
INCARNATION_REGRESSIONS = {
    "incarnation-successor-storage": {
        "test": STORE_PREFIX + "bucket_incarnation_heal_preserves_successor_shards_and_allows_fresh_repair",
        "sourceAssertions": ["normal delete/recreate changes the authoritative bucket UUID",
            "name-only positive control repairs a real missing successor shard",
            "old-incarnation metadata/path/version/rename/bucket/object mutations are rejected",
            "obsolete admission publishes no missing shard and leaves healthy metadata byte-identical",
            "fresh successor admission independently repairs the missing shard"],
        "fenceKind": "real physical storage incarnation guard decisions",
        "relatedCases": ["heal.bucket-incarnation-recreation"],
        "limits": "does not prove fleet checkpoint or MRF replay"},
    "incarnation-cancelled-owner": {
        "test": STORE_PREFIX + "bucket_incarnation_heal_owner_survives_cancelled_waiter_and_queued_writer",
        "sourceAssertions": ["cancelled caller retains the original lifecycle owner",
            "queued writer remains pending until storage proof drains",
            "exact incarnation absence proof spans both selected pools without reacquiring behind the writer"],
        "fenceKind": "deterministic held-owner and queued-writer assertions",
        "relatedCases": ["heal.bucket-incarnation-recreation"],
        "limits": "separate owner regression; not a physical final-rename pause"},
    "incarnation-final-rename": {
        "test": STORE_PREFIX + "bucket_incarnation_fence_survives_caller_abort_at_real_repair_commit",
        "sourceAssertions": ["real external exact-version reconstruction pauses before final scope check and rename",
            "missing target is unpublished at the pause",
            "caller abort cannot admit the lifecycle writer before repair drains",
            "the published exact version/data directory passes physical check_parts verification",
            "healthy metadata remains byte-identical",
            "normal version/bucket deletion and recreation reject obsolete admission without changing successor metadata"],
        "fenceKind": "deterministic actual reconstruction/final-rename barrier with emitted assertion milestones",
        "relatedCases": ["heal.bucket-incarnation-recreation"], "barrierEvents": INCARNATION_EVENTS,
        "limits": "native storage commit evidence only; not canonical fleet checkpoint/MRF replay"},
}
CARGO_LOCK_SHA = "d8f7bc416ba67d19873d34e9389e5aa823b22d7d7552706a5a810cee26f6eda2"
BUILD_ENV = {'CARGO_BUILD_JOBS': '2', 'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_RELEASE_DEBUG': '0',
             'CARGO_PROFILE_RELEASE_STRIP': 'debuginfo'}
BUILD_COMMAND = ['cargo', 'test', '--locked', '--release', '-p', 'rustfs-ecstore', '--lib', '--no-run', '--message-format=json']
REVIEWED_CANDIDATES = {
    CANDIDATE: {'sourceTree': '8071fb0a9e5353eb7d3fa58c8a58c8622473f2bd',
        'sourceFiles': {SOURCE: SOURCE_SHA}, 'regressions': REGRESSIONS},
    INCARNATION_CANDIDATE: {'sourceTree': 'd57bcda8b1551727ced7073ddd09f8c97c13485f',
        'sourceFiles': {
            SOURCE: 'a375fbf105d2491e1582c65c417509ac3eff23db826db788eb0aeb629e07f6b8',
            'crates/ecstore/src/store/heal.rs': '5ecf643f43f6f61f5b13f777f68faafba37e58a98eecb8355254f2ea75a1e9b1',
            'crates/ecstore/src/store/bucket.rs': 'a264de25fc4d3693ed962ea3613055b90f7092d0eb1cc748895030ab8adf1ae3',
            'crates/ecstore/src/store/bucket_fence.rs': '6f9d97cdd580bbd91f79af5723ff4712065b3d054cb5590db35fc00c79e1f262',
            'crates/ecstore/src/store/peer.rs': '85636a2536efc9e14fef0e1fe3d11de53093c45df44951b8fd08e4ed1edbf251',
            'crates/ecstore/src/set_disk/mod.rs': 'd4280fb89752f6198cb5641ea8ed23f5a41a4c87688e10bfce80a548e302e14f'},
        'regressions': {**REGRESSIONS, **INCARNATION_REGRESSIONS}},
}


def runtime_environment(commit):
    # Match the candidate's nextest ecstore stack setup for real storage tests.
    return {'RUST_MIN_STACK': '33554432'} if commit == INCARNATION_CANDIDATE else {}


def exact_command(binary, name, commit):
    # Captured output keeps libtest's result line intact while retaining every
    # successful test milestone; nocapture interleaves the first milestone.
    output_flag = '--show-output' if commit == INCARNATION_CANDIDATE else '--nocapture'
    return [str(binary), name, '--exact', output_flag, '--test-threads=1']


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
    require(type(code) is int and code == 0
            and re.findall(r"(?m)^running .*", output) == ['running 1 test']
            and re.findall(r"(?m)^test (?!result:).*", output) == [f'test {name} ... ok']
            and len(re.findall(r"(?m)^test result:.*", output)) == 1
            and re.search(r"(?m)^test result: ok\. 1 passed; 0 failed; 0 ignored;", output),
            "exact native execution did not report one real passing test")
    return {"exactTestExecuted": True, "nativeRegressionPassed": True,
            "fleetPassClaimed": False, "emittedBarrierLogsClaimed": False}


def barrier_events(output, spec):
    events = re.findall(r"(?m)^incarnation_commit_barrier: ([a-z_]+)\s*$", output)
    require(events == spec.get('barrierEvents', []), "native barrier milestones are absent, duplicated or out of order")
    return events


def verify_manifest(directory):
    manifest = json.loads((directory / 'native-regressions.json').read_text())
    contract = REVIEWED_CANDIDATES.get(manifest.get('candidateCommit'))
    require(contract is not None and manifest.get('schemaVersion') == 1
            and manifest.get('candidateSourceTree') == contract['sourceTree']
            and manifest.get('candidateCargoLockSha256') == CARGO_LOCK_SHA
            and manifest.get('sourceFiles') == contract['sourceFiles']
            and manifest.get('regressions') == contract['regressions'] and manifest.get('sourceUnmodified') is True
            and manifest.get('optimized') is True and manifest.get('buildEnvironment') == BUILD_ENV
            and manifest.get('buildCommand') == BUILD_COMMAND
            and manifest.get('binaryKind') == 'native-regression-not-released-service'
            and manifest.get('runtimeEnvironment') == runtime_environment(manifest.get('candidateCommit'))
            and manifest.get('collectorSha256') == sha(Path(__file__)) and manifest.get('fleetPassClaimed') is False,
            "native artifact identity or reviewed contract differs")
    require(manifest.get('binaryName') == 'ecstore-native-tests', "native binary path is not allowlisted")
    binary = directory / manifest['binaryName']
    require(binary.is_file() and not binary.is_symlink() and sha(binary) == manifest.get('binarySha256'), "native binary changed")
    return manifest, binary, contract


def build(source, commit, out):
    contract = REVIEWED_CANDIDATES.get(commit)
    require(contract is not None and subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip() == commit,
            "native regression contract requires the reviewed immutable candidate checkout")
    require(subprocess.run(['git', '-C', str(source), 'diff', '--quiet']).returncode == 0
            and subprocess.run(['git', '-C', str(source), 'diff', '--cached', '--quiet']).returncode == 0,
            "native source has tracked modifications")
    require(not subprocess.check_output(['git', '-C', str(source), 'ls-files', '--others', '--exclude-standard'], text=True).strip(),
            "native candidate checkout has untracked source/configuration")
    require(subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD^{tree}'], text=True).strip() == contract['sourceTree']
            and sha(source / 'Cargo.lock') == CARGO_LOCK_SHA
            and all(sha(source / path) == digest for path, digest in contract['sourceFiles'].items()),
            "native source/tree/lock differs from its reviewed fence assertions")
    require(not out.exists() and shutil.disk_usage(source).free >= 12 * 1024**3,
            "fresh output and at least 12 GiB build space required")
    out.mkdir(parents=True)
    save(out / 'native-build-request.json', {'candidateCommit': commit, 'sourceFiles': contract['sourceFiles'],
        'candidateCargoLockSha256': sha(source / 'Cargo.lock'), 'regressions': contract['regressions'],
        'runId': os.environ.get('GITHUB_RUN_ID'), 'runAttempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
        'fleetPassClaimed': False})
    target = out / 'target'
    build_env = BUILD_ENV
    env = {**os.environ, **build_env, 'CARGO_TARGET_DIR': str(target)}
    cargo = BUILD_COMMAND
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
    for spec in contract['regressions'].values():
        exact_listing(listing.stdout, spec['test'])
    manifest = {'schemaVersion': 1, 'candidateCommit': commit, 'candidateSourceTree': subprocess.check_output(
        ['git', '-C', str(source), 'rev-parse', 'HEAD^{tree}'], text=True).strip(),
        'sourceFiles': contract['sourceFiles'], 'candidateCargoLockSha256': sha(source / 'Cargo.lock'),
        'binarySha256': sha(binary), 'binaryName': binary.name, 'binaryKind': 'native-regression-not-released-service',
        'collectorSha256': sha(Path(__file__)), 'testingCommit': os.environ.get('AUTO_TESTING_SHA'),
        'sourceUnmodified': True, 'buildCommand': cargo, 'buildEnvironment': build_env, 'optimized': True, 'buildJobs': 2,
        'runtimeEnvironment': runtime_environment(commit),
        'toolchain': subprocess.check_output(['rustc', '-vV'], cwd=source, text=True),
        'runId': os.environ.get('GITHUB_RUN_ID'), 'runAttempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
        'regressions': contract['regressions'], 'fleetPassClaimed': False,
        'limits': 'native and fleet evidence are independently scored; only reviewed tests with verified original milestones claim emitted barriers'}
    save(out / 'native-regressions.json', manifest)
    # Remove only the build cache created under this fresh task output after
    # copying and identifying the executable and every build diagnostic.
    shutil.rmtree(target)
    return manifest


def run(directory, selected, out):
    manifest, binary, contract = verify_manifest(directory)
    regressions = contract['regressions']
    selected = list(regressions) if selected is None else selected
    require(selected and len(selected) == len(set(selected)) and set(selected) <= set(regressions), "invalid native selection")
    require(not out.exists(), "native evidence directory must be fresh")
    out.mkdir(parents=True)
    listing = subprocess.run([str(binary), '--list'], capture_output=True, text=True, timeout=60)
    (out / 'runtime-inventory.log').write_text(listing.stdout + listing.stderr)
    require(listing.returncode == 0, "native runtime inventory failed")
    results = []
    for key in selected:
        spec = regressions[key]
        exact_listing(listing.stdout, spec['test'])
        require(not binary.is_symlink() and sha(binary) == manifest['binarySha256'], "native executable changed before exact execution")
        args = exact_command(binary, spec['test'], manifest['candidateCommit'])
        started = time.monotonic_ns()
        record = {'regression': key, 'test': spec['test'], 'command': args, 'sourceAssertions': spec['sourceAssertions'],
            'binarySha256': manifest['binarySha256'], 'candidateCommit': manifest['candidateCommit'], 'fleetPassClaimed': False,
            'emittedBarrierLogsClaimed': False, 'fenceKind': spec['fenceKind'], 'limitations': spec['limits']}
        record['runtimeEnvironment'] = manifest['runtimeEnvironment']
        try:
            child = subprocess.run(args, capture_output=True, text=True, timeout=120,
                env={**os.environ, **manifest['runtimeEnvironment']})
            output = child.stdout + child.stderr
            (out / (key + '.log')).write_text(output)
            record['exitCode'] = child.returncode
            test_result(output, spec['test'], child.returncode)
            events = barrier_events(output, spec)
            if events:
                record.update(observedBarrierEvents=events, emittedBarrierLogsClaimed=True)
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


def verify_evidence(directory, evidence, candidate, run_id, run_attempt, helper_sha):
    manifest, binary, contract = verify_manifest(directory)
    require(manifest.get('candidateCommit') == candidate['source_sha'] and manifest.get('runId') == str(run_id)
            and manifest.get('runAttempt') == str(run_attempt) and manifest.get('testingCommit') == helper_sha
            and isinstance(helper_sha, str) and re.fullmatch(r'[0-9a-f]{40}', helper_sha),
            "native evidence belongs to another candidate, run attempt or helper")
    summary = json.loads((evidence / 'native-summary.json').read_text())
    rows = summary.get('results', [])
    regressions = contract['regressions']
    require(summary.get('complete') is True and summary.get('sourceUnmodified') is True
            and summary.get('fleetPassClaimed') is False and summary.get('canonicalCasePassClaimed') is False
            and len(rows) == len(regressions) and {row.get('regression') for row in rows} == set(regressions),
            "native summary omits or duplicates a required test or claims fleet success")
    listings = [(directory / 'test-inventory.log').read_text(), (evidence / 'runtime-inventory.log').read_text()]
    observed = {}
    for row in rows:
        key = row['regression']
        spec = regressions[key]
        require(row == json.loads((evidence / (key + '.json')).read_text()) and row.get('verdict') == 'PASS'
                and row.get('candidateCommit') == manifest['candidateCommit'] and row.get('binarySha256') == manifest['binarySha256']
                and all(row.get(name) == spec[name] for name in ('test', 'sourceAssertions', 'fenceKind'))
                and row.get('limitations') == spec['limits'] and row.get('fleetPassClaimed') is False,
                "native result changed exact source, scope or executable identity")
        command = row.get('command', [])
        require(len(command) == 5 and Path(command[0]).name == binary.name
                and command[1:] == exact_command(binary, spec['test'], manifest['candidateCommit'])[1:]
                and row.get('runtimeEnvironment') == manifest['runtimeEnvironment'],
                "native result has no exact one-test invocation")
        for listing in listings: exact_listing(listing, spec['test'])
        output = (evidence / (key + '.log')).read_text()
        test_result(output, spec['test'], row.get('exitCode'))
        events = barrier_events(output, spec)
        require(row.get('emittedBarrierLogsClaimed') is bool(events)
                and (row.get('observedBarrierEvents') == events if events else 'observedBarrierEvents' not in row),
                "native report invented or omitted original barrier evidence")
        if events: observed[key] = events
    return {'complete': True, 'candidateCommit': manifest['candidateCommit'], 'binarySha256': manifest['binarySha256'],
            'exactTests': len(rows), 'observedBarrierEvents': observed, 'fleetPassClaimed': False, 'canonicalCasePassClaimed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    compiling = commands.add_parser('build')
    compiling.add_argument('--source', type=Path, required=True)
    compiling.add_argument('--commit', required=True)
    compiling.add_argument('--out', type=Path, required=True)
    running = commands.add_parser('run')
    running.add_argument('--directory', type=Path, required=True)
    running.add_argument('--selection')
    running.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'build':
        build(args.source.resolve(strict=True), args.commit, args.out.resolve())
    else:
        run(args.directory.resolve(strict=True), args.selection.split(',') if args.selection is not None else None, args.out.resolve())


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'native validation failed: {error}', file=sys.stderr)
        sys.exit(1)
