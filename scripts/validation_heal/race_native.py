#!/usr/bin/env python3
"""Exact-source race fences, separate from retained legacy native artifacts."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys

CONTRACTS = Path(__file__).with_name('race_native_contracts.json')
BUILD_ENV = {'CARGO_BUILD_JOBS': '2', 'CARGO_INCREMENTAL': '0',
             'CARGO_PROFILE_RELEASE_DEBUG': '0', 'CARGO_PROFILE_RELEASE_STRIP': 'debuginfo'}
BUILD_COMMAND = ['cargo', 'test', '--locked', '--release', '-p', 'rustfs-ecstore',
                 '--lib', '--no-run', '--message-format=json']
RUNTIME_ENV = {'RUST_MIN_STACK': '33554432'}
BINARY = 'ecstore-race-native-tests'
KIND = 'exact-source-race-native-not-release-service'
CASE_IDS = {'heal.admin-nolock-safety', 'heal.race-unversioned-overwrite', 'heal.race-versioned-put',
            'heal.race-version-delete', 'heal.race-markers-and-batch-delete',
            'heal.race-multipart-complete-abort', 'heal.race-copy-and-metadata-update',
            'heal.lock-timeout-and-caller-abort'}


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024**2):
            value.update(chunk)
    return value.hexdigest()


def save(path, value):
    with path.open('x') as out:
        json.dump(value, out, indent=2)
        out.write('\n')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def registry():
    require(CONTRACTS.is_file() and not CONTRACTS.is_symlink() and CONTRACTS.stat().st_size < 1024**2,
            'bounded regular race native registry required')
    value = json.loads(CONTRACTS.read_text())
    require(isinstance(value, dict) and type(value.get('schemaVersion')) is int and value['schemaVersion'] == 1
            and set(value) == {'schemaVersion', 'candidates'} and isinstance(value['candidates'], dict)
            and len(value['candidates']) <= 256
            and all(re.fullmatch(r'[0-9a-f]{40}', key) for key in value['candidates']),
            'race native registry schema or candidate identity differs')
    return value['candidates']


def contract_for(commit):
    require(isinstance(commit, str) and re.fullmatch(r'[0-9a-f]{40}', commit),
            'native source must be an exact lowercase commit SHA')
    contract = registry().get(commit)
    require(contract is not None, 'race native producer source is not reviewed')
    require(isinstance(contract, dict) and set(contract) == {'sourceTree', 'cargoLockSha256', 'sourceFiles', 'regressions', 'cases'},
            'race native contract fields differ')
    require(re.fullmatch(r'[0-9a-f]{40}', contract['sourceTree'])
            and re.fullmatch(r'[0-9a-f]{64}', contract['cargoLockSha256'])
            and isinstance(contract['sourceFiles'], dict) and 1 <= len(contract['sourceFiles']) <= 64
            and isinstance(contract['regressions'], dict) and 1 <= len(contract['regressions']) <= 16
            and isinstance(contract['cases'], dict) and 1 <= len(contract['cases']) <= len(CASE_IDS),
            'incomplete reviewed race native contract')
    for path, digest in contract['sourceFiles'].items():
        relative = PurePosixPath(path)
        require(len(path) <= 256 and not relative.is_absolute() and '..' not in relative.parts
                and '\\' not in path and path.startswith('crates/') and path.endswith('.rs')
                and re.fullmatch(r'[0-9a-f]{64}', digest), 'native source path or digest differs')
    for label, spec in contract['regressions'].items():
        require(re.fullmatch(r'[a-z][a-z0-9-]{0,63}', label) and isinstance(spec, dict)
                and set(spec) == {'test', 'sourcePath', 'sourceAssertions', 'witnessKind', 'milestonePrefix', 'milestones'},
                'native regression fields differ')
        require(isinstance(spec['test'], str) and len(spec['test']) <= 512
                and re.fullmatch(r'(set_disk|store)(::[A-Za-z_][A-Za-z0-9_]*)+', spec['test'])
                and spec['sourcePath'] in contract['sourceFiles']
                and isinstance(spec['sourceAssertions'], list) and 1 <= len(spec['sourceAssertions']) <= 32
                and all(isinstance(item, str) and 0 < len(item) <= 512 for item in spec['sourceAssertions']),
                'native test identity or source assertions differ')
        require(isinstance(spec['milestones'], list) and len(spec['milestones']) <= 128
                and all(isinstance(item, str) and 0 < len(item) <= 512 and '\n' not in item and '\r' not in item
                        for item in spec['milestones'])
                and len(set(spec['milestones'])) == len(spec['milestones']), 'native milestone contract is malformed')
        if spec['witnessKind'] == 'ordered-barrier-milestones':
            require(isinstance(spec['milestonePrefix'], str)
                    and re.fullmatch(r'native_(a09|g0[1-6]|i05): ', spec['milestonePrefix'])
                    and spec['milestones'] and all(item.startswith(spec['milestonePrefix']) for item in spec['milestones']),
                    'native ordered milestone namespace differs')
        else:
            require(spec['witnessKind'] == 'reviewed-in-test-assertions'
                    and spec['milestonePrefix'] is None and spec['milestones'] == [], 'native witness kind differs')
    used = set()
    for case, labels in contract['cases'].items():
        require(case in CASE_IDS and isinstance(labels, list) and labels
                and all(isinstance(label, str) and label in contract['regressions'] for label in labels)
                and len(labels) == len(set(labels)), 'native case lost its exact required regression group')
        used.update(labels)
    require(used == set(contract['regressions']), 'native regressions contain an unbound case subset')
    return contract


def exact_result(output, name, code):
    require(type(code) is int and code == 0
            and re.findall(r'(?m)^running .*', output) == ['running 1 test']
            and re.findall(r'(?m)^test (?!result:).*', output) == [f'test {name} ... ok']
            and len(re.findall(r'(?m)^test result:.*', output)) == 1
            and re.search(r'(?m)^test result: ok\. 1 passed; 0 failed; 0 ignored;', output),
            'native execution must run one exact nonignored passing test')


def milestones(output, spec):
    if spec.get('witnessKind') == 'reviewed-in-test-assertions':
        require(spec.get('milestones') == [] and spec.get('milestonePrefix') is None
                and bool(spec.get('sourceAssertions')), 'native in-test assertion contract is incomplete')
        return []
    require(spec.get('witnessKind') == 'ordered-barrier-milestones', 'unreviewed native witness kind')
    prefix = spec['milestonePrefix']
    require(re.fullmatch(r'native_(a09|g0[1-6]|i05): ', prefix), 'unreviewed native milestone namespace')
    observed = [line for line in output.splitlines() if line.startswith(prefix)]
    require(bool(spec['milestones']) and observed == spec['milestones'],
            'actual native milestones are absent, duplicated or out of order')
    return observed


def verify(directory):
    require(directory.is_dir() and not directory.is_symlink(), 'native artifact directory is not regular')
    path = directory / 'race-native.json'
    require(path.is_file() and not path.is_symlink() and path.stat().st_size < 1024**2,
            'bounded regular race native manifest required')
    value = json.loads(path.read_text())
    contract = contract_for(value.get('candidateCommit'))
    require(type(value.get('schemaVersion')) is int and value['schemaVersion'] == 1 and value.get('artifactKind') == KIND
            and value.get('candidateSourceTree') == contract['sourceTree']
            and value.get('candidateCargoLockSha256') == contract['cargoLockSha256']
            and value.get('sourceFiles') == contract['sourceFiles']
            and value.get('regressions') == contract['regressions']
            and value.get('cases') == contract['cases']
            and value.get('buildEnvironment') == BUILD_ENV and value.get('buildCommand') == BUILD_COMMAND
            and value.get('runtimeEnvironment') == RUNTIME_ENV and value.get('optimized') is True
            and value.get('sourceUnmodified') is True and value.get('binaryName') == BINARY
            and value.get('collectorSha256') == sha(Path(__file__))
            and value.get('selectedContractSha256') == hashlib.sha256(canonical(contract)).hexdigest()
            and value.get('canonicalCasePassClaimed') is False and value.get('fleetExecutionClaimed') is False,
            'race native artifact identity or reviewed contract differs')
    require(isinstance(value.get('testingCommit'), str)
            and re.fullmatch(r'[0-9a-f]{40}', value['testingCommit']), 'native helper identity is absent')
    require(isinstance(value.get('driverCommit'), str) and re.fullmatch(r'[0-9a-f]{40}', value['driverCommit'])
            and isinstance(value.get('registrySourceSha256'), str)
            and re.fullmatch(r'[0-9a-f]{64}', value['registrySourceSha256']), 'native driver or registry source identity is absent')
    require(all(isinstance(value.get(key), str) and re.fullmatch(r'[1-9][0-9]*', value[key])
                for key in ('runId', 'runAttempt')), 'native build attempt identity is absent')
    binary = directory / BINARY
    require(binary.is_file() and not binary.is_symlink() and sha(binary) == value.get('binarySha256'),
            'exact race native executable changed')
    return value, binary


def build(source, commit, testing_commit, out):
    contract = contract_for(commit)
    helper_root = Path(__file__).resolve().parents[2]
    git = lambda *args: subprocess.check_output(['git', '-C', str(source), *args], text=True).strip()
    require(git('rev-parse', 'HEAD') == commit and git('rev-parse', 'HEAD^{tree}') == contract['sourceTree'],
            'candidate checkout differs from reviewed source')
    require(not git('status', '--porcelain', '--untracked-files=all'), 'native source has modified or untracked files')
    require(all(re.fullmatch(r'[1-9][0-9]*', os.environ.get(key, ''))
                for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT'))
            and re.fullmatch(r'[0-9a-f]{40}', os.environ.get('GITHUB_SHA', '')),
            'native producer requires an exact workflow build attempt')
    require(re.fullmatch(r'[0-9a-f]{40}', testing_commit)
            and subprocess.check_output(['git', '-C', str(helper_root), 'rev-parse', 'HEAD'], text=True).strip() == testing_commit,
            'native helper checkout differs from exact requested commit')
    require(sha(source / 'Cargo.lock') == contract['cargoLockSha256']
            and all((source / path).is_file() and not (source / path).is_symlink()
                    and (source / path).resolve() == source / path and sha(source / path) == digest
                    for path, digest in contract['sourceFiles'].items()),
            'candidate lock or runtime-fence source differs')
    collector_digest, registry_digest = sha(Path(__file__)), sha(CONTRACTS)
    for path in [Path(__file__), CONTRACTS]:
        relative = path.relative_to(helper_root).as_posix()
        committed = subprocess.check_output(['git', '-C', str(helper_root), 'show', testing_commit + ':' + relative])
        require(committed == path.read_bytes(), 'native helper registry or collector has tracked changes')
    for key in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER',
                'CARGO_BUILD_RUSTFLAGS', 'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS'):
        require(not os.environ.get(key), 'native build has an unbound compiler override')
    require(not out.exists() and shutil.disk_usage(source).free >= 12 * 1024**3,
            'fresh native output and build disk space required')
    out.mkdir(parents=True)
    target = out / 'target'
    request = {'candidateCommit': commit, 'candidateSourceTree': contract['sourceTree'],
               'candidateCargoLockSha256': contract['cargoLockSha256'], 'testingCommit': testing_commit,
               'sourceFiles': contract['sourceFiles'], 'regressions': contract['regressions'],
               'cases': contract['cases'],
               'runId': os.environ.get('GITHUB_RUN_ID'), 'runAttempt': os.environ.get('GITHUB_RUN_ATTEMPT'),
               'artifactKind': KIND, 'canonicalCasePassClaimed': False, 'fleetExecutionClaimed': False}
    request.update(selectedContractSha256=hashlib.sha256(canonical(contract)).hexdigest(),
                   registrySourceSha256=registry_digest, driverCommit=os.environ.get('GITHUB_SHA'))
    save(out / 'race-native-build-request.json', request)
    env = {**os.environ, **BUILD_ENV, 'CARGO_TARGET_DIR': str(target)}
    with (out / 'cargo.jsonl').open('x') as stdout, (out / 'cargo.stderr').open('x') as stderr:
        result = subprocess.run(BUILD_COMMAND, cwd=source, env=env, stdout=stdout, stderr=stderr, timeout=9000)
    require(result.returncode == 0, 'native build failed; preserve original diagnostics')
    require(not git('status', '--porcelain', '--untracked-files=all'), 'native build changed candidate source')
    require(sha(Path(__file__)) == collector_digest and sha(CONTRACTS) == registry_digest,
            'native build changed its helper source or registry')
    executables = []
    for line in (out / 'cargo.jsonl').read_text().splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (row.get('reason') == 'compiler-artifact' and row.get('profile', {}).get('test') is True
                and row.get('target', {}).get('name') == 'rustfs_ecstore' and row.get('executable')):
            executables.append(Path(row['executable']))
    require(len(executables) == 1 and not executables[0].is_symlink()
            and executables[0].resolve().is_relative_to(target.resolve()), 'native build executable escaped its target')
    binary = out / BINARY
    shutil.copyfile(executables[0], binary)
    binary.chmod(0o500)
    listing = subprocess.run([str(binary), '--list'], capture_output=True, text=True, timeout=60)
    (out / 'test-inventory.log').write_text(listing.stdout + listing.stderr)
    require(listing.returncode == 0, 'native test inventory failed')
    names = [line.removesuffix(': test') for line in listing.stdout.splitlines() if line.endswith(': test')]
    require(len(names) == len(set(names)), 'native test inventory duplicates an identity')
    require(all(names.count(spec['test']) == 1 for spec in contract['regressions'].values()),
            'required exact native test is absent or repeated')
    save(out / 'race-native.json', {**request, 'schemaVersion': 1, 'optimized': True, 'sourceUnmodified': True,
         'binaryName': BINARY, 'binarySha256': sha(binary), 'collectorSha256': sha(Path(__file__)),
         'buildEnvironment': BUILD_ENV, 'buildCommand': BUILD_COMMAND, 'runtimeEnvironment': RUNTIME_ENV})


def run(directory, out):
    value, binary = verify(directory)
    require(not out.exists(), 'retain original native run; no automatic retries')
    out.mkdir(parents=True)
    rows = []
    for label, spec in value['regressions'].items():
        require(verify(directory)[0] == value, 'native identity changed before execution')
        require(re.fullmatch(r'[a-z][a-z0-9-]*', label), 'invalid reviewed native label')
        command = [str(binary), spec['test'], '--exact', '--show-output', '--test-threads=1']
        result = subprocess.run(command, env={**os.environ, **RUNTIME_ENV}, capture_output=True, text=True, timeout=300)
        log = out / (label + '.log')
        log.write_text(result.stdout + result.stderr)
        require(verify(directory)[0] == value, 'native identity changed during execution')
        exact_result(result.stdout, spec['test'], result.returncode)
        observed = milestones(result.stdout, spec)
        rows.append({'label': label, 'test': spec['test'], 'command': command, 'nativeRegressionPassed': True,
                     'exitCode': result.returncode, 'runtimeEnvironment': RUNTIME_ENV,
                     'milestones': observed, 'originalLogSha256': sha(log)})
    save(out / 'race-native-results.json', {'schemaVersion': 1, 'candidateCommit': value['candidateCommit'],
         'candidateSourceTree': value['candidateSourceTree'], 'binarySha256': value['binarySha256'],
         'nativeManifestSha256': sha(directory / 'race-native.json'), 'results': rows,
         'requiredNativeSubsetsComplete': True, 'canonicalCasePassClaimed': False, 'fleetExecutionClaimed': False})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    supports_parser = sub.add_parser('supports')
    supports_parser.add_argument('--commit', required=True)
    supports_parser.add_argument('--case-id', choices=sorted(CASE_IDS))
    build_parser = sub.add_parser('build')
    build_parser.add_argument('--source', type=Path, required=True)
    build_parser.add_argument('--commit', required=True)
    build_parser.add_argument('--testing-commit', required=True)
    build_parser.add_argument('--out', type=Path, required=True)
    run_parser = sub.add_parser('run')
    run_parser.add_argument('--directory', type=Path, required=True)
    run_parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.action == 'supports':
        require(re.fullmatch(r'[0-9a-f]{40}', args.commit), 'exact native source SHA required')
        if args.commit not in registry():
            sys.exit(2)
        contract = contract_for(args.commit)
        if args.case_id and args.case_id not in contract['cases']:
            sys.exit(2)
    elif args.action == 'build':
        build(args.source.resolve(), args.commit, args.testing_commit, args.out.resolve())
    else:
        run(args.directory.absolute(), args.out.resolve())


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f'race native refusal: {error}', file=sys.stderr)
        sys.exit(1)
