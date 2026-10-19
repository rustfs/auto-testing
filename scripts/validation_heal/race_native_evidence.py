"""Admit exact-source native subsets without executing the downloaded binary."""

import argparse
import json
from pathlib import Path
import re

try:
    from . import race_native as suite
except ImportError:
    import race_native as suite


def read(path, limit=1024**2):
    suite.require(path.is_file() and not path.is_symlink() and path.stat().st_size < limit,
                  'native evidence requires a bounded regular original file')
    return json.loads(path.read_text())


def verify(directory, candidate, helper_sha, case_id):
    directory = Path(directory)
    suite.require(directory.is_dir() and not directory.is_symlink(), 'native evidence directory differs')
    suite.require(isinstance(helper_sha, str) and re.fullmatch(r'[0-9a-f]{40}', helper_sha),
                  'native consumer requires an exact runtime helper')
    suite.require(isinstance(candidate, dict) and isinstance(candidate.get('source_sha'), str)
                  and re.fullmatch(r'[0-9a-f]{40}', candidate['source_sha'])
                  and isinstance(candidate.get('package_sha256'), str)
                  and re.fullmatch(r'[0-9a-f]{64}', candidate['package_sha256']),
                  'native consumer has no immutable candidate identity')
    api = read(directory / 'tool-run.json')
    suite.require(type(api.get('id')) is int and api['id'] > 0
                  and type(api.get('run_attempt')) is int and api['run_attempt'] > 0
                  and api.get('status') == 'completed' and api.get('conclusion') == 'success'
                  and api.get('event') == 'workflow_dispatch'
                  and api.get('repository', {}).get('full_name') == 'rustfs/rustfs'
                  and api.get('path') == '.github/workflows/rustfs-functional-chain.yml',
                  'native tools lack a successful exact hosted workflow attempt')
    tools = directory / 'native-tools'
    evidence = directory / 'native-evidence'
    suite.require(evidence.is_dir() and not evidence.is_symlink(), 'native result directory differs')
    manifest, binary = suite.verify(tools)
    suite.require(manifest['candidateCommit'] == candidate['source_sha']
                  and manifest['driverCommit'] == api.get('head_sha')
                  and manifest['testingCommit'] == helper_sha
                  and manifest['runId'] == str(api['id'])
                  and manifest['runAttempt'] == str(api['run_attempt']),
                  'native artifact changed candidate, driver, helper or original attempt')
    contract = suite.contract_for(candidate['source_sha'])
    suite.require(case_id in contract['cases'], 'native subset is not reviewed for the selected case')
    summary = read(evidence / 'race-native-results.json')
    suite.require(type(summary.get('schemaVersion')) is int and summary['schemaVersion'] == 1
                  and summary.get('candidateCommit') == manifest['candidateCommit']
                  and summary.get('candidateSourceTree') == manifest['candidateSourceTree']
                  and summary.get('binarySha256') == manifest['binarySha256']
                  and summary.get('nativeManifestSha256') == suite.sha(tools / 'race-native.json')
                  and summary.get('requiredNativeSubsetsComplete') is True
                  and summary.get('canonicalCasePassClaimed') is False
                  and summary.get('fleetExecutionClaimed') is False,
                  'native results changed source, binary, manifest or proof scope')
    rows = summary.get('results')
    regressions = contract['regressions']
    suite.require(isinstance(rows, list) and len(rows) == len(regressions)
                  and all(isinstance(row, dict) for row in rows)
                  and {row.get('label') for row in rows} == set(regressions),
                  'native results omit or duplicate required exact tests')
    observed = {}
    for row in rows:
        label = row['label']
        spec = regressions[label]
        command = row.get('command')
        suite.require(row.get('test') == spec['test'] and row.get('nativeRegressionPassed') is True
                      and type(row.get('exitCode')) is int and row['exitCode'] == 0
                      and row.get('runtimeEnvironment') == suite.RUNTIME_ENV
                      and isinstance(command, list) and len(command) == 5
                      and isinstance(command[0], str) and Path(command[0]).name == binary.name
                      and command[1:] == [spec['test'], '--exact', '--show-output', '--test-threads=1'],
                      'native result has no exact one-test passing invocation')
        log = evidence / (label + '.log')
        suite.require(log.is_file() and not log.is_symlink() and log.stat().st_size < 16 * 1024**2
                      and suite.sha(log) == row.get('originalLogSha256'), 'original native log bytes differ')
        output = log.read_text()
        suite.exact_result(output, spec['test'], row['exitCode'])
        events = suite.milestones(output, spec)
        suite.require(row.get('milestones') == events, 'native result invented or omitted barrier events')
        observed[label] = {'test': spec['test'], 'witnessKind': spec['witnessKind'], 'milestones': events}
    suite.require(suite.verify(tools)[0] == manifest, 'native artifact changed during read-only admission')
    return {'caseId': case_id, 'candidateCommit': manifest['candidateCommit'],
        'candidatePackageSha256': candidate['package_sha256'], 'candidateSourceTree': manifest['candidateSourceTree'],
        'candidateCargoLockSha256': manifest['candidateCargoLockSha256'], 'binarySha256': manifest['binarySha256'],
        'runId': api['id'], 'runAttempt': api['run_attempt'], 'driverCommit': api['head_sha'],
        'testingCommit': helper_sha, 'selectedContractSha256': manifest['selectedContractSha256'],
        'requiredRegressionLabels': contract['cases'][case_id],
        'observedRegressions': {label: observed[label] for label in contract['cases'][case_id]},
        'requiredNativeSubsetPassed': True, 'wholeCasePassClaimed': False,
        'fleetExecutionClaimed': False, 'canonicalCasePassClaimed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--testing-commit', required=True)
    parser.add_argument('--case-id', choices=sorted(suite.CASE_IDS), required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory, read(args.candidate), args.testing_commit, args.case_id), sort_keys=True))


if __name__ == '__main__':
    main()
