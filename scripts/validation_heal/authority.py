"""C07 authority census and immutable native evidence for actual split fixtures."""

from collections import Counter
import argparse
import json
from pathlib import Path
import re

try:
    from .heal_probe import require
    from .healthy import validate_topology
    from .shards import files
    from .versions import public_version_id
    from . import native_regressions as native
except ImportError:
    from heal_probe import require
    from healthy import validate_topology
    from shards import files
    from versions import public_version_id
    import native_regressions as native


CASE_ID = 'heal.conflicting-subquorum-authority'
BRANCHES = ('disjoint-version-quorums', 'disjoint-incarnations', 'native-authority-fence')
NATIVE_KEYS = ('conflicting-authority-guard', 'reconstructable-authority-preservation')
SOURCE_TREE = '8071fb0a9e5353eb7d3fa58c8a58c8622473f2bd'
CARGO_LOCK = 'd8f7bc416ba67d19873d34e9389e5aa823b22d7d7552706a5a810cee26f6eda2'
HOSTED_BINARY = '/home/runner/work/rustfs/rustfs/native-tools/ecstore-native-tests'


def object_files(snapshot, bucket, key):
    prefix = bucket + '/' + key + '/'
    indexed = files(snapshot)
    return {(node, disk): [{'path': path.removeprefix(prefix), 'size': entry['size'], 'sha256': entry['sha256']}
                          for (owner, drive, path), entry in sorted(indexed.items())
                          if owner == node and drive == disk and path.startswith(prefix)]
            for node, value in snapshot.items() for disk in range(len(value['formats']))}


def census(snapshot, bucket, key, topology):
    layout = validate_topology(snapshot, topology)
    indexed = files(snapshot)
    identities, slots, geometry = Counter(), [], None
    for node, observed in snapshot.items():
        for fmt in observed['formats']:
            disk = fmt['disk']
            row = indexed.get((node, disk, f'{bucket}/{key}/xl.meta'))
            require(row and row['metadata'].get('bucket') == bucket and row['metadata'].get('key') == key,
                    'C07 lost an original metadata member or its exact object identity')
            versions = row['metadata']['versions']
            latest = [version for version in versions if version.get('isLatest') is True]
            require(len(latest) == 1 and latest[0].get('deleted') is False and latest[0].get('inline') is None,
                    'C07 requires one external current candidate on every original member')
            version = latest[0]
            erasure = version['erasure']
            coding = {name: erasure[name] for name in ('data_blocks', 'parity_blocks', 'block_size', 'distribution', 'algorithm')}
            require(geometry is None or geometry == coding, 'C07 generations use different coding/distribution contracts')
            geometry = coding
            members = fmt['value']['xl']['sets'][0]
            slot = members.index(fmt['value']['xl']['this'])
            require(type(erasure['index']) is int and erasure['index'] == erasure['distribution'][slot]
                    and version.get('role') == ('data' if erasure['index'] <= erasure['data_blocks'] else 'parity'),
                    'C07 object role does not bind its original format slot')
            # Count every live version separately. An older version hidden in
            # the other group's metadata must not silently reach read quorum.
            live = [item for item in versions if item.get('deleted') is False]
            for candidate in live:
                identity = {name: candidate.get(name) for name in ('dataDir', 'size', 'parts', 'deleted')}
                identity['versionId'] = public_version_id(candidate.get('versionId'))
                require(isinstance(identity['dataDir'], str) and identity['dataDir'], 'C07 version has no physical data identity')
                digest = json.dumps(identity, sort_keys=True, separators=(',', ':'))
                identities[digest] += 1
            slots.append({'node': node, 'disk': disk, 'diskUUID': fmt['value']['xl']['this'],
                'metadataSha256': row['sha256'], 'versionId': public_version_id(version.get('versionId')),
                'dataDir': version['dataDir'], 'setSlot': slot})
    total = len(slots)
    require(type(geometry['data_blocks']) is int and type(geometry['parity_blocks']) is int
            and geometry['data_blocks'] + geometry['parity_blocks'] == total
            and sorted(geometry['distribution']) == list(range(1, total + 1))
            and len({row['diskUUID'] for row in slots}) == total,
            'C07 census lacks one exact erasure set and all independent original members')
    return {'topology': topology, 'layout': layout, 'coding': geometry, 'N': total,
            'R': geometry['data_blocks'], 'groups': [{'identity': json.loads(identity), 'members': count}
                for identity, count in sorted(identities.items())], 'slots': slots}


def validate_split(snapshot, generation_a, generation_b, bucket, key, topology, branch):
    require(branch in BRANCHES[:2], 'native evidence cannot stand in for a physical split')
    before_a = census(generation_a, bucket, key, topology)
    before_b = census(generation_b, bucket, key, topology)
    mixed = census(snapshot, bucket, key, topology)
    require(before_a['N'] == before_b['N'] == mixed['N'] and before_a['coding'] == before_b['coding'] == mixed['coding']
            and len(before_a['groups']) == len(before_b['groups']) == 1
            and before_a['groups'][0]['members'] == before_b['groups'][0]['members'] == mixed['N'],
            'C07 archived generations are not independently complete single authorities')
    identities = [row['groups'][0]['identity'] for row in (before_a, before_b)]
    require(identities[0] != identities[1] and identities[0]['dataDir'] != identities[1]['dataDir'],
            'C07 generations reuse one logical or physical authority')
    if branch == 'disjoint-version-quorums':
        require(identities[0]['versionId'] != identities[1]['versionId']
                and all(identity['versionId'] != 'null' for identity in identities), 'C07 version groups are not distinct real ACKs')
    else:
        require(all(identity['versionId'] == 'null' for identity in identities),
                'C07 incarnation branch needs distinct unversioned physical generations, not bucket-incarnation claims')
    require(len(mixed['groups']) == 2 and all(row['identity'] in identities and row['members'] == mixed['N'] // 2
                and row['members'] < mixed['R'] for row in mixed['groups']),
            'C07 live identities do not form two independently sub-quorum groups')
    a, b, current = (object_files(value, bucket, key) for value in (generation_a, generation_b, snapshot))
    members = sorted(current)
    require(set(a) == set(b) == set(current) and mixed['N'] == len(members), 'C07 split omitted original members')
    for index, member in enumerate(members):
        require(current[member] == (a if index < len(members) // 2 else b)[member],
                'C07 split does not preserve every exact chosen generation byte')
    return {**mixed, 'everyLiveIdentityBelowR': True, 'crossVersionUnionClaimedAsAuthority': False,
            'bucketIncarnationSplitClaimed': False}


def validate_negative_task(status, bucket, options):
    outcome = status.get('outcome') or {}
    execution = outcome.get('execution') or {}
    require(status.get('summary') == 'stopped' and status.get('truncated', False) is False
            and status.get('settings') == options and outcome.get('objectsTruncated') is False
            and execution.get('state') in {'completed_with_errors', 'aborted'}
            and outcome.get('coverage') in {'complete', 'partial', 'unknown'},
            'C07 Heal lost its explicit terminal refusal/selector/coverage evidence')
    counters = outcome.get('counters') or {}
    names = ('processed', 'healed', 'unchanged', 'skipped', 'failed', 'unknown', 'attemptFailures')
    require(all(type(counters.get(name)) is int and counters[name] >= 0 for name in names)
            and counters.get('overflowed') is False and counters['healed'] == counters['unchanged'] == 0
            and (counters['failed'] + counters['unknown'] > 0 or execution == {'state': 'aborted', 'reason': 'untraversable'}),
            'C07 task claimed repair/health or concealed its unresolved responsibility')
    objects = outcome.get('objects')
    require(isinstance(objects, list), 'C07 task omitted its responsibility diagnostics')
    if execution.get('state') == 'aborted':
        require(execution == {'state': 'aborted', 'reason': 'untraversable'}
                and outcome['coverage'] in {'partial', 'unknown'},
                'C07 abort concealed its untraversable responsibility or claimed full coverage')
    else:
        require(bool(objects), 'C07 failed task has no exact conflict responsibility')
    identities = set()
    for row in objects:
        identity = row.get('identity') or {}
        require(identity.get('bucket') == bucket and identity.get('object') == 'conflict.bin'
                and identity.get('kind') == 'object' and bool(identity.get('bucketIncarnationId'))
                and row.get('disposition', {}).get('state') in {'failed', 'unknown', 'deferred'},
                'C07 task attributed a false repair or refusal to another object')
        require(type(identity.get('poolIndex')) is int and identity['poolIndex'] == options['pool'] == 0
                and type(identity.get('setIndex')) is int and identity['setIndex'] == options['set'] == 0,
                'C07 refusal escaped the exact requested original pool/set')
        key = (identity['bucketIncarnationId'], public_version_id(identity.get('versionId')))
        require(key not in identities, 'C07 refused-task diagnostics repeat one responsibility identity')
        identities.add(key)
    states = [row['disposition']['state'] for row in objects]
    require(counters['processed'] == len(objects) and counters['failed'] == states.count('failed')
            and counters['skipped'] == states.count('unknown') + states.count('deferred')
            and counters['unknown'] == states.count('unknown') and counters['attemptFailures'] >= counters['failed'],
            'C07 refused-task counters conceal missing or mismatched object diagnostics')
    return {'safeExplicitRefusal': True, 'healed': 0, 'verifiedHealthy': 0,
            'runtimeCommitPauseClaimed': False, 'coverage': outcome['coverage']}


def verify_native(directory, candidate):
    directory = Path(directory)
    tools, evidence = directory / 'native-tools', directory / 'native-evidence'
    manifest = json.loads((tools / 'native-regressions.json').read_text())
    build_run = json.loads((directory / 'tool-run.json').read_text())
    require(type(build_run.get('id')) is int and build_run['id'] > 0
            and type(build_run.get('run_attempt')) is int and build_run['run_attempt'] > 0
            and build_run.get('status') == 'completed' and build_run.get('conclusion') == 'success'
            and build_run.get('path') in {'.github/workflows/rustfs-functional-chain.yml', '.github/workflows/rustfs-validation-tools.yml'}
            and manifest.get('runId') == str(build_run['id']) and manifest.get('runAttempt') == str(build_run['run_attempt']),
            'C07 native artifact is not bound to the downloaded successful tools build attempt')
    require(candidate['source_sha'] == native.CANDIDATE and manifest.get('schemaVersion') == 1
            and manifest.get('candidateCommit') == native.CANDIDATE and manifest.get('candidateSourceTree') == SOURCE_TREE
            and manifest.get('candidateCargoLockSha256') == CARGO_LOCK
            and manifest.get('sourceFiles') == {native.SOURCE: native.SOURCE_SHA}
            and manifest.get('sourceUnmodified') is True and manifest.get('optimized') is True
            and manifest.get('collectorSha256') == native.sha(Path(native.__file__))
            and manifest.get('regressions') == native.REGRESSIONS and manifest.get('fleetPassClaimed') is False,
            'C07 native artifact changed its exact source/build/collector contract')
    require(manifest.get('binaryName') == 'ecstore-native-tests' and re.fullmatch(r'[0-9a-f]{64}', manifest.get('binarySha256', '')),
            'C07 native executable path or digest is not registered')
    binary = tools / 'ecstore-native-tests'
    require(binary.is_file() and not binary.is_symlink() and native.sha(binary) == manifest['binarySha256'],
            'C07 native executable differs from its hosted source build')
    summary = json.loads((evidence / 'native-summary.json').read_text())
    require(summary.get('complete') is True and summary.get('fleetPassClaimed') is False
            and summary.get('canonicalCasePassClaimed') is False and summary.get('sourceUnmodified') is True,
            'C07 native regressions did not complete independently')
    records = summary.get('results', [])
    require(len(records) == len(native.REGRESSIONS) and {row['regression'] for row in records} == set(native.REGRESSIONS),
            'C07 native summary omitted, duplicated or added exact tests')
    listing = (tools / 'test-inventory.log').read_text()
    runtime_listing = (evidence / 'runtime-inventory.log').read_text()
    proofs = []
    for key in native.REGRESSIONS:
        spec = native.REGRESSIONS[key]
        row = next(record for record in records if record['regression'] == key)
        require(row == json.loads((evidence / (key + '.json')).read_text()) and row.get('verdict') == 'PASS'
                and row.get('test') == spec['test'] and row.get('candidateCommit') == native.CANDIDATE
                and row.get('binarySha256') == manifest['binarySha256'] and row.get('emittedBarrierLogsClaimed') is False
                and row.get('fleetPassClaimed') is False
                and row.get('command') == [HOSTED_BINARY, spec['test'], '--exact', '--nocapture', '--test-threads=1']
                and row.get('sourceAssertions') == spec['sourceAssertions'] and row.get('fenceKind') == spec['fenceKind'],
                'C07 native result does not bind one exact original test execution')
        native.exact_listing(listing, spec['test'])
        native.exact_listing(runtime_listing, spec['test'])
        proof = native.test_result((evidence / (key + '.log')).read_text(), spec['test'], row.get('exitCode'))
        if key in NATIVE_KEYS:
            proofs.append({'regression': key, 'test': spec['test'], **proof})
    return {'candidateCommit': native.CANDIDATE, 'binarySha256': manifest['binarySha256'], 'results': proofs,
            'toolsBuildRun': build_run['id'], 'toolsBuildAttempt': build_run['run_attempt'],
            'nativeRegressionPassed': True, 'fleetPassClaimed': False, 'runtimeCommitPauseClaimed': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify_native(args.directory, json.loads(args.candidate.read_text())), indent=2))
