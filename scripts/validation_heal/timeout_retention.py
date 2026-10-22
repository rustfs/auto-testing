"""Isolate archived I05 H1/H2 failures with bounded, understood request effects."""

import hashlib
import ipaddress
import json
import re
import uuid

try:
    from .heal_probe import canonical, payload, require
    from .heal_workload import verify_seed
    from .healthy import physical_content, validate_topology
    from .fleet_node import runtime_config
    from .lock_observer import ADMISSION_SECONDS, CANDIDATE, DRAIN_SECONDS, TARGET, same_process
    from .lock_timeout import (CALLER_RECEIVE_BUFFER_BYTES, CONTENDER_BYTES,
                               MIN_BACKPRESSURE_BYTES, timeout_proof, validate_timeout_ack)
    from .race_retention import digest, validate
except ImportError:
    from heal_probe import canonical, payload, require
    from heal_workload import verify_seed
    from healthy import physical_content, validate_topology
    from fleet_node import runtime_config
    from lock_observer import ADMISSION_SECONDS, CANDIDATE, DRAIN_SECONDS, TARGET, same_process
    from lock_timeout import (CALLER_RECEIVE_BUFFER_BYTES, CONTENDER_BYTES,
                              MIN_BACKPRESSURE_BYTES, timeout_proof, validate_timeout_ack)
    from race_retention import digest, validate


CASE = 'heal.lock-timeout-and-caller-abort'
PRE_CONTENDER_HELPER = '704bd606071676dded130dca91d3d5278455bcaa'
CONTENDER_HELPER = 'd100abeb8e85c4000a493aa994031e4a5d01518e'
H2_CONTENDER_HELPER = '1b88cad3581e755d8e904697cb2ec471e04b5f8f'
DRAIN_BUDGET_HELPER = '0442d4dd814ebfb490dcbb3cc266fb6fbebb0365'
DESCRIPTOR_HELPER = 'dd7e54e6672cab76b5a6245ab3b4fbd051ddd39d'
FILTER_STARTUP_HELPER = 'a9725c88acd4977545b278eebe3653c916f907f8'
FILTER_STARTUP_ARCHIVE_SHA256 = 'c6760d0b4e657c306771a466e0a5aa4233f8e05db4aeae018ba037d289fca4aa'
H2_FILTER_STARTUP_HELPER = '31b787c51de23587da1f9a2ac0f910400ddf66b3'
H2_FILTER_STARTUP_ARCHIVE_SHA256 = '8a5399be0a35295703b079ed10cb55685ee4e347e9f9e276b29140afe6764e49'
DRAIN_HELPERS = {DRAIN_BUDGET_HELPER, DESCRIPTOR_HELPER}
FAILURES = {
    PRE_CONTENDER_HELPER: {'type': 'ValueError',
        'message': 'caller lock diagnostic escaped its exact GET identity or runtime budget'},
    CONTENDER_HELPER: {'type': 'ValueError',
        'message': 'I05 contender acquired a lock or lost its exact timeout lifecycle'},
    H2_CONTENDER_HELPER: {'type': 'ValueError',
        'message': 'I05 contender acquired a lock or lost its exact timeout lifecycle'},
    DRAIN_BUDGET_HELPER: {'type': 'ValueError',
        'message': 'NOT_OBSERVED: caller drain observation exceeded its budget'},
    DESCRIPTOR_HELPER: {'type': 'ValueError',
        'message': 'NOT_OBSERVED: caller drain observation exceeded its budget'},
}
COMMON_LABELS = ('binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation', 'reserved',
          'inspector-upload', 'started', 'deployed-topology', 'actual-topology', 'original-ACK-bound-S3',
          'original-ACK-proof', 'I05-timeout-original-physical', 'I05-timeout-original-S3',
          'I05-timeout-baseline-physical-after-public-read', 'I05-caller-baseline-resources',
          'I05-caller-checkpoint')
LABELS = {
    PRE_CONTENDER_HELPER: COMMON_LABELS + ('I05-caller-window', 'quiescent',
        'original-failure-scene', 'original-failure-logs', 'diagnostic-result'),
    CONTENDER_HELPER: COMMON_LABELS + ('I05-caller-window', 'I05-caller-before-disconnect-socket',
        'I05-caller-window', 'I05-caller-admission-resources', 'I05-caller-window', 'quiescent',
        'original-failure-scene', 'original-failure-logs', 'diagnostic-result'),
    H2_CONTENDER_HELPER: COMMON_LABELS + ('I05-caller-window', 'I05-caller-before-disconnect-socket',
        'I05-caller-window', 'I05-caller-admission-resources', 'I05-caller-window', 'quiescent',
        'original-failure-scene', 'original-failure-logs', 'diagnostic-result'),
}
DRAIN_PREFIX_LABELS = COMMON_LABELS + ('I05-caller-window', 'I05-caller-before-disconnect-socket',
    'I05-caller-window', 'I05-caller-admission-resources', 'I05-caller-window',
    'I05-timeout-caller-backpressure-socket')
DRAIN_SUFFIX_LABELS = ('quiescent', 'original-failure-scene', 'original-failure-logs', 'diagnostic-result')


def h2_labels(window_rounds=(1, 1, 1)):
    """Exact journal shape when H1 cleans successfully and H2 fails in its contender window."""
    return ('binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation',
            *('reserved' for _ in range(4)), *('inspector-upload' for _ in range(4)),
            'started', 'deployed-topology', 'actual-topology', 'original-ACK-bound-S3',
            'original-ACK-proof', 'I05-timeout-original-physical', 'I05-timeout-original-S3',
            'I05-timeout-baseline-physical-after-public-read',
            *('I05-caller-baseline-resources' for _ in range(4)),
            *('I05-caller-checkpoint' for _ in range(4)),
            *('I05-caller-window' for _ in range(4 * window_rounds[0])), 'I05-caller-before-disconnect-socket',
            *('I05-caller-window' for _ in range(4 * window_rounds[1])),
            *('I05-caller-admission-resources' for _ in range(4)),
            *('I05-caller-window' for _ in range(4 * window_rounds[2])), 'quiescent', 'original-failure-scene',
            *('original-failure-logs' for _ in range(4)), 'diagnostic-result')


def drain_failure_labels(folder):
    """Accept only complete repeated H1 drain samples from the pinned failed helper."""
    names = sorted(path.name for path in folder.glob('*.json'))
    labels = [name[6:-5] for name in names]
    prefix, suffix = list(DRAIN_PREFIX_LABELS), list(DRAIN_SUFFIX_LABELS)
    require(labels[:len(prefix)] == prefix and labels[-len(suffix):] == suffix,
            'I05 drain failure has a different caller or retained-scene boundary')
    samples = labels[len(prefix):-len(suffix)]
    first_drain = samples.index('I05-caller-drain-socket') if 'I05-caller-drain-socket' in samples else -1
    caller_windows = samples[:first_drain]
    rounds = samples[first_drain:]
    complete_rounds, terminal = divmod(len(rounds), 3)
    expected_rounds = ['I05-caller-drain-socket', 'I05-caller-drain-resources', 'I05-caller-window'] * complete_rounds
    require(first_drain > 0 and all(label == 'I05-caller-window' for label in caller_windows)
            and complete_rounds >= 1 and rounds[:len(expected_rounds)] == expected_rounds
            and (terminal == 0 or rounds[-terminal:] == ['I05-caller-drain-socket', 'I05-caller-drain-resources']),
            'I05 drain failure omits or reorders a complete socket/resource/journal sample')
    return prefix + caller_windows + rounds + suffix


def validate_census(value):
    """Keep the reviewed helper's complete snapshots without inferring no leak."""
    require(isinstance(value, dict)
            and value.get('descriptorContentsRead') is False
            and value.get('rawDescriptorPathsIncluded') is False
            and value.get('snapshotOnly') is True and value.get('noLeakProven') is False
            and type(value.get('startedMonotonicNs')) is int
            and type(value.get('finishedMonotonicNs')) is int
            and 0 < value['startedMonotonicNs'] <= value['finishedMonotonicNs']
            and type(value.get('fileDescriptors')) is int
            and 0 <= value['fileDescriptors'] <= 65536
            and value.get('vanishedDescriptors') == 0
            and isinstance(value.get('descriptors'), list)
            and len(value['descriptors']) == value['fileDescriptors']
            and isinstance(value.get('ownedTcpConnections'), list),
            'I05 descriptor census is incomplete or claims more than a snapshot')
    descriptors, sockets = {}, {}
    for row in value['descriptors']:
        fd, kind = row.get('descriptor'), row.get('kind')
        require(type(fd) is int and fd >= 0 and fd not in descriptors
                and kind in {'socket', 'pipe', 'path', 'anonymous'}
                and re.fullmatch(r'[0-9a-f]{64}', row.get('targetSha256', ''))
                and set(row) == {'descriptor', 'kind', 'targetSha256'} | (
                    {'inode'} if kind in {'socket', 'pipe'} else set()),
                'I05 descriptor identity is missing, duplicated or contains an unreviewed field')
        descriptors[fd] = row
        if kind in {'socket', 'pipe'}:
            inode = row.get('inode')
            require(type(inode) is int and inode > 0
                    and row['targetSha256'] == hashlib.sha256(f'{kind}:[{inode}]'.encode()).hexdigest(),
                    'I05 descriptor inode does not match its link fingerprint')
            if kind == 'socket':
                sockets.setdefault(inode, []).append(fd)
    seen = set()
    for row in value['ownedTcpConnections']:
        inode = row.get('inode')
        require(type(inode) is int and inode in sockets and inode not in seen
                and row.get('descriptors') == sockets[inode]
                and row.get('table') in {'tcp', 'tcp6'}
                and re.fullmatch(r'[0-9A-F]{2}', row.get('state', ''))
                and all(type(row.get(name)) is int and row[name] >= 0
                        for name in ('txQueueBytes', 'rxQueueBytes')),
                'I05 TCP census is not joined to the original owned descriptors')
        seen.add(inode)
        for name in ('localAddress', 'remoteAddress'):
            endpoint = row.get(name)
            require(isinstance(endpoint, list) and len(endpoint) == 2
                    and isinstance(endpoint[0], str) and type(endpoint[1]) is int
                    and 0 <= endpoint[1] <= 65535,
                    'I05 TCP census has an invalid numeric endpoint')
            ipaddress.ip_address(endpoint[0])


def filter_startup_plan(result_path, failed_run, candidate):
    """Preserve the exact pre-workload filter-check failure, without deleting it."""
    require(failed_run == 39761856012, 'I05 filter startup isolation requires its reviewed failed run')
    archive = result_path.parents[3]
    files = {}
    for path in archive.rglob('*'):
        require(not path.is_symlink() and (path.is_dir() or path.is_file()),
                'I05 filter startup archive contains a link or special file')
        if path.is_file():
            require(path.stat().st_size <= 2 * 1024**2 and len(files) < 22,
                    'I05 filter startup archive exceeds its reviewed bound')
            files[path.relative_to(archive).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    require(len(files) == 22 and digest(files) == FILTER_STARTUP_ARCHIVE_SHA256,
            'I05 filter startup archive differs from its byte-exact original')
    def read(path): return json.loads(path.read_text())
    result = read(result_path)
    provenance = result['provenance']
    evidence = result_path.parents[1] / 'evidence' / CASE
    folder = evidence / 'H1'
    labels = ('binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation',
              'reserved', 'inspector-upload', 'quiescent', 'original-failure-scene',
              'original-failure-logs', 'diagnostic-result')
    require({path.name for path in folder.iterdir()} == {
                f'{index:05d}-{label}.json' for index, label in enumerate(labels, 1)}
            and not (evidence / 'H2').exists(),
            'I05 filter startup isolation cannot include a workload or another topology')
    records = {label: read(folder / f'{index:05d}-{label}.json')
               for index, label in enumerate(labels, 1)}
    binding, reservation, scene = (records[label] for label in ('binding', 'reserved', 'original-failure-scene'))
    run_id = 'gh-39761856012-1-h1'
    owner = reservation['owner']
    original = scene['vm000']
    diagnostic = records['diagnostic-result']
    require(result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and result.get('case_id') == CASE
            and candidate == read(archive / 'candidate.json')
            and provenance['source_commit'] == binding['candidateCommit'] == CANDIDATE
            and provenance['candidate_sha256'] == binding['candidatePackageSha256'] == candidate['package_sha256']
            and provenance['testing_commit'] == binding['harnessCommit'] == FILTER_STARTUP_HELPER
            and provenance['binary_sha256'] == binding['candidateBinarySha256'] == owner['binarySha256']
            and binding['runId'] == run_id and reservation['node'] == 'vm000' and reservation['reserved'] is True
            and original.get('quiescent') is True and original.get('pid') == 0 and original.get('files') == []
            and original.get('unit') == owner['unit'] and original.get('hostname') == owner['hostname']
            and diagnostic.get('diagnosticComplete') is False and diagnostic.get('cleanup') == 'FAIL'
            and 'I05 kernel service BPF attachments are missing, foreign or incomplete' in
                diagnostic.get('failure', {}).get('message', '')
            and diagnostic.get('namespacesRetained') is True
            and diagnostic.get('canonicalCasePassClaimed') is False,
            'I05 filter startup isolation lost its original candidate, owner or stopped empty scene')
    validate_topology(scene, 'H1')
    plan = {'kind': 'unresolved-i05-timeout', 'runId': run_id, 'nodes': ['vm000'],
            'bucket': binding['bucketPrefix'], 'owners': {'vm000': owner},
            'candidateCommit': CANDIDATE, 'harnessCommit': FILTER_STARTUP_HELPER,
            'binarySha256': binding['candidateBinarySha256'], 'ackFilesSha256': digest(files),
            'businessSceneSha256': {'vm000': digest(physical_content(scene)['vm000'])}}
    validate(plan)
    return [plan]


def h2_filter_startup_plan(result_path, failed_run, candidate):
    """Preserve only the reviewed four-peer startup failure after clean H1."""
    require(failed_run == 39764496504, 'I05 H2 startup isolation requires its reviewed failed run')
    archive = result_path.parents[3]
    files = {}
    for path in archive.rglob('*'):
        require(not path.is_symlink() and (path.is_dir() or path.is_file()),
                'I05 H2 startup archive contains a link or special file')
        if path.is_file():
            require(path.stat().st_size <= 2 * 1024**2 and len(files) < 102,
                    'I05 H2 startup archive exceeds its reviewed bound')
            files[path.relative_to(archive).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    require(len(files) == 102 and digest(files) == H2_FILTER_STARTUP_ARCHIVE_SHA256,
            'I05 H2 startup archive differs from its byte-exact original')
    def read(path): return json.loads(path.read_text())
    result = read(result_path)
    provenance = result['provenance']
    evidence = result_path.parents[1] / 'evidence' / CASE
    summary = read(evidence / 'healthy-summary.json')
    h1 = {'topology': 'H1', 'diagnosticComplete': True, 'cleanup': 'PASS',
          'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False}
    require(summary.get('caseId') == CASE and summary.get('cleanup') == 'FAIL'
            and summary.get('requestedTopologies') == ['H1', 'H2']
            and summary.get('complete') is False and summary.get('diagnosticComplete') is False
            and summary.get('canonicalCasePassClaimed') is False
            and len(summary.get('diagnostics', [])) == 2 and summary['diagnostics'][0] == h1
            and read(evidence / 'H1/00042-diagnostic-result.json') == h1
            and read(evidence / 'H1/00041-recovery.json') == {'cleanup': 'PASS', 'errors': []}
            and read(evidence / 'H1/00040-cleanup.json') == {
                'node': 'vm000', 'cleanup': 'PASS', 'unitStopped': True, 'rootsRemoved': True,
                'hostname': 'rustfs000000'},
            'I05 H2 startup isolation requires completed H1 diagnostics and actual root cleanup')
    folder = evidence / 'H2'
    labels = ('binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation',
              *('reserved', 'inspector-upload') * 4, 'quiescent', 'original-failure-scene',
              *('original-failure-logs' for _ in range(4)), 'diagnostic-result')
    require({path.name for path in folder.iterdir()} == {
                f'{index:05d}-{label}.json' for index, label in enumerate(labels, 1)},
            'I05 H2 startup isolation cannot include workload or unreviewed actions')
    records = [(label, read(folder / f'{index:05d}-{label}.json'))
               for index, label in enumerate(labels, 1)]
    def many(label): return [value for name, value in records if name == label]
    def one(label):
        values = many(label)
        require(len(values) == 1, 'I05 H2 startup lacks one exact ' + label)
        return values[0]
    binding, scene, diagnostic = (one(label) for label in ('binding', 'original-failure-scene', 'diagnostic-result'))
    run_id = 'gh-39764496504-1-h2'
    nodes = ['vm000', 'vm001', 'vm002', 'vm003']
    owners = {row['node']: row['owner'] for row in many('reserved')}
    require(result.get('case_id') == CASE and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and candidate == read(archive / 'candidate.json')
            and provenance['source_commit'] == binding['candidateCommit'] == candidate['source_sha'] == CANDIDATE
            and provenance['candidate_sha256'] == binding['candidatePackageSha256'] == candidate['package_sha256']
            and provenance['testing_commit'] == binding['harnessCommit'] == H2_FILTER_STARTUP_HELPER
            and provenance['binary_sha256'] == binding['candidateBinarySha256']
            and binding['runId'] == run_id
            and binding['bucketPrefix'] == 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
            and binding['endpoints'] == {f'node{i}': f'http://rustfs-node{i}:9000' for i in range(1, 5)}
            and summary['diagnostics'][1] == diagnostic
            and diagnostic.get('topology') == 'H2' and diagnostic.get('diagnosticComplete') is False
            and diagnostic.get('cleanup') == 'FAIL' and diagnostic.get('namespacesRetained') is True
            and diagnostic.get('wholeCasePassClaimed') is False and diagnostic.get('canonicalCasePassClaimed') is False
            and diagnostic.get('failure', {}).get('type') == 'ValueError'
            and 'bpftool not found for kernel 6.17.0-1022' in diagnostic.get('failure', {}).get('message', '')
            and set(owners) == set(scene) == set(one('quiescent')) == set(nodes)
            and all(row.get('reserved') is True for row in many('reserved'))
            and one('integrity-fleet-attestation').get('nodes') == nodes
            and one('integrity-fleet-attestation').get('allParticipatingNodesUseCandidate') is True
            and one('inspector-build').get('built') is True
            and one('inspector-build').get('candidateCommit') == CANDIDATE,
            'I05 H2 startup isolation lost the original failure, candidate or all four peers')
    uploads = {row['node']: row for row in many('inspector-upload')}
    logs = {row['node']: row for row in many('original-failure-logs')}
    require(set(uploads) == set(logs) == set(one('preflight')) == set(nodes),
            'I05 H2 startup omits an original upload, preflight or service log')
    for node in nodes:
        owner, original = owners[node], scene[node]
        require(original.get('quiescent') is True and original.get('pid') == 0 and original.get('files') == []
                and original.get('unit') == logs[node].get('unit') == owner['unit']
                and original.get('hostname') == one('preflight')[node].get('hostname') == owner['hostname']
                and one('preflight')[node].get('mounts') == owner['mounts']
                and original.get('binarySha256') == owner['binarySha256'] == binding['candidateBinarySha256']
                and uploads[node].get('installed') is True
                and uploads[node].get('inspectorSha256') == owner.get('inspectorSha256')
                    == one('inspector-build').get('binarySha256')
                and one('quiescent')[node].get('quiescent') is True
                and one('quiescent')[node].get('service', {}).get('MainPID') == '0',
                'I05 H2 startup scene is not the stopped, empty original on ' + node)
    validate_topology(scene, 'H2')
    plan = {'kind': 'unresolved-i05-timeout', 'runId': run_id, 'nodes': nodes,
            'bucket': binding['bucketPrefix'], 'owners': owners, 'candidateCommit': CANDIDATE,
            'harnessCommit': H2_FILTER_STARTUP_HELPER, 'binarySha256': binding['candidateBinarySha256'],
            'ackFilesSha256': digest(files), 'businessSceneSha256': {
                node: digest(value) for node, value in physical_content(scene).items()}}
    validate(plan)
    return [plan]


def successor_files(folder):
    """Fingerprint the complete stopped diagnostic, including unknown receipts."""
    require(folder.is_dir() and not folder.is_symlink(), 'I05 successor evidence root differs')
    files, total = {}, 0
    for path in sorted(folder.rglob('*')):
        require(not path.is_symlink() and (path.is_dir() or path.is_file()),
                'I05 successor evidence contains a link or special file')
        if not path.is_file() or path == folder / 'successor-retention.json':
            continue
        size = path.stat().st_size
        total += size
        require(size <= 32 * 1024**2 and total <= 256 * 1024**2 and len(files) < 4096,
                'I05 successor evidence exceeds its preservation budget')
        files[path.relative_to(folder).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    require(files, 'I05 successor has no original evidence')
    return files


def successor_plan(folder, binding, scene):
    """Preservation needs exact stopped bytes, never a successful PUT outcome."""
    topology = 'H1' if binding['runId'].endswith('-h1') else 'H2'
    nodes = ['vm000'] if topology == 'H1' else ['vm000', 'vm001', 'vm002', 'vm003']
    reservations = [json.loads(path.read_text()) for path in folder.glob('*-reserved.json')]
    owners = {row.get('node'): row.get('owner') for row in reservations}
    require(len(reservations) == len(nodes) and set(owners) == set(scene) == set(nodes)
            and all(row.get('reserved') is True for row in reservations)
            and binding.get('candidateCommit') == CANDIDATE
            and binding.get('endpoints') == {f'node{i}': f'http://rustfs-node{i}:9000'
                                            for i in range(1, len(nodes) + 1)}
            and binding.get('bucketPrefix') == 'rv-heal-' + hashlib.sha256(binding['runId'].encode()).hexdigest()[:16],
            'I05 successor preservation requires the complete original fleet binding')
    for node, owner in owners.items():
        original = scene[node]
        require(owner.get('runtimeProfile') == 'lock-successor'
                and original.get('quiescent') is True and type(original.get('pid')) is int and original['pid'] == 0
                and original.get('hostname') == owner.get('hostname')
                and original.get('unit') == owner.get('unit')
                and original.get('binarySha256') == owner.get('binarySha256') == binding['candidateBinarySha256']
                and len(original.get('formats', [])) == 4
                and {part.get('disk') for part in original['formats']} == set(range(4)),
                'I05 successor requires the stopped original owner and all four disks')
    plan = {'kind': 'unresolved-i05-successor', 'runId': binding['runId'], 'nodes': nodes,
            'bucket': binding['bucketPrefix'], 'owners': owners, 'candidateCommit': CANDIDATE,
            'harnessCommit': binding['harnessCommit'], 'binarySha256': binding['candidateBinarySha256'],
            'ackFilesSha256': digest(successor_files(folder)),
            'businessSceneSha256': {node: digest(value) for node, value in physical_content(scene).items()}}
    validate(plan)
    return plan


def seal_successor_scene(folder, binding, scene):
    files = successor_files(folder)
    plan = successor_plan(folder, binding, scene)
    require(digest(files) == plan['ackFilesSha256'], 'I05 successor evidence changed while sealing')
    return {'schemaVersion': 1, 'plan': plan, 'originalFilesSha256': files,
            'candidatePackageSha256': binding['candidatePackageSha256'],
            'canonicalCasePassClaimed': False, 'unknownMutationMayBePresent': True,
            'namespaceDeletionPermitted': False, 'backupRestorationPermitted': False}


def successor_archived_plan(result_path, failed_run, candidate):
    result = json.loads(result_path.read_text())
    provenance = result.get('provenance') or {}
    evidence = result_path.parents[1] / 'evidence' / CASE
    summary = json.loads((evidence / 'healthy-summary.json').read_text())
    rows = summary.get('diagnostics', [])
    require(result.get('case_id') == CASE and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and summary.get('caseId') == CASE and summary.get('diagnosticBranch') == 'queued-successor'
            and summary.get('cleanup') == 'FAIL' and summary.get('complete') is False
            and summary.get('diagnosticComplete') is False
            and summary.get('canonicalCasePassClaimed') is False and summary.get('wholeCasePassClaimed') is False
            and isinstance(rows, list) and 1 <= len(rows) <= 2
            and [row.get('topology') for row in rows] in (['H1'], ['H2'], ['H1', 'H2'])
            and [row['topology'] for row in rows] == summary.get('requestedTopologies', [])[:len(rows)]
            and all(row.get('diagnosticComplete') is True and row.get('cleanup') == 'PASS' for row in rows[:-1])
            and rows[-1].get('diagnosticComplete') is False and rows[-1].get('cleanup') == 'FAIL'
            and rows[-1].get('namespacesRetained') is True
            and 'failureCaptureError' not in rows[-1] and 'retentionCaptureError' not in rows[-1]
            and all(row.get('branch') == 'queued-successor' and row.get('canonicalCasePassClaimed') is False
                    and row.get('wholeCasePassClaimed') is False for row in rows),
            'I05 successor recovery requires one retained diagnostic failure after clean predecessors')
    require({p.name for p in evidence.iterdir() if p.is_dir()} == {row['topology'] for row in rows},
            'I05 successor recovery cannot omit another topology')
    for row in rows:
        found = list((evidence / row['topology']).glob('*-diagnostic-result.json'))
        require(len(found) == 1 and json.loads(found[0].read_text()) == row,
                'I05 successor summary differs from its original diagnostic result')
    folder = evidence / rows[-1]['topology']
    def one(label):
        paths = list(folder.glob('*-' + label + '.json'))
        require(len(paths) == 1, 'I05 successor lacks one original ' + label)
        return json.loads(paths[0].read_text())
    manifest_path = folder / 'successor-retention.json'
    require(manifest_path.is_file() and not manifest_path.is_symlink()
            and manifest_path.stat().st_size <= 2 * 1024**2, 'I05 successor retention seal is missing or unsafe')
    seal = json.loads(manifest_path.read_text())
    require(seal.get('schemaVersion') == 1 and seal.get('canonicalCasePassClaimed') is False
            and seal.get('unknownMutationMayBePresent') is True
            and seal.get('namespaceDeletionPermitted') is False and seal.get('backupRestorationPermitted') is False
            and seal.get('originalFilesSha256') == successor_files(folder),
            'I05 successor original evidence differs from its retention seal')
    binding, scene = one('binding'), one('original-failure-scene')
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-h[12]', binding.get('runId', ''))
            and candidate.get('source_sha') == binding.get('candidateCommit') == provenance.get('source_commit') == CANDIDATE
            and candidate.get('package_sha256') == binding.get('candidatePackageSha256')
                == provenance.get('candidate_sha256') == seal.get('candidatePackageSha256')
            and binding.get('harnessCommit') == provenance.get('testing_commit')
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256'),
            'I05 successor archive changed run, candidate, helper or binary')
    plan = successor_plan(folder, binding, scene)
    require(seal.get('plan') == plan, 'I05 successor preservation plan differs from original evidence')
    return [plan]


def archived_plan(artifact, failed_run, candidate):
    """Refuse uncertain effects, later actions, mixed cases and physical drift.

    This is preservation, with no S3 call, backup restoration or verdict change.
    A contender is accepted only for the reviewed helper's exact refused PUT.
    """
    require(type(failed_run) is int and failed_run > 0, 'I05 requires an exact failed run')
    paths = list(artifact.glob('*/run/*/cases/*.json'))
    require(len(paths) == 1 and paths[0].stem == CASE, 'I05 cannot omit another failed case')
    def read(path):
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 2 * 1024**2,
                'I05 archive member is missing, unsafe or oversized')
        return json.loads(path.read_text())
    result = read(paths[0])
    provenance = result.get('provenance') or {}
    summary_path = paths[0].parents[1] / 'evidence' / CASE / 'healthy-summary.json'
    if read(summary_path).get('diagnosticBranch') == 'queued-successor':
        return successor_archived_plan(paths[0], failed_run, candidate)
    if provenance.get('testing_commit') == FILTER_STARTUP_HELPER:
        return filter_startup_plan(paths[0], failed_run, candidate)
    if provenance.get('testing_commit') == H2_FILTER_STARTUP_HELPER:
        return h2_filter_startup_plan(paths[0], failed_run, candidate)
    require(result.get('case_id') == CASE and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and provenance.get('source_commit') == candidate.get('source_sha') == CANDIDATE
            and provenance.get('candidate_sha256') == candidate.get('package_sha256')
            and provenance.get('testing_commit') in FAILURES,
            'I05 requires its original failed candidate and reviewed helper boundary')
    helper = provenance['testing_commit']
    failure = FAILURES[helper]
    evidence = paths[0].parents[1] / 'evidence' / CASE
    summary = read(evidence / 'healthy-summary.json')
    row = {'topology': 'H1', 'diagnosticComplete': False, 'cleanup': 'FAIL',
           'wholeCasePassClaimed': False, 'canonicalCasePassClaimed': False,
           'failure': failure, 'namespacesRetained': True}
    diagnostics = summary.get('diagnostics') or []
    if len(diagnostics) == 2 and diagnostics[0].get('topology') == 'H1':
        topology = 'H2'
        row = {**row, 'topology': 'H2'}
        h1 = diagnostics[0]
        h1_dir = evidence / 'H1'
        h1_results = list(h1_dir.glob('*-diagnostic-result.json'))
        require(helper in (CONTENDER_HELPER, H2_CONTENDER_HELPER)
                and h1.get('diagnosticComplete') is True and h1.get('cleanup') == 'PASS'
                and h1.get('wholeCasePassClaimed') is False and h1.get('canonicalCasePassClaimed') is False
                and len(h1_results) == 1,
                'I05 H2 isolation requires a completed and cleaned H1 diagnostic')
        h1_result = read(h1_results[0])
        require(h1_result.get('topology') == 'H1' and h1_result.get('diagnosticComplete') is True
                and h1_result.get('cleanup') == 'PASS'
                and h1_result.get('wholeCasePassClaimed') is False
                and h1_result.get('canonicalCasePassClaimed') is False,
                'I05 H1 diagnostic result does not prove completed cleanup')
        require(diagnostics[1] == row, 'I05 H2 failure differs from its exact known boundary')
    elif helper in DRAIN_HELPERS:
        topology = 'H1'
        require(diagnostics == [row], 'I05 drain failure differs from its exact H1 boundary')
    else:
        topology = 'H1'
        require(diagnostics == [row], 'I05 H1 failure differs from its exact known boundary')
    require(summary.get('caseId') == CASE and summary.get('cleanup') == 'FAIL'
            and summary.get('diagnostics') == diagnostics and summary.get('complete') is False
            and summary.get('diagnosticComplete') is False
            and summary.get('requestedTopologies') == ['H1', 'H2']
            and summary.get('canonicalCasePassClaimed') is False
            and (topology == 'H2' or not (evidence / 'H2').exists()),
            'I05 requires one retained failure and no canonical acceptance claim')
    folder = evidence / topology
    if topology == 'H2':
        archived_names = sorted(path.name for path in folder.iterdir())
        actual_labels = [re.sub(r'^\d{5}-', '', name)[:-5] for name in archived_names if name.endswith('.json')]
        checkpoint_end = actual_labels.index('I05-caller-checkpoint') + 4
        first_socket = actual_labels.index('I05-caller-before-disconnect-socket')
        first_admission = actual_labels.index('I05-caller-admission-resources')
        last_admission = first_admission + 4
        first_quiescent = actual_labels.index('quiescent')
        window_rounds = ((first_socket - checkpoint_end) // 4,
                         (first_admission - first_socket - 1) // 4,
                         (first_quiescent - last_admission) // 4)
        require(all(rounds >= 1 for rounds in window_rounds)
                and first_socket - checkpoint_end == 4 * window_rounds[0]
                and first_admission - first_socket - 1 == 4 * window_rounds[1]
                and first_quiescent - last_admission == 4 * window_rounds[2],
                'I05 H2 lock windows do not contain complete four-peer sampling rounds')
        labels = h2_labels(window_rounds)
    elif helper in DRAIN_HELPERS:
        labels = drain_failure_labels(folder)
    else:
        labels = LABELS[helper]
    numbered = [(label, f'{index:05d}-{label}.json') for index, label in enumerate(labels, 1)]
    require({path.name for path in folder.iterdir()} == {name for _, name in numbered} | {'fixture', 'timeout'},
            'I05 archive contains another action or lacks its original boundary')
    def one(label):
        matches = [name for current, name in numbered if current == label]
        require(len(matches) == 1, 'I05 archive lacks one exact ' + label)
        return read(folder / matches[0])
    def many(label):
        return [read(folder / name) for current, name in numbered if current == label]
    binding, scene = one('binding'), one('original-failure-scene')
    run_id, bucket = binding.get('runId', ''), binding.get('bucketPrefix', '')
    suffix = topology.lower()
    nodes = ['vm000'] if topology == 'H1' else ['vm000', 'vm001', 'vm002', 'vm003']
    endpoints = {f'node{index}': f'http://rustfs-node{index}:9000'
                 for index in range(1, len(nodes) + 1)}
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-{suffix}', run_id)
            and bucket == 'rv-heal-' + hashlib.sha256(run_id.encode()).hexdigest()[:16]
            and binding.get('candidateCommit') == CANDIDATE
            and binding.get('candidatePackageSha256') == candidate['package_sha256']
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and binding.get('harnessCommit') == helper
            and binding.get('endpoints') == endpoints
            and binding.get('defaultEndpoint') == 'node1' and one('diagnostic-result') == row,
            'I05 original run, package, helper, binary or failure boundary differs')
    reservations = many('reserved')
    owners = {item.get('node'): item.get('owner') for item in reservations}
    require(len(reservations) == len(nodes) and set(owners) == set(nodes)
            and all(item.get('reserved') is True for item in reservations)
            and all(owner.get('runtimeProfile') == 'lock-timeout' for owner in owners.values())
            and set(scene) == set(nodes),
            'I05 requires every original reserved node and retained scene')
    owner = owners['vm000']
    windows = many('I05-caller-window')
    sample_counts = {node: sum(window.get('node') == node for window in windows) for node in nodes}
    baselines = {item.get('node'): item for item in many('I05-caller-baseline-resources')}
    if topology == 'H2':
        attestation = one('integrity-fleet-attestation')
        actual = one('actual-topology')
        uploads = {item.get('node'): item for item in many('inspector-upload')}
        require(attestation.get('nodes') == nodes and attestation.get('allParticipatingNodesUseCandidate') is True
                and actual.get('topology') == 'H2' and actual.get('nodes') == 4
                and actual.get('independentDrives') == 16
                and set(uploads) == set(nodes)
                and all(uploads[node].get('installed') is True
                        and uploads[node].get('inspectorSha256') == owners[node].get('inspectorSha256')
                        for node in nodes)
                and set(many('preflight')[0]) == set(nodes)
                and all(many('preflight')[0][node].get('hostname') == owners[node].get('hostname')
                        and many('preflight')[0][node].get('mounts') == owners[node].get('mounts')
                        for node in nodes)
                and set(many('deployed-topology')[0]) == set(nodes)
                and set(many('started')[0]) == set(nodes)
                and {item.get('node') for item in many('original-failure-logs')} == set(nodes)
                and {item.get('node') for item in baselines.values()} == set(nodes)
                and {item.get('node') for item in many('I05-caller-checkpoint')} == set(nodes)
                and {item.get('node') for item in many('I05-caller-admission-resources')} == set(nodes)
                and len(set(sample_counts.values())) == 1 and min(sample_counts.values()) >= 3,
                'I05 H2 evidence omits a peer, inspector upload, lock checkpoint or service log')
        admission = {item.get('node'): item for item in many('I05-caller-admission-resources')}
        logs = {item.get('node'): item for item in many('original-failure-logs')}
        require(set(baselines) == set(admission) == set(logs) == set(nodes)
                and all(type(baselines[node].get('fileDescriptors')) is int
                        and type(admission[node].get('fileDescriptors')) is int
                        and baselines[node].get('identity', {}).get('binarySha256') == binding['candidateBinarySha256']
                        and admission[node].get('identity', {}).get('binarySha256') == binding['candidateBinarySha256']
                        and baselines[node].get('identity', {}).get('settings') == runtime_config('lock-timeout')
                        and admission[node].get('identity', {}).get('settings') == runtime_config('lock-timeout')
                        and logs[node].get('unit') == owners[node].get('unit')
                        and isinstance(logs[node].get('journal'), str) and bool(logs[node]['journal'])
                        for node in nodes),
                'I05 H2 resource identity, runtime settings or original service logs differ')
        for node in nodes:
            same_process(admission[node].get('identity'), baselines[node].get('identity'))
    inspector = one('inspector-build')
    require(inspector.get('candidateCommit') == CANDIDATE
            and all(inspector.get('binarySha256') == value.get('inspectorSha256') for value in owners.values())
            and inspector.get('built') is True, 'I05 original inspector identity differs')
    for node in nodes:
        original = scene[node]
        node_owner = owners[node]
        require(original.get('quiescent') is True and type(original.get('pid')) is int and original['pid'] == 0
                and original.get('hostname') == node_owner.get('hostname')
                and original.get('unit') == node_owner.get('unit')
                and original.get('binarySha256') == node_owner.get('binarySha256') == binding['candidateBinarySha256']
                and len(original.get('formats', [])) == 4
                and {part.get('disk') for part in original['formats']} == set(range(4)),
                'I05 stopped four-drive original physical scene changed on ' + node)
    require(physical_content(scene) == physical_content(one('I05-timeout-original-physical'))
            == physical_content(one('I05-timeout-baseline-physical-after-public-read')),
            'I05 stopped four-drive original physical scene changed')

    # Replay only the bounded seed ledger locally. No result is inferred from
    # current LISTs, and a missing, unknown or failed mutation refuses recovery.
    fixture = folder / 'fixture'
    names = {f'{i:06d}-{kind}.json' for i in range(1, 10) for kind in ('request', 'ack')}
    require({path.name for path in fixture.iterdir()} == names, 'I05 requires all nine seed request/ACK pairs')
    expected, journal, public_properties, seen, domain = [], {}, {}, set(), None
    for sequence in range(1, 10):
        request_path = fixture / f'{sequence:06d}-request.json'
        ack_path = fixture / f'{sequence:06d}-ack.json'
        request, ack = read(request_path), read(ack_path)
        operation = 'create_bucket' if sequence == 1 else 'put_bucket_versioning' if sequence == 2 else (
            'delete_object' if sequence == 6 else 'put_object')
        params = {'Bucket': bucket}
        if sequence == 2:
            params['VersioningConfiguration'] = {'Status': 'Enabled'}
        elif sequence >= 3:
            index, generation = (0, sequence - 3) if sequence <= 6 else (1, sequence - 7)
            key = f'objects/{index:04d}.bin'
            params['Key'] = key
            if operation == 'put_object':
                params.update(ContentType='application/octet-stream', Metadata={'heal-fixture': 'DV',
                              'nonce': f'{index}-{generation}'}, Tagging='suite=heal&fixture=DV')
        domain = domain or request.get('clockDomain')
        require(isinstance(domain, str) and bool(domain)
                and request.get('schemaVersion') == ack.get('schemaVersion') == 2
                and request.get('runId') == ack.get('runId') == run_id
                and request.get('sequence') == ack.get('sequence') == sequence
                and request.get('operation') == operation and request.get('params') == params
                and request.get('expectHttp') is None and request.get('identity') is None
                and request.get('clockDomain') == ack.get('clockDomain') == domain
                and ack.get('requestFile') == request_path.name
                and ack.get('requestSha256') == hashlib.sha256(request_path.read_bytes()).hexdigest()
                and ack.get('operationCheck') == 'PASS', 'I05 seed request/ACK binding differs')
        observation = ack.get('observation') or {}
        response = observation.get('response') or {}
        require(observation.get('operation') == operation and observation.get('params') == params
                and observation.get('endpoint') == 'node1' and observation.get('clockDomain') == domain
                and observation.get('clientCallInvoked') is True and observation.get('acknowledged') is True
                and observation.get('resultUnknown') is False and not observation.get('clientCleanupError')
                and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == (204 if sequence == 6 else 200)
                and response.get('ResponseMetadata', {}).get('RetryAttempts') == 0,
                'I05 seed contains an uncertain or rejected mutation')
        if sequence >= 3:
            version = response.get('VersionId', '')
            require(str(uuid.UUID(version)) == version and uuid.UUID(version).int and version not in seen,
                    'I05 seed has an invalid or repeated version')
            seen.add(version)
            entry = {'key': key, 'versionId': version, 'deleteMarker': sequence == 6,
                     'isLatest': sequence in (6, 9)}
            if sequence == 6:
                require(response.get('DeleteMarker') is True, 'I05 marker DELETE lacks its acknowledged marker')
            else:
                body = payload(f'{bucket}/{key}/{generation}', 32 * 1024**2 if index == 0 else 1024)
                expected_payload = {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
                require(request.get('payload') == observation.get('payload') == expected_payload,
                        'I05 seed payload differs from the deterministic request')
                require(isinstance(response.get('ETag'), str) and bool(response['ETag']),
                        'I05 seed PUT lacks its acknowledged ETag')
                public_properties[(key, version)] = {'ETag': response['ETag'], 'ContentLength': len(body),
                    'ContentType': params['ContentType'], 'Metadata': params['Metadata']}
                entry['payload'] = expected_payload
            expected.append(entry)
        for path in (request_path, ack_path):
            journal[path.relative_to(folder).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    baseline = one('original-ACK-bound-S3')
    require(baseline.get('bucket') == bucket and baseline == one('I05-timeout-original-S3'),
            'I05 original public baseline differs')
    verify_seed(expected, baseline)
    for entry in baseline['entries']:
        if entry['deleteMarker']:
            continue
        require(all(entry.get('properties', {}).get(field) == value
                    for field, value in public_properties[(entry['key'], entry['versionId'])].items())
                and entry.get('tags') == [{'Key': 'fixture', 'Value': 'DV'}, {'Key': 'suite', 'Value': 'heal'}],
                'I05 original public metadata, ETag or tags differ from seed request/ACK')

    timeout = folder / 'timeout'
    expected_timeout_items = {'scope.json', 'failure.json', 'caller'}
    expected_caller_items = {'scope.json', 'failure.json', 'transport'}
    has_contender = helper in {CONTENDER_HELPER, H2_CONTENDER_HELPER} | DRAIN_HELPERS
    if has_contender:
        expected_timeout_items.add('contender')
        expected_caller_items.add('admission.json')
    if helper in DRAIN_HELPERS:
        expected_timeout_items.add('timeout.json')
    require({p.name for p in timeout.iterdir()} == expected_timeout_items
            and {p.name for p in (timeout / 'caller').iterdir()} == expected_caller_items,
            'I05 archive contains an unreviewed action or lacks its exact failure boundary')
    timeout_failure = read(timeout / 'failure.json')
    caller_failure = read(timeout / 'caller/failure.json')
    expected_failure_state = 'NOT_OBSERVED' if helper in DRAIN_HELPERS else 'INCOMPLETE'
    require(timeout_failure.get('diagnosticComplete') is False
            and timeout_failure.get('errorType') == failure['type']
            and timeout_failure.get('state') == expected_failure_state
            and timeout_failure.get('canonicalCasePassClaimed') is False
            and timeout_failure.get('originalRequestsAndResponsesRetained') is True
            and caller_failure.get('observationComplete') is False
            and caller_failure.get('errorType') == failure['type']
            and caller_failure.get('state') == expected_failure_state
            and caller_failure.get('canonicalCasePassClaimed') is False
            and caller_failure.get('originalTransportEvidenceRetained') is True,
            'I05 nested failure records differ from the exact retained diagnostic failure')
    scope = read(timeout / 'scope.json')
    entry = scope.get('entry')
    require(entry in expected and entry.get('key') == 'objects/0000.bin' and not entry['isLatest']
            and entry.get('deleteMarker') is False and scope.get('runId') == run_id
            and scope.get('candidateCommit') == CANDIDATE
            and scope.get('bindingSha256') == hashlib.sha256(canonical(binding).encode()).hexdigest(),
            'I05 caller target is not its acknowledged historical seed')
    transport = timeout / 'caller/transport'
    require({p.name for p in transport.iterdir()} == {'request.json', 'request-written.json', 'disconnected.json'},
            'I05 caller transport contains another request or result')
    request, sent, closed = (read(transport / name)
                             for name in ('request.json', 'request-written.json', 'disconnected.json'))
    caller_transport_request = request
    target = f'/{bucket}/{entry["key"]}?versionId={entry["versionId"]}'
    require(request.get('operation') == 'get_object' and request.get('params') == {
                'Bucket': bucket, 'Key': entry['key'], 'VersionId': entry['versionId']}
            and request.get('endpoint') == 'node1' and request.get('endpointUrl') == binding['endpoints']['node1']
            and all(request.get(field) == binding[field] for field in (
                'candidateCommit', 'harnessCommit', 'candidatePackageSha256', 'candidateBinarySha256'))
            and request.get('bindingSha256') == scope['bindingSha256']
            and closed.get('clientSocketClosed') is True and closed.get('connectionAttempts') == 1
            and type(closed.get('clientClosedMonotonicNs')) is int
            and closed['clientClosedMonotonicNs'] >= sent.get('requestWrittenMonotonicNs', 0),
            'I05 transport is not the sole closed read-only historical GET')
    for value in (request, sent, closed):
        require(value.get('runId') == run_id and value.get('requestTarget') == target
                and value.get('callerMarker') == request.get('callerMarker')
                and re.fullmatch(r'auto-testing-i05/[0-9a-f-]{36}', value.get('callerMarker', ''))
                and value.get('clockDomain') == domain and value.get('retryAllowed') is False
                and value.get('canonicalCasePassClaimed') is False,
                'I05 closed caller lost its exact request identity')
    for value in (sent, closed):
        require(value.get('requestSha256') == hashlib.sha256((transport / 'request.json').read_bytes()).hexdigest()
                and value.get('requestWriteCompleted') is True
                and value.get('resultUnknown') is True and value.get('acknowledged') is False
                and value.get('serverOwnershipProven') is False,
                'I05 closed caller was retried or promoted to an acknowledged result')
    if helper in DRAIN_HELPERS:
        caller_scope = read(timeout / 'caller/scope.json')
        require(caller_scope.get('runId') == run_id and caller_scope.get('candidateCommit') == CANDIDATE
                and caller_scope.get('bindingSha256') == scope['bindingSha256']
                and caller_scope.get('entry') == entry
                and caller_scope.get('admissionSeconds') == ADMISSION_SECONDS
                and caller_scope.get('drainSeconds') == DRAIN_SECONDS
                and caller_scope.get('canonicalCasePassClaimed') is False
                and caller_scope.get('mutatesServiceConfiguration') is False
                and scope.get('minimumCallerPayloadBytes') == MIN_BACKPRESSURE_BYTES
                and scope.get('receiveBufferBytesRequested') == CALLER_RECEIVE_BUFFER_BYTES
                and request.get('receiveBufferBytesRequested') == CALLER_RECEIVE_BUFFER_BYTES
                and sent.get('requestInvocationStarted') is True
                and closed.get('responseObserved') is False and closed.get('connectionAttempts') == 1,
                'I05 drain archive changed its bounded read-only caller or time budget')
    if has_contender:
        contender = timeout / 'contender'
        require({p.name for p in contender.iterdir()} == {'000001-request.json', '000001-ack.json'},
                'I05 contender archive is not the sole one-shot request and ACK')
        request_path, ack_path = contender / '000001-request.json', contender / '000001-ack.json'
        request_bytes = request_path.read_bytes()
        request, ack = read(request_path), read(ack_path)
        body = payload(f'{run_id}/I05-timeout-contender', CONTENDER_BYTES)
        response = validate_timeout_ack(ack, request, request_bytes, run_id, 'node1', bucket,
                                        entry['key'], body, clock_domain=domain)
        require(ack.get('operationCheck') == 'PASS'
                and ack.get('observation', {}).get('acknowledged') is False
                and ack.get('observation', {}).get('resultUnknown') is False,
                'I05 contender result is not a durable, known refusal')
        caller_admission = read(timeout / 'caller/admission.json')
        holder = (caller_admission.get('attempt') or {})
        require(holder.get('admitted') is True and holder.get('observedAcquiredWithoutDrop') is True,
                'I05 contender lacks its original admitted GET lock holder')
        require(set(baselines) == set(nodes) and len(windows) == sample_counts['vm000'] * len(nodes),
                'I05 contender archive lacks each peer journal window')
        rows = {node: [] for node in nodes}
        previous_cursors = {}
        window_counts = {node: 0 for node in nodes}
        for window in windows:
            node = window.get('node')
            require(node in rows and window.get('complete') is True
                    and window.get('unit') == owners[node]['unit'] and not window.get('sourceMessageLoss')
                    and isinstance(window.get('beforeCursor'), str)
                    and isinstance(window.get('afterCursor'), str)
                    and (node not in previous_cursors or window['beforeCursor'] == previous_cursors[node])
                    and isinstance(window.get('journal'), str),
                    'I05 contender lock journal is incomplete or belongs to another service')
            previous_cursors[node] = window['afterCursor']
            window_counts[node] += 1
            for line in window['journal'].splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError('I05 contender lock journal contains malformed JSON') from error
                require(isinstance(event, dict), 'I05 contender lock journal contains a non-object event')
                rows[node].append(event)
        require(len(set(window_counts.values())) == 1 and min(window_counts.values()) >= 3,
                'I05 contender lock windows are incomplete or imbalanced across H2 peers')
        require(all(not any(event.get('request_id') == response.get('ResponseMetadata', {}).get('RequestId')
                            for event in events) for node, events in rows.items() if node != 'vm000'),
                'I05 contender request ID appeared on a foreign endpoint')
        proof = timeout_proof(rows['vm000'], response, holder, bucket, entry['key'])
        require(proof is not None and proof.get('typedTimeoutBeyondBudgetObserved') is True
                and proof.get('writeLockAcquired') is False,
                'I05 contender ACK lacks its exact failed target write-lock attempt')
        if helper in DRAIN_HELPERS:
            caller_request_id = holder.get('request_id')
            caller_lock_id = holder.get('lock_attempt_id')
            caller_events = holder.get('events') or []
            caller_socket = caller_admission.get('socket') or {}
            require(caller_admission.get('node') == 'vm000'
                    and holder.get('states') == ['acquiring', 'acquired']
                    and holder.get('diagnosticGuardDropped') is False
                    and holder.get('distributedReleaseProven') is False
                    and holder.get('heldAtDisconnectProven') is False
                    and isinstance(caller_request_id, str) and str(uuid.UUID(caller_request_id)) == caller_request_id
                    and uuid.UUID(caller_request_id).int
                    and isinstance(caller_lock_id, str) and str(uuid.UUID(caller_lock_id)) == caller_lock_id
                    and uuid.UUID(caller_lock_id).int and len(caller_events) == 2
                    and [event.get('state') for event in caller_events] == ['acquiring', 'acquired'],
                    'I05 caller lacks the exact admitted GET owner at its intentional disconnect')
            caller_uri = f'/{bucket}/{entry["key"]}?versionId={entry["versionId"]}'
            for event in caller_events:
                require(event.get('target') == TARGET and event.get('request_id') == caller_request_id
                        and event.get('lock_attempt_id') == caller_lock_id
                        and event.get('event') == 'object_namespace_lock' and event.get('component') == 'ecstore'
                        and event.get('subsystem') == 'object_lock' and event.get('op') == 'get_object'
                        and event.get('mode') == 'read' and event.get('bucket') == bucket
                        and event.get('object') == entry['key']
                        and event.get('requested_version_id') == entry['versionId']
                        and event.get('owner') == holder.get('owner')
                        and event.get('namespace') == holder.get('namespace')
                        and len([span for span in event.get('spans', [])
                                 if span.get('name') == 'http-request'
                                 and span.get('request_id') == caller_request_id
                                 and span.get('method') == 'GET' and span.get('uri') == caller_uri
                                 and span.get('user_agent') == caller_transport_request.get('callerMarker')]) == 1,
                        'I05 admitted caller lock owner is not bound to its exact historical GET')
            caller_lock_rows = [event for event in rows['vm000']
                                if event.get('target') == TARGET and event.get('request_id') == caller_request_id]
            require([event.get('state') for event in caller_lock_rows] == ['acquiring', 'acquired', 'guard_dropped']
                    and all(event.get('lock_attempt_id') == caller_lock_id
                            and event.get('op') == 'get_object' and event.get('mode') == 'read'
                            and event.get('bucket') == bucket and event.get('object') == entry['key']
                            and event.get('requested_version_id') == entry['versionId']
                            and event.get('owner') == holder.get('owner')
                            and event.get('namespace') == holder.get('namespace')
                            for event in caller_lock_rows),
                    'I05 caller journal lost or changed its original GET lock lifecycle')
            dropped = [event for event in rows['vm000']
                       if event.get('request_id') == caller_request_id
                       and event.get('event') == 'get_object_stream_body']
            require(len(dropped) == 1 and dropped[0].get('state') == 'dropped_incomplete'
                    and dropped[0].get('bucket') == bucket and dropped[0].get('object') == entry['key'],
                    'I05 caller disconnect lacks its exact incomplete response-body event')
            mutations = []
            for event in rows['vm000']:
                spans = [event.get('span'), *(event.get('spans') or [])]
                for span in spans:
                    if isinstance(span, dict) and span.get('method') in {'PUT', 'POST', 'PATCH', 'DELETE'}:
                        mutations.append((event.get('request_id'), span.get('method'), span.get('uri')))
            require(mutations and all(request_id == response['ResponseMetadata']['RequestId']
                    and method == 'PUT' and uri == f'/{bucket}/{entry["key"]}'
                    for request_id, method, uri in mutations),
                    'I05 caller window contains another or unknown S3 mutation')
            backpressure = one('I05-timeout-caller-backpressure-socket')
            expected_process = caller_socket.get('process')
            samples = backpressure.get('samples') or []
            require(backpressure.get('node') == 'vm000' and backpressure.get('readOnly') is True
                    and backpressure.get('process') == expected_process
                    and backpressure.get('clockDomain') == caller_socket.get('clockDomain')
                    and backpressure.get('networkNamespace') == caller_socket.get('networkNamespace')
                    and backpressure.get('clientAddress') == sent.get('localAddress')
                    and backpressure.get('serverAddress') == sent.get('peerAddress')
                    and len(samples) == 2
                    and all(sample.get('vanishedDescriptors') == 0
                            and len(sample.get('ownedMatches') or []) == 1
                            and sample['ownedMatches'][0].get('state') == '01'
                            and type(sample['ownedMatches'][0].get('txQueueBytes')) is int
                            and sample['ownedMatches'][0]['txQueueBytes'] > 0
                            for sample in samples)
                    and len({sample['ownedMatches'][0].get('inode') for sample in samples}) == 1,
                    'I05 contender is not bound to the original backpressured caller socket')
            timeout_proof_record = read(timeout / 'timeout.json')
            require(timeout_proof_record.get('requestId') == response['ResponseMetadata']['RequestId']
                    and timeout_proof_record.get('lockAttemptId') == proof['lockAttemptId']
                    and timeout_proof_record.get('owner') == proof['owner']
                    and timeout_proof_record.get('namespace') == proof['namespace']
                    and timeout_proof_record.get('events') == proof['events']
                    and timeout_proof_record.get('typedTimeoutBeyondBudgetObserved') is True
                    and timeout_proof_record.get('writeLockAcquired') is False
                    and timeout_proof_record.get('backpressureSocket') == backpressure
                    and timeout_proof_record.get('readResponseBackpressureObservedAfterTimeout') is True
                    and timeout_proof_record.get('canonicalCasePassClaimed') is False,
                    'I05 contender timeout proof differs from the exact archived lock and socket evidence')
            baseline = baselines.get('vm000')
            drain_sockets = many('I05-caller-drain-socket')
            drain_resources = many('I05-caller-drain-resources')
            first_drain = next(index for index, (label, _) in enumerate(numbered)
                               if label == 'I05-caller-drain-socket')
            drain_windows = [read(folder / name) for label, name in numbered[first_drain:]
                             if label == 'I05-caller-window']
            require(baseline is not None and len(drain_sockets) == len(drain_resources) >= 1
                    and len(drain_windows) in {len(drain_sockets), len(drain_sockets) - 1},
                    'I05 drain archive lacks a complete service and socket sample pair')
            def same_socket_process(value):
                process = value.get('process') or {}
                expected = baseline['identity']
                require(all(process.get(name) == expected.get(name) for name in (
                    'pid', 'startTicks', 'bootId', 'unit', 'executable', 'binarySha256', 'mountNamespace',
                    'commandSha256', 'environmentFileSha256', 'settings')),
                    'I05 drain socket sample crossed its original service process')
            for socket_state, resource in zip(drain_sockets, drain_resources, strict=True):
                same_socket_process(socket_state)
                require(socket_state.get('node') == resource.get('node') == 'vm000'
                        and socket_state.get('clockDomain') == caller_socket.get('clockDomain')
                        and socket_state.get('networkNamespace') == caller_socket.get('networkNamespace')
                        and socket_state.get('clientAddress') == sent.get('localAddress')
                        and socket_state.get('serverAddress') == sent.get('peerAddress')
                        and socket_state.get('readOnly') is True
                        and socket_state.get('continuousAbsenceProven') is False
                        and len(socket_state.get('samples') or []) == 2
                        and all(sample.get('vanishedDescriptors') == 0 and not sample.get('ownedMatches')
                                for sample in socket_state['samples'])
                        and type(resource.get('fileDescriptors')) is int and resource['fileDescriptors'] >= 0
                        and resource.get('readOnly') is True
                        and resource.get('identity', {}).get('settings') == baseline['identity'].get('settings'),
                        'I05 drain observation contains a changed process, socket or resource sample')
                same_process(resource.get('identity'), baseline['identity'])
            if helper == DESCRIPTOR_HELPER:
                resource_states = [baseline, one('I05-caller-admission-resources'), *drain_resources]
                for resource in resource_states:
                    same_process(resource.get('identity'), baseline['identity'])
                    census = resource.get('descriptorCensus')
                    validate_census(census)
                    require(resource.get('node') == 'vm000' and resource.get('readOnly') is True
                            and resource.get('clockDomain') == baseline['identity']['bootId']
                            and resource.get('networkNamespace') == caller_socket.get('networkNamespace')
                            and resource.get('fileDescriptors') == census['fileDescriptors']
                            and resource['identity'].get('settings') == {
                                **runtime_config('lock-timeout'), 'RUSTFS_HEAL_TASK_TIMEOUT_SECS': '300'},
                            'I05 resource census crossed its original process or network namespace')
                for socket_state in [one('I05-caller-before-disconnect-socket'), backpressure, *drain_sockets]:
                    same_socket_process(socket_state)
                    require(socket_state.get('networkNamespace') == baseline.get('networkNamespace')
                            and socket_state.get('clockDomain') == baseline['identity']['bootId']
                            and socket_state.get('clientAddress') == sent.get('localAddress')
                            and socket_state.get('serverAddress') == sent.get('peerAddress'),
                            'I05 socket census crossed its original caller or network namespace')
                    for sample in socket_state.get('samples') or []:
                        validate_census(sample)
                        matches = [{k: v for k, v in row.items()
                                    if k not in {'localAddress', 'remoteAddress'}}
                                   for row in sample['ownedTcpConnections']
                                   if row['localAddress'] == socket_state['serverAddress']
                                   and row['remoteAddress'] == socket_state['clientAddress']]
                        require(sample.get('ownedMatches') == matches,
                                'I05 caller socket census lost its exact owned TCP join')
            write_rows = [event for event in rows['vm000'] if event.get('mode') == 'write']
            require(write_rows == proof['events']
                    and [event.get('state') for event in write_rows] == ['acquiring', 'failed'],
                    'I05 drain journal contains a successful or unbound namespace write')
    for path in timeout.rglob('*.json'):
        read(path)
        journal[path.relative_to(folder).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {'kind': 'unresolved-i05-timeout', 'runId': run_id, 'nodes': nodes, 'bucket': bucket,
            'owners': owners, 'candidateCommit': CANDIDATE, 'harnessCommit': helper,
            'binarySha256': binding['candidateBinarySha256'], 'ackFilesSha256': digest(journal),
            'businessSceneSha256': {node: digest(physical_content(scene)[node]) for node in nodes}}
    validate(plan)
    return [plan]
