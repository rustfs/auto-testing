"""Preserve unresolved I03 business and MPU bytes without claiming restoration."""

import copy
import hashlib
import json
import re

try:
    from .heal_probe import require
    from .healthy import physical_content
    from .mpu_scene import keys_for
except ImportError:
    from heal_probe import require
    from healthy import physical_content
    from mpu_scene import keys_for


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def validate_plan(plan):
    require(set(plan) == {'kind', 'runId', 'nodes', 'candidateCommit', 'harnessCommit', 'binarySha256',
                         'bucket', 'owners', 'businessSceneSha256', 'mpuSceneSha256', 'mpuKeys'}
            and plan['kind'] == 'unresolved-network-load'
            and re.fullmatch(r'gh-[1-9][0-9]*-[1-9][0-9]*-h2', plan['runId'])
            and len(plan['nodes']) == len(set(plan['nodes'])) == 4
            and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in plan['nodes'])
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}', plan['bucket'])
            and all(re.fullmatch(r'[0-9a-f]{40}', plan[field]) for field in ('candidateCommit', 'harnessCommit'))
            and re.fullmatch(r'[0-9a-f]{64}', plan['binarySha256']), 'I03 retention plan identity or scope differs')
    for field in ('owners', 'businessSceneSha256', 'mpuSceneSha256'):
        require(set(plan[field]) == set(plan['nodes']), 'I03 retention omits an original peer')
    endpoints = sorted({key.split('/')[1] for key in plan['mpuKeys']})
    require(plan['mpuKeys'] == keys_for(endpoints), 'I03 retention MPU scope differs from its two original sides')
    for node in plan['nodes']:
        owner = plan['owners'][node]
        require(owner.get('runId') == plan['runId'] and owner.get('bucket') == plan['bucket']
                and owner.get('runtimeProfile') == 'lock-quorum' and owner.get('binarySha256') == plan['binarySha256']
                and owner.get('unit') == 'rustfs-validation-heal-' + plan['runId'] + '.service'
                and len(owner.get('mounts', [])) == 4
                and all(re.fullmatch(r'[0-9a-f]{64}', plan[field][node]) for field in ('businessSceneSha256', 'mpuSceneSha256')),
                'I03 retention lacks the exact reserved candidate/runtime owner and stopped scene digests')
    require(len({owner['hostname'] for owner in plan['owners'].values()}) == 4,
            'I03 retention aliases do not identify four distinct reserved hosts')


def prepare(fleet, binding, keys, business, mpu_nodes):
    """Freeze the actual stopped failure scene after gate release, before moving."""
    require(fleet.run_id == binding['runId'] and not fleet.network_gates and not fleet.quorum_gates
            and set(business) == set(mpu_nodes) == set(fleet.nodes) and len(fleet.nodes) == 4,
            'I03 retention requires all stopped peers and released owned network/write gates')
    reservations = [json.loads(path.read_text()) for path in fleet.evidence.glob('*-reserved.json')]
    require(len(reservations) == 4 and {row.get('node') for row in reservations} == set(fleet.nodes)
            and all(row.get('reserved') is True for row in reservations), 'I03 retention lacks its original reservations')
    plan = {'kind': 'unresolved-network-load', 'runId': fleet.run_id, 'nodes': list(fleet.nodes),
        'candidateCommit': binding['candidateCommit'], 'harnessCommit': binding['harnessCommit'],
        'binarySha256': binding['candidateBinarySha256'], 'bucket': binding['bucketPrefix'], 'mpuKeys': list(keys),
        'owners': {row['node']: row['owner'] for row in reservations},
        'businessSceneSha256': {node: digest(value) for node, value in physical_content(business).items()},
        'mpuSceneSha256': {node: value['sceneSha256'] for node, value in mpu_nodes.items()}}
    validate_plan(plan)
    for node in fleet.nodes:
        scene, mpu = business[node], mpu_nodes[node]
        require(scene.get('quiescent') is True and type(scene.get('pid')) is int and scene['pid'] == 0
                and scene.get('unit') == plan['owners'][node]['unit']
                and scene.get('binarySha256') == plan['binarySha256']
                and len(scene['formats']) == 4 and mpu.get('keys') == keys
                and mpu.get('bucket') == plan['bucket'] and mpu.get('runId') == fleet.run_id
                and mpu.get('binarySha256') == plan['binarySha256'] and mpu.get('quiescent') is True
                and mpu.get('readOnly') is True and mpu.get('originalPhysicalDrives') == 4
                and digest({field: mpu[field] for field in ('bucket', 'keys', 'scopes', 'directories', 'files', 'bytesHashed')})
                    == mpu['sceneSha256'], 'I03 retention lacks its stopped original business/MPU capture')
    return fleet.record('network-retention-intent', copy.deepcopy(plan))


def isolate(fleet, plan):
    """Retry the same archive intent; never recover a shard, Abort or delete data."""
    validate_plan(plan)
    require(fleet.run_id == plan['runId'] and list(fleet.nodes) == plan['nodes'], 'I03 retention controller changed its original fleet')
    def params(node):
        return {'expectedOwner': plan['owners'][node], 'expectedOriginalSceneSha256': plan['businessSceneSha256'][node],
                'expectedMpuSceneSha256': plan['mpuSceneSha256'][node], 'mpuKeys': plan['mpuKeys']}
    audits = {}
    for node in fleet.nodes:
        audits[node] = fleet.record('network-retention-preflight', {'node': node,
            **fleet.remote(node, 'retained-scene-audit', **params(node))})
        require(audits[node].get('inspectionOnly') is True and audits[node].get('mutated') is False
                and audits[node].get('credentialContentsRead') is False,
                'I03 retention preflight did not preserve read-only credential boundaries')
    for node in fleet.nodes:
        result = fleet.record('network-retention-isolation', {'node': node,
            **fleet.remote(node, 'retained-scene-isolate', expectedBaseline=audits[node]['baseline'], **params(node))})
        require(result.get('activeNamespaceIsolated') is True and result.get('baseline') == audits[node]['baseline']
                and result.get('originalSceneRestored') is False and result.get('backupRestorationUsed') is False
                and result.get('canonicalPassClaimed') is False and result.get('credentialContentsRead') is False,
                'I03 retained scene move did not preserve the exact original failure')
    for node in fleet.nodes:
        proof = fleet.record('network-retention-post-audit', {'node': node,
            **fleet.remote(node, 'retained-scene-audit', **params(node))})
        require(proof.get('inspectionOnly') is True and proof.get('mutated') is False
                and proof.get('activeNamespaceIsolated') is True and proof.get('baseline') == audits[node]['baseline']
                and len(proof.get('privateArchiveParents', [])) == 5
                and all(row.get('exists') is True and row.get('uid') == 0 and row.get('mode') == 0o700
                        for row in proof['privateArchiveParents']), 'I03 retained scene post-audit drifted')
    return {'runId': plan['runId'], 'cleanup': 'ISOLATED', 'activeNamespaceIsolated': True,
            'originalSceneRestored': False, 'originalEvidenceComplete': False, 'originalVerdictRewritten': False,
            'canonicalPassClaimed': False, 'namespaceDeletionPerformed': False, 'intentSha256': digest(plan)}


def archived_plan(artifact, failed_run, candidate):
    """Select one retained branch; an isolated namespace is never already-clean."""
    case = 'heal.network-partition-and-lock-quorum'
    results = list(artifact.glob(f'*/run/{case}/cases/{case}.json'))
    require(type(failed_run) is int and failed_run > 0 and len(results) == 1, 'I03 isolation needs one exact failed run')
    result = json.loads(results[0].read_text())
    provenance = result.get('provenance') or {}
    require(result.get('case_id') == case and result.get('verdict') == 'FAIL' and result.get('cleanup_status') == 'FAIL'
            and provenance.get('source_commit') == candidate['source_sha']
            and provenance.get('candidate_sha256') == candidate['package_sha256'],
            'I03 isolation failed-result candidate or cleanup differs')
    evidence = results[0].parents[1] / 'evidence' / case
    summary = json.loads((evidence / 'healthy-summary.json').read_text())
    retained = [row for row in summary['results'] if row.get('cleanup') != 'PASS']
    require(summary.get('cleanup') == 'FAIL' and len(retained) == 1
            and retained[0].get('verdict') == 'FAIL' and retained[0].get('topology') == 'H2'
            and retained[0].get('branch') in {'minority-partition', 'read-boundary', 'write-boundary', 'lock-boundary', 'rejoin'},
            'I03 isolation must preserve exactly one failed branch')
    folder = evidence / 'H2' / retained[0]['branch']
    def one(label):
        paths = list(folder.glob('*-' + label + '.json'))
        require(len(paths) == 1, 'I03 isolation lacks its original ' + label)
        return json.loads(paths[0].read_text())
    plan, binding = one('network-retention-intent'), one('binding')
    validate_plan(plan)
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-h2', plan['runId'])
            and plan['runId'] == binding.get('runId') and plan['bucket'] == binding.get('bucketPrefix')
            and plan['candidateCommit'] == binding.get('candidateCommit') == candidate['source_sha']
            and plan['harnessCommit'] == binding.get('harnessCommit') == provenance.get('testing_commit')
            and plan['binarySha256'] == binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and binding.get('candidatePackageSha256') == candidate['package_sha256'],
            'I03 isolation original plan/binding/provenance differs')
    reservations = [json.loads(path.read_text()) for path in folder.glob('*-reserved.json')]
    require(len(reservations) == 4 and {row.get('node') for row in reservations} == set(plan['nodes'])
            and all(row.get('reserved') is True and row.get('owner') == plan['owners'][row['node']] for row in reservations),
            'I03 isolation owner differs from its original reservation')
    return [plan]
