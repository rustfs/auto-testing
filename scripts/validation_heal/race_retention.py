"""Isolate a stopped G01 scene without replaying writes or restoring old shards."""

import hashlib
import json
import re

try:
    from .heal_probe import require
    from .healthy import physical_content
except ImportError:
    from heal_probe import require
    from healthy import physical_content


CASE = 'heal.race-unversioned-overwrite'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def validate(plan):
    require(set(plan) == {'kind', 'runId', 'nodes', 'bucket', 'owners', 'businessSceneSha256',
                         'candidateCommit', 'harnessCommit', 'binarySha256', 'ackFilesSha256'}
            and plan['kind'] == 'unresolved-g01-race'
            and re.fullmatch(r'gh-[1-9][0-9]*-[1-9][0-9]*-h[12]', plan['runId'])
            and len(plan['nodes']) == len(set(plan['nodes'])) == (1 if plan['runId'].endswith('-h1') else 4)
            and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in plan['nodes'])
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}', plan['bucket'])
            and all(re.fullmatch(r'[0-9a-f]{40}', plan[field]) for field in ('candidateCommit', 'harnessCommit'))
            and all(re.fullmatch(r'[0-9a-f]{64}', plan[field]) for field in ('binarySha256', 'ackFilesSha256'))
            and set(plan['owners']) == set(plan['businessSceneSha256']) == set(plan['nodes']),
            'G01 isolation plan identity, scope or evidence digest differs')
    for node in plan['nodes']:
        owner = plan['owners'][node]
        require(owner.get('runId') == plan['runId'] and owner.get('bucket') == plan['bucket']
                and owner.get('runtimeProfile') == 'manual' and owner.get('binarySha256') == plan['binarySha256']
                and owner.get('unit') == 'rustfs-validation-heal-' + plan['runId'] + '.service'
                and len(owner.get('mounts', [])) == 4
                and re.fullmatch(r'[0-9a-f]{64}', plan['businessSceneSha256'][node]),
                'G01 isolation lost its exact reserved owner or stopped physical scene')
    require(len({owner['hostname'] for owner in plan['owners'].values()}) == len(plan['nodes']),
            'G01 isolation host aliases overlap')


def archived_plan(artifact, failed_run, candidate):
    require(type(failed_run) is int and failed_run > 0, 'G01 isolation needs one exact failed run')
    paths = list(artifact.glob('*/run/*/cases/*.json'))
    require(len(paths) == 1 and paths[0].name == CASE + '.json', 'G01 isolation cannot omit another case')
    result = json.loads(paths[0].read_text())
    provenance = result.get('provenance') or {}
    require(result.get('case_id') == CASE and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and provenance.get('source_commit') == candidate['source_sha']
            and provenance.get('candidate_sha256') == candidate['package_sha256'],
            'G01 isolation original failed result or candidate differs')
    evidence = paths[0].parents[1] / 'evidence' / CASE
    summary = json.loads((evidence / 'healthy-summary.json').read_text())
    rows = summary.get('results', [])
    retained = [row for row in rows if row.get('cleanup') != 'PASS']
    require(summary.get('caseId') == CASE and summary.get('cleanup') == 'FAIL' and len(retained) == 1
            and retained[0].get('verdict') == 'FAIL' and retained[0].get('topology') in {'H1', 'H2'}
            and retained[0].get('branch') in {'overwrite-vs-deep', 'overwrite-vs-read-repair'}
            and all(row.get('caseId') == CASE for row in rows),
            'G01 isolation requires one unmixed retained public branch')
    row = retained[0]
    folder = evidence / row['topology'] / row['branch']
    def one(label):
        matches = list(folder.glob('*-' + label + '.json'))
        require(len(matches) == 1, 'G01 isolation lacks one original ' + label)
        return json.loads(matches[0].read_text())
    binding, scene, dirty = one('binding'), one('retained-failure-scene'), one('last-ACK-rescue-required')
    require(re.fullmatch(rf'gh-{failed_run}-[1-9][0-9]*-{row["topology"].lower()}', binding.get('runId', ''))
            and binding.get('candidateCommit') == candidate['source_sha']
            and binding.get('candidatePackageSha256') == candidate['package_sha256']
            and binding.get('candidateBinarySha256') == provenance.get('binary_sha256')
            and binding.get('harnessCommit') == provenance.get('testing_commit')
            and dirty.get('state') == 'DIRTY' and dirty.get('namespacesRetained') is True
            and dirty.get('lastACKPhysicalAuthorityVerified') is False
            and dirty.get('originalGenerationBlindRestorationProhibited') is True,
            'G01 isolation run/provenance or last-ACK refusal differs')
    reservations = [json.loads(path.read_text()) for path in folder.glob('*-reserved.json')]
    owners = {item['node']: item['owner'] for item in reservations}
    require(len(owners) == len(reservations) == len(scene) and set(owners) == set(scene)
            and all(item.get('reserved') is True for item in reservations), 'G01 isolation lacks all reserved hosts')
    ack_files = {}
    for name in ('seed-ACKs', 'foreground-ACKs'):
        journal = folder / name
        entries = sorted(journal.glob('*.json'))
        require(0 < len(entries) <= 1024, 'G01 isolation lacks its bounded original request/ACK journal')
        for path in entries:
            require(re.fullmatch(r'[0-9]{6}-(request|ack)\.json', path.name) and path.stat().st_size <= 1024**2,
                    'G01 isolation journal contains an unexpected member')
            item = json.loads(path.read_text())
            require(item.get('runId') == binding['runId'], 'G01 isolation journal belongs to another run')
            # A missing/unknown ACK remains unknown. Isolation never infers its
            # effect or sends another request, including for explicit refusals.
            operation = item.get('operation') if path.name.endswith('-request.json') else (item.get('observation') or {}).get('operation')
            require(operation in {'create_bucket', 'put_object'}, 'G01 isolation cannot omit a multipart mutation')
            ack_files[name + '/' + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {'kind': 'unresolved-g01-race', 'runId': binding['runId'], 'nodes': sorted(owners),
            'bucket': binding['bucketPrefix'], 'owners': owners, 'candidateCommit': candidate['source_sha'],
            'harnessCommit': binding['harnessCommit'], 'binarySha256': binding['candidateBinarySha256'],
            'businessSceneSha256': {node: digest(value) for node, value in physical_content(scene).items()},
            'ackFilesSha256': digest(ack_files)}
    validate(plan)
    for node, value in scene.items():
        require(value.get('quiescent') is True and type(value.get('pid')) is int and value['pid'] == 0
                and value.get('unit') == owners[node]['unit'] and value.get('hostname') == owners[node]['hostname']
                and value.get('binarySha256') == plan['binarySha256']
                and len(value['formats']) == 4 and all(type(part.get('disk')) is int for part in value['formats'])
                and {part['disk'] for part in value['formats']} == set(range(4)),
                'G01 isolation lacks a stopped four-drive original failure scene')
    return [plan]


def isolate(fleet, plan):
    validate(plan)
    require(fleet.run_id == plan['runId'] and list(fleet.nodes) == plan['nodes'], 'G01 isolation changed its fleet')
    def params(node):
        return {'expectedOwner': plan['owners'][node], 'expectedOriginalSceneSha256': plan['businessSceneSha256'][node]}
    audits = {}
    for node in fleet.nodes:
        audit = audits[node] = fleet.record('race-retention-preflight', {'node': node,
            **fleet.remote(node, 'retained-scene-audit', **params(node))})
        require(audit.get('inspectionOnly') is True and audit.get('mutated') is False
                and audit.get('credentialContentsRead') is False, 'G01 isolation preflight did not preserve its read boundary')
    for node in fleet.nodes:
        fleet.record('race-retained-service-logs', {'node': node, **fleet.remote(node, 'logs')})
        proof = fleet.record('race-retention-isolation', {'node': node,
            **fleet.remote(node, 'retained-scene-isolate', expectedBaseline=audits[node]['baseline'], **params(node))})
        require(proof.get('activeNamespaceIsolated') is True and proof.get('baseline') == audits[node]['baseline']
                and all(proof.get(field) is False for field in ('originalSceneRestored', 'backupRestorationUsed',
                    'canonicalPassClaimed', 'credentialContentsRead')), 'G01 isolation did not preserve its original failure')
    for node in fleet.nodes:
        proof = fleet.record('race-retention-post-audit', {'node': node,
            **fleet.remote(node, 'retained-scene-audit', **params(node))})
        require(proof.get('inspectionOnly') is True and proof.get('mutated') is False
                and proof.get('activeNamespaceIsolated') is True and proof.get('baseline') == audits[node]['baseline']
                and len(proof.get('privateArchiveParents', [])) == 5
                and all(row.get('exists') is True and row.get('uid') == 0 and row.get('mode') == 0o700
                        for row in proof['privateArchiveParents']), 'G01 isolation post-audit drifted')
    return {'runId': plan['runId'], 'cleanup': 'ISOLATED', 'activeNamespaceIsolated': True,
            'originalSceneRestored': False, 'backupRestorationUsed': False, 'originalVerdictRewritten': False,
            'canonicalPassClaimed': False, 'namespaceDeletionPerformed': False, 'intentSha256': digest(plan)}
