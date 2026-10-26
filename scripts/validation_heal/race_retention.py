"""Isolate stopped ACK-bearing races without replaying writes or restoring old shards."""

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
CASES = {CASE: ('unresolved-g01-race', {'overwrite-vs-deep', 'overwrite-vs-read-repair'}),
         'heal.race-versioned-put': ('unresolved-g02-race', {'new-version-vs-historical-heal', 'read-all-versions-during-heal'}),
         'heal.race-markers-and-batch-delete': ('unresolved-g04-race', {'create-marker','delete-marker','batch-version-delete','unversioned-delete'}),
         'heal.admin-nolock-safety': ('unresolved-a09-race', {'overwrite-nolock', 'delete-nolock'}),
         'heal.race-multipart-complete-abort': ('unresolved-g05-race', {'upload-continue', 'complete', 'abort'}),
         'heal.race-copy-and-metadata-update': ('unresolved-g06-race', {'copy-source', 'copy-destination', 'replace-metadata', 'update-tags'})}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def validate(plan):
    multipart = plan.get('kind') == 'unresolved-g05-race'
    fields = {'kind', 'runId', 'nodes', 'bucket', 'owners', 'businessSceneSha256',
              'candidateCommit', 'harnessCommit', 'binarySha256', 'ackFilesSha256'}
    require(set(plan) == fields | ({'mpuKeys', 'mpuSceneSha256'} if multipart else set())
            and plan['kind'] in ({kind for kind, _ in CASES.values()} | {'unresolved-i05-timeout', 'unresolved-i05-successor'})
            and re.fullmatch(r'gh-[1-9][0-9]*-[1-9][0-9]*-h[12]', plan['runId'])
            and len(plan['nodes']) == len(set(plan['nodes'])) == (1 if plan['runId'].endswith('-h1') else 4)
            and all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node) for node in plan['nodes'])
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}', plan['bucket'])
            and all(re.fullmatch(r'[0-9a-f]{40}', plan[field]) for field in ('candidateCommit', 'harnessCommit'))
            and all(re.fullmatch(r'[0-9a-f]{64}', plan[field]) for field in ('binarySha256', 'ackFilesSha256'))
            and set(plan['owners']) == set(plan['businessSceneSha256']) == set(plan['nodes']),
            'G01/G02 isolation plan identity, scope or evidence digest differs')
    for node in plan['nodes']:
        owner = plan['owners'][node]
        require(owner.get('runId') == plan['runId'] and owner.get('bucket') == plan['bucket']
                and (owner.get('runtimeProfile') == 'lock-timeout' and plan['kind'] == 'unresolved-i05-timeout' or
                     owner.get('runtimeProfile') == 'lock-successor' and plan['kind'] == 'unresolved-i05-successor' or
                     owner.get('runtimeProfile') == 'manual'
                     and plan['kind'] not in {'unresolved-i05-timeout', 'unresolved-i05-successor'} or
                     owner.get('runtimeProfile') == 'frozen-race-legacy'
                     and plan['kind'] in {'unresolved-g04-race', 'unresolved-g05-race'})
                and owner.get('binarySha256') == plan['binarySha256']
                and owner.get('unit') == 'rustfs-validation-heal-' + plan['runId'] + '.service'
                and len(owner.get('mounts', [])) == 4
                and re.fullmatch(r'[0-9a-f]{64}', plan['businessSceneSha256'][node]),
                'G01/G02 isolation lost its exact reserved owner or stopped physical scene')
    require(len({owner['hostname'] for owner in plan['owners'].values()}) == len(plan['nodes']),
            'G01/G02 isolation host aliases overlap')
    if multipart:
        require(plan['mpuKeys'] == ['objects-0256.bin'] and set(plan['mpuSceneSha256']) == set(plan['nodes'])
                and all(re.fullmatch(r'[0-9a-f]{64}', value) for value in plan['mpuSceneSha256'].values()),
                'G05 isolation lost its exact same-key stopped multipart scene')


def archived_plan(artifact, failed_run, candidate):
    require(type(failed_run) is int and failed_run > 0, 'G01/G02 isolation needs one exact failed run')
    paths = list(artifact.glob('*/run/*/cases/*.json'))
    require(len(paths) == 1 and paths[0].stem in CASES, 'G01/G02 isolation cannot omit another case')
    case = paths[0].stem
    kind, branches = CASES[case]
    result = json.loads(paths[0].read_text())
    provenance = result.get('provenance') or {}
    require(result.get('case_id') == case and result.get('verdict') == result.get('cleanup_status') == 'FAIL'
            and provenance.get('source_commit') == candidate['source_sha']
            and provenance.get('candidate_sha256') == candidate['package_sha256'],
            'G01/G02 isolation original failed result or candidate differs')
    evidence = paths[0].parents[1] / 'evidence' / case
    summary = json.loads((evidence / 'healthy-summary.json').read_text())
    rows = summary.get('results', [])
    retained = [row for row in rows if row.get('cleanup') != 'PASS']
    require(summary.get('caseId') == case and summary.get('cleanup') == 'FAIL' and len(retained) == 1
            and retained[0].get('verdict') in ({'FAIL','NOT_OBSERVED'} if kind=='unresolved-g04-race' else {'FAIL'})
            and retained[0].get('topology') in {'H1', 'H2'}
            and retained[0].get('branch') in branches
            and all(row.get('caseId', case if kind == 'unresolved-a09-race' else None) == case for row in rows),
            'G01/G02 isolation requires one unmixed retained public branch')
    row = retained[0]
    folder = evidence / row['topology'] / row['branch']
    def one(label):
        matches = list(folder.glob('*-' + label + '.json'))
        require(len(matches) == 1, 'G01/G02 isolation lacks one original ' + label)
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
            'G01/G02 isolation run/provenance or last-ACK refusal differs')
    reservations = [json.loads(path.read_text()) for path in folder.glob('*-reserved.json')]
    owners = {item['node']: item['owner'] for item in reservations}
    require(len(owners) == len(reservations) == len(scene) and set(owners) == set(scene)
            and all(item.get('reserved') is True for item in reservations), 'G01/G02 isolation lacks all reserved hosts')
    ack_files = {}
    multipart = kind == 'unresolved-g05-race'
    markers = kind == 'unresolved-g04-race'
    read_journals = (['mpu-read-ACKs', *['final-read-ACKs-node' + str(i) for i in range(1, len(owners) + 1)]]
                     if multipart else ['negative-read-ACKs-node'+str(i) for i in range(1,len(owners)+1)] if markers else
                     ['read-repair-ACKs'] if case == CASE and row['branch'] == 'overwrite-vs-read-repair' else [])
    if multipart:
        require({path.name for path in folder.glob('final-read-ACKs-*')} <= set(read_journals),
                'G05 isolation has an unbound final peer read journal')
    if markers:
        require({path.name for path in folder.glob('negative-read-ACKs-*')} <= set(read_journals),
                'G04 isolation has an unbound negative peer read journal')
    for name in ('seed-ACKs', 'foreground-ACKs', 'historical-read-ACKs', *read_journals):
        journal = folder / name
        entries = sorted(journal.glob('*.json'))
        if name == 'historical-read-ACKs' or name in read_journals or (multipart or markers) and name == 'foreground-ACKs':
            require(len(entries) <= 1024, 'G02 historical journal exceeds its bound')
            if not entries: continue
        require(0 < len(entries) <= 1024, 'G01/G02 isolation lacks its bounded original request/ACK journal')
        if multipart and name == 'seed-ACKs':
            require(any(re.fullmatch(r'[0-9]{6}-request\.json', path.name) for path in entries),
                    'G05 isolation requires an original SDK request, not only a model manifest')
        for path in entries:
            if markers and name in read_journals and re.fullmatch(r'[0-9]{6}-negative-proof\.json',path.name):
                require(path.stat().st_size<=1024**2,'G04 negative proof exceeds its evidence bound')
                item=json.loads(path.read_text());expected=item.get('expected') or {};params=expected.get('params') or {}
                require(params.get('Bucket')==binding['bucketPrefix']
                        and params.get('Key') in {f'objects-{i:04d}.bin' for i in range(257)}
                        and expected.get('httpStatus') in (404,405) and item.get('state') in ('PASS','FAIL')
                        and item.get('canonicalCasePassClaimed') is False,
                        'G04 negative proof belongs to another object or contract')
                ack_files[name+'/'+path.name]=hashlib.sha256(path.read_bytes()).hexdigest();continue
            if multipart and re.fullmatch(r'mpu-manifest-[0-9]{4}\.json', path.name):
                require(path.stat().st_size <= 1024**2, 'G05 isolation multipart manifest exceeds its bound')
                item = json.loads(path.read_text())
                require(item.get('bucket') == binding['bucketPrefix']
                        and item.get('key') == 'objects-0256.bin' and item.get('fixture') == 'DM',
                        'G05 isolation has a foreign multipart manifest')
                ack_files[name + '/' + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
                continue
            require(re.fullmatch(r'[0-9]{6}-(request|ack)\.json', path.name) and path.stat().st_size <= 1024**2,
                    'G01/G02 isolation journal contains an unexpected member')
            item = json.loads(path.read_text())
            require(item.get('runId') == binding['runId'], 'G01/G02 isolation journal belongs to another run')
            # A missing/unknown ACK remains unknown. Isolation never infers its
            # effect or sends another request, including for explicit refusals.
            operation = item.get('operation') if path.name.endswith('-request.json') else (item.get('observation') or {}).get('operation')
            if multipart or markers:
                request_item = item
                if path.name.endswith('-ack.json'):
                    request_name = item.get('requestFile', '')
                    require(re.fullmatch(r'[0-9]{6}-request\.json', request_name)
                            and request_name == path.name.replace('-ack.json', '-request.json'),
                            'G05 isolation receipt lacks its exact original request')
                    request_path = journal / request_name
                    require(request_path.is_file() and request_path.stat().st_size <= 1024**2
                            and hashlib.sha256(request_path.read_bytes()).hexdigest() == item.get('requestSha256'),
                            'G05 isolation receipt changed its original request bytes')
                    request_item = json.loads(request_path.read_text())
                    require(operation is None or operation == request_item.get('operation'),
                            'G05 isolation receipt reports a different SDK operation')
                operation = request_item.get('operation')
                params = request_item.get('params') or {}
                require(request_item.get('runId') == binding['runId'] and params.get('Bucket') == binding['bucketPrefix'],
                        'G05 isolation SDK request belongs to another reserved bucket')
                if operation in {'create_multipart_upload', 'upload_part', 'complete_multipart_upload',
                                 'abort_multipart_upload', 'list_parts', 'get_object', 'head_object', 'get_object_tagging'}:
                    if multipart:require(params.get('Key') == 'objects-0256.bin', 'G05 isolation SDK request changed its frozen multipart key')
                if markers:
                    keys=([v.get('Key') for v in params.get('Delete',{}).get('Objects',[])] if operation=='delete_objects'
                          else [params.get('Key')] if 'Key' in params else [])
                    require(all(isinstance(key,str) and key in {f'objects-{i:04d}.bin' for i in range(257)} for key in keys)
                            and (operation!='delete_objects' or 0<len(keys)<=4),
                            'G04 isolation SDK request changed its bounded flat key set')
            allowed = ({'list_multipart_uploads', 'list_parts', 'list_object_versions', 'get_object', 'head_object',
                        'get_object_tagging'} if multipart and name in read_journals else
                       {'get_object'} if name in {'historical-read-ACKs', 'read-repair-ACKs'} or markers and name in read_journals else
                       {'create_bucket','put_bucket_versioning','put_object','delete_object','delete_objects'} if markers else
                       {'create_bucket', 'put_bucket_versioning', 'put_object', 'create_multipart_upload',
                        'upload_part', 'complete_multipart_upload', 'abort_multipart_upload'} if multipart else
                       {'create_bucket', 'put_object', 'copy_object', 'put_object_tagging'} if kind == 'unresolved-g06-race' else
                       {'create_bucket', 'put_bucket_versioning', 'put_object', 'delete_object'} if kind == 'unresolved-a09-race' else
                       {'create_bucket', 'put_bucket_versioning', 'put_object'} if kind == 'unresolved-g02-race' else
                       {'create_bucket', 'put_object'})
            require(operation in allowed, 'G01/G02 isolation cannot omit a multipart mutation')
            ack_files[name + '/' + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {'kind': kind, 'runId': binding['runId'], 'nodes': sorted(owners),
            'bucket': binding['bucketPrefix'], 'owners': owners, 'candidateCommit': candidate['source_sha'],
            'harnessCommit': binding['harnessCommit'], 'binarySha256': binding['candidateBinarySha256'],
            'businessSceneSha256': {node: digest(value) for node, value in physical_content(scene).items()},
            'ackFilesSha256': digest(ack_files)}
    if multipart:
        mpu = one('raw-mpu-failure-scene')
        require(set(mpu) == set(owners), 'G05 isolation omitted an original stopped MPU peer')
        for value in mpu.values():
            require(value.get('runId') == binding['runId'] and value.get('bucket') == binding['bucketPrefix']
                    and value.get('keys') == ['objects-0256.bin'] and value.get('quiescent') is True
                    and value.get('readOnly') is True and value.get('originalPhysicalDrives') == 4
                    and value.get('binarySha256') == binding['candidateBinarySha256']
                    and digest({key: value[key] for key in ('bucket', 'keys', 'scopes', 'directories', 'files', 'bytesHashed')})
                        == value.get('sceneSha256'), 'G05 isolation original raw MPU capture differs')
        plan.update(mpuKeys=['objects-0256.bin'], mpuSceneSha256={node: value['sceneSha256'] for node, value in mpu.items()})
    validate(plan)
    for node, value in scene.items():
        require(value.get('quiescent') is True and type(value.get('pid')) is int and value['pid'] == 0
                and value.get('unit') == owners[node]['unit'] and value.get('hostname') == owners[node]['hostname']
                and value.get('binarySha256') == plan['binarySha256']
                and len(value['formats']) == 4 and all(type(part.get('disk')) is int for part in value['formats'])
                and {part['disk'] for part in value['formats']} == set(range(4)),
                'G01/G02 isolation lacks a stopped four-drive original failure scene')
    return [plan]


def isolate(fleet, plan):
    validate(plan)
    require(fleet.run_id == plan['runId'] and list(fleet.nodes) == plan['nodes'], 'G01/G02 isolation changed its fleet')
    def params(node):
        return {'expectedOwner': plan['owners'][node], 'expectedOriginalSceneSha256': plan['businessSceneSha256'][node],
                **({'mpuKeys': plan['mpuKeys'], 'expectedMpuSceneSha256': plan['mpuSceneSha256'][node]}
                   if plan['kind'] == 'unresolved-g05-race' else {})}
    audits = {}
    for node in fleet.nodes:
        audit = audits[node] = fleet.record('race-retention-preflight', {'node': node,
            **fleet.remote(node, 'retained-scene-audit', **params(node))})
        require(audit.get('inspectionOnly') is True and audit.get('mutated') is False
                and audit.get('credentialContentsRead') is False, 'G01/G02 isolation preflight did not preserve its read boundary')
    for node in fleet.nodes:
        fleet.record('race-retained-service-logs', {'node': node, **fleet.remote(node, 'logs')})
        proof = fleet.record('race-retention-isolation', {'node': node,
            **fleet.remote(node, 'retained-scene-isolate', expectedBaseline=audits[node]['baseline'], **params(node))})
        require(proof.get('activeNamespaceIsolated') is True and proof.get('baseline') == audits[node]['baseline']
                and all(proof.get(field) is False for field in ('originalSceneRestored', 'backupRestorationUsed',
                    'canonicalPassClaimed', 'credentialContentsRead')), 'G01/G02 isolation did not preserve its original failure')
    for node in fleet.nodes:
        proof = fleet.record('race-retention-post-audit', {'node': node,
            **fleet.remote(node, 'retained-scene-audit', **params(node))})
        require(proof.get('inspectionOnly') is True and proof.get('mutated') is False
                and proof.get('activeNamespaceIsolated') is True and proof.get('baseline') == audits[node]['baseline']
                and len(proof.get('privateArchiveParents', [])) == 5
                and all(row.get('exists') is True and row.get('uid') == 0 and row.get('mode') == 0o700
                        for row in proof['privateArchiveParents']), 'G01/G02 isolation post-audit drifted')
    return {'runId': plan['runId'], 'cleanup': 'ISOLATED', 'activeNamespaceIsolated': True,
            'originalSceneRestored': False, 'backupRestorationUsed': False, 'originalVerdictRewritten': False,
            'canonicalPassClaimed': False, 'namespaceDeletionPerformed': False, 'intentSha256': digest(plan)}
