"""Frozen G05 committed multipart geometry and bytes; caller owns staging/S3 proof."""
import copy
import hashlib
import re
import uuid

try:
    from . import mpu_race_fixture as model
    from .heal_probe import require
    from .shards import files, map_target
    from .mpu_race_backups import verify as verify_backups
except ImportError:
    import mpu_race_fixture as model
    from heal_probe import require
    from shards import files, map_target
    from mpu_race_backups import verify as verify_backups


def entries(manifest):
    require(manifest.get('key') == model.KEY and re.fullmatch(r'rv-heal-[0-9a-f]{16}', manifest.get('bucket', ''))
            and manifest.get('poisoned') is None and set(manifest.get('items', {})) == set(model.ROLES),
            'G05 committed proof requires the original known same-key model')
    completed = [manifest['items'][role] for role in model.ROLES if manifest['items'][role]['state'] == 'complete']
    require([item['role'] for item in completed] in (['committed'], ['committed', 'a']),
            'G05 committed population differs from the original and A Complete ACKs')
    result, seen = [], set()
    for index, item in enumerate(completed):
        version = item['versionId']
        require(isinstance(version, str) and str(uuid.UUID(version)) == version and uuid.UUID(version).int != 0
                and version not in seen and len(item['parts']) == 3, 'G05 Complete lacks one distinct real version')
        seen.add(version); digest = hashlib.sha256()
        for number, part in enumerate(item['parts'], 1):
            body = model.payload(manifest['bucket'], item['role'], number)
            require(type(part.get('number')) is int and part['number'] == number
                    and type(part.get('size')) is int and part['size'] == len(body)
                    and part.get('etag') == model.etag(body)
                    and part.get('sha256') == hashlib.sha256(body).hexdigest(),
                    'G05 committed part differs from its exact original role payload/ACK')
            digest.update(body)
        require(item['etag'] == model.multipart_etag(item['parts'])
                and type(item['payload'].get('size')) is int
                and item['payload'] == {'size': sum(model.PART_SIZES), 'sha256': digest.hexdigest()},
                'G05 Complete payload or ordered multipart ETag differs')
        result.append({'key': model.KEY, 'versionId': version, 'isLatest': index == len(completed) - 1,
            'deleteMarker': False, 'payload': copy.deepcopy(item['payload']),
            'multipartParts': [{'number': part['number'], 'size': part['size']} for part in item['parts']]})
    return result


def mapped(snapshot, bucket, expected, *, backup_directories=()):
    """Bind all three parts on every original coding index; reject unknown leaves."""
    require(len(snapshot) in (1, 4), 'G05 committed topology needs one or four original hosts')
    prefix = f'{bucket}/{model.KEY}/'
    scope = {node: {**value, 'files': [row for row in value['files'] if row['path'].startswith(prefix)]}
             for node, value in snapshot.items()}
    require(all(type(row.get('disk')) is int and row['disk'] in range(4)
                and type(row.get('size')) is int and row['size'] >= 0
                and isinstance(row.get('sha256'), str) and re.fullmatch(r'[0-9a-f]{64}', row['sha256'])
                for value in scope.values() for row in value['files']),
            'G05 committed file identity or digest/size is invalid')
    indexed = files(scope); cohorts = {}; used = set(); auxiliary = {}
    expected_versions = sorted((row['versionId'], row['isLatest'], False) for row in expected)
    for node, value in scope.items():
        require(len(value['formats']) == 4 and all(type(row.get('disk')) is int for row in value['formats'])
                and {row['disk'] for row in value['formats']} == set(range(4)), 'G05 committed scope lacks four distinct original drives')
        for drive in value['formats']:
            identity = (node, drive['disk'], prefix + 'xl.meta')
            metadata = indexed.get(identity, {}).get('metadata', {})
            versions = metadata.get('versions', [])
            require(metadata.get('bucket') == bucket and metadata.get('key') == model.KEY
                    and all(type(v.get('isLatest')) is bool and v.get('deleted') is False for v in versions)
                    and sorted((v['versionId'], v['isLatest'], v['deleted']) for v in versions) == expected_versions,
                    'G05 primary version/latest population differs from committed ACKs')
            used.add(identity)
    directories = set()
    for entry in expected:
        for number in (1, 2, 3):
            target = map_target(scope, bucket, model.KEY, 'data', number, entry['versionId'], latest_required=False)
            logical = target['logical']
            require(type(logical['size']) is int and logical['size'] == entry['payload']['size'] and uuid.UUID(target['dataDir']).int != 0
                    and all(type(p['number']) is int and type(p['size']) is int and type(p['actualSize']) is int for p in logical['parts'])
                    and [{'number': p['number'], 'size': p['size']} for p in logical['parts']] == entry['multipartParts']
                    and type(logical['erasure']['block_size']) is int and logical['erasure']['block_size'] > 0
                    and all(type(index) is int for index in logical['erasure']['distribution'])
                    and all(type(part['index']) is int for part in target['cohort'])
                    and logical['erasure']['data_blocks'] == (2 if len(snapshot) == 1 else 12)
                    and logical['erasure']['parity_blocks'] == (2 if len(snapshot) == 1 else 4),
                    'G05 committed total/part sizes or original EC geometry differs')
            cohorts[entry['versionId'], number] = target
            for part in target['cohort']:
                identity = (part['node'], part['disk'], part['relativePath']); used.add(identity)
                sidecar = (part['node'], part['disk'], part['relativePath'] + '.meta')
                if sidecar in indexed:
                    require(indexed[sidecar]['size'] > 0, 'G05 upload auxiliary sidecar is empty')
                    used.add(sidecar); auxiliary[sidecar] = indexed[sidecar]
        generation = {cohorts[entry['versionId'], n]['dataDir'] for n in (1, 2, 3)}
        require(len(generation) == 1 and not directories.intersection(generation),
                'G05 parts or distinct versions share the wrong committed generation')
        directories.update(generation)
    for identity, row in indexed.items():
        if identity in used:
            continue
        suffix = row['path'][len(prefix):]
        require(any(suffix == directory + '/xl.meta.bkp' for directory in backup_directories),
                'G05 committed key contains an unknown or unacknowledged leaf')
    return cohorts, auxiliary


def transition(before, after, original, final, backup_observations, binary_sha, inspector_sha):
    """Port physical_final; staging continuity and full S3 still close new payloads."""
    require(set(before) == set(after) and len(before) in (1, 4)
            and re.fullmatch(r'[0-9a-f]{64}', binary_sha)
            and original['bucket'] == final['bucket'] and original['key'] == final['key'] == model.KEY,
            'G05 committed transition changed original hosts, candidate or bucket/key')
    for node in before:
        require(before[node]['formats'] == after[node]['formats']
                and before[node].get('quiescent') is True and after[node].get('quiescent') is True
                and type(before[node].get('pid')) is int and type(after[node].get('pid')) is int
                and before[node].get('pid') == after[node].get('pid') == 0
                and before[node].get('binarySha256') == after[node].get('binarySha256') == binary_sha
                and before[node].get('hostname') == after[node].get('hostname'),
                'G05 committed proof lost stopped process, original formats or candidate')
    previous, current = entries(original), entries(final)
    require(len(previous) == 1 and len(current) == 2 and previous[0]['versionId'] == current[0]['versionId']
            and original['items']['committed'] == final['items']['committed']
            and original['items']['a']['state'] == original['items']['b']['state'] == 'pending'
            and len(original['items']['a']['parts']) in (2, 3) and len(original['items']['b']['parts']) == 3
            and original['items']['a']['parts'] == final['items']['a']['parts'][:len(original['items']['a']['parts'])]
            and original['items']['a']['metadata'] == final['items']['a']['metadata']
            and {k: v for k, v in original['items']['b'].items() if k != 'state'}
                == {k: v for k, v in final['items']['b'].items() if k != 'state'}
            and final['items']['a']['state'] == 'complete' and final['items']['b']['state'] == 'aborted'
            and all(original['items'][role]['uploadId'] == final['items'][role]['uploadId'] for role in model.ROLES),
            'G05 Complete replaced the old version or original sibling upload identities')
    old, old_aux = mapped(before, original['bucket'], previous)
    old_version = previous[0]['versionId']; new_version = current[1]['versionId']
    old_dir = old[old_version, 1]['dataDir']
    new, new_aux = mapped(after, final['bucket'], current, backup_directories=[old_dir])
    for number in (1, 2, 3):
        left, right = old[old_version, number], new[old_version, number]
        require({k: v for k, v in left['logical'].items() if k != 'isLatest'}
                == {k: v for k, v in right['logical'].items() if k != 'isLatest'}
                and {p['index']: (p['sha256'], p['size']) for p in left['cohort']}
                == {p['index']: (p['sha256'], p['size']) for p in right['cohort']},
                'G05 Complete changed an original committed multipart descriptor or shard')
    require(new[new_version, 1]['dataDir'] != old_dir, 'G05 A Complete reused the old generation')
    for identity, row in old_aux.items():
        require(identity not in new_aux or new_aux[identity] == row, 'G05 retained upload sidecar bytes changed')
    backups = verify_backups(before, after, backup_observations, list(before), original['bucket'],
                             old_version, new_version, binary_sha, inspector_sha)
    return new, {'oldVersionRetained': True, 'newVersionExactlyOnce': True,
        'allPartsCodingIndicesVerified': True, 'backupProof': backups,
        'reclaimedOldUploadSidecars': [list(identity) for identity in old_aux if identity not in new_aux],
        'uploadSidecarsAreCommittedAuthority': False, 'stagingContinuityStillRequired': True,
        'fullPayloadS3StillRequired': True, 'physicalRepairProved': False, 'canonicalCasePassClaimed': False}
