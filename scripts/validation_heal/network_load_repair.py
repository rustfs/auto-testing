"""Bind refused DL deletes to exact historical generations and product recovery."""

import copy
import uuid

try:
    from .heal_probe import require
    from .network_load import validate_call
    from .shards import files, map_target, run_task
    from .versions import public_version_id, validate_version_receipts
except ImportError:
    from heal_probe import require
    from network_load import validate_call
    from shards import files, map_target, run_task
    from versions import public_version_id, validate_version_receipts


CANDIDATE = '1c7fc818dd858247c45b39542abd8291b1e6e31b'
SOURCE_FILES = {
    'crates/ecstore/src/disk/local.rs': 'c715c122c06eafb5861ab560e1acac087e5326cd72e2063f0a437786fb4a965b',
    'crates/ecstore/src/disk/local/delete_recovery.rs': 'dc631db4794b0189d42636247f407e32d1d94fc65253c17b6f4942655f8ae7e3',
    'crates/ecstore/src/set_disk/ops/heal.rs': 'fc576119f455848ce279772ffca6d2943cb9f909968cae367860a9887d3e020e',
    'crates/ecstore/src/disk/mod.rs': 'cd045a322ad7e41e2253ac08554f10634192345cbd923f2854553a229c036f4b',
    'crates/filemeta/src/fileinfo.rs': 'e2553d6b0210ad15d723896735c5a5a40528d9bce10d9a6e02f7ac2fff271c24',
}


CANDIDATES = frozenset((CANDIDATE, '85c706cac09d53d0649487b8f1865f18beb53225',
                        '62ff96774fa4139d45f14d70b70519aae8070ac0'))


def source_reference(candidate):
    require(candidate in CANDIDATES, 'full I03 DL delete recovery is not reviewed for this exact candidate')
    return {'candidateCommit': candidate, 'sourceFiles': SOURCE_FILES.copy(),
        'deleteWriteQuorum': 'configured set / 2 + 1', 'failedDeleteAttemptsProductRollback': True,
        'rollbackFailureIsNotSuccessfulDelete': True, 'healSettlesDeleteBeforeReadingVersion': True,
        'preparedTransactionAloneProvesNeitherCommitNorRollback': True,
        'finalOriginalBusinessPhysicalAndPayloadProofRequired': True}


def classify_deletes(before, after, fixture, receipts, plan, nodes, delete_records):
    """Preserve all control assertions; only the original refused old-version delete may leave residue."""
    source_reference(fixture['sourceContract']['candidateCommit'])
    require(list(before) == list(after) == list(nodes), 'DL failure scene changed original node ordering')
    left, right = files(before), files(after)
    scopes, targets = {}, []
    for worker in receipts['workers']:
        if worker['role'] != 'writer': continue
        endpoint = worker['endpoint']
        for row in worker['rows']:
            if row['request']['operation'] != 'delete_object': continue
            validate_call({'row': row}, fixture, endpoint, readable=True)
            side = next(side for side in plan['sides'] if side['endpoint'] == endpoint)
            if side['lockClients'] < plan['exclusiveLockQuorum']: continue
            entry = fixture['sides'][endpoint]['versions'][0]
            require(row['request']['params']['VersionId'] == entry['versionId'], 'DL delete escaped its first exact historical ACK')
            target = map_target(before, fixture['bucket'], entry['key'], 'data', 1, entry['versionId'], latest_required=False)
            allowed = {(nodes[index], disk) for index in side['members'] for disk in range(4)
                       if disk not in plan['gates'].get(nodes[index], [])}
            scopes[entry['key']] = {'target': target, 'allowed': allowed, 'changed': set(), 'lost': set(), 'transactions': {}}
    for node in before:
        require(before[node]['formats'] == after[node]['formats'], 'DL refused mutation changed an original disk format')
    def scope_for(identity):
        node, disk, path = identity
        for key, scope in scopes.items():
            prefix = fixture['bucket'] + '/' + key + '/'
            if path.startswith(prefix) and (node, disk) in scope['allowed']:
                return scope, path[len(prefix):]
        return None, None
    for identity, original in left.items():
        actual = right.get(identity)
        if actual == original: continue
        scope, relative = scope_for(identity)
        require(scope is not None, 'DL refused mutation changed an unrelated original version, DB control or damaged DV target')
        slot = identity[:2];target = scope['target']
        if relative == 'xl.meta':
            require(actual is not None and original.get('metadata') and actual.get('metadata'), 'DL delete lost all original control metadata')
            old, new = original['metadata'], actual['metadata']
            reduced = {**old, 'versions': [version for version in old['versions']
                if public_version_id(version.get('versionId')) != target['versionId']]}
            require(new == old or new == reduced, 'DL delete changed another original generation or introduced metadata/version content')
            if new == reduced: scope['lost'].add(slot)
        else:
            require(relative.startswith(target['dataDir'] + '/') and actual is None,
                    'DL delete changed bytes or removed a different acknowledged data directory')
            scope['lost'].add(slot)
        scope['changed'].add(slot)
    for identity in set(right) - set(left):
        scope, relative = scope_for(identity)
        require(scope is not None, 'DL refused mutation created an unbound business file')
        parts = relative.split('/');target = scope['target'];slot = identity[:2]
        require(len(parts) in {2, 3} and str(uuid.UUID(parts[0])) == parts[0]
                and parts[0] not in {version['dataDir'] for version in left[(identity[0], identity[1],
                    fixture['bucket'] + '/' + target['key'] + '/xl.meta')]['metadata']['versions']},
                'DL refused delete residue is not a distinct source transaction directory')
        transaction = scope['transactions'].setdefault(parts[0], {})
        transaction.setdefault(slot, set()).add('/'.join(parts[1:]))
        if len(parts) == 2:
            require(parts[1] in {'xl.meta.bkp', 'delete-prepared', 'delete-target', 'delete-commit-finished'},
                    'DL delete residue contains a foreign transaction file')
            if parts[1] == 'xl.meta.bkp':
                original = left[(identity[0], identity[1], fixture['bucket'] + '/' + target['key'] + '/xl.meta')]
                require(all(right[identity][name] == original[name] for name in ('size', 'sha256')),
                        'DL transaction backup is not the exact original metadata bytes')
        else:
            require(parts[1] == target['dataDir'], 'DL transaction staged a different acknowledged generation')
            original = left.get((identity[0], identity[1], fixture['bucket'] + '/' + target['key'] + '/' + '/'.join(parts[1:])))
            require(original is not None and all(right[identity][name] == original[name] for name in ('size', 'sha256')),
                    'DL transaction staged bytes differ from the exact original file')
        scope['changed'].add(slot)
    require(set(delete_records) == set(before), 'DL delete record census omitted an original peer')
    prepared = {}
    control_keys = sorted(side['controlKey'] for side in fixture['sides'].values())
    for node, census in delete_records.items():
        require(census.get('readOnly') is True and census.get('quiescent') is True
                and census.get('runId') == fixture['runId'] and census.get('bucket') == fixture['bucket']
                and census.get('binarySha256') == before[node]['binarySha256'] and census.get('keys') == control_keys
                and census.get('credentialContentsRead') is False,
                'DL prepared delete records lack original stopped candidate/control ownership')
        for row in census['records']:
            identity = (node, row['disk'], row['path'])
            scope, relative = scope_for(identity)
            require(identity not in prepared and scope is not None and identity in right and identity not in left,
                    'DL prepared delete record is duplicated, pre-existing or unrelated to the original invocation')
            target = scope['target'];parts = relative.split('/')
            original = left[(node, row['disk'], fixture['bucket'] + '/' + target['key'] + '/xl.meta')]
            versions = original['metadata']['versions']
            require(len(parts) == 2 and parts[1] == 'delete-prepared' and row['transaction'] == parts[0]
                    and row.get('protocolVersion') == 1 and row['key'] == target['key']
                    and all(row[name] == right[identity][name] for name in ('size', 'sha256'))
                    and row['requestVersion'] == target['versionId'] and row['deletedVersions'] == [target['versionId']]
                    and row['dataDirs'] == [target['dataDir']] and row['originalMetadataSha256'] == original['sha256']
                    and len(row['originalVersions']) == len(row['originalDataDirs']) == 3
                    and set(zip(row['originalVersions'], row['originalDataDirs']))
                        == {(public_version_id(version.get('versionId')), version['dataDir']) for version in versions}
                    and isinstance(row.get('bucketIncarnation'), str) and row['bucketIncarnation'],
                    'DL prepared delete journal does not bind its raw bytes to the exact original generation/backup')
            target_identity = (node, row['disk'], fixture['bucket'] + '/' + target['key'] + '/' + parts[0] + '/delete-target')
            require(target_identity in right and row['targetMetadataSha256'] == right[target_identity]['sha256'],
                    'DL prepared delete target bytes differ from the original journal digest')
            prepared[identity] = row
    require(set(prepared) == {identity for identity in set(right) - set(left) if identity[2].endswith('/delete-prepared')},
            'DL original prepared delete file lacks an independent stopped raw-byte record')
    policies = {}
    for key, scope in scopes.items():
        require(len(scope['transactions']) <= 1, 'one original DL delete cannot explain multiple transaction IDs')
        complete_backups = {slot for transaction, locations in scope['transactions'].items() for slot, names in locations.items()
                            if {'xl.meta.bkp', 'delete-prepared', 'delete-target'} <= names
                            and (*slot, fixture['bucket'] + '/' + key + '/' + transaction + '/delete-prepared') in prepared}
        # A staged journal is only a recovery obligation. The subsequent product
        # task and final physical/payload proofs must redeem the original ACKs.
        missing = scope['lost']
        policies[key] = {'versionId': scope['target']['versionId'],
            'requiredRepair': bool(missing - complete_backups),
            'productSettlementAllowed': bool(missing) and missing <= complete_backups,
            'changedOriginalSlots': sorted(scope['changed']),
            'originalBucketIncarnations': sorted({row['bucketIncarnation'] for identity, row in prepared.items()
                                                 if row['key'] == key}),
            'originalTransactionIds': sorted(scope['transactions']), 'finalPhysicalProofRequired': True}
        for slot in scope['changed']:
            member = next(member for member in scope['target']['cohort'] if (member['node'], member['disk']) == slot)
            targets.append({**scope['target'], **member})
    return {'policies': policies, 'repairTargets': targets, 'refusalProvesRollback': False,
            'originalFailureSceneRetained': True, 'canonicalCasePassClaimed': False}


def recover_controls(probe, fleet, binding, fixture, damage, active):
    result = {}
    for side in fixture['sides'].values():
        expected, key = side['versions'], side['controlKey']
        policy = damage['policies'].get(key, {})
        status = run_task(probe, fleet, {'bucket': fixture['bucket'], 'key': key}, 2, active,
            option_overrides={'recursive': True}, status_endpoints=tuple(binding['endpoints'])[1:])
        pair = (key, expected[0]['versionId'])
        required = {pair} if policy.get('requiredRepair') else set()
        settlement = policy.get('productSettlementAllowed')
        if settlement:
            for row in status.get('outcome', {}).get('objects', []):
                identity = row.get('identity', {})
                if (identity.get('object'), public_version_id(identity.get('versionId'))) == pair:
                    require(row.get('disposition', {}).get('state') in {'repaired', 'verified_healthy'},
                            'DL original delete generation remains unresolved after product settlement')
                    if row['disposition']['state'] == 'repaired': required.add(pair)
        proof = validate_version_receipts(status, fixture['bucket'], expected, 2, required_repairs=required)
        if policy.get('originalBucketIncarnations'):
            require(policy['originalBucketIncarnations'] == [proof['bucketIncarnationId']],
                    'DL recovered task responsibility differs from the original delete journal incarnation')
        result[key] = {**proof, 'originalDeletePolicy': copy.deepcopy(policy),
            'productSettlementMayPrecedeShardCheck': bool(settlement), 'finalPhysicalProofRequired': True}
    return result
