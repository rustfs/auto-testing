"""Actual H5 pools/sets over sixteen original independent endpoints."""

try:
    from .heal_probe import require
except ImportError:
    from heal_probe import require


def volumes(run_id):
    require(run_id.endswith('-h5'), 'two-pool deployment requires an explicit H5 namespace')
    return [f'http://rustfs-node{{1...4}}:9000/data/rustfs{{{first}...{first + 1}}}/heal-validation/{run_id}/data'
            for first in (1, 3)]


def validate(snapshot):
    require(len(snapshot) == 4, 'H5 requires the four actual original hosts')
    rows = [(node, row) for node, value in snapshot.items() for row in value['formats']]
    require(len(rows) == 16 and len({(node, row['disk']) for node, row in rows}) == 16
            and all({row['disk'] for owner, row in rows if owner == node} == set(range(4)) for node in snapshot),
            'H5 omitted or duplicated an original independent drive')
    pools, all_ids = [], set()
    for pool_index in range(2):
        selected = [(node, row) for node, row in rows if row['disk'] // 2 == pool_index]
        value = selected[0][1]['value']
        sets = value['xl']['sets']
        identities = [identity for group in sets for identity in group]
        require(value['format'] == 'xl' and len(sets) == 2 and all(len(group) == 4 for group in sets)
                and len(identities) == len(set(identities)) == 8 and not all_ids.intersection(identities),
                'H5 needs two distinct pools, each with two complete four-member sets')
        require(all(row['value']['format'] == 'xl' and row['value']['id'] == value['id']
                    and row['value']['xl']['sets'] == sets for _, row in selected)
                and {row['value']['xl']['this'] for _, row in selected} == set(identities),
                'pool formats disagree or fail to cover their exact endpoint group')
        require(all(sum(row['value']['xl']['this'] in group for owner, row in selected if owner == node) == 1
                    for node in snapshot for group in sets), 'each H5 set must span all four actual hosts')
        all_ids.update(identities)
        pools.append({'poolIndex': pool_index, 'formatId': value['id'], 'actualSets': sets,
            'members': [{'node': node, 'disk': row['disk'], 'diskUUID': row['value']['xl']['this']} for node, row in selected]})
    require(set(pools[0]['actualSets'][0]).isdisjoint(set(pools[1]['actualSets'][0])), 'two pool selectors alias one physical set')
    return {'topology': 'H5', 'nodes': 4, 'independentDrives': 16, 'pools': pools,
            'setCountPerPool': 2, 'minimumAvailableSlotsAfterOnePeerLoss': 3,
            'codingMustBeMeasuredPerObject': True}
