"""Source-bound I03 DB fixtures; only public S3 writes and decoded physical witnesses."""

try:
    from .boundaries import boundary_sizes, verify_size_contract, verify_boundary_ranges
    from .heal_probe import require
    from .heal_workload import seed, verify_seed
    from .shards import map_target
    from .versions import map_inline_target, verify_proof
except ImportError:
    from boundaries import boundary_sizes, verify_size_contract, verify_boundary_ranges
    from heal_probe import require
    from heal_workload import seed, verify_seed
    from shards import map_target
    from versions import map_inline_target, verify_proof


DB_COUNT = 128
MPU_MINIMUM = 5 * 1024**2
# GLOBAL_MIN_PART_SIZE and is_min_allowed_part_size in these exact source files.
MPU_SOURCE = {
    '072b57b37559fbe626aeda8abf61617fdcbdd926': 'e60ff0ab886a5578512a123eb1757352ae3e5204d7b69b5aaf14e9e530ec2d31',
    '1c7fc818dd858247c45b39542abd8291b1e6e31b': '6975b05976e88667065dd4fd2d7649ff61ce0887504e2790bc68fc73c9ba048a',
    '85c706cac09d53d0649487b8f1865f18beb53225': '6975b05976e88667065dd4fd2d7649ff61ce0887504e2790bc68fc73c9ba048a',
}


def db_parameters(manifest, candidate, calibration, processes):
    contract = verify_size_contract(manifest, candidate)
    require(candidate['source_sha'] in MPU_SOURCE
            and contract['sourceFiles']['crates/ecstore/src/set_disk/mod.rs'] == MPU_SOURCE[candidate['source_sha']],
            'network DB multipart boundary is not bound to the exact reviewed candidate source')
    parameters = boundary_sizes(contract, calibration, processes, standard_class='EC:8')
    parameters['groups']['multipart-minimum-minus-exact-plus'] = [MPU_MINIMUM - 1, MPU_MINIMUM, MPU_MINIMUM + 1]
    sizes = [size for group in parameters['groups'].values() for size in group]
    require(len(sizes) <= DB_COUNT and max(sizes) <= 64 * 1024**2, 'DB boundary plan exceeds its fixed count or body cap')
    return {**parameters, 'objectCount': DB_COUNT, 'sizeCycle': sizes,
        'payloadBytes': sum(sizes[index % len(sizes)] for index in range(DB_COUNT)),
        'multipartMinimumPartBytes': MPU_MINIMUM, 'multipartBoundarySourceSha256': MPU_SOURCE[candidate['source_sha']],
        'multipartBoundaryIsObjectSizeControl': True,
        'completedMultipartProtocolClaimed': False}


def seed_db(recorder, bucket, parameters):
    require(parameters['objectCount'] == DB_COUNT
            and parameters['payloadBytes'] <= recorder.probe.binding['maxInventoryBytes']
            and DB_COUNT <= recorder.probe.binding['maxInventoryObjects'],
            'network DB plan exceeds the frozen inventory budget')
    return seed(recorder, bucket, 'DB', DB_COUNT, parameters['sizeCycle'])


def map_db(snapshot, bucket, expected, parameters):
    require(len(expected) == DB_COUNT and len({entry['key'] for entry in expected}) == DB_COUNT,
            'DB boundary fixture omits or duplicates a control')
    representatives, witnessed, selected_sizes = [], [], set()
    for index, entry in enumerate(expected):
        size = parameters['sizeCycle'][index % len(parameters['sizeCycle'])]
        require(entry['key'] == f'objects/{index:04d}.bin' and entry['versionId'] == 'null'
                and entry['isLatest'] is True and entry['deleteMarker'] is False and entry['payload']['size'] == size,
                'DB control identity or ACK size differs from its frozen cycle')
        shape = 'zero-metadata' if size == 0 else 'inline' if size <= parameters['actualLogicalInlineLimit'] else 'external'
        target = (map_target(snapshot, bucket, entry['key'], 'data', 1) if shape == 'external'
                  else map_inline_target(snapshot, bucket, entry, allow_empty=size == 0))
        require(len(target['cohort']) == 16 and target['logical']['size'] == size
                and target['logical']['erasure']['data_blocks'] == target['logical']['erasure']['parity_blocks'] == 8,
                'DB boundary lacks its actual complete sixteen-member size/shape authority')
        witnessed.append({'key': entry['key'], 'size': size, 'shape': shape,
            'versionId': entry['versionId'], 'members': len(target['cohort']),
            'dataBlocks': target['logical']['erasure']['data_blocks'],
            'parityBlocks': target['logical']['erasure']['parity_blocks']})
        if size not in selected_sizes:
            selected_sizes.add(size)
            representatives.append({'expected': entry, 'target': target, 'shape': shape})
    require(selected_sizes == set(parameters['sizeCycle']), 'physical DB representatives omit a required boundary')
    return representatives, witnessed


def prove_db(fleet, representatives):
    return [{'key': row['expected']['key'], 'shape': row['shape'],
             'proof': verify_proof(fleet, row['target'], 'DB-boundary-physical-proof')}
            for row in representatives]


def verify_db(probe, fleet, bucket, expected, baseline, representatives, parameters):
    try:
        from .heal_probe import Probe, compare_inventories
    except ImportError:
        from heal_probe import Probe, compare_inventories
    peers = []
    for endpoint in probe.binding['endpoints']:
        peer = Probe(probe.binding, endpoint)
        inventory = fleet.record('all-peer-DB-final-S3', {'endpoint': endpoint,
            **peer.inventory(bucket, unversioned_current_tags=True)})
        peers.append({'endpoint': endpoint, **verify_seed(expected, inventory), **compare_inventories(baseline, inventory)})
    ranges = verify_boundary_ranges(probe, fleet, bucket,
        [row['expected'] for row in representatives], parameters['actualECBlockSize'])
    return {'allDefault128ControlsPreserved': True, 'peers': peers, 'boundaryRanges': ranges}
