"""ACK-selected G05 staging census and continuity; not a Heal/EC proof."""
import base64
import hashlib
import os
import re
import stat
import uuid

KEY = 'objects-0256.bin'
ROLES = ('committed', 'a', 'b')
MAX_ENTRIES = 128
MAX_BYTES = 256 * 1024**2


def require(value, message):
    if not value:
        raise ValueError(message)


def selected(bucket, items):
    """Retain the frozen H1 upload-ID decoder, bound to the auto-test owner."""
    require(re.fullmatch(r'rv-heal-[0-9a-f]{16}', bucket) and isinstance(items, list) and len(items) == 3,
            'G05 requires one owned bucket and all three ACK-selected uploads')
    out, roles, uploads = [], set(), set()
    for item in items:
        require(set(item) == {'role', 'uploadId', 'bucket', 'key'} and item['role'] in ROLES
                and item['role'] not in roles and item['bucket'] == bucket and item['key'] == KEY,
                'foreign or duplicate G05 upload selection')
        encoded = item['uploadId']
        require(isinstance(encoded, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,256}', encoded)
                and encoded not in uploads, 'invalid or reused G05 upload identity')
        raw = base64.b64decode(encoded + '=' * ((-len(encoded)) % 4), altchars=b'-_', validate=True)
        require(base64.urlsafe_b64encode(raw).decode().rstrip('=') == encoded, 'noncanonical upload encoding')
        deployment, suffix = raw.decode().split('.', 1)
        require(str(uuid.UUID(deployment)) == deployment and uuid.UUID(deployment).int != 0,
                'invalid upload deployment UUID')
        match = re.fullmatch(r'([0-9a-f-]{36})x([0-9]{1,30})', suffix)
        require(match is not None and str(uuid.UUID(match[1])) == match[1] and uuid.UUID(match[1]).int != 0,
                'invalid upload suffix')
        key_hash = hashlib.sha256((bucket + '/' + KEY).encode()).hexdigest()
        out.append({**item, 'relative': '.rustfs.sys/multipart/' + key_hash + '/' + suffix})
        roles.add(item['role']); uploads.add(encoded)
    require(roles == set(ROLES), 'G05 upload selection omitted an original role')
    return out


def stable(info):
    return (info.st_mode, info.st_uid, info.st_gid, info.st_dev, info.st_ino, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def run(request, node):
    require(set(request) == {'runId', 'action', 'bucket', 'items', 'expectedBinarySha256', 'quiescent'}
            and request['action'] == 'mpu-race-scene' and type(request['quiescent']) is bool,
            'G05 census accepts an exact owned read-only upload scope')
    state, roots, owner = node['owned'](request['runId'])
    require(owner.get('runtimeProfile') == 'manual' and owner.get('bucket') == request['bucket']
            and owner.get('binarySha256') == request['expectedBinarySha256']
            and re.fullmatch(r'[0-9a-f]{64}', request['expectedBinarySha256']) and len(roots) == 4,
            'G05 census owner/runtime/binary differs')
    items = selected(request['bucket'], request['items'])
    process = None
    if request['quiescent']:
        node['stopped'](owner)
    else:
        process = node['process'](request)
        require(process['pid'] > 1 and process['binarySha256'] == owner['binarySha256'],
                'live G05 staging census lacks its original owned process')
    require(node['digest'](node['checked'](state / 'rustfs', state)) == owner['binarySha256'],
            'G05 census candidate changed')
    rows, prefixes, total, count = [], [], 0, 0
    for disk, root in enumerate(roots):
        data = node['checked'](root / 'data', root)
        device = data.lstat().st_dev
        parent = node['checked']((data / items[0]['relative']).parent, data)
        parent_before = parent.lstat() if parent.exists() else None
        children = []
        if parent_before:
            for child in parent.iterdir():
                require(len(children) < len(items), 'G05 key prefix exceeds its three original upload identities')
                children.append(child.name)
            children.sort()
        require(set(children) <= {item['relative'].rsplit('/', 1)[-1] for item in items},
                'G05 key prefix contains an unacknowledged upload directory')
        prefixes.append({'disk': disk, 'path': parent.relative_to(data).as_posix(),
                         'present': parent_before is not None, 'children': children})
        for item in items:
            folder = node['checked'](data / item['relative'], data)
            files, directories = {}, []
            def visit(path, depth=0):
                nonlocal total, count
                path = node['checked'](path, data)
                before = path.lstat()
                count += 1
                require(count <= MAX_ENTRIES, 'G05 staging entry budget exceeded')
                require(depth <= 2 and before.st_dev == device and (stat.S_ISDIR(before.st_mode)
                        or stat.S_ISREG(before.st_mode) and before.st_nlink == 1),
                        'G05 staging contains links, mounts, special files or excessive depth')
                if stat.S_ISDIR(before.st_mode):
                    if path != folder: directories.append(path.relative_to(folder).as_posix())
                    children = []
                    for child in path.iterdir():
                        require(len(children) < MAX_ENTRIES, 'G05 staging directory population exceeds its entry budget')
                        children.append(child)
                    for child in sorted(children): visit(child, depth + 1)
                else:
                    require(before.st_size <= 64 * 1024**2
                            and total + before.st_size <= MAX_BYTES, 'G05 staging file/byte budget exceeded')
                    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                    try:
                        require(stable(os.fstat(fd)) == stable(before), 'G05 file changed before hashing')
                        digest, size = hashlib.sha256(), 0
                        while chunk := os.read(fd, 1024**2):
                            digest.update(chunk); size += len(chunk)
                            require(size <= before.st_size, 'G05 staging file grew while hashing')
                        require(size == before.st_size and stable(os.fstat(fd)) == stable(before),
                                'G05 staging changed while hashing')
                    finally:
                        os.close(fd)
                    files[path.relative_to(folder).as_posix()] = {'size': size, 'sha256': digest.hexdigest()}
                    total += size
                require(stable(path.lstat()) == stable(before), 'G05 staging population changed during census')
            exists = folder.exists()
            if exists: visit(folder)
            rows.append({'disk': disk, 'role': item['role'], 'uploadId': item['uploadId'],
                         'relative': item['relative'], 'exists': exists, 'files': files, 'directories': directories})
        parent_after = parent.lstat() if parent.exists() else None
        require((parent_before is None and parent_after is None) or
                (parent_before is not None and parent_after is not None and stable(parent_before) == stable(parent_after)),
                'G05 key prefix changed during staging census')
    require(node['owned'](request['runId']) == (state, roots, owner), 'G05 owner or roots changed during census')
    require(node['digest'](node['checked'](state / 'rustfs', state)) == owner['binarySha256'],
            'G05 candidate changed during staging census')
    if request['quiescent']:
        node['stopped'](owner)
    else:
        after = node['process'](request)
        require(all(after.get(k) == process.get(k) for k in node['PROCESS_FIELDS']),
                'G05 owned process changed during live staging census')
    return {'rows': rows, 'keyPrefixes': prefixes, 'bytes': total, 'binarySha256': owner['binarySha256'], 'quiescent': request['quiescent'],
            'process': process, 'readOnly': True, 'rawContentsExported': False,
            'physicalRepairProved': False, 'codingLayoutProved': False, 'overlapProved': False}


def verify(manifest, evidence, nodes, binary_sha, *, previous=None, committed=None):
    """Generalize the frozen four-disk continuity oracle to each H1/H2 host."""
    require(len(nodes) in (1, 4) and len(set(nodes)) == len(nodes) and set(evidence) == set(nodes),
            'G05 staging proof omitted or duplicated an original host')
    items = [{'role': role, 'uploadId': item['uploadId'], 'bucket': manifest['bucket'], 'key': manifest['key']}
             for role, item in manifest['items'].items()]
    requests = {item['role']: item for item in selected(manifest['bucket'], items)}
    old = {} if previous is None else {(node, row['role'], row['disk']): row
          for node, value in previous.items() for row in value['rows']}
    if previous is not None:
        require(set(previous) == set(nodes) and len(old) == 12 * len(nodes)
                and all(value.get('readOnly') is True and value.get('binarySha256') == binary_sha
                        and len(value['rows']) == 12 for value in previous.values()),
                'G05 previous staging proof has incomplete ownership/candidate coverage')
    live = {} if committed is None else {(node, row['disk'], row['path']): row
           for node, value in committed.items() for row in value['files']}
    data_dirs, completed, witnesses = {role: set() for role in ROLES}, 0, 0
    for node, value in evidence.items():
        rows = value['rows']; expected = {(role, disk) for role in ROLES for disk in range(4)}
        require(value.get('readOnly') is True and value.get('binarySha256') == binary_sha
                and type(value.get('quiescent')) is bool and len(rows) == len(expected)
                and all(type(row['disk']) is int and row['role'] in ROLES for row in rows)
                and {(row['role'], row['disk']) for row in rows} == expected,
                'G05 staging proof omitted or duplicated a drive/role')
        prefixes = value.get('keyPrefixes', [])
        require(len(prefixes) == 4 and all(type(p['disk']) is int for p in prefixes)
                and {p['disk'] for p in prefixes} == set(range(4)),
                'G05 staging proof omitted or duplicated a complete key prefix')
        expected_children = sorted(item['relative'].rsplit('/', 1)[-1] for role, item in requests.items()
                                   if manifest['items'][role]['state'] == 'pending')
        for prefix in prefixes:
            require(prefix['path'] == requests['a']['relative'].rsplit('/', 1)[0]
                    and type(prefix['present']) is bool and prefix['children'] == expected_children
                    and (prefix['present'] or not expected_children),
                    'G05 complete key-prefix population differs from acknowledged pending uploads')
        for row in rows:
            item, identity = manifest['items'][row['role']], requests[row['role']]
            require(row['relative'] == identity['relative'] and row['uploadId'] == identity['uploadId'],
                    'G05 staging identity differs from its original ACK')
            require(all(type(v['size']) is int and v['size'] >= 0 and re.fullmatch(r'[0-9a-f]{64}', v['sha256'])
                        for v in row['files'].values()), 'G05 staging digest/size is invalid')
            prior = old.get((node, row['role'], row['disk']))
            if prior:
                require(prior['uploadId'] == identity['uploadId'] and prior['relative'] == identity['relative'],
                        'G05 previous staging identity belongs to another upload')
            if item['state'] == 'pending':
                require(row['exists'] is True and row['files'].get('xl.meta', {}).get('size', 0) > 0,
                        'G05 pending upload lost its metadata')
                parts, dirs = {}, set()
                for path, part in row['files'].items():
                    match = re.fullmatch(r'([0-9a-f-]{36})/part\.([1-3])', path)
                    if match:
                        directory, number = match[1], int(match[2])
                        require(str(uuid.UUID(directory)) == directory and uuid.UUID(directory).int and part['size'] > 32
                                and number not in parts, 'G05 pending part has an invalid/duplicate generation')
                        parts[number] = path; dirs.add(directory)
                require(set(parts) == {p['number'] for p in item['parts']} and len(dirs) == 1,
                        'G05 pending physical parts differ from acknowledged parts')
                require(set(row['files']) == {'xl.meta', *parts.values(), *(path + '.meta' for path in parts.values())}
                        and all(row['files'][path + '.meta']['size'] > 0 for path in parts.values())
                        and set(row['directories']) == dirs and len(row['directories']) == 1,
                        'G05 pending upload has an unknown or missing staging leaf/directory')
                data_dirs[row['role']].update(dirs)
                if prior and prior['exists']:
                    require(all(row['files'].get(path) == part for path, part in prior['files'].items() if path != 'xl.meta'),
                            'G05 continuation or sibling operation changed an acknowledged staging part')
                    if set(prior['files']) == set(row['files']):
                        require(prior['files'] == row['files'] and prior['directories'] == row['directories'],
                                'G05 unchanged sibling metadata or population changed')
            else:
                require(item['state'] in ('complete', 'aborted') and row['exists'] is False
                        and not row['files'] and not row['directories'], 'G05 terminal upload staging remains')
                if item['state'] == 'complete' and prior and prior['exists']:
                    paths = [p for p in prior['files'] if re.fullmatch(r'[0-9a-f-]{36}/part\.[1-3]', p)]
                    require(len(paths) == 3 and {p.rsplit('.', 1)[-1] for p in paths} == {'1', '2', '3'},
                            'G05 Complete lacks three prior physical pending parts')
                    for path in paths:
                        current = live.get((node, row['disk'], manifest['bucket'] + '/' + KEY + '/' + path), {})
                        require({k: current.get(k) for k in ('size', 'sha256')} == prior['files'][path],
                                'G05 completed part differs from original pending shard bytes')
                    completed += 1
            witnesses += 1
    require(all(len(dirs) <= 1 for dirs in data_dirs.values()), 'G05 pending dataDir differs across original hosts/drives')
    return {'pendingAndTerminalWitnesses': witnesses, 'completedMembersCompared': completed,
            'codingLayoutProved': False, 'physicalRepairProved': False, 'overlapProved': False,
            'canonicalCasePassClaimed': False}
