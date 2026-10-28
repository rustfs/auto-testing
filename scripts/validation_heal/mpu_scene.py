"""Read-only stopped I03/G05 multipart census, scoped by original bucket/key SHA-256."""

import hashlib
import json
import os
from pathlib import Path
import re
import stat


NAMES = ('abort', 'complete', 'create', 'upload')
G05_KEY = 'objects-0256.bin'
MAX_FILES = 4096
MAX_BYTES = 256 * 1024**2


def require(value, message):
    if not value:
        raise ValueError(message)


def keys_for(endpoints):
    require(len(endpoints) == len(set(endpoints)) == 2 and 'node1' in endpoints
            and set(endpoints) <= {'node1', 'node3', 'node4'}, 'I03 MPU scope requires its two actual partition sides')
    return sorted(f'dl/{endpoint}/{name}.bin' for endpoint in endpoints for name in NAMES)


def validate_scope(owner, keys):
    if owner.get('runtimeProfile') in {'manual', 'frozen-race-legacy'}:
        require(re.fullmatch(r'rv-heal-[0-9a-f]{16}', owner.get('bucket', ''))
                and keys == [G05_KEY], 'G05 MPU retention requires its one frozen same-key prefix')
        return 'the original G05 bucket/key SHA-256 prefix, including unknown upload IDs; not a bitrot/EC proof'
    require(owner.get('runtimeProfile') == 'lock-quorum' and isinstance(keys, list) and len(keys) == 8
            and all(isinstance(key, str) and re.fullmatch(r'dl/(node1|node3|node4)/(abort|complete|create|upload)\.bin', key)
                    for key in keys), 'MPU scene keys escaped the exact DL matrix')
    endpoints = sorted({key.split('/')[1] for key in keys})
    require(keys == keys_for(endpoints), 'MPU scene omitted or duplicated an original request key')
    return 'all files under the eight original DL bucket/key SHA-256 prefixes; not a bitrot/EC proof'


def stable(info):
    return (info.st_mode, info.st_uid, info.st_gid, info.st_dev, info.st_ino, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def run(request, node):
    require(set(request) == {'runId', 'action', 'bucket', 'keys', 'expectedBinarySha256'}
            and request['action'] == 'mpu-scene', 'MPU scene accepts only an exact owned read-only scope')
    state, roots, owner = node['owned'](request['runId'])
    require(owner.get('runtimeProfile') in {'lock-quorum', 'manual', 'frozen-race-legacy'} and owner.get('bucket') == request['bucket']
            and owner.get('binarySha256') == request['expectedBinarySha256']
            and re.fullmatch(r'[0-9a-f]{64}', request['expectedBinarySha256']) is not None and len(roots) == 4,
            'MPU scene candidate/runtime/primary bucket differs from the owned H2 fixture')
    node['stopped'](owner)
    require(node['digest'](node['checked'](state / 'rustfs', state)) == owner['binarySha256'],
            'MPU scene candidate binary changed')
    keys = request['keys']
    scope = validate_scope(owner, keys)
    files, directories, scopes, total = [], [], [], 0
    for disk, root in enumerate(roots):
        data = node['checked'](root / 'data', root)
        device = data.lstat().st_dev
        for key in keys:
            key_hash = hashlib.sha256(f"{request['bucket']}/{key}".encode()).hexdigest()
            folder = node['checked'](data / '.rustfs.sys' / 'multipart' / key_hash, data)
            scopes.append({'disk': disk, 'key': key, 'keySha256': key_hash, 'present': folder.exists()})
            if not folder.exists():
                continue
            def visit(path, depth=0):
                nonlocal total
                path = node['checked'](path, data)
                before = path.lstat()
                require(depth <= 8 and before.st_dev == device and (stat.S_ISDIR(before.st_mode)
                        or stat.S_ISREG(before.st_mode) and before.st_nlink == 1),
                        'MPU scene contains a link, mount, special file or excessive depth')
                relative = path.relative_to(data).as_posix()
                if stat.S_ISDIR(before.st_mode):
                    require(len(directories) < MAX_FILES, 'MPU scene directory budget exceeded')
                    directories.append({'disk': disk, 'path': relative, 'key': key})
                    children = []
                    for child in path.iterdir():
                        require(len(children) < (3 if depth == 0 and owner['runtimeProfile'] in {'manual', 'frozen-race-legacy'} else MAX_FILES),
                                'MPU scene directory population exceeded its bound')
                        children.append(child)
                    for child in sorted(children):
                        visit(child, depth + 1)
                else:
                    require(len(files) < MAX_FILES and before.st_size <= 64 * 1024**2
                            and total + before.st_size <= MAX_BYTES, 'MPU scene file/byte budget exceeded')
                    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                    try:
                        require(stable(os.fstat(fd)) == stable(before), 'MPU file changed before hashing')
                        checksum, count = hashlib.sha256(), 0
                        while chunk := os.read(fd, 1024**2):
                            checksum.update(chunk)
                            count += len(chunk)
                            require(count <= before.st_size, 'MPU file grew during hashing')
                        require(count == before.st_size and stable(os.fstat(fd)) == stable(before),
                                'MPU file changed during hashing')
                    finally:
                        os.close(fd)
                    files.append({'disk': disk, 'path': relative, 'key': key,
                                  'size': count, 'sha256': checksum.hexdigest()})
                    total += count
                require(stable(path.lstat()) == stable(before), 'MPU scene changed during its stopped census')
            visit(folder)
    again = node['owned'](request['runId'])
    node['stopped'](again[2])
    require(again == (state, roots, owner), 'MPU scene owner or original roots changed during census')
    value = {'bucket': request['bucket'], 'keys': keys, 'scopes': scopes,
             'directories': directories, 'files': files, 'bytesHashed': total}
    return {**value, 'runId': request['runId'], 'binarySha256': owner['binarySha256'],
        'quiescent': True, 'originalPhysicalDrives': 4,
        'sceneSha256': hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        'readOnly': True, 'rawContentsExported': False, 'payloadIntegrityClaimed': False,
        'scope': scope}
