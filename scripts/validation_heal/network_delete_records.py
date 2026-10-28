"""Read only the original stopped DL controls' bounded delete-prepared records."""

import hashlib
import json
import os
import re
import stat
import uuid


FIELDS = {'protocol_version', 'volume', 'object', 'transaction', 'bucket_incarnation', 'original_digest',
          'target_digest', 'data_dirs', 'deleted_versions', 'requests', 'original_versions', 'predecessors'}


def require(value, message):
    if not value: raise ValueError(message)


def stable(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def canonical_uuid(value):
    require(isinstance(value, str) and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0,
            'DL delete record has a missing/noncanonical generation or transaction')
    return value


def checksum(value):
    require(isinstance(value, list) and len(value) == 32 and all(type(x) is int and 0 <= x <= 255 for x in value),
            'DL delete record digest is not an exact SHA-256 byte array')
    return bytes(value).hex()


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'DL prepared delete contains duplicate JSON fields')
        result[key] = value
    return result


def run(request, node):
    require(set(request) == {'runId', 'action', 'bucket', 'keys', 'expectedBinarySha256'}
            and request['action'] == 'network-delete-records', 'DL delete census accepts only its exact owned controls')
    state, roots, owner = node['owned'](request['runId'])
    require(owner.get('runtimeProfile') == 'lock-quorum' and owner.get('bucket') == request['bucket']
            and owner.get('binarySha256') == request['expectedBinarySha256'] and len(roots) == 4
            and node['digest'](node['checked'](state / 'rustfs', state)) == owner['binarySha256'],
            'DL delete census candidate/runtime/bucket differs')
    node['stopped'](owner)
    keys = request['keys']
    require(isinstance(keys, list) and len(keys) == len(set(keys)) == 2 and keys == sorted(keys)
            and keys[0] == 'dl/node1/control.bin' and keys[1] in {'dl/node3/control.bin', 'dl/node4/control.bin'},
            'DL delete census escaped its original two control keys')
    records = []
    for disk, root in enumerate(roots):
        data = node['checked'](root / 'data', root)
        for key in keys:
            folder = node['checked'](data / request['bucket'] / key, data)
            if not folder.exists(): continue
            children = sorted(folder.iterdir())
            require(len(children) <= 64, 'DL control directory census exceeds its source fixture bound')
            for child in children:
                child = node['checked'](child, data)
                if not stat.S_ISDIR(child.lstat().st_mode): continue
                # Data directories without a prepared delete are not read.
                path = node['checked'](child / 'delete-prepared', data)
                if not path.exists(): continue
                canonical_uuid(child.name)
                before = path.lstat()
                require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= 1024**2
                        and len(records) < 32, 'DL prepared delete has an unsafe file type or excessive size/count')
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    require(stable(os.fstat(fd)) == stable(before), 'DL prepared delete changed before reading')
                    raw = bytearray()
                    while chunk := os.read(fd, min(65536, before.st_size + 1 - len(raw))):
                        raw.extend(chunk)
                        require(len(raw) <= before.st_size, 'DL prepared delete grew while reading')
                    require(len(raw) == before.st_size and stable(os.fstat(fd)) == stable(before),
                            'DL prepared delete changed while reading')
                finally:
                    os.close(fd)
                require(stable(path.lstat()) == stable(before), 'DL prepared delete changed after reading')
                value = json.loads(raw, object_pairs_hook=unique_fields)
                require(isinstance(value, dict) and set(value) == FIELDS and type(value['protocol_version']) is int
                        and value['protocol_version'] == 1 and value['volume'] == request['bucket']
                        and value['object'] == key and value['transaction'] == child.name
                        and isinstance(value['requests'], list) and len(value['requests']) == 1
                        and len(value['original_versions']) == 3 and value['predecessors'] == [],
                        'DL prepared delete escaped the reviewed one-request three-version protocol')
                request_row = value['requests'][0]
                require(request_row.get('name') == key and request_row.get('deleted') is False
                        and request_row.get('mark_deleted') is False
                        and len(value['data_dirs']) == len(value['deleted_versions']) == 1
                        and all(row.get('deleted') is False for row in value['original_versions']),
                        'DL prepared delete is not an exact ordinary historical-version delete')
                records.append({'disk': disk, 'key': key, 'path': path.relative_to(data).as_posix(),
                    'size': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'protocolVersion': 1,
                    'transaction': child.name, 'bucketIncarnation': canonical_uuid(value['bucket_incarnation']),
                    'originalMetadataSha256': checksum(value['original_digest']),
                    'targetMetadataSha256': checksum(value['target_digest']),
                    'dataDirs': [canonical_uuid(x) for x in value['data_dirs']],
                    'deletedVersions': [canonical_uuid(x) for x in value['deleted_versions']],
                    'requestVersion': canonical_uuid(request_row.get('version_id')),
                    'originalVersions': [canonical_uuid(x.get('version_id')) for x in value['original_versions']],
                    'originalDataDirs': [canonical_uuid(x.get('data_dir')) for x in value['original_versions']]})
    again = node['owned'](request['runId']);node['stopped'](again[2])
    require(again == (state, roots, owner), 'DL delete census original owner/roots changed')
    return {'runId': request['runId'], 'bucket': request['bucket'], 'keys': keys,
        'binarySha256': owner['binarySha256'], 'quiescent': True, 'readOnly': True, 'records': records,
        'rawContentsExported': False, 'credentialContentsRead': False, 'commitOrRollbackClaimed': False}
