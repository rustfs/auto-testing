"""C07: preserve two exact stopped object generations and restore only the last ACK.

All operations are relative to the original run-owned root's descriptors.
Archive copies and recovery are fixture operations, never product repair.
"""

import json
import os
import re
import stat
import uuid


def require(value, message):
    if not value:
        raise ValueError(message)


def scope(owner, request):
    bucket, key = request['bucket'], request['key']
    require(isinstance(bucket, str) and re.fullmatch(r'rv-heal-[0-9a-f]{16}(?:-dv)?', bucket)
            and (bucket == owner['bucket'] or bucket == owner['bucket'] + '-dv')
            and key == 'conflict.bin', 'generation fixture escaped its exact owned bucket/key')
    require(owner.get('runtimeProfile') == 'authority-split', 'generation replay requires the registered C07 runtime')
    return [bucket, key]


def tree(fd, fault):
    """Read the complete small object tree; reject aliases or unexpected leaves."""
    values = {}
    names = sorted(os.listdir(fd))
    require('xl.meta' in names and 1 < len(names) <= 17, 'generation lacks bounded external metadata/data directories')
    for name in names:
        if name == 'xl.meta':
            values[name] = fault.read_file(fd, name)
            continue
        require(str(uuid.UUID(name)) == name and uuid.UUID(name).int != 0, 'generation contains an unreviewed object child')
        with fault.parent(fd, [name]) as part_fd:
            parts = sorted(os.listdir(part_fd))
            require(parts and len(parts) <= 32, 'generation exceeds its part/commitment file budget')
            payloads, commitments = set(), set()
            for part in parts:
                match = re.fullmatch(r'part\.([1-9][0-9]*)(?:\.integrity\.([0-9a-f-]{36}))?', part)
                require(match is not None, 'generation contains unreviewed multipart leaves')
                number, nonce = match.groups()
                if nonce is None:
                    payloads.add(number)
                else:
                    require(str(uuid.UUID(nonce)) == nonce and uuid.UUID(nonce).int != 0 and number not in commitments,
                            'generation has ambiguous or noncanonical shard commitment identity')
                    commitments.add(number)
                values[name + '/' + part] = fault.read_file(part_fd, part)
            require(0 < len(payloads) <= 16 and commitments <= payloads,
                    'generation commitment has no exact corresponding original part')
    require(sum(map(len, values.values())) <= 32 * 1024**2, 'generation exceeds its per-drive byte budget')
    manifest = [{'path': name, 'size': len(raw), 'sha256': fault.digest(raw)} for name, raw in sorted(values.items())]
    return values, manifest


def tree_sha(manifest, fault):
    return fault.digest(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode())


def copy_into(fd, values, fault):
    require(not os.listdir(fd), 'exclusive generation copy target is not empty')
    for name, raw in sorted(values.items()):
        parts = name.split('/')
        if len(parts) == 1:
            fault.new_file(fd, name, raw)
        else:
            require(len(parts) == 2, 'generation copy depth changed')
            if parts[0] not in os.listdir(fd):
                os.mkdir(parts[0], 0o700, dir_fd=fd)
                os.fsync(fd)
            with fault.parent(fd, parts[:1]) as child:
                fault.new_file(child, parts[1], raw)
    os.fsync(fd)


def archive(root_fd, label, fault):
    return fault.parent(root_fd, ['quarantine', 'C07-' + label])


def capture(root_fd, owner, request, fault):
    parts = scope(owner, request)
    label = request['generation']
    require(label in {'a', 'b'}, 'unregistered generation archive')
    with fault.parent(root_fd, ['data', *parts]) as original:
        values, manifest = tree(original, fault)
    require(manifest == request['expectedFiles'], 'capture differs from the frozen full-drive object tree')
    record = {'schemaVersion': 1, 'runId': owner['runId'], 'bucket': parts[0], 'key': parts[1],
        'disk': request['disk'], 'generation': label, 'mountId': fault.mount_id(root_fd),
        'device': os.fstat(root_fd).st_dev, 'files': manifest, 'treeSha256': tree_sha(manifest, fault),
        'backupRestorationIsProductRepair': False}
    with fault.parent(root_fd, ['quarantine']) as quarantine:
        os.mkdir('C07-' + label, 0o700, dir_fd=quarantine)
        os.fsync(quarantine)
    with archive(root_fd, label, fault) as saved:
        fault.new_file(saved, 'manifest.json', fault.encode(record))
        os.mkdir('object', 0o700, dir_fd=saved)
        with fault.parent(saved, ['object']) as destination:
            copy_into(destination, values, fault)
            require(tree(destination, fault)[1] == manifest, 'archived generation is incomplete')
        fault.new_file(saved, 'complete.json', fault.encode(record))
    return record


def captured(root_fd, owner, request, label, fault):
    parts = scope(owner, request)
    with archive(root_fd, label, fault) as saved:
        record = json.loads(fault.read_file(saved, 'complete.json', 65536))
        require(record == json.loads(fault.read_file(saved, 'manifest.json', 65536))
                and record['schemaVersion'] == 1 and record['runId'] == owner['runId']
                and (record['bucket'], record['key'], record['disk'], record['generation'])
                == (parts[0], parts[1], request['disk'], label)
                and record['mountId'] == fault.mount_id(root_fd) and record['device'] == os.fstat(root_fd).st_dev,
                'archived generation ownership or original device drifted')
        with fault.parent(saved, ['object']) as original:
            values, manifest = tree(original, fault)
        require(manifest == record['files'] and tree_sha(manifest, fault) == record['treeSha256'],
                'archived generation bytes differ from the captured authority')
    return values, record


def install(root_fd, owner, request, fault):
    parts = scope(owner, request)
    old_values, old = captured(root_fd, owner, request, 'a', fault)
    _, current = captured(root_fd, owner, request, 'b', fault)
    require(old['treeSha256'] != current['treeSha256']
            and request['expectedSceneSha256'] == current['treeSha256'], 'generation fault has no distinct frozen last ACK')
    with fault.parent(root_fd, ['data', *parts]) as live:
        require(tree(live, fault)[1] == current['files'], 'last acknowledged object drifted before generation replay')
    intent = {'schemaVersion': 1, 'runId': owner['runId'], 'disk': request['disk'], 'bucket': parts[0], 'key': parts[1],
        'mountId': fault.mount_id(root_fd), 'device': os.fstat(root_fd).st_dev,
        'originalTreeSha256': current['treeSha256'], 'injectedTreeSha256': old['treeSha256'],
        'restoreGeneration': 'b', 'backupRestorationIsProductRepair': False}
    with fault.parent(root_fd, ['quarantine']) as quarantine:
        require('C07-pre-split' not in os.listdir(quarantine), 'generation replay would replace a retained scene')
        fault.new_file(quarantine, 'C07-replay-intent.json', fault.encode(intent))
        with fault.parent(root_fd, ['data', parts[0]]) as bucket:
            os.rename(parts[1], 'C07-pre-split', src_dir_fd=bucket, dst_dir_fd=quarantine)
            os.fsync(bucket)
            os.fsync(quarantine)
            os.mkdir(parts[1], 0o700, dir_fd=bucket)
            with fault.parent(bucket, [parts[1]]) as destination:
                copy_into(destination, old_values, fault)
                require(tree(destination, fault)[1] == old['files'], 'generation replay did not install every exact archived byte')
            os.fsync(bucket)
        fault.new_file(quarantine, 'C07-replay-applied.json', fault.encode(intent))
    return {**intent, 'state': 'APPLIED', 'oldAndNewArchivesPreserved': True}


def recover(root_fd, owner, request, fault):
    parts = scope(owner, request)
    values, original = captured(root_fd, owner, request, 'b', fault)
    with fault.parent(root_fd, ['quarantine']) as quarantine:
        intent = json.loads(fault.read_file(quarantine, 'C07-replay-intent.json', 65536))
        require(intent['schemaVersion'] == 1 and intent['runId'] == owner['runId']
                and (intent['bucket'], intent['key'], intent['disk']) == (parts[0], parts[1], request['disk'])
                and intent['restoreGeneration'] == 'b' and intent['originalTreeSha256'] == original['treeSha256']
                and intent['mountId'] == fault.mount_id(root_fd) and intent['device'] == os.fstat(root_fd).st_dev,
                'recovery cannot establish original run/device/last-ACK ownership')
        with fault.parent(root_fd, ['data', parts[0]]) as bucket:
            if 'C07-failed-current' in os.listdir(quarantine):
                # A completed rename can precede a lost SSH receipt. Confirm
                # exact restoration; partial recovery remains DIRTY.
                with fault.parent(bucket, [parts[1]]) as live:
                    require(tree(live, fault)[1] == original['files'], 'partial prior recovery cannot be discarded')
            else:
                if parts[1] in os.listdir(bucket):
                    observed = os.stat(parts[1], dir_fd=bucket, follow_symlinks=False)
                    require(stat.S_ISDIR(observed.st_mode) and observed.st_uid == 0
                            and observed.st_dev == os.fstat(root_fd).st_dev, 'fault scene no longer owns its original directory')
                    os.rename(parts[1], 'C07-failed-current', src_dir_fd=bucket, dst_dir_fd=quarantine)
                else:
                    os.mkdir('C07-failed-current', 0o700, dir_fd=quarantine)
                os.fsync(quarantine)
                os.fsync(bucket)
                os.mkdir(parts[1], 0o700, dir_fd=bucket)
                with fault.parent(bucket, [parts[1]]) as live:
                    copy_into(live, values, fault)
                    require(tree(live, fault)[1] == original['files'], 'recovery did not restore every last-ACK byte')
            os.fsync(bucket)
        receipt = {**intent, 'state': 'CLEAR', 'originalLastACKGenerationRestored': True,
                   'failedScenePreserved': True, 'productRepairClaimed': False}
        if 'C07-replay-recovered.json' not in os.listdir(quarantine):
            fault.new_file(quarantine, 'C07-replay-recovered.json', fault.encode(receipt))
    return receipt
