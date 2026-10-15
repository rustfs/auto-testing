"""C10 stopped descriptor archives, shared conversion and post-delete recovery."""

import json
import os
import re
import stat
import subprocess
import uuid


def require(value, message):
    if not value:
        raise ValueError(message)


def scope(owner, request):
    require(owner.get('runtimeProfile') in {'orphan-preserve', 'orphan-before', 'orphan-after', 'outcome-grace'}
            and request.get('bucket') == owner['bucket'] and request.get('key') == 'orphan.bin',
            'C10 escaped its registered runtime or exact owned object')
    require(type(request.get('disk')) is int and 0 <= request['disk'] < 4, 'C10 original disk absent')
    return [owner['bucket'], 'orphan.bin']


def scene(root, owner, request, fault, generation):
    parts = scope(owner, request)
    with fault.parent(root, ['data', parts[0]]) as bucket:
        if parts[1] not in os.listdir(bucket):
            return False, {}, []
        with fault.parent(bucket, [parts[1]]) as obj:
            values, files = generation.tree(obj, fault, allow_sparse=True)
    return True, values, files


def identity(root, owner, request, fault):
    return {'schemaVersion': 1, 'runId': owner['runId'], 'disk': request['disk'],
        'bucket': owner['bucket'], 'key': 'orphan.bin', 'mountId': fault.mount_id(root),
        'device': os.fstat(root).st_dev, 'binarySha256': owner['binarySha256']}


def capture(root, owner, request, fault, generation):
    label = request['generation']
    require(label in {'seed', 'old', 'authority'}, 'unregistered C10 archive')
    present, values, files = scene(root, owner, request, fault, generation)
    require(files == request['expectedFiles'] and present == request['expectedPresent'],
            'C10 archive differs from its complete stopped physical witness')
    record = {**identity(root, owner, request, fault), 'generation': label, 'present': present,
        'files': files, 'treeSha256': generation.tree_sha(files, fault), 'productRepairClaimed': False}
    with fault.parent(root, ['quarantine']) as quarantine:
        os.mkdir('C10-' + label, 0o700, dir_fd=quarantine)
        os.fsync(quarantine)
        with fault.parent(quarantine, ['C10-' + label]) as saved:
            fault.new_file(saved, 'manifest.json', fault.encode(record))
            os.mkdir('object', 0o700, dir_fd=saved)
            with fault.parent(saved, ['object']) as obj:
                generation.copy_into(obj, values, fault)
                require(generation.tree(obj, fault, allow_sparse=True)[1] == files, 'C10 archive incomplete')
            fault.new_file(saved, 'complete.json', fault.encode(record))
    return record


def captured(root, owner, request, label, fault, generation):
    scope(owner, request)
    require(label in {'seed', 'old', 'authority'}, 'unregistered C10 saved generation')
    with fault.parent(root, ['quarantine', 'C10-' + label]) as saved:
        record = json.loads(fault.read_file(saved, 'complete.json', 65536))
        require(record == json.loads(fault.read_file(saved, 'manifest.json', 65536))
                and all(record.get(key) == value for key, value in identity(root, owner, request, fault).items())
                and record['generation'] == label and type(record['present']) is bool,
                'C10 archive candidate, ownership, device or mount changed')
        with fault.parent(saved, ['object']) as obj:
            values, files = generation.tree(obj, fault, allow_sparse=True)
        require(files == record['files'] and generation.tree_sha(files, fault) == record['treeSha256']
                and (record['present'] or not files), 'C10 archived bytes differ from authority')
    return values, record


def arm(root, owner, request, fault, generation):
    label = request['generation']
    require(label in {'seed', 'authority'}, 'C10 recovery cannot restore a deleted old generation')
    _, record = captured(root, owner, request, label, fault, generation)
    present, _, files = scene(root, owner, request, fault, generation)
    require(present == record['present'] and files == record['files'], 'C10 authority drifted before arming')
    intent = {**identity(root, owner, request, fault), 'restoreGeneration': label,
        'originalTreeSha256': record['treeSha256'], 'originalPresent': record['present'], 'productRepairClaimed': False}
    with fault.parent(root, ['quarantine']) as quarantine:
        fault.new_file(quarantine, 'C10-' + label + '-intent.json', fault.encode(intent))
    return {**intent, 'state': 'ARMED'}


def intent(root, owner, request, label, fault, generation):
    values, record = captured(root, owner, request, label, fault, generation)
    with fault.parent(root, ['quarantine']) as quarantine:
        value = json.loads(fault.read_file(quarantine, 'C10-' + label + '-intent.json', 65536))
        require(all(value.get(key) == item for key, item in identity(root, owner, request, fault).items())
                and value['restoreGeneration'] == label and value['originalTreeSha256'] == record['treeSha256']
                and value['originalPresent'] == record['present']
                and 'C10-' + label + '-disarmed.json' not in os.listdir(quarantine),
                'C10 recovery authority is absent, foreign or already retired by later ACKs')
    return values, record, value


def install(root, owner, request, fault, generation):
    parts = scope(owner, request)
    _, authority, _ = intent(root, owner, request, 'authority', fault, generation)
    old, _ = captured(root, owner, request, 'old', fault, generation)
    present, current, files = scene(root, owner, request, fault, generation)
    require(present == authority['present'] and files == authority['files'], 'C10 last DELETE authority drifted')
    directory = request['dataDir']
    require(str(uuid.UUID(directory)) == directory and uuid.UUID(directory).int != 0,
            'C10 stale payload has no exact canonical data directory')
    stale = {name: raw for name, raw in old.items() if name.startswith(directory + '/')}
    require(stale and not any(name.startswith(directory + '/') for name in current),
            'C10 replay would overwrite a live physical reference')
    mode = request['mode']
    require(mode in {'object', 'data'}, 'unregistered C10 replay mode')
    values = old if mode == 'object' else {**current, **stale}
    with fault.parent(root, ['quarantine']) as quarantine, fault.parent(root, ['data', parts[0]]) as bucket:
        fault.new_file(quarantine, 'C10-replay-intent.json', fault.encode({'mode': mode, 'dataDir': directory,
            'restoreGeneration': 'authority', 'postDeleteTreeSha256': authority['treeSha256']}))
        require('C10-pre-replay' not in os.listdir(quarantine), 'C10 retained scene already exists')
        if present:
            os.rename(parts[1], 'C10-pre-replay', src_dir_fd=bucket, dst_dir_fd=quarantine)
        else:
            os.mkdir('C10-pre-replay', 0o700, dir_fd=quarantine)
        os.fsync(quarantine); os.fsync(bucket)
        os.mkdir(parts[1], 0o700, dir_fd=bucket)
        with fault.parent(bucket, [parts[1]]) as obj:
            generation.copy_into(obj, values, fault)
            actual, manifest = generation.tree(obj, fault, allow_sparse=True)
            require(actual == values, 'C10 replay did not preserve all original sidecars and parts')
        fault.new_file(quarantine, 'C10-replay-applied.json', fault.encode({'mode': mode, 'dataDir': directory,
            'files': manifest, 'state': 'APPLIED', 'postDeleteAuthorityRetained': True}))
    return {'state': 'APPLIED', 'mode': mode, 'dataDir': directory, 'files': manifest}


def recover(root, owner, request, fault, generation):
    parts = scope(owner, request)
    label = request['generation']
    values, authority, original_intent = intent(root, owner, request, label, fault, generation)
    name = 'C10-' + label + '-failed-current'
    with fault.parent(root, ['quarantine']) as quarantine, fault.parent(root, ['data', parts[0]]) as bucket:
        if name in os.listdir(quarantine):
            present, _, files = scene(root, owner, request, fault, generation)
            require(present == authority['present'] and files == authority['files'],
                    'C10 interrupted recovery remains DIRTY; never discard a partial restore')
        else:
            if parts[1] in os.listdir(bucket):
                with fault.parent(bucket, [parts[1]]) as obj:
                    require(os.fstat(obj).st_uid == 0, 'C10 live scene no longer root owned')
                os.rename(parts[1], name, src_dir_fd=bucket, dst_dir_fd=quarantine)
            else:
                os.mkdir(name, 0o700, dir_fd=quarantine)
            os.fsync(bucket); os.fsync(quarantine)
            if authority['present']:
                os.mkdir(parts[1], 0o700, dir_fd=bucket)
                with fault.parent(bucket, [parts[1]]) as obj:
                    generation.copy_into(obj, values, fault)
            os.fsync(bucket)
        present, _, files = scene(root, owner, request, fault, generation)
        require(present == authority['present'] and files == authority['files'], 'C10 authority restoration incomplete')
        receipt = {**original_intent, 'state': 'CLEAR', 'lastACKAuthorityRestored': True, 'failedScenePreserved': True}
        receipt_name = 'C10-' + label + '-recovered.json'
        if receipt_name not in os.listdir(quarantine): fault.new_file(quarantine, receipt_name, fault.encode(receipt))
    return receipt


def disarm(root, owner, request, fault, generation):
    label = request['generation']
    require(label == 'seed', 'only a verified shared seed representation may retire recovery before a later ACK')
    _, _, original = intent(root, owner, request, label, fault, generation)
    present, _, files = scene(root, owner, request, fault, generation)
    require(present and files == request['expectedFiles'], 'C10 shared fixture drifted before retiring seed rescue')
    with fault.parent(root, ['quarantine']) as quarantine:
        fault.new_file(quarantine, 'C10-seed-disarmed.json', fault.encode({**original,
            'state': 'RETIRED', 'verifiedSharedFiles': files, 'laterACKMayProceed': True}))
    return {'state': 'RETIRED', 'laterACKMayProceed': True}


def convert(root, owner, request, fault, generation, tool):
    scope(owner, request)
    _, saved, _ = intent(root, owner, request, 'seed', fault, generation)
    present, values, files = scene(root, owner, request, fault, generation)
    require(present and files == saved['files'], 'C10 shared seed changed before codec conversion')
    source, destination = request['sourceDataDir'], request['destinationDataDir']
    for directory in (source, destination):
        require(str(uuid.UUID(directory)) == directory and uuid.UUID(directory).int != 0,
                'C10 shared directory is not canonical')
    left = {name.removeprefix(source + '/'): raw for name, raw in values.items()
            if name.startswith(source + '/') and re.fullmatch(r'part\.[1-9][0-9]*', name.split('/')[-1])}
    right = {name.removeprefix(destination + '/'): raw for name, raw in values.items()
             if name.startswith(destination + '/') and re.fullmatch(r'part\.[1-9][0-9]*', name.split('/')[-1])}
    require(source != destination and left and left == right,
            'C10 independently ACKed generations do not contain identical physical payload parts')
    result = subprocess.run([str(tool), owner['bucket'], 'orphan.bin', request['sourceVersion'], request['destinationVersion']],
        input=values['xl.meta'], capture_output=True, timeout=60)
    require(result.returncode == 0 and 0 < len(result.stdout) <= fault.LIMIT,
            'candidate codec refused the exact shared fixture')
    receipt = json.loads(result.stderr)
    require(receipt.get('sourceVersion') == request['sourceVersion'] and receipt.get('destinationVersion') == request['destinationVersion']
            and receipt.get('sharedDataDir') == source and receipt.get('retiredDataDir') == destination
            and receipt.get('filesystemMutationPerformed') is False and receipt.get('physicalByteEqualityClaimed') is False,
            'C10 codec receipt lost its original ACK identity')
    with fault.parent(root, ['quarantine']) as quarantine, fault.parent(root, ['data', owner['bucket'], 'orphan.bin']) as obj:
        fault.new_file(quarantine, 'C10-shared-intent.json', fault.encode(receipt))
        fault.new_file(obj, 'C10-converted-meta', result.stdout)
        os.rename(destination, 'C10-retired-shared-data', src_dir_fd=obj, dst_dir_fd=quarantine)
        os.rename('xl.meta', 'C10-original-shared-meta', src_dir_fd=obj, dst_dir_fd=quarantine)
        os.rename('C10-converted-meta', 'xl.meta', src_dir_fd=obj, dst_dir_fd=obj)
        os.fsync(obj); os.fsync(quarantine)
        expected = {name: raw for name, raw in values.items() if not name.startswith(destination + '/')}
        expected['xl.meta'] = result.stdout
        require(generation.tree(obj, fault, allow_sparse=True)[0] == expected,
                'C10 shared conversion changed unrelated bytes or lost source commitments')
        fault.new_file(quarantine, 'C10-shared-applied.json', fault.encode(receipt))
    return {**receipt, 'state': 'APPLIED', 'physicalByteEqualityVerified': True, 'retiredBytesPreserved': True}


def multipart(root, fault):
    """Read the entire isolated run's MPU tree without following aliases."""
    files, total = [], 0
    def visit(fd, prefix):
        nonlocal total
        require(len(prefix) <= 8, 'C10 MPU nesting exceeds its fixture budget')
        for name in sorted(os.listdir(fd)):
            require(re.fullmatch(r'[A-Za-z0-9_.-]{1,256}', name) and name not in {'.', '..'}, 'C10 MPU leaf is unreviewed')
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                with fault.parent(fd, [name]) as child: visit(child, prefix + [name])
            else:
                raw = fault.read_file(fd, name)
                total += len(raw)
                require(len(files) < 512 and total <= 32 * 1024**2, 'C10 MPU census exceeds complete evidence budget')
                files.append({'path': '/'.join(prefix + [name]), 'size': len(raw), 'sha256': fault.digest(raw)})
    with fault.parent(root, ['data', '.rustfs.sys']) as system:
        if 'multipart' in os.listdir(system):
            with fault.parent(system, ['multipart']) as pending: visit(pending, [])
    return {'readOnly': True, 'files': files, 'totalBytes': total}
