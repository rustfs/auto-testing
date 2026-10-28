"""Audit exact stopped J02 bytes before restoring four owner-marker UID/GID pairs."""

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import stat


def require(value, message):
    if not value:
        raise ValueError(message)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def sha(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def identity(info):
    return {name: getattr(info, 'st_' + name) for name in
            ('dev', 'ino', 'uid', 'gid', 'mode', 'nlink', 'size', 'mtime_ns', 'ctime_ns')}


def marker(path, checked, service):
    with os.fdopen(os.open(checked(path, path.parent), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= 16384
                and stat.S_IMODE(before.st_mode) in {0o600, 0o644} and (before.st_uid, before.st_gid) in {(0, 0), service},
                'owner recovery marker has unknown permissions or ownership')
        raw = stream.read(16385)
        require(len(raw) == before.st_size and identity(before) == identity(os.fstat(stream.fileno())),
                'owner recovery marker changed during audit')
    return json.loads(raw), dict(identity(before), sha256=hashlib.sha256(raw).hexdigest())


def inspect(request, node):
    state, roots = node['paths'](request['runId']); checked = node['checked']
    expected = request['expectedOwner']; original = request['before']; faults = request['faults']
    service = pwd.getpwnam('rustfs'); service = (service.pw_uid, service.pw_gid)
    require(service[0] > 0 and isinstance(expected, dict) and expected.get('runId') == request['runId']
            and expected.get('unit') == node['unit'](request['runId'])
            and expected.get('hostname') == node['socket'].gethostname()
            and expected.get('runtimeProfile') == 'outcome-legacy'
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}', expected.get('bucket', '')) is not None,
            'owner recovery requires the exact original J02 reservation')
    state_info = checked(state, state.parent).lstat()
    require(stat.S_ISDIR(state_info.st_mode) and state_info.st_uid == 0 and stat.S_IMODE(state_info.st_mode) == 0o700,
            'owner recovery lost its private root-owned state')
    owner, central = marker(state / 'owner.json', checked, service)
    require((central['uid'], central['gid']) == (0, 0) and owner == expected, 'owner recovery central reservation differs')
    require(len(roots) == len(expected.get('mounts', [])) == 4
            and all(all(a[k] == b[k] for k in ('target', 'source', 'fstype', 'uuid', 'maj:min'))
                    for a, b in zip(node['mounts'](), expected['mounts'], strict=True)), 'owner recovery mounts differ')
    require(not any((state / name).exists() for name in ('J02-phase.json', 'J02-legacy-intent.json',
            'drive-gate.json', 'quorum-gate.json', 'write-quorum-gate.json', 'network-gate.json', 'disk-replacement.json')),
            'owner recovery has a phase or gate beyond the archived boundary')
    stopped = node['stopped'](owner)
    require(node['digest'](checked(state / 'rustfs', state)) == owner['binarySha256']
            and owner.get('inspectorSha256') and node['digest'](checked(state / 'heal-meta-inspect', state)) == owner['inspectorSha256'],
            'owner recovery candidate or inspector changed')
    wanted = {(e['disk'], e['path']): e for e in original['files']}
    require(len(wanted) == len(original['files']) and 8 <= len(wanted) <= 1024 and len(original['formats']) == 4
            and all(e['path'].startswith(owner['bucket'] + '/') for e in wanted.values()), 'owner recovery physical baseline differs')
    require(len(faults) == 2 and len({f['disk'] for f in faults}) == 2
            and all(type(f['disk']) is int and 0 <= f['disk'] < 4 and re.fullmatch(r'J02-[0-9a-f]{32}', f['faultId'])
                    and f['receipt']['relativePath'] == owner['bucket'] + '/opaque/source.bin/xl.meta'
                    and f['receipt']['runId'] == request['runId'] and f['receipt']['faultId'] == f['faultId']
                    and f['receipt']['kind'] == 'bitflip' and type(f['receipt']['offset']) is int
                    and f['receipt']['offset'] == 0 for f in faults), 'owner recovery original faults differ')
    damaged = {(f['disk'], f['receipt']['relativePath']): f['receipt']['fault'] for f in faults}
    require(all(f['receipt']['before'] == {k: wanted[(f['disk'], f['receipt']['relativePath'])][k] for k in ('size', 'sha256')}
                and f['receipt']['backup'] == f['faultId'] + '.original'
                and f['receipt']['fault']['size'] == f['receipt']['before']['size']
                and re.fullmatch(r'[0-9a-f]{64}', f['receipt']['fault']['sha256']) for f in faults),
            'owner recovery faults are not bound to the original metadata')
    raw_files, markers, backups, census = [], [], [], []
    module, retained = node['fault_module'](), node['retained_scene_backend']
    for disk, root in enumerate(roots):
        checked(root, root.parent)
        require(root.is_dir() and root.lstat().st_dev == node['MOUNTS'][disk].stat().st_dev
                and not any(p == root or p.is_relative_to(root) for p in retained.mounted_paths()),
                'owner recovery lost a drive root or found another mount')
        observed, evidence = marker(root / 'owner.json', checked, service)
        require(observed == owner and evidence['sha256'] == central['sha256'] and evidence['mode'] == central['mode'], 'owner recovery drive reservation differs')
        markers.append(dict(disk=disk, **evidence))
        data = checked(root / 'data', root); folder = checked(data / owner['bucket'], data)
        for directory, children, names in os.walk(folder, followlinks=False):
            for name in (*children, *names): checked(Path(directory) / name, data)
            for name in sorted(names):
                path = Path(directory) / name; info = path.lstat(); relative = str(path.relative_to(data)); key = (disk, relative)
                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and key in wanted
                        and info.st_size <= 64 * 1024**2, 'owner recovery has an extra or unsafe business file')
                raw = dict(size=info.st_size, sha256=node['digest'](path))
                require(identity(info) == identity(path.lstat()) and raw in
                        ({k: wanted[key][k] for k in ('size', 'sha256')}, damaged.get(key)),
                        'owner recovery business bytes differ from original or exact fault')
                raw_files.append(dict(disk=disk, path=relative, **raw))
        format_path = checked(data / '.rustfs.sys/format.json', data)
        require(format_path.stat().st_size < 65536 and original['formats'][disk]['disk'] == disk
                and node['digest'](format_path) == original['formats'][disk]['sha256']
                and json.loads(format_path.read_text()) == original['formats'][disk]['value'], 'owner recovery disk format differs')
        for fault in (f for f in faults if f['disk'] == disk):
            receipt = {k: v for k, v in fault['receipt'].items() if k != 'witnessedAt'}
            with module.directory(root) as fd, module.parent(fd, ['quarantine']) as backup_fd:
                binding = module.checked_owner(fd, request['runId'], recovery=True)
                require(binding['bucketPrefix'] == owner['bucket'], 'owner recovery fault owner differs')
                names = {fault['faultId'] + suffix for suffix in ('.original', '.intent.json', '.applied.json')}
                preserved = fault['faultId'] + '.failed-' + receipt['fault']['sha256']
                actual_names = set(os.listdir(backup_fd))
                require(names <= actual_names <= names | {preserved}, 'owner recovery quarantine differs')
                for name in sorted(actual_names):
                    raw = module.read_file(backup_fd, name, 64 * 1024**2)
                    if name.endswith('.intent.json'): require(json.loads(raw) == receipt, 'owner recovery intent differs')
                    elif name.endswith('.applied.json'): require(json.loads(raw) == fault['receipt'], 'owner recovery applied receipt differs')
                    else:
                        wanted_bytes = receipt['before'] if name.endswith('.original') else receipt['fault']
                        require(dict(size=len(raw), sha256=hashlib.sha256(raw).hexdigest()) == wanted_bytes, 'owner recovery backup bytes differ')
                    backups.append(dict(disk=disk, name=name, size=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
        if not any(f['disk'] == disk for f in faults):
            require(not any(checked(root / 'quarantine', root).iterdir()), 'owner recovery has another fault backup')
        census.append(retained.census(root, owner, state=False, node=node))
    require(len(raw_files) == len(wanted) and {(e['disk'], e['path']) for e in raw_files} == set(wanted),
            'owner recovery lost an original business file')
    contract = dict(runId=request['runId'], ownerSha256=sha(owner), beforeSha256=sha(original), faultsSha256=sha(faults))
    intent_path = checked(state / 'owner-marker-recovery.intent.json', state)
    prior = retained.private_json(intent_path) if intent_path.exists() else None
    if prior is not None:
        require(prior.get('contract') == contract and len(prior.get('markers', [])) == 4
                and all(all(old[k] == now[k] for k in ('disk', 'dev', 'ino', 'mode', 'nlink', 'size', 'mtime_ns', 'sha256'))
                        for old, now in zip(prior['markers'], markers, strict=True)), 'owner recovery durable intent differs')
    return dict(contract=contract, markers=markers, backupFiles=backups, rootCensus=census,
                rawPhysicalSha256=sha(sorted(raw_files, key=lambda e: (e['disk'], e['path']))),
                service=stopped, priorIntent=prior, markerRecoveryRequired=any((m['uid'], m['gid']) != (0, 0) for m in markers),
                inspectionOnly=True, mutated=False, canonicalPassClaimed=False), state, roots


def run(request, node):
    fields = {'runId', 'action', 'expectedOwner', 'before', 'faults'}
    if request['action'] == 'owner-markers-recover': fields.add('expectedAudit')
    require(set(request) == fields and request['action'] in {'owner-markers-audit', 'owner-markers-recover'},
            'owner recovery accepts only its archived scene and audit')
    audit, state, roots = inspect(request, node)
    if request['action'] == 'owner-markers-audit': return audit
    require(audit == request['expectedAudit'], 'owner recovery changed after read-only preflight')
    module = node['fault_module'](); prior = audit['priorIntent']
    if prior is None:
        prior = {k: audit[k] for k in ('contract', 'markers', 'backupFiles', 'rootCensus', 'rawPhysicalSha256')}
        with module.directory(state) as fd:
            module.new_file(fd, 'owner-marker-recovery.intent.json', encoded(prior))
    changed = []
    with ExitStack() as stack:
        descriptors = [stack.enter_context(os.fdopen(os.open(node['checked'](root / 'owner.json', root),
                       os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb')) for root in roots]
        for disk, stream in enumerate(descriptors):
            fresh, _, _ = inspect(request, node)
            require(fresh['rawPhysicalSha256'] == audit['rawPhysicalSha256'] and fresh['backupFiles'] == audit['backupFiles']
                    and all(all(a[k] == b[k] for k in ('disk', 'dev', 'ino', 'mode', 'nlink', 'size', 'mtime_ns', 'sha256'))
                            for a, b in zip(audit['markers'], fresh['markers'], strict=True)), 'owner recovery bytes or identities changed')
            info = identity(os.fstat(stream.fileno()))
            require(all(info[k] == fresh['markers'][disk][k] for k in info), 'owner recovery descriptor changed')
            if (info['uid'], info['gid']) != (0, 0):
                os.fchown(stream.fileno(), 0, 0); os.fsync(stream.fileno())
                node['retained_scene_backend'].sync_directory(roots[disk])
                changed.append(disk)
        final, _, _ = inspect(request, node)
        require(not final['markerRecoveryRequired'] and final['rawPhysicalSha256'] == audit['rawPhysicalSha256']
                and final['backupFiles'] == audit['backupFiles'], 'owner recovery post-audit differs')
    return dict(ownerMetadataRestored=True, changedDisks=changed, before=audit, after=final,
                intentSha256=sha(prior), businessBytesChanged=False, backupRestorationUsed=False,
                canonicalPassClaimed=False, originalVerdictRewritten=False)
