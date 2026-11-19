#!/usr/bin/env python3
"""Own disposable XFS image mounts and one RustFS process inside a test container."""

import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time


BACKING = Path('/backing')
STATE = BACKING / 'process.json'


def command(*args):
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f'{args[0]} failed: {result.stdout[:2000]}')
    return result.stdout.strip()


def roots():
    return [Path(f'/data/rustfs{index}') for index in range(1, int(os.environ['NR_DRIVES']) + 1)]


def data_roots(view='data'):
    return [root / ('mnmd' if view == 'data' else 'retired') for root in roots()]


def process():
    state = json.loads(STATE.read_text()) if STATE.exists() else {'pid': 0, 'startTicks': None}
    path = Path(f"/proc/{state['pid']}/stat")
    try:
        fields = path.read_text().rsplit(')', 1)[1].split()
        running = fields[0] != 'Z' and fields[19] == state['startTicks']
    except FileNotFoundError:
        running = False
    observed = {}
    if running:
        try:
            raw = Path(f"/proc/{state['pid']}/environ").read_bytes().decode().split('\0')
            environment = dict(item.split('=', 1) for item in raw if '=' in item)
            observed = {key: environment.get(key) for key in ('RUSTFS_UNSAFE_BYPASS_DISK_CHECK', 'MINIO_CI',
                        'RUSTFS_SCANNER_ENABLED', 'RUSTFS_HEAL_ENABLED', 'RUSTFS_HEAL_AUTO_HEAL_ENABLE', 'RUSTFS_HEAL_MRF_ENABLE')}
        except (FileNotFoundError, ProcessLookupError):
            running = False
    return {**state, 'running': running, 'observedEnvironment': observed}


def stop():
    old = process()
    if old['running']:
        os.kill(old['pid'], signal.SIGTERM)
        deadline = time.monotonic() + 30
        while process()['running'] and time.monotonic() < deadline:
            time.sleep(.1)
        if process()['running']:
            os.kill(old['pid'], signal.SIGKILL)
            deadline = time.monotonic() + 5
            while process()['running'] and time.monotonic() < deadline:
                time.sleep(.1)
        if process()['running']:
            raise RuntimeError('owned RustFS process did not exit')
    return process()


def start(repair=True):
    if process()['running']:
        raise RuntimeError('owned RustFS process is already running')
    candidate = json.loads(Path('/opt/node_replacement_candidate.json').read_text())
    actual = hashlib.sha256(Path('/usr/bin/rustfs').read_bytes()).hexdigest()
    if actual != candidate['binarySha256']:
        raise RuntimeError('runtime binary differs from the verified official release archive')
    environment = {**os.environ, 'RUSTFS_HEAL_ENABLED': str(repair).lower(),
                   'RUSTFS_HEAL_AUTO_HEAL_ENABLE': str(repair).lower(),
                   'RUSTFS_HEAL_MRF_ENABLE': str(repair).lower(),
                   'RUSTFS_SCANNER_ENABLED': str(repair).lower(),
                   'RUSTFS_UNSAFE_BYPASS_DISK_CHECK': 'false', 'MINIO_CI': 'false'}
    volumes = [f'http://node{node}:9000/data/rustfs{drive}/mnmd'
               for node in range(1, 5) for drive in range(1, len(roots()) + 1)]
    with (BACKING / 'stdout.log').open('ab') as output:
        child = subprocess.Popen(['/usr/bin/rustfs', *volumes], env=environment,
            stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    fields = Path(f'/proc/{child.pid}/stat').read_text().rsplit(')', 1)[1].split()
    value = {'pid': child.pid, 'startTicks': fields[19], 'repairEnabled': repair,
             'unsafeBypassDiskCheck': False, 'minioCI': False}
    temporary = STATE.with_suffix('.tmp')
    temporary.write_text(json.dumps(value))
    temporary.replace(STATE)
    return process()


def mounts():
    result = []
    root_device = os.stat('/').st_dev
    for mount in roots():
        observed = json.loads(command('findmnt', '--json', '--mountpoint', str(mount),
            '--output', 'TARGET,SOURCE,FSTYPE,UUID,MAJ:MIN,OPTIONS'))['filesystems'][0]
        if observed['fstype'] != 'xfs' or not observed['uuid'] or os.stat(mount).st_dev == root_device:
            raise RuntimeError('test endpoint lacks a distinct mounted XFS block device')
        observed['endpoint'] = str(mount / 'mnmd')
        probe = subprocess.run(['findmnt', '--mountpoint', str(mount / 'mnmd')], capture_output=True)
        observed['endpointIsMountpoint'] = probe.returncode == 0
        result.append(observed)
    if len({row['maj:min'] for row in result}) != len(result):
        raise RuntimeError('data endpoints share a block device')
    return result


def reset():
    if process()['running']:
        raise RuntimeError('empty-root replacement requires the original process to exit')
    for data, retired in zip(data_roots(), data_roots('retired'), strict=True):
        if retired.exists():
            raise RuntimeError('retired data already exists')
        if os.environ['NR_LAYOUT'] == 'mountpoint':
            command('umount', str(data))
        data.rename(retired)
        data.mkdir()
        if os.environ['NR_LAYOUT'] == 'mountpoint':
            command('mount', '--bind', str(data), str(data))
    return {'empty': all(not list(path.iterdir()) for path in data_roots()), 'mounts': mounts()}


def quarantine(keys):
    if process()['running']:
        raise RuntimeError('quarantine requires a stopped process')
    for disk, data in enumerate(data_roots()):
        archive = data / f'.nr-oracle-{time.time_ns()}'
        archive.mkdir()
        for index, relative in enumerate(keys[disk]):
            path = data / relative
            if not path.is_relative_to(data) or '..' in Path(relative).parts or path.is_symlink():
                raise RuntimeError('invalid owned shard path')
            path.rename(archive / str(index))
    return {'quarantinedPerDisk': len(keys)}


def log_cursors():
    return {path.name: path.stat().st_size for path in (BACKING / 'logs').glob('*') if path.is_file()}


def metadata_failures(cursors):
    matches = []
    for path in (BACKING / 'logs').glob('*'):
        if not path.is_file():
            continue
        size = path.stat().st_size
        offset = min(cursors.get(path.name, 0), size)
        with path.open('rb') as stream:
            stream.seek(max(offset, size - 2 * 1024**2))
            text = stream.read().decode(errors='replace')
        matches.extend(line for line in text.splitlines() if 'load_pool_meta' in line or 'classify_pool_meta_absence' in line
                       or 'pool.bin' in line and ('InsufficientReadQuorum' in line or 'quorum' in line.lower()))
    text = '\n'.join(matches[-8:])
    for key in ('RUSTFS_ACCESS_KEY', 'RUSTFS_SECRET_KEY'):
        text = text.replace(os.environ[key], '[REDACTED]')
    return {'observed': bool(matches), 'signatures': text}


def serve():
    mounted = []
    (BACKING / 'logs').mkdir(exist_ok=True)
    def interrupted(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupted)
    try:
        # Docker may expose only loop0..loop7 even when the kernel can allocate
        # more loop devices. Allocation remains delegated to mount/loop-control.
        for index in range(128):
            device = Path(f'/dev/loop{index}')
            if not device.exists():
                os.mknod(device, stat.S_IFBLK | 0o600, os.makedev(7, index))
        for index, mount in enumerate(roots(), 1):
            image = BACKING / f"{os.environ['NR_RUN_ID']}-node{os.environ['NR_SLOT']}-disk{index}.xfs"
            # Only a freshly and exclusively created regular file is formatted.
            with image.open('xb') as output:
                output.truncate(512 * 1024**2)
            command('mkfs.xfs', '-f', str(image))
            mount.mkdir(parents=True)
            command('mount', '-t', 'xfs', '-o', 'loop', str(image), str(mount))
            mounted.append(mount)
            (mount / 'mnmd').mkdir()
            if os.environ['NR_LAYOUT'] == 'mountpoint':
                command('mount', '--bind', str(mount / 'mnmd'), str(mount / 'mnmd'))
        print(json.dumps({'mounts': mounts()}), flush=True)
        start()
        while True:
            try:
                while os.waitpid(-1, os.WNOHANG)[0]:
                    pass
            except ChildProcessError:
                pass
            time.sleep(.2)
    except KeyboardInterrupt:
        pass
    finally:
        stop()
        for mount in reversed(mounted):
            if os.environ['NR_LAYOUT'] == 'mountpoint':
                command('umount', str(mount / 'mnmd'))
            command('umount', str(mount))


def main():
    action = sys.argv[1]
    if action == 'serve':
        serve()
        return
    request = json.load(sys.stdin)
    if action == 'state': value = process()
    elif action == 'stop': value = stop()
    elif action == 'start': value = start(request.get('repair', True))
    elif action == 'mounts': value = mounts()
    elif action == 'reset': value = reset()
    elif action == 'quarantine': value = quarantine(request['paths'])
    elif action == 'log-cursors': value = log_cursors()
    elif action == 'metadata-failure': value = metadata_failures(request['cursors'])
    elif action == 'identity':
        binary = Path('/usr/bin/rustfs')
        value = {'binarySha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                 'version': command(str(binary), '--version'), 'kernel': command('uname', '-a'),
                 'candidate': json.loads(Path('/opt/node_replacement_candidate.json').read_text())}
    else: raise ValueError('unsupported node operation')
    print(json.dumps(value))


if __name__ == '__main__':
    main()
