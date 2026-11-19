#!/usr/bin/env python3
"""Rebuild a member of a real XFS-backed four-node pool with disk bypass disabled."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import signal
import subprocess
import tarfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, build_opener

try:
    from .validation_heal.heal_probe import payload, require, save
except ImportError:
    from validation_heal.heal_probe import payload, require, save


LABEL = 'org.rustfs.auto-testing.node-replacement'
SIZES = (1024 * 1024 + 47, 3 * 1024 * 1024 + 19, 8 * 1024 * 1024 + 7)
SYSTEM_KEYS = ('pool.bin', 'pool.bin.identity')
ARCHIVE_LIMIT = 128 * 1024 * 1024
NODE_TOOL = '/opt/node_replacement_node.py'


def archive_files(raw):
    require(len(raw) <= ARCHIVE_LIMIT, 'physical archive exceeds the fixture budget')
    result = {}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for entry in archive:
            path = PurePosixPath(entry.name)
            require(not path.is_absolute() and '..' not in path.parts, 'unsafe physical archive path')
            if entry.isdir():
                continue
            require(entry.isfile() and entry.size <= ARCHIVE_LIMIT, 'unexpected physical file type or size')
            name = str(path)
            require(name not in result, 'duplicate physical file')
            result[name] = archive.extractfile(entry).read()
    return result


def file_manifest(files):
    return {name: {'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
            for name, data in sorted(files.items())}


def topology(disks, *, original=None, replaced_disks=None):
    require(len(disks) in (4, 16), 'expected a four-node 4x1 or 4x4 pool')
    formats = [json.loads(files['.rustfs.sys/format.json']) for files in disks]
    sets = formats[0]['xl']['sets']
    require(len(sets) == 1 and len(sets[0]) == len(set(sets[0])) == len(disks),
            'expected one complete cross-node erasure set')
    require(all(item['format'] == 'xl' and item['id'] == formats[0]['id'] and item['xl']['sets'] == sets
                for item in formats), 'pool or set membership differs')
    members = [item['xl']['this'] for item in formats]
    require(len(set(members)) == len(disks) and set(members) == set(sets[0]), 'incomplete physical membership')
    if original is not None:
        require(formats[0]['id'] == original['poolId'], 'replacement changed the pool identity')
        replaced_disks = {len(disks) - 1} if replaced_disks is None else set(replaced_disks)
        require(all(member == original['members'][index] for index, member in enumerate(members)
                    if index not in replaced_disks), 'replacement changed a surviving disk identity')
        require(all(sets[0][original['set'].index(old)] == members[index]
                    for index, old in enumerate(original['members'])), 'replacement moved another disk set slot')
        # Per-slot identity changes are recorded; a fresh process and the empty
        # endpoint census establish replacement even when the slot UUID is inherited.
    return {'poolId': formats[0]['id'], 'set': sets[0], 'members': members,
            'nodes': 4, 'drivesPerNode': len(disks) // 4}


def system_metadata_manifest(files):
    missing = []
    if not files.get('.rustfs.sys/format.json'):
        missing.append('format.json')
    result = {}
    for key in SYSTEM_KEYS:
        path = f'.rustfs.sys/{key}/xl.meta'
        metadata = files.get(path, b'')
        if metadata.startswith(b'XL2 ') and len(metadata) > 32:
            result[key] = file_manifest({path: metadata})[path]
        else:
            missing.append(key + '/xl.meta')
    require(not missing, 'replacement system metadata is missing or invalid: ' + ', '.join(missing))
    return result


def shard_manifest(files, bucket, objects):
    result = {}
    for key in objects:
        prefix = f'{bucket}/{key}/'
        metadata = files.get(prefix + 'xl.meta', b'')
        parts = [name for name in files if name.startswith(prefix) and name.endswith('/part.1')]
        require(metadata and len(parts) == 1, f'missing or ambiguous replacement metadata/part: {key}')
        part = parts[0]
        require(len(PurePosixPath(part).relative_to(prefix).parts) == 2 and len(files[part]) > 0,
                f'invalid external shard: {key}')
        result[key] = {'path': part, **file_manifest({part: files[part]})[part],
                       'metadataSha256': hashlib.sha256(metadata).hexdigest()}
    return result


def reconstruction_sources(drives, parity):
    data = 4 * drives - parity
    keep = (data + drives - 1) // drives - 1
    require(1 <= keep < 3 and keep * drives < data <= (keep + 1) * drives,
            'unsupported reconstruction quorum geometry')
    return keep


class Case:
    def __init__(self, image, artifacts, timeout, drives=4, target=2, layout='subdirectory'):
        self.image_ref, self.artifacts, self.timeout = image, artifacts, timeout
        self.drives, self.target, self.layout = drives, target - 1, layout
        self.parity = 2 if drives == 1 else 4
        self.run_id = 'nr-' + secrets.token_hex(6)
        self.network = self.run_id
        self.containers, self.volumes = [], []
        self.access, self.secret = secrets.token_hex(12), secrets.token_hex(24)
        self.bucket = 'node-replacement-' + self.run_id
        self.objects = {}
        self.sequence, self.image = 0, None
        self.env_file = artifacts / '.runtime.env'
        self.assertions = {}
        self.phase = 'preparation'

    def record(self, label, value):
        self.sequence += 1
        save(self.artifacts / f'{self.sequence:04d}-{label}.json', value)
        if label not in {'put-ack', 'degraded-read', 'recovered-read', 'replacement-required-read'}:
            print(label, flush=True)
        return value

    def docker(self, *arguments, timeout=90, binary=False, input=None, live_archive=False):
        deadline = time.monotonic() + timeout
        while True:
            result = subprocess.run(['docker', *arguments], capture_output=True, timeout=timeout, input=input)
            if not result.returncode:
                return result.stdout if binary else result.stdout.decode().strip()
            message = result.stderr.decode(errors='replace')
            lines = [line for line in message.splitlines() if line]
            changing = lines and all(line.endswith(('file changed as we read it', 'File removed before we read it')) for line in lines)
            if live_archive and changing and time.monotonic() < deadline:
                time.sleep(.2)
                continue
            for value in (self.access, self.secret):
                message = message.replace(value, '[REDACTED]')
            raise RuntimeError(f'docker {arguments[0]} failed: {message[:2000]}')

    def inspect(self, kind, name):
        return json.loads(self.docker(kind, 'inspect', name))[0]

    def node(self, name, action, **params):
        return json.loads(self.docker('exec', '-i', name, 'python3', NODE_TOOL, action,
                                     input=json.dumps(params).encode(), timeout=60))

    def resources(self):
        value = {'runId': self.run_id, 'network': self.network, 'containers': self.containers, 'volumes': self.volumes}
        path = self.artifacts / 'resources.tmp'
        path.write_text(json.dumps(value, indent=2) + '\n')
        path.replace(self.artifacts / 'resources.json')

    def create(self, slot):
        volume = f'{self.run_id}-backing{slot}'
        self.volumes.append(volume)
        self.resources()
        self.docker('volume', 'create', '--label', f'{LABEL}={self.run_id}', volume)
        name = f'{self.run_id}-node{slot}'
        self.containers.append(name)
        self.resources()
        self.docker('create', '--platform', self.platform, '--name', name, '--privileged', '--label', f'{LABEL}={self.run_id}',
                    '--hostname', f'node{slot}', '--network', self.network, '--network-alias', f'node{slot}',
                    '--publish', '127.0.0.1::9000', '--mount', f'type=volume,source={volume},target=/backing,volume-nocopy',
                    '--env-file', str(self.env_file), '--env', f'NR_DRIVES={self.drives}',
                    '--env', f'NR_SLOT={slot}', '--env', f'NR_RUN_ID={self.run_id}', '--env', f'NR_LAYOUT={self.layout}',
                    '--entrypoint', 'python3', self.image, NODE_TOOL, 'serve', timeout=180)
        require(self.inspect('container', name)['Image'] == self.image, 'candidate image changed')
        self.docker('start', name)
        return name

    def endpoint(self, name):
        info = self.inspect('container', name)
        require(info['State']['Running'], 'XFS test container exited; see bootstrap log')
        ports = info['NetworkSettings']['Ports']['9000/tcp']
        require(len(ports) == 1 and ports[0]['HostIp'] == '127.0.0.1', 'unexpected published endpoint')
        return 'http://127.0.0.1:' + ports[0]['HostPort']

    def wait_ready(self, names, *, writable=False):
        opener = build_opener(ProxyHandler({}))
        deadline = time.monotonic() + self.timeout
        endpoints = [self.endpoint(name) for name in names]
        while True:
            require(all(self.inspect('container', name)['State']['Running'] for name in names),
                    'an XFS test container exited before readiness')
            try:
                for endpoint in endpoints:
                    with opener.open(endpoint + '/health/ready', timeout=3) as response:
                        require(response.status == 200, 'node is not ready')
                    if writable:
                        with opener.open(endpoint + '/minio/health/cluster', timeout=3) as response:
                            require(response.status == 200 and json.load(response).get('ready') is True,
                                    'cluster has not admitted writes')
                return
            except (OSError, ValueError, URLError):
                require(time.monotonic() < deadline, 'node readiness deadline exceeded')
                time.sleep(1)

    def s3(self, name):
        import boto3
        from botocore.config import Config
        return boto3.client('s3', endpoint_url=self.endpoint(name), region_name='us-east-1',
            aws_access_key_id=self.access, aws_secret_access_key=self.secret,
            config=Config(s3={'addressing_style': 'path'}, proxies={}, connect_timeout=3, read_timeout=15,
                          retries={'total_max_attempts': 1}, request_checksum_calculation='when_required',
                          response_checksum_validation='when_required'))

    def write(self, name, phase, *, small=0):
        client = self.s3(name)
        sizes = [1024] * small + list(SIZES)
        for index, size in enumerate(sizes):
            key = f'{phase}-{index}.bin'
            body = payload(self.run_id + key, size)
            response = client.put_object(Bucket=self.bucket, Key=key, Body=body)
            require(response['ResponseMetadata']['HTTPStatusCode'] == 200, 'PUT was not acknowledged')
            self.objects[key] = {'size': size, 'sha256': hashlib.sha256(body).hexdigest(), 'phase': phase}
            self.record('put-ack', {'key': key, **self.objects[key],
                                   'requestId': response['ResponseMetadata'].get('RequestId')})

    def read(self, names, label, *, objects=None):
        for name in names:
            client = self.s3(name)
            for key, expected in (self.objects if objects is None else objects).items():
                response = client.get_object(Bucket=self.bucket, Key=key)
                with response['Body'] as stream:
                    body = stream.read()
                require(len(body) == expected['size'] and hashlib.sha256(body).hexdigest() == expected['sha256'],
                        f'acknowledged object content differs: {key}')
                self.record(label, {'node': name, 'key': key, **expected, 'contentVerified': True})

    def stop(self, names):
        for name in names:
            state = self.node(name, 'stop')
            require(state['running'] is False, 'RustFS process did not exit')

    def start(self, names, *, repair=False):
        for name in names:
            state = self.node(name, 'start', repair=repair)
            require(state['unsafeBypassDiskCheck'] is False and state['minioCI'] is False,
                    'disk-check bypass was enabled')
            observed = state['observedEnvironment']
            require(observed.get('RUSTFS_UNSAFE_BYPASS_DISK_CHECK') == 'false' and observed.get('MINIO_CI') == 'false',
                    'the actual service process enabled disk-check bypass')

    def read_without_replacement(self, name, *, objects=None):
        client = self.s3(name)
        for key in (self.objects if objects is None else objects):
            try:
                response = client.get_object(Bucket=self.bucket, Key=key)
            except Exception as exc:
                error = getattr(exc, 'response', {})
                code = error.get('Error', {}).get('Code')
                require(error.get('ResponseMetadata', {}).get('HTTPStatusCode') == 503
                        and code in {'SlowDownRead', 'InsufficientReadQuorum', 'XMinioReadQuorum'},
                        f'negative reconstruction control did not reach a read-quorum failure: {code}')
                self.record('without-replacement-read', {'key': key, 'httpStatus': 503, 'code': code})
            else:
                response['Body'].close()
                raise ValueError('GET succeeded without enough surviving shards; reconstruction oracle is invalid')

    def scenes(self, name, *, view='data'):
        directory = 'mnmd' if view == 'data' else 'retired'
        return [archive_files(self.docker('exec', name, 'tar', '--exclude=./.rustfs.sys/tmp',
                                         '-C', f'/data/rustfs{disk}/{directory}', '-cf', '-', '.', binary=True, live_archive=True))
                for disk in range(1, self.drives + 1)]

    def snapshot(self, name, label, *, view='data', quiescent=True):
        state = self.node(name, 'state')
        require(not quiescent or state['running'] is False, 'snapshot requires a stopped RustFS process')
        directory = 'mnmd' if view == 'data' else 'retired'
        raw = [self.docker('exec', name, 'tar', *(['--exclude=./.rustfs.sys/tmp'] if not quiescent else []),
                          '-C', f'/data/rustfs{disk}/{directory}', '-cf', '-', '.', binary=True, live_archive=not quiescent)
               for disk in range(1, self.drives + 1)]
        scenes = [archive_files(value) for value in raw]
        self.record(label, {'node': name, 'view': view, 'process': state, 'disks': [file_manifest(value) for value in scenes]})
        for disk, value in enumerate(raw, 1):
            (self.artifacts / f'{self.sequence:04d}-{label}-disk{disk}.tar').write_bytes(value)
        return scenes

    def wait_repair(self, name, large, reference=None):
        deadline, previous, last = time.monotonic() + self.timeout, None, None
        while True:
            scenes = self.scenes(name)
            observed, missing = [], []
            for disk, files in enumerate(scenes, 1):
                checks = {}
                for label, check in (('systemMetadata', lambda: system_metadata_manifest(files)),
                                     ('objectShards', lambda: shard_manifest(files, self.bucket, large))):
                    try:
                        checks[label] = check()
                        if label == 'objectShards' and reference is not None:
                            require(all(row['path'] == reference[disk-1][key]['path']
                                        and row['size'] == reference[disk-1][key]['size']
                                        for key, row in checks[label].items()), 'replacement shard geometry is incomplete')
                    except (KeyError, ValueError) as exc:
                        checks.pop(label, None)
                        missing.append({'disk': disk, 'assertion': label, 'error': str(exc)})
                observed.append(checks)
            if not missing and observed == previous:
                self.assertions.update(systemMetadataRecovered=True, replacementShardsPhysicallyPresent=True)
                self.record('automatic-repair', {'disks': observed})
                return
            previous = observed if not missing else None
            if missing != last:
                self.record('repair-missing', {'missing': missing})
                last = missing
            if time.monotonic() >= deadline:
                self.assertions.update(systemMetadataRecovered=all('systemMetadata' in row for row in observed),
                                       replacementShardsPhysicallyPresent=all('objectShards' in row for row in observed))
                self.record('repair-deadline', {'missing': missing, 'disks': observed})
                raise ValueError('automatic replacement repair deadline exceeded; see repair-deadline evidence')
            time.sleep(2)

    def health_snapshot(self, names, label):
        opener = build_opener(ProxyHandler({}))
        for name in names:
            for path in ('/health/ready', '/minio/health/cluster'):
                try:
                    with opener.open(self.endpoint(name) + path, timeout=3) as response:
                        body = response.read(65536).decode(errors='replace')
                        self.record(label, {'node': name, 'path': path, 'httpStatus': response.status, 'body': body})
                except (URLError, OSError) as exc:
                    self.record(label, {'node': name, 'path': path, 'error': str(exc)})

    def forced_reconstruction(self, names, large, repaired):
        replacement = names[self.target]
        survivors = [name for name in names if name != replacement]
        keep = reconstruction_sources(self.drives, self.parity)
        quarantined = survivors[keep:]
        replacement_proofs = [shard_manifest(files, self.bucket, large) for files in repaired[self.target]]
        small = {key: value for key, value in self.objects.items() if value['size'] <= 1024}
        for name in quarantined:
            proofs = [shard_manifest(files, self.bucket, large) for files in repaired[names.index(name)]]
            self.node(name, 'quarantine', paths=[[str(PurePosixPath(row['path']).parent) for row in disk.values()]
                                               + [f'{self.bucket}/{key}' for key in small]
                                               for disk in proofs])
        self.start(survivors)
        self.wait_ready(survivors)
        self.read_without_replacement(survivors[0], objects=large)
        self.start([replacement])
        self.wait_ready(names)
        self.read(names, 'replacement-required-read')
        self.stop(names)
        for name in quarantined:
            scenes = self.snapshot(name, 'quarantined-source-after-reads')
            original = repaired[names.index(name)]
            for files, old in zip(scenes, original, strict=True):
                proof = shard_manifest(old, self.bucket, large)
                require(all(row['path'] not in files for row in proof.values()), 'quarantined source was repaired during verification')
                require(all(f'{self.bucket}/{key}/xl.meta' not in files for key in small),
                        'quarantined inline metadata was repaired during verification')
        final = self.snapshot(replacement, 'replacement-after-forced-reads')
        require([shard_manifest(files, self.bucket, large) for files in final] == replacement_proofs,
                'replacement shards changed during reconstruction verification')
        for before, after in zip(repaired[self.target], final, strict=True):
            require(all(before[f'{self.bucket}/{key}/xl.meta'] == after[f'{self.bucket}/{key}/xl.meta'] for key in small),
                    'replacement inline data changed during reconstruction verification')
        self.assertions['replacementRequiredForReconstruction'] = True
        # Force a subsequent cold start to read both pool metadata objects from
        # a quorum that includes the replacement's restored system shards.
        # The system metadata writer uses balanced data/parity, independently
        # of the bucket's EC:4 object policy. Keep one old node's system shards.
        for name in survivors[1:]:
            self.node(name, 'quarantine', paths=[[f'.rustfs.sys/{key}' for key in SYSTEM_KEYS]] * self.drives)
        cursors = {name: self.node(name, 'log-cursors') for name in survivors}
        self.start(survivors)
        opener = build_opener(ProxyHandler({}))
        deadline = time.monotonic() + self.timeout
        while True:
            failures = {}
            for name in survivors:
                try:
                    with opener.open(self.endpoint(name) + '/health/ready', timeout=3) as response:
                        require(response.status != 200, 'cold-start pool metadata control unexpectedly became ready')
                except (HTTPError, URLError, OSError):
                    pass
                failures[name] = self.node(name, 'metadata-failure', cursors=cursors[name])
            if all(value['observed'] for value in failures.values()):
                break
            require(time.monotonic() < deadline, 'cold-start metadata failure was not witnessed in the new processes')
            time.sleep(1)
        self.record('pool-metadata-negative-control', {'replacementOffline': True, 'noSurvivorReady': True, 'failures': failures})
        self.stop(survivors)
        self.start(names)
        self.wait_ready(names)
        self.read(names, 'recovered-read', objects=large)
        self.assertions['replacementPoolMetadataRequiredForColdStart'] = True
        self.stop(names)

    def run(self):
        info = self.inspect('image', self.image_ref)
        self.image = info['Id']
        self.platform = 'linux/' + info['Architecture']
        self.record('candidate-image', {'imageRef': self.image_ref, 'imageId': self.image, 'architecture': info['Architecture']})
        descriptor = os.open(self.env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as output:
            config = {'RUSTFS_ACCESS_KEY': self.access, 'RUSTFS_SECRET_KEY': self.secret,
                      'RUSTFS_ADDRESS': ':9000', 'RUSTFS_CONSOLE_ENABLE': 'false',
                      'RUSTFS_STORAGE_CLASS_STANDARD': f'EC:{self.parity}', 'RUSTFS_ERASURE_SET_DRIVE_COUNT': str(4 * self.drives),
                      'RUSTFS_SCANNER_ENABLED': 'true', 'RUSTFS_HEAL_INTERVAL_SECS': '5',
                      'RUSTFS_UNSAFE_BYPASS_DISK_CHECK': 'false', 'MINIO_CI': 'false',
                      'RUSTFS_OBS_LOGGER_LEVEL': 'info', 'RUST_LOG': 'info', 'RUSTFS_OBS_LOG_DIRECTORY': '/backing/logs'}
            output.write('\n'.join(f'{key}={value}' for key, value in config.items()) + '\n')
        self.resources()
        self.docker('network', 'create', '--label', f'{LABEL}={self.run_id}', self.network)
        names = [self.create(slot) for slot in range(1, 5)]
        self.wait_ready(names, writable=True)
        mounts = {name: self.record('xfs-mounts', {'node': name, 'mounts': self.node(name, 'mounts')})['mounts'] for name in names}
        require(len({row['uuid'] for rows in mounts.values() for row in rows}) == self.drives * 4,
                'XFS filesystem identities are shared or missing')
        self.record('candidate-binary', self.node(names[0], 'identity'))
        for name in names:
            state = self.record('runtime', {'node': name, **self.node(name, 'state')})
            observed = state['observedEnvironment']
            require(observed.get('RUSTFS_UNSAFE_BYPASS_DISK_CHECK') == 'false' and observed.get('MINIO_CI') == 'false',
                    'the actual service process enabled disk-check bypass')
        self.phase = 'baseline'
        self.s3(names[0]).create_bucket(Bucket=self.bucket)
        self.write(names[0], 'before-outage', small=100)
        replacement = names[self.target]
        survivors = [name for name in names if name != replacement]
        original_process = self.node(replacement, 'state')
        self.stop([replacement])
        self.phase = 'outage'
        before = [self.snapshot(name, 'before-outage', quiescent=name == replacement) for name in names]
        layout = self.record('initial-layout', topology([disk for node in before for disk in node]))
        for disk in before[self.target]:
            system_metadata_manifest(disk)
        self.wait_ready(survivors, writable=True)
        self.write(survivors[0], 'during-outage')
        self.read(survivors, 'degraded-read')
        large = {key: value for key, value in self.objects.items() if value['size'] > 1024}
        reference = [shard_manifest(files, self.bucket, large) for files in self.scenes(survivors[0])]
        frozen = self.snapshot(replacement, 'retired-before-reset')
        require([file_manifest(files) for files in frozen] == [file_manifest(files) for files in before[self.target]],
                'offline endpoint changed during degraded writes')
        empty = self.record('replacement-empty', self.node(replacement, 'reset'))
        require(empty['empty'] is True and empty['mounts'] == mounts[replacement], 'replacement changed XFS mounts or retained data')
        require(all(not files for files in self.scenes(replacement)), 'replacement endpoints are not empty')
        self.start([replacement], repair=True)
        self.phase = 'rejoin'
        new_process = self.record('replacement-process', self.node(replacement, 'state'))
        require((new_process['pid'], new_process['startTicks']) != (original_process['pid'], original_process['startTicks']),
                'replacement did not start a new process')
        # Health probes do not read the fixture. Physical repair is decided
        # before any recovered-object GET could trigger read repair.
        self.wait_ready(names)
        self.health_snapshot(names, 'rejoined-health')
        self.phase = 'repair'
        try:
            self.wait_repair(replacement, large, reference)
        except ValueError:
            # Preserve the failed physical verdict even if survivor-backed
            # reads subsequently succeed, as observed in backlog #3283.
            try:
                self.read(names, 'recovered-read')
                self.assertions['allNodeContentVerified'] = True
            except Exception as exc:
                self.record('post-failure-read-error', {'type': type(exc).__name__})
            raise
        self.read(names, 'recovered-read')
        self.assertions['allNodeContentVerified'] = True
        self.stop(names)
        repaired = [self.snapshot(name, 'repaired-physical') for name in names]
        current = self.record('replacement-layout', topology([disk for node in repaired for disk in node], original=layout,
            replaced_disks=range(self.target*self.drives, (self.target+1)*self.drives)))
        for node, (old, new) in enumerate(zip(before, repaired, strict=True)):
            if node == self.target:
                continue
            require(current['members'][node*self.drives:(node+1)*self.drives]
                    == layout['members'][node*self.drives:(node+1)*self.drives], 'a surviving disk identity changed')
        self.record('replacement-pool-metadata', {'disks': [system_metadata_manifest(files) for files in repaired[self.target]]})
        before_large = {key: value for key, value in large.items() if value['phase'] == 'before-outage'}
        for old, files in zip(before[self.target], repaired[self.target], strict=True):
            require(all(files.get(f'{self.bucket}/{key}/xl.meta') for key in self.objects), 'replacement omitted inline object metadata')
            original_parts = shard_manifest(old, self.bucket, before_large)
            new_parts = shard_manifest(files, self.bucket, before_large)
            require(all(original_parts[key]['sha256'] == row['sha256'] for key, row in new_parts.items()),
                    'replacement changed pre-outage same-slot shard bytes')
        retired = self.snapshot(replacement, 'retired-after-repair', view='retired')
        require([file_manifest(files) for files in retired] == [file_manifest(files) for files in frozen], 'retired data changed during repair')
        self.phase = 'forced-reconstruction'
        self.forced_reconstruction(names, large, repaired)
        return {'topology': f'4x{self.drives}', 'poolCount': 1, 'parity': self.parity,
                'endpointLayout': self.layout, 'unsafeBypassDiskCheck': False, 'minioCI': False,
                'storage': 'independent loop-backed XFS filesystems', 'objects': self.objects,
                'newProcessAndEmptyRoots': True, **self.assertions}


def cleanup(artifacts):
    path = artifacts / 'resources.json'
    if not path.exists():
        return []
    resources = json.loads(path.read_text())
    failures = []
    for kind, names in (('container', reversed(resources['containers'])),
                        ('volume', reversed(resources['volumes'])), ('network', [resources['network']])):
        for name in names:
            result = subprocess.run(['docker', kind, 'inspect', name], capture_output=True, text=True, timeout=30)
            if result.returncode:
                missing = {'container': f'no such container: {name}', 'volume': f'get {name}: no such volume',
                           'network': f'network {name} not found'}
                if missing[kind] in result.stderr.lower():
                    continue
                failures.append({'kind': kind, 'name': name, 'error': 'inspect failed'})
                continue
            info = json.loads(result.stdout)[0]
            labels = info.get('Labels') if kind != 'container' else info.get('Config', {}).get('Labels')
            if (labels or {}).get(LABEL) != resources['runId']:
                failures.append({'kind': kind, 'name': name, 'error': 'ownership label differs'})
                continue
            if kind == 'container':
                subprocess.run(['docker', 'stop', '--time', '45', name], capture_output=True, timeout=60)
            removed = subprocess.run(['docker', kind, 'rm', *(['--force'] if kind == 'container' else []), name],
                                     capture_output=True, text=True, timeout=90)
            if removed.returncode:
                failures.append({'kind': kind, 'name': name, 'error': 'removal failed'})
    (artifacts / '.runtime.env').unlink(missing_ok=True)
    return failures


def finish(case, result):
    for name in case.containers:
        try:
            captured = subprocess.run(['docker', 'logs', '--tail', '1000', name], capture_output=True, timeout=30)
            text = (captured.stdout + captured.stderr).decode(errors='replace')
            text = text.replace(case.access, '[REDACTED]').replace(case.secret, '[REDACTED]')
            (case.artifacts / (name + '-bootstrap.log')).write_text(text)
        except Exception as exc:
            case.record('bootstrap-log-error', {'node': name, 'error': type(exc).__name__})
        try:
            logs = case.docker('cp', name + ':/backing/stdout.log', '-', binary=True)
            for path, data in archive_files(logs).items():
                text = data.decode(errors='replace').replace(case.access, '[REDACTED]').replace(case.secret, '[REDACTED]')
                (case.artifacts / (name + '.log')).write_text(text)
            log_directory = case.docker('cp', name + ':/backing/logs/.', '-', binary=True)
            for index, data in enumerate(archive_files(log_directory).values()):
                text = data.decode(errors='replace').replace(case.access, '[REDACTED]').replace(case.secret, '[REDACTED]')
                (case.artifacts / (name + f'-file-{index}.log')).write_text(text)
        except Exception as exc:
            case.record('service-log-error', {'node': name, 'error': type(exc).__name__})
        try:
            case.stop([name])
            scenes = case.snapshot(name, 'final-physical')
            if case.containers.index(name) == case.target:
                missing = []
                for disk, files in enumerate(scenes, 1):
                    try:
                        system_metadata_manifest(files)
                    except ValueError as exc:
                        missing.append({'disk': disk, 'error': str(exc)})
                case.assertions.setdefault('systemMetadataRecovered', not missing)
                for key in SYSTEM_KEYS:
                    present = all(files.get(f'.rustfs.sys/{key}/xl.meta', b'').startswith(b'XL2 ')
                                  and len(files.get(f'.rustfs.sys/{key}/xl.meta', b'')) > 32 for files in scenes)
                    field = {'pool.bin': 'poolBinRecovered', 'pool.bin.identity': 'poolIdentityRecovered'}[key]
                    case.assertions[field] = present
                case.record('final-system-metadata', {'missing': missing})
            case.record('final-mounts', {'node': name, 'mounts': case.node(name, 'mounts')})
        except Exception as exc:
            case.record('final-capture-error', {'node': name, 'error': str(exc)[:500]})
    try:
        failures = cleanup(case.artifacts)
        result.update(cleanup='FAIL' if failures else 'PASS', cleanupFailures=failures)
    except Exception as exc:
        result.update(cleanup='FAIL', cleanupError=type(exc).__name__)
    case.env_file.unlink(missing_ok=True)
    result['assertions'] = case.assertions
    save(case.artifacts / 'result.json', result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', help='image built with scripts/prepare_node_replacement.py')
    parser.add_argument('--artifacts-dir', type=Path, required=True)
    parser.add_argument('--timeout', type=int, default=300)
    parser.add_argument('--drives-per-node', type=int, choices=(1, 4), default=4)
    parser.add_argument('--target-node', type=int, choices=(1, 2, 3, 4), default=2)
    parser.add_argument('--endpoint-layout', choices=('subdirectory', 'mountpoint'), default='subdirectory')
    parser.add_argument('--cleanup', action='store_true')
    args = parser.parse_args()
    if args.cleanup:
        failures = cleanup(args.artifacts_dir)
        print(json.dumps({'cleanup': 'FAIL' if failures else 'PASS', 'failures': failures}))
        return bool(failures)
    require(args.image and 10 <= args.timeout <= 1800, 'image and a 10..1800 second timeout are required')
    args.artifacts_dir.mkdir(parents=True, exist_ok=False)
    case = Case(args.image, args.artifacts_dir.resolve(), args.timeout, args.drives_per_node, args.target_node, args.endpoint_layout)
    result = {'caseId': 'node-replacement-xfs', 'verdict': 'RUNNING', 'cleanup': 'PENDING',
              'topology': f'4x{args.drives_per_node}', 'targetNode': args.target_node,
              'storage': 'independent loop-backed XFS filesystems',
              'endpointLayout': args.endpoint_layout, 'unsafeBypassDiskCheck': False, 'minioCI': False}
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        result.update(case.run(), verdict='PASS')
    except (Exception, KeyboardInterrupt) as exc:
        message = str(exc).replace(case.access, '[REDACTED]').replace(case.secret, '[REDACTED]')
        infrastructure = isinstance(exc, (RuntimeError, OSError, subprocess.SubprocessError))
        verdict = 'INFRA_ERROR' if case.phase == 'preparation' or infrastructure else 'FAIL'
        result.update(verdict=verdict, phase=case.phase, error={'type': type(exc).__name__, 'message': message[:2000]})
    finally:
        finish(case, result)
    print(json.dumps({key: value for key, value in result.items() if key != 'objects'}, indent=2))
    return result['verdict'] != 'PASS' or result['cleanup'] != 'PASS'


if __name__ == '__main__':
    raise SystemExit(main())
