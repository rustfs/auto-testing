"""Read the exact caller socket from one run-owned Linux candidate process."""

import ipaddress
import os
from pathlib import Path
import re
import socket
import struct
import time


def require(value, message):
    if not value:
        raise ValueError(message)


def address(value):
    require(isinstance(value, list) and len(value) == 2 and isinstance(value[0], str)
            and type(value[1]) is int and 0 < value[1] < 65536, 'exact numeric TCP endpoint required')
    ip = ipaddress.ip_address(value[0])
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return str(ip), value[1]


def proc_address(value, ipv6):
    parts = value.split(':')
    require(len(parts) == 2 and re.fullmatch(r'[0-9A-Fa-f]{32}' if ipv6 else r'[0-9A-Fa-f]{8}', parts[0])
            and re.fullmatch(r'[0-9A-Fa-f]{4}', parts[1]), 'invalid Linux TCP address')
    packed = b''.join(struct.pack('=I', int(parts[0][offset:offset + 8], 16))
                      for offset in range(0, len(parts[0]), 8))
    ip = ipaddress.ip_address(socket.inet_ntop(socket.AF_INET6 if ipv6 else socket.AF_INET, packed))
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return str(ip), int(parts[1], 16)


def connections(raw, ipv6, client, server):
    require(isinstance(raw, str) and len(raw.encode()) <= 4 * 1024**2, 'bounded Linux TCP table required')
    lines = raw.splitlines()
    require(lines and 'local_address' in lines[0] and 'inode' in lines[0], 'Linux TCP table header differs')
    found = []
    for line in lines[1:]:
        fields = line.split()
        require(len(fields) >= 10 and fields[9].isdigit() and re.fullmatch(r'[0-9A-Fa-f]{2}', fields[3]),
                'incomplete Linux TCP connection row')
        local, remote = proc_address(fields[1], ipv6), proc_address(fields[2], ipv6)
        if local == server and remote == client:
            found.append({'inode': int(fields[9]), 'state': fields[3].upper()})
    require(len(found) <= 1, 'ambiguous exact TCP connection in network namespace')
    return found


def run(request, node):
    require(set(request) == {'runId', 'action', 'expectedProcess', 'clientAddress', 'serverAddress'}
            and request['action'] == 'caller-socket', 'caller socket accepts no arbitrary PID or path')
    client, server = address(request['clientAddress']), address(request['serverAddress'])
    require(server[1] == 9000, 'caller socket requires the bound service port')
    _, _, owner = node['owned'](request['runId'])
    require(owner.get('runtimeProfile') == 'lock-timeout', 'caller socket requires the isolated I05 runtime')
    expected = request['expectedProcess']
    require(isinstance(expected, dict) and all(name in expected for name in node['PROCESS_FIELDS']),
            'caller socket requires the original process birth')
    def process():
        observed = node['process'](request)
        require(type(observed.get('pid')) is int and observed['pid'] > 1
                and all(observed.get(name) == expected[name] for name in node['PROCESS_FIELDS']),
                'caller socket crossed a process birth or runtime change')
        return observed
    before = process()
    root = Path(f'/proc/{before["pid"]}')
    network_namespace = os.readlink(root / 'ns/net')
    def sample():
        started = time.monotonic_ns()
        fds, vanished, owned_sockets = list((root / 'fd').iterdir()), 0, {}
        require(len(fds) <= 65536, 'owned process descriptor census exceeds its budget')
        for fd in fds:
            try:
                target = os.readlink(fd)
            except FileNotFoundError:
                vanished += 1
                continue
            match = re.fullmatch(r'socket:\[([0-9]+)\]', target)
            if match:
                owned_sockets.setdefault(int(match[1]), []).append(int(fd.name))
        matches = []
        for table, ipv6 in (('tcp', False), ('tcp6', True)):
            # /proc/PID/net describes the namespace, not process ownership.
            # Only a matching descriptor inode establishes the latter.
            with (root / 'net' / table).open() as stream:
                raw = stream.read(4 * 1024**2 + 1)
            for row in connections(raw, ipv6, client, server):
                if row['inode'] in owned_sockets:
                    matches.append({**row, 'descriptors': sorted(owned_sockets[row['inode']]), 'table': table})
        require(len(matches) <= 1, 'caller TCP connection maps to ambiguous owned sockets')
        process()
        require(os.readlink(root / 'ns/net') == network_namespace, 'caller socket network namespace changed')
        return {'startedMonotonicNs': started, 'finishedMonotonicNs': time.monotonic_ns(),
                'fileDescriptors': len(fds), 'vanishedDescriptors': vanished, 'ownedMatches': matches}
    samples = [sample(), sample()]
    process()
    return {'process': {name: before[name] for name in node['PROCESS_FIELDS']}, 'clockDomain': before['bootId'],
            'clientAddress': list(client), 'serverAddress': list(server), 'samples': samples,
            'networkNamespace': network_namespace,
            'readOnly': True, 'continuousAbsenceProven': False, 'canonicalCasePassClaimed': False}
