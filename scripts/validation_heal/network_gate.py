"""I03: one owned nftables table, with a pre-armed independent expiry timer."""

from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys


MARKER = 'network-gate.json'
SCRIPT = 'network-gate-expiry.py'
EXPIRY = 'network-gate-expired.json'
TTL_SECONDS = 1200
PORT = 9000


def require(value, message):
    if not value:
        raise ValueError(message)


def command(*args, input=None):
    result = subprocess.run(args, input=input, capture_output=True, text=True, timeout=30)
    require(result.returncode == 0, f'{args[0]} network operation failed: {result.stderr[-600:]}')
    return result.stdout


def private_file(path, limit=128 * 1024):
    info = path.lstat()
    require(not path.is_symlink() and stat.S_ISREG(info.st_mode) and info.st_uid == 0
            and info.st_nlink == 1 and stat.S_IMODE(info.st_mode) == 0o600 and info.st_size <= limit,
            'network artifact is not a bounded private root-owned regular file')
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        before = os.fstat(stream.fileno())
        raw = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    require(len(raw) == before.st_size <= limit and all(getattr(before, name) == getattr(after, name)
            for name in ('st_ino', 'st_dev', 'st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_nlink')),
            'network artifact changed while reading')
    return raw


def write(path, value, *, replace=False):
    target = path.with_suffix('.pending') if replace else path
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(value if isinstance(value, bytes) else json.dumps(value).encode())
        stream.flush()
        os.fsync(stream.fileno())
    if replace:
        os.replace(target, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def identities(owner):
    require(re.fullmatch(r'gh-[1-9][0-9]*-[1-9][0-9]*-h2', owner.get('runId', ''))
            and owner.get('runtimeProfile') == 'lock-quorum', 'network fault requires its exact H2 EC:8 service')
    suffix = hashlib.sha256((owner['runId'] + '/' + owner['hostname']).encode()).hexdigest()[:24]
    return 'rv_i03_' + suffix, 'rv-i03-expiry-' + suffix


def addresses(values):
    require(isinstance(values, list) and len(values) == len(set(values)) == 4,
            'network topology requires four distinct IPv4 addresses')
    for value in values:
        address = ipaddress.ip_address(value)
        require(address.version == 4 and str(address) == value and address.is_private
                and not (address.is_loopback or address.is_unspecified or address.is_link_local or address.is_multicast),
                'network target must be a canonical private unicast fleet address')
    return values


def resolve():
    values = []
    for index in range(1, 5):
        rows = socket.getaddrinfo(f'rustfs-node{index}', PORT, socket.AF_UNSPEC, socket.SOCK_STREAM)
        require(rows and all(row[0] == socket.AF_INET for row in rows),
                'peer alias exposes an unguarded IPv6 path; no partition will be installed')
        found = {row[4][0] for row in rows}
        require(len(found) == 1, 'peer alias has ambiguous IPv4 resolution')
        values.append(found.pop())
    return addresses(values)


def spec(table, blocked):
    result = [{'add': {'table': {'family': 'inet', 'name': table}}}]
    for chain in ('input', 'output'):
        result.append({'add': {'chain': {'family': 'inet', 'table': table, 'name': chain,
            'type': 'filter', 'hook': chain, 'prio': -10, 'policy': 'accept'}}})
        for peer in blocked:
            for field in ('sport', 'dport'):
                result.append({'add': {'rule': {'family': 'inet', 'table': table, 'chain': chain,
                    'comment': f'{table}:{chain}:{peer}:{field}', 'expr': [
                        # IP/TCP payload selectors imply their protocol guards.
                        # nft removes redundant explicit meta guards on export.
                        {'match': {'op': '==', 'left': {'payload': {'protocol': 'ip',
                            'field': 'saddr' if chain == 'input' else 'daddr'}}, 'right': peer}},
                        {'match': {'op': '==', 'left': {'payload': {'protocol': 'tcp', 'field': field}}, 'right': PORT}},
                        {'counter': None}, {'drop': None}]}}})
    return {'nftables': result}


def table_present(table):
    values = json.loads(command('nft', '-j', 'list', 'tables'))['nftables']
    return any(row.get('table', {}).get('family') == 'inet' and row['table'].get('name') == table for row in values if 'table' in row)


def validate_table(value, table, blocked):
    expected = spec(table, blocked)['nftables']
    chains = {row['add']['chain']['name']: row['add']['chain'] for row in expected if 'chain' in row['add']}
    wanted = {row['add']['rule']['comment']: row['add']['rule'] for row in expected if 'rule' in row['add']}
    seen_chains, seen_rules, tables, counters = set(), set(), 0, []
    for row in value.get('nftables', []):
        if 'metainfo' in row:
            continue
        require(len(row) == 1 and next(iter(row)) in {'table', 'chain', 'rule'}, 'foreign network table object')
        kind, item = next(iter(row.items()))
        require(item.get('family') == 'inet' and (item.get('name') if kind == 'table' else item.get('table')) == table,
                'network table identity changed')
        if kind == 'table':
            require(set(item) <= {'family', 'name', 'handle'} and tables == 0, 'network table flags or ownership changed')
            tables += 1
        elif kind == 'chain':
            name = item.get('name')
            require(name in chains and name not in seen_chains and {k: v for k, v in item.items() if k != 'handle'} == chains[name],
                    'network chain has an unexpected hook, priority or policy')
            seen_chains.add(name)
        else:
            name = item.get('comment')
            require(name in wanted and name not in seen_rules, 'foreign or duplicate network rule')
            normalized = dict(item)
            normalized.pop('handle', None)
            normalized.pop('index', None)
            expressions = []
            for expression in normalized.get('expr', []):
                if 'counter' in expression:
                    count = expression['counter']
                    require(set(expression) == {'counter'} and isinstance(count, dict) and set(count) == {'packets', 'bytes'}
                            and all(type(count[k]) is int and count[k] >= 0 for k in count), 'invalid network packet counter')
                    counters.append({'rule': name, **count})
                    expressions.append({'counter': None})
                else:
                    expressions.append(expression)
            normalized['expr'] = expressions
            require(normalized == wanted[name], 'network rule changed its addresses, TCP port or drop expression: '
                    + json.dumps({'observed': normalized, 'expected': wanted[name]}, sort_keys=True))
            seen_rules.add(name)
    require(tables == 1 and seen_chains == set(chains) and seen_rules == set(wanted)
            and len(counters) == len(wanted), 'network table omits a registered rule or chain')
    return counters


def read_marker(state, owner):
    if not (state / MARKER).exists():
        return None
    value = json.loads(private_file(state / MARKER))
    table, timer = identities(owner)
    values = addresses(value.get('addresses'))
    local, blocked = value.get('localIndex'), value.get('blockedIndices')
    require(type(local) is int and 0 <= local < 4 and isinstance(blocked, list) and blocked == sorted(set(blocked))
            and 0 < len(blocked) <= 3 and all(type(i) is int and 0 <= i < 4 and i != local for i in blocked)
            and value.get('runId') == owner['runId'] and value.get('hostname') == owner['hostname']
            and value.get('binarySha256') == owner['binarySha256'] and value.get('unit') == owner['unit']
            and value.get('schemaVersion') == 1 and value.get('table') == table and value.get('timer') == timer
            and value.get('phase') in {'armed', 'active'} and value.get('ttlSeconds') == TTL_SECONDS
            and value.get('spec') == spec(table, [values[i] for i in blocked])
            and value.get('scriptSha256') == hashlib.sha256(private_file(state / SCRIPT)).hexdigest(),
            'network ownership, exact rules or expiry script changed; retain namespace')
    require(datetime.fromisoformat(value['expiresAt']).tzinfo is not None, 'network timer expiry has no timezone')
    return value


def prepare(state, owner, request, source):
    table, timer = identities(owner)
    values = addresses(request.get('addresses'))
    local, blocked = request.get('localIndex'), request.get('blockedIndices')
    require(values == resolve() and type(local) is int and 0 <= local < 4
            and isinstance(blocked, list) and blocked == sorted(set(blocked)) and 0 < len(blocked) <= 3
            and all(type(i) is int and 0 <= i < 4 and i != local for i in blocked), 'network peer binding differs')
    # Binding a socket to this address is an independent ownership check.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test:
        test.bind((values[local], 0))
    require(not (state / MARKER).exists() and not (state / SCRIPT).exists() and not (state / EXPIRY).exists()
            and not table_present(table), 'network namespace or table is already occupied')
    for suffix in ('.timer', '.service'):
        require(command('systemctl', 'show', timer + suffix, '-p', 'LoadState', '--value').strip() == 'not-found',
                'network expiry unit is already occupied')
    command('nft', '-j', '-c', '-f', '-', input=json.dumps(spec(table, [values[i] for i in blocked])))
    write(state / SCRIPT, source.encode())
    value = {'schemaVersion': 1, 'runId': owner['runId'], 'hostname': owner['hostname'],
        'unit': owner['unit'], 'binarySha256': owner['binarySha256'], 'table': table, 'timer': timer,
        'addresses': values, 'localIndex': local, 'blockedIndices': blocked, 'ttlSeconds': TTL_SECONDS,
        'expiresAt': (datetime.now(timezone.utc) + timedelta(seconds=TTL_SECONDS)).isoformat(), 'phase': 'armed',
        'scriptSha256': hashlib.sha256(source.encode()).hexdigest(), 'spec': spec(table, [values[i] for i in blocked])}
    write(state / MARKER, value)
    command('systemd-run', '--quiet', '--unit=' + timer, '--on-active=' + str(TTL_SECONDS) + 's',
            '--timer-property=AccuracySec=1s', '--property=RemainAfterExit=yes', '/usr/bin/python3', str(state / SCRIPT), str(state))
    require(command('systemctl', 'show', timer + '.timer', '-p', 'ActiveState', '--value').strip() == 'active',
            'network expiry timer was not armed before mutation')
    return {**value, 'state': 'ARMED', 'rulesApplied': False, 'expiryArmedBeforeMutation': True}


def activate(state, owner):
    value = read_marker(state, owner)
    require(value and value['phase'] == 'armed' and not table_present(value['table'])
            and not (state / EXPIRY).exists()
            and datetime.now(timezone.utc) < datetime.fromisoformat(value['expiresAt'])
            and command('systemctl', 'show', value['timer'] + '.timer', '-p', 'ActiveState', '--value').strip() == 'active',
            'network activation requires its still-armed independent timer')
    # nft's whole transaction creates only this table; it never flushes rules.
    command('nft', '-j', '-f', '-', input=json.dumps(value['spec']))
    counters = validate_table(json.loads(command('nft', '-j', 'list', 'table', 'inet', value['table'])),
        value['table'], [value['addresses'][i] for i in value['blockedIndices']])
    value['phase'] = 'active'
    write(state / MARKER, value, replace=True)
    return {'state': 'ACTIVE', 'table': value['table'], 'counters': counters,
            'onlyExactFleetTCP9000': True, 'unrelatedTablesChanged': False}


def observe(state, owner):
    value = read_marker(state, owner)
    require(value and value['phase'] == 'active' and not (state / EXPIRY).exists()
            and datetime.now(timezone.utc) < datetime.fromisoformat(value['expiresAt'])
            and command('systemctl', 'show', value['timer'] + '.timer', '-p', 'ActiveState', '--value').strip() == 'active',
            'network partition expired or lost its independent recovery timer')
    counters = validate_table(json.loads(command('nft', '-j', 'list', 'table', 'inet', value['table'])),
        value['table'], [value['addresses'][i] for i in value['blockedIndices']])
    probes = []
    for index, address in enumerate(value['addresses']):
        if index == value['localIndex']:
            continue
        row = {'peerIndex': index, 'peerAddress': address}
        for port, label in ((PORT, 'service'), (22, 'management')):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.settimeout(1)
                client.bind((value['addresses'][value['localIndex']], 0))
                try:
                    client.connect((address, port))
                except OSError as exc:
                    row[label] = {'connected': False, 'error': type(exc).__name__, 'errno': exc.errno}
                else:
                    row[label] = {'connected': True}
        require(row['service']['connected'] is (index not in value['blockedIndices'])
                and row['management']['connected'] is True,
                'actual service isolation or preserved management connectivity differs')
        probes.append(row)
    after = validate_table(json.loads(command('nft', '-j', 'list', 'table', 'inet', value['table'])),
        value['table'], [value['addresses'][i] for i in value['blockedIndices']])
    require(sum(row['packets'] for row in after) > sum(row['packets'] for row in counters),
            'network isolation did not hit a real owned packet rule')
    return {'state': 'ACTIVE', 'localIndex': value['localIndex'], 'table': value['table'],
            'probes': probes, 'countersBefore': counters, 'countersAfter': after, 'managementPort': 22,
            'allServicesRemainRunning': True, 'expiryArmed': True}


def release(state, owner, *, expired=False):
    value = read_marker(state, owner)
    if value is None:
        table, _ = identities(owner)
        require(not table_present(table), 'network table has no ownership intent; preserve namespace')
        return {'state': 'CLEAR', 'gateWasAbsent': True}
    expired = expired or (state / EXPIRY).exists()
    if expired:
        for suffix in ('.timer', '.service'):
            require(command('systemctl', 'show', value['timer'] + suffix, '-p', 'LoadState', '--value').strip()
                    in {'not-found', 'loaded'}, 'expired network unit identity is unknown')
            require(command('systemctl', 'show', value['timer'] + suffix, '-p', 'ActiveState', '--value').strip()
                    in {'inactive', 'failed', 'unknown'}, 'expired network unit remains active')
    # Stop the expiry worker before inspecting/deleting, avoiding a concurrent
    # deletion. A TTL worker does not stop its own unit.
    if not expired:
        command('systemctl', 'stop', value['timer'] + '.timer', value['timer'] + '.service')
    counters = []
    if table_present(value['table']):
        counters = validate_table(json.loads(command('nft', '-j', 'list', 'table', 'inet', value['table'])),
            value['table'], [value['addresses'][i] for i in value['blockedIndices']])
        command('nft', '-j', '-f', '-', input=json.dumps({'nftables': [{'delete': {'table': {
            'family': 'inet', 'name': value['table']}}}]}))
    require(not table_present(value['table']), 'owned network table was not removed')
    receipt = {'state': 'CLEAR', 'table': value['table'], 'rulesRemaining': 0,
        'removedCounters': counters, 'expiryTriggered': expired or (state / EXPIRY).exists(),
        'unrelatedTablesChanged': False}
    if expired:
        if not (state / EXPIRY).exists():
            write(state / EXPIRY, receipt)
        (state / MARKER).unlink()
        (state / SCRIPT).unlink()
    else:
        for suffix in ('.timer', '.service'):
            require(command('systemctl', 'show', value['timer'] + suffix, '-p', 'ActiveState', '--value').strip() in {'inactive', 'failed'},
                    'network expiry unit remains active')
        (state / MARKER).unlink()
        (state / SCRIPT).unlink()
    return receipt


def expiry_main():
    require(os.geteuid() == 0 and len(sys.argv) == 2, 'root-owned expiry invocation required')
    state = Path(sys.argv[1])
    require(state.is_absolute() and state.parent == Path('/var/lib/rustfs-auto-testing/heal')
            and re.fullmatch(r'gh-[1-9][0-9]*-[1-9][0-9]*-h2', state.name), 'expiry namespace is not a registered H2 run')
    for entry in (state, *state.parents):
        require(not entry.is_symlink() and stat.S_ISDIR(entry.stat().st_mode) and entry.stat().st_uid == 0,
                'expiry namespace ownership changed')
    # owner.json is created by the existing node reserve path (mode 0644).
    path = state / 'owner.json'
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1 and info.st_size < 65536
            and info.st_mode & 0o022 == 0 and not path.is_symlink(), 'expiry owner changed')
    owner = json.loads(path.read_text())
    require(owner['runId'] == state.name and owner['hostname'] == socket.gethostname(), 'expiry owner differs from actual host')
    print(json.dumps(release(state, owner, expired=True)))


if __name__ == '__main__':
    expiry_main()
