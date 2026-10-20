"""Frozen race write bandwidth control on one owned unit and its fourth disk."""
from pathlib import Path
import re

RATE = '65536'
CAP_BRANCHES = {
    'heal.admin-nolock-safety': ('overwrite-nolock', 'delete-nolock'),
    'heal.race-unversioned-overwrite': ('overwrite-vs-deep', 'overwrite-vs-read-repair'),
    'heal.race-versioned-put': ('new-version-vs-historical-heal', 'read-all-versions-during-heal'),
    'heal.race-version-delete': ('delete-old-version-vs-deep', 'delete-current-version-vs-deep'),
    'heal.race-markers-and-batch-delete': ('create-marker', 'delete-marker', 'batch-version-delete', 'unversioned-delete'),
    'heal.race-copy-and-metadata-update': ('copy-source', 'copy-destination', 'replace-metadata', 'update-tags'),
}


def require(value, message):
    if not value:
        raise ValueError(message)


def limits(raw):
    require(isinstance(raw, str) and len(raw) <= 65536, 'bounded cgroup IO evidence required')
    out = {}
    for line in raw.splitlines():
        parts = line.split()
        require(len(out) < 64 and len(parts) == 5 and re.fullmatch(r'[0-9]+:[0-9]+', parts[0])
                and parts[0] not in out, 'invalid or duplicate cgroup IO device')
        values = {}
        for item in parts[1:]:
            pair = item.split('=')
            require(len(pair) == 2 and pair[0] in {'rbps', 'wbps', 'riops', 'wiops'} and pair[0] not in values
                    and (pair[1] == 'max' or re.fullmatch(r'[1-9][0-9]{0,19}', pair[1])),
                    'invalid or duplicate cgroup IO limit')
            values[pair[0]] = pair[1]
        out[parts[0]] = values
    return out


def run(request, node):
    require(set(request) == {'runId', 'action', 'caseId', 'branch', 'enabled', 'expectedProcess'}
            and request['action'] == 'race-io-cap' and type(request['enabled']) is bool
            and request['branch'] in CAP_BRANCHES.get(request['caseId'], ()),
            'write cap requires a registered frozen public race; G05 forbids this cap')
    state, roots, owner = node['owned'](request['runId'])
    require(owner.get('runtimeProfile') == 'manual' and len(roots) == len(node['MOUNTS']) == 4,
            'write cap requires the original four-drive manual owner')
    expected = request['expectedProcess']
    def process():
        actual = node['process'](request)
        require(isinstance(expected, dict) and type(actual.get('pid')) is int and actual['pid'] > 1
                and all(k in expected and actual.get(k) == expected[k] for k in node['PROCESS_FIELDS']),
                'write cap refuses a changed process birth or candidate')
        require(actual['unit'] == owner['unit'] and actual['binarySha256'] == owner['binarySha256'],
                'write cap process escaped its original owner')
        return actual
    original = process()
    require(node['mounts']() == owner['mounts'], 'write cap original mount identities changed')
    target = owner['mounts'][3]
    require(target['target'] == str(node['MOUNTS'][3]) and re.fullmatch(r'[0-9]+:[0-9]+', target['maj:min']),
            'write cap must target the original fourth physical disk')
    group = node['command']('systemctl', 'show', '--value', '-p', 'ControlGroup', owner['unit']).stdout.strip()
    require(group == '/system.slice/' + owner['unit'] and '/' not in owner['unit'],
            'write cap requires the exact owned systemd cgroup')
    cgroup = Path('/sys/fs/cgroup') / group.lstrip('/')
    def scope():
        systemd_group = node['command']('systemctl', 'show', '--value', '-p', 'ControlGroup', owner['unit']).stdout.strip()
        membership = node['command']('cat', f'/proc/{original["pid"]}/cgroup').stdout
        require(systemd_group == group and
                [line for line in membership.splitlines() if line.startswith('0:')] == ['0::' + group],
                'write cap process or unit moved to another cgroup')
        return {'systemdControlGroup': systemd_group, 'processCgroup': membership}
    scope()
    def evidence():
        value = node['command']('cat', str(cgroup / 'io.max'), check=False)
        if value.returncode:
            # A fresh unthrottled unit need not have its IO controller enabled.
            # Verify the group still exists; any other cat error remains fatal.
            require(value.returncode == 1 and 'No such file or directory' in value.stderr
                    and node['command']('test', '-d', str(cgroup), check=False).returncode == 0,
                    'owned cgroup IO limits could not be read')
            return '', {}
        return value.stdout, limits(value.stdout)
    raw_before, before = evidence()
    device = target['maj:min']
    defaults = dict.fromkeys(('rbps','wbps','riops','wiops'), 'max')
    prior = before.get(device, defaults)
    require(all(v == {**defaults, 'wbps': RATE if d == device and not request['enabled'] else 'max'}
                for d,v in before.items()) and (request['enabled'] or prior['wbps'] == RATE),
            'write cap refuses an existing foreign limit or an unbound release')
    process()
    scope_before = scope()
    node['command']('systemctl', 'set-property', '--runtime', owner['unit'],
                    'IOWriteBandwidthMax=' + (target['target'] + ' ' + RATE if request['enabled'] else ''))
    raw_after, after = evidence()
    wanted = {**defaults, 'wbps': RATE if request['enabled'] else 'max'}
    require(after.get(device, defaults) == wanted and
            all(v == (wanted if d == device else defaults) for d,v in after.items()),
            'kernel cgroup IO write cap did not match the exact original device/rate')
    process()
    scope_after = scope()
    require(node['owned'](request['runId']) == (state, roots, owner) and node['mounts']() == owner['mounts'],
            'write cap owner or mount changed during control')
    return {'enabled': request['enabled'], 'writeBytesPerSecond': int(RATE) if request['enabled'] else None,
            'disk': 3, 'device': device, 'mount': target['target'], 'controlGroup': group,
            'beforeIoMax': raw_before, 'afterIoMax': raw_after, 'process': original,
            'beforeScope': scope_before, 'afterScope': scope_after,
            'actualKernelLimitVerified': True, 'publicOverlapProved': False, 'canonicalCasePassClaimed': False}


def set_fleet(fleet, case_id, branch, enabled, *, nodes):
    require(type(enabled) is bool and branch in CAP_BRANCHES.get(case_id, ())
            and isinstance(nodes, list) and nodes and len(set(nodes)) == len(nodes)
            and set(nodes) <= set(fleet.nodes),
            'write cap is not part of this frozen public race')
    for host in nodes:
        process = fleet.record('race-io-process', {'node': host, **fleet.remote(host, 'process')})
        expected = {key: value for key, value in process.items() if key != 'node'}
        request = {'caseId': case_id, 'branch': branch, 'enabled': enabled, 'expectedProcess': expected}
        fleet.record('race-io-cap-intent', {'node': host, **request})
        fleet.record('race-io-cap-applied', {'node': host, **fleet.remote(host, 'race-io-cap', **request)})
