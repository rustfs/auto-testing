"""Network fault ownership, real kernel rules and negative quorum truth."""

import copy
import errno
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import network, network_gate as gate, fleet_node
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
from scripts.validation_heal import shards
import test_validation_heal as oracle


class NetworkTests(unittest.TestCase):
    def scene(self, branch='read-boundary'):
        base = oracle.HealReadQuorumBoundaryTest()
        snapshot, expected = base.scene(total=16, data=8)
        target = shards.map_target(snapshot, base.bucket, expected[0]['key'], 'data', 1)
        target = {**target, **next(row for row in target['cohort'] if row['node'] == 'node4')}
        return snapshot, target, network.plan_for(snapshot, target, branch)

    def test_both_actual_r_w_and_lock_boundaries_are_independent(self):
        for branch in network.BRANCHES:
            snapshot, target, plan = self.scene(branch)
            self.assertEqual(plan['readQuorum'], 8)
            self.assertEqual(plan['writeQuorum'], 9)
            self.assertEqual(plan['exclusiveLockQuorum'], 3)
            self.assertEqual(plan['sides'][0]['writableDrives'], 8)
            self.assertGreaterEqual(plan['sides'][0]['readableSources'], 8)
            self.assertTrue(all(side['writableDrives'] < plan['writeQuorum'] for side in plan['sides']))
            if branch in {'read-boundary', 'lock-boundary'}:
                self.assertEqual(plan['sides'][0]['readableSources'], 8)
                self.assertEqual(plan['sides'][1]['readableSources'], 7)
                self.assertFalse(plan['gates'])
                self.assertTrue(all(side['lockClients'] < 3 for side in plan['sides']))
            else:
                self.assertEqual(plan['sides'][0]['lockClients'], 3)
                self.assertTrue(plan['sides'][0]['exclusiveLockQuorumPresent'])
                self.assertEqual(plan['gates'], {'node1': [0, 1, 2, 3]})
                self.assertEqual(plan['sides'][1]['readableSources'], 3)
            for index, node in enumerate(snapshot):
                for other in plan['blocked'][node]:
                    self.assertIn(index, plan['blocked'][list(snapshot)[other]])

    def test_foreign_geometry_duplicate_slots_and_wrong_defect_member_are_rejected(self):
        for kind in ('geometry', 'duplicate', 'foreign-node', 'target-node', 'branch'):
            snapshot, target, _ = self.scene()
            branch = 'read-boundary'
            if kind == 'geometry': target['logical']['erasure']['data_blocks'] = 12
            elif kind == 'duplicate': target['cohort'][-1] = copy.deepcopy(target['cohort'][0])
            elif kind == 'foreign-node': target['cohort'][-1]['node'] = 'foreign'
            elif kind == 'target-node': target['node'] = 'node1'
            else: branch = 'invented'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                network.plan_for(snapshot, target, branch)

    def test_partial_coverage_cannot_be_converted_into_global_idle(self):
        value = {'clusterStatusComplete': False, 'state': 'degraded',
                 'coverage': {'expected': 4, 'responded': 2, 'unknown': 2, 'reasons': ['partition']}}
        network.validate_partial(value, 2)
        for kind in ('complete', 'idle', 'hidden-peer', 'no-reason', 'untyped'):
            broken = copy.deepcopy(value)
            if kind == 'complete': broken['clusterStatusComplete'] = True
            elif kind == 'idle': broken['state'] = 'idle'
            elif kind == 'hidden-peer': broken['coverage']['unknown'] = 0
            elif kind == 'no-reason': broken['coverage']['reasons'] = []
            else: broken['coverage']['responded'] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                network.validate_partial(broken, 2)

    def negative(self, state='deferred'):
        expected = [{'key': 'objects/a', 'versionId': 'v0'}, {'key': 'objects/a', 'versionId': 'v1'}]
        target = {'key': 'objects/a', 'versionId': 'v1'}
        objects = [{'identity': {'bucket': 'owned', 'object': 'objects/a', 'versionId': version,
                    'bucketIncarnationId': 'incarnation', 'poolIndex': 0, 'setIndex': 0},
                    'disposition': {'state': 'verified_healthy' if version == 'v0' else state}} for version in ('v0', 'v1')]
        failed = state == 'failed'
        current = {'summary': 'stopped' if failed else 'finished', 'truncated': False,
            'outcome': {'execution': {'state': 'completed_with_errors' if failed else 'completed'},
                'coverage': 'complete', 'objectsTruncated': False, 'objects': objects,
                'counters': {'processed': 2, 'healed': 0, 'unchanged': 1, 'failed': int(failed),
                    'skipped': int(not failed), 'unknown': int(state == 'unknown'),
                    'attemptFailures': int(failed), 'overflowed': False}}}
        return current, expected, target

    def test_failed_deferred_and_unknown_remain_unresolved_without_unsafe_repair(self):
        for state in ('failed', 'deferred', 'unknown'):
            current, expected, target = self.negative(state)
            self.assertFalse(network.validate_negative_task(current, 'owned', expected, target)['unsafeRepairClaimed'])
        current, expected, target = self.negative()
        for kind in ('healed', 'false-healthy', 'hidden-failure', 'foreign', 'duplicate', 'lost-target', 'uncounted-unknown'):
            broken = copy.deepcopy(current)
            objects, counts = broken['outcome']['objects'], broken['outcome']['counters']
            if kind == 'healed': counts['healed'] = 1
            elif kind == 'false-healthy': objects[-1]['disposition']['state'] = 'verified_healthy'
            elif kind == 'hidden-failure': objects[-1]['disposition']['state'] = 'failed'
            elif kind == 'foreign': objects[-1]['identity']['bucket'] = 'foreign'
            elif kind == 'duplicate': objects[-1] = copy.deepcopy(objects[0])
            elif kind == 'lost-target': objects.pop(); counts.update(processed=1, skipped=0)
            else: objects[-1]['disposition']['state'] = 'unknown'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                network.validate_negative_task(broken, 'owned', expected, target)

    def test_structured_abort_preserves_untraversable_responsibility_without_success(self):
        current, expected, target = self.negative()
        current['outcome'].update(execution={'state': 'aborted', 'reason': 'untraversable'}, coverage='partial', objects=[])
        current['outcome']['counters'].update(processed=0, unchanged=0, skipped=0)
        network.validate_negative_task(current, 'owned', expected, target)
        current['outcome']['coverage'] = 'complete'
        with self.assertRaises(ValueError): network.validate_negative_task(current, 'owned', expected, target)

    def test_write_identity_mismatch_stays_pending_and_no_retry_is_issued(self):
        row = {'key': 'unique', 'payload': {'size': 1, 'sha256': 'a'},
            'observed': {'acknowledged': True, 'resultUnknown': False, 'response': {'VersionId': 'ack-version'}}}
        class Fleet:
            def record(self, label, value): return value
        observed = {'acknowledged': True, 'resultUnknown': False, 'payload': row['payload'], 'response': {'VersionId': 'foreign'}}
        with patch.object(network, 'Probe') as probe, self.assertRaises(ValueError):
            probe.return_value.s3.return_value = observed
            network.resolve_writes(Fleet(), {'endpoints': {'node1': 'url', 'node2': 'url2'}}, 'bucket', [row])
        self.assertNotIn('resolution', row)

    def test_owned_address_allowlist_rejects_nonfleet_destinations(self):
        gate.addresses(['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4'])
        for address in ('127.0.0.1', '0.0.0.0', '169.254.1.1', '8.8.8.8', '224.0.0.1', '::1', '10.0.0.2'):
            with self.subTest(address=address), self.assertRaises(ValueError):
                gate.addresses([address, '10.0.0.2', '10.0.0.3', '10.0.0.4'])

    def test_ipv6_alias_cannot_bypass_the_registered_ipv4_partition(self):
        rows = [(gate.socket.AF_INET, gate.socket.SOCK_STREAM, 6, '', ('10.0.0.1', 9000)),
                (gate.socket.AF_INET6, gate.socket.SOCK_STREAM, 6, '', ('fd00::1', 9000, 0, 0))]
        with patch.object(gate.socket, 'getaddrinfo', return_value=rows), self.assertRaisesRegex(ValueError, 'unguarded IPv6'):
            gate.resolve()

    def table(self):
        table, blocked = 'rv_i03_test', ['10.0.0.2', '10.0.0.3']
        rows = []
        for entry in gate.spec(table, blocked)['nftables']:
            kind, value = next(iter(entry['add'].items()))
            value = copy.deepcopy(value)
            value['handle'] = len(rows) + 1
            if kind == 'rule':
                value['expr'][-2]['counter'] = {'packets': 3, 'bytes': 180}
            rows.append({kind: value})
        return {'nftables': [{'metainfo': {'json_schema_version': 1}}] + rows}, table, blocked

    def test_exact_rules_exclude_ssh_and_reject_foreign_expressions_before_deletion(self):
        raw, table, blocked = self.table()
        counters = gate.validate_table(raw, table, blocked)
        self.assertEqual(len(counters), 8)
        for kind in ('ssh', 'address', 'accept', 'priority', 'foreign-rule', 'missing', 'chain'):
            broken = copy.deepcopy(raw)
            rule = next(row['rule'] for row in broken['nftables'] if 'rule' in row)
            chain = next(row['chain'] for row in broken['nftables'] if 'chain' in row)
            if kind == 'ssh': rule['expr'][1]['match']['right'] = 22
            elif kind == 'address': rule['expr'][0]['match']['right'] = '10.0.0.9'
            elif kind == 'accept': rule['expr'][-1] = {'accept': None}
            elif kind == 'priority': chain['prio'] = -100
            elif kind == 'foreign-rule': rule['comment'] = 'foreign'
            elif kind == 'missing': broken['nftables'].pop()
            else: chain['hook'] = 'forward'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                gate.validate_table(broken, table, blocked)

    def test_all_peer_directions_and_packet_counter_hits_are_required(self):
        snapshot, _, plan = self.scene()
        nodes = list(snapshot)
        observed = {node: {'state': 'ACTIVE', 'localIndex': index, 'expiryArmed': True, 'process': {'pid': 123},
            'probes': [{'peerIndex': other, 'service': {'connected': other not in plan['blocked'][node]},
                       'management': {'connected': True}} for other in range(4) if other != index],
            'countersBefore': [{'packets': 0}], 'countersAfter': [{'packets': 1}]} for index, node in enumerate(nodes)}
        network.validate_witness(observed, plan, nodes)
        for kind in ('stopped-service', 'management-blocked', 'still-connected', 'missing-peer', 'no-hit', 'expired'):
            broken = copy.deepcopy(observed)
            first = broken[nodes[0]]
            if kind == 'stopped-service': first['process']['pid'] = 0
            elif kind == 'management-blocked': first['probes'][0]['management']['connected'] = False
            elif kind == 'still-connected': first['probes'][-1]['service']['connected'] = True
            elif kind == 'missing-peer': first['probes'].pop()
            elif kind == 'no-hit': first['countersAfter'][0]['packets'] = 0
            else: first['expiryArmed'] = False
            with self.subTest(kind=kind), self.assertRaises(ValueError): network.validate_witness(broken, plan, nodes)

    def test_source_branches_and_existing_dispatch_contract_remain_complete(self):
        self.assertEqual(select(network.CASE_ID)[0]['variants'], list(network.BRANCHES))
        self.assertIn(network.CASE_ID, AUTOMATIC_CASES)
        source = Path('scripts/validation_heal/run_case.sh').read_text()
        self.assertIn(network.CASE_ID + ') driver=network.py', source)


class NetworkExpiryTests(unittest.TestCase):
    owner = {'runId': 'gh-123-1-h2', 'hostname': 'node1', 'unit': 'owned.service',
             'runtimeProfile': 'lock-quorum', 'binarySha256': 'a' * 64}
    peers = ['10.17.0.1', '10.17.0.2', '10.17.0.3', '10.17.0.4']

    def prepare(self, folder, *, timer_active=True):
        calls = []
        def command(*args, input=None):
            calls.append((args, input))
            if args[:4] == ('nft', '-j', 'list', 'tables'): return '{"nftables":[]}'
            if 'LoadState' in args: return 'not-found'
            if 'ActiveState' in args: return 'active' if timer_active else 'inactive'
            return ''
        source = Path(gate.__file__).read_text()
        with patch.object(gate, 'command', side_effect=command), patch.object(gate, 'resolve', return_value=self.peers), patch.object(gate.socket, 'socket'):
            value = gate.prepare(folder, self.owner, {'addresses': self.peers, 'localIndex': 0, 'blockedIndices': [2, 3]}, source)
        return value, calls

    def test_independent_expiry_is_armed_and_intent_is_durable_before_any_rule_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            value, calls = self.prepare(state)
            self.assertEqual(value['state'], 'ARMED')
            self.assertEqual(json.loads((state / gate.MARKER).read_text())['phase'], 'armed')
            self.assertTrue((state / gate.SCRIPT).is_file())
            self.assertEqual((state / gate.MARKER).stat().st_mode & 0o777, 0o600)
            self.assertTrue(any(args[0] == 'systemd-run' for args, _ in calls))
            self.assertFalse(any(args[:4] == ('nft', '-j', '-f', '-') for args, _ in calls))

    def test_failed_timer_keeps_undo_intent_and_never_installs_rules(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            with self.assertRaisesRegex(ValueError, 'timer was not armed'):
                self.prepare(state, timer_active=False)
            self.assertTrue((state / gate.MARKER).is_file())
            self.assertEqual(json.loads((state / gate.MARKER).read_text())['phase'], 'armed')

    def test_lost_timer_refuses_activation_before_touching_nft(self):
        with tempfile.TemporaryDirectory() as folder:
            value, _ = self.prepare(Path(folder))
            with patch.object(gate, 'read_marker', return_value=value), patch.object(gate, 'table_present', return_value=False), \
                    patch.object(gate, 'command', return_value='inactive') as command, self.assertRaises(ValueError):
                gate.activate(Path(folder), self.owner)
            self.assertTrue(all(call.args[0] != 'nft' for call in command.call_args_list))

    def release_fixture(self, state, *, service_loaded=False, timer_loaded=True, stop_fails=False):
        value, _ = self.prepare(state)
        states = {'.timer': 'active' if timer_loaded else 'inactive', '.service': 'active' if service_loaded else 'inactive'}
        calls = []
        def command(*args, input=None):
            calls.append(args)
            if args[:2] == ('systemctl', 'show'):
                suffix = '.timer' if args[2].endswith('.timer') else '.service'
                loaded = timer_loaded if suffix == '.timer' else service_loaded
                if 'LoadState' in args: return 'loaded' if loaded else 'not-found'
                return states[suffix]
            if args[:2] == ('systemctl', 'stop'):
                for unit in args[2:]:
                    suffix = '.timer' if unit.endswith('.timer') else '.service'
                    if suffix == '.service' and not service_loaded:
                        raise ValueError('systemctl network operation failed: Unit expiry.service not loaded')
                    if stop_fails: raise ValueError('systemctl network operation failed: stop refused')
                    states[suffix] = 'inactive'
            return ''
        return value, command, calls

    def test_release_stops_timer_when_its_service_has_never_been_loaded(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            value, command, calls = self.release_fixture(state)
            with patch.object(gate, 'read_marker', return_value=value), patch.object(gate, 'command', side_effect=command), patch.object(gate, 'table_present', return_value=False):
                result = gate.release(state, self.owner)
            self.assertEqual(result['rulesRemaining'], 0)
            self.assertFalse(result['expiryTriggered'])
            self.assertIn(('systemctl', 'stop', value['timer'] + '.timer'), calls)
            self.assertFalse(any(value['timer'] + '.service' in args for args in calls if args[:2] == ('systemctl', 'stop')))
            self.assertFalse((state / gate.MARKER).exists())

    def test_release_stops_an_active_worker_after_stopping_its_timer(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            value, command, calls = self.release_fixture(state, service_loaded=True)
            with patch.object(gate, 'read_marker', return_value=value), patch.object(gate, 'command', side_effect=command), patch.object(gate, 'table_present', return_value=False):
                gate.release(state, self.owner)
            stops = [args[2:] for args in calls if args[:2] == ('systemctl', 'stop')]
            self.assertEqual(stops, [(value['timer'] + '.timer',), (value['timer'] + '.service',)])

    def test_missing_normal_timer_or_failed_stop_preserves_intent_and_rules(self):
        for config in ({'timer_loaded': False}, {'stop_fails': True}):
            with self.subTest(config=config), tempfile.TemporaryDirectory() as folder:
                state = Path(folder)
                value, command, calls = self.release_fixture(state, **config)
                with patch.object(gate, 'read_marker', return_value=value), patch.object(gate, 'command', side_effect=command), patch.object(gate, 'table_present', return_value=True), self.assertRaises(ValueError):
                    gate.release(state, self.owner)
                self.assertTrue((state / gate.MARKER).exists())
                self.assertFalse(any(args[:4] == ('nft', '-j', '-f', '-') for args in calls))

    def test_explicit_recovery_accepts_unloaded_timer_and_stops_any_remaining_worker(self):
        for service_loaded in (False, True):
            with self.subTest(service_loaded=service_loaded), tempfile.TemporaryDirectory() as folder:
                state = Path(folder)
                value, command, calls = self.release_fixture(state, timer_loaded=False, service_loaded=service_loaded)
                with patch.object(gate, 'read_marker', return_value=value), patch.object(gate, 'command', side_effect=command), patch.object(gate, 'table_present', return_value=False):
                    result = gate.release(state, self.owner, expired=True)
                self.assertTrue(result['expiryTriggered'])
                self.assertEqual(result['rulesRemaining'], 0)
                self.assertTrue((state / gate.EXPIRY).is_file())
                self.assertFalse((state / gate.MARKER).exists())
                self.assertFalse(any(value['timer'] + '.timer' in args for args in calls if args[:2] == ('systemctl', 'stop')))

    def test_foreign_rule_blocks_cleanup_instead_of_deleting_the_table(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            value, _ = self.prepare(state)
            broken = {'nftables': [{'table': {'family': 'inet', 'name': value['table']}},
                {'rule': {'family': 'inet', 'table': value['table'], 'chain': 'input',
                          'comment': 'foreign', 'expr': [{'accept': None}]}}]}
            def command(*args, input=None):
                if args[:4] == ('nft', '-j', 'list', 'table'): return json.dumps(broken)
                if 'LoadState' in args: return 'loaded'
                if 'ActiveState' in args: return 'inactive'
                return ''
            with patch.object(gate, 'read_marker', return_value=value), patch.object(gate, 'table_present', return_value=True), \
                    patch.object(gate, 'command', side_effect=command) as observed, self.assertRaises(ValueError):
                gate.release(state, self.owner)
            self.assertTrue((state / gate.MARKER).exists())
            self.assertFalse(any(call.args[:4] == ('nft', '-j', '-f', '-') for call in observed.call_args_list))


class NetworkKernelTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'linux', 'requires an isolated Linux network namespace')
    def test_real_nft_json_round_trip_and_foreign_rule_refusal(self):
        self.assertIsNotNone(shutil.which('nft'), 'Linux CI must install nftables; this gate may not skip silently')
        program = '''import copy,json
from scripts.validation_heal import network_gate as gate
table='rv_i03_kernel_test'
peers=['10.17.0.2','10.17.0.3']
spec=gate.spec(table,peers)
gate.command('nft','-j','-c','-f','-',input=json.dumps(spec))
gate.command('nft','-j','-f','-',input=json.dumps(spec))
raw=json.loads(gate.command('nft','-j','list','table','inet',table))
assert len(gate.validate_table(raw,table,peers))==8
foreign={'nftables':[{'add':{'rule':{'family':'inet','table':table,'chain':'input','expr':[{'accept':None}]}}}]}
gate.command('nft','-j','-f','-',input=json.dumps(foreign))
try:
    gate.validate_table(json.loads(gate.command('nft','-j','list','table','inet',table)),table,peers)
except ValueError:
    pass
else:
    raise AssertionError('foreign rule was accepted')
gate.command('nft','-j','-f','-',input=json.dumps({'nftables':[{'delete':{'table':{'family':'inet','name':table}}}]}))
assert not gate.table_present(table)
print('PASS: actual private-network kernel rules and deletion guards')
'''
        command = ['unshare', '--net', sys.executable, '-B', '-c', program]
        if os.geteuid() != 0: command = ['sudo', '-n', *command]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('PASS: actual private-network kernel rules', result.stdout)


if __name__ == '__main__':
    unittest.main()
