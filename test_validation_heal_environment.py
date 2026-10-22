import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import environment
from scripts.validation_heal import fleet_node
from scripts.validation_heal.healthy import Fleet


class EnvironmentTests(unittest.TestCase):
    def fixture(self, count=4):
        return {f'n{node}': {'hostname': f'actual-host-{node}', 'inspectionOnly': True, 'mutated': False,
            'canonicalPassClaimed': False, 'mounts': [{'target': f'/data/rustfs{number}', 'mounted': number <= count,
                'independentBlockDeviceVerified': number <= count, 'writableMount': True,
                'physicalDevice': f'8:{number}', 'availableBytes': 4 * 1024**3,
                'mount': {'uuid': f'uuid-{node}-{number}', 'target': f'/data/rustfs{number}', 'fstype': 'xfs'}}
                for number in range(1, 9)]} for node in range(4)}

    def test_four_current_mounts_prove_resources_for_h3_and_do_not_invent_h4(self):
        result = environment.summarize(self.fixture())
        self.assertEqual(result['independentEligibleEndpoints'], 16)
        self.assertTrue(result['resourceShapes']['H3']['mountsAvailable'])
        self.assertFalse(result['resourceShapes']['H4']['mountsAvailable'])
        self.assertFalse(result['actualErasureLayoutVerified'])
        self.assertFalse(result['canonicalPassClaimed'])
        self.assertFalse(result['resourceShapes']['H6']['platformRestartAuthorityVerified'])

    def test_eight_independent_mounts_are_only_a_resource_shape_not_live_ec84_proof(self):
        result = environment.summarize(self.fixture(8))
        self.assertEqual(result['independentEligibleEndpoints'], 32)
        self.assertTrue(result['resourceShapes']['H4']['mountsAvailable'])
        self.assertEqual(result['resourceShapes']['H4']['requiredEndpoints'], 24)
        self.assertFalse(result['faultBackendVerified'])
        self.assertFalse(result['compatibleReleasePairVerified'])

    def test_partition_aliases_missing_endpoints_and_shared_filesystem_identities_fail(self):
        for kind in ('partition', 'endpoint', 'shared', 'host', 'mutated', 'pass'):
            observed = self.fixture()
            if kind == 'partition': observed['n0']['mounts'][1]['physicalDevice'] = '8:1'
            elif kind == 'endpoint': observed['n0']['mounts'][-1] = copy.deepcopy(observed['n0']['mounts'][0])
            elif kind == 'shared': observed['n1']['mounts'][0]['mount']['uuid'] = observed['n0']['mounts'][0]['mount']['uuid']
            elif kind == 'host': observed['n1']['hostname'] = observed['n0']['hostname']
            elif kind == 'mutated': observed['n0']['mutated'] = True
            else: observed['n0']['canonicalPassClaimed'] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError): environment.summarize(observed)

    def test_directories_stacked_devices_readonly_mounts_or_low_space_do_not_count(self):
        for kind in ('directory', 'stacked', 'readonly', 'space', 'fs'):
            observed = self.fixture()
            row = observed['n0']['mounts'][0]
            if kind == 'directory': row['mounted'] = False
            elif kind == 'stacked': row['independentBlockDeviceVerified'] = False
            elif kind == 'readonly': row['writableMount'] = False
            elif kind == 'space': row['availableBytes'] = 1024
            else: row['mount']['fstype'] = 'tmpfs'
            result = environment.summarize(observed)
            self.assertEqual(result['independentEligibleEndpoints'], 15)
            self.assertFalse(result['resourceShapes']['H1']['mountsAvailable'])

    def test_current_remote_adapter_includes_a_compilable_readonly_inventory_action(self):
        def invoke(argv, **params):
            source = params['input']
            compile(source, 'remote-inventory', 'exec')
            self.assertIn('environment_capabilities', source)
            self.assertIn('environment-capabilities', source)
            return types.SimpleNamespace(returncode=0, stdout='{"inspectionOnly":true,"mutated":false}', stderr='')
        with tempfile.TemporaryDirectory() as name, patch('scripts.validation_heal.healthy.subprocess.run', side_effect=invoke):
            fleet = Fleet(['n0'], Path(name), 'gh-1234-1-h2')
            self.assertFalse(fleet.remote('n0', 'environment-capabilities')['mutated'])

    def runtime_command(self, *, units='', listeners='', state=None):
        state = state or 'LoadState=loaded\nActiveState=active\nSubState=running\nMainPID=42\n'
        def invoke(*args, **params):
            if args[:2] == ('systemctl', 'show'): output = state
            elif args[:2] == ('systemctl', 'list-units'): output = units
            elif args[0] == 'ss': output = listeners
            else: self.fail(f'Unexpected runtime command: {args}')
            return types.SimpleNamespace(returncode=0, stdout=output, stderr='')
        return invoke

    def test_runtime_records_service_and_port_without_exposing_process_arguments(self):
        command = self.runtime_command(
            units='rustfs-validation-heal-gh-123-1-h2.service loaded inactive dead private fixture\n',
            listeners='LISTEN 0 4096 [::]:9000 [::]:* users:(("private-label",pid=42,fd=7))\n')
        with patch.object(fleet_node, 'command', side_effect=command) as calls:
            result = fleet_node.environment_service_state()
        self.assertEqual(result['standardService']['MainPID'], 42)
        self.assertEqual(result['port9000Listeners'], [{'localAddress': '[::]:9000', 'pids': [42]}])
        self.assertEqual(result['loadedValidationUnits'][0]['active'], 'inactive')
        self.assertNotIn('private-label', str(result))
        self.assertNotIn('private fixture', str(result))
        self.assertEqual(len(calls.call_args_list), 6)
        for field in ('mutated', 'cleanStateCertified', 'globalWorkflowIdleCertified', 'retainedSceneRecoveryCertified'):
            self.assertFalse(result[field])

    def test_idle_runtime_and_unattributed_listener_never_certify_clean_state(self):
        for listener in ('', 'LISTEN 0 4096 0.0.0.0:9000 0.0.0.0:*\n'):
            with self.subTest(listener=listener), patch.object(fleet_node, 'command', side_effect=self.runtime_command(
                    state='LoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0\n', listeners=listener)):
                result = fleet_node.environment_service_state()
            self.assertFalse(result['cleanStateCertified'])
            self.assertEqual(result['standardService']['MainPID'], 0)
            if listener: self.assertEqual(result['port9000Listeners'][0]['pids'], [])

    def test_runtime_refuses_partial_failed_ambiguous_or_unrecognized_observations(self):
        scenarios = [
            self.runtime_command(state='LoadState=loaded\nMainPID=42\n'),
            self.runtime_command(units='rustfs-validation-heal-gh-123-1-h2.service loaded active running\n' * 2),
            self.runtime_command(units='rustfs-validation-heal-unrecognized.service loaded active running\n'),
            self.runtime_command(listeners='LISTEN 0 4096 0.0.0.0:9001 0.0.0.0:*\n'),
        ]
        for command in scenarios:
            with patch.object(fleet_node, 'command', side_effect=command), self.assertRaises(ValueError):
                fleet_node.environment_service_state()
        failed = types.SimpleNamespace(returncode=1, stdout='LoadState=loaded\nActiveState=active\nSubState=running\nMainPID=42\n', stderr='')
        with patch.object(fleet_node, 'command', return_value=failed), self.assertRaises(ValueError):
            fleet_node.environment_service_state()

    def test_runtime_refuses_service_or_listener_changes_between_observations(self):
        for changing in ('service', 'listener', 'validation-unit'):
            command = self.runtime_command()
            counts = {}
            def invoke(*args, **params):
                result = command(*args, **params)
                counts[args] = counts.get(args, 0) + 1
                if counts[args] == 2:
                    if changing == 'service' and args[:2] == ('systemctl', 'show'):
                        result.stdout = result.stdout.replace('MainPID=42', 'MainPID=43')
                    elif changing == 'listener' and args[0] == 'ss':
                        result.stdout = 'LISTEN 0 4096 0.0.0.0:9000 0.0.0.0:*\n'
                    elif changing == 'validation-unit' and args[:2] == ('systemctl', 'list-units'):
                        result.stdout = 'rustfs-validation-heal-gh-123-1-h2.service loaded active running\n'
                return result
            with self.subTest(changing=changing), patch.object(fleet_node, 'command', side_effect=invoke), self.assertRaises(ValueError):
                fleet_node.environment_service_state()

    def test_legacy_mount_only_inventory_does_not_imply_runtime_or_clean_proof(self):
        result = environment.summarize(self.fixture())
        self.assertFalse(result['runtimeObservationsComplete'])
        self.assertFalse(result['cleanStateCertified'])


class I05PlatformFilterTests(unittest.TestCase):
    def test_tool_inventory_keeps_broken_kernel_wrapper_separate_from_working_binary(self):
        wrapper = Path('/usr/sbin/bpftool')
        binary = Path('/usr/lib/linux-tools/6.17.0-1020-azure/bpftool')
        calls = []
        def invoke(*args, **kwargs):
            calls.append(args)
            failed = args[0] == str(wrapper)
            return types.SimpleNamespace(returncode=1 if failed else 0,
                stdout='' if failed else ('bpftool v7.6.0' if args[1] == '--version' else '[]'),
                stderr='bpftool not found for kernel 6.17.0-1022' if failed else '')
        with patch.object(fleet_node.shutil, 'which', return_value=str(wrapper)), \
                patch.object(Path, 'glob', return_value=[binary]), \
                patch.object(Path, 'resolve', autospec=True, side_effect=lambda path, **kw: path), \
                patch.object(Path, 'is_file', return_value=True), \
                patch.object(Path, 'stat', return_value=types.SimpleNamespace(st_uid=0, st_mode=0o755)), \
                patch.object(fleet_node, 'digest', return_value='a' * 64), \
                patch.object(fleet_node, 'command', side_effect=invoke):
            result = fleet_node.bpf_tool_inventory()
        self.assertEqual(result['selectedPath'], str(wrapper))
        self.assertEqual([row['version']['returncode'] for row in result['tools']], [1, 0])
        self.assertEqual([row['rootCgroupQuery']['jsonListObserved'] for row in result['tools']], [False, True])
        self.assertTrue(all(row['serviceAttachmentsVerified'] is False for row in result['tools']))
        self.assertFalse(result['mutated'])
        self.assertEqual(calls, [(str(path), *args) for path in (wrapper, binary) for args in (
            ('--version',), ('-j', 'cgroup', 'show', '/sys/fs/cgroup'))])

    def owner(self):
        return {'runtimeProfile': 'lock-timeout', 'runId': 'gh-123-1-h1',
                'unit': 'rustfs-validation-heal-gh-123-1-h1.service'}

    def test_only_i05_private_unit_receives_filter_and_missing_backend_refuses(self):
        with patch.object(fleet_node.shutil, 'which', return_value='/usr/sbin/bpftool'), \
                patch.object(fleet_node, 'paths'), \
                patch.object(Path, 'is_file', return_value=True):
            properties = fleet_node.i05_filter_properties(self.owner())
            self.assertEqual(properties, ['--property=IPAddressDeny=168.63.129.16/32',
                                          '--property=IPAddressAllow='])
            with self.assertRaises(ValueError):
                fleet_node.i05_filter_properties({**self.owner(), 'unit': 'rustfs.service'})
        with patch.object(fleet_node.shutil, 'which', return_value=None):
            with self.assertRaises(ValueError): fleet_node.i05_filter_properties(self.owner())
            self.assertEqual(fleet_node.i05_filter_properties({'runtimeProfile': 'manual'}), [])

    def policy(self, variant=None):
        owner = self.owner()
        group = '/system.slice/' + owner['unit']
        values = {'IPAddressDeny': '168.63.129.16/32', 'IPAddressAllow': '',
                  'ControlGroup': group, 'MainPID': '42'}
        rows = [{'id': i, 'attach_type': direction, 'name': 'sd_fw_' + direction, 'attach_flags': 'multi'}
                for i, direction in enumerate(('ingress', 'egress'), 1)]
        programs = {row['id']: {'id': row['id'], 'name': row['name'], 'type': 'cgroup_skb',
                              'uid': 0, 'tag': 'a' * 16} for row in rows}
        if variant == 'libbpf-names':
            for row in rows: row['attach_type'] = 'cgroup_inet_' + row['attach_type']
        elif variant == 'duplicate-direction': rows[1]['attach_type'] = 'cgroup_inet_ingress'
        elif variant == 'foreign-direction': rows[0]['attach_type'] = 'cgroup_inet_sock_create'
        elif variant == 'invalid-direction': rows[0]['attach_type'] = ['ingress']
        elif variant == 'flags-array': rows[0]['attach_flags'] = ['multi']
        elif variant == 'ignored-property': values['IPAddressDeny'] = ''
        elif variant == 'allow': values['IPAddressAllow'] = 'any'
        elif variant == 'foreign-cgroup': values['ControlGroup'] = '/system.slice/rustfs.service'
        elif variant == 'pid': values['MainPID'] = '43'
        elif variant == 'unsupported-kernel': rows = []
        elif variant == 'missing-egress': rows.pop()
        elif variant == 'foreign-program': rows[0]['name'] = 'foreign'
        elif variant == 'duplicate-id': rows[1]['id'] = rows[0]['id']
        elif variant == 'override': rows[0]['attach_flags'] = 'override'
        elif variant == 'program-id': programs[1]['id'] = 999
        elif variant == 'program-type': programs[1]['type'] = 'socket_filter'
        elif variant == 'program-owner': programs[1]['uid'] = 1000
        elif variant == 'program-tag': programs[1]['tag'] = ''
        elif variant == 'query-failed': pass
        def invoke(*args, **kwargs):
            if args[:2] == ('systemctl', 'show'):
                raw = '\n'.join(k + '=' + v for k, v in values.items())
            elif args[:4] == ('bpftool', '-j', 'cgroup', 'show'):
                if variant == 'query-failed': raise ValueError('kernel query refused')
                raw = json.dumps(rows)
            elif args[:5] == ('bpftool', '-j', 'prog', 'show', 'id'):
                # Both bpftool versions' single-program JSON forms are accepted.
                program = programs[int(args[5])]
                raw = json.dumps([program] if int(args[5]) == 1 else program)
            else: self.fail(f'Unexpected filter command: {args}')
            return types.SimpleNamespace(stdout=raw)
        membership = '0::' + (group if variant != 'process-cgroup' else '/foreign')
        with patch.object(fleet_node, 'command', side_effect=invoke) as command, \
                patch.object(fleet_node, 'paths'), \
                patch.object(Path, 'read_text', return_value=membership + '\n'), \
                patch.object(Path, 'is_dir', return_value=variant != 'missing-cgroup'), \
                patch.object(Path, 'resolve', autospec=True, side_effect=lambda path, **kw:
                    Path('/foreign') if variant == 'aliased-cgroup' else path):
            result = fleet_node.i05_filter_policy(owner, 42)
        return result, command

    def test_effective_properties_require_actual_process_bound_kernel_programs(self):
        result, calls = self.policy()
        self.assertEqual(result['deniedAddress'], '168.63.129.16/32')
        self.assertEqual({row['attach_type'] for row in result['kernelPrograms']}, {'ingress', 'egress'})
        self.assertFalse(result['hostFirewallChanged'])
        self.assertFalse(result['persistentNetworkSettingsChanged'])
        self.assertTrue(result['snapshotOnly'])
        self.assertEqual([call.args[0] for call in calls.call_args_list],
                         ['systemctl', 'bpftool', 'bpftool', 'bpftool'])

    def test_libbpf_attach_names_keep_raw_kernel_evidence_and_same_directions(self):
        result, _ = self.policy('libbpf-names')
        self.assertEqual({row['attach_type'] for row in result['kernelPrograms']},
                         {'cgroup_inet_ingress', 'cgroup_inet_egress'})
        self.assertEqual({row['direction'] for row in result['kernelPrograms']}, {'ingress', 'egress'})

    def test_configured_properties_do_not_hide_missing_foreign_or_changed_kernel_filter(self):
        for variant in ('ignored-property', 'allow', 'foreign-cgroup', 'pid', 'unsupported-kernel',
                        'missing-egress', 'foreign-program', 'duplicate-id', 'override', 'program-id',
                        'program-type', 'program-owner', 'program-tag', 'query-failed',
                        'process-cgroup', 'missing-cgroup', 'aliased-cgroup',
                        'duplicate-direction', 'foreign-direction', 'invalid-direction', 'flags-array'):
            with self.subTest(variant=variant), self.assertRaises(ValueError): self.policy(variant)


if __name__ == '__main__': unittest.main()
