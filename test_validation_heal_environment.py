import copy
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


if __name__ == '__main__': unittest.main()
