import copy
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import environment
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


if __name__ == '__main__': unittest.main()
