"""Read-only DNS admission for the current auto-testing race endpoints."""
import subprocess
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts.validation_heal import fleet_node, race


class EndpointResolutionTests(unittest.TestCase):
    def request(self, topology='h2'):
        return {'runId': 'gh-12345-1-' + topology, 'action': 'endpoint-resolution'}

    def test_observation_resolves_only_existing_fixed_aliases_without_retries_or_mutations(self):
        def resolve(args, **kwargs):
            self.assertEqual(args[:2], ['getent', 'ahosts'])
            self.assertEqual(kwargs['timeout'], 10)
            return SimpleNamespace(returncode=0, stdout='10.0.0.1 STREAM\n10.0.0.1 DGRAM\n')
        for topology, count in [('h1', 1), ('h2', 4)]:
            with self.subTest(topology=topology), patch.object(fleet_node.subprocess, 'run', side_effect=resolve) as run:
                observed = fleet_node.endpoint_resolution(self.request(topology))
                self.assertTrue(observed['resolved'] and observed['readOnly'])
                self.assertFalse(observed['mutated'] or observed['canonicalPassClaimed'] or observed['hostIdentityCertified'])
                self.assertEqual(run.call_count, count)
                self.assertEqual(list(observed['entries']), [f'rustfs-node{i}' for i in range(1, count + 1)])
                self.assertTrue(all(row['addresses'] == ['10.0.0.1'] for row in observed['entries'].values()))

    def test_unresolved_empty_timeout_or_malformed_resolver_output_is_retained_and_refused(self):
        variants = [SimpleNamespace(returncode=2, stdout=''), SimpleNamespace(returncode=0, stdout=''),
                    SimpleNamespace(returncode=0, stdout='not-an-ip STREAM\n'),
                    subprocess.TimeoutExpired(['getent'], 10), OSError('getent unavailable')]
        for value in variants:
            with self.subTest(value=value), patch.object(fleet_node.subprocess, 'run') as run:
                if isinstance(value, Exception): run.side_effect = value
                else: run.return_value = value
                observed = fleet_node.endpoint_resolution(self.request('h1'))
                self.assertFalse(observed['resolved'])
                self.assertEqual(run.call_count, 1)
                self.assertFalse(observed['entries']['rustfs-node1']['resolved'])

    def test_operator_cannot_supply_a_different_hostname_or_command(self):
        with patch.object(fleet_node.subprocess, 'run') as run:
            with self.assertRaises(ValueError):
                fleet_node.endpoint_resolution({**self.request(), 'host': 'other'})
            with self.assertRaises(ValueError):
                fleet_node.endpoint_resolution({**self.request(), 'action': 'reserve'})
            run.assert_not_called()

    def test_controller_retains_all_read_only_results_and_stops_before_reservation(self):
        good = {'readOnly': True, 'mutated': False, 'resolved': True}
        bad = {**good, 'resolved': False}
        for bad_host in ('runner', 'vm002', None):
            with self.subTest(bad_host=bad_host), tempfile.TemporaryDirectory() as directory:
                fleet = race.Fleet(['vm000','vm001','vm002','vm003'], Path(directory), 'gh-12345-1-h2')
                calls = []; records = {}
                def remote(node, action):
                    calls.append((node, action))
                    return bad if node == bad_host else good
                fleet.remote = remote
                fleet.record = lambda label, value: records.setdefault(label, value)
                with patch.object(race, 'endpoint_resolution', return_value=bad if bad_host == 'runner' else good):
                    if bad_host is None: race.verify_endpoint_resolution(fleet)
                    else:
                        with self.assertRaisesRegex(ValueError, 'name resolution failed'):
                            race.verify_endpoint_resolution(fleet)
                self.assertEqual(set(calls), {(n,'endpoint-resolution') for n in fleet.nodes})
                self.assertEqual(set(records['endpoint-resolution']['peers']), set(fleet.nodes))
                self.assertEqual(fleet.reserved, [])

    def test_real_case_refuses_dns_precondition_before_deploy_seed_or_fault(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / 'candidate.json'
            candidate.write_text(json.dumps({'source_sha': race.VERSION_CANDIDATE,'package_sha256': 'a' * 64}))
            rc = root / 'rc'; rc.write_bytes(b'fixture-only')
            args = SimpleNamespace(evidence=root / 'evidence',candidate=candidate,rc_bin=rc,
                tool_directory=root,testing_commit='b' * 40,binary_sha='c' * 64,case_id='heal.race-unversioned-overwrite')
            fleet = Mock(nodes=['vm000'],run_id='gh-12345-1-h1')
            fleet.record.side_effect = lambda label,value:value
            fleet.remote.return_value = {'readOnly':True,'mutated':False,'resolved':False}
            fleet.cleanup.return_value = True
            with patch.object(race,'Fleet',return_value=fleet), patch.object(race,'verify_inspector',return_value={}), \
                 patch.object(race,'endpoint_resolution',return_value={'readOnly':True,'mutated':False,'resolved':False}), \
                 patch.object(race,'seed_fixture') as seed, patch.dict(race.os.environ,{'GITHUB_RUN_ID':'12345'}):
                result = race.run_branch(args,'H1',['vm000'],'overwrite-vs-deep')
            self.assertEqual(result['verdict'],'FAIL')
            self.assertIn('name resolution failed',result['failure']['message'])
            fleet.deploy.assert_not_called();seed.assert_not_called()
            self.assertTrue(all(call.args[1] == 'endpoint-resolution' for call in fleet.remote.call_args_list))


if __name__ == '__main__':
    unittest.main()
