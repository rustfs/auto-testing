"""I04 physical oracles must include every run-owned pressure and durable bucket."""

import ast
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_heal import queue_pressure as queue
from scripts.validation_heal.fleet_node import snapshot_buckets
from scripts.validation_heal.healthy import Fleet


class QueueBucketScopeTest(unittest.TestCase):
    def test_real_executor_passes_all_owned_buckets_through_actual_fleet_snapshot(self):
        class ReachedPhysicalMapping(BaseException):
            pass
        for topology, nodes in [('H1', ['vm000']), ('H2', ['vm000', 'vm001', 'vm002', 'vm003'])]:
            with self.subTest(topology=topology), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                root = Path(directory)
                candidate = root / 'candidate.json'
                candidate.write_text(json.dumps({'source_sha': queue.REFERENCE['candidateCommit'], 'package_sha256': 'a' * 64}))
                rc = root / 'rc'
                rc.write_bytes(b'client')
                args = SimpleNamespace(evidence=root, candidate=candidate, rc_bin=rc, tool_directory=root,
                    testing_commit='b' * 40, binary_sha='c' * 64)
                records, mapped, owner = {}, [], {}
                fleet = Fleet(nodes, root, 'not-used')
                fleet.record = lambda label, value: records.setdefault(label, value)
                def deploy(binary, prefix, *credentials):
                    owner['bucket'] = prefix
                fleet.deploy = deploy
                fleet.stop = lambda: None
                fleet.cleanup = lambda: True
                requests = []
                def execute(command, *, input, **kwargs):
                    request = ast.literal_eval(ast.parse(input.rsplit('\nmain(', 1)[1].rsplit(')', 1)[0], mode='eval').body)
                    self.assertEqual(request['action'], 'snapshot')
                    buckets = snapshot_buckets(owner, request)
                    requests.append(request)
                    return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({'formats': ['format'],
                        'files': [{'path': bucket + '/objects/0000.bin/xl.meta'} for bucket in buckets]}))
                stack.enter_context(patch('scripts.validation_heal.healthy.subprocess.run', side_effect=execute))
                probe = SimpleNamespace(inventory=lambda *args, **kwargs: {})
                stack.enter_context(patch.dict('os.environ', {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}))
                stack.enter_context(patch.object(queue, 'Fleet', return_value=fleet))
                stack.enter_context(patch.object(queue, 'Probe', return_value=probe))
                stack.enter_context(patch.object(queue, 'Recorder', return_value=None))
                stack.enter_context(patch.object(queue, 'seed', return_value=[]))
                stack.enter_context(patch.object(queue, 'verify_seed', return_value={}))
                stack.enter_context(patch.object(queue, 'verify_reference', return_value={}))
                stack.enter_context(patch.object(queue, 'verify_inspector', return_value={}))
                stack.enter_context(patch.object(queue, 'validate_topology', return_value={}))
                def map_cohorts(snapshot, bucket, expected, **kwargs):
                    mapped.append(snapshot)
                    raise ReachedPhysicalMapping
                stack.enter_context(patch.object(queue, 'map_cohorts', side_effect=map_cohorts))
                with self.assertRaises(ReachedPhysicalMapping): queue.run_topology(args, topology, nodes)
                expected = {owner['bucket'], *[owner['bucket'] + f'-queue-{i}' for i in range(4)], owner['bucket'] + '-durable'}
                baseline = records['physical-baseline']
                self.assertEqual(set(baseline), set(nodes))
                for observed in baseline.values():
                    self.assertEqual({row['path'].split('/')[0] for row in observed['files']}, expected)
                for observed in mapped[0].values():
                    self.assertEqual([row['path'].split('/')[0] for row in observed['files']], [owner['bucket'] + '-queue-0'])

                # Every later phase uses the same actual remote adapter and
                # cannot silently fall back to the empty primary bucket.
                for label in ['physical-fault', 'physical-before-pressure', 'physical-product-result-before-GET',
                              'physical-after-S3', 'physical-failure']:
                    fleet.snapshot(label, quiescent=label != 'physical-before-pressure')
                self.assertEqual(len(requests), 6 * len(nodes))
                for request in requests:
                    self.assertEqual(set(request['buckets']), expected)
                with self.assertRaisesRegex(ValueError, 'frozen fleet binding'):
                    fleet.snapshot('changed-scope', buckets=[owner['bucket']])


if __name__ == '__main__':
    unittest.main()
