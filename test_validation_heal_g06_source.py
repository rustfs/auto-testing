"""Frozen DS and COPY direction contracts; synthetic scenes are not live proof."""
import copy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

import test_validation_heal as physical_fixtures
from scripts.validation_heal import race


class G06SourceTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def baseline(self):
        return {'entries': [
            {'key': key, 'versionId': 'null', 'isLatest': True, 'deleteMarker': False,
             'payload': {'size': size, 'sha256': role},
             'properties': {'ETag': '"' + role + '"', 'ContentLength': size,
                            'ContentType': 'application/' + role, 'Metadata': {'role': role}},
             'tags': [{'Key': 'role', 'Value': role}]}
            for key, role, size in [(race.G06_TARGET_KEY, 'selected', 17),
                                    (race.G06_REFERENCE_KEY, 'other', 23)]]}

    def test_frozen_ds_keys_and_seed_use_the_existing_workload(self):
        self.assertEqual(race.G06_TARGET_KEY, '中文/文件/0252.bin')
        self.assertEqual(race.G06_REFERENCE_KEY, 'literal%2Fslash/0254.bin')
        recorder = Mock()
        recorder.probe = SimpleNamespace(binding={'bucketPrefix': self.bucket})
        recorder.call.return_value = {'ETag': '"ack"'}
        with patch.object(race, 'SIZE', 31):
            expected = race.seed_fixture(recorder, self.bucket, 'copy-source')
        prefixes = ('中文/文件', 'space key', 'literal%2Fslash', 'actual/slash', 'a/child', 'ab/control', 'b/other')
        keys = [f'{prefixes[i % 7]}/{i:04d}.bin' for i in range(257)]
        self.assertEqual([r['key'] for r in expected], keys)
        puts = [c for c in recorder.call.call_args_list if c.args[0] == 'put_object']
        self.assertEqual(len(puts), 257)
        self.assertEqual([c.args[1]['Key'] for c in puts], keys)
        self.assertEqual(puts[252].args[1]['Metadata'], {'heal-fixture': 'DS', 'nonce': '252-0'})
        self.assertEqual(puts[254].args[1]['Tagging'], 'suite=heal&fixture=DS')

    def test_copy_source_or_destination_is_the_original_missing_selected_key(self):
        for branch in ('copy-source', 'copy-destination'):
            with self.subTest(branch=branch):
                params = race.operation_plan(self.bucket, branch, self.baseline()['entries'])[0]['params']
                source, destination = ((race.G06_TARGET_KEY, race.G06_REFERENCE_KEY) if branch == 'copy-source'
                                       else (race.G06_REFERENCE_KEY, race.G06_TARGET_KEY))
                self.assertEqual(params['Key'], destination)
                self.assertEqual(params['CopySource'], {'Bucket': self.bucket, 'Key': source, 'VersionId': 'null'})

    def test_copy_final_s3_follows_source_content_metadata_and_tags(self):
        for branch in ('copy-source', 'copy-destination'):
            with self.subTest(branch=branch):
                before = self.baseline()
                plan = race.operation_plan(self.bucket, branch, before['entries'])
                source, destination = ((race.G06_TARGET_KEY, race.G06_REFERENCE_KEY) if branch == 'copy-source'
                                       else (race.G06_REFERENCE_KEY, race.G06_TARGET_KEY))
                original = {r['key']: r for r in before['entries']}
                after = race.final_inventory(before, branch, plan, [{'CopyObjectResult': {'ETag': '"copied"'}}])
                actual = {r['key']: r for r in after['entries']}
                self.assertEqual(actual[source], original[source])
                self.assertEqual(actual[destination]['payload'], original[source]['payload'])
                self.assertEqual(actual[destination]['tags'], original[source]['tags'])
                self.assertEqual(actual[destination]['properties'], {**original[source]['properties'], 'ETag': '"copied"'})

    def scene(self, branch):
        before = physical_fixtures.HealShardOracleTest().snapshot()
        for value in before.values():
            original = value['files']
            value['files'] = []
            for meta, part in zip(original[::3], original[1::3], strict=True):
                for key, generation, role in [(race.G06_TARGET_KEY, 100, 'selected'),
                                              (race.G06_REFERENCE_KEY, 200, 'other')]:
                    m, p = copy.deepcopy(meta), copy.deepcopy(part)
                    m['path'] = f'{self.bucket}/{key}/xl.meta'
                    m['metadata']['key'] = key
                    m['metadata']['versions'][0]['dataDir'] = str(uuid.UUID(int=generation))
                    p['path'] = f'{self.bucket}/{key}/{uuid.UUID(int=generation)}/part.1'
                    p['sha256'] += '-' + role
                    value['files'] += [m, p]
        expected = self.baseline()['entries']
        mapped = race.map_fixture(before, self.bucket, expected)
        source, destination = ((race.G06_TARGET_KEY, race.G06_REFERENCE_KEY) if branch == 'copy-source'
                               else (race.G06_REFERENCE_KEY, race.G06_TARGET_KEY))
        after = copy.deepcopy(before)
        for value in after.values():
            by_path = {(f['disk'], f['path']): f for f in value['files']}
            for f in value['files']:
                if not f['path'].startswith(f'{self.bucket}/{destination}/'): continue
                if f['path'].endswith('/xl.meta'):
                    f['metadata']['versions'][0]['dataDir'] = str(uuid.UUID(int=300))
                else:
                    slot = 100 if source == race.G06_TARGET_KEY else 200
                    f['sha256'] = by_path[f['disk'], f'{self.bucket}/{source}/{uuid.UUID(int=slot)}/part.1']['sha256']
                    f['path'] = f'{self.bucket}/{destination}/{uuid.UUID(int=300)}/part.1'
        return before, after, mapped, {'entries': expected}, destination

    def test_real_copy_transition_accepts_both_directions_before_get(self):
        for branch in ('copy-source', 'copy-destination'):
            with self.subTest(branch=branch):
                before, after, mapped, final, _ = self.scene(branch)
                _, proof = race.physical_transition(before, after, self.bucket, mapped, final, branch)
                self.assertTrue(proof['physicalProofBeforeGETOrRescue'])

    def test_copy_rejects_wrong_source_shards_and_resurrected_destination_generation(self):
        for branch in ('copy-source', 'copy-destination'):
            for fault in ('wrong-shard', 'old-generation', 'source-shard'):
                with self.subTest(branch=branch, fault=fault):
                    before, after, mapped, final, destination = self.scene(branch)
                    part = next(f for f in after['node1']['files'] if f['path'].startswith(f'{self.bucket}/{destination}/')
                                and f['path'].endswith('/part.1'))
                    if fault == 'wrong-shard': part['sha256'] = 'wrong-copy'
                    elif fault == 'old-generation':
                        old = next(f for f in before['node1']['files'] if f['path'].startswith(f'{self.bucket}/{destination}/')
                                   and f['path'].endswith('/part.1'))
                        after['node1']['files'].append(copy.deepcopy(old))
                    else:
                        source = race.G06_TARGET_KEY if branch == 'copy-source' else race.G06_REFERENCE_KEY
                        original = next(f for f in after['node1']['files'] if f['path'].startswith(f'{self.bucket}/{source}/')
                                        and f['path'].endswith('/part.1'))
                        original['sha256'] = 'changed-copy-source'
                    with self.assertRaises(ValueError):
                        race.physical_transition(before, after, self.bucket, mapped, final, branch)


if __name__ == '__main__':
    unittest.main()
