"""Decoded G05 business generations across H1/H2; no staging/live PASS claim."""
import copy
import hashlib
import tempfile
import unittest
from unittest.mock import patch
import uuid

from scripts.validation_heal import mpu_race_committed as committed, mpu_race_fixture as model
import test_validation_heal_mpu_race_fixture as model_fixtures


class MultipartCommittedTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def scene(self, directory, topology):
        recorder = model_fixtures.Recorder(directory)
        fixture = model.MpuRaceFixture(recorder, self.bucket); fixture.seed()
        before_model = fixture.manifest()
        fixture.upload('a', 3); fixture.complete('a'); fixture.abort_b()
        after_model = fixture.manifest()
        nodes = ['vm000'] if topology == 'H1' else [f'vm00{i}' for i in range(4)]
        width = 4 * len(nodes); data = 2 if topology == 'H1' else 12
        ids = [str(uuid.UUID(int=1000 + i)) for i in range(width)]; distribution = list(range(width, 0, -1))
        def snapshot(manifest):
            expected = committed.entries(manifest); result = {}
            for node_number, node in enumerate(nodes):
                value = result[node] = {'hostname': 'host-' + node, 'binarySha256': 'a' * 64,
                    'quiescent': True, 'pid': 0, 'formats': [], 'files': []}
                for disk in range(4):
                    slot = node_number * 4 + disk; index = distribution[slot]
                    value['formats'].append({'disk': disk, 'value': {'format': 'xl', 'id': 'pool',
                        'xl': {'sets': [ids], 'this': ids[slot]}}})
                    versions = []
                    for entry in expected:
                        role = next(r for r in ('committed', 'a') if manifest['items'][r].get('versionId') == entry['versionId'])
                        directory_id = str(uuid.UUID(int=100 if role == 'committed' else 200))
                        versions.append({'versionId': entry['versionId'], 'isLatest': entry['isLatest'], 'deleted': False,
                            'dataDir': directory_id, 'size': entry['payload']['size'], 'inline': None, 'usesLegacyChecksum': True,
                            'role': 'data' if index <= data else 'parity',
                            'parts': [{'number': p['number'], 'size': p['size'], 'actualSize': p['size']} for p in entry['multipartParts']],
                            'erasure': {'index': index, 'data_blocks': data, 'parity_blocks': width - data,
                                       'block_size': 1048576, 'distribution': distribution, 'algorithm': 'rs-vandermonde'}})
                        for part in entry['multipartParts']:
                            path = f'{self.bucket}/{model.KEY}/{directory_id}/part.{part["number"]}'
                            value['files'].append({'disk': disk, 'path': path, 'size': (part['size'] + data - 1) // data + 32,
                                'sha256': hashlib.sha256(f'{role}/{index}/{part["number"]}'.encode()).hexdigest()})
                            value['files'].append({'disk': disk, 'path': path + '.meta', 'size': 29,
                                'sha256': hashlib.sha256(f'upload/{role}/{index}/{part["number"]}'.encode()).hexdigest()})
                    value['files'].append({'disk': disk, 'path': f'{self.bucket}/{model.KEY}/xl.meta', 'size': 1000,
                        'sha256': 'd' * 64, 'metadata': {'bucket': self.bucket, 'key': model.KEY,
                                                       'physicalBitrotVerified': False, 'versions': versions}})
            return result
        before, after = snapshot(before_model), snapshot(after_model)
        evidence = {node: {'hostname': 'host-' + node, 'binarySha256': 'a' * 64, 'inspectorSha256': 'b' * 64,
            'quiescent': True, 'readOnly': True, 'sameHeldFileHashedAndDecoded': True,
            'rawContentsExported': False, 'liveAuthorityClaimed': False, 'rows': []} for node in nodes}
        return before, after, before_model, after_model, evidence

    def test_three_part_full_cohorts_and_old_version_survive_complete_in_both_topologies(self):
        for topology in ('H1', 'H2'):
            with self.subTest(topology=topology), tempfile.TemporaryDirectory() as directory, patch.object(model, 'PART_SIZES', (101, 111, 131)):
                before, after, original, final, evidence = self.scene(directory, topology)
                cohorts, proof = committed.transition(before, after, original, final, evidence, 'a' * 64, 'b' * 64)
                self.assertEqual(len(cohorts), 6)
                self.assertTrue(all(len(t['cohort']) == 4 * len(before) for t in cohorts.values()))
                self.assertTrue(proof['oldVersionRetained'] and proof['newVersionExactlyOnce'] and proof['allPartsCodingIndicesVerified'])
                self.assertTrue(proof['stagingContinuityStillRequired'] and proof['fullPayloadS3StillRequired'])
                self.assertFalse(proof['canonicalCasePassClaimed'] or proof['physicalRepairProved'])

    def test_reclaimed_upload_sidecar_is_reported_and_never_used_as_committed_authority(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(model, 'PART_SIZES', (101, 111, 131)):
            before, after, original, final, evidence = self.scene(directory, 'H1')
            old_path = f'{self.bucket}/{model.KEY}/{uuid.UUID(int=100)}/part.2.meta'
            after['vm000']['files'] = [r for r in after['vm000']['files'] if not (r['disk'] == 2 and r['path'] == old_path)]
            _, proof = committed.transition(before, after, original, final, evidence, 'a' * 64, 'b' * 64)
            self.assertEqual(proof['reclaimedOldUploadSidecars'], [['vm000', 2, old_path]])
            self.assertFalse(proof['uploadSidecarsAreCommittedAuthority'])

    def test_backup_prefix_decoder_is_required_by_the_actual_business_transition(self):
        for count in (0, 1):
            with self.subTest(prefix=count), tempfile.TemporaryDirectory() as directory, patch.object(model, 'PART_SIZES', (101, 111, 131)):
                before, after, original, final, evidence = self.scene(directory, 'H1')
                for disk in range(4):
                    metadata = next(r['metadata'] for r in (before if count == 0 else after)['vm000']['files']
                                    if r['disk'] == disk and r['path'].endswith('/xl.meta'))
                    row = {'disk': disk, 'path': f'{self.bucket}/{model.KEY}/{uuid.UUID(int=100)}/xl.meta.bkp',
                           'size': 101 + count, 'sha256': hashlib.sha256(f'backup/{disk}/{count}'.encode()).hexdigest()}
                    after['vm000']['files'].append(copy.deepcopy(row))
                    evidence['vm000']['rows'].append({**row, 'metadata': copy.deepcopy(metadata)})
                _, proof = committed.transition(before, after, original, final, evidence, 'a' * 64, 'b' * 64)
                self.assertEqual({p['acknowledgedCompletePrefix'] for p in proof['backupProof']['proofs']}, {count})
                evidence['vm000']['rows'][0]['metadata']['versions'][0]['parts'][0]['size'] += 1
                with self.assertRaises(ValueError): committed.transition(before, after, original, final, evidence, 'a' * 64, 'b' * 64)

    def test_drift_missing_parts_wrong_geometry_and_unknown_leaves_are_rejected(self):
        for topology in ('H1', 'H2'):
            for fault in ('old-shard', 'missing-new-part', 'old-sidecar', 'generation', 'extra-version', 'logical-size',
                          'unknown-leaf', 'format', 'live', 'bool-pid', 'bool-disk', 'sibling-id', 'model-payload', 'multipart-etag',
                          'a-before-state', 'a-metadata', 'b-parts'):
                with self.subTest(topology=topology, fault=fault), tempfile.TemporaryDirectory() as directory, patch.object(model, 'PART_SIZES', (101, 111, 131)):
                    before, after, original, final, evidence = self.scene(directory, topology)
                    rows = after['vm000']['files']; prefix = f'{self.bucket}/{model.KEY}/'
                    old_part = next(r for r in rows if r['path'] == prefix + str(uuid.UUID(int=100)) + '/part.2')
                    meta = next(r['metadata'] for r in rows if r['path'] == prefix + 'xl.meta')
                    if fault == 'old-shard': old_part['sha256'] = 'e' * 64
                    elif fault == 'missing-new-part': rows.remove(next(r for r in rows if r['path'] == prefix + str(uuid.UUID(int=200)) + '/part.3'))
                    elif fault == 'old-sidecar': next(r for r in rows if r['path'] == old_part['path'] + '.meta')['sha256'] = 'e' * 64
                    elif fault == 'generation': meta['versions'][-1]['dataDir'] = str(uuid.UUID(int=100))
                    elif fault == 'extra-version': meta['versions'].append({**copy.deepcopy(meta['versions'][0]), 'versionId': str(uuid.UUID(int=999))})
                    elif fault == 'logical-size': meta['versions'][-1]['parts'][1]['size'] += 1
                    elif fault == 'unknown-leaf': rows.append({**old_part, 'path': old_part['path'] + '.unknown'})
                    elif fault == 'format': after['vm000']['formats'][0]['value']['id'] = 'foreign'
                    elif fault == 'live': after['vm000']['quiescent'] = False
                    elif fault == 'bool-pid': after['vm000']['pid'] = False
                    elif fault == 'bool-disk': old_part['disk'] = False
                    elif fault == 'sibling-id': final['items']['b']['uploadId'] += '-different'
                    elif fault == 'model-payload': final['items']['a']['payload']['sha256'] = 'e' * 64
                    elif fault == 'multipart-etag': final['items']['a']['etag'] = '"wrong-3"'
                    elif fault == 'a-before-state': original['items']['a']['state'] = 'unknown'
                    elif fault == 'a-metadata': final['items']['a']['metadata']['nonce'] += '-changed'
                    else: final['items']['b']['parts'].pop()
                    with self.assertRaises(ValueError): committed.transition(before, after, original, final, evidence, 'a' * 64, 'b' * 64)


if __name__ == '__main__':
    unittest.main()
