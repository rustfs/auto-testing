"""Exact historical DL deletion effects cannot hide corruption or new commits."""

import copy
import hashlib
import unittest
from unittest.mock import patch

import test_validation_heal as oracle
from scripts.validation_heal import network_load_repair as repair, network_load as load, network
from scripts.validation_heal.shards import map_target


class LoadRepairTests(unittest.TestCase):
    def scene(self):
        base, _ = oracle.HealReadQuorumBoundaryTest().scene(total=16, data=8)
        bucket = oracle.HealReadQuorumBoundaryTest.bucket
        snapshot = {node: {**copy.deepcopy(value), 'files': [], 'binarySha256': 'a' * 64} for node, value in base.items()}
        fixture = {'bucket': bucket, 'runId': 'gh-12345-1-h2', 'sourceContract': load.source_reference(repair.CANDIDATE), 'sides': {}}
        for side_index, endpoint in enumerate(('node1', 'node4')):
            key = 'dl/' + endpoint + '/control.bin'
            expected = [{'key': key, 'versionId': f'v{side_index}{nonce}', 'isLatest': nonce == 2,
                         'deleteMarker': False, 'payload': {'size': 100, 'sha256': 'original' + str(nonce)}} for nonce in range(3)]
            fixture['sides'][endpoint] = {'controlKey': key, 'versions': expected,
                'uploads': {name: {'key': f'dl/{endpoint}/{name}.bin', 'uploadId': name + '-original',
                    'parts': [{'PartNumber': 1, 'ETag': 'original'}]} for name in ('complete', 'upload', 'abort')}}
            for node, value in base.items():
                for original in value['files']:
                    if 'metadata' not in original: continue
                    disk = original['disk'];meta = copy.deepcopy(original['metadata']);meta['key'] = key
                    template = meta['versions'][0];versions = []
                    for nonce, entry in enumerate(expected):
                        version = copy.deepcopy(template)
                        version.update(versionId=entry['versionId'], isLatest=entry['isLatest'],
                            dataDir=f'12345678-1234-1234-1234-{side_index * 10 + nonce:012d}')
                        versions.append(version)
                        snapshot[node]['files'].append({'disk': disk, 'path': f"{bucket}/{key}/{version['dataDir']}/part.1",
                            'size': 82, 'sha256': hashlib.sha256(f'{node}/{disk}/{key}/{nonce}'.encode()).hexdigest()})
                    meta['versions'] = versions
                    snapshot[node]['files'].append({'disk': disk, 'path': f'{bucket}/{key}/xl.meta', 'size': 600,
                        'sha256': hashlib.sha256(f'meta/{node}/{disk}/{key}'.encode()).hexdigest(), 'metadata': meta})
        first = fixture['sides']['node1']['versions'][0]
        target = map_target(snapshot, bucket, first['key'], 'data', 1, first['versionId'], latest_required=False)
        target = {**target, **next(row for row in target['cohort'] if row['node'] == 'node4')}
        plan = network.plan_for(snapshot, target, 'minority-partition')
        workers = []
        for endpoint in fixture['sides']:
            step = load.mutation_steps(fixture, endpoint)[3]
            request = {**copy.deepcopy(step), 'identity': {'role': 'writer', 'endpoint': endpoint, 'step': 4, 'nonce': 6},
                       'expectHttp': 503}
            row = {'request': request, 'ackPresent': True, 'ack': {'operationCheck': 'PASS', 'observation': {
                'clientCallInvoked': True, 'resultUnknown': False, 'acknowledged': False,
                'response': {'ResponseMetadata': {'HTTPStatusCode': 503}, 'Error': {'Code': 'ServiceUnavailable'}}}}}
            workers.append({'endpoint': endpoint, 'role': 'writer', 'rows': [row]})
        return snapshot, fixture, {'workers': workers}, plan

    def deleted(self, snapshot, fixture, *, staged=True):
        after = copy.deepcopy(snapshot);node, disk = 'node2', 0
        key = fixture['sides']['node1']['controlKey'];bucket = fixture['bucket']
        meta = next(row for row in after[node]['files'] if row['disk'] == disk and row['path'] == f'{bucket}/{key}/xl.meta')
        original = copy.deepcopy(meta);version = meta['metadata']['versions'].pop(0);meta['sha256'] = 'new-metadata'
        path = f"{bucket}/{key}/{version['dataDir']}/part.1"
        after[node]['files'] = [row for row in after[node]['files'] if (row['disk'], row['path']) != (disk, path)]
        if staged:
            prefix = f'{bucket}/{key}/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/'
            after[node]['files'].extend([
                {'disk': disk, 'path': prefix + 'xl.meta.bkp', 'size': original['size'], 'sha256': original['sha256']},
                {'disk': disk, 'path': prefix + 'delete-prepared', 'size': 300, 'sha256': 'prepared'},
                {'disk': disk, 'path': prefix + 'delete-target', 'size': 500, 'sha256': 'target'}])
        return after

    def records(self, before, after, fixture):
        keys = sorted(side['controlKey'] for side in fixture['sides'].values())
        result = {node: {'readOnly': True, 'quiescent': True, 'runId': fixture['runId'], 'bucket': fixture['bucket'],
            'binarySha256': 'a' * 64, 'keys': keys, 'credentialContentsRead': False, 'records': []} for node in before}
        for node, value in after.items():
            for file in value['files']:
                if not file['path'].endswith('/delete-prepared'): continue
                path = file['path'];prefix, transaction, _ = path.rsplit('/', 2)
                key = prefix[len(fixture['bucket']) + 1:]
                original = next(row for row in before[node]['files'] if row['disk'] == file['disk'] and row['path'] == prefix + '/xl.meta')
                target = next(row for row in value['files'] if row['disk'] == file['disk'] and row['path'] == prefix + '/' + transaction + '/delete-target')
                versions = original['metadata']['versions'];first = versions[0]
                result[node]['records'].append({'disk': file['disk'], 'key': key, 'path': path, 'size': file['size'],
                    'sha256': file['sha256'], 'protocolVersion': 1, 'transaction': transaction,
                    'requestVersion': first['versionId'], 'deletedVersions': [first['versionId']], 'dataDirs': [first['dataDir']],
                    'originalMetadataSha256': original['sha256'], 'targetMetadataSha256': target['sha256'],
                    'bucketIncarnation': 'original-incarnation',
                    'originalVersions': [row['versionId'] for row in versions], 'originalDataDirs': [row['dataDir'] for row in versions]})
        return result

    def status(self, fixture, key, repaired=()):
        expected = next(side['versions'] for side in fixture['sides'].values() if side['controlKey'] == key)
        objects = [{'identity': {'bucket': fixture['bucket'], 'object': key, 'versionId': row['versionId'],
            'bucketIncarnationId': 'original-incarnation', 'poolIndex': 0, 'setIndex': 0},
            'disposition': {'state': 'repaired' if row['versionId'] in repaired else 'verified_healthy'}} for row in expected]
        return {'summary': 'finished', 'truncated': False, 'outcome': {'execution': {'state': 'completed'},
            'coverage': 'complete', 'objectsTruncated': False, 'objects': objects, 'counters': {
            'processed': 3, 'healed': len(repaired), 'unchanged': 3 - len(repaired), 'skipped': 0,
            'failed': 0, 'unknown': 0, 'attemptFailures': 0, 'overflowed': False}}}

    def test_source_contract_is_exact_and_no_change_means_both_three_version_keys_remain_healthy(self):
        before, fixture, receipts, plan = self.scene()
        damage = repair.classify_deletes(before, copy.deepcopy(before), fixture, receipts, plan, list(before), self.records(before, before, fixture))
        self.assertEqual(damage['repairTargets'], [])
        with self.assertRaises(ValueError): repair.source_reference('072b57b37559fbe626aeda8abf61617fdcbdd926')
        calls = []
        def task(probe, fleet, selection, mode, active, **kwargs):
            calls.append(selection);self.assertTrue(kwargs['option_overrides']['recursive'])
            return self.status(fixture, selection['key'])
        with patch.object(repair, 'run_task', side_effect=task):
            result = repair.recover_controls(None, None, {'endpoints': {f'node{i}': 'url' for i in range(1, 5)}}, fixture, damage, [])
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(value['receipts'] == 3 and value['repaired'] == 0 for value in result.values()))

    def test_staged_original_delete_can_be_product_settled_but_does_not_itself_claim_rollback(self):
        before, fixture, receipts, plan = self.scene()
        after = self.deleted(before, fixture)
        damage = repair.classify_deletes(before, after, fixture, receipts, plan, list(before), self.records(before, after, fixture))
        key = fixture['sides']['node1']['controlKey'];policy = damage['policies'][key]
        self.assertFalse(policy['requiredRepair'] or damage['refusalProvesRollback'] or damage['canonicalCasePassClaimed'])
        self.assertTrue(policy['productSettlementAllowed'] and policy['finalPhysicalProofRequired'])
        self.assertEqual(policy['changedOriginalSlots'], [('node2', 0)])
        self.assertEqual(len(damage['repairTargets']), 1)
        for state in ('healthy-after-product-rollback', 'shard-repaired'):
            with self.subTest(state=state):
                def task(probe, fleet, selection, mode, active, **kwargs):
                    repaired = ['v00'] if state == 'shard-repaired' and selection['key'] == key else []
                    return self.status(fixture, selection['key'], repaired)
                with patch.object(repair, 'run_task', side_effect=task):
                    result = repair.recover_controls(None, None, {'endpoints': {'node1': '1', 'node2': '2', 'node3': '3', 'node4': '4'}}, fixture, damage, [])
                self.assertTrue(result[key]['productSettlementMayPrecedeShardCheck'])
                self.assertTrue(result[key]['finalPhysicalProofRequired'])

    def test_loss_without_exact_prepared_backup_requires_real_repaired_responsibility(self):
        before, fixture, receipts, plan = self.scene();after = self.deleted(before, fixture, staged=False)
        damage = repair.classify_deletes(before, after, fixture, receipts, plan, list(before), self.records(before, after, fixture))
        key = fixture['sides']['node1']['controlKey']
        self.assertTrue(damage['policies'][key]['requiredRepair'])
        with patch.object(repair, 'run_task', return_value=self.status(fixture, key)):
            with self.assertRaisesRegex(ValueError, 'repaired responsibility'):
                repair.recover_controls(None, None, {'endpoints': {'node1': '1'}}, fixture, damage, [])

    def test_other_versions_roots_foreign_files_and_bad_original_request_never_gain_an_exception(self):
        for kind in ('other-version', 'changed-bytes', 'foreign-version', 'read-only-slot', 'disconnected-slot',
                     'new-key', 'bad-backup', 'foreign-staged-version', 'unknown-transaction-file', 'wrong-request', 'unknown-response'):
            with self.subTest(kind=kind):
                before, fixture, receipts, plan = self.scene();after = self.deleted(before, fixture)
                rows = after['node2']['files'];bucket = fixture['bucket'];key = fixture['sides']['node1']['controlKey']
                if kind == 'other-version':
                    chosen = next(row for row in rows if row['disk'] == 0 and row['path'].endswith('000000000001/part.1'))
                    rows.remove(chosen)
                elif kind == 'changed-bytes': next(row for row in rows if row['path'].endswith('000000000001/part.1'))['sha256'] = 'changed'
                elif kind == 'foreign-version': next(row for row in rows if row.get('metadata'))['metadata']['versions'][0]['versionId'] = 'foreign'
                elif kind in {'read-only-slot', 'disconnected-slot'}:
                    node = 'node1' if kind == 'read-only-slot' else 'node4'
                    after[node]['files'] = [row for row in after[node]['files'] if not (row['disk'] == 0 and row['path'].endswith('000000000000/part.1'))]
                elif kind == 'new-key': rows.append({'disk': 0, 'path': bucket + '/dl/node1/copy.bin/part.1', 'size': 1, 'sha256': 'new'})
                elif kind == 'bad-backup': next(row for row in rows if '/aaaaaaaa-' in row['path'] and row['path'].endswith('xl.meta.bkp'))['sha256'] = 'foreign'
                elif kind == 'foreign-staged-version': rows.append({'disk': 0, 'path': f'{bucket}/{key}/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/12345678-1234-1234-1234-000000000001/part.1', 'size': 82, 'sha256': 'foreign'})
                elif kind == 'unknown-transaction-file': rows.append({'disk': 0, 'path': f'{bucket}/{key}/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/unknown', 'size': 1, 'sha256': 'foreign'})
                elif kind == 'wrong-request': receipts['workers'][0]['rows'][0]['request']['params']['VersionId'] = 'v01'
                else: receipts['workers'][0]['rows'][0]['ack']['observation']['resultUnknown'] = True
                with self.assertRaises(ValueError): repair.classify_deletes(before, after, fixture, receipts, plan, list(before), self.records(before, after, fixture))

    def test_raw_journal_census_must_match_exact_original_ack_selector_backup_target_and_incarnation(self):
        for kind in ('missing-peer', 'missing-record', 'wrong-selector', 'wrong-backup', 'wrong-target', 'wrong-generation', 'wrong-incarnation'):
            with self.subTest(kind=kind):
                before, fixture, receipts, plan = self.scene();after = self.deleted(before, fixture)
                records = self.records(before, after, fixture);row = records['node2']['records'][0]
                if kind == 'missing-peer': records.pop('node4')
                elif kind == 'missing-record': records['node2']['records'] = []
                elif kind == 'wrong-selector': row['requestVersion'] = 'unbound'
                elif kind == 'wrong-backup': row['originalMetadataSha256'] = 'unbound'
                elif kind == 'wrong-target': row['targetMetadataSha256'] = 'unbound'
                elif kind == 'wrong-generation': row['originalDataDirs'][1] = 'unbound'
                else: row['bucketIncarnation'] = 'foreign-incarnation'
                if kind == 'wrong-incarnation':
                    damage = repair.classify_deletes(before, after, fixture, receipts, plan, list(before), records)
                    key = fixture['sides']['node1']['controlKey']
                    with patch.object(repair, 'run_task', return_value=self.status(fixture, key)), self.assertRaisesRegex(ValueError, 'incarnation'):
                        repair.recover_controls(None, None, {'endpoints': {'node1': 'url'}}, fixture, damage, [])
                else:
                    with self.assertRaises(ValueError): repair.classify_deletes(before, after, fixture, receipts, plan, list(before), records)

    def test_hidden_status_failure_wrong_counters_and_control_repairs_are_not_settlement(self):
        for kind in ('unknown', 'counters', 'other-repair', 'missing-version'):
            with self.subTest(kind=kind):
                before, fixture, receipts, plan = self.scene();after = self.deleted(before, fixture)
                damage = repair.classify_deletes(before, after, fixture, receipts, plan, list(before), self.records(before, after, fixture))
                key = fixture['sides']['node1']['controlKey'];status = self.status(fixture, key)
                if kind == 'unknown': status['outcome']['objects'][0]['disposition']['state'] = 'unknown'
                elif kind == 'counters': status['outcome']['counters']['healed'] = 1
                elif kind == 'other-repair': status['outcome']['objects'][1]['disposition']['state'] = 'repaired'
                else: status['outcome']['objects'].pop()
                with patch.object(repair, 'run_task', return_value=status), self.assertRaises(ValueError):
                    repair.recover_controls(None, None, {'endpoints': {'node1': '1'}}, fixture, damage, [])


if __name__ == '__main__': unittest.main()
