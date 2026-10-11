import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import types
import unittest
import uuid
from unittest.mock import Mock, patch

from scripts.validation_heal import multi_pool as runner
from scripts.validation_heal.multi_pool_layout import validate, volumes
from scripts.validation_heal.fleet_node import runtime_config
from scripts.validation_heal.healthy import BASE_OPTIONS, validate_topology
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES
from scripts.validation_heal.shards import files, map_target
from scripts.validation_heal.scopes import physical_selection


class MultiPoolTests(unittest.TestCase):
    buckets = ('rv-heal-0123456789abcdef', 'rv-heal-0123456789abcdef-dv')

    def fixture(self):
        sets = [[[f'p{pool}-s{group}-n{node}' for node in range(4)] for group in range(2)] for pool in range(2)]
        snapshot = {f'node{node + 1}': {'formats': [], 'files': []} for node in range(4)}
        expected = {bucket: [] for bucket in self.buckets}
        for bucket in self.buckets:
            for pool in range(2):
                for group in range(2):
                    key = f'p{pool}/s{group}.bin'
                    versions = ('v1', 'v2', 'v3') if bucket.endswith('-dv') else ('null',)
                    for offset, version in enumerate(versions):
                        expected[bucket].append({'key': key, 'versionId': version})
                    for node, observed in enumerate(snapshot.values()):
                        disk = pool * 2 + group
                        metadata = {'bucket': bucket, 'key': key, 'versions': []}
                        for offset, version in enumerate(versions):
                            data_dir = str(uuid.UUID(int=pool * 100 + group * 10 + offset + 1))
                            metadata['versions'].append({'versionId': None if version == 'null' else version,
                                'isLatest': offset == len(versions) - 1, 'deleted': False, 'inline': None,
                                'dataDir': data_dir, 'size': runner.SIZE, 'usesLegacyChecksum': False,
                                'parts': [{'number': 1, 'size': runner.SIZE, 'actualSize': runner.SIZE}],
                                'role': 'data' if node < 2 else 'parity',
                                'erasure': {'index': node + 1, 'data_blocks': 2, 'parity_blocks': 2,
                                    'block_size': 1048576, 'distribution': [1, 2, 3, 4], 'algorithm': 'rs-vandermonde'}})
                            observed['files'].append({'disk': disk, 'path': f'{bucket}/{key}/{data_dir}/part.1',
                                'size': 400, 'sha256': f'{bucket}-{pool}-{group}-{offset}-{node}'})
                        observed['files'].append({'disk': disk, 'path': f'{bucket}/{key}/xl.meta', 'size': 600,
                            'sha256': f'meta-{bucket}-{pool}-{group}-{node}', 'metadata': metadata})
        for node, observed in enumerate(snapshot.values()):
            for disk in range(4):
                pool, group = disk // 2, disk % 2
                observed['formats'].append({'disk': disk, 'value': {'format': 'xl', 'id': 'deployment',
                    'xl': {'this': sets[pool][group][node], 'sets': sets[pool]}}})
        return snapshot, expected

    def mapping(self):
        snapshot, expected = self.fixture()
        return runner.map_versions(snapshot, expected)[1]

    def receipt(self, branch):
        mapped = self.mapping()
        delta, selected, targets = runner.selection(mapped, branch)
        options = {**BASE_OPTIONS, **delta}
        repaired = {(target['bucket'], target['key'], target['versionId']) for target in targets} & selected
        objects = [{'identity': {'kind': 'object', 'bucket': bucket, 'object': key, 'versionId': version,
                    'poolIndex': options['pool'], 'setIndex': options['set'], 'bucketIncarnationId': 'incarnation-' + bucket},
                    'disposition': {'state': 'repaired' if (bucket, key, version) in repaired else 'verified_healthy'}}
                   for bucket, key, version in sorted(selected)]
        current = {'summary': 'finished', 'truncated': False, 'settings': options,
            'outcome': {'execution': {'state': 'completed'}, 'coverage': 'complete', 'objectsTruncated': False,
                'objects': objects, 'counters': {'processed': len(selected), 'healed': len(repaired),
                    'unchanged': len(selected) - len(repaired), 'failed': 0, 'skipped': 0, 'unknown': 0,
                    'attemptFailures': 0, 'overflowed': False}}}
        return current, mapped, selected, repaired, options

    def test_preserves_all_three_source_branches_and_real_two_pool_arguments(self):
        self.assertEqual(select(runner.CASE_ID)[0]['variants'], list(runner.BRANCHES))
        self.assertIn(runner.CASE_ID, AUTOMATIC_CASES)
        args = volumes('gh-123-1-h5')
        self.assertEqual(len(args), 2)
        self.assertIn('rustfs{1...2}', args[0])
        self.assertIn('rustfs{3...4}', args[1])
        self.assertEqual(runtime_config('multi-pool')['RUSTFS_ERASURE_SET_DRIVE_COUNT'], '4')
        self.assertEqual(runtime_config('multi-pool')['RUSTFS_STORAGE_CLASS_STANDARD'], 'EC:2')
        with self.assertRaises(ValueError): volumes('gh-123-1-h2')

    def test_quota_status_reads_exact_owned_config_on_every_peer_without_scanner(self):
        probe, fleet = Mock(), Mock()
        probe.binding = {'endpoints': {'node1': 'a', 'node2': 'b', 'node3': 'c', 'node4': 'd'}}
        fleet.record.side_effect = lambda label, value: value
        config = {'quota': 0, 'size': 0, 'rate': 0, 'requests': 0}
        with patch.object(runner, 'Probe') as peers:
            peers.return_value.admin.return_value = {'httpStatus': 200, 'response': config}
            self.assertEqual(runner.quota_status(probe, fleet, self.buckets, 'quota'),
                             {bucket: config for bucket in self.buckets})
        self.assertEqual(peers.call_count, 8)
        self.assertEqual([call.args[1] for call in peers.call_args_list], list(probe.binding['endpoints']) * 2)
        self.assertEqual(peers.return_value.admin.call_count, 8)
        for index, call in enumerate(peers.return_value.admin.call_args_list):
            self.assertEqual(call.args, ('/rustfs/admin/v3/get-bucket-quota',
                                         [('bucket', self.buckets[index // 4])], b''))
            self.assertEqual(call.kwargs, {'method': 'GET'})

    def test_quota_config_retries_only_same_bucket_metadata_convergence(self):
        probe, fleet = Mock(), Mock()
        fleet.record.side_effect = lambda label, value: value
        not_ready = {'httpStatus': 404, 'response':
                     '<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>bucket not found: '
                     + self.buckets[0] + '</Message></Error>'}
        ready = {'httpStatus': 200, 'response': {'quota': 0, 'size': 0, 'rate': 0, 'requests': 0}}
        probe.admin.side_effect = [not_ready, ready]
        with patch.object(runner.time, 'sleep') as sleep:
            self.assertEqual(runner.quota_config(probe, fleet, self.buckets[0], 'quota'), ready['response'])
        sleep.assert_called_once_with(1)
        with self.assertRaises(ValueError):
            runner.quota_config(Mock(admin=Mock(return_value={'httpStatus': 404, 'response':
                '<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>bucket not found: foreign</Message></Error>'}),
                binding={}), fleet, self.buckets[0], 'quota')

    def test_quota_config_rejects_changed_limits_bool_and_peer_refusal(self):
        probe, fleet = Mock(), Mock()
        probe.binding = {'endpoints': {'node1': 'a', 'node2': 'b'}}
        fleet.record.side_effect = lambda label, value: value
        good = {'httpStatus': 200, 'response': {'quota': 0, 'size': 0, 'rate': 0, 'requests': 0}}
        for drift in ('limit', 'bool', 'soft', 'missing', 'http', 'usage'):
            changed = copy.deepcopy(good)
            if drift == 'limit': changed['response'].update(quota=1024, size=1024, quotatype='hard')
            elif drift == 'bool': changed['response']['quota'] = False
            elif drift == 'soft': changed['response']['quotatype'] = 'soft'
            elif drift == 'missing': changed['response'].pop('requests')
            elif drift == 'http': changed['httpStatus'] = 503
            else: changed['response'] = {'bucket': self.buckets[0], 'quota': None, 'quota_type': 'HARD'}
            with self.subTest(drift=drift), patch.object(runner, 'Probe') as peers:
                peers.return_value.admin.side_effect = [good, changed]
                with self.assertRaises(ValueError): runner.quota_status(probe, fleet, self.buckets, 'quota')

    def test_h5_pool_observations_accept_only_exact_signed_get_queries(self):
        binding = {'schemaVersion': 1, 'runId': 'gh-123-1-h5', 'jumpHostOnly': True, 'exclusiveHealService': True,
            'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
            'bucketPrefix': self.buckets[0], 'endpoints': {f'node{i}': f'http://rustfs-node{i}:9000' for i in range(1, 5)},
            'defaultEndpoint': 'node1', 'candidateCommit': 'a' * 40, 'harnessCommit': 'b' * 40,
            'candidatePackageSha256': 'c' * 64, 'candidateBinarySha256': 'd' * 64, 'rcBinarySha256': 'e' * 64,
            'maxInventoryObjects': 256, 'maxInventoryBytes': 512 * 1024**2}
        for path, query, method, auth, allowed in (
                ('list', [], 'GET', 'signed', True), ('status', [('pool', '1'), ('by-id', 'true')], 'GET', 'signed', True),
                ('list', [], 'POST', 'signed', False), ('list', [], 'GET', 'anonymous', False),
                ('list', [('pool', '0')], 'GET', 'signed', False), ('status', [('pool', '2'), ('by-id', 'true')], 'GET', 'signed', False),
                ('status', [('pool', '0'), ('by-id', 'true'), ('pool', '1')], 'GET', 'signed', False)):
            probe = runner.Probe(binding)
            with self.subTest(path=path, query=query, method=method, auth=auth), patch.object(probe, 'credentials', side_effect=RuntimeError('allowed route reached signing')):
                if allowed:
                    with self.assertRaisesRegex(RuntimeError, 'reached signing'):
                        probe.admin('/rustfs/admin/v3/pools/' + path, query, b'', method=method, auth=auth)
                else:
                    with self.assertRaises(ValueError): probe.admin('/rustfs/admin/v3/pools/' + path, query, b'', method=method, auth=auth)

    def test_original_h2_mapper_rejects_multi_pool_without_explicit_authority(self):
        snapshot, expected = self.fixture()
        with self.assertRaises(ValueError): validate_topology(snapshot, 'H2')
        with self.assertRaises(ValueError): map_target(snapshot, self.buckets[0], 'p1/s1.bin', 'data', 1)
        before = copy.deepcopy(snapshot)
        layout, mapped = runner.map_versions(snapshot, expected)
        self.assertEqual(snapshot, before)
        self.assertEqual(layout['independentDrives'], 16)
        self.assertEqual(len(mapped), 16)
        self.assertEqual({(row['poolIndex'], row['setIndex']) for row in mapped.values()}, {(0, 0), (0, 1), (1, 0), (1, 1)})
        self.assertTrue(all(len(row['cohort']) == len({member['node'] for member in row['cohort']}) == 4 for row in mapped.values()))

    def test_rejects_missing_original_drives_wrong_pools_aliases_and_host_local_sets(self):
        for kind in ('drive', 'pool', 'alias', 'host-local', 'duplicate'):
            snapshot, _ = self.fixture()
            if kind == 'drive': snapshot['node1']['formats'].pop()
            elif kind == 'pool': snapshot['node1']['formats'][2]['value']['xl']['sets'] = copy.deepcopy(snapshot['node1']['formats'][0]['value']['xl']['sets'])
            elif kind == 'alias': snapshot['node1']['formats'][2]['value']['xl']['this'] = snapshot['node1']['formats'][0]['value']['xl']['this']
            elif kind == 'duplicate': snapshot['node1']['formats'][0]['disk'] = 1
            else:
                sets = [[f'p0-s{group}-n{node}' for group in range(2) for node in range(start, start + 2)] for start in (0, 2)]
                for value in snapshot.values():
                    for fmt in value['formats'][:2]: fmt['value']['xl']['sets'] = sets
            with self.subTest(kind=kind), self.assertRaises(ValueError): validate(snapshot)

    def test_missing_ack_version_wrong_role_geometry_or_extra_pool_authority_fail(self):
        for kind in ('missing', 'role', 'geometry', 'authority', 'unpopulated'):
            snapshot, expected = self.fixture()
            entry = next(row for row in snapshot['node1']['files'] if row['path'] == self.buckets[0] + '/p1/s1.bin/xl.meta')
            if kind == 'missing': entry['metadata']['versions'].clear()
            elif kind == 'role': entry['metadata']['versions'][0]['role'] = 'parity'
            elif kind == 'geometry': entry['metadata']['versions'][0]['erasure']['parity_blocks'] = 3
            elif kind == 'authority': snapshot['node1']['files'].append({**copy.deepcopy(entry), 'disk': 0})
            else: expected[self.buckets[0]] = [row for row in expected[self.buckets[0]] if row['key'] != 'p0/s0.bin']
            with self.subTest(kind=kind), self.assertRaises(ValueError): runner.map_versions(snapshot, expected)

    def test_raw_pool_only_receipts_and_exact_version_scope_are_not_rewritten(self):
        current, mapped, selected, repaired, options = self.receipt('selected-pool')
        original = copy.deepcopy(current)
        proof = runner.responsibility(current, mapped, selected, repaired, options)
        self.assertEqual(current, original)
        self.assertTrue(proof['rawUnrestrictedSetNotRewritten'])
        self.assertEqual({row['setIndex'] for row in proof['physicalScopeBindings']}, {0, 1})
        self.assertEqual(proof['repaired'], 4)
        self.assertEqual(proof['versions'], 8)

    def test_wrong_selectors_versions_responsibility_and_false_counts_fail(self):
        for kind in ('pool', 'set', 'bool', 'version', 'duplicate', 'missing', 'foreign', 'unknown', 'counter', 'settings', 'truncated', 'incarnation'):
            current, mapped, selected, repaired, options = self.receipt('selected-set')
            item = current['outcome']['objects'][0]
            if kind in ('pool', 'set', 'bool', 'version', 'incarnation'):
                name, value = {'pool': ('poolIndex', None), 'set': ('setIndex', 0), 'bool': ('poolIndex', True),
                    'version': ('versionId', 'foreign'), 'incarnation': ('bucketIncarnationId', '')}[kind]
                item['identity'][name] = value
            elif kind == 'duplicate': current['outcome']['objects'][-1] = copy.deepcopy(item)
            elif kind == 'missing': current['outcome']['objects'].pop()
            elif kind == 'foreign': item['identity']['bucket'] = 'foreign'
            elif kind == 'unknown': item['disposition']['state'] = 'unknown'
            elif kind == 'counter': current['outcome']['counters']['healed'] += 1
            elif kind == 'settings': current['settings'] = {**options, 'set': 0}
            else: current['outcome']['objectsTruncated'] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError): runner.responsibility(current, mapped, selected, repaired, options)

    def test_control_missing_part_is_preserved_until_selected_physical_proof(self):
        original, expected = self.fixture()
        _, mapped = runner.map_versions(original, expected)
        _, selected, targets = runner.selection(mapped, 'selected-set')
        after = copy.deepcopy(original)
        missing = {(target['node'], target['disk'], target['relativePath']) for target in targets
                   if (target['bucket'], target['key'], target['versionId']) not in selected}
        for node, observed in after.items():
            observed['files'] = [row for row in observed['files'] if (node, row['disk'], row['path']) not in missing]
        proof = physical_selection(original, after, targets, {identity[:2] for identity in selected})
        self.assertEqual(proof['selectedReconstructedParts'], 2)
        self.assertEqual(proof['unselectedStillMissingParts'], 4)
        identity = next(iter(missing))
        after[identity[0]]['files'].append(files(original)[identity])
        with self.assertRaises(ValueError): physical_selection(original, after, targets, {identity[:2] for identity in selected})

    def seed_row(self, version='v1', resolved=False):
        params = {'Bucket': self.buckets[1], 'Key': 'object', 'ContentType': 'application/octet-stream', 'Metadata': {'nonce': version}}
        row = {'operation': 'put_object', 'params': params, 'payload': {'size': 1, 'sha256': version}, 'resolved': resolved,
            'observation': {'acknowledged': resolved, 'resultUnknown': not resolved, 'response': {'VersionId': version}}}
        entry = {'key': 'object', 'versionId': version, 'deleteMarker': False, 'isLatest': True, 'payload': row['payload'],
            'properties': {'Metadata': params['Metadata'], 'ContentType': params['ContentType']},
            'tags': [{'Key': 'fixture', 'Value': 'H06'}, {'Key': 'suite', 'Value': 'heal'}]}
        return row, entry

    def test_unknown_seed_is_resolved_as_committed_or_absent_without_retry(self):
        first, previous = self.seed_row('v1', True)
        pending, newest = self.seed_row('v2')
        rows = [first, pending]
        absent = runner.resolve_fixture_inventory(rows, {'entries': [previous]})
        self.assertEqual(absent[-1]['state'], 'ABSENT')
        previous['isLatest'] = False
        committed = runner.resolve_fixture_inventory(rows, {'entries': [previous, newest]})
        self.assertEqual(committed[-1]['versionId'], 'v2')

    def test_unknown_seed_drift_ack_replacement_tags_extra_version_and_latest_fail(self):
        for kind in ('payload', 'version', 'tags', 'extra', 'latest'):
            row, entry = self.seed_row('v1', True)
            if kind == 'payload': entry['payload'] = {'size': 2, 'sha256': 'drift'}
            elif kind == 'version': entry['versionId'] = 'replacement'
            elif kind == 'tags': entry['tags'] = []
            elif kind == 'latest': entry['isLatest'] = False
            entries = [entry, {**entry, 'versionId': 'extra'}] if kind == 'extra' else [entry]
            with self.subTest(kind=kind), self.assertRaises(ValueError): runner.resolve_fixture_inventory([row], {'entries': entries})

    def test_pending_registration_precedes_recorder_or_ack_validation_failure(self):
        ledger = []
        recorder = Mock()
        def fail(*args):
            self.assertEqual(len(ledger), 1)
            raise OSError('collector failed before durable receipt')
        recorder.call.side_effect = fail
        with self.assertRaises(OSError): runner.fixture_call(recorder, ledger, 'create_bucket', {'Bucket': self.buckets[0]})
        self.assertFalse(ledger[0]['resolved'])
        ledger = []
        recorder.call.side_effect = None
        recorder.call.return_value = {'VersionId': 'null'}
        with self.assertRaises(ValueError): runner.fixture_call(recorder, ledger, 'put_object', {'Bucket': self.buckets[1]}, b'x')
        self.assertFalse(ledger[0]['resolved'])

    def test_uncertain_bucket_mutations_and_cross_peer_divergence_retain_dirty_boundary(self):
        fleet = Mock()
        with self.assertRaises(ValueError): runner.resolve_pending_fixture(fleet, {}, [{'operation': 'create_bucket', 'resolved': False}])
        fleet.record.assert_not_called()
        row, entry = self.seed_row()
        binding = {'endpoints': {'node1': 'a', 'node2': 'b'}}
        fleet.record.side_effect = lambda label, value: value
        with patch.object(runner, 'Probe') as probe:
            probe.return_value.inventory.side_effect = [{'entries': [entry]}, {'entries': []}]
            with self.assertRaises(ValueError): runner.resolve_pending_fixture(fleet, binding, [row])
        self.assertFalse(row['resolved'])

    def test_node_redundancy_requires_actual_owned_stop_and_exact_reads_from_every_survivor(self):
        snapshot, expected = self.fixture()
        _, mapped = runner.map_versions(snapshot, expected)
        for entries in expected.values():
            for entry in entries: entry['payload'] = {'size': runner.SIZE, 'sha256': entry['key'] + entry['versionId']}
        binding = {'candidateBinarySha256': 'candidate', 'endpoints': {f'node{i}': f'http://node{i}:9000' for i in range(1, 5)}}
        for drift in (False, True):
            fleet = Mock(nodes=list(snapshot))
            fleet.record.side_effect = lambda label, value: value
            fleet.remote.side_effect = [
                {'pid': 5, 'startTicks': 1, 'unit': 'owned', 'bootId': 'boot', 'binarySha256': 'candidate'},
                {'quiescent': True, 'service': {'MainPID': '0'}}, {},
                {'pid': 6, 'startTicks': 2, 'unit': 'owned', 'bootId': 'boot', 'binarySha256': 'candidate'}]
            with patch.object(runner, 'endpoint_refused', return_value={'connectionRefused': True}), patch.object(runner, 'Probe') as probe:
                def read(operation, params):
                    entry = next(item for item in expected[params['Bucket']] if item['key'] == params['Key'] and item['versionId'] == params['VersionId'])
                    return {'acknowledged': True, 'resultUnknown': False, 'payload': {} if drift else entry['payload'],
                            'response': {'VersionId': entry['versionId']}}
                probe.return_value.s3.side_effect = read
                if drift:
                    with self.assertRaises(ValueError): runner.witness_peer_loss(fleet, binding, expected, mapped)
                else:
                    proof = runner.witness_peer_loss(fleet, binding, expected, mapped)
                    self.assertEqual(proof['exactVersions'], 16)
                    self.assertEqual(probe.return_value.s3.call_count, 48)
                    self.assertEqual({call.args[1] for call in probe.call_args_list}, {'node1', 'node2', 'node3'})

    def test_unresolved_seed_prevents_namespace_cleanup_and_next_branch_admission(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            candidate = directory / 'candidate.json'
            candidate.write_text(__import__('json').dumps({'source_sha': runner.REFERENCE['candidateCommit'], 'package_sha256': 'package'}))
            rc = directory / 'rc'
            rc.write_bytes(b'collector')
            args = types.SimpleNamespace(evidence=directory / 'evidence', tool_directory=directory,
                candidate=candidate, binary_sha='binary', testing_commit='harness', rc_bin=rc)
            fleet = Mock(nodes=['node1', 'node2', 'node3', 'node4'], quorum_gates=set())
            fleet.record.side_effect = lambda label, value: value
            def failed_seed(probe, path, buckets, ledger):
                ledger.append({'operation': 'create_bucket', 'params': {'Bucket': buckets[0]}, 'resolved': False})
                raise OSError('unknown initial bucket mutation')
            with patch.dict(runner.os.environ, {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}), \
                    patch.object(runner, 'Fleet', return_value=fleet), patch.object(runner, 'verify_inspector', return_value={}), \
                    patch.object(runner, 'Probe'), patch.object(runner, 'seed_fixtures', side_effect=failed_seed):
                result = runner.run_branch(args, fleet.nodes, 'selected-pool')
            self.assertEqual(result['verdict'], 'FAIL')
            self.assertEqual(result['cleanup'], 'FAIL')
            self.assertEqual(result['seedResolution']['state'], 'UNRESOLVED')
            fleet.cleanup.assert_not_called()
            self.assertTrue(any(call.args[0] == 'retained-unresolved-seed' for call in fleet.record.call_args_list))


if __name__ == '__main__':
    unittest.main()
