"""Frozen G02 ACK/reference and decoded physical contracts; no live proof."""
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from scripts.validation_heal import race, mpu_race_backups as backups, heal_fault, cursor
from scripts.validation_heal.heal_workload import Recorder
import test_validation_heal_mpu_race_backups as backup_fixtures


class VersionPutSourceTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def baseline(self):
        entries = []
        for number, key in enumerate([f'objects/{i:04d}.bin' for i in range(3)] + [race.TARGET_KEY]):
            for generation in range(3 if key == race.TARGET_KEY else 1):
                nonce = f'{self.bucket}/{key}/{generation}'
                body = race.payload(nonce, race.SIZE)
                entries.append({'key': key, 'versionId': str(uuid.UUID(int=1 + number * 10 + generation)),
                    'isLatest': key != race.TARGET_KEY or generation == 2, 'deleteMarker': False,
                    'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()},
                    'properties': {'ETag': '"seed"', 'ContentLength': race.SIZE,
                                   'ContentType': 'application/octet-stream', 'Metadata': {'nonce': nonce}}, 'tags': []})
        return {'entries': entries}

    def test_original_three_puts_copy_distinct_immutable_control_payloads(self):
        for branch in race.G02_BRANCHES:
            baseline = self.baseline(); plan = race.operation_plan(self.bucket, branch, baseline['entries'])
            self.assertEqual(len(plan), 3)
            for i, step in enumerate(plan):
                control = baseline['entries'][i]
                self.assertEqual(step['referenceKey'], control['key'])
                body = race.payload(step['nonce'], race.SIZE)
                self.assertEqual({'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()}, control['payload'])

    def test_final_manifest_retains_every_original_and_serial_put_ack(self):
        for branch in race.G02_BRANCHES:
            baseline = self.baseline(); plan = race.operation_plan(self.bucket, branch, baseline['entries'])
            acks = [{'VersionId': str(uuid.UUID(int=100 + i)), 'ETag': f'"new-{i}"'} for i in range(3)]
            final = race.final_inventory(baseline, branch, plan, acks)
            target = [r for r in final['entries'] if r['key'] == race.TARGET_KEY]
            self.assertEqual([r['versionId'] for r in target],
                [r['versionId'] for r in baseline['entries'] if r['key'] == race.TARGET_KEY] + [a['VersionId'] for a in acks])
            self.assertEqual([r['isLatest'] for r in target], [False] * 5 + [True])
            self.assertEqual([r['payload'] for r in target[-3:]], [r['payload'] for r in baseline['entries'][:3]])
            self.assertEqual(baseline, self.baseline())

    def test_final_manifest_refuses_duplicate_original_nil_or_missing_new_version_acks(self):
        for kind in ('duplicate', 'original', 'nil', 'missing', 'reference-drift'):
            with self.subTest(kind=kind):
                baseline = self.baseline(); branch = race.G02_BRANCHES[0]
                plan = race.operation_plan(self.bucket, branch, baseline['entries'])
                acks = [{'VersionId': str(uuid.UUID(int=100 + i)), 'ETag': f'"new-{i}"'} for i in range(3)]
                if kind == 'duplicate': acks[1]['VersionId'] = acks[0]['VersionId']
                elif kind == 'original': acks[1]['VersionId'] = baseline['entries'][-1]['versionId']
                elif kind == 'nil': acks[1]['VersionId'] = str(uuid.UUID(int=0))
                elif kind == 'missing': del acks[1]['VersionId']
                else: plan[1]['nonce'] = 'foreign payload'
                with self.assertRaises(ValueError): race.final_inventory(baseline, branch, plan, acks)

    def test_public_manifest_sorts_uuid_identity_without_reordering_the_serial_ack_or_latest(self):
        baseline = self.baseline(); branch = race.G02_BRANCHES[0]
        plan = race.operation_plan(self.bucket, branch, baseline['entries'])
        acks = [{'VersionId': str(uuid.UUID(int=i)), 'ETag': f'"new-{i}"'} for i in (900, 700, 800)]
        final = race.final_inventory(baseline, branch, plan, acks)
        self.assertEqual(final['entries'], sorted(final['entries'], key=lambda r: (r['key'], r['versionId'])))
        self.assertEqual(final['acknowledgedVersionAppendOrder'], [a['VersionId'] for a in acks])
        latest = [r for r in final['entries'] if r['key'] == race.TARGET_KEY and r['isLatest']]
        self.assertEqual([r['versionId'] for r in latest], [acks[-1]['VersionId']])

    def scene(self, topology):
        baseline = self.baseline(); old = [r for r in baseline['entries'] if r['key'] == race.TARGET_KEY]
        new = [{**copy.deepcopy(baseline['entries'][i]), 'key': race.TARGET_KEY,
                'versionId': str(uuid.UUID(int=100 + i)), 'isLatest': i == 2} for i in range(3)]
        final = {'entries': [copy.deepcopy(r) for r in baseline['entries']] + new,
                 'acknowledgedVersionAppendOrder': [r['versionId'] for r in new]}
        for r in final['entries']:
            if r['key'] == race.TARGET_KEY: r['isLatest'] = r['versionId'] == new[-1]['versionId']
        nodes = ['vm000'] if topology == 'H1' else [f'vm00{i}' for i in range(4)]
        count = 4 * len(nodes); data = 2 if topology == 'H1' else 12
        disk_ids = [str(uuid.UUID(int=1000 + i)) for i in range(count)]
        distribution = list(range(count, 0, -1))
        def build(entries):
            result = {}
            for node_number, node in enumerate(nodes):
                value = result[node] = {'files': [], 'formats': []}
                for disk in range(4):
                    slot = node_number * 4 + disk; index = distribution[slot]
                    value['formats'].append({'disk': disk, 'value': {'format': 'xl', 'id': 'pool',
                        'xl': {'sets': [disk_ids], 'this': disk_ids[slot]}}})
                    for key in sorted({r['key'] for r in entries}):
                        versions = []
                        for row in (r for r in entries if r['key'] == key):
                            generation = str(uuid.UUID(int=10000 + uuid.UUID(row['versionId']).int))
                            version = {'versionId': row['versionId'], 'isLatest': row['isLatest'], 'deleted': False,
                                'dataDir': generation, 'size': race.SIZE, 'inline': None, 'usesLegacyChecksum': False,
                                'role': 'data' if index <= data else 'parity',
                                'parts': [{'number': 1, 'size': race.SIZE, 'actualSize': race.SIZE}],
                                'erasure': {'index': index, 'data_blocks': data, 'parity_blocks': count - data,
                                    'block_size': 1048576, 'distribution': distribution, 'algorithm': 'rs-vandermonde'}}
                            versions.append(version)
                            digest = hashlib.sha256((row['payload']['sha256'] + '/index/' + str(index)).encode()).hexdigest()
                            value['files'].append({'disk': disk, 'path': f'{self.bucket}/{key}/{generation}/part.1',
                                'size': (race.SIZE + data - 1) // data + 32, 'sha256': digest})
                        metadata = {'bucket': self.bucket, 'key': key, 'versions': versions}
                        value['files'].append({'disk': disk, 'path': f'{self.bucket}/{key}/xl.meta', 'size': 1000,
                            'sha256': hashlib.sha256(json.dumps(metadata, sort_keys=True).encode()).hexdigest(), 'metadata': metadata})
            return result
        before, after = build(baseline['entries']), build(final['entries'])
        return before, after, race.map_fixture(before, self.bucket, baseline['entries']), final

    def test_actual_transition_accepts_encoded_lengths_and_all_six_versions_in_h1_and_h2(self):
        for topology in ('H1', 'H2'):
            for branch in race.G02_BRANCHES:
                with self.subTest(topology=topology, branch=branch):
                    before, after, mapped, final = self.scene(topology)
                    survivors, proof = race.physical_transition(before, after, self.bucket, mapped, final, branch)
                    target = [v for (key, _), v in survivors.items() if key == race.TARGET_KEY]
                    self.assertEqual(len(target), 6)
                    self.assertTrue(all(p['size'] != race.SIZE for v in target for p in v['cohort']))
                    self.assertTrue(proof['physicalProofBeforeGETOrRescue'])

    def test_transition_rejects_wrong_new_or_retained_shards_metadata_and_unacknowledged_versions(self):
        for topology in ('H1', 'H2'):
            for fault in ('new-size', 'new-sha', 'old-sha', 'metadata-size', 'extra-version', 'missing-part', 'old-generation', 'ack-payload'):
                with self.subTest(topology=topology, fault=fault):
                    before, after, mapped, final = self.scene(topology)
                    files = after['vm000']['files']
                    meta = next(r for r in files if r['path'] == f'{self.bucket}/{race.TARGET_KEY}/xl.meta')
                    old_dir = meta['metadata']['versions'][0]['dataDir']; new_dir = meta['metadata']['versions'][-1]['dataDir']
                    part = next(r for r in files if r['path'] == f'{self.bucket}/{race.TARGET_KEY}/{new_dir}/part.1')
                    if fault == 'new-size': part['size'] = race.SIZE
                    elif fault == 'new-sha': part['sha256'] = 'f' * 64
                    elif fault == 'old-sha': next(r for r in files if r['path'] == f'{self.bucket}/{race.TARGET_KEY}/{old_dir}/part.1')['sha256'] = 'e' * 64
                    elif fault == 'metadata-size':
                        for scene in after.values():
                            m = next(r for r in scene['files'] if r['path'] == f'{self.bucket}/{race.TARGET_KEY}/xl.meta')
                            m['metadata']['versions'][-1]['size'] += 1
                    elif fault == 'extra-version': meta['metadata']['versions'].append({**copy.deepcopy(meta['metadata']['versions'][0]), 'versionId': str(uuid.UUID(int=999))})
                    elif fault == 'missing-part': files.remove(part)
                    elif fault == 'old-generation': meta['metadata']['versions'][-1]['dataDir'] = old_dir
                    else: final['entries'][-1]['payload']['sha256'] = 'd' * 64
                    with self.assertRaises(ValueError): race.physical_transition(before, after, self.bucket, mapped, final, race.G02_BRANCHES[0])

    def backup_scene(self, topology, prefix):
        before, after, mapped, final = self.scene(topology)
        old = [v['versionId'] for (key, _), v in mapped.items() if key == race.TARGET_KEY]
        new = final['acknowledgedVersionAppendOrder']; observations = {}
        for node in before:
            common = {'hostname': 'host-' + node, 'binarySha256': 'a' * 64, 'quiescent': True}
            before[node].update(common); after[node].update(common)
            observations[node] = {**common, 'inspectorSha256': 'b' * 64, 'rows': [], 'readOnly': True,
                'sameHeldFileHashedAndDecoded': True, 'rawContentsExported': False, 'liveAuthorityClaimed': False}
            for value in (before[node], after[node]):
                for row in value['files']:
                    if 'metadata' in row: row['metadata']['physicalBitrotVerified'] = False
            for disk in range(4):
                path = f'{self.bucket}/{race.TARGET_KEY}/xl.meta'
                primary = next(r['metadata'] for r in after[node]['files'] if r['disk'] == disk and r['path'] == path)
                versions = {v['versionId']: v for v in primary['versions']}
                ids = old + new[:prefix]
                decoded = {**primary, 'versions': [{**copy.deepcopy(versions[vid]), 'isLatest': vid == ids[-1]} for vid in ids]}
                for vid in old:
                    row = {'disk': disk, 'path': f'{self.bucket}/{race.TARGET_KEY}/{versions[vid]["dataDir"]}/xl.meta.bkp',
                           'size': 100 + prefix, 'sha256': hashlib.sha256(f'{disk}/{vid}/{prefix}'.encode()).hexdigest()}
                    after[node]['files'].append(copy.deepcopy(row))
                    observations[node]['rows'].append({**row, 'metadata': copy.deepcopy(decoded)})
        return before, after, observations, old, new

    def test_all_three_original_generation_backups_require_a_contiguous_three_ack_prefix(self):
        for topology in ('H1', 'H2'):
            for count in range(4):
                with self.subTest(topology=topology, prefix=count):
                    before, after, observed, old, new = self.backup_scene(topology, count)
                    result = backups.verify_versions(before, after, observed, list(before), self.bucket, race.TARGET_KEY,
                        old, new, 'a' * 64, 'b' * 64)
                    self.assertEqual(len(result['proofs']), 12 * len(before))
                    self.assertEqual({r['acknowledgedCompletePrefix'] for r in result['proofs']}, {count})
                    self.assertFalse(result['liveAuthorityClaimed'] or result['physicalRepairProved'] or result['canonicalCasePassClaimed'])
        for fault in ('nonprefix', 'missing-old', 'descriptor', 'latest', 'sha', 'new-directory', 'omitted', 'foreign-host'):
            with self.subTest(fault=fault):
                before, after, observed, old, new = self.backup_scene('H1', 1)
                row = observed['vm000']['rows'][0]
                if fault == 'nonprefix': row['metadata']['versions'][-1]['versionId'] = new[1]
                elif fault == 'missing-old': row['metadata']['versions'].pop(0)
                elif fault == 'descriptor': row['metadata']['versions'][0]['parts'][0]['size'] += 1
                elif fault == 'latest': row['metadata']['versions'][0]['isLatest'] = True
                elif fault == 'sha': row['sha256'] = 'e' * 64
                elif fault == 'new-directory':
                    old_path = row['path']; row['path'] = row['path'].replace(str(uuid.UUID(int=10031)), str(uuid.UUID(int=10100)))
                    next(r for r in after['vm000']['files'] if r['path'] == old_path and r['disk'] == row['disk'])['path'] = row['path']
                elif fault == 'omitted': observed['vm000']['rows'].pop()
                else: observed['vm000']['hostname'] = 'foreign'
                with self.assertRaises(ValueError):
                    backups.verify_versions(before, after, observed, list(before), self.bucket, race.TARGET_KEY, old, new, 'a' * 64, 'b' * 64)

    def test_nested_key_collector_binds_actual_open_fd_and_all_six_bounded_generations(self):
        with tempfile.TemporaryDirectory() as directory:
            helper = backup_fixtures.MultipartBackupTests()
            request, node, files, _ = helper.fixture(directory)
            for mount in node['MOUNTS']:
                parent = mount / 'heal-validation' / request['runId'] / 'data' / self.bucket
                (parent / 'objects').mkdir(); (parent / backups.KEY).rename(parent / race.TARGET_KEY)
                for i in range(101, 106): (parent / race.TARGET_KEY / str(uuid.UUID(int=i))).mkdir()
            request['action'] = 'version-race-backups'
            def command(tool, path, bucket, key, **kwargs):
                self.assertEqual((bucket, key), (self.bucket, race.TARGET_KEY))
                fd = int(path.rsplit('/', 1)[1]); raw = os.pread(fd, backups.LIMIT + 1, 0)
                return SimpleNamespace(returncode=0, stdout=json.dumps({'bucket': bucket, 'key': key,
                    'physicalBitrotVerified': False, 'heldFileSha256': hashlib.sha256(raw).hexdigest()}))
            node['command'] = command
            with patch.object(heal_fault, 'mount_id', side_effect=lambda fd: os.fstat(fd).st_dev):
                value = backups.run(request, node)
            self.assertEqual(len(value['rows']), 4)
            self.assertTrue(all(r['metadata']['heldFileSha256'] == r['sha256'] for r in value['rows']))
            self.assertFalse(value['liveAuthorityClaimed'] or value['rawContentsExported'])

    def test_actual_reader_keeps_durable_acks_and_final_round_for_all_old_and_new_versions(self):
        for fault in (None, 'version', 'payload', 'property', 'unknown'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                baseline = self.baseline(); original = [r for r in baseline['entries'] if r['key'] == race.TARGET_KEY]
                plan = race.operation_plan(self.bucket, race.G02_BRANCHES[1], baseline['entries'])
                acks = [{'VersionId': str(uuid.UUID(int=100 + i)), 'ETag': f'"new-{i}"'} for i in range(3)]
                added = race.version_put_entries(baseline, plan, acks)
                all_entries = {r['versionId']: r for r in original + added}
                lock = threading.Lock(); done = threading.Event()
                ledger = {'bucket': self.bucket, 'entries': copy.deepcopy(original)}; calls = []
                def invoke(operation, params, body):
                    self.assertEqual(operation, 'get_object'); calls.append(params['VersionId'])
                    entry = all_entries[params['VersionId']]
                    value = {'operation': operation, 'params': copy.deepcopy(params), 'acknowledged': True,
                        'resultUnknown': False, 'response': {'ResponseMetadata': {'HTTPStatusCode': 200},
                            'VersionId': entry['versionId'], **copy.deepcopy(entry['properties'])}, 'payload': copy.deepcopy(entry['payload'])}
                    if fault == 'version': value['response']['VersionId'] = str(uuid.UUID(int=999))
                    elif fault == 'payload': value['payload']['sha256'] = 'e' * 64
                    elif fault == 'property': del value['response']['ContentLength']
                    elif fault == 'unknown': value.update(acknowledged=False, resultUnknown=True)
                    if len(calls) == 3:
                        with lock: ledger['entries'] = copy.deepcopy(original + added)
                        done.set()
                    return value
                probe = SimpleNamespace(binding={'runId': 'gh-12345-1-h1', 'bucketPrefix': self.bucket,
                                                  'maxInventoryBytes': 512 * 1024**2}, s3=invoke)
                recorder = Recorder(probe, Path(directory) / 'reads')
                if fault is None:
                    proofs = race.read_versions(recorder, ledger, lock, done)
                    self.assertEqual(len(proofs), 9)
                    self.assertEqual(set(calls), set(all_entries))
                    self.assertEqual(len(list(recorder.output.glob('*-ack.json'))), 9)
                else:
                    with self.assertRaises(ValueError): race.read_versions(recorder, ledger, lock, done)
                    self.assertEqual(len(calls), 1)
                    self.assertEqual(len(list(recorder.output.glob('*-ack.json'))), 1)

    def test_existing_part_verifier_streams_all_259_seed_or_262_final_versions_in_h1_h2(self):
        for nodes in (['vm000'], [f'vm00{i}' for i in range(4)]):
            for count in (259, 262):
                with self.subTest(nodes=nodes, count=count):
                    cohorts = []; indexed = {}; calls = []
                    for number in range(count):
                        members = []
                        for node in nodes:
                            for disk in range(4):
                                row = {'node': node, 'disk': disk, 'relativePath': f'bucket/key/{number}/part.1',
                                    'versionId': str(uuid.UUID(int=number + 1)), 'partNumber': 1,
                                    'sha256': hashlib.sha256(f'{node}/{disk}/{number}'.encode()).hexdigest(), 'size': 131678}
                                members.append(row); indexed[node, disk, row['relativePath']] = row
                        cohorts.append({'cohort': members})
                    def remote(node, action, **params):
                        self.assertEqual(action, 'verify-parts'); calls.extend((node, r['disk'], r['relativePath']) for r in params['parts'])
                        return {'complete': True, 'parts': [{**indexed[node, r['disk'], r['relativePath']],
                            'physicalSize': indexed[node, r['disk'], r['relativePath']]['size'], 'physicalBitrotVerified': True} for r in params['parts']]}
                    fleet = SimpleNamespace(nodes=nodes, remote=remote, record=lambda label, value: value)
                    result = cursor.verify_part_manifest(fleet, cohorts, 'bounded-version-manifest', count=count)
                    self.assertEqual(result['allPartsStreamingVerified'], count * 4 * len(nodes))
                    self.assertEqual(len(calls), len(set(calls)))
                    for wrong_count in (False, -1, cursor.OBJECTS + 1, count - 1):
                        with self.assertRaises(ValueError): cursor.verify_part_manifest(fleet, cohorts, 'refused', count=wrong_count)

    def test_real_g02_caller_faults_the_original_oldest_version_in_both_public_branches(self):
        class FaultBoundaryReached(BaseException):
            pass
        for branch in race.G02_BRANCHES:
            with self.subTest(branch=branch), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); before, _, mapped, _ = self.scene('H1'); baseline = self.baseline()
                candidate = root / 'candidate.json'; candidate.write_text(json.dumps({'source_sha': race.REFERENCE['candidateCommit'], 'package_sha256': 'b' * 64}))
                rc = root / 'rc'; rc.write_bytes(b'fixture-rc')
                fleet = Mock(); fleet.faults = []; fleet.cleanup.return_value = True
                fleet.record.side_effect = lambda label, value: value
                fleet.snapshot.return_value = before
                chosen = mapped[race.TARGET_KEY, next(r['versionId'] for r in baseline['entries'] if r['key'] == race.TARGET_KEY)]
                def remote(node, action, **params):
                    self.assertEqual(action, 'apply-fault')
                    if params['relativePath'].startswith(f'{self.bucket}/{race.TARGET_KEY}/'):
                        self.assertEqual(params['relativePath'], chosen['relativePath'])
                        self.assertEqual(params['disk'], 3)
                        raise FaultBoundaryReached()
                    return {'kind': 'missing'}
                fleet.remote.side_effect = remote
                probe = Mock(); probe.inventory.return_value = baseline
                args = SimpleNamespace(evidence=root / 'evidence', candidate=candidate, rc_bin=rc,
                    tool_directory=root, testing_commit='c' * 40, binary_sha='a' * 64, case_id='heal.race-versioned-put')
                with patch.object(race, 'Fleet', return_value=fleet), patch.object(race, 'verify_inspector', return_value={}), \
                        patch.object(race, 'Probe', return_value=probe), patch.object(race, 'validate_topology'), \
                        patch.object(race, 'seed_fixture', return_value=baseline['entries']), patch.object(race, 'verify_seed'), \
                        patch.object(race, 'map_fixture', return_value=mapped), patch.object(race, 'OBJECTS', 4), \
                        patch.object(race, 'verify_endpoint_resolution'), \
                        patch.object(race, 'verify_part_manifest') as verify, \
                        patch.dict(race.os.environ, {'GITHUB_RUN_ID': '12345', 'GITHUB_RUN_ATTEMPT': '1'}):
                    # The actual binding selects a fresh bucket; its read-only
                    # physical plan here remains the known synthetic baseline.
                    actual_plan = race.operation_plan(self.bucket, branch, baseline['entries'])
                    with patch.object(race, 'operation_plan', return_value=actual_plan), self.assertRaises(FaultBoundaryReached):
                        race.run_branch(args, 'H1', ['vm000'], branch)
                    self.assertEqual(verify.call_args.kwargs['count'], len(mapped))

    def test_controller_requires_both_complete_six_version_reads_and_original_token_reader_overlap(self):
        for complete, reader_overlap, writer_overlap in ((True, True, True), (True, False, True),
                                                         (False, True, True), (True, True, False)):
            with self.subTest(complete=complete, reader_overlap=reader_overlap, writer_overlap=writer_overlap), tempfile.TemporaryDirectory() as directory:
                baseline = self.baseline(); branch = race.G02_BRANCHES[1]
                plan = race.operation_plan(self.bucket, branch, baseline['entries'])
                for step in plan: step['branch'] = branch
                output = Path(directory) / 'foreground'; output.mkdir()
                writer = SimpleNamespace(output=output, mutations=SimpleNamespace(poison=lambda message: None))
                sequence = 0
                def call(operation, params, body):
                    nonlocal sequence
                    sequence += 1
                    ack = {'VersionId': str(uuid.UUID(int=100 + sequence)), 'ETag': f'"new-{sequence}"'}
                    (output / f'{sequence:06d}-ack.json').write_text(json.dumps({'operationCheck': 'PASS',
                        'observation': {'operation': operation, 'params': params}}))
                    return ack
                writer.call = call
                sampled = threading.Event(); records = {}
                def observe(*args):
                    sampled.set(); return {'originalTokenSample': True}
                def read(recorder, ledger, lock, done):
                    self.assertTrue(done.wait(2)); self.assertTrue(sampled.wait(2))
                    with lock: entries = copy.deepcopy(ledger['entries'])
                    return [{'observation': {'operation': 'get_object', 'params': {'VersionId': row['versionId']}}}
                            for row in (entries if complete else entries[:3])]
                fleet = SimpleNamespace(record=lambda label, value: records.setdefault(label, value))
                probe = SimpleNamespace(binding={'runId': 'gh-12345-1-h1', 'bucketPrefix': self.bucket,
                                                  'maxInventoryBytes': 512 * 1024**2})
                def overlap(ack, *args):
                    return (writer_overlap and ack['observation']['params']['Metadata']['nonce'] == '1'
                            if ack['observation']['operation'] == 'put_object' else reader_overlap)
                with patch.object(race, 'observe', side_effect=observe), patch.object(race, 'read_versions', side_effect=read), \
                        patch.object(race, 'sample_overlap', side_effect=overlap):
                    responses, accepted = race.race(writer, plan, probe, fleet, '/original/path', 'original-token', baseline=baseline)
                self.assertEqual(len(responses), 3)
                self.assertEqual(accepted, complete and reader_overlap and writer_overlap)
                self.assertEqual(records['G02-historical-read-coverage']['complete'], complete)
                self.assertEqual(records['G02-historical-read-coverage']['readerCallOverlapObserved'], reader_overlap)

    def test_frozen_partial_window_requires_repaired_controls_32_pending_and_old_target_still_missing(self):
        keys = [f'objects/{i:04d}.bin' for i in range(32)] + [race.TARGET_KEY]
        targets = [{'key': key} for key in keys]
        scene = {'vm000': {'files': [], 'formats': []}}
        for kind in ('valid', '31-pending', 'no-repaired', 'target-repaired', 'all-repaired', 'root-finished', 'bool-progress'):
            with self.subTest(kind=kind):
                repaired = ({keys[0]} if kind not in {'31-pending', 'no-repaired', 'target-repaired', 'all-repaired'} else
                            set(keys[:2]) if kind == '31-pending' else set() if kind == 'no-repaired' else
                            {race.TARGET_KEY} if kind == 'target-repaired' else set(keys))
                def validate(before, after, target):
                    if target['key'] not in repaired: raise ValueError('original missing part remains absent')
                response = {'summary': 'finished' if kind == 'root-finished' else 'running',
                            'outcome': {'counters': {'processed': True if kind == 'bool-progress' else 1}}}
                fleet = SimpleNamespace(snapshot=lambda label: scene, record=lambda label, value: value)
                with patch.object(race, 'observe', return_value={'response': response}), \
                        patch.object(race, 'validate_repaired_physical', side_effect=validate), \
                        patch.object(race.time, 'monotonic', side_effect=[0, 0, 121]), patch.object(race.time, 'sleep'):
                    if kind == 'valid':
                        self.assertEqual(race.wait_version_window(None, fleet, '/original', 'token', {}, scene, targets, self.bucket), scene)
                    else:
                        with self.assertRaises(ValueError): race.wait_version_window(None, fleet, '/original', 'token', {}, scene, targets, self.bucket)


if __name__ == '__main__':
    unittest.main()
