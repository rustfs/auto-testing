"""Interrupted opaque probes restore only the two archived pre-task file faults."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import heal_fault, interrupted_recovery, outcome_recovery, recover_environment, retained_scene
from scripts.validation_heal.healthy import physical_content
from scripts.validation_heal.shards import map_target
from scripts.validation_heal.versions import metadata_target
from test_validation_heal import versioned_scene
import test_validation_heal_retained_scene as scene_tests


def emit(path, value):
    path.write_text(json.dumps(value))


class OpaqueRecoveryTests(unittest.TestCase):
    def fixture(self, root):
        case = 'heal.outcome-counters-and-error-truth'
        base = root / 'artifact/run' / case
        folder = base / 'evidence' / case / 'DB/H1/unknown-negative'
        folder.mkdir(parents=True); (base / 'cases').mkdir()
        candidate = dict(source_sha=outcome_recovery.CANDIDATE, package_sha256='a' * 64)
        provenance = dict(source_commit=candidate['source_sha'], candidate_sha256='a' * 64,
                          testing_commit='b' * 40, binary_sha256='c' * 64)
        emit(base / 'cases' / (case + '.json'), dict(case_id=case, verdict='FAIL', cleanup_status='FAIL', provenance=provenance))
        failure = dict(type='ValueError', message='Heal node vm000 snapshot failed: ValueError: metadata decoder failed outside an exact witnessed file fault\n')
        branches = ('mixed-healthy-repaired-failed', 'dry-run', 'deferred', 'readonly-terminal-failure', 'unknown-negative')
        rows = [dict(topology='H1', fixture='DB', branch=branch, verdict='FAIL',
                     cleanup='FAIL' if branch == branches[-1] else 'PASS', failure=failure) for branch in branches]
        emit(base / 'evidence' / case / 'healthy-summary.json', dict(caseId=case, results=rows,
             cleanup='FAIL', complete=False, canonicalCasePassClaimed=False))
        original, bucket, expected = versioned_scene(['objects/0000.bin', 'opaque/source.bin'], sizes=(100,))
        original['vm000'] = original.pop('node1')
        original['vm000'].update(pid=0, quiescent=True, binarySha256='c' * 64)
        for entry in expected:
            entry.update(versionId='null', isLatest=True); entry['payload']['sha256'] = 'd' * 64
        for entry in original['vm000']['files']:
            if 'metadata' in entry:
                entry['metadata']['versions'][0].update(versionId='null', isLatest=True)
                entry['sha256'] = hashlib.sha256(str(entry['disk']).encode()).hexdigest()
        run_id = 'gh-12345-1-h1'
        owner = dict(runId=run_id, bucket=bucket, runtimeProfile='outcome-legacy', binarySha256='c' * 64, mounts=[{}] * 4)
        labels = ['binding', 'inspector-build', 'preflight', 'integrity-fleet-attestation', 'reserved', 'inspector-upload',
                  'started', 'deployed-topology', 'topology-proof', 'ACK-bound-full-S3', 'complete-ACK-manifest', 'trigger',
                  'control', 'control-repeat', 'control-repeat', 'task-exit-background', 'quiescent', 'started',
                  'original-opaque-ACK-full-S3', 'original-opaque-complete-ACK-manifest', 'trigger', 'control',
                  'control-repeat', 'control-repeat', 'task-exit-background', 'quiescent', 'original-opaque-complete-physical',
                  *(['original-opaque-full-cohort-bitrot'] * 4), 'original-opaque-metadata-fault-plan',
                  'original-opaque-byte-zero-bitflip', 'original-opaque-physical-injection', 'original-opaque-byte-zero-bitflip',
                  'failure-scene-error', 'quiescent', 'retain-unknown-seed-receipt', 'branch-result']
        data = {1: dict(runId=run_id, bucketPrefix=bucket, candidateCommit=candidate['source_sha'],
                       candidatePackageSha256='a' * 64, candidateBinarySha256='c' * 64, harnessCommit='b' * 40),
                5: dict(node='vm000', reserved=True, owner=owner), 19: dict(entries=expected),
                20: dict(expected=expected), 27: original,
                38: dict(state='DIRTY', namespacesRetained=True,
                         request=dict(operation='original-corrupt-metadata-source-probe', bucket=bucket))}
        target = map_target(original, bucket, 'opaque/source.bin', 'data', 1, 'null', latest_required=False)
        targets = [metadata_target(original, {**target, **member}) for member in target['cohort']]
        data[32] = targets
        first = copy.deepcopy(original)
        for index, number in enumerate((33, 35)):
            selected = targets[index]; fault_id = heal_fault.bounded_fault_id('J02', 'original-opaque', index)
            receipt = dict(runId=run_id, faultId=fault_id, node='vm000', relativePath=selected['relativePath'], kind='bitflip',
                offset=0, before={k: selected[k] for k in ('size', 'sha256')},
                fault=dict(size=selected['size'], sha256=str(index + 4) * 64), backup=fault_id + '.original',
                requiresQuiescentService=True, backend='logical-file-fault', witnessedAt='timestamp')
            data[number] = receipt
            if index == 0:
                entry = next(e for e in first['vm000']['files'] if e['disk'] == selected['disk'] and e['path'] == selected['relativePath'])
                entry.pop('metadata'); entry.update(receipt['fault'], metadataError=dict(code=1, message='Error: FileCorrupt\n',
                                                    exactWitnessedFault=True, validMetadataClaimed=False))
                data[34] = first
        for index, label in enumerate(labels, 1): emit(folder / f'{index:05d}-{label}.json', data.get(index, {}))
        for name, operations in (('fixture', ('create_bucket', 'put_object')), ('original-opaque-fixture', ('put_object',))):
            ledger = folder / name; ledger.mkdir()
            for seq, operation in enumerate(operations, 1):
                params = dict(Bucket=bucket); response = dict(ResponseMetadata=dict(HTTPStatusCode=200))
                request = dict(sequence=seq, runId=run_id, operation=operation, params=params)
                if operation == 'put_object':
                    entry = expected[0 if name == 'fixture' else 1]
                    params['Key'] = entry['key']; request['payload'] = entry['payload']; response['VersionId'] = 'null'
                path = ledger / f'{seq:06d}-request.json'; emit(path, request)
                emit(ledger / f'{seq:06d}-ack.json', dict(sequence=seq, runId=run_id, requestFile=path.name,
                    requestSha256=hashlib.sha256(path.read_bytes()).hexdigest(), operationCheck='PASS',
                    observation=dict(operation=operation, params=params, payload=request.get('payload'), response=response,
                                     acknowledged=True, clientCallInvoked=True, resultUnknown=False)))
        return candidate, folder

    def test_plan_binds_each_disk_to_its_original_receipt_and_rejects_unknown_boundaries(self):
        for poison in (None, 'candidate', 'unknown-ack', 'request-hash', 'extra-delete', 'extra-ledger', 'later-task',
                       'task-directory', 'replaced-bitrot', 'fault-id', 'before-hash', 'fault-offset', 'first-witness', 'lost-format', 'prior-cleanup', 'owner'):
            with self.subTest(poison=poison), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); candidate, folder = self.fixture(root)
                def change(pattern, function):
                    path = next(folder.glob(pattern)); value = json.loads(path.read_text()); function(value); emit(path, value)
                if poison == 'candidate': candidate['source_sha'] = 'e' * 40
                elif poison == 'unknown-ack': change('original-opaque-fixture/*-ack.json', lambda x: x['observation'].update(resultUnknown=True))
                elif poison == 'request-hash': change('fixture/*-ack.json', lambda x: x.update(requestSha256='f' * 64))
                elif poison == 'extra-delete': emit(folder / 'fixture/000003-request.json', {})
                elif poison == 'extra-ledger': emit(folder / 'fixture/unknown.json', {})
                elif poison == 'later-task': emit(folder / '00040-trigger.json', {})
                elif poison == 'task-directory': (folder / 'admin').mkdir()
                elif poison == 'replaced-bitrot': next(folder.glob('00031-*')).rename(folder / '00031-trigger.json')
                elif poison == 'fault-id': change('00035-*', lambda x: x.update(faultId=heal_fault.bounded_fault_id('J02', 'original-opaque', 0)))
                elif poison == 'before-hash': change('00035-*', lambda x: x['before'].update(sha256='e' * 64))
                elif poison == 'fault-offset': change('00035-*', lambda x: x.update(offset=1))
                elif poison == 'first-witness': change('00034-*', lambda x: next(e for e in x['vm000']['files'] if 'metadataError' in e)['metadataError'].update(exactWitnessedFault=False))
                elif poison == 'lost-format': change('00027-*', lambda x: x['vm000']['formats'].pop())
                elif poison == 'owner': change('00005-*', lambda x: x['owner'].update(runtimeProfile='manual'))
                elif poison == 'prior-cleanup':
                    path = folder.parents[2] / 'healthy-summary.json'; value = json.loads(path.read_text())
                    value['results'][0]['cleanup'] = 'FAIL'; emit(path, value)
                if poison:
                    with self.assertRaises((ValueError, KeyError)): recover_environment.recovery_plan(root, 12345, candidate)
                else:
                    plan = recover_environment.recovery_plan(root, 12345, candidate)[0]
                    self.assertEqual(plan['kind'], 'interrupted-j02-opaque')
                    self.assertEqual([f['disk'] for f in plan['faults']], [0, 1])
                    self.assertEqual(len(plan['metadataFaults']['vm000']), 2)
                    self.assertNotEqual(*[f['receipt']['fault']['sha256'] for f in plan['faults']])
                    for current in (plan['failure'], plan['before']): self.assertTrue(recover_environment.recovery_scene_matches(current, plan))
                    partial = copy.deepcopy(plan['failure']); key = plan['faults'][0]['receipt']['relativePath']
                    for entry in partial['vm000']['files']:
                        if entry['path'] == key and entry['disk'] == 0:
                            entry.clear(); entry.update(copy.deepcopy(next(e for e in plan['before']['vm000']['files'] if e['path'] == key and e['disk'] == 0)))
                    self.assertTrue(recover_environment.recovery_scene_matches(partial, plan))
                    for drift in ('control', 'unknown-meta', 'format', 'extra-file', 'lost-file'):
                        current = copy.deepcopy(partial)
                        if drift == 'control': next(e for e in current['vm000']['files'] if 'objects/0000.bin' in e['path'])['sha256'] = 'e' * 64
                        elif drift == 'unknown-meta': next(e for e in current['vm000']['files'] if 'metadataError' in e)['sha256'] = 'e' * 64
                        elif drift == 'format': current['vm000']['formats'].pop()
                        elif drift == 'extra-file': current['vm000']['files'].append(copy.deepcopy(current['vm000']['files'][0]))
                        elif drift == 'lost-file': current['vm000']['files'].pop()
                        with self.subTest(drift=drift): self.assertFalse(recover_environment.recovery_scene_matches(current, plan))

    def test_restore_passes_disk_witnesses_and_exact_intent_then_archives_only_restored_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); candidate, _ = self.fixture(root)
            plan = recover_environment.recovery_plan(root, 12345, candidate)[0]
            current = copy.deepcopy(plan['failure']); events = []
            def remote(node, action, **params):
                events.append(action)
                if action == 'stop': return {}
                if action == 'snapshot':
                    self.assertEqual(params, dict(quiescent=True, **({'metadataFaults': plan['metadataFaults'][node]} if len(events) == 2 else {})))
                    return copy.deepcopy(current[node])
                if action == 'recover-fault':
                    fault = next(f for f in plan['faults'] if f['faultId'] == params['faultId'])
                    self.assertEqual(params['disk'], fault['disk'])
                    self.assertEqual(params['expectedReceiptSha256'], outcome_recovery.digest({k: v for k, v in fault['receipt'].items() if k != 'witnessedAt'}))
                    entry = next(e for e in current[node]['files'] if e['disk'] == fault['disk'] and e['path'] == fault['receipt']['relativePath'])
                    entry.clear(); entry.update(copy.deepcopy(next(e for e in plan['before'][node]['files'] if e['disk'] == fault['disk'] and e['path'] == fault['receipt']['relativePath'])))
                    return dict(runId=plan['runId'], faultId=fault['faultId'], physicalRepairPassAllowed=False)
                self.assertEqual(physical_content(current), physical_content(plan['before']))
                return dict(baseline=dict(originalSceneSha256=params['expectedOriginalSceneSha256']), inspectionOnly=True,
                            mutated=False, activeNamespaceIsolated=True, canonicalPassClaimed=False)
            fleet = types.SimpleNamespace(remote=remote, record=lambda label, value: value)
            states = [dict(node='vm000', state=dict(exists=True), roots=[dict(exists=True)] * 4)]
            result = interrupted_recovery.restore_and_isolate(fleet, plan, states, recover_environment.recovery_scene_matches)
            self.assertEqual(result['cleanup'], 'ISOLATED'); self.assertFalse(result['canonicalPassClaimed'])
            self.assertEqual(events[:4], ['stop', 'snapshot', 'recover-fault', 'recover-fault'])
            self.assertFalse(set(events) & {'start', 'cleanup', 'journal-recover'})

    def test_real_opaque_archive_retries_all_moves_and_hashes_fault_backups_without_credentials(self):
        helper = scene_tests.RetainedSceneTests()
        for lost_after in range(1, 6):
            with self.subTest(lost_after=lost_after), tempfile.TemporaryDirectory() as temporary:
                state, roots, node, request = helper.scene(temporary)
                roots[0].mkdir(parents=True); owner = request['expectedOwner']; owner['runtimeProfile'] = 'outcome-legacy'
                for folder in (*roots, state): emit(folder / 'owner.json', owner)
                fault_id = heal_fault.bounded_fault_id('J02', 'original-opaque', 0)
                for suffix in ('original', 'intent.json', 'applied.json', 'failed-' + 'a' * 64):
                    (roots[1] / 'quarantine' / (fault_id + '.' + suffix)).write_bytes(b'fault-evidence')
                snapshot = dict(files=[], formats=[{}] * 4, pid=0, quiescent=True, binarySha256=owner['binarySha256'])
                node['snapshot'] = lambda value: snapshot
                request['expectedOriginalSceneSha256'] = outcome_recovery.digest({k: snapshot[k] for k in ('files', 'formats')})
                opened = []; original_open = retained_scene.os.open
                def checked_open(path, *args, **kwargs):
                    opened.append(str(path)); return original_open(path, *args, **kwargs)
                with helper.root_stats(), patch.object(retained_scene.os, 'open', side_effect=checked_open), patch.object(retained_scene, 'rename_new', side_effect=helper.local_rename):
                    baseline = retained_scene.run(request, node)['baseline']
                    self.assertTrue(all(any(fault_id + '.' + suffix in p for p in opened) for suffix in ('original', 'intent.json', 'applied.json', 'failed-' + 'a' * 64)))
                    self.assertFalse(any('service.env' in p or '/iam/credential' in p for p in opened))
                    count = 0
                    def move(source, target):
                        nonlocal count
                        helper.local_rename(source, target)
                        if target.is_dir():
                            count += 1
                            if count == lost_after: raise RuntimeError('lost ACK')
                    change = dict(request, action='retained-scene-isolate', expectedBaseline=baseline)
                    with patch.object(retained_scene, 'rename_new', side_effect=move), self.assertRaises(RuntimeError): retained_scene.run(change, node)
                    node['snapshot'] = lambda value: self.fail('partial archive must use durable original digest')
                    result = retained_scene.run(change, node)
                    self.assertEqual(result['locations'], ['retained'] * 5); self.assertEqual(result['baseline'], baseline)
                    self.assertFalse(result['canonicalPassClaimed']); self.assertFalse(result['credentialContentsRead'])

    def test_real_file_recovery_preserves_corrupt_bytes_and_refuses_later_data(self):
        for drift in (None, 'intent', 'backup', 'target'):
            with self.subTest(drift=drift), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(); (root / 'quarantine').mkdir()
                bucket = 'rv-heal-0123456789abcdef'; relative = bucket + '/opaque/source.bin/xl.meta'
                target = root / 'data' / relative; target.parent.mkdir(parents=True)
                original = b'XL2 original acknowledged metadata'; target.write_bytes(original)
                owner = dict(runId='gh-12345-1-h1', bucketPrefix=bucket)
                fault_id = heal_fault.bounded_fault_id('J02', 'original-opaque', 1)
                with patch.object(heal_fault, 'mount_id', return_value=1), heal_fault.directory(root) as fd:
                    receipt = heal_fault.apply_fault(fd, owner, relative, fault_id, 'bitflip', hashlib.sha256(original).hexdigest(), 0)
                    corrupt = target.read_bytes(); receipt_sha = outcome_recovery.digest({k: v for k, v in receipt.items() if k != "witnessedAt"})
                    if drift == 'intent': receipt_sha = 'f' * 64
                    elif drift == 'backup': (root / 'quarantine' / (fault_id + '.original')).write_bytes(b'foreign backup')
                    elif drift == 'target': target.write_bytes(b'later acknowledged bytes')
                    before = target.read_bytes()
                    if drift:
                        with self.assertRaises(ValueError): heal_fault.recover_fault(fd, owner, fault_id, expected_receipt_sha256=receipt_sha)
                        self.assertEqual(target.read_bytes(), before)
                        continue
                    proof = heal_fault.recover_fault(fd, owner, fault_id, expected_receipt_sha256=receipt_sha)
                    self.assertTrue(proof['restoredFromBackup']); self.assertFalse(proof['physicalRepairPassAllowed'])
                    self.assertEqual(target.read_bytes(), original)
                    preserved = root / 'quarantine' / (fault_id + '.failed-' + hashlib.sha256(corrupt).hexdigest())
                    self.assertEqual(preserved.read_bytes(), corrupt)
                    self.assertFalse(heal_fault.recover_fault(fd, owner, fault_id, expected_receipt_sha256=receipt_sha)['restoredFromBackup'])
