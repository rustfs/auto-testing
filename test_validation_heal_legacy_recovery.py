"""An acknowledged one-way phase before any legacy SDK write can only be isolated."""
import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import healthy, outcome_recovery, recover_environment, heal_fault
import test_validation_heal_outcome_recovery as opaque_tests


class LegacyRecoveryTests(unittest.TestCase):
    def fixture(self, root):
        candidate, folder = opaque_tests.OpaqueRecoveryTests().fixture(root)
        def read(path): return json.loads(path.read_text())
        records = sorted(folder.glob('*.json')); original = read(records[26]); targets = read(records[31])
        data = {i: read(p) for i, p in enumerate(records[:32], 1)}
        for path in records: path.unlink()
        labels = [p.name[6:-5] for p in records[:32]] + [
            'original-opaque-byte-zero-bitflip', 'original-opaque-physical-injection',
            'original-opaque-byte-zero-bitflip', 'original-opaque-physical-injection',
            'original-opaque-byte-zero-bitflip', 'original-opaque-physical-injection',
            'original-opaque-byte-zero-bitflip', 'original-opaque-physical-injection', 'started', 'trigger', 'control',
            'control-repeat', 'control-repeat', 'original-opaque-before-admission', 'trigger', 'control', 'control-repeat',
            'control-repeat', 'original-opaque-terminal-before-rescue', 'task-exit-background', 'quiescent',
            'original-opaque-after-task-before-rescue', *(['restore-fixture-without-relabeling-original-task'] * 4),
            'original-opaque-restored-original-bytes', 'started', 'original-opaque-restored-full-S3',
            'original-opaque-token-after-fixture-rescue', 'task-exit-background', 'quiescent',
            'original-opaque-after-restored-business-reads', 'protected-control-before-legacy-writing',
            'one-way-legacy-write-phase', 'failure-scene-error', 'quiescent', 'retain-unknown-seed-receipt', 'branch-result']
        candidate['source_sha'] = '1c7fc818dd858247c45b39542abd8291b1e6e31b'
        data[1]['candidateCommit'] = candidate['source_sha']; binding = data[1]
        for i, target in enumerate(targets):
            fault_id = heal_fault.bounded_fault_id('J02', 'original-opaque', i)
            data[33 + 2 * i] = dict(node='vm000', runId=binding['runId'], faultId=fault_id, relativePath=target['relativePath'],
                before={k:target[k] for k in ('sha256','size')}, kind='bitflip', offset=0)
            data[55 + i] = dict(node='vm000', runId=binding['runId'], faultId=fault_id,
                               restoredFromBackup=True, physicalRepairPassAllowed=False)
        for i in (59,65,66): data[i] = copy.deepcopy(original)
        data[61] = data[19]
        data[67] = dict(node='vm000', runId=binding['runId'], binarySha256=binding['candidateBinarySha256'],
                       phase='legacy', oneWay=True, candidateUnchanged=True, credentialsDisclosed=False, otherRuntimeSettingsUnchanged=True)
        data[70] = dict(state='DIRTY', namespacesRetained=True, request=dict(operation='append-real-legacy-fixture', bucket=binding['bucketPrefix']))
        for i,label in enumerate(labels,1): opaque_tests.emit(folder/f'{i:05d}-{label}.json',data.get(i,{}))
        case_path = next((root/'artifact/run/heal.outcome-counters-and-error-truth/cases').glob('*.json'))
        value=read(case_path); value['provenance']['source_commit']=candidate['source_sha']; opaque_tests.emit(case_path,value)
        path=folder.parents[2]/'healthy-summary.json';value=read(path)
        value['results'][-1]['failure']=dict(type='ValueError',message='Heal node vm000 start failed: ImportError: attempted relative import with no known parent package\n')
        opaque_tests.emit(path,value)
        return candidate,folder

    def test_exact_pre_sdk_plan_refuses_later_writes_or_changed_original_authority(self):
        for poison in (None,'source','unknown-ack','later-sdk','phase','control','rescue','fault-id','extra-task','prior-cleanup'):
            with self.subTest(poison=poison),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);candidate,folder=self.fixture(root)
                def edit(pattern,change):
                    path=next(folder.glob(pattern));value=json.loads(path.read_text());change(value);opaque_tests.emit(path,value)
                if poison=='source': candidate['source_sha']=outcome_recovery.CANDIDATE
                elif poison=='unknown-ack':edit('original-opaque-fixture/*-ack.json',lambda v:v['observation'].update(resultUnknown=True))
                elif poison=='later-sdk':(folder/'legacy-fixture').mkdir()
                elif poison=='phase':edit('00067-*',lambda v:v.update(oneWay=False))
                elif poison=='control':edit('00066-*',lambda v:v['vm000']['files'][0].update(sha256='f'*64))
                elif poison=='rescue':edit('00055-*',lambda v:v.update(restoredFromBackup=False))
                elif poison=='fault-id':edit('00037-*',lambda v:v.update(faultId='J02-'+'f'*32))
                elif poison=='extra-task':opaque_tests.emit(folder/'00072-trigger.json',{})
                elif poison=='prior-cleanup':
                    path=folder.parents[2]/'healthy-summary.json';value=json.loads(path.read_text());value['results'][0]['cleanup']='FAIL';opaque_tests.emit(path,value)
                if poison:
                    with self.assertRaises((ValueError,KeyError)):recover_environment.recovery_plan(root,12345,candidate)
                else:
                    plan=recover_environment.recovery_plan(root,12345,candidate)[0]
                    self.assertEqual(plan['kind'],'acknowledged-j02-before-legacy-put')
                    self.assertEqual(plan['nodes'],['vm000']);self.assertNotIn('faults',plan)

    def test_start_bundles_real_fault_dependency_for_common_or_node_specific_legacy_profile(self):
        for override in (False,True,None):
            with self.subTest(override=override),tempfile.TemporaryDirectory() as temporary:
                def execute(*args,**kw):
                    source=kw['input'].rsplit('\nmain(',1)[0];scope=dict(__name__='__standalone_node__')
                    exec(compile(source,'legacy-start-node','exec'),scope)
                    self.assertTrue(callable(scope['fault_module']().new_file))
                    return types.SimpleNamespace(returncode=0,stdout='{}',stderr='')
                fleet=healthy.Fleet(['vm000'],Path(temporary),'gh-12345-1-h1',runtime_profile='outcome-legacy' if override is False else 'manual')
                if override:fleet.node_runtime_profiles['vm000']='outcome-legacy'
                with patch.object(healthy.subprocess,'run',side_effect=execute):fleet.remote('vm000','start')
