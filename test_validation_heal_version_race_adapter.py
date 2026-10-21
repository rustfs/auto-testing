"""Real H1/H2 oracle adaptation, source metadata fault shape and negative probes."""
import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock,patch

import test_validation_heal_version_race_physical as fixtures
from scripts.validation_heal import version_race_adapter as adapter,version_race_physical as oracle
from scripts.validation_heal import version_race_fixture as model,version_race_runner as runner,race
from scripts.validation_heal.heal_workload import Recorder


def fleet_scene(entries,width=4):
    snapshot,metadata=fixtures.physical(entries)
    slots=[str(i) for i in range(width)];distribution=list(range(2,width+1))+[1]
    result={f'node{n}':{'formats':[],'files':[]} for n in range(1,2 if width==4 else 5)}
    for slot in range(width):
        node=f'node{slot%4+1}' if width==16 else 'node1';disk=slot//4 if width==16 else slot
        value=result[node]
        value['formats'].append({'disk':disk,'value':{'format':'xl','id':'pool','xl':{'this':slots[slot],'sets':[slots]}}})
        for row in snapshot['files']:
            if row['disk']!=0:continue
            current={**row,'disk':disk};current['sha256']=hashlib.sha256((row['sha256']+str(slot)).encode()).hexdigest()
            if row['path'].endswith('/xl.meta'):
                key=row['path'][len(fixtures.BUCKET)+1:].removesuffix('/xl.meta')
                decoded=copy.deepcopy(next(v['decoded'] for v in metadata['rows'] if v['disk']==0 and v['key']==key))
                for version in decoded['versions']:
                    if not version['deleted']:
                        version['erasure'].update(index=distribution[slot],distribution=distribution,
                            data_blocks=2 if width==4 else 12,parity_blocks=2 if width==4 else 4)
                current['metadata']=decoded
            value['files'].append(current)
    return result


class VersionAdapterTests(unittest.TestCase):
    def test_h1_and_interleaved_h2_map_real_set_slots_and_all_coding_indices(self):
        entries=[fixtures.entry(1,latest=True)]
        for width in (4,16):
            with self.subTest(width=width):
                scene=fleet_scene(entries,width)
                normalized,metadata=adapter.view(scene,fixtures.BUCKET,[fixtures.KEY],fixtures.INSPECTOR_SHA)
                proof=oracle.key_state(normalized,metadata,fixtures.BUCKET,fixtures.KEY,entries,
                    inspector_sha=fixtures.INSPECTOR_SHA,disk_count=width)
                self.assertEqual(set(proof['versions'][fixtures.uid(1)]['coding']),set(range(1,width+1)))
                self.assertTrue(proof['allLiveVersionsFullSet'])
                self.assertEqual(proof['allLiveVersionsFourShards'],width==4)
                if width==16:
                    self.assertEqual([row['disk'] for row in metadata['rows'][:4]],[0,4,8,12])

    def test_h2_missing_peer_geometry_duplicate_slot_and_wrong_inspector_are_refused(self):
        entries=[fixtures.entry(1,latest=True)]
        for fault in ('peer','slot','geometry','inspector','unknown-key','decode'):
            with self.subTest(fault=fault):
                scene=fleet_scene(entries,16)
                if fault=='peer':scene.pop('node4')
                elif fault=='slot':scene['node1']['formats'][1]['value']['xl']['this']='0'
                elif fault=='unknown-key':scene['node1']['files'][0]['path']=fixtures.BUCKET+'/foreign/xl.meta'
                elif fault=='decode':next(v for v in scene['node1']['files'] if 'metadata' in v).pop('metadata')
                elif fault=='geometry':next(v for v in scene['node1']['files'] if 'metadata' in v)['metadata']['versions'][0]['erasure']['data_blocks']=8
                with self.assertRaises(ValueError):
                    normalized,metadata=adapter.view(scene,fixtures.BUCKET,[fixtures.KEY],fixtures.INSPECTOR_SHA)
                    oracle.key_state(normalized,metadata,fixtures.BUCKET,fixtures.KEY,entries,
                        inspector_sha='b'*64 if fault=='inspector' else fixtures.INSPECTOR_SHA,disk_count=16)

    def test_complete_g04_fault_plan_contains_257_metadata_members_on_original_fourth_drive(self):
        keys={f'objects-{i:04d}.bin' for i in range(257)}
        entries=[fixtures.entry(i+1,key=key,latest=True) for i,key in enumerate(sorted(keys))]
        for width in (4,16):
            scene=fleet_scene(entries,width)
            ledger=SimpleNamespace(bucket=fixtures.BUCKET,bound=True,poisoned=None,keys=keys,entries=lambda:entries)
            targets=adapter.metadata_targets(scene,fixtures.BUCKET,ledger,'create-marker',fixtures.INSPECTOR_SHA)
            self.assertEqual(len(targets),257)
            self.assertEqual({(v['node'],v['disk']) for v in targets},{('node1',3)})
            self.assertTrue(all(v['relativePath'].endswith('/xl.meta') for v in targets))
            self.assertEqual(targets[-1]['key'],model.TARGET)
            wrong=copy.deepcopy(scene);wrong['node1']['files'].pop()
            with self.assertRaises((ValueError,KeyError)):
                adapter.metadata_targets(wrong,fixtures.BUCKET,ledger,'create-marker',fixtures.INSPECTOR_SHA)

    def test_metadata_reconstruction_changes_only_one_metadata_file(self):
        entries=[fixtures.entry(1,latest=True)];before=fleet_scene(entries)
        row=next(v for v in before['node1']['files'] if v['disk']==3 and 'metadata' in v)
        target={'node':'node1','disk':3,'relativePath':row['path'],'key':fixtures.KEY}
        after=copy.deepcopy(before);selected=next(v for v in after['node1']['files'] if v['disk']==3 and 'metadata' in v)
        selected['sha256']='f'*64
        self.assertTrue(adapter.reconstruction(before,after,fixtures.BUCKET,target)['allDataShardsUnchanged'])
        for fault in ('data','other-meta','missing','format'):
            wrong=copy.deepcopy(after)
            if fault=='format':wrong['node1']['formats'][0]['sha256']='changed'
            elif fault=='missing':wrong['node1']['files'].remove(next(v for v in wrong['node1']['files'] if v['disk']==3 and 'metadata' in v))
            else:next(v for v in wrong['node1']['files'] if ('metadata' not in v if fault=='data' else v['disk']==0 and 'metadata' in v))['sha256']='e'*64
            with self.subTest(fault=fault),self.assertRaises(ValueError):adapter.reconstruction(before,wrong,fixtures.BUCKET,target)

    def test_source_partial_window_requires_repaired_control_32_pending_and_missing_selected_metadata(self):
        keys=sorted({f'objects-{i:04d}.bin' for i in range(257)})
        entries=[fixtures.entry(i+1,key=key,latest=True) for i,key in enumerate(keys)]
        before=fleet_scene(entries);snapshot=copy.deepcopy(before)
        targets=[{'node':'node1','disk':3,'key':key,'relativePath':f'{fixtures.BUCKET}/{key}/xl.meta'} for key in keys]
        snapshot['node1']['files']=[row for row in snapshot['node1']['files'] if not
            (row['disk']==3 and row['path'].endswith('/xl.meta') and row['path']!=targets[0]['relativePath'])]
        fleet=SimpleNamespace(record=lambda label,value:value,snapshot=lambda label:snapshot)
        status={'httpStatus':200,'response':{'summary':'running','settings':race.MARKER_REFERENCE['effective'],
                                            'outcome':{'counters':{'processed':1}}}}
        with patch.object(runner.race,'observe',return_value=status):
            self.assertEqual(runner.partial(None,fleet,'path','token',race.MARKER_REFERENCE,before,targets,fixtures.BUCKET),snapshot)
            fleet.snapshot=lambda label:before
            with self.assertRaises(race.WindowNotObserved):runner.partial(None,fleet,'path','token',race.MARKER_REFERENCE,before,targets,fixtures.BUCKET)

    def test_h2_deleted_generation_is_not_live_but_its_unreferenced_bytes_are_reported(self):
        before_entries=[fixtures.entry(1),fixtures.entry(2,latest=True)]
        after_entries=[fixtures.entry(1,latest=True)]
        before=fleet_scene(before_entries,16);after=fleet_scene(after_entries,16)
        # Retained bytes stay original even when metadata latest is promoted.
        for node,value in after.items():
            for row in value['files']:
                if row['path'].endswith('/part.1'):
                    row['sha256']=next(v['sha256'] for v in before[node]['files'] if v['disk']==row['disk'] and v['path']==row['path'])
            value['files'] += [copy.deepcopy(v) for v in before[node]['files'] if fixtures.uid(1002) in v['path']]
        proof=adapter.transition(before,after,fixtures.BUCKET,before_entries,after_entries,fixtures.INSPECTOR_SHA)
        key=proof['keys'][fixtures.KEY]
        self.assertEqual(key['deletedVersions'],[fixtures.uid(2)])
        self.assertEqual(len(key['unreferencedDeletedGenerationFiles']),16)
        self.assertFalse(key['garbageCollectorResponsibilityProved'])
        bad=copy.deepcopy(after)
        next(v for v in bad['node4']['files'] if 'metadata' in v)['metadata']['versions'].append(
            copy.deepcopy(next(v for v in before['node4']['files'] if 'metadata' in v)['metadata']['versions'][-1]))
        with self.assertRaises(ValueError):adapter.transition(before,bad,fixtures.BUCKET,before_entries,after_entries,fixtures.INSPECTOR_SHA)

    def test_negative_diagnostics_drain_read_failures_but_stop_on_evidence_io(self):
        expected=[{'params':{'Bucket':fixtures.BUCKET,'Key':fixtures.KEY,'VersionId':fixtures.uid(i)},
                   'httpStatus':404,'errorCode':'NoSuchVersion'} for i in (1,2)]
        with tempfile.TemporaryDirectory() as tmp:
            recorder=SimpleNamespace(output=Path(tmp),call=Mock(side_effect=[ValueError('wrong original 403'),
                {'ResponseMetadata':{'HTTPStatusCode':404},'Error':{'Code':'NoSuchVersion'}}]))
            proof=runner.negative_checks(recorder,expected)
            self.assertEqual([v['state'] for v in proof['results']],['FAIL','PASS'])
            recorder.call.reset_mock()
            recorder.call.side_effect=ValueError('wrong original 403')
            with patch.object(runner,'save',side_effect=OSError('fsync failed')):
                with self.assertRaises(OSError):runner.negative_checks(recorder,expected)
            self.assertEqual(recorder.call.call_count,1)


if __name__=='__main__':unittest.main()
