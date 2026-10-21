import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from scripts.validation_heal import version_race_fixture as f
from scripts.validation_heal.heal_workload import Recorder,RecordedCallError
from test_validation_heal_status_keepalive import binding as transport_binding

BUCKET='rv-heal-test-001'


def binding():
    return {**transport_binding(), 'bucketPrefix': BUCKET}


def ack(code=200,**values):return {'ResponseMetadata':{'HTTPStatusCode':code},**values}


class SeedRecorder:
    def __init__(self,versioned=True):
        self.probe=SimpleNamespace(binding={'bucketPrefix':BUCKET});self.calls=[];self.index=0;self.versioned=versioned
    def call(self,operation,params,body=None,**kwargs):
        self.calls.append((operation,copy.deepcopy(params)));self.index+=1
        if operation=='put_object':return ack(VersionId=f'v-{self.index}' if self.versioned else 'null',ETag='"'+hashlib.md5(body).hexdigest()+'"')
        if operation=='delete_object':return ack(204,VersionId=f'm-{self.index}',DeleteMarker=True)
        return ack()


def fixture(branch='new-version-vs-historical-heal'):
    recorder=SeedRecorder(branch!='unversioned-delete');ledger=f.seed_fixture(recorder,BUCKET,branch,controls=3,size=17)
    inventory={'bucket':BUCKET,'entries':ledger.entries()}
    for row in inventory['entries']:
        if not row['deleteMarker']:row['properties']['StorageClass']='STANDARD'
    ledger.bind_baseline(inventory)
    return recorder,ledger


class VersionFixtureTests(unittest.TestCase):
    def test_full_g04_population_uses_real_durable_recorder_and_four_bound_batch_receipts(self):
        for branch in f.BRANCHES[4:]:
            with self.subTest(branch=branch),tempfile.TemporaryDirectory() as tmp:
                sdk=SeedRecorder(branch!='unversioned-delete')
                def invoke(operation,params,body=None):
                    if operation=='delete_objects':response=ack(Deleted=copy.deepcopy(params['Delete']['Objects']))
                    elif operation=='delete_object' and ('VersionId' in params or branch=='unversioned-delete'):
                        response=ack(204,**({'VersionId':params['VersionId'],'DeleteMarker':True} if 'VersionId' in params else {}))
                    else:response=sdk.call(operation,params,body)
                    return {'acknowledged':True,'resultUnknown':False,'operation':operation,
                            'params':copy.deepcopy(params),'response':response}
                recorder=Recorder(SimpleNamespace(binding=binding(),s3=invoke),Path(tmp)/'journal')
                ledger=f.seed_fixture(recorder,BUCKET,branch,size=17)
                self.assertEqual(ledger.keys,{f'objects-{i:04d}.bin' for i in range(257)})
                self.assertEqual(len(ledger.records),257 if branch=='unversioned-delete' else 260 if branch in ('delete-marker','batch-version-delete') else 259)
                inventory={'bucket':BUCKET,'entries':ledger.entries()};ledger.bind_baseline(inventory)
                order=ledger.history[f.TARGET][:];plan=f.mutation_plan(ledger,branch,size=17)
                f.submit(recorder,ledger,plan[0])
                ledger.verify_inventory({'bucket':BUCKET,'entries':ledger.entries()})
                if branch=='batch-version-delete':
                    self.assertEqual(ledger.history[f.TARGET],[order[1]])
                    self.assertEqual(ledger.history['objects-0002.bin'],[])
                    self.assertEqual(len(ledger.negative_reads()),4)
                    receipt=json.loads((recorder.output/f'{recorder.sequence:06d}-ack.json').read_text())
                    self.assertEqual([v['state'] for v in receipt['batchDelete']['items']],['ACK']*4)
                self.assertEqual(len(list(recorder.output.glob('*-request.json'))),recorder.sequence)
                self.assertEqual(len(list(recorder.output.glob('*-ack.json'))),recorder.sequence)

    def test_all_eight_branches_have_exact_bounded_fixtures_and_plans(self):
        for branch in f.BRANCHES:
            recorder,ledger=fixture(branch);plan=f.mutation_plan(ledger,branch,size=17)
            self.assertTrue(plan);self.assertFalse(ledger.poisoned)
            puts=[v for v in recorder.calls if v[0]=='put_object']
            self.assertEqual(len(puts),4 if branch=='unversioned-delete' else 6)
            self.assertEqual(sum(v[0]=='put_bucket_versioning' for v in recorder.calls),branch!='unversioned-delete')
            self.assertTrue(all(v['params']['Bucket']==BUCKET for v in plan))

    def test_controls_reference_new_payloads_and_real_version_ack_ids(self):
        _,ledger=fixture();plan=f.mutation_plan(ledger,f.BRANCHES[0],size=17)
        self.assertEqual(len(plan),3);self.assertEqual([v['referenceKey'] for v in plan],[f'objects-{i:04d}.bin' for i in range(3)])
        before=ledger.entries()
        for i,step in enumerate(plan):
            body=f.payload(step['nonce'],step['size']);ledger.apply(step['operation'],step['params'],ack(VersionId=f'new-{i}',ETag=f'etag-{i}'),body)
        after=ledger.entries();self.assertEqual(len(after),len(before)+3)
        target=[v for v in after if v['key']==f.TARGET]
        self.assertEqual([v['versionId'] for v in target if v['isLatest']],['new-2'])
        self.assertTrue(all(v['properties']['StorageClass']=='STANDARD' for v in target))
        self.assertTrue(all('VersionId' in v['params'] for v in ledger.exact_reads()))
        with self.assertRaises(ValueError):f.mutation_plan(ledger,f.BRANCHES[0],size=17)

    def test_reference_size_or_unbound_fixture_cannot_produce_race_plan(self):
        _,ledger=fixture()
        with self.assertRaisesRegex(ValueError,'reference payload'):f.mutation_plan(ledger,f.BRANCHES[0],size=19)
        ledger.bound=False
        with self.assertRaisesRegex(ValueError,'bound healthy'):f.mutation_plan(ledger,f.BRANCHES[0],size=17)

    def test_latest_is_serial_ack_order_not_lexical_id_or_timestamp(self):
        _,ledger=fixture();plan=f.mutation_plan(ledger,f.BRANCHES[0],size=17)
        for vid,step in zip(('zzz','aaa','bbb'),plan):
            ledger.apply('put_object',step['params'],ack(VersionId=vid,ETag='e'),f.payload(step['nonce'],17))
        self.assertEqual([r['versionId'] for r in ledger.entries() if r['key']==f.TARGET and r['isLatest']],['bbb'])

    def test_delete_current_exposes_previous_version_without_resurrection(self):
        _,ledger=fixture('delete-current-version-vs-deep');original=ledger.history[f.TARGET][:]
        step=f.mutation_plan(ledger,'delete-current-version-vs-deep')[0]
        self.assertEqual(step['params']['VersionId'],original[-1])
        ledger.apply(step['operation'],step['params'],ack(204,VersionId=original[-1]))
        self.assertEqual([r['versionId'] for r in ledger.entries() if r['key']==f.TARGET and r['isLatest']],[original[-2]])
        self.assertEqual(ledger.negative_reads(),[dict(params={'Bucket':BUCKET,'Key':f.TARGET,'VersionId':original[-1]},httpStatus=404,errorCode='NoSuchVersion')])

    def test_delete_old_does_not_create_marker_or_change_latest(self):
        _,ledger=fixture('delete-old-version-vs-deep');original=ledger.history[f.TARGET][:]
        step=f.mutation_plan(ledger,'delete-old-version-vs-deep')[0]
        self.assertEqual(step['params']['VersionId'],original[0])
        ledger.apply(step['operation'],step['params'],ack(204))
        rows=[r for r in ledger.entries() if r['key']==f.TARGET]
        self.assertTrue(all(not v['deleteMarker'] for v in rows));self.assertEqual([r['versionId'] for r in rows if r['isLatest']],[original[-1]])

    def test_marker_create_and_exact_marker_delete_have_different_expectations(self):
        _,ledger=fixture('create-marker');step=f.mutation_plan(ledger,'create-marker')[0]
        ledger.apply(step['operation'],step['params'],ack(204,VersionId='marker',DeleteMarker=True))
        latest=[r for r in ledger.entries() if r['key']==f.TARGET and r['isLatest']]
        self.assertEqual(latest[0]['versionId'],'marker');self.assertTrue(latest[0]['deleteMarker'])
        negatives=ledger.negative_reads();self.assertEqual([r['httpStatus'] for r in negatives],[405,404])
        _,other=fixture('delete-marker');old=other.history[f.TARGET][:]
        step=f.mutation_plan(other,'delete-marker')[0]
        self.assertEqual(step['params']['VersionId'],old[-1])
        other.apply(step['operation'],step['params'],ack(204,VersionId=old[-1],DeleteMarker=True))
        self.assertEqual([r['versionId'] for r in other.entries() if r['key']==f.TARGET and r['isLatest']],[old[-2]])
        self.assertFalse(any(r['deleteMarker'] for r in other.entries()))

    def test_never_versioned_delete_is_not_marker_creation(self):
        _,ledger=fixture('unversioned-delete');step=f.mutation_plan(ledger,'unversioned-delete')[0]
        self.assertNotIn('VersionId',step['params']);ledger.apply(step['operation'],step['params'],ack(204))
        self.assertFalse(any(v['key']==f.TARGET for v in ledger.entries()))
        self.assertEqual(ledger.negative_reads()[0]['errorCode'],'NoSuchKey')
        _,ledger=fixture('unversioned-delete')
        with self.assertRaises(ValueError):ledger.apply('delete_object',step['params'],ack(204,DeleteMarker=True,VersionId='marker'))

    def test_successful_batch_removes_only_frozen_key_version_pairs(self):
        _,ledger=fixture('batch-version-delete');step=f.mutation_plan(ledger,'batch-version-delete')[0]
        pairs=step['params']['Delete']['Objects'];self.assertEqual(len(pairs),4);self.assertIs(step['params']['Delete']['Quiet'],False)
        middle=ledger.history[f.TARGET][1]
        ledger.apply('delete_objects',step['params'],ack(Deleted=pairs))
        self.assertEqual(ledger.history[f.TARGET],[middle]);self.assertEqual(ledger.history['objects-0002.bin'],[])
        self.assertEqual(len(ledger.deleted),4);self.assertEqual(len(ledger.records),3)

    def test_partial_batch_retains_known_effects_and_never_accepts_final_inventory(self):
        _,ledger=fixture('batch-version-delete');step=f.mutation_plan(ledger,'batch-version-delete')[0];items=step['params']['Delete']['Objects']
        response=ack(Deleted=[items[0]],Errors=[{**v,'Code':'InternalError'} for v in items[1:]])
        with self.assertRaisesRegex(ValueError,'rejected/unknown'):ledger.apply('delete_objects',step['params'],response)
        self.assertNotIn((items[0]['Key'],items[0]['VersionId']),ledger.records)
        self.assertTrue(all((v['Key'],v['VersionId']) in ledger.records for v in items[1:]))
        with self.assertRaisesRegex(ValueError,'unresolved'):ledger.verify_inventory({'bucket':BUCKET,'entries':ledger.entries()})

    def test_duplicate_batch_receipt_is_unknown_not_two_deletes(self):
        _,ledger=fixture('batch-version-delete');step=f.mutation_plan(ledger,'batch-version-delete')[0];items=step['params']['Delete']['Objects']
        with self.assertRaises(ValueError):ledger.apply('delete_objects',step['params'],ack(Deleted=[items[0],items[0],*items[1:]]))
        self.assertIn((items[0]['Key'],items[0]['VersionId']),ledger.records)
        self.assertEqual(len(ledger.deleted),3)

    def test_real_recorder_partial_exception_keeps_successful_batch_members(self):
        _,ledger=fixture('batch-version-delete');step=f.mutation_plan(ledger,'batch-version-delete')[0];items=step['params']['Delete']['Objects']
        observed={'acknowledged':True,'resultUnknown':False,'response':ack(Deleted=[items[0]],Errors=[{**v,'Code':'AccessDenied'} for v in items[1:]])}
        with tempfile.TemporaryDirectory() as tmp:
            probe=SimpleNamespace(binding=binding(),s3=Mock(return_value=observed));recorder=Recorder(probe,Path(tmp)/'ledger')
            with self.assertRaises(RecordedCallError) as caught:f.submit(recorder,ledger,step)
            self.assertEqual(caught.exception.observation,observed)
            self.assertNotIn((items[0]['Key'],items[0]['VersionId']),ledger.records)
            self.assertEqual(len(ledger.deleted),1);self.assertTrue(ledger.poisoned)
            probe.s3.assert_called_once()

    def test_ack_with_client_cleanup_error_is_retained_but_not_accepted(self):
        _,ledger=fixture('delete-current-version-vs-deep');step=f.mutation_plan(ledger,'delete-current-version-vs-deep')[0]
        observed={'acknowledged':True,'resultUnknown':False,'response':ack(204,VersionId=step['params']['VersionId']),
                  'clientCleanupError':{'message':'close failed'}}
        with tempfile.TemporaryDirectory() as tmp:
            recorder=Recorder(SimpleNamespace(binding=binding(),s3=Mock(return_value=observed)),Path(tmp)/'ledger')
            with self.assertRaises(RecordedCallError):f.submit(recorder,ledger,step)
            self.assertNotIn((f.TARGET,step['params']['VersionId']),ledger.records)
            self.assertEqual(ledger.events[-1]['state'],'COLLECTOR_ERROR')

    def test_missing_or_reused_version_ack_cannot_hide_old_or_deleted_version(self):
        for vid in (None,'null',''):
            _,ledger=fixture();step=f.mutation_plan(ledger,f.BRANCHES[0],size=17)[0];before=len(ledger.records)
            with self.assertRaises(ValueError):ledger.apply('put_object',step['params'],ack(VersionId=vid,ETag='e'),b'new')
            self.assertEqual(len(ledger.records),before)
        _,ledger=fixture('delete-current-version-vs-deep');step=f.mutation_plan(ledger,'delete-current-version-vs-deep')[0];vid=step['params']['VersionId']
        ledger.apply('delete_object',step['params'],ack(204))
        params=f.put_step(BUCKET,f.TARGET,'new',3)['params']
        with self.assertRaisesRegex(ValueError,'reused'):ledger.apply('put_object',params,ack(VersionId=vid,ETag='e'),b'new')

    def test_inventory_rejects_missing_historical_bytes_wrong_latest_and_metadata(self):
        _,ledger=fixture();good={'bucket':BUCKET,'entries':ledger.entries()};ledger.verify_inventory(good)
        for kind in ('missing','duplicate','latest','payload','metadata','tags','foreign'):
            bad=copy.deepcopy(good)
            if kind=='missing':bad['entries'].pop()
            if kind=='duplicate':bad['entries'].append(bad['entries'][0])
            if kind=='latest':bad['entries'][0]['isLatest']=not bad['entries'][0]['isLatest']
            if kind=='payload':bad['entries'][0]['payload']['sha256']='0'*64
            if kind=='metadata':bad['entries'][0]['properties']['Metadata']={}
            if kind=='tags':bad['entries'][0]['tags']=[]
            if kind=='foreign':bad['bucket']='other'
            with self.subTest(kind=kind),self.assertRaises(ValueError):ledger.verify_inventory(bad)
        with self.assertRaisesRegex(ValueError,'rebound'):ledger.bind_baseline(good)

    def test_marker_rejection_cannot_be_replaced_by_access_denied_or_other_id(self):
        expected=dict(httpStatus=405,errorCode='MethodNotAllowed',deleteMarker=True,versionId='marker')
        response=ack(405,Error={'Code':'MethodNotAllowed'});response['ResponseMetadata']['HTTPHeaders']={'X-Amz-Delete-Marker':'true','X-Amz-Version-Id':'marker'}
        f.check_negative(response,expected)
        for field,value in (('X-Amz-Version-Id','other'),('X-Amz-Delete-Marker','false')):
            bad=copy.deepcopy(response);bad['ResponseMetadata']['HTTPHeaders'][field]=value
            with self.assertRaises(ValueError):f.check_negative(bad,expected)
        with self.assertRaises(ValueError):f.check_negative(ack(403,Error={'Code':'AccessDenied'}),expected)

    def test_unknown_never_disappears_after_successful_list(self):
        _,ledger=fixture();step=f.mutation_plan(ledger,f.BRANCHES[0],size=17)[0]
        recorder=Mock();recorder.call.side_effect=TimeoutError('unknown')
        with self.assertRaises(TimeoutError):f.submit(recorder,ledger,step)
        self.assertEqual(ledger.events[-1]['state'],'UNKNOWN')
        with self.assertRaises(ValueError):ledger.verify_inventory({'bucket':BUCKET,'entries':ledger.entries()})
        with self.assertRaises(ValueError):ledger.apply('put_object',step['params'],ack(VersionId='new',ETag='e'),b'new')

    def test_mismatched_failed_receipt_poison_and_http_rejection_are_distinct(self):
        for variant in ('foreign','rejected','missing-http'):
            _,ledger=fixture();step=f.mutation_plan(ledger,f.BRANCHES[0],size=17)[0]
            params=copy.deepcopy(step['params'])
            if variant=='foreign':params['Key']='foreign'
            observed={'acknowledged':False,'resultUnknown':False,
                      'response':ack(403) if variant=='rejected' else None}
            recorder=Mock();recorder.call.side_effect=RecordedCallError('failed','put_object',params,observed,'a'*64)
            with self.assertRaises(ValueError):f.submit(recorder,ledger,step)
            self.assertEqual(ledger.events[-1]['state'],'REJECTED' if variant=='rejected' else 'UNKNOWN')
            with self.assertRaises(ValueError):ledger.verify_inventory({'bucket':BUCKET,'entries':ledger.entries()})

    def test_seed_scope_and_budget_fail_before_any_call(self):
        recorder=SeedRecorder()
        for kwargs in ({'controls':2},{'size':64*1024**2},{'controls':True}):
            with self.assertRaises(ValueError):f.seed_fixture(recorder,BUCKET,f.BRANCHES[0],**kwargs)
        with self.assertRaises(ValueError):f.seed_fixture(recorder,'foreign-bucket',f.BRANCHES[0])
        self.assertFalse(recorder.calls)


if __name__=='__main__':unittest.main()
