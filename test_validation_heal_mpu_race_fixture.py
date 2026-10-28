import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch
import uuid

from scripts.validation_heal import mpu_race_fixture as m
from scripts.validation_heal.heal_workload import Recorder as DurableRecorder


class Recorder:
    def __init__(self,folder):
        self.output=Path(folder);self.sequence=0;self.uploads={};self.versions=[];self.calls=[];self.fail=None;self.corrupt=None

    def call(self,operation,params,body=None,**kwargs):
        self.calls.append((operation,copy.deepcopy(params),kwargs));self.sequence+=1
        if self.fail==operation:raise ValueError('uncertain call; durable production Recorder owns raw receipt')
        response={'ResponseMetadata':{'HTTPStatusCode':204 if operation=='abort_multipart_upload' else 200}}
        if operation=='create_multipart_upload':
            uid=f'upload-{len(self.uploads)+1}';self.uploads[uid]={'params':copy.deepcopy(params),'parts':{},'state':'pending'}
            response['UploadId']=uid
        elif operation=='upload_part':
            self.uploads[params['UploadId']]['parts'][params['PartNumber']]=body;response['ETag']=m.etag(body)
        elif operation=='complete_multipart_upload':
            item=self.uploads[params['UploadId']];parts=[{'number':n,'etag':m.etag(b)} for n,b in sorted(item['parts'].items())]
            item['state']='complete';raw=b''.join(v for _,v in sorted(item['parts'].items()))
            version={'VersionId':str(uuid.UUID(int=len(self.versions)+1)),'ETag':m.multipart_etag(parts),
                     'Metadata':item['params']['Metadata'],'ContentType':'application/octet-stream',
                     'ContentLength':len(raw),'body':raw}
            self.versions.append(version);response.update({k:version[k] for k in ('VersionId','ETag')})
        elif operation=='abort_multipart_upload':self.uploads[params['UploadId']]['state']='aborted'
        elif operation=='list_multipart_uploads':
            response.update(IsTruncated=False,Uploads=[{'Key':m.KEY,'UploadId':u} for u,v in self.uploads.items() if v['state']=='pending'])
        elif operation=='list_parts':
            item=self.uploads[params['UploadId']]
            if item['state']=='pending':response.update(IsTruncated=False,Parts=[{'PartNumber':n,'ETag':m.etag(b),'Size':len(b)} for n,b in sorted(item['parts'].items())])
            else:response={'ResponseMetadata':{'HTTPStatusCode':404},'Error':{'Code':'NoSuchUpload'}}
        elif operation=='list_object_versions':
            response.update(IsTruncated=False,Versions=[{'Key':m.KEY,'VersionId':v['VersionId'],'ETag':v['ETag'],
                'Size':v['ContentLength'],'IsLatest':v is self.versions[-1]} for v in self.versions])
        elif operation in ('head_object','get_object'):
            version=next(v for v in self.versions if v['VersionId']==params['VersionId']) if 'VersionId' in params else self.versions[-1]
            response.update({k:v for k,v in version.items() if k!='body'})
        else:raise AssertionError(operation)
        if self.corrupt:response=self.corrupt(operation,response)
        observation={'params':copy.deepcopy(params),'response':copy.deepcopy(response)}
        if operation=='get_object':observation['payload']={'size':len(version['body']),'sha256':hashlib.sha256(version['body']).hexdigest()}
        m.save(self.output/f'{self.sequence:06d}-ack.json',{'observation':observation})
        return response


class MpuRaceFixtureTests(unittest.TestCase):
    def setUp(self):
        patcher=patch.object(m,'PART_SIZES',(12,13,14));patcher.start();self.addCleanup(patcher.stop)
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.rec=Recorder(tmp.name);self.fixture=m.MpuRaceFixture(self.rec,'rv-g05-owned')

    def test_seed_same_key_original_and_two_distinct_pending_uploads(self):
        value=self.fixture.seed();items=value['items']
        self.assertEqual([items[r]['state'] for r in m.ROLES],['complete','pending','pending'])
        self.assertEqual([len(items[r]['parts']) for r in m.ROLES],[3,2,3])
        self.assertEqual(len({v['uploadId'] for v in items.values()}),3)
        self.assertEqual({v[1]['Key'] for v in self.rec.calls},{m.KEY})
        self.assertEqual(len({items[r]['parts'][0]['etag'] for r in m.ROLES}),3)
        self.assertFalse(value['casePassClaimed']);self.assertFalse(value['physicalRepairProved'])
        self.assertEqual(self.fixture.verify_s3(self.rec)['pendingUploads'],2)

    def test_continue_complete_abort_preserve_sibling_and_old_committed_version_both_orders(self):
        for abort_first in (False,True):
            with self.subTest(abort_first=abort_first),tempfile.TemporaryDirectory() as tmp:
                rec=Recorder(tmp);fixture=m.MpuRaceFixture(rec,'rv-g05-owned');fixture.seed()
                if abort_first:fixture.abort_b();fixture.verify_s3(rec)
                fixture.upload('a',3);fixture.verify_s3(rec)
                fixture.complete('a');fixture.verify_s3(rec)
                if not abort_first:fixture.abort_b()
                proof=fixture.verify_s3(rec)
                self.assertEqual(proof['committedVersions'],2);self.assertEqual(proof['pendingUploads'],0)
                self.assertTrue(proof['sameKeySiblingIsolationVerified']);self.assertFalse(proof['overlapProved'])
                abort=next(c for c in rec.calls if c[0]=='abort_multipart_upload')
                self.assertEqual(abort[1]['UploadId'],fixture.items['b']['uploadId']);self.assertEqual(abort[2]['expect_http'],204)

    def test_unknown_mutation_halts_sibling_and_never_retries(self):
        self.fixture.seed();self.rec.fail='upload_part'
        with self.assertRaisesRegex(ValueError,'uncertain'):self.fixture.upload('a',3)
        calls=len(self.rec.calls)
        with self.assertRaisesRegex(ValueError,'halted'):self.fixture.abort_b()
        self.assertEqual(len(self.rec.calls),calls);self.assertIsNotNone(self.fixture.poisoned)
        self.assertEqual(self.fixture.items['a']['state'],'pending')
        self.assertEqual(self.fixture.events[-1]['state'],'UNRESOLVED')
        with self.assertRaises(ValueError):self.fixture.verify_s3(self.rec)

    def test_malformed_successful_ack_keeps_durable_failure_and_prevents_retry(self):
        self.fixture.seed();self.fixture.upload('a',3)
        self.rec.corrupt=lambda op,r:{**r,'ETag':'"wrong"'} if op=='complete_multipart_upload' else r
        with self.assertRaisesRegex(ValueError,'multipart ETag'):self.fixture.complete('a')
        self.assertTrue(self.fixture.events[-1]['responseReceived'])
        self.assertEqual(self.fixture.items['a']['state'],'pending')
        with self.assertRaisesRegex(ValueError,'halted'):self.fixture.abort_b()

    def test_out_of_order_duplicate_or_unprepared_calls_never_go_to_network(self):
        with self.assertRaises(ValueError):self.fixture.create('b')
        self.assertFalse(self.rec.calls);self.fixture.seed();count=len(self.rec.calls)
        for method in (lambda:self.fixture.seed(),lambda:self.fixture.create('a'),lambda:self.fixture.upload('a',2),
                       lambda:self.fixture.upload('a',True),lambda:self.fixture.complete('a'),lambda:self.fixture.complete('b')):
            with self.assertRaises(ValueError):method()
        self.assertEqual(len(self.rec.calls),count)

    def test_wrong_part_ack_or_reused_upload_identity_is_not_accepted(self):
        self.fixture.seed();self.rec.corrupt=lambda op,r:{**r,'ETag':'"'+'0'*32+'"'} if op=='upload_part' else r
        with self.assertRaisesRegex(ValueError,'exact submitted'):self.fixture.upload('a',3)
        self.assertEqual(len(self.fixture.items['a']['parts']),2)
        with tempfile.TemporaryDirectory() as tmp:
            rec=Recorder(tmp);fixture=m.MpuRaceFixture(rec,'rv-g05-owned')
            rec.corrupt=lambda op,r:{**r,'UploadId':'same'} if op=='create_multipart_upload' else r
            fixture.create('committed')
            fixture.items['committed']['state']='complete'
            with self.assertRaisesRegex(ValueError,'reused UploadId'):fixture.create('a')

    def test_s3_oracle_rejects_sibling_loss_part_corruption_and_version_resurrection(self):
        self.fixture.seed()
        for kind in ('upload','part','version','marker','head','get','terminal-error'):
            def corrupt(op,r):
                r=copy.deepcopy(r)
                if kind=='upload' and op=='list_multipart_uploads':r['Uploads'].pop()
                if kind=='part' and op=='list_parts' and r.get('Parts'):r['Parts'][0]['ETag']='wrong'
                if kind=='version' and op=='list_object_versions':r['Versions'].append(copy.deepcopy(r['Versions'][0]))
                if kind=='marker' and op=='list_object_versions':r['DeleteMarkers']=[{'Key':m.KEY}]
                if kind=='head' and op=='head_object':r['Metadata']={}
                if kind=='get' and op=='get_object':r['VersionId']=str(uuid.UUID(int=999))
                if kind=='terminal-error' and op=='list_parts' and 'Error' in r:r['Error']['Code']='NoSuchKey'
                return r
            self.rec.corrupt=corrupt
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.fixture.verify_s3(self.rec)

    def test_manifest_persistence_failure_stops_later_mutation(self):
        self.fixture.seed()
        with patch.object(self.fixture,'_persist',side_effect=OSError('full')):
            with self.assertRaises(OSError):self.fixture.upload('a',3)
        self.assertIsNotNone(self.fixture.poisoned)
        count=len(self.rec.calls)
        with self.assertRaises(ValueError):self.fixture.abort_b()
        self.assertEqual(count,len(self.rec.calls))

    def test_multipart_digest_requires_all_ordered_plain_parts(self):
        parts=[{'number':n,'etag':m.etag(m.payload('rv-owned','a',n))} for n in (1,2,3)]
        self.assertTrue(m.multipart_etag(parts).endswith('-3"'))
        for bad in (parts[:2],parts[::-1],[{**p,'etag':'wrong'} for p in parts]):
            with self.assertRaises(ValueError):m.multipart_etag(bad)

    def test_pagination_rejects_missing_typed_flags_missing_cursor_and_cycles(self):
        recorder=Mock();recorder.call.return_value={}
        with self.assertRaises(ValueError):m.pages(recorder,'list_parts',{})
        recorder.call.return_value={'IsTruncated':True,'NextPartNumberMarker':1}
        with self.assertRaisesRegex(ValueError,'advance'):m.pages(recorder,'list_parts',{})
        recorder.call.return_value={'IsTruncated':True,'NextKeyMarker':m.KEY,'NextUploadIdMarker':'u'}
        with self.assertRaisesRegex(ValueError,'repeated'):m.pages(recorder,'list_multipart_uploads',{})
        recorder.call.side_effect=[{'IsTruncated':True,'NextPartNumberMarker':1},{'IsTruncated':False}]
        self.assertEqual(len(m.pages(recorder,'list_parts',{})),2)


class MigratedRecorderTests(unittest.TestCase):
    def test_frozen_common_dm_sizes_are_retained(self):
        self.assertEqual(m.PART_SIZES, (5 * 1024**2 + 17, 5 * 1024**2 + 93, 1024**2 + 47))

    def test_model_uses_actual_auto_test_request_ack_ledger_and_halts_unknown_writes(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(m, 'PART_SIZES', (12, 13, 14)):
            root = Path(folder)
            backend_dir = root / 'backend'
            backend_dir.mkdir()
            backend = Recorder(backend_dir)
            class Probe:
                binding = {'runId': 'gh-12345-1-h1', 'bucketPrefix': 'rv-g05-owned',
                           'maxInventoryBytes': 64 * 1024**2}
                fail_upload = False
                def s3(self, operation, params, body=None):
                    if self.fail_upload and operation == 'upload_part':
                        return {'operation': operation, 'params': copy.deepcopy(params),
                                'acknowledged': False, 'resultUnknown': True,
                                'error': {'type': 'TimeoutError', 'message': 'unknown write'}}
                    backend.call(operation, params, body)
                    observation = json.loads((backend.output / f'{backend.sequence:06d}-ack.json').read_text())['observation']
                    status = observation['response']['ResponseMetadata']['HTTPStatusCode']
                    return {**observation, 'operation': operation, 'acknowledged': 200 <= status < 300,
                            'resultUnknown': False}
            probe = Probe()
            recorder = DurableRecorder(probe, root / 'durable')
            fixture = m.MpuRaceFixture(recorder, 'rv-g05-owned')
            fixture.seed()
            self.assertTrue(fixture.verify_s3(recorder)['sameKeySiblingIsolationVerified'])
            probe.fail_upload = True
            with self.assertRaisesRegex(ValueError, 'unexpected upload_part'):
                fixture.upload('a', 3)
            sequence = recorder.sequence
            with self.assertRaisesRegex(ValueError, 'halted'):
                fixture.abort_b()
            self.assertEqual(recorder.sequence, sequence)
            ack = json.loads((recorder.output / f'{sequence:06d}-ack.json').read_text())
            request = recorder.output / ack['requestFile']
            self.assertEqual(ack['requestSha256'], hashlib.sha256(request.read_bytes()).hexdigest())
            self.assertEqual(ack['identity'], {'mpuRole': 'a'})
            self.assertEqual(ack['operationCheck'], 'FAIL')
            self.assertTrue(ack['observation']['resultUnknown'])
            self.assertEqual(fixture.items['b']['state'], 'pending')
            self.assertEqual(fixture.items['committed']['state'], 'complete')
            self.assertEqual(fixture.events[-1]['state'], 'UNRESOLVED')


if __name__=='__main__':unittest.main()
