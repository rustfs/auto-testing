"""Actual Recorder/controller boundaries; synthetic fleet data is not live proof."""
import copy
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch
import uuid

from scripts.validation_heal import version_race_runner as runner,version_race_fixture as model,race
from scripts.validation_heal.heal_probe import CLOCK_DOMAIN
from scripts.validation_heal.heal_workload import Recorder


class SDKProbe:
    def __init__(self,binding,trace):
        self.binding=binding;self.trace=trace;self.rows={};self.sequence=0;self.versioned=False;self.fail=None

    def s3(self,operation,params,body=None):
        started=time.monotonic_ns();self.trace.append(('s3',operation,copy.deepcopy(params)))
        if operation==self.fail:raise OSError('unknown original request; no retry')
        response={'ResponseMetadata':{'HTTPStatusCode':200}};key=params.get('Key');self.sequence+=1
        if operation=='put_bucket_versioning':self.versioned=True
        elif operation=='put_object':
            vid=str(uuid.UUID(int=10000-self.sequence)) if self.versioned else 'null'
            etag='"'+hashlib.md5(body,usedforsecurity=False).hexdigest()+'"'
            row={'key':key,'versionId':vid,'deleteMarker':False,'payload':{'size':len(body),'sha256':hashlib.sha256(body).hexdigest()},
                 'properties':{'ETag':etag,'ContentLength':len(body),'ContentType':params['ContentType'],
                               'Metadata':copy.deepcopy(params['Metadata']),'StorageClass':'STANDARD'},
                 'tags':[{'Key':'fixture','Value':'DV' if self.versioned else 'DB'},{'Key':'suite','Value':'heal'}]}
            self.rows.setdefault(key,[]).append(row);response.update(VersionId=vid,ETag=etag)
        elif operation=='delete_object':
            response['ResponseMetadata']['HTTPStatusCode']=204
            if 'VersionId' in params:
                original=next(v for v in self.rows[key] if v['versionId']==params['VersionId'])
                self.rows[key].remove(original);response.update(VersionId=original['versionId'],DeleteMarker=original['deleteMarker'])
            elif self.versioned:
                vid=str(uuid.UUID(int=10000-self.sequence));self.rows[key].append({'key':key,'versionId':vid,'deleteMarker':True})
                response.update(VersionId=vid,DeleteMarker=True)
            else:self.rows[key]=[]
        elif operation=='delete_objects':
            response['Deleted']=[]
            for item in params['Delete']['Objects']:
                original=next(v for v in self.rows[item['Key']] if v['versionId']==item['VersionId'])
                self.rows[item['Key']].remove(original)
                response['Deleted'].append({**item,'DeleteMarker':original['deleteMarker']})
        elif operation=='get_object':
            wanted=next((v for v in self.rows.get(key,[]) if v['versionId']==params['VersionId']),None) if 'VersionId' in params else (self.rows.get(key) or [None])[-1]
            marker=bool(wanted and wanted['deleteMarker']);code=405 if marker and 'VersionId' in params else 404
            response={'ResponseMetadata':{'HTTPStatusCode':code,'HTTPHeaders':{}},
                      'Error':{'Code':'MethodNotAllowed' if code==405 else 'NoSuchVersion' if 'VersionId' in params else 'NoSuchKey'}}
            if marker:response['ResponseMetadata']['HTTPHeaders'].update({'x-amz-delete-marker':'true','x-amz-version-id':wanted['versionId']})
        elif operation!='create_bucket':raise AssertionError(operation)
        return {'operation':operation,'params':copy.deepcopy(params),'response':response,
                'acknowledged':200<=response['ResponseMetadata']['HTTPStatusCode']<300,'resultUnknown':False,
                'clockDomain':CLOCK_DOMAIN,'clientCallInvoked':True,'callStartedMonotonicNs':started,
                'callFinishedMonotonicNs':time.monotonic_ns()}

    def inventory(self,bucket,**unused):
        self.trace.append(('inventory',bucket));rows=[]
        for values in self.rows.values():
            for i,row in enumerate(values):rows.append({**copy.deepcopy(row),'isLatest':i==len(values)-1})
        return {'bucket':bucket,'entries':sorted(rows,key=lambda v:(v['key'],v['versionId']))}

    def admin(self,path,query,body):
        return {'httpStatus':200,'response':{'clientToken':str(uuid.UUID(int=1))}}


class FakeFleet:
    def __init__(self,nodes,directory,run_id,*unused,**kwargs):
        self.nodes=nodes;self.evidence=directory;self.run_id=run_id;self.faults=[];self.records=[];self.cleaned=False
    def record(self,label,value):self.records.append((label,value));return value
    def deploy(self,*unused):pass
    def stop(self):self.records.append(('stop',{}))
    def start(self):self.records.append(('start',{}))
    def snapshot(self,label,**kwargs):
        self.records.append((label,{}));return {node:{'formats':[],'files':[]} for node in self.nodes}
    def remote(self,node,action,**params):
        self.records.append(('remote',{'node':node,'action':action,**params}));return {}
    def cleanup(self):self.cleaned=True;return True


class VersionRunnerTests(unittest.TestCase):
    def test_actual_foreground_uses_one_sdk_receipt_and_releases_cap_without_sdk_retry(self):
        for reject in (False,True):
            with self.subTest(reject=reject),tempfile.TemporaryDirectory() as tmp:
                trace=[];binding={'bucketPrefix':'rv-heal-owned','runId':'gh-12345-1-h1','maxInventoryBytes':512*1024**2}
                probe=SDKProbe(binding,trace);seed=Recorder(probe,Path(tmp)/'seed')
                ledger=model.seed_fixture(seed,binding['bucketPrefix'],'batch-version-delete',controls=3,size=17)
                ledger.bind_baseline(probe.inventory(ledger.bucket));plan=model.mutation_plan(ledger,'batch-version-delete',size=17)
                entered,release=threading.Event(),threading.Event();real=probe.s3
                def call(operation,params,body=None):
                    start=time.monotonic_ns();entered.set()
                    if not release.wait(5):raise TimeoutError('test worker did not drain')
                    if reject:raise OSError('original reply lost')
                    value=real(operation,params,body);value.update(callStartedMonotonicNs=start,callFinishedMonotonicNs=time.monotonic_ns())
                    return value
                probe.s3=call;writer=Recorder(probe,Path(tmp)/'foreground');fleet=FakeFleet(['node1'],Path(tmp),'gh-12345-1-h1')
                path='/rustfs/admin/v3/heal/'+ledger.bucket;token=str(uuid.UUID(int=1))
                def status(*unused):
                    self.assertTrue(entered.wait(5));a=time.monotonic_ns();b=time.monotonic_ns()
                    value={'httpStatus':200,'path':path,'query':[('clientToken',token)],'clockDomain':CLOCK_DOMAIN,
                           'callStartedMonotonicNs':a,'callFinishedMonotonicNs':b,
                           'response':{'summary':'running','settings':race.MARKER_REFERENCE['effective'],
                                       'outcome':{'counters':{'processed':1}}}}
                    release.set();return value
                with patch.object(runner.race,'observe',side_effect=status),patch.object(runner,'io_cap') as cap:
                    if reject:
                        with self.assertRaises(ValueError):runner.foreground(writer,ledger,plan,probe,fleet,path,token,race.MARKER_REFERENCE,'batch-version-delete','node1')
                        self.assertTrue(ledger.poisoned)
                    else:
                        values,overlap=runner.foreground(writer,ledger,plan,probe,fleet,path,token,race.MARKER_REFERENCE,'batch-version-delete','node1')
                        self.assertEqual(len(values),1);self.assertTrue(overlap)
                    cap.assert_called_once_with(fleet,runner.CASE,'batch-version-delete',False,nodes=['node1'])
                self.assertEqual(writer.sequence,1)

    def controller(self,branch,*,overlap=True,fail=None,width=4):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);root=Path(tmp.name)
        candidate=root/'candidate.json';candidate.write_text(json.dumps({'source_sha':race.MARKER_CANDIDATE,'package_sha256':'c'*64}))
        rc=root/'rc';rc.write_bytes(b'client');nodes=['node1'] if width==4 else ['node1','node2','node3','node4']
        args=SimpleNamespace(candidate=candidate,evidence=root/'evidence',rc_bin=rc,tool_directory=root,
                             testing_commit='a'*40,binary_sha='b'*64,case_id=runner.CASE)
        instances=[];probes=[];trace=[]
        def fleet(*a,**kw):value=FakeFleet(*a,**kw);instances.append(value);return value
        def probe(binding,endpoint=None):
            if not probes:probes.append(SDKProbe(binding,trace))
            return probes[0]
        real_seed=model.seed_fixture
        def seed(*a,**kw):
            probes[0].fail='put_object' if fail=='seed' else None
            return real_seed(*a,**kw,size=31)
        def foreground(recorder,ledger,plan,*a):
            trace.append(('foreground',));probes[0].fail=plan[0]['operation'] if fail=='foreground' else None
            values=[model.submit(recorder,ledger,step) for step in plan]
            runner.receipts(recorder);return values,overlap
        def mapped(snapshot,bucket,entries):
            return {(row['key'],row['versionId']):{'cohort':[]} for row in entries if not row['deleteMarker']}
        def targets(snapshot,bucket,ledger,*a):
            return [{'node':nodes[0],'disk':3,'key':key,'relativePath':f'{bucket}/{key}/xl.meta',
                     'sha256':'d'*64,'size':300} for key in sorted(ledger.keys)]
        def physical_transition(*a):
            trace.append(('physical',))
            if fail=='physical':raise ValueError('original physical result refused')
            return {'physicalProofBeforeGETOrRescue':True}
        def root_records(*a,**kw):
            return {'original-root':{}} if fail=='root' else {}
        def streams(*a,**kw):
            if fail=='streams-io' and a[2]=='G04-final-full-streams':raise OSError('fsync failed')
            return {'count':kw['count']}
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ,{'GITHUB_RUN_ID':'12345','GITHUB_RUN_ATTEMPT':'1'}))
            for owner,name,value in [(runner,'Fleet',fleet),(runner,'Probe',probe),(runner,'verify_inspector',lambda *a:{'binarySha256':'e'*64}),
                (runner,'validate_topology',Mock()),(model,'seed_fixture',seed),(runner.physical,'metadata_targets',targets),
                (runner.race,'map_fixture',mapped),(runner,'verify_part_manifest',streams),(runner,'validate_missing_batch',Mock()),
                (runner,'io_cap',Mock()),(runner,'partial',Mock()),(runner,'foreground',foreground),
                (runner,'terminal',lambda *a:{'summary':'finished'}),(runner.physical,'transition',physical_transition),
                (runner.physical,'reconstruction',Mock(return_value={})),(runner.race,'terminal_proof',lambda *a:{'completed':True}),
                (runner,'journal_records',Mock()),(runner,'exact_records',root_records)]:
                stack.enter_context(patch.object(owner,name,value))
            result=runner.run_branch(args,'H1' if width==4 else 'H2',nodes,branch)
        return result,instances[0],probes[0],trace

    def test_all_source_g04_plans_preserve_257_metadata_faults_and_full_negative_contracts(self):
        for width in (4,16):
            for branch in model.BRANCHES[4:]:
                with self.subTest(width=width,branch=branch):
                    result,fleet,probe,trace=self.controller(branch,width=width)
                    self.assertEqual((result['verdict'],result['cleanup']),('PASS','PASS'))
                    faults=[v for label,v in fleet.records if label=='remote' and v['action']=='apply-fault']
                    self.assertEqual(len(faults),257);self.assertEqual({v['disk'] for v in faults},{3})
                    self.assertTrue(all(v['relativePath'].endswith('/xl.meta') and v['faultId'].startswith('G04-') for v in faults))
                    self.assertEqual(fleet.faults,[]);self.assertTrue(fleet.cleaned)
                    foreground_index=trace.index(('foreground',));physical_index=trace.index(('physical',))
                    final_reads=[i for i,v in enumerate(trace) if v[0]=='s3' and v[1]=='get_object']
                    self.assertTrue(foreground_index<physical_index<min(final_reads))
                    per_peer={'create-marker':2,'delete-marker':1,'batch-version-delete':4,'unversioned-delete':1}[branch]
                    self.assertEqual(len(final_reads),per_peer*(1 if width==4 else 4))
                    if branch=='batch-version-delete':
                        self.assertEqual(probe.rows['objects-0002.bin'],[])
                        self.assertEqual(len(probe.rows[model.TARGET]),1)
                    self.assertFalse(result['canonicalCasePassClaimed'])

    def test_unknown_seed_or_foreground_never_cleans_retries_or_blindly_restores(self):
        for fail in ('seed','foreground'):
            with self.subTest(fail=fail):
                result,fleet,probe,trace=self.controller('batch-version-delete',fail=fail)
                self.assertEqual((result['verdict'],result['cleanup']),('FAIL','FAIL'));self.assertFalse(fleet.cleaned)
                self.assertTrue(any(label=='retained-failure-scene' for label,_ in fleet.records))
                self.assertFalse(any(v[0]=='s3' and v[1]=='get_object' for v in trace))
                operation='put_object' if fail=='seed' else 'delete_objects'
                self.assertEqual(sum(v[0]=='s3' and v[1]==operation for v in trace),1)

    def test_independent_diagnostics_continue_after_assertion_but_evidence_io_stops(self):
        for fail in ('physical','root','streams-io'):
            with self.subTest(fail=fail):
                result,fleet,probe,trace=self.controller('create-marker',fail=fail)
                self.assertEqual((result['verdict'],result['cleanup']),('FAIL','FAIL'));self.assertFalse(fleet.cleaned)
                reads=[v for v in trace if v[0]=='s3' and v[1]=='get_object']
                self.assertEqual(bool(reads),fail!='streams-io')
                self.assertTrue(any(label=='retained-failure-scene' for label,_ in fleet.records))

    def test_no_overlap_after_complete_authority_is_not_observed_with_safe_cleanup(self):
        result,fleet,_,_=self.controller('delete-marker',overlap=False)
        self.assertEqual((result['verdict'],result['cleanup']),('NOT_OBSERVED','PASS'))
        self.assertTrue(fleet.cleaned)


if __name__=='__main__':unittest.main()
