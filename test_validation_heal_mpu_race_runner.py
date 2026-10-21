"""Actual controller/Recorder boundaries; synthetic SDK/physical data is not live proof."""
import copy
from contextlib import ExitStack, nullcontext
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

import test_validation_heal_mpu_race_fixture as model_fixtures
import test_validation_heal_status_keepalive as transport_fixtures
from scripts.validation_heal import mpu_race_runner as runner, mpu_race_fixture as model, race
from scripts.validation_heal.heal_probe import CLOCK_DOMAIN, save
from scripts.validation_heal.heal_workload import Recorder
from scripts.validation_heal.healthy import Fleet


class SDKProbe:
    def __init__(self, root):
        self.binding = transport_fixtures.binding()
        self.sdk = model_fixtures.Recorder(root);Path(root).mkdir()
        self.calls = [];self.gate = None;self.fail = None;self.control = 500;self.controls = {}

    def s3(self, operation, params, body=None):
        started = time.monotonic_ns();self.calls.append((operation,copy.deepcopy(params)))
        if self.gate and self.gate[0] == operation:
            _,entered,release = self.gate;self.gate = None;entered.set()
            if not release.wait(5): raise TimeoutError('component gate not released')
        if self.fail == operation: raise OSError('lost original response; do not retry')
        if operation in {'create_bucket','put_bucket_versioning'}:
            response = {'ResponseMetadata':{'HTTPStatusCode':200}}
        elif operation == 'put_object':
            self.control += 1
            response = {'ResponseMetadata':{'HTTPStatusCode':200},'ETag':model.etag(body),
                        'VersionId':str(uuid.UUID(int=self.control))}
            self.controls[params['Key']] = {'key':params['Key'],'versionId':response['VersionId'],
                'isLatest':True,'deleteMarker':False,'payload':{'size':len(body),'sha256':hashlib.sha256(body).hexdigest()}}
        else:
            response = self.sdk.call(operation,params,body)
        result = {'operation':operation,'params':copy.deepcopy(params),'response':response,
                'acknowledged':200 <= response['ResponseMetadata']['HTTPStatusCode'] < 300,'resultUnknown':False,
                'clockDomain':CLOCK_DOMAIN,'callStartedMonotonicNs':started,'callFinishedMonotonicNs':time.monotonic_ns(),
                'clientCallInvoked':True}
        if operation == 'get_object':
            version = next(v for v in self.sdk.versions if v['VersionId']==params['VersionId']) if 'VersionId' in params else self.sdk.versions[-1]
            result['payload'] = {'size':len(version['body']),'sha256':hashlib.sha256(version['body']).hexdigest()}
        return result

    def observation_for(self, sample): return {'phases':{}}

    def inventory(self, bucket):
        entries = copy.deepcopy(list(self.controls.values()))
        for version in self.sdk.versions:
            entries.append({'key':model.KEY,'versionId':version['VersionId'],'isLatest':version is self.sdk.versions[-1],
                'deleteMarker':False,'payload':{'size':len(version['body']),'sha256':hashlib.sha256(version['body']).hexdigest()}})
        return {'bucket':bucket,'entries':entries}

    def status_session(self, token, path): return nullcontext({'componentOnly':True})


class MpuRunnerTests(unittest.TestCase):
    def controller(self, directory, *, fail_stage=None, overlapped=True):
        """Exercise the actual controller, durable SDK model and final diagnostics.

        Physical/terminal oracles are mocked here; their full-byte tests remain
        independent. This fixture proves ordering and cleanup authority only.
        """
        root = Path(directory);candidate=root/'candidate.json';rc=root/'rc'
        candidate.write_text(json.dumps({'source_sha':race.COPY_CANDIDATE,'package_sha256':'a'*64}));rc.write_bytes(b'client')
        probe=SDKProbe(root/'sdk');trace=[];owned=[]
        args=SimpleNamespace(case_id=runner.CASE,candidate=candidate,evidence=root/'evidence',tool_directory=root,
                             testing_commit='b'*40,binary_sha='c'*64,rc_bin=rc)
        scene={'vm000':{'files':[],'formats':[]}}
        def fleet_factory(*positional,**kwargs):
            fleet=Fleet(*positional,**kwargs);owned.append(fleet)
            fleet.deploy=Mock(side_effect=lambda *a:trace.append('deploy'))
            fleet.stop=Mock(side_effect=lambda:trace.append('stop'))
            fleet.start=Mock(side_effect=lambda:trace.append('start'))
            fleet.snapshot=Mock(side_effect=lambda label,**kw:(trace.append(label) or scene))
            def remote(node,action,**kw):
                if action == 'apply-fault':
                    self.assertEqual(node,'vm000');self.assertEqual(kw['disk'],3)
                    self.assertRegex(kw['faultId'],r'^G05-[0-9a-f]{32}$')
                trace.append(action)
                return {}
            fleet.remote=Mock(side_effect=remote)
            fleet.cleanup=Mock(return_value=True)
            return fleet
        def probe_factory(binding,endpoint=None):
            probe.binding=binding;return probe
        probe.admin=Mock(return_value={'httpStatus':200,'response':{'clientToken':transport_fixtures.TOKEN}})
        real_inventory=probe.inventory
        def inventory(bucket):
            trace.append('inventory');return real_inventory(bucket)
        probe.inventory=inventory
        real_verify=runner.model.MpuRaceFixture.verify_s3
        def verify_s3(fixture,reader):
            trace.append('model-S3');return real_verify(fixture,reader)
        def mapped_controls(snapshot,bucket,controls):
            return {(row['key'],row['versionId']):{**row,'node':'vm000','disk':3,'relativePath':row['key']+'/part.1','sha256':'d'*64}
                    for row in controls}
        def mapped_mpu(snapshot,bucket,entries,**kw):
            return {(row['versionId'],part):{**row,'dataDir':'old-dir','cohort':[{'node':'vm000','disk':3,
                'relativePath':model.KEY+'/part.'+str(part),'sha256':'d'*64}]}
                    for row in entries for part in (1,2,3)},{}
        def committed(*a):
            trace.append('committed-physical')
            if fail_stage=='committed':raise ValueError('committed proof refused')
            return {},{'physicalComponentOnly':True}
        def streams(fleet,parts,label,**kw):
            trace.append(label);self.assertEqual(kw['count'],259 if label=='baseline-streaming' else 262)
            if fail_stage=='streams-io' and label=='final-streaming':raise OSError('evidence fsync failed')
            return {'count':kw['count']}
        def execute(fixture,prepared,*unused):
            trace.append('foreground')
            result=runner.schedule.execute(fixture,prepared['branch'],prepared,
                lambda label,manifest:{'manifest':manifest,'physical':{'componentOnly':True}})
            return result,overlapped
        def terminal_proof(*a):trace.append('terminal-proof');return {'componentOnly':True}
        def roots(*a,**kw):
            trace.append('root-retirement')
            return {'.rustfs.sys/root-heal-'+transport_fixtures.TOKEN+'.json':{}} if fail_stage=='root' else {}
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ,{'GITHUB_RUN_ID':'12345','GITHUB_RUN_ATTEMPT':'1'}))
            patches=[(runner,'Fleet',fleet_factory),(runner,'KeepAliveStatusProbe',probe_factory),(runner,'Probe',probe_factory),
                (runner,'verify_inspector',lambda *a:{'binarySha256':'e'*64}),(runner,'validate_topology',Mock()),
                (runner,'physical_population',Mock()),(runner.race,'map_fixture',mapped_controls),(runner.business,'mapped',mapped_mpu),
                (runner,'verify_part_manifest',streams),(runner.staging,'verify',Mock(return_value={})),
                (runner,'validate_missing_batch',Mock()),(runner,'observe_io',lambda *a:trace.append('io-observe')),
                (runner,'observe_status',Mock()),(runner,'foreground',execute),(runner,'wait_terminal',lambda *a:{'summary':'finished'}),
                (runner,'observe_terminal',lambda *a:{'response':{'summary':'finished'}}),
                (runner,'validate_repaired_physical',Mock()),(runner.business,'transition',committed),
                (runner.race,'terminal_proof',terminal_proof),(runner,'journal_records',Mock()),(runner,'exact_records',roots),
                (runner.model.MpuRaceFixture,'verify_s3',verify_s3),(model,'PART_SIZES',(12,13,14)),(race,'SIZE',31)]
            for target,name,value in patches:stack.enter_context(patch.object(target,name,value))
            result=runner.run_branch(args,'H1',['vm000'],'upload-continue')
        return result,owned[0],trace,probe

    def test_complete_controller_proves_physical_before_final_get_and_only_then_cleans(self):
        with tempfile.TemporaryDirectory() as d:
            result,fleet,trace,probe=self.controller(d)
            self.assertEqual((result['verdict'],result['cleanup']),('PASS','PASS'))
            self.assertFalse(result['canonicalCasePassClaimed']);fleet.cleanup.assert_called_once()
            self.assertEqual(trace.count('apply-fault'),257)
            self.assertEqual(trace.count('io-observe'),2)
            self.assertLess(trace.index('committed-physical'),trace.index('final-streaming'))
            self.assertLess(trace.index('final-streaming'),trace.index('model-S3',trace.index('foreground')))
            self.assertLess(trace.index('physical-after-S3'),trace.index('terminal-proof'))
            self.assertLess(trace.index('terminal-proof'),trace.index('root-retirement'))
            self.assertFalse(fleet.faults)

    def test_assertion_failure_collects_independent_final_diagnostics_and_retains_all_acks(self):
        for stage in ('committed','root'):
            with self.subTest(stage=stage),tempfile.TemporaryDirectory() as d:
                result,fleet,trace,probe=self.controller(d,fail_stage=stage)
                self.assertEqual((result['verdict'],result['cleanup']),('FAIL','FAIL'))
                fleet.cleanup.assert_not_called();self.assertEqual(len(fleet.faults),257)
                self.assertIn('final-streaming',trace);self.assertIn('terminal-proof',trace);self.assertIn('root-retirement',trace)
                self.assertEqual(trace[-1],'mpu-scene')
                acks=sorted((fleet.evidence/'foreground-ACKs').glob('*-ack.json'))
                self.assertEqual(len(acks),3)
                self.assertEqual(json.loads(acks[-1].read_text())['observation']['operation'],'abort_multipart_upload')

    def test_evidence_io_failure_stops_final_diagnostics_and_never_authorizes_cleanup(self):
        with tempfile.TemporaryDirectory() as d:
            result,fleet,trace,_=self.controller(d,fail_stage='streams-io')
            self.assertEqual((result['verdict'],result['cleanup']),('FAIL','FAIL'))
            self.assertEqual(result['failure']['type'],'OSError');fleet.cleanup.assert_not_called()
            self.assertNotIn('terminal-proof',trace);self.assertIn('mpu-scene',trace)

    def test_missing_overlap_can_clean_only_after_all_physical_s3_and_root_proofs(self):
        with tempfile.TemporaryDirectory() as d:
            result,fleet,trace,_=self.controller(d,overlapped=False)
            self.assertEqual((result['verdict'],result['cleanup']),('NOT_OBSERVED','PASS'))
            self.assertIn('root-retirement',trace);fleet.cleanup.assert_called_once();self.assertFalse(fleet.faults)

    def test_real_seed_and_durable_pairing_preserve_all_source_controls_and_same_key_uploads(self):
        for branch in runner.schedule.SELECTED:
            with self.subTest(branch=branch),tempfile.TemporaryDirectory() as d, \
                 patch.object(model,'PART_SIZES',(12,13,14)),patch.object(race,'SIZE',31):
                probe = SDKProbe(Path(d)/'sdk');recorder = Recorder(probe,Path(d)/'seed')
                controls,fixture,prepared = runner.seed(recorder,probe.binding['bucketPrefix'],branch)
                self.assertEqual([row['key'] for row in controls],[f'objects-{i:04d}.bin' for i in range(256)])
                self.assertEqual(len(runner.ack_records(recorder)),270+int(branch!='upload-continue'))
                self.assertEqual([len(prepared['manifest']['items'][r]['parts']) for r in model.ROLES],
                                 [3,2 if branch=='upload-continue' else 3,3])
                self.assertEqual(len({item['uploadId'] for item in fixture.items.values()}),3)
                self.assertTrue(all(params.get('Key') == model.KEY for op,params in probe.calls if 'multipart' in op or op=='upload_part'))

    def test_original_sdk_pairing_rejects_orphans_changed_requests_or_unknown_results(self):
        for fault in ('request','ack','sha','sequence','operation','params','unknown'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as d:
                probe=SDKProbe(Path(d)/'sdk');recorder=Recorder(probe,Path(d)/'seed')
                recorder.call('create_bucket',{'Bucket':probe.binding['bucketPrefix']})
                req=recorder.output/'000001-request.json';ack=recorder.output/'000001-ack.json'
                if fault=='request':req.unlink()
                elif fault=='ack':ack.unlink()
                else:
                    value=json.loads(ack.read_text())
                    if fault=='sha':value['requestSha256']='f'*64
                    elif fault=='sequence':value['sequence']=2
                    else:value['observation'][{'operation':'operation','params':'params','unknown':'resultUnknown'}[fault]]=True if fault=='unknown' else 'foreign'
                    ack.write_text(json.dumps(value))
                with self.assertRaises(ValueError):runner.ack_records(recorder)

    def test_unknown_selected_foreground_result_halts_all_companions_without_retry(self):
        for branch,operation in runner.schedule.SELECTED.items():
            with self.subTest(branch=branch),tempfile.TemporaryDirectory() as d, \
                 patch.object(model,'PART_SIZES',(12,13,14)),patch.object(race,'SIZE',31):
                root=Path(d);probe=SDKProbe(root/'sdk');seed=Recorder(probe,root/'seed')
                _,fixture,prepared=runner.seed(seed,probe.binding['bucketPrefix'],branch)
                fixture.recorder=Recorder(probe,root/'foreground');probe.fail=operation
                original_calls=len(probe.calls)
                fleet=SimpleNamespace(nodes=['vm000'],evidence=root,record=Mock())
                with patch.object(runner,'observe_status',return_value={}),patch.object(runner,'capture_staging') as capture:
                    with self.assertRaises(ValueError):
                        runner.foreground(fixture,prepared,probe,fleet,'path','token',race.COPY_REFERENCE,'a'*64)
                self.assertEqual([op for op,_ in probe.calls[original_calls:]],[operation]);capture.assert_not_called()
                self.assertIsNotNone(fixture.poisoned)
                ack=json.loads((root/'foreground/000001-ack.json').read_text())
                self.assertTrue(ack['observation']['resultUnknown']);self.assertEqual(ack['operationCheck'],'FAIL')

    def test_stopped_original_root_reaches_final_diagnostics_without_becoming_success(self):
        probe=Mock();fleet=Mock();fleet.record.side_effect=lambda label,value:value
        probe.admin.return_value={'httpStatus':200,'response':{'summary':'stopped','settings':race.COPY_REFERENCE['effective']}}
        with patch.object(runner.time,'sleep',side_effect=AssertionError('stopped root must drain immediately')):
            terminal=runner.wait_terminal(probe,fleet,'path','token',race.COPY_REFERENCE)
        self.assertEqual(terminal['summary'],'stopped')
        with self.assertRaisesRegex(ValueError,'terminal coverage'):
            race.terminal_proof(terminal,'bucket',{'entries':[]},{'entries':[]})

    def test_selected_first_sdk_overlap_does_not_require_later_companions_to_overlap(self):
        for branch in runner.schedule.SELECTED:
            with self.subTest(branch=branch),tempfile.TemporaryDirectory() as d, \
                 patch.object(model,'PART_SIZES',(12,13,14)),patch.object(race,'SIZE',31):
                root=Path(d);probe=SDKProbe(root/'sdk');seed=Recorder(probe,root/'seed')
                _,fixture,prepared=runner.seed(seed,probe.binding['bucketPrefix'],branch)
                fixture.recorder=Recorder(probe,root/'foreground')
                entered,release=threading.Event(),threading.Event();probe.gate=(prepared['selectedOperation'],entered,release)
                fleet=SimpleNamespace(nodes=['vm000'],evidence=root,record=Mock(side_effect=lambda label,value:value))
                path='/rustfs/admin/v3/heal/'+probe.binding['bucketPrefix'];token=str(uuid.UUID(int=1))
                def observed(*args):
                    self.assertTrue(entered.wait(5));started=time.monotonic_ns();finished=time.monotonic_ns()
                    release.set()
                    return {'path':path,'query':[('clientToken',token)],'httpStatus':200,'clockDomain':CLOCK_DOMAIN,
                        'callStartedMonotonicNs':started,'callFinishedMonotonicNs':finished,
                        'statusObservation':{'sessionId':'component','sequence':1},
                        'response':{'summary':'running','settings':race.COPY_REFERENCE['effective'],
                                    'outcome':{'counters':{'processed':1}}}}
                with patch.object(runner,'observe_status',side_effect=observed),patch.object(runner,'capture_staging',return_value={'mock':True}):
                    result,overlapped=runner.foreground(fixture,prepared,probe,fleet,path,token,race.COPY_REFERENCE,'a'*64)
                self.assertTrue(overlapped)
                acks=runner.ack_records(fixture.recorder)
                self.assertEqual(result['selectedAckSequence'],1)
                proof=fleet.record.call_args.args[1]
                self.assertEqual(proof['selectedOperation'],prepared['selectedOperation'])
                self.assertEqual(len(acks),3 if branch=='upload-continue' else 2)
                self.assertTrue(proof['allCompanionACKsVerified']);self.assertFalse(proof['nativeFenceProved'])
                self.assertEqual((fixture.items['a']['state'],fixture.items['b']['state']),('complete','aborted'))

    def test_seed_unknown_response_preserves_stopped_raw_scene_without_abort_restore_or_cleanup(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);candidate=root/'candidate.json';candidate.write_text(json.dumps({'source_sha':race.COPY_CANDIDATE,'package_sha256':'a'*64}))
            rc=root/'rc';rc.write_bytes(b'owned client');probe=SDKProbe(root/'sdk');probe.fail='create_multipart_upload'
            fleet=Mock();fleet.nodes=['vm000'];fleet.record.side_effect=lambda label,value:value
            fleet.remote.return_value={'readOnly':True};fleet.snapshot.return_value={'vm000':{'files':[],'formats':[]}}
            args=SimpleNamespace(case_id=runner.CASE,candidate=candidate,evidence=root/'evidence',tool_directory=root,
                                 testing_commit='b'*40,binary_sha='c'*64,rc_bin=rc)
            def failed_seed(recorder,bucket,branch):
                recorder.probe.binding['bucketPrefix']=bucket
                recorder.call('create_multipart_upload',{'Bucket':bucket,'Key':model.KEY})
            with patch.dict(os.environ,{'GITHUB_RUN_ID':'12345','GITHUB_RUN_ATTEMPT':'1'}), \
                 patch.object(runner,'Fleet',return_value=fleet),patch.object(runner,'KeepAliveStatusProbe',return_value=probe), \
                 patch.object(runner,'verify_inspector',return_value={}),patch.object(runner,'validate_topology'), \
                 patch.object(runner,'seed',side_effect=failed_seed):
                result=runner.run_branch(args,'H1',['vm000'],'upload-continue')
            self.assertEqual((result['verdict'],result['cleanup']),('FAIL','FAIL'))
            fleet.cleanup.assert_not_called();fleet.start.assert_not_called()
            self.assertEqual([op for op,_ in probe.calls],['create_multipart_upload'])
            self.assertTrue(any(call.args[:2] == ('vm000','mpu-scene') for call in fleet.remote.call_args_list))
            self.assertTrue(any(call.args[0]=='last-ACK-rescue-required' for call in fleet.record.call_args_list))


class DurableStatusTests(unittest.TestCase):
    # Reuse the actual socket fixture; only these new integration checks run here.
    setUp = transport_fixtures.KeepaliveTests.setUp
    tearDown = transport_fixtures.KeepaliveTests.tearDown

    def test_g05_status_persists_original_request_before_send_and_response_after_receive(self):
        with tempfile.TemporaryDirectory() as d,patch.object(race,'settings'), \
             patch('scripts.validation_heal.heal_probe.os.fsync',wraps=os.fsync) as sync, \
             patch.dict(os.environ,{'HEAL_ACCESS_KEY':'local-access','HEAL_SECRET_KEY':'local-secret'}):
            fleet=Fleet(['vm000'],Path(d),'gh-12345-1-h1');path='/rustfs/admin/v3/heal/'+self.probe.binding['bucketPrefix']
            with self.probe.status_session(transport_fixtures.TOKEN,path) as state:
                value=runner.observe_status(self.probe,fleet,path,transport_fixtures.TOKEN,race.COPY_REFERENCE)
                measured=state['observations'][0]
                self.assertEqual(sync.call_count,4);self.assertTrue(measured['responsePersisted'])
                self.assertLessEqual(measured['phases']['requestPersistence']['finishedMonotonicNs'],value['callStartedMonotonicNs'])
                self.assertLessEqual(value['callFinishedMonotonicNs'],measured['phases']['responsePersistence']['startedMonotonicNs'])
                request=Path(d)/measured['requestFile'];response=Path(d)/measured['responseFile']
                self.assertEqual(hashlib.sha256(request.read_bytes()).hexdigest(),value['requestSha256'])
                self.assertEqual(json.loads(response.read_text())['requestSha256'],value['requestSha256'])
                self.assertNotIn('local-secret',json.dumps(state))
