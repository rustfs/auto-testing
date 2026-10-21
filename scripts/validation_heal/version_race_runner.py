"""Frozen G04 metadata-loss races on the owned auto-testing H1/H2 fleet."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import secrets
import time
from urllib.parse import quote
import uuid

try:
    from . import race, version_race_fixture as model, version_race_adapter as physical
    from .heal_probe import Probe, canonical, require, save
    from .heal_workload import Recorder
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .cursor import validate_missing_batch, verify_part_manifest
    from .heal_fault import bounded_fault_id
    from .race_io import set_fleet as io_cap
    from .checkpoint import records as journal_records
    from .persistence import exact_records
except ImportError:
    import race, version_race_fixture as model, version_race_adapter as physical
    from heal_probe import Probe, canonical, require, save
    from heal_workload import Recorder
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from cursor import validate_missing_batch, verify_part_manifest
    from heal_fault import bounded_fault_id
    from race_io import set_fleet as io_cap
    from checkpoint import records as journal_records
    from persistence import exact_records


CASE='heal.race-markers-and-batch-delete'


def receipts(recorder):
    requests=sorted(recorder.output.glob('*-request.json'));acks=sorted(recorder.output.glob('*-ack.json'))
    require(len(requests)==len(acks)==recorder.sequence,'G04 SDK journal has an orphan request or receipt')
    values=[]
    for index,(request_path,ack_path) in enumerate(zip(requests,acks,strict=True),1):
        request=json.loads(request_path.read_bytes());ack=json.loads(ack_path.read_bytes())
        observation=ack.get('observation') or {}
        require(request_path.name==f'{index:06d}-request.json' and ack_path.name==f'{index:06d}-ack.json'
                and request.get('sequence')==ack.get('sequence')==index
                and request.get('runId')==ack.get('runId')==recorder.probe.binding['runId']
                and ack.get('requestFile')==request_path.name
                and ack.get('requestSha256')==hashlib.sha256(request_path.read_bytes()).hexdigest()
                and ack.get('operationCheck')=='PASS' and observation.get('resultUnknown') is False
                and observation.get('operation')==request.get('operation')
                and observation.get('params')==request.get('params'),
                'G04 SDK request/receipt identity, hash or result differs')
        values.append(ack)
    return values


def partial(probe,fleet,path,token,reference,before,targets,bucket):
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        observed=race.observe(probe,fleet,path,token,reference)
        snapshot=fleet.snapshot('G04-partial-physical-window');complete=[];pending=[]
        for target in targets:
            try:physical.reconstruction(before,snapshot,bucket,target)
            except ValueError:pending.append(target['key'])
            else:complete.append(target['key'])
        fleet.record('G04-partial-window',{'status':observed,'completeKeys':complete,'pendingKeys':pending})
        value=observed['response'];processed=(value.get('outcome') or {}).get('counters',{}).get('processed')
        if value['summary']=='running' and type(processed) is int and processed>0 and complete and len(pending)>=32:
            target=targets[-1]
            if model.TARGET not in pending or (target['node'],3,target['relativePath']) in race.files(snapshot):
                raise race.WindowNotObserved('G04 selected metadata repaired before the frozen partial window')
            return snapshot
        if not pending:raise race.WindowNotObserved('G04 all metadata repaired before the partial window')
        require(value['summary']=='running','G04 original root stopped before physical partial repair')
        time.sleep(.25)
    raise race.WindowNotObserved('G04 no nonzero physical partial window within 120 seconds')


def foreground(recorder,ledger,plan,probe,fleet,path,token,reference,branch,target_node):
    require(len(plan)==1,'G04 requires one selected source mutation')
    samples=[]
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            worker=pool.submit(model.submit,recorder,ledger,plan[0]);deadline=time.monotonic()+90
            try:
                while not worker.done() and time.monotonic()<deadline:
                    samples.append(race.observe(probe,fleet,path,token,reference));time.sleep(.025)
                if not worker.done():recorder.mutations.poison('G04 observation deadline expired; drain only')
            except BaseException:
                recorder.mutations.poison('G04 status failed; drain the original SDK call only');raise
            finally:
                io_cap(fleet,CASE,branch,False,nodes=[target_node])
            response=worker.result(timeout=40)
    finally:
        fleet.record('G04-ACK-model',{'entries':ledger.entries(),'deleted':ledger.deleted,
                                    'events':ledger.events,'poisoned':ledger.poisoned})
    acks=receipts(recorder);require(len(acks)==1 and ledger.poisoned is None,'G04 mutation lacks complete original authority')
    overlap=any(race.sample_overlap(acks[0],sample,path,token,reference) for sample in samples)
    fleet.record('G04-public-call-overlap',{'selectedCallOverlapObserved':overlap,'nativeFenceProved':False})
    return [response],overlap


def terminal(probe,fleet,path,token,reference):
    deadline=time.monotonic()+180
    while True:
        value=fleet.record('G04-terminal-status',probe.admin(path,[('clientToken',token)],b''))
        require(value.get('httpStatus')==200 and isinstance(value.get('response'),dict)
                and canonical(value['response'].get('settings'))==canonical(reference['effective']),
                'G04 original terminal status or settings disappeared')
        if value['response'].get('summary')!='running':return value['response']
        require(time.monotonic()<deadline,'G04 original completion deadline exceeded');time.sleep(.25)


def negative_checks(recorder,expected):
    results=[];errors=[]
    for index,wanted in enumerate(expected,1):
        result={'expected':wanted,'state':'FAIL','canonicalCasePassClaimed':False}
        try:
            value=recorder.call('get_object',wanted['params'],expect_http=wanted['httpStatus'])
            model.check_negative(value,wanted);result['state']='PASS'
        except ValueError as error:
            result['error']=str(error);errors.append({'index':index,'error':str(error)})
        save(recorder.output/f'{index:06d}-negative-proof.json',result);results.append(result)
    return {'attempted':len(results),'results':results,'errors':errors,'canonicalCasePassClaimed':False}


def final_checks(fleet,binding,before,baseline,ledger,targets,terminal_value,path,token,reference,inspector_sha):
    violations=[]
    def check(label,operation):
        try:proof=operation()
        except ValueError as error:
            proof={'state':'FAIL','error':str(error),'passClaimed':False};violations.append({'stage':label,'error':str(error)})
        fleet.record(label,proof);return proof
    bucket=ledger.bucket
    fleet.stop();after=fleet.snapshot('product-physical-before-GET-or-rescue',quiescent=True)
    def controls():
        for target in targets:
            if target['key'] not in physical.SELECTED:physical.reconstruction(before,after,bucket,target)
        return {'otherControlsReconstructed':253}
    check('G04-control-reconstruction',controls)
    proof=check('G04-version-physical-transition',lambda:physical.transition(before,after,bucket,
        baseline['entries'],ledger.entries(),inspector_sha))
    def streams():
        mapped=race.map_fixture(after,bucket,ledger.entries())
        return verify_part_manifest(fleet,list(mapped.values()),'G04-final-full-streams',count=len(mapped))
    check('G04-final-stream-proof',streams)
    fleet.start();inventory=None
    for endpoint in binding['endpoints']:
        peer=Probe(binding,endpoint);reader=Recorder(peer,fleet.evidence/('negative-read-ACKs-'+endpoint))
        negatives=negative_checks(reader,ledger.negative_reads());fleet.record('G04-negative-reads-'+endpoint,negatives)
        if negatives['errors']:violations.append({'stage':'negative-reads-'+endpoint,'errors':negatives['errors']})
        check('G04-negative-ledger-'+endpoint,lambda:receipts(reader))
        current=fleet.record('G04-final-S3-'+endpoint,peer.inventory(bucket,unversioned_current_tags=not ledger.versioned))
        check('G04-final-S3-proof-'+endpoint,lambda:ledger.verify_inventory(current));inventory=current
    fleet.stop()
    def unchanged():
        require(physical_content(after)==physical_content(fleet.snapshot('physical-after-S3',quiescent=True)),
                'G04 final negative GET or inventory changed the pre-GET physical result')
        return {'postGetPhysicalUnchanged':True}
    check('G04-post-GET-physical',unchanged)
    completion=check('G04-original-terminal-proof',lambda:race.terminal_proof(terminal_value,bucket,baseline,inventory))
    def retired():
        roots=exact_records(journal_records(fleet,token,'G04-original-root-retirement',quiescent=True,family='root'),token,'root')
        require(not roots,'G04 original runnable root responsibility was not retired');return {'originalRootRetired':True}
    check('G04-root-retirement-proof',retired)
    fleet.record('G04-contract-violations',violations)
    require(not violations,'G04 final contracts failed; independent diagnostics retained')
    return proof,completion


def run_branch(args,topology,nodes,branch):
    require(args.case_id==CASE and branch in model.BRANCHES[4:] and topology in ('H1','H2')
            and len(nodes)==(1 if topology=='H1' else 4),'G04 requires its registered H1/H2 branch')
    candidate=json.loads(args.candidate.read_text());reference=race.source_contract(candidate,CASE)
    require(candidate['source_sha']==race.MARKER_CANDIDATE,'G04 requires its reviewed marker/batch native candidate')
    directory=args.evidence/topology/branch;directory.mkdir(parents=True,exist_ok=False)
    run_id=f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT','1')}-{topology.lower()}"
    fleet=Fleet(nodes,directory,run_id,args.tool_directory,runtime_profile='frozen-race-legacy')
    inspector=fleet.record('inspector-build',verify_inspector(args.tool_directory,candidate))
    bucket='rv-heal-'+hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access,secret=secrets.token_hex(16),secrets.token_hex(24);os.environ.update(HEAL_ACCESS_KEY=access,HEAL_SECRET_KEY=secret)
    binding={'schemaVersion':1,'runId':run_id,'jumpHostOnly':True,'exclusiveHealService':True,
        'leaseExpiresAt':(datetime.now(timezone.utc)+timedelta(minutes=45)).isoformat(),'bucketPrefix':bucket,
        'endpoints':{f'node{i}':f'http://rustfs-node{i}:9000' for i in range(1,len(nodes)+1)},'defaultEndpoint':'node1',
        'candidateCommit':candidate['source_sha'],'harnessCommit':args.testing_commit,
        'candidatePackageSha256':candidate['package_sha256'],'candidateBinarySha256':args.binary_sha,
        'rcBinarySha256':hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        'maxInventoryObjects':512,'maxInventoryBytes':512*1024**2}
    fleet.record('binding',binding);probe=Probe(binding)
    fleet.record('source-contract',{'caseId':CASE,'branch':branch,'objects':257,'fault':'fourth-drive-xl.meta',
        'requested':reference['requested'],'effective':reference['effective'],'canonicalCasePassClaimed':False})
    result={'caseId':CASE,'topology':topology,'branch':branch,'fixture':'DB' if branch=='unversioned-delete' else 'DV',
            'verdict':'RUNNING','cleanup':'PENDING','canonicalCasePassClaimed':False}
    recorder=None;authority=False
    try:
        fleet.deploy(args.binary_sha,bucket,access,secret)
        fleet.record('topology-proof',validate_topology(fleet.snapshot('deployed-topology'),topology))
        recorder=Recorder(probe,directory/'seed-ACKs');ledger=model.seed_fixture(recorder,bucket,branch)
        fleet.record('G04-original-seed-ledger',receipts(recorder))
        baseline=fleet.record('complete-baseline-S3',probe.inventory(bucket,unversioned_current_tags=not ledger.versioned))
        fleet.record('G04-baseline-proof',ledger.bind_baseline(baseline));plan=model.mutation_plan(ledger,branch)
        fleet.record('frozen-foreground-plan',plan);fleet.stop()
        before=fleet.snapshot('complete-baseline-physical',quiescent=True)
        targets=physical.metadata_targets(before,bucket,ledger,branch,inspector['binarySha256'])
        mapped=race.map_fixture(before,bucket,ledger.entries())
        fleet.record('G04-baseline-stream-proof',verify_part_manifest(fleet,list(mapped.values()),'G04-baseline-full-streams',count=len(mapped)))
        fault_receipts=[]
        for index,target in enumerate(targets):
            params={'disk':3,'faultId':bounded_fault_id('G04',branch,index)};fleet.faults.append((target['node'],params))
            fault_receipts.append(fleet.record('exact-missing-metadata',{'node':target['node'],**fleet.remote(target['node'],
                'apply-fault',**params,relativePath=target['relativePath'],expectedSha256=target['sha256'],kind='missing')}))
        damaged=fleet.snapshot('complete-fault-physical',quiescent=True)
        fleet.record('fault-batch-proof',validate_missing_batch(before,damaged,targets,fault_receipts))
        fleet.start();io_cap(fleet,CASE,branch,True,nodes=[nodes[0]])
        require(physical_content(damaged)==physical_content(fleet.snapshot('attribution-before-START')),
                'G04 metadata changed before original manual admission')
        path='/rustfs/admin/v3/heal/'+quote(bucket,safe='')
        start=fleet.record('original-Admin-start',probe.admin(path,[],json.dumps(reference['requested']).encode()))
        token=start.get('response',{}).get('clientToken') if isinstance(start.get('response'),dict) else None
        require(start.get('httpStatus')==200 and isinstance(token,str) and str(uuid.UUID(token))==token,
                'G04 admission lacks one original canonical token')
        partial(probe,fleet,path,token,reference,before,targets,bucket)
        writer=Recorder(probe,directory/'foreground-ACKs')
        _,overlap=foreground(writer,ledger,plan,probe,fleet,path,token,reference,branch,nodes[0])
        done=terminal(probe,fleet,path,token,reference)
        proof,result['terminal']=final_checks(fleet,binding,before,baseline,ledger,targets,done,path,token,reference,inspector['binarySha256'])
        authority=True;fleet.faults.clear()
        if not overlap:raise race.WindowNotObserved('G04 original SDK call lacks a contained running-token sample')
        result.update(verdict='PASS',publicCallOverlapObserved=True,physical=proof,nativeFenceProved=False)
    except (Exception,KeyboardInterrupt) as error:
        result.update(verdict='NOT_OBSERVED' if isinstance(error,race.WindowNotObserved) else 'FAIL',
                      failure={'type':type(error).__name__,'message':str(error)})
    finally:
        if recorder is not None and recorder.sequence>0 and not authority:
            try:fleet.stop();fleet.snapshot('retained-failure-scene',quiescent=True)
            except Exception as error:fleet.record('failure-scene-error',{'type':type(error).__name__,'message':str(error)})
            fleet.record('last-ACK-rescue-required',{'state':'DIRTY','namespacesRetained':True,
                'lastACKPhysicalAuthorityVerified':False,'originalGenerationBlindRestorationProhibited':True})
            result['cleanup']='FAIL'
        else:result['cleanup']='PASS' if fleet.cleanup() else 'FAIL'
        fleet.record('branch-result',result)
    return result
