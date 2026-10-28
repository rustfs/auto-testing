#!/usr/bin/env python3
"""ACK-derived G02-G04 fixtures; never infer versions from wall clocks/current GET.

The fleet owner supplies Recorder/Probe and runs fault/overlap/physical oracles.
This component is not a standalone concurrency or whole-case PASS generator.
"""
# Frozen validation source: 524eb5b32c3236497fdd8a9271df0494fa3d9974
# scripts/heal_version_race_fixture.py; only local import adaptation.
import copy
import hashlib
import threading

try:
    from .heal_probe import bucket_allowed,payload,require
    from .heal_workload_journal import batch_receipts,validate_batch_request
    from .heal_workload import RecordedCallError
except ImportError:
    from heal_probe import bucket_allowed,payload,require
    from heal_workload_journal import batch_receipts,validate_batch_request
    from heal_workload import RecordedCallError

BRANCHES=('new-version-vs-historical-heal','read-all-versions-during-heal',
          'delete-old-version-vs-deep','delete-current-version-vs-deep',
          'create-marker','delete-marker','batch-version-delete','unversioned-delete')
TARGET='objects-0256.bin'
SIZE=257*1024+123


def version_id(value):
    require(isinstance(value,str) and 0<len(value)<=1024 and value!='null',
            'enabled-versioning ACK must contain a non-null VersionId')
    return value


def status(response,expected):
    value=response.get('ResponseMetadata',{}).get('HTTPStatusCode')
    require(type(value) is int and value==expected,'unexpected mutation ACK status')


class VersionLedger:
    """Single serial writer; readers may take immutable identity snapshots.

    Only a complete successful receipt advances a single mutation. A batch
    keeps known per-item ACK effects even when another member is unresolved,
    but permanently blocks final acceptance and subsequent mutations.
    """
    def __init__(self,bucket,keys,*,versioned=True):
        require(type(versioned) is bool,'explicit versioning mode required')
        require(bool(keys) and len(keys)==len(set(keys)) and len(keys)<=512,'bounded distinct owned keys required')
        self.bucket=bucket;self.versioned=versioned;self.keys=set(keys)
        self.history={k:[] for k in keys};self.records={};self.seen=set();self.deleted=[]
        self.poisoned=None;self.bound=False;self.defaults={};self.events=[];self.lock=threading.RLock()

    def _params(self,params):
        require(params.get('Bucket')==self.bucket,'foreign bucket in version ledger')
        if 'Key' in params:require(params['Key'] in self.keys,'unregistered mutation key')

    def _new(self,key,vid,entry):
        identity=(key,vid)
        require(identity not in self.seen,'ACK reused an existing/deleted version identity')
        require(len(self.records)<512,'version ledger exceeds bounded inventory')
        self.seen.add(identity);self.records[identity]=entry;self.history[key].append(vid)

    def _remove(self,key,vid,response):
        identity=(key,vid);require(identity in self.records,'DELETE did not select a known exact version')
        old=self.records[identity]
        if response.get('VersionId') is not None:require(response['VersionId']==vid,'DELETE ACK version differs from request')
        if response.get('DeleteMarker') is not None:
            require(response['DeleteMarker'] is old['deleteMarker'],'DELETE ACK marker/data type differs')
        self.deleted.append(copy.deepcopy(old));del self.records[identity];self.history[key].remove(vid)

    def apply(self,operation,params,response,body=None):
        with self.lock:
            require(self.poisoned is None,'version ledger halted after uncertain or rejected mutation')
            self._params(params)
            try:
                if operation=='put_object':
                    status(response,200);require(isinstance(body,bytes),'PUT requires its exact submitted bytes')
                    require(not response.get('DeleteMarker'),'PUT ACK cannot be a delete marker')
                    vid=version_id(response.get('VersionId')) if self.versioned else 'null'
                    if not self.versioned:require(response.get('VersionId') in (None,'null'),'never-versioned PUT returned a real version')
                    require(isinstance(response.get('ETag'),str) and response['ETag'],'PUT ACK lacks ETag')
                    properties=copy.deepcopy(self.defaults)
                    properties.update(ETag=response['ETag'],ContentLength=len(body),ContentType=params['ContentType'],
                                      Metadata=copy.deepcopy(params['Metadata']))
                    self._new(params['Key'],vid,dict(key=params['Key'],versionId=vid,deleteMarker=False,
                         payload={'size':len(body),'sha256':hashlib.sha256(body).hexdigest()},properties=properties,
                         tags=[{'Key':'fixture','Value':'DV' if self.versioned else 'DB'},{'Key':'suite','Value':'heal'}]))
                elif operation=='delete_object':
                    status(response,204);key=params['Key']
                    if 'VersionId' in params:
                        require(self.versioned,'exact-version branch requires an enabled bucket')
                        self._remove(key,params['VersionId'],response)
                    elif self.versioned:
                        require(response.get('DeleteMarker') is True,'unqualified versioned DELETE lacks marker ACK')
                        vid=version_id(response.get('VersionId'))
                        self._new(key,vid,dict(key=key,versionId=vid,deleteMarker=True))
                    else:
                        require(response.get('VersionId') in (None,'null') and not response.get('DeleteMarker'),
                                'never-versioned DELETE unexpectedly created a marker/version')
                        self._remove(key,'null',response)
                elif operation=='delete_objects':
                    require(self.versioned,'version batch requires enabled bucket');status(response,200)
                    validate_batch_request(params)
                    wanted=params['Delete']['Objects']
                    require(all(v.get('Key') in self.keys and (v['Key'],v.get('VersionId')) in self.records for v in wanted),
                            'batch must freeze only known key/VersionId pairs')
                    checked=batch_receipts(params,response)
                    for item in checked['items']:
                        if item['state']=='ACK':
                            request=item['request'];self._remove(request['Key'],request['VersionId'],item['receipt'])
                    require(not checked['errors'],'DELETE batch contains rejected/unknown/duplicate/unbound receipts')
                else:raise ValueError('unsupported version-ledger mutation')
            except BaseException as exc:
                self.poisoned=str(exc)
                self.events.append(dict(operation=operation,params=copy.deepcopy(params),state='UNRESOLVED',error=str(exc)))
                raise
            self.events.append(dict(operation=operation,params=copy.deepcopy(params),state='ACK',response=copy.deepcopy(response)))

    def halt(self,operation,params,error,*,state='UNKNOWN'):
        with self.lock:
            require(state in ('UNKNOWN','REJECTED','COLLECTOR_ERROR','CHECK_FAILED'),'invalid halt classification')
            self._params(params);self.poisoned=self.poisoned or str(error)
            self.events.append(dict(operation=operation,params=copy.deepcopy(params),state=state,error=str(error)))

    def entries(self):
        with self.lock:
            rows=[]
            for key,ids in self.history.items():
                for vid in ids:rows.append({**copy.deepcopy(self.records[(key,vid)]),'isLatest':vid==ids[-1]})
            return sorted(rows,key=lambda v:(v['key'],v['versionId']))

    def verify_inventory(self,inventory,*,baseline=False):
        with self.lock:
            require(self.poisoned is None,'unresolved mutation prevents inventory acceptance')
            require(baseline or self.bound,'final inventory requires a bound pre-race baseline')
            require(inventory.get('bucket')==self.bucket,'inventory belongs to another bucket')
            expected={(v['key'],v['versionId']):v for v in self.entries()}
            rows=inventory['entries'];actual={(v['key'],v['versionId']):v for v in rows}
            require(len(rows)==len(actual) and set(actual)==set(expected),'ACK version/marker population differs from inventory')
            for identity,wanted in expected.items():
                got=actual[identity]
                for field in ('isLatest','deleteMarker'):require(got.get(field) is wanted[field],'ACK order/marker kind differs from inventory')
                if wanted['deleteMarker']:continue
                require(got.get('payload')==wanted['payload'] and got.get('tags')==wanted['tags'],
                        'version-addressed payload or tags differ from ACK')
                if baseline:
                    require(all(got.get('properties',{}).get(k)==v for k,v in wanted['properties'].items()),
                            'baseline public metadata differs from seed request/ACK')
                else:require(got.get('properties')==wanted['properties'],'retained/new version public properties changed')
            return dict(entries=len(rows),versionSetAndAckOrderVerified=True,casePassClaimed=False,
                        overlapProved=False,physicalRepairProved=False)

    def bind_baseline(self,inventory):
        with self.lock:
            require(not self.bound,'baseline cannot be rebound after race')
            proof=self.verify_inventory(inventory,baseline=True)
            for entry in inventory['entries']:
                current=self.records[(entry['key'],entry['versionId'])]
                if not entry['deleteMarker']:current['properties']=copy.deepcopy(entry['properties'])
            normal=next(v for v in self.records.values() if not v['deleteMarker'])
            self.defaults={k:copy.deepcopy(v) for k,v in normal['properties'].items()
                           if k not in ('ETag','ContentLength','ContentType','Metadata')}
            self.bound=True;return proof

    def exact_reads(self):
        """Never substitute current GET for a historical version identity."""
        return [dict(operation='get_object',params={'Bucket':self.bucket,'Key':v['key'],'VersionId':v['versionId']},
                     expectedPayload=v['payload'],expectedProperties=v['properties']) for v in self.entries() if not v['deleteMarker']]

    def negative_reads(self):
        with self.lock:
            requests=[]
            for old in self.deleted:
                params={'Bucket':self.bucket,'Key':old['key']}
                if self.versioned:params['VersionId']=old['versionId']
                requests.append(dict(params=params,httpStatus=404,errorCode='NoSuchVersion' if self.versioned else 'NoSuchKey'))
            for row in self.entries():
                if row['deleteMarker']:
                    requests.append(dict(params={'Bucket':self.bucket,'Key':row['key'],'VersionId':row['versionId']},
                                         httpStatus=405,errorCode='MethodNotAllowed',deleteMarker=True,versionId=row['versionId']))
                    if row['isLatest']:
                        requests.append(dict(params={'Bucket':self.bucket,'Key':row['key']},httpStatus=404,
                                             errorCode='NoSuchKey',deleteMarker=True,versionId=row['versionId']))
            return requests


def check_negative(response,expected):
    status(response,expected['httpStatus']);require(response.get('Error',{}).get('Code')==expected['errorCode'],
                                                   'negative GET failed for the wrong reason')
    headers={str(k).lower():v for k,v in response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).items()}
    if expected.get('deleteMarker'):
        require(headers.get('x-amz-delete-marker')=='true' and headers.get('x-amz-version-id')==expected['versionId'],
                'marker rejection lacks exact marker headers')


def submit(recorder,ledger,step):
    body=payload(step['nonce'],step['size']) if 'nonce' in step else None
    try:response=recorder.call(step['operation'],step['params'],body,identity=step.get('identity'))
    except RecordedCallError as exc:
        if exc.operation!=step['operation'] or exc.params!=step['params']:
            ledger.halt(step['operation'],step['params'],'recorded failed observation belongs to another request')
            raise ValueError('recorded failed observation belongs to another request') from exc
        observed=exc.observation;response=observed.get('response') or {}
        if observed.get('acknowledged') is True:
            try:ledger.apply(step['operation'],step['params'],response,body)
            except ValueError:pass  # Partial per-item effects and poison were retained by apply.
            ledger.halt(step['operation'],step['params'],exc,state='COLLECTOR_ERROR' if observed.get('clientCleanupError') else 'CHECK_FAILED')
        else:
            code=response.get('ResponseMetadata',{}).get('HTTPStatusCode')
            known_rejection=not observed.get('resultUnknown') and type(code) is int and 400<=code<600
            ledger.halt(step['operation'],step['params'],exc,state='REJECTED' if known_rejection else 'UNKNOWN')
        raise
    except Exception as exc:
        # Recorder has already persisted the raw rejected/unknown result.
        # The caller must not retry it or accept a fresh LIST as resolution.
        ledger.halt(step['operation'],step['params'],exc);raise
    ledger.apply(step['operation'],step['params'],response,body)
    return response


def put_step(bucket,key,nonce,size,*,versioned=True):
    fixture='DV' if versioned else 'DB'
    return dict(operation='put_object',params={'Bucket':bucket,'Key':key,'ContentType':'application/octet-stream',
                 'Metadata':{'heal-fixture':fixture,'nonce':nonce},'Tagging':'suite=heal&fixture='+fixture},nonce=nonce,size=size)


def seed_fixture(recorder,bucket,branch,*,controls=256,size=SIZE):
    require(branch in BRANCHES and type(controls) is int and 3<=controls<=256,'registered branch and 3..256 control keys required')
    require(type(size) is int and 0<size<=64*1024**2 and (controls+6)*size<=128*1024**2,'fixture byte budget exceeded')
    bucket_allowed(recorder.probe.binding,bucket)
    keys=[f'objects-{i:04d}.bin' for i in range(controls)]+[TARGET]
    versioned=branch!='unversioned-delete';ledger=VersionLedger(bucket,keys,versioned=versioned)
    recorder.call('create_bucket',{'Bucket':bucket})
    if versioned:recorder.call('put_bucket_versioning',{'Bucket':bucket,'VersioningConfiguration':{'Status':'Enabled'}})
    for key in keys:
        for generation in range(3 if versioned and key==TARGET else 1):
            submit(recorder,ledger,put_step(bucket,key,f'{bucket}/{key}/{generation}',size,versioned=versioned))
    if branch in ('delete-marker','batch-version-delete'):
        submit(recorder,ledger,dict(operation='delete_object',params={'Bucket':bucket,'Key':TARGET}))
    return ledger


def mutation_plan(ledger,branch,*,size=SIZE):
    with ledger.lock:return _mutation_plan(ledger,branch,size)


def _mutation_plan(ledger,branch,size):
    require(branch in BRANCHES and ledger.bound and ledger.poisoned is None,'bound healthy fixture required before race plan')
    require(ledger.versioned==(branch!='unversioned-delete'),'branch/bucket versioning mismatch')
    rows=[v for v in ledger.entries() if v['key']==TARGET]
    data=[ledger.records[(TARGET,v)] for v in ledger.history[TARGET] if not ledger.records[(TARGET,v)]['deleteMarker']]
    require(len(data)==(3 if ledger.versioned else 1),'target must preserve the exact three-generation seed')
    bucket=ledger.bucket
    if branch in BRANCHES[:2]:
        require(not any(v['deleteMarker'] for v in rows),'new-version seed unexpectedly contains marker')
        for i in range(3):
            key=f'objects-{i:04d}.bin';identity=(key,ledger.history[key][-1])
            body=payload(f'{bucket}/{key}/0',size)
            require(ledger.records[identity]['payload']=={'size':len(body),'sha256':hashlib.sha256(body).hexdigest()},
                    'new-version reference payload differs from its seeded control')
        return [{**put_step(bucket,TARGET,f'{bucket}/objects-{i:04d}.bin/0',size),
                 'referenceKey':f'objects-{i:04d}.bin','identity':{'serialGeneration':i}} for i in range(3)]
    if branch in ('delete-old-version-vs-deep','delete-current-version-vs-deep'):
        chosen=data[0] if branch.startswith('delete-old') else data[-1]
        require(not any(v['deleteMarker'] for v in rows),'version-delete seed unexpectedly contains marker')
        return [dict(operation='delete_object',params={'Bucket':bucket,'Key':TARGET,'VersionId':chosen['versionId']})]
    if branch in ('create-marker','unversioned-delete'):
        require(not any(v['deleteMarker'] for v in rows),'unexpected seed marker')
        return [dict(operation='delete_object',params={'Bucket':bucket,'Key':TARGET})]
    markers=[v for v in rows if v['deleteMarker']];require(len(markers)==1 and markers[0]['isLatest'],'exact latest marker required')
    if branch=='delete-marker':return [dict(operation='delete_object',params={'Bucket':bucket,'Key':TARGET,'VersionId':markers[0]['versionId']})]
    # Distinct key/version pairs; preserve middle data version and other controls.
    control='objects-0002.bin';ids=[(TARGET,data[0]['versionId']),(TARGET,data[-1]['versionId']),
                                   (TARGET,markers[0]['versionId']),(control,ledger.history[control][-1])]
    return [dict(operation='delete_objects',params={'Bucket':bucket,'Delete':{'Quiet':False,
                  'Objects':[{'Key':k,'VersionId':v} for k,v in ids]}})]
