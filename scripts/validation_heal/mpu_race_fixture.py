#!/usr/bin/env python3
"""G05 ACK-bound same-key multipart fixture; no topology or Heal PASS claim.

One previously committed MPU plus two independent uploads for the same key.
Caller owns the fresh versioned bucket, endpoint, physical witnesses and race.
"""
import copy
import hashlib
import json
import re
import threading
import uuid

try:
    from .heal_probe import require, save
except ImportError:
    from heal_probe import require, save

# Match the frozen common DM recipe while retaining its same-key A/B model.
PART_SIZES=(5*1024**2+17,5*1024**2+93,1024**2+47)
ROLES=('committed','a','b')
KEY='objects-0256.bin'


def payload(bucket,role,number):
    require(role in ROLES and type(number) is int and 1<=number<=3,'registered multipart payload required')
    return hashlib.shake_256(f'G05:{bucket}:{role}:{number}'.encode()).digest(PART_SIZES[number-1])


def etag(body):return '"'+hashlib.md5(body,usedforsecurity=False).hexdigest()+'"'


def multipart_etag(parts):
    require([p['number'] for p in parts]==[1,2,3],'exact ordered three-part ACK manifest required')
    require(all(re.fullmatch(r'"[0-9a-f]{32}"',p['etag']) for p in parts),'plain fixture part MD5 required')
    raw=b''.join(bytes.fromhex(p['etag'][1:-1]) for p in parts)
    return '"'+hashlib.md5(raw,usedforsecurity=False).hexdigest()+'-3"'


def pages(recorder,operation,params):
    markers={'list_parts':('PartNumberMarker','NextPartNumberMarker'),
             'list_multipart_uploads':('KeyMarker','NextKeyMarker','UploadIdMarker','NextUploadIdMarker'),
             'list_object_versions':('KeyMarker','NextKeyMarker','VersionIdMarker','NextVersionIdMarker')}
    require(operation in markers,'only bounded owned multipart/version listings allowed')
    request=copy.deepcopy(params);seen=set();out=[]
    for _ in range(32):
        value=recorder.call(operation,request);out.append(value)
        require(type(value.get('IsTruncated')) is bool,'listing omitted typed truncation flag')
        if not value['IsTruncated']:return out
        fields=markers[operation];next_values={fields[i]:value.get(fields[i+1]) for i in range(0,len(fields),2)}
        if operation=='list_parts':
            require(type(next_values['PartNumberMarker']) is int and next_values['PartNumberMarker']>request.get('PartNumberMarker',0),
                    'part pagination did not advance')
        else:require(all(isinstance(v,str) and v for v in next_values.values()),'listing omitted continuation identity')
        identity=json.dumps(next_values,sort_keys=True)
        require(identity not in seen,'listing repeated a continuation cursor');seen.add(identity)
        request.update(next_values)
    raise ValueError('owned listing exceeds 32-page budget')


class MpuRaceFixture:
    def __init__(self,recorder,bucket,key=KEY):
        require(key==KEY and isinstance(bucket,str) and re.fullmatch('[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',bucket),
                'fixed G05 key and owned fixture bucket required')
        self.recorder=recorder;self.bucket=bucket;self.key=key
        self.items={};self.events=[];self.poisoned=None;self.lock=threading.RLock();self.seed_started=False

    def manifest(self):
        with self.lock:return copy.deepcopy(dict(bucket=self.bucket,key=self.key,items=self.items,events=self.events,
             poisoned=self.poisoned,fixture='DM',topologyProved=False,physicalRepairProved=False,overlapProved=False,
             casePassClaimed=False,matrix84PassClaimed=False))

    def _persist(self):
        (getattr(self.recorder,'writer',None) or save)(self.recorder.output/f'mpu-manifest-{len(self.events):04d}.json',self.manifest())

    def _mutation(self,role,operation,params,apply,body=None):
        require(self.poisoned is None,'multipart fixture halted; do not retry or mutate sibling upload')
        response=None
        try:
            response=self.recorder.call(operation,params,body,identity={'mpuRole':role},
                                        expect_http=204 if operation=='abort_multipart_upload' else 200)
            apply(response)
        except BaseException as exc:
            self.poisoned=f'{type(exc).__name__}: {exc}'
            self.events.append(dict(role=role,operation=operation,params=copy.deepcopy(params),
                 state='UNRESOLVED',responseReceived=response is not None,error=self.poisoned))
            self._persist();raise
        self.events.append(dict(role=role,operation=operation,params=copy.deepcopy(params),state='ACK',response=copy.deepcopy(response)))
        try:self._persist()
        except BaseException as exc:
            self.poisoned=f'manifest persistence failed: {exc}';raise
        return response

    def create(self,role):
        with self.lock:
            require(len(self.items)<3 and role==ROLES[len(self.items)],'unknown/repeated/out-of-order fixture upload role')
            if role!='committed':require(self.items['committed']['state']=='complete','original committed object must exist first')
            metadata={'case':'G05','role':role,'nonce':hashlib.sha256(f'{self.bucket}:{role}'.encode()).hexdigest()[:24]}
            def apply(response):
                upload=response.get('UploadId')
                require(isinstance(upload,str) and 1<=len(upload)<=256 and
                        upload not in {v['uploadId'] for v in self.items.values()},'missing/reused UploadId')
                self.items[role]=dict(role=role,uploadId=upload,state='pending',parts=[],metadata=metadata)
            return self._mutation(role,'create_multipart_upload',{'Bucket':self.bucket,'Key':self.key,
                         'Metadata':metadata,'ContentType':'application/octet-stream'},apply)

    def upload(self,role,number):
        with self.lock:
            item=self.items[role]
            require(item['state']=='pending' and type(number) is int and number==len(item['parts'])+1 and number<=3,
                    'only next known pending part can be uploaded; no retry/overwrite')
            body=payload(self.bucket,role,number)
            def apply(response):
                require(response.get('ETag')==etag(body),'UploadPart ACK differs from exact submitted bytes')
                item['parts'].append(dict(number=number,size=len(body),etag=response['ETag'],sha256=hashlib.sha256(body).hexdigest()))
            return self._mutation(role,'upload_part',{'Bucket':self.bucket,'Key':self.key,'UploadId':item['uploadId'],
                                  'PartNumber':number},apply,body)

    def complete(self,role):
        with self.lock:
            require(role in ('committed','a'),'B is the independently aborted upload')
            item=self.items[role];require(item['state']=='pending','upload is already terminal')
            if role=='committed':require(set(self.items)=={'committed'},'original MPU must complete before pending pair')
            expected=multipart_etag(item['parts'])
            def apply(response):
                vid=response.get('VersionId')
                require(isinstance(vid,str) and str(uuid.UUID(vid))==vid and uuid.UUID(vid).int!=0 and
                        vid not in {v.get('versionId') for v in self.items.values()},'missing/reused committed VersionId')
                require(response.get('ETag')==expected,'Complete ACK multipart ETag differs from ordered ACK parts')
                body=b''.join(payload(self.bucket,role,n) for n in (1,2,3))
                item.update(state='complete',versionId=vid,etag=expected,payload={'size':len(body),'sha256':hashlib.sha256(body).hexdigest()})
            return self._mutation(role,'complete_multipart_upload',{'Bucket':self.bucket,'Key':self.key,
                'UploadId':item['uploadId'],'MultipartUpload':{'Parts':[{'PartNumber':p['number'],'ETag':p['etag']} for p in item['parts']]}},apply)

    def abort_b(self):
        with self.lock:
            item=self.items['b'];require(item['state']=='pending','B is already terminal')
            return self._mutation('b','abort_multipart_upload',{'Bucket':self.bucket,'Key':self.key,'UploadId':item['uploadId']},
                                  lambda response:item.update(state='aborted'))

    def seed(self):
        with self.lock:
            require(not self.seed_started and not self.items,'fresh fixture required');self.seed_started=True
            self.create('committed')
            for number in (1,2,3):self.upload('committed',number)
            self.complete('committed')
            self.create('a')
            for number in (1,2):self.upload('a',number)
            self.create('b')
            for number in (1,2,3):self.upload('b',number)
            return self.manifest()

    def verify_s3(self,recorder):
        """Verify one endpoint; parent repeats for required topology endpoints.

        This deliberately requires current/old object identity while A/B are
        pending, unlike merely checking HEAD404 on separate unused keys.
        """
        with self.lock:
            require(self.poisoned is None and set(self.items)==set(ROLES),'complete known fixture model required')
            items=copy.deepcopy(self.items)
        uploads=[v for page in pages(recorder,'list_multipart_uploads',{'Bucket':self.bucket,'Prefix':self.key,'MaxUploads':1})
                 for v in page.get('Uploads',[])]
        require(all(v.get('Key')==self.key for v in uploads) and len({v['UploadId'] for v in uploads})==len(uploads) and
                {v['UploadId'] for v in uploads}=={v['uploadId'] for v in items.values() if v['state']=='pending'},
                'pending UploadId population differs; sibling upload affected')
        for item in items.values():
            params={'Bucket':self.bucket,'Key':self.key,'UploadId':item['uploadId'],'MaxParts':1}
            if item['state']=='pending':
                parts=[p for page in pages(recorder,'list_parts',params) for p in page.get('Parts',[])]
                require([{'number':p['PartNumber'],'etag':p['ETag'],'size':p['Size']} for p in parts]==
                        [{k:p[k] for k in ('number','etag','size')} for p in item['parts']], 'pending part order/ETag/length differs')
            else:
                response=recorder.call('list_parts',params,expect_http=404)
                require(response.get('Error',{}).get('Code')=='NoSuchUpload','terminal UploadId failed for wrong reason')
        committed=[v for v in items.values() if v['state']=='complete']
        latest=next(v for v in reversed(list(items.values())) if v['state']=='complete')
        listed=pages(recorder,'list_object_versions',{'Bucket':self.bucket,'Prefix':self.key,'MaxKeys':1})
        versions=[v for page in listed for v in page.get('Versions',[])]
        require(not any(page.get('DeleteMarkers') for page in listed) and len(versions)==len(committed) and
                all(v.get('Key')==self.key for v in versions) and
                {v['VersionId'] for v in versions}=={v['versionId'] for v in committed},
                'Complete duplicated/lost a version or pending upload became visible')
        for row in versions:
            item=next(v for v in committed if v['versionId']==row['VersionId'])
            require(row.get('IsLatest') is (item['role']==latest['role']) and row.get('ETag')==item['etag'] and
                    row.get('Size')==item['payload']['size'],'committed version order/ETag/size differs')
        for item,selector in [(v,{'VersionId':v['versionId']}) for v in committed]+[(latest,{})]:
            params={'Bucket':self.bucket,'Key':self.key,**selector}
            expected={'VersionId':item['versionId'],'ETag':item['etag'],'Metadata':item['metadata'],
                      'ContentLength':item['payload']['size'],'ContentType':'application/octet-stream'}
            head=recorder.call('head_object',params)
            require(all(head.get(k)==v for k,v in expected.items()),'HEAD changed committed/current object')
            recorder.call('get_object',params)
            observed=json.loads((recorder.output/f'{recorder.sequence:06d}-ack.json').read_text())['observation']
            require(observed.get('params')==params and observed.get('payload')==item['payload'] and
                    all(observed['response'].get(k)==v for k,v in expected.items()),'GET committed/current bytes or properties differ')
        with self.lock:require(self.poisoned is None and self.items==items,'multipart model changed during verification')
        return dict(committedVersions=len(committed),pendingUploads=len(uploads),sameKeySiblingIsolationVerified=True,
                    physicalRepairProved=False,overlapProved=False,casePassClaimed=False)
