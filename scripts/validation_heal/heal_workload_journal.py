#!/usr/bin/env python3
"""Foreground mutation ordering and per-item DELETE ACKs; no repair verdict."""
from contextlib import contextmanager
import threading
import time

try:
    from .heal_probe import require
except ImportError:
    from heal_probe import require


class AdmissionExpired(TimeoutError):
    """No call was sent: the admission deadline expired while waiting."""


def mutation_resources(operation,params):
    if operation.startswith(('get_','head_','list_')):return ()
    bucket=params['Bucket']
    if operation=='delete_objects':
        keys=[v['Key'] for v in params['Delete']['Objects']]
        return tuple(sorted({(bucket,k) for k in keys}))
    if 'Key' in params:
        require(isinstance(params['Key'],str) and params['Key'],'empty/invalid mutation key')
        return ((bucket,params['Key']),)
    # Bucket configuration changes exclude all foreground mutations in that
    # bucket, while different keys and buckets can otherwise run concurrently.
    return ((bucket,None),)


def conflicts(left,right):
    return any(a==b and (x is None or y is None or x==y) for a,x in left for b,y in right)


class MutationOrder:
    def __init__(self):
        self.condition=threading.Condition();self.active={};self.poisoned=None;self.next_ticket=0

    @contextmanager
    def hold(self,operation,params,deadline=None):
        resources=mutation_resources(operation,params);ticket=None
        if resources:
            with self.condition:
                while any(conflicts(resources,r) for r in self.active.values()) and self.poisoned is None:
                    remaining=None if deadline is None else deadline-time.monotonic()
                    if remaining is not None and remaining<=0:raise AdmissionExpired('foreground admission deadline expired')
                    self.condition.wait(timeout=remaining)
                require(self.poisoned is None,'foreground mutations halted after prior failure/unknown result')
                if deadline is not None and time.monotonic()>=deadline:raise AdmissionExpired('foreground admission deadline expired')
                ticket=self.next_ticket;self.next_ticket+=1;self.active[ticket]=resources
        elif deadline is not None and time.monotonic()>=deadline:raise AdmissionExpired('foreground admission deadline expired')
        try:yield resources
        finally:
            if ticket is not None:
                with self.condition:
                    del self.active[ticket];self.condition.notify_all()

    def poison(self,reason):
        with self.condition:self.poisoned=self.poisoned or reason;self.condition.notify_all()


def validate_batch_request(params):
    delete=params['Delete'];items=delete['Objects']
    require(delete.get('Quiet',False) is False,'DELETE batch requires explicit per-item receipts; Quiet is unsupported')
    identities=[(v['Key'],v.get('VersionId')) for v in items]
    require(all(isinstance(k,str) and k and (v is None or isinstance(v,str) and v) for k,v in identities),'invalid DELETE batch identity')
    require(len(identities)==len(set(identities)),'duplicate DELETE batch identity')


def batch_receipts(params,response):
    """HTTP 200 is not an acknowledgement of every member of a DELETE batch."""
    validate_batch_request(params);items=params['Delete']['Objects'];out=[];seen=set();errors=[]
    wanted={(v['Key'],v.get('VersionId')) for v in items};by_identity={}
    for name,state in (('Deleted','ACK'),('Errors','REJECTED')):
        for row in response.get(name,[]):
            key=(row.get('Key'),row.get('VersionId'))
            if key not in wanted or key in seen:errors.append('unbound/duplicate DELETE result: '+repr(key))
            seen.add(key);by_identity.setdefault(key,[]).append({'state':state,'receipt':row})
    for item in items:
        matches=by_identity.get((item['Key'],item.get('VersionId')),[])
        value=matches[0] if len(matches)==1 else {'state':'UNKNOWN','receipts':matches}
        out.append({'request':item,**value})
        if value['state']!='ACK':errors.append('DELETE member not acknowledged: '+repr(item))
    return {'items':out,'errors':errors}
