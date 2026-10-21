#!/usr/bin/env python3
"""Version-addressed H1 physical oracle; inputs must be archived before S3 reads.

No metadata majority selection: every drive must agree with the ACK ledger.
Unreferenced bytes of explicitly deleted old generations are reported, not
called resurrection or silently mistaken for live version redundancy.
"""
import copy
import re
import uuid

try:
    from .heal_probe import require
except ImportError:
    from heal_probe import require

# Frozen validation oracle at 524eb5b32c3236497fdd8a9271df0494fa3d9974.
# The caller binds the inspector to its immutable auto-testing tool manifest.

NIL='00000000-0000-0000-0000-000000000000'


def canonical_version(value):
    if value is None or value==NIL:return 'null'
    require(isinstance(value,str) and str(uuid.UUID(value))==value and value!=NIL,
            'candidate metadata version must be a canonical UUID or nil')
    return value


def file_index(snapshot):
    rows=snapshot['files'];indexed={}
    require(isinstance(rows,list) and len(rows)<=257*12,'bounded physical snapshot required')
    for row in rows:
        require(type(row['disk']) is int and row['disk'] in range(4),'invalid H1 physical disk')
        require(isinstance(row['path'],str) and not row['path'].startswith('/') and
                all(v not in ('','.','..') for v in row['path'].split('/')),'unsafe physical path')
        require(type(row['size']) is int and row['size']>=0 and
                re.fullmatch('[0-9a-f]{64}',row['sha256']) is not None,'invalid file size or digest')
        identity=(row['disk'],row['path'])
        require(identity not in indexed,'duplicate physical member');indexed[identity]=row
    return indexed


def key_state(snapshot,metadata,bucket,key,expected,*,inspector_sha,allowed_orphans=(),allowed_backups=None,allow_multipart=False):
    """Bind the exact expected versions to 4 xl.meta decodes and coding indices.

    `expected` is an immutable ACK-ledger slice, not a version list discovered
    from this same snapshot. `allowed_orphans` are exact old dataDir UUIDs from
    the verified baseline, only for versions with successful deletion ACKs.
    """
    require(isinstance(inspector_sha,str) and re.fullmatch('[0-9a-f]{64}',inspector_sha) is not None
            and metadata.get('inspectorSha256')==inspector_sha,'candidate inspector digest mismatch')
    require(isinstance(key,str) and re.fullmatch(r'objects-[0-9]{4}\.bin',key) is not None,
            'registered flat fixture key required')
    require(isinstance(bucket,str) and re.fullmatch('[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',bucket) is not None,
            'invalid fixture bucket')
    require(all(v.get('key')==key for v in expected),'foreign key in expected version slice')
    wanted={v['versionId']:v for v in expected}
    require(len(wanted)==len(expected) and len(wanted)<=8,'duplicate or excessive expected versions')
    require(not wanted or sum(v.get('isLatest') is True for v in wanted.values())==1,
            'expected ACK ledger must have exactly one latest entry')
    rows=[v for v in metadata['rows'] if v['key']==key]
    require(len(rows)==4 and all(type(v['disk']) is int for v in rows) and
            {v['disk'] for v in rows}==set(range(4)),'four distinct metadata observations required')
    files=file_index(snapshot);prefix=f'{bucket}/{key}/'
    members={i:v for i,v in files.items() if i[1].startswith(prefix)}
    used=set();versions={};directories=set();multipart_sidecars=[]
    for row in rows:
        disk=row['disk'];meta_id=(disk,prefix+'xl.meta')
        if row.get('absent') is True:
            require(not wanted and meta_id not in files and 'decoded' not in row,
                    'metadata absence conflicts with live ACK version or snapshot')
            continue
        require(row.get('absent') in (None,False) and meta_id in files,'metadata file/observation missing')
        require(files[meta_id]['sha256']==row['metadataSha256'],'metadata decode not bound to snapshot SHA')
        used.add(meta_id);decoded=row['decoded']
        require(decoded['bucket']==bucket and decoded['key']==key,'decoded object identity differs')
        decoded_versions=decoded['versions']
        by_id={canonical_version(v['versionId']):v for v in decoded_versions}
        require(len(by_id)==len(decoded_versions) and set(by_id)==set(wanted),
                'physical version/marker population differs from ACK ledger')
        require(bool(wanted),'empty xl.meta is not authoritative deletion proof')
        for vid,v in by_id.items():
            entry=wanted[vid]
            require(type(entry.get('deleteMarker')) is bool and type(entry.get('isLatest')) is bool,
                    'ACK marker/latest must be booleans')
            require(v['deleted'] is entry['deleteMarker'] and v['isLatest'] is entry['isLatest'],
                    'physical latest/marker differs from serial ACK order')
            item=versions.setdefault(vid,dict(deleteMarker=entry['deleteMarker'],isLatest=entry['isLatest'],
                                              coding={},layouts=[],dataDirs=[],descriptors=[]))
            if v['deleted']:
                require(v.get('dataDir') in (None,NIL) and not v.get('parts') and v.get('inline') is None,
                        'delete marker unexpectedly references data')
                continue
            directory=v['dataDir']
            require(isinstance(directory,str) and str(uuid.UUID(directory))==directory and directory!=NIL,
                    'invalid external data directory')
            require(v.get('inline') is None and v['size']==entry['payload']['size'],
                    'physical external object size differs from ACK bytes')
            parts=v['parts'];wanted_parts=entry.get('multipartParts') if allow_multipart else None
            if wanted_parts is not None:
                require(len(wanted_parts)==3 and [p['number'] for p in wanted_parts]==[1,2,3]
                        and sum(p['size'] for p in wanted_parts)==entry['payload']['size'],
                        'three ordered ACK multipart descriptors required')
                require([{k:p.get(k) for k in ('number','size')} for p in parts]==wanted_parts,
                        'multipart metadata differs from ACK part sizes/order')
            else:
                require(len(parts)==1 and type(parts[0]['number']) is int and parts[0]['number']==1
                        and parts[0]['size']==entry['payload']['size'],'single-part external fixture required')
            ec=v['erasure'];index=ec['index']
            require(type(index) is int and index in range(1,5) and index not in item['coding'],
                    'invalid or duplicate erasure index')
            require(type(ec['data_blocks']) is int and ec['data_blocks']==2 and
                    type(ec['parity_blocks']) is int and ec['parity_blocks']==2 and
                    type(ec['block_size']) is int and ec['block_size']>0 and
                    len(ec['distribution'])==4 and all(type(i) is int for i in ec['distribution']) and
                    set(ec['distribution'])==set(range(1,5)),'invalid H1 EC2+2 geometry')
            require(ec['distribution'][disk]==index,'erasure index differs from disk distribution')
            coding={}
            for descriptor in parts:
                part_id=(disk,prefix+directory+'/part.'+str(descriptor['number']))
                require(part_id in files,'live version lacks a physical shard');used.add(part_id)
                part=files[part_id];require(part['size']>0,'external shard cannot be empty')
                coding[descriptor['number']]={'sha256':part['sha256'],'size':part['size']}
                if wanted_parts is not None:
                    side_id=(disk,part_id[1]+'.meta')
                    # Committed authority is xl.meta + all part.N shards.
                    # part.N.meta belongs to the upload protocol and may be
                    # reclaimed after commit or omitted by Heal reconstruction.
                    # Keep any remaining auxiliary bytes separate from coding.
                    if side_id in files:
                        require(files[side_id]['size']>0,'empty multipart sidecar')
                        used.add(side_id);multipart_sidecars.append(copy.deepcopy(files[side_id]))
            item['coding'][index]=coding if wanted_parts is not None else coding[1]
            item['layouts'].append({k:copy.deepcopy(ec[k]) for k in ('data_blocks','parity_blocks','block_size','distribution')})
            item['dataDirs'].append(directory)
            item['descriptors'].append({k:copy.deepcopy(v[k]) for k in ('size','parts')})
    for item in versions.values():
        if item['deleteMarker']:continue
        require(set(item['coding'])==set(range(1,5)) and len(set(item['dataDirs']))==1 and
                all(v==item['layouts'][0] for v in item['layouts']) and
                all(v==item['descriptors'][0] for v in item['descriptors']),
                'live version generation/layout/parts differ across drives')
        item['dataDir']=item.pop('dataDirs')[0];item['layout']=item.pop('layouts')[0]
        item['descriptor']=item.pop('descriptors')[0]
        require(item['dataDir'] not in directories,'distinct live versions share a fixture data directory')
        directories.add(item['dataDir'])
    allowed=set(allowed_orphans)
    require(not directories.intersection(allowed),'deleted generation became a live version')
    orphans=[];backups=[]
    for identity,row in members.items():
        if identity in used:continue
        if identity in (allowed_backups or {}):
            original=allowed_backups[identity]
            require(all(row[k]==original[k] for k in ('size','sha256')),'metadata backup differs from exact pre-fault bytes')
            backups.append(copy.deepcopy(row));continue
        relative=identity[1][len(prefix):].split('/')
        require(len(relative)==2 and relative[0] in allowed and relative[1]=='part.1',
                'unregistered/orphan physical member not bound to a deleted generation')
        orphans.append(copy.deepcopy(row))
    return dict(versions=versions,unreferencedDeletedGenerationFiles=orphans,verifiedMetadataBackups=backups,allLiveVersionsFourShards=True,
                observedMultipartSidecars=multipart_sidecars,
                casePassClaimed=False,deletedGenerationFilesAbsent=not orphans,garbageCollectorResponsibilityProved=False)


def additive_backups(after,before_metadata,after_metadata,bucket,key,before_entries,after_entries,order,allowed,observations,*,inspector_sha):
    """Validate exposed decoder semantics, not arbitrary bytes or live authority.

    Only serial additive PUTs are admitted. A rollback view may represent any
    ACK prefix; every old descriptor comes from the pre-race decode, every new
    one from the final decode which transition also binds to independent data.
    """
    old={v['versionId']:v for v in before_entries};new={v['versionId']:v for v in after_entries}
    require(isinstance(order,list) and 1<=len(order)<=5 and len(order)==len(set(order)) and
            set(old)<set(new) and set(order)==set(new)-set(old) and
            all(not v['deleteMarker'] for v in [*before_entries,*after_entries]),'serial additive data-only ACK order required')
    require(all({k:v for k,v in old[vid].items() if k!='isLatest'}==
                {k:v for k,v in new[vid].items() if k!='isLatest'} for vid in old) and
            [v['versionId'] for v in after_entries if v['isLatest']]==[order[-1]],'additive ACK population/order differs')
    files=file_index(after);prefix=f'{bucket}/{key}/'
    candidates={i:v for i,v in files.items() if i[1].startswith(prefix) and i[1].endswith('/xl.meta.bkp')}
    changed={i:v for i,v in candidates.items() if i not in allowed or
             any(v[k]!=allowed[i][k] for k in ('size','sha256'))}
    require(set(candidates)<=set(allowed),'backup directory not bound to an original generation')
    if not changed:return allowed,[]
    require(isinstance(observations,dict) and observations.get('inspectorSha256')==inspector_sha,
            'changed backup needs fixed-inspector snapshot-bound decoding')
    rows=[v for v in observations['rows'] if v['path'].startswith(prefix)]
    decoded={(v['disk'],v['path']):v for v in rows}
    require(len(decoded)==len(rows) and set(decoded)==set(candidates),'backup decode population differs from snapshot')
    result=copy.deepcopy(allowed);proofs=[]
    for identity,current in changed.items():
        witness=decoded[identity]
        require(all(witness[k]==current[k] for k in ('disk','path','size','sha256')),
                'backup decode not bound to exact snapshot bytes')
        before=next(v['decoded'] for v in before_metadata['rows'] if v['key']==key and v['disk']==identity[0])
        final=next(v['decoded'] for v in after_metadata['rows'] if v['key']==key and v['disk']==identity[0])
        value=witness['decoded']
        require({k:v for k,v in value.items() if k!='versions'}==
                {k:v for k,v in final.items() if k!='versions'},'backup decoder object/schema identity differs')
        versions=value['versions'];by_id={canonical_version(v['versionId']):v for v in versions}
        require(len(by_id)==len(versions),'duplicate backup version')
        count=len(by_id)-len(old)
        require(0<=count<=len(order) and set(by_id)==set(old)|set(order[:count]),
                'backup versions do not match a contiguous serial ACK prefix')
        latest=order[count-1] if count else next(v['versionId'] for v in before_entries if v['isLatest'])
        before_ids={canonical_version(v['versionId']):v for v in before['versions']}
        final_ids={canonical_version(v['versionId']):v for v in final['versions']}
        for vid,version in by_id.items():
            reference=(before_ids if vid in old else final_ids)[vid]
            require(version.get('isLatest') is (vid==latest) and
                    {k:v for k,v in version.items() if k!='isLatest'}==
                    {k:v for k,v in reference.items() if k!='isLatest'},'backup version descriptor or latest differs from ACK-bound view')
        result[identity]=current
        proofs.append(dict(disk=identity[0],path=identity[1],sha256=current['sha256'],size=current['size'],
                           acknowledgedPutPrefix=count,versionIds=sorted(by_id),
                           decoderFieldsVerified=True,unexposedMetadataVerified=False,liveAuthorityClaimed=False))
    return result,proofs


def transition(before,after,before_metadata,after_metadata,bucket,key,before_entries,after_entries,*,inspector_sha,references=None,
               append_order=None,backup_metadata=None):
    """Retained versions stay byte-identical; new ones match immutable references.

    references maps each new VersionId to {key, versionId, entry}; the referenced
    control is decoded and hashed in `before`, not trusted from its current GET.
    Marker creation/deletion changes only the ACK-derived version population.
    """
    require(before['formats']==after['formats'],'format changed during version race')
    old=key_state(before,before_metadata,bucket,key,before_entries,inspector_sha=inspector_sha)
    old_ids={v['versionId'] for v in before_entries};new_ids={v['versionId'] for v in after_entries}
    deleted=old_ids-new_ids
    allowed=[old['versions'][vid]['dataDir'] for vid in deleted if not old['versions'][vid]['deleteMarker']]
    originals=file_index(before)
    backups={(disk,f'{bucket}/{key}/{v["dataDir"]}/xl.meta.bkp'):originals[(disk,f'{bucket}/{key}/xl.meta')]
             for v in old['versions'].values() if not v['deleteMarker'] for disk in range(4)}
    backup_proofs=[]
    if append_order is not None:
        backups,backup_proofs=additive_backups(after,before_metadata,after_metadata,bucket,key,before_entries,
                                             after_entries,append_order,backups,backup_metadata,inspector_sha=inspector_sha)
    new=key_state(after,after_metadata,bucket,key,after_entries,inspector_sha=inspector_sha,allowed_orphans=allowed,allowed_backups=backups)
    refs=references or {};needed={vid for vid in new_ids-old_ids if not new['versions'][vid]['deleteMarker']}
    require(set(refs)==needed,'exact independent references required for every new data version')
    retained=[];created=[]
    for vid,item in new['versions'].items():
        if vid in old_ids:
            previous=old['versions'][vid]
            require({k:v for k,v in previous.items() if k!='isLatest'}==
                    {k:v for k,v in item.items() if k!='isLatest'},'retained version bytes/layout/generation changed')
            retained.append(vid)
        elif not item['deleteMarker']:
            ref=refs[vid]
            require(ref['key']!=key and ref['entry']['versionId']==ref['versionId'],
                    'new version reference must be a separate exact baseline control')
            expected=next(v for v in after_entries if v['versionId']==vid)
            require(expected['payload']==ref['entry']['payload'],'new version ACK bytes differ from reference payload')
            reference=key_state(before,before_metadata,bucket,ref['key'],[ref['entry']],inspector_sha=inspector_sha)['versions'][ref['versionId']]
            require(item['coding']==reference['coding'] and
                    all(item['layout'][k]==reference['layout'][k] for k in ('data_blocks','parity_blocks','block_size')),
                    'new version coding-index bytes differ from immutable reference')
            require(item['dataDir'] not in {v.get('dataDir') for v in old['versions'].values()},
                    'new version reused an old generation')
            created.append(vid)
    return dict(retainedVersions=sorted(retained),createdDataVersions=sorted(created),deletedVersions=sorted(deleted),
                unreferencedDeletedGenerationFiles=new['unreferencedDeletedGenerationFiles'],
                verifiedMetadataBackups=new['verifiedMetadataBackups'],
                backupSemanticProofs=backup_proofs,
                deletedGenerationFilesAbsent=new['deletedGenerationFilesAbsent'],garbageCollectorResponsibilityProved=False,
                allLiveVersionsFourShards=True,exactAckVersionPopulation=True,casePassClaimed=False,
                overlapProved=False,publicApiDeletionProved=False)
