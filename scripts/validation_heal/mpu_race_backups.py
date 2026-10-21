"""Stopped G02/G05 backups, decoded from the same held file that was hashed."""
import hashlib
import json
import os
import re
import stat
import uuid

KEY = 'objects-0256.bin'
LIMIT = 1024**2


def require(value, message):
    if not value:
        raise ValueError(message)


def stable(info):
    return (info.st_mode,info.st_uid,info.st_gid,info.st_dev,info.st_ino,info.st_nlink,
            info.st_size,info.st_mtime_ns,info.st_ctime_ns)


def run(request,node):
    require(set(request)=={'runId','action','bucket','expectedBinarySha256','expectedInspectorSha256'}
            and request['action'] in {'mpu-race-backups','version-race-backups'}
            and re.fullmatch(r'rv-heal-[0-9a-f]{16}',request['bucket']),
            'G05 backup census requires the exact original owned key')
    key = KEY if request['action']=='mpu-race-backups' else 'objects/0256.bin'
    generations = 3 if request['action']=='mpu-race-backups' else 6
    state,roots,owner=node['owned'](request['runId'])
    require((owner.get('runtimeProfile')=='manual' or
             owner.get('runtimeProfile')=='frozen-race-legacy' and request['action']=='mpu-race-backups')
            and owner.get('bucket')==request['bucket']
            and owner.get('binarySha256')==request['expectedBinarySha256']
            and owner.get('inspectorSha256')==request['expectedInspectorSha256']
            and all(re.fullmatch(r'[0-9a-f]{64}',request[k]) for k in ('expectedBinarySha256','expectedInspectorSha256'))
            and len(roots)==len(node['MOUNTS'])==4,'G05 backup owner/binary/inspector differs')
    node['stopped'](owner)
    tool=node['checked'](state/'heal-meta-inspect',state)
    require(node['digest'](tool)==owner['inspectorSha256'] and node['digest'](state/'rustfs')==owner['binarySha256'],
            'G05 original candidate or inspector changed')
    fault=node['fault'];rows=[]
    for disk,(root,mount) in enumerate(zip(roots,node['MOUNTS'],strict=True)):
        require(root==mount/'heal-validation'/request['runId'],'G05 backup root escaped the original mount')
        with fault.directory(mount) as mount_fd, fault.parent(mount_fd,['heal-validation',request['runId'],'data',request['bucket'],*key.split('/')]) as key_fd:
            before_key=os.fstat(key_fd);names=sorted(os.listdir(key_fd))
            require(len(names)<=generations+1 and 'xl.meta' in names,'G02/G05 key population differs from its bounded version fixture')
            for name in names:
                if name=='xl.meta':continue
                require(str(uuid.UUID(name))==name and uuid.UUID(name).int!=0,'G05 backup parent is not an original generation UUID')
                with fault.parent(key_fd,[name]) as part_fd:
                    before_parent=os.fstat(part_fd)
                    try:fd=os.open('xl.meta.bkp',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=part_fd)
                    except FileNotFoundError:
                        require(stable(os.fstat(part_fd))==stable(before_parent),
                                'G05 backup population changed during absent-file census')
                        continue
                    try:
                        before=os.fstat(fd)
                        require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_size<=LIMIT
                                and fault.mount_id(fd)==fault.mount_id(part_fd),
                                'G05 backup is oversized, linked, special or on a different mount')
                        raw=os.read(fd,LIMIT+1)
                        require(len(raw)==before.st_size and stable(before)==stable(os.fstat(fd)),
                                'G05 backup changed while hashing')
                        os.lseek(fd,0,os.SEEK_SET)
                        # Linux parent-fd access keeps the decoder on the checked
                        # inode even if an attacker replaces its pathname.
                        observed=node['command'](str(tool),f'/proc/{os.getpid()}/fd/{fd}',request['bucket'],key,check=False)
                        require(observed.returncode==0 and len(observed.stdout)<=LIMIT,
                                'G05 backup metadata decoder failed or exceeded its bound')
                        decoded=json.loads(observed.stdout)
                        require(isinstance(decoded,dict) and decoded.get('bucket')==request['bucket']
                                and decoded.get('key')==key and decoded.get('physicalBitrotVerified') is False,
                                'G05 backup decoder identity or proof role differs')
                        require(stable(os.fstat(fd))==stable(before)
                                and stable(os.stat('xl.meta.bkp',dir_fd=part_fd,follow_symlinks=False))==stable(before),
                                'G05 backup inode changed during descriptor-bound decode')
                        rows.append({'disk':disk,'path':f"{request['bucket']}/{key}/{name}/xl.meta.bkp",
                                     'size':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'metadata':decoded})
                    finally:os.close(fd)
                    require(stable(os.fstat(part_fd))==stable(before_parent),'G05 backup generation changed during census')
            require(stable(os.fstat(key_fd))==stable(before_key) and sorted(os.listdir(key_fd))==names,
                    'G05 committed key population changed during backup census')
    node['stopped'](owner)
    require(node['owned'](request['runId'])==(state,roots,owner)
            and node['digest'](tool)==owner['inspectorSha256'] and node['digest'](state/'rustfs')==owner['binarySha256'],
            'G05 original ownership or tools changed during backup census')
    return {'hostname':owner['hostname'],'rows':rows,'inspectorSha256':owner['inspectorSha256'],
            'binarySha256':owner['binarySha256'],'quiescent':True,'readOnly':True,
            'sameHeldFileHashedAndDecoded':True,'rawContentsExported':False,'liveAuthorityClaimed':False,
            'physicalRepairProved':False,'canonicalCasePassClaimed':False}


def version_map(metadata):
    versions=metadata.get('versions')
    require(isinstance(versions,list) and 1<=len(versions)<=6,'G02/G05 backup needs bounded acknowledged data versions')
    result={}
    for version in versions:
        vid=version.get('versionId')
        require(isinstance(vid,str) and str(uuid.UUID(vid))==vid and uuid.UUID(vid).int!=0
                and vid not in result and version.get('deleted') is False and type(version.get('isLatest')) is bool,
                'G05 backup version identity, kind or latest flag differs')
        result[vid]=version
    require(sum(v['isLatest'] for v in result.values())==1,'G05 backup needs exactly one latest data version')
    return result


def verify(before,after,observations,nodes,bucket,old_version,new_version,binary_sha,inspector_sha):
    return verify_versions(before,after,observations,nodes,bucket,KEY,[old_version],[new_version],binary_sha,inspector_sha)


def verify_versions(before,after,observations,nodes,bucket,key,old_versions,new_versions,binary_sha,inspector_sha):
    """Adapt frozen additive_backups to H1/H2; backups never supply live authority."""
    require(re.fullmatch(r'rv-heal-[0-9a-f]{16}',bucket)
            and all(isinstance(v,str) and re.fullmatch(r'[0-9a-f]{64}',v) for v in (binary_sha,inspector_sha))
            and len(nodes) in (1,4) and len(set(nodes))==len(nodes)
            and set(before)==set(after)==set(observations)==set(nodes)
            and (key==KEY and len(old_versions)==len(new_versions)==1
                 or key=='objects/0256.bin' and len(old_versions)==len(new_versions)==3)
            and len(set(old_versions+new_versions))==len(old_versions)+len(new_versions),
            'G05 backup proof omitted or mixed an original host/version')
    proofs=[];prefix=f'{bucket}/{key}/'
    for node in nodes:
        evidence=observations[node]
        require(evidence.get('quiescent') is True and evidence.get('readOnly') is True
                and evidence.get('sameHeldFileHashedAndDecoded') is True
                and evidence.get('binarySha256')==before[node].get('binarySha256')==after[node].get('binarySha256')==binary_sha
                and evidence.get('inspectorSha256')==inspector_sha
                and evidence.get('hostname')==before[node].get('hostname')==after[node].get('hostname')
                and before[node].get('quiescent') is True and after[node].get('quiescent') is True
                and evidence.get('liveAuthorityClaimed') is False and evidence.get('rawContentsExported') is False,
                'G05 backup proof lost its candidate/inspector/stopped binding')
        def index(scene):
            rows=[r for r in scene[node]['files'] if r['path'].startswith(prefix)]
            values={(r['disk'],r['path']):r for r in rows}
            require(len(values)==len(rows),'G05 snapshot contains duplicate physical backup identities')
            return values
        previous,current=index(before),index(after)
        primary={}
        for scene in (previous,current):
            meta=[r for (disk,path),r in scene.items() if path==prefix+'xl.meta']
            require(len(meta)==4 and all(type(r['disk']) is int for r in meta)
                    and {r['disk'] for r in meta}==set(range(4)), 'G05 backup scope lacks every original primary member')
            require(all(r.get('metadata',{}).get('bucket')==bucket and r['metadata'].get('key')==key
                        and r['metadata'].get('physicalBitrotVerified') is False for r in meta),
                    'G05 primary decoder scope or proof role differs')
        for disk in range(4):
            old=version_map(previous[disk,prefix+'xl.meta']['metadata'])
            new=version_map(current[disk,prefix+'xl.meta']['metadata'])
            require(set(old)==set(old_versions) and set(new)==set(old_versions+new_versions)
                    and old[old_versions[-1]]['isLatest'] and new[new_versions[-1]]['isLatest'],
                    'G05 primary versions differ from original and Complete ACK order')
            primary[disk]=(old,new)
        candidates={k:v for k,v in current.items() if k[1].endswith('/xl.meta.bkp')}
        rows=evidence['rows'];require(isinstance(rows,list) and len(rows)<=4*len(old_versions),'G05 backup decode rows exceed original generations')
        decoded={(r['disk'],r['path']):r for r in rows}
        require(len(decoded)==len(rows) and set(decoded)==set(candidates),'G05 backup decode population differs from snapshot')
        for (disk,path),row in candidates.items():
            require(type(disk) is int and disk in range(4),'G05 backup has a foreign local drive')
            final=current[disk,prefix+'xl.meta']
            old,new=primary[disk]
            require(path in {prefix+old[vid]['dataDir']+'/xl.meta.bkp' for vid in old_versions},
                    'G05 backup directory is not bound to the original committed generation')
            witness=decoded[disk,path]
            require(all(witness[k]==row[k] for k in ('disk','path','size','sha256')),
                    'G05 backup decoder does not describe its exact snapshotted bytes')
            value=witness['metadata'];versions=version_map(value)
            require({k:v for k,v in value.items() if k!='versions'}==
                    {k:v for k,v in final['metadata'].items() if k!='versions'},
                    'G05 backup decoder schema/object identity differs')
            count=len(versions)-len(old_versions)
            require(0<=count<=len(new_versions) and set(versions)==set(old_versions+new_versions[:count]),
                    'G05 backup is not a contiguous prefix of acknowledged Complete versions')
            latest=old_versions[-1] if count==0 else new_versions[count-1]
            for vid,version in versions.items():
                reference=old[vid] if vid in old else new[vid]
                require(version['isLatest'] is (vid==latest)
                        and {k:v for k,v in version.items() if k!='isLatest'}==
                        {k:v for k,v in reference.items() if k!='isLatest'},
                        'G05 backup version descriptor changed outside the acknowledged prefix')
            proofs.append({'node':node,'disk':disk,'path':path,'sha256':row['sha256'],'size':row['size'],
                           'acknowledgedCompletePrefix':count,'decoderFieldsVerified':True,
                           'unexposedMetadataVerified':False,'liveAuthorityClaimed':False})
    return {'proofs':proofs,'allSnapshottedBackupsVerified':True,'liveAuthorityClaimed':False,
            'physicalRepairProved':False,'canonicalCasePassClaimed':False}
