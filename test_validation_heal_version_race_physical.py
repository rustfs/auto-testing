import copy
import hashlib
import unittest
import uuid
from types import SimpleNamespace

from functools import partial
from scripts.validation_heal import version_race_physical as actual
from scripts.validation_heal import version_race_fixture as fixture

INSPECTOR_SHA='a'*64
p=SimpleNamespace(INSPECTOR_SHA=INSPECTOR_SHA,
    key_state=partial(actual.key_state,inspector_sha=INSPECTOR_SHA),
    transition=partial(actual.transition,inspector_sha=INSPECTOR_SHA),
    file_index=actual.file_index)

BUCKET='rv-heal-version-physical'
KEY='objects-0256.bin'
CONTROL='objects-0000.bin'


def uid(n):return str(uuid.UUID(int=n))


def entry(n,*,key=KEY,latest=False,marker=False,payload='old'):
    value=dict(key=key,versionId=uid(n),isLatest=latest,deleteMarker=marker)
    if not marker:value['payload']={'size':263291,'sha256':hashlib.sha256(payload.encode()).hexdigest()}
    return value


def physical(entries):
    snapshot={'formats':['unchanged'],'files':[]};metadata={'inspectorSha256':p.INSPECTOR_SHA,'rows':[]}
    for key in sorted({v['key'] for v in entries}):
        rows=[v for v in entries if v['key']==key]
        for disk in range(4):
            digest=hashlib.sha256(repr(rows).encode()).hexdigest()
            snapshot['files'].append(dict(disk=disk,path=f'{BUCKET}/{key}/xl.meta',size=300,sha256=digest))
            versions=[]
            # Non-self-inverse permutation catches confusion between physical
            # drive order and coding index order.
            distribution=[3,1,4,2] if key==KEY else [1,2,3,4]
            for row in rows:
                version=dict(versionId=row['versionId'],isLatest=row['isLatest'],deleted=row['deleteMarker'],inline=None,
                             dataDir=None,size=0,parts=[],erasure={})
                if not row['deleteMarker']:
                    directory=uid(uuid.UUID(row['versionId']).int+1000);index=distribution[disk]
                    version.update(dataDir=directory,size=row['payload']['size'],parts=[dict(number=1,size=row['payload']['size'])],
                                   erasure=dict(index=index,data_blocks=2,parity_blocks=2,block_size=1048576,distribution=distribution))
                    digest=hashlib.sha256(f"{row['payload']['sha256']}/{index}".encode()).hexdigest()
                    snapshot['files'].append(dict(disk=disk,path=f'{BUCKET}/{key}/{directory}/part.1',size=131681,sha256=digest))
                versions.append(version)
            meta=next(v for v in snapshot['files'] if v['disk']==disk and v['path']==f'{BUCKET}/{key}/xl.meta')
            metadata['rows'].append(dict(disk=disk,key=key,metadataSha256=meta['sha256'],
                                        decoded=dict(bucket=BUCKET,key=key,versions=versions)))
    return snapshot,metadata


class VersionPhysicalTests(unittest.TestCase):
    def test_auto_testing_inspector_identity_is_mandatory_and_not_inferred_from_observation(self):
        entries=[entry(1,latest=True)];snapshot,metadata=physical(entries)
        with self.assertRaises(TypeError):actual.key_state(snapshot,metadata,BUCKET,KEY,entries)
        for digest in ('b'*64,'',True,None):
            with self.subTest(digest=digest),self.assertRaises(ValueError):
                actual.key_state(snapshot,metadata,BUCKET,KEY,entries,inspector_sha=digest)
        bad=copy.deepcopy(metadata);bad['inspectorSha256']='b'*64
        with self.assertRaises(ValueError):p.key_state(snapshot,bad,BUCKET,KEY,entries)




    def test_each_historical_version_requires_all_four_coding_indices(self):
        entries=[entry(1),entry(2,latest=True)];snapshot,metadata=physical(entries)
        proof=p.key_state(snapshot,metadata,BUCKET,KEY,entries)
        self.assertEqual(set(proof['versions']),{uid(1),uid(2)})
        self.assertEqual(set(proof['versions'][uid(1)]['coding']),{1,2,3,4})
        self.assertFalse(proof['casePassClaimed'])
        snapshot['files']=[v for v in snapshot['files'] if not (v['disk']==3 and uid(1001) in v['path'])]
        with self.assertRaisesRegex(ValueError,'lacks a physical shard'):p.key_state(snapshot,metadata,BUCKET,KEY,entries)

    def test_exact_marker_population_and_current_flags_on_every_drive(self):
        entries=[entry(1),entry(2,latest=True,marker=True)];snapshot,metadata=physical(entries)
        p.key_state(snapshot,metadata,BUCKET,KEY,entries)
        for kind in ('missing-marker','resurrect','latest','marker-data','duplicate','different-key','missing-decode'):
            bad=copy.deepcopy(metadata);rows=bad['rows'][0]['decoded']['versions']
            if kind=='missing-marker':rows.pop()
            if kind=='resurrect':rows[1]['deleted']=False
            if kind=='latest':rows[0]['isLatest']=True
            if kind=='marker-data':rows[1]['dataDir']=uid(999)
            if kind=='duplicate':rows.append(copy.deepcopy(rows[0]))
            if kind=='different-key':bad['rows'][0]['decoded']['key']='other'
            if kind=='missing-decode':bad['rows'].pop()
            with self.subTest(kind=kind),self.assertRaises(ValueError):p.key_state(snapshot,bad,BUCKET,KEY,entries)

    def test_metadata_sha_and_geometry_are_not_optional(self):
        entries=[entry(1,latest=True)];snapshot,metadata=physical(entries)
        for kind in ('sha','inspector','index','distribution','bool-index','inline','parts','generation','size','bool-disk'):
            bad=copy.deepcopy(metadata);v=bad['rows'][0]['decoded']['versions'][0]
            if kind=='sha':bad['rows'][0]['metadataSha256']='0'*64
            if kind=='inspector':bad['inspectorSha256']='0'*64
            if kind=='index':v['erasure']['index']=1
            if kind=='distribution':v['erasure']['distribution']=[3,1,1,2]
            if kind=='bool-index':v['erasure']['index']=True
            if kind=='inline':v['inline']={'length':123}
            if kind=='parts':v['parts'][0]['size']=1
            if kind=='generation':v['dataDir']=uid(999)
            if kind=='size':v['size']=1
            if kind=='bool-disk':bad['rows'][0]['disk']=False
            with self.subTest(kind=kind),self.assertRaises(ValueError):p.key_state(snapshot,bad,BUCKET,KEY,entries)

    def test_deleted_old_version_never_returns_but_gc_bytes_are_separate(self):
        old=[entry(1),entry(2,latest=True)];new=[entry(2,latest=True)]
        before,bm=physical(old);after,am=physical(new)
        orphan=next(v for v in before['files'] if uid(1001) in v['path']);after['files'].append(orphan)
        proof=p.transition(before,after,bm,am,BUCKET,KEY,old,new)
        self.assertEqual(proof['deletedVersions'],[uid(1)]);self.assertEqual(proof['unreferencedDeletedGenerationFiles'],[orphan])
        self.assertFalse(proof['deletedGenerationFilesAbsent']);self.assertFalse(proof['publicApiDeletionProved'])
        self.assertFalse(proof['garbageCollectorResponsibilityProved'])
        with self.assertRaises(ValueError):p.transition(before,before,bm,bm,BUCKET,KEY,old,new)

    def test_deleted_current_exposes_previous_and_marker_creation_preserves_bytes(self):
        cases=[([entry(1),entry(2,latest=True)],[entry(1,latest=True)]),
               ([entry(1,latest=True)],[entry(1),entry(2,latest=True,marker=True)]),
               ([entry(1),entry(2,latest=True,marker=True)],[entry(1,latest=True)])]
        for old,new in cases:
            before,bm=physical(old);after,am=physical(new)
            proof=p.transition(before,after,bm,am,BUCKET,KEY,old,new)
            self.assertEqual(proof['retainedVersions'],[uid(1)])

    def test_unversioned_delete_requires_four_absence_observations(self):
        old=[entry(1,latest=True)];before,bm=physical(old)
        # Candidate uses nil UUID for an unversioned metadata entry.
        old[0]['versionId']='null'
        for row in bm['rows']:row['decoded']['versions'][0]['versionId']=None
        am={'inspectorSha256':p.INSPECTOR_SHA,'rows':[dict(disk=d,key=KEY,absent=True) for d in range(4)]}
        proof=p.transition(before,{'formats':before['formats'],'files':[]},bm,am,BUCKET,KEY,old,[])
        self.assertEqual(proof['deletedVersions'],['null'])
        with self.assertRaises(ValueError):p.key_state(before,am,BUCKET,KEY,[])
        with self.assertRaises(ValueError):p.key_state({'files':[]},am,BUCKET,KEY,old)

    def test_new_version_matches_reference_by_coding_index_not_physical_disk(self):
        old=[entry(1,latest=True)];ref=entry(9,key=CONTROL,latest=True,payload='new')
        new=[entry(1),entry(2,latest=True,payload='new')]
        before,bm=physical(old+[ref]);after,am=physical(new)
        refs={uid(2):dict(key=CONTROL,versionId=uid(9),entry=ref)}
        proof=p.transition(before,after,bm,am,BUCKET,KEY,old,new,references=refs)
        self.assertEqual(proof['createdDataVersions'],[uid(2)])
        for kind in ('none','unknown','wrong-bytes','same-key','wrong-payload'):
            bad=copy.deepcopy(after);badrefs=copy.deepcopy(refs)
            if kind=='none':badrefs={}
            if kind=='unknown':badrefs[uid(3)]=badrefs[uid(2)]
            if kind=='wrong-bytes':next(v for v in bad['files'] if uid(1002) in v['path'])['sha256']='0'*64
            if kind=='same-key':badrefs[uid(2)]['key']=KEY
            if kind=='wrong-payload':badrefs[uid(2)]['entry']['payload']['size']=1
            with self.subTest(kind=kind),self.assertRaises(ValueError):p.transition(before,bad,bm,am,BUCKET,KEY,old,new,references=badrefs)

    def test_retained_corruption_and_unbound_physical_files_fail(self):
        entries=[entry(1,latest=True)];before,bm=physical(entries)
        for kind in ('duplicate','foreign-generation','bad-old-shard','bad-format'):
            after=copy.deepcopy(before)
            if kind=='duplicate':after['files'].append(copy.deepcopy(after['files'][0]))
            if kind=='foreign-generation':after['files'].append(dict(disk=0,path=f'{BUCKET}/{KEY}/{uid(8000)}/part.1',size=1,sha256='a'*64))
            if kind=='bad-old-shard':after['files'][1]['sha256']='0'*64
            if kind=='bad-format':after['formats']=[]
            with self.subTest(kind=kind),self.assertRaises(ValueError):p.transition(before,after,bm,bm,BUCKET,KEY,entries,entries)

    def test_repair_backup_only_accepts_exact_original_metadata_bytes(self):
        entries=[entry(1,latest=True)];before,bm=physical(entries);after=copy.deepcopy(before)
        original=next(v for v in before['files'] if v['disk']==3 and v['path'].endswith('/xl.meta'))
        backup={**original,'path':f'{BUCKET}/{KEY}/{uid(1001)}/xl.meta.bkp'};after['files'].append(backup)
        proof=p.transition(before,after,bm,bm,BUCKET,KEY,entries,entries)
        self.assertEqual(proof['verifiedMetadataBackups'],[backup])
        for bad in ({**backup,'sha256':'0'*64},{**backup,'path':f'{BUCKET}/{KEY}/{uid(9000)}/xl.meta.bkp'}):
            after['files'][-1]=bad
            with self.assertRaises(ValueError):p.transition(before,after,bm,bm,BUCKET,KEY,entries,entries)

    def test_additive_backup_accepts_each_ack_prefix_not_only_original_or_final_bytes(self):
        old=[entry(1,latest=True)];new=[entry(1),entry(2,payload='new'),entry(3,latest=True,payload='new')]
        ref=entry(9,key=CONTROL,latest=True,payload='new');before,bm=physical(old+[ref]);after,am=physical(new)
        refs={uid(i):dict(key=CONTROL,versionId=uid(9),entry=ref) for i in (2,3)}
        for count in range(3):
            with self.subTest(count=count):
                snapshot=copy.deepcopy(after);full=copy.deepcopy(am['rows'][3]['decoded'])
                full['versions']=full['versions'][:count+1]
                for v in full['versions']:v['isLatest']=v['versionId']==uid(count+1)
                backup=dict(disk=3,path=f'{BUCKET}/{KEY}/{uid(1001)}/xl.meta.bkp',size=321,sha256='a'*64)
                snapshot['files'].append(backup)
                observations=dict(inspectorSha256=p.INSPECTOR_SHA,rows=[{**backup,'decoded':full}])
                proof=p.transition(before,snapshot,bm,am,BUCKET,KEY,old,new,references=refs,
                                   append_order=[uid(2),uid(3)],backup_metadata=observations)
                self.assertEqual(proof['backupSemanticProofs'][0]['acknowledgedPutPrefix'],count)
                self.assertEqual(proof['verifiedMetadataBackups'],[backup])
                self.assertFalse(proof['backupSemanticProofs'][0]['liveAuthorityClaimed'])
                self.assertFalse(proof['backupSemanticProofs'][0]['unexposedMetadataVerified'])

    def test_changed_backup_rejects_unknown_or_corrupt_or_nonserial_views(self):
        old=[entry(1,latest=True)];new=[entry(1),entry(2,payload='new'),entry(3,latest=True,payload='new')]
        ref=entry(9,key=CONTROL,latest=True,payload='new');before,bm=physical(old+[ref]);after,am=physical(new)
        refs={uid(i):dict(key=CONTROL,versionId=uid(9),entry=ref) for i in (2,3)}
        backup=dict(disk=3,path=f'{BUCKET}/{KEY}/{uid(1001)}/xl.meta.bkp',size=321,sha256='a'*64)
        after['files'].append(backup)
        original=dict(inspectorSha256=p.INSPECTOR_SHA,rows=[{**backup,'decoded':copy.deepcopy(am['rows'][3]['decoded'])}])
        for kind in ('no-decode','sha','size','inspector','duplicate-row','missing-old','skipped-put','unknown-version',
                     'duplicate-version','latest','directory','parts','erasure','inline','unknown-field','bucket',
                     'order','duplicate-order','no-order','unbound-directory','live-shard'):
            snapshot=copy.deepcopy(after);obs=copy.deepcopy(original);order=[uid(2),uid(3)]
            value=obs['rows'][0]['decoded'];versions=value['versions']
            if kind=='no-decode':obs=None
            if kind=='sha':obs['rows'][0]['sha256']='b'*64
            if kind=='size':obs['rows'][0]['size']=322
            if kind=='inspector':obs['inspectorSha256']='b'*64
            if kind=='duplicate-row':obs['rows'].append(copy.deepcopy(obs['rows'][0]))
            if kind=='missing-old':versions.pop(0)
            if kind=='skipped-put':versions.pop(1)
            if kind=='unknown-version':versions[1]['versionId']=uid(99)
            if kind=='duplicate-version':versions.append(copy.deepcopy(versions[0]))
            if kind=='latest':versions[0]['isLatest']=True
            if kind=='directory':versions[0]['dataDir']=uid(99)
            if kind=='parts':versions[1]['parts'][0]['size']=1
            if kind=='erasure':versions[1]['erasure']['index']=1
            if kind=='inline':versions[1]['inline']={}
            if kind=='unknown-field':versions[1]['unbound']='hidden mutation'
            if kind=='bucket':value['bucket']='foreign'
            if kind=='order':order.reverse()
            if kind=='duplicate-order':order=[uid(2),uid(2)]
            if kind=='no-order':order=None
            if kind=='unbound-directory':snapshot['files'][-1]['path']=f'{BUCKET}/{KEY}/{uid(999)}/xl.meta.bkp'
            if kind=='live-shard':snapshot['files'][1]['sha256']='0'*64
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                p.transition(before,snapshot,bm,am,BUCKET,KEY,old,new,references=refs,append_order=order,backup_metadata=obs)



if __name__=='__main__':unittest.main()
