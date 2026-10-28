"""Actual file/descriptor checks with a mock decoder; no live metadata proof."""
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch
import uuid

from scripts.validation_heal import fleet_node, heal_fault, mpu_race_backups as backup
from scripts.validation_heal.healthy import Fleet


class MultipartBackupTests(unittest.TestCase):
    def frames(self,nodes,prefix_count):
        bucket='rv-heal-0123456789abcdef';old,new=str(uuid.UUID(int=1)),str(uuid.UUID(int=2));binary='a'*64;inspector='b'*64
        before,after,evidence={},{},{}
        for host_number,host in enumerate(nodes):
            common={'hostname':'host-'+host,'binarySha256':binary,'quiescent':True}
            before[host]={**common,'files':[]};after[host]={**common,'files':[]}
            evidence[host]={**common,'readOnly':True,'sameHeldFileHashedAndDecoded':True,'rawContentsExported':False,
                            'liveAuthorityClaimed':False,'inspectorSha256':inspector,'rows':[]}
            for disk in range(4):
                v0={'versionId':old,'dataDir':str(uuid.UUID(int=100)),'isLatest':True,'deleted':False,
                    'codingIndex':host_number*4+disk+1,'parts':[{'number':n,'digest':'old-'+str(n)} for n in (1,2,3)]}
                v1={**v0,'versionId':new,'dataDir':str(uuid.UUID(int=200)),'parts':[{'number':n,'digest':'new-'+str(n)} for n in (1,2,3)]}
                m0={'bucket':bucket,'key':backup.KEY,'physicalBitrotVerified':False,'versions':[copy.deepcopy(v0)]}
                m1={**m0,'versions':[{**v0,'isLatest':False},v1]}
                before[host]['files'].append({'disk':disk,'path':f'{bucket}/{backup.KEY}/xl.meta','size':100,'sha256':'0'*64,'metadata':m0})
                after[host]['files'].append({'disk':disk,'path':f'{bucket}/{backup.KEY}/xl.meta','size':200,'sha256':'1'*64,'metadata':m1})
                row={'disk':disk,'path':f'{bucket}/{backup.KEY}/{v0["dataDir"]}/xl.meta.bkp','size':100+prefix_count,
                     'sha256':str(prefix_count+2)*64}
                after[host]['files'].append(copy.deepcopy(row))
                evidence[host]['rows'].append({**row,'metadata':copy.deepcopy(m0 if prefix_count==0 else m1)})
        return before,after,evidence,bucket,old,new,binary,inspector

    def test_frozen_backup_oracle_accepts_only_original_or_complete_ack_prefix_in_h1_h2(self):
        for nodes in (['vm000'],['vm000','vm001','vm002','vm003']):
            for count in (0,1):
                with self.subTest(nodes=nodes,prefix=count):
                    before,after,evidence,bucket,old,new,binary,inspector=self.frames(nodes,count)
                    proof=backup.verify(before,after,evidence,nodes,bucket,old,new,binary,inspector)
                    self.assertEqual(len(proof['proofs']),4*len(nodes))
                    self.assertEqual({r['acknowledgedCompletePrefix'] for r in proof['proofs']},{count})
                    self.assertFalse(proof['liveAuthorityClaimed'] or proof['physicalRepairProved'] or proof['canonicalCasePassClaimed'])
                    self.assertTrue(all(not r['unexposedMetadataVerified'] for r in proof['proofs']))

    def test_backup_oracle_rejects_drift_foreign_generation_nonprefix_and_incomplete_scope(self):
        for fault in ('sha','generation','version','latest','descriptor','omitted','duplicate','hostname','primary','bool-disk','binary'):
            with self.subTest(fault=fault):
                nodes=['vm000'];before,after,evidence,bucket,old,new,binary,inspector=self.frames(nodes,1)
                row=evidence['vm000']['rows'][0]
                if fault=='sha':row['sha256']='f'*64
                elif fault=='generation':
                    row['path']=row['path'].replace(str(uuid.UUID(int=100)),str(uuid.UUID(int=200)))
                    after['vm000']['files'][1]['path']=row['path']
                elif fault=='version':row['metadata']['versions'][1]['versionId']=str(uuid.UUID(int=3))
                elif fault=='latest':row['metadata']['versions'][0]['isLatest']=True
                elif fault=='descriptor':row['metadata']['versions'][0]['parts'][0]['digest']='wrong'
                elif fault=='omitted':evidence['vm000']['rows'].pop()
                elif fault=='duplicate':evidence['vm000']['rows'].append(copy.deepcopy(row))
                elif fault=='hostname':evidence['vm000']['hostname']='foreign'
                elif fault=='primary':after['vm000']['files']=[r for r in after['vm000']['files'] if not (r['disk']==2 and r['path'].endswith('/xl.meta'))]
                elif fault=='bool-disk':before['vm000']['files'][0]['disk']=False
                else:binary=None
                with self.assertRaises(ValueError):backup.verify(before,after,evidence,nodes,bucket,old,new,binary,inspector)

    def fixture(self,folder):
        folder=Path(folder).resolve();state=folder/'state';state.mkdir()
        (state/'rustfs').write_bytes(b'owned candidate');(state/'heal-meta-inspect').write_bytes(b'owned inspector')
        (state/'credentials.env').write_bytes(b'private mock credential; never opened by collector')
        mounts=tuple(folder/f'mount-{i}' for i in range(4));run='gh-12345-1-h1';bucket='rv-heal-0123456789abcdef'
        roots=tuple(m/'heal-validation'/run for m in mounts);files=[]
        for i,root in enumerate(roots):
            key=root/'data'/bucket/backup.KEY;key.mkdir(parents=True);(key/'xl.meta').write_bytes(b'live current metadata not read here')
            generation=key/str(uuid.UUID(int=100));generation.mkdir()
            path=generation/'xl.meta.bkp';path.write_bytes(f'actual original backup on disk{i}'.encode());files.append(path)
            (generation/'part.1').write_bytes(b'payload part not read by collector')
        owner={'runtimeProfile':'manual','bucket':bucket,'hostname':'owned-test-host',
               'binarySha256':fleet_node.digest(state/'rustfs'),'inspectorSha256':fleet_node.digest(state/'heal-meta-inspect')}
        stopped=Mock();decoded=[]
        def digest(p):
            self.assertIn(p,(state/'rustfs',state/'heal-meta-inspect'))
            return fleet_node.digest(p)
        def command(*args,**kwargs):
            self.assertEqual(args[0],str(state/'heal-meta-inspect'))
            self.assertTrue(args[1].startswith(f'/proc/{os.getpid()}/fd/'))
            fd=int(args[1].rsplit('/',1)[1]);raw=os.pread(fd,backup.LIMIT+1,0)
            self.assertIn(raw,[p.read_bytes() for p in files])
            decoded.append(hashlib.sha256(raw).hexdigest())
            return SimpleNamespace(returncode=0,stdout=json.dumps({'bucket':bucket,'key':backup.KEY,
                'physicalBitrotVerified':False,'mockInputSha256':decoded[-1]}),stderr='')
        node={'owned':lambda run:(state,roots,copy.deepcopy(owner)),'MOUNTS':mounts,'checked':fleet_node.checked,
              'digest':digest,'stopped':stopped,'fault':heal_fault,'command':command}
        request={'runId':run,'action':'mpu-race-backups','bucket':bucket,
                 'expectedBinarySha256':owner['binarySha256'],'expectedInspectorSha256':owner['inspectorSha256']}
        return request,node,files,decoded

    def test_stopped_census_hash_and_decoder_share_actual_opened_inode_on_every_drive(self):
        with tempfile.TemporaryDirectory() as tmp:
            request,node,files,decoded=self.fixture(tmp);original=[p.read_bytes() for p in files]
            with patch.object(heal_fault,'mount_id',side_effect=lambda fd:os.fstat(fd).st_dev):value=backup.run(request,node)
            self.assertEqual(len(value['rows']),4);self.assertEqual(len(decoded),4)
            for i,row in enumerate(value['rows']):
                self.assertEqual(row['disk'],i);self.assertEqual(row['size'],len(original[i]))
                self.assertEqual(row['sha256'],hashlib.sha256(original[i]).hexdigest())
                self.assertEqual(row['metadata']['mockInputSha256'],row['sha256'])
            self.assertTrue(value['readOnly'] and value['quiescent'] and value['sameHeldFileHashedAndDecoded'])
            self.assertFalse(value['rawContentsExported'] or value['liveAuthorityClaimed'] or value['canonicalCasePassClaimed'])
            self.assertEqual([p.read_bytes() for p in files],original)
            self.assertEqual(node['stopped'].call_count,2)

    def test_hardlinks_symlinks_fifo_oversize_and_unknown_generation_refuse_without_decoder(self):
        for fault in ('hardlink','symlink','fifo','oversize','generation'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as tmp:
                request,node,files,decoded=self.fixture(tmp);p=files[0]
                if fault=='hardlink':os.link(p,p.with_name('extra-link'))
                elif fault=='symlink':p.unlink();p.symlink_to(Path(tmp)/'state/credentials.env')
                elif fault=='fifo':p.unlink();os.mkfifo(p)
                elif fault=='oversize':p.write_bytes(b'x'*(backup.LIMIT+1))
                else:p.parent.rename(p.parent.with_name('unregistered-directory'))
                with patch.object(heal_fault,'mount_id',side_effect=lambda fd:os.fstat(fd).st_dev),self.assertRaises((OSError,ValueError)):
                    backup.run(request,node)
                self.assertFalse(decoded)

    def test_injected_file_mount_escape_and_decoder_identity_or_inode_drift_refuse(self):
        for fault in ('mount','decoder','inode'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as tmp:
                request,node,files,decoded=self.fixture(tmp);p=files[0];inode=p.stat().st_ino
                original=node['command']
                def command(*args,**kwargs):
                    value=original(*args,**kwargs)
                    if fault=='decoder':value.stdout=json.dumps({'bucket':'foreign','key':backup.KEY,'physicalBitrotVerified':False})
                    if fault=='inode':p.unlink();p.write_bytes(b'replaced after held-file decode')
                    return value
                node['command']=command
                def mount_id(fd):return os.fstat(fd).st_dev+(1 if fault=='mount' and os.fstat(fd).st_ino==inode else 0)
                with patch.object(heal_fault,'mount_id',side_effect=mount_id),self.assertRaises(ValueError):backup.run(request,node)

    def test_real_remote_adapter_embeds_backend_and_existing_descriptor_reader(self):
        def execute(*args,**kwargs):
            source=kwargs['input'].rsplit('\nmain(',1)[0];scope={'__name__':'__mpu_backups_test__'}
            exec(compile(source,'g05-backups-node','exec'),scope)
            self.assertTrue(callable(scope['mpu_race_backups_backend'].run))
            self.assertTrue(callable(scope['fault'].parent))
            return SimpleNamespace(returncode=0,stdout='{}',stderr='')
        with tempfile.TemporaryDirectory() as tmp,patch('scripts.validation_heal.healthy.subprocess.run',side_effect=execute):
            Fleet(['vm000'],Path(tmp),'gh-12345-1-h1').remote('vm000','mpu-race-backups')
