"""Adapt owned H1/H2 fleet snapshots to the frozen version physical oracle."""
import copy

try:
    from . import version_race_fixture as model, version_race_physical as physical
    from .heal_probe import require
    from .healthy import validate_topology
    from .shards import files
except ImportError:
    import version_race_fixture as model, version_race_physical as physical
    from heal_probe import require
    from healthy import validate_topology
    from shards import files


SELECTED=('objects-0000.bin','objects-0001.bin','objects-0002.bin',model.TARGET)


def view(snapshot,bucket,keys,inspector_sha):
    """Use format set slots, never node/disk sorting, as decoder drive indices."""
    topology='H1' if len(snapshot)==1 else 'H2'
    proof=validate_topology(snapshot,topology)
    require(len(keys)==len(set(keys)) and set(keys)<={f'objects-{i:04d}.bin' for i in range(257)},
            'version snapshot keys differ from the frozen flat namespace')
    formats={};rows=[];metadata=[];identities={};indexed=files(snapshot)
    for node,value in snapshot.items():
        require(len(value['formats'])==4 and {v['disk'] for v in value['formats']}==set(range(4)),
                'version snapshot lacks four distinct local drives')
        for drive in value['formats']:
            slot=proof['diskUUIDs'].index(drive['value']['xl']['this'])
            identities[node,drive['disk']]=slot;formats[slot]=copy.deepcopy(drive)
            for key in keys:
                identity=node,drive['disk'],f'{bucket}/{key}/xl.meta'
                row=indexed.get(identity)
                if row is None:metadata.append({'disk':slot,'key':key,'absent':True})
                else:
                    require(isinstance(row.get('metadata'),dict) and not row.get('metadataError'),
                            'metadata absence cannot substitute for a decode error')
                    metadata.append({'disk':slot,'key':key,'metadataSha256':row['sha256'],
                                     'decoded':copy.deepcopy(row['metadata'])})
        for row in value['files']:
            require(row['path'].startswith(bucket+'/'),'version snapshot contains another bucket')
            key=row['path'][len(bucket)+1:].split('/')[0]
            require(key in keys,'version snapshot contains an unregistered key')
            rows.append({k:copy.deepcopy(v) for k,v in {**row,'disk':identities[node,row['disk']]}.items()
                         if k!='metadata'})
    require(len(set(identities.values()))==len(identities)==len(proof['diskUUIDs']),
            'version snapshot duplicates a global set slot')
    normalized={'formats':formats,'files':rows}
    physical.file_index(normalized,disk_count=len(identities))
    return normalized,{'inspectorSha256':inspector_sha,'rows':metadata}


def slice_entries(entries,key):return [copy.deepcopy(v) for v in entries if v['key']==key]


def metadata_targets(snapshot,bucket,ledger,branch,inspector_sha):
    require(branch in model.BRANCHES[4:] and ledger.bucket==bucket and ledger.bound and not ledger.poisoned
            and ledger.keys=={f'objects-{i:04d}.bin' for i in range(257)},
            'G04 requires the complete bound original 257-key ledger')
    entries=ledger.entries();keys=sorted(ledger.keys)
    normalized,decoded=view(snapshot,bucket,keys,inspector_sha)
    for key in SELECTED:
        physical.key_state(normalized,decoded,bucket,key,slice_entries(entries,key),
                           inspector_sha=inspector_sha,disk_count=len(snapshot)*4)
    indexed=files(snapshot);node=next(iter(snapshot));targets=[]
    for key in keys:
        data=[v for v in entries if v['key']==key and not v['deleteMarker']]
        members={identity:row for identity,row in indexed.items() if identity[2].startswith(f'{bucket}/{key}/')}
        require(len(members)==len(snapshot)*4*(1+len(data))
                and sum(path.endswith('/part.1') for _,_,path in members)==len(snapshot)*4*len(data),
                'G04 baseline has unregistered files or incomplete original version shards')
        relative=f'{bucket}/{key}/xl.meta';row=indexed[node,3,relative]
        targets.append({'node':node,'disk':3,'relativePath':relative,'key':key,
                        'sha256':row['sha256'],'size':row['size']})
    require(targets[-1]['key']==model.TARGET,'G04 selected target is not the final flat key')
    return targets


def reconstruction(before,after,bucket,target):
    """Only the selected metadata may change; every data byte stays original."""
    identity=target['node'],target['disk'],target['relativePath']
    prefix=f"{bucket}/{target['key']}/"
    left={i:v for i,v in files(before).items() if i[2].startswith(prefix)}
    right={i:v for i,v in files(after).items() if i[2].startswith(prefix)}
    require(set(left)==set(right) and identity in left
            and all(before[node]['formats']==after[node]['formats'] for node in before),
            'G04 metadata repair changed file population or formats')
    require(all(value==right[i] for i,value in left.items() if i!=identity),
            'G04 metadata repair changed another metadata member or data shard')
    require(0<right[identity]['size']<1024**2,'G04 reconstructed metadata is empty or excessive')
    return {'metadataReconstructed':True,'allDataShardsUnchanged':True}


def transition(before,after,bucket,baseline,final,inspector_sha):
    keys=sorted({v['key'] for v in baseline})
    old,old_metadata=view(before,bucket,keys,inspector_sha)
    new,new_metadata=view(after,bucket,keys,inspector_sha)
    proofs={}
    for key in keys:
        proofs[key]=physical.transition(old,new,old_metadata,new_metadata,bucket,key,
            slice_entries(baseline,key),slice_entries(final,key),inspector_sha=inspector_sha,
            disk_count=len(before)*4)
    return {'keys':proofs,'allOriginalKeysVerified':len(proofs),'physicalProofBeforeGETOrRescue':True,
            'canonicalCasePassClaimed':False}
