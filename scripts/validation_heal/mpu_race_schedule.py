"""Frozen G05 preparation and ordered SDK/checkpoint schedule, without I/O cap."""
try:
    from .heal_probe import canonical, require
except ImportError:
    from heal_probe import canonical, require

SELECTED = {'upload-continue':'upload_part', 'complete':'complete_multipart_upload', 'abort':'abort_multipart_upload'}


def prepare(fixture, branch):
    with fixture.lock:
        require(branch in SELECTED and not fixture.seed_started and not fixture.items and fixture.poisoned is None,
                'G05 branch preparation requires an original fresh model')
        fixture.seed()
        if branch != 'upload-continue':
            fixture.upload('a', 3)
        return {'branch':branch, 'selectedOperation':SELECTED[branch], 'manifest':fixture.manifest()}


def execute(fixture, branch, prepared, checkpoint):
    """Worker callback writes named physical checkpoints outside Fleet.sequence.

    Only the selected first SDK call needs the public running-token witness.
    Every companion still needs its original ACK and immediate physical check.
    """
    with fixture.lock:
        require(branch in SELECTED and callable(checkpoint) and isinstance(prepared,dict)
                and set(prepared)=={'branch','selectedOperation','manifest'} and prepared['branch']==branch
                and prepared['selectedOperation']==SELECTED[branch] and fixture.poisoned is None
                and canonical(prepared['manifest'])==canonical(fixture.manifest()),
                'G05 foreground differs from its original prepared branch/model')
        expected_a = 2 if branch=='upload-continue' else 3
        require(set(fixture.items)=={'committed','a','b'} and fixture.items['committed']['state']=='complete'
                and fixture.items['a']['state']==fixture.items['b']['state']=='pending'
                and len(fixture.items['committed']['parts'])==len(fixture.items['b']['parts'])==3
                and len(fixture.items['a']['parts'])==expected_a,
                'G05 requires the exact original committed/A/B population before Heal')
        steps = ([('continued',lambda:fixture.upload('a',3)),('completed',lambda:fixture.complete('a')),
                  ('aborted',fixture.abort_b)] if branch=='upload-continue' else
                 [('completed',lambda:fixture.complete('a')),('aborted',fixture.abort_b)] if branch=='complete' else
                 [('aborted',fixture.abort_b),('completed',lambda:fixture.complete('a'))])
        records=[]
        try:
            for label,call in steps:
                response=call()
                sequence=fixture.recorder.sequence
                manifest=fixture.manifest()
                witness=checkpoint(label,manifest)
                require(canonical(manifest)==canonical(fixture.manifest()),'G05 physical checkpoint mutated its ACK model')
                records.append({'phase':label,'ackSequence':sequence,'response':response,'checkpoint':witness})
        except BaseException:
            # Every received ACK already has the original durable Recorder/model
            # files. A missing physical witness cannot authorize another mutation.
            if fixture.poisoned is None:
                fixture.poisoned='G05 physical checkpoint failed; retain original ACK state and halt companion mutations'
            raise
        require(fixture.items['a']['state']=='complete' and fixture.items['b']['state']=='aborted',
                'G05 foreground did not retire both original uploads')
        return {'branch':branch,'selectedOperation':SELECTED[branch],'selectedAckSequence':records[0]['ackSequence'],
                'steps':records,'manifest':fixture.manifest(),'ioCapRequested':False,'physicalRepairProved':False,
                'publicOverlapProved':False,'canonicalCasePassClaimed':False}
