"""Owned cgroup control boundaries; mock kernel evidence is not live proof."""
import copy
from contextlib import ExitStack
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts.validation_heal import fleet_node, nolock, race, race_io
from scripts.validation_heal.healthy import Fleet
import test_validation_heal as physical_fixtures


class RaceIoTests(unittest.TestCase):
    def test_actual_mapper_selects_fourth_physical_drive_independent_of_coding_role(self):
        scene = physical_fixtures.HealShardOracleTest().snapshot()
        entry = next(v for v in scene.values())['files'][0]
        bucket, key = entry['metadata']['bucket'], entry['metadata']['key']
        mapped = race.map_fixture(scene,bucket,[{'key':key,'versionId':'null','deleteMarker':False}])
        target = mapped[key,'null']
        self.assertEqual((target['node'],target['disk']),('node1',3))
        self.assertEqual(target['role'],'parity')
        self.assertEqual(len(target['cohort']),4)

    def fixture(self):
        process = {k: k for k in fleet_node.PROCESS_FIELDS}
        process.update(pid=1234, startTicks=17, unit='owned.service', binarySha256='candidate')
        mounts = [{'target': f'/data/rustfs{i+1}', 'maj:min': f'8:{i+1}', 'uuid': str(i)} for i in range(4)]
        owner = {'runtimeProfile':'manual', 'unit':'owned.service', 'binarySha256':'candidate', 'mounts':mounts}
        kernel = {'raw':'', 'writes':[], 'wrongReadback':False, 'membership':'0::/system.slice/owned.service\n',
                  'group':'/system.slice/owned.service\n'}
        def command(*args, **kwargs):
            if args[:2] == ('systemctl', 'show'): raw = kernel['group']
            elif args[:2] == ('cat', '/proc/1234/cgroup'): raw = kernel['membership']
            elif args[:1] == ('cat',):
                self.assertEqual(args[1], '/sys/fs/cgroup/system.slice/owned.service/io.max')
                raw = kernel['raw']
            elif args[:2] == ('systemctl', 'set-property'):
                self.assertEqual(args[2:5], ('--runtime', 'owned.service', args[4]))
                kernel['writes'].append(args)
                enabled = args[4] == 'IOWriteBandwidthMax=/data/rustfs4 65536'
                self.assertIn(args[4], ('IOWriteBandwidthMax=/data/rustfs4 65536', 'IOWriteBandwidthMax='))
                kernel['raw'] = '8:4 rbps=max wbps=' + ('65536' if enabled and not kernel['wrongReadback'] else 'max') + ' riops=max wiops=max\n'
                raw = ''
            else: raise AssertionError(args)
            return SimpleNamespace(stdout=raw, stderr='', returncode=0)
        node = {'owned':lambda run:(Path('/state'), tuple(Path(f'/root{i}') for i in range(4)), copy.deepcopy(owner)),
                'MOUNTS':fleet_node.MOUNTS, 'PROCESS_FIELDS':fleet_node.PROCESS_FIELDS,
                'process':lambda request:copy.deepcopy(process), 'mounts':lambda:copy.deepcopy(mounts), 'command':command}
        request = {'runId':'gh-12345-1-h1', 'action':'race-io-cap', 'caseId':race.CASE_IDS[0],
                   'branch':'overwrite-vs-deep', 'enabled':True, 'expectedProcess':copy.deepcopy(process)}
        return request, node, kernel, process, owner

    def test_enable_and_release_require_exact_kernel_fourth_device_and_original_process(self):
        request, node, kernel, process, owner = self.fixture()
        enabled = race_io.run(request, node)
        self.assertTrue(enabled['actualKernelLimitVerified'])
        self.assertEqual((enabled['disk'], enabled['device'], enabled['writeBytesPerSecond']), (3,'8:4',65536))
        self.assertEqual(enabled['process'], process)
        self.assertFalse(enabled['publicOverlapProved'] or enabled['canonicalCasePassClaimed'])
        released = race_io.run({**request, 'enabled':False}, node)
        self.assertIsNone(released['writeBytesPerSecond'])
        self.assertEqual(len(kernel['writes']), 2)
        self.assertEqual(owner['mounts'][3]['target'], '/data/rustfs4')

    def test_unregistered_g05_bad_identity_membership_or_foreign_limit_refuse_before_write(self):
        for fault in ('g05','unknown-branch','boolean','extra','birth','runtime','membership','group','foreign','unbound-release'):
            with self.subTest(fault=fault):
                request, node, kernel, process, owner = self.fixture()
                if fault == 'g05': request.update(caseId=race.CASE_IDS[4], branch='complete')
                elif fault == 'unknown-branch': request['branch'] = 'unknown'
                elif fault == 'boolean': request['enabled'] = 1
                elif fault == 'extra': request['device'] = '/etc/passwd'
                elif fault == 'birth': process['startTicks'] += 1
                elif fault == 'runtime': owner['runtimeProfile'] = 'root-restart'
                elif fault == 'membership': kernel['membership'] = '0::/system.slice/another.service\n'
                elif fault == 'group': kernel['group'] = '/system.slice/owned.service/../../foreign.service\n'
                elif fault == 'foreign': kernel['raw'] = '8:1 rbps=max wbps=65536 riops=max wiops=max\n'
                else: request['enabled'] = False
                with self.assertRaises(ValueError): race_io.run(request, node)
                self.assertFalse(kernel['writes'])

    def test_wrong_kernel_readback_cannot_claim_effective_cap(self):
        request, node, kernel, process, _ = self.fixture()
        kernel['wrongReadback'] = True
        with self.assertRaisesRegex(ValueError, 'kernel cgroup'): race_io.run(request, node)
        self.assertEqual(len(kernel['writes']), 1)

    def test_process_or_unit_cgroup_move_during_control_cannot_claim_effective_cap(self):
        for move in ('process','unit'):
            with self.subTest(move=move):
                request, node, kernel, _, _ = self.fixture()
                original = node['command']
                def command(*args, **kwargs):
                    value = original(*args,**kwargs)
                    if args[:2] == ('systemctl','set-property'):
                        if move == 'process': kernel['membership'] = '0::/system.slice/other.service\n'
                        else: kernel['group'] = '/system.slice/other.service\n'
                    return value
                node['command'] = command
                with self.assertRaisesRegex(ValueError,'cgroup'): race_io.run(request,node)
                self.assertEqual(len(kernel['writes']),1)

    def test_process_birth_change_during_control_cannot_claim_effective_cap(self):
        request, node, kernel, process, _ = self.fixture()
        original = node['process']; calls = 0
        def changing(request):
            nonlocal calls
            calls += 1
            if calls == 3: process['startTicks'] += 1
            return original(request)
        node['process'] = changing
        with self.assertRaisesRegex(ValueError, 'process birth'): race_io.run(request, node)
        self.assertEqual(len(kernel['writes']), 1)

    def test_kernel_parser_rejects_incomplete_duplicate_or_unbounded_fields(self):
        for raw in ('8:4 wbps=65536', '8:4 rbps=max wbps=0 riops=max wiops=max',
                    '8:4 rbps=max wbps=65536 riops=max rbps=max',
                    '8:4 rbps=max wbps=max riops=max wiops=max\n'*2, 'x'*65537):
            with self.subTest(raw=raw[:80]), self.assertRaises(ValueError): race_io.limits(raw)

    def observation(self):
        request, node, kernel, process, owner = self.fixture()
        request.pop('enabled')
        request.update(action='race-io-observe', caseId=race.CASE_IDS[4], branch='complete')
        original = node['command']
        kernel['ancestors'] = {'/sys/fs/cgroup/system.slice/io.max': '', '/sys/fs/cgroup/io.max': ''}
        kernel['reads'] = []
        def command(*args, **kwargs):
            if args[0] == 'cat' and args[1].endswith('/io.max'):
                kernel['reads'].append(args[1])
                if args[1] in kernel['ancestors']:
                    return SimpleNamespace(stdout=kernel['ancestors'][args[1]], stderr='', returncode=0)
            return original(*args, **kwargs)
        node['command'] = command
        return request, node, kernel, process, owner

    def test_g05_observes_owned_and_inherited_io_limits_without_writes(self):
        request, node, kernel, _, _ = self.observation()
        for branch in ('upload-continue', 'complete', 'abort'):
            result = race_io.run({**request, 'branch': branch}, node)
            self.assertTrue(result['inspectionOnly'] and result['actualKernelNoLimitVerifiedAtSamples'])
            self.assertFalse(result['mutated'] or result['continuousNoLimitProved'] or result['ioCapRequested'])
            self.assertEqual(len(result['beforeIoScopes']), 3)
            self.assertEqual(result['beforeIoScopes'], result['afterIoScopes'])
        self.assertFalse(kernel['writes'])
        self.assertEqual(len(kernel['reads']), 18)

    def test_g05_refuses_each_ancestor_cap_and_cannot_release_a_foreign_limit(self):
        for where in ('owned', '/sys/fs/cgroup/system.slice/io.max', '/sys/fs/cgroup/io.max'):
            for limit in ('rbps', 'wbps', 'riops', 'wiops'):
                with self.subTest(where=where, limit=limit):
                    request, node, kernel, _, _ = self.observation()
                    raw = '8:4 ' + ' '.join(f'{k}={65536 if k == limit else "max"}'
                        for k in ('rbps', 'wbps', 'riops', 'wiops')) + '\n'
                    if where == 'owned': kernel['raw'] = raw
                    else: kernel['ancestors'][where] = raw
                    with self.assertRaisesRegex(ValueError, 'inherited kernel IO cap'):
                        race_io.run(request, node)
                    self.assertFalse(kernel['writes'])

    def test_g05_observation_rejects_scope_changes_and_mutation_parameters(self):
        for fault in ('case', 'branch', 'enabled', 'process', 'group', 'runtime', 'mount', 'late-cap', 'late-birth', 'late-group'):
            with self.subTest(fault=fault):
                request, node, kernel, process, owner = self.observation()
                if fault == 'case': request['caseId'] = race.CASE_IDS[0]
                elif fault == 'branch': request['branch'] = 'copy-source'
                elif fault == 'enabled': request['enabled'] = False
                elif fault == 'process': process['startTicks'] += 1
                elif fault == 'group': kernel['membership'] = '0::/system.slice/other.service\n'
                elif fault == 'runtime': owner['runtimeProfile'] = 'lock-quorum'
                elif fault == 'mount':
                    mounts = copy.deepcopy(owner['mounts'])
                    mounts[0]['maj:min'] = '8:99'
                    node['mounts'] = lambda: mounts
                else:
                    original = node['command']
                    def change(*args, **kwargs):
                        result = original(*args, **kwargs)
                        if len(kernel['reads']) == 3:
                            if fault == 'late-cap': kernel['ancestors']['/sys/fs/cgroup/io.max'] = '8:4 rbps=max wbps=7 riops=max wiops=max\n'
                            elif fault == 'late-birth': process['startTicks'] += 1
                            else: kernel['membership'] = '0::/system.slice/foreign.service\n'
                        return result
                    node['command'] = change
                with self.assertRaises(ValueError): race_io.run(request, node)
                self.assertFalse(kernel['writes'])

    def test_missing_unenabled_controller_is_accepted_only_for_an_existing_owned_group(self):
        for group_exists in (True, False):
            with self.subTest(group_exists=group_exists):
                request, node, kernel, _, _ = self.fixture()
                original = node['command']; first = True
                def command(*args, **kwargs):
                    nonlocal first
                    if args == ('cat','/sys/fs/cgroup/system.slice/owned.service/io.max') and first:
                        first = False
                        return SimpleNamespace(returncode=1,stdout='',stderr='cat: io.max: No such file or directory')
                    if args == ('test','-d','/sys/fs/cgroup/system.slice/owned.service'):
                        return SimpleNamespace(returncode=0 if group_exists else 1,stdout='',stderr='')
                    return original(*args,**kwargs)
                node['command'] = command
                if group_exists:
                    self.assertTrue(race_io.run(request,node)['actualKernelLimitVerified'])
                else:
                    with self.assertRaisesRegex(ValueError,'limits could not be read'): race_io.run(request,node)
                    self.assertFalse(kernel['writes'])

    def test_real_remote_adapter_embeds_the_reviewed_node_module(self):
        def execute(*args, **kwargs):
            source = kwargs['input'].rsplit('\nmain(',1)[0]
            scope = {'__name__':'__race_io_test__'}
            exec(compile(source,'race-io-node','exec'),scope)
            self.assertEqual(scope['race_io_backend'].RATE,'65536')
            self.assertTrue(callable(scope['race_io_backend'].run))
            return SimpleNamespace(returncode=0,stdout='{}',stderr='')
        for action in ('race-io-cap','race-io-observe'):
            with self.subTest(action=action),tempfile.TemporaryDirectory() as tmp, \
                 patch('scripts.validation_heal.healthy.subprocess.run',side_effect=execute):
                Fleet(['vm000'],Path(tmp),'gh-12345-1-h1').remote('vm000',action)

    def test_controller_records_intent_before_single_call_and_never_retries_a_lost_result(self):
        fleet = Mock(); fleet.nodes = ['vm000','vm001']; trace = []
        def record(label,value): trace.append(label); return value
        def remote(host,action,**params):
            self.assertEqual(host,'vm001')
            if action == 'process': return {'pid':1234,'birth':'original'}
            self.assertEqual(action,'race-io-cap')
            self.assertEqual(params['expectedProcess'],{'pid':1234,'birth':'original'})
            self.assertEqual(trace,['race-io-process','race-io-cap-intent'])
            raise OSError('lost response; caller must stop its owned service')
        fleet.record.side_effect = record; fleet.remote.side_effect = remote
        with self.assertRaises(OSError):
            race_io.set_fleet(fleet,race.CASE_IDS[0],'overwrite-vs-deep',True,nodes=['vm001'])
        self.assertEqual(fleet.remote.call_count,2)
        self.assertEqual(trace,['race-io-process','race-io-cap-intent'])
        for nodes in ([],['foreign'],['vm001','vm001']):
            fleet.remote.reset_mock()
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                race_io.set_fleet(fleet,race.CASE_IDS[0],'overwrite-vs-deep',True,nodes=nodes)
            fleet.remote.assert_not_called()

    def test_real_runner_arms_before_admin_and_releases_after_foreground_before_terminal(self):
        class ReachedTerminalBoundary(BaseException): pass
        for driver, case_id, branch in [(race,race.CASE_IDS[0],'overwrite-vs-deep'),
                                       (nolock,nolock.CASE_ID,'overwrite-nolock')]:
            with self.subTest(case=case_id), tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
                root = Path(tmp); candidate = root/'candidate.json'
                candidate.write_text('{"source_sha":"072b57b37559fbe626aeda8abf61617fdcbdd926","package_sha256":"digest"}')
                rc = root/'rc'; rc.write_bytes(b'client')
                selected = race.target_key(branch) if driver is race else nolock.TARGET_KEY
                expected = [{'key':key,'versionId':'null','isLatest':True} for key in ('control/a','control/b',selected)]
                mapped = {(r['key'],'null'):{**r,'disk':0,'node':'vm000','relativePath':r['key'],'sha256':'original'} for r in expected}
                trace = []
                fleet = Mock(); fleet.nodes = ['vm000']
                fleet.record.side_effect = lambda label,value:value
                fleet.snapshot.return_value = {'vm000':{'files':[],'formats':[]}}
                fleet.remote.return_value = {}
                fleet.cleanup.return_value = True
                probe = Mock(); probe.admin.side_effect = lambda *a:(trace.append('admin') or {'httpStatus':200,'response':{'clientToken':'token'}})
                probe.inventory.return_value = {'entries':expected}
                def cap(f,c,b,enabled,*,nodes):
                    self.assertIs(f,fleet); self.assertEqual(nodes,['vm000']); trace.append('cap-on' if enabled else 'cap-off')
                def foreground(*args, **kwargs): trace.append('foreground'); return [],False
                def terminal(*args): trace.append('terminal'); raise ReachedTerminalBoundary()
                values = {'Fleet':Mock(return_value=fleet),'Probe':Mock(return_value=probe),'verify_inspector':Mock(return_value={}),
                          'validate_topology':Mock(),'seed_fixture':Mock(return_value=expected),'verify_seed':Mock(),
                          'operation_plan':Mock(return_value=[{}]),'map_fixture':Mock(return_value=mapped),'verify_part_manifest':Mock(),
                          'validate_missing_batch':Mock(),'wait_running':Mock(),'race':Mock(side_effect=foreground),
                          'final_inventory':Mock(return_value={'entries':expected}),
                          'wait_terminal':Mock(side_effect=terminal),'set_race_io_cap':Mock(side_effect=cap),'OBJECTS':3}
                if driver is race: values['verify_endpoint_resolution'] = Mock()
                for name,value in values.items(): stack.enter_context(patch.object(driver,name,value))
                stack.enter_context(patch.dict(race.os.environ,{'GITHUB_RUN_ID':'12345','GITHUB_RUN_ATTEMPT':'1'}))
                args = SimpleNamespace(evidence=root/'evidence',candidate=candidate,rc_bin=rc,tool_directory=root,
                                       testing_commit='helper',binary_sha='binary',case_id=case_id)
                with self.assertRaises(ReachedTerminalBoundary): driver.run_branch(args,'H1',['vm000'],branch)
                self.assertEqual(trace[:trace.index('terminal')+1],['cap-on','admin','foreground','cap-off','terminal'])


if __name__ == '__main__': unittest.main()
