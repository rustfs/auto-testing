"""Exercise real grace oracles and source-procedure ordering without a live fleet."""

import copy
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import subprocess
import types
import unittest
import uuid
from contextlib import ExitStack
from unittest.mock import patch

from scripts.validation_heal import fleet_node, healthy, outcome, outcome_grace as grace
from scripts.validation_heal.healthy import BASE_OPTIONS, physical_content
from test_validation_heal import versioned_scene
import test_validation_heal_orphan as orphan_tests


BUCKET = 'rv-heal-0123456789abcdef'
INCARNATION = str(uuid.UUID(int=30))
OLD, CURRENT = str(uuid.UUID(int=21)), str(uuid.UUID(int=22))
OPTIONS = {**BASE_OPTIONS, 'recursive': True, 'remove': True}


def terminal(*, cleanup=False, healthy_only=False):
    old_state = {'state': 'repaired' if cleanup else 'verified_healthy'} if cleanup or healthy_only else {
        'state': 'deferred', 'details': {'reason': 'dangling_delete_grace',
            'retryNotBefore': {'secs_since_epoch': 1120, 'nanos_since_epoch': 0}}}
    return {'summary': 'finished', 'settings': OPTIONS, 'truncated': False, 'outcome': {
        'execution': {'state': 'completed'}, 'coverage': 'complete', 'objectsTruncated': False,
        'counters': dict(processed=2, healed=int(cleanup), unchanged=2 if healthy_only else 1,
            skipped=int(not cleanup and not healthy_only), failed=0, unknown=0,
            attemptFailures=int(not cleanup and not healthy_only), overflowed=False),
        'objects': [{'identity': dict(kind='object', bucket=BUCKET, object=grace.KEY, versionId=version,
            bucketIncarnationId=INCARNATION, poolIndex=0, setIndex=0), 'disposition': disposition}
            for version, disposition in ((OLD, old_state), (CURRENT, {'state': 'verified_healthy'}))]}}


def scene(nodes=1):
    initial, bucket, entries = versioned_scene([grace.KEY, 'control.bin'])
    for entry in initial['node1']['files']:
        if not entry['path'].startswith(bucket + '/' + grace.KEY + '/'): continue
        if 'metadata' in entry:
            versions = entry['metadata']['versions'][:2]
            for index, version in enumerate(versions):
                version.update(versionId=(OLD, CURRENT)[index], isLatest=index == 1)
            entry['metadata']['versions'] = versions
    old_dir = next(entry['metadata']['versions'][0]['dataDir'] for entry in initial['node1']['files']
                   if entry['path'] == bucket + '/' + grace.KEY + '/xl.meta')
    unused = '12345678-1234-1234-1234-000000000002'
    initial['node1']['files'] = [entry for entry in initial['node1']['files']
        if not entry['path'].startswith(bucket + '/' + grace.KEY + '/' + unused + '/')]
    for index in range(2, nodes + 1): initial['node' + str(index)] = copy.deepcopy(initial['node1'])
    authority = copy.deepcopy(initial)
    for value in authority.values():
        value['files'] = [entry for entry in value['files']
            if not entry['path'].startswith(bucket + '/' + grace.KEY + '/' + old_dir + '/')]
        for entry in value['files']:
            if entry['path'] == bucket + '/' + grace.KEY + '/xl.meta':
                entry['metadata']['versions'] = entry['metadata']['versions'][1:]
                entry['sha256'] = 'e' * 64
    target = ('node' + str(nodes), 0)
    injected = copy.deepcopy(authority)
    prefix = bucket + '/' + grace.KEY + '/'
    injected[target[0]]['files'] = [entry for entry in injected[target[0]]['files']
        if not (entry['disk'] == 0 and entry['path'].startswith(prefix))]
    injected[target[0]]['files'] += [copy.deepcopy(entry) for entry in initial[target[0]]['files']
        if entry['disk'] == 0 and entry['path'].startswith(prefix)]
    return initial, authority, injected, target, old_dir


class GraceOracleTest(unittest.TestCase):
    old = {'versionId': OLD, 'lastModifiedEpoch': 1000.0}
    current = {'versionId': CURRENT}

    def check(self, value, observed=1010, cleanup=False):
        return grace.grace_outcome(value, BUCKET, self.old, self.current, OPTIONS, INCARNATION, observed, cleanup=cleanup)

    def test_exact_future_deferral_and_distinct_post_expiry_repair(self):
        self.assertEqual(self.check(terminal())['retryNotBeforeEpoch'], 1120)
        self.assertTrue(self.check(terminal(cleanup=True), 1120, True)['newTaskProductCleanup'])
        with self.assertRaises(ValueError): self.check(terminal(cleanup=True), 1119.999, True)
        with self.assertRaises(ValueError): self.check(terminal(), 1120)

    def test_source_omits_false_public_truncation_without_losing_canonical_coverage(self):
        status = terminal(); status.pop('truncated')
        self.assertEqual(self.check(status)['retryNotBeforeEpoch'], 1120)
        for value in (None, True, 0, 'false'):
            with self.subTest(value=value), self.assertRaises(ValueError): self.check({**status, 'truncated': value})
        for value in (None, True):
            changed = copy.deepcopy(status)
            if value is None: changed['outcome'].pop('objectsTruncated')
            else: changed['outcome']['objectsTruncated'] = value
            with self.assertRaises(ValueError): self.check(changed)

    def test_wrong_truth_and_legacy_counts_cannot_replace_original_responsibility(self):
        for path, values in (
            (('summary',), ['running', 'stopped']), (('truncated',), [True, None]),
            (('outcome', 'coverage'), ['partial', None]),
            (('outcome', 'execution'), [{'state': 'completed_with_errors'}]),
            (('outcome', 'objectsTruncated'), [True, None]),
            (('settings',), [{**OPTIONS, 'remove': False}])):
            for value in values:
                changed = terminal(); parent = changed
                for name in path[:-1]: parent = parent[name]
                parent[path[-1]] = value
                with self.subTest(path=path, value=value), self.assertRaises(ValueError): self.check(changed)
        for name, original in terminal()['outcome']['counters'].items():
            for value in (None, not original if type(original) is bool else original + 1,
                          0 if type(original) is bool else bool(original)):
                changed = terminal(); changed['outcome']['counters'][name] = value
                with self.subTest(counter=name, value=value), self.assertRaises(ValueError): self.check(changed)
        changed = terminal(); changed['outcome']['counters']['legacyHealed'] = 88
        with self.assertRaises(ValueError): self.check(changed)

    def test_every_identity_boundary_and_duplicate_version_is_rejected(self):
        for name, values in {'kind': ['bucket'], 'bucket': ['foreign'], 'object': ['other.bin'],
                'versionId': [None, CURRENT], 'bucketIncarnationId': [None, 'new'],
                'poolIndex': [True, 1], 'setIndex': [False, 1]}.items():
            for value in values:
                changed = terminal(); changed['outcome']['objects'][0]['identity'][name] = value
                with self.subTest(name=name, value=value), self.assertRaises(ValueError): self.check(changed)
        for items in ([], terminal()['outcome']['objects'][:1], terminal()['outcome']['objects'] * 2):
            changed = terminal(); changed['outcome']['objects'] = items
            with self.assertRaises(ValueError): self.check(changed)

    def test_exact_retry_wire_shape_bounds_and_original_age_are_required(self):
        for value in (None, 1120, {}, {'secs_since_epoch': True, 'nanos_since_epoch': 0},
                {'secs_since_epoch': -1, 'nanos_since_epoch': 0},
                {'secs_since_epoch': 2**64, 'nanos_since_epoch': 0},
                {'secs_since_epoch': 1120, 'nanos_since_epoch': -1},
                {'secs_since_epoch': 1120, 'nanos_since_epoch': 10**9},
                {'secs_since_epoch': 1120, 'nanos_since_epoch': False},
                {'secs_since_epoch': 1120, 'nanos_since_epoch': 0, 'extra': 0},
                {'secs_since_epoch': 1126, 'nanos_since_epoch': 0}):
            changed = terminal(); changed['outcome']['objects'][0]['disposition']['details']['retryNotBefore'] = value
            with self.subTest(value=value), self.assertRaises(ValueError): self.check(changed)
        for details in ({'reason': 'dangling_delete_grace', 'retry_not_before': None},
                {'reason': 'transient_existence_check', 'retryNotBefore': None}):
            changed = terminal(); changed['outcome']['objects'][0]['disposition']['details'] = details
            with self.assertRaises(ValueError): self.check(changed)
        for value in (True, -1, float('inf'), float('nan')):
            with self.assertRaises(ValueError): self.check(terminal(), value)

    def test_false_health_unknown_skip_or_repair_cannot_certify_deferral(self):
        for index in (0, 1):
            for state in ('unknown', 'skipped', 'failed', 'repaired', 'metadata_healthy'):
                changed = terminal(); changed['outcome']['objects'][index]['disposition'] = {'state': state}
                with self.subTest(index=index, state=state), self.assertRaises(ValueError): self.check(changed)

    def test_all_original_members_and_current_logical_metadata_remain_exact(self):
        for count in (1, 4):
            initial, authority, injected, target, directory = scene(count)
            proof = grace.grace_fixture(initial, authority, injected, BUCKET, self.old, self.current, target, directory)
            self.assertEqual(proof['allOriginalMembers'], count * 4)
            for field, value in (('dataDir', str(uuid.UUID(int=99))), ('deleted', True), ('isLatest', False), ('size', 999)):
                changed = copy.deepcopy(authority)
                meta = next(entry for entry in changed['node1']['files']
                    if entry['path'] == BUCKET + '/' + grace.KEY + '/xl.meta')
                meta['metadata']['versions'][0][field] = value
                with self.subTest(nodes=count, field=field), self.assertRaises(ValueError):
                    grace.grace_fixture(initial, changed, None, BUCKET, self.old, self.current, target, directory)
            for source, label in ((authority, 'authority'), (injected, 'injected')):
                for kind in ('control', 'format', 'extra-old'):
                    changed = copy.deepcopy(source)
                    if kind == 'control':
                        next(entry for entry in changed['node1']['files'] if '/control.bin/' in entry['path'])['sha256'] = 'b' * 64
                    elif kind == 'format': changed['node1']['formats'][0]['sha256'] = 'b' * 64
                    else:
                        extra = next(entry for entry in initial['node1']['files'] if '/' + directory + '/' in entry['path'])
                        # An additional replay on a different disk is forbidden, including H1.
                        extra = copy.deepcopy(extra); extra['disk'] = 1
                        changed['node1']['files'].append(extra)
                    with self.subTest(nodes=count, source=label, poison=kind), self.assertRaises(ValueError):
                        grace.grace_fixture(initial, changed if label == 'authority' else authority,
                            changed if label == 'injected' else None, BUCKET, self.old, self.current, target, directory)


class GraceRuntimeTest(unittest.TestCase):
    def identity(self):
        return {**{name: name for name in fleet_node.PROCESS_FIELDS}, 'pid': 123,
            'binarySha256': 'a' * 64, 'settings': fleet_node.runtime_config('outcome-grace')}

    def test_owned_clock_is_read_only_fixed_policy_and_process_bound(self):
        identity = self.identity()
        request = {'runId': 'gh-123-1-h1', 'action': 'process-clock'}
        with patch.object(fleet_node, 'owned', return_value=(None, None, {'runtimeProfile': 'outcome-grace'})), \
                patch.object(fleet_node, 'process', return_value=identity), patch.object(fleet_node.time, 'time', return_value=1000):
            witness = fleet_node.process_clock(request)
            self.assertEqual(witness, dict(identity=identity, nodeEpoch=1000, readOnly=True, clockChanged=False))
            for name in ('pid', 'clock', 'path'):
                with self.assertRaises(ValueError): fleet_node.process_clock({**request, name: 1})
        for name in fleet_node.PROCESS_FIELDS:
            changed = copy.deepcopy(identity); changed[name] = 'changed'
            with patch.object(fleet_node, 'owned', return_value=(None, None, {'runtimeProfile': 'outcome-grace'})), \
                    patch.object(fleet_node, 'process', side_effect=[identity, changed]), self.subTest(field=name), self.assertRaises(ValueError):
                fleet_node.process_clock(request)
        for profile in ('manual', 'orphan-before'):
            with patch.object(fleet_node, 'owned', return_value=(None, None, {'runtimeProfile': profile})), self.assertRaises(ValueError):
                fleet_node.process_clock(request)
        for changes in ({'pid': 0}, {'pid': True}, {'settings': {'RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS': '0'}}):
            with patch.object(fleet_node, 'owned', return_value=(None, None, {'runtimeProfile': 'outcome-grace'})), \
                    patch.object(fleet_node, 'process', return_value={**identity, **changes}), self.assertRaises(ValueError):
                fleet_node.process_clock(request)

    def test_grace_descriptor_reuses_last_ack_authority_without_reviving_old(self):
        helper = orphan_tests.OrphanDescriptorTests()
        helper.setUp(); self.addCleanup(helper.tearDown)
        helper.owner['runtimeProfile'] = 'outcome-grace'
        helper.authority()
        helper.invoke('install', mode='object', dataDir=helper.old_dir)
        self.assertTrue((helper.object / helper.old_dir).is_dir())
        self.assertTrue(helper.recover()['lastACKAuthorityRestored'])
        self.assertFalse((helper.object / helper.old_dir).exists())
        self.assertEqual((helper.object / 'xl.meta').read_bytes(), b'XL2 live')
        for field, value in (('key', '../escape'), ('disk', True), ('disk', 4), ('bucket', 'foreign')):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                helper.invoke('capture', **{field: value}, generation='authority', expectedFiles=[], expectedPresent=False)

    def test_grace_runtime_cannot_access_other_orphan_mutators(self):
        owner = {'runtimeProfile': 'outcome-grace', 'binarySha256': 'a' * 64}
        with patch.object(fleet_node, 'owned', return_value=(Path('/owned'), (), owner)), \
                patch.object(fleet_node, 'stopped'), patch.object(fleet_node, 'digest', return_value='a' * 64), \
                patch.object(fleet_node, 'fault_module'), \
                patch.dict(fleet_node.__dict__, generation_backend=object(), orphan_backend=object()):
            for action in ('orphan-convert', 'orphan-install-tool', 'orphan-disarm', 'orphan-multipart'):
                with self.subTest(action=action), self.assertRaises(ValueError):
                    fleet_node.orphan_operation({'runId': 'gh-123-1-h1', 'action': action})

    def test_controller_rejects_clock_skew_policy_change_and_process_replacement(self):
        identity = self.identity()
        original = dict(identity=identity, nodeEpoch=1000, readOnly=True, clockChanged=False)
        fleet = types.SimpleNamespace(nodes=['node1'], record=lambda label, value: value,
                                      remote=lambda *a: original)
        binding = {'candidateBinarySha256': 'a' * 64}
        with patch.object(grace, 'time', types.SimpleNamespace(time=lambda: 1000)):
            previous = grace.clocks(fleet, binding, 'before')
            grace.clocks(fleet, binding, 'after', previous)
            for name, value in (('nodeEpoch', 1004), ('nodeEpoch', float('nan')),
                    ('nodeEpoch', True), ('readOnly', False), ('clockChanged', True)):
                changed = {**original, name: value}; fleet.remote = lambda *a: changed
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    grace.clocks(fleet, binding, 'poison')
            for name in fleet_node.PROCESS_FIELDS:
                changed = {**original, 'identity': {**identity, name: 'changed'}}
                fleet.remote = lambda *a: changed
                with self.subTest(field=name), self.assertRaises(ValueError):
                    grace.clocks(fleet, binding, 'replaced', previous)

    def test_remote_adapter_contains_registered_clock_and_descriptor_backends(self):
        with tempfile.TemporaryDirectory() as temporary:
            fleet = healthy.Fleet(['node1'], Path(temporary), 'gh-123-1-h1', runtime_profile='outcome-grace')
            for action in ('process-clock', 'orphan-observe', 'orphan-capture', 'orphan-arm', 'orphan-install', 'orphan-recover'):
                with patch.object(healthy.subprocess, 'run', return_value=types.SimpleNamespace(returncode=0, stdout='{}', stderr='')) as remote:
                    fleet.remote('node1', action)
                source = remote.call_args.kwargs['input']
                compile(source, '<remote-J02>', 'exec')
                self.assertIn('actions["process-clock"] = process_clock', source)
                if action.startswith('orphan-'):
                    self.assertIn('orphan_backend = types.ModuleType', source)
                    self.assertIn('generation_backend = types.ModuleType', source)


class GraceIntegrationTest(unittest.TestCase):
    def test_uncertain_original_grace_procedure_retains_namespace_and_stops_next_branch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); rc = root / 'rc'; rc.write_bytes(b'cli')
            args = types.SimpleNamespace(evidence=root / 'evidence', rc_bin=rc, binary_sha='b' * 64,
                testing_commit='a' * 40, tool_directory=root / 'tools')
            with ExitStack() as stack:
                factory = stack.enter_context(patch.object(outcome, 'Fleet'))
                fleet = factory.return_value
                fleet.reserved = ['node1']; fleet.record.side_effect = lambda label, value: value
                stack.enter_context(patch.dict(os.environ, GITHUB_RUN_ID='12345', GITHUB_RUN_ATTEMPT='1'))
                for name in ('Probe', 'Recorder', 'verify_inspector', 'verify_quorum_contract', 'validate_topology', 'verify_seed', 'idle_fleet', 'run_task'):
                    stack.enter_context(patch.object(outcome, name))
                stack.enter_context(patch.object(outcome, 'seed', return_value=[]))
                stack.enter_context(patch.object(outcome, 'validate_version_receipts', return_value={'bucketIncarnationId': INCARNATION}))
                stack.enter_context(patch.object(outcome, 'original_grace_probe', side_effect=TimeoutError('lost replay ACK')))
                result = outcome.run_branch(args, 'H1', ['node1'], 'DB', 'deferred',
                    {'source_sha': outcome.CANDIDATE, 'package_sha256': 'c' * 64})
            self.assertEqual(factory.call_args.kwargs['runtime_profile'], 'outcome-grace')
            self.assertEqual((result['verdict'], result['cleanup']), ('FAIL', 'FAIL'))
            fleet.cleanup.assert_not_called()
            retained = next(call.args[1] for call in fleet.record.call_args_list if call.args[0] == 'retain-unknown-seed-receipt')
            self.assertEqual(retained['request']['operation'], 'original-version-specific-grace-source-procedure')
            self.assertTrue(retained['namespacesRetained'])
            self.assertGreaterEqual(fleet.stop.call_count, 2)

    def test_manual_diagnostic_entry_cannot_certify_canonical_even_if_driver_returns_zero(self):
        root = Path(__file__).resolve().parent
        dry = subprocess.run(['bash', str(root / 'scripts/validation_heal/run_case.sh'), '--validation-case',
            outcome.CASE_ID, '--dry-run'], capture_output=True, text=True)
        self.assertEqual(dry.returncode, 0, dry.stderr)
        shell = (root / 'scripts/validation_heal/run_case.sh').read_text()
        verdict = shell[shell.index('verdict=FAIL reason='):shell.index('args=("${ROOT}/scripts/validation_result.py"')]
        with tempfile.TemporaryDirectory() as temporary:
            evidence = Path(temporary)
            for code, all_pass, cleanup in ((0, True, 'PASS'), (1, True, 'PASS'), (1, False, 'PASS'), (0, True, 'FAIL')):
                (evidence / 'healthy-summary.json').write_text(json.dumps(dict(diagnosticBranchesPassed=all_pass,
                    complete=False, canonicalCasePassClaimed=False)))
                script = verdict + '\nprintf "%s" "$verdict"\n'
                checked = subprocess.run(['bash', '-euc', script], capture_output=True, text=True,
                    env={**os.environ, 'case_id': outcome.CASE_ID, 'cleanup_status': cleanup,
                         'case_rc': str(code), 'evidence': str(evidence)})
                self.assertEqual(checked.returncode, 0, checked.stderr)
                self.assertEqual(checked.stdout, 'BLOCKED' if cleanup == 'PASS' and (code == 0 or all_pass)
                                 else 'INFRA_ERROR' if code == 0 else 'FAIL')


class GraceProtocolTest(unittest.TestCase):
    def execute(self, *, nodes=1, mode='normal'):
        initial, authority, injected, target, directory = scene(nodes)
        clock = types.SimpleNamespace(now=1000.0, elapsed=0.0)
        events, records, admissions = [], {}, []
        ack_entries = []
        def sleep(seconds):
            self.assertLessEqual(seconds, 1)
            if mode != 'stalled-clock': clock.now += seconds
            clock.elapsed += seconds; events.append(('wait', seconds))
        timer = types.SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.elapsed, sleep=sleep)
        identity = GraceRuntimeTest().identity()
        class Fleet:
            def __init__(self): self.nodes = list(initial); self.orphan_faults = []; self.phase = 'initial'
            def start(self): events.append(('start', clock.now))
            def stop(self): events.append(('stop', clock.now))
            def record(self, label, value): records[label] = copy.deepcopy(value); return value
            def remote(self, node, action, **params):
                events.append((action, node))
                if action == 'orphan-observe':
                    snapshot = initial if self.phase == 'initial' else authority
                    return dict(present=True, files=grace.object_files(snapshot, BUCKET, grace.KEY)[target], nodeEpoch=clock.now)
                if action == 'orphan-install':
                    if mode == 'miss-admission': clock.now = 1111
                    self.phase = 'fault'
                if action == 'process-clock':
                    actual = {**identity, 'pid': 124} if mode == 'replacement-after-compensation' and len(tasks) >= 3 else identity
                    return dict(identity=actual, nodeEpoch=clock.now, readOnly=True, clockChanged=False)
                return {}
            def snapshot(self, label, **kwargs):
                records[label] = self.phase
                return copy.deepcopy(initial if self.phase == 'initial' else authority if self.phase in ('authority', 'clean') else injected)
        fleet = Fleet()
        class Probe:
            def inventory(self, *args, **kwargs): return {'bucket': BUCKET, 'entries': copy.deepcopy(ack_entries)}
            def s3(self, operation, params, body=None):
                self_outer.assertEqual(operation, 'head_object')
                return dict(acknowledged=True, resultUnknown=False, response=dict(VersionId=params['VersionId'],
                    ContentLength=grace.SIZE, ETag='etag', LastModified=datetime.fromtimestamp(1000, timezone.utc)))
        self_outer = self
        class Recorder:
            def __init__(self, *args): self.versions = iter((OLD, CURRENT))
            def call(self, operation, params, body=None):
                events.append((operation, clock.now))
                if operation == 'put_object':
                    version = next(self.versions)
                    for entry in ack_entries: entry['isLatest'] = False
                    ack_entries.append(dict(key=grace.KEY, versionId=version, isLatest=True, deleteMarker=False,
                        payload=dict(size=len(body), sha256=hashlib.sha256(body).hexdigest())))
                    return dict(VersionId=version, ETag='etag')
                if operation == 'delete_object':
                    fleet.phase = 'authority'
                    ack_entries[:] = [entry for entry in ack_entries if entry['versionId'] != OLD]
                    return dict(VersionId=OLD, ResponseMetadata={'HTTPStatusCode': 204})
                return {}
        tasks = []
        def task(*args, **kwargs):
            tasks.append(clock.now); events.append(('task', clock.now))
            if len(tasks) == 1: return terminal(healthy_only=True)
            token = 'same' if mode == 'reused-token' else 'token-' + str(len(tasks))
            admission = {'clientToken': token}; kwargs['admissions'].append(admission); admissions.append(admission)
            if len(tasks) == 2:
                if mode == 'late-original': clock.now = 1121
                if mode == 'late-cleanup': clock.now = 1121; fleet.phase = 'clean'; return terminal(cleanup=True)
                if mode == 'partial-original':
                    fleet.phase = 'clean'
                    raise ValueError('partial outcome')
                return terminal()
            if mode != 'false-cleanup': fleet.phase = 'clean'
            return terminal(cleanup=True)
        def preserve(*args): events.append(('preserve', clock.now))
        def verify_public(*args):
            self.assertEqual(fleet.phase, 'clean'); events.append(('public', clock.now))
        def recover(*args):
            events.append(('rescue', clock.now)); fleet.phase = 'authority'; fleet.orphan_faults.clear()
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            for name, value in {'time': timer, 'Recorder': Recorder,
                    'run_task': task, 'idle_fleet': lambda *a: None, 'verify_proof': lambda *a: {},
                    'map_target': lambda *a, **kw: {'dataDir': directory}}.items():
                stack.enter_context(patch.object(grace, name, value))
            stack.enter_context(patch.object(grace.orphan, 'verify_public', verify_public))
            stack.enter_context(patch.object(grace.orphan, 'recover_all', recover))
            self.events, self.fleet = events, fleet
            result = grace.run(Probe(), fleet, dict(bucketPrefix=BUCKET, candidateBinarySha256='a' * 64,
                endpoints={node: 'http://' + node for node in fleet.nodes}), Path(temporary), [], INCARNATION, [], preserve)
        return result, records, tasks, events

    def test_natural_expiry_new_token_and_product_cleanup_precede_get_and_rescue(self):
        for nodes in (1, 4):
            result, records, tasks, events = self.execute(nodes=nodes)
            self.assertEqual(result[2]['verdict'], 'PASS')
            self.assertFalse(result[2]['canonicalPassClaimed'])
            self.assertEqual(result[2]['physicalFixture']['allOriginalMembers'], 4 * nodes)
            self.assertGreaterEqual(tasks[2], 1125)
            kinds = [event[0] for event in events]
            self.assertLess(kinds.index('public'), kinds.index('rescue'))
            self.assertEqual(kinds.count('preserve'), 2)
            # No service restart across original, wait, compensation, public checks or token rechecks.
            first, last = kinds.index('wait'), kinds.index('public')
            self.assertFalse(set(kinds[first:last + 1]) & {'start', 'stop'})
            self.assertEqual(records['J02-grace-product-cleaned-before-any-GET-or-rescue'], 'clean')
            self.assertEqual(self.fleet.orphan_faults, [])

    def test_missed_admission_or_terminal_window_remains_not_observed(self):
        for mode in ('miss-admission', 'late-original', 'late-cleanup'):
            result, _, tasks, events = self.execute(mode=mode)
            self.assertEqual(result[2]['verdict'], 'NOT_OBSERVED')
            self.assertEqual(len(tasks), 1 if mode == 'miss-admission' else 2)
            self.assertNotIn('public', [event[0] for event in events])
            self.assertEqual(self.fleet.orphan_faults, [])

    def test_reused_token_false_cleanup_and_partial_truth_never_rescue_or_pass(self):
        for mode in ('reused-token', 'false-cleanup', 'partial-original', 'stalled-clock', 'replacement-after-compensation'):
            with self.subTest(mode=mode), self.assertRaises(ValueError): self.execute(mode=mode)
            self.assertNotIn('rescue', [event[0] for event in self.events])
            self.assertNotIn('public', [event[0] for event in self.events])
            self.assertEqual(len(self.fleet.orphan_faults), 1)




class GraceRecoveryTest(unittest.TestCase):
    def fixture(self, root):
        from scripts.validation_heal import outcome_recovery as recovery
        keys = ['objects/0000.bin', 'objects/0001.bin', 'objects/0002.bin', grace.KEY]
        original, bucket, _ = versioned_scene(keys)
        value = original.pop('node1'); original['vm000'] = value
        expected = []
        for key in keys:
            ids = (OLD, CURRENT) if key == grace.KEY else ('null',)
            expected += [dict(key=key, versionId=version, isLatest=index == len(ids) - 1,
                deleteMarker=False, payload=dict(size=100, sha256='d' * 64)) for index, version in enumerate(ids)]
            for entry in value['files']:
                if entry['path'] == bucket + '/' + key + '/xl.meta':
                    entry['metadata']['versions'] = entry['metadata']['versions'][:len(ids)]
                    for index, version in enumerate(entry['metadata']['versions']):
                        version.update(versionId=ids[index], isLatest=index == len(ids) - 1)
        candidate = dict(source_sha=recovery.CANDIDATE, package_sha256='a' * 64)
        run_id = 'gh-12345-1-h1'; case = outcome.CASE_ID
        folder = root / 'artifact/run' / case
        evidence = folder / 'evidence' / case; branch = evidence / 'DB/H1/deferred'
        branch.mkdir(parents=True); (folder / 'cases').mkdir()
        def save(path, data): path.write_text(json.dumps(data))
        save(folder / 'cases' / (case + '.json'), dict(case_id=case, verdict='FAIL', cleanup_status='FAIL',
            provenance=dict(source_commit=candidate['source_sha'], candidate_sha256='a' * 64,
                testing_commit='b' * 40, binary_sha256='c' * 64)))
        rows = [dict(topology='H1', fixture='DB', branch=name, verdict='FAIL', cleanup='PASS')
                for name in ('mixed-healthy-repaired-failed', 'dry-run')]
        rows.append(dict(topology='H1', fixture='DB', branch='deferred', verdict='FAIL', cleanup='FAIL',
            failure=dict(type='FixtureStateMismatch', message='baseline content/version/marker differs from deterministic fixture ACK')))
        save(evidence / 'healthy-summary.json', dict(caseId=case, results=rows, cleanup='FAIL', complete=False, canonicalCasePassClaimed=False))
        owner = dict(runId=run_id, bucket=bucket, runtimeProfile='outcome-grace', binarySha256='c' * 64, mounts=[{}] * 4)
        for index, (label, data) in enumerate((('binding', dict(runId=run_id, bucketPrefix=bucket,
                harnessCommit='b' * 40, candidateCommit=candidate['source_sha'], candidatePackageSha256='a' * 64,
                candidateBinarySha256='c' * 64)), ('failure-scene-before-fault-rescue', original),
                ('reserved', dict(node='vm000', reserved=True, owner=owner)),
                ('retain-unknown-seed-receipt', dict(state='DIRTY', namespacesRetained=True,
                    request=dict(operation='original-version-specific-grace-source-procedure', bucket=bucket))),
                ('complete-ACK-manifest', dict(expected=expected[:3]))), 1):
            save(branch / (f'{index:05d}-' + label + '.json'), data)
        for name, operations in (('fixture', ['create_bucket'] + ['put_object'] * 3),
                                 ('original-grace-fixture', ['put_bucket_versioning', 'put_object', 'put_object'])):
            ledger = branch / name; ledger.mkdir()
            for sequence, operation in enumerate(operations, 1):
                params = {'Bucket': bucket}; payload = None; response = {'ResponseMetadata': {'HTTPStatusCode': 200}}
                if operation == 'put_object':
                    entry = expected[sequence - 2 if name == 'fixture' else sequence + 1]
                    params['Key'] = entry['key']; payload = entry['payload']; response['VersionId'] = entry['versionId']
                request = dict(sequence=sequence, runId=run_id, operation=operation, params=params)
                if payload: request['payload'] = payload
                path = ledger / f'{sequence:06d}-request.json'; save(path, request)
                save(ledger / f'{sequence:06d}-ack.json', dict(sequence=sequence, runId=run_id,
                    requestFile=path.name, requestSha256=hashlib.sha256(path.read_bytes()).hexdigest(), operationCheck='PASS',
                    observation=dict(operation=operation, params=params, payload=payload, response=response,
                        acknowledged=True, clientCallInvoked=True, resultUnknown=False)))
        return candidate, branch

    def test_acknowledged_pre_replay_plan_and_poisoned_ledger_boundaries(self):
        from scripts.validation_heal import outcome_recovery as recovery
        for poison in (None, 'unknown', 'request-digest', 'extra-delete', 'later-grace', 'foreign-key', 'lost-root', 'another-retained-branch'):
            with self.subTest(poison=poison), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); candidate, branch = self.fixture(root)
                path = branch / 'original-grace-fixture/000003-ack.json'
                ack = json.loads(path.read_text())
                if poison == 'unknown': ack['observation']['resultUnknown'] = True
                elif poison == 'request-digest': ack['requestSha256'] = 'e' * 64
                elif poison == 'foreign-key': ack['observation']['params']['Key'] = 'foreign'
                path.write_text(json.dumps(ack))
                if poison == 'extra-delete': (path.parent / '000004-request.json').write_text('{}')
                elif poison == 'later-grace': (branch / '00100-J02-grace-single-member-old-replay.json').write_text('{}')
                elif poison == 'lost-root':
                    path = branch / '00002-failure-scene-before-fault-rescue.json'; scene = json.loads(path.read_text())
                    scene['vm000']['formats'].pop(); path.write_text(json.dumps(scene))
                elif poison == 'another-retained-branch':
                    path = branch.parents[2] / 'healthy-summary.json'; summary = json.loads(path.read_text())
                    summary['results'][0]['cleanup'] = 'FAIL'; path.write_text(json.dumps(summary))
                if poison:
                    with self.assertRaises(ValueError): recovery.acknowledged_grace_plan(root, 12345, candidate)
                else:
                    plan = recovery.acknowledged_grace_plan(root, 12345, candidate)[0]
                    self.assertEqual(plan['nodes'], ['vm000'])
                    self.assertEqual(plan['owners']['vm000']['runtimeProfile'], 'outcome-grace')

    def test_real_descriptor_archive_requires_complete_scene_and_retries_after_every_move(self):
        from scripts.validation_heal import retained_scene as retained, outcome_recovery as recovery
        import test_validation_heal_retained_scene as retained_tests
        helper = retained_tests.RetainedSceneTests()
        for lost_after in range(1, 6):
            with self.subTest(lost_after=lost_after), tempfile.TemporaryDirectory() as temporary:
                state, roots, node, request = helper.scene(temporary)
                roots[0].mkdir(parents=True); owner = request['expectedOwner']; owner['runtimeProfile'] = 'outcome-grace'
                for folder in (*roots, state): (folder / 'owner.json').write_text(json.dumps(owner))
                snapshot = dict(files=[dict(path='original', sha256='d' * 64)], formats=[{}] * 4,
                    pid=0, quiescent=True, binarySha256=owner['binarySha256'])
                node['snapshot'] = lambda value: snapshot
                request['expectedOriginalSceneSha256'] = recovery.digest({key: snapshot[key] for key in ('files', 'formats')})
                with helper.root_stats(), patch.object(retained, 'rename_new', side_effect=helper.local_rename):
                    baseline = retained.run(request, node)['baseline']
                    change = {**request, 'action': 'retained-scene-isolate', 'expectedBaseline': baseline}
                    count = 0
                    def move(source, target):
                        nonlocal count
                        helper.local_rename(source, target)
                        if target.is_dir():
                            count += 1
                            if count == lost_after: raise RuntimeError('lost rename ACK')
                    with patch.object(retained, 'rename_new', side_effect=move), self.assertRaises(RuntimeError): retained.run(change, node)
                    checked = retained.run(change, node)
                    self.assertEqual(checked['locations'], ['retained'] * 5)
                    self.assertEqual(checked['baseline'], baseline)
                    self.assertFalse(checked['canonicalPassClaimed'])
                    self.assertFalse(checked['backupRestorationUsed'])
                    self.assertFalse(checked['credentialContentsRead'])
                    with self.assertRaises(ValueError): retained.run({**request, 'expectedOriginalSceneSha256': 'e' * 64}, node)
                    def remote(host, action, **params):
                        return retained.run(dict(runId=request['runId'], action=action, **params), node)
                    fleet = types.SimpleNamespace(record=lambda label, value: value, remote=remote)
                    plan = dict(runId=request['runId'], before={'vm000': snapshot}, owners={'vm000': owner},
                                originalCaseSha256='d' * 64)
                    result = recovery.isolate_acknowledged_grace(fleet, plan, [dict(node='vm000',
                        retained={'state': True, 'roots': [True] * 4})])
                    self.assertEqual(result['cleanup'], 'ISOLATED')
                    self.assertFalse(result['originalBusinessFixtureRestored'])
                    self.assertFalse(result['originalVerdictRewritten'])

    def test_drifted_acknowledged_scene_never_starts_archive(self):
        from scripts.validation_heal import outcome_recovery as recovery
        for poison in ('bytes', 'missing-root', 'live-pid'):
            with self.subTest(poison=poison):
                before = {'vm000': dict(files=[], formats=[], pid=0, quiescent=True, binarySha256='a' * 64)}
                current = copy.deepcopy(before)
                state = dict(node='vm000', state={'exists': True}, roots=[{'exists': True}] * 4, retained={})
                if poison == 'bytes': current['vm000']['files'] = [{}]
                elif poison == 'live-pid': current['vm000']['pid'] = 123
                else: state['roots'] = [{'exists': False}] * 4
                fleet = types.SimpleNamespace(record=lambda label, value: value,
                    remote=unittest.mock.Mock(), snapshot=lambda *a, **kw: current)
                plan = dict(before=before, binarySha256='a' * 64)
                with self.assertRaises(ValueError): recovery.isolate_acknowledged_grace(fleet, plan, [state])
                self.assertFalse(any(call.args[1].startswith('retained-scene-') for call in fleet.remote.call_args_list))


if __name__ == '__main__': unittest.main()
