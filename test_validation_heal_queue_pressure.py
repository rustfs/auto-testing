"""Reject false queue admission, responsibility retirement and resource proofs."""

import copy
from contextlib import ExitStack
from datetime import datetime, timezone
import json
from pathlib import Path
import stat
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import queue_pressure as queue
from scripts.validation_heal.fleet_node import runtime_config
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
from scripts.validation_heal import drive_gate, fleet_node
import test_validation_heal_persistence_mrf as mrf_gate_fixtures


class QueuePressureTests(unittest.TestCase):
    def durable_gate_fixture(self, root):
        state, disk, bucket, system = mrf_gate_fixtures.MrfCorruptionTests().bucket_gate_fixture(root)
        prefix = bucket.name
        durable = bucket.with_name(prefix + '-durable')
        bucket.rename(durable)
        (state / 'owner.json').write_text(json.dumps({'runtimeProfile': 'queue-pressure', 'bucket': prefix}))
        for i in range(4): (disk / 'data' / (prefix + f'-queue-{i}')).mkdir()
        return state, disk, durable, system

    def test_actual_durable_gate_preserves_system_and_queue_buckets_through_activation_and_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, disk, durable, system = self.durable_gate_fixture(Path(tmp))
            controls = [system, disk / 'data', *list((disk / 'data').glob('*-queue-*'))]
            original = {p: (p.stat().st_mode, p.stat().st_ino) for p in controls}
            mode = stat.S_IMODE(durable.stat().st_mode)
            with mrf_gate_fixtures.MrfCorruptionTests().rooted_lstat():
                value = drive_gate.apply_gate(state, [disk], 0, 'permission-denied', scope='durable-bucket')
                self.assertEqual(value['dataPath'], str(durable))
                self.assertEqual(value['scope'], 'durable-bucket')
                drive_gate.activate_gate(state, [disk])
                self.assertEqual(stat.S_IMODE(durable.stat().st_mode), 0)
                self.assertEqual({p: (p.stat().st_mode, p.stat().st_ino) for p in controls}, original)
                self.assertEqual((system / 'format.json').read_bytes(), b'format-control')
                self.assertTrue(drive_gate.release_gate(state, [disk])['originalPermissionsRestored'])
            self.assertEqual(stat.S_IMODE(durable.stat().st_mode), mode)

    def test_durable_gate_rejects_foreign_owner_scope_symlink_and_marker_redirection(self):
        for kind in ('profile', 'prefix', 'symlink', 'marker'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                state, disk, durable, system = self.durable_gate_fixture(Path(tmp))
                owner = json.loads((state / 'owner.json').read_text())
                if kind == 'profile': owner['runtimeProfile'] = 'flapping'
                elif kind == 'prefix': owner['bucket'] = '../foreign'
                elif kind == 'symlink':
                    saved = durable.with_name(durable.name + '-saved')
                    durable.rename(saved)
                    durable.symlink_to(saved, target_is_directory=True)
                (state / 'owner.json').write_text(json.dumps(owner))
                with mrf_gate_fixtures.MrfCorruptionTests().rooted_lstat():
                    if kind == 'marker':
                        drive_gate.apply_gate(state, [disk], 0, 'permission-denied', scope='durable-bucket')
                        p = state / 'drive-gate.json'
                        value = json.loads(p.read_text())
                        value['dataPath'] = str(system)
                        p.write_text(json.dumps(value));p.chmod(0o600)
                        with self.assertRaises(ValueError): drive_gate.activate_gate(state, [disk])
                        with self.assertRaises(ValueError): drive_gate.release_gate(state, [disk])
                    else:
                        with self.assertRaises(ValueError):
                            drive_gate.apply_gate(state, [disk], 0, 'permission-denied', scope='durable-bucket')
                        self.assertFalse((state / 'drive-gate.json').exists())

    def test_live_arm_selects_fixed_durable_scope_only_for_queue_pressure(self):
        before = {name: {'pid': 10, 'startTicks': 100}.get(name, name) for name in fleet_node.PROCESS_FIELDS}
        request = {'runId': 'owned', 'action': 'drive-gate-arm-live', 'disk': 0, 'expected': before}
        for profile, expected in [('queue-pressure', {'scope': 'durable-bucket'}), ('flapping', {})]:
            calls = []
            gate = types.SimpleNamespace(validate_live_boundary=lambda pid: None,
                apply_gate=lambda *args, **kwargs: calls.append(kwargs) or {'state': 'ARMED'})
            with patch.object(fleet_node, 'owned', return_value=(Path('/owned'), [Path('/disk')], {'runtimeProfile': profile})), \
                 patch.object(fleet_node, 'process', return_value=before), \
                 patch.object(fleet_node, 'gate_module', return_value=gate), \
                 patch.object(fleet_node, 'quorum_gate_module', return_value=types.SimpleNamespace(checked=lambda *args: None)):
                self.assertTrue(fleet_node.drive_gate_arm_live(request)['sameOriginalProcess'])
            self.assertEqual(calls, [expected])

    def test_actual_durable_scope_probes_business_denial_and_both_system_controls_in_same_namespace(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, disk, durable, _ = self.durable_gate_fixture(Path(tmp))
            denied = {'uid': 0, 'gid': 0, 'errno': 13}
            control = {'uid': 0, 'gid': 0, 'errno': 0}
            value = {'mode': 'permission-denied', 'scope': 'durable-bucket', 'phase': 'active',
                     'dataPath': str(durable), 'disk': 0}
            outputs = [types.SimpleNamespace(returncode=0, stdout=json.dumps(v)) for v in (denied, denied, control, control)]
            with mrf_gate_fixtures.MrfCorruptionTests().rooted_lstat(), \
                 patch.object(drive_gate, 'checked_gate', return_value=value), \
                 patch.object(drive_gate, 'process_identity', return_value={'uid': 0, 'gid': 0, 'capEff': 0}), \
                 patch.object(Path, 'readlink', side_effect=[Path('mnt:[candidate]'), Path('mnt:[host]')]), \
                 patch.object(drive_gate.subprocess, 'run', side_effect=outputs) as invoke:
                proof = drive_gate.probe_gate(state, [disk], 123)
            self.assertTrue(proof['systemMetadataAccessible'])
            self.assertEqual(proof['scope'], 'durable-bucket')
            self.assertEqual(len(invoke.call_args_list), 4)
            for call in invoke.call_args_list:
                self.assertIn('--mount=/proc/123/ns/mnt', call.args[0])
                self.assertIn('--bounding-set=-dac_override,-dac_read_search', call.args[0])
            self.assertTrue(invoke.call_args_list[1].args[0][-1].endswith('-durable/objects/0000.bin/xl.meta'))
            self.assertTrue(invoke.call_args_list[2].args[0][-1].endswith('/data/.rustfs.sys/.validation-mrf-write-probe'))
            self.assertTrue(invoke.call_args_list[3].args[0][-1].endswith('/data/.rustfs.sys/format.json'))

    def test_durable_refusal_requires_both_successful_system_controls_and_exact_owned_path(self):
        run_id, bucket = 'gh-123-1-h1', 'rv-heal-0123456789abcdef-durable'
        control = {'errno': 0, 'uid': 0, 'gid': 0}
        value = {'scope': 'durable-bucket', 'disk': 0, 'errno': 13, 'name': 'EACCES', 'readProbe': {'errno': 13},
            'dataPath': f'/data/rustfs1/heal-validation/{run_id}/data/{bucket}', 'systemMetadataAccessible': True,
            'systemMetadataControl': {'sameServiceIdentity': True, 'read': dict(control), 'write': dict(control)},
            'sameServiceIdentity': True, 'privateMountNamespace': True, 'dacBypassRemoved': True}
        queue.validate_durable_gate(value, run_id, bucket)
        for kind in ('scope', 'path', 'disk', 'read', 'write', 'control', 'identity', 'namespace', 'dac', 'flag', 'false-success'):
            bad = copy.deepcopy(value)
            if kind == 'scope': bad['scope'] = 'disk'
            elif kind == 'path': bad['dataPath'] = bad['dataPath'].removesuffix('-durable')
            elif kind == 'disk': bad['disk'] = 1
            elif kind in {'read', 'write'}: bad['systemMetadataControl'][kind]['errno'] = 13
            elif kind == 'control': bad['systemMetadataControl'].pop('read')
            elif kind == 'identity': bad['systemMetadataControl']['write']['uid'] = 1000
            elif kind == 'namespace': bad['privateMountNamespace'] = False
            elif kind == 'dac': bad['dacBypassRemoved'] = False
            elif kind == 'flag': bad['systemMetadataAccessible'] = False
            else: bad['systemMetadataControl']['read']['errno'] = False
            with self.subTest(kind=kind), self.assertRaises(ValueError): queue.validate_durable_gate(bad, run_id, bucket)

    def status(self):
        return {"clusterStatusComplete": True, "coverage": {"expected": 1, "responded": 1, "unknown": 0},
            "healQueueLength": 2, "healActiveTasks": 1, "healOperations": {"queueLength": 2,
                "activeTasks": 1, "retryingTasks": 0, "queuedByPriority": {"high": 2, "normal": 0},
                "activeByPriority": {"high": 1}, "queuedBySource": {"admin": 2}, "activeBySource": {"admin": 1},
                "admission": {"accepted": 3, "full": 1, "dropped": 1, "merged": 1}}}

    def terminal(self):
        expected = [{"key": "objects/0", "versionId": "null"}, {"key": "objects/1", "versionId": "null"}]
        rows = [{"identity": {"bucket": "owned", "object": entry["key"], "versionId": "null",
                    "poolIndex": 0, "setIndex": 0, "bucketIncarnationId": "original-incarnation"},
                 "disposition": {"state": "repaired" if index else "verified_healthy"}}
                for index, entry in enumerate(expected)]
        current = {"summary": "finished", "outcome": {"execution": {"state": "completed"}, "coverage": "complete",
            "objectsTruncated": False, "objects": rows, "counters": {"processed": 2, "healed": 1, "unchanged": 1,
                "failed": 0, "skipped": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}}}
        return current, expected, {("objects/1", "null")}

    def durable(self):
        obligation = {"bucket": "owned", "key": "offline/new", "versionId": "null", "poolIndex": 0, "setIndex": 0, "kind": 3}
        intent = {**obligation, "intentDigest": "original-digest", "lifecycle": {"responsibility_id": "original-id",
            "source_bucket_incarnation_id": "original-incarnation", "state": {"state": "active"}, "last_operator_acceptance": None}}
        records = {"node": {"present": True, "owner": "checkpoint-owner", "sequence": 9, "intents": [intent]}}
        bindings = {queue.identity(obligation): [{"node": "node", "checkpointOwner": "checkpoint-owner", "sequence": 8,
            "responsibilityId": "original-id", "bucketIncarnationId": "original-incarnation", "intentDigest": "original-digest"}]}
        return records, [obligation], bindings

    def test_all_source_branches_and_automatic_registration(self):
        self.assertEqual(set(select(queue.CASE_ID)[0]["variants"]), set(queue.BRANCHES))
        self.assertIn(queue.CASE_ID, AUTOMATIC_CASES)

    def test_profile_has_finite_queue_and_actual_two_priorities(self):
        value = runtime_config("queue-pressure")
        self.assertEqual(value["RUSTFS_HEAL_QUEUE_SIZE"], "2")
        self.assertEqual(value["RUSTFS_HEAL_MAX_CONCURRENT_HEALS"], "1")
        self.assertEqual(value["RUSTFS_HEAL_MAX_CONCURRENT_PER_SET"], "1")
        self.assertEqual(value["RUSTFS_HEAL_MRF_ENABLE"], "true")
        self.assertEqual(value["RUSTFS_SCANNER_ENABLED"], "false")
        self.assertEqual(value["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertNotEqual(queue.REFERENCE["adminPriority"], queue.REFERENCE["partialWritePriority"])

    def test_full_has_exact_structured_rejection_and_no_admitted_task(self):
        observed = {"httpStatus": 503, "response": {"Code": "SlowDown", "Message": "heal request not admitted: admission=full, reason=none"}}
        expected = dict(admission="full", reason="none", code="SlowDown", http=503)
        self.assertEqual(queue.rejection(observed, **expected)["independentRepairCount"], 0)
        for changes in [{"httpStatus": 429}, {"transportError": "timeout"}, {"response": {**observed["response"], "clientToken": "invented"}},
                        {"response": {**observed["response"], "Message": "queue was busy"}}, {"response": {"Code": "OperationAborted"}}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                queue.rejection({**observed, **changes}, **expected)

    def test_dropped_xml_preserves_its_distinct_reason(self):
        value = {"httpStatus": 409, "response": "<Error><Code>OperationAborted</Code><Message>heal request not admitted: admission=dropped, reason=overlapping_paths</Message></Error>"}
        self.assertEqual(queue.rejection(value, admission="dropped", reason="overlapping_paths", code="OperationAborted", http=409)["reason"], "overlapping_paths")

    def test_queue_requires_all_peers_and_consistent_source_priority_counts(self):
        self.assertEqual(queue.queue_snapshot(self.status(), 1)["queueLength"], 2)
        for name in ["coverage", "global-count", "source-count", "priority-count", "capacity", "bool-count", "admission"]:
            value = self.status()
            if name == "coverage": value["coverage"]["unknown"] = 1
            elif name == "global-count": value["healQueueLength"] = 1
            elif name == "source-count": value["healOperations"]["queuedBySource"]["admin"] = 1
            elif name == "priority-count": value["healOperations"]["activeByPriority"]["high"] = 0
            elif name == "capacity": value["healOperations"]["queueLength"] = 3
            elif name == "bool-count": value["healOperations"]["activeTasks"] = True
            else: del value["healOperations"]["admission"]["full"]
            with self.subTest(name=name), self.assertRaises(ValueError): queue.queue_snapshot(value, 1)

    def test_complete_scope_counters_and_dispositions_are_independent_of_idle(self):
        value, expected, missing = self.terminal()
        self.assertEqual(queue.terminal(value, "owned", expected, missing)["canonicalCounters"]["healed"], 1)
        for name in ["coverage", "execution", "unknown", "attemptFailures", "wrong-bucket", "wrong-version", "wrong-set", "incarnation", "duplicate", "false-repair", "truncated"]:
            value, expected, missing = self.terminal()
            outcome = value["outcome"]
            if name == "coverage": outcome["coverage"] = "partial"
            elif name == "execution": outcome["execution"]["state"] = "completed_with_errors"
            elif name in {"unknown", "attemptFailures"}: outcome["counters"][name] = 1
            elif name == "wrong-bucket": outcome["objects"][0]["identity"]["bucket"] = "foreign"
            elif name == "wrong-version": outcome["objects"][0]["identity"]["versionId"] = "another-version"
            elif name == "wrong-set": outcome["objects"][0]["identity"]["setIndex"] = 1
            elif name == "incarnation": outcome["objects"][0]["identity"]["bucketIncarnationId"] = "different"
            elif name == "duplicate": outcome["objects"][1] = copy.deepcopy(outcome["objects"][0])
            elif name == "false-repair": outcome["objects"][0]["disposition"]["state"] = "repaired"
            else: outcome["objectsTruncated"] = True
            with self.subTest(name=name), self.assertRaises(ValueError): queue.terminal(value, "owned", expected, missing)

    def test_terminal_maps_only_reviewed_null_version_forms_to_exact_sdk_identity(self):
        for version in (None, "", "null", "00000000-0000-0000-0000-000000000000"):
            value, expected, missing = self.terminal()
            for row in value['outcome']['objects']: row['identity']['versionId'] = version
            with self.subTest(version=version):
                self.assertEqual(queue.terminal(value, 'owned', expected, missing)['diagnosticObjects'], 2)
        for version in (False, True, 0, [], {}, 'another-version', '01234567-89ab-4cde-8fab-0123456789ab'):
            value, expected, missing = self.terminal()
            value['outcome']['objects'][0]['identity']['versionId'] = version
            with self.subTest(version=version), self.assertRaises(ValueError):
                queue.terminal(value, 'owned', expected, missing)

    def test_real_orchestrator_finishes_durable_gate_before_finite_pressure_admission(self):
        for system_accessible in (True, False):
            with self.subTest(system_accessible=system_accessible), tempfile.TemporaryDirectory() as tmp:
                calls = []

                class Fleet:
                    def __init__(self, nodes, directory, run_id, *args, **kwargs):
                        self.nodes, self.run_id = nodes, run_id
                        self.faults, self.drive_gates, self.reserved = [], set(), True

                    def record(self, label, value):
                        if label == 'binding': self.binding = value
                        return value

                    def remote(self, node, action, **kwargs):
                        calls.append(action)
                        if action == 'drive-gate-probe':
                            control = {name: {'uid': 0, 'gid': 0, 'errno': 0} for name in ('read', 'write')}
                            return {'scope': 'durable-bucket', 'disk': 0,
                                'dataPath': f'/data/rustfs1/heal-validation/{self.run_id}/data/{self.binding["bucketPrefix"]}-durable',
                                'systemMetadataAccessible': system_accessible,
                                'systemMetadataControl': {**control, 'sameServiceIdentity': True},
                                'sameServiceIdentity': True, 'privateMountNamespace': True, 'dacBypassRemoved': True,
                                'name': 'EACCES', 'errno': 13, 'readProbe': {'errno': 13}}
                        return {}

                    def deploy(self, *args): pass
                    def snapshot(self, *args, **kwargs): return {}
                    def start(self): pass
                    def stop(self): pass
                    def cleanup(self): return True

                class Pressure:
                    def __init__(self, *args): pass
                    def admit(self, path):
                        calls.append('pressure-admission')
                        raise ValueError('end test at first pressure admission')

                root = Path(tmp)
                (root / 'evidence').mkdir()
                (root / 'candidate.json').write_text(json.dumps({
                    'source_sha': queue.REFERENCE['candidateCommit'], 'package_sha256': 'c' * 64}))
                (root / 'rc').write_bytes(b'unit-test-only')
                args = types.SimpleNamespace(evidence=root / 'evidence', candidate=root / 'candidate.json',
                    rc_bin=root / 'rc', tool_directory=root, testing_commit='a' * 40, binary_sha='b' * 64)
                target = {'node': 'vm000', 'disk': 0, 'relativePath': 'owned/objects/0/generation/part.1',
                          'sha256': 'a' * 64, 'key': 'objects/0', 'versionId': 'null'}
                with ExitStack() as patches:
                    replacements = {'Fleet': Fleet, 'Pressure': Pressure,
                        'Probe': lambda *args: types.SimpleNamespace(inventory=lambda *args, **kwargs: {}),
                        'Recorder': lambda *args: None, 'seed': lambda *args: [{'key': 'objects/0', 'versionId': 'null'}],
                        'verify_inspector': lambda *args: {}, 'verify_reference': lambda *args: {},
                        'verify_seed': lambda *args: {}, 'validate_topology': lambda *args: {},
                        'map_cohorts': lambda *args, **kwargs: [target], 'verify_part_manifest': lambda *args, **kwargs: {},
                        'validate_missing_batch': lambda *args: {}, 'physical_content': lambda *args: 'unchanged',
                        'read_checkpoints': lambda *args, **kwargs: {'vm000': {'intents': []}},
                        'JournalWindow': lambda *args, **kwargs: None}
                    for name, value in replacements.items(): patches.enter_context(patch.object(queue, name, value))
                    patches.enter_context(patch.dict(queue.os.environ, {'GITHUB_RUN_ID': '321', 'GITHUB_RUN_ATTEMPT': '1'}))
                    result = queue.run_topology(args, 'H1', ['vm000'])
                self.assertEqual(result['verdict'], 'FAIL')
                for action in ('drive-gate-arm-live', 'drive-gate-activate', 'drive-gate-probe'):
                    self.assertIn(action, calls)
                if system_accessible:
                    self.assertLess(calls.index('drive-gate-probe'), calls.index('pressure-admission'))
                    self.assertEqual(result['failure']['message'], 'end test at first pressure admission')
                else:
                    self.assertNotIn('pressure-admission', calls)

    def test_running_window_requires_partial_actual_processing(self):
        value = {"summary": "running", "outcome": {"execution": {"state": "running"}, "counters": {"processed": 5}}}
        self.assertTrue(queue.active_window(value, 10))
        for count in [0, 10, True, None]:
            value["outcome"]["counters"]["processed"] = count
            self.assertFalse(queue.active_window(value, 10))

    def test_queue_failure_keeps_exact_durable_owner_and_generation(self):
        records, obligations, bindings = self.durable()
        self.assertTrue(queue.retained(records, obligations, bindings))
        for name in ["absent", "owner", "sequence", "missing", "duplicate", "digest", "responsibility", "incarnation", "held", "operator", "version"]:
            records, obligations, bindings = self.durable()
            current = records["node"]; item = current["intents"][0]
            if name == "absent": current["present"] = False
            elif name == "owner": current["owner"] = "different"
            elif name == "sequence": current["sequence"] = 7
            elif name == "missing": current["intents"] = []
            elif name == "duplicate": current["intents"].append(copy.deepcopy(item))
            elif name == "digest": item["intentDigest"] = "changed"
            elif name == "responsibility": item["lifecycle"]["responsibility_id"] = "new"
            elif name == "incarnation": item["lifecycle"]["source_bucket_incarnation_id"] = "new"
            elif name == "held": item["lifecycle"]["state"] = {"state": "held"}
            elif name == "operator": item["lifecycle"]["last_operator_acceptance"] = {"actor": "operator"}
            else: item["versionId"] = "other-version"
            with self.subTest(name=name), self.assertRaises(ValueError): queue.retained(records, obligations, bindings)

    def test_mrf_full_is_real_source_priority_capacity_and_time(self):
        after = datetime(2026, 10, 11, tzinfo=timezone.utc)
        row = {"target": "rustfs::heal::manager", "event": "heal_queue_admission", "source": "mrf", "priority": "Normal",
            "result": "rejected_full", "queue_capacity": 2, "queue_len": 2, "request_id": "actual-request", "timestamp": "2026-10-11T01:00:00Z"}
        self.assertEqual(len(queue.mrf_full_events({"node": [row]}, ["node"], after)), 1)
        for changes in [{"source": "admin"}, {"priority": "High"}, {"result": "accepted"}, {"queue_len": 1},
                        {"queue_capacity": 99}, {"request_id": ""}, {"timestamp": "2026-10-10T23:59:59Z"}]:
            self.assertEqual(queue.mrf_full_events({"node": [{**row, **changes}]}, ["node"], after), [])
        self.assertEqual(queue.mrf_full_events({"foreign": [row]}, ["node"], after), [])

    def test_resource_ceiling_and_process_birth_are_preregistered(self):
        baseline = {"node": {"clockDomain": "boot", "monotonicNs": 1, "identity": {"pid": 12, "startTicks": 100}}}
        sample = {"node": {**baseline["node"], "monotonicNs": 2, "readOnly": True, "rssBytes": 123, "fileDescriptors": 30, "threads": 20}}
        self.assertTrue(queue.resources(sample, baseline)["withinFrozenLimits"])
        for name in ["pid", "clock", "time", "rssBytes", "fileDescriptors", "threads", "missing-node"]:
            current = copy.deepcopy(sample)
            if name == "pid": current["node"]["identity"]["pid"] = 13
            elif name == "clock": current["node"]["clockDomain"] = "another-boot"
            elif name == "time": current["node"]["monotonicNs"] = 1
            elif name == "missing-node": current = {}
            else: current["node"][name] = queue.RESOURCE_BOUNDS[name] + 1
            with self.subTest(name=name), self.assertRaises(ValueError): queue.resources(current, baseline)

    def test_request_budget_counts_failed_and_merged_requests(self):
        calls = []
        probe = types.SimpleNamespace(admin=lambda *args: calls.append(args) or {"httpStatus": 503})
        fleet = types.SimpleNamespace(nodes=[], record=lambda label, value: value)
        pressure = queue.Pressure(probe, fleet, {}, [])
        for _ in range(queue.MAX_ADMIN_REQUESTS): pressure.call("/owned")
        with self.assertRaises(ValueError): pressure.call("/owned")
        self.assertEqual(len(calls), queue.MAX_ADMIN_REQUESTS)

    def test_multi_bucket_physical_subset_keeps_every_format_and_control(self):
        snapshot = {"node": {"formats": ["actual-format"], "files": [{"path": "a/objects/0/xl.meta"}, {"path": "b/objects/0/part.1"}]}}
        a = queue.original_subset(snapshot, "a")
        self.assertEqual(a["node"]["formats"], snapshot["node"]["formats"])
        self.assertEqual(a["node"]["files"], [snapshot["node"]["files"][0]])
        self.assertEqual(queue.original_subset(snapshot, None, excluded="b"), a)
        self.assertEqual(len(snapshot["node"]["files"]), 2)


if __name__ == "__main__":
    unittest.main()
