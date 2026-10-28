"""An actual set checkpoint is distinct from a bucket/root scan and counters."""

import base64
import copy
import hashlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.validation_heal import checkpoint as reader, journal_fault as journal


TOKEN = "12345678-1234-4234-8234-123456789abc"
INCARNATION = "22345678-1234-4234-8234-123456789abc"
BUCKET = "rv-heal-0123456789abcdef"


def encoded(value, *, signed=False):
    value = copy.deepcopy(value)
    if signed:
        value["integrity_digest"] = None
        for field in ("processed_objects", "failed_objects", "skipped_objects"): value[field].sort()
        unsigned = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        value["integrity_digest"] = base64.b64encode(hashlib.sha256(unsigned).digest()).decode()
    raw = json.dumps(value).encode()
    return {"present": True, **journal.entry(raw), "rawBase64": base64.b64encode(raw).decode()}


class AdminCheckpointTests(unittest.TestCase):
    def fixture(self):
        value = {"schema_version": 8, "task_id": TOKEN, "checkpoint_time": 100,
            "current_bucket_index": 0, "current_object_index": 2,
            "processed_objects": ["objects/first", "objects/second"], "failed_objects": [], "skipped_objects": [],
            "successful_objects": 2, "failed_object_count": 0, "skipped_object_count": 0,
            "skipped_new_versions": 0, "skipped_ilm_expired": 0, "processed_bytes": 2,
            "total_objects": 3, "total_bytes": 3, "baseline_generation": None, "baseline_known": False,
            "counter_unknown": False, "integrity_digest": None,
            "admin": {"set_disk_id": "pool_0_set_0", "options": reader.OPTIONS, "buckets": {BUCKET: INCARNATION},
                "completed": False, "outcome": {"execution": {"state": "running"}, "coverage": "partial",
                    "objects": [], "objectsTruncated": False, "counters": {"processed": 2, "healed": 1, "unchanged": 1,
                        "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}}}}
        resume = {"schema_version": 7, "task_id": TOKEN, "task_type": "admin_erasure_set",
            "set_disk_id": "pool_0_set_0", "completed": False, "replacement_targets": [], "replacement_generation": None}
        rows = [{"disk": disk, "relativePath": name, "present": False}
                for disk in range(4) for name in journal.names("checkpoint", TOKEN)]
        rows[4].update(encoded(value, signed=True)); rows[5].update(encoded(resume))
        return {"node": {"readOnly": True, "quiescent": True, "family": "checkpoint", "taskId": TOKEN,
                         "directoryEnumerationUsed": False, "records": rows}}, value, resume

    def test_exact_original_checkpoint_resume_pair_has_one_physical_owner(self):
        observed, _, _ = self.fixture()
        owner = reader.owned_checkpoint(observed, TOKEN, BUCKET)
        self.assertEqual((owner["node"], owner["disk"], owner["bucketIncarnationId"]), ("node", 1, INCARNATION))
        self.assertTrue(owner["originalDurableResponsibility"])

    def test_root_or_bucket_scan_without_checkpoint_cannot_be_used_as_durable_progress(self):
        observed, _, _ = self.fixture()
        for row in observed["node"]["records"]: row["present"] = False
        with self.assertRaisesRegex(ValueError, "NOT_OBSERVED"): reader.owned_checkpoint(observed, TOKEN, BUCKET)

    def test_original_bytes_hash_exact_rows_and_unique_owner_are_required(self):
        for change in ("bytes", "hash", "duplicate-row", "second-owner", "wrong-disk", "resume-owner", "omitted-disk", "blocked"):
            observed, _, _ = self.fixture(); rows = observed["node"]["records"]
            if change == "bytes": rows[4]["rawBase64"] = base64.b64encode(b"changed").decode()
            elif change == "hash": rows[4]["sha256"] = "b" * 64
            elif change == "duplicate-row": rows[0] = copy.deepcopy(rows[4])
            elif change == "second-owner": observed["other"] = copy.deepcopy(observed["node"])
            elif change == "wrong-disk": rows[4]["disk"] = True
            elif change == "resume-owner": rows[1].update({key: rows[5][key] for key in ("present", "size", "sha256", "rawBase64")}); rows[5]["present"] = False
            elif change == "omitted-disk": rows.pop()
            else: rows[6].update(encoded({"blocked": True}))
            with self.subTest(change=change), self.assertRaises(ValueError): reader.owned_checkpoint(observed, TOKEN, BUCKET)

    def test_wrong_scope_incarnation_options_and_fabricated_progress_are_rejected(self):
        changes = [("set_disk_id", "pool_0_set_1"), ("options", {**reader.OPTIONS, "pool": True}),
            ("options", {**reader.OPTIONS, "nolock": True}), ("buckets", {"foreign": INCARNATION}),
            ("buckets", {BUCKET: None}), ("completed", True)]
        for key, new in changes:
            observed, value, _ = self.fixture(); value["admin"][key] = new
            observed["node"]["records"][4].update(encoded(value, signed=True))
            with self.subTest(key=key, new=new), self.assertRaises(ValueError): reader.owned_checkpoint(observed, TOKEN, BUCKET)
        observed, value, _ = self.fixture()
        for name in ("processed", "healed", "unchanged"): value["admin"]["outcome"]["counters"][name] = 0
        observed["node"]["records"][4].update(encoded(value, signed=True))
        with self.assertRaisesRegex(ValueError, "NOT_OBSERVED"): reader.owned_checkpoint(observed, TOKEN, BUCKET)

    def test_replacement_or_completed_resume_cannot_be_rebound_to_admin_task(self):
        for key, new in [("task_id", INCARNATION), ("task_type", "erasure_set"), ("schema_version", 6),
                         ("set_disk_id", "pool_1_set_0"), ("completed", True), ("replacement_generation", TOKEN)]:
            observed, _, resume = self.fixture(); resume[key] = new
            observed["node"]["records"][5].update(encoded(resume))
            with self.subTest(key=key), self.assertRaises(ValueError): reader.owned_checkpoint(observed, TOKEN, BUCKET)

    def test_live_census_requires_two_stable_reads_bound_to_original_process(self):
        observed, _, _ = self.fixture(); value = observed["node"]
        value.update(quiescent=False, stable=True, processBound=True, independentReads=2)
        reader.owned_checkpoint(observed, TOKEN, BUCKET)
        for key, new in [("stable", False), ("processBound", False), ("independentReads", 1)]:
            changed = copy.deepcopy(observed); changed["node"][key] = new
            with self.subTest(key=key), self.assertRaises(ValueError): reader.owned_checkpoint(changed, TOKEN, BUCKET)

    def test_set_admission_keeps_explicit_selectors_and_actual_canonical_token(self):
        active = []; calls = []
        fleet = SimpleNamespace(record=lambda label, value: value)
        probe = SimpleNamespace(admin=lambda path, query, body: calls.append((path, query, json.loads(body)))
                                or {"httpStatus": 200, "response": {"clientToken": TOKEN}})
        self.assertEqual(reader.admit(probe, fleet, active), TOKEN)
        self.assertEqual(calls, [(reader.PATH, [], reader.OPTIONS)])
        self.assertEqual(active, [(reader.PATH, TOKEN)])

    def test_earlier_capture_requires_original_bytes_bound_to_process_and_physical_owner(self):
        observed, _, _ = self.fixture()
        owner = reader.owned_checkpoint(observed, TOKEN, BUCKET)
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        identity = {name: name for name in PROCESS_FIELDS}
        identity.update(pid=50, state="S", binarySha256="a" * 64)
        receipt = {"processBound": True, "originalProductBytes": True, "productMetadataMutated": False,
            "runId": "gh-12345-1-h1", "family": "checkpoint", "taskId": TOKEN, "disk": 1,
            "relativePath": owner["checkpoint"]["relativePath"], "faultId": "F07-early", "binarySha256": "a" * 64,
            "captured": journal.entry(reader.bytes_of(owner["checkpoint"]))}
        requests = []
        fleet = SimpleNamespace(run_id=receipt["runId"], frozen_processes={"node": identity}, record=lambda label, value: value,
            remote=lambda node, action, **params: requests.append((node, action, params)) or
                (identity if action == "process" else receipt))
        with patch.object(reader, "freeze", return_value=(owner, identity)):
            result = reader.capture_earlier(fleet, TOKEN, BUCKET, "F07-early")
            self.assertEqual(result["captureId"], "F07-early")
            self.assertEqual(requests[0], ("node", "journal-capture", {
                "family": "checkpoint", "taskId": TOKEN, "disk": 1, "relativePath": receipt["relativePath"],
                "faultId": "F07-early", "expectedBefore": receipt["captured"], "expected": identity}))
            self.assertEqual([request[1] for request in requests], ['journal-capture', 'process-signal', 'process'])
            self.assertEqual(fleet.frozen_processes, {})
            for key, value in [("processBound", False), ("productMetadataMutated", True), ("disk", True),
                               ("binarySha256", "b" * 64), ("captured", {"size": 3, "sha256": "a" * 64})]:
                fleet.frozen_processes["node"] = identity
                original = receipt[key]; receipt[key] = value
                with self.subTest(key=key), self.assertRaises(ValueError):
                    reader.capture_earlier(fleet, TOKEN, BUCKET, "F07-early")
                self.assertEqual(fleet.frozen_processes, {"node": identity})
                receipt[key] = original

    def test_atomic_earlier_capture_keeps_the_registered_birth_frozen_without_an_ssh_resume_gap(self):
        observed, _, _ = self.fixture(); owner = reader.owned_checkpoint(observed, TOKEN, BUCKET)
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50)
        receipt = {'processBound': True, 'originalProductBytes': True, 'productMetadataMutated': False,
            'runId': 'gh-12345-1-h1', 'family': 'checkpoint', 'taskId': TOKEN, 'disk': owner['disk'],
            'relativePath': owner['checkpoint']['relativePath'], 'faultId': 'F07-early',
            'binarySha256': identity['binarySha256'], 'captured': journal.entry(reader.bytes_of(owner['checkpoint'])),
            'checkpointAdvancedAndFrozen': True, 'pidfdBound': True, 'serviceFrozen': True,
            'observedProcessed': 3, 'processIdentity': identity}
        calls = []
        fleet = SimpleNamespace(run_id=receipt['runId'], frozen_processes={'node': identity},
            record=lambda label, value: value, remote=lambda node, action, **params: calls.append(action) or receipt)
        with patch.object(reader, 'freeze', return_value=(owner, identity)):
            earlier = reader.capture_earlier(fleet, TOKEN, BUCKET, 'F07-early', advance=True)
        self.assertEqual(calls, ['journal-capture-advance'])
        self.assertTrue(earlier['advancedAndFrozen'])
        self.assertEqual(earlier['processIdentity'], identity)
        self.assertEqual(fleet.frozen_processes, {'node': identity})
        for key, bad in [('checkpointAdvancedAndFrozen', False), ('pidfdBound', False), ('serviceFrozen', False),
                         ('observedProcessed', True), ('observedProcessed', 2), ('processIdentity', {})]:
            original = receipt[key]; receipt[key] = bad
            with self.subTest(key=key, bad=bad), patch.object(reader, 'freeze', return_value=(owner, identity)), \
                    self.assertRaises(ValueError):
                reader.capture_earlier(fleet, TOKEN, BUCKET, 'F07-early', advance=True)
            receipt[key] = original
            self.assertEqual(fleet.frozen_processes, {'node': identity})

    def test_atomic_frozen_generation_still_requires_full_fleet_checkpoint_and_root_censuses(self):
        observed, value, _ = self.fixture(); roots, _ = self.root_fixture()
        earlier = reader.owned_checkpoint(observed, TOKEN, BUCKET)
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50)
        earlier.update(advancedAndFrozen=True, processIdentity=identity)
        value['checkpoint_time'] = 101; value['current_object_index'] = 3
        value['processed_objects'].append('objects/third'); value['successful_objects'] = 3
        value['admin']['outcome']['counters'].update(processed=3, unchanged=2)
        observed['node']['records'][4].update(encoded(value, signed=True))
        for tree in (observed, roots):
            for peer in ('peer1', 'peer2', 'peer3'):
                tree[peer] = copy.deepcopy(tree['node'])
                tree[peer]['records'] = [{'disk': r['disk'], 'relativePath': r['relativePath'], 'present': False}
                                         for r in tree[peer]['records']]
        for change in (None, 'unregistered', 'changed-birth', 'not-frozen', 'second-owner'):
            fleet = SimpleNamespace(nodes=['node', 'peer1', 'peer2', 'peer3'],
                frozen_processes={} if change == 'unregistered' else {'node': identity},
                record=lambda label, value: value)
            if change == 'changed-birth': fleet.frozen_processes['node'] = {**identity, 'pid': 51}
            calls = []
            fleet.remote = lambda node, action, **params: calls.append(action) or {
                **identity, 'state': 'S' if change == 'not-frozen' else 'T'}
            current = copy.deepcopy(observed)
            if change == 'second-owner': current['peer1'] = copy.deepcopy(current['node'])
            def census(fleet, token, label, **params):
                self.assertNotIn('nodes', params)
                self.assertIn(label, ('Admin-checkpoint-frozen', 'Admin-root-frozen'))
                return roots if params.get('family') == 'root' else current
            with self.subTest(change=change), patch.object(reader, 'records', side_effect=census) as records:
                if change:
                    with self.assertRaises(ValueError): reader.freeze(fleet, TOKEN, BUCKET, earlier=earlier)
                else:
                    frozen, birth = reader.freeze(fleet, TOKEN, BUCKET, earlier=earlier)
                    self.assertEqual(birth, identity)
                    self.assertEqual(frozen['value']['admin']['outcome']['counters']['processed'], 3)
                    self.assertEqual(records.call_count, 2)
                self.assertNotIn('process-signal', calls)

    def test_stale_boundary_requires_actual_newer_progress_in_the_same_original_responsibility(self):
        observed, value, _ = self.fixture()
        earlier = reader.owned_checkpoint(observed, TOKEN, BUCKET)
        with self.assertRaisesRegex(ValueError, "NOT_OBSERVED"): reader.after_generation(earlier, earlier)
        value["checkpoint_time"] += 1; value["current_object_index"] += 1
        value["admin"]["outcome"]["counters"]["processed"] += 1
        value["admin"]["outcome"]["counters"]["unchanged"] += 1
        observed["node"]["records"][4].update(encoded(value, signed=True))
        current = reader.owned_checkpoint(observed, TOKEN, BUCKET)
        reader.after_generation(current, earlier)
        for key, new in [("disk", 2), ("node", "peer"), ("taskId", INCARNATION), ("bucketIncarnationId", TOKEN)]:
            with self.subTest(key=key), self.assertRaises(ValueError): reader.after_generation({**current, key: new}, earlier)
        changed = copy.deepcopy(current)
        changed["value"]["admin"]["options"]["pool"] = False
        changed["checkpoint"].update(encoded(changed["value"], signed=True))
        with self.assertRaises(ValueError): reader.after_generation(changed, earlier)

    def root_fixture(self):
        value = {'schema': 3, 'task_id': TOKEN, 'priority': 'High', 'retry_attempts': 0,
            'created_at': {'secs_since_epoch': 100, 'nanos_since_epoch': 0},
            'heal_type': {'type': 'erasure_set', 'buckets': [], 'set_disk_id': 'pool_0_set_0', 'all_buckets': True},
            'options': {'scan_mode': 2, 'remove_corrupted': False, 'recreate_missing': False, 'update_parity': False,
                        'recursive': True, 'dry_run': False, 'no_lock': False, 'pool_index': 0, 'set_index': 0}}
        rows = [{'disk': disk, 'relativePath': name, 'present': False}
                for disk in range(4) for name in journal.names('root', TOKEN)]
        rows[3].update(encoded(value))
        census = {'node': {'family': 'root', 'taskId': TOKEN, 'quiescent': True, 'readOnly': True,
                          'directoryEnumerationUsed': False, 'records': rows}}
        return census, value

    def test_root_admission_is_original_erasure_set_responsibility_with_one_exact_owner(self):
        census, _ = self.root_fixture()
        owner = reader.owned_root_admission(census, TOKEN)
        self.assertEqual((owner['node'], owner['disk']), ('node', 1))
        self.assertTrue(owner['originalDurableResponsibility'])
        for change in ('missing', 'terminal', 'duplicate', 'second-owner', 'hash', 'omitted-disk', 'unstable'):
            census, value = self.root_fixture(); rows = census['node']['records']
            if change == 'missing': rows[3]['present'] = False
            elif change == 'terminal': rows[4].update(encoded(value))
            elif change == 'duplicate': rows[0] = copy.deepcopy(rows[3])
            elif change == 'second-owner': census['peer'] = copy.deepcopy(census['node'])
            elif change == 'hash': rows[3]['sha256'] = 'a' * 64
            elif change == 'omitted-disk': rows.pop()
            else: census['node'].update(quiescent=False, stable=False, processBound=True, independentReads=2)
            with self.subTest(change=change), self.assertRaises(ValueError): reader.owned_root_admission(census, TOKEN)

    def test_root_erasure_admission_rejects_foreign_task_scope_options_time_and_terminal_shape(self):
        for change in ('task', 'schema', 'scope', 'scope-type', 'pool-type', 'lock', 'retry', 'priority', 'time', 'bucket-incarnation'):
            census, value = self.root_fixture()
            if change == 'task': value['task_id'] = INCARNATION
            elif change == 'schema': value['schema'] = 2
            elif change == 'scope': value['heal_type'] = {'type': 'cluster'}
            elif change == 'scope-type': value['heal_type']['all_buckets'] = 1
            elif change == 'pool-type': value['options']['pool_index'] = False
            elif change == 'lock': value['options']['no_lock'] = True
            elif change == 'retry': value['retry_attempts'] = 1
            elif change == 'priority': value['priority'] = 'Normal'
            elif change == 'time': value['created_at']['nanos_since_epoch'] = 1_000_000_000
            else: value['bucket_incarnation_id'] = INCARNATION
            census['node']['records'][3].update(encoded(value))
            with self.subTest(change=change), self.assertRaises(ValueError): reader.owned_root_admission(census, TOKEN)

    def test_freeze_and_crash_preserve_checkpoint_resume_and_the_actual_root_admission(self):
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        observed, _, _ = self.fixture(); roots, _ = self.root_fixture()
        root = reader.owned_root_admission(roots, TOKEN)
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50, state='S')
        observed['node'].update(processIdentity=identity, processBound=True, stable=True, independentReads=2)
        actions = []
        fleet = SimpleNamespace(nodes=['node'], frozen_processes={}, record=lambda label, value: value, stop=lambda: actions.append('stop'))
        def remote(node, action, **params):
            actions.append(action)
            if action == 'process': return {**identity, 'state': 'T' if 'process-signal' in actions else 'S'}
            if action == 'process-crash': return {'actualSignal': 9, 'pidfdBound': True, 'graceful': False, 'platformEventIssued': False}
            return {}
        fleet.remote = remote
        def records(fleet, token, label, **params): return roots if params.get('family') == 'root' else observed
        with patch.object(reader, 'records', side_effect=records):
            frozen, process = reader.freeze(fleet, TOKEN, BUCKET)
            self.assertEqual(frozen['rootAdmission'], root)
            stopped = reader.stop_for_fault(fleet, TOKEN, BUCKET, frozen, process)
            self.assertEqual(stopped['checkpoint'], frozen['checkpoint'])
            self.assertTrue(stopped['rootContinuity']['nonTimeoutIntentUnchanged'])
            self.assertTrue(stopped['rootContinuity']['budgetNotRefunded'])
        self.assertEqual(fleet.frozen_processes, {})

    def test_freeze_precedes_root_observation_to_keep_the_actual_checkpoint_window(self):
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        observed, _, _ = self.fixture(); roots, _ = self.root_fixture()
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50, state='S')
        observed['node'].update(processIdentity=identity, processBound=True, stable=True, independentReads=2)
        actions = []
        fleet = SimpleNamespace(nodes=['node'], frozen_processes={}, record=lambda label, value: value)
        def remote(node, action, **params):
            actions.append(action)
            return {**identity, 'state': 'T' if 'process-signal' in actions else 'S'}
        fleet.remote = remote
        def records(fleet, token, label, **params):
            if params.get('family') == 'root':
                self.assertIn('process-signal', actions)
                self.assertEqual(label, 'Admin-root-frozen')
                return roots
            return observed
        with patch.object(reader, 'records', side_effect=records):
            frozen, _ = reader.freeze(fleet, TOKEN, BUCKET)
        self.assertTrue(frozen['rootAdmission']['originalDurableResponsibility'])

    def test_foreign_root_after_freeze_retains_the_original_paused_process_for_cleanup(self):
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        observed, _, _ = self.fixture(); roots, value = self.root_fixture()
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50, state='T')
        observed['node'].update(processIdentity=identity, processBound=True, stable=True, independentReads=2)
        fleet = SimpleNamespace(nodes=['node'], frozen_processes={}, record=lambda label, value: value, remote=lambda *args, **kwargs: identity)
        def records(fleet, token, label, **params):
            if label == 'Admin-root-frozen':
                changed = copy.deepcopy(roots); altered = copy.deepcopy(value); altered['task_id'] = INCARNATION
                changed['node']['records'][3].update(encoded(altered)); return changed
            return roots if params.get('family') == 'root' else observed
        with patch.object(reader, 'records', side_effect=records), self.assertRaisesRegex(ValueError, 'original explicit Admin'):
            reader.freeze(fleet, TOKEN, BUCKET)
        self.assertEqual(fleet.frozen_processes, {'node': identity})

    def test_owner_is_frozen_before_observing_other_nodes_or_refreshing_its_process(self):
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        observed, _, _ = self.fixture(); roots, _ = self.root_fixture()
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50)
        observed['node'].update(processIdentity=identity, processBound=True, stable=True, independentReads=2)
        fleet = SimpleNamespace(nodes=['node', 'peer1', 'peer2', 'peer3'], frozen_processes={},
                                record=lambda label, value: value)
        actions = []
        def remote(node, action, **params):
            if action == 'process': self.assertIn('process-signal', actions)
            actions.append(action)
            return {**identity, 'state': 'T'}
        fleet.remote = remote
        def all_nodes(value):
            full = copy.deepcopy(value)
            for peer in fleet.nodes[1:]:
                full[peer] = copy.deepcopy(value['node'])
                full[peer]['records'] = [{'disk': r['disk'], 'relativePath': r['relativePath'], 'present': False}
                                        for r in full[peer]['records']]
            return full
        def records(fleet, token, label, **params):
            if label == 'Admin-checkpoint-live':
                self.assertEqual(params.get('nodes'), ('node',))
                return observed
            self.assertIn('process-signal', actions)
            self.assertNotIn('nodes', params)
            return all_nodes(roots if params.get('family') == 'root' else observed)
        with patch.object(reader, 'records', side_effect=records):
            frozen, birth = reader.freeze(fleet, TOKEN, BUCKET)
        self.assertEqual(birth, identity)
        self.assertTrue(frozen['rootAdmission']['originalDurableResponsibility'])

    def test_live_record_drift_is_retried_inside_the_original_observation_budget(self):
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        observed, _, _ = self.fixture(); roots, _ = self.root_fixture()
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50)
        observed['node'].update(processIdentity=identity, processBound=True, stable=True, independentReads=2)
        fleet = SimpleNamespace(nodes=['node'], frozen_processes={}, record=lambda label, value: value,
            remote=lambda *a, **k: {**identity, 'state': 'T'})
        calls = []
        def records(fleet, token, label, **params):
            calls.append(label)
            if len(calls) == 1:
                raise ValueError('NOT_OBSERVED: exact durable records changed between the two bounded reads')
            return roots if params.get('family') == 'root' else observed
        with patch.object(reader, 'records', side_effect=records), patch.object(reader.time, 'sleep'):
            frozen, birth = reader.freeze(fleet, TOKEN, BUCKET)
        self.assertEqual(calls.count('Admin-checkpoint-live'), 2)
        self.assertEqual(birth, identity)
        self.assertTrue(frozen['rootAdmission']['originalDurableResponsibility'])

    def test_record_drift_timeout_and_non_observation_errors_do_not_signal(self):
        for message in ('NOT_OBSERVED: exact durable record changed during a bounded read',
                        'foreign process birth', 'checkpoint digest is invalid'):
            events = []
            fleet = SimpleNamespace(nodes=['node'], frozen_processes={}, record=lambda label, value: value,
                remote=lambda *a, **k: events.append(a))
            with self.subTest(message=message), patch.object(reader, 'records', side_effect=ValueError(message)), \
                    patch.object(reader.time, 'monotonic', side_effect=[0, 61]), self.assertRaises(ValueError):
                reader.freeze(fleet, TOKEN, BUCKET)
            self.assertEqual(events, [])
            self.assertEqual(fleet.frozen_processes, {})

    def test_full_census_after_early_freeze_still_rejects_a_second_physical_owner(self):
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        observed, _, _ = self.fixture()
        identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50)
        observed['node'].update(processIdentity=identity, processBound=True, stable=True, independentReads=2)
        fleet = SimpleNamespace(nodes=['node', 'peer1', 'peer2', 'peer3'], frozen_processes={},
            record=lambda label, value: value, remote=lambda *args, **params: {**identity, 'state': 'T'})
        def records(fleet, token, label, **params):
            if label == 'Admin-checkpoint-live': return observed
            full = {node: copy.deepcopy(observed['node']) for node in fleet.nodes}
            return full
        with patch.object(reader, 'records', side_effect=records), self.assertRaises(ValueError):
            reader.freeze(fleet, TOKEN, BUCKET)
        self.assertEqual(fleet.frozen_processes, {'node': identity})

    def test_unbound_census_birth_never_signals_a_process(self):
        from scripts.validation_heal.fleet_node import PROCESS_FIELDS
        for change in ('missing-birth', 'missing-field', 'boolean-pid', 'unstable', 'unbound'):
            observed, _, _ = self.fixture()
            identity = {name: name for name in PROCESS_FIELDS}; identity.update(pid=50)
            observed['node'].update(processIdentity=identity, processBound=True, stable=True, independentReads=2)
            if change == 'missing-birth': observed['node'].pop('processIdentity')
            elif change == 'missing-field': identity.pop('startTicks')
            elif change == 'boolean-pid': identity['pid'] = True
            elif change == 'unstable': observed['node']['stable'] = False
            else: observed['node']['processBound'] = False
            actions = []
            fleet = SimpleNamespace(nodes=['node'], frozen_processes={}, record=lambda label, value: value,
                                    remote=lambda *a, **k: actions.append(a))
            with patch.object(reader, 'records', return_value=observed), self.subTest(change=change), \
                    self.assertRaises(ValueError):
                reader.freeze(fleet, TOKEN, BUCKET)
            self.assertEqual(actions, [])
            self.assertEqual(fleet.frozen_processes, {})


if __name__ == "__main__": unittest.main()
