"""Reject false queue admission, responsibility retirement and resource proofs."""

import copy
from datetime import datetime, timezone
import json
import types
import unittest

from scripts.validation_heal import queue_pressure as queue
from scripts.validation_heal.fleet_node import runtime_config
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select


class QueuePressureTests(unittest.TestCase):
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
