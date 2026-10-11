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


if __name__ == "__main__": unittest.main()
