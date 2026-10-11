import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import full_restart as restart
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES
from scripts.validation_heal.fleet_node import PROCESS_FIELDS, resource_limits, snapshot_buckets, verify_part_owned


class FullRestartTests(unittest.TestCase):
    token = "12345678-1234-4234-8234-123456789abc"

    def test_combined_fixture_has_explicit_bounded_physical_census_budget(self):
        # Four roots retain xl.meta, an external part and its integrity
        # sidecar, plus a possible metadata backup. MPU adds three parts.
        conservative_files = 4 * (4 * (1025 + 128 + 16) + 7)
        self.assertGreater(conservative_files, resource_limits("cursor")["maxSnapshotFiles"])
        limits = resource_limits("full-restart")
        self.assertLess(conservative_files, limits["maxSnapshotFiles"])
        self.assertEqual(limits["runtimeMaxSeconds"], 2700)
        self.assertEqual(limits["minimumDriveFreeBytes"], 2 * 1024**3)
        with self.assertRaises(ValueError): resource_limits("unbounded")

    def wanted(self):
        wanted = {("db", f"objects/{index:04d}", "null"): {"deleteMarker": False, "payload": {"size": 17}}
                  for index in range(1025)}
        wanted[("versions", "history", "old-version")] = {"deleteMarker": False, "payload": {"size": 19}}
        wanted[("versions", "history", "marker")] = {"deleteMarker": True}
        return wanted

    def receipt(self):
        repaired = {("db", "objects/0000", "null")}
        objects = []
        for pair in (next(iter(repaired)), ("versions", "history", "old-version"), ("versions", "history", "marker")):
            state = "repaired" if pair in repaired else "metadata_healthy" if pair[2] == "marker" else "verified_healthy"
            objects.append({"identity": {"bucket": pair[0], "object": pair[1], "versionId": pair[2],
                "poolIndex": 0, "setIndex": 0, "bucketIncarnationId": "incarnation-" + pair[0]}, "disposition": {"state": state}})
        return {"summary": "finished", "outcome": {"execution": {"state": "completed"}, "coverage": "complete",
            "objectsTruncated": True, "objects": objects,
            "counters": {"processed": 1027, "healed": 1, "unchanged": 1026, "skipped": 0,
                         "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}}}, repaired

    def test_complete_counters_do_not_make_unknown_or_cross_bucket_receipts_pass(self):
        current, repaired = self.receipt()
        proof = restart.responsibility(current, self.wanted(), repaired)
        self.assertFalse(proof["diagnosticsAreFullManifest"])
        for kind in ("unknown", "false-repair", "duplicate", "bucket", "version", "incarnation", "counter", "bool", "coverage", "truncation"):
            with self.subTest(kind=kind):
                bad = copy.deepcopy(current)
                if kind == "unknown": bad["outcome"]["objects"][0]["disposition"]["state"] = "unknown"
                elif kind == "false-repair": bad["outcome"]["objects"][1]["disposition"]["state"] = "repaired"
                elif kind == "duplicate": bad["outcome"]["objects"].append(copy.deepcopy(bad["outcome"]["objects"][0]))
                elif kind == "bucket": bad["outcome"]["objects"][0]["identity"]["bucket"] = "versions"
                elif kind == "version": bad["outcome"]["objects"][1]["identity"]["versionId"] = "current-version"
                elif kind == "incarnation": bad["outcome"]["objects"][2]["identity"]["bucketIncarnationId"] = "other"
                elif kind == "counter": bad["outcome"]["counters"]["processed"] -= 1
                elif kind == "bool": bad["outcome"]["counters"]["healed"] = True
                elif kind == "coverage": bad["outcome"]["coverage"] = "partial"
                elif kind == "truncation": bad["outcome"]["objectsTruncated"] = False
                with self.assertRaises(ValueError): restart.responsibility(bad, self.wanted(), repaired)

    def journal(self):
        current, _ = self.receipt()
        owner = {"node": "n0", "disk": 2, "value": {"options": {"timeout": {"secs": 149, "nanos": 0}}}}
        terminal = {"schema": 1, "task_id": self.token, "status": "Completed", "heal_type": {"type": "cluster"},
                    "options": copy.deepcopy(owner["value"]["options"])}
        item = {"type": "object", "bucket": "versions", "object": "history", "versionId": "marker", "objectSize": 0}
        report = {"schema": 1, "terminal": terminal, "outcome": current["outcome"], "min_seq": 1027,
                  "next_seq": 1028, "seqed_items": [[1027, item]]}
        observed = {node: {"taskId": self.token, "readOnly": True, "records": [
            {"disk": disk, "kind": kind, "present": node == "n0" and disk == 2 and kind != "intent",
             **({"value": terminal if kind == "terminal" else report} if node == "n0" and disk == 2 and kind != "intent" else {})}
            for disk in range(4) for kind in ("intent", "terminal", "report")]} for node in ("n0", "n1", "n2", "n3")}
        return observed, owner, current

    def test_original_record_and_retained_suffix_are_required_after_all_peer_restart(self):
        observed, owner, current = self.journal()
        self.assertTrue(restart.terminal_records(observed, self.token, owner, current, self.wanted())["sameRecordDisk"])
        # The product sequence also contains durable bucket events; the
        # retained suffix must remain contiguous while next_seq exceeds the
        # object counter by those explicit events.
        with_offset = copy.deepcopy(observed)
        offset_report = next(row for row in with_offset["n0"]["records"] if row["kind"] == "report" and row["present"])
        offset_report["value"]["min_seq"] = 1028
        offset_report["value"]["next_seq"] = 1030
        offset_report["value"]["seqed_items"] = [[1028, {"type": "bucket", "bucket": "db", "object": "", "versionId": "", "objectSize": 0}],
                                                    [1029, offset_report["value"]["seqed_items"][0][1]]]
        self.assertTrue(restart.terminal_records(with_offset, self.token, owner, current, self.wanted())["sameRecordDisk"])
        for kind in ("missing", "owner", "intent", "token", "scope", "budget", "outcome", "sequence", "suffix", "marker-size"):
            with self.subTest(kind=kind):
                bad = copy.deepcopy(observed)
                terminal = next(row for row in bad["n0"]["records"] if row["kind"] == "terminal" and row["present"])
                report = next(row for row in bad["n0"]["records"] if row["kind"] == "report" and row["present"])
                if kind == "missing": terminal["present"] = False
                elif kind == "owner": report["disk"] = 1
                elif kind == "intent": next(row for row in bad["n0"]["records"] if row["disk"] == 2 and row["kind"] == "intent")["present"] = True
                elif kind == "token": terminal["value"]["task_id"] = "new-task"
                elif kind == "scope": terminal["value"]["heal_type"] = {"type": "bucket", "bucket": "db"}
                elif kind == "budget": terminal["value"]["options"]["timeout"]["secs"] = 300
                elif kind == "outcome": report["value"]["outcome"]["coverage"] = "partial"
                elif kind == "sequence": report["value"]["seqed_items"][0][0] = 1026
                elif kind == "suffix": report["value"]["next_seq"] += 1
                elif kind == "marker-size": report["value"]["seqed_items"][0][1]["objectSize"] = 17
                with self.assertRaises(ValueError): restart.terminal_records(bad, self.token, owner, current, self.wanted())

    def processes(self):
        return {node: {**{name: name + "-" + node for name in PROCESS_FIELDS}, "pid": 100 + index, "startTicks": 1000 + index}
                for index, node in enumerate(("n0", "n1", "n2", "n3"))}

    def test_each_original_peer_must_really_restart_without_changing_environment(self):
        before = self.processes()
        after = copy.deepcopy(before)
        for item in after.values(): item["pid"] += 100; item["startTicks"] += 100
        self.assertTrue(restart.continuity(before, after)["allFourOriginalProcessesRestarted"])
        for field in ("pid", "environmentFileSha256", "binarySha256", "bootId", "mountNamespace", "unit"):
            with self.subTest(field=field):
                bad = copy.deepcopy(after)
                if field == "pid": bad["n2"] = before["n2"]
                else: bad["n2"][field] = "changed"
                with self.assertRaises(ValueError): restart.continuity(before, bad)

    def test_all_peer_stop_is_bound_to_original_birth_identity_and_real_zero_exit(self):
        processes, calls = self.processes(), []
        def remote(node, action, **params):
            calls.append((node, action, params))
            if action == "process-graceful": return {"actualExitCode": 0, "signal": "SIGTERM", "pidfdBound": True}
            return {}
        fleet = types.SimpleNamespace(nodes=list(processes), frozen_processes={"n2": processes["n2"]},
            remote=remote, record=lambda label, value: value)
        exits = restart.graceful_all(fleet, processes, "n2")
        self.assertEqual(list(exits), ["n2", "n0", "n1", "n3"])
        self.assertFalse(fleet.frozen_processes)
        for node, action, params in calls:
            if action != "stop": self.assertEqual(params["expected"], processes[node])
        fleet.remote = lambda *args, **kwargs: {"actualExitCode": 9, "signal": "SIGTERM", "pidfdBound": True}
        with self.assertRaisesRegex(ValueError, "normal graceful exit"): restart.graceful_all(fleet, processes, "n0")
        self.assertIn("n0", fleet.frozen_processes)

    def test_every_source_branch_has_an_automatic_h2_entry(self):
        recipe = select("F05")[0]
        self.assertEqual(recipe["variants"], list(restart.BRANCHES))
        self.assertEqual(recipe["contract"]["probes"]["topology"], "H2")
        self.assertIn(restart.CASE_ID, AUTOMATIC_CASES)

    def test_multi_bucket_physical_scope_preserves_the_original_run_boundary(self):
        owner = {"bucket": "rv-heal-0123456789abcdef"}
        prefix = owner["bucket"]
        self.assertEqual(snapshot_buckets(owner, {}), [prefix])
        self.assertEqual(snapshot_buckets(owner, {"buckets": [prefix, prefix + "-dx", prefix + "-versions"]}),
                         [prefix, prefix + "-dx", prefix + "-versions"])
        for buckets in ([prefix, "other"], [prefix, prefix + "/../outside"], [prefix, prefix], [prefix + "-dx"], [prefix, 7], []):
            with self.subTest(buckets=buckets), self.assertRaises(ValueError): snapshot_buckets(owner, {"buckets": buckets})

    def test_secondary_bucket_streaming_proof_cannot_bind_to_primary_bucket_with_same_key(self):
        prefix = "rv-heal-0123456789abcdef"
        bucket = prefix + "-dx"
        relative = bucket + "/objects/object.bin/generation/part.1"
        request = {"disk": 0, "relativePath": relative, "versionId": "null", "partNumber": 1}
        result = {"physicalBitrotVerified": True, "bucket": bucket, "key": "objects/object.bin", "versionId": "null", "partNumber": 1}
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            part = root / "data" / relative
            part.parent.mkdir(parents=True)
            part.write_bytes(b"original-protected-shard")
            with patch("scripts.validation_heal.fleet_node.command", return_value=types.SimpleNamespace(stdout=json.dumps(result))) as command:
                proof = verify_part_owned(request, [root], {"bucket": prefix}, root / "tool")
                self.assertEqual(proof["bucket"], bucket)
                self.assertEqual(command.call_args.args[3:5], (bucket, "objects/object.bin"))
            result["bucket"] = prefix
            with patch("scripts.validation_heal.fleet_node.command", return_value=types.SimpleNamespace(stdout=json.dumps(result))), self.assertRaisesRegex(ValueError, "identity differs"):
                verify_part_owned(request, [root], {"bucket": prefix}, root / "tool")


if __name__ == "__main__": unittest.main()
