import copy
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from scripts.validation_heal import scopes
from scripts.validation_heal.healthy import validate_topology, Fleet
from scripts.validation_heal.fleet_node import runtime_config
from scripts.validation_heal.shards import map_target, files
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES


class ScopeTests(unittest.TestCase):
    primary, secondary = "rv-heal-0123456789abcdef", "rv-heal-0123456789abcdef-other"

    def snapshot(self):
        sets = [[f"disk-{group}-{slot}" for slot in range(8)] for group in range(2)]
        snapshot = {f"node{node + 1}": {"formats": [], "files": []} for node in range(4)}
        objects = [(self.primary, "a/target.bin", 0), (self.primary, "a/child/neighbor.bin", 1),
                   (self.primary, "ab/control.bin", 1), (self.primary, "b/control.bin", 0),
                   (self.secondary, "a/target.bin", 1)]
        for node, observation in enumerate(snapshot.values()):
            for disk in range(4):
                group, slot = disk // 2, (disk % 2) * 4 + node
                observation["formats"].append({"disk": disk, "value": {"format": "xl", "id": "pool",
                                              "xl": {"sets": sets, "this": sets[group][slot]}}})
                for index, (bucket, key, selected_set) in enumerate(objects):
                    if selected_set != group: continue
                    data_dir = str(uuid.UUID(int=index + 1))
                    metadata = {"bucket": bucket, "key": key, "versions": [{"versionId": None, "isLatest": True,
                        "deleted": False, "dataDir": data_dir, "size": scopes.SIZE, "inline": None, "usesLegacyChecksum": False,
                        "role": "data" if slot < 4 else "parity", "parts": [{"number": 1, "size": scopes.SIZE, "actualSize": scopes.SIZE}],
                        "erasure": {"index": slot + 1, "data_blocks": 4, "parity_blocks": 4, "block_size": 1048576,
                                    "distribution": list(range(1, 9)), "algorithm": "rs-vandermonde"}}]}
                    observation["files"].extend([
                        {"disk": disk, "path": f"{bucket}/{key}/xl.meta", "size": 600, "sha256": f"meta-{index}-{slot}", "metadata": metadata},
                        {"disk": disk, "path": f"{bucket}/{key}/{data_dir}/part.1", "size": 400, "sha256": f"part-{index}-{slot}"}])
        return snapshot

    def expected(self):
        return {self.primary: [{"key": key} for key in ("a/target.bin", "a/child/neighbor.bin", "ab/control.bin", "b/control.bin")],
                self.secondary: [{"key": "a/target.bin"}]}

    def mapping(self):
        return scopes.object_mapping(self.snapshot(), self.expected(), "H2")

    def receipt(self, branch):
        mapped = self.mapping()
        selected = scopes.selection(branch, self.primary, mapped)
        _, options = scopes.request_spec(branch, self.primary)
        objects = [{"identity": {"kind": "object", "bucket": bucket, "object": key, "versionId": "null",
                    "poolIndex": options["pool"], "setIndex": options["set"], "bucketIncarnationId": "incarnation-" + bucket},
                    "disposition": {"state": "repaired"}} for bucket, key in sorted(selected)]
        counts = {"processed": len(selected), "healed": len(selected), "unchanged": 0,
                  "failed": 0, "skipped": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
        current = {"summary": "finished", "outcome": {"execution": {"state": "completed"}, "coverage": "complete",
                    "objectsTruncated": False, "objects": objects, "counters": counts}}
        return current, mapped, selected, options

    def test_five_source_branches_and_real_two_set_runtime_are_retained(self):
        self.assertEqual(select(scopes.CASE_ID)[0]["variants"], list(scopes.BRANCHES))
        self.assertIn(scopes.CASE_ID, AUTOMATIC_CASES)
        config = runtime_config("scope-isolation")
        self.assertEqual(config["RUSTFS_ERASURE_SET_DRIVE_COUNT"], "8")
        self.assertEqual(config["RUSTFS_SCANNER_ENABLED"], "false")
        self.assertEqual(config["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertEqual(config["RUSTFS_HEAL_MRF_ENABLE"], "false")

    def test_existing_single_set_oracle_rejects_a_multi_set_baseline_without_explicit_binding(self):
        snapshot = self.snapshot()
        with self.assertRaises(ValueError): validate_topology(snapshot, "H2")
        with self.assertRaises(ValueError): map_target(snapshot, self.primary, "a/target.bin", "data", 1)
        proof = validate_topology(snapshot, "H2", expected_sets=2)
        self.assertEqual(proof["setCount"], 2)
        self.assertEqual(len(proof["diskUUIDs"]), 16)
        for group in proof["actualSets"]: self.assertEqual(len(group), 8)

    def test_every_set_must_span_all_four_original_hosts(self):
        snapshot = self.snapshot()
        records = [record for observation in snapshot.values() for record in observation["formats"]]
        sets = [[record["value"]["xl"]["this"] for record in records[:8]],
                [record["value"]["xl"]["this"] for record in records[8:]]]
        for record in records: record["value"]["xl"]["sets"] = sets
        with self.assertRaisesRegex(ValueError, "span every"): validate_topology(snapshot, "H2", expected_sets=2)

    def test_mapping_uses_actual_members_and_role_distribution_without_projecting_format_values(self):
        snapshot = self.snapshot()
        before = copy.deepcopy(snapshot)
        mapped = scopes.object_mapping(snapshot, self.expected(), "H2")
        self.assertEqual(snapshot, before)
        self.assertEqual(mapped[(self.primary, "a/target.bin")]["setIndex"], 0)
        other = mapped[(self.primary, "ab/control.bin")]
        self.assertEqual(other["setIndex"], 1)
        self.assertEqual({member["node"] for member in other["cohort"]}, set(snapshot))
        self.assertEqual(len(other["cohort"]), 8)
        self.assertTrue(all(member["setSlot"] + 1 == member["index"] for member in other["cohort"]))
        self.assertEqual(len(scopes.fault_targets(self.primary, self.secondary, mapped, "H2")), 5)
        with self.assertRaises(ValueError): map_target(snapshot, self.primary, "ab/control.bin", "data", 1, multi_set=True, set_index=0)

    def test_orphan_payload_on_an_unselected_set_is_not_a_complete_baseline(self):
        snapshot = self.snapshot()
        part = next(entry for entry in snapshot["node1"]["files"] if "a/target.bin/" in entry["path"] and entry["path"].endswith("part.1"))
        snapshot["node1"]["files"].append({**part, "disk": 2})
        with self.assertRaisesRegex(ValueError, "selected and unselected"):
            scopes.object_mapping(snapshot, self.expected(), "H2")

    def test_each_selector_uses_exact_bucket_key_prefix_and_physical_set(self):
        mapped = self.mapping()
        self.assertEqual(len(scopes.selection("root", self.primary, mapped)), 5)
        self.assertEqual(len(scopes.selection("bucket", self.primary, mapped)), 4)
        prefix = scopes.selection("prefix", self.primary, mapped)
        self.assertEqual(prefix, {(self.primary, "a/target.bin"), (self.primary, "a/child/neighbor.bin")})
        self.assertEqual(scopes.selection("object", self.primary, mapped), {(self.primary, "a/target.bin")})
        self.assertEqual(scopes.selection("pool-set", self.primary, mapped), {(self.primary, "a/target.bin"), (self.primary, "b/control.bin")})
        self.assertFalse(scopes.request_spec("object", self.primary)[1]["recursive"])
        self.assertIsNone(scopes.request_spec("root", self.primary)[1]["pool"])
        self.assertEqual(scopes.request_spec("pool-set", self.primary)[1]["set"], 0)

    def test_nullable_unrestricted_receipts_keep_raw_fields_and_independent_physical_bindings(self):
        current, mapped, selected, options = self.receipt("root")
        original = copy.deepcopy(current)
        proof = scopes.responsibility(current, mapped, selected, selected, options)
        self.assertEqual(current, original)
        self.assertTrue(proof["rawIdentitiesNotRewritten"])
        self.assertEqual({row["physicalSetIndex"] for row in proof["physicalScopeBindings"]}, {0, 1})
        self.assertTrue(all(row["rawSelectorScope"] == [None, None] for row in proof["physicalScopeBindings"]))
        item = current["outcome"]["objects"][0]["identity"]
        item.update(poolIndex=0, setIndex=mapped[(item["bucket"], item["object"])]["setIndex"])
        scopes.responsibility(current, mapped, selected, selected, options)

    def test_explicit_scope_nulls_wrong_sets_extra_objects_unknowns_and_false_counts_fail(self):
        for kind in ("null", "set", "bool", "missing", "duplicate", "kind", "version", "incarnation", "unknown", "counter", "coverage"):
            with self.subTest(kind=kind):
                current, mapped, selected, options = self.receipt("pool-set")
                item = current["outcome"]["objects"][0]
                if kind == "null": item["identity"].update(poolIndex=None, setIndex=None)
                elif kind == "set": item["identity"]["setIndex"] = 1
                elif kind == "bool": item["identity"]["setIndex"] = False
                elif kind == "missing": current["outcome"]["objects"].pop()
                elif kind == "duplicate": current["outcome"]["objects"][-1] = copy.deepcopy(item)
                elif kind in {"kind", "version", "incarnation"}:
                    item["identity"][{"kind": "kind", "version": "versionId", "incarnation": "bucketIncarnationId"}[kind]] = "" if kind == "incarnation" else "foreign"
                elif kind == "unknown": item["disposition"]["state"] = "unknown"
                elif kind == "counter": current["outcome"]["counters"]["healed"] += 1
                else: current["outcome"]["coverage"] = "partial"
                with self.assertRaises(ValueError): scopes.responsibility(current, mapped, selected, selected, options)
        current, mapped, selected, options = self.receipt("root")
        current["outcome"]["objects"][0]["identity"].update(poolIndex=None, setIndex=0)
        with self.assertRaises(ValueError): scopes.responsibility(current, mapped, selected, selected, options)

    def test_outside_control_faults_must_still_be_missing_before_backup_rescue(self):
        original = self.snapshot()
        mapped = self.mapping()
        targets = list(mapped.values())
        selected = scopes.selection("prefix", self.primary, mapped)
        after = copy.deepcopy(original)
        removed = {(target["node"], target["disk"], target["relativePath"]) for target in targets if (target["bucket"], target["key"]) not in selected}
        for name, observation in after.items():
            observation["files"] = [entry for entry in observation["files"] if (name, entry["disk"], entry["path"]) not in removed]
        proof = scopes.physical_selection(original, after, targets, selected)
        self.assertEqual(proof["selectedReconstructedParts"], 2)
        self.assertEqual(proof["unselectedStillMissingParts"], 3)
        self.assertTrue(proof["beforeAnyTargetGETOrBackupRescue"])
        wrong = copy.deepcopy(after)
        identity = next(iter(removed))
        wrong[identity[0]]["files"].append(files(original)[identity])
        with self.assertRaisesRegex(ValueError, "unselected control was repaired"):
            scopes.physical_selection(original, wrong, targets, selected)
        bad = copy.deepcopy(after)
        metadata = next(entry for entry in bad["node1"]["files"] if "ab/control.bin/xl.meta" in entry["path"])
        metadata["sha256"] = "changed"
        with self.assertRaises(ValueError): scopes.physical_selection(original, bad, targets, selected)

    def test_scope_adapter_uploads_a_compilable_complete_node_program(self):
        import types
        def invoke(argv, **params):
            compile(params["input"], "remote-scoped-node", "exec")
            return types.SimpleNamespace(returncode=0, stdout='{"checked":true}', stderr="")
        with tempfile.TemporaryDirectory() as name, patch("scripts.validation_heal.healthy.subprocess.run", side_effect=invoke):
            fleet = Fleet(["n0", "n1", "n2", "n3"], Path(name), "gh-1234-1-h2", runtime_profile="scope-isolation")
            self.assertTrue(fleet.remote("n0", "process-metrics")["checked"])
            self.assertTrue(fleet.remote("n0", "start")["checked"])


if __name__ == "__main__": unittest.main()
