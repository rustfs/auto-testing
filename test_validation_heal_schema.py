import base64
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import schema, schema_faults, heal_fault, fleet_node
from scripts.validation_heal.cursor import verify_all_parts, verify_part_manifest
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select


class SchemaRefusalTests(unittest.TestCase):
    def format(self, foreign=False, width=4):
        prefix = "4" if foreign else "3"
        members = [f"{prefix}2345678-1234-4234-8234-{index:012x}" for index in range(width)]
        return {"version": "1", "format": "xl", "id": ("2" if foreign else "1") + "2345678-1234-4234-8234-123456789abc",
                "xl": {"version": "3", "this": members[0], "sets": [members], "distributionAlgo": "SIPMOD+PARITY"}}

    def donor(self):
        raw = b" \n" + json.dumps(self.format(True), indent=2).encode() + b"\n"
        return {"readOnly": True, "binarySha256": "candidate", "rawBase64": base64.b64encode(raw).decode(),
                "sha256": hashlib.sha256(raw).hexdigest(), "value": self.format(True)}

    def test_foreign_disk_uses_actual_donor_uuid_and_keeps_original_deployment_and_set(self):
        original = self.format(width=16)
        raw, changed, detail = schema_faults.derive(json.dumps(original).encode(), "foreign-disk", self.donor())
        self.assertEqual(json.loads(raw), changed)
        self.assertEqual(changed["xl"]["this"], self.donor()["value"]["xl"]["this"])
        changed["xl"]["this"] = original["xl"]["this"]
        self.assertEqual(changed, original)
        self.assertTrue(detail["derivedIdentityFixture"])

    def test_foreign_set_copies_exact_donor_bytes_even_when_topology_width_differs(self):
        donor = self.donor()
        raw, changed, detail = schema_faults.derive(json.dumps(self.format(width=16)).encode(), "foreign-set", donor)
        self.assertEqual(raw, base64.b64decode(donor["rawBase64"]))
        self.assertEqual(changed, donor["value"])
        self.assertFalse(detail["derivedIdentityFixture"])
        self.assertEqual(len(changed["xl"]["sets"][0]), 4)

    def test_future_format_changes_only_reviewed_semantic_version(self):
        original = self.format()
        raw, changed, detail = schema_faults.derive(json.dumps(original).encode(), "future-format")
        self.assertEqual(json.loads(raw)["version"], "999")
        self.assertTrue(detail["otherSemanticFieldsUnchanged"])
        changed["version"] = "1"
        self.assertEqual(changed, original)

    def test_foreign_provenance_rejects_shared_ids_changed_bytes_and_unreviewed_schema(self):
        for kind in ("sha", "value", "read-only", "shared-deployment", "shared-member", "schema", "geometry"):
            donor = self.donor()
            if kind == "sha": donor["sha256"] = "0" * 64
            elif kind == "value": donor["value"]["id"] = self.format()["id"]
            elif kind == "read-only": donor["readOnly"] = False
            else:
                value = donor["value"]
                if kind == "shared-deployment": value["id"] = self.format()["id"]
                if kind == "shared-member": value["xl"]["sets"][0][1] = self.format()["xl"]["sets"][0][1]
                if kind == "schema": value["xl"]["version"] = "4"
                if kind == "geometry": value["xl"]["sets"][0].pop()
                raw = json.dumps(value).encode()
                donor.update(rawBase64=base64.b64encode(raw).decode(), sha256=hashlib.sha256(raw).hexdigest())
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                schema_faults.derive(json.dumps(self.format()).encode(), "foreign-set", donor)

    def test_future_object_schema_changes_exact_major_and_preserves_every_other_byte(self):
        original = b"XL2 \x01\x00\x03\x00original-msgpack-and-payload"
        changed = heal_fault.mutation(original, "future-xl-schema")
        self.assertEqual(changed, original[:4] + b"\x02\x00" + original[6:])
        for value, offset in ((b"XL2 \x02\x00\x03\x00", None), (b"bad!\x01\x00\x03\x00", None), (original, 5), (b"XL2 ", None)):
            with self.subTest(value=value, offset=offset), self.assertRaises(ValueError):
                heal_fault.mutation(value, "future-xl-schema", offset)
        with self.assertRaisesRegex(ValueError, "exact object metadata"):
            heal_fault.apply_fault(0, {"bucketPrefix": "bucket"}, "bucket/key/data/part.1", "C09-future-0", "future-xl-schema", "a" * 64)

    @unittest.skipUnless(sys.platform == "linux", "actual Linux FD/mount guards run in CI")
    def test_format_install_uses_one_empty_root_preserves_archive_and_rejects_other_disk(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "data").mkdir()
            saved = root / "quarantine/old-disk/.rustfs.sys"
            saved.mkdir(parents=True)
            original = json.dumps(self.format()).encode()
            (saved / "format.json").write_bytes(original)
            replacement = {"disk": 0, "originalArchiveUnchanged": True, "originalStillAtDataRoot": False,
                           "originalFormatSha256": hashlib.sha256(original).hexdigest()}
            owner = {"runId": "run", "binarySha256": "candidate"}
            with heal_fault.directory(root) as fd:
                for kind in ("wrong-disk", "occupied", "wrong-candidate", "archive"):
                    request = {"disk": 0, "branch": "foreign-set", "donor": self.donor()}
                    current = copy.deepcopy(replacement)
                    if kind == "wrong-disk": request["disk"] = 1
                    if kind == "occupied": (root / "data/occupied").write_bytes(b"live")
                    if kind == "wrong-candidate": request["donor"]["binarySha256"] = "other"
                    if kind == "archive": current["originalArchiveUnchanged"] = False
                    with self.subTest(kind=kind), self.assertRaises(ValueError): schema_faults.install(fd, current, owner, request, heal_fault)
                    (root / "data/occupied").unlink(missing_ok=True)
                proof = schema_faults.install(fd, replacement, owner, {"disk": 0, "branch": "future-format"}, heal_fault)
                self.assertEqual((saved / "format.json").read_bytes(), original)
                self.assertEqual(hashlib.sha256((root / "data/.rustfs.sys/format.json").read_bytes()).hexdigest(), proof["injectedSha256"])
                self.assertFalse(proof["sharedFormatTouched"])
                self.assertFalse(proof["credentialsRead"])

    def scene(self):
        before = {"node": {"formats": [{"disk": i, "sha256": f"format-{i}"} for i in range(4)], "files": []}}
        after = copy.deepcopy(before)
        targets, receipts = [], []
        for i in range(4):
            relative = "bucket/key/xl.meta"
            original = {"disk": i, "path": relative, "sha256": f"original-{i}", "size": 91,
                        "metadata": {"physicalBitrotVerified": False}}
            before["node"]["files"].extend([original, {"disk": i, "path": "bucket/key/data/part.1", "sha256": f"part-{i}", "size": 100}])
            changed = {"sha256": f"future-{i}", "size": 91}
            after["node"]["files"].extend([{**original, **changed, "metadataError": {"exactWitnessedFault": True,
                "validMetadataClaimed": False, "message": "xl file version err"}}, copy.deepcopy(before["node"]["files"][-1])])
            del after["node"]["files"][-2]["metadata"]
            targets.append({"node": "node", "disk": i, "relativePath": relative})
            receipts.append({"kind": "future-xl-schema", "relativePath": relative,
                "before": {"sha256": original["sha256"], "size": 91}, "fault": changed})
        return before, after, targets, receipts

    def test_negative_scene_cannot_hide_unknown_record_rewrite_absence_or_payload_mutation(self):
        before, after, targets, receipts = self.scene()
        self.assertEqual(schema.metadata_scene(before, after, targets, receipts)["unknownRecordsRetainedExactly"], 4)
        for kind in ("absent", "rewrite", "valid", "control", "format", "parser", "duplicate", "length"):
            bad, bad_receipts = copy.deepcopy(after), copy.deepcopy(receipts)
            if kind == "absent": bad["node"]["files"].pop(0)
            if kind == "rewrite": bad["node"]["files"][0]["sha256"] = "rewritten"
            if kind == "valid": bad["node"]["files"][0]["metadata"] = {}
            if kind == "control": bad["node"]["files"][1]["sha256"] = "changed-payload"
            if kind == "format": bad["node"]["formats"][0]["sha256"] = "changed"
            if kind == "parser": bad["node"]["files"][0]["metadataError"]["message"] = "other corrupt metadata"
            if kind == "duplicate": bad["node"]["files"].append(bad["node"]["files"][0])
            if kind == "length": bad_receipts[0]["fault"]["size"] += 1
            with self.subTest(kind=kind), self.assertRaises(ValueError): schema.metadata_scene(before, bad, targets, bad_receipts)

    def test_per_disk_fault_hash_does_not_accept_another_drives_metadata(self):
        entry = {"disk": 1, "path": "bucket/key/xl.meta", "sha256": "second", "size": 8}
        faults = {"0:bucket/key/xl.meta": {"sha256": "first", "size": 8}, "1:bucket/key/xl.meta": {"sha256": "second", "size": 8}}
        with patch.object(fleet_node, "command", return_value=types.SimpleNamespace(returncode=1, stderr="xl file version err")):
            observed = fleet_node.inspect_metadata(Path("tool"), Path("path"), "bucket", "key", entry, faults)
            self.assertFalse(observed["metadataError"]["validMetadataClaimed"])
            entry["sha256"] = "first"
            with self.assertRaises(ValueError): fleet_node.inspect_metadata(Path("tool"), Path("path"), "bucket", "key", entry, faults)

    def test_unknown_get_requires_explicit_server_refusal_without_payload_or_absence(self):
        observed = {"acknowledged": False, "resultUnknown": False,
                    "response": {"ResponseMetadata": {"HTTPStatusCode": 500}, "Error": {"Code": "InternalError"}}}
        self.assertFalse(schema.unsupported_get(observed)["unknownTreatedAsMissing"])
        for kind in ("transport", "payload", "absence", "success"):
            bad = copy.deepcopy(observed)
            if kind == "transport": bad["resultUnknown"] = True
            if kind == "payload": bad["payload"] = {"sha256": "bytes"}
            if kind == "absence": bad["response"] = {"ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": "NoSuchKey"}}
            if kind == "success": bad["acknowledged"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError): schema.unsupported_get(bad)

    def test_full_128_object_streaming_manifest_keeps_cursor_gap_requirement_separate(self):
        cohorts = [{"cohort": [{"node": "node", "disk": i, "relativePath": f"bucket/key{n}/part.1",
            "versionId": "null", "partNumber": 1, "sha256": f"hash-{n}-{i}", "size": 81} for i in range(4)]} for n in range(128)]
        authority = {(item["disk"], item["relativePath"]): item for cohort in cohorts for item in cohort["cohort"]}
        def remote(node, action, *, parts):
            self.assertLessEqual(len(parts), 128)
            return {"complete": True, "parts": [{**part, "sha256": authority[part["disk"], part["relativePath"]]["sha256"],
                "physicalBitrotVerified": True, "physicalSize": 81} for part in parts]}
        fleet = types.SimpleNamespace(nodes=["node"], remote=remote, record=lambda label, value: value)
        self.assertEqual(verify_part_manifest(fleet, cohorts, "proof", count=128)["allPartsStreamingVerified"], 512)
        with self.assertRaises(ValueError): verify_all_parts(fleet, cohorts, "proof", count=128)
        with self.assertRaises(ValueError): verify_part_manifest(fleet, cohorts[:-1], "proof", count=128)

    def test_preserves_four_source_branches_and_disables_unrelated_automatic_heal(self):
        self.assertEqual(select("C09")[0]["variants"], list(schema.BRANCHES))
        self.assertIn(schema.CASE_ID, AUTOMATIC_CASES)
        self.assertEqual(fleet_node.runtime_config("schema-refusal")["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertEqual(schema.CONTRACT["candidateCommit"], "072b57b37559fbe626aeda8abf61617fdcbdd926")


if __name__ == "__main__": unittest.main()
