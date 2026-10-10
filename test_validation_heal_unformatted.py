import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import replace_drive, unformatted
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES


class UnformattedReplacementTests(unittest.TestCase):
    original_id = "12345678-1234-4234-8234-123456789abc"
    foreign_id = "22345678-1234-4234-8234-123456789abc"
    original_members = [f"32345678-1234-4234-8234-123456789ab{index}" for index in range(4)]
    foreign_members = [f"42345678-1234-4234-8234-123456789ab{index}" for index in range(4)]

    def format(self, foreign=False):
        members = self.foreign_members if foreign else self.original_members
        return {"version": "1", "format": "xl", "id": self.foreign_id if foreign else self.original_id,
                "xl": {"version": "3", "this": members[0], "sets": [members], "distributionAlgo": "SIPMOD+PARITY"}}

    def scene(self):
        before = {"node": {"formats": [{"disk": index, "sha256": "original-" + str(index), "value": {"id": self.original_id}}
                                        for index in range(4)],
                           "files": [{"disk": index, "path": "bucket/object/part.1", "sha256": "part-" + str(index), "size": 17}
                                     for index in range(4)]}}
        after = copy.deepcopy(before)
        after["node"]["files"] = after["node"]["files"][1:]
        after["node"]["formats"][0] = {"disk": 0, "present": False, "unformattedReplacement": True}
        return before, after

    def test_negative_scene_cannot_hide_survivor_write_or_foreign_reidentification(self):
        before, after = self.scene()
        self.assertFalse(unformatted.negative_scene(before, after, ("node", 0), "blank-unformatted", None)["productRepairClaimed"])
        foreign = {"sha256": "foreign-digest", "value": self.format(True)}
        after["node"]["formats"][0] = {"disk": 0, "sha256": foreign["sha256"], "value": foreign["value"]}
        unformatted.negative_scene(before, after, ("node", 0), "foreign-format", foreign)
        for kind in ("survivor", "target", "format", "foreign"):
            bad = copy.deepcopy(after)
            if kind == "survivor": bad["node"]["files"][0]["sha256"] = "changed"
            elif kind == "target": bad["node"]["files"].append(before["node"]["files"][0])
            elif kind == "format": bad["node"]["formats"][1]["value"]["id"] = "changed"
            else: bad["node"]["formats"][0]["value"]["id"] = self.original_id
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                unformatted.negative_scene(before, bad, ("node", 0), "foreign-format", foreign)

    def admin(self):
        return {"httpStatus": 200, "response": {"info": {"servers": [{"drives": [
            {"pool_index": 0, "set_index": 0, "disk_index": index, "endpoint": "endpoint-" + str(index),
             "uuid": self.original_members[index] if index else "", "state": "ok" if index else "unformatted disk",
             "runtimeState": "online" if index else "offline"} for index in range(4)]}]}}}

    def test_structured_refusal_requires_exact_slot_and_each_other_original_member(self):
        target = {"setSlot": 0, "diskUUID": self.original_members[0],
                  "cohort": [{"setSlot": index, "diskUUID": value} for index, value in enumerate(self.original_members)]}
        value = self.admin()
        self.assertFalse(unformatted.refusal(value, target, "endpoint-0", None)["unreadyMemberHealthyClaimed"])
        for field, changed in (("state", "ok"), ("state", "unknown"), ("runtimeState", "online"), ("endpoint", "other"), ("uuid", "invented")):
            bad = self.admin(); bad["response"]["info"]["servers"][0]["drives"][0][field] = changed
            with self.subTest(field=field), self.assertRaises(ValueError): unformatted.refusal(bad, target, "endpoint-0", None)
        bad = self.admin(); bad["response"]["info"]["servers"][0]["drives"][2]["uuid"] = self.foreign_members[2]
        with self.assertRaises(ValueError): unformatted.refusal(bad, target, "endpoint-0", None)

    def archive(self, root):
        state = root / "state"; state.mkdir()
        drive = root / "drive"; (drive / "quarantine" / "old-disk" / ".rustfs.sys").mkdir(parents=True)
        data = drive / "data"; data.mkdir()
        backup = drive / "quarantine" / "old-disk"
        (backup / ".rustfs.sys" / "format.json").write_text(json.dumps(self.format()))
        value = {"disk": 0, "dataPath": str(data), "backupPath": str(backup), "originalStillAtDataRoot": False,
                 "originalArchiveUnchanged": True, "original": {"kind": "original"}}
        return state, drive, data, backup, value

    def test_import_copies_released_donor_bytes_and_rejects_original_or_shared_members(self):
        with tempfile.TemporaryDirectory() as folder:
            state, drive, data, backup, value = self.archive(Path(folder))
            with patch.object(replace_drive, "attest", return_value=value):
                for donor in (self.format(), {**self.format(True), "xl": self.format()["xl"]},
                              {**self.format(True), "version": "future"}):
                    raw = json.dumps(donor).encode()
                    with self.assertRaises(ValueError): replace_drive.install_foreign(state, [drive], base64.b64encode(raw).decode(), hashlib.sha256(raw).hexdigest())
                    self.assertFalse(list(data.iterdir()))
                raw = b'  ' + json.dumps(self.format(True), indent=2).encode() + b'\n'
                proof = replace_drive.install_foreign(state, [drive], base64.b64encode(raw).decode(), hashlib.sha256(raw).hexdigest())
                self.assertEqual((data / ".rustfs.sys/format.json").read_bytes(), raw)
                self.assertFalse(proof["syntheticFormatOrUUIDUsed"])
                self.assertEqual(json.loads((backup / ".rustfs.sys/format.json").read_text()), self.format())
                with self.assertRaises(ValueError): replace_drive.install_foreign(state, [drive], base64.b64encode(raw).decode(), hashlib.sha256(raw).hexdigest())

    def test_negative_recovery_preserves_rejected_scene_and_cannot_claim_reconstruction(self):
        with tempfile.TemporaryDirectory() as folder:
            state, drive, data, backup, value = self.archive(Path(folder))
            (state / "disk-replacement.json").write_text("intent")
            (data / "foreign.bin").write_bytes(b"exact-rejected-scene")
            def tree(path):
                return {"kind": "rejected" if (path / "foreign.bin").exists() else "original"}
            with patch.object(replace_drive, "attest", return_value=value), patch.object(replace_drive, "tree_identity", side_effect=tree):
                proof = replace_drive.restore_original(state, [drive])
            self.assertEqual(json.loads((data / ".rustfs.sys/format.json").read_text()), self.format())
            self.assertEqual((drive / "quarantine/rejected-root/foreign.bin").read_bytes(), b"exact-rejected-scene")
            self.assertFalse(proof["productRepairClaimed"])
            self.assertTrue(proof["backupRestorationUsed"])
            self.assertFalse((state / "disk-replacement.json").exists())
            self.assertFalse(backup.exists())

    def test_both_source_branches_have_an_automatic_entry(self):
        self.assertEqual(select("B04")[0]["variants"], list(unformatted.BRANCHES))
        self.assertIn(unformatted.CASE_ID, AUTOMATIC_CASES)


if __name__ == "__main__": unittest.main()
