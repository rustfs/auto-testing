"""Regression tests for replacement evidence and resource ownership."""

import io
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts.node_replacement import Case, LABEL, archive_files, cleanup, shard_manifest, topology


def disks(members):
    return [{".rustfs.sys/format.json": json.dumps({"format": "xl", "id": "pool",
        "xl": {"this": identity, "sets": [members]}}).encode()} for identity in members]


class ReplacementEvidenceTests(unittest.TestCase):
    def case(self, root):
        case = Case("image", Path(root), 30)
        case.objects = {"key": {"size": 4, "sha256": hashlib.sha256(b"data").hexdigest()}}
        return case

    def test_reconstructed_reads_must_match_the_acknowledged_bytes(self):
        for value in (b"data", b"bad!", b"data-extra"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as root:
                case = self.case(root)
                client = Mock()
                client.get_object.return_value = {"Body": io.BytesIO(value)}
                case.s3 = lambda name: client
                if value == b"data":
                    case.read(["replacement"], "proof")
                else:
                    with self.assertRaises(ValueError): case.read(["replacement"], "proof")

    def test_negative_control_requires_read_quorum_failure_and_rejects_success(self):
        for status, code in ((503, "SlowDownRead"), (503, "InternalError"), (404, "NoSuchKey"), (403, "AccessDenied"), (200, None)):
            with self.subTest(status=status, code=code), tempfile.TemporaryDirectory() as root:
                case = self.case(root)
                client = Mock()
                if status == 200:
                    client.get_object.return_value = {"Body": io.BytesIO(b"data")}
                else:
                    error = RuntimeError("request failed")
                    error.response = {"ResponseMetadata": {"HTTPStatusCode": status}, "Error": {"Code": code}}
                    client.get_object.side_effect = error
                case.s3 = lambda name: client
                if code == "SlowDownRead":
                    case.read_without_replacement("survivor")
                else:
                    with self.assertRaises(ValueError): case.read_without_replacement("survivor")

    def test_new_disk_must_take_the_original_slot_with_inherited_or_fresh_uuid(self):
        original = topology(disks(["a", "b", "c", "old"]))
        changed = disks(["a", "b", "c", "new"])
        self.assertEqual(topology(changed, original=original)["members"][-1], "new")
        self.assertEqual(topology(disks(["a", "b", "c", "old"]), original=original)["members"][-1], "old")
        for invalid in (disks(["a", "b", "other", "new"]),
                        disks(["a", "b", "new", "c"])):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                topology(invalid, original=original)

    def test_separate_pools_partial_membership_and_wrong_pool_fail(self):
        original = topology(disks(["a", "b", "c", "old"]))
        for field in ("pool", "sets", "this"):
            changed = disks(["a", "b", "c", "new"])
            value = json.loads(changed[-1][".rustfs.sys/format.json"])
            if field == "pool": value["id"] = "another-pool"
            elif field == "sets": value["xl"]["sets"] = [["a", "b"], ["c", "new"]]
            else: value["xl"]["this"] = "c"
            changed[-1][".rustfs.sys/format.json"] = json.dumps(value).encode()
            with self.subTest(field=field), self.assertRaises(ValueError):
                topology(changed, original=original)

    def test_metadata_only_or_another_objects_shard_cannot_count_as_repair(self):
        files = {"bucket/key/xl.meta": b"meta", "bucket/key/generation/part.1": b"shard"}
        self.assertEqual(shard_manifest(files, "bucket", ["key"])["key"]["size"], 5)
        for changed in ({"bucket/key/xl.meta": b"meta"},
                        {**files, "bucket/key/another-generation/part.1": b"stale"},
                        {**files, "bucket/key/generation/part.1": b""},
                        {**files, "bucket/key/xl.meta": b""}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                shard_manifest(changed, "bucket", ["key"])
        with self.assertRaises(ValueError):
            shard_manifest(files, "bucket", ["key", "offline-write"])

    def test_archives_reject_links_traversal_and_duplicate_file_evidence(self):
        for kind in ("good", "traversal", "absolute", "link", "duplicate"):
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w") as archive:
                name = {"traversal": "../key", "absolute": "/key"}.get(kind, "./key")
                item = tarfile.TarInfo(name)
                item.size = 4
                if kind == "link":
                    item.type, item.linkname = tarfile.SYMTYPE, "other"
                archive.addfile(item, io.BytesIO(b"data"))
                if kind == "duplicate": archive.addfile(item, io.BytesIO(b"data"))
            if kind == "good":
                self.assertEqual(archive_files(output.getvalue()), {"key": b"data"})
            else:
                with self.subTest(kind=kind), self.assertRaises(ValueError):
                    archive_files(output.getvalue())

    def test_recovery_removes_only_owned_resources_and_keeps_cleanup_failures(self):
        for failure in (None, "ownership", "daemon", "remove"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                resources = {"runId": "run", "network": "net", "containers": ["old", "new"], "volumes": ["disk"]}
                (root / "resources.json").write_text(json.dumps(resources))
                (root / ".runtime.env").write_text("private")
                removed = []
                def invoke(argv, **kwargs):
                    kind, action, name = argv[1], argv[2], argv[-1]
                    if action == "inspect":
                        if name == "old":
                            return subprocess.CompletedProcess(argv, 1, "", "No such container: old")
                        if failure == "daemon":
                            return subprocess.CompletedProcess(argv, 1, "", "Cannot connect to daemon")
                        labels = {LABEL: "other" if failure == "ownership" else "run"}
                        value = {"Config": {"Labels": labels}} if kind == "container" else {"Labels": labels}
                        return subprocess.CompletedProcess(argv, 0, json.dumps([value]), "")
                    removed.append(name)
                    return subprocess.CompletedProcess(argv, int(failure == "remove"), "", "")
                with patch("scripts.node_replacement.subprocess.run", side_effect=invoke):
                    failures = cleanup(root)
                self.assertEqual(bool(failures), failure is not None)
                self.assertFalse((root / ".runtime.env").exists())
                self.assertNotIn("old", removed)
                if failure in ("ownership", "daemon"):
                    self.assertEqual(removed, [])

    def test_repeated_cleanup_accepts_dockers_distinct_missing_resource_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "resources.json").write_text(json.dumps({"runId": "run", "network": "net",
                "containers": ["old"], "volumes": ["disk"]}))
            def invoke(argv, **kwargs):
                errors = {"container": "No such container: old", "volume": "get disk: no such volume",
                          "network": "network net not found"}
                return subprocess.CompletedProcess(argv, 1, "[]", "Error response from daemon: " + errors[argv[1]])
            with patch("scripts.node_replacement.subprocess.run", side_effect=invoke):
                self.assertEqual(cleanup(root), [])


if __name__ == "__main__":
    unittest.main()
