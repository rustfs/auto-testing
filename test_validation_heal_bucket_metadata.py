import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_heal import bucket_metadata as executor, bucket_records as backend, heal_fault as fault
from scripts.validation_heal import fleet_node as node
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select


class BucketMetadataTests(unittest.TestCase):
    bucket = "rv-heal-0123456789abcdef"
    value = b"XL2 " + b"\x01\x00\x03\x00" + b"same-run-config-envelope" * 10

    def test_system_allowlist_cannot_escape_or_widen_generic_faults(self):
        owned = backend.bucket_path(self.bucket, self.bucket) + "/xl.meta"
        self.assertEqual(backend.record_parts(owned, self.bucket, self.bucket)[-1], "xl.meta")
        for path, bucket in ((owned.replace("buckets", "config/iam"), self.bucket),
                             (owned + "/../xl.meta", self.bucket),
                             (owned.replace(".metadata.bin", ".bucket-incarnation"), self.bucket),
                             (owned.replace("xl.meta", "part.1"), self.bucket), (owned, "foreign")):
            with self.subTest(path=path, bucket=bucket), self.assertRaises(ValueError):
                backend.record_parts(path, bucket, self.bucket)
        with self.assertRaises(ValueError):
            fault.target_parts(owned, self.bucket)

    def test_unknown_envelope_preserves_every_byte_besides_reviewed_major(self):
        changed = backend.changed_bytes(self.value, "future-xl-schema")
        self.assertEqual(changed[:4], b"XL2 ")
        self.assertEqual(changed[4:6], b"\x02\x00")
        self.assertEqual(changed[6:], self.value[6:])
        for original in (b"", b"not-XL2!", changed):
            with self.assertRaises(ValueError):
                backend.changed_bytes(original, "future-xl-schema")

    def scene(self, root, kind):
        relative = backend.bucket_path(self.bucket, self.bucket) + "/xl.meta"
        path = root / "data" / relative
        path.parent.mkdir(parents=True)
        (root / "quarantine").mkdir()
        path.write_bytes(self.value)
        owner = {"runId": "test-run", "bucketPrefix": self.bucket}
        request = {"bucket": self.bucket, "faultId": "D11-negative-0", "relativePath": relative,
                   "expectedSha256": fault.digest(self.value), "kind": kind}
        return path, owner, request

    @unittest.skipUnless(Path('/proc/self/fdinfo').is_dir(), 'Linux fd/mount guard required')
    def test_faults_keep_originals_and_reject_unrecognized_post_fault_writes(self):
        for kind in ("missing", "truncate", "future-xl-schema"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                path, owner, request = self.scene(root, kind)
                with fault.directory(root) as fd:
                    receipt = backend.apply(fd, owner, fault, request)
                    self.assertEqual((root / "quarantine" / receipt["backup"]).read_bytes(), self.value)
                    self.assertTrue((root / "quarantine" / (request["faultId"] + ".intent.json")).exists())
                    path.write_bytes(b"new acknowledged config generation")
                    with self.assertRaisesRegex(ValueError, "post-fault rewrite"):
                        backend.recover(fd, owner, fault, request)
                    self.assertEqual(path.read_bytes(), b"new acknowledged config generation")
                    changed = backend.changed_bytes(self.value, kind)
                    if changed is None:
                        path.unlink()
                    else:
                        path.write_bytes(changed)
                    restored = backend.recover(fd, owner, fault, request)
                    self.assertEqual(path.read_bytes(), self.value)
                    self.assertFalse(restored["physicalRepairPassAllowed"])

    @unittest.skipUnless(Path('/proc/self/fdinfo').is_dir(), 'Linux fd/mount guard required')
    def test_symlink_and_hardlink_record_targets_are_rejected_before_mutation(self):
        for kind in ("symlink", "hardlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                path, owner, request = self.scene(root, "truncate")
                other = root / "unrelated"
                other.write_bytes(self.value)
                path.unlink()
                if kind == "symlink":
                    path.symlink_to(other)
                else:
                    path.hardlink_to(other)
                with fault.directory(root) as fd, self.assertRaises((ValueError, OSError)):
                    backend.apply(fd, owner, fault, request)
                self.assertEqual(other.read_bytes(), self.value)
                self.assertFalse(list((root / "quarantine").iterdir()))

    def test_empty_default_success_and_transport_error_are_not_unknown_schema_refusal(self):
        good = {"acknowledged": False, "resultUnknown": False, "response": {
            "ResponseMetadata": {"HTTPStatusCode": 500}, "Error": {"Code": "InternalError"}}}
        executor.refusal(good)
        for observed in ({"acknowledged": True, "resultUnknown": False, "response": {}},
                         {"acknowledged": False, "resultUnknown": True, "response": None},
                         {**good, "response": {"ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": "NoSuchBucket"}}}):
            with self.assertRaises(ValueError):
                executor.refusal(observed)
        with self.assertRaises(ValueError):
            executor.configuration({"acknowledged": True, "resultUnknown": False, "response": {"ResponseMetadata": {}}},
                                   "get_bucket_versioning")

    def test_duplicate_files_or_sensitive_iam_block_the_oracle(self):
        row = {"disk": 0, "path": "exact-record", "size": 5, "sha256": "a" * 64}
        sample = {"node": {"credentialsRead": False, "files": [row],
            "iamNamespace": [{"disk": 0, "present": True, "traversed": False, "formatOnlyRead": True,
                              "sensitiveSubtrees": dict.fromkeys(backend.IAM_SENSITIVE_SUBTREES, False)}]}}
        executor.system_files(sample)
        for kind in ("duplicate", "iam", "traversal"):
            broken = copy.deepcopy(sample)
            if kind == "duplicate":
                broken["node"]["files"].append(row)
            elif kind == "iam":
                broken["node"]["iamNamespace"][0]["sensitiveSubtrees"]["users"] = True
            else:
                broken["node"]["iamNamespace"][0]["traversed"] = True
            with self.assertRaises(ValueError):
                executor.system_files(broken)

    def observe_format(self, root, *, corrupt=False):
        path = root / "data/.rustfs.sys" / backend.IAM_FORMAT_KEY / "xl.meta"
        calls, reads = [], []

        def digest(file):
            reads.append(file)
            return hashlib.sha256(file.read_bytes()).hexdigest()

        def command(*args, **kwargs):
            calls.append(args)
            if corrupt:
                return SimpleNamespace(returncode=1, stdout="")
            if args[1] == "--verify-inline":
                value = {"physicalBitrotVerified": True, "bucket": ".rustfs.sys", "key": backend.IAM_FORMAT_KEY}
            else:
                value = {"bucket": ".rustfs.sys", "key": backend.IAM_FORMAT_KEY,
                         "versions": [{"deleted": False, "versionId": "null", "inline": {"length": 42}}]}
            return SimpleNamespace(returncode=0, stdout=json.dumps(value))

        result = backend.snapshot((root,), {"bucketPrefix": self.bucket},
            {"buckets": [self.bucket, self.bucket + "-control"],
             "metadataFaults": {"0:" + str(path.relative_to(root / "data")):
                                {"size": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}}},
            node.checked, digest, Path('/inspector'), command)
        return result, reads, calls

    def test_startup_format_is_read_only_frozen_and_verified_without_payload_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            path = root / "data/.rustfs.sys" / backend.IAM_FORMAT_KEY / "xl.meta"
            path.parent.mkdir(parents=True)
            path.write_bytes(self.value)
            observed, reads, calls = self.observe_format(root)
            self.assertEqual(reads, [path])
            self.assertEqual([call[-1] for call in calls], [backend.IAM_FORMAT_KEY, "null"])
            self.assertEqual(observed["files"][0]["recordClass"], "iam-format")
            self.assertTrue(observed["iamNamespace"][0]["present"])
            self.assertNotIn("same-run-config-envelope", json.dumps(observed))
            before = executor.system_files({"node": observed})
            path.write_bytes(self.value + b"changed")
            after, _, _ = self.observe_format(root)
            self.assertNotEqual(before, executor.system_files({"node": after}))
            with self.assertRaises(ValueError):
                backend.record_parts(str(path.relative_to(root / "data")), self.bucket, self.bucket)

    def test_sensitive_subtree_presence_blocks_before_any_walk_or_file_read(self):
        for subtree in backend.IAM_SENSITIVE_SUBTREES:
            with self.subTest(subtree=subtree), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                folder = root / "data/.rustfs.sys/config/iam" / subtree
                folder.mkdir(parents=True)
                (folder / "never-read").write_bytes(b"sensitive")
                with patch.object(backend.os, "walk", side_effect=AssertionError("IAM walk")), \
                     self.assertRaisesRegex(ValueError, "sensitive IAM subtree"):
                    backend.snapshot((root,), {"bucketPrefix": self.bucket},
                        {"buckets": [self.bucket, self.bucket + "-control"]}, node.checked,
                        lambda _: self.fail("credential read"), Path('/inspector'), lambda *_: self.fail("decode"))

    def test_iam_format_decode_failure_cannot_use_bucket_fault_exception(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            path = root / "data/.rustfs.sys" / backend.IAM_FORMAT_KEY / "xl.meta"
            path.parent.mkdir(parents=True)
            path.write_bytes(self.value)
            with self.assertRaisesRegex(ValueError, "decoding failed"):
                self.observe_format(root, corrupt=True)

    def test_remote_node_script_compiles_with_dedicated_backend(self):
        directory = Path(__file__).parent / "scripts/validation_heal"
        source = (directory / "fleet_node.py").read_text()
        for name in ("heal_fault", "bucket_records"):
            source += f"\nimport types\n{name} = types.ModuleType('{name}')\nexec(" + repr((directory / (name + ".py")).read_text()) + f", {name}.__dict__)\n"
        compile(source, "remote-D11", "exec")
        self.assertIn(executor.CASE_ID, AUTOMATIC_CASES)
        self.assertEqual(select(executor.CASE_ID)[0]["variants"], list(executor.BRANCHES))

    def test_system_snapshot_receives_the_real_service_bucket_prefix(self):
        owner = {"bucket": self.bucket, "inspectorSha256": "a" * 64}
        with patch.object(node, "owned", return_value=(Path('/owned'), (), owner)), \
             patch.object(node, "stopped"), patch.object(node, "checked", side_effect=lambda path, _: path), \
             patch.object(node, "digest", return_value="a" * 64), patch.object(node, "fault_module"), \
             patch.object(node, "bucket_records", backend, create=True), patch.object(backend, "snapshot", return_value={}) as observe:
            node.bucket_record_operation({"runId": "test", "action": "bucket-record-snapshot"})
            self.assertEqual(observe.call_args.args[1]["bucketPrefix"], self.bucket)

    @unittest.skipUnless(Path('/proc/self/fdinfo').is_dir(), 'Linux fd/mount guard required')
    def test_first_system_fault_creates_a_checked_mount_and_lease_marker(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            path, _, request = self.scene(root, "missing")
            owner = {"runId": "test-run", "bucket": self.bucket}
            with patch.object(node, "owned", return_value=(root, (root,), owner)), patch.object(node, "stopped"), \
                 patch.object(node, "fault_root", return_value=(root, owner)), patch.object(node, "fault_module", return_value=fault), \
                 patch.object(node, "bucket_records", backend, create=True):
                node.bucket_record_operation({**request, "runId": "test-run", "action": "bucket-record-apply"})
                self.assertFalse(path.exists())
                with fault.directory(root) as fd:
                    marker = fault.checked_owner(fd, "test-run")
                self.assertEqual(marker["bucketPrefix"], self.bucket)


if __name__ == "__main__":
    unittest.main()
