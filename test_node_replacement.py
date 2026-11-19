"""Regression tests for replacement evidence and resource ownership."""

import io
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from scripts.node_replacement import Case, LABEL, archive_files, cleanup, reconstruction_sources, shard_manifest, system_metadata_manifest, topology
from scripts import node_replacement_node as node


def disks(members):
    return [{".rustfs.sys/format.json": json.dumps({"format": "xl", "id": "pool",
        "xl": {"this": identity, "sets": [members]}}).encode()} for identity in members]


class ReplacementEvidenceTests(unittest.TestCase):
    def test_quarantine_stays_inside_the_endpoint_mount_and_preserves_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory) / 'mnmd'
            part = data / 'bucket/key/generation/part.1'
            part.parent.mkdir(parents=True)
            part.write_bytes(b'committed shard')
            metadata = data / 'bucket/key/xl.meta'
            metadata.write_bytes(b'committed metadata')
            with patch.object(node, 'process', return_value={'running': False}), patch.object(node, 'data_roots', return_value=[data]):
                node.quarantine([['bucket/key/generation']])
            self.assertFalse(part.exists())
            self.assertEqual(metadata.read_bytes(), b'committed metadata')
            archived = list(data.glob('.nr-oracle-*/0/part.1'))
            self.assertEqual(len(archived), 1)
            self.assertEqual(archived[0].read_bytes(), b'committed shard')

    def test_cold_start_control_ignores_metadata_errors_from_earlier_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'logs').mkdir()
            log = root / 'logs/rustfs.log'
            log.write_text('old load_pool_meta failed\n')
            with patch.object(node, 'BACKING', root), patch.dict('os.environ', {'RUSTFS_ACCESS_KEY': 'testaccess', 'RUSTFS_SECRET_KEY': 'testsecret'}):
                cursor = node.log_cursors()
                with log.open('a') as output: output.write('new harmless startup\n')
                self.assertFalse(node.metadata_failures(cursor)['observed'])
                with log.open('a') as output: output.write('InsufficientReadQuorum .rustfs.sys pool.bin\n')
                self.assertTrue(node.metadata_failures(cursor)['observed'])
                cursor = node.log_cursors()
                with log.open('a') as output: output.write('init failed during classify_pool_meta_absence: pool.bin replica unavailable\n')
                self.assertTrue(node.metadata_failures(cursor)['observed'])

    def test_live_archive_retries_only_expected_concurrent_file_changes(self):
        with tempfile.TemporaryDirectory() as root:
            case = self.case(root)
            changing = subprocess.CompletedProcess([], 1, b'', b'tar: ./key: file changed as we read it\n')
            success = subprocess.CompletedProcess([], 0, b'archive', b'')
            with patch('scripts.node_replacement.subprocess.run', side_effect=[changing, success]) as run, \
                 patch('scripts.node_replacement.time.sleep'):
                self.assertEqual(case.docker('exec', binary=True, live_archive=True), b'archive')
                self.assertEqual(run.call_count, 2)
            for live, error in ((False, changing), (True, subprocess.CompletedProcess([], 2, b'', b'tar: Permission denied'))):
                with patch('scripts.node_replacement.subprocess.run', return_value=error):
                    with self.assertRaises(RuntimeError): case.docker('exec', live_archive=live)

    def test_existing_backing_file_is_rejected_before_formatting(self):
        with tempfile.TemporaryDirectory() as root:
            backing = Path(root)
            image = backing / 'test-node2-disk1.xfs'
            image.write_bytes(b'preserved evidence')
            with patch.object(node, 'BACKING', backing), patch.object(node, 'roots', return_value=[backing / 'mount']), \
                 patch.object(node, 'stop'), patch.object(node.signal, 'signal'), patch.object(node.os, 'mknod'), \
                 patch.object(node, 'command') as command, \
                 patch.dict('os.environ', {'NR_RUN_ID': 'test', 'NR_SLOT': '2'}):
                with self.assertRaises(FileExistsError): node.serve()
            command.assert_not_called()
            self.assertEqual(image.read_bytes(), b'preserved evidence')

    def test_mount_inventory_rejects_root_devices_wrong_filesystems_and_duplicate_devices(self):
        for invalid in (None, 'root', 'fstype', 'duplicate'):
            with self.subTest(invalid=invalid):
                rows = [{'target': '/data/rustfs1', 'fstype': 'xfs', 'uuid': 'one', 'maj:min': '7:1'},
                        {'target': '/data/rustfs2', 'fstype': 'xfs', 'uuid': 'two', 'maj:min': '7:2'}]
                if invalid == 'fstype': rows[0]['fstype'] = 'ext4'
                if invalid == 'duplicate': rows[1]['maj:min'] = '7:1'
                outputs = [json.dumps({'filesystems': [row]}) for row in rows]
                devices = [types.SimpleNamespace(st_dev=value) for value in (1, 1 if invalid == 'root' else 2, 3)]
                with patch.object(node, 'roots', return_value=[Path(row['target']) for row in rows]), \
                     patch.object(node, 'command', side_effect=outputs), patch.object(node.os, 'stat', side_effect=devices), \
                     patch.object(node.subprocess, 'run', return_value=types.SimpleNamespace(returncode=1)):
                    if invalid:
                        with self.assertRaises(RuntimeError): node.mounts()
                    else:
                        observed = node.mounts()
                        self.assertFalse(observed[0]['endpointIsMountpoint'])

    def test_ready_and_complete_object_parts_cannot_hide_missing_pool_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            case = Case('image', Path(root), 10)
            files = {'bucket/key/xl.meta': b'meta', 'bucket/key/generation/part.1': b'shard'}
            case.bucket = 'bucket'
            case.scenes = lambda name: [files] * 4
            with patch('scripts.node_replacement.time.monotonic', side_effect=[0, 11]):
                with self.assertRaisesRegex(ValueError, 'repair deadline'):
                    case.wait_repair('replacement', {'key': {}})
            self.assertFalse(case.assertions['systemMetadataRecovered'])
            self.assertTrue(case.assertions['replacementShardsPhysicallyPresent'])

    def test_both_pool_metadata_objects_require_nonempty_xlmeta(self):
        valid = {'.rustfs.sys/format.json': b'format',
                 '.rustfs.sys/pool.bin/xl.meta': b'XL2 ' + b'x' * 64,
                 '.rustfs.sys/pool.bin.identity/xl.meta': b'XL2 ' + b'y' * 64}
        self.assertEqual(set(system_metadata_manifest(valid)), {'pool.bin', 'pool.bin.identity'})
        for path in valid:
            for replacement in (None, b'', b'XL2 ', b'broken' * 16):
                if path.endswith('format.json') and replacement:
                    continue
                changed = dict(valid)
                if replacement is None:
                    del changed[path]
                else:
                    changed[path] = replacement
                with self.subTest(path=path, replacement=replacement), self.assertRaises(ValueError):
                    system_metadata_manifest(changed)

    def test_forced_reconstruction_uses_actual_topology_quorum(self):
        self.assertEqual(reconstruction_sources(1, 2), 1)
        self.assertEqual(reconstruction_sources(4, 4), 2)
        with self.assertRaises(ValueError): reconstruction_sources(4, 1)

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
