import copy
import errno
import json
import os
from pathlib import Path
from types import SimpleNamespace
from io import BytesIO
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import bounded_volume as volume, capacity, fleet_node
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
from scripts.validation_heal.heal_probe import Probe
from test_validation_heal_authorization import binding


ROOT = Path(__file__).parent


class CapacityContractTests(unittest.TestCase):
    def test_all_catalogued_automatic_cases_reach_the_top_level_dispatcher(self):
        for case in sorted(AUTOMATIC_CASES):
            with self.subTest(case=case):
                observed = subprocess.run(["bash", str(ROOT / "scripts/run_validation_case.sh"),
                    case, "/nonexistent/candidate.json", "a" * 40, "/nonexistent/evidence"],
                    capture_output=True, text=True, timeout=5)
                self.assertEqual(observed.returncode, 2)
                self.assertIn("candidate manifest missing", observed.stderr)
                self.assertNotIn("executor is not migrated", observed.stderr)
        self.assertIn(capacity.CASE_ID, AUTOMATIC_CASES)
        self.assertEqual(select(capacity.CASE_ID)[0]["variants"], list(capacity.BRANCHES))

    def test_inner_dispatch_uses_the_actual_capacity_executor(self):
        observed = subprocess.run(["bash", str(ROOT / "scripts/validation_heal/run_case.sh"),
            "--validation-case", capacity.CASE_ID, "--dry-run"], capture_output=True, text=True, timeout=5)
        self.assertEqual(observed.returncode, 0, observed.stderr)
        source = (ROOT / "scripts/validation_heal/run_case.sh").read_text()
        self.assertIn("heal.enospc-inodes-and-quota) driver=capacity.py", source)

    def test_capacity_refusals_do_not_accept_unrelated_denials_or_success(self):
        observed = {"acknowledged": False, "resultUnknown": False, "clientCallInvoked": True,
            "response": {"ResponseMetadata": {"HTTPStatusCode": 400}, "Error": {
                "Code": "InvalidRequest", "Message": "Bucket quota exceeded. Current usage: 1 bytes, limit: 2 bytes"}}}
        self.assertEqual(capacity.refusal(observed, quota=True)["state"], "REFUSED")
        for change in ("success", "not-invoked", "auth", "generic-invalid", "wrong-http"):
            broken = copy.deepcopy(observed)
            if change == "success": broken["acknowledged"] = True
            elif change == "not-invoked": broken["clientCallInvoked"] = False
            elif change == "auth": broken["response"]["Error"]["Code"] = "AccessDenied"
            elif change == "generic-invalid": broken["response"]["Error"]["Message"] = "bad query"
            else: broken["response"]["ResponseMetadata"]["HTTPStatusCode"] = 503
            with self.subTest(change=change), self.assertRaises(ValueError):
                capacity.refusal(broken, quota=True)
        unknown = {"acknowledged": False, "resultUnknown": True, "clientCallInvoked": True}
        self.assertFalse(capacity.refusal(unknown, quota=False)["retryAllowed"])
        storage = {**observed, "response": {"ResponseMetadata": {"HTTPStatusCode": 503},
            "Error": {"Code": "ServiceUnavailable"}}}
        self.assertEqual(capacity.refusal(storage, quota=False)["state"], "REFUSED")
        with self.assertRaises(ValueError): capacity.refusal(observed, quota=False)

    def test_exact_candidate_probe_distinguishes_blocks_inodes_and_release(self):
        observed = {"uid": 0, "gid": 0, "device": 8, "writeErrno": errno.ENOSPC,
                    "freeBlocks": 0, "availableBlocks": 0, "freeInodes": 91}
        volume.validate_probe(observed, 8, capacity.BRANCHES[0], True)
        with self.assertRaises(ValueError): volume.validate_probe(observed, 8, capacity.BRANCHES[1], True)
        inode = {**observed, "freeInodes": 0, "freeBlocks": 11, "availableBlocks": 11}
        volume.validate_probe(inode, 8, capacity.BRANCHES[1], True)
        released = {**inode, "freeInodes": 30, "writeErrno": 0}
        volume.validate_probe(released, 8, capacity.BRANCHES[1], False)
        for name, value in (("uid", 1000), ("device", 9), ("writeErrno", errno.EACCES),
                            ("freeInodes", True), ("writeErrno", 0)):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                volume.validate_probe({**inode, name: value}, 8, capacity.BRANCHES[1], True)
        with self.assertRaises(ValueError): volume.validate_probe(inode, 8, capacity.BRANCHES[1], False)

    def test_fill_is_bounded_and_requires_real_enospc(self):
        stats = SimpleNamespace(f_bfree=0, f_ffree=900)
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / "padding").mkdir()
            with patch.object(volume.os, "open", return_value=7), patch.object(volume.os, "close"), \
                 patch.object(volume.os, "write", side_effect=[1024**2, OSError(errno.ENOSPC, "full")]), \
                 patch.object(volume.os, "fsync"), patch.object(volume.os, "statvfs", return_value=stats):
                result = volume.fill(folder, capacity.BRANCHES[0])
                self.assertEqual(result["allocatedFillerBytes"], 1024**2)
                self.assertEqual(result["actualErrno"], errno.ENOSPC)
            with patch.object(volume.os, "open", side_effect=OSError(errno.ENOSPC, "inodes")), \
                 patch.object(volume.os, "statvfs", return_value=SimpleNamespace(f_bfree=500, f_ffree=0)):
                self.assertEqual(volume.fill(folder, capacity.BRANCHES[1])["actualErrno"], errno.ENOSPC)
            with patch.object(volume.os, "open", side_effect=OSError(errno.EIO, "unrelated")), self.assertRaises(ValueError):
                volume.fill(folder, capacity.BRANCHES[1])

    def test_recovery_rejects_unrecognized_filler_without_deleting_it(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / "padding").mkdir()
            unexpected = folder / "padding" / "acknowledged-object"
            unexpected.write_bytes(b"preserve")
            with self.assertRaises(ValueError): volume.release_filler(folder)
            self.assertEqual(unexpected.read_bytes(), b"preserve")

    def test_block_fill_consumes_small_tail_after_large_write_enospc(self):
        tail = SimpleNamespace(f_bfree=1, f_ffree=900, f_frsize=4096)
        full = SimpleNamespace(f_bfree=0, f_ffree=900, f_frsize=4096)
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); (folder / 'padding').mkdir()
            with patch.object(volume.os, 'open', return_value=7), patch.object(volume.os, 'close'), \
                    patch.object(volume.os, 'write', side_effect=[1024**2, OSError(errno.ENOSPC, 'large allocation'),
                        4096, OSError(errno.ENOSPC, 'full')]) as write, patch.object(volume.os, 'fsync'), \
                    patch.object(volume.os, 'statvfs', side_effect=[tail, full, full]):
                result = volume.fill(folder, capacity.BRANCHES[0])
            self.assertEqual(result['allocatedFillerBytes'], 1024**2 + 4096)
            self.assertEqual(result['freeBlocks'], 0)
            self.assertEqual(result['actualErrno'], errno.ENOSPC)
            self.assertEqual(len(write.call_args_list[2].args[1]), 4096)

    def test_minimum_block_refusal_with_free_blocks_never_claims_full_exhaustion(self):
        tail = SimpleNamespace(f_bfree=1, f_ffree=900, f_frsize=4096)
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); (folder / 'padding').mkdir()
            with patch.object(volume.os, 'open', return_value=7), patch.object(volume.os, 'close'), \
                    patch.object(volume.os, 'write', side_effect=OSError(errno.ENOSPC, 'refused')) as write, \
                    patch.object(volume.os, 'statvfs', return_value=tail), \
                    self.assertRaisesRegex(ValueError, 'minimum-block fill left 1 free blocks'):
                volume.fill(folder, capacity.BRANCHES[0])
            self.assertEqual(write.call_count, 2)

    def test_loop_detach_binding_requires_the_exact_backing_inode_and_device(self):
        row = {"name": "/dev/loop7", "back-file": "/owned/image", "back-ino": 31,
               "back-maj:min": f"{os.major(2049)}:{os.minor(2049)}"}
        with patch.object(volume, "command", return_value=json.dumps({"loopdevices": [row]})), \
             patch.object(volume, "regular", return_value=SimpleNamespace(st_ino=31, st_dev=2049)):
            self.assertEqual(volume.loop_for(Path("/owned/image")), "/dev/loop7")
            for change in ("inode", "device", "duplicate", "nonloop"):
                rows = [dict(row)]
                if change == "inode": rows[0]["back-ino"] = 99
                elif change == "device": rows[0]["back-maj:min"] = "999:999"
                elif change == "duplicate": rows.append(dict(row))
                else: rows[0]["name"] = "/dev/sda"
                with self.subTest(change=change), patch.object(volume, "command", return_value=json.dumps({"loopdevices": rows})), \
                     self.assertRaises(ValueError):
                    volume.loop_for(Path("/owned/image"))

    def test_only_the_dedicated_runtime_can_allocate_bounded_volumes(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            with self.assertRaises(ValueError):
                volume.create(state, (), {"runtimeProfile": "manual", "runId": "gh-1-1-h1"}, capacity.BRANCHES[0])
            self.assertFalse((state / volume.MARKER).exists())
        config = fleet_node.runtime_config("bounded-capacity")
        self.assertEqual(config["RUSTFS_UNSAFE_BYPASS_DISK_CHECK"], "true")
        self.assertEqual(config["RUSTFS_HEAL_MRF_ENABLE"], "false")
        self.assertEqual(config["RUSTFS_HEAL_AUTO_HEAL_ENABLE"], "false")
        self.assertEqual(config["RUSTFS_SCANNER_ENABLED"], "false")

    def test_bounded_journal_profile_requires_fixed_block_exhaustion_and_disables_background_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            owner = {"runtimeProfile": "journal-bounded", "runId": "gh-1-1-h1"}
            with self.assertRaisesRegex(ValueError, "only fixed block"):
                volume.create(state, (), owner, "bounded-inode-exhaustion")
            self.assertFalse((state / volume.MARKER).exists())
            with self.assertRaises(ValueError): volume.views(state, (), owner)
            with self.assertRaises(ValueError): volume.properties(state, (), owner)
        config = fleet_node.runtime_config("journal-bounded")
        self.assertEqual(config["RUSTFS_UNSAFE_BYPASS_DISK_CHECK"], "true")
        self.assertEqual(config["RUSTFS_HEAL_PAGE_PARALLEL_ENABLE"], "false")
        for field in ("RUSTFS_HEAL_MRF_ENABLE", "RUSTFS_HEAL_AUTO_HEAL_ENABLE", "RUSTFS_SCANNER_ENABLED"):
            self.assertEqual(config[field], "false")

    def test_bounded_journal_start_uses_only_verified_private_service_bindings(self):
        state, roots = Path('/owned/state'), [Path('/owned/disk' + str(i)) for i in range(4)]
        owner = {"runtimeProfile": "journal-bounded", "unit": "owned-test.service", "binarySha256": "a" * 64,
                 "volumes": [str(root / 'data') for root in roots]}
        bindings = ['--property=PrivateMounts=true', '--property=BindPaths=/verified/private/data:/owned/disk0/data']
        with patch.object(fleet_node, "owned", return_value=(state, roots, owner)), patch.object(fleet_node, "stopped"), \
             patch.object(fleet_node, "digest", return_value=owner["binarySha256"]), \
             patch.object(fleet_node, "command") as command, \
             patch.object(fleet_node, "gate_module", return_value=SimpleNamespace(checked_gate=lambda *args: None, start_properties=lambda *args: [])), \
             patch.object(fleet_node, "quorum_gate_module", return_value=SimpleNamespace(properties=lambda *args: [])), \
             patch.object(fleet_node, "replacement_module", return_value=SimpleNamespace(start_properties=lambda *args: [])), \
             patch.object(fleet_node, "volume_module", return_value=SimpleNamespace(properties=lambda *args: bindings)):
            fleet_node.start({"runId": "gh-1-1-h1"})
        issued = command.call_args.args
        self.assertEqual(issued[0], 'systemd-run')
        for property in bindings: self.assertIn(property, issued)
        self.assertIn('--property=RemainAfterExit=yes', issued)
        self.assertEqual(issued[-4:], tuple(owner["volumes"]))

    def test_bounded_journal_capacity_refuses_arbitrary_limits_paths_or_process_birth(self):
        owner = {"runtimeProfile": "journal-bounded"}
        identity = {field: field for field in fleet_node.PROCESS_FIELDS}; identity['pid'] = 50
        request = {"runId": "gh-1-1-h1", "action": "bounded-volumes-fill", "expected": identity}
        backend = SimpleNamespace(views=lambda *args: (), read=lambda *args: {'branch': 'bounded-volume-enospc'},
                                  fill=lambda *args: {}, witness=lambda *args, **kwargs: {})
        with patch.object(fleet_node, 'owned', return_value=(None, (), owner)), \
             patch.object(fleet_node, 'volume_module', return_value=backend):
            with patch.object(fleet_node, 'process', side_effect=[identity, identity]):
                self.assertTrue(fleet_node.volume_operation(request)['sameOriginalProcess'])
            for extra in ({'path': '/foreign'}, {'imageBytes': 10**12}, {'branch': 'bounded-inode-exhaustion'}):
                with self.subTest(extra=extra), self.assertRaises(ValueError): fleet_node.volume_operation({**request, **extra})
            with patch.object(fleet_node, 'process', return_value={**identity, 'startTicks': 999}), self.assertRaises(ValueError):
                fleet_node.volume_operation(request)

    def test_stopped_fill_is_restricted_to_fixed_journal_volumes_and_stopped_candidate(self):
        owner = {"runtimeProfile": "journal-bounded"}
        events = []
        backend = SimpleNamespace(read=lambda *args: {"branch": "bounded-volume-enospc"},
            views=lambda *args: tuple(range(4)), fill=lambda view, branch: events.append((view, branch)) or {"actualErrno": 28})
        request = {"runId": "gh-1-1-h1", "action": "bounded-volumes-fill", "quiescent": True}
        with patch.object(fleet_node, "owned", return_value=(None, (), owner)), \
             patch.object(fleet_node, "stopped") as stopped, patch.object(fleet_node, "volume_module", return_value=backend):
            result = fleet_node.volume_operation(request)
            stopped.assert_called_once_with(owner)
            self.assertEqual(events, [(index, "bounded-volume-enospc") for index in range(4)])
            self.assertTrue(result["candidateProcessWitnessRequired"])
            self.assertFalse(result["productRepairClaimed"])
            for changes in ({"path": "/shared"}, {"expected": {}}, {"limitBytes": 1}):
                events.clear()
                with self.assertRaises(ValueError): fleet_node.volume_operation({**request, **changes})
                self.assertFalse(events)
            for profile in ("manual", "bounded-capacity"):
                owner["runtimeProfile"] = profile; events.clear()
                with self.assertRaises(ValueError): fleet_node.volume_operation(request)
                self.assertFalse(events)
            owner["runtimeProfile"] = "journal-bounded"; events.clear()
            with patch.object(fleet_node, "stopped", side_effect=ValueError("candidate still running")):
                with self.assertRaises(ValueError): fleet_node.volume_operation(request)
            self.assertFalse(events)

    def test_allocation_registers_exact_undo_before_formatting_only_bounded_files(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            state.mkdir()
            roots = tuple(Path(directory) / f"root{index}" for index in range(4))
            for root in roots:
                (root / "data").mkdir(parents=True)
            owner = {"runtimeProfile": "bounded-capacity", "runId": "gh-1-1-h1"}
            calls = []
            def checked_command(*args):
                calls.append(args)
                self.assertTrue((state / volume.MARKER).exists())
                intent = json.loads((state / volume.MARKER).read_text())
                self.assertEqual(len(intent["volumes"]), 4)
                if args[0] == "mkfs.ext4":
                    self.assertIn(Path(args[-1]).parent, roots)
                    self.assertEqual(Path(args[-1]).name, "bounded-volume.img")
                    self.assertNotIn("/dev/", args[-1])
                return "/dev/loop7\n" if args[0] == "losetup" else ""
            with patch.object(volume, "command", side_effect=checked_command), \
                 patch.object(volume, "regular", return_value=SimpleNamespace(st_size=100)), \
                 patch.object(volume, "loop_for", return_value="/dev/loop7"), \
                 patch.object(volume, "mounted", return_value={"verified": True}), \
                 patch.object(volume.os, "posix_fallocate", create=True) as allocate, \
                 patch.object(volume.os, "statvfs", return_value=SimpleNamespace(f_blocks=32768, f_frsize=4096, f_files=1024)), \
                 patch.object(volume.shutil, "disk_usage", return_value=SimpleNamespace(free=10 * 1024**3)):
                result = volume.create(state, roots, owner, capacity.BRANCHES[0])
            self.assertEqual(allocate.call_count, 4)
            self.assertTrue(all(call.args[1:] == (0, volume.IMAGE_BYTES) for call in allocate.call_args_list))
            self.assertEqual(sum(args[0] == "mkfs.ext4" for args in calls), 4)
            self.assertFalse(result["hostSharedMountRemounted"])
            self.assertFalse(result["independentPhysicalDriveCountIncreased"])

    def test_registered_quota_probe_signs_and_preserves_the_exact_request(self):
        class Response(BytesIO):
            code = 200
            def __init__(self):
                super().__init__(b"{}")
                self.headers = {"Content-Length": "2"}
        probe = Probe(binding())
        path = "/rustfs/admin/v3/quota/" + probe.binding["bucketPrefix"]
        with patch.object(probe, "credentials", return_value=("access", "secret")), \
             patch.object(probe, "_open_admin", side_effect=lambda outgoing: Response()) as outgoing:
            for limit in (1024**2, 256 * 1024**2):
                body = json.dumps({"quota": limit, "quota_type": "HARD"}).encode()
                observed = probe.admin(path, [], body, method="PUT")
                self.assertEqual(observed["requestBody"].encode(), body)
                self.assertEqual(outgoing.call_args.args[0].data, body)
                self.assertIn("Authorization", outgoing.call_args.args[0].headers)
            self.assertEqual(probe.admin(path, [], b"", method="GET")["httpStatus"], 200)

    def test_quota_config_probe_signs_the_exact_owned_bucket_query(self):
        probe = Probe(binding())
        path = "/rustfs/admin/v3/get-bucket-quota"
        class Response(BytesIO):
            code = 200
            def __init__(self):
                super().__init__(b'{"quota":0,"size":0,"rate":0,"requests":0}')
                self.headers = {"Content-Length": str(self.getbuffer().nbytes)}
        with patch.object(probe, "credentials", return_value=("access", "secret")), \
             patch.object(probe, "_open_admin", return_value=Response()) as outgoing:
            observed = probe.admin(path, [("bucket", probe.binding['bucketPrefix'])], b"", method="GET")
        self.assertEqual(observed["httpStatus"], 200)
        self.assertEqual(outgoing.call_args.args[0].get_method(), "GET")
        self.assertEqual(outgoing.call_args.args[0].full_url,
                         probe.endpoint + path + '?bucket=' + probe.binding['bucketPrefix'])
        self.assertIn('Authorization', outgoing.call_args.args[0].headers)
        for query, method, body, auth in (([], 'GET', b'', 'signed'),
                ([('bucket', 'foreign-bucket')], 'GET', b'', 'signed'),
                ([('bucket', probe.binding['bucketPrefix'])] * 2, 'GET', b'', 'signed'),
                ([('bucket', probe.binding['bucketPrefix'])], 'PUT', b'', 'signed'),
                ([('bucket', probe.binding['bucketPrefix'])], 'GET', b'{}', 'signed'),
                ([('bucket', probe.binding['bucketPrefix'])], 'GET', b'', 'anonymous')):
            with self.subTest(query=query, method=method, body=body, auth=auth), \
                 patch.object(probe, '_open_admin') as outgoing, self.assertRaises(ValueError):
                probe.admin(path, query, body, method=method, auth=auth)
            outgoing.assert_not_called()

    def test_quota_probe_refuses_foreign_buckets_unregistered_limits_and_auth(self):
        probe = Probe(binding())
        path = "/rustfs/admin/v3/quota/" + probe.binding["bucketPrefix"]
        for route, method, body, auth in (
            ("/rustfs/admin/v3/quota/foreign-bucket", "PUT", b'{"quota":1048576,"quota_type":"HARD"}', "signed"),
            (path, "DELETE", b"", "signed"),
            (path, "PUT", b'{"quota":123,"quota_type":"HARD"}', "signed"),
            (path, "PUT", b'{"quota":true,"quota_type":"HARD"}', "signed"),
            (path, "GET", b"mutation", "signed"),
            (path, "GET", b"", "anonymous")):
            with self.subTest(route=route, method=method, body=body, auth=auth), \
                 patch.object(probe, "_open_admin") as outgoing, self.assertRaises(ValueError):
                probe.admin(route, [], body, method=method, auth=auth)
            outgoing.assert_not_called()


if __name__ == "__main__":
    unittest.main()
