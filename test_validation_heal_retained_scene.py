"""Incomplete scenes remain preserved and cannot become restored PASS evidence."""

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import fleet_node, retained_scene as retained, recover_environment as recovery, mpu_scene
from scripts.validation_heal.healthy import Fleet


class RetainedSceneTests(unittest.TestCase):
    def scene(self, directory):
        base = Path(directory).resolve()
        mounts = [base / str(index) for index in range(4)]
        for mount in mounts: mount.mkdir()
        run_id = "gh-12345-1-h2"
        state = base / "state" / run_id; state.mkdir(parents=True, mode=0o700)
        roots = tuple(mount / "heal-validation" / run_id for mount in mounts)
        binary = b"original-candidate"
        owner = {"runId": run_id, "unit": "rustfs-validation-heal-" + run_id + ".service",
                 "hostname": socket.gethostname(), "runtimeProfile": "mrf-crash", "bucket": "rv-heal-0123456789abcdef",
                 "binarySha256": hashlib.sha256(binary).hexdigest(), "mounts": [
                     {key: str(index) for key in ("target", "source", "fstype", "uuid", "maj:min")} for index in range(4)]}
        (state / "rustfs").write_bytes(binary)
        (state / "owner.json").write_text(json.dumps(owner))
        (state / "service.env").write_bytes(b"credential-must-never-be-read")
        for root in roots:
            (root / "data" / owner["bucket"]).mkdir(parents=True)
            (root / "data" / owner["bucket"] / "part.1").write_bytes(b"acknowledged-fixture")
            (root / "data" / ".rustfs.sys" / "config" / "iam").mkdir(parents=True)
            (root / "data" / ".rustfs.sys" / "config" / "iam" / "credential").write_bytes(b"private-iam")
            (root / "quarantine").mkdir()
            (root / "quarantine" / "original").write_bytes(b"original-backup")
            (root / "owner.json").write_text(json.dumps(owner))
        shutil.rmtree(roots[0])
        node = {"paths": lambda run: (state, roots), "unit": lambda run: owner["unit"], "socket": socket,
                "mounts": lambda: copy.deepcopy(owner["mounts"]), "stopped": lambda value: {}, "MOUNTS": mounts,
                "digest": fleet_node.digest, "checked": fleet_node.checked}
        request = {"runId": run_id, "action": "retained-scene-audit", "expectedOwner": owner}
        return state, roots, node, request

    @staticmethod
    def local_rename(source, target):
        if target.exists(): raise ValueError("destination exists")
        source.rename(target)
        retained.sync_directory(source.parent)
        retained.sync_directory(target.parent)

    def test_real_files_keep_inodes_bytes_and_retry_after_each_interrupted_move(self):
        for interrupt_after in (1, 2, 3, 4):
            with self.subTest(interrupt_after=interrupt_after), tempfile.TemporaryDirectory() as directory:
                state, roots, node, request = self.scene(directory)
                # Run the full production ownership check using root-owned
                # stat proxies, without replacing filesystem identities.
                with self.root_stats(), patch.object(retained, "rename_new", side_effect=self.local_rename):
                    before = retained.run(request, node)
                    self.assertFalse((state.parent / "retained").exists())
                    original = {p: (p.stat().st_ino, p.read_bytes()) for root in (*roots[1:], state)
                                for p in root.rglob("*") if p.is_file()}
                    count = 0
                    def interrupted(source, target):
                        nonlocal count
                        self.local_rename(source, target)
                        if target.is_dir():
                            count += 1
                            if count == interrupt_after: raise RuntimeError("lost SSH after atomic rename")
                    change = {**request, "action": "retained-scene-isolate", "expectedBaseline": before["baseline"]}
                    with patch.object(retained, "rename_new", side_effect=interrupted), self.assertRaises(RuntimeError):
                        retained.run(change, node)
                    result = retained.run(change, node)
                    self.assertTrue(result["activeNamespaceIsolated"])
                    self.assertEqual(result["baseline"], before["baseline"])
                    self.assertEqual(result["locations"], ["absent", "retained", "retained", "retained", "retained"])
                    retained_state, retained_roots = retained.destinations(state, roots)
                    for old, (inode, raw) in original.items():
                        source = next(root for root in (*roots[1:], state) if old.is_relative_to(root))
                        target = retained_state if source == state else retained_roots[roots.index(source)]
                        moved = target / old.relative_to(source)
                        self.assertEqual((moved.stat().st_ino, moved.read_bytes()), (inode, raw))
                    self.assertFalse(result["originalSceneRestored"])
                    self.assertFalse(result["canonicalPassClaimed"])
                    self.assertEqual(retained.run(request, node)["baseline"], before["baseline"])
                    self.assertFalse(retained.run(change, node)["mutated"])

    def root_stats(self):
        # Root UID checks are meaningful in production. Only rewrite st_uid in
        # returned host stat objects; preserve inode, mode and nanosecond times.
        original_lstat, original_stat, original_fstat = Path.lstat, Path.stat, os.fstat
        def mapped(value):
            values = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
            values["st_uid"] = 0
            return types.SimpleNamespace(**values)
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch.object(retained, "mounted_paths", return_value=[]))
        stack.enter_context(patch.object(Path, "lstat", lambda path: mapped(original_lstat(path))))
        stack.enter_context(patch.object(Path, "stat", lambda path, **kw: mapped(original_stat(path, **kw))))
        stack.enter_context(patch.object(os, "fstat", lambda fd: mapped(original_fstat(fd))))
        return stack

    def test_no_credentials_read_and_no_paths_or_marker_content_emitted(self):
        with tempfile.TemporaryDirectory() as directory:
            state, roots, node, request = self.scene(directory)
            opened = []
            original_open = os.open
            def record(path, *args, **kwargs):
                opened.append(str(path)); return original_open(path, *args, **kwargs)
            with self.root_stats(), patch.object(os, "open", side_effect=record):
                result = retained.run(request, node)
            self.assertFalse(any(name.endswith("service.env") or name.endswith("credential") for name in opened))
            serialized = json.dumps(result)
            for private in ("credential-must-never-be-read", "private-iam", request["expectedOwner"]["bucket"], str(state)):
                self.assertNotIn(private, serialized)

    def test_drift_or_unsafe_paths_refuse_before_any_move(self):
        for variant in ("owner", "binary", "gate", "symlink", "hardlink", "mount", "live", "content-drift", "collision", "nested-mount"):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                state, roots, node, request = self.scene(directory)
                with self.root_stats():
                    baseline = retained.run(request, node)["baseline"]
                    if variant == "owner": (roots[1] / "owner.json").write_text("{}")
                    elif variant == "binary": (state / "rustfs").write_bytes(b"different")
                    elif variant == "gate": (state / "drive-gate.json").write_text("{}")
                    elif variant == "symlink": (roots[1] / "escape").symlink_to(state)
                    elif variant == "hardlink": os.link(roots[1] / "quarantine/original", roots[1] / "alias")
                    elif variant == "mount": node["mounts"] = lambda: [{**row, "uuid": "foreign"} for row in request["expectedOwner"]["mounts"]]
                    elif variant == "live": node["stopped"] = lambda owner: (_ for _ in ()).throw(ValueError("live"))
                    elif variant == "content-drift": (roots[1] / "quarantine/original").write_bytes(b"advanced-record")
                    elif variant == "nested-mount": retained.mounted_paths.return_value = [roots[1] / "data"]
                    elif variant == "collision":
                        target = retained.destinations(state, roots)[1][1]; target.mkdir(parents=True, mode=0o700)
                    with patch.object(retained, "rename_new") as move, self.assertRaises(ValueError):
                        retained.run({**request, "action": "retained-scene-isolate", "expectedBaseline": baseline}, node)
                    move.assert_not_called()
                    self.assertTrue(state.exists()); self.assertTrue(roots[1].exists())

    def test_disappeared_retained_directory_cannot_be_reclassified_as_originally_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            state, roots, node, request = self.scene(directory)
            with self.root_stats(), patch.object(retained, "rename_new", side_effect=self.local_rename):
                baseline = retained.run(request, node)["baseline"]
                retained.run({**request, "action": "retained-scene-isolate", "expectedBaseline": baseline}, node)
                shutil.rmtree(retained.destinations(state, roots)[1][1])
                with self.assertRaisesRegex(ValueError, "durable isolation intent"):
                    retained.run(request, node)

    def test_remote_adapter_loads_real_module(self):
        def execute(*args, **kwargs):
            source = kwargs["input"].rsplit("\nmain(", 1)[0]
            scope = {"__name__": "__node_test__"}
            exec(compile(source, "retained-node", "exec"), scope)
            self.assertTrue(callable(scope["retained_scene_backend"].run))
            self.assertTrue(callable(scope["mpu_scene_backend"].run))
            return types.SimpleNamespace(returncode=0, stdout="{}", stderr="")
        with tempfile.TemporaryDirectory() as directory, patch("scripts.validation_heal.healthy.subprocess.run", side_effect=execute):
            Fleet(["vm000"], Path(directory), "gh-12345-1-h2").remote("vm000", "retained-scene-audit", expectedOwner={})

    def test_mountinfo_decodes_escaped_paths_without_accepting_a_missing_inventory(self):
        with patch.object(Path, "read_text", return_value=r"1 0 8:1 / /data/rustfs1/a\040b rw - xfs /dev/disk rw"):
            self.assertEqual(retained.mounted_paths(), [Path("/data/rustfs1/a b")])
        with patch.object(Path, "read_text", return_value="invalid"), self.assertRaises(ValueError):
            retained.mounted_paths()

    def test_linux_rename_explicitly_uses_noreplace_and_syncs_both_parents(self):
        call = unittest.mock.Mock(return_value=0)
        with patch.object(retained.ctypes, "CDLL", return_value=types.SimpleNamespace(renameat2=call)), \
             patch.object(retained, "sync_directory") as sync:
            retained.rename_new(Path("/original/run"), Path("/retained/run"))
        call.assert_called_once_with(-100, b"/original/run", -100, b"/retained/run", 1)
        self.assertEqual(sync.call_args_list, [unittest.mock.call(Path("/original")), unittest.mock.call(Path("/retained"))])

    def mpu_partial_scene(self, directory):
        state, roots, node, request = self.scene(directory)
        shutil.copytree(roots[1], roots[0])
        owner = request['expectedOwner'];owner['runtimeProfile'] = 'manual'
        for folder in (*roots, state): (folder/'owner.json').write_text(json.dumps(owner))
        snapshot = {'files':[], 'formats':[], 'pid':0, 'quiescent':True, 'binarySha256':owner['binarySha256']}
        node.update(owned=lambda run:(state, roots, copy.deepcopy(owner)),
                    snapshot=lambda value:copy.deepcopy(snapshot), mpu_scene_backend=mpu_scene)
        key_hash = hashlib.sha256(f"{owner['bucket']}/{mpu_scene.G05_KEY}".encode()).hexdigest()
        for root in roots:
            folder = root/'data/.rustfs.sys/multipart'/key_hash/'unknown-create-before-ACK'
            folder.mkdir(parents=True)
            (folder/'part.1').write_bytes(b'unknown MPU original bytes')
            (root/'quarantine/G05-partial.original').write_bytes(b'original missing-shard backup')
        raw = mpu_scene.run({'runId':request['runId'], 'action':'mpu-scene', 'bucket':owner['bucket'],
            'keys':[mpu_scene.G05_KEY], 'expectedBinarySha256':owner['binarySha256']}, node)
        request.update(expectedOriginalSceneSha256=retained.sha({k:snapshot[k] for k in ('files','formats')}),
                       mpuKeys=[mpu_scene.G05_KEY], expectedMpuSceneSha256=raw['sceneSha256'])
        return state, roots, node, request

    def test_g05_unknown_partial_seed_isolates_original_mpu_inodes_without_reading_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            state, roots, node, request = self.mpu_partial_scene(directory)
            original = {p:(p.stat().st_ino,p.read_bytes()) for root in roots
                        for p in root.rglob('*') if p.is_file()}
            opened, real_open = [], os.open
            def record(path, *args, **kwargs):
                opened.append(str(path));return real_open(path, *args, **kwargs)
            with self.root_stats(), patch.object(os,'open',side_effect=record), \
                 patch.object(retained,'rename_new',side_effect=self.local_rename):
                before = retained.run(request,node)
                self.assertTrue(all(row['contentFilesHashed']==3 for row in before['baseline']['roots']))
                change = {**request,'action':'retained-scene-isolate','expectedBaseline':before['baseline']}
                def interrupted(source, target):
                    self.local_rename(source, target)
                    if source == roots[0]: raise RuntimeError('lost response after first MPU root move')
                with patch.object(retained,'rename_new',side_effect=interrupted),self.assertRaises(RuntimeError):
                    retained.run(change,node)
                result = retained.run(change,node)
                self.assertEqual(result['baseline'],before['baseline'])
                self.assertEqual(result['locations'],['retained']*5)
                self.assertTrue(result['activeNamespaceIsolated'])
                self.assertFalse(result['originalSceneRestored'] or result['canonicalPassClaimed'])
                _, targets = retained.destinations(state,roots,per_run_parent=True)
                for old,(inode,raw) in original.items():
                    index = next(i for i,root in enumerate(roots) if old.is_relative_to(root))
                    new = targets[index]/old.relative_to(roots[index])
                    self.assertEqual((new.stat().st_ino,new.read_bytes()),(inode,raw))
            self.assertFalse(any(p.endswith(('service.env','credential')) for p in opened))

    def test_g05_unknown_mpu_scope_or_digest_drift_refuses_before_any_move(self):
        for fault in ('scope','digest','missing-digest','changed-content','byte-cap'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
                state,roots,node,request = self.mpu_partial_scene(directory)
                with self.root_stats():
                    before = retained.run(request,node)
                    if fault == 'scope': request['mpuKeys'] = ['objects-0255.bin']
                    elif fault == 'digest': request['expectedMpuSceneSha256'] = 'f'*64
                    elif fault == 'missing-digest': request.pop('expectedMpuSceneSha256')
                    elif fault == 'changed-content': next((roots[0]/'data/.rustfs.sys/multipart').rglob('part.1')).write_bytes(b'changed MPU')
                    with patch.object(retained,'rename_new') as move, \
                         patch.object(mpu_scene,'MAX_BYTES',1 if fault == 'byte-cap' else mpu_scene.MAX_BYTES), \
                         self.assertRaises(ValueError):
                        retained.run({**request,'action':'retained-scene-isolate','expectedBaseline':before['baseline']},node)
                    move.assert_not_called()


class IncompleteFleetIsolationTests(unittest.TestCase):
    def test_main_does_not_treat_an_archived_namespace_as_restored_or_already_clean(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); candidate = root / "candidate.json"; candidate.write_text("{}")
            plan = {"kind": "advanced-mrf-fixture", "nodes": ["vm000"], "runId": "gh-12345-1-h2"}
            states = {"state": {"exists": False}, "roots": [{"exists": False}] * 4,
                      "retained": {"state": True, "roots": [False, True, True, True]}}
            result = {"cleanup": "ISOLATED", "originalSceneRestored": False}
            argv = ["recover", "--failed-run-id", "12345", "--artifact", str(root), "--candidate", str(candidate),
                    "--evidence", str(root / "evidence")]
            with patch("sys.argv", argv), patch.object(recovery, "recovery_plan", return_value=[plan]), \
                 patch.object(Fleet, "remote", return_value=states) as remote, \
                 patch.object(recovery, "isolate_incomplete_scene", return_value=result) as isolate:
                recovery.main()
            self.assertEqual([call.args[1] for call in remote.call_args_list], ["namespace-state"])
            isolate.assert_called_once()
            summary = json.loads((root / "evidence/recovery-summary.json").read_text())
            self.assertEqual(summary["cleanup"], "ISOLATED")
            self.assertFalse(summary["canonicalPassClaimed"])

    def test_all_preflights_precede_moves_and_original_failure_is_not_restored(self):
        for failure in (None, "audit-vm003", "move-vm001"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                case = "heal.checkpoint-and-mrf-corruption"
                folder = root / "artifact/run" / case / "evidence" / case / "DB/H2/mrf-checksum"
                folder.mkdir(parents=True)
                nodes = [f"vm00{i}" for i in range(4)]
                plan = {"nodes": nodes, "runId": "gh-12345-1-h2", "bucket": "rv-heal-0123456789abcdef", "binarySha256": "b" * 64}
                for index, node in enumerate(nodes):
                    (folder / f"{index:05d}-reserved.json").write_text(json.dumps({"node": node, "reserved": True,
                        "owner": {**{key: plan[key] for key in ("runId", "bucket", "binarySha256")}, "runtimeProfile": "mrf-crash"}}))
                events = []
                def remote(node, action, **kwargs):
                    events.append((node, action))
                    if (failure == "audit-" + node and action.endswith("audit")) or (failure == "move-" + node and action.endswith("isolate")):
                        raise ValueError("injected failure")
                    return {"baseline": {"roots": [None, {}, {}, {}]}, "inspectionOnly": action.endswith("audit"),
                            "mutated": action.endswith("isolate"), "credentialContentsRead": False,
                            "activeNamespaceIsolated": True, "originalSceneRestored": False, "originalEvidenceComplete": False,
                            "backupRestorationUsed": False, "canonicalPassClaimed": False}
                fleet = types.SimpleNamespace(remote=remote, record=lambda label, value: value)
                if failure:
                    with self.assertRaises(ValueError): recovery.isolate_incomplete_scene(fleet, plan, root)
                else:
                    result = recovery.isolate_incomplete_scene(fleet, plan, root)
                    self.assertEqual(result["cleanup"], "ISOLATED")
                    self.assertFalse(result["originalSceneRestored"])
                    self.assertFalse(result["originalVerdictRewritten"])
                self.assertEqual(events[:4], [(node, "retained-scene-audit") for node in nodes])
                if failure == "audit-vm003": self.assertFalse(any(action.endswith("isolate") for _, action in events))


if __name__ == "__main__":
    unittest.main()
