"""Exact durable-record corruption preserves ownership and refuses stale rescue."""

import base64
import copy
import hashlib
import contextlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import time
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch
import zlib

from scripts.validation_heal import journal_fault as journal, heal_fault as fault, mrf_records as mrf
from scripts.validation_heal.healthy import Fleet
from scripts.validation_heal import fleet_node
from scripts.validation_heal import healthy
from scripts.validation_heal.checkpoint import OPTIONS


class DurableRecordFaultTests(unittest.TestCase):
    token = "12345678-1234-4234-8234-123456789abc"
    incarnation = "22345678-1234-4234-8234-123456789abc"
    bucket = "rv-heal-0123456789abcdef"

    def test_atomic_replace_after_open_rejects_detached_bytes_as_read_churn(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current, successor = root / 'checkpoint', root / 'successor'
            current.write_bytes(b'earlier')
            successor.write_bytes(b'later')
            original_open = os.open
            observed = []
            def replace_after_open(name, flags, **kwargs):
                descriptor = original_open(name, flags, **kwargs)
                if name == current.name:
                    os.replace(successor, current)
                    observed.append(os.fstat(descriptor).st_nlink)
                return descriptor
            descriptor = original_open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with patch.object(fault.os, 'open', side_effect=replace_after_open), \
                     patch.object(fault, 'mount_id', return_value=1):
                    with self.assertRaisesRegex(ValueError, '^file changed while reading$'):
                        fault.read_file(descriptor, current.name)
                self.assertEqual(observed, [0])
                with patch.object(fault, 'mount_id', return_value=1):
                    self.assertEqual(fault.read_file(descriptor, current.name), b'later')
                self.assertEqual(current.stat().st_nlink, 1)
            finally:
                os.close(descriptor)

    def test_detached_inode_does_not_hide_type_size_or_mount_refusals(self):
        for poison in ('oversized', 'fifo', 'mount'):
            with self.subTest(poison=poison), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                current, successor = root / 'checkpoint', root / 'successor'
                if poison == 'fifo': os.mkfifo(current)
                else: current.write_bytes(b'12345' if poison == 'oversized' else b'old')
                successor.write_bytes(b'new')
                original_open = os.open
                def replace_after_open(name, flags, **kwargs):
                    descriptor = original_open(name, flags, **kwargs)
                    if name == current.name:
                        os.replace(successor, current)
                        self.assertEqual(os.fstat(descriptor).st_nlink, 0)
                    return descriptor
                descriptor = original_open(root, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    with patch.object(fault.os, 'open', side_effect=replace_after_open), \
                         patch.object(fault, 'mount_id', side_effect=[2, 1] if poison == 'mount' else None, return_value=1):
                        message = 'file bind-mount escape' if poison == 'mount' else 'expected bounded regular non-hardlinked file'
                        with self.assertRaisesRegex(ValueError, '^' + message + '$'):
                            fault.read_file(descriptor, current.name, limit=4)
                finally:
                    os.close(descriptor)

    def test_live_read_race_is_retryable_but_stopped_read_and_path_failures_are_not(self):
        for message in ('file changed while reading', 'file bind-mount escape', 'expected bounded regular non-hardlinked file'):
            with self.subTest(message=message), patch.object(journal, 'census', side_effect=ValueError(message)):
                expected = 'NOT_OBSERVED:' if message == 'file changed while reading' else message
                with self.assertRaisesRegex(ValueError, expected): journal.stable_census([], {}, fault)
                with self.assertRaisesRegex(ValueError, message): journal.census([], {}, fault)

    def test_only_live_observation_refusals_survive_the_remote_boundary_as_retryable(self):
        refusal = 'NOT_OBSERVED: exact durable records changed between the two bounded reads'
        for action in ('journal-observe', 'journal-capture', 'journal-capture-advance', 'journal-apply', 'journal-census'):
            output = io.StringIO()
            with self.subTest(action=action), patch.object(fleet_node.os, 'geteuid', return_value=0), \
                 patch.object(fleet_node, 'journal_operation', side_effect=ValueError(refusal)), contextlib.redirect_stdout(output):
                if action in ('journal-observe', 'journal-capture'):
                    fleet_node.main({'action': action})
                    self.assertEqual(json.loads(output.getvalue()), {'observationUnavailable': refusal})
                else:
                    with self.assertRaisesRegex(ValueError, 'NOT_OBSERVED:'): fleet_node.main({'action': action})
        with patch.object(fleet_node.os, 'geteuid', return_value=0), \
             patch.object(fleet_node, 'journal_operation', side_effect=ValueError('service restarted while observing exact durable records')):
            with self.assertRaisesRegex(ValueError, 'service restarted'): fleet_node.main({'action': 'journal-observe'})
        with tempfile.TemporaryDirectory() as temp:
            fleet = Fleet(['node'], Path(temp), 'gh-12345-1-h1')
            for action in ('journal-observe', 'journal-capture'):
                response = types.SimpleNamespace(returncode=0, stdout=json.dumps({'observationUnavailable': refusal}), stderr='')
                with self.subTest(action=action), patch.object(healthy.subprocess, 'run', return_value=response):
                    with self.assertRaisesRegex(ValueError, '^NOT_OBSERVED:'): fleet.remote('node', action)
            for body in ({'observationUnavailable': 'other'}, {'observationUnavailable': refusal, 'records': []}, {'observationUnavailable': None}):
                response = types.SimpleNamespace(returncode=0, stdout=json.dumps(body), stderr='')
                with self.subTest(body=body), patch.object(healthy.subprocess, 'run', return_value=response):
                    with self.assertRaisesRegex(ValueError, 'invalid durable-record'): fleet.remote('node', 'journal-observe')
            response = types.SimpleNamespace(returncode=1, stdout=json.dumps({'observationUnavailable': refusal}), stderr='failed')
            with patch.object(healthy.subprocess, 'run', return_value=response):
                with self.assertRaisesRegex(ValueError, '^Heal node'): fleet.remote('node', 'journal-observe')

    def test_atomic_advance_uses_one_held_pidfd_and_refreezes_on_observation_failure(self):
        for change in (None, 'timeout', 'birth', 'corrupt', 'incarnation', 'options', 'counter', 'read-error', 'unstable', 'unlinked-open', 'unlinked-open-timeout', 'post-birth', 'late-progress', 'continue-interrupted', 'stop-pending', 'stop-timeout', 'stop-birth'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp, contextlib.ExitStack() as stack:
                base = Path(temp).resolve(); roots = tuple(base / str(i) for i in range(4))
                for root in roots: root.mkdir()
                owner, request, path, raw = self.fixture(roots[0]); request.pop('kind')
                for root in roots[1:]:
                    (root / 'data/.rustfs.sys/buckets').mkdir(parents=True); (root / 'quarantine').mkdir()
                request['action'] = 'journal-capture-advance'
                identity = {name: name for name in fleet_node.PROCESS_FIELDS}
                identity.update(pid=50, startTicks=20, binarySha256=owner['binarySha256'])
                request['expected'] = identity
                next_raw = self.checkpoint(processed=3, time=101)
                value = json.loads(next_raw)
                if change == 'incarnation': value['admin']['buckets'][self.bucket] = str(uuid.uuid4())
                elif change == 'options': value['admin']['options']['nolock'] = True
                elif change == 'counter': value['admin']['outcome']['counters']['processed'] = True
                if change in ('incarnation', 'options', 'counter'): next_raw = self.signed(value)
                if change == 'corrupt': next_raw = b'invalid checkpoint'
                events = []; running = [False]; raced = [False]
                def signal_pidfd(fd, signum, *args):
                    self.assertEqual(fd, 55); events.append(signum)
                    running[0] = signum == fleet_node.signal.SIGCONT
                    if running[0] and change != 'timeout': path.write_bytes(next_raw)
                    if running[0] and change == 'continue-interrupted': raise KeyboardInterrupt('lost CONT receipt')
                def process(request):
                    if len(process_calls) >= 2: self.assertEqual(events[-1], fleet_node.signal.SIGSTOP)
                    process_calls.append(True)
                    return {**identity, 'state': 'S' if change == 'stop-pending' and len(process_calls) == 3
                            and not stop_ready[0] else 'T',
                            **({'startTicks': 21} if change == 'post-birth' and len(process_calls) == 3 else {})}
                process_calls = []; stop_ready = [False]; stopped_reads = [0]
                old_read_text = Path.read_text
                def read_text(p, *a, **kw):
                    if str(p) == '/proc/50/stat':
                        state = 'S' if running[0] else 'T'
                        if not running[0] and events:
                            stopped_reads[0] += 1
                            if change == 'stop-timeout' or change == 'stop-pending' and stopped_reads[0] == 1: state = 'S'
                            stop_ready[0] = state == 'T'
                        changed_birth = change == 'birth' or change == 'stop-birth' and not running[0] and events
                        return '50 (owned candidate) ' + state + ' ' + ' '.join(['0'] * 18 + ['21' if changed_birth else '20'])
                    return old_read_text(p, *a, **kw)
                real_read = journal.read
                def read(*args, **kwargs):
                    if running[0] and not raced[0] and change in ('read-error', 'unstable'):
                        raced[0] = True
                        if change == 'read-error': raise PermissionError('unexpected access denial')
                        raise ValueError('file changed while reading')
                    if running[0] and not raced[0] and change in ('unlinked-open', 'unlinked-open-timeout'):
                        raced[0] = True
                        original_open = os.open
                        def replace_after_open(name, flags, **params):
                            descriptor = original_open(name, flags, **params)
                            if name == path.name:
                                successor = path.with_name('natural-checkpoint-successor')
                                successor.write_bytes(next_raw)
                                os.replace(successor, path)
                                self.assertEqual(os.fstat(descriptor).st_nlink, 0)
                            return descriptor
                        with patch.object(fault.os, 'open', side_effect=replace_after_open):
                            return real_read(*args, **kwargs)
                    return real_read(*args, **kwargs)
                for context in (patch.object(fleet_node, 'owned', return_value=(base, roots, owner)),
                    patch.object(fleet_node, 'digest', return_value=owner['binarySha256']),
                    patch.object(fleet_node, 'fault_module', return_value=fault),
                    patch.dict(fleet_node.__dict__, journal_fault_backend=journal),
                    patch.object(fleet_node, 'process', side_effect=process)):
                    stack.enter_context(context)
                opened = stack.enter_context(patch.object(fleet_node.os, 'pidfd_open', create=True, return_value=55))
                sent = stack.enter_context(patch.object(fleet_node.signal, 'pidfd_send_signal', create=True, side_effect=signal_pidfd))
                real_close = os.close
                closed = []
                def close(fd):
                    if fd == 55: closed.append(fd)
                    else: real_close(fd)
                stack.enter_context(patch.object(fleet_node.os, 'close', side_effect=close))
                stack.enter_context(patch.object(fault, 'mount_id', return_value=1))
                stack.enter_context(patch.object(Path, 'read_text', read_text))
                stack.enter_context(patch.object(journal, 'read', side_effect=read))
                if change == 'timeout': stack.enter_context(patch.object(fleet_node.time, 'monotonic', side_effect=[0, 61]))
                elif change == 'unlinked-open-timeout': stack.enter_context(patch.object(fleet_node.time, 'monotonic', side_effect=[0, .01, 61]))
                elif change == 'late-progress': stack.enter_context(patch.object(fleet_node.time, 'monotonic', side_effect=[0, .01, 61]))
                elif change == 'stop-timeout': stack.enter_context(patch.object(fleet_node.time, 'monotonic', side_effect=[0, .01, .02, 61]))
                if change in (None, 'unstable', 'unlinked-open', 'stop-pending'):
                    result = fleet_node.journal_operation(request)
                    self.assertTrue(result['checkpointAdvancedAndFrozen'])
                    self.assertTrue(result['pidfdBound'])
                    self.assertEqual(result['observedProcessed'], 3)
                    self.assertEqual(path.read_bytes(), next_raw)
                    self.assertEqual(len(process_calls), 3)
                elif change == 'unlinked-open-timeout':
                    with self.assertRaisesRegex(ValueError, 'actual checkpoint advance exceeded its frozen 60-second budget'):
                        fleet_node.journal_operation(request)
                else:
                    with self.assertRaises((ValueError, OSError, KeyboardInterrupt)): fleet_node.journal_operation(request)
                self.assertEqual(events, [fleet_node.signal.SIGCONT, fleet_node.signal.SIGSTOP])
                self.assertEqual(closed, [55])
                opened.assert_called_once_with(50, 0)
                self.assertEqual((roots[0] / ('quarantine/' + request['faultId'] + '.early.checkpoint')).read_bytes(), raw)

    def test_atomic_advance_rejects_unpaused_foreign_or_unbounded_requests_before_capture_or_signal(self):
        for change in ('runtime', 'live', 'birth', 'input', 'budget', 'path', 'disk', 'family'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp, contextlib.ExitStack() as stack:
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root); request.pop('kind')
                request['action'] = 'journal-capture-advance'
                identity = {name: name for name in fleet_node.PROCESS_FIELDS}
                identity.update(pid=50, startTicks=20, binarySha256=owner['binarySha256']); request['expected'] = identity.copy()
                if change == 'runtime': owner['runtimeProfile'] = 'mrf-crash'
                elif change == 'birth': request['expected']['startTicks'] = 21
                elif change == 'input': request['command'] = 'arbitrary'
                elif change == 'budget': request['timeout'] = 600
                elif change == 'path': request['relativePath'] = '.rustfs.sys/foreign.json'
                elif change == 'disk': request['disk'] = True
                elif change == 'family': request['family'] = 'mrf'
                for context in (patch.object(fleet_node, 'owned', return_value=(root, [root] * 4, owner)),
                    patch.object(fleet_node, 'digest', return_value=owner['binarySha256']),
                    patch.object(fleet_node, 'fault_module', return_value=fault),
                    patch.dict(fleet_node.__dict__, journal_fault_backend=journal),
                    patch.object(fleet_node, 'process', return_value={**identity, 'state': 'S' if change == 'live' else 'T'}),
                    patch.object(fleet_node.os, 'pidfd_open', create=True, return_value=55),
                    patch.object(fleet_node.signal, 'pidfd_send_signal', create=True)):
                    stack.enter_context(context)
                captured = stack.enter_context(patch.object(journal, 'capture'))
                with self.assertRaises(ValueError): fleet_node.journal_operation(request)
                captured.assert_not_called()
                self.assertEqual(path.read_bytes(), raw)
                self.assertEqual(list((root / 'quarantine').iterdir()), [])

    def test_advance_rejects_reopened_capture_device_or_mount_before_resuming(self):
        for key in ('device', 'mountId'):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as temp, contextlib.ExitStack() as stack:
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root); request.pop('kind')
                request['action'] = 'journal-capture-advance'
                identity = {name: name for name in fleet_node.PROCESS_FIELDS}
                identity.update(pid=50, startTicks=20, binarySha256=owner['binarySha256']); request['expected'] = identity
                real_capture = journal.capture
                def capture(*args): return {**real_capture(*args), key: -1}
                for context in (patch.object(fleet_node, 'owned', return_value=(root, [root] * 4, owner)),
                    patch.object(fleet_node, 'digest', return_value=owner['binarySha256']),
                    patch.object(fleet_node, 'fault_module', return_value=fault),
                    patch.dict(fleet_node.__dict__, journal_fault_backend=journal),
                    patch.object(fleet_node, 'process', return_value={**identity, 'state': 'T'}),
                    patch.object(fleet_node.os, 'pidfd_open', create=True, return_value=55),
                    patch.object(fault, 'mount_id', return_value=1), patch.object(journal, 'capture', side_effect=capture)):
                    stack.enter_context(context)
                signals = stack.enter_context(patch.object(fleet_node.signal, 'pidfd_send_signal', create=True))
                real_close = os.close; closed = []
                def close(fd):
                    if fd == 55: closed.append(fd)
                    else: real_close(fd)
                stack.enter_context(patch.object(fleet_node.os, 'close', side_effect=close))
                with self.assertRaisesRegex(ValueError, 'root device or mount changed'): fleet_node.journal_operation(request)
                signals.assert_not_called(); self.assertEqual(closed, [55]); self.assertEqual(path.read_bytes(), raw)

    @unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'pidfd_open')
                         and hasattr(fleet_node.signal, 'pidfd_send_signal'), 'actual Linux pidfd required')
    def test_actual_linux_pidfd_stops_the_same_child_after_real_checkpoint_file_progress(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp).resolve(); roots = tuple(base / str(i) for i in range(4))
            for root in roots: root.mkdir()
            owner, request, path, early = self.fixture(roots[0]); request.pop('kind')
            request['action'] = 'journal-capture-advance'
            for root in roots[1:]: (root / 'data/.rustfs.sys/buckets').mkdir(parents=True)
            resume_path = roots[0] / 'data' / journal.names('checkpoint', self.token)[1]
            resume = json.dumps({'schema_version': 7, 'task_id': self.token, 'task_type': 'admin_erasure_set',
                'set_disk_id': 'pool_0_set_0', 'completed': False, 'replacement_targets': [], 'replacement_generation': None}).encode()
            resume_path.write_bytes(resume)
            later = self.checkpoint(processed=3, time=101); next_bytes = base / 'next'; next_bytes.write_bytes(later)
            child_source = ("import os,signal,sys,time; os.kill(os.getpid(),signal.SIGSTOP); "
                "raw=open(sys.argv[2],'rb').read(); f=open(sys.argv[1]+'.next','xb'); "
                "f.write(raw); f.flush(); os.fsync(f.fileno()); f.close(); os.replace(sys.argv[1]+'.next',sys.argv[1]); "
                "time.sleep(120)")
            child = subprocess.Popen([sys.executable, '-c', child_source, str(path), str(next_bytes)])
            cleanup_fd = None
            real_send = fleet_node.signal.pidfd_send_signal
            def stat_fields(): return Path(f'/proc/{child.pid}/stat').read_text().rpartition(') ')[2].split()
            try:
                cleanup_fd = os.pidfd_open(child.pid, 0)
                deadline = time.monotonic() + 5
                while stat_fields()[0] != 'T':
                    self.assertIsNone(child.poll()); self.assertLess(time.monotonic(), deadline); time.sleep(.01)
                birth = int(stat_fields()[19])
                identity = {name: name for name in fleet_node.PROCESS_FIELDS}
                identity.update(pid=child.pid, startTicks=birth, binarySha256=owner['binarySha256'])
                request['expected'] = identity
                snapshots, signals = [], []
                def process(request):
                    fields = stat_fields(); self.assertEqual(int(fields[19]), birth)
                    if len(snapshots) >= 2: self.assertEqual(signals[-1], fleet_node.signal.SIGSTOP)
                    snapshots.append(fields[0]); return {**identity, 'state': fields[0]}
                def send(fd, signum, *args):
                    signals.append(signum); return real_send(fd, signum, *args)
                with patch.object(fleet_node, 'owned', return_value=(base, roots, owner)), \
                        patch.object(fleet_node, 'digest', return_value=owner['binarySha256']), \
                        patch.object(fleet_node, 'fault_module', return_value=fault), \
                        patch.dict(fleet_node.__dict__, journal_fault_backend=journal), \
                        patch.object(fleet_node, 'process', side_effect=process), \
                        patch.object(fleet_node.signal, 'pidfd_send_signal', side_effect=send):
                    result = fleet_node.journal_operation(request)
                self.assertEqual(signals, [fleet_node.signal.SIGCONT, fleet_node.signal.SIGSTOP])
                self.assertEqual(snapshots, ['T', 'T', 'T'])
                self.assertEqual(result['observedProcessed'], 3)
                self.assertEqual(stat_fields()[0], 'T')
                self.assertEqual(path.read_bytes(), later)
                self.assertEqual(resume_path.read_bytes(), resume)
                self.assertEqual((roots[0] / ('quarantine/' + request['faultId'] + '.early.checkpoint')).read_bytes(), early)
                print('ATOMIC_PIDFD_COMPONENT ' + json.dumps({'simulatedCheckpoint': True, 'actualLinuxPidfd': True,
                    'sameProcessBirth': True, 'checkpointCounts': [2, 3], 'earlierBackupByteIdentical': True,
                    'laterFileByteIdentical': True, 'companionUnchanged': True, 'childStoppedBeforeReturn': True,
                    'rustfsCandidateExecuted': False, 'canonicalPassClaimed': False}))
            finally:
                try:
                    if cleanup_fd is not None: real_send(cleanup_fd, fleet_node.signal.SIGKILL, None, 0)
                    elif child.poll() is None: child.kill()
                except ProcessLookupError: pass
                finally:
                    if cleanup_fd is not None: os.close(cleanup_fd)
                child.wait(timeout=5)

    def test_full_server_checkpoint_fixture_preserves_feature_unified_json_order(self):
        # Original bytes from candidate 1aa9be87, diagnostic 38261190390/1.
        raw = (Path(__file__).parent / "testdata/validation/checkpoint-preserve-order.json").read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), "af9f1217284fdf9516d111ae8f8cc28b264bec200ba640c5f06c696b2303982b")
        value = json.loads(raw)
        bucket = next(iter(value["admin"]["buckets"]))
        observed = journal.checkpoint_value(raw, value["task_id"], bucket)
        self.assertEqual(observed["admin"]["outcome"]["counters"]["processed"], 8)
        for mutate in ("counter", "cursor", "incarnation", "digest"):
            changed = copy.deepcopy(value)
            if mutate == "counter": changed["admin"]["outcome"]["counters"]["processed"] = 9
            elif mutate == "cursor": changed["current_object_index"] += 1
            elif mutate == "incarnation": changed["admin"]["buckets"][bucket] = self.incarnation
            else: changed["integrity_digest"] = "A" * 44
            with self.subTest(mutate=mutate), self.assertRaisesRegex(ValueError, "integrity digest"):
                journal.checkpoint_value(json.dumps(changed,separators=(",", ":")).encode(), value["task_id"], bucket)

    def test_original_mixed_mrf_payload_retains_only_registered_internal_companions(self):
        raw = (Path(__file__).parent / "testdata/validation/mrf-mixed-payload.bin").read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), "3a6db05b46bb7e3a66a971bb4cc4759b3cd4700d2f31d55f0a19fd023635eb80")
        intents = mrf.decode_journal(raw)
        bucket = "rv-heal-084db3ab418d55ba"
        owned = journal.owned_mrf_intents(intents, bucket)
        self.assertEqual(len(owned), 3)
        self.assertEqual(len(intents), 6)
        request = {"family": "mrf", "kind": "checksum", "relativePath": ".rustfs.sys/.heal-mrf-snapshot.0.bin"}
        self.assertEqual(journal.transform(raw, request, {"bucket": bucket}, mrf), raw[:-1] + bytes([raw[-1] ^ 1]))
        system_bucket, system_key = b".rustfs.sys", b"buckets/.rustfs.sys/.metadata.bin"
        body = struct.pack("<BBBBQBIIII", 1, 2, 3, 0, 100, 0, 0, 0, len(system_bucket), len(system_key)) + system_bucket + system_key
        system = body + struct.pack("<I", zlib.crc32(body))
        mixed = raw + system
        changed = journal.transform(mixed, request, {"bucket": bucket}, mrf)
        self.assertEqual(changed[len(raw):], system)
        self.assertEqual([index for index, (left, right) in enumerate(zip(mixed, changed)) if left != right], [len(raw) - 1])
        from test_validation_heal_mrf import DurableMrfTests
        marker = DurableMrfTests().journal(layout=3, kind=4, key="中文/marker.bin")
        self.assertEqual(journal.transform(marker, request, {"bucket": self.bucket}, mrf),
                         marker[:-1] + bytes([marker[-1] ^ 1]))
        for change in ("foreign-business", "iam-key", "system-version", "no-business"):
            values = copy.deepcopy(intents)
            business = next(item for item in values if item["bucket"] == bucket)
            system = next(item for item in values if item["bucket"] == ".rustfs.sys")
            if change == "foreign-business": business["bucket"] = "foreign"
            elif change == "iam-key": system["key"] = "config/iam/users/credentials.json"
            elif change == "system-version": system["versionId"] = self.token
            else: values = [item for item in values if item["bucket"] != bucket]
            with self.subTest(change=change), self.assertRaises(ValueError): journal.owned_mrf_intents(values, bucket)

    def test_future_checkpoint_schema_has_valid_candidate_digest_and_original_scope(self):
        live = (Path(__file__).parent / "testdata/validation/checkpoint-preserve-order.json").read_bytes()
        for raw, sort_keys in ((live, False), (self.checkpoint(), True)):
            original = json.loads(raw)
            bucket = next(iter(original["admin"]["buckets"]))
            request = {"family": "checkpoint", "kind": "future-schema", "taskId": original["task_id"],
                       "relativePath": journal.names("checkpoint", original["task_id"])[0]}
            with self.subTest(sort_keys=sort_keys):
                changed = journal.transform(raw, request, {"bucket": bucket}, mrf)
                future = json.loads(changed)
                self.assertEqual(future["schema_version"], 9)
                unsigned = copy.deepcopy(future)
                unsigned["integrity_digest"] = None
                for field in ("processed_objects", "failed_objects", "skipped_objects"): unsigned[field].sort()
                digest = base64.b64encode(hashlib.sha256(json.dumps(unsigned, sort_keys=sort_keys,
                    ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).digest()).decode()
                self.assertEqual(future["integrity_digest"], digest)
                self.assertNotEqual(future["integrity_digest"], original["integrity_digest"])
                future.update(schema_version=8, integrity_digest=original["integrity_digest"])
                self.assertEqual(future, original)
                # Invalid originals must fail before a fault can re-sign them.
                original["current_object_index"] += 1
                with self.assertRaisesRegex(ValueError, "integrity digest"):
                    journal.transform(json.dumps(original).encode(), request, {"bucket": bucket}, mrf)

    def test_mrf_adapter_requires_active_original_business_work_on_the_exact_frozen_replica(self):
        raw = (Path(__file__).parent / "testdata/validation/mrf-mixed-payload.bin").read_bytes()
        bucket = "rv-heal-084db3ab418d55ba"
        for change in (None, "held", "unbound", "accepted", "other-replica"):
            with tempfile.TemporaryDirectory() as directory, self.subTest(change=change):
                root = Path(directory).resolve()
                intents = mrf.decode_journal(raw)
                for item in intents:
                    item["lifecycle"] = {"state": {"state": "active"}, "source_bucket_incarnation_id": self.incarnation,
                                         "last_operator_acceptance": None}
                business = next(item for item in intents if item["bucket"] == bucket)
                if change == "held": business["lifecycle"]["state"] = {"state": "bucket_incarnation_changed"}
                elif change == "unbound": business["lifecycle"]["source_bucket_incarnation_id"] = None
                elif change == "accepted": business["lifecycle"]["last_operator_acceptance"] = {"operator": "test"}
                checkpoint = {"present": True, "intents": intents, "replicas": [{"disk": 0, "slot": 0}],
                              "owner": self.token, "sequence": 7, "payloadSha256": hashlib.sha256(raw).hexdigest()}
                def exact_checkpoint(observed, *, replica):
                    if replica != (0, 0): raise ValueError("exact MRF committed replica is absent")
                    return checkpoint
                backend = types.SimpleNamespace(read_records=lambda *a: {}, checkpoint=exact_checkpoint)
                owner = {"runtimeProfile": "mrf-crash", "bucket": bucket, "binarySha256": "b" * 64}
                request = {"runId": "gh-12345-1-h2", "action": "journal-apply", "family": "mrf", "taskId": None,
                    "disk": 0, "relativePath": ".rustfs.sys/.heal-mrf-snapshot.1.bin" if change == "other-replica" else ".rustfs.sys/.heal-mrf-snapshot.0.bin",
                    "kind": "checksum", "faultId": "F07-mrf-checksum-0-0-0", "expectedBefore": journal.entry(raw),
                    "mrfIdentity": {"owner": self.token, "sequence": 7, "payloadSha256": hashlib.sha256(raw).hexdigest(),
                                    "bucketIncarnationId": self.incarnation}}
                with patch.object(fleet_node, "owned", return_value=(root, [root], owner)), patch.object(fleet_node, "stopped"), \
                     patch.object(fleet_node, "digest", return_value="b" * 64), patch.object(fleet_node, "fault_module", return_value=fault), \
                     patch.dict(fleet_node.__dict__, journal_fault_backend=journal, mrf=backend), \
                     patch.object(journal, "install", return_value={"applied": True}) as install:
                    if change is None: self.assertTrue(fleet_node.journal_operation(request)["applied"])
                    else:
                        with self.assertRaises(ValueError): fleet_node.journal_operation(request)
                        install.assert_not_called()

    def signed(self, value):
        value = copy.deepcopy(value)
        value["integrity_digest"] = None
        unsigned = copy.deepcopy(value)
        for name in ("processed_objects", "failed_objects", "skipped_objects"): unsigned[name].sort()
        value["integrity_digest"] = base64.b64encode(hashlib.sha256(json.dumps(unsigned,
            sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).digest()).decode()
        return json.dumps(value).encode()

    def checkpoint(self, processed=2, time=100):
        return self.signed({"task_id": self.token, "schema_version": 8, "checkpoint_time": time,
            "current_bucket_index": 0, "current_object_index": processed,
            "admin": {"completed": False, "buckets": {self.bucket: self.incarnation}, "options": OPTIONS,
                "set_disk_id": "pool_0_set_0", "outcome": {"execution": {"state": "running"}, "coverage": "partial",
                    "objects": [], "objectsTruncated": False, "counters": {"processed": processed, "healed": 1,
                        "unchanged": processed - 1, "skipped": 0, "failed": 0, "unknown": 0,
                        "attemptFailures": 0, "overflowed": False}}},
            "processed_objects": [f"objects/{i}" for i in reversed(range(processed))],
            "failed_objects": [], "skipped_objects": [], "successful_objects": processed,
            "failed_object_count": 0, "skipped_object_count": 0, "skipped_new_versions": 0, "skipped_ilm_expired": 0,
            "processed_bytes": processed, "total_objects": 8, "total_bytes": 8,
            "baseline_generation": None, "baseline_known": False, "counter_unknown": False})

    def stale_fixture(self, root):
        owner, request, path, early = self.fixture(root, kind="stale-generation")
        capture_request = {key: value for key, value in request.items() if key != "kind"}
        capture_request["faultId"] = "F07-early"
        with fault.directory(root) as fd: journal.capture(fd, capture_request, owner, fault)
        newer = self.checkpoint(processed=3, time=101)
        path.write_bytes(newer)
        request.update(expectedBefore=journal.entry(newer), captureId=capture_request["faultId"])
        return owner, request, path, early, newer

    def fixture(self, root, kind="truncate", family="checkpoint"):
        (root / "data/.rustfs.sys/buckets").mkdir(parents=True)
        (root / "quarantine").mkdir()
        owner = {"runtimeProfile": "root-restart", "runId": "gh-12345-1-h1", "bucket": self.bucket, "binarySha256": "a" * 64}
        request = {"runId": owner["runId"], "family": family, "taskId": self.token, "disk": 0,
            "faultId": "F07-checkpoint-0", "kind": kind, "relativePath": journal.names(family, self.token)[0]}
        raw = self.checkpoint() if family == "checkpoint" else json.dumps({"schema": 3, "task_id": self.token,
            "heal_type": {"type": "bucket", "bucket": self.bucket}, "bucket_incarnation_id": self.incarnation}).encode()
        path = root / "data" / request["relativePath"]
        path.write_bytes(raw)
        request["expectedBefore"] = journal.entry(raw)
        (root / "data/.rustfs.sys/unrelated.json").write_bytes(b"untouched control")
        return owner, request, path, raw

    def test_real_file_fault_and_idempotent_rescue_retain_original_and_failed_bytes(self):
        for kind in ["truncate", "future-schema"]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root, kind)
                with fault.directory(root) as fd:
                    result = journal.install(fd, request, owner, fault, mrf)
                    self.assertEqual(result["before"], journal.entry(raw))
                    self.assertNotEqual(path.read_bytes(), raw)
                    self.assertFalse(result["productRepairClaimed"])
                    if kind == "future-schema": self.assertEqual(json.loads(path.read_bytes())["schema_version"], 9)
                    rescued = journal.recover(fd, request, owner, fault)
                    self.assertTrue(rescued["restoredFromBackup"])
                    self.assertEqual(path.read_bytes(), raw)
                    self.assertFalse(journal.recover(fd, request, owner, fault)["restoredFromBackup"])
                self.assertEqual((root / "data/.rustfs.sys/unrelated.json").read_bytes(), b"untouched control")
                self.assertEqual((root / "quarantine/F07-checkpoint-0.original").read_bytes(), raw)
                self.assertEqual(len(list((root / "quarantine").glob("*.failed-*"))), 1)

    def test_stale_injection_uses_original_capture_and_rescue_restores_newer_pre_fault_bytes(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, early, newer = self.stale_fixture(root)
            with fault.directory(root) as fd:
                result = journal.install(fd, request, owner, fault, mrf)
                self.assertEqual(path.read_bytes(), early)
                self.assertEqual(result["capturedGeneration"], journal.entry(early))
                self.assertEqual(result["before"], journal.entry(newer))
                self.assertEqual((root / "quarantine/F07-checkpoint-0.original").read_bytes(), newer)
                self.assertTrue(journal.recover(fd, request, owner, fault)["restoredFromBackup"])
                self.assertEqual(path.read_bytes(), newer)
                self.assertFalse(journal.recover(fd, request, owner, fault)["restoredFromBackup"])
            self.assertEqual((root / "quarantine/F07-early.early.checkpoint").read_bytes(), early)
            self.assertEqual(next((root / "quarantine").glob("*.failed-*")).read_bytes(), early)
            self.assertEqual((root / "data/.rustfs.sys/unrelated.json").read_bytes(), b"untouched control")

    def test_stale_generation_rejects_valid_digests_with_different_scopes_or_unproven_progress(self):
        owner = {"bucket": self.bucket}
        request = {"family": "checkpoint", "taskId": self.token, "kind": "stale-generation",
                   "relativePath": journal.names("checkpoint", self.token)[0]}
        current = self.checkpoint(processed=3, time=101)
        for change in ("task", "incarnation", "options", "option-type", "set", "equal", "newer", "zero",
                       "counter-type", "regressed-counter", "dishonest-counter", "overflow", "time", "time-type",
                       "cursor", "cursor-type", "completed", "coverage", "schema-type"):
            old = json.loads(self.checkpoint())
            counters = old["admin"]["outcome"]["counters"]
            if change == "task": old["task_id"] = str(uuid.uuid4())
            elif change == "incarnation": old["admin"]["buckets"][self.bucket] = str(uuid.uuid4())
            elif change == "options": old["admin"]["options"]["nolock"] = True
            elif change == "option-type": old["admin"]["options"]["pool"] = False
            elif change == "set": old["admin"]["set_disk_id"] = "pool_0_set_1"
            elif change == "equal": old = json.loads(current)
            elif change == "newer": old = json.loads(self.checkpoint(processed=4, time=102))
            elif change == "zero": counters.update(processed=0, healed=0, unchanged=0)
            elif change == "counter-type": counters["healed"] = True
            elif change == "regressed-counter": counters.update(healed=2, unchanged=0)
            elif change == "dishonest-counter": counters["unchanged"] = 0
            elif change == "overflow": counters["overflowed"] = True
            elif change == "time": old["checkpoint_time"] = 102
            elif change == "time-type": old["checkpoint_time"] = True
            elif change == "cursor": old["current_object_index"] = 4
            elif change == "cursor-type": old["current_object_index"] = 2.0
            elif change == "completed": old["admin"]["completed"] = True
            elif change == "coverage": old["admin"]["outcome"]["coverage"] = "complete"
            else: old["schema_version"] = 8.0
            with self.subTest(change=change), self.assertRaises(ValueError):
                journal.transform(current, request, owner, mrf, earlier=self.signed(old))
        self.assertEqual(journal.transform(current, request, owner, mrf, earlier=self.checkpoint()), self.checkpoint())

    def test_missing_corrupt_or_foreign_capture_never_modifies_current_checkpoint(self):
        for change in ("missing-proof", "missing-bytes", "proof", "bytes", "candidate", "device", "mount",
                       "disk", "disk-type", "run", "task", "path", "capture-id", "same-id", "entry-type", "duplicate-key"):
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, _, newer = self.stale_fixture(root)
                proof_path = root / "quarantine/F07-early.early.intent.json"
                bytes_path = root / "quarantine/F07-early.early.checkpoint"
                proof = json.loads(proof_path.read_text())
                if change == "missing-proof": proof_path.unlink()
                elif change == "missing-bytes": bytes_path.unlink()
                elif change == "proof": proof_path.write_bytes(b'{"invalid":true}')
                elif change == "bytes": bytes_path.write_bytes(b"changed product bytes")
                elif change == "capture-id": request["captureId"] = "F07-foreign"
                elif change == "same-id": request["captureId"] = request["faultId"]
                elif change == "duplicate-key": proof_path.write_bytes(proof_path.read_bytes()[:-1] + b',"disk":0}')
                else:
                    if change == "candidate": proof["binarySha256"] = "b" * 64
                    elif change == "device": proof["device"] = -1
                    elif change == "mount": proof["mountId"] = 2
                    elif change == "disk": proof["disk"] = 1
                    elif change == "disk-type": proof["disk"] = False
                    elif change == "run": proof["runId"] = "gh-other-1-h1"
                    elif change == "task": proof["taskId"] = str(uuid.uuid4())
                    elif change == "path": proof["relativePath"] = ".rustfs.sys/foreign"
                    else: proof["captured"]["size"] = float(proof["captured"]["size"])
                    proof_path.write_text(json.dumps(proof))
                with fault.directory(root) as fd, self.subTest(change=change), self.assertRaises((ValueError, FileNotFoundError)):
                    journal.install(fd, request, owner, fault, mrf)
                self.assertEqual(path.read_bytes(), newer)
                self.assertFalse((root / "quarantine/F07-checkpoint-0.original").exists())

    def test_capture_is_immutable_and_rejects_drift_without_touching_product_bytes(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
            request.pop("kind")
            with fault.directory(root) as fd:
                with self.assertRaisesRegex(ValueError, "NOT_OBSERVED"):
                    journal.capture(fd, {**request, "expectedBefore": journal.entry(raw + b" ")}, owner, fault)
                self.assertEqual(list((root / "quarantine").iterdir()), [])
                captured = journal.capture(fd, request, owner, fault)
                self.assertTrue(captured["originalProductBytes"])
                self.assertFalse(captured["productMetadataMutated"])
                with self.assertRaises(FileExistsError): journal.capture(fd, request, owner, fault)
                self.assertEqual(path.read_bytes(), raw)

    def test_stale_rescue_rejects_advanced_deleted_checkpoint_or_lost_capture_identity(self):
        for change in ("advance", "delete", "capture-id", "capture-bytes", "capture-proof", "device-type"):
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, _, _ = self.stale_fixture(root)
                with fault.directory(root) as fd:
                    journal.install(fd, request, owner, fault, mrf)
                    if change == "advance": path.write_bytes(self.checkpoint(processed=4, time=102))
                    elif change == "delete": path.unlink()
                    elif change == "capture-id": request["captureId"] = "F07-other"
                    elif change == "capture-bytes": (root / "quarantine/F07-early.early.checkpoint").write_bytes(b"changed")
                    elif change == "capture-proof": (root / "quarantine/F07-early.early.intent.json").unlink()
                    else:
                        p = root / "quarantine/F07-checkpoint-0.intent.json"
                        intent = json.loads(p.read_text()); intent["device"] = float(intent["device"])
                        p.write_text(json.dumps(intent))
                    current = path.read_bytes() if path.exists() else None
                    with self.subTest(change=change), self.assertRaises((ValueError, FileNotFoundError)):
                        journal.recover(fd, request, owner, fault)
                    self.assertEqual(path.read_bytes() if path.exists() else None, current)

    def test_root_fault_requires_exact_bucket_admission_and_preserves_terminal_names(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root, family="root")
            with fault.directory(root) as fd:
                journal.install(fd, request, owner, fault, mrf)
                journal.recover(fd, request, owner, fault)
                self.assertEqual(path.read_bytes(), raw)
            for field, value in [("task_id", str(uuid.uuid4())), ("bucket_incarnation_id", str(uuid.UUID(int=0))),
                                 ("heal_type", {"type": "bucket", "bucket": "foreign"})]:
                bad = json.loads(raw); bad[field] = value
                with self.subTest(field=field), self.assertRaises(ValueError):
                    journal.transform(json.dumps(bad).encode(), request, owner, mrf)

    def test_path_token_slot_profile_and_expected_bytes_cannot_escape_scope(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
            changes = [{"relativePath": ".rustfs.sys/config/config.json"}, {"relativePath": "../foreign"},
                {"relativePath": journal.names("checkpoint", self.token)[1]}, {"taskId": str(uuid.UUID(int=0))},
                {"taskId": "../escape"}, {"disk": True}, {"disk": 4}, {"runId": "gh-other-1-h1"},
                {"faultId": "C11-foreign"}, {"kind": "stale-generation"}, {"expectedBefore": {"sha256": "b" * 64, "size": len(raw)}}]
            with fault.directory(root) as fd:
                for change in changes:
                    with self.subTest(change=change), self.assertRaises(ValueError): journal.install(fd, {**request, **change}, owner, fault, mrf)
                    self.assertEqual(path.read_bytes(), raw)
                with self.assertRaises(ValueError): journal.install(fd, request, {**owner, "runtimeProfile": "manual"}, fault, mrf)

    def test_corrupt_or_foreign_original_checkpoint_is_never_mutated(self):
        owner = {"bucket": self.bucket}
        request = {"family": "checkpoint", "taskId": self.token, "kind": "truncate",
                   "relativePath": journal.names("checkpoint", self.token)[0]}
        raw = self.checkpoint()
        for field, value in [("task_id", str(uuid.uuid4())), ("schema_version", 9), ("integrity_digest", "wrong"),
                             ("admin", {"completed": True, "buckets": {self.bucket: self.incarnation}}),
                             ("admin", {"completed": False, "buckets": {"foreign": self.incarnation}})]:
            bad = json.loads(raw); bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): journal.transform(json.dumps(bad).encode(), request, owner, mrf)

    def test_successor_deleted_or_foreign_bytes_are_not_overwritten_by_rescue(self):
        for changed in ["advanced", "missing", "backup", "candidate", "device"]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, _ = self.fixture(root)
                with fault.directory(root) as fd:
                    journal.install(fd, request, owner, fault, mrf)
                    if changed == "advanced": path.write_bytes(b"new product responsibility")
                    elif changed == "missing": path.unlink()
                    elif changed == "backup": (root / "quarantine/F07-checkpoint-0.original").write_bytes(b"bad backup")
                    elif changed == "candidate": owner["binarySha256"] = "b" * 64
                    else:
                        p = root / "quarantine/F07-checkpoint-0.intent.json"; intent = json.loads(p.read_text()); intent["device"] = -1
                        p.write_text(json.dumps(intent))
                    current = path.read_bytes() if path.exists() else None
                    with self.subTest(changed=changed), self.assertRaises((ValueError, FileNotFoundError)):
                        journal.recover(fd, request, owner, fault)
                    self.assertEqual(path.read_bytes() if path.exists() else None, current)

    def test_record_symlink_hardlink_and_retained_backup_prevent_mutation(self):
        for changed in ["symlink", "hardlink", "backup"]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
                if changed == "symlink": path.unlink(); original = root / "original"; original.write_bytes(raw); path.symlink_to(original)
                elif changed == "hardlink": os.link(path, root / "alias")
                else: (root / "quarantine/F07-checkpoint-0.original").write_bytes(b"retained")
                with fault.directory(root) as fd, self.subTest(changed=changed), self.assertRaises((OSError, ValueError)):
                    journal.install(fd, request, owner, fault, mrf)
                self.assertEqual(path.read_bytes(), raw)

    def test_interrupted_rescue_temp_is_revalidated_without_erasing_injection_evidence(self):
        for changed in [False, True]:
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
                with fault.directory(root) as fd:
                    journal.install(fd, request, owner, fault, mrf)
                    injection = path.parent / ".F07-F07-checkpoint-0.inject"
                    injection.write_bytes(b"interrupted injection evidence")
                    rescue = path.parent / ".F07-F07-checkpoint-0.rescue"
                    rescue.write_bytes(b"wrong temporary" if changed else raw)
                    if changed:
                        current = path.read_bytes()
                        with self.assertRaises(ValueError): journal.recover(fd, request, owner, fault)
                        self.assertEqual(path.read_bytes(), current)
                    else:
                        journal.recover(fd, request, owner, fault)
                        self.assertEqual(path.read_bytes(), raw)
                    self.assertEqual(injection.read_bytes(), b"interrupted injection evidence")

    def test_mrf_crc_fault_retains_every_original_identity_and_refuses_foreign_payload(self):
        bucket, key = self.bucket.encode(), b"offline/new.bin"
        raw = struct.pack("<BBBBQB", 1, 2, 3, 0, 100, 1) + uuid.UUID(self.token).bytes
        raw += struct.pack("<II", 0, 0) + struct.pack("<II", len(bucket), len(key)) + bucket + key
        raw += struct.pack("<I", zlib.crc32(raw))
        request = {"family": "mrf", "kind": "checksum", "relativePath": ".rustfs.sys/.heal-mrf-snapshot.0.bin"}
        damaged = journal.transform(raw, request, {"bucket": self.bucket}, mrf)
        self.assertEqual(damaged[:-1], raw[:-1])
        with self.assertRaises(ValueError): mrf.decode_journal(damaged)
        with self.assertRaises(ValueError): journal.transform(raw, request, {"bucket": "foreign"}, mrf)

    def test_census_is_exact_and_never_enumerates_unrelated_system_metadata(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); _, request, _, raw = self.fixture(root)
            with patch.object(os, "listdir", side_effect=AssertionError("system metadata enumeration forbidden")):
                result = journal.census([root], request, fault)
            self.assertEqual(len(result["records"]), 4)
            present = [row for row in result["records"] if row["present"]]
            self.assertEqual(len(present), 1)
            self.assertEqual(base64.b64decode(present[0]["rawBase64"]), raw)
            self.assertFalse(result["directoryEnumerationUsed"])

    def test_stable_observation_rejects_atomic_replacement_between_reads(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); _, request, path, raw = self.fixture(root)
            stable = journal.stable_census([root], request, fault)
            self.assertFalse(stable["quiescent"])
            self.assertTrue(stable["stable"])
            original = journal.census
            calls = []
            def changed(*args, **kwargs):
                value = original(*args, **kwargs)
                if not calls: path.write_bytes(raw + b" ")
                calls.append(value)
                return value
            with patch.object(journal, "census", side_effect=changed), self.assertRaisesRegex(ValueError, "NOT_OBSERVED"):
                journal.stable_census([root], request, fault)

    def test_duplicate_and_nonfinite_json_never_become_candidate_checkpoint_bytes(self):
        raw = self.checkpoint()
        for bad in [raw[:-1] + b',"task_id":"foreign"}', raw.replace(b'"current_object_index": 2', b'"current_object_index": NaN')]:
            with self.subTest(raw=bad), self.assertRaises(ValueError): journal.checkpoint_value(bad, self.token, self.bucket)

    def test_live_adapter_binds_both_reads_to_original_process_and_cannot_mutate(self):
        identity = {name: name for name in fleet_node.PROCESS_FIELDS}
        identity.update(pid=50, state="T")
        owner = {"runtimeProfile": "root-restart", "binarySha256": "a" * 64}
        observed = {"readOnly": True, "quiescent": False, "stable": True, "independentReads": 2}
        request = {"runId": "gh-12345-1-h1", "action": "journal-observe", "family": "checkpoint",
                   "taskId": self.token, "expected": identity}
        backend = types.SimpleNamespace(stable_census=lambda *args: observed)
        with patch.object(fleet_node, "owned", return_value=(Path('/owned'), [], owner)), \
             patch.object(fleet_node, "digest", return_value="a" * 64), \
             patch.object(fleet_node, "fault_module", return_value=fault), \
             patch.dict(fleet_node.__dict__, journal_fault_backend=backend), \
             patch.object(fleet_node, "stopped", side_effect=AssertionError("read-only live observation must not stop service")):
            with patch.object(fleet_node, "process", side_effect=[identity, identity]):
                result = fleet_node.journal_operation(request)
                self.assertTrue(result["processBound"])
                self.assertTrue(result["serviceFrozen"])
                self.assertFalse(result["quiescent"])
            for changed in [{**identity, "pid": 60}, {**identity, "binarySha256": "b" * 64},
                            {**identity, "startTicks": 999}]:
                with self.subTest(changed=changed), patch.object(fleet_node, "process", side_effect=[identity, changed]), self.assertRaises(ValueError):
                    fleet_node.journal_operation(request)
            with patch.object(fleet_node, "process", return_value=identity), self.assertRaises(ValueError):
                fleet_node.journal_operation({**request, "kind": "truncate"})

    def test_live_capture_requires_the_same_stable_record_and_service_birth(self):
        for changed in (None, "record", "process"):
            with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
                root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
                identity = {name: name for name in fleet_node.PROCESS_FIELDS}
                identity.update(pid=50, state="S")
                request.pop("kind"); request.update(action="journal-capture", expected=identity)
                if changed == "record": request["expectedBefore"] = journal.entry(raw + b" ")
                successor = {**identity, "startTicks": 999} if changed == "process" else identity
                with patch.object(fleet_node, "owned", return_value=(root, [root], owner)), \
                     patch.object(fleet_node, "digest", return_value=owner["binarySha256"]), \
                     patch.object(fleet_node, "fault_module", return_value=fault), \
                     patch.dict(fleet_node.__dict__, journal_fault_backend=journal), \
                     patch.object(fleet_node, "stopped", side_effect=AssertionError("capture must not stop service")), \
                     patch.object(fleet_node, "process", side_effect=[identity, successor]):
                    if changed:
                        with self.subTest(changed=changed), self.assertRaises(ValueError): fleet_node.journal_operation(request)
                    else:
                        result = fleet_node.journal_operation(request)
                        self.assertTrue(result["processBound"])
                        self.assertFalse(result["serviceFrozen"])
                        self.assertEqual(result["observation"]["independentReads"], 2)
                        self.assertEqual(result["captured"], journal.entry(raw))
                self.assertEqual(path.read_bytes(), raw)
                captured = root / "quarantine/F07-checkpoint-0.early.checkpoint"
                self.assertEqual(captured.exists(), changed != "record")

    def test_non_stale_fault_cannot_consume_a_capture_and_capture_cannot_mutate_records(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, raw = self.fixture(root)
            with fault.directory(root) as fd, self.assertRaises(ValueError):
                journal.install(fd, {**request, "captureId": "F07-early"}, owner, fault, mrf)
            identity = {name: name for name in fleet_node.PROCESS_FIELDS}; identity.update(pid=50, state="S")
            capture = {**request, "action": "journal-capture", "expected": identity}
            with patch.object(fleet_node, "owned", return_value=(root, [root], owner)), \
                 patch.object(fleet_node, "digest", return_value=owner["binarySha256"]), \
                 patch.object(fleet_node, "fault_module", return_value=fault), \
                 patch.dict(fleet_node.__dict__, journal_fault_backend=journal), self.assertRaises(ValueError):
                fleet_node.journal_operation(capture)
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(list((root / "quarantine").iterdir()), [])

    def test_remote_action_loads_exact_record_and_fault_dependencies(self):
        with tempfile.TemporaryDirectory() as temp, patch("scripts.validation_heal.healthy.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout='{}', stderr='')) as command:
            Fleet(["node"], Path(temp), "gh-12345-1-h1").remote("node", "journal-census", family="root", taskId=self.token)
            source = command.call_args.kwargs["input"].rsplit("\nmain(", 1)[0]
            scope = {"__name__": "validation_journal_remote"}
            exec(compile(source, "journal-remote", "exec"), scope)
            self.assertTrue(callable(scope["journal_fault_backend"].install))
            self.assertTrue(callable(scope["fault_module"]().read_file))
            self.assertTrue(callable(scope["mrf"].checkpoint))

    def test_fleet_cleanup_restores_newer_original_before_deleting_private_namespace(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, _, newer = self.stale_fixture(root)
            fleet = Fleet(["node"], root, owner["runId"]); fleet.reserved = ["node"]
            fleet.journal_faults.append(("node", request))
            with fault.directory(root) as fd: journal.install(fd, request, owner, fault, mrf)
            actions = []
            def remote(node, action, **params):
                actions.append(action)
                if action == "journal-recover":
                    with fault.directory(root) as fd: return journal.recover(fd, params, owner, fault)
                if action == "cleanup": self.assertEqual(path.read_bytes(), newer)
                return {}
            with patch.object(fleet, "remote", side_effect=remote): self.assertTrue(fleet.cleanup())
            self.assertLess(actions.index("stop"), actions.index("journal-recover"))
            self.assertLess(actions.index("journal-recover"), actions.index("cleanup"))
            self.assertEqual(fleet.journal_faults, [])

    def test_fleet_cleanup_keeps_every_namespace_and_pending_fault_when_product_advanced(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, path, _, _ = self.stale_fixture(root)
            fleet = Fleet(["node", "peer"], root, owner["runId"]); fleet.reserved = ["node", "peer"]
            fleet.journal_faults.append(("node", request))
            with fault.directory(root) as fd: journal.install(fd, request, owner, fault, mrf)
            advanced = self.checkpoint(processed=4, time=102); path.write_bytes(advanced)
            actions = []
            def remote(node, action, **params):
                actions.append(action)
                if action == "journal-recover":
                    with fault.directory(root) as fd: return journal.recover(fd, params, owner, fault)
                return {}
            with patch.object(fleet, "remote", side_effect=remote): self.assertFalse(fleet.cleanup())
            self.assertNotIn("cleanup", actions)
            self.assertEqual(path.read_bytes(), advanced)
            self.assertEqual(fleet.journal_faults, [("node", request)])
            recovery = json.loads(next(root.glob("*-recovery.json")).read_text())
            self.assertEqual(recovery["cleanup"], "FAIL")
            self.assertTrue(recovery["errors"][0]["namespacesRetained"])

    def test_bounded_journal_census_reads_verified_candidate_views_and_rejects_other_families(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, "mount_id", return_value=1):
            root = Path(temp).resolve(); owner, request, _, raw = self.fixture(root)
            owner["runtimeProfile"] = "journal-bounded"
            request = {key: request[key] for key in ("runId", "family", "taskId")}; request["action"] = "journal-census"
            with patch.object(fleet_node, "owned", return_value=(root, ['/original/unbound-root'], owner)), \
                 patch.object(fleet_node, "volume_views", return_value=[root]) as views, \
                 patch.object(fleet_node, "digest", return_value=owner['binarySha256']), \
                 patch.object(fleet_node, "fault_module", return_value=fault), patch.object(fleet_node, "stopped"), \
                 patch.dict(fleet_node.__dict__, journal_fault_backend=journal):
                result = fleet_node.journal_operation(request)
                self.assertEqual(base64.b64decode(next(row for row in result['records'] if row['present'])['rawBase64']), raw)
                views.assert_called_once()
                for family, action in (('mrf', 'journal-census'), ('root', 'journal-apply'), ('root', 'journal-capture')):
                    with self.subTest(family=family, action=action), self.assertRaises(ValueError):
                        fleet_node.journal_operation({**request, 'family': family, 'action': action})

    def test_race_root_census_reads_original_bytes_only_after_stopping_owned_service(self):
        for profile in ('manual', 'frozen-race-legacy'):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as temp, \
                 patch.object(fault, 'mount_id', return_value=1):
                root = Path(temp).resolve(); owner, request, _, _ = self.fixture(root)
                owner['runtimeProfile'] = profile
                raw = json.dumps({'schema': 3, 'task_id': self.token, 'heal_type': {'type': 'erasure_set'}}).encode()
                path = root / 'data/.rustfs.sys' / ('root-heal-' + self.token + '.json'); path.write_bytes(raw)
                request = {'runId': owner['runId'], 'action': 'journal-census', 'family': 'root', 'taskId': self.token}
                with patch.object(fleet_node, 'owned', return_value=(root, [root], owner)), \
                     patch.object(fleet_node, 'digest', return_value=owner['binarySha256']), \
                     patch.object(fleet_node, 'fault_module', return_value=fault), patch.object(fleet_node, 'stopped') as stopped, \
                     patch.dict(fleet_node.__dict__, journal_fault_backend=journal):
                    result = fleet_node.journal_operation(request)
                    stopped.assert_called_once_with(owner)
                    present = [row for row in result['records'] if row['present']]
                    self.assertEqual(len(present), 1)
                    self.assertEqual(base64.b64decode(present[0]['rawBase64']), raw)
                    self.assertTrue(result['readOnly']); self.assertTrue(result['quiescent'])
                    self.assertFalse(result['directoryEnumerationUsed'])
                    self.assertEqual(path.read_bytes(), raw)
                    with patch.object(fleet_node, 'stopped', side_effect=ValueError('service remains active')):
                        with self.assertRaisesRegex(ValueError, 'service remains active'):
                            fleet_node.journal_operation(request)
                    for change in ({'family': 'checkpoint'}, {'family': 'mrf'}, {'action': 'journal-observe'},
                                   {'action': 'journal-apply', 'disk': 0}, {'action': 'journal-recover', 'disk': 0},
                                   {'expected': {}}, {'relativePath': '.rustfs.sys/foreign.json'}):
                        with self.subTest(change=change), self.assertRaises(ValueError):
                            fleet_node.journal_operation({**request, **change})
                    self.assertEqual(path.read_bytes(), raw)
                    path.unlink()
                    result = fleet_node.journal_operation(request)
                    self.assertTrue(all(not row['present'] for row in result['records']))

    def test_bounded_root_admission_reads_the_actual_service_view_without_mutating_it(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(fault, 'mount_id', return_value=1):
            root = Path(temp).resolve(); owner, request, _, _ = self.fixture(root)
            owner['runtimeProfile'] = 'journal-bounded'
            raw = json.dumps({'schema': 3, 'task_id': self.token, 'heal_type': {'type': 'erasure_set'}}).encode()
            path = root / 'data/.rustfs.sys' / ('root-heal-' + self.token + '.json'); path.write_bytes(raw)
            request = {'runId': owner['runId'], 'taskId': self.token, 'quiescent': True}
            with patch.object(fleet_node, 'owned', return_value=(root, ['/original/unbound'], owner)), \
                 patch.object(fleet_node, 'volume_views', return_value=[root]), \
                 patch.object(fleet_node, 'digest', return_value=owner['binarySha256']), patch.object(fleet_node, 'stopped'):
                observed = fleet_node.task_records(request)
            present = [row for row in observed['records'] if row['present']]
            self.assertEqual(len(present), 1)
            self.assertEqual(present[0]['sha256'], journal.entry(raw)['sha256'])
            self.assertEqual(path.read_bytes(), raw)

    def test_bounded_journal_cleanup_releases_filler_before_rescue_and_preserves_failed_release(self):
        for failed in (False, True):
            with tempfile.TemporaryDirectory() as temp:
                fleet = Fleet(['node'], Path(temp), 'gh-12345-1-h1', runtime_profile='journal-bounded')
                fleet.reserved = ['node']; fleet.journal_faults.append(('node', {'faultId': 'F07-checkpoint-0'}))
                actions = []
                def remote(node, action, **params):
                    actions.append(action)
                    if action == 'bounded-volumes-release':
                        self.assertTrue(params['quiescent'])
                        if failed: raise ValueError('owned filesystem identity changed')
                    if action == 'journal-recover':
                        return {'backupRetained': True, 'originalResponsibilityRecreated': False, 'productRepairClaimed': False}
                    return {}
                with patch.object(fleet, 'remote', side_effect=remote): self.assertEqual(fleet.cleanup(), not failed)
                if failed:
                    self.assertNotIn('journal-recover', actions); self.assertNotIn('cleanup', actions)
                    self.assertTrue(fleet.journal_faults)
                else:
                    self.assertLess(actions.index('bounded-volumes-release'), actions.index('journal-recover'))
                    self.assertLess(actions.index('journal-recover'), actions.index('cleanup'))

    def test_bounded_remote_journal_and_cleanup_embed_the_exact_capacity_backend(self):
        with tempfile.TemporaryDirectory() as temp, patch('scripts.validation_heal.healthy.subprocess.run',
                return_value=types.SimpleNamespace(returncode=0, stdout='{}', stderr='')) as command:
            fleet = Fleet(['node'], Path(temp), 'gh-12345-1-h1', runtime_profile='journal-bounded')
            for action in ('journal-census', 'cleanup'):
                fleet.remote('node', action, family='checkpoint', taskId=self.token)
                source = command.call_args.kwargs['input'].rsplit('\nmain(', 1)[0]
                scope = {'__name__': 'bounded_journal_remote'}
                exec(compile(source, 'bounded-journal-remote', 'exec'), scope)
                self.assertTrue(callable(scope['volume_module']().views))
                self.assertTrue(callable(scope['volume_module']().dispose))
                if action == 'journal-census': self.assertTrue(callable(scope['journal_fault_backend'].census))


if __name__ == "__main__": unittest.main()
