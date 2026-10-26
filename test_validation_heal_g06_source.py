"""Frozen DS and COPY direction contracts; synthetic scenes are not live proof."""
import copy
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

import test_validation_heal as physical_fixtures
from scripts.validation_heal import race


class G06DispatchTests(unittest.TestCase):
    def server(self, *, summary='running', hold_copy=True, first_only=False):
        copy_seen, sampled = threading.Event(), threading.Event()
        failures, calls = [], [0]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def reply(self, raw, kind):
                self.send_response(200); self.send_header('Content-Type', kind)
                self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)
            def do_PUT(self):
                self.rfile.read(int(self.headers.get('Content-Length', '0')))
                calls[0] += 1
                copy_seen.set()
                if hold_copy and (not first_only or calls[0] == 1) and not sampled.wait(3):
                    failures.append('SDK response did not observe Admin completion')
                self.reply(b'' if '?tagging' in self.path else
                    b'<CopyObjectResult><ETag>"copy-ack"</ETag><LastModified>2026-10-26T00:00:00Z</LastModified></CopyObjectResult>', 'application/xml')
            def do_POST(self):
                if not copy_seen.wait(3): failures.append('Admin request preceded an actual COPY')
                state = summary if not first_only or calls[0] == 1 else 'finished'
                self.reply(json.dumps({'summary': state, 'settings': race.COPY_REFERENCE['effective'],
                    'outcome': {'counters': {'processed': 1}}}).encode(), 'application/json')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True); worker.start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        binding = {'schemaVersion': 1, 'runId': 'gh-12345-1-h1', 'jumpHostOnly': True,
            'exclusiveHealService': True, 'leaseExpiresAt': (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            'bucketPrefix': 'rv-heal-0123456789abcdef', 'endpoints': {'node1': f'http://127.0.0.1:{server.server_port}'},
            'defaultEndpoint': 'node1', 'candidateCommit': race.COPY_CANDIDATE, 'harnessCommit': '2' * 40,
            'candidatePackageSha256': '3' * 64, 'candidateBinarySha256': '4' * 64, 'rcBinarySha256': '5' * 64,
            'maxInventoryObjects': 512, 'maxInventoryBytes': 512 * 1024**2}
        return binding, sampled, failures

    def test_real_sdk_dispatch_schedules_original_status_and_keeps_strict_interval_oracle(self):
        for summary in ('running', 'finished'):
            with self.subTest(summary=summary), tempfile.TemporaryDirectory() as directory:
                binding, sampled, failures = self.server(summary=summary)
                writer_probe = race.DispatchProbe(binding, credential_pair=('test-access', 'test-secret'))
                admin_probe = race.Probe(binding, credential_pair=('test-access', 'test-secret'))
                original_admin = admin_probe.admin
                def admin(*args):
                    result = original_admin(*args)
                    sampled.set()  # Server-only fixture barrier; production notification never waits.
                    return result
                admin_probe.admin = admin
                writer = race.RaceRecorder(writer_probe, Path(directory) / 'foreground-ACKs', versioned=False)
                plan = race.operation_plan(binding['bucketPrefix'], 'copy-destination', [])
                plan[0]['branch'] = 'copy-destination'
                records = {}
                fleet = SimpleNamespace(record=lambda label, value: records.setdefault(label, value))
                responses, accepted = race.race(writer, plan, admin_probe, fleet,
                    '/rustfs/admin/v3/heal/' + binding['bucketPrefix'], 'original-token', race.COPY_REFERENCE)
                self.assertEqual(responses[0]['CopyObjectResult']['ETag'], '"copy-ack"')
                self.assertEqual(accepted, summary == 'running')
                self.assertFalse(failures)
                ack = json.loads(next(writer.output.glob('*-ack.json')).read_text())
                dispatch = records['G06-request-dispatch-observation']
                sent = dispatch['requestSendMonotonicNs'][0]
                observation = ack['observation']; status = records['original-token-status']
                self.assertLess(observation['callStartedMonotonicNs'], sent)
                self.assertLess(sent, status['callStartedMonotonicNs'])
                self.assertLess(status['callFinishedMonotonicNs'], observation['callFinishedMonotonicNs'])
                self.assertFalse(dispatch['SDKCallDelayed'] or dispatch['publicOverlapProved'] or dispatch['canonicalCasePassClaimed'])

    def test_notification_never_waits_for_an_observer_or_changes_an_ack(self):
        binding, _, failures = self.server(hold_copy=False)
        probe = race.DispatchProbe(binding, credential_pair=('test-access', 'test-secret'))
        plan = race.operation_plan(binding['bucketPrefix'], 'copy-source', [])
        result = probe.s3(plan[0]['operation'], plan[0]['params'])
        self.assertTrue(result['acknowledged'])
        sent = probe.dispatched.get_nowait()
        self.assertLess(result['callStartedMonotonicNs'], sent)
        self.assertLess(sent, result['callFinishedMonotonicNs'])
        self.assertEqual(result['response']['CopyObjectResult']['ETag'], '"copy-ack"')
        self.assertFalse(failures)

    def test_frozen_sixteen_tag_writes_require_all_acks_and_one_nested_running_sample(self):
        for summary in ('running', 'finished'):
            with self.subTest(summary=summary), tempfile.TemporaryDirectory() as directory:
                binding, sampled, failures = self.server(summary=summary, first_only=True)
                writer_probe = race.DispatchProbe(binding, credential_pair=('test-access', 'test-secret'))
                admin_probe = race.Probe(binding, credential_pair=('test-access', 'test-secret'))
                original_admin = admin_probe.admin
                def admin(*args):
                    value = original_admin(*args); sampled.set(); return value
                admin_probe.admin = admin
                writer = race.RaceRecorder(writer_probe, Path(directory) / 'foreground-ACKs', versioned=False)
                plan = race.operation_plan(binding['bucketPrefix'], 'update-tags', [])
                for step in plan: step['branch'] = 'update-tags'
                records = {}; fleet = SimpleNamespace(record=lambda label, value: records.setdefault(label, value))
                responses, accepted = race.race(writer, plan, admin_probe, fleet,
                    '/rustfs/admin/v3/heal/' + binding['bucketPrefix'], 'original-token', race.COPY_REFERENCE)
                self.assertEqual(len(responses), 16)
                self.assertEqual(len(list(writer.output.glob('*-request.json'))), 16)
                acks = [json.loads(p.read_text()) for p in writer.output.glob('*-ack.json')]
                self.assertEqual(len(acks), 16)
                self.assertTrue(all(row['operationCheck'] == 'PASS' for row in acks))
                self.assertEqual(accepted, summary == 'running')
                proof = records['public-call-overlap-proof']
                self.assertEqual(proof['overlapRequirement'], 'at-least-one-of-frozen-16-tag-updates')
                self.assertEqual(proof['requiredCallOverlapObserved'], summary == 'running')
                self.assertFalse(proof['everyMutationOverlapped'])
                self.assertFalse(failures)


class G06SourceTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def baseline(self):
        return {'entries': [
            {'key': key, 'versionId': 'null', 'isLatest': True, 'deleteMarker': False,
             'payload': {'size': size, 'sha256': role},
             'properties': {'ETag': '"' + role + '"', 'ContentLength': size,
                            'ContentType': 'application/' + role, 'Metadata': {'role': role}},
             'tags': [{'Key': 'role', 'Value': role}]}
            for key, role, size in [(race.G06_TARGET_KEY, 'selected', 17),
                                    (race.G06_REFERENCE_KEY, 'other', 23)]]}

    def test_frozen_ds_keys_and_seed_use_the_existing_workload(self):
        self.assertEqual(race.G06_TARGET_KEY, '中文/文件/0252.bin')
        self.assertEqual(race.G06_REFERENCE_KEY, 'literal%2Fslash/0254.bin')
        recorder = Mock()
        recorder.probe = SimpleNamespace(binding={'bucketPrefix': self.bucket})
        recorder.call.return_value = {'ETag': '"ack"'}
        with patch.object(race, 'SIZE', 31):
            expected = race.seed_fixture(recorder, self.bucket, 'copy-source')
        prefixes = ('中文/文件', 'space key', 'literal%2Fslash', 'actual/slash', 'a/child', 'ab/control', 'b/other')
        keys = [f'{prefixes[i % 7]}/{i:04d}.bin' for i in range(257)]
        self.assertEqual([r['key'] for r in expected], keys)
        puts = [c for c in recorder.call.call_args_list if c.args[0] == 'put_object']
        self.assertEqual(len(puts), 257)
        self.assertEqual([c.args[1]['Key'] for c in puts], keys)
        self.assertEqual(puts[252].args[1]['Metadata'], {'heal-fixture': 'DS', 'nonce': '252-0'})
        self.assertEqual(puts[254].args[1]['Tagging'], 'suite=heal&fixture=DS')

    def test_copy_source_or_destination_is_the_original_missing_selected_key(self):
        for branch in ('copy-source', 'copy-destination'):
            with self.subTest(branch=branch):
                params = race.operation_plan(self.bucket, branch, self.baseline()['entries'])[0]['params']
                source, destination = ((race.G06_TARGET_KEY, race.G06_REFERENCE_KEY) if branch == 'copy-source'
                                       else (race.G06_REFERENCE_KEY, race.G06_TARGET_KEY))
                self.assertEqual(params['Key'], destination)
                self.assertEqual(params['CopySource'], {'Bucket': self.bucket, 'Key': source, 'VersionId': 'null'})

    def test_copy_final_s3_follows_source_content_metadata_and_tags(self):
        for branch in ('copy-source', 'copy-destination'):
            with self.subTest(branch=branch):
                before = self.baseline()
                plan = race.operation_plan(self.bucket, branch, before['entries'])
                source, destination = ((race.G06_TARGET_KEY, race.G06_REFERENCE_KEY) if branch == 'copy-source'
                                       else (race.G06_REFERENCE_KEY, race.G06_TARGET_KEY))
                original = {r['key']: r for r in before['entries']}
                after = race.final_inventory(before, branch, plan, [{'CopyObjectResult': {'ETag': '"copied"'}}])
                actual = {r['key']: r for r in after['entries']}
                self.assertEqual(actual[source], original[source])
                self.assertEqual(actual[destination]['payload'], original[source]['payload'])
                self.assertEqual(actual[destination]['tags'], original[source]['tags'])
                self.assertEqual(actual[destination]['properties'], {**original[source]['properties'], 'ETag': '"copied"'})

    def scene(self, branch):
        before = physical_fixtures.HealShardOracleTest().snapshot()
        for value in before.values():
            original = value['files']
            value['files'] = []
            for meta, part in zip(original[::3], original[1::3], strict=True):
                for key, generation, role in [(race.G06_TARGET_KEY, 100, 'selected'),
                                              (race.G06_REFERENCE_KEY, 200, 'other')]:
                    m, p = copy.deepcopy(meta), copy.deepcopy(part)
                    m['path'] = f'{self.bucket}/{key}/xl.meta'
                    m['metadata']['key'] = key
                    m['metadata']['versions'][0]['dataDir'] = str(uuid.UUID(int=generation))
                    p['path'] = f'{self.bucket}/{key}/{uuid.UUID(int=generation)}/part.1'
                    p['sha256'] += '-' + role
                    value['files'] += [m, p]
        expected = self.baseline()['entries']
        mapped = race.map_fixture(before, self.bucket, expected)
        source, destination = ((race.G06_TARGET_KEY, race.G06_REFERENCE_KEY) if branch == 'copy-source'
                               else (race.G06_REFERENCE_KEY, race.G06_TARGET_KEY))
        after = copy.deepcopy(before)
        for value in after.values():
            by_path = {(f['disk'], f['path']): f for f in value['files']}
            for f in value['files']:
                if not f['path'].startswith(f'{self.bucket}/{destination}/'): continue
                if f['path'].endswith('/xl.meta'):
                    f['metadata']['versions'][0]['dataDir'] = str(uuid.UUID(int=300))
                else:
                    slot = 100 if source == race.G06_TARGET_KEY else 200
                    f['sha256'] = by_path[f['disk'], f'{self.bucket}/{source}/{uuid.UUID(int=slot)}/part.1']['sha256']
                    f['path'] = f'{self.bucket}/{destination}/{uuid.UUID(int=300)}/part.1'
        return before, after, mapped, {'entries': expected}, destination

    def test_real_copy_transition_accepts_both_directions_before_get(self):
        for branch in ('copy-source', 'copy-destination'):
            with self.subTest(branch=branch):
                before, after, mapped, final, _ = self.scene(branch)
                _, proof = race.physical_transition(before, after, self.bucket, mapped, final, branch)
                self.assertTrue(proof['physicalProofBeforeGETOrRescue'])

    def test_copy_rejects_wrong_source_shards_and_resurrected_destination_generation(self):
        for branch in ('copy-source', 'copy-destination'):
            for fault in ('wrong-shard', 'old-generation', 'source-shard'):
                with self.subTest(branch=branch, fault=fault):
                    before, after, mapped, final, destination = self.scene(branch)
                    part = next(f for f in after['node1']['files'] if f['path'].startswith(f'{self.bucket}/{destination}/')
                                and f['path'].endswith('/part.1'))
                    if fault == 'wrong-shard': part['sha256'] = 'wrong-copy'
                    elif fault == 'old-generation':
                        old = next(f for f in before['node1']['files'] if f['path'].startswith(f'{self.bucket}/{destination}/')
                                   and f['path'].endswith('/part.1'))
                        after['node1']['files'].append(copy.deepcopy(old))
                    else:
                        source = race.G06_TARGET_KEY if branch == 'copy-source' else race.G06_REFERENCE_KEY
                        original = next(f for f in after['node1']['files'] if f['path'].startswith(f'{self.bucket}/{source}/')
                                        and f['path'].endswith('/part.1'))
                        original['sha256'] = 'changed-copy-source'
                    with self.assertRaises(ValueError):
                        race.physical_transition(before, after, self.bucket, mapped, final, branch)

    def metadata_scene(self):
        before, after, mapped, final, _ = self.scene('copy-destination')
        original = {(f['disk'], f['path']): f for f in before['node1']['files']}
        for f in after['node1']['files']:
            if f['path'].startswith(f'{self.bucket}/{race.G06_TARGET_KEY}/') and f['path'].endswith('/part.1'):
                old_path = f'{self.bucket}/{race.G06_TARGET_KEY}/{uuid.UUID(int=100)}/part.1'
                f['sha256'] = original[f['disk'], old_path]['sha256']
        return before, after, mapped, final

    def test_metadata_and_tags_allow_one_current_directory_with_original_coding_bytes(self):
        # The frozen generation_proof requires replacement only for COPY and
        # overwrite. Metadata/tag mutations still require the complete coding set.
        for branch in ('replace-metadata', 'update-tags'):
            for replace in (False, True):
                with self.subTest(branch=branch, replace=replace):
                    before, after, mapped, final = self.metadata_scene()
                    if not replace: after = copy.deepcopy(before)
                    _, proof = race.physical_transition(before, after, self.bucket, mapped, final, branch)
                    self.assertTrue(proof['physicalProofBeforeGETOrRescue'])

    def test_metadata_and_tags_reject_incomplete_mixed_or_wrong_coding_generations(self):
        for branch in ('replace-metadata', 'update-tags'):
            for fault in ('wrong-shard', 'wrong-index', 'missing-part', 'mixed-generation', 'old-leaf', 'control'):
                with self.subTest(branch=branch, fault=fault):
                    before, after, mapped, final = self.metadata_scene()
                    rows = after['node1']['files']
                    parts = [f for f in rows if f['path'].startswith(f'{self.bucket}/{race.G06_TARGET_KEY}/')
                             and f['path'].endswith('/part.1')]
                    if fault == 'wrong-shard': parts[0]['sha256'] = 'wrong'
                    elif fault == 'wrong-index':
                        parts[0]['sha256'], parts[1]['sha256'] = parts[1]['sha256'], parts[0]['sha256']
                    elif fault == 'missing-part': rows.remove(parts[0])
                    elif fault == 'mixed-generation':
                        meta = next(f for f in rows if f['disk'] == 0
                                    and f['path'] == f'{self.bucket}/{race.G06_TARGET_KEY}/xl.meta')
                        meta['metadata']['versions'][0]['dataDir'] = str(uuid.UUID(int=100))
                    elif fault == 'old-leaf':
                        rows.append(copy.deepcopy(next(f for f in before['node1']['files']
                            if f['path'].startswith(f'{self.bucket}/{race.G06_TARGET_KEY}/')
                            and f['path'].endswith('/part.1'))))
                    else:
                        next(f for f in rows if f['path'].startswith(f'{self.bucket}/{race.G06_REFERENCE_KEY}/')
                             and f['path'].endswith('/part.1'))['sha256'] = 'changed-control'
                    with self.assertRaises(ValueError):
                        race.physical_transition(before, after, self.bucket, mapped, final, branch)


if __name__ == '__main__':
    unittest.main()
