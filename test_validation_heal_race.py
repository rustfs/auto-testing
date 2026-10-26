import copy
import hashlib
import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
import tempfile
import threading

from scripts.validation_heal import race
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
from scripts.validation_heal.heal_workload import RecordedCallError


class UnversionedReaderTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def model(self):
        body = race.payload('original', 31)
        baseline = {'entries': [{'key': race.TARGET_KEY, 'versionId': 'null', 'isLatest': True,
            'deleteMarker': False, 'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()},
            'properties': {'ETag': '"original"', 'ContentLength': len(body),
                'ContentType': 'application/octet-stream', 'Metadata': {'nonce': 'original'}}}]}
        plan = race.operation_plan(self.bucket, 'overwrite-vs-read-repair', baseline['entries'])
        for step in plan: step['branch'] = 'overwrite-vs-read-repair'
        responses = [{'ETag': f'"new-{index}"'} for index in range(3)]
        return baseline, plan, responses

    def ack(self, entry):
        return {'operationCheck': 'PASS', 'observation': {'operation': 'get_object',
            'params': {'Bucket': self.bucket, 'Key': race.TARGET_KEY}, 'acknowledged': True,
            'resultUnknown': False, 'payload': copy.deepcopy(entry['payload']),
            'response': {**copy.deepcopy(entry['properties']), 'ResponseMetadata': {'HTTPStatusCode': 200}}}}

    def test_reads_accept_only_complete_original_or_ack_known_generations(self):
        baseline, plan, responses = self.model()
        reads = [self.ack(baseline['entries'][0])]
        for step, response in zip(plan, responses, strict=True):
            body = race.payload(step['nonce'], 31)
            reads.append(self.ack({'payload': {'size': len(body), 'sha256': hashlib.sha256(body).hexdigest()},
                'properties': {'ETag': response['ETag'], 'ContentLength': len(body),
                    'ContentType': step['params']['ContentType'], 'Metadata': step['params']['Metadata']}}))
        with patch.object(race, 'SIZE', 31):
            self.assertTrue(race.verify_unversioned_reads(reads, baseline, plan, responses))
            self.assertFalse(race.verify_unversioned_reads([], baseline, plan, responses))
            for mutation in ('body', 'etag', 'metadata', 'length', 'version', 'unknown', 'foreign', 'refused'):
                with self.subTest(mutation=mutation):
                    changed = copy.deepcopy(reads); observed = changed[0]['observation']
                    if mutation == 'body': observed['payload']['sha256'] = '0' * 64
                    elif mutation == 'etag': observed['response']['ETag'] = responses[1]['ETag']
                    elif mutation == 'metadata': observed['response']['Metadata'] = plan[1]['params']['Metadata']
                    elif mutation == 'length': observed['response']['ContentLength'] += 1
                    elif mutation == 'version': observed['response']['VersionId'] = 'foreign-version'
                    elif mutation == 'unknown': observed['resultUnknown'] = True
                    elif mutation == 'foreign': observed['params']['Bucket'] += '-other'
                    else: changed[0]['operationCheck'] = 'FAIL'
                    with self.assertRaises(ValueError): race.verify_unversioned_reads(changed, baseline, plan, responses)

    def test_real_recorder_retains_read_requests_and_acks_without_retrying_unknowns(self):
        for unknown in (False, True):
            with self.subTest(unknown=unknown), tempfile.TemporaryDirectory() as directory:
                baseline, _, _ = self.model(); done = threading.Event(); calls = []
                def s3(operation, params, body):
                    calls.append((operation, params)); done.set()
                    observed = self.ack(baseline['entries'][0])['observation']
                    if unknown: observed.update(acknowledged=False, resultUnknown=True, response=None)
                    return observed
                probe = SimpleNamespace(binding={'runId': 'gh-12345-1-h1', 'bucketPrefix': self.bucket,
                    'maxInventoryBytes': 512 * 1024**2}, s3=s3)
                recorder = race.Recorder(probe, Path(directory) / 'read-repair-ACKs')
                params = {'Bucket': self.bucket, 'Key': race.TARGET_KEY}
                if unknown:
                    with self.assertRaises(RecordedCallError): race.read_unversioned(recorder, params, done)
                else:
                    self.assertEqual(len(race.read_unversioned(recorder, params, done)), 1)
                self.assertEqual(calls, [('get_object', params)])
                request = recorder.output / '000001-request.json'; ack = json.loads((recorder.output / '000001-ack.json').read_text())
                self.assertEqual(ack['requestSha256'], hashlib.sha256(request.read_bytes()).hexdigest())
                self.assertEqual(ack['operationCheck'], 'FAIL' if unknown else 'PASS')

    def test_controller_requires_a_successful_reader_and_its_own_original_token_window(self):
        for present, reader_window, writer_window in ((True, True, True), (False, True, True),
                                                     (True, False, True), (True, True, False)):
            with self.subTest(present=present, reader_window=reader_window, writer_window=writer_window), tempfile.TemporaryDirectory() as directory:
                baseline, plan, responses = self.model()
                output = Path(directory) / 'foreground-ACKs'; output.mkdir()
                writer = SimpleNamespace(output=output, mutations=SimpleNamespace(poison=Mock()))
                ready, sampled = threading.Event(), threading.Event(); records = {}
                def foreground(*args, **kwargs):
                    for index in range(3):
                        (output / f'{index+1:06d}-ack.json').write_text(json.dumps({'operationCheck': 'PASS',
                            'sequence': index + 1, 'observation': {'operation': 'put_object'}}))
                    self.assertTrue(ready.wait(2)); self.assertTrue(sampled.wait(2))
                    return responses, {}
                def read(*args):
                    ready.set(); self.assertTrue(sampled.wait(2))
                    return [self.ack(baseline['entries'][0])] if present else []
                def observe(*args): sampled.set(); return {'originalToken': True}
                def overlap(ack, *args):
                    return writer_window and ack.get('sequence') == 2 if ack['observation']['operation'] == 'put_object' else reader_window
                probe = SimpleNamespace(binding={'runId': 'gh-12345-1-h1', 'bucketPrefix': self.bucket})
                fleet = SimpleNamespace(record=lambda label, value: records.setdefault(label, value))
                with patch.object(race, 'SIZE', 31), patch.object(race, 'foreground', side_effect=foreground), \
                     patch.object(race, 'read_unversioned', side_effect=read), patch.object(race, 'observe', side_effect=observe), \
                     patch.object(race, 'sample_overlap', side_effect=overlap):
                    _, accepted = race.race(writer, plan, probe, fleet, '/original-path', 'original-token', baseline=baseline)
                self.assertEqual(accepted, present and reader_window and writer_window)
                self.assertEqual(records['G01-read-repair-coverage']['complete'], present)
                self.assertEqual(records['public-call-overlap-proof']['requiredCallOverlapObserved'],
                                 present and reader_window and writer_window)
                self.assertFalse(records['public-call-overlap-proof']['everyMutationOverlapped'])

    def test_missing_baseline_or_existing_reader_journal_refuses_before_any_put(self):
        for stale in (False, True):
            with self.subTest(stale=stale), tempfile.TemporaryDirectory() as directory:
                baseline, plan, _ = self.model(); output = Path(directory) / 'foreground-ACKs'; output.mkdir()
                if stale: (output.parent / 'read-repair-ACKs').mkdir()
                writer = SimpleNamespace(output=output, mutations=SimpleNamespace(poison=Mock()))
                probe = SimpleNamespace(binding={'runId': 'gh-12345-1-h1', 'bucketPrefix': self.bucket})
                with patch.object(race, 'foreground') as foreground, self.assertRaises((ValueError, FileExistsError)):
                    race.race(writer, plan, probe, SimpleNamespace(), '/original-path', 'original-token',
                              baseline=baseline if stale else None)
                foreground.assert_not_called()

    def test_status_observation_continues_until_the_inflight_reader_drains(self):
        with tempfile.TemporaryDirectory() as directory:
            baseline, plan, responses = self.model(); output = Path(directory) / 'foreground-ACKs'; output.mkdir()
            writer = SimpleNamespace(output=output, mutations=SimpleNamespace(poison=Mock()))
            ready, observed_after_drain = threading.Event(), threading.Event(); shared = {}; samples = []
            def foreground(*args, **kwargs):
                for index in range(3):
                    (output / f'{index+1:06d}-ack.json').write_text(json.dumps({'operationCheck': 'PASS',
                        'sequence': index + 1, 'observation': {'operation': 'put_object'}}))
                self.assertTrue(ready.wait(2)); return responses, {}
            def read(recorder, params, done):
                shared['done'] = done; ready.set()
                self.assertTrue(done.wait(2)); self.assertTrue(observed_after_drain.wait(2))
                return [self.ack(baseline['entries'][0])]
            def observe(*args):
                after = shared.get('done') is not None and shared['done'].is_set()
                samples.append(after)
                if samples.count(True) >= 2: observed_after_drain.set()
                return {'afterWriterDrained': after}
            def overlap(ack, sample, *args):
                return ack.get('sequence') == 2 if ack['observation']['operation'] == 'put_object' else sample['afterWriterDrained']
            probe = SimpleNamespace(binding={'runId': 'gh-12345-1-h1', 'bucketPrefix': self.bucket})
            fleet = SimpleNamespace(record=lambda label, value: value)
            with patch.object(race, 'SIZE', 31), patch.object(race, 'foreground', side_effect=foreground), \
                 patch.object(race, 'read_unversioned', side_effect=read), patch.object(race, 'observe', side_effect=observe), \
                 patch.object(race, 'sample_overlap', side_effect=overlap):
                _, accepted = race.race(writer, plan, probe, fleet, '/original-path', 'original-token', baseline=baseline)
            self.assertTrue(accepted)
            self.assertGreaterEqual(samples.count(True), 2)


class RaceTests(unittest.TestCase):
    def test_source_case_branch_and_topology_contracts(self):
        for case_id, branches in race.CASE_BRANCHES.items():
            recipe = select(case_id)[0]
            self.assertEqual(recipe["variants"], list(branches))
            self.assertIn(case_id, AUTOMATIC_CASES)
            self.assertEqual(recipe["contract"]["probes"]["topology"], "H1,H2")

    def test_g01_plan_keeps_three_ack_ordered_overwrites(self):
        expected = [{"key": race.TARGET_KEY, "versionId": "null", "isLatest": True}]
        plan = race.operation_plan("rv-heal-0123456789abcdef", "overwrite-vs-read-repair", expected)
        self.assertEqual([step["params"]["Metadata"]["nonce"] for step in plan], ["0", "1", "2"])
        self.assertEqual(plan[-1]["nonce"], "rv-heal-0123456789abcdef/objects/0000.bin/0")

    def test_g03_selects_exact_old_or_current_version(self):
        expected = [{"key": race.TARGET_KEY, "versionId": value, "isLatest": value == "v2"}
                    for value in ("v0", "v1", "v2")]
        old = race.operation_plan("rv-heal-0123456789abcdef", "delete-old-version-vs-deep", expected)
        current = race.operation_plan("rv-heal-0123456789abcdef", "delete-current-version-vs-deep", expected)
        self.assertEqual(old[0]["params"]["VersionId"], "v0")
        self.assertEqual(current[0]["params"]["VersionId"], "v2")

    def test_g02_plan_creates_three_acknowledged_versions(self):
        expected = [{"key": race.TARGET_KEY, "versionId": value, "isLatest": value == "v2"}
                    for value in ("v0", "v1", "v2")]
        plan = race.operation_plan("rv-heal-0123456789abcdef", "read-all-versions-during-heal", expected)
        self.assertEqual(len(plan), 3)
        self.assertEqual([step['referenceKey'] for step in plan], [f'objects/{i:04d}.bin' for i in range(3)])
        self.assertTrue(all('readVersionId' not in step for step in plan))
        self.assertEqual([step["params"]["Metadata"]["nonce"] for step in plan], ["0", "1", "2"])

    def test_g06_keeps_copy_metadata_and_tag_operations_distinct(self):
        expected = [{"key": race.TARGET_KEY, "versionId": "null", "isLatest": True},
                    {"key": race.REFERENCE_KEY, "versionId": "null", "isLatest": True}]
        self.assertEqual(race.operation_plan("rv-heal-0123456789abcdef", "copy-source", expected)[0]["operation"], "copy_object")
        self.assertEqual(race.operation_plan("rv-heal-0123456789abcdef", "replace-metadata", expected)[0]["operation"], "copy_object")
        tags = race.operation_plan("rv-heal-0123456789abcdef", "update-tags", expected)
        self.assertEqual(len(tags), 16)
        self.assertEqual(tags[-1]["operation"], "put_object_tagging")
        self.assertEqual(race.target_key("copy-source"), race.G06_TARGET_KEY)
        self.assertEqual(race.reference_key("copy-source"), race.G06_REFERENCE_KEY)

    def test_g05_cannot_use_the_superseded_unversioned_fixture_or_generic_plan(self):
        recorder = Mock()
        for branch in race.G05_BRANCHES:
            with self.subTest(branch=branch), self.assertRaises(ValueError):
                race.seed_fixture(recorder, 'rv-heal-0123456789abcdef', branch)
            with self.subTest(branch=branch), self.assertRaises(ValueError):
                race.operation_plan('rv-heal-0123456789abcdef', branch, [])
        recorder.call.assert_not_called()
        for operation in ('create_multipart_upload', 'upload_part', 'complete_multipart_upload', 'abort_multipart_upload'):
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                race.foreground(recorder, [{'operation': operation}])
        recorder.call.assert_not_called()

    def test_g05_controller_delegates_all_public_branches_and_refuses_old_source_before_bootstrap(self):
        case = race.CASE_IDS[4]
        with tempfile.TemporaryDirectory() as d:
            candidate = Path(d) / 'candidate.json'
            args = SimpleNamespace(case_id=case, candidate=candidate)
            with patch('scripts.validation_heal.mpu_race_runner.run_branch', return_value={'owned': True}) as execute, \
                 patch.object(race, 'Fleet', side_effect=AssertionError('generic fleet must not run')):
                candidate.write_text(json.dumps({'source_sha': race.REFERENCE['candidateCommit']}))
                with self.assertRaisesRegex(ValueError, 'exact-source multipart or marker'):
                    race.run_branch(args, 'H1', ['vm000'], 'upload-continue')
                execute.assert_not_called()
                candidate.write_text(json.dumps({'source_sha': race.COPY_CANDIDATE}))
                for branch in race.G05_BRANCHES:
                    self.assertEqual(race.run_branch(args, 'H1', ['vm000'], branch), {'owned': True})
                    execute.assert_called_with(args, 'H1', ['vm000'], branch)

    def test_g04_uses_complete_frozen_marker_controller_and_rejects_legacy_before_bootstrap(self):
        with tempfile.TemporaryDirectory() as d:
            candidate=Path(d)/'candidate.json';args=SimpleNamespace(case_id=race.CASE_IDS[3],candidate=candidate)
            with patch('scripts.validation_heal.version_race_runner.run_branch',return_value={'owned':True}) as execute, \
                 patch.object(race,'Fleet',side_effect=AssertionError('generic fleet must not run')):
                candidate.write_text(json.dumps({'source_sha':race.REFERENCE['candidateCommit']}))
                with self.assertRaisesRegex(ValueError,'exact-source'):race.run_branch(args,'H1',['vm000'],'create-marker')
                execute.assert_not_called()
                candidate.write_text(json.dumps({'source_sha':race.MARKER_CANDIDATE}))
                for branch in race.G04_BRANCHES:
                    self.assertEqual(race.run_branch(args,'H1',['vm000'],branch),{'owned':True})
                    execute.assert_called_with(args,'H1',['vm000'],branch)

    def test_native_branch_remains_in_denominator_for_each_case(self):
        for case_id, branches in race.CASE_BRANCHES.items():
            native = race.native_blocked({"source_sha": race.REFERENCE["candidateCommit"]}, "H1", case_id)
            rows = [{"topology": topology, "branch": branch, "verdict": "PASS", "cleanup": "PASS"}
                    for topology in ("H1", "H2") for branch in branches[:2]]
            self.assertFalse(race.complete_results([*rows, native], case_id))
            self.assertEqual(native["branch"], branches[-1])

    def test_dispatch_and_source_contract_are_present(self):
        dispatch = Path("scripts/validation_heal/run_case.sh").read_text()
        top_dispatch = Path("scripts/run_validation_case.sh").read_text()
        for case_id, code in (("heal.race-unversioned-overwrite", "G01"), ("heal.race-versioned-put", "G02"),
                              ("heal.race-version-delete", "G03"), ("heal.race-markers-and-batch-delete", "G04"),
                              ("heal.race-multipart-complete-abort", "G05"),
                              ("heal.race-copy-and-metadata-update", "G06")):
            self.assertIn(case_id, dispatch)
            self.assertIn(case_id, top_dispatch)
            catalog = json.loads(Path("validation-cases.json").read_text())
            source = {case["id"]: case for module in catalog["modules"] for case in module["cases"]}[case_id]
            self.assertEqual(source["probes"]["designCode"], code)


if __name__ == "__main__":
    unittest.main()
