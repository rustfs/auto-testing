"""Own I03 DL intents and reconcile public effects before any namespace cleanup."""

import copy
import hashlib
import json
from pathlib import Path
import threading

try:
    from .heal_probe import Probe, compare_inventories, require, save
    from .heal_workload import Recorder, verify_seed
    from .network_load import (LoadJournal, MUTATIONS, binding_digest, concurrent_load, read_journal,
                               read_receipts, seed_load, source_reference, validate_planned_request)
    from .mpu_scene import keys_for
    from .shards import files, validate_repaired_physical
except ImportError:
    from heal_probe import Probe, compare_inventories, require, save
    from heal_workload import Recorder, verify_seed
    from network_load import (LoadJournal, MUTATIONS, binding_digest, concurrent_load, read_journal,
                              read_receipts, seed_load, source_reference, validate_planned_request)
    from mpu_scene import keys_for
    from shards import files, validate_repaired_physical


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()).hexdigest()


class LoadSession:
    def __init__(self, binding, bucket, endpoints, directory, *, probe_factory=Probe):
        self.binding, self.bucket = copy.deepcopy(binding), bucket
        self.endpoints, self.keys = tuple(endpoints), keys_for(endpoints)
        require(bucket == binding['bucketPrefix'] and set(endpoints) <= set(binding['endpoints'])
                and len(binding['endpoints']) == 4, 'DL session requires its original four-peer primary bucket binding')
        self.directory, self.probe_factory = Path(directory), probe_factory
        self.directory.mkdir(parents=True, exist_ok=False)
        self.serial = threading.Lock()
        self.fixture, self.baseline, self.load_result = None, None, None
        self.safe_to_cleanup, self.pre_read, self.capture_count, self.attempt = False, None, 0, 0
        self.phase, self.post_mpu, self.business_baseline = 'REGISTERED', None, None
        # The intent names every recorder before the first SDK call, including
        # a seed that fails before it can return a complete fixture.
        save(self.directory / 'intent.json', {'runId': binding['runId'], 'bucket': bucket,
            'bindingSha256': binding_digest(binding), 'sourceContract': source_reference(binding['candidateCommit']),
            'keys': self.keys, 'seedJournal': 'seed', 'workers': [
                {'endpoint': endpoint, 'role': role, 'directory': endpoint + '-' + role}
                for endpoint in self.endpoints for role in ('reader', 'writer')], 'cleanupAllowed': False})
        self.seed_recorder = Recorder(probe_factory(self.binding), self.directory / 'seed')
        self.journals = {(endpoint, role): LoadJournal(probe_factory(self.binding, endpoint),
            self.directory / (endpoint + '-' + role), role) for endpoint in self.endpoints for role in ('reader', 'writer')}

    def seed(self):
        with self.serial:
            require(self.phase == 'REGISTERED' and self.seed_recorder.sequence == 0, 'DL seed cannot be retried or replaced')
            self.phase = 'SEED_STARTED'
            try:
                self.fixture = seed_load(self.seed_recorder, self.bucket, self.endpoints)
            except BaseException:
                self.phase = 'SEED_FAILED'
                raise
            save(self.directory / 'fixture.json', self.fixture)
            self.fixture_sha = json_hash(self.fixture)
            self.phase = 'SEEDED'
            return copy.deepcopy(self.fixture)

    def register_baseline(self, inventory):
        with self.serial:
            require(self.phase == 'SEEDED' and self.baseline is None and inventory['bucket'] == self.bucket,
                    'DL baseline lacks its original seeded bucket or was already frozen')
            subset = {**inventory, 'entries': [row for row in inventory['entries'] if row['key'].startswith('dl/')]}
            verify_seed(self.fixture['expected'], subset)
            self.baseline = copy.deepcopy(inventory)
            save(self.directory / 'original-baseline.json', self.baseline)
            self.baseline_sha = json_hash(self.baseline)
            self.phase = 'BASELINED'

    def _stopped_business_scope(self, snapshot):
        require(len(snapshot) == 4 and all(value.get('quiescent') is True
                and type(value.get('pid')) is int and value['pid'] == 0
                and value.get('binarySha256') == self.binding['candidateBinarySha256']
                and value.get('unit') == 'rustfs-validation-heal-' + self.binding['runId'] + '.service'
                and len(value['formats']) == 4 for value in snapshot.values()),
                'DL business scene lacks the stopped original sixteen-drive candidate')
        require(all(entry['path'].split('/')[0] in {self.bucket, self.bucket + '-db'}
                    for entry in files(snapshot).values()), 'DL business scene escaped its primary/DB buckets')

    def register_business_baseline(self, snapshot):
        with self.serial:
            require(self.phase == 'BASELINED' and self.business_baseline is None,
                    'DL original business scene must be frozen before any load')
            self._stopped_business_scope(snapshot)
            self.business_baseline = copy.deepcopy(snapshot)
            self.business_baseline_sha = json_hash(snapshot)
            save(self.directory / 'original-business-physical.json', snapshot)

    def run_load(self, readable):
        with self.serial:
            require(self.phase == 'BASELINED' and self.business_baseline is not None
                    and not (self.directory / 'load-started.json').exists(), 'DL requires one original seeded/baselined load')
            save(self.directory / 'load-started.json', {'readable': readable, 'bindingSha256': binding_digest(self.binding)})
            self.phase = 'LOAD_STARTED'
            try:
                self.load_result = concurrent_load(self.journals, self.fixture, readable)
            finally:
                self.phase = 'LOAD_FINISHED'
            save(self.directory / 'load-result.json', self.load_result)
            return self.load_result

    def registered_receipts(self):
        return {'seed': read_receipts(self.seed_recorder.output, self.binding, self.seed_recorder.probe.endpoint_name),
            'workers': [{'endpoint': endpoint, 'role': role,
                'rows': read_journal(journal.output, self.binding, endpoint, role)}
                for (endpoint, role), journal in self.journals.items()]}

    def _validate_seed(self, rows):
        require(len(rows) == 27, 'DL complete seed omits or adds an original request')
        versions = {(r['key'], r['versionId']): r for r in self.fixture['expected']}
        uploads = {u['key']: u for side in self.fixture['sides'].values() for u in side['uploads'].values()}
        seen, counts = set(), {}
        for row in rows:
            require(row['ackPresent'], 'DL complete seed has a prepared request without its ACK')
            req, obs = row['request'], row['ack']['observation']
            op, params, response = req['operation'], req['params'], obs.get('response') or {}
            require(params['Bucket'] == self.bucket and obs.get('clientCallInvoked') is True
                    and obs.get('acknowledged') is True and obs.get('resultUnknown') is False
                    and row['ack']['operationCheck'] == 'PASS', 'DL seed ACK is missing, foreign or uncertain')
            counts[op] = counts.get(op, 0) + 1
            if op in {'put_object', 'head_object'}:
                pair = params['Key'], response.get('VersionId') if op == 'put_object' else params['VersionId']
                require(pair in versions and (op, *pair) not in seen, 'DL seed omitted or duplicated a control version ACK')
                seen.add((op, *pair))
                if op == 'put_object': require(req.get('payload') == versions[pair]['payload'], 'DL original seed body changed')
            elif op == 'create_multipart_upload':
                require(params['Key'] in uploads and response.get('UploadId') == uploads[params['Key']]['uploadId']
                        and (op, params['Key']) not in seen, 'DL seed Create ACK belongs to another upload')
                seen.add((op, params['Key']))
            elif op == 'upload_part':
                upload = uploads.get(params['Key'])
                part = next((p for p in upload['parts'] if p['PartNumber'] == params['PartNumber']), None) if upload else None
                require(part is not None and params['UploadId'] == upload['uploadId']
                        and req.get('payload') == part['payload'] and response.get('ETag') == part['ETag']
                        and (op, params['Key'], params['PartNumber']) not in seen, 'DL seed part ACK changed its upload/number/body')
                seen.add((op, params['Key'], params['PartNumber']))
            else:
                require(op == 'get_bucket_versioning' and params == {'Bucket': self.bucket}
                        and response.get('Status') == 'Enabled', 'DL seed operation escaped its original plan')
        require(counts == {'get_bucket_versioning': 1, 'put_object': 6, 'head_object': 6,
                           'create_multipart_upload': 6, 'upload_part': 8}, 'DL seed request matrix differs')

    def capture_mpu(self, fleet, stage):
        with self.serial:
            require(stage in {'baseline', 'failure', 'pre-reconcile', 'post-reconcile'} and len(fleet.nodes) == len(set(fleet.nodes)) == 4
                    and (stage != 'post-reconcile' or self.phase == 'PUBLIC_RECONCILED'),
                    'DL MPU capture requires the complete original stopped fleet and a named phase')
            scenes = {node: fleet.remote(node, 'mpu-scene', bucket=self.bucket, keys=self.keys,
                expectedBinarySha256=self.binding['candidateBinarySha256']) for node in fleet.nodes}
            for node, scene in scenes.items():
                require(scene.get('quiescent') is True and scene.get('readOnly') is True
                        and scene.get('runId') == self.binding['runId'] and scene.get('bucket') == self.bucket
                        and scene.get('binarySha256') == self.binding['candidateBinarySha256']
                        and scene.get('keys') == self.keys and scene.get('originalPhysicalDrives') == 4
                        and scene.get('payloadIntegrityClaimed') is False,
                        'DL MPU scene is not the original stopped candidate/key scope')
                value = {name: scene[name] for name in ('bucket', 'keys', 'scopes', 'directories', 'files', 'bytesHashed')}
                require(json_hash(value) == scene['sceneSha256'] and len(scene['scopes']) == 32
                        and {(row['disk'], row['key']) for row in scene['scopes']}
                        == {(disk, key) for disk in range(4) for key in self.keys}, 'DL MPU census is incomplete or changed')
                key_hashes = {key: hashlib.sha256((self.bucket + '/' + key).encode()).hexdigest() for key in self.keys}
                require(all(row['keySha256'] == key_hashes[row['key']] and type(row['present']) is bool
                            for row in scene['scopes']), 'DL MPU census hashes refer to another bucket/key')
                for kind in ('files', 'directories'):
                    rows = scene[kind]
                    require(len({(row['disk'], row['path']) for row in rows}) == len(rows)
                            and all(type(row['disk']) is int and 0 <= row['disk'] < 4 and row['key'] in key_hashes
                                and '..' not in Path(row['path']).parts
                                and (row['path'] == '.rustfs.sys/multipart/' + key_hashes[row['key']]
                                     or row['path'].startswith('.rustfs.sys/multipart/' + key_hashes[row['key']] + '/'))
                                for row in rows), 'DL MPU file/directory escaped its original exact key scope')
            self.capture_count += 1
            saved = {'stage': stage, 'bindingSha256': binding_digest(self.binding), 'nodes': scenes}
            save(self.directory / f'mpu-{self.capture_count:02d}-{stage}.json', saved)
            fleet.record('DL-stopped-MPU-' + stage, saved)
            if stage == 'pre-reconcile': self.pre_read = json_hash(saved)
            if stage == 'post-reconcile': self.post_mpu = saved
            return saved

    def _call(self, recorder, operation, params, expected):
        recorder.call(operation, params, expect_http=expected)
        row = read_receipts(recorder.output, self.binding, recorder.probe.endpoint_name)[-1]
        observed = row['ack']['observation']
        require(observed.get('clientCallInvoked') is True and observed.get('resultUnknown') is False
                and row['ack']['operationCheck'] == 'PASS' and not observed.get('clientCleanupError'),
                'DL reconciliation call has no exact known invoked response')
        return observed

    def _uploads(self, recorders, label, folder):
        result = {}
        for endpoint, recorder in recorders.items():
            observed = self._call(recorder, 'list_multipart_uploads', {
                'Bucket': self.bucket, 'Prefix': 'dl/', 'MaxUploads': 32}, 200)
            response, uploads = observed['response'], []
            require(response.get('IsTruncated') is False, 'DL pending upload listing is incomplete')
            for row in response.get('Uploads', []):
                require(row.get('Key') in self.keys and isinstance(row.get('UploadId'), str) and row['UploadId'],
                        'DL pending upload escaped an original key or omitted its opaque ID')
                uploads.append((row['Key'], row['UploadId']))
            require(len(uploads) <= 32 and len(set(uploads)) == len(uploads), 'DL pending upload list duplicates/exceeds its cap')
            result[endpoint] = sorted(uploads)
        save(folder / (label + '.json'), result)
        return result

    def _inventories(self, folder, label):
        observed = {}
        # Inventory itself verifies full version GET payloads, tags and current
        # LIST consistency. Save each peer before checking equality.
        for endpoint in self.binding['endpoints']:
            value = self.probe_factory(self.binding, endpoint).inventory(self.bucket)
            save(folder / (endpoint + '-' + label + '.json'), value)
            observed[endpoint] = value
        for value in observed.values(): compare_inventories(self.baseline, value)
        return observed

    def reconcile(self):
        """Retire pending uploads with distinct recovery ACKs, never retry PUT/Complete."""
        with self.serial:
            self.safe_to_cleanup = False
            require(self.fixture is not None and self.baseline is not None and self.pre_read is not None
                    and self.phase in {'BASELINED', 'LOAD_FINISHED', 'RECONCILE_FAILED'}
                    and self.attempt < 2 and json_hash(self.fixture) == self.fixture_sha
                    and json_hash(self.baseline) == self.baseline_sha,
                    'DL reconciliation lacks a complete frozen seed/baseline or original stopped MPU scene')
            self.attempt += 1
            self.phase = 'RECONCILING'
            folder = self.directory / f'reconcile-{self.attempt:02d}'
            folder.mkdir()
            original = self.registered_receipts()
            self.original_receipts_sha = json_hash(original)
            save(folder / 'original-request-states.json', original)
            recorders = {endpoint: Recorder(self.probe_factory(self.binding, endpoint), folder / endpoint)
                         for endpoint in self.binding['endpoints']}
            try:
                self._validate_seed(original['seed'])
                for worker in original['workers']:
                    if worker['role'] == 'writer':
                        for row in worker['rows']:
                            validate_planned_request(row['request'], self.fixture, worker['endpoint'], readable=True)
                self._inventories(folder, 'before-retirement')
                writers = [row for worker in original['workers'] if worker['role'] == 'writer' for row in worker['rows']]
                require(all(not (row['ackPresent'] and row['ack']['observation'].get('acknowledged') is True) for row in writers),
                        'DL unexpected positive mutation ACK must be independently redeemed; retain the original failure')
                known = {(upload['key'], upload['uploadId']) for side in self.fixture['sides'].values() for upload in side['uploads'].values()}
                discovery_keys = {row['request']['params']['Key'] for row in writers if row['request']['operation'] == 'create_multipart_upload'}
                pending = self._uploads(recorders, 'pending-before', folder)
                discovered = {pair for rows in pending.values() for pair in rows} - known
                require(all(key in discovery_keys for key, identity in discovered)
                        and len({key for key, identity in discovered}) == len(discovered),
                        'DL observed an unbound or duplicate unknown CreateMultipartUpload effect')
                registered = sorted(known | discovered)
                recovery = recorders[self.binding['defaultEndpoint']]
                retirements = []
                for key, upload_id in registered:
                    params = {'Bucket': self.bucket, 'Key': key, 'UploadId': upload_id}
                    # A refused abort may have partially removed upload/parts.
                    # These observations precede a distinct, fully restored
                    # cluster Abort; neither is relabeled as the original ACK.
                    for recorder in recorders.values():
                        part = self._call(recorder, 'list_parts', {**params, 'MaxParts': 32}, (200, 404, 503))
                        status = part['response']['ResponseMetadata']['HTTPStatusCode']
                        require(status == 200 and part['response'].get('IsTruncated') is False
                                and part['response'].get('Key') == key and part['response'].get('UploadId') == upload_id
                                or status == 404 and part['response'].get('Error', {}).get('Code') == 'NoSuchUpload'
                                or status == 503 and part['response'].get('Error', {}).get('Code') in {'ServiceUnavailable', 'SlowDownRead'},
                                'DL pre-retirement parts observation is foreign or unrecognized')
                    response = self._call(recovery, 'abort_multipart_upload', params, (204, 404))
                    status = response['response']['ResponseMetadata']['HTTPStatusCode']
                    require(status == 204 and response['acknowledged'] is True
                            or status == 404 and response['acknowledged'] is False
                            and response['response'].get('Error', {}).get('Code') == 'NoSuchUpload',
                            'DL recovery Abort has neither its own ACK nor exact upload absence')
                    retirements.append({'key': key, 'uploadId': upload_id, 'newRecoveryAbortAck': status == 204,
                        'observedNoSuchUpload': status == 404, 'originalAbortAckClaimed': False})
                remaining = self._uploads(recorders, 'pending-after', folder)
                require(all(not values for values in remaining.values()), 'DL pending upload survived original-intent retirement')
                for key, upload_id in registered:
                    for recorder in recorders.values():
                        absent = self._call(recorder, 'list_parts', {
                            'Bucket': self.bucket, 'Key': key, 'UploadId': upload_id, 'MaxParts': 32}, 404)
                        require(absent['response'].get('Error', {}).get('Code') == 'NoSuchUpload'
                                and absent['acknowledged'] is False, 'DL retired upload still has addressable parts')
                self._inventories(folder, 'after-retirement')
                result = {'allOriginalBusinessVersionsPreserved': True, 'allOriginalRequestsResolved': False,
                    'allPeerPendingUploadsAbsent': True, 'retirements': retirements,
                    'discoveredUploadIdsAreNotCreateAcks': sorted(discovered), 'originalRequestBytesRewritten': False,
                    'originalUnknownResultsRelabeledAcknowledged': False, 'PUTOrCompleteRetryIssued': False,
                    'preReadStoppedMpuEvidenceSha256': self.pre_read,
                    'originalLoadComplete': self.load_result is not None and self.load_result['complete'],
                    'canonicalCasePassClaimed': False, 'namespaceCleanupAllowed': False,
                    'stoppedPostRetirementPhysicalCheckRequired': True}
                save(folder / 'public-closure.json', result)
                self.public_candidate, self.public_candidate_sha = result, json_hash(result)
                self.phase = 'PUBLIC_RECONCILED'
                return result
            except Exception as exc:
                self.phase = 'RECONCILE_FAILED'
                save(folder / 'unresolved.json', {'type': type(exc).__name__, 'message': str(exc),
                    'namespaceCleanupAllowed': False, 'canonicalCasePassClaimed': False})
                raise

    def finish_reconciliation(self, business_after, *, repair_targets):
        with self.serial:
            require(self.phase == 'PUBLIC_RECONCILED' and self.post_mpu is not None
                    and json_hash(self.public_candidate) == self.public_candidate_sha
                    and self.business_baseline is not None
                    and json_hash(self.business_baseline) == self.business_baseline_sha,
                    'DL closure lacks its original public and stopped post-retirement evidence')
            self._stopped_business_scope(business_after)
            require(set(business_after) == set(self.business_baseline), 'DL business scene changed original nodes')
            expected = {(row['key'], row['versionId']) for row in self.baseline['entries'] if not row['deleteMarker']}
            require(isinstance(repair_targets, list) and all(target['bucket'] == self.bucket
                    and (target['key'], target['versionId']) in expected for target in repair_targets),
                    'DL repair exceptions escaped original acknowledged business versions')
            if repair_targets:
                physical = validate_repaired_physical(self.business_baseline, business_after, repair_targets[0],
                                                      repair_targets=repair_targets)
            else:
                require(files(self.business_baseline) == files(business_after)
                        and all(value['formats'] == business_after[node]['formats']
                                for node, value in self.business_baseline.items()),
                        'DL business scene changed files or formats without an independent repair')
                physical = {'allOriginalPhysicalFilesAndFormatsExact': True}
            save(self.directory / f'reconcile-{self.attempt:02d}' / 'final-business-physical.json', business_after)
            require(all(not scene['files'] for scene in self.post_mpu['nodes'].values()),
                    'DL unaccounted multipart files remain; retain the original namespace')
            original = self.registered_receipts()
            require(json_hash(original) == self.original_receipts_sha, 'DL original request journals changed during reconciliation')
            effects = []
            rows = [('seed', row) for row in original['seed'] if row['request']['operation'] in MUTATIONS]
            rows.extend((worker['endpoint'], row) for worker in original['workers'] if worker['role'] == 'writer' for row in worker['rows'])
            for actor, row in rows:
                operation = row['request']['operation']
                upload = operation in {'create_multipart_upload', 'upload_part', 'abort_multipart_upload'}
                effects.append({'actor': actor, 'operation': operation, 'requestSha256': row['requestSha256'],
                    'effect': 'NO_REMAINING_UPLOAD_AFTER_EXPLICIT_RECOVERY' if upload else
                              'ORIGINAL_VERSION_PRESERVED' if actor == 'seed' or operation == 'delete_object' else 'NO_BUSINESS_COMMIT',
                    'originalAckPresent': row['ackPresent'],
                    'originalResultUnknown': not row['ackPresent'] or row['ack']['observation'].get('resultUnknown') is True,
                    'originalAcknowledged': row['ackPresent'] and row['ack']['observation'].get('acknowledged') is True})
            result = {**self.public_candidate, 'allOriginalRequestsResolved': True,
                'requestEffectResolutions': effects, 'stoppedPostRetirementPhysicalCheckRequired': False,
                'originalBusinessPhysicalSha256': self.business_baseline_sha,
                'postRetirementBusinessPhysicalSha256': json_hash(business_after), 'businessPhysical': physical,
                'postRetirementMpuEvidenceSha256': json_hash(self.post_mpu), 'namespaceCleanupAllowed': True}
            save(self.directory / f'reconcile-{self.attempt:02d}' / 'closure.json', result)
            self.safe_to_cleanup, self.phase = True, 'CLOSED'
            return result
