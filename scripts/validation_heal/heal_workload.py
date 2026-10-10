#!/usr/bin/env python3
"""Create deterministic Heal fixtures or run a bounded, ACK-logged S3 workload."""

from __future__ import annotations

import argparse
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json
import hashlib
from pathlib import Path
import threading
import time

try:
    from .heal_probe import CLOCK_DOMAIN, MAX_BODY, Probe, bucket_allowed, payload, require, save, validate_s3_request
    from .heal_workload_journal import AdmissionExpired, MutationOrder, batch_receipts, validate_batch_request
except ImportError:
    from heal_probe import CLOCK_DOMAIN, MAX_BODY, Probe, bucket_allowed, payload, require, save, validate_s3_request
    from heal_workload_journal import AdmissionExpired, MutationOrder, batch_receipts, validate_batch_request


class FixtureStateMismatch(ValueError):
    """Observed S3 state disagrees with valid, successful fixture ACKs."""


class RecordedCallError(ValueError):
    """Failed check with an already durable, exactly bound observation.

    Callers can retain known per-item batch effects without treating the whole
    call as success or guessing its result from a later LIST. Never a retry hint.
    """
    def __init__(self,message,operation,params,observation,request_sha256):
        super().__init__(message)
        self.operation=operation;self.params=copy.deepcopy(params)
        self.observation=copy.deepcopy(observation);self.request_sha256=request_sha256


class Recorder:
    def __init__(self, probe: Probe, output: Path, *, writer=None):
        self.probe, self.output = probe, output
        self.writer = writer
        output.mkdir(parents=True, exist_ok=False)
        self.sequence, self.failures, self.bytes_written = 0, 0, 0
        self.lock = threading.Lock()
        self.mutations = MutationOrder()

    def call(self, operation: str, params: dict, body: bytes | None = None,
             identity: dict | None = None, expect_http: int | None = None, *, admission_deadline=None) -> dict:
        params=copy.deepcopy(params);identity=copy.deepcopy(identity)
        validate_s3_request(self.probe.binding, operation, params)
        if operation == 'delete_objects':validate_batch_request(params)
        require(body is None or isinstance(body,bytes) and len(body)<=MAX_BODY and operation in {'put_object','upload_part'},
                'invalid foreground payload/type/budget')
        with self.mutations.hold(operation, params,admission_deadline):
            try:
                return self._call(operation, params, body, identity, expect_http)
            except BaseException:
                # Already in-flight calls drain and retain their own evidence;
                # no later foreground mutation starts after an uncertain write.
                self.mutations.poison('request/receipt failed')
                raise

    def _call(self, operation, params, body, identity, expect_http):
        with self.lock:
            self.sequence += 1
            sequence = self.sequence
            self.bytes_written += len(body) if body is not None else 0
            require(self.bytes_written <= self.probe.binding["maxInventoryBytes"], "batch payload byte budget exceeded")
        request = {"schemaVersion": 2, "runId": self.probe.binding["runId"], "sequence": sequence,
                   "operation": operation, "params": params, "identity": identity, "expectHttp": expect_http,
                   "clockDomain": CLOCK_DOMAIN, "preparedMonotonicNs": time.monotonic_ns(),
                   "preparedAt": datetime.now(UTC).isoformat(), "casePassClaimed": False}
        if body is not None:request['payload']={'size':len(body),'sha256':hashlib.sha256(body).hexdigest()}
        request_path = self.output / f"{sequence:06d}-request.json"
        # fsync file+directory before allowing any network invocation. A
        # request without a durable result remains UNKNOWN, never disappears.
        (self.writer or save)(request_path, request)
        request_sha = hashlib.sha256(request_path.read_bytes()).hexdigest()
        invoked = time.monotonic_ns()
        try:
            result = self.probe.s3(operation, params, body)
        except Exception as exc:
            result = {"acknowledged": False, "resultUnknown": True, "response": getattr(exc,'response',None),
                      "error": {"type": type(exc).__name__, "message": str(exc)}}
        finished = time.monotonic_ns()
        status = (result.get("response") or {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        checked = status == expect_http if expect_http is not None else (
            result["acknowledged"] and type(status) is int and 200<=status<300)
        checked = checked and not result.get('clientCleanupError')
        per_item = None
        if operation == 'delete_objects' and status == 200:
            per_item = batch_receipts(params, result['response'])
            checked = checked and not per_item['errors']
        (self.writer or save)(self.output / f"{sequence:06d}-ack.json", {"schemaVersion": 2,
             "runId": self.probe.binding["runId"], "sequence": sequence, "identity": identity,
             "requestFile":request_path.name,"requestSha256":request_sha,"batchDelete":per_item,
             "clockDomain":CLOCK_DOMAIN,"recordedMonotonicNs":time.monotonic_ns(),
             "collectorCallStartedMonotonicNs":invoked,"collectorCallFinishedMonotonicNs":finished,
             "operationCheck": "PASS" if checked else "FAIL", "observation": result,
             "casePassClaimed": False})
        with self.lock:
            self.failures += not checked
        if not checked:
            raise RecordedCallError(f"unexpected {operation} result; inspect ACK {sequence:06d}, do not silently retry",
                                    operation,params,result,request_sha)
        return result.get("response") or {}


def seed(recorder: Recorder, bucket: str, fixture: str, count: int, sizes: list[int],
         *, key_prefix: str = "objects/") -> list[dict]:
    bucket_allowed(recorder.probe.binding, bucket)
    require(1 <= count <= 2048 and bool(sizes) and all(0 <= size <= 64 * 1024**2 for size in sizes),
            "invalid bounded fixture dimensions")
    require(key_prefix in {"objects/", "objects-"} and (key_prefix == "objects/" or fixture == "DB"),
            "flat keys are only supported for the bounded DB fixture")
    recorder.call("create_bucket", {"Bucket": bucket})
    expected = []
    if fixture == "DV":
        recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    if fixture == "DM":
        for index in range(count):
            key = f"multipart/{index:04d}.bin"
            upload = recorder.call("create_multipart_upload", {"Bucket": bucket, "Key": key,
                                    "Metadata": {"heal-fixture": "DM"}, "ContentType": "application/octet-stream"})
            parts = []
            digest, full_size = hashlib.sha256(), 0
            for number, size in enumerate((5 * 1024**2 + 17, 5 * 1024**2 + 93, 1024**2 + 47), 1):
                body = payload(f"{bucket}/{key}/{number}", size)
                digest.update(body)
                full_size += len(body)
                result = recorder.call("upload_part", {"Bucket": bucket, "Key": key,
                                       "UploadId": upload["UploadId"], "PartNumber": number},
                                       body)
                parts.append({"PartNumber": number, "ETag": result["ETag"]})
            completed = recorder.call("complete_multipart_upload", {"Bucket": bucket, "Key": key,
                          "UploadId": upload["UploadId"], "MultipartUpload": {"Parts": parts}})
            expected.append({"key": key, "versionId": completed.get("VersionId", "null"),
                             "deleteMarker": False, "isLatest": True,
                             "payload": {"sha256": digest.hexdigest(), "size": full_size}})
        # Separate uploads for Complete/Abort races. Their actual IDs live in
        # the ACK ledger, never inferred from a later upload's ID.
        for name in ("pending-a", "pending-b"):
            response = recorder.call("create_multipart_upload", {"Bucket": bucket, "Key": name})
            recorder.call("upload_part", {"Bucket": bucket, "Key": name, "UploadId": response["UploadId"],
                          "PartNumber": 1}, payload(f"{bucket}/{name}", 5 * 1024**2 + 3))
        return expected
    for index in range(count):
        key = f"{key_prefix}{index:04d}.bin"
        if fixture == "DS":
            prefixes = ("中文/文件", "space key", "literal%2Fslash", "actual/slash", "a/child", "ab/control", "b/other")
            key = f"{prefixes[index % len(prefixes)]}/{index:04d}.bin"
        versions = 3 if fixture == "DV" else 1
        for version in range(versions):
            size = sizes[index % len(sizes)]
            body = payload(f"{bucket}/{key}/{version}", size)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                          "ContentType": "application/octet-stream",
                          "Metadata": {"heal-fixture": fixture, "nonce": f"{index}-{version}"},
                          "Tagging": "suite=heal&fixture=" + fixture},
                          body)
            require(fixture != "DV" or response.get("VersionId") not in {None, "null"},
                    "enabled versioning PUT omitted a real version ID")
            expected.append({"key": key, "versionId": response.get("VersionId", "null"),
                             "isLatest": version == versions - 1, "deleteMarker": False,
                             "payload": {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body)}})
        if fixture == "DV" and index % 2 == 0:
            response = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
            require(response.get("DeleteMarker") is True and response.get("VersionId"), "marker ACK is incomplete")
            expected[-1]["isLatest"] = False
            expected.append({"key": key, "versionId": response["VersionId"], "isLatest": True, "deleteMarker": True})
    return expected


def verify_seed(expected: list[dict], inventory: dict) -> dict:
    actual = {(entry["key"], entry["versionId"]): entry for entry in inventory["entries"]}
    wanted = {(entry["key"], entry["versionId"]): entry for entry in expected}
    require(bool(wanted) and len(wanted) == len(expected), "fixture ACK identities are empty or duplicate")
    if len(actual) != len(inventory["entries"]) or set(wanted) != set(actual):
        raise FixtureStateMismatch("fixture version/marker set differs from ACKs")
    for identity, entry in wanted.items():
        if not all(actual[identity].get(key) == value for key, value in entry.items()):
            raise FixtureStateMismatch("baseline content/version/marker differs from deterministic fixture ACK")
    return {"expectedEntries": len(expected), "seedAckAndFullGetAgree": True, "casePassClaimed": False}


def substitute(value, round_number: int, worker: int):
    if isinstance(value, dict):
        return {key: substitute(item, round_number, worker) for key, item in value.items()}
    if isinstance(value, list):
        return [substitute(item, round_number, worker) for item in value]
    if isinstance(value, str):
        return value.replace("${round}", str(round_number)).replace("${worker}", str(worker))
    return value


def run_trace(recorder: Recorder, trace: list[dict], rounds: int, workers: int,
              interval: float, duration: int) -> dict:
    require(1 <= rounds <= 100000 and 1 <= workers <= 16 and 0 <= interval <= 60
            and 1 <= duration <= 72 * 3600 and 1 <= len(trace) <= 100,
            "invalid bounded workload dimensions")
    require(rounds * workers * len(trace) <= 1_000_000, "request count exceeds batch cap")
    # Reject the whole trace before its first mutation. A later cross-run
    # bucket or invalid payload must not leave an earlier partial workload.
    for step in trace:
        require(isinstance(step, dict) and set(step) <= {"operation", "params", "size", "expectHttp"},
                "invalid trace fields")
        validate_s3_request(recorder.probe.binding, step["operation"], substitute(step["params"], 0, 0))
        if step['operation']=='delete_objects':validate_batch_request(step['params'])
        if "size" in step:
            require(type(step["size"]) is int and 0 <= step["size"] <= 64 * 1024**2
                    and step["operation"] in {"put_object", "upload_part"}, "invalid trace payload")
        if "expectHttp" in step:
            require(type(step["expectHttp"]) is int and 100 <= step["expectHttp"] <= 599, "invalid expected HTTP code")
    deadline, abort = time.monotonic() + duration, threading.Event()

    def worker_run(worker: int) -> dict:
        completed = 0
        for round_number in range(rounds):
            for index, step in enumerate(trace):
                if abort.is_set() or time.monotonic() >= deadline:
                    return {'worker':worker,'completedOperations':completed,'stopReason':'abort' if abort.is_set() else 'deadline'}
                identity = {"worker": worker, "round": round_number, "step": index}
                params = substitute(step["params"], round_number, worker)
                body = payload(f"{recorder.probe.binding['runId']}/{worker}/{round_number}/{index}", step["size"]) if "size" in step else None
                try:
                    recorder.call(step["operation"], params, body, identity, step.get("expectHttp"),admission_deadline=deadline)
                except AdmissionExpired:
                    return {'worker':worker,'completedOperations':completed,'stopReason':'admission-deadline'}
                except Exception:
                    abort.set()
                    raise
                completed += 1
                if interval:
                    abort.wait(min(interval, max(0, deadline - time.monotonic())))
        return {'worker':worker,'completedOperations':completed,'stopReason':'round-limit'}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker_run, worker) for worker in range(workers)]
        outcomes = [future.result() for future in futures]
    return {'workers':outcomes,'plannedOperationUpperBound':rounds*workers*len(trace),
            'completedOperations':sum(v['completedOperations'] for v in outcomes),
            'allRoundsCompleted':all(v['stopReason']=='round-limit' for v in outcomes),'casePassClaimed':False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--endpoint")
    sub = parser.add_subparsers(dest="command", required=True)
    fixture = sub.add_parser("seed")
    fixture.add_argument("--fixture", choices=("DB", "DM", "DV", "DS", "DX"), required=True)
    fixture.add_argument("--bucket", required=True)
    fixture.add_argument("--count", type=int)
    fixture.add_argument("--sizes", help="comma-separated actual byte sizes, including candidate-specific boundaries")
    workload = sub.add_parser("run")
    workload.add_argument("--trace", type=Path, required=True)
    workload.add_argument("--rounds", type=int, required=True)
    workload.add_argument("--workers", type=int, default=1)
    workload.add_argument("--interval", type=float, default=0.1, help="minimum seconds between each worker's requests")
    workload.add_argument("--duration", type=int, required=True, help="hard admission deadline in seconds")
    args = parser.parse_args()
    binding = json.loads(args.binding.read_text())
    recorder = Recorder(Probe(binding, args.endpoint), args.out)
    summary = {"runId": binding["runId"], "startedAt": datetime.now(UTC).isoformat(),
               "command": args.command, "casePassClaimed": False, "state": "RUNNING"}
    start = time.monotonic()
    try:
        if args.command == "seed":
            defaults = {"DB": 128, "DM": 1, "DV": 4, "DS": 257, "DX": 257}
            sizes = [int(value) for value in args.sizes.split(",")] if args.sizes else (
                [0, 1, 4095, 4096, 4097, 1024**2 - 1, 1024**2, 1024**2 + 1, 5 * 1024**2 + 123]
                if args.fixture == "DB" else [5 * 1024**2 + 123])
            expected = seed(recorder, args.bucket, args.fixture, args.count if args.count is not None else defaults[args.fixture], sizes)
            save(args.out / "expected-fixture.json", {"runId": binding["runId"], "bucket": args.bucket, "entries": expected})
            baseline = recorder.probe.inventory(args.bucket)
            save(args.out / "baseline.json", {"runId": binding["runId"], "inventory": baseline})
            summary["baselineVerification"] = verify_seed(expected, baseline)
        else:
            summary['workload'] = run_trace(recorder, json.loads(args.trace.read_text()), args.rounds, args.workers, args.interval, args.duration)
        summary["state"] = "OPERATIONS_COMPLETE"
    except Exception as exc:
        summary.update(state="FAIL", error={"type": type(exc).__name__, "message": str(exc)})
    summary.update(requests=recorder.sequence, failedOperations=recorder.failures,
                   elapsedSeconds=time.monotonic() - start, finishedAt=datetime.now(UTC).isoformat())
    save(args.out / "summary.json", summary)
    print(json.dumps(summary))
    return 0 if summary["state"] == "OPERATIONS_COMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
