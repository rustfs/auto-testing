#!/usr/bin/env python3
"""Compare one-shot bulk deletes at idle and under GET load on explicit endpoints.

The caller must hold its environment's exclusive test lock for the entire run.
Only fresh receipt-owned buckets are mutated; nodes and configuration are untouched.
"""

from concurrent.futures import ThreadPoolExecutor
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import threading
import time

try:
    from .performance_s3 import Session, RequestFailure, cleanup, delete_batch, prepare, remaining_objects, save
except ImportError:
    from performance_s3 import Session, RequestFailure, cleanup, delete_batch, prepare, remaining_objects, save


def read_load(endpoint, events, bucket, key, expected_hash, stop, ready, deadline):
    session = Session(endpoint, events, 10)
    counts = {"endpoint": endpoint, "successes": 0, "failures": 0, "http_503_attempts": 0,
              "first_success": False, "error_samples": []}
    try:
        client = session.client(5)
        while not stop.is_set() and time.monotonic() < deadline:
            try:
                response = client.get_object(Bucket=bucket, Key=key)
                body = response["Body"]
                try:
                    actual = hashlib.sha256(body.read()).hexdigest()
                finally:
                    body.close()
                if actual != expected_hash:
                    raise RequestFailure("ReadIntegrityMismatch")
                counts["successes"] += 1
                counts["first_success"] = True
                ready.set()
            except Exception as exc:
                counts["failures"] += 1
                response = getattr(exc, "response", {})
                code = getattr(exc, "code", response.get("Error", {}).get("Code", type(exc).__name__))
                counts["http_503_attempts"] += int(response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 503)
                if len(counts["error_samples"]) < 5:
                    counts["error_samples"].append(code)
                    session.record_response("probe_get", response, code)
                stop.wait(0.1)
    finally:
        ready.set()
    return counts


def run_case(endpoints, endpoint, count, loaded, result_dir, workers=32, object_size=131072,
             budget=300, cleanup_budget=180):
    state = result_dir / "bucket.s3.json"
    events = result_dir / "s3-events.jsonl"
    result = {"endpoint": endpoint, "batch_size": count, "load": "get" if loaded else "idle",
              "read_workers": workers if loaded else 0, "verdict": "FAIL", "cleanup": "NOT_RUN",
              "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    session = Session(endpoint, events, budget)
    stop = threading.Event()
    futures, ready, pool = [], [], None
    try:
        document = prepare(session, state, "bulk-delete-probe")
        bucket = document["bucket"]
        result["bucket"] = bucket
        payload = b"x" * object_size
        for index in range(count):
            session.call("put_object", Bucket=bucket, Key=f"delete/{index:04}", Body=payload)
        if loaded:
            session.call("put_object", Bucket=bucket, Key="read-source", Body=payload)
            pool = ThreadPoolExecutor(max_workers=workers)
            load_deadline = min(session.deadline, time.monotonic() + 90)
            for index in range(workers):
                event = threading.Event()
                ready.append(event)
                futures.append(pool.submit(read_load, endpoints[index % len(endpoints)], events, bucket,
                                           "read-source", hashlib.sha256(payload).hexdigest(), stop, event, load_deadline))
            warm_deadline = time.monotonic() + 10
            for event in ready:
                event.wait(max(0, warm_deadline - time.monotonic()))
            result["load_started_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            result["load_ready_workers"] = sum(event.is_set() and not future.done()
                                                for event, future in zip(ready, futures))
        keys = [f"delete/{index:04}" for index in range(count)]
        http_before, failed_before = session.http_503, session.request_failures
        started = time.monotonic()
        result["delete_started_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # No cleanup retry is allowed to turn this diagnostic delete into a success.
        pending, errors = delete_batch(session, bucket, keys, attempts=1)
        result.update(delete_seconds=round(time.monotonic() - started, 3),
                      delete_finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      delete_error_entries=errors, failed_keys=pending,
                      delete_http_503_attempts=session.http_503 - http_before,
                      delete_request_failures=session.request_failures - failed_before)
        stop.set()
        reads = [future.result() for future in futures]
        result["reads"] = reads
        count_left, sample = remaining_objects(session, bucket)
        result.update(remaining_objects_before_cleanup=count_left, remaining_sample=sample)
        expected_residue = int(loaded)
        if (not pending and errors == 0 and count_left == expected_residue
                and (not loaded or result["load_ready_workers"] == workers)
                and all(row["first_success"] and row["failures"] == 0 for row in reads)):
            result["verdict"] = "PASS"
    except Exception as exc:
        result["error_code"] = getattr(exc, "code", type(exc).__name__)
    finally:
        stop.set()
        if pool:
            pool.shutdown(wait=True)
        if state.exists():
            try:
                document = cleanup(Session(endpoint, events, cleanup_budget), state)
                result.update(cleanup=document["cleanup"],
                              remaining_objects_after_cleanup=document.get("remaining_objects"),
                              cleanup_error_entries=document.get("cleanup_error_entries", 0),
                              cleanup_http_503_attempts=document.get("cleanup_http_503_attempts", 0),
                              cleanup_had_errors=document.get("cleanup_had_errors", False))
                if (document["cleanup"] != "PASS" or document.get("cleanup_error_entries", 0)
                        or document.get("cleanup_had_errors", False)):
                    result["verdict"] = "FAIL"
            except Exception as exc:
                result.update(verdict="FAIL", cleanup="FAIL",
                              cleanup_error_code=getattr(exc, "code", type(exc).__name__))
        result["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save(result_dir / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", action="append", required=True)
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--batch-sizes", default="1,32,128,500,1000")
    parser.add_argument("--read-workers", type=int, default=32)
    parser.add_argument("--object-size", type=int, default=131072)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        sizes = [int(value) for value in args.batch_sizes.split(",")]
    except ValueError:
        parser.error("batch sizes must be integers")
    if (not sizes or any(not 1 <= size <= 1000 for size in sizes)
            or len(sizes) != len(set(sizes)) or not 1 <= args.read_workers <= 64
            or not 1 <= args.object_size <= 4194304
            or any(not endpoint.startswith(("http://", "https://")) for endpoint in args.endpoint)):
        parser.error("invalid endpoint, batch sizes, worker count or object size")
    args.result_dir.mkdir(parents=True, exist_ok=False)
    def on_term(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, on_term)
    results, aborted = [], False
    for index, endpoint in enumerate(args.endpoint):
        for loaded in (False, True):
            for count in sizes:
                directory = args.result_dir / f"endpoint-{index + 1}-{'get' if loaded else 'idle'}-{count}"
                directory.mkdir()
                result = run_case(args.endpoint, endpoint, count, loaded, directory,
                                  args.read_workers, args.object_size)
                results.append(result)
                print(json.dumps({key: result[key] for key in ("endpoint", "load", "batch_size", "verdict", "cleanup")}), flush=True)
                if result["cleanup"] != "PASS":
                    aborted = True
                    break
            if aborted:
                break
        if aborted:
            break
    save(args.result_dir / "summary.json", {"results": results, "aborted": aborted,
                                            "expected_cases": len(args.endpoint) * len(sizes) * 2})
    return int(aborted or any(result["verdict"] != "PASS" for result in results))


if __name__ == "__main__":
    raise SystemExit(main())
