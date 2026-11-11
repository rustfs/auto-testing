#!/usr/bin/env python3
"""Run bounded bulk-delete regressions on an already deployed S3 cluster.

The caller owns the environment lock. Only fresh, receipt-owned buckets are used.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import threading
import time
from urllib.parse import urlsplit

try:
    from .performance_s3 import Session, RequestFailure, RETRYABLE, abort_uploads, prepare, read_state, save
except ImportError:
    from performance_s3 import Session, RequestFailure, RETRYABLE, abort_uploads, prepare, read_state, save

GROUPS = {"S3C-213": "batch", "S3C-214": "duplicates",
          "S3C-1306": "get-load", "S3C-1307": "put-delete"}
MODES = ("off", "on", "suspended")
PAYLOAD = (b"bulk-delete-regression\n" * 8192)[:131072]
CONTROL = "control/immutable"


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def require(ok, code):
    if not ok:
        raise RequestFailure(code)


def credentials():
    explicit = bool(os.environ.get("RUSTFS_ACCESS_KEY") or os.environ.get("RUSTFS_SECRET_KEY"))
    access = os.environ.get("RUSTFS_ACCESS_KEY" if explicit else "AWS_ACCESS_KEY_ID")
    secret = os.environ.get("RUSTFS_SECRET_KEY" if explicit else "AWS_SECRET_ACCESS_KEY")
    require(access and secret, "MissingS3Credentials")
    token = os.environ.get("RUSTFS_SESSION_TOKEN" if explicit else "AWS_SESSION_TOKEN") or None
    return access, secret, token


class Requests(Session):
    def __init__(self, endpoint, events, budget, phase, factory=None):
        super().__init__(endpoint, events, budget, factory)
        self.phase = phase

    def client(self, remaining):
        import boto3
        from botocore.config import Config
        access, secret, token = credentials()
        return boto3.client(
            "s3", endpoint_url=self.endpoint,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            aws_access_key_id=access, aws_secret_access_key=secret, aws_session_token=token,
            config=Config(s3={"addressing_style": "path"}, proxies={},
                          connect_timeout=min(3, remaining), read_timeout=min(10, remaining),
                          retries={"total_max_attempts": 1}),
        )

    def emit(self, **record):
        super().emit(phase=self.phase, **record)

    def call(self, operation, **arguments):
        started = time.monotonic()
        code = None
        try:
            require(started < self.deadline, "PhaseDeadlineExceeded")
            return super().call(operation, **arguments)
        except RequestFailure as exc:
            code = exc.code
            raise
        finally:
            self.emit(kind="operation_time", operation=operation,
                      seconds=round(time.monotonic() - started, 6), error_code=code)


def delete_once(session, bucket, objects):
    require(0 < len(objects) <= 1000, "InvalidDeleteBatchSize")
    response = session.call("delete_objects", Bucket=bucket,
                            Delete={"Objects": objects, "Quiet": False})
    deleted, errors = response.get("Deleted", []), response.get("Errors", [])
    require(isinstance(deleted, list) and isinstance(errors, list), "InvalidDeleteResponse")
    rows = deleted + errors
    require(all(isinstance(row, dict) and isinstance(row.get("Key"), str) for row in rows)
            and all(isinstance(row.get("Code"), str) for row in errors), "InvalidDeleteResponse")
    versioned = all("VersionId" in obj for obj in objects)
    identity = lambda row: (row["Key"], row.get("VersionId")) if versioned else row["Key"]
    # Count request entries, including duplicate keys; they are not unique objects.
    require(Counter(map(identity, rows)) == Counter(map(identity, objects)), "IncompleteDeleteResponse")
    retained = [{k: row[k] for k in ("Key", "VersionId", "Code") if k in row} for row in errors]
    session.emit(kind="delete_response", requested_entries=len(objects),
                 distinct_keys=len({obj["Key"] for obj in objects}), errors=retained)
    return retained


def listed(session, bucket, versions=False, prefix=""):
    result, seen, objects_seen, pagination = [], set(), set(), {}
    while True:
        operation = "list_object_versions" if versions else "list_objects_v2"
        response = session.call(operation, Bucket=bucket, Prefix=prefix, MaxKeys=1000, **pagination)
        rows = []
        for field in (("Versions", "DeleteMarkers") if versions else ("Contents",)):
            page = response.get(field, [])
            require(isinstance(page, list) and all(isinstance(row, dict)
                    and isinstance(row.get("Key"), str)
                    and (not versions or isinstance(row.get("VersionId"), str) and row["VersionId"])
                    for row in page), "InvalidListResponse")
            rows.extend({"Key": row["Key"], **({"VersionId": row["VersionId"]} if versions else {})}
                        for row in page)
        identities = [(row["Key"], row.get("VersionId")) for row in rows]
        require(len(set(identities)) == len(identities) and not objects_seen.intersection(identities),
                "DuplicateListEntry")
        objects_seen.update(identities)
        result.extend(rows)
        if not response.get("IsTruncated"):
            return result
        if versions:
            marker = response.get("NextKeyMarker")
            pagination = {"KeyMarker": marker}
            if response.get("NextVersionIdMarker"):
                pagination["VersionIdMarker"] = response["NextVersionIdMarker"]
        else:
            marker = response.get("NextContinuationToken")
            pagination = {"ContinuationToken": marker}
        token = tuple(sorted(pagination.items()))
        require(rows and isinstance(marker, str) and marker and token not in seen, "InvalidListPagination")
        seen.add(token)


def read_exact(session, bucket, key, payload, version=None):
    arguments = {"Bucket": bucket, "Key": key}
    if version is not None:
        arguments["VersionId"] = version
    response = session.call("get_object", **arguments)
    body = response["Body"]
    try:
        actual = body.read(len(payload) + 1)
    finally:
        body.close()
    require(actual == payload, "ReadIntegrityMismatch")
    session.emit(kind="read_integrity", key=key, version_id=version,
                 sha256=hashlib.sha256(actual).hexdigest())


def absent(session, bucket, key):
    try:
        session.call("head_object", Bucket=bucket, Key=key)
    except RequestFailure as exc:
        if exc.code in ("404", "NoSuchKey", "NotFound"):
            return
        raise
    raise RequestFailure("DeletedKeyStillReadable")


def cleanup_bucket(session, state, result):
    receipt = read_state(state)
    require(receipt.get("created") is True and receipt.get("endpoint") == session.endpoint,
            "UnownedCleanupReceipt")
    bucket = receipt["bucket"]
    receipt.update(cleanup="FAIL", cleanup_in_progress=True,
                   cleanup_error_entries=receipt.get("cleanup_error_entries", 0),
                   cleanup_had_errors=receipt.get("cleanup_had_errors", False))
    save(state, receipt)
    try:
        abort_uploads(session, bucket)
        objects = listed(session, bucket, versions=True)
        for offset in range(0, len(objects), 1000):
            pending = objects[offset:offset + 1000]
            for attempt in range(3):
                errors = delete_once(session, bucket, pending)
                receipt["cleanup_error_entries"] += len(errors)
                receipt["cleanup_had_errors"] |= bool(errors)
                save(state, receipt)
                if not errors:
                    break
                if any(row["Code"] not in RETRYABLE for row in errors) or attempt == 2:
                    raise RequestFailure("CleanupDeleteFailed")
                pending = [{key: row[key] for key in ("Key", "VersionId")} for row in errors]
            else:
                raise RequestFailure("CleanupDeleteFailed")
        remaining = listed(session, bucket, versions=True)
        visible = listed(session, bucket)
        receipt.update(remaining_versions=len(remaining), remaining_objects=len(visible))
        require(not remaining and not visible, "CleanupResidue")
        session.call("delete_bucket", Bucket=bucket)
        receipt["cleanup"] = "PASS"
    except Exception as exc:
        receipt.update(cleanup_had_errors=True, cleanup_error_code=getattr(exc, "code", type(exc).__name__))
    finally:
        receipt["cleanup_in_progress"] = False
        save(state, receipt)
        result.update({key: receipt[key] for key in ("cleanup", "cleanup_had_errors", "cleanup_error_entries")})
        result["remaining_versions"] = receipt.get("remaining_versions")
        result["remaining_objects"] = receipt.get("remaining_objects")


def load_worker(session, bucket, key, write, stop, ready, release, rounds):
    result = {"successes": 0, "failures": 0, "first_success": False,
              "started_at": now(), "error_codes": []}
    try:
        for index in range(rounds):
            if stop.is_set() or time.monotonic() >= session.deadline:
                break
            try:
                if write:
                    session.call("put_object", Bucket=bucket, Key=key, Body=PAYLOAD)
                else:
                    read_exact(session, bucket, key, PAYLOAD)
                result["successes"] += 1
                result["first_success"] = True
                ready.set()
                if index == 0:
                    release.wait(max(0, session.deadline - time.monotonic()))
            except Exception as exc:
                result["failures"] += 1
                if len(result["error_codes"]) < 5:
                    result["error_codes"].append(getattr(exc, "code", type(exc).__name__))
                break
            stop.wait(0.01)
    finally:
        result["finished_at"] = now()
    return result


def run_case(case_id, mode, count, endpoints, directory, workers=32, factory=None,
             prepare_budget=300, request_budget=60, load_budget=30, cleanup_budget=180):
    directory.mkdir(parents=True, exist_ok=False)
    case_started = time.monotonic()
    state, events = directory / "bucket.s3.json", directory / "s3-events.jsonl"
    result = {"case_id": case_id, "mode": mode, "batch_size": count, "endpoints": endpoints,
              "verdict": "RUNNING", "recovery": "NOT_RUN", "cleanup": "NOT_RUN", "started_at": now(), "phases": []}
    save(directory / "result.json", result)
    phase, phase_started = "prepare", time.monotonic()
    stop, release = threading.Event(), threading.Event()
    futures, pool = [], None
    deadlines = {}
    def session(endpoint, phase_name, budget):
        requests = Requests(endpoint, events, budget, phase_name, factory)
        requests.deadline = deadlines.setdefault(phase_name, requests.deadline)
        return requests
    def transition(name):
        nonlocal phase, phase_started
        result["phases"].append({"phase": phase, "seconds": round(time.monotonic() - phase_started, 3)})
        phase, phase_started = name, time.monotonic()
    def failed(exc, failure_phase):
        result["primary_verdict"] = "FAIL"
        result.setdefault("failure_phase", failure_phase)
        result.setdefault("error_code", getattr(exc, "code", type(exc).__name__))
        result.setdefault("failures", []).append({"phase": failure_phase,
            "error_code": getattr(exc, "code", type(exc).__name__)})
    try:
        setup = session(endpoints[0], phase, prepare_budget)
        bucket = prepare(setup, state, "s3-bulk-regression")["bucket"]
        result["bucket"] = bucket
        keys = [f"target/{index:04}" for index in range(count)]
        history = {}
        if mode != "off":
            setup.call("put_bucket_versioning", Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        if mode == "suspended":
            for key in (keys[0], keys[-1]):
                response = setup.call("put_object", Bucket=bucket, Key=key, Body=PAYLOAD)
                require(response.get("VersionId") not in (None, "", "null"), "MissingHistoricalVersion")
                history[key] = response["VersionId"]
            setup.call("put_bucket_versioning", Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
        for key in keys + [CONTROL]:
            response = setup.call("put_object", Bucket=bucket, Key=key, Body=PAYLOAD)
            if mode == "on" and key in (keys[0], keys[-1]):
                require(response.get("VersionId") not in (None, "", "null"), "MissingHistoricalVersion")
                history[key] = response["VersionId"]
        for endpoint in endpoints:
            seed = session(endpoint, phase, prepare_budget)
            require({row["Key"] for row in listed(seed, bucket, prefix="target/")} == set(keys),
                    "SeedInventoryMismatch")
        objects = [{"Key": key} for key in keys]
        if case_id in ("S3C-214", "S3C-1307"):
            objects = [{"Key": keys[0]}] * 16 + objects[1:]
            if case_id == "S3C-214":
                objects += [{"Key": "already-absent"}] * 2
        loaded = case_id in ("S3C-1306", "S3C-1307")
        if loaded:
            transition("load-start")
            pool = ThreadPoolExecutor(max_workers=workers)
            ready = []
            for index in range(workers):
                event = threading.Event()
                ready.append(event)
                endpoint = endpoints[index % len(endpoints)]
                futures.append(pool.submit(load_worker, session(endpoint, "load", load_budget), bucket,
                                           keys[0] if case_id == "S3C-1307" else CONTROL,
                                           case_id == "S3C-1307", stop, event, release,
                                           32 if case_id == "S3C-1307" else 4096))
            deadline = time.monotonic() + min(10, load_budget)
            for event, future in zip(ready, futures):
                while not event.is_set() and not future.done() and time.monotonic() < deadline:
                    event.wait(min(0.01, max(0, deadline - time.monotonic())))
            require(all(event.is_set() and not future.done() for event, future in zip(ready, futures)),
                    "LoadNotReady")
        transition("delete")
        release.set()
        deletion = session(endpoints[0], phase, request_budget)
        result.update(primary_verdict="PASS", requested_entries=len(objects), distinct_keys=len(keys))
        try:
            errors = delete_once(deletion, bucket, objects)
            result.update(delete_error_entries=len(errors), delete_errors=errors)
            if errors:
                failed(RequestFailure("DeleteObjectErrors"), phase)
        except RequestFailure as exc:
            failed(exc, phase)
        stop.set()
        if pool:
            pool.shutdown(wait=True)
            pool = None
        result["load"] = [future.result() for future in futures]
        if any(row["failures"] or not row["first_success"] for row in result["load"]):
            failed(RequestFailure("LoadRequestFailed"), "load")
        transition("verify")
        try:
            hot_views = []
            for endpoint in endpoints:
                check = session(endpoint, phase, request_budget)
                remaining = listed(check, bucket, prefix="target/")
                if case_id == "S3C-1307":
                    require(all(row["Key"] == keys[0] for row in remaining), "UnrelatedKeysNotDeleted")
                    visible = any(row["Key"] == keys[0] for row in remaining)
                    hot_views.append(visible)
                    if visible:
                        read_exact(check, bucket, keys[0], PAYLOAD)
                else:
                    require(not remaining, "DeleteResidue")
                samples = keys[1:] if case_id == "S3C-1307" else keys
                for key in dict.fromkeys(samples[:1] + samples[-1:]):
                    absent(check, bucket, key)
                read_exact(check, bucket, CONTROL, PAYLOAD)
                for key, version in history.items():
                    read_exact(check, bucket, key, PAYLOAD, version)
            require(len(set(hot_views)) <= 1, "ReplicaViewMismatch")
        except RequestFailure as exc:
            failed(exc, phase)
        if case_id == "S3C-1307":
            transition("recovery")
            result["recovery"] = "FAIL"
            # A fresh same-key PUT/DELETE after all writers join detects leaked ownership.
            recover = session(endpoints[0], phase, request_budget)
            sentinel = b"after-contention-sentinel"
            recover.call("put_object", Bucket=bucket, Key=keys[0], Body=sentinel)
            for endpoint in endpoints:
                read_exact(session(endpoint, phase, request_budget), bucket, keys[0], sentinel)
            require(not delete_once(recover, bucket, [{"Key": keys[0]}]), "RecoveryDeleteFailed")
            cold = listed(recover, bucket, prefix="target/")
            if cold:
                require(not delete_once(recover, bucket, cold), "RecoveryDeleteFailed")
            for endpoint in endpoints:
                check = session(endpoint, phase, request_budget)
                require(not listed(check, bucket, prefix="target/"), "RecoveryResidue")
                absent(check, bucket, keys[0])
            result["recovery"] = "PASS"
        result["verdict"] = result["primary_verdict"]
    except BaseException as exc:
        failed(exc, phase)
        result["verdict"] = "FAIL"
        if isinstance(exc, KeyboardInterrupt):
            result["interrupted"] = True
    finally:
        stop.set()
        release.set()
        if pool:
            pool.shutdown(wait=True)
            result["load"] = [future.result() for future in futures]
        transition("cleanup")
        # Save the primary verdict before recovery can change or fail anything.
        save(directory / "result.json", result)
        if state.exists():
            try:
                cleanup_bucket(session(endpoints[0], phase, cleanup_budget), state, result)
            except BaseException as exc:
                result.update(cleanup="FAIL", cleanup_error_code=getattr(exc, "code", type(exc).__name__))
        if result["cleanup"] != "PASS" or result.get("cleanup_had_errors"):
            result["verdict"] = "FAIL"
        result["phases"].append({"phase": phase, "seconds": round(time.monotonic() - phase_started, 3)})
        records = [json.loads(line) for line in events.read_text().splitlines()] if events.exists() else []
        result["http_503_responses"] = sum(row.get("kind") == "s3_request" and row.get("http_status") == 503
                                            for row in records)
        result["service_unavailable_entries"] = sum(row["Code"] == "ServiceUnavailable"
            for record in records if record.get("kind") == "delete_response" for row in record["errors"])
        result["phase_seconds"] = {row["phase"]: row["seconds"] for row in result["phases"]}
        result["http_503_by_phase"] = dict(Counter(row["phase"] for row in records
            if row.get("kind") == "s3_request" and row.get("http_status") == 503))
        result["duration_seconds"] = round(time.monotonic() - case_started, 3)
        result["finished_at"] = now()
        save(directory / "result.json", result)
    return result


def plan(groups, sizes):
    return [(case_id, mode, count) for case_id in groups for mode in MODES
            for count in (sizes if case_id == "S3C-213" else [32] if case_id == "S3C-214"
                          else [1000] if case_id == "S3C-1306" else [128])]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", action="append", required=True)
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--case", choices=GROUPS, action="append")
    parser.add_argument("--batch-sizes", default="1,32,128,500,1000")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--prepare-budget", type=int, default=300)
    parser.add_argument("--request-budget", type=int, default=60)
    parser.add_argument("--load-budget", type=int, default=30)
    parser.add_argument("--cleanup-budget", type=int, default=180)
    args = parser.parse_args(argv)
    try:
        sizes = [int(value) for value in args.batch_sizes.split(",")]
        endpoints = [urlsplit(value) for value in args.endpoint]
        require(sizes and len(set(sizes)) == len(sizes) and all(1 <= n <= 1000 for n in sizes)
                and 1 <= args.workers <= 64
                and all(1 <= value <= 3600 for value in (args.prepare_budget, args.request_budget,
                                                        args.load_budget, args.cleanup_budget))
                and len(set(args.endpoint)) == len(args.endpoint)
                and all(e.scheme in ("http", "https") and e.hostname and not e.username and not e.password
                        and e.path in ("", "/") and not e.query and not e.fragment for e in endpoints), "InvalidArguments")
        groups = args.case or list(GROUPS)
        require(len(set(groups)) == len(groups), "DuplicateCaseSelection")
        credentials()
    except (ValueError, RequestFailure) as exc:
        parser.error(getattr(exc, "code", "InvalidArguments"))
    os.umask(0o077)
    args.result_dir.mkdir(parents=True, exist_ok=False)
    interrupted = False
    def terminate(*_):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, terminate)
    results, aborted = [], False
    inventory = plan(groups, sizes)
    for case_id, mode, count in inventory:
        name = f"{case_id}-{mode}-{count}"
        if aborted:
            result = {"case_id": case_id, "mode": mode, "batch_size": count, "verdict": "NOT_RUN"}
        else:
            result = run_case(case_id, mode, count, args.endpoint, args.result_dir / name, args.workers,
                              prepare_budget=args.prepare_budget, request_budget=args.request_budget,
                              load_budget=args.load_budget, cleanup_budget=args.cleanup_budget)
            aborted = result["cleanup"] != "PASS" or result.get("interrupted", False)
        results.append(result)
        print(json.dumps(result), flush=True)
        save(args.result_dir / "summary.json", {"results": results, "expected_cases": len(inventory),
                                                "aborted": aborted})
    lines = ["| 用例 | 版本状态 | 批量大小 | 结果 | 失败阶段 | 无负载恢复 | 清理 | HTTP 503 响应 | 逐对象 ServiceUnavailable 条目 |",
             "|---|---|---:|---|---|---|---|---:|---:|"]
    for row in results:
        lines.append("| " + " | ".join(str(row.get(k, "—")) for k in
            ("case_id", "mode", "batch_size", "verdict", "failure_phase", "recovery", "cleanup",
             "http_503_responses", "service_unavailable_entries")) + " |")
    (args.result_dir / "summary.md").write_text("\n".join(lines) + "\n")
    return 130 if interrupted else int(aborted or any(row["verdict"] != "PASS" for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
