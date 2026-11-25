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
import itertools
import json
import os
from pathlib import Path
import signal
import threading
import time
from urllib.parse import urlsplit

try:
    from .performance_s3 import Session, RequestFailure, RETRYABLE, abort_uploads, prepare, read_state, save
    from .s3_bulk_delete_keys import KEY_SHAPES, make_keys
except ImportError:
    from performance_s3 import Session, RequestFailure, RETRYABLE, abort_uploads, prepare, read_state, save
    from s3_bulk_delete_keys import KEY_SHAPES, make_keys

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
    read_timeout = 10

    def __init__(self, endpoint, events, budget, phase, factory=None, *, read_timeout=None):
        super().__init__(endpoint, events, budget, factory)
        if read_timeout is not None:
            self.read_timeout = read_timeout
        self.phase = phase
        self.window = None
        self.worker = None
        self.pending_read = None
        self.delete_http_status = None
        self.delete_entries = None

    def record_response(self, operation, response, code=None):
        if operation == "delete_objects":
            metadata = response.get("ResponseMetadata")
            self.delete_http_status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
        super().record_response(operation, response, code)

    def client(self, remaining):
        import boto3
        from botocore.config import Config
        access, secret, token = credentials()
        return boto3.client(
            "s3", endpoint_url=self.endpoint,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            aws_access_key_id=access, aws_secret_access_key=secret, aws_session_token=token,
            config=Config(s3={"addressing_style": "path"}, proxies={},
                          connect_timeout=min(3, remaining), read_timeout=min(self.read_timeout, remaining),
                          retries={"total_max_attempts": 1}),
        )

    def emit(self, **record):
        super().emit(phase=self.phase, **record)

    def invoke(self, operation, **arguments):
        # Measure SDK calls after client construction and before response
        # logging. A streamed GET stays active until read_exact consumes it.
        if self.phase == "delete" and operation == "delete_objects":
            if self.window:
                self.window.begin_delete(self.deadline)
            self.delete_started_at = now()
            started = time.monotonic()
            try:
                return super().invoke(operation, **arguments)
            finally:
                self.delete_finished_monotonic = time.monotonic()
                self.delete_finished_at = now()
                self.delete_seconds = self.delete_finished_monotonic - started
                if self.window:
                    self.window.finish_delete()
        if self.window is None:
            return super().invoke(operation, **arguments)
        started = self.window.begin_request(self.worker)
        try:
            response = super().invoke(operation, **arguments)
        except BaseException:
            self.finish_load_request(started, False)
            raise
        if operation == "get_object":
            self.pending_read = started
        else:
            self.finish_load_request(started, True)
        return response

    def finish_load_request(self, started, success):
        overlap = self.window.finish_request(self.worker, started, success)
        if overlap is not None:
            self.emit(kind="load_overlap", worker=self.worker, success=success, **overlap)

    def finish_read(self, success):
        if self.pending_read is not None:
            started, self.pending_read = self.pending_read, None
            self.finish_load_request(started, success)

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
    complete = Counter(map(identity, rows)) == Counter(map(identity, objects))
    retained = [{k: row[k] for k in ("Key", "VersionId", "Code") if k in row} for row in errors]
    acknowledgments = [{k: row[k] for k in ("Key", "VersionId", "DeleteMarker", "DeleteMarkerVersionId") if k in row}
                       for row in deleted]
    session.delete_entries = {"deleted_count": len(deleted), "error_count": len(errors), "complete": complete}
    session.emit(kind="delete_response", requested_entries=len(objects),
                 distinct_keys=len({obj["Key"] for obj in objects}), errors=retained,
                 deleted=acknowledgments, complete=complete)
    require(complete, "IncompleteDeleteResponse")
    return retained


def listed(session, bucket, versions=False, prefix="", stats=None):
    result, seen, objects_seen, pagination = [], set(), set(), {}
    while True:
        operation = "list_object_versions" if versions else "list_objects_v2"
        response = session.call(operation, Bucket=bucket, Prefix=prefix, MaxKeys=1000, **pagination)
        if stats is not None:
            stats["pages"] += 1
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
    body, success = None, False
    try:
        body = response["Body"]
        actual = body.read(len(payload) + 1)
        require(actual == payload, "ReadIntegrityMismatch")
        success = True
    finally:
        try:
            session.finish_read(success)
        finally:
            if body is not None:
                body.close()
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


def cleanup_bucket(session, state, result, delete_session=None):
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
                errors = delete_once(delete_session or session, bucket, pending)
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


class LoadWindow:
    """Measure client operation overlap, excluding warmup and scheduling gaps."""

    def __init__(self, workers):
        self.condition = threading.Condition()
        self.workers = workers
        self.active = set()
        self.finished_workers = 0
        self.started = None
        self.finished = None
        self.results = [{"post_warmup_requests": 0, "overlapping_requests": 0, "overlapping_successes": 0}
                        for _ in range(workers)]

    def begin_request(self, worker):
        with self.condition:
            started = time.monotonic()
            self.active.add(worker)
            self.results[worker]["post_warmup_requests"] += 1
            self.condition.notify_all()
            return started

    def finish_request(self, worker, started, success):
        finished = time.monotonic()
        with self.condition:
            self.active.remove(worker)
            if self.started is None:
                return None
            overlap = min(finished, self.finished if self.finished is not None else finished) - max(started, self.started)
            if overlap > 0:
                span = {"started_monotonic": started, "finished_monotonic": finished, "overlap_seconds": overlap}
                self.results[worker]["overlapping_requests"] += 1
                self.results[worker]["overlapping_successes"] += int(success)
                self.results[worker].setdefault("first_overlap", {**span, "success": success})
                return span
        return None

    def finish_worker(self):
        with self.condition:
            self.finished_workers += 1
            self.condition.notify_all()

    def begin_delete(self, deadline):
        with self.condition:
            self.condition.wait_for(lambda: self.active or self.finished_workers == self.workers,
                                    timeout=max(0, deadline - time.monotonic()))
            require(self.active and time.monotonic() < deadline, "LoadNotActive")
            self.started = time.monotonic()

    def finish_delete(self):
        with self.condition:
            self.finished = time.monotonic()


def load_worker(session, bucket, key, write, stop, ready, release, rounds, window, worker):
    result = {"successes": 0, "failures": 0, "first_success": False,
              "started_at": now(), "error_codes": []}
    try:
        for index in itertools.count() if rounds is None else range(rounds):
            if stop.is_set() or time.monotonic() >= session.deadline:
                break
            try:
                if index:
                    session.window, session.worker = window, worker
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
            finally:
                # Covers malformed responses or diagnostics failures before
                # read_exact can close a successfully opened response body.
                session.finish_read(False)
            # Start post-warmup activity immediately after release. A warmup
            # success does not establish that any load overlapped the delete.
            if index:
                stop.wait(0.01)
    finally:
        result["finished_at"] = now()
        result.update(window.results[worker])
        window.finish_worker()
    return result


def target_view(session, bucket, expected=None, sdk_finished=None):
    """Measure one complete recursive LIST of target/, separately from mutation time."""
    started = time.monotonic()
    view = {"endpoint": session.endpoint, "started_at": now(), "pages": 0}
    try:
        keys = [row["Key"] for row in listed(session, bucket, prefix="target/", stats=view)]
        view.update(visible=len(keys), first_key=keys[0] if keys else None, last_key=keys[-1] if keys else None)
        if expected is not None:
            view["matches_manifest"] = set(keys) == expected
    except RequestFailure as exc:
        view["error_code"] = exc.code
    finally:
        finished = time.monotonic()
        view.update(finished_at=now(), seconds=round(finished - started, 6))
        if sdk_finished is not None:
            view.update(start_after_sdk_seconds=round(started - sdk_finished, 6),
                        end_after_sdk_seconds=round(finished - sdk_finished, 6))
    return view


def run_case(case_id, mode, count, endpoints, directory, workers=32, factory=None,
             prepare_budget=300, request_budget=60, load_budget=30, cleanup_budget=180, *,
             profile="compatibility", key_shape="short", observation_budget=180):
    require(profile in ("compatibility", "client-10s") and 0 < observation_budget < float("inf"), "InvalidObservationProfile")
    keys = make_keys(count, key_shape)
    require(bool(keys), "InvalidDeleteBatchSize")
    observed = case_id in ("S3C-213", "S3C-1306")
    extended = observed and profile == "compatibility"
    directory.mkdir(parents=True, exist_ok=False)
    case_started = time.monotonic()
    state, events = directory / "bucket.s3.json", directory / "s3-events.jsonl"
    result = {"case_id": case_id, "mode": mode, "batch_size": count, "endpoints": endpoints,
              "verdict": "RUNNING", "recovery": "NOT_RUN", "cleanup": "NOT_RUN", "started_at": now(), "phases": [],
              "profile": profile, "key_shape": key_shape, "observation_budget_seconds": observation_budget,
              "key_shape_stats": {"min_utf8_bytes": min(len(key.encode()) for key in keys),
                                  "max_utf8_bytes": max(len(key.encode()) for key in keys),
                                  "min_components": min(key.count("/") + 1 for key in keys),
                                  "max_components": max(key.count("/") + 1 for key in keys)},
              "physical_completion": "not_measured"}
    save(directory / "result.json", result)
    phase, phase_started = "prepare", time.monotonic()
    stop, release = threading.Event(), threading.Event()
    window = LoadWindow(workers)
    futures, pool = [], None
    deadlines = {}
    def session(endpoint, phase_name, budget, read_timeout=None):
        requests = Requests(endpoint, events, budget, phase_name, factory, read_timeout=read_timeout)
        requests.deadline = deadlines.setdefault(phase_name, requests.deadline)
        return requests
    def transition(name):
        nonlocal phase, phase_started
        result["phases"].append({"phase": phase, "seconds": round(time.monotonic() - phase_started, 3)})
        phase, phase_started = name, time.monotonic()
    def endpoint_views(phase_name, budget, deadline=None):
        def read(endpoint):
            check = session(endpoint, phase_name, budget)
            if deadline is not None:
                check.deadline = min(check.deadline, deadline)
            return target_view(check, bucket, sdk_finished=getattr(deletion, "delete_finished_monotonic", None))
        with ThreadPoolExecutor(max_workers=len(endpoints)) as readers:
            return list(readers.map(read, endpoints))
    def failed(exc, failure_phase):
        if failure_phase != "recovery":
            result["primary_verdict"] = "FAIL"
        result.setdefault("failure_phase", failure_phase)
        result.setdefault("error_code", getattr(exc, "code", type(exc).__name__))
        result.setdefault("failures", []).append({"phase": failure_phase,
            "error_code": getattr(exc, "code", type(exc).__name__)})
    try:
        setup = session(endpoints[0], phase, prepare_budget)
        bucket = prepare(setup, state, "s3-bulk-regression")["bucket"]
        result["bucket"] = bucket
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
        inventory_started = time.monotonic()
        inventory = result["seed_inventory"] = {"prefix": "target/", "started_at": now(), "views": []}
        manifest = set(keys)
        try:
            for endpoint in endpoints:
                view = target_view(session(endpoint, phase, prepare_budget), bucket, manifest)
                inventory["views"].append(view)
                if "error_code" in view:
                    raise RequestFailure(view["error_code"])
                require(view["matches_manifest"], "SeedInventoryMismatch")
        finally:
            inventory.update(finished_at=now(), seconds=round(time.monotonic() - inventory_started, 6))
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
                worker_budget = load_budget + observation_budget if extended else load_budget
                futures.append(pool.submit(load_worker, session(endpoint, "load", worker_budget), bucket,
                                           keys[0] if case_id == "S3C-1307" else CONTROL,
                                           case_id == "S3C-1307", stop, event, release,
                                           32 if case_id == "S3C-1307" else (None if extended else 4096), window, index))
            deadline = time.monotonic() + min(10, load_budget)
            for event, future in zip(ready, futures):
                while not event.is_set() and not future.done() and time.monotonic() < deadline:
                    event.wait(min(0.01, max(0, deadline - time.monotonic())))
            require(all(event.is_set() and not future.done() for event, future in zip(ready, futures)),
                    "LoadNotReady")
        transition("delete")
        deletion = session(endpoints[0], phase, observation_budget if extended else request_budget,
                           read_timeout=observation_budget if extended else None)
        if loaded:
            deletion.window = window
        result.update(primary_verdict="PASS", requested_entries=len(objects), distinct_keys=len(keys))
        release.set()
        delete_start = time.monotonic()
        observation = result["delete_observation"] = {
            "started_at": now(), "endpoint": endpoints[0], "read_timeout_seconds": deletion.read_timeout,
            "reference_timeout_seconds": 10, "sdk_response": "not_received", "logical_outcome": "unknown"}
        try:
            errors = delete_once(deletion, bucket, objects)
            observation.update(sdk_response="complete", logical_outcome="entry_errors" if errors else "acknowledged")
            result.update(delete_error_entries=len(errors), delete_errors=errors)
            if errors:
                failed(RequestFailure("DeleteObjectErrors"), phase)
        except RequestFailure as exc:
            observation["error_code"] = exc.code
            if not hasattr(deletion, "delete_started_at"):
                observation["sdk_response"] = "not_sent"
            elif deletion.delete_http_status is not None and deletion.delete_http_status >= 300:
                observation.update(sdk_response="http_error", logical_outcome="request_error")
            elif deletion.delete_entries is not None or exc.code in ("InvalidS3Response", "InvalidDeleteResponse", "IncompleteDeleteResponse"):
                observation["sdk_response"] = "incomplete"
            failed(exc, phase)
        finally:
            elapsed = getattr(deletion, "delete_seconds", None)
            observation.update(finished_at=now(), sdk_started_at=getattr(deletion, "delete_started_at", None),
                               sdk_finished_at=getattr(deletion, "delete_finished_at", None),
                               seconds=round(elapsed, 6) if elapsed is not None else None,
                               exceeded_reference_timeout=elapsed > 10 if elapsed is not None else None,
                               http_status=deletion.delete_http_status, entries=deletion.delete_entries)
        stop.set()
        if observed:
            transition("verify")
            # Capture the first post-response view before joining outstanding load requests.
            first_views = endpoint_views("first_visibility", request_budget)
            visibility = result["visibility_observation"] = {"prefix": "target/", "rounds": [first_views],
                                                              "first_all_absent_at": None}
        if pool:
            pool.shutdown(wait=True)
            pool = None
        result["load"] = [future.result() for future in futures]
        if loaded:
            result["load_overlap"] = {
                "delete_started_monotonic": window.started, "delete_finished_monotonic": window.finished,
                "successful_requests": sum(row["overlapping_successes"] for row in result["load"]),
                "successful_workers": sum(row["overlapping_successes"] > 0 for row in result["load"]),
            }
            if not result["load_overlap"]["successful_requests"]:
                failed(RequestFailure("LoadDidNotOverlapDelete"), "load")
        if any(row["failures"] or not row["first_success"] for row in result["load"]):
            failed(RequestFailure("LoadRequestFailed"), "load")
        if not observed:
            transition("verify")
        try:
            hot_views = []
            verify_absence = True
            if observed:
                observation_deadline = delete_start + observation_budget
                views = first_views
                while True:
                    view_error = next((view["error_code"] for view in views if "error_code" in view), None)
                    if view_error:
                        raise RequestFailure(view_error)
                    if all(view["visible"] == 0 for view in views):
                        visibility["first_all_absent_at"] = max(view["finished_at"] for view in views)
                        verify_absence = True
                        break
                    if observation["sdk_response"] == "complete":
                        raise RequestFailure("DeleteResidue")
                    verify_absence = False
                    # Give the original 10-second client a bounded, read-only view of a late completion.
                    # A missing reply remains unresolved even if every endpoint later lists an empty prefix.
                    remaining = observation_deadline - time.monotonic()
                    if profile != "client-10s" or observation["sdk_response"] != "not_received" or remaining <= 0:
                        break
                    time.sleep(min(1, remaining))
                    if time.monotonic() >= observation_deadline:
                        break
                    views = endpoint_views("observe", observation_budget, observation_deadline)
                    visibility["rounds"].append(views)
            for endpoint in endpoints:
                check = session(endpoint, phase, request_budget)
                if case_id == "S3C-1307":
                    remaining = listed(check, bucket, prefix="target/")
                    require(all(row["Key"] == keys[0] for row in remaining), "UnrelatedKeysNotDeleted")
                    visible = any(row["Key"] == keys[0] for row in remaining)
                    hot_views.append(visible)
                    if visible:
                        read_exact(check, bucket, keys[0], PAYLOAD)
                elif not observed:
                    require(not listed(check, bucket, prefix="target/"), "DeleteResidue")
                samples = keys[1:] if case_id == "S3C-1307" else keys
                if verify_absence:
                    for key in dict.fromkeys(samples[:1] + samples[-1:]):
                        absent(check, bucket, key)
                read_exact(check, bucket, CONTROL, PAYLOAD)
                for key, version in history.items():
                    read_exact(check, bucket, key, PAYLOAD, version)
            require(len(set(hot_views)) <= 1, "ReplicaViewMismatch")
        except Exception as exc:
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
                cleanup = session(endpoints[0], phase, cleanup_budget)
                cleanup_delete = session(endpoints[0], phase, cleanup_budget,
                                         read_timeout=min(cleanup_budget, observation_budget)) if observed else None
                cleanup_bucket(cleanup, state, result, cleanup_delete)
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


def plan(groups, sizes, modes=MODES):
    return [(case_id, mode, count) for case_id in groups for mode in modes
            for count in (sizes if case_id == "S3C-213" else [32] if case_id == "S3C-214"
                          else [1000] if case_id == "S3C-1306" else [128])]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", action="append", required=True)
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--case", choices=GROUPS, action="append")
    parser.add_argument("--batch-sizes", default="1,32,128,500,1000")
    parser.add_argument("--profile", choices=("compatibility", "client-10s"), default="compatibility",
                        help="observe the S3 response or reproduce the original ten-second client")
    parser.add_argument("--key-shape", choices=KEY_SHAPES, default="short",
                        help="key layout for S3C-213 and S3C-1306")
    parser.add_argument("--mode", choices=MODES, action="append",
                        help="versioning mode to exercise (default: all three)")
    parser.add_argument("--observation-budget", type=int, default=180,
                        help="seconds available to observe a bulk delete in the compatibility profile")
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
                                                        args.load_budget, args.cleanup_budget, args.observation_budget))
                and len(set(args.endpoint)) == len(args.endpoint)
                and all(e.scheme in ("http", "https") and e.hostname and not e.username and not e.password
                        and e.path in ("", "/") and not e.query and not e.fragment for e in endpoints), "InvalidArguments")
        groups = args.case or list(GROUPS)
        require(len(set(groups)) == len(groups), "DuplicateCaseSelection")
        modes = args.mode or MODES
        require(len(set(modes)) == len(modes), "DuplicateModeSelection")
        require(args.key_shape == "short" or all(case in ("S3C-213", "S3C-1306") for case in groups),
                "KeyShapeRequiresBatchOrGetLoadCase")
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
    inventory = plan(groups, sizes, modes)
    for case_id, mode, count in inventory:
        name = f"{case_id}-{mode}-{count}"
        if aborted:
            result = {"case_id": case_id, "mode": mode, "batch_size": count, "verdict": "NOT_RUN"}
        else:
            result = run_case(case_id, mode, count, args.endpoint, args.result_dir / name, args.workers,
                              prepare_budget=args.prepare_budget, request_budget=args.request_budget,
                              load_budget=args.load_budget, cleanup_budget=args.cleanup_budget,
                              profile=args.profile, key_shape=args.key_shape, observation_budget=args.observation_budget)
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
    observed = [row for row in results if "visibility_observation" in row]
    if observed:
        lines.extend(["", "| Case | Mode | Keys | Profile | Shape | SDK seconds | SDK response | Per-key outcome | First all absent (UTC) |",
                      "|---|---|---:|---|---|---:|---|---|---|"])
        for row in observed:
            reply = row["delete_observation"]
            cells = (row["case_id"], row["mode"], row["batch_size"], row["profile"], row["key_shape"],
                     reply["seconds"], reply["sdk_response"], reply["logical_outcome"],
                     row["visibility_observation"]["first_all_absent_at"])
            lines.append("| " + " | ".join("—" if value is None else str(value) for value in cells) + " |")
    (args.result_dir / "summary.md").write_text("\n".join(lines) + "\n")
    return 130 if interrupted else int(aborted or any(row["verdict"] != "PASS" for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
