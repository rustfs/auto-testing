"""Strict, side-effect-free validators for pool lifecycle Admin API output."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class PoolStatusError(ValueError):
    """A pool lifecycle response does not satisfy the release-gate contract."""


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise PoolStatusError(f"{field} must be an integer >= {minimum}, got {value!r}")
    return value


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PoolStatusError(f"{field} must be numeric, got {value!r}")
    result = float(value)
    if not 0.0 <= result <= 1.0:
        raise PoolStatusError(f"{field} must be in 0.0..=1.0, got {value!r}")
    return result


def _text(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        qualifier = "a string" if allow_empty else "a non-empty string"
        raise PoolStatusError(f"{field} must be {qualifier}, got {value!r}")
    return value


def _timestamp(value: Any, field: str, *, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as exc:
        raise PoolStatusError(f"{field} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise PoolStatusError(f"{field} must include a timezone")
    return text


def pool_entries(document: Any) -> list[dict[str, Any]]:
    """Return pool entries from raw API, rc list, or rc/API status shapes."""

    raw: Any
    if isinstance(document, list):
        raw = document
    elif isinstance(document, dict) and isinstance(document.get("pools"), list):
        raw = document["pools"]
    elif isinstance(document, dict) and isinstance(document.get("pool"), dict):
        raw = [document["pool"]]
    elif isinstance(document, dict) and "id" in document:
        raw = [document]
    else:
        raise PoolStatusError("pool response must be an array or contain pools/pool")
    if not raw:
        raise PoolStatusError("pool response must contain at least one pool")
    if not all(isinstance(item, dict) for item in raw):
        raise PoolStatusError("every pool response entry must be an object")
    return list(raw)


def canonical_pool(entry: dict[str, Any], *, require_capacity: bool = True) -> dict[str, Any]:
    """Validate and normalize fields shared by pool list and status output."""

    result: dict[str, Any] = {
        "id": _integer(entry.get("id"), "pool.id"),
        "cmdline": _text(entry.get("cmdline"), "pool.cmdline"),
        "status": _text(entry.get("status"), "pool.status"),
        "decommissionStatus": _text(
            entry.get("decommissionStatus"),
            "pool.decommissionStatus",
            allow_empty=True,
        ),
        "rebalanceStatus": _text(
            entry.get("rebalanceStatus"),
            "pool.rebalanceStatus",
            allow_empty=True,
        ),
    }
    _timestamp(entry.get("lastUpdate"), "pool.lastUpdate")
    if require_capacity:
        total = _integer(entry.get("totalSize"), "pool.totalSize")
        current = _integer(entry.get("currentSize"), "pool.currentSize")
        used_size = _integer(entry.get("usedSize"), "pool.usedSize")
        used = _number(entry.get("used"), "pool.used")
        if current > total or used_size > total:
            raise PoolStatusError(
                "pool currentSize/usedSize cannot exceed totalSize: "
                f"total={total} current={current} used={used_size}"
            )
        expected_used_size = total - current
        if used_size != expected_used_size:
            raise PoolStatusError(
                "pool capacity counters are inconsistent: "
                f"total={total} current={current} used={used_size}"
            )
        expected_ratio = used_size / total if total else 0.0
        if abs(used - expected_ratio) > 1e-9:
            raise PoolStatusError(
                "pool used ratio is inconsistent with byte counters: "
                f"used={used} expected={expected_ratio}"
            )
        result.update(
            {
                "totalSize": total,
                "currentSize": current,
                "usedSize": used_size,
                # ``used`` is derived from the integer counters. Independent
                # JSON encoders may emit adjacent decimal representations, so
                # normalize it after validating the supplied ratio above.
                "used": expected_ratio,
            }
        )
    return result


def rc_failure_is_retryable(*streams: str) -> bool:
    """Recognize an rc JSON failure that explicitly permits a bounded retry."""

    if any("requires a live fleet capability proof" in stream for stream in streams):
        return True
    for stream in streams:
        try:
            document = json.loads(stream)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(document, dict):
            continue
        details = document.get("details")
        if isinstance(details, dict) and details.get("retryable") is True:
            return True
    return False


def validate_pool_inventory(
    document: Any,
    expected_count: int,
    *,
    require_active: bool = True,
) -> tuple[dict[str, Any], ...]:
    entries = tuple(canonical_pool(item) for item in pool_entries(document))
    if len(entries) != expected_count:
        raise PoolStatusError(
            f"expected exactly {expected_count} pools, observed {len(entries)}"
        )
    ids = tuple(item["id"] for item in entries)
    if len(ids) != len(set(ids)):
        raise PoolStatusError(f"pool IDs are not unique: {ids}")
    if ids != tuple(sorted(ids)):
        raise PoolStatusError(f"pool IDs are not in stable ascending order: {ids}")
    cmdlines = tuple(item["cmdline"] for item in entries)
    if len(cmdlines) != len(set(cmdlines)):
        raise PoolStatusError("pool command lines are not unique")
    if require_active:
        inactive = [
            item["id"]
            for item in entries
            if item["status"].lower() != "active"
        ]
        if inactive:
            raise PoolStatusError(f"expected active pools, inactive IDs: {inactive}")
    return entries


def assert_pool_parity(
    left: Any,
    right: Any,
    expected_count: int,
    *,
    require_active: bool = True,
) -> None:
    left_entries = validate_pool_inventory(
        left, expected_count, require_active=require_active
    )
    right_entries = validate_pool_inventory(
        right, expected_count, require_active=require_active
    )
    if left_entries != right_entries:
        raise PoolStatusError(
            f"rc/Admin pool status mismatch: rc={left_entries!r} admin={right_entries!r}"
        )


def decommission_entry(document: Any, pool_id: int) -> dict[str, Any]:
    matches = [
        entry
        for entry in pool_entries(document)
        if entry.get("id") == pool_id
    ]
    if len(matches) != 1:
        raise PoolStatusError(
            f"decommission status must contain pool {pool_id} exactly once"
        )
    entry = matches[0]
    _integer(entry.get("id"), "decommission.id")
    _text(entry.get("cmdline"), "decommission.cmdline")
    _text(entry.get("status"), "decommission.status")
    _text(entry.get("poolStatus"), "decommission.poolStatus")
    return entry


def _decommission_info(entry: dict[str, Any]) -> dict[str, Any]:
    info = entry.get("decommissionInfo")
    if not isinstance(info, dict):
        raise PoolStatusError("decommissionInfo must be an object while an operation exists")
    for field in ("complete", "failed", "canceled", "queued"):
        if type(info.get(field)) is not bool:
            raise PoolStatusError(f"decommissionInfo.{field} must be a boolean")
    for field in ("startSize", "totalSize", "currentSize"):
        _optional_counter(info, field)
    for field in (
        "objectsDecommissioned",
        "objectsDecommissionedFailed",
        "bytesDecommissioned",
        "bytesDecommissionedFailed",
    ):
        _optional_counter(info, field)
    for field in ("queuedBuckets", "decommissionedBuckets"):
        value = info.get(field, [])
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise PoolStatusError(f"decommissionInfo.{field} must be a string array")
    for field in ("bucket", "prefix", "object", "stage"):
        _text(info.get(field, ""), f"decommissionInfo.{field}", allow_empty=True)
    _timestamp(
        info.get("startTime"), "decommissionInfo.startTime", allow_none=True
    )
    waiting_reason = info.get("waitingReason")
    if waiting_reason is not None and not isinstance(waiting_reason, str):
        raise PoolStatusError(
            "decommissionInfo.waitingReason must be a string or null"
        )
    unresolved = info.get("unresolvedEntries", [])
    if not isinstance(unresolved, list):
        raise PoolStatusError("decommissionInfo.unresolvedEntries must be an array")
    return info


def _decommission_failure(entry: dict[str, Any]) -> str | None:
    info = entry.get("decommissionInfo")
    if not isinstance(info, dict):
        return None
    _decommission_info(entry)
    status = str(entry.get("status", "")).lower()
    if status in {"failed", "canceled", "cancelled"} or info.get("failed") is True:
        return "decommission reported failed or canceled"
    if _optional_counter(info, "objectsDecommissionedFailed") > 0:
        return "decommission reported failed objects"
    if _optional_counter(info, "bytesDecommissionedFailed") > 0:
        return "decommission reported failed bytes"
    unresolved = info.get("unresolvedEntries", [])
    if unresolved is not None and not isinstance(unresolved, list):
        return "decommission unresolvedEntries is malformed"
    if unresolved:
        return "decommission reported unresolved entries"
    return None


def _optional_counter(document: dict[str, Any], field: str) -> int:
    value = document.get(field, 0)
    return _integer(value, field)


def decommission_active(document: Any, pool_id: int, *, require_progress: bool) -> bool:
    entry = decommission_entry(document, pool_id)
    reason = _decommission_failure(entry)
    if reason:
        raise PoolStatusError(reason)
    info = _decommission_info(entry)
    running = str(entry["status"]).lower() == "running" and str(
        entry["poolStatus"]
    ).lower() == "decommissioning"
    queued = info.get("queued") is True
    if not running or queued or info.get("complete") is True or info.get("canceled") is True:
        return False
    if not require_progress:
        return True
    return (
        _optional_counter(info, "objectsDecommissioned") > 0
        and _optional_counter(info, "bytesDecommissioned") > 0
    )


def decommission_active_or_completed(document: Any, pool_id: int) -> bool:
    """Accept durable movement counters when decommission finishes before polling."""

    return decommission_active(
        document, pool_id, require_progress=True
    ) or decommission_complete(document, pool_id)


def decommission_complete(document: Any, pool_id: int) -> bool:
    entry = decommission_entry(document, pool_id)
    reason = _decommission_failure(entry)
    if reason:
        raise PoolStatusError(reason)
    info = _decommission_info(entry)
    queued_buckets = info.get("queuedBuckets", [])
    if not isinstance(queued_buckets, list):
        raise PoolStatusError("queuedBuckets must be an array")
    terminal = (
        info.get("complete") is True
        and str(entry["status"]).lower() == "complete"
        and str(entry["poolStatus"]).lower() == "decommissioned"
    )
    clean = (
        info.get("failed") is False
        and info.get("canceled") is False
        and info.get("queued") is False
        and not queued_buckets
        and _optional_counter(info, "objectsDecommissionedFailed") == 0
        and _optional_counter(info, "bytesDecommissionedFailed") == 0
    )
    moved = (
        _optional_counter(info, "objectsDecommissioned") > 0
        and _optional_counter(info, "bytesDecommissioned") > 0
    )
    return terminal and clean and moved


def decommission_canceled(document: Any, pool_id: int) -> bool:
    entry = decommission_entry(document, pool_id)
    info = _decommission_info(entry)
    status_canceled = str(entry["status"]).lower() in {
        "canceled",
        "cancelled",
    }
    if info.get("failed") is True:
        raise PoolStatusError("canceled decommission also reports failed=true")
    if info.get("complete") is True:
        raise PoolStatusError("canceled decommission also reports complete=true")
    if (
        _optional_counter(info, "objectsDecommissionedFailed") > 0
        or _optional_counter(info, "bytesDecommissionedFailed") > 0
        or info.get("unresolvedEntries", [])
    ):
        raise PoolStatusError("canceled decommission reports unresolved failures")
    return (
        info.get("canceled") is True
        and status_canceled
        and info.get("queued") is False
        # Cancel retains unfinished buckets for diagnostics; queued is the
        # scheduling flag. Completion, unlike cancellation, drains the list.
        and info.get("startTime") is None
        and str(entry["poolStatus"]).lower() == "blocked"
    )


def decommission_cleared(document: Any, pool_id: int) -> bool:
    entry = decommission_entry(document, pool_id)
    return (
        entry.get("decommissionInfo") is None
        and str(entry["status"]).lower() in {"", "none"}
        and str(entry["poolStatus"]).lower() == "active"
    )


def decommission_multipart_blocked(document: Any, pool_id: int) -> bool:
    """Recognize a clean post-check failure; the caller must prove its MPU cause."""
    entry = decommission_entry(document, pool_id)
    info = _decommission_info(entry)
    for field in ("objectsDecommissioned", "bytesDecommissioned", "objectsDecommissionedFailed", "bytesDecommissionedFailed"):
        _integer(info.get(field), f"MPU drain.{field}")
    if (info["complete"] or info["canceled"] or entry["status"].lower() in {"complete", "canceled", "cancelled"}
            or entry["poolStatus"].lower() == "decommissioned"):
        raise PoolStatusError("MPU drain protection completed or canceled unexpectedly")
    if any(_optional_counter(info, field) for field in ("objectsDecommissionedFailed", "bytesDecommissionedFailed")) or info.get("unresolvedEntries"):
        raise PoolStatusError("MPU drain observation contains unrelated migration failures")
    if info["failed"] or entry["status"].lower() == "failed":
        if not (
            info["failed"] and entry["status"].lower() == "failed"
            and entry["poolStatus"].lower() == "blocked" and not info["queued"]
            and info.get("startTime") is None and info.get("queuedBuckets") == []
            and _optional_counter(info, "objectsDecommissioned") > 0
            and _optional_counter(info, "bytesDecommissioned") > 0
        ):
            raise PoolStatusError("MPU drain failure has inconsistent terminal state")
        return True
    return False


def multipart_drain_failure_records(
    document: Any, *, bucket: str, since: str, node_count: int,
) -> list[dict[str, Any]]:
    """Require fresh, scoped journal evidence before accepting an expected failure."""
    if not isinstance(document, dict) or document.get("mode") != "decommission-journal" or document.get("since") != since:
        raise PoolStatusError("MPU drain journal has the wrong collection identity")
    nodes = document.get("nodes")
    if not isinstance(nodes, dict) or len(nodes) != node_count or not all(isinstance(node, str) and node for node in nodes):
        raise PoolStatusError("MPU drain journal omitted a node")
    _timestamp(since, "MPU journal.since")
    lower_bound = datetime.fromisoformat(since.replace("Z", "+00:00"))
    matches = []
    for node, output in nodes.items():
        if not isinstance(output, str):
            raise PoolStatusError("MPU drain journal node output must be text")
        for line in output.splitlines():
            if not line.startswith("{"):
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PoolStatusError("MPU drain journal contains malformed JSON") from exc
            if not isinstance(record, dict) or record.get("event") != "decommission_state" or record.get("state") != "routine_failed":
                continue
            _timestamp(record.get("timestamp"), "MPU failure.timestamp")
            if datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00")) < lower_bound:
                continue
            error = record.get("error")
            if not (
                type(record.get("pool_index")) is int and record["pool_index"] == 0
                and isinstance(error, str) and "post-check failed:" in error
                and "pool 0 still contains multipart upload `" in error
                and f"` for bucket `{bucket}`; resolve it before retrying decommission" in error
            ):
                raise PoolStatusError("unexpected failure instead of the owned MPU drain protection")
            matches.append({**record, "node": node})
    if not matches:
        raise PoolStatusError("missing fresh MPU drain failure evidence for the owned bucket")
    return matches


def rebalance_entries(
    document: Any,
    expected_id: str,
    *,
    allow_stopping: bool = False,
    allow_stopped: bool = False,
) -> tuple[dict[str, Any], ...]:
    if not isinstance(document, dict):
        raise PoolStatusError("rebalance status must be an object")
    operation_id = _text(document.get("id"), "rebalance.id")
    if operation_id != expected_id:
        raise PoolStatusError(
            f"rebalance operation ID changed: expected {expected_id!r}, got {operation_id!r}"
        )
    pools = document.get("pools")
    if not isinstance(pools, list) or not pools or not all(
        isinstance(pool, dict) for pool in pools
    ):
        raise PoolStatusError("rebalance status must contain non-empty object pools")
    ids: list[int] = []
    for pool in pools:
        ids.append(_integer(pool.get("id"), "rebalance.pool.id"))
        state = _text(pool.get("status"), "rebalance.pool.status")
        _number(pool.get("used"), "rebalance.pool.used")
        stopping = pool.get("stopping")
        if stopping is not None and type(stopping) is not bool:
            raise PoolStatusError("rebalance.pool.stopping must be a boolean")
        if state.lower() in {"failed", "canceled", "cancelled"}:
            raise PoolStatusError(f"rebalance entered unsuccessful state {state!r}")
        if state.lower() == "stopped" and not allow_stopped:
            raise PoolStatusError("rebalance stopped before successful completion")
        if pool.get("stopping") is True and not allow_stopping:
            raise PoolStatusError("rebalance still reports stopping=true")
        last_error = pool.get("lastError")
        if last_error not in {None, ""}:
            raise PoolStatusError(f"rebalance reported lastError={last_error!r}")
        warnings = pool.get("cleanupWarnings")
        if not isinstance(warnings, dict):
            raise PoolStatusError("rebalance cleanupWarnings must be an object")
        warning_count = _optional_counter(warnings, "count")
        warning_details: list[str] = []
        for field in ("lastMsg", "lastBucket", "lastObject", "lastAt"):
            value = warnings.get(field)
            if value is not None and not isinstance(value, str):
                raise PoolStatusError(
                    f"rebalance cleanupWarnings.{field} must be a string or null"
                )
            if isinstance(value, str) and value:
                warning_details.append(value)
        _timestamp(
            warnings.get("lastAt"),
            "rebalance cleanupWarnings.lastAt",
            allow_none=True,
        )
        warning_entries = warnings.get("entries", [])
        if not isinstance(warning_entries, list) or not all(
            isinstance(item, dict) for item in warning_entries
        ):
            raise PoolStatusError(
                "rebalance cleanupWarnings.entries must be an object array"
            )
        if warning_count != 0:
            raise PoolStatusError("rebalance reported cleanup warnings")
        if warning_details or warning_entries:
            raise PoolStatusError(
                "rebalance reported cleanup warning details with count=0"
            )
    if len(ids) != len(set(ids)):
        raise PoolStatusError(f"rebalance pool IDs are duplicated: {ids}")
    propagation = document.get("stopPropagation")
    if propagation is not None:
        if not isinstance(propagation, dict):
            raise PoolStatusError("stopPropagation must be an object")
        for field in ("failedPeers", "terminalReloadFailedPeers"):
            peers = propagation.get(field, [])
            if not isinstance(peers, list) or not all(
                isinstance(peer, str) for peer in peers
            ):
                raise PoolStatusError(
                    f"stopPropagation.{field} must be a string array"
                )
            if peers:
                raise PoolStatusError(f"stopPropagation.{field} is not empty")
        pending_reload = propagation.get("pendingTerminalReload", False)
        if type(pending_reload) is not bool:
            raise PoolStatusError(
                "stopPropagation.pendingTerminalReload must be a boolean"
            )
        if pending_reload:
            raise PoolStatusError("rebalance has a pending terminal reload")
        for field in ("lastAttemptAt", "terminalReloadAttemptAt"):
            _timestamp(
                propagation.get(field),
                f"stopPropagation.{field}",
                allow_none=True,
            )
    stopped_at = document.get("stoppedAt")
    _timestamp(stopped_at, "rebalance.stoppedAt", allow_none=True)
    return tuple(pools)


def _rebalance_progress(pool: dict[str, Any]) -> int:
    progress = pool.get("progress")
    if progress is None:
        return 0
    if not isinstance(progress, dict):
        raise PoolStatusError("rebalance progress must be an object or null")
    moved = sum(
        _optional_counter(progress, field)
        for field in ("objects", "versions", "bytes")
    )
    for field in ("remainingBuckets", "elapsed", "eta"):
        _optional_counter(progress, field)
    for field in ("bucket", "object"):
        _text(progress.get(field, ""), f"rebalance.progress.{field}", allow_empty=True)
    return moved


def rebalance_active(document: Any, expected_id: str, *, require_progress: bool) -> bool:
    pools = rebalance_entries(document, expected_id)
    active = [pool for pool in pools if str(pool["status"]).lower() == "started"]
    if not active:
        return False
    return not require_progress or any(_rebalance_progress(pool) > 0 for pool in active)


def rebalance_active_or_completed(document: Any, expected_id: str) -> bool:
    """Accept observable progress even when a small rebalance finishes before polling."""

    return rebalance_active(
        document, expected_id, require_progress=True
    ) or rebalance_complete(document, expected_id)


def rebalance_complete(document: Any, expected_id: str) -> bool:
    pools = rebalance_entries(document, expected_id)
    completed = [pool for pool in pools if str(pool["status"]).lower() == "completed"]
    all_terminal = all(
        str(pool["status"]).lower() in {"completed", "none"} for pool in pools
    )
    return all_terminal and bool(completed) and any(
        _rebalance_progress(pool) > 0 for pool in completed
    )


def rebalance_stopped(document: Any, expected_id: str) -> bool:
    pools = rebalance_entries(
        document,
        expected_id,
        allow_stopping=True,
        allow_stopped=True,
    )
    stopped_at = document.get("stoppedAt")
    if not isinstance(stopped_at, str) or not stopped_at:
        return False
    return not any(pool.get("stopping") is True for pool in pools) and all(
        str(pool["status"]).lower() in {"stopped", "completed", "none"}
        for pool in pools
    ) and any(str(pool["status"]).lower() == "stopped" for pool in pools)
