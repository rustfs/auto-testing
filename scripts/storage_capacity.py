#!/usr/bin/env python3
"""Bound a capacity-filling workload using the least available online disk."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time


def object_bytes(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(B|KiB|MiB|GiB)?", value)
    if not match:
        raise ValueError(f"unsupported object size: {value}")
    return int(match[1]) * {None: 1, "B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[match[2]]


def sample(document: dict, *, now: float | None = None) -> dict:
    disks = document.get("info", document).get("disks")
    if not isinstance(disks, list) or not disks:
        raise ValueError("storageinfo must contain a non-empty disk inventory")
    online = []
    for disk in disks:
        if not isinstance(disk, dict):
            raise ValueError("storageinfo disk must be an object")
        if str(disk.get("state", "")).lower() == "offline":
            continue
        total, used = disk.get("totalspace"), disk.get("usedspace")
        if any(type(value) not in (int, float) or not math.isfinite(value) for value in (total, used)):
            raise ValueError("online disk capacity must be numeric")
        if total <= 0 or used < 0 or used > total:
            raise ValueError("online disk capacity is outside its valid range")
        online.append((total, used))
    if not online:
        raise ValueError("storageinfo has no usable online disks")
    total, used = sum(d[0] for d in online), sum(d[1] for d in online)
    return {
        "sample_time": time.monotonic() if now is None else now,
        "disk_count": len(online), "total_bytes": total, "used_bytes": used,
        "usage_percent": used / total * 100,
        "max_disk_percent": max(d[1] / d[0] * 100 for d in online),
        "min_free_bytes": min(d[0] - d[1] for d in online),
    }


def assess(current: dict, previous: dict | None = None, *, threshold: float = 85,
           max_disk_percent: float = 95, min_free_bytes: int = 512 * 1024**2,
           in_flight_bytes: int, poll_seconds: float = 2, stop_seconds: float = 5,
           sample_timeout: float = 3) -> dict:
    if not 0 < threshold < max_disk_percent <= 100:
        raise ValueError("require 0 < storage threshold < max disk percent <= 100")
    if min_free_bytes <= 0 or in_flight_bytes <= 0 or poll_seconds <= 0 or stop_seconds <= 0 or sample_timeout <= 0:
        raise ValueError("capacity reserve and time bounds must be positive")
    rate = 0.0
    if previous:
        if (current["total_bytes"], current["disk_count"]) != (previous["total_bytes"], previous["disk_count"]):
            raise ValueError("disk inventory changed during capacity filling")
        elapsed = current["sample_time"] - previous["sample_time"]
        if elapsed > 0:
            # Cluster growth is a conservative bound for any one disk's growth;
            # do not assume the workload or EC shards are evenly distributed.
            rate = max(0, current["used_bytes"] - previous["used_bytes"]) / elapsed
    reserve = math.ceil(min_free_bytes + in_flight_bytes + rate * (poll_seconds + stop_seconds + sample_timeout))
    guarded = current["max_disk_percent"] >= max_disk_percent or current["min_free_bytes"] <= reserve
    action = "capacity_guard" if guarded else "target_reached" if current["usage_percent"] >= threshold else "continue"
    return dict(current, action=action, reserve_bytes=reserve, growth_bytes_per_second=rate,
                emergency=current["max_disk_percent"] >= max_disk_percent or current["min_free_bytes"] <= min_free_bytes)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("sample", "assess"))
    parser.add_argument("--previous", default="")
    parser.add_argument("--threshold", type=float, default=85)
    parser.add_argument("--max-disk-percent", type=float, default=95)
    parser.add_argument("--min-free-bytes", type=int, default=512 * 1024**2)
    parser.add_argument("--object-size", default="100MiB")
    parser.add_argument("--concurrent", type=int, default=32)
    parser.add_argument("--poll-seconds", type=float, default=2)
    parser.add_argument("--stop-seconds", type=float, default=5)
    parser.add_argument("--sample-timeout", type=float, default=3)
    args = parser.parse_args()
    try:
        current = json.load(sys.stdin)
        if args.command == "sample":
            result = sample(current)
        else:
            if args.concurrent <= 0:
                raise ValueError("warp concurrency must be positive")
            result = assess(current, json.loads(args.previous) if args.previous else None,
                            threshold=args.threshold, max_disk_percent=args.max_disk_percent,
                            min_free_bytes=args.min_free_bytes,
                            in_flight_bytes=object_bytes(args.object_size) * args.concurrent,
                            poll_seconds=args.poll_seconds, stop_seconds=args.stop_seconds,
                            sample_timeout=args.sample_timeout)
        print(json.dumps(result, separators=(",", ":")))
    except (TypeError, AttributeError, KeyError, ValueError) as exc:
        print(f"[HARNESS-ERROR] capacity monitor: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
