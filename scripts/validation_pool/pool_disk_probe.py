#!/usr/bin/env python3
"""Read-only, descriptor-scoped census and allowlisted pool metadata evidence."""

from __future__ import annotations

import argparse
import base64
import bisect
import errno
import hashlib
import json
import os
import stat
import time
from pathlib import Path, PurePosixPath
from typing import Any


DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
CACHE_OBJECTS = {".usage-cache.bin", ".usage-cache.bin.bkp"}
DIAGNOSTIC_OBJECTS = (
    ".rustfs.sys/buckets/.scanner-pause-backlog.json",
    ".rustfs.sys/pool.bin",
)
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_OBJECT_BYTES = 16 * 1024 * 1024


class CensusEntryMissing(FileNotFoundError):
    """A specific child disappeared while its anchored parent was being read."""


def _cache_path(relative: str) -> bool:
    parts = PurePosixPath(relative).parts
    return (
        len(parts) >= 3
        and parts[:2] == (".rustfs.sys", "buckets")
        and parts[2] in CACHE_OBJECTS
    )


def _check_root(root: Path, descriptor: int) -> os.stat_result:
    expected = os.fstat(descriptor)
    observed = root.lstat()
    if not stat.S_ISDIR(observed.st_mode) or (
        observed.st_dev, observed.st_ino
    ) != (expected.st_dev, expected.st_ino):
        raise RuntimeError("census root identity changed")
    return expected


def _census_once(descriptor: int) -> dict[str, Any]:
    device = os.fstat(descriptor).st_dev
    result: dict[str, Any] = {
        "files": 0, "directories": 1, "xlMeta": 0, "bytes": 0, "samples": [],
    }

    def walk(parent: int, prefix: str, depth: int) -> None:
        if depth > 128:
            raise RuntimeError("census directory depth exceeds 128")
        with os.scandir(parent) as entries:
            for entry in entries:
                relative = f"{prefix}/{entry.name}" if prefix else entry.name
                try:
                    info = os.stat(entry.name, dir_fd=parent, follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        if info.st_dev != device:
                            raise RuntimeError(f"unexpected nested mount: {relative}")
                        child = os.open(entry.name, DIRECTORY_FLAGS, dir_fd=parent)
                        try:
                            opened = os.fstat(child)
                            if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                                raise RuntimeError(f"directory identity changed: {relative}")
                            result["directories"] += 1
                            walk(child, relative, depth + 1)
                        finally:
                            os.close(child)
                    elif stat.S_ISREG(info.st_mode):
                        result["files"] += 1
                        result["bytes"] += info.st_size
                        result["xlMeta"] += int(entry.name == "xl.meta")
                        bisect.insort(result["samples"], (relative, info.st_size))
                        del result["samples"][20:]
                except CensusEntryMissing:
                    raise
                except FileNotFoundError as exc:
                    raise CensusEntryMissing(errno.ENOENT, "entry disappeared", relative) from exc

    walk(descriptor, "", 0)
    return result


def census(root: Path, *, attempts: int = 3) -> dict[str, Any]:
    if attempts < 1:
        raise ValueError("census attempts must be positive")
    descriptor = os.open(root, DIRECTORY_FLAGS)
    races: list[str] = []
    try:
        for attempt in range(1, attempts + 1):
            _check_root(root, descriptor)
            try:
                result = _census_once(descriptor)
            except CensusEntryMissing as exc:
                _check_root(root, descriptor)
                if not _cache_path(str(exc.filename)) or attempt == attempts:
                    raise
                races.append(str(exc.filename))
                time.sleep(0.05)
                continue
            _check_root(root, descriptor)
            result.update({
                "sampling": "non-atomic-online",
                "attempts": attempt,
                "retriedCachePaths": races,
            })
            return result
        raise AssertionError("unreachable census retry state")
    finally:
        os.close(descriptor)


def metadata_snapshot(root: Path) -> dict[str, Any]:
    """Read only the two non-credential objects needed for migration diagnosis."""
    descriptor = os.open(root, DIRECTORY_FLAGS)
    records: dict[str, Any] = {}
    try:
        _check_root(root, descriptor)
        for object_name in DIAGNOSTIC_OBJECTS:
            parent = os.dup(descriptor)
            record: dict[str, Any] = {"files": []}
            records[object_name] = record
            try:
                for component in PurePosixPath(object_name).parts:
                    child = os.open(component, DIRECTORY_FLAGS, dir_fd=parent)
                    os.close(parent)
                    parent = child
                total = 0
                for name in sorted(os.listdir(parent)):
                    # Object metadata and inline/single-part payloads only.
                    file_descriptor = os.open(
                        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
                    )
                    try:
                        before = os.fstat(file_descriptor)
                        if not stat.S_ISREG(before.st_mode):
                            continue
                        if before.st_size > MAX_FILE_BYTES or total + before.st_size > MAX_OBJECT_BYTES:
                            record["files"].append({"name": name, "state": "size-limit"})
                            continue
                        chunks = []
                        remaining = MAX_FILE_BYTES + 1
                        while remaining:
                            block = os.read(file_descriptor, min(65536, remaining))
                            if not block:
                                break
                            chunks.append(block)
                            remaining -= len(block)
                        data = b"".join(chunks)
                        if len(data) > MAX_FILE_BYTES or total + len(data) > MAX_OBJECT_BYTES:
                            raise RuntimeError("diagnostic file grew past size limit")
                        after = os.fstat(file_descriptor)
                        total += len(data)
                        record["files"].append({
                            "name": name,
                            "bytes": len(data),
                            "sha256": hashlib.sha256(data).hexdigest(),
                            "mtimeNs": before.st_mtime_ns,
                            "stableDuringRead": (
                                before.st_size, before.st_mtime_ns, before.st_ctime_ns
                            ) == (after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                            "base64": base64.b64encode(data).decode("ascii"),
                        })
                    finally:
                        os.close(file_descriptor)
                record["state"] = "captured"
            except OSError as exc:
                record.update({"state": "unavailable", "errno": exc.errno})
            finally:
                os.close(parent)
        _check_root(root, descriptor)
        return {"sampling": "non-atomic-online", "objects": records}
    finally:
        os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("census", "metadata"))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    result = census(args.root) if args.mode == "census" else metadata_snapshot(args.root)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
