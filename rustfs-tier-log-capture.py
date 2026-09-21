#!/usr/bin/env python3
"""Synchronous, race-safe stdout/file fan-out for rustfs-tier-test.sh."""

from __future__ import annotations

import argparse
import errno
import os
import secrets
import select
import signal
import stat
import sys
import tempfile
from typing import Optional, Tuple


class LogOpenError(Exception):
    pass


def _open_directory(path: str, *, create: bool) -> int:
    absolute = os.path.abspath(path)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    current = os.open("/", flags)
    try:
        for component in (part for part in absolute.split(os.sep) if part):
            try:
                child = os.open(component, flags, dir_fd=current)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(component, 0o700, dir_fd=current)
                except FileExistsError:
                    pass
                child = os.open(component, flags, dir_fd=current)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def _open_requested(path: str) -> int:
    absolute = os.path.abspath(path)
    parent, name = os.path.split(absolute)
    if not name:
        raise LogOpenError("the log path does not name a file")

    directory = _open_directory(parent, create=True)
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_CLOEXEC
            | os.O_NOFOLLOW
            | os.O_NONBLOCK
        )
        descriptor = os.open(name, flags, 0o600, dir_fd=directory)
    finally:
        os.close(directory)

    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise LogOpenError("the log destination is not a regular file")
        if metadata.st_uid != os.geteuid():
            raise LogOpenError("the log destination is owned by another user")
        if metadata.st_nlink != 1:
            raise LogOpenError("the log destination has multiple hard links")
        os.set_blocking(descriptor, True)
        os.fchmod(descriptor, 0o600)
        os.ftruncate(descriptor, 0)
        os.lseek(descriptor, 0, os.SEEK_SET)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_fallback() -> Tuple[int, str]:
    fallback_dir = os.environ.get("TMPDIR") or tempfile.gettempdir()
    descriptor, path = tempfile.mkstemp(prefix="rustfs-tier.log.", dir=fallback_dir)
    os.fchmod(descriptor, 0o600)
    return descriptor, path


def _select_log(path: str) -> Tuple[int, Optional[str]]:
    try:
        return _open_requested(path), None
    except (LogOpenError, OSError) as requested_error:
        try:
            descriptor, fallback = _open_fallback()
        except OSError as fallback_error:
            raise LogOpenError(
                f"cannot safely open {path!r} ({requested_error}); "
                f"fallback creation failed ({fallback_error})"
            ) from fallback_error
        return descriptor, fallback


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError(errno.EIO, "short write")
        view = view[written:]


def _publish_ready(path: str) -> None:
    temporary = f"{path}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        _write_all(descriptor, b"READY\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def _stop_requested(path: str) -> bool:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(metadata.st_mode):
        raise OSError(errno.EPERM, "log stop path is not a regular file")
    if metadata.st_uid != os.geteuid() or metadata.st_nlink != 1:
        raise OSError(errno.EPERM, "log stop path is not privately owned")
    return True


def _fan_out(log_descriptor: int, stop_file: str) -> None:
    input_descriptor = sys.stdin.fileno()
    os.set_blocking(input_descriptor, False)
    # Orphaned producers that inherited the FIFO write end can keep the inner
    # read loop busy indefinitely; a terminal signal is the only way out then.
    stopped = {"requested": False}

    def _on_terminate(_signum, _frame):
        stopped["requested"] = True

    previous_term = signal.signal(signal.SIGTERM, _on_terminate)
    previous_int = signal.signal(signal.SIGINT, _on_terminate)
    try:
        while True:
            for _ in range(64):
                try:
                    chunk = os.read(input_descriptor, 65536)
                except BlockingIOError:
                    break
                if not chunk:
                    if _stop_requested(stop_file):
                        os.fsync(log_descriptor)
                        return
                    raise OSError(
                        errno.EPIPE, "log stream ended before stop was requested"
                    )
                _write_all(log_descriptor, chunk)
                _write_all(sys.stdout.fileno(), chunk)
                # Drain in bounded batches so a busy producer cannot starve
                # the stop-file poll below.
                if _stop_requested(stop_file):
                    os.fsync(log_descriptor)
                    return
            if stopped["requested"] or _stop_requested(stop_file):
                os.fsync(log_descriptor)
                return
            select.select([input_descriptor], [], [], 0.1)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--status-file", required=True)
    parser.add_argument("--stop-file", required=True)
    args = parser.parse_args()

    try:
        log_descriptor, fallback = _select_log(args.log_file)
    except LogOpenError as error:
        print(f"[ERROR] secure log capture: {error}", file=sys.stderr)
        return 73

    try:
        if fallback:
            print(
                f"[WARN] log file is unsafe or not writable ({args.log_file}); "
                f"using {fallback}",
                file=sys.stderr,
                flush=True,
            )
        _publish_ready(args.status_file)
        _fan_out(log_descriptor, args.stop_file)
    except OSError as error:
        print(f"[ERROR] secure log capture failed: {error}", file=sys.stderr)
        return 74
    finally:
        os.close(log_descriptor)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
