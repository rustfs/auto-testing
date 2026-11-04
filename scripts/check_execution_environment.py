#!/usr/bin/env python3
"""Report whether a host can execute the live suites without changing them."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import socket
import subprocess
from pathlib import Path


def result(name: str, status: str, detail: str) -> dict:
    return {"name": name, "status": status, "detail": detail}


def command_check(command: str) -> dict:
    path = shutil.which(command)
    if path:
        return result(f"command:{command}", "pass", path)
    return result(f"command:{command}", "blocked", "command not found")


def bash_version_check(minimum_major: int) -> dict:
    executable = shutil.which("bash")
    if not executable:
        return result("bash:version", "blocked", "bash command not found")
    try:
        completed = subprocess.run(
            [executable, "--version"], capture_output=True, text=True,
            timeout=10, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return result("bash:version", "blocked", str(exc))
    match = re.search(r"version\s+(\d+)\.(\d+)\.(\d+)", completed.stdout)
    if completed.returncode or not match:
        return result("bash:version", "blocked", completed.stderr.strip() or "unknown bash version")
    version = tuple(int(part) for part in match.groups())
    if version[0] < minimum_major:
        return result(
            "bash:version", "blocked",
            f"{version[0]}.{version[1]}.{version[2]} requires major >= {minimum_major}",
        )
    return result(
        "bash:version", "pass",
        f"{version[0]}.{version[1]}.{version[2]} major >= {minimum_major}",
    )


def aws_list_objects_check(command: str = "aws") -> dict:
    executable = shutil.which(command)
    if not executable:
        return result("aws:list-objects-v2", "blocked", "aws command not found")
    try:
        completed = subprocess.run(
            [executable, "s3api", "list-objects-v2", "help"],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return result("aws:list-objects-v2", "blocked", str(exc))
    detail = (completed.stderr or completed.stdout).strip()
    if completed.returncode == 0:
        return result("aws:list-objects-v2", "pass", "CLI parser accepted the operation")
    if "badly formed help string" in detail.lower():
        return result(
            "aws:list-objects-v2", "blocked",
            "AWS CLI cannot render list-objects-v2 help; install a compatible AWS CLI v2",
        )
    return result(
        "aws:list-objects-v2", "blocked",
        detail[-1000:] or f"aws exited {completed.returncode}",
    )


def port_check(port: int, host: str = "0.0.0.0") -> dict:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind((host, port))
    except OSError as exc:
        return result(f"port:{port}", "blocked", str(exc))
    return result(f"port:{port}", "pass", f"available on {host}")


def drive_layout_check(
    data_root: Path, count: int, minimum_free_gib: float, distinct_devices: bool,
) -> list[dict]:
    checks = []
    devices: dict[int, Path] = {}
    for index in range(1, count + 1):
        drive = data_root.with_name(data_root.name + str(index))
        name = f"drive:{drive}"
        if not drive.is_dir() or drive.is_symlink():
            checks.append(result(name, "blocked", "missing directory or symlink"))
            continue
        stat = drive.stat()
        free_gib = shutil.disk_usage(drive).free / (1024 ** 3)
        if free_gib < minimum_free_gib:
            checks.append(result(
                name, "blocked",
                f"free={free_gib:.2f}GiB minimum={minimum_free_gib:.2f}GiB",
            ))
            continue
        if distinct_devices and stat.st_dev in devices:
            checks.append(result(
                name, "blocked",
                f"shares device with {devices[stat.st_dev]}",
            ))
            continue
        devices[stat.st_dev] = drive
        checks.append(result(
            name, "pass",
            f"device={stat.st_dev} free={free_gib:.2f}GiB",
        ))
    return checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--command", action="append", default=[])
    parser.add_argument("--minimum-bash-major", type=int, default=0)
    parser.add_argument("--check-aws-list-objects", action="store_true")
    parser.add_argument("--data-root", type=Path, default=Path("/data/rustfs"))
    parser.add_argument("--drive-count", type=int, default=0)
    parser.add_argument("--minimum-free-gib", type=float, default=0)
    parser.add_argument("--require-distinct-devices", action="store_true")
    parser.add_argument("--require-port", action="append", type=int, default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    checks = [command_check(command) for command in args.command]
    if args.minimum_bash_major:
        checks.append(bash_version_check(args.minimum_bash_major))
    if args.check_aws_list_objects:
        checks.append(aws_list_objects_check())
    checks.extend(port_check(port) for port in args.require_port)
    if args.drive_count:
        checks.extend(drive_layout_check(
            args.data_root,
            args.drive_count,
            args.minimum_free_gib,
            args.require_distinct_devices,
        ))
    blocked = [check for check in checks if check["status"] != "pass"]
    print(json.dumps({"ready": not blocked, "checks": checks}, indent=2))
    return 0 if not blocked else 2


if __name__ == "__main__":
    raise SystemExit(main())
