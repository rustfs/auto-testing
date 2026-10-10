#!/usr/bin/env python3
"""Run-owned Heal deployment and physical evidence on current auto-testing nodes.

The runner sends this module and a structured request over SSH stdin. Paths,
units and mounts are derived here; no shell program or fault path is accepted.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess


MOUNTS = tuple(Path(f"/data/rustfs{index}") for index in range(1, 5))
STATE_ROOT = Path("/var/lib/rustfs-auto-testing/heal")
RUN_PATTERN = re.compile(r"gh-[1-9][0-9]*-[1-9][0-9]*-(?:h1|h2)")
BACKGROUND_CONFIG = {"RUSTFS_SCANNER_ENABLED": "false", "RUSTFS_HEAL_ENABLED": "true",
                     "RUSTFS_HEAL_AUTO_HEAL_ENABLE": "false", "RUSTFS_HEAL_MRF_ENABLE": "false"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def command(*args, check=True):
    result = subprocess.run(args, capture_output=True, text=True, timeout=90)
    require(not check or result.returncode == 0, f"{args[0]} failed: {result.stderr[-1000:]}")
    return result


def checked(path, boundary):
    require(path.is_absolute() and path.is_relative_to(boundary) and ".." not in path.parts,
            "path is outside the run boundary")
    for entry in (path, *path.parents):
        require(not entry.is_symlink(), "symlink in run path")
    require(path.resolve() == path, "noncanonical run path")
    if boundary.exists():
        for entry in (path, *path.parents):
            if entry == boundary:
                break
            require(not entry.exists() or entry.stat().st_dev == boundary.stat().st_dev,
                    "nested mount in run path")
    return path


def digest(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 2 * 1024**3,
            "expected bounded regular file")
    result = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024**2):
            result.update(chunk)
    return result.hexdigest()


def paths(run_id):
    require(bool(RUN_PATTERN.fullmatch(run_id)), "invalid auto-testing Heal run ID")
    state = checked(STATE_ROOT / run_id, STATE_ROOT)
    roots = tuple(checked(mount / "heal-validation" / run_id, mount) for mount in MOUNTS)
    return state, roots


def unit(run_id):
    paths(run_id)
    return "rustfs-validation-heal-" + run_id + ".service"


def mounts():
    observed = []
    for mount in MOUNTS:
        checked(mount, mount)
        entries = json.loads(command("findmnt", "--json", "--mountpoint", str(mount), "--output",
                                     "TARGET,SOURCE,FSTYPE,UUID,MAJ:MIN,OPTIONS").stdout).get("filesystems", [])
        require(len(entries) == 1, "four explicit disk mountpoints are required")
        value = entries[0]
        require(value["target"] == str(mount) and value.get("uuid") and value.get("maj:min")
                and "rw" in value["options"].split(","), "disk mount identity or write capability is missing")
        observed.append(value)
    require(len({value["uuid"] for value in observed}) == 4
            and len({value["maj:min"] for value in observed}) == 4,
            "four independent drives are required; directories on one drive do not qualify")
    return observed


def preflight(request):
    state, roots = paths(request["runId"])
    require(not state.exists() and all(not root.exists() for root in roots), "run namespace already exists")
    observed = mounts()
    require(all(shutil.disk_usage(mount).free >= 1024**3 for mount in MOUNTS), "1 GiB free per drive required")
    return {"hostname": socket.gethostname(), "mounts": observed, "statePath": str(state),
            "dataRoots": [str(root / "data") for root in roots], "mutated": False}


def reserve(request):
    run_id = request["runId"]
    observed = preflight(request)
    require(observed["hostname"] == request["hostname"] and observed["mounts"] == request["mounts"],
            "preflight host or disk identities changed")
    require(command("systemctl", "is-active", "--quiet", "rustfs", check=False).returncode != 0,
            "shared RustFS service must be stopped before allocating port 9000")
    listeners = command("ss", "-H", "-lnt").stdout
    require(not any(len(parts := line.split()) >= 4 and parts[3].endswith(":9000")
                    for line in listeners.splitlines()), "port 9000 remains occupied")
    binary_sha = digest(Path("/usr/bin/rustfs"))
    require(binary_sha == request["binarySha256"], "installed candidate binary differs")
    for name in ("access", "secret"):
        require(bool(re.fullmatch(r"[A-Za-z0-9]{16,80}", request[name])), "invalid runtime credential")
    volumes = request["volumes"]
    require(isinstance(volumes, list) and len(volumes) in {4, 16}, "unsupported automatic Heal topology")
    local_paths = [str(root / "data") for root in paths(run_id)[1]]
    h1 = run_id.endswith("-h1")
    expected = local_paths if h1 else [f"http://rustfs-node{node}:9000{path}"
                                      for node in range(1, 5) for path in local_paths]
    require(volumes == expected, "endpoint sequence differs from H1/H2 deployment contract")
    state, roots = paths(run_id)
    owner = {"runId": run_id, "unit": unit(run_id), "hostname": observed["hostname"],
             "mounts": observed["mounts"], "binarySha256": binary_sha,
             "bucket": request["bucket"], "volumes": volumes}
    if request.get("inspectorSha256"):
        require(bool(re.fullmatch(r"[0-9a-f]{64}", request["inspectorSha256"])), "invalid inspector digest")
        owner["inspectorSha256"] = request["inspectorSha256"]
    require(bool(re.fullmatch(r"rv-heal-[a-f0-9]{16}", owner["bucket"])), "invalid run bucket")
    state.mkdir(parents=True, mode=0o700)
    (state / "owner.json").write_text(json.dumps(owner))
    for root in roots:
        root.mkdir(parents=True, mode=0o700)
        (root / "owner.json").write_text(json.dumps(owner))
        (root / "data").mkdir(mode=0o700)
        (root / "quarantine").mkdir(mode=0o700)
    shutil.copyfile("/usr/bin/rustfs", state / "rustfs")
    (state / "rustfs").chmod(0o500)
    require(digest(state / "rustfs") == binary_sha, "copied candidate binary differs")
    config = {**BACKGROUND_CONFIG, "RUSTFS_ACCESS_KEY": request["access"], "RUSTFS_SECRET_KEY": request["secret"],
              "RUSTFS_ADDRESS": ":9000", "RUSTFS_CONSOLE_ENABLE": "false",
              "RUSTFS_HEAL_TASK_TIMEOUT_SECS": "300", "RUSTFS_OBS_LOGGER_LEVEL": "info"}
    env_file = state / "service.env"
    descriptor = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write("\n".join(f"{key}={value}" for key, value in config.items()) + "\n")
    return {"owner": owner, "reserved": True}


def owned(run_id):
    state, roots = paths(run_id)
    marker = checked(state / "owner.json", state)
    require(marker.stat().st_uid == 0 and marker.stat().st_nlink == 1, "root-owned marker required")
    owner = json.loads(marker.read_text())
    require(owner["runId"] == run_id and owner["unit"] == unit(run_id)
            and owner["hostname"] == socket.gethostname(), "run owner or hostname differs")
    current = mounts()
    for actual, previous in zip(current, owner["mounts"], strict=True):
        require(all(actual[key] == previous[key] for key in ("target", "uuid", "maj:min", "source", "fstype")),
                "disk identity changed")
    for root in roots:
        marker = checked(root / "owner.json", root)
        require(marker.stat().st_uid == 0 and json.loads(marker.read_text()) == owner, "drive owner differs")
    return state, roots, owner


def start(request):
    state, _, owner = owned(request["runId"])
    require(digest(state / "rustfs") == owner["binarySha256"], "candidate changed before start")
    command("systemd-run", "--unit=" + owner["unit"], "--property=Type=exec",
            "--property=KillMode=control-group", "--property=TimeoutStopSec=30",
            "--property=RuntimeMaxSec=1800", "--property=MemoryMax=4G",
            "--property=WorkingDirectory=" + str(state),
            "--property=EnvironmentFile=" + str(state / "service.env"), str(state / "rustfs"), *owner["volumes"])
    return {"unit": owner["unit"], "started": True}


def install_inspector(request):
    state, _, owner = owned(request["runId"])
    tool = checked(state / "heal-meta-inspect", state)
    require(digest(tool) == owner.get("inspectorSha256"), "uploaded inspector digest differs")
    tool.chmod(0o500)
    return {"inspectorSha256": owner["inspectorSha256"], "installed": True}


def snapshot(request):
    state, roots, owner = owned(request["runId"])
    pid = int(command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"]).stdout.strip())
    require(pid > 0, "run-owned service has no live PID")
    executable = Path(f"/proc/{pid}/exe").resolve(strict=True)
    require(executable == state / "rustfs" and digest(executable) == owner["binarySha256"],
            "running binary differs from candidate")
    environment = dict(value.split("=", 1) for value in Path(f"/proc/{pid}/environ").read_bytes().decode().split("\0")
                       if "=" in value)
    require(all(environment.get(key) == value for key, value in BACKGROUND_CONFIG.items()),
            "background repair isolation configuration differs")
    files, formats = [], []
    tool = state / "heal-meta-inspect"
    if owner.get("inspectorSha256"):
        require(digest(checked(tool, state)) == owner["inspectorSha256"], "metadata inspector changed")
    for index, root in enumerate(roots):
        data = root / "data"
        folder = checked(data / owner["bucket"], data)
        if folder.exists():
            for directory, children, names in os.walk(folder, followlinks=False):
                for name in (*children, *names):
                    checked(Path(directory) / name, data)
                for name in sorted(names):
                    path = Path(directory) / name
                    require(len(files) < 4096 and path.stat().st_size <= 64 * 1024**2, "fixture exceeds physical evidence budget")
                    entry = {"disk": index, "path": str(path.relative_to(data)),
                             "size": path.stat().st_size, "sha256": digest(path)}
                    if path.name == "xl.meta" and owner.get("inspectorSha256"):
                        key = path.parent.relative_to(folder).as_posix()
                        decoded = json.loads(command(str(tool), str(path), owner["bucket"], key).stdout)
                        require(decoded.get("bucket") == owner["bucket"] and decoded.get("key") == key
                                and decoded.get("physicalBitrotVerified") is False,
                                "metadata decoder identity or verification role differs")
                        entry["metadata"] = decoded
                    files.append(entry)
        path = checked(data / ".rustfs.sys/format.json", data)
        require(path.stat().st_size < 65536, "disk format exceeds budget")
        formats.append({"disk": index, "sha256": digest(path), "value": json.loads(path.read_text())})
    return {"hostname": owner["hostname"], "unit": owner["unit"], "pid": pid,
            "binarySha256": owner["binarySha256"], "backgroundConfig": BACKGROUND_CONFIG,
            "files": sorted(files, key=lambda value: (value["disk"], value["path"])), "formats": formats}


def logs(request):
    _, _, owner = owned(request["runId"])
    return {"unit": owner["unit"], "journal": command("journalctl", "-u", owner["unit"], "--no-pager", "-n", "2000").stdout}


def cleanup(request):
    state, roots, owner = owned(request["runId"])
    command("systemctl", "stop", owner["unit"], check=False)
    pid = command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"], check=False).stdout.strip()
    require(pid in {"", "0"}, "run-owned service remains active")
    command("systemctl", "reset-failed", owner["unit"], check=False)
    for root in (*roots, state):
        for directory, children, names in os.walk(root, followlinks=False):
            for name in (*children, *names):
                checked(Path(directory) / name, root)
        shutil.rmtree(root)
    require(not state.exists() and all(not root.exists() for root in roots), "run namespace remains")
    return {"cleanup": "PASS", "unitStopped": True, "rootsRemoved": True, "hostname": owner["hostname"]}


def main(request):
    require(os.geteuid() == 0, "node adapter requires passwordless sudo")
    actions = {"preflight": preflight, "reserve": reserve, "start": start, "snapshot": snapshot,
               "logs": logs, "cleanup": cleanup, "install-inspector": install_inspector}
    require(request.get("action") in actions, "unsupported node operation")
    print(json.dumps(actions[request["action"]](request)))
