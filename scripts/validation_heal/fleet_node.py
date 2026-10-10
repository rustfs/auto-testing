#!/usr/bin/env python3
"""Run-owned Heal deployment and physical evidence on current auto-testing nodes.

The runner sends this module and a structured request over SSH stdin. Paths,
units and mounts are derived here; no shell program or fault path is accepted.
"""

from __future__ import annotations

import hashlib
import base64
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import signal
import shutil
import socket
import stat
import subprocess
import time
import uuid


MOUNTS = tuple(Path(f"/data/rustfs{index}") for index in range(1, 5))
STATE_ROOT = Path("/var/lib/rustfs-auto-testing/heal")
RUN_PATTERN = re.compile(r"gh-[1-9][0-9]*-[1-9][0-9]*-(?:h1|h2)")
BACKGROUND_CONFIG = {"RUSTFS_SCANNER_ENABLED": "false", "RUSTFS_HEAL_ENABLED": "true",
                     "RUSTFS_HEAL_AUTO_HEAL_ENABLE": "false", "RUSTFS_HEAL_MRF_ENABLE": "false",
                     "RUSTFS_SHARD_INTEGRITY_WRITE": "true", "RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED": "true",
                     "RUSTFS_OBS_LOGGER_LEVEL": "info"}


def runtime_config(profile):
    require(profile in {"manual", "offline-replay", "durable-mrf", "read-repair", "scanner", "disabled", "overlap", "root-restart", "stale-rejoin", "replacement", "replacement-target", "scope-isolation"}, "unsupported Heal runtime profile")
    if profile == "scope-isolation":
        return {**BACKGROUND_CONFIG, "RUSTFS_ERASURE_SET_DRIVE_COUNT": "8"}
    if profile == "durable-mrf":
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_MRF_ENABLE": "true", "RUSTFS_OBS_LOGGER_LEVEL": "debug"}
    if profile in {"replacement", "replacement-target"}:
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_AUTO_HEAL_ENABLE": "true" if profile == "replacement" else "false", "RUSTFS_HEAL_INTERVAL_SECS": "10",
                "RUSTFS_UNSAFE_BYPASS_DISK_CHECK": "true", "RUSTFS_OBS_LOGGER_LEVEL": "debug"}
    if profile == "stale-rejoin":
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS": "0", "RUSTFS_OBS_LOGGER_LEVEL": "debug"}
    if profile == "root-restart":
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_PAGE_PARALLEL_ENABLE": "false"}
    if profile == "overlap":
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_OVERLAP_POLICY": "merge",
                "RUSTFS_HEAL_PAGE_PARALLEL_ENABLE": "false"}
    if profile == "disabled":
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_ENABLED": "false"}
    if profile == "scanner":
        return {**BACKGROUND_CONFIG, "RUSTFS_SCANNER_ENABLED": "true", "RUSTFS_OBS_LOGGER_LEVEL": "debug",
                "RUSTFS_SCANNER_START_DELAY_SECS": "5", "RUSTFS_SCANNER_CYCLE": "5",
                "RUSTFS_SCANNER_SPEED": "fastest", "RUSTFS_SCANNER_DELAY": "0",
                "RUSTFS_SCANNER_BITROT_CYCLE_SECS": "0", "RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS": "0",
                "RUSTFS_HEAL_OBJECT_SELECT_PROB": "1", "RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES": "1"}
    return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_MRF_ENABLE": "true" if profile == "offline-replay" else "false",
            "RUSTFS_OBS_LOGGER_LEVEL": "debug" if profile == "read-repair" else "info"}


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


def resource_limits(profile):
    require(profile in {"standard", "cursor", "full-restart"}, "unsupported Heal resource profile")
    # DX has 1,025 external objects on four local roots. The candidate keeps
    # an xl.meta, part and integrity sidecar for each: 12,300 files even
    # without backups. Retain every sidecar within an explicit allowance.
    # F05 combines the 1,025-object cursor fixture with DB, DV and completed
    # MPU objects. Each of four local roots also retains metadata backups.
    # Its combined census needs a distinct, still bounded file allowance.
    return {"maxSnapshotFiles": {"standard": 4096, "cursor": 16384, "full-restart": 24576}[profile],
            "minimumDriveFreeBytes": (1 if profile == "standard" else 2) * 1024**3,
            "runtimeMaxSeconds": 1800 if profile == "standard" else 2700}


def preflight(request):
    state, roots = paths(request["runId"])
    require(not state.exists() and all(not root.exists() for root in roots), "run namespace already exists")
    observed = mounts()
    limits = resource_limits(request.get("resourceProfile", "standard"))
    require(all(shutil.disk_usage(mount).free >= limits["minimumDriveFreeBytes"] for mount in MOUNTS),
            "insufficient free space for the explicit Heal resource profile")
    return {"hostname": socket.gethostname(), "mounts": observed, "statePath": str(state),
            "dataRoots": [str(root / "data") for root in roots], "resourceLimits": limits, "mutated": False}


def reserve(request):
    run_id = request["runId"]
    profile = request.get("runtimeProfile", "manual")
    background = runtime_config(profile)
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
    if profile == "scope-isolation":
        require(not h1, "two-set runtime requires the four-node topology")
        expected = [f"http://rustfs-node{node}:9000{path}" for path in local_paths for node in range(1, 5)]
    require(volumes == expected, "endpoint sequence differs from H1/H2 deployment contract")
    state, roots = paths(run_id)
    owner = {"runId": run_id, "unit": unit(run_id), "hostname": observed["hostname"],
             "mounts": observed["mounts"], "binarySha256": binary_sha,
             "bucket": request["bucket"], "volumes": volumes, "runtimeProfile": profile,
             "resourceProfile": request.get("resourceProfile", "standard")}
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
    config = {**background, "RUSTFS_ACCESS_KEY": request["access"], "RUSTFS_SECRET_KEY": request["secret"],
              "RUSTFS_ADDRESS": ":9000", "RUSTFS_CONSOLE_ENABLE": "false",
              "RUSTFS_HEAL_TASK_TIMEOUT_SECS": "300"}
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
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    command("systemctl", "reset-failed", owner["unit"], check=False)
    require(digest(state / "rustfs") == owner["binarySha256"], "candidate changed before start")
    command("systemd-run", "--unit=" + owner["unit"], "--property=Type=exec",
            "--property=KillMode=control-group", "--property=TimeoutStopSec=30",
            "--property=RuntimeMaxSec=" + str(resource_limits(owner.get("resourceProfile", "standard"))["runtimeMaxSeconds"]),
            "--property=MemoryMax=4G",
            *(["--property=RemainAfterExit=yes"] if owner.get("runtimeProfile") in {"root-restart", "replacement", "replacement-target"} else []),
            "--property=WorkingDirectory=" + str(state),
            "--property=EnvironmentFile=" + str(state / "service.env"),
            *gate_module().start_properties(gate_module().checked_gate(state, roots)),
            *replacement_module().start_properties(state, roots), str(state / "rustfs"), *owner["volumes"])
    return {"unit": owner["unit"], "started": True}


def stopped(owner):
    values = command("systemctl", "show", owner["unit"], "-p", "MainPID", "-p", "ActiveState",
                     "-p", "ControlGroup", check=False).stdout
    state = dict(line.split("=", 1) for line in values.splitlines() if "=" in line)
    require(state.get("MainPID") == "0" and state.get("ActiveState") in {"inactive", "failed"},
            "file operations require the run-owned service to be stopped")
    group = state.get("ControlGroup", "")
    if group:
        require(group.startswith("/") and ".." not in Path(group).parts, "invalid systemd control group")
        root = Path("/sys/fs/cgroup") / group.lstrip("/")
        if root.exists():
            require(all(not entry.read_text().strip() for entry in root.rglob("cgroup.procs")),
                    "run-owned control group still has live processes")
    return state


def stop(request):
    _, _, owner = owned(request["runId"])
    command("systemctl", "stop", owner["unit"], check=False)
    return {"unit": owner["unit"], "quiescent": True, "service": stopped(owner)}


PROCESS_FIELDS = ("pid", "startTicks", "bootId", "unit", "executable", "binarySha256", "mountNamespace",
                  "commandSha256", "environmentFileSha256", "settings")


def process_once(request):
    state, _, owner = owned(request["runId"])
    fields = ("MainPID", "ActiveState", "SubState", "ExecMainCode", "ExecMainStatus", "Result")
    service = dict(line.split("=", 1) for line in command("systemctl", "show", owner["unit"],
        *(part for name in fields for part in ("-p", name)), check=False).stdout.splitlines() if "=" in line)
    pid = int(service.get("MainPID", "0"))
    base = {"pid": pid, "unit": owner["unit"], "service": service,
            "bootId": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "binarySha256": digest(state / "rustfs"), "environmentFileSha256": digest(state / "service.env")}
    require(base["binarySha256"] == owner["binarySha256"], "process candidate changed")
    if pid == 0:
        return base
    require(pid > 1, "invalid owned process PID")
    root = Path(f"/proc/{pid}")
    before = (root / "stat").read_text().rpartition(") ")[2].split()
    require(len(before) >= 20 and before[19].isdigit(), "invalid process birth record")
    executable = (root / "exe").resolve(strict=True)
    require(executable == state / "rustfs" and digest(executable) == owner["binarySha256"], "signal target is not the owned candidate")
    environment = dict(item.split("=", 1) for item in (root / "environ").read_bytes().decode().split("\0") if "=" in item)
    settings = {**runtime_config(owner.get("runtimeProfile", "manual")), "RUSTFS_HEAL_TASK_TIMEOUT_SECS": "300"}
    require(all(environment.get(key) == value for key, value in settings.items()), "owned process runtime changed")
    result = {**base, "state": before[0], "startTicks": int(before[19]), "executable": str(executable),
        "mountNamespace": os.readlink(root / "ns/mnt"), "commandSha256": hashlib.sha256((root / "cmdline").read_bytes()).hexdigest(),
        "settings": settings}
    after = (root / "stat").read_text().rpartition(") ")[2].split()
    require(after[19] == before[19], "process identity changed while collecting evidence")
    return result


def process(request):
    # systemd's MainPID can remain visible briefly after /proc disappears.
    # Refresh the whole service snapshot; absence alone never proves exit or
    # permits an invented PID=0. Exact exit code/result checks remain in the
    # graceful/crash observer, and pidfd callers still compare process birth.
    for attempt in range(3):
        try:
            return process_once(request)
        except FileNotFoundError as exc:
            if not isinstance(exc.filename, str) or not re.match(r"^/proc/[1-9][0-9]*/", exc.filename):
                raise
            require(attempt < 2, "owned process/service state did not settle after process exit")
            time.sleep(.05)


def pidfd_signal(request, signum):
    expected = request.get("expected")
    require(isinstance(expected, dict) and all(name in expected for name in PROCESS_FIELDS)
            and type(expected["pid"]) is int and expected["pid"] > 1, "signal lacks an exact process birth identity")
    require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"), "safe process control requires Linux pidfd support")
    descriptor = os.pidfd_open(expected["pid"], 0)
    try:
        current = process(request)
        require(all(current.get(name) == expected[name] for name in PROCESS_FIELDS), "stale process identity; no signal sent")
        signal.pidfd_send_signal(descriptor, signum, None, 0)
    finally:
        os.close(descriptor)
    return current


def process_metrics(request):
    """Read only the owned service's resource counters, with process birth binding."""
    require(set(request) == {"runId", "action"}, "process metrics accept no arbitrary PID or path")
    before = process(request)
    require(before["pid"] > 1, "resource sample requires the original live service")
    root = Path(f"/proc/{before['pid']}")
    fields = (root / "stat").read_text().rpartition(") ")[2].split()
    require(len(fields) >= 22 and int(fields[19]) == before["startTicks"], "resource sample crossed process birth")
    io = {name: int(value.strip()) for name, value in (line.split(":", 1) for line in (root / "io").read_text().splitlines())}
    required_io = {"rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes", "cancelled_write_bytes"}
    require(required_io <= set(io) and all(value >= 0 for value in io.values()), "owned process IO counters are incomplete")
    result = {"identity": before, "clockDomain": before["bootId"], "monotonicNs": time.monotonic_ns(),
              "cpuTicks": int(fields[11]) + int(fields[12]), "clockTicksPerSecond": os.sysconf("SC_CLK_TCK"),
              "rssBytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"), "virtualBytes": int(fields[20]),
              "threads": int(fields[17]), "fileDescriptors": sum(1 for _ in (root / "fd").iterdir()),
              "io": {name: io[name] for name in sorted(required_io)}, "readOnly": True,
              "ioScope": "owned process counters; cached logical IO and storage IO reported separately"}
    after = process(request)
    require(all(after.get(name) == before[name] for name in PROCESS_FIELDS), "resource sample crossed a service identity change")
    return result


def process_signal(request):
    name = request.get("signal")
    require(name in {"SIGSTOP", "SIGCONT"}, "only owned-process pause/resume signals are allowed")
    before = pidfd_signal(request, getattr(signal, name))
    return {"signal": name, "before": before, "pidfdBound": True, "platformEventIssued": False}


def process_graceful(request):
    require(owned(request["runId"])[2].get("runtimeProfile") in {"root-restart", "replacement", "replacement-target"}
            and process(request).get("state") == "T", "graceful observation requires the paused run-owned root service")
    before = pidfd_signal(request, signal.SIGTERM)
    # A paused process must receive CONT to handle TERM. Use the same pidfd
    # and exact birth identity again; a recycled PID never receives a signal.
    pidfd_signal(request, signal.SIGCONT)
    deadline = time.monotonic() + 60
    while True:
        after = process(request)
        if after["pid"] == 0:
            break
        require(all(after.get(name) == before[name] for name in PROCESS_FIELDS), "another process replaced the graceful target")
        require(time.monotonic() < deadline, "owned coordinator did not exit gracefully in 60 seconds")
        time.sleep(0.1)
    service = after["service"]
    require(after["bootId"] == before["bootId"] and after["binarySha256"] == before["binarySha256"]
            and after["environmentFileSha256"] == before["environmentFileSha256"]
            and service.get("ExecMainCode") == "1" and service.get("ExecMainStatus") == "0"
            and service.get("Result") == "success", "process has no witnessed normal exit zero; signal/timeout exits are not graceful")
    return {"before": before, "after": after, "signal": "SIGTERM", "actualExitCode": 0,
            "pidfdBound": True, "platformEventIssued": False}


def process_crash(request):
    require(owned(request["runId"])[2].get("runtimeProfile") in {"replacement", "replacement-target"}
            and process(request).get("state") == "T", "crash observation requires the paused owned replacement service")
    before = pidfd_signal(request, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while True:
        after = process(request)
        if after["pid"] == 0: break
        require(all(after.get(name) == before[name] for name in PROCESS_FIELDS), "another process replaced the crash target")
        require(time.monotonic() < deadline, "owned replacement process did not exit after SIGKILL")
        time.sleep(0.1)
    service = after["service"]
    require(after["bootId"] == before["bootId"] and after["binarySha256"] == before["binarySha256"]
            and after["environmentFileSha256"] == before["environmentFileSha256"]
            and service.get("ExecMainCode") == "2" and service.get("ExecMainStatus") == "9"
            and service.get("Result") == "signal", "process has no exact non-graceful SIGKILL exit witness")
    return {"before": before, "after": after, "signal": "SIGKILL", "actualSignal": 9,
            "pidfdBound": True, "graceful": False, "platformEventIssued": False}


def fault_module():
    # SSH requests carry this exact repository module in memory; local imports
    # support contract tests without creating an importable file on a node.
    if "fault" in globals():
        return globals()["fault"]
    from . import heal_fault
    return heal_fault


def gate_module():
    if "gate" in globals(): return globals()["gate"]
    from . import drive_gate
    return drive_gate


def replacement_module():
    if "replacement" in globals(): return globals()["replacement"]
    from . import replace_drive
    return replace_drive


def replace_drive(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    require(gate_module().checked_gate(state, roots) is None, "replacement cannot share an active drive gate")
    return replacement_module().begin(state, roots, request.get("disk"), request.get("expectedFormatSha256"))


def replacement_state(request):
    state, roots, owner = owned(request["runId"])
    require(digest(state / "rustfs") == owner["binarySha256"], "replacement candidate changed")
    evidence = replacement_module().attest(state, roots)
    formats = []
    for disk, root in enumerate(roots):
        path = checked(root / "data/.rustfs.sys/format.json", root)
        if path.exists():
            require(path.stat().st_size < 65536, "replacement format exceeds its bound")
            formats.append({"disk": disk, "present": True, "sha256": digest(path), "value": json.loads(path.read_text())})
        else: formats.append({"disk": disk, "present": False})
    return {**evidence, "formats": formats, "binarySha256": owner["binarySha256"], "readOnly": True}


def replacement_probe(request):
    state, roots, _ = owned(request["runId"])
    before = process(request)
    result = replacement_module().probe(state, roots, before["pid"])
    after = process(request)
    require(all(before.get(name) == after.get(name) for name in PROCESS_FIELDS),
            "replacement backup probe crossed a service process change")
    return {**result, "sameOriginalServiceProcess": True}


def format_export(request):
    require(set(request) == {"runId", "action", "disk"}, "format export accepts one owned member only")
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    disk = request["disk"]
    require(type(disk) is int and 0 <= disk < len(roots), "invalid exported disk")
    path = checked(roots[disk] / "data/.rustfs.sys/format.json", roots[disk])
    require(path.stat().st_size < 65536, "exported format exceeds its bound")
    sha = digest(path)
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == sha == digest(path) and digest(state / "rustfs") == owner["binarySha256"],
            "released donor format or candidate changed during export")
    return {"runId": owner["runId"], "disk": disk, "binarySha256": owner["binarySha256"],
            "sha256": sha, "rawBase64": base64.b64encode(raw).decode(), "value": json.loads(raw), "readOnly": True}


def foreign_format(request):
    require(set(request) == {"runId", "action", "rawBase64", "sha256"}, "foreign format accepts exact exported bytes only")
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    require(owner.get("runtimeProfile") == "replacement", "foreign format requires the isolated replacement runtime")
    return replacement_module().install_foreign(state, roots, request["rawBase64"], request["sha256"])


def restore_replacement(request):
    require(set(request) == {"runId", "action"}, "replacement recovery accepts no paths")
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    return replacement_module().restore_original(state, roots)


def drive_gate(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    if request.get("mode") == "clear": return gate_module().release_gate(state, roots)
    return gate_module().apply_gate(state, roots, request.get("disk"), request.get("mode"))


def drive_gate_probe(request):
    state, roots, owner = owned(request["runId"])
    pid = int(command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"]).stdout.strip())
    require(Path(f"/proc/{pid}/exe").resolve(strict=True) == state / "rustfs", "gate probe candidate process differs")
    require(digest(state / "rustfs") == owner["binarySha256"], "gate probe candidate digest differs")
    return gate_module().probe_gate(state, roots, pid)


def drive_gate_activate(request):
    state, roots, _ = owned(request["runId"])
    before = process(request)
    require(before["pid"] > 1, "permission fault requires the running owned candidate")
    receipt = gate_module().activate_gate(state, roots)
    after = process(request)
    require(all(before.get(name) == after.get(name) for name in PROCESS_FIELDS),
            "permission injection crossed the original process identity")
    return {**receipt, "processBefore": before, "processAfter": after, "sameOriginalProcess": True}


def drive_gate_state(request):
    state, roots, _ = owned(request["runId"])
    return gate_module().checked_gate(state, roots) or {"state": "CLEAR"}


def drive_gate_release_live(request):
    state, roots, _ = owned(request["runId"])
    gate = gate_module().checked_gate(state, roots)
    require(gate and gate.get("mode") == "permission-denied" and gate.get("phase") == "active",
            "live rejoin only releases an actual active permission fault")
    before = process(request)
    require(before["pid"] > 1, "live rejoin requires the original running candidate")
    receipt = gate_module().release_gate(state, roots)
    after = process(request)
    require(all(before.get(name) == after.get(name) for name in PROCESS_FIELDS),
            "live rejoin crossed the original process identity")
    return {**receipt, "processBefore": before, "processAfter": after,
            "sameOriginalProcess": True, "serviceRestarted": False}


def fault_root(request):
    _, roots, owner = owned(request["runId"])
    stopped(owner)
    index = request.get("disk")
    require(type(index) is int and 0 <= index < len(roots), "invalid physical disk index")
    return roots[index], owner


def apply_fault(request):
    root, owner = fault_root(request)
    module = fault_module()
    with module.directory(root) as fd:
        marker = root / "fault-owner.json"
        if not marker.exists():
            module.new_file(fd, marker.name, module.encode({"schemaVersion": 1, "runId": owner["runId"],
                "bucketPrefix": owner["bucket"], "mountId": module.mount_id(fd), "device": os.fstat(fd).st_dev,
                "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()}))
        binding = module.checked_owner(fd, owner["runId"])
        return module.apply_fault(fd, binding, request["relativePath"], request["faultId"], request["kind"],
                                  request["expectedSha256"], request.get("offset"),
                                  donor_relative=request.get("donorRelativePath"), donor_sha=request.get("donorSha256"))


def recover_fault(request):
    root, owner = fault_root(request)
    module = fault_module()
    with module.directory(root) as fd:
        return module.recover_fault(fd, module.checked_owner(fd, owner["runId"], recovery=True), request["faultId"])


def verify_part(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    require(owner.get("inspectorSha256"), "candidate-bound bitrot verifier is required")
    tool = checked(state / "heal-meta-inspect", state)
    require(digest(tool) == owner["inspectorSha256"], "bitrot verifier changed")
    return verify_part_owned(request, roots, owner, tool)


def verify_part_owned(request, roots, owner, tool):
    module = fault_module()
    relative = request["relativePath"]
    parts = module.target_parts(relative, owner["bucket"])
    require(parts[-1] != "xl.meta" and type(request.get("partNumber")) is int
            and parts[-1] == f"part.{request['partNumber']}", "part identity differs from request")
    require(type(request.get("disk")) is int and 0 <= request["disk"] < len(roots), "invalid physical disk index")
    root = roots[request["disk"]]
    data = root / "data"
    part = checked(data / relative, data)
    metadata = checked(part.parent.parent / "xl.meta", data)
    bucket = parts[0]
    key = metadata.parent.relative_to(data / bucket).as_posix()
    result = json.loads(command(str(tool), "--verify-part", str(metadata), bucket, key,
                                str(part), request["versionId"], str(request["partNumber"])).stdout)
    require(result.get("physicalBitrotVerified") is True and result.get("bucket") == bucket and result.get("key") == key
            and result.get("versionId") == request["versionId"]
            and result.get("partNumber") == request["partNumber"], "bitrot proof identity differs")
    return {**result, "sha256": digest(part), "relativePath": relative, "disk": request["disk"]}


def verify_parts(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    entries = request.get("parts")
    require(isinstance(entries, list) and 1 <= len(entries) <= 128
            and all(isinstance(entry, dict) and set(entry) == {"disk", "relativePath", "versionId", "partNumber"}
                    for entry in entries), "invalid bounded part verification batch")
    require(len({(entry["disk"], entry["relativePath"], entry["versionId"], entry["partNumber"])
                 for entry in entries}) == len(entries), "duplicate part in verification batch")
    require(owner.get("inspectorSha256"), "candidate-bound bitrot verifier is required")
    tool = checked(state / "heal-meta-inspect", state)
    require(digest(tool) == owner["inspectorSha256"], "bitrot verifier changed")
    return {"complete": True, "parts": [verify_part_owned(entry, roots, owner, tool) for entry in entries]}


def verify_inline(request):
    state, _, owner = owned(request["runId"])
    root, _ = fault_root(request)
    module = fault_module()
    parts = module.target_parts(request["relativePath"], owner["bucket"])
    require(parts[-1] == "xl.meta", "inline verification requires the exact object metadata")
    path = checked(root / "data" / request["relativePath"], root / "data")
    key = "/".join(parts[1:-1])
    tool = checked(state / "heal-meta-inspect", state)
    require(digest(tool) == owner.get("inspectorSha256"), "inline verifier changed")
    result = json.loads(command(str(tool), "--verify-inline", str(path), parts[0], key, request["versionId"]).stdout)
    require(result.get("physicalBitrotVerified") is True and result.get("bucket") == parts[0] and result.get("key") == key
            and result.get("versionId") == request["versionId"], "inline proof identity differs")
    return {**result, "relativePath": request["relativePath"], "disk": request["disk"]}


def install_inspector(request):
    state, _, owner = owned(request["runId"])
    tool = checked(state / "heal-meta-inspect", state)
    require(digest(tool) == owner.get("inspectorSha256"), "uploaded inspector digest differs")
    tool.chmod(0o500)
    return {"inspectorSha256": owner["inspectorSha256"], "installed": True}


def inspect_metadata(tool, path, bucket, key, entry, metadata_faults):
    observed = command(str(tool), str(path), bucket, key, check=False)
    if observed.returncode:
        expected_fault = metadata_faults.get(entry["path"])
        require(expected_fault == {name: entry[name] for name in ("sha256", "size")},
                "metadata decoder failed outside an exact witnessed file fault")
        return {"metadataError": {"code": observed.returncode, "message": observed.stderr[-500:],
                                  "exactWitnessedFault": True, "validMetadataClaimed": False}}
    decoded = json.loads(observed.stdout)
    require(decoded.get("bucket") == bucket and decoded.get("key") == key
            and decoded.get("physicalBitrotVerified") is False,
            "metadata decoder identity or verification role differs")
    return {"metadata": decoded}


def snapshot_buckets(owner, request):
    buckets = request.get("buckets", [owner["bucket"]])
    require(isinstance(buckets, list) and 1 <= len(buckets) <= 8
            and all(isinstance(bucket, str) and 3 <= len(bucket) <= 63
                    and re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", bucket)
                    and (bucket == owner["bucket"] or bucket.startswith(owner["bucket"] + "-")) for bucket in buckets)
            and len(set(buckets)) == len(buckets) and owner["bucket"] in buckets,
            "physical bucket scope escaped the original run-owned prefix or omitted its primary bucket")
    return buckets


def snapshot(request):
    state, roots, owner = owned(request["runId"])
    pid = int(command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"], check=False).stdout.strip() or "0")
    if request.get("quiescent") is True:
        stopped(owner)
        require(digest(state / "rustfs") == owner["binarySha256"], "stopped candidate changed")
    else:
        require(pid > 0, "run-owned service has no live PID")
        executable = Path(f"/proc/{pid}/exe").resolve(strict=True)
        require(executable == state / "rustfs" and digest(executable) == owner["binarySha256"],
                "running binary differs from candidate")
        environment = dict(value.split("=", 1) for value in Path(f"/proc/{pid}/environ").read_bytes().decode().split("\0")
                           if "=" in value)
        expected_config = runtime_config(owner.get("runtimeProfile", "manual"))
        require(all(environment.get(key) == value for key, value in expected_config.items()),
                "bound Heal runtime configuration differs")
    files, formats = [], []
    limits = resource_limits(owner.get("resourceProfile", "standard"))
    tool = state / "heal-meta-inspect"
    if owner.get("inspectorSha256"):
        require(digest(checked(tool, state)) == owner["inspectorSha256"], "metadata inspector changed")
    for index, root in enumerate(roots):
        data = root / "data"
        for bucket in snapshot_buckets(owner, request):
            folder = checked(data / bucket, data)
            if folder.exists():
                for directory, children, names in os.walk(folder, followlinks=False):
                    for name in (*children, *names):
                        checked(Path(directory) / name, data)
                    for name in sorted(names):
                        path = Path(directory) / name
                        require(len(files) < limits["maxSnapshotFiles"] and path.stat().st_size <= 64 * 1024**2,
                                "fixture exceeds physical evidence budget")
                        entry = {"disk": index, "path": str(path.relative_to(data)),
                                 "size": path.stat().st_size, "sha256": digest(path)}
                        if path.name == "xl.meta" and owner.get("inspectorSha256"):
                            key = path.parent.relative_to(folder).as_posix()
                            entry.update(inspect_metadata(tool, path, bucket, key, entry, request.get("metadataFaults", {})))
                        files.append(entry)
        path = checked(data / ".rustfs.sys/format.json", data)
        if not path.exists() and request.get("allowUnformattedReplacement") is True:
            replacement = replacement_module().attest(state, roots)
            require(not replacement.get("replacementAbsent") and replacement["disk"] == index
                    and not replacement["originalStillAtDataRoot"], "missing format is outside the exact isolated replacement member")
            formats.append({"disk": index, "present": False, "unformattedReplacement": True})
            continue
        require(path.stat().st_size < 65536, "disk format exceeds budget")
        formats.append({"disk": index, "sha256": digest(path), "value": json.loads(path.read_text())})
    return {"hostname": owner["hostname"], "unit": owner["unit"], "pid": pid,
            "binarySha256": owner["binarySha256"], "quiescent": request.get("quiescent") is True,
            "backgroundConfig": None if request.get("quiescent") is True else expected_config,
            "storageClassEnvironment": None if request.get("quiescent") is True else {
                key: environment.get(key) for key in ("RUSTFS_STORAGE_CLASS_INLINE_BLOCK", "RUSTFS_STORAGE_CLASS_STANDARD")},
            "resourceLimits": limits,
            "files": sorted(files, key=lambda value: (value["disk"], value["path"])), "formats": formats}


def logs(request):
    _, _, owner = owned(request["runId"])
    if request.get("checkpoint") is True or "afterCursor" in request:
        cursor = request.get("afterCursor")
        require(cursor is None or isinstance(cursor, str) and re.fullmatch(r"[A-Za-z0-9=;_-]{1,512}", cursor),
                "invalid journal cursor")
        args = ["journalctl", "-u", owner["unit"], "--quiet", "--no-pager", "--all", "-o", "json",
                "--output-fields=__CURSOR,MESSAGE"]
        # +N selects the FIRST N entries after the cursor. A plain -n N
        # selects the tail and could silently drop the beginning of a window.
        args += ["--after-cursor", cursor, "-n", "+10000"] if cursor else ["-n", "1"]
        output = command(*args).stdout
        lines, observed_cursor, entries = [], None, 0
        for line in output.splitlines():
            row = json.loads(line)
            entry_cursor = row.get("__CURSOR")
            require(isinstance(entry_cursor, str) and re.fullmatch(r"[A-Za-z0-9=;_-]{1,512}", entry_cursor)
                    and entry_cursor not in {cursor, observed_cursor}, "journal page repeated or omitted its exact entry cursor")
            message = row.get("MESSAGE")
            if isinstance(message, list) and all(type(value) is int and 0 <= value <= 255 for value in message):
                message = bytes(message).decode("utf-8")
            require(isinstance(message, str), "journal message is not complete text")
            entries += 1
            require(entries <= 10000, "journal page exceeds its bound; source attribution is incomplete")
            observed_cursor = entry_cursor
            lines.append(message)
        require(observed_cursor is not None or cursor is not None and not lines,
                "journal did not expose a complete cursor window")
        return {"unit": owner["unit"], "journal": "\n".join(lines), "beforeCursor": cursor,
                "afterCursor": observed_cursor or cursor, "complete": True,
                "entries": entries, "hasMore": cursor is not None and entries == 10000}
    return {"unit": owner["unit"], "journal": command("journalctl", "-u", owner["unit"], "--no-pager", "-n", "2000").stdout}


def cleanup(request):
    state, roots, owner = owned(request["runId"])
    command("systemctl", "stop", owner["unit"], check=False)
    pid = command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"], check=False).stdout.strip()
    require(pid in {"", "0"}, "run-owned service remains active")
    gate_module().release_gate(state, roots)
    replacement_module().attest(state, roots)
    command("systemctl", "reset-failed", owner["unit"], check=False)
    for root in (*roots, state):
        for directory, children, names in os.walk(root, followlinks=False):
            for name in (*children, *names):
                checked(Path(directory) / name, root)
        shutil.rmtree(root)
    require(not state.exists() and all(not root.exists() for root in roots), "run namespace remains")
    return {"cleanup": "PASS", "unitStopped": True, "rootsRemoved": True, "hostname": owner["hostname"]}


def audit_finalize(request):
    """Finalize only an invalid-ID rejection proved identical to its archived pre-fault scene."""
    _, _, owner = owned(request["runId"])
    stopped(owner)
    fault_id = request.get("rejectedFaultId", "")
    require(re.fullmatch(r"[A-J][0-9]{2}-[a-z0-9-]{49,128}", fault_id) is not None,
            "audited finalization only supports an overlength ID rejected before injection")
    require(request.get("binarySha256") == owner["binarySha256"], "audited candidate identity differs")
    root, _ = fault_root(request)
    for suffix in (".intent.json", ".original", ".applied.json"):
        require(not checked(root / "quarantine" / (fault_id + suffix), root).exists(),
                "rejected fault unexpectedly has intent or backup bytes; retain scene")
    observed = snapshot({**request, "quiescent": True})
    content = {key: observed[key] for key in ("files", "formats")}
    scene_sha = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    require(re.fullmatch(r"[0-9a-f]{64}", request.get("expectedSceneSha256", "")) is not None
            and scene_sha == request["expectedSceneSha256"], "physical scene drifted from archived pre-fault evidence; retain namespace")
    result = cleanup(request)
    return {**result, "faultNeverApplied": True, "archivedPreFaultSceneEqual": True, "sceneSha256": scene_sha,
            "backupRestorationUsed": False, "productRepairClaimed": False}


def task_records(request):
    """Read three exact Admin journal names; never mutate system metadata."""
    state, roots, owner = owned(request["runId"])
    token = request.get("taskId")
    require(isinstance(token, str) and str(uuid.UUID(token)) == token, "noncanonical Admin journal task ID")
    require(digest(state / "rustfs") == owner["binarySha256"], "journal candidate changed")
    if request.get("quiescent") is True:
        stopped(owner)
    else:
        pid = int(command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"]).stdout.strip() or "0")
        require(pid > 0 and Path(f"/proc/{pid}/exe").resolve(strict=True) == state / "rustfs", "journal service is not run-owned")
    records = []
    for disk, root in enumerate(roots):
        data = root / "data"
        for kind, prefix, limit in (("intent", "root-heal-", 64 * 1024),
                                    ("terminal", "terminal-root-heal-", 64 * 1024),
                                    ("report", "heal-terminal-report-", 8 * 1024**2)):
            relative = ".rustfs.sys/" + prefix + token + ".json"
            path = checked(data / relative, data)
            entry = {"disk": disk, "kind": kind, "relativePath": relative, "present": False}
            try:
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            except FileNotFoundError:
                records.append(entry)
                continue
            with os.fdopen(descriptor, "rb") as stream:
                info = os.fstat(stream.fileno())
                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= limit,
                        "Admin journal exceeds its exact regular-file budget")
                raw = stream.read(limit + 1)
                after = os.fstat(stream.fileno())
                require(len(raw) == info.st_size and len(raw) <= limit
                        and all(getattr(after, name) == getattr(info, name) for name in
                                ("st_ino", "st_dev", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink")),
                        "Admin journal changed during its bounded read")
            entry.update(present=True, size=len(raw), sha256=hashlib.sha256(raw).hexdigest(), value=json.loads(raw))
            records.append(entry)
    return {"taskId": token, "binarySha256": owner["binarySha256"], "readOnly": True, "records": records}


def mrf_records(request):
    require(set(request) <= {"runId", "action", "quiescent"}, "MRF observer does not accept paths or mutation options")
    state, roots, owner = owned(request["runId"])
    if request.get("quiescent") is True:
        stopped(owner)
    else:
        pid = int(command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"]).stdout.strip() or "0")
        require(pid > 0 and Path(f"/proc/{pid}/exe").resolve(strict=True) == state / "rustfs", "MRF observer requires its owned process")
    module = globals().get("mrf")
    require(module is not None, "MRF observer source missing")
    return module.read_records(state, roots, owner, checked)


def main(request):
    require(os.geteuid() == 0, "node adapter requires passwordless sudo")
    actions = {"preflight": preflight, "reserve": reserve, "start": start, "stop": stop, "snapshot": snapshot,
               "mrf-records": mrf_records,
               "logs": logs, "cleanup": cleanup, "audit-finalize": audit_finalize, "install-inspector": install_inspector,
               "apply-fault": apply_fault, "recover-fault": recover_fault, "verify-part": verify_part, "verify-parts": verify_parts,
               "verify-inline": verify_inline, "drive-gate": drive_gate, "drive-gate-probe": drive_gate_probe,
               "drive-gate-activate": drive_gate_activate, "gate-state": drive_gate_state,
               "drive-gate-release-live": drive_gate_release_live}
    actions["task-records"] = task_records
    actions.update({"replace-drive": replace_drive, "replacement-state": replacement_state, "replacement-probe": replacement_probe})
    actions.update({"format-export": format_export, "foreign-format": foreign_format, "restore-replacement": restore_replacement})
    actions.update({"process": process, "process-metrics": process_metrics, "process-signal": process_signal, "process-graceful": process_graceful, "process-crash": process_crash})
    require(request.get("action") in actions, "unsupported node operation")
    print(json.dumps(actions[request["action"]](request)))
