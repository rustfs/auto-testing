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
RUN_PATTERN = re.compile(r"gh-[1-9][0-9]*-[1-9][0-9]*-(?:h1|h2|h5)")
BACKGROUND_CONFIG = {"RUSTFS_SCANNER_ENABLED": "false", "RUSTFS_HEAL_ENABLED": "true",
                     "RUSTFS_HEAL_AUTO_HEAL_ENABLE": "false", "RUSTFS_HEAL_MRF_ENABLE": "false",
                     "RUSTFS_SHARD_INTEGRITY_WRITE": "true", "RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED": "true",
                     "RUSTFS_OBS_LOGGER_LEVEL": "info"}


def runtime_config(profile):
    if profile in {"incarnation-checkpoint", "incarnation-mrf"}:
        logging_filter = "info,rustfs::heal::manager=debug,rustfs::heal::task=debug,rustfs::heal::mrf=debug"
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_MRF_ENABLE": "true" if profile == "incarnation-mrf" else "false",
                "AUTO_TESTING_C11_PHASE": "capture",
                "RUSTFS_HEAL_PAGE_PARALLEL_ENABLE": "false", "RUSTFS_HEAL_MAX_CONCURRENT_HEALS": "1",
                "RUSTFS_HEAL_MAX_CONCURRENT_PER_SET": "1", "RUSTFS_OBS_LOGGER_LEVEL": logging_filter,
                "RUST_LOG": logging_filter}
    require(profile in {"queue-pressure", "orphan-preserve", "orphan-before", "orphan-after", "manual", "authority-split", "multi-pool", "bounded-capacity", "journal-bounded", "lock-quorum", "offline-replay", "durable-mrf", "mrf-crash", "flapping", "read-repair", "scanner", "disabled", "overlap", "root-restart", "stale-rejoin", "replacement", "replacement-target", "schema-refusal", "scope-isolation"}, "unsupported Heal runtime profile")
    if profile == "queue-pressure":
        logging_filter = "info,rustfs::heal::manager=debug,rustfs::heal::task=debug,rustfs::heal::mrf=debug"
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_MRF_ENABLE": "true",
                "RUSTFS_OBS_LOGGER_LEVEL": logging_filter, "RUST_LOG": logging_filter,
                "RUSTFS_HEAL_QUEUE_SIZE": "2", "RUSTFS_HEAL_MAX_CONCURRENT_HEALS": "1",
                "RUSTFS_HEAL_MAX_CONCURRENT_PER_SET": "1", "RUSTFS_HEAL_PAGE_PARALLEL_ENABLE": "false",
                "RUSTFS_HEAL_OVERLAP_POLICY": "merge", "RUSTFS_HEAL_LOW_PRIORITY_MERGE_ENABLE": "true",
                "RUSTFS_HEAL_LOW_PRIORITY_DROP_WHEN_FULL": "true", "RUSTFS_HEAL_MRF_REPLAY_BATCH": "1"}
    if profile in {"orphan-preserve", "orphan-before", "orphan-after"}:
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS":
                {"orphan-preserve": "0", "orphan-before": "300", "orphan-after": "30"}[profile]}
    if profile == "authority-split":
        return {**BACKGROUND_CONFIG, "RUSTFS_STORAGE_CLASS_STANDARD": "EC:1"}
    if profile == "multi-pool":
        return {**BACKGROUND_CONFIG, "RUSTFS_ERASURE_SET_DRIVE_COUNT": "4", "RUSTFS_STORAGE_CLASS_STANDARD": "EC:2"}
    if profile in {"bounded-capacity", "journal-bounded"}:
        logging_filter = "info,rustfs::heal::manager=debug,rustfs::heal::task=debug,rustfs::heal::resume=debug"
        return {**BACKGROUND_CONFIG, "RUSTFS_UNSAFE_BYPASS_DISK_CHECK": "true",
                **({"RUSTFS_HEAL_PAGE_PARALLEL_ENABLE": "false", "RUSTFS_OBS_LOGGER_LEVEL": logging_filter,
                    "RUST_LOG": logging_filter}
                   if profile == "journal-bounded" else {})}
    if profile == "lock-quorum":
        return {**BACKGROUND_CONFIG, "RUSTFS_STORAGE_CLASS_STANDARD": "EC:8"}
    if profile == "scope-isolation":
        return {**BACKGROUND_CONFIG, "RUSTFS_ERASURE_SET_DRIVE_COUNT": "8"}
    if profile in {"durable-mrf", "mrf-crash", "flapping"}:
        # Keep actual MRF admission/start/terminal evidence without enabling
        # unrelated HTTP/HTTP2 frame debugging on the high-volume fleet.
        logging_filter = "info,rustfs::heal::manager=debug,rustfs::heal::task=debug,rustfs::heal::mrf=debug"
        return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_MRF_ENABLE": "true",
                "RUSTFS_OBS_LOGGER_LEVEL": logging_filter, "RUST_LOG": logging_filter,
                **({"RUSTFS_HEAL_MAX_CONCURRENT_HEALS": "1", "RUSTFS_HEAL_MAX_CONCURRENT_PER_SET": "1",
                    "RUSTFS_HEAL_MRF_REPLAY_BATCH": "1"} if profile in {"mrf-crash", "flapping"} else {})}
    if profile in {"replacement", "replacement-target", "schema-refusal"}:
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
    if profile == "read-repair":
        # Admission evidence must survive journald's per-unit rate limit;
        # global DEBUG floods it with unrelated HTTP/HTTP2 frame events.
        logging_filter = "info,rustfs_ecstore=debug,rustfs::heal=debug"
        return {**BACKGROUND_CONFIG, "RUSTFS_OBS_LOGGER_LEVEL": logging_filter, "RUST_LOG": logging_filter}
    if profile == "scanner":
        return {**BACKGROUND_CONFIG, "RUSTFS_SCANNER_ENABLED": "true", "RUSTFS_OBS_LOGGER_LEVEL": "debug",
                "RUSTFS_SCANNER_START_DELAY_SECS": "5", "RUSTFS_SCANNER_CYCLE": "5",
                "RUSTFS_SCANNER_SPEED": "fastest", "RUSTFS_SCANNER_DELAY": "0",
                "RUSTFS_SCANNER_BITROT_CYCLE_SECS": "0", "RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS": "0",
                "RUSTFS_HEAL_OBJECT_SELECT_PROB": "1", "RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES": "1"}
    return {**BACKGROUND_CONFIG, "RUSTFS_HEAL_MRF_ENABLE": "true" if profile == "offline-replay" else "false"}


def runtime_settings(owner):
    config = runtime_config(owner.get("runtimeProfile", "manual"))
    if owner.get("runtimeProfile") in {"incarnation-checkpoint", "incarnation-mrf"}:
        config["AUTO_TESTING_C11_PHASE"] = owner.get("incarnationPhase", "capture")
    if owner.get("incarnationPhase") == "transition":
        config.update(RUSTFS_HEAL_MRF_ENABLE="false")
    return config


def incarnation_configuration(state, owner):
    env = checked(state / "service.env", state)
    info = env.stat()
    require(info.st_uid == 0 and info.st_nlink == 1 and stat.S_ISREG(info.st_mode) and info.st_size <= 65536,
            "C11 configuration lost its bounded private ownership")
    lines = env.read_text().splitlines()
    require(all("=" in line for line in lines), "C11 configuration has an invalid entry")
    values = dict(line.split("=", 1) for line in lines)
    require(len(lines) == len(values) and all(values.get(key) == value for key, value in runtime_settings(owner).items()),
            "C11 phase configuration drifted or its durable phase commit is incomplete")
    return values


def incarnation_operation(request):
    require(set(request) <= {"runId", "action", "family", "taskId", "bucketIncarnationId", "expectedRecords", "phase"},
            "C11 adapter accepts no arbitrary paths, replacement bytes or commands")
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    require(owner.get("runtimeProfile") in {"incarnation-checkpoint", "incarnation-mrf"}
            and digest(state / "rustfs") == owner["binarySha256"], "C11 replay runtime/candidate changed")
    backend = globals()["incarnation_replay_backend"]
    journal, fault = globals()["journal_fault_backend"], fault_module()
    if request["action"] == "incarnation-phase":
        require(set(request) == {"runId", "action", "phase"} and request["phase"] in {"transition", "replay"},
                "C11 phase cannot change arbitrary environment settings")
        current = owner.get("incarnationPhase", "capture")
        require((current, request["phase"]) in {("capture", "transition"), ("transition", "replay")},
                "C11 phase transition cannot refund an earlier replay window")
        try:
            with fault.directory(state) as fd:
                detached = journal.decode_json(fault.read_file(fd, "C11-replay-detached.json", 16384))
                installed = journal.decode_json(fault.read_file(fd, "C11-replay-installed.json", 16384)) if current == "transition" else None
            require(isinstance(detached, dict), "C11 phase transaction binding is invalid")
            params = {key: detached.get(key) for key in ("runId", "family", "taskId", "bucketIncarnationId")}
            binding = backend.identity(owner, params, journal)
            require(journal.same_json(detached, binding) and (current == "capture" or journal.same_json(installed, binding)),
                    "C11 phase transaction changed its original run/candidate/task/incarnation binding")
            manifest = backend.archived(state, roots, owner, params, journal, fault)
            records = backend.census(roots, binding, journal, fault)
            require(all(not row["present"] for row in records) if current == "capture"
                    else journal.same_json(records, manifest["records"]),
                    "C11 phase runnable records differ from the completed original transaction")
        except FileNotFoundError as exc:
            raise ValueError("C11 phase requires a complete original record transaction") from exc
        values = incarnation_configuration(state, owner)
        owner["incarnationPhase"] = request["phase"]
        replacement = runtime_settings(owner)
        env = state / "service.env"
        values.update(replacement)
        temporary = state / "C11-service.env.new"
        with fault_module().directory(state) as fd:
            fault_module().new_file(fd, temporary.name, ("\n".join(f"{key}={value}" for key, value in values.items()) + "\n").encode())
            os.rename(temporary.name, env.name, src_dir_fd=fd, dst_dir_fd=fd)
            os.fsync(fd)
        with fault_module().directory(state) as fd:
            fault_module().new_file(fd, "C11-phase.new", fault_module().encode({"runId": owner["runId"], "phase": request["phase"]}))
            os.rename("C11-phase.new", "C11-phase.json", src_dir_fd=fd, dst_dir_fd=fd)
            os.fsync(fd)
        return {"previousPhase": current, "phase": request["phase"], "candidateUnchanged": True,
                "workerLimit": replacement["RUSTFS_HEAL_MAX_CONCURRENT_HEALS"],
                "mrfEnabled": replacement["RUSTFS_HEAL_MRF_ENABLE"]}
    require("phase" not in request, "C11 record operation accepts no runtime override")
    if request["action"] == "incarnation-census":
        require(set(request) == {"runId", "action", "family", "taskId", "bucketIncarnationId"},
                "C11 stopped census accepts only the original binding")
        binding = backend.identity(owner, request, journal)
        return {**binding, "readOnly": True, "stable": True, "records": backend.census(roots, binding, journal, fault)}
    methods = {"incarnation-capture": backend.capture, "incarnation-install": backend.install,
               "incarnation-finalize": backend.finalize}
    require(request["action"] in methods, "unknown C11 record transaction")
    if request["action"] == "incarnation-capture":
        return backend.capture(state, roots, owner, request, journal, fault, globals()["mrf"], checked)
    return methods[request["action"]](state, roots, owner, request, journal, fault)


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


def environment_capabilities(request):
    """Read existing mounts and kernel interfaces without reserving a run."""
    paths(request['runId'])
    observed = []
    for number in range(1, 9):
        mount = Path(f'/data/rustfs{number}')
        row = {'target': str(mount), 'mounted': False, 'independentBlockDeviceVerified': False}
        raw = command('findmnt', '--json', '--mountpoint', str(mount), '--output',
                      'TARGET,SOURCE,FSTYPE,UUID,MAJ:MIN,OPTIONS', check=False)
        require(raw.returncode in {0, 1}, 'environment mount inventory failed')
        if raw.returncode == 0:
            values = json.loads(raw.stdout).get('filesystems', [])
            require(len(values) == 1 and values[0].get('target') == str(mount), 'environment inventory has an ambiguous mount')
            value = values[0]
            row.update(mounted=True, mount=value, availableBytes=shutil.disk_usage(mount).free)
            device = value.get('maj:min', '')
            require(re.fullmatch(r'[0-9]+:[0-9]+', device), 'environment inventory omitted the actual device identity')
            link = Path('/sys/dev/block') / device
            if link.exists():
                block = link.resolve(strict=True)
                if (block / 'partition').exists(): block = block.parent
                # Stacked/virtual devices need a separately reviewed fault
                # backend; directory or partition counts cannot prove H4.
                stacked = (block / 'slaves').is_dir() and any((block / 'slaves').iterdir())
                physical = (block / 'dev').read_text().strip()
                require(re.fullmatch(r'[0-9]+:[0-9]+', physical), 'environment physical device is ambiguous')
                row.update(physicalDevice=physical, blockName=block.name,
                    independentBlockDeviceVerified=not stacked and not block.name.startswith(('loop', 'dm-', 'ram')))
            row['writableMount'] = 'rw' in value.get('options', '').split(',')
        observed.append(row)
    binary = Path('/usr/bin/rustfs')
    return {'hostname': socket.gethostname(), 'mounts': observed, 'inspectionOnly': True, 'mutated': False,
        'kernel': {'pidfd': hasattr(os, 'pidfd_open'), 'cgroupV2': Path('/sys/fs/cgroup/cgroup.controllers').is_file(),
                   'fuseDevice': Path('/dev/fuse').exists() and stat.S_ISCHR(Path('/dev/fuse').stat().st_mode)},
        'commands': {name: shutil.which(name) is not None for name in ('docker', 'fusermount3', 'nft', 'az')},
        'installedBinarySha256': digest(binary) if binary.is_file() and not binary.is_symlink() else None,
        'platformRestartAuthorityVerified': False, 'compatibleReleasePairVerified': False,
        'canonicalPassClaimed': False}


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
    require(isinstance(volumes, list) and len(volumes) in {2, 4, 16}, "unsupported automatic Heal topology")
    local_paths = [str(root / "data") for root in paths(run_id)[1]]
    h1 = run_id.endswith("-h1")
    require(profile != "lock-quorum" or not h1, "eight-parity lock boundary requires actual sixteen-drive H2")
    expected = local_paths if h1 else [f"http://rustfs-node{node}:9000{path}"
                                      for node in range(1, 5) for path in local_paths]
    if profile == "multi-pool":
        require(run_id.endswith("-h5"), "multi-pool service requires its explicit H5 namespace")
        expected = [f"http://rustfs-node{{1...4}}:9000/data/rustfs{{{first}...{first + 1}}}/heal-validation/{run_id}/data"
                    for first in (1, 3)]
    else:
        require(not run_id.endswith("-h5"), "H5 cannot use an H1/H2 runtime")
    if profile == "scope-isolation":
        require(not h1, "two-set runtime requires the four-node topology")
        expected = [f"http://rustfs-node{node}:9000{path}" for path in local_paths for node in range(1, 5)]
    require(volumes == expected, "endpoint sequence differs from the explicit Heal deployment contract")
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
    if owner.get("runtimeProfile") in {"incarnation-checkpoint", "incarnation-mrf"}:
        phase_file = checked(state / "C11-phase.json", state)
        if phase_file.exists():
            require(phase_file.stat().st_uid == 0 and phase_file.stat().st_nlink == 1,
                    "C11 phase marker lost its private root ownership")
            phase = json.loads(phase_file.read_text())
            require(set(phase) == {"runId", "phase"} and phase["runId"] == run_id
                    and phase["phase"] in {"transition", "replay"}, "C11 phase marker differs")
            owner = {**owner, "incarnationPhase": phase["phase"]}
    return state, roots, owner


def start(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    command("systemctl", "reset-failed", owner["unit"], check=False)
    require(digest(state / "rustfs") == owner["binarySha256"], "candidate changed before start")
    if owner.get("runtimeProfile") in {"incarnation-checkpoint", "incarnation-mrf"}:
        incarnation_configuration(state, owner)
    command("systemd-run", "--unit=" + owner["unit"], "--property=Type=exec",
            "--property=KillMode=control-group", "--property=TimeoutStopSec=30",
            "--property=RuntimeMaxSec=" + str(resource_limits(owner.get("resourceProfile", "standard"))["runtimeMaxSeconds"]),
            "--property=MemoryMax=4G",
            *(["--property=PrivateMounts=true", "--property=CapabilityBoundingSet=~CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH"]
              if owner.get("runtimeProfile") in {"flapping", "queue-pressure"} else []),
            *(["--property=RemainAfterExit=yes"] if owner.get("runtimeProfile") in {"root-restart", "journal-bounded", "durable-mrf", "mrf-crash", "replacement", "replacement-target", "schema-refusal", "incarnation-checkpoint", "incarnation-mrf"} else []),
            "--property=WorkingDirectory=" + str(state),
            "--property=EnvironmentFile=" + str(state / "service.env"),
            *gate_module().start_properties(gate_module().checked_gate(state, roots)),
            *quorum_gate_module().properties(state, roots),
            *(volume_module().properties(state, roots, owner) if owner.get("runtimeProfile") in {"bounded-capacity", "journal-bounded"} else []),
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
    settings = {**runtime_settings(owner), "RUSTFS_HEAL_TASK_TIMEOUT_SECS": "300"}
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
    require(owned(request["runId"])[2].get("runtimeProfile") in {"root-restart", "journal-bounded", "replacement", "replacement-target"}
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
    require(owned(request["runId"])[2].get("runtimeProfile") in {"replacement", "replacement-target", "root-restart", "journal-bounded", "durable-mrf", "mrf-crash", "incarnation-checkpoint", "incarnation-mrf"}
            and process(request).get("state") == "T", "crash observation requires a paused owned recovery service")
    before = pidfd_signal(request, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while True:
        after = process(request)
        if after["pid"] == 0: break
        require(all(after.get(name) == before[name] for name in PROCESS_FIELDS), "another process replaced the crash target")
        require(time.monotonic() < deadline, "owned recovery process did not exit after SIGKILL")
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


def encryption_module():
    if "encryption_runtime" in globals(): return globals()["encryption_runtime"]
    from . import encryption_runtime
    return encryption_runtime


def gate_module():
    if "gate" in globals(): return globals()["gate"]
    from . import drive_gate
    return drive_gate


def quorum_gate_module():
    if "quorum_gate_backend" in globals(): return globals()["quorum_gate_backend"]
    from . import quorum_gate
    return quorum_gate


def volume_module():
    if "bounded_volume_backend" in globals(): return globals()["bounded_volume_backend"]
    from . import bounded_volume
    return bounded_volume


def volume_views(state, roots, owner):
    return volume_module().views(state, roots, owner) if owner.get("runtimeProfile") in {"bounded-capacity", "journal-bounded"} else roots


def volume_operation(request):
    state, roots, owner = owned(request["runId"])
    require(owner.get("runtimeProfile") in {"bounded-capacity", "journal-bounded"}, "bounded operation requires its exact dedicated runtime")
    module = volume_module()
    if request["action"] == "bounded-volumes-create":
        require(set(request) == {"runId", "action", "branch"}, "bounded allocation accepts no supplied path or limits")
        stopped(owner)
        return module.create(state, roots, owner, request["branch"])
    if request["action"] == "bounded-volumes-release" and request.get("quiescent") is True:
        require(set(request) == {"runId", "action", "quiescent"}, "bounded recovery accepts no supplied path")
        stopped(owner)
        changed = [module.release_filler(view) for view in module.views(state, roots, owner)]
        return {"changes": changed, "ownedFillerReleased": True, "productRepairClaimed": False,
                "quiescent": True, "privateMountRemovedByStoppedService": True}
    if request["action"] == "bounded-volumes-fill" and request.get("quiescent") is True:
        require(set(request) == {"runId", "action", "quiescent"}
                and owner.get("runtimeProfile") == "journal-bounded",
                "stopped exhaustion requires the dedicated journal runtime")
        stopped(owner)
        require(module.read(state, roots, owner)["branch"] == "bounded-volume-enospc",
                "journal exhaustion cannot use another resource fault")
        changed = [module.fill(view, "bounded-volume-enospc") for view in module.views(state, roots, owner)]
        return {"changes": changed, "quiescent": True, "hostSharedMountRemounted": False,
                "candidateProcessWitnessRequired": True, "productRepairClaimed": False}
    require(set(request) == {"runId", "action", "expected"}, "bounded operation lacks exact process binding")
    before = process(request)
    require(before["pid"] > 1 and isinstance(request["expected"], dict)
            and all(before.get(name) == request["expected"].get(name) for name in PROCESS_FIELDS),
            "bounded operation crossed its expected original process birth")
    views = module.views(state, roots, owner)
    if request["action"] == "bounded-volumes-fill":
        branch = module.read(state, roots, owner)["branch"]
        changed = [module.fill(view, branch) for view in views]
        observed = module.witness(state, roots, owner, before["pid"], exhausted=True)
    elif request["action"] == "bounded-volumes-release":
        changed = [module.release_filler(view) for view in views]
        observed = module.witness(state, roots, owner, before["pid"], exhausted=False)
    else:
        require(request["action"] == "bounded-volumes-witness", "unsupported bounded operation")
        changed = []
        observed = module.witness(state, roots, owner, before["pid"], exhausted=True)
    after = process(request)
    require(all(before.get(name) == after.get(name) for name in PROCESS_FIELDS),
            "bounded operation crossed a candidate process identity")
    return {**observed, "changes": changed, "processBefore": before, "processAfter": after, "sameOriginalProcess": True}


def quorum_gate_operation(request):
    state, roots, owner = owned(request["runId"])
    module = quorum_gate_module()
    if request["action"] == "quorum-gate":
        stopped(owner)
        return module.release(state, roots) if request.get("mode") == "clear" else module.apply(state, roots, request["disks"])
    before = process(request)
    require(before["pid"] > 1 and digest(state / "rustfs") == owner["binarySha256"], "quorum probe candidate differs")
    relative = request["relativePath"]
    require(isinstance(relative, str) and relative.startswith(owner["bucket"] + "/")
            and relative.endswith("/xl.meta"), "quorum read probe escaped its owned business bucket")
    for root in roots:
        checked(root / "data" / relative, root / "data")
    observed = module.probe(state, roots, before["pid"], relative, request["expectedReads"])
    after = process(request)
    require(all(before.get(name) == after.get(name) for name in PROCESS_FIELDS), "quorum probe crossed candidate process identity")
    return {**observed, "processBefore": before, "processAfter": after}


def replacement_module():
    if "replacement" in globals(): return globals()["replacement"]
    from . import replace_drive
    return replace_drive


def replace_drive(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    require(gate_module().checked_gate(state, roots) is None, "replacement cannot share an active drive gate")
    require(quorum_gate_module().checked(state, roots) is None, "replacement cannot share an active quorum gate")
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
    require(quorum_gate_module().checked(state, roots) is None, "single drive gate cannot share a quorum gate")
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


def drive_gate_arm_live(request):
    """Re-arm one owned root without changing the running service's birth."""
    require(set(request) == {"runId", "action", "disk", "expected"}, "live arm requires an exact owned process binding")
    state, roots, owner = owned(request["runId"])
    require(owner.get("runtimeProfile") in {"flapping", "queue-pressure"}, "live arm requires a preconfigured private service")
    before = process(request)
    require(before["pid"] > 1 and isinstance(request["expected"], dict)
            and all(before.get(name) == request["expected"].get(name) for name in PROCESS_FIELDS),
            "live drive gate crossed the expected process birth")
    module = gate_module()
    module.validate_live_boundary(before["pid"])
    require(quorum_gate_module().checked(state, roots) is None, "live drive gate cannot share a quorum gate")
    receipt = module.apply_gate(state, roots, request["disk"], "permission-denied")
    after = process(request)
    require(all(before.get(name) == after.get(name) for name in PROCESS_FIELDS), "live arm crossed its owned process identity")
    return {**receipt, "processBefore": before, "processAfter": after, "sameOriginalProcess": True}


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
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    roots = volume_views(state, roots, owner)
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
    roots = volume_views(state, roots, owner)
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
    roots = volume_views(state, roots, owner)
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
        expected_fault = metadata_faults.get(f"{entry['disk']}:{entry['path']}", metadata_faults.get(entry["path"]))
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
    roots = volume_views(state, roots, owner)
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
        expected_config = runtime_settings(owner)
        require(all(environment.get(key) == value for key, value in expected_config.items()),
                "bound Heal runtime configuration differs")
    files, formats = [], []
    encryption = encryption_module().attest(state, owner, checked, digest,
        None if request.get("quiescent") is True else environment)
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
            "files": sorted(files, key=lambda value: (value["disk"], value["path"])), "formats": formats,
            "encryptionConfigAttestation": encryption,
            "boundedVolumeViews": None if owner.get("runtimeProfile") not in {"bounded-capacity", "journal-bounded"} else {
                "actualBackingRoots": [str(root) for root in roots],
                "privateServiceBindingsRequired": True, "originalPhysicalDriveCountIncreased": False}}


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


def bucket_record_operation(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    module, fault = globals()["bucket_records"], fault_module()
    if request["action"] == "bucket-record-snapshot":
        tool = checked(state / "heal-meta-inspect", state)
        require(owner.get("inspectorSha256") and digest(tool) == owner["inspectorSha256"],
                "bucket-config inspector identity differs")
        return module.snapshot(roots, {**owner, "bucketPrefix": owner["bucket"]}, request, checked, digest, tool, command)
    root, owner = fault_root(request)
    with fault.directory(root) as fd:
        marker = root / "fault-owner.json"
        if request["action"] == "bucket-record-apply" and not marker.exists():
            fault.new_file(fd, marker.name, fault.encode({"schemaVersion": 1, "runId": owner["runId"],
                "bucketPrefix": owner["bucket"], "mountId": fault.mount_id(fd), "device": os.fstat(fd).st_dev,
                "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()}))
        # Retain the common owner/mount/lease checks without widening its
        # generic target allowlist to include any system path.
        binding = fault.checked_owner(fd, owner["runId"], recovery=request["action"] == "bucket-record-recover")
        if request["action"] == "bucket-record-apply":
            return module.apply(fd, binding, fault, request)
        return module.recover(fd, binding, fault, request)


def format_schema_fault(request):
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    replacement = replacement_module().attest(state, roots)
    root, _ = fault_root(request)
    fault = fault_module()
    with fault.directory(root) as fd:
        marker = root / "fault-owner.json"
        if not marker.exists():
            fault.new_file(fd, marker.name, fault.encode({"schemaVersion": 1, "runId": owner["runId"],
                "bucketPrefix": owner["bucket"], "mountId": fault.mount_id(fd), "device": os.fstat(fd).st_dev,
                "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()}))
        fault.checked_owner(fd, owner["runId"])
        return globals()["schema_faults"].install(fd, replacement, owner, request, fault)


def configure_encryption(request):
    state, _, owner = owned(request["runId"])
    stopped(owner)
    return encryption_module().configure(state, owner, request, fault_module())


def journal_operation(request):
    state, roots, owner = owned(request["runId"])
    require(digest(state / "rustfs") == owner["binarySha256"], "durable-record candidate changed")
    require(owner.get("runtimeProfile") in {"root-restart", "mrf-crash", "journal-bounded", "incarnation-checkpoint", "incarnation-mrf"}, "durable records require the owned recovery runtime")
    require(set(request) <= {"runId", "action", "family", "taskId", "disk", "relativePath", "faultId", "kind", "expectedBefore", "expected", "captureId"},
            "durable-record adapter accepts no arbitrary system paths or commands")
    if owner.get("runtimeProfile") == "journal-bounded":
        require(request.get("family") == "checkpoint" or request.get("family") == "root"
                and request.get("action") in {"journal-census", "journal-observe"},
                "bounded journal runtime mutates only the owned Admin checkpoint family")
        roots = volume_views(state, roots, owner)
    module, fault = globals()["journal_fault_backend"], fault_module()
    if request["action"] in {"journal-observe", "journal-capture"}:
        fields = {"runId", "action", "family", "taskId", "expected"}
        if request["action"] == "journal-capture":
            fields |= {"disk", "relativePath", "faultId", "expectedBefore"}
        require(set(request) == fields, "live durable-record observation/capture has unknown mutation or path inputs")
        before = process(request)
        expected = request.get("expected")
        require(isinstance(expected, dict) and before.get("pid", 0) > 1
                and all(name in expected and before.get(name) == expected[name] for name in PROCESS_FIELDS),
                "live durable-record observation lost its exact service birth or candidate")
        observed = module.stable_census(roots, request, fault)
        if request["action"] == "journal-capture":
            require(request["family"] == "checkpoint" and type(request["disk"]) is int and 0 <= request["disk"] < 4,
                    "earlier checkpoint capture has no exact owned Admin record/disk")
            selected = [row for row in observed["records"]
                        if (row["disk"], row["relativePath"]) == (request["disk"], request["relativePath"])]
            require(len(selected) == 1 and selected[0].get("present") is True
                    and module.same_json({name: selected[0].get(name) for name in ("size", "sha256")}, request["expectedBefore"]),
                    "NOT_OBSERVED: earlier checkpoint differs from its two stable process-bound reads")
            with fault.directory(roots[request["disk"]]) as fd:
                observed = {**module.capture(fd, request, owner, fault), "observation": observed}
        after = process(request)
        require(all(after.get(name) == before.get(name) for name in PROCESS_FIELDS),
                "service restarted while observing exact durable records")
        return {**observed, "processIdentity": {name: before[name] for name in PROCESS_FIELDS},
                "processBound": True, "serviceFrozen": before.get("state") == after.get("state") == "T"}
    require("expected" not in request, "stopped durable-record operations accept no live-process override")
    stopped(owner)
    if request["action"] == "journal-census":
        return module.census(roots, request, fault)
    require(request["action"] in {"journal-apply", "journal-recover"}
            and type(request.get("disk")) is int and 0 <= request["disk"] < 4, "invalid durable-record operation or slot")
    root = roots[request["disk"]]
    with fault.directory(root) as fd:
        if request["action"] == "journal-apply":
            if request["family"] == "mrf":
                observed = globals()["mrf"].read_records(state, roots, owner, checked)
                committed = globals()["mrf"].checkpoint(observed)
                require(committed.get("present") is True and committed["intents"]
                        and all(item["bucket"] == owner["bucket"] and item.get("lifecycle", {}).get("state") == {"state": "active"}
                                for item in committed["intents"])
                        and any(row["disk"] == request["disk"] and request["relativePath"] ==
                                f".rustfs.sys/.heal-mrf-snapshot.{row['slot']}.bin" for row in committed["replicas"]),
                        "MRF fault lacks an actual committed active owned responsibility on this exact slot")
            return module.install(fd, request, owner, fault, globals()["mrf"])
        return module.recover(fd, request, owner, fault)


def network_operation(request):
    state, _, owner = owned(request["runId"])
    module = globals()["network_gate_backend"]
    module.identities(owner)
    action = request["action"]
    require(set(request) <= {"runId", "action", "addresses", "localIndex", "blockedIndices", "expired"},
            "network adapter accepts no external paths, ports, rules or commands")
    if action == "network-release":
        require(set(request) <= {"runId", "action", "expired"},
                "network release accepts only its expiry proof")
        return module.release(state, owner, expired=request.get("expired", False))
    identity = process(request)
    require(identity["pid"] > 1 and identity["executable"] == str(state / "rustfs"),
            "network fault requires an actual owned candidate process")
    listeners = command("ss", "-H", "-lntp", "sport = :9000").stdout
    pids = {int(value) for value in re.findall(r"pid=([0-9]+)", listeners)}
    require(pids == {identity["pid"]} and listeners.strip(),
            "TCP 9000 is not exclusively owned by this run's candidate")
    if action == "network-preflight":
        values = module.resolve()
        local = []
        for index, address in enumerate(values):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                try: client.bind((address, 0))
                except OSError: continue
                local.append(index)
        require(len(local) == 1, "fleet aliases do not bind exactly one actual local address")
        return {"addresses": values, "localIndex": local[0], "process": identity,
                "exclusiveServicePort": 9000, "nftVersion": command("nft", "--version").stdout.strip()}
    if action == "network-prepare":
        return module.prepare(state, owner, request, globals()["NETWORK_GATE_SOURCE"])
    if action == "network-activate":
        return module.activate(state, owner)
    require(action == "network-observe", "unsupported network fault operation")
    return {**module.observe(state, owner), "process": identity}


def cleanup(request):
    state, roots, owner = owned(request["runId"])
    command("systemctl", "stop", owner["unit"], check=False)
    pid = command("systemctl", "show", "-p", "MainPID", "--value", owner["unit"], check=False).stdout.strip()
    require(pid in {"", "0"}, "run-owned service remains active")
    stopped(owner)
    if (state / "network-gate.json").exists():
        globals()["network_gate_backend"].release(state, owner)
    gate_module().release_gate(state, roots)
    quorum_gate_module().release(state, roots)
    replacement_module().attest(state, roots)
    if owner.get("runtimeProfile") in {"bounded-capacity", "journal-bounded"}:
        volume_module().dispose(state, roots, owner)
    command("systemctl", "reset-failed", owner["unit"], check=False)
    for root in (*roots, state):
        for directory, children, names in os.walk(root, followlinks=False):
            for name in (*children, *names):
                checked(Path(directory) / name, root)
        shutil.rmtree(root)
    require(not state.exists() and all(not root.exists() for root in roots), "run namespace remains")
    return {"cleanup": "PASS", "unitStopped": True, "rootsRemoved": True, "hostname": owner["hostname"]}


def namespace_state(request):
    """Inspect one exact run namespace without requiring its owner marker."""
    require(set(request) == {"runId", "action"}, "namespace inspection accepts only the run ID")
    state, roots = paths(request["runId"])
    service = {}
    for field in ("LoadState", "ActiveState", "SubState", "MainPID"):
        service[field] = command("systemctl", "show", "-p", field, "--value",
                                 unit(request["runId"]), check=False).stdout.strip()
    return {"runId": request["runId"], "service": service,
            "state": {"path": str(state), "exists": state.exists(),
                      "ownerMarker": (state / "owner.json").is_file()},
            "roots": [{"path": str(root), "exists": root.exists(),
                       "ownerMarker": (root / "owner.json").is_file(),
                       "data": (root / "data").is_dir()} for root in roots]}


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


def quota_audit_finalize(request):
    """Remove the private quota fixture only after exact pre-fault rescue."""
    require(set(request) == {"runId", "action", "bucket", "binarySha256", "expectedSceneSha256"},
            "quota finalization accepts only the exact archived namespace and scene")
    state, roots, owner = owned(request["runId"])
    stopped(owner)
    require(request["runId"].endswith("-h1") and owner.get("runtimeProfile") == "manual"
            and owner.get("bucket") == request["bucket"] and request["binarySha256"] == owner["binarySha256"]
            and digest(state / "rustfs") == owner["binarySha256"], "quota rescue candidate, bucket or private runtime differs")
    require(owner.get("volumes") == [str(root / "data") for root in roots]
            and all(not (state / name).exists() for name in ("network-gate.json", "drive-gate.json", "quorum-gate.json", "bounded-volumes.json")),
            "quota rescue cannot remove a shared/foreign volume or another active fault backend")
    observed = snapshot({"runId": request["runId"], "action": "snapshot", "quiescent": True})
    content = {key: observed[key] for key in ("files", "formats")}
    scene_sha = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    require(re.fullmatch(r"[0-9a-f]{64}", request.get("expectedSceneSha256", "")) is not None
            and scene_sha == request["expectedSceneSha256"], "quota rescue did not restore the complete original fixture")
    result = cleanup(request)
    return {**result, "privateBucketQuotaNamespaceRemoved": True, "archivedPreFaultSceneEqual": True,
            "sceneSha256": scene_sha, "serviceStartedDuringRecovery": False,
            "backupRestorationUsed": True, "productRepairClaimed": False}


def task_records(request):
    """Read three exact Admin journal names; never mutate system metadata."""
    state, roots, owner = owned(request["runId"])
    roots = volume_views(state, roots, owner)
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


def generation_operation(request):
    state, roots, owner = owned(request['runId'])
    stopped(owner)
    require(digest(state / 'rustfs') == owner['binarySha256'], 'generation candidate binary changed')
    disk = request['disk']
    require(type(disk) is int and 0 <= disk < 4, 'generation operation escaped an original independent root')
    operation = request['action']
    require(operation in {'generation-capture', 'generation-install', 'generation-recover'}, 'unregistered generation operation')
    backend = globals()['generation_backend']
    with fault_module().directory(roots[disk]) as root:
        method = {'generation-capture': backend.capture, 'generation-install': backend.install, 'generation-recover': backend.recover}[operation]
        return method(root, owner, request, fault_module())


def orphan_operation(request):
    state, roots, owner = owned(request['runId'])
    stopped(owner)
    require(owner.get('runtimeProfile') in {'orphan-preserve', 'orphan-before', 'orphan-after'}
            and digest(state / 'rustfs') == owner['binarySha256'], 'C10 runtime/candidate changed')
    fault, generation, backend = fault_module(), globals()['generation_backend'], globals()['orphan_backend']
    action = request['action']
    tool = state / 'heal-shared-fixture'
    if action == 'orphan-install-tool':
        require(re.fullmatch(r'[0-9a-f]{64}', request['sha256']), 'C10 encoder digest absent')
        with fault.directory(state) as fd:
            raw = fault.read_file(fd, tool.name)
            require(fault.digest(raw) == request['sha256'] and os.stat(tool, follow_symlinks=False).st_uid == 0,
                    'C10 uploaded encoder changed or is not privately root owned')
            fault.new_file(fd, 'shared-tool.json', fault.encode({'runId': owner['runId'],
                'ownerSha256': digest(state / 'owner.json'), 'binarySha256': owner['binarySha256'], 'sha256': request['sha256']}))
        tool.chmod(0o500)
        return {'installed': True, 'sha256': request['sha256']}
    disk = request['disk']
    require(type(disk) is int and 0 <= disk < 4, 'C10 escaped an original independent root')
    with fault.directory(roots[disk]) as root:
        if action == 'orphan-multipart': return backend.multipart(root, fault)
        if action == 'orphan-observe':
            present, _, files = backend.scene(root, owner, request, fault, generation)
            return {'present': present, 'files': files, 'nodeEpoch': time.time()}
        if action == 'orphan-convert':
            with fault.directory(state) as fd:
                record = json.loads(fault.read_file(fd, 'shared-tool.json', 65536))
                require(record == {'runId': owner['runId'], 'ownerSha256': digest(state / 'owner.json'),
                    'binarySha256': owner['binarySha256'], 'sha256': digest(tool)}, 'C10 encoder binding changed')
            return backend.convert(root, owner, request, fault, generation, checked(tool, state))
        methods = {'orphan-capture': backend.capture, 'orphan-arm': backend.arm, 'orphan-install': backend.install,
                   'orphan-recover': backend.recover, 'orphan-disarm': backend.disarm}
        require(action in methods, 'unregistered C10 operation')
        return methods[action](root, owner, request, fault, generation)


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


def replacement_records(request):
    require(set(request) <= {"runId", "action", "taskId", "quiescent"},
            "replacement journal accepts no paths or mutation options")
    state, roots, owner = owned(request["runId"])
    require(owner.get("runtimeProfile") in {"replacement", "replacement-target"}
            and digest(state / "rustfs") == owner["binarySha256"], "replacement checkpoint candidate/runtime changed")
    if request.get("quiescent") is True: stopped(owner)
    else: require(process(request)["pid"] > 1, "replacement checkpoint has no live owned service")
    return globals()["replacement_journal"].read_records(roots, request.get("taskId"), checked)


def main(request):
    require(os.geteuid() == 0, "node adapter requires passwordless sudo")
    actions = {"preflight": preflight, "reserve": reserve, "start": start, "stop": stop, "snapshot": snapshot,
               "namespace-state": namespace_state,
               "mrf-records": mrf_records,
               "logs": logs, "cleanup": cleanup, "audit-finalize": audit_finalize, "install-inspector": install_inspector,
               "apply-fault": apply_fault, "recover-fault": recover_fault, "verify-part": verify_part, "verify-parts": verify_parts,
               "verify-inline": verify_inline, "drive-gate": drive_gate, "drive-gate-probe": drive_gate_probe,
               "drive-gate-activate": drive_gate_activate, "gate-state": drive_gate_state,
               "drive-gate-release-live": drive_gate_release_live, "drive-gate-arm-live": drive_gate_arm_live}
    for action in ("incarnation-census", "incarnation-capture", "incarnation-install", "incarnation-finalize", "incarnation-phase"):
        actions[action] = incarnation_operation
    actions["task-records"] = task_records
    actions["environment-capabilities"] = environment_capabilities
    actions["quota-audit-finalize"] = quota_audit_finalize
    for action in ("journal-census", "journal-observe", "journal-capture", "journal-apply", "journal-recover"):
        actions[action] = journal_operation
    for action in ('generation-capture', 'generation-install', 'generation-recover'):
        actions[action] = generation_operation
    for action in ('orphan-install-tool', 'orphan-observe', 'orphan-multipart', 'orphan-convert',
                   'orphan-capture', 'orphan-arm', 'orphan-install', 'orphan-recover', 'orphan-disarm'):
        actions[action] = orphan_operation
    for action in ("bucket-record-snapshot", "bucket-record-apply", "bucket-record-recover"):
        actions[action] = bucket_record_operation
    for action in ("quorum-gate", "quorum-gate-probe"):
        actions[action] = quorum_gate_operation
    for action in ("bounded-volumes-create", "bounded-volumes-fill", "bounded-volumes-release", "bounded-volumes-witness"):
        actions[action] = volume_operation
    actions["format-schema-fault"] = format_schema_fault
    actions["encryption-configure"] = configure_encryption
    for action in ("network-preflight", "network-prepare", "network-activate", "network-observe", "network-release"):
        actions[action] = network_operation
    actions["replacement-records"] = replacement_records
    actions.update({"replace-drive": replace_drive, "replacement-state": replacement_state, "replacement-probe": replacement_probe})
    actions.update({"format-export": format_export, "foreign-format": foreign_format, "restore-replacement": restore_replacement})
    actions.update({"process": process, "process-metrics": process_metrics, "process-signal": process_signal, "process-graceful": process_graceful, "process-crash": process_crash})
    require(request.get("action") in actions, "unsupported node operation")
    print(json.dumps(actions[request["action"]](request)))
