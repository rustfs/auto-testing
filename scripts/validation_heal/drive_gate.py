"""B02 gates in run-owned units; the host's data mounts are never remounted."""

import errno
import json
import os
from pathlib import Path
import stat
import subprocess


DENY_DAC_CAPS = (1 << 1) | (1 << 2)  # CAP_DAC_OVERRIDE, CAP_DAC_READ_SEARCH
PROBE_SOURCE = '''import errno,json,os
path = __import__('sys').argv[1]
result = {"uid": os.geteuid(), "gid": os.getegid()}
try:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
except OSError as exc:
    result["errno"] = exc.errno
else:
    os.close(fd)
    os.unlink(path)
    result["errno"] = 0
print(json.dumps(result))
'''
READ_PROBE_SOURCE = '''import json,os
result = {"uid": os.geteuid(), "gid": os.getegid()}
try:
    fd = os.open(__import__('sys').argv[1], os.O_RDONLY | os.O_NOFOLLOW)
except OSError as exc:
    result["errno"] = exc.errno
else:
    os.close(fd)
    result["errno"] = 0
print(json.dumps(result))
'''


def require(value, message):
    if not value: raise ValueError(message)


def checked_gate(state, roots):
    path = state / "drive-gate.json"
    if not path.exists(): return None
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
            and info.st_size < 4096 and info.st_mode & 0o077 == 0, "invalid drive gate ownership")
    value = json.loads(path.read_text())
    require(value.get("schemaVersion") == 1 and value.get("mode") in {"read-only", "permission-denied"}
            and value.get("phase", "active") in {"armed", "active"}
            and type(value.get("disk")) is int and 0 <= value["disk"] < len(roots)
            and value.get("dataPath") == str(roots[value["disk"]] / "data")
            and type(value.get("originalMode")) is int and value["originalMode"] == 0o700,
            "drive gate no longer matches the run-owned data root")
    return value


def apply_gate(state, roots, disk, mode):
    require(type(disk) is int and 0 <= disk < len(roots) and mode in {"read-only", "permission-denied"}, "invalid drive gate")
    require(checked_gate(state, roots) is None, "a drive gate is already active")
    folder = roots[disk] / "data"
    info = folder.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700,
            "expected private run-owned root with original mode 0700")
    value = {"schemaVersion": 1, "disk": disk, "mode": mode, "dataPath": str(folder),
             "originalMode": 0o700, "device": info.st_dev, "inode": info.st_ino,
             "phase": "armed" if mode == "permission-denied" else "active"}
    # Intent precedes chmod/start. Cleanup can restore a partially applied gate.
    fd = os.open(state / "drive-gate.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    return {**value, "state": "ARMED", "hostMountRemounted": False, "sharedPermissionsChanged": False}


def activate_gate(state, roots):
    value = checked_gate(state, roots)
    require(value and value["mode"] == "permission-denied" and value.get("phase") == "armed",
            "permission activation requires a newly armed run-owned gate")
    folder = Path(value["dataPath"])
    info = folder.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and info.st_dev == value["device"]
            and info.st_ino == value["inode"] and stat.S_IMODE(info.st_mode) == 0o700,
            "armed permission root changed before live injection")
    # Keep the recovery intent before chmod. A partially activated gate remains
    # recoverable from the armed marker and original inode/mode.
    folder.chmod(0)
    temporary = state / "drive-gate-active.json"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    value["phase"] = "active"
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, state / "drive-gate.json")
    return {**value, "state": "ACTIVE", "injectedAfterActualServiceStart": True,
            "hostMountRemounted": False, "sharedPermissionsChanged": False}


def release_gate(state, roots):
    value = checked_gate(state, roots)
    if value is None: return {"state": "CLEAR", "gateWasAbsent": True}
    folder = Path(value["dataPath"])
    info = folder.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and info.st_dev == value["device"] and info.st_ino == value["inode"],
            "gated data root changed; preserve namespace")
    if value["mode"] == "permission-denied":
        require(stat.S_IMODE(info.st_mode) in {0, value["originalMode"]}, "gated permissions drifted")
        folder.chmod(value["originalMode"])
    require(stat.S_IMODE(folder.stat().st_mode) == value["originalMode"], "data root permissions were not restored")
    (state / "drive-gate.json").unlink()
    return {"state": "CLEAR", "originalPermissionsRestored": True, "hostMountRemounted": False}


def start_properties(value):
    if value is None: return []
    properties = ["--property=PrivateMounts=true"]
    if value["mode"] == "read-only": properties.append("--property=ReadOnlyPaths=" + value["dataPath"])
    else: properties.append("--property=CapabilityBoundingSet=~CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH")
    return properties


def process_identity(pid):
    text = Path(f"/proc/{pid}/status").read_text()
    fields = dict(line.split(":", 1) for line in text.splitlines() if ":" in line)
    uid, gid = list(map(int, fields["Uid"].split())), list(map(int, fields["Gid"].split()))
    return {"uid": uid[1], "gid": gid[1], "capEff": int(fields["CapEff"].strip(), 16)}


def validate_probe(observed, mode, identity):
    expected = errno.EROFS if mode == "read-only" else errno.EACCES
    require(identity["uid"] == observed.get("uid") == 0 and identity["gid"] == observed.get("gid") == 0,
            "write probe does not share the service identity")
    require(observed.get("errno") == expected, "drive gate did not cause its precise Linux write refusal")
    if mode == "permission-denied":
        require(identity["capEff"] & DENY_DAC_CAPS == 0, "root service still bypasses filesystem permissions")
    return {"errno": expected, "name": errno.errorcode[expected], "sameServiceIdentity": True,
            "dacBypassRemoved": mode == "permission-denied"}


def probe_gate(state, roots, pid):
    value = checked_gate(state, roots)
    require(value is not None and type(pid) is int and pid > 0, "active gated service required")
    require(value.get("phase", "active") == "active", "permission gate is still armed; no refusal claim")
    identity = process_identity(pid)
    require(Path(f"/proc/{pid}/ns/mnt").readlink() != Path("/proc/self/ns/mnt").readlink(),
            "gate requires an independent service mount namespace")
    path = value["dataPath"] + "/.validation-write-probe"
    args = ["nsenter", "--mount=/proc/" + str(pid) + "/ns/mnt"]
    if value["mode"] == "permission-denied": args += ["setpriv", "--bounding-set=-dac_override,-dac_read_search"]
    args += ["python3", "-c", PROBE_SOURCE, path]
    result = subprocess.run(args, capture_output=True, text=True, timeout=15)
    require(result.returncode == 0, "service-namespace write probe did not complete")
    observed = json.loads(result.stdout)
    read = None
    if value["mode"] == "permission-denied":
        # A denied write alone cannot prove a stale member cannot read. Use
        # the same namespace/identity/capability boundary to open its format.
        read_args = [*args[:-4], "python3", "-c", READ_PROBE_SOURCE,
                     value["dataPath"] + "/.rustfs.sys/format.json"]
        result = subprocess.run(read_args, capture_output=True, text=True, timeout=15)
        require(result.returncode == 0, "service-namespace read probe did not complete")
        read = json.loads(result.stdout)
        validate_probe(read, value["mode"], identity)
    return {**validate_probe(observed, value["mode"], identity), "disk": value["disk"],
            "dataPath": value["dataPath"], "readProbe": read,
            "privateMountNamespace": True, "hostMountRemounted": False, "observed": observed}


def validate_unwritable(status, bucket, entry):
    outcome = status.get("outcome") or {}
    objects = outcome.get("objects", [])
    terminal = status.get("summary") == "finished" or status.get("summary") == "stopped" \
        and outcome.get("execution", {}).get("state") == "completed_with_errors"
    require(terminal and status.get("truncated", False) is False
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False
            and len(objects) == 1, "unwritable task did not preserve complete exact responsibility")
    receipt = objects[0]
    identity = receipt.get("identity", {})
    public_version = identity.get("versionId") or "null"
    if public_version == "00000000-0000-0000-0000-000000000000": public_version = "null"
    require(identity.get("bucket") == bucket and identity.get("object") == entry["key"]
            and public_version == entry["versionId"] and identity.get("bucketIncarnationId")
            and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
            and type(identity.get("setIndex")) is int and identity["setIndex"] == 0,
            "unwritable target receipt has a wrong or incomplete identity")
    disposition = receipt.get("disposition", {})
    state, detail = disposition.get("state"), disposition.get("details")
    require(state == "failed" and detail in {"recoverable", "retry_exhausted", "permanent"}
            or state == "deferred" and isinstance(detail, dict)
            and detail.get("reason") in {"deadline", "transient_existence_check"},
            "unwritable target was claimed repaired/healthy/absent or has an unrelated pending reason")
    failed = state == "failed"
    require(outcome.get("execution", {}).get("state") == ("completed_with_errors" if failed else "completed"),
            "unwritable execution hides the unresolved result")
    wanted = {"processed": 1, "healed": 0, "unchanged": 0, "failed": int(failed),
              "skipped": int(not failed), "unknown": 0, "overflowed": False}
    counters = outcome.get("counters", {})
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in wanted.items())
            and type(counters.get("attemptFailures")) is int and counters["attemptFailures"] >= int(failed),
            "unwritable responsibility counters claim success or hide a failure")
    return {"disposition": disposition, "repairClaimedWhileUnwritable": False, "completeResponsibility": True}
