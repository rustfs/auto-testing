"""Read-only gates for exact run-owned roots inside the candidate's mount namespace."""

import errno
import json
import os
from pathlib import Path
import stat
import subprocess


MARKER = "write-quorum-gate.json"
PROBE_SOURCE = '''import errno,hashlib,json,os,stat,sys
folder,metadata = sys.argv[1:]
result = {"uid": os.geteuid(), "gid": os.getegid()}
try:
    fd = os.open(metadata, os.O_RDONLY | os.O_NOFOLLOW)
    digest = hashlib.sha256()
    count = 0
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 64*1024**2:
            raise ValueError("expected bounded single-link metadata")
        while chunk := os.read(fd, 1024**2):
            count += len(chunk)
            if count > 64*1024**2: raise ValueError("metadata grew")
            digest.update(chunk)
    finally:
        os.close(fd)
    result["read"] = {"errno": 0, "size": count, "sha256": digest.hexdigest()}
except OSError as exc:
    result["read"] = {"errno": exc.errno}
name = folder + "/.validation-quorum-write-probe"
try:
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
except OSError as exc:
    result["writeErrno"] = exc.errno
else:
    try:
        os.write(fd, b"quorum-probe")
        os.fsync(fd)
    finally:
        os.close(fd)
        os.unlink(name)
    result["writeErrno"] = 0
print(json.dumps(result))
'''


def require(value, message):
    if not value:
        raise ValueError(message)


def checked(state, roots):
    path = state / MARKER
    if not path.exists():
        return None
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
            and stat.S_IMODE(info.st_mode) == 0o600 and info.st_size < 4096, "invalid quorum gate marker")
    value = json.loads(path.read_text())
    entries = value.get("roots")
    require(value.get("schemaVersion") == 1 and value.get("mode") == "read-only"
            and isinstance(entries, list) and 0 < len(entries) <= len(roots), "invalid quorum gate contract")
    disks = [row.get("disk") for row in entries]
    require(all(type(disk) is int and 0 <= disk < len(roots) for disk in disks)
            and disks == sorted(set(disks)), "quorum gate disks differ")
    for row in entries:
        folder = roots[row["disk"]] / "data"
        info = folder.lstat()
        require(row["path"] == str(folder) and not folder.is_symlink() and stat.S_ISDIR(info.st_mode)
                and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700
                and row["device"] == info.st_dev and row["inode"] == info.st_ino,
                "quorum gate root ownership/device/inode drifted; retain namespace")
    return value


def apply(state, roots, disks):
    require(isinstance(disks, list) and bool(disks) and disks == sorted(set(disks))
            and all(type(disk) is int and 0 <= disk < len(roots) for disk in disks), "invalid quorum gate disk list")
    require(checked(state, roots) is None and not (state / "drive-gate.json").exists()
            and not (state / "disk-replacement.json").exists(), "quorum gate cannot share another drive mutation")
    rows = []
    for disk in disks:
        folder = roots[disk] / "data"
        info = folder.lstat()
        require(not folder.is_symlink() and stat.S_ISDIR(info.st_mode) and info.st_uid == 0
                and stat.S_IMODE(info.st_mode) == 0o700, "expected private owned data root")
        rows.append({"disk": disk, "path": str(folder), "device": info.st_dev, "inode": info.st_ino})
    value = {"schemaVersion": 1, "mode": "read-only", "roots": rows}
    fd = os.open(state / MARKER, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    require(checked(state, roots) == value, "quorum gate intent differs")
    return {**value, "state": "ARMED", "hostMountRemounted": False, "sharedPermissionsChanged": False}


def properties(state, roots):
    value = checked(state, roots)
    return [] if value is None else ["--property=PrivateMounts=true",
        "--property=ReadOnlyPaths=" + " ".join(row["path"] for row in value["roots"])]


def release(state, roots):
    value = checked(state, roots)
    if value is not None:
        (state / MARKER).unlink()
    return {"state": "CLEAR", "gateWasAbsent": value is None, "hostMountRemounted": False,
            "privateMountRemovedByStoppedService": True}


def validate_probe(observed, expected_read, read_only):
    require(observed.get("uid") == observed.get("gid") == 0, "quorum probe differs from root service identity")
    require(observed.get("read") == {"errno": 0, **expected_read}, "quorum probe cannot read exact original metadata")
    require(observed.get("writeErrno") == (errno.EROFS if read_only else 0),
            "quorum probe did not witness the expected actual writable/refused root")
    return {"readableMetadata": True, "writable": not read_only, "writeErrno": observed["writeErrno"],
            "sameServiceUidAndMountNamespace": True}


def probe(state, roots, pid, relative, expected_reads):
    value = checked(state, roots)
    gated = {row["disk"] for row in value["roots"]} if value else set()
    require(type(pid) is int and pid > 1 and len(expected_reads) == len(roots), "live service and all disk expectations required")
    namespace = Path(f"/proc/{pid}/ns/mnt")
    if gated:
        require(namespace.readlink() != Path('/proc/self/ns/mnt').readlink(), "read-only gate lacks private mount namespace")
    fields = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines() if ":" in line)
    require(list(map(int, fields["Uid"].split()))[1] == list(map(int, fields["Gid"].split()))[1] == 0,
            "quorum candidate effective identity changed")
    rows = []
    for disk, root in enumerate(roots):
        result = subprocess.run(["nsenter", "--mount=" + str(namespace), "python3", "-c", PROBE_SOURCE,
                                 str(root / "data"), str(root / "data" / relative)],
                                capture_output=True, text=True, timeout=15)
        require(result.returncode == 0, "candidate-namespace quorum probe did not complete")
        observed = json.loads(result.stdout)
        rows.append({"disk": disk, "observed": observed,
                     **validate_probe(observed, expected_reads[disk], disk in gated)})
    return {"disks": rows, "backend": "candidate-service-private-read-only-mounts",
            "hostMountRemounted": False, "sharedPermissionsChanged": False}
