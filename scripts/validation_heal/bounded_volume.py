"""I02: fixed-size filesystems bound only into the run-owned service's roots."""

import errno
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time
import uuid


MARKER = "bounded-volumes.json"
IMAGE_BYTES = 128 * 1024**2
MAX_INODES = 1024
MIN_SHARED_FREE = 1024**3
PROBE = '''import errno,json,os,sys
folder=sys.argv[1]
info=os.statvfs(folder)
result={"uid":os.geteuid(),"gid":os.getegid(),"device":os.stat(folder).st_dev,
        "freeBlocks":info.f_bfree,"availableBlocks":info.f_bavail,"freeInodes":info.f_ffree}
name=folder+"/.i02-write-witness"
fd=None
try:
    fd=os.open(name,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    os.write(fd,b"i02"*2048)
    os.fsync(fd)
except OSError as exc:
    result["writeErrno"]=exc.errno
else:
    result["writeErrno"]=0
finally:
    if fd is not None:
        os.close(fd)
        os.unlink(name)
print(json.dumps(result))
'''


def require(value, message):
    if not value:
        raise ValueError(message)


def command(*args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=90)
    require(result.returncode == 0, f"bounded-volume {args[0]} failed: {result.stderr[-800:]}")
    return result.stdout


def regular(path, *, size=None):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
            and stat.S_IMODE(info.st_mode) == 0o600 and (size is None or info.st_size == size),
            "bounded-volume file ownership/type/size differs")
    return info


def read(state, roots, owner):
    path = state / MARKER
    if not path.exists():
        return None
    require(regular(path).st_size < 16384, "bounded-volume marker exceeds budget")
    value = json.loads(path.read_text())
    require(value.get("schemaVersion") == 1 and value.get("runId") == owner["runId"]
            and value.get("branch") in {"bounded-volume-enospc", "bounded-inode-exhaustion"}
            and value.get("imageBytes") == IMAGE_BYTES and value.get("maxInodes") == MAX_INODES
            and len(value.get("volumes", [])) == len(roots) == 4,
            "bounded-volume contract differs from its original run")
    require(owner.get("runtimeProfile") != "journal-bounded" or value["branch"] == "bounded-volume-enospc",
            "bounded journal runtime has no reviewed inode-exhaustion branch")
    for index, row in enumerate(value["volumes"]):
        require(type(row.get("disk")) is int and row["disk"] == index and row.get("image") == str(roots[index] / "bounded-volume.img")
                and row.get("mount") == str(state / "bounded" / f"disk{index}")
                and str(uuid.UUID(row["uuid"])) == row["uuid"], "bounded-volume paths or filesystem identities differ")
        folder = Path(row["mount"])
        require(not any(path.is_symlink() for path in (folder, *folder.parents)), "symlink in bounded mount boundary")
    return value


def loop_for(image):
    rows = json.loads(command("losetup", "--json", "--list", "--output", "NAME,BACK-FILE,BACK-INO,BACK-MAJ:MIN"))["loopdevices"]
    selected = [row for row in rows if row["back-file"] == str(image)]
    if selected:
        info = regular(image)
        require(all(int(row["back-ino"]) == info.st_ino
                    and row["back-maj:min"] == f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}" for row in selected),
                "loop attachment belongs to a different backing inode/device")
    found = [row["name"] for row in selected]
    require(len(found) <= 1 and all(re.fullmatch(r"/dev/loop[0-9]+", item) for item in found),
            "bounded image has ambiguous loop attachments")
    return found[0] if found else None


def mounted(row):
    observed = subprocess.run(["findmnt", "--json", "--mountpoint", row["mount"], "--output",
                               "SOURCE,FSTYPE,UUID,TARGET,MAJ:MIN,OPTIONS"], capture_output=True, text=True, timeout=15)
    if observed.returncode == 1:
        return None
    require(observed.returncode == 0, "bounded mount identity unavailable")
    entries = json.loads(observed.stdout).get("filesystems", [])
    require(len(entries) == 1, "bounded mount is ambiguous")
    entry = entries[0]
    require(entry["target"] == row["mount"] and entry["fstype"] == "ext4" and entry["uuid"] == row["uuid"]
            and entry["source"] == loop_for(Path(row["image"])) and "rw" in entry["options"].split(","),
            "bounded mount no longer belongs to its exact image/UUID")
    return entry


def configure_block_exhaustion(row):
    # ext4's internal reserve is separate from mkfs -m 0. Only this newly
    # created, UUID/backing-inode-bound loop may expose that reserve to padding.
    entry = mounted(row)
    require(entry is not None and re.fullmatch(r"/dev/loop[0-9]+", entry["source"]),
            "block exhaustion requires its exact mounted private loop")
    source = entry["source"]
    knob = Path("/sys/fs/ext4") / Path(source).name / "reserved_clusters"
    fd = os.open(knob, os.O_RDWR | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0,
                "private ext4 reserve control is not root-owned sysfs data")
        raw = os.read(fd, 64)
        require(re.fullmatch(rb"[0-9]{1,20}\n", raw) is not None,
                "private ext4 reserve is not a bounded cluster count")
        before = int(raw)
        require(before <= IMAGE_BYTES // 1024 and mounted(row) == entry,
                "private ext4 mount changed before configuring its reserve")
        os.lseek(fd, 0, os.SEEK_SET)
        require(os.write(fd, b"0\n") == 2, "private ext4 reserve write was incomplete")
        os.lseek(fd, 0, os.SEEK_SET)
        require(os.read(fd, 64) == b"0\n" and mounted(row) == entry,
                "private ext4 reserve or mount differs after configuration")
    finally:
        os.close(fd)
    return {"disk": row["disk"], "uuid": row["uuid"], "source": source,
            "reservedClustersBefore": before, "reservedClustersAfter": 0,
            "sharedFilesystemModified": False}


def create(state, roots, owner, branch):
    require(owner.get("runtimeProfile") in {"bounded-capacity", "journal-bounded"} and read(state, roots, owner) is None
            and branch in {"bounded-volume-enospc", "bounded-inode-exhaustion"}, "invalid bounded capacity allocation")
    require(owner.get("runtimeProfile") != "journal-bounded" or branch == "bounded-volume-enospc",
            "bounded journal allocation accepts only fixed block exhaustion")
    require(all(not any((root / "data").iterdir()) and not (root / "bounded-volume.img").exists()
                and shutil.disk_usage(root).free >= MIN_SHARED_FREE + IMAGE_BYTES for root in roots),
            "bounded allocation requires empty owned roots and shared-drive safety margin")
    value = {"schemaVersion": 1, "runId": owner["runId"], "branch": branch, "imageBytes": IMAGE_BYTES,
             "maxInodes": MAX_INODES, "volumes": [{"disk": index, "image": str(root / "bounded-volume.img"),
                "mount": str(state / "bounded" / f"disk{index}"), "uuid": str(uuid.uuid4())}
                for index, root in enumerate(roots)]}
    # The complete undo identity exists before allocating an image or loop.
    fd = os.open(state / MARKER, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    (state / "bounded").mkdir(mode=0o700)
    reserve_configuration = []
    for row in value["volumes"]:
        image = Path(row["image"])
        descriptor = os.open(image, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            os.posix_fallocate(descriptor, 0, IMAGE_BYTES)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        regular(image, size=IMAGE_BYTES)
        # mkfs receives an exclusively created bounded regular file, never a
        # supplied device. The subsequently attached loop is derived from it.
        command("mkfs.ext4", "-F", "-q", "-m", "0", "-N", str(MAX_INODES), "-U", row["uuid"], str(image))
        loop = command("losetup", "--find", "--show", "--nooverlap", str(image)).strip()
        require(loop == loop_for(image), "new loop identity differs")
        folder = Path(row["mount"])
        folder.mkdir(mode=0o700)
        command("mount", "-t", "ext4", "-o", "nodev,nosuid,noexec", loop, str(folder))
        mounted(row)
        if branch == "bounded-volume-enospc":
            reserve_configuration.append(configure_block_exhaustion(row))
        folder.chmod(0o700)
        for name in ("data", "quarantine", "padding"):
            (folder / name).mkdir(mode=0o700)
        info = os.statvfs(folder)
        require(info.f_blocks * info.f_frsize <= IMAGE_BYTES and 0 < info.f_files <= MAX_INODES,
                "actual bounded filesystem exceeds registered block/inode limits")
    return {**value, "ext4ReservedClusterConfiguration": reserve_configuration,
            "hostSharedMountRemounted": False, "allocatedBytesPerNode": len(roots) * IMAGE_BYTES,
            "independentPhysicalDriveCountIncreased": False}


def views(state, roots, owner):
    value = read(state, roots, owner)
    if value is None:
        require(owner.get("runtimeProfile") not in {"bounded-capacity", "journal-bounded"}, "bounded runtime has no durable allocation intent")
        return roots
    result = []
    for original, row in zip(roots, value["volumes"], strict=True):
        require(not any((original / "data").iterdir()), "candidate wrote outside its private bounded binding")
        regular(Path(row["image"]), size=IMAGE_BYTES)
        require(mounted(row) is not None, "bounded filesystem is not mounted")
        folder = Path(row["mount"])
        require(folder.stat().st_uid == 0 and stat.S_IMODE(folder.stat().st_mode) == 0o700,
                "bounded filesystem root is not private/root-owned")
        for name in ("data", "quarantine", "padding"):
            path = folder / name
            info = path.lstat()
            require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not path.is_symlink()
                    and info.st_dev == folder.stat().st_dev and stat.S_IMODE(info.st_mode) == 0o700,
                    "bounded data/quarantine/padding boundary differs")
        result.append(folder)
    return tuple(result)


def properties(state, roots, owner):
    if read(state, roots, owner) is None:
        require(owner.get("runtimeProfile") not in {"bounded-capacity", "journal-bounded"}, "bounded runtime is not configured")
        return []
    actual = views(state, roots, owner)
    return ["--property=PrivateMounts=true", "--property=BindPaths=" + " ".join(
        f"{view / 'data'}:{root / 'data'}" for view, root in zip(actual, roots, strict=True))]


def fill(folder, branch):
    padding = folder / "padding"
    require(not any(padding.iterdir()), "bounded filler is already present")
    deadline, written, count = time.monotonic() + 60, 0, 0
    if branch == "bounded-volume-enospc":
        fd = os.open(padding / "blocks.bin", os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        chunk, file_written = 1024**2, 0
        try:
            while True:
                require(time.monotonic() < deadline and written < IMAGE_BYTES, "bounded block fill exceeded its budget")
                try:
                    requested = min(chunk, IMAGE_BYTES - written)
                    actual = os.write(fd, b"\0" * requested)
                    require(0 < actual <= requested, "bounded block fill made no valid byte progress")
                    written += actual
                    file_written += actual
                    os.fsync(fd)
                except OSError as error:
                    require(error.errno == errno.ENOSPC, "bounded fill reached an unrelated IO error")
                    info = os.statvfs(folder)
                    if info.f_bfree == 0:
                        break
                    require(type(info.f_frsize) is int and 0 < info.f_frsize <= chunk,
                            "bounded fill has no valid minimum allocation unit")
                    if info.f_frsize < chunk:
                        chunk = info.f_frsize
                        continue
                    # A separate private inode may allocate a final block that
                    # the current file cannot extend into. Never retry an empty
                    # inode or exceed the fixture's 128 padding-inode budget.
                    require(file_written > 0 and count < IMAGE_BYTES // 1024**2 - 1,
                            f"bounded minimum-block fill left {info.f_bfree} free blocks")
                    os.close(fd)
                    fd = None
                    fd = os.open(padding / f"inode-{count:04d}", os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                    count += 1
                    file_written = 0
        finally:
            if fd is not None:
                os.close(fd)
    else:
        require(branch == "bounded-inode-exhaustion", "unreviewed bounded fill mode")
        while True:
            require(count < MAX_INODES and time.monotonic() < deadline, "bounded inode fill exceeded its budget")
            try:
                fd = os.open(padding / f"inode-{count:04d}", os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            except OSError as error:
                require(error.errno == errno.ENOSPC, "bounded inode fill reached an unrelated IO error")
                break
            os.fsync(fd)
            os.close(fd)
            count += 1
    info = os.statvfs(folder)
    require(info.f_bfree == 0 if branch == "bounded-volume-enospc" else info.f_ffree == 0,
            "bounded fill did not exhaust its registered resource")
    return {"allocatedFillerBytes": written, "allocatedFillerInodes": count, "actualErrno": errno.ENOSPC,
            "freeBlocks": info.f_bfree, "freeInodes": info.f_ffree}


def release_filler(folder):
    padding = folder / "padding"
    entries = list(padding.iterdir())
    require(len(entries) <= MAX_INODES and all(path.name == "blocks.bin" or re.fullmatch(r"inode-[0-9]{4}", path.name)
                for path in entries), "unrecognized file in bounded filler; retain namespace")
    for path in entries:
        require(regular(path).st_size <= IMAGE_BYTES, "bounded filler grew outside its limit")
        path.unlink()
    descriptor = os.open(padding, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    info = os.statvfs(folder)
    require(info.f_bfree > 0 and info.f_ffree > 0, "bounded resource was not released")
    return {"removedOwnedFillerFiles": len(entries), "freeBlocks": info.f_bfree, "freeInodes": info.f_ffree}


def witness(state, roots, owner, pid, *, exhausted):
    actual = views(state, roots, owner)
    namespace = Path(f"/proc/{pid}/ns/mnt")
    require(pid > 1 and namespace.readlink() != Path('/proc/self/ns/mnt').readlink(),
            "bounded witness lacks a private candidate mount namespace")
    fields = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines() if ":" in line)
    require(list(map(int, fields["Uid"].split()))[1] == list(map(int, fields["Gid"].split()))[1] == 0,
            "bounded witness service UID/GID changed")
    branch = read(state, roots, owner)["branch"]
    rows = []
    for disk, (root, view) in enumerate(zip(roots, actual, strict=True)):
        raw = json.loads(command("nsenter", "--mount=" + str(namespace), "python3", "-c", PROBE, str(root / "data")))
        validate_probe(raw, view.stat().st_dev, branch, exhausted)
        rows.append({"disk": disk, **raw, "sameServiceUidAndMountNamespace": True})
    require(all(shutil.disk_usage(root).free >= MIN_SHARED_FREE for root in roots),
            "shared backing drive crossed its retained safety margin")
    return {"branch": branch, "disks": rows, "hostSharedMountRemounted": False,
            "realMediaDamageClaimed": False, "sharedDriveSafetyMarginBytes": MIN_SHARED_FREE}


def validate_probe(raw, device, branch, exhausted):
    require(branch in {"bounded-volume-enospc", "bounded-inode-exhaustion"} and type(exhausted) is bool
            and all(type(raw.get(name)) is int and raw[name] >= 0 for name in
                ("uid", "gid", "device", "writeErrno", "freeBlocks", "freeInodes", "availableBlocks")),
            "bounded probe omitted a real integer resource/identity value")
    require(raw["uid"] == raw["gid"] == 0 and raw["device"] == device
            and raw["writeErrno"] == (errno.ENOSPC if exhausted else 0),
            "candidate mount/UID did not witness the exact bounded write error")
    if exhausted:
        require(raw["freeBlocks"] == 0 if branch == "bounded-volume-enospc" else raw["freeInodes"] == 0,
                "candidate resource view differs from the exhausted filesystem")
    else:
        require(raw["freeBlocks"] > 0 and raw["freeInodes"] > 0, "released candidate resource remains exhausted")


def dispose(state, roots, owner):
    value = read(state, roots, owner)
    if value is None:
        return {"boundedVolumesAbsent": True}
    for row in value["volumes"]:
        image, folder = Path(row["image"]), Path(row["mount"])
        if image.exists():
            info = regular(image)
            require(info.st_size in {0, IMAGE_BYTES}, "partially allocated image has unrecognized size")
            entry = mounted(row)
            if entry is not None:
                command("umount", str(folder))
                require(mounted(row) is None, "bounded filesystem remains mounted")
            loop = loop_for(image)
            if loop:
                command("losetup", "--detach", loop)
                require(loop_for(image) is None, "bounded loop remains attached")
            image.unlink()
        else:
            require(not folder.exists() or not os.path.ismount(folder), "missing image has a retained mount")
        if folder.exists():
            require(not folder.is_symlink() and not any(folder.iterdir()), "bounded mountpoint is not empty after detach")
            folder.rmdir()
    if (state / "bounded").exists():
        (state / "bounded").rmdir()
    (state / MARKER).unlink()
    return {"boundedVolumesRemoved": True, "originalSharedMountsRetained": True}
