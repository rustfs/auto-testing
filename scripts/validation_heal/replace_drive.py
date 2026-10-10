"""Whole private logical-root replacement; the shared mount stays attached."""

import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import uuid

if __package__:
    from .drive_gate import DENY_DAC_CAPS, READ_PROBE_SOURCE, process_identity
else:
    from drive_gate import DENY_DAC_CAPS, READ_PROBE_SOURCE, process_identity


def require(value, message):
    if not value: raise ValueError(message)


def digest(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 64 * 1024**2,
            "replacement archive contains a link or unsupported file")
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024**2): value.update(chunk)
    return value.hexdigest()


def tree_identity(root):
    info = root.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700,
            "replacement original root is not private/root-owned")
    rows = []
    for folder, children, names in os.walk(root, followlinks=False):
        for name in (*children, *names):
            path = Path(folder) / name
            item = path.lstat()
            require(item.st_dev == info.st_dev and not path.is_symlink(), "replacement archive crossed a mount or link")
            if stat.S_ISDIR(item.st_mode): continue
            require(len(rows) < 16384, "replacement archive exceeds the registered file budget")
            rows.append({"path": path.relative_to(root).as_posix(), "size": item.st_size, "sha256": digest(path)})
    value = json.dumps(sorted(rows, key=lambda item: item["path"]), sort_keys=True, separators=(",", ":")).encode()
    return {"sha256": hashlib.sha256(value).hexdigest(), "files": len(rows), "device": info.st_dev, "inode": info.st_ino}


def checked_replacement(state, roots):
    path = state / "disk-replacement.json"
    if not path.exists(): return None
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
            and stat.S_IMODE(info.st_mode) == 0o600 and info.st_size < 4096, "invalid replacement ownership marker")
    value = json.loads(path.read_text())
    disk = value.get("disk")
    require(value.get("schemaVersion") == 1 and type(disk) is int and 0 <= disk < len(roots)
            and value.get("dataPath") == str(roots[disk] / "data")
            and value.get("backupPath") == str(roots[disk] / "quarantine" / "old-disk")
            and value.get("phase") == "armed" and isinstance(value.get("original"), dict),
            "replacement marker moved outside its exact private member")
    quarantine = roots[disk] / "quarantine"
    info = quarantine.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700
            and info.st_dev == roots[disk].lstat().st_dev, "replacement quarantine crossed a link, mount or owner")
    return value


def begin(state, roots, disk, expected_format_sha256):
    require(type(disk) is int and 0 <= disk < len(roots) and checked_replacement(state, roots) is None,
            "invalid or repeated logical disk replacement")
    data, backup = roots[disk] / "data", roots[disk] / "quarantine" / "old-disk"
    quarantine = backup.parent.lstat()
    require(stat.S_ISDIR(quarantine.st_mode) and quarantine.st_uid == 0 and stat.S_IMODE(quarantine.st_mode) == 0o700
            and quarantine.st_dev == roots[disk].lstat().st_dev, "replacement quarantine is not an exact private same-mount directory")
    require(not backup.exists() and not backup.is_symlink(), "replacement original backup already exists")
    original = tree_identity(data)
    format_path = data / ".rustfs.sys" / "format.json"
    require(digest(format_path) == expected_format_sha256, "original disk format differs before whole-root replacement")
    value = {"schemaVersion": 1, "phase": "armed", "disk": disk, "dataPath": str(data),
             "backupPath": str(backup), "original": original, "originalFormatSha256": expected_format_sha256}
    fd = os.open(state / "disk-replacement.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream); stream.flush(); os.fsync(stream.fileno())
    os.rename(data, backup)
    data.mkdir(mode=0o700)
    for folder in (roots[disk], backup.parent):
        fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    require(tree_identity(backup) == original and not any(data.iterdir()), "replacement did not retain exact original bytes and an empty new root")
    return {**value, "oldRootMovedIntact": True, "newDataRootEmpty": True,
            "sharedMountDetached": False, "backupRestorationUsed": False}


def attest(state, roots):
    value = checked_replacement(state, roots)
    if value is None: return {"replacementAbsent": True}
    backup, data = Path(value["backupPath"]), Path(value["dataPath"])
    # An interrupted begin remains recoverable only when the original tree is
    # still at one of the two exact intended roots; otherwise retain the run.
    original_path = backup if backup.exists() else data
    require(tree_identity(original_path) == value["original"], "original replacement archive drifted; retain namespace")
    return {**value, "originalArchiveUnchanged": True, "originalStillAtDataRoot": original_path == data}


def install_foreign(state, roots, raw_base64, expected_sha256):
    value = attest(state, roots)
    require(not value.get("replacementAbsent") and not value["originalStillAtDataRoot"], "foreign format requires an isolated original member")
    data = Path(value["dataPath"])
    require(not any(data.iterdir()), "foreign format requires the witnessed empty replacement root")
    raw = base64.b64decode(raw_base64, validate=True)
    require(len(raw) < 65536 and hashlib.sha256(raw).hexdigest() == expected_sha256, "foreign donor bytes differ from their exported digest")
    donor = json.loads(raw)
    original = json.loads((Path(value["backupPath"]) / ".rustfs.sys/format.json").read_bytes())
    require(donor.get("version") == "1" and donor.get("format") == "xl" and donor.get("xl", {}).get("version") == "3",
            "foreign donor is not a supported released candidate format")
    donor_id, original_id = uuid.UUID(donor["id"]), uuid.UUID(original["id"])
    members = [member for group in donor["xl"]["sets"] for member in group]
    original_members = {member for group in original["xl"]["sets"] for member in group}
    require(donor_id.int != 0 and donor_id != original_id and donor["xl"]["this"] in members
            and len(members) == len(set(members)) and all(uuid.UUID(member).int != 0 for member in members)
            and not set(members) & original_members, "foreign donor reused the original cluster or a member identity")
    folder = data / ".rustfs.sys"
    folder.mkdir(mode=0o700)
    descriptor = os.open(folder / "format.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    descriptor = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)
    return {"disk": value["disk"], "foreignFormatSha256": expected_sha256, "foreignClusterId": str(donor_id),
            "foreignDiskUUID": donor["xl"]["this"], "actualDonorBytesCopiedUnchanged": True,
            "originalArchiveUnchanged": True, "syntheticFormatOrUUIDUsed": False}


def restore_original(state, roots):
    value = attest(state, roots)
    require(not value.get("replacementAbsent") and not value["originalStillAtDataRoot"], "original member is not at its witnessed archive")
    backup, data = Path(value["backupPath"]), Path(value["dataPath"])
    rejected = backup.parent / "rejected-root"
    require(not rejected.exists() and not rejected.is_symlink(), "rejected replacement archive already exists")
    rejected_identity = tree_identity(data)
    # Retain the rejected scene; this recovery is explicitly not a product
    # rebuild. The existing intent still makes a partial rename recoverable.
    os.rename(data, rejected)
    os.rename(backup, data)
    require(tree_identity(data) == value["original"] and tree_identity(rejected) == rejected_identity,
            "original-member recovery changed its frozen bytes or rejected scene")
    descriptor = os.open(data.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)
    (state / "disk-replacement.json").unlink()
    return {"disk": value["disk"], "originalRestoredExactly": True, "rejectedScene": rejected_identity,
            "backupRestorationUsed": True, "productRepairClaimed": False, "sharedMountDetached": False}


def start_properties(state, roots):
    value = checked_replacement(state, roots)
    if value is None: return []
    require(Path(value["backupPath"]).is_dir(), "replacement original has not moved to quarantine")
    return ["--property=PrivateMounts=true", "--property=InaccessiblePaths=" + str(Path(value["backupPath"]).parent),
            "--property=CapabilityBoundingSet=~CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH"]


def probe(state, roots, pid):
    value = attest(state, roots)
    require(not value.get("replacementAbsent") and not value["originalStillAtDataRoot"]
            and type(pid) is int and pid > 1, "live replacement namespace required")
    identity = process_identity(pid)
    require(identity["uid"] == 0 and identity["gid"] == 0 and identity["capEff"] & DENY_DAC_CAPS == 0
            and Path(f"/proc/{pid}/ns/mnt").readlink() != Path("/proc/self/ns/mnt").readlink(),
            "replacement backup is not isolated in the actual service identity/namespace")
    result = subprocess.run(["nsenter", "--mount=/proc/" + str(pid) + "/ns/mnt", "setpriv",
        "--bounding-set=-dac_override,-dac_read_search", "python3", "-c", READ_PROBE_SOURCE,
        value["backupPath"] + "/.rustfs.sys/format.json"], capture_output=True, text=True, timeout=15)
    require(result.returncode == 0, "replacement service read probe did not finish")
    observed = json.loads(result.stdout)
    require(observed.get("errno") == 13 and observed.get("uid") == identity["uid"] and observed.get("gid") == identity["gid"],
            "service can read old replacement backup or lacks exact EACCES")
    return {"disk": value["disk"], "backupPath": value["backupPath"], "serviceReadDenied": True,
            "sameServiceIdentity": True, "privateMountNamespace": True, "readProbe": observed,
            "originalArchiveUnchanged": True, "backupRestorationUsed": False}
