#!/usr/bin/env python3
"""Linux-only, descriptor-relative file faults in a dedicated Heal run root.

No arbitrary shell, disk formatting, shared mount changes, or internal system
metadata. Keep the service quiescent while applying/undoing a file fault.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys


LIMIT = 64 * 1024**2
ROOT_PATTERN = re.compile(r"/data/rustfs[1-9][0-9]*/heal-validation/((?:gh|manual)-[a-z0-9-]{5,60})")
FAULTS = ("missing", "bitflip", "truncate", "zero", "header", "replace-part", "future-xl-schema")


def bounded_fault_id(code: str, branch: str, index: int) -> str:
    require(bool(re.fullmatch(r"[A-J][0-9]{2}", code)) and isinstance(branch, str) and branch
            and type(index) is int and index >= 0, "invalid fault identity inputs")
    return code + "-" + hashlib.sha256(f"{branch}/{index}".encode()).hexdigest()[:32]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def mount_id(fd: int) -> int:
    text = Path(f"/proc/self/fdinfo/{fd}").read_text()
    values = re.findall(r"^mnt_id:\s*([0-9]+)$", text, re.MULTILINE)
    require(len(values) == 1, "Linux mount identity unavailable")
    return int(values[0])


def target_parts(relative: str, prefix: str) -> list[str]:
    parts = relative.split("/")
    require(3 <= len(parts) <= 48 and all(part not in {"", ".", ".."} for part in parts),
            "invalid relative object path")
    require(parts[0] == prefix or parts[0].startswith(prefix + "-"), "target bucket not owned by run")
    require(not any(part.startswith(".rustfs") for part in parts), "system metadata faults are prohibited")
    require(parts[-1] == "xl.meta" or bool(re.fullmatch(r"part\.[1-9][0-9]*", parts[-1])),
            "only explicit object xl.meta/part files are supported")
    return parts


@contextmanager
def directory(path: Path):
    require(path.is_absolute() and ".." not in path.parts, "noncanonical root")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


@contextmanager
def parent(root_fd: int, parts: list[str]):
    fd = os.dup(root_fd)
    expected_mount = mount_id(root_fd)
    try:
        for part in parts:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
            require(mount_id(fd) == expected_mount, "nested mount or bind-mount escape")
        yield fd
    finally:
        os.close(fd)


def read_file(fd: int, name: str, limit: int = LIMIT) -> bytes:
    opened = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    try:
        info = os.fstat(opened)
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= limit,
                "expected bounded regular non-hardlinked file")
        require(mount_id(opened) == mount_id(fd), "file bind-mount escape")
        with os.fdopen(opened, "rb", closefd=False) as stream:
            value = stream.read(limit + 1)
        after = os.fstat(opened)
        require(len(value) <= limit and info.st_size == len(value)
                and (info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), "file changed while reading")
        return value
    finally:
        os.close(opened)


def new_file(fd: int, name: str, value: bytes) -> None:
    opened = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
    with os.fdopen(opened, "wb") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    os.fsync(fd)


def encode(value: dict) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()


def mutation(value: bytes, kind: str, offset: int | None = None) -> bytes | None:
    require(kind in FAULTS, "unknown fault")
    require(kind != "replace-part", "replacement requires a verified owned donor")
    if kind == "missing":
        return None
    if kind == "future-xl-schema":
        require(offset is None and len(value) >= 8 and value[:4] == b"XL2 "
                and int.from_bytes(value[4:6], "little") == 1, "future envelope requires exact reviewed XL2 major 1")
        return value[:4] + (2).to_bytes(2, "little") + value[6:]
    require(bool(value), "empty file cannot witness this fault")
    if kind == "truncate":
        length = len(value) // 2 if offset is None else offset
        require(type(length) is int and 0 <= length < len(value), "truncate length outside file")
        return value[:length]
    if kind == "zero":
        changed = bytes(len(value))
    elif kind == "header":
        changed = bytes(byte ^ 0xFF for byte in value[:32]) + value[32:]
    else:
        position = len(value) // 2 if offset is None else offset
        require(type(position) is int and 0 <= position < len(value), "bitflip offset outside file")
        changed = value[:position] + bytes([value[position] ^ 1]) + value[position + 1:]
    require(changed != value, "fault would not change the target")
    return changed


def checked_owner(root_fd: int, run_id: str, recovery: bool = False) -> dict:
    owner = json.loads(read_file(root_fd, "fault-owner.json", 16384))
    require(owner.get("runId") == run_id and owner.get("schemaVersion") == 1, "owner mismatch")
    require(owner.get("mountId") == mount_id(root_fd) and owner.get("device") == os.fstat(root_fd).st_dev,
            "mount identity changed; rebind only after an operator audit")
    expiry = datetime.fromisoformat(owner["leaseExpiresAt"].replace("Z", "+00:00"))
    require(expiry.tzinfo is not None, "lease must have timezone")
    require(recovery or expiry > datetime.now(UTC), "fault lease expired")
    return owner


def apply_fault(root_fd: int, owner: dict, relative: str, fault_id: str, kind: str,
                expected_sha: str, offset: int | None = None, *, donor_relative: str | None = None,
                donor_sha: str | None = None) -> dict:
    parts = target_parts(relative, owner["bucketPrefix"])
    require(kind != "future-xl-schema" or parts[-1] == "xl.meta", "future schema fault requires exact object metadata")
    require(bool(re.fullmatch(r"[A-J][0-9]{2}-[a-z0-9-]{1,48}", fault_id)), "invalid fault ID")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", expected_sha)), "baseline SHA-256 required")
    with parent(root_fd, ["data", *parts[:-1]]) as target_fd, parent(root_fd, ["quarantine"]) as backup_fd:
        before = read_file(target_fd, parts[-1])
        require(digest(before) == expected_sha, "target differs from frozen baseline")
        donor = None
        if kind == "replace-part":
            require(offset is None and isinstance(donor_relative, str) and isinstance(donor_sha, str)
                    and re.fullmatch(r"[0-9a-f]{64}", donor_sha), "verified replacement donor required")
            source = target_parts(donor_relative, owner["bucketPrefix"])
            require(source[-1] == parts[-1] and source[-1].startswith("part.") and source != parts,
                    "replacement must use another owned generation's same-number part")
            with parent(root_fd, ["data", *source[:-1]]) as source_fd:
                changed = read_file(source_fd, source[-1])
            require(digest(changed) == donor_sha and changed != before, "donor changed or is identical to target")
            donor = {"relativePath": donor_relative, "sha256": donor_sha, "size": len(changed)}
        else:
            require(donor_relative is None and donor_sha is None, "donor parameters require replace-part")
            changed = mutation(before, kind, offset)
        receipt = {"schemaVersion": 1, "runId": owner["runId"], "faultId": fault_id,
                   "relativePath": relative, "kind": kind, "offset": offset,
                   "before": {"sha256": digest(before), "size": len(before)},
                   "fault": None if changed is None else {"sha256": digest(changed), "size": len(changed)},
                   "createdAt": datetime.now(UTC).isoformat(), "backup": fault_id + ".original",
                   "backend": "logical-file-fault", "requiresQuiescentService": True}
        if donor is not None:
            receipt["donor"] = donor
        # Intent and recoverable bytes precede the mutation. An interrupted
        # apply remains recoverable even when no completion receipt was emitted.
        new_file(backup_fd, fault_id + ".intent.json", encode(receipt))
        new_file(backup_fd, receipt["backup"], before)
        require(read_file(target_fd, parts[-1]) == before, "target changed before injection")
        if changed is None:
            os.unlink(parts[-1], dir_fd=target_fd)
        else:
            temporary = ".heal-fault-" + fault_id
            new_file(target_fd, temporary, changed)
            os.rename(temporary, parts[-1], src_dir_fd=target_fd, dst_dir_fd=target_fd)
        os.fsync(target_fd)
        if changed is None:
            require(not os.path.lexists(f"/proc/self/fd/{target_fd}/{parts[-1]}"), "missing fault not observed")
        else:
            require(read_file(target_fd, parts[-1]) == changed, "changed bytes not observed")
        receipt["witnessedAt"] = datetime.now(UTC).isoformat()
        new_file(backup_fd, fault_id + ".applied.json", encode(receipt))
        return receipt


def recover_fault(root_fd: int, owner: dict, fault_id: str) -> dict:
    require(bool(re.fullmatch(r"[A-J][0-9]{2}-[a-z0-9-]{1,48}", fault_id)), "invalid fault ID")
    with parent(root_fd, ["quarantine"]) as backup_fd:
        receipt = json.loads(read_file(backup_fd, fault_id + ".intent.json", 16384))
        require(receipt["runId"] == owner["runId"] and receipt["faultId"] == fault_id
                and receipt["backup"] == fault_id + ".original", "recovery receipt mismatch")
        parts = target_parts(receipt["relativePath"], owner["bucketPrefix"])
        before = read_file(backup_fd, receipt["backup"])
        require(digest(before) == receipt["before"]["sha256"] and len(before) == receipt["before"]["size"],
                "backup corrupted; preserve scene")
        with parent(root_fd, ["data", *parts[:-1]]) as target_fd:
            try:
                current = read_file(target_fd, parts[-1])
            except FileNotFoundError:
                current = None
            restored = current != before
            if restored:
                # A new generation or a legitimate metadata rewrite is not
                # ours to overwrite. Preserve it for an operator-led rescue.
                expected_fault = receipt["fault"]
                require(current is None or expected_fault is not None
                        and len(current) == expected_fault["size"]
                        and digest(current) == expected_fault["sha256"],
                        "target changed after fault; refusing to overwrite possible acknowledged data")
                # Never erase post-failure bytes. Exact recovery is a rescue,
                # not proof that the product healed the target.
                if current is not None:
                    preserved = fault_id + ".failed-" + digest(current)
                    try:
                        new_file(backup_fd, preserved, current)
                    except FileExistsError:
                        require(read_file(backup_fd, preserved) == current, "preserved bytes differ")
                temporary = ".heal-recovery-" + fault_id
                try:
                    new_file(target_fd, temporary, before)
                except FileExistsError:
                    require(read_file(target_fd, temporary) == before, "incomplete recovery temporary")
                os.rename(temporary, parts[-1], src_dir_fd=target_fd, dst_dir_fd=target_fd)
                os.fsync(target_fd)
            require(read_file(target_fd, parts[-1]) == before, "recovery verification failed")
            return {"runId": owner["runId"], "faultId": fault_id, "restoredFromBackup": restored,
                    "physicalRepairPassAllowed": False, "retainedBackup": receipt["backup"],
                    "recoveredAt": datetime.now(UTC).isoformat()}


def snapshot(root_fd: int, owner: dict, bucket: str) -> dict:
    require(bucket == owner["bucketPrefix"] or bucket.startswith(owner["bucketPrefix"] + "-"),
            "snapshot bucket outside run")
    require(bool(re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket)), "invalid bucket")
    files, total = [], 0

    def walk(fd: int, relative: list[str], depth: int = 0) -> None:
        nonlocal total
        require(depth <= 48, "snapshot depth exceeds budget")
        for name in sorted(os.listdir(fd)):
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                with parent(fd, [name]) as child:
                    walk(child, [*relative, name], depth + 1)
            else:
                value = read_file(fd, name)
                total += len(value)
                require(total <= 8 * 1024**3 and len(files) < 10000, "snapshot budget exceeded")
                files.append({"path": "/".join([*relative, name]), "size": len(value), "sha256": digest(value)})

    with parent(root_fd, ["data", bucket]) as bucket_fd:
        walk(bucket_fd, [bucket])
    return {"runId": owner["runId"], "bucket": bucket, "files": files, "bytes": total,
            "capturedAt": datetime.now(UTC).isoformat(), "decodedMetadata": False,
            "physicalRepairPassAllowed": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    sub = parser.add_subparsers(dest="action", required=True)
    init = sub.add_parser("init")
    init.add_argument("--bucket-prefix", required=True)
    init.add_argument("--lease-expires-at", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--path", required=True)
    capture = sub.add_parser("snapshot")
    capture.add_argument("--bucket", required=True)
    apply = sub.add_parser("apply")
    apply.add_argument("--path", required=True)
    apply.add_argument("--fault-id", required=True)
    apply.add_argument("--kind", choices=FAULTS, required=True)
    apply.add_argument("--expected-sha256", required=True)
    apply.add_argument("--offset", type=int)
    apply.add_argument("--donor-path")
    apply.add_argument("--donor-sha256")
    apply.add_argument("--service-quiescent", action="store_true", required=True)
    recover = sub.add_parser("recover")
    recover.add_argument("--fault-id", required=True)
    recover.add_argument("--service-quiescent", action="store_true", required=True)
    args = parser.parse_args()
    require(sys.platform == "linux" and os.geteuid() == 0, "run only with sudo on the Linux jump-host target")
    match = ROOT_PATTERN.fullmatch(str(args.root))
    require(match is not None and match.group(1) == args.run_id, "root is not a dedicated Heal run root")
    with directory(args.root) as root_fd:
        require(os.fstat(root_fd).st_uid == 0 and os.fstat(root_fd).st_mode & 0o022 == 0,
                "run root must be root-owned and not group/world writable")
        if args.action == "init":
            require(bool(re.fullmatch(r"rv-heal-[a-z0-9-]{6,35}", args.bucket_prefix)), "invalid bucket prefix")
            expiry = datetime.fromisoformat(args.lease_expires_at.replace("Z", "+00:00"))
            require(expiry.tzinfo is not None and expiry > datetime.now(UTC), "invalid lease")
            # Existing run ownership must already be provisioned by the lab
            # operator (or pilot setup). This tool never provisions a data disk.
            existing = json.loads(read_file(root_fd, "owner.json", 16384))
            require(existing.get("runId") == args.run_id, "missing lab ownership marker")
            with parent(root_fd, ["data"]), parent(root_fd, ["quarantine"]):
                pass
            result = {"schemaVersion": 1, "runId": args.run_id, "bucketPrefix": args.bucket_prefix,
                      "leaseExpiresAt": args.lease_expires_at, "mountId": mount_id(root_fd),
                      "device": os.fstat(root_fd).st_dev}
            new_file(root_fd, "fault-owner.json", encode(result))
        else:
            owner = checked_owner(root_fd, args.run_id, recovery=args.action == "recover")
            if args.action == "snapshot":
                result = snapshot(root_fd, owner, args.bucket)
            elif args.action == "inspect":
                parts = target_parts(args.path, owner["bucketPrefix"])
                with parent(root_fd, ["data", *parts[:-1]]) as target_fd:
                    value = read_file(target_fd, parts[-1])
                result = {"runId": args.run_id, "path": args.path, "size": len(value), "sha256": digest(value)}
            elif args.action == "apply":
                result = apply_fault(root_fd, owner, args.path, args.fault_id, args.kind,
                                     args.expected_sha256, args.offset,
                                     donor_relative=args.donor_path, donor_sha=args.donor_sha256)
            else:
                result = recover_fault(root_fd, owner, args.fault_id)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as error:
        print(json.dumps({"state": "INFRA_ERROR", "error": str(error)}), file=sys.stderr)
        raise SystemExit(1) from error
