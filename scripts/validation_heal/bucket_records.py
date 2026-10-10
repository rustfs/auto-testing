"""D11 node backend: exact owned bucket-config records, never an IAM walk."""

import json
import os
import re
import uuid


def require(value, message):
    if not value:
        raise ValueError(message)


def bucket_path(bucket, prefix):
    require(isinstance(bucket, str) and (bucket == prefix or bucket.startswith(prefix + "-"))
            and re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket),
            "system record is outside the exact owned bucket prefix")
    return f".rustfs.sys/buckets/{bucket}/.metadata.bin"


def record_parts(relative, bucket, prefix):
    base = bucket_path(bucket, prefix)
    require(relative == base + "/xl.meta", "only the owned bucket-config xl.meta is mutable")
    return relative.split("/")


def changed_bytes(before, kind):
    require(kind in {"missing", "truncate", "future-xl-schema"}, "unsupported bucket-record fault")
    require(len(before) >= 8 and before[:4] == b"XL2 " and int.from_bytes(before[4:6], "little") == 1,
            "bucket record is not the reviewed candidate XL2 major 1 envelope")
    if kind == "missing":
        return None
    if kind == "truncate":
        return before[:len(before) // 2]
    # The candidate rejects an outer major above 1 before decoding/checksums.
    # Preserve every other byte; this is not a fabricated inner msgpack record.
    return before[:4] + (2).to_bytes(2, "little") + before[6:]


def apply(root_fd, owner, fault, request):
    bucket, identity = request["bucket"], request["faultId"]
    parts = record_parts(request["relativePath"], bucket, owner["bucketPrefix"])
    require(re.fullmatch(r"D11-[a-z0-9-]{1,48}", identity), "invalid bucket-record fault ID")
    require(re.fullmatch(r"[0-9a-f]{64}", request["expectedSha256"]), "missing frozen record digest")
    with fault.parent(root_fd, ["data", *parts[:-1]]) as target, fault.parent(root_fd, ["quarantine"]) as backup:
        before = fault.read_file(target, parts[-1], 4 * 1024**2)
        require(fault.digest(before) == request["expectedSha256"], "bucket record changed before injection")
        changed = changed_bytes(before, request["kind"])
        receipt = {"schemaVersion": 1, "runId": owner["runId"], "bucket": bucket, "faultId": identity,
            "relativePath": request["relativePath"], "kind": request["kind"], "backup": identity + ".original",
            "before": {"sha256": fault.digest(before), "size": len(before)},
            "fault": None if changed is None else {"sha256": fault.digest(changed), "size": len(changed)},
            "backend": "exact-owned-bucket-config-envelope", "requiresQuiescentService": True,
            "credentialsRead": False, "innerMetadataPayloadRewritten": False}
        fault.new_file(backup, identity + ".intent.json", fault.encode(receipt))
        fault.new_file(backup, receipt["backup"], before)
        require(fault.read_file(target, parts[-1]) == before, "record changed after its intent")
        if changed is None:
            os.unlink(parts[-1], dir_fd=target)
        else:
            temporary = ".bucket-fault-" + identity
            fault.new_file(target, temporary, changed)
            os.rename(temporary, parts[-1], src_dir_fd=target, dst_dir_fd=target)
        os.fsync(target)
        if changed is None:
            try:
                os.stat(parts[-1], dir_fd=target, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise ValueError("bucket record removal was not witnessed")
        else:
            require(fault.read_file(target, parts[-1]) == changed, "bucket record mutation was not witnessed")
        fault.new_file(backup, identity + ".applied.json", fault.encode(receipt))
        return receipt


def recover(root_fd, owner, fault, request):
    identity = request["faultId"]
    require(re.fullmatch(r"D11-[a-z0-9-]{1,48}", identity), "invalid bucket-record recovery ID")
    with fault.parent(root_fd, ["quarantine"]) as backup:
        receipt = json.loads(fault.read_file(backup, identity + ".intent.json", 16384))
        require(receipt["runId"] == owner["runId"] and receipt["faultId"] == identity
                and receipt["backup"] == identity + ".original", "bucket-record recovery identity differs")
        parts = record_parts(receipt["relativePath"], receipt["bucket"], owner["bucketPrefix"])
        before = fault.read_file(backup, receipt["backup"])
        require({"sha256": fault.digest(before), "size": len(before)} == receipt["before"], "record backup changed")
        with fault.parent(root_fd, ["data", *parts[:-1]]) as target:
            try:
                current = fault.read_file(target, parts[-1])
            except FileNotFoundError:
                current = None
            restored = current != before
            if restored:
                require(current is None or {"sha256": fault.digest(current), "size": len(current)} == receipt["fault"],
                        "bucket record has an unrecognized post-fault rewrite; retain namespace")
                if current is not None:
                    name = identity + ".failed-" + fault.digest(current)
                    try:
                        fault.new_file(backup, name, current)
                    except FileExistsError:
                        require(fault.read_file(backup, name) == current, "preserved record differs")
                temporary = ".bucket-rescue-" + identity
                try:
                    fault.new_file(target, temporary, before)
                except FileExistsError:
                    require(fault.read_file(target, temporary) == before, "interrupted rescue bytes differ")
                os.rename(temporary, parts[-1], src_dir_fd=target, dst_dir_fd=target)
                os.fsync(target)
            require(fault.read_file(target, parts[-1]) == before, "record rescue did not restore exact baseline")
            return {"faultId": identity, "restored": restored, "physicalRepairPassAllowed": False,
                    "restorationIsProductRepair": False}


def snapshot(roots, owner, request, checked, digest, tool, command):
    buckets = request["buckets"]
    require(isinstance(buckets, list) and len(buckets) == 2 and len(set(buckets)) == 2
            and owner["bucketPrefix"] in buckets, "exact primary/control bucket pair required")
    entries, iam = [], []
    for disk, root in enumerate(roots):
        data = root / "data"
        # Root credentials live in the owned service environment. An IAM
        # subtree is a precondition failure: never enumerate credential files.
        iam_root = checked(data / ".rustfs.sys/config/iam", data)
        iam.append({"disk": disk, "present": iam_root.exists(), "traversed": False})
        require(not iam_root.exists(), "D11 requires the fresh owned namespace without persisted IAM records")
        for bucket in buckets:
            for record_name in (".metadata.bin", ".bucket-incarnation"):
                base = bucket_path(bucket, owner["bucketPrefix"]).rsplit("/", 1)[0] + "/" + record_name
                folder = checked(data / base, data)
                if not folder.exists():
                    continue
                for directory, children, names in os.walk(folder, followlinks=False):
                    for name in (*children, *names):
                        checked(type(folder)(directory) / name, data)
                    for name in sorted(names):
                        path = type(folder)(directory) / name
                        relative = path.relative_to(folder).as_posix()
                        segments = relative.split("/")
                        valid = relative == "xl.meta" or len(segments) == 2 and bool(re.fullmatch(
                            r"part\.[1-9][0-9]*(?:\.integrity\.[a-z0-9-]+)?", segments[1]))
                        if len(segments) == 2:
                            require(str(uuid.UUID(segments[0])) == segments[0], "noncanonical bucket-config dataDir")
                        require(valid and len(entries) < 128 and path.stat().st_size <= 4 * 1024**2,
                                "bucket-config record exceeds the exact file/byte allowlist")
                        entry = {"disk": disk, "bucket": bucket, "path": str(path.relative_to(data)),
                            "size": path.stat().st_size, "sha256": digest(path)}
                        if relative == "xl.meta":
                            decoded = command(str(tool), str(path), ".rustfs.sys", f"buckets/{bucket}/{record_name}", check=False)
                            if decoded.returncode == 0:
                                entry["metadata"] = json.loads(decoded.stdout)
                                versions = entry["metadata"].get("versions", [])
                                require(len(versions) == 1 and not versions[0].get("deleted"),
                                        "system streaming proof requires one live record")
                                version = versions[0]
                                version_id = version.get("versionId") or "null"
                                args = [str(tool), "--verify-inline", str(path), ".rustfs.sys",
                                        f"buckets/{bucket}/{record_name}", version_id]
                                if version.get("inline") is not None:
                                    entry["streaming"] = [json.loads(command(*args).stdout)]
                                else:
                                    data_dir = version.get("dataDir")
                                    require(isinstance(data_dir, str) and str(uuid.UUID(data_dir)) == data_dir,
                                            "system record lacks a canonical external dataDir")
                                    proofs = []
                                    for part in version["parts"]:
                                        number = part["number"]
                                        require(type(number) is int and 0 < number <= 8, "unbounded system record part")
                                        part_path = checked(folder / data_dir / f"part.{number}", data)
                                        proofs.append(json.loads(command(str(tool), "--verify-part", str(path), ".rustfs.sys",
                                            f"buckets/{bucket}/{record_name}", str(part_path), version_id, str(number)).stdout))
                                    require(proofs, "system record has no streaming part proof")
                                    entry["streaming"] = proofs
                                require(all(proof.get("physicalBitrotVerified") is True and proof.get("bucket") == ".rustfs.sys"
                                    and proof.get("key") == f"buckets/{bucket}/{record_name}" for proof in entry["streaming"]),
                                    "system record streaming proof differs from its exact identity")
                            else:
                                expected = request.get("metadataFaults", {}).get(f"{disk}:{entry['path']}")
                                require(expected == {name: entry[name] for name in ("size", "sha256")},
                                        "bucket record decoding failed outside its exact witnessed mutation")
                                entry["decodeFailure"] = True
                        entries.append(entry)
    return {"files": entries, "iamNamespace": iam, "credentialsRead": False,
            "scope": "two exact owned bucket metadata object directories; no system-directory traversal"}
