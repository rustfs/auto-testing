"""Retain incomplete stopped fixtures by same-filesystem, non-overwriting rename.

Only owner/binary and fixture/MRF bytes are read. Other entries, including
credentials, are retained by inode and metadata without reading their contents.
"""

import ctypes
import hashlib
import json
import os
import re
from pathlib import Path
import stat
import uuid


def require(value, message):
    if not value:
        raise ValueError(message)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def sha(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def stable(info):
    return (info.st_mode, info.st_uid, info.st_gid, info.st_dev, info.st_ino, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def private_json(path, *, root_owned=True):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= 16384
                and not before.st_mode & 0o022 and (not root_owned or before.st_uid == 0),
                "retained scene marker has unsafe ownership, type or size")
        raw = os.read(fd, 16385)
        after = os.fstat(fd)
        require(len(raw) == before.st_size and stable(before) == stable(after), "retained scene marker changed during audit")
        value = json.loads(raw)
        require(isinstance(value, dict), "retained scene marker is not an object")
        return value
    finally:
        os.close(fd)


def destinations(state, roots, *, per_run_parent=False):
    if per_run_parent:
        return state.parent / ("retained-" + state.name) / state.name, tuple(
            root.parent.parent / ("heal-validation-retained-" + root.name) / root.name for root in roots)
    return state.parent / "retained" / state.name, tuple(
        root.parent.parent / "heal-validation-retained" / root.name for root in roots)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def rename_new(source, target):
    # Linux RENAME_NOREPLACE prevents replacing even an empty competing directory.
    rename = ctypes.CDLL(None, use_errno=True).renameat2
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    result = rename(-100, os.fsencode(source), -100, os.fsencode(target), 1)
    require(result == 0, "retained scene rename refused (errno=" + str(ctypes.get_errno()) + ")")
    sync_directory(source.parent)
    if source.parent != target.parent:
        sync_directory(target.parent)


def parent_ready(parent, boundary, node, *, create=False):
    node["checked"](parent, boundary)
    if create and not parent.exists():
        parent.mkdir(mode=0o700)
        sync_directory(parent.parent)
    if parent.exists():
        info = parent.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700
                and info.st_dev == boundary.stat().st_dev, "retained archive parent is not private on the original filesystem")


def census(folder, owner, *, state, node):
    """Hash the complete inode inventory; read only explicitly scoped content."""
    rows, content_count = [], 0
    boundary_device = folder.lstat().st_dev
    network = owner.get("runtimeProfile") == "lock-quorum"
    fixture_buckets = {owner["bucket"]}
    if network:
        fixture_buckets.add(owner["bucket"] + "-db")
    multipart_keys = {hashlib.sha256(f"{owner['bucket']}/dl/{endpoint}/{name}.bin".encode()).hexdigest()
                      for endpoint in ("node1", "node3", "node4")
                      for name in ("abort", "complete", "create", "upload")} if network else set()
    def visit(path):
        nonlocal content_count
        node["checked"](path, folder)
        info = path.lstat()
        require(info.st_dev == boundary_device and (stat.S_ISDIR(info.st_mode)
                or stat.S_ISREG(info.st_mode) and info.st_nlink == 1),
                "retained scene contains a mount, link or special file")
        relative = path.relative_to(folder).as_posix()
        require(len(path.relative_to(folder).parts) <= 64, "retained scene tree is too deep")
        # Renaming the top directory changes only its ctime. Descendant ctime
        # remains part of the witness, including for unread credential files.
        row = [hashlib.sha256(relative.encode()).hexdigest(), info.st_mode, info.st_uid, info.st_gid,
               info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
               info.st_ctime_ns if path != folder else None]
        require(len(rows) < 30000, "retained scene inventory exceeds its explicit bound")
        rows.append(row)
        if stat.S_ISDIR(info.st_mode):
            for child in sorted(path.iterdir()):
                visit(child)
            after = path.lstat()
            require(stable(info) == stable(after), "retained scene directory changed during audit")
        else:
            parts = Path(relative).parts
            fixture = not state and len(parts) > 2 and parts[0] == "data" and parts[1] in fixture_buckets
            network_mpu = (not state and network and len(parts) > 4
                           and parts[:3] == ("data", ".rustfs.sys", "multipart") and parts[3] in multipart_keys)
            network_backup = (not state and network and len(parts) == 2 and parts[0] == "quarantine"
                              and re.fullmatch(r"I03-missing-target\.(original|intent\.json|applied\.json)", parts[1]))
            mrf = not state and relative in {
                "data/.rustfs.sys/.heal-mrf-snapshot.0.bin", "data/.rustfs.sys/.heal-mrf-snapshot.1.bin",
                "data/.rustfs.sys/.heal-mrf-manifest.json"}
            backup = (not state and owner.get("runtimeProfile") == "root-restart"
                      and len(parts) == 2 and parts[0] == "quarantine"
                      and re.fullmatch(r"F07-shard-[0-3]\.(original|intent\.json|applied\.json)", parts[1]))
            opaque_backup = (not state and owner.get("runtimeProfile") == "outcome-legacy"
                             and len(parts) == 2 and parts[0] == "quarantine"
                             and re.fullmatch(r"J02-[0-9a-f]{32}\.(original|intent\.json|applied\.json|failed-[0-9a-f]{64})", parts[1]))
            race_backup = (not state and owner.get("runtimeProfile") == "manual"
                           and len(parts) == 2 and parts[0] == "quarantine"
                           and re.fullmatch(r"G0[12]-[a-z0-9-]{1,48}\.(original|intent\.json|applied\.json)", parts[1]))
            if (state and relative == "rustfs") or fixture or mrf or backup or opaque_backup or network_mpu or network_backup or race_backup:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    require(stable(os.fstat(fd)) == stable(info) and info.st_size <= 2 * 1024**3,
                            "retained content changed or exceeds its bound")
                    digest = hashlib.sha256()
                    while chunk := os.read(fd, 1024**2):
                        digest.update(chunk)
                    require(stable(os.fstat(fd)) == stable(info), "retained content changed while hashing")
                    row.append(digest.hexdigest())
                    content_count += 1
                finally:
                    os.close(fd)
            require(stable(path.lstat()) == stable(info), "retained scene file changed during audit")
    visit(folder)
    return {"treeSha256": sha(rows), "entries": len(rows), "contentFilesHashed": content_count,
            "device": boundary_device, "inode": folder.lstat().st_ino}


def mounted_paths():
    values = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        require(len(fields) >= 6, "retained scene mount inventory is malformed")
        value = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), fields[4])
        values.append(Path(value))
    return values


def inspect(request, node):
    state, roots = node["paths"](request["runId"])
    expected = request["expectedOwner"]
    per_run = expected.get("runtimeProfile") in {"outcome-legacy", "lock-quorum", "manual"}
    retained_state, retained_roots = destinations(state, roots, per_run_parent=per_run)
    if per_run:
        old_state, old_roots = destinations(state, roots)
        require(not any(node["checked"](path, state.parent if index == 4 else node["MOUNTS"][index]).exists()
                        for index, path in enumerate((*old_roots, old_state))),
                "shared archive already exists; refusing another retention layout")
    require(isinstance(expected, dict) and expected.get("runId") == request["runId"]
            and expected.get("unit") == node["unit"](request["runId"])
            and expected.get("hostname") == node["socket"].gethostname()
            and expected.get("runtimeProfile") in {"mrf-crash", "root-restart", "outcome-grace", "outcome-legacy", "lock-quorum", "manual"}, "retained scene archived owner differs")
    current = node["mounts"]()
    require(len(current) == len(expected.get("mounts", [])) == 4
            and all(all(actual[key] == old[key] for key in ("target", "source", "fstype", "uuid", "maj:min"))
                    for actual, old in zip(current, expected["mounts"], strict=True)),
            "retained scene original mount identities changed")
    node["stopped"](expected)
    for parent, boundary in [(retained_state.parent, state.parent),
                             *((target.parent, mount) for target, mount in zip(retained_roots, node["MOUNTS"], strict=True))]:
        parent_ready(parent, boundary, node)
    locations, witnesses = [], []
    mounted = mounted_paths()
    for index, (source, target) in enumerate(zip((*roots, state), (*retained_roots, retained_state), strict=True)):
        boundary = node["MOUNTS"][index] if index < 4 else state.parent
        node["checked"](target, boundary)
        require(not (source.exists() and target.exists()), "both active and retained scene exist; refusing ambiguity")
        folder = source if source.exists() else target if target.exists() else None
        locations.append("active" if folder == source else "retained" if folder == target else "absent")
        if folder is None:
            require(index < 4, "retained scene lost its original state and candidate")
            witnesses.append(None)
            continue
        require(not any(mount == folder or mount.is_relative_to(folder) for mount in mounted),
                "retained scene contains a mounted subtree")
        info = folder.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_dev == boundary.stat().st_dev,
                "retained scene directory is not on its original filesystem")
        owner = private_json(node["checked"](folder / "owner.json", folder), root_owned=index == 4)
        require(encoded(owner) == encoded(expected), "retained scene owner content differs from original receipt")
        if index == 4:
            require(info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700,
                    "retained scene state is not private")
            require(node["digest"](node["checked"](folder / "rustfs", folder)) == expected["binarySha256"],
                    "retained scene candidate binary differs")
            require(not any((folder / name).exists() for name in (
                "drive-gate.json", "write-quorum-gate.json", "network-gate.json", "disk-replacement.json")),
                "retained scene still has a gate or replacement intent")
        witnesses.append(census(folder, expected, state=index == 4, node=node))
    baseline = {"runId": request["runId"], "ownerSha256": sha(expected), "roots": witnesses[:4], "state": witnesses[4]}
    if per_run:
        baseline["archiveLayout"] = "per-run-parent"
    intent = retained_state.parent / (request["runId"] + ".intent.json")
    node["checked"](intent, state.parent)
    network = expected["runtimeProfile"] == "lock-quorum"
    if expected["runtimeProfile"] in {"root-restart", "outcome-grace", "outcome-legacy", "lock-quorum", "manual"}:
        original_sha = request.get("expectedOriginalSceneSha256", "")
        require(re.fullmatch(r"[0-9a-f]{64}", original_sha) is not None,
                "restored fixture requires its original physical scene digest")
        baseline["originalSceneSha256"] = original_sha
        if network:
            require(re.fullmatch(r"[0-9a-f]{64}", request.get("expectedMpuSceneSha256", "")) is not None,
                    "network retention requires its stopped original MPU scene digest")
            baseline["originalMpuSceneSha256"] = request["expectedMpuSceneSha256"]
            baseline["originalMpuKeysSha256"] = sha(request["mpuKeys"])
        require("absent" not in locations, "restored fixture cannot omit an original root")
        if not intent.exists():
            require(all(location == "active" for location in locations), "unbound retained fixture")
            scene = node["snapshot"]({"runId": request["runId"], "action": "snapshot", "quiescent": True,
                **({"buckets": [expected["bucket"], expected["bucket"] + "-db"]} if network else {})})
            require(scene.get("quiescent") is True and type(scene.get("pid")) is int and scene["pid"] == 0
                    and scene.get("binarySha256") == expected["binarySha256"]
                    and sha({key: scene[key] for key in ("files", "formats")}) == original_sha,
                    "restored fixture differs from the original business scene")
            if network:
                mpu = node["mpu_scene_backend"].run({"runId": request["runId"], "action": "mpu-scene",
                    "bucket": expected["bucket"], "keys": request["mpuKeys"],
                    "expectedBinarySha256": expected["binarySha256"]}, node)
                require(mpu["sceneSha256"] == request["expectedMpuSceneSha256"],
                        "network retained multipart scene differs from the original stopped census")
    if intent.exists():
        require(encoded(private_json(intent)) == encoded(baseline), "retained scene drifted from its durable isolation intent")
    else:
        require("retained" not in locations, "retained directory lacks its durable isolation intent")
    return baseline, locations, state, roots, retained_state, retained_roots, intent


def run(request, node):
    fields = {"runId", "action", "expectedOwner"}
    if request.get("expectedOwner", {}).get("runtimeProfile") in {"root-restart", "outcome-grace", "outcome-legacy", "lock-quorum", "manual"}:
        fields.add("expectedOriginalSceneSha256")
    if request.get("expectedOwner", {}).get("runtimeProfile") == "lock-quorum":
        fields.update({"expectedMpuSceneSha256", "mpuKeys"})
    if request["action"] == "retained-scene-isolate":
        fields.add("expectedBaseline")
    require(set(request) == fields,
            "retained scene accepts only its archived owner and frozen inventory")
    require(request["action"] in {"retained-scene-audit", "retained-scene-isolate"}, "invalid retained scene action")
    baseline, locations, state, roots, retained_state, retained_roots, intent = inspect(request, node)
    mutated = False
    if request["action"] == "retained-scene-isolate":
        require(encoded(request["expectedBaseline"]) == encoded(baseline), "retained scene changed after fleet preflight")
        for parent, boundary in [(retained_state.parent, state.parent),
                                 *((target.parent, mount) for target, mount in zip(retained_roots, node["MOUNTS"], strict=True))]:
            parent_ready(parent, boundary, node, create=True)
        if not intent.exists():
            temporary = intent.with_name(intent.name + "." + uuid.uuid4().hex + ".new")
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded(baseline)); stream.flush(); os.fsync(stream.fileno())
            rename_new(temporary, intent)
        for index, (source, target) in enumerate(zip((*roots, state), (*retained_roots, retained_state), strict=True)):
            if locations[index] != "active":
                continue
            fresh = inspect(request, node)
            require(encoded(fresh[0]) == encoded(baseline), "retained scene changed immediately before rename")
            rename_new(source, target)
            mutated = True
        final = inspect(request, node)
        require(encoded(final[0]) == encoded(baseline) and "active" not in final[1],
                "retained scene post-rename inventory differs")
        locations = final[1]
    result = {"baseline": baseline, "locations": locations, "originalEvidenceComplete": False,
            "originalSceneRestored": False, "canonicalPassClaimed": False, "backupRestorationUsed": False,
            "credentialContentsRead": False, "inspectionOnly": request["action"] == "retained-scene-audit",
            "mutated": mutated, "activeNamespaceIsolated": "active" not in locations}
    if request["expectedOwner"].get("runtimeProfile") in {"outcome-legacy", "lock-quorum", "manual"}:
        result["privateArchiveParents"] = []
        for target in (*retained_roots, retained_state):
            info = target.parent.lstat() if target.parent.exists() else None
            result["privateArchiveParents"].append({"exists": False} if info is None else {
                "exists": True, "uid": info.st_uid, "gid": info.st_gid, "mode": stat.S_IMODE(info.st_mode),
                "device": info.st_dev, "inode": info.st_ino})
    return result
