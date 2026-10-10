"""B03: full private-drive replacement with original same-slot shard oracles."""

import copy
import time

try:
    from .heal_probe import Probe, canonical, compare_inventories, require
    from .heal_workload import verify_seed
    from .healthy import validate_topology
    from .shards import files, map_target, run_task
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, require
    from heal_workload import verify_seed
    from healthy import validate_topology
    from shards import files, map_target, run_task


BRANCHES = ("new-disk-data", "new-disk-versioned")
FORMAT_SECONDS = 90


def wait_format(fleet, node, disk):
    deadline = time.monotonic() + FORMAT_SECONDS
    while True:
        observed = fleet.record("replacement-format-initialization-window", fleet.remote(node, "replacement-state"))
        require(observed.get("readOnly") is True and observed.get("originalArchiveUnchanged") is True,
                "replacement initialization lost its read-only original-archive witness")
        formats = observed.get("formats")
        require(isinstance(formats, list) and len(formats) == 4
                and {item.get("disk") for item in formats} == set(range(4)), "replacement format probe omitted a member")
        target = next(item for item in formats if item["disk"] == disk)
        require(type(target.get("present")) is bool, "replacement format presence is unknown")
        if target["present"]: return observed
        require(time.monotonic() < deadline,
                "candidate did not initialize the replacement format within its registered 90-second budget")
        time.sleep(2)


def format_identity(before, after, member):
    topology = "H1" if len(before) == 1 else "H2"
    validate_topology(before, topology); validate_topology(after, topology)
    old = next(entry for entry in before[member[0]]["formats"] if entry["disk"] == member[1])
    new = next(entry for entry in after[member[0]]["formats"] if entry["disk"] == member[1])
    old_uuid, new_uuid = old["value"]["xl"]["this"], new["value"]["xl"]["this"]
    sets = old["value"]["xl"]["sets"]
    slot = sets[0].index(old_uuid)
    require(new_uuid != old_uuid and new_uuid not in sets[0], "replacement did not expose a distinct new disk UUID")
    require(set(before) == set(after), "replacement changed the original node fleet")
    for node, observation in before.items():
        current = {entry["disk"]: entry for entry in after[node]["formats"]}
        require(set(current) == {entry["disk"] for entry in observation["formats"]}, "replacement omitted a physical member")
        for entry in observation["formats"]:
            expected = copy.deepcopy(entry["value"])
            expected["xl"]["sets"][0][slot] = new_uuid
            if (node, entry["disk"]) == member: expected["xl"]["this"] = new_uuid
            require(current[entry["disk"]]["value"] == expected,
                    "replacement changed pool/set geometry, another disk identity or an unrelated format field")
    return {"oldDiskUUID": old_uuid, "newDiskUUID": new_uuid, "sameOriginalSetSlot": slot,
            "originalPoolAndAllOtherMemberIdentitiesPreserved": True}


def metadata_semantics(metadata):
    value = copy.deepcopy(metadata)
    for version in value.get("versions", []):
        inline = version.get("inline")
        if inline: version["inline"] = {name: inline.get(name) for name in ("length", "sha256")}
        version.get("erasure", {}).pop("checksums", None)
    return value


def physical_rebuild(before, after, member):
    identity = format_identity(before, after, member)
    left, right = files(before), files(after)
    require(left and set(left) <= set(right), "replacement lost an original object/version/part file")
    original_target = {key: entry for key, entry in left.items() if key[:2] == member}
    require(original_target, "replacement has no original target file oracle")
    for key, entry in left.items():
        actual = right[key]
        if key[:2] != member:
            require(actual == entry, "replacement changed a surviving member's original shard/metadata")
        elif entry.get("metadata"):
            require(actual.get("metadata") and canonical(metadata_semantics(actual["metadata"])) ==
                    canonical(metadata_semantics(entry["metadata"])), "new disk changed an original version/marker/inline/role authority")
        else:
            require(actual == entry, "replacement did not reconstruct exact original same-role part bytes")
    for key in set(right) - set(left):
        # A target-side rollback backup is bounded to the exact original xl.meta
        # bytes for that same member/key. No orphan or system record is allowed.
        original = left.get((key[0], key[1], key[2].removesuffix(".bkp"))) if key[2].endswith("/xl.meta.bkp") else None
        require(key[:2] == member and original and original.get("metadata")
                and all(right[key].get(name) == original.get(name) for name in ("sha256", "size")),
                "replacement created an unexpected file or foreign rollback backup")
    return {**identity, "allOriginalTargetFilesRebuilt": len(original_target),
            "originalSameRolePartAndInlineDigestsRestored": True, "survivingFilesUnchanged": True,
            "backupRestorationUsed": False}


def run_branch(recorder, fleet, bucket, branch, expected, baseline, active, admissions):
    try:
        from .versions import map_inline_target, validate_version_receipts, verify_version_s3
    except ImportError:
        from versions import map_inline_target, validate_version_receipts, verify_version_s3
    require(branch in BRANCHES, "unknown formatted replacement branch")
    before = fleet.snapshot("replacement-original-" + branch, quiescent=True)
    node, disk = fleet.nodes[-1], 3
    member = node, disk
    physical_format = next(entry for entry in before[node]["formats"] if entry["disk"] == disk)
    proofs = []
    indexed = files(before)
    for entry in expected:
        if entry["deleteMarker"]: continue
        metadata = indexed[(node, disk, f"{bucket}/{entry['key']}/xl.meta")]["metadata"]
        version = next(version for version in metadata["versions"] if (version.get("versionId") or "null") == entry["versionId"])
        target = map_inline_target(before, bucket, entry, allow_empty=True) if version.get("inline") or entry["payload"]["size"] == 0 \
            else map_target(before, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False)
        shard = next(value for value in target["cohort"] if (value["node"], value["disk"]) == member)
        proofs.append({**target, **shard})
    replaced = fleet.record("replacement-original-root-isolated", fleet.remote(node, "replace-drive", disk=disk,
        expectedFormatSha256=physical_format["sha256"]))
    require(replaced.get("oldRootMovedIntact") is True and replaced.get("newDataRootEmpty") is True
            and replaced.get("sharedMountDetached") is False, "replacement lacks exact original and empty-root evidence")
    fleet.start()
    denied = fleet.record("replacement-backup-service-refusal", fleet.remote(node, "replacement-probe"))
    require(denied.get("serviceReadDenied") is True and denied.get("sameOriginalServiceProcess") is True,
            "replacement candidate can access its old physical backup")
    wait_format(fleet, node, disk)
    initialized = fleet.snapshot("replacement-candidate-initialization")
    fleet.record("replacement-new-format-proof", format_identity(before, initialized, member))
    require(not any(item["disk"] == disk for item in initialized[node]["files"]),
            "ATTRIBUTION_NOT_OBSERVED: new drive received object files before its manual repair token")
    require({key: value for key, value in files(initialized).items() if key[:2] != member} ==
            {key: value for key, value in indexed.items() if key[:2] != member}, "replacement initialization changed surviving objects")
    status = run_task(recorder.probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
                      option_overrides={"recursive": True}, admissions=admissions)
    receipts = validate_version_receipts(status, bucket, expected, 2,
        required_repairs=[(entry["key"], entry["versionId"]) for entry in expected])
    fleet.stop()
    after = fleet.snapshot("replacement-after-manual", quiescent=True)
    physical = fleet.record("replacement-full-physical-proof", physical_rebuild(before, after, member))
    fleet.record("replacement-original-archive-after-rebuild", fleet.remote(node, "replacement-state"))
    streaming = []
    for target in proofs:
        if target.get("zeroProof"):
            streaming.append({"key": target["key"], "zeroLengthMetadataVerified": True, "noPartInvented": True})
            continue
        operation = "verify-inline" if target.get("inlineProof") else "verify-part"
        params = {name: target[name] for name in ("disk", "relativePath", "versionId")}
        if operation == "verify-part": params["partNumber"] = target["partNumber"]
        proof = fleet.record("replacement-target-streaming-check", fleet.remote(node, operation, **params))
        require(proof.get("physicalBitrotVerified") is True, "reconstructed target has no verified streaming checksum")
        if operation == "verify-inline":
            require(proof.get("inlineLength") == target["inlineValue"]["length"]
                    and proof.get("inlineSha256") == target["inlineValue"]["sha256"], "new target inline bytes differ from the original same-role oracle")
        else:
            require(proof.get("sha256") == target["sha256"] and proof.get("physicalSize") == target["size"],
                    "new target external shard differs from the original same-role oracle")
        streaming.append({"key": target["key"], "versionId": target["versionId"], "proof": proof})
    fleet.start()
    fleet.record("replacement-backup-service-refusal-after", fleet.remote(node, "replacement-probe"))
    fixture = "DB" if branch == "new-disk-data" else "DV"
    s3 = verify_version_s3(recorder.probe, fleet, bucket, expected, baseline, "replacement-s3-after", fixture=fixture)
    peers = []
    for endpoint in recorder.probe.binding["endpoints"]:
        inventory = fleet.record("replacement-all-peer-s3", {"endpoint": endpoint,
            **Probe(recorder.probe.binding, endpoint).inventory(bucket, unversioned_current_tags=fixture == "DB")})
        peers.append({"endpoint": endpoint, **verify_seed(expected, inventory),
                      "manifest": compare_inventories(baseline, inventory)})
    return {"verdict": "PASS", "responsibility": receipts, "physical": physical,
            "allTargetVersionStreamingProofs": streaming, "s3": s3, "allPeerS3": peers}
