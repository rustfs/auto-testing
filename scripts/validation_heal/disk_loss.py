"""B01: original member return and physical repair of degraded write ACKs."""

import copy
import hashlib
import time

try:
    from .heal_probe import Probe, canonical, compare_inventories, payload, require
    from .heal_workload import verify_seed
    from .healthy import physical_content
    from .shards import files, map_target, run_task
    from .stale_rejoin import logical_metadata, identities
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, payload, require
    from heal_workload import verify_seed
    from healthy import physical_content
    from shards import files, map_target, run_task
    from stale_rejoin import logical_metadata, identities


BRANCHES = ("original-drive-return", "offline-ack-repair")
SIZE = 5 * 1024**2 + 123


def quorum_plan(target, contract):
    require(contract.get("recognized") is True
            and contract.get("writeQuorumRule") == "data-plus-one-if-equal-parity",
            "disk loss requires the recognized candidate quorum policy")
    geometry = target["logical"]["erasure"]
    read, parity = geometry["data_blocks"], geometry["parity_blocks"]
    total, write = read + parity, read + int(read == parity)
    require(total == len(target["cohort"]) and total in {4, 16} and total - 1 >= max(read, write),
            "one member loss cannot admit the actual layout's degraded read/write ACKs")
    return {"totalDrives": total, "readQuorum": read, "writeQuorum": write,
            "onlineSameSetDrives": total - 1, "diskUUID": target["diskUUID"],
            "setSlot": target["setSlot"], "node": target["node"], "disk": target["disk"],
            "lockAdmission": "each write's actual ACK; no disk-count lock inference"}


def info_disks(observed, total):
    require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
            "member-state Admin response is unavailable")
    servers = observed["response"].get("info", {}).get("servers")
    require(isinstance(servers, list) and servers, "Admin info omitted its actual server members")
    disks = [disk for server in servers for disk in server.get("drives", [])]
    require(len(disks) == total, "Admin info omitted or duplicated a set member")
    indexed = {}
    for disk in disks:
        slot = tuple(disk.get(name) for name in ("pool_index", "set_index", "disk_index"))
        require(all(type(value) is int for value in slot) and slot[:2] == (0, 0)
                and 0 <= slot[2] < total and slot not in indexed
                and isinstance(disk.get("endpoint"), str) and disk["endpoint"],
                "Admin info lacks distinct exact endpoint/pool/set/disk identities")
        indexed[slot] = disk
    return indexed


def member_state(observed, target, total, baseline=None):
    indexed = info_disks(observed, total)
    slot = (0, 0, target["setSlot"])
    selected = indexed[slot]
    expected_uuids = {(0, 0, shard["setSlot"]): shard["diskUUID"] for shard in target["cohort"]}
    if baseline is None:
        require(all(disk.get("state") == "ok" and disk.get("runtimeState") in {None, "online"}
                    and isinstance(disk.get("uuid"), str) and disk["uuid"] for disk in indexed.values()),
                "baseline Admin members were not positively online")
        require(selected["uuid"] == target["diskUUID"], "Admin slot differs from the original physical disk UUID")
        require(all(indexed[member_slot]["uuid"] == expected_uuid for member_slot, expected_uuid in expected_uuids.items()),
                "Admin topology differs from the actual formatted full-set membership")
        return {"endpoint": selected["endpoint"], "slot": list(slot), "uuid": selected["uuid"]}
    require(selected["endpoint"] == baseline["endpoint"] and selected.get("uuid") in {"", baseline["uuid"]},
            "offline Admin witness changed the original member's endpoint/UUID")
    for other_slot, disk in indexed.items():
        if other_slot == slot: continue
        require(disk.get("state") == "ok" and disk.get("runtimeState") in {None, "online"}
                and disk.get("uuid") == expected_uuids[other_slot],
                "another Admin member is unavailable; single-drive admission is unproven")
    # The pinned candidate counts these explicit disk errors as offline. An
    # unknown/initializing server or aggregate count never witnesses this disk.
    require(selected.get("state") in {"disk not found", "drive access denied", "disk access denied", "file access denied",
                "volume access denied", "offline"} or selected.get("runtimeState") == "offline",
            "NOT_OBSERVED: exact gated member is not explicitly offline in Admin info")
    return {**baseline, "reportedUUID": selected.get("uuid"), "state": selected.get("state"),
            "runtimeState": selected.get("runtimeState"), "exactOriginalMemberOffline": True}


def offline_ack(recorder, fleet, bucket, branch, expected):
    require(branch in BRANCHES, "unknown disk loss branch")
    manifest, new = copy.deepcopy(expected), []
    # Each branch admits new keys while the original member is genuinely
    # inaccessible; neither overwrites controls nor reuses another branch ACK.
    for index in range(1 if branch == "original-drive-return" else 6):
        key = f"disk-loss/{branch}/new-{index:02d}.bin"
        require(not any(entry["key"] == key for entry in manifest), "offline key already has a fixture authority")
        body = payload(f"{bucket}/{key}/0", SIZE + index)
        response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
            "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "DB", "branch": branch},
            "Tagging": "suite=heal&fixture=DB"}, body)
        require(response.get("VersionId") in {None, "null"}, "degraded unique PUT created a foreign version")
        entry = {"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                 "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}}
        manifest.append(entry); new.append(entry)
        # GET every new ACK plus the complete original manifest below. The gate
        # remains active and its final witness brackets all operations.
        observed = fleet.record("disk-loss-degraded-get", recorder.probe.s3("get_object", {"Bucket": bucket, "Key": key}))
        require(observed.get("acknowledged") is True and observed.get("payload") == entry["payload"],
                "degraded GET differs from the exact unique-key ACK")
    return manifest, new


def authority_witness(before, authority, bucket, target, new):
    left, right = files(before), files(authority)
    member = target["node"], target["disk"]
    require({node: value["formats"] for node, value in before.items()} ==
            {node: value["formats"] for node, value in authority.items()}, "offline ACKs changed original disk UUID/slot")
    require(all(right.get(identity) == entry for identity, entry in left.items()),
            "offline ACKs changed an existing original object/shard/control")
    require({identity: entry for identity, entry in left.items() if identity[:2] == member} ==
            {identity: entry for identity, entry in right.items() if identity[:2] == member},
            "offline member received writes during degraded ACKs")
    additions = [identity for identity in right if identity not in left]
    prefixes = [f"{bucket}/{entry['key']}/" for entry in new]
    require(new and len({entry["key"] for entry in new}) == len(new)
            and all(identity[:2] != member and any(identity[2].startswith(prefix) for prefix in prefixes)
                    for identity in additions), "degraded ACKs wrote outside their exact new-key responsibility")
    frozen = {}
    for entry in new:
        logical = None
        for shard in target["cohort"]:
            peer = shard["node"], shard["disk"]
            if peer == member: continue
            path = f"{bucket}/{entry['key']}/xl.meta"
            metadata = right.get((*peer, path), {}).get("metadata")
            require(metadata and metadata.get("bucket") == bucket and metadata.get("key") == entry["key"]
                    and identities(metadata) == {("null", False, True)} and len(metadata["versions"]) == 1,
                    "online member lacks the exact unique-key ACK authority")
            normalized = logical_metadata(metadata)
            require(logical is None or canonical(logical) == canonical(normalized),
                    "degraded new-key metadata lacks one full online authority")
            logical = normalized
            version = metadata["versions"][0]
            require(version.get("inline") is None and version.get("size") == entry["payload"]["size"]
                    and version.get("dataDir") and version.get("usesLegacyChecksum") is False,
                    "degraded new-key ACK lacks a protected external part")
            part = right.get((*peer, f"{bucket}/{entry['key']}/{version['dataDir']}/part.1"))
            require(part and part["size"] > 32, "online ACK authority omitted its actual payload shard")
        frozen[entry["key"]] = logical
    return {"logicalAuthorities": frozen, "originalMemberUnchangedDuringAllACKs": True,
            "originalControlsAndFormatsUnchanged": True, "onlineAuthorities": len(target["cohort"]) - 1}


def repaired_physical(authority, after, bucket, target, new, witness):
    left, right = files(authority), files(after)
    member = target["node"], target["disk"]
    require({node: value["formats"] for node, value in authority.items()} ==
            {node: value["formats"] for node, value in after.items()}, "return changed original disk UUID/slot")
    metadata_paths = {f"{bucket}/{entry['key']}/xl.meta": entry["key"] for entry in new}
    for identity, entry in left.items():
        if identity[2] in metadata_paths:
            metadata = right.get(identity, {}).get("metadata")
            require(metadata and canonical(logical_metadata(metadata)) == canonical(
                witness["logicalAuthorities"][metadata_paths[identity[2]]]), "repair changed online ACK metadata authority")
        else:
            require(right.get(identity) == entry, "repair changed an original control or surviving payload shard")
    cohorts, allowed = [], set()
    for entry in new:
        cohort = map_target(after, bucket, entry["key"], "data", 1)
        require(canonical(logical_metadata(cohort["metadata"])) == canonical(witness["logicalAuthorities"][entry["key"]]),
                "rejoined key differs from its frozen degraded ACK authority")
        shard = next(item for item in cohort["cohort"] if (item["node"], item["disk"]) == member)
        part = (*member, shard["relativePath"])
        metadata = (*member, f"{bucket}/{entry['key']}/xl.meta")
        require(part not in left and metadata not in left and part in right and metadata in right,
                "NOT_OBSERVED: original member lacks newly reconstructed ACK metadata/payload")
        require(shard["diskUUID"] == target["diskUUID"] and shard["setSlot"] == target["setSlot"],
                "repair used a replacement disk or different member slot")
        allowed.update((part, metadata)); cohorts.append(cohort)
    require(set(right) - set(left) == allowed, "repair created unexpected or missing files outside the original member's ACKs")
    return cohorts, {"sameDiskUUIDAndSetSlot": True, "allDegradedACKTargetPartsCreated": True,
        "originalAndOnlineShardBytesUnchanged": True, "backupRestorationUsed": False,
        "newShardOracle": "frozen protected online metadata, actual streaming checksums, independent ACK plaintext"}


def run_branch(recorder, fleet, bucket, branch, expected, active, admissions, contract):
    try:
        from .versions import validate_version_receipts, verify_version_s3
    except ImportError:
        from versions import validate_version_receipts, verify_version_s3
    before = fleet.snapshot("disk-loss-original-" + branch, quiescent=True)
    target = map_target(before, bucket, "objects/0008.bin", "data", 1)
    target = {**target, **next(shard for shard in reversed(target["cohort"]) if shard["node"] == fleet.nodes[-1])}
    plan = fleet.record("disk-loss-quorum-plan", quorum_plan(target, contract))
    fleet.start()
    baseline = member_state(fleet.record("disk-loss-admin-baseline", recorder.probe.admin(
        "/rustfs/admin/v3/info", [], b"", method="GET")), target, plan["totalDrives"])
    fleet.stop()
    node = target["node"]
    fleet.drive_gates.add(node)
    gate = fleet.record("disk-loss-gate-armed", fleet.remote(node, "drive-gate", disk=target["disk"], mode="permission-denied"))
    fleet.start()
    def witness_offline(label):
        denial = fleet.record("disk-loss-service-read-refusal-" + label, fleet.remote(node, "drive-gate-probe"))
        require(denial.get("readProbe", {}).get("errno") == 13 and denial.get("errno") == 13
                and denial.get("dacBypassRemoved") is True, "member gate lacks actual same-service read/write refusal")
        deadline = time.monotonic() + 90
        while True:
            observed = fleet.record("disk-loss-admin-offline-" + label, recorder.probe.admin(
                "/rustfs/admin/v3/info", [], b"", method="GET"))
            try:
                proof = member_state(observed, target, plan["totalDrives"], baseline)
                break
            except ValueError as exc:
                if not str(exc).startswith("NOT_OBSERVED:"): raise
                require(time.monotonic() < deadline, str(exc))
                time.sleep(2)
        require(gate["dataPath"] == denial["dataPath"], "service denial moved to a different logical root")
        return fleet.record("disk-loss-double-witness-" + label, {"service": denial, "admin": proof})
    first = witness_offline("before-acks")
    manifest, new = offline_ack(recorder, fleet, bucket, branch, expected)
    baseline_s3 = fleet.record("disk-loss-degraded-full-inventory", recorder.probe.inventory(bucket, unversioned_current_tags=True))
    fleet.record("disk-loss-degraded-full-proof", {"expected": manifest, **verify_seed(manifest, baseline_s3)})
    last = witness_offline("after-acks")
    fleet.stop()
    authority = fleet.snapshot("disk-loss-online-authorities", quiescent=True)
    witness = fleet.record("disk-loss-ack-authority", authority_witness(before, authority, bucket, target, new))
    fleet.record("disk-loss-original-member-return", fleet.remote(node, "drive-gate", mode="clear"))
    fleet.drive_gates.remove(node)
    fleet.start()
    require(physical_content(authority) == physical_content(fleet.snapshot("disk-loss-before-manual")),
            "ATTRIBUTION_NOT_OBSERVED: original member gained ACK shards before the manual token")
    receipts = []
    for entry in new:
        status = run_task(recorder.probe, fleet, {"bucket": bucket, "key": entry["key"]}, 2, active,
                          option_overrides={"recursive": True}, admissions=admissions)
        receipts.append(validate_version_receipts(status, bucket, [entry], 2,
            required_repairs=[(entry["key"], "null")]))
    fleet.stop()
    after = fleet.snapshot("disk-loss-repaired", quiescent=True)
    cohorts, physical = repaired_physical(authority, after, bucket, target, new, witness)
    streaming = []
    for cohort in cohorts:
        for shard in cohort["cohort"]:
            proof = fleet.record("disk-loss-protected-streaming-check", {"node": shard["node"],
                **fleet.remote(shard["node"], "verify-part", disk=shard["disk"], relativePath=shard["relativePath"],
                    versionId="null", partNumber=1)})
            require(proof.get("physicalBitrotVerified") is True and proof.get("sha256") == shard["sha256"]
                    and proof.get("physicalSize") == shard["size"], "rejoined shard lacks a matching actual protected checksum")
            streaming.append(proof)
    fleet.start()
    s3 = verify_version_s3(recorder.probe, fleet, bucket, manifest, baseline_s3, "disk-loss-full-s3-after", fixture="DB")
    peers = []
    for endpoint in recorder.probe.binding["endpoints"]:
        inventory = fleet.record("disk-loss-all-peer-s3", {"endpoint": endpoint,
            **Probe(recorder.probe.binding, endpoint).inventory(bucket, unversioned_current_tags=True)})
        peers.append({"endpoint": endpoint, **verify_seed(manifest, inventory),
                      "fullPublicManifest": compare_inventories(baseline_s3, inventory)})
    return manifest, baseline_s3, {"verdict": "PASS", "quorums": plan, "beforeACKs": first, "afterACKs": last,
        "degradedUniqueACKs": len(new), "responsibility": receipts, "physical": physical,
        "streaming": streaming, "s3": s3, "allPeerS3": peers}
