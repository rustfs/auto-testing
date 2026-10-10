"""C06: real offline member, acknowledged transitions and corrected authority."""

import copy
import hashlib

try:
    from .heal_probe import Probe, canonical, payload, require
    from .heal_workload import verify_seed
    from .healthy import physical_content
    from .shards import files, map_target, run_task, verify_cohort
    from .markers import add_marker, verify_markers
    from .scanner_repair import JournalWindow
except ImportError:
    from heal_probe import Probe, canonical, payload, require
    from heal_workload import verify_seed
    from healthy import physical_content
    from shards import files, map_target, run_task, verify_cohort
    from markers import add_marker, verify_markers
    from scanner_repair import JournalWindow


BRANCHES = ("unversioned-overwrite", "delete-historical-version", "delete-marker")


def version_id(value):
    return "null" if value in {None, "", "null", "00000000-0000-0000-0000-000000000000"} else value


def logical_metadata(metadata):
    result = copy.deepcopy(metadata)
    for version in result.get("versions", []):
        version["versionId"] = version_id(version.get("versionId"))
        version.pop("role", None)
        for name in ("index", "checksums"):
            version.get("erasure", {}).pop(name, None)
    return result


def identities(metadata):
    return {(version_id(entry.get("versionId")), entry.get("deleted"), entry.get("isLatest"))
            for entry in metadata.get("versions", [])}


def transition(recorder, bucket, branch, key, expected):
    manifest = copy.deepcopy(expected)
    data = [entry for entry in manifest if entry["key"] == key and not entry["deleteMarker"]]
    require(data and not any(entry["deleteMarker"] for entry in manifest if entry["key"] == key),
            "stale transition must begin from acknowledged data without a current marker")
    if branch == "unversioned-overwrite":
        require(len(data) == 1 and data[0]["versionId"] == "null", "overwrite requires a never-versioned object")
        nonce, size = 1, data[0]["payload"]["size"]
        body = payload(f"{bucket}/{key}/{nonce}", size)
        response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
            "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "DB", "nonce": "offline-overwrite"},
            "Tagging": "suite=heal&fixture=DB"}, body)
        require(response.get("VersionId") in {None, "null"}, "unversioned overwrite created an unexpected version")
        data[0].update(payload={"size": size, "sha256": hashlib.sha256(body).hexdigest()}, payloadNonce=nonce)
        return manifest, {"operation": "put_object", "newPayload": data[0]["payload"], "backupRestorationAllowed": False}
    if branch == "delete-historical-version":
        require(len(data) == 3 and not data[0]["isLatest"] and data[0]["versionId"] != "null", "delete requires a precise historical version")
        removed = data[0]
        response = recorder.call("delete_object", {"Bucket": bucket, "Key": key, "VersionId": removed["versionId"]})
        require(response.get("VersionId") == removed["versionId"] and response.get("DeleteMarker") in {None, False}
                and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 204, "historical DELETE lost its exact data-version ACK")
        return [entry for entry in manifest if entry != removed], {"operation": "delete_object_version",
            "deletedVersionId": removed["versionId"], "deletedDataDirFromOriginalRequired": True, "backupRestorationAllowed": False}
    require(branch == "delete-marker" and all(entry["versionId"] != "null" for entry in data), "marker branch requires enabled versions")
    response = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
    return add_marker(manifest, key, response), {"operation": "delete_object", "deleteMarkerId": response.get("VersionId"),
                                               "backupRestorationAllowed": False}


def authority_witness(before, authority, bucket, key, expected, target):
    left, right = files(before), files(authority)
    member = target["node"], target["disk"]
    path, prefix = f"{bucket}/{key}/xl.meta", f"{bucket}/{key}/"
    require({node: value["formats"] for node, value in before.items()} ==
            {node: value["formats"] for node, value in authority.items()}, "stale transition changed disk UUID/slot/format")
    require({identity: entry for identity, entry in left.items() if identity[:2] == member} ==
            {identity: entry for identity, entry in right.items() if identity[:2] == member}, "offline member changed during the acknowledged transition")
    require({identity: entry for identity, entry in left.items() if not identity[2].startswith(prefix)} ==
            {identity: entry for identity, entry in right.items() if not identity[2].startswith(prefix)}, "transition changed a control or another key")
    selected = [entry for entry in expected if entry["key"] == key]
    wanted = {(entry["versionId"], entry["deleteMarker"], entry["isLatest"]) for entry in selected}
    donors, normalized = [], None
    for shard in target["cohort"]:
        identity = shard["node"], shard["disk"], path
        if identity[:2] == member: continue
        metadata = right.get(identity, {}).get("metadata")
        require(metadata and metadata.get("bucket") == bucket and metadata.get("key") == key
                and identities(metadata) == wanted and len(metadata["versions"]) == len(wanted),
                "online member lacks the full acknowledged version/marker authority")
        logical = logical_metadata(metadata)
        require(normalized is None or canonical(logical) == canonical(normalized), "online same-version metadata does not form one full authority")
        normalized = logical
        donors.append(identity[:2])
    geometry = target["logical"]["erasure"]
    read = geometry["data_blocks"]
    write = read + int(read == geometry["parity_blocks"])
    require(len(donors) == len(target["cohort"]) - 1 >= max(read, write), "one member loss cannot admit this layout's read/write quorum")
    stale = right.get((*member, path), {}).get("metadata")
    require(stale and canonical(logical_metadata(stale)) != canonical(normalized), "NOT_OBSERVED: rejoin member was not genuinely stale")
    return {"readQuorum": read, "writeQuorum": write, "onlineAuthorities": len(donors), "member": list(member),
            "staleMetadataSha256": right[(*member, path)]["sha256"], "logicalAuthority": normalized,
            "offlineMemberAllOriginalFilesPreserved": True, "originalFormatsAndControlFilesPreserved": True}


def physical_rejoin(authority, after, bucket, key, expected, target, witness):
    left, right = files(authority), files(after)
    path, prefix, member = f"{bucket}/{key}/xl.meta", f"{bucket}/{key}/", (target["node"], target["disk"])
    require({node: value["formats"] for node, value in authority.items()} ==
            {node: value["formats"] for node, value in after.items()}, "rejoin changed original format/disk/set identities")
    # Only the stale member's selected key may gain/remove payload files. All
    # existing quorum shards and every control remain exact independent oracles.
    for identity, entry in left.items():
        if identity[:2] == member and identity[2].startswith(prefix): continue
        actual = right.get(identity)
        if identity[2] == path:
            require(actual and canonical(logical_metadata(actual.get("metadata", {}))) == canonical(witness["logicalAuthority"]),
                    "repair changed the committed online metadata authority")
        else:
            require(actual == entry, "stale repair lost or changed a quorum shard or control")
    for identity in set(right) - set(left):
        require(identity[:2] == member and identity[2].startswith(prefix), "stale repair created a file outside its exact member/key")
    current = right.get((*member, path), {})
    require(current.get("metadata") and current.get("sha256") != witness["staleMetadataSha256"]
            and canonical(logical_metadata(current["metadata"])) == canonical(witness["logicalAuthority"]),
            "stale member did not converge to the frozen ACK authority")
    live_directories = {entry.get("dataDir") for entry in witness["logicalAuthority"]["versions"] if not entry.get("deleted")}
    for identity, entry in left.items():
        if identity[:2] == member and any(identity[2].startswith(prefix + data_dir + "/")
                for data_dir in live_directories if data_dir) and identity[2].rsplit("/", 1)[-1].startswith("part."):
            require(right.get(identity) == entry, "stale repair changed an original retained same-drive data shard")
    cohorts = []
    for entry in expected:
        if entry["key"] != key or entry["deleteMarker"]: continue
        cohort = map_target(after, bucket, key, "data", 1, entry["versionId"], latest_required=False)
        require(cohort["logical"]["size"] == entry["payload"]["size"], "rejoined payload metadata differs from its ACK length")
        cohorts.append(cohort)
    require(cohorts, "rejoined key lost every retained acknowledged data version")
    return cohorts, {"staleMemberMatchesFrozenAuthority": True, "sameDiskUUIDAndSetSlot": True,
                     "controlsAndExistingQuorumShardBytesUnchanged": True, "backupRestoreUsed": False,
                     "newGenerationProof": "frozen quorum metadata, streaming protected-shard checks and independent ACK plaintext"}


def responsibility(status, bucket, key, expected, *, deleted=None):
    outcome = status.get("outcome") or {}
    wanted = {(entry["key"], entry["versionId"]): entry for entry in expected if entry["key"] == key}
    require(wanted and status.get("summary") == "finished" and status.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed" and outcome.get("coverage") == "complete"
            and outcome.get("objectsTruncated") is False, "stale task did not preserve complete terminal responsibility")
    allowed = set(wanted) | ({(key, deleted)} if deleted else set())
    seen, repairs, incarnation = set(), set(), None
    objects = outcome.get("objects", [])
    for receipt in objects:
        identity = receipt.get("identity", {})
        pair = identity.get("object"), version_id(identity.get("versionId"))
        require(pair in allowed and pair not in seen and identity.get("bucket") == bucket
                and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
                and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
                and isinstance(identity.get("bucketIncarnationId"), str) and identity["bucketIncarnationId"],
                "stale task mixed wrong/duplicate version, incarnation or pool/set responsibility")
        incarnation = incarnation or identity["bucketIncarnationId"]
        require(incarnation == identity["bucketIncarnationId"], "stale receipts mixed bucket incarnations")
        state = receipt.get("disposition", {}).get("state")
        if pair == (key, deleted):
            require(state in {"repaired", "authoritatively_absent"}, "deleted historical version was falsely reported live or unknown")
        else:
            require(state in {"repaired", "metadata_healthy" if wanted[pair]["deleteMarker"] else "verified_healthy"},
                    "stale version lacks an explicit correct repair/healthy disposition")
        if state == "repaired": repairs.add(pair)
        seen.add(pair)
    require(set(wanted) <= seen and repairs, "NOT_OBSERVED: original task did not cover and repair the stale ACK namespace")
    counters = outcome.get("counters", {})
    counts = {"processed": len(seen), "healed": len(repairs), "unchanged": len(seen) - len(repairs),
              "failed": 0, "skipped": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(counters.get(name)) is type(value) and counters[name] == value for name, value in counts.items()),
            "stale outcome counters hid errors or misrepresented actual responsibility")
    return {"completeRetainedAndOptionalDeletedVersionReceipts": True, "repairedIdentities": sorted(repairs),
            "bucketIncarnationId": incarnation, "counters": counters}


def run_branch(recorder, fleet, bucket, branch, expected, active, admissions, contract):
    # Lazy imports keep the shared version executor's dependency graph acyclic.
    try:
        from .versions import verify_version_s3, public_version_id
    except ImportError:
        from versions import verify_version_s3, public_version_id
    require(contract.get("recognized") is True and contract.get("writeQuorumRule") == "data-plus-one-if-equal-parity",
            "stale rejoin requires a recognized candidate read/write policy")
    key = "objects/0003.bin" if branch == "delete-marker" else "objects/0001.bin"
    selected = [entry for entry in expected if entry["key"] == key and not entry["deleteMarker"]]
    require(selected, "stale branch lost its target ACK")
    before = fleet.snapshot("stale-original-" + branch, quiescent=True)
    target = map_target(before, bucket, key, "data", 1, public_version_id(selected[0]["versionId"]), latest_required=False)
    # Pick an actual member from decoded geometry, keeping coordinator node1
    # separate on H2. Four disks are one lock host, not four independent peers.
    shard = next(item for item in reversed(target["cohort"]) if item["node"] == fleet.nodes[-1])
    target = {**target, **shard}
    verify_cohort(fleet, target, "stale-baseline-bitrot-" + branch)
    journal = JournalWindow(fleet, "stale-authority-" + branch)
    node = target["node"]
    fleet.drive_gates.add(node)
    fleet.record("stale-member-isolated", fleet.remote(node, "drive-gate", disk=target["disk"], mode="permission-denied"))
    fleet.start()
    denial = fleet.record("stale-member-service-denial", fleet.remote(node, "drive-gate-probe"))
    require(denial.get("readProbe", {}).get("errno") == 13 and denial.get("errno") == 13
            and denial.get("dacBypassRemoved") is True, "offline member lacks actual service-namespace read and write refusal")
    manifest, ack = transition(recorder, bucket, branch, key, expected)
    fleet.record("stale-transition-contract", {"branch": branch, **ack})
    baseline = fleet.record("stale-online-ack-inventory", recorder.probe.inventory(bucket, unversioned_current_tags=branch == "unversioned-overwrite"))
    fleet.record("stale-online-ack-proof", {"expected": manifest, **verify_seed(manifest, baseline)})
    fleet.stop()
    authority = fleet.snapshot("stale-online-authorities", quiescent=True)
    witness = fleet.record("stale-generation-authority-proof", authority_witness(before, authority, bucket, key, manifest, target))
    fleet.record("stale-original-member-rejoin", fleet.remote(node, "drive-gate", mode="clear"))
    fleet.drive_gates.remove(node)
    fleet.start()
    require(physical_content(authority) == physical_content(fleet.snapshot("stale-before-manual")),
            "ATTRIBUTION_NOT_OBSERVED: stale namespace changed before its manual token")
    status = run_task(recorder.probe, fleet, {"bucket": bucket, "key": key}, 2, active,
                      option_overrides={"recursive": True}, admissions=admissions)
    fleet.stop()
    after = fleet.snapshot("stale-after-manual", quiescent=True)
    cohorts, physical = physical_rejoin(authority, after, bucket, key, manifest, target, witness)
    streaming = [verify_cohort(fleet, cohort, "stale-repaired-streaming") for cohort in cohorts]
    receipts = responsibility(status, bucket, key, manifest, deleted=ack.get("deletedVersionId"))
    journal.capture()
    fleet.start()
    s3 = verify_version_s3(recorder.probe, fleet, bucket, manifest, baseline, "stale-s3-after", fixture="DB" if branch == "unversioned-overwrite" else "DV")
    deleted_probes = []
    for endpoint in recorder.probe.binding["endpoints"]:
        peer = Probe(recorder.probe.binding, endpoint)
        inventory = fleet.record("stale-all-peer-inventory", {"endpoint": endpoint, **peer.inventory(bucket,
            unversioned_current_tags=branch == "unversioned-overwrite")})
        verify_seed(manifest, inventory)
        if ack.get("deletedVersionId"):
            denied = fleet.record("stale-deleted-version-check", peer.s3("get_object", {"Bucket": bucket, "Key": key, "VersionId": ack["deletedVersionId"]}))
            response = denied.get("response") or {}
            require(denied.get("acknowledged") is False and denied.get("resultUnknown") is False and "payload" not in denied
                    and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
                    and response.get("Error", {}).get("Code") in {"NoSuchKey", "NoSuchVersion"}, "deleted historical version was resurrected or its absence is unproven")
            deleted_probes.append({"endpoint": endpoint, "deletedVersionId": ack["deletedVersionId"], "absent": True})
        if branch == "unversioned-overwrite":
            current = fleet.record("stale-current-get", peer.s3("get_object", {"Bucket": bucket, "Key": key}))
            wanted = next(entry for entry in manifest if entry["key"] == key)
            require(current.get("acknowledged") is True and current.get("payload") == wanted["payload"], "latest overwrite regressed to the old plaintext")
    if branch == "delete-marker": s3["markerIdentity"] = verify_markers(recorder.probe, fleet, bucket, key, manifest)
    fleet.stop()
    require(physical_content(after) == physical_content(fleet.snapshot("stale-after-s3", quiescent=True)), "S3 reads caused repair after manual convergence proof")
    fleet.start()
    return manifest, baseline, {"verdict": "PASS", "transition": ack, "authority": witness, "physical": physical,
        "streaming": streaming, "responsibility": receipts, "s3": s3, "deletedVersionProbes": deleted_probes,
        "generationSelectionJournalRetained": True, "manualTaskId": admissions[-1]["clientToken"]}
