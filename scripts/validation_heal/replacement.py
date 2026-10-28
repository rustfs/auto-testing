"""B03: full private-drive replacement with original same-slot shard oracles."""

import copy
import time
import uuid

try:
    from .heal_probe import Probe, canonical, compare_inventories, require
    from .heal_workload import verify_seed
    from .healthy import validate_topology
    from .shards import files, map_target, run_task
    from .fleet_node import PROCESS_FIELDS
    from .scanner_repair import JournalWindow, SOURCE_TARGETS, event_time
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, require
    from heal_workload import verify_seed
    from healthy import validate_topology
    from shards import files, map_target, run_task
    from fleet_node import PROCESS_FIELDS
    from scanner_repair import JournalWindow, SOURCE_TARGETS, event_time


BRANCHES = ("new-disk-data", "new-disk-versioned")
AUTOMATIC_BRANCHES = ("replacement-unversioned", "replacement-versioned-marker")
GRACEFUL_BRANCHES = ("target-partial-graceful", "versioned-target-partial")
CRASH_BRANCHES = ("target-partial-sigkill", "target-partial-sigkill-versioned")
COORDINATOR_BRANCHES = ("replacement-coordinator-kill",)
FORMAT_SECONDS = 90
REBUILD_SECONDS = 300
STATUS_PATH = "/rustfs/admin/v4/heal/replacement-recovery"


def replacement_records(probe, fleet, label):
    observed = {}
    for endpoint in probe.binding["endpoints"]:
        value = fleet.record(label, {"endpoint": endpoint,
            **Probe(probe.binding, endpoint).admin(STATUS_PATH, [], b"", method="GET")})
        body = value.get("response", {})
        cluster = body.get("cluster", {})
        require(value.get("httpStatus") == 200 and type(body.get("contractVersion")) is int
                and body["contractVersion"] == 2 and body.get("scope") == "cluster_survivor_disks"
                and cluster.get("definitive") is True and cluster.get("reason") is None
                and isinstance(cluster.get("records"), list), "replacement source status is partial, unknown or incompatible")
        observed[endpoint] = cluster["records"]
    return observed


def generation(records, endpoint, earliest, *, original=None, completed=False):
    require(isinstance(records, list) and len(records) == 1 and isinstance(records[0], dict),
            "replacement did not expose exactly one owned target generation")
    record = records[0]
    token = record.get("taskId")
    require(isinstance(token, str) and str(uuid.UUID(token)) == token and record.get("generation") == token
            and record.get("setDiskId") == "pool_0_set_0" and record.get("targetSlots") == [endpoint],
            "replacement generation is not bound to the exact target endpoint/set")
    binding = {name: record[name] for name in ("taskId", "generation", "setDiskId", "targetSlots")}
    require(original is None or binding == original, "automatic repair crossed replacement generations or target members")
    require(record.get("state") in {"waiting_for_replacement", "running", "cleanup_pending", "completed"}
            and record.get("reason") is None, "replacement generation is incomplete, unrecoverable or unknown")
    if record["state"] == "completed":
        require(type(record.get("verifiedAt")) is int and earliest <= record["verifiedAt"] <= int(time.time()) + 5,
                "replacement completion is stale or has no actual verification timestamp")
    else: require(record.get("verifiedAt") is None, "unfinished replacement claimed a completion timestamp")
    require(not completed or record["state"] == "completed", "replacement generation has not completed its responsibility")
    return binding


def target_progress(snapshot, proofs):
    indexed, complete, external_complete, external_pending = files(snapshot), [], 0, 0
    for target in proofs:
        identity = target["node"], target["disk"], f"{target['bucket']}/{target['key']}/xl.meta"
        metadata = indexed.get(identity, {}).get("metadata")
        matched = None if not metadata else next((version for version in metadata["versions"]
            if (version.get("versionId") or "null") == target["versionId"]), None)
        valid = False
        if matched is not None:
            logical = {name: matched.get(name) for name in target["logical"] if name != "erasure"}
            logical["erasure"] = {name: matched.get("erasure", {}).get(name) for name in target["logical"]["erasure"]}
            require(logical == target["logical"] and matched.get("role") == target["role"]
                    and matched.get("erasure", {}).get("index") == target["index"],
                    "partial replacement changed the acknowledged version authority or original shard role")
            if target.get("zeroProof"):
                require(matched.get("size") == 0 and not matched.get("inline"), "replacement invented data for a zero-length ACK")
                valid = True
            elif target.get("inlineProof"):
                inline = matched.get("inline") or {}
                require(inline.get("length") == target["inlineValue"]["length"]
                        and inline.get("sha256") == target["inlineValue"]["sha256"], "partial replacement committed incorrect inline bytes")
                valid = True
            else:
                entry = indexed.get((target["node"], target["disk"], target["relativePath"]))
                if entry:
                    require(entry["sha256"] == target["sha256"] and entry["size"] == target["size"],
                            "partial replacement committed incorrect original same-role shard bytes")
                    valid = True
        if valid: complete.append((target["key"], target["versionId"]))
        if not target.get("zeroProof") and not target.get("inlineProof"):
            if valid: external_complete += 1
            else: external_pending += 1
    return {"completeDataVersions": complete, "dataVersions": len(proofs),
            "externalComplete": external_complete, "externalPending": external_pending,
            "partial": 0 < len(complete) < len(proofs) and external_complete > 0 and external_pending > 0}


def target_restart_identity(before, after, target):
    require(set(before) == set(after) and target in before and len(before) == 4, "target restart omitted an actual H2 peer")
    for node, old in before.items():
        new = after[node]
        ignored = {"pid", "startTicks", "mountNamespace"} if node == target else set()
        require(all(new.get(name) == old.get(name) for name in PROCESS_FIELDS if name not in ignored),
                "target restart changed candidate, boot, unit, settings or a non-target peer")
        if node == target:
            require(type(new.get("pid")) is int and new["pid"] > 1 and type(new.get("startTicks")) is int
                    and isinstance(new.get("mountNamespace"), str) and new["mountNamespace"]
                    and (new["pid"], new["startTicks"]) != (old["pid"], old["startTicks"]), "replacement target was not actually restarted")
    return {"onlyOriginalTargetProcessRestarted": True, "sameBootCandidateUnitSettingsAndRoots": True,
            "newPrivateMountNamespaceMustProveArchiveRefusal": True}


def interrupt_target(fleet, node, proofs, mode, original_format):
    before = fleet.record("replacement-processes-before-interruption", {peer: fleet.remote(peer, "process") for peer in fleet.nodes})
    identity = before[node]
    fleet.frozen_processes[node] = identity
    fleet.record("replacement-target-freeze", fleet.remote(node, "process-signal", signal="SIGSTOP", expected=identity))
    frozen = fleet.snapshot("replacement-target-frozen-physical")
    progress = target_progress(frozen, proofs)
    require(progress["partial"], "NOT_OBSERVED: replacement was not physically partial at its frozen interruption boundary")
    require(all(frozen[peer]["formats"] == original_format[peer]["formats"] for peer in fleet.nodes),
            "partial replacement changed its new target UUID or another disk identity")
    exit_proof = fleet.record("replacement-target-actual-exit", fleet.remote(node,
        "process-graceful" if mode == "graceful" else "process-crash", expected=identity))
    if mode == "graceful":
        require(exit_proof.get("actualExitCode") == 0 and exit_proof.get("signal") == "SIGTERM", "target has no real graceful exit")
    else:
        require(exit_proof.get("actualSignal") == 9 and exit_proof.get("graceful") is False
                and exit_proof.get("platformEventIssued") is False, "target crash lacks an exact SIGKILL witness")
    del fleet.frozen_processes[node]
    fleet.record("replacement-target-release-unit", fleet.remote(node, "stop"))
    stopped = fleet.record("replacement-target-after-exit-physical", {peer: fleet.remote(peer, "snapshot", quiescent=peer == node) for peer in fleet.nodes})
    after_progress = target_progress(stopped, proofs)
    require(after_progress["partial"] and set(progress["completeDataVersions"]) <= set(after_progress["completeDataVersions"]),
            "target exit lost a committed shard or missed the partial interruption window")
    require(all(stopped[peer]["formats"] == original_format[peer]["formats"] for peer in fleet.nodes),
            "target exit changed the original replacement generation's physical identity")
    fleet.record("replacement-original-target-start", fleet.remote(node, "start"))
    fleet.wait_ready()
    after = fleet.record("replacement-processes-after-interruption", {peer: fleet.remote(peer, "process") for peer in fleet.nodes})
    continuity = target_restart_identity(before, after, node)
    archive = fleet.record("replacement-backup-refusal-after-interruption", fleet.remote(node, "replacement-probe"))
    require(archive.get("serviceReadDenied") is True and archive.get("sameOriginalServiceProcess") is True,
            "restarted target can access its original backup")
    return {"interruption": mode, "before": progress, "afterActualExit": after_progress,
            "actualExit": exit_proof, "processContinuity": continuity, "sameNewDiskUUIDAndSetSlot": True}


def coordinator(events, token, target):
    owners = []
    for node, rows in events.items():
        admitted = [event for event in rows if event.get("target") == "rustfs::heal::manager"
            and event.get("request_id") == token and event.get("source") == "auto_heal"
            and event.get("result") == "accepted" and event.get("message") == "Heal queue request accepted"
            and event_time(event) is not None]
        started = [event for event in rows if event.get("target") == "rustfs::heal::task"
            and event.get("task_id") == token and event.get("set_disk_id") == "pool_0_set_0"
            and event.get("stage") == "start" and event.get("message") == "Heal erasure set started"
            and event_time(event) is not None]
        if any(event_time(start) >= event_time(admission) for start in started for admission in admitted):
            owners.append(node)
    require(len(owners) == 1 and owners[0] != target,
            "NOT_OBSERVED: replacement has no uniquely witnessed coordinator distinct from the interrupted target")
    return owners[0]


def automatic_completion(probe, fleet, endpoint, earliest, *, interruption=None, proofs=(), original_format=None,
                         journal=None, coordinator_interruptor=None):
    deadline, original, timeline = time.monotonic() + REBUILD_SECONDS, None, []
    interrupted = None
    while True:
        observed = replacement_records(probe, fleet, "automatic-replacement-generation-window")
        ready = True
        for peer, records in observed.items():
            if not records:
                ready = False
                continue
            bound = generation(records, endpoint, earliest, original=original)
            original = original or bound
            ready = ready and records[0]["state"] == "completed"
        timeline.append(observed)
        if interruption is not None and interrupted is None:
            require(not ready, "NOT_OBSERVED: replacement completed before its required target interruption")
            if any(records and records[0]["state"] == "running" for records in observed.values()):
                progress = target_progress(fleet.snapshot("replacement-running-physical-window"), proofs)
                if progress["partial"]:
                    owner = coordinator(journal.capture(), original["taskId"], proofs[0]["node"])
                    fleet.record("replacement-distinct-coordinator-proof", {"node": owner,
                        "target": proofs[0]["node"], "originalGeneration": original, "source": "fresh accepted auto_heal and exact erasure task start"})
                    if interruption == "coordinator-sigkill":
                        require(coordinator_interruptor is not None, "replacement coordinator observer missing")
                        interrupted = coordinator_interruptor(fleet, owner, proofs, original_format, original)
                    else:
                        interrupted = interrupt_target(fleet, proofs[0]["node"], proofs, interruption, original_format)
                        interrupted["coordinatorKeptRunning"] = owner
                    continue
        if ready and original is not None:
            require(len({canonical(records[0]) for records in observed.values()}) == 1,
                    "completed replacement responsibility differs across peers")
            break
        require(time.monotonic() < deadline, "ATTRIBUTION_NOT_OBSERVED: original automatic replacement generation did not complete on every peer")
        time.sleep(2)
    for index in range(2):
        repeated = replacement_records(probe, fleet, "automatic-replacement-completion-repeat")
        for records in repeated.values(): generation(records, endpoint, earliest, original=original, completed=True)
        require(canonical(repeated) == canonical(observed), "completed replacement generation changed after its terminal observation")
    require(interruption is None or interrupted is not None, "NOT_OBSERVED: no actual partial target interruption occurred")
    return {"source": "automatic-replacement", "generation": original, "observations": len(timeline),
            "completionObservedOnEveryPeer": True, "budgetSeconds": REBUILD_SECONDS,
            "adminHealMutations": 0, "targetS3ReadsBeforePhysicalProof": 0,
            "coordinatorInterruption" if interruption == "coordinator-sigkill" else "targetInterruption": interrupted}


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


def run_branch(recorder, fleet, bucket, branch, expected, baseline, active, admissions, *, coordinator_interruptor=None):
    try:
        from .versions import map_inline_target, validate_version_receipts, verify_version_s3
    except ImportError:
        from versions import map_inline_target, validate_version_receipts, verify_version_s3
    automatic = branch in (*AUTOMATIC_BRANCHES, *GRACEFUL_BRANCHES, *CRASH_BRANCHES, *COORDINATOR_BRANCHES)
    require(branch in (*BRANCHES, *AUTOMATIC_BRANCHES, *GRACEFUL_BRANCHES, *CRASH_BRANCHES, *COORDINATOR_BRANCHES), "unknown replacement branch")
    require(branch not in COORDINATOR_BRANCHES or len(fleet.nodes) == 4 and coordinator_interruptor is not None,
            "replacement coordinator crash requires actual H2 and its independent observer")
    require(not automatic or fleet.runtime_profile == "replacement", "automatic replacement lacks its isolated source configuration")
    before = fleet.snapshot("replacement-original-" + branch, quiescent=True)
    if automatic:
        fleet.start()
        healthy = replacement_records(recorder.probe, fleet, "automatic-replacement-empty-baseline")
        require(all(not records for records in healthy.values()), "replacement baseline retained an older generation")
        fleet.stop()
        require(files(before) == files(fleet.snapshot("replacement-original-after-source-preflight", quiescent=True)),
                "replacement source preflight changed an acknowledged original file")
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
    earliest = int(time.time())
    journal = JournalWindow(fleet, "replacement-interruption-source", event_targets=SOURCE_TARGETS) \
        if branch in (*GRACEFUL_BRANCHES, *CRASH_BRANCHES, *COORDINATOR_BRANCHES) else None
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
    if not automatic:
        require(not any(item["disk"] == disk for item in initialized[node]["files"]),
                "ATTRIBUTION_NOT_OBSERVED: new drive received object files before its manual repair token")
    require({key: value for key, value in files(initialized).items() if key[:2] != member} ==
            {key: value for key, value in indexed.items() if key[:2] != member}, "replacement initialization changed surviving objects")
    if automatic:
        endpoint = replaced["dataPath"] if len(fleet.nodes) == 1 else f"http://rustfs-node{fleet.nodes.index(node) + 1}:9000{replaced['dataPath']}"
        receipts = automatic_completion(recorder.probe, fleet, endpoint, earliest,
            interruption="graceful" if branch in GRACEFUL_BRANCHES else "sigkill" if branch in CRASH_BRANCHES else
                "coordinator-sigkill" if branch in COORDINATOR_BRANCHES else None,
            proofs=proofs, original_format=initialized, journal=journal, coordinator_interruptor=coordinator_interruptor)
    else:
        status = run_task(recorder.probe, fleet, {"bucket": bucket, "key": ""}, 2, active,
                          option_overrides={"recursive": True}, admissions=admissions)
        receipts = validate_version_receipts(status, bucket, expected, 2,
            required_repairs=[(entry["key"], entry["versionId"]) for entry in expected])
    fleet.stop()
    after = fleet.snapshot("replacement-after-manual", quiescent=True)
    physical = fleet.record("replacement-full-physical-proof", physical_rebuild(before, after, member))
    fleet.record("replacement-original-archive-after-rebuild", fleet.remote(node, "replacement-state"))
    streaming, external = [], []
    for target in proofs:
        if target.get("zeroProof"):
            streaming.append({"key": target["key"], "zeroLengthMetadataVerified": True, "noPartInvented": True})
            continue
        operation = "verify-inline" if target.get("inlineProof") else "verify-part"
        if operation == "verify-part":
            external.append(target)
            continue
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
    for offset in range(0, len(external), 128):
        batch = external[offset:offset + 128]
        observed = fleet.record("replacement-target-streaming-batch", fleet.remote(node, "verify-parts", parts=[
            {name: target[name] for name in ("disk", "relativePath", "versionId", "partNumber")} for target in batch]))
        require(observed.get("complete") is True and isinstance(observed.get("parts"), list)
                and len(observed["parts"]) == len(batch), "replacement streaming batch omitted an exact target part")
        for target, proof in zip(batch, observed["parts"], strict=True):
            require(proof.get("physicalBitrotVerified") is True
                    and all(proof.get(name) == target[name] for name in ("disk", "relativePath", "versionId", "partNumber", "sha256"))
                    and proof.get("physicalSize") == target["size"], "replacement target streaming batch changed an acknowledged same-role part")
            streaming.append({"key": target["key"], "versionId": target["versionId"], "proof": proof})
    fleet.start()
    fleet.record("replacement-backup-service-refusal-after", fleet.remote(node, "replacement-probe"))
    fixture = "DB" if branch in {"new-disk-data", "replacement-unversioned", "target-partial-graceful", "target-partial-sigkill", *COORDINATOR_BRANCHES} else "DV"
    s3 = verify_version_s3(recorder.probe, fleet, bucket, expected, baseline, "replacement-s3-after", fixture=fixture)
    peers = []
    for endpoint in recorder.probe.binding["endpoints"]:
        inventory = fleet.record("replacement-all-peer-s3", {"endpoint": endpoint,
            **Probe(recorder.probe.binding, endpoint).inventory(bucket, unversioned_current_tags=fixture == "DB")})
        peers.append({"endpoint": endpoint, **verify_seed(expected, inventory),
                      "manifest": compare_inventories(baseline, inventory)})
    return {"verdict": "PASS", "responsibility": receipts, "physical": physical,
            "allTargetVersionStreamingProofs": streaming, "s3": s3, "allPeerS3": peers}
