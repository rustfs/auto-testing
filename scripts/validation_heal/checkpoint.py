"""Locate original Admin erasure-set responsibility before F07/C11 faults."""

import base64
import json
import time

try:
    from . import journal_fault as journal
    from .healthy import BASE_OPTIONS
    from .heal_probe import canonical, require
    from .fleet_node import PROCESS_FIELDS
except ImportError:
    import journal_fault as journal
    from healthy import BASE_OPTIONS
    from heal_probe import canonical, require
    from fleet_node import PROCESS_FIELDS


PATH = "/rustfs/admin/v3/heal/"
OPTIONS = {**BASE_OPTIONS, "recursive": True}
REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926", "sourceFiles": {
    "rustfs/src/admin/handlers/heal.rs": "7e4007c29a8329ced8e48b88111400e79cffd845bd6ddc6243b79f3cd48e8199",
    "crates/heal/src/heal/channel.rs": "7f669ca43b9559db9b97fba6b419a6c76a94d76acd8526db132d74b7c534d6b0",
    "crates/heal/src/heal/resume.rs": "ed55baa03fafa79bf662393eafcf53b4f7752ca8b2f77824cc6deff085a0cf1d",
    "crates/heal/src/heal/resume/checkpoint.rs": "fe067b39ee406e16517bd665f7f49457daffa05d5d7bc298bd965e927b1381c1",
    "crates/heal/src/heal/manager/root_recovery.rs": "ba6beba9d9d14985e4c5448d85727036388d3d72f28879ac5c67fc5f760048ba",
    "crates/heal/src/heal/erasure_healer/admin.rs": "776c03e4dfbd27f51da506d8f637de3a3973d86d22dc83a7cda24f2e0d642ee5"},
    "route": "empty bucket/object plus explicit pool=0,set=0 selects Admin ErasureSet",
    "checkpointSchema": 8, "resumeSchema": 7, "durableOwnerRequired": True,
    "bucketOrClusterScanIsCheckpointProof": False, "canonicalPassClaimed": False}


def bytes_of(row):
    require(row.get("present") is True and isinstance(row.get("rawBase64"), str), "record has no original bytes")
    raw = base64.b64decode(row["rawBase64"], validate=True)
    require(journal.entry(raw) == {name: row.get(name) for name in ("size", "sha256")},
            "record bytes differ from their exact archived size/digest")
    return raw


def owned_checkpoint(observed, token, bucket, *, require_progress=True):
    journal.task_id(token)
    names = journal.names("checkpoint", token)
    require(isinstance(observed, dict) and len(observed) in {1, 4}, "checkpoint census omitted bound nodes")
    present = {}
    for node, census in observed.items():
        rows = census.get("records")
        require(census.get("readOnly") is True and census.get("family") == "checkpoint"
                and census.get("taskId") == token and census.get("directoryEnumerationUsed") is False
                and (census.get("quiescent") is True or census.get("stable") is True
                     and census.get("processBound") is True and census.get("independentReads") == 2)
                and isinstance(rows, list) and len(rows) == 16, "checkpoint has no stopped or stable process-bound census")
        seen = set()
        for row in rows:
            pair = row.get("disk"), row.get("relativePath")
            require(type(pair[0]) is int and 0 <= pair[0] < 4 and pair[1] in names and pair not in seen
                    and type(row.get("present")) is bool, "checkpoint census duplicated or escaped its exact task/disk paths")
            seen.add(pair)
            if row["present"]:
                require(pair[1] not in present, "Admin task has multiple physical checkpoint owners")
                bytes_of(row)
                present[pair[1]] = {"node": node, **row}
    require(set(present) == set(names[:2]),
            "NOT_OBSERVED: original runnable Admin checkpoint/resume pair is absent, blocked or has an unexpected digest record")
    checkpoint, resume = present[names[0]], present[names[1]]
    require((checkpoint["node"], checkpoint["disk"]) == (resume["node"], resume["disk"]),
            "Admin checkpoint and resume state have different physical owners")
    value = journal.checkpoint_value(bytes_of(checkpoint), token, bucket)
    admin = value["admin"]
    require(admin.get("set_disk_id") == "pool_0_set_0" and canonical(admin.get("options")) == canonical(OPTIONS),
            "checkpoint changed the exact admitted set or Admin options")
    outcome = admin.get("outcome") or {}
    counters = outcome.get("counters") or {}
    require(outcome.get("execution", {}).get("state") == "running"
            and outcome.get("coverage") == "partial"
            and all(type(counters.get(name)) is int and 0 <= counters[name] < 2**64
                    for name in ("processed", "healed", "unchanged", "skipped", "failed", "unknown", "attemptFailures"))
            and counters.get("overflowed") is False, "Admin checkpoint omitted truthful running outcome counters")
    require(counters["processed"] == sum(counters[name] for name in ("healed", "unchanged", "skipped", "failed"))
            and counters["unknown"] <= counters["skipped"] and counters["attemptFailures"] >= counters["failed"],
            "Admin checkpoint counters do not account for the actual durable dispositions")
    require(not require_progress or counters["processed"] > 0,
            "NOT_OBSERVED: original Admin checkpoint has no durable object progress")
    state = journal.decode_json(bytes_of(resume))
    require(type(state.get("schema_version")) is int and state["schema_version"] == 7
            and state.get("task_id") == token and state.get("task_type") == "admin_erasure_set"
            and state.get("set_disk_id") == "pool_0_set_0" and state.get("completed") is False
            and not state.get("replacement_generation") and not state.get("replacement_targets"),
            "checkpoint companion is not the original runnable Admin erasure-set resume state")
    return {"taskId": token, "bucket": bucket, "bucketIncarnationId": admin["buckets"][bucket],
            "node": checkpoint["node"], "disk": checkpoint["disk"], "checkpoint": checkpoint,
            "resume": resume, "value": value, "resumeValue": state, "originalDurableResponsibility": True}


def owned_root_admission(observed, token):
    journal.task_id(token)
    names = journal.names("root", token)
    require(isinstance(observed, dict) and len(observed) in {1, 4}, "root admission omitted bound nodes")
    present = []
    for node, census in observed.items():
        rows = census.get("records")
        require(census.get("readOnly") is True and census.get("family") == "root" and census.get("taskId") == token
                and census.get("directoryEnumerationUsed") is False
                and (census.get("quiescent") is True or census.get("stable") is True
                     and census.get("processBound") is True and census.get("independentReads") == 2)
                and isinstance(rows, list) and len(rows) == 12,
                "root admission lacks a stopped or stable process-bound exact-path census")
        seen = set()
        for row in rows:
            pair = row.get("disk"), row.get("relativePath")
            require(type(pair[0]) is int and 0 <= pair[0] < 4 and pair[1] in names
                    and pair not in seen and type(row.get("present")) is bool,
                    "root admission census duplicated or escaped its task/disk paths")
            seen.add(pair)
            if row["present"]: present.append({"node": node, **row})
    require(len(present) == 1 and present[0]["relativePath"] == names[0],
            "NOT_OBSERVED: runnable root admission is absent, duplicated or already terminal")
    owner = present[0]
    value = journal.decode_json(bytes_of(owner))
    require(type(value.get("schema")) is int and value["schema"] == 3 and value.get("task_id") == token
            and journal.same_json(value.get("heal_type"),
                {"type": "erasure_set", "buckets": [], "set_disk_id": "pool_0_set_0", "all_buckets": True})
            and value.get("bucket_incarnation_id") is None and value.get("priority") == "High"
            and type(value.get("retry_attempts")) is int and value["retry_attempts"] == 0,
            "root admission does not bind the original explicit Admin erasure-set task")
    required = {"scan_mode": 2, "remove_corrupted": False, "recreate_missing": False, "update_parity": False,
                "recursive": True, "dry_run": False, "no_lock": False, "pool_index": 0, "set_index": 0}
    options = value.get("options")
    require(isinstance(options, dict) and all(name in options and journal.same_json(options[name], expected)
            for name, expected in required.items()), "root admission changed the original Admin options/selectors")
    created = value.get("created_at")
    require(isinstance(created, dict) and set(created) == {"secs_since_epoch", "nanos_since_epoch"}
            and type(created["secs_since_epoch"]) is int and 0 <= created["secs_since_epoch"] < 2**64
            and type(created["nanos_since_epoch"]) is int and 0 <= created["nanos_since_epoch"] < 1_000_000_000,
            "root admission lost its original admitted time")
    return {**owner, "value": value, "originalDurableResponsibility": True}


def records(fleet, token, label, *, quiescent=False, family="checkpoint", nodes=None):
    result = {}
    selected = fleet.nodes if nodes is None else nodes
    require(selected and set(selected) <= set(fleet.nodes), "checkpoint observation escaped its bound nodes")
    for node in selected:
        params = {"family": family, "taskId": token}
        if not quiescent:
            params["expected"] = fleet.record(label + "-process", {"node": node, **fleet.remote(node, "process")})
        result[node] = fleet.remote(node, "journal-census" if quiescent else "journal-observe", **params)
    return fleet.record(label, result)


def admit(probe, fleet, active):
    fleet.record("Admin-checkpoint-route-contract", REFERENCE)
    result = fleet.record("Admin-erasure-set-admission", probe.admin(PATH, [], json.dumps(OPTIONS).encode()))
    require(result.get("httpStatus") == 200 and isinstance(result.get("response"), dict),
            "Admin erasure-set admission failed")
    token = journal.task_id(result["response"].get("clientToken"))
    active.append((PATH, token))
    return token


def capture_earlier(fleet, token, bucket, identifier, *, timeout=60):
    require(type(timeout) is int and 0 < timeout <= 120, "unreviewed checkpoint capture budget")
    owner, identity = freeze(fleet, token, bucket, timeout=timeout)
    node = owner["node"]
    receipt = fleet.record("Admin-checkpoint-original-capture", fleet.remote(node, "journal-capture",
        family="checkpoint", taskId=token, disk=owner["disk"], relativePath=owner["checkpoint"]["relativePath"],
        faultId=identifier, expectedBefore=journal.entry(bytes_of(owner["checkpoint"])), expected=identity))
    require(receipt.get("processBound") is True and receipt.get("originalProductBytes") is True
            and receipt.get("productMetadataMutated") is False
            and all(journal.same_json(receipt.get(name), value) for name, value in {
                "runId": fleet.run_id, "family": "checkpoint", "taskId": token, "disk": owner["disk"],
                "relativePath": owner["checkpoint"]["relativePath"], "faultId": identifier,
                "binarySha256": identity.get("binarySha256"),
                "captured": journal.entry(bytes_of(owner["checkpoint"]))}.items()),
            "checkpoint capture lost its original process, bytes or exact physical owner")
    fleet.record("checkpoint-capture-resume", fleet.remote(node, "process-signal", signal="SIGCONT", expected=identity))
    resumed = fleet.record("checkpoint-capture-resumed-process", fleet.remote(node, "process"))
    require(resumed.get("state") in {"R", "S", "D", "I"}
            and all(resumed.get(name) == identity.get(name) for name in PROCESS_FIELDS),
            "checkpoint capture did not resume its original process")
    del fleet.frozen_processes[node]
    return {**owner, "captureId": identifier, "captureReceipt": receipt}


def after_generation(current, earlier):
    require(all(journal.same_json(current.get(name), earlier.get(name))
                for name in ("taskId", "bucket", "bucketIncarnationId", "node", "disk")),
            "checkpoint advanced on a different original task, incarnation or physical owner")
    require(current["value"]["admin"]["outcome"]["counters"]["processed"] >
            earlier["value"]["admin"]["outcome"]["counters"]["processed"],
            "NOT_OBSERVED: original checkpoint has not durably advanced past its captured generation")
    # The same admission, counters, cursor and digest checks precede actual mutation.
    journal.transform(bytes_of(current["checkpoint"]), {"family": "checkpoint", "taskId": current["taskId"],
        "relativePath": current["checkpoint"]["relativePath"], "kind": "stale-generation"},
        {"bucket": current["bucket"]}, None, earlier=bytes_of(earlier["checkpoint"]))


def freeze(fleet, token, bucket, *, timeout=60, earlier=None):
    require(type(timeout) is int and 0 < timeout <= 120, "unreviewed checkpoint observation budget")
    deadline = time.monotonic() + timeout
    while True:
        owner = None
        for candidate in fleet.nodes:
            try:
                observed = records(fleet, token, "Admin-checkpoint-live", nodes=(candidate,))
                owner = owned_checkpoint(observed, token, bucket)
                if earlier is not None: after_generation(owner, earlier)
            except ValueError as exc:
                if not str(exc).startswith("NOT_OBSERVED:"): raise
                owner = None
                continue
            identity = observed[candidate].get("processIdentity")
            require(owner["node"] == candidate and observed[candidate].get("processBound") is True
                    and observed[candidate].get("stable") is True and observed[candidate].get("independentReads") == 2
                    and isinstance(identity, dict) and all(name in identity for name in PROCESS_FIELDS)
                    and type(identity.get("pid")) is int and identity["pid"] > 1,
                    "checkpoint owner has no census-bound process birth identity")
            break
        if owner is not None: break
        require(time.monotonic() < deadline, "NOT_OBSERVED: actual durable Admin checkpoint window exceeded its frozen budget")
        time.sleep(.1)
    node = owner["node"]
    # Freeze the witnessed owner before another SSH read can outlast its task.
    # The complete census below still rejects any second physical owner.
    fleet.record("checkpoint-owner-process", identity)
    require(node not in fleet.frozen_processes, "checkpoint owner is already tracked as frozen")
    fleet.frozen_processes[node] = identity
    fleet.record("checkpoint-owner-freeze", fleet.remote(node, "process-signal", signal="SIGSTOP", expected=identity))
    frozen = fleet.record("checkpoint-owner-frozen-process", fleet.remote(node, "process"))
    require(frozen.get("state") == "T" and all(frozen.get(name) == identity.get(name) for name in PROCESS_FIELDS),
            "checkpoint freeze missed the original process")
    current = owned_checkpoint(records(fleet, token, "Admin-checkpoint-frozen"), token, bucket)
    require((current["node"], current["disk"], current["bucketIncarnationId"]) ==
            (owner["node"], owner["disk"], owner["bucketIncarnationId"]), "checkpoint owner changed during freeze")
    if earlier is not None: after_generation(current, earlier)
    # Reading another live census can outlast the actual checkpoint window.
    # Validate the runnable root only after freezing its original checkpoint owner.
    frozen_root = owned_root_admission(records(fleet, token, "Admin-root-frozen", family="root"), token)
    return {**current, "rootAdmission": frozen_root}, identity


def stop_for_fault(fleet, token, bucket, frozen, identity):
    node = frozen["node"]
    require(fleet.frozen_processes.get(node) == identity, "checkpoint crash lost its tracked original process")
    exit_proof = fleet.record("checkpoint-owner-exact-crash", fleet.remote(node, "process-crash", expected=identity))
    require(exit_proof.get("actualSignal") == 9 and exit_proof.get("pidfdBound") is True
            and exit_proof.get("graceful") is False and exit_proof.get("platformEventIssued") is False,
            "checkpoint interruption has no exact process-bound SIGKILL witness")
    del fleet.frozen_processes[node]
    fleet.stop()
    current = owned_checkpoint(records(fleet, token, "Admin-checkpoint-stopped", quiescent=True), token, bucket)
    require(current["checkpoint"] == frozen["checkpoint"] and current["resume"] == frozen["resume"],
            "Admin durable responsibility changed between freeze and stopped fault boundary")
    root = owned_root_admission(records(fleet, token, "Admin-root-stopped", quiescent=True, family="root"), token)
    try:
        from .root_restart import same_owner
    except ImportError:
        from root_restart import same_owner
    # A different root coordinator may shut down gracefully when all peers
    # stop. Preserve every non-timeout field and forbid a refunded budget.
    previous = frozen["rootAdmission"]
    if all(root[name] == previous[name] for name in ("node", "disk", "relativePath", "size", "sha256")):
        continuity = {"samePhysicalOwner": True, "nonTimeoutIntentUnchanged": True,
                      "budgetNotRefunded": True, "gracefulTimeoutCheckpointClaimed": False}
    else:
        continuity = same_owner(previous, root)
    fleet.record("Admin-root-stop-continuity", continuity)
    return {**current, "rootAdmission": root, "rootContinuity": continuity}
