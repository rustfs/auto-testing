"""F07: corrupt actual committed MRF CRCs and retain original responsibility."""

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import os
import secrets
import time

try:
    from . import journal_fault as journal, mrf_records as records, mrf_replay as mrf
    from .heal_probe import Probe, compare_inventories, require
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .scanner_repair import JournalWindow
    from .shards import map_target, run_task, verify_cohort
    from .stop import idle_fleet
    from .versions import validate_version_receipts, verify_quorum_contract
except ImportError:
    import journal_fault as journal, mrf_records as records, mrf_replay as mrf
    from heal_probe import Probe, compare_inventories, require
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from scanner_repair import JournalWindow
    from shards import map_target, run_task, verify_cohort
    from stop import idle_fleet
    from versions import validate_version_receipts, verify_quorum_contract


CASE_ID = "heal.checkpoint-and-mrf-corruption"
SIZE = 5 * 1024**2 + 123


def indexed(observed):
    require(observed.get("readOnly") is True and observed.get("stable") is True,
            "F07 MRF evidence is not an independent stable read")
    rows = observed.get("records")
    expected = {(disk, path) for disk in range(4) for path in records.FILES}
    require(isinstance(rows, list) and len(rows) == len(expected), "F07 MRF census is incomplete")
    result = {}
    for row in rows:
        pair = row.get("disk"), row.get("relativePath")
        require(type(pair[0]) is int and pair in expected and pair not in result and type(row.get("present")) is bool,
                "F07 MRF census duplicated or escaped its exact physical scope")
        if row["present"]:
            raw = base64.b64decode(row["rawBase64"], validate=True)
            require(journal.entry(raw) == {key: row.get(key) for key in ("size", "sha256")}
                    and len(raw) <= records.FILES[pair[1]], "F07 MRF evidence differs from original bytes")
            result[pair] = raw
        else:
            require(not any(key in row for key in ("size", "sha256", "rawBase64")), "F07 absent MRF file has invented bytes")
            result[pair] = None
    return result


def corruption_targets(observed, bucket):
    """Cover every committed replay source, including readable older slots."""
    selected = records.checkpoint(observed)
    require(selected.get("present") is True, "F07 MRF owner has no committed replay source")
    latest_identity = journal.active_mrf_identity(selected, bucket)
    entries, targets = indexed(observed), []
    for disk in range(4):
        for slot in (0, 1):
            manifest = entries[disk, f".rustfs.sys/.heal-mrf-commit.{slot}.bin"]
            if manifest is None:
                continue
            path = f".rustfs.sys/.heal-mrf-snapshot.{slot}.bin"
            raw = entries[disk, path]
            binding = records.commit(manifest, raw)
            exact = records.checkpoint(observed, replica=(disk, slot))
            identity = journal.active_mrf_identity(exact, bucket)
            require(identity["owner"] == latest_identity["owner"]
                    and identity["bucketIncarnationId"] == latest_identity["bucketIncarnationId"],
                    "F07 MRF replay sources mixed process owners or bucket incarnations")
            targets.append({"disk": disk, "relativePath": path, **binding,
                            "mrfIdentity": identity, "before": journal.entry(raw)})
    require(targets, "F07 MRF corruption has no bound replay source")
    return targets


def retained_corruption(original, current, receipts, bucket):
    require(set(original) == set(current) and len(original) in {1, 4}, "F07 MRF evidence lost a bound physical node")
    expected, changed = {node: indexed(value) for node, value in original.items()}, set()
    require(receipts, "F07 MRF corruption has no actual original payload mutation")
    for receipt in receipts:
        node, disk, path = receipt.get("node"), receipt.get("disk"), receipt.get("relativePath")
        identity = node, disk, path
        require(identity not in changed and node in expected and type(disk) is int and (disk, path) in expected[node]
                and path in {f".rustfs.sys/.heal-mrf-snapshot.{slot}.bin" for slot in (0, 1)},
                "F07 MRF mutation changed scope or duplicated a payload owner")
        raw = expected[node][disk, path]
        require(raw is not None and records.decode_journal(raw) and receipt.get("family") == "mrf"
                and receipt.get("taskId") is None and receipt.get("kind") == "checksum" and receipt.get("applied") is True
                and receipt.get("originalBackupRetained") is True and receipt.get("productRepairClaimed") is False
                and journal.same_json(receipt.get("before"), journal.entry(raw)), "F07 MRF mutation lost its original payload binding")
        corrupt = journal.transform(raw, receipt, {"bucket": bucket}, records)
        require(journal.same_json(receipt.get("injected"), journal.entry(corrupt)), "F07 MRF corruption changed bytes outside the actual CRC")
        expected[node][disk, path] = corrupt
        changed.add(identity)
    require(all(expected[node] == indexed(value) for node, value in current.items()),
            "F07 product deleted, rewrote or rebound original corrupt MRF records, manifests or lifecycle responsibility")
    return {"originalCorruptRecordsRetained": True, "commitsAndLifecycleUnchanged": True,
            "corruptedPayloads": len(changed), "originalResponsibilityRetired": False,
            "safeRescanIsOriginalMrfCompletion": False, "backupRescueUsed": False}


def diagnostics(events, mutated_nodes):
    found = {}
    for node in mutated_nodes:
        matching = [event for event in events.get(node, [])
            if event.get("target") == "rustfs::heal::mrf"
            and event.get("message") == "MRF committed replay checkpoint could not be inspected"
            and isinstance(event.get("error"), str)
            and any(word in event["error"].lower() for word in ("corrupt", "invalid", "integrity"))]
        require(matching, "NOT_OBSERVED: original MRF corruption has no candidate diagnostic on " + node)
        found[node] = matching
    require(found, "F07 MRF diagnostic has no bound mutated process")
    return found


def observe(fleet, label):
    return fleet.record(label, {node: fleet.remote(node, "mrf-records", quiescent=True) for node in fleet.nodes})


def validate_member_denial(value, disk):
    control = value.get("systemMetadataControl") or {}
    require(value.get("scope") == "bucket" and type(value.get("disk")) is int and value["disk"] == disk
            and value.get("readProbe", {}).get("errno") == 13 and value.get("errno") == 13
            and value.get("dacBypassRemoved") is True and value.get("systemMetadataAccessible") is True
            and control.get("sameServiceIdentity") is True
            and all(type(control.get(kind, {}).get("errno")) is int and control[kind]["errno"] == 0
                    for kind in ("read", "write")),
            "F07 needs exact business read/write denial with accessible candidate system metadata")


def run(args, topology, nodes, fixture, candidate):
    require(topology in {"H1", "H2"} and fixture in {"DB", "DV"}, "F07 MRF source fixture changed")
    directory = args.evidence / fixture / topology / "mrf-checksum"
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="mrf-crash")
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    fleet.snapshot_buckets = [bucket]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=45)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
        "maxInventoryObjects": 128, "maxInventoryBytes": 256 * 1024**2}
    fleet.record("binding", binding)
    tool = fleet.record("candidate-bound-inspector", verify_inspector(args.tool_directory, candidate))
    verify_quorum_contract(tool, candidate)
    fleet.record("candidate-MRF-source-contract", records.verify_reference(candidate))
    probe, active, pending = Probe(binding), [], None
    result = {"topology": topology, "fixture": fixture, "branch": "mrf-checksum", "verdict": "RUNNING",
              "cleanup": "PENDING", "canonicalCasePassClaimed": False}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        fleet.record("actual-topology", validate_topology(fleet.snapshot("deployed-topology"), topology))
        recorder = Recorder(probe, directory / "fixture")
        pending = {"operation": "seed-original-MRF-fixture"}
        expected = seed(recorder, bucket, fixture, 3, [SIZE])
        verify_seed(expected, probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        pending = None
        initial = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active, option_overrides={"recursive": True})
        incarnation = validate_version_receipts(initial, bucket, expected, 2)["bucketIncarnationId"]
        idle_fleet(fleet, binding)
        fleet.stop()
        before = fleet.snapshot("healthy-before-partial-write-gate", quiescent=True)
        entry = next(item for item in expected if not item["deleteMarker"])
        target = map_target(before, bucket, entry["key"], "data", 1, entry["versionId"], latest_required=False)
        shard = next(row for row in reversed(target["cohort"]) if row["node"] == nodes[-1])
        fleet.drive_gates.add(shard["node"])
        fleet.record("actual-partial-write-gate", fleet.remote(shard["node"], "drive-gate", disk=shard["disk"],
            mode="permission-denied", scope="bucket"))
        fleet.start()
        denial = fleet.record("actual-member-denial", fleet.remote(shard["node"], "drive-gate-probe"))
        validate_member_denial(denial, shard["disk"])
        earliest = int(time.time() * 1000) - 5000
        pending = {"operation": "partial-write-durable-ACKs"}
        expected, obligations, _ = mrf.acknowledge(recorder, bucket, "offline-put", expected, fixture=fixture)
        baseline = fleet.record("complete-degraded-ACK-S3", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        verify_seed(expected, baseline)
        pending = None
        deadline = time.monotonic() + mrf.PERSIST_SECONDS
        while True:
            try:
                bindings = mrf.pending_bindings(mrf.read_checkpoints(fleet, nodes, "original-committed-MRF"), obligations, earliest)
                break
            except ValueError as exc:
                if not str(exc).startswith("NOT_OBSERVED:"): raise
                require(time.monotonic() < deadline, "F07 ACK-bound durable MRF window was not observed")
                time.sleep(.5)
        require(all(owner["bucketIncarnationId"] == incarnation for owners in bindings.values() for owner in owners),
                "F07 MRF ACK responsibility changed the original incarnation")
        fleet.stop()
        original = observe(fleet, "original-stopped-MRF-records")
        stopped = {node: records.checkpoint(value) for node, value in original.items()}
        bindings = mrf.pending_bindings(stopped, obligations, earliest)
        fleet.record("original-durable-responsibilities", {"obligations": obligations,
            "bindings": [{"identity": list(key), "owners": value} for key, value in bindings.items()]})
        authorities = fleet.snapshot("original-partial-write-physical", quiescent=True)
        mutation_plan = {node: corruption_targets(observed, bucket) for node, observed in original.items()
                         if any(owner["node"] == node for owners in bindings.values() for owner in owners)}
        require(mutation_plan, "F07 MRF fault plan has no original responsibility owner")
        fleet.record("all-original-committed-MRF-corruption-targets", mutation_plan)
        receipts = []
        # A healthy replica or older committed slot can still replay. Corrupt
        # every owned source while retaining all manifests and lifecycle bytes.
        for node, targets in mutation_plan.items():
            for target in targets:
                disk, path = target["disk"], target["relativePath"]
                params = {"family": "mrf", "taskId": None, "disk": disk, "relativePath": path,
                    "faultId": f"F07-mrf-checksum-{nodes.index(node)}-{disk}-{path[-5]}", "kind": "checksum",
                    "expectedBefore": target["before"], "mrfIdentity": target["mrfIdentity"]}
                fleet.journal_faults.append((node, params.copy()))
                receipts.append(fleet.record("actual-original-MRF-CRC-injection", {"node": node,
                    **fleet.remote(node, "journal-apply", **params)}))
        retained_corruption(original, observe(fleet, "injected-stopped-MRF-records"), receipts, bucket)
        require({(row.get("node"), row.get("disk"), row.get("relativePath")) for row in receipts} ==
                {(node, row["disk"], row["relativePath"]) for node, targets in mutation_plan.items() for row in targets},
                "F07 MRF corruption omitted an original readable replay source")
        window = JournalWindow(fleet, "F07-corrupt-MRF-replay", event_targets={"rustfs::heal::mrf"})
        # Only business data is denied; disk initialization can still read
        # system metadata. Keep the same gate active through startup so MRF
        # replay cannot repair the target before the readiness observation.
        gate = fleet.record("stopped-original-business-gate", fleet.remote(shard["node"], "gate-state"))
        require(gate.get("scope") == "bucket" and gate.get("mode") == "permission-denied"
                and gate.get("phase") == "active" and type(gate.get("disk")) is int and gate["disk"] == shard["disk"],
                "F07 restart gate changed its original denied member")
        fleet.start()
        denied = fleet.record("restarted-original-member-denial", fleet.remote(shard["node"], "drive-gate-probe"))
        validate_member_denial(denied, shard["disk"])
        deadline = time.monotonic() + 60
        while True:
            try:
                result["originalCorruptionDiagnostics"] = diagnostics(window.capture(), {row["node"] for row in receipts})
                break
            except ValueError as exc:
                if not str(exc).startswith("NOT_OBSERVED:"): raise
                require(time.monotonic() < deadline, str(exc))
                time.sleep(.5)
        fleet.stop()
        result["originalResponsibilityBeforeRescan"] = retained_corruption(
            original, observe(fleet, "retained-corrupt-MRF-before-rescan"), receipts, bucket)
        require(physical_content(authorities) == physical_content(fleet.snapshot("before-separate-rescan", quiescent=True)),
                "F07 corrupt MRF replay silently changed original object responsibility")
        fleet.record("release-original-member-gate", fleet.remote(shard["node"], "drive-gate", mode="clear"))
        fleet.drive_gates.remove(shard["node"])
        fleet.start()
        rescanned = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active, option_overrides={"recursive": True})
        result["separateRescan"] = validate_version_receipts(rescanned, bucket, expected, 2,
            required_repairs={(item["key"], item["versionId"]) for item in obligations},
            metadata_repairs={item["key"] for item in obligations})
        require(result["separateRescan"]["bucketIncarnationId"] == incarnation, "F07 rescan rebound the original incarnation")
        idle_fleet(fleet, binding)
        fleet.stop()
        repaired = fleet.snapshot("complete-rescan-physical-before-business-reads", quiescent=True)
        cohorts = mrf.replica_census(repaired, bucket, expected, authorities, topology=topology)
        result["fullShardIntegrity"] = [verify_cohort(fleet, cohort, "F07-full-shard-proof") for cohort in cohorts]
        retained_corruption(original, observe(fleet, "retained-corrupt-MRF-after-rescan"), receipts, bucket)
        fleet.start()
        for endpoint in binding["endpoints"]:
            inventory = fleet.record("all-peer-S3", {"endpoint": endpoint,
                **Probe(binding, endpoint).inventory(bucket, unversioned_current_tags=fixture == "DB")})
            verify_seed(expected, inventory); compare_inventories(baseline, inventory)
        fleet.stop()
        require(physical_content(repaired) == physical_content(fleet.snapshot("after-business-reads", quiescent=True)),
                "F07 business reads repaired work not proved by the separate rescan")
        result["originalResponsibilityAfterReads"] = retained_corruption(original, observe(fleet, "retained-corrupt-MRF-after-reads"), receipts, bucket)
        result.update(verdict="PASS", originalMrfRepairClaimed=False, completePhysicalAndS3ManifestsVerified=True)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        try:
            fleet.stop(); observe(fleet, "original-MRF-failure-scene")
            fleet.snapshot("original-physical-failure-scene", quiescent=True)
        except Exception as error: result["failureEvidenceError"] = str(error)
    finally:
        clean = fleet.cleanup(preserve_namespaces=pending is not None)
        result["cleanup"] = "PASS" if clean and pending is None else "FAIL"
        if pending is not None: result["unresolvedMutation"] = pending
        fleet.record("MRF-corruption-branch-result", result)
    return result
