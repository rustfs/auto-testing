"""Restore and retain only the bounded F07 cancellation before Admin admission."""

from collections import Counter
import hashlib
import json
import re

try:
    from .healthy import physical_content
    from .heal_probe import require
except ImportError:
    from healthy import physical_content
    from heal_probe import require


CASE = "heal.checkpoint-and-mrf-corruption"
BRANCHES = ("checkpoint-truncate", "future-schema", "mrf-checksum", "stale-generation", "journal-write-failure")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def checkpoint_plan(artifact, failed_run, candidate):
    results = list(artifact.glob("*/run/*/cases/*.json"))
    require(len(results) == 1 and results[0].name == CASE + ".json", "interrupted recovery requires one unmixed case")
    raw = results[0].read_bytes()
    result = json.loads(raw)
    require(result.get("case_id") == CASE and result.get("verdict") == "RUNNING"
            and result.get("cleanup_status") == "PENDING" and not result.get("provenance"),
            "interrupted recovery requires the unchanged unfinished result")
    root = results[0].parents[3]
    require(json.loads((root / "candidate.json").read_text()) == candidate, "interrupted candidate differs")
    evidence = results[0].parents[1] / "evidence" / CASE
    require(not (evidence / "healthy-summary.json").exists(), "interrupted case already has an aggregate result")
    finished = {(fixture, topology, branch) for fixture in ("DB", "DV") for topology in ("H1", "H2")
                for branch in BRANCHES} - {("DV", "H2", "stale-generation"), ("DV", "H2", "journal-write-failure")}
    observed = list(evidence.glob("*/*/*/*branch-result.json"))
    require(len(observed) == len(finished) == 18
            and {tuple(path.parent.relative_to(evidence).parts) for path in observed} == finished,
            "interrupted recovery cannot omit another unfinished branch")
    for path in observed:
        row = json.loads(path.read_text())
        fixture, topology, branch = path.parent.relative_to(evidence).parts
        require(row.get("fixture") == fixture and row.get("topology") == topology and row.get("branch") == branch
                and row.get("verdict") in {"PASS", "FAIL"} and row.get("cleanup") == "PASS",
                "prior interrupted branch was not finalized")
        cleanup = [json.loads(p.read_text()) for p in path.parent.glob("*-cleanup.json")]
        nodes = {"vm000"} if topology == "H1" else {f"vm00{i}" for i in range(4)}
        require(len(cleanup) == len(nodes) and {r.get("node") for r in cleanup} == nodes
                and all(r.get("cleanup") == "PASS" and r.get("unitStopped") is True and r.get("rootsRemoved") is True
                        for r in cleanup), "prior branch lacks its full stopped and removed fleet")
        recovery = [p for p in path.parent.glob("*-recovery.json") if re.fullmatch(r"\d{5}-recovery.json", p.name)]
        require(len(recovery) == 1 and json.loads(recovery[0].read_text()) == {"cleanup": "PASS", "errors": []},
                "prior branch has an unresolved recovery")
    expected_folders = finished | {("DV", "H2", "stale-generation")}
    require({tuple(path.relative_to(evidence).parts) for path in evidence.glob("*/*/*") if path.is_dir()} == expected_folders,
            "interrupted evidence contains another unfinished branch")
    folder = evidence / "DV/H2/stale-generation"
    require(not (evidence / "DV/H2/journal-write-failure").exists(), "later interrupted branch already started")
    require(all(path.name == "fixture" or re.fullmatch(r"[0-9]{5}-.+\.json", path.name)
                for path in folder.iterdir()), "interrupted folder contains an unknown operation")
    require(all(re.fullmatch(r"[0-9]{6}-(request|ack)\.json", path.name)
                for path in (folder / "fixture").iterdir()), "interrupted fixture contains an unknown operation")
    files = sorted(folder.glob("*.json"))
    require(len(files) == 97 and all(path.name.startswith(f"{i:05d}-") for i, path in enumerate(files, 1)),
            "interrupted recovery requires its exact pre-admission boundary")
    labels = Counter(path.name[6:-5] for path in files)
    require(labels == Counter({"binding": 1, "candidate-bound-inspector": 1, "candidate-terminal-options-contract": 1,
        "preflight": 1, "integrity-fleet-attestation": 1, "reserved": 4, "inspector-upload": 4, "started": 2,
        "deployed-topology": 1, "topology-proof": 1, "ACK-bound-complete-S3": 1, "complete-ACK-manifest": 1,
        "task-exit-background": 4, "quiescent": 1, "healthy-physical-baseline": 1,
        "baseline-full-cohort-bitrot": 64, "missing-original-shard": 4, "actual-missing-shard": 4})
        and files[-1].name == "00097-started.json", "interrupted scene has a task, durable fault or unknown action")
    def one(label):
        return json.loads(next(p for p in files if p.name[6:-5] == label).read_text())
    binding = one("binding")
    run_id, bucket = binding.get("runId", ""), binding.get("bucketPrefix", "")
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h2", run_id)
            and re.fullmatch(r"rv-heal-[0-9a-f]{16}", bucket)
            and binding.get("candidateCommit") == candidate["source_sha"]
            and binding.get("candidatePackageSha256") == candidate["package_sha256"]
            and re.fullmatch(r"[0-9a-f]{64}", binding.get("candidateBinarySha256", ""))
            and re.fullmatch(r"[0-9a-f]{40}", binding.get("harnessCommit", "")), "interrupted binding differs")
    nodes = [f"vm00{i}" for i in range(4)]
    owners = {}
    for path in files:
        if path.name[6:-5] != "reserved": continue
        receipt = json.loads(path.read_text()); owner = receipt.get("owner") or {}
        node = receipt.get("node")
        require(receipt.get("reserved") is True and node in nodes and node not in owners
                and owner.get("runId") == run_id and owner.get("bucket") == bucket
                and owner.get("binarySha256") == binding["candidateBinarySha256"]
                and owner.get("unit") == "rustfs-validation-heal-" + run_id + ".service"
                and owner.get("runtimeProfile") == "root-restart" and len(owner.get("mounts", [])) == 4,
                "interrupted owner receipt differs")
        owners[node] = owner
    require(set(owners) == set(nodes), "interrupted fleet owner is incomplete")
    requests, acks = sorted((folder / "fixture").glob("*-request.json")), sorted((folder / "fixture").glob("*-ack.json"))
    require(len(requests) == len(acks) == 282, "interrupted fixture has unresolved SDK operations")
    operations = Counter()
    for seq, (request_path, ack_path) in enumerate(zip(requests, acks, strict=True), 1):
        request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
        observation = ack.get("observation") or {}
        operation, params = request.get("operation"), request.get("params") or {}
        require(type(request.get("sequence")) is int and type(ack.get("sequence")) is int
                and request["sequence"] == ack["sequence"] == seq
                and request.get("runId") == ack.get("runId") == run_id
                and ack.get("requestFile") == request_path.name
                and ack.get("requestSha256") == hashlib.sha256(request_path.read_bytes()).hexdigest()
                and ack.get("operationCheck") == "PASS" and observation.get("operation") == operation
                and observation.get("params") == params and params.get("Bucket") == bucket
                and observation.get("acknowledged") is True and observation.get("clientCallInvoked") is True
                and observation.get("resultUnknown") is False
                and operation in {"create_bucket", "put_bucket_versioning", "put_object", "delete_object"},
                "interrupted SDK operation is unacknowledged, changed or outside its fixture")
        if operation in {"put_object", "delete_object"}:
            require(re.fullmatch(r"objects/[0-9]{4}\.bin", params.get("Key", "")) and "VersionId" not in params,
                    "interrupted SDK object operation differs")
        operations[operation] += 1
    require(operations == Counter(create_bucket=1, put_bucket_versioning=1, put_object=240, delete_object=40),
            "interrupted fixture operation census differs")
    before = one("healthy-physical-baseline")
    def indexed(scene):
        require(set(scene) == set(nodes), "interrupted physical fleet differs")
        out = {}
        for node, value in scene.items():
            require(value.get("quiescent") is True and type(value.get("pid")) is int and value["pid"] == 0
                    and value.get("binarySha256") == binding["candidateBinarySha256"]
                    and len(value.get("formats", [])) == 4, "interrupted scene is not the stopped exact candidate")
            rows = value["files"]
            out[node] = {(entry["disk"], entry["path"]): entry for entry in rows}
            require(len(out[node]) == len(rows), "interrupted physical scene has duplicate files")
        return out
    previous = before; left = indexed(previous); faults = []
    for index, receipt_path in enumerate(sorted(folder.glob("*-missing-original-shard.json"))):
        receipt = json.loads(receipt_path.read_text())
        number = int(receipt_path.name[:5])
        after_path = folder / f"{number+1:05d}-actual-missing-shard.json"
        require(after_path in files, "interrupted injection has no immediate physical snapshot")
        after = json.loads(after_path.read_text()); right = indexed(after)
        removed = [(node, *key) for node in nodes for key in set(left[node]) - set(right[node])]
        require(len(removed) == 1, "interrupted injection is not one exact missing part")
        node, disk, relative = removed[0]; entry = left[node][(disk, relative)]
        require(type(disk) is int and 0 <= disk < 4
                and re.fullmatch(re.escape(bucket) + r"/objects/[0-9]{4}\.bin/[0-9a-f-]{36}/part\.1", relative)
                and receipt.get("schemaVersion") == 1 and receipt.get("runId") == run_id
                and receipt.get("faultId") == f"F07-shard-{index}" and receipt.get("relativePath") == relative
                and receipt.get("kind") == "missing" and receipt.get("fault") is None and receipt.get("offset") is None
                and receipt.get("before") == {key: entry[key] for key in ("size", "sha256")}
                and receipt.get("backup") == f"F07-shard-{index}.original"
                and receipt.get("backend") == "logical-file-fault" and receipt.get("requiresQuiescentService") is True,
                "interrupted missing-part receipt differs")
        del left[node][(disk, relative)]
        require(left == right and all(previous[n]["formats"] == after[n]["formats"] for n in nodes),
                "interrupted injection changed other files or disk formats")
        faults.append({"node": node, "disk": disk, "faultId": receipt["faultId"], "receipt": receipt})
        previous, left = after, right
    require(len(faults) == 4, "interrupted fault census differs")
    started = json.loads(files[-1].read_text())
    require(set(started) == set(nodes) and all(value == {"unit": owners[n]["unit"], "started": True}
            for n, value in started.items()), "interrupted final service start differs")
    return [{"kind": "interrupted-checkpoint-before-admission", "runId": run_id, "nodes": nodes,
        "bucket": bucket, "binarySha256": binding["candidateBinarySha256"], "owners": owners,
        "before": before, "failure": previous, "faults": faults,
        "originalCaseSha256": hashlib.sha256(raw).hexdigest(), "originalHarnessCommit": binding["harnessCommit"]}]


def restore_and_isolate(fleet, plan, states, scene_matches):
    """Preserve all backups and bytes, including when recovery is interrupted."""
    require(len(states) == 4 and {item["node"] for item in states} == set(plan["nodes"]), "interrupted current fleet differs")
    active = []
    for item in states:
        retained = item.get("retained") or {}
        if retained.get("state") or any(retained.get("roots", [])):
            continue
        require(item["state"]["exists"] and len(item["roots"]) == 4 and all(root["exists"] for root in item["roots"]),
                "interrupted fixture lost an original root; retain remaining scene")
        active.append(item["node"])
    for node in active:
        fleet.record("interrupted-recovery-stop", {"node": node, **fleet.remote(node, "stop")})
    current = {node: fleet.remote(node, "snapshot", quiescent=True) for node in active}
    fleet.record("interrupted-current-physical", current)
    if active:
        subset = {**plan, "before": {n: plan["before"][n] for n in active},
                  "failure": {n: plan["failure"][n] for n in active}}
        require(all(value.get("quiescent") is True and type(value.get("pid")) is int and value["pid"] == 0
                    and value.get("binarySha256") == plan["binarySha256"] for value in current.values())
                and scene_matches(current, subset), "interrupted current scene drifted; no backup restoration performed")
        for fault in plan["faults"]:
            if fault["node"] in active:
                proof = fleet.record("interrupted-original-shard-rescue", {"node": fault["node"],
                    **fleet.remote(fault["node"], "recover-fault", disk=fault["disk"], faultId=fault["faultId"],
                                   expectedReceiptSha256=digest({key: value for key, value in fault["receipt"].items()
                                                                if key != "witnessedAt"}))})
                require(proof.get("runId") == plan["runId"] and proof.get("faultId") == fault["faultId"]
                        and proof.get("physicalRepairPassAllowed") is False, "interrupted rescue receipt differs")
        restored = {node: fleet.remote(node, "snapshot", quiescent=True) for node in active}
        fleet.record("interrupted-original-business-restored", restored)
        require(physical_content(restored) == {n: physical_content(plan["before"])[n] for n in active},
                "interrupted fixture did not restore its full original business scene")
    audits = {}
    def params(node):
        return {"expectedOwner": plan["owners"][node],
                "expectedOriginalSceneSha256": digest(physical_content(plan["before"])[node])}
    for node in plan["nodes"]:
        audits[node] = fleet.record("interrupted-archive-preflight", {"node": node,
            **fleet.remote(node, "retained-scene-audit", **params(node))})
        require(audits[node].get("inspectionOnly") is True and audits[node].get("mutated") is False
                and audits[node].get("baseline", {}).get("originalSceneSha256") == params(node)["expectedOriginalSceneSha256"],
                "interrupted restored archive preflight differs")
    for node in plan["nodes"]:
        proof = fleet.record("interrupted-archive-isolation", {"node": node,
            **fleet.remote(node, "retained-scene-isolate", **params(node), expectedBaseline=audits[node]["baseline"])})
        require(proof.get("activeNamespaceIsolated") is True and proof.get("canonicalPassClaimed") is False
                and proof.get("baseline") == audits[node]["baseline"], "interrupted archive isolation differs")
    for node in plan["nodes"]:
        proof = fleet.record("interrupted-archive-post-audit", {"node": node,
            **fleet.remote(node, "retained-scene-audit", **params(node))})
        require(proof.get("activeNamespaceIsolated") is True and proof.get("inspectionOnly") is True
                and proof.get("mutated") is False and proof.get("baseline") == audits[node]["baseline"],
                "interrupted archive changed after isolation")
    return {"runId": plan["runId"], "cleanup": "ISOLATED", "activeNamespaceIsolated": True,
            "originalBusinessFixtureRestored": True, "originalSceneRestored": False,
            "canonicalPassClaimed": False, "originalVerdictRewritten": False,
            "originalCaseSha256": plan["originalCaseSha256"]}
