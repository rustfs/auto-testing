#!/usr/bin/env python3
"""Audit and finalize only explicitly supported, exactly archived failures."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re

try:
    from .healthy import Fleet, physical_content
    from .heal_probe import require, save
    from .interrupted_recovery import checkpoint_plan, restore_and_isolate
    from .outcome_recovery import acknowledged_grace_plan, isolate_acknowledged_grace, interrupted_opaque_plan, acknowledged_legacy_plan
    from .network_retention import archived_plan as network_retention_plan, isolate as isolate_network_scene
    from .race_retention import archived_plan as race_retention_plan, isolate as isolate_race_scene
except ImportError:
    from healthy import Fleet, physical_content
    from heal_probe import require, save
    from interrupted_recovery import checkpoint_plan, restore_and_isolate
    from outcome_recovery import acknowledged_grace_plan, isolate_acknowledged_grace, interrupted_opaque_plan, acknowledged_legacy_plan
    from network_retention import archived_plan as network_retention_plan, isolate as isolate_network_scene
    from race_retention import archived_plan as race_retention_plan, isolate as isolate_race_scene


def quota_recovery_plan(artifact, failed_run, candidate):
    """A private H1 quota failure before workload/Heal admits no unknown writes."""
    case = "heal.enospc-inodes-and-quota"
    results = list(artifact.glob(f"*/run/{case}/cases/{case}.json"))
    require(len(results) == 1, "quota recovery requires one exact I02 result")
    result = json.loads(results[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL" and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"], "quota failed-result candidate or cleanup differs")
    evidence = results[0].parents[1] / f"evidence/{case}"
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    retained = [row for row in summary["results"] if row.get("cleanup") != "PASS"]
    require(summary.get("cleanup") == "FAIL" and len(retained) == 1, "quota recovery cannot omit another retained instance")
    row = retained[0]
    require(row.get("topology") == "H1" and row.get("branch") == "bucket-quota" and row.get("verdict") == "FAIL"
            and row.get("failure", {}).get("message") == "peer did not observe the exact acknowledged quota limit"
            and set(row) <= {"topology", "branch", "verdict", "cleanup", "failure", "recoveryError"},
            "quota recovery only supports failure before any negative PUT or Heal task")
    folder = evidence / "H1/bucket-quota"
    def one(suffix):
        matches = list(folder.glob("*-" + suffix + ".json"))
        require(len(matches) == 1, "quota recovery lacks one exact " + suffix)
        return json.loads(matches[0].read_text())
    binding, before, failure = one("binding"), one("original-complete-physical"), one("physical-original-failure-before-any-rescue")
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h1", binding["runId"]) is not None
            and binding["candidateBinarySha256"] == provenance["binary_sha256"]
            and binding["harnessCommit"] == provenance["testing_commit"] and len(before) == 1
            and set(before) == set(failure), "quota archived run/candidate/host differs")
    node = next(iter(before))
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) is not None, "quota archived node is invalid")
    live = one("manual-source-isolation-before-fault")
    require(all(value.get("quiescent") is True for scene in (before, failure) for value in scene.values())
            and set(live) == {node} and (live[node].get("backgroundConfig") or {}).get("RUSTFS_SCANNER_ENABLED") == "false"
            and live[node].get("binarySha256") == provenance["binary_sha256"],
            "quota archive has no stopped fixture and actual isolated candidate runtime")
    quota = one("owned-bucket-quota-PUT")
    read = one("all-peer-owned-quota-GET")
    require(quota.get("path") == read.get("path") == "/rustfs/admin/v3/quota/" + binding["bucketPrefix"]
            and quota.get("method") == "PUT" and quota.get("httpStatus") == 200
            and quota.get("response", {}).get("quota") == 1024**2 and quota["response"].get("quota_type") == "HARD"
            and read.get("method") == "GET" and read.get("httpStatus") == 503
            and "authoritative bucket usage is not available yet" in str(read.get("response")),
            "quota recovery lacks its exact acknowledged limit and usage-unavailable failure")
    require(not (folder / "negative-PUT").exists() and not (folder / "capacity-released-PUT").exists(),
            "quota recovery cannot finalize an invoked or uncertain workload")
    receipts = [json.loads(path.read_text()) for path in folder.glob("*-exact-missing-part.json")]
    require(len(receipts) == 2 and len({item.get("faultId") for item in receipts}) == 2,
            "quota rescue requires both original fault receipts")
    left = {(item["disk"], item["path"]): item for item in before[node]["files"]}
    right = {(item["disk"], item["path"]): item for item in failure[node]["files"]}
    require(before[node]["formats"] == failure[node]["formats"] and len(left) == len(before[node]["files"])
            and len(right) == len(failure[node]["files"]), "quota archived disk identity or file census differs")
    faults, removed = [], set()
    for receipt in receipts:
        matches = [key for key, entry in left.items() if entry["path"] == receipt.get("relativePath")
                   and key not in right and receipt.get("before") == {"sha256": entry["sha256"], "size": entry["size"]}]
        require(receipt.get("runId") == binding["runId"] and receipt.get("kind") == "missing"
                and re.fullmatch(r"I02-[a-z0-9-]{1,48}", receipt.get("faultId", "")) is not None and len(matches) == 1
                and matches[0] not in removed, "quota fault receipt has a wrong/duplicate run, shard or backup identity")
        removed.add(matches[0])
        faults.append({"disk": matches[0][0], "faultId": receipt["faultId"]})
    require({key: value for key, value in left.items() if key not in removed} == right,
            "quota archived failure changed other content or versions; preserve namespace")
    return [{"kind": "owned-quota", "runId": binding["runId"], "nodes": [node], "node": node,
             "bucket": binding["bucketPrefix"], "binarySha256": provenance["binary_sha256"],
             "before": before, "failure": failure, "faults": faults,
             "expectedSceneSha256": hashlib.sha256(json.dumps(physical_content(before)[node], sort_keys=True,
                                                               separators=(",", ":")).encode()).hexdigest()}]


def network_recovery_plan(artifact, failed_run, candidate):
    """Select exact I03 expiry recovery or unresolved-load scene isolation."""
    case = "heal.network-partition-and-lock-quorum"
    if list(artifact.glob(f"*/run/{case}/evidence/{case}/H2/*/*-network-retention-intent.json")):
        return network_retention_plan(artifact, failed_run, candidate)
    results = list(artifact.glob(f"*/run/{case}/cases/{case}.json"))
    require(len(results) == 1, "network recovery requires one exact I03 result")
    result = json.loads(results[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL"
            and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"],
            "network failed-result candidate or cleanup differs")
    evidence = results[0].parents[1] / f"evidence/{case}/H2/minority-partition"
    summary = json.loads((results[0].parents[1] / f"evidence/{case}/healthy-summary.json").read_text())
    rows = [row for row in summary.get("results", []) if row.get("cleanup") != "PASS"]
    require(summary.get("cleanup") == "FAIL" and len(rows) == 1
            and rows[0].get("branch") == "minority-partition"
            and "below-quorum GET returned data" in rows[0].get("failure", {}).get("message", "")
            and "network-release failed" in rows[0].get("recoveryError", "")
            and "not loaded" in rows[0].get("recoveryError", ""),
            "network recovery only supports the expired I03 gate boundary")
    bindings = list(evidence.glob("*-binding.json"))
    intents = sorted(evidence.glob("*-network-expiry-and-rule-intent.json"))
    activations = sorted(evidence.glob("*-atomic-owned-table-activation.json"))
    require(len(bindings) == 1 and len(intents) == len(activations) == 4,
            "network recovery lacks four exact gate intents and activations")
    binding = json.loads(bindings[0].read_text())
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h2", binding.get("runId", ""))
            and binding.get("candidateBinarySha256") == provenance.get("binary_sha256")
            and binding.get("candidateCommit") == candidate["source_sha"],
            "network binding is not the original candidate-bound H2 run")
    intent_values = [json.loads(path.read_text()) for path in intents]
    activation_values = [json.loads(path.read_text()) for path in activations]
    nodes = [value.get("node") for value in intent_values]
    require(set(nodes) == {"vm000", "vm001", "vm002", "vm003"}
            and all(value.get("runId") == binding["runId"] and value.get("phase") == "armed"
                    and value.get("binarySha256") == provenance.get("binary_sha256")
                    and re.fullmatch(r"rv_i03_[0-9a-f]{24}", value.get("table", ""))
                    and re.fullmatch(r"rv-i03-expiry-[0-9a-f]{24}", value.get("timer", ""))
                    for value in intent_values)
            and all(value.get("state") == "ACTIVE" and value.get("node") in set(nodes)
                    and value.get("table") in {intent["table"] for intent in intent_values}
                    for value in activation_values),
            "network gate evidence is not a complete four-node owned expiry boundary")
    return [{"kind": "network-expiry-recovery", "runId": binding["runId"], "nodes": nodes,
             "node": nodes[0], "binarySha256": provenance["binary_sha256"],
             "intents": intent_values, "activations": activation_values}]


def recovery_plan(artifact, failed_run, candidate):
    require(type(failed_run) is int and failed_run > 0, "exact failed workflow run required")
    if (list(artifact.glob("*/run/heal.race-unversioned-overwrite/cases/*.json"))
            or list(artifact.glob("*/run/heal.race-versioned-put/cases/*.json"))
            or list(artifact.glob("*/run/heal.race-markers-and-batch-delete/cases/*.json"))
            or list(artifact.glob("*/run/heal.admin-nolock-safety/cases/*.json"))
            or list(artifact.glob("*/run/heal.race-copy-and-metadata-update/cases/*.json"))
            or list(artifact.glob("*/run/heal.race-multipart-complete-abort/cases/*.json"))):
        return race_retention_plan(artifact, failed_run, candidate)
    if list(artifact.glob("*/run/heal.outcome-counters-and-error-truth/cases/*.json")):
        if list(artifact.glob("*/run/heal.outcome-counters-and-error-truth/evidence/heal.outcome-counters-and-error-truth/DB/H1/unknown-negative/*-one-way-legacy-write-phase.json")):
            return acknowledged_legacy_plan(artifact, failed_run, candidate)
        if list(artifact.glob("*/run/heal.outcome-counters-and-error-truth/evidence/heal.outcome-counters-and-error-truth/DB/H1/unknown-negative/*-original-opaque-byte-zero-bitflip.json")):
            return interrupted_opaque_plan(artifact, failed_run, candidate)
        return acknowledged_grace_plan(artifact, failed_run, candidate)
    if list(artifact.glob("*/run/heal.checkpoint-and-mrf-corruption/cases/*.json")):
        results = list(artifact.glob("*/run/heal.checkpoint-and-mrf-corruption/cases/*.json"))
        if any(json.loads(path.read_text()).get("verdict") == "RUNNING" for path in results):
            return checkpoint_plan(artifact, failed_run, candidate)
        folder = next(artifact.glob("*/run/heal.checkpoint-and-mrf-corruption/evidence/heal.checkpoint-and-mrf-corruption"))
        if list((folder / "DB/H2/mrf-checksum").glob("*-actual-original-MRF-CRC-injection.json")):
            return retained_mrf_recovery_plan(artifact, failed_run, candidate)
        return rejected_mrf_recovery_plan(artifact, failed_run, candidate)
    if list(artifact.glob("*/run/heal.bucket-incarnation-recreation/cases/*.json")):
        return recreation_recovery_plan(artifact, failed_run, candidate)
    if list(artifact.glob("*/run/heal.enospc-inodes-and-quota/cases/heal.enospc-inodes-and-quota.json")):
        require(not list(artifact.glob("*/run/heal.object-size-boundaries/cases/*.json")), "mixed failed case recovery artifact")
        return quota_recovery_plan(artifact, failed_run, candidate)
    network = list(artifact.glob("*/run/heal.network-partition-and-lock-quorum/cases/heal.network-partition-and-lock-quorum.json"))
    if network:
        return network_recovery_plan(artifact, failed_run, candidate)
    performance = list(artifact.glob("*/run/heal.performance-abba/cases/heal.performance-abba.json"))
    if performance:
        return performance_recovery_plan(artifact, failed_run, candidate)
    results = list(artifact.glob("*/run/heal.object-size-boundaries/cases/heal.object-size-boundaries.json"))
    require(len(results) == 1, "failed artifact must have one exact D01 canonical result")
    result = json.loads(results[0].read_text())
    require(result["verdict"] == "FAIL" and result["cleanup_status"] == "FAIL"
            and result["provenance"]["source_commit"] == candidate["source_sha"]
            and result["provenance"]["candidate_sha256"] == candidate["package_sha256"], "failed result candidate or cleanup identity differs")
    evidence = results[0].parents[1] / "evidence/heal.object-size-boundaries"
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    plans = []
    for topology in summary["results"]:
        if topology["cleanup"] == "PASS":
            continue
        require(topology["topology"] in {"H1", "H2"} and topology["fixture"] == "DB"
                and "invalid fault ID" in topology.get("failure", {}).get("message", ""), "cleanup failure needs a different recovery procedure")
        failed = [(name, branch) for name, branch in topology["branches"].items()
                  if "invalid fault ID" in branch.get("failure", {}).get("message", "")]
        require(len(failed) == 1, "cannot identify one rejected fault branch")
        branch, _ = failed[0]
        folder = evidence / "DB" / topology["topology"]
        files = list(folder.glob("*-physical-before-" + branch + ".json"))
        faults = list(folder.glob("*-fault-plan-" + branch + ".json"))
        bindings = list(folder.glob("*-binding.json"))
        require(len(files) == len(faults) == len(bindings) == 1, "recovery lacks exact archived pre-fault/binding/plan evidence")
        before, fault, binding = (json.loads(path.read_text()) for path in (files[0], faults[0], bindings[0]))
        require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-{topology['topology'].lower()}", binding["runId"]) is not None,
                "archived run namespace differs from failed Actions run")
        require(len(fault["faults"]) == 1, "rejected-ID recovery cannot omit earlier faults in the same branch")
        target = fault["faults"][0]
        rejected_id = "D01-" + branch + "-0"
        require(re.fullmatch(r"D01-[a-z0-9-]{49,128}", rejected_id) is not None, "failed ID was not rejected for excessive length")
        require(target["node"] in before and target["sha256"] and binding["candidateBinarySha256"] == result["provenance"]["binary_sha256"],
                "fault/candidate physical identity differs")
        nodes = list(before)
        require(all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "invalid archived node name")
        plans.append({"runId": binding["runId"], "nodes": nodes, "node": target["node"], "disk": target["disk"],
            "rejectedFaultId": rejected_id, "binarySha256": binding["candidateBinarySha256"], "before": before,
            "expectedSceneSha256": hashlib.sha256(json.dumps(physical_content(before)[target["node"]],
                sort_keys=True, separators=(",", ":")).encode()).hexdigest()})
    require(plans, "failed artifact has no supported retained environment")
    return plans


def rejected_mrf_recovery_plan(artifact, failed_run, candidate):
    """Finalize only a refused first MRF injection with unchanged original bytes."""
    case = "heal.checkpoint-and-mrf-corruption"
    results = list(artifact.glob("*/run/*/cases/*.json"))
    require(len(results) == 1 and results[0].name == case + ".json", "F07 recovery cannot omit another failed case")
    result = json.loads(results[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL" and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"], "F07 recovery candidate or result differs")
    evidence = results[0].parents[1] / "evidence" / case
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    rows = summary.get("results") or []
    require(summary.get("caseId") == case and summary.get("cleanup") == "FAIL"
            and summary.get("complete") is False and summary.get("canonicalCasePassClaimed") is False
            and rows and all(row.get("cleanup") == "PASS" for row in rows[:-1])
            and rows[-1].get("topology") == "H2" and rows[-1].get("fixture") == "DB"
            and rows[-1].get("branch") == "mrf-checksum" and rows[-1].get("verdict") == "FAIL"
            and rows[-1].get("cleanup") == "FAIL" and "unresolvedMutation" not in rows[-1]
            and "MRF fault lacks an actual committed active owned responsibility on this exact slot" in
                rows[-1].get("failure", {}).get("message", ""), "F07 recovery supports only its first rejected MRF injection")
    folder = evidence / "DB/H2/mrf-checksum"
    def one(label):
        found = list(folder.glob("*-" + label + ".json"))
        require(len(found) == 1, "F07 recovery lacks one exact " + label)
        return json.loads(found[0].read_text())
    binding, before, failure = one("binding"), one("original-partial-write-physical"), one("original-physical-failure-scene")
    original, retained = one("original-stopped-MRF-records"), one("original-MRF-failure-scene")
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h2", binding.get("runId", ""))
            and binding.get("candidateCommit") == candidate["source_sha"]
            and binding.get("candidatePackageSha256") == candidate["package_sha256"]
            and binding.get("candidateBinarySha256") == provenance.get("binary_sha256")
            and binding.get("harnessCommit") == provenance.get("testing_commit")
            and len(before) == 4 and set(before) == set(failure) == set(original) == set(retained)
            and physical_content(before) == physical_content(failure) and original == retained
            and all(value.get("quiescent") is True and value.get("pid") == 0
                and value.get("binarySha256") == provenance.get("binary_sha256") for value in failure.values()),
            "F07 rejected injection changed its candidate, physical scene or original MRF records")
    nodes, bucket = list(failure), binding.get("bucketPrefix", "")
    require(all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes)
            and re.fullmatch(r"rv-heal-[0-9a-f]{16}", bucket), "F07 recovery has invalid owned nodes or bucket")
    gate = one("gate-recovery")
    require(gate.get("node") in nodes and gate.get("state") == "CLEAR"
            and gate.get("originalPermissionsRestored") is True
            and not list(folder.glob("*-actual-original-MRF-CRC-injection.json"))
            and not list(folder.glob("*-injected-stopped-MRF-records.json")), "F07 injection or gate recovery is ambiguous")
    requests, acks = sorted((folder / "fixture").glob("*-request.json")), sorted((folder / "fixture").glob("*-ack.json"))
    require(len(requests) == len(acks) == 7, "F07 recovery lacks its complete create and six PUT acknowledgements")
    for seq, (request_path, ack_path) in enumerate(zip(requests, acks), 1):
        request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
        observed = ack.get("observation") or {}
        require(type(request.get("sequence")) is int and type(ack.get("sequence")) is int
                and request["sequence"] == ack["sequence"] == seq
                and request.get("runId") == ack.get("runId") == binding["runId"]
                and ack.get("requestFile") == request_path.name
                and ack.get("requestSha256") == hashlib.sha256(request_path.read_bytes()).hexdigest()
                and observed.get("operation") == request.get("operation") == ("create_bucket" if seq == 1 else "put_object")
                and observed.get("params") == request.get("params") and request.get("params", {}).get("Bucket") == bucket
                and observed.get("clientCallInvoked") is True and observed.get("resultUnknown") is False
                and observed.get("acknowledged") is True and ack.get("operationCheck") == "PASS",
                "F07 recovery ledger changed identity or contains an unknown mutation")
    return [{"kind": "rejected-mrf-injection", "runId": binding["runId"], "nodes": nodes, "node": nodes[0],
             "bucket": bucket, "before": failure, "originalMrf": original,
             "binarySha256": provenance["binary_sha256"], "rejectedFaultId": "F07-mrf-checksum-0-0-0"}]


def retained_mrf_recovery_plan(artifact, failed_run, candidate):
    """Export and discard an advanced stopped fixture without resurrecting MRF bytes."""
    try:
        from . import persistence_mrf, journal_fault
    except ImportError:
        import persistence_mrf, journal_fault
    case = "heal.checkpoint-and-mrf-corruption"
    paths = list(artifact.glob("*/run/*/cases/*.json"))
    require(len(paths) == 1 and paths[0].name == case + ".json", "retained MRF recovery requires one unmixed result")
    result = json.loads(paths[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL" and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"], "retained MRF candidate or result differs")
    evidence = paths[0].parents[1] / "evidence" / case
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    rows = summary.get("results") or []
    require(summary.get("caseId") == case and summary.get("cleanup") == "FAIL" and summary.get("complete") is False
            and summary.get("canonicalCasePassClaimed") is False and rows
            and all(row.get("cleanup") == "PASS" for row in rows[:-1])
            and rows[-1].get("topology") == "H2" and rows[-1].get("fixture") == "DB"
            and rows[-1].get("branch") == "mrf-checksum" and rows[-1].get("verdict") == "FAIL"
            and rows[-1].get("cleanup") == "FAIL" and "unresolvedMutation" not in rows[-1]
            and rows[-1].get("failure", {}).get("message") ==
                "NOT_OBSERVED: original MRF corruption has no candidate diagnostic on vm000",
            "retained MRF recovery supports only its archived post-restart diagnostic refusal")
    folder = evidence / "DB/H2/mrf-checksum"
    def one(label):
        found = [path for path in folder.glob("*-" + label + ".json")
                 if re.fullmatch(r"[0-9]{5}-" + re.escape(label) + r"\.json", path.name)]
        require(len(found) == 1, "retained MRF recovery lacks one exact " + label)
        return json.loads(found[0].read_text())
    binding, failure = one("binding"), one("original-physical-failure-scene")
    original, injected, retained = (one(label) for label in
        ("original-stopped-MRF-records", "injected-stopped-MRF-records", "original-MRF-failure-scene"))
    nodes = [f"vm00{index}" for index in range(4)]
    bucket = binding.get("bucketPrefix", "")
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h2", binding.get("runId", ""))
            and re.fullmatch(r"rv-heal-[0-9a-f]{16}", bucket)
            and binding.get("candidateCommit") == candidate["source_sha"]
            and binding.get("candidatePackageSha256") == candidate["package_sha256"]
            and binding.get("candidateBinarySha256") == provenance.get("binary_sha256")
            and binding.get("harnessCommit") == provenance.get("testing_commit")
            and all(set(scene) == set(nodes) for scene in (failure, original, injected, retained))
            and all(value.get("quiescent") is True and value.get("pid") == 0
                    and value.get("binarySha256") == provenance.get("binary_sha256") for value in failure.values()),
            "retained MRF run, candidate, stopped scene or node identity differs")
    receipts = [json.loads(path.read_text()) for path in folder.glob("*-actual-original-MRF-CRC-injection.json")]
    require(len(receipts) == 1 and receipts[0].get("node") == "vm000"
            and receipts[0].get("faultId") == "F07-mrf-checksum-0-0-0" and receipts[0].get("disk") == 0
            and receipts[0].get("runId") == binding["runId"]
            and receipts[0].get("binarySha256") == provenance.get("binary_sha256"),
            "retained MRF recovery cannot omit another injection")
    persistence_mrf.retained_corruption(original, injected, receipts, bucket)
    for observed in retained.values():
        persistence_mrf.indexed(observed)
        require(observed.get("binarySha256") == provenance.get("binary_sha256"), "retained MRF candidate bytes differ")
    gate = one("gate-recovery")
    require(gate.get("node") == "vm003" and gate.get("state") == "CLEAR"
            and gate.get("originalPermissionsRestored") is True, "retained MRF gate was not restored")
    recovery = one("recovery")
    require(recovery.get("cleanup") == "FAIL" and len(recovery.get("errors", [])) == 1
            and recovery["errors"][0].get("state") == "DIRTY"
            and recovery["errors"][0].get("namespacesRetained") is True
            and "product changed or advanced the durable responsibility; refusing old-record resurrection" in
                recovery["errors"][0].get("error", ""), "retained MRF rescue refusal differs")
    requests, acks = sorted((folder / "fixture").glob("*-request.json")), sorted((folder / "fixture").glob("*-ack.json"))
    require(len(requests) == len(acks) == 7, "retained MRF recovery lacks complete fixture acknowledgements")
    for seq, (request_path, ack_path) in enumerate(zip(requests, acks), 1):
        request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
        observed = ack.get("observation") or {}
        require(type(request.get("sequence")) is int and type(ack.get("sequence")) is int
                and request["sequence"] == ack["sequence"] == seq
                and request.get("runId") == ack.get("runId") == binding["runId"]
                and ack.get("requestFile") == request_path.name
                and ack.get("requestSha256") == hashlib.sha256(request_path.read_bytes()).hexdigest()
                and observed.get("operation") == request.get("operation") == ("create_bucket" if seq == 1 else "put_object")
                and observed.get("params") == request.get("params") and request.get("params", {}).get("Bucket") == bucket
                and observed.get("clientCallInvoked") is True and observed.get("resultUnknown") is False
                and observed.get("acknowledged") is True and ack.get("operationCheck") == "PASS",
                "retained MRF ledger changed identity or contains an unknown mutation")
    fault = {key: value for key, value in receipts[0].items() if key not in {"node", "applied", "originalBackupRetained"}}
    raw = persistence_mrf.indexed(original["vm000"])[0, fault["relativePath"]]
    require(journal_fault.entry(raw) == fault["before"], "retained MRF original backup identity differs")
    return [{"kind": "advanced-mrf-fixture", "runId": binding["runId"], "nodes": nodes, "node": nodes[0],
        "bucket": bucket, "before": failure, "originalMrf": retained,
        "binarySha256": provenance["binary_sha256"], "faults": {"vm000": [fault]},
        "originalBackup": {"vm000": {fault["faultId"]: base64.b64encode(raw).decode()}}}]


def recreation_recovery_plan(artifact, failed_run, candidate):
    """Archive a stopped C11 fixture after an exact known DeleteBucket refusal."""
    case = "heal.bucket-incarnation-recreation"
    paths = list(artifact.glob("*/run/*/cases/*.json"))
    require(len(paths) == 1 and paths[0].name == case + ".json",
            "C11 recovery requires one unmixed failed result")
    result = json.loads(paths[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL"
            and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"],
            "C11 failed-result candidate or cleanup differs")
    evidence = paths[0].parents[1] / f"evidence/{case}"
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    rows = summary.get("results", [])
    require(summary.get("caseId") == case and summary.get("cleanup") == "FAIL"
            and summary.get("complete") is False and summary.get("canonicalCasePassClaimed") is False
            and len(rows) == 1 and rows[0].get("topology") == "H1"
            and rows[0].get("fixture") == "DV" and rows[0].get("branch") == "old-metadata-rejoin"
            and rows[0].get("verdict") == "FAIL" and rows[0].get("cleanup") == "FAIL"
            and rows[0].get("failure", {}).get("type") == "RecordedCallError"
            and rows[0].get("unresolvedMutation") == {"operation": "normal-S3-delete-and-recreate"},
            "C11 recovery supports only its first known DeleteBucket refusal")
    folder = evidence / "H1/old-metadata-rejoin"
    require(len(list(evidence.rglob("*-binding.json"))) == 1,
            "C11 recovery cannot omit another bound namespace")
    def one(label):
        found = list(folder.glob("*-" + label + ".json"))
        require(len(found) == 1, "C11 recovery lacks one exact " + label)
        return json.loads(found[0].read_text())
    binding, scene, old = one("binding"), one("original-recreation-failure-scene"), one("old-ACK-manifest")
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h1", binding.get("runId", ""))
            and binding.get("candidateCommit") == candidate["source_sha"]
            and binding.get("candidatePackageSha256") == candidate["package_sha256"]
            and binding.get("candidateBinarySha256") == provenance.get("binary_sha256")
            and binding.get("harnessCommit") == provenance.get("testing_commit")
            and len(scene) == 1 and all(value.get("quiescent") is True and value.get("pid") == 0
                and value.get("binarySha256") == provenance.get("binary_sha256") for value in scene.values()),
            "C11 archive lost its stopped run/candidate identity")
    node, bucket = next(iter(scene)), binding.get("bucketPrefix", "")
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node)
            and re.fullmatch(r"rv-heal-[0-9a-f]{16}", bucket), "C11 archive has invalid owned host/bucket")
    recovered = one("gate-recovery")
    require(recovered.get("node") == node and recovered.get("state") == "CLEAR"
            and recovered.get("originalPermissionsRestored") is True,
            "C11 physical gate was not already recovered")
    requests = sorted((folder / "fixture").glob("*-request.json"))
    acknowledgements = sorted((folder / "fixture").glob("*-ack.json"))
    require(len(requests) == len(acknowledgements) and len(requests) > 1,
            "C11 archive has an unacknowledged or missing S3 call")
    observed = []
    for sequence, (request_path, ack_path) in enumerate(zip(requests, acknowledgements), 1):
        request, ack = json.loads(request_path.read_text()), json.loads(ack_path.read_text())
        observation = ack.get("observation") or {}
        require(type(request.get("sequence")) is int and type(ack.get("sequence")) is int
                and request["sequence"] == ack["sequence"] == sequence
                and request.get("runId") == ack.get("runId") == binding["runId"]
                and ack.get("requestFile") == request_path.name
                and ack.get("requestSha256") == hashlib.sha256(request_path.read_bytes()).hexdigest()
                and observation.get("operation") == request.get("operation")
                and observation.get("params") == request.get("params")
                and observation.get("clientCallInvoked") is True
                and observation.get("resultUnknown") is False,
                "C11 S3 ledger changed identity/digest or contains an unknown mutation")
        require(request.get("params", {}).get("Bucket") in {bucket, bucket + "-control"},
                "C11 S3 call escaped its archived owned buckets")
        if sequence < len(requests):
            require(ack.get("operationCheck") == "PASS" and observation.get("acknowledged") is True,
                    "C11 archive has an earlier rejected S3 operation")
        observed.append(observation)
    refusal = observed[-1]
    response = refusal.get("response") or {}
    require(rows[0]["failure"].get("message") ==
                f"unexpected delete_bucket result; inspect ACK {len(requests):06d}, do not silently retry"
            and refusal.get("operation") == "delete_bucket" and refusal.get("params") == {"Bucket": bucket}
            and refusal.get("acknowledged") is False
            and json.loads(acknowledgements[-1].read_text()).get("operationCheck") == "FAIL"
            and response.get("Error") == {"Code": "InternalError", "Message": "File access denied"}
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 500,
            "C11 refusal is uncertain or differs from the audited product error")
    wanted = {(row["key"], row["versionId"]) for row in old["expected"]}
    deletes = [(row["params"].get("Key"), row["params"].get("VersionId")) for row in observed[:-1]
               if row["operation"] == "delete_object" and row["params"]["Bucket"] == bucket]
    require(wanted and len(wanted) == len(old["expected"]) and len(deletes) >= len(wanted)
            and len(set(deletes[-len(wanted):])) == len(wanted) and set(deletes[-len(wanted):]) == wanted,
            "C11 archive lacks every acknowledged original version/marker deletion")
    require(not list(folder.glob("*-archive-and-detach-original-replay.json"))
            and not list(folder.glob("*-successor-ACK-bound-S3.json"))
            and all(row.get("path", "").split("/", 1)[0] in {bucket, bucket + "-control"}
                    for value in scene.values() for row in value["files"]),
            "C11 retained scene includes another replay transaction or successor")
    return [{"kind": "rejected-incarnation-delete", "runId": binding["runId"], "nodes": [node],
             "node": node, "buckets": [bucket, bucket + "-control"], "before": scene,
             "binarySha256": provenance["binary_sha256"], "originalProductError": response["Error"],
             "acknowledgedVersionDeletions": len(wanted), "unknownMutationClaimed": False}]


def performance_recovery_plan(artifact, failed_run, candidate):
    """Finalize a performance run that expired before its next fault was applied.

    The performance executor registers a fault before invoking the node-side
    mutation.  If the lease expires at that boundary, cleanup still sees the
    registration and tries to recover an intent that was never created.  This
    path is deliberately narrower than a general fault rescue: it accepts only
    the archived phase boundary with no fault or fault-proof record and an
    unchanged physical scene, then asks the node to perform its ordinary
    run-owned cleanup.
    """
    case = "heal.performance-abba"
    results = list(artifact.glob(f"*/run/{case}/cases/{case}.json"))
    require(len(results) == 1, "performance recovery requires one exact J06 result")
    result = json.loads(results[0].read_text())
    provenance = result.get("provenance") or {}
    require(result.get("case_id") == case and result.get("verdict") == "FAIL"
            and result.get("cleanup_status") == "FAIL"
            and provenance.get("source_commit") == candidate["source_sha"]
            and provenance.get("candidate_sha256") == candidate["package_sha256"],
            "performance failed-result candidate or cleanup differs")
    evidence = results[0].parents[1] / f"evidence/{case}"
    summary = json.loads((evidence / "healthy-summary.json").read_text())
    require(summary.get("cleanup") == "FAIL" and len(summary.get("results", [])) == 1,
            "performance recovery cannot omit or combine a retained instance")
    row = summary["results"][0]
    failure = row.get("failure", {})
    require(row.get("topology") == "H2" and row.get("verdict") == "FAIL"
            and row.get("cleanup") == "FAIL"
            and failure.get("type") == "ValueError"
            and "apply-fault failed" in failure.get("message", "")
            and "lease expired" in failure.get("message", ""),
            "performance recovery only supports a fault lease boundary failure")
    folder = evidence / "H2"
    binding_files = list(folder.glob("*-binding.json"))
    failure_files = list(folder.glob("*-physical-failure.json"))
    boundary_files = list(folder.glob("*-physical-phase-baseline-g3-p3-B.json"))
    protocol_files = list(folder.glob("*-phase-protocol-g3-p3-B.json"))
    require(len(binding_files) == len(failure_files) == len(boundary_files) == len(protocol_files) == 1,
            "performance recovery lacks one exact retained boundary")
    binding = json.loads(binding_files[0].read_text())
    before = json.loads(boundary_files[0].read_text())
    failure_scene = json.loads(failure_files[0].read_text())
    protocol = json.loads(protocol_files[0].read_text())
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h2", binding.get("runId", ""))
            and binding.get("candidateBinarySha256") == provenance.get("binary_sha256")
            and binding.get("harnessCommit") == provenance.get("testing_commit")
            and protocol.get("group") == 3 and protocol.get("phase") == 3 and protocol.get("mode") == "B"
            and physical_content(before) == physical_content(failure_scene),
            "performance retained scene or run identity changed")
    # A failed remote apply has no durable fault evidence.  A fault record or
    # proof at this boundary would require the normal registered-fault rescue.
    require(not list(folder.glob("*-fault-g3-p3-B.json"))
            and not list(folder.glob("*-fault-proof-g3-p3-B.json")),
            "performance boundary contains an applied fault; refuse shortcut cleanup")
    nodes = sorted(failure_scene)
    require(nodes and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "performance archived node identity is invalid")
    bucket = binding.get("bucketPrefix")
    require(isinstance(bucket, str) and re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", bucket),
            "performance binding has no exact primary bucket")
    buckets = {bucket}
    for value in failure_scene.values():
        for item in value.get("files", []):
            name = item.get("path", "").split("/", 1)[0]
            if name == bucket or name.startswith(bucket + "-"):
                buckets.add(name)
    require(buckets == {bucket, bucket + "-load"},
            "performance retained scene omitted or added a run-owned bucket")
    return [{"kind": "performance-no-fault-applied", "runId": binding["runId"], "nodes": nodes,
             "node": nodes[0],
             "buckets": sorted(buckets),
             "before": failure_scene, "expectedSceneSha256": hashlib.sha256(json.dumps(
                 physical_content(failure_scene)[nodes[0]], sort_keys=True, separators=(",", ":")
             ).encode()).hexdigest(), "binarySha256": provenance["binary_sha256"]}]


def recovery_scene_matches(current, plan):
    """A retry may contain only the exact original shards already rescued."""
    observed = physical_content(current)
    expected = physical_content(plan.get("failure", plan["before"]))
    if plan.get("kind") == "interrupted-j02-opaque":
        if set(observed) != set(expected): return False
        original = physical_content(plan["before"])
        for node, scene in observed.items():
            if scene["formats"] != expected[node]["formats"]: return False
            current = {(entry["disk"], entry["path"]): entry for entry in scene["files"]}
            failed = {(entry["disk"], entry["path"]): entry for entry in expected[node]["files"]}
            before = {(entry["disk"], entry["path"]): entry for entry in original[node]["files"]}
            if len(current) != len(scene["files"]) or set(current) != set(failed) or set(before) != set(failed): return False
            if any(value != failed[key] and value != before[key] for key, value in current.items()): return False
        return True
    if plan.get("kind") not in {"owned-quota", "interrupted-checkpoint-before-admission"}:
        return observed == expected
    original = physical_content(plan["before"])
    if set(observed) != set(expected):
        return False
    for node, scene in observed.items():
        if scene["formats"] != expected[node]["formats"]:
            return False
        files = {(entry["disk"], entry["path"]): entry for entry in scene["files"]}
        failed = {(entry["disk"], entry["path"]): entry for entry in expected[node]["files"]}
        before = {(entry["disk"], entry["path"]): entry for entry in original[node]["files"]}
        if len(files) != len(scene["files"]) or not set(failed) <= set(files) <= set(before):
            return False
        if any(files[key] != entry for key, entry in failed.items()):
            return False
        if any(files[key] != before[key] for key in set(files) - set(failed)):
            return False
    return True


def isolate_incomplete_scene(fleet, plan, artifact):
    """Preserve a drifted fixture; this never restores or finalizes its evidence."""
    case = "heal.checkpoint-and-mrf-corruption"
    folders = list(artifact.glob(f"*/run/{case}/evidence/{case}/DB/H2/mrf-checksum"))
    require(len(folders) == 1, "incomplete fixture lacks its original archive")
    receipts = [json.loads(path.read_text()) for path in folders[0].glob("*-reserved.json")]
    require(len(receipts) == len(plan["nodes"]) and {item.get("node") for item in receipts} == set(plan["nodes"]),
            "incomplete fixture lacks one original reservation per node")
    owners = {}
    for item in receipts:
        owner = item.get("owner") or {}
        require(item.get("reserved") is True and owner.get("runId") == plan["runId"]
                and owner.get("bucket") == plan["bucket"] and owner.get("binarySha256") == plan["binarySha256"]
                and owner.get("runtimeProfile") == "mrf-crash", "incomplete fixture original reservation differs")
        owners[item["node"]] = owner
    audits = {}
    for node in plan["nodes"]:
        audits[node] = fleet.record("incomplete-scene-preflight", {"node": node,
            **fleet.remote(node, "retained-scene-audit", expectedOwner=owners[node])})
        require(audits[node].get("inspectionOnly") is True and audits[node].get("mutated") is False
                and audits[node].get("credentialContentsRead") is False, "incomplete fixture audit was not read-only")
    require(any(any(root is None for root in audit["baseline"]["roots"]) for audit in audits.values()),
            "incomplete-scene isolation requires actual missing roots")
    # All nodes are frozen and audited before the first rename. Each node keeps
    # a durable inventory intent and repeats it before every same-device move.
    for node in plan["nodes"]:
        proof = fleet.record("incomplete-scene-isolation", {"node": node,
            **fleet.remote(node, "retained-scene-isolate", expectedOwner=owners[node],
                           expectedBaseline=audits[node]["baseline"])})
        require(proof.get("activeNamespaceIsolated") is True and proof.get("originalSceneRestored") is False
                and proof.get("originalEvidenceComplete") is False and proof.get("backupRestorationUsed") is False
                and proof.get("canonicalPassClaimed") is False
                and proof.get("baseline") == audits[node]["baseline"], "incomplete fixture isolation failed")
    for node in plan["nodes"]:
        proof = fleet.record("incomplete-scene-post-audit", {"node": node,
            **fleet.remote(node, "retained-scene-audit", expectedOwner=owners[node])})
        require(proof.get("activeNamespaceIsolated") is True and proof.get("inspectionOnly") is True
                and proof.get("mutated") is False and proof.get("baseline") == audits[node]["baseline"],
                "incomplete fixture changed after isolation")
    return {"runId": plan["runId"], "cleanup": "ISOLATED", "activeNamespaceIsolated": True,
            "originalEvidenceComplete": False, "originalSceneRestored": False, "originalVerdictRewritten": False,
            "missingDisks": {node: [index for index, row in enumerate(audits[node]["baseline"]["roots"]) if row is None]
                             for node in plan["nodes"]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--failed-run-id", type=int, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    plans = recovery_plan(args.artifact, args.failed_run_id, json.loads(args.candidate.read_text()))
    os.environ["RUSTFS_SSH_USER"] = os.environ.get("RUSTFS_SSH_USER") or "azureuser"
    assigned = (os.environ.get("RUSTFS_NODES") or "vm000 vm001 vm002 vm003").split()
    require(all(set(plan["nodes"]) <= set(assigned) for plan in plans), "archived nodes do not belong to the current auto-testing fleet")
    args.evidence.mkdir(parents=True, exist_ok=False)
    complete = []
    for plan in plans:
        fleet = Fleet(plan["nodes"], args.evidence, plan["runId"])
        if plan.get("buckets"):
            fleet.snapshot_buckets = plan["buckets"]
        fleet.record("archived-recovery-plan", {key: value for key, value in plan.items() if key != "before"})
        if plan.get("kind") in {"unresolved-g01-race", "unresolved-g02-race", "unresolved-a09-race", "unresolved-g04-race", "unresolved-g05-race", "unresolved-g06-race"}:
            complete.append({"failedRunId": args.failed_run_id, **isolate_race_scene(fleet, plan)})
            continue
        if plan.get("kind") == "unresolved-network-load":
            complete.append({"failedRunId": args.failed_run_id, **isolate_network_scene(fleet, plan)})
            continue
        if plan.get("kind") == "network-expiry-recovery":
            for node in plan["nodes"]:
                fleet.record("network-recovery-stop", {"node": node, **fleet.remote(node, "stop")})
            for node in plan["nodes"]:
                proof = fleet.record("network-expiry-recovery", {"node": node,
                    **fleet.remote(node, "network-release", expired=True)})
                require(proof.get("state") == "CLEAR" and proof.get("rulesRemaining") == 0,
                        "expired network gate was not removed exactly")
            for node in plan["nodes"]:
                proof = fleet.record("network-finalization", {"node": node, **fleet.remote(node, "cleanup")})
                require(proof.get("cleanup") == "PASS" and proof.get("unitStopped") is True
                        and proof.get("rootsRemoved") is True, "network namespace finalization failed")
            complete.append({"failedRunId": args.failed_run_id, "runId": plan["runId"],
                             "cleanup": "PASS", "originalVerdictRewritten": False})
            continue
        states = [fleet.record("namespace-state", {"node": node,
            **fleet.remote(node, "namespace-state")}) for node in plan["nodes"]]
        has_retained = any(item.get("retained", {}).get("state")
                           or any(item.get("retained", {}).get("roots", [])) for item in states)
        all_absent = all(not item["state"]["exists"] and not any(root["exists"] for root in item["roots"]) for item in states)
        if plan.get("kind") in {"acknowledged-j02-before-replay", "acknowledged-j02-before-legacy-put"}:
            complete.append({"failedRunId": args.failed_run_id, **isolate_acknowledged_grace(fleet, plan, states)})
            continue
        if plan.get("kind") in {"interrupted-checkpoint-before-admission", "interrupted-j02-opaque"}:
            complete.append({"failedRunId": args.failed_run_id,
                             **restore_and_isolate(fleet, plan, states, recovery_scene_matches)})
            continue
        if plan.get("kind") == "advanced-mrf-fixture" and (has_retained or (not all_absent and any(
                not root["exists"] for item in states for root in item["roots"]))):
            complete.append({"failedRunId": args.failed_run_id, **isolate_incomplete_scene(fleet, plan, args.artifact)})
            continue
        if all(not item["state"]["exists"] and not any(root["exists"] for root in item["roots"])
               for item in states):
            for item in states:
                require(item["service"].get("MainPID") == "0"
                        and item["service"].get("ActiveState") == "inactive"
                        and item["service"].get("SubState") == "dead",
                        "absent namespace still has an active or unknown service; no finalization claimed")
                fleet.record("already-clean", {"node": item["node"], "cleanup": "PASS",
                    "unitStopped": True, "rootsRemoved": True})
            complete.append({"failedRunId": args.failed_run_id, "runId": plan["runId"],
                             "cleanup": "PASS", "originalVerdictRewritten": False,
                             "alreadyClean": True})
            continue
        current = fleet.snapshot("audited-physical-scene", quiescent=True)
        require(recovery_scene_matches(current, plan),
                "retained fleet drifted from its exact archived scene; no recovery performed")
        fleet.record("retained-service-logs", fleet.remote(plan["node"], "logs"))
        if plan.get("kind") == "advanced-mrf-fixture":
            archived = fleet.record("retained-advanced-MRF-records", {node: fleet.remote(node,
                "mrf-records", quiescent=True) for node in plan["nodes"]})
            require(archived == plan["originalMrf"], "advanced MRF scene drifted; retain namespace")
            for action in ("journal-retained-audit", "journal-retained-audit-finalize"):
                for node in plan["nodes"]:
                    scene = physical_content(plan["before"])[node]
                    proof = fleet.record("audited-finalization" if action.endswith("finalize") else "exported-original-journal-backups", {"node": node,
                        **fleet.remote(node, action, bucket=plan["bucket"], binarySha256=plan["binarySha256"],
                            faults=plan["faults"].get(node, []), originalBackup=plan["originalBackup"].get(node, {}),
                            expectedMrfSha256=hashlib.sha256(json.dumps(plan["originalMrf"][node], sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                            expectedSceneSha256=hashlib.sha256(json.dumps(scene, sort_keys=True, separators=(",", ":")).encode()).hexdigest())})
                    require(proof.get("originalEvidenceExported") is True and proof.get("backupRestorationUsed") is False
                            and proof.get("productRepairClaimed") is False
                            and (proof.get("readyToFinalize") is True if action.endswith("audit") else
                                proof.get("cleanup") == "PASS" and proof.get("unitStopped") is True and proof.get("rootsRemoved") is True),
                            "advanced MRF evidence export or finalization failed")
        elif plan.get("kind") == "rejected-mrf-injection":
            archived = fleet.record("retained-original-MRF-records", {node: fleet.remote(node,
                "mrf-records", quiescent=True) for node in plan["nodes"]})
            require(archived == plan["originalMrf"], "F07 original MRF bytes drifted; retain namespace")
            for node in plan["nodes"]:
                gate = fleet.record("audited-gate-state", {"node": node, **fleet.remote(node, "gate-state")})
                require(gate.get("state") == "CLEAR", "F07 member gate is not clear")
            require(recovery_scene_matches(fleet.snapshot("physical-immediately-before-finalization", quiescent=True), plan),
                    "F07 stopped scene changed during evidence export; retain namespace")
            for action in ("journal-rejection-audit", "journal-rejection-audit-finalize"):
                for node in plan["nodes"]:
                    scene = physical_content(plan["before"])[node]
                    proof = fleet.record("audited-finalization" if action.endswith("finalize") else "audited-no-injection", {"node": node,
                        **fleet.remote(node, action, bucket=plan["bucket"],
                            binarySha256=plan["binarySha256"], rejectedFaultId=plan["rejectedFaultId"],
                            expectedMrfSha256=hashlib.sha256(json.dumps(plan["originalMrf"][node], sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                            expectedSceneSha256=hashlib.sha256(json.dumps(scene, sort_keys=True, separators=(",", ":")).encode()).hexdigest())})
                    require(proof.get("faultNeverApplied") is True and (proof.get("readyToFinalize") is True if action.endswith("audit") else
                            proof.get("cleanup") == "PASS" and proof.get("unitStopped") is True and proof.get("rootsRemoved") is True),
                            "F07 rejected journal namespace audit or finalization failed")
        elif plan.get("kind") == "rejected-incarnation-delete":
            require(all(value.get("binarySha256") == plan["binarySha256"] for value in current.values()),
                    "retained candidate binary differs from the archived failed scene")
            gate = fleet.record("audited-gate-state", fleet.remote(plan["node"], "gate-state"))
            require(gate.get("state") == "CLEAR", "C11 gate drifted after archived recovery")
            system = fleet.record("retained-system-bucket-records", fleet.remote(plan["node"],
                "bucket-record-snapshot", buckets=plan["buckets"]))
            require(system.get("credentialsRead") is False, "C11 recovery crossed its credential boundary")
            require(recovery_scene_matches(fleet.snapshot("physical-immediately-before-finalization", quiescent=True), plan),
                    "C11 stopped scene changed during its evidence export; retain namespace")
            proof = fleet.record("audited-finalization", fleet.remote(plan["node"], "cleanup"))
            require(proof.get("unitStopped") is True and proof.get("rootsRemoved") is True,
                    "C11 owned namespace finalization was not complete")
        elif plan.get("kind") == "performance-no-fault-applied":
            for node in plan["nodes"]:
                proof = fleet.record("audited-finalization", {"node": node,
                    **fleet.remote(node, "cleanup")})
                require(proof.get("cleanup") == "PASS" and proof.get("unitStopped") is True
                        and proof.get("rootsRemoved") is True,
                        "performance retained environment finalization failed")
        elif plan.get("kind") == "owned-quota":
            for fault in plan["faults"]:
                fleet.record("quota-original-shard-rescue", fleet.remote(plan["node"], "recover-fault", **fault))
            require(physical_content(fleet.snapshot("complete-pre-fault-rescue", quiescent=True)) == physical_content(plan["before"]),
                    "quota rescue did not restore every original file; preserve namespace")
            proof = fleet.record("audited-finalization", fleet.remote(plan["node"], "quota-audit-finalize", **{
                key: plan[key] for key in ("bucket", "binarySha256", "expectedSceneSha256")}))
        else:
            proof = fleet.record("audited-finalization", fleet.remote(plan["node"], "audit-finalize", **{
                key: plan[key] for key in ("disk", "rejectedFaultId", "binarySha256", "expectedSceneSha256")}))
        if plan.get("kind") not in {"performance-no-fault-applied", "rejected-mrf-injection", "advanced-mrf-fixture"}:
            for node in plan["nodes"]:
                if node != plan["node"]:
                    fleet.record("unchanged-peer-finalization", fleet.remote(node, "cleanup"))
        require(proof.get("cleanup") == "PASS", "audited environment finalization failed")
        complete.append({"failedRunId": args.failed_run_id, "runId": plan["runId"], "cleanup": "PASS", "originalVerdictRewritten": False})
    save(args.evidence / "recovery-summary.json", {
        "cleanup": "ISOLATED" if any(item["cleanup"] == "ISOLATED" for item in complete) else "PASS",
        "environments": complete, "canonicalPassClaimed": False})


if __name__ == "__main__":
    main()
