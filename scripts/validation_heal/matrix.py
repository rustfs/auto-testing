#!/usr/bin/env python3
"""Preserve all Heal procedures and audit branch evidence on the auto-testing fleet."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sys


ROOT = Path(__file__).resolve().parents[2]
RECIPES = Path(__file__).with_name("recipes.json")
CATALOG = ROOT / "validation-cases.json"
OPERATIONS = ROOT / "docs/validation-heal-operations.md"
CONTRACT = ROOT / "docs/validation-heal-contract.md"
TOPOLOGIES = {"H1", "H2", "H3", "H4", "H5", "H6"}
STAGES = {"preflight", "baseline", "fault", "trigger", "control", "physical", "s3", "recovery", "logs"}
AUTOMATIC_CASES = {"heal.foreign-format-and-unknown-schema", "heal.below-write-quorum", "heal.bucket-metadata-integrity", "heal.admin-scope-isolation", "heal.admin-healthy-noop", "heal.admin-invalid-request", "heal.admin-token-cross-node-status", "heal.admin-dry-run", "heal.missing-part-file",
                   "heal.data-shard-bitrot", "heal.parity-shard-bitrot", "heal.truncated-and-zeroed-shard",
                   "heal.completed-multipart-object", "heal.range-head-and-conditions", "heal.manual-normal-attribution", "heal.manual-deep-attribution",
                   "heal.performance-abba", "heal.historical-versions", "heal.repeat-idempotency", "heal.versioning-suspended-null", "heal.below-read-quorum",
                   "heal.missing-or-corrupt-xlmeta", "heal.object-metadata-and-tags", "heal.multiple-shard-loss", "heal.special-keys-and-pagination", "heal.object-size-boundaries", "heal.admin-authorization", "heal.node-outage-rejoin", "heal.object-lock-retention", "heal.metadata-payload-mismatch", "heal.read-repair-attribution", "heal.disk-readonly-and-permission", "heal.scanner-cold-object-repair", "heal.delete-marker-semantics", "heal.status-partial-disabled-unknown", "heal.incremental-cursor-and-truncation", "heal.admin-stop-and-stale-token", "heal.cancelled-task-restart", "heal.admin-overlap-and-force-start", "heal.root-graceful-coordinator-restart", "heal.read-quorum-boundary", "heal.stale-version-rejoin", "heal.disk-loss-and-repair", "heal.disk-replacement-formatted", "heal.full-cluster-restart", "heal.replacement-auto-rebuild", "heal.target-graceful-restart", "heal.target-process-crash", "heal.mrf-offline-write-replay", "heal.disk-replacement-unformatted"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def variant_topologies(item: dict) -> dict[str, set[str]]:
    allowed = set(item["contract"]["probes"]["topology"].split(","))
    require(bool(allowed) and allowed <= TOPOLOGIES, "invalid Heal topology")
    if "variantTopologies" not in item:
        return {variant: allowed for variant in item["variants"]}
    mapping = item["variantTopologies"]
    require(set(mapping) == set(item["variants"]), "variant topology mapping omits a branch")
    require(bool(item.get("topologyApplicabilityReason")), "missing topology applicability reason")
    for values in mapping.values():
        require(isinstance(values, list) and bool(values) and len(values) == len(set(values))
                and set(values) <= allowed, "invalid variant topology mapping")
    require(set().union(*(set(values) for values in mapping.values())) == allowed,
            "variant topology mapping omits a required topology")
    return {variant: set(values) for variant, values in mapping.items()}


def cases() -> list[dict]:
    catalog = json.loads(CATALOG.read_text())
    designed = {case["id"]: case for module in catalog["modules"] for case in module["cases"]
                if case["id"].startswith("heal.")}
    document = json.loads(RECIPES.read_text())
    require(document.get("schemaVersion") == 1 and document.get("executionMode") == "operator-assisted",
            "invalid Heal procedure contract")
    observed = document["cases"]
    require(len(observed) == 84 and {item["id"] for item in observed} == set(designed),
            "Heal procedures must cover exactly the 84 canonical source cases")
    require(len({item["code"] for item in observed}) == 84, "duplicate Heal procedure code")
    anchors = set(re.findall(r'<a id="([A-Z0-9_]+)"></a>', OPERATIONS.read_text()))
    for item in observed:
        contract = designed[item["id"]]
        require(item["code"] == contract["probes"]["designCode"], "Heal procedure identity mismatch")
        require(bool(item["variants"]) and len(item["variants"]) == len(set(item["variants"]))
                and all(re.fullmatch(r"[a-z0-9][a-z0-9-]*", variant) for variant in item["variants"]),
                "invalid Heal procedure variants")
        require(len(item["steps"]) >= 3 and set(item["requiredEvidence"]) == STAGES,
                "incomplete Heal procedure or evidence stages")
        require(bool(item["commands"]) and set(item["commands"]) <= anchors,
                "Heal procedure refers to an undefined operation")
        require(bool(item["recovery"]) and bool(item["verdict"]), "missing Heal recovery/verdict contract")
        item["contract"] = contract
        variant_topologies(item)
    return sorted(observed, key=lambda item: item["code"])


def select(selection: str) -> list[dict]:
    inventory = cases()
    if selection == "all":
        return inventory
    requested = selection.split(",")
    require(len(requested) == len(set(requested)), "duplicate Heal case selection")
    lookup = {key: item for item in inventory for key in (item["id"], item["code"])}
    require(all(key in lookup for key in requested), "unknown Heal case")
    selected = [lookup[key] for key in requested]
    require(len({item["id"] for item in selected}) == len(selected), "same Heal case selected by code and ID")
    return selected


def fleet_binding() -> dict:
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes))
            and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "Heal requires the four distinct current auto-testing SSH nodes")
    user = os.environ.get("RUSTFS_SSH_USER", "azureuser")
    require(bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", user)), "invalid auto-testing SSH user")
    return {"provider": "auto-testing", "jumpHost": socket.gethostname(), "sshUser": user,
            "sshNodes": nodes, "servicePort": 9000, "verified": False,
            "topologies": {
                "H1": {"nodes": nodes[:1], "drivesPerNode": 4, "requiredSets": 1},
                "H2": {"nodes": nodes[:4], "drivesPerNode": 4, "crossNodeSetRequired": True},
                "H3": {"nodes": nodes[:3], "drivesPerNode": 4, "dataShards": 8, "parityShards": 4, "requiredSets": 1},
                "H4": {"nodes": nodes[:3], "drivesPerNode": 8, "dataShards": 8, "parityShards": 4,
                       "minimumSets": 2, "requiresExistingAdditionalDrives": True},
                "H5": {"nodes": nodes[:4], "minimumPools": 2, "poolAndSetMappingRequired": True,
                       "nodeRedundancyMustBeMeasured": True},
                "H6": {"nodes": [], "requiresDedicatedHostBinding": True,
                       "requiresPlatformEventAndRecoveryEvidence": True}},
            "beforeMutation": ["workflow fleet lock and unexpired environment lease", "actual hostname and unit/PID",
                               "independent mount identities and disk UUIDs", "actual pool/set/slot mapping and EC quorums",
                               "run-owned roots, fixture ACKs, fault witnesses and undo plan"]}


def compile_plan(selected: list[dict], topologies: set[str], run_id: str) -> dict:
    require(bool(re.fullmatch(r"(?:gh|manual)-[a-z0-9-]{5,60}", run_id)), "invalid Heal run ID")
    require(bool(topologies) and topologies <= TOPOLOGIES, "unknown Heal topology")
    instances, excluded, non_applicable = [], [], []
    for item in selected:
        allowed = set(item["contract"]["probes"]["topology"].split(","))
        mapping = variant_topologies(item)
        for topology in sorted(allowed):
            if topology not in topologies:
                excluded.append({"caseId": item["id"], "topology": topology, "state": "OUT_OF_SELECTED_SCOPE"})
            for variant in item["variants"]:
                if topology not in mapping[variant]:
                    non_applicable.append({"caseId": item["id"], "topology": topology, "branch": variant,
                                           "state": "NOT_APPLICABLE_TO_TOPOLOGY",
                                           "reason": item["topologyApplicabilityReason"]})
                elif topology in topologies:
                    instances.append({"instanceId": f"{item['code']}.{topology}.{variant}", "caseId": item["id"],
                                      "code": item["code"], "topology": topology, "branch": variant,
                                      "state": "NOT_RUN", "parameterBindings": {}, "sourceAssertions": item["contract"]["assertions"],
                                      "operatorSteps": item["steps"], "operations": item["commands"],
                                      "requiredEvidence": item["requiredEvidence"], "caseVerdict": None})
    require(bool(instances), "selected Heal cases have no requested topology")
    return {"schemaVersion": 1, "runId": run_id, "createdAt": datetime.now(timezone.utc).isoformat(),
            "executionMode": "operator-assisted", "state": "PLAN_ONLY", "automaticExecution": False,
            "livePassClaimed": False, "recipeSha256": digest(RECIPES), "catalogSha256": digest(CATALOG),
            "operationBookSha256": digest(OPERATIONS), "publicContractSha256": digest(CONTRACT),
            "selectedBaseCaseCount": len(selected),
            "selectedCaseIds": [item["id"] for item in selected], "selectedTopologies": sorted(topologies),
            "branchTemplateCount": len(instances), "scopeExclusions": excluded,
            "nonApplicableCombinations": non_applicable, "environmentBinding": fleet_binding(), "instances": instances}


def audit_evidence(plan: dict, directory: Path) -> dict:
    require(plan.get("schemaVersion") == 1 and plan.get("state") == "PLAN_ONLY"
            and plan.get("automaticExecution") is False, "invalid Heal operator plan")
    require(plan.get("recipeSha256") == digest(RECIPES) and plan.get("catalogSha256") == digest(CATALOG)
            and plan.get("operationBookSha256") == digest(OPERATIONS)
            and plan.get("publicContractSha256") == digest(CONTRACT), "Heal plan contract changed")
    expected = compile_plan(select(",".join(plan["selectedCaseIds"])), set(plan["selectedTopologies"]), plan["runId"])
    require(plan["instances"] == expected["instances"] and plan["scopeExclusions"] == expected["scopeExclusions"],
            "Heal plan omitted or changed required branches")
    root = directory.resolve(strict=True)
    problems, recorded = [], []
    for instance in plan["instances"]:
        identity = instance["instanceId"]
        index = root / identity / "evidence.json"
        if not index.is_file() or index.is_symlink():
            problems.append({"instanceId": identity, "reason": "missing evidence index"})
            continue
        require(index.resolve().is_relative_to(root), "Heal evidence index escaped archive")
        value = json.loads(index.read_text())
        require(value.get("runId") == plan["runId"] and value.get("instanceId") == identity,
                "Heal evidence identity mismatch")
        entries = value.get("evidence", {})
        missing = STAGES - set(entries)
        if missing:
            problems.append({"instanceId": identity, "reason": "missing evidence stages", "stages": sorted(missing)})
        for stage, references in entries.items():
            require(stage in STAGES and isinstance(references, list) and bool(references), "invalid Heal evidence stage")
            for reference in references:
                relative = Path(reference["path"])
                require(not relative.is_absolute() and ".." not in relative.parts, "unsafe Heal evidence path")
                file = index.parent / relative
                require(file.resolve().is_relative_to(index.parent.resolve()) and not file.is_symlink() and file.is_file(),
                        "missing or escaped Heal evidence file")
                require(digest(file) == reference["sha256"], "Heal evidence digest mismatch")
        recorded.append(identity)
    return {"state": "EVIDENCE_INCOMPLETE" if problems else "EVIDENCE_COMPLETE_REVIEW_REQUIRED",
            "expectedBranches": len(plan["instances"]), "recordedBranches": len(recorded), "problems": problems,
            "livePassClaimed": False, "canonicalVerdict": None,
            "note": "Evidence completeness does not prove physical repair or satisfy the source assertions."}


def render(selected: list[dict]) -> str:
    lines = ["# Heal validation procedures", "", "These source procedures require live execution and branch evidence on the current auto-testing Heal fleet.",
             "Use [the operations reference](validation-heal-operations.md) before injecting a fault. A procedure card or complete archive is not a PASS.", ""]
    for item in selected:
        lines += [f'<a id="{item["code"]}"></a>', "", f'## {item["code"]}: {item["id"]}', "",
                  f'Topologies: {item["contract"]["probes"]["topology"]}. Branches: {", ".join(item["variants"])}.', ""]
        lines += [f"{index}. {step}" for index, step in enumerate(item["steps"], 1)]
        lines += ["", "Operations: " + " → ".join(f"[{command}](validation-heal-operations.md#{command})" for command in item["commands"]),
                  "", "Source assertions:", ""]
        lines += [f"- {assertion}" for assertion in item["contract"]["assertions"]]
        lines += ["", item["recovery"], "", item["verdict"], ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    sub.add_parser("list")
    show = sub.add_parser("show")
    show.add_argument("selection")
    rendering = sub.add_parser("render")
    rendering.add_argument("--selection", default="all")
    rendering.add_argument("--out", type=Path)
    plan = sub.add_parser("plan")
    plan.add_argument("--selection", default="all")
    plan.add_argument("--topologies", default="H1,H2,H3,H4,H5,H6")
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--out", type=Path, required=True)
    audit = sub.add_parser("audit")
    audit.add_argument("--plan", type=Path, required=True)
    audit.add_argument("--evidence-dir", type=Path, required=True)
    audit.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.command in {"check", "list"}:
        inventory = cases()
        full = compile_plan(inventory, TOPOLOGIES, "manual-heal-contract-check")
        result = {"baseCases": len(inventory), "operatorBranches": sum(len(item["variants"]) for item in inventory),
                  "topologyBranchTemplates": len(full["instances"]), "automaticCaseCount": len(AUTOMATIC_CASES), "livePassClaimed": False}
        if args.command == "list":
            result["cases"] = [{"code": item["code"], "id": item["id"], "variants": item["variants"],
                                "automaticExecutor": item["id"] in AUTOMATIC_CASES} for item in inventory]
        print(json.dumps(result, ensure_ascii=False))
    elif args.command == "show":
        print(json.dumps(select(args.selection), ensure_ascii=False, indent=2))
    elif args.command == "render":
        text = render(select(args.selection))
        if args.out:
            args.out.write_text(text + "\n")
        else:
            print(text)
    elif args.command == "plan":
        save(args.out, compile_plan(select(args.selection), set(args.topologies.split(",")), args.run_id))
    else:
        result = audit_evidence(json.loads(args.plan.read_text()), args.evidence_dir)
        save(args.out, result)
        return 0 if not result["problems"] else 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, KeyError, ValueError) as exc:
        print(f"Heal validation contract error: {exc}", file=sys.stderr)
        sys.exit(1)
