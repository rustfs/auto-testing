#!/usr/bin/env python3
"""C09: refuse witnessed foreign identities and future format/object envelopes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys
import time

try:
    from .heal_probe import Probe, compare_inventories, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .shards import files, map_target, run_task
    from .versions import metadata_target, validate_failed_version, validate_version_receipts
    from .unformatted import binding, foreign_donor, negative_scene, refusal, OBSERVATION_SECONDS, SIZE
    from .disk_loss import member_state
    from .cursor import verify_part_manifest
    from .heal_fault import bounded_fault_id
except ImportError:
    from heal_probe import Probe, compare_inventories, require, save
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import files, map_target, run_task
    from versions import metadata_target, validate_failed_version, validate_version_receipts
    from unformatted import binding, foreign_donor, negative_scene, refusal, OBSERVATION_SECONDS, SIZE
    from disk_loss import member_state
    from cursor import verify_part_manifest
    from heal_fault import bounded_fault_id


CASE_ID = "heal.foreign-format-and-unknown-schema"
BRANCHES = ("foreign-disk", "foreign-set", "future-format", "future-object-schema")
OBJECTS = 128
CONTRACT = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926",
    "sourceFiles": {
        "crates/ecstore/src/layout/format.rs": "a730e6c897de6fca6071a806bcb9ad27a13e292b6c3975dfda6f436187f4784d",
        "crates/ecstore/src/store/init_format.rs": "b6f5d009eccaeb28f9700f721bbf9a005b289716bfec939309193e06e098263c",
        "crates/filemeta/src/filemeta/codec.rs": "ef82b3390b0a377e423f99fb384c6a8a96beaf5722b0c907852cc0c904c4f04d"},
    "futureFormat": {"field": "version", "original": "1", "injected": "999"},
    "futureObjectSchema": {"envelope": "XL2 ", "majorOffset": 4, "byteOrder": "little", "originalMajor": 1,
        "injectedMajor": 2, "parserRefusal": "xl file version err", "innerMsgpackSchemaChanged": False},
    "formatFaultScope": "one witnessed empty owned replacement root; shared formats never mutated"}


def exact_get(observed, expected):
    require(observed.get("acknowledged") is True and observed.get("resultUnknown") is False
            and observed.get("response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
            and observed.get("payload") == expected["payload"], "C09 surviving/recovered GET differs from its independent ACK")
    return {"fullPayloadEqualsACK": True}


def unsupported_get(observed):
    response = observed.get("response") or {}
    require(observed.get("acknowledged") is False and observed.get("resultUnknown") is False
            and "payload" not in observed and response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {500, 503}
            and response.get("Error", {}).get("Code") in {"InternalError", "SlowDownRead"},
            "future-schema GET returned bytes, absence, a transport error or an unreviewed refusal")
    return {"structuredServerRefusal": response["Error"]["Code"], "unknownTreatedAsMissing": False}


def metadata_scene(before, after, targets, receipts):
    """Every unknown record survives byte-for-byte; every control remains frozen."""
    left, right = files(before), files(after)
    require(len(targets) == len(receipts) in {4, 16} and set(left) == set(right), "C09 unknown record was deleted or duplicated")
    changed = {}
    for target, receipt in zip(targets, receipts, strict=True):
        identity = target["node"], target["disk"], target["relativePath"]
        require(identity not in changed and identity in left
                and receipt.get("kind") == "future-xl-schema" and receipt.get("relativePath") == identity[2]
                and receipt.get("before") == {name: left[identity][name] for name in ("sha256", "size")}
                and isinstance(receipt.get("fault"), dict) and receipt["fault"]["size"] == left[identity]["size"]
                and receipt["fault"]["sha256"] != left[identity]["sha256"], "future envelope receipt differs from the exact original")
        require({name: right[identity][name] for name in ("sha256", "size")} == receipt["fault"]
                and "metadata" not in right[identity]
                and right[identity].get("metadataError", {}).get("exactWitnessedFault") is True
                and right[identity].get("metadataError", {}).get("validMetadataClaimed") is False
                and CONTRACT["futureObjectSchema"]["parserRefusal"] in right[identity].get("metadataError", {}).get("message", ""),
                "unknown metadata was rewritten or did not actually reach the reviewed parser refusal")
        changed[identity] = receipt["fault"]
    require({key: value for key, value in left.items() if key not in changed}
            == {key: value for key, value in right.items() if key not in changed}
            and all(before[node]["formats"] == after[node]["formats"] for node in before),
            "future-schema test changed a payload, control object or disk format")
    return {"unknownRecordsRetainedExactly": len(changed), "allPayloadsControlsAndFormatsUnchanged": True,
            "innerMsgpackSchemaChanged": False, "productRepairClaimed": False}


def format_outcome(status, bucket, expected, target):
    # A quarantined disk may leave the existing object verifiable on surviving
    # authorities. Its exact disk refusal is proved independently, never inferred
    # from a healthy object receipt or from a startup failure.
    execution = (status.get("outcome") or {}).get("execution", {}).get("state")
    if execution == "completed":
        proof = validate_version_receipts(status, bucket, expected, 2)
    else:
        proof = validate_failed_version(status, bucket, expected, target)
    require((status.get("outcome") or {}).get("counters", {}).get("healed") == 0,
            "foreign/future format was reported reconstructed")
    return {**proof, "refusedSlotValidatedSeparately": True, "productRepairClaimed": False}


def run_branch(args, topology, nodes, branch, donor):
    directory = args.evidence / f"{topology}-{branch}"
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile="schema-refusal")
    candidate = json.loads(args.candidate.read_text())
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    require(candidate["source_sha"] == CONTRACT["candidateCommit"], "unreviewed C09 parser/refusal contract")
    fleet.record("candidate-schema-contract", CONTRACT)
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    lab = binding(args, candidate, nodes, run_id, bucket)
    fleet.record("binding", lab)
    probe, active, metadata_faults = Probe(lab), [], {}
    replacement_node, replacement_dirty = None, False
    result = {"topology": topology, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        expected = seed(Recorder(probe, directory / "fixture"), bucket, "DB", OBJECTS, [SIZE])
        baseline = fleet.record("S3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("fixture-ACK-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        fleet.stop()
        before = fleet.snapshot("original-complete-physical", quiescent=True)
        fleet.record("actual-original-topology", validate_topology(before, topology))
        cohorts = [map_target(before, bucket, entry["key"], "data", 1) for entry in expected]
        fleet.record("all-original-streaming-proof", verify_part_manifest(fleet, cohorts, "original-part", count=OBJECTS))
        target = cohorts[8]
        selected = [expected[8]]
        require(target["key"] == selected[0]["key"], "C09 target lost its registered ACK order")
        if branch == "future-object-schema":
            targets = [metadata_target(before, {**target, **shard}) for shard in target["cohort"]]
            receipts = []
            for index, fault in enumerate(targets):
                params = {"disk": fault["disk"], "faultId": bounded_fault_id("C09", branch, index)}
                # Register before mutation so a lost SSH response still restores
                # the exact intent and retains an unresolved namespace on failure.
                fleet.faults.append((fault["node"], params))
                receipt = fleet.record("future-envelope-injection", {"node": fault["node"], **fleet.remote(fault["node"],
                    "apply-fault", **params, relativePath=fault["relativePath"], kind="future-xl-schema", expectedSha256=fault["sha256"])})
                receipts.append(receipt)
                metadata_faults.setdefault(fault["node"], {})[f"{fault['disk']}:{fault['relativePath']}"] = receipt["fault"]
            faulted = fleet.snapshot("actual-future-envelope-scene", quiescent=True, metadata_faults=metadata_faults)
            fleet.record("future-envelope-witness", metadata_scene(before, faulted, targets, receipts))
            fleet.start()
            for endpoint in lab["endpoints"]:
                observed = fleet.record("unknown-target-GET", Probe(lab, endpoint).s3("get_object", {"Bucket": bucket, "Key": target["key"]}))
                fleet.record("unknown-target-refusal", unsupported_get(observed))
                control = fleet.record("unaffected-control-GET", Probe(lab, endpoint).s3("get_object", {"Bucket": bucket, "Key": expected[0]["key"]}))
                fleet.record("unaffected-control-ACK", exact_get(control, expected[0]))
            status = run_task(probe, fleet, target, 2, active)
            fleet.record("explicit-unknown-schema-responsibility", validate_failed_version(status, bucket, selected, target))
            fleet.stop()
            scene = fleet.snapshot("refused-unknown-records-preserved", quiescent=True, metadata_faults=metadata_faults)
            fleet.record("negative-full-physical-proof", metadata_scene(before, scene, targets, receipts))
            require(physical_content(faulted) == physical_content(scene), "GET/deep Heal changed an unknown record or control")
            for node, params in tuple(fleet.faults):
                fleet.record("negative-record-rescue", {"node": node, **fleet.remote(node, "recover-fault", **params)})
                fleet.faults.remove((node, params))
            metadata_faults.clear()
        else:
            member = next(shard for shard in target["cohort"] if shard["node"] == nodes[-1])
            target = {**target, **member}
            node, disk = member["node"], member["disk"]
            original_format = next(item for item in before[node]["formats"] if item["disk"] == disk)
            fleet.start()
            original = member_state(fleet.record("original-member-Admin", probe.admin("/rustfs/admin/v3/info", [], b"", method="GET")),
                                    target, len(target["cohort"]))
            fleet.stop()
            replacement_node = node
            fleet.record("one-owned-original-root-quarantined", fleet.remote(node, "replace-drive", disk=disk,
                expectedFormatSha256=original_format["sha256"]))
            if branch != "future-format": fleet.record("released-foreign-donor", donor)
            injected = fleet.record("exact-schema-format-injection", fleet.remote(node, "format-schema-fault", disk=disk,
                branch=branch, donor=donor if branch != "future-format" else None))
            foreign = {"sha256": injected["injectedSha256"], "value": injected["injectedValue"]}
            fleet.record("registered-format-refusal-window", {"seconds": OBSERVATION_SECONDS,
                "automaticHealDisabled": True, "directoryTestRootFallback": True, "actualDeviceReplacementClaimed": False,
                "startupFailureAloneEarnsPass": False})
            fleet.start()
            fleet.record("original-root-inaccessible-to-candidate", fleet.remote(node, "replacement-probe"))
            deadline, samples = time.monotonic() + OBSERVATION_SECONDS, []
            while True:
                state = fleet.record("injected-format-window", fleet.remote(node, "replacement-state"))
                current = next(item for item in state["formats"] if item["disk"] == disk)
                require(state.get("originalArchiveUnchanged") is True and current.get("sha256") == foreign["sha256"]
                        and current.get("value") == foreign["value"], "candidate altered or reidentified foreign/future format")
                samples.append(refusal(fleet.record("structured-format-refusal", probe.admin("/rustfs/admin/v3/info", [], b"", method="GET")),
                    target, original["endpoint"], foreign))
                if time.monotonic() >= deadline: break
                time.sleep(5)
            require(len(samples) >= 3, "format refusal did not span its registered window")
            status = run_task(probe, fleet, target, 2, active)
            fleet.record("deep-Heal-format-responsibility", format_outcome(status, bucket, selected, target))
            for endpoint in lab["endpoints"]:
                observed = fleet.record("surviving-authority-target-GET", Probe(lab, endpoint).s3("get_object", {"Bucket": bucket, "Key": target["key"]}))
                fleet.record("surviving-authority-ACK-proof", exact_get(observed, selected[0]))
            # GET and deep Heal must both preserve the injected identity and all
            # survivors. Only then can the original root be used for rescue.
            fleet.stop()
            scene = fleet.snapshot("refused-format-complete-physical", quiescent=True)
            fleet.record("format-negative-full-physical-proof", negative_scene(before, scene, (node, disk), "foreign-format", foreign))
            restored = fleet.record("original-identity-negative-rescue", fleet.remote(node, "restore-replacement"))
            require(restored.get("originalRestoredExactly") is True and restored.get("productRepairClaimed") is False,
                    "C09 rescue differs from the original or claimed product repair")
            replacement_node = None
            result["refusalSamples"] = len(samples)
        recovered = fleet.snapshot("exact-original-identity-recovered", quiescent=True)
        require(physical_content(before) == physical_content(recovered), "C09 rescue changed any original record or payload")
        fleet.record("all-recovered-streaming-proof", verify_part_manifest(fleet, cohorts, "recovered-part", count=OBJECTS))
        fleet.start()
        for endpoint in lab["endpoints"]:
            inventory = fleet.record("all-peer-complete-S3", {"endpoint": endpoint,
                **Probe(lab, endpoint).inventory(bucket, unversioned_current_tags=True)})
            fleet.record("all-peer-independent-ACK-proof", {**verify_seed(expected, inventory), **compare_inventories(baseline, inventory)})
        fleet.stop()
        require(physical_content(before) == physical_content(fleet.snapshot("after-recovery-S3-physical", quiescent=True)),
                "recovery verification repaired records that the negative proof had not preserved")
        result.update(verdict="PASS", all128ACKsPreserved=True, fullPhysicalAndStreamingAndS3Verified=True,
                      backupRestorationUsed=True, productRepairClaimed=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try:
                fleet.stop()
                fleet.snapshot("C09-failure-physical", quiescent=True, metadata_faults=metadata_faults)
                if replacement_node:
                    fleet.record("C09-failure-original-archive", fleet.remote(replacement_node, "replacement-state"))
            except Exception as evidence_error:
                result["failureEvidenceError"] = str(evidence_error)
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-error", {"error": str(exc)})
        if replacement_node:
            try:
                fleet.stop()
                fleet.record("failed-format-original-rescue", fleet.remote(replacement_node, "restore-replacement"))
            except Exception as exc:
                replacement_dirty = True
                fleet.record("failed-format-recovery-DIRTY", {"error": str(exc), "namespacesRetained": True})
        result["cleanup"] = "PASS" if not replacement_dirty and fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--case-id", choices=(CASE_ID,), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    parser.add_argument("--rc-bin", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--tool-directory", type=Path, required=True)
    args = parser.parse_args()
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(nodes) == len(set(nodes))
            and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal nodes required")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha))
            and bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "verified workflow candidate required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "C09 executor omitted a canonical source branch")
    results, donor_failure = [], None
    try:
        donor = foreign_donor(args, nodes[0])
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, selected, branch, donor))
                if results[-1]["cleanup"] != "PASS": break
            if results[-1]["cleanup"] != "PASS": break
    except (Exception, KeyboardInterrupt) as exc:
        donor_failure = {"type": type(exc).__name__, "message": str(exc)}
    finally:
        complete = len(results) == 8 and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results, "donorFailure": donor_failure,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "C09 requires all four branches on both H1/H2, a released donor and complete recovery")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal foreign/future schema validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
