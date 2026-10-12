#!/usr/bin/env python3
"""D08: same-role ciphertext reconstruction and independently decrypted ACKs."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys

try:
    from .heal_probe import Probe, compare_inventories, payload, require, save
    from .heal_workload import Recorder, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .shards import _map_external_target, files, run_task, validate_repaired_physical, validate_fault, verify_cohort
    from .versions import validate_version_receipts
    from .unformatted import binding
    from .authorization import iam_exchange
    from .schema import exact_get
except ImportError:
    from heal_probe import Probe, compare_inventories, payload, require, save
    from heal_workload import Recorder, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import _map_external_target, files, run_task, validate_repaired_physical, validate_fault, verify_cohort
    from versions import validate_version_receipts
    from unformatted import binding
    from authorization import iam_exchange
    from schema import exact_get


CASE_ID = "heal.encrypted-objects"
BRANCHES = ("sse-s3", "sse-kms", "sse-c", "encrypted-versioned")
COUNT = 16
SIZE = 5 * 1024**2 + 123
CANDIDATE = "072b57b37559fbe626aeda8abf61617fdcbdd926"
REVIEWED_CANDIDATES = {
    CANDIDATE: {},
    # This source keeps the original storage/KMS geometry and fixes the
    # customer-key PUT/HEAD/GET response projection without changing the oracle.
    "16cf19817e0c81e24d0d787a916f734a34517ec5": {
        "rustfs/src/app/object/put.rs": "01256c2995f04845840d28c8434cdfaca072766f88693a7d8cb8d24ed78463c7",
        "rustfs/src/app/object/head.rs": "d3ed1e0dcb548dbcf80769fb6ea8e37912e76a59c85ae75628baea36dab542f7",
        "rustfs/src/app/object/get.rs": "e2ce8101de7e50b4941bb6ecca36440f439cc3212d2be50f79380d212d2ed704",
    },
}
REFERENCE = {"candidateCommit": CANDIDATE, "dependency": "fresh run-owned static AES-256 KMS key on every candidate node",
    "sourceFiles": {
        "crates/kms/src/config.rs": "7931bb4c6757e60ca3e7fdbff4ac56c5fab59d6d6df8756c9d0287ca692a26f9",
        "crates/kms/src/backends/static_kms.rs": "a5cf1cbe4251f79221d171c58b3844b6aa7f073a4173da0783a6d9208c5b580a",
        "rustfs/src/storage/sse.rs": "37ecffdb4dd18a0528014afd30237c8772c3fb7f27fe2c4cc018e6d475a29641",
        "crates/e2e_test/src/kms/kms_authorization_negative_matrix_test.rs": "d64724dbec366fb54eabcfa80cd1b11448a11174f12c7c801107d37b10825643"},
    "ciphertextPolicy": "single uncompressed encrypted part; decoded ciphertext size and original plaintext actualSize are distinct",
    "negativeKeyPolicy": "SSE-C missing/wrong customer key; SSE-KMS absent/other-key decrypt grant; explicit per-key enforcement",
    "managedS3ReadKey": "service-managed; S3 caller does not supply a decryption key"}
SSEC = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": "$HEAL_SSE_C_KEY"}


def mode_for(branch):
    require(branch in BRANCHES, "unrecognized D08 source branch")
    return "sse-kms" if branch == "encrypted-versioned" else branch


def put_options(branch, key_id):
    mode = mode_for(branch)
    if mode == "sse-c": return dict(SSEC)
    if mode == "sse-s3": return {"ServerSideEncryption": "AES256"}
    return {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": key_id}


def encryption_headers(response, branch, key_id):
    mode = mode_for(branch)
    if mode == "sse-c":
        key_md5 = base64.b64encode(hashlib.md5(os.environ["HEAL_SSE_C_KEY"].encode(), usedforsecurity=False).digest()).decode()
        require(response.get("SSECustomerAlgorithm") == "AES256" and response.get("SSECustomerKeyMD5") == key_md5
                and not response.get("ServerSideEncryption"), "SSE-C ACK/GET did not preserve its actual encryption classification")
    else:
        require(response.get("ServerSideEncryption") == ("AES256" if mode == "sse-s3" else "aws:kms"),
                "encrypted ACK/GET silently downgraded its server encryption mode")
        if mode == "sse-kms": require(response.get("SSEKMSKeyId") == key_id, "KMS object used another key identity")
    return {"encrypted": True, "mode": mode, "keyMaterialLogged": False}


def cipher_part_policy(expected):
    plaintext = expected["payload"]["size"]
    require(type(plaintext) is int and plaintext > 1024**2 and not expected["deleteMarker"], "D08 requires its full external plaintext ACK")
    def checked(version, part):
        require(len(version.get("parts", [])) == 1 and part.get("number") == 1
                and type(part.get("size")) is int and type(part.get("actualSize")) is int
                and part["actualSize"] == plaintext and plaintext < part["size"] <= plaintext + 1024**2
                and version.get("size") == part["size"] and version.get("inline") is None,
                "ciphertext geometry is not the reviewed uncompressed single-part encoding of this plaintext ACK")
    return checked


def map_cipher_target(snapshot, bucket, expected, role):
    target = _map_external_target(snapshot, bucket, expected["key"], role, 1, expected["versionId"],
        latest_required=expected["isLatest"], part_policy=cipher_part_policy(expected))
    return {**target, "encryptedPhysicalLayer": True, "plaintextACK": expected["payload"],
            "ciphertextPartSize": target["logical"]["parts"][0]["size"]}


def denied_key(observed, *, customer):
    expected = (400, "InvalidRequest") if customer else (403, "AccessDenied")
    response = observed.get("response") or {}
    require(observed.get("acknowledged") is False and observed.get("resultUnknown") is False and "payload" not in observed
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == expected[0]
            and response.get("Error", {}).get("Code") == expected[1],
            "missing/wrong decryption authority returned data, absence or an unreviewed error")
    return {"httpStatus": expected[0], "code": expected[1], "missingOrWrongKeyIsNotObjectAbsence": True}


def seed_encrypted(recorder, bucket, branch, key_id):
    recorder.call("create_bucket", {"Bucket": bucket})
    versioned = branch == "encrypted-versioned"
    if versioned:
        recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
        require(recorder.call("get_bucket_versioning", {"Bucket": bucket}).get("Status") == "Enabled", "encrypted versioning was not enabled")
    expected, identities = [], set()
    for index in range(COUNT):
        key = f"encrypted/{index:04d}.bin"
        for generation in range(3 if versioned else 1):
            body = payload(f"{bucket}/{key}/{generation}", SIZE + generation * 37)
            observed = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "ContentType": "application/octet-stream", "Metadata": {"heal-fixture": "DE", "generation": str(generation)},
                "Tagging": "suite=heal&fixture=DE", **put_options(branch, key_id)}, body)
            encryption_headers(observed, branch, key_id)
            version = observed.get("VersionId", "null")
            require((versioned and isinstance(version, str) and version not in {"", "null"})
                    or not versioned and version in {None, "null"}, "encrypted PUT omitted its exact expected version identity")
            version = version or "null"
            require((key, version) not in identities, "encrypted ACK reused a logical version")
            identities.add((key, version))
            expected.append({"key": key, "versionId": version, "isLatest": not versioned or generation == 2,
                "deleteMarker": False, "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
    return expected


def principal_setup(probe, fleet, bucket, key_id, context):
    suffix = hashlib.sha256(probe.binding["runId"].encode()).hexdigest()[:14]
    for index, grant in enumerate((None, key_id + "-other")):
        user, policy = "rvheal" + suffix + f"encrypted{index}", "rvheal" + suffix + f"keypolicy{index}"
        secret = secrets.token_hex(24)
        fleet.secret_values.update({user, secret})
        item = {"user": user, "policy": policy, "attached": False, "policyCreated": False,
                "principal": Probe(probe.binding, credential_pair=(user, secret)), "grant": grant}
        context.append(item)
        iam_exchange(probe, fleet, "add-user", user, secret=secret)
        statements = [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                       "Resource": [f"arn:aws:s3:::{bucket}/*", f"arn:aws:s3:::{bucket}-control/*"]}]
        if grant:
            statements.append({"Effect": "Allow", "Action": ["kms:Decrypt"], "Resource": ["arn:aws:kms:::key/" + grant]})
        item["policyCreated"] = True
        iam_exchange(probe, fleet, "add-policy", user, policy=policy, document={"Version": "2012-10-17", "Statement": statements})
        item["attached"] = True
        iam_exchange(probe, fleet, "attach", user, policy=policy)


def key_negatives(probe, fleet, bucket, branch, entry, control, context, label):
    params = {"Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]}
    proofs = []
    if mode_for(branch) == "sse-c":
        for name, options in (("missing", {}), ("wrong", {**SSEC, "SSECustomerKey": "$HEAL_SSE_C_WRONG_KEY"})):
            observed = fleet.record(label + "-customer-key-" + name, probe.s3("get_object", {**params, **options}))
            proofs.append({"variant": name, **denied_key(observed, customer=True)})
    elif mode_for(branch) == "sse-kms":
        for item in context:
            principal = item["principal"]
            readable = fleet.record(label + "-same-principal-plaintext-control", principal.s3("get_object", {
                "Bucket": bucket + "-control", "Key": control["key"]}))
            exact_get(readable, control)
            observed = fleet.record(label + "-KMS-grant-denied", principal.s3("get_object", params))
            proofs.append({"variant": "missing-grant" if item["grant"] is None else "other-key-grant", **denied_key(observed, customer=False),
                "samePrincipalS3ReadAllowed": True})
    else:
        # SSE-S3 does not take a customer key on GET. Test its positive public
        # classification; inventing a missing client key would be false coverage.
        proofs.append({"variant": "service-managed-key", "callerKeyArgumentApplicable": False})
    return proofs


def run_branch(args, topology, nodes, branch):
    directory = args.evidence / f"{topology}-{branch}"
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory)
    candidate = json.loads(args.candidate.read_text())
    require(candidate["source_sha"] in REVIEWED_CANDIDATES, "unreviewed encrypted geometry/KMS policy contract")
    fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    fleet.record("encryption-contract", {**REFERENCE, "candidateCommit": candidate["source_sha"],
        "reviewedResponseSourceFiles": REVIEWED_CANDIDATES[candidate["source_sha"]]})
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    key_id, material = "rv-heal-key-" + run_id, base64.b64encode(secrets.token_bytes(32)).decode()
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    customer, wrong = secrets.token_hex(16), secrets.token_hex(16)
    require(customer != wrong, "negative customer key duplicated the valid key")
    fleet.secret_values.update({material, key_id + ":" + material, customer, wrong,
        base64.b64encode(customer.encode()).decode(), base64.b64encode(wrong.encode()).decode()})
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret, HEAL_SSE_C_KEY=customer, HEAL_SSE_C_WRONG_KEY=wrong)
    lab = binding(args, candidate, nodes, run_id, bucket)
    fleet.record("binding", lab)
    probe, active, context = Probe(lab), [], []
    fleet.snapshot_buckets = [bucket, bucket + "-control"]
    result = {"topology": topology, "branch": branch, "verdict": "RUNNING", "cleanup": "PENDING"}
    iam_dirty = False
    try:
        def configure():
            for node in nodes:
                fleet.record("dedicated-KMS-configured", {"node": node, **fleet.remote(node, "encryption-configure", keyId=key_id, keyBase64=material)})
        fleet.deploy(args.binary_sha, bucket, access, secret, before_start=configure)
        recorder = Recorder(probe, directory / "fixture")
        expected = seed_encrypted(recorder, bucket, branch, key_id)
        read_options = SSEC if mode_for(branch) == "sse-c" else {}
        baseline = fleet.record("decrypted-S3-baseline", probe.inventory(bucket,
            unversioned_current_tags=branch != "encrypted-versioned", object_read_options=read_options))
        fleet.record("independent-plaintext-ACKs", {"expected": expected, **verify_seed(expected, baseline)})
        for entry in baseline["entries"]: encryption_headers(entry["properties"], branch, key_id)
        control_bucket = bucket + "-control"
        recorder.call("create_bucket", {"Bucket": control_bucket})
        control_body = payload(control_bucket + "/unaffected", SIZE)
        recorder.call("put_object", {"Bucket": control_bucket, "Key": "unaffected.bin"}, control_body)
        control = {"key": "unaffected.bin", "versionId": "null", "deleteMarker": False, "isLatest": True,
            "payload": {"size": len(control_body), "sha256": hashlib.sha256(control_body).hexdigest()}}
        control_baseline = fleet.record("plaintext-control-baseline", probe.inventory(control_bucket, unversioned_current_tags=True))
        verify_seed([control], control_baseline)
        require(all(not any(name in entry["properties"] for name in
            ("ServerSideEncryption", "SSEKMSKeyId", "SSECustomerAlgorithm")) for entry in control_baseline["entries"]),
            "key-authority control is not an independently readable plaintext object")
        if mode_for(branch) == "sse-kms": principal_setup(probe, fleet, bucket, key_id, context)
        selected = [next(entry for entry in expected if entry["key"] == key and (entry["isLatest"] if index else True))
                    for index, key in enumerate(("encrypted/0002.bin", "encrypted/0004.bin"))]
        result["baselineKeyNegatives"] = key_negatives(probe, fleet, bucket, branch, selected[0], control, context, "baseline")
        # Each peer must already serve the correct decrypted version before a
        # shard fault. This proves actual key propagation and service readiness.
        for endpoint in lab["endpoints"]:
            observed = fleet.record("all-peer-dependency-readiness", Probe(lab, endpoint).s3("get_object", {
                "Bucket": bucket, "Key": selected[0]["key"], "VersionId": selected[0]["versionId"], **read_options}))
            exact_get(observed, selected[0]); encryption_headers(observed["response"], branch, key_id)
        fleet.stop()
        before = fleet.snapshot("original-ciphertext-full-physical", quiescent=True)
        fleet.record("actual-topology", validate_topology(before, topology))
        require(all(before[node]["encryptionConfigAttestation"]["runOwnedSecretFileUnchanged"] for node in nodes), "dedicated KMS identity changed")
        targets = [map_cipher_target(before, bucket, entry, "data" if index == 0 else "parity") for index, entry in enumerate(selected)]
        for target in targets: fleet.record("original-ciphertext-streaming", verify_cohort(fleet, target, "original-ciphertext-part"))
        prior = before
        registrations = []
        for index, target in enumerate(targets):
            params = {"disk": target["disk"], "faultId": f"D08-{branch}-{index}"}
            registrations.append((target["node"], params)); fleet.faults.append(registrations[-1])
            receipt = fleet.record("ciphertext-fault", fleet.remote(target["node"], "apply-fault", **params,
                relativePath=target["relativePath"], kind="missing" if index == 0 else "bitflip", expectedSha256=target["sha256"],
                offset=None if index == 0 else target["size"] // 2))
            changed = fleet.snapshot("witnessed-ciphertext-fault", quiescent=True)
            validate_fault(prior, changed, target, receipt); prior = changed
        fleet.start()
        require(physical_content(prior) == physical_content(fleet.snapshot("ciphertext-before-manual-token")),
                "ciphertext repaired before its attributed manual token")
        status = run_task(probe, fleet, {"bucket": bucket, "key": ""}, 2, active, option_overrides={"recursive": True},
                          status_endpoints=tuple(lab["endpoints"])[1:])
        result["responsibility"] = validate_version_receipts(status, bucket, expected, 2,
            required_repairs={(entry["key"], entry["versionId"]) for entry in selected})
        fleet.stop()
        repaired = fleet.snapshot("ciphertext-repaired-before-plaintext-GET", quiescent=True)
        result["physical"] = validate_repaired_physical(before, repaired, targets[0], repair_targets=targets)
        for target in targets: fleet.record("reconstructed-ciphertext-streaming", verify_cohort(fleet, target, "repaired-ciphertext-part"))
        for registration in registrations: fleet.faults.remove(registration)
        fleet.start()
        result["postRepairKeyNegatives"] = key_negatives(probe, fleet, bucket, branch, selected[0], control, context, "post-repair")
        for endpoint in lab["endpoints"]:
            entry_probe = Probe(lab, endpoint)
            inventory = fleet.record("all-peer-full-decrypted-manifest", {"endpoint": endpoint, **entry_probe.inventory(bucket,
                unversioned_current_tags=branch != "encrypted-versioned", object_read_options=read_options)})
            fleet.record("all-peer-plaintext-and-properties", {**verify_seed(expected, inventory), **compare_inventories(baseline, inventory)})
            for entry in inventory["entries"]: encryption_headers(entry["properties"], branch, key_id)
            controls = fleet.record("all-peer-plaintext-controls", entry_probe.inventory(control_bucket, unversioned_current_tags=True))
            compare_inventories(control_baseline, controls)
        fleet.stop()
        require(physical_content(repaired) == physical_content(fleet.snapshot("ciphertext-after-decryption-and-negatives", quiescent=True)),
                "decryption verification changed or downgraded the proven ciphertext repair")
        result.update(verdict="PASS", allPlaintextACKsAndVersionsPreserved=True, sameRoleCiphertextRestored=True,
                      keyMaterialLogged=False, sharedKmsTouched=False, backupRestorationUsed=False)
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try: fleet.stop(); fleet.snapshot("D08-failure-ciphertext-scene", quiescent=True)
            except Exception as capture: result["failureEvidenceError"] = str(capture)
    finally:
        for path, token in active:
            try: fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc: fleet.record("stop-error", {"error": str(exc)})
        if context:
            try: fleet.stop(); fleet.start()
            except Exception as exc: iam_dirty = True; fleet.record("IAM-recovery-start-failed", {"error": str(exc)})
            if not iam_dirty:
                for item in context:
                    for operation, needed in (("detach", item["attached"]), ("remove-user", True), ("remove-policy", item["policyCreated"])):
                        if not needed: continue
                        try: iam_exchange(probe, fleet, operation, item["user"], policy=item["policy"] if operation != "remove-user" else None)
                        except Exception as exc: iam_dirty = True; fleet.record("IAM-recovery-DIRTY", {"operation": operation, "error": str(exc)})
        if iam_dirty:
            fleet.stop()
            fleet.record("retained-key-authority-environment", {"state": "DIRTY", "namespacesRetained": True})
        result["cleanup"] = "PASS" if not iam_dirty and fleet.cleanup() else "FAIL"
        fleet.record("branch-result", result)
    return result


def main():
    def interrupted(signum, frame): raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "rc-bin", "evidence", "tool-directory"): parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case-id", choices=(CASE_ID,), required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    args = parser.parse_args()
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes)
            and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal nodes required")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha))
            and bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "verified workflow candidate required")
    require(select(CASE_ID)[0]["variants"] == list(BRANCHES), "D08 executor omitted a canonical encryption branch")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for branch in BRANCHES:
                results.append(run_branch(args, topology, selected, branch))
                if results[-1]["cleanup"] != "PASS": break
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == 8 and all(item["verdict"] == "PASS" and item["cleanup"] == "PASS" for item in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE_ID, "results": results,
            "cleanup": "PASS" if results and all(item["cleanup"] == "PASS" for item in results) else "FAIL", "complete": complete})
    require(complete, "D08 requires all four encryption branches on H1/H2 with exact ciphertext/plaintext/key-negative proofs")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal encrypted object validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
