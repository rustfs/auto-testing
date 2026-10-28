#!/usr/bin/env python3
"""Exact candidate manager regressions for persistence and incarnation guards."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

try:
    from . import native_regressions as native
except ImportError:
    import native_regressions as native


SOURCE_TREE = "8071fb0a9e5353eb7d3fa58c8a58c8622473f2bd"
LOCK_SHA = "d8f7bc416ba67d19873d34e9389e5aa823b22d7d7552706a5a810cee26f6eda2"
REVIEWED_CANDIDATES = {
    native.CANDIDATE: {"sourceTree": SOURCE_TREE, "cargoLockSha256": LOCK_SHA},
    # The manager, checkpoint, MRF and outcome sources are byte-identical to
    # the original reviewed candidate; this source adds the validation fixes
    # elsewhere in the RustFS tree.
    "16cf19817e0c81e24d0d787a916f734a34517ec5": {
        "sourceTree": "7edb1a2d02d60323cdfab4173485c33cb06b297a",
        "cargoLockSha256": LOCK_SHA,
    },
    "e224eb48e5311cff022b04fd78c97a8572d5c270": {
        "sourceTree": "098d5486f801e689ef30c00e5cedb775f0f1be0d",
        "cargoLockSha256": LOCK_SHA,
    },
    "1aa9be87d18cf941e5020f19e8622c26ba4b10ac": {
        "sourceTree": "d57bcda8b1551727ced7073ddd09f8c97c13485f",
        "cargoLockSha256": LOCK_SHA,
    },
}
SOURCE_FILES = {
    "crates/heal/src/heal/resume/tests.rs": "cea1558f8659f01968a5592af55cd0626f1d309c9fecfa0bee26c36775229900",
    "crates/heal/src/heal/resume/checkpoint.rs": "fe067b39ee406e16517bd665f7f49457daffa05d5d7bc298bd965e927b1381c1",
    "crates/heal/src/heal/mrf_queue/snapshot.rs": "ecfcb5ee63edef5c5d3d6713edc127e16f489522afc5fc02dc1044ccca241d14",
    "crates/heal/src/heal/mrf_queue/snapshot/migration.rs": "9163c87bcdee1439667f79aea2a36f6ffce14c6d7c64be0786306dd3a75478b9",
    "crates/heal/src/heal/manager/tests/root_recovery.rs": "7c4a0cb7834390f053737a8950f24e070a01e1c74583d9db206ea1b620385594",
    "crates/heal/src/heal/manager/root_recovery.rs": "ba6beba9d9d14985e4c5448d85727036388d3d72f28879ac5c67fc5f760048ba",
    "crates/heal/src/heal/erasure_healer/resume_loop_tests/admin_outcome.rs": "f2251a9ab5f2df10a2299cd23bb3231615d946ce95a104002088f06235dde621",
    "crates/heal/src/heal/storage.rs": "52ce6edff5b64b69a0d0c2a425b980984cf927fab79139e33d5ff9c7ad98efe9",
}
# Native prerequisites only; fleet executors retain their own candidate gates.
REVIEWED_CANDIDATES["1c7fc818dd858247c45b39542abd8291b1e6e31b"] = {
    "sourceTree": "9713c550ce148e1e32004f166a19e3a67eb83434",
    "cargoLockSha256": "2589b3f5afc54f94419a3e2bfd537c11fae7ae3ee2d2a73ba31c72ee8d8a62be",
    "sourceFiles": {**SOURCE_FILES,
        "crates/heal/src/heal/storage.rs": "0e27cc1fbb7b732d211e744f385223bee23844c95ba86918e1765b546c197096"},
}
REVIEWED_CANDIDATES["7951bb629821246868ee3761b321dd07c06db45c"] = {
    "sourceTree": "e4d863efff2792c0ab4da0036c04c3e0902aca89",
    "cargoLockSha256": "2589b3f5afc54f94419a3e2bfd537c11fae7ae3ee2d2a73ba31c72ee8d8a62be",
    "sourceFiles": {**REVIEWED_CANDIDATES["1c7fc818dd858247c45b39542abd8291b1e6e31b"]["sourceFiles"],
        "crates/heal/src/heal/manager.rs": "6e41b39d74aedf3b40b6c6a8f7c5de60bf2cb7927d7d76a10745033002dbc00d",
        "crates/heal/src/heal/manager/tests.rs": "844b84870e85fca4de1c72faf66e4f7a43561d8d6db341db43fd089223ff9a1e",
        "crates/heal/tests/shard_identity_receipt_test.rs": "51ffe71b57efceb03ef40aa2333e235c5515149c89b64df0553dbe26b331db32"},
}
RESUME = "heal::resume::tests::"
SNAPSHOT = "heal::mrf_queue::snapshot::tests::"
ROOT = "heal::manager::tests::root_recovery::"
OUTCOME = "heal::erasure_healer::resume_loop_tests::admin_outcome::"


def spec(test, cases, assertions, kind="deterministic candidate guard assertions"):
    return {"test": test, "relatedCases": cases, "sourceAssertions": assertions,
            "fenceKind": kind, "fleetPassClaimed": False, "physicalPowerLossClaimed": False,
            "emittedBarrierLogsClaimed": False}


F07 = ["heal.checkpoint-and-mrf-corruption"]
C11 = ["heal.bucket-incarnation-recreation"]
J02 = ["heal.outcome-counters-and-error-truth"]
REGRESSIONS = {
    "checkpoint-truncated": spec(RESUME + "checkpoint_save_does_not_replace_a_non_empty_truncated_snapshot", F07,
        ["save fails closed", "original truncated bytes remain", "task is blocked"]),
    "checkpoint-future": spec(RESUME + "checkpoint_save_does_not_replace_a_future_schema_snapshot", F07,
        ["future schema save fails closed", "unknown bytes remain", "task is blocked"]),
    "checkpoint-digest": spec(RESUME + "checkpoint_digest_rejects_same_length_progress_tampering", F07,
        ["same-length progress tampering fails load", "task is blocked"]),
    "checkpoint-stale-manager": spec(RESUME + "stale_checkpoint_manager_cannot_overwrite_newer_progress", F07,
        ["stale manager save is rejected", "reopened newer bucket/object cursor remains 4/20"]),
    "checkpoint-page-order": spec(RESUME + "checkpoint_page_commit_keeps_ledger_until_cursor_is_durable", F07,
        ["reload preserves completed page cursor, counters and ledger", "ledger prunes only after durable cursor"],
        "real checkpoint publication and reload ordering; no injected fsync interruption"),
    "mrf-payload-integrity": spec(SNAPSHOT + "whole_payload_integrity_is_required_even_with_a_valid_manifest", F07,
        ["valid outer manifest cannot hide damaged full-record integrity"]),
    "mrf-manifest-write-failure": spec(SNAPSHOT + "committed_snapshot_writer_manifest_failure_preserves_previous_anchor", F07,
        ["actual successor manifest path blocks publication", "error remains visible", "reopened previous payload and manifest are unchanged"]),
    "mrf-payload-write-failure": spec(SNAPSHOT + "committed_snapshot_writer_payload_failure_does_not_publish_manifest", F07,
        ["actual successor payload path blocks staging", "error remains visible", "no successor manifest", "previous anchor survives reload"]),
    "mrf-migration-boundaries": spec("heal::mrf_queue::snapshot::migration::tests::migration_commit_boundaries_and_lost_response_recover_idempotently", F07,
        ["actual AfterPayload/BeforeManifest/AfterManifest/AfterReadback interruptions are injected",
         "source bytes survive every interruption", "uncommitted recovery is refused", "same owner retry recovers identical records"],
        "four actual migration publication interruption hooks; no physical power-loss or individual fsync-hook claim"),
    "root-uncertain-publication": spec(ROOT + "root_recovery_uncertain_terminal_publication_preserves_commit_and_pending_fence", F07,
        ["failure is injected after terminal write", "pending owner remains", "restart fences stale runnable work and restores committed outcome"],
        "actual candidate terminal publication failure hook"),
    "incarnation-root-replay": spec(ROOT + "bucket_incarnation_recovery_retires_old_owner_and_admits_successor", C11,
        ["same incarnation replays original token", "new incarnation rejects old owner", "new token admits successor",
         "reintroduced original pending bytes cannot defeat durable stale terminal"]),
    "incarnation-deferred-metadata": spec(ROOT + "bucket_incarnation_metadata_failure_defers_replay_without_retiring_owner", C11,
        ["metadata lookup failure preserves original pending owner instead of retiring or rebinding"]),
    "incarnation-checkpoint": spec(OUTCOME + "admin_erasure_recreated_bucket_cannot_rebind_checkpoint", C11,
        ["recreated incarnation returns StaleBucketIncarnation", "storage calls do not advance", "acknowledged healed counter is retained"]),
    "outcome-truth": spec(OUTCOME + "admin_erasure_dispositions_do_not_invent_success_and_retries_count_once", J02,
        ["verified/absent/unproven/transient cases remain distinct", "exact 5/0/2/0/3/1 counters", "eight attempt failures remain visible",
         "exhausted missing-version retries remain deferred"]),
    "outcome-wrong-identity": spec(OUTCOME + "admin_erasure_rejects_every_receipt_identity_mismatch", J02,
        ["bucket/object/version/incarnation/pool/set mismatches each yield unknown and zero healed"]),
}
BUILD_ENV = {"CARGO_BUILD_JOBS": "2", "CARGO_INCREMENTAL": "0",
             "CARGO_PROFILE_RELEASE_DEBUG": "0", "CARGO_PROFILE_RELEASE_STRIP": "debuginfo"}


def collectors():
    return {path.name: native.sha(path) for path in (Path(__file__), Path(native.__file__))}


def verify_manifest(directory):
    manifest = json.loads((directory / "manager-native.json").read_text())
    candidate = manifest.get("candidateCommit")
    contract = REVIEWED_CANDIDATES.get(candidate)
    native.require(manifest.get("schemaVersion") == 1 and contract is not None
        and manifest.get("candidateSourceTree") == contract["sourceTree"]
        and manifest.get("candidateCargoLockSha256") == contract["cargoLockSha256"]
        and manifest.get("sourceFiles") == contract.get("sourceFiles", SOURCE_FILES) and manifest.get("collectorSources") == collectors()
        and manifest.get("regressions") == REGRESSIONS and manifest.get("sourceUnmodified") is True
        and manifest.get("optimized") is True and manifest.get("buildEnvironment") == BUILD_ENV
        and manifest.get("binaryName") == "heal-manager-native-tests" and manifest.get("fleetPassClaimed") is False,
        "manager native artifact changed its reviewed source, test or collector contract")
    binary = directory / manifest["binaryName"]
    native.require(binary.is_file() and not binary.is_symlink() and native.sha(binary) == manifest.get("binarySha256"),
                   "manager native executable identity differs")
    return manifest, binary


def build(source, commit, out):
    contract = REVIEWED_CANDIDATES.get(commit)
    native.require(contract is not None and subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() == commit,
        "manager native tests require the exact reviewed candidate")
    native.require(subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"], text=True).strip() == contract["sourceTree"]
        and native.sha(source / "Cargo.lock") == contract["cargoLockSha256"]
        and all(native.sha(source / path) == digest for path, digest in contract.get("sourceFiles", SOURCE_FILES).items()),
        "manager candidate source/tree/lock differs")
    for args in (["diff", "--quiet"], ["diff", "--cached", "--quiet"]):
        native.require(subprocess.run(["git", "-C", str(source), *args]).returncode == 0, "candidate source is modified")
    native.require(not subprocess.check_output(["git", "-C", str(source), "ls-files", "--others", "--exclude-standard"], text=True).strip(),
                   "candidate contains untracked source/configuration")
    native.require(not out.exists() and shutil.disk_usage(source).free >= 12 * 1024**3, "fresh output and 12 GiB build space required")
    out.mkdir(parents=True)
    request = {"schemaVersion": 1, "candidateCommit": commit, "candidateSourceTree": contract["sourceTree"],
        "candidateCargoLockSha256": contract["cargoLockSha256"], "sourceFiles": contract.get("sourceFiles", SOURCE_FILES), "collectorSources": collectors(),
        "regressions": REGRESSIONS, "testingCommit": os.environ.get("AUTO_TESTING_SHA"),
        "runId": os.environ.get("GITHUB_RUN_ID"), "runAttempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "fleetPassClaimed": False, "sourceUnmodified": True, "optimized": True, "buildEnvironment": BUILD_ENV}
    native.save(out / "manager-build-request.json", request)
    target = out / "target"
    command = ["cargo", "test", "--locked", "--release", "-p", "rustfs-heal", "--lib", "--no-run", "--message-format=json"]
    with (out / "cargo.jsonl").open("x") as stdout, (out / "cargo.stderr").open("x") as stderr:
        result = subprocess.run(command, cwd=source, env={**os.environ, **BUILD_ENV, "CARGO_TARGET_DIR": str(target)},
                                stdout=stdout, stderr=stderr, timeout=150 * 60)
    native.require(result.returncode == 0, "manager native compilation failed; original diagnostics retained")
    executables = []
    for line in (out / "cargo.jsonl").read_text().splitlines():
        try: row = json.loads(line)
        except json.JSONDecodeError: continue
        if row.get("reason") == "compiler-artifact" and row.get("profile", {}).get("test") is True \
                and row.get("target", {}).get("name") == "rustfs_heal" and row.get("executable"):
            executables.append(Path(row["executable"]))
    native.require(len(executables) == 1 and executables[0].resolve().is_relative_to(target.resolve()), "one owned manager test executable required")
    binary = out / "heal-manager-native-tests"
    shutil.copyfile(executables[0], binary)
    binary.chmod(0o500)
    listing = subprocess.run([str(binary), "--list"], capture_output=True, text=True, timeout=60)
    (out / "test-inventory.log").write_text(listing.stdout + listing.stderr)
    native.require(listing.returncode == 0, "manager native inventory failed")
    for value in REGRESSIONS.values(): native.exact_listing(listing.stdout, value["test"])
    for args in (["diff", "--quiet"], ["diff", "--cached", "--quiet"]):
        native.require(subprocess.run(["git", "-C", str(source), *args]).returncode == 0, "build changed tracked candidate source")
    native.save(out / "manager-native.json", {**request, "binaryName": binary.name, "binarySha256": native.sha(binary),
        "binaryKind": "native-regression-not-released-service", "buildCommand": command,
        "toolchain": subprocess.check_output(["rustc", "-vV"], cwd=source, text=True)})
    shutil.rmtree(target)


def run(directory, out):
    manifest, binary = verify_manifest(directory)
    native.require(not out.exists(), "manager native evidence must be fresh")
    out.mkdir(parents=True)
    listing = subprocess.run([str(binary), "--list"], capture_output=True, text=True, timeout=60)
    (out / "runtime-inventory.log").write_text(listing.stdout + listing.stderr)
    native.require(listing.returncode == 0, "manager runtime inventory failed")
    results = []
    for key, value in REGRESSIONS.items():
        native.exact_listing(listing.stdout, value["test"])
        verify_manifest(directory)
        command = [str(binary), value["test"], "--exact", "--nocapture", "--test-threads=1"]
        record = {"regression": key, **value, "candidateCommit": manifest["candidateCommit"],
                  "binarySha256": manifest["binarySha256"], "command": command}
        started = time.monotonic_ns()
        try:
            child = subprocess.run(command, capture_output=True, text=True, timeout=120)
            (out / (key + ".log")).write_text(child.stdout + child.stderr)
            record["exitCode"] = child.returncode
            native.test_result(child.stdout + child.stderr, value["test"], child.returncode)
            record["verdict"] = "PASS"
        except subprocess.TimeoutExpired as error:
            parts = [part.decode(errors="replace") if isinstance(part, bytes) else part or "" for part in (error.stdout, error.stderr)]
            (out / (key + ".log")).write_text("".join(parts))
            record.update(verdict="FAIL", reason="fixed 120-second native deadline exceeded")
        except (OSError, ValueError) as error:
            record.update(verdict="FAIL", reason=str(error))
        record["elapsedNanoseconds"] = time.monotonic_ns() - started
        native.save(out / (key + ".json"), record)
        results.append(record)
    complete = all(row["verdict"] == "PASS" for row in results)
    native.save(out / "manager-native-summary.json", {"results": results, "complete": complete,
        "canonicalCasePassClaimed": False, "fleetPassClaimed": False, "physicalPowerLossClaimed": False})
    native.require(complete, "manager native failures remain in original evidence")


def verify_evidence(directory, evidence, candidate, run_id, run_attempt, helper_sha):
    manifest, _ = verify_manifest(directory)
    native.require(candidate["source_sha"] == manifest.get("candidateCommit")
        and manifest.get("candidateCommit") in REVIEWED_CANDIDATES and manifest.get("runId") == str(run_id)
        and manifest.get("runAttempt") == str(run_attempt) and manifest.get("testingCommit") == helper_sha
        and re.fullmatch(r"[0-9a-f]{40}", helper_sha), "manager native evidence belongs to another build attempt")
    summary = json.loads((evidence / "manager-native-summary.json").read_text())
    rows = summary.get("results", [])
    native.require(summary.get("complete") is True and summary.get("canonicalCasePassClaimed") is False
        and summary.get("fleetPassClaimed") is False and summary.get("physicalPowerLossClaimed") is False
        and len(rows) == len(REGRESSIONS) and {row.get("regression") for row in rows} == set(REGRESSIONS),
        "manager native summary is incomplete, duplicated or claims fleet success")
    listings = [(path).read_text() for path in (directory / "test-inventory.log", evidence / "runtime-inventory.log")]
    for row in rows:
        key = row["regression"]
        native.require(row == json.loads((evidence / (key + ".json")).read_text()) and row.get("verdict") == "PASS"
            and row.get("binarySha256") == manifest["binarySha256"] and row.get("candidateCommit") == manifest["candidateCommit"]
            and all(row.get(name) == value for name, value in REGRESSIONS[key].items()), "manager result changed exact scope or identity")
        command = row.get("command", [])
        native.require(len(command) == 5 and Path(command[0]).name == manifest["binaryName"]
            and command[1:] == [row["test"], "--exact", "--nocapture", "--test-threads=1"], "manager result lacks exact one-test invocation")
        for listing in listings: native.exact_listing(listing, row["test"])
        native.test_result((evidence / (key + ".log")).read_text(), row["test"], row.get("exitCode"))
    return {"complete": True, "candidateCommit": manifest["candidateCommit"], "binarySha256": manifest["binarySha256"],
            "exactTests": len(rows), "canonicalCasePassClaimed": False, "fleetPassClaimed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    compiling = commands.add_parser("build")
    compiling.add_argument("--source", type=Path, required=True)
    compiling.add_argument("--commit", required=True)
    compiling.add_argument("--out", type=Path, required=True)
    running = commands.add_parser("run")
    running.add_argument("--directory", type=Path, required=True)
    running.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "build": build(args.source.resolve(strict=True), args.commit, args.out.resolve())
    else: run(args.directory.resolve(strict=True), args.out.resolve())


if __name__ == "__main__":
    try: main()
    except Exception as error:
        print(f"manager native validation failed: {error}", file=sys.stderr)
        sys.exit(1)
