#!/usr/bin/env python3
"""Run existing F07 directory-fsync regressions from the verified candidate binary."""

import argparse
import json
import os
import re
from pathlib import Path
import subprocess
import time

try:
    from . import native_regressions as native
except ImportError:
    import native_regressions as native


PREFIX = "disk::local::test::"
REGRESSIONS = {
    "cas-update-directory-sync": (
        "conditional_file_update_dir_fsync_failure_restores_previous_bytes",
        "post-rename directory fsync failure is commit-ambiguous and restores the previous owner bytes"),
    "cas-create-directory-sync": (
        "conditional_file_update_dir_fsync_failure_removes_new_file_without_anchor",
        "post-rename directory fsync failure removes an uncommitted first owner"),
    "cas-delete-directory-sync": (
        "conditional_file_delete_dir_fsync_failure_restores_previous_bytes",
        "post-delete directory fsync failure restores the previous owner bytes"),
    "mrf-manifest-directory-sync": (
        "conditional_mrf_manifest_dir_fsync_failure_keeps_recovery_anchors",
        "actual system-metadata directory fsync is required even in relaxed mode; failed replacement and first creation retain recovery anchors"),
    "mrf-manifest-storage-full": (
        "conditional_mrf_manifest_storage_full_keeps_recovery_anchors",
        "StorageFull at the actual manifest directory fsync preserves the previous manifest and legacy journal"),
    "mrf-delete-storage-full": (
        "conditional_mrf_manifest_storage_full_delete_keeps_recovery_anchors",
        "StorageFull at cleanup directory fsync restores the committed manifest and preserves the legacy journal"),
}
LIMITS = {
    "directoryFsyncFailureTestScope": True,
    "individualFileFsyncCovered": False,
    "physicalPowerLossClaimed": False,
    "requiredNativeCoverageComplete": False,
    "canonicalCasePassClaimed": False,
    "fleetPassClaimed": False,
    "emittedBarrierLogsClaimed": False,
}


def binding(directory, evidence):
    manifest, binary, _ = native.verify_manifest(directory)
    native.require(manifest["candidateCommit"] == native.INCARNATION_CANDIDATE,
                   "F07 directory fsync tests are reviewed only for the exact immutable candidate")
    native.verify_evidence(directory, evidence, {"source_sha": manifest["candidateCommit"]},
                          manifest["runId"], manifest["runAttempt"], manifest["testingCommit"])
    listing = (directory / "test-inventory.log").read_text()
    for suffix, _ in REGRESSIONS.values(): native.exact_listing(listing, PREFIX + suffix)
    return manifest, binary


def identity(manifest, testing_commit):
    native.require(isinstance(testing_commit, str) and re.fullmatch(r"[0-9a-f]{40}", testing_commit),
                   "F07 collector requires an exact independent harness commit")
    return {"candidateCommit": manifest["candidateCommit"], "candidateSourceTree": manifest["candidateSourceTree"],
            "candidateCargoLockSha256": manifest["candidateCargoLockSha256"],
            "binarySha256": manifest["binarySha256"], "runId": manifest["runId"],
            "runAttempt": manifest["runAttempt"], "nativeTestingCommit": manifest["testingCommit"],
            "collectorCommit": testing_commit,
            "collectorSha256": native.sha(Path(__file__)), **LIMITS}


def run(directory, native_evidence, out, testing_commit):
    manifest, binary = binding(directory, native_evidence)
    proof_identity = identity(manifest, testing_commit)
    native.require(not out.exists(), "F07 native evidence must be fresh")
    out.mkdir(parents=True)
    listing = subprocess.run([str(binary), "--list"], capture_output=True, text=True, timeout=60)
    (out / "runtime-inventory.log").write_text(listing.stdout + listing.stderr)
    native.require(listing.returncode == 0, "F07 native runtime inventory failed")
    rows = []
    for key, (suffix, assertion) in REGRESSIONS.items():
        name = PREFIX + suffix
        native.exact_listing(listing.stdout, name)
        native.require(not binary.is_symlink() and native.sha(binary) == manifest["binarySha256"],
                       "F07 native executable changed before exact execution")
        command = native.exact_command(binary, name, manifest["candidateCommit"])
        row = {**proof_identity, "regression": key, "test": name, "sourceAssertion": assertion,
               "command": command, "runtimeEnvironment": manifest["runtimeEnvironment"]}
        started = time.monotonic_ns()
        try:
            child = subprocess.run(command, capture_output=True, text=True, timeout=120,
                                   env={**os.environ, **manifest["runtimeEnvironment"]})
            output = child.stdout + child.stderr
            (out / (key + ".log")).write_text(output)
            row["exitCode"] = child.returncode
            native.test_result(output, name, child.returncode)
            row["verdict"] = "PASS"
        except subprocess.TimeoutExpired as error:
            parts = [part.decode(errors="replace") if isinstance(part, bytes) else part or ""
                     for part in (error.stdout, error.stderr)]
            (out / (key + ".log")).write_text("".join(parts))
            row.update(verdict="FAIL", reason="fixed 120-second F07 native deadline exceeded")
        except (OSError, ValueError) as error:
            if not (out / (key + ".log")).exists(): (out / (key + ".log")).write_text(str(error))
            row.update(verdict="FAIL", reason=str(error))
        row["elapsedNanoseconds"] = time.monotonic_ns() - started
        native.save(out / (key + ".json"), row)
        rows.append(row)
    complete = all(row["verdict"] == "PASS" for row in rows)
    native.save(out / "persistence-native-summary.json", {**proof_identity, "complete": complete, "results": rows})
    native.require(complete, "F07 native fsync failures remain in original evidence")


def verify(directory, native_evidence, evidence, testing_commit):
    manifest, binary = binding(directory, native_evidence)
    summary = json.loads((evidence / "persistence-native-summary.json").read_text())
    rows = summary.get("results", [])
    expected_identity = identity(manifest, testing_commit)
    native.require(all(type(summary.get(key)) is type(value) and summary[key] == value
                       for key, value in expected_identity.items()) and summary.get("complete") is True
                   and len(rows) == len(REGRESSIONS) and {row.get("regression") for row in rows} == set(REGRESSIONS),
                   "F07 native summary changed source, scope, limitations or collector identity")
    listings = [(directory / "test-inventory.log").read_text(), (evidence / "runtime-inventory.log").read_text()]
    for row in rows:
        key = row["regression"]
        suffix, assertion = REGRESSIONS[key]
        name = PREFIX + suffix
        command = row.get("command", [])
        native.require(row == json.loads((evidence / (key + ".json")).read_text()) and row.get("verdict") == "PASS"
                       and all(type(row.get(k)) is type(v) and row[k] == v for k, v in expected_identity.items())
                       and row.get("test") == name and row.get("sourceAssertion") == assertion
                       and len(command) == 5 and Path(command[0]).name == binary.name
                       and command[1:] == native.exact_command(binary, name, manifest["candidateCommit"])[1:]
                       and row.get("runtimeEnvironment") == manifest["runtimeEnvironment"],
                       "F07 native result changed exact test, command, source assertion or binary")
        for listing in listings: native.exact_listing(listing, name)
        native.test_result((evidence / (key + ".log")).read_text(), name, row.get("exitCode"))
    return {**expected_identity, "exactTests": len(rows), "complete": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "verify"))
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--native-evidence", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "run": run(args.directory, args.native_evidence, args.out, args.testing_commit)
    else: print(json.dumps(verify(args.directory, args.native_evidence, args.out, args.testing_commit), indent=2))


if __name__ == "__main__": main()
