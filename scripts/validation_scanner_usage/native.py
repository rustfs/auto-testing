"""Run exact candidate-native regressions and validate assertion milestones."""

from __future__ import annotations

import json
from pathlib import Path
import re
import time

from .client import ProbeError, run_command, sha256
from .contracts import assertion_id


def run_native(case, manifest, source_commit, evidence, timeout, required_ids):
    """Return native assertion records; no manifest means a capability gap."""
    if manifest is None:
        raise ProbeError("Candidate-native assertion adapter was not supplied; no deterministic hook evidence is available", "BLOCKED")
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1 or manifest.get("source_commit") != source_commit:
        raise ProbeError("Native manifest schema or candidate source identity does not match")
    cases = manifest.get("cases", {})
    if not isinstance(cases, dict):
        raise ProbeError("Native manifest cases must be an object")
    entry = cases.get(case.design_id)
    if entry is None:
        raise ProbeError("Native manifest does not provide this case's capability", "UNSUPPORTED")
    specs = entry.get("tests") if isinstance(entry, dict) else None
    if not isinstance(specs, list) or not specs:
        raise ProbeError("Native case manifest requires a nonempty tests list")
    claimed = set()
    for spec in specs:
        ids = spec.get("assertions") if isinstance(spec, dict) else None
        if not isinstance(ids, list) or not all(isinstance(item, str) for item in ids):
            raise ProbeError("Native test assertions must be a list of IDs")
        if claimed.intersection(ids):
            raise ProbeError("Each native assertion needs one complete test owner; duplicate coverage is ambiguous")
        claimed.update(ids)
    declared = set()
    records = {}
    deadline = time.monotonic() + timeout
    for index, spec in enumerate(specs):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            error = ProbeError("Native case exhausted its total execution deadline")
            error.native_records = {identifier: row for identifier, row in records.items() if identifier in required_ids}
            raise error
        try:
            current = _run_native_test(case, spec, evidence, remaining)
        except (ProbeError, OSError, ValueError) as exc:
            failure = exc if isinstance(exc, ProbeError) else ProbeError(f"Native test could not run: {exc}")
            failure.native_records = {identifier: row for identifier, row in records.items() if identifier in required_ids}
            failure.native_not_run = {identifier for pending in specs[index + 1:] for identifier in pending["assertions"]}
            if failure is exc:
                raise
            raise failure from exc
        declared.update(current)
        for identifier, row in current.items():
            records[identifier] = {**row, "native_evidence": [row["native_evidence"]]}
    if not set(required_ids) <= declared:
        error = ProbeError("Native manifest does not cover every required assertion", "UNSUPPORTED")
        error.native_records = {identifier: row for identifier, row in records.items() if identifier in required_ids}
        raise error
    return {identifier: row for identifier, row in records.items() if identifier in required_ids}


def _run_native_test(case, spec, evidence, timeout):
    started = time.monotonic()
    if not isinstance(spec, dict):
        raise ProbeError("Native test specification must be an object")
    if not isinstance(spec.get("binary"), str):
        raise ProbeError("Native executable path must be a string")
    binary = Path(spec["binary"])
    if (not binary.is_absolute() or not binary.is_file() or binary.is_symlink()
            or sha256(binary) != spec.get("binary_sha256")):
        raise ProbeError("Native executable is missing or its SHA-256 does not match")
    name = spec.get("test")
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_:]+", name):
        raise ProbeError("Native manifest must name one exact libtest test")
    known = {assertion_id(case, index) for index in range(len(case.assertions))}
    declared = spec.get("assertions")
    if (not isinstance(declared, list) or not all(isinstance(item, str) for item in declared) or len(set(declared)) != len(declared)
            or not declared or not set(declared) <= known):
        raise ProbeError("Native manifest declares empty or unknown assertion IDs")
    inventory = run_command([str(binary), name, "--exact", "--list"], min(timeout, 30))
    inventory_path = evidence.save("native-inventory", inventory)
    tests = [line[:-6] for line in inventory["stdout"].splitlines() if line.endswith(": test")]
    if inventory["timed_out"] or inventory["exit_code"]:
        raise ProbeError("Native inventory command failed or timed out")
    if tests != [name]:
        raise ProbeError("Candidate binary does not expose the exact requested native test", "UNSUPPORTED")
    remaining = timeout - (time.monotonic() - started)
    if remaining <= 0:
        raise ProbeError("Native inventory exhausted the execution deadline")
    try:
        return _execute_native_test(case, binary, name, declared, evidence, remaining, inventory_path)
    except (ProbeError, OSError, ValueError) as exc:
        failure = exc if isinstance(exc, ProbeError) else ProbeError(f"Native execution was interrupted: {exc}")
        failure.cleanup_status = "FAIL"
        if failure is exc:
            raise
        raise failure from exc


def _execute_native_test(case, binary, name, declared, evidence, remaining, inventory_path):
    """Cleanup stays unverified until both execution and its milestones settle."""
    result = run_command([str(binary), name, "--exact", "--show-output", "--test-threads=1"], remaining)
    execution_path = evidence.save("native-execution", result)
    if result["timed_out"]:
        raise ProbeError("Native regression exceeded its deadline", cleanup_status="FAIL")
    if result["exit_code"]:
        raise ProbeError("Native regression failed; inspect " + execution_path, "FAIL", cleanup_status="FAIL")
    output = result["stdout"]
    if (re.findall(r"(?m)^running .*", output) != ["running 1 test"]
            or re.findall(r"(?m)^test (?!result:).*", output) != [f"test {name} ... ok"]
            or len(re.findall(r"(?m)^test result:", output)) != 1
            or not re.search(r"(?m)^test result: ok\. 1 passed; 0 failed; 0 ignored;", output)):
        raise ProbeError("Native output does not prove one real passing test", cleanup_status="FAIL")
    records = {}
    cleanups = []
    for line in output.splitlines():
        prefix = next((prefix for prefix in ("usage_assertion: ", "usage_cleanup: ") if line.startswith(prefix)), None)
        if prefix is None:
            continue
        try:
            item = json.loads(line[len(prefix):])
        except ValueError as exc:
            raise ProbeError("Malformed native assertion milestone", cleanup_status="FAIL") from exc
        if not isinstance(item, dict) or item.get("case_id") != case.design_id:
            raise ProbeError("Native milestone belongs to a different case", cleanup_status="FAIL")
        if prefix == "usage_cleanup: ":
            cleanups.append(item)
            continue
        identifier = item.get("assertion_id")
        if identifier not in declared or identifier in records:
            raise ProbeError("Native assertion milestone is unknown or repeated", cleanup_status="FAIL")
        if item.get("status") != "PASS" or not isinstance(item.get("evidence"), dict) or not item["evidence"]:
            raise ProbeError("Native assertion lacks positive structured evidence", cleanup_status="FAIL")
        records[identifier] = {"status": "PASS", "evidence": [inventory_path, execution_path],
                               "native_evidence": item["evidence"]}
    if set(records) != set(declared):
        raise ProbeError("Native test passed but omitted required assertion evidence", cleanup_status="FAIL")
    if len(cleanups) != 1 or cleanups[0].get("status") != "PASS":
        raise ProbeError("Native fixture cleanup was not confirmed", cleanup_status="FAIL")
    return records
