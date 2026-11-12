#!/usr/bin/env python3
"""Record a safe, disabled-by-default result for environment-gated Heal cases.

The first migration batch has complete procedures in ``recipes.json`` but the
current auto-testing fleet does not expose all required resources.  This
default path never mutates the fleet. It records missing capabilities and
requires an explicit switch, capability manifest and candidate-bound runtime
before invoking the extended scenario driver.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Mapping, Sequence

try:
    from .execution_policy import CAPABILITIES_ENV, ENABLE_ENV, ENVIRONMENT_GATED_CASES, enabled
except ImportError:
    from execution_policy import CAPABILITIES_ENV, ENABLE_ENV, ENVIRONMENT_GATED_CASES, enabled

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "validation-cases.json"


def _catalog_case(case_id: str) -> dict:
    document = json.loads(CATALOG.read_text(encoding="utf-8"))
    for module in document["modules"]:
        for case in module["cases"]:
            if case["id"] == case_id:
                return case
    raise ValueError(f"unknown validation case: {case_id}")


def load_capabilities(path: Path | None) -> set[str]:
    if path is None:
        return set()
    if not path.is_file() or path.is_symlink():
        return set()
    value = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(value, list):
        values = value
    elif isinstance(value, dict):
        values = value.get("capabilities", [])
    else:
        raise ValueError("capability manifest must be a list or an object with capabilities")
    if not isinstance(values, list) or any(not isinstance(item, str) or not item for item in values):
        raise ValueError("capability manifest contains an invalid capability")
    return set(values)


def decision(case_id: str, *, environ: Mapping[str, str] | None = None,
             capabilities: Sequence[str] = ()) -> dict:
    policy = ENVIRONMENT_GATED_CASES[case_id]
    values = environ if environ is not None else os.environ
    requested = set(capabilities)
    if not enabled(values):
        return {
            "state": "disabled",
            "reasonCode": "ENV_DISABLED_BY_DEFAULT",
            "enabled": False,
            "missingCapabilities": list(policy["requiredCapabilities"]),
            "reason": (
                f"{policy['reason']} Disabled by default; a real test is not started. Preparation is allowed only after "
                f"{ENABLE_ENV}=1 is set and {CAPABILITIES_ENV} is provided, and only once the manifest satisfies every capability."
            ),
        }
    missing = sorted(set(policy["requiredCapabilities"]) - requested)
    if missing:
        return {
            "state": "blocked",
            "reasonCode": "ENV_CAPABILITY_MISSING",
            "enabled": True,
            "missingCapabilities": missing,
            "reason": (
                f"{ENABLE_ENV}=1 is explicitly enabled, but the auto-testing Heal environment capability manifest is still missing: "
                f"{', '.join(missing)}. Original block reason: {policy['reason']}"
            ),
        }
    return {
        "state": "ready",
        "reasonCode": "ENV_RUNTIME_REQUIRED",
        "enabled": True,
        "missingCapabilities": [],
        "reason": (
            "Explicitly enabled, and the environment capability manifest is satisfied. "
            "VALIDATION_HEAL_ENVIRONMENT_RUNTIME must still name the candidate-bound resources and control adapter. "
            "A missing real resource binding remains BLOCKED. A capability declaration itself is not recorded as PASS."
        ),
    }


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    os.replace(temporary, path)


def write_blocked(case_id: str, artifacts: Path, *, candidate: Path | None,
                  testing_commit: str | None, environ: Mapping[str, str] | None = None,
                  override: tuple[str, str] | None = None) -> dict:
    if case_id not in ENVIRONMENT_GATED_CASES:
        raise ValueError(f"case is not environment-gated: {case_id}")
    policy = ENVIRONMENT_GATED_CASES[case_id]
    capabilities_path = None
    values = environ if environ is not None else os.environ
    raw_capabilities = values.get(CAPABILITIES_ENV, "").strip()
    if raw_capabilities:
        capabilities_path = Path(raw_capabilities).expanduser()
    capabilities_error = None
    try:
        capabilities = load_capabilities(capabilities_path) if enabled(values) else set()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        capabilities = set()
        capabilities_error = str(exc)
    result_decision = decision(case_id, environ=values, capabilities=capabilities)
    if capabilities_error:
        result_decision.update(state="blocked", reasonCode="ENV_CAPABILITY_MANIFEST_INVALID",
                               reason=f"Capability manifest is invalid: {capabilities_error}; original block reason: {policy['reason']}")
    if override:
        result_decision.update(state="blocked", reasonCode=override[0], reason=override[1])

    evidence_dir = artifacts / "evidence"
    evidence_path = evidence_dir / "environment-gate.json"
    evidence = {
        "schemaVersion": 1,
        "caseId": case_id,
        "code": policy["code"],
        "policy": policy,
        "enabled": result_decision["enabled"],
        "state": result_decision["state"],
        "reasonCode": result_decision["reasonCode"],
        "reason": result_decision["reason"],
        "missingCapabilities": result_decision["missingCapabilities"],
        "enableVariable": ENABLE_ENV,
        "capabilitiesVariable": CAPABILITIES_ENV,
        "capabilitiesPath": str(capabilities_path) if capabilities_path else None,
        "capabilities": sorted(capabilities),
        "candidateManifest": str(candidate.resolve()) if candidate and candidate.exists() else None,
        "candidateManifestSha256": hashlib.sha256(candidate.read_bytes()).hexdigest()
        if candidate and candidate.is_file() and not candidate.is_symlink() else None,
        "testingCommit": testing_commit,
        "fleetTouched": False,
        "canonicalPassClaimed": False,
    }
    _write_json(evidence_path, evidence)
    case = _catalog_case(case_id)
    result = {
        "schema_version": 1,
        "case_id": case_id,
        "source_status": case["source_status"],
        "migration_status": "unverified",
        "profile": policy["profile"],
        "verdict": "BLOCKED",
        "cleanup_status": "NOT_RUN",
        "reason": f"[{policy['code']}:{result_decision['reasonCode']}] {result_decision['reason']}",
        "evidence": ["evidence/environment-gate.json"],
        "execution_policy": {
            "kind": policy["kind"],
            "disabledByDefault": policy["disabledByDefault"],
            "enableVariable": policy["enableVariable"],
            "capabilitiesVariable": policy["capabilitiesVariable"],
            "requiredCapabilities": policy["requiredCapabilities"],
            "reasonCode": policy["reasonCode"],
        },
    }
    result_path = artifacts / "cases" / f"{case_id}.json"
    _write_json(result_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--candidate-manifest", type=Path)
    parser.add_argument("--testing-commit")
    parser.add_argument("--validation-artifacts-dir", type=Path, required=True)
    args = parser.parse_args()
    # The default path imports no live test dependencies and never reads or
    # invokes a resource adapter. A capability declaration alone cannot run.
    capabilities_error = None
    try:
        capabilities = load_capabilities(Path(os.environ[CAPABILITIES_ENV])) if enabled() and os.environ.get(CAPABILITIES_ENV) else set()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        capabilities = set(); capabilities_error = str(exc)
    gate = decision(args.case_id, capabilities=capabilities)
    if capabilities_error:
        gate.update(state="blocked", reasonCode="ENV_CAPABILITY_MANIFEST_INVALID", reason=capabilities_error)
    if gate["state"] == "ready" and os.environ.get("VALIDATION_HEAL_ENVIRONMENT_RUNTIME"):
        try:
            from .environment_cases import run, runtime_binding, RUNTIME_ENV
        except ImportError:
            from environment_cases import run, runtime_binding, RUNTIME_ENV
        runtime_path = Path(os.environ[RUNTIME_ENV])
        try:
            runtime_binding(json.loads(runtime_path.read_text()), args.case_id,
                            json.loads(args.candidate_manifest.read_text()), args.testing_commit)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            write_blocked(args.case_id, args.validation_artifacts_dir,
                          candidate=args.candidate_manifest, testing_commit=args.testing_commit,
                          override=("ENV_RUNTIME_INVALID", str(exc)))
            return 0
        result = run(runtime_path, args.case_id, args.candidate_manifest, args.testing_commit,
                     args.validation_artifacts_dir)
        print(f"{args.case_id}: {result['verdict']}")
        return 0 if result["verdict"] == "PASS" else 1
    write_blocked(args.case_id, args.validation_artifacts_dir,
                  candidate=args.candidate_manifest, testing_commit=args.testing_commit)
    print(f"{args.case_id}: BLOCKED by environment gate; fleet untouched")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"environment gate error: {exc}", file=os.sys.stderr)
        raise SystemExit(2)
