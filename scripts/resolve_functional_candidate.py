#!/usr/bin/env python3
"""Resolve a nightly candidate from a dispatch payload or its GitHub artifact.

Upstream rustfs Nightly dispatches repository_dispatch type
nightly-candidate-ready only after the immutable R2 object and the
auto-testing GitHub Release tag ``nightly`` both succeed. GitHub allows at
most 10 top-level client_payload properties, so the unchanged schema 2
candidate is nested under ``client_payload.candidate``. ``package_url`` is
that immutable object. ``latest_alias_url`` is the rolling alias and is never
installed.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import zipfile

# Candidate artifacts are published by rustfs/rustfs. Chain runs, runners and
# retained evidence live in this repository.
UPSTREAM_REPOSITORY = "rustfs/rustfs"
CHAIN_REPOSITORY = "rustfs/auto-testing"
REPOSITORY = UPSTREAM_REPOSITORY
ROOT = Path(__file__).resolve().parents[1]
MAX_ARCHIVE = 1024 * 1024
VALIDATION_BUILD_WORKFLOWS = {".github/workflows/rustfs-validation-package.yml", ".github/workflows/rustfs-functional-chain.yml"}
DISPATCH_TYPE = "nightly-candidate-ready"
DISPATCH_FIELDS = {
    "event_type", "source_repo", "candidate", "latest_alias_url",
    "auto_testing_release", "channel", "deb_date", "nightly_workflow",
}
SCHEMA2_FIELDS = {
    "schema", "workflow_sha", "source_ref", "source_sha",
    "build_run_id", "build_run_attempt", "package_url", "package_sha256",
}
LATEST_ALIAS_URL = "https://dl.rustfs.com/artifacts/rustfs/packages/nightly/rustfs-nightly-latest.deb"
RELEASE_URL = "https://github.com/rustfs/auto-testing/releases/tag/nightly"
NIGHTLY_WORKFLOW = ".github/workflows/nightly-gnu.yml"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(value, length=40):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value) is not None


def positive(value):
    return type(value) is int and value > 0


def api(path, binary=False):
    result = subprocess.run(["gh", "api", path], check=True, capture_output=True, timeout=60)
    return result.stdout if binary else json.loads(result.stdout)


def validate_manifest(manifest, run):
    require(isinstance(manifest, dict), "manifest must be an object")
    common = {"schema", "source_sha", "build_run_id", "build_run_attempt", "package_url", "package_sha256"}
    version = manifest.get("schema")
    require(type(version) is int and version in (1, 2, 3), "unsupported candidate schema")
    expected_fields = common if version == 1 else common | {"workflow_sha", "source_ref"}
    if version == 3:
        expected_fields |= {"channel"}
    require(set(manifest) == expected_fields, "unexpected candidate fields")
    require(positive(manifest["build_run_id"]) and positive(manifest["build_run_attempt"]), "invalid build identity")
    require((manifest["build_run_id"], manifest["build_run_attempt"]) == (run["id"], run["run_attempt"]), "candidate belongs to another build attempt")
    require(sha(manifest["source_sha"]) and sha(manifest["package_sha256"], 64), "invalid candidate hash")
    if version == 1:
        require(manifest["source_sha"] == run["head_sha"], "legacy manifest cannot identify a different build source")
    else:
        require(manifest["workflow_sha"] == run["head_sha"] and sha(manifest["workflow_sha"]), "candidate workflow SHA differs from artifact provenance")
        require(isinstance(manifest["source_ref"], str) and bool(re.fullmatch(r"[A-Za-z0-9_./-]{1,200}", manifest["source_ref"])), "invalid build source ref")
        if version == 3:
            require(manifest["channel"] == "validation", "validation candidate has the wrong publication channel")
            require(manifest["source_ref"] == manifest["source_sha"], "validation candidate must bind an exact source SHA")
    channel = "validation" if version == 3 else "nightly"
    expected = (f"https://dl.rustfs.com/artifacts/rustfs/packages/{channel}/runs/{run['id']}/"
                f"{run['run_attempt']}/{manifest['package_sha256']}/rustfs.deb")
    require(manifest["package_url"] == expected, "package URL does not bind the run, attempt and checksum")
    return manifest


def read_json_artifact(archive, artifact, run, expected_name, member, max_json=16384):
    require(artifact.get("expired") is False, "candidate artifact expired")
    require(positive(artifact.get("id")), "invalid artifact id")
    require(artifact.get("name") == expected_name, "candidate artifact belongs to another attempt")
    provenance = artifact.get("workflow_run") or {}
    require(provenance.get("id") == run["id"] and provenance.get("head_sha") == run["head_sha"], "candidate artifact belongs to another workflow run")
    require(0 < len(archive) <= MAX_ARCHIVE and artifact.get("size_in_bytes") == len(archive), "candidate artifact size mismatch")
    require(artifact.get("digest") == "sha256:" + hashlib.sha256(archive).hexdigest(), "candidate artifact checksum mismatch")
    with zipfile.ZipFile(io.BytesIO(archive)) as source:
        files = source.infolist()
        require(len(files) == 1 and files[0].filename == member, "unexpected candidate archive members")
        require(0 < files[0].file_size <= max_json and not files[0].is_dir(), "candidate manifest too large or empty")
        manifest = json.loads(source.read(files[0]))
    return manifest


def read_manifest(archive, artifact, run):
    prefix = "validation-candidate" if run.get("path") in VALIDATION_BUILD_WORKFLOWS else "nightly-candidate"
    name = f"{prefix}-{run['id']}-{run['run_attempt']}"
    manifest = validate_manifest(read_json_artifact(archive, artifact, run, name, name + ".json"), run)
    if prefix == "validation-candidate":
        require(manifest["schema"] == 3 and manifest["channel"] == "validation",
                "validation workflow requires a schema-3 validation candidate")
    else:
        require(manifest["schema"] != 3, "nightly workflow cannot publish a validation candidate")
    return manifest


def resolve(run_id, attempt, *, allow_validation=False):
    require(positive(run_id) and positive(attempt), "build run and attempt are required positive integers")
    endpoint = f"repos/{REPOSITORY}/actions/runs/{run_id}"
    run = api(f"{endpoint}/attempts/{attempt}")
    require(run.get("id") == run_id and run.get("run_attempt") == attempt, "GitHub returned a different build attempt")
    path = run.get("path")
    validation = allow_validation and path in VALIDATION_BUILD_WORKFLOWS
    require(path == ".github/workflows/nightly-gnu.yml" and run.get("head_branch") == "main" or validation,
            "candidate must come from nightly-gnu on main or an explicitly allowed validation build")
    require((run.get("head_repository") or {}).get("full_name") == REPOSITORY, "candidate came from another repository")
    require(run.get("event") in ("schedule", "workflow_dispatch") and run.get("status") == "completed" and run.get("conclusion") == "success", "candidate attempt has not completed successfully")
    if validation:
        require(run.get("event") == "workflow_dispatch", "validation candidate must be manually dispatched")
    name = f"{'validation' if validation else 'nightly'}-candidate-{run_id}-{attempt}"
    artifacts = []
    for page in range(1, 11):
        batch = api(f"{endpoint}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(item for item in batch if item.get("name") == name)
        if len(batch) < 100:
            break
    else:
        raise ValueError("too many build artifacts to resolve safely")
    require(len(artifacts) == 1, "missing or ambiguous candidate artifact")
    artifact = artifacts[0]
    require(type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= MAX_ARCHIVE, "candidate artifact size is invalid")
    archive = api(f"repos/{REPOSITORY}/actions/artifacts/{artifact['id']}/zip", binary=True)
    manifest = read_manifest(archive, artifact, run)
    return {"manifest": manifest, "artifact_id": artifact["id"], "artifact_digest": artifact["digest"],
            "workflow_sha": run["head_sha"], "workflow_ref": run["head_branch"], "build_started_at": run["run_started_at"]}


def chain_repository():
    override = os.environ.get("FUNCTIONAL_CHAIN_REPOSITORY", "").strip()
    if override:
        return override
    current = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if current:
        return current
    return CHAIN_REPOSITORY


def _coerce_int(value, label):
    if isinstance(value, str) and value.isdigit():
        return int(value)
    require(type(value) is int, f"{label} must be a positive integer")
    return value


def schema2_manifest(manifest):
    """Validate one unchanged nightly-candidate schema 2 object."""
    require(isinstance(manifest, dict), "candidate must be an object")
    value = dict(manifest)
    for key in ("schema", "build_run_id", "build_run_attempt"):
        if key in value:
            value[key] = _coerce_int(value[key], key)
    require(value.get("schema") == 2 and set(value) == SCHEMA2_FIELDS, "nightly candidate must be schema 2 with its exact fields")
    run = {"id": value["build_run_id"], "run_attempt": value["build_run_attempt"], "head_sha": value["workflow_sha"]}
    validated = validate_manifest(value, run)
    require(validated["package_url"] != LATEST_ALIAS_URL, "package_url must be the immutable candidate, not the rolling alias")
    return validated


def dispatch_manifest(payload):
    """Validate client_payload and return the nested schema 2 candidate.

    The wrapper is not the candidate. ``latest_alias_url`` is recorded by the
    caller only as context; suites install ``candidate.package_url``.
    """
    require(isinstance(payload, dict) and set(payload) == DISPATCH_FIELDS,
            "client_payload must nest the schema 2 candidate under candidate (at most 10 top-level properties)")
    require(payload.get("event_type") == DISPATCH_TYPE, "unexpected dispatch event_type")
    require(payload.get("source_repo") == UPSTREAM_REPOSITORY, "dispatch source_repo must be rustfs/rustfs")
    require(payload.get("channel") == "nightly", "dispatch channel must be nightly")
    require(payload.get("nightly_workflow") == NIGHTLY_WORKFLOW, "dispatch nightly_workflow is not nightly-gnu")
    require(payload.get("latest_alias_url") == LATEST_ALIAS_URL, "latest_alias_url must be the rolling nightly alias")
    require(payload.get("auto_testing_release") == RELEASE_URL, "auto_testing_release must point at the nightly release")
    require(isinstance(payload.get("deb_date"), str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", payload["deb_date"]),
            "deb_date must be YYYY-MM-DD")
    manifest = schema2_manifest(payload.get("candidate"))
    print(f"nightly dispatch deb_date={payload['deb_date']} immutable={manifest['package_url']} alias={payload['latest_alias_url']}")
    return manifest


def upstream_token():
    # GITHUB_TOKEN is always present in Actions and cannot be the cross-repo
    # artifact credential. Only the explicit PAT counts as opt-in re-verify.
    return os.environ.get("GH_TOKEN", "").strip()


def unverified_candidate(manifest):
    return {
        "manifest": manifest,
        "artifact_id": 0,
        "artifact_digest": "unverified",
        "workflow_sha": manifest["workflow_sha"],
        "workflow_ref": "main",
        "build_started_at": "unverified",
    }


def candidate_from_manifest(manifest, *, verify):
    manifest = schema2_manifest(manifest)
    if not verify:
        print("PF_TESTING_GH_TOKEN unset; trusting schema 2 package_url without nightly-candidate artifact re-verify")
        return unverified_candidate(manifest)
    resolved = resolve(manifest["build_run_id"], manifest["build_run_attempt"])
    require(resolved["manifest"] == manifest, "nightly-candidate artifact does not match the dispatch payload")
    return resolved


def chain_envelope(candidate):
    revision = os.environ["GITHUB_SHA"]
    require(sha(revision), "workflow SHA is not a commit")
    return {"schema": 1, "run_id": int(os.environ["GITHUB_RUN_ID"]), "attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
            "workflow_sha": revision, "testing_sha": revision, "candidate": candidate}


def write_chain(chain):
    encoded = json.dumps(chain, sort_keys=True, separators=(",", ":"))
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write("manifest=" + encoded + "\n")
    Path(os.environ["CHAIN_OUTPUT"]).write_text(encoded + "\n")


def manual_manifest():
    fields = {
        "schema": 2,
        "workflow_sha": os.environ.get("WORKFLOW_SHA", "").strip(),
        "source_ref": os.environ.get("SOURCE_REF", "").strip(),
        "source_sha": os.environ.get("SOURCE_SHA", "").strip(),
        "package_url": os.environ.get("PACKAGE_URL", "").strip(),
        "package_sha256": os.environ.get("PACKAGE_SHA256", "").strip(),
    }
    run_id = os.environ.get("BUILD_RUN_ID", "").strip()
    attempt = os.environ.get("BUILD_RUN_ATTEMPT", "").strip()
    if any(fields.values()):
        require(all(fields.values()) and run_id and attempt,
                "manual schema 2 replay requires the full candidate, including build run and attempt")
        fields["build_run_id"] = int(run_id)
        fields["build_run_attempt"] = int(attempt)
        return fields
    require(run_id and attempt, "manual replay requires build_run_id and build_run_attempt, or a full schema 2 candidate")
    return None


def prepare():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    name = os.environ["GITHUB_EVENT_NAME"]
    verify = bool(upstream_token())
    if name == "repository_dispatch":
        require(event.get("action") == DISPATCH_TYPE, "unexpected repository_dispatch type")
        manifest = dispatch_manifest(event.get("client_payload"))
        require(event["client_payload"].get("event_type") == event.get("action"), "dispatch action does not match client_payload.event_type")
        candidate = candidate_from_manifest(manifest, verify=verify)
    elif name == "workflow_dispatch":
        manifest = manual_manifest()
        if manifest is None:
            require(verify, "resolving nightly-candidate from rustfs/rustfs requires PF_TESTING_GH_TOKEN (actions:read)")
            candidate = resolve(int(os.environ["BUILD_RUN_ID"]), int(os.environ["BUILD_RUN_ATTEMPT"]))
        else:
            candidate = candidate_from_manifest(manifest, verify=verify)
    elif name == "schedule":
        # Validation sweep pins the newest successful nightly, then calls this
        # resolver. There is no dispatch payload on a schedule.
        require(verify, "scheduled candidate resolution requires PF_TESTING_GH_TOKEN (actions:read on rustfs/rustfs)")
        candidate = resolve(int(os.environ["BUILD_RUN_ID"]), int(os.environ["BUILD_RUN_ATTEMPT"]))
    else:
        raise ValueError("functional candidate resolution accepts repository_dispatch, workflow_dispatch, or schedule")
    write_chain(chain_envelope(candidate))


if __name__ == "__main__":
    prepare()
