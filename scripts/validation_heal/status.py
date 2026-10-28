#!/usr/bin/env python3
"""J01: real startup/disabled/missing-peer truth through raw API and rc."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import tempfile
import threading
import time
from urllib import request
from urllib.error import URLError

try:
    from .heal_probe import Probe, NoRedirect, require, save, compare_inventories
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import Fleet, physical_content, validate_topology
    from .inspector import verify as verify_inspector
    from .matrix import select
    from .shards import SIZES
except ImportError:
    from heal_probe import Probe, NoRedirect, require, save, compare_inventories
    from heal_workload import Recorder, seed, verify_seed
    from healthy import Fleet, physical_content, validate_topology
    from inspector import verify as verify_inspector
    from matrix import select
    from shards import SIZES


CASE = "heal.status-partial-disabled-unknown"
BRANCHES = ("enabled-idle", "disabled", "uninitialized", "missing-peer", "recovered-complete")


class TimedProbe(Probe):
    def __init__(self, *args, timeout=8, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout

    def _open_admin(self, outgoing):
        return request.build_opener(request.ProxyHandler({}), NoRedirect()).open(outgoing, timeout=self.timeout)


def validate_background(value, branch, peers):
    coverage = value.get("coverage") or {}
    require(all(type(coverage.get(name)) is int and 0 <= coverage[name] <= peers
                for name in ("expected", "responded", "unknown")) and coverage["expected"] == peers
            and coverage["responded"] + coverage["unknown"] == peers,
            "background status lacks balanced exact peer coverage")
    state = value.get("state")
    if branch == "missing-peer":
        require(value.get("clusterStatusComplete") is False and coverage["responded"] == peers - 1
                and coverage["unknown"] == 1 and state in {"degraded", "active"}
                and isinstance(coverage.get("reasons"), list) and coverage["reasons"],
                "missing peer was hidden as complete/idle or lacks its reason")
    else:
        wanted = "disabled" if branch == "disabled" else "uninitialized" if branch == "uninitialized" else "idle"
        require(state == wanted and value.get("clusterStatusComplete") is True and coverage["responded"] == peers
                and coverage["unknown"] == 0 and coverage.get("reasons") == [],
                "background runtime state or complete coverage differs from the actual fixture")
    for name in ("healQueueLength", "healActiveTasks"):
        require(type(value.get(name)) is int and value[name] == 0, "status fixture has unresolved Heal work")
    operations = value.get("healOperations") or {}
    require(all(type(operations.get(name)) is int and operations[name] == 0
                for name in ("queueLength", "activeTasks", "retryingTasks")), "status fixture still has queued, active or retrying responsibility")


def validate_replacement(value, branch):
    cluster = value.get("cluster") or {}
    require(value.get("contractVersion") == 2 and value.get("scope") == "cluster_survivor_disks"
            and isinstance(cluster.get("records"), list) and not cluster["records"],
            "fresh status fixture lacks replacement contract or has residual records")
    uncertain = branch in {"missing-peer", "disabled", "uninitialized"}
    require(cluster.get("definitive") is (not uncertain)
            and (isinstance(cluster.get("reason"), str) and bool(cluster["reason"]) if uncertain else cluster.get("reason") is None),
            "replacement status erased uncertainty or claimed a definitive cluster")


def validate_cli(json_result, text_result, background, branch, peers):
    require(json_result.get("exitCode") == 0 and text_result.get("exitCode") == 0,
            "rc status did not complete successfully")
    value = json_result.get("json")
    require(isinstance(value, dict), "rc omitted parseable JSON status")
    allowed = {"degraded", "active", "unknown"} if branch == "missing-peer" else {background["state"]}
    require(value.get("state") in allowed, "rc converted actual runtime state to a different completion claim")
    if branch == "missing-peer":
        require(value.get("clusterStatusComplete") is not True, "rc falsely completed a partial cluster")
    # When rc exposes coverage, all counters must agree with the raw API.
    if "coverage" in value:
        require(value["coverage"] == background["coverage"], "rc peer coverage differs from raw Admin")
    if "clusterStatusComplete" in value:
        require(value["clusterStatusComplete"] is background["clusterStatusComplete"], "rc completeness differs from raw Admin")
    text = text_result.get("stdout", "")
    if branch in {"missing-peer", "disabled", "uninitialized"}:
        require("Heal Status: Idle" not in text and not re.search(r"(?im)^\s*(?:status|heal status)\s*:\s*(?:completed|complete|idle)\s*$", text),
                "rc text presented partial/disabled/uninitialized status as completed/idle")
    require(bool(text.strip()), "rc omitted its human-readable status")
    return {"rawAndRcRuntimeAgree": True, "expectedPeers": peers, "rawState": background["state"], "rcState": value["state"]}


class Cli:
    def __init__(self, binary, binding, access, secret):
        self.binary, self.binding = binary, binding
        self.folder = tempfile.TemporaryDirectory(prefix="validation-heal-rc-")
        root = Path(self.folder.name)
        root.chmod(0o700)
        text = "schema_version = 1\n"
        for alias, endpoint in binding["endpoints"].items():
            text += "[[aliases]]\n" + f"name = {json.dumps(alias)}\nendpoint = {json.dumps(endpoint)}\n"
            text += f"access_key = {json.dumps(access)}\nsecret_key = {json.dumps(secret)}\nbucket_lookup = \"path\"\n"
            text += "[aliases.retry]\nmax_attempts = 0\ninitial_backoff_ms = 100\nmax_backoff_ms = 1000\n"
        descriptor = os.open(root / "config.toml", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream: stream.write(text)
        self.env = {**os.environ, "RC_CONFIG_DIR": str(root), "NO_PROXY": "*", "HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": ""}

    def read(self, endpoint, *, as_json, timeout=10):
        args = [str(self.binary), *(["--json"] if as_json else []), "--no-color", "admin", "heal", "status", endpoint]
        try:
            result = subprocess.run(args, env=self.env, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            return {"exitCode": None, "timeout": True, "stdout": (exc.stdout or b"").decode(errors="replace") if isinstance(exc.stdout, bytes) else exc.stdout,
                    "stderr": "rc status deadline", "json": None}
        try: value = json.loads(result.stdout)
        except ValueError: value = None
        return {"exitCode": result.returncode, "stdout": result.stdout, "stderr": result.stderr, "json": value}

    def close(self):
        self.folder.cleanup()
        require(not Path(self.folder.name).exists(), "private rc credential configuration remains")


def observations(fleet, binding, cli, endpoints, *, timeout=8):
    observed = {}
    for endpoint in endpoints:
        probe = TimedProbe(binding, endpoint, timeout=timeout)
        def raw(path, method="POST"):
            try: return probe.admin(path, [], b"", method=method)
            except Exception as exc: return {"httpStatus": None, "error": str(exc)}
        observed[endpoint] = {"background": raw("/rustfs/admin/v3/background-heal/status"),
            "replacement": raw("/rustfs/admin/v4/heal/replacement-recovery", "GET"),
            "rcJson": cli.read(endpoint, as_json=True, timeout=timeout),
            "rcText": cli.read(endpoint, as_json=False, timeout=timeout)}
    return fleet.record("status-observations", observed)


def validate_observations(observed, branch, peers):
    proofs = {}
    for endpoint, value in observed.items():
        for name in ("background", "replacement"):
            require(value[name].get("httpStatus") == 200 and isinstance(value[name].get("response"), dict),
                    "status observation lacks a successful raw Admin response")
        background = value["background"]["response"]
        validate_background(background, branch, peers)
        validate_replacement(value["replacement"]["response"], branch)
        proofs[endpoint] = validate_cli(value["rcJson"], value["rcText"], background, branch, peers)
    require(bool(proofs), "status observation has no actual endpoint")
    return proofs


def reject_unsafe_completion(observed):
    for value in observed.values():
        background = value["background"].get("response")
        if not isinstance(background, dict): continue
        partial = background.get("clusterStatusComplete") is False
        uncertain = partial or background.get("state") in {"disabled", "uninitialized", "degraded"}
        require(not partial or background.get("state") != "idle", "partial cluster falsely reported idle during convergence")
        if not uncertain: continue
        cli = value["rcJson"].get("json")
        if isinstance(cli, dict):
            require(cli.get("state") not in {"idle", "complete", "completed"}, "rc falsely reported uncertain status as completed/idle")
            require(not partial or cli.get("clusterStatusComplete") is not True, "rc falsely completed a partial cluster")
        text = value["rcText"].get("stdout") or ""
        require("Heal Status: Idle" not in text and not re.search(r"(?im)^\s*(?:status|heal status)\s*:\s*(?:completed|complete|idle)\s*$", text),
                "rc text erased status uncertainty during convergence")


def stable_status(fleet, binding, cli, branch, endpoints):
    deadline = time.monotonic() + 180
    while True:
        observed = observations(fleet, binding, cli, endpoints)
        reject_unsafe_completion(observed)
        try: return validate_observations(observed, branch, len(binding["endpoints"]))
        except ValueError:
            if time.monotonic() >= deadline: raise
            time.sleep(0.5)


def watch_startup(fleet, binding, cli, done):
    deadline, result = time.monotonic() + 30, None
    while not done.is_set() and time.monotonic() < deadline:
        # Bracket CLI/replacement reads with the same raw uninitialized state;
        # a transport failure or a sample from an already initialized service
        # cannot serve as startup-window evidence.
        observed = observations(fleet, binding, cli, ["node1"], timeout=0.7)
        reject_unsafe_completion(observed)
        try:
            proof = validate_observations(observed, "uninitialized", len(binding["endpoints"]))
            after = fleet.record("startup-after-cli", TimedProbe(binding, "node1", timeout=0.7).admin(
                "/rustfs/admin/v3/background-heal/status", [], b""))
            require(after.get("httpStatus") == 200 and isinstance(after.get("response"), dict), "startup after-CLI raw status unavailable")
            validate_background(after["response"], "uninitialized", len(binding["endpoints"]))
            result = {"verdict": "PASS", "proof": proof, "rawBracketedCliWindow": True}
            break
        except Exception:
            time.sleep(0.1)
    return result or {"verdict": "NOT_OBSERVED", "reason": "full raw/rc uninitialized startup window was not witnessed; transport failure is not proof"}


def run_topology(args, topology, nodes, profile):
    directory = args.evidence / topology / profile
    directory.mkdir(parents=True)
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory, runtime_profile=profile)
    candidate = json.loads(args.candidate.read_text())
    if args.tool_directory: fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
        "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(), "bucketPrefix": bucket,
        "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000" for index in range(1, len(nodes) + 1)},
        "defaultEndpoint": "node1", "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
        "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
        "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(), "maxInventoryObjects": 512,
        "maxInventoryBytes": 512 * 1024**2}
    fleet.record("binding", binding)
    result = {"topology": topology, "runtimeProfile": profile, "branches": {}, "verdict": "RUNNING", "cleanup": "PENDING"}
    cli, pool, future, done = None, None, None, threading.Event()
    try:
        cli = Cli(args.rc_bin, binding, access, secret)
        if profile == "manual":
            pool = ThreadPoolExecutor(max_workers=1)
            def before_start():
                nonlocal future
                future = pool.submit(watch_startup, fleet, binding, cli, done)
            fleet.deploy(args.binary_sha, bucket, access, secret, before_start=before_start)
            done.set()
            result["branches"]["uninitialized"] = future.result(timeout=10)
        else:
            fleet.deploy(args.binary_sha, bucket, access, secret)
        physical = fleet.snapshot("deployed-runtime-and-topology")
        fleet.record("topology-proof", validate_topology(physical, topology))
        recorder = Recorder(Probe(binding), directory / "fixture")
        expected = seed(recorder, bucket, "DB", 128, SIZES)
        baseline = fleet.record("s3-baseline", recorder.probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        before = fleet.snapshot("physical-baseline")
        branch = "disabled" if profile == "disabled" else "enabled-idle"
        result["branches"][branch] = {"verdict": "PASS", "proof": stable_status(fleet, binding, cli, branch, binding["endpoints"])}
        if profile == "manual":
            if topology == "H2":
                node = nodes[-1]
                stopped = fleet.record("missing-peer-stopped", fleet.remote(node, "stop"))
                require(stopped.get("quiescent") is True and stopped.get("service", {}).get("MainPID") == "0", "peer stop lacks actual process exit")
                try:
                    with request.build_opener(request.ProxyHandler({})).open(binding["endpoints"]["node4"] + "/health/ready", timeout=2):
                        raise ValueError("stopped peer still accepts requests")
                except URLError as exc:
                    require(isinstance(exc.reason, ConnectionRefusedError), "peer outage is not a witnessed endpoint refusal")
                    fleet.record("missing-peer-refused", {"connectionRefused": True, "endpoint": "node4"})
                result["branches"]["missing-peer"] = {"verdict": "PASS", "proof": stable_status(fleet, binding, cli, "missing-peer", ["node1", "node2", "node3"])}
                fleet.record("same-peer-restarted", fleet.remote(node, "start"))
                fleet.wait_ready()
            else:
                result["branches"]["missing-peer"] = {"verdict": "NOT_APPLICABLE", "reason": "source J01 step 2 explicitly requires H2 for stopping one peer while querying survivors"}
                fleet.stop()
                fleet.start()
            result["branches"]["recovered-complete"] = {"verdict": "PASS", "proof": stable_status(fleet, binding, cli, "recovered-complete", binding["endpoints"])}
        fleet.stop()
        require(physical_content(before) == physical_content(fleet.snapshot("physical-after-status", quiescent=True)), "status/process observations changed object files or format identities")
        fleet.start()
        after = fleet.record("s3-after-status", recorder.probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("status-full-fixture-proof", compare_inventories(baseline, after))
        wanted = {"disabled"} if profile == "disabled" else set(BRANCHES) - {"disabled"}
        result["verdict"] = "PASS" if set(result["branches"]) == wanted and all(
            value["verdict"] == "PASS" or branch == "missing-peer" and topology == "H1" and value["verdict"] == "NOT_APPLICABLE"
            for branch, value in result["branches"].items()) else "FAIL"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
    finally:
        done.set()
        if pool: pool.shutdown(wait=True)
        cli_clean = True
        if cli:
            try: cli.close()
            except Exception as exc:
                cli_clean = False
                fleet.record("rc-config-cleanup-error", {"error": str(exc)})
        clean = fleet.cleanup()
        result["cleanup"] = "PASS" if clean and cli_clean else "FAIL"
        fleet.record("topology-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"status workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--case-id", choices=[CASE], required=True)
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    parser.add_argument("--rc-bin", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--tool-directory", type=Path)
    args = parser.parse_args()
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)), "invalid candidate binary digest")
    require(bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "workflow fleet identity required")
    require(select(CASE)[0]["variants"] == list(BRANCHES), "status executor omitted a source branch")
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "four current Heal nodes required")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            for profile in ("manual", "disabled"):
                results.append(run_topology(args, topology, selected, profile))
                if results[-1]["cleanup"] != "PASS": break
            if results[-1]["cleanup"] != "PASS": break
    finally:
        complete = len(results) == 4 and all(value["verdict"] == "PASS" and value["cleanup"] == "PASS" for value in results)
        save(args.evidence / "healthy-summary.json", {"caseId": CASE, "results": results, "complete": complete,
            "cleanup": "PASS" if results and all(value["cleanup"] == "PASS" for value in results) else "FAIL"})
    require(complete, "J01 requires every applicable raw/rc startup, disabled and peer-recovery branch")


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"Heal status validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
