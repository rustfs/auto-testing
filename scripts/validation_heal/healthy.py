#!/usr/bin/env python3
"""Execute A01 healthy and A07 invalid-request branches on the current H1/H2 fleet."""

from __future__ import annotations

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
import time
from urllib import request
from urllib.parse import quote
import xml.etree.ElementTree as ET

try:
    from .heal_probe import Probe, canonical, compare_inventories, require, save
    from .heal_workload import Recorder, seed, verify_seed
    from .inspector import verify as verify_inspector
except ImportError:
    from heal_probe import Probe, canonical, compare_inventories, require, save
    from heal_workload import Recorder, seed, verify_seed
    from inspector import verify as verify_inspector


NODE_SOURCE = Path(__file__).with_name("fleet_node.py")
ROOT = Path(__file__).resolve().parents[2]
BASE_OPTIONS = {"recursive": False, "remove": False, "recreate": False, "scanMode": 2,
                "dryRun": False, "updateParity": False, "nolock": False, "readRepair": False, "pool": 0, "set": 0}


def validate_rejection(observed, expected_code):
    require(observed.get("httpStatus") == 400, "invalid Heal request was not rejected with HTTP 400")
    body = observed.get("response")
    if isinstance(body, str):
        code = next((element.text for element in ET.fromstring(body).iter()
                     if element.tag.rsplit("}", 1)[-1] == "Code"), None)
    elif isinstance(body, dict):
        code = body.get("Code") or body.get("code")
        require(not body.get("clientToken"), "invalid request admitted a Heal task")
    else:
        code = None
    require(code == expected_code, f"invalid Heal request expected {expected_code}; observed {code}")


def invalid_requests(probe, fleet, bucket, before, baseline):
    path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/objects/0000.bin"
    variants = [
        ("duplicate-query", BASE_OPTIONS, [("clientToken", "a"), ("clientToken", "b")], "InvalidArgument", None),
        ("invalid-bool", BASE_OPTIONS, [("forceStop", "not-a-bool")], "InvalidArgument", None),
        ("invalid-scan", {**BASE_OPTIONS, "scanMode": 99}, [], "InvalidRequest", None),
        ("conflicting-flags", BASE_OPTIONS, [("forceStart", "true"), ("forceStop", "true")], "InvalidRequest", None),
        ("invalid-selector", {**BASE_OPTIONS, "pool": -1}, [], "InvalidRequest", None),
        ("oversize-body", BASE_OPTIONS, [], "EntityTooLarge", b" " * (1024**2 + 1)),
    ]
    branches = {}
    for name, options, query, code, raw in variants:
        observed = fleet.record("trigger-" + name, probe.admin(path, query, raw if raw is not None else json.dumps(options).encode()))
        validate_rejection(observed, code)
        for endpoint in probe.binding["endpoints"]:
            state = fleet.record("control-" + name, Probe(probe.binding, endpoint).admin(
                "/rustfs/admin/v3/background-heal/status", [], b""))
            require(state.get("httpStatus") == 200 and isinstance(state.get("response"), dict),
                    "invalid-request task isolation status is unavailable")
            value = state["response"]
            coverage = value.get("coverage") or {}
            require(value.get("clusterStatusComplete") is True
                    and coverage.get("expected") == len(fleet.nodes) and coverage.get("responded") == len(fleet.nodes)
                    and coverage.get("unknown") == 0 and value.get("healActiveTasks") == 0 and value.get("healQueueLength") == 0,
                    "invalid request left active responsibility or incomplete peer coverage")
        branches[name] = {"verdict": "PASS", "httpStatus": 400, "code": code, "noActiveResponsibility": True}
    after = fleet.snapshot("physical-after-negatives")
    require(physical_content(before) == physical_content(after), "invalid requests changed the physical fixture")
    inventory = fleet.record("s3-after-negatives", probe.inventory(bucket, unversioned_current_tags=True))
    fleet.record("negative-isolation-proof", {"physicalUnchanged": True, "s3": compare_inventories(baseline, inventory)})
    return branches


def physical_content(snapshot):
    return {node: {key: value[key] for key in ("files", "formats")} for node, value in snapshot.items()}


def validate_topology(snapshot, topology, *, expected_sets=1):
    require(topology in {"H1", "H2"} and len(snapshot) == (1 if topology == "H1" else 4),
            "wrong node count for Heal topology")
    records = [(node, value) for node, entry in snapshot.items() for value in entry["formats"]]
    require(len(records) == (4 if topology == "H1" else 16), "wrong physical disk count")
    values = [item["value"] for _, item in records]
    sets = values[0]["xl"]["sets"]
    require(type(expected_sets) is int and (expected_sets == 1 or topology == "H2" and expected_sets == 2),
            "unsupported explicit set layout")
    identities = [identity for group in sets for identity in group]
    require(len(sets) == expected_sets and all(len(group) == len(records) // expected_sets for group in sets)
            and len(identities) == len(set(identities)) == len(records),
            "explicit topology lacks complete independently formatted erasure sets")
    require(all(value["format"] == "xl" and value["xl"]["sets"] == sets
                and value["id"] == values[0]["id"] for value in values)
            and {value["xl"]["this"] for value in values} == set(identities),
            "physical disk UUID, pool and set mapping differs")
    if topology == "H2":
        require(all(len({item["value"]["xl"]["this"] for owner, item in records if owner == node
                         and item["value"]["xl"]["this"] in group}) == 4 // expected_sets
                    for node in snapshot for group in sets), "each H2 set must span every original physical node")
    return {"topology": topology, "nodes": len(snapshot), "independentDrives": len(records),
            "poolIndex": 0, "setIndex": 0 if expected_sets == 1 else None, "diskUUIDs": identities,
            "setCount": expected_sets, "actualSets": sets}


def validate_healthy(status, bucket, key, disposition):
    outcome = status.get("outcome") or {}
    require(status.get("summary") == "finished" and status.get("truncated", False) is False
            and outcome.get("execution", {}).get("state") == "completed"
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False,
            "Heal did not prove complete terminal coverage")
    counters = outcome.get("counters", {})
    expected = {"processed": 1, "healed": 0, "unchanged": 1, "skipped": 0,
                "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(counters.get(name)) is type(value) and counters[name] == value
                for name, value in expected.items()), "healthy object counters do not prove exactly one unchanged object")
    objects = outcome.get("objects", [])
    require(len(objects) == 1, "healthy task returned incomplete or additional object receipts")
    identity = objects[0].get("identity", {})
    require(identity.get("bucket") == bucket and identity.get("object") == key
            and type(identity.get("poolIndex")) is int and identity["poolIndex"] == 0
            and type(identity.get("setIndex")) is int and identity["setIndex"] == 0
            and bool(identity.get("bucketIncarnationId")), "healthy receipt lacks the exact object responsibility identity")
    require(objects[0].get("disposition", {}).get("state") == disposition,
            f"expected {disposition} receipt; observed {objects[0].get('disposition')}")


class Fleet:
    def __init__(self, nodes, evidence, run_id, tool_directory=None, *, runtime_profile="manual", resource_profile="standard"):
        require(runtime_profile in {"queue-pressure", "orphan-preserve", "orphan-before", "orphan-after", "manual", "authority-split", "multi-pool", "bounded-capacity", "journal-bounded", "lock-quorum", "offline-replay", "durable-mrf", "mrf-crash", "flapping", "read-repair", "scanner", "disabled", "overlap", "root-restart", "stale-rejoin", "replacement", "schema-refusal", "scope-isolation", "incarnation-checkpoint", "incarnation-mrf"}, "unsupported Heal runtime profile")
        require(resource_profile in {"standard", "cursor", "full-restart"}, "unsupported Heal resource profile")
        self.nodes, self.evidence, self.run_id = nodes, evidence, run_id
        self.tool_directory = tool_directory
        self.runtime_profile = runtime_profile
        self.node_runtime_profiles = {}
        self.resource_profile = resource_profile
        self.sequence = 0
        self.reserved = []
        self.faults = []
        self.bucket_record_faults = []
        self.drive_gates = set()
        self.quorum_gates = set()
        self.network_gates = set()
        self.generation_faults = []
        self.orphan_faults = []
        self.journal_faults = []
        self.incarnation_replays = []
        self.secret_values = set()
        self.frozen_processes = {}
        self.snapshot_buckets = None

    def record(self, label, value):
        self.sequence += 1
        def sanitized(item):
            if isinstance(item, str):
                for secret in sorted(self.secret_values, key=len, reverse=True):
                    item = item.replace(secret, "[REDACTED]")
                return item
            if isinstance(item, dict):
                return {sanitized(key): sanitized(entry) for key, entry in item.items()}
            if isinstance(item, list):
                return [sanitized(entry) for entry in item]
            return item
        save(self.evidence / f"{self.sequence:05d}-{label}.json", sanitized(value))
        return value

    def ssh(self, node):
        return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new",
                "-p", os.environ.get("RUSTFS_SSH_PORT", "22"),
                os.environ.get("RUSTFS_SSH_USER", "azureuser") + "@" + node]

    def remote(self, node, action, **params):
        if action == "snapshot" and self.snapshot_buckets is not None:
            require("buckets" not in params, "physical bucket scope must use the frozen fleet binding")
            params["buckets"] = self.snapshot_buckets
        value = {"runId": self.run_id, "action": action, **params}
        source = NODE_SOURCE.read_text()
        if action.startswith("network-") or action in {"cleanup", "quota-audit-finalize", "journal-rejection-audit", "journal-rejection-audit-finalize"}:
            network_source = NODE_SOURCE.with_name("network_gate.py").read_text()
            source += "\nimport types\nnetwork_gate_backend = types.ModuleType('network_gate')\nexec(" + repr(
                network_source) + ", network_gate_backend.__dict__)\nNETWORK_GATE_SOURCE = " + repr(network_source) + "\n"
        if self.runtime_profile in {"bounded-capacity", "journal-bounded"}:
            source += "\nimport types\nbounded_volume_backend = types.ModuleType('bounded_volume')\nexec(" + repr(
                NODE_SOURCE.with_name("bounded_volume.py").read_text()) + ", bounded_volume_backend.__dict__)\n"
        if action == "mrf-records" or action.startswith(("journal-", "incarnation-")):
            source += "\nimport types\nmrf = types.ModuleType('mrf_records')\nexec(" + repr(
                NODE_SOURCE.with_name("mrf_records.py").read_text()) + ", mrf.__dict__)\n"
        if action == "replacement-records":
            source += "\nimport types\nreplacement_journal = types.ModuleType('replacement_journal')\nexec(" + repr(
                NODE_SOURCE.with_name("replacement_journal.py").read_text()) + ", replacement_journal.__dict__)\n"
        if action.startswith("generation-") or action.startswith("orphan-"):
            source += "\nimport types\ngeneration_backend = types.ModuleType('generation_fault')\nexec(" + repr(
                NODE_SOURCE.with_name("generation_fault.py").read_text()) + ", generation_backend.__dict__)\n"
        if action in {"apply-fault", "recover-fault", "verify-part", "verify-parts", "verify-inline",
                      "bucket-record-snapshot", "bucket-record-apply", "bucket-record-recover", "format-schema-fault", "encryption-configure"} or action.startswith("generation-") or action.startswith("orphan-") or action.startswith(("journal-", "incarnation-")):
            source += "\nimport types\nfault = types.ModuleType('heal_fault')\nexec(" + repr(
                NODE_SOURCE.with_name("heal_fault.py").read_text()) + ", fault.__dict__)\n"
        if action.startswith(("journal-", "incarnation-")):
            source += "\nimport types\njournal_fault_backend = types.ModuleType('journal_fault')\nexec(" + repr(
                NODE_SOURCE.with_name("journal_fault.py").read_text()) + ", journal_fault_backend.__dict__)\n"
        if action.startswith("incarnation-"):
            source += "\nimport types\nincarnation_replay_backend = types.ModuleType('incarnation_replay')\nexec(" + repr(
                NODE_SOURCE.with_name("incarnation_replay.py").read_text()) + ", incarnation_replay_backend.__dict__)\n"
        if action.startswith("orphan-"):
            source += "\nimport types\norphan_backend = types.ModuleType('orphan_fault')\nexec(" + repr(
                NODE_SOURCE.with_name("orphan_fault.py").read_text()) + ", orphan_backend.__dict__)\n"
        if action.startswith("bucket-record-"):
            source += "\nimport types\nbucket_records = types.ModuleType('bucket_records')\nexec(" + repr(
                NODE_SOURCE.with_name("bucket_records.py").read_text()) + ", bucket_records.__dict__)\n"
        if action == "format-schema-fault":
            source += "\nimport types\nschema_faults = types.ModuleType('schema_faults')\nexec(" + repr(
                NODE_SOURCE.with_name("schema_faults.py").read_text()) + ", schema_faults.__dict__)\n"
        if action in {"encryption-configure", "snapshot", "audit-finalize", "quota-audit-finalize", "journal-rejection-audit", "journal-rejection-audit-finalize"}:
            source += "\nimport types\nencryption_runtime = types.ModuleType('encryption_runtime')\nexec(" + repr(
                NODE_SOURCE.with_name("encryption_runtime.py").read_text()) + ", encryption_runtime.__dict__)\n"
        replacement_actions = {"start", "cleanup", "audit-finalize", "quota-audit-finalize", "journal-rejection-audit", "journal-rejection-audit-finalize", "replace-drive", "replacement-state", "replacement-probe", "foreign-format", "restore-replacement", "snapshot", "format-schema-fault"}
        if action in {"start", "cleanup", "audit-finalize", "quota-audit-finalize", "journal-rejection-audit", "journal-rejection-audit-finalize", "drive-gate", "drive-gate-arm-live", "replace-drive", "quorum-gate", "quorum-gate-probe"}:
            source += "\nimport types\nquorum_gate_backend = types.ModuleType('quorum_gate')\nexec(" + repr(
                NODE_SOURCE.with_name("quorum_gate.py").read_text()) + ", quorum_gate_backend.__dict__)\n"
        if action in {"drive-gate", "drive-gate-arm-live", "drive-gate-probe", "drive-gate-activate", "gate-state", "drive-gate-release-live"} | replacement_actions:
            source += "\nimport types\ngate = types.ModuleType('drive_gate')\nexec(" + repr(
                NODE_SOURCE.with_name("drive_gate.py").read_text()) + ", gate.__dict__)\n"
        if action in replacement_actions:
            source += "\nimport sys\nsys.modules['drive_gate'] = gate\nreplacement = types.ModuleType('replace_drive')\nexec(" + repr(
                NODE_SOURCE.with_name("replace_drive.py").read_text()) + ", replacement.__dict__)\n"
        source += "\nmain(" + repr(value) + ")\n"
        timeout = 600 if self.resource_profile in {"cursor", "full-restart"} and action == "snapshot" else 180
        if action in {"bounded-volumes-create", "bounded-volumes-fill"}:
            timeout = 600
        result = subprocess.run([*self.ssh(node), "sudo -n python3 -"],
                                input=source, capture_output=True, text=True, timeout=timeout)
        require(result.returncode == 0, f"Heal node {node} {action} failed: {result.stderr[-1200:]}")
        return json.loads(result.stdout)

    def snapshot(self, label, *, quiescent=False, metadata_faults=None):
        return self.record(label, {node: self.remote(node, "snapshot", quiescent=quiescent,
            metadataFaults=(metadata_faults or {}).get(node, {})) for node in self.nodes})

    def stop(self):
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            results = list(executor.map(lambda node: self.remote(node, "stop"), self.nodes))
        return self.record("quiescent", dict(zip(self.nodes, results, strict=True)))

    def start(self):
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            results = list(executor.map(lambda node: self.remote(node, "start"), self.nodes))
        self.record("started", dict(zip(self.nodes, results, strict=True)))
        self.wait_ready()
        for node in sorted(self.drive_gates):
            gate = self.remote(node, "gate-state")
            if gate.get("mode") == "permission-denied" and gate.get("phase") == "armed":
                self.record("live-permission-injection", {"node": node, **self.remote(node, "drive-gate-activate")})

    def deploy(self, binary_sha, bucket, access, secret, *, before_start=None):
        self.secret_values.update({access, secret})
        checked = {node: self.remote(node, "preflight", resourceProfile=self.resource_profile) for node in self.nodes}
        self.record("preflight", checked)
        require(len({value["hostname"] for value in checked.values()}) == len(self.nodes),
                "Heal node aliases must resolve to distinct actual hosts")
        paths = checked[self.nodes[0]]["dataRoots"]
        volumes = paths if len(self.nodes) == 1 else [f"http://rustfs-node{index}:9000{path}"
                    for index in range(1, 5) for path in paths]
        if self.runtime_profile == "multi-pool":
            require(len(self.nodes) == 4, "H5 requires all four current Heal nodes")
            try: from .multi_pool_layout import volumes as pool_volumes
            except ImportError: from multi_pool_layout import volumes as pool_volumes
            volumes = pool_volumes(self.run_id)
        if self.runtime_profile == "scope-isolation":
            require(len(self.nodes) == 4, "two-set isolation requires the four-node topology")
            volumes = [f"http://rustfs-node{index}:9000{path}" for path in paths for index in range(1, 5)]
        self.record("integrity-fleet-attestation", {"scope": "this run's private H1/H2 endpoint set",
            "allParticipatingNodesUseCandidate": True, "binarySha256": binary_sha,
            "nodes": self.nodes, "volumes": volumes, "mixedVersionsAllowed": False,
            "precondition": "reserve verifies the candidate copy on every peer before any service starts",
            "newFixtureWrites": "protected independent shard commitments",
            "legacyDefaultResult": "retained separately; unknown is never a successful positive receipt"})
        for node in self.nodes:
            # Reserve may partially mutate before an SSH failure. Preserve its
            # namespace unless the root-owned marker proves safe finalization.
            self.reserved.append(node)
            inspector_sha = hashlib.sha256((self.tool_directory / "heal-meta-inspect").read_bytes()).hexdigest() \
                if self.tool_directory else None
            value = self.remote(node, "reserve", hostname=checked[node]["hostname"], mounts=checked[node]["mounts"],
                                binarySha256=binary_sha, bucket=bucket, volumes=volumes, access=access, secret=secret,
                                inspectorSha256=inspector_sha, runtimeProfile=self.node_runtime_profiles.get(node, self.runtime_profile),
                                resourceProfile=self.resource_profile)
            self.record("reserved", {"node": node, **value})
            if self.tool_directory:
                tool = self.tool_directory / "heal-meta-inspect"
                target = "/var/lib/rustfs-auto-testing/heal/" + self.run_id + "/heal-meta-inspect"
                transfer = subprocess.run([*self.ssh(node), "sudo -n dd of=" + target
                                           + " conv=excl,fsync oflag=nofollow bs=1M status=none"],
                                          input=tool.read_bytes(), capture_output=True, timeout=120)
                require(transfer.returncode == 0, "exclusive inspector upload failed: " + transfer.stderr.decode()[-800:])
                self.record("inspector-upload", {"node": node, **self.remote(node, "install-inspector")})
        if before_start is not None:
            before_start()
        self.start()

    def wait_ready(self):
        opener = request.build_opener(request.ProxyHandler({}))
        deadline = time.monotonic() + 180
        pending = {f"http://rustfs-node{index}:9000" for index in range(1, len(self.nodes) + 1)}
        while pending and time.monotonic() < deadline:
            for endpoint in tuple(pending):
                try:
                    with opener.open(endpoint + "/health/ready", timeout=5) as response:
                        if response.status == 200:
                            pending.remove(endpoint)
                except OSError:
                    pass
            if pending:
                time.sleep(2)
        require(not pending, "Heal deployment did not make every node ready")

    def cleanup(self, *, preserve_namespaces=False):
        failures = [{"state": "DIRTY", "namespacesRetained": True,
                     "error": "unresolved C11 S3 mutation requires retained physical evidence"}] if preserve_namespaces else []
        if self.incarnation_replays:
            try:
                self.stop()
                for node, params in tuple(self.incarnation_replays):
                    receipt = self.record("C11-original-replay-evidence-finalization", {"node": node,
                        **self.remote(node, "incarnation-finalize", **params)})
                    require(receipt.get("originalEvidenceExported") is True
                            and receipt.get("namespaceFinalizationAllowed") is True
                            and receipt.get("backupRescueUsed") is False
                            and receipt.get("productRepairClaimed") is False,
                            "C11 original replay evidence was not exported before namespace finalization")
                    self.incarnation_replays.remove((node, params))
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})

        if self.journal_faults:
            try:
                self.stop()
                if self.runtime_profile == "journal-bounded":
                    for node in self.reserved:
                        self.record("bounded-journal-filler-recovery", {"node": node, **self.remote(
                            node, "bounded-volumes-release", quiescent=True)})
                for node, params in tuple(self.journal_faults):
                    receipt = self.record("durable-record-recovery", {"node": node, **self.remote(node, "journal-recover", **params)})
                    require(receipt.get("backupRetained") is True and receipt.get("originalResponsibilityRecreated") is False
                            and receipt.get("productRepairClaimed") is False,
                            "durable-record rescue lost the original evidence or claims product repair")
                    self.journal_faults.remove((node, params))
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        if self.orphan_faults:
            try:
                self.stop()
                for node, params in tuple(self.orphan_faults):
                    receipt = self.record("orphan-last-ACK-recovery", {"node": node, **self.remote(node, "orphan-recover", **params)})
                    require(receipt.get("state") == "CLEAR" and receipt.get("lastACKAuthorityRestored") is True,
                            "C10 lacks complete last-ACK authority restoration")
                    self.orphan_faults.remove((node, params))
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        if self.generation_faults:
            try:
                self.stop()
                for node, params in tuple(self.generation_faults):
                    receipt = self.record("generation-last-ACK-recovery", {"node": node, **self.remote(node, "generation-recover", **params)})
                    require(receipt.get("state") == "CLEAR" and receipt.get("originalLastACKGenerationRestored") is True,
                            "generation replay lacks complete last-ACK restoration")
                    self.generation_faults.remove((node, params))
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        if self.network_gates:
            try:
                self.stop()
                for node in tuple(self.network_gates):
                    self.record("network-gate-recovery", {"node": node, **self.remote(node, "network-release")})
                    self.network_gates.remove(node)
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        for node, identity in tuple(self.frozen_processes.items()):
            try:
                current = self.remote(node, "process")
                if current["pid"]:
                    self.record("resume-for-finalization", {"node": node, **self.remote(node, "process-signal",
                        signal="SIGCONT", expected=identity)})
                del self.frozen_processes[node]
            except Exception as exc:
                failures.append({"node": node, "error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        if self.drive_gates:
            try:
                self.stop()
                for node in tuple(self.drive_gates):
                    self.record("gate-recovery", {"node": node, **self.remote(node, "drive-gate", mode="clear")})
                    self.drive_gates.remove(node)
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        if self.quorum_gates:
            try:
                self.stop()
                for node in tuple(self.quorum_gates):
                    self.record("quorum-gate-recovery", {"node": node, **self.remote(node, "quorum-gate", mode="clear")})
                    self.quorum_gates.remove(node)
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        for node in self.reserved:
            try:
                self.record("logs", {"node": node, **self.remote(node, "logs")})
            except Exception as exc:
                self.record("logs-error", {"node": node, "error": str(exc)})
        if (self.faults or self.bucket_record_faults) and not failures:
            try:
                self.stop()
                if self.runtime_profile in {"bounded-capacity", "journal-bounded"}:
                    for node in self.reserved:
                        self.record("bounded-filler-recovery", {"node": node, **self.remote(
                            node, "bounded-volumes-release", quiescent=True)})
                for node, params in self.faults:
                    self.record("fault-recovery", {"node": node, **self.remote(node, "recover-fault", **params)})
                for node, params in self.bucket_record_faults:
                    self.record("bucket-record-rescue", {"node": node, **self.remote(node, "bucket-record-recover", **params)})
            except Exception as exc:
                failures.append({"error": str(exc), "state": "DIRTY", "namespacesRetained": True})
        if not failures:
            for node in self.reserved:
                try:
                    self.record("cleanup", {"node": node, **self.remote(node, "cleanup")})
                except Exception as exc:
                    failures.append({"node": node, "error": str(exc)})
        self.record("recovery", {"cleanup": "FAIL" if failures else "PASS", "errors": failures})
        return not failures


def run_topology(args, topology, nodes):
    directory = args.evidence / topology
    directory.mkdir()
    run_id = f"gh-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{topology.lower()}"
    fleet = Fleet(nodes, directory, run_id, args.tool_directory)
    bucket = "rv-heal-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    access, secret = secrets.token_hex(16), secrets.token_hex(24)
    os.environ.update(HEAL_ACCESS_KEY=access, HEAL_SECRET_KEY=secret)
    candidate = json.loads(args.candidate.read_text())
    if args.tool_directory:
        fleet.record("inspector-build", verify_inspector(args.tool_directory, candidate))
    binding = {"schemaVersion": 1, "runId": run_id, "jumpHostOnly": True, "exclusiveHealService": True,
               "leaseExpiresAt": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
               "bucketPrefix": bucket, "endpoints": {f"node{index}": f"http://rustfs-node{index}:9000"
                   for index in range(1, len(nodes) + 1)}, "defaultEndpoint": "node1",
               "candidateCommit": candidate["source_sha"], "harnessCommit": args.testing_commit,
               "candidatePackageSha256": candidate["package_sha256"], "candidateBinarySha256": args.binary_sha,
               "rcBinarySha256": hashlib.sha256(args.rc_bin.read_bytes()).hexdigest(),
               "maxInventoryObjects": 128, "maxInventoryBytes": 512 * 1024**2}
    fleet.record("binding", binding)
    result = {"topology": topology, "verdict": "RUNNING", "branches": {}, "cleanup": "PENDING"}
    active = []
    probe = Probe(binding)
    try:
        fleet.deploy(args.binary_sha, bucket, access, secret)
        initial = fleet.snapshot("deployed-topology")
        fleet.record("topology-proof", validate_topology(initial, topology))
        recorder = Recorder(probe, directory / "fixture")
        expected = seed(recorder, bucket, "DB", 128,
                        [0, 1, 4095, 4096, 4097, 1024**2 - 1, 1024**2, 1024**2 + 1, 5 * 1024**2 + 123])
        baseline = fleet.record("s3-baseline", probe.inventory(bucket, unversioned_current_tags=True))
        fleet.record("fixture-manifest", {"expected": expected, **verify_seed(expected, baseline)})
        before = fleet.snapshot("physical-baseline")
        fleet.record("fault", {"fault": "F0", "injected": False, "backgroundRepairDisabled": True})
        if args.case_id == "heal.admin-invalid-request":
            result["branches"] = invalid_requests(probe, fleet, bucket, before, baseline)
        if args.case_id == "heal.admin-authorization":
            try:
                from .authorization import authorization_requests
            except ImportError:
                from authorization import authorization_requests
            result["branches"] = authorization_requests(probe, fleet, bucket, before, baseline, active, BASE_OPTIONS)
        healthy_branches = (("normal", 1, "metadata_healthy"), ("deep", 2, "verified_healthy")) \
            if args.case_id == "heal.admin-healthy-noop" else ()
        for branch, mode, disposition in healthy_branches:
            receipts = []
            for entry in expected:
                key = entry["key"]
                path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="") + "/" + quote(key, safe="/")
                options = {**BASE_OPTIONS, "scanMode": mode}
                started = fleet.record("trigger-" + branch, probe.admin(path, [], json.dumps(options).encode()))
                require(started.get("httpStatus") == 200 and isinstance(started.get("response"), dict),
                        f"healthy Heal task admission failed (HTTP {started.get('httpStatus')})")
                token = started["response"].get("clientToken")
                require(isinstance(token, str) and bool(token), "healthy Heal task admission omitted its token")
                active.append((path, token))
                deadline = time.monotonic() + 120
                while True:
                    observed = fleet.record("status-" + branch, probe.admin(path, [("clientToken", token)], b""))
                    require(observed.get("httpStatus") == 200 and isinstance(observed.get("response"), dict),
                            "Heal status request failed")
                    terminal = observed.get("response") or {}
                    if terminal.get("summary") != "running":
                        break
                    require(time.monotonic() < deadline, "healthy Heal task exceeded deadline")
                    time.sleep(0.2)
                validate_healthy(terminal, bucket, key, disposition)
                active.remove((path, token))
                for _ in range(2):
                    repeated = fleet.record("repeat-" + branch, probe.admin(path, [("clientToken", token)], b""))
                    require(repeated.get("httpStatus") == 200, "repeated Heal status request failed")
                    validate_healthy(repeated.get("response") or {}, bucket, key, disposition)
                    require(canonical(repeated["response"]["outcome"]) == canonical(terminal["outcome"]),
                            "terminal responsibility changed on repeated status")
                receipts.append({"key": key, "token": token, "disposition": disposition})
            after = fleet.snapshot("physical-after-" + branch)
            require(physical_content(before) == physical_content(after), "healthy Heal changed physical fixture or format")
            inventory = fleet.record("s3-after-" + branch, probe.inventory(bucket, unversioned_current_tags=True))
            comparison = compare_inventories(baseline, inventory)
            result["branches"][branch] = {"verdict": "PASS", "receipts": receipts,
                                           "physicalUnchanged": True, "s3": comparison}
        result["verdict"] = "PASS"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(verdict="FAIL", failure={"type": type(exc).__name__, "message": str(exc)})
        if fleet.reserved:
            try:
                fleet.snapshot("physical-failure-scene")
            except Exception as capture:
                fleet.record("failure-scene-error", {"error": str(capture)})
    finally:
        for path, token in active:
            try:
                fleet.record("stop-unresolved", probe.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
            except Exception as exc:
                fleet.record("stop-error", {"error": str(exc)})
        result["cleanup"] = "PASS" if fleet.cleanup() else "FAIL"
        fleet.record("topology-result", result)
    return result


def main():
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Heal workflow interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--case-id", choices=("heal.admin-healthy-noop", "heal.admin-invalid-request", "heal.admin-authorization"),
                        default="heal.admin-healthy-noop")
    parser.add_argument("--testing-commit", required=True)
    parser.add_argument("--binary-sha", required=True)
    parser.add_argument("--rc-bin", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--tool-directory", type=Path)
    args = parser.parse_args()
    nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(len(nodes) >= 4 and len(set(nodes)) == len(nodes)
            and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes),
            "A01 H1/H2 requires the four current auto-testing Heal nodes")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", args.binary_sha)), "invalid installed binary digest")
    require(bool(re.fullmatch(r"[1-9][0-9]*", os.environ.get("GITHUB_RUN_ID", ""))), "workflow fleet identity required")
    results = []
    try:
        for topology, selected in (("H1", nodes[:1]), ("H2", nodes[:4])):
            result = run_topology(args, topology, selected)
            results.append(result)
            if result["cleanup"] != "PASS":
                break
    finally:
        save(args.evidence / "healthy-summary.json", {"caseId": args.case_id, "results": results,
             "cleanup": "PASS" if results and all(value["cleanup"] == "PASS" for value in results) else "FAIL",
             "complete": len(results) == 2 and all(value["verdict"] == "PASS" and value["cleanup"] == "PASS" for value in results)})
    require(len(results) == 2 and all(value["verdict"] == "PASS" and value["cleanup"] == "PASS" for value in results),
            "Heal case requires every H1/H2 branch; inspect topology-result evidence")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Heal validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
