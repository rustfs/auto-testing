#!/usr/bin/env python3
"""Disabled-by-default extended Heal scenarios on assigned auto-testing resources.

S3 fixtures, ACKs, Admin tasks, physical and data oracles are owned here. An
assigned resource adapter supplies lifecycle operations that the ordinary Heal
fleet cannot provide (power control, IO devices, extra pools and dependencies).
It receives JSON on stdin and returns raw, run-bound observations. It cannot
supply a PASS verdict. No release-validation checkout is used.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import time
from urllib.parse import quote

try:
    from .execution_policy import ENVIRONMENT_GATED_CASES
    from .heal_probe import Probe, canonical, compare_inventories, payload, redact, require, save, validate_binding
    from .heal_workload import Recorder, seed, verify_seed
    from .healthy import BASE_OPTIONS
    from .matrix import select, variant_topologies
    from .shards import files, validate_fault, validate_repaired_physical
except ImportError:
    from execution_policy import ENVIRONMENT_GATED_CASES
    from heal_probe import Probe, canonical, compare_inventories, payload, redact, require, save, validate_binding
    from heal_workload import Recorder, seed, verify_seed
    from healthy import BASE_OPTIONS
    from matrix import select, variant_topologies
    from shards import files, validate_fault, validate_repaired_physical

RUNTIME_ENV = "VALIDATION_HEAL_ENVIRONMENT_RUNTIME"
SHA = re.compile(r"[0-9a-f]{64}\Z")
SOAK_SECONDS = {"24h": 24 * 3600, "72h": 72 * 3600}
NATIVE_BRANCHES = {"native-ilm-fence", "native-replication-fence"}


class Blocked(ValueError):
    pass


class NotObserved(ValueError):
    pass


@dataclass(frozen=True)
class Scenario:
    code: str
    family: str
    # Actions run after actual Heal admission. Each has a raw evidence oracle.
    actions: tuple[str, ...]
    fault_kind: str = "missing"
    role: str = "data"
    interrupted: bool = False


def scenario(code: str, branch: str) -> Scenario:
    """Executable branches; strings are adapter operations, never shell text."""
    if code == "F08":
        actions = {
            "rolling-old-new": ("upgrade-next", "wait-mixed", "upgrade-rest"),
            "mixed-peer-status": ("upgrade-next", "mixed-status", "upgrade-rest"),
            "rollback-new-old": ("upgrade-all", "rollback-next", "wait-mixed", "rollback-rest"),
            "mixed-restart": ("upgrade-next", "restart-mixed", "mixed-status", "upgrade-rest"),
        }[branch]
        return Scenario(code, "upgrade", actions)
    if code == "F09":
        return Scenario(code, "host", ("replace-drive", "replacement-window", branch, "recover-host"), interrupted=True)
    if code == "G07":
        return Scenario(code, "ilm", ("ilm-" + branch, "ilm-window", "ilm-settle"))
    if code == "G08":
        return Scenario(code, "replication", (branch, "replication-window", "replication-settle"))
    if code == "H01":
        return Scenario(code, "pool", ("expand-pool", "rebalance-start", "pool-window", "pool-settle"),
                        role="parity" if "parity" in branch else "data")
    if code == "H02":
        actions = ["rebalance-start", "pool-window", "rebalance-stop"]
        if branch == "rebalance-service-restart": actions.append("restart-service")
        actions.extend(("rebalance-resume", "pool-window", "pool-settle"))
        return Scenario(code, "pool", tuple(actions))
    if code == "H03":
        return Scenario(code, "pool", ("decommission-start", "pool-window", "decommission-write-guard", "pool-settle", "source-offline"))
    if code == "H04":
        return Scenario(code, "pool", ("decommission-start", "pool-window", "decommission-cancel", "wait-inflight-exit",
                                        "decommission-clear", "decommission-resume", "capacity-lock-window"
                                        if branch == "capacity-lock-retry-window" else "pool-window", "pool-settle"))
    if code == "H05":
        return Scenario(code, "retired-pool", ("survivor-window",),
                        fault_kind="metadata-missing" if branch == "survivor-missing-metadata" else "missing")
    if code == "H07":
        return Scenario(code, "ec84", ("replace-drive", "replacement-window",
                        "crash-service" if branch.endswith("crash") else "restart-service", "replacement-settle"), interrupted=True)
    if code == "I01":
        return Scenario(code, "io", ("io-" + branch, "io-window", "io-recover"))
    if code == "I06":
        return Scenario(code, "dependency", ("dependency-" + branch, "dependency-window", "dependency-recover"))
    if code == "J05":
        require(branch in SOAK_SECONDS, "unregistered soak duration")
        return Scenario(code, "soak", ())
    raise ValueError(f"unknown environment scenario: {code}/{branch}")


def required_operations(case_id: str) -> set[str]:
    item = select(case_id)[0]
    common = {"preflight", "prepare", "stop", "start", "snapshot", "apply-fault", "recover", "cleanup", "verify-parts", "checkpoint", "metrics"}
    for branch in item["variants"]:
        if branch in NATIVE_BRANCHES: continue
        spec = scenario(item["code"], branch)
        common.update(spec.actions)
        if spec.family == "retired-pool": common.add("retire-pool-prepare")
        if spec.family in {"ilm", "replication", "dependency"}: common.add(spec.family + "-prepare")
        if spec.family == "soak": common.update(("soak-fault", "soak-recover", "metrics"))
    return common


def runtime_binding(value: dict, case_id: str, candidate: dict, testing_commit: str) -> dict:
    """Reject unassigned resources before the first command or mutation."""
    require(value.get("schemaVersion") == 1 and value.get("provider") == "auto-testing", "auto-testing runtime manifest required")
    binding = value["binding"]
    validate_binding(binding)
    require(binding["candidateCommit"] == candidate["source_sha"]
            and binding["candidatePackageSha256"] == candidate["package_sha256"]
            and binding["harnessCommit"] == testing_commit, "runtime is bound to a different candidate/checkout")
    if candidate.get("binary_sha256"):
        require(binding["candidateBinarySha256"] == candidate["binary_sha256"], "runtime binary differs from candidate")
    rc = Path(value["rcBinary"])
    require(rc.is_absolute() and rc.is_file() and not rc.is_symlink()
            and hashlib.sha256(rc.read_bytes()).hexdigest() == binding["rcBinarySha256"], "runtime rc identity differs")
    configured_nodes = os.environ.get("RUSTFS_NODES", "vm000 vm001 vm002 vm003").split()
    require(value.get("sshNodes") == configured_nodes, "runtime must reuse the current auto-testing Heal fleet")
    required = set(ENVIRONMENT_GATED_CASES[case_id]["requiredCapabilities"])
    require(required <= set(value.get("capabilities", [])), "runtime lacks required environment capabilities")
    selected = value.get("topologies", [])
    wanted = set(select(case_id)[0]["contract"]["probes"]["topology"].split(","))
    require(isinstance(selected, list) and len(set(selected)) == len(selected) and set(selected) == wanted,
            "runtime must bind every required topology")
    adapter = value.get("adapter", {})
    executable = Path(adapter.get("executable", ""))
    require(executable.is_absolute() and executable.is_file() and not executable.is_symlink()
            and os.access(executable, os.X_OK) and hashlib.sha256(executable.read_bytes()).hexdigest() == adapter.get("sha256"),
            "assigned resource adapter is absent, non-executable or changed")
    require(required_operations(case_id) <= set(adapter.get("operations", [])), "resource adapter omits a required operation")
    code = ENVIRONMENT_GATED_CASES[case_id]["code"]
    require(isinstance(value.get("inspectorSha256"), str) and SHA.fullmatch(value["inspectorSha256"]),
            "candidate-bound inspector digest required")
    resource_fields = {"F09": ("hostResourceId",), "G07": ("kmsKeyId", "tierEndpoint"),
                       "G08": ("replicaEndpoint",), "I01": ("faultDeviceId",),
                       "I06": ("kmsKeyId", "tierEndpoint", "tenantId", "dependencyNamespaceId")}
    require(all(isinstance(value.get(field), str) and value[field] for field in resource_fields.get(code, ())),
            "runtime omits a required dedicated resource binding")
    if code in {"G07", "G08", "I06"}:
        field = "replicaEndpoint" if code == "G08" else "tierEndpoint"
        require(value[field] in binding["endpoints"] and value[field] != binding["defaultEndpoint"],
                "remote dependency/replica must bind a separate endpoint")
    if code == "F08":
        pair = value.get("releasePair", {})
        releases = pair.get("releases", [])
        require(pair.get("rollbackCompatible") is True and SHA.fullmatch(pair.get("compatibilityEvidenceSha256", ""))
                and len(releases) == 2 and len({r.get("packageSha256") for r in releases}) == 2
                and all(SHA.fullmatch(r.get("packageSha256", "")) and SHA.fullmatch(r.get("binarySha256", ""))
                        and re.fullmatch(r"[0-9a-f]{40}", r.get("sourceCommit", "")) and r.get("packageUrl") for r in releases)
                and any(r["binarySha256"] == binding["candidateBinarySha256"] for r in releases),
                "F08 requires two immutable compatible releases including the candidate")
    if code == "J05":
        expiry = datetime.fromisoformat(binding["leaseExpiresAt"].replace("Z", "+00:00"))
        # Both source windows run sequentially; include final verification/recovery.
        require((expiry - datetime.now(timezone.utc)).total_seconds() >= len(wanted) * sum(SOAK_SECONDS.values()) + 3600,
                "soak lease cannot cover all actual 24h/72h windows and recovery")
        limits = value.get("soakLimits", {})
        require(all(type(limits.get(k)) is int and limits[k] >= 0 for k in
                    ("rssBytes", "openFds", "queuedTasks", "unresolvedRecords", "temporaryBytes")), "soak resource limits required")
    return binding


class Adapter:
    """A resource operation protocol, similar to the existing Pool suite hooks."""
    def __init__(self, runtime, evidence):
        self.runtime, self.evidence = runtime, evidence
        self.sequence = 0
        self.reserved = False
        self.calls = []

    def record(self, label, value):
        self.sequence += 1
        save(self.evidence / f"{self.sequence:06d}-{label}.json", value)
        return value

    def call(self, operation, *, scope, **parameters):
        binding = self.runtime["binding"]
        if operation not in {"recover", "cleanup", "soak-recover"}:
            validate_binding(binding)
        require(operation in self.runtime["adapter"]["operations"], "unregistered resource operation")
        executable = Path(self.runtime["adapter"]["executable"])
        require(hashlib.sha256(executable.read_bytes()).hexdigest() == self.runtime["adapter"]["sha256"], "resource adapter changed during run")
        request = {"schemaVersion": 1, "runId": binding["runId"], "sequence": len(self.calls) + 1,
                   "candidateCommit": binding["candidateCommit"], "scope": scope, "operation": operation,
                   "bucketPrefix": binding["bucketPrefix"], "leaseExpiresAt": binding["leaseExpiresAt"],
                   "parameters": parameters}
        digest = hashlib.sha256(canonical(request).encode()).hexdigest()
        self.calls.append(request)
        self.record("request-" + operation, request)
        if operation == "prepare": self.reserved = True  # A lost ACK can leave partial setup.
        # Files retain partial stdout/stderr even on timeout. Never emit credentials
        # from adapter diagnostics to workflow stdout.
        timeout = 600 if operation in {"prepare", "cleanup", "pool-settle", "replacement-settle"} else 180
        def safe_text(raw):
            text = redact(raw.decode(errors="replace"))
            for name in self.runtime.get("redactionEnvironmentVariables", []):
                secret = os.environ.get(name)
                if secret: text = text.replace(secret, "[REDACTED]")
            return text
        # Raw adapter output lives outside the uploaded evidence directory.
        # Preserve bounded, redacted diagnostics even when the child times out.
        with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
            try:
                result = subprocess.run([str(executable)], input=json.dumps(request).encode(), stdout=out, stderr=err, timeout=timeout)
            finally:
                out.seek(0); raw = out.read(16 * 1024**2 + 1)
                err.seek(0); diagnostic = err.read(1024**2)
                self.record("adapter-diagnostic", {"operation": operation, "stdout": safe_text(raw), "stderr": safe_text(diagnostic)})
        require(result.returncode == 0, f"resource adapter {operation} failed; inspect retained diagnostics")
        require(len(raw) <= 16 * 1024**2, "adapter observation exceeds budget")
        response = json.loads(raw)
        require(response.get("schemaVersion") == 1 and response.get("requestSha256") == digest and response.get("runId") == binding["runId"]
                and response.get("sequence") == request["sequence"] and response.get("scope") == scope
                and response.get("operation") == operation and isinstance(response.get("observation"), dict)
                and "verdict" not in response["observation"], "resource observation is not bound to the original operation")
        return self.record("response-" + operation, response)["observation"]


def assert_topology(observed, topology, candidate_sha, *, allow_old=None):
    disks = observed.get("disks", [])
    expected_min = {"H1": 4, "H2": 16, "H3": 12, "H4": 24, "H5": 16, "H6": 4}[topology]
    require(len(disks) >= expected_min, "insufficient independent endpoints")
    identities = set()
    slots = set()
    sets = {}
    for disk in disks:
        require(all(type(disk.get(k)) is int and disk[k] >= 0 for k in ("poolIndex", "setIndex", "setSlot")),
                "invalid physical pool/set/slot")
        identity = (disk["hostname"], disk["mountDevice"])
        slot = (disk["poolIndex"], disk["setIndex"], disk["setSlot"])
        require(identity not in identities and slot not in slots and disk.get("diskUUID"), "directories alias a device or erasure slot")
        identities.add(identity); slots.add(slot)
        sets.setdefault(slot[:2], []).append(disk)
    require(topology != "H5" or len({pool for pool, _ in sets}) >= 2, "H5 omitted separate pools")
    if topology in {"H3", "H4"}:
        require(len(sets) >= (2 if topology == "H4" else 1), "EC8+4 omitted required sets")
        require(all(len(group) == 12 and all(d["dataShards"] == 8 and d["parityShards"] == 4 for d in group)
                    for group in sets.values()), "actual set is not EC8+4")
    processes = observed.get("processes", [])
    require(processes and {p["hostname"] for p in processes} == {d["hostname"] for d in disks}, "process attestation omitted hosts")
    allowed = {candidate_sha} | ({allow_old} if allow_old else set())
    require(all(p.get("binarySha256") in allowed and type(p.get("pid")) is int and p["pid"] > 0 for p in processes),
            "service process is not a verified immutable candidate")
    return sets


def target_from_snapshot(snapshot, bucket, entry, role, *, set_index=None):
    indexed = files(snapshot)
    cohort = []
    for node, observation in snapshot.items():
        for form in observation["formats"]:
            disk = form["disk"]
            meta = indexed.get((node, disk, f"{bucket}/{entry['key']}/xl.meta"), {}).get("metadata", {})
            versions = [v for v in meta.get("versions", []) if (v.get("versionId") or "null") == entry["versionId"]]
            if not versions: continue
            require(len(versions) == 1, "ambiguous acknowledged version")
            version = versions[0]
            erasure = version["erasure"]
            layout = form["value"]["xl"]
            group = next(i for i, ids in enumerate(layout["sets"]) if layout["this"] in ids)
            if set_index is not None and group != set_index: continue
            pool = form.get("poolIndex", 0)
            slot = layout["sets"][group].index(layout["this"])
            require(erasure["index"] == erasure["distribution"][slot] and version.get("deleted") is False,
                    "shard slot/role does not match actual format")
            part = version["parts"][0]
            relative = f"{bucket}/{entry['key']}/{version['dataDir']}/part.{part['number']}"
            physical = indexed.get((node, disk, relative))
            if not physical: continue
            actual_role = "data" if erasure["index"] <= erasure["data_blocks"] else "parity"
            cohort.append({"node": node, "disk": disk, "relativePath": relative, "sha256": physical["sha256"],
                           "size": physical["size"], "role": actual_role, "index": erasure["index"],
                           "versionId": entry["versionId"], "bucket": bucket, "key": entry["key"],
                           "partNumber": part["number"], "poolIndex": pool, "setIndex": group,
                           "diskUUID": layout["this"], "dataDir": version["dataDir"], "setSlot": slot,
                           "logical": version, "metadata": meta})
    require(cohort, "no external physical target for the ACK fixture")
    first = cohort[0]["logical"]["erasure"]
    require(len(cohort) == first["data_blocks"] + first["parity_blocks"]
            and len({(c["poolIndex"], c["setIndex"]) for c in cohort}) == 1
            and len({c["index"] for c in cohort}) == len(cohort), "target lacks one complete erasure authority")
    target = next((c for c in cohort if c["role"] == role), None)
    require(target is not None, "requested shard role is absent")
    return {**target, "cohort": cohort}


def assert_overlap(observed, *, task_id, kind):
    # Server events share one provider clock, and must intersect a task running
    # interval. A broad wall-clock client request duration is not a witness.
    intervals = observed.get("intervals", [])
    require(observed.get("clockDomain") and intervals, "overlap lacks a common server observation clock")
    heal = [v for v in intervals if v.get("kind") == "heal" and v.get("taskId") == task_id]
    other = [v for v in intervals if v.get("kind") == kind and v.get("taskId") != task_id]
    for row in intervals:
        require(type(row.get("startNs")) is int and type(row.get("endNs")) is int
                and row["endNs"] > row["startNs"] and row.get("evidenceId"), "invalid activity interval")
    if not any(max(a["startNs"], b["startNs"]) < min(a["endNs"], b["endNs"]) for a in heal for b in other):
        raise NotObserved("actual Heal/" + kind + " overlap not observed")


def assert_terminal(status, bucket, expected, *, allowed_extra=(), allowed_absent=()):
    outcome = status.get("outcome", {})
    require(status.get("summary") == "finished" and not status.get("truncated", False)
            and outcome.get("coverage") == "complete" and outcome.get("objectsTruncated") is False
            and outcome.get("execution", {}).get("state") == "completed", "task did not finish with complete canonical coverage")
    wanted = {(x["key"], x["versionId"]) for x in expected}
    objects = outcome.get("objects", [])
    actual = set()
    repaired = healthy = 0
    for row in objects:
        identity = row.get("identity", {})
        pair = (identity.get("object"), identity.get("versionId") or "null")
        require(pair not in actual and identity.get("bucket") == bucket and identity.get("bucketIncarnationId")
                and type(identity.get("poolIndex")) is int and type(identity.get("setIndex")) is int,
                "receipt lacks exact bucket/version/pool/set responsibility")
        actual.add(pair)
        state = row.get("disposition", {}).get("state")
        valid = {"repaired", "verified_healthy", "delete_marker_healthy"}
        if pair in set(allowed_absent): valid.add("already_absent")
        require(state in valid, "unknown/deferred/failed/skipped is not repair success")
        if state == "delete_marker_healthy":
            require(any(e["key"] == pair[0] and e["versionId"] == pair[1] and e["deleteMarker"]
                        for e in [*expected, *allowed_extra]), "data version received marker disposition")
        repaired += state == "repaired"
        healthy += state != "repaired"
    permitted = wanted | {(e["key"], e["versionId"]) for e in allowed_extra}
    require(wanted <= actual <= permitted, "receipt identities differ from the complete ACK fixture")
    counters = outcome.get("counters", {})
    expected_counts = {"processed": len(actual), "healed": repaired, "unchanged": healthy,
                       "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}
    require(all(type(counters.get(k)) is type(v) and counters[k] == v for k, v in expected_counts.items()),
            "canonical counters disagree with complete per-object receipts")


def assert_action(action, observed, *, task_id, runtime):
    """Raw infrastructure evidence oracles. An adapter's booleans cannot pass."""
    if action == "survivor-window":
        interval = observed.get("taskInterval", {})
        require(interval.get("taskId") == task_id and type(interval.get("startNs")) is int
                and type(interval.get("endNs")) is int and interval["endNs"] > interval["startNs"]
                and observed.get("sourceReachable") is False, "survivor Heal interval or retired-source isolation is missing")
    elif action.endswith("window"):
        kind = {"replacement-window": "replacement", "ilm-window": "ilm", "replication-window": "replication",
                "pool-window": "pool", "survivor-window": "heal", "io-window": "io", "dependency-window": "dependency",
                "capacity-lock-window": "capacity-lock"}[action]
        assert_overlap(observed, task_id=task_id, kind=kind)
        if action == "capacity-lock-window":
            raise Blocked("H04 exact capacity-lock owner/commit barrier still needs reviewed native evidence")
    if action in {"dedicated-vm-forced-stop", "dedicated-host-power-cycle"}:
        wanted = "platform-forced-stop" if action.endswith("forced-stop") else "physical-power-cycle"
        require(observed.get("eventKind") == wanted and observed.get("platformActivityId")
                and observed.get("resourceId") == runtime["hostResourceId"]
                and observed.get("bootIdBefore") and observed.get("bootIdAfter")
                and observed["bootIdBefore"] != observed["bootIdAfter"]
                and observed.get("recoveryChannel") == "out-of-band", "power event is not the assigned H6 platform event")
    if action in {"restart-service", "crash-service", "restart-mixed"}:
        require(observed.get("pidBefore") != observed.get("pidAfter") and observed.get("rootBefore")
                and observed.get("rootBefore") == observed.get("rootAfter")
                and observed.get("signal") == ("SIGKILL" if action == "crash-service" else "SIGTERM"),
                "restart did not preserve the original data root/process interruption")
    if action.startswith("upgrade-") or action.startswith("rollback-"):
        pair = runtime["releasePair"]
        require(pair.get("rollbackCompatible") is True and pair.get("compatibilityEvidenceSha256")
                and len({p["packageSha256"] for p in pair["releases"]}) == 2, "immutable release pair/compatibility proof absent")
        allowed = {p["binarySha256"] for p in pair["releases"]}
        processes = observed.get("processes", [])
        require(processes and all(p.get("binarySha256") in allowed and p.get("rootBefore") == p.get("rootAfter")
                                  and p.get("rootBefore") for p in processes), "upgrade used unverified bytes or replaced data roots")
    if action in {"mixed-status", "wait-mixed"}:
        peers = observed.get("peers", [])
        require(len({p.get("binarySha256") for p in peers}) == 2 and len(peers) >= 2, "no real mixed-release peers observed")
        require(all(p.get("coverage") in {"partial", "unsupported"} for p in peers if not p.get("capabilitySupported")),
                "unsupported peer reported complete status")
    if action.startswith("io-") and action not in {"io-recover", "io-window"}:
        fault = action[3:]
        probe = observed.get("probe", {})
        require(probe.get("uid") == observed.get("serviceUid") and probe.get("mountNamespace") == observed.get("serviceMountNamespace")
                and probe.get("path") and observed.get("deviceId") == runtime["faultDeviceId"], "IO fault is not in the service namespace/device")
        if fault == "bounded-latency":
            require(type(probe.get("elapsedMs")) is int and probe["elapsedMs"] >= runtime["ioLatencyMs"], "bounded latency was not witnessed")
        elif fault == "short-read":
            require(type(probe.get("bytesRead")) is int and 0 <= probe["bytesRead"] < probe.get("bytesRequested", 0), "short read was not witnessed")
        else:
            require(probe.get("errno") == 5, "EIO was not witnessed")
    if action.startswith("dependency-") and action not in {"dependency-window", "dependency-recover"}:
        wanted = "AccessDenied" if action.endswith("denied") else "NoSuchVersion" if action.endswith("version-unavailable") else "Unavailable"
        require(observed.get("errorCode") == wanted and observed.get("tenantId") == runtime["tenantId"]
                and observed.get("namespaceId") == runtime["dependencyNamespaceId"], "dependency fault lacks isolated tenant/namespace/error evidence")
        outcomes = observed.get("objectDispositions", [])
        require(outcomes and all(o.get("state") in {"failed", "deferred", "unknown"} and o.get("reason") for o in outcomes),
                "dependency error was treated as absent/healthy/removable")
    if action in {"pool-settle", "replication-settle", "ilm-settle", "replacement-settle"}:
        tasks = observed.get("tasks", [])
        require(tasks and all(t.get("taskId") and t.get("state") == "completed" and not t.get("error") for t in tasks),
                "background operation has an incomplete/failed task")
    if action in {"decommission-stop", "decommission-cancel", "rebalance-stop"}:
        require(observed.get("taskId") and observed.get("state") in {"stopped", "cancelled"}
                and observed.get("healTaskId") == task_id and observed.get("healState") == "running", "pool cancellation also cancelled Heal or lost task ownership")
    if action == "wait-inflight-exit":
        require(observed.get("inflight") == 0 and observed.get("cancelCommitNs", 0) < observed.get("lastExitNs", 0),
                "inflight work did not exit after durable cancellation")
    if action == "decommission-clear":
        require(observed.get("oldGeneration") and observed.get("inflight") == 0 and observed.get("state") == "cleared",
                "clear ran before old inflight exit")
    if action in {"decommission-resume", "rebalance-resume"}:
        require(observed.get("newGeneration") and observed.get("oldGeneration")
                and observed["newGeneration"] != observed["oldGeneration"], "resume reused an old generation")
    if action == "decommission-write-guard":
        require(observed.get("httpStatus") in {405, 409, 503} and observed.get("errorCode") in {"MethodNotAllowed", "OperationAborted", "SlowDown"},
                "decommission did not enforce the frozen write restriction")
    if action == "source-offline":
        require(observed.get("sourceEndpoints") and observed.get("reachableEndpoints") == []
                and observed.get("survivorEndpoints"), "retired source remains reachable")


class Runner:
    def __init__(self, runtime, candidate, testing_commit, evidence, *, adapter=None, probe_factory=Probe,
                 clock=time.monotonic, sleep=time.sleep):
        self.runtime, self.candidate, self.testing_commit, self.evidence = runtime, candidate, testing_commit, evidence
        self.adapter = adapter or Adapter(runtime, evidence)
        self.probe_factory, self.clock, self.sleep = probe_factory, clock, sleep
        self.active = []

    def admit(self, probe, bucket):
        path = "/rustfs/admin/v3/heal/" + quote(bucket, safe="")
        options = {**BASE_OPTIONS, "recursive": True, "pool": None, "set": None}
        row = self.adapter.record("heal-admission", probe.admin(path, [], json.dumps(options).encode()))
        token = row.get("response", {}).get("clientToken")
        require(row.get("httpStatus") == 200 and isinstance(token, str) and token, "Heal was not admitted with an owned token")
        self.active.append((probe, path, token))
        return path, token

    def wait_task(self, probe, path, token):
        deadline = self.clock() + 180
        while True:
            row = self.adapter.record("heal-status", probe.admin(path, [("clientToken", token)], b""))
            require(row.get("httpStatus") == 200 and isinstance(row.get("response"), dict), "Heal status unavailable")
            status = row["response"]
            if status.get("summary") != "running": break
            require(self.clock() < deadline, "Heal task exceeded its registered budget")
            self.sleep(.5)
        for name in probe.binding["endpoints"]:
            other = self.probe_factory(probe.binding, name)
            repeated = self.adapter.record("heal-terminal-peer", other.admin(path, [("clientToken", token)], b""))
            require(repeated.get("httpStatus") == 200 and repeated.get("response", {}).get("summary") == status.get("summary")
                    and canonical(repeated["response"].get("outcome")) == canonical(status.get("outcome")), "peer terminal responsibility differs")
        self.active.remove((probe, path, token))
        return status

    def fixture(self, probe, directory, bucket, spec, branch):
        recorder = Recorder(probe, directory / "ACKs")
        fixture = "DB" if spec.code == "I01" else "DV"
        count = 257 if spec.interrupted else 8
        expected = seed(recorder, bucket, fixture, count, [5 * 1024**2 + 123])
        if spec.code == "H03" and branch == "versioned-multipart":
            key = "multipart/0000.bin"
            upload = recorder.call("create_multipart_upload", {"Bucket": bucket, "Key": key})
            parts, full = [], b""
            for number, size in enumerate((5 * 1024**2 + 17, 1024**2 + 47), 1):
                body = payload(f"{bucket}/{key}/{number}", size); full += body
                response = recorder.call("upload_part", {"Bucket": bucket, "Key": key, "UploadId": upload["UploadId"], "PartNumber": number}, body)
                parts.append({"PartNumber": number, "ETag": response["ETag"]})
            response = recorder.call("complete_multipart_upload", {"Bucket": bucket, "Key": key, "UploadId": upload["UploadId"], "MultipartUpload": {"Parts": parts}})
            expected.append({"key": key, "versionId": response["VersionId"], "isLatest": True, "deleteMarker": False,
                             "payload": {"size": len(full), "sha256": hashlib.sha256(full).hexdigest()}})
        if spec.code in {"I06", "G07"}:
            key = "encrypted.bin"; body = payload(bucket + "/encrypted", 5 * 1024**2 + 73)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key, "ServerSideEncryption": "aws:kms",
                                                    "SSEKMSKeyId": self.runtime["kmsKeyId"]}, body)
            require(response.get("ServerSideEncryption") == "aws:kms" and response.get("SSEKMSKeyId"), "encrypted fixture was not encrypted")
            expected.append({"key": key, "versionId": response["VersionId"], "isLatest": True, "deleteMarker": False,
                             "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
        baseline = self.adapter.record("baseline-inventory", probe.inventory(bucket, unversioned_current_tags=fixture == "DB"))
        verify_seed(expected, baseline)
        return recorder, expected, baseline

    def soak(self, scope, recorder, bucket, expected, topology):
        duration = SOAK_SECONDS[scope["branch"]]
        started = self.clock(); deadline = started + duration
        writes = 0
        metrics = []
        schedule = ("drive-outage", "service-restart", "dependency-outage")
        while self.clock() < deadline:
            body = payload(f"{bucket}/soak/{writes}", 1024)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": "soak.bin"}, body)
            require(response.get("VersionId") not in {None, "null"}, "soak write lost a version ACK")
            for entry in expected:
                if entry["key"] == "soak.bin": entry["isLatest"] = False
            expected.append({"key": "soak.bin", "versionId": response["VersionId"], "isLatest": True, "deleteMarker": False,
                             "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
            old = [e for e in expected if e["key"] == "soak.bin" and not e["isLatest"]]
            if len(old) > 64:
                entry = old[0]
                deleted = recorder.call("delete_object", {"Bucket": bucket, "Key": entry["key"], "VersionId": entry["versionId"]})
                require(deleted.get("VersionId") == entry["versionId"], "soak delete ACK differs from the exact version")
                expected.remove(entry)
            kind = schedule[writes % len(schedule)]
            # The control operation must acknowledge the real fault and undo,
            # separately from the data ACK. Both are retained on failure.
            witness = self.adapter.call("soak-fault", scope=scope, kind=kind, iteration=writes, bucket=bucket)
            require(witness.get("faultId") and witness.get("kind") == kind and witness.get("injectedAtNs"), "soak fault has no actual witness")
            restored = self.adapter.call("soak-recover", scope=scope, faultId=witness["faultId"])
            require(restored.get("faultId") == witness["faultId"] and restored.get("remainingFaults") == [],
                    "soak fault was not recovered")
            sample = self.adapter.call("metrics", scope=scope)
            for field in ("rssBytes", "openFds", "queuedTasks", "unresolvedRecords", "temporaryBytes"):
                limit = self.runtime["soakLimits"][field]
                require(type(sample.get(field)) is int and 0 <= sample[field] <= limit, "soak resource threshold exceeded: " + field)
            require(sample.get("panicCount") == 0 and sample.get("busyLoopCount") == 0, "soak observed a panic or busy loop")
            metrics.append(sample); writes += 1
            self.sleep(min(120, max(0, deadline - self.clock())))
        elapsed = self.clock() - started
        require(elapsed >= duration and writes > 0, "short run cannot satisfy a 24h/72h window")
        return self.adapter.record("soak-window", {"durationSeconds": duration, "actualElapsedSeconds": elapsed,
                                  "acknowledgedWrites": writes, "samples": len(metrics), "topology": topology})

    def branch(self, case_id, topology, branch):
        recipe = select(case_id)[0]
        scope = {"caseId": case_id, "topology": topology, "branch": branch}
        if branch in NATIVE_BRANCHES:
            return {**scope, "verdict": "BLOCKED", "cleanup": "NOT_RUN", "reason": "reviewed candidate-bound " + branch + " is unavailable"}
        spec = scenario(recipe["code"], branch)
        directory = self.evidence / f"{topology}-{branch}"
        directory.mkdir()
        bucket = self.runtime["binding"]["bucketPrefix"] + "-" + hashlib.sha256(canonical(scope).encode()).hexdigest()[:8]
        binding = self.runtime["binding"]
        probe = self.probe_factory(binding)
        result = {**scope, "verdict": "RUNNING", "cleanup": "PENDING"}
        reserved = False
        def control(operation, **params):
            return self.adapter.call(operation, scope=scope, bucket=bucket, **params)
        try:
            preflight = control("preflight")
            assert_topology(preflight, topology, binding["candidateBinarySha256"], allow_old=self.runtime.get("oldBinarySha256"))
            reserved = True
            control("prepare", candidate=self.candidate)
            recorder, expected, baseline = self.fixture(probe, directory, bucket, spec, branch)
            if spec.family in {"ilm", "replication", "dependency"}:
                dependency_prepared = control(spec.family + "-prepare", fixture=expected, branch=branch)
            if spec.family == "retired-pool":
                prepared = control("retire-pool-prepare", fixture=expected)
                assert_action("source-offline", prepared, task_id="", runtime=self.runtime)
            if spec.family == "soak":
                self.soak(scope, recorder, bucket, expected, topology)
                baseline = None  # The final oracle uses every newly ACKed version.
            control("stop")
            original = control("snapshot", quiescent=True)["snapshot"]
            faulted = original
            targets = []
            groups = sorted({disk["setIndex"] for disk in preflight["disks"]}) if spec.code == "H07" else [None]
            for group in groups:
                target = None
                for entry in expected:
                    if entry["deleteMarker"]: continue
                    try: target = target_from_snapshot(original, bucket, entry, spec.role, set_index=group)
                    except ValueError: continue
                    break
                require(target is not None, "fixture does not cover a required erasure set")
                request = {"target": target, "kind": "missing" if spec.fault_kind == "metadata-missing" else spec.fault_kind,
                           "faultId": recipe["code"] + "-" + str(len(targets))}
                if spec.fault_kind == "metadata-missing":
                    request["target"] = {**target, "relativePath": f"{bucket}/{target['key']}/xl.meta"}
                    physical = files(original)[(target["node"], target["disk"], request["target"]["relativePath"])]
                    request["target"].update(sha256=physical["sha256"], size=physical["size"])
                before_fault = faulted
                targets.append(request["target"])
                receipt = control("apply-fault", **request)
                faulted = control("snapshot", quiescent=True)["snapshot"]
                validate_fault(before_fault, faulted, request["target"], receipt)
            control("start")
            witness = control("snapshot", quiescent=False)["snapshot"]
            require(files(witness) == files(faulted), "NOT_OBSERVED: target changed before Heal admission")
            preliminary = {}
            if spec.family in {"io", "dependency"}:
                action = spec.actions[0]
                preliminary[action] = control(action, targets=targets, fixture=expected)
                assert_action(action, preliminary[action], task_id="", runtime=self.runtime)
            admitted_fixture = list(expected)
            path, task_id = self.admit(probe, bucket)
            durable = control("checkpoint", taskId=task_id, fixture=expected)
            records = durable.get("records", [])
            require(records and all(r.get("taskId") == task_id and r.get("bucketIncarnationId")
                    and SHA.fullmatch(r.get("sha256", "")) and r.get("durableProgress", 0) > 0 for r in records),
                    "original durable responsibility was not witnessed")
            # Keep S3 foreground writes in our ACK ledger during pool/upgrade IO.
            if spec.family in {"pool", "upgrade", "replication"}:
                body = payload(bucket + "/foreground", 1024)
                response = recorder.call("put_object", {"Bucket": bucket, "Key": "foreground.bin"}, body)
                version = response.get("VersionId", "null")
                expected.append({"key": "foreground.bin", "versionId": version, "isLatest": True, "deleteMarker": False,
                                 "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
                # Add a new task after the first one if its snapshot predates this
                # ACK; never pretend it had already included the new object.
            observations = dict(preliminary)
            if spec.code == "G08" and branch == "replicate-delete":
                key = next(e["key"] for e in expected if e["isLatest"] and not e["deleteMarker"])
                deleted = recorder.call("delete_object", {"Bucket": bucket, "Key": key})
                require(deleted.get("DeleteMarker") is True and deleted.get("VersionId") not in {None, "null"},
                        "replicated delete has no exact marker ACK")
                for entry in expected:
                    if entry["key"] == key: entry["isLatest"] = False
                marker = {"key": key, "versionId": deleted["VersionId"], "deleteMarker": True, "isLatest": True}
                expected.append(marker)
            stop_writes = threading.Event()
            def foreground():
                number = 0
                while not stop_writes.is_set():
                    body = payload(f"{bucket}/live/{number}", 1024)
                    key = f"live/{number:06d}.bin"
                    response = recorder.call("put_object", {"Bucket": bucket, "Key": key}, body)
                    expected.append({"key": key, "versionId": response.get("VersionId", "null"), "isLatest": True,
                                     "deleteMarker": False, "payload": {"size": len(body), "sha256": hashlib.sha256(body).hexdigest()}})
                    number += 1
                    stop_writes.wait(2)
                return number
            with ThreadPoolExecutor(max_workers=1) as workers:
                future = workers.submit(foreground) if spec.family in {"host", "upgrade", "pool", "replication"} else None
                try:
                    for action in spec.actions:
                        if action in preliminary: continue
                        observation = control(action, taskId=task_id, targets=targets, fixture=list(expected))
                        observations[action] = observation
                        assert_action(action, observation, task_id=task_id, runtime=self.runtime)
                finally:
                    stop_writes.set()
                if future is not None:
                    require(future.result() > 0, "no foreground ACK was recorded during lifecycle operations")
            if spec.code == "G07" and branch in {"current-expiry", "noncurrent-expiry"}:
                expired = observations["ilm-settle"].get("expiredVersions", [])
                require(expired and all(e.get("eventId") and e.get("ruleId") and e.get("versionId") for e in expired), "expiration has no actual version/rule event")
                gone = {(e["key"], e["versionId"]) for e in expired}
                require(gone <= {(e["key"], e["versionId"]) for e in expected}, "ILM event refers to an unacknowledged version")
                if branch == "current-expiry":
                    require(all(e.get("deleteMarkerVersionId") for e in expired), "current expiry lacks the created marker identity")
                    for event in expired:
                        for entry in expected:
                            if entry["key"] == event["key"]: entry["isLatest"] = False
                        expected.append({"key": event["key"], "versionId": event["deleteMarkerVersionId"], "deleteMarker": True, "isLatest": True})
                    gone = set()  # Current expiration creates a marker; old versions survive.
                else:
                    require(all(not e["isLatest"] for e in expected if (e["key"], e["versionId"]) in gone),
                            "noncurrent expiration selected a current version")
                    expected = [e for e in expected if (e["key"], e["versionId"]) not in gone]
            terminal = self.wait_task(probe, path, task_id)
            # Keep the complete pre-admission denominator. A task cannot
            # choose its own expected identities by returning fewer receipts.
            assert_terminal(terminal, bucket, admitted_fixture, allowed_extra=expected,
                            allowed_absent=gone if spec.code == "G07" and branch in {"current-expiry", "noncurrent-expiry"} else ())
            control("stop")
            repaired = control("snapshot", quiescent=True)["snapshot"]
            if spec.family in {"io", "upgrade", "dependency", "retired-pool"} and not any(e["key"] == "foreground.bin" for e in expected):
                for target in targets:
                    if spec.fault_kind != "metadata-missing": validate_repaired_physical(original, repaired, target)
            proof = control("verify-parts", fixture=expected, targets=targets, beforeAnyTargetGET=True)
            require(proof.get("toolSha256") == self.runtime["inspectorSha256"] and proof.get("candidateCommit") == binding["candidateCommit"],
                    "part verification tool differs from the candidate-bound inspector")
            verified = proof.get("parts", [])
            require(verified and all(p.get("sha256") and p.get("checksumVerified") is True for p in verified), "physical shard verification is incomplete")
            physical_ids = {(p["key"], p["versionId"]) for p in verified}
            require({(e["key"], e["versionId"]) for e in expected if not e["deleteMarker"]} <= physical_ids,
                    "physical proof omitted an acknowledged version")
            if spec.family in {"pool", "retired-pool"}:
                movements = proof.get("movements", [])
                require(movements and all(m.get("sourcePool") != m.get("targetPool") and m.get("targetCommitNs", 0) > 0
                        and m.get("sourceDeleteNs", 0) >= m["targetCommitNs"] for m in movements), "source removed before target authority committed")
                if spec.code in {"H03", "H05"}:
                    require(proof.get("sourceReachable") is False and proof.get("retiredRootHashesBefore")
                            and proof["retiredRootHashesBefore"] == proof.get("retiredRootHashesAfter"), "retired pool changed or remains reachable")
            control("start")
            final = self.adapter.record("final-inventory", probe.inventory(bucket, unversioned_current_tags=spec.code == "I01"))
            verify_seed(expected, final)
            if baseline is not None and spec.family not in {"pool", "upgrade", "replication", "ilm"}: compare_inventories(baseline, final)
            if spec.family in {"replication", "ilm", "dependency"}:
                endpoint = self.runtime["replicaEndpoint"] if spec.family == "replication" else self.runtime["tierEndpoint"]
                remote = self.probe_factory(binding, endpoint)
                remote_inventory = self.adapter.record("remote-inventory", remote.inventory(bucket))
                mapping = observations.get("replication-settle", observations.get("ilm-settle", observations.get("dependency-recover", {}))).get("versionMapping", [])
                require(mapping, "remote versions have no exact mapping")
                remote_ids = {(e["key"], e["versionId"]): e for e in remote_inventory["entries"]}
                local_ids = {(e["key"], e["versionId"]): e for e in final["entries"]}
                for row in mapping:
                    left = local_ids[(row["key"], row["sourceVersionId"])]; right = remote_ids[(row["key"], row["targetVersionId"])]
                    require(left.get("payload") == right.get("payload") and left["deleteMarker"] == right["deleteMarker"], "remote payload or marker differs")
                require({(r["key"], r["sourceVersionId"]) for r in mapping} == set(local_ids), "remote mapping omits local acknowledged versions")
            if spec.code == "H04" and branch == "capacity-lock-retry-window":
                raise Blocked("exact capacity-lock retry barrier requires reviewed candidate native evidence")
            result.update(verdict="BLOCKED", checksCompleted=True,
                          reason="environment scenario checks completed; assigned adapter evidence requires source-assertion review before canonical PASS")
        except (Exception, KeyboardInterrupt) as exc:
            result.update(verdict="BLOCKED" if isinstance(exc, Blocked) else "NOT_OBSERVED" if isinstance(exc, NotObserved) else "FAIL",
                          reason=str(exc))
        finally:
            if reserved:
                try:
                    for owner, path, token in tuple(self.active):
                        self.adapter.record("cancel-original-task", owner.admin(path, [("clientToken", token), ("forceStop", "true")], b""))
                        self.active.remove((owner, path, token))
                    control("recover", lastACKsOnly=True, originalFailureRetained=result["verdict"] != "PASS")
                    cleaned = control("cleanup")
                    require(cleaned.get("remainingOwnedPaths") == [] and cleaned.get("activeOwnedServices") == []
                            and cleaned.get("remainingFaults") == [] and cleaned.get("pendingMutations") == [], "cleanup left a dirty environment")
                    result["cleanup"] = "PASS"
                except (Exception, KeyboardInterrupt) as exc:
                    result.update(cleanup="FAIL", recoveryError=str(exc))
            else: result["cleanup"] = "NOT_RUN"
            self.adapter.record("branch-result", result)
        return result

    def run(self, case_id):
        recipe = select(case_id)[0]
        mapping = variant_topologies(recipe)
        results = []
        for topology in sorted(self.runtime["topologies"]):
            for branch in recipe["variants"]:
                if topology not in mapping[branch]: continue
                results.append(self.branch(case_id, topology, branch))
                if results[-1]["cleanup"] == "FAIL": return results
        return results


def run(runtime_path, case_id, candidate_path, testing_commit, artifacts):
    runtime = json.loads(runtime_path.read_text())
    candidate = json.loads(candidate_path.read_text())
    runtime_binding(runtime, case_id, candidate, testing_commit)
    evidence = artifacts / "evidence" / "execution"
    evidence.mkdir(parents=True)
    runner = Runner(runtime, candidate, testing_commit, evidence)
    rows = runner.run(case_id)
    checks_complete = bool(rows) and all(row.get("checksCompleted") and row["cleanup"] == "PASS" for row in rows)
    verdict = "INFRA_ERROR" if any(r["cleanup"] == "FAIL" for r in rows) else "FAIL" if any(r["verdict"] == "FAIL" for r in rows) else "BLOCKED"
    recipe = select(case_id)[0]
    result = {"schema_version": 1, "case_id": case_id, "source_status": recipe["contract"]["source_status"],
              "migration_status": "unverified", "profile": ENVIRONMENT_GATED_CASES[case_id]["profile"],
              "verdict": verdict, "cleanup_status": "PASS" if all(r["cleanup"] in {"PASS", "NOT_RUN"} for r in rows) else "FAIL",
              "evidence": ["evidence/execution/summary.json"],
              "reason": "; ".join(r.get("reason", r.get("recoveryError", "")) for r in rows) or "required branch evidence is incomplete"}
    binding = runtime["binding"]
    result["provenance"] = {"candidate_url": candidate["package_url"], "candidate_sha256": candidate["package_sha256"],
        "binary_sha256": binding["candidateBinarySha256"], "source_commit": candidate["source_sha"], "testing_commit": testing_commit,
        "rc_path": str(Path(runtime["rcBinary"]).resolve()), "rc_sha256": binding["rcBinarySha256"]}
    save(evidence / "summary.json", {"caseId": case_id, "results": rows, "checksCompleted": checks_complete,
                                    "canonicalPassClaimed": False, "reviewRequired": True})
    save(artifacts / "cases" / (case_id + ".json"), result)
    return result
