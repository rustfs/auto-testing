"""Isolate only G06's interrupted H1 COPY after complete last-ACK authority.

This is an environment recovery boundary, never a completed canonical result.
Original request/ACK bytes and the stopped scene must still agree. No backup
restoration, new SDK request or inferred acknowledgement is permitted.
"""

from collections import Counter
import hashlib
import json
import re
from types import SimpleNamespace

try:
    from . import race, race_retention
    from .heal_probe import require
    from .healthy import physical_content
except ImportError:
    import race
    import race_retention
    from heal_probe import require
    from healthy import physical_content


CASE = "heal.race-copy-and-metadata-update"


class Replay:
    """Consume every original SDK operation once, without invoking a client."""

    def __init__(self, folder, run_id, bucket):
        self.folder, self.run_id, self.sequence = folder, run_id, 0
        self.probe = SimpleNamespace(binding={"bucketPrefix": bucket})
        self.files = {}
        require(folder.is_dir() and all(re.fullmatch(r"[0-9]{6}-(request|ack)\.json", p.name)
                and p.is_file() and not p.is_symlink() for p in folder.iterdir()),
                "interrupted COPY journal has an unknown member")

    def call(self, operation, params, body=None):
        self.sequence += 1
        request_path = self.folder / f"{self.sequence:06d}-request.json"
        ack_path = self.folder / f"{self.sequence:06d}-ack.json"
        request_raw, ack_raw = request_path.read_bytes(), ack_path.read_bytes()
        require(max(len(request_raw), len(ack_raw)) <= 1024**2, "interrupted COPY journal exceeds its bound")
        request, ack = json.loads(request_raw), json.loads(ack_raw)
        observation = ack.get("observation") or {}
        require(request.get("schemaVersion") == ack.get("schemaVersion") == 2
                and request.get("runId") == ack.get("runId") == self.run_id
                and request.get("sequence") == ack.get("sequence") == self.sequence
                and request.get("operation") == observation.get("operation") == operation
                and request.get("params") == observation.get("params") == params
                and ack.get("requestFile") == request_path.name
                and ack.get("requestSha256") == hashlib.sha256(request_raw).hexdigest()
                and ack.get("operationCheck") == "PASS"
                and observation.get("acknowledged") is True and observation.get("resultUnknown") is False
                and observation.get("clientCallInvoked") is True,
                "interrupted COPY has an unresolved, changed or foreign SDK operation")
        require(request.get("payload") == (None if body is None else {
            "size": len(body), "sha256": hashlib.sha256(body).hexdigest()}),
            "interrupted COPY deterministic seed body differs")
        for path, raw in ((request_path, request_raw), (ack_path, ack_raw)):
            self.files[self.folder.name + "/" + path.name] = hashlib.sha256(raw).hexdigest()
        return observation["response"]

    def finish(self, count):
        require(self.sequence == count and len(list(self.folder.iterdir())) == 2 * count,
                "interrupted COPY journal has an omitted or additional SDK operation")
        return self.files


def archived_plan(artifact, failed_run, candidate):
    results = list(artifact.glob("*/run/*/cases/*.json"))
    require(len(results) == 1 and results[0].name == CASE + ".json", "interrupted COPY requires one unmixed case")
    raw = results[0].read_bytes()
    result = json.loads(raw)
    require(result.get("case_id") == CASE and result.get("verdict") == "RUNNING"
            and result.get("cleanup_status") == "PENDING" and not result.get("provenance"),
            "interrupted COPY original result must remain unfinished")
    require(json.loads((results[0].parents[3] / "candidate.json").read_text()) == candidate,
            "interrupted COPY candidate differs")
    evidence = results[0].parents[1] / "evidence" / CASE
    require(not (evidence / "healthy-summary.json").exists()
            and {p.relative_to(evidence).as_posix() for p in evidence.glob("*/*") if p.is_dir()}
                == {"H1/copy-source", "H1/copy-destination"},
            "interrupted COPY cannot omit another unfinished branch")
    previous = evidence / "H1/copy-source"
    def one(folder, label):
        paths = list(folder.glob("*-" + label + ".json"))
        require(len(paths) == 1, "interrupted COPY lacks one original " + label)
        return json.loads(paths[0].read_text())
    prior = one(previous, "branch-result")
    cleanup = one(previous, "cleanup")
    require(prior.get("topology") == "H1" and prior.get("branch") == "copy-source"
            and prior.get("verdict") in {"PASS", "NOT_OBSERVED", "FAIL"} and prior.get("cleanup") == "PASS"
            and cleanup.get("node") == "vm000" and cleanup.get("cleanup") == "PASS"
            and cleanup.get("unitStopped") is True and cleanup.get("rootsRemoved") is True
            and one(previous, "recovery") == {"cleanup": "PASS", "errors": []},
            "interrupted COPY prior shared namespace was not finalized")
    folder = evidence / "H1/copy-destination"
    require({p.name for p in folder.iterdir() if p.is_dir()} == {"seed-ACKs", "foreground-ACKs"}
            and all(p.is_dir() or re.fullmatch(r"[0-9]{5}-.+\.json", p.name) for p in folder.iterdir()),
            "interrupted COPY has an unknown operation")
    files = sorted(folder.glob("*.json"))
    labels = Counter(p.name[6:-5] for p in files)
    expected = {"binding": 1, "inspector-build": 1, "source-contract": 1, "preflight": 1,
        "reserved": 1, "inspector-upload": 1, "started": 3, "deployed-topology": 1,
        "topology-proof": 1, "complete-baseline-S3": 1, "frozen-foreground-plan": 1,
        "quiescent": 2, "complete-baseline-physical": 1, "baseline-streaming": 9,
        "exact-missing-part": 257, "complete-fault-physical": 1, "fault-batch-proof": 1,
        "race-io-cap-intent": 2, "race-io-process": 2, "race-io-cap-applied": 2,
        "attribution-before-START": 1, "prepared-Admin-request": 1, "original-Admin-start": 1,
        "target-missing-before-foreground": 1, "read-repair-overlap-proof": 1,
        "public-call-overlap-proof": 1, "ACK-derived-final-manifest": 1,
        "product-physical-before-GET-or-rescue": 1, "last-ACK-physical-proof": 1,
        "final-streaming": 9, "obsolete-target-fault-retired": 1, "integrity-fleet-attestation": 1}
    statuses = labels.pop("original-token-status", 0)
    require(labels == Counter(expected) and 3 <= statuses <= 2000
            and all(p.name.startswith(f"{i:05d}-") for i, p in enumerate(files, 1))
            and files[-1].name.endswith("-started.json")
            and files[-2].name.endswith("-obsolete-target-fault-retired.json"),
            "interrupted COPY is outside its reviewed after-authority boundary")
    binding = one(folder, "binding")
    require(re.fullmatch(rf"gh-{failed_run}-[1-9][0-9]*-h1", binding.get("runId", ""))
            and binding.get("candidateCommit") == candidate["source_sha"]
            and binding.get("candidatePackageSha256") == candidate["package_sha256"],
            "interrupted COPY run or candidate binding differs")
    reservation = one(folder, "reserved")
    require(reservation.get("node") == "vm000" and reservation.get("reserved") is True,
            "interrupted COPY original owner is incomplete")
    owner = reservation["owner"]
    run_id, bucket = binding["runId"], binding["bucketPrefix"]
    seed = Replay(folder / "seed-ACKs", run_id, bucket)
    expected_seed = race.seed_fixture(seed, bucket, "copy-destination")
    ack_files = seed.finish(258)
    baseline = one(folder, "complete-baseline-S3")
    race.verify_seed(expected_seed, baseline)
    plan = race.operation_plan(bucket, "copy-destination", expected_seed)
    for step in plan:
        step["branch"] = "copy-destination"
    require(one(folder, "frozen-foreground-plan") == plan, "interrupted COPY changed its frozen operation")
    writer = Replay(folder / "foreground-ACKs", run_id, bucket)
    responses = [writer.call(step["operation"], step["params"]) for step in plan]
    ack_files.update(writer.finish(1))
    final = race.final_inventory(baseline, "copy-destination", plan, responses)
    require(final == one(folder, "ACK-derived-final-manifest"), "interrupted COPY final ACK authority differs")
    before, after = (one(folder, label) for label in (
        "complete-baseline-physical", "product-physical-before-GET-or-rescue"))
    for scene in (before, after):
        require(set(scene) == {"vm000"} and scene["vm000"].get("quiescent") is True
                and type(scene["vm000"].get("pid")) is int and scene["vm000"]["pid"] == 0
                and scene["vm000"].get("binarySha256") == binding["candidateBinarySha256"]
                and scene["vm000"].get("hostname") == owner["hostname"]
                and scene["vm000"].get("unit") == owner["unit"],
                "interrupted COPY scene is not the stopped exact owner")
    mapped = race.map_fixture(before, bucket, expected_seed)
    survivors, proof = race.physical_transition(before, after, bucket, mapped, final, "copy-destination")
    require(proof == one(folder, "last-ACK-physical-proof"), "interrupted COPY physical authority differs")
    # Reuse the complete streaming oracle against archived receipts, without SSH.
    class ArchivedParts:
        nodes = ["vm000"]
        def __init__(self):
            self.proofs = iter(json.loads(p.read_text()) for p in sorted(folder.glob("*-final-streaming.json")))
        def remote(self, node, action, *, parts):
            observed = next(self.proofs)
            require(node == observed.get("node") and action == "verify-parts"
                    and [{k: p[k] for k in ("disk", "relativePath", "versionId", "partNumber")}
                         for p in observed["parts"]] == parts, "interrupted COPY streaming batch differs")
            return observed
        def record(self, label, value):
            return value
    stream = ArchivedParts()
    race.verify_part_manifest(stream, list(survivors.values()), "final-streaming", count=257)
    require(next(stream.proofs, None) is None, "interrupted COPY omitted a streaming batch")
    retired = one(folder, "obsolete-target-fault-retired")
    require(retired.get("oldBytesRestored") is False and retired.get("backupRetainedUntilCleanup") is True
            and json.loads(files[-1].read_text()).get("vm000", {}).get("unit") == owner["unit"]
            and json.loads(files[-1].read_text()).get("vm000", {}).get("started") is True,
            "interrupted COPY retirement or final start differs")
    isolation = {"kind": "unresolved-g06-race", "runId": run_id, "nodes": ["vm000"], "bucket": bucket,
        "owners": {"vm000": owner}, "candidateCommit": candidate["source_sha"],
        "harnessCommit": binding["harnessCommit"], "binarySha256": binding["candidateBinarySha256"],
        "businessSceneSha256": {"vm000": race_retention.digest(physical_content(after)["vm000"])},
        "ackFilesSha256": race_retention.digest(ack_files)}
    race_retention.validate(isolation)
    return [{"kind": "interrupted-g06-after-authority", "isolation": isolation,
        "runId": run_id, "nodes": ["vm000"], "originalCaseSha256": hashlib.sha256(raw).hexdigest()}]


def isolate(fleet, plan):
    isolation = plan["isolation"]
    race_retention.validate(isolation)
    require(plan["kind"] == "interrupted-g06-after-authority" and fleet.run_id == isolation["runId"]
            and list(fleet.nodes) == isolation["nodes"], "interrupted COPY recovery fleet differs")
    states = [fleet.record("interrupted-COPY-namespace", {"node": node,
        **fleet.remote(node, "namespace-state")}) for node in fleet.nodes]
    for state in states:
        if state.get("retained", {}).get("state") or any(state.get("retained", {}).get("roots", [])):
            continue  # Existing durable isolation intents are revalidated by isolate().
        owner_sha = race_retention.digest(isolation["owners"][state["node"]])
        entries = [state["state"], *state["roots"]]
        require(len(entries) == 5 and all(entry.get("exists") is True
                and entry.get("ownerEvidence", {}).get("uid") == 0
                and entry.get("ownerEvidence", {}).get("canonicalSha256") == owner_sha for entry in entries),
                "interrupted COPY original owner drifted; no service stopped")
    for state in states:
        if not (state.get("retained", {}).get("state") or any(state.get("retained", {}).get("roots", []))):
            stopped = fleet.record("interrupted-COPY-stop", {"node": state["node"],
                **fleet.remote(state["node"], "stop")})
            require(stopped.get("unit") == isolation["owners"][state["node"]]["unit"]
                    and stopped.get("quiescent") is True, "interrupted COPY exact service did not stop")
    return {**race_retention.isolate(fleet, isolation), "originalCaseSha256": plan["originalCaseSha256"],
            "interruptedAfterLastACKAuthority": True, "canonicalCaseComplete": False}
