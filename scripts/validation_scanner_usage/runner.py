"""Run usage probes on explicitly configured endpoints and retain every verdict."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import signal
import sys
import time
from uuid import UUID

from .client import Client, Evidence, ProbeError, bucket_view, endpoint_url, integer, run_command, sha256, utc_now
from .contracts import CASES, assertion_id, select_cases
from .native import run_native

ROOT = Path(__file__).resolve().parents[2]


def require(condition, message, verdict="FAIL"):
    if not condition:
        raise ProbeError(message, verdict)


class Fixture:
    """Only buckets successfully created by this invocation are mutable."""

    def __init__(self, client, directory, evidence):
        self.client, self.directory, self.evidence = client, Path(directory), evidence
        self.owned = set()
        self.unconfirmed_creates = set()
        self.objects = {}
        self.versions = {}
        self.request_count = 0
        self.journal_path = self.directory / "acknowledged-requests.jsonl"
        self.prefix = "usage-" + secrets.token_hex(10)

    def persist_ownership(self):
        (self.directory / "owned-buckets.json").write_text(json.dumps({
            "prefix": self.prefix, "buckets": sorted(self.owned),
            "unconfirmed_creates": sorted(self.unconfirmed_creates),
            "request_journal": self.journal_path.name}, indent=2) + "\n")

    def record_acknowledgement(self, entry):
        with self.journal_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"sequence": self.request_count + 1, **entry}) + "\n")
        self.request_count += 1

    def create(self, suffix, *, name=None, versioned=False):
        bucket = name or self.prefix + "-" + suffix
        require(bucket.startswith(self.prefix + "-"), "refusing a foreign bucket name", "INFRA_ERROR")
        # Record the intent before the request: a lost response cannot prove
        # either ownership or absence, so cleanup must expose that uncertainty.
        self.unconfirmed_creates.add(bucket)
        self.persist_ownership()
        self.client.s3("create-bucket", "--bucket", bucket)
        self.owned.add(bucket)
        self.unconfirmed_creates.remove(bucket)
        self.objects[bucket] = {}
        self.versions[bucket] = [] if versioned else None
        self.persist_ownership()
        if versioned:
            self.client.s3("put-bucket-versioning", "--bucket", bucket,
                           "--versioning-configuration", '{"Status":"Enabled"}')
        return bucket

    def put(self, bucket, key, size, *, endpoint_index=0):
        require(bucket in self.owned, "refusing writes to an unowned bucket", "INFRA_ERROR")
        payload = (hashlib.sha256((key + str(self.request_count)).encode()).digest() * ((size + 31) // 32))[:size]
        path = self.directory / "request-payload.bin"
        path.write_bytes(payload)
        response = self.client.s3("put-object", "--bucket", bucket, "--key", key, "--body", path,
                                  endpoint_index=endpoint_index)
        entry = {"key": key, "size": size, "sha256": hashlib.sha256(payload).hexdigest(),
                 "version_id": response.get("VersionId"), "marker": False}
        if self.versions[bucket] is not None:
            require(bool(entry["version_id"]), "versioned PUT did not return a VersionId")
            self.versions[bucket].append(entry)
        self.objects[bucket][key] = entry
        self.record_acknowledgement({"operation": "put", "bucket": bucket, "endpoint_index": endpoint_index,
                                     "acknowledged_at": utc_now(), **entry})
        return entry

    def delete(self, bucket, key):
        require(bucket in self.owned, "refusing deletion from an unowned bucket", "INFRA_ERROR")
        response = self.client.s3("delete-object", "--bucket", bucket, "--key", key)
        if self.versions[bucket] is not None:
            require(response.get("DeleteMarker") is True and bool(response.get("VersionId")),
                    "versioned DELETE did not create the expected marker")
            self.versions[bucket].append({"key": key, "version_id": response["VersionId"], "marker": True, "size": 0})
        self.objects[bucket].pop(key, None)
        self.record_acknowledgement({"operation": "delete", "bucket": bucket, "key": key, "response": response,
                                     "acknowledged_at": utc_now()})

    def inventory(self, bucket, *, versioned=False, endpoint_index=0):
        require(bucket in self.owned, "refusing inventory of an unowned bucket", "INFRA_ERROR")
        records, continuation, seen = [], [], set()
        for _ in range(10000):
            operation = "list-object-versions" if versioned else "list-objects-v2"
            value = self.client.s3(operation, "--bucket", bucket, "--max-keys", "100", "--no-paginate", *continuation,
                                   endpoint_index=endpoint_index)
            if versioned:
                for field, marker in (("Versions", False), ("DeleteMarkers", True)):
                    for row in value.get(field, []):
                        records.append({"key": row["Key"], "version_id": row["VersionId"],
                                        "size": 0 if marker else row["Size"], "marker": marker})
            else:
                records.extend({"key": row["Key"], "size": row["Size"]} for row in value.get("Contents", []))
            if value.get("IsTruncated") is not True:
                break
            token = ((value.get("NextKeyMarker"), value.get("NextVersionIdMarker")) if versioned
                     else (value.get("NextContinuationToken"),))
            require(token[0] is not None and token not in seen, "S3 listing repeated or omitted its continuation", "INFRA_ERROR")
            seen.add(token)
            continuation = (["--key-marker", token[0]] + (["--version-id-marker", token[1]] if token[1] else [])
                            if versioned else ["--continuation-token", token[0]])
        else:
            raise ProbeError("S3 listing exceeded the bounded pagination limit")
        identities = [(row["key"], row.get("version_id")) for row in records]
        require(len(identities) == len(set(identities)), "S3 inventory duplicated an object/version")
        return records

    def oracle(self, bucket, *, verify_payloads=False, endpoint_index=0):
        versioned = self.versions[bucket] is not None
        actual = self.inventory(bucket, versioned=versioned, endpoint_index=endpoint_index)
        expected = self.versions[bucket] if versioned else list(self.objects[bucket].values())
        fields = ("key", "version_id", "size", "marker") if versioned else ("key", "size")
        normalize = lambda rows: sorted(tuple(row[field] for field in fields) for row in rows)
        require(normalize(actual) == normalize(expected), "Independent S3 inventory differs from acknowledged operations")
        if verify_payloads:
            target = self.directory / "readback.bin"
            for row in expected:
                if row.get("marker"):
                    continue
                arguments = ["--bucket", bucket, "--key", row["key"]]
                if row.get("version_id"):
                    arguments += ["--version-id", row["version_id"]]
                self.client.s3("get-object", *arguments, target, endpoint_index=endpoint_index)
                require(target.is_file() and sha256(target) == row["sha256"], "GET payload hash differs from the acknowledged fixture")
        # RustFS scanner counts a key with retained object versions once even
        # when its latest version is a delete marker. Fixtures avoid marker-only
        # keys so the oracle does not silently choose a disputed empty-key rule.
        values = {"count": len({row["key"] for row in actual}), "size": sum(row["size"] for row in actual)}
        if versioned:
            values.update(versions=sum(not row["marker"] for row in actual), markers=sum(row["marker"] for row in actual))
        path = self.evidence.save("independent-oracle", {"bucket": bucket, "versioned": versioned,
                                                        "endpoint_index": endpoint_index,
                                                        "endpoint": self.client.endpoints[endpoint_index],
                                                        "inventory": actual, "expected": values,
                                                        "payloads_verified": verify_payloads})
        return values, path

    def remove(self, bucket):
        require(bucket in self.owned, "refusing cleanup of an unowned bucket", "INFRA_ERROR")
        # Always enumerate versions: this also removes markers if a setup
        # operation succeeded but its response was lost later in the case.
        for row in self.inventory(bucket, versioned=True):
            self.client.s3("delete-object", "--bucket", bucket, "--key", row["key"], "--version-id", row["version_id"])
        self.client.s3("delete-bucket", "--bucket", bucket)
        self.owned.remove(bucket)
        self.persist_ownership()

    def cleanup(self):
        failures = [{"bucket": bucket, "error": "CreateBucket did not confirm ownership; remote cleanup is unverified"}
                    for bucket in sorted(self.unconfirmed_creates)]
        for bucket in sorted(self.owned):
            try:
                self.remove(bucket)
            except (OSError, KeyError, ProbeError) as exc:
                failures.append({"bucket": bucket, "error": str(exc)})
        path = self.evidence.save("cleanup", {"status": "FAIL" if failures else "PASS",
                                              "failures": failures, "remaining_owned_buckets": sorted(self.owned),
                                              "unconfirmed_creates": sorted(self.unconfirmed_creates)})
        return ("FAIL" if failures else "PASS"), path


class PublicProbe:
    def __init__(self, client, fixture, evidence, options):
        self.client, self.fixture, self.evidence, self.options = client, fixture, evidence, options
        self.passed = {}
        self.observed = []

    def check(self, index, facts):
        self.passed[index] = self.evidence.save("assertion", facts)

    def sample(self, bucket, endpoint_index=0):
        snapshot = self.client.admin(endpoint_index=endpoint_index)
        view = bucket_view(snapshot, bucket)
        row = {"collected_at": utc_now(), "bucket": bucket, "view": view,
               "scanner_cycle": snapshot.get("scanner_cycle"), "scanner_epoch": snapshot.get("scanner_epoch"),
               "raw_evidence": self.client.last_evidence}
        self.observed.append(row)
        return view

    def wait_usage(self, bucket, expected, endpoint_index=0):
        deadline = time.monotonic() + self.options.convergence_timeout
        samples = 0
        while True:
            view = self.sample(bucket, endpoint_index)
            samples += 1
            if view["converged"] and all(view[field] == value for field, value in expected.items()):
                return self.evidence.save("convergence", {"bucket": bucket, "expected": expected,
                                                          "actual": view, "samples": samples})
            if time.monotonic() >= deadline:
                self.evidence.save("convergence-timeout", {"bucket": bucket, "expected": expected,
                                                           "actual": view, "samples": samples})
                raise ProbeError("Usage did not converge to exact independently listed values within the configured deadline", "FAIL")
            time.sleep(min(self.options.poll_interval, max(0, deadline - time.monotonic())))

    def seed(self, bucket, count=None, *, endpoints=False):
        for index in range(count if count is not None else self.options.objects):
            self.fixture.put(bucket, f"range-{index % 4}/nested/key-{index:06d}", 1024 + index,
                             endpoint_index=index % len(self.client.endpoints) if endpoints else 0)

    def static(self):
        bucket = self.fixture.create("static")
        absent = self.sample(self.fixture.prefix + "-never-created")
        require(not absent["present"] and absent["count"] is None and absent["size"] is None,
                "An absent bucket was represented as a measured empty bucket")
        self.seed(bucket)
        values, oracle = self.fixture.oracle(bucket, verify_payloads=True)
        self.check(0, {"oracle": oracle, "owned_bucket": bucket})
        writes = self.wait_usage(bucket, values)
        for key in list(self.fixture.objects[bucket]):
            self.fixture.delete(bucket, key)
        empty, empty_oracle = self.fixture.oracle(bucket)
        deletes = self.wait_usage(bucket, empty)
        self.check(1, {"write_convergence": writes, "delete_convergence": deletes, "empty_oracle": empty_oracle})
        self.check(2, {"absent": absent, "measured_empty": empty, "convergence": deletes})

    def mutations(self):
        bucket = self.fixture.create("mutation")
        self.seed(bucket)
        values, _ = self.fixture.oracle(bucket)
        before = self.wait_usage(bucket, values)
        keys = list(self.fixture.objects[bucket])
        self.fixture.put(bucket, keys[0], 8193)
        self.fixture.delete(bucket, keys[1])
        self.fixture.put(bucket, "range-0/nested/new-after-baseline", 4099)
        values, oracle = self.fixture.oracle(bucket, verify_payloads=True)
        after = self.wait_usage(bucket, values)
        self.check(0, {"baseline": before, "final_oracle": oracle, "final_convergence": after,
                       "limits": "Public cycle observations do not prove which internal range was scanned before mutation."})

    def multi_bucket(self):
        hot = self.fixture.create("hot")
        cold = [self.fixture.create(f"cold-{index}") for index in range(3)]
        self.seed(hot)
        targets = {}
        oracles, incarnations, accepted = {}, {}, {}
        for bucket in cold:
            self.seed(bucket, 3)
            targets[bucket], oracles[bucket] = self.fixture.oracle(bucket, verify_payloads=True)
            initial = self.sample(bucket)
            if initial.get("incarnation") is not None:
                incarnations[bucket] = initial["incarnation"]
        deadline = time.monotonic() + self.options.convergence_timeout
        done = set()
        operations = 0
        while time.monotonic() < deadline and len(done) != len(cold):
            self.fixture.put(hot, "hot-prefix/mutating", 2048 + operations % 64)
            operations += 1
            for bucket in cold:
                view = self.sample(bucket)
                incarnation = view.get("incarnation")
                if incarnation is not None:
                    require(isinstance(incarnation, str), "Cold-bucket incarnation is not a UUID")
                    try:
                        valid_identity = UUID(incarnation).int != 0
                    except ValueError:
                        valid_identity = False
                    require(valid_identity, "Cold-bucket incarnation is not a non-nil UUID")
                    require(incarnations.setdefault(bucket, incarnation) == incarnation,
                            "Cold-bucket incarnation changed during the hot workload")
                # A carried cold value retains its original certificate. It can
                # satisfy this bucket probe while the hot bucket is still pending.
                if (view["state"] in {"certified", "carried"} and view["certificate"] is not None
                        and incarnation is not None
                        and view.get("complete") and not view.get("pending")
                        and not view.get("partial") and not view.get("observational")
                        and not view.get("admin_projection")
                        and all(view[field] == value for field, value in targets[bucket].items())):
                    done.add(bucket)
                    accepted[bucket] = view
            if len(done) < len(cold):
                time.sleep(self.options.poll_interval)
        require(len(done) == len(cold), "Cold-bucket certification was not observed during the hot workload",
                "UNSUPPORTED" if not any(row["view"].get("metadata_available") for row in self.observed) else "FAIL")
        self.check(0, {"cold_buckets": sorted(done), "hot_puts": operations,
                       "independent_oracles": oracles, "incarnations": incarnations, "accepted_values": accepted,
                       "limits": "This public probe does not prove that a cycle budget actually expired."})

    def multi_endpoint(self):
        require(len(self.client.endpoints) >= 2, "Multiple explicit S3 endpoints are required", "BLOCKED")
        require(len(self.client.admin_endpoints) >= 2, "Multiple explicit Admin endpoints are required", "BLOCKED")
        bucket = self.fixture.create("sources")
        self.seed(bucket, max(self.options.objects, len(self.client.endpoints)), endpoints=True)
        evidence = []
        for index in range(len(self.client.endpoints)):
            values, oracle = self.fixture.oracle(bucket, verify_payloads=True, endpoint_index=index)
            evidence.append(oracle)
        for index in range(len(self.client.admin_endpoints)):
            evidence.append(self.wait_usage(bucket, values, index))
        self.check(0, {"observations": evidence, "limits": "Endpoint count is not physical source/set coverage."})

    def no_baseline(self):
        bucket = self.fixture.prefix + "-bootstrap"
        before = self.sample(bucket)
        require(not before["present"] and before["count"] is None and before["size"] is None,
                "The unique absent fixture bucket did not remain unknown")
        self.fixture.create("bootstrap")
        self.seed(bucket)
        values, oracle = self.fixture.oracle(bucket)
        convergence = self.wait_usage(bucket, values)
        self.check(0, {"before": before, "oracle": oracle, "convergence": convergence,
                       "limits": "A new bucket is not proof of cluster cold startup or missing internal LKG."})

    def freshness(self):
        bucket = self.fixture.create("freshness")
        self.seed(bucket)
        values, _ = self.fixture.oracle(bucket)
        self.wait_usage(bucket, values)
        baseline = self.sample(bucket)
        require(baseline.get("metadata_available"), "Candidate lacks per-bucket value metadata", "UNSUPPORTED")
        self.fixture.put(bucket, "range-0/nested/key-000000", 8291)
        carried_or_observed = []
        for _ in range(self.options.freshness_samples):
            view = self.sample(bucket)
            if view["state"] == "carried":
                require(view["certificate"] == baseline["certificate"], "Carried value replaced its original certificate")
                carried_or_observed.append(view)
            if view["state"] == "observed":
                require(view["certificate"] is None, "Observed value inherited a certificate")
                carried_or_observed.append(view)
            time.sleep(self.options.poll_interval)
        require(carried_or_observed, "No carried or observed value was exposed during this public sampling window", "BLOCKED")
        self.check(0, {"baseline": baseline, "carried_or_observed": carried_or_observed})

    def recreation(self):
        bucket = self.fixture.create("recreated")
        self.seed(bucket, 3)
        old, _ = self.fixture.oracle(bucket)
        self.wait_usage(bucket, old)
        self.fixture.remove(bucket)
        self.fixture.create("recreated", name=bucket)
        self.fixture.put(bucket, "successor-only", 7193)
        new, oracle = self.fixture.oracle(bucket, verify_payloads=True)
        result = self.wait_usage(bucket, new)
        self.check(0, {"old": old, "successor_oracle": oracle, "successor_convergence": result})

    def versions(self):
        bucket = self.fixture.create("versions", versioned=True)
        for index, key in enumerate(("a", "a/child", "a/child/deeper", "space key", "unicode-测试", "percent%+key")):
            self.fixture.put(bucket, key, 1027 + index)
        self.fixture.put(bucket, "a", 4097)
        self.fixture.put(bucket, "a/child", 5099)
        self.fixture.delete(bucket, "a")
        values, oracle = self.fixture.oracle(bucket, verify_payloads=True)
        self.check(0, {"oracle": oracle})
        result = self.wait_usage(bucket, values)
        self.check(1, {"oracle": oracle, "usage_convergence": result})

    def diagnostics(self):
        bucket = self.fixture.create("diagnostics")
        self.seed(bucket, 3)
        values, _ = self.fixture.oracle(bucket)
        self.wait_usage(bucket, values)
        for _ in range(self.options.freshness_samples):
            view = self.sample(bucket)
            require(view.get("metadata_available"), "Candidate lacks per-bucket value metadata", "UNSUPPORTED")
            time.sleep(self.options.poll_interval)
        self.check(0, {"samples": self.observed, "collection_clock": "client UTC",
                       "certification_clock": "unmodified server SystemTime; no last_update synthesis"})

    def load(self):
        require(self.options.load_seconds >= 3600, "USAGE-17 requires an explicit workload of at least 3600 seconds", "BLOCKED")
        bucket = self.fixture.create("load")
        end = time.monotonic() + self.options.load_seconds
        index = 0
        while time.monotonic() < end:
            self.fixture.put(bucket, f"bounded-key-{index % 32}", 4096 + index % 257)
            if index % 4 == 0:
                self.fixture.oracle(bucket, verify_payloads=True)
            if index % 7 == 0:
                self.fixture.delete(bucket, f"bounded-key-{index % 32}")
            self.sample(bucket)
            index += 1
        values, oracle = self.fixture.oracle(bucket, verify_payloads=True)
        result = self.wait_usage(bucket, values)
        self.check(0, {"workload_seconds": self.options.load_seconds, "rounds": index, "oracle": oracle,
                       "convergence": result, "latency_scope": "AWS CLI end-to-end, including subprocess startup",
                       "limits": "This bounded sequential probe is not a saturation benchmark or an A/B comparison."})


def execute_case(case, directory, options, provenance, native_manifest=None, client_factory=Client):
    directory = Path(directory)
    evidence = Evidence(directory / "evidence" / case.id)
    fixture = None
    result = {"schema_version": 1, "case_id": case.id, "design_id": case.design_id,
              "source_status": "procedure-ready", "profile": "native-test" if options.execution == "native" else "configured-endpoint",
              "execution_mode": options.execution, "migration_status": "unverified",
              "verdict": "RUNNING", "started_at": utc_now(), "cleanup_status": "PENDING",
              "provenance": provenance, "assertions": [], "capabilities": {}, "hit_count": 0}
    records, errors = {}, []
    try:
        if case.scenario and options.execution != "native":
            require(options.endpoints, "Explicit --endpoint is required for public probes", "BLOCKED")
            client = client_factory(options.endpoints, options.admin_endpoints, evidence,
                                    aws_bin=options.aws_bin, timeout=options.request_timeout, region=options.region)
            fixture = Fixture(client, evidence.root, evidence)
            probe = PublicProbe(client, fixture, evidence, options)
            try:
                getattr(probe, case.scenario.replace("-", "_"))()
            except (ProbeError, OSError, KeyError, ValueError) as exc:
                failure = exc if isinstance(exc, ProbeError) else ProbeError(str(exc))
                errors.append(failure)
                attempted = next((index for index, (kind, _) in enumerate(case.assertions)
                                  if kind == "public" and index not in probe.passed), None)
                if attempted is not None:
                    path = evidence.save("public-probe-error", {"reason": str(failure), "verdict": failure.verdict,
                                                                "last_request_evidence": client.last_evidence})
                    records[assertion_id(case, attempted)] = {"status": failure.verdict, "reason": str(failure), "evidence": [path]}
            finally:
                samples = evidence.save("usage-samples", probe.observed)
                for index, path in probe.passed.items():
                    records[assertion_id(case, index)] = {"status": "PASS", "evidence": [path, samples]}
        missing = [assertion_id(case, index) for index in range(len(case.assertions))
                   if assertion_id(case, index) not in records]
        native_needed = ([identifier for index, identifier in enumerate(
            [assertion_id(case, i) for i in range(len(case.assertions))])
                          if case.assertions[index][0] == "native" and identifier in missing]
                         if options.execution != "native" else missing)
        if native_needed and not getattr(options, "cancelled", False):
            try:
                records.update(run_native(case, native_manifest, provenance.get("source_commit"), evidence,
                                          options.native_timeout, native_needed))
                result["capabilities"]["native_assertions"] = "executed"
            except (ProbeError, OSError, ValueError) as exc:
                failure = exc if isinstance(exc, ProbeError) else ProbeError(str(exc))
                errors.append(failure)
                records.update(getattr(failure, "native_records", {}))
                path = None
                try:
                    path = evidence.save("native-adapter-error", {"reason": str(failure), "verdict": failure.verdict,
                                                                  "cleanup_status": failure.cleanup_status})
                except (OSError, ValueError) as diagnostic_error:
                    errors.append(ProbeError(f"Native adapter diagnostic could not be saved: {diagnostic_error}"))
                for identifier in native_needed:
                    if identifier not in records:
                        not_run = identifier in getattr(failure, "native_not_run", set())
                        records[identifier] = {"status": "BLOCKED" if not_run else failure.verdict,
                                               "reason": "Not executed after an earlier native test failed" if not_run else str(failure),
                                               "evidence": [path] if path is not None else []}
                result["capabilities"]["native_assertions"] = "unavailable or rejected"
    except (ProbeError, OSError, ValueError) as exc:
        errors.append(exc if isinstance(exc, ProbeError) else ProbeError(str(exc)))
    finally:
        if fixture:
            cleanup, cleanup_path = fixture.cleanup()
        else:
            cleanup = "PASS"
            cleanup_path = evidence.save("cleanup", {"status": "PASS", "owned_buckets": [], "reason": "no public fixture created"})
        result["cleanup_status"] = cleanup
        if any(error.cleanup_status == "FAIL" for error in errors):
            result["cleanup_status"] = "FAIL"
        if cleanup != "PASS":
            errors.append(ProbeError("Fixture cleanup failed; retain owned-buckets.json for scoped recovery"))
        for index, (_, text) in enumerate(case.assertions):
            identifier = assertion_id(case, index)
            item = records.get(identifier, {"status": "BLOCKED", "evidence": [], "reason": "Required assertion was not demonstrated"})
            result["assertions"].append({"assertion_id": identifier, "source_assertion": text, **item})
        if errors:
            priorities = {"FAIL": 0, "INFRA_ERROR": 1, "BLOCKED": 2, "UNSUPPORTED": 3}
            result["verdict"] = min((error.verdict for error in errors), key=priorities.__getitem__)
            result["reason"] = "; ".join(dict.fromkeys(str(error) for error in errors))
        elif len(records) != len(case.assertions):
            result["verdict"] = "BLOCKED"
            result["reason"] = "Required assertion evidence is missing"
        else:
            result["verdict"] = "PASS"
            result["migration_status"] = "verified"
            result["hit_count"] = 1
        prefix = f"evidence/{case.id}/"
        for item in result["assertions"]:
            item["evidence"] = [prefix + path for path in item["evidence"]]
        result["evidence"] = [prefix + path.name for path in sorted(evidence.root.iterdir()) if path.is_file()]
        result["cleanup_evidence"] = prefix + cleanup_path
        result["finished_at"] = utc_now()
        result["limits"] = ["Public observations cannot certify private ACK, checkpoint, source-turn or watch race boundaries."]
        (directory / "cases").mkdir(exist_ok=True)
        (directory / "cases" / (case.id + ".json")).write_text(json.dumps(result, indent=2) + "\n")
    return result


def render_report(results):
    lines = ["# Scanner usage regression report", "", "PASS requires every listed assertion and successful cleanup.", "",
             "| Design case | Verdict | Assertions passed | Cleanup |", "| --- | --- | --- | --- |"]
    for result in results:
        passed = sum(item["status"] == "PASS" for item in result["assertions"])
        lines.append(f"| {result['design_id']} | {result['verdict']} | {passed}/{len(result['assertions'])} | {result['cleanup_status']} |")
    for result in results:
        lines += ["", f"## {result['design_id']} — {result['case_id']}", "", result.get("reason", "Every required assertion passed."), "",
                  f"Started: {result['started_at']}  Finished: {result['finished_at']}", "",
                  "| Assertion | Status | Evidence |", "| --- | --- | --- |"]
        for item in result["assertions"]:
            links = ", ".join(f"[artifact]({path})" for path in item["evidence"])
            lines.append(f"| {item['assertion_id']}: {item['source_assertion']} | {item['status']} | {links or item.get('reason', '')} |")
        lines += ["", f"[Cleanup evidence]({result['cleanup_evidence']})", ""]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default="blackbox", help="blackbox, all, USAGE-NN or canonical IDs, comma-separated")
    parser.add_argument("--validation-case", help="one canonical validation ID")
    parser.add_argument("--endpoint", action="append", default=[])
    parser.add_argument("--admin-endpoint", action="append", default=[])
    parser.add_argument("--artifacts-dir", "--validation-artifacts-dir", dest="artifacts", type=Path, required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--testing-commit")
    parser.add_argument("--native-manifest", type=Path, default=os.environ.get("RUSTFS_USAGE_NATIVE_MANIFEST"))
    parser.add_argument("--execution", choices=("combined", "native"), default="combined")
    parser.add_argument("--aws-bin", default="aws")
    parser.add_argument("--rc-bin", default="rc", help="client provenance required by the shared validation report")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--request-timeout", type=float, default=30)
    parser.add_argument("--convergence-timeout", type=float, default=600)
    parser.add_argument("--native-timeout", type=float, default=900)
    parser.add_argument("--poll-interval", type=float, default=5)
    parser.add_argument("--objects", type=int, default=32)
    parser.add_argument("--freshness-samples", type=int, default=6)
    parser.add_argument("--load-seconds", type=float, default=0, help="USAGE-17 requires at least 3600; disabled by default")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-y", "--yes", action="store_true", help="accepted for the canonical runner; no node changes are performed")
    args = parser.parse_args(argv)
    args.endpoints = args.endpoint or ([os.environ["RUSTFS_USAGE_ENDPOINT"]] if os.environ.get("RUSTFS_USAGE_ENDPOINT") else [])
    args.admin_endpoints = args.admin_endpoint or ([os.environ["RUSTFS_USAGE_ADMIN_ENDPOINT"]] if os.environ.get("RUSTFS_USAGE_ADMIN_ENDPOINT") else [])
    try:
        args.endpoints = [endpoint_url(value) for value in args.endpoints]
        args.admin_endpoints = [endpoint_url(value) for value in args.admin_endpoints]
        selected = select_cases(args.validation_case or args.cases)
        require(all(math.isfinite(value) and value > 0 for value in (args.request_timeout, args.convergence_timeout, args.native_timeout,
                                           args.poll_interval, args.freshness_samples)), "Timeouts and sample counts must be positive", "INFRA_ERROR")
        require(2 <= args.objects <= 100000 and math.isfinite(args.load_seconds) and args.load_seconds >= 0, "Fixture counts or load duration are outside the supported bounds", "INFRA_ERROR")
        if args.dry_run:
            print(json.dumps({"cases": [case.id for case in selected], "mutations": "suite-owned buckets only",
                              "node_actions": [], "native_capability_required": [case.id for case in selected
                               if any(kind == "native" for kind, _ in case.assertions)]}, indent=2))
            return 0
        require(not args.artifacts.exists(), "Artifacts directory must not already exist", "INFRA_ERROR")
        candidate = json.loads(args.candidate_manifest.read_text())
        for field, length in (("source_sha", 40), ("package_sha256", 64), ("binary_sha256", 64)):
            value = candidate.get(field)
            require(isinstance(value, str) and len(value) == length and all(c in "0123456789abcdef" for c in value),
                    f"Candidate manifest requires {field}", "INFRA_ERROR")
        require(bool(candidate.get("package_url")), "Candidate manifest requires package_url", "INFRA_ERROR")
        actual_commit = run_command(["git", "-C", str(ROOT), "rev-parse", "HEAD"], 15)["stdout"].strip()
        require(not args.testing_commit or args.testing_commit == actual_commit, "Testing commit differs from checkout", "INFRA_ERROR")
        rc_path = shutil.which(args.rc_bin)
        require(rc_path is not None, "rc executable is required for shared report provenance", "BLOCKED")
        provenance = {"candidate_url": candidate["package_url"], "candidate_sha256": candidate["package_sha256"],
                      "binary_sha256": candidate["binary_sha256"], "source_commit": candidate["source_sha"],
                      "testing_commit": actual_commit, "rc_path": str(Path(rc_path).resolve()), "rc_sha256": sha256(rc_path),
                      "binary_identity_source": "operator-supplied candidate manifest; suite does not deploy or inspect host binaries"}
        native = json.loads(args.native_manifest.read_text()) if args.native_manifest else None
        args.artifacts.mkdir(parents=True)
        (args.artifacts / "run-configuration.json").write_text(json.dumps({
            "selected_cases": [case.id for case in selected], "endpoints": args.endpoints,
            "admin_endpoints": args.admin_endpoints, "objects": args.objects,
            "request_timeout": args.request_timeout, "convergence_timeout": args.convergence_timeout,
            "poll_interval": args.poll_interval, "load_seconds": args.load_seconds,
            "provenance": provenance, "node_actions": []}, indent=2) + "\n")
        args.cancelled = False
        def cancel(signum, _frame):
            if args.cancelled:
                return
            args.cancelled = True
            raise ProbeError(f"Runner interrupted by signal {signum}")
        prior_handlers = {number: signal.signal(number, cancel) for number in (signal.SIGINT, signal.SIGTERM)}
        results = []
        try:
            for case in selected:
                results.append(execute_case(case, args.artifacts, args, provenance, native))
                if args.cancelled:
                    break
        finally:
            for number, handler in prior_handlers.items():
                signal.signal(number, handler)
            (args.artifacts / "report.md").write_text(render_report(results) + "\n")
            (args.artifacts / "summary.json").write_text(json.dumps({"verdicts": dict(Counter(result["verdict"] for result in results)),
                                                                    "results": results}, indent=2) + "\n")
        return 0 if all(result["verdict"] == "PASS" for result in results) else 1
    except (OSError, ValueError, ProbeError) as exc:
        print(f"scanner usage runner: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
