#!/usr/bin/env python3
"""Test the usage runner, not the RustFS product, with explicit local doubles."""

from __future__ import annotations

import copy
from contextlib import redirect_stderr
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.validation_catalog import catalog_modules, validate_snapshot
from scripts.validation_report import check_result
from scripts.select_validation_cases import select_validation_cases, sweepable_cases
from scripts.validation_scanner_usage.client import Client, Evidence, ProbeError, bucket_view, endpoint_url, run_command, sha256
from scripts.validation_scanner_usage.contracts import CASES, assertion_id, catalog_module, select_cases
from scripts.validation_scanner_usage.native import run_native
from scripts.validation_scanner_usage.runner import execute_case, Fixture, main, render_report

ROOT = Path(__file__).resolve().parent


def certificate(cycle=1):
    return {"epoch": 1, "cycle": cycle, "certified_at": {"secs_since_epoch": 1800000000, "nanos_since_epoch": 0}}


def snapshot(entry=None, **changes):
    value = {"usage_snapshot_complete": True, "usage_snapshot_converged": True,
             "buckets_usage": {"bucket": entry or {"objects_count": 1, "size": 8}}}
    value.update(changes)
    return value


def options(**changes):
    value = dict(endpoints=["http://example.invalid:9000"], admin_endpoints=[],
                 execution="combined", aws_bin="aws", region="us-east-1", request_timeout=1,
                 convergence_timeout=0.002, native_timeout=2, poll_interval=0.001,
                 objects=3, freshness_samples=2, load_seconds=0)
    value.update(changes)
    return SimpleNamespace(**value)


PROVENANCE = {"candidate_url": "https://example.invalid/rustfs.deb", "candidate_sha256": "a" * 64,
              "binary_sha256": "b" * 64, "source_commit": "c" * 40, "testing_commit": "d" * 40,
              "rc_path": "/bin/true", "rc_sha256": "e" * 64}


class FakeClient:
    """Acknowledgement/inventory double used only to exercise runner branches."""
    def __init__(self, endpoints, admin_endpoints, evidence, **_):
        self.endpoints = endpoints
        self.admin_endpoints = admin_endpoints or endpoints
        self.evidence = evidence
        self.buckets = {}
        self.versioned = set()
        self.serial = 0
        self.last_evidence = None
        self.fail_cleanup = False
        self.fail_put = False
        self.pending = False
        self.observed = False
        self.malformed = False

    def s3(self, operation, *arguments, endpoint_index=0):
        self.last_evidence = self.evidence.save("fake-s3", {"operation": operation, "arguments": list(map(str, arguments))})
        def arg(name, default=None):
            return arguments[arguments.index(name) + 1] if name in arguments else default
        bucket, key = arg("--bucket"), arg("--key")
        if operation == "create-bucket":
            if bucket in self.buckets:
                raise ProbeError("already exists", "FAIL")
            self.buckets[bucket] = []
        elif operation == "put-bucket-versioning":
            self.versioned.add(bucket)
        elif operation == "put-object":
            if self.fail_put:
                raise ProbeError("The service is unavailable (HTTP 503)", "FAIL")
            self.serial += 1
            version = str(self.serial) if bucket in self.versioned else "null"
            if bucket not in self.versioned:
                self.buckets[bucket] = [row for row in self.buckets[bucket] if row["Key"] != key]
            self.buckets[bucket].append({"Key": key, "VersionId": version, "payload": Path(arg("--body")).read_bytes(), "marker": False})
            return {"VersionId": version, "ETag": "test"}
        elif operation == "delete-object":
            version = arg("--version-id")
            if version is not None or bucket not in self.versioned:
                self.buckets[bucket] = [row for row in self.buckets[bucket] if not (row["Key"] == key and (version is None or row["VersionId"] == version))]
            else:
                self.serial += 1
                self.buckets[bucket].append({"Key": key, "VersionId": str(self.serial), "payload": b"", "marker": True})
                return {"VersionId": str(self.serial), "DeleteMarker": True}
        elif operation == "list-objects-v2":
            return {"Contents": [{"Key": row["Key"], "Size": len(row["payload"])} for row in self.buckets[bucket] if not row["marker"]], "IsTruncated": False}
        elif operation == "list-object-versions":
            return {"Versions": [{"Key": row["Key"], "VersionId": row["VersionId"], "Size": len(row["payload"])} for row in self.buckets[bucket] if not row["marker"]],
                    "DeleteMarkers": [{"Key": row["Key"], "VersionId": row["VersionId"]} for row in self.buckets[bucket] if row["marker"]], "IsTruncated": False}
        elif operation == "get-object":
            version = arg("--version-id")
            row = next(row for row in reversed(self.buckets[bucket]) if row["Key"] == key and (version is None or row["VersionId"] == version))
            Path(arguments[-1]).write_bytes(row["payload"])
        elif operation == "delete-bucket":
            if self.fail_cleanup:
                raise ProbeError("cleanup failed")
            assert not self.buckets[bucket]
            del self.buckets[bucket]
        else:
            raise AssertionError(operation)
        return {}

    def admin(self, path="/rustfs/admin/v3/datausageinfo", endpoint_index=0):
        if self.malformed:
            raise ProbeError("Admin returned invalid JSON")
        value = {"scanner_cycle": self.serial, "usage_snapshot_complete": True,
                 "usage_snapshot_converged": not self.pending, "buckets_usage": {}}
        for bucket, rows in self.buckets.items():
            value["buckets_usage"][bucket] = {"objects_count": len({row["Key"] for row in rows}),
                "size": sum(len(row["payload"]) for row in rows), "versions_count": sum(not row["marker"] for row in rows),
                "delete_markers_count": sum(row["marker"] for row in rows),
                "usage_value_state": "observed" if self.observed else "certified",
                "usage_certification": None if self.observed else certificate(self.serial)}
        self.last_evidence = self.evidence.save("fake-admin", value)
        return value


class ValueContractTests(unittest.TestCase):
    def test_absence_and_missing_counters_do_not_become_zero(self):
        view = bucket_view({"buckets_usage": {}}, "bucket")
        self.assertEqual((view["count"], view["size"], view["state"]), (None, None, "unknown"))
        view = bucket_view(snapshot({"size": 0}), "bucket")
        self.assertIsNone(view["count"])
        self.assertEqual(view["size"], 0)

    def test_pending_partial_observed_and_unknown_cannot_converge(self):
        for changes in ({"usage_snapshot_converged": False}, {"usage_snapshot_partial": True},
                        {"usage_snapshot_observational": True}, {"usage_snapshot_pending_buckets": ["bucket"]},
                        {"usage_snapshot_admin_projection": True},
                        {"usage_snapshot_complete": False}):
            self.assertFalse(bucket_view(snapshot(**changes), "bucket")["converged"])
        for state in ("carried", "observed", "future-state"):
            entry = {"objects_count": 1, "size": 8, "usage_value_state": state}
            self.assertFalse(bucket_view(snapshot(entry), "bucket")["converged"])
        value = snapshot()
        del value["usage_snapshot_converged"]
        self.assertFalse(bucket_view(value, "bucket")["converged"])

    def test_diagnostic_contract_and_no_last_update_synthesis(self):
        value = snapshot({"objects_count": 1, "size": 8, "usage_value_state": "observed", "usage_certification": certificate()})
        with self.assertRaisesRegex(ProbeError, "Observed"):
            bucket_view(value, "bucket")
        value = snapshot({"objects_count": 1, "size": 8, "usage_value_state": "certified"}, last_update=certificate()["certified_at"])
        with self.assertRaisesRegex(ProbeError, "no per-bucket"):
            bucket_view(value, "bucket")
        value = snapshot({"objects_count": 1, "size": 8, "usage_value_state": "future", "usage_certification": {"future": True}})
        self.assertIsNone(bucket_view(value, "bucket")["certificate"])

    def test_invalid_numeric_and_timestamp_types(self):
        for bad in (-1, True, "1"):
            with self.assertRaises(ProbeError):
                bucket_view(snapshot({"objects_count": bad, "size": 8}), "bucket")
        cert = certificate()
        cert["certified_at"]["nanos_since_epoch"] = 10**9
        with self.assertRaises(ProbeError):
            bucket_view(snapshot({"usage_value_state": "certified", "usage_certification": cert}), "bucket")

    def test_endpoint_rejects_embedded_secrets_and_non_origins(self):
        for bad in ("http://user:pass@host", "file:///etc/passwd", "http://host/path", "http://host?token=a",
                    "http://@host", "http://host:65536", "http://host:secret-port", "http://host\n"):
            with self.assertRaises(ValueError):
                endpoint_url(bad)
        self.assertEqual(endpoint_url("https://host:9001/"), "https://host:9001")

    def test_usage_state_rejects_non_string_json_values(self):
        for state in ([], {}, 1, True):
            with self.subTest(state=state), self.assertRaisesRegex(ProbeError, "usage_value_state") as error:
                bucket_view(snapshot({"objects_count": 1, "size": 8, "usage_value_state": state}), "bucket")
            self.assertEqual(error.exception.verdict, "FAIL")

    def test_signed_admin_requests_never_follow_redirects(self):
        received = []
        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append((self.server.server_port, self.path))
                self.send_response(self.server.response_status)
                self.send_header("Location", self.server.destination)
                self.end_headers()
                self.wfile.write(b'{"redirect":true}')

            def log_message(self, *_):
                pass

        servers = [ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler) for _ in range(2)]
        for server in servers:
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
            thread.start()
            self.addCleanup(thread.join, 2)
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            server.response_status = 200
            server.destination = "http://127.0.0.1/unused"
        origin = f"http://127.0.0.1:{servers[0].server_port}"
        with tempfile.TemporaryDirectory() as directory:
            client = Client([origin], [], Evidence(directory), environment={
                "RUSTFS_ACCESS_KEY": "unit-access", "RUSTFS_SECRET_KEY": "unit-secret", "AWS_SESSION_TOKEN": "unit-token"})
            for status in (301, 302, 303, 307, 308):
                for destination in (origin + "/unexpected", f"http://127.0.0.1:{servers[1].server_port}/unexpected"):
                    with self.subTest(status=status, destination=destination):
                        received.clear()
                        servers[0].response_status, servers[0].destination = status, destination
                        with self.assertRaisesRegex(ProbeError, f"HTTP {status}") as error:
                            client.admin()
                        self.assertEqual(error.exception.verdict, "FAIL")
                        self.assertEqual(received, [(servers[0].server_port, "/rustfs/admin/v3/datausageinfo")])
                        record = json.loads((Path(directory) / client.last_evidence).read_text())
                        self.assertEqual((record["status"], record["url"]), (status, origin + "/rustfs/admin/v3/datausageinfo"))


class RunnerTests(unittest.TestCase):
    def run_case(self, case=CASES[0], *, configure=lambda _: None, **changes):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        def factory(*args, **kwargs):
            self.client = FakeClient(*args, **kwargs)
            configure(self.client)
            return self.client
        return execute_case(case, self.directory, options(**changes), PROVENANCE, client_factory=factory)

    def test_static_scenario_runs_oracle_convergence_and_cleanup(self):
        result = self.run_case()
        self.assertEqual(result["verdict"], "PASS", result)
        self.assertEqual(self.client.buckets, {})
        self.assertEqual(len(result["assertions"]), 3)
        self.assertFalse(check_result(catalog_module()["cases"][0], result, gate=True,
                                     artifacts_root=self.directory, target_environments=["configured-endpoint"]))
        self.assertIn("USAGE-01-A03", render_report([result]))

    def test_versioned_scenario_preserves_historical_payloads_and_marker_counts(self):
        result = self.run_case(CASES[19])
        self.assertEqual(result["verdict"], "PASS", result)
        oracles = list((self.directory / "evidence" / CASES[19].id).glob("*-independent-oracle.json"))
        expected = json.loads(oracles[0].read_text())["expected"]
        self.assertEqual((expected["count"], expected["versions"], expected["markers"]), (6, 8, 1))

    def test_product_error_and_cleanup_failure_cannot_pass(self):
        for attr, verdict in (("fail_put", "FAIL"), ("fail_cleanup", "INFRA_ERROR"), ("malformed", "INFRA_ERROR")):
            with self.subTest(attr=attr):
                result = self.run_case(configure=lambda client: setattr(client, attr, True))
                self.assertEqual(result["verdict"], verdict, result)
                if attr == "fail_cleanup":
                    self.assertEqual(result["cleanup_status"], "FAIL")
                    self.assertTrue(self.client.buckets)

    def test_pending_and_observed_exact_values_are_not_converged(self):
        for attr in ("pending", "observed"):
            result = self.run_case(configure=lambda client: setattr(client, attr, True))
            self.assertEqual(result["verdict"], "FAIL")
            self.assertIn("did not converge", result["reason"])

    def test_mutation_probe_does_not_claim_internal_cycle_or_ack_evidence(self):
        result = self.run_case(CASES[2])
        self.assertEqual(result["verdict"], "BLOCKED", result)
        self.assertEqual([item["status"] for item in result["assertions"]], ["PASS", "BLOCKED", "BLOCKED"])

    def test_no_endpoint_is_blocked_without_creating_a_fixture(self):
        result = self.run_case(endpoints=[])
        self.assertEqual(result["verdict"], "BLOCKED")
        self.assertEqual(result["cleanup_status"], "PASS")

    def test_one_endpoint_cannot_prove_multiple_sources(self):
        result = self.run_case(CASES[6])
        self.assertEqual(result["verdict"], "BLOCKED")
        self.assertIn("Multiple explicit", result["reason"])

    def test_minimum_fixture_still_writes_through_every_endpoint(self):
        endpoints = [f"http://endpoint-{index}.invalid:9000" for index in range(3)]
        result = self.run_case(CASES[6], endpoints=endpoints, admin_endpoints=endpoints, objects=2)
        self.assertEqual(result["assertions"][0]["status"], "PASS", result)
        directory = self.directory / "evidence" / CASES[6].id
        acknowledged = [json.loads(line) for line in (directory / "acknowledged-requests.jsonl").read_text().splitlines()]
        puts = [row for row in acknowledged if row["operation"] == "put"]
        self.assertEqual({row["endpoint_index"] for row in puts}, {0, 1, 2})
        self.assertEqual(len(puts), 3)

    def test_malformed_usage_state_preserves_results_and_allows_the_next_case(self):
        class MalformedStateClient(FakeClient):
            def admin(self, *args, **kwargs):
                result = super().admin(*args, **kwargs)
                for entry in result["buckets_usage"].values():
                    entry["usage_value_state"] = []
                return result
        with tempfile.TemporaryDirectory() as directory:
            first = execute_case(CASES[0], directory, options(), PROVENANCE, client_factory=MalformedStateClient)
            second = execute_case(CASES[19], directory, options(), PROVENANCE, client_factory=FakeClient)
            self.assertEqual(first["verdict"], "FAIL")
            self.assertEqual([row["status"] for row in first["assertions"]], ["PASS", "FAIL", "BLOCKED"])
            self.assertEqual(first["cleanup_status"], "PASS")
            self.assertEqual(second["verdict"], "PASS")
            self.assertEqual(len(list((Path(directory) / "cases").glob("*.json"))), 2)

    def test_request_journal_appends_without_rewriting_ownership_or_prior_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Evidence(directory)
            client = FakeClient(["http://example.invalid"], [], evidence)
            fixture = Fixture(client, directory, evidence)
            bucket = fixture.create("append-only")
            ownership_path = Path(directory) / "owned-buckets.json"
            original_ownership = ownership_path.read_bytes()
            with patch.object(fixture, "persist_ownership", wraps=fixture.persist_ownership) as ownership:
                for _ in range(512):
                    fixture.put(bucket, "replaced", 1)
                    fixture.delete(bucket, "replaced")
                ownership.assert_not_called()
            self.assertEqual(ownership_path.read_bytes(), original_ownership)
            journal = fixture.journal_path.read_bytes()
            rows = [json.loads(line) for line in journal.splitlines()]
            self.assertEqual([row["sequence"] for row in rows], list(range(1, 1025)))
            self.assertEqual([row["operation"] for row in rows], ["put", "delete"] * 512)
            self.assertLess(len(journal), 1024 * 512, "journal bytes must be linear in acknowledgement count")
            fixture.put(bucket, "later", 1)
            self.assertTrue(fixture.journal_path.read_bytes().startswith(journal))
            self.assertEqual(fixture.cleanup()[0], "PASS")

    def test_cleanup_never_accepts_a_foreign_bucket(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Evidence(directory)
            client = FakeClient(["http://example.invalid"], [], evidence)
            fixture = Fixture(client, directory, evidence)
            client.buckets["desktop"] = []
            with self.assertRaisesRegex(ProbeError, "unowned"):
                fixture.remove("desktop")
            self.assertEqual(fixture.cleanup()[0], "PASS")
            self.assertIn("desktop", client.buckets)

    def test_lost_create_response_cannot_report_cleanup_success(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Evidence(directory)
            client = FakeClient(["http://example.invalid"], [], evidence)
            fixture = Fixture(client, directory, evidence)
            operation = client.s3
            def lose_response(*args, **kwargs):
                operation(*args, **kwargs)
                raise ProbeError("CreateBucket response was lost")
            with patch.object(client, "s3", side_effect=lose_response), self.assertRaises(ProbeError):
                fixture.create("lost-response")
            ownership = json.loads((Path(directory) / "owned-buckets.json").read_text())
            self.assertEqual(ownership["buckets"], [])
            self.assertEqual(ownership["unconfirmed_creates"], sorted(client.buckets))
            status, cleanup_path = fixture.cleanup()
            self.assertEqual(status, "FAIL")
            self.assertTrue(json.loads((Path(directory) / cleanup_path).read_text())["unconfirmed_creates"])
            self.assertTrue(client.buckets, "unconfirmed ownership cannot authorize deletion")

    def test_inventory_follows_pages_and_rejects_repeated_continuations(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Evidence(directory)
            client = FakeClient(["http://example.invalid"], [], evidence)
            fixture = Fixture(client, directory, evidence)
            bucket = fixture.create("pages")
            pages = [{"Contents": [{"Key": "first", "Size": 1}], "IsTruncated": True,
                      "NextContinuationToken": "page-two"},
                     {"Contents": [{"Key": "second", "Size": 2}], "IsTruncated": False}]
            with patch.object(client, "s3", side_effect=pages) as request:
                self.assertEqual(fixture.inventory(bucket), [{"key": "first", "size": 1}, {"key": "second", "size": 2}])
            self.assertEqual(request.call_args_list[1].args[-2:], ("--continuation-token", "page-two"))
            with patch.object(client, "s3", return_value=pages[0]), self.assertRaisesRegex(ProbeError, "continuation"):
                fixture.inventory(bucket)

    def test_version_inventory_preserves_both_markers_across_pages(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Evidence(directory)
            client = FakeClient(["http://example.invalid"], [], evidence)
            fixture = Fixture(client, directory, evidence)
            bucket = fixture.create("version-pages", versioned=True)
            pages = [{"Versions": [{"Key": "key", "VersionId": "v1", "Size": 3}], "IsTruncated": True,
                      "NextKeyMarker": "key", "NextVersionIdMarker": "v1"},
                     {"DeleteMarkers": [{"Key": "key", "VersionId": "v2"}], "IsTruncated": False}]
            with patch.object(client, "s3", side_effect=pages) as request:
                rows = fixture.inventory(bucket, versioned=True)
            self.assertEqual([(row["version_id"], row["marker"]) for row in rows], [("v1", False), ("v2", True)])
            self.assertEqual(request.call_args_list[1].args[-4:], ("--key-marker", "key", "--version-id-marker", "v1"))

    def test_command_timeout_is_recorded(self):
        result = run_command([sys.executable, "-c", "import time;time.sleep(10)"], 0.02)
        self.assertTrue(result["timed_out"])
        self.assertNotEqual(result["exit_code"], 0)

    def test_s3_subprocess_cannot_inherit_ambient_proxy_routes(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = {"RUSTFS_ACCESS_KEY": "unit-access", "RUSTFS_SECRET_KEY": "unit-secret",
                           "HTTPS_PROXY": "http://proxy.invalid", "http_proxy": "http://proxy.invalid"}
            client = Client(["http://example.invalid"], [], Evidence(directory), environment=environment)
            self.assertNotIn("HTTPS_PROXY", client.environment)
            self.assertNotIn("http_proxy", client.environment)
            self.assertEqual(client.environment["NO_PROXY"], "*")

    def test_failed_request_retains_service_code_without_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client(["http://example.invalid"], [], Evidence(directory), environment={
                "RUSTFS_ACCESS_KEY": "unit-access-secret", "RUSTFS_SECRET_KEY": "unit-signing-secret"})
            response = {"command": ["aws"], "exit_code": 1, "timed_out": False, "duration_seconds": 1,
                        "stdout": "", "stderr": "An error occurred (ServiceUnavailable): unit-signing-secret unit-access-secret"}
            with patch("scripts.validation_scanner_usage.client.run_command", return_value=response), self.assertRaises(ProbeError) as error:
                client.s3("put-object", "--bucket", "unit-fixture")
            self.assertEqual(error.exception.verdict, "FAIL")
            text = (Path(directory) / client.last_evidence).read_text()
            self.assertNotIn("unit-signing-secret", text)
            self.assertNotIn("unit-access-secret", text)
            self.assertEqual(json.loads(text)["service_error_code"], "ServiceUnavailable")

    def test_admin_timeout_is_infra_error_and_retains_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client(["http://example.invalid"], [], Evidence(directory), environment={
                "RUSTFS_ACCESS_KEY": "unit-access", "RUSTFS_SECRET_KEY": "unit-secret"})
            with patch.object(client.http, "open", side_effect=TimeoutError("socket deadline")), self.assertRaises(ProbeError) as error:
                client.admin()
            self.assertEqual(error.exception.verdict, "INFRA_ERROR")
            self.assertTrue((Path(directory) / client.last_evidence).is_file())


class NativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.case = CASES[13]
        self.name = "scanner::tests::usage_exact_fixture"
        self.ids = [assertion_id(self.case, index) for index in range(len(self.case.assertions))]

    def manifest(self, *, missing=False, cleanup="PASS", zero=False, fail=False):
        binary = self.root / "native-test"
        output = ["running 1 test", f"test {self.name} ... ok"]
        output += ["usage_assertion: " + json.dumps({"case_id": self.case.design_id, "assertion_id": identifier,
                    "status": "PASS", "evidence": {"test_double": True}}) for identifier in self.ids if not missing]
        output += ["usage_cleanup: " + json.dumps({"case_id": self.case.design_id, "status": cleanup}),
                   "test result: ok. 1 passed; 0 failed; 0 ignored; 20 filtered out;"]
        if zero:
            output = ["running 0 tests", "test result: ok. 0 passed; 0 failed; 0 ignored;"]
        script = "#!" + sys.executable + "\nimport sys\n"
        script += f"print({(self.name + ': test')!r} if '--list' in sys.argv else {chr(10).join(output)!r})\n"
        if fail:
            script += "sys.exit(0 if '--list' in sys.argv else 1)\n"
        binary.write_text(script)
        binary.chmod(0o700)
        return {"schema_version": 1, "source_commit": PROVENANCE["source_commit"], "cases": {
            self.case.design_id: {"tests": [{"binary": str(binary), "binary_sha256": sha256(binary),
                                            "test": self.name, "assertions": self.ids}]}}}

    def run_native(self, manifest):
        return run_native(self.case, manifest, PROVENANCE["source_commit"], Evidence(self.root / "evidence"), 2, self.ids)

    def test_exact_test_and_each_marker_are_required(self):
        records = self.run_native(self.manifest())
        self.assertEqual(set(records), set(self.ids))
        for values in ({"missing": True}, {"zero": True}, {"cleanup": "FAIL"}, {"fail": True}):
            with self.subTest(values=values), self.assertRaises(ProbeError) as error:
                self.run_native(self.manifest(**values))
            self.assertEqual(error.exception.cleanup_status, "FAIL")

    def test_native_identity_and_capability_fail_closed(self):
        with self.assertRaises(ProbeError) as error:
            self.run_native(None)
        self.assertEqual(error.exception.verdict, "BLOCKED")
        manifest = self.manifest()
        manifest["source_commit"] = "0" * 40
        with self.assertRaisesRegex(ProbeError, "identity"):
            self.run_native(manifest)
        manifest = self.manifest()
        manifest["cases"][self.case.design_id]["tests"][0]["binary_sha256"] = "f" * 64
        with self.assertRaisesRegex(ProbeError, "SHA-256"):
            self.run_native(manifest)

    def test_two_tests_cannot_claim_the_same_atomic_assertion(self):
        manifest = self.manifest()
        specs = manifest["cases"][self.case.design_id]["tests"]
        specs.append(copy.deepcopy(specs[0]))
        with patch("scripts.validation_scanner_usage.native.run_command") as execute:
            with self.assertRaisesRegex(ProbeError, "duplicate coverage"):
                self.run_native(manifest)
            execute.assert_not_called()

    def test_another_cases_milestone_cannot_be_reused(self):
        manifest = self.manifest()
        spec = manifest["cases"][self.case.design_id]["tests"][0]
        binary = Path(spec["binary"])
        binary.write_text(binary.read_text().replace(self.case.design_id, "USAGE-01"))
        spec["binary_sha256"] = sha256(binary)
        with self.assertRaisesRegex(ProbeError, "different case") as error:
            self.run_native(manifest)
        self.assertEqual(error.exception.cleanup_status, "FAIL")

    def test_malformed_native_manifest_produces_a_terminal_adapter_error(self):
        for invalid in ([], {"schema_version": 1, "source_commit": PROVENANCE["source_commit"], "cases": []}):
            with self.subTest(manifest=invalid), self.assertRaises(ProbeError):
                self.run_native(invalid)
        for field, invalid in (("binary", None), ("assertions", [{}])):
            manifest = self.manifest()
            manifest["cases"][self.case.design_id]["tests"][0][field] = invalid
            with self.subTest(field=field), self.assertRaises(ProbeError):
                self.run_native(manifest)

    def test_missing_native_cleanup_cannot_be_hidden_by_public_cleanup(self):
        result = execute_case(self.case, self.root, options(execution="native"), PROVENANCE,
                              native_manifest=self.manifest(cleanup="FAIL"))
        self.assertEqual(result["verdict"], "INFRA_ERROR")
        self.assertEqual(result["cleanup_status"], "FAIL")

    def test_partial_native_coverage_retains_true_assertions_without_passing_case(self):
        all_ids = self.ids
        self.ids = all_ids[:1]
        manifest = self.manifest()
        self.ids = all_ids
        result = execute_case(self.case, self.root, options(execution="native"), PROVENANCE,
                              native_manifest=manifest)
        self.assertEqual(result["verdict"], "UNSUPPORTED")
        self.assertEqual(result["assertions"][0]["status"], "PASS")
        self.assertTrue(all(row["status"] == "UNSUPPORTED" for row in result["assertions"][1:]))
        self.assertEqual(result["migration_status"], "unverified")

    def test_unexecuted_tests_after_failure_are_blocked_not_product_failures(self):
        case = CASES[12]
        ids = [assertion_id(case, index) for index in range(len(case.assertions))]
        manifest = {"schema_version": 1, "source_commit": PROVENANCE["source_commit"],
                    "cases": {case.design_id: {"tests": [{"assertions": [identifier]} for identifier in ids]}}}
        with patch("scripts.validation_scanner_usage.native._run_native_test", side_effect=ProbeError("native regression failed", "FAIL")) as execute:
            result = execute_case(case, self.root, options(execution="native"), PROVENANCE, native_manifest=manifest)
        execute.assert_called_once()
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["assertions"][0]["status"], "FAIL")
        self.assertTrue(all(row["status"] == "BLOCKED" for row in result["assertions"][1:]), result)

    def test_nonexecutable_later_binary_keeps_prior_pass_and_later_blocked_records(self):
        self.case = CASES[12]
        all_ids = [assertion_id(self.case, index) for index in range(len(self.case.assertions))]
        self.ids = all_ids[:1]
        manifest = self.manifest()
        specs = manifest["cases"][self.case.design_id]["tests"]
        blocked_binary = self.root / "not-executable"
        blocked_binary.write_text(Path(specs[0]["binary"]).read_text())
        blocked_binary.chmod(0o600)
        specs.extend({**specs[0], "binary": str(blocked_binary), "binary_sha256": sha256(blocked_binary),
                      "assertions": [identifier]} for identifier in all_ids[1:])
        result = execute_case(self.case, self.root, options(execution="native"), PROVENANCE, native_manifest=manifest)
        self.assertEqual(result["verdict"], "INFRA_ERROR")
        self.assertEqual([row["status"] for row in result["assertions"]], ["PASS", "INFRA_ERROR"] + ["BLOCKED"] * 6)
        self.assertTrue(result["assertions"][0]["native_evidence"])
        self.assertIn("Permission denied", result["reason"])

    def test_real_signal_interrupts_native_process_and_leaves_cleanup_unverified(self):
        manifest = self.manifest()
        spec = manifest["cases"][self.case.design_id]["tests"][0]
        binary, ready = Path(spec["binary"]), self.root / "native-ready.json"
        binary.write_text("#!" + sys.executable + "\nimport json, os, pathlib, sys, time\n"
                          + f"if '--list' in sys.argv:\n    print({(self.name + ': test')!r})\n"
                          + "else:\n"
                          + f"    ready = pathlib.Path({str(ready)!r})\n"
                          + "    ready.with_suffix('.tmp').write_text(json.dumps({'pid': os.getpid()}))\n"
                          + "    ready.with_suffix('.tmp').replace(ready)\n"
                          + "    time.sleep(30)\n")
        spec["binary_sha256"] = sha256(binary)
        manifest_path, candidate_path = self.root / "native.json", self.root / "candidate.json"
        manifest_path.write_text(json.dumps(manifest))
        candidate_path.write_text(json.dumps({"source_sha": PROVENANCE["source_commit"],
            "package_sha256": "a" * 64, "binary_sha256": "b" * 64, "package_url": "https://example.invalid/package"}))
        artifacts = self.root / "interrupted-run"
        command = [sys.executable, "-B", "-m", "scripts.validation_scanner_usage.runner", "--execution", "native",
                   "--cases", self.case.design_id, "--native-manifest", str(manifest_path),
                   "--candidate-manifest", str(candidate_path), "--artifacts-dir", str(artifacts),
                   "--rc-bin", sys.executable, "--native-timeout", "20"]
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists(), "native command did not start")
            pid = json.loads(ready.read_text())["pid"]
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 1, (stdout, stderr))
            result = json.loads((artifacts / "cases" / (self.case.id + ".json")).read_text())
            self.assertEqual((result["verdict"], result["cleanup_status"]), ("INFRA_ERROR", "FAIL"))
            self.assertIn("interrupted by signal", result["reason"])
            errors = list((artifacts / "evidence" / self.case.id).glob("*-native-adapter-error.json"))
            self.assertEqual(json.loads(errors[0].read_text())["cleanup_status"], "FAIL")
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
            process.communicate()

    def test_interruption_before_native_milestone_validation_cannot_report_cleanup_pass(self):
        manifest = self.manifest()
        original_save = Evidence.save
        def interrupted_save(evidence, stem, value):
            if stem == "native-execution":
                raise ProbeError("Runner interrupted before native milestones were validated")
            return original_save(evidence, stem, value)
        with patch.object(Evidence, "save", interrupted_save):
            result = execute_case(self.case, self.root, options(execution="native"), PROVENANCE, native_manifest=manifest)
        self.assertEqual((result["verdict"], result["cleanup_status"]), ("INFRA_ERROR", "FAIL"))


class RegistrationTests(unittest.TestCase):
    def test_cli_rejects_endpoint_secrets_before_artifacts_in_every_mode(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            artifacts = Path(directory) / "must-not-exist"
            for execution in ("combined", "native"):
                for flag in ("--endpoint", "--admin-endpoint"):
                    for endpoint in ("http://user:private-password@host", "http://host?token=private-token",
                                     "http://host:private-port"):
                        with self.subTest(execution=execution, flag=flag, endpoint=endpoint):
                            stderr = io.StringIO()
                            with redirect_stderr(stderr):
                                result = main(["--execution", execution, flag, endpoint, "--cases", "USAGE-14",
                                               "--artifacts-dir", str(artifacts), "--candidate-manifest", "not-read.json"])
                            self.assertEqual(result, 2)
                            self.assertIn("endpoint must be", stderr.getvalue())
                            self.assertNotIn("private-", stderr.getvalue())
                            self.assertFalse(artifacts.exists())

    def test_native_cli_records_only_normalized_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.json"
            candidate.write_text(json.dumps({"source_sha": "c" * 40, "package_sha256": "a" * 64,
                                            "binary_sha256": "b" * 64, "package_url": "https://example.invalid/package"}))
            artifacts = root / "artifacts"
            result = main(["--execution", "native", "--cases", "USAGE-14", "--endpoint", "http://host:9000/",
                           "--admin-endpoint", "http://host:9001/", "--artifacts-dir", str(artifacts),
                           "--candidate-manifest", str(candidate), "--rc-bin", sys.executable])
            self.assertEqual(result, 1, "missing native capability stays blocked")
            config = json.loads((artifacts / "run-configuration.json").read_text())
            self.assertEqual(config["endpoints"], ["http://host:9000"])
            self.assertEqual(config["admin_endpoints"], ["http://host:9001"])

    def test_twenty_native_rows_without_capabilities_remain_blocked(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("scripts.validation_scanner_usage.runner.Client", side_effect=AssertionError("no endpoint access")) as client:
                results = [execute_case(case, directory, options(execution="native", endpoints=[]), PROVENANCE, client_factory=client)
                           for case in select_cases("all")]
                client.assert_not_called()
            self.assertEqual(len(results), 20)
            self.assertTrue(all(result["verdict"] == "BLOCKED" for result in results))
            self.assertTrue(all(result["profile"] == "native-test" and result["hit_count"] == 0 for result in results))
            self.assertTrue(all(result["cleanup_status"] == "PASS" for result in results))
            self.assertTrue(all(assertion["status"] == "BLOCKED" for result in results for assertion in result["assertions"]))
            self.assertEqual(len(list((Path(directory) / "cases").glob("*.json"))), 20)

    def test_twenty_rows_are_registered_without_rewriting_source_counts(self):
        catalog = json.loads((ROOT / "validation-cases.json").read_text())
        validate_snapshot(catalog)
        self.assertEqual(catalog["local_modules"], [catalog_module()])
        self.assertEqual(sum(len(module["cases"]) for module in catalog["modules"]), 158)
        self.assertEqual(sum(len(module["cases"]) for module in catalog_modules(catalog)), 178)
        self.assertEqual(len(select_cases("all")), 20)
        self.assertEqual([case.number for case in select_cases("blackbox")], [1, 20])
        self.assertNotIn(CASES[0].id, [case for case, _ in sweepable_cases(catalog)])
        self.assertEqual(select_validation_cases(catalog, case=CASES[0].id), [(CASES[0].id, "scanner-usage-regressions")])

    def test_missing_assertion_artifact_fails_the_shared_report(self):
        with tempfile.TemporaryDirectory() as directory:
            result = {"schema_version": 1, "case_id": CASES[0].id, "source_status": "procedure-ready",
                      "profile": "configured-endpoint", "verdict": "PASS", "migration_status": "verified",
                      "cleanup_status": "PASS", "provenance": PROVENANCE,
                      "assertions": [{"source_assertion": text, "status": "PASS", "evidence": ["missing.json"]}
                                     for _, text in CASES[0].assertions]}
            errors = check_result(catalog_module()["cases"][0], result, gate=True,
                                  artifacts_root=Path(directory), target_environments=["configured-endpoint"])
            self.assertTrue(any("missing or unsafe evidence" in error for error in errors))

    def test_dry_run_neither_reads_candidate_nor_creates_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "not-created"
            result = run_command(["bash", str(ROOT / "rustfs-scanner-usage-test.sh"), "--dry-run",
                                  "--cases", "all", "--artifacts-dir", str(destination),
                                  "--candidate-manifest", str(Path(directory) / "not-a-manifest")], 10)
            self.assertEqual(result["exit_code"], 0, result)
            self.assertEqual(len(json.loads(result["stdout"])["cases"]), 20)
            self.assertFalse(destination.exists())


if __name__ == "__main__":
    unittest.main()
