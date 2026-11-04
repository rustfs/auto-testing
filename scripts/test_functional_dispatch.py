#!/usr/bin/env python3
"""Receiver contract for nightly-candidate-ready and the local harness checkout."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock

import resolve_functional_candidate as resolver
from datetime import datetime, timezone

from functional_chain_evidence import aggregate, current_chain
from select_validation_cases import select_validation_cases
from validation_registry import check_wiring, load_registry, select_plan, workflow_entry


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"


def candidate(**overrides):
    digest = "ab" * 32
    manifest = {
        "schema": 2,
        "workflow_sha": "c" * 40,
        "source_ref": "main",
        "source_sha": "d" * 40,
        "build_run_id": 12345,
        "build_run_attempt": 1,
        "package_sha256": digest,
        "package_url": f"https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/12345/1/{digest}/rustfs.deb",
    }
    manifest.update(overrides)
    return manifest


def payload(manifest=None, **overrides):
    body = {
        "event_type": "nightly-candidate-ready",
        "source_repo": "rustfs/rustfs",
        "candidate": candidate() if manifest is None else manifest,
        "latest_alias_url": resolver.LATEST_ALIAS_URL,
        "auto_testing_release": resolver.RELEASE_URL,
        "channel": "nightly",
        "deb_date": "2026-09-06",
        "nightly_workflow": resolver.NIGHTLY_WORKFLOW,
    }
    body.update(overrides)
    return body


class DispatchContractTests(unittest.TestCase):
    def test_nested_candidate_is_schema_2_and_not_the_alias(self):
        manifest = resolver.dispatch_manifest(payload())
        self.assertEqual(manifest["schema"], 2)
        self.assertEqual(set(manifest), resolver.SCHEMA2_FIELDS)
        self.assertNotEqual(manifest["package_url"], resolver.LATEST_ALIAS_URL)
        self.assertIn("/runs/12345/1/", manifest["package_url"])
        self.assertTrue(manifest["package_url"].endswith("/" + manifest["package_sha256"] + "/rustfs.deb"))

    def test_flat_schema_2_object_is_rejected(self):
        with self.assertRaises(ValueError):
            resolver.dispatch_manifest(candidate())

    def test_wrapper_fields_are_exact(self):
        body = payload()
        body["extra"] = "nope"
        with self.assertRaises(ValueError):
            resolver.dispatch_manifest(body)
        for key in ("latest_alias_url", "channel", "deb_date", "source_repo", "nightly_workflow", "auto_testing_release"):
            broken = payload()
            broken[key] = "wrong"
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    resolver.dispatch_manifest(broken)

    def test_candidate_url_must_bind_run_attempt_and_checksum(self):
        digest = "ab" * 32
        for url in (
            resolver.LATEST_ALIAS_URL,
            f"https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/12345/2/{digest}/rustfs.deb",
            f"https://github.com/rustfs/auto-testing/releases/download/nightly/rustfs.deb",
            f"https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/12345/1/{'cd' * 32}/rustfs.deb",
        ):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    resolver.schema2_manifest(candidate(package_url=url))

    def test_short_shas_are_rejected(self):
        with self.assertRaises(ValueError):
            resolver.schema2_manifest(candidate(source_sha="d" * 39))
        with self.assertRaises(ValueError):
            resolver.schema2_manifest(candidate(package_sha256="ab" * 31))

    def test_reverify_requires_the_artifact_to_match_the_nested_candidate(self):
        manifest = candidate()
        resolved = resolver.unverified_candidate(manifest)
        resolved["artifact_id"] = 9
        resolved["artifact_digest"] = "sha256:" + manifest["package_sha256"]
        resolved["build_started_at"] = "2026-09-06T00:00:00Z"
        with mock.patch.object(resolver, "resolve", return_value=resolved) as resolve:
            self.assertEqual(resolver.candidate_from_manifest(manifest, verify=True), resolved)
        resolve.assert_called_once_with(12345, 1)
        mismatch = json.loads(json.dumps(resolved))
        mismatch["manifest"] = candidate(source_sha="e" * 40)
        with mock.patch.object(resolver, "resolve", return_value=mismatch):
            with self.assertRaises(ValueError):
                resolver.candidate_from_manifest(manifest, verify=True)

    def test_missing_token_trusts_the_payload_without_calling_upstream(self):
        manifest = candidate()
        with mock.patch.object(resolver, "resolve") as resolve:
            got = resolver.candidate_from_manifest(manifest, verify=False)
        resolve.assert_not_called()
        self.assertEqual(got["artifact_id"], 0)
        self.assertEqual(got["artifact_digest"], "unverified")
        self.assertEqual(got["manifest"]["package_url"], manifest["package_url"])

    def test_prepare_reads_client_payload_candidate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            event = root / "event.json"
            output = root / "output"
            chain = root / "chain.json"
            event.write_text(json.dumps({"action": "nightly-candidate-ready", "client_payload": payload()}))
            env = {
                "GITHUB_EVENT_PATH": str(event),
                "GITHUB_EVENT_NAME": "repository_dispatch",
                "GITHUB_SHA": "a" * 40,
                "GITHUB_RUN_ID": "50",
                "GITHUB_RUN_ATTEMPT": "2",
                "GITHUB_OUTPUT": str(output),
                "CHAIN_OUTPUT": str(chain),
                "PATH": os.environ["PATH"],
            }
            with mock.patch.dict(os.environ, env, clear=True):
                resolver.prepare()
            written = json.loads(chain.read_text())
            self.assertEqual(written["testing_sha"], "a" * 40)
            self.assertEqual(written["workflow_sha"], "a" * 40)
            self.assertEqual(written["candidate"]["manifest"]["build_run_id"], 12345)
            self.assertNotIn("latest_alias_url", json.dumps(written["candidate"]))
            self.assertTrue(output.read_text().startswith("manifest="))

    def test_prepare_rejects_workflow_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            event = root / "event.json"
            event.write_text("{}")
            env = {
                "GITHUB_EVENT_PATH": str(event),
                "GITHUB_EVENT_NAME": "workflow_run",
                "GITHUB_SHA": "a" * 40,
                "GITHUB_RUN_ID": "1",
                "GITHUB_RUN_ATTEMPT": "1",
                "GITHUB_OUTPUT": str(root / "output"),
                "CHAIN_OUTPUT": str(root / "chain.json"),
                "PATH": os.environ["PATH"],
            }
            with mock.patch.dict(os.environ, env, clear=True):
                with self.assertRaises(ValueError):
                    resolver.prepare()

    def test_unverified_chain_is_accepted_by_the_suite_binder(self):
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        chain = {
            "schema": 1,
            "run_id": 7,
            "attempt": 1,
            "workflow_sha": head,
            "testing_sha": head,
            "candidate": resolver.unverified_candidate(candidate()),
        }
        env = {"CHAIN_MANIFEST": json.dumps(chain), "GITHUB_RUN_ID": "7", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SHA": head}
        with mock.patch.dict(os.environ, env, clear=False):
            self.assertEqual(current_chain()["candidate"]["artifact_digest"], "unverified")
            self.assertEqual(current_chain()["candidate"]["manifest"]["package_url"], candidate()["package_url"])

    def test_repository_dispatch_selects_the_full_plan(self):
        plan = select_plan(load_registry(), "nightly", "", "repository_dispatch")
        self.assertIn("upgrade", plan["suites"])
        self.assertIn("validation-cases", plan["suites"])
        self.assertNotIn("performance", plan["suites"])
        self.assertNotIn("performance", plan["optional_suites"])
        self.assertEqual(workflow_entry(".github/workflows/rustfs-performance-test.yml")["policy"], "disabled")
        with self.assertRaises(ValueError):
            select_plan(load_registry(), "nightly", "s3", "repository_dispatch")
        with self.assertRaises(ValueError):
            select_plan(load_registry(), "nightly", "performance", "workflow_dispatch")

    def test_disabled_performance_is_not_required_in_aggregate_needs(self):
        plan = select_plan(load_registry(), "nightly", "", "repository_dispatch")
        needs = {name: {"result": "skipped"} for name in plan["suites"]}
        with self.assertRaises(ValueError) as caught:
            aggregate({"schema": 1}, Path("/tmp/chain-evidence-absent"), needs, plan=plan)
        message = str(caught.exception)
        self.assertIn("skipped without preflight permission", message)
        self.assertNotIn("performance", message)
        omitted = {name: {"result": "success"} for name in plan["suites"] if name != "validation-cases"}
        with self.assertRaises(ValueError) as missing:
            aggregate({"schema": 1}, Path("/tmp/chain-evidence-absent"), omitted, plan=plan)
        self.assertIn("missing a scheduled lane", str(missing.exception))

    def test_nightly_validation_window_rotates_and_skips_interactive_heal(self):
        catalog = json.loads((ROOT / "validation-cases.json").read_text())
        first = select_validation_cases(catalog, limit=12, rotate=True, when=datetime(2026, 1, 1, tzinfo=timezone.utc))
        again = select_validation_cases(catalog, limit=12, rotate=True, when=datetime(2026, 1, 1, tzinfo=timezone.utc))
        second = select_validation_cases(catalog, limit=12, rotate=True, when=datetime(2026, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(first, again)
        self.assertEqual(len(first), 12)
        self.assertNotEqual(first, second)
        self.assertTrue(all(not case_id.startswith("heal.") for case_id, _ in first))
        self.assertEqual(select_validation_cases(catalog, case="pool.status"), [("pool.status", "pool-lifecycle")])
        with self.assertRaises(ValueError):
            select_validation_cases(catalog, case="heal.admin-healthy-noop")

    def test_registry_matches_ported_workflows(self):
        check_wiring(load_registry())

    def test_registry_ignores_retired_event_names_in_comments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / ".github/workflows"
            workflows.mkdir(parents=True)
            (workflows / "suite.yml").write_text(
                "name: Suite\n"
                "# repository_dispatch relay retired\n"
                "on:\n"
                "  workflow_dispatch:\n"
                "jobs:\n"
                "  suite:\n"
                "    concurrency:\n"
                "      group: shared-lock\n"
                "      cancel-in-progress: false\n"
                "    steps:\n"
                "      - run: python3 scripts/functional_chain_evidence.py record --suite suite\n"
            )
            registry = {
                "workflows": [{
                    "workflow": ".github/workflows/suite.yml",
                    "name": "Suite",
                    "events": ["workflow_dispatch"],
                    "schedule": [],
                }],
                "suites": [{
                    "id": "suite",
                    "workflow": ".github/workflows/suite.yml",
                    "resource_lock": "shared-lock",
                }],
            }
            self.assertTrue(check_wiring(registry, root))

    def test_workflows_checkout_this_repo_for_the_harness(self):
        chain = (WORKFLOWS / "rustfs-functional-chain.yml").read_text()
        self.assertIn("types: [nightly-candidate-ready]", chain)
        self.assertNotIn("\n  workflow_run:\n", chain)
        self.assertIn("performance_ready == 'online'", chain)
        self.assertIn("validation-cases:", chain)
        self.assertIsNone(re.search(r"(?m)^  performance:\s*$", chain))
        self.assertIn("\n  # performance:\n", chain)
        self.assertNotIn("repository: rustfs/auto-testing", chain)
        heal = (WORKFLOWS / "rustfs-heal-test.yml").read_text()
        self.assertIn("http://rustfs-node1:9000", heal)
        self.assertNotIn('--endpoint "${{ env.RUSTFS_API_ENDPOINT }}"', heal)
        self.assertIn('ENDPOINT="${RUSTFS_API_ENDPOINT:-http://rustfs-node1:9000}"', heal)
        performance = (WORKFLOWS / "rustfs-performance-test.yml").read_text()
        self.assertIn("if: false", performance)
        self.assertIn("pf-testing environment is broken", performance)
        sweep = (WORKFLOWS / "rustfs-validation-sweep.yml").read_text()
        self.assertIn("select_validation_cases.py", sweep)
        self.assertIn("group: rustfs-shared-functional-tests-v2", sweep)
        self.assertIsNone(re.search(r"(?m)^  schedule:\s*$", sweep))
        for path in WORKFLOWS.glob("rustfs-*-test.yml"):
            text = path.read_text()
            self.assertNotIn("repository: rustfs/auto-testing", text, path.name)
            self.assertNotIn("repository_dispatch", text, path.name)
            self.assertIn("cancel-in-progress: false", text, path.name)
        security = (WORKFLOWS / "rustfs-security-test.yml").read_text()
        self.assertIn("repository: rustfs/rustfs", security)
        self.assertIn("oidc_keycloak_live.sh", security)
        self.assertIn("runs-on: smoke-testing", (WORKFLOWS / "rustfs-upgrade-test.yml").read_text())
        self.assertIn("group: rustfs-shared-functional-tests-v2", (WORKFLOWS / "rustfs-heal-test.yml").read_text())
        self.assertIn("group: rustfs-performance-suite", (WORKFLOWS / "rustfs-performance-test.yml").read_text())


if __name__ == "__main__":
    unittest.main()
