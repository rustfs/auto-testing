"""Tests for the 257-byte root MRF refresh regression probe (backlog#3194)."""

import json
import os
from pathlib import Path
import unittest
import uuid

from scripts.mrf_long_root_refresh import (
    BUCKET,
    OBJECT_KEY,
    build_partial_write_snapshot,
    exercise,
    sigv4_headers,
    verify_refresh_response,
    verify_risk_acceptance_refusal,
)
from scripts.validation_heal import mrf_records as records


class MrfLongRootRefreshTests(unittest.TestCase):
    def test_snapshot_seed_decodes_under_mrf_record_validator(self):
        owner = uuid.UUID("12345678-1234-4234-8234-123456789abc")
        payload, manifest = build_partial_write_snapshot(owner=owner)
        decoded = records.decode_journal(payload)
        self.assertEqual(len(decoded), 1)
        self.assertEqual(decoded[0]["bucket"], BUCKET)
        self.assertEqual(decoded[0]["key"], OBJECT_KEY)
        self.assertEqual(decoded[0]["versionId"], "null")
        self.assertEqual(decoded[0]["poolIndex"], 0)
        self.assertEqual(decoded[0]["setIndex"], 0)
        binding = records.commit(manifest, payload)
        self.assertEqual(binding["owner"], str(owner))
        self.assertEqual(binding["sequence"], 1)

    def test_sigv4_headers_preserve_257_byte_credential_prefix(self):
        long_key = "M" * 257
        headers = sigv4_headers("127.0.0.1:9000", long_key, "secret-value", "POST", "/rustfs/admin/v4/heal/mrf/responsibilities/actions", b"{}")
        self.assertIn(f"Credential={long_key}/", headers["Authorization"])
        self.assertEqual(headers["content-type"], "application/json")

    def test_refresh_verifier_accepts_refreshed_and_rejects_actor_refusal(self):
        rid = "42345678-1234-4234-8234-123456789abc"
        good = json.dumps({
            "responsibilityId": rid,
            "state": "refreshed",
            "dataVerified": False,
            "durableResponsibilityRetained": True,
        }).encode("utf-8")
        self.assertEqual(verify_refresh_response(200, good, rid)["state"], "refreshed")

        with self.assertRaisesRegex(ValueError, "HTTP 400"):
            verify_refresh_response(400, b"<Error><Code>InvalidRequest</Code><Message>invalid authenticated actor</Message></Error>", rid)

        for change in ({"state": "active"}, {"responsibilityId": "other"}, {"dataVerified": True}, {"durableResponsibilityRetained": False}):
            bad = json.loads(good.decode("utf-8"))
            bad.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                verify_refresh_response(200, json.dumps(bad).encode("utf-8"), rid)

    def test_risk_acceptance_refusal_verifier(self):
        verify_risk_acceptance_refusal(
            400,
            b"<Error><Code>InvalidRequest</Code><Message>operator action is invalid: invalid authenticated actor</Message></Error>",
        )
        with self.assertRaises(ValueError):
            verify_risk_acceptance_refusal(200, b'{"state":"operatorAcceptedUnverified"}')
        with self.assertRaises(ValueError):
            verify_risk_acceptance_refusal(400, b"<Error><Code>InvalidRequest</Code><Message>missing reason</Message></Error>")

    @unittest.skipUnless(os.environ.get("RUSTFS_BIN"), "set RUSTFS_BIN to test a local RustFS binary")
    def test_mrf_long_root_refresh_on_local_binary(self):
        exercise(Path(os.environ["RUSTFS_BIN"]))


if __name__ == "__main__":
    unittest.main()
