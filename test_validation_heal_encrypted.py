import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import encrypted, encryption_runtime, heal_fault
from scripts.validation_heal.heal_probe import Probe, redact
from scripts.validation_heal.shards import map_target
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
import test_validation_heal as shard_fixtures
from test_validation_heal_authorization import binding


class EncryptedObjectTests(unittest.TestCase):
    def geometry(self):
        before = shard_fixtures.HealShardOracleTest().snapshot()
        key, bucket = "objects/0008.bin", "rv-heal-0123456789abcdef"
        expected = {"key": key, "versionId": "null", "isLatest": True, "deleteMarker": False,
                    "payload": {"size": encrypted.SIZE, "sha256": "plaintext"}}
        for observation in before.values():
            for entry in observation["files"]:
                if entry["path"] == f"{bucket}/{key}/xl.meta":
                    for version in entry["metadata"]["versions"]:
                        version["size"] = encrypted.SIZE + 16000
                        version["parts"][0].update(size=encrypted.SIZE + 16000, actualSize=encrypted.SIZE)
        return before, bucket, expected

    def test_ciphertext_geometry_keeps_plain_mapper_strict_and_does_not_rewrite_evidence(self):
        before, bucket, expected = self.geometry()
        saved = copy.deepcopy(before)
        target = encrypted.map_cipher_target(before, bucket, expected, "data")
        self.assertTrue(target["encryptedPhysicalLayer"])
        self.assertEqual(target["plaintextACK"], expected["payload"])
        self.assertNotEqual(target["ciphertextPartSize"], expected["payload"]["size"])
        self.assertEqual(before, saved)
        with self.assertRaisesRegex(ValueError, "encrypted"):
            map_target(before, bucket, expected["key"], "data", 1)

    def test_ciphertext_policy_rejects_plaintext_downgrade_wrong_ack_and_unsupported_transforms(self):
        for kind in ("plain", "actual", "size", "multipart", "inline", "unbounded", "bool"):
            before, bucket, expected = self.geometry()
            meta = next(entry for entry in before["node1"]["files"] if entry["path"] == f"{bucket}/{expected['key']}/xl.meta")
            version = meta["metadata"]["versions"][0]
            if kind == "plain": version["parts"][0]["size"] = encrypted.SIZE
            if kind == "actual": version["parts"][0]["actualSize"] -= 1
            if kind == "size": version["size"] -= 1
            if kind == "multipart": version["parts"].append(copy.deepcopy(version["parts"][0]))
            if kind == "inline": version["inline"] = {"length": 1}
            if kind == "unbounded": version["parts"][0]["size"] = encrypted.SIZE + 2 * 1024**2
            if kind == "bool": version["parts"][0]["actualSize"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError): encrypted.map_cipher_target(before, bucket, expected, "data")

    def test_customer_key_is_resolved_only_for_sdk_and_both_key_forms_are_redacted(self):
        key, wrong = "0123456789abcdef0123456789abcdef", "fedcba9876543210fedcba9876543210"
        probe = Probe(binding())
        sdk = unittest.mock.Mock()
        sdk.head_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        with patch.dict(os.environ, {"HEAL_SSE_C_KEY": key, "HEAL_SSE_C_WRONG_KEY": wrong}), patch.object(probe, "s3_client", return_value=sdk):
            for reference, actual in (("$HEAL_SSE_C_KEY", key), ("$HEAL_SSE_C_WRONG_KEY", wrong)):
                observed = probe.s3("head_object", {"Bucket": probe.binding["bucketPrefix"], "Key": "encrypted",
                    "SSECustomerAlgorithm": "AES256", "SSECustomerKey": reference})
                self.assertTrue(observed["acknowledged"])
                self.assertEqual(sdk.head_object.call_args.kwargs["SSECustomerKey"], actual)
                self.assertEqual(observed["params"]["SSECustomerKey"], "[REDACTED]")
                self.assertNotIn(actual, json.dumps(redact({"error": "rejected " + actual})))

    def test_encryption_headers_reject_silent_plaintext_or_wrong_key_identity(self):
        with patch.dict(os.environ, {"HEAL_SSE_C_KEY": "0123456789abcdef0123456789abcdef"}):
            good = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "owned-key"}
            encrypted.encryption_headers(good, "encrypted-versioned", "owned-key")
            for changed in ({}, {**good, "SSEKMSKeyId": "other"}, {**good, "ServerSideEncryption": "AES256"}):
                with self.assertRaises(ValueError): encrypted.encryption_headers(changed, "encrypted-versioned", "owned-key")
            raw_key = os.environ["HEAL_SSE_C_KEY"].encode()
            customer = {"SSECustomerAlgorithm": "AES256", "SSECustomerKeyMD5": base64.b64encode(hashlib.md5(raw_key, usedforsecurity=False).digest()).decode()}
            encrypted.encryption_headers(customer, "sse-c", "unused")
            with self.assertRaises(ValueError): encrypted.encryption_headers({**customer, "ServerSideEncryption": "AES256"}, "sse-c", "unused")

    def test_key_refusal_requires_reviewed_explicit_error_and_never_accepts_missing_object(self):
        for customer, status, code in ((True, 400, "InvalidRequest"), (False, 403, "AccessDenied")):
            good = {"acknowledged": False, "resultUnknown": False,
                "response": {"ResponseMetadata": {"HTTPStatusCode": status}, "Error": {"Code": code}}}
            self.assertTrue(encrypted.denied_key(good, customer=customer)["missingOrWrongKeyIsNotObjectAbsence"])
            for kind in ("transport", "payload", "absence", "success"):
                bad = copy.deepcopy(good)
                if kind == "transport": bad["resultUnknown"] = True
                if kind == "payload": bad["payload"] = {"sha256": "decrypted"}
                if kind == "absence": bad["response"] = {"ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": "NoSuchKey"}}
                if kind == "success": bad["acknowledged"] = True
                with self.subTest(customer=customer, kind=kind), self.assertRaises(ValueError): encrypted.denied_key(bad, customer=customer)

    def test_versioned_fixture_preserves_real_distinct_acks_and_every_generation(self):
        sequence = [0]
        def invoke(operation, params, body=None):
            if operation == "get_bucket_versioning": return {"Status": "Enabled"}
            if operation == "put_object":
                sequence[0] += 1
                return {"VersionId": f"real-{sequence[0]}", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "owned-key"}
            return {}
        recorder = types.SimpleNamespace(call=invoke)
        with patch.object(encrypted, "COUNT", 2):
            expected = encrypted.seed_encrypted(recorder, "bucket", "encrypted-versioned", "owned-key")
        self.assertEqual(len(expected), 6)
        self.assertEqual(sum(item["isLatest"] for item in expected), 2)
        self.assertEqual(len({item["versionId"] for item in expected}), 6)
        self.assertEqual(len({item["payload"]["sha256"] for item in expected}), 6)
        with patch.object(encrypted, "COUNT", 1), self.assertRaises(ValueError):
            encrypted.seed_encrypted(types.SimpleNamespace(call=lambda *args: {"Status": "Enabled", "VersionId": "null",
                "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "owned-key"}), "bucket", "encrypted-versioned", "owned-key")

    def test_inventory_decryption_options_cannot_override_identity_or_expose_raw_key(self):
        probe = Probe.__new__(Probe)
        probe.binding = {"bucketPrefix": "rv-heal-test-bucket"}
        for options in ({"Bucket": "unowned"}, {"Key": "other"}, {"VersionId": "other"},
                        {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": "raw-secret"}, {"SSECustomerKey": "$HEAL_SSE_C_WRONG_KEY"}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                probe.inventory("rv-heal-test-bucket", object_read_options=options)

    @unittest.skipUnless(sys.platform == "linux", "actual Linux FD/mount guard executes in CI")
    def test_static_dependency_keeps_private_key_file_and_configured_process_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder)
            original = b"RUSTFS_ACCESS_KEY=temporary\n"
            (state / "service.env").write_bytes(original)
            owner = {"runId": "gh-12345-1-h1"}
            key_id = "rv-heal-key-" + owner["runId"]
            key = base64.b64encode(bytes(range(32))).decode()
            proof = encryption_runtime.configure(state, owner, {"keyId": key_id, "keyBase64": key}, heal_fault)
            self.assertNotIn(key, json.dumps(proof))
            self.assertFalse(proof["sharedKmsTouched"])
            self.assertNotIn(key.encode(), (state / "service.env").read_bytes())
            settings = encryption_runtime.settings_for(state, key_id)
            digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            checked = lambda path, boundary: path
            self.assertTrue(encryption_runtime.attest(state, owner, checked, digest, settings)["loadedByCandidate"])
            with self.assertRaises(ValueError): encryption_runtime.attest(state, owner, checked, digest, {**settings, "RUSTFS_KMS_BACKEND": "local"})
            (state / "static-kms-secret").chmod(0o644)
            with self.assertRaises(ValueError): encryption_runtime.attest(state, owner, checked, digest, settings)
            with self.assertRaises(ValueError): encryption_runtime.configure(state, owner, {"keyId": key_id, "keyBase64": key}, heal_fault)

    def test_all_source_encryption_branches_have_an_automatic_entry(self):
        self.assertEqual(select("D08")[0]["variants"], list(encrypted.BRANCHES))
        self.assertIn(encrypted.CASE_ID, AUTOMATIC_CASES)


if __name__ == "__main__": unittest.main()
