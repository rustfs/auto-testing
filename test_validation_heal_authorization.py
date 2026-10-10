"""Exercise signing, denial isolation and temporary-principal finalization."""

from datetime import UTC, datetime, timedelta
import hashlib
import hmac
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from botocore.awsrequest import AWSRequest

from scripts.validation_heal.authorization import DENIALS, authorization_requests, iam_exchange, validate_denial
from scripts.validation_heal.heal_probe import Probe
from scripts.validation_heal.heal_signing import sign_admin_request
from scripts.validation_heal.healthy import BASE_OPTIONS, Fleet
from scripts.validation_heal.matrix import select


def binding():
    return {"schemaVersion": 1, "runId": "manual-auth-test", "jumpHostOnly": True,
            "exclusiveHealService": True, "leaseExpiresAt": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            "bucketPrefix": "rv-heal-auth-test", "endpoints": {"node1": "http://127.0.0.1:19000"},
            "defaultEndpoint": "node1", "candidateCommit": "a" * 40, "harnessCommit": "b" * 40,
            "candidatePackageSha256": "c" * 64, "candidateBinarySha256": "d" * 64,
            "rcBinarySha256": "e" * 64, "maxInventoryObjects": 128, "maxInventoryBytes": 1024**2}


class Response(io.BytesIO):
    code = 200
    headers = {}


class AuthorizationBoundaryTest(unittest.TestCase):
    def test_old_timestamp_recomputes_valid_signature_and_credential_scope(self):
        timestamp, access, secret = "20000102T030405Z", "isolated-access", "isolated-secret"
        body = b'{"dryRun":true}'
        digest = hashlib.sha256(body).hexdigest()
        outgoing = AWSRequest(method="POST", url="http://127.0.0.1:19000/rustfs/admin/v3/heal/rv-heal-auth-test?clientToken=token",
                              data=body, headers={"Content-Type": "application/json", "x-amz-content-sha256": digest})
        sign_admin_request(outgoing, access, secret, timestamp=timestamp)
        # Independent SigV4 calculation for this fixed wire request. The old
        # date participates in canonical headers, credential scope and HMAC.
        canonical = "\n".join(("POST", "/rustfs/admin/v3/heal/rv-heal-auth-test", "clientToken=token",
            "content-type:application/json\nhost:127.0.0.1:19000\nx-amz-content-sha256:" + digest + "\nx-amz-date:" + timestamp + "\n",
            "content-type;host;x-amz-content-sha256;x-amz-date", digest))
        scope = "20000102/us-east-1/s3/aws4_request"
        string_to_sign = "\n".join(("AWS4-HMAC-SHA256", timestamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
        key = ("AWS4" + secret).encode()
        for value in ("20000102", "us-east-1", "s3", "aws4_request"):
            key = hmac.new(key, value.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
        self.assertEqual(outgoing.headers["X-Amz-Date"], timestamp)
        self.assertIn("Credential=" + access + "/" + scope, outgoing.headers["Authorization"])
        self.assertTrue(outgoing.headers["Authorization"].endswith("Signature=" + signature))
        tampered = canonical.replace(timestamp, "20000103T030405Z")
        self.assertNotEqual(hashlib.sha256(canonical.encode()).hexdigest(), hashlib.sha256(tampered.encode()).hexdigest())

    def test_probe_uses_isolated_principal_and_expired_client_clock(self):
        probe = Probe(binding(), credential_pair=("client-access", "client-secret"))
        sent = []
        def opened(outgoing):
            sent.append(outgoing)
            return Response(b'{"Code":"RequestTimeTooSkewed"}')
        with patch.dict("os.environ", {"HEAL_ACCESS_KEY": "root-access", "HEAL_SECRET_KEY": "root-secret", "HEAL_SESSION_TOKEN": "root-token"}), \
             patch.object(probe, "_open_admin", side_effect=opened):
            observed = probe.admin("/rustfs/admin/v3/heal/rv-heal-auth-test", [], b"{}", auth="expired-signature")
        signed_at = datetime.strptime(observed["isolatedClientSigningTimestamp"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        self.assertAlmostEqual((datetime.now(UTC) - signed_at).total_seconds(), 86400, delta=3)
        headers = dict(sent[0].header_items())
        self.assertIn("Credential=client-access/", headers["Authorization"])
        self.assertNotIn("X-amz-security-token", headers)
        self.assertNotIn("root-access", json.dumps(headers))

    def test_denial_requires_exact_code_and_no_nested_json_or_xml_disclosure(self):
        for mode, (status, code) in DENIALS.items():
            good = {"httpStatus": status, "response": {"Code": code}}
            self.assertTrue(validate_denial(good, mode)["noTaskDisclosure"])
            for poisoned in ({**good, "httpStatus": 200}, {**good, "response": {"Code": "InternalError"}},
                             {**good, "response": {"Code": code, "detail": [{"objects": []}]}},
                             {**good, "response": {"Code": code, "clientToken": "admitted"}},
                             {**good, "response": f'<Error><Code>{code}</Code><detail><Outcome/></detail></Error>'},
                             {"response": {"Code": code}, "transportError": "timeout"}):
                with self.subTest(mode=mode, response=poisoned), self.assertRaises(ValueError):
                    validate_denial(poisoned, mode)

    def test_iam_evidence_redacts_credentials_without_modifying_oracle_input(self):
        with tempfile.TemporaryDirectory() as directory:
            fleet = Fleet([], Path(directory), "manual-auth-test")
            fleet.secret_values.update({"root-access", "root-secret", "user-secret"})
            probe = Probe(binding(), credential_pair=("root-access", "root-secret"))
            with patch.object(probe, "_open_admin", return_value=Response(b'{"message":"user-secret root-access root-secret"}')):
                result = iam_exchange(probe, fleet, "add-user", "rvhealowneduser", secret="user-secret")
            self.assertIn("user-secret", result["response"])
            archive = (Path(directory) / "00001-iam-add-user.json").read_text()
            for secret in fleet.secret_values:
                self.assertNotIn(secret, archive)
            self.assertFalse(json.loads(archive)["requestBodyArchived"])
            with self.assertRaisesRegex(ValueError, "namespace"):
                iam_exchange(probe, fleet, "remove-user", "unrelateduser")
            original = {"user-secret": {"details": ["user-secret"]}}
            self.assertIs(fleet.record("secret-key", original), original)
            self.assertNotIn("user-secret", (Path(directory) / "00002-secret-key.json").read_text())


class PrincipalLifecycleTest(unittest.TestCase):
    def execute(self, directory, *, failed_operation=None, negative_transport=False, revocation_broken=False):
        users, iam_calls, negative_calls = set(), [], []
        active = []
        fleet = Fleet([], Path(directory), "manual-auth-test")
        scene = {"node1": {"files": [], "formats": []}}
        class Client:
            def __init__(self, bound, *, credential_pair=None):
                self.binding = bound
                self.credential_pair = credential_pair
            def credentials(self):
                return self.credential_pair or ("root-access", "root-secret")
            def s3(self, operation, params):
                return {"acknowledged": False, "resultUnknown": False,
                        "response": {"ResponseMetadata": {"HTTPStatusCode": 403}, "Error": {"Code": "AccessDenied"}}}
            def admin(self, path, query, body, auth="signed"):
                if self.credential_pair and self.credential_pair[0] not in users:
                    return {"httpStatus": 403, "response": {"Code": "AccessDenied" if revocation_broken else "InvalidAccessKeyId"}}
                denied = self.credential_pair and self.credential_pair[0].endswith("denied")
                if auth != "signed" or denied:
                    mode = "denied-user" if denied else auth
                    operation = "stop" if dict(query).get("forceStop") else "status" if query else "start"
                    negative_calls.append((mode, operation))
                    if negative_transport and (mode, operation) == ("bad-signature", "status"):
                        raise OSError("isolated transport failure")
                    status, code = DENIALS[mode]
                    return {"httpStatus": status, "response": {"Code": code}}
                if not query:
                    return {"httpStatus": 200, "response": {"clientToken": "allowed-token" if self.credential_pair else "root-token"}}
                return {"httpStatus": 200, "response": {"summary": "finished"}}
            def inventory(self, *args, **kwargs):
                return {}
        def exchange(probe, recorded, operation, user, **kwargs):
            iam_calls.append((operation, user))
            if operation == "add-user": users.add(user)
            if operation == failed_operation: raise ValueError("injected IAM failure: " + operation)
            if operation == "remove-user": users.discard(user)
        self.addCleanup(lambda: self.assertFalse(active, "tokens must be finalized even on IAM failure"))
        with patch("scripts.validation_heal.authorization.Probe", Client), \
             patch("scripts.validation_heal.authorization.iam_exchange", side_effect=exchange), \
             patch.object(fleet, "snapshot", return_value=scene), \
             patch("scripts.validation_heal.authorization.compare_inventories", return_value={"unchanged": True}):
            try:
                branches = authorization_requests(Client(binding()), fleet, "rv-heal-auth-test", scene, {}, active, BASE_OPTIONS)
                return branches, iam_calls, negative_calls
            finally:
                self.last_calls, self.last_negatives = iam_calls, negative_calls

    def test_every_source_principal_controls_start_status_stop_and_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            branches, iam_calls, negatives = self.execute(directory)
            self.assertEqual(set(branches), set(select("A08")[0]["variants"]))
            self.assertTrue(all(branch["verdict"] == "PASS" for branch in branches.values()))
            self.assertEqual(set(negatives), {(mode, operation) for mode in DENIALS for operation in ("start", "status", "stop")})
            self.assertEqual([operation for operation, _ in iam_calls].count("remove-user"), 2)
            self.assertEqual([operation for operation, _ in iam_calls][-1], "remove-policy")

    def test_failed_or_ambiguous_admission_keeps_owned_principal_for_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "injected IAM failure"):
                self.execute(directory, failed_operation="add-user")
            self.assertEqual([operation for operation, _ in self.last_calls], ["add-user", "remove-user"])

    def test_cleanup_error_cannot_pass_and_other_cleanup_operations_continue(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "finalization failed"):
                self.execute(directory, failed_operation="detach")
            self.assertEqual([operation for operation, _ in self.last_calls][-3:], ["remove-user", "remove-user", "remove-policy"])
            result = next(Path(directory).glob("*-iam-finalization.json"))
            self.assertEqual(json.loads(result.read_text())["cleanup"], "FAIL")

    def test_one_denial_transport_failure_preserves_all_branches_and_never_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "every start/status/stop"):
                self.execute(directory, negative_transport=True)
            self.assertEqual(len(self.last_negatives), 12)
            result = next(Path(directory).glob("*-authorization-negative-branches.json"))
            self.assertEqual(json.loads(result.read_text())["bad-signature"]["verdict"], "FAIL")

    def test_removed_credentials_must_return_the_exact_revocation_error(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "removed temporary credentials"):
                self.execute(directory, revocation_broken=True)


if __name__ == "__main__":
    unittest.main()
