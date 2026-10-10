"""Reject false OIDC validation success at authentication boundaries."""

from datetime import datetime, timedelta, timezone
import os
import subprocess
import unittest
from unittest.mock import patch

from scripts.validation_oidc import aws, sts


class OidcBoundaryTest(unittest.TestCase):
    def test_sts_requires_subject_and_bounded_expiration(self):
        def response(subject, seconds):
            expiration = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
            return ("<AssumeRoleWithWebIdentityResponse><Credentials>"
                    "<AccessKeyId>test-key</AccessKeyId><SecretAccessKey>test-secret</SecretAccessKey>"
                    "<SessionToken>test-token</SessionToken>"
                    f"<Expiration>{expiration}</Expiration></Credentials>"
                    f"<SubjectFromWebIdentityToken>{subject}</SubjectFromWebIdentityToken>"
                    "</AssumeRoleWithWebIdentityResponse>").encode()
        with patch("scripts.validation_oidc.http", return_value=(200, response("expected", 900))):
            credentials, observation = sts("http://fixture", "identity-token", "expected")
            self.assertEqual(credentials["AWS_SESSION_TOKEN"], "test-token")
            self.assertNotIn("test-secret", str(observation))
        for subject, seconds in (("different", 900), ("expected", -1), ("expected", 3600)):
            with self.subTest(subject=subject, seconds=seconds):
                with patch("scripts.validation_oidc.http", return_value=(200, response(subject, seconds))):
                    with self.assertRaises(RuntimeError):
                        sts("http://fixture", "identity-token", "expected")

    def test_transport_error_cannot_prove_access_denial(self):
        credentials = {"AWS_ACCESS_KEY_ID": "temporary-key", "AWS_SECRET_ACCESS_KEY": "temporary-secret"}
        network_error = subprocess.CompletedProcess([], 255, "", "Could not connect to endpoint")
        with patch("scripts.validation_oidc.subprocess.run", return_value=network_error):
            with self.assertRaisesRegex(RuntimeError, "expected access boundary rejection"):
                aws("http://fixture", credentials, "get-object", denied=True)
        access_denied = subprocess.CompletedProcess([], 255, "", "An error occurred (AccessDenied) when calling GetObject")
        with patch.dict(os.environ, {"AWS_SESSION_TOKEN": "ambient-token", "AWS_PROFILE": "ambient-profile"}):
            with patch("scripts.validation_oidc.subprocess.run", return_value=access_denied) as run:
                self.assertEqual(aws("http://fixture", credentials, "get-object", denied=True), "AccessDenied")
                environment = run.call_args.kwargs["env"]
                self.assertNotIn("AWS_SESSION_TOKEN", environment)
                self.assertNotIn("AWS_PROFILE", environment)


if __name__ == "__main__":
    unittest.main()
