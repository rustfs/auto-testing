"""MinIO-compatible S3 signing for raw Heal Admin requests."""

from urllib.parse import quote_from_bytes, unquote_to_bytes


def sign_admin_request(request, access_key: str, secret_key: str, session_token: str | None = None,
                       *, timestamp: str | None = None) -> None:
    # Keep SDK imports lazy so offline plan/binding tools need only stdlib.
    from botocore.auth import S3SigV4Auth
    from botocore.credentials import Credentials

    class AdminS3Auth(S3SigV4Auth):
        def add_auth(self, request):
            if timestamp is None:
                return super().add_auth(request)
            # Recompute the complete signature for the isolated client's old
            # timestamp. Editing an already signed date would test bad signing.
            from datetime import datetime
            datetime.strptime(timestamp, "%Y%m%dT%H%M%SZ")
            request.context["timestamp"] = timestamp
            self._modify_request_before_signing(request)
            canonical = self.canonical_request(request)
            string_to_sign = self.string_to_sign(request, canonical)
            self._inject_signature_to_request(request, self.signature(string_to_sign, request))

        def _normalize_url_path(self, path):
            # MinIO signs the decoded URL.Path after S3 URI encoding. Do not
            # normalize dot segments or decode twice: %252F is a literal %2F
            # key, while %2F and / identify the same separator. The wire URL
            # stays untouched so the server's routing/decoding is still tested.
            return quote_from_bytes(unquote_to_bytes(path), safe="/~")

    AdminS3Auth(Credentials(access_key, secret_key, session_token), "s3", "us-east-1").add_auth(request)
