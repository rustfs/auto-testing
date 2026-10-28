"""D08 static KMS material only inside the stopped run-owned service namespace."""

import base64
import hashlib
import json
import os
import re
import stat


def settings_for(state, key_id):
    return {"RUSTFS_KMS_ENABLE": "true", "RUSTFS_KMS_BACKEND": "static", "RUSTFS_KMS_DEFAULT_KEY_ID": key_id,
        "RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY": "true", "RUSTFS_KMS_STATIC_SECRET_KEY_FILE": str(state / "static-kms-secret")}


def require(value, message):
    if not value: raise ValueError(message)


def configure(state, owner, request, fault):
    key_id = request.get("keyId")
    material = request.get("keyBase64")
    require(key_id == "rv-heal-key-" + owner["runId"] and isinstance(material, str), "encryption key is outside its exact run")
    decoded = base64.b64decode(material, validate=True)
    require(len(decoded) == 32 and len(set(decoded)) > 1, "fresh AES-256 material required")
    value = {"schemaVersion": 1, "runId": owner["runId"], "backend": "static", "keyId": key_id,
        "secretPath": str(state / "static-kms-secret"), "keyMaterialLogged": False, "sharedKmsTouched": False}
    with fault.directory(state) as fd:
        original = fault.read_file(fd, "service.env", 65536)
        require(not re.search(rb"(?m)^RUSTFS_KMS_", original), "run already has an encryption dependency configuration")
        secret = (key_id + ":" + material + "\n").encode()
        fault.new_file(fd, "static-kms-secret", secret)
        value["secretSha256"] = hashlib.sha256(secret).hexdigest()
        settings = settings_for(state, key_id)
        updated = original + "".join(f"{name}={item}\n" for name, item in settings.items()).encode()
        value.update(settings=settings, serviceEnvSha256=hashlib.sha256(updated).hexdigest())
        fault.new_file(fd, "encryption-intent.json", fault.encode(value))
        fault.new_file(fd, "service-encryption.env", updated)
        os.replace("service-encryption.env", "service.env", src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
        fault.new_file(fd, "encryption-config.json", fault.encode(value))
    return {name: value[name] for name in ("schemaVersion", "runId", "backend", "keyId", "keyMaterialLogged", "sharedKmsTouched")}


def attest(state, owner, checked, digest, environment=None):
    marker = checked(state / "encryption-config.json", state)
    intent = checked(state / "encryption-intent.json", state)
    if not marker.exists():
        require(not intent.exists(), "partial encryption configuration remains unresolved")
        return None
    require(marker.stat().st_size < 8192, "encryption attestation exceeds its bound")
    value = json.loads(marker.read_text())
    secret = checked(state / "static-kms-secret", state)
    require(stat.S_ISREG(secret.stat().st_mode) and stat.S_IMODE(secret.stat().st_mode) == 0o600
            and secret.stat().st_nlink == 1 and secret.stat().st_uid == marker.stat().st_uid,
            "dedicated key material no longer has its private file ownership")
    require(value.get("schemaVersion") == 1 and value.get("runId") == owner["runId"]
            and value.get("backend") == "static" and value.get("keyId") == "rv-heal-key-" + owner["runId"]
            and value.get("secretPath") == str(state / "static-kms-secret")
            and value.get("settings") == settings_for(state, value["keyId"])
            and digest(checked(state / "static-kms-secret", state)) == value["secretSha256"]
            and digest(checked(state / "service.env", state)) == value["serviceEnvSha256"],
            "run-owned KMS configuration or key material changed")
    if environment is not None:
        require(all(environment.get(name) == item for name, item in value["settings"].items()),
                "candidate process did not load the exact dedicated KMS configuration")
    return {"backend": "static", "keyId": value["keyId"], "runOwnedSecretFileUnchanged": True,
            "loadedByCandidate": environment is not None, "sharedKmsTouched": False, "keyMaterialLogged": False}
