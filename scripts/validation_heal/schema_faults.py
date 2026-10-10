"""C09 format faults in one witnessed empty replacement root, never shared format."""

import base64
import copy
import hashlib
import json
import os
import uuid


def require(value, message):
    if not value:
        raise ValueError(message)


def reviewed_format(value):
    require(isinstance(value, dict) and value.get("version") == "1" and value.get("format") == "xl"
            and value.get("xl", {}).get("version") == "3", "format is outside the reviewed V1/XL V3 contract")
    require(str(uuid.UUID(value["id"])) == value["id"] and uuid.UUID(value["id"]).int != 0, "invalid deployment UUID")
    erasure = value["xl"]
    groups = erasure["sets"]
    require(isinstance(groups, list) and len(groups) == 1 and len(groups[0]) in {4, 16}
            and erasure.get("distributionAlgo") == "SIPMOD+PARITY", "unreviewed donor/set geometry")
    members = groups[0]
    require(len(members) == len(set(members)) and erasure["this"] in members
            and all(str(uuid.UUID(member)) == member and uuid.UUID(member).int != 0 for member in members),
            "format lacks distinct canonical member identities")
    return set(members)


def derive(original_raw, branch, donor=None):
    require(branch in {"foreign-disk", "foreign-set", "future-format"}, "unsupported C09 format branch")
    original = json.loads(original_raw)
    original_members = reviewed_format(original)
    changed = copy.deepcopy(original)
    if branch == "future-format":
        changed["version"] = "999"
        raw = json.dumps(changed, sort_keys=True, separators=(",", ":")).encode()
        detail = {"futureField": "version", "before": "1", "after": "999", "otherSemanticFieldsUnchanged": True}
    else:
        require(isinstance(donor, dict) and donor.get("readOnly") is True, "released-process donor evidence required")
        donor_raw = base64.b64decode(donor["rawBase64"], validate=True)
        require(len(donor_raw) < 65536, "donor format exceeds its exact byte budget")
        require(hashlib.sha256(donor_raw).hexdigest() == donor["sha256"], "donor raw format digest differs")
        foreign = json.loads(donor_raw)
        foreign_members = reviewed_format(foreign)
        require(donor.get("value") == foreign and foreign["id"] != original["id"]
                and not foreign_members & original_members, "donor reused the original deployment or member UUIDs")
        if branch == "foreign-disk":
            changed["xl"]["this"] = foreign["xl"]["this"]
            raw = json.dumps(changed, sort_keys=True, separators=(",", ":")).encode()
            detail = {"changedField": "xl.this", "actualForeignDiskUUID": foreign["xl"]["this"],
                      "originalDeploymentAndSetPreserved": True, "derivedIdentityFixture": True}
        else:
            # Preserve the real foreign cluster/set bytes without fabricating
            # set membership, disk UUIDs, deployment IDs or format versions.
            changed, raw = foreign, donor_raw
            detail = {"actualForeignDeploymentId": foreign["id"], "actualForeignSet": foreign["xl"]["sets"],
                      "releasedDonorBytesCopiedUnchanged": True, "derivedIdentityFixture": False}
    return raw, changed, detail


def install(root_fd, replacement, owner, request, fault):
    require(request.get("disk") == replacement["disk"] and replacement.get("originalArchiveUnchanged") is True
            and replacement.get("originalStillAtDataRoot") is False, "C09 requires its exact isolated original member")
    branch = request["branch"]
    donor = request.get("donor")
    if donor is not None:
        require(donor.get("binarySha256") == owner["binarySha256"], "donor candidate binary differs")
    with fault.parent(root_fd, ["quarantine", "old-disk", ".rustfs.sys"]) as saved:
        original = fault.read_file(saved, "format.json", 65536)
        require(fault.digest(original) == replacement["originalFormatSha256"], "original format archive differs")
    raw, changed, detail = derive(original, branch, donor)
    require(len(raw) < 65536, "derived format exceeds its byte budget")
    receipt = {"schemaVersion": 1, "runId": owner["runId"], "disk": replacement["disk"], "branch": branch,
        "relativePath": ".rustfs.sys/format.json", "originalSha256": fault.digest(original),
        "injectedSha256": fault.digest(raw), "injectedValue": changed, "detail": detail,
        "backend": "one-exact-empty-owned-replacement-format", "credentialsRead": False,
        "sharedFormatTouched": False, "backupRestorationIsProductRepair": False}
    with fault.parent(root_fd, ["quarantine"]) as archive, fault.parent(root_fd, ["data"]) as data:
        require(not os.listdir(data), "format mutation requires the witnessed empty replacement root")
        fault.new_file(archive, "C09-format-intent.json", fault.encode(receipt))
        fault.new_file(archive, "C09-injected-format.original", raw)
        os.mkdir(".rustfs.sys", 0o700, dir_fd=data)
        with fault.parent(root_fd, ["data", ".rustfs.sys"]) as target:
            fault.new_file(target, "format.json", raw)
            require(fault.read_file(target, "format.json", 65536) == raw, "format injection was not witnessed")
        os.fsync(data)
        fault.new_file(archive, "C09-format-applied.json", fault.encode(receipt))
    return receipt
