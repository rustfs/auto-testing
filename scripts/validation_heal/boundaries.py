#!/usr/bin/env python3
"""D01 size parameters from the fixed candidate contract and observed EC geometry."""

import hashlib
from pathlib import Path
import re

try:
    from .heal_probe import payload, require
except ImportError:
    from heal_probe import payload, require


BRANCHES = ("zero", "one", "4kib-minus-exact-plus", "inline-minus-exact-plus",
            "stripe-minus-exact-plus", "large-external")


def verify_size_contract(manifest, candidate):
    contract = manifest.get("sizeContract") or {}
    extractor = Path(__file__).with_name("prepare_heal_meta_inspector.py")
    require(contract.get("recognized") is True and contract.get("candidateCommit") == candidate["source_sha"]
            and contract.get("extractorSourceSha256") == hashlib.sha256(extractor.read_bytes()).hexdigest()
            and contract.get("inlineRule") == "ceil(size/dataBlocks)<=effectiveInlineBlock AND size<=actualECBlock"
            and contract.get("effectiveDefaultRule") == "min(ceil(2*defaultInlineBlock/dataBlocks),defaultInlineBlock)"
            and type(contract.get("defaultInlineBlock")) is int and contract["defaultInlineBlock"] > 0
            and contract.get("defaultObjectBudgetFactor") == 2
            and set(contract.get("sourceFiles", {})) == {
                "crates/ecstore/src/config/storageclass.rs", "crates/ecstore/src/set_disk/mod.rs",
                "crates/ecstore/src/set_disk/ops/object.rs", "crates/ecstore/src/erasure/coding/erasure.rs"}
            and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in contract["sourceFiles"].values()),
            "size boundaries require a recognized fresh exact-candidate source contract")
    return contract


def boundary_sizes(contract, target, processes, *, standard_class=None):
    require(standard_class in {None, "EC:8"}, "unreviewed boundary storage class")
    require(processes and all(entry.get("storageClassEnvironment") == {
                "RUSTFS_STORAGE_CLASS_INLINE_BLOCK": None, "RUSTFS_STORAGE_CLASS_STANDARD": standard_class}
            for entry in processes.values()), "size policy requires actual process environment confirmation")
    geometry = target["logical"]["erasure"]
    data, block = geometry["data_blocks"], geometry["block_size"]
    require(standard_class is None or data == geometry.get("parity_blocks") == 8
            and len(geometry.get("distribution", [])) == 16,
            "EC:8 boundary calibration requires the actual balanced sixteen-member object")
    require(type(data) is int and data > 0 and type(block) is int and 4097 < block < 12 * 1024**2
            and target["logical"]["usesLegacyChecksum"] is False,
            "observed size calibration needs bounded current-codec EC geometry")
    default = contract["defaultInlineBlock"]
    effective = min((2 * default + data - 1) // data, default)
    inline = min(effective * data, block)
    require(4097 < inline < block, "candidate policy lacks independently testable inline and stripe boundaries")
    groups = {"zero": [0], "one": [1], "4kib-minus-exact-plus": [4095, 4096, 4097],
              "inline-minus-exact-plus": [inline - 1, inline, inline + 1],
              "stripe-minus-exact-plus": [block - 1, block, block + 1], "large-external": [5 * block + 123]}
    require(set(groups) == set(BRANCHES), "size parameter matrix omitted a source boundary")
    return {"actualDataBlocks": data, "actualECBlockSize": block, "defaultInlineBlock": default,
            "effectiveInlineShardBudget": effective, "actualLogicalInlineLimit": inline,
            "sourceBoundRule": contract["inlineRule"], "groups": groups,
            "processConfigurationReadBack": True, "codec": "current"}


def seed_boundaries(recorder, bucket, parameters):
    expected, instances = [], []
    for branch, sizes in parameters["groups"].items():
        for size in sizes:
            key = f"boundaries/{len(expected):04d}-{size}.bin"
            body = payload(f"{bucket}/{key}/0", size)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "Metadata": {"heal-fixture": "D01", "size": str(size)}, "ContentType": "application/octet-stream"}, body)
            require(response.get("VersionId") in {None, "null"}, "boundary fixture became versioned")
            entry = {"key": key, "versionId": "null", "deleteMarker": False, "isLatest": True,
                     "payload": {"size": size, "sha256": hashlib.sha256(body).hexdigest()}}
            expected.append(entry)
            shape = "zero-metadata" if size == 0 else "inline" if size <= parameters["actualLogicalInlineLimit"] else "external"
            for role in ("data", "parity"):
                for kind in ("missing", "bitflip"):
                    instances.append({"name": f"{branch}-s{size}-{role}-{kind}", "sourceBranch": branch,
                        "size": size, "key": key, "role": role, "kind": kind, "expectedShape": shape,
                        "ecBlockSize": parameters["actualECBlockSize"]})
    return expected, instances


def verify_boundary_ranges(probe, fleet, bucket, expected, block):
    proofs = []
    for entry in expected:
        key, size = entry["key"], entry["payload"]["size"]
        if size == 0:
            denied = fleet.record("zero-range", probe.s3("get_object", {
                "Bucket": bucket, "Key": key, "VersionId": "null", "Range": "bytes=0-0"}))
            response = denied.get("response") or {}
            require(denied.get("acknowledged") is False and denied.get("resultUnknown") is False
                    and "payload" not in denied and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 416
                    and response.get("Error", {}).get("Code") == "InvalidRange",
                    "empty object Range must satisfy the frozen S3 416/InvalidRange contract")
            proofs.append({"key": key, "zeroRangeRejected": True, "status": 416, "code": "InvalidRange"})
            continue
        body = payload(f"{bucket}/{key}/0", size)
        intervals = {(size - 1, size - 1), (0, min(size - 1, 46))}
        if size > block:
            intervals.add((max(0, block - 17), min(size - 1, block + 29)))
        for start, end in sorted(intervals):
            observed = fleet.record("size-range", probe.s3("get_object", {
                "Bucket": bucket, "Key": key, "VersionId": "null", "Range": f"bytes={start}-{end}"}))
            response = observed.get("response") or {}
            require(observed.get("acknowledged") is True
                    and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 206
                    and response.get("ContentRange") == f"bytes {start}-{end}/{size}"
                    and observed.get("payload") == {"size": end - start + 1,
                        "sha256": hashlib.sha256(body[start:end + 1]).hexdigest()}, "actual size/stripe Range changed bytes or bounds")
            proofs.append({"key": key, "start": start, "end": end, "httpStatus": 206})
    return proofs
