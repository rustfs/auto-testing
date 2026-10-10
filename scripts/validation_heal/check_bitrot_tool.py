#!/usr/bin/env python3
"""Check the read-only bitrot tool against pinned external release captures."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile


def check(binary: Path, source: Path):
    verified, rejected = 0, 0
    for kind in ("minio", "legacy"):
        root = source / "crates/ecstore/tests/fixtures/erasure-shards" / kind
        manifest = json.loads((root / "manifest.json").read_text())
        for relative, digest in manifest["files"].items():
            if hashlib.sha256((root / relative).read_bytes()).hexdigest() != digest:
                raise ValueError("external corpus differs from its pinned file manifest")
        for relative in sorted(manifest["files"]):
            part = root / relative
            if part.name != "part.1":
                continue
            metadata = part.parent.parent / "xl.meta"
            decoded = json.loads(subprocess.check_output([str(binary), str(metadata), manifest["bucket"], manifest["object"]], text=True))
            version = decoded["versions"][0].get("versionId") or "null"
            args = [str(binary), "--verify-part", str(metadata), manifest["bucket"], manifest["object"]]
            result = json.loads(subprocess.check_output([*args, str(part), version, "1"], text=True))
            if result.get("physicalBitrotVerified") is not True or result["physicalSize"] != part.stat().st_size:
                raise ValueError("external healthy shard did not pass the bitrot oracle")
            verified += 1
            if verified not in {1, 13}:
                continue
            original = part.read_bytes()
            changes = {"first": bytes([original[0] ^ 1]) + original[1:],
                       "middle": original[:len(original) // 2] + bytes([original[len(original) // 2] ^ 1]) + original[len(original) // 2 + 1:],
                       "last": original[:-1] + bytes([original[-1] ^ 1]),
                       "truncate": original[:-1], "zero": bytes(len(original)), "trailing": original + b"x"}
            with tempfile.TemporaryDirectory(prefix="heal-bitrot-oracle-") as directory:
                copied = Path(directory) / part.parent.name / part.name
                copied.parent.mkdir()
                for name, value in changes.items():
                    copied.write_bytes(value)
                    bad = subprocess.run([*args, str(copied), version, "1"], capture_output=True, text=True)
                    if bad.returncode == 0 or not any(message in bad.stderr for message in
                                                    ("hash mismatch", "short shard read", "trailing shard bytes")):
                        raise ValueError(f"{kind} {name} corruption was not rejected by the integrity oracle")
                    rejected += 1
    if verified != 24 or rejected != 12:
        raise ValueError("external bitrot corpus coverage is incomplete")
    return {"externalHealthyShardsVerified": verified, "corruptedShardsRejected": rejected,
            "corpora": ["MinIO released GF8", "RustFS released legacy GF16"], "liveCasePassClaimed": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(args.binary.resolve(strict=True), args.source.resolve(strict=True))))
