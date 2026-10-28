#!/usr/bin/env python3
"""Verify a source-bound Linux Heal metadata decoder before distributing it."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import stat


def verify(directory: Path, candidate: dict) -> dict:
    root = directory.resolve(strict=True)
    paths = {name: root / name for name in ("heal-meta-inspect", "heal-meta-inspect.json", "inspector-Cargo.lock")}
    for name, path in paths.items():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 256 * 1024**2:
            raise ValueError(f"invalid inspector artifact file: {name}")
    manifest = json.loads(paths["heal-meta-inspect.json"].read_text())
    if (manifest.get("built") is not True or manifest.get("candidateCommit") != candidate["source_sha"]
            or manifest.get("target") != "x86_64-unknown-linux-gnu"
            or manifest.get("role") != "read-only decoder; not the RustFS service under test"):
        raise ValueError("inspector candidate or build role differs")
    helper = Path(__file__).with_name("tools") / "heal_meta_inspect.rs"
    digests = {"helperSourceSha256": hashlib.sha256(helper.read_bytes()).hexdigest(),
               "binarySha256": hashlib.sha256(paths["heal-meta-inspect"].read_bytes()).hexdigest(),
               "inspectorLockSha256": hashlib.sha256(paths["inspector-Cargo.lock"].read_bytes()).hexdigest()}
    if any(manifest.get(name) != value for name, value in digests.items()):
        raise ValueError("inspector source, binary or lock digest differs")
    if not re.fullmatch(r"[0-9a-f]{64}", manifest.get("candidateLockSha256", "")):
        raise ValueError("inspector omitted the candidate lock identity")
    with paths["heal-meta-inspect"].open("rb") as stream:
        header = stream.read(20)
    if header[:6] != b"\x7fELF\x02\x01" or header[18:20] != b"\x3e\x00":
        raise ValueError("inspector is not Linux x86_64 ELF")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory, json.loads(args.candidate.read_text())), indent=2))


if __name__ == "__main__":
    main()
