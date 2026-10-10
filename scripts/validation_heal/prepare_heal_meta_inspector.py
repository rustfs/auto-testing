#!/usr/bin/env python3
"""Prepare (do not silently build) a read-only metadata inspector for one SHA."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def prepare(source: Path, commit: str, output: Path) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("full candidate commit required")
    source = source.resolve(strict=True)
    head = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True,
                          text=True, capture_output=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=no"],
                           check=True, text=True, capture_output=True).stdout
    if head != commit or dirty.strip():
        raise ValueError("inspector requires an unmodified checkout of the exact candidate SHA")
    crate = source / "crates/filemeta"
    if not (crate / "Cargo.toml").is_file():
        raise ValueError("candidate has no rustfs-filemeta crate")
    output.mkdir(parents=True, exist_ok=False)
    (output / "src").mkdir()
    helper = Path(__file__).resolve().parent / "tools/heal_meta_inspect.rs"
    text = helper.read_text()
    (output / "src/main.rs").write_text(text)
    manifest = ('[package]\nname = "heal-meta-inspect"\nversion = "0.1.0"\nedition = "2024"\n'
                '[workspace]\n[dependencies]\nserde_json = "1"\n'
                'rustfs-filemeta = { path = ' + json.dumps(str(crate)) + ' }\n')
    (output / "Cargo.toml").write_text(manifest)
    (output / "source.json").write_text(json.dumps({"candidateCommit": head,
        "candidateLockSha256": hashlib.sha256((source / "Cargo.lock").read_bytes()).hexdigest(),
        "helperSourceSha256": hashlib.sha256(text.encode()).hexdigest(),
        "role": "read-only decoder; not the RustFS service under test",
        "built": False}, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.source, args.commit, args.out)
    print(f"Prepared {args.out}; build on an approved host, then record its Cargo.lock and binary SHA-256.")


if __name__ == "__main__":
    main()
