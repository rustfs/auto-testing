#!/usr/bin/env python3
"""Stage the complete product Table smoke sources without importing dependencies."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys


ENTRY = "pyiceberg_smoke.py"
LOCAL_MODULES = ("engine_compatibility", "failure_coverage")


def validate(smoke_script: Path) -> list[Path]:
    files = [smoke_script]
    if not smoke_script.is_file():
        raise ValueError(f"Table smoke entry is missing: {smoke_script}")
    tree = ast.parse(smoke_script.read_text(), filename=str(smoke_script))
    imports = {
        alias.name.split(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    # Older product refs may not use both modules. Only require those imported
    # by this entry; never import the smoke just to validate local preparation.
    for name in LOCAL_MODULES:
        if name not in imports:
            continue
        module = smoke_script.with_name(f"{name}.py")
        if not module.is_file():
            raise ValueError(f"Table smoke local dependency is missing: {module.name}")
        ast.parse(module.read_text(), filename=str(module))
        files.append(module)
    return files


def prepare(source_root: Path, destination: Path, expected_sha: str = "") -> Path:
    source_root = source_root.resolve()
    source = source_root / "scripts/table-catalog"
    validate(source / ENTRY)
    result = subprocess.run(
        ["git", "-C", str(source_root), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    )
    sha = result.stdout.strip()
    if expected_sha and sha != expected_sha:
        raise ValueError(f"Table source SHA mismatch: expected {expected_sha}, got {sha}")
    dirty = subprocess.run(
        ["git", "-C", str(source_root), "status", "--porcelain", "--", "scripts/table-catalog"],
        check=True, capture_output=True, text=True,
    )
    if dirty.stdout.strip():
        raise ValueError("Table smoke source tree has uncommitted changes; use a clean candidate checkout")
    # Never merge a new source version with leftovers from an older run.
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns("._*", "__pycache__", "*.pyc"))
    entry = destination / ENTRY
    validate(entry)
    manifest = {
        "source_sha": sha,
        "files": {
            str(path.relative_to(destination)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(destination.rglob("*")) if path.is_file()
        },
    }
    (destination / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return entry


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check")
    check.add_argument("--smoke-script", type=Path, required=True)
    stage = sub.add_parser("prepare")
    stage.add_argument("--source-root", type=Path, required=True)
    stage.add_argument("--destination", type=Path, required=True)
    stage.add_argument("--expected-sha", default="")
    args = parser.parse_args()
    try:
        if args.command == "check":
            validate(args.smoke_script)
        else:
            print(prepare(args.source_root, args.destination, args.expected_sha))
    except (OSError, ValueError, SyntaxError, subprocess.CalledProcessError) as exc:
        print(f"[HARNESS-ERROR] Table smoke preparation: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
