#!/usr/bin/env python3
"""Check every real shell script and retain each command's actual exit status."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess


EXCLUDED = {".git", "node_modules", ".venv", "venv", "__pycache__"}


def check(root: Path, *, bash: str = "bash") -> dict:
    files = sorted(path for path in root.rglob("*.sh")
                   if path.is_file() and not any(part in EXCLUDED or part.startswith("._")
                                                for part in path.relative_to(root).parts))
    results = []
    for path in files:
        try:
            completed = subprocess.run([bash, "-n", str(path.resolve())], capture_output=True, text=True)
            result = {"file": str(path.relative_to(root)), "exit_code": completed.returncode,
                      "stdout": completed.stdout, "stderr": completed.stderr}
        except OSError as exc:
            result = {"file": str(path.relative_to(root)), "exit_code": 127,
                      "stdout": "", "stderr": str(exc)}
        results.append(result)
    return {"complete": bool(results), "passed": bool(results) and all(r["exit_code"] == 0 for r in results),
            "results": results}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    document = check(args.root)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(document, indent=2) + "\n")
    for result in document["results"]:
        print(f'{"PASS" if result["exit_code"] == 0 else "FAIL"} {result["file"]} exit={result["exit_code"]}')
        if result["stderr"]:
            print(result["stderr"], end="")
    if not document["complete"]:
        print("FAIL no shell scripts selected")
    return 0 if document["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
