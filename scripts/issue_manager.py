#!/usr/bin/env python3
"""Backlog issue manager for the RustFS functional suites.

One entry point (`handle`) replaces the per-run "file failure issue" steps.
Its contract:

- Issues are keyed by SIGNAL (suite category + failing case IDs), not by run.
  Before filing, open issues are searched by label; duplicates become a
  comment on the existing issue instead of a new issue.
- Every issue gets labels: functional-test, the suite category (s3, kms,
  tier, storage, security, replication, heal, pool, upgrade,
  fault-tolerance), one label per failing case ID (S3C-405 style, lazily
  created), and `env` when the failure looks environmental.
- On a fully green run, open issues whose tracked cases all passed are
  closed automatically with the evidence in the comment. Cancelled or
  partially-reported runs never file or close anything.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = "rustfs/backlog"
CASE_ID_RE = re.compile(r"^[A-Z][A-Z0-9]*-[0-9]+$")
# Case rows in cases.md as produced by scripts/functional_case_report.py:
# | S3C-101 | CreateBucket (topology:single-single) | PASS |
TABLE_ROW_RE = re.compile(r"^\|\s*([A-Z][A-Z0-9]*-[0-9]+)\s*\|(.+)\|\s*(PASS|FAIL|UNSUPPORTED|RUNNING|SKIP)\s*\|")
# Suite-log verdict lines: [FAIL] STOR-115 InlineEncryption / [PASS] KMS-317 ...
LOG_VERDICT_RE = re.compile(r"^\[(PASS|FAIL|UNSUPPORTED)\]\s+([A-Z][A-Z0-9]*-[0-9]+)\b")
# Fault-tolerance suite verdicts: FT-CASE: <case> verdict=pass|known-divergence|UNEXPECTED
FT_VERDICT_RE = re.compile(r"^FT-CASE:\s+(\S+)\s+verdict=(\S+)")
# Heal suite step lines carry no case IDs: [HEAL-STEP] 5 start cluster heal PASS
HEAL_STEP_RE = re.compile(r"^\[HEAL-STEP\]\s+\d+\s+(.+?)\s+(PASS|FAIL|SKIP)\s*$")
# A failing suite-level verdict (heal steps/results, pool steps) means the
# harness reached the product even though no case IDs exist: classify as a
# suite failure, not env.
SUITE_LEVEL_FAIL_RE = re.compile(r"^\[(?:HEAL-RESULT|HEAL-STEP|POOL-STEP)\].*\bFAIL\b", re.M)
# A pool script abort goes through die(), which prints [ERROR] <reason>; the
# aborted step never emits its own [POOL-STEP] FAIL verdict, so the die
# signature is the only trace of a product-level abort in the log.
POOL_ABORT_RE = re.compile(r"^\x1b\[[0-9;]*m?\[ERROR\]", re.M)

# Signatures that mean "the harness never reached the product".
ENV_SIGNATURES = (
    "curl: (22)",
    "ssh: connect to host",
    "No route to host",
    "Connection timed out",
    "unable to clone",
    "dpkg: error processing package",
    "unbound variable",
    "command not found",
    "Failed to connect to 127.0.0.1",
    "not first disk",
    "gh: command not found",
)

REDACT_SED = [
    (re.compile(r"(RUSTFS_(ACCESS_KEY|SECRET_KEY)[=: ]+)[^ \n]+", re.I), r"\1[REDACTED]"),
    (re.compile(r"(Authorization:).*", re.I), r"\1 [REDACTED]"),
    (re.compile(r"(X-Amz-Signature=)[^& \n]+", re.I), r"\1[REDACTED]"),
    (re.compile(r"rustfs@test"), "[REDACTED]"),
    (re.compile(r"^.*(password|secret|token)[=: ].*$", re.I | re.M), "[REDACTED SENSITIVE LINE]"),
]

CATEGORY_COLORS = {
    "s3": "1D76DB", "kms": "5319E7", "tier": "B60205", "storage": "C2E0C6",
    "security": "D93F0B", "replication": "0E8A16", "heal": "F9D0C4",
    "pool": "FEF2C0", "upgrade": "BFD4F2", "fault-tolerance": "E99695",
    "env": "FBCA04",
}
CASE_COLOR = "D4C5F9"


def redact(text: str) -> str:
    for pattern, repl in REDACT_SED:
        text = pattern.sub(repl, text)
    return text


def run_gh(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=check)


def ensure_label(name: str, dry: bool) -> None:
    color = CATEGORY_COLORS.get(name, CASE_COLOR)
    if dry:
        print(f"[dry] ensure label {name} (color {color})")
        return
    run_gh(["label", "create", name, "-R", REPO, "--color", color,
            "--description", f"managed by issue_manager.py ({name})", "--force"],
           check=False)


def search_open(labels: list[str]) -> list[dict]:
    args = ["issue", "list", "-R", REPO, "--state", "open", "--json", "number,title,labels"]
    for label in labels:
        args += ["--label", label]
    proc = run_gh(args, check=False)
    if proc.returncode != 0:
        print(f"warn: issue search failed: {proc.stderr.strip()}", file=sys.stderr)
        return []
    return json.loads(proc.stdout or "[]")


def issue_labels(number: int) -> set[str]:
    proc = run_gh(["issue", "view", str(number), "-R", REPO, "--json", "labels",
                   "--jq", ".labels[].name"], check=False)
    return {l.strip() for l in (proc.stdout or "").splitlines() if l.strip()}


def has_comment_with(number: int, needle: str) -> bool:
    proc = run_gh(["issue", "view", str(number), "-R", REPO, "--json", "comments",
                   "--jq", ".comments[].body"], check=False)
    return needle in (proc.stdout or "")


def parse_cases(report: Path | None, log: Path | None) -> dict[str, dict]:
    """Return {case_id: {name, statuses:set}} aggregated across topologies."""
    cases: dict[str, dict] = {}

    def record(cid: str, status: str, name: str = "") -> None:
        entry = cases.setdefault(cid, {"name": name or cid, "statuses": set()})
        if name and entry["name"] == cid:
            entry["name"] = name
        entry["statuses"].add(status)

    if report and report.exists():
        for raw in report.read_text(errors="replace").splitlines():
            m = TABLE_ROW_RE.match(raw.strip())
            if m:
                cid, name, status = m.group(1), m.group(2).strip(), m.group(3)
                record(cid, status, name.split("(")[0].strip() or cid)
    if log and log.exists():
        ansi = re.compile(r"\x1b\[[0-9;]*m")
        for raw in log.read_text(errors="replace").splitlines():
            line = ansi.sub("", raw).strip()
            m = LOG_VERDICT_RE.match(line)
            if m:
                record(m.group(2), m.group(1))
                continue
            m = FT_VERDICT_RE.match(line)
            if m:
                status = "FAIL" if m.group(2) == "UNEXPECTED" else (
                    "PASS" if m.group(2).startswith("pass") else "SKIP")
                if status != "SKIP":
                    record(m.group(1), status)
    return cases


def classify(outcome: str, cases: dict, log: Path | None) -> tuple[str, str]:
    """Return (bucket, reason): bucket is 'success' | 'case' | 'env' | 'noop'."""
    if outcome == "success":
        return "success", "suite reported success"
    if outcome != "failure":
        return "noop", f"outcome {outcome}: never file or close on cancelled runs"
    if not cases:
        if log and log.exists():
            text = log.read_text(errors="replace")
            if SUITE_LEVEL_FAIL_RE.search(text) or POOL_ABORT_RE.search(text):
                return "suite", "suite-level failure verdict without case IDs"
        return "env", "no case results were recorded"
    fail = [c for c, v in cases.items() if "FAIL" in v["statuses"]]
    pas = [c for c, v in cases.items() if "PASS" in v["statuses"] and "FAIL" not in v["statuses"]]
    if not pas and len(fail) >= 3:
        return "env", f"wholesale failure (0 passed, {len(fail)} failed)"
    if log and log.exists():
        text = log.read_text(errors="replace")
        for sig in ENV_SIGNATURES:
            if sig in text:
                return "env", f"bootstrap signature: {sig!r}"
    return "case", f"{len(fail)} failing case(s)"


def close_issue(number: int, comment: str, dry: bool) -> None:
    print(f"[action] close #{number}: {comment.splitlines()[0]}")
    if dry:
        return
    cfile = Path("/tmp") / f"im-close-{number}.md"
    cfile.write_text(comment)
    if run_gh(["issue", "close", str(number), "-R", REPO, "--comment-file", str(cfile)],
              check=False).returncode != 0:
        run_gh(["issue", "close", str(number), "-R", REPO, "--comment", comment], check=False)
    cfile.unlink(missing_ok=True)


def comment_issue(number: int, body: str, dry: bool) -> None:
    print(f"[action] comment on #{number}: {body.splitlines()[0]}")
    if dry:
        return
    bfile = Path("/tmp") / f"im-comment-{number}.md"
    bfile.write_text(body)
    run_gh(["issue", "comment", str(number), "-R", REPO, "--body-file", str(bfile)], check=False)
    bfile.unlink(missing_ok=True)


def file_issue(title: str, body: str, labels: list[str], dry: bool) -> None:
    print(f"[action] file new issue {title!r} labels={labels}")
    if dry:
        return
    for label in labels:
        ensure_label(label, dry)
    bfile = Path("/tmp") / "im-new-issue.md"
    bfile.write_text(body)
    args = ["issue", "create", "-R", REPO, "--title", title, "--body-file", str(bfile)]
    for label in labels:
        args += ["--label", label]
    proc = run_gh(args, check=False)
    if proc.returncode != 0:  # label may have been rejected; retry bare
        proc = run_gh(["issue", "create", "-R", REPO, "--title", title,
                       "--body-file", str(bfile)], check=False)
    bfile.unlink(missing_ok=True)
    if proc.returncode == 0:
        print(proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "filed")
    else:
        print(f"error filing issue: {proc.stderr.strip()}", file=sys.stderr)


def suite_level_matches(category: str) -> list[dict]:
    """Open issues of this suite that track no case IDs and are not env issues."""
    out = []
    for m in search_open([category]):
        names = {l["name"] for l in m.get("labels", [])}
        if not any(CASE_ID_RE.match(n) for n in names) and "env" not in names:
            out.append(m)
    return out


def handle_failure(args, cases, bucket, reason) -> None:
    failing = sorted(cid for cid, v in cases.items() if "FAIL" in v["statuses"])
    if bucket == "suite":
        body = (f"Suite-level failure still reproducing: {reason}\n\n"
                f"- Run: {args.run_url}\n- Suite: `{args.suite}`\n"
                f"- Detected: {reason}\n")
        matches = suite_level_matches(args.category)
        if matches:
            n = min(m["number"] for m in matches)
            if not has_comment_with(n, args.run_url):
                comment_issue(n, body, args.dry_run)
            else:
                print(f"#{n} already records run {args.run_id}; skipping comment")
        else:
            file_issue(f"[functional][{args.suite}] suite-level failure",
                       redact(body) + "\nManaged by issue_manager.py: closes automatically "
                       "on the next fully green run of this suite.\n",
                       ["functional-test", args.category], args.dry_run)
        return
    if bucket == "env":
        matches = search_open([args.category, "env"])
        body = (f"Environment/bootstrap failure still reproducing: {reason}\n\n"
                f"- Run: {args.run_url}\n- Suite: `{args.suite}`\n"
                f"- Detected: {reason}\n")
        if matches:
            n = min(m["number"] for m in matches)
            if not has_comment_with(n, args.run_url):
                comment_issue(n, body, args.dry_run)
            else:
                print(f"#{n} already records run {args.run_id}; skipping comment")
        else:
            title = f"[functional][{args.suite}] environment/bootstrap failure"
            file_issue(title, redact(body) + "\nManaged by issue_manager.py: closes "
                       "automatically on the next fully green run of this suite.\n",
                       ["functional-test", args.category, "env"], args.dry_run)
        return

    covered: dict[str, int] = {}
    for cid in failing:
        matches = search_open([args.category, cid])
        if matches:
            covered[cid] = min(m["number"] for m in matches)

    new_ids = [cid for cid in failing if cid not in covered]

    # One coalesced comment per issue per run (never two for the same run).
    by_issue: dict[int, list[str]] = {}
    for cid, number in covered.items():
        by_issue.setdefault(number, []).append(cid)
    for number, cids in sorted(by_issue.items()):
        if not has_comment_with(number, args.run_url):
            listed = ", ".join(f"`{c}`" for c in sorted(cids))
            comment_issue(number,
                          f"Still reproducing: {listed} FAIL in "
                          f"[run {args.run_id}]({args.run_url}) (suite `{args.suite}`).",
                          args.dry_run)
        else:
            print(f"#{number} already records run {args.run_id}; skipping comment")

    if not new_ids:
        print("all failing cases are covered by open issues; nothing to file")
        return

    shown = ", ".join(f"`{c}`" for c in new_ids[:4]) + (f" +{len(new_ids)-4} more" if len(new_ids) > 4 else "")
    title = f"[functional][{args.suite}] failing: {shown}"
    lines = [
        f"The **{args.suite_label}** suite reports new failing case(s) not covered by an open issue: {shown}.",
        "",
        f"- Run: {args.run_url}",
        f"- Attempt: {args.attempt}",
        f"- Workflow Commit: {args.commit}",
        f"- Trigger: {args.trigger}",
        f"- Package: {args.package_source or 'N/A'}",
        f"- Date: {args.date}",
        f"- Detection: {reason}",
        "",
        "## Failing cases",
        "",
        "| Case | Name | Statuses (all topologies) |",
        "| --- | --- | --- |",
    ]
    for cid in new_ids:
        v = cases[cid]
        lines.append(f"| {cid} | {v['name']} | {', '.join(sorted(v['statuses']))} |")
    lines += ["", "## Report tail", "", "```text"]
    tail = ""
    if args.report_file and Path(args.report_file).exists():
        tail = Path(args.report_file).read_text(errors="replace")[-6000:]
    elif args.log and Path(args.log).exists():
        tail = "\n".join(Path(args.log).read_text(errors="replace").splitlines()[-120:])
    lines.append(redact(tail)[:6000])
    lines += ["```", "", "Managed by issue_manager.py: duplicates are commented, not re-filed; "
              "this issue closes automatically once every tracked case passes a full run. "
              "If this is environmental, it carries the `env` label."]

    labels = ["functional-test", args.category] + new_ids
    file_issue(title, "\n".join(lines), labels, args.dry_run)


def handle_success(args, cases) -> None:
    # A green process exit is not execution evidence. In particular, one
    # topology passing must not hide another topology that never ran the case.
    if not cases or any(value["statuses"] != {"PASS"} for value in cases.values()):
        print("no complete all-PASS case evidence; keeping historical issues open")
        return
    passing = set(cases)
    matches = search_open([args.category])
    if not matches:
        print(f"no open issues labeled {args.category}; nothing to close")
        return
    for m in matches:
        number = m["number"]
        labels = issue_labels(number)
        if not {"functional-test", args.category}.issubset(labels):
            continue
        tracked = sorted(l for l in labels if CASE_ID_RE.match(l))
        if tracked:
            not_passed = [c for c in tracked if c not in passing]
            if not_passed:
                print(f"#{number}: keeps open ({', '.join(not_passed)} did not pass this run)")
                continue
            close_issue(number,
                        f"All tracked cases ({', '.join(f'`{c}`' for c in tracked)}) passed in "
                        f"[run {args.run_id}]({args.run_url}); closing as fixed. "
                        f"If it regresses, the suite will file a new issue automatically.", args.dry_run)
        elif "env" in labels:
            close_issue(number,
                        f"Suite `{args.suite}` completed a fully green run "
                        f"([run {args.run_id}]({args.run_url})); closing this environment issue. "
                        f"It will be re-filed if the environment breaks again.", args.dry_run)
        else:
            close_issue(number,
                        f"Suite `{args.suite}` completed a fully green run "
                        f"([run {args.run_id}]({args.run_url})); closing as fixed. "
                        f"Reopen or re-file if the symptom reproduces.", args.dry_run)


def main() -> int:
    global REPO
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    h = sub.add_parser("handle")
    h.add_argument("--repo", default=REPO, help="backlog repository (default: %(default)s)")
    h.add_argument("--suite", required=True)
    h.add_argument("--category", required=True)
    h.add_argument("--suite-label", required=True)
    h.add_argument("--outcome", required=True, choices=["success", "failure", "cancelled", "skipped"])
    h.add_argument("--report", default="")       # cases.md
    h.add_argument("--report-file", default="")  # report.md
    h.add_argument("--log", default="")          # suite.log
    h.add_argument("--run-url", default="")
    h.add_argument("--run-id", default="")
    h.add_argument("--attempt", default="")
    h.add_argument("--commit", default="")
    h.add_argument("--trigger", default="")
    h.add_argument("--package-source", default="")
    h.add_argument("--date", default="")
    h.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    REPO = args.repo

    cases = parse_cases(Path(args.report) if args.report else None,
                        Path(args.log) if args.log else None)
    bucket, reason = classify(args.outcome, cases, Path(args.log) if args.log else None)
    print(f"issue_manager: suite={args.suite} outcome={args.outcome} bucket={bucket} ({reason}); "
          f"cases={len(cases)} failing={sum(1 for v in cases.values() if 'FAIL' in v['statuses'])}")

    if bucket == "success":
        handle_success(args, cases)
    elif bucket in ("env", "case", "suite"):
        handle_failure(args, cases, bucket, reason)
    else:
        print("noop: no issue actions for this run")
    return 0


if __name__ == "__main__":
    sys.exit(main())
